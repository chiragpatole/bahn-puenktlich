"""Poll the DB Timetables API and store every response untouched.

One run = one gzipped JSONL file, one line per API call. Nothing is parsed
here on purpose: the XML goes to bronze as it arrived, so parsing bugs can be
fixed later without losing data.
"""

import argparse
import csv
import gzip
import json
import logging
import os
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

DEFAULT_BASE_URL = "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1"
DEFAULT_VOLUME_PATH = "/Volumes/bahn/bronze/landing"
BERLIN = ZoneInfo("Europe/Berlin")
REPO_ROOT = Path(__file__).resolve().parent.parent

log = logging.getLogger("collector")


def load_stations(path):
    with open(path, newline="", encoding="utf-8") as fh:
        rows = [r for r in csv.DictReader(fh) if r.get("eva")]
    evas = [r["eva"] for r in rows]
    dupes = {e for e in evas if evas.count(e) > 1}
    if dupes:
        raise ValueError(f"duplicate EVA numbers in {path}: {sorted(dupes)}")
    return rows


def plan_slices(now_utc, hours_ahead):
    """(yyMMdd, HH) pairs in German local time, starting with the current hour.

    Hours are added in UTC and converted afterwards. Adding them to local time
    would produce 02:xx on the night the clocks jump forward, an hour that
    does not exist. On the night they go back, 02 comes up twice and is only
    requested once.
    """
    slices = []
    for offset in range(hours_ahead + 1):
        local = (now_utc + timedelta(hours=offset)).astimezone(BERLIN)
        key = (local.strftime("%y%m%d"), local.strftime("%H"))
        if key not in slices:
            slices.append(key)
    return slices


class TimetablesClient:
    def __init__(self, client_id, api_key, base_url=DEFAULT_BASE_URL, session=None,
                 min_interval=1.1, max_attempts=3, sleep=None):
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.headers = {
            "DB-Client-Id": client_id,
            "DB-Api-Key": api_key,
            "Accept": "application/xml",
        }
        self.min_interval = min_interval
        self.max_attempts = max_attempts
        self.sleep = sleep or time.sleep
        self._last_call = None

    def _throttle(self):
        if self._last_call is not None:
            wait = self.min_interval - (time.monotonic() - self._last_call)
            if wait > 0:
                self.sleep(wait)
        self._last_call = time.monotonic()

    def get(self, path):
        url = f"{self.base_url}/{path}"
        attempts = 0
        while True:
            attempts += 1
            self._throttle()
            started = time.monotonic()
            fetched_at = datetime.now(UTC)
            try:
                resp = self.session.get(url, headers=self.headers, timeout=30)
                status, body, error = resp.status_code, resp.text, None
            except requests.RequestException as exc:
                status, body, error = None, None, f"{type(exc).__name__}: {exc}"
            duration_ms = round((time.monotonic() - started) * 1000)

            retryable = status is None or status == 429 or status >= 500
            if not retryable or attempts >= self.max_attempts:
                break
            self.sleep(2 ** attempts)

        if error is None and status != 200:
            error = f"HTTP {status}"
        return {
            "url": url,
            "fetched_at": fetched_at.isoformat(timespec="seconds"),
            "status_code": status,
            "duration_ms": duration_ms,
            "attempts": attempts,
            "error": error,
            "body": body,
        }


def collect(stations, client, now_utc, hours_ahead, run_id):
    slices = plan_slices(now_utc, hours_ahead)
    records = []
    for station in stations:
        eva = station["eva"]
        calls = [("fchg", f"fchg/{eva}", None, None)]
        calls += [("plan", f"plan/{eva}/{d}/{h}", d, h) for d, h in slices]
        for api_name, path, plan_date, plan_hour in calls:
            rec = client.get(path)
            rec.update(
                run_id=run_id,
                api_name=api_name,
                eva=eva,
                station_name=station.get("name"),
                plan_date=plan_date,
                plan_hour=plan_hour,
            )
            records.append(rec)
            if rec["status_code"] in (401, 403):
                # every later call would fail the same way and burn the quota
                raise PermissionError(
                    f"API rejected the credentials ({rec['error']}). Check that the "
                    "application is subscribed to Timetables in the DB API Marketplace."
                )
    return records


def is_failure(rec):
    if rec["status_code"] == 200:
        return False
    # A plan slice for an hour with no trains is recorded but not counted
    # against the run.
    empty_plan = rec["api_name"] == "plan" and rec["status_code"] == 404
    return not empty_plan


def bundle_relpath(run_start_utc, run_id):
    stamp = run_start_utc.strftime("%Y%m%dT%H%M%SZ")
    return (
        f"timetables/year={run_start_utc:%Y}/month={run_start_utc:%m}/day={run_start_utc:%d}/"
        f"timetables_{stamp}_{run_id[:8]}.jsonl.gz"
    )


def write_bundle(records, out_dir, relpath):
    target = Path(out_dir) / relpath
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    tmp.rename(target)
    return target


def upload_to_adls(local_path, relpath, account, container):
    from azure.identity import DefaultAzureCredential
    from azure.storage.filedatalake import DataLakeServiceClient

    service = DataLakeServiceClient(
        account_url=f"https://{account}.dfs.core.windows.net",
        credential=DefaultAzureCredential(),
    )
    file_client = service.get_file_system_client(container).get_file_client(relpath)
    with open(local_path, "rb") as fh:
        file_client.upload_data(fh, overwrite=True)


def upload_to_volume(local_path, relpath, host, token, volume_path, session=None):
    """Databricks Files API. volume_path looks like /Volumes/<catalog>/<schema>/<volume>."""
    session = session or requests.Session()
    host = host.rstrip("/")
    remote = f"{volume_path.rstrip('/')}/{relpath}"
    auth = {"Authorization": f"Bearer {token}"}

    parent = remote.rsplit("/", 1)[0]
    session.put(f"{host}/api/2.0/fs/directories{parent}", headers=auth, timeout=60).raise_for_status()
    with open(local_path, "rb") as fh:
        resp = session.put(
            f"{host}/api/2.0/fs/files{remote}",
            params={"overwrite": "true"},
            headers={**auth, "Content-Type": "application/octet-stream"},
            data=fh,
            timeout=300,
        )
    resp.raise_for_status()


def run(args):
    env = os.environ
    client = TimetablesClient(
        client_id=env["DB_CLIENT_ID"],
        api_key=env["DB_API_KEY"],
        base_url=env.get("DB_API_BASE_URL") or DEFAULT_BASE_URL,
        min_interval=args.min_interval,
    )
    stations = load_stations(args.stations)
    run_start = datetime.now(UTC)
    run_id = uuid.uuid4().hex

    records = collect(stations, client, run_start, args.hours_ahead, run_id)
    relpath = bundle_relpath(run_start, run_id)
    local_path = write_bundle(records, args.out_dir, relpath)

    sink_errors = []
    if env.get("AZURE_STORAGE_ACCOUNT"):
        try:
            upload_to_adls(local_path, relpath, env["AZURE_STORAGE_ACCOUNT"],
                           env.get("AZURE_STORAGE_CONTAINER") or "raw")
        except Exception as exc:  # keep going so the other sink still gets the file
            sink_errors.append(f"adls: {exc}")
    if env.get("DATABRICKS_HOST") and env.get("DATABRICKS_TOKEN"):
        try:
            upload_to_volume(local_path, relpath, env["DATABRICKS_HOST"], env["DATABRICKS_TOKEN"],
                             env.get("DATABRICKS_VOLUME_PATH") or DEFAULT_VOLUME_PATH)
        except Exception as exc:
            sink_errors.append(f"volume: {exc}")

    failed = [r for r in records if is_failure(r)]
    summary = {
        "run_id": run_id,
        "stations": len(stations),
        "calls": len(records),
        "failed": len(failed),
        "bytes": local_path.stat().st_size,
        "file": str(local_path),
        "sink_errors": sink_errors,
    }
    log.info("run summary %s", json.dumps(summary))
    for rec in failed[:10]:
        log.warning("failed call %s -> %s", rec["url"], rec["error"])

    if sink_errors:
        return 2
    if len(failed) > args.max_failed_share * len(records):
        return 1
    return 0


def _client_from_env():
    env = os.environ
    return TimetablesClient(env["DB_CLIENT_ID"], env["DB_API_KEY"],
                            base_url=env.get("DB_API_BASE_URL") or DEFAULT_BASE_URL)


def lookup_station(client, eva):
    """(name, meta EVA numbers, error) for one station number.

    Big stations are split across several numbers (e.g. Berlin Hbf upper level,
    lower level and S-Bahn). The number people know can return empty timetables;
    the trains live under the numbers listed in the station's meta attribute.
    """
    rec = client.get(f"station/{eva}")
    if rec["status_code"] in (401, 403):
        raise PermissionError(
            f"API rejected the credentials ({rec['error']}). Check that the "
            "application is subscribed to Timetables in the DB API Marketplace."
        )
    if rec["status_code"] != 200 or not rec["body"]:
        return None, [], rec["error"]
    node = ET.fromstring(rec["body"]).find("station")
    if node is None:
        return None, [], "no station in response"
    meta = [m for m in (node.get("meta") or "").split("|") if m]
    return node.get("name"), meta, None


def check_stations(args):
    """Ask the API what each EVA number in stations.csv is called, and flag
    meta numbers that are not polled yet."""
    client = _client_from_env()
    stations = load_stations(args.stations)
    known = {s["eva"] for s in stations}
    problems = 0
    for station in stations:
        found, meta, error = lookup_station(client, station["eva"])
        ok = found is not None and found.casefold() == station["name"].casefold()
        missing = [m for m in meta if m not in known]
        problems += (not ok) + bool(missing)
        line = f"{'ok ' if ok else 'FIX'}  {station['eva']}  csv={station['name']!r}  api={found!r}"
        if error:
            line += f"  ({error})"
        if missing:
            line += f"  meta not polled: {', '.join(missing)}"
        print(line)
    if problems:
        print("\nRun `python -m collector.collect expand-meta` to add missing meta stations.")
    return 1 if problems else 0


STATION_COLUMNS = ["eva", "name", "group", "parent_eva"]


def expand_meta(args):
    """Add every meta station number to stations.csv, linked to its parent."""
    client = _client_from_env()
    stations = load_stations(args.stations)
    known = {s["eva"] for s in stations}
    added = []
    for station in list(stations):
        if station.get("parent_eva"):
            continue
        _, meta, _ = lookup_station(client, station["eva"])
        for eva in meta:
            if eva in known:
                continue
            name, _, error = lookup_station(client, eva)
            if name is None:
                print(f"skip {eva} (meta of {station['name']}): {error}")
                continue
            row = {"eva": eva, "name": name, "group": station.get("group", ""),
                   "parent_eva": station["eva"]}
            stations.append(row)
            known.add(eva)
            added.append(row)
            print(f"add  {eva}  {name}  (part of {station['name']})")

    write_stations(args.stations, stations)
    print(f"{len(added)} stations added, {len(stations)} in total")
    return 0


def write_stations(path, stations):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=STATION_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in stations:
            writer.writerow({col: row.get(col) or "" for col in STATION_COLUMNS})


# Categories that are road vehicles. Meta lists also contain bus stops in front of
# the station; their stops would mix bus punctuality into train punctuality.
ROAD_CATEGORIES = {"BUS", "SEV", "STR", "TRAM", "FLX-BUS", "FLXB"}


def count_stops_in_bundles(raw_dir):
    """Per eva: Counter of train categories seen in the plan responses of local bundles."""
    from collections import Counter, defaultdict

    from transform.iris import parse_timetable

    seen = defaultdict(Counter)
    files = sorted(Path(raw_dir).rglob("*.jsonl.gz"))
    for f in files:
        with gzip.open(f, "rt", encoding="utf-8") as fh:
            for line in fh:
                rec = json.loads(line)
                if rec.get("api_name") != "plan":
                    continue
                for stop in parse_timetable(rec.get("body")) or []:
                    seen[rec["eva"]][(stop["category"] or "?").upper()] += 1
    return seen, len(files)


def prune_meta(args):
    """Drop sub-stations that had no train stops in the local bundles."""
    stations = load_stations(args.stations)
    seen, n_files = count_stops_in_bundles(args.raw_dir)
    if n_files == 0:
        print(f"no bundles under {args.raw_dir}, run the collector first")
        return 1
    parents = {s["eva"]: s["name"] for s in stations}
    kept, dropped = [], []
    for s in stations:
        if not s.get("parent_eva"):
            kept.append(s)
            continue
        cats = seen.get(s["eva"], {})
        trains = sum(n for c, n in cats.items() if c not in ROAD_CATEGORIES)
        mix = " ".join(f"{c}:{n}" for c, n in sorted(cats.items(), key=lambda kv: -kv[1])) or "-"
        line = f"{trains:5d}  {s['eva']:>8}  {s['name']}  -> {parents.get(s['parent_eva'])}  [{mix}]"
        (kept if trains >= args.min_stops else dropped).append(s)
        print(("keep " if trains >= args.min_stops else "drop ") + line)
    if not args.dry_run:
        write_stations(args.stations, kept)
    verb = "would drop" if args.dry_run else "dropped"
    print(f"{n_files} bundles read; {verb} {len(dropped)}, {len(kept)} stations left")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(prog="collector")
    parser.add_argument("--stations", default=str(REPO_ROOT / "collector" / "stations.csv"))
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="collect one bundle")
    p_run.add_argument("--out-dir", default=str(REPO_ROOT / "data" / "raw"))
    p_run.add_argument("--hours-ahead", type=int, default=2,
                       help="plan slices to fetch beyond the current hour")
    p_run.add_argument("--min-interval", type=float, default=1.1,
                       help="seconds between API calls")
    p_run.add_argument("--max-failed-share", type=float, default=0.2)
    p_run.set_defaults(func=run)

    p_check = sub.add_parser("check-stations", help="verify stations.csv against the API")
    p_check.set_defaults(func=check_stations)

    p_meta = sub.add_parser("expand-meta", help="add meta station numbers to stations.csv")
    p_meta.set_defaults(func=expand_meta)

    p_prune = sub.add_parser("prune-meta", help="drop sub-stations without train stops")
    p_prune.add_argument("--raw-dir", default=str(REPO_ROOT / "data" / "raw"))
    p_prune.add_argument("--min-stops", type=int, default=1,
                         help="train stops (buses not counted) a sub-station needs to stay")
    p_prune.add_argument("--dry-run", action="store_true", help="print only, leave stations.csv")
    p_prune.set_defaults(func=prune_meta)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        return args.func(args)
    except KeyError as exc:
        log.error("missing environment variable %s", exc)
        return 3
    except PermissionError as exc:
        log.error("%s", exc)
        return 3


if __name__ == "__main__":
    sys.exit(main())
