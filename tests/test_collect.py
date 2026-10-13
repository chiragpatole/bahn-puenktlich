import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import requests

from collector import collect as c

STATIONS = [{"eva": "8011160", "name": "Berlin Hbf"}, {"eva": "8012666", "name": "Potsdam Hbf"}]


class FakeResponse:
    def __init__(self, status_code=200, text="<timetable/>"):
        self.status_code = status_code
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    """Returns queued responses in order, then 200s. Exceptions in the queue are raised."""

    def __init__(self, queue=None):
        self.queue = list(queue or [])
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        item = self.queue.pop(0) if self.queue else FakeResponse()
        if isinstance(item, Exception):
            raise item
        return item

    def put(self, url, **kwargs):
        self.calls.append(("PUT", url, kwargs))
        return FakeResponse(204, "")


def make_client(session):
    return c.TimetablesClient("id", "key", session=session, min_interval=0, sleep=lambda s: None)


def utc(*args):
    return datetime(*args, tzinfo=UTC)


def test_plan_slices_use_german_local_time():
    # 21:30 UTC in October is 23:30 in Berlin, so the third slice is tomorrow
    assert c.plan_slices(utc(2026, 10, 10, 21, 30), 2) == [
        ("261010", "23"), ("261011", "00"), ("261011", "01"),
    ]


def test_plan_slices_when_clocks_go_back():
    # 25 Oct 2026: 02:00-03:00 happens twice. 00:10 UTC is the first 02:10 (CEST),
    # 01:10 UTC is the second 02:10 (CET). The hour must be requested once.
    assert c.plan_slices(utc(2026, 10, 25, 0, 10), 2) == [("261025", "02"), ("261025", "03")]


def test_plan_slices_when_clocks_go_forward():
    # 29 Mar 2026: 02:00-03:00 does not exist
    assert c.plan_slices(utc(2026, 3, 29, 0, 10), 2) == [
        ("260329", "01"), ("260329", "03"), ("260329", "04"),
    ]


def test_collect_makes_one_fchg_and_one_plan_per_slice():
    session = FakeSession()
    records = c.collect(STATIONS, make_client(session), utc(2026, 10, 10, 12, 0), 2, "run1")

    assert len(records) == 2 * 4
    paths = [url.split("/v1/")[1] for _, url, _ in session.calls]
    assert paths[:4] == [
        "fchg/8011160", "plan/8011160/261010/14", "plan/8011160/261010/15", "plan/8011160/261010/16",
    ]
    plan = records[1]
    assert (plan["api_name"], plan["eva"], plan["plan_date"], plan["plan_hour"]) == (
        "plan", "8011160", "261010", "14")
    assert session.calls[0][2]["headers"]["DB-Client-Id"] == "id"


def test_retries_on_429_then_succeeds():
    session = FakeSession([FakeResponse(429, "slow down"), FakeResponse(200, "<timetable/>")])
    rec = make_client(session).get("fchg/8011160")
    assert rec["status_code"] == 200
    assert rec["attempts"] == 2
    assert rec["error"] is None


def test_network_error_is_recorded_not_raised():
    session = FakeSession([requests.ConnectionError("boom")] * 3)
    rec = make_client(session).get("fchg/8011160")
    assert rec["status_code"] is None
    assert rec["attempts"] == 3
    assert "ConnectionError" in rec["error"]
    assert rec["body"] is None


def test_client_error_is_not_retried():
    session = FakeSession([FakeResponse(404, "")])
    rec = make_client(session).get("plan/8011160/261010/03")
    assert rec["attempts"] == 1
    assert rec["error"] == "HTTP 404"


def test_rejected_credentials_stop_the_run():
    session = FakeSession([FakeResponse(401, "")])
    with pytest.raises(PermissionError):
        c.collect(STATIONS, make_client(session), utc(2026, 10, 10, 12, 0), 2, "run1")
    assert len(session.calls) == 1


def test_empty_plan_slice_is_not_a_failure():
    assert not c.is_failure({"api_name": "plan", "status_code": 404})
    assert c.is_failure({"api_name": "fchg", "status_code": 404})
    assert c.is_failure({"api_name": "plan", "status_code": 500})
    assert c.is_failure({"api_name": "plan", "status_code": None})


def test_bundle_is_partitioned_by_utc_day_and_round_trips(tmp_path):
    relpath = c.bundle_relpath(utc(2026, 10, 10, 23, 7, 5), "abcdef1234567890")
    assert relpath == "timetables/year=2026/month=10/day=10/timetables_20261010T230705Z_abcdef12.jsonl.gz"

    records = [{"eva": "8011160", "body": "<timetable station='Berlin Hbf'>ä</timetable>"}]
    path = c.write_bundle(records, tmp_path, relpath)
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        assert [json.loads(line) for line in fh] == records
    assert not list(tmp_path.rglob("*.tmp"))


def test_volume_upload_creates_directory_then_file(tmp_path):
    local = tmp_path / "f.jsonl.gz"
    local.write_bytes(b"x")
    session = FakeSession()
    c.upload_to_volume(local, "timetables/year=2026/f.jsonl.gz", "https://example.cloud.databricks.com/",
                       "tok", "/Volumes/bahn/bronze/landing", session=session)
    urls = [url for _, url, _ in session.calls]
    assert urls == [
        "https://example.cloud.databricks.com/api/2.0/fs/directories/Volumes/bahn/bronze/landing/timetables/year=2026",
        "https://example.cloud.databricks.com/api/2.0/fs/files/Volumes/bahn/bronze/landing/timetables/year=2026/f.jsonl.gz",
    ]
    assert session.calls[1][2]["headers"]["Authorization"] == "Bearer tok"


def test_station_list_has_no_duplicates_and_valid_evas():
    stations = c.load_stations(Path(c.REPO_ROOT) / "collector" / "stations.csv")
    assert len(stations) >= 20
    assert all(s["eva"].isdigit() and len(s["eva"]) == 7 for s in stations)
    assert all(s["name"] for s in stations)


def test_duplicate_eva_is_rejected(tmp_path):
    f = tmp_path / "s.csv"
    f.write_text("eva,name,group\n8011160,Berlin Hbf,a\n8011160,Berlin Hbf,b\n", encoding="utf-8")
    with pytest.raises(ValueError):
        c.load_stations(f)


def test_empty_volume_path_variable_falls_back_to_default(tmp_path, monkeypatch):
    # GitHub Actions hands over unset repository variables as "", not as missing
    uploads = []
    monkeypatch.setattr(c, "upload_to_volume", lambda *a, **k: uploads.append(a))
    monkeypatch.setattr(c.requests, "Session", lambda: FakeSession())
    for key, value in {
        "DB_CLIENT_ID": "id", "DB_API_KEY": "key", "DB_API_BASE_URL": "",
        "DATABRICKS_HOST": "https://example.cloud.databricks.com", "DATABRICKS_TOKEN": "tok",
        "DATABRICKS_VOLUME_PATH": "", "AZURE_STORAGE_ACCOUNT": "",
    }.items():
        monkeypatch.setenv(key, value)

    code = c.main(["run", "--out-dir", str(tmp_path), "--min-interval", "0"])

    assert code == 0
    assert uploads[0][4] == "/Volumes/bahn/bronze/landing"


def test_check_stations_stops_on_rejected_credentials(monkeypatch, capsys):
    monkeypatch.setattr(c.requests, "Session", lambda: FakeSession([FakeResponse(401, "")]))
    monkeypatch.setenv("DB_CLIENT_ID", "id")
    monkeypatch.setenv("DB_API_KEY", "key")
    monkeypatch.setenv("DB_API_BASE_URL", "")
    assert c.main(["check-stations"]) == 3
    assert capsys.readouterr().out == ""


def test_check_stations_shows_the_http_error(monkeypatch, capsys):
    monkeypatch.setattr(c.requests, "Session", lambda: FakeSession([FakeResponse(404, "")] * 30))
    monkeypatch.setattr(c.time, "sleep", lambda s: None)
    monkeypatch.setenv("DB_CLIENT_ID", "id")
    monkeypatch.setenv("DB_API_KEY", "key")
    monkeypatch.setenv("DB_API_BASE_URL", "")
    assert c.main(["check-stations"]) == 1
    assert "(HTTP 404)" in capsys.readouterr().out.splitlines()[0]
