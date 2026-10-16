"""Parse responses from DB's Timetables API (the IRIS system) into flat stop records.

Two kinds of response come in:

plan  The scheduled stops at one station for one hour. Each <s> has the train
      (<tl>) and planned arrival/departure (<ar>/<dp> with pt = planned time).
fchg  All known changes for a station. Each <s> repeats the stop id and carries
      only what changed: ct (changed time), cs="c" (cancelled), cp (platform).

Both are turned into the same record shape. A field the response does not
mention stays None, so silver can merge plan and change records with
"ignore null updates": a change record never wipes out a planned time.

Times stay as the API's yyMMddHHmm strings in German local time; the pipeline
converts them to UTC.
"""

import xml.etree.ElementTree as ET

STOP_FIELDS = (
    "stop_id", "eva", "station_name", "category", "train_number", "line",
    "arr_planned", "arr_changed", "arr_cancelled", "arr_platform", "arr_platform_changed",
    "dep_planned", "dep_changed", "dep_cancelled", "dep_platform", "dep_platform_changed",
)


def _event(node):
    """Fields from an <ar> or <dp> element. Missing element -> all None."""
    if node is None:
        return None, None, None, None, None
    status = node.get("cs")
    # cs: p = planned, a = added, c = cancelled. Absent means "no news", not "not cancelled".
    cancelled = None if status is None else status == "c"
    return node.get("pt"), node.get("ct"), cancelled, node.get("pp"), node.get("cp")


def parse_timetable(body, eva=None):
    """Return a list of stop dicts, or None if the body is not valid XML.

    None (rather than []) lets the pipeline send broken responses to quarantine,
    while an empty timetable (no trains that hour) is simply no rows.
    """
    if not body:
        return None
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return None
    if root.tag != "timetable":
        return None

    station = root.get("station")
    station_eva = root.get("eva") or eva
    stops = []
    for s in root.findall("s"):
        stop_id = s.get("id")
        if not stop_id:
            continue
        ar, dp = s.find("ar"), s.find("dp")
        if ar is None and dp is None:
            # fchg often lists stops that only have messages (<m>), nothing to merge
            continue
        tl = s.find("tl")
        line = None
        for node in (dp, ar):
            if node is not None and node.get("l"):
                line = node.get("l")
                break
        arr = _event(ar)
        dep = _event(dp)
        stops.append(dict(zip(STOP_FIELDS, (
            stop_id,
            s.get("eva") or station_eva,
            station,
            tl.get("c") if tl is not None else None,
            tl.get("n") if tl is not None else None,
            line,
            *arr,
            *dep,
        ), strict=True)))
    return stops
