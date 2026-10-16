from transform.iris import STOP_FIELDS, parse_timetable

PLAN = """<?xml version='1.0' encoding='UTF-8'?>
<timetable station='Berlin Hbf'>
  <s id="-5414170453134686351-2610131310-7">
    <tl f="F" t="p" o="80" c="ICE" n="1007"/>
    <ar pt="2610131402" pp="13" ppth="Hamburg Hbf|Berlin-Spandau"/>
    <dp pt="2610131407" pp="13" ppth="Berlin Südkreuz|Halle(Saale)Hbf"/>
  </s>
  <s id="7330104915622134961-2610131350-3">
    <tl f="N" t="p" o="800157" c="RE" n="73788"/>
    <ar pt="2610131412" pp="2" l="1"/>
    <dp pt="2610131413" pp="2" l="1"/>
  </s>
  <s id="1180334571021718101-2610131405-1">
    <tl f="N" t="p" o="800157" c="RB" n="18601"/>
    <dp pt="2610131405" pp="4" l="23"/>
  </s>
</timetable>"""

CHANGES = """<?xml version='1.0' encoding='UTF-8'?>
<timetable station="Berlin Hbf" eva="8011160">
  <s id="-5414170453134686351-2610131310-7" eva="8011160">
    <m id="r1" t="d" c="43" ts="2610131350"/>
    <ar ct="2610131419" cp="14"/>
    <dp ct="2610131424" cp="14"/>
  </s>
  <s id="7330104915622134961-2610131350-3" eva="8011160">
    <ar cs="c" clt="2610131330"/>
    <dp cs="c" clt="2610131330"/>
  </s>
  <s id="999-2610131200-2" eva="8011160">
    <m id="r9" t="h" c="0" ts="2610131100"/>
  </s>
</timetable>"""


def by_id(stops):
    return {s["stop_id"]: s for s in stops}


def test_plan_gives_train_and_planned_times():
    stops = by_id(parse_timetable(PLAN, eva="8011160"))
    assert len(stops) == 3

    ice = stops["-5414170453134686351-2610131310-7"]
    assert ice["eva"] == "8011160"
    assert ice["station_name"] == "Berlin Hbf"
    assert (ice["category"], ice["train_number"], ice["line"]) == ("ICE", "1007", None)
    assert (ice["arr_planned"], ice["dep_planned"]) == ("2610131402", "2610131407")
    assert ice["arr_platform"] == "13"
    # plan says nothing about changes, so these must stay unknown
    assert ice["arr_changed"] is None
    assert ice["arr_cancelled"] is None


def test_regional_line_and_first_stop_without_arrival():
    stops = by_id(parse_timetable(PLAN, eva="8011160"))
    assert stops["7330104915622134961-2610131350-3"]["line"] == "1"
    rb = stops["1180334571021718101-2610131405-1"]
    assert rb["arr_planned"] is None
    assert rb["dep_planned"] == "2610131405"
    assert rb["line"] == "23"


def test_changes_carry_only_what_changed():
    stops = by_id(parse_timetable(CHANGES))
    ice = stops["-5414170453134686351-2610131310-7"]
    assert (ice["arr_changed"], ice["dep_changed"]) == ("2610131419", "2610131424")
    assert ice["arr_platform_changed"] == "14"
    # the change feed has no planned times and no train info for this stop
    assert ice["arr_planned"] is None
    assert ice["category"] is None
    assert ice["arr_cancelled"] is None


def test_cancellation():
    stops = by_id(parse_timetable(CHANGES))
    re = stops["7330104915622134961-2610131350-3"]
    assert re["arr_cancelled"] is True
    assert re["dep_cancelled"] is True


def test_revoked_cancellation_is_false_not_none():
    body = '<timetable station="X"><s id="a-1-1"><dp cs="p"/></s></timetable>'
    assert parse_timetable(body, eva="1")[0]["dep_cancelled"] is False


def test_message_only_stops_are_skipped():
    assert "999-2610131200-2" not in by_id(parse_timetable(CHANGES))


def test_every_record_has_all_fields():
    for stop in parse_timetable(PLAN, eva="8011160") + parse_timetable(CHANGES):
        assert tuple(stop) == STOP_FIELDS


def test_empty_hour_is_no_rows_but_broken_xml_is_none():
    assert parse_timetable("<timetable station='Berlin Hbf'/>", eva="8011160") == []
    assert parse_timetable("<timetable><s id=") is None
    assert parse_timetable("") is None
    assert parse_timetable(None) is None
    assert parse_timetable("<html>rate limited</html>") is None
