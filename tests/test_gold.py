"""Spark tests for the gold logic: punctuality rules, roll-up to stations, source choice.

All test times carry tzinfo=UTC. A naive datetime would be read in the local zone
of whichever machine runs the tests.
"""

import shutil
from datetime import UTC, date, datetime

import pytest

pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Spark needs Java, which is not installed here", allow_module_level=True)
from pyspark.sql import functions as F  # noqa: E402

from transform.gold import (  # noqa: E402
    STATION_SCHEMA,
    berlin_date,
    berlin_hour,
    collection_health,
    measured,
    punctuality,
    station_rows,
    stop_events,
)

TRAIN_STOPS = (
    "stop_id string, eva string, source string, category string, train_number string, line string, "
    "planned_arrival timestamp, arr_cancelled boolean, arrival_delay_min int, "
    "planned_departure timestamp, dep_cancelled boolean, departure_delay_min int"
)


def now():
    # "now" for the tests: 16 Oct 2026, 14:00 in Berlin
    return F.lit(datetime(2026, 10, 16, 12, 0, tzinfo=UTC)).cast("timestamp")


def stations(spark, tmp_path):
    f = tmp_path / "stations.csv"
    f.write_text(
        "eva,name,group,parent_eva,poll\n"
        "8011160,Berlin Hbf,berlin,\n"
        "8089021,Berlin Hbf (S-Bahn),berlin,8011160\n"
        "8012666,Potsdam Hbf,berlin,\n"
        "8011155,Berlin Alexanderplatz,berlin,,no\n",
        encoding="utf-8",
    )
    return spark.createDataFrame(station_rows(f), STATION_SCHEMA)


def stop(stop_id, eva, source="collector", category="RE", line="1", arr=None, arr_c=False, arr_d=0,
         dep=None, dep_c=False, dep_d=0):
    return (stop_id, eva, source, category, "4711", line, arr, arr_c, arr_d, dep, dep_c, dep_d)


T = datetime(2026, 10, 15, 8, 0, tzinfo=UTC)  # 10:00 in Berlin


def test_station_parts_roll_up_to_their_station(spark, tmp_path):
    rows = {r.eva: r for r in stations(spark, tmp_path).collect()}
    assert rows["8089021"].station_eva == "8011160"
    assert rows["8089021"].station_name == "Berlin Hbf"
    assert rows["8011160"].station_eva == "8011160"
    assert rows["8011160"].parent_eva is None


@pytest.mark.parametrize("session_tz", ["UTC", "Europe/Berlin", "America/New_York"])
def test_day_and_hour_are_german_whatever_the_session_zone(spark, session_tz):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    df = spark.createDataFrame(
        [(datetime(2026, 10, 15, 22, 30, tzinfo=UTC),), (datetime(2026, 1, 15, 22, 30, tzinfo=UTC),),
         (datetime(2026, 10, 25, 0, 30, tzinfo=UTC),)],
        "t timestamp",
    )
    spark.conf.set("spark.sql.session.timeZone", session_tz)
    try:
        out = df.select(berlin_date("t").alias("d"), berlin_hour("t").alias("h")).collect()
        got = [(r.d, r.h) for r in out]
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    # 22:30 UTC is 00:30 next day in summer, 23:30 same day in winter;
    # 00:30 UTC on 25 Oct is the first 02:30 of the night the clocks go back
    assert got == [(date(2026, 10, 16), 0), (date(2026, 1, 15), 23), (date(2026, 10, 25), 2)]


def test_stop_measured_at_arrival_or_departure_at_first_stop(spark, tmp_path):
    ts = spark.createDataFrame([
        stop("a", "8011160", arr=T, arr_d=7, dep=T, dep_d=2),     # late arrival, early-ish departure
        stop("b", "8011160", dep=T, dep_d=3),                     # first stop: departure counts
        stop("c", "8011160", arr=T, arr_c=True, dep=T),           # arrival cancelled
        stop("d", "8011160", arr=T, arr_d=5),                     # 5 min late is still punctual
    ], TRAIN_STOPS)
    out = {r.stop_id: r for r in stop_events(ts, stations(spark, tmp_path), now=now()).collect()}
    assert (out["a"].delay_min, out["a"].punctual) == (7, False)
    assert (out["b"].delay_min, out["b"].punctual) == (3, True)
    assert (out["c"].cancelled, out["c"].punctual) == (True, None)
    assert out["d"].punctual is True
    assert out["a"].hour_local == 10
    assert (out["a"].is_origin, out["b"].is_origin) == (False, True)


RECENT = datetime(2026, 10, 16, 11, 30, tzinfo=UTC)  # 30 min before "now"


def test_buses_untracked_stations_and_unsettled_stops_are_left_out(spark, tmp_path):
    ts = spark.createDataFrame([
        stop("bus", "8011160", category="Bus", arr=T),
        stop("elsewhere", "8000105", arr=T),
        stop("too_recent", "8011160", arr=RECENT),
        stop("future_archive_ok", "8011160", source="archive", arr=RECENT),
        stop("kept", "8089021", category="S", line="7", arr=T),
    ], TRAIN_STOPS)
    out = {r.stop_id: r for r in stop_events(ts, stations(spark, tmp_path), now=now()).collect()}
    assert set(out) == {"future_archive_ok", "kept"}
    assert (out["kept"].station_eva, out["kept"].product_class, out["kept"].line_label) == (
        "8011160", "s_bahn", "S7")


def test_one_source_per_station_and_day(spark, tmp_path):
    ts = spark.createDataFrame([
        stop("x", "8011160", source="collector", arr=T),
        stop("x", "8011160", source="archive", arr=T),            # same train, other source: dropped
        stop("y", "8012666", source="archive", arr=T),            # collector missed Potsdam that day
    ], TRAIN_STOPS)
    rows = stop_events(ts, stations(spark, tmp_path), now=now()).collect()
    out = sorted((r.stop_id, r.source) for r in rows)
    assert out == [("x", "collector"), ("y", "archive")]


def test_both_punctuality_figures(spark, tmp_path):
    ts = spark.createDataFrame([
        stop("1", "8011160", arr=T, arr_d=0),
        stop("2", "8011160", arr=T, arr_d=12),
        stop("3", "8011160", arr=T, arr_d=20),
        stop("4", "8011160", arr=T, arr_c=True),
    ], TRAIN_STOPS)
    ev = stop_events(ts, stations(spark, tmp_path), now=now())
    r = punctuality(ev, "station_eva").collect()[0]
    assert (r.stops, r.cancelled, r.punctual, r.late_15_plus) == (4, 1, 1, 1)
    assert r.punctuality_db == round(1 / 3, 4)
    assert r.punctuality_incl_cancelled == 0.25
    assert r.avg_delay_min == round(32 / 3, 2)


def test_health_shows_station_numbers_without_stops(spark, tmp_path):
    ts = spark.createDataFrame([
        stop("1", "8011160", arr=T),
        stop("2", "8089021", arr=T),
        stop("today", "8011160", arr=datetime(2026, 10, 16, 8, 0, tzinfo=UTC)),
    ], TRAIN_STOPS)
    st = stations(spark, tmp_path)
    health = collection_health(stop_events(ts, st, now=now()), st, now=now())
    got = sorted((r.eva, str(r.service_date), r.stops) for r in health.collect())
    # today is not finished, so only 15 Oct is checked; Potsdam had nothing;
    # Alexanderplatz is not polled, so it is not expected to have stops
    assert got == [("8011160", "2026-10-15", 1), ("8012666", "2026-10-15", 0), ("8089021", "2026-10-15", 1)]


def test_flixtrain_is_not_counted_as_db_long_distance(spark, tmp_path):
    ts = spark.createDataFrame([stop("f", "8011160", category="FLX", line=None, arr=T),
                                stop("i", "8011160", category="ICE", line=None, arr=T)], TRAIN_STOPS)
    out = {r.stop_id: r for r in stop_events(ts, stations(spark, tmp_path), now=now()).collect()}
    assert (out["f"].product_class, out["i"].product_class) == ("other_operator", "long_distance")
    assert out["i"].line_label == "ICE"


def test_partial_days_and_first_stops_are_not_measured(spark, tmp_path):
    day = lambda d, h=8: datetime(2026, 10, d, h, 0, tzinfo=UTC)  # noqa: E731
    rows = []
    for d in (12, 13, 14):                                        # normal days: 4 stops
        rows += [stop(f"{d}-{i}", "8011160", arr=day(d)) for i in range(4)]
    rows.append(stop("origin", "8011160", dep=day(13)))           # train starts here
    rows.append(stop("thin", "8011160", arr=day(15)))             # only 1 stop: partial day
    rows += [stop(f"today-{i}", "8011160", arr=day(16, 6)) for i in range(4)]  # not finished
    ts = spark.createDataFrame(rows, TRAIN_STOPS)
    ev = stop_events(ts, stations(spark, tmp_path), now=now())

    days = {str(r.service_date): r.complete_day for r in ev.select("service_date", "complete_day").collect()}
    assert days == {"2026-10-12": True, "2026-10-13": True, "2026-10-14": True,
                    "2026-10-15": False, "2026-10-16": False}
    got = measured(ev)
    assert got.count() == 12
    assert got.where("stop_id = 'origin'").count() == 0
