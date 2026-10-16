"""Spark tests for time zones and delays. Skipped where pyspark is not installed.

Times are compared as epoch seconds: collect() turns timestamps into the local
time of whatever machine runs the test, epoch seconds mean the same everywhere.
"""

import shutil
from datetime import UTC, datetime

import pytest

pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Spark needs Java, which is not installed here", allow_module_level=True)
from pyspark.sql import functions as F  # noqa: E402

from transform.columns import (  # noqa: E402
    berlin_to_utc,
    iris_time_to_utc,
    normalize_eva,
    with_actuals_and_delays,
)


def utc(*args):
    return int(datetime(*args, tzinfo=UTC).timestamp())


def epochs(df, column):
    return [r[0] for r in df.select(F.unix_timestamp(column)).collect()]


@pytest.mark.parametrize("session_tz", ["UTC", "Europe/Berlin", "Asia/Kolkata", "America/New_York"])
def test_iris_times_convert_to_utc_regardless_of_session_zone(spark, session_tz):
    spark.conf.set("spark.sql.session.timeZone", session_tz)
    try:
        df = spark.createDataFrame([("2610131402",), ("2601151402",), (None,)], "t string")
        got = epochs(df.select(iris_time_to_utc("t").alias("x")), "x")
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    # 13 Oct is summer time (UTC+2), 15 Jan is winter time (UTC+1)
    assert got == [utc(2026, 10, 13, 12, 2), utc(2026, 1, 15, 13, 2), None]


def test_archive_ntz_times_convert_to_utc(spark):
    df = spark.createDataFrame([(datetime(2026, 7, 1, 8, 15),)], "t timestamp_ntz")
    assert epochs(df.select(berlin_to_utc("t").alias("x")), "x") == [utc(2026, 7, 1, 6, 15)]


def test_night_clocks_go_back_still_converts(spark):
    df = spark.createDataFrame([("2610250230",), ("2610250330",)], "t string")
    got = epochs(df.select(iris_time_to_utc("t").alias("x")), "x")
    # 02:30 happens twice that night; either offset is acceptable, but not null
    assert got[0] in (utc(2026, 10, 25, 0, 30), utc(2026, 10, 25, 1, 30))
    assert got[1] == utc(2026, 10, 25, 2, 30)


def test_delays_cancellations_and_on_time(spark):
    t = datetime
    rows = [
        # late arrival, departure as planned (no changed time)
        (t(2026, 10, 13, 12, 2), t(2026, 10, 13, 12, 19), None, t(2026, 10, 13, 12, 7), None, None),
        # cancelled both ways
        (t(2026, 10, 13, 12, 12), None, True, t(2026, 10, 13, 12, 13), None, True),
        # first stop without arrival, leaves a minute early
        (None, None, None, t(2026, 10, 13, 12, 5), t(2026, 10, 13, 12, 4), False),
    ]
    schema = ("planned_arrival timestamp, changed_arrival timestamp, arr_cancelled boolean, "
              "planned_departure timestamp, changed_departure timestamp, dep_cancelled boolean")
    out = with_actuals_and_delays(spark.createDataFrame(rows, schema)).collect()

    assert (out[0].arrival_delay_min, out[0].departure_delay_min) == (17, 0)
    assert out[0].arr_cancelled is False
    assert out[1].arr_cancelled is True and out[1].actual_arrival is None
    assert out[1].arrival_delay_min is None
    assert out[2].arrival_delay_min is None
    assert out[2].departure_delay_min == -1


def test_station_numbers_lose_leading_zeros(spark):
    df = spark.createDataFrame([("08012666",), ("8012666",), (" 08011160 ",), (None,)], "eva string")
    got = [r[0] for r in df.select(normalize_eva("eva")).collect()]
    assert got == ["8012666", "8012666", "8011160", None]
