"""Column logic shared by the pipeline and the tests. Needs pyspark, not Databricks."""

from pyspark.sql import functions as F


def berlin_to_utc(ntz_sql):
    """German local wall-clock time -> UTC instant, whatever the session time zone is.

    make_timestamp with an explicit zone does not depend on spark.sql.session.timeZone,
    unlike to_timestamp or a plain cast. In the repeated hour when clocks go back
    (last Sunday of October) the local time is ambiguous; Spark picks one offset.
    """
    e = ntz_sql
    return F.expr(
        f"make_timestamp(year({e}), month({e}), day({e}), hour({e}), minute({e}), "
        f"second({e}), 'Europe/Berlin')"
    )


def iris_time_to_utc(column):
    """The API's yyMMddHHmm strings (German local time) -> UTC instant."""
    return berlin_to_utc(f"to_timestamp_ntz({column}, 'yyMMddHHmm')")


def with_actuals_and_delays(stops):
    """Add actual times and delays in minutes for arrival and departure.

    A cancelled leg has no actual time and no delay; it is counted separately.
    No changed time means the train ran as planned (delay 0).
    """
    for leg, prefix in (("arrival", "arr"), ("departure", "dep")):
        planned, changed = F.col(f"planned_{leg}"), F.col(f"changed_{leg}")
        cancelled = F.coalesce(F.col(f"{prefix}_cancelled"), F.lit(False))
        actual = F.when(cancelled, None).otherwise(F.coalesce(changed, planned))
        delay = ((F.unix_timestamp(actual) - F.unix_timestamp(planned)) / 60).cast("int")
        stops = (
            stops.withColumn(f"{prefix}_cancelled", cancelled)
            .withColumn(f"actual_{leg}", actual)
            .withColumn(f"{leg}_delay_min", delay)
        )
    return stops
