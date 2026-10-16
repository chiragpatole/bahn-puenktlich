"""Silver layer: one row per train stop, times in UTC.

collector_stop_updates  every stop record parsed from every API response
collector_parse_errors  responses that came back 200 but were not valid XML
collector_stops         latest known state per stop (AUTO CDC merge of the above)
archive_stops           the open archive, renamed to the same columns, times in UTC
train_stops             both sources together, with actual times and delays

Why AUTO CDC: the API reports a stop in pieces. The hourly plan gives the
planned times; the change feed later adds delays, platform changes and
cancellations, often several times as the delay grows. Merging with
ignore_null_updates means each piece fills in its own columns and the latest
change wins, without the change feed erasing planned times it never sent.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T

import transform.iris as iris
from transform.columns import berlin_to_utc, iris_time_to_utc, with_actuals_and_delays

CATALOG = spark.conf.get("bahn.catalog", "bahn")
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"

# The parser UDF runs on workers, which may not have the repo on their import path.
# Shipping transform.iris inside the pickled function avoids "No module named transform".
try:
    from pyspark import cloudpickle

    cloudpickle.register_pickle_by_value(iris)
except (ImportError, AttributeError):
    pass

STOP_SCHEMA = T.ArrayType(T.StructType([
    T.StructField(name, T.BooleanType() if name.endswith("_cancelled") else T.StringType())
    for name in iris.STOP_FIELDS
]))

parse_stops = F.udf(iris.parse_timetable, STOP_SCHEMA)

TIME_COLUMNS = {
    "arr_planned": "planned_arrival",
    "arr_changed": "changed_arrival",
    "dep_planned": "planned_departure",
    "dep_changed": "changed_departure",
}


def parsed_responses():
    return (
        spark.readStream.table(f"{BRONZE}.timetables_raw")
        .where("status_code = 200 AND api_name IN ('plan', 'fchg')")
        .select(
            "run_id", "api_name", "eva", "_source_file",
            F.to_timestamp("fetched_at").alias("fetched_at"),
            "body",
            parse_stops("body", "eva").alias("stops"),
        )
    )


@dp.table(
    name=f"{SILVER}.collector_parse_errors",
    comment="API responses with HTTP 200 whose body was not a valid timetable",
)
def collector_parse_errors():
    return parsed_responses().where("stops IS NULL").drop("stops")


@dp.table(
    name=f"{SILVER}.collector_stop_updates",
    comment="One row per stop per API response, before merging",
)
@dp.expect_or_drop("has_keys", "stop_id IS NOT NULL AND eva IS NOT NULL")
@dp.expect("time_parses", "arr_planned IS NULL OR planned_arrival IS NOT NULL")
def collector_stop_updates():
    stops = (
        parsed_responses()
        .where("stops IS NOT NULL")
        .select("run_id", "api_name", "fetched_at", "_source_file", F.explode("stops").alias("s"))
        .select("run_id", "api_name", "fetched_at", "_source_file", "s.*")
    )
    for raw, clean in TIME_COLUMNS.items():
        stops = stops.withColumn(clean, iris_time_to_utc(raw))
    return stops


dp.create_streaming_table(
    name=f"{SILVER}.collector_stops",
    comment="Latest known state of each stop seen by the collector",
)

dp.create_auto_cdc_flow(
    target=f"{SILVER}.collector_stops",
    source=f"{SILVER}.collector_stop_updates",
    keys=["eva", "stop_id"],
    sequence_by=F.struct("fetched_at", "api_name"),
    ignore_null_updates=True,
    except_column_list=["run_id", "_source_file", *TIME_COLUMNS],
    stored_as_scd_type=1,
)


@dp.table(
    name=f"{SILVER}.archive_stops",
    comment="Open archive stops (piebro/deutsche-bahn-data, CC BY 4.0) with times in UTC",
)
@dp.expect_or_drop("has_keys", "stop_id IS NOT NULL AND eva IS NOT NULL")
@dp.expect("has_planned_time", "planned_arrival IS NOT NULL OR planned_departure IS NOT NULL")
def archive_stops():
    a = spark.readStream.table(f"{BRONZE}.archive_stops")
    return a.select(
        F.col("id").alias("stop_id"),
        F.col("eva").cast("string").alias("eva"),
        "station_name",
        F.col("train_type").alias("category"),
        F.col("train_number").cast("string").alias("train_number"),
        F.col("line_number").cast("string").alias("line"),
        berlin_to_utc("arrival_planned_time").alias("planned_arrival"),
        berlin_to_utc("arrival_change_time").alias("changed_arrival"),
        F.col("arrival_is_canceled").alias("arr_cancelled"),
        berlin_to_utc("departure_planned_time").alias("planned_departure"),
        berlin_to_utc("departure_change_time").alias("changed_departure"),
        F.col("departure_is_canceled").alias("dep_cancelled"),
        "_source_file",
    )


UNIFIED = [
    "stop_id", "eva", "station_name", "category", "train_number", "line",
    "planned_arrival", "changed_arrival", "arr_cancelled",
    "planned_departure", "changed_departure", "dep_cancelled",
]


@dp.materialized_view(
    name=f"{SILVER}.train_stops",
    comment="Every train stop from both sources, with actual times and delays in minutes",
)
@dp.expect("delay_plausible", "arrival_delay_min IS NULL OR arrival_delay_min BETWEEN -30 AND 1440")
def train_stops():
    collector = (
        spark.read.table(f"{SILVER}.collector_stops")
        # stops only seen in the change feed belong to hours we never fetched a plan for
        .where("planned_arrival IS NOT NULL OR planned_departure IS NOT NULL")
        .select(*UNIFIED)
        .withColumn("source", F.lit("collector"))
    )
    archive = (
        spark.read.table(f"{SILVER}.archive_stops")
        .select(*UNIFIED)
        .withColumn("source", F.lit("archive"))
    )
    return with_actuals_and_delays(archive.unionByName(collector))
