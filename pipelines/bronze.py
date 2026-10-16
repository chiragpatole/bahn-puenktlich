"""Bronze layer: the raw files as Delta tables, unchanged.

timetables_raw  one row per API call from the hourly collector, XML still in `body`
archive_stops   one row per train stop from the open piebro/deutsche-bahn-data archive

Nothing is parsed or filtered here. If silver has a bug, it can be rebuilt from
these tables without calling the API again.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

LANDING_PATH = spark.conf.get("bahn.landing_path", "/Volumes/bahn/bronze/landing/timetables")
ARCHIVE_PATH = spark.conf.get("bahn.archive_path", "/Volumes/bahn/bronze/archive/monthly")

# Matches what collector/collect.py writes. Declared rather than inferred, so a
# run where every call failed (body always null) cannot change the column types.
COLLECTOR_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("api_name", StringType()),
    StructField("eva", StringType()),
    StructField("station_name", StringType()),
    StructField("plan_date", StringType()),
    StructField("plan_hour", StringType()),
    StructField("url", StringType()),
    StructField("fetched_at", StringType()),
    StructField("status_code", IntegerType()),
    StructField("duration_ms", LongType()),
    StructField("attempts", IntegerType()),
    StructField("error", StringType()),
    StructField("body", StringType()),
])


def with_file_info(df):
    return df.select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )


@dp.table(name="timetables_raw", comment="Raw Timetables API responses from the hourly collector")
@dp.expect("successful_call_has_body", "status_code <> 200 OR body IS NOT NULL")
def timetables_raw():
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(COLLECTOR_SCHEMA)
        .load(LANDING_PATH)
    )
    return with_file_info(stream)


# The archive's times carry no time zone (German local time), so Spark reads them
# as TIMESTAMP_NTZ. Bronze keeps them that way; silver converts to UTC.
@dp.table(
    name="archive_stops",
    comment="Train stops from the piebro/deutsche-bahn-data archive (CC BY 4.0)",
    table_properties={"delta.feature.timestampNtz": "supported"},
)
@dp.expect("has_station", "eva IS NOT NULL")
def archive_stops():
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(ARCHIVE_PATH)
    )
    return with_file_info(stream)
