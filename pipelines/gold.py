"""Gold layer: punctuality per station, hour and line, in DB's definition and with cancellations.

dim_station           every polled station number, mapped to the station it belongs to
stop_events           one row per stop, with punctual / cancelled / is_origin / complete_day flags
punctuality_daily     per day, station and product class (S-Bahn, regional, long distance, other operator)
punctuality_by_hour   per station, class and hour of the day, over all data
punctuality_by_line   per month and line
collection_health     collector stops per station number and day, zeros included

The logic lives in transform/gold.py and is tested there.
"""

from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transform.gold import (
    STATION_SCHEMA,
    collection_health,
    measured,
    punctuality,
    station_rows,
    stop_events,
)

CATALOG = spark.conf.get("bahn.catalog", "bahn")
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"


def stations_path():
    # stations.csv is the list the collector polls, so gold reads the same file
    # from the Git folder instead of keeping a second copy.
    configured = spark.conf.get("bahn.stations_path", "")
    if configured:
        return configured
    import collector

    return str(Path(collector.__path__[0]) / "stations.csv")


@dp.materialized_view(
    name=f"{GOLD}.dim_station",
    comment="Polled station numbers and the station each belongs to",
)
@dp.expect_or_fail("has_eva", "eva IS NOT NULL AND station_eva IS NOT NULL")
def dim_station():
    return spark.createDataFrame(station_rows(stations_path()), STATION_SCHEMA)


@dp.materialized_view(
    name=f"{GOLD}.stop_events",
    comment="One row per train stop at a tracked station, measured at arrival (departure at the first stop)",
)
@dp.expect("delay_known_when_ran", "cancelled OR delay_min IS NOT NULL")
def stop_events_view():
    return stop_events(spark.read.table(f"{SILVER}.train_stops"), spark.read.table(f"{GOLD}.dim_station"))


def events():
    return spark.read.table(f"{GOLD}.stop_events")


def measured_events():
    # finished, fully covered days; first stops left out, as in DB's figure
    return measured(events())


@dp.materialized_view(
    name=f"{GOLD}.punctuality_daily",
    comment="Punctuality per day, station and product class",
)
def punctuality_daily():
    return punctuality(measured_events(), "service_date", "station_eva", "station_name", "product_class")


@dp.materialized_view(
    name=f"{GOLD}.punctuality_by_hour",
    comment="Punctuality per station, class and hour of day",
)
def punctuality_by_hour():
    return punctuality(measured_events(), "station_eva", "station_name", "product_class", "hour_local")


@dp.materialized_view(
    name=f"{GOLD}.punctuality_by_line",
    comment="Punctuality per month and line",
)
def punctuality_by_line():
    monthly = measured_events().withColumn("month", F.trunc("service_date", "month"))
    return punctuality(monthly, "month", "product_class", "line_label")


@dp.materialized_view(
    name=f"{GOLD}.collection_health",
    comment="Collector stops per polled station number and finished day; 0 means the API returned nothing",
)
@dp.expect("station_has_stops", "stops > 0")
def collection_health_view():
    return collection_health(events(), spark.read.table(f"{GOLD}.dim_station"))
