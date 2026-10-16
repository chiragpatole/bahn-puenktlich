"""Gold logic: stop events with DB's punctuality rule, and the aggregates on top.

DB counts a stop as punctual when the train arrives less than 6 minutes late
(departure at the first stop, which has no arrival). Cancelled stops are left
out of DB's published figure. This project reports both:

punctuality_db               punctual / stops that ran         (DB's way)
punctuality_incl_cancelled   punctual / all planned stops      (a cancelled train is not on time)

Kept free of Databricks imports so the tests can run it on a laptop.
"""

import csv

from pyspark.sql import Window
from pyspark.sql import functions as F

PUNCTUAL_BELOW_MIN = 6

# Train categories as IRIS writes them (<tl c="...">). S-Bahn gets its own class
# because one S-Bahn station has more stops per hour than a long-distance hub.
LONG_DISTANCE = ["ICE", "IC", "EC", "ECE", "EN", "NJ", "RJ", "RJX", "TGV", "ES", "EST", "FLX", "WB", "D"]
ROAD = ["BUS", "SEV", "STR", "TRAM"]

STATION_SCHEMA = (
    "eva string, eva_name string, parent_eva string, "
    "station_eva string, station_name string, station_group string"
)


def station_rows(path):
    """stations.csv -> rows for dim_station. Parts of a big station point to it."""
    with open(path, encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    names = {r["eva"]: r["name"] for r in rows}
    out = []
    for r in rows:
        parent = (r.get("parent_eva") or "").strip() or None
        station = parent or r["eva"]
        station_name = names.get(station, r["name"])
        out.append((r["eva"], r["name"], parent, station, station_name, r.get("group") or None))
    return out


def _berlin_epoch(ts):
    col = F.col(ts) if isinstance(ts, str) else ts
    # from_utc_timestamp shifts the instant by Berlin's offset at that instant;
    # unix_timestamp of the result is then "Berlin wall clock as seconds".
    return F.unix_timestamp(F.from_utc_timestamp(col, "Europe/Berlin"))


def berlin_date(ts):
    """Calendar day in Germany of a UTC instant. Independent of the session time zone."""
    return F.date_from_unix_date(F.floor(_berlin_epoch(ts) / 86400).cast("int"))


def berlin_hour(ts):
    """Hour of day (0-23) in Germany of a UTC instant. Independent of the session time zone."""
    return (F.pmod(_berlin_epoch(ts), F.lit(86400)) / 3600).cast("int")


def product_class(category):
    c = F.upper(F.trim(F.col(category)))
    return (
        F.when(c == "S", "s_bahn")
        .when(c.isin(LONG_DISTANCE), "long_distance")
        .when(c.isin(ROAD), "road")
        .otherwise("regional")
    )


def line_label(category, line):
    """'S' + '7' -> 'S7'; a line that already names itself ('RE1') stays; no line -> category."""
    cat, ln = F.upper(F.trim(F.col(category))), F.trim(F.col(line))
    return (
        F.when(ln.isNull() | (ln == ""), cat)
        .when(ln.rlike("^[0-9]+$"), F.concat(cat, ln))
        .otherwise(ln)
    )


def stop_events(train_stops, stations, now=None, settle_minutes=60):
    """One row per stop at a station we track, measured at arrival (departure at the first stop).

    Rules:
    - road vehicles at bus stops in front of stations are dropped
    - the collector's stops are only counted once they are settle_minutes in the past;
      before that the change feed may still report a delay
    - per station and day only one source counts: the collector if it has that day,
      otherwise the archive. Both sources for the same day would count each train twice.
    """
    now = F.current_timestamp() if now is None else now
    has_arr = F.col("planned_arrival").isNotNull()

    def at_stop(arrival, departure):
        return F.when(has_arr, F.col(arrival)).otherwise(F.col(departure))

    ev = (
        train_stops.select(
            "stop_id", "eva", "source", "category", "train_number", "line",
            at_stop("planned_arrival", "planned_departure").alias("planned_time"),
            at_stop("arr_cancelled", "dep_cancelled").alias("cancelled"),
            at_stop("arrival_delay_min", "departure_delay_min").alias("delay_min"),
        )
        .where("planned_time IS NOT NULL")
        .withColumn("product_class", product_class("category"))
        .where("product_class <> 'road'")
        .where((F.col("source") != "collector")
               | (F.col("planned_time") <= now - F.expr(f"INTERVAL {int(settle_minutes)} MINUTES")))
        .withColumn("service_date", berlin_date("planned_time"))
        .withColumn("hour_local", berlin_hour("planned_time"))
    )

    per_day = Window.partitionBy("eva", "service_date")
    ev = (
        ev.withColumn("_day_has_collector", F.max((F.col("source") == "collector").cast("int")).over(per_day))
        .where("source = 'collector' OR _day_has_collector = 0")
        .drop("_day_has_collector")
    )

    return (
        ev.join(stations.select("eva", "station_eva", "station_name", "station_group"), "eva")
        .withColumn("cancelled", F.coalesce(F.col("cancelled"), F.lit(False)))
        .withColumn("punctual", F.when(~F.col("cancelled"), F.col("delay_min") < PUNCTUAL_BELOW_MIN))
        .withColumn("line_label", line_label("category", "line"))
    )


def punctuality(events, *keys):
    """Stops, cancellations and both punctuality figures per group."""
    ran = F.col("stops") - F.col("cancelled")
    return (
        events.groupBy(*keys)
        .agg(
            F.count("*").alias("stops"),
            F.sum(F.col("cancelled").cast("int")).alias("cancelled"),
            F.sum(F.coalesce(F.col("punctual"), F.lit(False)).cast("int")).alias("punctual"),
            F.sum((F.col("delay_min") >= 15).cast("int")).alias("late_15_plus"),
            F.round(F.avg(F.when(~F.col("cancelled"), F.col("delay_min"))), 2).alias("avg_delay_min"),
        )
        .withColumn("punctuality_db", F.round(F.col("punctual") / F.when(ran > 0, ran), 4))
        .withColumn("punctuality_incl_cancelled", F.round(F.col("punctual") / F.col("stops"), 4))
        .withColumn("cancelled_share", F.round(F.col("cancelled") / F.col("stops"), 4))
    )


def collection_health(events, stations, now=None):
    """Collector stops per polled station number and finished day, including the zeros.

    A station number that answers the API but returns no trains looks like success in
    the collector's run summary. Here it shows up as a row with stops = 0.
    """
    now = F.current_timestamp() if now is None else now
    collected = events.where("source = 'collector'")
    days = (
        collected.select("service_date").distinct()
        .where(F.col("service_date") < berlin_date(now))
    )
    counts = collected.groupBy("eva", "service_date").agg(F.count("*").alias("stops"))
    return (
        stations.select("eva", "eva_name", "station_eva", "station_name").crossJoin(days)
        .join(counts, ["eva", "service_date"], "left")
        .withColumn("stops", F.coalesce(F.col("stops"), F.lit(0)))
    )

