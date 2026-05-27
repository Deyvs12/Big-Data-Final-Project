"""Enrich silver trips with pickup_zone and dropoff_zone.

Joins each trip's pickup and dropoff coordinates against the zones bounding
boxes loaded from data/zones.csv. Points outside any zone get
zone_name="Desconocida".

Usage:
    spark-submit jobs/enrich.py \
        --trips /opt/bitnami/spark/data/silver/trips \
        --zones /opt/bitnami/spark/data/zones.csv \
        --output /opt/bitnami/spark/data/gold/trips_enriched
"""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def build_spark(app_name: str = "mobility-enrich") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def load_zones(spark, path: str):
    zones = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )
    logger.info("Loaded %d zones from %s", zones.count(), path)
    return zones


def assign_zone(trips, zones, lon_col: str, lat_col: str, out_col: str):
    """Attach the matching zone to each trip via a broadcast bbox join.

    Multiple zones may overlap a single point (e.g. LaGuardia bbox sits inside
    Queens). We resolve ties by keeping the zone with the lowest ``priority``
    (1 = airports/specific, 2 = generic borough). Zones table is tiny (~12
    rows) so the broadcast join + window dedup is cheap.
    """
    pri_col = f"_pri_{out_col}"
    zones_b = F.broadcast(
        zones.select(
            F.col("zone_id").alias(f"{out_col}_id"),
            F.col("zone_name").alias(out_col),
            F.col("min_longitude").alias("_min_lon"),
            F.col("max_longitude").alias("_max_lon"),
            F.col("min_latitude").alias("_min_lat"),
            F.col("max_latitude").alias("_max_lat"),
            F.col("priority").alias(pri_col),
        )
    )
    joined = trips.join(
        zones_b,
        (F.col(lon_col) >= F.col("_min_lon"))
        & (F.col(lon_col) < F.col("_max_lon"))
        & (F.col(lat_col) >= F.col("_min_lat"))
        & (F.col(lat_col) < F.col("_max_lat")),
        how="left",
    )

    # When a point matches multiple zones, keep the most specific one.
    rank_col = f"_rank_{out_col}"
    w = Window.partitionBy("trip_id").orderBy(F.col(pri_col).asc_nulls_last())
    joined = (joined
              .withColumn(rank_col, F.row_number().over(w))
              .filter(F.col(rank_col) == 1)
              .drop(rank_col, pri_col, "_min_lon", "_max_lon", "_min_lat", "_max_lat"))

    joined = joined.fillna({out_col: "Desconocida"})
    return joined


def run(trips_path: str, zones_path: str, output_path: str) -> None:
    spark = build_spark()
    logger.info("Reading silver trips from %s", trips_path)
    trips = spark.read.parquet(trips_path)
    logger.info("Silver row count: %d", trips.count())

    zones = load_zones(spark, zones_path)

    logger.info("Assigning pickup_zone")
    trips = assign_zone(trips, zones, "pickup_longitude", "pickup_latitude", "pickup_zone")
    logger.info("Assigning dropoff_zone")
    trips = assign_zone(trips, zones, "dropoff_longitude", "dropoff_latitude", "dropoff_zone")

    out_cols = [
        "trip_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_longitude",
        "pickup_latitude",
        "dropoff_longitude",
        "dropoff_latitude",
        "passenger_count",
        "trip_distance_km",
        "fare_amount",
        "payment_type",
        "driver_id",
        "trip_duration_min",
        "hour_of_day",
        "day_of_week",
        "pickup_zone",
        "pickup_zone_id",
        "dropoff_zone",
        "dropoff_zone_id",
    ]
    trips = trips.select(*[c for c in out_cols if c in trips.columns])

    logger.info("Writing enriched parquet to %s", output_path)
    (trips.write
        .mode("overwrite")
        .parquet(output_path))
    logger.info("Enrich complete. Wrote %d rows.", trips.count())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trips", default="/opt/bitnami/spark/data/silver/trips")
    parser.add_argument("--zones", default="/opt/bitnami/spark/data/zones.csv")
    parser.add_argument("--output", default="/opt/bitnami/spark/data/gold/trips_enriched")
    args = parser.parse_args()
    run(args.trips, args.zones, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
