"""Generate the 8 analytical aggregations.

Reads the gold/trips_enriched parquet and writes one parquet folder per
aggregation under <output>/<name>/. Same names are used as MongoDB
collections by export_to_mongo.py.

Aggregations:
    1. demand_by_hour              hour_of_day -> trips, avg_fare
    2. demand_by_day               day_of_week -> trips, avg_fare
    3. top_zones_origin            pickup_zone -> trips (desc)
    4. top_zones_destination       dropoff_zone -> trips (desc)
    5. revenue_by_zone             pickup_zone -> total_fare, avg_fare, trips
    6. avg_duration_by_hour        hour_of_day -> avg_duration_min, avg_distance_km
    7. avg_duration_by_zone        pickup_zone -> avg_duration_min, avg_distance_km
    8. revenue_by_payment_type     payment_type -> total_fare, avg_fare, trips

Usage:
    spark-submit jobs/analysis.py \
        --input  /opt/bitnami/spark/data/gold/trips_enriched \
        --output /opt/bitnami/spark/data/gold/agg
"""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DAY_NAMES = {
    0: "Lunes", 1: "Martes", 2: "Miercoles", 3: "Jueves",
    4: "Viernes", 5: "Sabado", 6: "Domingo",
}


def build_spark(app_name: str = "mobility-analysis") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def demand_by_hour(trips: DataFrame) -> DataFrame:
    return (trips.groupBy("hour_of_day")
            .agg(F.count("*").alias("trips"),
                 F.round(F.avg("fare_amount"), 2).alias("avg_fare"))
            .orderBy("hour_of_day"))


def demand_by_day(trips: DataFrame) -> DataFrame:
    name_map = F.create_map(*[
        x for kv in DAY_NAMES.items() for x in (F.lit(kv[0]), F.lit(kv[1]))
    ])
    return (trips.groupBy("day_of_week")
            .agg(F.count("*").alias("trips"),
                 F.round(F.avg("fare_amount"), 2).alias("avg_fare"))
            .withColumn("day_name", name_map[F.col("day_of_week")])
            .withColumn(
                "is_weekend",
                F.when(F.col("day_of_week") >= 5, F.lit(True)).otherwise(F.lit(False)),
            )
            .orderBy("day_of_week"))


def top_zones_origin(trips: DataFrame) -> DataFrame:
    return (trips.groupBy("pickup_zone")
            .agg(F.count("*").alias("trips"),
                 F.round(F.sum("fare_amount"), 2).alias("total_fare"))
            .orderBy(F.col("trips").desc())
            .limit(10))


def top_zones_destination(trips: DataFrame) -> DataFrame:
    return (trips.groupBy("dropoff_zone")
            .agg(F.count("*").alias("trips"),
                 F.round(F.sum("fare_amount"), 2).alias("total_fare"))
            .orderBy(F.col("trips").desc())
            .limit(10))


def revenue_by_zone(trips: DataFrame) -> DataFrame:
    return (trips.groupBy("pickup_zone")
            .agg(F.round(F.sum("fare_amount"), 2).alias("total_fare"),
                 F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                 F.count("*").alias("trips"))
            .orderBy(F.col("total_fare").desc()))


def avg_duration_by_hour(trips: DataFrame) -> DataFrame:
    return (trips.groupBy("hour_of_day")
            .agg(F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
                 F.round(F.avg("trip_distance_km"), 2).alias("avg_distance_km"),
                 F.count("*").alias("trips"))
            .orderBy("hour_of_day"))


def avg_duration_by_zone(trips: DataFrame) -> DataFrame:
    return (trips.groupBy("pickup_zone")
            .agg(F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
                 F.round(F.avg("trip_distance_km"), 2).alias("avg_distance_km"),
                 F.count("*").alias("trips"))
            .orderBy(F.col("avg_duration_min").desc()))


def revenue_by_payment_type(trips: DataFrame) -> DataFrame:
    return (trips.groupBy("payment_type")
            .agg(F.round(F.sum("fare_amount"), 2).alias("total_fare"),
                 F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                 F.count("*").alias("trips"))
            .orderBy(F.col("total_fare").desc()))


AGGREGATIONS = {
    "demand_by_hour": demand_by_hour,
    "demand_by_day": demand_by_day,
    "top_zones_origin": top_zones_origin,
    "top_zones_destination": top_zones_destination,
    "revenue_by_zone": revenue_by_zone,
    "avg_duration_by_hour": avg_duration_by_hour,
    "avg_duration_by_zone": avg_duration_by_zone,
    "revenue_by_payment_type": revenue_by_payment_type,
}


def run(input_path: str, output_root: str) -> None:
    spark = build_spark()
    logger.info("Reading enriched trips from %s", input_path)
    trips = spark.read.parquet(input_path).cache()
    logger.info("Trip count: %d", trips.count())

    for name, fn in AGGREGATIONS.items():
        out = f"{output_root.rstrip('/')}/{name}"
        logger.info("Generating %s -> %s", name, out)
        result = fn(trips)
        (result.coalesce(1)
               .write
               .mode("overwrite")
               .parquet(out))
        logger.info("  %s rows: %d", name, result.count())

    logger.info("All aggregations written under %s", output_root)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="/opt/bitnami/spark/data/gold/trips_enriched")
    parser.add_argument("--output", default="/opt/bitnami/spark/data/gold/agg")
    args = parser.parse_args()
    run(args.input, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
