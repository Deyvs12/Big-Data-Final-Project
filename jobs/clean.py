"""Clean and preprocess trips parquet.

- Drop nulls in key fields: pickup_datetime, dropoff_datetime, trip_distance_km, fare_amount.
- Cast types where necessary.
- Derive trip_duration_min, hour_of_day, day_of_week (0=Mon, 6=Sun).
- Filter outliers:
    trip_distance_km <= 0
    fare_amount      <= 0
    trip_duration_min <= 0 or > 180
    passenger_count  <= 0

Usage:
    spark-submit jobs/clean.py \
        --input  /opt/bitnami/spark/data/bronze/trips \
        --output /opt/bitnami/spark/data/silver/trips
"""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KEY_FIELDS = [
    "pickup_datetime",
    "dropoff_datetime",
    "trip_distance_km",
    "fare_amount",
]


def build_spark(app_name: str = "mobility-clean") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def coerce_types(df):
    casts = {
        "pickup_datetime": TimestampType(),
        "dropoff_datetime": TimestampType(),
        "pickup_longitude": DoubleType(),
        "pickup_latitude": DoubleType(),
        "dropoff_longitude": DoubleType(),
        "dropoff_latitude": DoubleType(),
        "passenger_count": IntegerType(),
        "trip_distance_km": DoubleType(),
        "fare_amount": DoubleType(),
    }
    for col, t in casts.items():
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(t))
    return df


def add_derived(df):
    df = df.withColumn(
        "trip_duration_min",
        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0,
    )
    df = df.withColumn("hour_of_day", F.hour("pickup_datetime"))
    # Spark dayofweek: 1=Sun..7=Sat. Convert to 0=Mon..6=Sun.
    df = df.withColumn(
        "day_of_week",
        ((F.dayofweek("pickup_datetime") + F.lit(5)) % F.lit(7)),
    )
    return df


def filter_outliers(df):
    return df.filter(
        (F.col("trip_distance_km") > 0)
        & (F.col("fare_amount") > 0)
        & (F.col("trip_duration_min") > 0)
        & (F.col("trip_duration_min") <= 180)
        & (F.col("passenger_count") > 0)
    )


def run(input_path: str, output_path: str) -> None:
    spark = build_spark()
    logger.info("Reading bronze trips from %s", input_path)
    df = spark.read.parquet(input_path)
    before = df.count()
    logger.info("Bronze row count: %d", before)

    df = coerce_types(df)
    df = df.dropna(subset=KEY_FIELDS)
    after_nulls = df.count()
    logger.info("After null drop: %d (removed %d)", after_nulls, before - after_nulls)

    df = add_derived(df)
    df = filter_outliers(df)
    after_outliers = df.count()
    logger.info(
        "After outlier filter: %d (removed %d)",
        after_outliers,
        after_nulls - after_outliers,
    )

    logger.info("Writing silver parquet to %s", output_path)
    (df.write
       .mode("overwrite")
       .parquet(output_path))
    logger.info("Clean complete. Wrote %d rows.", after_outliers)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="/opt/bitnami/spark/data/bronze/trips")
    parser.add_argument("--output", default="/opt/bitnami/spark/data/silver/trips")
    args = parser.parse_args()
    run(args.input, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
