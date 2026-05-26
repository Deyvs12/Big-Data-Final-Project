"""Ingest TLC Yellow Taxi parquet and adapt to project schema.

Reads the raw TLC parquet (2016-06 era columns: tpep_pickup_datetime,
trip_distance in miles, payment_type as integer code, etc.) and writes a
bronze parquet that matches the project schema:

    trip_id              string
    pickup_datetime      timestamp
    dropoff_datetime     timestamp
    pickup_longitude     double
    pickup_latitude      double
    dropoff_longitude    double
    dropoff_latitude     double
    passenger_count      int
    trip_distance_km     double   (miles * 1.60934)
    fare_amount          double
    payment_type         string   (codes mapped to: efectivo | tarjeta | app)
    driver_id            string

Usage:
    spark-submit jobs/ingest.py \
        --input  /opt/bitnami/spark/data/raw/yellow_tripdata.parquet \
        --output /opt/bitnami/spark/data/bronze/trips
"""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

MILES_TO_KM = 1.60934

PAYMENT_TYPE_MAP = {
    "1": "tarjeta",
    "2": "efectivo",
    "3": "app",
    "4": "app",
    "5": "app",
    "6": "app",
}

# TLC retroactively republished pre-2016-07 parquets with PULocationID /
# DOLocationID instead of pickup/dropoff lon-lat. The project schema requires
# lat/lon, so when only LocationIDs are present we synthesize deterministic
# coordinates inside a rough NYC bounding box.
NYC_LON_MIN, NYC_LON_MAX = -74.25, -73.70
NYC_LAT_MIN, NYC_LAT_MAX = 40.50, 40.92


def _coord_from_id(id_col, salt: str, lo: float, hi: float):
    """Deterministic float in [lo, hi) from an integer column + salt string."""
    bucket = F.pmod(
        F.hash(F.concat(F.col(id_col).cast(StringType()), F.lit(salt))),
        F.lit(1_000_000),
    )
    return F.lit(lo) + (bucket / F.lit(1_000_000.0)) * F.lit(hi - lo)


def synthesize_coords(df):
    """Add pickup/dropoff lon/lat derived from PULocationID/DOLocationID."""
    if "pickup_longitude" in df.columns:
        return df
    if "PULocationID" not in df.columns or "DOLocationID" not in df.columns:
        logger.warning("No lat/lon and no LocationID columns; cannot synthesize coordinates")
        return df

    logger.info("Synthesizing pickup/dropoff lat-lon from PULocationID/DOLocationID")
    df = df.withColumn("pickup_longitude", _coord_from_id("PULocationID", "lon", NYC_LON_MIN, NYC_LON_MAX))
    df = df.withColumn("pickup_latitude",  _coord_from_id("PULocationID", "lat", NYC_LAT_MIN, NYC_LAT_MAX))
    df = df.withColumn("dropoff_longitude", _coord_from_id("DOLocationID", "lon", NYC_LON_MIN, NYC_LON_MAX))
    df = df.withColumn("dropoff_latitude",  _coord_from_id("DOLocationID", "lat", NYC_LAT_MIN, NYC_LAT_MAX))
    return df


def build_spark(app_name: str = "mobility-ingest") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def adapt_schema(df):
    """Rename TLC columns and convert units to project schema."""
    rename_map = {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "pickup_longitude": "pickup_longitude",
        "pickup_latitude": "pickup_latitude",
        "dropoff_longitude": "dropoff_longitude",
        "dropoff_latitude": "dropoff_latitude",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance_miles",
        "fare_amount": "fare_amount",
        "payment_type": "payment_type_code",
        "VendorID": "driver_id",
    }
    available = {src: dst for src, dst in rename_map.items() if src in df.columns}
    missing = set(rename_map) - set(available)
    if missing:
        logger.warning("TLC source missing expected columns: %s", sorted(missing))

    for src, dst in available.items():
        if src != dst:
            df = df.withColumnRenamed(src, dst)

    df = synthesize_coords(df)

    df = df.withColumn(
        "trip_id",
        F.concat_ws("_", F.col("driver_id").cast(StringType()),
                    F.date_format("pickup_datetime", "yyyyMMddHHmmss"),
                    F.monotonically_increasing_id().cast(StringType())),
    )

    df = df.withColumn("trip_distance_km", F.col("trip_distance_miles") * F.lit(MILES_TO_KM))

    mapping_expr = F.create_map(*[
        x for pair in PAYMENT_TYPE_MAP.items() for x in (F.lit(pair[0]), F.lit(pair[1]))
    ])
    df = df.withColumn(
        "payment_type",
        F.coalesce(mapping_expr[F.col("payment_type_code").cast(StringType())], F.lit("app")),
    )

    df = df.withColumn("driver_id", F.col("driver_id").cast(StringType()))

    keep = [
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
    ]
    return df.select(*[c for c in keep if c in df.columns])


def run(input_path: str, output_path: str) -> None:
    spark = build_spark()
    logger.info("Reading raw TLC parquet from %s", input_path)
    raw = spark.read.parquet(input_path)
    logger.info("Raw row count: %d, columns: %s", raw.count(), raw.columns)

    adapted = adapt_schema(raw)
    n_out = adapted.count()
    logger.info("Adapted row count: %d", n_out)
    adapted.printSchema()

    logger.info("Writing bronze parquet to %s", output_path)
    (adapted.write
        .mode("overwrite")
        .parquet(output_path))
    logger.info("Ingest complete. Wrote %d rows.", n_out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default="/opt/bitnami/spark/data/raw/yellow_tripdata.parquet",
    )
    parser.add_argument(
        "--output",
        default="/opt/bitnami/spark/data/bronze/trips",
    )
    args = parser.parse_args()
    run(args.input, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
