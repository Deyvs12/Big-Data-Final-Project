"""Export gold aggregation parquets to MongoDB.

Reads each subfolder under <input>/, converts the Spark DataFrame to a
Pandas DataFrame, then writes it to MongoDB as one collection per folder.
Existing collection contents are replaced.

Usage:
    spark-submit jobs/export_to_mongo.py \
        --input    /opt/bitnami/spark/data/gold/agg \
        --mongo-uri mongodb://admin:admin@mobility-mongodb:27017/ \
        --database mobility
"""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

COLLECTIONS = [
    "demand_by_hour",
    "demand_by_day",
    "top_zones_origin",
    "top_zones_destination",
    "revenue_by_zone",
    "avg_duration_by_hour",
    "avg_duration_by_zone",
    "revenue_by_payment_type",
]


def build_spark(app_name: str = "mobility-export") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def export_collection(spark, parquet_path: str, mongo_uri: str, db_name: str, coll_name: str) -> int:
    from pymongo import MongoClient

    logger.info("Reading parquet %s", parquet_path)
    df = spark.read.parquet(parquet_path)
    pdf = df.toPandas()
    records = pdf.to_dict(orient="records")
    if not records:
        logger.warning("  %s is empty, skipping", coll_name)
        return 0

    client = MongoClient(mongo_uri)
    try:
        coll = client[db_name][coll_name]
        coll.drop()
        coll.insert_many(records)
        n = coll.count_documents({})
        logger.info("  -> %s.%s: inserted %d documents", db_name, coll_name, n)
        return n
    finally:
        client.close()


def run(input_root: str, mongo_uri: str, db_name: str) -> None:
    spark = build_spark()
    root = Path(input_root)
    if not root.exists():
        raise FileNotFoundError(f"Aggregations folder not found: {root}")

    total = 0
    for name in COLLECTIONS:
        sub = root / name
        if not sub.exists():
            logger.warning("Missing aggregation folder: %s (skipping)", sub)
            continue
        total += export_collection(spark, str(sub), mongo_uri, db_name, name)

    logger.info("Export complete. %d total documents across %d collections.",
                total, len(COLLECTIONS))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="/opt/bitnami/spark/data/gold/agg")
    parser.add_argument(
        "--mongo-uri",
        default=os.environ.get("MONGO_URI", "mongodb://admin:admin@mobility-mongodb:27017/"),
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("MONGO_DB", "mobility"),
    )
    args = parser.parse_args()
    run(args.input, args.mongo_uri, args.database)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
