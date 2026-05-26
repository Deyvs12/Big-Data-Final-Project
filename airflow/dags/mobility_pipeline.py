"""Airflow DAG: mobility_pipeline.

Chains the 5 PySpark stages on the spark cluster:

    ingest -> clean -> enrich -> analysis -> export_to_mongo

Triggered manually. The spark connection 'spark_default' must point to
spark://mobility-spark-master:7077 (set via the Airflow UI Admin > Connections
or via env var AIRFLOW_CONN_SPARK_DEFAULT).
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

JOBS_DIR = "/opt/airflow/jobs"
DATA_DIR_SPARK = "/opt/bitnami/spark/data"
SPARK_CONN_ID = "spark_default"
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://admin:admin@mobility-mongodb:27017/")
MONGO_DB = os.environ.get("MONGO_DB", "mobility")

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="mobility_pipeline",
    description="NYC TLC mobility analytics: ingest -> clean -> enrich -> analysis -> mongo",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["mobility", "spark", "mongodb"],
) as dag:

    ingest = SparkSubmitOperator(
        task_id="ingest",
        application=f"{JOBS_DIR}/ingest.py",
        conn_id=SPARK_CONN_ID,
        name="mobility-ingest",
        application_args=[
            "--input", f"{DATA_DIR_SPARK}/raw/yellow_tripdata.parquet",
            "--output", f"{DATA_DIR_SPARK}/bronze/trips",
        ],
        verbose=True,
    )

    clean = SparkSubmitOperator(
        task_id="clean",
        application=f"{JOBS_DIR}/clean.py",
        conn_id=SPARK_CONN_ID,
        name="mobility-clean",
        application_args=[
            "--input", f"{DATA_DIR_SPARK}/bronze/trips",
            "--output", f"{DATA_DIR_SPARK}/silver/trips",
        ],
        verbose=True,
    )

    enrich = SparkSubmitOperator(
        task_id="enrich",
        application=f"{JOBS_DIR}/enrich.py",
        conn_id=SPARK_CONN_ID,
        name="mobility-enrich",
        application_args=[
            "--trips", f"{DATA_DIR_SPARK}/silver/trips",
            "--zones", f"{DATA_DIR_SPARK}/zones.csv",
            "--output", f"{DATA_DIR_SPARK}/gold/trips_enriched",
        ],
        verbose=True,
    )

    analysis = SparkSubmitOperator(
        task_id="analysis",
        application=f"{JOBS_DIR}/analysis.py",
        conn_id=SPARK_CONN_ID,
        name="mobility-analysis",
        application_args=[
            "--input", f"{DATA_DIR_SPARK}/gold/trips_enriched",
            "--output", f"{DATA_DIR_SPARK}/gold/agg",
        ],
        verbose=True,
    )

    export_to_mongo = SparkSubmitOperator(
        task_id="export_to_mongo",
        application=f"{JOBS_DIR}/export_to_mongo.py",
        conn_id=SPARK_CONN_ID,
        name="mobility-export-mongo",
        packages="org.mongodb:mongodb-driver-sync:4.11.1",
        application_args=[
            "--input", f"{DATA_DIR_SPARK}/gold/agg",
            "--mongo-uri", MONGO_URI,
            "--database", MONGO_DB,
        ],
        verbose=True,
    )

    ingest >> clean >> enrich >> analysis >> export_to_mongo
