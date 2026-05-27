#!/usr/bin/env python3
"""Reset pipeline state so the next run starts from zero.

Use this before recording a demo: it removes everything the pipeline
generates (bronze/silver/gold parquets, MongoDB collections, Airflow run
history, Python caches) without touching source code or Docker containers.

Default:
    python reset.py

Optional flags:
    --raw     Also delete data/raw/yellow_tripdata.parquet (forces re-download)
    --logs    Also delete the Airflow logs Docker volume (requires restart)
    --yes     Skip confirmation prompt

Examples:
    python reset.py              # safe reset, keeps the downloaded parquet
    python reset.py --raw --yes  # full nuke, no questions asked
"""
from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("reset")

ROOT = Path(__file__).parent.resolve()
DATA_LAYERS = ["data/bronze", "data/silver", "data/gold"]
RAW_PATH = "data/raw/yellow_tripdata.parquet"
AIRFLOW_LOGS_VOLUME = "big-data-final-project_mobility-airflow-logs"
MONGO_DB_NAME = "mobility"
DAG_ID = "mobility_pipeline"


def container_running(name: str) -> bool:
    res = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", name],
        capture_output=True, text=True,
    )
    return res.returncode == 0 and res.stdout.strip() == "true"


# ---------------------------------------------------------------------------
# Cleaners
# ---------------------------------------------------------------------------

def clean_data_layers() -> None:
    for d in DATA_LAYERS:
        p = ROOT / d
        if p.exists():
            shutil.rmtree(p)
            logger.info("Removed %s", p.relative_to(ROOT))
        else:
            logger.info("Skipped %s (not present)", d)


def clean_pycache() -> None:
    removed = 0
    for cache in ROOT.rglob("__pycache__"):
        shutil.rmtree(cache, ignore_errors=True)
        removed += 1
    logger.info("Removed %d __pycache__ directories", removed)


def clean_raw() -> None:
    p = ROOT / RAW_PATH
    if p.exists():
        p.unlink()
        logger.info("Removed %s", RAW_PATH)
    else:
        logger.info("Skipped %s (not present)", RAW_PATH)


def clean_mongodb() -> None:
    if not container_running("mobility-mongodb"):
        logger.warning("mobility-mongodb is not running; skipping MongoDB reset")
        return
    cmd = [
        "docker", "exec", "mobility-mongodb",
        "mongosh", "-u", "admin", "-p", "admin",
        "--authenticationDatabase", "admin",
        "--quiet",
        "--eval", f"db.getSiblingDB('{MONGO_DB_NAME}').dropDatabase()",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode == 0:
        logger.info("Dropped MongoDB database '%s'", MONGO_DB_NAME)
    else:
        logger.error("Failed to drop Mongo DB: %s", res.stderr.strip())


def clean_airflow_runs() -> None:
    if not container_running("mobility-airflow-scheduler"):
        logger.warning("mobility-airflow-scheduler is not running; skipping Airflow reset")
        return
    script = (
        "from airflow.models import DagRun, TaskInstance, Log;"
        "from airflow.utils.session import create_session;"
        "import sys;"
        "ses = next(create_session().__enter__().__class__.__mro__[0]() for _ in [0]) if False else None;"
    )
    # Use a cleaner inline script via a file-like heredoc isn't easy through
    # `docker exec`, so we pass the whole snippet as one -c argument.
    snippet = f"""
from airflow.models import DagRun, TaskInstance, Log
from airflow.utils.session import create_session
with create_session() as s:
    ti = s.query(TaskInstance).filter(TaskInstance.dag_id == '{DAG_ID}').delete()
    dr = s.query(DagRun).filter(DagRun.dag_id == '{DAG_ID}').delete()
    try:
        lg = s.query(Log).filter(Log.dag_id == '{DAG_ID}').delete()
    except Exception:
        lg = 0
    s.commit()
    print(f'deleted task_instances={{ti}} dag_runs={{dr}} logs={{lg}}')
"""
    res = subprocess.run(
        ["docker", "exec", "mobility-airflow-scheduler", "python", "-c", snippet],
        capture_output=True, text=True,
    )
    if res.returncode == 0:
        logger.info("Airflow: %s", res.stdout.strip())
    else:
        logger.error("Failed to clear Airflow history: %s", res.stderr.strip())


def clean_logs_volume() -> None:
    res = subprocess.run(
        ["docker", "volume", "rm", AIRFLOW_LOGS_VOLUME],
        capture_output=True, text=True,
    )
    if res.returncode == 0:
        logger.info("Removed Docker volume %s", AIRFLOW_LOGS_VOLUME)
        logger.warning("Restart Airflow services: docker compose up -d "
                       "mobility-airflow-init mobility-airflow-webserver mobility-airflow-scheduler")
    else:
        msg = res.stderr.strip()
        if "in use" in msg.lower():
            logger.error("Volume in use. Stop Airflow first: docker compose stop "
                         "mobility-airflow-webserver mobility-airflow-scheduler mobility-airflow-init")
        else:
            logger.warning("Could not remove logs volume: %s", msg)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def confirm(prompt: str) -> bool:
    try:
        return input(f"{prompt} [y/N] ").strip().lower() in ("y", "yes")
    except (EOFError, KeyboardInterrupt):
        return False


def summary(args) -> None:
    print("\nThis will reset:")
    print(f"  - Data layers     : {', '.join(DATA_LAYERS)}")
    print(f"  - MongoDB         : database '{MONGO_DB_NAME}'")
    print(f"  - Airflow history : dag '{DAG_ID}' (runs + tasks + logs in DB)")
    print(f"  - Python caches   : all __pycache__ folders")
    if args.raw:
        print(f"  - Raw data        : {RAW_PATH}")
    if args.logs:
        print(f"  - Logs volume     : {AIRFLOW_LOGS_VOLUME}")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--raw", action="store_true",
                        help="Also delete the downloaded TLC parquet")
    parser.add_argument("--logs", action="store_true",
                        help="Also delete the Airflow logs Docker volume")
    parser.add_argument("--yes", "-y", action="store_true",
                        help="Skip confirmation prompt")
    args = parser.parse_args()

    summary(args)
    if not args.yes and not confirm("Proceed?"):
        logger.info("Aborted.")
        return 1

    clean_data_layers()
    clean_pycache()
    clean_mongodb()
    clean_airflow_runs()
    if args.raw:
        clean_raw()
    if args.logs:
        clean_logs_volume()

    logger.info("Reset complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
