"""Download NYC TLC Yellow Taxi parquet (2016-06 last month with lat/lon).

Usage:
    python data/download_tlc_data.py [--fraction 0.05]

The downloaded file lands in data/raw/yellow_tripdata.parquet and is
consumed by jobs/ingest.py which maps the TLC schema to the project schema.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import urllib.request
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_URL = os.environ.get(
    "TLC_PARQUET_URL",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2015-06.parquet",
)
DEFAULT_OUTPUT = Path(__file__).parent / "raw" / "yellow_tripdata.parquet"


def _progress(block_num: int, block_size: int, total_size: int) -> None:
    if total_size <= 0:
        return
    downloaded = block_num * block_size
    pct = min(100.0, downloaded * 100.0 / total_size)
    mb = downloaded / (1024 * 1024)
    total_mb = total_size / (1024 * 1024)
    sys.stdout.write(f"\r  {pct:5.1f}%  ({mb:6.1f} / {total_mb:6.1f} MB)")
    sys.stdout.flush()


def download(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        logger.info("File already exists at %s, skipping download", dest)
        return dest
    logger.info("Downloading %s -> %s", url, dest)
    tmp = dest.with_suffix(dest.suffix + ".part")
    urllib.request.urlretrieve(url, tmp, reporthook=_progress)
    sys.stdout.write("\n")
    tmp.rename(dest)
    logger.info("Download complete (%.1f MB)", dest.stat().st_size / (1024 * 1024))
    return dest


def sample(src: Path, fraction: float) -> None:
    """Reduce file size for local development by random sampling."""
    if fraction >= 1.0:
        return
    try:
        import pyarrow.parquet as pq  # type: ignore
    except ImportError:
        logger.warning("pyarrow not installed; skipping sampling. Install with: pip install pyarrow")
        return
    logger.info("Sampling %.0f%% of rows", fraction * 100)
    table = pq.read_table(src)
    n = table.num_rows
    keep = max(1, int(n * fraction))
    import random
    random.seed(42)
    indices = sorted(random.sample(range(n), keep))
    sampled = table.take(indices)
    pq.write_table(sampled, src)
    logger.info("Sampled %d rows -> %d rows", n, keep)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), type=Path)
    parser.add_argument(
        "--fraction",
        type=float,
        default=float(os.environ.get("TLC_SAMPLE_FRACTION", "0.05")),
        help="Fraction of rows to keep after download (0 < f <= 1).",
    )
    args = parser.parse_args()

    dest = download(args.url, args.output)
    sample(dest, args.fraction)
    logger.info("Done. Raw parquet ready at %s", dest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
