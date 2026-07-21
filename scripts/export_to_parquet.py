"""One-off/periodic: export the live Postgres tables to Parquet files and
publish them to a Hugging Face Hub dataset repo.

webapp.app no longer holds a live DATABASE_URL connection — Spaces has no
free persistent disk, and a hosted Postgres eventually costs money at this
project's scale (see project notes on Hub storage vs. a hosted Postgres).
Instead the webapp queries DuckDB against a periodic Parquet snapshot
published here, and this script is the only thing besides sync.main and
ingest.add_star that still talks to the real Postgres database. There is no
automatic trigger — run it by hand (or your own cron) whenever you want the
hosted search to pick up a run's worth of sync results.

Usage:
    DATABASE_URL=postgresql:///spectra_local \\
    HF_DATASET_REPO=yourname/spectra-database \\
    python3 -m scripts.export_to_parquet

    # Export locally without touching the Hub, e.g. for webapp local dev:
    DATABASE_URL=postgresql:///spectra_local \\
    python3 -m scripts.export_to_parquet --out-dir ./data --no-publish
"""

from __future__ import annotations

import argparse
import logging
import os
import tempfile

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Just the tables webapp.app actually queries — archive_sync_state is
# internal sync-operational state, no reason to publish it.
TABLES = ["stars", "archives", "spectroscopy_holdings"]


def export_tables(database_url: str, out_dir: str) -> None:
    con = duckdb.connect()
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(f"ATTACH '{database_url}' AS pg (TYPE postgres, READ_ONLY)")
    for table in TABLES:
        path = os.path.join(out_dir, f"{table}.parquet")
        con.execute(f"COPY pg.{table} TO '{path}' (FORMAT PARQUET)")
        logger.info("exported %s -> %s", table, path)
    con.close()


def publish(out_dir: str, repo_id: str) -> None:
    from huggingface_hub import HfApi

    api = HfApi()
    api.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True)
    for table in TABLES:
        path = os.path.join(out_dir, f"{table}.parquet")
        api.upload_file(
            path_or_fileobj=path,
            path_in_repo=f"{table}.parquet",
            repo_id=repo_id,
            repo_type="dataset",
        )
        logger.info("published %s -> %s", table, repo_id)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", help="write Parquet files here instead of a temp dir")
    parser.add_argument("--no-publish", action="store_true", help="export locally only, don't push to the Hub")
    args = parser.parse_args()

    database_url = os.environ["DATABASE_URL"]

    if args.out_dir:
        os.makedirs(args.out_dir, exist_ok=True)
        export_tables(database_url, args.out_dir)
        if not args.no_publish:
            publish(args.out_dir, os.environ["HF_DATASET_REPO"])
    else:
        with tempfile.TemporaryDirectory() as tmp:
            export_tables(database_url, tmp)
            if not args.no_publish:
                publish(tmp, os.environ["HF_DATASET_REPO"])


if __name__ == "__main__":
    main()
