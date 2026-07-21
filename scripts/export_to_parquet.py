"""One-off/periodic: export the live Postgres tables to Parquet files where
joy's Apache can serve them directly.

webapp.app no longer holds a live DATABASE_URL connection — it reads a
Parquet snapshot over plain HTTP instead (see its module docstring). That
snapshot needs to land somewhere joy's Apache (mod_userdir) already serves
publicly, e.g. ~/public_html/spectra_data on morgan — since morgan and joy
share the same NFS home directory, writing there is enough, no separate
publish/sync step. This script is the only thing besides sync.main and
ingest.add_star that still talks to the real Postgres database. There is no
automatic trigger — run it by hand (or your own cron) whenever you want the
hosted search to pick up a run's worth of sync results.

Usage:
    DATABASE_URL=postgresql:///spectra_local \\
    python3 -m scripts.export_to_parquet --out-dir ~/public_html/spectra_data
"""

from __future__ import annotations

import argparse
import logging
import os

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TABLES = ["stars", "archives", "spectroscopy_holdings", "archive_sync_state"]


def export_tables(database_url: str, out_dir: str) -> None:
    con = duckdb.connect()
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(f"ATTACH '{database_url}' AS pg (TYPE postgres, READ_ONLY)")
    for table in TABLES:
        path = os.path.join(out_dir, f"{table}.parquet")
        con.execute(f"COPY pg.{table} TO '{path}' (FORMAT PARQUET)")
        # Apache runs as a different user — needs world-read to serve these,
        # same as everything else already public under public_html.
        os.chmod(path, 0o644)
        logger.info("exported %s -> %s", table, path)
    con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", required=True, help="directory Apache serves, e.g. ~/public_html/spectra_data")
    args = parser.parse_args()

    out_dir = os.path.expanduser(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)
    os.chmod(out_dir, 0o755)

    database_url = os.environ["DATABASE_URL"]
    export_tables(database_url, out_dir)


if __name__ == "__main__":
    main()
