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

# Precomputed per (star, 6-month period) observation counts, for the
# Leaderboard page. webapp.app used to run this GROUP BY itself against the
# full spectroscopy_holdings table on every /timeplots request — cheap when
# the archive was small, but it means pulling the entire (multi-million-row,
# and growing) holdings table over HTTP into the Cloud Run container's 512Mi
# just to produce a few thousand aggregate rows. Confirmed live as the cause
# of a same-day OOM crash loop after a large sync run grew the table.
# Precomputing it here instead — on morgan, against live Postgres, with
# GROUP BY pushed down rather than materialized client-side — means the
# hosted app only ever reads an output already bounded by distinct
# star-periods, regardless of how large holdings gets.
LEADERBOARD_QUERY = """
SELECT
    gaia_source_id,
    year(obs_date) AS yr,
    CASE WHEN month(obs_date) <= 6 THEN 1 ELSE 2 END AS half,
    count(*) AS n
FROM pg.spectroscopy_holdings
WHERE obs_date IS NOT NULL AND gaia_source_id IS NOT NULL
GROUP BY gaia_source_id, yr, half
"""


def _atomic_copy(con: duckdb.DuckDBPyConnection, select_sql: str, path: str) -> None:
    # COPY TO writes straight to `path`, with no atomicity -- a request that
    # reads the file mid-write (webapp.app reads this snapshot live over
    # HTTP while exports happen independently, on no fixed schedule) sees a
    # torn/partial Parquet file and errors out. Confirmed live as the cause
    # of a one-off duckdb "don't know what type:" crash on /stats. Writing
    # to a temp path and rename()-ing into place avoids that: rename is
    # atomic on the same filesystem, so readers only ever see a complete
    # file at `path`, never a partial one.
    tmp_path = path + ".tmp"
    con.execute(f"COPY ({select_sql}) TO '{tmp_path}' (FORMAT PARQUET)")
    os.chmod(tmp_path, 0o644)
    os.rename(tmp_path, path)


def export_tables(database_url: str, out_dir: str) -> None:
    con = duckdb.connect()
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(f"ATTACH '{database_url}' AS pg (TYPE postgres, READ_ONLY)")
    for table in TABLES:
        path = os.path.join(out_dir, f"{table}.parquet")
        _atomic_copy(con, f"SELECT * FROM pg.{table}", path)
        logger.info("exported %s -> %s", table, path)

    leaderboard_path = os.path.join(out_dir, "leaderboard.parquet")
    _atomic_copy(con, LEADERBOARD_QUERY, leaderboard_path)
    logger.info("exported leaderboard -> %s", leaderboard_path)

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
