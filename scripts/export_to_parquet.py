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
import json
import logging
import os

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TABLES = ["stars", "archives", "spectroscopy_holdings", "archive_sync_state"]

LEADERBOARD_TOP_N = 5

# Fully precomputed Leaderboard chart data — not just the raw per-(star,
# period) counts, but the actual top-5-per-period selection webapp.app plots.
#
# First cut of this only moved the raw GROUP BY here and left webapp.app to
# pick the top 5 per period in Python — that GROUP BY alone still produces
# one row per (star, period) with no cap on distinct stars, and this catalog
# tracks millions of stars (DESI/SDSS-V/LAMOST alone put it past 2M), so the
# "aggregated, therefore small" assumption baked into the old in-app version
# was wrong at this catalog's actual scale. webapp.app was then calling
# Python's sorted() over the full ~2M-star population once per period
# (~70+ periods, x2 for both the within-period and cumulative rankings) just
# to keep the top 5 -- confirmed live as what was actually driving the OOM
# (1.1GB+) even after the raw-GROUP-BY-only version of this fix shipped.
#
# Ranking is real work better left to a real query engine than a Python
# loop, so it's done here with window functions instead: a full
# star x period grid (needed so a star's cumulative total carries forward
# through periods where it had no new observations, and can still rank) is
# ranked with ROW_NUMBER() partitioned per period, and only rows where the
# star made top-5 by *either* metric at *any* period survive. That collapses
# ~2M-star x ~70-period grid down to a few tens of thousands of rows --
# confirmed live (17,612 rows from a real run, ~49s) -- which webapp.app can
# now just read and reshape into chart traces with no computation of its own.
LEADERBOARD_QUERY = f"""
WITH counts AS (
    SELECT
        gaia_source_id,
        year(obs_date) AS yr,
        CASE WHEN month(obs_date) <= 6 THEN 1 ELSE 2 END AS half,
        count(*) AS n
    FROM pg.spectroscopy_holdings
    WHERE obs_date IS NOT NULL AND gaia_source_id IS NOT NULL
    GROUP BY gaia_source_id, yr, half
),
periods AS (
    SELECT DISTINCT yr, half FROM counts
),
stars_with_counts AS (
    SELECT DISTINCT gaia_source_id FROM counts
),
grid AS (
    SELECT s.gaia_source_id, p.yr, p.half
    FROM stars_with_counts s CROSS JOIN periods p
),
filled AS (
    SELECT g.gaia_source_id, g.yr, g.half, COALESCE(c.n, 0) AS n
    FROM grid g
    LEFT JOIN counts c USING (gaia_source_id, yr, half)
),
cum AS (
    SELECT gaia_source_id, yr, half, n,
        SUM(n) OVER (
            PARTITION BY gaia_source_id ORDER BY yr, half
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_n
    FROM filled
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY yr, half ORDER BY n DESC, gaia_source_id) AS period_rank,
        ROW_NUMBER() OVER (PARTITION BY yr, half ORDER BY cum_n DESC, gaia_source_id) AS cum_rank
    FROM cum
),
cast_stars AS (
    SELECT DISTINCT gaia_source_id FROM ranked
    WHERE period_rank <= {LEADERBOARD_TOP_N} OR cum_rank <= {LEADERBOARD_TOP_N}
)
SELECT
    r.gaia_source_id,
    COALESCE(s.name_aliases[1], s.input_name, CAST(r.gaia_source_id AS VARCHAR)) AS label,
    r.yr, r.half,
    CASE WHEN r.period_rank <= {LEADERBOARD_TOP_N} THEN r.n ELSE NULL END AS within_n,
    CASE WHEN r.cum_rank <= {LEADERBOARD_TOP_N} THEN r.cum_n ELSE NULL END AS cumulative_n
FROM ranked r
JOIN cast_stars cs USING (gaia_source_id)
JOIN pg.stars s ON s.gaia_source_id = r.gaia_source_id
ORDER BY r.gaia_source_id, r.yr, r.half
"""

# Precomputed "most observed" star list for the CMD page — was a random
# USING SAMPLE over `stars` (cheap: no join needed), changed to the N
# most-observed stars instead, which does need a join against the full
# holdings table to count observations per star. Counting is done here
# rather than in webapp.app for the same reason as the Leaderboard: no
# reason to make the memory-constrained hosted container re-scan a
# multi-million-row, ever-growing table on every request when the output is
# a fixed-size, infrequently-changing top-N. Deliberately counts *all*
# holdings rows, not just ones with obs_date (unlike the Leaderboard query
# above) -- DESI and SDSS-V carry no per-observation dates at all (see
# webapp.app's /info page), so filtering on obs_date here would silently
# drop their stars from "most observed" entirely.
CMD_SAMPLE_SIZE = 30000

CMD_STARS_QUERY = f"""
WITH obs_counts AS (
    SELECT gaia_source_id, count(*) AS n
    FROM pg.spectroscopy_holdings
    WHERE gaia_source_id IS NOT NULL
    GROUP BY gaia_source_id
)
SELECT
    s.gaia_source_id,
    s.phot_bp_mean_mag - s.phot_rp_mean_mag AS bp_rp,
    s.phot_g_mean_mag + 5 * log10(s.parallax) - 10 AS abs_g_mag,
    COALESCE(s.name_aliases[1], s.input_name, CAST(s.gaia_source_id AS VARCHAR)) AS label
FROM pg.stars s
JOIN obs_counts oc USING (gaia_source_id)
WHERE s.phot_bp_mean_mag IS NOT NULL AND s.phot_rp_mean_mag IS NOT NULL
  AND s.phot_g_mean_mag IS NOT NULL AND s.parallax > 0
ORDER BY oc.n DESC
LIMIT {CMD_SAMPLE_SIZE}
"""


# /stats used to run five separate full (or near-full) scans of
# spectroscopy_holdings per request -- most-observed, trending, a bare
# count(*), by-archive, by-method -- against the same growing table
# responsible for the Leaderboard's OOM. None of these involve a cross join
# like the Leaderboard did, so each individually is a cheap single-pass
# aggregation, but "cheap x5, every request, over an ever-growing table,
# streamed over HTTP into a memory-capped container" adds up the same way.
# Precomputed here as one small JSON blob instead — total_stars and
# total_holdings are just scalars, and every list here is bounded (top-20,
# or one row per archive/match-method, both small fixed sets) regardless of
# how large the underlying tables get.
TRENDING_YEARS = 5
MOST_OBSERVED_TOP_N = 20
TRENDING_TOP_N = 20

STATS_QUERIES = {
    "most_observed": f"""
        SELECT s.gaia_source_id,
               COALESCE(s.name_aliases[1], s.input_name, CAST(s.gaia_source_id AS VARCHAR)) AS known_as,
               count(*) AS n
        FROM pg.spectroscopy_holdings h
        JOIN pg.stars s ON s.gaia_source_id = h.gaia_source_id
        GROUP BY s.gaia_source_id, s.name_aliases, s.input_name
        ORDER BY n DESC
        LIMIT {MOST_OBSERVED_TOP_N}
    """,
    "trending": f"""
        SELECT s.gaia_source_id,
               COALESCE(s.name_aliases[1], s.input_name, CAST(s.gaia_source_id AS VARCHAR)) AS known_as,
               count(*) AS n
        FROM pg.spectroscopy_holdings h
        JOIN pg.stars s ON s.gaia_source_id = h.gaia_source_id
        WHERE h.obs_date >= CURRENT_DATE - INTERVAL '{TRENDING_YEARS}' YEAR
        GROUP BY s.gaia_source_id, s.name_aliases, s.input_name
        ORDER BY n DESC
        LIMIT {TRENDING_TOP_N}
    """,
    "by_archive": """
        SELECT a.display_name, count(*) AS n
        FROM pg.spectroscopy_holdings h
        JOIN pg.archives a ON a.archive_code = h.archive_code
        GROUP BY a.display_name
        ORDER BY n DESC
    """,
    "by_method": """
        SELECT match_method, count(*) AS n
        FROM pg.spectroscopy_holdings
        WHERE match_status = 'matched'
        GROUP BY match_method
        ORDER BY n DESC
    """,
}


def _fetch_all(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    con.execute(sql)
    cols = [c[0] for c in con.description]
    return [dict(zip(cols, row)) for row in con.fetchall()]


def export_stats_summary(con: duckdb.DuckDBPyConnection, out_dir: str) -> None:
    summary = {
        "total_stars": con.execute("SELECT count(*) FROM pg.stars").fetchone()[0],
        "total_holdings": con.execute("SELECT count(*) FROM pg.spectroscopy_holdings").fetchone()[0],
        "trending_years": TRENDING_YEARS,
        **{name: _fetch_all(con, sql) for name, sql in STATS_QUERIES.items()},
    }
    path = os.path.join(out_dir, "stats_summary.json")
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(summary, f)
    os.chmod(tmp_path, 0o644)
    os.rename(tmp_path, path)
    logger.info("exported stats_summary -> %s", path)


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

    cmd_stars_path = os.path.join(out_dir, "cmd_stars.parquet")
    _atomic_copy(con, CMD_STARS_QUERY, cmd_stars_path)
    logger.info("exported cmd_stars -> %s", cmd_stars_path)

    export_stats_summary(con, out_dir)

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
