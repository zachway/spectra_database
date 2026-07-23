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
import shutil
import tempfile

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TABLES = ["stars", "archives", "spectroscopy_holdings", "archive_sync_state"]

# Per-archive status breakdown (last sync time/status, observation date
# range, plus a count per match category) for the Archive Status page. Was
# assembled live in webapp.app from a plain LEFT JOIN of
# archives/archive_sync_state -- cheap on its own (both are small tables),
# but the richer per-archive counts the page now shows (how many
# direct-Gaia-matched, name-resolved, positional, needs-review, skipped)
# need a GROUP BY over the full, ever-growing holdings table, same
# OOM-shaped risk as everything else precomputed here.
#
# category must be a CASE, not COALESCE(match_method, match_status) --
# confirmed live that match_method is NOT null on skipped/needs_review rows
# (it retains whichever method was *attempted*, e.g. positional_easy_match
# tried and failed -> skipped, but match_method still says
# positional_easy_match). COALESCE would pick match_method every time it's
# non-null, silently recategorizing skipped/needs_review rows under
# whatever method almost worked -- confirmed live as a real bug: 5.7M
# skipped rows were showing up as "Positional" matches on the Archive
# Status page instead of "Skipped".
ARCHIVE_STATUS_QUERY = """
WITH counts AS (
    SELECT
        archive_code,
        CASE WHEN match_status = 'matched' THEN match_method ELSE match_status END AS category,
        count(*) AS n
    FROM pg.spectroscopy_holdings
    GROUP BY archive_code, category
),
date_ranges AS (
    SELECT archive_code, min(obs_date) AS min_obs_date, max(obs_date) AS max_obs_date
    FROM pg.spectroscopy_holdings
    WHERE obs_date IS NOT NULL
    GROUP BY archive_code
)
SELECT
    a.archive_code,
    a.display_name,
    s.last_run_at,
    s.last_run_status,
    s.rows_seen_last_run,
    d.min_obs_date,
    d.max_obs_date,
    c.category,
    c.n
FROM pg.archives a
LEFT JOIN pg.archive_sync_state s ON s.archive_code = a.archive_code
LEFT JOIN date_ranges d ON d.archive_code = a.archive_code
LEFT JOIN counts c ON c.archive_code = a.archive_code
ORDER BY a.display_name, c.category
"""

# Per-archive, per-instrument holdings counts -- backs the "Tracked
# instruments" table on the Archive Status page. Same GROUP-BY-over-the-
# full-holdings-table reasoning as everything else precomputed here. One
# archive can span several instruments (e.g. Gemini alone has 18), so this
# is its own table rather than folded into ARCHIVE_STATUS_QUERY above.
INSTRUMENTS_QUERY = """
SELECT a.display_name, h.instrument, count(*) AS n
FROM pg.spectroscopy_holdings h
JOIN pg.archives a ON a.archive_code = h.archive_code
WHERE h.instrument IS NOT NULL
GROUP BY a.display_name, h.instrument
ORDER BY a.display_name, n DESC
"""

LEADERBOARD_TOP_N = 10

# Fully precomputed Leaderboard chart data — not just the raw per-(star,
# period) counts, but the actual top-N-per-period selection webapp.app plots.
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
# Second cut moved the ranking into one SQL query using window functions
# over a star x period grid (cross join, needed so a star's cumulative
# total carries forward through periods where it had no new observations,
# and can still rank). That worked at ~2M stars / ~70 periods, but LAMOST
# pushed the real star count with dated observations to 6.1M -- a 6.1M x
# 74 = ~453M row grid -- which OOM'd DuckDB's 24.9 GiB memory_limit even
# with disk-spilling enabled (temp_directory set), confirmed live. The
# grid was always ~60x bigger than it needed to be: only ~7.3M (star,
# period) pairs actually have any observations at all.
#
# Rewritten below to never materialize that grid:
#   - Within-period top-N doesn't need it at all -- a star with zero new
#     observations this period can never outrank one with a positive
#     count, so ranking directly against the real (star, period, n) rows
#     (no zero-filled ones) gives the same top-N.
#   - Cumulative top-N does need every active star's running total as of
#     each period (including periods where it didn't newly observe), but
#     that's a sweep, not a cross join: walk the ~74 periods in order,
#     keep one running total per star ever observed (bounded by star
#     count, not star x period), and snapshot the top-N after each
#     period's update. Same ranking result as the old grid-based window
#     function, without ever holding more than one row per star.
#   - Only once the (small, low tens-of-thousands) set of stars that ever
#     made top-N by either metric is known does a star x period grid get
#     built -- cast stars only, ~74x that small set, nowhere near the
#     full 6.1M-star grid.
LEADERBOARD_COUNTS_QUERY = """
SELECT
    gaia_source_id,
    year(obs_date) AS yr,
    CASE WHEN month(obs_date) <= 6 THEN 1 ELSE 2 END AS half,
    count(*) AS n
FROM pg.spectroscopy_holdings
WHERE obs_date IS NOT NULL AND gaia_source_id IS NOT NULL
GROUP BY gaia_source_id, yr, half
"""

LEADERBOARD_FINAL_QUERY = f"""
WITH cast_stars AS (
    SELECT gaia_source_id FROM leaderboard_top_period
    UNION
    SELECT gaia_source_id FROM leaderboard_top_cum
),
periods AS (
    SELECT DISTINCT yr, half FROM leaderboard_counts
),
grid AS (
    SELECT cs.gaia_source_id, p.yr, p.half
    FROM cast_stars cs CROSS JOIN periods p
)
SELECT
    g.gaia_source_id,
    COALESCE(s.name_aliases[1], s.input_name, CAST(g.gaia_source_id AS VARCHAR)) AS label,
    g.yr, g.half,
    tp.n AS within_n,
    tc.cum_n AS cumulative_n
FROM grid g
LEFT JOIN leaderboard_top_period tp USING (gaia_source_id, yr, half)
LEFT JOIN leaderboard_top_cum tc USING (gaia_source_id, yr, half)
JOIN pg.stars s ON s.gaia_source_id = g.gaia_source_id
ORDER BY g.gaia_source_id, g.yr, g.half
"""


def _export_leaderboard(con: duckdb.DuckDBPyConnection, path: str) -> None:
    con.execute(f"CREATE OR REPLACE TEMP TABLE leaderboard_counts AS {LEADERBOARD_COUNTS_QUERY}")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE leaderboard_top_period AS
        SELECT gaia_source_id, yr, half, n
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY yr, half ORDER BY n DESC, gaia_source_id
            ) AS period_rank
            FROM leaderboard_counts
        )
        WHERE period_rank <= {LEADERBOARD_TOP_N}
    """)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE leaderboard_cum_state (
            gaia_source_id BIGINT PRIMARY KEY, cum_n BIGINT
        )
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE leaderboard_top_cum (
            gaia_source_id BIGINT, yr INTEGER, half INTEGER, cum_n BIGINT
        )
    """)
    periods = con.execute(
        "SELECT DISTINCT yr, half FROM leaderboard_counts ORDER BY yr, half"
    ).fetchall()
    for yr, half in periods:
        con.execute(
            """
            INSERT INTO leaderboard_cum_state (gaia_source_id, cum_n)
            SELECT gaia_source_id, n FROM leaderboard_counts
            WHERE yr = ? AND half = ?
            ON CONFLICT (gaia_source_id) DO UPDATE
                SET cum_n = leaderboard_cum_state.cum_n + excluded.cum_n
            """,
            [yr, half],
        )
        con.execute(
            f"""
            INSERT INTO leaderboard_top_cum
            SELECT gaia_source_id, ?, ?, cum_n
            FROM leaderboard_cum_state
            ORDER BY cum_n DESC, gaia_source_id
            LIMIT {LEADERBOARD_TOP_N}
            """,
            [yr, half],
        )

    _atomic_copy(con, LEADERBOARD_FINAL_QUERY, path)

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
    # An in-memory connection has no temp_directory by default, so DuckDB
    # can't spill oversized intermediate results (e.g. the leaderboard
    # query's star x period grid) to disk -- it just errors out once
    # memory_limit is hit instead. Confirmed live: LAMOST's addition pushed
    # the leaderboard grid past the box's 24.9 GiB default memory_limit for
    # the first time. Pointing temp_directory somewhere writable lets
    # DuckDB spill instead of OOMing.
    # dir=out_dir (not system /tmp, which can be small/quota-limited on a
    # shared login node) since out_dir is already known to have room for
    # the multi-GB parquet exports themselves.
    spill_dir = tempfile.mkdtemp(prefix=".duckdb_export_spill_", dir=out_dir)
    try:
        con.execute(f"SET temp_directory = '{spill_dir}'")
        con.execute("INSTALL postgres")
        con.execute("LOAD postgres")
        con.execute(f"ATTACH '{database_url}' AS pg (TYPE postgres, READ_ONLY)")
        for table in TABLES:
            path = os.path.join(out_dir, f"{table}.parquet")
            _atomic_copy(con, f"SELECT * FROM pg.{table}", path)
            logger.info("exported %s -> %s", table, path)

        leaderboard_path = os.path.join(out_dir, "leaderboard.parquet")
        _export_leaderboard(con, leaderboard_path)
        logger.info("exported leaderboard -> %s", leaderboard_path)

        cmd_stars_path = os.path.join(out_dir, "cmd_stars.parquet")
        _atomic_copy(con, CMD_STARS_QUERY, cmd_stars_path)
        logger.info("exported cmd_stars -> %s", cmd_stars_path)

        archive_status_path = os.path.join(out_dir, "archive_status.parquet")
        _atomic_copy(con, ARCHIVE_STATUS_QUERY, archive_status_path)
        logger.info("exported archive_status -> %s", archive_status_path)

        instruments_path = os.path.join(out_dir, "instruments.parquet")
        _atomic_copy(con, INSTRUMENTS_QUERY, instruments_path)
        logger.info("exported instruments -> %s", instruments_path)

        export_stats_summary(con, out_dir)
    finally:
        con.close()
        shutil.rmtree(spill_dir, ignore_errors=True)


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
