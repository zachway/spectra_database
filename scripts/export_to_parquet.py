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
import psycopg

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

# A sample of position-tagged holdings per top instrument, for the /instruments
# page's sky-coverage-by-instrument map -- a live-per-request query here would
# need a ROW_NUMBER()/random() window over potentially tens of millions of
# rows for the biggest instruments, the same shape of full-table-sort cost
# that already OOM'd the hosted container elsewhere in this file (see the
# Leaderboard's long comment). Precomputed here where memory isn't capped,
# same tradeoff as everything else in this module.
INSTRUMENT_SKY_SAMPLE_TOP_N = 12
INSTRUMENT_SKY_SAMPLE_PER_INSTRUMENT = 2000

INSTRUMENT_SKY_SAMPLE_QUERY = f"""
WITH top_instruments AS (
    SELECT instrument, count(*) AS n
    FROM pg.spectroscopy_holdings
    WHERE instrument IS NOT NULL AND raw_ra IS NOT NULL AND raw_dec IS NOT NULL
    GROUP BY instrument
    ORDER BY n DESC
    LIMIT {INSTRUMENT_SKY_SAMPLE_TOP_N}
),
sampled AS (
    SELECT h.instrument, h.raw_ra, h.raw_dec,
           ROW_NUMBER() OVER (PARTITION BY h.instrument ORDER BY random()) AS rn
    FROM pg.spectroscopy_holdings h
    JOIN top_instruments t ON t.instrument = h.instrument
    WHERE h.raw_ra IS NOT NULL AND h.raw_dec IS NOT NULL
)
SELECT instrument, raw_ra, raw_dec
FROM sampled
WHERE rn <= {INSTRUMENT_SKY_SAMPLE_PER_INSTRUMENT}
"""

# Precomputed sample of all stars for the /sky all-sky map -- was a live
# `USING SAMPLE n` against the full `stars` table on every page load, which
# forces DuckDB's remote-parquet reader to scan nearly the whole ~500MB+
# file over HTTP from joy every time (confirmed live: ~27s for a 5,000-row
# sample against a 9.8M-row table, the dominant cost behind "webapp is
# sluggish switching tabs"). Precomputed here where the SAMPLE only runs
# once per export instead of once per tab click; must match webapp.app's
# SKY_SAMPLE_SIZE constant.
SKY_SAMPLE_SIZE = 30000

SKY_SAMPLE_QUERY = f"""
    SELECT gaia_source_id, ra, dec, phot_g_mean_mag,
           COALESCE(name_aliases[1], input_name, CAST(gaia_source_id AS VARCHAR)) AS known_as
    FROM pg.stars
    WHERE ra IS NOT NULL AND dec IS NOT NULL AND phot_g_mean_mag IS NOT NULL
    USING SAMPLE {SKY_SAMPLE_SIZE}
"""

LEADERBOARD_TOP_N = 20

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
    star_id,
    year(obs_date) AS yr,
    CASE WHEN month(obs_date) <= 6 THEN 1 ELSE 2 END AS half,
    count(*) AS n
FROM pg.spectroscopy_holdings
WHERE obs_date IS NOT NULL AND star_id IS NOT NULL
GROUP BY star_id, yr, half
"""

# gaia_source_id (still a real, if no longer primary, column on stars -- see
# db/schema.sql) is only resolved at the very end, via the join to pg.stars
# below -- everything upstream (holdings, the grid, both top-N temp tables)
# is keyed by star_id, since that's the only identifier every tracked star
# has (a small number are BSC5-sourced with no gaia_source_id at all -- see
# db/migrations/0001_star_id_surrogate_key.sql).
LEADERBOARD_FINAL_QUERY = f"""
WITH cast_stars AS (
    SELECT star_id FROM leaderboard_top_period
    UNION
    SELECT star_id FROM leaderboard_top_cum
),
periods AS (
    SELECT DISTINCT yr, half FROM leaderboard_counts
),
grid AS (
    SELECT cs.star_id, p.yr, p.half
    FROM cast_stars cs CROSS JOIN periods p
)
SELECT
    s.star_id,
    s.gaia_source_id,
    COALESCE(s.name_aliases[1], s.input_name, CAST(s.gaia_source_id AS VARCHAR)) AS label,
    g.yr, g.half,
    tp.n AS within_n,
    tc.cum_n AS cumulative_n
FROM grid g
LEFT JOIN leaderboard_top_period tp USING (star_id, yr, half)
LEFT JOIN leaderboard_top_cum tc USING (star_id, yr, half)
JOIN pg.stars s ON s.star_id = g.star_id
ORDER BY s.star_id, g.yr, g.half
"""


def _export_leaderboard(con: duckdb.DuckDBPyConnection, path: str) -> None:
    con.execute(f"CREATE OR REPLACE TEMP TABLE leaderboard_counts AS {LEADERBOARD_COUNTS_QUERY}")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE leaderboard_top_period AS
        SELECT star_id, yr, half, n
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY yr, half ORDER BY n DESC, star_id
            ) AS period_rank
            FROM leaderboard_counts
        )
        WHERE period_rank <= {LEADERBOARD_TOP_N}
    """)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE leaderboard_cum_state (
            star_id BIGINT PRIMARY KEY, cum_n BIGINT
        )
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE leaderboard_top_cum (
            star_id BIGINT, yr INTEGER, half INTEGER, cum_n BIGINT
        )
    """)
    periods = con.execute(
        "SELECT DISTINCT yr, half FROM leaderboard_counts ORDER BY yr, half"
    ).fetchall()
    for yr, half in periods:
        con.execute(
            """
            INSERT INTO leaderboard_cum_state (star_id, cum_n)
            SELECT star_id, n FROM leaderboard_counts
            WHERE yr = ? AND half = ?
            ON CONFLICT (star_id) DO UPDATE
                SET cum_n = leaderboard_cum_state.cum_n + excluded.cum_n
            """,
            [yr, half],
        )
        con.execute(
            f"""
            INSERT INTO leaderboard_top_cum
            SELECT star_id, ?, ?, cum_n
            FROM leaderboard_cum_state
            ORDER BY cum_n DESC, star_id
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
    SELECT star_id, count(*) AS n
    FROM pg.spectroscopy_holdings
    WHERE star_id IS NOT NULL
    GROUP BY star_id
)
SELECT
    s.gaia_source_id,
    s.phot_bp_mean_mag - s.phot_rp_mean_mag AS bp_rp,
    s.phot_g_mean_mag + 5 * log10(s.parallax) - 10 AS abs_g_mag,
    COALESCE(s.name_aliases[1], s.input_name, CAST(s.gaia_source_id AS VARCHAR)) AS label
FROM pg.stars s
JOIN obs_counts oc ON oc.star_id = s.star_id
WHERE s.phot_bp_mean_mag IS NOT NULL AND s.phot_rp_mean_mag IS NOT NULL
  AND s.phot_g_mean_mag IS NOT NULL AND s.parallax > 0
ORDER BY oc.n DESC
LIMIT {CMD_SAMPLE_SIZE}
"""


# /timeplots (the Leaderboard/Stats tab) used to run eight separate full (or
# near-full) scans per request -- most-observed, trending, a bare count(*),
# by-archive, by-method against spectroscopy_holdings, plus nearest,
# fastest-movers and a spectral-type histogram against `stars` -- against the
# same growing tables responsible for the Leaderboard's OOM. None of these
# involve a cross join like the Leaderboard did, so each individually is a
# cheap single-pass aggregation, but "cheap x8, every request, over
# ever-growing tables, streamed over HTTP into a memory-capped container"
# adds up the same way -- confirmed live: nearest/fastest-movers/spectral-
# types each took multiple seconds against a 9.8M-row `stars` served over
# plain HTTP, since ORDER BY/GROUP BY with no filter can't skip row groups.
# Precomputed here as one small JSON blob instead — total_stars and
# total_holdings are just scalars, and every list here is bounded (top-20,
# or one row per archive/match-method/spectral-bucket, all small fixed sets)
# regardless of how large the underlying tables get.
TRENDING_YEARS = 5
MOST_OBSERVED_TOP_N = 20
TRENDING_TOP_N = 20
NEAREST_TOP_N = 20
FASTEST_MOVERS_TOP_N = 20

STATS_QUERIES = {
    "most_observed": f"""
        SELECT s.gaia_source_id,
               COALESCE(s.name_aliases[1], s.input_name, CAST(s.gaia_source_id AS VARCHAR)) AS known_as,
               count(*) AS n
        FROM pg.spectroscopy_holdings h
        JOIN pg.stars s ON s.star_id = h.star_id
        GROUP BY s.star_id, s.gaia_source_id, s.name_aliases, s.input_name
        ORDER BY n DESC
        LIMIT {MOST_OBSERVED_TOP_N}
    """,
    "trending": f"""
        SELECT s.gaia_source_id,
               COALESCE(s.name_aliases[1], s.input_name, CAST(s.gaia_source_id AS VARCHAR)) AS known_as,
               count(*) AS n
        FROM pg.spectroscopy_holdings h
        JOIN pg.stars s ON s.star_id = h.star_id
        WHERE h.obs_date >= CURRENT_DATE - INTERVAL '{TRENDING_YEARS}' YEAR
        GROUP BY s.star_id, s.gaia_source_id, s.name_aliases, s.input_name
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
    "nearest": f"""
        SELECT gaia_source_id,
               COALESCE(name_aliases[1], input_name, CAST(gaia_source_id AS VARCHAR)) AS known_as,
               1000.0 / parallax AS distance_pc
        FROM pg.stars
        WHERE parallax > 0
        ORDER BY parallax DESC
        LIMIT {NEAREST_TOP_N}
    """,
    "fastest_movers": f"""
        SELECT gaia_source_id,
               COALESCE(name_aliases[1], input_name, CAST(gaia_source_id AS VARCHAR)) AS known_as,
               sqrt(pmra * pmra + pmdec * pmdec) AS total_pm
        FROM pg.stars
        WHERE pmra IS NOT NULL AND pmdec IS NOT NULL
        ORDER BY total_pm DESC
        LIMIT {FASTEST_MOVERS_TOP_N}
    """,
    "spectral_types": """
        SELECT
            CASE
                WHEN phot_bp_mean_mag - phot_rp_mean_mag < 0.0 THEN 'O/B (hot)'
                WHEN phot_bp_mean_mag - phot_rp_mean_mag < 0.3 THEN 'A'
                WHEN phot_bp_mean_mag - phot_rp_mean_mag < 0.6 THEN 'F'
                WHEN phot_bp_mean_mag - phot_rp_mean_mag < 0.9 THEN 'G'
                WHEN phot_bp_mean_mag - phot_rp_mean_mag < 1.5 THEN 'K'
                ELSE 'M (cool)'
            END AS bucket,
            count(*) AS n
        FROM pg.stars
        WHERE phot_bp_mean_mag IS NOT NULL AND phot_rp_mean_mag IS NOT NULL
        GROUP BY bucket
    """,
}


# Precomputed per-(archive, reported target name) triage queue -- the
# /triage page used to run this grouping live against the hosted
# DuckDB/Parquet snapshot, but a true GROUP BY (archive_code, raw_target_name)
# over the full skipped set (12M+ rows, 900k+ distinct names) OOM'd the 1 GiB
# Cloud Run container outright, confirmed live against production -- same
# OOM-shaped risk as everything else precomputed here, just without the
# option of even a windowed live fallback (there's no cheap way to know which
# recent rows share a name without grouping first). Computed here instead,
# where memory isn't capped.
#
# Each group's member list is capped at TRIAGE_QUEUE_MAX_RECORDS via a
# ROW_NUMBER()-then-FILTER, not a plain list() -- a plain list() still has to
# build the *entire* array before anyone could trim it, and some names (e.g.
# calibration-frame placeholders repeated across a whole run, or an archive
# that reports no name at all) recur hundreds of thousands of times. The
# FILTER means the aggregate only ever receives up to the cap's worth of
# rows, so the array itself never grows past that -- confirmed against a
# synthetic 200-row group that n_records still reports the true total (200)
# while the array stays capped at 50. NULL and empty-string raw_target_name
# are both treated as "no reported name" (COALESCE + NULLIF/TRIM), falling
# back to one singleton group per record via its archive_obs_id -- otherwise
# every nameless row in an archive would collapse into a single enormous
# group, which is exactly the shape that caused the OOM in the first place.
TRIAGE_QUEUE_MAX_RECORDS = 50
TRIAGE_QUEUE_TOP_N = 200

TRIAGE_QUEUE_QUERY = f"""
WITH ranked AS (
    SELECT h.archive_code, a.display_name, h.raw_target_name, h.archive_obs_id, h.archive_url,
           h.raw_ra, h.raw_dec, h.obs_date, h.instrument, h.updated_at,
           COALESCE(NULLIF(TRIM(h.raw_target_name), ''), 'obs:' || h.archive_obs_id) AS group_key,
           ROW_NUMBER() OVER (
               PARTITION BY h.archive_code,
                            COALESCE(NULLIF(TRIM(h.raw_target_name), ''), 'obs:' || h.archive_obs_id)
               ORDER BY h.updated_at DESC
           ) AS rn,
           -- Unit-vector components of (raw_ra, raw_dec), summed per group
           -- below to measure how tightly clustered a group's positions
           -- actually are. Avoids the wraparound (RA 359 vs RA 1) and pole
           -- (dec near +/-90) false positives a naive max()-min() on ra/dec
           -- would have. NULL automatically when either coordinate is NULL,
           -- so these just drop out of the sum()/count() below.
           cos(radians(h.raw_dec)) * cos(radians(h.raw_ra)) AS vx,
           cos(radians(h.raw_dec)) * sin(radians(h.raw_ra)) AS vy,
           sin(radians(h.raw_dec)) AS vz
    FROM pg.spectroscopy_holdings h
    JOIN pg.archives a ON a.archive_code = h.archive_code
    WHERE h.match_status = 'skipped'
)
SELECT
    archive_code, display_name, group_key,
    NULLIF(TRIM(any_value(raw_target_name)), '') AS raw_target_name,
    count(*) AS n_records,
    list(archive_obs_id) FILTER (WHERE rn <= {TRIAGE_QUEUE_MAX_RECORDS}) AS archive_obs_ids,
    list(archive_url) FILTER (WHERE rn <= {TRIAGE_QUEUE_MAX_RECORDS}) AS archive_urls,
    -- From the single rn=1 row (most recently updated), not independent
    -- per-column min()s -- those can pair one file's RA with a different
    -- file's Dec, producing a synthetic position that matches no real file
    -- (confirmed live: a noirlab group showed RA from a 2010 exposure and
    -- Dec from an unrelated 2011 exposure, ~200 degrees apart from the
    -- group's actual, overwhelmingly common position).
    any_value(raw_ra) FILTER (WHERE rn = 1) AS raw_ra,
    any_value(raw_dec) FILTER (WHERE rn = 1) AS raw_dec,
    -- Angular deviation (degrees) of the group's reported positions from
    -- their mean direction -- 0 means every record with a position agrees,
    -- larger means this "one name" actually covers records at genuinely
    -- different places on the sky (webapp.app warns on this so a reviewer
    -- doesn't trust the single raw_ra/raw_dec above as if it speaks for the
    -- whole group). NULL when fewer than 2 records report a position.
    CASE WHEN count(vx) >= 2 THEN
        degrees(acos(least(1.0, greatest(0.0,
            sqrt(sum(vx) * sum(vx) + sum(vy) * sum(vy) + sum(vz) * sum(vz)) / count(vx)
        ))))
    END AS position_spread_deg,
    max(updated_at) AS updated_at,
    min(obs_date) AS obs_date,
    any_value(instrument) AS instrument
FROM ranked
GROUP BY archive_code, display_name, group_key
-- Named groups first, then largest group first within each bucket -- a
-- human can actually look a reported name up and make a judgment call,
-- unlike an anonymous calibration-frame/no-name record, so named groups are
-- higher-value to surface; within that, a name attached to more records is
-- higher-value still, since one classification resolves all of them at
-- once. This ordering determines the LIMIT selection itself, not just
-- display order -- otherwise a burst of recent nameless activity (e.g. a
-- big LAMOST MRS sync) could crowd every named group out of the top
-- {TRIAGE_QUEUE_TOP_N} entirely before webapp.app ever sees them.
--
-- Repeats the NULLIF(TRIM(any_value(...))) expression rather than
-- referencing the `raw_target_name` output alias -- confirmed live that
-- referencing the bare alias here binds to `ranked.raw_target_name` (the
-- raw, ungrouped column of the same name carried through the CTE) instead
-- of the SELECT list's aggregate, which DuckDB then rejects as ungrouped:
-- "column must appear in the GROUP BY clause or be part of an aggregate
-- function." Wrapping the same expression in an aggregate again here
-- sidesteps the ambiguity entirely.
ORDER BY (NULLIF(TRIM(any_value(raw_target_name)), '') IS NOT NULL) DESC, count(*) DESC
LIMIT {TRIAGE_QUEUE_TOP_N}
"""


# Star-overlap between archives/instruments -- backs the /instruments page's
# overlap heatmap and Venn-diagram picker. A live per-request version would
# need either a full self-join of spectroscopy_holdings against itself (a
# multi-million-row x multi-million-row join for the biggest archives) or a
# GROUP BY over the whole table, same OOM-shaped risk as everything else
# precomputed here.
#
# Both queries take the same shape: first collapse each star down to the
# small array of distinct archives/instruments its *matched* holdings span
# (bounded by however many archives ever observed that one star -- typically
# 1-3, rarely more than a handful even for the most-studied stars), then
# UNNEST that array against itself (twice, for pairs; three times, for
# triples) and count. This is NOT a cross join over the full table -- the
# per-star arrays are what's crossed, so the work for one star is k^2 (or
# k^3) where k is that star's own small archive/instrument count, not the
# catalog's total row count. a<=b(<=c) keeps only one ordering per
# unordered pair/triple and, as a side effect, includes the a==b(==c)
# "self-pair" -- that's not wasted: it's exactly each set's own total
# distinct-star count, so the heatmap's diagonal and the Venn picker's
# per-circle totals come from this same query instead of a separate one.
# The GROUP BY also means a combo that never co-occurs for any star simply
# has no output row at all (implicit zero) rather than an explicit zero row
# -- both tables stay small (low hundreds of archive rows; instrument pairs
# are a bit more, bounded by how many distinct instruments ever share an
# observer, still nowhere near a full N^2/N^3 of all possible combos).
ARCHIVE_OVERLAP_QUERY = """
WITH per_star AS (
    SELECT star_id, array_agg(DISTINCT archive_code) AS codes
    FROM pg.spectroscopy_holdings
    WHERE match_status = 'matched' AND star_id IS NOT NULL
    GROUP BY star_id
)
SELECT
    a.archive_code AS archive_a, da.display_name AS display_a,
    b.archive_code AS archive_b, db.display_name AS display_b,
    count(*) AS n_overlap
FROM per_star, UNNEST(codes) AS a(archive_code), UNNEST(codes) AS b(archive_code)
JOIN pg.archives da ON da.archive_code = a.archive_code
JOIN pg.archives db ON db.archive_code = b.archive_code
WHERE a.archive_code <= b.archive_code
GROUP BY 1, 2, 3, 4
ORDER BY 1, 3
"""

# Triple overlap (a<=b<=c, including the a==b==c self-triple -- redundant
# with ARCHIVE_OVERLAP_QUERY's diagonal, kept anyway since dropping just the
# self-triples would need an extra filter for no real size benefit at this
# table's scale) -- needed for the picker's 3-circle case, since a
# proportional 3-circle Venn needs the true center (A∩B∩C) region size, not
# just the three pairwise overlaps.
ARCHIVE_OVERLAP_TRIPLE_QUERY = """
WITH per_star AS (
    SELECT star_id, array_agg(DISTINCT archive_code) AS codes
    FROM pg.spectroscopy_holdings
    WHERE match_status = 'matched' AND star_id IS NOT NULL
    GROUP BY star_id
)
SELECT
    a.archive_code AS archive_a, b.archive_code AS archive_b, c.archive_code AS archive_c,
    count(*) AS n_overlap
FROM per_star,
     UNNEST(codes) AS a(archive_code), UNNEST(codes) AS b(archive_code), UNNEST(codes) AS c(archive_code)
WHERE a.archive_code <= b.archive_code AND b.archive_code <= c.archive_code
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
"""

# Same shape as ARCHIVE_OVERLAP_QUERY/ARCHIVE_OVERLAP_TRIPLE_QUERY, but keyed
# by instrument instead of archive_code -- one archive can host several
# instruments (see INSTRUMENTS_QUERY above), and "which instruments share
# stars" is a finer-grained, separate question from "which archives share
# stars". instrument is nullable (not every archive reports one), filtered
# out here the same way INSTRUMENTS_QUERY does.
INSTRUMENT_OVERLAP_QUERY = """
WITH per_star AS (
    SELECT star_id, array_agg(DISTINCT instrument) AS insts
    FROM pg.spectroscopy_holdings
    WHERE match_status = 'matched' AND star_id IS NOT NULL AND instrument IS NOT NULL
    GROUP BY star_id
)
SELECT a.instrument AS instrument_a, b.instrument AS instrument_b, count(*) AS n_overlap
FROM per_star, UNNEST(insts) AS a(instrument), UNNEST(insts) AS b(instrument)
WHERE a.instrument <= b.instrument
GROUP BY 1, 2
ORDER BY 1, 2
"""

INSTRUMENT_OVERLAP_TRIPLE_QUERY = """
WITH per_star AS (
    SELECT star_id, array_agg(DISTINCT instrument) AS insts
    FROM pg.spectroscopy_holdings
    WHERE match_status = 'matched' AND star_id IS NOT NULL AND instrument IS NOT NULL
    GROUP BY star_id
)
SELECT
    a.instrument AS instrument_a, b.instrument AS instrument_b, c.instrument AS instrument_c,
    count(*) AS n_overlap
FROM per_star,
     UNNEST(insts) AS a(instrument), UNNEST(insts) AS b(instrument), UNNEST(insts) AS c(instrument)
WHERE a.instrument <= b.instrument AND b.instrument <= c.instrument
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
"""


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
            select_sql = f"SELECT * FROM pg.{table}"
            if table == "spectroscopy_holdings":
                # webapp.app's single-star search filters this table on
                # star_id over httpfs -- confirmed live that an unclustered
                # export means Parquet's row-group min/max stats can't skip
                # anything on that filter, so a lookup pulls a large chunk
                # of this (now >1GB) file into memory and tipped the Cloud
                # Run container's 1GiB limit into repeated OOM kills.
                # Exporting pre-sorted by star_id lets row-group pruning
                # actually work for that query, independent of table size.
                select_sql += " ORDER BY star_id"
            elif table == "stars":
                # Same class of bug as spectroscopy_holdings above, just
                # discovered later: _lookup_local_star's numeric-query path
                # filters this table on gaia_source_id over httpfs.
                # Confirmed live -- an unsorted export of this (now >650MB)
                # table meant every row group's gaia_source_id min/max
                # spanned nearly the whole 0..~6.9e18 range, so row-group
                # pruning couldn't skip anything and a single-ID search
                # pulled the whole file into memory, OOM-killing the Cloud
                # Run container. Sorting by gaia_source_id fixes pruning for
                # that lookup; NULLs (bsc5 stars with no Gaia source) sort
                # together at the end instead of scattering across every
                # row group.
                select_sql += " ORDER BY gaia_source_id"
            _atomic_copy(con, select_sql, path)
            logger.info("exported %s -> %s", table, path)

        leaderboard_path = os.path.join(out_dir, "leaderboard.parquet")
        _export_leaderboard(con, leaderboard_path)
        logger.info("exported leaderboard -> %s", leaderboard_path)

        cmd_stars_path = os.path.join(out_dir, "cmd_stars.parquet")
        _atomic_copy(con, CMD_STARS_QUERY, cmd_stars_path)
        logger.info("exported cmd_stars -> %s", cmd_stars_path)

        sky_sample_path = os.path.join(out_dir, "sky_sample.parquet")
        _atomic_copy(con, SKY_SAMPLE_QUERY, sky_sample_path)
        logger.info("exported sky_sample -> %s", sky_sample_path)

        archive_status_path = os.path.join(out_dir, "archive_status.parquet")
        _atomic_copy(con, ARCHIVE_STATUS_QUERY, archive_status_path)
        logger.info("exported archive_status -> %s", archive_status_path)

        instruments_path = os.path.join(out_dir, "instruments.parquet")
        _atomic_copy(con, INSTRUMENTS_QUERY, instruments_path)
        logger.info("exported instruments -> %s", instruments_path)

        instrument_sky_sample_path = os.path.join(out_dir, "instrument_sky_sample.parquet")
        _atomic_copy(con, INSTRUMENT_SKY_SAMPLE_QUERY, instrument_sky_sample_path)
        logger.info("exported instrument_sky_sample -> %s", instrument_sky_sample_path)

        triage_queue_path = os.path.join(out_dir, "triage_queue.parquet")
        _atomic_copy(con, TRIAGE_QUEUE_QUERY, triage_queue_path)
        logger.info("exported triage_queue -> %s", triage_queue_path)

        archive_overlap_path = os.path.join(out_dir, "archive_overlap.parquet")
        _atomic_copy(con, ARCHIVE_OVERLAP_QUERY, archive_overlap_path)
        logger.info("exported archive_overlap -> %s", archive_overlap_path)

        archive_overlap_triple_path = os.path.join(out_dir, "archive_overlap_triple.parquet")
        _atomic_copy(con, ARCHIVE_OVERLAP_TRIPLE_QUERY, archive_overlap_triple_path)
        logger.info("exported archive_overlap_triple -> %s", archive_overlap_triple_path)

        instrument_overlap_path = os.path.join(out_dir, "instrument_overlap.parquet")
        _atomic_copy(con, INSTRUMENT_OVERLAP_QUERY, instrument_overlap_path)
        logger.info("exported instrument_overlap -> %s", instrument_overlap_path)

        instrument_overlap_triple_path = os.path.join(out_dir, "instrument_overlap_triple.parquet")
        _atomic_copy(con, INSTRUMENT_OVERLAP_TRIPLE_QUERY, instrument_overlap_triple_path)
        logger.info("exported instrument_overlap_triple -> %s", instrument_overlap_triple_path)

        export_stats_summary(con, out_dir)
    finally:
        con.close()
        shutil.rmtree(spill_dir, ignore_errors=True)


# =============================================================================
# Import pending /triage classification submissions (design sketch).
#
# webapp.app's /triage/submit route has no live Postgres access (see its
# module docstring) -- it appends one JSON line per submission to a public
# file on joy instead, over a narrowly-scoped, forced-command-restricted SSH
# connection (see webapp/app.py's _append_triage_submission and
# scripts/joy_triage_append.py). This is the other half: reads that same
# file (this script already runs on/near joy, with local filesystem access
# to the same directory it just wrote the Parquet snapshot to) and imports
# each line into the real skip_classifications table.
#
# Idempotent by construction, not by tracking an offset: every run re-reads
# the *entire* file and INSERTs with ON CONFLICT (archive_code,
# archive_obs_id, submitter) DO NOTHING, relying on skip_classifications'
# existing one-vote-per-submitter unique index -- a line that was already
# imported on a previous run just inserts 0 rows the second time, so there's
# no offset/cursor state to track or get out of sync. The file is
# deliberately never truncated -- this feature's traffic is human-submission
# scale, re-scanning it every run costs nothing worth optimizing for.
# =============================================================================

TRIAGE_SUBMISSIONS_FILENAME = "triage_submissions.jsonl"

REQUIRED_TRIAGE_FIELDS = {"archive_code", "outcome", "submitter", "submitted_at"}
ALLOWED_TRIAGE_OUTCOMES = {
    "attach_gaia_source", "attach_bright_star",
    "not_a_real_target", "not_a_star",
    "confirmed_absent_from_gaia",
}


def import_triage_submissions(database_url: str, out_dir: str) -> None:
    path = os.path.join(out_dir, TRIAGE_SUBMISSIONS_FILENAME)
    if not os.path.exists(path):
        logger.info("no triage submissions file at %s yet, skipping import", path)
        return

    with open(path, encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    n_inserted = 0
    n_skipped = 0
    # autocommit -- each line is its own statement, so one line Postgres
    # rejects (stale archive_obs_id, a CHECK-constraint mismatch scripts/
    # joy_triage_append.py's own validation didn't already catch) doesn't
    # poison a transaction and block every line after it.
    with psycopg.connect(database_url, autocommit=True) as conn, conn.cursor() as cur:
        for line in lines:
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("skipping malformed JSON line in %s: %r", path, line[:200])
                n_skipped += 1
                continue

            if not (isinstance(obj, dict) and REQUIRED_TRIAGE_FIELDS.issubset(obj)
                    and obj.get("outcome") in ALLOWED_TRIAGE_OUTCOMES
                    and ("raw_target_name" in obj) != ("archive_obs_id" in obj)):
                # The != above requires *exactly* one of the two -- neither
                # present would otherwise reach the VALUES branch below and
                # KeyError on the missing %(archive_obs_id)s binding (not a
                # psycopg.Error, so the per-line except wouldn't catch it and
                # this whole import would crash instead of just skipping the
                # one bad line).
                logger.warning("skipping malformed submission (missing fields/bad outcome): %r", obj)
                n_skipped += 1
                continue

            raw_target_name = (obj.get("raw_target_name") or "").strip()
            # .get() rather than a bare key, so a line written before
            # proposed_bsc_hr_number existed (attach_bright_star wasn't a
            # valid outcome yet) still binds cleanly instead of KeyError-ing
            # this whole line's cur.execute() -- same reasoning as the
            # REQUIRED_TRIAGE_FIELDS comment above.
            params = {**obj, "raw_target_name": raw_target_name, "proposed_bsc_hr_number": obj.get("proposed_bsc_hr_number")}
            try:
                if raw_target_name:
                    # Expands one vote-by-name into every record the *live*
                    # spectroscopy_holdings table currently has under that
                    # (archive_code, name) and still marked 'skipped' -- not
                    # just whatever was true at submission time, so a vote
                    # on a big group still covers all of it even if more
                    # records under that name showed up since.
                    cur.execute(
                        """
                        INSERT INTO skip_classifications
                            (archive_code, archive_obs_id, outcome, proposed_gaia_source_id,
                             proposed_bsc_hr_number, gaia_cone_search_radius_arcsec,
                             gaia_cone_search_result, submitter, note, submitted_at)
                        SELECT h.archive_code, h.archive_obs_id, %(outcome)s, %(proposed_gaia_source_id)s,
                               %(proposed_bsc_hr_number)s, %(gaia_cone_search_radius_arcsec)s,
                               %(gaia_cone_search_result)s, %(submitter)s, %(note)s, %(submitted_at)s
                        FROM spectroscopy_holdings h
                        WHERE h.archive_code = %(archive_code)s AND h.match_status = 'skipped'
                          AND NULLIF(TRIM(h.raw_target_name), '') = %(raw_target_name)s
                        ON CONFLICT (archive_code, archive_obs_id, submitter) DO NOTHING
                        """,
                        params,
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO skip_classifications
                            (archive_code, archive_obs_id, outcome, proposed_gaia_source_id,
                             proposed_bsc_hr_number, gaia_cone_search_radius_arcsec,
                             gaia_cone_search_result, submitter, note, submitted_at)
                        VALUES (%(archive_code)s, %(archive_obs_id)s, %(outcome)s, %(proposed_gaia_source_id)s,
                                %(proposed_bsc_hr_number)s, %(gaia_cone_search_radius_arcsec)s,
                                %(gaia_cone_search_result)s, %(submitter)s, %(note)s, %(submitted_at)s)
                        ON CONFLICT (archive_code, archive_obs_id, submitter) DO NOTHING
                        """,
                        params,
                    )
            except psycopg.Error as exc:
                logger.warning("skipping submission Postgres rejected (%s): %r", exc, obj)
                n_skipped += 1
                continue
            n_inserted += cur.rowcount

    logger.info(
        "imported triage submissions from %s: %d line(s) read, %d new skip_classifications row(s), %d skipped",
        path, len(lines), n_inserted, n_skipped,
    )


def _apply_skip_classification_if_quorum(pg_conn: psycopg.Connection, archive_code: str, archive_obs_id: str, quorum: int = 2) -> None:
    """Design stub -- NOT called anywhere yet. Sketching the intended
    consensus step so it's clear what's deferred and why, per the crowdsourced-
    triage design notes. Lives here rather than in webapp.app because this
    is the only piece of the pipeline with live Postgres access at all now
    (see import_triage_submissions above and webapp.app's module docstring).

    Intended logic once wired up (e.g. right after import_triage_submissions
    processes a batch): tally this holding's not-yet-applied
    skip_classifications rows grouped by outcome; if any one outcome has
    >= `quorum` independent submitters agreeing:
      - attach_gaia_source: call ingest.add_star.add_star(pg_conn,
        proposed_gaia_source_id, input_name=...) (see add_star.py:188) to
        fetch-and-track the star if it isn't already tracked, then UPDATE the
        matching spectroscopy_holdings row to that star with
        match_status='matched', match_method='manual'.
      - attach_bright_star: call ingest.add_star.add_bsc_star(pg_conn,
        proposed_bsc_hr_number, input_name=...) (see add_star.py:275) --
        same idea as attach_gaia_source, but for a naked-eye star Gaia never
        saw, tracked via bsc_hr_number instead of gaia_source_id.
      - not_a_real_target / not_a_star: UPDATE spectroscopy_holdings.match_status =
        'rejected' (already a valid value in the match_status CHECK
        constraint -- see db/schema.sql). Both are terminal in the same way;
        kept as separate outcomes only so the vote itself stays informative
        (junk data vs. a real non-stellar object) rather than for any
        difference in how they're applied here.
      - confirmed_absent_from_gaia: no spectroscopy_holdings change (there's
        genuinely no gaia_source_id/star to assign) -- just mark applied so
        the row stops resurfacing in the /triage queue.
      - Mark every skip_classifications row for this holding applied_at =
        now(), not just the winning outcome's rows, so disagreeing/minority
        submissions don't linger as "pending" forever once a decision is made.

    Open design questions this stub deliberately leaves unresolved:
      - Exact quorum size (2 above is a placeholder) -- should it scale with
        total submission count, or require a margin over the runner-up
        outcome rather than a flat count?
      - What happens if two different outcomes both individually reach quorum
        (shouldn't happen if one-vote-per-submitter holds and quorum requires
        genuine agreement, but worth a real tie-break rule before this goes
        live, not just an assumption)?
      - Should a submitter ever be able to revise their own vote? Currently
        blocked outright by db/schema.sql's one-vote-per-submitter unique
        index -- deliberate for this sketch, but worth revisiting.
    """
    raise NotImplementedError("apply/quorum step is a design stub -- see docstring, not wired up yet")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", required=True, help="directory Apache serves, e.g. ~/public_html/spectra_data")
    args = parser.parse_args()

    out_dir = os.path.expanduser(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)
    os.chmod(out_dir, 0o755)

    database_url = os.environ["DATABASE_URL"]
    # Import first, so this run's triage_queue.parquet (built inside
    # export_tables) reflects any records that just got voted 'rejected'
    # or similar by a *future* apply step -- doesn't matter yet since that
    # step isn't wired up (see _apply_skip_classification_if_quorum), but
    # importing before exporting is the right order once it is.
    import_triage_submissions(database_url, out_dir)
    export_tables(database_url, out_dir)


if __name__ == "__main__":
    main()
