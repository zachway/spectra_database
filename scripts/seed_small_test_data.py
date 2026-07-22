"""One-off: pull a bounded number of rows from every implemented archive and
seed a real, populated local test database — for laptop-scale testing (see
project to-do list), not a production sync (see sync.main for that).

Star discovery (ingest.add_star.discover_stars, shared with sync.runner so
production syncs and this seeder can't diverge in what counts as a new
star) is batched — a handful of Gaia/SIMBAD round trips per archive, not one
per row — which is what makes ~2000 rows x 14 archives tractable at all in
one run. The --limit flag exists only to keep that batch small for fast
local testing; sync.main has no such cap and relies on discover_stars
scaling incrementally, one page at a time, as it converges through each
archive's full history.

Usage:
    DATABASE_URL=postgresql:///spectra_local python3 -m scripts.seed_small_test_data --limit 2000
"""

import argparse
import logging
import os

import psycopg

from ingest.add_star import discover_stars
from sync import matcher
from sync.archives import (
    carmenes,
    cfht_cadc,
    desi,
    eso,
    galah,
    gemini,
    koa,
    lamost,
    lbt,
    mast,
    noirlab,
    rave,
    sdss_legacy_optical,
    sdss_v_apogee,
    sdss_v_optical,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Archives with a module-level PAGE_SIZE we can shrink before calling fetch().
PAGE_SIZE_ARCHIVES = {
    "eso": eso,
    "cfht_cadc": cfht_cadc,
    "koa": koa,
    "lamost": lamost,
    "lbt": lbt,
    "mast": mast,
    "noirlab": noirlab,
    "sdss_legacy_optical": sdss_legacy_optical,
    "sdss_v_apogee": sdss_v_apogee,
}
# Same idea, different attribute name.
ROWS_PER_PAGE_ARCHIVES = {"desi": desi}
# No adjustable cap at all — galah has no maxrec/TOP on its query (pyvo's own
# default silently applies, ~20000, same class of issue fixed in eso.py
# earlier but not worth re-touching production code just for this script);
# gemini has a fixed maxrec=20000 per 7-day window, unrelated to row count.
# Both are naturally bounded enough for laptop-scale testing as they stand.
NO_CAP_ARCHIVES = {"galah": galah, "gemini": gemini}
# One-shot full-pull archives — no partial-pull mode at all, fetch returns
# everything in one query (RAVE ~518K rows, CARMENES 362) and _fetch_records
# truncates to `limit` after, same as the other categories.
FULL_PULL = {"rave": rave, "carmenes": carmenes}
# Bulk-file archive with no page-size knob — truncate the returned records
# after the (unavoidable) full-file download instead.
BULK_FILE = {"sdss_v_optical": sdss_v_optical}

ALL_ARCHIVES = {
    **PAGE_SIZE_ARCHIVES,
    **ROWS_PER_PAGE_ARCHIVES,
    **NO_CAP_ARCHIVES,
    **FULL_PULL,
    **BULK_FILE,
}
# gemini_ghost and gemini_igrins deliberately excluded -- both need a real,
# manually-obtained GOA_SESSION_COOKIE (see sync/archives/_goa_common.py),
# which a scripted local test seed can't provide.


def _fetch_records(archive_code: str, module, limit: int) -> list:
    if archive_code in PAGE_SIZE_ARCHIVES:
        original = module.PAGE_SIZE
        module.PAGE_SIZE = limit
        try:
            records, _ = module.fetch({})
        finally:
            module.PAGE_SIZE = original
        return records
    if archive_code in ROWS_PER_PAGE_ARCHIVES:
        original = module.ROWS_PER_PAGE
        module.ROWS_PER_PAGE = limit
        try:
            records, _ = module.fetch({})
        finally:
            module.ROWS_PER_PAGE = original
        return records
    if archive_code in NO_CAP_ARCHIVES:
        # galah's query has no TOP/maxrec at all — an empty cursor pulls its
        # full ~1.08M-row catalog. Truncating here (not just for display)
        # matters a lot: unsliced, this would feed >1M raw ids into
        # add_stars_batch's chunking loop, turning ~4 batched Gaia queries
        # into ~2000+ (confirmed live — this is what actually made the first
        # run of this script hang, not galah.fetch() itself).
        records, _ = module.fetch({})
        return records[:limit]
    if archive_code in BULK_FILE:
        records, _ = module.fetch({})
        return records[:limit]
    # FULL_PULL: no partial-pull mode at all, take whatever comes back and
    # truncate after. CARMENES is genuinely small (362) so this is a no-op
    # there, but RAVE's "one query" is ~518K rows — confirmed live that,
    # left unsliced, that many direct Gaia ids blow add_stars_batch's
    # 500-per-chunk loop out to 1000+ sequential Gaia TAP calls (this is
    # what actually hung the run, same class of bug as the galah fix above,
    # the "naturally small" assumption below was wrong for RAVE specifically).
    records, _ = module.fetch({})
    return records[:limit]


def seed_archive(conn: psycopg.Connection, archive_code: str, module, limit: int) -> dict:
    records = _fetch_records(archive_code, module, limit)

    discovery = discover_stars(conn, archive_code, records)

    counts = matcher.match_records(conn, archive_code, records)
    logger.info("%s: fetched %d rows -> %s | %s", archive_code, len(records), discovery, counts)
    return {"rows_fetched": len(records), **discovery, **counts}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, default=2000, help="rows to pull per archive")
    parser.add_argument("--only", nargs="+", choices=sorted(ALL_ARCHIVES), help="run only these archives")
    args = parser.parse_args()

    archive_codes = args.only or sorted(ALL_ARCHIVES)

    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        for archive_code in archive_codes:
            logger.info("%s: starting", archive_code)
            try:
                seed_archive(conn, archive_code, ALL_ARCHIVES[archive_code], args.limit)
            except Exception:
                logger.exception("%s: failed", archive_code)
                conn.rollback()


if __name__ == "__main__":
    main()
