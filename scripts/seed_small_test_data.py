"""One-off: pull a bounded number of rows from every implemented archive and
seed a real, populated local test database — for laptop-scale testing (see
project to-do list), not a production sync (see sync.main for that).

Unlike sync.main, this does NOT require stars to already be tracked. It
discovers new tracked stars directly from each archive's own query results:

- Archives with a native Gaia column (or that resolve one internally, like
  carmenes.py): every returned row's gaia_source_id is trusted as a star
  outright (Gaia's own catalog is the "is this real" check already).
- Archives without one (eso, cfht_cadc, gemini, koa, mast, noirlab,
  sdss_legacy_optical): unique non-blank target_name values are batch-
  resolved via SIMBAD and kept only if SIMBAD's object type is stellar
  (code ends in "*" — see ingest.add_star.resolve_stellar_gaia_ids_batch).
  Rows with no usable name are left to positional matching against
  whatever's already tracked, same as a normal sync.

Both star-discovery paths are batched (a handful of Gaia/SIMBAD round trips
per archive, not one per row) — this is what makes ~2000 rows x 14 archives
tractable at all.

Usage:
    DATABASE_URL=postgresql:///spectra_local python3 -m scripts.seed_small_test_data --limit 2000
"""

import argparse
import logging
import os

import psycopg

from ingest.add_star import add_stars_batch, resolve_stellar_gaia_ids_batch
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

    known_aliases: dict[int, list[str]] = {}

    # Records that already carry a Gaia id (native column, or resolved
    # internally like carmenes.py) can still carry a useful raw_target_name
    # alongside it — cache that too, don't just add the star anonymously.
    direct = [r for r in records if r.gaia_source_id is not None]
    for r in direct:
        if r.raw_target_name:
            known_aliases.setdefault(r.gaia_source_id, []).append(r.raw_target_name)

    unnamed = [r for r in records if r.gaia_source_id is None]
    names = [r.raw_target_name for r in unnamed if r.raw_target_name]
    name_to_gaia: dict[str, int] = {}
    if names:
        try:
            name_to_gaia = resolve_stellar_gaia_ids_batch(names)
        except Exception:
            # SIMBAD outages happen (confirmed live during this project) —
            # degrade to direct-Gaia-only + positional matching against
            # whatever's already tracked, rather than losing the whole
            # archive's fetch to one dependency being briefly down.
            logger.warning("%s: SIMBAD resolution failed, continuing without it", archive_code, exc_info=True)
    for name, gaia_id in name_to_gaia.items():
        known_aliases.setdefault(gaia_id, []).append(name)

    all_ids = [r.gaia_source_id for r in direct] + list(name_to_gaia.values())
    stars_added = add_stars_batch(conn, all_ids, known_aliases=known_aliases)

    counts = matcher.match_records(conn, archive_code, records)
    logger.info(
        "%s: fetched %d rows, %d new stars, %d/%d unique names SIMBAD-confirmed as stellar -> %s",
        archive_code,
        len(records),
        stars_added,
        len(name_to_gaia),
        len(set(names)),
        counts,
    )
    return {"rows_fetched": len(records), "stars_added": stars_added, **counts}


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
