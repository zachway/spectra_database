"""Production entry point: run every implemented archive sync to convergence.

Usage:
    python -m sync.main                              # all implemented archives
    python -m sync.main --only rave galah             # just these
    python -m sync.main --max-pages-per-archive 2      # cap pages, for debugging

Each archive's fetch() is called repeatedly until it returns no new records.
Paginated archives (eso, desi, sdss_v_optical's MJD watermark, ...) converge
to their current edge; static/gated archives (rave, galah, sdss_v_apogee's
one-shot pulls, carmenes) short-circuit to a no-op after their first run via
their own cursor. One archive failing doesn't stop the others — the error is
logged, the connection is rolled back so the failure can't poison the next
archive's transaction, and the driver moves on. Not-yet-implemented archives
(weave, four_most) aren't registered here at all — they have no public data.

gemini_ghost and gemini_igrins both need a GOA_SESSION_COOKIE env var (see
sync/archives/_goa_common.py) -- morgan is headless, so this means logging
into archive.gemini.edu in a browser elsewhere and copying the session
cookie value over by hand before including either in --only. Without it,
those archives fail (handled the same as any other failure) rather than
blocking the rest.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import psycopg

from sync.archives import (
    carmenes,
    carmenes_caha,
    cfht_cadc,
    dao,
    desi,
    eso,
    feros_gavo,
    flashheros_gavo,
    galah,
    gemini,
    gemini_ghost,
    gemini_igrins,
    koa,
    lamost,
    lbt,
    lick,
    mast,
    noirlab,
    rave,
    sdss_legacy_optical,
    sdss_v_apogee,
    sdss_v_optical,
)
from sync.runner import run_sync

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ARCHIVES = {
    "rave": rave.fetch,
    "galah": galah.fetch,
    "eso": eso.fetch,
    "cfht_cadc": cfht_cadc.fetch,
    "dao": dao.fetch,
    "gemini": gemini.fetch,
    "gemini_ghost": gemini_ghost.fetch,
    "gemini_igrins": gemini_igrins.fetch,
    "koa": koa.fetch,
    "lamost": lamost.fetch,
    "lbt": lbt.fetch,
    "lick": lick.fetch,
    "mast": mast.fetch,
    "noirlab": noirlab.fetch,
    "sdss_v_apogee": sdss_v_apogee.fetch,
    "sdss_v_optical": sdss_v_optical.fetch,
    "sdss_legacy_optical": sdss_legacy_optical.fetch,
    "carmenes": carmenes.fetch,
    "carmenes_caha": carmenes_caha.fetch,
    "desi": desi.fetch,
    "feros_gavo": feros_gavo.fetch,
    "flashheros_gavo": flashheros_gavo.fetch,
}


def sync_archive(conn: psycopg.Connection, archive_code: str, fetch_fn, max_pages: int | None = None) -> dict:
    totals: dict[str, int] = {}
    pages = 0
    while max_pages is None or pages < max_pages:
        counts = run_sync(conn, archive_code, fetch_fn)
        pages += 1
        for key, value in counts.items():
            totals[key] = totals.get(key, 0) + value
        logger.info("%s: page %d -> %s", archive_code, pages, counts)
        if sum(counts.values()) == 0:
            break
    logger.info("%s: done after %d page(s), totals: %s", archive_code, pages, totals)
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only", nargs="+", choices=sorted(ARCHIVES), help="run only these archives")
    parser.add_argument("--max-pages-per-archive", type=int, default=None, help="cap pages per archive (debugging)")
    args = parser.parse_args()

    archive_codes = args.only or sorted(ARCHIVES)

    failed = []
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        for archive_code in archive_codes:
            logger.info("%s: starting", archive_code)
            try:
                sync_archive(conn, archive_code, ARCHIVES[archive_code], args.max_pages_per_archive)
            except Exception:
                logger.exception("%s: failed", archive_code)
                conn.rollback()
                failed.append(archive_code)

    if failed:
        logger.error("archives failed: %s", ", ".join(failed))
        sys.exit(1)


if __name__ == "__main__":
    main()
