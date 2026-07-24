"""One-off: re-check existing 'name_resolved' matches against the position
sanity check added to sync.matcher (see NAME_MATCH_SANITY_RADIUS_ARCSEC and
the module docstring's "Mira" case) -- "Mira" is SIMBAD's own proper name for
omicron Ceti *and* an informal class label for any Mira-type long-period
variable, so any archive record that used "Mira" generically for some other
physical star was getting silently merged onto omicron Ceti's
gaia_source_id, with no position check to catch it. The same risk applies to
any other ambiguous alias, not just "Mira" -- this defaults to re-checking
every name_resolved match, not just that one name.

Only rows with match_method='name_resolved', match_status='matched', and a
raw_ra/raw_dec on file can possibly be affected -- rows with no reported
position never had anything to sanity-check and are left untouched. Safe to
re-run: replays the exact same sync.matcher.match_records path real syncs
use, keyed on each row's already-stored raw_target_name/raw_ra/raw_dec, so a
row that still passes the sanity check comes back byte-for-byte identical
(ON CONFLICT DO UPDATE with the same values). Rows that fail it get
reassigned via the normal positional fallback -- matched to whichever
tracked star is actually nearby, needs_review if that's ambiguous, or
skipped if nothing tracked is nearby at all.

Usage:
    DATABASE_URL=postgresql:///spectra_local python3 -m scripts.reconcile_name_matches
    # scoped to one suspect name (faster, e.g. for a quick before/after check):
    DATABASE_URL=postgresql:///spectra_local python3 -m scripts.reconcile_name_matches --name Mira
"""

from __future__ import annotations

import argparse
import logging
import os
from collections import defaultdict
from datetime import date

import psycopg

from sync import matcher
from sync.base import RawObservation

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CHUNK_SIZE = 2000


def _load_candidates(conn: psycopg.Connection, name_filter: str | None) -> dict[str, list[RawObservation]]:
    query = """
        SELECT archive_code, archive_obs_id, archive_url, instrument, obs_date, program_id,
               raw_target_name, raw_ra, raw_dec
        FROM spectroscopy_holdings
        WHERE match_status = 'matched' AND match_method = 'name_resolved'
          AND raw_ra IS NOT NULL AND raw_dec IS NOT NULL
    """
    params: list = []
    if name_filter:
        query += " AND raw_target_name = %s"
        params.append(name_filter)

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    by_archive: dict[str, list[RawObservation]] = defaultdict(list)
    for archive_code, archive_obs_id, archive_url, instrument, obs_date, program_id, raw_target_name, raw_ra, raw_dec in rows:
        by_archive[archive_code].append(
            RawObservation(
                archive_obs_id=archive_obs_id,
                archive_url=archive_url,
                instrument=instrument,
                obs_date=obs_date if isinstance(obs_date, date) else None,
                program_id=program_id,
                gaia_source_id=None,
                ra=raw_ra,
                dec=raw_dec,
                raw_target_name=raw_target_name,
            )
        )
    return by_archive


def reconcile(conn: psycopg.Connection, name_filter: str | None = None) -> dict:
    by_archive = _load_candidates(conn, name_filter)
    total_candidates = sum(len(v) for v in by_archive.values())
    logger.info("%d existing name_resolved matches to re-check across %d archives", total_candidates, len(by_archive))

    totals = {"candidates": 0, "reassigned": 0}
    for archive_code, records in by_archive.items():
        for i in range(0, len(records), CHUNK_SIZE):
            chunk = records[i : i + CHUNK_SIZE]
            obs_ids = [r.archive_obs_id for r in chunk]

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT archive_obs_id, gaia_source_id FROM spectroscopy_holdings "
                    "WHERE archive_code = %s AND archive_obs_id = ANY(%s)",
                    (archive_code, obs_ids),
                )
                before = dict(cur.fetchall())

            matcher.match_records(conn, archive_code, chunk)

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT archive_obs_id, gaia_source_id, match_method, match_status FROM spectroscopy_holdings "
                    "WHERE archive_code = %s AND archive_obs_id = ANY(%s)",
                    (archive_code, obs_ids),
                )
                after = cur.fetchall()

            reassigned = [row for row in after if before.get(row[0]) != row[1]]
            for archive_obs_id, gaia_source_id, method, status in reassigned:
                logger.info(
                    "%s/%s: gaia_source_id %s -> %s (now %s/%s)",
                    archive_code, archive_obs_id, before.get(archive_obs_id), gaia_source_id, method, status,
                )

            totals["candidates"] += len(chunk)
            totals["reassigned"] += len(reassigned)
            logger.info(
                "%s: re-checked %d/%d (%d reassigned so far)",
                archive_code, min(i + CHUNK_SIZE, len(records)), len(records), totals["reassigned"],
            )

    return totals


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--name", default=None, help="only re-check matches whose raw_target_name equals this (e.g. Mira)")
    args = parser.parse_args()

    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        totals = reconcile(conn, name_filter=args.name)
    logger.info("done: %s", totals)


if __name__ == "__main__":
    main()
