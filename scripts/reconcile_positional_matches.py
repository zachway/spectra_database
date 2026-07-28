"""One-off: re-check existing positional-match failures (skipped/needs_review)
for archive/instrument pairs in sync.matcher.INSTRUMENT_MATCH_RADIUS_OVERRIDES_ARCSEC
against the now-wider match radius (see that dict's docstring for why, e.g.
noirlab/chiron's confirmed real pointing-model offset).

A normal sync run won't touch these rows -- each archive's fetch() advances a
cursor past dateobs ranges it's already pulled, so historical records that
were previously matched (successfully or not) are never re-fetched. This
replays sync.matcher.match_records directly against what's already stored,
the same way scripts/reconcile_name_matches.py does for name matches.

Only rows with match_method='positional_easy_match' and match_status in
('skipped', 'needs_review') are touched -- a 'matched' positional row already
found its star within the old (or overridden) radius and re-checking it can't
improve on that, only needs_review could jump to matched or vice versa if the
wider candidate radius pulls in an extra star.

Usage:
    DATABASE_URL=postgresql:///spectra_local python3 -m scripts.reconcile_positional_matches
    # scoped to one archive/instrument (faster, e.g. for a quick before/after check):
    DATABASE_URL=postgresql:///spectra_local python3 -m scripts.reconcile_positional_matches --archive noirlab --instrument chiron
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


def _load_candidates(
    conn: psycopg.Connection, archive_filter: str | None, instrument_filter: str | None
) -> dict[str, list[RawObservation]]:
    targets = list(matcher.INSTRUMENT_MATCH_RADIUS_OVERRIDES_ARCSEC.keys())
    if archive_filter or instrument_filter:
        targets = [
            (a, i) for a, i in targets
            if (archive_filter is None or a == archive_filter)
            and (instrument_filter is None or i == instrument_filter)
        ]
    if not targets:
        return {}

    by_archive: dict[str, list[RawObservation]] = defaultdict(list)
    with conn.cursor() as cur:
        for archive_code, instrument in targets:
            cur.execute(
                """
                SELECT archive_obs_id, archive_url, instrument, obs_date, program_id,
                       raw_target_name, raw_ra, raw_dec
                FROM spectroscopy_holdings
                WHERE archive_code = %s AND instrument = %s
                  AND match_method = 'positional_easy_match'
                  AND match_status IN ('skipped', 'needs_review')
                  AND raw_ra IS NOT NULL AND raw_dec IS NOT NULL
                """,
                (archive_code, instrument),
            )
            for archive_obs_id, archive_url, rec_instrument, obs_date, program_id, raw_target_name, raw_ra, raw_dec in cur.fetchall():
                by_archive[archive_code].append(
                    RawObservation(
                        archive_obs_id=archive_obs_id,
                        archive_url=archive_url,
                        instrument=rec_instrument,
                        obs_date=obs_date if isinstance(obs_date, date) else None,
                        program_id=program_id,
                        gaia_source_id=None,
                        ra=raw_ra,
                        dec=raw_dec,
                        raw_target_name=raw_target_name,
                    )
                )
    return by_archive


def reconcile(conn: psycopg.Connection, archive_filter: str | None = None, instrument_filter: str | None = None) -> dict:
    by_archive = _load_candidates(conn, archive_filter, instrument_filter)
    total_candidates = sum(len(v) for v in by_archive.values())
    logger.info("%d existing skipped/needs_review positional rows to re-check across %d archives", total_candidates, len(by_archive))

    totals = {"candidates": 0, "newly_matched": 0, "changed": 0}
    for archive_code, records in by_archive.items():
        for i in range(0, len(records), CHUNK_SIZE):
            chunk = records[i : i + CHUNK_SIZE]
            obs_ids = [r.archive_obs_id for r in chunk]

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT archive_obs_id, star_id, match_status FROM spectroscopy_holdings "
                    "WHERE archive_code = %s AND archive_obs_id = ANY(%s)",
                    (archive_code, obs_ids),
                )
                before = {row[0]: (row[1], row[2]) for row in cur.fetchall()}

            matcher.match_records(conn, archive_code, chunk)

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT archive_obs_id, star_id, match_status FROM spectroscopy_holdings "
                    "WHERE archive_code = %s AND archive_obs_id = ANY(%s)",
                    (archive_code, obs_ids),
                )
                after = cur.fetchall()

            changed = [row for row in after if before.get(row[0]) != (row[1], row[2])]
            newly_matched = [row for row in changed if row[2] == "matched"]
            for archive_obs_id, star_id, status in changed:
                prev_star_id, prev_status = before.get(archive_obs_id, (None, None))
                logger.info(
                    "%s/%s: %s/star_id=%s -> %s/star_id=%s",
                    archive_code, archive_obs_id, prev_status, prev_star_id, status, star_id,
                )

            totals["candidates"] += len(chunk)
            totals["changed"] += len(changed)
            totals["newly_matched"] += len(newly_matched)
            logger.info(
                "%s: re-checked %d/%d (%d changed, %d newly matched so far)",
                archive_code, min(i + CHUNK_SIZE, len(records)), len(records), totals["changed"], totals["newly_matched"],
            )

    return totals


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--archive", default=None, help="only re-check this archive_code (e.g. noirlab)")
    parser.add_argument("--instrument", default=None, help="only re-check this instrument (e.g. chiron)")
    args = parser.parse_args()

    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        totals = reconcile(conn, archive_filter=args.archive, instrument_filter=args.instrument)
    logger.info("done: %s", totals)


if __name__ == "__main__":
    main()
