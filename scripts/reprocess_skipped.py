"""One-off: re-run discovery + matching for spectroscopy_holdings rows that
were written as 'skipped' during a transient dependency outage (e.g. SIMBAD
TAP being briefly unreachable during ingest.add_star.discover_stars — logged
as "SIMBAD resolution failed during star discovery, continuing without it").

Doesn't touch the archive's sync_cursor or re-fetch from the archive itself:
spectroscopy_holdings already stores everything sync.base.RawObservation
needs (raw_target_name, raw_ra/raw_dec, obs_date, instrument, program_id,
archive_url, archive_obs_id), so this just replays discover_stars + matcher
against the already-ingested rows in the affected window. Safe to re-run —
rows that get matched drop out of the 'skipped' filter, so an interrupted
run just picks up where it left off next time.

Usage:
    DATABASE_URL=postgresql:///spectra_local python3 -m scripts.reprocess_skipped \\
        eso --since "2026-07-23 17:15:16"
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime

import psycopg

from ingest.add_star import discover_stars
from sync import matcher
from sync.base import RawObservation

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CHUNK_SIZE = 2000


def _load_pending(conn: psycopg.Connection, archive_code: str, since: datetime) -> list[RawObservation]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT archive_obs_id, archive_url, instrument, obs_date, program_id,
                   raw_target_name, raw_ra, raw_dec
            FROM spectroscopy_holdings
            WHERE archive_code = %s AND match_status = 'skipped' AND updated_at >= %s
            """,
            (archive_code, since),
        )
        rows = cur.fetchall()
    records = []
    for archive_obs_id, archive_url, instrument, obs_date, program_id, raw_target_name, raw_ra, raw_dec in rows:
        records.append(
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
    return records


def reprocess(conn: psycopg.Connection, archive_code: str, since: datetime) -> dict:
    pending = _load_pending(conn, archive_code, since)
    logger.info("%s: %d skipped rows since %s to reprocess", archive_code, len(pending), since)

    totals: dict[str, int] = {}
    for i in range(0, len(pending), CHUNK_SIZE):
        chunk = pending[i : i + CHUNK_SIZE]
        discovery = discover_stars(conn, archive_code, chunk)
        counts = matcher.match_records(conn, archive_code, chunk)
        counts.update(discovery)
        for key, value in counts.items():
            totals[key] = totals.get(key, 0) + value
        logger.info("%s: reprocessed %d/%d -> %s", archive_code, min(i + CHUNK_SIZE, len(pending)), len(pending), counts)

    return totals


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("archive_code", help="e.g. eso")
    parser.add_argument("--since", required=True, help="ISO timestamp; only rows updated at/after this are retried")
    args = parser.parse_args()

    since = datetime.fromisoformat(args.since)
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        totals = reprocess(conn, args.archive_code, since)
    logger.info("%s: done, totals: %s", args.archive_code, totals)


if __name__ == "__main__":
    main()
