"""One-off: reprocess archive holdings that might resolve differently now
that a large batch of new stars (e.g. the full BSC5 catalog, via
scripts.seed_bright_star_catalog) has been added to `stars`.

Two distinct populations, reprocessed for two distinct reasons:

1. Every currently skipped/needs_review holding, across every archive, no
   time window -- the straightforward case: a record whose raw_target_name
   or position previously had no tracked-star candidate at all might now
   match one of the newly-added stars.

2. Existing MATCHED holdings (match_method='positional_easy_match') whose
   assigned star sits suspiciously close to one of the newly-added stars.
   This is the sharper risk, not just "might now match": a record that
   should have matched a bright star which wasn't tracked yet could have
   positionally matched some OTHER nearby tracked star instead -- most
   dangerously one of Gaia's own spurious detections near a bright/
   saturated source (diffraction-spike artifacts), if that spurious source
   happened to already be tracked. That holding shows up as 'matched'
   today, not 'skipped', so population #1 alone would never re-examine it.
   Found via a q3c self-join on stars(ra,dec) -- cheap, since it's
   new-star-count x nearby-old-star count, not new-star-count x
   all-holdings, so it doesn't need a new index on
   spectroscopy_holdings(raw_ra,raw_dec).

Both populations get replayed through the exact same
sync.matcher.match_records used for live syncs (same chunked,
incrementally-committed pattern as scripts.reprocess_skipped), so the same
identifier-before-position priority and ambiguity handling applies: a
genuinely better match wins, a newly-ambiguous one correctly demotes to
needs_review for a human to look at rather than silently staying wrong.

Population #1 at full catalog scale (millions of skipped/needs_review
rows) is a real, possibly many-hour job even chunked -- unlike the star_id
schema migration this doesn't hold one giant open transaction (each chunk
commits via matcher.match_records itself), so it's safe to let run and
safe to interrupt/resume by re-invoking (already-resolved rows just won't
still be 'skipped' on the next pass), but budget accordingly before
kicking it off against production.

Usage:
    DATABASE_URL=postgresql:///spectra_local python3 -m scripts.reprocess_against_new_stars \\
        --since "2026-07-25 11:00:00"
"""

from __future__ import annotations

import argparse
import logging
import os
from collections import defaultdict
from datetime import date, datetime

import psycopg

from ingest.add_star import discover_stars
from sync import matcher
from sync.base import RawObservation

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Much larger than scripts.reprocess_skipped's CHUNK_SIZE=2000 (fine at
# that script's usual scale: one archive, one outage window) -- at full
# catalog-sweep scale (the skipped/needs_review population alone is
# ~12.85M rows, confirmed live 2026-07-25), sync.matcher.match_records
# reloads and rebuilds its full star-alias dict (1.4M rows, confirmed live)
# from scratch on every single call, once per chunk. At CHUNK_SIZE=2000
# that's ~6,425 redundant 1.4M-row reads across one sweep -- almost
# entirely wasted work. A much bigger chunk cuts that by the same factor;
# 20000 keeps memory use for one chunk's RawObservation/SkyCoord arrays
# modest regardless.
CHUNK_SIZE = 20000

# How close an existing tracked star has to be to a newly-added one before
# its own positional-matched holdings get re-checked -- tight, not the
# 15-30" reconnaissance radius used to find BSC5-gap stars in the first
# place: this is specifically about "close enough to plausibly have stolen
# a 1" easy-match", not "anywhere in the general vicinity". Both stars'
# positions are propagated to a common epoch before this radius is applied
# (see _find_nearby_old_stars), so this only needs to cover coincidental
# closeness, not proper-motion drift -- a little slack above
# EASY_MATCH_RADIUS_ARCSEC is enough.
NEARBY_OLD_STAR_RADIUS_ARCSEC = 5.0


def _rows_to_records_by_archive(rows: list[tuple]) -> dict[str, list[RawObservation]]:
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


def _load_all_pending(conn: psycopg.Connection) -> dict[str, list[RawObservation]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT archive_code, archive_obs_id, archive_url, instrument, obs_date, program_id,
                   raw_target_name, raw_ra, raw_dec
            FROM spectroscopy_holdings
            WHERE match_status IN ('skipped', 'needs_review')
            """
        )
        rows = cur.fetchall()
    return _rows_to_records_by_archive(rows)


def _find_nearby_old_stars(conn: psycopg.Connection, since: datetime) -> list[int]:
    """star_id of every pre-existing (added before `since`) tracked star
    within NEARBY_OLD_STAR_RADIUS_ARCSEC of any newly-added one -- see
    module docstring's population #2.

    Comparing stars' raw stored (ra, dec) directly here would be wrong for
    exactly the reason this whole script exists: a high-PM star's stored
    position (BSC5-sourced stars use ref_epoch=1991.25, Gaia-sourced ones
    2016.0) can differ from where it actually was by many arcseconds --
    confirmed live via HR 15 (pmra/pmdec ~137/-163 mas/yr, ~7.5" of drift
    between its ref_epoch and a 2026 observation date). A radius wide
    enough to cover worst-case multi-decade PM drift would make "nearby"
    meaningless (hundreds of arcsec). Instead, every newly-added star's
    position is propagated (via the exact same sync.matcher._propagate used
    for live positional matching) to each distinct ref_epoch actually
    present among the old stars, so the comparison happens at a shared
    reference epoch -- then NEARBY_OLD_STAR_RADIUS_ARCSEC only needs to
    cover "coincidentally close by chance", the same kind of tolerance
    EASY_MATCH_RADIUS_ARCSEC already relies on elsewhere.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT star_id, ra, dec, ref_epoch, pmra, pmdec FROM stars WHERE added_at >= %s",
            (since,),
        )
        new_stars = cur.fetchall()
        if not new_stars:
            return []

        cur.execute("SELECT DISTINCT ref_epoch FROM stars WHERE added_at < %s", (since,))
        old_ref_epochs = [row[0] for row in cur.fetchall()]

    radius_deg = NEARBY_OLD_STAR_RADIUS_ARCSEC / 3600.0
    nearby: set[int] = set()
    for old_epoch in old_ref_epochs:
        _, propagated = matcher._propagate(new_stars, old_epoch)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT old.star_id
                FROM unnest(%(ra)s::float8[], %(dec)s::float8[]) AS t(ra, dec)
                JOIN stars old ON old.added_at < %(since)s AND old.ref_epoch = %(old_epoch)s
                    AND q3c_join(t.ra, t.dec, old.ra, old.dec, %(radius_deg)s)
                """,
                {
                    "ra": propagated.ra.deg.tolist(),
                    "dec": propagated.dec.deg.tolist(),
                    "since": since,
                    "old_epoch": old_epoch,
                    "radius_deg": radius_deg,
                },
            )
            nearby.update(row[0] for row in cur.fetchall())

    return sorted(nearby)


def _load_matched_near_new_stars(conn: psycopg.Connection, since: datetime) -> dict[str, list[RawObservation]]:
    nearby_old_star_ids = _find_nearby_old_stars(conn, since)
    logger.info(
        "%d pre-existing stars sit within %g\" of a newly-added star",
        len(nearby_old_star_ids), NEARBY_OLD_STAR_RADIUS_ARCSEC,
    )
    if not nearby_old_star_ids:
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT archive_code, archive_obs_id, archive_url, instrument, obs_date, program_id,
                   raw_target_name, raw_ra, raw_dec
            FROM spectroscopy_holdings
            WHERE match_status = 'matched' AND match_method = 'positional_easy_match'
              AND star_id = ANY(%s)
            """,
            (nearby_old_star_ids,),
        )
        rows = cur.fetchall()
    return _rows_to_records_by_archive(rows)


def _reprocess_batch(conn: psycopg.Connection, by_archive: dict[str, list[RawObservation]], label: str) -> dict:
    totals: dict[str, int] = {}
    total_candidates = sum(len(v) for v in by_archive.values())
    logger.info("%s: %d candidates across %d archives", label, total_candidates, len(by_archive))

    for archive_code, records in by_archive.items():
        for i in range(0, len(records), CHUNK_SIZE):
            chunk = records[i : i + CHUNK_SIZE]
            discovery = discover_stars(conn, archive_code, chunk)
            counts = matcher.match_records(conn, archive_code, chunk)
            counts.update(discovery)
            for key, value in counts.items():
                totals[key] = totals.get(key, 0) + value
            logger.info(
                "%s/%s: reprocessed %d/%d -> %s",
                label, archive_code, min(i + CHUNK_SIZE, len(records)), len(records), counts,
            )
    return totals


def reprocess(conn: psycopg.Connection, since: datetime) -> dict:
    skipped_totals = _reprocess_batch(conn, _load_all_pending(conn), "skipped/needs_review sweep")
    near_new_totals = _reprocess_batch(conn, _load_matched_near_new_stars(conn, since), "near-new-star recheck")
    return {"skipped_sweep": skipped_totals, "near_new_star_recheck": near_new_totals}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--since", required=True,
        help="ISO timestamp; stars with added_at at/after this are treated as 'new' for the near-new-star recheck",
    )
    args = parser.parse_args()

    since = datetime.fromisoformat(args.since)
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        totals = reprocess(conn, since)
    logger.info("done: %s", totals)


if __name__ == "__main__":
    main()
