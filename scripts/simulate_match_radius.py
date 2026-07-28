"""One-off: before adding an entry to
sync.matcher.INSTRUMENT_MATCH_RADIUS_OVERRIDES_ARCSEC, simulate what widening
the positional match radius would actually do to an archive/instrument's
existing skipped/needs_review rows -- how many gain exactly one candidate
(a clean recoverable match) vs. more than one (newly ambiguous, needs_review)
vs. still zero, at each candidate radius. Read-only: reuses
sync.matcher's own candidate-loading/propagation so the simulation matches
production behavior exactly, but never writes to spectroscopy_holdings.

Usage:
    DATABASE_URL=postgresql:///spectra_local python3 -m scripts.simulate_match_radius \\
        --archive noirlab --instrument chiron --radii 20,30,45,60,90,120,150,200
"""

from __future__ import annotations

import argparse
import logging
import os
from collections import defaultdict
from datetime import date

import psycopg
from astropy import units as u
from astropy.coordinates import SkyCoord

from sync import matcher

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def simulate(conn: psycopg.Connection, archive_code: str, instrument: str, radii: list[float]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT raw_ra, raw_dec, obs_date
            FROM spectroscopy_holdings
            WHERE archive_code = %s AND instrument = %s
              AND match_status IN ('skipped', 'needs_review')
              AND match_method = 'positional_easy_match'
              AND raw_ra IS NOT NULL AND raw_dec IS NOT NULL AND obs_date IS NOT NULL
            """,
            (archive_code, instrument),
        )
        rows = cur.fetchall()
    logger.info("%d skipped/needs_review %s/%s rows with a position", len(rows), archive_code, instrument)
    if not rows:
        return

    max_radius = max(radii)
    by_epoch = defaultdict(list)
    for raw_ra, raw_dec, obs_date in rows:
        if not isinstance(obs_date, date):
            continue
        by_epoch[matcher._to_jyear(obs_date)].append((raw_ra, raw_dec))

    results = {r: {"zero": 0, "one": 0, "many": 0} for r in radii}
    for epoch, recs in by_epoch.items():
        ras = [r[0] for r in recs]
        decs = [r[1] for r in recs]
        targets = SkyCoord(ra=ras * u.deg, dec=decs * u.deg)

        max_years = abs(epoch - matcher.GAIA_DR3_REF_EPOCH)
        radius_deg = (max_radius + matcher.MAX_PM_ARCSEC_PER_YEAR * max_years) / 3600.0
        candidate_rows = matcher._load_candidate_stars(conn, ras, decs, radius_deg)
        if not candidate_rows:
            for r in radii:
                results[r]["zero"] += len(recs)
            continue

        ids, propagated = matcher._propagate(candidate_rows, epoch)
        idx_cat, idx_target, sep2d, _ = targets.search_around_sky(propagated, max_radius * u.arcsec)
        candidates = defaultdict(list)
        for cat_i, target_i, sep in zip(idx_cat, idx_target, sep2d):
            candidates[target_i].append(sep.arcsec)

        for i in range(len(recs)):
            seps = candidates.get(i, [])
            for r in radii:
                n_within = sum(1 for s in seps if s <= r)
                key = "zero" if n_within == 0 else "one" if n_within == 1 else "many"
                results[r][key] += 1

    logger.info("radius_arcsec\tstill_zero\tclean_one\tambiguous_many")
    for r in radii:
        res = results[r]
        logger.info("%s\t%s\t%s\t%s", r, res["zero"], res["one"], res["many"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--radii", default="20,30,45,60,90,120,150,200", help="comma-separated radii in arcsec")
    args = parser.parse_args()
    radii = [float(r) for r in args.radii.split(",")]

    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        simulate(conn, args.archive, args.instrument, radii)


if __name__ == "__main__":
    main()
