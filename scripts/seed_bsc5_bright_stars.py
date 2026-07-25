"""One-off: seed the naked-eye stars Gaia can't see at all.

Gaia saturates on the very brightest stars in the sky. A cross-match of the
Yale Bright Star Catalogue (BSC5, via VizieR's V/50/catalog) against
gaiadr3.gaia_source (30" radius, accounting for stale BSC positions plus
real proper motion on nearby bright stars) found that of the 170 BSC5 stars
brighter than V=3, these 70 have no credible Gaia counterpart: 18 with zero
Gaia sources within 30" at all, another 52 where the closest candidate is
>3 mag fainter than expected (almost certainly an unrelated neighbor, not
the star itself — Gaia often has spurious faint detections near bright
stars from diffraction spikes). Past V=3 the effect drops off fast (1.3%
of BSC5 stars in 3<=V<5 show the same pattern), so this list -- run once,
live, 2026-07-25 -- isn't expected to need much revisiting.

Arcturus is HR 5340, in this list.

Usage:
    DATABASE_URL=postgresql:///spectra_local python3 -m scripts.seed_bsc5_bright_stars
"""

from __future__ import annotations

import logging
import os

import psycopg

from ingest.add_star import add_bsc_star

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# HR (Harvard Revised / Bright Star) numbers confirmed live to have no
# credible gaiadr3.gaia_source counterpart within 30" — see module
# docstring for the cross-match that produced this list.
BSC5_HR_NUMBERS_MISSING_FROM_GAIA = [
    15, 98, 188, 337, 424, 472, 553, 617, 911, 936, 1017, 1457, 1708, 1713,
    1790, 1791, 1903, 1948, 2061, 2088, 2095, 2294, 2326, 2421, 2491, 2618,
    2693, 2943, 2990, 3207, 3307, 3485, 3634, 3685, 3748, 3982, 4057, 4301,
    4534, 4662, 4730, 4731, 4763, 4853, 5056, 5132, 5231, 5267, 5288, 5340,
    5440, 5563, 5953, 5958, 6134, 6217, 6508, 6527, 6553, 6705, 6879, 7001,
    7121, 7557, 7790, 7924, 8308, 8636, 8728, 8775,
]


def seed(conn: psycopg.Connection) -> int:
    added = 0
    for hr in BSC5_HR_NUMBERS_MISSING_FROM_GAIA:
        try:
            star = add_bsc_star(conn, hr)
            logger.info("HR %d -> star_id %d (%s)", hr, star["star_id"], star["name_aliases"][:1] or "no alias")
            added += 1
        except Exception:
            logger.warning("HR %d failed", hr, exc_info=True)
    return added


def main() -> None:
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        total = seed(conn)
    logger.info("done: %d/%d stars added", total, len(BSC5_HR_NUMBERS_MISSING_FROM_GAIA))


if __name__ == "__main__":
    main()
