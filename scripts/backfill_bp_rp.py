"""One-off: backfill phot_bp_mean_mag/phot_rp_mean_mag for stars added
before those columns existed.

Doesn't touch anything else — a dedicated re-query of Gaia by
gaia_source_id, same batched-TAP-query pattern as
ingest.add_star.add_stars_batch, but scoped to just these two columns so
it's safe to run against the full tracked-star catalog (1M+ rows and
growing) without re-doing any of the archive sync/matching work. Resumable:
only selects rows where both columns are still NULL, so a re-run after a
partial run (or a Gaia TAP outage mid-way) just picks up where it left
off — and doesn't keep re-querying stars where Gaia itself has no BP or RP
measurement (a real, permanent null, not a "not backfilled yet" one).

Usage:
    DATABASE_URL=postgresql:///spectra_local python3 -m scripts.backfill_bp_rp
"""

from __future__ import annotations

import logging
import os

import psycopg
from astroquery.gaia import Gaia

from sync.base import clean_float

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CHUNK_SIZE = 500

BP_RP_QUERY = """
SELECT source_id, phot_bp_mean_mag, phot_rp_mean_mag
FROM gaiadr3.gaia_source
WHERE source_id IN ({id_list})
"""


def backfill(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT gaia_source_id FROM stars WHERE phot_bp_mean_mag IS NULL AND phot_rp_mean_mag IS NULL")
        pending = [row[0] for row in cur.fetchall()]

    logger.info("%d stars missing bp/rp", len(pending))
    updated = 0
    for i in range(0, len(pending), CHUNK_SIZE):
        chunk = pending[i : i + CHUNK_SIZE]
        id_list = ",".join(str(sid) for sid in chunk)
        job = Gaia.launch_job(BP_RP_QUERY.format(id_list=id_list))
        table = job.get_results()

        with conn.cursor() as cur:
            for row in table:
                cur.execute(
                    "UPDATE stars SET phot_bp_mean_mag = %s, phot_rp_mean_mag = %s WHERE gaia_source_id = %s",
                    (clean_float(row["phot_bp_mean_mag"]), clean_float(row["phot_rp_mean_mag"]), int(row["source_id"])),
                )
        conn.commit()
        updated += len(table)
        logger.info("backfilled %d/%d", min(i + CHUNK_SIZE, len(pending)), len(pending))

    return updated


def main() -> None:
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        total = backfill(conn)
    logger.info("done, %d stars updated", total)


if __name__ == "__main__":
    main()
