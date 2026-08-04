"""Gaia RVS — radial-velocity spectra, native to Gaia itself.

Every other archive here reports on stars that may or may not be tracked
yet; this one runs in reverse. has_rvs comes back on the same
gaiadr3.gaia_source row ingest.add_star already reads to register a star at
all (see stars.has_gaia_rvs), so there's no separate external source to
poll. fetch() instead walks the stars this project already tracks, in
star_id order, and reports one record per star with has_gaia_rvs set --
folding what used to be a special-cased INSERT straight into
spectroscopy_holdings inside ingest.add_star itself into the same
sync/archives/*.py + sync.runner pipeline (discover_stars/matcher/
archive_sync_state) every other archive goes through.

No external API involved, so the cursor is a star_id watermark over our own
`stars` table rather than anything archive-native -- it keeps advancing
indefinitely as every other archive's sync discovers new stars, the same
"paginate until no new rows" shape sync.main already expects.
"""

from __future__ import annotations

import os

import psycopg

from sync.base import RawObservation

RVS_DEEP_LINK = (
    "https://gea.esac.esa.int/data-server/data"
    "?RETRIEVAL_TYPE=RVS&ID=Gaia+DR3+{source_id}&DATA_STRUCTURE=INDIVIDUAL"
)

PAGE_SIZE = 50000


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    after_star_id = cursor.get("after_star_id", 0)
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT star_id, gaia_source_id
            FROM stars
            WHERE has_gaia_rvs AND star_id > %(after_star_id)s
            ORDER BY star_id
            LIMIT %(limit)s
            """,
            {"after_star_id": after_star_id, "limit": PAGE_SIZE},
        )
        rows = cur.fetchall()

    records = [
        RawObservation(
            archive_obs_id=str(gaia_source_id),
            archive_url=RVS_DEEP_LINK.format(source_id=gaia_source_id),
            instrument="Gaia RVS",
            gaia_source_id=gaia_source_id,
        )
        for _, gaia_source_id in rows
    ]
    new_cursor = {"after_star_id": rows[-1][0]} if rows else cursor
    return records, new_cursor
