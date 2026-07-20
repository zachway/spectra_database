"""GALAH DR4 — DataCentral TAP, direct Gaia DR3 source_id column.

galah_dr4.mainspectable carries essentially 100% Gaia DR3 coverage (verified:
1,085,520 / 1,085,520 rows non-null), so no positional fallback is needed —
any null gaiadr3_source_id is just skipped defensively.

Incremental via an mjd watermark: GALAH DR4 is itself a fixed release (not a
live-growing table), so this mostly matters for re-runs after a future DR.
"""

import numpy as np
import pyvo
from astropy.time import Time

from sync.base import RawObservation

TAP_URL = "https://datacentral.org.au/vo/tap"

QUERY = """
SELECT sobject_id, gaiadr3_source_id, mjd
FROM galah_dr4.mainspectable
WHERE mjd > {last_mjd}
"""

# Confirmed live: returns a real FITS file. FILT=B (Blue/CCD1) is one of four
# camera bands (B/G/R/I) per sobject_id — a representative pointer, not
# exhaustive; the full per-camera listing lives behind the same slink service.
DEEP_LINK = (
    "https://datacentral.org.au/vo/slink/links"
    "?ID={sobject_id}&DR=galah_dr4&IDX=0&FILT=B&RESPONSEFORMAT=fits"
)


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_mjd = cursor.get("last_mjd", 0)

    tap = pyvo.dal.TAPService(TAP_URL)
    table = tap.search(QUERY.format(last_mjd=last_mjd)).to_table()

    records = []
    max_mjd = last_mjd
    for row in table:
        if np.ma.is_masked(row["gaiadr3_source_id"]):
            continue
        mjd = float(row["mjd"])
        max_mjd = max(max_mjd, mjd)
        sobject_id = str(row["sobject_id"])
        records.append(
            RawObservation(
                archive_obs_id=sobject_id,
                archive_url=DEEP_LINK.format(sobject_id=sobject_id),
                instrument="GALAH (HERMES)",
                obs_date=Time(mjd, format="mjd").to_datetime().date(),
                gaia_source_id=int(row["gaiadr3_source_id"]),
            )
        )

    return records, {"last_mjd": max_mjd}
