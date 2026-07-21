"""RAVE DR6 — VizieR TAP, direct Gaia EDR3 source_id column.

Final, static data release (RAVE observations ended 2013-04-04) — one full
pull is enough forever, so the fetch is a no-op once the cursor marks it done.
~1030 of 518,387 rows (~0.2%) lack a Gaia match; those are skipped rather than
falling back to positional matching, not worth the complexity for this small
a fraction.
"""

import numpy as np
from astropy.time import Time

from sync.base import RawObservation, make_tap_service

TAP_URL = "https://tapvizier.cds.unistra.fr/TAPVizieR/tap"

QUERY = """
SELECT x."ObsID", x."Gaiae3", r."Obs.date", s."FileName"
FROM "III/283/xgaiae3" AS x
JOIN "III/283/ravedr6" AS r ON x."ObsID" = r."ObsID"
JOIN "III/283/spectra" AS s ON x."ObsID" = s."ObsID"
"""

SPECTRUM_BASE_URL = "https://cdsarc.cds.unistra.fr/ftp/III/283/sp/"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    if cursor.get("synced_at"):
        return [], cursor

    tap = make_tap_service(TAP_URL)
    table = tap.search(QUERY).to_table()

    records = []
    for row in table:
        if np.ma.is_masked(row["Gaiae3"]):
            continue
        obs_date = Time(float(row["Obs_date"]), format="jd").to_datetime().date()
        records.append(
            RawObservation(
                archive_obs_id=str(row["ObsID"]),
                archive_url=SPECTRUM_BASE_URL + str(row["FileName"]),
                instrument="RAVE",
                obs_date=obs_date,
                gaia_source_id=int(row["Gaiae3"]),
            )
        )

    new_cursor = {"synced_at": Time.now().isot, "row_count": len(records)}
    return records, new_cursor
