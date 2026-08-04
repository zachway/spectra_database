"""SDSS-V APOGEE — SkyServer CAS SQL, direct Gaia EDR3 source_id column.

apStar is the per-star *combined* spectrum (cumulative across visits and
across SDSS generations, per project decision — one row per star here, not
per-visit, so no obs_date). apstar_id isn't numeric (it's a composite string
like "apogee.apo1m.stars.Bestars.2M...") so pagination watermarks on
apogee_id (the 2MASS-style id) instead, which sorts consistently even if not
chronologically.

reduction_status is hardcoded 'reduced' -- apStar is the pipeline-combined,
flux/wavelength-calibrated spectrum (redux/dr17 in the URL above names the
reduction version), never a raw CCD frame; SDSS has no public raw-frame
distribution path at all.
"""

import requests

from sync.base import RawObservation

SQL_URL = "https://skyserver.sdss.org/dr20/SkyServerWS/SearchTools/SqlSearch"

QUERY = """
SELECT TOP {page_size} apogee_id, telescope, field, [file], gaiaedr3_source_id
FROM apogeeStar
WHERE gaiaedr3_source_id IS NOT NULL AND apogee_id > '{last_apogee_id}'
ORDER BY apogee_id ASC
"""

PAGE_SIZE = 50000

# Confirmed live against DR20: dr17 is still the reduction pipeline version,
# independent of the DR19/DR20 data-release path it's served under.
SPECTRUM_URL = "https://data.sdss.org/sas/dr20/spectro/apogee/redux/dr17/stars/{telescope}/{field}/{file}"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_apogee_id = cursor.get("last_apogee_id", "")

    resp = requests.get(
        SQL_URL,
        params={"cmd": QUERY.format(page_size=PAGE_SIZE, last_apogee_id=last_apogee_id), "format": "json"},
        timeout=120,
    )
    resp.raise_for_status()
    rows = resp.json()[0]["Rows"]

    records = []
    max_apogee_id = last_apogee_id
    for row in rows:
        apogee_id = row["apogee_id"]
        max_apogee_id = max(max_apogee_id, apogee_id)
        records.append(
            RawObservation(
                archive_obs_id=apogee_id,
                archive_url=SPECTRUM_URL.format(telescope=row["telescope"], field=row["field"], file=row["file"]),
                instrument="APOGEE",
                gaia_source_id=int(row["gaiaedr3_source_id"]),
                reduction_status="reduced",
            )
        )

    return records, {"last_apogee_id": max_apogee_id}
