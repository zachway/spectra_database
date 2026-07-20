"""SDSS Legacy Optical (BOSS/eBOSS, pre-SDSS-V) — SkyServer CAS SQL.

Confirmed real, not just bookkeeping: legacy specObj caps at MJD 58932
(~2020) with zero SDSS-V rows — SDSS-V optical lives in a separate table
(mos_sdssv_boss_spall) on a different pipeline version. No Gaia column is
exposed here, but none is needed: this goes through positional_easy_match
like ESO. Deep-link pattern confirmed live (returns a real viewer page, not
just docs-inferred).
"""

import requests
from astropy.time import Time

from sync.base import RawObservation

SQL_URL = "https://skyserver.sdss.org/dr19/SkyServerWS/SearchTools/SqlSearch"

LEGACY_MJD_CUTOFF = 58932  # first SDSS-V (FPS/robot-era) rows start after this

QUERY = """
SELECT TOP {page_size} specobjid, ra, dec, mjd, plate, fiberid, run2d
FROM specObj
WHERE class='STAR' AND mjd < {legacy_cutoff} AND mjd > {last_mjd}
ORDER BY mjd ASC
"""

PAGE_SIZE = 50000

VIEWER_URL = "https://dr19.sdss.org/optical/spectrum/view?plateid={plate}&mjd={mjd}&fiberid={fiberid}"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_mjd = cursor.get("last_mjd", 0)

    resp = requests.get(
        SQL_URL,
        params={
            "cmd": QUERY.format(page_size=PAGE_SIZE, legacy_cutoff=LEGACY_MJD_CUTOFF, last_mjd=last_mjd),
            "format": "json",
        },
        timeout=120,
    )
    resp.raise_for_status()
    rows = resp.json()[0]["Rows"]

    records = []
    max_mjd = last_mjd
    for row in rows:
        mjd = int(row["mjd"])
        max_mjd = max(max_mjd, mjd)
        records.append(
            RawObservation(
                archive_obs_id=str(row["specobjid"]),
                archive_url=VIEWER_URL.format(plate=row["plate"], mjd=mjd, fiberid=row["fiberid"]),
                instrument="SDSS/BOSS",
                obs_date=Time(mjd, format="mjd").to_datetime().date(),
                program_id=row["run2d"],
                ra=float(row["ra"]),
                dec=float(row["dec"]),
            )
        )

    return records, {"last_mjd": max_mjd}
