"""SDSS-V Optical (BOSS, current era) — bulk spAll-lite FITS file.

https://data.sdss.org/sas/dr19/spectro/boss/redux/v6_1_3/spAll-lite-v6_1_3.fits.gz

Turned out downloadable after all — 612MB gzip-compressed (unlike the DESI
MWS VAC file, gzip isn't seekable, so unlike desi.py this can't use HTTP
Range windows; the whole file has to be fetched and decompressed to read
any of it). Live-verified: the SPALL table's GAIA_ID column is a first-party
Gaia join, 100% populated among CLASS='STAR' rows (923,306 / 923,306 in the
v6_1_3 sample pulled here) — the earlier CAS SQL-only investigation just
hadn't found the bulk file's column exposed anywhere queryable.

DR2/DR3 caveat unresolved by this: a handful of sampled GAIA_IDs matched
both gaiadr2.gaia_source and gaiadr3.gaia_source with consistent positions,
which doesn't distinguish which release GAIA_ID is actually drawn from
(most stars keep the same source_id across releases; only crowded-field/
binary cases get reassigned, so a clean sample proves nothing either way).
Public docs still say DR2 — see the sdss-gaia-id-dr20-transition project
memory. Treat as DR2 until DR20 ships (~Aug 2026) confirms the switch.

SPEC_FILE gives the exact per-observation filename directly (no need to
reconstruct it) — confirmed live against the real SAS directory listing.
"""

import tempfile
from pathlib import Path

import numpy as np
import requests
from astropy.io import fits
from astropy.time import Time

from sync.base import RawObservation

SPALL_URL = "https://data.sdss.org/sas/dr19/spectro/boss/redux/v6_1_3/spAll-lite-v6_1_3.fits.gz"

SPECTRUM_URL = "https://data.sdss.org/sas/dr19/spectro/boss/redux/v6_1_3/spectra/lite/{field:06d}/{mjd}/{spec_file}"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_mjd = cursor.get("last_mjd", 0)

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "spAll-lite.fits.gz"
        with requests.get(SPALL_URL, stream=True, timeout=600) as resp:
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    f.write(chunk)

        with fits.open(local_path, lazy_load_hdus=True) as hdul:
            data = hdul["SPALL"].data

        is_star = data["CLASS"] == "STAR  "
        is_new = data["MJD"] > last_mjd
        rows = data[is_star & is_new]

    records = []
    max_mjd = last_mjd
    for row in rows:
        mjd = int(row["MJD"])
        max_mjd = max(max_mjd, mjd)
        field = int(row["FIELD"])
        spec_file = row["SPEC_FILE"].strip()
        records.append(
            RawObservation(
                archive_obs_id=row["SPECOBJID"].strip(),
                archive_url=SPECTRUM_URL.format(field=field, mjd=mjd, spec_file=spec_file),
                instrument="SDSS-V/BOSS",
                obs_date=Time(mjd, format="mjd").to_datetime().date(),
                program_id=row["SURVEY"].strip(),
                gaia_source_id=int(row["GAIA_ID"]),
            )
        )

    return records, {"last_mjd": max_mjd}
