"""SDSS-V Optical (BOSS, current era) — bulk spAll-lite FITS file.

https://data.sdss.org/sas/dr19/spectro/boss/redux/v6_1_3/spAll-lite-v6_1_3.fits.gz

Turned out downloadable after all — 612MB gzip-compressed (unlike the DESI
MWS VAC file, gzip isn't seekable, so unlike desi.py this can't use HTTP
Range windows; the whole file has to be fetched and decompressed to read
any of it). Live-verified: the SPALL table's GAIA_ID column is a first-party
Gaia join, 100% populated among CLASS='STAR' rows (923,306 / 923,306 in the
v6_1_3 sample pulled here) — the earlier CAS SQL-only investigation just
hadn't found the bulk file's column exposed anywhere queryable.

First cut of this re-downloaded the full 612MB on every fetch() call (into
a tempfile.TemporaryDirectory() that deletes itself when the call returns)
-- since this archive isn't paginated (one fetch() returns every new row in
a single pass, so sync.main only calls it twice per run: once to get
everything new, once to confirm zero), that meant downloading the whole
file twice per sync run. Caches it in a persistent local path instead, so
the second (confirming, always-empty) call is a local re-read rather than
another 612MB fetch. The cache is deleted once that empty confirmation
happens, so it's temporary scratch space, not a permanent fixture -- same
pattern as desi.py's row-window cache.

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

import os

import numpy as np
import requests
from astropy.io import fits
from astropy.time import Time

from sync.base import RawObservation

SPALL_URL = "https://data.sdss.org/sas/dr19/spectro/boss/redux/v6_1_3/spAll-lite-v6_1_3.fits.gz"

SPECTRUM_URL = "https://data.sdss.org/sas/dr19/spectro/boss/redux/v6_1_3/spectra/lite/{field:06d}/{mjd}/{spec_file}"

# Not under public_html (morgan and joy share that NFS home, and Apache
# serves it publicly) -- this is scratch space, not something to publish.
CACHE_DIR = os.environ.get("SDSS_V_OPTICAL_CACHE_DIR", os.path.expanduser("~/.cache/spectra_database"))
SPALL_CACHE_PATH = os.path.join(CACHE_DIR, "sdss_v_spall_lite.fits.gz")


def _ensure_cached() -> None:
    if os.path.exists(SPALL_CACHE_PATH):
        return
    os.makedirs(CACHE_DIR, exist_ok=True)
    tmp_path = SPALL_CACHE_PATH + ".tmp"
    with requests.get(SPALL_URL, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    # Rename into place only after a full download -- avoids a half-written
    # file looking "present" to the os.path.exists check above if a run
    # gets interrupted mid-download.
    os.rename(tmp_path, SPALL_CACHE_PATH)


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_mjd = cursor.get("last_mjd", 0)

    _ensure_cached()
    with fits.open(SPALL_CACHE_PATH, lazy_load_hdus=True) as hdul:
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

    if not records:
        # Caught up -- drop the cache rather than let a 612MB scratch file
        # sit around indefinitely. A future SDSS-V reduction just
        # re-downloads fresh on its first call.
        if os.path.exists(SPALL_CACHE_PATH):
            os.remove(SPALL_CACHE_PATH)

    return records, {"last_mjd": max_mjd}
