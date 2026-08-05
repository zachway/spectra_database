"""SDSS-V Optical (BOSS, current era) — bulk spAll-lite FITS file.

https://data.sdss.org/sas/dr20/spectro/boss/redux/v6_2_1/summary/daily/spAll-lite-v6_2_1.fits.gz

Turned out downloadable after all — DR20 ships this as ~2.5GB gzip-compressed
(unlike the DESI MWS VAC file, gzip isn't seekable, so unlike desi.py this
can't use HTTP Range windows; the whole file has to be fetched and
decompressed to read any of it). Live-verified: the SPALL table's GAIA_ID
column is a first-party Gaia join, 100% populated among CLASS='STAR' rows in
the DR19 v6_1_3 sample pulled here — the earlier CAS SQL-only investigation
just hadn't found the bulk file's column exposed anywhere queryable.

First cut of this re-downloaded the full file on every fetch() call (into
a tempfile.TemporaryDirectory() that deletes itself when the call returns)
-- since this archive isn't paginated (one fetch() returns every new row in
a single pass, so sync.main only calls it twice per run: once to get
everything new, once to confirm zero), that meant downloading the whole
file twice per sync run. Caches it in a persistent local path instead, so
the second (confirming, always-empty) call is a local re-read rather than
another multi-GB fetch. The cache is deleted once that empty confirmation
happens, so it's temporary scratch space, not a permanent fixture -- same
pattern as desi.py's row-window cache.

DR20 (live-verified 2026-07-31 against the real FITS header, TTYPE36
comment): GAIA_ID has switched from "Gaia DR2 SourceID" to "Gaia DR3
SourceID" as expected -- see the sdss-gaia-id-dr20-transition project
memory. No reconciliation needed for rows pulled via this DR20 URL.

DR20 also reorganized the reduction pipeline layout, both live-verified:
reduction version is v6_2_1 (was v6_1_3 for DR19), and the summary/spectra
trees now split into daily/epoch/allepoch flavors -- "daily" (used here) is
the one-row-per-visit flavor matching this project's per-observation model
(the other two are coadds, like apStar, with no single obs_date). The
per-observation spectrum path also gained a field-prefix grouping directory
level not present in DR19 (e.g. field 015002 lives under 015XXX/015002/...,
grouping by the field number's first 3 of 6 zero-padded digits) -- computed
in fetch() below.

SPEC_FILE gives the exact per-observation filename directly (no need to
reconstruct it) — confirmed live against the real SAS directory listing.

reduction_status is hardcoded 'reduced' -- spAll-lite is the pipeline-
reduced, flux/wavelength-calibrated per-visit spectrum (the whole point of
the "reduction version" v6_2_1 tag above), never a raw CCD frame; SDSS has
no public raw-frame distribution path at all.
"""

import os

import numpy as np
import requests
from astropy.io import fits
from astropy.time import Time

from sync.base import RawObservation

SPALL_URL = "https://data.sdss.org/sas/dr20/spectro/boss/redux/v6_2_1/summary/daily/spAll-lite-v6_2_1.fits.gz"

SPECTRUM_URL = (
    "https://data.sdss.org/sas/dr20/spectro/boss/redux/v6_2_1/spectra/daily/lite/"
    "{field_group}XXX/{field:06d}/{mjd}/{spec_file}"
)

# Not under public_html (morgan and joy share that NFS home, and Apache
# serves it publicly) -- this is scratch space, not something to publish.
CACHE_DIR = os.environ.get("SDSS_V_OPTICAL_CACHE_DIR", os.path.expanduser("~/.cache/spectra_pointer"))
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
        field_group = f"{field:06d}"[:3]
        spec_file = row["SPEC_FILE"].strip()
        records.append(
            RawObservation(
                archive_obs_id=row["SPECOBJID"].strip(),
                archive_url=SPECTRUM_URL.format(field_group=field_group, field=field, mjd=mjd, spec_file=spec_file),
                instrument="SDSS-V/BOSS",
                obs_date=Time(mjd, format="mjd").to_datetime().date(),
                program_id=row["SURVEY"].strip(),
                gaia_source_id=int(row["GAIA_ID"]),
                reduction_status="reduced",
            )
        )

    if not records:
        # Caught up -- drop the cache rather than let a multi-GB scratch
        # file sit around indefinitely. A future SDSS-V reduction just
        # re-downloads fresh on its first call.
        if os.path.exists(SPALL_CACHE_PATH):
            os.remove(SPALL_CACHE_PATH)

    return records, {"last_mjd": max_mjd}
