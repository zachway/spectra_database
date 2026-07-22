"""SDSS Legacy Optical (BOSS/eBOSS, pre-SDSS-V) — DR19 bulk "allspec" catalog.

https://data.sdss.org/sas/dr19/spectro/allspec/1.0.1/allspec-dr19-1.0.1.fits.gz

First cut of this used SkyServer's SqlSearch REST API, paginated 50,000 rows
at a time by an mjd watermark — confirmed live to work for 18 consecutive
pages (~900K rows) before getting HTTP 403 Forbidden, the signature of
SkyServer's anti-bot/rate-limiting kicking in after sustained automated
querying (plain requests.get(), default User-Agent, no delay between
calls). Switched to DR19's unified "allspec" bulk catalog instead (812MB,
14.6M rows spanning every SDSS instrument/era) — confirmed live to cover
3.96M BOSS rows in the legacy MJD range (55176-58932) alone, 4x what
SkyServer had delivered before the block, via a single plain download with
no rate limit to hit at all. Same local-cache-then-read pattern as
desi.py/sdss_v_optical.py: downloaded once to a persistent local path,
reused across the run, deleted once the archive catches up.

No CLASS column here (unlike SkyServer's specObj, which the old version
filtered on CLASS='STAR' server-side) — confirmed live via programname/
survey: BOSS/eBOSS's legacy programs (DEEP_QSO, ELG_NGC/SGC, RM, eFEDS,
XMMXLL, ...) are cosmology-survey target-selection tags, not a stellar
filter, so there's no clean way to isolate stars from this file alone.
Goes through positional_easy_match unfiltered instead (this archive never
had a Gaia column either way) and accepts the extra non-stellar candidate
volume — positional matching against tracked stars already discards
non-matches harmlessly, same class-less approach already used for eso.py.

Not paginated — one fetch() call returns every legacy BOSS row at once,
same single-shot pattern already proven at comparable scale by
sdss_v_optical.py (923K rows), galah.py (1.08M), and rave.py (517K) in this
codebase; sync.main calls fetch() twice per run (once for results, once to
confirm zero), same as those.

plate_or_fps_field/fiberid/mjd/specobjid/run2d/ra/dec/cas_url all confirmed
live as fully populated (no zero/NaN sentinels) for the legacy (instrument=
'boss', mjd<58932) subset — cas_url is a ready-made SkyServer object
explorer deep link, used directly rather than reconstructed.
"""

import os

import numpy as np
import requests
from astropy.io import fits
from astropy.time import Time

from sync.base import RawObservation, clean_float

ALLSPEC_URL = "https://data.sdss.org/sas/dr19/spectro/allspec/1.0.1/allspec-dr19-1.0.1.fits.gz"

LEGACY_MJD_CUTOFF = 58932  # first SDSS-V (FPS/robot-era) rows start after this

# Not under public_html (morgan and joy share that NFS home, and Apache
# serves it publicly) -- this is scratch space, not something to publish.
CACHE_DIR = os.environ.get("SDSS_LEGACY_OPTICAL_CACHE_DIR", os.path.expanduser("~/.cache/spectra_database"))
ALLSPEC_CACHE_PATH = os.path.join(CACHE_DIR, "sdss_allspec_dr19.fits.gz")


def _ensure_cached() -> None:
    if os.path.exists(ALLSPEC_CACHE_PATH):
        return
    os.makedirs(CACHE_DIR, exist_ok=True)
    tmp_path = ALLSPEC_CACHE_PATH + ".tmp"
    with requests.get(ALLSPEC_URL, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    # Rename into place only after a full download -- avoids a half-written
    # file looking "present" to the os.path.exists check above if a run
    # gets interrupted mid-download.
    os.rename(tmp_path, ALLSPEC_CACHE_PATH)


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_mjd = cursor.get("last_mjd", 0)

    _ensure_cached()
    with fits.open(ALLSPEC_CACHE_PATH, lazy_load_hdus=True) as hdul:
        data = hdul[1].data
        is_boss = np.char.strip(data["instrument"]) == "boss"
        is_legacy_new = (data["mjd"] > last_mjd) & (data["mjd"] < LEGACY_MJD_CUTOFF) & (data["mjd"] > 0)
        rows = data[is_boss & is_legacy_new]

        records = []
        max_mjd = last_mjd
        for row in rows:
            mjd = int(row["mjd"])
            max_mjd = max(max_mjd, mjd)
            records.append(
                RawObservation(
                    archive_obs_id=row["specobjid"].strip(),
                    archive_url=row["cas_url"].strip(),
                    instrument="SDSS/BOSS",
                    obs_date=Time(mjd, format="mjd").to_datetime().date(),
                    program_id=row["run2d"].strip(),
                    ra=clean_float(row["ra"]),
                    dec=clean_float(row["dec"]),
                )
            )

    if not records:
        # Caught up -- drop the cache rather than let an 812MB scratch file
        # sit around indefinitely. A future SDSS data release just
        # re-downloads fresh on its first call.
        if os.path.exists(ALLSPEC_CACHE_PATH):
            os.remove(ALLSPEC_CACHE_PATH)

    return records, {"last_mjd": max_mjd}
