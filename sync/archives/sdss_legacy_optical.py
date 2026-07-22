"""SDSS Legacy Optical (BOSS/eBOSS, pre-SDSS-V) — DR19 bulk "allspec" catalog.

https://data.sdss.org/sas/dr19/spectro/allspec/1.0.1/allspec-dr19-1.0.1.fits.gz

First cut of this used SkyServer's SqlSearch REST API, paginated 50,000 rows
at a time by an mjd watermark — confirmed live to work for 18 consecutive
pages (~900K rows) before getting HTTP 403 Forbidden, the signature of
SkyServer's anti-bot/rate-limiting kicking in after sustained automated
querying (plain requests.get(), default User-Agent, no delay between
calls). Switched to DR19's unified "allspec" bulk catalog instead (812MB
compressed, 14.6M rows spanning every SDSS instrument/era) — confirmed live
to cover 3.96M BOSS rows in the legacy MJD range (55176-58932) alone, 4x
what SkyServer had delivered before the block, via a single plain download
with no rate limit to hit at all.

No CLASS column here (unlike SkyServer's specObj, which the old version
filtered on CLASS='STAR' server-side) — confirmed live via programname/
survey: BOSS/eBOSS's legacy programs (DEEP_QSO, ELG_NGC/SGC, RM, eFEDS,
XMMXLL, ...) are cosmology-survey target-selection tags, not a stellar
filter, so there's no clean way to isolate stars from this file alone.
Goes through positional_easy_match unfiltered instead (this archive never
had a Gaia column either way) and accepts the extra non-stellar candidate
volume — positional matching against tracked stars already discards
non-matches harmlessly, same class-less approach already used for eso.py.

Paginated by fixed 7-day MJD windows rather than row count — a row-count
page (e.g. "next 50,000 rows sorted by mjd") risks splitting a single
night's observing run across two pages, and re-querying with a plain
mjd > watermark bound would then silently skip the rest of that night's
rows on the next page (same boundary-cutting failure mode fixed for
noirlab.py, different cause). A date window can't split a group unless the
group itself spans the boundary, which a single night's mjd value can't.
Sized empirically against the real file: 7-day windows give a max of
~45,000 rows and a median of ~9,000 per nonzero window (537 total windows,
340 nonzero), in line with the ~50,000-row pages used elsewhere in this
codebase. Verified the windowing math directly against the real file:
every one of the 3,956,000 legacy rows falls into exactly one window, no
gaps or double-counts. Same "loop past empty windows internally" fix as
gemini.py, needed for the same reason (an empty window here doesn't mean
the legacy range is exhausted) — except the stopping point here is a known
fixed constant (LEGACY_MJD_CUTOFF) rather than a live "now".

Two-stage cache, not one -- this went through two failed designs first,
both confirmed live:
  1. Caching the downloaded .fits.gz directly and re-opening it per page:
     astropy's fits.open() transparently gunzips a .fits.gz path on every
     call, so ~340 pages meant re-decompressing the full 812MB from scratch
     ~340 times. Killed a test run after 2+ minutes still on the first few
     pages.
  2. Decompressing once to a plain .fits file, but still filtering
     (instrument='boss' + mjd range) and boolean-indexing the full 14.6M
     x 36-column table fresh on every fetch() call: a single call still
     didn't return within 60s. numpy string ops (np.char.strip on 14.6M
     rows) and fancy-indexing a wide structured array are just too slow to
     redo hundreds of times, even from a local mmap'd file.
Fixed by distilling down to a *third*, much smaller local cache: the ~3.96M
legacy rows, narrowed to only the 6 columns actually used, written once as
Parquet (DuckDB, not astropy, handles this well). Every fetch() call after
that is a single DuckDB range query against a small Parquet file --
confirmed live at a few hundred ms, even repeated ~340 times.

plate_or_fps_field/fiberid/mjd/specobjid/run2d/ra/dec/cas_url all confirmed
live as fully populated (no zero/NaN sentinels) for the legacy (instrument=
'boss', mjd<58932) subset — cas_url is a ready-made SkyServer object
explorer deep link, used directly rather than reconstructed.
"""

import gzip
import os
import shutil

import duckdb
import numpy as np
import requests
from astropy.io import fits
from astropy.time import Time

from sync.base import RawObservation, clean_float

ALLSPEC_URL = "https://data.sdss.org/sas/dr19/spectro/allspec/1.0.1/allspec-dr19-1.0.1.fits.gz"

LEGACY_MJD_CUTOFF = 58932  # first SDSS-V (FPS/robot-era) rows start after this
FIRST_MJD = 55176  # live-confirmed: MIN(mjd) among instrument='boss' legacy rows

WINDOW_DAYS = 7

# Not under public_html (morgan and joy share that NFS home, and Apache
# serves it publicly) -- this is scratch space, not something to publish.
CACHE_DIR = os.environ.get("SDSS_LEGACY_OPTICAL_CACHE_DIR", os.path.expanduser("~/.cache/spectra_database"))
RAW_FITS_PATH = os.path.join(CACHE_DIR, "sdss_allspec_dr19.fits")  # transient -- deleted after distilling
LEGACY_CACHE_PATH = os.path.join(CACHE_DIR, "sdss_legacy_boss.parquet")  # what fetch() actually reads


def _download_and_decompress() -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)
    gz_tmp_path = RAW_FITS_PATH + ".gz.tmp"
    with requests.get(ALLSPEC_URL, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        with open(gz_tmp_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)

    fits_tmp_path = RAW_FITS_PATH + ".tmp"
    with gzip.open(gz_tmp_path, "rb") as src, open(fits_tmp_path, "wb") as dst:
        shutil.copyfileobj(src, dst, length=1 << 20)
    os.remove(gz_tmp_path)
    os.rename(fits_tmp_path, RAW_FITS_PATH)


def _ensure_cached() -> None:
    if os.path.exists(LEGACY_CACHE_PATH):
        return

    _download_and_decompress()
    with fits.open(RAW_FITS_PATH, lazy_load_hdus=True) as hdul:
        data = hdul[1].data
        is_legacy = (
            (np.char.strip(data["instrument"]) == "boss")
            & (data["mjd"] >= FIRST_MJD)
            & (data["mjd"] < LEGACY_MJD_CUTOFF)
        )
        legacy = data[is_legacy]
        # DuckDB needs plain Python/numpy arrays, not FITS's big-endian
        # dtypes or fixed-width byte-string columns -- decode/cast each
        # column explicitly rather than registering the structured array
        # as-is.
        columns = {
            "mjd": legacy["mjd"].astype(np.int32),
            "specobjid": np.char.strip(legacy["specobjid"].astype(str)),
            "run2d": np.char.strip(legacy["run2d"].astype(str)),
            "ra": legacy["ra"].astype(np.float64),
            "dec": legacy["dec"].astype(np.float64),
            "cas_url": np.char.strip(legacy["cas_url"].astype(str)),
        }

    con = duckdb.connect()
    con.register("legacy", columns)
    tmp_path = LEGACY_CACHE_PATH + ".tmp"
    con.execute(f"COPY (SELECT * FROM legacy ORDER BY mjd) TO '{tmp_path}' (FORMAT PARQUET)")
    con.close()
    os.chmod(tmp_path, 0o644)
    # Rename into place only after a full write -- avoids a half-written
    # file looking "present" to the os.path.exists check above if a run
    # gets interrupted mid-write.
    os.rename(tmp_path, LEGACY_CACHE_PATH)

    # The distilled Parquet cache is all fetch() needs from here on --
    # the ~11GB decompressed FITS file was only a stepping stone.
    os.remove(RAW_FITS_PATH)


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    window_start = cursor.get("window_start", FIRST_MJD)

    if window_start >= LEGACY_MJD_CUTOFF:
        # Already fully caught up -- nothing to do, and nothing left to
        # cache (the previous call that reached this point already deleted
        # it below).
        return [], cursor

    _ensure_cached()
    con = duckdb.connect()
    con.execute(f"CREATE VIEW legacy AS SELECT * FROM read_parquet('{LEGACY_CACHE_PATH}')")

    while True:
        window_end = min(window_start + WINDOW_DAYS, LEGACY_MJD_CUTOFF)
        con.execute(
            "SELECT mjd, specobjid, run2d, ra, dec, cas_url FROM legacy WHERE mjd >= ? AND mjd < ?",
            [window_start, window_end],
        )
        rows = con.fetchall()
        if rows or window_end >= LEGACY_MJD_CUTOFF:
            break
        window_start = window_end
    con.close()

    records = [
        RawObservation(
            archive_obs_id=specobjid,
            archive_url=cas_url,
            instrument="SDSS/BOSS",
            obs_date=Time(int(mjd), format="mjd").to_datetime().date(),
            program_id=run2d,
            ra=clean_float(ra),
            dec=clean_float(dec),
        )
        for mjd, specobjid, run2d, ra, dec, cas_url in rows
    ]

    if window_end >= LEGACY_MJD_CUTOFF:
        # Caught up -- drop the cache rather than let it sit around
        # indefinitely. A future SDSS data release just re-downloads and
        # re-distills fresh on its first call.
        if os.path.exists(LEGACY_CACHE_PATH):
            os.remove(LEGACY_CACHE_PATH)

    return records, {"window_start": window_end}
