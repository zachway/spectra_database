"""DESI DR1 MWS (Milky Way Survey) value-added catalog.

https://data.desi.lbl.gov/doc/releases/dr1/vac/mws/

Turns out NOT to need NOIRLab Data Lab at all (contrary to the earlier
assumption) — it's a single ~12GB master FITS file
(mwsall-pix-iron.fits) served directly by data.desi.lbl.gov, with the
RVTAB and GAIA extensions row-aligned 1:1 (verified live: same row index
gives matching TARGETID/GAIA SOURCE_ID and consistent RA between the two).

The file is far too large to download whole (even the two extensions we
need are 1.65GB and 4.08GB on their own — the server doesn't expose an
API, just the raw file). This reads the raw bytes directly with a
hand-built numpy dtype (astropy's own HDU.data access was tried first and
pulls the whole extension into memory before slicing — not useful here).

First cut of this issued one HTTP Range request per page (2 requests x
~ROWS_PER_PAGE rows), which meant every one of the ~100+ pages needed for
the full ~2M-row catalog cost a network round trip to data.desi.lbl.gov —
confirmed live as consistently 1.5-4.5 minutes per page, on track for many
hours total. Downloading each extension once instead (still just the two
needed extensions, not the full 12GB file) to a local cache on morgan and
reading row windows from local disk turns that into a single ~5.7GB
sequential download plus fast local seeks. The cache is deliberately
temporary: fetch() deletes it once the archive catches up (last_row >=
n_rows), so nothing multi-GB sits around after the backfill converges —
a future DESI data release just re-downloads fresh.

SOURCE_ID == 999999 is GAIA's sentinel for "no Gaia crossmatch"; those rows
are skipped. No per-observation date is available in RVTAB/GAIA without
also pulling FIBERMAP (another multi-GB extension) — not done, so these
holdings carry no obs_date, same tradeoff as the other direct-Gaia-column
archives that lack one.
"""

import os
import re

import numpy as np
import requests
from astropy.io import fits

from sync.base import RawObservation

FITS_URL = "https://data.desi.lbl.gov/public/dr1/vac/dr1/mws/iron/v1.0/mwsall-pix-iron.fits"

GAIA_NO_MATCH_SENTINEL = 999999

ROWS_PER_PAGE = 20000

SPECTRUM_URL = (
    "https://data.desi.lbl.gov/public/dr1/spectro/redux/iron/healpix/"
    "{survey}/{program}/{healpix_group}/{healpix}/coadd-{survey}-{program}-{healpix}.fits"
)

_FITS_TO_NUMPY = {"D": ">f8", "E": ">f4", "K": ">i8", "J": ">i4", "I": ">i2", "B": "u1", "L": "S1"}

# Not under public_html (morgan and joy share that NFS home, and Apache
# serves it publicly) -- this is a multi-GB scratch cache, not something to
# publish. Overridable for local dev / testing without touching morgan's
# real cache.
CACHE_DIR = os.environ.get("DESI_CACHE_DIR", os.path.expanduser("~/.cache/spectra_database"))
RV_CACHE_PATH = os.path.join(CACHE_DIR, "desi_mwsall_iron_rvtab.dat")
GAIA_CACHE_PATH = os.path.join(CACHE_DIR, "desi_mwsall_iron_gaia.dat")

_DOWNLOAD_CHUNK_SIZE = 8 * 1024 * 1024


def _build_dtype(columns):
    fields = []
    for c in columns:
        count, code = re.match(r"^(\d*)([A-Z])$", c.format).groups()
        if code == "A":
            fields.append((c.name, f"S{int(count) if count else 1}"))
        else:
            fields.append((c.name, _FITS_TO_NUMPY[code]))
    return np.dtype(fields)


def _get_layout():
    """Header-only metadata (dtype, data offset, row count) for RVTAB and GAIA — cheap, no data pull."""
    with fits.open(FITS_URL, use_fsspec=True, lazy_load_hdus=True) as hdul:
        rv_dtype = _build_dtype(hdul["RVTAB"].columns)
        gaia_dtype = _build_dtype(hdul["GAIA"].columns)
        rv_info = hdul.fileinfo(hdul.index_of("RVTAB"))
        gaia_info = hdul.fileinfo(hdul.index_of("GAIA"))
        n_rows = hdul["RVTAB"].header["NAXIS2"]
    return {
        "rv_dtype": rv_dtype,
        "gaia_dtype": gaia_dtype,
        "rv_dat_loc": rv_info["datLoc"],
        "gaia_dat_loc": gaia_info["datLoc"],
        "n_rows": n_rows,
    }


def _ensure_cached(dat_loc: int, dtype: np.dtype, n_rows: int, cache_path: str) -> None:
    expected_size = n_rows * dtype.itemsize
    if os.path.exists(cache_path) and os.path.getsize(cache_path) == expected_size:
        return
    os.makedirs(CACHE_DIR, exist_ok=True)
    start = dat_loc
    end = start + expected_size - 1
    tmp_path = cache_path + ".tmp"
    with requests.get(FITS_URL, headers={"Range": f"bytes={start}-{end}"}, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=_DOWNLOAD_CHUNK_SIZE):
                f.write(chunk)
    # Rename into place only after a full, successful download -- same
    # torn-file concern as scripts.export_to_parquet's atomic writes, here
    # guarding against a half-downloaded cache looking "present" to the
    # os.path.getsize check above if a later run gets interrupted mid-write.
    os.rename(tmp_path, cache_path)


def _read_cached_rows(cache_path: str, dtype: np.dtype, start_row: int, n_rows: int) -> np.ndarray:
    with open(cache_path, "rb") as f:
        f.seek(start_row * dtype.itemsize)
        buf = f.read(n_rows * dtype.itemsize)
    return np.frombuffer(buf, dtype=dtype)


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_row = cursor.get("last_row", 0)
    layout = _get_layout()

    if last_row >= layout["n_rows"]:
        # Caught up -- drop the cache rather than let a multi-GB scratch
        # file sit around indefinitely. A future DESI data release just
        # re-downloads fresh on its first page.
        for path in (RV_CACHE_PATH, GAIA_CACHE_PATH):
            if os.path.exists(path):
                os.remove(path)
        return [], cursor

    _ensure_cached(layout["rv_dat_loc"], layout["rv_dtype"], layout["n_rows"], RV_CACHE_PATH)
    _ensure_cached(layout["gaia_dat_loc"], layout["gaia_dtype"], layout["n_rows"], GAIA_CACHE_PATH)

    n = min(ROWS_PER_PAGE, layout["n_rows"] - last_row)
    rv_rows = _read_cached_rows(RV_CACHE_PATH, layout["rv_dtype"], last_row, n)
    gaia_rows = _read_cached_rows(GAIA_CACHE_PATH, layout["gaia_dtype"], last_row, n)

    records = []
    for rv, gaia in zip(rv_rows, gaia_rows):
        source_id = int(gaia["SOURCE_ID"])
        if source_id == GAIA_NO_MATCH_SENTINEL:
            continue
        survey = rv["SURVEY"].decode().strip()
        program = rv["PROGRAM"].decode().strip()
        healpix = int(rv["HEALPIX"])
        records.append(
            RawObservation(
                archive_obs_id=str(int(rv["TARGETID"])),
                archive_url=SPECTRUM_URL.format(
                    survey=survey, program=program, healpix_group=healpix // 100, healpix=healpix
                ),
                instrument="DESI",
                gaia_source_id=source_id,
            )
        )

    return records, {"last_row": last_row + n}
