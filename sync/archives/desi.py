"""DESI DR1 MWS (Milky Way Survey) value-added catalog.

https://data.desi.lbl.gov/doc/releases/dr1/vac/mws/

Turns out NOT to need NOIRLab Data Lab at all (contrary to the earlier
assumption) — it's a single ~12GB master FITS file
(mwsall-pix-iron.fits) served directly by data.desi.lbl.gov, with the
RVTAB and GAIA extensions row-aligned 1:1 (verified live: same row index
gives matching TARGETID/GAIA SOURCE_ID and consistent RA between the two).

The file is far too large to download whole (even the two extensions we
need are 1.65GB and 4.08GB on their own — the server doesn't expose an
API, just the raw file). Instead this reads fixed-size row windows via
HTTP Range requests, parsing the raw bytes directly with a hand-built
numpy dtype (astropy's own HDU.data access was tried first and pulls the
whole extension into memory before slicing — not useful here).

SOURCE_ID == 999999 is GAIA's sentinel for "no Gaia crossmatch"; those rows
are skipped. No per-observation date is available in RVTAB/GAIA without
also pulling FIBERMAP (another multi-GB extension) — not done, so these
holdings carry no obs_date, same tradeoff as the other direct-Gaia-column
archives that lack one.
"""

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


def _fetch_rows(dat_loc: int, dtype: np.dtype, start_row: int, n_rows: int) -> np.ndarray:
    start = dat_loc + start_row * dtype.itemsize
    end = start + n_rows * dtype.itemsize - 1
    resp = requests.get(FITS_URL, headers={"Range": f"bytes={start}-{end}"}, timeout=120)
    resp.raise_for_status()
    return np.frombuffer(resp.content, dtype=dtype)


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_row = cursor.get("last_row", 0)
    layout = _get_layout()

    if last_row >= layout["n_rows"]:
        return [], cursor

    n = min(ROWS_PER_PAGE, layout["n_rows"] - last_row)
    rv_rows = _fetch_rows(layout["rv_dat_loc"], layout["rv_dtype"], last_row, n)
    gaia_rows = _fetch_rows(layout["gaia_dat_loc"], layout["gaia_dtype"], last_row, n)

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
