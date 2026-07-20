"""Keck Observatory Archive (HIRES) — TAP, no native Gaia column.

TAP endpoint: https://koa.ipac.caltech.edu/TAP, schema koa_tap, one table per
instrument (koa_hires, koa_deimos, koa_lris, ...) — found directly from
KOA's own PyKOA docs. Scoped to koa_hires for now; koa_deimos and koa_esi
share an identical column shape (ra, dec, mjd, koaimtyp, object, filehand)
and should be trivial same-shape additions. koa_lris and koa_nires (at
least) use `mjd_obs` instead of `mjd` — not a uniform schema across
instruments, needs its own small per-table config to extend properly.

The previously-flagged "ORDER BY + TOP returns unsorted results" bug could
NOT be reproduced live in this session — tested 200 rows, strictly
non-decreasing mjd throughout. Standard TOP+ORDER BY+watermark pagination
used here; if the old bug resurfaces in practice, fall back to WHERE-range
chunking like eso.py instead.

koaimtyp='object' does not reliably exclude calibration frames (confirmed
live: several returned rows had object='flat') — same tradeoff as CFHT/CADC
and Gemini, left unfiltered rather than chasing a cleaner filter; harmless
rows just get skipped by the matcher.

Deep link confirmed live: filehand (e.g.
"/koadata14/HIRES/20170924/lev0/HI.20170924.17613.fits") through
cgi-bin/getKOA/nph-getKOA?filehand=... resolves to real FITS bytes (despite
a misleading text/html content-type header).
"""

import pyvo
from astropy.time import Time

from sync.base import RawObservation, clean_float

TAP_URL = "https://koa.ipac.caltech.edu/TAP"

QUERY = """
SELECT TOP {page_size} koaid, ra, dec, mjd, object, filehand
FROM koa_hires
WHERE koaimtyp='object' AND mjd > {last_mjd}
ORDER BY mjd ASC
"""

PAGE_SIZE = 50000

DOWNLOAD_URL = "https://koa.ipac.caltech.edu/cgi-bin/getKOA/nph-getKOA?filehand={filehand}"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_mjd = cursor.get("last_mjd", 0)

    tap = pyvo.dal.TAPService(TAP_URL)
    query = QUERY.format(page_size=PAGE_SIZE, last_mjd=last_mjd)
    table = tap.search(query, maxrec=PAGE_SIZE).to_table()

    records = []
    max_mjd = last_mjd
    for row in table:
        mjd = float(row["mjd"])
        max_mjd = max(max_mjd, mjd)
        filehand = str(row["filehand"])
        records.append(
            RawObservation(
                archive_obs_id=str(row["koaid"]),
                archive_url=DOWNLOAD_URL.format(filehand=filehand),
                instrument="HIRES",
                obs_date=Time(mjd, format="mjd").to_datetime().date(),
                ra=clean_float(row["ra"]),
                dec=clean_float(row["dec"]),
                raw_target_name=str(row["object"]),
            )
        )

    return records, {"last_mjd": max_mjd}
