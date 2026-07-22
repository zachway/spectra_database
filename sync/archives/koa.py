"""Keck Observatory Archive — TAP, no native Gaia column.

TAP endpoint: https://koa.ipac.caltech.edu/TAP, schema koa_tap, one table per
instrument (koa_hires, koa_deimos, koa_lris, ...) — found directly from
KOA's own PyKOA docs.

Originally scoped to koa_hires alone; extended to koa_deimos, koa_esi,
koa_lris, koa_nires. Confirmed live (TAP_SCHEMA column listing) that
koa_deimos/koa_esi carry both `mjd` and `mjd_obs` (same shape as koa_hires),
while koa_lris/koa_nires carry only `mjd_obs` — no `mjd` column at all, per
this module's own earlier note. INSTRUMENTS below records the right column
per table instead of assuming a uniform schema.

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
a misleading text/html content-type header). Same resolver works for every
instrument here (path prefix changes, cgi endpoint doesn't).

Cursor shape changed from a single flat {"last_mjd": ...} (HIRES only) to
{instrument: {"last_mjd": ...}, ...} (many). Reads a pre-existing flat
HIRES-only cursor as a fallback for the "HIRES" key specifically, so a
production cursor written before this change doesn't silently restart
HIRES from mjd 0 (UNIQUE (archive_code, archive_obs_id) would make that
harmless, just wasteful — this avoids the waste).

fetch() queries every instrument once per call rather than converging one
at a time — sync.main's driver only stops once a whole page returns zero
records, so an instrument that's already caught up just contributes an
empty result on each subsequent call until every instrument is caught up
together, same pattern as sync/archives/noirlab.py's multi-instrument fetch.
"""

from astropy.time import Time

from sync.base import RawObservation, clean_float, make_tap_service

TAP_URL = "https://koa.ipac.caltech.edu/TAP"

QUERY = """
SELECT TOP {page_size} koaid, ra, dec, {mjd_col} AS mjd, object, filehand
FROM {table}
WHERE koaimtyp='object' AND {mjd_col} > {last_mjd} AND {mjd_col} < {mjd_sanity_bound}
ORDER BY {mjd_col} ASC
"""

# Confirmed live: koa_esi carries real garbage in both its mjd and mjd_obs
# columns for a majority of rows (23,283 of 35,102) -- values around
# 2.9-3.2 million (implying a year past datetime's year-9999 ceiling,
# confirmed via a live crash on Time(...).to_datetime()), not just a rare
# outlier. Filtered directly in the SQL rather than skipped client-side:
# skipping without excluding them from the query would let a bad value
# become the page's max_mjd and get persisted as the next watermark, which
# -- being ~50x any real mjd -- would make every future query's `mjd >
# last_mjd` exclude all real data forever. 100000 (~year 2132) is
# comfortably past any legitimate observation date and comfortably short
# of the corrupted values seen live.
MJD_SANITY_BOUND = 100000

PAGE_SIZE = 50000

DOWNLOAD_URL = "https://koa.ipac.caltech.edu/cgi-bin/getKOA/nph-getKOA?filehand={filehand}"

# instrument name -> (TAP table, mjd column). koa_deimos/koa_esi share
# koa_hires's shape (both mjd and mjd_obs present, mjd used for
# consistency); koa_lris/koa_nires only carry mjd_obs (confirmed live).
INSTRUMENTS = {
    "HIRES": ("koa_hires", "mjd"),
    "DEIMOS": ("koa_deimos", "mjd"),
    "ESI": ("koa_esi", "mjd"),
    "LRIS": ("koa_lris", "mjd_obs"),
    "NIRES": ("koa_nires", "mjd_obs"),
}

# Pre-existing production cursors from before multi-instrument support were
# a flat {"last_mjd": ...} for HIRES alone.
_LEGACY_INSTRUMENT = "HIRES"


def _last_mjd(cursor: dict, instrument: str) -> float:
    if instrument in cursor:
        return cursor[instrument].get("last_mjd", 0)
    if instrument == _LEGACY_INSTRUMENT and "last_mjd" in cursor:
        return cursor["last_mjd"]
    return 0


def _fetch_instrument(tap, instrument: str, table: str, mjd_col: str, last_mjd: float) -> tuple[list[RawObservation], float]:
    query = QUERY.format(
        page_size=PAGE_SIZE, mjd_col=mjd_col, table=table, last_mjd=last_mjd, mjd_sanity_bound=MJD_SANITY_BOUND
    )
    result_table = tap.search(query, maxrec=PAGE_SIZE).to_table()

    records = []
    max_mjd = last_mjd
    for row in result_table:
        mjd = float(row["mjd"])
        max_mjd = max(max_mjd, mjd)
        filehand = str(row["filehand"])
        records.append(
            RawObservation(
                archive_obs_id=str(row["koaid"]),
                archive_url=DOWNLOAD_URL.format(filehand=filehand),
                instrument=instrument,
                obs_date=Time(mjd, format="mjd").to_datetime().date(),
                ra=clean_float(row["ra"]),
                dec=clean_float(row["dec"]),
                raw_target_name=str(row["object"]),
            )
        )

    return records, max_mjd


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    tap = make_tap_service(TAP_URL)

    records = []
    new_cursor = {}
    for instrument, (table, mjd_col) in INSTRUMENTS.items():
        last_mjd = _last_mjd(cursor, instrument)
        instrument_records, max_mjd = _fetch_instrument(tap, instrument, table, mjd_col, last_mjd)
        records.extend(instrument_records)
        new_cursor[instrument] = {"last_mjd": max_mjd}

    return records, new_cursor
