"""LBT — PEPSI, MODS, LUCI, via a real IVOA TAP service.

archive.lbto.org (a Vue.js SPA, "portal-gui") calls a TAP endpoint at
https://archive.lbto.org/tap — found by grepping the app's own JS bundle
for URL literals, not documented anywhere findable. The `lbt` schema has
one table per instrument (irtc, lbc, lbt, lbti, luci, mods, pepsi, pis).
Originally scoped to pepsi alone (spectroscopy-only, no imaging mode at
all); extended to mods and luci, which mix imaging and spectroscopy in
the same table — each has its own `dataprod` column with a clean
'spectrum'/'image'/'' split (confirmed live via DISTINCT), unlike pepsi
which has no such column at all (imagetyp='object' does that job there
instead). Not a uniform schema across instruments at all: mods/luci don't
have pepsi's `ra`/`dec` columns either — see INSTRUMENTS below.

No ObsCore, no access_url column — ra/dec (where present) are sexagesimal
strings (not decimal degrees, unlike every other TAP-based archive here),
parsed via astropy.

mods has per-target objra/objdec, but only ~56% populated (confirmed live:
1132/2000 spectrum rows) — the literal string 'none' is a real sentinel
for missing, not just masked/empty, so it's checked for explicitly. luci
has no per-target position column at all, only telra/teldec (telescope
pointing, not target position) — used as a best-effort stand-in; real for
on-axis pointings, an approximation for anything with a slit/IFU offset.
Not solved further here, same "accept present limitations" stance as the
matcher's own documented Stein 2051 A case.

archive_url points at the general search portal, not a specific file: the
only real download mechanism is an async job system (submit the entire
search-form state as a job, poll /jobs, extract a result URL from the
completed job) — confirmed live, no direct-file URL and no query-param
deep-link exist. That's designed for bulk "download my whole search"
workflows, not per-file lookups, and implementing it just to construct
one URL column would be wildly disproportionate to how every other
archive here works.

`object` sometimes already reports "Gaia DR3 <source_id>" directly for
pepsi (confirmed live) — parsed straight into gaia_source_id when it
matches that pattern, skipping SIMBAD entirely for those (guaranteed-
accurate, no round trip needed). Applied uniformly to mods/luci too
(cheap, harmless if it never matches there). Everything else (IRAS/TIC/
TOI designations, etc.) goes through as raw_target_name for the normal
SIMBAD-then-position fallback. ra/dec are always populated when parseable
regardless — kept as the raw-position audit trail same as every other
archive, not just for records that fall through to positional matching.

No paging cliff found for mods/luci (confirmed live: 20,000 rows in
~1.7-1.8s for both) — kept at the same PAGE_SIZE as pepsi for consistency
rather than tuned up, since pepsi's own cliff was never characterized
either (no cliff found there, just never pushed further than 5,000).

Cursor shape changed from a single flat {"last_date_obs": ...} (pepsi
only) to {instrument: {"last_date_obs": ...}, ...} (many). Reads a
pre-existing flat pepsi-only cursor as a fallback for the "PEPSI" key
specifically, so a production cursor written before this change doesn't
silently restart pepsi from scratch.

fetch() queries every instrument once per call rather than converging one
at a time — same pattern as sync/archives/noirlab.py's and
sync/archives/koa.py's multi-instrument fetch.
"""

from __future__ import annotations

import re

import astropy.units as u
import numpy as np
from astropy.coordinates import SkyCoord
from astropy.time import Time

from sync.base import RawObservation, make_tap_service

TAP_URL = "https://archive.lbto.org/tap"

QUERY = """
SELECT TOP {page_size} object, {ra_col} AS ra, {dec_col} AS dec, date_obs, file_name, propid
FROM {table}
WHERE {filter_col} = '{filter_value}' AND date_obs > '{last_date_obs}'
ORDER BY date_obs ASC
"""

PAGE_SIZE = 5000

SEARCH_PORTAL_URL = "https://archive.lbto.org"

GAIA_OBJECT_RE = re.compile(r"^Gaia\s+DR3\s+(\d+)$", re.IGNORECASE)

# instrument name -> (table, filter column, filter value, ra column, dec column).
# pepsi is spectroscopy-only, isolated via imagetyp; mods/luci mix imaging
# and spectroscopy, isolated via dataprod instead (confirmed live, neither
# column exists on the other table). luci has no per-target position
# column at all -- telra/teldec (telescope pointing) used as a stand-in.
INSTRUMENTS = {
    "PEPSI": ("lbt.pepsi", "imagetyp", "object", "ra", "dec"),
    "MODS": ("lbt.mods", "dataprod", "spectrum", "objra", "objdec"),
    "LUCI": ("lbt.luci", "dataprod", "spectrum", "telra", "teldec"),
}

# Pre-existing production cursors from before multi-instrument support were
# a flat {"last_date_obs": ...} for pepsi alone.
_LEGACY_INSTRUMENT = "PEPSI"

# mods reports this literal string for an unset objra/objdec, not just a
# masked/empty value (confirmed live).
_NULL_SENTINELS = {"none", ""}


def _clean_str(value) -> str | None:
    if value is None or np.ma.is_masked(value):
        return None
    text = str(value).strip()
    if text.lower() in _NULL_SENTINELS:
        return None
    return text or None


def _last_date_obs(cursor: dict, instrument: str) -> str:
    if instrument in cursor:
        return cursor[instrument].get("last_date_obs", "0000-01-01T00:00:00.000")
    if instrument == _LEGACY_INSTRUMENT and "last_date_obs" in cursor:
        return cursor["last_date_obs"]
    return "0000-01-01T00:00:00.000"


def _fetch_instrument(
    tap, instrument: str, table: str, filter_col: str, filter_value: str, ra_col: str, dec_col: str, last_date_obs: str
) -> tuple[list[RawObservation], str]:
    query = QUERY.format(
        page_size=PAGE_SIZE,
        ra_col=ra_col,
        dec_col=dec_col,
        table=table,
        filter_col=filter_col,
        filter_value=filter_value,
        last_date_obs=last_date_obs,
    )
    result_table = tap.search(query).to_table()

    records = []
    max_date_obs = last_date_obs
    for row in result_table:
        date_obs = _clean_str(row["date_obs"])
        if date_obs is None:
            continue
        max_date_obs = max(max_date_obs, date_obs)

        object_name = _clean_str(row["object"])
        gaia_source_id = None
        raw_target_name = None
        if object_name:
            m = GAIA_OBJECT_RE.match(object_name)
            if m:
                gaia_source_id = int(m.group(1))
            else:
                raw_target_name = object_name

        ra_str, dec_str = _clean_str(row["ra"]), _clean_str(row["dec"])
        ra = dec = None
        if ra_str and dec_str:
            coord = SkyCoord(ra=ra_str, dec=dec_str, unit=(u.hourangle, u.deg))
            ra, dec = coord.ra.deg, coord.dec.deg

        records.append(
            RawObservation(
                archive_obs_id=_clean_str(row["file_name"]) or f"{instrument.lower()}-{date_obs}",
                archive_url=SEARCH_PORTAL_URL,
                instrument=instrument,
                obs_date=Time(date_obs).to_datetime().date(),
                program_id=_clean_str(row["propid"]),
                gaia_source_id=gaia_source_id,
                ra=ra,
                dec=dec,
                raw_target_name=raw_target_name,
            )
        )

    return records, max_date_obs


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    tap = make_tap_service(TAP_URL)

    records = []
    new_cursor = {}
    for instrument, (table, filter_col, filter_value, ra_col, dec_col) in INSTRUMENTS.items():
        last_date_obs = _last_date_obs(cursor, instrument)
        instrument_records, max_date_obs = _fetch_instrument(
            tap, instrument, table, filter_col, filter_value, ra_col, dec_col, last_date_obs
        )
        records.extend(instrument_records)
        new_cursor[instrument] = {"last_date_obs": max_date_obs}

    return records, new_cursor
