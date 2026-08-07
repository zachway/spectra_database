"""IACOB Spectroscopic Database (IAC, Spain) — SSA, whole-sky one-shot pull.

A curated database of massive OB-star spectra maintained by the Instituto
de Astrofisica de Canarias, drawing on Mercator/HERMES and NOT/FIES data
(both instruments already have their own primary archives in this project
-- hermes_mercator.py, not_fies.py where present -- but IACOB is a separate,
independently-discoverable curation of a subset of those same
observations, same "different door into overlapping holdings" situation
this project already accepts elsewhere; it isn't deduped against them).

No plain TAP endpoint exists on this host, unlike the GAVO DaCHS family
(feros_gavo.py/hermes_mercator.py): `ocan.iac.es:8080/tap`, `/iacob/tap`,
and the DaCHS `__system__/tap/run/tap` convention all 404 with a plain
Tomcat default page (confirmed live), not a DaCHS instance at all -- this
is a bespoke JSP-based SSA service, so a TAP query shape was never on the
table here.

Real, working SSA endpoint: `ocan.iac.es:8080/iacob/jsp/ssap.jsp`. No sky
crawl needed to cover the whole archive -- confirmed live that `POS=0,0`
with a large enough `SIZE` (cone-search radius in degrees) converges to a
fixed total regardless of center or further radius increase (74 rows at
SIZE=60, 1085 at 120, 1240 at 150, 1255 at 180 and every larger SIZE tried
up to 1000) -- 1255 is the genuine whole-archive row count, not a
response-size cutoff, and one `POS=0,0&SIZE=360` request returns it in a
single ~730KB response. `TIME` (the SSA-standard time-range filter) was
tried as a possible incremental-pagination axis but errors out
(`QUERY_STATUS=ERROR`) on this service -- not supported here, so there is
no server-side way to ask for only new records since a prior run.

Because of that, and because match_records/run_sync always report nonzero
counts for any nonempty record list (there's no "0 new" outcome for a
record that was already seen -- matched/skipped/needs_review are all
still >0), returning the same 1255 rows on every call would loop
sync.main's per-archive driver forever. This module instead does one full
pull and then no-ops via a `synced_at` cursor, the same static-dataset
shape as feros_gavo.py/elodie.py -- except those archives are genuinely
decommissioned instruments, while Mercator/NOT are still operating
telescopes, so this is a documented current limitation (no way to detect
or fetch IACOB's incremental growth from this SSA service, not a claim
that the archive itself is finished growing).

The SSA response is a real, standards-shaped VOTable, but with one
genuinely malformed field: `TIME` is declared `datatype="TIMESTAMP"`
(confirmed live), which isn't a valid VOTable datatype at all (the IVOA
spec only defines char/int/float/double/boolean/etc) -- astropy's VOTable
parser (and so pyvo.dal.SSAService, which relies on it) rejects the whole
response outright with `E06: Unknown datatype 'TIMESTAMP'`. Parsed via a
plain regex TR/TD walk instead, same workaround shape as not_fies.py's/
ing.py's own malformed-markup parsing, rather than trying to coerce
astropy into accepting it.

Column order within each `<TR>` (confirmed live, stable across repeated
identical queries) is exactly the service's declared FIELD order:
AssocID, AcRef, Format, Title, Location, TIME, mys_filename, TARGET,
SP_CLASS, EXPTIME, SNR, DATA_RELEASE, INSTR, RESOL, DOWNLOAD, AXES, UNITS,
DIMEQ, SCALEQ. `AssocID` is not used as the per-row key here -- it reads
like a plain 1..N row counter over that query's result set rather than a
stable per-spectrum identifier, so `mys_filename` (the real FITS filename,
e.g. "HD10205_20131216_212859_M_V85000.fits", confirmed live unique
across all 1255 rows) is used for `archive_obs_id` instead. `AcRef` is a
real, working download URL once the CDATA wrapper and the extra pair of
literal double-quotes the service wraps around it are stripped (confirmed
live via a real GET: 200, `application/octet-stream`, a genuine FITS
file) -- note it 401s on a bare HEAD request, so don't use HEAD to
sanity-check it.

`Location` is a plain "ra,dec" degree pair (confirmed live against the
VOTable's own `<COOSYS equinox="2000">` and `ucd="pos.eq"` on the field --
equatorial J2000, same convention as everywhere else in this project).
`TARGET` carries real catalog names (e.g. "HD10205", "HD36629" --
confirmed live, real known OB stars). All 1255 rows have a populated
Location/TIME/TARGET/AcRef (confirmed live, 0 blank across every one of
those four fields) -- no missing-field handling needed for the happy
path, though parsing still degrades to None rather than raising if a
field is ever empty.

Only two instruments appear in the live data: MERCATOR (362 rows, R
85,000 on every one) and NOT (893 rows, split across R 46,000/67,000/
25,000 depending on FIES's fiber/mode) -- despite the database's broader
scope (INT and others, per IACOB's own published description), this SSA
service's public rows are Mercator/HERMES and NOT/FIES only, confirmed
live.
"""

from __future__ import annotations

import re
from datetime import date, datetime

import requests

from sync.base import RawObservation

SSA_URL = "http://ocan.iac.es:8080/iacob/jsp/ssap.jsp"

# Field order within each <TR>, matching the service's declared FIELD list
# (see module docstring) -- position, not name, since the response can't be
# parsed as a real VOTable at all (the malformed TIME/TIMESTAMP datatype).
_COL_ACREF = 1
_COL_LOCATION = 4
_COL_TIME = 5
_COL_FILENAME = 6
_COL_TARGET = 7
_COL_INSTRUMENT = 12

_TR_RE = re.compile(r"<TR>(.*?)</TR>", re.DOTALL)
_TD_RE = re.compile(r"<TD>(.*?)</TD>", re.DOTALL)
_CDATA_RE = re.compile(r"^<!\[CDATA\[(.*)\]\]>$", re.DOTALL)


def _parse_row(row_xml: str) -> list[str]:
    return [td.strip() for td in _TD_RE.findall(row_xml)]


def _clean_acref(raw: str) -> str | None:
    """Strips the CDATA wrapper and the literal double-quotes the service
    puts around the URL itself (confirmed live: `<![CDATA["https://...
    fits"]]>`, quotes and all) -- see module docstring."""
    match = _CDATA_RE.match(raw)
    value = match.group(1) if match else raw
    return value.strip().strip('"') or None


def _parse_location(raw: str) -> tuple[float | None, float | None]:
    parts = raw.split(",")
    if len(parts) != 2:
        return None, None
    try:
        return float(parts[0]), float(parts[1])
    except ValueError:
        return None, None


def _parse_obs_date(raw: str) -> date | None:
    """"YYYY-MM-DD HH:MM:SS[.f]" -> date. Only the date part is kept
    (RawObservation.obs_date is a plain date, same as every other archive
    module here)."""
    date_part = raw.split(" ", 1)[0]
    try:
        return date.fromisoformat(date_part)
    except ValueError:
        return None


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    if cursor.get("synced_at"):
        return [], cursor

    response = requests.get(
        SSA_URL,
        params={"REQUEST": "queryData", "POS": "0,0", "SIZE": "360"},
        timeout=(15, 180),
    )
    response.raise_for_status()

    records = []
    for row_xml in _TR_RE.findall(response.text):
        cols = _parse_row(row_xml)
        if len(cols) <= _COL_INSTRUMENT:
            continue

        filename = cols[_COL_FILENAME]
        if not filename:
            continue

        ra, dec = _parse_location(cols[_COL_LOCATION])
        target = cols[_COL_TARGET].strip()

        records.append(
            RawObservation(
                archive_obs_id=filename,
                archive_url=_clean_acref(cols[_COL_ACREF]) or filename,
                instrument=cols[_COL_INSTRUMENT].strip() or None,
                obs_date=_parse_obs_date(cols[_COL_TIME]),
                ra=ra,
                dec=dec,
                raw_target_name=target or None,
            )
        )

    new_cursor = {"synced_at": datetime.now().isoformat(), "row_count": len(records)}
    return records, new_cursor
