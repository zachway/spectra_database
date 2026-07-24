"""ELODIE (OHP, France, 1994-2006, decommissioned) — plain-text table scrape.

No TAP/API, and no per-object query needed either: the "advanced query"
CGI (fE.cgi) returns the *entire* archive as one plain-text table when no
object filter (`o=`) is given (confirmed live: 35,535 rows in one request,
matching the archive's own published total) — a genuine one-shot bulk dump,
same shape as rave.py, not a per-target scrape like lick.py/carmenes_caha.py.
Final, decommissioned instrument (last observation 2006, fully public since
2011) — one full pull is enough forever.

Half the archive is calibration frames, not science exposures: `imatyp`
starting with "OBTH" (Th-Ar arc lamp, confirmed live: 19,289 of 35,535 rows)
vs "OBJ*" (real object spectra, 16,246 rows) — filtered at parse time via a
prefix check, no separate metadata field needed.

Coordinates come back as a single packed J2000 string ("J152749.7+290620",
confirmed live on every row) rather than separate ra/dec columns — parsed
into `HH:MM:SS.s`/`+DD:MM:SS` and handed to SkyCoord the same way lbt.py
does for its own sexagesimal ra/dec, just needing the colons inserted first
since the source has none.

Real catalog names are in `objname` (e.g. "*BETA_CRB_-_HD137909") — both
name and position are available, unlike feros_gavo.py/flashheros_gavo.py,
so this goes through the matcher's normal identifier-then-position order.

The download endpoint (`fE.cgi?...&o=elodie:{dataset}/{imanum}`) needs
`http://`, not `https://` — the archive doesn't serve TLS at all (confirmed
live: the site is plain HTTP only).

The response is a fixed-width plain-text table, not whitespace-delimited —
confirmed live that a naive `line.split()` misparses/shifts columns on rows
where `objname` or the packed J2000 coordinate is blank (both happen: e.g.
one real row has an empty objname and a literal control character, not
whitespace, in place of a missing coordinate) — parsed via fixed character
offsets instead, measured directly against a real row rather than trusting
the header's own dashed column-width line (which turned out to be off by
one relative to the actual data rows). `dataset` (always a clean 8-digit
string, confirmed on all 35,535 rows) and `imanum` are unaffected by either
blank-field case, so archive_obs_id/obs_date/download-URL construction stay
reliable even on a row with a garbage or missing coordinate.
"""

from __future__ import annotations

from datetime import datetime

import astropy.units as u
import requests
from astropy.coordinates import SkyCoord

from sync.base import RawObservation

LIST_URL = "http://atlas.obs-hp.fr/elodie/fE.cgi?n=e500&c=o&a=t&ob=objname,dataset,imanum,imatyp&z=d"

DOWNLOAD_URL = "http://atlas.obs-hp.fr/elodie/fE.cgi?n=e500&c=i&z=s1d&a=mime:application/fits&o=elodie:{dataset}/{imanum}"

# Fixed character offsets into each data row, measured directly against a
# real row (see module docstring) rather than the header's own column-width
# markers, which are off by one relative to the data.
OBJNAME_SLICE = slice(0, 31)
J2000_SLICE = slice(31, 47)
DATASET_SLICE = slice(52, 60)
IMANUM_SLICE = slice(61, 65)
IMATYP_SLICE = slice(66, 71)


def _parse_j2000(coord: str) -> tuple[float, float] | tuple[None, None]:
    """"JHHMMSS.s+DDMMSS" -> (ra_deg, dec_deg). Not every row has a
    well-formed value here (see module docstring) — anything that doesn't
    match the expected shape falls back to no position rather than raising."""
    if len(coord) != 16 or coord[0] != "J" or coord[9] not in "+-":
        return None, None
    ra_raw, dec_raw = coord[1:9], coord[9:16]
    ra_str = f"{ra_raw[0:2]}:{ra_raw[2:4]}:{ra_raw[4:]}"
    dec_str = f"{dec_raw[0]}{dec_raw[1:3]}:{dec_raw[3:5]}:{dec_raw[5:7]}"
    coord_obj = SkyCoord(ra=ra_str, dec=dec_str, unit=(u.hourangle, u.deg))
    return coord_obj.ra.deg, coord_obj.dec.deg


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    if cursor.get("synced_at"):
        return [], cursor

    response = requests.get(LIST_URL, timeout=(15, 180))
    response.raise_for_status()

    records = []
    for line in response.text.splitlines():
        if not line or line.startswith("#"):
            continue
        imatyp = line[IMATYP_SLICE].strip()
        if not imatyp.startswith("OBJ"):
            continue

        dataset = line[DATASET_SLICE].strip()
        imanum = line[IMANUM_SLICE].strip()
        objname = line[OBJNAME_SLICE].strip()
        ra, dec = _parse_j2000(line[J2000_SLICE].strip())

        records.append(
            RawObservation(
                archive_obs_id=f"{dataset}/{imanum}",
                archive_url=DOWNLOAD_URL.format(dataset=dataset, imanum=imanum),
                instrument="ELODIE",
                obs_date=datetime.strptime(dataset, "%Y%m%d").date(),
                ra=ra,
                dec=dec,
                raw_target_name=objname or None,
            )
        )

    new_cursor = {"synced_at": datetime.now().isoformat(), "row_count": len(records)}
    return records, new_cursor
