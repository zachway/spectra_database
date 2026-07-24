"""SOPHIE (OHP, France, ELODIE's successor, still active) — plain-text scrape.

Same Pleinpot CGI engine and OHP host as elodie.py, but this table doesn't
support a blank/wildcard "give me everything" query the way ELODIE's e500
table does (confirmed live: no object filter at all, or a bare "%"
wildcard, both return "0 lines were successfully retrieved" — a per-table
config difference, not a URL/encoding mistake). The archive's own
documentation (advanced.html) demonstrates catalog-prefix wildcards instead
("o=GJ%" or "o=HD%" for all Gliese/HD-catalog stars) as the intended way to
pull a broad group — so this iterates a fixed list of common stellar
catalog prefixes rather than a single unfiltered query. Since SOPHIE mostly
observes catalogued RV-survey/exoplanet-host targets, catalog coverage is
good but not complete: HD% alone returns 67,714 rows (confirmed live) of
the ~104,105 the archive reports across all prefixes combined, and a star
cross-matched under a name outside this prefix list (an uncommon informal
name, or a designation system not covered) will be silently missed until
someone extends PREFIXES with the archive's own name-search intent.

Each prefix run is one page — a real page can be tens of thousands of rows
(matches other bulk archives elsewhere in this codebase), not one row per
star.

Fixed-width parsing, same reasoning and same J2000-packed-coordinate shape
as elodie.py (measured directly against real rows since the header's own
column markers aren't reliable — see elodie.py) — objname is a fixed
22-char field, j2000 a fixed 16-char field right after it; seq and date are
right-aligned/fixed-width further along the line.

Object-name search implicitly excludes calibration frames — querying by a
star name only ever returns that star's own science exposures (confirmed:
no bias/flat/ThAr rows appear under any star-name query), unlike ELODIE's
unfiltered dump which mixed both — so no imatyp-equivalent filter is needed
here.
"""

from datetime import date, datetime

import astropy.units as u
import requests
from astropy.coordinates import SkyCoord

from sync.base import RawObservation

LIST_URL = "http://atlas.obs-hp.fr/sophie/sophie.cgi"

DOWNLOAD_URL = "http://atlas.obs-hp.fr/sophie/sophie.cgi?c=i&a=mime:application/fits&o=sophie:[s1d,{seq}]"

# Common stellar catalog prefixes, per the archive's own documented "search
# a whole catalog" pattern (advanced.html) — see module docstring for the
# coverage tradeoff this implies.
PREFIXES = ["HD%", "BD%", "TYC%", "HIP%", "GJ%", "2MASS%", "Gaia DR%"]

OBJNAME_SLICE = slice(0, 22)
J2000_SLICE = slice(22, 38)
SEQ_SLICE = slice(44, 54)
DATE_SLICE = slice(60, 70)


def _parse_j2000(coord: str) -> tuple[float, float] | tuple[None, None]:
    """Same packed "JHHMMSS.s+DDMMSS" shape as elodie.py — see there for
    why malformed values fall back to no position rather than raising."""
    if len(coord) != 16 or coord[0] != "J" or coord[9] not in "+-":
        return None, None
    ra_raw, dec_raw = coord[1:9], coord[9:16]
    ra_str = f"{ra_raw[0:2]}:{ra_raw[2:4]}:{ra_raw[4:]}"
    dec_str = f"{dec_raw[0]}{dec_raw[1:3]}:{dec_raw[3:5]}:{dec_raw[5:7]}"
    coord_obj = SkyCoord(ra=ra_str, dec=dec_str, unit=(u.hourangle, u.deg))
    return coord_obj.ra.deg, coord_obj.dec.deg


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    prefix_index = cursor.get("prefix_index", 0)
    if prefix_index >= len(PREFIXES):
        return [], cursor

    prefix = PREFIXES[prefix_index]
    response = requests.get(
        LIST_URL,
        params={"n": "sophie", "a": "t", "ob": "ra,seq", "c": "o", "o": prefix},
        timeout=(15, 180),
    )
    response.raise_for_status()

    records = []
    for line in response.text.splitlines():
        if not line or line.startswith("#"):
            continue
        objname = line[OBJNAME_SLICE].strip()
        seq = line[SEQ_SLICE].strip()
        date_str = line[DATE_SLICE].strip()
        if not seq or not date_str:
            continue
        ra, dec = _parse_j2000(line[J2000_SLICE].strip())

        try:
            obs_date: date | None = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            obs_date = None

        records.append(
            RawObservation(
                archive_obs_id=seq,
                archive_url=DOWNLOAD_URL.format(seq=seq),
                instrument="SOPHIE",
                obs_date=obs_date,
                ra=ra,
                dec=dec,
                raw_target_name=objname or None,
            )
        )

    new_cursor = {"prefix_index": prefix_index + 1}
    return records, new_cursor
