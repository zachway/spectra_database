"""LBT — PEPSI spectrograph, via a real IVOA TAP service.

archive.lbto.org (a Vue.js SPA, "portal-gui") calls a TAP endpoint at
https://archive.lbto.org/tap — found by grepping the app's own JS bundle
for URL literals, not documented anywhere findable. The `lbt` schema has
one table per instrument (irtc, lbc, lbt, lbti, luci, mods, pepsi, pis);
scoped to pepsi here since it's spectroscopy-only (no imaging mode at
all), unlike mods/luci which mix imaging and spectroscopy in the same
table — same-shape additions later if wanted, not done here.

No ObsCore, no access_url column — ra/dec are sexagesimal strings (not
decimal degrees, unlike every other TAP-based archive here), parsed via
astropy. imagetyp='object' cleanly excludes calibration frames (bias/
flat/comp/etalon/test/traces — confirmed live via DISTINCT).

archive_url points at the general search portal, not a specific file: the
only real download mechanism is an async job system (submit the entire
search-form state as a job, poll /jobs, extract a result URL from the
completed job) — confirmed live, no direct-file URL and no query-param
deep-link exist. That's designed for bulk "download my whole search"
workflows, not per-file lookups, and implementing it just to construct
one URL column would be wildly disproportionate to how every other
archive here works.

`object` sometimes already reports "Gaia DR3 <source_id>" directly
(confirmed live) — parsed straight into gaia_source_id when it matches
that pattern, skipping SIMBAD entirely for those (guaranteed-accurate, no
round trip needed). Everything else (IRAS/TIC/TOI designations, etc.)
goes through as raw_target_name for the normal SIMBAD-then-position
fallback. ra/dec are always populated when parseable regardless — kept as
the raw-position audit trail same as every other archive, not just for
records that fall through to positional matching.
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
SELECT TOP {page_size} object, ra, dec, date_obs, file_name, propid
FROM lbt.pepsi
WHERE imagetyp = 'object' AND date_obs > '{last_date_obs}'
ORDER BY date_obs ASC
"""

PAGE_SIZE = 5000

SEARCH_PORTAL_URL = "https://archive.lbto.org"

GAIA_OBJECT_RE = re.compile(r"^Gaia\s+DR3\s+(\d+)$", re.IGNORECASE)


def _clean_str(value) -> str | None:
    if value is None or np.ma.is_masked(value):
        return None
    text = str(value).strip()
    return text or None


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_date_obs = cursor.get("last_date_obs", "0000-01-01T00:00:00.000")

    tap = make_tap_service(TAP_URL)
    query = QUERY.format(page_size=PAGE_SIZE, last_date_obs=last_date_obs)
    table = tap.search(query).to_table()

    records = []
    max_date_obs = last_date_obs
    for row in table:
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
                archive_obs_id=_clean_str(row["file_name"]) or f"pepsi-{date_obs}",
                archive_url=SEARCH_PORTAL_URL,
                instrument="PEPSI",
                obs_date=Time(date_obs).to_datetime().date(),
                program_id=_clean_str(row["propid"]),
                gaia_source_id=gaia_source_id,
                ra=ra,
                dec=dec,
                raw_target_name=raw_target_name,
            )
        )

    return records, {"last_date_obs": max_date_obs}
