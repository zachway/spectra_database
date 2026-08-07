"""CCD700 Ondrejov Spectra (Ondrejov 2m Perek Telescope, Czech Republic) — TAP.

Found via the same reg.g-vo.org-style approach as feros_gavo/hermes_mercator
-- a real GAVO DaCHS/SSA service at voarchive.asu.cas.cz, identifier
ivo://asu.cas.cz/ccd700/q/ssa. Same software as feros_gavo.py/
hermes_mercator.py, so the same shortcut applies: the DaCHS host also
exposes a plain TAP endpoint (voarchive.asu.cas.cz/tap, table ccd700.data)
rather than needing SSA cone-search-style POS/SIZE queries for a full-
archive pull.

Coude spectrograph fed by the 700mm camera, confirmed live via the
service's own info page: "typical spectral resolving power is 13000 in
first order around Halpha region and twice in Hbeta" (R~13,000), one-year
proprietary period. Mostly Be stars/emission-line objects per the info
page's keywords ("Optical spectroscopy", "Stars").

Same paired-row mime quirk as feros_gavo/flashheros_gavo: every real
spectrum has a companion application/x-votable+xml metadata row (confirmed
live: 65,378 total rows split 22,325 image/fits + 43,053
application/x-votable+xml, exactly the fits-row count) -- filtered via the
mime column. All 22,325 real rows confirmed live to have a populated
ssa_targname and ssa_location -- normal identifier-then-position matching
applies, not name-only. instrument is a constant "COUDE700" on every row.

ssa_location is a plain text string ("Position ICRS <ra> <dec>"), not a
2-element array the way hermes_mercator.py's ssa_location column comes
back -- parsed by splitting on whitespace and taking the last two tokens.

The service's own "Data updated" metadata reports today's date on every
check this session, and ssa_dateobs (MJD) reaches into 2026 -- still
actively growing, so this paginates by an ssa_dateobs watermark (same
"t_min-style" shape as eso.py/dao.py) rather than a one-shot pull, even
though the whole table comfortably fits in a single page today.

Confirmed live: the server-side `>` comparison against the watermark we
send back doesn't always exclude the exact boundary row -- the same single
accref (its own ssa_dateobs value, re-queried directly, matches nothing
else) kept re-matching `ssa_dateobs > {last_dateobs}` on a second,
otherwise-empty page, most likely the same string-round-trip-vs-stored-
double mismatch documented in irtf_spex.py's own watermark guard -- so this
uses the identical same-timestamp id-dedup guard rather than a one-shot
pull.

embargo is a real populated date string on every row, including several
years in the future -- not used as a proprietary-period filter, same
reasoning feros_gavo.py/hermes_mercator.py give for including
still-embargoed rows (this project doesn't download bytes anyway).
"""

from astropy.time import Time

from sync.base import RawObservation, clean_float, make_tap_service

TAP_URL = "http://voarchive.asu.cas.cz/tap"

QUERY = """
SELECT TOP {page_size} accref, ssa_targname, ssa_dateobs, ssa_location, instrument
FROM ccd700.data
WHERE mime = 'image/fits' AND ssa_dateobs > {last_dateobs}
ORDER BY ssa_dateobs ASC
"""

# Whole table is 22,325 real rows (confirmed live) -- comfortably one page,
# but kept generous rather than hardcoded to today's exact count so future
# growth doesn't silently truncate.
PAGE_SIZE = 100000

INSTRUMENT = "COUDE700"


def _parse_location(location: str) -> tuple[float | None, float | None]:
    # "Position ICRS <ra> <dec>" -- see module docstring.
    if not location:
        return None, None
    parts = location.split()
    if len(parts) < 2:
        return None, None
    return clean_float(parts[-2]), clean_float(parts[-1])


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_dateobs = cursor.get("last_dateobs", 0)
    last_accrefs = set(cursor.get("last_accrefs", []))

    tap = make_tap_service(TAP_URL)
    query = QUERY.format(page_size=PAGE_SIZE, last_dateobs=last_dateobs)
    table = tap.search(query, maxrec=PAGE_SIZE).to_table()

    records = []
    max_dateobs = last_dateobs
    max_dateobs_accrefs: set[str] = set(last_accrefs)
    for row in table:
        accref = str(row["accref"])
        dateobs = clean_float(row["ssa_dateobs"])

        # See module docstring -- guards against re-matching the exact
        # boundary row on the next page (same shape as irtf_spex.py's
        # last_planeids guard).
        if dateobs == last_dateobs and accref in last_accrefs:
            continue

        if dateobs is not None:
            if dateobs > max_dateobs:
                max_dateobs = dateobs
                max_dateobs_accrefs = set()
            if dateobs == max_dateobs:
                max_dateobs_accrefs.add(accref)

        ra, dec = _parse_location(str(row["ssa_location"]) if row["ssa_location"] else "")
        records.append(
            RawObservation(
                archive_obs_id=accref,
                archive_url=accref,
                instrument=INSTRUMENT,
                obs_date=Time(dateobs, format="mjd").to_datetime().date() if dateobs is not None else None,
                ra=ra,
                dec=dec,
                raw_target_name=str(row["ssa_targname"]) or None,
            )
        )

    new_cursor = {"last_dateobs": max_dateobs, "last_accrefs": sorted(max_dateobs_accrefs)}
    return records, new_cursor
