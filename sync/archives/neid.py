"""NEID (WIYN telescope, Kitt Peak) -- TAP (neidl2), no native Gaia column.

Real, standards-compliant, no-auth TAP service at neid.ipac.caltech.edu/TAP/sync
-- confirmed live via TAP_SCHEMA.tables, which lists real tables neidl1/neidl2
(L1/L2 extracted, wavelength-calibrated 1D spectra) plus solar variants
neidsolarl1/neidsolarl2 (NEID's dedicated solar feed -- the Sun, not a star,
excluded) and neidl1_all/neidl2_all/neidsolar*_all (unexplored, not used).
neidl2 is used here, not neidl1: per its own TAP_SCHEMA description it's the
fully wavelength-calibrated, RV-ready extracted-spectrum tier, the more
appropriate "final science product" of the two. Backed by an Oracle database,
not the DaCHS/PostgreSQL engine behind most other TAP archives in this
project -- confirmed live: a malformed nested-subquery test surfaced a
literal "ORA-00937: not a single-group group function" error.

Real columns confirmed via TAP_SCHEMA.columns and a live sample query:
expid (int, real per-exposure primary key), object (target/frame name),
obstype ('Eng'/'Sci' -- 21,518 Sci / 16 Eng of 21,534 total rows confirmed
live, no other values), datalvl (uniformly 2 across the *entire* table --
neidl2 only ever holds the fully-reduced L2 tier, no raw/partial rows
co-mingled the way naoj.py's single table has to disambiguate), obsdate (a
string whose first 10 characters are already a plain ISO date -- used
directly rather than parsing the full variable-precision timestamp),
obsjd (float Julian Date -- confirmed live 0 nulls and 0 duplicate values
across the whole table, unlike xmm.py/naoj.py this needs no tie-handling
as a watermark), tcsra/tcsdec/tcsrad/tcsdecd (the real telescope-pointing
coordinates for that exposure, confirmed live 0 nulls) vs qra/qdec/qrad/
qdecd (the queue/planned target's catalog coordinates, kept separate --
tcsrad/tcsdecd is what's used here as ra/dec), program, obsmode ('hr'/'he'
fiber mode -- 19,667/1,867 of 21,534, confirmed live 0 nulls), and
simbadmainid (a SIMBAD name the pipeline itself has already resolved, e.g.
'HD  89269' -- not read here since this project's own generic
discover_stars step does that independently, but it corroborates that
object/raw_target_name is genuinely resolvable).

987 distinct object names confirmed live, topped by real, well-known bright
RV-survey targets: HD 10700 (Tau Ceti, 1,062 obs), HD 185144, HD 127334,
HD 89269, HD 26965 (40 Eri A, 181 obs), HD 219134, alongside occasional
solar-system calibration targets (e.g. 'Venus', 276 obs) and a small number
(19 of 21,534, confirmed live) of LFC/"Cal"-named hybrid calibration-during-
science frames -- left unfiltered rather than hand-excluded, same "simply
fails to resolve downstream" reasoning gtc.py gives for its own unfiltered
free-text object names.

No cliff found: a single unbounded TOP 50000 pull (real headroom over the
confirmed 21,534-row total) returns in ~9s -- standard TOP+watermark on
obsjd, same shape as dao.py/mast.py.

archive_url: this table has no ObsCore access_url-equivalent column, and the
archive's own web frontend (neid.ipac.caltech.edu/search.php) is a Firefly-
based single-page app (confirmed live -- its HTML explicitly references
"firefly") with no discoverable static per-exposure deep link (guessed
getdata.php/getfile.php direct-file endpoints both confirmed live 404) --
archive_url instead points at that general search portal, same "point at a
real page in the home archive, not a fabricated deep link" convention as
lbt.py's own no-direct-file-URL case.

An 18-month proprietary period applies to the actual FITS data, but not to
this TAP metadata -- confirmed live that very recent (2026-02) exposures are
already fully populated here (real target name, real tcsra/tcsdec, real
obsjd), same "an embargoed row still answers this project's core question"
convention as salt_hrs.py/harpsn_tng.py including their own embargoed rows.

reduction_status is hardcoded 'reduced': neidl2 is specifically the
pipeline's final wavelength-calibrated, RV-ready extracted-spectrum tier
(datalvl=2 on literally every row, confirmed live), unlike naoj.py's single
table mixing raw/reduced product tiers that need disambiguating.

INSTRUMENT_RESOLVING_POWER / INSTRUMENT_WAVELENGTH_RANGE_NM (webapp/app.py)
deliberately do NOT gain entries for NEID here, despite the task brief's own
suggested range (R ~ 110,000-190,000) -- multiple live lookups attempted
this session (NEID/PSU/NOIRLab/Wikipedia pages, the arXiv and Semantic
Scholar APIs) all failed to turn up a citable published number (dead links,
empty responses, or rate-limited), so this is left out rather than guessed,
same fallback convention that dict's own comment block documents for other
unconfirmed instruments.
"""

from __future__ import annotations

from datetime import date

from sync.base import RawObservation, clean_float, make_tap_service

TAP_URL = "https://neid.ipac.caltech.edu/TAP/sync"

SEARCH_URL = "https://neid.ipac.caltech.edu/search.php"

QUERY = """
SELECT TOP {page_size} expid, object, obsdate, obsjd, tcsrad, tcsdecd, program, obsmode
FROM neidl2
WHERE obsjd > {last_obsjd}
ORDER BY obsjd ASC
"""

PAGE_SIZE = 50000

# NEID saw first light in 2019 (commissioning) / began science operations in
# 2020 -- 0 is a safe sentinel below any real obsjd (a Julian Date around
# 2.459e6) and covers the full archive on a first run.
EPOCH = 0


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_obsjd = cursor.get("last_obsjd", EPOCH)

    tap = make_tap_service(TAP_URL)
    query = QUERY.format(page_size=PAGE_SIZE, last_obsjd=last_obsjd)
    table = tap.search(query, maxrec=PAGE_SIZE).to_table()

    max_obsjd = last_obsjd
    records = []
    for row in table:
        obsjd = float(row["obsjd"])
        max_obsjd = max(max_obsjd, obsjd)

        obsdate_str = str(row["obsdate"])
        obs_mode = str(row["obsmode"]).strip().upper() if row["obsmode"] is not None else None
        instrument = f"NEID ({obs_mode})" if obs_mode else "NEID"

        records.append(
            RawObservation(
                archive_obs_id=str(row["expid"]),
                archive_url=SEARCH_URL,
                instrument=instrument,
                obs_date=date.fromisoformat(obsdate_str[:10]),
                program_id=str(row["program"]) if row["program"] is not None else None,
                ra=clean_float(row["tcsrad"]),
                dec=clean_float(row["tcsdecd"]),
                raw_target_name=str(row["object"]).strip() or None,
                reduction_status="reduced",
            )
        )

    new_cursor = {"last_obsjd": max_obsjd}
    return records, new_cursor
