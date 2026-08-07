"""Chandra X-ray Observatory — TAP (cxc.observation), no native Gaia column.

Real, standards-compliant, no-auth TAP service at cda.harvard.edu/cxctap
(ivoid ivo://cxc.harvard.edu/cda), found via the same reg.g-vo.org registry
sweep used elsewhere in this project. The endpoint 303-redirects a sync
query to its actual result URL -- pyvo (like every other TAP call in this
project) follows that transparently, but a bare `curl` without `-L` looks
like an empty response and could get wrongly written off as dead.

X-ray grating spectroscopy, not imaging: `cxc.observation`'s `grating`
column is `NONE`/`HETG`/`LETG` -- filtered to HETG/LETG only, which cleanly
isolates real dispersed-spectrum exposures from Chandra's much larger
imaging-only holdings (confirmed live: 3,243 real archived/observed grating
rows out of the full table). `status` is also filtered to `archived`/
`observed` (a real exposure has actually happened) excluding `unobserved`/
`untriggered` (scheduled but not yet taken -- no data exists yet, same
reasoning as excluding calibration frames elsewhere in this project).

instrument records grating and detector together, e.g. "HETG (ACIS-S)" --
grating is what actually sets the spectral resolving power (the thing
INSTRUMENT_RESOLVING_POWER in webapp/app.py keys off), but the detector
(ACIS-S/ACIS-I/HRC-S/HRC-I) is real, distinct metadata worth keeping rather
than discarding.

No access_url/ObsCore shape here (unlike most other TAP archives in this
project) -- this table is CDA's own observation-log schema, not ivoa.obscore.
archive_url instead points at the real per-observation archive browser page,
`chaser/startViewer.do?menuItem=details&obsid=...` (confirmed live, 200, a
real "Chandra Data Archive: Observation Viewer" page) -- same "point at a
real page in the home archive" convention as lbt.py's portal link and
ing.py's displayHeader link, not a direct FITS download (this table has no
such column to build one from, and CDA's actual data retrieval is a
multi-step download-request flow disproportionate to build for one link).

reduction_status intentionally left unset -- no calib_level-equivalent
column exists on this table (unlike the ObsCore-based archives that use
reduction_status_from_calib_level), and `status='archived'` describes
archive-ingestion state, not calibration level. Honestly 'unknown' rather
than guessed, same reasoning as most archives in project_reduction_status_
tracking notes.

No cliff found: a single unbounded TOP 5000 pull (the whole HETG/LETG
history) returns in well under a second, so this doesn't need aggressive
paging the way eso.py/gemini.py do -- PAGE_SIZE is generous headroom over
the current total, not a tuned-against-a-real-limit value.

Confirmed live end-to-end: Proxima Centauri alone has 4 real HETG exposures
(obsids 2388/12360/22185/22186) and 4 real LETG exposures (obsids
19708/20073/20080/20084), all with real, populated ra/dec (0 masked across
the whole HETG/LETG/archived/observed set) and real ISO-8601 start_date
timestamps.
"""

from __future__ import annotations

from datetime import datetime

from sync.base import RawObservation, clean_float, make_tap_service

TAP_URL = "https://cda.harvard.edu/cxctap"

QUERY = """
SELECT TOP {page_size} obsid, target_name, ra, dec, instrument, grating, start_date
FROM cxc.observation
WHERE grating IN ('HETG', 'LETG')
  AND status IN ('archived', 'observed')
  AND start_date > '{last_start_date}'
ORDER BY start_date ASC
"""

PAGE_SIZE = 5000

# Chandra observations pre-date this project's cursor format -- the mission
# launched 1999-07-23, so any fixed sentinel before that covers the full
# archive on a first run.
EPOCH = "1999-01-01T00:00:00"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_start_date = cursor.get("last_start_date", EPOCH)

    tap = make_tap_service(TAP_URL)
    query = QUERY.format(page_size=PAGE_SIZE, last_start_date=last_start_date)
    table = tap.search(query, maxrec=PAGE_SIZE).to_table()

    max_start_date = last_start_date
    records = []
    for row in table:
        start_date_str = str(row["start_date"])
        max_start_date = max(max_start_date, start_date_str)

        detector = str(row["instrument"])
        grating = str(row["grating"])
        obs_dt = datetime.fromisoformat(start_date_str)

        records.append(
            RawObservation(
                archive_obs_id=str(row["obsid"]),
                archive_url=f"https://cda.harvard.edu/chaser/startViewer.do?menuItem=details&obsid={row['obsid']}",
                instrument=f"{grating} ({detector})",
                obs_date=obs_dt.date(),
                ra=clean_float(row["ra"]),
                dec=clean_float(row["dec"]),
                raw_target_name=str(row["target_name"]),
            )
        )

    new_cursor = {"last_start_date": max_start_date}
    return records, new_cursor
