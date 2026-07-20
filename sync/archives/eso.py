"""ESO Science Archive — TAP (ivoa.ObsCore), no native Gaia column.

No upload-JOIN support (confirmed earlier), so this pulls the full spectrum
table incrementally and lets the generic positional_easy_match path in
sync.matcher do the cross-match locally. ~2.4M spectrum rows total — this
paginates by t_min (MJD) watermark rather than pulling it all in one shot.
"""

import pyvo
from astropy.time import Time

from sync.base import RawObservation

TAP_URL = "http://archive.eso.org/tap_obs"

QUERY = """
SELECT TOP {page_size} dp_id, s_ra, s_dec, t_min, instrument_name, target_name, proposal_id
FROM ivoa.ObsCore
WHERE dataproduct_type='spectrum' AND obs_collection != ''
AND t_min > {last_t_min}
ORDER BY t_min ASC
"""

# Row cap per page — ESO TAP has no documented hard limit once queried
# correctly, but paginating keeps individual requests bounded regardless.
PAGE_SIZE = 50000

DATASET_LANDING_PAGE = "https://archive.eso.org/dataset/{dp_id}"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_t_min = cursor.get("last_t_min", 0)

    tap = pyvo.dal.TAPService(TAP_URL)
    query = QUERY.format(last_t_min=last_t_min, page_size=PAGE_SIZE)
    # pyvo defaults maxrec to ~20000 regardless of the ADQL TOP clause —
    # confirmed live (DALOverflowWarning) — so it must be set explicitly.
    table = tap.search(query, maxrec=PAGE_SIZE).to_table()

    records = []
    max_t_min = last_t_min
    for row in table:
        t_min = float(row["t_min"])
        max_t_min = max(max_t_min, t_min)
        dp_id = str(row["dp_id"])
        records.append(
            RawObservation(
                archive_obs_id=dp_id,
                archive_url=DATASET_LANDING_PAGE.format(dp_id=dp_id),
                instrument=str(row["instrument_name"]),
                obs_date=Time(t_min, format="mjd").to_datetime().date(),
                program_id=str(row["proposal_id"]),
                ra=float(row["s_ra"]),
                dec=float(row["s_dec"]),
                raw_target_name=str(row["target_name"]),
            )
        )

    # If this page came back short of PAGE_SIZE, we've reached the end of
    # what's currently available — otherwise leave the watermark where it is
    # so the next run picks up mid-stream rather than skipping ahead.
    new_cursor = {"last_t_min": max_t_min if records else last_t_min}
    return records, new_cursor
