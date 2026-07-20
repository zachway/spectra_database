"""MAST (HST) — VO-TAP service at mast.stsci.edu/vo-tap/, no native Gaia column.

The old objid/obsid reconciliation concern turns out to be moot: each
ivoa.obscore row already carries a directly-usable access_url (confirmed
live — a real 722KB FITS file, 200 OK), so there's no need to reconcile
namespaces to build a deep link at all.

TAP endpoint found by reading the VO-TAP landing page's own nav links (no
docs page listed it directly): mast.stsci.edu/vo-tap/api/v0.1/caom exposes
ivoa.obscore. Real ADQL, real TAP_SCHEMA. `access_format='application/fits'`
filters out the thumbnail/preview jpgs that share the same obs_id.

No cliff found for obs_collection='HST' — unlike CADC (used for
gemini.py/cfht_cadc.py), ORDER BY t_min is fast here (20,000 rows in 0.7s,
no truncation). Standard TOP+ORDER BY+watermark pagination works.

Not covered: obs_collection='JWST' on this same service hit a genuine
504 Gateway Timeout on the very query shape that works for HST — a real
server-side issue, not a row-count or sort cliff, needs its own
investigation pass before adding. IUE/FUSE (MAST's older collections)
weren't checked either.

s_ra/s_dec can be masked on real rows (calibration exposures like WAVE/
DEUTERIUM lamp exposures lack real sky coordinates) — confirmed live, it
crashes the matcher's KD-tree build outright if not handled (NaN, not just
wrong). Filtered via clean_float + dropping records with no position, same
as the existing ra/dec-required check in sync.matcher.
"""

import pyvo
from astropy.time import Time

from sync.base import RawObservation, clean_float

TAP_URL = "https://mast.stsci.edu/vo-tap/api/v0.1/caom"

QUERY = """
SELECT TOP {page_size} obs_id, s_ra, s_dec, t_min, instrument_name, target_name, access_url
FROM ivoa.obscore
WHERE dataproduct_type='spectrum' AND obs_collection='HST' AND access_format='application/fits'
AND t_min > {last_t_min}
ORDER BY t_min ASC
"""

PAGE_SIZE = 20000


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_t_min = cursor.get("last_t_min", 0)

    tap = pyvo.dal.TAPService(TAP_URL)
    query = QUERY.format(page_size=PAGE_SIZE, last_t_min=last_t_min)
    table = tap.search(query, maxrec=PAGE_SIZE).to_table()

    records = []
    max_t_min = last_t_min
    for row in table:
        t_min = float(row["t_min"])
        max_t_min = max(max_t_min, t_min)
        records.append(
            RawObservation(
                archive_obs_id=str(row["obs_id"]),
                archive_url=str(row["access_url"]),
                instrument=str(row["instrument_name"]),
                obs_date=Time(t_min, format="mjd").to_datetime().date(),
                ra=clean_float(row["s_ra"]),
                dec=clean_float(row["s_dec"]),
                raw_target_name=str(row["target_name"]),
            )
        )

    new_cursor = {"last_t_min": max_t_min if records else last_t_min}
    return records, new_cursor
