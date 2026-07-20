"""Gemini via CADC — same TAP infrastructure as cfht_cadc.py, different cliff.

CADC hosts Gemini data too (obs_collection 'GEMINI', plus a tiny 'GEMINICADC'
subset — both included). No native Gaia column — positional match.

Real, sharp cliff found live, worse than CFHT's: `ORDER BY t_min` makes even
a 1,000-row query take 72.7s, regardless of page size (10,000 rows timed out
past 90s). Without ORDER BY, the same filters return in 1-4s. So this can't
paginate via TOP+ORDER BY+watermark like eso.py/cfht_cadc.py — instead it
chunks by a fixed 7-day date window (no sort needed at all), the same
approach Gemini's own native REST API required (per earlier research: weekly
chunking, ~260 sci-spectra/week). Watermark is the window start, advancing
7 days per call regardless of row count.

t_min=0.0 is a real sentinel for missing dates (not just theoretical) —
filtered out. Real Gemini spectrum data starts at MJD 51946.48 (2001-01-24,
live-confirmed as MIN(t_min) WHERE t_min > 0) — used as the default cursor
start to avoid iterating empty weeks back to MJD 0.

Deep link: same DataLink resolver as CFHT, confirmed live on a real Gemini
record (GNIRS spectrum, direct FITS file resolved and downloadable).

s_ra/s_dec read via clean_float — can be masked on real rows (confirmed as a
real pattern via mast.py), and a bare float() would turn that into NaN and
crash the matcher's KD-tree build outright.
"""

from urllib.parse import quote

import pyvo
from astropy.time import Time

from sync.base import RawObservation, clean_float

TAP_URL = "https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus"

WINDOW_DAYS = 7
FIRST_REAL_T_MIN = 51946.0  # live-confirmed: MIN(t_min) WHERE t_min > 0

QUERY = """
SELECT obs_publisher_did, s_ra, s_dec, t_min, instrument_name, target_name
FROM ivoa.ObsCore
WHERE obs_collection IN ('GEMINI', 'GEMINICADC') AND dataproduct_type = 'spectrum'
AND t_min >= {window_start} AND t_min < {window_end}
"""

DATALINK_URL = "https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/caom2ops/datalink?ID={did}"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    window_start = cursor.get("window_start", FIRST_REAL_T_MIN)
    window_end = window_start + WINDOW_DAYS

    tap = pyvo.dal.TAPService(TAP_URL)
    query = QUERY.format(window_start=window_start, window_end=window_end)
    table = tap.search(query, maxrec=20000).to_table()

    records = []
    for row in table:
        t_min = float(row["t_min"])
        did = str(row["obs_publisher_did"])
        records.append(
            RawObservation(
                archive_obs_id=did,
                archive_url=DATALINK_URL.format(did=quote(did, safe="")),
                instrument=str(row["instrument_name"]),
                obs_date=Time(t_min, format="mjd").to_datetime().date(),
                ra=clean_float(row["s_ra"]),
                dec=clean_float(row["s_dec"]),
                raw_target_name=str(row["target_name"]),
            )
        )

    return records, {"window_start": window_end}
