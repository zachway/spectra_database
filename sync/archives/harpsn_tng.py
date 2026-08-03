"""HARPS-N @ TNG (Telescopio Nazionale Galileo, La Palma) — TAP via IA2.

Same IA2 (Italian VO center) infrastructure as asiago.py, found in the same
archive-gap survey. `tng.TNG_TAP` is an umbrella table across every TNG
instrument (7.59M rows total, confirmed live) — filtered here to
INSTRUMENT='HARPN' AND policy='FREE' (the archive's own field distinguishing
public from still-proprietary data, used directly instead of guessing an
embargo period the way lick.py has to). OBJECT != 'NONE' filters out
calibration frames at the query level (confirmed live: calibration rows
report RA_RAD=DEC_RAD=0.0 literally, not masked/null — letting those through
would risk a false positional match near RA=0/Dec=0, the same kind of
garbage-sentinel problem koa.py's mjd bound and lbt.py's dataprod filter
solve for their own archives).

A full COUNT(*)/DISTINCT over the unfiltered 7.59M-row table times out
synchronously (confirmed live: "Time out! ... try again ... asynchronous
mode") — but a TOP-bounded, id-watermarked, already-filtered page query
comes back in ~1.5s for 20,000 rows (confirmed live), so this paginates the
same id-watermark way as asiago.py rather than needing TAP_ASYNC.

RA_RAD/DEC_RAD are radians, same convention as asiago.py (same IA2
infrastructure) — every other TAP archive elsewhere in this codebase
reports degrees directly.

DATE_OBS parsing is wrapped in a try/except falling back to obs_date=None —
asiago.py (same IA2 infrastructure) found a real systematic malformation in
this field (a bare trailing "0" instead of a proper ".0" on some rows), not
yet independently confirmed here but cheap to guard against regardless.

EVERY DRS pipeline data product is cataloged as its own row sharing one
exposure timestamp but a distinct id (confirmed live: a single exposure
turned up 18 rows — the raw frame, old-pipeline e2ds/s1d/ccf/bis for fibers
A+B, and a full new-pipeline reprocessing producing S1D/S2D/CCF/
DRIFT_MATRIX/etc for A+B). Left unfiltered this inflated observations/star
to ~493 (vs ~126 for the next-highest archive, cfht_cadc) without any of it
being a distinct observation. OBJECT != 'NONE' does not catch this — it's a
different problem from the calibration-frame sentinel described above.
Fixed two ways: OBS_MODE = 'SCIENCE' in the WHERE clause drops CALIB frames
(FP/ThAr wavelength-cal exposures, drift monitoring, etc) at the query
level; and _is_raw_exposure() keeps only the bare, no-product-suffix
filename (e.g. "HARPN.2015-08-19T00-01-42.732.fits.gz", not
"..._e2ds_A.fits.gz" or "r.HARPN..._S1D_A.fits.gz") client-side, since ADQL
LIKE escaping for a literal underscore wasn't worth the risk of silently
matching wrong. The raw file was chosen as the one-row-per-exposure anchor
over a reduced product like s1d/S1D because it is cataloged with total
coverage back to 2012-07-17 (confirmed live) — the new-pipeline
reprocessing has ~3,090 exposures missing a S1D_A counterpart (an early
~2012-07-17..2013-01-02 gap before reprocessing existed, plus very recent
exposures not yet reprocessed), and old vs new-style s1d/S1D can both exist
for the same exposure, so anchoring on either reduced product would need
extra cross-referencing to avoid re-introducing the same double-count this
fix removes.

This query-level fix does not need extra TAP round-trips: the existing
id-watermark pagination already scopes to the INSTRUMENT/policy/OBJECT
(now +OBS_MODE)-filtered subset before TOP applies, so keeping only raw
exposures client-side just discards ~93% of already-fetched rows rather
than requiring more pages.

Non-stellar contamination (solar-system flux/RV calibration targets like
Europa, Ganymede, Vesta, observed as OBS_MODE='SCIENCE' since they're real
pointed exposures, just not stars) turned out to already be handled: the
matcher's position-sanity check rejects a moving body's coordinates against
the fixed-star catalog, so these were already match_status='skipped' in
prod (confirmed live) before this fix, not corrupting matched data.
"""

import math
import re

from astropy.time import Time

from sync.base import RawObservation, clean_float, make_tap_service

TAP_URL = "http://archives.ia2.inaf.it/vo/tap/tng"

QUERY = """
SELECT TOP {page_size} id, OBJECT, DATE_OBS, RA_RAD, DEC_RAD, file_url, PROGRAM
FROM tng.TNG_TAP
WHERE id > {last_id} AND INSTRUMENT = 'HARPN' AND policy = 'FREE'
  AND OBJECT != 'NONE' AND OBS_MODE = 'SCIENCE'
ORDER BY id ASC
"""

PAGE_SIZE = 20000

RAD_TO_DEG = 180.0 / math.pi

RAW_EXPOSURE_RE = re.compile(r"/(?:A-)?HARPN\.[\dT:.\-]+\.fits(?:\.gz)?$")


def _is_raw_exposure(file_url: str) -> bool:
    return RAW_EXPOSURE_RE.search(file_url) is not None


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_id = cursor.get("last_id", 0)

    tap = make_tap_service(TAP_URL)
    query = QUERY.format(page_size=PAGE_SIZE, last_id=last_id)
    table = tap.search(query, maxrec=PAGE_SIZE).to_table()

    records = []
    max_id = last_id
    for row in table:
        row_id = int(row["id"])
        max_id = max(max_id, row_id)
        file_url = str(row["file_url"])
        if not _is_raw_exposure(file_url):
            continue
        ra_rad = clean_float(row["RA_RAD"])
        dec_rad = clean_float(row["DEC_RAD"])
        try:
            obs_date = Time(str(row["DATE_OBS"]), format="isot").to_datetime().date()
        except ValueError:
            obs_date = None
        records.append(
            RawObservation(
                archive_obs_id=str(row_id),
                archive_url=file_url,
                instrument="HARPS-N",
                obs_date=obs_date,
                program_id=str(row["PROGRAM"]),
                ra=ra_rad * RAD_TO_DEG if ra_rad is not None else None,
                dec=dec_rad * RAD_TO_DEG if dec_rad is not None else None,
                raw_target_name=str(row["OBJECT"]),
            )
        )

    new_cursor = {"last_id": max_id}
    return records, new_cursor
