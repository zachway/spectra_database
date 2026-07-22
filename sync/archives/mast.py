"""MAST (HST, IUE, FUSE) — VO-TAP service at mast.stsci.edu/vo-tap/, no native Gaia column.

The old objid/obsid reconciliation concern turns out to be moot: each
ivoa.obscore row already carries a directly-usable access_url (confirmed
live — a real 722KB FITS file, 200 OK), so there's no need to reconcile
namespaces to build a deep link at all.

TAP endpoint found by reading the VO-TAP landing page's own nav links (no
docs page listed it directly): mast.stsci.edu/vo-tap/api/v0.1/caom exposes
ivoa.obscore. Real ADQL, real TAP_SCHEMA.

Originally scoped to obs_collection='HST' with access_format='application/
fits' (filters out the thumbnail/preview jpgs that share the same obs_id).
Extended to IUE and FUSE: same dataproduct_type='spectrum' filter works
live for both (955,434 / 1,532,075 total rows respectively), but their
access_format is 'image/fits', not HST's 'application/fits' — confirmed
live, that's the entire reason the original "not yet checked" note existed,
not a genuine access problem.

IUE/FUSE need one more thing HST didn't: a single obs_id there returns many
rows, one per processing stage/file (raw, calibrated, housekeeping,
trailer logs, ...) — confirmed live, one IUE obs_id alone had 6 variants,
one FUSE obs_id had 15. HST's access_format filter already yields exactly
one row per obs_id on its own, so this never showed up before. Both IUE
and FUSE consistently expose one clearly-canonical merged/calibrated
product per observation, named with a `_vo.fits` suffix (e.g.
"lwr01024mxlo_vo.fits", "i801010100000nvo4ttagfcal_vo.fits") — MAST's own
"VO-ready" product convention, not something specific to either mission.
Tried filtering this in SQL directly (`access_url LIKE '%_vo.fits'`) but a
leading-wildcard LIKE hit a genuine 504 Gateway Timeout on this service —
deduped client-side instead: within each fetched page, group by obs_id and
keep the `_vo.fits` variant when present, otherwise the first row seen.
Harmless no-op for HST (already exactly one row per obs_id, so "first row
seen" is the only row either way) — this dedup applies uniformly across
all three collections rather than special-casing HST out of it.

No cliff found for obs_collection='HST' alone — unlike CADC (used for
gemini.py/cfht_cadc.py), ORDER BY t_min is fast here (20,000 rows in 0.7s,
no truncation). Re-confirmed live with IUE/FUSE included in the same
query (despite the extra per-obs_id row multiplicity): still no cliff.
Standard TOP+ORDER BY+watermark pagination works.

Still not covered: obs_collection='JWST' hit a genuine 504 Gateway Timeout
on the very query shape that works for HST/IUE/FUSE — a real server-side
issue, not a row-count or sort cliff, needs its own investigation pass.

s_ra/s_dec can be masked on real rows (calibration exposures like WAVE/
DEUTERIUM lamp exposures lack real sky coordinates) — confirmed live, it
crashes the matcher's KD-tree build outright if not handled (NaN, not just
wrong). Filtered via clean_float + dropping records with no position, same
as the existing ra/dec-required check in sync.matcher.
"""

from astropy.time import Time

from sync.base import RawObservation, clean_float, make_tap_service

TAP_URL = "https://mast.stsci.edu/vo-tap/api/v0.1/caom"

QUERY = """
SELECT TOP {page_size} obs_id, s_ra, s_dec, t_min, instrument_name, target_name, access_url
FROM ivoa.obscore
WHERE dataproduct_type='spectrum' AND obs_collection IN ('HST', 'IUE', 'FUSE')
AND access_format IN ('application/fits', 'image/fits')
AND t_min > {last_t_min}
ORDER BY t_min ASC
"""

PAGE_SIZE = 20000

# MAST's own "VO-ready" merged/calibrated product naming convention for
# IUE/FUSE (confirmed live on both) -- the one row per obs_id worth
# keeping when an obs_id has several (raw/calibration/housekeeping/...).
_CANONICAL_SUFFIX = "_vo.fits"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_t_min = cursor.get("last_t_min", 0)

    tap = make_tap_service(TAP_URL)
    query = QUERY.format(page_size=PAGE_SIZE, last_t_min=last_t_min)
    table = tap.search(query, maxrec=PAGE_SIZE).to_table()

    max_t_min = last_t_min
    by_obs_id: dict[str, dict] = {}
    for row in table:
        t_min = float(row["t_min"])
        max_t_min = max(max_t_min, t_min)

        obs_id = str(row["obs_id"])
        access_url = str(row["access_url"])
        existing = by_obs_id.get(obs_id)
        if existing is None or access_url.endswith(_CANONICAL_SUFFIX):
            by_obs_id[obs_id] = {
                "t_min": t_min,
                "access_url": access_url,
                "instrument_name": str(row["instrument_name"]),
                "s_ra": row["s_ra"],
                "s_dec": row["s_dec"],
                "target_name": str(row["target_name"]),
            }

    records = [
        RawObservation(
            archive_obs_id=obs_id,
            archive_url=data["access_url"],
            instrument=data["instrument_name"],
            obs_date=Time(data["t_min"], format="mjd").to_datetime().date(),
            ra=clean_float(data["s_ra"]),
            dec=clean_float(data["s_dec"]),
            raw_target_name=data["target_name"],
        )
        for obs_id, data in by_obs_id.items()
    ]

    new_cursor = {"last_t_min": max_t_min if records else last_t_min}
    return records, new_cursor
