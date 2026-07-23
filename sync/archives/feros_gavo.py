"""FEROS Public Spectra (GAVO Heidelberg) — TAP, no positions, name-only match.

Not the same thing as FEROS data already pulled in via eso.py: this is a
small, separate GAVO-hosted DaCHS/SSA service covering FEROS's commissioning
and guaranteed-time spectra from 1999 (MJD 51093-51394), entirely before
ESO's own archive coverage starts (earliest FEROS row there is MJD 52955,
confirmed live) — disjoint date ranges, not a duplicate.

Final, static dataset (FEROS moved fully into ESO's regular archive after
guaranteed time ended) — one full pull is enough forever, same shape as
rave.py.

feros.data has no position column populated at all (confirmed live:
COUNT(ssa_targetpos) is 0 across all 2359 real spectra) — every record here
can only ever go through the matcher's name_resolved path, never
positional_easy_match. Records whose ssa_targname doesn't resolve to a
tracked star are silently skipped, same outcome positional matching would
give a non-tracked target anyway.

Each real spectrum has a paired application/x-votable+xml metadata-only row
alongside its application/fits row (confirmed live: exactly 2x count) —
filtered out via the mime column rather than deduping client-side.
"""

from astropy.time import Time

from sync.base import RawObservation, make_tap_service

TAP_URL = "https://dc.g-vo.org/tap"

QUERY = """
SELECT accref, ssa_targname, ssa_dateobs AS dateobs, ssa_instrument AS instrument
FROM feros.data
WHERE mime = 'application/fits'
"""


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    if cursor.get("synced_at"):
        return [], cursor

    tap = make_tap_service(TAP_URL)
    table = tap.search(QUERY).to_table()

    records = []
    for row in table:
        accref = str(row["accref"])
        records.append(
            RawObservation(
                archive_obs_id=accref,
                archive_url=accref,
                instrument=str(row["instrument"]),
                obs_date=Time(float(row["dateobs"]), format="mjd").to_datetime().date(),
                raw_target_name=str(row["ssa_targname"]),
            )
        )

    new_cursor = {"synced_at": Time.now().isot, "row_count": len(records)}
    return records, new_cursor
