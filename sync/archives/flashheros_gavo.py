"""Flash/Heros Public Spectra (GAVO Heidelberg) — TAP, no positions, name-only match.

Same GAVO Heidelberg DaCHS/SSA hosting as feros_gavo.py, found via the same
reg.g-vo.org registry sweep — a different, unrelated instrument (Flash and
Heros are two echelle spectrographs run together on La Silla in the late
1990s bright-star survey era, not affiliated with ESO's FEROS). Confirmed
live: 14,573 real spectra (application/fits rows), real bright-star target
names (e.g. "68 Cyg"). Final, static dataset — one full pull is enough
forever, same shape as rave.py/feros_gavo.py.

flashheros.data has no populated position at all on every row sampled (both
the plain ra/dec columns and ssa_targetpos come back empty) — same as
feros_gavo.py, so every record here can only ever go through the matcher's
name_resolved path.

Same paired-row shape as feros_gavo.py too: each real spectrum has a
sibling application/x-votable+xml metadata-only row (confirmed live: exactly
2x count) — filtered via mime rather than deduping client-side.

Unlike feros.data, flashheros.data has no ssa_instrument *column* — it's a
per-service constant (always "Flash/Heros"), exposed only as a VOTable INFO
comment rather than a per-row field (confirmed live: selecting it raises
"No such field known"), so it's hardcoded here instead of read from a row.

ssa_dateObs is masked on 36 of 14,573 rows (confirmed live) — a bare
Time(float(masked_value), ...) crashes with "Input values for mjd class
must be finite doubles" rather than silently producing NaT, so those rows
go in with obs_date=None instead.
"""

import numpy as np
from astropy.time import Time

from sync.base import RawObservation, make_tap_service

TAP_URL = "https://dc.g-vo.org/tap"

INSTRUMENT = "Flash/Heros"

QUERY = """
SELECT accref, ssa_targname, ssa_dateobs AS dateobs
FROM flashheros.data
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
        dateobs = row["dateobs"]
        obs_date = None if np.ma.is_masked(dateobs) else Time(float(dateobs), format="mjd").to_datetime().date()
        records.append(
            RawObservation(
                archive_obs_id=accref,
                archive_url=accref,
                instrument=INSTRUMENT,
                obs_date=obs_date,
                raw_target_name=str(row["ssa_targname"]),
            )
        )

    new_cursor = {"synced_at": Time.now().isot, "row_count": len(records)}
    return records, new_cursor
