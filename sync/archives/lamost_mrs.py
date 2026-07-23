"""LAMOST DR11 MRS (Medium Resolution Spectrograph) — same SQL API as lamost.py
(LRS), different table and deep link.

`med_combined` is the per-target combined-spectrum table (one row per
obsid+mobsid), reached the same way lamost.py's endpoint was found: not
documented on the SQL page's own table list (only med_catalogue/med_stellar/
med_mec/med_plan/med_inputcatalog are listed there), but it's the table
backing the "Medium Resolution Catalogue Query" web form
(/dr11/v2.0/medcas/search — field names in that form's HTML are literally
`med_combined.<column>`) and confirmed live queryable through the same
unauthenticated SQL API as everything else here.

MRS has no CLASS column at all (confirmed live against med_catalogue's own
column list) — unlike LRS, which spans star/galaxy/QSO, MRS only ever
targets stars, so there's nothing to filter on beyond gaia_source_id itself.

obsid is not unique in med_combined: MRS takes multiple exposures per
target, each further split by band (B/R) and epoch, each with its own
`mobsid` (e.g. "58890200383556119R") — confirmed live, one real obsid had 8+
distinct mobsid rows. But `file`/`spec` (the combined-spectrum product
covering both bands, all epochs) are identical across every mobsid row for
a given obsid — confirmed live (single obsid, 5 different mobsid rows, one
`file` value). SELECT DISTINCT on the obsid-level columns collapses this
cleanly server-side rather than pulling and deduping ~9x the necessary rows
client-side (47M raw rows vs 5.14M distinct obsid, confirmed live via
COUNT) — throughput with DISTINCT is still ~1000 rows/sec (10,000 rows in
~9.5s, confirmed live), better than LRS's ~500/sec despite the extra work,
so no separate page-size tuning was needed.

Deep link: found by brute-force probing plausible paths (medspectrum/fits/,
spectrum/medfits/, mrs/spectrum/fits/, ...) since MRS has no equivalent of
lrs_spectrum.js's readable download-button href to read the pattern from
directly. `medspectrum/fits/{obsid}` confirmed live as the one real hit
(Content-Type: application/gzip, valid gzip, unpacks to a real
`med-58025-HIP507401_sp02-003.fits`) — every other guess 404s dressed up as
a 200 (its own JSON error body, Content-Type: application/json).
"""

import requests
from astropy.time import Time

from sync.base import RawObservation

SQL_URL = "https://www.lamost.org/dr11/v2.0/sql/q"

QUERY = """
select distinct obsid, ra, dec, mjd, gaia_source_id
from med_combined
where gaia_source_id is not null and obsid > {last_obsid}
order by obsid
limit {page_size}
"""

PAGE_SIZE = 10000

SPECTRUM_URL = "https://www.lamost.org/dr11/v2.0/medspectrum/fits/{obsid}"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_obsid = cursor.get("last_obsid", 0)

    resp = requests.post(
        SQL_URL,
        data={"sql": QUERY.format(last_obsid=last_obsid, page_size=PAGE_SIZE), "output.fmt": "json"},
        timeout=120,
    )
    resp.raise_for_status()
    rows = resp.json()

    records = []
    max_obsid = last_obsid
    for row in rows:
        obsid = int(row["obsid"])
        max_obsid = max(max_obsid, obsid)
        records.append(
            RawObservation(
                archive_obs_id=str(obsid),
                archive_url=SPECTRUM_URL.format(obsid=obsid),
                instrument="LAMOST-MRS",
                obs_date=Time(int(row["mjd"]), format="mjd").to_datetime().date(),
                gaia_source_id=int(row["gaia_source_id"]),
            )
        )

    return records, {"last_obsid": max_obsid}
