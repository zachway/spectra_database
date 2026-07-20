"""LAMOST DR11 LRS — SQL API, direct Gaia DR3 source_id column.

https://www.lamost.org/dr11/v2.0/sql (form) posts to
https://www.lamost.org/dr11/v2.0/sql/q — a real, unauthenticated, PostgreSQL
(pg_sphere) query API. No REST/TAP docs exist for it; found by reading the
form's action attribute directly. `catalogue` table, gaia_source_id column,
100% populated among CLASS='STAR' rows (11,570,626 / 11,570,626 — better than
the ~97.6% previously assumed for an older DR).

Deep link found the same way — not documented, read out of the viewer page's
own JS (lrs_spectrum.js sets the download button's href to
../spectrum/fits/{obsid}): confirmed live at
https://www.lamost.org/dr11/v2.0/spectrum/fits/{obsid}, no auth needed for
this public DR.

Throughput is the real constraint here, not a row cap: ~500 rows/sec
regardless of ORDER BY (tested with and without — not a sort-cost cliff like
CFHT/KOA, the API is just generally slow). A full initial sync of 11.57M
rows would take hours — fine as a one-time server-side pull, paginated here
at a modest page size to keep each call bounded.
"""

import requests
from astropy.time import Time

from sync.base import RawObservation

SQL_URL = "https://www.lamost.org/dr11/v2.0/sql/q"

QUERY = """
select obsid, ra, dec, mjd, gaia_source_id
from catalogue
where class='STAR' and gaia_source_id is not null and obsid > {last_obsid}
order by obsid
limit {page_size}
"""

PAGE_SIZE = 10000

SPECTRUM_URL = "https://www.lamost.org/dr11/v2.0/spectrum/fits/{obsid}"


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
                instrument="LAMOST",
                obs_date=Time(int(row["mjd"]), format="mjd").to_datetime().date(),
                gaia_source_id=int(row["gaia_source_id"]),
            )
        )

    return records, {"last_obsid": max_obsid}
