"""NOIRLab Astro Data Archive (SOAR Goodman Spectrograph) — REST JSON API.

The /tap endpoint 404s — that's a dead end for datalab.noirlab.edu, a
different, unnecessary service. The real, working API is
astroarchive.noirlab.edu, found via its OpenAPI/Swagger schema at
/api/docs/?format=openapi (linked from the docs page's own swagger-ui-init.js,
not from any docs text). POST /api/adv_search/find/, JSON body
{"outfields": [...], "search": [[field, value_or_range], ...]}.

No native Gaia column — positional match. Every returned row already
carries a direct, working `url` field (.../api/retrieve/{md5sum}/) —
confirmed live to a real downloadable FITS file, no separate resolution
step needed.

Field names are non-obvious and had to be reverse-engineered from a live
error message (BADFIELD) that dumped the real available fields: `ra_center`/
`dec_center`/`dateobs_center` (not `ra`/`dec`/`dateobs`), `obs_mode` (not
`obsmode`). `obs_mode` itself is null for Goodman's raw files, so it can't be
used as a spectroscopy filter — instrument identity does that job instead
(`goodman` is a dedicated spectrograph, no imaging ambiguity).

Scoped to `goodman` (SOAR Goodman Spectrograph) for now. NOIRLab hosts
several other dedicated spectrographs on the same API with the identical
query shape — ghts_blue, ghts_red, chiron, echelle, kosmos, arcoiris,
triplespec, cosmos, sami — trivial same-shape additions, just swap the
instrument value.

No hard row cap found, but slower than most: 20,000 rows took 28.7s.
Paginated at 10,000/page here.
"""

import requests
from astropy.time import Time

from sync.base import RawObservation, clean_float

FIND_URL = "https://astroarchive.noirlab.edu/api/adv_search/find/"

PAGE_SIZE = 10000

INSTRUMENT = "goodman"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_dateobs = cursor.get("last_dateobs", "1990-01-01")

    resp = requests.post(
        f"{FIND_URL}?limit={PAGE_SIZE}&format=json&sort=dateobs_center",
        json={
            "outfields": ["md5sum", "ra_center", "dec_center", "dateobs_center", "proposal", "url"],
            "search": [
                ["instrument", INSTRUMENT],
                ["proc_type", "raw"],
                ["obs_type", "object"],
                ["dateobs_center", last_dateobs, "2099-01-01"],
            ],
        },
        timeout=120,
    )
    resp.raise_for_status()
    rows = resp.json()[1:]  # first element is a META/PARAMETERS block, not data

    records = []
    max_dateobs = last_dateobs
    for row in rows:
        dateobs = row["dateobs_center"]
        max_dateobs = max(max_dateobs, dateobs)
        records.append(
            RawObservation(
                archive_obs_id=row["md5sum"],
                archive_url=row["url"],
                instrument=INSTRUMENT,
                obs_date=Time(dateobs).to_datetime().date(),
                program_id=row.get("proposal"),
                ra=clean_float(row["ra_center"]),
                dec=clean_float(row["dec_center"]),
            )
        )

    return records, {"last_dateobs": max_dateobs}
