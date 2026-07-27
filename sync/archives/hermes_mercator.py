"""HERMES @ Mercator Telescope (KU Leuven, La Palma) -- TAP.

Found from the user-supplied web form at
mercatorvo.ster.kuleuven.be/hermes/q/web/form -- same GAVO DaCHS/SSA
software as feros_gavo.py/flashheros_gavo.py (identical ssa_* field
naming), and like every DaCHS service it also exposes a real TAP endpoint
(mercatorvo.ster.kuleuven.be/tap, table hermes.data) rather than needing
to scrape the HTML form itself.

Unlike feros_gavo/flashheros_gavo (both frozen historical datasets with
no position at all), hermes.data has a populated ssa_location on every
single row (confirmed live: 0 of 119,650 rows null) and a real
ssa_targname on all but 23 -- normal identifier-then-position matching
applies here, not name-only. mime is always 'application/fits' (confirmed
live, no paired application/x-votable+xml rows the way feros/flashheros
have) so no mime filter is needed.

ssa_instrument reports the literal per-row string "HERMES ()" (confirmed
live, on every row) -- a real upstream formatting artifact, not a parsing
bug; hardcoded to "HERMES" here instead of read verbatim.

The embargo column is always an empty string on every row (confirmed
live) -- not usable as an embargo signal, unlike harpsn_tng.py's `policy`
field; no embargo filtering is applied at all (this project doesn't
download bytes anyway, same reasoning most other archives here give for
including proprietary-period rows).

Only 119,650 rows total (confirmed live) -- small enough that no cliff
was found paginating by unique_seqno at PAGE_SIZE, but paginated anyway
via an id watermark (same shape as harpsn_tng.py/asiago.py) rather than a
one-shot pull, since the archive is still actively growing (a live TAP
COUNT(*) went up between two queries run minutes apart during
investigation, and the service's own metadata reports "Data updated"
within the last few months).
"""

from astropy.time import Time

from sync.base import RawObservation, clean_float, make_tap_service

TAP_URL = "https://mercatorvo.ster.kuleuven.be/tap"

QUERY = """
SELECT TOP {page_size} accref, ssa_targname, ssa_dateobs, ssa_location, unique_seqno
FROM hermes.data
WHERE unique_seqno > {last_id}
ORDER BY unique_seqno ASC
"""

PAGE_SIZE = 20000

INSTRUMENT = "HERMES"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_id = cursor.get("last_id", 0)

    tap = make_tap_service(TAP_URL)
    query = QUERY.format(page_size=PAGE_SIZE, last_id=last_id)
    table = tap.search(query, maxrec=PAGE_SIZE).to_table()

    records = []
    max_id = last_id
    for row in table:
        seqno = int(row["unique_seqno"])
        max_id = max(max_id, seqno)
        location = row["ssa_location"]
        ra = clean_float(location[0])
        dec = clean_float(location[1])
        dateobs = clean_float(row["ssa_dateObs"])
        obs_date = Time(dateobs, format="mjd").to_datetime().date() if dateobs is not None else None
        records.append(
            RawObservation(
                archive_obs_id=str(seqno),
                archive_url=str(row["accref"]),
                instrument=INSTRUMENT,
                obs_date=obs_date,
                ra=ra,
                dec=dec,
                raw_target_name=str(row["ssa_targname"]) or None,
            )
        )

    new_cursor = {"last_id": max_id}
    return records, new_cursor
