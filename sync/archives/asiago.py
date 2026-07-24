"""Asiago Observatory (Italy) — TAP via IA2, id-watermark pagination.

Same IA2 (Italian VO center) infrastructure as harpsn_tng.py, found via the
same archive-gap survey — a real TAP service at archives.ia2.inaf.it/vo/tap,
not documented anywhere on the archive's own (currently under maintenance)
public portal; found by reverse-engineering the underlying app's own JS
bundle. Covers aao.ECH (the Asiago Echelle spectrograph) only — aao.AAO
(1.49M rows) is mostly Schmidt imaging and aao.AFO (1.06M rows, AFOSC) mixes
imaging and spectroscopy without a clean isolating column, both deliberately
excluded, same reasoning as koa.py's excluded imaging-only tables.

41,419 rows confirmed live, 1994-12-16 to present. Real target names in
OBJECT (e.g. "17 Lep", a real star) — some rows use "Manual Coords"/
descriptive placeholders instead (confirmed live: only 15,505 of 41,419 rows
have RA_RAD/DEC_RAD populated at all), so this relies on the matcher's
name_resolved path more than most TAP archives, with positional_easy_match
as a genuine fallback rather than the primary path DAO/CFHT/etc. get.

RA_RAD/DEC_RAD are literally radians (confirmed: values in the +/-pi/2pi*2
range, not degrees) — every other archive in this codebase reports degrees
directly, so these need an explicit conversion, unlike everywhere else.

id is a plain sequential integer with no cliff at 41k rows (confirmed live:
a full unpaginated pull took 1.4s) — paginated by id watermark anyway since
this is a live, growing archive, not a one-shot historical dump.

DATE_OBS carries a real, systematic malformation on a non-trivial number of
rows (confirmed live, e.g. "2008-03-20T21:43:010" — a bare trailing "0"
where every other row has a proper ".0" fractional-second decimal point,
looks like a formatting bug on the archive's own side, not a one-off typo)
— astropy's isot parser raises ValueError on these rather than silently
truncating, so parsing is wrapped and falls back to obs_date=None instead of
crashing the whole page.
"""

import math

from astropy.time import Time

from sync.base import RawObservation, clean_float, make_tap_service

TAP_URL = "http://archives.ia2.inaf.it/vo/tap/aao"

QUERY = """
SELECT TOP {page_size} id, OBJECT, DATE_OBS, RA_RAD, DEC_RAD, file_name, INSTRUMENT, PROGRAM
FROM aao.ECH
WHERE id > {last_id}
ORDER BY id ASC
"""

PAGE_SIZE = 20000

FILE_URL = "http://archives.ia2.inaf.it/files/aao/{file_name}"

RAD_TO_DEG = 180.0 / math.pi


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
        ra_rad = clean_float(row["RA_RAD"])
        dec_rad = clean_float(row["DEC_RAD"])
        try:
            obs_date = Time(str(row["DATE_OBS"]), format="isot").to_datetime().date()
        except ValueError:
            obs_date = None
        records.append(
            RawObservation(
                archive_obs_id=str(row_id),
                archive_url=FILE_URL.format(file_name=str(row["file_name"])),
                instrument=str(row["INSTRUMENT"]),
                obs_date=obs_date,
                program_id=str(row["PROGRAM"]),
                ra=ra_rad * RAD_TO_DEG if ra_rad is not None else None,
                dec=dec_rad * RAD_TO_DEG if dec_rad is not None else None,
                raw_target_name=str(row["OBJECT"]),
            )
        )

    new_cursor = {"last_id": max_id}
    return records, new_cursor
