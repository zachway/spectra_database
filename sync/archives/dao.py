"""DAO (Dominion Astrophysical Observatory, Canada) via CADC — TAP (ivoa.ObsCore).

Same CADC TAP endpoint as cfht_cadc.py/gemini.py
(https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus), just obs_collection='DAO'
— found while surveying for archive gaps, not a new access pattern. Confirmed
live: 263,980 spectrum rows, real s_ra/s_dec/target_name, t_min from MJD
46450 (1986-01-14, Cassegrain + coude spectrographs) to present.

Cliff shape matches CFHT, not Gemini: ORDER BY t_min stays fast well past
Gemini's ~1000-row wall (confirmed live: 10,000 rows in 2.9s, 20,000 in
16.9s — the same kind of sharp-but-later cliff as CFHT's). Standard
TOP+ORDER BY+watermark pagination works, paginated well under that.

No native Gaia column — positional match, same shape as cfht_cadc.py/eso.py.
Deep link is the standard CADC DataLink resolver URL built from
obs_publisher_did (confirmed live to return a real DataLink VOTable), same
as cfht_cadc.py/gemini.py — no separate resolution step needed.

s_ra/s_dec read via clean_float — can be masked on real rows (confirmed as
a real pattern via mast.py), and a bare float() would turn that into NaN
and crash the matcher's KD-tree build outright.
"""

from urllib.parse import quote

from astropy.time import Time

from sync.base import RawObservation, clean_float, make_tap_service

TAP_URL = "https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus"

QUERY = """
SELECT TOP {page_size} obs_publisher_did, s_ra, s_dec, t_min, instrument_name, target_name
FROM ivoa.ObsCore
WHERE obs_collection = 'DAO' AND dataproduct_type = 'spectrum' AND t_min > {last_t_min}
ORDER BY t_min ASC
"""

# Kept well under the cliff found live (20,000 rows already up to 16.9s).
PAGE_SIZE = 10000

DATALINK_URL = "https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/caom2ops/datalink?ID={did}"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_t_min = cursor.get("last_t_min", 0)

    tap = make_tap_service(TAP_URL)
    query = QUERY.format(page_size=PAGE_SIZE, last_t_min=last_t_min)
    table = tap.search(query, maxrec=PAGE_SIZE).to_table()

    records = []
    max_t_min = last_t_min
    for row in table:
        t_min = float(row["t_min"])
        max_t_min = max(max_t_min, t_min)
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

    new_cursor = {"last_t_min": max_t_min if records else last_t_min}
    return records, new_cursor
