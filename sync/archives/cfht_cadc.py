"""CFHT via CADC — TAP (ivoa.ObsCore), no native Gaia column.

The TAP endpoint isn't the guessable /tap or /youcat/tap path — it's
resolved from the ivo://cadc.nrc.ca/argus registry identifier (confirmed via
astroquery.cadc, which does this resolution internally) to
https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus.

Real cliff found live, not just "byte/time-bounded" as previously noted:
10,000 rows in 6s, 20,000 in 11.4s (~linear), but 30,000 in 59.7s — a sharp
non-linear jump, not gradual degradation. Paginating well under that.

No Gaia column on ivoa.ObsCore — positional match, same shape as eso.py.
Deep link is the standard DataLink resolver URL (constructible directly from
obs_publisher_did, no per-record extra request needed) rather than the
resolved direct-file canfar.net URL — resolving that for every record during
sync isn't practical at this scale (831,455 CFHT spectrum rows), but the
DataLink URL itself is confirmed live to resolve to a real downloadable FITS
file one click further in.

No calibration-frame filter: caom2.Observation.target_type='object' would
exclude things like "Polarized Flat Q" calibration exposures, but the join
needed to reach it (ObsCore has no intent/target_type of its own) made an
already-slow query much slower — not worth it. Calibration rows just flow
through and get harmlessly skipped by the matcher (their positions won't
coincide with a tracked star), at the cost of some wasted row-processing.

Positional-match caveat, found live and not a matcher bug: a real target
(Stein 2051 A / Gl169.1A, a known visual binary) failed to match within 1"
even after correct proper-motion propagation — Gaia's single-star
astrometric solution can be biased for astrometric binaries, so its
proper motion doesn't always predict the star's true position. The matcher
correctly skipped rather than force a wrong match; this is a real Gaia data
quality limit the "easy match" design accepts and defers, not a bug.

s_ra/s_dec read via clean_float — can be masked on real rows (confirmed as a
real pattern via mast.py), and a bare float() would turn that into NaN and
crash the matcher's KD-tree build outright.
"""

from urllib.parse import quote

from astropy.time import Time

from sync.base import RawObservation, clean_float, make_tap_service

TAP_URL = "https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus"

QUERY = """
SELECT TOP {page_size} obs_publisher_did, s_ra, s_dec, t_min, instrument_name, target_name
FROM ivoa.ObsCore
WHERE obs_collection = 'CFHT' AND dataproduct_type = 'spectrum' AND t_min > {last_t_min}
ORDER BY t_min ASC
"""

# Kept well under the ~20k-30k cliff found live.
PAGE_SIZE = 15000

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
