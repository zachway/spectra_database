"""OIRSA (CfA, Optical/Infrared Science Archive) — TAP (ivoa.ObsCore).

Real DaCHS-backed TAP service at oirsa.cfa.harvard.edu:8080/tap — note the
:8080 port, not the public :443 site (the archive's own web frontend is a
stateful dojo/prototype.js search app with no scriptable API; its `/search/*`
AJAX endpoints 404 for any non-browser client regardless of headers/cookies,
confirmed live — this TAP service is a completely separate thing, found via
the reg.g-vo.org registry, not linked from that frontend at all).

Covers all four CfA instruments in one unfiltered pull across the whole
spectrum table rather than filtering per-instrument — obs_collection is only
populated for Echelle, as "CfA:Echelle" (FAST/Hectospec/Hectochelle all leave
it blank, confirmed live), so it's not usable as a collection discriminator;
instrument_name is read per-row instead, just to label each RawObservation.
Breakdown confirmed live: FAST (132,452 rows), Hectospec (599,592),
Hectochelle (393,267), Echelle (171,278); ~1.3M spectra total.

target_name is only a resolvable star name for FAST/Echelle (e.g.
"FAST:RWAur", "ECH:HR1454" — single-object spectrographs). Hectospec/
Hectochelle are multi-fiber instruments, so target_name there is a plate/
configuration id instead (e.g. "SPEC:a0689a2_1"), not a star name — but
s_ra/s_dec are still per-fiber, per-target positions, not the field center
(confirmed live: rows sharing one Hectospec target_name carry visibly
different s_ra/s_dec, and s_fov is ~1.5 arcsec — a fiber aperture, not an
MMT field of view) — so positional matching still works correctly per
target even where the name doesn't. raw_target_name is passed through as-is
regardless; unresolvable configuration ids are simply skipped by the
generic discover_stars step, same tolerance as any other unmatched name.

access_url is already a direct per-row file link (confirmed live: a real
200 with a real Content-Length) — no DataLink resolution step needed, unlike
dao.py/cfht_cadc.py/gemini.py's CADC-hosted equivalent.

No cliff found: TOP+ORDER BY t_min tested live up to 50,000 rows/page
(~9s) and even an unbounded TOP 2,000,000 pull of the whole spectrum table
(~12s) — standard TOP+watermark pagination, same shape as dao.py. No native
Gaia column — positional match, same as dao.py/cfht_cadc.py. s_ra/s_dec/
t_min confirmed live to have zero NULLs across the whole table, but read via
clean_float anyway for consistency with every other ObsCore-based module
here (masked values are a live-confirmed real pattern elsewhere, e.g.
mast.py) rather than assuming this holds forever.
"""

from astropy.time import Time

from sync.base import RawObservation, clean_float, make_tap_service

TAP_URL = "http://oirsa.cfa.harvard.edu:8080/tap"

QUERY = """
SELECT TOP {page_size} obs_publisher_did, s_ra, s_dec, t_min, instrument_name, target_name, access_url
FROM ivoa.obscore
WHERE dataproduct_type = 'spectrum' AND t_min > {last_t_min}
ORDER BY t_min ASC
"""

# Kept well under the cliff-free range confirmed live (50,000 rows in ~9s).
PAGE_SIZE = 50000


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
        records.append(
            RawObservation(
                archive_obs_id=str(row["obs_publisher_did"]),
                archive_url=str(row["access_url"]),
                instrument=str(row["instrument_name"]),
                obs_date=Time(t_min, format="mjd").to_datetime().date(),
                ra=clean_float(row["s_ra"]),
                dec=clean_float(row["s_dec"]),
                raw_target_name=str(row["target_name"]),
            )
        )

    new_cursor = {"last_t_min": max_t_min if records else last_t_min}
    return records, new_cursor
