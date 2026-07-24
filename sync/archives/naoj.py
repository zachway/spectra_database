"""NAOJ / Subaru HDS, via JVO (Japanese Virtual Observatory) — TAP.

Not the SMOKA archive (the prior investigation's dead end still holds for
SMOKA specifically: registration-gated web wizard, no bulk API). This is a
completely separate TAP+SSA service run by JVO (jvo.nao.ac.jp) for Subaru's
High Dispersion Spectrograph, found via the reg.g-vo.org registry sweep — see
project memory for how. Checked the registry for other Subaru instruments
(FOCAS, IRCS, Suprime-Cam, MOIRCS): only HDS has a registered spectroscopy
TAP/SSA capability, the rest are imaging-only (SIA).

This is a custom JVOQL engine, not DaCHS/PostgreSQL like every other TAP
archive in this project — several real quirks follow from that:

- `SELECT *` is unusable: it declares `access_estsize` as VOTable datatype
  "int" but actually emits decimal strings like "849.9200000000000000" for
  some rows, which astropy's VOTable parser rejects outright (confirmed
  live, DALFormatError). Fix is simply to not select that column — every
  column actually needed here (position, time, name, URL) parses fine.
- No `instrument_name` column at all — this table is HDS-only by
  construction, so instrument is hardcoded below rather than read.
- `obs_publisher_did` is not a per-row identifier here (confirmed live: the
  same literal 'ivo://jvo/subaru/hds/spec' on every row) — the real per-
  observation key is `raw_id` (e.g. "HDSA00027401").
- `COUNT(DISTINCT raw_id)` silently ignores DISTINCT and returns the same
  value as COUNT(*) (confirmed live) — this engine doesn't seem to support
  it at all, so don't rely on DISTINCT anywhere in this module.
- Server-side row cap of 200,000 regardless of TOP/maxrec requested
  (confirmed live via the RECORD_MAX info element) — irrelevant at this
  module's PAGE_SIZE, just don't assume a single unpaginated pull can ever
  see the whole table (253,389 rows total).

Row multiplicity: most raw_ids appear multiple times, not as duplicates but
as different pipeline products of the same single exposure — confirmed
live, one raw_id had 6 rows sharing an identical target/time/position: three
`application/fits` variants (raw/wavelength-corrected/1D-extracted,
distinguished only by filename infix) plus matching `text/plain` dumps of
each. `_product_rank` below picks the single best row per raw_id per page:
the fully-processed 1D fits product first, less-processed fits next, then
any other fits, then a packaged tar (older pre-pipeline observations, ~1,600
raw_ids have no fits row at all and only ship as one .tar), then text/plain
last. Dedup is per-page (grouped by raw_id within whatever one `fetch` call
pulls back) rather than global — a raw_id whose rows happen to straddle a
page boundary could in principle pick a slightly worse product than the
true best, but ties in `t_mid` (the pagination watermark) are tight (at most
~6 rows) relative to PAGE_SIZE, so this is a rare cosmetic edge case, not a
correctness one.

date_obs is already a plain ISO date string — parsed directly rather than
converting t_mid (MJD) through astropy, which would just reproduce the same
day with extra steps. obs_title packs the target name and wavelength range
together as "NAME [lo:hi]" (confirmed live, format-consistent across a
200,000-row sample, e.g. "Arcturus [657.49:778.01]"; a handful of BIAS
calibration frames slip through under the same shape too, resolved to "BIAS"
as raw_target_name — same as SIMBAD-unresolvable names elsewhere in this
project, naturally skipped by the generic discover_stars step) — the name is
everything before " [".

No native Gaia column, no cliff found in ORDER BY t_mid pagination up to
50,000 rows/page (1.3-4.7s) — standard TOP+watermark, same shape as
dao.py/mast.py.
"""

from datetime import date

from sync.base import RawObservation, clean_float, make_tap_service

TAP_URL = "http://jvo.nao.ac.jp/skynode/do/tap/hds/sync"

QUERY = """
SELECT TOP {page_size} raw_id, t_mid, s_ra, s_dec, date_obs, obs_title, access_url, access_format
FROM public.spec
WHERE t_mid > {last_t_min}
ORDER BY t_mid ASC
"""

PAGE_SIZE = 20000

INSTRUMENT = "HDS"

# Preference order for the one row worth keeping per raw_id -- see module
# docstring. Checked longest/most-specific infix first since "1d_nrmwec_
# fsclmo" also contains "nrmwec_fsclmo" as a substring.
_FITS_INFIX_PRIORITY = ["1d_nrmwec_fsclmo", "nrmwec_fsclmo", "rmwec_fsclmo"]


def _product_rank(access_url: str, access_format: str) -> int:
    if access_format == "application/fits":
        for rank, infix in enumerate(_FITS_INFIX_PRIORITY):
            if infix in access_url:
                return rank
        return len(_FITS_INFIX_PRIORITY)
    if access_format == "application/x-tar":
        return len(_FITS_INFIX_PRIORITY) + 1
    return len(_FITS_INFIX_PRIORITY) + 2  # text/plain, or anything else


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_t_min = cursor.get("last_t_min", 0)

    tap = make_tap_service(TAP_URL)
    query = QUERY.format(page_size=PAGE_SIZE, last_t_min=last_t_min)
    table = tap.search(query, maxrec=PAGE_SIZE).to_table()

    max_t_min = last_t_min
    by_raw_id: dict[str, dict] = {}
    for row in table:
        t_mid = float(row["t_mid"])
        max_t_min = max(max_t_min, t_mid)

        raw_id = str(row["raw_id"])
        access_url = str(row["access_url"])
        access_format = str(row["access_format"])
        existing = by_raw_id.get(raw_id)
        if existing is None or _product_rank(access_url, access_format) < _product_rank(
            existing["access_url"], existing["access_format"]
        ):
            obs_title = str(row["obs_title"])
            target_name = obs_title.split(" [", 1)[0]
            by_raw_id[raw_id] = {
                "t_mid": t_mid,
                "access_url": access_url,
                "access_format": access_format,
                "s_ra": row["s_ra"],
                "s_dec": row["s_dec"],
                "date_obs": str(row["date_obs"]),
                "target_name": target_name,
            }

    records = [
        RawObservation(
            archive_obs_id=raw_id,
            archive_url=data["access_url"],
            instrument=INSTRUMENT,
            obs_date=date.fromisoformat(data["date_obs"]),
            ra=clean_float(data["s_ra"]),
            dec=clean_float(data["s_dec"]),
            raw_target_name=data["target_name"],
        )
        for raw_id, data in by_raw_id.items()
    ]

    new_cursor = {"last_t_min": max_t_min}
    return records, new_cursor
