"""Subaru/MOIRCS, via JVO (Japanese Virtual Observatory) — TAP.

A second Subaru instrument on the same JVO engine as naoj.py's HDS
endpoint, but a real one this project got wrong the first time: naoj.py's
own docstring wrote MOIRCS off as "imaging-only" based only on checking the
IVOA registry for a registered SSA/SIA capability. That check was too
narrow — MOIRCS has a real, live TAP endpoint
(jvo.nao.ac.jp/skynode/do/tap/moircs, table public.raw) that just isn't
registry-advertised as a spectroscopy service, and it carries real
multi-object and long-slit spectra alongside its (much larger) imaging
holdings.

`public.raw`'s `obs_mode` column (confirmed live via GROUP BY) has six
distinct values: IMAG (89,858 rows, imaging, excluded), LAUNCHER (305),
SPEC (7,515, real long-slit spectra), SPEC_MOS (29,089, real multi-object
spectra), SUBROUTINE (122), TOOL (10,971). Filtered to
`obs_mode IN ('SPEC', 'SPEC_MOS')` -- 36,604 rows confirmed live, exactly
matching a live COUNT(*) with that filter.

Same custom JVOQL engine as naoj.py, and most of its quirks carry over
unchanged, confirmed independently against this table rather than assumed:

- `COUNT(DISTINCT ...)` is silently ignored here too (confirmed live:
  `COUNT(DISTINCT object)` returns 36,604 -- the same as COUNT(*) -- even
  though a GROUP BY on the same column shows real duplicates, e.g. mask
  name "TK0316" appears on 12 rows).
- 200,000-row RECORD_MAX server-side cap, confirmed live via the same INFO
  element naoj.py documents -- moot at this table's scale.
- `FORMAT=csv` is ignored, confirmed live -- a raw HTTP request with
  `FORMAT=csv` still comes back `Content-Type: text/xml` VOTable.
- `SELECT *` is unusable here too, but for a *different* reason than HDS's
  malformed `access_estsize` column: it fails server-side outright with
  `ERROR: column t0.center_ra does not exist` (confirmed live,
  DALQueryError) -- this table has no `center_ra`/`center_dec` at all, but
  the engine's `SELECT *` expansion tries to inject them anyway. Selecting
  real column names explicitly avoids it, same fix as naoj.py, different
  bug.
- No `instrument_name` column here either -- hardcoded below, same as
  naoj.py does for HDS.

Where this table differs from naoj.py's HDS table: no per-exposure
pipeline-product multiplicity to dedup. Every row here is one genuinely
distinct FITS frame from one of MOIRCS's two physically-separate HAWAII-2
detector chips (`det_id` 1/2, confirmed live: same `exp_id` shared by
exactly two rows, one per chip, each with its own `data_id`, own
`ref_val1`/`ref_val2` sky position, and own `access_ref`) -- not
duplicates of the same product. A live GROUP BY data_id HAVING COUNT(*) > 1
over the whole SPEC/SPEC_MOS set returned zero rows, confirming `data_id`
is already a clean one-row-per-record key with no dedup needed.

Position: `ref_val1`/`ref_val2` are plain RA/Dec in degrees, not a
CRVAL-style WCS reference needing conversion -- confirmed live against the
paired `frame`/`equinox`/`projection` columns, which read 'FK5'/2000.0/
'TAN' on every sampled row (standard J2000 equatorial, close enough to
ICRS for this project's purposes, same as every other archive here that
reports FK5/J2000). Confirmed live: 0 of 36,604 rows have a masked/null
`ref_val1`.

Target name: `object` is the right column (confirmed live against real
recent data, e.g. "RUBIN149D", "TK0316", "M10185" -- MOS mask/target
names). A small number of early-commissioning 2005 rows (24 of 36,604,
confirmed live) carry a garbage value starting with a literal `"` (e.g.
`"Command`, `"Mark`) -- an upstream truncated command-log string, not a
real target name. Left as-is rather than filtered out: same as naoj.py's
BIAS frames, these simply fail to resolve against SIMBAD downstream and
are silently skipped by discover_stars, no special-casing needed here.

`date` is already a plain ISO date string (confirmed live: 0 unparseable
across all 36,604 rows) -- parsed directly, same as naoj.py's date_obs.

Paginated via an `id` integer watermark (confirmed live: unique and
strictly increasing across the whole SPEC/SPEC_MOS set) rather than
`data_id` (a string) or `date` (too coarse, many rows share a day) -- same
watermark shape as naoj.py's `t_mid`, just an int instead of a float. No
cliff found: the whole 36,604-row set came back in one ~6.4s query.

`access_ref` (an http `requestData.do` link) is used for archive_url
rather than `ftp_access_ref` -- a real working http download-request URL,
same preference every other archive in this project gives http over ftp
when both are available.
"""

from __future__ import annotations

from datetime import date

from sync.base import RawObservation, clean_float, make_tap_service

TAP_URL = "http://jvo.nao.ac.jp/skynode/do/tap/moircs"

QUERY = """
SELECT TOP {page_size} id, data_id, date, object, ref_val1, ref_val2, access_ref
FROM public.raw
WHERE obs_mode IN ('SPEC', 'SPEC_MOS') AND id > {last_id}
ORDER BY id ASC
"""

PAGE_SIZE = 20000

INSTRUMENT = "MOIRCS"


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
        object_name = str(row["object"]).strip()
        records.append(
            RawObservation(
                archive_obs_id=str(row["data_id"]),
                archive_url=str(row["access_ref"]),
                instrument=INSTRUMENT,
                obs_date=date.fromisoformat(str(row["date"])),
                ra=clean_float(row["ref_val1"]),
                dec=clean_float(row["ref_val2"]),
                raw_target_name=object_name or None,
            )
        )

    new_cursor = {"last_id": max_id}
    return records, new_cursor
