"""One-off: upgrade harpsn_tng's stored archive_url from the raw exposure
frame to the best available DRS-reduced 1D spectrum, where one exists.

sync/archives/harpsn_tng.py deliberately anchors one spectroscopy_holdings
row per physical exposure on the bare raw frame (see that module's
docstring) -- every DRS pipeline product for an exposure shares one exposure
timestamp but gets its own `id` in tng.TNG_TAP, and the raw frame is the only
one of these guaranteed to exist for every exposure back to 2012-07-17. That
fix is about *counting* (one row per exposure, not up to 18), not about
*which file* the row points at -- rows still end up with archive_url pointing
at a raw, non-science-ready FITS frame, unlike every other 'reduced'-tagged
archive in this project (bess, cfht_cadc, eso, gemini, mast, naoj, ...).

Why this can't just be folded into harpsn_tng.py's fetch(): confirmed live
(2026-08-04) that a reduced product's `id` is NOT reliably close to its raw
sibling's `id`. Old-pipeline products (lowercase e2ds/s1d/ccf/bis) do land
near the raw frame's id (generated near-real-time alongside ingestion), but
the new-pipeline reprocessing (uppercase S1D/S2D/CCF, "r.HARPN." prefix) was
inserted as a separate, later bulk campaign -- sampling id blocks near the
current watermark (id ~4.39M) found ~448 reprocessed exposures' worth of
r.HARPN products but only 15 raw siblings in that same 12,574-id window, i.e.
most raw counterparts for even *recent* reprocessed exposures sit far away in
id-space. fetch()'s single forward pass over an id-ordered cursor can't
correctly prefer a reduced product without either (a) unbounded lookahead
before emitting any row, which breaks incremental pagination for every
archive that shares this id-watermark shape (asiago.py too), or (b) silently
missing upgrades that land outside the current page -- neither acceptable.
Doing it here instead, as a full separate pass with the entire id-space
available before any decision is made, sidesteps the ordering problem
entirely.

Priority per exposure (best first) -- picks fiber A, since that's the
science-target fiber in HARPS-N's normal setup (fiber B carries sky or the
simultaneous ThAr/FP reference):
  1. r.HARPN...S1D_FLUXCAL_A.fits.gz   (new pipeline, flux+wavelength cal'd)
  2. r.HARPN...S1D_A.fits.gz           (new pipeline, merged 1D spectrum)
  3. r.HARPN...S1D_SKYSUB_FLUXCAL_A.fits.gz  (new pipeline, sky-sub mode)
  4. r.HARPN...S1D_SKYSUB_A.fits.gz          (new pipeline, sky-sub mode)
  5. HARPN...s1d_A.fits.gz             (old pipeline, merged 1D spectrum)
  6. (nothing found -> leave the raw frame, reduction_status='raw')
S2D (unmerged per-order, not a single spectrum) and CCF (a cross-correlation
function, not a spectrum) are deliberately excluded from every tier -- they
exist for nearly every exposure but aren't "the spectrum" a user asking for
HARPS-N data would expect.

Two-pass design, same shape as scripts/backfill_reduction_status.py's
"expensive" archives:
  1. Scan the full INSTRUMENT='HARPN' AND policy='FREE' table once, id-
     watermarked same as harpsn_tng.py itself, building an in-memory dict of
     exposure-timestamp -> best (priority, url) found so far. No OBS_MODE/
     OBJECT filter here (unlike the live sync's own query) -- cheaper to
     just index everything than to risk missing a product whose own row
     happens to carry a stale/blank OBJECT value.
  2. Re-scan spectroscopy_holdings rows for archive_code='harpsn_tng' with
     reduction_status='unknown' (matches the idiom scripts/
     backfill_reduction_status.py already established -- new rows a normal
     sync inserts always start 'unknown', so re-running this script later
     safely picks up newly-synced or newly-reprocessed exposures without
     re-touching rows it already upgraded), extract each row's own exposure
     timestamp from its *current* (raw) archive_url, look it up in the
     dict, and UPDATE archive_url + reduction_status accordingly.

Expected to need periodic re-runs, same as the other 'expensive' backfills in
backfill_reduction_status.py -- IA2's reprocessing lags real-time ingestion,
so a freshly-synced exposure often won't have its S1D counterpart yet.

IA2's TAP service showed real instability today (2026-08-04): a plain
INSTRUMENT-bound query intermittently returned "Time out! ... limited to 0
seconds" with no load from this project active at the time, separate from
the unrelated page-40 failure harpsn_tng.py's own sync hit earlier in the
day. Retries with backoff below, same pattern backfill_reduction_status.py
uses for CADC's own intermittent stalls -- don't run this back-to-back with
a fresh harpsn_tng sync; give IA2 a quiet window.

Usage:
    DATABASE_URL=postgresql:///spectra_local python3 -m scripts.backfill_harpsn_reduction
"""

from __future__ import annotations

import logging
import os
import re
import time

import psycopg

from sync.archives.harpsn_tng import TAP_URL
from sync.base import make_tap_service

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PAGE_SIZE = 20000

INDEX_QUERY = """
SELECT TOP {page_size} id, file_url
FROM tng.TNG_TAP
WHERE id > {last_id} AND INSTRUMENT = 'HARPN' AND policy = 'FREE'
ORDER BY id ASC
"""

TAP_SEARCH_RETRIES = 3
TAP_SEARCH_BACKOFF_SECONDS = 30

TIMESTAMP_RE = re.compile(r"(?:^|/)(?:r\.|A-)?HARPN\.([\dT.\-]+?)(?:_[A-Za-z0-9_]+)?\.fits(?:\.gz)?$")

# Best first -- matched against the basename in order, first match wins.
PRIORITY_PATTERNS = [
    re.compile(r"^r\.HARPN\.[\dT.\-]+_S1D_FLUXCAL_A\.fits\.gz$"),
    re.compile(r"^r\.HARPN\.[\dT.\-]+_S1D_A\.fits\.gz$"),
    re.compile(r"^r\.HARPN\.[\dT.\-]+_S1D_SKYSUB_FLUXCAL_A\.fits\.gz$"),
    re.compile(r"^r\.HARPN\.[\dT.\-]+_S1D_SKYSUB_A\.fits\.gz$"),
    re.compile(r"^HARPN\.[\dT.\-]+_s1d_A\.fits\.gz$"),
]


def _tap_search(tap, query: str, maxrec: int):
    for attempt in range(1, TAP_SEARCH_RETRIES + 1):
        try:
            return tap.search(query, maxrec=maxrec).to_table()
        except Exception:
            if attempt == TAP_SEARCH_RETRIES:
                raise
            logger.warning(
                "TAP query failed (attempt %d/%d), retrying in %ds", attempt, TAP_SEARCH_RETRIES, TAP_SEARCH_BACKOFF_SECONDS
            )
            time.sleep(TAP_SEARCH_BACKOFF_SECONDS)


def _extract_timestamp(file_url: str) -> str | None:
    name = file_url.rsplit("/", 1)[-1]
    m = TIMESTAMP_RE.search(name)
    return m.group(1) if m else None


def _priority(file_url: str) -> int | None:
    name = file_url.rsplit("/", 1)[-1]
    for rank, pattern in enumerate(PRIORITY_PATTERNS):
        if pattern.match(name):
            return rank
    return None


def build_product_index(tap) -> dict[str, str]:
    """Full scan of tng.TNG_TAP -> {exposure_timestamp: best_reduced_url}."""
    best: dict[str, tuple[int, str]] = {}
    last_id = 0
    pages = 0
    while True:
        table = _tap_search(tap, INDEX_QUERY.format(page_size=PAGE_SIZE, last_id=last_id), PAGE_SIZE)
        if len(table) == 0:
            break
        max_id = last_id
        for row in table:
            max_id = max(max_id, int(row["id"]))
            file_url = str(row["file_url"])
            rank = _priority(file_url)
            if rank is None:
                continue
            ts = _extract_timestamp(file_url)
            if ts is None:
                continue
            current = best.get(ts)
            if current is None or rank < current[0]:
                best[ts] = (rank, file_url)
        pages += 1
        logger.info("index: page %d -> %d rows scanned, %d exposures with a reduced product so far", pages, len(table), len(best))
        if len(table) < PAGE_SIZE:
            break
        last_id = max_id
    return {ts: url for ts, (_, url) in best.items()}


def upgrade_rows(conn: psycopg.Connection, index: dict[str, str]) -> None:
    batch_size = 5000
    total_reduced = 0
    total_raw = 0
    with conn.cursor(name="harpsn_unknown_rows") as read_cur:
        read_cur.execute(
            "SELECT archive_obs_id, archive_url FROM spectroscopy_holdings "
            "WHERE archive_code = 'harpsn_tng' AND reduction_status = 'unknown'"
        )
        while True:
            rows = read_cur.fetchmany(batch_size)
            if not rows:
                break
            obs_ids, urls, statuses = [], [], []
            for archive_obs_id, archive_url in rows:
                ts = _extract_timestamp(archive_url)
                best_url = index.get(ts) if ts else None
                obs_ids.append(archive_obs_id)
                if best_url is not None:
                    urls.append(best_url)
                    statuses.append("reduced")
                    total_reduced += 1
                else:
                    urls.append(archive_url)
                    statuses.append("raw")
                    total_raw += 1
            with conn.cursor() as write_cur:
                write_cur.execute(
                    """
                    UPDATE spectroscopy_holdings h
                    SET archive_url = v.archive_url, reduction_status = v.reduction_status, updated_at = now()
                    FROM (
                        SELECT * FROM unnest(%(obs_ids)s::text[], %(urls)s::text[], %(statuses)s::text[])
                            AS t(archive_obs_id, archive_url, reduction_status)
                    ) v
                    WHERE h.archive_code = 'harpsn_tng'
                      AND h.archive_obs_id = v.archive_obs_id
                      AND h.reduction_status = 'unknown'
                    """,
                    {"obs_ids": obs_ids, "urls": urls, "statuses": statuses},
                )
            conn.commit()
            logger.info("upgrade: %d rows processed so far (%d -> reduced, %d -> raw)", total_reduced + total_raw, total_reduced, total_raw)
    logger.info("done: %d rows upgraded to reduced, %d rows confirmed raw-only", total_reduced, total_raw)


def main() -> None:
    tap = make_tap_service(TAP_URL)
    logger.info("building exposure -> best-reduced-product index (full table scan)...")
    index = build_product_index(tap)
    logger.info("index built: %d exposures have a preferred reduced product", len(index))

    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        upgrade_rows(conn, index)


if __name__ == "__main__":
    main()
