"""One-off: backfill spectroscopy_holdings.reduction_status for rows synced
before PR #46/#49 added the column (everything currently in production, as
of 2026-08-03 -- confirmed live: 100% of 40M+ rows read 'unknown').

sync.matcher's ON CONFLICT upsert only sets a real value when a record is
re-ingested through a fresh sync run, and sync cursors are incremental
watermarks that don't touch already-synced rows (see the migration's own
comment) -- so without this script, existing rows would only slowly get a
real value as each archive happens to sync new observations, which for a
mostly-static historical archive like ESO/CFHT could be a very long wait.

Two very different costs here, kept in the same file since both feed the
same reduction_status column:

- "Cheap" archives (koa, gemini_ghost, gemini_igrins, naoj): the value
  doesn't depend on re-querying the archive at all -- it's either a
  hardcoded constant (koa.py's own docstring already establishes every row
  synced there is raw; gemini_ghost.py/gemini_igrins.py already filter to
  reduced-only filenames before a record is built) or, for naoj, derivable
  straight from the archive_url this project already stored (the same
  filename-infix/extension check naoj.py's _reduction_status does, applied
  to the stored URL instead of a fresh TAP row). Pure in-database UPDATEs,
  no network calls, seconds to run.

- "Expensive" archives (mast, mast_jwst, eso, cfht_cadc, dao, gemini,
  oirsa): reduction_status here comes from calib_level, an IVOA ObsCore
  column this project never stored on spectroscopy_holdings (only
  archive_url/instrument/obs_date/etc. -- see db/schema.sql). The only way
  to get it for already-synced rows is a fresh TAP query against each
  archive, re-using the exact same pagination/cliff-avoidance shape each
  sync/archives/<name>.py module already worked out live (gemini's 7-day
  windows to dodge its ORDER BY cliff, mast_jwst's bounded MJD windows,
  cfht_cadc/dao's page-size caps under their cliffs, ...) -- millions of
  rows, real load against those archives' own TAP services, expected to
  take up to roughly an hour total (gemini's fixed-window walk over ~25
  years dominates that estimate).

  mast/mast_jwst additionally match on (obs_id, archive_url) together, not
  obs_id alone: a single obs_id there can have several rows (raw/
  calibrated/housekeeping/... for mast, or a dozen-plus processing-stage
  variants for JWST), and sync/archives/mast.py|mast_jwst.py already pick
  one specific row's access_url per obs_id at sync time (stored verbatim as
  archive_url) -- matching on obs_id alone here could silently pick a
  different variant's calib_level than the one actually stored. cfht_cadc/
  dao/eso/gemini/oirsa don't have this problem (confirmed by their sync
  modules doing no per-ID dedup at all -- one ObsCore row per id there).

Idempotent/safe to re-run: every UPDATE is guarded by
`reduction_status = 'unknown'`, so a partial run (interrupted, or a TAP
outage mid-archive) just re-scans that archive's full table again next
time -- no cursor/resume state needed for a one-off like this, and it will
never clobber a real value a normal sync run set in the meantime.

Usage:
    DATABASE_URL=postgresql:///spectra_local python3 -m scripts.backfill_reduction_status [archive_code ...]

With no arguments, runs every archive listed in ARCHIVE_BACKFILLS in order
(cheap ones first). Pass one or more archive_codes to run only those.
"""

from __future__ import annotations

import logging
import os
import sys
import time

import psycopg
from astropy.time import Time

from sync.base import make_tap_service, reduction_status_from_calib_level

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CADC_TAP_URL = "https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus"
MAST_TAP_URL = "https://mast.stsci.edu/vo-tap/api/v0.1/caom"
ESO_TAP_URL = "http://archive.eso.org/tap_obs"
OIRSA_TAP_URL = "http://oirsa.cfa.harvard.edu:8080/tap"

# CADC's TAP service was confirmed live (2026-08-03) to intermittently stall
# past make_tap_service's 180s read timeout under load -- without a retry, a
# single bad page kills the whole archive's progress (gemini's ~1300-window
# walk in particular would have to restart from window 0). 3 attempts with a
# fixed 30s backoff between them; a failure on the 3rd attempt still
# propagates up to main()'s per-archive try/except, which moves on to the
# next archive rather than crashing the whole script.
TAP_SEARCH_RETRIES = 3
TAP_SEARCH_BACKOFF_SECONDS = 30


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


def _update_by_obs_id(conn: psycopg.Connection, archive_code: str, obs_ids: list[str], statuses: list[str]) -> int:
    if not obs_ids:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE spectroscopy_holdings h
            SET reduction_status = v.reduction_status, updated_at = now()
            FROM (SELECT * FROM unnest(%(obs_ids)s::text[], %(statuses)s::text[]) AS t(archive_obs_id, reduction_status)) v
            WHERE h.archive_code = %(archive_code)s
              AND h.archive_obs_id = v.archive_obs_id
              AND h.reduction_status = 'unknown'
            """,
            {"obs_ids": obs_ids, "statuses": statuses, "archive_code": archive_code},
        )
        n = cur.rowcount
    conn.commit()
    return n


def _update_by_obs_id_and_url(
    conn: psycopg.Connection, archive_code: str, obs_ids: list[str], urls: list[str], statuses: list[str]
) -> int:
    if not obs_ids:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE spectroscopy_holdings h
            SET reduction_status = v.reduction_status, updated_at = now()
            FROM (
                SELECT * FROM unnest(%(obs_ids)s::text[], %(urls)s::text[], %(statuses)s::text[])
                    AS t(archive_obs_id, archive_url, reduction_status)
            ) v
            WHERE h.archive_code = %(archive_code)s
              AND h.archive_obs_id = v.archive_obs_id
              AND h.archive_url = v.archive_url
              AND h.reduction_status = 'unknown'
            """,
            {"obs_ids": obs_ids, "urls": urls, "statuses": statuses, "archive_code": archive_code},
        )
        n = cur.rowcount
    conn.commit()
    return n


# =============================================================================
# Cheap: no external queries at all.
# =============================================================================


def backfill_koa(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE spectroscopy_holdings SET reduction_status = 'raw', updated_at = now() "
            "WHERE archive_code = 'koa' AND reduction_status = 'unknown'"
        )
        n = cur.rowcount
    conn.commit()
    logger.info("koa: %d rows set to 'raw'", n)


def backfill_gemini_goa(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE spectroscopy_holdings SET reduction_status = 'reduced', updated_at = now() "
            "WHERE archive_code IN ('gemini_ghost', 'gemini_igrins') AND reduction_status = 'unknown'"
        )
        n = cur.rowcount
    conn.commit()
    logger.info("gemini_ghost/gemini_igrins: %d rows set to 'reduced'", n)


# Same infix list naoj.py's _FITS_INFIX_PRIORITY uses -- any match means at
# least wavelength-correction was applied (see that module's _reduction_status).
_NAOJ_FITS_INFIXES = ["1d_nrmwec_fsclmo", "nrmwec_fsclmo", "rmwec_fsclmo"]


def backfill_naoj(conn: psycopg.Connection) -> None:
    """Derives reduction_status from the archive_url already stored for each
    row -- naoj.py's own _reduction_status applied to a FITS/tar/other
    extension and infix check, no TAP query needed."""
    infix_clause = " OR ".join("archive_url LIKE '%%" + infix + "%%'" for infix in _NAOJ_FITS_INFIXES)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE spectroscopy_holdings
            SET reduction_status = 'reduced', updated_at = now()
            WHERE archive_code = 'naoj' AND reduction_status = 'unknown'
              AND archive_url LIKE '%%.fits' AND ({infix_clause})
            """
        )
        n_reduced = cur.rowcount
        cur.execute(
            """
            UPDATE spectroscopy_holdings
            SET reduction_status = 'raw', updated_at = now()
            WHERE archive_code = 'naoj' AND reduction_status = 'unknown'
              AND (archive_url LIKE '%.tar' OR archive_url LIKE '%.fits')
            """
        )
        n_raw = cur.rowcount
    conn.commit()
    logger.info("naoj: %d rows set to 'reduced', %d rows set to 'raw' (rest left 'unknown')", n_reduced, n_raw)


# =============================================================================
# Expensive: re-query each archive's TAP service for calib_level.
# =============================================================================


def backfill_cfht_cadc(conn: psycopg.Connection) -> None:
    _backfill_cadc_style(conn, "cfht_cadc", "CFHT", page_size=15000)


def backfill_dao(conn: psycopg.Connection) -> None:
    _backfill_cadc_style(conn, "dao", "DAO", page_size=10000)


def _backfill_cadc_style(conn: psycopg.Connection, archive_code: str, obs_collection: str, page_size: int) -> None:
    """cfht_cadc.py/dao.py shape: plain t_min watermark, one ObsCore row per
    obs_publisher_did (no per-ID dedup needed, confirmed by those modules'
    own docstrings), paginated well under each archive's known cliff."""
    tap = make_tap_service(CADC_TAP_URL)
    query = """
    SELECT TOP {page_size} obs_publisher_did, calib_level, t_min
    FROM ivoa.ObsCore
    WHERE obs_collection = '{obs_collection}' AND dataproduct_type = 'spectrum' AND t_min > {last_t_min}
    ORDER BY t_min ASC
    """
    last_t_min = 0.0
    total = 0
    while True:
        table = _tap_search(
            tap, query.format(page_size=page_size, obs_collection=obs_collection, last_t_min=last_t_min), page_size
        )
        if len(table) == 0:
            break
        obs_ids, statuses = [], []
        max_t_min = last_t_min
        for row in table:
            max_t_min = max(max_t_min, float(row["t_min"]))
            status = reduction_status_from_calib_level(row["calib_level"])
            if status is not None:
                obs_ids.append(str(row["obs_publisher_did"]))
                statuses.append(status)
        n = _update_by_obs_id(conn, archive_code, obs_ids, statuses)
        total += n
        logger.info("%s: page of %d rows -> %d updated (t_min watermark now %.3f)", archive_code, len(table), n, max_t_min)
        if len(table) < page_size:
            break
        last_t_min = max_t_min
    logger.info("%s: done, %d rows updated total", archive_code, total)


def backfill_gemini(conn: psycopg.Connection) -> None:
    """gemini.py's own shape: ORDER BY t_min has a ~73s cliff on this
    endpoint regardless of page size, so this walks fixed 7-day windows
    instead, same as the real sync module -- the slowest of these backfills
    (~25 years of weekly windows) but the only safe way to page this table."""
    tap = make_tap_service(CADC_TAP_URL)
    query = """
    SELECT obs_publisher_did, calib_level, t_min
    FROM ivoa.ObsCore
    WHERE obs_collection IN ('GEMINI', 'GEMINICADC') AND dataproduct_type = 'spectrum'
    AND t_min >= {window_start} AND t_min < {window_end}
    """
    window_days = 7
    window_start = 51946.0  # live-confirmed in gemini.py: MIN(t_min) WHERE t_min > 0
    now_t_min = Time.now().mjd
    total = 0
    windows_done = 0
    while window_start < now_t_min:
        window_end = window_start + window_days
        table = _tap_search(tap, query.format(window_start=window_start, window_end=window_end), 20000)
        if len(table) > 0:
            obs_ids, statuses = [], []
            for row in table:
                status = reduction_status_from_calib_level(row["calib_level"])
                if status is not None:
                    obs_ids.append(str(row["obs_publisher_did"]))
                    statuses.append(status)
            n = _update_by_obs_id(conn, "gemini", obs_ids, statuses)
            total += n
        window_start = window_end
        windows_done += 1
        if windows_done % 100 == 0:
            logger.info("gemini: %d windows scanned (up to t_min %.1f), %d rows updated so far", windows_done, window_start, total)
    logger.info("gemini: done, %d windows scanned, %d rows updated total", windows_done, total)


def backfill_eso(conn: psycopg.Connection) -> None:
    """eso.py's shape: plain t_min watermark, no per-ID dedup (one ObsCore
    row per dp_id, confirmed by that module doing none either)."""
    tap = make_tap_service(ESO_TAP_URL)
    query = """
    SELECT TOP {page_size} dp_id, calib_level, t_min
    FROM ivoa.ObsCore
    WHERE dataproduct_type='spectrum' AND obs_collection != ''
    AND t_min > {last_t_min}
    ORDER BY t_min ASC
    """
    page_size = 50000
    last_t_min = 0.0
    total = 0
    while True:
        table = _tap_search(tap, query.format(page_size=page_size, last_t_min=last_t_min), page_size)
        if len(table) == 0:
            break
        obs_ids, statuses = [], []
        max_t_min = last_t_min
        for row in table:
            max_t_min = max(max_t_min, float(row["t_min"]))
            status = reduction_status_from_calib_level(row["calib_level"])
            if status is not None:
                obs_ids.append(str(row["dp_id"]))
                statuses.append(status)
        n = _update_by_obs_id(conn, "eso", obs_ids, statuses)
        total += n
        logger.info("eso: page of %d rows -> %d updated (t_min watermark now %.3f)", len(table), n, max_t_min)
        if len(table) < page_size:
            break
        last_t_min = max_t_min
    logger.info("eso: done, %d rows updated total", total)


def backfill_oirsa(conn: psycopg.Connection) -> None:
    """oirsa.py's shape: no cliff found up to a 2,000,000-row unbounded
    pull (~12s, confirmed live in that module) -- one shot rather than
    paginating, since there's nothing to gain from chunking here."""
    tap = make_tap_service(OIRSA_TAP_URL)
    query = "SELECT TOP 2000000 obs_publisher_did, calib_level FROM ivoa.obscore WHERE dataproduct_type = 'spectrum'"
    table = _tap_search(tap, query, 2000000)
    obs_ids, statuses = [], []
    for row in table:
        status = reduction_status_from_calib_level(row["calib_level"])
        if status is not None:
            obs_ids.append(str(row["obs_publisher_did"]))
            statuses.append(status)
    n = _update_by_obs_id(conn, "oirsa", obs_ids, statuses)
    logger.info("oirsa: %d rows fetched, %d updated", len(table), n)


def backfill_mast(conn: psycopg.Connection) -> None:
    """mast.py's shape: plain t_min watermark, no cliff -- but a single
    obs_id can carry several rows (raw/calibrated/housekeeping/... for HST,
    or up to 15 processing-stage variants for IUE/FUSE), so this matches on
    (obs_id, archive_url) together rather than obs_id alone -- otherwise a
    row here could get matched against a *different* variant's calib_level
    than the one mast.py's own _vo.fits-preferring dedup actually stored."""
    tap = make_tap_service(MAST_TAP_URL)
    query = """
    SELECT TOP {page_size} obs_id, access_url, calib_level, t_min
    FROM ivoa.obscore
    WHERE dataproduct_type='spectrum' AND obs_collection IN ('HST', 'IUE', 'FUSE')
    AND access_format IN ('application/fits', 'image/fits')
    AND t_min > {last_t_min}
    ORDER BY t_min ASC
    """
    page_size = 20000
    last_t_min = 0.0
    total = 0
    while True:
        table = _tap_search(tap, query.format(page_size=page_size, last_t_min=last_t_min), page_size)
        if len(table) == 0:
            break
        obs_ids, urls, statuses = [], [], []
        max_t_min = last_t_min
        for row in table:
            max_t_min = max(max_t_min, float(row["t_min"]))
            status = reduction_status_from_calib_level(row["calib_level"])
            if status is not None:
                obs_ids.append(str(row["obs_id"]))
                urls.append(str(row["access_url"]))
                statuses.append(status)
        n = _update_by_obs_id_and_url(conn, "mast", obs_ids, urls, statuses)
        total += n
        logger.info("mast: page of %d rows -> %d updated (t_min watermark now %.3f)", len(table), n, max_t_min)
        if len(table) < page_size:
            break
        last_t_min = max_t_min
    logger.info("mast: done, %d rows updated total", total)


def backfill_mast_jwst(conn: psycopg.Connection) -> None:
    """mast_jwst.py's shape: bounded MJD windows (an unbounded query 504s
    for this collection specifically), same (obs_id, archive_url) matching
    rationale as backfill_mast -- JWST's row multiplicity per obs_id is
    worse (20+ rows, including unrelated guide-star cal files)."""
    tap = make_tap_service(MAST_TAP_URL)
    query = """
    SELECT TOP {page_size} obs_id, access_url, calib_level, t_min
    FROM ivoa.obscore
    WHERE dataproduct_type='spectrum' AND obs_collection='JWST'
    AND access_format='application/fits'
    AND t_min > {lo} AND t_min < {hi}
    ORDER BY t_min ASC
    """
    page_size = 20000
    window_days = 10
    lo = 59643  # JWST_LAUNCH_MJD, see sync/archives/mast_jwst.py
    now_mjd = Time.now().mjd
    total = 0
    windows_done = 0
    while lo < now_mjd:
        hi = lo + window_days
        table = _tap_search(tap, query.format(page_size=page_size, lo=lo, hi=hi), page_size)
        if len(table) > 0:
            obs_ids, urls, statuses = [], [], []
            for row in table:
                status = reduction_status_from_calib_level(row["calib_level"])
                if status is not None:
                    obs_ids.append(str(row["obs_id"]))
                    urls.append(str(row["access_url"]))
                    statuses.append(status)
            n = _update_by_obs_id_and_url(conn, "mast_jwst", obs_ids, urls, statuses)
            total += n
        lo = hi
        windows_done += 1
        if windows_done % 20 == 0:
            logger.info("mast_jwst: %d windows scanned (up to MJD %.1f), %d rows updated so far", windows_done, lo, total)
    logger.info("mast_jwst: done, %d windows scanned, %d rows updated total", windows_done, total)


ARCHIVE_BACKFILLS = {
    # Cheap first. gemini_ghost/gemini_igrins share one function (a single
    # UPDATE covers both archive_codes) -- either key runs the same thing.
    "koa": backfill_koa,
    "gemini_ghost": backfill_gemini_goa,
    "gemini_igrins": backfill_gemini_goa,
    "naoj": backfill_naoj,
    # Expensive, roughly cheapest-to-slowest.
    "mast_jwst": backfill_mast_jwst,
    "dao": backfill_dao,
    "mast": backfill_mast,
    "cfht_cadc": backfill_cfht_cadc,
    "oirsa": backfill_oirsa,
    "eso": backfill_eso,
    "gemini": backfill_gemini,
}


def main() -> None:
    requested = sys.argv[1:]
    archive_codes = requested if requested else list(ARCHIVE_BACKFILLS.keys())
    unknown = set(archive_codes) - set(ARCHIVE_BACKFILLS.keys())
    if unknown:
        raise SystemExit(f"Unknown archive_code(s): {sorted(unknown)}. Valid: {sorted(ARCHIVE_BACKFILLS.keys())}")

    # Same per-archive isolation as sync/main.py's own driver -- an external
    # TAP service timing out (confirmed live: CADC hung mid-page during this
    # script's first production run, 2026-08-03) shouldn't take down every
    # archive after it. Each backfill function already commits per-page, so
    # a failed archive just leaves its own reduction_status = 'unknown' rows
    # in place -- safe to re-run (this whole script is idempotent) once the
    # underlying service recovers.
    failed = []
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        for archive_code in archive_codes:
            logger.info("=== starting %s ===", archive_code)
            try:
                ARCHIVE_BACKFILLS[archive_code](conn)
            except Exception:
                logger.exception("%s: failed", archive_code)
                conn.rollback()
                failed.append(archive_code)

    if failed:
        logger.error("archives failed: %s", ", ".join(failed))
        sys.exit(1)


if __name__ == "__main__":
    main()
