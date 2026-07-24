"""MAST JWST spectra — same ivoa.obscore TAP endpoint as mast.py (HST/IUE/
FUSE), but split into its own module because both the pagination shape and
the product-dedup logic are genuinely different, not just a config tweak.

The 504 previously written off in mast.py's docstring turns out to be a
real, reproducible cliff specific to obs_collection='JWST', not a "JWST
isn't queryable" dead end -- confirmed live:
  - A plain unbounded `t_min > watermark ORDER BY t_min` query -- the exact
    shape that works fine for HST/IUE/FUSE at 20,000 rows/page -- 504s for
    JWST even at TOP 10, and even narrowed to a single instrument
    (NIRSPEC%, 3.48M rows). COUNT(*) over the whole collection 504s too.
  - A TOP 10 query with the ORDER BY removed succeeds, but takes ~55s for
    10 rows -- confirms this isn't a sort-cost cliff layered on an
    otherwise-fast query (unlike CFHT/KOA), it's the JWST portion of the
    view itself that's slow to touch at all without a tight bound.
  - Adding a bounded t_min window (`t_min > lo AND t_min < hi`) fixes it
    completely: same ORDER BY, same TOP, sub-second to low-single-digit
    seconds every time tested, from JWST's 2022 commissioning period
    through mid-2026. COUNT(*) still 504s even inside a bounded window (an
    aggregate-specific issue, not shared by TOP), so cursor advancement
    below relies only on TOP+ORDER BY, never COUNT.

So: paginate by a bounded MJD window instead of mast.py's single open
watermark. WINDOW_DAYS=10 was chosen against live row counts across the
mission's lifetime so far -- sparse in 2022 (0-25k/30-day-window early on)
through the busiest period sampled (2026-ish, ~1,100 rows in a 10-day
window) -- with more than an order of magnitude of headroom under
PAGE_SIZE. If a future window is unexpectedly dense, cursor advancement
still handles it safely: last_t_min moves to the max t_min actually seen in
that page rather than jumping to the window's end, so a truncated window
gets picked up again (now narrower) on the next call, exactly like
mast.py's plain watermark -- it just also has an upper bound. A window with
zero rows (real scheduling gaps exist) advances the cursor to the window's
end too -- see below for why fetch() doesn't just return that empty page
straight to the caller.

Starting watermark (JWST_LAUNCH_MJD) is set before any real commissioning
spectra rather than 0, to avoid ~163 years of empty pre-launch windows on a
fresh cursor -- each empty window is cheap (confirmed live, ~0.2s) but
there's no reason to pay for thousands of them.

A real scheduling gap (JWST doesn't observe continuously) returning one
empty window is not itself a problem -- but sync.main's driver (sync_
archive in sync/main.py) stops paging an archive entirely the moment a
single fetch() call returns zero net records, on the (correct, for every
other archive here) assumption that an empty page means "caught up to the
present." A window-based cursor breaks that assumption: an empty window can
have real future data sitting just beyond it within the same run. So
fetch() loops internally over consecutive empty windows (bounded by
_MAX_WINDOWS_PER_CALL as a paranoid backstop, not a real limit -- at
WINDOW_DAYS=10 it covers ~8 years, comfortably past JWST's ~4-year archive
so far) until it either finds real rows or reaches the present (Time.now()
mjd), rather than handing sync.main a same-cursor-position empty page that
would wedge the driver's loop.

Product-row multiplicity is much worse here than IUE/FUSE's already-solved
version of this problem: a single science obs_id can carry 20+ rows of
dataproduct_type='spectrum' under access_format='application/fits' alone --
not just processing stages of the real exposure (uncal/rate/rateints/cal/
crf/s2d/x1d/...) but guide-star acquisition/tracking calibration files from
the same visit, which share the science exposure's obs_id despite being
unrelated engineering data (confirmed live: gs-acq/gs-fg/gs-track/gs-id
*_cal.fits rows mixed in under one obs_id). access_format itself is even
noisier before filtering -- the same obs_id/dataproduct_type='spectrum' rows
include image/jpeg and image/png preview thumbnails and text/plain logs
alongside the real FITS (confirmed live), so access_format='application/
fits' is filtered in SQL up front, same as mast.py already does.

Within the FITS-only rows, mast.py's single-suffix "_vo.fits" dedup doesn't
apply -- JWST has no equivalent convention, and the desired product varies
by mode. _x1d (extracted 1D spectrum) is standard across NIRSpec, NIRISS,
NIRCam grism, and MIRI's slit/slitless spectroscopy (confirmed live across
all four); MIRI MRS's IFU mode and NIRISS SOSS instead produce _s3d/_c1d
respectively. _JWST_PRODUCT_PRIORITY ranks these ahead of the intermediate
products (s2d/cal/crf/rate/rateints/bsub/trapsfilled/...) and, implicitly,
ahead of the unrelated guide-star cal.fits files (which match no ranked
suffix and only win the per-obs_id slot if no better product is present --
harmless if that ever happens, since it's still a real FITS file, just not
a spectrum; unlike mast.py's HST/IUE/FUSE, calibration-frame rows aren't
filtered out for JWST source selection, so this is the same accepted
tradeoff as CFHT/KOA/Gemini's calibration-frame notes elsewhere in sync/).

s_ra/s_dec masking (calibration-type exposures lacking real sky
coordinates) is the same live-confirmed pattern as mast.py/cfht_cadc.py/
eso.py -- read via clean_float, not a bare float().
"""

from astropy.time import Time

from sync.base import RawObservation, clean_float, make_tap_service

TAP_URL = "https://mast.stsci.edu/vo-tap/api/v0.1/caom"

QUERY = """
SELECT TOP {page_size} obs_id, s_ra, s_dec, t_min, instrument_name, target_name, access_url
FROM ivoa.obscore
WHERE dataproduct_type='spectrum' AND obs_collection='JWST'
AND access_format='application/fits'
AND t_min > {lo} AND t_min < {hi}
ORDER BY t_min ASC
"""

PAGE_SIZE = 20000

# Kept well under every window density sampled live (mission-lifetime worst
# case so far: ~1,100 rows/10-day window) -- see module docstring.
WINDOW_DAYS = 10

# 2022-03-05 -- ahead of JWST's first commissioning spectra (science ops
# began mid-2022) but not so far back that fast-forwarding through empty
# windows on a fresh cursor costs anything real.
JWST_LAUNCH_MJD = 59643

# Preference order for the one row worth keeping per obs_id -- see module
# docstring for why this varies by instrument mode rather than being a
# single fixed suffix like mast.py's IUE/FUSE "_vo.fits".
_JWST_PRODUCT_PRIORITY = ["_x1d.fits", "_s3d.fits", "_c1d.fits", "_x1dints.fits"]

# Paranoid backstop on the empty-window skip-ahead loop below -- see module
# docstring. Not expected to ever bind in practice.
_MAX_WINDOWS_PER_CALL = 300


def _product_rank(access_url: str) -> int:
    for rank, suffix in enumerate(_JWST_PRODUCT_PRIORITY):
        if access_url.endswith(suffix):
            return rank
    return len(_JWST_PRODUCT_PRIORITY)


def _fetch_window(tap, lo: float, hi: float) -> tuple[list[RawObservation], float, int]:
    """Returns (records, max_t_min_seen, raw_row_count) for one bounded window."""
    query = QUERY.format(page_size=PAGE_SIZE, lo=lo, hi=hi)
    table = tap.search(query, maxrec=PAGE_SIZE).to_table()

    max_t_min = lo
    by_obs_id: dict[str, dict] = {}
    for row in table:
        t_min = float(row["t_min"])
        max_t_min = max(max_t_min, t_min)

        obs_id = str(row["obs_id"])
        access_url = str(row["access_url"])
        existing = by_obs_id.get(obs_id)
        if existing is None or _product_rank(access_url) < _product_rank(existing["access_url"]):
            by_obs_id[obs_id] = {
                "t_min": t_min,
                "access_url": access_url,
                "instrument_name": str(row["instrument_name"]),
                "s_ra": row["s_ra"],
                "s_dec": row["s_dec"],
                "target_name": str(row["target_name"]),
            }

    records = [
        RawObservation(
            archive_obs_id=obs_id,
            archive_url=data["access_url"],
            instrument=data["instrument_name"],
            obs_date=Time(data["t_min"], format="mjd").to_datetime().date(),
            ra=clean_float(data["s_ra"]),
            dec=clean_float(data["s_dec"]),
            raw_target_name=data["target_name"],
        )
        for obs_id, data in by_obs_id.items()
    ]
    return records, max_t_min, len(table)


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    lo = cursor.get("last_t_min", JWST_LAUNCH_MJD)
    now_mjd = Time.now().mjd
    tap = make_tap_service(TAP_URL)

    for _ in range(_MAX_WINDOWS_PER_CALL):
        if lo >= now_mjd:
            return [], {"last_t_min": lo}

        hi = lo + WINDOW_DAYS
        records, max_t_min, row_count = _fetch_window(tap, lo, hi)

        if records:
            # Truncated window (hit page_size): stay in it, just narrower
            # next time -- advance to the max t_min actually seen rather
            # than the window's end, same as mast.py's plain watermark.
            new_last_t_min = max_t_min if row_count >= PAGE_SIZE else hi
            return records, {"last_t_min": new_last_t_min}

        # Real scheduling gap, not truncation -- skip past this window and
        # keep looking rather than handing sync.main an empty page (see
        # module docstring for why that would wedge its convergence check).
        lo = hi

    return [], {"last_t_min": lo}
