"""Gemini/IGRINS spectrograph — authenticated GOA JSON API (not CADC).

https://archive.gemini.edu/help/api.html

Same class of gap as gemini_ghost.py, confirmed live via caom2.Plane: every
one of IGRINS's 43,058 planes in CADC is raw (calibrationLevel=1) -- zero
reduced (level 2) planes at all, a complete gap rather than GHOST's patchy
one. Confirmed live via the GOA web UI (IGRINS/science search) that real
reduced products exist on GOA itself: alongside the bare raw H/K-band
frames (e.g. SDCH_20180421_0031.fits, SDCK_20180421_0031.fits), many
observations also have .spec2d.fits (2D-reduced), .spec.fits (1D-extracted),
.spec_a0v.fits (telluric-corrected against an A0V standard), and
.flux_a0v.fits (flux-calibrated) companion files -- IGRINS's own reduction
pipeline's naming convention, unrelated to GHOST's _calibrated/_dragons one.

Filters to filenames containing "spec_a0v.fits" -- one specific tier of
that reduction chain (telluric-corrected, standard IGRINS science
product), not every tier. Picking exactly one avoids creating multiple
duplicate holdings rows for what's really one physical spectrum just
reduced to different stages (spec2d -> spec -> spec_a0v -> flux_a0v all
describe the same underlying H-band or K-band exposure). Not every raw
exposure has a full reduction chain -- some early/test observations in the
real result set only ever got the bare raw file -- so this naturally skips
those, which is the intended behavior (nothing usable to point at yet).
A substring check, not an exact suffix match -- GOA serves these
bzip2-compressed (filenames end in .fits.bz2), confirmed live; an
endswith("spec_a0v.fits") check silently matched nothing at all against a
real response, which combined with the WINDOW_DAYS bug below produced a
convincing but wrong "zero results" first run (see below).

AUTHENTICATION: shared with gemini_ghost.py -- see that module's docstring
and sync/archives/_goa_common.py for the full GOA_SESSION_COOKIE story
(no API key, session cookie only, morgan being headless means a human has
to log in elsewhere and refresh the env var by hand).

FIRST_DATE live-confirmed (real result set went back to at least
2018-04-01). WINDOW_DAYS was originally 180, a guess -- confirmed live to
be badly wrong: jsonsummary silently caps responses at ~2000 rows (see
_goa_common.py), and a 180-day window's raw H/K frames alone (2 per
exposure, sometimes many more once reduction products are included) filled
that cap before ever reaching any spec_a0v.fits entries, which don't start
appearing until ~3 weeks into the window. The first real run consequently
returned zero matches and terminated after 1 page, looking like "archive
empty" rather than "window too wide to see the data." Shrunk to 7 days,
matching the precedent already established for gemini.py and
sdss_legacy_optical.py; _goa_common.py's row-cap guard now also makes any
future overly-wide window fail loudly instead of silently, so if 7 days
still isn't safe during a dense observing stretch, it'll be obvious.

VERIFIED LIVE (2026-07-22) that spec_a0v.fits files genuinely exist in
GOA's response for this instrument/date range (the 2000-row-capped
response just didn't reach them) -- not yet verified that a 7-day window
actually surfaces one end to end; watch the first real run.
"""

from datetime import date

from sync.archives._goa_common import fetch_reduced
from sync.base import RawObservation

INSTRUMENT = "IGRINS"

# Real result set (GOA web UI, IGRINS/science, unconstrained -- 500-row
# cap hit) showed observations back to 2018-04-01; used directly as a
# live-confirmed start rather than a guess.
FIRST_DATE = date(2018, 4, 1)

WINDOW_DAYS = 7


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    return fetch_reduced(cursor, INSTRUMENT, FIRST_DATE, WINDOW_DAYS, lambda filename: "spec_a0v.fits" in filename)
