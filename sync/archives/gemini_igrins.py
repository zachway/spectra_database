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

Filters to filenames ending in "spec_a0v.fits" specifically -- one specific
tier of that reduction chain (telluric-corrected, standard IGRINS science
product), not every tier. Picking exactly one avoids creating multiple
duplicate holdings rows for what's really one physical spectrum just
reduced to different stages (spec2d -> spec -> spec_a0v -> flux_a0v all
describe the same underlying H-band or K-band exposure). Not every raw
exposure has a full reduction chain -- some early/test observations in the
real result set only ever got the bare raw file -- so this naturally skips
those, which is the intended behavior (nothing usable to point at yet).

AUTHENTICATION: shared with gemini_ghost.py -- see that module's docstring
and sync/archives/_goa_common.py for the full GOA_SESSION_COOKIE story
(no API key, session cookie only, morgan being headless means a human has
to log in elsewhere and refresh the env var by hand).

FIRST_DATE and WINDOW_DAYS are rougher guesses than gemini_ghost.py's --
IGRINS's real result set went back to at least 2018-04-01 (vs. GHOST's
2023), a ~7-year span on an instrument that's toured multiple telescopes
(McDonald, DCT, Gemini) with Gemini being only one host, and no per-window
row-density check was run against real data the way gemini.py's 7-day
window was. WINDOW_DAYS=180 is a conservative guess for a lower-volume,
longer-history instrument; revisit if a real run proves it wrong.

NOT LIVE-TESTED end to end, same reason as gemini_ghost.py's first cut --
GOA blocks anonymous access from every environment available while writing
this. First real run needs a human with a valid cookie watching it.
"""

from datetime import date

from sync.archives._goa_common import fetch_reduced
from sync.base import RawObservation

INSTRUMENT = "IGRINS"

# Real result set (GOA web UI, IGRINS/science, unconstrained -- 500-row
# cap hit) showed observations back to 2018-04-01; used directly as a
# live-confirmed start rather than a guess.
FIRST_DATE = date(2018, 4, 1)

WINDOW_DAYS = 180


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    return fetch_reduced(cursor, INSTRUMENT, FIRST_DATE, WINDOW_DAYS, lambda filename: filename.endswith("spec_a0v.fits"))
