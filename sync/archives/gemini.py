"""Gemini Observatory Archive — REST JSON /jsonsummary/.

Access mechanism and its real constraint are verified (not the docs' claim):
~2000 rows/query hard cap on /jsonsummary/, undocumented — confirmed by two
different-width date windows both truncating at exactly 2000. Workaround is
weekly date chunking (~260 sci-spectra/week gives ~8x headroom), splitting
and retrying any chunk landing >=1900. Filter on
canonical/OBJECT/spectroscopy/science; trust the `spectroscopy` boolean over
the `mode` string (saw one inconsistent engineering record).

Not implemented: the deep-link scheme (dataset ID -> browsable page or raw
FITS URL) was never live-verified — everything else here is ready, but I'm
not fabricating a URL pattern I haven't actually checked.
"""

from sync.base import RawObservation


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    raise NotImplementedError(
        "Gemini: query mechanism and row cap are verified (see module docstring), "
        "but the deep-link URL pattern from a dataset ID to a viewable/downloadable "
        "spectrum was never live-confirmed. Verify that before wiring this up."
    )
