"""Shared types for per-archive sync jobs."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

import numpy as np
import pyvo
import requests


class _TimeoutSession(requests.Session):
    def __init__(self, timeout: tuple[float, float], **kwargs) -> None:
        super().__init__(**kwargs)
        self._timeout = timeout

    def request(self, *args, **kwargs):
        kwargs.setdefault("timeout", self._timeout)
        return super().request(*args, **kwargs)


def make_tap_service(url: str, timeout: tuple[float, float] = (15, 180)) -> pyvo.dal.TAPService:
    """pyvo.dal.TAPService with a bounded (connect, read) timeout instead of
    none at all — confirmed live, twice: a sync run sat blocked for 9+
    minutes reading a stalled SIMBAD response (fixed separately, see
    ingest.add_star's simbad_conf.timeout), then again reading a stalled
    CFHT/CADC TAP response via this exact unbounded pyvo.dal.TAPService
    pattern, used identically across cfht_cadc/eso/gemini/koa/mast/rave/
    galah. 180s read timeout leaves real margin over the slowest
    legitimately-observed query so far (gemini's documented 72.7s/1000
    rows) while still turning a stall into a caught, recoverable exception
    (DALFormatError, confirmed live) instead of an indefinite hang.
    """
    return pyvo.dal.TAPService(url, session=_TimeoutSession(timeout=timeout))


def clean_float(value) -> float | None:
    """Astropy/VO table rows carry masked (missing) numeric fields — a plain
    `is not None` check doesn't catch those (they're numpy.ma.masked, not
    Python None), so a naive `float(value)` silently produces NaN instead of
    a proper missing value. NaN ra/dec in particular crashes the matcher's
    KD-tree build outright (confirmed live via mast.py) rather than just
    being wrong — always use this when reading a possibly-masked column.
    """
    if value is None or np.ma.is_masked(value):
        return None
    return float(value)


@dataclass
class RawObservation:
    """One archive-native observation record, ready for matching.

    gaia_source_id: set only if the archive itself carries a Gaia source_id
    for this record (direct_gaia_column path). Leave None to go through
    positional_easy_match instead, in which case ra/dec/obs_date are required.
    """

    archive_obs_id: str
    archive_url: str
    instrument: str | None = None
    obs_date: date | None = None
    program_id: str | None = None
    gaia_source_id: int | None = None
    ra: float | None = None   # deg, ICRS, at obs_date
    dec: float | None = None  # deg, ICRS, at obs_date
    raw_target_name: str | None = None


# A per-archive fetch function has this shape: given the last sync_cursor,
# return new/updated records plus the cursor to persist for next time.
FetchFn = Callable[[dict], tuple[list[RawObservation], dict]]
