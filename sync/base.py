"""Shared types for per-archive sync jobs."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date


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
