"""Generic driver: cursor in, discover, match, cursor out — same shape for every archive."""

import psycopg

from ingest.add_star import discover_stars
from sync import matcher, state
from sync.base import FetchFn


def run_sync(conn: psycopg.Connection, archive_code: str, fetch_fn: FetchFn) -> dict:
    cursor = state.get_cursor(conn, archive_code)
    try:
        records, new_cursor = fetch_fn(cursor)
    except Exception as exc:
        state.record_run(conn, archive_code, cursor, "failed", str(exc), 0)
        raise

    # Discover new stars from this page before matching — otherwise every
    # record for a not-yet-tracked star gets silently counted as "skipped"
    # by matcher.match_records (it only matches against stars already in the
    # table). See ingest.add_star.discover_stars.
    stars_added = discover_stars(conn, archive_code, records)

    counts = matcher.match_records(conn, archive_code, records)
    counts["stars_added"] = stars_added
    state.record_run(conn, archive_code, new_cursor, "success", str(counts), len(records))
    return counts
