"""Generic driver: cursor in, fetch, match, cursor out — same shape for every archive."""

import psycopg

from sync import matcher, state
from sync.base import FetchFn


def run_sync(conn: psycopg.Connection, archive_code: str, fetch_fn: FetchFn) -> dict:
    cursor = state.get_cursor(conn, archive_code)
    try:
        records, new_cursor = fetch_fn(cursor)
    except Exception as exc:
        state.record_run(conn, archive_code, cursor, "failed", str(exc), 0)
        raise

    counts = matcher.match_records(conn, archive_code, records)
    state.record_run(conn, archive_code, new_cursor, "success", str(counts), len(records))
    return counts
