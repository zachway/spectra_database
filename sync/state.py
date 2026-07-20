"""Read/write archive_sync_state — per-archive sync progress bookkeeping."""

import psycopg
from psycopg.types.json import Jsonb


def get_cursor(conn: psycopg.Connection, archive_code: str) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT sync_cursor FROM archive_sync_state WHERE archive_code = %s", (archive_code,))
        row = cur.fetchone()
    return row[0] if row else {}


def record_run(
    conn: psycopg.Connection,
    archive_code: str,
    cursor: dict,
    status: str,
    notes: str,
    rows_seen: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO archive_sync_state
                (archive_code, sync_cursor, last_run_at, last_run_status, last_run_notes, rows_seen_last_run)
            VALUES (%s, %s, now(), %s, %s, %s)
            ON CONFLICT (archive_code) DO UPDATE SET
                sync_cursor = EXCLUDED.sync_cursor,
                last_run_at = EXCLUDED.last_run_at,
                last_run_status = EXCLUDED.last_run_status,
                last_run_notes = EXCLUDED.last_run_notes,
                rows_seen_last_run = EXCLUDED.rows_seen_last_run
            """,
            (archive_code, Jsonb(cursor), status, notes, rows_seen),
        )
    conn.commit()
