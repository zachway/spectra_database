"""SALT HRS (SAAO/SALT Data Archive, SSDA) — GraphQL, not TAP.

A genuinely different access pattern from every other archive in this
codebase: SSDA (ssda.saao.ac.za) exposes a GraphQL API at /api, not a TAP/VO
service — the site's own `/tap` and `/vo/tap` routes are red herrings (just
React-Router catch-all paths in the SPA, not a real endpoint; confirmed
live, both return the SPA shell). Found the real API and its query shape by
pulling the SPA's own JS bundle (`static/js/main.*.chunk.js`) and grepping
for how it builds `where` clauses — the GraphQL schema's `where` argument is
a plain String, but the string itself must be a specific JSON shape the
bundle constructs via internal helpers, e.g. `{"EQUALS": {"column":
"instrument.name", "value": "HRS"}}` — passing anything else (a bare SQL-ish
expression, a naively-nested `{instrument: {name: ...}}`) fails with either
a JSON parse error or "where condition could not be parsed", confirmed both
live while reverse-engineering the real shape.

`columns` must be exact dotted table.column paths (also pulled from the
bundle, e.g. "artifact.artifact_id" not "artifact_id" — the latter fails
with "table ... does not exist in the database model", confirmed live).
Results come back as a flat `metadata: [{name, value}]` list per row
(`dataFiles.dataFiles[].metadata`), not a normal object with typed fields.

47,495 HRS rows confirmed live via `pageInfo.itemsTotal`. `position.ra`/
`position.dec` are null on every row (confirmed) — name-only match, same
situation as feros_gavo.py/flashheros_gavo.py, not a limitation of this
implementation. `observation.data_release` timestamps show this archive
serves metadata for embargoed files too (a real future release date exists
on live rows) — this module doesn't filter them out (same reasoning as
lick.py including nights whose proprietary status it can't otherwise know):
the record is a legitimate holding either way, its `archive_url` will just
403 until release, same as any other archive's embargoed content.

Two-phase cursor, not a single date watermark from the start — the API has
no explicit ORDER BY control, and confirmed live to default to *descending*
observation_time (newest first, not oldest first like every TAP archive's
ORDER BY t_min ASC elsewhere in this codebase). A naive "advance a
GREATER_EQUAL watermark to the max seen per page" scheme — the pattern
every TAP archive here uses — breaks completely under descending order: the
very first page's max IS the global max, so the cursor would jump straight
to "caught up" after one page and silently abandon the other ~90% of the
archive forever. Confirmed this failure live before switching approach.

Phase 1 ("backfill", the default/starting state — no `max_seen_ms` in the
cursor yet): walks the whole archive via plain `startIndex` pagination with
only the instrument filter (no date bound), relying on `startIndex` offset
pagination being stable call-to-call — the same assumption any offset-based
REST/GraphQL pagination makes. Tracks the running max observation_time seen
across every page. Once a page comes back shorter than PAGE_SIZE (the true
end of the archive), backfill is done and the cursor switches permanently
to phase 2 by writing `max_seen_ms`.

Phase 2 ("incremental", once `max_seen_ms` is present): now safe to use a
GREATER_EQUAL watermark on observation_time.start_time, same idea as every
other archive's t_min watermark, to pick up only new observations added
since backfill finished. The schema only exposes GREATER_EQUAL, not a strict
GREATER_THAN (confirmed via the same bundle grep) — stores `max_seen_ms + 1`
rather than `max_seen_ms` so `>=` doesn't keep re-matching the boundary row
forever, the same effect eso.py/dao.py get from a real `>` in SQL.

In both phases, the GREATER_EQUAL/date-bound `value` must be an ISO-8601
timestamp string, not the raw epoch-millisecond integer the same field
returns in results — confirmed live: passing the ms value as a bare
number/numeric string fails ("date/time field value out of range" or
"invalid input syntax for type timestamp", depending on the value), while
an ISO string like "1970-01-01T00:00:00.000Z" works.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import requests

from sync.base import RawObservation

API_URL = "https://ssda.saao.ac.za/api"

DOWNLOAD_URL = "https://ssda.saao.ac.za/api/data/{artifact_id}/{filename}"

PAGE_SIZE = 5000

COLUMNS = [
    "target.name",
    "observation_time.start_time",
    "artifact.name",
    "artifact.artifact_id",
]

QUERY = """
query($where: String!, $columns: [String!]!, $limit: Int, $startIndex: Int) {
  dataFiles(where: $where, columns: $columns, limit: $limit, startIndex: $startIndex) {
    dataFiles { id metadata { name value } }
    pageInfo { itemsTotal }
  }
}
"""


INSTRUMENT_FILTER = {"EQUALS": {"column": "instrument.name", "value": "HRS"}}


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _run_query(where: dict, start_index: int) -> list[dict]:
    response = requests.post(
        API_URL,
        json={
            "query": QUERY,
            "variables": {
                "where": json.dumps(where),
                "columns": COLUMNS,
                "limit": PAGE_SIZE,
                "startIndex": start_index,
            },
        },
        timeout=(15, 180),
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("errors"):
        raise RuntimeError(f"SSDA GraphQL error: {payload['errors']}")
    return payload["data"]["dataFiles"]["dataFiles"]


def _to_records(items: list[dict]) -> tuple[list[RawObservation], int]:
    records = []
    max_start_time_ms = 0
    for item in items:
        meta = {m["name"]: m["value"] for m in item["metadata"]}

        start_time_raw = meta.get("observation_time.start_time")
        obs_date: date | None = None
        if start_time_raw:
            start_time_ms = int(start_time_raw)
            max_start_time_ms = max(max_start_time_ms, start_time_ms)
            obs_date = datetime.fromtimestamp(start_time_ms / 1000, tz=timezone.utc).date()

        artifact_id = meta.get("artifact.artifact_id") or item["id"]
        filename = meta.get("artifact.name")

        records.append(
            RawObservation(
                archive_obs_id=artifact_id,
                archive_url=DOWNLOAD_URL.format(artifact_id=artifact_id, filename=filename),
                instrument="HRS",
                obs_date=obs_date,
                raw_target_name=meta.get("target.name"),
            )
        )
    return records, max_start_time_ms


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    if "max_seen_ms" in cursor:
        # Phase 2: incremental, safe now that phase 1 has covered history.
        watermark = cursor["max_seen_ms"]
        where = {"AND": [INSTRUMENT_FILTER, {"GREATER_EQUAL": {"column": "observation_time.start_time", "value": _iso(watermark)}}]}
        items = _run_query(where, start_index=0)
        records, page_max = _to_records(items)
        new_cursor = {"max_seen_ms": page_max + 1 if records else watermark}
        return records, new_cursor

    # Phase 1: backfill via plain startIndex pagination (see module
    # docstring for why a date watermark can't be used yet here).
    start_index = cursor.get("start_index", 0)
    running_max = cursor.get("running_max_ms", 0)
    items = _run_query({"AND": [INSTRUMENT_FILTER]}, start_index=start_index)
    records, page_max = _to_records(items)
    running_max = max(running_max, page_max)

    if len(records) < PAGE_SIZE:
        # Reached the true end of the archive — switch to phase 2 for good.
        new_cursor = {"max_seen_ms": running_max + 1}
    else:
        new_cursor = {"start_index": start_index + PAGE_SIZE, "running_max_ms": running_max}
    return records, new_cursor
