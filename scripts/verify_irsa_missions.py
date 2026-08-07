"""Standalone (non-pytest) live verification for sync/archives/irsa_missions.py.

Run: python3 scripts/verify_irsa_missions.py

Phase 1: fetch({}) -> the four whole-sky collections (spitzer_sass,
spitzer_irs_std, sofia_exes, irtf_mearth) in one call.

Phase 2: repeatedly call fetch(cursor) to walk the full iso_sws/iras_lrs
sky-grid crawl (34 cells total), reporting per-collection totals -- both the
raw (pre-dedup, cells deliberately overlap) row count and the count of
distinct archive_obs_id (access_url) seen, which is what production's
UNIQUE(archive_code, archive_obs_id) upsert would actually retain.

Phase 3: one more fetch(cursor) call after the grid is exhausted, to confirm
it converges to 0 new records (the grid-done, no-op-forever cursor state).
"""

import collections
import time

from sync.archives import irsa_missions


def main() -> None:
    t0 = time.time()
    records, cursor = irsa_missions.fetch({})
    print(f"[phase 1] whole-sky fetch({{}}) -> {len(records)} records in {time.time() - t0:.1f}s")

    by_instrument = collections.Counter(r.instrument for r in records)
    for instrument, count in sorted(by_instrument.items()):
        print(f"  {instrument}: {count}")
    assert len(by_instrument) == 4, f"expected 4 whole-sky sub-collections, got {sorted(by_instrument)}"

    all_records = list(records)
    seen_obs_ids: set[str] = {r.archive_obs_id for r in records}
    grid_calls = 0
    grid_records_raw = 0
    grid_by_instrument_raw: collections.Counter = collections.Counter()
    grid_by_instrument_distinct: collections.Counter = collections.Counter()

    while cursor.get("grid_index", 0) < len(irsa_missions.GRID_TASKS):
        t1 = time.time()
        page_records, cursor = irsa_missions.fetch(cursor)
        grid_calls += 1
        grid_records_raw += len(page_records)
        for r in page_records:
            grid_by_instrument_raw[r.instrument] += 1
            if r.archive_obs_id not in seen_obs_ids:
                seen_obs_ids.add(r.archive_obs_id)
                grid_by_instrument_distinct[r.instrument] += 1
                all_records.append(r)
        print(
            f"[phase 2] grid call {grid_calls}/{len(irsa_missions.GRID_TASKS)} "
            f"(cursor grid_index={cursor['grid_index']}) -> {len(page_records)} rows "
            f"in {time.time() - t1:.1f}s"
        )

    print(f"\n[phase 2 summary] {grid_calls} grid calls, {grid_records_raw} raw rows "
          f"(cells deliberately overlap)")
    for instrument in sorted(set(grid_by_instrument_raw) | set(grid_by_instrument_distinct)):
        print(
            f"  {instrument}: {grid_by_instrument_raw[instrument]} raw rows -> "
            f"{grid_by_instrument_distinct[instrument]} distinct archive_obs_id"
        )
    assert set(grid_by_instrument_distinct) == {"ISO/SWS", "IRAS/LRS"}

    # Phase 3: confirm convergence -- grid exhausted, cursor is now a no-op.
    final_records, final_cursor = irsa_missions.fetch(cursor)
    print(f"\n[phase 3] fetch(cursor) after grid exhausted -> {len(final_records)} records (expect 0)")
    assert len(final_records) == 0
    assert final_cursor == cursor

    print(f"\nTOTAL distinct records across all 6 sub-collections: {len(all_records)}")
    total_by_instrument = collections.Counter(r.instrument for r in all_records)
    for instrument, count in sorted(total_by_instrument.items()):
        print(f"  {instrument}: {count}")

    with_date = sum(1 for r in all_records if r.obs_date is not None)
    print(f"records with a real obs_date: {with_date} (expected: sofia_exes + irtf_mearth only)")

    print("\nOK")


if __name__ == "__main__":
    main()
