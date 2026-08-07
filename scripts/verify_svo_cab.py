"""Standalone (non-pytest) live verification for sync/archives/svo_cab.py.

Run: python3 scripts/verify_svo_cab.py

Confirms fetch({}) returns real records spanning all 5 sub-collections, and
that calling fetch() again with the returned cursor converges to 0 new
records (svo_cab is a one-shot static-catalog pull, same shape as
rave.py/feros_gavo.py -- the cursor short-circuits the second call entirely).
"""

import collections

from sync.archives import svo_cab


def main() -> None:
    records, cursor = svo_cab.fetch({})
    print(f"fetch({{}}) -> {len(records)} records")

    by_instrument = collections.Counter(r.instrument for r in records)
    for instrument, count in sorted(by_instrument.items()):
        print(f"  {instrument}: {count}")

    assert len(by_instrument) == 5, f"expected all 5 sub-collections, got {sorted(by_instrument)}"

    with_date = sum(1 for r in records if r.obs_date is not None)
    print(f"records with a real obs_date: {with_date} (expected: XSL only, partial)")

    with_pos = sum(1 for r in records if r.ra is not None and r.dec is not None)
    print(f"records with ra/dec: {with_pos} / {len(records)}")

    sample = next(r for r in records if r.instrument == "MILES")
    print(f"sample MILES record: {sample}")

    records2, cursor2 = svo_cab.fetch(cursor)
    print(f"second fetch(cursor) -> {len(records2)} records (expect 0, static one-shot pull)")
    assert len(records2) == 0

    print("OK")


if __name__ == "__main__":
    main()
