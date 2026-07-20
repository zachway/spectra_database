"""MAST (HST/JWST/IUE/FUSE) — no upload-JOIN support (confirmed).

Not implemented: MAST's TAP interface reports objects by `objid`, while the
classic API (and most human-facing deep links) use `obsid` — these are
different namespaces and the reconciliation between them was never
live-verified. There's also an undocumented hang somewhere in the
100k-200k row range on bulk queries that needs a paging strategy before
this is safe to run unattended.
"""

from sync.base import RawObservation


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    raise NotImplementedError(
        "MAST: objid (TAP) vs obsid (classic API / deep links) namespace mismatch "
        "not yet reconciled, and the >100k-row query hang isn't characterized well "
        "enough to design safe paging. Needs another live investigation pass."
    )
