"""WEAVE — no public data yet."""

from sync.base import RawObservation


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    raise NotImplementedError("WEAVE has no public data yet — nothing to sync.")
