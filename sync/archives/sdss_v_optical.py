"""SDSS-V Optical (BOSS, current era) — CAS SQL, no Gaia column exposed there.

The bulk spAll/spAll-lite FITS product has a first-party GAIA_ID column
(public docs say Gaia DR2 source_id; confirmed already DR3 internally, with
DR20 — due ~Aug 2026 — expected to make that public), but it isn't exposed
through CAS SQL's mos_sdssv_boss_spall projection. Live-verified: FPS-era
(robot, field_id 16200-16299) catalogid-to-Gaia population via the
mos_catalog_to_gaia_dr2_source crosswalk is ~75.8%, comparable to plate-era
(~68.4%) — the crosswalk itself is usable — but that's a proxy, not the
literal GAIA_ID column inside an FPS-era spAll row (the public CAS spAll
view currently has zero FPS-era rows to check directly).
"""

from sync.base import RawObservation


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    raise NotImplementedError(
        "SDSS-V Optical: GAIA_ID requires pulling the bulk spAll/spAll-lite file "
        "(large — not downloadable from this dev machine) or an authenticated "
        "CasJobs check that was never done. Until DR20 confirms the DR2->DR3 "
        "switch, treat any pulled GAIA_ID as DR2 and reconcile explicitly. The "
        "mos_catalog_to_gaia_dr2_source crosswalk is a workable interim fallback "
        "if a positional/catalogid-based path is preferred over waiting on the "
        "bulk file."
    )
