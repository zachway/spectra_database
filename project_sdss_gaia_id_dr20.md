---
name: sdss-gaia-id-dr20-transition
description: SDSS BOSS spAll GAIA_ID column switches from Gaia DR2 to DR3 source_id in DR20 (~August 2026)
metadata: 
  node_type: memory
  type: project
  originSessionId: 016ee0ce-4ed9-42f7-b998-5c296a3dedaa
---

SDSS-V's bulk `spAll`/`spAll-lite` FITS product (BOSS optical spectrograph) has a `GAIA_ID` column that is currently documented as Gaia **DR2** source_id in the public DR19 release. Per the user (2026-07-17), this was updated to hold Gaia **DR3** source_id in newer internal SDSS data releases, and the next public release, **DR20, is expected around mid-to-late August 2026** (~1 month out from 2026-07-17).

**Why:** Affects how the spectra_database project should interpret/join on `GAIA_ID` for SDSS-V optical (BOSS) holdings — see [[archive-access-reference]].

**How to apply:** Until DR20 ships, treat `GAIA_ID` from the public bulk file as Gaia DR2 source_id (not a safe direct DR3 join without reconciliation). Once DR20 is public, re-check whether `GAIA_ID` has switched to DR3 source_id as expected, and update the SDSS-V ingestion/matching logic accordingly — this removes the need for the DR2-to-DR3 reconciliation caveat once confirmed.
