"""One-off: track every star in the Yale Bright Star Catalogue (BSC5), not
just the ones Gaia is missing.

scripts.seed_bsc5_bright_stars only seeded the ~70 BSC5 stars confirmed
absent from Gaia. This is the complement: the other ~9,000+ BSC5 stars
that DO have a Gaia counterpart, added via the normal Gaia path
(ingest.add_star.add_stars_batch) instead of source_catalog='bsc5' -- so
every naked-eye star ends up tracked one way or another, not just the ones
some archive happened to already report.

Cross-matches the full BSC5 catalog (VizieR V/50/catalog) against
gaiadr3.gaia_source in chunks (a single ~9,000-row upload/join in one TAP
call is what caused the timeouts/500s seen during the original BSC5-gap
analysis -- see project history). For each BSC star, picks the brightest
Gaia candidate within radius as the match, but only trusts it if it isn't
implausibly faint (>3 mag fainter than the BSC star's own Vmag is almost
certainly an unrelated neighbor/artifact, not the star itself -- same
sanity check used to build the original 70-star gap list). Anything with
no credible Gaia match at all gets added via add_bsc_star instead --
should only be the ~70 already-known ones, but computed fresh here rather
than hardcoded, in case this ever needs re-running against a newer Gaia
data release.

Every star, Gaia- or BSC5-sourced, gets "HR <n>" seeded as a known alias
(SIMBAD recognizes this directly, and it's always available regardless of
whether the star has a Bayer/Flamsteed name) plus the BSC5 Name field where
present -- so an archive reporting a bright star by its HR designation
matches immediately, not just the ones ingest.add_star.add_bsc_star already
covers via SIMBAD's full alias list.

Usage:
    DATABASE_URL=postgresql:///spectra_local python3 -m scripts.seed_bright_star_catalog
"""

from __future__ import annotations

import logging
import os

import numpy as np
from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.table import Table
from astroquery.gaia import Gaia
from astroquery.vizier import Vizier
import psycopg

from ingest.add_star import add_bsc_star, add_stars_batch

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Chunked, not one ~9,000-row upload -- a single unchunked cross-match of
# the full catalog timed out/500'd during the original BSC5-gap analysis
# (confirmed live). This size cleared reliably at that time.
XMATCH_CHUNK_SIZE = 1500

# Generous compared to a typical archive's own position error, to absorb
# BSC5's dated positions plus real proper motion on nearby bright stars --
# same reasoning as the original gap analysis, just applied catalog-wide
# instead of only to the V<3 sample.
XMATCH_RADIUS_ARCSEC = 15.0

# A Gaia candidate fainter than the BSC star's own Vmag by more than this is
# almost certainly an unrelated neighbor (diffraction-spike artifact, faint
# background/foreground source), not the star itself -- confirmed live
# during the original 70-star gap analysis.
SUSPICIOUS_MAG_GAP = 3.0

XMATCH_QUERY = """
SELECT u.bsc_row, u.hr,
       g.source_id, g.phot_g_mean_mag,
       DISTANCE(POINT('ICRS', u.ra, u.dec), POINT('ICRS', g.ra, g.dec)) * 3600 AS sep_arcsec
FROM tap_upload.bsc AS u
JOIN gaiadr3.gaia_source AS g
  ON 1 = CONTAINS(POINT('ICRS', u.ra, u.dec), CIRCLE('ICRS', g.ra, g.dec, {radius_deg}))
"""


def _load_bsc5_catalog() -> Table:
    Vizier.ROW_LIMIT = -1
    bsc = Vizier.get_catalogs("V/50")["V/50/catalog"]
    bsc = bsc[~bsc["RAJ2000"].mask & ~bsc["DEJ2000"].mask & ~bsc["Vmag"].mask]
    coords = SkyCoord(bsc["RAJ2000"], bsc["DEJ2000"], unit=(u.hourangle, u.deg))
    out = Table()
    out["hr"] = np.array(bsc["HR"], dtype=int)
    out["name"] = [str(n).strip() or None for n in bsc["Name"]]
    out["ra"] = coords.ra.deg
    out["dec"] = coords.dec.deg
    out["vmag"] = np.array(bsc["Vmag"], dtype=float)
    return out


def _resolve_gaia_ids(catalog: Table) -> dict[int, int]:
    """hr -> best-candidate gaia_source_id, for stars with a credible match."""
    resolved: dict[int, int] = {}
    radius_deg = XMATCH_RADIUS_ARCSEC / 3600.0

    for i in range(0, len(catalog), XMATCH_CHUNK_SIZE):
        chunk = catalog[i : i + XMATCH_CHUNK_SIZE]
        up = Table()
        up["bsc_row"] = np.arange(len(chunk))
        up["hr"] = chunk["hr"]
        up["ra"] = chunk["ra"]
        up["dec"] = chunk["dec"]
        upload_path = "/tmp/seed_bsc_xmatch_upload.vot"
        up.write(upload_path, format="votable", overwrite=True)

        job = Gaia.launch_job_async(
            query=XMATCH_QUERY.format(radius_deg=radius_deg),
            upload_resource=upload_path,
            upload_table_name="bsc",
        )
        res = job.get_results()
        vmag_by_hr = {int(r["hr"]): float(r["vmag"]) for r in chunk}

        best: dict[int, tuple[float, int]] = {}
        for r in res:
            gmag = r["phot_g_mean_mag"]
            if np.ma.is_masked(gmag):
                continue
            hr = int(r["hr"])
            if hr not in best or gmag < best[hr][0]:
                best[hr] = (float(gmag), int(r["source_id"]))

        for hr, (gmag, source_id) in best.items():
            row_vmag = vmag_by_hr.get(hr)
            if row_vmag is not None and gmag - row_vmag > SUSPICIOUS_MAG_GAP:
                continue  # implausibly faint -- treat as no credible match
            resolved[hr] = source_id

        logger.info(
            "cross-matched %d/%d BSC5 stars, %d resolved so far",
            min(i + XMATCH_CHUNK_SIZE, len(catalog)), len(catalog), len(resolved),
        )

    return resolved


def seed(conn: psycopg.Connection) -> dict:
    catalog = _load_bsc5_catalog()
    logger.info("%d BSC5 stars with usable position/Vmag", len(catalog))

    resolved = _resolve_gaia_ids(catalog)
    by_hr = {int(row["hr"]): row for row in catalog}
    unresolved_hrs = sorted(set(by_hr) - set(resolved))
    logger.info("%d resolved to Gaia, %d need the BSC5 path", len(resolved), len(unresolved_hrs))

    gaia_ids = sorted(set(resolved.values()))
    known_aliases: dict[int, list[str]] = {}
    for hr, gaia_id in resolved.items():
        aliases = [f"HR {hr}"]
        name = by_hr[hr]["name"]
        if name:
            aliases.append(name)
        known_aliases.setdefault(gaia_id, []).extend(aliases)

    gaia_added = add_stars_batch(conn, gaia_ids, known_aliases=known_aliases)
    logger.info("add_stars_batch: %d/%d newly inserted (rest already tracked)", gaia_added, len(gaia_ids))

    bsc5_added = 0
    for hr in unresolved_hrs:
        try:
            add_bsc_star(conn, hr, input_name=by_hr[hr]["name"])
            bsc5_added += 1
        except Exception:
            logger.warning("HR %d failed via BSC5 path", hr, exc_info=True)

    return {
        "total_catalog": len(catalog),
        "resolved_to_gaia": len(resolved),
        "gaia_newly_added": gaia_added,
        "bsc5_path_count": len(unresolved_hrs),
        "bsc5_newly_added": bsc5_added,
    }


def main() -> None:
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        totals = seed(conn)
    logger.info("done: %s", totals)


if __name__ == "__main__":
    main()
