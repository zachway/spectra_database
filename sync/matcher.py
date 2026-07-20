"""Match raw archive observation records against tracked stars.

Three paths, in priority order — identifier match first, position as backup
only, matching the "easy match first" design (full likelihood-ratio matching
is still deferred):

- direct_gaia_column: the archive already reports a Gaia source_id — just
  check it's one of ours.
- name_resolved: no Gaia column, but the record's raw_target_name matches one
  of a tracked star's cached SIMBAD aliases. Tried before positional matching
  because position can fail even when correctly propagated — Gaia's
  single-star astrometric fit can be biased for binaries/crowded fields (seen
  live: a CFHT/CADC record for Stein 2051 A, a known visual binary, missed
  its positional match despite correct proper motion — its identifier would
  have caught it). Identifier match sidesteps that entirely.
- positional_easy_match: only for records that didn't identifier-match.
  Propagate every tracked star's proper motion to the observation's epoch,
  then tight-radius match against the raw record's position, using only our
  own tracked star list as the candidate catalog (not the full Gaia catalog —
  deferred along with full LR matching). Exactly one star within radius ->
  matched; more than one -> needs_review (ambiguous); zero -> the record
  isn't one of ours and is silently skipped (bulk archive tables hold far
  more objects than we track).
"""

import re
import warnings
from collections import defaultdict

import numpy as np
import psycopg
from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time
from erfa import ErfaWarning

from sync.base import RawObservation

EASY_MATCH_RADIUS_ARCSEC = 1.0


def _normalize_name(name: str) -> str:
    key = re.sub(r"\s+", "", name).upper()
    if key.startswith("GL"):
        # "Gl" (Gliese) and "GJ" (Gliese-Jahreiss) are used interchangeably
        # for the same catalog in practice — e.g. CFHT's "Gl169.1A" vs
        # SIMBAD's "GJ 169.1 A" for the same star.
        key = "GJ" + key[2:]
    return key


def _load_stars(conn: psycopg.Connection) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute("SELECT gaia_source_id, ra, dec, ref_epoch, pmra, pmdec FROM stars")
        return cur.fetchall()


def _load_star_aliases(conn: psycopg.Connection) -> dict[str, int]:
    """Normalized alias -> gaia_source_id, for identifier matching."""
    with conn.cursor() as cur:
        cur.execute("SELECT gaia_source_id, name_aliases FROM stars WHERE name_aliases IS NOT NULL")
        rows = cur.fetchall()
    lookup: dict[str, int] = {}
    for gaia_source_id, aliases in rows:
        for alias in aliases or []:
            lookup[_normalize_name(alias)] = gaia_source_id
    return lookup


def _propagate(star_rows: list[tuple], obs_jyear: float) -> tuple[list[int], SkyCoord]:
    ids, ra, dec, ref_epoch, pmra, pmdec = zip(*star_rows)
    coords = SkyCoord(
        ra=np.array(ra) * u.deg,
        dec=np.array(dec) * u.deg,
        pm_ra_cosdec=np.nan_to_num(np.array(pmra, dtype=float)) * u.mas / u.yr,
        pm_dec=np.nan_to_num(np.array(pmdec, dtype=float)) * u.mas / u.yr,
        obstime=Time(np.array(ref_epoch, dtype=float), format="jyear"),
        frame="icrs",
    )
    with warnings.catch_warnings():
        # No distance/parallax is passed in, so ERFA substitutes a default
        # distance for the (irrelevant at our precision) perspective term —
        # confirmed negligible (< 1e-10 arcsec even for Barnard's Star over
        # 10 years).
        warnings.filterwarnings("ignore", category=ErfaWarning, message=".*distance overridden.*")
        propagated = coords.apply_space_motion(new_obstime=Time(obs_jyear, format="jyear"))
    return list(ids), propagated


def _to_jyear(obs_date) -> float:
    return Time(obs_date.isoformat()).jyear


def _upsert_holding(
    cur: psycopg.Cursor,
    archive_code: str,
    rec: RawObservation,
    gaia_source_id: int | None,
    match_method: str,
    match_status: str,
    theta_arcsec: float | None,
) -> None:
    cur.execute(
        """
        INSERT INTO spectroscopy_holdings
            (gaia_source_id, archive_code, archive_obs_id, archive_url, instrument,
             obs_date, program_id, match_method, match_status, theta_arcsec,
             raw_target_name, raw_ra, raw_dec, updated_at)
        VALUES (%(gaia_source_id)s, %(archive_code)s, %(archive_obs_id)s, %(archive_url)s,
                %(instrument)s, %(obs_date)s, %(program_id)s, %(match_method)s, %(match_status)s,
                %(theta_arcsec)s, %(raw_target_name)s, %(raw_ra)s, %(raw_dec)s, now())
        ON CONFLICT (archive_code, archive_obs_id) DO UPDATE SET
            gaia_source_id = EXCLUDED.gaia_source_id,
            archive_url = EXCLUDED.archive_url,
            instrument = EXCLUDED.instrument,
            obs_date = EXCLUDED.obs_date,
            program_id = EXCLUDED.program_id,
            match_method = EXCLUDED.match_method,
            match_status = EXCLUDED.match_status,
            theta_arcsec = EXCLUDED.theta_arcsec,
            raw_target_name = EXCLUDED.raw_target_name,
            raw_ra = EXCLUDED.raw_ra,
            raw_dec = EXCLUDED.raw_dec,
            updated_at = now()
        """,
        {
            "gaia_source_id": gaia_source_id,
            "archive_code": archive_code,
            "archive_obs_id": rec.archive_obs_id,
            "archive_url": rec.archive_url,
            "instrument": rec.instrument,
            "obs_date": rec.obs_date,
            "program_id": rec.program_id,
            "match_method": match_method,
            "match_status": match_status,
            "theta_arcsec": theta_arcsec,
            "raw_target_name": rec.raw_target_name,
            "raw_ra": rec.ra,
            "raw_dec": rec.dec,
        },
    )


def match_records(conn: psycopg.Connection, archive_code: str, records: list[RawObservation]) -> dict:
    counts = {"direct_matched": 0, "name_matched": 0, "positional_matched": 0, "needs_review": 0, "skipped": 0}

    direct = [r for r in records if r.gaia_source_id is not None]
    no_gaia_column = [r for r in records if r.gaia_source_id is None]

    with conn.cursor() as cur:
        for r in direct:
            cur.execute("SELECT 1 FROM stars WHERE gaia_source_id = %s", (r.gaia_source_id,))
            if cur.fetchone() is None:
                counts["skipped"] += 1
                continue
            _upsert_holding(cur, archive_code, r, r.gaia_source_id, "direct_gaia_column", "matched", None)
            counts["direct_matched"] += 1
    conn.commit()

    # Identifier match — tried before position, not just as a tiebreaker.
    alias_lookup = _load_star_aliases(conn)
    positional = []
    with conn.cursor() as cur:
        for r in no_gaia_column:
            gaia_id = alias_lookup.get(_normalize_name(r.raw_target_name)) if r.raw_target_name else None
            if gaia_id is not None:
                _upsert_holding(cur, archive_code, r, gaia_id, "name_resolved", "matched", None)
                counts["name_matched"] += 1
            else:
                positional.append(r)
    conn.commit()

    positional = [r for r in positional if r.ra is not None and r.dec is not None and r.obs_date is not None]
    if not positional:
        return counts

    star_rows = _load_stars(conn)
    if not star_rows:
        counts["skipped"] += len(positional)
        return counts

    by_epoch = defaultdict(list)
    for r in positional:
        by_epoch[_to_jyear(r.obs_date)].append(r)

    with conn.cursor() as cur:
        for epoch, recs in by_epoch.items():
            ids, propagated = _propagate(star_rows, epoch)
            targets = SkyCoord(ra=[r.ra for r in recs] * u.deg, dec=[r.dec for r in recs] * u.deg)

            # search_around_sky's first return value indexes the *argument*
            # (propagated), the second indexes self (targets) — the reverse
            # of what the field names suggest. Verified empirically.
            idx_cat, idx_target, sep2d, _ = targets.search_around_sky(propagated, EASY_MATCH_RADIUS_ARCSEC * u.arcsec)
            candidates = defaultdict(list)
            for cat_i, target_i, sep in zip(idx_cat, idx_target, sep2d):
                candidates[target_i].append((ids[cat_i], sep.arcsec))

            for i, r in enumerate(recs):
                cands = candidates.get(i, [])
                if not cands:
                    counts["skipped"] += 1
                elif len(cands) == 1:
                    gaia_id, theta = cands[0]
                    _upsert_holding(cur, archive_code, r, gaia_id, "positional_easy_match", "matched", float(theta))
                    counts["positional_matched"] += 1
                else:
                    best_theta = min(c[1] for c in cands)
                    _upsert_holding(cur, archive_code, r, None, "positional_easy_match", "needs_review", float(best_theta))
                    counts["needs_review"] += 1
    conn.commit()
    return counts
