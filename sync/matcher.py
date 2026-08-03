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
  have caught it). Identifier match sidesteps that entirely. Still sanity-
  checked against the record's own reported position when one is present
  (see NAME_MATCH_SANITY_RADIUS_ARCSEC) — confirmed live: "Mira" is SIMBAD's
  own proper name for omicron Ceti *and* an informal class label for any
  Mira-type long-period variable, so an archive using "Mira" generically for
  some other physical star was getting silently merged onto omicron Ceti's
  gaia_source_id. A record whose name matches but whose own position is
  nowhere near that star falls through to positional matching instead of
  being trusted blindly. If that fallback also finds no positional
  candidate, the record lands in needs_review rather than skipped — a
  rejected name match is a real, often-correct candidate (e.g. the archive's
  own logged position for that one exposure is simply wrong, not evidence of
  a different star — confirmed live: an HR9070/LQ And record 49 degrees from
  where that star is, SIMBAD-confirmed to be the correct alias regardless),
  so it's worth a human's attention rather than being silently dropped.
- positional_easy_match: only for records that didn't identifier-match. A
  q3c-indexed radial query (see _load_candidate_stars) narrows the tracked
  star list down to a small spatially-relevant candidate set per observation
  epoch, those get their proper motion propagated to the observation's
  epoch, then it's a tight-radius match against the raw record's position —
  using only our own tracked star list as the candidate catalog (not the
  full Gaia catalog — deferred along with full LR matching). Exactly one
  star within radius -> matched; more than one -> needs_review (ambiguous);
  zero -> the record isn't one of ours and is silently skipped (bulk archive
  tables hold far more objects than we track).
"""

from __future__ import annotations

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

# Barnard's Star, ~10.3"/yr, is the fastest known proper motion of any star —
# a safe upper bound on how far *any* tracked star's true position can have
# drifted per year since its ref_epoch. Used to size the q3c radial query in
# _load_candidate_stars: any star further than EASY_MATCH_RADIUS_ARCSEC plus
# its max possible drift from every target in an epoch group cannot possibly
# match, real proper motion or not, so the DB never needs to hand it back.
MAX_PM_ARCSEC_PER_YEAR = 10.3

# Gaia DR3's ref_epoch is uniformly 2016.0 for every source in the release
# (not a per-star varying value) — used directly to size the query radius
# below, since q3c needs it *before* it knows which stars it'll return.
GAIA_DR3_REF_EPOCH = 2016.0

# How far a name-matched record's own reported position may sit from the
# star it named before the name match is distrusted (see module docstring's
# "Mira" case). Set well above any offset a legitimate identifier-over-
# position case should ever show — the Stein 2051 A regression test uses a
# ~50" offset to represent a biased single-star astrometric fit on a real
# binary, and even a wide binary's true separation tops out at a few
# arcmin — while still tight enough to reliably catch a name collision that
# lands on a completely different part of the sky.
NAME_MATCH_SANITY_RADIUS_ARCSEC = 600.0

# (archive_code, instrument) -> positional match radius override, arcsec.
# Confirmed live via the name_resolved match pool (matched independent of
# position, so not subject to EASY_MATCH_RADIUS_ARCSEC's own cutoff): these
# instruments carry a real, persistent pointing bias well beyond 1" that
# silently drops otherwise-good positional matches to skipped/needs_review.
#
# noirlab/chiron: median offset (RA, Dec) = (+12.2", -2.4") across 28k
# name-resolved matches (2017-2023), n large enough that this is many-sigma
# significant, not noise. Not a single fixed vector though -- ruled out
# proper motion (corr with pmra/pmdec ~ -0.03/-0.06) and per-program causes
# (every row shares program_id='smarts', no finer tagging). RA-offset and
# Dec-offset are anti-correlated with each other (r=-0.62) and the median
# RA offset grows with declination -- the signature of a real pointing-model
# residual (this telescope's own spec page lists an equatorial mount, first
# light 1968, permanently on one side of the pier -- a plausible source of
# uncorrected polar-axis/non-perpendicularity terms), not per-exposure
# randomness. Radius chosen by simulating several candidates against
# spectroscopy_holdings' existing skipped/needs_review chiron rows -- see
# scripts/simulate_match_radius.py -- and picking the one that maximizes
# clean (single-candidate) recoveries before ambiguity (needs_review) takes
# over. Confirmed live: clean recoveries rise from 8,875 at 20" to a peak of
# 13,021 at 60", then *fall* to 11,190 at 90" as more of them pick up a
# second nearby candidate faster than new ones appear -- 60" is the actual
# optimum, not just "wide enough."
#
# eso/FEROS: median offset (RA, Dec) = (-59.6", +23.5") across 48k
# name-resolved matches (2003-2026), tightly clustered (90th-percentile
# separation only 99" vs. a 75" median -- most of the mass sits in a narrow
# band, unlike a random-mismatch tail). Ruled out proper motion and epoch
# drift the same way as chiron (both correlations ~0). Simulated the same
# way: clean recoveries peak at 100" (20,635, essentially flat 95-105")
# before ambiguity takes over past ~120" -- matches the 99" p90 almost
# exactly, so 100" captures the real offset population without yet
# overreaching into denser fields.
#
# Investigated and explicitly NOT added: gemini/NIFS has an even tighter,
# more persistent offset (median separation 180", 90th percentile only
# 187" -- essentially a fixed vector, stable 2005-2024) most likely because
# NIFS's tiny IFU sits off the acquisition camera's optical axis on the
# focal plane, and Gemini's alt-az mount rotates that fixed instrumental
# offset's RA/Dec projection with parallactic angle. But NIFS targets sit
# in far more crowded fields: simulated clean recoveries peak at just 80"
# (4,961) and are already declining by 100" -- by the radius needed to
# actually reach the real ~180" offset, ambiguous matches (11,929) outnumber
# clean ones (2,191) more than 5:1. Unlike chiron/FEROS, widening the radius
# here would create far more false ambiguity than it resolves.
INSTRUMENT_MATCH_RADIUS_OVERRIDES_ARCSEC: dict[tuple[str, str], float] = {
    ("noirlab", "chiron"): 60.0,
    ("eso", "FEROS"): 100.0,
}


def _normalize_name(name: str) -> str:
    key = re.sub(r"\s+", "", name).upper()
    if key.startswith("GL"):
        # "Gl" (Gliese) and "GJ" (Gliese-Jahreiss) are used interchangeably
        # for the same catalog in practice — e.g. CFHT's "Gl169.1A" vs
        # SIMBAD's "GJ 169.1 A" for the same star.
        key = "GJ" + key[2:]
    return key


def _load_candidate_stars(
    conn: psycopg.Connection, target_ra: list[float], target_dec: list[float], radius_deg: float
) -> list[tuple]:
    """Coarse spatial candidates via q3c's indexed radial join — replaces
    loading the entire tracked-star catalog into Python and rebuilding a
    KD-tree per observation epoch (see MAX_PM_ARCSEC_PER_YEAR for why
    radius_deg is a safe upper bound to use before propagation), which
    stopped scaling once the catalog passed ~1M rows: confirmed live, a
    single ESO page took over an hour, and even after an in-Python KD-tree
    pre-filter cut that to minutes, date-heavy archives like MAST still paid
    that cost once per distinct observation date in every page. q3c pushes
    the spatial filter into Postgres's own index instead of Python.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT s.star_id, s.ra, s.dec, s.ref_epoch, s.pmra, s.pmdec
            FROM stars s, unnest(%(target_ra)s::float8[], %(target_dec)s::float8[]) AS t(ra, dec)
            WHERE q3c_join(t.ra, t.dec, s.ra, s.dec, %(radius_deg)s)
            """,
            {"target_ra": target_ra, "target_dec": target_dec, "radius_deg": radius_deg},
        )
        return cur.fetchall()


def _load_star_aliases(conn: psycopg.Connection) -> tuple[dict[str, int], dict[int, tuple]]:
    """Normalized alias -> star_id, for identifier matching, plus each
    aliased star's own (ra, dec, ref_epoch, pmra, pmdec) for the name-match
    sanity check in match_records — no separate query needed since every
    star with an alias already comes back in this same row.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT star_id, name_aliases, ra, dec, ref_epoch, pmra, pmdec "
            "FROM stars WHERE name_aliases IS NOT NULL"
        )
        rows = cur.fetchall()
    lookup: dict[str, int] = {}
    positions: dict[int, tuple] = {}
    for star_id, aliases, ra, dec, ref_epoch, pmra, pmdec in rows:
        positions[star_id] = (ra, dec, ref_epoch, pmra, pmdec)
        for alias in aliases or []:
            lookup[_normalize_name(alias)] = star_id
    return lookup, positions


def _name_match_plausible(star_id: int, star_positions: dict[int, tuple], r: RawObservation) -> bool:
    """False if a name-matched record's own reported position sits farther
    than NAME_MATCH_SANITY_RADIUS_ARCSEC from the star its name resolved to
    — see module docstring's "Mira" case. True (trust the name match, as
    before this check existed) whenever there's no position to check against.
    """
    if r.ra is None or r.dec is None or r.obs_date is None or not (-90.0 <= r.dec <= 90.0):
        return True
    star_row = star_positions.get(star_id)
    if star_row is None:
        return True
    ra, dec, ref_epoch, pmra, pmdec = star_row
    _, propagated = _propagate([(star_id, ra, dec, ref_epoch, pmra, pmdec)], _to_jyear(r.obs_date))
    target = SkyCoord(ra=r.ra * u.deg, dec=r.dec * u.deg)
    return propagated[0].separation(target).arcsec <= NAME_MATCH_SANITY_RADIUS_ARCSEC


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


def _match_radius_arcsec(archive_code: str, instrument: str | None) -> float:
    return INSTRUMENT_MATCH_RADIUS_OVERRIDES_ARCSEC.get((archive_code, instrument), EASY_MATCH_RADIUS_ARCSEC)


def _upsert_holding(
    cur: psycopg.Cursor,
    archive_code: str,
    rec: RawObservation,
    star_id: int | None,
    match_method: str,
    match_status: str,
    theta_arcsec: float | None,
) -> None:
    cur.execute(
        """
        INSERT INTO spectroscopy_holdings
            (star_id, archive_code, archive_obs_id, archive_url, instrument,
             obs_date, program_id, match_method, match_status, theta_arcsec,
             raw_target_name, raw_ra, raw_dec, reduction_status, updated_at)
        VALUES (%(star_id)s, %(archive_code)s, %(archive_obs_id)s, %(archive_url)s,
                %(instrument)s, %(obs_date)s, %(program_id)s, %(match_method)s, %(match_status)s,
                %(theta_arcsec)s, %(raw_target_name)s, %(raw_ra)s, %(raw_dec)s,
                %(reduction_status)s, now())
        ON CONFLICT (archive_code, archive_obs_id) DO UPDATE SET
            star_id = EXCLUDED.star_id,
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
            reduction_status = EXCLUDED.reduction_status,
            updated_at = now()
        """,
        {
            "star_id": star_id,
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
            "reduction_status": rec.reduction_status or "unknown",
        },
    )


def match_records(conn: psycopg.Connection, archive_code: str, records: list[RawObservation]) -> dict:
    counts = {"direct_matched": 0, "name_matched": 0, "positional_matched": 0, "needs_review": 0, "skipped": 0}

    direct = [r for r in records if r.gaia_source_id is not None]
    no_gaia_column = [r for r in records if r.gaia_source_id is None]

    with conn.cursor() as cur:
        for r in direct:
            cur.execute("SELECT star_id FROM stars WHERE gaia_source_id = %s", (r.gaia_source_id,))
            row = cur.fetchone()
            if row is None:
                # Not a match failure on our end — the archive reports a
                # Gaia source_id that doesn't exist in Gaia DR3 itself (a
                # stale/incorrect ID on the archive's side), confirmed by
                # discover_stars already having tried and failed to add it
                # earlier in this same run. star_id must be NULL here (FK),
                # same as needs_review.
                _upsert_holding(cur, archive_code, r, None, "direct_gaia_column", "skipped", None)
                counts["skipped"] += 1
                continue
            _upsert_holding(cur, archive_code, r, row[0], "direct_gaia_column", "matched", None)
            counts["direct_matched"] += 1
    conn.commit()

    # Identifier match — tried before position, not just as a tiebreaker.
    # Still sanity-checked against the record's own reported position when
    # one is present (see _name_match_plausible / the "Mira" case in the
    # module docstring) — a name match that fails the check falls through to
    # positional matching below instead of being trusted blindly.
    alias_lookup, star_positions = _load_star_aliases(conn)
    positional = []
    # Records whose name match was rejected by the sanity check — tracked by
    # identity (not content) since a positional fallback that also comes up
    # empty must land in needs_review for these, not skipped (see module
    # docstring), while a record that never had a name match at all keeps
    # the ordinary skipped outcome.
    name_match_rejected: set[int] = set()
    with conn.cursor() as cur:
        for r in no_gaia_column:
            star_id = alias_lookup.get(_normalize_name(r.raw_target_name)) if r.raw_target_name else None
            if star_id is not None and _name_match_plausible(star_id, star_positions, r):
                _upsert_holding(cur, archive_code, r, star_id, "name_resolved", "matched", None)
                counts["name_matched"] += 1
            else:
                if star_id is not None:
                    name_match_rejected.add(id(r))
                positional.append(r)
    conn.commit()

    # dec must be a real latitude — clean_float only catches masked/None
    # values, not a *present-but-bogus* sentinel for "no real position."
    # Confirmed live: MAST reports -99.0 for calibration exposures lacking
    # real sky coordinates (undocumented, distinct from the masked-column
    # case clean_float handles), which crashed SkyCoord construction for
    # the whole epoch group outright rather than just that one record.
    no_position, has_position = [], []
    for r in positional:
        if r.ra is not None and r.dec is not None and r.obs_date is not None and -90.0 <= r.dec <= 90.0:
            has_position.append(r)
        else:
            no_position.append(r)
    positional = has_position
    if no_position:
        with conn.cursor() as cur:
            for r in no_position:
                _upsert_holding(cur, archive_code, r, None, "positional_easy_match", "skipped", None)
                counts["skipped"] += 1
        conn.commit()
    if not positional:
        return counts

    # Grouped by (epoch, radius) rather than just epoch -- almost always the
    # same as grouping by epoch alone, since every instrument shares
    # EASY_MATCH_RADIUS_ARCSEC by default, but instruments in
    # INSTRUMENT_MATCH_RADIUS_OVERRIDES_ARCSEC need their own wider search
    # both for the candidate-loading query and the final match radius.
    by_epoch = defaultdict(list)
    for r in positional:
        by_epoch[(_to_jyear(r.obs_date), _match_radius_arcsec(archive_code, r.instrument))].append(r)

    with conn.cursor() as cur:
        for (epoch, radius_arcsec), recs in by_epoch.items():
            targets = SkyCoord(ra=[r.ra for r in recs] * u.deg, dec=[r.dec for r in recs] * u.deg)

            max_years = abs(epoch - GAIA_DR3_REF_EPOCH)
            radius_deg = (radius_arcsec + MAX_PM_ARCSEC_PER_YEAR * max_years) / 3600.0
            candidate_rows = _load_candidate_stars(conn, [r.ra for r in recs], [r.dec for r in recs], radius_deg)
            if not candidate_rows:
                for r in recs:
                    status = "needs_review" if id(r) in name_match_rejected else "skipped"
                    _upsert_holding(cur, archive_code, r, None, "positional_easy_match", status, None)
                    counts[status] += 1
                continue

            ids, propagated = _propagate(candidate_rows, epoch)

            # search_around_sky's first return value indexes the *argument*
            # (propagated), the second indexes self (targets) — the reverse
            # of what the field names suggest. Verified empirically.
            idx_cat, idx_target, sep2d, _ = targets.search_around_sky(propagated, radius_arcsec * u.arcsec)
            candidates = defaultdict(list)
            for cat_i, target_i, sep in zip(idx_cat, idx_target, sep2d):
                candidates[target_i].append((ids[cat_i], sep.arcsec))

            for i, r in enumerate(recs):
                cands = candidates.get(i, [])
                if not cands:
                    status = "needs_review" if id(r) in name_match_rejected else "skipped"
                    _upsert_holding(cur, archive_code, r, None, "positional_easy_match", status, None)
                    counts[status] += 1
                elif len(cands) == 1:
                    star_id, theta = cands[0]
                    _upsert_holding(cur, archive_code, r, star_id, "positional_easy_match", "matched", float(theta))
                    counts["positional_matched"] += 1
                else:
                    best_theta = min(c[1] for c in cands)
                    _upsert_holding(cur, archive_code, r, None, "positional_easy_match", "needs_review", float(best_theta))
                    counts["needs_review"] += 1
    conn.commit()
    return counts
