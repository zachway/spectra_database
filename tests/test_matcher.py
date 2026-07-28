from datetime import date

import pytest
from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time

from sync import matcher
from sync.base import RawObservation


def _offset(ra, dec, position_angle_deg, sep_arcsec):
    base = SkyCoord(ra=ra * u.deg, dec=dec * u.deg)
    moved = base.directional_offset_by(position_angle_deg * u.deg, sep_arcsec * u.arcsec)
    return moved.ra.deg, moved.dec.deg


def _insert_star(cur, gaia_source_id, ra, dec, name_aliases=None):
    cur.execute(
        """
        INSERT INTO stars (gaia_source_id, ra, dec, ref_epoch, pmra, pmdec, name_aliases)
        VALUES (%s, %s, %s, 2016.0, 0, 0, %s)
        ON CONFLICT (gaia_source_id) DO UPDATE SET ra = EXCLUDED.ra, dec = EXCLUDED.dec,
                                                     name_aliases = EXCLUDED.name_aliases
        """,
        (gaia_source_id, ra, dec, name_aliases),
    )


def test_direct_gaia_column_match(conn):
    with conn.cursor() as cur:
        _insert_star(cur, 900000000000000001, 50.0, 20.0)
    conn.commit()

    rec = RawObservation(
        archive_obs_id="direct-1", archive_url="http://example.test/1", gaia_source_id=900000000000000001
    )
    counts = matcher.match_records(conn, "unit_test", [rec])
    assert counts["direct_matched"] == 1

    with conn.cursor() as cur:
        cur.execute(
            "SELECT match_status, match_method FROM spectroscopy_holdings "
            "WHERE archive_code='unit_test' AND archive_obs_id='direct-1'"
        )
        status, method = cur.fetchone()
    assert status == "matched"
    assert method == "direct_gaia_column"


# Every query below joins spectroscopy_holdings.star_id back to
# stars.gaia_source_id (LEFT JOIN, since a skipped/needs_review holding's
# star_id is NULL) so tests can keep asserting against the same literal
# Gaia-like test IDs _insert_star uses, rather than the internal surrogate
# star_id — see db/migrations/0001_star_id_surrogate_key.sql for why
# spectroscopy_holdings no longer stores gaia_source_id directly.


def test_direct_gaia_column_skips_untracked_star(conn):
    rec = RawObservation(
        archive_obs_id="direct-2", archive_url="http://example.test/2", gaia_source_id=999999999999999999
    )
    counts = matcher.match_records(conn, "unit_test", [rec])
    assert counts["skipped"] == 1

    # Persisted (not discarded) — gaia_source_id NULL (FK: we don't track
    # that id), raw report kept for later review/crowd-sourcing.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT s.gaia_source_id, h.match_status, h.match_method FROM spectroscopy_holdings h "
            "LEFT JOIN stars s ON s.star_id = h.star_id "
            "WHERE h.archive_code='unit_test' AND h.archive_obs_id='direct-2'"
        )
        gaia_id, status, method = cur.fetchone()
    assert gaia_id is None
    assert status == "skipped"
    assert method == "direct_gaia_column"


def test_positional_single_match(conn):
    with conn.cursor() as cur:
        _insert_star(cur, 900000000000000010, 100.0, -30.0)
    conn.commit()

    ra, dec = _offset(100.0, -30.0, 45.0, 0.3)  # 0.3" away — comfortably inside the radius
    rec = RawObservation(
        archive_obs_id="pos-1", archive_url="http://example.test/pos1",
        ra=ra, dec=dec, obs_date=date(2016, 1, 1),
    )
    counts = matcher.match_records(conn, "unit_test", [rec])
    assert counts["positional_matched"] == 1

    with conn.cursor() as cur:
        cur.execute(
            "SELECT s.gaia_source_id, h.match_status, h.theta_arcsec FROM spectroscopy_holdings h "
            "LEFT JOIN stars s ON s.star_id = h.star_id "
            "WHERE h.archive_code='unit_test' AND h.archive_obs_id='pos-1'"
        )
        gaia_id, status, theta = cur.fetchone()
    assert gaia_id == 900000000000000010
    assert status == "matched"
    assert theta == pytest.approx(0.3, abs=0.05)


def test_positional_ambiguous_needs_review(conn):
    with conn.cursor() as cur:
        _insert_star(cur, 900000000000000020, 200.0, 40.0)
        ra2, dec2 = _offset(200.0, 40.0, 90.0, 0.4)
        _insert_star(cur, 900000000000000021, ra2, dec2)
    conn.commit()

    # Sits within 1" of both stars above.
    ra, dec = _offset(200.0, 40.0, 90.0, 0.2)
    rec = RawObservation(
        archive_obs_id="pos-2", archive_url="http://example.test/pos2",
        ra=ra, dec=dec, obs_date=date(2016, 1, 1),
    )
    counts = matcher.match_records(conn, "unit_test", [rec])
    assert counts["needs_review"] == 1

    with conn.cursor() as cur:
        cur.execute(
            "SELECT s.gaia_source_id, h.match_status FROM spectroscopy_holdings h "
            "LEFT JOIN stars s ON s.star_id = h.star_id "
            "WHERE h.archive_code='unit_test' AND h.archive_obs_id='pos-2'"
        )
        gaia_id, status = cur.fetchone()
    assert gaia_id is None
    assert status == "needs_review"


def test_positional_no_candidate_skipped(conn):
    rec = RawObservation(
        archive_obs_id="pos-3", archive_url="http://example.test/pos3",
        ra=10.0, dec=10.0, obs_date=date(2016, 1, 1),
    )
    counts = matcher.match_records(conn, "unit_test", [rec])
    assert counts["skipped"] >= 1

    # Persisted with the raw reported position, not discarded.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT s.gaia_source_id, h.match_status, h.raw_ra, h.raw_dec FROM spectroscopy_holdings h "
            "LEFT JOIN stars s ON s.star_id = h.star_id "
            "WHERE h.archive_code='unit_test' AND h.archive_obs_id='pos-3'"
        )
        gaia_id, status, raw_ra, raw_dec = cur.fetchone()
    assert gaia_id is None
    assert status == "skipped"
    assert raw_ra == 10.0
    assert raw_dec == 10.0


def test_idempotent_rerun(conn):
    with conn.cursor() as cur:
        _insert_star(cur, 900000000000000030, 300.0, -10.0)
    conn.commit()

    rec = RawObservation(
        archive_obs_id="idem-1", archive_url="http://example.test/idem1", gaia_source_id=900000000000000030
    )
    matcher.match_records(conn, "unit_test", [rec])
    matcher.match_records(conn, "unit_test", [rec])

    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM spectroscopy_holdings WHERE archive_code='unit_test' AND archive_obs_id='idem-1'"
        )
        assert cur.fetchone()[0] == 1


def test_name_resolved_beats_missing_positional_match(conn):
    """Identifier match must succeed even when the record's position is far
    enough off that positional matching alone would skip it — the whole
    point of trying identifier first (e.g. Gaia's astrometric fit can be
    biased for binaries, breaking positional matching even with correct PM).
    """
    with conn.cursor() as cur:
        _insert_star(cur, 900000000000000040, 40.0, 40.0, name_aliases=["GJ 169.1 A", "NAME Stein 2051"])
    conn.commit()

    rec = RawObservation(
        archive_obs_id="name-1", archive_url="http://example.test/name1",
        ra=40.01, dec=40.01, obs_date=date(2016, 1, 1),  # ~50" off — well outside the 1" radius
        raw_target_name="Gl169.1A",  # "Gl" vs "GJ" — must normalize to match
    )
    counts = matcher.match_records(conn, "unit_test", [rec])
    assert counts["name_matched"] == 1
    assert counts["positional_matched"] == 0

    with conn.cursor() as cur:
        cur.execute(
            "SELECT s.gaia_source_id, h.match_method, h.match_status, h.theta_arcsec FROM spectroscopy_holdings h "
            "LEFT JOIN stars s ON s.star_id = h.star_id "
            "WHERE h.archive_code='unit_test' AND h.archive_obs_id='name-1'"
        )
        gaia_id, method, status, theta = cur.fetchone()
    assert gaia_id == 900000000000000040
    assert method == "name_resolved"
    assert status == "matched"
    assert theta is None


def test_name_resolution_falls_back_to_positional_when_no_alias_hit(conn):
    with conn.cursor() as cur:
        _insert_star(cur, 900000000000000050, 60.0, -20.0, name_aliases=["GJ 999"])
    conn.commit()

    ra, dec = _offset(60.0, -20.0, 0.0, 0.3)
    rec = RawObservation(
        archive_obs_id="name-2", archive_url="http://example.test/name2",
        ra=ra, dec=dec, obs_date=date(2016, 1, 1),
        raw_target_name="Some Other Name",
    )
    counts = matcher.match_records(conn, "unit_test", [rec])
    assert counts["name_matched"] == 0
    assert counts["positional_matched"] == 1


def test_name_match_rejected_when_position_is_far_off(conn):
    """The "Mira" case: "Mira" is SIMBAD's own proper name for omicron Ceti
    *and* an informal class label for any Mira-type long-period variable. A
    record whose raw_target_name matches a tracked star's alias but whose
    own reported position is nowhere near that star (i.e. it's actually some
    other physical star sharing the same informal name) must not be force-
    matched onto it — it should fall through to positional matching instead.
    With nothing else tracked nearby, it lands in needs_review rather than
    skipped: confirmed live that a rejected name match is often actually
    correct (the archive's own logged position for one exposure was just
    wrong, not a different star), so it's worth a human's attention rather
    than being silently dropped with no gaia_source_id at all.
    """
    with conn.cursor() as cur:
        _insert_star(cur, 900000000000000080, 4.9, -3.0, name_aliases=["Mira", "omi Cet"])
    conn.commit()

    rec = RawObservation(
        archive_obs_id="mira-1", archive_url="http://example.test/mira1",
        ra=200.0, dec=50.0, obs_date=date(2016, 1, 1),  # far across the sky from omicron Ceti
        raw_target_name="Mira",
    )
    counts = matcher.match_records(conn, "unit_test", [rec])
    assert counts["name_matched"] == 0
    assert counts["skipped"] == 0
    assert counts["needs_review"] == 1

    with conn.cursor() as cur:
        cur.execute(
            "SELECT s.gaia_source_id, h.match_status FROM spectroscopy_holdings h "
            "LEFT JOIN stars s ON s.star_id = h.star_id "
            "WHERE h.archive_code='unit_test' AND h.archive_obs_id='mira-1'"
        )
        gaia_id, status = cur.fetchone()
    assert gaia_id is None
    assert status == "needs_review"


def test_name_match_rejected_still_skipped_if_zero_candidates_and_no_name_at_all(conn):
    """Distinguishes the two "nothing nearby" outcomes: a record with no name
    match at all (never had a rejected candidate to flag) still gets the
    ordinary skipped status, not needs_review — needs_review is specifically
    for a rejected *name* match, not every position-only miss.
    """
    rec = RawObservation(
        archive_obs_id="no-name-1", archive_url="http://example.test/noname1",
        ra=10.0, dec=10.0, obs_date=date(2016, 1, 1),
        raw_target_name="Some Unrelated Name",
    )
    counts = matcher.match_records(conn, "unit_test", [rec])
    assert counts["skipped"] == 1
    assert counts["needs_review"] == 0


def test_name_match_accepted_when_position_is_close(conn):
    """Same alias collision risk as above, but this time the record's own
    position genuinely is close to the named star — the sanity check must
    not reject a real match.
    """
    with conn.cursor() as cur:
        _insert_star(cur, 900000000000000081, 4.9, -3.0, name_aliases=["Mira", "omi Cet"])
    conn.commit()

    ra, dec = _offset(4.9, -3.0, 0.0, 5.0)  # 5" away — a real observation of the real star
    rec = RawObservation(
        archive_obs_id="mira-2", archive_url="http://example.test/mira2",
        ra=ra, dec=dec, obs_date=date(2016, 1, 1),
        raw_target_name="Mira",
    )
    counts = matcher.match_records(conn, "unit_test", [rec])
    assert counts["name_matched"] == 1

    with conn.cursor() as cur:
        cur.execute(
            "SELECT s.gaia_source_id, h.match_status, h.match_method FROM spectroscopy_holdings h "
            "LEFT JOIN stars s ON s.star_id = h.star_id "
            "WHERE h.archive_code='unit_test' AND h.archive_obs_id='mira-2'"
        )
        gaia_id, status, method = cur.fetchone()
    assert gaia_id == 900000000000000081
    assert status == "matched"
    assert method == "name_resolved"


def test_positional_match_survives_prefilter_for_fast_proper_motion(conn):
    """A star with real (but sub-Barnard's-Star) proper motion should still
    match even though its un-propagated position is well outside the tight
    1" match radius by the observation epoch — the coarse pre-filter's
    safety margin (MAX_PM_ARCSEC_PER_YEAR) must be generous enough not to
    exclude it before propagation ever runs.
    """
    ra0, dec0 = 150.0, 10.0
    pm_ra_cosdec, pm_dec = 8000.0, 0.0  # mas/yr = 8"/yr, under the 10.3"/yr safety bound

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO stars (gaia_source_id, ra, dec, ref_epoch, pmra, pmdec) VALUES (%s, %s, %s, 2016.0, %s, %s)",
            (900000000000000060, ra0, dec0, pm_ra_cosdec, pm_dec),
        )
    conn.commit()

    obs_date = date(2020, 1, 1)
    obs_jyear = matcher._to_jyear(obs_date)

    # Ground truth: where astropy itself says this star actually is by obs_jyear.
    base = SkyCoord(
        ra=ra0 * u.deg, dec=dec0 * u.deg,
        pm_ra_cosdec=pm_ra_cosdec * u.mas / u.yr, pm_dec=pm_dec * u.mas / u.yr,
        obstime=Time(2016.0, format="jyear"), frame="icrs",
    )
    true_position = base.apply_space_motion(new_obstime=Time(obs_jyear, format="jyear"))

    # Sanity check this scenario actually exercises the pre-filter: the
    # un-propagated position must be well outside the match radius.
    raw_sep = SkyCoord(ra=ra0 * u.deg, dec=dec0 * u.deg).separation(true_position).arcsec
    assert raw_sep > matcher.EASY_MATCH_RADIUS_ARCSEC * 5

    rec = RawObservation(
        archive_obs_id="pm-1", archive_url="http://example.test/pm1",
        ra=true_position.ra.deg, dec=true_position.dec.deg,
        obs_date=obs_date,
    )
    counts = matcher.match_records(conn, "unit_test", [rec])
    assert counts["positional_matched"] == 1

    with conn.cursor() as cur:
        cur.execute(
            "SELECT s.gaia_source_id FROM spectroscopy_holdings h "
            "LEFT JOIN stars s ON s.star_id = h.star_id "
            "WHERE h.archive_code='unit_test' AND h.archive_obs_id='pm-1'"
        )
        gaia_id = cur.fetchone()[0]
    assert gaia_id == 900000000000000060


def test_instrument_radius_override_recovers_offset_beyond_default_radius(conn):
    """noirlab/chiron carries a confirmed real pointing offset well beyond
    EASY_MATCH_RADIUS_ARCSEC (see INSTRUMENT_MATCH_RADIUS_OVERRIDES_ARCSEC's
    docstring) -- a record whose raw position is off by more than 1" but
    still within its override radius must match, using archive_code=noirlab,
    instrument=chiron specifically (not just any archive/instrument).
    """
    with conn.cursor() as cur:
        _insert_star(cur, 900000000000000090, 120.0, -15.0)
    conn.commit()

    assert matcher.INSTRUMENT_MATCH_RADIUS_OVERRIDES_ARCSEC[("noirlab", "chiron")] > 30.0
    ra, dec = _offset(120.0, -15.0, 45.0, 30.0)  # well outside 1", inside the chiron override
    rec = RawObservation(
        archive_obs_id="chiron-1", archive_url="http://example.test/chiron1",
        instrument="chiron", ra=ra, dec=dec, obs_date=date(2016, 1, 1),
    )
    counts = matcher.match_records(conn, "noirlab", [rec])
    assert counts["positional_matched"] == 1

    with conn.cursor() as cur:
        cur.execute(
            "SELECT s.gaia_source_id, h.match_status FROM spectroscopy_holdings h "
            "LEFT JOIN stars s ON s.star_id = h.star_id "
            "WHERE h.archive_code='noirlab' AND h.archive_obs_id='chiron-1'"
        )
        gaia_id, status = cur.fetchone()
    assert gaia_id == 900000000000000090
    assert status == "matched"


def test_instrument_radius_override_recovers_offset_for_eso_feros(conn):
    """eso/FEROS carries its own confirmed pointing offset (see
    INSTRUMENT_MATCH_RADIUS_OVERRIDES_ARCSEC's docstring) -- distinct
    magnitude and direction from chiron's, but the same override mechanism
    keyed on (archive_code, instrument) must apply to it too.
    """
    with conn.cursor() as cur:
        _insert_star(cur, 900000000000000092, 140.0, -35.0)
    conn.commit()

    assert matcher.INSTRUMENT_MATCH_RADIUS_OVERRIDES_ARCSEC[("eso", "FEROS")] > 30.0
    ra, dec = _offset(140.0, -35.0, 200.0, 80.0)  # well outside 1", inside the FEROS override
    rec = RawObservation(
        archive_obs_id="feros-1", archive_url="http://example.test/feros1",
        instrument="FEROS", ra=ra, dec=dec, obs_date=date(2016, 1, 1),
    )
    counts = matcher.match_records(conn, "eso", [rec])
    assert counts["positional_matched"] == 1

    with conn.cursor() as cur:
        cur.execute(
            "SELECT s.gaia_source_id, h.match_status FROM spectroscopy_holdings h "
            "LEFT JOIN stars s ON s.star_id = h.star_id "
            "WHERE h.archive_code='eso' AND h.archive_obs_id='feros-1'"
        )
        gaia_id, status = cur.fetchone()
    assert gaia_id == 900000000000000092
    assert status == "matched"


def test_instrument_radius_override_does_not_apply_to_other_instruments(conn):
    """The same 30" offset on a plain noirlab instrument (not chiron) must
    stay outside EASY_MATCH_RADIUS_ARCSEC and skip, same as any other
    archive -- the override is keyed on (archive_code, instrument), not just
    archive_code.
    """
    with conn.cursor() as cur:
        _insert_star(cur, 900000000000000091, 130.0, -25.0)
    conn.commit()

    ra, dec = _offset(130.0, -25.0, 45.0, 30.0)
    rec = RawObservation(
        archive_obs_id="goodman-1", archive_url="http://example.test/goodman1",
        instrument="goodman", ra=ra, dec=dec, obs_date=date(2016, 1, 1),
    )
    counts = matcher.match_records(conn, "noirlab", [rec])
    assert counts["skipped"] == 1
    assert counts["positional_matched"] == 0


def test_bogus_sentinel_dec_does_not_crash(conn):
    """Confirmed live: MAST reports dec=-99.0 (not masked/None, a genuine
    present-but-physically-invalid sentinel) for calibration exposures
    lacking real sky coordinates — clean_float doesn't catch this since
    it's not a masked value, and an un-filtered -99 crashes SkyCoord
    construction (dec must be in [-90, 90]) for the *whole* epoch group,
    not just that one record. A record sharing the epoch with a bogus one
    must still match normally.
    """
    with conn.cursor() as cur:
        _insert_star(cur, 900000000000000070, 70.0, -20.0)
    conn.commit()

    ra, dec = _offset(70.0, -20.0, 0.0, 0.3)
    good_rec = RawObservation(
        archive_obs_id="sentinel-1", archive_url="http://example.test/sentinel1",
        ra=ra, dec=dec, obs_date=date(2016, 1, 1),
    )
    bogus_rec = RawObservation(
        archive_obs_id="sentinel-2", archive_url="http://example.test/sentinel2",
        ra=123.456, dec=-99.0, obs_date=date(2016, 1, 1),  # same epoch as good_rec
    )
    counts = matcher.match_records(conn, "unit_test", [good_rec, bogus_rec])
    assert counts["positional_matched"] == 1

    # Persisted as skipped (raw bogus dec kept as-is for review), not
    # silently dropped — the crash-prevention is about not corrupting the
    # rest of the epoch group's matching, not about hiding the bad record.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT s.gaia_source_id, h.match_status FROM spectroscopy_holdings h "
            "LEFT JOIN stars s ON s.star_id = h.star_id "
            "WHERE h.archive_code='unit_test' AND h.archive_obs_id='sentinel-2'"
        )
        gaia_id, status = cur.fetchone()
    assert gaia_id is None
    assert status == "skipped"
