from datetime import date

import pytest
from astropy import units as u
from astropy.coordinates import SkyCoord

from sync import matcher
from sync.base import RawObservation


def _offset(ra, dec, position_angle_deg, sep_arcsec):
    base = SkyCoord(ra=ra * u.deg, dec=dec * u.deg)
    moved = base.directional_offset_by(position_angle_deg * u.deg, sep_arcsec * u.arcsec)
    return moved.ra.deg, moved.dec.deg


def _insert_star(cur, gaia_source_id, ra, dec):
    cur.execute(
        """
        INSERT INTO stars (gaia_source_id, ra, dec, ref_epoch, pmra, pmdec)
        VALUES (%s, %s, %s, 2016.0, 0, 0)
        ON CONFLICT (gaia_source_id) DO UPDATE SET ra = EXCLUDED.ra, dec = EXCLUDED.dec
        """,
        (gaia_source_id, ra, dec),
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


def test_direct_gaia_column_skips_untracked_star(conn):
    rec = RawObservation(
        archive_obs_id="direct-2", archive_url="http://example.test/2", gaia_source_id=999999999999999999
    )
    counts = matcher.match_records(conn, "unit_test", [rec])
    assert counts["skipped"] == 1

    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM spectroscopy_holdings WHERE archive_code='unit_test' AND archive_obs_id='direct-2'"
        )
        assert cur.fetchone() is None


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
            "SELECT gaia_source_id, match_status, theta_arcsec FROM spectroscopy_holdings "
            "WHERE archive_code='unit_test' AND archive_obs_id='pos-1'"
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
            "SELECT gaia_source_id, match_status FROM spectroscopy_holdings "
            "WHERE archive_code='unit_test' AND archive_obs_id='pos-2'"
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

    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM spectroscopy_holdings WHERE archive_code='unit_test' AND archive_obs_id='pos-3'"
        )
        assert cur.fetchone() is None


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
