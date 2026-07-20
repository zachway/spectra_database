"""Register a star (by Gaia DR3 source_id) into the tracking database.

Idempotent: re-running on a source_id already present just refreshes its
astrometry. Also seeds the Gaia RVS holding in the same call, since RVS
availability (has_rvs) comes back on the same gaia_source row — no separate
archive sync needed for that one.
"""

import argparse
import os

import psycopg
from astroquery.gaia import Gaia
from astroquery.simbad import Simbad

GAIA_QUERY = """
SELECT source_id, ra, dec, ref_epoch, pmra, pmdec, parallax,
       phot_g_mean_mag, has_rvs, has_xp_continuous
FROM gaiadr3.gaia_source
WHERE source_id = {source_id}
"""

GAIA_CONE_QUERY = """
SELECT source_id
FROM gaiadr3.gaia_source
WHERE 1=CONTAINS(
    POINT('ICRS', ra, dec),
    CIRCLE('ICRS', {ra}, {dec}, {radius_deg})
)
"""

# Gaia archive Datalink endpoint for retrieving an individual source's RVS spectrum.
RVS_DEEP_LINK = (
    "https://gea.esac.esa.int/data-server/data"
    "?RETRIEVAL_TYPE=RVS&ID=Gaia+DR3+{source_id}&DATA_STRUCTURE=INDIVIDUAL"
)


def resolve_gaia_source_id(name: str, cone_radius_arcsec: float = 2.0) -> int:
    """Resolve a star name to a Gaia DR3 source_id via SIMBAD.

    Prefers SIMBAD's own Gaia DR3 cross-match id (present for the large
    majority of resolvable objects). Falls back to a tight-radius Gaia cone
    search around the SIMBAD position only when SIMBAD doesn't carry one, and
    raises if that fallback is itself ambiguous — same easy-match-or-defer
    rule used for archive cross-matching, applied here at ingestion time.
    """
    simbad = Simbad()
    simbad.add_votable_fields("ids")
    result = simbad.query_object(name)
    if result is None or len(result) == 0:
        raise ValueError(f"could not resolve {name!r} via SIMBAD")

    gaia_tokens = [tok for tok in result["ids"][0].split("|") if tok.startswith("Gaia DR3 ")]
    if gaia_tokens:
        return int(gaia_tokens[0].removeprefix("Gaia DR3 "))

    ra, dec = float(result["ra"][0]), float(result["dec"][0])
    job = Gaia.launch_job(GAIA_CONE_QUERY.format(ra=ra, dec=dec, radius_deg=cone_radius_arcsec / 3600))
    table = job.get_results()
    if len(table) == 0:
        raise ValueError(
            f"{name!r} resolved via SIMBAD to ({ra}, {dec}) but no Gaia DR3 source "
            f"found within {cone_radius_arcsec}\""
        )
    if len(table) > 1:
        raise ValueError(
            f"{name!r} resolved via SIMBAD to ({ra}, {dec}) but {len(table)} Gaia DR3 "
            f"sources found within {cone_radius_arcsec}\" — needs manual resolution"
        )
    return int(table[0]["source_id"])


def fetch_name_aliases(gaia_source_id: int) -> list[str]:
    """All of SIMBAD's known aliases for this star, for identifier-matching an
    archive's own target_name against a tracked star — the primary match path,
    with positional matching as fallback (see sync.matcher). Empty list if
    SIMBAD doesn't carry this source at all.
    """
    simbad = Simbad()
    simbad.add_votable_fields("ids")
    result = simbad.query_object(f"Gaia DR3 {gaia_source_id}")
    if result is None or len(result) == 0 or result["ids"][0] is None:
        return []
    return [tok.strip() for tok in str(result["ids"][0]).split("|")]


def fetch_gaia_row(gaia_source_id: int) -> dict:
    job = Gaia.launch_job(GAIA_QUERY.format(source_id=gaia_source_id))
    table = job.get_results()
    if len(table) == 0:
        raise ValueError(f"Gaia source_id {gaia_source_id} not found in gaiadr3.gaia_source")
    row = table[0]
    return {
        "gaia_source_id": int(row["source_id"]),
        "ra": float(row["ra"]),
        "dec": float(row["dec"]),
        "ref_epoch": float(row["ref_epoch"]),
        "pmra": float(row["pmra"]) if row["pmra"] is not None else None,
        "pmdec": float(row["pmdec"]) if row["pmdec"] is not None else None,
        "parallax": float(row["parallax"]) if row["parallax"] is not None else None,
        "phot_g_mean_mag": float(row["phot_g_mean_mag"]) if row["phot_g_mean_mag"] is not None else None,
        "has_rvs": bool(row["has_rvs"]),
        "has_xp_continuous": bool(row["has_xp_continuous"]),
    }


def add_star(conn: psycopg.Connection, gaia_source_id: int, input_name: str | None = None) -> dict:
    star = fetch_gaia_row(gaia_source_id)
    star["input_name"] = input_name
    star["name_aliases"] = fetch_name_aliases(gaia_source_id)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stars (gaia_source_id, ra, dec, ref_epoch, pmra, pmdec,
                                parallax, phot_g_mean_mag, has_gaia_rvs, has_xp_continuous,
                                input_name, name_aliases)
            VALUES (%(gaia_source_id)s, %(ra)s, %(dec)s, %(ref_epoch)s, %(pmra)s,
                    %(pmdec)s, %(parallax)s, %(phot_g_mean_mag)s, %(has_rvs)s, %(has_xp_continuous)s,
                    %(input_name)s, %(name_aliases)s)
            ON CONFLICT (gaia_source_id) DO UPDATE SET
                ra = EXCLUDED.ra,
                dec = EXCLUDED.dec,
                ref_epoch = EXCLUDED.ref_epoch,
                pmra = EXCLUDED.pmra,
                pmdec = EXCLUDED.pmdec,
                parallax = EXCLUDED.parallax,
                phot_g_mean_mag = EXCLUDED.phot_g_mean_mag,
                has_gaia_rvs = EXCLUDED.has_gaia_rvs,
                has_xp_continuous = EXCLUDED.has_xp_continuous,
                input_name = COALESCE(EXCLUDED.input_name, stars.input_name),
                name_aliases = EXCLUDED.name_aliases
            """,
            star,
        )

        if star["has_rvs"]:
            cur.execute(
                """
                INSERT INTO spectroscopy_holdings
                    (gaia_source_id, archive_code, archive_obs_id, archive_url,
                     instrument, match_method, match_status)
                VALUES (%(gaia_source_id)s, 'gaia_rvs', %(archive_obs_id)s, %(archive_url)s,
                        'Gaia RVS', 'direct_gaia_column', 'matched')
                ON CONFLICT (archive_code, archive_obs_id) DO NOTHING
                """,
                {
                    "gaia_source_id": star["gaia_source_id"],
                    "archive_obs_id": str(star["gaia_source_id"]),
                    "archive_url": RVS_DEEP_LINK.format(source_id=star["gaia_source_id"]),
                },
            )

    conn.commit()
    return star


def add_star_by_name(conn: psycopg.Connection, name: str) -> dict:
    gaia_source_id = resolve_gaia_source_id(name)
    return add_star(conn, gaia_source_id, input_name=name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", help="Gaia DR3 source_id, or a star name to resolve via SIMBAD")
    args = parser.parse_args()

    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        if args.target.isdigit():
            star = add_star(conn, int(args.target))
        else:
            star = add_star_by_name(conn, args.target)
    print(star)


if __name__ == "__main__":
    main()
