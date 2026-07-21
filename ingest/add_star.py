"""Register a star (by Gaia DR3 source_id) into the tracking database.

Idempotent: re-running on a source_id already present just refreshes its
astrometry. Also seeds the Gaia RVS holding in the same call, since RVS
availability (has_rvs) comes back on the same gaia_source row — no separate
archive sync needed for that one.
"""

from __future__ import annotations

import argparse
import logging
import os

import psycopg
from astroquery.gaia import Gaia
from astroquery.simbad import Simbad

from sync.base import RawObservation, clean_float

logger = logging.getLogger(__name__)

GAIA_QUERY = """
SELECT source_id, ra, dec, ref_epoch, pmra, pmdec, parallax,
       phot_g_mean_mag, has_rvs, has_xp_continuous
FROM gaiadr3.gaia_source
WHERE source_id = {source_id}
"""

GAIA_BATCH_QUERY = """
SELECT source_id, ra, dec, ref_epoch, pmra, pmdec, parallax,
       phot_g_mean_mag, has_rvs, has_xp_continuous
FROM gaiadr3.gaia_source
WHERE source_id IN ({id_list})
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
        "pmra": clean_float(row["pmra"]),
        "pmdec": clean_float(row["pmdec"]),
        "parallax": clean_float(row["parallax"]),
        "phot_g_mean_mag": clean_float(row["phot_g_mean_mag"]),
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


BATCH_CHUNK_SIZE = 500


def add_stars_batch(
    conn: psycopg.Connection,
    gaia_source_ids: list[int],
    known_aliases: dict[int, list[str]] | None = None,
) -> int:
    """Add many stars in a handful of batched Gaia TAP queries instead of one
    call per star — add_star() itself is a live TAP round trip each time,
    which doesn't scale past a few dozen stars. Used for bulk-seeding a local
    test dataset directly from archive query results.

    Does NOT fetch SIMBAD's full alias list per star (that's one more live
    call each — fine for a single add_star(), not for thousands at once).
    known_aliases lets a caller who already resolved a star by name (e.g. via
    resolve_stellar_gaia_ids_batch) pass that specific name through so it
    gets cached — cheap, no extra API call, and it's what lets
    sync.matcher's name-priority-over-position path actually apply to these
    stars instead of falling through to a positional check that can
    spuriously fail (archive-reported coordinates are sometimes from a
    different physical instrument — e.g. the finder/acquisition camera —
    and can be off by arcminutes even when the name is correct). Merges with
    any aliases already cached rather than overwriting. Returns the number
    of stars actually inserted/updated.
    """
    unique_ids = sorted(set(gaia_source_ids))
    if not unique_ids:
        return 0
    known_aliases = known_aliases or {}

    total = 0
    for i in range(0, len(unique_ids), BATCH_CHUNK_SIZE):
        chunk = unique_ids[i : i + BATCH_CHUNK_SIZE]
        id_list = ",".join(str(sid) for sid in chunk)
        job = Gaia.launch_job(GAIA_BATCH_QUERY.format(id_list=id_list))
        table = job.get_results()

        with conn.cursor() as cur:
            for row in table:
                gaia_source_id = int(row["source_id"])
                star = {
                    "gaia_source_id": gaia_source_id,
                    "ra": float(row["ra"]),
                    "dec": float(row["dec"]),
                    "ref_epoch": float(row["ref_epoch"]),
                    "pmra": clean_float(row["pmra"]),
                    "pmdec": clean_float(row["pmdec"]),
                    "parallax": clean_float(row["parallax"]),
                    "phot_g_mean_mag": clean_float(row["phot_g_mean_mag"]),
                    "has_rvs": bool(row["has_rvs"]),
                    "has_xp_continuous": bool(row["has_xp_continuous"]),
                    "input_name": None,
                    "name_aliases": known_aliases.get(gaia_source_id) or None,
                }
                cur.execute(
                    """
                    INSERT INTO stars (gaia_source_id, ra, dec, ref_epoch, pmra, pmdec,
                                        parallax, phot_g_mean_mag, has_gaia_rvs, has_xp_continuous,
                                        input_name, name_aliases)
                    VALUES (%(gaia_source_id)s, %(ra)s, %(dec)s, %(ref_epoch)s, %(pmra)s,
                            %(pmdec)s, %(parallax)s, %(phot_g_mean_mag)s, %(has_rvs)s, %(has_xp_continuous)s,
                            %(input_name)s, %(name_aliases)s)
                    ON CONFLICT (gaia_source_id) DO UPDATE SET
                        name_aliases = ARRAY(
                            SELECT DISTINCT UNNEST(
                                COALESCE(stars.name_aliases, ARRAY[]::TEXT[])
                                || COALESCE(EXCLUDED.name_aliases, ARRAY[]::TEXT[])
                            )
                        )
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
                total += 1
        conn.commit()

    return total


SIMBAD_BATCH_CHUNK_SIZE = 300


def resolve_stellar_gaia_ids_batch(names: list[str]) -> dict[str, int]:
    """Batch-resolve names to Gaia DR3 source_ids, keeping only SIMBAD-confirmed
    stars: SIMBAD's object-type codes for stars all end in '*' (e.g. '*',
    'PM*', 'WD*', 'SB*'), while non-stellar types don't ('AGN', 'G', 'OpC',
    'BLL', ...) — live-verified against Proxima Centauri, M31, 3C 273,
    Sirius B, TRAPPIST-1, the Pleiades, and NGC 1.

    Used when bulk-seeding tracked stars from an archive's raw target_name
    field, where a full resolve_gaia_source_id() per name (with its
    cone-search fallback) would be too slow at this volume — SIMBAD-or-
    nothing here, no fallback.
    """
    unique_names = sorted({n for n in names if n})
    if not unique_names:
        return {}

    resolved: dict[str, int] = {}
    for i in range(0, len(unique_names), SIMBAD_BATCH_CHUNK_SIZE):
        chunk = unique_names[i : i + SIMBAD_BATCH_CHUNK_SIZE]
        simbad = Simbad()
        simbad.add_votable_fields("ids", "otype")
        result = simbad.query_objects(chunk)
        if result is None:
            continue
        for row in result:
            otype = row["otype"]
            if otype is None or not str(otype).strip().endswith("*"):
                continue
            ids_field = row["ids"]
            if ids_field is None:
                continue
            gaia_tokens = [tok for tok in str(ids_field).split("|") if tok.startswith("Gaia DR3 ")]
            if not gaia_tokens:
                continue
            queried_name = str(row["user_specified_id"]).strip()
            resolved[queried_name] = int(gaia_tokens[0].removeprefix("Gaia DR3 "))
    return resolved


def discover_stars(conn: psycopg.Connection, archive_code: str, records: list[RawObservation]) -> int:
    """Track any new stars a batch of archive records reveals, before matching.

    Same discovery rule for every archive: a record with its own Gaia
    source_id is trusted outright (Gaia's own catalog is the "is this real"
    check already); a record with only a raw_target_name gets that name
    batch-resolved via SIMBAD and kept only if SIMBAD calls it stellar (see
    resolve_stellar_gaia_ids_batch). Records with neither are left for
    sync.matcher's positional fallback against whatever's already tracked.

    Shared between sync.runner (incremental production syncs) and
    scripts.seed_small_test_data (one-off bulk seeding) so the two can't
    silently diverge in what counts as a new star.
    """
    known_aliases: dict[int, list[str]] = {}

    direct = [r for r in records if r.gaia_source_id is not None]
    for r in direct:
        if r.raw_target_name:
            known_aliases.setdefault(r.gaia_source_id, []).append(r.raw_target_name)

    unnamed = [r for r in records if r.gaia_source_id is None]
    names = [r.raw_target_name for r in unnamed if r.raw_target_name]
    name_to_gaia: dict[str, int] = {}
    if names:
        try:
            name_to_gaia = resolve_stellar_gaia_ids_batch(names)
        except Exception:
            # SIMBAD outages happen (confirmed live during this project) —
            # degrade to direct-Gaia-only + positional matching against
            # whatever's already tracked, rather than losing the whole
            # sync page to one dependency being briefly down.
            logger.warning("%s: SIMBAD resolution failed during star discovery, continuing without it", archive_code, exc_info=True)
    for name, gaia_id in name_to_gaia.items():
        known_aliases.setdefault(gaia_id, []).append(name)

    all_ids = [r.gaia_source_id for r in direct] + list(name_to_gaia.values())
    return add_stars_batch(conn, all_ids, known_aliases=known_aliases)


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
