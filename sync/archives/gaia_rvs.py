"""Gaia RVS — radial-velocity spectra, native Gaia TAP, source_id-watermark
pagination.

Independent discovery archive, not a walk over stars this project already
tracks: queries gaiadr3.gaia_source directly via Gaia's own TAP service for
every source with has_rvs='true'. Confirmed live (2026-08-04) that
`SELECT COUNT(*) ... WHERE has_rvs = 'true'` returns exactly 999,645 — the
same figure published at https://www.cosmos.esa.int/web/gaia/dr3 as the DR3
mean-RVS-spectra release total. discover_stars registers any source_id not
already tracked via some other archive, the same as any other archive's
direct_gaia_column records.

Filtering has_rvs='true' before the ORDER BY/TOP keeps this fast (confirmed
live: 50,000-row page in ~5s) even though gaiadr3.gaia_source itself is
~1.8B rows — this isn't the unbounded-ORDER-BY cliff that bit mast_jwst.py,
because the qualifying set is ~1M rows, not the full table.

Paginated by a source_id watermark rather than by date/mjd: DR3 is a single
fixed release (not a live-growing feed), and source_id is a unique integer
under ORDER BY, so (unlike a t_min/mjd watermark shared by many rows at a
page boundary, see eso.py) a plain max-seen-value watermark can't skip or
duplicate rows at the boundary.

Old cursor shape from before this was a real archive module (an
after_star_id watermark over this project's own `stars` table) is silently
ignored by cursor.get's default here — the first run under this module
starts over from source_id 0 and re-walks the full ~1M RVS sources. That's
intentional: this project only had 734,664 of them tracked before (only
those also seen by some other archive), so a full re-walk is needed to pick
up the remaining ~265k RVS stars this project never had a reason to track
before. Already-tracked stars are cheap to re-see — add_stars_batch skips
the Gaia round trip for any gaia_source_id already in `stars`, and the
matcher's ON CONFLICT DO NOTHING makes re-matching an already-held spectrum
a no-op.
"""

from sync.base import RawObservation, make_tap_service

TAP_URL = "https://gea.esac.esa.int/tap-server/tap"

QUERY = """
SELECT TOP {page_size} source_id
FROM gaiadr3.gaia_source
WHERE has_rvs = 'true' AND source_id > {last_source_id}
ORDER BY source_id ASC
"""

PAGE_SIZE = 50000

RVS_DEEP_LINK = (
    "https://gea.esac.esa.int/data-server/data"
    "?RETRIEVAL_TYPE=RVS&ID=Gaia+DR3+{source_id}&DATA_STRUCTURE=INDIVIDUAL"
)


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_source_id = cursor.get("last_source_id", 0)

    tap = make_tap_service(TAP_URL)
    query = QUERY.format(page_size=PAGE_SIZE, last_source_id=last_source_id)
    # pyvo defaults maxrec to ~20000 regardless of the ADQL TOP clause —
    # confirmed live elsewhere in this codebase (eso.py) — set explicitly.
    table = tap.search(query, maxrec=PAGE_SIZE).to_table()

    records = []
    max_source_id = last_source_id
    for row in table:
        source_id = int(row["source_id"])
        max_source_id = max(max_source_id, source_id)
        records.append(
            RawObservation(
                archive_obs_id=str(source_id),
                archive_url=RVS_DEEP_LINK.format(source_id=source_id),
                instrument="Gaia RVS",
                gaia_source_id=source_id,
            )
        )

    new_cursor = {"last_source_id": max_source_id}
    return records, new_cursor
