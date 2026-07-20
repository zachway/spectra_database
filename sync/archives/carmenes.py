"""CARMENES GTO DR1 — static HTML table, resolved to Gaia via SIMBAD.

The DR1 portal table carries two identifiers per star: Karmn (Carmencita ID,
e.g. "J00051+457") and a separate SIMBAD discovery name (e.g. "GJ 2"). The
Karmn id doesn't resolve reliably through SIMBAD, but the discovery name
does — same resolution path as ingest.add_star, batched via
Simbad.query_objects for all ~362 stars in one request instead of one call
each.

One holding per star, not per epoch: each star's zip bundles multiple
SERVAL-format epochs, and unzipping every star's archive to enumerate
individual epoch dates is real extra work not justified yet — this follows
the same cumulative, one-row-per-star treatment as sdss_v_apogee.py. DR1 is
a fixed release (2016-2020 GTO), so — like rave.py — this is a one-time pull
gated by a "synced_at" cursor rather than an incremental watermark.

TAC (the co-added template library) and the broader CAHA archive are not
covered here — see the artifact's CARMENES card for that context.
"""

import requests
from astropy.time import Time
from astroquery.simbad import Simbad
from bs4 import BeautifulSoup

from sync.base import RawObservation

DR1_URL = "http://carmenes.cab.inta-csic.es/gto/jsp/dr1Public.jsp"
ZIP_URL = "http://carmenes.cab.inta-csic.es/gto/getDR1DataPublic.action?id={filename}"


def _parse_dr1_table() -> list[tuple[str, str, str]]:
    resp = requests.get(DR1_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    seen = {}
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        karmn = tds[0].get_text(strip=True)
        name = tds[1].get_text(strip=True)
        zip_link = tds[2].find("a")
        if not karmn or not name or zip_link is None:
            continue
        # The source HTML embeds the filename as "id=\t<name>\t" — the
        # surrounding whitespace is literally part of the attribute value.
        _, _, filename = zip_link["href"].partition("id=")
        seen[karmn] = (karmn, name, filename.strip())

    return list(seen.values())


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    if cursor.get("synced_at"):
        return [], cursor

    rows = _parse_dr1_table()

    simbad = Simbad()
    simbad.add_votable_fields("ids")
    resolved = simbad.query_objects([name for _, name, _ in rows])
    gaia_by_name = {}
    for r in resolved:
        if r["ids"] is None:
            continue
        tokens = [tok for tok in str(r["ids"]).split("|") if tok.startswith("Gaia DR3 ")]
        if tokens:
            # user_specified_id is a fixed-width VOTable field, space-padded.
            gaia_by_name[r["user_specified_id"].strip()] = int(tokens[0].removeprefix("Gaia DR3 "))

    records = []
    for karmn, name, filename in rows:
        gaia_source_id = gaia_by_name.get(name)
        if gaia_source_id is None:
            continue
        records.append(
            RawObservation(
                archive_obs_id=karmn,
                archive_url=ZIP_URL.format(filename=filename),
                instrument="CARMENES VIS",
                gaia_source_id=gaia_source_id,
                raw_target_name=name,
            )
        )

    new_cursor = {"synced_at": Time.now().isot, "row_count": len(records), "unresolved": len(rows) - len(records)}
    return records, new_cursor
