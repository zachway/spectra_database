"""BeSS -- Be Star Spectra database (Observatoire de Paris / OHP), amateur+pro.

http://basebe.obspm.fr/basebe/ -- a ~20-year-old pure-PHP app (Apache
2.2.16, PHP 5.3.3, no TAP/VO/REST, no client-side JS at all -- confirmed
live, zero <script>/onclick anywhere). Previously written off after a
partial investigation as blocked on a real bug (the per-file FITS download
button returns HTTP 200 with correct headers but a 0-byte body, confirmed
live, reproduced on multiple IDs) -- but since this project never
downloads/stores actual file bytes anywhere (every archive_url here is a
pointer back to the source, same reasoning ing.py's docstring gives), that
bug turned out to not matter: there's a second, completely different,
always-public static asset that serves as a perfectly good archive_url --
see ARCHIVE_URL_TMPL below.

Session dance (confirmed live, required): a plain unauthenticated request
to StarConsul.php/Consul.php gets a 302 bounce to Accueil.php (StarConsul)
or a 200 with an empty 0-byte body (Consul, specobj GET) -- both need a
PHPSESSID first "unlocked" by visiting Accueil.php then MenuIntro.php (the
two frames the top-level site loads into on a real page load), *and* a
plausible Referer header on every subsequent request pointing at whichever
page a real click would have come from. Both requirements confirmed by
toggling each independently and watching the request fail without it.

There are two independent, differently-broken query paths on this site --
confirmed by direct experimentation, not assumed:
  - Consul.php's own POST search form (query by name/RA/Dec/date/etc
    directly) always returns "no spectrum corresponding to your query"
    regardless of input (confirmed with a real star, real resolved
    coordinates, every submit-button variant) -- a genuine dead end, not
    used here.
  - The working path is two-step: StarConsul.php (POST, "Be stars" menu
    item) lists every Be star with at least one spectrum in BeSS, each row
    ending in a real link `Consul.php?specobj=<url-encoded star name>`
    with that star's spectrum count. GET-ing that link returns the star's
    actual spectra table (confirmed live: gam Cas, HD5394's real BeSS
    name, has 12,723 rows). This module only uses this second path.

Both listings are paginated via a `deb_next` hidden field + a `next`
submit button (same shape, confirmed on both): StarConsul.php's deb_next
is a plain row-count offset (100 per page, confirmed: 100 -> 200 -> ...);
Consul.php's is an opaque server-side cursor, *not* a row/id offset
(confirmed: 741 -> 2041 after only 65 displayed rows) -- treated as an
opaque continuation token here rather than assumed to have any numeric
meaning, same "don't assume a hidden field's shape" lesson salt_hrs.py
documents for its own pagination.

Row format (confirmed live, both listings): stars with zero spectra show
a bare "0" with no link in the last column (skipped -- 60 of 100 stars on
a real sampled page) -- 1506 stars have a real specobj link, per BeSS's
own Stat.php ("1506 different Be stars"). RA/Dec are plain space-separated
sexagesimal ("00 56 42.53" / "-17 20 09.57", confirmed negative Dec uses a
literal leading '-', astropy parses this natively). Date is plain ISO
"YYYY-MM-DD". Most rows' spectrum id is a bare int in both the `path_<id>`
hidden field name and the `v_ids=<id>` plot link. A minority (confirmed
live on BD+62 2346: 2 of 8 rows) are multi-exposure echelle bundles --
`path_30to59_2_251098` (a compound key, not a bare int -- used as-is for
archive_obs_id, still globally unique) and `v_ids=251098+251099+...+251127`
(every individual order's id, `+`-joined -- only the first is used, as a
representative single-order plot rather than the combined one, for
archive_url). A separate minority of rows (confirmed live on gam Cas) list
2-3 observer `<A>` links back to back instead of one -- the observer field
isn't used for anything here (name/position matching only), but the row
regex still has to tolerate a variable number of them -- and an
inconsistently-present closing `</TD>` right after the last one, confirmed
present on some rows (BD+34 113, RX J0048.5-7302) and absent on others
(gam Cas) with no visible pattern to which -- to correctly reach the
date/HJD/id fields that follow.

ARCHIVE_URL_TMPL: BeSS renders a PNG plot of every spectrum at a
predictable static path outside any PHP session -- confirmed live,
unauthenticated, across 4 different ids spanning different id ranges:
    Spectres_png/S{id:07d}[:3]/sp_{id:07d}.png
(discovered by following SendPng.php?v_ids=<id>, a tiny PHP redirector
that itself just echoes an <img src> at this same static path -- calling
that PHP endpoint isn't necessary once the path formula is known, and
skipping it avoids needing the session dance at all for this one URL).

Pagination/cursor shape: cursor["stars"] holds the full (name, specobj)
list, built once (fully paginated within the first fetch() call -- only
~16 StarConsul.php pages for 1506 stars, cheap to do in one shot) and
persisted from then on rather than re-fetched every call. cursor["star_idx"]
/ cursor["spec_deb"] track how far through that list -- and how far through
the *current* star's own spectra pages -- this run has gotten. Each
fetch() call does exactly one Consul.php page (one star's page of spectra,
or the next page of the same star if it wasn't done), so a single call
never blows up in size even for gam Cas's 12,723 spectra (~160 pages).
Once star_idx reaches the end of the list, fetch() returns no records --
same "static archive short-circuits to a no-op after finishing" pattern
rave.py/galah.py/elodie.py/sophie.py/salt_hrs.py already use; re-running
a finished sync here needs the cursor cleared by hand to do a fresh crawl,
same caveat those modules' callers already accept.
"""

from __future__ import annotations

import re
from datetime import date
from urllib.parse import unquote

import astropy.units as u
import requests
from astropy.coordinates import SkyCoord

from sync.base import RawObservation

BASE_URL = "http://basebe.obspm.fr/basebe"
ACCUEIL_URL = f"{BASE_URL}/Accueil.php?flag_lang=en"
MENUINTRO_URL = f"{BASE_URL}/MenuIntro.php?flag_lang=en"
MENU_STARCONSUL_URL = f"{BASE_URL}/BeSS/MenuStarConsulReq.php"
STARCONSUL_URL = f"{BASE_URL}/BeSS/StarConsul.php"
CONSUL_URL = f"{BASE_URL}/BeSS/Consul.php"

STAR_LIST_PAGE_SIZE = 100
MAX_STAR_LIST_PAGES = 200  # 1506 stars / 100 per page ~= 16, generous safety cap

TIMEOUT = (15, 180)

_ROW_START_RE = re.compile(r"<TR><TD>(\d+)</TD>")
_STAR_ROW_RE = re.compile(
    r"objet=[^ >]+ target=visutype>(?P<name>[^<]*)</A>.*?"
    r"Consul\.php\?specobj=(?P<specobj>[^>]+)>(?P<count>\d+)</TD>",
    re.S,
)
_SPEC_ROW_RE = re.compile(
    r"<TD>\d+</TD><TD><A href=visutype\.php\?type=objet&objet=[^ >]+ target=visutype>(?P<name>[^<]*)</A></TD>"
    r"<TD align=center>[^<]*</TD>"
    r"<TD>(?P<ra>[^<]*)</TD><TD>(?P<dec>[^<]*)</TD>"
    r"<TD><A href=visutype\.php\?type=instru&instru=[^ >]+ target=visutype>(?P<instrument>[^<]*)</A></TD>"
    r"<TD><A href=visutype\.php\?type=site&site=[^ >]+ target=visutype>[^<]*</A></TD>"
    r"<TD>(?:<A href=visutype\.php\?type=obs&obs=[^ >]+ target=visutype>[^<]*</A>\s*)+(?:</TD>)?"
    r"<TD>(?P<date>[^<]*)</TD><TD>(?P<hjd>[^<]*)</TD>"
    r".*?SendPng\.php\?v_ids=(?P<vid>\d+)"
    r".*?name=\"path_(?P<pathkey>[^\"]+)\"",
    re.S,
)
_DEB_NEXT_RE = re.compile(r"deb_next\s+value=(\d+)")
_HAS_NEXT_RE = re.compile(r"name=next class=bouton value=\"Next page\"")

_session = requests.Session()
_bootstrapped = False


def _bootstrap() -> None:
    global _bootstrapped
    if _bootstrapped:
        return
    _session.get(ACCUEIL_URL, timeout=TIMEOUT)
    _session.get(MENUINTRO_URL, timeout=TIMEOUT)
    _bootstrapped = True


def _archive_url(spec_id: int) -> str:
    zid = f"{spec_id:07d}"
    return f"{BASE_URL}/Spectres_png/S{zid[:3]}/sp_{zid}.png"


def _fetch_star_list_page(deb: int | None) -> str:
    _bootstrap()
    if deb is None:
        data = {
            "req_onlyautour": "1",
            "req_morecrit": "",
            "req_tri": "",
            "req_objet": "",
            "req_category": "4294967295",
            "req_submit": "Submit",
        }
        referer = MENU_STARCONSUL_URL
    else:
        data = {
            "req_onlyautour": "1",
            "req_morecrit": "",
            "req_tri": "",
            "req_objet": "",
            "req_category": "4294967295",
            "deb_next": str(deb),
            "next": "Next page",
        }
        referer = STARCONSUL_URL
    response = _session.post(STARCONSUL_URL, data=data, headers={"Referer": referer}, timeout=TIMEOUT)
    response.raise_for_status()
    return response.text


def _parse_star_list_page(html: str) -> tuple[list[tuple[str, str]], int | None, bool]:
    stars = []
    starts = [m.start() for m in _ROW_START_RE.finditer(html)] + [len(html)]
    for i in range(len(starts) - 1):
        row = html[starts[i] : starts[i + 1]]
        m = _STAR_ROW_RE.search(row)
        if not m:
            continue  # zero-spectra star, no specobj link
        stars.append((m["name"].strip(), m["specobj"]))
    deb_match = _DEB_NEXT_RE.search(html)
    next_deb = int(deb_match.group(1)) if deb_match else None
    has_next = bool(_HAS_NEXT_RE.search(html))
    return stars, next_deb, has_next


def _fetch_all_stars() -> list[tuple[str, str]]:
    stars: list[tuple[str, str]] = []
    deb = None
    for _ in range(MAX_STAR_LIST_PAGES):
        html = _fetch_star_list_page(deb)
        page_stars, next_deb, has_next = _parse_star_list_page(html)
        stars.extend(page_stars)
        if not has_next:
            break
        deb = next_deb
    return stars


def _fetch_spectra_page(specobj: str, deb: str | None) -> str:
    _bootstrap()
    url = f"{CONSUL_URL}?specobj={specobj}"
    if deb is None:
        response = _session.get(url, headers={"Referer": STARCONSUL_URL}, timeout=TIMEOUT)
    else:
        name = unquote(specobj, encoding="latin-1")
        data = {
            "req_onlyautour": "1",
            "req_morecrit": "",
            "req_objet": name,
            "req_id_obs": "TOUS",
            "req_instrument": "TOUS",
            "req_site": "TOUS",
            "req_amateur_pro": "PA",
            "deb_next": deb,
            "next": "Next page",
        }
        response = _session.post(url, data=data, headers={"Referer": url}, timeout=TIMEOUT)
    response.raise_for_status()
    return response.text


def _parse_coords(ra_str: str, dec_str: str) -> tuple[float, float] | tuple[None, None]:
    ra_str = " ".join(ra_str.split())
    dec_str = " ".join(dec_str.split())
    if not ra_str or not dec_str:
        return None, None
    try:
        coord = SkyCoord(ra=ra_str, dec=dec_str, unit=(u.hourangle, u.deg))
    except ValueError:
        return None, None
    return coord.ra.deg, coord.dec.deg


def _parse_spectra_page(html: str, star_name: str) -> tuple[list[RawObservation], str | None, bool]:
    records = []
    starts = [m.start() for m in re.finditer(r"<TR class=(?:amateur|pro)>", html)] + [len(html)]
    for i in range(len(starts) - 1):
        row = html[starts[i] : starts[i + 1]]
        m = _SPEC_ROW_RE.search(row)
        if not m:
            continue
        ra, dec = _parse_coords(m["ra"], m["dec"])
        try:
            obs_date = date.fromisoformat(m["date"].strip())
        except ValueError:
            obs_date = None
        records.append(
            RawObservation(
                archive_obs_id=m["pathkey"],
                archive_url=_archive_url(int(m["vid"])),
                instrument=m["instrument"].strip() or None,
                obs_date=obs_date,
                ra=ra,
                dec=dec,
                raw_target_name=(m["name"].strip() or star_name) or None,
            )
        )
    deb_match = _DEB_NEXT_RE.search(html)
    next_deb = deb_match.group(1) if deb_match else None
    has_next = bool(_HAS_NEXT_RE.search(html))
    return records, next_deb, has_next


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    stars = cursor.get("stars")
    if stars is None:
        stars = [list(pair) for pair in _fetch_all_stars()]
        star_idx = 0
        spec_deb = None
    else:
        star_idx = cursor.get("star_idx", 0)
        spec_deb = cursor.get("spec_deb")

    records: list[RawObservation] = []
    while star_idx < len(stars):
        name, specobj = stars[star_idx]
        html = _fetch_spectra_page(specobj, spec_deb)
        page_records, next_deb, has_next = _parse_spectra_page(html, name)
        records.extend(page_records)
        if has_next:
            spec_deb = next_deb
            break
        star_idx += 1
        spec_deb = None
        if page_records:
            break

    return records, {"stars": stars, "star_idx": star_idx, "spec_deb": spec_deb}
