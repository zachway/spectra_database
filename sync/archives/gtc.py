"""GTC (Gran Telescopio CANARIAS) Public Archive -- HTML form POST, no TAP/API.

Same CAB/INTA JSP application family as carmenes_caha.py
(gtc.sdc.cab.inta-csic.es vs caha.sdc.cab.inta-csic.es), but this archive
was previously written off as a dead end (see project memory
project_archive_survey_batch2_results): a bare GET on `searchform.jsp`
reproducibly 500s. That 500 turned out to be the JSP app rejecting
requests with no session cookie and no browser-like User-Agent/Accept
headers -- confirmed live, twice, with a fresh unauthenticated session:
hitting `/gtc/` first to pick up a JSESSIONID and sending a real
User-Agent made `searchform.jsp` return 200 every time. Once past that,
the actual search (`searchres.jsp`, POST multipart/form-data) turned out
to need *no* session/cookie at all -- a completely stateless one-shot POST
with every filter field set returns real results (confirmed live,
719,927+ products for a full-history spectroscopy-only query). The
"500 even with a session cookie" conclusion in the earlier investigation
was itself a User-Agent/Accept-header issue, not a real server bug.

No registry/API shortcut exists either: reg.g-vo.org lists a SIAP
(imaging) service at /gtc/siap/{gtc,osiris}_siap.jsp, real and live, but
SIAP is image-only and disjoint from the spectroscopy modes covered here.

instCode restricts the query server-side to spectroscopy-only observing
modes: OSI_LSS/OSI_MOS (OSIRIS Long Slit / Multi-Object Spectroscopy),
MEG_SPE/MEG_IFU (MEGARA MOS / IFU), HORuS_SPE, CC_SPE (CanariCam
spectroscopy), EMIR_SPE -- excludes every imaging/polarimetry/tunable-
filter mode the same form also offers (confirmed real server-side
filtering, not a client-side illusion). No further calibration-frame
filter is applied -- a sample of 3,000 recent rows showed no BIAS/ARC/FLAT-
style object names, unlike koa.py/lbt.py's archives; whatever engineering
frames do exist should simply fail to resolve downstream, same reasoning
lick.py/ing.py give for their own unfiltered free-text object names.

Each row's Program ID + OBlock ID + numeric ProdId (e.g.
"GTC78-19B..0007..2809328") is the archive's own composite key for
`FetchProd`, its direct per-exposure raw-FITS download servlet -- confirmed
live to serve a real ungated FITS.gz (200 OK, ~11MB, no login) for public
data. Public rows carry this link directly in the HTML; embargoed rows
collapse everything past the "Pub" column into one
"Private Data. They will become public on: <date>" cell with no link at
all -- but Program ID/OBlock ID/ProdId are still visible before that
collapse, so the same composite URL is built by hand for those rows too
(same convention as salt_hrs.py: the URL will 403/redirect until the
listed release date passes, harmless since nothing here downloads bytes).
GTC's own front page states data become public after a 1-year proprietary
period.

Pagination: rpp (results per page) is a plain form field, not capped to
the dropdown's advertised 10/50/100 -- passing an arbitrary larger value
works (confirmed live: rpp=3000 returns exactly 3000 rows in ~18s,
rpp=5000 in ~46s), but there's a real cliff beyond that (rpp=10000 didn't
finish in 90s, confirmed live twice) -- capped well under it here. Bigger
problem: a *fixed* full-history query is default-ordered newest-first
(order_by=0, Observing Date) with no ascending option, and the archive is
live -- its own "N products found" count changed by hundreds between two
back-to-back requests during investigation. A page-number/frontier cursor
like carmenes_caha.py's would silently miss everything new (new rows land
at page 1, and a frontier cursor only ever advances to *later*, i.e.
older, pages). Paginates instead as an adaptive calendar-window walk on
the dateinit/dateend fields, same shape as ing.py: request one page per
window at a size well under the rpp cliff, and if the returned count hits
that cap (a real page is likely truncated), bisect the window and retry;
grow the window back up after an unsaturated page. This walks strictly
forward through the fixed [FIRST_DATE, today-at-window-open] history
regardless of how new inserts reorder later pages, so nothing at the
growing edge is ever skipped.
"""

from __future__ import annotations

from datetime import date, timedelta

import requests
from astropy.time import Time
from bs4 import BeautifulSoup

from sync.base import RawObservation

SEARCH_URL = "https://gtc.sdc.cab.inta-csic.es/gtc/jsp/searchres.jsp"
FETCH_URL = "https://gtc.sdc.cab.inta-csic.es/gtc/servlet/FetchProd?prod_id={prod_id}"

# Spectroscopy-only instCode values, read off the form's own checkboxes --
# excludes every imaging/polarimetry/tunable-filter mode.
INST_CODES = ["OSI_LSS", "OSI_MOS", "MEG_SPE", "MEG_IFU", "HORuS_SPE", "CC_SPE", "EMIR_SPE"]

# GTC saw first light in 2007; science operations ramped up after that.
# Not worth pinning down further -- the empty-window growth below skips
# any early gap quickly regardless, same reasoning ing.py's FIRST_DATE gives.
FIRST_DATE = date(2007, 1, 1)

# Comfortably under the confirmed ~rpp=10000 cliff (rpp=5000 took ~46s,
# rpp=3000 ~18s) -- used both as the page size and as the "this window is
# probably truncated" signal (a page landing exactly on this count).
PAGE_SIZE = 3000

START_WINDOW_DAYS = 14
MIN_WINDOW_DAYS = 1
MAX_WINDOW_DAYS = 365

# Mirrors ing.py's own per-call cap -- lets one fetch() call skip a long
# empty/sparse stretch of history without the generic stop-on-zero driver
# concluding the whole archive is exhausted.
MAX_WINDOWS_PER_CALL = 60

_session = requests.Session()


def _query(start: date, end: date, rpp: int, pts: int = 1) -> str:
    data = {
        "origen": "searchform.jsp",
        "id": "null",
        "obj_list": "",
        "radius": "5",
        "dateinit_d": str(start.day), "dateinit_m": str(start.month), "dateinit_y": str(start.year),
        "dateend_d": str(end.day), "dateend_m": str(end.month), "dateend_y": str(end.year),
        "onlyRed": "",
        "instCode": INST_CODES,
        "prog_id": "", "prog_pi": "", "obl_id": "", "lp": "0",
        "order_by": "0", "rpp": str(rpp), "pts": str(pts), "submit": "Send",
    }
    # The form declares enctype=multipart/form-data (it has a file-upload
    # field, unused here) -- forcing multipart via an empty `files` entry
    # rather than a plain urlencoded POST, confirmed both required live.
    resp = _session.post(SEARCH_URL, data=data, files={"file": ("", "")}, timeout=(15, 90))
    resp.raise_for_status()
    resp.encoding = "iso-8859-1"
    return resp.text


def _parse_rows(html: str) -> list[RawObservation]:
    soup = BeautifulSoup(html, "html.parser")
    records = []
    for tr in soup.find_all("tr"):
        cells = tr.find_all("td", class_="resfield")
        # Public rows have ~40 resfield cells (reduced/raw/calib link
        # columns included); embargoed rows collapse to 19 -- but the
        # first 16 (through the "Pub" column) are always present either way.
        if len(cells) < 16:
            continue
        vals = [c.get_text(strip=True) for c in cells]

        prodid = vals[0]
        program = vals[1].rstrip("?")
        oblock = vals[3]
        object_name = vals[4]
        try:
            ra = float(vals[5])
            dec = float(vals[6])
        except ValueError:
            ra = dec = None
        instrument = vals[9]
        init_time = vals[11]

        fetch_link = tr.find("a", href=lambda h: h and "FetchProd" in h)
        if fetch_link is not None:
            archive_url = "https://gtc.sdc.cab.inta-csic.es" + fetch_link["href"]
        else:
            archive_url = FETCH_URL.format(prod_id=f"{program}..{oblock}..{prodid}")

        try:
            obs_date = Time(init_time.replace(" ", "T")).to_datetime().date()
        except ValueError:
            obs_date = None

        records.append(
            RawObservation(
                archive_obs_id=prodid,
                archive_url=archive_url,
                instrument=instrument,
                obs_date=obs_date,
                program_id=program,
                ra=ra,
                dec=dec,
                raw_target_name=object_name or None,
            )
        )
    return records


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    window_start = date.fromisoformat(cursor["window_start"]) if cursor.get("window_start") else FIRST_DATE
    window_days = cursor.get("window_days", START_WINDOW_DAYS)
    today = date.today()

    records: list[RawObservation] = []
    windows_scanned = 0

    while window_start <= today and windows_scanned < MAX_WINDOWS_PER_CALL:
        window_end = min(window_start + timedelta(days=window_days - 1), today)

        while True:
            html = _query(window_start, window_end, PAGE_SIZE)
            page_records = _parse_rows(html)
            if len(page_records) >= PAGE_SIZE and window_days > MIN_WINDOW_DAYS:
                window_days = max(MIN_WINDOW_DAYS, window_days // 2)
                window_end = min(window_start + timedelta(days=window_days - 1), today)
                continue
            break

        records.extend(page_records)
        windows_scanned += 1

        window_start = window_end + timedelta(days=1)
        if page_records:
            window_days = min(window_days * 2, MAX_WINDOW_DAYS)
            break
        # Empty-but-untruncated window: keep scanning within this same
        # fetch() call instead of returning a zero-record page, which
        # would make the generic stop-on-zero sync driver think the whole
        # archive is exhausted -- same concern ing.py/lick.py document.
        window_days = min(window_days * 4, MAX_WINDOW_DAYS)

    new_cursor = {"window_start": window_start.isoformat(), "window_days": window_days}
    return records, new_cursor
