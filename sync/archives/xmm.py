"""XMM-Newton RGS (Reflection Grating Spectrometer) -- TAP (xsa.v_exposure /
xsa.v_public_observations), no native Gaia column.

Real, standards-compliant, no-auth TAP service at nxsa.esac.esa.int/tap-server/tap
(ESA's XSA -- XMM-Newton Science Archive), queried the same way as every other
TAP archive in this project (see sync/base.make_tap_service). No 303 redirect
quirk here, unlike chandra.py's cxctap endpoint.

X-ray grating spectroscopy, not imaging or EPIC/OM photometry: `xsa.v_exposure`
covers every XMM instrument (RGS1/RGS2 gratings, the three EPIC cameras, OM)
in one table, filtered here to `instrument LIKE 'RGS%'`. Even within RGS,
`mode_friendly_name` also carries non-dispersed readout modes on the exact
same instrument (confirmed live distinct values: 'Diagnostic 3x3',
'Diagnostic 1x1', 'HTR Single CCD', 'HTR Multiple CCD', 'UNDEFINED' alongside
the real spectroscopy modes 'Spectroscopy', 'Spectroscopy HER',
'Spectroscopy HER + SER', 'Spectroscopy HER + SES', 'Spectroscopy Small
Window') -- filtered to `mode_friendly_name LIKE 'Spectroscopy%'` to isolate
real dispersed-spectrum exposures, plus `is_scientific = 'true'` to exclude
engineering/calibration exposures. 33,819 real rows confirmed live under this
filter (exposure-level, i.e. counting RGS1 and RGS2 separately).

Joined to `xsa.v_public_observations` on `observation_id` (both tables carry
it as `char`, confirmed live -- no type-mismatch surprise) to pick up
`target`/`ra`/`dec`; `v_exposure` itself has no target name or position of
its own. 0 masked/null ra or dec across the whole filtered join (confirmed
live), but 42 of the 33,819 rows have a blank (empty-string, not NULL)
`target` -- handled as a missing name rather than crashing on it.

RGS1 and RGS2 are kept as two separate holdings, not merged into one per
observation -- they're real, physically distinct gratings that both expose
simultaneously (same reasoning as chandra.py keeping HETG/LETG separate).
This turned out to matter for the archive_obs_id key specifically: RGS1 and
RGS2 can share the same `exposure_id` within one observation (confirmed
live, e.g. obsid 0830800201's and 0852580101's exposure_id 'U002' each occur
once under RGS1 and once under RGS2) -- `exposure_id` alone is not a unique
per-instrument key, so the key here is observation_id + instrument +
exposure_id together.

start_utc ties matter for pagination too: RGS1 and RGS2 exposures very often
share the exact same start_utc timestamp down to the second (confirmed live,
e.g. Proxima Centauri obsid 0049350201's RGS1/RGS2 pair both start at
2001-08-12T03:14:23.0; 5,116 of the 33,819 rows' start_utc values are shared
by >1 row) -- a windowed page boundary landing exactly on one of those ties
could split a simultaneous RGS1/RGS2 pair across two pages and, worse, could
let a strict `>` watermark silently drop the later-sorted half. Sidesteps
the whole problem the same way chandra.py does: a single unbounded pull
(TOP 50000, real headroom over the confirmed 33,819 total) returns in ~2.7s,
so no windowing is needed at the current scale -- ties only become a live
risk again once the real total exceeds PAGE_SIZE.

proprietary_end_date on v_public_observations is a real, populated embargo
field -- confirmed live it carries real future dates (e.g. GD 153's
observation 0830801101 shows 2026-07-09). Not filtered on: same convention
as salt_hrs.py, an embargoed row's existence still answers this project's
core question (has this star been observed at all), and archive_url will
simply show XSA's own proprietary-data messaging until each observation's
release date passes.

archive_url points at `nxsa-web/#obsid=<observation_id>` -- confirmed live
this isn't a guess: the archive's own web frontend (a GWT single-page app,
served from nxsa-web/) ships a compiled JS bundle whose History-token parser
contains the literal branch `f.startsWith('obsid=')` (confirmed by fetching
and grepping the real .cache.js permutation), i.e. `#obsid=...` is a real,
app-parsed deep-link route, not a server-rendered page the way chandra.py's
chaser/startViewer.do is -- same "point at a real page in the home archive"
convention as lbt.py/ing.py's own links, just client-side-routed instead of
server-side.

reduction_status intentionally left unset -- no calib_level-equivalent
column exists on either v_exposure or v_public_observations, same reasoning
as chandra.py.

Confirmed live end-to-end: Proxima Centauri has all 9 of its real XMM
observations represented here with RGS1+RGS2 pairs (18 rows total) --
0049350101, 0049350201, 0551120201, 0551120301, 0551120401, 0801880201,
0801880301, 0801880401, 0801880501 -- spanning 2001-08-12 through
2018-03-11, all with real, populated ra/dec.
"""

from __future__ import annotations

from datetime import datetime

from sync.base import RawObservation, clean_float, make_tap_service


def _parse_start_utc(start_utc_str: str) -> datetime:
    # Python 3.9's datetime.fromisoformat only accepts 0, 3, or 6 fractional
    # digits, but XSA returns arbitrary precision (e.g. "...T14:08:26.0") --
    # confirmed live to crash the import on exactly that shape. Pad/truncate
    # to 6 digits so any precision round-trips.
    if "." in start_utc_str:
        base, frac = start_utc_str.split(".", 1)
        start_utc_str = f"{base}.{(frac + '000000')[:6]}"
    return datetime.fromisoformat(start_utc_str)

TAP_URL = "https://nxsa.esac.esa.int/tap-server/tap"

QUERY = """
SELECT TOP {page_size} e.observation_id, e.exposure_id, e.instrument, e.start_utc,
       o.target, o.ra, o.dec
FROM xsa.v_exposure AS e
JOIN xsa.v_public_observations AS o ON e.observation_id = o.observation_id
WHERE e.instrument LIKE 'RGS%'
  AND e.mode_friendly_name LIKE 'Spectroscopy%'
  AND e.is_scientific = 'true'
  AND e.start_utc > '{last_start_utc}'
ORDER BY e.start_utc ASC
"""

PAGE_SIZE = 50000

# XMM-Newton launched 1999-12-10 -- any fixed sentinel before that covers the
# full archive on a first run.
EPOCH = "1999-01-01T00:00:00"


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    last_start_utc = cursor.get("last_start_utc", EPOCH)

    tap = make_tap_service(TAP_URL)
    query = QUERY.format(page_size=PAGE_SIZE, last_start_utc=last_start_utc)
    table = tap.search(query, maxrec=PAGE_SIZE).to_table()

    max_start_utc = last_start_utc
    records = []
    for row in table:
        start_utc_str = str(row["start_utc"])
        max_start_utc = max(max_start_utc, start_utc_str)

        target = str(row["target"]).strip()
        obs_dt = _parse_start_utc(start_utc_str)

        records.append(
            RawObservation(
                archive_obs_id=f"{row['observation_id']}_{row['instrument']}_{row['exposure_id']}",
                archive_url=f"https://nxsa.esac.esa.int/nxsa-web/#obsid={row['observation_id']}",
                instrument=str(row["instrument"]),
                obs_date=obs_dt.date(),
                ra=clean_float(row["ra"]),
                dec=clean_float(row["dec"]),
                raw_target_name=target or None,
            )
        )

    new_cursor = {"last_start_utc": max_start_utc}
    return records, new_cursor
