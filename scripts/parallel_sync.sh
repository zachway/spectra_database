#!/bin/bash
# Runs sync.main as several concurrent processes instead of one long
# sequential pass. Safe to do because the DB writes are already partitioned
# per archive_code (spectroscopy_holdings, archive_sync_state) except for the
# shared `stars` table, whose upserts sort gaia_source_id ascending before
# writing (ingest/add_star.py) so concurrent transactions always take row
# locks in the same order -- no deadlock risk, just an occasional short wait.
#
# The groups below are a rough round-robin split of sync/main.py's ARCHIVES
# list, chosen to keep the long-running archives (lick, lamost, lamost_mrs)
# in separate groups and the two GOA-cookie-gated archives (gemini_ghost,
# gemini_igrins) in separate groups too, so neither pairing bottlenecks a
# single group. Rebalance based on what you actually observe -- this hasn't
# been run yet.
#
# Usage: scripts/parallel_sync.sh
# Logs land in logs/parallel_sync_<group>_<timestamp>.log

set -euo pipefail

cd /nfs/morgan/users/way/spectra_database
source venv/bin/activate

export DATABASE_URL="postgresql:///spectra_db?host=/tmp"
# GOA_SESSION_COOKIE, if already set in the environment, is inherited as-is --
# required for gemini_ghost/gemini_igrins, see sync/archives/_goa_common.py.

GROUP_A=(rave dao koa lick sdss_v_apogee carmenes_caha asiago salt_hrs)
GROUP_B=(galah gemini lamost mast sdss_v_optical desi harpsn_tng ing gtc)
GROUP_C=(eso gemini_ghost lamost_mrs mast_jwst sdss_legacy_optical feros_gavo elodie hermes_mercator)
GROUP_D=(cfht_cadc gemini_igrins lbt noirlab carmenes flashheros_gavo sophie)

mkdir -p logs
STAMP="$(date +%Y%m%d_%H%M%S)"

pids=()
for group_name in A B C D; do
    var="GROUP_$group_name[@]"
    archives=("${!var}")
    log="logs/parallel_sync_${group_name}_${STAMP}.log"
    echo "starting group $group_name -> ${archives[*]} (log: $log)"
    python3 -m sync.main --only "${archives[@]}" >"$log" 2>&1 &
    pids+=("$!")
done

status=0
for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
        status=1
    fi
done

exit "$status"
