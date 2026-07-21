#!/bin/bash
# Self-expiring wrapper for the hourly Parquet export cron job — checks an
# expiry timestamp and removes its own crontab entry once past it, so a
# temporary "keep the hosted snapshot fresh for a couple days" cron doesn't
# need to be remembered and cleaned up by hand later.
#
# Setup (not done by this script):
#   date -d "+2 days" +%s > ~/.spectra_export_cron_expires_at
#   (crontab -l 2>/dev/null; echo "0 * * * * $PWD/scripts/export_cron_wrapper.sh >> $PWD/export_cron.log 2>&1") | crontab -
set -euo pipefail

EXPIRE_FILE="$HOME/.spectra_export_cron_expires_at"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -f "$EXPIRE_FILE" ] && [ "$(date +%s)" -ge "$(cat "$EXPIRE_FILE")" ]; then
    crontab -l | grep -vF "export_cron_wrapper.sh" | crontab -
    rm -f "$EXPIRE_FILE"
    echo "$(date): expired, removed from crontab"
    exit 0
fi

"$SCRIPT_DIR/export.sh"
