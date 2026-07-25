"""Forced SSH command for appending one /triage classification submission to
the public triage_submissions.jsonl file on joy.

This file is NOT run by webapp.app or by anything in this repo automatically
-- it must be copied onto joy by hand and wired up as an authorized_keys
forced command. It is the server-side half of webapp.app's
_append_triage_submission(): that function opens an SSH session and writes
one JSON object to stdin; this script is the only thing that session is ever
allowed to run.

Setup on joy (do this once):
    1. Generate a dedicated keypair just for this -- not your personal key:
           ssh-keygen -t ed25519 -f ~/.ssh/triage_submit_key -N ""
       The private half (~/.ssh/triage_submit_key) goes into Cloud Run's
       Secret Manager as JOY_SSH_KEY_PATH content, never into this repo.
    2. Copy this script onto joy, e.g. ~/bin/joy_triage_append.py.
    3. Add ONE line to ~/.ssh/authorized_keys on joy, using the *public*
       half of the key from step 1 and the real target path (matching
       scripts.export_to_parquet's --out-dir):

           command="/usr/bin/python3 /home/you/bin/joy_triage_append.py /home/you/public_html/spectra_data/triage_submissions.jsonl",no-port-forwarding,no-X11-forwarding,no-agent-forwarding,no-pty ssh-ed25519 AAAA...

       The `command=` restriction is what actually matters here: no matter
       what the client (webapp.app) asks to execute over that SSH session,
       the server ignores it and always runs this script instead -- so even
       if the private key leaks in full, whoever has it can only ever append
       one well-formed submission line to that one file. They cannot get a
       shell, read other files, or reach Postgres. Confirmed live against a
       throwaway local sshd during development: a session that requested
       `rm -rf / ; cat /etc/passwd` as its exec string still only ever ran
       this script.
    4. Publish the host's public key as JOY_SSH_HOST_KEY (see
       webapp.app._joy_ssh_client's docstring for the exact format) so the
       webapp pins the expected host key instead of trusting on first use.

Deliberately validates the same shape scripts.export_to_parquet's
import_triage_submissions() will later require (required fields, allowed
outcomes, exactly one of raw_target_name/archive_obs_id) -- rejecting a
malformed line here is cheap and immediate; catching it only at the next
export run would mean a silently-dropped submission with no feedback to
the person who submitted it.
"""

from __future__ import annotations

import fcntl
import json
import sys

REQUIRED_FIELDS = {"archive_code", "outcome", "submitter", "submitted_at"}
ALLOWED_OUTCOMES = {"attach_gaia_source", "not_a_real_target", "confirmed_absent_from_gaia"}


def validate(obj: object) -> str | None:
    """Returns an error message, or None if obj is well-formed."""
    if not isinstance(obj, dict):
        return "submission must be a JSON object"
    if not REQUIRED_FIELDS.issubset(obj):
        return f"missing required fields (need {sorted(REQUIRED_FIELDS)})"
    if obj["outcome"] not in ALLOWED_OUTCOMES:
        return f"unrecognized outcome {obj['outcome']!r}"
    if ("raw_target_name" in obj) == ("archive_obs_id" in obj):
        return "exactly one of raw_target_name/archive_obs_id is required"
    if obj["outcome"] == "attach_gaia_source" and obj.get("proposed_gaia_source_id") is None:
        return "attach_gaia_source requires proposed_gaia_source_id"
    if obj["outcome"] == "confirmed_absent_from_gaia" and not obj.get("gaia_cone_search_result"):
        return "confirmed_absent_from_gaia requires gaia_cone_search_result"
    return None


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: joy_triage_append.py <target-jsonl-path>", file=sys.stderr)
        return 2
    target_path = sys.argv[1]

    raw = sys.stdin.readline()
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        print("invalid JSON", file=sys.stderr)
        return 1

    error = validate(obj)
    if error:
        print(error, file=sys.stderr)
        return 1

    # A single write() of one line, guarded by flock -- safe against
    # concurrent SSH sessions interleaving partial lines even though each
    # session is a separate process (no shared in-process lock possible).
    line = json.dumps(obj, separators=(",", ":")) + "\n"
    with open(target_path, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            f.write(line)
            f.flush()
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)
    return 0


if __name__ == "__main__":
    sys.exit(main())
