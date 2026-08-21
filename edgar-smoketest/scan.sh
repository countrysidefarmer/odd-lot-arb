#!/usr/bin/env bash
# One-command EDGAR insider/activist scan.
#   ./scan.sh                 # last 30 days, prints top-25 signal table
#   ./scan.sh --days 30 --top 40
#   ./scan.sh --days 7        # quick week
# Re-runs are instant for anything already in ./cache (safe to Ctrl-C).
set -euo pipefail
cd "$(dirname "$0")"

# Pin the working interpreter: on this Mac `python3` resolves to a broken
# 2018 Python 3.6; the Apple system one at /usr/bin/python3 (3.9) has requests.
PY=/usr/bin/python3

# SEC requires a declared User-Agent naming a real contact. Keep it out of the
# repo: set SEC_UA in the environment, or drop it in a local .env (gitignored).
if [ -z "${SEC_UA:-}" ] && [ -f .env ]; then
  set -a; . ./.env; set +a
fi
if [ -z "${SEC_UA:-}" ]; then
  echo 'SEC_UA is not set. SEC requires a contact in the User-Agent.' >&2
  echo 'Run:  export SEC_UA="Your Name you@example.com"' >&2
  echo 'or create edgar-smoketest/.env containing that line.' >&2
  exit 1
fi
export SEC_UA

exec "$PY" run.py "$@"
