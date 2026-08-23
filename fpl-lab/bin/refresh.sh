#!/bin/zsh
# Rebuild the FPL Lab dashboard from the live API. Run by launchd on a schedule;
# safe to run by hand at any time.
set -u
cd "$(dirname "$0")/.." || exit 1

# launchd starts with a minimal PATH; node and npm live in the Homebrew prefix.
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

LOG="logs/refresh.log"
mkdir -p logs
stamp() { date "+%Y-%m-%d %H:%M:%S"; }

echo "[$(stamp)] refresh starting" >> "$LOG"
if npm run --silent refresh >> "$LOG" 2>&1; then
  echo "[$(stamp)] refresh ok" >> "$LOG"
else
  echo "[$(stamp)] REFRESH FAILED (exit $?) — previous site/index.html left in place" >> "$LOG"
fi

# Keep the log from growing without bound.
tail -n 500 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
