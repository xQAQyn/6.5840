#!/usr/bin/env bash
# Repeatedly run `make raft1` (full 3A-3D suite, with -race) until a run
# FAILS or the script is interrupted (Ctrl+C). Useful for catching flaky /
# random test failures.
#
# Each run's full output goes to /tmp/raft_runs/run-NNN.log; only a one-line
# summary is printed to the terminal (plus the tail of the log on failure).

set -u

# Run from the directory that contains the Makefile (this script's dir).
cd "$(dirname "$0")"

LOGDIR="/tmp/raft_runs"
mkdir -p "$LOGDIR"

run=0
pass=0

cleanup_leftovers() {
  # Kill any raft test processes left behind by a previous (possibly
  # interrupted) run, so they don't interfere with the next one.
  pkill -f 'raft1\.test' 2>/dev/null
  pkill -f 'main/raft1d' 2>/dev/null
  return 0
}

on_interrupt() {
  echo ""
  echo "=== interrupted: $run run(s) started, $pass passed ==="
  exit 130
}
trap on_interrupt INT TERM

while true; do
  run=$((run + 1))
  cleanup_leftovers
  log="$LOGDIR/run-$(printf '%03d' "$run").log"
  printf '[%s] === run #%d starting (log: %s) ===\n' "$(date '+%H:%M:%S')" "$run" "$log"

  start=$(date +%s)
  make RUN="-run 3A" raft1 > "$log" 2>&1
  rc=$?
  dur=$(( $(date +%s) - start ))

  if [ "$rc" -eq 0 ]; then
    pass=$((pass + 1))
    printf '[%s] === run #%d PASSED in %ds (passed %d/%d) ===\n' \
      "$(date '+%H:%M:%S')" "$run" "$dur" "$pass" "$run"
  else
    printf '[%s] !!! run #%d FAILED (exit %d) after %ds !!!\n' \
      "$(date '+%H:%M:%S')" "$run" "$rc" "$dur"
    echo "----- tail of $log -----"
    tail -50 "$log"
    echo "----- full log: $log -----"
    echo "=== stopped after $run run(s), $pass passed, 1 failure ==="
    cleanup_leftovers
    exit 1
  fi
done
