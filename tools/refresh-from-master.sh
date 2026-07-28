#!/usr/bin/env bash
# refresh-from-master.sh — refresh this consumer's MEOS facades against the latest MEOS API,
# end to end, with one command:
#
#   tools/refresh-from-master.sh
#
# It runs the shared MobilityDB/MEOS-API refresh-jvm-chain.sh over this repo — deriving the
# catalog and libmeos from the latest MobilityDB master, building the JMEOS jar, and
# regenerating this consumer's facades. The per-consumer last leg is in tools/refresh.conf.
#
# All of refresh-jvm-chain.sh's options pass through, e.g.:
#   tools/refresh-from-master.sh --mdb ~/src/MobilityDB   # refresh against a local MobilityDB branch
#   tools/refresh-from-master.sh --skip-tests             # regenerate + compile, skip the test run
#
# MEOSAPI=<path> uses an existing MEOS-API checkout (any branch); otherwise MEOS-API master is
# cloned into the work dir. WORK_DIR overrides the scratch location (default <repo>/.meos-chain).
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
WORK="${WORK_DIR:-$HERE/.meos-chain}"
MEOSAPI="${MEOSAPI:-}"

if [ -z "$MEOSAPI" ]; then
  MEOSAPI="$WORK/MEOS-API"
  mkdir -p "$WORK"
  if [ -d "$MEOSAPI/.git" ]; then
    git -C "$MEOSAPI" fetch --quiet https://github.com/MobilityDB/MEOS-API master
    git -C "$MEOSAPI" checkout --quiet FETCH_HEAD
  else
    git clone --quiet https://github.com/MobilityDB/MEOS-API "$MEOSAPI"
  fi
fi

exec "$MEOSAPI/tools/refresh-jvm-chain.sh" \
  --consumer "$HERE" --meos-api "$MEOSAPI" --work-dir "$WORK" "$@"
