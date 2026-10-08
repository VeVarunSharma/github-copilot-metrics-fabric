#!/usr/bin/env sh
set -eu

CONFIG="config/config.yml"
YES=""
PLAN_ONLY=""
FORCE_INIT=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --config)
      CONFIG="$2"
      shift 2
      ;;
    --yes)
      YES="--yes"
      shift
      ;;
    --plan-only)
      PLAN_ONLY="1"
      shift
      ;;
    --force-init)
      FORCE_INIT="--force"
      shift
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

REPO_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$REPO_ROOT"

PYTHON=".venv/bin/python"
if [ ! -x "$PYTHON" ]; then
  python3 -m venv .venv
fi

"$PYTHON" -m pip install --upgrade pip
"$PYTHON" -m pip install -e ".[dev]"

if [ ! -f "$CONFIG" ] || [ -n "$FORCE_INIT" ]; then
  # shellcheck disable=SC2086
  "$PYTHON" -m copilot_metrics_fabric init --output "$CONFIG" $FORCE_INIT
fi

"$PYTHON" -m copilot_metrics_fabric validate --config "$CONFIG"
"$PYTHON" -m copilot_metrics_fabric bootstrap plan --config "$CONFIG"

if [ -z "$PLAN_ONLY" ]; then
  # shellcheck disable=SC2086
  "$PYTHON" -m copilot_metrics_fabric bootstrap apply --config "$CONFIG" $YES
fi
