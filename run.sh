#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

ENV_NAME="${MUSIC_GALAXY_ENV:-music-galaxy}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8000}"
ORIGINAL_PORT="${PORT}"

if ! command -v conda >/dev/null 2>&1; then
  echo "Error: conda not found in PATH. Install Miniconda/Anaconda or open a terminal where conda is initialized." >&2
  exit 1
fi

if ! conda run -n "${ENV_NAME}" python -c "import fastapi, uvicorn" >/dev/null 2>&1; then
  echo "Error: conda env '${ENV_NAME}' is missing or incomplete." >&2
  echo "Create it with:" >&2
  echo "  conda env create -f music_galaxy_environment.yml --override-channels -c conda-forge" >&2
  exit 1
fi

if [[ "${1:-}" == "--init-db" ]]; then
  shift || true
  echo "Initializing database and importing CSV data..."
  conda run -n "${ENV_NAME}" python -m app.init_db
fi

if command -v lsof >/dev/null 2>&1; then
  if lsof -iTCP:"${PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
    FOUND_FREE_PORT=0
    for candidate in $(seq "$((PORT + 1))" "$((PORT + 20))"); do
      if ! lsof -iTCP:"${candidate}" -sTCP:LISTEN >/dev/null 2>&1; then
        PORT="${candidate}"
        FOUND_FREE_PORT=1
        break
      fi
    done

    if [[ "${FOUND_FREE_PORT}" == "1" ]]; then
      echo "Port ${ORIGINAL_PORT} is already in use. Switching to http://${HOST}:${PORT}/"
    else
      echo "Error: port ${ORIGINAL_PORT} is already in use, and no free port was found in the next 20 ports." >&2
      echo "Tip: run with a custom port, for example: PORT=8010 ./run.sh" >&2
      exit 1
    fi
  fi
fi

echo "Starting Music Galaxy at http://${HOST}:${PORT}/"
echo "Press Ctrl+C to stop."
if [[ "${UVICORN_RELOAD:-}" == "1" ]]; then
  exec conda run -n "${ENV_NAME}" uvicorn app.main:app --reload --host "${HOST}" --port "${PORT}"
else
  exec conda run -n "${ENV_NAME}" uvicorn app.main:app --host "${HOST}" --port "${PORT}"
fi
