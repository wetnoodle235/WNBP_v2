#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────
# V5.0 — Start all services (backend + website)
# ──────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"
VENV_PY="$ROOT/../.venv/bin/python"

echo "Starting V5.0 services…"

# Pick a backend runner that works in this repo.
if [[ -x "$VENV_PY" ]]; then
	BACKEND_CMD=("$VENV_PY" -m uvicorn)
elif command -v uvicorn >/dev/null 2>&1; then
	BACKEND_CMD=(uvicorn)
else
	echo "ERROR: uvicorn not found. Install it or create .venv at $ROOT/../.venv" >&2
	exit 1
fi

# Start backend
echo "  → Starting backend (port 8000)…"
cd "$ROOT/backend"
"${BACKEND_CMD[@]}" main:app --host 127.0.0.1 --port 8000 --reload &
BACKEND_PID=$!
echo "    PID: $BACKEND_PID"

for _ in {1..20}; do
	if curl -sf http://127.0.0.1:8000/docs >/dev/null 2>&1; then
		echo "    Backend ready"
		break
	fi
	sleep 0.5
done

# Start website
echo "  → Starting website (port 3000)…"
cd "$ROOT/website"
rm -rf .next-dev
npm run dev -- --hostname 0.0.0.0 &
WEBSITE_PID=$!
echo "    PID: $WEBSITE_PID"

for _ in {1..40}; do
	if curl -sf http://127.0.0.1:3000 >/dev/null 2>&1; then
		echo "    Website ready"
		break
	fi
	sleep 0.5
done

echo ""
echo "Services running:"
echo "  Backend:  http://localhost:8000 (docs: http://localhost:8000/docs)"
echo "  Website:  http://localhost:3000"
echo ""
echo "Press Ctrl+C to stop all services"

trap "kill $BACKEND_PID $WEBSITE_PID 2>/dev/null; exit" INT TERM
wait
