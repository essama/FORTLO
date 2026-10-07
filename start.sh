#!/bin/sh
set -eu

python -m uvicorn crm_app:app --host 0.0.0.0 --port 8000 &
UI_PID=$!

python app3.py &
SENDER_PID=$!

# Leadgen page (ICP, personas, ranked leads). Restarted on exit; never stops the sender.
( while :; do python -m leadgen serve || true; sleep 10; done ) &

term() {
  kill "$UI_PID" "$SENDER_PID" 2>/dev/null || true
  wait "$UI_PID" "$SENDER_PID" 2>/dev/null || true
}
trap term TERM INT

while :; do
  if ! kill -0 "$UI_PID" 2>/dev/null; then
    echo "CRM UI exited" >&2
    kill "$SENDER_PID" 2>/dev/null || true
    exit 1
  fi
  if ! kill -0 "$SENDER_PID" 2>/dev/null; then
    echo "Email sender exited" >&2
    kill "$UI_PID" 2>/dev/null || true
    exit 1
  fi
  sleep 5
done
