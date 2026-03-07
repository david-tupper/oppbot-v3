#!/bin/bash
set -e

cd "$(dirname "$0")"

# Forward port 80 → 5555 so oppbot.local works without a port number
# (requires sudo once per boot; pfctl is reset on restart)
echo "rdr pass on lo0 proto tcp from any to any port 80 -> 127.0.0.1 port 5555" \
  | sudo pfctl -ef - 2>/dev/null || true

# Kill any existing instance
pkill -f triage_server.py 2>/dev/null || true
sleep 0.5

# Start server in background
python3 triage_server.py &
SERVER_PID=$!

# Wait for it to be ready
for i in {1..10}; do
  curl -s -o /dev/null http://localhost:5555 && break
  sleep 0.3
done

# Open browser
open http://oppbot.local

echo "oppbot running at http://oppbot.local (pid $SERVER_PID)"
echo "Press Ctrl+C to stop."

# Keep script alive; kill server on exit
trap "kill $SERVER_PID 2>/dev/null; sudo pfctl -F all 2>/dev/null || true" EXIT
wait $SERVER_PID
