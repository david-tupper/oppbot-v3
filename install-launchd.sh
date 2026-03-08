#!/usr/bin/env bash
# install-launchd.sh — install a launchd agent to run gong_fetch.py --sync daily at 12pm PT

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
USER="$(whoami)"
LABEL="com.${USER}.gongsync"
PLIST="$HOME/Library/LaunchAgents/${LABEL}.plist"
PYTHON="$(command -v python3)"
LOG="$HOME/Library/Logs/gongsync.log"

cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>${PYTHON}</string>
        <string>${SCRIPT_DIR}/gong_fetch.py</string>
        <string>--sync</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>12</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <key>WorkingDirectory</key>
    <string>${SCRIPT_DIR}</string>
    <key>StandardOutPath</key>
    <string>${LOG}</string>
    <key>StandardErrorPath</key>
    <string>${LOG}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
EOF

# Unload existing agent if present (ignore errors)
launchctl unload "$PLIST" 2>/dev/null || true
launchctl load "$PLIST"

echo "Installed: $PLIST"
echo "Label:     $LABEL"
echo "Runs:      daily at 12:00pm"
echo "Logs:      $LOG"
echo ""
echo "To trigger manually:  launchctl start $LABEL"
echo "To uninstall:         launchctl unload $PLIST && rm $PLIST"
