#!/usr/bin/env bash
# ==============================================================================
# stop_web.sh: 停止 Gateway、SearXNG 與 TensorFold 模型
# ==============================================================================
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

GATEWAY_PID_FILE="$DIR/.gateway.pid"

if [ -f "$GATEWAY_PID_FILE" ]; then
    PID=$(cat "$GATEWAY_PID_FILE" 2>/dev/null || true)
    if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
        echo "正在停止 Web Gateway (PID $PID)..."
        kill "$PID" 2>/dev/null || true
    fi
    rm -f "$GATEWAY_PID_FILE"
fi

if groups | grep -qw docker; then
    ./stop.sh
else
    sg docker -c "./stop.sh" || ./stop.sh
fi

echo "所有服務已停止。"
