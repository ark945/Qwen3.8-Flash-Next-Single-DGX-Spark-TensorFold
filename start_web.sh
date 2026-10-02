#!/usr/bin/env bash
# ==============================================================================
# start_web.sh: 啟動 TensorFold (1235) + SearXNG (8080) + Web Gateway (1234)
# ==============================================================================
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

GATEWAY_PORT="${PORT:-1234}"
TENSORFOLD_PORT="1235"
SEARXNG_PORT="8080"
GATEWAY_LOG="$DIR/gateway.log"
GATEWAY_PID_FILE="$DIR/.gateway.pid"

# 顏色輸出
G=$'\033[1;32m'; Y=$'\033[1;33m'; M=$'\033[1;35m'; R=$'\033[0m'

run_docker() {
    if groups | grep -qw docker; then
        docker "$@"
    else
        sg docker -c "docker $*"
    fi
}

echo "============================================================"
echo "${M}  啟動 TensorFold 智慧聯網服務 (SearXNG + Gateway)${R}"
echo "============================================================"

# 1. 檢查並啟動 SearXNG 搜尋容器
if curl -s "http://127.0.0.1:$SEARXNG_PORT" >/dev/null 2>&1; then
    echo "${G}[SearXNG] searxng 服務已正常運行中 (Port $SEARXNG_PORT)${R}"
elif command -v docker >/dev/null 2>&1; then
    if ! run_docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "searxng"; then
        if run_docker ps -a --format '{{.Names}}' 2>/dev/null | grep -qx "searxng"; then
            echo "${Y}[SearXNG] 正在啟動現有 searxng 容器...${R}"
            run_docker start searxng >/dev/null 2>&1 || true
        else
            echo "${Y}[SearXNG] 正在建立並啟動 searxng 容器 (Port $SEARXNG_PORT)...${R}"
            run_docker run -d --name searxng --restart always -p "$SEARXNG_PORT:8080" \
                -e "BASE_URL=http://localhost:$SEARXNG_PORT/" \
                -e "INSTANCE_NAME=LocalSearch" \
                searxng/searxng:latest >/dev/null 2>&1 || {
                echo "${Y}[SearXNG] 容器建立略過（可能無 docker 權限或映像檔拉取中）${R}"
            }
        fi
    else
        echo "${G}[SearXNG] searxng 容器運行中 (Port $SEARXNG_PORT)${R}"
    fi
fi

# 確保 SearXNG 開啟 JSON API 格式支援 (避免 403 Forbidden)
if command -v docker >/dev/null 2>&1; then
    run_docker exec searxng sh -c 'grep -q "json" /etc/searxng/settings.yml 2>/dev/null || (printf "\nsearch:\n  formats:\n    - html\n    - json\n" >> /etc/searxng/settings.yml && kill -HUP 1 2>/dev/null || true)' >/dev/null 2>&1 || true
fi

# 2. 啟動 TensorFold 模型（指定內部 1235 Port，不要思考鏈 --no-thinking）
if curl -s "http://127.0.0.1:$TENSORFOLD_PORT/v1/models" >/dev/null 2>&1; then
    echo "${G}[TensorFold] 模型核心已在內部 Port $TENSORFOLD_PORT 運行中，沿用現有服務。${R}"
else
    echo "${Y}[TensorFold] 正在啟動模型本體於內部 Port $TENSORFOLD_PORT (不要思考鏈 --no-thinking)...${R}"
    if groups | grep -qw docker; then
        PORT="$TENSORFOLD_PORT" ./start.sh restart --no-thinking
    else
        sg docker -c "PORT='$TENSORFOLD_PORT' ./start.sh restart --no-thinking" || PORT="$TENSORFOLD_PORT" ./start.sh restart --no-thinking
    fi
fi

# 3. 停止舊的 Gateway（若有）
if [ -f "$GATEWAY_PID_FILE" ]; then
    OLD_PID=$(cat "$GATEWAY_PID_FILE" 2>/dev/null || true)
    if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
        echo "${Y}[Gateway] 停止舊的 Gateway 進程 (PID $OLD_PID)...${R}"
        kill "$OLD_PID" 2>/dev/null || true
        sleep 1
    fi
    rm -f "$GATEWAY_PID_FILE"
fi

# 釋放 Port 佔用（防範孤兒進程或手動啟動的實例佔用 Port）
PORT_PIDS=$(lsof -ti :$GATEWAY_PORT 2>/dev/null || true)
if [ -n "$PORT_PIDS" ]; then
    echo "${Y}[Gateway] 釋放佔用 Port $GATEWAY_PORT 的殘留進程 ($PORT_PIDS)...${R}"
    kill -9 $PORT_PIDS 2>/dev/null || true
    sleep 1
fi

# 4. 準備 Gateway 虛擬環境與依賴 (DuckDB)
GATEWAY_VENV="$DIR/gateway/.venv"
if [ ! -f "$GATEWAY_VENV/bin/python3" ]; then
    echo "${Y}[Gateway] 正在建立 Gateway 獨立虛擬環境 ($GATEWAY_VENV)...${R}"
    python3 -m venv "$GATEWAY_VENV"
    "$GATEWAY_VENV/bin/pip" install --upgrade pip >/dev/null 2>&1 || true
    echo "${Y}[Gateway] 安裝 DuckDB 數據引擎...${R}"
    "$GATEWAY_VENV/bin/pip" install duckdb >/dev/null 2>&1
fi

# 5. 啟動 Web Gateway（監聽 1234）
echo "${Y}[Gateway] 啟動智慧股票分析與搜尋網關於 Port $GATEWAY_PORT...${R}"
nohup setsid "$GATEWAY_VENV/bin/python3" -u "$DIR/gateway/gateway.py" \
    --port "$GATEWAY_PORT" \
    --upstream "127.0.0.1:$TENSORFOLD_PORT" \
    --searxng "http://127.0.0.1:$SEARXNG_PORT" \
    --stock-dir "/home/blue/stock_data" </dev/null >> "$GATEWAY_LOG" 2>&1 &

GATEWAY_PID=$!
echo "$GATEWAY_PID" > "$GATEWAY_PID_FILE"
disown "$GATEWAY_PID" 2>/dev/null || true

# 等待 Gateway 就緒與 DuckDB 視圖載入完成 (最多 15 秒)
READY=0
for i in {1..15}; do
    if curl -s "http://127.0.0.1:$GATEWAY_PORT/v1/models" >/dev/null 2>&1; then
        READY=1
        break
    fi
    sleep 1
done

if [ "$READY" -eq 1 ]; then
    IP=$(hostname -I 2>/dev/null | awk '{print $1}')
    echo ""
    echo "${G}  ✔ 智慧股票分析與聯網服務已就緒！${R}"
    echo "============================================================"
    echo "    對外 API 端點 : http://${IP:-127.0.0.1}:$GATEWAY_PORT/v1"
    echo "    模型名稱 (ID) : huihui-qwen3.8-27b-abliterated"
    echo "    API Key       : LM-STUDIO"
    echo "    股票量化分析  : 已啟用 (DuckDB + 原生 Tool Calling: /home/blue/stock_data)"
    echo "    聯網搜尋能力  : 已啟用 (SearXNG Tool: web_search)"
    echo "    思考鏈模式    : 已全域關閉 (不要思考鏈 --no-thinking)"
    echo "    Gateway 日誌  : tail -f $GATEWAY_LOG"
    echo "============================================================"
else
    echo "Gateway 啟動失敗，請查看 $GATEWAY_LOG"
    exit 1
fi

