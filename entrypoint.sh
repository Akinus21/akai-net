#!/bin/bash
set -euo pipefail

SECRETS_FILE="/app/.secrets"
if [ -f "$SECRETS_FILE" ]; then
    set -a
    source "$SECRETS_FILE"
    set +a
fi

QUEUE_URL="${QUEUE_URL:-http://ollama-queue:8000}"
WORKER_KEY="${WORKER_KEY:?WORKER_KEY env var is required}"
MODEL_PATH="${MODEL_PATH:-/models/${AKAI_MODEL_FILENAME:-model.gguf}}"
MODEL_ALIAS="${MODEL_ALIAS:-${AKAI_MODEL_ALIAS:-akai-model}}"
CTX_SIZE="${CTX_SIZE:-${AKAI_CTX_SIZE:-8192}}"
SERVER_PORT="${SERVER_PORT:-8080}"
HEALTHD_PORT="${HEALTHD_PORT:-8081}"
POLL_INTERVAL="${POLL_INTERVAL:-10}"
STATE_FILE="/tmp/rpc_workers.json"

HUB_COMMIT=$(llama-server --version 2>&1 | grep -oP '\([a-f0-9]+\)' | tr -d '()' || echo "unknown")
echo "  hub commit: $HUB_COMMIT"

echo "=== akai-net hub starting ==="
echo "  model:   $MODEL_PATH"
echo "  alias:   $MODEL_ALIAS"
echo "  context: $CTX_SIZE tokens"
echo "  port:    $SERVER_PORT"
echo "  healthd: $HEALTHD_PORT"

WAITED=0
until [ -f "$MODEL_PATH" ]; do
    echo "  Waiting for model at $MODEL_PATH... (${WAITED}s elapsed)"
    sleep 10
    WAITED=$((WAITED + 10))
    if [ $WAITED -gt 600 ]; then
        echo "ERROR: Model not found after 10 minutes."
        echo "       Set AKAI_MODEL_FILENAME in .secrets and ensure the file"
        echo "       exists in the akai-models volume."
        exit 1
    fi
done
echo "✓ Model ready ($(du -sh "$MODEL_PATH" | cut -f1))"

post_hub_info() {
    for i in $(seq 1 3); do
        curl -sf -X POST \
            -H "X-Worker-Key: $WORKER_KEY" \
            -H "Content-Type: application/json" \
            "${QUEUE_URL}/hub-info" \
            -d "{\"hub_commit\": \"${HUB_COMMIT}\"}" >/dev/null 2>&1 && break
        sleep 2
    done
}

read_rpc_from_state() {
    if [ -f "$STATE_FILE" ]; then
        jq -r '[.[] | .wg_ip + ":" + (.port | tostring)] | join(",")' "$STATE_FILE" 2>/dev/null || echo ""
    else
        echo ""
    fi
}

build_args() {
    local rpc_string="$1"
    ARGS=(
        --model    "$MODEL_PATH"
        --alias    "$MODEL_ALIAS"
        --host     0.0.0.0
        --port     "$SERVER_PORT"
        --ctx-size "$CTX_SIZE"
        -ngl       99
        -fit       off
    )
    if [ -n "$rpc_string" ]; then
        ARGS+=(--rpc "$rpc_string")
    fi
}

start_llama() {
    local rpc_string="$1"
    if [ -z "$rpc_string" ]; then
        echo "✘ No RPC workers online — llama-server not started"
        return 1
    fi
    build_args "$rpc_string"
    echo ""
    echo "→ Starting llama-server with RPC: $rpc_string"
    echo "  exec: llama-server ${ARGS[*]}"
    llama-server "${ARGS[@]}" 2>&1 &
    LLAMA_PID=$!
    echo "  PID: $LLAMA_PID"
}

stop_llama() {
    if [ -n "${LLAMA_PID:-}" ] && kill -0 "$LLAMA_PID" 2>/dev/null; then
        echo "→ Stopping llama-server (PID $LLAMA_PID)"
        kill "$LLAMA_PID" 2>/dev/null || true
        local waited=0
        while kill -0 "$LLAMA_PID" 2>/dev/null && [ $waited -lt 30 ]; do
            sleep 1
            waited=$((waited + 1))
        done
        if kill -0 "$LLAMA_PID" 2>/dev/null; then
            echo "  Force killing llama-server"
            kill -9 "$LLAMA_PID" 2>/dev/null || true
        fi
        wait "$LLAMA_PID" 2>/dev/null || true
    fi
    LLAMA_PID=""
}

echo "→ Starting healthd on :$HEALTHD_PORT"
python3 /app/healthd.py "$HEALTHD_PORT" &
HEALTHD_PID=$!
sleep 1
if ! kill -0 "$HEALTHD_PID" 2>/dev/null; then
    echo "ERROR: healthd failed to start"
    exit 1
fi
echo "✓ healthd started (PID $HEALTHD_PID)"

post_hub_info

CURRENT_RPC=""
CONSECUTIVE_EMPTY=0
echo ""
echo "=== Starting worker monitor (checking state every ${POLL_INTERVAL}s) ==="

while true; do
    NEW_RPC=$(read_rpc_from_state)

    if [ "$NEW_RPC" != "$CURRENT_RPC" ]; then
        if [ -z "$NEW_RPC" ]; then
            CONSECUTIVE_EMPTY=$((CONSECUTIVE_EMPTY + 1))
            if [ $CONSECUTIVE_EMPTY -lt 3 ]; then
                echo "  (worker loss ${CONSECUTIVE_EMPTY}/3 — not restarting yet)"
                sleep "$POLL_INTERVAL"
                continue
            fi
        else
            CONSECUTIVE_EMPTY=0
        fi

        echo ""
        echo "⚡ Workers changed:"
        echo "   was: '${CURRENT_RPC:-(none)}'"
        echo "   now: '${NEW_RPC:-(none)}'"

        stop_llama

        if [ -n "$NEW_RPC" ]; then
            if start_llama "$NEW_RPC"; then
                sleep 10
                if ! kill -0 "$LLAMA_PID" 2>/dev/null; then
                    echo "✘ llama-server exited early — will retry on next poll"
                    stop_llama
                    NEW_RPC=""
                fi
            fi
        fi

        CURRENT_RPC="$NEW_RPC"
    else
        CONSECUTIVE_EMPTY=0
    fi

    if [ -n "${LLAMA_PID:-}" ] && ! kill -0 "$LLAMA_PID" 2>/dev/null; then
        echo "✘ llama-server died unexpectedly — clearing RPC state"
        LLAMA_PID=""
        CURRENT_RPC=""
    fi

    sleep "$POLL_INTERVAL"
done