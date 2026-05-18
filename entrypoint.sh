#!/bin/bash
set -euo pipefail

QUEUE_URL="${QUEUE_URL:-http://ollama-queue:8000}"
WORKER_KEY="${WORKER_KEY:?WORKER_KEY env var is required}"
MODEL_PATH="${MODEL_PATH:-/models/model.gguf}"
MODEL_ALIAS="${MODEL_ALIAS:-akai-model}"
CTX_SIZE="${CTX_SIZE:-8192}"
SERVER_PORT="${SERVER_PORT:-8080}"

echo "=== akai-net hub starting ==="
echo "  model:   $MODEL_PATH"
echo "  alias:   $MODEL_ALIAS"
echo "  context: $CTX_SIZE tokens"
echo "  port:    $SERVER_PORT"

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

RPC_STRING=""
WORKER_COUNT=0
for i in $(seq 1 12); do
    RESPONSE=$(curl -sf \
        -H "X-Worker-Key: $WORKER_KEY" \
        "${QUEUE_URL}/workers" 2>/dev/null || echo '{}')
    RPC_STRING=$(echo "$RESPONSE"  | jq -r '.rpc_string // ""')
    WORKER_COUNT=$(echo "$RESPONSE" | jq -r '.workers | length' 2>/dev/null || echo 0)
    if [ -n "$RPC_STRING" ]; then
        echo "✓ Found $WORKER_COUNT live worker(s): $RPC_STRING"
        break
    fi
    echo "  No live workers yet (attempt $i/12) — retrying in 10s..."
    sleep 10
done

ARGS=(
    --model    "$MODEL_PATH"
    --alias    "$MODEL_ALIAS"
    --host     0.0.0.0
    --port     "$SERVER_PORT"
    --ctx-size "$CTX_SIZE"
    -ngl       99
)

if [ -n "$RPC_STRING" ]; then
    ARGS+=(--rpc "$RPC_STRING")
    echo "→ Starting with RPC workers: $RPC_STRING"
else
    echo "⚠  No workers found — CPU-only mode (slow)."
    echo "   Connect a worker then: docker restart akai-net"
fi

echo ""
echo "→ exec: llama-server ${ARGS[*]}"
exec llama-server "${ARGS[@]}"