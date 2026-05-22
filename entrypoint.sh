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

HUB_COMMIT=$(llama-server --version 2>&1 | grep -oP '\([a-f0-9]+\)' | tr -d '()' || echo "unknown")
echo "  hub commit: $HUB_COMMIT"

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
    echo "  worker query returned: $(echo "$RESPONSE" | jq -r '.workers | length' 2>/dev/null || echo '?') workers"
    RPC_STRING=$(echo "$RESPONSE" | jq -r '[.workers[] | select(.online == true) | .wg_ip] | map(. + ":50052") | join(",")]' 2>/dev/null || echo "")
    WORKER_COUNT=$(echo "$RESPONSE" | jq -r '.workers | length' 2>/dev/null || echo 0)
    if [ -n "$RPC_STRING" ]; then
        echo "✓ Found $WORKER_COUNT live worker(s): $RPC_STRING"
        break
    fi
    echo "  No live workers yet (attempt $i/12) — retrying in 10s..."
    sleep 10
done

for i in $(seq 1 3); do
    curl -sf -X POST \
        -H "X-Worker-Key: $WORKER_KEY" \
        -H "Content-Type: application/json" \
        "${QUEUE_URL}/hub-info" \
        -d "{\"hub_commit\": \"${HUB_COMMIT}\"}" >/dev/null 2>&1 && break
    sleep 2
done

ARGS=(
    --model    "$MODEL_PATH"
    --alias    "$MODEL_ALIAS"
    --host     0.0.0.0
    --port     "$SERVER_PORT"
    --ctx-size "$CTX_SIZE"
    -ngl       99
    -fit       off
)

if [ -n "$RPC_STRING" ]; then
    ARGS+=(--rpc "$RPC_STRING")
    echo "→ Starting with RPC workers: $RPC_STRING"
else
    echo "✘ No workers found — cannot start without GPU workers."
    echo "   Waiting for workers..."
    exit 0
fi

echo ""
echo "→ exec: llama-server ${ARGS[*]}"

llama-server "${ARGS[@]}" 2>&1 &
LLAMA_PID=$!

sleep 30

if ! kill -0 $LLAMA_PID 2>/dev/null; then
    echo ""
    echo "✘ llama-server exited early — RPC connection likely failed."
    echo "   Check that worker rpc-server protocol matches hub commit: $HUB_COMMIT"
    exit 1
fi

wait $LLAMA_PID