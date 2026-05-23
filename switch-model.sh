#!/bin/bash
set -euo pipefail

MODELS_DIR="/models"
SECRETS_FILE="/app/.secrets"

usage() {
    echo ""
    echo "Usage:"
    echo "  switch-model <url>        Download GGUF from URL"
    echo "  switch-model -f <path>    Use file already in /models"
    echo ""
    echo "Examples:"
    echo "  switch-model https://huggingface.co/.../Qwen2.5-32B-Q4_K_M.gguf"
    echo "  switch-model hf.co/unsloth/gemma-4-E2B-it-GGUF:Q6_K"
    echo "  switch-model -f /models/my-model.gguf"
    echo ""
    exit 1
}

resolve_hf_url() {
    local REF="$1"
    local HF_URL="https://huggingface.co"

    if [[ "$REF" =~ ^hf\.co/([^/]+)/([^:]+):(.+)$ ]]; then
        local USER="${BASH_REMATCH[1]}"
        local REPO="${BASH_REMATCH[2]}"
        local VARIANT="${BASH_REMATCH[3]}"
        local TREE_URL="${HF_URL}/api/models/${USER}/${REPO}/tree/main?recursive=true"

        echo "  Searching for *${VARIANT}*.gguf in ${USER}/${REPO}..." >&2
        FILENAME=$(curl -sL "$TREE_URL" | \
            jq -r '.[] | select(.type=="file") | select(.path | test("'"${VARIANT}"'";"i")) | .path' | head -1)

        if [[ -z "$FILENAME" ]] || [[ "$FILENAME" == *"null"* ]]; then
            echo "ERROR: Could not find *${VARIANT}*.gguf in ${USER}/${REPO}"
            exit 1
        fi
        echo "${HF_URL}/${USER}/${REPO}/resolve/main/${FILENAME}"
    else
        echo "$REF"
    fi
}

[ $# -eq 0 ] && usage

if [ "${1}" = "-f" ]; then
    [ $# -lt 2 ] && usage
    MODEL_PATH="$2"
    [ ! -f "$MODEL_PATH" ] && echo "ERROR: Not found: $MODEL_PATH" && exit 1
    FILENAME=$(basename "$MODEL_PATH")
    echo "✓ Using existing file: $FILENAME ($(du -sh "$MODEL_PATH" | cut -f1))"
else
    URL=$(resolve_hf_url "$1")
    FILENAME=$(basename "$URL" | cut -d'?' -f1)
    [[ "$FILENAME" != *.gguf ]] && \
        echo "ERROR: URL must point to a .gguf file (got: $FILENAME)" && exit 1

    DEST="$MODELS_DIR/$FILENAME"
    if [ -f "$DEST" ]; then
        echo "✓ Already downloaded: $FILENAME ($(du -sh "$DEST" | cut -f1))"
        MODEL_PATH="$DEST"
    else
        echo "→ Downloading $FILENAME..."
        curl -L --progress-bar -o "$DEST" "$URL"
        echo ""
        echo "✓ Download complete ($(du -sh "$DEST" | cut -f1))"
        MODEL_PATH="$DEST"
    fi
fi

RAW=$(basename "$MODEL_PATH" .gguf)
ALIAS=$(echo "$RAW" | sed 's/-[Qq][0-9][^-]*$//' | tr '[:upper:]' '[:lower:]')
echo "  Alias: $ALIAS"

get_model_ctx_size() {
    local MODEL="$1"
    local CTX

    CTX=$(timeout 30s llama-server -m "$MODEL" --log-disable -ngl 99 --host 127.0.0.1 --port 9999 2>&1 | \
        grep -iE 'n_ctx|context.*size' | head -1 | grep -oE '[0-9]+' | head -1)

    if [ -z "$CTX" ]; then
        CTX=$(timeout 30s curl -s http://127.0.0.1:9999/v1/models 2>/dev/null | \
            jq -r '.data[0].meta.n_ctx // empty' 2>/dev/null)
    fi

    echo "${CTX:-8192}"
}

get_worker_vram_gb() {
    local RESPONSE=$(curl -sf -H "X-Worker-Key: ${WORKER_KEY:-}" "${QUEUE_URL:-http://ollama-queue:11433}/workers" 2>/dev/null)
    echo "$RESPONSE" | jq -r '[.workers[] | select(.online == true) | .vram_gb] | add // 0' 2>/dev/null
}

echo "  Probing context size..."
MODEL_CTX=$(get_model_ctx_size "$MODEL_PATH")
echo "  Model native context: $MODEL_CTX tokens"

WORKER_VRAM=$(get_worker_vram_gb 2>/dev/null || echo 0)
echo "  Worker VRAM available: ${WORKER_VRAM} GB"

CTX_SIZE="$MODEL_CTX"

echo "  Context size: $CTX_SIZE tokens"

[ ! -f "$SECRETS_FILE" ] && \
    echo "ERROR: $SECRETS_FILE not found — is it bind-mounted?" && exit 1

update_secret() {
    local KEY="$1" VAL="$2"
    if grep -q "^${KEY}=" "$SECRETS_FILE"; then
        grep -v "^${KEY}=" "$SECRETS_FILE" > "${SECRETS_FILE}.tmp"
        echo "${KEY}=${VAL}" >> "${SECRETS_FILE}.tmp"
        cat "${SECRETS_FILE}.tmp" > "$SECRETS_FILE"
        rm "${SECRETS_FILE}.tmp"
    else
        echo "${KEY}=${VAL}" >> "$SECRETS_FILE"
    fi
}

OLD_FILE=$(grep "^AKAI_MODEL_FILENAME=" "$SECRETS_FILE" 2>/dev/null | cut -d= -f2 || true)

update_secret "AKAI_MODEL_FILENAME" "$(basename "$MODEL_PATH")"
update_secret "AKAI_MODEL_ALIAS"    "$ALIAS"
update_secret "AKAI_CTX_SIZE"      "$CTX_SIZE"
echo "✓ Updated .secrets"
echo "    AKAI_MODEL_FILENAME=$(basename "$MODEL_PATH")"
echo "    AKAI_MODEL_ALIAS=$ALIAS"
echo "    AKAI_CTX_SIZE=$CTX_SIZE"

if [ -n "$OLD_FILE" ] && [ "$OLD_FILE" != "$(basename "$MODEL_PATH")" ] && [ -f "$MODELS_DIR/$OLD_FILE" ]; then
    echo "→ Removing old model: $OLD_FILE ($(du -sh "$MODELS_DIR/$OLD_FILE" | cut -f1))"
    rm -f "$MODELS_DIR/$OLD_FILE"
    echo "✓ Old model deleted"
fi

echo "→ Restarting akai-net..."
kill 1