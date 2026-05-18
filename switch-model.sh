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

        echo "  Searching for *${VARIANT}*.gguf in ${USER}/${REPO}..."
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
    else
        echo "→ Downloading $FILENAME..."
        curl -L --progress-bar -o "$DEST" "$URL"
        echo ""
        echo "✓ Download complete ($(du -sh "$DEST" | cut -f1))"
    fi
    MODEL_PATH="$DEST"
fi

RAW=$(basename "$MODEL_PATH" .gguf)
ALIAS=$(echo "$RAW" | sed 's/-[Qq][0-9][^-]*$//' | tr '[:upper:]' '[:lower:]')
echo "  Alias: $ALIAS"

[ ! -f "$SECRETS_FILE" ] && \
    echo "ERROR: $SECRETS_FILE not found — is it bind-mounted?" && exit 1

update_secret() {
    local KEY="$1" VAL="$2"
    if grep -q "^${KEY}=" "$SECRETS_FILE"; then
        sed -i "s|^${KEY}=.*|${KEY}=${VAL}|" "$SECRETS_FILE"
    else
        echo "${KEY}=${VAL}" >> "$SECRETS_FILE"
    fi
}

update_secret "AKAI_MODEL_FILENAME" "$(basename "$MODEL_PATH")"
update_secret "AKAI_MODEL_ALIAS"    "$ALIAS"
echo "✓ Updated .secrets"
echo "    AKAI_MODEL_FILENAME=$(basename "$MODEL_PATH")"
echo "    AKAI_MODEL_ALIAS=$ALIAS"

echo "→ Restarting akai-net..."
kill 1