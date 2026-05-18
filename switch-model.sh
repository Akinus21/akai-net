#!/bin/bash
set -euo pipefail

MODELS_DIR="/models"
SECRETS_FILE="/app/.secrets"

DEFAULT_QUANTS=("Q4_K_M" "Q5_K_M" "Q6_K" "Q4_K_S" "Q8_0" "Q4_0")

usage() {
    echo ""
    echo "Usage:"
    echo "  switch-model pull hf.co/<author>/<repo>          (default quant)"
    echo "  switch-model pull hf.co/<author>/<repo>:<QUANT>  (specific quant)"
    echo "  switch-model -f                        (file in /models)"
    echo ""
    echo "Examples:"
    echo "  switch-model pull hf.co/bartowski/gemma-4-E2B-it-GGUF"
    echo "  switch-model pull hf.co/bartowski/gemma-4-E2B-it-GGUF:Q6_K"
    echo "  switch-model pull huggingface.co/bartowski/gemma-4-E2B-it-GGUF:Q6_K"
    echo "  switch-model -f gemma-4-E2B-it-Q6_K.gguf"
    echo ""
    exit 1
}

check_secrets() {
    if [ ! -f "$SECRETS_FILE" ]; then
        echo "ERROR: $SECRETS_FILE not found."
        echo "  Add to akai-net in compose.yml:"
        echo "    volumes:"
        echo "      - /home/akinus/dockge-stacks/ollama-stack/.secrets:/app/.secrets"
        exit 1
    fi
}

update_secret() {
    local KEY="$1" VAL="$2"
    if grep -q "^${KEY}=" "$SECRETS_FILE"; then
        sed -i "s|^${KEY}=.*|${KEY}=${VAL}|" "$SECRETS_FILE"
    else
        echo "${KEY}=${VAL}" >> "$SECRETS_FILE"
    fi
}

current_model() {
    grep "^AKAI_MODEL_FILENAME=" "$SECRETS_FILE" 2>/dev/null | cut -d= -f2 || echo ""
}

derive_alias() {
    local filename="$1"
    echo "$filename" | sed 's/\.gguf$//' \
        | sed 's/-[IiQq][^-]*$//' \
        | tr '[:upper:]' '[:lower:]'
}

hf_list_gguf_files() {
    local author="$1" repo="$2"
    curl -sf "https://huggingface.co/api/models/${author}/${repo}" \
        | jq -r '.siblings[]?.rfilename | select(endswith(".gguf"))' 2>/dev/null || true
}

hf_find_by_quant() {
    local files="$1" quant="$2"
    echo "$files" | grep -i "${quant}" | head -1 || true
}

hf_find_default_quant() {
    local files="$1"
    for q in "${DEFAULT_QUANTS[@]}"; do
        local match
        match=$(echo "$files" | grep -i "${q}" | head -1 || true)
        if [ -n "$match" ]; then
            echo "$match"
            return
        fi
    done
    echo "$files" | head -1
}

do_switch() {
    local filepath="$1"
    local filename
    filename=$(basename "$filepath")

    check_secrets

    local current
    current=$(current_model)
    if [ "$current" = "$filename" ]; then
        echo ""
        echo "⚠  Model is already active: $filename"
        echo "   No restart needed."
        exit 0
    fi

    local alias
    alias=$(derive_alias "$filename")

    update_secret "AKAI_MODEL_FILENAME" "$filename"
    update_secret "AKAI_MODEL_ALIAS"    "$alias"

    echo ""
    echo "✓ Updated .secrets:"
    echo "    AKAI_MODEL_FILENAME=$filename"
    echo "    AKAI_MODEL_ALIAS=$alias"
    echo ""
    echo "→ Restarting akai-net to load new model..."
    echo "  Container will be back in ~30s."
    kill 1
}

cmd_local() {
    local filename="$1"
    filename=$(basename "$filename")
    local filepath="$MODELS_DIR/$filename"

    if [ ! -f "$filepath" ]; then
        echo ""
        echo "ERROR: '$filename' not found in /models."
        echo ""
        echo "  To download it from HuggingFace, run:"
        echo "    docker exec akai-net switch-model pull hf.co/<author>/<repo>:<quant>"
        echo ""
        echo "  Files currently in /models:"
        ls "$MODELS_DIR"/*.gguf 2>/dev/null | xargs -n1 basename || echo "    (none)"
        echo ""
        exit 1
    fi

    echo "✓ Found: $filename ($(du -sh "$filepath" | cut -f1))"
    do_switch "$filepath"
}

cmd_pull() {
    local ref="$1"

    ref="${ref#hf.co/}"
    ref="${ref#huggingface.co/}"

    local author_repo quant
    if [[ "$ref" == *:* ]]; then
        author_repo="${ref%%:*}"
        quant="${ref##*:}"
    else
        author_repo="$ref"
        quant=""
    fi

    local author repo
    author="${author_repo%%/*}"
    repo="${author_repo##*/}"

    if [ -z "$author" ] || [ -z "$repo" ] || [ "$author" = "$repo" ]; then
        echo "ERROR: Invalid format. Expected hf.co/<author>/<repo> or hf.co/<author>/<repo>:<quant>"
        usage
    fi

    echo "→ Looking up files in hf.co/$author/$repo ..."
    local files
    files=$(hf_list_gguf_files "$author" "$repo")

    if [ -z "$files" ]; then
        echo ""
        echo "ERROR: No .gguf files found in hf.co/$author/$repo"
        echo "  Check the repo exists and contains GGUF files:"
        echo "    https://huggingface.co/$author/$repo"
        exit 1
    fi

    local target_file
    if [ -n "$quant" ]; then
        target_file=$(hf_find_by_quant "$files" "$quant")
        if [ -z "$target_file" ]; then
            echo ""
            echo "ERROR: No file matching quant '$quant' found in hf.co/$author/$repo"
            echo ""
            echo "  Available quants:"
            echo "$files" | sed 's/^/    /'
            exit 1
        fi
    else
        target_file=$(hf_find_default_quant "$files")
        if [ -z "$target_file" ]; then
            echo "ERROR: Could not select a default quant from hf.co/$author/$repo"
            exit 1
        fi
        echo "  No quant specified — selected: $target_file"
        echo "  (Use hf.co/$author/$repo:<QUANT> to specify. Available: $(echo "$files" | xargs))"
    fi

    local dest="$MODELS_DIR/$target_file"

    if [ -f "$dest" ]; then
        echo "✓ Already downloaded: $target_file ($(du -sh "$dest" | cut -f1))"
    else
        local download_url="https://huggingface.co/$author/$repo/resolve/main/$target_file"
        echo "→ Downloading $target_file ..."
        echo "  From: $download_url"
        echo "  To:   $dest"
        echo ""
        curl -L --progress-bar -o "$dest" "$download_url"
        echo ""
        echo "✓ Download complete ($(du -sh "$dest" | cut -f1))"
    fi

    do_switch "$dest"
}

[ $# -eq 0 ] && usage

case "$1" in
    pull)
        [ $# -lt 2 ] && usage
        cmd_pull "$2"
        ;;
    -f)
        [ $# -lt 2 ] && usage
        cmd_local "$2"
        ;;
    *)
        echo "ERROR: Unknown command '$1'"
        usage
        ;;
esac