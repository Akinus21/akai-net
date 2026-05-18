# AGENTS.md — akai-net

## Identity
This repo is `Akinus21/akai-net`.
It contains the Docker image for the akai-net llama.cpp hub container.
The image is published to `ghcr.io/akinus21/akai-net` via GitHub Actions on every push to `main`.

## SSH Key
All git operations use the SSH key at `/config/.ssh/github`.

Always push like this:
```bash
GIT_SSH_COMMAND="ssh -i /config/.ssh/github" git push origin main
```

Or set it for the session:
```bash
export GIT_SSH_COMMAND="ssh -i /config/.ssh/github"
```

## Workflow — Always Push When Done
After every meaningful change, push to main:
```bash
GIT_SSH_COMMAND="ssh -i /config/.ssh/github" git push origin main
```
CI will run automatically. The build takes ~10 minutes (compiles llama.cpp from source).
You will receive build results via webhook.

## Repository Structure
akai-net/
├── .github/workflows/build.yml   ← CI: build, push to GHCR, tag, webhook
├── Dockerfile                    ← Two-stage: builder (cmake) + runtime
├── entrypoint.sh                 ← Container startup: wait for model, discover workers, launch llama-server
├── switch-model.sh               ← Installed as /usr/local/bin/switch-model inside container
├── AGENTS.md                     ← This file
└── README.md

## What This Image Does
- Runs `llama-server` (from llama.cpp) compiled with `-DGGML_RPC=ON`
- On startup, queries `ollama-queue` for live RPC worker endpoints
- Launches `llama-server --model <gguf> --rpc <worker-ips> -ngl 99`
- Exposes OpenAI-compatible `/v1/chat/completions` on port 8080
- Falls back to CPU-only if no workers are connected

## Key Design Facts
- The official `ghcr.io/ggml-org/llama.cpp:server` image does NOT support `--rpc`
  (compiled without GGML_RPC=ON). This is why we build our own image.
- The hub holds the GGUF model file. Workers hold NO model file.
- Workers run `rpc-server` (separate binary, also from llama.cpp).
- RPC traffic travels only over WireGuard VPN (10.8.0.0/24).
- `llama-server` reads `--rpc` at startup only — restart the container to pick up new workers.

## Environment Variables
| Variable | Default | Purpose |
|---|---|---|
| `QUEUE_URL` | `http://ollama-queue:8000` | Where to discover live workers |
| `WORKER_KEY` | (required) | Auth key for ollama-queue `/workers` endpoint |
| `MODEL_PATH` | `/models/model.gguf` | Full path to GGUF inside container |
| `MODEL_ALIAS` | `akai-model` | Model name exposed in `/v1/models` |
| `CTX_SIZE` | `8192` | Context window size in tokens |
| `SERVER_PORT` | `8080` | Port llama-server binds to |

## Switching Models
From the VPS (outside the container):
```bash
# Download from HuggingFace
docker exec akai-net switch-model https://huggingface.co/.../model.gguf

# Use a file already in the volume
docker exec akai-net switch-model -f /models/existing.gguf
```
`switch-model` downloads the file, updates `.secrets`, then kills PID 1 so Docker restarts the container with the new model.

## CI/CD
- **Trigger:** every push to `main`
- **What it does:** builds image → pushes to GHCR → creates git tag → creates GitHub release → notifies webhook
- **Image tags:** `ghcr.io/akinus21/akai-net:latest` and `ghcr.io/akinus21/akai-net:<version>`
- **Webhook endpoint:** `https://webhook.akinus21.com/webhook/akai-net-build`
- **On failure:** webhook fires with `event: build_failure` and a link to the failed run
- **On success:** webhook fires with `event: build_success`, the new tag, and the image name

## Required GitHub Secrets
These must be set in the repo Settings → Secrets → Actions:
| Secret | Purpose |
|---|---|
| `GH_TOKEN` | PAT with `contents:write` and `packages:write` for tagging + GHCR push |
| `WEBHOOK_HMAC_SECRET` | Shared secret for HMAC-signing webhook payloads |

## Deployment
The image is consumed by `ollama-stack` on the Hetzner VPS:
```
~/dockge-stacks/ollama-stack/
```
After a successful build, update the stack to pull the new image:
```bash
cd ~/dockge-stacks/ollama-stack
docker compose pull akai-net
docker compose up -d akai-net
```

## Build Notes
- Build takes ~10 minutes — llama.cpp compiles from source
- GitHub Actions build cache (type=gha) is enabled — subsequent builds are faster
- `LLAMACPP_VERSION=master` tracks latest llama.cpp; pin to a tag if stability is needed
  e.g. `LLAMACPP_VERSION=b4444`