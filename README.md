# akai-net

Docker image for the akai-net llama.cpp hub container. Compiled with `-DGGML_RPC=ON` to support distributed inference over RPC workers.

## Image

`ghcr.io/akinus21/akai-net:latest`

## Features

- `llama-server` compiled with RPC support (`-DGGML_RPC=ON`)
- Discovers live RPC workers via `ollama-queue`
- Falls back to CPU-only if no workers are available
- OpenAI-compatible API on port 8080

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `QUEUE_URL` | `http://ollama-queue:8000` | Worker discovery endpoint |
| `WORKER_KEY` | (required) | Auth key for ollama-queue |
| `MODEL_PATH` | `/models/model.gguf` | Path to GGUF model |
| `MODEL_ALIAS` | `akai-model` | Name in `/v1/models` |
| `CTX_SIZE` | `8192` | Context window size |
| `SERVER_PORT` | `8080` | Server port |

## Switching Models

```bash
docker exec akai-net switch-model https://huggingface.co/.../model.gguf
docker exec akai-net switch-model -f /models/existing.gguf
```

## CI/CD

Every push to `main` triggers a build and push to GHCR. See [AGENTS.md](AGENTS.md) for details.