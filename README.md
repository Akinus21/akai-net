# akai-net

Docker image for the akai-net llama.cpp hub. Compiled with `-DGGML_RPC=ON` to support distributed inference over RPC workers.

## Image

`ghcr.io/akinus21/akai-net:latest`

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `QUEUE_URL` | `http://ollama-queue:8000` | Worker discovery endpoint |
| `WORKER_KEY` | (required) | Auth key for worker discovery |
| `MODEL_PATH` | `/models/model.gguf` | Path to GGUF model file |
| `MODEL_ALIAS` | `akai-model` | Model name in `/v1/models` |
| `CTX_SIZE` | `8192` | Context window size |
| `SERVER_PORT` | `8080` | Server port |

## Switching Models

```bash
# Download from HuggingFace
docker exec akai-net switch-model https://huggingface.co/.../model.gguf

# Use existing file
docker exec akai-net switch-model -f /models/existing.gguf
```