# AGENTS.md — akai-net

## Identity
This repo is `Akinus21/akai-net`.
Contains the Rust-based distributed layer pipeline hub. Published to `ghcr.io/akinus21/akai-net` via GitHub Actions.

## SSH Key
All git operations use the SSH key at `/config/.ssh/github`.

## New Architecture

```
                         ┌──────────────────────────────┐
                         │       akai-net Hub          │
                         │   (Rust + tokio)            │
                         │   OpenAI API :8080           │
                         │   Worker Protocol :50051     │
                         └──────────────┬───────────────┘
                                        │
           ┌────────────────────────────┼────────────────────────────┐
           │                            │                            │
  ┌────────▼─────────┐        ┌────────▼─────────┐        ┌────────▼─────────┐
  │  Desktop Worker │        │    Pi Worker     │        │   Phone Worker   │
  │   (llama.cpp)   │        │    (llama.cpp)  │        │     (Candle)     │
  │      GPU        │        │       CPU        │        │       CPU        │
  │  layers 0-15    │        │  layers 16-25     │        │  layers 26-31     │
  └─────────────────┘        └──────────────────┘        └──────────────────┘
```

Workers report their GPU/CPU capability and the hub routes tokens:
**weakest → strongest** (Pi → Phone → Desktop)

## Repository Structure
```
akai-net/
├── src/
│   ├── hub_main.rs      # Hub server (OpenAI API + worker coordinator)
│   ├── worker_main.rs   # Standalone worker binary
│   └── pipeline.rs      # Shared protocol types
├── Cargo.toml
├── Dockerfile           # Two-stage: builder + runtime
└── AGENTS.md
```

## Protocol

Workers connect via TCP to port 50051. Protocol is simple JSON messages:

| Message | Direction | Description |
|---------|-----------|-------------|
| `HubMessage::Register` | Worker→Hub | Worker announces capabilities |
| `HubMessage::Heartbeat` | Worker→Hub | Periodic alive check |
| `HubMessage::InferenceRequest` | Hub→Worker | Tokens to process |
| `HubMessage::InferenceResponse` | Worker→Hub | Token + hidden states |

Layer assignment is calculated by hub based on worker capabilities:
- Workers sorted by score: `if gpu { vram_gb * 100 } else { 1 }`
- First worker (weakest) gets first layers, last worker (strongest) gets last layers

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `MODEL_NAME` | `unknown` | Model name/HF stub (e.g., `hf.co/author/model:Q5_K_S`) |
| `MODEL_LAYERS` | `32` | Total layers in model |
| `HIDDEN_SIZE` | `4096` | Hidden state dimension |
| `HUB_PORT` | `8080` | HTTP API port |
| `WORKER_PORT` | `50051` | Worker protocol port |
| `ADMIN_KEY` | (none) | Bearer token for admin API |
| `ADMIN_USERS` | `akinus` | Comma-separated list of authorized usernames |

## Admin API

```bash
# Hot-swap model
curl -X POST http://localhost:8080/admin/model \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"username": "akinus", "name": "hf.co/author/model:Q5_K_S", "layers": 32, "url": "hf.co/author/model:Q5_K_S"}'
```

## Build & Deployment

```bash
# Build locally
cargo build --release

# Or use Docker
docker build -t akai-net .
docker run -p 8080:8080 -p 50051:50051 \
  -e ADMIN_KEY=your-secret-key \
  -e ADMIN_USERS=akinus,otheruser \
  -e MODEL_NAME=hf.co/author/model:Q5_K_S \
  -e MODEL_LAYERS=32 \
  akai-net
```

## CI/CD

- **Trigger:** every push to `main`
- **Build:** Rust compilation + Docker push to GHCR
- **Tags:** `ghcr.io/akinus21/akai-net:latest` and version tag
- **Webhook:** notifies on success/failure

## Worker Implementation

| Device | Backend | Notes |
|--------|---------|-------|
| Desktop | llama.cpp | Full GPU, layers 0-15 |
| Pi | llama.cpp | CPU, layers 16-25 |
| Phone | Candle (Rust) | CPU only, layers 26-31 |

Workers implement `HubMessage` protocol to connect. See `worker_main.rs` for reference implementation.

## Infrastructure Notes

### Docker Socket Access
The agent has access to the Docker socket at `/var/run/docker.sock` to run docker commands directly.

### Port Routing Architecture
```
Internet → Caddy (443) → akai-net:50051 (worker protocol, no direct host exposure)
                     └→ tunnel-server:50053 (mTLS tunnel for workers)
```

- Port 50051 should **NOT** be directly exposed to host (remove `- "50051:50051"` from compose)
- All worker traffic routes through Caddy layer4 proxy
- akai-hub.akinus21.com uses `reverse_proxy tcp/akai-net:50051` for raw TCP passthrough
- tunnel.akinus21.com routes to tunnel-server:50053 for mTLS tunnel workers
- CrowdSec protection applied via `import secure` directive in Caddy