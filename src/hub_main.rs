mod pipeline;

use anyhow::Result;
use pipeline::{HubMessage, WorkerInfo, ModelConfig, calculate_layer_assignment};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, warn, error};

type WorkerMap = Arc<RwLock<HashMap<String, WorkerConnection>>>;

struct WorkerConnection {
    stream: TcpStream,
    info: WorkerInfo,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .init();

    let hub_port: u16 = std::env::var("HUB_PORT").unwrap_or_else(|_| "8080".to_string()).parse().unwrap_or(8080);
    let worker_port: u16 = std::env::var("WORKER_PORT").unwrap_or_else(|_| "50051".to_string()).parse().unwrap_or(50051);

    let model = ModelConfig {
        name: std::env::var("MODEL_NAME").unwrap_or_else(|_| "unknown".to_string()),
        num_layers: std::env::var("MODEL_LAYERS").unwrap_or_else(|_| "32".to_string()).parse().unwrap_or(32),
        hidden_size: std::env::var("HIDDEN_SIZE").unwrap_or_else(|_| "4096".to_string()).parse().unwrap_or(4096),
        num_heads: 32,
        vocab_size: 32000,
    };

    info!("Akai-Net Hub starting...");
    info!("Model: {} ({} layers)", model.name, model.num_layers);
    info!("HTTP API: 0.0.0.0:{}", hub_port);
    info!("Worker protocol: 0.0.0.0:{}", worker_port);

    let workers: WorkerMap = Arc::new(RwLock::new(HashMap::new()));
    let model = Arc::new(model);

    // Worker protocol server
    let worker_workers = workers.clone();
    let worker_model = model.clone();
    tokio::spawn(async move {
        let listener = match TcpListener::bind(format!("0.0.0.0:{}", worker_port)).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind worker port {}: {}", worker_port, e);
                return;
            }
        };
        info!("Worker protocol server listening on 0.0.0.0:{}", worker_port);

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let workers = worker_workers.clone();
                    let model = worker_model.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_worker_connection(stream, addr, workers, model).await {
                            error!("Worker connection error: {}", e);
                        }
                    });
                }
                Err(e) => error!("Failed to accept worker connection: {}", e),
            }
        }
    });

    // HTTP API server (simple implementation)
    start_http_server(hub_port, workers, model).await?;

    Ok(())
}

async fn handle_worker_connection(
    mut stream: TcpStream,
    addr: std::net::SocketAddr,
    workers: WorkerMap,
    _model: Arc<ModelConfig>,
) -> Result<()> {
    let mut buf = vec![0u8; 65536];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }

    let message: HubMessage = match serde_json::from_slice(&buf[..n]) {
        Ok(m) => m,
        Err(e) => {
            error!("Failed to parse worker message: {}", e);
            return Ok(());
        }
    };

    match message {
        HubMessage::Register(info) => {
            info!(
                "Worker registered: {} (GPU: {}, VRAM: {:.1} GB)",
                info.id,
                info.has_gpu,
                info.vram_gb
            );

            // Store worker with placeholder layer offset (hub will assign)
            let worker_stream = stream;
            {
                let mut workers_guard = workers.write().await;
                workers_guard.insert(info.id.clone(), WorkerConnection {
                    stream: worker_stream.try_clone().await?,
                    info: info.clone(),
                });
            }

            // Calculate layer assignments for all workers
            let assignments = {
                let workers_guard = workers.read().await;
                let worker_list: Vec<_> = workers_guard.values().map(|w| w.info.clone()).collect();
                calculate_layer_assignment(&worker_list, _model.num_layers)
            };

            info!("Layer assignments: {:?}", assignments);

            // Send each worker their assignment
            {
                let mut workers_guard = workers.write().await;
                for (worker_id, layer_offset, num_layers) in &assignments {
                    if let Some(conn) = workers_guard.get_mut(worker_id) {
                        let msg = HubMessage::LayerAssignment {
                            layer_offset: *layer_offset,
                            num_layers: *num_layers,
                        };
                        let data = serde_json::to_vec(&msg)?;
                        if let Err(e) = conn.stream.write_all(&data).await {
                            error!("Failed to send assignment to {}: {}", worker_id, e);
                        } else {
                            info!("Sent assignment to {}: layers {} to {}",
                                worker_id, layer_offset, layer_offset + num_layers);
                            conn.info.layer_offset = *layer_offset;
                            conn.info.num_layers = *num_layers;
                        }
                    }
                }
            }

            Ok(())
        }
        HubMessage::Heartbeat { worker_id, load, active } => {
            info!(
                "Heartbeat from {}: load={:.2}, active={}",
                worker_id, load, active
            );
            Ok(())
        }
        _ => {
            warn!("Unexpected message type from worker");
            Ok(())
        }
    }
}

async fn start_http_server(port: u16, workers: WorkerMap, model: Arc<ModelConfig>) -> Result<()> {
    use tokio::net::TcpListener as HttpListener;

    let listener = HttpListener::bind(format!("0.0.0.0:{}", port)).await?;
    info!("HTTP server listening on 0.0.0.0:{}", port);

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                let mut buf = [0u8; 8192];
                let n = match stream.read(&mut buf).await {
                    Ok(n) => n,
                    Err(_) => continue,
                };

                let request = String::from_utf8_lossy(&buf[..n]).to_string();
                let path = request.lines().next().unwrap_or("");

                let (status, body) = if path.starts_with("GET /health") {
                    let workers_guard = workers.blocking_read();
                    let worker_list: Vec<_> = workers_guard
                        .values()
                        .map(|w| {
                            serde_json::json!({
                                "id": w.info.id,
                                "layers": format!("{}-{}", w.info.layer_offset, w.info.layer_offset + w.info.num_layers),
                                "gpu": w.info.has_gpu,
                                "vram_gb": w.info.vram_gb,
                            })
                        })
                        .collect();
                    let resp = serde_json::json!({
                        "status": "ok",
                        "model": model.name,
                        "workers": worker_list,
                    });
                    (200, serde_json::to_string(&resp).unwrap_or_default())
                } else if path.starts_with("GET /v1/models") {
                    let resp = serde_json::json!({
                        "object": "list",
                        "data": [{
                            "id": model.name,
                            "object": "model",
                            "created": 1234567890,
                            "owned_by": "akai-net",
                        }]
                    });
                    (200, serde_json::to_string(&resp).unwrap_or_default())
                } else if path.starts_with("POST /v1/chat/completions") {
                    let resp = serde_json::json!({
                        "choices": [{
                            "message": {
                                "role": "assistant",
                                "content": "Pipeline hub ready. Awaiting worker connections.",
                            }
                        }]
                    });
                    (200, serde_json::to_string(&resp).unwrap_or_default())
                } else if path.starts_with("POST /v1/completions") {
                    let resp = serde_json::json!({
                        "choices": [{
                            "text": "Pipeline hub ready. Awaiting worker connections.",
                        }]
                    });
                    (200, serde_json::to_string(&resp).unwrap_or_default())
                } else {
                    (404, "{}".to_string())
                };

                let response = format!(
                    "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    status,
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).await?;
            }
            Err(e) => error!("HTTP connection error: {}", e),
        }
    }
}