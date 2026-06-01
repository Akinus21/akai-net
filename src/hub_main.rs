mod pipeline;

use anyhow::Result;
use pipeline::{HubMessage, WorkerInfo, ModelConfig, HeartbeatResponse, calculate_layer_assignment};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, warn, error};

type WorkerMap = Arc<RwLock<HashMap<String, WorkerInfo>>>;

struct HubState {
    model: ModelConfig,
    model_url: String,
}

type HubStateRef = Arc<Mutex<HubState>>;

fn parse_admin_users() -> Vec<String> {
    std::env::var("ADMIN_USERS")
        .unwrap_or_else(|_| "akinus".to_string())
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .init();

    let hub_port: u16 = std::env::var("HUB_PORT").unwrap_or_else(|_| "8080".to_string()).parse().unwrap_or(8080);
    let worker_port: u16 = std::env::var("WORKER_PORT").unwrap_or_else(|_| "50051".to_string()).parse().unwrap_or(50051);
    let admin_key = std::env::var("ADMIN_KEY").unwrap_or_else(|_| "".to_string());
    let admin_users = parse_admin_users();

    info!("Admin users: {}", admin_users.join(", "));

    let state = Arc::new(Mutex::new(HubState {
        model: ModelConfig {
            name: std::env::var("MODEL_NAME").unwrap_or_else(|_| "unknown".to_string()),
            num_layers: std::env::var("MODEL_LAYERS").unwrap_or_else(|_| "32".to_string()).parse().unwrap_or(32),
            hidden_size: std::env::var("HIDDEN_SIZE").unwrap_or_else(|_| "4096".to_string()).parse().unwrap_or(4096),
            num_heads: 32,
            vocab_size: 32000,
        },
        model_url: std::env::var("MODEL_URL").unwrap_or_else(|_| "".to_string()),
    }));

    info!("Akai-Net Hub starting...");
    info!("HTTP API: 0.0.0.0:{}", hub_port);
    info!("Worker protocol: 0.0.0.0:{}", worker_port);

    let workers: WorkerMap = Arc::new(RwLock::new(HashMap::new()));

    // Worker protocol server
    let worker_workers = workers.clone();
    let worker_state = state.clone();
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
                    let state = worker_state.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_worker_connection(stream, addr, workers, state).await {
                            error!("Worker connection error: {}", e);
                        }
                    });
                }
                Err(e) => error!("Failed to accept worker connection: {}", e),
            }
        }
    });

    // HTTP API server
    let http_workers = workers.clone();
    let http_state = state.clone();
    tokio::spawn(async move {
        start_http_server(hub_port, http_workers, http_state, admin_key, admin_users).await
    });

    // Keep main task alive
    tokio::signal::ctrl_c().await.ok();
    info!("Hub shutting down");
    Ok(())
}

async fn handle_worker_connection(
    mut stream: TcpStream,
    addr: std::net::SocketAddr,
    workers: WorkerMap,
    state: HubStateRef,
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

            // Insert worker info
            {
                let mut workers_guard = workers.write().await;
                workers_guard.insert(info.id.clone(), info.clone());
            }

            // Recalculate assignments
            let (layer_offset, num_layers) = {
                let workers_guard = workers.read().await;
                let worker_list: Vec<_> = workers_guard.values().cloned().collect();
                let state_guard = state.lock().await;
                let assignments = calculate_layer_assignment(&worker_list, state_guard.model.num_layers);
                assignments.iter()
                    .find(|(id, _, _)| id == &info.id)
                    .map(|(_, offset, layers)| (*offset, *layers))
                    .unwrap_or((0, 0))
            };

            // Update worker's layer info
            {
                let mut workers_guard = workers.write().await;
                if let Some(conn) = workers_guard.get_mut(&info.id) {
                    conn.layer_offset = layer_offset;
                    conn.num_layers = num_layers;
                }
            }

            // Send heartbeat response with assignment
            let response = HeartbeatResponse {
                layer_offset,
                num_layers,
                reassign: false,
                model_name: state.lock().await.model.name.clone(),
                model_url: state.lock().await.model_url.clone(),
            };
            let msg = HubMessage::HeartbeatResponse(response);
            let data = serde_json::to_vec(&msg)?;
            stream.write_all(&data).await?;

            info!("Sent initial assignment to {}: layers {} to {}", info.id, layer_offset, layer_offset + num_layers);
            Ok(())
        }
        HubMessage::Heartbeat(hb) => {
            let (layer_offset, num_layers, reassign) = {
                let workers_guard = workers.read().await;
                let worker_list: Vec<_> = workers_guard.values().cloned().collect();
                let state_guard = state.lock().await;
                let assignments = calculate_layer_assignment(&worker_list, state_guard.model.num_layers);

                // Check if this worker needs reassignment
                if let Some(current) = workers_guard.get(&hb.worker_id) {
                    if current.layer_offset != hb.layer_offset || current.num_layers != hb.num_layers {
                        // Worker has old assignment, needs reassignment
                        if let Some((_, offset, layers)) = assignments.iter().find(|(id, _, _)| id == &hb.worker_id) {
                            // Update worker info
                            let mut workers_guard = workers.write().await;
                            if let Some(conn) = workers_guard.get_mut(&hb.worker_id) {
                                conn.layer_offset = *offset;
                                conn.num_layers = *layers;
                            }
                            (Some(*offset), Some(*layers), true)
                        } else {
                            (None, None, false)
                        }
                    } else {
                        (None, None, false)
                    }
                } else {
                    (None, None, false)
                }
            };

            // Get model info
            let (model_name, model_url) = {
                let state_guard = state.lock().await;
                (state_guard.model.name.clone(), state_guard.model_url.clone())
            };

            let response = HeartbeatResponse {
                layer_offset: layer_offset.unwrap_or(hb.layer_offset),
                num_layers: num_layers.unwrap_or(hb.num_layers),
                reassign,
                model_name,
                model_url,
            };
            let msg = HubMessage::HeartbeatResponse(response);
            let data = serde_json::to_vec(&msg)?;
            stream.write_all(&data).await?;

            // Update worker info with latest capability
            {
                let mut workers_guard = workers.write().await;
                if let Some(conn) = workers_guard.get_mut(&hb.worker_id) {
                    conn.load = hb.load;
                    conn.has_gpu = hb.has_gpu;
                    conn.vram_gb = hb.vram_gb;
                    conn.active = hb.active;
                }
            }

            Ok(())
        }
        HubMessage::HeartbeatResponse(_) => {
            warn!("Unexpected HeartbeatResponse from worker");
            Ok(())
        }
        _ => {
            warn!("Unexpected message type from worker");
            Ok(())
        }
    }
}

async fn start_http_server(port: u16, workers: WorkerMap, state: HubStateRef, admin_key: String, admin_users: Vec<String>) -> Result<()> {
    use tokio::net::TcpListener as HttpListener;

    let listener = HttpListener::bind(format!("0.0.0.0:{}", port)).await?;
    info!("HTTP server listening on 0.0.0.0:{}", port);

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                let mut buf = [0u8; 16384];
                let n = match stream.read(&mut buf).await {
                    Ok(n) => n,
                    Err(_) => continue,
                };

                let request = String::from_utf8_lossy(&buf[..n]).to_string();
                let lines: Vec<&str> = request.lines().collect();
                let path = lines.first().unwrap_or(&"");

                // Extract auth header
                let auth_key = lines.iter()
                    .find(|l| l.to_lowercase().starts_with("authorization: bearer "))
                    .map(|l| l.trim_start_matches("Authorization: Bearer ").trim())
                    .unwrap_or("");

                // Extract body for POST requests
                let body_start = request.find("\r\n\r\n").map(|p| p + 4);
                let body = body_start.map(|start| request[start..].trim()).unwrap_or("");

                let (status, resp_body) = if path.starts_with("GET /health") {
                    let workers_guard = workers.read().await;
                    let state_guard = state.lock().await;
                    let worker_list: Vec<_> = workers_guard
                        .values()
                        .map(|w| {
                            serde_json::json!({
                                "id": w.id,
                                "layers": format!("{}-{}", w.layer_offset, w.layer_offset + w.num_layers),
                                "gpu": w.has_gpu,
                                "vram_gb": w.vram_gb,
                                "load": w.load,
                            })
                        })
                        .collect();
                    let resp = serde_json::json!({
                        "status": "ok",
                        "model": state_guard.model.name,
                        "model_url": state_guard.model_url,
                        "num_layers": state_guard.model.num_layers,
                        "workers": worker_list,
                    });
                    (200, serde_json::to_string(&resp).unwrap_or_default())
                } else if path.starts_with("GET /v1/models") {
                    let model_name = state.lock().await.model.name.clone();
                    let resp = serde_json::json!({
                        "object": "list",
                        "data": [{
                            "id": model_name,
                            "object": "model",
                            "created": 1234567890,
                            "owned_by": "akai-net",
                        }]
                    });
                    (200, serde_json::to_string(&resp).unwrap_or_default())
                } else if path.starts_with("POST /admin/model") {
                    if admin_key.is_empty() || auth_key == admin_key {
                        match serde_json::from_str::<serde_json::Value>(body) {
                            Ok(json) => {
                                let username = json["username"].as_str()
                                    .unwrap_or("")
                                    .to_lowercase();
                                let authorized = admin_users.is_empty() || admin_users.iter().any(|u| u == &username);
                                if !authorized {
                                    info!("Model change rejected: user '{}' not authorized", username);
                                    (403, r#"{"error":"user not authorized"}"#.to_string())
                                } else {
                                    let name = json["name"].as_str().unwrap_or("unknown").to_string();
                                    let layers = json["layers"].as_u64().unwrap_or(32) as usize;
                                    let url = json["url"].as_str().unwrap_or("").to_string();

                                    let mut state_guard = state.lock().await;
                                    state_guard.model.name = name;
                                    state_guard.model.num_layers = layers;
                                    state_guard.model_url = url;

                                    info!("Model updated by {}: {} ({} layers)", username, state_guard.model.name, layers);

                                    let resp = serde_json::json!({
                                        "status": "ok",
                                        "model": state_guard.model.name,
                                        "layers": layers,
                                    });
                                    (200, serde_json::to_string(&resp).unwrap_or_default())
                                }
                            }
                            Err(e) => {
                                error!("Failed to parse admin request: {}", e);
                                (400, r#"{"error":"invalid request body"}"#.to_string())
                            }
                        }
                    } else {
                        (401, r#"{"error":"unauthorized"}"#.to_string())
                    }
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
                    resp_body.len(),
                    resp_body
                );
                stream.write_all(response.as_bytes()).await?;
            }
            Err(e) => error!("HTTP connection error: {}", e),
        }
    }
}
