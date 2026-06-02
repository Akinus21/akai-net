mod pipeline;

use anyhow::Result;
use pipeline::{HubMessage, WorkerInfo, ModelConfig, HeartbeatResponse, calculate_layer_assignment, build_pipeline_info, PipelineInfo};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Duration;
use std::net::SocketAddr;
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
    let admin_users = parse_admin_users();
    let queue_addr = std::env::var("QUEUE_ADDR").unwrap_or_else(|_| "http://ollama-queue:50053".to_string());
    let hub_id = std::env::var("HUB_ID").unwrap_or_else(|_| "hub-1".to_string());

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
    let worker_addrs: Arc<RwLock<HashMap<String, SocketAddr>>> = Arc::new(RwLock::new(HashMap::new()));

    // Worker protocol server
    let worker_workers = workers.clone();
    let worker_addrs_clone = worker_addrs.clone();
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
                    let addrs = worker_addrs_clone.clone();
                    let state = worker_state.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_worker_connection(stream, addr, workers, addrs, state).await {
                            error!("Worker connection error: {}", e);
                        }
                    });
                }
                Err(e) => error!("Failed to accept worker connection: {}", e),
            }
        }
    });

    // Start heartbeat cascade timer (every 30 seconds)
    let hb_workers = workers.clone();
    let hb_state = state.clone();
    let hb_addrs = worker_addrs.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            if let Err(e) = initiate_heartbeat_cascade(&hb_workers, &hb_state, &hb_addrs).await {
                warn!("Heartbeat cascade failed: {}", e);
            }
        }
    });

    // HTTP API server
    let http_workers = workers.clone();
    let http_state = state.clone();
    tokio::spawn(async move {
        start_http_server(hub_port, http_workers, http_state, admin_users).await
    });

    // Keep connection to queue alive
    let queue_state = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(25)).await;
            info!("Queue keepalive ping from hub {}", hub_id);
        }
    });

    // Keep main task alive - no ctrl_c, run forever
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

async fn initiate_heartbeat_cascade(
    workers: &WorkerMap,
    state: &HubStateRef,
    addrs: &Arc<RwLock<HashMap<String, SocketAddr>>>,
) -> Result<()> {
    let pipeline = {
        let workers_guard = workers.read().await;
        let state_guard = state.lock().await;
        let worker_list: Vec<_> = workers_guard.values().cloned().collect();
        if worker_list.is_empty() {
            return Ok(());
        }
        build_pipeline_info(
            &worker_list,
            &state_guard.model.name,
            &state_guard.model_url,
            state_guard.model.num_layers,
        )
    };

    let first_worker = pipeline.workers.iter().find(|w| w.is_first);
    if let Some(first) = first_worker {
        info!("Initiating heartbeat cascade to first worker: {}", first.worker_id);
        
        let addrs_guard = addrs.read().await;
        if let Some(&addr) = addrs_guard.get(&first.worker_id) {
            match TcpStream::connect(addr).await {
                Ok(mut stream) => {
                    let msg = HubMessage::HeartbeatForward { pipeline: pipeline.clone() };
                    let data = serde_json::to_vec(&msg)?;
                    stream.write_all(&data).await?;
                    info!("Heartbeat forward sent to {}", first.worker_id);
                }
                Err(e) => {
                    warn!("Failed to connect to first worker {}: {}", first.worker_id, e);
                }
            }
        } else {
            warn!("First worker {} not in address map", first.worker_id);
        }
    }

    Ok(())
}

async fn handle_worker_connection(
    mut stream: TcpStream,
    addr: SocketAddr,
    workers: WorkerMap,
    addrs: Arc<RwLock<HashMap<String, SocketAddr>>>,
    state: HubStateRef,
) -> Result<()> {
    let mut registered_id = None;
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

            // Build pipeline info to broadcast
            let (model_name, model_url, num_layers_total) = {
                let state_guard = state.lock().await;
                (state_guard.model.name.clone(), state_guard.model_url.clone(), state_guard.model.num_layers)
            };
            let workers_guard = workers.read().await;
            let worker_list: Vec<_> = workers_guard.values().cloned().collect();
            let pipeline = build_pipeline_info(&worker_list, &model_name, &model_url, num_layers_total);

            // Send heartbeat response with assignment + pipeline
            let response = HeartbeatResponse {
                layer_offset,
                num_layers,
                reassign: false,
                model_name,
                model_url,
                pipeline: Some(pipeline),
            };
            let msg = HubMessage::HeartbeatResponse(response);
            let data = serde_json::to_vec(&msg)?;
            stream.write_all(&data).await?;

            info!("Sent initial assignment to {}: layers {} to {}", info.id, layer_offset, layer_offset + num_layers);
            
            // Store worker address for cascade
            {
                let mut addrs_guard = addrs.write().await;
                addrs_guard.insert(info.id.clone(), addr);
            }
            
            registered_id = Some(info.id.clone());
            
            Ok(())
        }
        HubMessage::Heartbeat(hb) => {
            // This is a heartbeat returning from a worker (end of cascade)
            info!("Heartbeat received from {}: load={:.2}, last_hop_conn={}, next_hop_conn={}",
                hb.worker_id, hb.load, hb.last_hop_connected, hb.next_hop_connected);
            
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
            
            // Log cascade complete if last worker
            let is_last = {
                let workers_guard = workers.read().await;
                let worker_list: Vec<_> = workers_guard.values().cloned().collect();
                let pipeline = build_pipeline_info(
                    &worker_list,
                    &state.lock().await.model.name,
                    &state.lock().await.model_url,
                    state.lock().await.model.num_layers,
                );
                pipeline.workers.iter().any(|w| w.worker_id == hb.worker_id && w.is_last)
            };
            
            if is_last {
                info!("Pipeline heartbeat cascade complete - all workers responding");
            }
            
            // Send acknowledgment with pipeline info
            let pipeline = {
                let workers_guard = workers.read().await;
                let worker_list: Vec<_> = workers_guard.values().cloned().collect();
                let state_guard = state.lock().await;
                if worker_list.is_empty() {
                    None
                } else {
                    Some(build_pipeline_info(
                        &worker_list,
                        &state_guard.model.name,
                        &state_guard.model_url,
                        state_guard.model.num_layers,
                    ))
                }
            };
            
            let response = HeartbeatResponse {
                layer_offset: hb.layer_offset,
                num_layers: hb.num_layers,
                reassign: false,
                model_name: state.lock().await.model.name.clone(),
                model_url: state.lock().await.model_url.clone(),
                pipeline,
            };
            let msg = HubMessage::HeartbeatResponse(response);
            let data = serde_json::to_vec(&msg)?;
            stream.write_all(&data).await?;

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

async fn start_http_server(port: u16, workers: WorkerMap, state: HubStateRef, admin_users: Vec<String>) -> Result<()> {
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
                    // No admin key required - authenticated user handled by queue
                    // Just check if username is in authorized list
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
                } else if path.starts_with("GET /pipeline/status") {
                    // Return current pipeline registry for monitoring
                    let workers_guard = workers.read().await;
                    let state_guard = state.lock().await;
                    let pipeline = build_pipeline_info(
                        &workers_guard.values().cloned().collect::<Vec<_>>(),
                        &state_guard.model.name,
                        &state_guard.model_url,
                        state_guard.model.num_layers,
                    );
                    (200, serde_json::to_string(&pipeline).unwrap_or_default())
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
