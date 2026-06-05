mod pipeline;

use anyhow::Result;
use pipeline::{HubMessage, WorkerInfo, ModelConfig, HeartbeatResponse, calculate_layer_assignment, build_pipeline_info};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Duration;
use tracing::{info, warn, error};

type WorkerMap = Arc<RwLock<HashMap<String, WorkerInfo>>>;
type WorkerStreams = Arc<RwLock<HashMap<String, Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>>>>;

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
    let _ = hub_id; // Suppress unused warning

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
    let worker_streams: WorkerStreams = Arc::new(RwLock::new(HashMap::new()));

    // Worker protocol server
    let worker_workers = workers.clone();
    let worker_streams_clone = worker_streams.clone();
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
                    let streams = worker_streams_clone.clone();
                    let state = worker_state.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_worker_connection(stream, addr, workers, streams, state).await {
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
    let hb_streams = worker_streams.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            if let Err(e) = initiate_heartbeat_cascade(&hb_workers, &hb_state, &hb_streams).await {
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
    streams: &WorkerStreams,
) -> Result<()> {
    let pipeline = {
        let workers_guard = workers.read().await;
        let state_guard = state.lock().await;
        let worker_list: Vec<_> = workers_guard.values().cloned().collect();
        let stream_count = streams.read().await.len();
        
        if worker_list.is_empty() {
            info!("Cascade: no workers registered");
            return Ok(());
        }
        
        if stream_count == 0 {
            info!("Cascade: no worker streams available");
            return Ok(());
        }
        
        let pipeline = build_pipeline_info(
            &worker_list,
            &state_guard.model.name,
            &state_guard.model_url,
            state_guard.model.num_layers,
        );
        info!("Cascade: {} workers, {} streams, first={}", 
              worker_list.len(), stream_count,
              pipeline.workers.first().map(|w| w.worker_id.as_str()).unwrap_or("none"));
        pipeline
    };

    // Send HeartbeatForward to first worker through its persistent connection
    if let Some(first) = pipeline.workers.first() {
        let streams_guard = streams.read().await;
        if let Some(writer) = streams_guard.get(&first.worker_id) {
            let msg = HubMessage::HeartbeatForward { pipeline: pipeline.clone() };
            let data = serde_json::to_vec(&msg)?;
            let mut writer = writer.lock().await;
            match writer.write_all(&data).await {
                Ok(_) => info!("HeartbeatForward sent to {} via persistent connection", first.worker_id),
                Err(e) => warn!("Failed to send HeartbeatForward to {}: {}", first.worker_id, e),
            }
        } else {
            warn!("No persistent stream for first worker {}", first.worker_id);
        }
    }

    Ok(())
}

async fn handle_worker_connection(
    stream: TcpStream,
    _addr: std::net::SocketAddr,
    workers: WorkerMap,
    streams: WorkerStreams,
    state: HubStateRef,
) -> Result<()> {
    let (reader, writer) = stream.into_split();
    let mut reader = reader;
    let writer = Arc::new(Mutex::new(writer));
    let mut current_worker_id: Option<String> = None;
    let mut buf = vec![0u8; 65536];
    
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            if let Some(ref id) = current_worker_id {
                info!("Worker {} disconnected", id);
                workers.write().await.remove(id);
                streams.write().await.remove(id);
                info!("Removed {} from workers and streams", id);
            } else {
                info!("Worker disconnected");
            }
            break;
        }

        let message: HubMessage = match serde_json::from_slice(&buf[..n]) {
            Ok(m) => m,
            Err(e) => {
                error!("Failed to parse worker message: {}", e);
                continue;
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

                // Store the write half for this worker so we can send messages to it
                {
                    let mut streams_guard = streams.write().await;
                    streams_guard.insert(info.id.clone(), writer.clone());
                    info!("Streams map now has {} entries", streams_guard.len());
                }

                // Insert worker info
                {
                    let mut workers_guard = workers.write().await;
                    workers_guard.insert(info.id.clone(), info.clone());
                    info!("Workers HashMap now has {} entries", workers_guard.len());
                }

                current_worker_id = Some(info.id.clone());

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

                // Build pipeline info to send
                let (model_name, model_url, num_layers_total) = {
                    let state_guard = state.lock().await;
                    (state_guard.model.name.clone(), state_guard.model_url.clone(), state_guard.model.num_layers)
                };
                let workers_guard = workers.read().await;
                let worker_list: Vec<_> = workers_guard.values().cloned().collect();
                let pipeline = build_pipeline_info(&worker_list, &model_name, &model_url, num_layers_total);

                // Send heartbeat response with assignment + pipeline via persistent connection
                let response = HeartbeatResponse {
                    layer_offset,
                    num_layers,
                    reassign: false,
                    model_name,
                    model_url,
                    pipeline: Some(pipeline.clone()),
                };
                let msg = HubMessage::HeartbeatResponse(response);
                let data = serde_json::to_vec(&msg)?;
                {
                    let mut w = writer.lock().await;
                    w.write_all(&data).await?;
                }

                info!("Sent initial assignment to {}: layers {} to {}", info.id, layer_offset, layer_offset + num_layers);
            }
            HubMessage::Heartbeat(hb) => {
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
                
                // Send acknowledgment with current pipeline
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
                {
                    let mut w = writer.lock().await;
                    w.write_all(&data).await?;
                }
            }
            HubMessage::HeartbeatResponse(_) => {
                warn!("Unexpected HeartbeatResponse from worker");
            }
            HubMessage::InferenceResponse(resp) => {
                info!("Inference response from {}: done={}", resp.id, resp.is_done);
                // TODO: Handle inference response - relay to next worker or return to queue
            }
            _ => {
                warn!("Unexpected message type from worker");
            }
        }
    }
    
    Ok(())
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
                let _auth_key = lines.iter()
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
                } else if path.starts_with("POST /workers/register") {
                    match serde_json::from_str::<serde_json::Value>(body) {
                        Ok(json) => {
                            let worker_id = json["id"].as_str().unwrap_or("").to_string();
                            let gpu = json["gpu"].as_bool().unwrap_or(false);
                            let vram_gb = json["vram_gb"].as_f64().unwrap_or(0.0) as f32;
                            
                            info!("Worker registration request: id={}, gpu={}, vram={}GB", worker_id, gpu, vram_gb);
                            
                            let resp = serde_json::json!({
                                "status": "registered",
                                "worker_id": worker_id,
                                "message": "Worker registered successfully"
                            });
                            (200, serde_json::to_string(&resp).unwrap_or_default())
                        }
                        Err(e) => {
                            error!("Failed to parse worker registration: {}", e);
                            (400, r#"{"error":"invalid request body"}"#.to_string())
                        }
                    }
                } else if path.starts_with("POST /auth/register") {
                    match serde_json::from_str::<serde_json::Value>(body) {
                        Ok(json) => {
                            let username = json["username"].as_str().unwrap_or("").to_string();
                            let worker_name = json["worker_name"].as_str().unwrap_or("").to_string();
                            
                            info!("Auth register: user={}, worker={}", username, worker_name);
                            
                            // Check if username is authorized
                            let authorized = admin_users.iter().any(|u| u == &username.to_lowercase());
                            if !authorized {
                                info!("Auth register rejected: user '{}' not authorized", username);
                                (403, r#"{"error":"user not authorized"}"#.to_string())
                            } else {
                                // Auto-approve for now - TODO: integrate with Duo
                                info!("Auth register approved for: {}", username);
                                let resp = serde_json::json!({
                                    "status": "provisioned",
                                    "username": username,
                                    "worker_id": format!("{}:{}", username, worker_name),
                                    "wg_ip": format!("10.8.0.{}", rand::random::<u8>() % 200 + 2),
                                    "rpc_port": 50052
                                });
                                (200, serde_json::to_string(&resp).unwrap_or_default())
                            }
                        }
                        Err(e) => {
                            error!("Failed to parse auth register: {}", e);
                            (400, r#"{"error":"invalid request body"}"#.to_string())
                        }
                    }
                } else if path.starts_with("POST /auth/login") {
                    match serde_json::from_str::<serde_json::Value>(body) {
                        Ok(json) => {
                            let username = json["username"].as_str().unwrap_or("").to_string();
                            info!("Auth login attempt for: {}", username);
                            
                            // Check if username is authorized
                            let authorized = admin_users.iter().any(|u| u == &username.to_lowercase());
                            if !authorized {
                                info!("Auth rejected: user '{}' not in admin list", username);
                                (403, r#"{"error":"user not authorized"}"#.to_string())
                            } else {
                                // For now, auto-approve if user is authorized
                                // TODO: Integrate with Duo for real 2FA
                                info!("Auth approved for: {}", username);
                                let resp = serde_json::json!({
                                    "status": "approved",
                                    "username": username,
                                    "token": format!("token-{}", rand::random::<u64>())
                                });
                                (200, serde_json::to_string(&resp).unwrap_or_default())
                            }
                        }
                        Err(e) => {
                            error!("Failed to parse auth request: {}", e);
                            (400, r#"{"error":"invalid request body"}"#.to_string())
                        }
                    }
                } else if path.starts_with("POST /admin/model") {
                    // Check username in authorized list
                    match serde_json::from_str::<serde_json::Value>(body) {
                        Ok(json) => {
                            let username = json["username"].as_str()
                                .unwrap_or("")
                                .to_lowercase();
                            let authorized = admin_users.iter().any(|u| u == &username);
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
                    // Parse incoming chat request
                    match serde_json::from_str::<serde_json::Value>(body) {
                        Ok(json) => {
                            let model = json["model"].as_str().unwrap_or("unknown");
                            let messages = json["messages"].as_array().cloned().unwrap_or_default();
                            
                            info!("Chat completion request: model={}, messages={}", model, messages.len());
                            
                            // TODO: Route to workers for distributed inference
                            // For now, return that pipeline is processing
                            let resp = serde_json::json!({
                                "id": format!("chatcmpl-{}", rand::random::<u64>()),
                                "object": "chat.completion",
                                "created": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
                                "model": model,
                                "choices": [{
                                    "index": 0,
                                    "message": {
                                        "role": "assistant",
                                        "content": "Pipeline processing request. Workers will handle distributed inference.",
                                    },
                                    "finish_reason": "stop"
                                }],
                                "usage": {
                                    "prompt_tokens": 0,
                                    "completion_tokens": 0,
                                    "total_tokens": 0
                                }
                            });
                            (200, serde_json::to_string(&resp).unwrap_or_default())
                        }
                        Err(e) => {
                            error!("Failed to parse chat request: {}", e);
                            (400, r#"{"error":"invalid request body"}"#.to_string())
                        }
                    }
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
