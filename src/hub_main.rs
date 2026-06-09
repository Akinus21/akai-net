mod pipeline;
mod duo;

use anyhow::Result;
use pipeline::{HubMessage, WorkerInfo, ModelConfig, HeartbeatResponse, InferenceResponse, calculate_layer_assignment, build_pipeline_info};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock, oneshot};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Duration;
use tracing::{info, warn, error};

type WorkerMap = Arc<RwLock<HashMap<String, WorkerInfo>>>;
type WorkerStreams = Arc<RwLock<HashMap<String, Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>>>>;
type PendingInferences = Arc<Mutex<HashMap<String, oneshot::Sender<InferenceResponse>>>>;
type MissedHeartbeats = Arc<Mutex<HashMap<String, u32>>>;

struct HubState {
    model: ModelConfig,
    model_url: String,
    model_hash: String,
}

type HubStateRef = Arc<Mutex<HubState>>;

fn model_proxy_url(hub_vpn_addr: &str) -> String {
    format!("http://{}/model/download", hub_vpn_addr)
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

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
    std::panic::set_hook(Box::new(|info| {
        error!("PANIC: {}", info);
    }));
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .init();

    let hub_port: u16 = std::env::var("HUB_PORT").unwrap_or_else(|_| "8080".to_string()).parse().unwrap_or(8080);
    let worker_port: u16 = std::env::var("WORKER_PORT").unwrap_or_else(|_| "50051".to_string()).parse().unwrap_or(50051);
    let hub_vpn_addr = std::env::var("HUB_VPN_ADDR").unwrap_or_else(|_| format!("10.8.0.1:{}", worker_port));
    let hub_http_vpn_addr = std::env::var("HUB_HTTP_VPN_ADDR").unwrap_or_else(|_| format!("10.8.0.1:{}", hub_port));
    let admin_users = parse_admin_users();
    let _queue_addr = std::env::var("QUEUE_ADDR").unwrap_or_else(|_| "http://ollama-queue:50053".to_string());
    let hub_id = std::env::var("HUB_ID").unwrap_or_else(|_| "hub-1".to_string());
    let _ = hub_id;
    let tunnel_certs_dir = std::env::var("TUNNEL_CERTS_DIR").unwrap_or_else(|_| "/etc/akai-tunnel/certs".to_string());
    let duo_config = duo::load_duo_config();
    let wg_easy_password = std::env::var("WG_EASY_PASSWORD").unwrap_or_default();
    let wg_easy_host = std::env::var("WG_EASY_HOST").unwrap_or_else(|_| "wireguard:51821".to_string());
    if duo_config.is_some() {
        info!("Duo 2FA enabled (host: {})", duo_config.as_ref().unwrap().host);
    } else {
        info!("Duo 2FA not configured (set DUO_IKEY, DUO_SKEY, DUO_HOST)");
    }
    if !wg_easy_password.is_empty() {
        info!("WireGuard VPN enrollment enabled (wg-easy at {})", wg_easy_host);
    } else {
        info!("WireGuard VPN enrollment not configured (set WG_EASY_PASSWORD)");
    }

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
        model_hash: String::new(),
    }));

    info!("Akai-Net Hub starting...");
    info!("HTTP API: 0.0.0.0:{}", hub_port);
    info!("Worker protocol: 0.0.0.0:{}", worker_port);

    let workers: WorkerMap = Arc::new(RwLock::new(HashMap::new()));
    let worker_streams: WorkerStreams = Arc::new(RwLock::new(HashMap::new()));
    let pending_inferences: PendingInferences = Arc::new(Mutex::new(HashMap::new()));
    let missed_heartbeats: MissedHeartbeats = Arc::new(Mutex::new(HashMap::new()));
    let cascade_responded: Arc<Mutex<HashMap<String, bool>>> = Arc::new(Mutex::new(HashMap::new()));

    // Worker protocol server
    let worker_workers = workers.clone();
    let worker_streams_clone = worker_streams.clone();
    let worker_state = state.clone();
    let worker_pending = pending_inferences.clone();
    let worker_missed = missed_heartbeats.clone();
    let worker_responded = cascade_responded.clone();
    let hb_vpn_addr = hub_http_vpn_addr.clone();
    let http_worker_vpn_addr = hub_vpn_addr.clone();
    let http_http_vpn_addr = hub_http_vpn_addr.clone();
    let wp_vpn_addr = hub_vpn_addr;
    let wp_http_vpn_addr = hub_http_vpn_addr;
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
                    let pending = worker_pending.clone();
                    let missed = worker_missed.clone();
                    let responded = worker_responded.clone();
                    let wp_vpn_addr = wp_vpn_addr.clone();
                    let wp_http_vpn_addr = wp_http_vpn_addr.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_worker_connection(stream, addr, workers, streams, state, pending, wp_vpn_addr, wp_http_vpn_addr, missed, responded).await {
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
    let hb_missed = missed_heartbeats.clone();
    let hb_responded = cascade_responded.clone();
    let hb_wg_password = wg_easy_password.clone();
    let hb_wg_host = wg_easy_host.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            match initiate_heartbeat_cascade(&hb_workers, &hb_state, &hb_streams, &hb_vpn_addr, &hb_missed, &hb_responded, &hb_wg_password, &hb_wg_host).await {
                Ok(()) => {},
                Err(e) => error!("Heartbeat cascade error: {}", e),
            }
        }
    });

    // HTTP API server
    let http_workers = workers.clone();
    let http_state = state.clone();
    let http_streams = worker_streams.clone();
    let http_pending = pending_inferences.clone();
    tokio::spawn(async move {
        start_http_server(hub_port, worker_port, http_workers, http_state, http_streams, http_pending, admin_users, duo_config, tunnel_certs_dir, wg_easy_password, wg_easy_host, http_worker_vpn_addr, http_http_vpn_addr).await
    });

    // Keep connection to queue alive
    let _queue_state = state.clone();
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
    hub_http_vpn_addr: &str,
    missed_hbs: &MissedHeartbeats,
    cascade_responded: &Arc<Mutex<HashMap<String, bool>>>,
    wg_password: &str,
    wg_host: &str,
) -> Result<()> {
    // Check which workers responded in the previous cascade, increment missed counters
    info!("Cascade: checking previous responses");
    let mut to_deregister: Vec<String> = Vec::new();
    {
        let responded = cascade_responded.lock().await;
        let mut missed_guard = missed_hbs.lock().await;
        for (worker_id, &did_respond) in responded.iter() {
            if did_respond {
                missed_guard.insert(worker_id.clone(), 0);
            } else {
                let count = missed_guard.entry(worker_id.clone()).or_insert(0);
                *count += 1;
                warn!("[cascade] {} missed heartbeat ({} consecutive)", worker_id, *count);
                if *count >= 4 {
                    to_deregister.push(worker_id.clone());
                }
            }
        }
    }

    // Reset responded map for this cascade cycle
    {
        let mut responded = cascade_responded.lock().await;
        responded.clear();
        let workers_guard = workers.read().await;
        for worker_id in workers_guard.keys() {
            responded.insert(worker_id.clone(), false);
        }
    }

    // Deregister workers with 4+ consecutive misses
    if !to_deregister.is_empty() {
        for worker_id in &to_deregister {
            warn!("[deregister] Removing worker {} after 4 consecutive missed heartbeats", worker_id);

            let removed_worker = {
                let mut workers_guard = workers.write().await;
                workers_guard.remove(worker_id)
            };
            {
                let mut streams_guard = streams.write().await;
                streams_guard.remove(worker_id);
            }
            {
                let mut missed_guard = missed_hbs.lock().await;
                missed_guard.remove(worker_id);
            }
            {
                let mut responded = cascade_responded.lock().await;
                responded.remove(worker_id);
            }

            if let Some(ref worker_info) = removed_worker {
                if !worker_info.wg_peer_id.is_empty() && !wg_password.is_empty() {
                    match remove_wireguard_client(wg_host, wg_password, &worker_info.wg_peer_id).await {
                        Ok(()) => info!("[deregister] VPN peer {} removed", worker_info.wg_peer_id),
                        Err(e) => warn!("[deregister] Failed to remove VPN peer {}: {}", worker_info.wg_peer_id, e),
                    }
                }
            }
        }

        // Notify remaining workers about new layer assignments after deregistration
        if !to_deregister.is_empty() {
            let worker_list: Vec<_> = {
                let workers_guard = workers.read().await;
                let mut list: Vec<_> = workers_guard.values().cloned().collect();
                list.sort_by(|a, b| {
                    let a_score = if a.has_gpu { a.vram_gb * 100.0 } else { 1.0 };
                    let b_score = if b.has_gpu { b.vram_gb * 100.0 } else { 1.0 };
                    a_score.partial_cmp(&b_score).unwrap()
                });
                list
            };
            if !worker_list.is_empty() {
                let proxy_url = model_proxy_url(&hub_http_vpn_addr);
                let (model_name, model_hash, num_layers_total) = {
                    let state_guard = state.lock().await;
                    (state_guard.model.name.clone(), state_guard.model_hash.clone(), state_guard.model.num_layers)
                };
                let pipeline = build_pipeline_info(&worker_list, &model_name, &proxy_url, num_layers_total);
                
                for worker in &pipeline.workers {
                    let streams_guard = streams.read().await;
                    if let Some(writer) = streams_guard.get(&worker.worker_id) {
                        let response = HeartbeatResponse {
                            layer_offset: worker.layer_offset,
                            num_layers: worker.num_layers,
                            reassign: true,
                            model_name: model_name.clone(),
                            model_url: proxy_url.clone(),
                            model_hash: model_hash.clone(),
                            pipeline: Some(pipeline.clone()),
                        };
                        let msg = HubMessage::HeartbeatResponse(response);
                        if let Ok(data) = encode_msg(&msg) {
                            let mut w = writer.lock().await;
                            if let Err(e) = w.write_all(&data).await {
                                warn!("[deregister] Failed to notify {}: {}", worker.worker_id, e);
                            } else {
                                info!("[deregister] Notified {} of new assignment: layers {}-{}", 
                                    worker.worker_id, worker.layer_offset, worker.layer_offset + worker.num_layers);
                            }
                        }
                    }
                }
            } else {
                warn!("[deregister] No workers remaining after deregistration");
            }
        }
    }

    let pipeline = {
        info!("Cascade: building pipeline info");
        let state_guard = state.lock().await;
        let workers_guard = workers.read().await;
        let worker_list: Vec<_> = workers_guard.values().cloned().collect();
        drop(workers_guard);
        drop(state_guard);
        
        let stream_count = streams.read().await.len();
        
        if worker_list.is_empty() {
            info!("Cascade: no workers registered");
            return Ok(());
        }
        
        if stream_count == 0 {
            warn!("Cascade: {} workers but 0 streams", worker_list.len());
            return Ok(());
        }
        
        let state_guard = state.lock().await;
        let proxy_url = model_proxy_url(hub_http_vpn_addr);
        let mut sorted_workers: Vec<_> = worker_list.clone();
        sorted_workers.sort_by(|a, b| {
            let a_score = if a.has_gpu { a.vram_gb * 100.0 } else { 1.0 };
            let b_score = if b.has_gpu { b.vram_gb * 100.0 } else { 1.0 };
            a_score.partial_cmp(&b_score).unwrap()
        });
        let mut pipeline = build_pipeline_info(
            &sorted_workers,
            &state_guard.model.name,
            &proxy_url,
            state_guard.model.num_layers,
        );
        drop(state_guard);
        info!("Cascade: {} workers, {} streams, first={}", 
              worker_list.len(), stream_count,
              pipeline.workers.first().map(|w| w.worker_id.as_str()).unwrap_or("none"));
        pipeline.model_url = proxy_url;
        pipeline
    };

    // Send HeartbeatForward to first worker through its persistent connection
    if let Some(first) = pipeline.workers.first() {
        let streams_guard = streams.read().await;
        if let Some(writer) = streams_guard.get(&first.worker_id) {
            let msg = HubMessage::HeartbeatForward { pipeline: pipeline.clone() };
            let data = match encode_msg(&msg) {
                Ok(d) => d,
                Err(e) => {
                    warn!("[-> {}] HeartbeatForward encode failed: {}", first.worker_id, e);
                    return Ok(());
                }
            };

            let mut w = writer.lock().await;
            match tokio::time::timeout(Duration::from_secs(5), w.write_all(&data)).await {
                Ok(Ok(_)) => info!("[-> {}] HeartbeatForward: pipeline_id={}, {} workers, model={}", 
                    first.worker_id, pipeline.pipeline_id, pipeline.workers.len(), pipeline.model_name),
                Ok(Err(e)) => warn!("[-> {}] HeartbeatForward FAILED: {}", first.worker_id, e),
                Err(_) => warn!("[-> {}] HeartbeatForward timed out (worker may be dead)", first.worker_id),
            }
        } else {
            warn!("No persistent stream for first worker {}", first.worker_id);
        }
    }

    info!("Cascade: cycle complete");
    Ok(())
}

fn encode_msg(msg: &HubMessage) -> Result<Vec<u8>> {
    let mut data = serde_json::to_vec(msg)?;
    data.push(b'\n');
    Ok(data)
}

async fn handle_worker_connection(
    stream: TcpStream,
    _addr: std::net::SocketAddr,
    workers: WorkerMap,
    streams: WorkerStreams,
    state: HubStateRef,
    pending: PendingInferences,
    _hub_vpn_addr: String,
    hub_http_vpn_addr: String,
    missed_hbs: MissedHeartbeats,
    cascade_responded: Arc<Mutex<HashMap<String, bool>>>,
) -> Result<()> {
    let (reader, writer) = stream.into_split();
    let mut reader = reader;
    let writer = Arc::new(Mutex::new(writer));
    let mut current_worker_id: Option<String> = None;
    let mut read_buf = Vec::new();
    
    loop {
        let mut tmp = [0u8; 65536];
        let n = reader.read(&mut tmp).await?;
        if n == 0 {
            if let Some(ref id) = current_worker_id {
                info!("Worker {} disconnected", id);
                workers.write().await.remove(id);
                streams.write().await.remove(id);
                missed_hbs.lock().await.remove(id);
                cascade_responded.lock().await.remove(id);
                info!("Removed {} from workers and streams", id);
            } else {
                info!("Worker disconnected");
            }
            break;
        }
        read_buf.extend_from_slice(&tmp[..n]);

        // Process complete lines (newline-delimited JSON)
        while let Some(pos) = read_buf.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = read_buf.drain(..=pos).collect();
            let line = &line[..line.len() - 1]; // strip trailing \n

            if line.is_empty() {
                continue;
            }

            let message: HubMessage = match serde_json::from_slice(line) {
                Ok(m) => m,
                Err(e) => {
                    error!("Failed to parse worker message: {} (line: {} bytes)", e, line.len());
                    continue;
                }
            };

        match message {
            HubMessage::Register(info) => {
                info!("[<- {}] Register: GPU={}, VRAM={:.1}GB", info.id, info.has_gpu, info.vram_gb);

                // Store the write half for this worker so we can send messages to it
                {
                    let mut streams_guard = streams.write().await;
                    streams_guard.insert(info.id.clone(), writer.clone());
                    info!("Streams map now has {} entries", streams_guard.len());
                }

                // Insert worker info and reset missed counter
                {
                    let mut workers_guard = workers.write().await;
                    workers_guard.insert(info.id.clone(), info.clone());
                    info!("Workers HashMap now has {} entries: {:?}", 
                        workers_guard.len(),
                        workers_guard.values().map(|w| format!("{}:{:.0}GB", w.id, w.vram_gb)).collect::<Vec<_>>());
                }
                {
                    missed_hbs.lock().await.insert(info.id.clone(), 0);
                }

                current_worker_id = Some(info.id.clone());

                // Recalculate assignments
                let (layer_offset, num_layers) = {
                    let state_guard = state.lock().await;
                    let workers_guard = workers.read().await;
                    let worker_list: Vec<_> = workers_guard.values().cloned().collect();
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

                // Build pipeline info to send (use proxy URL for model downloads)
                let (model_name, _model_url_raw, num_layers_total, model_hash) = {
                    let state_guard = state.lock().await;
                    (state_guard.model.name.clone(), state_guard.model_url.clone(), state_guard.model.num_layers, state_guard.model_hash.clone())
                };
                let proxy_url = model_proxy_url(&hub_http_vpn_addr);
                let workers_guard = workers.read().await;
                let mut worker_list: Vec<_> = workers_guard.values().cloned().collect();
                drop(workers_guard);
                worker_list.sort_by(|a, b| {
                    let a_score = if a.has_gpu { a.vram_gb * 100.0 } else { 1.0 };
                    let b_score = if b.has_gpu { b.vram_gb * 100.0 } else { 1.0 };
                    a_score.partial_cmp(&b_score).unwrap()
                });
                let pipeline = build_pipeline_info(&worker_list, &model_name, &proxy_url, num_layers_total);

                // Send heartbeat response with assignment + pipeline via persistent connection
                let response = HeartbeatResponse {
                    layer_offset,
                    num_layers,
                    reassign: false,
                    model_name: model_name.clone(),
                    model_url: proxy_url,
                    model_hash,
                    pipeline: Some(pipeline.clone()),
                };
                let msg = HubMessage::HeartbeatResponse(response);
                let data = encode_msg(&msg)?;
                {
                    let mut w = writer.lock().await;
                    w.write_all(&data).await?;
                }

                info!("[-> {}] HeartbeatResponse: layers {}-{}, model={}, pipeline={}",
                    info.id, layer_offset, layer_offset + num_layers, model_name, pipeline.workers.len());

                // Notify all OTHER workers about the updated pipeline
                // This ensures existing workers learn about new peers immediately
                let streams_clone = streams.clone();
                let pipeline_clone = pipeline.clone();
                let new_worker_id = info.id.clone();
                tokio::spawn(async move {
                    let streams_guard = streams_clone.read().await;
                    for (worker_id, writer) in streams_guard.iter() {
                        if worker_id == &new_worker_id {
                            continue; // Skip the newly registered worker
                        }
                        let (layer_offset, num_layers) = pipeline_clone.workers.iter()
                            .find(|w| w.worker_id == *worker_id)
                            .map(|w| (w.layer_offset, w.num_layers))
                            .unwrap_or((0, 0));
                        let response = HeartbeatResponse {
                            layer_offset,
                            num_layers,
                            reassign: false,
                            model_name: pipeline_clone.model_name.clone(),
                            model_url: pipeline_clone.model_url.clone(),
                            model_hash: String::new(),
                            pipeline: Some(pipeline_clone.clone()),
                        };
                        let msg = HubMessage::HeartbeatResponse(response);
                        if let Ok(data) = encode_msg(&msg) {
                            match writer.try_lock() {
                                Ok(mut w) => {
                                    if w.write_all(&data).await.is_err() {
                                        warn!("[hub] Failed to send pipeline update to {}", worker_id);
                                    } else {
                                        info!("[-> {}] PipelineUpdate: {} workers, layers {}-{}", worker_id, pipeline_clone.workers.len(), layer_offset, layer_offset + num_layers);
                                    }
                                }
                                Err(_) => {
                                    warn!("[hub] Could not lock writer for {}", worker_id);
                                }
                            }
                        }
                    }
                });
            }
            HubMessage::Heartbeat(hb) => {
                info!("[<- {}] Heartbeat: load={:.2}, last_hop_conn={}, next_hop_conn={}",
                    hb.worker_id, hb.load, hb.last_hop_connected, hb.next_hop_connected);
                
                // Mark worker as responded for this cascade
                {
                    let mut responded = cascade_responded.lock().await;
                    responded.insert(hb.worker_id.clone(), true);
                }
                // Reset missed heartbeat counter
                {
                    let mut missed_guard = missed_hbs.lock().await;
                    missed_guard.insert(hb.worker_id.clone(), 0);
                }
                
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
                
                let (model_name, model_url_raw, _num_layers) = {
                    let state_guard = state.lock().await;
                    (state_guard.model.name.clone(), state_guard.model_url.clone(), state_guard.model.num_layers)
                };
                
                // Log cascade complete if last worker
                let is_last = {
                    let state_guard = state.lock().await;
                    let workers_guard = workers.read().await;
                    let mut worker_list: Vec<_> = workers_guard.values().cloned().collect();
                    worker_list.sort_by(|a, b| {
                        let a_score = if a.has_gpu { a.vram_gb * 100.0 } else { 1.0 };
                        let b_score = if b.has_gpu { b.vram_gb * 100.0 } else { 1.0 };
                        a_score.partial_cmp(&b_score).unwrap()
                    });
                    let pipeline = build_pipeline_info(
                        &worker_list,
                        &model_name,
                        &model_url_raw,
                        state_guard.model.num_layers,
                    );
                    drop(state_guard);
                    drop(workers_guard);
                    pipeline.workers.iter().any(|w| w.worker_id == hb.worker_id && w.is_last)
                };
                
                if is_last {
                    info!("[cascade] complete - all workers responded");
                }
                
                // Send acknowledgment with current pipeline
                let pipeline = {
                    let state_guard = state.lock().await;
                    let workers_guard = workers.read().await;
                    let mut worker_list: Vec<_> = workers_guard.values().cloned().collect();
                    drop(workers_guard);
                    // Debug: log workers in hashmap
                    info!("[heartbeat] workers in hashmap: {}", worker_list.iter()
                        .map(|w| format!("{}:{:.0}GB", w.id, w.vram_gb))
                        .collect::<Vec<_>>().join(", "));
                    worker_list.sort_by(|a, b| {
                        let a_score = if a.has_gpu { a.vram_gb * 100.0 } else { 1.0 };
                        let b_score = if b.has_gpu { b.vram_gb * 100.0 } else { 1.0 };
                        let cmp = a_score.partial_cmp(&b_score).unwrap();
                        info!("[heartbeat] sort compare {} vs {}: {:?}", a.id, b.id, cmp);
                        cmp
                    });
                    // Debug: log sorted worker order
                    for (i, w) in worker_list.iter().enumerate() {
                        let score = if w.has_gpu { w.vram_gb * 100.0 } else { 1.0 };
                        info!("[heartbeat] sorted worker[{}] {}: score={:.0}, layers={}-{}", 
                            i, w.id, score, w.layer_offset, w.layer_offset + w.num_layers);
                    }
                    if worker_list.is_empty() {
                        drop(state_guard);
                        None
                    } else {
                        let proxy_url = model_proxy_url(&hub_http_vpn_addr);
                        let pipeline = build_pipeline_info(
                            &worker_list,
                            &state_guard.model.name,
                            &proxy_url,
                            state_guard.model.num_layers,
                        );
                        drop(state_guard);
                        Some(pipeline)
                    }
                };
                
                let pipeline_count = pipeline.as_ref().map(|p| p.workers.len()).unwrap_or(0);
                let proxy_url = model_proxy_url(&hub_http_vpn_addr);
                let (model_name2, model_hash, model_num_layers) = {
                    let state_guard = state.lock().await;
                    (state_guard.model.name.clone(), state_guard.model_hash.clone(), state_guard.model.num_layers)
                };
                
                // Get this worker's correct assignment from the sorted pipeline
                let (correct_offset, correct_num, should_reassign) = if let Some(ref p) = pipeline {
                    p.workers.iter()
                        .find(|w| w.worker_id == hb.worker_id)
                        .map(|w| (w.layer_offset, w.num_layers, w.layer_offset != hb.layer_offset || w.num_layers != hb.num_layers))
                        .unwrap_or((hb.layer_offset, hb.num_layers, false))
                } else {
                    (hb.layer_offset, hb.num_layers, false)
                };
                
                let response = HeartbeatResponse {
                    layer_offset: correct_offset,
                    num_layers: correct_num,
                    reassign: should_reassign,
                    model_name: model_name2,
                    model_url: proxy_url,
                    model_hash,
                    pipeline,
                };
                let msg = HubMessage::HeartbeatResponse(response);
                let data = encode_msg(&msg)?;
                {
                    let mut w = writer.lock().await;
                    w.write_all(&data).await?;
                }
                if current_worker_id.as_deref() == Some(&hb.worker_id) {
                    info!("[-> {}] HeartbeatResponse: ack, pipeline={}",
                        hb.worker_id, pipeline_count);
                }
            }
            HubMessage::HeartbeatResponse(_) => {
                warn!("Unexpected HeartbeatResponse from worker");
            }
            HubMessage::InferenceResponse(resp) => {
                info!("[<- worker] InferenceResponse: id={}, done={}, text={}bytes", 
                    resp.id, resp.is_done, resp.text.as_deref().unwrap_or("").len());
                let mut pending_guard = pending.lock().await;
                if let Some(sender) = pending_guard.remove(&resp.id) {
                    let _ = sender.send(resp);
                    info!("[-> http] InferenceResponse delivered to waiting HTTP handler");
                } else {
                    warn!("No pending inference request for id={}", resp.id);
                }
            }
            HubMessage::InferenceForward(fwd) => {
                let to_worker = fwd.to_worker.clone();
                let from_worker = fwd.from_worker.clone();
                let data_len = fwd.data.len();
                info!("[<- {}] InferenceForward: -> {}, {} bytes", from_worker, to_worker, data_len);
                let streams_guard = streams.read().await;
                if let Some(target_writer) = streams_guard.get(&to_worker) {
                    let data = match encode_msg(&HubMessage::InferenceForward(fwd)) {
                        Ok(d) => d,
                        Err(e) => {
                            warn!("Failed to serialize InferenceForward: {}", e);
                            continue;
                        }
                    };
                    let mut w = target_writer.lock().await;
                    match w.write_all(&data).await {
                        Ok(_) => info!("[-> {}] InferenceForward: forwarded {} bytes", to_worker, data_len),
                        Err(e) => warn!("[-> {}] InferenceForward FAILED: {}", to_worker, e),
                    }
                } else {
                    warn!("Target worker {} not found for inference forward", to_worker);
                }
            }
            _ => {
                warn!("Unexpected message type from worker");
            }
        }
        } // end while let Some(pos)
    } // end loop
    
    Ok(())
}

async fn start_http_server(port: u16, _worker_port: u16, workers: WorkerMap, state: HubStateRef, streams: WorkerStreams, pending: PendingInferences, admin_users: Vec<String>, duo_config: Option<duo::DuoConfig>, tunnel_certs_dir: String, wg_easy_password: String, wg_easy_host: String, hub_worker_vpn_addr: String, _hub_http_vpn_addr: String) -> Result<()> {
    use tokio::net::TcpListener as HttpListener;

    let listener = HttpListener::bind(format!("0.0.0.0:{}", port)).await?;
    info!("HTTP server listening on 0.0.0.0:{}", port);

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                let mut buf = vec![0u8; 65536];
                let mut total = 0;
                loop {
                    let n = match stream.read(&mut buf[total..]).await {
                        Ok(0) => break,
                        Ok(n) => n,
                        Err(_) => break,
                    };
                    total += n;

                    let request_so_far = String::from_utf8_lossy(&buf[..total]);
                    let headers_end = request_so_far.find("\r\n\r\n");
                    if let Some(hdr_end) = headers_end {
                        let header_section = &request_so_far[..hdr_end];
                        let content_length = header_section.lines()
                            .find(|l| l.to_lowercase().starts_with("content-length:"))
                            .and_then(|l| l.trim_start_matches("Content-Length:").trim_start_matches("content-length:").trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        let body_received = total - (hdr_end + 4);
                        if body_received >= content_length {
                            break;
                        }
                    }
                    if total >= buf.len() - 1 { break; }
                }

                let request = String::from_utf8_lossy(&buf[..total]).to_string();
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
                    let state_guard = state.lock().await;
                    let workers_guard = workers.read().await;
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
                    drop(workers_guard);
                    drop(state_guard);
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
                            
                            info!("[auth] Register: user={}, worker={}", username, worker_name);
                            
                            let authorized = admin_users.iter().any(|u| u == &username.to_lowercase());
                            if !authorized {
                                info!("[auth] Register rejected: user '{}' not authorized", username);
                                (403, r#"{"error":"user not authorized"}"#.to_string())
                            } else if let Some(ref duo_cfg) = duo_config {
                                info!("[auth] Sending Duo push to '{}'...", username);
                                match duo::auth_push(duo_cfg, &username).await {
                                    Ok(result) => {
                                        if result.allowed {
                                            info!("[auth] Duo approved for '{}' ({})", username, result.status);
                                            let resp = serde_json::json!({
                                                "status": "provisioned",
                                                "username": username,
                                                "worker_id": format!("{}:{}", username, worker_name),
                                                "rpc_port": 50052
                                            });
                                            (200, serde_json::to_string(&resp).unwrap_or_default())
                                        } else {
                                            info!("[auth] Duo denied for '{}' ({})", username, result.status);
                                            (403, serde_json::json!({"error": format!("Duo denied: {}", result.status)}).to_string())
                                        }
                                    }
                                    Err(e) => {
                                        error!("[auth] Duo API error for '{}': {}", username, e);
                                        (500, serde_json::json!({"error": format!("Duo error: {}", e)}).to_string())
                                    }
                                }
                            } else {
                                info!("[auth] No Duo configured, auto-approving '{}'", username);
                                let resp = serde_json::json!({
                                    "status": "provisioned",
                                    "username": username,
                                    "worker_id": format!("{}:{}", username, worker_name),
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
                            info!("[auth] Login: user={}", username);
                            
                            let authorized = admin_users.iter().any(|u| u == &username.to_lowercase());
                            if !authorized {
                                info!("[auth] Login rejected: user '{}' not in admin list", username);
                                (403, r#"{"error":"user not authorized"}"#.to_string())
                            } else if let Some(ref duo_cfg) = duo_config {
                                info!("[auth] Sending Duo push to '{}'...", username);
                                match duo::auth_push(duo_cfg, &username).await {
                                    Ok(result) => {
                                        if result.allowed {
                                            info!("[auth] Duo approved for '{}'", username);
                                            let resp = serde_json::json!({
                                                "status": "approved",
                                                "username": username,
                                                "token": format!("token-{}", rand::random::<u64>())
                                            });
                                            (200, serde_json::to_string(&resp).unwrap_or_default())
                                        } else {
                                            info!("[auth] Duo denied for '{}': {}", username, result.status);
                                            (403, serde_json::json!({"error": format!("Duo denied: {}", result.status)}).to_string())
                                        }
                                    }
                                    Err(e) => {
                                        error!("[auth] Duo API error for '{}': {}", username, e);
                                        (500, serde_json::json!({"error": format!("Duo error: {}", e)}).to_string())
                                    }
                                }
                            } else {
                                info!("[auth] No Duo configured, auto-approving '{}'", username);
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
                } else if path.starts_with("POST /auth/vpn") {
                    match serde_json::from_str::<serde_json::Value>(body) {
                        Ok(json) => {
                            let username = json["username"].as_str().unwrap_or("").to_string();
                            let worker_name = json["worker_name"].as_str().unwrap_or("").to_string();
                            info!("[auth] VPN enrollment: user={}, worker={}", username, worker_name);

                            let authorized = admin_users.iter().any(|u| u == &username.to_lowercase());
                            if !authorized {
                                (403, r#"{"error":"user not authorized"}"#.to_string())
                            } else {
                                let duo_ok = if let Some(ref duo_cfg) = duo_config {
                                    info!("[auth] VPN: sending Duo push to '{}'...", username);
                                    match duo::auth_push(duo_cfg, &username).await {
                                        Ok(result) if result.allowed => {
                                            info!("[auth] VPN: Duo approved for '{}'", username);
                                            true
                                        }
                                        Ok(result) => {
                                            info!("[auth] VPN: Duo denied for '{}': {}", username, result.status);
                                            false
                                        }
                                        Err(e) => {
                                            error!("[auth] VPN: Duo API error: {}", e);
                                            false
                                        }
                                    }
                                } else {
                                    info!("[auth] VPN: No Duo, auto-approving '{}'", username);
                                    true
                                };

                                if !duo_ok {
                                    (403, serde_json::json!({"error": "Duo denied"}).to_string())
                                } else if wg_easy_password.is_empty() {
                                    (503, r#"{"error":"WireGuard VPN not configured on hub"}"#.to_string())
                                } else {
                                    match create_wireguard_client(&wg_easy_host, &wg_easy_password, &format!("akai-agent-{}", worker_name)).await {
                                        Ok((client_id, config_text)) => {
                                            info!("[auth] VPN: created WG client {} for '{}'", client_id, username);
                                            let resp = serde_json::json!({
                                                "status": "enrolled",
                                                "client_id": client_id,
                                                "wireguard_config": config_text,
                                                "hub_vpn_addr": hub_worker_vpn_addr,
                                            });
                                            (200, serde_json::to_string(&resp).unwrap_or_default())
                                        }
                                        Err(e) => {
                                            error!("[auth] VPN: failed to create WG client: {}", e);
                                            (500, serde_json::json!({"error": format!("WG client creation failed: {}", e)}).to_string())
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to parse auth VPN request: {}", e);
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
                                info!("[admin] Model change rejected: user '{}' not authorized", username);
                                (403, r#"{"error":"user not authorized"}"#.to_string())
                            } else if let Some(ref duo_cfg) = duo_config {
                                info!("[admin] Model change: sending Duo push to '{}'...", username);
                                match duo::auth_push(duo_cfg, &username).await {
                                    Ok(result) if result.allowed => {
                                        let name = json["name"].as_str().unwrap_or("unknown").to_string();
                                        let layers = json["layers"].as_u64().unwrap_or(32) as usize;
                                        let url = json["url"].as_str().unwrap_or("").to_string();

                                        let mut state_guard = state.lock().await;
                                        state_guard.model.name = name;
                                        state_guard.model.num_layers = layers;
                                        state_guard.model_url = url;

                                        info!("[admin] Model updated by {} (Duo approved): {} ({} layers)", username, state_guard.model.name, layers);

                                        let resp = serde_json::json!({
                                            "status": "ok",
                                            "model": state_guard.model.name,
                                            "layers": layers,
                                        });
                                        (200, serde_json::to_string(&resp).unwrap_or_default())
                                    }
                                    Ok(result) => {
                                        info!("[admin] Duo denied model change for '{}': {}", username, result.status);
                                        (403, serde_json::json!({"error": format!("Duo denied: {}", result.status)}).to_string())
                                    }
                                    Err(e) => {
                                        error!("[admin] Duo API error for '{}': {}", username, e);
                                        (500, serde_json::json!({"error": format!("Duo error: {}", e)}).to_string())
                                    }
                                }
                            } else {
                                let name = json["name"].as_str().unwrap_or("unknown").to_string();
                                let layers = json["layers"].as_u64().unwrap_or(32) as usize;
                                let url = json["url"].as_str().unwrap_or("").to_string();

                                let mut state_guard = state.lock().await;
                                state_guard.model.name = name;
                                state_guard.model.num_layers = layers;
                                state_guard.model_url = url;

                                info!("[admin] Model updated by {}: {} ({} layers)", username, state_guard.model.name, layers);

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
                } else if path.starts_with("GET /tunnel/certs") {
                    let ca = std::fs::read(format!("{}/ca.crt", tunnel_certs_dir)).unwrap_or_default();
                    let worker_cert = std::fs::read(format!("{}/worker.crt", tunnel_certs_dir)).unwrap_or_default();
                    let worker_key = std::fs::read(format!("{}/worker.key", tunnel_certs_dir)).unwrap_or_default();
                    if ca.is_empty() {
                        (500, r#"{"error":"tunnel certs not available"}"#.to_string())
                    } else {
                        let resp = serde_json::json!({
                            "ca_cert": String::from_utf8_lossy(&ca).to_string(),
                            "worker_cert": String::from_utf8_lossy(&worker_cert).to_string(),
                            "worker_key": String::from_utf8_lossy(&worker_key).to_string(),
                            "tunnel_host": "tunnel.akinus21.com",
                            "tunnel_port": 443,
                        });
                        (200, serde_json::to_string(&resp).unwrap_or_default())
                    }
                } else if path.starts_with("GET /pipeline/status") {
                    // Return current pipeline registry for monitoring
                    let state_guard = state.lock().await;
                    let workers_guard = workers.read().await;
                    let mut worker_list: Vec<_> = workers_guard.values().cloned().collect();
                    worker_list.sort_by(|a, b| {
                        let a_score = if a.has_gpu { a.vram_gb * 100.0 } else { 1.0 };
                        let b_score = if b.has_gpu { b.vram_gb * 100.0 } else { 1.0 };
                        a_score.partial_cmp(&b_score).unwrap()
                    });
                    let pipeline = build_pipeline_info(
                        &worker_list,
                        &state_guard.model.name,
                        &state_guard.model_url,
                        state_guard.model.num_layers,
                    );
                    drop(workers_guard);
                    drop(state_guard);
                    (200, serde_json::to_string(&pipeline).unwrap_or_default())
                } else if path.starts_with("GET /model/download") {
                    let state_guard = state.lock().await;
                    let model_url = state_guard.model_url.clone();
                    drop(state_guard);
                    
                    if model_url.is_empty() {
                        (404, r#"{"error":"no model configured"}"#.to_string())
                    } else {
                        let client = reqwest::Client::new();
                        match client.get(&model_url)
                            .timeout(Duration::from_secs(600))
                            .send().await
                        {
                            Ok(resp) => {
                                if resp.status().is_success() {
                                    let total = resp.content_length().unwrap_or(0);
                                    info!("[model-proxy] Downloading {} ({:.1} MB)...", model_url, total as f64 / 1_048_576.0);
                                    match resp.bytes().await {
                                        Ok(bytes) => {
                                            let hash = sha256_hex(&bytes);
                                            {
                                                let mut state_guard = state.lock().await;
                                                state_guard.model_hash = hash.clone();
                                            }
                                            info!("[model-proxy] Downloaded {} bytes, hash={}, proxying to worker", bytes.len(), &hash[..12]);
                                            let header = format!(
                                                "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nX-Model-Hash: {}\r\n\r\n",
                                                bytes.len(), hash
                                            );
                                            let _ = stream.write_all(header.as_bytes()).await;
                                            let _ = stream.write_all(&bytes).await;
                                            continue;
                                        }
                                        Err(e) => {
                                            error!("[model-proxy] Failed to read model body: {}", e);
                                            (502, serde_json::json!({"error": format!("model download failed: {}", e)}).to_string())
                                        }
                                    }
                                } else {
                                    error!("[model-proxy] Upstream returned status {}", resp.status());
                                    (502, serde_json::json!({"error": format!("upstream status: {}", resp.status())}).to_string())
                                }
                            }
                            Err(e) => {
                                error!("[model-proxy] Failed to fetch model: {}", e);
                                (502, serde_json::json!({"error": format!("model fetch failed: {}", e)}).to_string())
                            }
                        }
                    }
                } else if path.starts_with("POST /v1/chat/completions") {
                    match serde_json::from_str::<serde_json::Value>(body) {
                        Ok(json) => {
                            let model = json["model"].as_str().unwrap_or("unknown");
                            let messages = json["messages"].as_array().cloned().unwrap_or_default();
                            let max_tokens = json["max_tokens"].as_u64().unwrap_or(128) as usize;
                            let temperature = json["temperature"].as_f64().unwrap_or(0.7) as f32;

                            info!("Chat completion request: model={}, messages={}, max_tokens={}", model, messages.len(), max_tokens);

                            // Find first worker in pipeline
                            let state_guard = state.lock().await;
                            let workers_guard = workers.read().await;
                            let mut worker_list: Vec<_> = workers_guard.values().cloned().collect();
                            worker_list.sort_by(|a, b| {
                                let a_score = if a.has_gpu { a.vram_gb * 100.0 } else { 1.0 };
                                let b_score = if b.has_gpu { b.vram_gb * 100.0 } else { 1.0 };
                                a_score.partial_cmp(&b_score).unwrap()
                            });
                            let pipeline = build_pipeline_info(
                                &worker_list,
                                &state_guard.model.name,
                                &state_guard.model_url,
                                state_guard.model.num_layers,
                            );
                            drop(state_guard);
                            drop(workers_guard);

                            let first_worker = pipeline.workers.first();
                            if first_worker.is_none() {
                                let resp = serde_json::json!({
                                    "error": {"message": "No workers available", "type": "server_error"}
                                });
                                (503, serde_json::to_string(&resp).unwrap_or_default())
                            } else if let Some(first) = first_worker {
                                let streams_guard = streams.read().await;
                                let writer = streams_guard.get(&first.worker_id).cloned();
                                drop(streams_guard);

                                match writer {
                                    Some(writer) => {
                                        let request_id = format!("inf-{}", rand::random::<u64>());

                                        let (tx, rx) = oneshot::channel();
                                        pending.lock().await.insert(request_id.clone(), tx);

                                        let prompt = messages.iter().filter_map(|m| {
                                            m.get("content").and_then(|c| c.as_str())
                                        }).collect::<Vec<_>>().join("\n");

                                        let req = pipeline::InferenceRequest {
                                            id: request_id.clone(),
                                            tokens: vec![],
                                            is_first: true,
                                            is_last: true,
                                            max_new_tokens: max_tokens,
                                            temperature,
                                            prompt: Some(prompt),
                                        };

                                        let msg = HubMessage::InferenceRequest(req);
                                        let data = match encode_msg(&msg) {
                                            Ok(d) => d,
                                            Err(e) => {
                                                pending.lock().await.remove(&request_id);
                                                error!("Failed to serialize inference request: {}", e);
                                                let resp = serde_json::json!({
                                                    "error": {"message": format!("Internal error: {}", e), "type": "server_error"}
                                                });
                                                let body = serde_json::to_string(&resp).unwrap_or_default();
                                                let response = format!(
                                                    "HTTP/1.1 500 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                                                    body.len(), body
                                                );
                                                let _ = stream.write_all(response.as_bytes()).await;
                                                continue;
                                            }
                                        };

                                        match {
                                            let mut w = writer.lock().await;
                                            w.write_all(&data).await
                                        } {
                                            Ok(_) => {
                                                info!("Sent inference request {} to {}", request_id, first.worker_id);

                                                match tokio::time::timeout(Duration::from_secs(120), rx).await {
                                                    Ok(Ok(inf_resp)) => {
                                                        let content = inf_resp.text.unwrap_or_default();
                                                        let resp = serde_json::json!({
                                                            "id": format!("chatcmpl-{}", rand::random::<u64>()),
                                                            "object": "chat.completion",
                                                            "created": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
                                                            "model": model,
                                                            "choices": [{
                                                                "index": 0,
                                                                "message": {
                                                                    "role": "assistant",
                                                                    "content": content,
                                                                },
                                                                "finish_reason": "stop"
                                                            }],
                                                            "usage": {
                                                                "prompt_tokens": inf_resp.prompt_tokens,
                                                                "completion_tokens": inf_resp.completion_tokens,
                                                                "total_tokens": inf_resp.prompt_tokens + inf_resp.completion_tokens,
                                                            }
                                                        });
                                                        (200, serde_json::to_string(&resp).unwrap_or_default())
                                                    }
                                                    Ok(Err(_)) => {
                                                        error!("Inference request {} channel dropped", request_id);
                                                        let resp = serde_json::json!({
                                                            "error": {"message": "Worker disconnected", "type": "server_error"}
                                                        });
                                                        (502, serde_json::to_string(&resp).unwrap_or_default())
                                                    }
                                                    Err(_) => {
                                                        error!("Inference request {} timed out", request_id);
                                                        pending.lock().await.remove(&request_id);
                                                        let resp = serde_json::json!({
                                                            "error": {"message": "Request timed out", "type": "server_error"}
                                                        });
                                                        (504, serde_json::to_string(&resp).unwrap_or_default())
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                error!("Failed to send inference request to worker: {}", e);
                                                pending.lock().await.remove(&request_id);
                                                let resp = serde_json::json!({
                                                    "error": {"message": "Failed to send request to worker", "type": "server_error"}
                                                });
                                                (500, serde_json::to_string(&resp).unwrap_or_default())
                                            }
                                        }
                                    }
                                    None => {
                                        let resp = serde_json::json!({
                                            "error": {"message": "Worker not connected", "type": "server_error"}
                                        });
                                        (503, serde_json::to_string(&resp).unwrap_or_default())
                                    }
                                }
                            } else {
                                let resp = serde_json::json!({
                                    "error": {"message": "No workers available", "type": "server_error"}
                                });
                                (503, serde_json::to_string(&resp).unwrap_or_default())
                            }
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

                let reason = match status {
                    200 => "OK",
                    400 => "Bad Request",
                    403 => "Forbidden",
                    404 => "Not Found",
                    500 => "Internal Server Error",
                    502 => "Bad Gateway",
                    503 => "Service Unavailable",
                    504 => "Gateway Timeout",
                    _ => "OK",
                };
                let response = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    status,
                    reason,
                    resp_body.len(),
                    resp_body
                );
                stream.write_all(response.as_bytes()).await?;
            }
            Err(e) => error!("HTTP connection error: {}", e),
        }
    }
}

async fn create_wireguard_client(wg_easy_host: &str, wg_easy_password: &str, name: &str) -> Result<(String, String)> {
    let client = reqwest::Client::new();
    let base = format!("http://{}", wg_easy_host);

    let login_resp = client.post(format!("{}/api/session", base))
        .json(&serde_json::json!({"password": wg_easy_password}))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !login_resp.status().is_success() {
        anyhow::bail!("wg-easy auth failed: {}", login_resp.status());
    }

    let cookies: Vec<String> = login_resp.headers()
        .get_all("set-cookie")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .filter_map(|v| v.split(';').next())
        .map(|s| s.trim().to_string())
        .collect();
    let cookie_header = cookies.join("; ");

    let create_resp = client.post(format!("{}/api/wireguard/client", base))
        .header("cookie", &cookie_header)
        .json(&serde_json::json!({"name": name}))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !create_resp.status().is_success() {
        anyhow::bail!("wg-easy create client failed: {}", create_resp.status());
    }

    let clients_resp = client.get(format!("{}/api/wireguard/client", base))
        .header("cookie", &cookie_header)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !clients_resp.status().is_success() {
        anyhow::bail!("wg-easy list clients failed: {}", clients_resp.status());
    }

    let clients: serde_json::Value = clients_resp.json().await?;
    let clients_arr = clients.as_array().ok_or_else(|| anyhow::anyhow!("invalid clients response"))?;

    let found = clients_arr.iter().find(|c| c["name"].as_str() == Some(name))
        .ok_or_else(|| anyhow::anyhow!("created client not found in list"))?;

    let client_id = found["id"].as_str().ok_or_else(|| anyhow::anyhow!("missing client id"))?.to_string();

    let config_resp = client.get(format!("{}/api/wireguard/client/{}/configuration", base, client_id))
        .header("cookie", &cookie_header)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !config_resp.status().is_success() {
        anyhow::bail!("wg-easy get config failed: {}", config_resp.status());
    }

    let config_text = config_resp.text().await?;

    Ok((client_id, config_text))
}

async fn remove_wireguard_client(wg_easy_host: &str, wg_easy_password: &str, client_id: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let base = format!("http://{}", wg_easy_host);

    let login_resp = client.post(format!("{}/api/session", base))
        .json(&serde_json::json!({"password": wg_easy_password}))
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !login_resp.status().is_success() {
        anyhow::bail!("wg-easy auth failed: {}", login_resp.status());
    }

    let cookies: Vec<String> = login_resp.headers()
        .get_all("set-cookie")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .filter_map(|v| v.split(';').next())
        .map(|s| s.trim().to_string())
        .collect();
    let cookie_header = cookies.join("; ");

    let delete_resp = client.delete(format!("{}/api/wireguard/client/{}", base, client_id))
        .header("cookie", &cookie_header)
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !delete_resp.status().is_success() {
        anyhow::bail!("wg-easy delete client failed: {}", delete_resp.status());
    }

    Ok(())
}
