use anyhow::Result;
use crate::pipeline::{HubMessage, WorkerInfo};
use std::env;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, error, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .init();

    let hub_addr = env::var("HUB_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".to_string());
    let worker_id = env::var("WORKER_ID").unwrap_or_else(|| "unknown-worker".to_string());
    let has_gpu = env::var("HAS_GPU").unwrap_or_else(|_| "false".to_string()) == "true";
    let vram_gb = env::var("VRAM_GB").unwrap_or_else(|_| "0".to_string()).parse().unwrap_or(0.0);
    let layer_offset: usize = env::var("LAYER_OFFSET").unwrap_or_else(|_| "0".to_string()).parse().unwrap_or(0);
    let num_layers: usize = env::var("NUM_LAYERS").unwrap_or_else(|_| "32".to_string()).parse().unwrap_or(32);
    let model_path = env::var("MODEL_PATH").unwrap_or_else(|_| "".to_string());

    info!("Akai-Net Worker starting...");
    info!("  Worker ID: {}", worker_id);
    info!("  Hub: {}", hub_addr);
    info!("  GPU: {}, VRAM: {:.1} GB", has_gpu, vram_gb);
    info!("  Layers: {} to {} ({})", layer_offset, layer_offset + num_layers, num_layers);

    let worker_info = WorkerInfo {
        id: worker_id.clone(),
        layer_offset,
        num_layers,
        vram_gb,
        has_gpu,
    };

    loop {
        match TcpStream::connect(&hub_addr).await {
            Ok(mut stream) => {
                info!("Connected to hub at {}", hub_addr);

                let register = HubMessage::Register(worker_info.clone());
                let data = serde_json::to_vec(&register)?;
                stream.write_all(&data).await?;
                info!("Sent registration to hub");

                let mut buf = vec![0u8; 65536];
                while let Ok(n) = stream.read(&mut buf).await {
                    if n == 0 {
                        warn!("Connection closed by hub");
                        break;
                    }
                    let msg: HubMessage = match serde_json::from_slice(&buf[..n]) {
                        Ok(m) => m,
                        Err(e) => {
                            error!("Failed to parse message: {}", e);
                            continue;
                        }
                    };

                    match msg {
                        HubMessage::InferenceRequest(req) => {
                            info!("Received inference request {} ({} tokens)", req.id, req.tokens.len());
                        }
                        HubMessage::Heartbeat { .. } => {
                            let resp = HubMessage::Heartbeat {
                                worker_id: worker_id.clone(),
                                load: 0.5,
                                active: true,
                            };
                            let data = serde_json::to_vec(&resp)?;
                            stream.write_all(&data).await?;
                        }
                        HubMessage::Error { code, message } => {
                            error!("Hub error {}: {}", code, message);
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                error!("Failed to connect to hub: {}", e);
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}