use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub layer_offset: usize,
    #[serde(default)]
    pub num_layers: usize,
    #[serde(default)]
    pub vram_gb: f32,
    #[serde(default)]
    pub has_gpu: bool,
    #[serde(default)]
    pub load: f32,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub wg_ip: String,
    #[serde(default)]
    pub wg_peer_id: String,
    #[serde(default)]
    pub models: Vec<String>,
    #[serde(default)]
    pub rpc_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum HubMessage {
    #[serde(rename = "register")]
    Register(WorkerInfo),
    #[serde(rename = "inference_request")]
    InferenceRequest(InferenceRequest),
    #[serde(rename = "inference_response")]
    InferenceResponse(InferenceResponse),
    #[serde(rename = "heartbeat")]
    Heartbeat(WorkerHeartbeat),
    #[serde(rename = "heartbeat_response")]
    HeartbeatResponse(HeartbeatResponse),
    #[serde(rename = "pipeline_info")]
    PipelineInfo(PipelineInfo),
    #[serde(rename = "heartbeat_forward")]
    HeartbeatForward { pipeline: PipelineInfo },
    #[serde(rename = "error")]
    Error {
        code: String,
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceRequest {
    pub id: String,
    pub tokens: Vec<i64>,
    pub is_first: bool,
    pub is_last: bool,
    pub max_new_tokens: usize,
    pub temperature: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceResponse {
    pub id: String,
    pub token: Option<i64>,
    pub hidden_states: Option<Vec<f32>>,
    pub is_done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubInitiate {
    pub worker_id: String,
    pub layer_offset: usize,
    pub num_layers: usize,
    pub next_hop: Option<HopInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HopInfo {
    pub worker_id: String,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineInfo {
    pub pipeline_id: String,
    pub workers: Vec<PipelineWorker>,
    pub model_name: String,
    pub model_url: String,
    pub num_layers: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineWorker {
    pub worker_id: String,
    pub layer_offset: usize,
    pub num_layers: usize,
    pub last_hop: Option<HopInfo>,
    pub next_hop: Option<HopInfo>,
    pub is_first: bool,
    pub is_last: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerHeartbeat {
    pub worker_id: String,
    pub load: f32,
    pub layer_offset: usize,
    pub num_layers: usize,
    pub has_gpu: bool,
    pub vram_gb: f32,
    pub active: bool,
    pub last_hop_connected: bool,
    pub next_hop_connected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub layer_offset: usize,
    pub num_layers: usize,
    pub reassign: bool,
    pub model_name: String,
    pub model_url: String,
    pub pipeline: Option<PipelineInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    pub name: String,
    pub num_layers: usize,
    pub hidden_size: usize,
    pub num_heads: usize,
    pub vocab_size: usize,
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            name: "unknown".to_string(),
            num_layers: 32,
            hidden_size: 4096,
            num_heads: 32,
            vocab_size: 32000,
        }
    }
}

pub const PROTOCOL_VERSION: &str = "1.0";

pub fn calculate_layer_assignment(
    workers: &[WorkerInfo],
    total_layers: usize,
) -> Vec<(String, usize, usize)> {
    let mut assignments = Vec::new();
    let mut sorted_workers: Vec<_> = workers.iter().collect();

    sorted_workers.sort_by(|a, b| {
        let a_score = if a.has_gpu { a.vram_gb * 100.0 } else { 1.0 };
        let b_score = if b.has_gpu { b.vram_gb * 100.0 } else { 1.0 };
        a_score.partial_cmp(&b_score).unwrap()
    });

    let base = total_layers / sorted_workers.len();
    let remainder = total_layers % sorted_workers.len();

    let mut offset = 0usize;
    for (i, worker) in sorted_workers.iter().enumerate() {
        let num = base + if i < remainder { 1 } else { 0 };
        assignments.push((worker.id.clone(), offset, num));
        offset += num;
    }

    assignments
}

pub fn build_pipeline_info(
    workers: &[WorkerInfo],
    model_name: &str,
    model_url: &str,
    num_layers: usize,
) -> PipelineInfo {
    let sorted_workers: Vec<_> = {
        let mut s = workers.iter().collect::<Vec<_>>();
        s.sort_by(|a, b| {
            let a_score = if a.has_gpu { a.vram_gb * 100.0 } else { 1.0 };
            let b_score = if b.has_gpu { b.vram_gb * 100.0 } else { 1.0 };
            a_score.partial_cmp(&b_score).unwrap()
        });
        s
    };

    let pipeline_workers: Vec<PipelineWorker> = sorted_workers
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let is_first = i == 0;
            let is_last = i == sorted_workers.len() - 1;

            let last_hop = if !is_first {
                sorted_workers.get(i - 1).map(|prev| HopInfo {
                    worker_id: prev.id.clone(),
                    host: prev.wg_ip.clone(),
                    port: prev.rpc_port,
                })
            } else {
                None
            };

            let next_hop = if !is_last {
                sorted_workers.get(i + 1).map(|next| HopInfo {
                    worker_id: next.id.clone(),
                    host: next.wg_ip.clone(),
                    port: next.rpc_port,
                })
            } else {
                None
            };

            PipelineWorker {
                worker_id: w.id.clone(),
                layer_offset: w.layer_offset,
                num_layers: w.num_layers,
                last_hop,
                next_hop,
                is_first,
                is_last,
            }
        })
        .collect();

    PipelineInfo {
        pipeline_id: "main".to_string(),
        workers: pipeline_workers,
        model_name: model_name.to_string(),
        model_url: model_url.to_string(),
        num_layers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_layer_assignment_sorted_by_capacity() {
        let workers = vec![
            WorkerInfo {
                id: "pi".to_string(),
                name: "".to_string(),
                layer_offset: 0,
                num_layers: 0,
                vram_gb: 0.0,
                has_gpu: false,
                ..Default::default()
            },
            WorkerInfo {
                id: "desktop".to_string(),
                name: "".to_string(),
                layer_offset: 0,
                num_layers: 0,
                vram_gb: 16.0,
                has_gpu: true,
                ..Default::default()
            },
            WorkerInfo {
                id: "phone".to_string(),
                name: "".to_string(),
                layer_offset: 0,
                num_layers: 0,
                vram_gb: 4.0,
                has_gpu: false,
                ..Default::default()
            },
        ];
        let assignments = calculate_layer_assignment(&workers, 32);
        assert_eq!(assignments.len(), 3);
        assert_eq!(assignments[0].0, "pi");
        assert_eq!(assignments[1].0, "phone");
        assert_eq!(assignments[2].0, "desktop");
    }
}
