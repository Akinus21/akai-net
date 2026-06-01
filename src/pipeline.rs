use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub id: String,
    pub layer_offset: usize,
    pub num_layers: usize,
    pub vram_gb: f32,
    pub has_gpu: bool,
    #[serde(default)]
    pub load: f32,
    #[serde(default)]
    pub active: bool,
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
    #[serde(rename = "error")]
    Error {
        code: String,
        message: String,
    },
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub layer_offset: usize,
    pub num_layers: usize,
    pub reassign: bool,
    pub model_name: String,
    pub model_url: String,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_layer_assignment_sorted_by_capacity() {
        let workers = vec![
            WorkerInfo {
                id: "pi".to_string(),
                layer_offset: 0,
                num_layers: 0,
                vram_gb: 0.0,
                has_gpu: false,
            },
            WorkerInfo {
                id: "desktop".to_string(),
                layer_offset: 0,
                num_layers: 0,
                vram_gb: 16.0,
                has_gpu: true,
            },
            WorkerInfo {
                id: "phone".to_string(),
                layer_offset: 0,
                num_layers: 0,
                vram_gb: 4.0,
                has_gpu: false,
            },
        ];
        let assignments = calculate_layer_assignment(&workers, 32);
        assert_eq!(assignments.len(), 3);
        assert_eq!(assignments[0].0, "pi");
        assert_eq!(assignments[1].0, "phone");
        assert_eq!(assignments[2].0, "desktop");
    }
}