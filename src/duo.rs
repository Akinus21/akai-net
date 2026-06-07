use anyhow::{bail, Context, Result};
use base64::Engine;
use chrono::Utc;
use hmac::{Hmac, Mac};
use sha1::Sha1;

type HmacSha1 = Hmac<Sha1>;

pub struct DuoConfig {
    pub ikey: String,
    pub skey: String,
    pub host: String,
}

pub struct DuoAuthResult {
    pub allowed: bool,
    pub status: String,
}

fn duo_sign(ikey: &str, skey: &str, date: &str, method: &str, host: &str, path: &str, params: &str) -> String {
    let canon = format!("{date}\n{method}\n{host}\n{path}\n{params}");
    let mut mac = HmacSha1::new_from_slice(skey.as_bytes()).expect("HMAC key");
    mac.update(canon.as_bytes());
    let sig = mac.finalize().into_bytes();
    let hex_sig: String = sig.iter().map(|b| format!("{:02x}", b)).collect();
    let auth = format!("{}:{}", ikey, hex_sig);
    format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(auth))
}

pub async fn auth_push(config: &DuoConfig, username: &str) -> Result<DuoAuthResult> {
    let date = Utc::now().format("%a, %d %b %Y %H:%M:%S %z").to_string();
    let method = "POST";
    let path = "/auth/v2/auth";
    let encoded_user = percent_encode(username);
    let params = format!("device=auto&factor=push&username={}", encoded_user);
    let auth_header = duo_sign(&config.ikey, &config.skey, &date, method, &config.host.to_lowercase(), path, &params);

    let url = format!("https://{}{}", config.host, path);
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .header("Date", &date)
        .header("Authorization", auth_header)
        .form(&[("factor", "push"), ("device", "auto"), ("username", username)])
        .timeout(std::time::Duration::from_secs(120))
        .send()
        .await
        .context("Duo API request failed")?;

    let json: serde_json::Value = resp.json().await.context("Failed to parse Duo response")?;

    let stat = json["stat"].as_str().unwrap_or("FAIL");
    if stat != "OK" {
        let msg = json["message"].as_str().unwrap_or("unknown error");
        bail!("Duo API error: {}", msg);
    }

    let result = json["response"].as_object();
    if result.is_none() {
        bail!("Duo response missing 'response' field");
    }
    let result = result.unwrap();

    let allowed = result["result"].as_str() == Some("allow");
    let status = result["status"].as_str().unwrap_or("unknown").to_string();

    Ok(DuoAuthResult { allowed, status })
}

pub fn load_duo_config() -> Option<DuoConfig> {
    let ikey = std::env::var("DUO_IKEY").ok()?;
    let skey = std::env::var("DUO_SKEY").ok()?;
    let host = std::env::var("DUO_HOST").ok()?;
    if ikey.is_empty() || skey.is_empty() || host.is_empty() {
        return None;
    }
    Some(DuoConfig { ikey, skey, host })
}

fn percent_encode(s: &str) -> String {
    s.chars().map(|c| {
        if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '~' {
            c.to_string()
        } else {
            format!("%{:02X}", c as u8)
        }
    }).collect()
}