use std::collections::BTreeMap;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{SecondsFormat, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

use crate::wire::{DeviceDescriptor, DeviceRef};

pub const DEFAULT_LEASE_STATE_BUCKET: &str = "deckr_lease_v1";
pub const DEFAULT_DISCOVERY_STATE_BUCKET: &str = "deckr_discovery_v1";
pub const STATE_TTL_SECONDS: u64 = 30;
pub const HEARTBEAT_SECONDS: u64 = 5;
pub const STATE_RECONCILE_SECONDS: u64 = 1;
pub const WATCH_RETRY_SECONDS: u64 = 1;
pub const HARDWARE_MESSAGES_LANE: &str = "hardware_messages";

static SAFE_TOKEN_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z0-9][A-Za-z0-9_-]*$").unwrap());
static PROVIDER_INSTANCE_ID_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z0-9][A-Za-z0-9._-]*$").unwrap());

const CORE_ENDPOINT_FAMILIES: &[&str] = &[
    "action_provider",
    "controller",
    "hardware_manager",
    "service",
];
const RESERVED_ACTION_PROVIDER_INSTANCE_IDS: &[&str] = &["dev.deckr.controller.builtin"];

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EndpointAddress {
    pub address: String,
    pub family: String,
    pub endpoint_id: String,
}

impl EndpointAddress {
    pub fn parse(value: impl AsRef<str>) -> Option<Self> {
        let value = value.as_ref();
        if value.trim() != value {
            return None;
        }
        let (family, endpoint_id) = value.split_once(':')?;
        if !CORE_ENDPOINT_FAMILIES.contains(&family)
            || endpoint_id.is_empty()
            || endpoint_id.trim() != endpoint_id
            || endpoint_id.contains(':')
        {
            return None;
        }
        if family == "action_provider"
            && (!PROVIDER_INSTANCE_ID_RE.is_match(endpoint_id)
                || RESERVED_ACTION_PROVIDER_INSTANCE_IDS.contains(&endpoint_id))
        {
            return None;
        }
        Some(Self {
            address: value.to_string(),
            family: family.to_string(),
            endpoint_id: endpoint_id.to_string(),
        })
    }
}

pub fn hardware_manager_address(manager_id: &str) -> String {
    format!("hardware_manager:{manager_id}")
}

pub fn encode_key_token(raw: &str) -> String {
    if SAFE_TOKEN_RE.is_match(raw) && !raw.starts_with("b64_") {
        raw.to_string()
    } else {
        format!("b64_{}", URL_SAFE_NO_PAD.encode(raw.as_bytes()))
    }
}

pub fn decode_key_token(token: &str) -> Option<String> {
    if !token.starts_with("b64_") {
        return Some(token.to_string());
    }
    let bytes = URL_SAFE_NO_PAD.decode(&token.as_bytes()[4..]).ok()?;
    String::from_utf8(bytes).ok()
}

pub fn presence_endpoint_key(lane: &str, endpoint: &str) -> Option<String> {
    let endpoint = EndpointAddress::parse(endpoint)?;
    Some(format!(
        "presence.endpoint.{}.{}.{}",
        encode_key_token(lane),
        encode_key_token(&endpoint.family),
        encode_key_token(&endpoint.endpoint_id)
    ))
}

pub fn controller_presence_prefix() -> String {
    format!(
        "presence.endpoint.{}.{}.",
        encode_key_token(HARDWARE_MESSAGES_LANE),
        encode_key_token("controller")
    )
}

pub fn hardware_inventory_key(manager_id: &str) -> String {
    format!("inventory.hardware.{}", encode_key_token(manager_id))
}

pub fn device_claim_prefix(manager_id: &str) -> String {
    format!("claim.device.{}.", encode_key_token(manager_id))
}

pub fn parse_device_claim_key(key: &str) -> Option<(String, String)> {
    let mut parts = key.split('.');
    if parts.next()? != "claim" || parts.next()? != "device" {
        return None;
    }
    let manager_id = decode_key_token(parts.next()?)?;
    let device_id = decode_key_token(parts.next()?)?;
    if parts.next().is_some() {
        return None;
    }
    Some((manager_id, device_id))
}

pub fn parse_presence_endpoint_key(key: &str) -> Option<(String, EndpointAddress)> {
    let mut parts = key.split('.');
    if parts.next()? != "presence" || parts.next()? != "endpoint" {
        return None;
    }
    let lane = decode_key_token(parts.next()?)?;
    let family = decode_key_token(parts.next()?)?;
    let endpoint_id = decode_key_token(parts.next()?)?;
    if parts.next().is_some() {
        return None;
    }
    let endpoint = EndpointAddress::parse(format!("{family}:{endpoint_id}"))?;
    Some((lane, endpoint))
}

pub fn timestamp_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EndpointPresence {
    pub endpoint: String,
    pub lane: String,
    pub session_id: String,
    pub timestamp: String,
    pub ttl_seconds: u64,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

impl EndpointPresence {
    pub fn manager(manager_id: &str, session_id: &str) -> Self {
        let mut metadata = BTreeMap::new();
        metadata.insert("runtime".to_string(), "deckr-mirabox-manager".to_string());
        metadata.insert("version".to_string(), env!("CARGO_PKG_VERSION").to_string());
        Self {
            endpoint: hardware_manager_address(manager_id),
            lane: HARDWARE_MESSAGES_LANE.to_string(),
            session_id: session_id.to_string(),
            timestamp: timestamp_now(),
            ttl_seconds: STATE_TTL_SECONDS,
            metadata,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HardwareInventoryDevice {
    pub device_ref: DeviceRef,
    pub descriptor: DeviceDescriptor,
}

impl HardwareInventoryDevice {
    pub fn from_device(manager_id: &str, device: &DeviceDescriptor) -> Self {
        Self {
            device_ref: DeviceRef {
                manager_id: manager_id.to_string(),
                device_id: device.device_id.clone(),
                fingerprint: Some(device.fingerprint.clone()),
            },
            descriptor: device.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HardwareInventory {
    pub manager_id: String,
    pub manager_endpoint: String,
    pub session_id: String,
    pub timestamp: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub devices: BTreeMap<String, HardwareInventoryDevice>,
}

impl HardwareInventory {
    pub fn new(
        manager_id: &str,
        session_id: &str,
        devices: BTreeMap<String, HardwareInventoryDevice>,
    ) -> Self {
        Self {
            manager_id: manager_id.to_string(),
            manager_endpoint: hardware_manager_address(manager_id),
            session_id: session_id.to_string(),
            timestamp: timestamp_now(),
            labels: BTreeMap::new(),
            devices,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeviceClaim {
    pub claimed_by_endpoint: String,
    pub claimed_by_session_id: String,
    pub timestamp: String,
    pub ttl_seconds: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_tokens_match_deckr_rules() {
        assert_eq!(encode_key_token("deck_1"), "deck_1");
        assert_eq!(decode_key_token("deck_1").unwrap(), "deck_1");

        let encoded = encode_key_token("b64_native");
        assert!(encoded.starts_with("b64_"));
        assert_eq!(decode_key_token(&encoded).unwrap(), "b64_native");

        let encoded = encode_key_token("deck:one");
        assert!(encoded.starts_with("b64_"));
        assert_eq!(decode_key_token(&encoded).unwrap(), "deck:one");
    }

    #[test]
    fn key_shapes_match_deckr_current_state() {
        assert_eq!(
            presence_endpoint_key("hardware_messages", "hardware_manager:mirabox-main").unwrap(),
            "presence.endpoint.hardware_messages.hardware_manager.mirabox-main"
        );
        assert_eq!(
            presence_endpoint_key("services", "action_provider:python-dev.deckr.sonos").unwrap(),
            "presence.endpoint.services.action_provider.b64_cHl0aG9uLWRldi5kZWNrci5zb25vcw"
        );
        assert_eq!(
            hardware_inventory_key("mirabox-main"),
            "inventory.hardware.mirabox-main"
        );
        assert_eq!(
            device_claim_prefix("mirabox-main"),
            "claim.device.mirabox-main."
        );
        assert_eq!(
            parse_device_claim_key("claim.device.mirabox-main.deck").unwrap(),
            ("mirabox-main".to_string(), "deck".to_string())
        );
    }

    #[test]
    fn endpoint_addresses_follow_core_family_rules() {
        assert!(EndpointAddress::parse("controller:main").is_some());
        assert!(EndpointAddress::parse("service:sonos-home").is_some());
        assert!(EndpointAddress::parse("action_provider:python-dev.deckr.sonos").is_some());
        assert!(EndpointAddress::parse("driver:mirabox-main").is_none());
        assert!(EndpointAddress::parse("controller:").is_none());
        assert!(EndpointAddress::parse("controller: main").is_none());
        assert!(EndpointAddress::parse("action_provider:dev.deckr.controller.builtin").is_none());
    }

    #[test]
    fn current_state_payloads_use_python_wire_names() {
        let presence = EndpointPresence::manager("mirabox-main", "session");
        let value = serde_json::to_value(presence).unwrap();
        assert_eq!(value["sessionId"], "session");
        assert_eq!(value["ttlSeconds"], STATE_TTL_SECONDS);
        assert_eq!(value["endpoint"], "hardware_manager:mirabox-main");

        let inventory = HardwareInventory::new("mirabox-main", "session", BTreeMap::new());
        let value = serde_json::to_value(inventory).unwrap();
        assert_eq!(value["managerId"], "mirabox-main");
        assert_eq!(value["managerEndpoint"], "hardware_manager:mirabox-main");
        assert_eq!(value["sessionId"], "session");
        assert_eq!(value["labels"], serde_json::json!({}));
        assert!(value.get("ttlSeconds").is_none());
    }
}
