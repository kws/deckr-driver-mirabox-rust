use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{bail, Result};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

pub const HARDWARE_MESSAGES_LANE: &str = "hardware_messages";
pub const HARDWARE_MESSAGES_SCHEMA_ID: &str = "deckr.message.hardware_messages.v1";
pub const DECKR_PROTOCOL_VERSION: &str = "1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeviceRef {
    pub manager_id: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ControlGeometry {
    pub x: f64,
    pub y: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<f64>,
    pub unit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapabilityConstraint {
    #[serde(rename = "type")]
    pub constraint_type: String,
    pub subject: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapabilityDescriptor {
    pub capability_id: String,
    pub family: String,
    #[serde(rename = "type")]
    pub capability_type: String,
    pub direction: String,
    pub access: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub constraints: Vec<CapabilityConstraint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_types: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command_types: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ControlDescriptor {
    pub control_id: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub geometry: Option<ControlGeometry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_capabilities: Vec<CapabilityDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub output_capabilities: Vec<CapabilityDescriptor>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeviceInfo {
    pub device_id: String,
    pub fingerprint: String,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manufacturer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serial_number: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub controls: Vec<ControlDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<CapabilityDescriptor>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "targetType")]
pub enum MessageTarget {
    #[serde(rename = "endpoint")]
    Endpoint { endpoint: String },
    #[serde(rename = "broadcast")]
    Broadcast {
        scope: String,
        #[serde(rename = "endpointFamily")]
        endpoint_family: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        domain: Option<String>,
        #[serde(rename = "hopLimit", skip_serializing_if = "Option::is_none")]
        hop_limit: Option<u32>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EntitySubject {
    pub kind: String,
    #[serde(default)]
    pub identifiers: BTreeMap<String, String>,
}

impl EntitySubject {
    pub fn hardware_device(manager_id: &str, device_id: &str) -> Self {
        let mut identifiers = BTreeMap::new();
        identifiers.insert("managerId".to_string(), manager_id.to_string());
        identifiers.insert("deviceId".to_string(), device_id.to_string());
        Self {
            kind: "hardware_device".to_string(),
            identifiers,
        }
    }

    pub fn hardware_capability(
        manager_id: &str,
        device_id: &str,
        control_id: Option<&str>,
        capability_id: &str,
    ) -> Self {
        let mut identifiers = BTreeMap::new();
        identifiers.insert("managerId".to_string(), manager_id.to_string());
        identifiers.insert("deviceId".to_string(), device_id.to_string());
        if let Some(control_id) = control_id {
            identifiers.insert("controlId".to_string(), control_id.to_string());
        }
        identifiers.insert("capabilityId".to_string(), capability_id.to_string());
        Self {
            kind: "hardware_capability".to_string(),
            identifiers,
        }
    }

    pub fn device_id(&self) -> Option<&str> {
        self.identifiers.get("deviceId").map(String::as_str)
    }

    pub fn manager_id(&self) -> Option<&str> {
        self.identifiers.get("managerId").map(String::as_str)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeckrMessage {
    #[serde(rename = "messageId")]
    pub message_id: String,
    #[serde(rename = "protocolVersion")]
    pub protocol_version: String,
    #[serde(rename = "schemaVersion")]
    pub schema_version: String,
    pub lane: String,
    #[serde(rename = "messageType")]
    pub message_type: String,
    pub sender: String,
    pub recipient: MessageTarget,
    pub subject: EntitySubject,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "expiresAt", skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    #[serde(rename = "ttlMs", skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    #[serde(rename = "inReplyTo", skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    #[serde(rename = "causationId", skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace: Option<Value>,
    pub body: Value,
}

impl DeckrMessage {
    pub fn hardware_input(
        manager_id: &str,
        device_id: &str,
        body: HardwareMessageBody,
    ) -> Result<Self> {
        Self::hardware(
            format!("hardware_manager:{manager_id}"),
            MessageTarget::Broadcast {
                scope: "controllers".to_string(),
                endpoint_family: "controller".to_string(),
                domain: None,
                hop_limit: None,
            },
            manager_id,
            device_id,
            body,
        )
    }

    pub fn hardware_input_to(
        manager_id: &str,
        device_id: &str,
        controller_endpoint: &str,
        body: HardwareMessageBody,
    ) -> Result<Self> {
        Self::hardware(
            format!("hardware_manager:{manager_id}"),
            MessageTarget::Endpoint {
                endpoint: controller_endpoint.to_string(),
            },
            manager_id,
            device_id,
            body,
        )
    }

    pub fn hardware_command(
        controller_id: &str,
        manager_id: &str,
        device_id: &str,
        body: HardwareMessageBody,
    ) -> Result<Self> {
        Self::hardware(
            format!("controller:{controller_id}"),
            MessageTarget::Endpoint {
                endpoint: format!("hardware_manager:{manager_id}"),
            },
            manager_id,
            device_id,
            body,
        )
    }

    fn hardware(
        sender: String,
        recipient: MessageTarget,
        manager_id: &str,
        device_id: &str,
        body: HardwareMessageBody,
    ) -> Result<Self> {
        let subject = match body.capability_id() {
            Some(capability_id) => EntitySubject::hardware_capability(
                manager_id,
                device_id,
                body.control_id(),
                capability_id,
            ),
            _ => EntitySubject::hardware_device(manager_id, device_id),
        };
        Ok(Self {
            message_id: new_message_id(),
            protocol_version: DECKR_PROTOCOL_VERSION.to_string(),
            schema_version: "1".to_string(),
            lane: HARDWARE_MESSAGES_LANE.to_string(),
            message_type: body.message_type().to_string(),
            sender,
            recipient,
            subject,
            created_at: created_at(),
            expires_at: None,
            ttl_ms: None,
            in_reply_to: None,
            causation_id: None,
            trace: None,
            body: body.to_value()?,
        })
    }

    pub fn to_text(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn from_text(text: &str) -> Result<Self> {
        Ok(serde_json::from_str(text)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }

    pub fn hardware_body(&self) -> Result<HardwareMessageBody> {
        HardwareMessageBody::from_message(&self.message_type, &self.body)
    }

    pub fn recipient_endpoint(&self) -> Option<&str> {
        match &self.recipient {
            MessageTarget::Endpoint { endpoint } => Some(endpoint.as_str()),
            MessageTarget::Broadcast { .. } => None,
        }
    }

    pub fn is_expired(&self) -> bool {
        let now = Utc::now();
        let expires_at = self
            .expires_at
            .as_deref()
            .and_then(parse_datetime)
            .into_iter()
            .chain(self.ttl_ms.and_then(|ttl_ms| {
                parse_datetime(&self.created_at).and_then(|created_at| {
                    created_at.checked_add_signed(Duration::milliseconds(ttl_ms as i64))
                })
            }))
            .min();
        expires_at.is_some_and(|expires_at| expires_at <= now)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum HardwareMessageBody {
    DeviceAvailable {
        descriptor: DeviceInfo,
    },
    DeviceDescriptorChanged {
        descriptor: DeviceInfo,
    },
    DeviceUnavailable {
        device_ref: DeviceRef,
        reason: Option<String>,
    },
    ControlInput {
        device_ref: DeviceRef,
        control_id: String,
        capability_id: String,
        event_type: String,
        value: Option<Value>,
    },
    ControlCommand {
        device_ref: DeviceRef,
        control_id: Option<String>,
        capability_id: String,
        command_type: String,
        params: serde_json::Map<String, Value>,
    },
}

impl HardwareMessageBody {
    pub fn message_type(&self) -> &'static str {
        match self {
            Self::DeviceAvailable { .. } => "deviceAvailable",
            Self::DeviceDescriptorChanged { .. } => "deviceDescriptorChanged",
            Self::DeviceUnavailable { .. } => "deviceUnavailable",
            Self::ControlInput { .. } => "controlInput",
            Self::ControlCommand { .. } => "controlCommand",
        }
    }

    pub fn control_id(&self) -> Option<&str> {
        match self {
            Self::ControlInput { control_id, .. } => Some(control_id),
            Self::ControlCommand { control_id, .. } => control_id.as_deref(),
            Self::DeviceAvailable { .. }
            | Self::DeviceDescriptorChanged { .. }
            | Self::DeviceUnavailable { .. } => None,
        }
    }

    pub fn capability_id(&self) -> Option<&str> {
        match self {
            Self::ControlInput { capability_id, .. }
            | Self::ControlCommand { capability_id, .. } => Some(capability_id),
            Self::DeviceAvailable { .. }
            | Self::DeviceDescriptorChanged { .. }
            | Self::DeviceUnavailable { .. } => None,
        }
    }

    pub fn is_input(&self) -> bool {
        matches!(
            self,
            Self::DeviceAvailable { .. }
                | Self::DeviceDescriptorChanged { .. }
                | Self::DeviceUnavailable { .. }
                | Self::ControlInput { .. }
        )
    }

    pub fn is_command(&self) -> bool {
        matches!(self, Self::ControlCommand { .. })
    }

    pub fn to_value(&self) -> Result<Value> {
        Ok(match self {
            Self::DeviceAvailable { descriptor } => json!({ "descriptor": descriptor }),
            Self::DeviceDescriptorChanged { descriptor } => json!({ "descriptor": descriptor }),
            Self::DeviceUnavailable { device_ref, reason } => {
                let mut value = json!({ "deviceRef": device_ref });
                if let Some(reason) = reason {
                    value["reason"] = json!(reason);
                }
                value
            }
            Self::ControlInput {
                device_ref,
                control_id,
                capability_id,
                event_type,
                value,
            } => json!({
                "deviceRef": device_ref,
                "controlId": control_id,
                "capabilityId": capability_id,
                "eventType": event_type,
                "value": value
            }),
            Self::ControlCommand {
                device_ref,
                control_id,
                capability_id,
                command_type,
                params,
            } => {
                let mut value = json!({
                    "deviceRef": device_ref,
                    "capabilityId": capability_id,
                    "commandType": command_type,
                    "params": params
                });
                if let Some(control_id) = control_id {
                    value["controlId"] = json!(control_id);
                }
                value
            }
        })
    }

    pub fn from_message(message_type: &str, body: &Value) -> Result<Self> {
        Ok(match message_type {
            "deviceAvailable" => {
                let body: DeviceDescriptorBody = serde_json::from_value(body.clone())?;
                Self::DeviceAvailable {
                    descriptor: body.descriptor,
                }
            }
            "deviceDescriptorChanged" => {
                let body: DeviceDescriptorBody = serde_json::from_value(body.clone())?;
                Self::DeviceDescriptorChanged {
                    descriptor: body.descriptor,
                }
            }
            "deviceUnavailable" => {
                serde_json::from_value::<DeviceUnavailableBody>(body.clone())?.into()
            }
            "controlInput" => serde_json::from_value::<ControlInputBody>(body.clone())?.into(),
            "controlCommand" => serde_json::from_value::<ControlCommandBody>(body.clone())?.into(),
            other => bail!("unknown hardware message type {other}"),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeviceDescriptorBody {
    descriptor: DeviceInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeviceUnavailableBody {
    device_ref: DeviceRef,
    reason: Option<String>,
}

impl From<DeviceUnavailableBody> for HardwareMessageBody {
    fn from(body: DeviceUnavailableBody) -> Self {
        Self::DeviceUnavailable {
            device_ref: body.device_ref,
            reason: body.reason,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ControlInputBody {
    device_ref: DeviceRef,
    control_id: String,
    capability_id: String,
    event_type: String,
    value: Option<Value>,
}

impl From<ControlInputBody> for HardwareMessageBody {
    fn from(body: ControlInputBody) -> Self {
        Self::ControlInput {
            device_ref: body.device_ref,
            control_id: body.control_id,
            capability_id: body.capability_id,
            event_type: body.event_type,
            value: body.value,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ControlCommandBody {
    device_ref: DeviceRef,
    control_id: Option<String>,
    capability_id: String,
    command_type: String,
    #[serde(default)]
    params: serde_json::Map<String, Value>,
}

impl From<ControlCommandBody> for HardwareMessageBody {
    fn from(body: ControlCommandBody) -> Self {
        Self::ControlCommand {
            device_ref: body.device_ref,
            control_id: body.control_id,
            capability_id: body.capability_id,
            command_type: body.command_type,
            params: body.params,
        }
    }
}

pub fn load_fixture(path: &Path) -> Result<DeckrMessage> {
    let content = fs::read_to_string(path)?;
    DeckrMessage::from_text(&content)
}

fn new_message_id() -> String {
    Uuid::new_v4().to_string()
}

fn created_at() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn parse_datetime(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::{DeckrMessage, DeviceRef, HardwareMessageBody};
    use serde_json::json;

    #[test]
    fn round_trips_control_input_message() {
        let message = DeckrMessage::hardware_input_to(
            "bedroom-pi",
            "deck",
            "controller:main",
            HardwareMessageBody::ControlInput {
                device_ref: DeviceRef {
                    manager_id: "bedroom-pi".to_string(),
                    device_id: "deck".to_string(),
                    fingerprint: None,
                },
                control_id: "0,0".to_string(),
                capability_id: "button.momentary".to_string(),
                event_type: "down".to_string(),
                value: Some(json!({"eventType": "down"})),
            },
        )
        .expect("message should build");
        let text = message.to_text().expect("message should serialize");
        let parsed = DeckrMessage::from_text(&text).expect("text should parse");
        assert_eq!(parsed, message);
        assert!(matches!(
            parsed.hardware_body().expect("body should parse"),
            HardwareMessageBody::ControlInput { .. }
        ));
    }

    #[test]
    fn round_trips_control_command_message() {
        let mut params = serde_json::Map::new();
        params.insert("image".to_string(), json!("b2s="));
        let message = DeckrMessage::hardware_command(
            "main",
            "bedroom-pi",
            "deck",
            HardwareMessageBody::ControlCommand {
                device_ref: DeviceRef {
                    manager_id: "bedroom-pi".to_string(),
                    device_id: "deck".to_string(),
                    fingerprint: None,
                },
                control_id: Some("0,0".to_string()),
                capability_id: "raster.bitmap".to_string(),
                command_type: "set_frame".to_string(),
                params,
            },
        )
        .expect("message should build");
        let text = message.to_text().expect("message should serialize");
        assert!(text.contains("\"messageType\":\"controlCommand\""));
        let parsed = DeckrMessage::from_text(&text).expect("text should parse");
        assert_eq!(parsed, message);
        assert!(matches!(
            parsed.hardware_body().expect("body should parse"),
            HardwareMessageBody::ControlCommand { .. }
        ));
    }

    #[test]
    fn rejects_non_canonical_envelope_fields() {
        let mut value = json!({
            "messageId": "fixture",
            "protocolVersion": "1",
            "schemaVersion": "1",
            "lane": "hardware_messages",
            "messageType": "controlInput",
            "sender": "hardware_manager:bedroom-pi",
            "recipient": {
                "targetType": "endpoint",
                "endpoint": "controller:main"
            },
            "subject": {
                "kind": "hardware_capability",
                "identifiers": {
                    "managerId": "bedroom-pi",
                    "deviceId": "deck",
                    "controlId": "0,0",
                    "capabilityId": "button.momentary"
                }
            },
            "createdAt": "2026-04-26T00:00:00.000Z",
            "body": {
                "deviceRef": {"managerId": "bedroom-pi", "deviceId": "deck"},
                "controlId": "0,0",
                "capabilityId": "button.momentary",
                "eventType": "down"
            }
        });
        value["unexpectedField"] = json!(true);
        let text = serde_json::to_string(&value).unwrap();
        assert!(DeckrMessage::from_text(&text).is_err());
    }
}
