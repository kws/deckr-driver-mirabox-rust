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
#[serde(deny_unknown_fields)]
pub struct Coordinates {
    #[serde(rename = "column")]
    pub column: i32,
    #[serde(rename = "row")]
    pub row: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ImageFormat {
    #[serde(rename = "width")]
    pub width: u32,
    #[serde(rename = "height")]
    pub height: u32,
    #[serde(rename = "format")]
    pub format: String,
    #[serde(rename = "rotation", default)]
    pub rotation: i32,
    #[serde(rename = "flipX", default)]
    pub flip_x: bool,
    #[serde(rename = "flipY", default)]
    pub flip_y: bool,
    #[serde(rename = "formatOptions", default)]
    pub format_options: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Slot {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "coordinates")]
    pub coordinates: Coordinates,
    #[serde(rename = "imageFormat", skip_serializing_if = "Option::is_none")]
    pub image_format: Option<ImageFormat>,
    #[serde(rename = "slotType")]
    pub slot_type: String,
    #[serde(rename = "gestures", default)]
    pub gestures: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeviceInfo {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "fingerprint")]
    pub fingerprint: String,
    #[serde(rename = "hid")]
    pub hid: String,
    #[serde(rename = "slots")]
    pub slots: Vec<Slot>,
    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
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
    pub fn hardware(
        manager_id: &str,
        device_id: &str,
        control_id: Option<&str>,
        control_kind: Option<&str>,
    ) -> Self {
        let mut identifiers = BTreeMap::new();
        identifiers.insert("managerId".to_string(), manager_id.to_string());
        identifiers.insert("deviceId".to_string(), device_id.to_string());
        if let Some(control_id) = control_id {
            identifiers.insert("controlId".to_string(), control_id.to_string());
        }
        if let Some(control_kind) = control_kind {
            identifiers.insert("controlKind".to_string(), control_kind.to_string());
        }
        Self {
            kind: if control_id.is_some() {
                "hardware_control".to_string()
            } else {
                "hardware_device".to_string()
            },
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
        let control_id = body.control_id().map(str::to_string);
        let control_kind = body.control_kind().map(str::to_string);
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
            control_id.as_deref(),
            control_kind.as_deref(),
            body,
        )
    }

    pub fn hardware_input_to(
        manager_id: &str,
        device_id: &str,
        controller_endpoint: &str,
        body: HardwareMessageBody,
    ) -> Result<Self> {
        let control_id = body.control_id().map(str::to_string);
        let control_kind = body.control_kind().map(str::to_string);
        Self::hardware(
            format!("hardware_manager:{manager_id}"),
            MessageTarget::Endpoint {
                endpoint: controller_endpoint.to_string(),
            },
            manager_id,
            device_id,
            control_id.as_deref(),
            control_kind.as_deref(),
            body,
        )
    }

    pub fn hardware_command(
        controller_id: &str,
        manager_id: &str,
        device_id: &str,
        body: HardwareMessageBody,
    ) -> Result<Self> {
        let control_id = body.control_id().map(str::to_string);
        let control_kind = body.control_kind().map(str::to_string);
        Self::hardware(
            format!("controller:{controller_id}"),
            MessageTarget::Endpoint {
                endpoint: format!("hardware_manager:{manager_id}"),
            },
            manager_id,
            device_id,
            control_id.as_deref(),
            control_kind.as_deref(),
            body,
        )
    }

    fn hardware(
        sender: String,
        recipient: MessageTarget,
        manager_id: &str,
        device_id: &str,
        control_id: Option<&str>,
        control_kind: Option<&str>,
        body: HardwareMessageBody,
    ) -> Result<Self> {
        Ok(Self {
            message_id: new_message_id(),
            protocol_version: DECKR_PROTOCOL_VERSION.to_string(),
            schema_version: "1".to_string(),
            lane: HARDWARE_MESSAGES_LANE.to_string(),
            message_type: body.message_type().to_string(),
            sender,
            recipient,
            subject: EntitySubject::hardware(manager_id, device_id, control_id, control_kind),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HardwareMessageBody {
    DeviceConnected { device: DeviceInfo },
    DeviceDisconnected,
    KeyDown { key_id: String },
    KeyUp { key_id: String },
    DialRotate { dial_id: String, direction: String },
    TouchTap { touch_id: String },
    TouchSwipe { touch_id: String, direction: String },
    SetImage { slot_id: String, image: Vec<u8> },
    ClearSlot { slot_id: String },
    SleepScreen,
    WakeScreen,
}

impl HardwareMessageBody {
    pub fn message_type(&self) -> &'static str {
        match self {
            Self::DeviceConnected { .. } => "deviceConnected",
            Self::DeviceDisconnected => "deviceDisconnected",
            Self::KeyDown { .. } => "keyDown",
            Self::KeyUp { .. } => "keyUp",
            Self::DialRotate { .. } => "dialRotate",
            Self::TouchTap { .. } => "touchTap",
            Self::TouchSwipe { .. } => "touchSwipe",
            Self::SetImage { .. } => "setImage",
            Self::ClearSlot { .. } => "clearSlot",
            Self::SleepScreen => "sleepScreen",
            Self::WakeScreen => "wakeScreen",
        }
    }

    pub fn control_id(&self) -> Option<&str> {
        match self {
            Self::KeyDown { key_id } | Self::KeyUp { key_id } => Some(key_id),
            Self::DialRotate { dial_id, .. } => Some(dial_id),
            Self::TouchTap { touch_id } | Self::TouchSwipe { touch_id, .. } => Some(touch_id),
            Self::SetImage { slot_id, .. } | Self::ClearSlot { slot_id } => Some(slot_id),
            Self::DeviceConnected { .. }
            | Self::DeviceDisconnected
            | Self::SleepScreen
            | Self::WakeScreen => None,
        }
    }

    pub fn control_kind(&self) -> Option<&str> {
        match self {
            Self::KeyDown { .. } | Self::KeyUp { .. } => Some("key"),
            Self::DialRotate { .. } => Some("dial"),
            Self::TouchTap { .. } | Self::TouchSwipe { .. } => Some("touch"),
            Self::SetImage { .. } | Self::ClearSlot { .. } => Some("slot"),
            Self::DeviceConnected { .. }
            | Self::DeviceDisconnected
            | Self::SleepScreen
            | Self::WakeScreen => None,
        }
    }

    pub fn is_input(&self) -> bool {
        matches!(
            self,
            Self::DeviceConnected { .. }
                | Self::DeviceDisconnected
                | Self::KeyDown { .. }
                | Self::KeyUp { .. }
                | Self::DialRotate { .. }
                | Self::TouchTap { .. }
                | Self::TouchSwipe { .. }
        )
    }

    pub fn is_command(&self) -> bool {
        matches!(
            self,
            Self::SetImage { .. } | Self::ClearSlot { .. } | Self::SleepScreen | Self::WakeScreen
        )
    }

    pub fn to_value(&self) -> Result<Value> {
        Ok(match self {
            Self::DeviceConnected { device } => json!({ "device": device }),
            Self::DeviceDisconnected => json!({}),
            Self::KeyDown { key_id } => json!({ "keyId": key_id }),
            Self::KeyUp { key_id } => json!({ "keyId": key_id }),
            Self::DialRotate { dial_id, direction } => {
                json!({ "dialId": dial_id, "direction": direction })
            }
            Self::TouchTap { touch_id } => json!({ "touchId": touch_id }),
            Self::TouchSwipe {
                touch_id,
                direction,
            } => json!({ "touchId": touch_id, "direction": direction }),
            Self::SetImage { slot_id, image } => serde_json::to_value(SetImageBody {
                slot_id: slot_id.clone(),
                image: image.clone(),
            })?,
            Self::ClearSlot { slot_id } => json!({ "slotId": slot_id }),
            Self::SleepScreen => json!({}),
            Self::WakeScreen => json!({}),
        })
    }

    pub fn from_message(message_type: &str, body: &Value) -> Result<Self> {
        Ok(match message_type {
            "deviceConnected" => {
                let body: DeviceConnectedBody = serde_json::from_value(body.clone())?;
                Self::DeviceConnected {
                    device: body.device,
                }
            }
            "deviceDisconnected" => Self::DeviceDisconnected,
            "keyDown" => {
                let body: KeyBody = serde_json::from_value(body.clone())?;
                Self::KeyDown {
                    key_id: body.key_id,
                }
            }
            "keyUp" => {
                let body: KeyBody = serde_json::from_value(body.clone())?;
                Self::KeyUp {
                    key_id: body.key_id,
                }
            }
            "dialRotate" => {
                let body: DialBody = serde_json::from_value(body.clone())?;
                Self::DialRotate {
                    dial_id: body.dial_id,
                    direction: body.direction,
                }
            }
            "touchTap" => {
                let body: TouchBody = serde_json::from_value(body.clone())?;
                Self::TouchTap {
                    touch_id: body.touch_id,
                }
            }
            "touchSwipe" => {
                let body: TouchSwipeBody = serde_json::from_value(body.clone())?;
                Self::TouchSwipe {
                    touch_id: body.touch_id,
                    direction: body.direction,
                }
            }
            "setImage" => serde_json::from_value::<SetImageBody>(body.clone())?.into(),
            "clearSlot" => {
                let body: SlotBody = serde_json::from_value(body.clone())?;
                Self::ClearSlot {
                    slot_id: body.slot_id,
                }
            }
            "sleepScreen" => Self::SleepScreen,
            "wakeScreen" => Self::WakeScreen,
            other => bail!("unknown hardware message type {other}"),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeviceConnectedBody {
    device: DeviceInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct KeyBody {
    #[serde(rename = "keyId")]
    key_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DialBody {
    #[serde(rename = "dialId")]
    dial_id: String,
    direction: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TouchBody {
    #[serde(rename = "touchId")]
    touch_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TouchSwipeBody {
    #[serde(rename = "touchId")]
    touch_id: String,
    direction: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SlotBody {
    #[serde(rename = "slotId")]
    slot_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SetImageBody {
    #[serde(rename = "slotId")]
    slot_id: String,
    #[serde(rename = "image", with = "base64_bytes")]
    image: Vec<u8>,
}

impl From<SetImageBody> for HardwareMessageBody {
    fn from(body: SetImageBody) -> Self {
        Self::SetImage {
            slot_id: body.slot_id,
            image: body.image,
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

mod base64_bytes {
    use base64::engine::general_purpose::URL_SAFE;
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&URL_SAFE.encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        URL_SAFE
            .decode(text.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::{load_fixture, DeckrMessage, HardwareMessageBody};
    use serde_json::json;
    use std::path::PathBuf;

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(format!("{name}.json"))
    }

    #[test]
    fn round_trips_device_connected_fixture() {
        let message = load_fixture(&fixture("device_connected")).expect("fixture should parse");
        let text = message.to_text().expect("message should serialize");
        let parsed = DeckrMessage::from_text(&text).expect("text should parse");
        assert_eq!(parsed, message);
        assert!(matches!(
            parsed.hardware_body().expect("body should parse"),
            HardwareMessageBody::DeviceConnected { .. }
        ));
    }

    #[test]
    fn round_trips_binary_image_fixture() {
        let message = load_fixture(&fixture("set_image")).expect("fixture should parse");
        let text = message.to_text().expect("message should serialize");
        assert!(text.contains("\"image\":\"-_8=\""));
        let parsed = DeckrMessage::from_text(&text).expect("text should parse");
        assert_eq!(parsed, message);
        assert!(matches!(
            parsed.hardware_body().expect("body should parse"),
            HardwareMessageBody::SetImage { .. }
        ));
    }

    #[test]
    fn rejects_non_canonical_envelope_fields() {
        let mut value = json!({
            "messageId": "fixture",
            "protocolVersion": "1",
            "schemaVersion": "1",
            "lane": "hardware_messages",
            "messageType": "keyDown",
            "sender": "hardware_manager:bedroom-pi",
            "recipient": {
                "targetType": "endpoint",
                "endpoint": "controller:main"
            },
            "subject": {
                "kind": "hardware_control",
                "identifiers": {
                    "managerId": "bedroom-pi",
                    "deviceId": "deck",
                    "controlId": "0,0",
                    "controlKind": "key"
                }
            },
            "createdAt": "2026-04-26T00:00:00.000Z",
            "body": {"keyId": "0,0"}
        });
        value["unexpectedField"] = json!(true);
        let text = serde_json::to_string(&value).unwrap();
        assert!(DeckrMessage::from_text(&text).is_err());
    }
}
