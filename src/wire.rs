use std::fs;
use std::path::Path;

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Coordinates {
    #[serde(rename = "column")]
    pub column: i32,
    #[serde(rename = "row")]
    pub row: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
pub struct DeviceInfo {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "hid")]
    pub hid: String,
    #[serde(rename = "slots")]
    pub slots: Vec<Slot>,
    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum HardwareTransportMessage {
    #[serde(rename = "managerHello")]
    ManagerHello {
        #[serde(rename = "managerId")]
        manager_id: String,
    },
    #[serde(rename = "controllerHello")]
    ControllerHello {
        #[serde(rename = "controllerId")]
        controller_id: String,
    },
    #[serde(rename = "deviceConnected")]
    DeviceConnected {
        #[serde(rename = "deviceId")]
        device_id: String,
        device: DeviceInfo,
    },
    #[serde(rename = "deviceDisconnected")]
    DeviceDisconnected {
        #[serde(rename = "deviceId")]
        device_id: String,
    },
    #[serde(rename = "keyDown")]
    KeyDown {
        #[serde(rename = "deviceId")]
        device_id: String,
        #[serde(rename = "keyId")]
        key_id: String,
    },
    #[serde(rename = "keyUp")]
    KeyUp {
        #[serde(rename = "deviceId")]
        device_id: String,
        #[serde(rename = "keyId")]
        key_id: String,
    },
    #[serde(rename = "dialRotate")]
    DialRotate {
        #[serde(rename = "deviceId")]
        device_id: String,
        #[serde(rename = "dialId")]
        dial_id: String,
        direction: String,
    },
    #[serde(rename = "touchTap")]
    TouchTap {
        #[serde(rename = "deviceId")]
        device_id: String,
        #[serde(rename = "touchId")]
        touch_id: String,
    },
    #[serde(rename = "touchSwipe")]
    TouchSwipe {
        #[serde(rename = "deviceId")]
        device_id: String,
        #[serde(rename = "touchId")]
        touch_id: String,
        direction: String,
    },
    #[serde(rename = "setImage")]
    SetImage {
        #[serde(rename = "deviceId")]
        device_id: String,
        #[serde(rename = "slotId")]
        slot_id: String,
        #[serde(rename = "image", with = "base64_bytes")]
        image: Vec<u8>,
    },
    #[serde(rename = "clearSlot")]
    ClearSlot {
        #[serde(rename = "deviceId")]
        device_id: String,
        #[serde(rename = "slotId")]
        slot_id: String,
    },
    #[serde(rename = "sleepScreen")]
    SleepScreen {
        #[serde(rename = "deviceId")]
        device_id: String,
    },
    #[serde(rename = "wakeScreen")]
    WakeScreen {
        #[serde(rename = "deviceId")]
        device_id: String,
    },
}

impl HardwareTransportMessage {
    pub fn to_text(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn from_text(text: &str) -> Result<Self> {
        Ok(serde_json::from_str(text)?)
    }
}

pub fn load_fixture(path: &Path) -> Result<HardwareTransportMessage> {
    let content = fs::read_to_string(path)?;
    HardwareTransportMessage::from_text(&content)
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
    use super::{load_fixture, HardwareTransportMessage};
    use std::path::PathBuf;

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(format!("{name}.json"))
    }

    #[test]
    fn round_trips_manager_hello_fixture() {
        let message = load_fixture(&fixture("manager_hello")).expect("fixture should parse");
        let text = message.to_text().expect("message should serialize");
        let parsed = HardwareTransportMessage::from_text(&text).expect("text should parse");
        assert_eq!(parsed, message);
    }

    #[test]
    fn round_trips_device_connected_fixture() {
        let message =
            load_fixture(&fixture("device_connected")).expect("fixture should parse");
        let text = message.to_text().expect("message should serialize");
        let parsed = HardwareTransportMessage::from_text(&text).expect("text should parse");
        assert_eq!(parsed, message);
    }

    #[test]
    fn round_trips_binary_image_fixture() {
        let message = load_fixture(&fixture("set_image")).expect("fixture should parse");
        let text = message.to_text().expect("message should serialize");
        assert!(text.contains("\"image\":\"-_8=\""));
        let parsed = HardwareTransportMessage::from_text(&text).expect("text should parse");
        assert_eq!(parsed, message);
    }
}
