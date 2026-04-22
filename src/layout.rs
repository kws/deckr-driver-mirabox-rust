use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use include_dir::{include_dir, Dir};
use serde::Deserialize;

use crate::policy::{eval_expression, Value};
use crate::protocol::InteractionEvent;
use crate::wire::{
    Coordinates, DeviceInfo, HardwareTransportMessage, ImageFormat as WireImageFormat, Slot,
};

static LAYOUTS_DIR: Dir<'_> = include_dir!(
    "$CARGO_MANIFEST_DIR/layouts/built-in"
);

#[derive(Debug, Clone, Deserialize)]
pub struct Layout {
    pub name: String,
    pub candidate: String,
    #[serde(rename = "match")]
    pub match_expr: String,
    #[serde(default)]
    pub init_sequence: Vec<InitCommand>,
    #[serde(default)]
    pub heartbeats: Vec<Heartbeat>,
    #[serde(default)]
    pub controls: Vec<Control>,
    #[serde(default)]
    pub image_config: HashMap<String, ImageFormat>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InitCommand {
    pub cmd: String,
    #[serde(default)]
    pub args: serde_yaml::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Heartbeat {
    pub period: u64,
    pub commands: Vec<InitCommand>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ImageFormat {
    pub width: u32,
    pub height: u32,
    pub format: String,
    #[serde(default)]
    pub rotation: i32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Control {
    Key {
        name: String,
        #[serde(default)]
        row: i32,
        #[serde(default)]
        column: i32,
        events: KeyEvents,
        display: Display,
    },
    Button {
        name: String,
        #[serde(default)]
        row: i32,
        #[serde(default)]
        column: i32,
        events: KeyEvents,
    },
    TouchDial {
        name: String,
        #[serde(default)]
        row: i32,
        #[serde(default)]
        column: i32,
        events: TouchDialEvents,
        display: Display,
    },
    Dial {
        name: String,
        #[serde(default)]
        row: i32,
        #[serde(default)]
        column: i32,
        events: DialEvents,
    },
    TouchStrip {
        name: String,
        #[serde(default)]
        row: i32,
        #[serde(default)]
        column: i32,
        events: TouchStripEvents,
        display: Display,
    },
    Screen {
        name: String,
        #[serde(default)]
        row: i32,
        #[serde(default)]
        column: i32,
        display: Display,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct Display {
    pub id: u8,
    pub format: ImageFormat,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KeyEvents {
    pub key: Option<u16>,
    pub press: Option<u16>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DialEvents {
    pub clockwise: u16,
    pub counterclockwise: u16,
    pub key: Option<u16>,
    pub press: Option<u16>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TouchDialEvents {
    pub tap: u16,
    pub clockwise: u16,
    pub counterclockwise: u16,
    pub key: Option<u16>,
    pub press: Option<u16>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TouchStripEvents {
    pub left_swipe: u16,
    pub right_swipe: u16,
}

#[derive(Debug, Clone)]
pub struct DeviceDescriptor {
    pub path: Vec<u8>,
    pub vendor_id: u16,
    pub product_id: u16,
    pub serial_number: String,
    pub usage_page: Option<u16>,
    pub interface_number: Option<i32>,
}

#[derive(Debug, Clone)]
pub enum EventBinding {
    Key { control_name: String },
    Press { control_name: String },
    Clockwise { control_name: String },
    Counterclockwise { control_name: String },
    Tap { control_name: String },
    LeftSwipe { control_name: String },
    RightSwipe { control_name: String },
}

impl Layout {
    pub fn matches_candidate(&self, descriptor: &DeviceDescriptor) -> Result<bool> {
        eval_expression(&self.candidate, &descriptor_context(descriptor))
    }

    pub fn matches_firmware(&self, descriptor: &DeviceDescriptor, firmware: &str) -> Result<bool> {
        let mut context = descriptor_context(descriptor);
        context.insert("firmware".to_string(), Value::Str(firmware.to_string()));
        eval_expression(&self.match_expr, &context)
    }

    pub fn device_info(&self, device_id: &str, hid: &str) -> DeviceInfo {
        DeviceInfo {
            id: device_id.to_string(),
            hid: hid.to_string(),
            slots: self.slots(),
            name: Some(self.name.clone()),
        }
    }

    pub fn slots(&self) -> Vec<Slot> {
        self.controls
            .iter()
            .map(|control| {
                let (name, row, column, slot_type, gestures, display) = match control {
                    Control::Key {
                        name,
                        row,
                        column,
                        display,
                        ..
                    } => (
                        name,
                        *row,
                        *column,
                        "key",
                        vec!["key_down", "key_up"],
                        Some(display),
                    ),
                    Control::Button {
                        name,
                        row,
                        column,
                        ..
                    } => (
                        name,
                        *row,
                        *column,
                        "button",
                        vec!["key_down", "key_up"],
                        None,
                    ),
                    Control::TouchDial {
                        name,
                        row,
                        column,
                        display,
                        ..
                    } => (
                        name,
                        *row,
                        *column,
                        "touch_dial",
                        vec!["encoder_down", "encoder_rotate", "encoder_up", "touch_tap"],
                        Some(display),
                    ),
                    Control::Dial {
                        name,
                        row,
                        column,
                        ..
                    } => (
                        name,
                        *row,
                        *column,
                        "encoder",
                        vec!["encoder_down", "encoder_rotate", "encoder_up"],
                        None,
                    ),
                    Control::TouchStrip {
                        name,
                        row,
                        column,
                        display,
                        ..
                    } => (
                        name,
                        *row,
                        *column,
                        "touch_strip",
                        vec!["touch_swipe"],
                        Some(display),
                    ),
                    Control::Screen {
                        name,
                        row,
                        column,
                        display,
                        ..
                    } => (name, *row, *column, "screen", Vec::new(), Some(display)),
                };
                let mut gestures = gestures.into_iter().map(str::to_string).collect::<Vec<_>>();
                gestures.sort();
                Slot {
                    id: name.clone(),
                    coordinates: Coordinates { column, row },
                    image_format: display.map(|display| WireImageFormat {
                        width: display.format.width,
                        height: display.format.height,
                        format: display.format.format.clone(),
                        rotation: display.format.rotation,
                        flip_x: false,
                        flip_y: false,
                        format_options: Default::default(),
                    }),
                    slot_type: slot_type.to_string(),
                    gestures,
                }
            })
            .collect()
    }

    pub fn binding_for_event(&self, event_id: u16) -> Option<EventBinding> {
        for control in &self.controls {
            match control {
                Control::Key { name, events, .. } => {
                    if events.key == Some(event_id) {
                        return Some(EventBinding::Key {
                            control_name: name.clone(),
                        });
                    }
                    if events.press == Some(event_id) {
                        return Some(EventBinding::Press {
                            control_name: name.clone(),
                        });
                    }
                }
                Control::Button { name, events, .. } => {
                    if events.key == Some(event_id) {
                        return Some(EventBinding::Key {
                            control_name: name.clone(),
                        });
                    }
                    if events.press == Some(event_id) {
                        return Some(EventBinding::Press {
                            control_name: name.clone(),
                        });
                    }
                }
                Control::TouchDial { name, events, .. } => {
                    if events.tap == event_id {
                        return Some(EventBinding::Tap {
                            control_name: name.clone(),
                        });
                    }
                    if events.clockwise == event_id {
                        return Some(EventBinding::Clockwise {
                            control_name: name.clone(),
                        });
                    }
                    if events.counterclockwise == event_id {
                        return Some(EventBinding::Counterclockwise {
                            control_name: name.clone(),
                        });
                    }
                    if events.key == Some(event_id) || events.press == Some(event_id) {
                        return Some(EventBinding::Press {
                            control_name: name.clone(),
                        });
                    }
                }
                Control::Dial { name, events, .. } => {
                    if events.clockwise == event_id {
                        return Some(EventBinding::Clockwise {
                            control_name: name.clone(),
                        });
                    }
                    if events.counterclockwise == event_id {
                        return Some(EventBinding::Counterclockwise {
                            control_name: name.clone(),
                        });
                    }
                    if events.key == Some(event_id) || events.press == Some(event_id) {
                        return Some(EventBinding::Press {
                            control_name: name.clone(),
                        });
                    }
                }
                Control::TouchStrip { name, events, .. } => {
                    if events.left_swipe == event_id {
                        return Some(EventBinding::LeftSwipe {
                            control_name: name.clone(),
                        });
                    }
                    if events.right_swipe == event_id {
                        return Some(EventBinding::RightSwipe {
                            control_name: name.clone(),
                        });
                    }
                }
                Control::Screen { .. } => {}
            }
        }
        None
    }

    pub fn display_id_for_slot(&self, slot_id: &str) -> Option<u8> {
        self.controls.iter().find_map(|control| match control {
            Control::Key { name, display, .. }
            | Control::TouchDial { name, display, .. }
            | Control::TouchStrip { name, display, .. }
            | Control::Screen { name, display, .. } if name == slot_id => Some(display.id),
            _ => None,
        })
    }

    pub fn translate_event(
        &self,
        device_id: &str,
        event: InteractionEvent,
    ) -> Vec<HardwareTransportMessage> {
        let Some(binding) = self.binding_for_event(event.button_id) else {
            return Vec::new();
        };

        match binding {
            EventBinding::Key { control_name } => {
                if event.payload == 0 {
                    vec![HardwareTransportMessage::KeyUp {
                        device_id: device_id.to_string(),
                        key_id: control_name,
                    }]
                } else {
                    vec![HardwareTransportMessage::KeyDown {
                        device_id: device_id.to_string(),
                        key_id: control_name,
                    }]
                }
            }
            EventBinding::Press { control_name } => vec![
                HardwareTransportMessage::KeyDown {
                    device_id: device_id.to_string(),
                    key_id: control_name.clone(),
                },
                HardwareTransportMessage::KeyUp {
                    device_id: device_id.to_string(),
                    key_id: control_name,
                },
            ],
            EventBinding::Clockwise { control_name } => vec![HardwareTransportMessage::DialRotate {
                device_id: device_id.to_string(),
                dial_id: control_name,
                direction: "clockwise".to_string(),
            }],
            EventBinding::Counterclockwise { control_name } => {
                vec![HardwareTransportMessage::DialRotate {
                    device_id: device_id.to_string(),
                    dial_id: control_name,
                    direction: "counterclockwise".to_string(),
                }]
            }
            EventBinding::Tap { control_name } => vec![HardwareTransportMessage::TouchTap {
                device_id: device_id.to_string(),
                touch_id: control_name,
            }],
            EventBinding::LeftSwipe { control_name } => vec![HardwareTransportMessage::TouchSwipe {
                device_id: device_id.to_string(),
                touch_id: control_name,
                direction: "left".to_string(),
            }],
            EventBinding::RightSwipe { control_name } => {
                vec![HardwareTransportMessage::TouchSwipe {
                    device_id: device_id.to_string(),
                    touch_id: control_name,
                    direction: "right".to_string(),
                }]
            }
        }
    }
}

pub fn load_embedded_layouts() -> Result<Vec<Layout>> {
    let mut layouts = Vec::new();
    for file in LAYOUTS_DIR.files() {
        let text = file
            .contents_utf8()
            .with_context(|| format!("layout {} must be utf-8", file.path().display()))?;
        let layout: Layout =
            serde_yaml::from_str(text).with_context(|| format!("parsing {}", file.path().display()))?;
        layouts.push(layout);
    }
    layouts.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(layouts)
}

fn descriptor_context(descriptor: &DeviceDescriptor) -> HashMap<String, Value> {
    let mut context = HashMap::new();
    context.insert("vendor_id".to_string(), Value::Int(descriptor.vendor_id as i64));
    context.insert("product_id".to_string(), Value::Int(descriptor.product_id as i64));
    context.insert(
        "serial_number".to_string(),
        Value::Str(descriptor.serial_number.clone()),
    );
    if let Some(usage_page) = descriptor.usage_page {
        context.insert("usage_page".to_string(), Value::Int(usage_page as i64));
    }
    if let Some(interface_number) = descriptor.interface_number {
        context.insert(
            "interface_number".to_string(),
            Value::Int(interface_number as i64),
        );
    }
    context
}

impl DeviceDescriptor {
    pub fn hardware_id(&self) -> String {
        format!(
            "{:04X}:{:04X}:{}",
            self.vendor_id, self.product_id, self.serial_number
        )
    }

    pub fn path_hex(&self) -> String {
        self.path
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<Vec<_>>()
            .join("")
    }
}

pub fn resolve_layout<'a>(
    layouts: &'a [Layout],
    descriptor: &DeviceDescriptor,
    firmware: &str,
) -> Result<&'a Layout> {
    let candidates = layouts
        .iter()
        .filter(|layout| layout.matches_candidate(descriptor).unwrap_or(false))
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        bail!("no candidate layouts for {}", descriptor.hardware_id());
    }

    let matches = candidates
        .into_iter()
        .filter(|layout| layout.matches_firmware(descriptor, firmware).unwrap_or(false))
        .collect::<Vec<_>>();
    if matches.is_empty() {
        bail!(
            "no matching layout for {} with firmware {}",
            descriptor.hardware_id(),
            firmware
        );
    }

    Ok(matches[0])
}

#[cfg(test)]
mod tests {
    use super::{load_embedded_layouts, resolve_layout, DeviceDescriptor};

    fn sample_descriptor() -> DeviceDescriptor {
        DeviceDescriptor {
            path: b"device".to_vec(),
            vendor_id: 2816,
            product_id: 4097,
            serial_number: "0300D0785616".to_string(),
            usage_page: None,
            interface_number: Some(0),
        }
    }

    #[test]
    fn parses_embedded_layouts() {
        let layouts = load_embedded_layouts().expect("layouts should load");
        assert!(!layouts.is_empty());
    }

    #[test]
    fn resolves_layout_without_usage_page() {
        let layouts = load_embedded_layouts().expect("layouts should load");
        let layout = resolve_layout(&layouts, &sample_descriptor(), "V25.MSD_TWO.01.005")
            .expect("layout should resolve");
        assert_eq!(layout.name, "MSD_TWO");
    }
}
