use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use include_dir::{include_dir, Dir};
use serde::Deserialize;

use crate::policy::{eval_expression, Value};
use crate::protocol::InteractionEvent;
use crate::wire::{
    CapabilityConstraint, CapabilityDescriptor, ControlDescriptor, ControlGeometry,
    DeviceDescriptor, DeviceRef, HardwareMessageBody,
};

static LAYOUTS_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/layouts/built-in");

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
pub struct HidDeviceCandidate {
    pub path: Vec<u8>,
    pub vendor_id: u16,
    pub product_id: u16,
    pub serial_number: String,
    pub usage_page: Option<u16>,
    pub usage: Option<u16>,
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

fn button_input_capabilities(include_momentary: bool) -> Vec<CapabilityDescriptor> {
    let mut capabilities = Vec::new();
    if include_momentary {
        capabilities.push(CapabilityDescriptor {
            capability_id: "button.momentary".to_string(),
            family: "deckr.input.button".to_string(),
            capability_type: "momentary".to_string(),
            direction: "input".to_string(),
            access: vec!["emits".to_string()],
            constraints: Vec::new(),
            event_types: vec!["down".to_string(), "up".to_string()],
            command_types: Vec::new(),
        });
    }
    capabilities.push(CapabilityDescriptor {
        capability_id: "button.press".to_string(),
        family: "deckr.input.button".to_string(),
        capability_type: "activation".to_string(),
        direction: "input".to_string(),
        access: vec!["emits".to_string()],
        constraints: Vec::new(),
        event_types: vec!["press".to_string()],
        command_types: Vec::new(),
    });
    capabilities
}

fn encoder_input_capabilities() -> Vec<CapabilityDescriptor> {
    vec![CapabilityDescriptor {
        capability_id: "encoder.relative".to_string(),
        family: "deckr.input.encoder".to_string(),
        capability_type: "relative".to_string(),
        direction: "input".to_string(),
        access: vec!["emits".to_string()],
        constraints: Vec::new(),
        event_types: vec!["rotate".to_string()],
        command_types: Vec::new(),
    }]
}

fn touch_input_capability(event_types: Vec<&str>) -> CapabilityDescriptor {
    CapabilityDescriptor {
        capability_id: "touch.gesture".to_string(),
        family: "deckr.input.touch".to_string(),
        capability_type: "gesture".to_string(),
        direction: "input".to_string(),
        access: vec!["emits".to_string()],
        constraints: Vec::new(),
        event_types: event_types.into_iter().map(str::to_string).collect(),
        command_types: Vec::new(),
    }
}

fn raster_output_capability(width: u32, height: u32, rotation: i32) -> CapabilityDescriptor {
    CapabilityDescriptor {
        capability_id: "raster.bitmap".to_string(),
        family: "deckr.output.raster".to_string(),
        capability_type: "bitmap".to_string(),
        direction: "output".to_string(),
        access: vec!["settable".to_string()],
        constraints: vec![
            CapabilityConstraint {
                constraint_type: "fixed".to_string(),
                subject: "width".to_string(),
                value: Some(serde_json::json!(width)),
            },
            CapabilityConstraint {
                constraint_type: "fixed".to_string(),
                subject: "height".to_string(),
                value: Some(serde_json::json!(height)),
            },
            CapabilityConstraint {
                constraint_type: "fixed".to_string(),
                subject: "rotation".to_string(),
                value: Some(serde_json::json!(rotation)),
            },
        ],
        event_types: Vec::new(),
        command_types: vec!["set_frame".to_string(), "clear".to_string()],
    }
}

fn power_capability() -> CapabilityDescriptor {
    CapabilityDescriptor {
        capability_id: "device.power".to_string(),
        family: "deckr.device.power".to_string(),
        capability_type: "screen".to_string(),
        direction: "command".to_string(),
        access: vec!["invokable".to_string()],
        constraints: Vec::new(),
        event_types: Vec::new(),
        command_types: vec!["sleep".to_string(), "wake".to_string()],
    }
}

impl Layout {
    pub fn matches_candidate(&self, descriptor: &HidDeviceCandidate) -> Result<bool> {
        eval_expression(&self.candidate, &descriptor_context(descriptor))
    }

    pub fn matches_firmware(
        &self,
        descriptor: &HidDeviceCandidate,
        firmware: &str,
    ) -> Result<bool> {
        let mut context = descriptor_context(descriptor);
        context.insert("firmware".to_string(), Value::Str(firmware.to_string()));
        eval_expression(&self.match_expr, &context)
    }

    pub fn device_descriptor(
        &self,
        device_id: &str,
        fingerprint: &str,
        hid: &str,
    ) -> DeviceDescriptor {
        let _ = hid;
        DeviceDescriptor {
            device_id: device_id.to_string(),
            fingerprint: fingerprint.to_string(),
            display_name: self.name.clone(),
            manufacturer: Some("MiraBox".to_string()),
            model: Some(self.name.clone()),
            serial_number: Some(fingerprint.to_string()),
            controls: self.control_descriptors(),
            capabilities: vec![power_capability()],
        }
    }

    pub fn control_descriptors(&self) -> Vec<ControlDescriptor> {
        self.controls
            .iter()
            .map(|control| {
                let (name, row, column, kind, input_capabilities, display) = match control {
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
                        button_input_capabilities(true),
                        Some(display),
                    ),
                    Control::Button {
                        name, row, column, ..
                    } => (
                        name,
                        *row,
                        *column,
                        "button",
                        button_input_capabilities(true),
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
                        {
                            let mut capabilities = encoder_input_capabilities();
                            capabilities.extend(button_input_capabilities(true));
                            capabilities.push(touch_input_capability(vec!["tap"]));
                            capabilities
                        },
                        Some(display),
                    ),
                    Control::Dial {
                        name, row, column, ..
                    } => (
                        name,
                        *row,
                        *column,
                        "dial",
                        encoder_input_capabilities(),
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
                        vec![touch_input_capability(vec!["swipe"])],
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
                ControlDescriptor {
                    control_id: name.clone(),
                    kind: kind.to_string(),
                    label: Some(name.clone()),
                    geometry: Some(ControlGeometry {
                        x: column as f64,
                        y: row as f64,
                        width: Some(1.0),
                        height: Some(1.0),
                        unit: "grid".to_string(),
                    }),
                    input_capabilities,
                    output_capabilities: display
                        .map(|display| {
                            raster_output_capability(
                                display.format.width,
                                display.format.height,
                                display.format.rotation,
                            )
                        })
                        .into_iter()
                        .collect(),
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

    pub fn display_id_for_control(&self, control_id: &str) -> Option<u8> {
        self.controls.iter().find_map(|control| match control {
            Control::Key { name, display, .. }
            | Control::TouchDial { name, display, .. }
            | Control::TouchStrip { name, display, .. }
            | Control::Screen { name, display, .. }
                if name == control_id =>
            {
                Some(display.id)
            }
            _ => None,
        })
    }

    pub fn translate_event(
        &self,
        event: InteractionEvent,
        manager_id: &str,
        device_id: &str,
        fingerprint: &str,
    ) -> Vec<HardwareMessageBody> {
        let Some(binding) = self.binding_for_event(event.button_id) else {
            return Vec::new();
        };

        match binding {
            EventBinding::Key { control_name } => {
                if event.payload == 0 {
                    vec![
                        control_input(
                            manager_id,
                            device_id,
                            fingerprint,
                            &control_name,
                            "button.momentary",
                            "up",
                            serde_json::json!({"eventType": "up"}),
                        ),
                        control_input(
                            manager_id,
                            device_id,
                            fingerprint,
                            &control_name,
                            "button.press",
                            "press",
                            serde_json::json!({"eventType": "press"}),
                        ),
                    ]
                } else {
                    vec![control_input(
                        manager_id,
                        device_id,
                        fingerprint,
                        &control_name,
                        "button.momentary",
                        "down",
                        serde_json::json!({"eventType": "down"}),
                    )]
                }
            }
            EventBinding::Press { control_name } => vec![control_input(
                manager_id,
                device_id,
                fingerprint,
                &control_name,
                "button.press",
                "press",
                serde_json::json!({"eventType": "press"}),
            )],
            EventBinding::Clockwise { control_name } => vec![control_input(
                manager_id,
                device_id,
                fingerprint,
                &control_name,
                "encoder.relative",
                "rotate",
                serde_json::json!({"delta": 1, "direction": "clockwise"}),
            )],
            EventBinding::Counterclockwise { control_name } => vec![control_input(
                manager_id,
                device_id,
                fingerprint,
                &control_name,
                "encoder.relative",
                "rotate",
                serde_json::json!({"delta": -1, "direction": "counterclockwise"}),
            )],
            EventBinding::Tap { control_name } => vec![control_input(
                manager_id,
                device_id,
                fingerprint,
                &control_name,
                "touch.gesture",
                "tap",
                serde_json::json!({"eventType": "tap"}),
            )],
            EventBinding::LeftSwipe { control_name } => vec![control_input(
                manager_id,
                device_id,
                fingerprint,
                &control_name,
                "touch.gesture",
                "swipe",
                serde_json::json!({"eventType": "swipe", "direction": "left"}),
            )],
            EventBinding::RightSwipe { control_name } => vec![control_input(
                manager_id,
                device_id,
                fingerprint,
                &control_name,
                "touch.gesture",
                "swipe",
                serde_json::json!({"eventType": "swipe", "direction": "right"}),
            )],
        }
    }
}

fn control_input(
    manager_id: &str,
    device_id: &str,
    fingerprint: &str,
    control_id: &str,
    capability_id: &str,
    event_type: &str,
    value: serde_json::Value,
) -> HardwareMessageBody {
    HardwareMessageBody::ControlInput {
        device_ref: DeviceRef {
            manager_id: manager_id.to_string(),
            device_id: device_id.to_string(),
            fingerprint: Some(fingerprint.to_string()),
        },
        control_id: control_id.to_string(),
        capability_id: capability_id.to_string(),
        event_type: event_type.to_string(),
        value: Some(value),
    }
}

pub fn load_embedded_layouts() -> Result<Vec<Layout>> {
    let mut layouts = Vec::new();
    for file in LAYOUTS_DIR.files() {
        let text = file
            .contents_utf8()
            .with_context(|| format!("layout {} must be utf-8", file.path().display()))?;
        let layout: Layout = serde_yaml::from_str(text)
            .with_context(|| format!("parsing {}", file.path().display()))?;
        layouts.push(layout);
    }
    layouts.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(layouts)
}

fn descriptor_context(descriptor: &HidDeviceCandidate) -> HashMap<String, Value> {
    let mut context = HashMap::new();
    context.insert(
        "vendor_id".to_string(),
        Value::Int(descriptor.vendor_id as i64),
    );
    context.insert(
        "product_id".to_string(),
        Value::Int(descriptor.product_id as i64),
    );
    context.insert(
        "serial_number".to_string(),
        Value::Str(descriptor.serial_number.clone()),
    );
    if let Some(usage_page) = descriptor.usage_page {
        context.insert("usage_page".to_string(), Value::Int(usage_page as i64));
    }
    if let Some(usage) = descriptor.usage {
        context.insert("usage".to_string(), Value::Int(usage as i64));
    }
    if let Some(interface_number) = descriptor.interface_number {
        context.insert(
            "interface_number".to_string(),
            Value::Int(interface_number as i64),
        );
    }
    context
}

impl HidDeviceCandidate {
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
    descriptor: &HidDeviceCandidate,
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
        .filter(|layout| {
            layout
                .matches_firmware(descriptor, firmware)
                .unwrap_or(false)
        })
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
    use super::{load_embedded_layouts, resolve_layout, HidDeviceCandidate};

    fn sample_descriptor() -> HidDeviceCandidate {
        HidDeviceCandidate {
            path: b"device".to_vec(),
            vendor_id: 2816,
            product_id: 4097,
            serial_number: "0300D0785616".to_string(),
            usage_page: None,
            usage: None,
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

    #[test]
    fn rejects_known_vid_pid_when_non_deckr_usage_page_is_present() {
        let layouts = load_embedded_layouts().expect("layouts should load");
        let descriptor = HidDeviceCandidate {
            usage_page: Some(1),
            usage: Some(6),
            ..sample_descriptor()
        };
        let matched = layouts
            .iter()
            .any(|layout| layout.matches_candidate(&descriptor).unwrap_or(false));
        assert!(!matched);
    }
}
