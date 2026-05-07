use anyhow::{bail, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InteractionEvent {
    pub button_id: u16,
    pub payload: u8,
    pub supports_release: bool,
}

#[derive(Debug, Clone)]
pub enum DeviceCommand {
    WakeDisplay,
    SleepDisplay,
    ClearKey {
        target: u32,
    },
    Refresh,
    Connect,
    SetBrightness {
        value: u8,
    },
    SetMode {
        mode: u8,
    },
    SetLedBrightness {
        value: u8,
    },
    SetLedColors {
        colors: Vec<[u8; 3]>,
    },
    ShutdownClear,
    SetKeyImage {
        key: u8,
        image: Vec<u8>,
    },
    SetLogo {
        image: Vec<u8>,
    },
    SetBackgroundImage {
        image: Vec<u8>,
        x: u16,
        y: u16,
        width: u16,
        height: u16,
        frame_buffer: u16,
    },
}

#[derive(Debug, Clone)]
pub struct MiraBoxProtocol {
    protocol_version: u8,
    report_id: u8,
    packet_size: usize,
    read_size: usize,
}

impl MiraBoxProtocol {
    pub fn for_version(protocol_version: u8) -> Result<Self> {
        if !(1..=3).contains(&protocol_version) {
            anyhow::bail!("MiraBox protocol version must be 1, 2, or 3");
        }
        Ok(Self {
            protocol_version,
            report_id: 0x00,
            packet_size: if protocol_version >= 2 { 1024 } else { 512 },
            read_size: 64,
        })
    }

    pub fn read_size(&self) -> usize {
        self.read_size
    }

    pub fn packet_size(&self) -> usize {
        self.packet_size
    }

    pub fn protocol_version(&self) -> u8 {
        self.protocol_version
    }

    pub fn supports_release_events(&self) -> bool {
        self.protocol_version >= 3
    }

    pub fn encode_command(&self, command: &DeviceCommand) -> Result<Vec<Vec<u8>>> {
        Ok(match command {
            DeviceCommand::WakeDisplay => self.to_chunks(b"CRT\x00\x00DIS".to_vec()),
            DeviceCommand::SleepDisplay => self.to_chunks(b"CRT\x00\x00HAN".to_vec()),
            DeviceCommand::ClearKey { target } => {
                let mut payload = b"CRT\x00\x00CLE".to_vec();
                payload.extend_from_slice(&target.to_be_bytes());
                self.to_chunks(payload)
            }
            DeviceCommand::Refresh => self.to_chunks(b"CRT\x00\x00STP".to_vec()),
            DeviceCommand::Connect => self.to_chunks(b"CRT\x00\x00CONNECT".to_vec()),
            DeviceCommand::SetBrightness { value } => {
                require_percent("value", *value)?;
                let mut payload = b"CRT\x00\x00LIG".to_vec();
                payload.extend_from_slice(&[0, 0, *value]);
                self.to_chunks(payload)
            }
            DeviceCommand::SetMode { mode } => {
                if *mode > 9 {
                    bail!("mode must be 0-9");
                }
                let mut payload = b"CRT\x00\x00MOD\x00\x00".to_vec();
                payload.push(0x30 + *mode);
                self.to_chunks(payload)
            }
            DeviceCommand::SetLedBrightness { value } => {
                require_percent("value", *value)?;
                let mut payload = b"CRT\x00\x00LBLIG".to_vec();
                payload.push(*value);
                self.to_chunks(payload)
            }
            DeviceCommand::SetLedColors { colors } => {
                let mut payload = b"CRT\x00\x00SETLB".to_vec();
                for color in colors {
                    payload.extend_from_slice(color);
                }
                self.to_chunks(payload)
            }
            DeviceCommand::ShutdownClear => self.to_chunks(b"CRT\x00\x00CLE\x00\x00DC".to_vec()),
            DeviceCommand::SetKeyImage { key, image } => {
                if image.len() > u16::MAX as usize {
                    bail!("set key image payload must be at most 65535 bytes");
                }
                let mut command = b"CRT\x00\x00BAT".to_vec();
                command.extend_from_slice(&[0, 0]);
                command.extend_from_slice(&(image.len() as u16).to_be_bytes());
                command.push(*key);
                let mut packets = self.to_chunks(command);
                packets.extend(self.to_chunks(image.clone()));
                packets
            }
            DeviceCommand::SetLogo { image } => {
                let mut command = b"CRT\x00\x00LOG".to_vec();
                command.extend_from_slice(&(image.len() as u32).to_be_bytes());
                let mut packets = self.to_chunks(command);
                packets.extend(self.to_chunks(image.clone()));
                packets
            }
            DeviceCommand::SetBackgroundImage {
                image,
                x,
                y,
                width,
                height,
                frame_buffer,
            } => {
                let mut command = b"CRT\x00\x00BGPIC".to_vec();
                command.extend_from_slice(&(image.len() as u32).to_be_bytes());
                command.extend_from_slice(&x.to_be_bytes());
                command.extend_from_slice(&y.to_be_bytes());
                command.extend_from_slice(&width.to_be_bytes());
                command.extend_from_slice(&height.to_be_bytes());
                command.extend_from_slice(&frame_buffer.to_be_bytes());
                let mut packets = self.to_chunks(command);
                packets.extend(self.to_chunks(image.clone()));
                packets
            }
        })
    }

    pub fn parse_event(&self, report: &[u8]) -> Result<Option<InteractionEvent>> {
        if report.len() < 11 {
            return Ok(None);
        }
        if &report[0..3] != b"ACK" {
            return Ok(None);
        }

        Ok(Some(InteractionEvent {
            button_id: u16::from_be_bytes([report[8], report[9]]),
            payload: if self.supports_release_events() {
                report[10]
            } else {
                1
            },
            supports_release: self.supports_release_events(),
        }))
    }

    fn to_chunks(&self, data: Vec<u8>) -> Vec<Vec<u8>> {
        let mut chunks = Vec::new();
        let mut offset = 0usize;
        while offset < data.len() {
            let end = usize::min(offset + self.packet_size, data.len());
            let mut chunk = data[offset..end].to_vec();
            if chunk.len() < self.packet_size {
                chunk.resize(self.packet_size, 0);
            }
            let mut payload = Vec::with_capacity(1 + self.packet_size);
            payload.push(self.report_id);
            payload.extend_from_slice(&chunk);
            chunks.push(payload);
            offset += self.packet_size;
        }
        if chunks.is_empty() {
            let mut payload = vec![self.report_id];
            payload.resize(1 + self.packet_size, 0);
            chunks.push(payload);
        }
        chunks
    }
}

fn require_percent(name: &str, value: u8) -> Result<()> {
    if value > 100 {
        bail!("{name} must be a 0-100 percent value");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{DeviceCommand, InteractionEvent, MiraBoxProtocol};

    #[test]
    fn encodes_wake_display() {
        let protocol = MiraBoxProtocol::for_version(3).expect("valid protocol version");
        let payloads = protocol
            .encode_command(&DeviceCommand::WakeDisplay)
            .expect("command should encode");
        assert_eq!(payloads.len(), 1);
        assert_eq!(&payloads[0][0..9], b"\x00CRT\x00\x00DIS");
        assert_eq!(payloads[0].len(), 1025);
    }

    #[test]
    fn encodes_set_key_image() {
        let protocol = MiraBoxProtocol::for_version(3).expect("valid protocol version");
        let payloads = protocol
            .encode_command(&DeviceCommand::SetKeyImage {
                key: 7,
                image: vec![1, 2, 3],
            })
            .expect("command should encode");
        assert_eq!(payloads.len(), 2);
        assert_eq!(
            &payloads[0][0..14],
            b"\x00CRT\x00\x00BAT\x00\x00\x00\x03\x07"
        );
        assert_eq!(&payloads[1][1..4], &[1, 2, 3]);
    }

    #[test]
    fn parses_ack_events() {
        let protocol = MiraBoxProtocol::for_version(3).expect("valid protocol version");
        let mut report = vec![0; 64];
        report[0..3].copy_from_slice(b"ACK");
        report[8..10].copy_from_slice(&81u16.to_be_bytes());
        report[10] = 1;
        let event = protocol
            .parse_event(&report)
            .expect("parse should succeed")
            .expect("ack should produce an event");
        assert_eq!(
            event,
            InteractionEvent {
                button_id: 81,
                payload: 1,
                supports_release: true
            }
        );
    }

    #[test]
    fn protocol_v1_uses_512_byte_reports() {
        let protocol = MiraBoxProtocol::for_version(1).expect("valid protocol version");
        let payloads = protocol
            .encode_command(&DeviceCommand::SetKeyImage {
                key: 1,
                image: vec![0; 600],
            })
            .expect("command should encode");

        assert_eq!(protocol.packet_size(), 512);
        assert_eq!(
            payloads.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![513, 513, 513]
        );
    }

    #[test]
    fn protocol_v2_uses_1024_byte_reports() {
        let protocol = MiraBoxProtocol::for_version(2).expect("valid protocol version");
        let payloads = protocol
            .encode_command(&DeviceCommand::SetKeyImage {
                key: 1,
                image: vec![0; 600],
            })
            .expect("command should encode");

        assert_eq!(protocol.packet_size(), 1024);
        assert_eq!(
            payloads.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![1025, 1025]
        );
    }

    #[test]
    fn protocol_v3_uses_1024_byte_reports() {
        let protocol = MiraBoxProtocol::for_version(3).expect("valid protocol version");
        let payloads = protocol
            .encode_command(&DeviceCommand::SetKeyImage {
                key: 1,
                image: vec![0; 600],
            })
            .expect("command should encode");

        assert_eq!(protocol.packet_size(), 1024);
        assert_eq!(
            payloads.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![1025, 1025]
        );
    }

    #[test]
    fn encodes_brightness_percent() {
        let protocol = MiraBoxProtocol::for_version(3).expect("valid protocol version");
        let payloads = protocol
            .encode_command(&DeviceCommand::SetBrightness { value: 50 })
            .expect("command should encode");

        assert_eq!(&payloads[0][0..12], b"\x00CRT\x00\x00LIG\x00\x002");
    }

    #[test]
    fn encodes_mode() {
        let protocol = MiraBoxProtocol::for_version(3).expect("valid protocol version");
        let payloads = protocol
            .encode_command(&DeviceCommand::SetMode { mode: 2 })
            .expect("command should encode");

        assert_eq!(&payloads[0][0..12], b"\x00CRT\x00\x00MOD\x00\x002");
    }

    #[test]
    fn encodes_led_commands() {
        let protocol = MiraBoxProtocol::for_version(3).expect("valid protocol version");
        let brightness = protocol
            .encode_command(&DeviceCommand::SetLedBrightness { value: 40 })
            .expect("command should encode");
        let colors = protocol
            .encode_command(&DeviceCommand::SetLedColors {
                colors: vec![[1, 2, 3], [4, 5, 6]],
            })
            .expect("command should encode");

        assert_eq!(&brightness[0][0..12], b"\x00CRT\x00\x00LBLIG(");
        assert_eq!(
            &colors[0][0..17],
            b"\x00CRT\x00\x00SETLB\x01\x02\x03\x04\x05\x06"
        );
    }

    #[test]
    fn encodes_shutdown_clear() {
        let protocol = MiraBoxProtocol::for_version(3).expect("valid protocol version");
        let payloads = protocol
            .encode_command(&DeviceCommand::ShutdownClear)
            .expect("command should encode");

        assert_eq!(&payloads[0][0..13], b"\x00CRT\x00\x00CLE\x00\x00DC");
    }

    #[test]
    fn protocol_v2_synthesizes_press_payload_for_no_release_input_reports() {
        let protocol = MiraBoxProtocol::for_version(2).expect("valid protocol version");
        let mut report = vec![0; 64];
        report[0..3].copy_from_slice(b"ACK");
        report[8..10].copy_from_slice(&81u16.to_be_bytes());
        report[10] = 0;

        let event = protocol
            .parse_event(&report)
            .expect("parse should succeed")
            .expect("ack should produce an event");

        assert_eq!(event.button_id, 81);
        assert_eq!(event.payload, 1);
        assert!(!event.supports_release);
    }

    #[test]
    fn protocol_v3_uses_release_payload_from_input_reports() {
        let protocol = MiraBoxProtocol::for_version(3).expect("valid protocol version");
        let mut report = vec![0; 64];
        report[0..3].copy_from_slice(b"ACK");
        report[8..10].copy_from_slice(&81u16.to_be_bytes());
        report[10] = 0;

        let event = protocol
            .parse_event(&report)
            .expect("parse should succeed")
            .expect("ack should produce an event");

        assert_eq!(event.button_id, 81);
        assert_eq!(event.payload, 0);
        assert!(event.supports_release);
    }
}
