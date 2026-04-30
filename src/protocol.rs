use anyhow::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InteractionEvent {
    pub button_id: u16,
    pub payload: u8,
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
        value: u32,
    },
    SetKeyImage {
        key: u8,
        image: Vec<u8>,
        x: u16,
        y: u16,
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
    report_id: u8,
    packet_size: usize,
    read_size: usize,
}

impl Default for MiraBoxProtocol {
    fn default() -> Self {
        Self {
            report_id: 0x00,
            packet_size: 1024,
            read_size: 64,
        }
    }
}

impl MiraBoxProtocol {
    pub fn read_size(&self) -> usize {
        self.read_size
    }

    pub fn encode_command(&self, command: &DeviceCommand) -> Vec<Vec<u8>> {
        match command {
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
                let mut payload = b"CRT\x00\x00LIG".to_vec();
                payload.extend_from_slice(&value.to_be_bytes()[1..]);
                self.to_chunks(payload)
            }
            DeviceCommand::SetKeyImage { key, image, x, y } => {
                let mut command = b"CRT\x00\x00BAT".to_vec();
                command.extend_from_slice(&(image.len() as u32).to_be_bytes());
                command.push(*key);
                command.extend_from_slice(&x.to_be_bytes());
                command.extend_from_slice(&y.to_be_bytes());
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
        }
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
            payload: report[10],
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

#[cfg(test)]
mod tests {
    use super::{DeviceCommand, InteractionEvent, MiraBoxProtocol};

    #[test]
    fn encodes_wake_display() {
        let protocol = MiraBoxProtocol::default();
        let payloads = protocol.encode_command(&DeviceCommand::WakeDisplay);
        assert_eq!(payloads.len(), 1);
        assert_eq!(&payloads[0][0..9], b"\x00CRT\x00\x00DIS");
        assert_eq!(payloads[0].len(), 1025);
    }

    #[test]
    fn encodes_set_key_image() {
        let protocol = MiraBoxProtocol::default();
        let payloads = protocol.encode_command(&DeviceCommand::SetKeyImage {
            key: 7,
            image: vec![1, 2, 3],
            x: 0,
            y: 0,
        });
        assert_eq!(payloads.len(), 2);
        assert_eq!(
            &payloads[0][0..14],
            b"\x00CRT\x00\x00BAT\x00\x00\x00\x03\x07"
        );
        assert_eq!(&payloads[1][1..4], &[1, 2, 3]);
    }

    #[test]
    fn parses_ack_events() {
        let protocol = MiraBoxProtocol::default();
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
                payload: 1
            }
        );
    }
}
