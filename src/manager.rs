use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc as tokio_mpsc, oneshot, Mutex};
use tokio::time;
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};
use tracing::{debug, error, info, warn};

use crate::backend::{Backend, DeviceHandle, HidBackend};
use crate::layout::{load_embedded_layouts, resolve_layout, DeviceDescriptor, InitCommand, Layout};
use crate::protocol::{DeviceCommand as ProtocolCommand, MiraBoxProtocol};
use crate::wire::{DeckrMessage, HardwareMessageBody, TransportFrame, HARDWARE_EVENTS_LANE};

const DISCOVERY_INTERVAL: Duration = Duration::from_secs(1);
const READ_TIMEOUT_MS: i32 = 100;
const MAX_BACKOFF_SECS: u64 = 10;
const PING_INTERVAL: Duration = Duration::from_secs(20);

#[derive(Debug, Clone)]
pub enum RuntimeCommand {
    SetImage { slot_id: String, image: Vec<u8> },
    ClearSlot { slot_id: String },
    SleepScreen,
    WakeScreen,
    Stop,
}

#[derive(Debug, Clone)]
enum WorkerEvent {
    Connected {
        path_key: String,
        device_id: String,
        command_tx: Sender<RuntimeCommand>,
        message: DeckrMessage,
    },
    Message(DeckrMessage),
    Disconnected {
        path_key: String,
        device_id: String,
    },
    Failed {
        path_key: String,
        error: String,
    },
}

pub struct MiraBoxRemoteManager {
    transport_url: String,
    manager_id: String,
    backend: Arc<dyn Backend>,
    layouts: Arc<Vec<Layout>>,
}

impl MiraBoxRemoteManager {
    pub fn new(transport_url: String, manager_id: String) -> Result<Self> {
        Ok(Self::with_backend(
            transport_url,
            manager_id,
            Arc::new(HidBackend),
            Arc::new(load_embedded_layouts()?),
        ))
    }

    pub fn with_backend(
        transport_url: String,
        manager_id: String,
        backend: Arc<dyn Backend>,
        layouts: Arc<Vec<Layout>>,
    ) -> Self {
        Self {
            transport_url,
            manager_id,
            backend,
            layouts,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut backoff = 1u64;
        loop {
            match self.run_connected_session().await {
                Ok(()) => {
                    warn!(
                        "Transport websocket closed for {}; reconnecting",
                        self.manager_id
                    );
                }
                Err(error) => {
                    error!(
                        "Transport client {} disconnected; retrying in {}s: {error:#}",
                        self.manager_id, backoff
                    );
                }
            }
            time::sleep(Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(MAX_BACKOFF_SECS);
        }
    }

    async fn run_connected_session(&self) -> Result<()> {
        let (stream, _) = connect_async(&self.transport_url)
            .await
            .with_context(|| format!("connecting to {}", self.transport_url))?;
        let (mut write, mut read) = stream.split();
        info!(
            "Connected manager {} to {}",
            self.manager_id, self.transport_url
        );

        let (outbound_tx, mut outbound_rx) = tokio_mpsc::unbounded_channel::<DeckrMessage>();
        let command_map = Arc::new(Mutex::new(HashMap::<String, Sender<RuntimeCommand>>::new()));
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let supervisor = Supervisor::new(
            self.manager_id.clone(),
            self.backend.clone(),
            self.layouts.clone(),
            outbound_tx.clone(),
            command_map.clone(),
        );
        let transport_id = self.manager_id.clone();

        let writer = tokio::spawn(async move {
            let mut ping_interval = time::interval(PING_INTERVAL);
            loop {
                tokio::select! {
                    maybe_message = outbound_rx.recv() => {
                        let Some(message) = maybe_message else { break; };
                        write.send(Message::Text(
                            TransportFrame::new(transport_id.clone(), message).to_text()?.into(),
                        )).await.context("sending websocket message")?;
                    }
                    _ = ping_interval.tick() => {
                        write.send(Message::Ping(Vec::new())).await.context("sending ping")?;
                    }
                }
            }
            Result::<()>::Ok(())
        });

        let supervisor_handle = tokio::spawn(async move { supervisor.run(shutdown_rx).await });

        let read_result = reader_loop(&mut read, &self.manager_id, command_map).await;
        let _ = shutdown_tx.send(());
        drop(outbound_tx);
        supervisor_handle.await.context("joining supervisor")??;
        writer.await.context("joining websocket writer")??;
        read_result?;
        Ok(())
    }
}

struct Supervisor {
    manager_id: String,
    backend: Arc<dyn Backend>,
    layouts: Arc<Vec<Layout>>,
    outbound_tx: tokio_mpsc::UnboundedSender<DeckrMessage>,
    command_map: Arc<Mutex<HashMap<String, Sender<RuntimeCommand>>>>,
}

impl Supervisor {
    fn new(
        manager_id: String,
        backend: Arc<dyn Backend>,
        layouts: Arc<Vec<Layout>>,
        outbound_tx: tokio_mpsc::UnboundedSender<DeckrMessage>,
        command_map: Arc<Mutex<HashMap<String, Sender<RuntimeCommand>>>>,
    ) -> Self {
        Self {
            manager_id,
            backend,
            layouts,
            outbound_tx,
            command_map,
        }
    }

    async fn run(self, mut shutdown_rx: oneshot::Receiver<()>) -> Result<()> {
        let (worker_tx, mut worker_rx) = tokio_mpsc::unbounded_channel::<WorkerEvent>();
        let mut discovery = time::interval(DISCOVERY_INTERVAL);
        let mut active_paths = HashSet::<String>::new();
        let mut launched_paths = HashSet::<String>::new();
        let mut worker_senders = Vec::<Sender<RuntimeCommand>>::new();

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    break;
                }
                _ = discovery.tick() => {
                    let descriptors = enumerate_canonical(self.backend.clone()).await?;
                    for descriptor in descriptors {
                        let path_key = descriptor.path_hex();
                        if active_paths.contains(&path_key) || launched_paths.contains(&path_key) {
                            continue;
                        }
                        launched_paths.insert(path_key.clone());
                        let (command_tx, command_rx) = mpsc::channel::<RuntimeCommand>();
                        worker_senders.push(command_tx.clone());
                        spawn_device_worker(
                            self.manager_id.clone(),
                            self.backend.clone(),
                            self.layouts.clone(),
                            descriptor,
                            worker_tx.clone(),
                            command_tx,
                            command_rx,
                        );
                    }
                }
                maybe_event = worker_rx.recv() => {
                    let Some(event) = maybe_event else { continue; };
                    match event {
                        WorkerEvent::Connected { path_key, device_id, command_tx, message } => {
                            launched_paths.remove(&path_key);
                            active_paths.insert(path_key);
                            self.command_map.lock().await.insert(device_id, command_tx);
                            let _ = self.outbound_tx.send(message);
                        }
                        WorkerEvent::Message(message) => {
                            let _ = self.outbound_tx.send(message);
                        }
                        WorkerEvent::Disconnected { path_key, device_id } => {
                            launched_paths.remove(&path_key);
                            active_paths.remove(&path_key);
                            self.command_map.lock().await.remove(&device_id);
                            if let Ok(message) = DeckrMessage::hardware_event(
                                &self.manager_id,
                                &device_id,
                                HardwareMessageBody::DeviceDisconnected,
                            ) {
                                let _ = self.outbound_tx.send(message);
                            }
                        }
                        WorkerEvent::Failed { path_key, error } => {
                            launched_paths.remove(&path_key);
                            active_paths.remove(&path_key);
                            warn!("Device worker {path_key} failed: {error}");
                        }
                    }
                }
            }
        }

        for sender in worker_senders {
            let _ = sender.send(RuntimeCommand::Stop);
        }
        Ok(())
    }
}

async fn enumerate_canonical(backend: Arc<dyn Backend>) -> Result<Vec<DeviceDescriptor>> {
    let rows = tokio::task::spawn_blocking(move || backend.enumerate())
        .await
        .context("joining enumerate task")??;

    let mut grouped = HashMap::<(u16, u16, String), Vec<DeviceDescriptor>>::new();
    for descriptor in rows {
        grouped
            .entry((
                descriptor.vendor_id,
                descriptor.product_id,
                descriptor.serial_number.clone(),
            ))
            .or_default()
            .push(descriptor);
    }

    let mut canonical = grouped
        .into_values()
        .filter_map(|mut descriptors| {
            descriptors.sort_by_key(|descriptor| {
                (
                    descriptor.interface_number.unwrap_or(i32::MAX),
                    descriptor.path.clone(),
                )
            });
            descriptors.into_iter().next()
        })
        .collect::<Vec<_>>();
    canonical.sort_by_key(|descriptor| descriptor.path.clone());
    Ok(canonical)
}

fn spawn_device_worker(
    manager_id: String,
    backend: Arc<dyn Backend>,
    layouts: Arc<Vec<Layout>>,
    descriptor: DeviceDescriptor,
    worker_tx: tokio_mpsc::UnboundedSender<WorkerEvent>,
    command_tx: Sender<RuntimeCommand>,
    command_rx: mpsc::Receiver<RuntimeCommand>,
) {
    let path_key = descriptor.path_hex();
    thread::spawn(move || {
        if let Err(error) = device_worker(
            manager_id,
            backend,
            layouts,
            descriptor,
            worker_tx.clone(),
            command_tx,
            command_rx,
        ) {
            let _ = worker_tx.send(WorkerEvent::Failed {
                path_key,
                error: format!("{error:#}"),
            });
        }
    });
}

fn device_worker(
    manager_id: String,
    backend: Arc<dyn Backend>,
    layouts: Arc<Vec<Layout>>,
    descriptor: DeviceDescriptor,
    worker_tx: tokio_mpsc::UnboundedSender<WorkerEvent>,
    command_tx: Sender<RuntimeCommand>,
    command_rx: mpsc::Receiver<RuntimeCommand>,
) -> Result<()> {
    let path_key = descriptor.path_hex();
    let mut handle = backend.open(&descriptor.path)?;
    let protocol = MiraBoxProtocol::default();
    let firmware_report = handle.get_input_report(0, protocol.read_size())?;
    let firmware = decode_firmware(&firmware_report)?;
    let layout = resolve_layout(&layouts, &descriptor, &firmware)?;
    let local_device_id = descriptor.hardware_id();

    info!(
        "Using layout {} for device {}",
        layout.name, local_device_id
    );

    run_init_sequence(&mut *handle, &protocol, &layout.init_sequence)?;
    worker_tx
        .send(WorkerEvent::Connected {
            path_key: path_key.clone(),
            device_id: local_device_id.clone(),
            command_tx: command_tx.clone(),
            message: DeckrMessage::hardware_event(
                &manager_id,
                &local_device_id,
                HardwareMessageBody::DeviceConnected {
                    device: layout.device_info(&local_device_id, &local_device_id),
                },
            )?,
        })
        .ok();

    let mut next_heartbeat = layout
        .heartbeats
        .iter()
        .map(|heartbeat| {
            (
                Instant::now() + Duration::from_secs(heartbeat.period),
                heartbeat,
            )
        })
        .collect::<Vec<_>>();

    loop {
        while let Ok(command) = command_rx.try_recv() {
            if matches!(command, RuntimeCommand::Stop) {
                return Ok(());
            }
            if let Err(error) = apply_runtime_command(&mut *handle, &protocol, layout, command) {
                let _ = worker_tx.send(WorkerEvent::Disconnected {
                    path_key: path_key.clone(),
                    device_id: local_device_id.clone(),
                });
                return Err(error);
            }
        }

        let now = Instant::now();
        for (due, heartbeat) in &mut next_heartbeat {
            if now >= *due {
                run_init_sequence(&mut *handle, &protocol, &heartbeat.commands)?;
                *due = now + Duration::from_secs(heartbeat.period);
            }
        }

        match command_rx.recv_timeout(Duration::from_millis(0)) {
            Ok(RuntimeCommand::Stop) => return Ok(()),
            Ok(command) => {
                if let Err(error) = apply_runtime_command(&mut *handle, &protocol, layout, command)
                {
                    let _ = worker_tx.send(WorkerEvent::Disconnected {
                        path_key: path_key.clone(),
                        device_id: local_device_id.clone(),
                    });
                    return Err(error);
                }
            }
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
            Err(RecvTimeoutError::Timeout) => {}
        }

        let report = match handle.read(protocol.read_size(), READ_TIMEOUT_MS) {
            Ok(report) => report,
            Err(error) => {
                let _ = worker_tx.send(WorkerEvent::Disconnected {
                    path_key: path_key.clone(),
                    device_id: local_device_id.clone(),
                });
                return Err(error);
            }
        };
        if report.is_empty() {
            continue;
        }
        if let Some(event) = match protocol.parse_event(&report) {
            Ok(event) => event,
            Err(error) => {
                let _ = worker_tx.send(WorkerEvent::Disconnected {
                    path_key: path_key.clone(),
                    device_id: local_device_id.clone(),
                });
                return Err(error);
            }
        } {
            for body in layout.translate_event(event) {
                if let Ok(message) =
                    DeckrMessage::hardware_event(&manager_id, &local_device_id, body)
                {
                    let _ = worker_tx.send(WorkerEvent::Message(message));
                }
            }
        }
    }
}

fn run_init_sequence(
    handle: &mut dyn DeviceHandle,
    protocol: &MiraBoxProtocol,
    commands: &[InitCommand],
) -> Result<()> {
    for command in commands {
        let packets = protocol.encode_command(&layout_command_to_protocol(command)?);
        for packet in packets {
            handle.write(&packet)?;
        }
        thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

fn layout_command_to_protocol(command: &InitCommand) -> Result<ProtocolCommand> {
    let args = command.args.as_mapping();
    match command.cmd.as_str() {
        "wake_screen" => Ok(ProtocolCommand::WakeScreen),
        "sleep_screen" => Ok(ProtocolCommand::SleepScreen),
        "clear_key" => Ok(ProtocolCommand::ClearKey {
            target: args
                .and_then(|map| map.get(serde_yaml::Value::String("target".into())))
                .and_then(|value| value.as_u64())
                .unwrap_or(0xFF) as u32,
        }),
        "refresh" => Ok(ProtocolCommand::Refresh),
        "connect" => Ok(ProtocolCommand::Connect),
        "set_brightness" => Ok(ProtocolCommand::SetBrightness {
            value: args
                .and_then(|map| map.get(serde_yaml::Value::String("value".into())))
                .and_then(|value| value.as_u64())
                .unwrap_or(0xFF) as u32,
        }),
        other => bail!("unsupported init command {other}"),
    }
}

fn apply_runtime_command(
    handle: &mut dyn DeviceHandle,
    protocol: &MiraBoxProtocol,
    layout: &Layout,
    command: RuntimeCommand,
) -> Result<()> {
    let command = match command {
        RuntimeCommand::SetImage { slot_id, image } => {
            let Some(display_id) = layout.display_id_for_slot(&slot_id) else {
                warn!("Ignoring setImage for unknown slot {slot_id}");
                return Ok(());
            };
            ProtocolCommand::SetKeyImage {
                key: display_id,
                image,
                x: 0,
                y: 0,
            }
        }
        RuntimeCommand::ClearSlot { slot_id } => {
            let Some(display_id) = layout.display_id_for_slot(&slot_id) else {
                warn!("Ignoring clearSlot for unknown slot {slot_id}");
                return Ok(());
            };
            ProtocolCommand::ClearKey {
                target: display_id as u32,
            }
        }
        RuntimeCommand::SleepScreen => ProtocolCommand::SleepScreen,
        RuntimeCommand::WakeScreen => ProtocolCommand::WakeScreen,
        RuntimeCommand::Stop => return Ok(()),
    };
    for packet in protocol.encode_command(&command) {
        handle.write(&packet)?;
    }
    Ok(())
}

fn decode_firmware(report: &[u8]) -> Result<String> {
    if report.len() < 2 {
        bail!("firmware report too short");
    }
    Ok(String::from_utf8_lossy(&report[1..])
        .trim_end_matches('\0')
        .to_string())
}

async fn reader_loop(
    read: &mut futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    manager_id: &str,
    command_map: Arc<Mutex<HashMap<String, Sender<RuntimeCommand>>>>,
) -> Result<()> {
    while let Some(message) = read.next().await {
        let Some(message) = parse_ws_message(manager_id, message?)? else {
            continue;
        };
        let Some(device_id) = message.subject.device_id().map(str::to_string) else {
            debug!("Ignoring inbound hardware command without device subject");
            continue;
        };
        match message.hardware_body()? {
            HardwareMessageBody::SetImage { slot_id, image } => {
                dispatch_command(
                    &command_map,
                    &device_id,
                    RuntimeCommand::SetImage { slot_id, image },
                )
                .await;
            }
            HardwareMessageBody::ClearSlot { slot_id } => {
                dispatch_command(
                    &command_map,
                    &device_id,
                    RuntimeCommand::ClearSlot { slot_id },
                )
                .await;
            }
            HardwareMessageBody::SleepScreen => {
                dispatch_command(&command_map, &device_id, RuntimeCommand::SleepScreen).await;
            }
            HardwareMessageBody::WakeScreen => {
                dispatch_command(&command_map, &device_id, RuntimeCommand::WakeScreen).await;
            }
            HardwareMessageBody::DeviceConnected { .. }
            | HardwareMessageBody::DeviceDisconnected
            | HardwareMessageBody::KeyDown { .. }
            | HardwareMessageBody::KeyUp { .. }
            | HardwareMessageBody::DialRotate { .. }
            | HardwareMessageBody::TouchTap { .. }
            | HardwareMessageBody::TouchSwipe { .. } => {
                debug!("Ignoring unexpected inbound message");
            }
        }
    }
    Ok(())
}

async fn dispatch_command(
    command_map: &Arc<Mutex<HashMap<String, Sender<RuntimeCommand>>>>,
    device_id: &str,
    command: RuntimeCommand,
) {
    let sender = { command_map.lock().await.get(device_id).cloned() };
    if let Some(sender) = sender {
        if sender.send(command).is_err() {
            warn!("Dropping command for disconnected device {device_id}");
        }
    } else {
        warn!("Ignoring transport command for unknown local device {device_id}");
    }
}

fn parse_ws_message(manager_id: &str, message: Message) -> Result<Option<DeckrMessage>> {
    let text = match message {
        Message::Text(text) => Some(text.to_string()),
        Message::Binary(bytes) => Some(std::str::from_utf8(&bytes)?.to_string()),
        Message::Ping(_) | Message::Pong(_) | Message::Close(_) | Message::Frame(_) => None,
    };
    let Some(text) = text else {
        return Ok(None);
    };

    let frame = TransportFrame::from_text(&text)?;
    let message = frame.message;
    if message.lane != HARDWARE_EVENTS_LANE {
        debug!("Ignoring websocket message for lane {}", message.lane);
        return Ok(None);
    }
    let expected_endpoint = format!("hardware_manager:{manager_id}");
    if message.recipient_endpoint() != Some(expected_endpoint.as_str()) {
        debug!("Ignoring hardware message addressed away from this manager");
        return Ok(None);
    }
    Ok(Some(message))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    use super::*;
    use crate::backend::{Backend, DeviceHandle};
    use crate::wire::{DeckrMessage, HardwareMessageBody, TransportFrame};

    #[derive(Clone)]
    struct FakeBackend {
        enumerate_rows: Arc<Mutex<Vec<DeviceDescriptor>>>,
        device: Arc<Mutex<FakeDeviceState>>,
    }

    struct FakeDeviceState {
        firmware_report: Vec<u8>,
        reports: VecDeque<Vec<u8>>,
        writes: Vec<Vec<u8>>,
    }

    impl FakeBackend {
        fn new() -> Self {
            Self {
                enumerate_rows: Arc::new(Mutex::new(vec![DeviceDescriptor {
                    path: b"fake-path".to_vec(),
                    vendor_id: 2816,
                    product_id: 4097,
                    serial_number: "0300D0785616".to_string(),
                    usage_page: None,
                    interface_number: Some(0),
                }])),
                device: Arc::new(Mutex::new(FakeDeviceState {
                    firmware_report: {
                        let mut report = vec![0; 64];
                        report[1..19].copy_from_slice(b"V25.MSD_TWO.01.005");
                        report
                    },
                    reports: VecDeque::new(),
                    writes: Vec::new(),
                })),
            }
        }

        fn push_report(&self, report: Vec<u8>) {
            self.device.lock().unwrap().reports.push_back(report);
        }

        fn writes(&self) -> Vec<Vec<u8>> {
            self.device.lock().unwrap().writes.clone()
        }
    }

    impl Backend for FakeBackend {
        fn enumerate(&self) -> Result<Vec<DeviceDescriptor>> {
            Ok(self.enumerate_rows.lock().unwrap().clone())
        }

        fn open(&self, _path: &[u8]) -> Result<Box<dyn DeviceHandle>> {
            Ok(Box::new(FakeHandle {
                state: self.device.clone(),
            }))
        }
    }

    struct FakeHandle {
        state: Arc<Mutex<FakeDeviceState>>,
    }

    impl DeviceHandle for FakeHandle {
        fn get_input_report(&mut self, _report_id: u8, _read_size: usize) -> Result<Vec<u8>> {
            Ok(self.state.lock().unwrap().firmware_report.clone())
        }

        fn read(&mut self, _read_size: usize, timeout_ms: i32) -> Result<Vec<u8>> {
            if let Some(report) = self.state.lock().unwrap().reports.pop_front() {
                return Ok(report);
            }
            thread::sleep(Duration::from_millis(timeout_ms as u64));
            Ok(Vec::new())
        }

        fn write(&mut self, payload: &[u8]) -> Result<usize> {
            self.state.lock().unwrap().writes.push(payload.to_vec());
            Ok(payload.len())
        }
    }

    async fn next_text_message(
        ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    ) -> DeckrMessage {
        loop {
            let frame = ws
                .next()
                .await
                .expect("frame should arrive")
                .expect("frame should decode");
            let text = match frame {
                Message::Text(text) => text.to_string(),
                Message::Binary(bytes) => String::from_utf8(bytes.to_vec()).expect("utf-8 frame"),
                Message::Ping(_) | Message::Pong(_) | Message::Close(_) | Message::Frame(_) => {
                    continue
                }
            };
            let frame = TransportFrame::from_text(&text).expect("frame should parse");
            if frame.message.lane == HARDWARE_EVENTS_LANE {
                return frame.message;
            }
        }
    }

    fn ack_report(button_id: u16, payload: u8) -> Vec<u8> {
        let mut report = vec![0; 64];
        report[0..3].copy_from_slice(b"ACK");
        report[8..10].copy_from_slice(&button_id.to_be_bytes());
        report[10] = payload;
        report
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconnects_and_rediscover_devices() {
        let backend = Arc::new(FakeBackend::new());
        let layouts = Arc::new(load_embedded_layouts().expect("layouts should load"));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("listener should have address");

        let accepted = tokio::spawn(async move {
            let mut connections = 0usize;
            while connections < 2 {
                let (stream, _) = listener.accept().await.expect("should accept");
                let mut ws = accept_async(stream)
                    .await
                    .expect("handshake should succeed");
                let connected = next_text_message(&mut ws).await;
                assert_eq!(connected.sender, "hardware_manager:bedroom-pi");
                assert!(matches!(
                    connected.hardware_body().expect("body should parse"),
                    HardwareMessageBody::DeviceConnected { .. }
                ));
                connections += 1;
                ws.close(None).await.expect("close should succeed");
            }
        });

        let manager = MiraBoxRemoteManager::with_backend(
            format!("ws://{}", address),
            "bedroom-pi".to_string(),
            backend,
            layouts,
        );
        let run = tokio::spawn(async move { manager.run().await });

        time::sleep(Duration::from_secs(3)).await;
        run.abort();
        let _ = run.await;
        accepted.await.expect("controller task should finish");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transports_device_events_and_controller_commands() {
        let backend = Arc::new(FakeBackend::new());
        backend.push_report(ack_report(1, 1));
        let layouts = Arc::new(load_embedded_layouts().expect("layouts should load"));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("listener should have address");

        let controller = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("should accept");
            let mut ws = accept_async(stream)
                .await
                .expect("handshake should succeed");
            let connected = next_text_message(&mut ws).await;
            assert!(matches!(
                connected.hardware_body().expect("body should parse"),
                HardwareMessageBody::DeviceConnected { .. }
            ));

            let key_down = next_text_message(&mut ws).await;
            assert!(matches!(
                key_down.hardware_body().expect("body should parse"),
                HardwareMessageBody::KeyDown { ref key_id } if key_id == "0,0"
            ));

            let device_id = "0B00:1001:0300D0785616";
            for command in [
                HardwareMessageBody::SetImage {
                    slot_id: "0,0".to_string(),
                    image: vec![0, 255, 16],
                },
                HardwareMessageBody::ClearSlot {
                    slot_id: "0,0".to_string(),
                },
                HardwareMessageBody::SleepScreen,
                HardwareMessageBody::WakeScreen,
            ] {
                ws.send(Message::Text(
                    TransportFrame::new(
                        "controller-ws",
                        DeckrMessage::hardware_command(
                            "controller-main",
                            "bedroom-pi",
                            device_id,
                            command,
                        )
                        .expect("command should build"),
                    )
                    .to_text()
                    .expect("command should serialize")
                    .into(),
                ))
                .await
                .expect("command should send");
            }

            time::sleep(Duration::from_millis(500)).await;
            ws.close(None).await.expect("close should succeed");
        });

        let manager = MiraBoxRemoteManager::with_backend(
            format!("ws://{}", address),
            "bedroom-pi".to_string(),
            backend.clone(),
            layouts,
        );
        let run = tokio::spawn(async move { manager.run().await });

        controller.await.expect("controller task should finish");
        time::sleep(Duration::from_millis(500)).await;
        run.abort();
        let _ = run.await;

        let writes = backend.writes();
        assert!(writes
            .iter()
            .any(|payload| payload.starts_with(b"\x00CRT\x00\x00BAT")));
        assert!(writes
            .iter()
            .any(|payload| payload.starts_with(b"\x00CRT\x00\x00CLE")));
        assert!(writes
            .iter()
            .any(|payload| payload.starts_with(b"\x00CRT\x00\x00HAN")));
        assert!(writes
            .iter()
            .any(|payload| payload.starts_with(b"\x00CRT\x00\x00DIS")));
    }
}
