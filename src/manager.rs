use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use futures_util::StreamExt;
use tokio::sync::{mpsc as tokio_mpsc, oneshot, Mutex};
use tokio::task::JoinSet;
use tokio::time;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::backend::{Backend, DeviceHandle, HidBackend};
use crate::layout::{
    load_embedded_layouts, resolve_layout, HidDeviceCandidate, InitCommand, Layout,
};
use crate::nats::NatsDeckrRuntime;
use crate::protocol::{DeviceCommand as ProtocolCommand, MiraBoxProtocol};
use crate::routing::RoutingState;
use crate::state::{
    controller_presence_prefix, device_claim_prefix, hardware_inventory_key,
    hardware_manager_address, parse_device_claim_key, parse_presence_endpoint_key,
    presence_endpoint_key, DeviceClaim, EndpointPresence, HardwareInventory,
    HardwareInventoryDevice, HARDWARE_MESSAGES_LANE, HEARTBEAT_SECONDS, STATE_RECONCILE_SECONDS,
    WATCH_RETRY_SECONDS,
};
use crate::wire::{
    DeckrMessage, DeviceRef, HardwareMessageBody, HARDWARE_MESSAGES_LANE as WIRE_HARDWARE_LANE,
};

const DISCOVERY_INTERVAL: Duration = Duration::from_secs(1);
const READ_TIMEOUT_MS: i32 = 100;
const MAX_BACKOFF_SECS: u64 = 10;

#[derive(Debug, Clone)]
pub enum RuntimeCommand {
    SetRasterFrame { control_id: String, image: Vec<u8> },
    ClearRaster { control_id: String },
    SleepDevice,
    WakeDevice,
    ResetDevice,
    Stop,
}

#[derive(Debug, Clone)]
enum WorkerEvent {
    Connected {
        path_key: String,
        device_id: String,
        command_tx: Sender<RuntimeCommand>,
        device: crate::wire::DeviceDescriptor,
    },
    Input {
        device_id: String,
        body: HardwareMessageBody,
    },
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
    nats_url: String,
    state_bucket: String,
    manager_id: String,
    session_id: String,
    backend: Arc<dyn Backend>,
    layouts: Arc<Vec<Layout>>,
}

impl MiraBoxRemoteManager {
    pub fn new(nats_url: String, state_bucket: String, manager_id: String) -> Result<Self> {
        Ok(Self::with_backend(
            nats_url,
            state_bucket,
            manager_id,
            Arc::new(HidBackend),
            Arc::new(load_embedded_layouts()?),
        ))
    }

    pub fn with_backend(
        nats_url: String,
        state_bucket: String,
        manager_id: String,
        backend: Arc<dyn Backend>,
        layouts: Arc<Vec<Layout>>,
    ) -> Self {
        Self {
            nats_url,
            state_bucket,
            manager_id,
            session_id: Uuid::new_v4().to_string(),
            backend,
            layouts,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut backoff = 1u64;
        loop {
            match self.run_connected_session().await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    error!(
                        "NATS manager {} disconnected; retrying in {}s: {error:#}",
                        self.manager_id, backoff
                    );
                }
            }
            time::sleep(Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(MAX_BACKOFF_SECS);
        }
    }

    async fn run_connected_session(&self) -> Result<()> {
        let runtime = Arc::new(
            NatsDeckrRuntime::connect(&self.nats_url, &self.state_bucket)
                .await
                .with_context(|| format!("connecting manager {} to NATS", self.manager_id))?,
        );
        info!(
            "Connected manager {} to NATS at {} using state bucket {}",
            self.manager_id, self.nats_url, self.state_bucket
        );

        let shared = Arc::new(Mutex::new(ManagerState::new(
            self.manager_id.clone(),
            self.session_id.clone(),
        )));
        let (supervisor_event_tx, supervisor_event_rx) =
            tokio_mpsc::unbounded_channel::<WorkerEvent>();
        let (manager_event_tx, manager_event_rx) = tokio_mpsc::unbounded_channel::<WorkerEvent>();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let supervisor = Supervisor::new(
            self.manager_id.clone(),
            self.backend.clone(),
            self.layouts.clone(),
            supervisor_event_tx,
            supervisor_event_rx,
            manager_event_tx,
        );

        publish_presence_safely(runtime.clone(), shared.clone()).await;
        publish_inventory_safely(runtime.clone(), shared.clone()).await;

        let mut supervisor_handle = tokio::spawn(async move { supervisor.run(shutdown_rx).await });
        let mut tasks = JoinSet::<Result<()>>::new();
        tasks.spawn(worker_event_loop(
            runtime.clone(),
            shared.clone(),
            manager_event_rx,
        ));
        tasks.spawn(inbound_command_loop(runtime.clone(), shared.clone()));
        tasks.spawn(heartbeat_loop(runtime.clone(), shared.clone()));
        tasks.spawn(claim_watch_loop(runtime.clone(), shared.clone()));
        tasks.spawn(controller_presence_loop(runtime.clone(), shared.clone()));
        tasks.spawn(routing_reconciliation_loop(runtime.clone(), shared.clone()));

        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("waiting for shutdown signal")?;
                info!("Shutting down MiraBox manager {}", self.manager_id);
                let _ = shutdown_tx.send(());
                tasks.abort_all();
                let _ = supervisor_handle.await;
                withdraw_inventory_safely(runtime.clone(), shared.clone()).await;
                withdraw_presence_safely(runtime, shared).await;
                Ok(())
            }
            result = &mut supervisor_handle => {
                tasks.abort_all();
                result.context("joining device supervisor")??;
                bail!("device supervisor stopped unexpectedly")
            }
            result = tasks.join_next() => {
                let _ = shutdown_tx.send(());
                tasks.abort_all();
                let _ = supervisor_handle.await;
                match result {
                    Some(Ok(Ok(()))) => bail!("manager runtime task stopped unexpectedly"),
                    Some(Ok(Err(error))) => Err(error),
                    Some(Err(error)) => Err(error).context("joining manager runtime task"),
                    None => bail!("manager runtime tasks stopped unexpectedly"),
                }
            }
        }
    }
}

struct ManagerState {
    manager_id: String,
    endpoint: String,
    session_id: String,
    devices: BTreeMap<String, crate::wire::DeviceDescriptor>,
    command_map: HashMap<String, Sender<RuntimeCommand>>,
    routing: RoutingState,
    presence_revision: Option<u64>,
    inventory_revision: Option<u64>,
}

impl ManagerState {
    fn new(manager_id: String, session_id: String) -> Self {
        let endpoint = hardware_manager_address(&manager_id);
        Self {
            manager_id,
            endpoint,
            session_id,
            devices: BTreeMap::new(),
            command_map: HashMap::new(),
            routing: RoutingState::default(),
            presence_revision: None,
            inventory_revision: None,
        }
    }

    fn inventory(&self) -> HardwareInventory {
        HardwareInventory::new(
            &self.manager_id,
            &self.session_id,
            self.devices
                .iter()
                .map(|(device_id, device)| {
                    (
                        device_id.clone(),
                        HardwareInventoryDevice::from_device(&self.manager_id, device),
                    )
                })
                .collect(),
        )
    }
}

async fn heartbeat_loop(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) -> Result<()> {
    loop {
        publish_presence_safely(runtime.clone(), shared.clone()).await;
        publish_inventory_safely(runtime.clone(), shared.clone()).await;
        time::sleep(Duration::from_secs(HEARTBEAT_SECONDS)).await;
    }
}

async fn publish_presence_safely(runtime: Arc<NatsDeckrRuntime>, shared: Arc<Mutex<ManagerState>>) {
    if let Err(error) = publish_presence(runtime, shared).await {
        warn!("MiraBox manager current state is unavailable; heartbeat will retry: {error:#}");
    }
}

async fn publish_presence(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) -> Result<()> {
    let (key, value) = {
        let state = shared.lock().await;
        (
            presence_endpoint_key(HARDWARE_MESSAGES_LANE, &state.endpoint)
                .context("manager endpoint should be valid")?,
            EndpointPresence::manager(&state.manager_id, &state.session_id),
        )
    };
    let revision = runtime.state().put(&key, &value).await?;
    shared.lock().await.presence_revision = Some(revision);
    Ok(())
}

async fn publish_inventory_safely(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) {
    if let Err(error) = publish_inventory(runtime, shared).await {
        warn!("MiraBox inventory current state is unavailable; heartbeat will retry: {error:#}");
    }
}

async fn publish_inventory(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) -> Result<()> {
    let (key, value) = {
        let state = shared.lock().await;
        (hardware_inventory_key(&state.manager_id), state.inventory())
    };
    let revision = runtime.state().put(&key, &value).await?;
    shared.lock().await.inventory_revision = Some(revision);
    Ok(())
}

async fn withdraw_presence_safely(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) {
    let (key, revision) = {
        let state = shared.lock().await;
        let Some(key) = presence_endpoint_key(HARDWARE_MESSAGES_LANE, &state.endpoint) else {
            return;
        };
        (key, state.presence_revision)
    };
    if let Some(revision) = revision {
        if let Err(error) = runtime.state().delete_revision(&key, Some(revision)).await {
            warn!("Failed to withdraw MiraBox manager presence: {error:#}");
        }
    }
}

async fn withdraw_inventory_safely(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) {
    let (key, revision) = {
        let state = shared.lock().await;
        (
            hardware_inventory_key(&state.manager_id),
            state.inventory_revision,
        )
    };
    if let Some(revision) = revision {
        if let Err(error) = runtime.state().delete_revision(&key, Some(revision)).await {
            warn!("Failed to withdraw MiraBox inventory: {error:#}");
        }
    }
}

async fn claim_watch_loop(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) -> Result<()> {
    let prefix = {
        let state = shared.lock().await;
        device_claim_prefix(&state.manager_id)
    };
    loop {
        match runtime.state().wait_for_change(&prefix).await {
            Ok(()) => {
                reconcile_routing_current_state(
                    runtime.clone(),
                    shared.clone(),
                    "device claim watch",
                )
                .await?
            }
            Err(error) => {
                warn!("MiraBox device claim state is unavailable; watch will retry: {error:#}");
                time::sleep(Duration::from_secs(WATCH_RETRY_SECONDS)).await;
            }
        }
    }
}

async fn controller_presence_loop(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) -> Result<()> {
    let prefix = controller_presence_prefix();
    loop {
        match runtime.state().wait_for_change(&prefix).await {
            Ok(()) => {
                reconcile_routing_current_state(
                    runtime.clone(),
                    shared.clone(),
                    "controller presence watch",
                )
                .await?
            }
            Err(error) => {
                warn!("Controller endpoint presence state is unavailable; watch will retry: {error:#}");
                time::sleep(Duration::from_secs(WATCH_RETRY_SECONDS)).await;
            }
        }
    }
}

async fn routing_reconciliation_loop(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) -> Result<()> {
    loop {
        if let Err(error) =
            reconcile_routing_current_state(runtime.clone(), shared.clone(), "broker snapshot")
                .await
        {
            warn!(
                "MiraBox routing current state unavailable; reconciliation will retry: {error:#}"
            );
        }
        time::sleep(Duration::from_secs(STATE_RECONCILE_SECONDS)).await;
    }
}

async fn reconcile_routing_current_state(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
    reason: &'static str,
) -> Result<()> {
    let (manager_id, claim_prefix) = {
        let state = shared.lock().await;
        (
            state.manager_id.clone(),
            device_claim_prefix(&state.manager_id),
        )
    };
    let claim_entries = runtime.state().items(&claim_prefix).await?;
    let presence_entries = runtime.state().items(&controller_presence_prefix()).await?;

    let mut next_claims = HashMap::<String, DeviceClaim>::new();
    let mut invalid_claim_devices = HashSet::<String>::new();
    let mut next_controller_sessions = HashMap::<String, String>::new();

    for entry in claim_entries {
        let Some((entry_manager_id, device_id)) = parse_device_claim_key(&entry.key) else {
            continue;
        };
        if entry_manager_id != manager_id {
            continue;
        }
        match serde_json::from_value::<DeviceClaim>(entry.value) {
            Ok(claim) => {
                next_claims.insert(device_id, claim);
            }
            Err(error) => {
                warn!(
                    "Ignoring invalid MiraBox device claim {}: {error}",
                    entry.key
                );
                invalid_claim_devices.insert(device_id);
            }
        }
    }

    for entry in presence_entries {
        let Some((lane, endpoint)) = parse_presence_endpoint_key(&entry.key) else {
            continue;
        };
        if lane != WIRE_HARDWARE_LANE || endpoint.family != "controller" {
            continue;
        }
        match serde_json::from_value::<EndpointPresence>(entry.value) {
            Ok(presence) => {
                if presence.endpoint != endpoint.address || presence.lane != lane {
                    warn!(
                        "Ignoring controller presence {} with mismatched payload",
                        entry.key
                    );
                    continue;
                }
                next_controller_sessions.insert(endpoint.address, presence.session_id);
            }
            Err(error) => {
                warn!(
                    "Ignoring invalid controller presence {}: {error}",
                    entry.key
                );
            }
        }
    }

    debug!("Reconciling MiraBox routing current state via {reason}");
    let senders_to_reset = {
        let mut state = shared.lock().await;
        let devices_to_reset = state.routing.reconcile_snapshot(
            next_claims,
            next_controller_sessions,
            invalid_claim_devices,
        );
        devices_to_reset
            .into_iter()
            .filter_map(|device_id| state.command_map.get(&device_id).cloned())
            .collect::<Vec<_>>()
    };
    for sender in senders_to_reset {
        let _ = sender.send(RuntimeCommand::ResetDevice);
    }
    Ok(())
}

async fn worker_event_loop(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
    mut worker_rx: tokio_mpsc::UnboundedReceiver<WorkerEvent>,
) -> Result<()> {
    while let Some(event) = worker_rx.recv().await {
        match event {
            WorkerEvent::Connected {
                path_key,
                device_id,
                command_tx,
                device,
            } => {
                debug!("MiraBox device connected path={path_key} device={device_id}");
                let descriptor = device.clone();
                let manager_id = {
                    let mut state = shared.lock().await;
                    let manager_id = state.manager_id.clone();
                    state.devices.insert(device_id.clone(), device);
                    state.command_map.insert(device_id.clone(), command_tx);
                    manager_id
                };
                publish_inventory_safely(runtime.clone(), shared.clone()).await;
                let message = DeckrMessage::hardware_input(
                    &manager_id,
                    &device_id,
                    HardwareMessageBody::DeviceAvailable { descriptor },
                )?;
                runtime.publish(&message).await?;
            }
            WorkerEvent::Input { device_id, body } => {
                if !matches!(body, HardwareMessageBody::ControlInput { .. }) {
                    continue;
                }
                let recipient = {
                    let state = shared.lock().await;
                    state
                        .routing
                        .claim_recipient(&device_id)
                        .map(str::to_string)
                };
                let Some(recipient) = recipient else {
                    debug!("Dropping unclaimed MiraBox input for {device_id}");
                    continue;
                };
                let manager_id = match &body {
                    HardwareMessageBody::ControlInput { device_ref, .. } => {
                        device_ref.manager_id.clone()
                    }
                    _ => unreachable!(),
                };
                let message =
                    DeckrMessage::hardware_input_to(&manager_id, &device_id, &recipient, body)?;
                runtime.publish(&message).await?;
            }
            WorkerEvent::Disconnected {
                path_key,
                device_id,
            } => {
                debug!("MiraBox device disconnected path={path_key} device={device_id}");
                let manager_id = {
                    let mut state = shared.lock().await;
                    let manager_id = state.manager_id.clone();
                    state.devices.remove(&device_id);
                    state.command_map.remove(&device_id);
                    state.routing.remove_device(&device_id);
                    manager_id
                };
                publish_inventory_safely(runtime.clone(), shared.clone()).await;
                let message = DeckrMessage::hardware_input(
                    &manager_id,
                    &device_id,
                    HardwareMessageBody::DeviceUnavailable {
                        device_ref: DeviceRef {
                            manager_id: manager_id.clone(),
                            device_id: device_id.clone(),
                            fingerprint: None,
                        },
                        reason: Some("disconnected".to_string()),
                    },
                )?;
                runtime.publish(&message).await?;
            }
            WorkerEvent::Failed { path_key, error } => {
                warn!("Device worker {path_key} failed: {error}");
            }
        }
    }
    bail!("device worker event stream closed")
}

async fn inbound_command_loop(
    runtime: Arc<NatsDeckrRuntime>,
    shared: Arc<Mutex<ManagerState>>,
) -> Result<()> {
    let mut subscriber = runtime.subscribe_hardware_messages().await?;
    while let Some(message) = subscriber.next().await {
        match runtime.message_from_nats(message) {
            Ok(envelope) => route_inbound_command(shared.clone(), envelope).await?,
            Err(error) => debug!("Dropping invalid NATS Deckr lane message: {error:#}"),
        }
    }
    bail!("hardware_messages subscription ended")
}

async fn route_inbound_command(
    shared: Arc<Mutex<ManagerState>>,
    envelope: DeckrMessage,
) -> Result<()> {
    if envelope.lane != WIRE_HARDWARE_LANE || envelope.is_expired() {
        return Ok(());
    }
    let body = match envelope.hardware_body() {
        Ok(body) => body,
        Err(error) => {
            debug!("Ignoring unsupported hardware message body: {error:#}");
            return Ok(());
        }
    };
    if !body.is_command() {
        return Ok(());
    }
    let Some(device_id) = envelope.subject.device_id().map(str::to_string) else {
        debug!("Ignoring inbound hardware command without device subject");
        return Ok(());
    };
    let Some(subject_manager_id) = envelope.subject.manager_id() else {
        return Ok(());
    };
    let command = match runtime_command_from_body(body) {
        Ok(command) => command,
        Err(error) => {
            debug!("Ignoring unsupported hardware command: {error:#}");
            return Ok(());
        }
    };
    let sender = {
        let state = shared.lock().await;
        if envelope.recipient_endpoint() != Some(state.endpoint.as_str()) {
            return Ok(());
        }
        if subject_manager_id != state.manager_id {
            return Ok(());
        }
        if !state.devices.contains_key(&device_id) {
            debug!(
                "Dropping command for unknown MiraBox device {}/{}",
                subject_manager_id, device_id
            );
            return Ok(());
        }
        if state.routing.claim_recipient(&device_id) != Some(envelope.sender.as_str()) {
            debug!(
                "Dropping unroutable MiraBox command for {}/{} from {}",
                subject_manager_id, device_id, envelope.sender
            );
            return Ok(());
        }
        state.command_map.get(&device_id).cloned()
    };
    if let Some(sender) = sender {
        if sender.send(command).is_err() {
            warn!("Dropping command for disconnected device {device_id}");
        }
    } else {
        debug!(
            "Dropping command for closed MiraBox device {}/{}",
            subject_manager_id, device_id
        );
    }
    Ok(())
}

fn runtime_command_from_body(body: HardwareMessageBody) -> Result<RuntimeCommand> {
    match body {
        HardwareMessageBody::ControlCommand {
            control_id,
            capability_id,
            command_type,
            params,
            ..
        } if capability_id == "raster.bitmap" && command_type == "set_frame" => {
            let control_id = control_id.context("raster set_frame requires controlId")?;
            let image = params
                .get("image")
                .and_then(|value| value.as_str())
                .context("controlCommand set_frame requires image string")?;
            Ok(RuntimeCommand::SetRasterFrame {
                control_id,
                image: STANDARD
                    .decode(image.as_bytes())
                    .context("decoding controlCommand image")?,
            })
        }
        HardwareMessageBody::ControlCommand {
            control_id,
            capability_id,
            command_type,
            ..
        } if capability_id == "raster.bitmap" && command_type == "clear" => {
            let control_id = control_id.context("raster clear requires controlId")?;
            Ok(RuntimeCommand::ClearRaster { control_id })
        }
        HardwareMessageBody::ControlCommand {
            capability_id,
            command_type,
            ..
        } if capability_id == "device.power" && command_type == "sleep" => {
            Ok(RuntimeCommand::SleepDevice)
        }
        HardwareMessageBody::ControlCommand {
            capability_id,
            command_type,
            ..
        } if capability_id == "device.power" && command_type == "wake" => {
            Ok(RuntimeCommand::WakeDevice)
        }
        HardwareMessageBody::ControlCommand {
            capability_id,
            command_type,
            ..
        } => {
            bail!("unsupported controlCommand {capability_id}/{command_type}")
        }
        _ => bail!("not a runtime command"),
    }
}

struct Supervisor {
    manager_id: String,
    backend: Arc<dyn Backend>,
    layouts: Arc<Vec<Layout>>,
    worker_tx: tokio_mpsc::UnboundedSender<WorkerEvent>,
    worker_rx: tokio_mpsc::UnboundedReceiver<WorkerEvent>,
    manager_tx: tokio_mpsc::UnboundedSender<WorkerEvent>,
}

impl Supervisor {
    fn new(
        manager_id: String,
        backend: Arc<dyn Backend>,
        layouts: Arc<Vec<Layout>>,
        worker_tx: tokio_mpsc::UnboundedSender<WorkerEvent>,
        worker_rx: tokio_mpsc::UnboundedReceiver<WorkerEvent>,
        manager_tx: tokio_mpsc::UnboundedSender<WorkerEvent>,
    ) -> Self {
        Self {
            manager_id,
            backend,
            layouts,
            worker_tx,
            worker_rx,
            manager_tx,
        }
    }

    async fn run(mut self, mut shutdown_rx: oneshot::Receiver<()>) -> Result<()> {
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
                    let descriptors = enumerate_canonical(self.backend.clone(), self.layouts.clone()).await?;
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
                            self.worker_tx.clone(),
                            command_tx,
                            command_rx,
                        );
                    }
                }
                maybe_event = self.worker_rx.recv() => {
                    let Some(event) = maybe_event else { continue; };
                    match &event {
                        WorkerEvent::Connected { path_key, .. } => {
                            launched_paths.remove(path_key.as_str());
                            active_paths.insert(path_key.clone());
                        }
                        WorkerEvent::Disconnected { path_key, .. }
                        | WorkerEvent::Failed { path_key, .. } => {
                            launched_paths.remove(path_key.as_str());
                            active_paths.remove(path_key.as_str());
                        }
                        WorkerEvent::Input { .. } => {}
                    }
                    let _ = self.manager_tx.send(event);
                }
            }
        }

        for sender in worker_senders {
            let _ = sender.send(RuntimeCommand::Stop);
        }
        Ok(())
    }
}

async fn enumerate_canonical(
    backend: Arc<dyn Backend>,
    layouts: Arc<Vec<Layout>>,
) -> Result<Vec<HidDeviceCandidate>> {
    let rows = tokio::task::spawn_blocking(move || backend.enumerate())
        .await
        .context("joining enumerate task")??;

    let mut grouped = HashMap::<(u16, u16, String), Vec<HidDeviceCandidate>>::new();
    for descriptor in rows {
        if !layouts
            .iter()
            .any(|layout| layout.matches_candidate(&descriptor).unwrap_or(false))
        {
            continue;
        }
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
    descriptor: HidDeviceCandidate,
    worker_tx: tokio_mpsc::UnboundedSender<WorkerEvent>,
    command_tx: Sender<RuntimeCommand>,
    command_rx: mpsc::Receiver<RuntimeCommand>,
) {
    let path_key = descriptor.path_hex();
    thread::spawn(move || {
        if let Err(error) = device_worker(
            backend,
            manager_id,
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
    backend: Arc<dyn Backend>,
    manager_id: String,
    layouts: Arc<Vec<Layout>>,
    descriptor: HidDeviceCandidate,
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
            device: layout.device_descriptor(&local_device_id, &local_device_id, &local_device_id),
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
            for body in
                layout.translate_event(event, &manager_id, &local_device_id, &local_device_id)
            {
                let _ = worker_tx.send(WorkerEvent::Input {
                    device_id: local_device_id.clone(),
                    body,
                });
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
        "wake_display" => Ok(ProtocolCommand::WakeDisplay),
        "sleep_display" => Ok(ProtocolCommand::SleepDisplay),
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
    let commands = match command {
        RuntimeCommand::SetRasterFrame { control_id, image } => {
            let Some(display_id) = layout.display_id_for_control(&control_id) else {
                warn!("Ignoring raster set_frame for unknown control {control_id}");
                return Ok(());
            };
            vec![ProtocolCommand::SetKeyImage {
                key: display_id,
                image,
                x: 0,
                y: 0,
            }]
        }
        RuntimeCommand::ClearRaster { control_id } => {
            let Some(display_id) = layout.display_id_for_control(&control_id) else {
                warn!("Ignoring raster clear for unknown control {control_id}");
                return Ok(());
            };
            vec![ProtocolCommand::ClearKey {
                target: display_id as u32,
            }]
        }
        RuntimeCommand::SleepDevice => vec![ProtocolCommand::SleepDisplay],
        RuntimeCommand::WakeDevice => vec![ProtocolCommand::WakeDisplay],
        RuntimeCommand::ResetDevice => vec![
            ProtocolCommand::ClearKey { target: 0xFF },
            ProtocolCommand::Refresh,
        ],
        RuntimeCommand::Stop => return Ok(()),
    };
    for command in commands {
        for packet in protocol.encode_command(&command) {
            handle.write(&packet)?;
        }
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex as StdMutex};

    use super::*;
    use crate::backend::{Backend, DeviceHandle};

    #[derive(Clone)]
    struct FakeBackend {
        enumerate_rows: Arc<StdMutex<Vec<HidDeviceCandidate>>>,
        device: Arc<StdMutex<FakeDeviceState>>,
    }

    struct FakeDeviceState {
        firmware_report: Vec<u8>,
        reports: VecDeque<Vec<u8>>,
        writes: Vec<Vec<u8>>,
    }

    impl FakeBackend {
        fn new() -> Self {
            Self {
                enumerate_rows: Arc::new(StdMutex::new(vec![HidDeviceCandidate {
                    path: b"fake-path".to_vec(),
                    vendor_id: 2816,
                    product_id: 4097,
                    serial_number: "0300D0785616".to_string(),
                    usage_page: None,
                    usage: None,
                    interface_number: Some(0),
                }])),
                device: Arc::new(StdMutex::new(FakeDeviceState {
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
        fn enumerate(&self) -> Result<Vec<HidDeviceCandidate>> {
            Ok(self.enumerate_rows.lock().unwrap().clone())
        }

        fn open(&self, _path: &[u8]) -> Result<Box<dyn DeviceHandle>> {
            Ok(Box::new(FakeHandle {
                state: self.device.clone(),
            }))
        }
    }

    struct FakeHandle {
        state: Arc<StdMutex<FakeDeviceState>>,
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

    fn ack_report(button_id: u16, payload: u8) -> Vec<u8> {
        let mut report = vec![0; 64];
        report[0..3].copy_from_slice(b"ACK");
        report[8..10].copy_from_slice(&button_id.to_be_bytes());
        report[10] = payload;
        report
    }

    fn raster_command(command_type: &str) -> HardwareMessageBody {
        let mut params = serde_json::Map::new();
        if command_type == "set_frame" {
            params.insert(
                "image".to_string(),
                serde_json::Value::String(STANDARD.encode(b"ok")),
            );
        }
        HardwareMessageBody::ControlCommand {
            device_ref: DeviceRef {
                manager_id: "mirabox-main".to_string(),
                device_id: "deck".to_string(),
                fingerprint: None,
            },
            control_id: Some("0,0".to_string()),
            capability_id: "raster.bitmap".to_string(),
            command_type: command_type.to_string(),
            params,
        }
    }

    fn power_command(command_type: &str) -> HardwareMessageBody {
        HardwareMessageBody::ControlCommand {
            device_ref: DeviceRef {
                manager_id: "mirabox-main".to_string(),
                device_id: "deck".to_string(),
                fingerprint: None,
            },
            control_id: None,
            capability_id: "device.power".to_string(),
            command_type: command_type.to_string(),
            params: serde_json::Map::new(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn discovery_filters_non_candidate_hid_rows() {
        let backend = Arc::new(FakeBackend::new());
        backend
            .enumerate_rows
            .lock()
            .unwrap()
            .push(HidDeviceCandidate {
                path: b"keyboard".to_vec(),
                vendor_id: 2816,
                product_id: 4097,
                serial_number: "0300D0785616".to_string(),
                usage_page: Some(1),
                usage: Some(6),
                interface_number: Some(1),
            });
        let layouts = Arc::new(load_embedded_layouts().expect("layouts should load"));

        let descriptors = enumerate_canonical(backend, layouts)
            .await
            .expect("discovery should succeed");

        assert_eq!(descriptors.len(), 1);
        assert_eq!(descriptors[0].path, b"fake-path");
    }

    #[test]
    fn reset_device_writes_clear_all_and_refresh() {
        let backend = FakeBackend::new();
        let mut handle = FakeHandle {
            state: backend.device.clone(),
        };
        let layouts = load_embedded_layouts().expect("layouts should load");
        let descriptor = backend.enumerate().unwrap().remove(0);
        let protocol = MiraBoxProtocol::default();
        let layout = resolve_layout(&layouts, &descriptor, "V25.MSD_TWO.01.005").unwrap();

        apply_runtime_command(&mut handle, &protocol, layout, RuntimeCommand::ResetDevice).unwrap();

        let writes = backend.writes();
        assert!(writes
            .iter()
            .any(|payload| payload.starts_with(b"\x00CRT\x00\x00CLE")));
        assert!(writes
            .iter()
            .any(|payload| payload.starts_with(b"\x00CRT\x00\x00STP")));
    }

    #[test]
    fn runtime_command_from_hardware_body_maps_outputs() {
        assert!(matches!(
            runtime_command_from_body(raster_command("set_frame")).unwrap(),
            RuntimeCommand::SetRasterFrame { .. }
        ));
        assert!(matches!(
            runtime_command_from_body(raster_command("clear")).unwrap(),
            RuntimeCommand::ClearRaster { .. }
        ));
        assert!(matches!(
            runtime_command_from_body(power_command("sleep")).unwrap(),
            RuntimeCommand::SleepDevice
        ));
        assert!(matches!(
            runtime_command_from_body(power_command("wake")).unwrap(),
            RuntimeCommand::WakeDevice
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn command_routing_requires_claiming_controller() {
        let (command_tx, command_rx) = mpsc::channel();
        let shared = Arc::new(Mutex::new(ManagerState::new(
            "mirabox-main".to_string(),
            "manager-session".to_string(),
        )));
        {
            let mut state = shared.lock().await;
            state.devices.insert(
                "deck".to_string(),
                crate::wire::DeviceDescriptor {
                    device_id: "deck".to_string(),
                    fingerprint: "deck".to_string(),
                    display_name: "MiraBox".to_string(),
                    manufacturer: Some("MiraBox".to_string()),
                    model: Some("MiraBox".to_string()),
                    serial_number: Some("deck".to_string()),
                    controls: Vec::new(),
                    capabilities: Vec::new(),
                },
            );
            state.command_map.insert("deck".to_string(), command_tx);
            state.routing.reconcile_snapshot(
                HashMap::from([(
                    "deck".to_string(),
                    DeviceClaim {
                        claimed_by_endpoint: "controller:main".to_string(),
                        claimed_by_session_id: "s1".to_string(),
                        timestamp: "2026-04-29T00:00:00Z".to_string(),
                        ttl_seconds: 15,
                    },
                )]),
                HashMap::from([("controller:main".to_string(), "s1".to_string())]),
                HashSet::new(),
            );
        }

        let wrong = DeckrMessage::hardware_command(
            "other",
            "mirabox-main",
            "deck",
            raster_command("set_frame"),
        )
        .unwrap();
        route_inbound_command(shared.clone(), wrong).await.unwrap();
        assert!(command_rx.try_recv().is_err());

        let right = DeckrMessage::hardware_command(
            "main",
            "mirabox-main",
            "deck",
            raster_command("set_frame"),
        )
        .unwrap();
        route_inbound_command(shared, right).await.unwrap();
        assert!(matches!(
            command_rx.try_recv().unwrap(),
            RuntimeCommand::SetRasterFrame { .. }
        ));
    }

    #[test]
    fn fake_backend_can_translate_input_report() {
        let backend = FakeBackend::new();
        backend.push_report(ack_report(1, 1));
        assert_eq!(backend.device.lock().unwrap().reports.len(), 1);
    }
}
