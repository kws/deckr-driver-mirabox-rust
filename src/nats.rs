use std::collections::BTreeMap;
use std::fmt::Display;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_nats::jetstream::kv::{Config as KvConfig, Operation, Store};
use async_nats::jetstream::Context as JetStreamContext;
use async_nats::{HeaderMap, Message, Subscriber};
use deckr_core::{
    encode_key_token, headers_for as deckr_headers_for, payload_json_bytes,
    subject_for as deckr_subject_for, validate_headers as deckr_validate_headers,
    validate_subject_hint as deckr_validate_subject_hint, DeckrMessage, HARDWARE_MESSAGES_LANE,
    STATE_TTL_SECONDS,
};
use futures_util::{StreamExt, TryStreamExt};
use serde::Serialize;
use serde_json::Value;
use tracing::debug;

const LANE_PREFIX: &str = "deckr.lane";

#[derive(Debug, Clone)]
pub struct StateEntry {
    pub key: String,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub struct NatsDeckrRuntime {
    client: async_nats::Client,
    lease_state: NatsStateStore,
    discovery_state: NatsStateStore,
}

impl NatsDeckrRuntime {
    pub async fn connect(url: &str, lease_bucket: &str, discovery_bucket: &str) -> Result<Self> {
        let client = async_nats::connect(url)
            .await
            .with_context(|| format!("connecting to NATS at {url}"))?;
        let jetstream = async_nats::jetstream::new(client.clone());
        let lease_state = NatsStateStore::new(open_lease_bucket(&jetstream, lease_bucket).await?);
        let discovery_state =
            NatsStateStore::new(open_discovery_bucket(&jetstream, discovery_bucket).await?);
        Ok(Self {
            client,
            lease_state,
            discovery_state,
        })
    }

    pub fn lease_state(&self) -> &NatsStateStore {
        &self.lease_state
    }

    pub fn discovery_state(&self) -> &NatsStateStore {
        &self.discovery_state
    }

    pub async fn publish(&self, message: &DeckrMessage) -> Result<()> {
        let subject = deckr_subject_for(message);
        self.client
            .publish_with_headers(
                subject,
                header_map_for(message),
                payload_json_bytes(message)?.into(),
            )
            .await
            .context("publishing Deckr NATS lane message")
    }

    pub async fn subscribe_hardware_messages(&self) -> Result<Subscriber> {
        self.client
            .subscribe(format!(
                "{LANE_PREFIX}.{}.>",
                encode_key_token(HARDWARE_MESSAGES_LANE)
            ))
            .await
            .context("subscribing to hardware_messages")
    }

    pub fn message_from_nats(&self, message: Message) -> Result<DeckrMessage> {
        let envelope = DeckrMessage::from_bytes(&message.payload)
            .context("NATS message payload is not a Deckr envelope")?;
        deckr_validate_subject_hint(message.subject.as_str(), &envelope)?;
        validate_nats_headers(message.headers.as_ref(), &envelope)?;
        Ok(envelope)
    }
}

async fn open_lease_bucket(jetstream: &JetStreamContext, bucket: &str) -> Result<Store> {
    jetstream
        .create_or_update_key_value(KvConfig {
            bucket: bucket.to_string(),
            history: 1,
            max_age: Duration::from_secs(STATE_TTL_SECONDS),
            limit_markers: Some(Duration::from_secs(STATE_TTL_SECONDS)),
            ..Default::default()
        })
        .await
        .with_context(|| format!("opening Deckr KV lease-state bucket {bucket}"))
}

async fn open_discovery_bucket(jetstream: &JetStreamContext, bucket: &str) -> Result<Store> {
    match jetstream.get_key_value(bucket).await {
        Ok(store) => {
            validate_discovery_bucket(&store, bucket).await?;
            Ok(store)
        }
        Err(get_error) => {
            let created = jetstream
                .create_key_value(KvConfig {
                    bucket: bucket.to_string(),
                    history: 1,
                    max_age: Duration::ZERO,
                    limit_markers: None,
                    ..Default::default()
                })
                .await;
            match created {
                Ok(store) => {
                    validate_discovery_bucket(&store, bucket).await?;
                    Ok(store)
                }
                Err(create_error) => {
                    let store = jetstream.get_key_value(bucket).await.with_context(|| {
                        format!(
                            "opening Deckr KV discovery-state bucket {bucket}; initial open \
                                 failed with {get_error}; create failed with {create_error}"
                        )
                    })?;
                    validate_discovery_bucket(&store, bucket).await?;
                    Ok(store)
                }
            }
        }
    }
}

async fn validate_discovery_bucket(store: &Store, bucket: &str) -> Result<()> {
    let status = store
        .status()
        .await
        .with_context(|| format!("inspecting Deckr KV discovery-state bucket {bucket}"))?;
    if status.history() != 1 {
        bail!(
            "Deckr KV discovery-state bucket {bucket} has history {}; expected 1",
            status.history()
        );
    }
    if status.max_age() != Duration::ZERO {
        bail!(
            "Deckr KV discovery-state bucket {bucket} has broker TTL {:?}; expected no TTL",
            status.max_age()
        );
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct NatsStateStore {
    kv: Store,
}

impl NatsStateStore {
    fn new(kv: Store) -> Self {
        Self { kv }
    }

    pub async fn put<T: Serialize>(&self, key: &str, value: &T) -> Result<u64> {
        let payload = serde_json::to_vec(value)?;
        self.kv
            .put(key, payload.into())
            .await
            .with_context(|| format!("putting state key {key}"))
    }

    pub async fn delete_revision(&self, key: &str, revision: Option<u64>) -> Result<()> {
        self.kv
            .delete_expect_revision(key, revision)
            .await
            .with_context(|| format!("deleting state key {key}"))
    }

    pub async fn get(&self, key: &str) -> Result<Option<StateEntry>> {
        let Some(entry) = self
            .kv
            .entry(key.to_string())
            .await
            .with_context(|| format!("reading state key {key}"))?
        else {
            return Ok(None);
        };
        if entry.operation != Operation::Put {
            return Ok(None);
        }
        let value = serde_json::from_slice(&entry.value)
            .with_context(|| format!("decoding state key {key}"))?;
        Ok(Some(StateEntry {
            key: key.to_string(),
            value,
        }))
    }

    pub async fn items(&self, prefix: &str) -> Result<Vec<StateEntry>> {
        let mut keys = match self.kv.keys().await {
            Ok(keys) => keys,
            Err(error) if is_no_keys_error(&error) => return Ok(Vec::new()),
            Err(error) => return Err(error).context("listing Deckr current-state keys"),
        };
        let mut entries = Vec::new();
        while let Some(key) = match keys.try_next().await {
            Ok(key) => key,
            Err(error) if is_no_keys_error(&error) => None,
            Err(error) => return Err(error).context("reading Deckr current-state key list"),
        } {
            if !key.starts_with(prefix) {
                continue;
            }
            let Some(entry) = self
                .kv
                .entry(key.clone())
                .await
                .with_context(|| format!("reading state key {key}"))?
            else {
                continue;
            };
            if entry.operation != Operation::Put {
                continue;
            }
            let value = serde_json::from_slice(&entry.value)
                .with_context(|| format!("decoding state key {key}"))?;
            entries.push(StateEntry { key, value });
        }
        Ok(entries)
    }

    pub async fn wait_for_change(&self, prefix: &str) -> Result<()> {
        let watch_key = if prefix.ends_with('.') {
            format!("{prefix}>")
        } else {
            prefix.to_string()
        };
        let mut watch = self
            .kv
            .watch(watch_key)
            .await
            .with_context(|| format!("watching state prefix {prefix}"))?;
        match watch.next().await {
            Some(Ok(entry)) => {
                debug!(
                    "Deckr current-state wakeup key={} operation={:?}",
                    entry.key, entry.operation
                );
                Ok(())
            }
            Some(Err(error)) => Err(error).context("watching Deckr current state"),
            None => bail!("Deckr current-state watch ended for prefix {prefix}"),
        }
    }
}

fn header_map_for(message: &DeckrMessage) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (key, value) in deckr_headers_for(message) {
        headers.insert(key.as_str(), value.as_str());
    }
    headers
}

fn validate_nats_headers(headers: Option<&HeaderMap>, message: &DeckrMessage) -> Result<()> {
    let Some(headers) = headers else {
        return Ok(());
    };
    let mut present = BTreeMap::new();
    for key in deckr_headers_for(message).keys() {
        if let Some(value) = headers.get(key.as_str()) {
            present.insert(key.clone(), value.as_str().to_string());
        }
    }
    deckr_validate_headers(Some(&present), message)?;
    Ok(())
}

fn is_no_keys_error(error: &impl Display) -> bool {
    let message = error.to_string().to_lowercase();
    message.contains("no keys") || message.contains("no messages")
}

#[cfg(test)]
mod tests {
    use super::*;
    use deckr_core::{
        controller_presence_prefix, device_claim_prefix, hardware_inventory_key,
        hardware_manager_address, presence_endpoint_key, validate_lane_message, DeviceDescriptor,
        DeviceRef, HardwareMessageBody,
    };

    #[test]
    fn subject_and_headers_match_python_nats_adapter() {
        let message = DeckrMessage::hardware_input_to(
            "mirabox-main",
            "manager-session",
            "deck",
            "controller:main".parse().unwrap(),
            "controller-session",
            HardwareMessageBody::ControlInput {
                device_ref: DeviceRef {
                    manager_id: "mirabox-main".to_string(),
                    device_id: "deck".to_string(),
                    fingerprint: None,
                },
                control_id: "0,0".to_string(),
                capability_id: "button.momentary".to_string(),
                event_type: "down".to_string(),
                value: Some(serde_json::json!({"eventType": "down"})),
                sequence: None,
                occurred_at: None,
                sources: Vec::new(),
            },
        )
        .unwrap();

        assert_eq!(
            deckr_subject_for(&message),
            "deckr.lane.hardware_messages.hardware_manager.mirabox-main"
        );
        let headers = header_map_for(&message);
        assert_eq!(
            headers.get("Deckr-Message-Id").unwrap().as_str(),
            message.message_id.as_str()
        );
        assert_eq!(
            headers.get("Deckr-Message-Type").unwrap().as_str(),
            "controlInput"
        );
        assert_eq!(
            headers.get("Deckr-Sender").unwrap().as_str(),
            "hardware_manager:mirabox-main"
        );
        assert_eq!(
            headers.get("Deckr-Sender-Session").unwrap().as_str(),
            "manager-session"
        );
        assert_eq!(
            headers.get("Deckr-Recipient").unwrap().as_str(),
            "controller:main"
        );
        assert_eq!(
            headers.get("Deckr-Recipient-Session").unwrap().as_str(),
            "controller-session"
        );
    }

    #[test]
    fn core_subject_validation_rejects_mismatched_subjects() {
        let message = DeckrMessage::hardware_input(
            "mirabox-main",
            "manager-session",
            "deck",
            HardwareMessageBody::DeviceUnavailable {
                device_ref: DeviceRef {
                    manager_id: "mirabox-main".to_string(),
                    device_id: "deck".to_string(),
                    fingerprint: None,
                },
                reason: None,
            },
        )
        .unwrap();

        assert!(deckr_validate_subject_hint(
            "deckr.lane.hardware_messages.driver.mirabox-main",
            &message
        )
        .is_err());
    }

    #[test]
    fn representative_hardware_messages_use_core_contract_shapes() {
        let descriptor = DeviceDescriptor {
            device_id: "deck".to_string(),
            fingerprint: "fingerprint".to_string(),
            display_name: "MiraBox".to_string(),
            manufacturer: Some("MiraBox".to_string()),
            model: Some("MSD_TWO".to_string()),
            ..Default::default()
        };
        let device_available = DeckrMessage::hardware_input(
            "mirabox-main",
            "manager-session",
            "deck",
            HardwareMessageBody::DeviceAvailable {
                descriptor: descriptor.clone(),
            },
        )
        .unwrap();
        validate_lane_message(&device_available).unwrap();
        assert_eq!(device_available.message_type, "deviceAvailable");
        assert_eq!(
            device_available.body["descriptor"]["deviceId"].as_str(),
            Some("deck")
        );

        let control_input = DeckrMessage::hardware_input(
            "mirabox-main",
            "manager-session",
            "deck",
            HardwareMessageBody::ControlInput {
                device_ref: DeviceRef {
                    manager_id: "mirabox-main".to_string(),
                    device_id: "deck".to_string(),
                    fingerprint: Some("fingerprint".to_string()),
                },
                control_id: "0,0".to_string(),
                capability_id: "button.momentary".to_string(),
                event_type: "down".to_string(),
                value: Some(serde_json::json!({"eventType": "down"})),
                sequence: None,
                occurred_at: None,
                sources: Vec::new(),
            },
        )
        .unwrap();
        validate_lane_message(&control_input).unwrap();
        assert_eq!(control_input.message_type, "controlInput");
        assert_eq!(control_input.body["controlId"].as_str(), Some("0,0"));

        let control_command = DeckrMessage::hardware_command(
            "main",
            "controller-session",
            "mirabox-main",
            "manager-session",
            "deck",
            HardwareMessageBody::ControlCommand {
                device_ref: DeviceRef {
                    manager_id: "mirabox-main".to_string(),
                    device_id: "deck".to_string(),
                    fingerprint: None,
                },
                control_id: Some("0,0".to_string()),
                capability_id: "raster.bitmap".to_string(),
                command_type: "clear".to_string(),
                params: serde_json::Map::new(),
            },
        )
        .unwrap();
        validate_lane_message(&control_command).unwrap();
        assert_eq!(control_command.message_type, "controlCommand");
        assert_eq!(control_command.body["commandType"].as_str(), Some("clear"));
    }

    #[test]
    fn state_keys_and_nats_binding_are_core_helpers() {
        let manager_endpoint = hardware_manager_address("mirabox-main").unwrap();
        assert_eq!(
            presence_endpoint_key(HARDWARE_MESSAGES_LANE, &manager_endpoint),
            "presence.endpoint.hardware_messages.hardware_manager.mirabox-main"
        );
        assert_eq!(
            controller_presence_prefix(),
            "presence.endpoint.hardware_messages.controller."
        );
        assert_eq!(
            device_claim_prefix("mirabox-main"),
            "claim.device.mirabox-main."
        );
        assert_eq!(
            hardware_inventory_key("mirabox-main"),
            "inventory.hardware.mirabox-main"
        );

        let message = DeckrMessage::hardware_input(
            "mirabox-main",
            "manager-session",
            "deck",
            HardwareMessageBody::DeviceUnavailable {
                device_ref: DeviceRef {
                    manager_id: "mirabox-main".to_string(),
                    device_id: "deck".to_string(),
                    fingerprint: None,
                },
                reason: Some("test".to_string()),
            },
        )
        .unwrap();
        assert_eq!(
            deckr_subject_for(&message),
            "deckr.lane.hardware_messages.hardware_manager.mirabox-main"
        );
        assert_eq!(
            deckr_headers_for(&message)
                .get("Deckr-Sender")
                .map(String::as_str),
            Some("hardware_manager:mirabox-main")
        );
    }
}
