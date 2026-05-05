use std::fmt::Display;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_nats::jetstream::kv::{Config as KvConfig, Operation, Store};
use async_nats::jetstream::Context as JetStreamContext;
use async_nats::{HeaderMap, Message, Subscriber};
use futures_util::{StreamExt, TryStreamExt};
use serde::Serialize;
use serde_json::Value;
use tracing::debug;

use crate::state::{encode_key_token, EndpointAddress, STATE_TTL_SECONDS};
use crate::wire::{DeckrMessage, MessageTarget, HARDWARE_MESSAGES_LANE};

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
        let subject = subject_for(message)?;
        self.client
            .publish_with_headers(
                subject,
                headers_for(message),
                serde_json::to_vec(message)?.into(),
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
        validate_subject_hint(message.subject.as_str(), &envelope)?;
        validate_headers(message.headers.as_ref(), &envelope)?;
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

pub fn subject_for(message: &DeckrMessage) -> Result<String> {
    let (family, endpoint_id) = parse_endpoint(&message.sender)
        .with_context(|| format!("invalid Deckr sender endpoint {}", message.sender))?;
    Ok(format!(
        "{LANE_PREFIX}.{}.{}.{}",
        encode_key_token(&message.lane),
        encode_key_token(&family),
        encode_key_token(&endpoint_id)
    ))
}

pub fn headers_for(message: &DeckrMessage) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("Deckr-Message-Id", message.message_id.as_str());
    headers.insert("Deckr-Message-Type", message.message_type.as_str());
    headers.insert("Deckr-Sender", message.sender.as_str());
    headers.insert("Deckr-Sender-Session", message.sender_session_id.as_str());
    headers.insert("Deckr-Recipient", recipient_header(message).as_str());
    if let Some(recipient_session_id) = &message.recipient_session_id {
        headers.insert("Deckr-Recipient-Session", recipient_session_id.as_str());
    }
    if let Some(in_reply_to) = &message.in_reply_to {
        headers.insert("Deckr-In-Reply-To", in_reply_to.as_str());
    }
    headers
}

fn recipient_header(message: &DeckrMessage) -> String {
    match &message.recipient {
        MessageTarget::Endpoint { endpoint } => endpoint.clone(),
        MessageTarget::Broadcast {
            scope,
            endpoint_family,
            ..
        } => format!("broadcast:{scope}:{endpoint_family}"),
    }
}

fn validate_headers(headers: Option<&HeaderMap>, message: &DeckrMessage) -> Result<()> {
    let Some(headers) = headers else {
        return Ok(());
    };
    for (key, expected) in [
        ("Deckr-Message-Id", message.message_id.clone()),
        ("Deckr-Message-Type", message.message_type.clone()),
        ("Deckr-Sender", message.sender.clone()),
        ("Deckr-Sender-Session", message.sender_session_id.clone()),
        ("Deckr-Recipient", recipient_header(message)),
    ] {
        if let Some(value) = headers.get(key) {
            if value.as_str() != expected {
                bail!("NATS header {key} disagrees with Deckr envelope");
            }
        }
    }
    if let Some(in_reply_to) = &message.in_reply_to {
        if let Some(value) = headers.get("Deckr-In-Reply-To") {
            if value.as_str() != in_reply_to {
                bail!("NATS header Deckr-In-Reply-To disagrees with Deckr envelope");
            }
        }
    }
    if let Some(recipient_session_id) = &message.recipient_session_id {
        if let Some(value) = headers.get("Deckr-Recipient-Session") {
            if value.as_str() != recipient_session_id {
                bail!("NATS header Deckr-Recipient-Session disagrees with Deckr envelope");
            }
        }
    }
    Ok(())
}

fn validate_subject_hint(subject: &str, message: &DeckrMessage) -> Result<()> {
    if !subject.starts_with(&format!("{LANE_PREFIX}.")) {
        return Ok(());
    }
    let Some((family, endpoint_id)) = parse_endpoint(&message.sender) else {
        bail!("invalid Deckr sender endpoint {}", message.sender);
    };
    let expected = [
        "deckr".to_string(),
        "lane".to_string(),
        encode_key_token(&message.lane),
        encode_key_token(&family),
        encode_key_token(&endpoint_id),
    ];
    let tokens = subject
        .split('.')
        .take(5)
        .map(str::to_string)
        .collect::<Vec<_>>();
    if tokens != expected {
        bail!("NATS subject disagrees with Deckr envelope sender");
    }
    Ok(())
}

fn parse_endpoint(endpoint: &str) -> Option<(String, String)> {
    let endpoint = EndpointAddress::parse(endpoint)?;
    Some((endpoint.family, endpoint.endpoint_id))
}

fn is_no_keys_error(error: &impl Display) -> bool {
    let message = error.to_string().to_lowercase();
    message.contains("no keys") || message.contains("no messages")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{DeckrMessage, DeviceRef, HardwareMessageBody};

    #[test]
    fn subject_and_headers_match_python_nats_adapter() {
        let message = DeckrMessage::hardware_input_to(
            "mirabox-main",
            "manager-session",
            "deck",
            "controller:main",
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
            },
        )
        .unwrap();

        assert_eq!(
            subject_for(&message).unwrap(),
            "deckr.lane.hardware_messages.hardware_manager.mirabox-main"
        );
        let headers = headers_for(&message);
        assert_eq!(
            headers.get("Deckr-Message-Id").unwrap().as_str(),
            message.message_id
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
    fn subjects_reject_non_core_sender_families() {
        let mut message = DeckrMessage::hardware_input(
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
        message.sender = "driver:mirabox-main".to_string();

        assert!(subject_for(&message).is_err());
    }
}
