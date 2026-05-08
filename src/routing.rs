use std::collections::{HashMap, HashSet};

use deckr_core::DeviceClaim;

#[derive(Debug, Default)]
pub struct RoutingState {
    claims: HashMap<String, DeviceClaim>,
    controller_presence_sessions: HashMap<String, String>,
    unroutable_devices: HashSet<String>,
}

impl RoutingState {
    pub fn claim_recipient(&self, device_id: &str) -> Option<ClaimRecipient> {
        let claim = self.claims.get(device_id)?;
        claim_recipient(claim, &self.controller_presence_sessions)
    }

    pub fn reconcile_snapshot(
        &mut self,
        next_claims: HashMap<String, DeviceClaim>,
        next_controller_sessions: HashMap<String, String>,
        invalid_claim_devices: HashSet<String>,
    ) -> HashSet<String> {
        let devices_to_reset = self.devices_to_reset_for_snapshot(
            &next_claims,
            &next_controller_sessions,
            &invalid_claim_devices,
        );
        self.claims = next_claims;
        self.controller_presence_sessions = next_controller_sessions;
        self.unroutable_devices = self
            .claims
            .iter()
            .filter_map(|(device_id, claim)| {
                if claim_recipient(claim, &self.controller_presence_sessions).is_none() {
                    Some(device_id.clone())
                } else {
                    None
                }
            })
            .collect();
        devices_to_reset
    }

    pub fn remove_device(&mut self, device_id: &str) {
        self.claims.remove(device_id);
        self.unroutable_devices.remove(device_id);
    }

    pub fn claim_device_ids(&self) -> Vec<String> {
        self.claims.keys().cloned().collect()
    }

    pub fn controller_endpoints(&self) -> Vec<String> {
        self.controller_presence_sessions.keys().cloned().collect()
    }

    fn devices_to_reset_for_snapshot(
        &self,
        next_claims: &HashMap<String, DeviceClaim>,
        next_controller_sessions: &HashMap<String, String>,
        invalid_claim_devices: &HashSet<String>,
    ) -> HashSet<String> {
        let mut devices_to_reset = invalid_claim_devices.clone();
        for (device_id, old_claim) in &self.claims {
            let Some(next_claim) = next_claims.get(device_id) else {
                devices_to_reset.insert(device_id.clone());
                continue;
            };
            if claim_route_identity(old_claim) != claim_route_identity(next_claim) {
                devices_to_reset.insert(device_id.clone());
                continue;
            }
            if claim_recipient(old_claim, &self.controller_presence_sessions).is_some()
                && claim_recipient(next_claim, next_controller_sessions).is_none()
            {
                devices_to_reset.insert(device_id.clone());
            }
        }

        for (device_id, next_claim) in next_claims {
            if !self.claims.contains_key(device_id)
                && claim_recipient(next_claim, next_controller_sessions).is_none()
            {
                devices_to_reset.insert(device_id.clone());
            }
        }
        devices_to_reset
    }
}

fn claim_route_identity(claim: &DeviceClaim) -> (String, &str) {
    (
        claim.claimed_by_endpoint.to_string(),
        &claim.claimed_by_session_id,
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimRecipient {
    pub endpoint: String,
    pub session_id: String,
}

fn claim_recipient(
    claim: &DeviceClaim,
    controller_presence_sessions: &HashMap<String, String>,
) -> Option<ClaimRecipient> {
    let endpoint = claim.claimed_by_endpoint.to_string();
    let session_id = controller_presence_sessions.get(&endpoint)?;
    if session_id == &claim.claimed_by_session_id {
        Some(ClaimRecipient {
            endpoint,
            session_id: session_id.clone(),
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use deckr_core::{EndpointAddress, STATE_TTL_SECONDS};

    fn claim(endpoint: &str, session: &str) -> DeviceClaim {
        DeviceClaim {
            claimed_by_endpoint: endpoint.parse::<EndpointAddress>().unwrap(),
            claimed_by_session_id: session.to_string(),
            timestamp: DateTime::parse_from_rfc3339("2026-04-29T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            ttl_seconds: STATE_TTL_SECONDS,
        }
    }

    #[test]
    fn claim_requires_matching_controller_presence() {
        let mut routing = RoutingState::default();
        routing.reconcile_snapshot(
            HashMap::from([("deck".to_string(), claim("controller:main", "s1"))]),
            HashMap::new(),
            HashSet::new(),
        );
        assert_eq!(routing.claim_recipient("deck"), None);

        routing.reconcile_snapshot(
            HashMap::from([("deck".to_string(), claim("controller:main", "s1"))]),
            HashMap::from([("controller:main".to_string(), "s1".to_string())]),
            HashSet::new(),
        );
        assert_eq!(
            routing.claim_recipient("deck"),
            Some(ClaimRecipient {
                endpoint: "controller:main".to_string(),
                session_id: "s1".to_string()
            })
        );
    }

    #[test]
    fn delete_and_session_mismatch_reset_devices() {
        let mut routing = RoutingState::default();
        routing.reconcile_snapshot(
            HashMap::from([("deck".to_string(), claim("controller:main", "s1"))]),
            HashMap::from([("controller:main".to_string(), "s1".to_string())]),
            HashSet::new(),
        );

        let reset = routing.reconcile_snapshot(HashMap::new(), HashMap::new(), HashSet::new());
        assert!(reset.contains("deck"));

        routing.reconcile_snapshot(
            HashMap::from([("deck".to_string(), claim("controller:main", "s1"))]),
            HashMap::from([("controller:main".to_string(), "s1".to_string())]),
            HashSet::new(),
        );
        let reset = routing.reconcile_snapshot(
            HashMap::from([("deck".to_string(), claim("controller:main", "s1"))]),
            HashMap::from([("controller:main".to_string(), "s2".to_string())]),
            HashSet::new(),
        );
        assert!(reset.contains("deck"));
        assert_eq!(routing.claim_recipient("deck"), None);
    }

    #[test]
    fn transferred_claim_resets_device() {
        let mut routing = RoutingState::default();
        routing.reconcile_snapshot(
            HashMap::from([("deck".to_string(), claim("controller:main", "s1"))]),
            HashMap::from([("controller:main".to_string(), "s1".to_string())]),
            HashSet::new(),
        );
        let reset = routing.reconcile_snapshot(
            HashMap::from([("deck".to_string(), claim("controller:other", "s2"))]),
            HashMap::from([("controller:other".to_string(), "s2".to_string())]),
            HashSet::new(),
        );
        assert!(reset.contains("deck"));
        assert_eq!(
            routing.claim_recipient("deck"),
            Some(ClaimRecipient {
                endpoint: "controller:other".to_string(),
                session_id: "s2".to_string()
            })
        );
    }

    #[test]
    fn invalid_claim_resets_and_is_not_recorded() {
        let mut routing = RoutingState::default();
        let reset = routing.reconcile_snapshot(
            HashMap::new(),
            HashMap::new(),
            HashSet::from(["deck".to_string()]),
        );
        assert!(reset.contains("deck"));
        assert_eq!(routing.claim_recipient("deck"), None);
    }
}
