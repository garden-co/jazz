//! SessionMap implementation - one instance per CoValue
//!
//! This module provides the `SessionMapImpl` struct which owns all session data
//! for a single CoValue, including the header, sessions, and known state tracking.

use crate::core::keys::{CoID, KeyID, KeySecret, Signature, SignerID, SignerSecret};
use crate::core::session_log::{SessionID, SessionLogInternal, Transaction, TransactionMode};
use crate::core::CoJsonCoreError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// SessionMap implementation - one instance per CoValue
/// Owns the header and all session data for a single CoValue
pub struct SessionMapImpl {
    co_id: CoID,
    header: CoValueHeader,
    sessions: HashMap<String, SessionLogInternal>,
    known_state: KnownState,
    known_state_with_streaming: Option<KnownState>,
    streaming_known_state: Option<KnownStateSessions>,
    is_deleted: bool,
}

// ============================================================================
// Header Types
// ============================================================================

/// Custom JSON value type with stable (sorted) object key ordering
/// This ensures serialization matches TypeScript's stableStringify
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<JsonValue>),
    Object(BTreeMap<String, JsonValue>), // Sorted keys!
}

/// Header matching TypeScript CoValueHeader
/// CRITICAL: Fields MUST be in alphabetical order to match stableStringify!
/// serde serializes struct fields in definition order.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CoValueHeader {
    // Fields in alphabetical order: createdAt, meta, ruleset, type, uniqueness
    #[serde(rename = "createdAt", skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    pub meta: Option<JsonValue>,
    pub ruleset: RulesetDef,
    #[serde(rename = "type")]
    pub co_type: String, // "comap" | "colist" | "costream" | "coplaintext"
    pub uniqueness: Uniqueness,
}

/// RulesetDef - NOT using serde(tag) because it puts tag first, not alphabetically
/// Instead, we manually include the "type" field in alphabetical position
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum RulesetDef {
    Group(RulesetGroup),
    OwnedByGroup(RulesetOwnedByGroup),
    UnsafeAllowAll(RulesetUnsafeAllowAll),
}

/// {"initialAdmin": "...", "type": "group"} - fields in alphabetical order
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RulesetGroup {
    #[serde(rename = "initialAdmin")]
    pub initial_admin: String,
    #[serde(rename = "type")]
    pub ruleset_type: RulesetGroupType,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum RulesetGroupType {
    #[serde(rename = "group")]
    Group,
}

/// {"group": "...", "type": "ownedByGroup"} - fields in alphabetical order
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RulesetOwnedByGroup {
    pub group: String,
    #[serde(rename = "type")]
    pub ruleset_type: RulesetOwnedByGroupType,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum RulesetOwnedByGroupType {
    #[serde(rename = "ownedByGroup")]
    OwnedByGroup,
}

/// {"type": "unsafeAllowAll"} - only has type field
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RulesetUnsafeAllowAll {
    #[serde(rename = "type")]
    pub ruleset_type: RulesetUnsafeAllowAllType,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum RulesetUnsafeAllowAllType {
    #[serde(rename = "unsafeAllowAll")]
    UnsafeAllowAll,
}

// Helper constructors for ergonomic RulesetDef creation
impl RulesetDef {
    pub fn group(initial_admin: impl Into<String>) -> Self {
        RulesetDef::Group(RulesetGroup {
            initial_admin: initial_admin.into(),
            ruleset_type: RulesetGroupType::Group,
        })
    }

    pub fn owned_by_group(group: impl Into<String>) -> Self {
        RulesetDef::OwnedByGroup(RulesetOwnedByGroup {
            group: group.into(),
            ruleset_type: RulesetOwnedByGroupType::OwnedByGroup,
        })
    }

    pub fn unsafe_allow_all() -> Self {
        RulesetDef::UnsafeAllowAll(RulesetUnsafeAllowAll {
            ruleset_type: RulesetUnsafeAllowAllType::UnsafeAllowAll,
        })
    }
}

/// Uniqueness type - Object variant uses BTreeMap for stable serialization
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Uniqueness {
    String(String),
    Bool(bool),
    Integer(i64),
    Null,
    Object(BTreeMap<String, String>), // BTreeMap for stable key ordering!
}

// ============================================================================
// Known State Types
// ============================================================================

/// KnownState - fields in alphabetical order, uses BTreeMap for sessions
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct KnownState {
    // Alphabetical order: header, id, sessions
    pub header: bool,
    pub id: String,
    pub sessions: BTreeMap<String, u32>, // BTreeMap for stable ordering!
}

/// KnownStateSessions - uses BTreeMap for stable serialization
pub type KnownStateSessions = BTreeMap<String, u32>;

// ============================================================================
// Error Types
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum SessionMapError {
    #[error("Session not found: {0}")]
    SessionNotFound(String),

    #[error("Invalid header JSON: {0}")]
    InvalidHeader(String),

    #[error("Cannot add to deleted CoValue: {0}")]
    DeletedCoValue(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Core error: {0}")]
    Core(#[from] CoJsonCoreError),
}

// ============================================================================
// SessionMapImpl Implementation
// ============================================================================

impl SessionMapImpl {
    /// Create a new SessionMap for a CoValue
    pub fn new(co_id: &str, header_json: &str) -> Result<Self, SessionMapError> {
        let header: CoValueHeader = serde_json::from_str(header_json)
            .map_err(|e| SessionMapError::InvalidHeader(e.to_string()))?;

        Ok(Self {
            co_id: CoID(co_id.to_string()),
            header,
            sessions: HashMap::new(),
            known_state: KnownState {
                header: true,
                id: co_id.to_string(),
                sessions: BTreeMap::new(),
            },
            known_state_with_streaming: None,
            streaming_known_state: None,
            is_deleted: false,
        })
    }

    // === Header ===

    /// Get the header as JSON
    pub fn get_header(&self) -> String {
        serde_json::to_string(&self.header).expect("header serialization should not fail")
    }

    // === Transaction Operations ===

    /// Add transactions to a session
    pub fn add_transactions(
        &mut self,
        session_id: &str,
        signer_id: Option<&str>,
        transactions_json: &str,
        signature: &str,
        skip_verify: bool,
    ) -> Result<(), SessionMapError> {
        if self.is_deleted && !is_delete_session_id(session_id) {
            return Err(SessionMapError::DeletedCoValue(self.co_id.0.clone()));
        }

        let transactions: Vec<Transaction> = serde_json::from_str(transactions_json)?;

        // Get or create session log
        let session_log = self.sessions.entry(session_id.to_string()).or_insert_with(|| {
            SessionLogInternal::new(
                self.co_id.clone(),
                SessionID(session_id.to_string()),
                signer_id.map(|s| SignerID(s.to_string())),
            )
        });

        // Add transactions to staging area
        for tx in &transactions {
            match tx {
                Transaction::Private(private_tx) => {
                    session_log.add_existing_private_transaction(
                        private_tx.encrypted_changes.value.clone(),
                        private_tx.key_used.0.clone(),
                        private_tx.made_at.as_u64().unwrap_or(0),
                        private_tx.meta.as_ref().map(|m| m.value.clone()),
                    )?;
                }
                Transaction::Trusting(trusting_tx) => {
                    session_log.add_existing_trusting_transaction(
                        trusting_tx.changes.clone(),
                        trusting_tx.made_at.as_u64().unwrap_or(0),
                        trusting_tx.meta.clone(),
                    )?;
                }
            }
        }

        // Commit transactions with signature verification
        let sig = Signature(signature.to_string());
        session_log.commit_transactions(&sig, skip_verify)?;

        // Update known state
        let tx_count = session_log.transactions_json().len() as u32;
        self.known_state
            .sessions
            .insert(session_id.to_string(), tx_count);

        // Check if streaming state is now satisfied
        if let Some(streaming) = &self.streaming_known_state {
            if is_known_state_subset_of(streaming, &self.known_state.sessions) {
                self.streaming_known_state = None;
                self.known_state_with_streaming = None;
            }
        }

        // Update known_state_with_streaming if present
        if let Some(ref mut ks_streaming) = self.known_state_with_streaming {
            ks_streaming
                .sessions
                .entry(session_id.to_string())
                .and_modify(|c| *c = (*c).max(tx_count))
                .or_insert(tx_count);
        }

        Ok(())
    }

    /// Create new private transaction (for local writes)
    /// Returns JSON: { signature: string, transaction: Transaction }
    pub fn make_new_private_transaction(
        &mut self,
        session_id: &str,
        signer_secret: &str,
        changes_json: &str,
        key_id: &str,
        key_secret: &str,
        meta_json: Option<&str>,
        made_at: u64,
    ) -> Result<String, SessionMapError> {
        if self.is_deleted {
            return Err(SessionMapError::DeletedCoValue(self.co_id.0.clone()));
        }

        // Get or create session log
        let session_log = self.sessions.entry(session_id.to_string()).or_insert_with(|| {
            SessionLogInternal::new(
                self.co_id.clone(),
                SessionID(session_id.to_string()),
                None, // signerID derived from secret
            )
        });

        // Add new transaction
        let (signature, transaction) = session_log.add_new_transaction(
            changes_json,
            TransactionMode::Private {
                key_id: KeyID(key_id.to_string()),
                key_secret: KeySecret(key_secret.to_string()),
            },
            &SignerSecret(signer_secret.to_string()),
            made_at,
            meta_json.map(|s| s.to_string()),
        )?;

        // Update known state
        let tx_count = session_log.transactions_json().len() as u32;
        self.known_state.sessions.insert(session_id.to_string(), tx_count);

        // Update known_state_with_streaming if present
        if let Some(ref mut ks_streaming) = self.known_state_with_streaming {
            ks_streaming.sessions.insert(session_id.to_string(), tx_count);
        }

        // Build result JSON
        let tx_json = serde_json::to_string(&transaction)?;
        let result = format!(
            r#"{{"signature":"{}","transaction":{}}}"#,
            signature.0, tx_json
        );

        Ok(result)
    }

    /// Create new trusting transaction (for local writes)
    /// Returns JSON: { signature: string, transaction: Transaction }
    pub fn make_new_trusting_transaction(
        &mut self,
        session_id: &str,
        signer_secret: &str,
        changes_json: &str,
        meta_json: Option<&str>,
        made_at: u64,
    ) -> Result<String, SessionMapError> {
        if self.is_deleted {
            return Err(SessionMapError::DeletedCoValue(self.co_id.0.clone()));
        }

        // Get or create session log
        let session_log = self.sessions.entry(session_id.to_string()).or_insert_with(|| {
            SessionLogInternal::new(
                self.co_id.clone(),
                SessionID(session_id.to_string()),
                None, // signerID derived from secret
            )
        });

        // Add new transaction
        let (signature, transaction) = session_log.add_new_transaction(
            changes_json,
            TransactionMode::Trusting,
            &SignerSecret(signer_secret.to_string()),
            made_at,
            meta_json.map(|s| s.to_string()),
        )?;

        // Update known state
        let tx_count = session_log.transactions_json().len() as u32;
        self.known_state.sessions.insert(session_id.to_string(), tx_count);

        // Update known_state_with_streaming if present
        if let Some(ref mut ks_streaming) = self.known_state_with_streaming {
            ks_streaming.sessions.insert(session_id.to_string(), tx_count);
        }

        // Build result JSON
        let tx_json = serde_json::to_string(&transaction)?;
        let result = format!(
            r#"{{"signature":"{}","transaction":{}}}"#,
            signature.0, tx_json
        );

        Ok(result)
    }

    // === Session Queries ===

    /// Get all session IDs
    pub fn get_session_ids(&self) -> Vec<String> {
        self.sessions.keys().cloned().collect()
    }

    /// Get transaction count for a session (None if session not found)
    pub fn get_transaction_count(&self, session_id: &str) -> Option<u32> {
        self.sessions
            .get(session_id)
            .map(|sl| sl.transactions_json().len() as u32)
    }

    /// Get single transaction by index
    pub fn get_transaction(&self, session_id: &str, tx_index: u32) -> Option<String> {
        self.sessions
            .get(session_id)
            .and_then(|sl| sl.transactions_json().get(tx_index as usize))
            .cloned()
    }

    /// Get transactions for a session from index
    pub fn get_session_transactions(&self, session_id: &str, from_index: u32) -> Option<String> {
        let session_log = self.sessions.get(session_id)?;
        let transactions = session_log.transactions_json();

        let slice: Vec<&str> = transactions
            .iter()
            .skip(from_index as usize)
            .map(|s| s.as_str())
            .collect();

        serde_json::to_string(&slice).ok()
    }

    /// Get last signature for a session
    pub fn get_last_signature(&self, session_id: &str) -> Option<String> {
        self.sessions
            .get(session_id)
            .and_then(|sl| sl.last_signature())
            .map(|s| s.0.clone())
    }

    /// Get signature after specific transaction index
    pub fn get_signature_after(&self, session_id: &str, tx_index: u32) -> Option<String> {
        self.sessions
            .get(session_id)
            .and_then(|sl| sl.get_signature_after(tx_index))
            .map(|s| s.to_string())
    }

    /// Get the last signature checkpoint index (max index in signatureAfter map, or -1 if no checkpoints)
    pub fn get_last_signature_checkpoint(&self, session_id: &str) -> Option<i32> {
        self.sessions
            .get(session_id)
            .map(|sl| sl.get_last_signature_checkpoint())
    }

    // === Known State ===

    /// Get the known state as JSON
    pub fn get_known_state(&self) -> String {
        serde_json::to_string(&self.known_state).expect("known_state serialization should not fail")
    }

    /// Get the known state with streaming as JSON
    pub fn get_known_state_with_streaming(&self) -> Option<String> {
        self.known_state_with_streaming
            .as_ref()
            .map(|ks| serde_json::to_string(ks).expect("known_state serialization should not fail"))
    }

    /// Set streaming known state
    pub fn set_streaming_known_state(
        &mut self,
        streaming_json: &str,
    ) -> Result<(), SessionMapError> {
        if self.is_deleted {
            return Ok(());
        }

        let streaming: KnownStateSessions = serde_json::from_str(streaming_json)?;

        // Check if streaming state is subset of current known state
        if is_known_state_subset_of(&streaming, &self.known_state.sessions) {
            return Ok(()); // Already have this data
        }

        // Get the actual streaming known state (what we don't have yet)
        let actual_streaming = get_known_state_to_send(&streaming, &self.known_state.sessions);

        // Update or create streaming_known_state
        if let Some(ref mut current) = self.streaming_known_state {
            combine_known_state_sessions(current, &actual_streaming);
        } else {
            self.streaming_known_state = Some(actual_streaming.clone());
        }

        // Update known_state_with_streaming
        if self.known_state_with_streaming.is_none() {
            self.known_state_with_streaming = Some(self.known_state.clone());
        }

        if let Some(ref mut ks_streaming) = self.known_state_with_streaming {
            combine_known_state_sessions(&mut ks_streaming.sessions, &actual_streaming);
        }

        Ok(())
    }

    // === Deletion ===

    /// Mark this CoValue as deleted
    pub fn mark_as_deleted(&mut self) {
        self.is_deleted = true;

        // Reset known state to only report delete sessions
        let mut new_known_state = KnownState {
            header: true,
            id: self.co_id.0.clone(),
            sessions: BTreeMap::new(),
        };

        // Only keep delete session counts in known state
        for (session_id, session_log) in &self.sessions {
            if is_delete_session_id(session_id) {
                new_known_state
                    .sessions
                    .insert(session_id.clone(), session_log.transactions_json().len() as u32);
            }
        }

        self.known_state = new_known_state;
        self.known_state_with_streaming = None;
        self.streaming_known_state = None;
    }

    /// Check if this CoValue is deleted
    pub fn is_deleted(&self) -> bool {
        self.is_deleted
    }

    // === Decryption ===

    /// Decrypt transaction changes
    pub fn decrypt_transaction(
        &self,
        session_id: &str,
        tx_index: u32,
        key_secret: &str,
    ) -> Result<Option<String>, SessionMapError> {
        let session_log = self
            .sessions
            .get(session_id)
            .ok_or_else(|| SessionMapError::SessionNotFound(session_id.to_string()))?;

        let decrypted = session_log
            .decrypt_next_transaction_changes_json(tx_index, KeySecret(key_secret.to_string()))?;
        Ok(Some(decrypted))
    }

    /// Decrypt transaction meta
    pub fn decrypt_transaction_meta(
        &self,
        session_id: &str,
        tx_index: u32,
        key_secret: &str,
    ) -> Result<Option<String>, SessionMapError> {
        let session_log = self
            .sessions
            .get(session_id)
            .ok_or_else(|| SessionMapError::SessionNotFound(session_id.to_string()))?;

        Ok(session_log
            .decrypt_next_transaction_meta_json(tx_index, KeySecret(key_secret.to_string()))?)
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Check if session ID is a delete session
fn is_delete_session_id(session_id: &str) -> bool {
    session_id.contains("_session_d") && session_id.ends_with('$')
}

/// Check if streaming state is a subset of current state
fn is_known_state_subset_of(streaming: &KnownStateSessions, current: &KnownStateSessions) -> bool {
    streaming.iter().all(|(session_id, &count)| {
        current
            .get(session_id)
            .map(|&current_count| count <= current_count)
            .unwrap_or(false)
    })
}

/// Get the known state to send (what the peer doesn't have)
fn get_known_state_to_send(
    streaming: &KnownStateSessions,
    current: &KnownStateSessions,
) -> KnownStateSessions {
    streaming
        .iter()
        .filter_map(|(session_id, &count)| {
            let current_count = current.get(session_id).copied().unwrap_or(0);
            if count > current_count {
                Some((session_id.clone(), count))
            } else {
                None
            }
        })
        .collect()
}

/// Combine known state sessions (max of each session)
fn combine_known_state_sessions(target: &mut KnownStateSessions, source: &KnownStateSessions) {
    for (session_id, &count) in source {
        target
            .entry(session_id.clone())
            .and_modify(|c| *c = (*c).max(count))
            .or_insert(count);
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_HEADER: &str =
        r#"{"meta":null,"ruleset":{"type":"unsafeAllowAll"},"type":"comap","uniqueness":"test"}"#;

    #[test]
    fn test_session_map_creation() {
        let session_map = SessionMapImpl::new("co_test", TEST_HEADER).unwrap();

        assert_eq!(session_map.co_id.0, "co_test");
        assert!(!session_map.is_deleted());
        assert!(session_map.get_session_ids().is_empty());
    }

    #[test]
    fn test_header_round_trip() {
        let session_map = SessionMapImpl::new("co_test", TEST_HEADER).unwrap();

        let header_json = session_map.get_header();
        // Parse back to verify
        let header: CoValueHeader = serde_json::from_str(&header_json).unwrap();
        assert_eq!(header.co_type, "comap");
    }

    #[test]
    fn test_known_state() {
        let session_map = SessionMapImpl::new("co_test", TEST_HEADER).unwrap();

        let known_state_json = session_map.get_known_state();
        let known_state: KnownState = serde_json::from_str(&known_state_json).unwrap();

        assert!(known_state.header);
        assert_eq!(known_state.id, "co_test");
        assert!(known_state.sessions.is_empty());
    }

    #[test]
    fn test_mark_as_deleted() {
        let mut session_map = SessionMapImpl::new("co_test", TEST_HEADER).unwrap();

        session_map.mark_as_deleted();
        assert!(session_map.is_deleted());
    }

    #[test]
    fn test_ruleset_serialization() {
        // Test unsafeAllowAll
        let ruleset = RulesetDef::unsafe_allow_all();
        let json = serde_json::to_string(&ruleset).unwrap();
        assert_eq!(json, r#"{"type":"unsafeAllowAll"}"#);

        // Test group
        let ruleset = RulesetDef::group("co_admin123");
        let json = serde_json::to_string(&ruleset).unwrap();
        // Fields should be in alphabetical order: initialAdmin, type
        assert_eq!(json, r#"{"initialAdmin":"co_admin123","type":"group"}"#);

        // Test ownedByGroup
        let ruleset = RulesetDef::owned_by_group("co_group123");
        let json = serde_json::to_string(&ruleset).unwrap();
        // Fields should be in alphabetical order: group, type
        assert_eq!(json, r#"{"group":"co_group123","type":"ownedByGroup"}"#);
    }

    #[test]
    fn test_header_serialization_alphabetical_order() {
        let header = CoValueHeader {
            created_at: None,
            meta: None,
            ruleset: RulesetDef::unsafe_allow_all(),
            co_type: "comap".to_string(),
            uniqueness: Uniqueness::String("test".to_string()),
        };

        let json = serde_json::to_string(&header).unwrap();
        // Fields should be in alphabetical order: meta, ruleset, type, uniqueness
        // (createdAt is skipped because it's None)
        assert_eq!(
            json,
            r#"{"meta":null,"ruleset":{"type":"unsafeAllowAll"},"type":"comap","uniqueness":"test"}"#
        );
    }

    #[test]
    fn test_is_delete_session_id() {
        assert!(is_delete_session_id("co_test_session_dabc123$"));
        assert!(!is_delete_session_id("co_test_session_zabc123"));
        assert!(!is_delete_session_id("co_test_session_dabc123")); // missing $
    }
}
