//! Storage data types corresponding to TypeScript storage types.
//!
//! These types mirror the TypeScript definitions in `packages/cojson/src/storage/types.ts`
//! to ensure compatibility between Rust and TypeScript storage implementations.

use std::collections::HashMap;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// A raw CoValue ID (e.g., "co_z123abc...")
pub type RawCoID = String;

/// A session ID (e.g., "co_z123abc_session@device")
pub type SessionID = String;

/// A peer ID for sync tracking
pub type PeerID = String;

/// A cryptographic signature
pub type Signature = String;

/// A signer ID
pub type SignerID = String;

/// A key ID for encryption
pub type KeyID = String;

/// Encrypted data wrapper (phantom type for documentation purposes)
pub type Encrypted = String;

/// Stringified JSON (phantom type for documentation purposes)
pub type Stringified = String;

/// JSON value (generic)
#[cfg(feature = "serde")]
pub type JsonValue = serde_json::Value;

#[cfg(not(feature = "serde"))]
pub type JsonValue = String;

/// JSON object
#[cfg(feature = "serde")]
pub type JsonObject = serde_json::Map<String, serde_json::Value>;

#[cfg(not(feature = "serde"))]
pub type JsonObject = String;

/// Uniqueness value for CoValue headers.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum Uniqueness {
    String(String),
    Bool(bool),
    Null,
    Object(HashMap<String, String>),
}

impl Default for Uniqueness {
    fn default() -> Self {
        Uniqueness::Null
    }
}

/// Ruleset definition for CoValue permissions.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(tag = "type"))]
pub enum RulesetDef {
    #[cfg_attr(feature = "serde", serde(rename = "unsafeAllowAll"))]
    UnsafeAllowAll,
    #[cfg_attr(feature = "serde", serde(rename = "ownedByGroup"))]
    OwnedByGroup { group: RawCoID },
    #[cfg_attr(feature = "serde", serde(rename = "group"))]
    Group,
    #[cfg_attr(feature = "serde", serde(rename = "account"))]
    Account,
}

impl Default for RulesetDef {
    fn default() -> Self {
        RulesetDef::UnsafeAllowAll
    }
}

/// CoValue header containing metadata about a CoValue.
///
/// Corresponds to TypeScript `CoValueHeader` in `verifiedState.ts`.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
pub struct CoValueHeader {
    /// The type of the CoValue (e.g., "comap", "colist", "costream", etc.)
    #[cfg_attr(feature = "serde", serde(rename = "type"))]
    pub covalue_type: String,

    /// The ruleset defining permissions for this CoValue
    pub ruleset: RulesetDef,

    /// Optional metadata
    #[cfg_attr(feature = "serde", serde(default))]
    pub meta: Option<JsonObject>,

    /// Uniqueness value for deduplication
    #[cfg_attr(feature = "serde", serde(default))]
    pub uniqueness: Uniqueness,

    /// Creation timestamp (ISO 8601 format starting with "2")
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_at: Option<String>,
}

impl Default for CoValueHeader {
    fn default() -> Self {
        Self {
            covalue_type: "comap".to_string(),
            ruleset: RulesetDef::default(),
            meta: None,
            uniqueness: Uniqueness::default(),
            created_at: None,
        }
    }
}

/// A private transaction with encrypted changes.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
pub struct PrivateTransaction {
    pub privacy: PrivateTransactionPrivacy,
    pub made_at: i64,
    pub key_used: KeyID,
    pub encrypted_changes: String,
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub meta: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PrivateTransactionPrivacy {
    #[cfg_attr(feature = "serde", serde(rename = "private"))]
    Private,
}

/// A trusting transaction with plaintext changes.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
pub struct TrustingTransaction {
    pub privacy: TrustingTransactionPrivacy,
    pub made_at: i64,
    pub changes: String,
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub meta: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TrustingTransactionPrivacy {
    #[cfg_attr(feature = "serde", serde(rename = "trusting"))]
    Trusting,
}

/// A transaction, either private (encrypted) or trusting (plaintext).
///
/// Corresponds to TypeScript `Transaction` in `verifiedState.ts`.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum Transaction {
    Private(PrivateTransaction),
    Trusting(TrustingTransaction),
}

impl Transaction {
    /// Returns true if this is a private (encrypted) transaction.
    pub fn is_private(&self) -> bool {
        matches!(self, Transaction::Private(_))
    }

    /// Returns true if this is a trusting (plaintext) transaction.
    pub fn is_trusting(&self) -> bool {
        matches!(self, Transaction::Trusting(_))
    }

    /// Returns the timestamp when the transaction was made.
    pub fn made_at(&self) -> i64 {
        match self {
            Transaction::Private(tx) => tx.made_at,
            Transaction::Trusting(tx) => tx.made_at,
        }
    }
}

/// A row in the CoValues table.
///
/// Corresponds to TypeScript `CoValueRow` in `storage/types.ts`.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CoValueRow {
    /// The CoValue ID
    pub id: RawCoID,
    /// The CoValue header
    pub header: CoValueHeader,
}

/// A stored CoValue row with its database row ID.
///
/// Corresponds to TypeScript `StoredCoValueRow` in `storage/types.ts`.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StoredCoValueRow {
    /// Database row ID
    pub row_id: u64,
    /// The CoValue ID
    pub id: RawCoID,
    /// The CoValue header
    pub header: CoValueHeader,
}

impl From<(u64, CoValueRow)> for StoredCoValueRow {
    fn from((row_id, row): (u64, CoValueRow)) -> Self {
        Self {
            row_id,
            id: row.id,
            header: row.header,
        }
    }
}

/// A row in the Sessions table.
///
/// Corresponds to TypeScript `SessionRow` in `storage/types.ts`.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SessionRow {
    /// Foreign key to CoValues table
    pub covalue: u64,
    /// The session ID
    pub session_id: SessionID,
    /// Index of the last transaction
    pub last_idx: u64,
    /// Signature of the last transaction
    pub last_signature: Signature,
    /// Bytes since the last signature checkpoint (optional)
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub bytes_since_last_signature: Option<u64>,
}

/// A stored session row with its database row ID.
///
/// Corresponds to TypeScript `StoredSessionRow` in `storage/types.ts`.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StoredSessionRow {
    /// Database row ID
    pub row_id: u64,
    /// Foreign key to CoValues table
    pub covalue: u64,
    /// The session ID
    pub session_id: SessionID,
    /// Index of the last transaction
    pub last_idx: u64,
    /// Signature of the last transaction
    pub last_signature: Signature,
    /// Bytes since the last signature checkpoint (optional)
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub bytes_since_last_signature: Option<u64>,
}

impl From<(u64, SessionRow)> for StoredSessionRow {
    fn from((row_id, row): (u64, SessionRow)) -> Self {
        Self {
            row_id,
            covalue: row.covalue,
            session_id: row.session_id,
            last_idx: row.last_idx,
            last_signature: row.last_signature,
            bytes_since_last_signature: row.bytes_since_last_signature,
        }
    }
}

/// A row in the Transactions table.
///
/// Corresponds to TypeScript `TransactionRow` in `storage/types.ts`.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TransactionRow {
    /// Foreign key to Sessions table (named `ses` in TypeScript)
    pub ses: u64,
    /// Transaction index within the session
    pub idx: u64,
    /// The transaction data
    pub tx: Transaction,
}

/// A row in the SignaturesAfter table.
///
/// Corresponds to TypeScript `SignatureAfterRow` in `storage/types.ts`.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SignatureAfterRow {
    /// Foreign key to Sessions table
    pub ses: u64,
    /// Transaction index this signature covers
    pub idx: u64,
    /// The signature
    pub signature: Signature,
}

/// Known state of a CoValue's sessions.
///
/// Maps session IDs to transaction counts.
pub type KnownStateSessions = HashMap<SessionID, u64>;

/// Known state for a CoValue.
///
/// Corresponds to TypeScript `CoValueKnownState` in `knownState.ts`.
#[derive(Debug, Clone, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CoValueKnownState {
    /// The CoValue ID
    pub id: RawCoID,
    /// Whether the header is known
    pub header: bool,
    /// Session states (session ID -> transaction count)
    pub sessions: KnownStateSessions,
}

/// Deletion work queue status.
///
/// Corresponds to TypeScript `DeletedCoValueDeletionStatus` in `storage/types.ts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[repr(u8)]
pub enum DeletionStatus {
    /// Deletion is pending
    Pending = 0,
    /// Deletion is complete (tombstone preserved)
    Done = 1,
}

impl From<u8> for DeletionStatus {
    fn from(value: u8) -> Self {
        match value {
            0 => DeletionStatus::Pending,
            1 => DeletionStatus::Done,
            _ => DeletionStatus::Pending,
        }
    }
}

impl From<DeletionStatus> for u8 {
    fn from(status: DeletionStatus) -> Self {
        status as u8
    }
}

/// A sync state update for tracking which peers have synced a CoValue.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SyncStateUpdate {
    /// The CoValue ID
    pub id: RawCoID,
    /// The peer ID
    pub peer_id: PeerID,
    /// Whether the peer has synced
    pub synced: bool,
}

/// Session update data for adding or updating sessions.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SessionUpdate {
    /// The session row data
    pub session_update: SessionRow,
    /// Existing session row if this is an update
    pub session_row: Option<StoredSessionRow>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deletion_status_conversion() {
        assert_eq!(DeletionStatus::from(0), DeletionStatus::Pending);
        assert_eq!(DeletionStatus::from(1), DeletionStatus::Done);
        assert_eq!(DeletionStatus::from(99), DeletionStatus::Pending);

        assert_eq!(u8::from(DeletionStatus::Pending), 0);
        assert_eq!(u8::from(DeletionStatus::Done), 1);
    }

    #[test]
    fn test_transaction_helpers() {
        let private_tx = Transaction::Private(PrivateTransaction {
            privacy: PrivateTransactionPrivacy::Private,
            made_at: 1234567890,
            key_used: "key_123".to_string(),
            encrypted_changes: "encrypted_data".to_string(),
            meta: None,
        });

        let trusting_tx = Transaction::Trusting(TrustingTransaction {
            privacy: TrustingTransactionPrivacy::Trusting,
            made_at: 1234567891,
            changes: r#"[{"op":"set","path":["key"],"value":"val"}]"#.to_string(),
            meta: None,
        });

        assert!(private_tx.is_private());
        assert!(!private_tx.is_trusting());
        assert_eq!(private_tx.made_at(), 1234567890);

        assert!(!trusting_tx.is_private());
        assert!(trusting_tx.is_trusting());
        assert_eq!(trusting_tx.made_at(), 1234567891);
    }

    #[test]
    fn test_covalue_header_default() {
        let header = CoValueHeader::default();
        assert_eq!(header.covalue_type, "comap");
        assert_eq!(header.ruleset, RulesetDef::UnsafeAllowAll);
        assert!(header.meta.is_none());
        assert_eq!(header.uniqueness, Uniqueness::Null);
        assert!(header.created_at.is_none());
    }

    #[test]
    fn test_stored_row_conversions() {
        let covalue_row = CoValueRow {
            id: "co_z123".to_string(),
            header: CoValueHeader::default(),
        };

        let stored: StoredCoValueRow = (42, covalue_row.clone()).into();
        assert_eq!(stored.row_id, 42);
        assert_eq!(stored.id, "co_z123");

        let session_row = SessionRow {
            covalue: 42,
            session_id: "session_123".to_string(),
            last_idx: 5,
            last_signature: "sig_abc".to_string(),
            bytes_since_last_signature: Some(1024),
        };

        let stored_session: StoredSessionRow = (99, session_row.clone()).into();
        assert_eq!(stored_session.row_id, 99);
        assert_eq!(stored_session.covalue, 42);
        assert_eq!(stored_session.session_id, "session_123");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_covalue_header_serialization() {
        let header = CoValueHeader {
            covalue_type: "comap".to_string(),
            ruleset: RulesetDef::OwnedByGroup {
                group: "co_zGroup123".to_string(),
            },
            meta: None,
            uniqueness: Uniqueness::String("unique_123".to_string()),
            created_at: Some("2024-01-01T00:00:00Z".to_string()),
        };

        let json = serde_json::to_string(&header).unwrap();
        let parsed: CoValueHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(header, parsed);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_transaction_serialization() {
        let trusting = Transaction::Trusting(TrustingTransaction {
            privacy: TrustingTransactionPrivacy::Trusting,
            made_at: 1234567890,
            changes: r#"[]"#.to_string(),
            meta: None,
        });

        let json = serde_json::to_string(&trusting).unwrap();
        assert!(json.contains("trusting"));

        let parsed: Transaction = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_trusting());
    }
}
