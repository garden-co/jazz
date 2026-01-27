# Rust SessionMap - Design Document

## Overview

This document describes the architecture for moving `SessionMap` and `CoValueHeader` data structures to Rust. The focus is on **data structure ownership** - Rust owns the data per-CoValue, TypeScript becomes a thin orchestration layer.

### Design Goals

1. **Data ownership in Rust**: `SessionMap` (including header) lives entirely in Rust, one instance per CoValue
2. **TypeScript as orchestrator**: TS handles cross-CoValue logic (permissions, key lookup)
3. **Minimal FFI surface**: JSON serialization for simplicity
4. **Consistent pattern**: Follows the existing `SessionLogImpl` pattern - each TS wrapper owns a Rust impl
5. **Active by default**: Rust implementation is the only implementation, no feature flags
6. **No performance regression**: Benchmarks must show improvement or parity

### Scope

| Component | Location | Reason |
|-----------|----------|--------|
| `SessionMap` data storage | **Rust** | Core data structure |
| `CoValueHeader` storage | **Rust** | Core data structure |
| Transaction storage | **Rust** | Part of SessionMap |
| Known state tracking | **Rust** | Part of SessionMap |
| Signature verification | **Rust** | Unified in SessionLog |
| Decryption (crypto) | **Rust** | Unified in SessionLog |
| `newContentSince()` | **TypeScript** (for now) | Deferred - uses Rust data via FFI |
| `determineValidTransactions()` | **TypeScript** | Requires group state + cross-CoValue |
| Key lookup (`getReadKey`) | **TypeScript** | Requires group hierarchy traversal |
| `atTime()` filtering | **TypeScript** | Complex time-travel views |

## Architecture

### High-Level Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    TypeScript Layer                                      │
├─────────────────────────────────────────────────────────────────────────┤
│  CoValueCore                                                             │
│  ├── verified: VerifiedState (thin wrapper)                             │
│  ├── determineValidTransactions()  ← STAYS (needs group access)         │
│  ├── decryptTransaction()  ← Orchestrates, key from groups              │
│  └── newContentSince()  ← STAYS for now, reads from Rust               │
├─────────────────────────────────────────────────────────────────────────┤
│  VerifiedState (thin wrapper, per CoValue)                               │
│  ├── sessions: SessionMap (owns Rust impl)                              │
│  ├── header → delegates to SessionMap.impl                              │
│  └── newContentSince() → reads data from Rust, builds messages          │
├─────────────────────────────────────────────────────────────────────────┤
│  SessionMap.ts (thin wrapper, per CoValue)                               │
│  ├── impl: SessionMapImpl (Rust-backed)                                 │
│  ├── All methods delegate to impl                                       │
│  └── Converts between TS types and Rust JSON                            │
├─────────────────────────────────────────────────────────────────────────┤
│  RawGroup (permission logic) - UNCHANGED                                 │
│  ├── roleOfInternal()  ← Recursive parent traversal                     │
│  ├── getReadKey()  ← Cross-CoValue key lookup                           │
│  └── atTime()  ← Time-travel filtering                                  │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ FFI Boundary (NAPI/WASM/UniFFI)
                                    │ JSON serialization
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Rust Layer (DATA OWNERSHIP, per CoValue)              │
├─────────────────────────────────────────────────────────────────────────┤
│  SessionMapImpl (one instance per CoValue)                               │
│  ├── co_id: String                                                      │
│  ├── header: CoValueHeader                                              │
│  ├── sessions: HashMap<SessionID, SessionLogInternal>                   │
│  ├── known_state: KnownState                                            │
│  ├── known_state_with_streaming: Option<KnownState>                     │
│  ├── is_deleted: bool                                                   │
│  └── Methods: add_transaction, get_session, mark_deleted, etc.          │
├─────────────────────────────────────────────────────────────────────────┤
│  SessionLogInternal (per session) - EXTENDED with new fields            │
│  ├── [EXISTING] public_key, hasher, nonce_generator, crypto_cache       │
│  ├── [EXISTING] transactions_json: Vec<String>                          │
│  ├── [EXISTING] last_signature, pending_transactions                    │
│  ├── [NEW] signature_after: HashMap<u32, String>                        │
│  ├── [NEW] tx_size_since_last_inbetween_signature: usize                │
│  └── [NEW] transaction_count(), get_signature_after(), etc.             │
└─────────────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

#### CryptoProvider Pattern

`SessionMapImpl` follows the same pattern as `SessionLogImpl` - created by the platform-specific crypto provider:

```
CryptoProvider (abstract)
├── createSessionLog() → SessionLogImpl     // Existing
├── createSessionMap() → SessionMapImpl     // NEW (one per CoValue)
│
├── NapiCrypto (Node.js)
│   └── creates SessionMapImpl from cojson-core-napi
├── WasmCrypto (Browser)
│   └── creates SessionMapImpl from cojson-core-wasm
└── RNCrypto (React Native)
    └── creates SessionMapImpl from cojson-core-rn
```

#### TypeScript: `SessionMapImpl` Interface (NEW)

```typescript
// packages/cojson/src/crypto/crypto.ts

export interface SessionMapImpl {
  // Header (stored in this SessionMap)
  getHeader(): string;
  
  // Transaction operations
  addTransactions(
    sessionId: string,
    signerId: string | null,
    transactionsJson: string,
    signature: string,
    skipVerify: boolean,
  ): void;
  
  makeNewPrivateTransaction(
    sessionId: string,
    signerId: string,
    changesJson: string,
    keyId: string,
    keySecret: string,
    metaJson: string | null,
    madeAt: number,
  ): string;  // Returns { signature, transaction } JSON
  
  makeNewTrustingTransaction(
    sessionId: string,
    signerId: string,
    changesJson: string,
    metaJson: string | null,
    madeAt: number,
  ): string;  // Returns { signature, transaction } JSON
  
  // Session queries - return undefined if session not found
  getSessionIds(): string[];
  getTransactionCount(sessionId: string): number | undefined;
  getTransaction(sessionId: string, txIndex: number): string | undefined;
  getSessionTransactions(sessionId: string, fromIndex: number): string | undefined;
  getLastSignature(sessionId: string): string | undefined;
  getSignatureAfter(sessionId: string, txIndex: number): string | undefined;
  getLastSignatureCheckpoint(sessionId: string): number | undefined;
  
  // Known state
  getKnownState(): string;
  getKnownStateWithStreaming(): string | undefined;
  setStreamingKnownState(streamingJson: string): void;
  
  // Deletion
  markAsDeleted(): void;
  isDeleted(): boolean;
  
  // Decryption (throws if session not found)
  decryptTransaction(sessionId: string, txIndex: number, keySecret: string): string | undefined;
  decryptTransactionMeta(sessionId: string, txIndex: number, keySecret: string): string | undefined;
  
  // Lifecycle
  free(): void;
}
```

#### TypeScript: `CryptoProvider` (MODIFIED)

```typescript
// packages/cojson/src/crypto/crypto.ts

export abstract class CryptoProvider {
  // ... existing methods ...
  
  abstract createSessionLog(
    coID: RawCoID,
    sessionID: SessionID,
    signerID?: SignerID,
  ): SessionLogImpl;
  
  // NEW: Create a SessionMap for a CoValue (one per CoValue)
  abstract createSessionMap(
    coID: RawCoID,
    headerJson: string,
  ): SessionMapImpl;
}
```

#### Platform Implementations

```typescript
// packages/cojson/src/crypto/NapiCrypto.ts
import { SessionMap as NativeSessionMap } from "cojson-core-napi";

export class NapiCrypto extends CryptoProvider {
  createSessionMap(coID: RawCoID, headerJson: string): SessionMapImpl {
    return new NativeSessionMap(coID, headerJson);
  }
}
```

```typescript
// packages/cojson/src/crypto/WasmCrypto.ts
import { SessionMap as WasmSessionMap } from "cojson-core-wasm";

export class WasmCrypto extends CryptoProvider {
  createSessionMap(coID: RawCoID, headerJson: string): SessionMapImpl {
    return new WasmSessionMap(coID, headerJson);
  }
}
```

```typescript
// packages/cojson/src/crypto/RNCrypto.ts
import { SessionMap as RNSessionMap } from "cojson-core-rn";

export class RNCrypto extends CryptoProvider {
  createSessionMap(coID: RawCoID, headerJson: string): SessionMapImpl {
    return new RNSessionMap(coID, headerJson);
  }
}
```

#### Rust: `SessionMapImpl`
- **Per-CoValue instance**: One Rust object per CoValue, owned by TypeScript wrapper
- **Header storage**: Stores the CoValueHeader
- **Session storage**: HashMap of `SessionID` → `SessionLogInternal`
- **Known state tracking**: Tracks transaction counts per session
- **Streaming state**: Handles `knownStateWithStreaming` for partial loads
- **Deletion handling**: `markAsDeleted`, filters non-delete sessions
- **Transaction operations**: `addTransactions`, `makeNewPrivateTransaction`, `makeNewTrustingTransaction`
- **Decryption**: `decryptTransaction`, `decryptTransactionMeta` (delegated to SessionLogInternal)
- **Exposed via**: NAPI, WASM, UniFFI bindings

#### Rust: `SessionLogInternal`
- **Transaction storage**: Vector of transactions
- **Signature tracking**: `lastSignature`, `signatureAfter` map
- **Size tracking**: For chunking in sync messages
- **Crypto operations**: Handles verification and decryption directly (unified)

#### TypeScript: `SessionMap.ts` (Thin Wrapper)
- Owns a `SessionMapImpl` instance from `CryptoProvider`
- Delegates all operations to impl
- Converts between TypeScript branded types and JSON strings
- Maintains API compatibility with existing code

#### TypeScript: `VerifiedState`
- Owns a `SessionMap` which owns the Rust impl
- `header` getter delegates to `SessionMap.impl.getHeader()`
- `newContentSince()` stays here for now - reads data from Rust, builds messages

#### TypeScript: `LocalNode` (UNCHANGED)
- No changes needed - does not hold a registry
- Creates `VerifiedState` instances which create their own `SessionMap` with Rust impl

## Data Models

### Stable Serialization (Critical)

TypeScript uses `stableStringify` which **sorts object keys alphabetically** before JSON serialization. This is critical because:

1. **CoValue IDs are computed from header hashes**: `idforHeader(header)` → `crypto.shortHash(header)` → `stableStringify(header)`
2. **The hash must be deterministic** - different key ordering = different hash = different ID

**Rust serialization requirements to match `stableStringify`:**

1. **Struct fields MUST be defined in alphabetical order** - serde serializes fields in definition order
2. **Use `BTreeMap` for any map/object** - ensures keys are serialized in sorted order
3. **DO NOT use `#[serde(tag = "...")]`** - it puts the tag field FIRST, not alphabetically
   - Instead, use `#[serde(untagged)]` with separate structs that have the tag as a regular field in alphabetical position
   - Example: `{"group":"co_z...","type":"ownedByGroup"}` requires `group` field before `type` field in struct definition

Structures requiring `BTreeMap`:
- `Uniqueness::Object` - part of header, gets hashed
- `KnownState.sessions` - serialized for sync messages
- `KnownStateSessions` - serialized for sync messages
- Any `meta` objects that get round-tripped

**For `serde_json::Value` (used in `meta` field):**
- `serde_json::Value` with `preserve_order` only keeps insertion order, NOT sorted order
- **Solution**: Define a custom `JsonValue` type using `BTreeMap<String, JsonValue>` for objects
- The `meta` field is part of the header and gets hashed, so key order matters

```rust
/// Custom JSON value type with stable (sorted) object key ordering
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<JsonValue>),
    Object(BTreeMap<String, JsonValue>),  // Sorted keys!
}
```

Internal storage structures (`SessionMapImpl.sessions`) can use `HashMap` since they're never hashed, only used for lookups.

### Rust Data Structures

```rust
// crates/cojson-core/src/core/session_map.rs

use std::collections::{HashMap, BTreeMap};

/// SessionMap implementation - one instance per CoValue
/// Owns the header and all session data for a single CoValue
pub struct SessionMapImpl {
    co_id: String,
    header: CoValueHeader,
    sessions: HashMap<String, SessionLogInternal>,
    known_state: KnownState,
    known_state_with_streaming: Option<KnownState>,
    streaming_known_state: Option<KnownStateSessions>,
    is_deleted: bool,
}

/// Header matching TypeScript CoValueHeader
/// CRITICAL: Fields MUST be in alphabetical order to match stableStringify!
/// serde serializes struct fields in definition order.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CoValueHeader {
    // Fields in alphabetical order: createdAt, meta, ruleset, type, uniqueness
    #[serde(rename = "createdAt", skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    pub meta: Option<JsonValue>,  // Custom JsonValue with BTreeMap for stable ordering
    pub ruleset: RulesetDef,
    #[serde(rename = "type")]
    pub co_type: String,  // "comap" | "colist" | "costream" | "coplaintext"
    pub uniqueness: Uniqueness,
}

/// RulesetDef - NOT using serde(tag) because it puts tag first, not alphabetically
/// Instead, we manually include the "type" field in alphabetical position
/// 
/// IMPORTANT: serde(tag = "type") would produce {"type":"group","initialAdmin":"..."}
/// but stableStringify needs {"initialAdmin":"...","type":"group"} (alphabetical)
///
/// Solution: Use untagged enum with explicit type fields in alphabetical order
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RulesetDef {
    Group(RulesetGroup),
    OwnedByGroup(RulesetOwnedByGroup),
    UnsafeAllowAll(RulesetUnsafeAllowAll),
}

/// {"initialAdmin": "...", "type": "group"} - fields in alphabetical order
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RulesetGroup {
    #[serde(rename = "initialAdmin")]
    pub initial_admin: String,
    #[serde(rename = "type")]
    pub ruleset_type: RulesetGroupType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RulesetGroupType {
    #[serde(rename = "group")]
    Group,
}

/// {"group": "...", "type": "ownedByGroup"} - fields in alphabetical order
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RulesetOwnedByGroup {
    pub group: String,
    #[serde(rename = "type")]
    pub ruleset_type: RulesetOwnedByGroupType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RulesetOwnedByGroupType {
    #[serde(rename = "ownedByGroup")]
    OwnedByGroup,
}

/// {"type": "unsafeAllowAll"} - only has type field
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RulesetUnsafeAllowAll {
    #[serde(rename = "type")]
    pub ruleset_type: RulesetUnsafeAllowAllType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Uniqueness {
    String(String),
    Bool(bool),
    Integer(i64),
    Null,
    Object(BTreeMap<String, String>),  // BTreeMap for stable key ordering!
}

/// SessionLogInternal - EXTENDS existing SessionLogInternal from cojson-core/src/core/session_log.rs
/// 
/// The existing SessionLogInternal already stores:
///   - public_key: Option<VerifyingKey>     (for verification)
///   - hasher: blake3::Hasher               (for signature chaining)
///   - transactions_json: Vec<String>       (transaction storage)
///   - last_signature: Option<Signature>    (last signature)
///   - nonce_generator: NonceGenerator      (for crypto)
///   - crypto_cache: CryptoCache            (for decryption)
///   - pending_transactions: Vec<String>    (staging area)
///
/// We ADD these fields to SessionLogInternal:
///   - signature_after: HashMap<u32, String>           (in-between signatures)
///   - tx_size_since_last_inbetween_signature: usize   (size tracking)
///
/// This removes the data duplication in TypeScript where SessionLog stored
/// transactions/lastSignature separately from impl (SessionLogInternal).

// In crates/cojson-core/src/core/session_log.rs - EXTEND existing struct:
#[derive(Clone)]
pub struct SessionLogInternal {
    // === EXISTING fields (already implemented) ===
    public_key: Option<VerifyingKey>,
    hasher: blake3::Hasher,
    transactions_json: Vec<String>,
    last_signature: Option<Signature>,
    nonce_generator: NonceGenerator,
    crypto_cache: CryptoCache,
    pending_transactions: Vec<String>,
    
    // === NEW fields to add ===
    signature_after: HashMap<u32, String>,           // In-between signatures
    tx_size_since_last_inbetween_signature: usize,   // Size tracking for chunking
}

// The WASM/NAPI/RN wrappers remain thin wrappers around SessionLogInternal:
// (No changes needed to wrapper structure)

impl SessionLogInternal {
    // === EXISTING methods (already implemented) ===
    // - new(co_id, session_id, signer_id)
    // - add_existing_private_transaction(...)
    // - add_existing_trusting_transaction(...)
    // - commit_transactions(signature, skip_verify)
    // - add_new_private_transaction(...)
    // - add_new_trusting_transaction(...)
    // - decrypt_next_transaction_changes_json(tx_index, key_secret)
    // - decrypt_next_transaction_meta_json(tx_index, key_secret)
    // - clone()
    // - free()
    
    // === NEW methods to add ===
    
    /// Get transaction count
    pub fn transaction_count(&self) -> usize {
        self.transactions_json.len()
    }
    
    /// Get transaction at index (as JSON string)
    pub fn get_transaction(&self, tx_index: usize) -> Option<&str> {
        self.transactions_json.get(tx_index).map(|s| s.as_str())
    }
    
    /// Get last signature
    pub fn get_last_signature(&self) -> Option<&str> {
        self.last_signature.as_ref().map(|s| s.0.as_str())
    }
    
    /// Get signature after specific transaction index
    pub fn get_signature_after(&self, tx_index: u32) -> Option<&str> {
        self.signature_after.get(&tx_index).map(|s| s.as_str())
    }
    
    /// Get the last signature checkpoint index (max index in signature_after, or -1)
    pub fn get_last_signature_checkpoint(&self) -> i32 {
        self.signature_after.keys()
            .max()
            .map(|&idx| idx as i32)
            .unwrap_or(-1)
    }
    
    /// Record an in-between signature after committing transactions
    /// Called when tx_size_since_last_inbetween_signature exceeds threshold
    pub fn record_inbetween_signature(&mut self, tx_index: u32, signature: String) {
        self.signature_after.insert(tx_index, signature);
        self.tx_size_since_last_inbetween_signature = 0;
    }
    
    /// Update size tracking after adding transactions
    pub fn add_to_size_tracking(&mut self, size: usize) {
        self.tx_size_since_last_inbetween_signature += size;
    }
    
    /// Check if we need an in-between signature
    pub fn needs_inbetween_signature(&self) -> bool {
        self.tx_size_since_last_inbetween_signature > 100_000  // 100KB threshold
    }
}

/// KnownState - fields in alphabetical order, uses BTreeMap for sessions
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KnownState {
    // Alphabetical order: header, id, sessions
    pub header: bool,
    pub id: String,
    pub sessions: BTreeMap<String, u32>,  // BTreeMap for stable ordering!
}

/// KnownStateSessions - uses BTreeMap for stable serialization
pub type KnownStateSessions = BTreeMap<String, u32>;

/// Transaction types matching TypeScript
/// NOT using serde(tag) because it puts tag first, not alphabetically
/// 
/// IMPORTANT: serde(tag = "privacy") would produce {"privacy":"private","encryptedChanges":"..."}
/// but stableStringify needs {"encryptedChanges":"...","privacy":"private"} (alphabetical)
///
/// Solution: Use untagged enum with explicit privacy fields in alphabetical order
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Transaction {
    Private(PrivateTransaction),
    Trusting(TrustingTransaction),
}

/// Private transaction - fields in ALPHABETICAL order:
/// encryptedChanges, keyUsed, madeAt, meta, privacy
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrivateTransaction {
    #[serde(rename = "encryptedChanges")]
    pub encrypted_changes: String,
    #[serde(rename = "keyUsed")]
    pub key_used: String,
    #[serde(rename = "madeAt")]
    pub made_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<String>,
    pub privacy: PrivatePrivacy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PrivatePrivacy {
    #[serde(rename = "private")]
    Private,
}

/// Trusting transaction - fields in ALPHABETICAL order:
/// changes, madeAt, meta, privacy
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrustingTransaction {
    pub changes: String,
    #[serde(rename = "madeAt")]
    pub made_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<String>,
    pub privacy: TrustingPrivacy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TrustingPrivacy {
    #[serde(rename = "trusting")]
    Trusting,
}

// Helper constructors for ergonomic Transaction creation
impl Transaction {
    pub fn private(
        encrypted_changes: impl Into<String>,
        key_used: impl Into<String>,
        made_at: u64,
        meta: Option<String>,
    ) -> Self {
        Transaction::Private(PrivateTransaction {
            encrypted_changes: encrypted_changes.into(),
            key_used: key_used.into(),
            made_at,
            meta,
            privacy: PrivatePrivacy::Private,
        })
    }
    
    pub fn trusting(
        changes: impl Into<String>,
        made_at: u64,
        meta: Option<String>,
    ) -> Self {
        Transaction::Trusting(TrustingTransaction {
            changes: changes.into(),
            made_at,
            meta,
            privacy: TrustingPrivacy::Trusting,
        })
    }
}
```

### TypeScript Types (Reference - from verifiedState.ts)

```typescript
// These types define the contract - Rust must serialize to match

export type CoValueHeader = {
  type: AnyRawCoValue["type"];
  ruleset: RulesetDef;
  meta: JsonObject | null;
} & CoValueUniqueness;

export type CoValueUniqueness = {
  uniqueness: Uniqueness;
  createdAt?: `2${string}` | null;
};

export type Uniqueness =
  | string
  | boolean
  | null
  | undefined
  | { [key: string]: string };

export type PrivateTransaction = {
  privacy: "private";
  madeAt: number;
  keyUsed: KeyID;
  encryptedChanges: Encrypted<JsonValue[], { in: RawCoID; tx: TransactionID }>;
  meta?: Encrypted<JsonObject, { in: RawCoID; tx: TransactionID }>;
};

export type TrustingTransaction = {
  privacy: "trusting";
  madeAt: number;
  changes: Stringified<JsonValue[]>;
  meta?: Stringified<JsonObject>;
};

export type Transaction = PrivateTransaction | TrustingTransaction;
```

## API Design

### Rust Public API (`SessionMapImpl`)

```rust
impl SessionMapImpl {
    // === Constructor ===
    
    /// Create a new SessionMap for a CoValue
    pub fn new(co_id: &str, header_json: &str) -> Result<Self, SessionMapError>;
    
    // === Header ===
    
    /// Get the header (always present after construction)
    pub fn get_header(&self) -> String;
    
    // === Transaction Operations ===
    
    /// Add transactions to a session
    pub fn add_transactions(
        &mut self,
        session_id: &str,
        signer_id: Option<&str>,
        transactions_json: &str,  // JSON array of Transaction
        signature: &str,
        skip_verify: bool,
    ) -> Result<(), SessionMapError>;
    
    /// Create new private transaction (for local writes)
    pub fn make_new_private_transaction(
        &mut self,
        session_id: &str,
        signer_id: &str,
        changes_json: &str,
        key_id: &str,
        key_secret: &str,
        meta_json: Option<&str>,
        made_at: u64,
    ) -> Result<String, SessionMapError>;  // Returns { signature, transaction } JSON
    
    /// Create new trusting transaction (for local writes)
    pub fn make_new_trusting_transaction(
        &mut self,
        session_id: &str,
        signer_id: &str,
        changes_json: &str,
        meta_json: Option<&str>,
        made_at: u64,
    ) -> Result<String, SessionMapError>;  // Returns { signature, transaction } JSON
    
    // === Session Queries ===
    // Return Option to indicate "not found" - bindings convert to undefined
    
    /// Get all session IDs
    pub fn get_session_ids(&self) -> Vec<String>;
    
    /// Get transaction count for a session (None if session not found)
    pub fn get_transaction_count(&self, session_id: &str) -> Option<u32>;
    
    /// Get single transaction by index
    pub fn get_transaction(&self, session_id: &str, tx_index: u32) -> Option<String>;
    
    /// Get transactions for a session from index (for newContentSince iteration)
    pub fn get_session_transactions(&self, session_id: &str, from_index: u32) -> Option<String>;
    
    /// Get last signature for a session
    pub fn get_last_signature(&self, session_id: &str) -> Option<String>;
    
    /// Get signature after specific transaction index
    pub fn get_signature_after(&self, session_id: &str, tx_index: u32) -> Option<String>;
    
    /// Get the last signature checkpoint index (max index in signatureAfter map, or -1 if no checkpoints)
    pub fn get_last_signature_checkpoint(&self, session_id: &str) -> Option<i32>;
    
    // === Known State ===
    
    pub fn get_known_state(&self) -> String;
    pub fn get_known_state_with_streaming(&self) -> Option<String>;
    pub fn set_streaming_known_state(&mut self, streaming_json: &str) -> Result<(), SessionMapError>;
    
    // === Deletion ===
    
    pub fn mark_as_deleted(&mut self);
    pub fn is_deleted(&self) -> bool;
    
    // === Decryption (key provided by TypeScript from group lookup) ===
    
    pub fn decrypt_transaction(
        &self,
        session_id: &str,
        tx_index: u32,
        key_secret: &str,
    ) -> Result<Option<String>, SessionMapError>;  // Returns decrypted changes JSON
    
    pub fn decrypt_transaction_meta(
        &self,
        session_id: &str,
        tx_index: u32,
        key_secret: &str,
    ) -> Result<Option<String>, SessionMapError>;  // Returns decrypted meta JSON
    
    // === Lifecycle ===
    
    pub fn free(self);
}
```

### Rust Implementation Examples

```rust
// crates/cojson-core/src/core/session_map.rs

impl SessionMapImpl {
    /// Create a new SessionMap for a CoValue
    pub fn new(co_id: &str, header_json: &str) -> Result<Self, SessionMapError> {
        let header: CoValueHeader = serde_json::from_str(header_json)?;
        
        Ok(Self {
            co_id: co_id.to_string(),
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
    
    pub fn get_header(&self) -> String {
        serde_json::to_string(&self.header).expect("header serialization should not fail")
    }
    
    // === Transaction Operations ===
    
    pub fn add_transactions(
        &mut self,
        session_id: &str,
        signer_id: Option<&str>,
        transactions_json: &str,
        signature: &str,
        skip_verify: bool,
    ) -> Result<(), SessionMapError> {
        if self.is_deleted && !is_delete_session_id(session_id) {
            return Err(SessionMapError::DeletedCoValue(self.co_id.clone()));
        }
        
        let transactions: Vec<Transaction> = serde_json::from_str(transactions_json)?;
        
        // Get or create session log
        let session_log = self.sessions
            .entry(session_id.to_string())
            .or_insert_with(|| SessionLogInternal::new(&self.co_id, session_id, signer_id));
        
        // Add transactions to staging area
        for tx in &transactions {
            match tx {
                Transaction::Private(PrivateTransaction { encrypted_changes, key_used, made_at, meta, .. }) => {
                    session_log.add_existing_private_transaction(
                        encrypted_changes, key_used, *made_at, meta.as_deref()
                    );
                }
                Transaction::Trusting(TrustingTransaction { changes, made_at, meta, .. }) => {
                    session_log.add_existing_trusting_transaction(
                        changes, *made_at, meta.as_deref()
                    );
                }
            }
        }
        
        // Commit transactions with signature verification
        session_log.commit_transactions(signature, skip_verify)?;
        
        // Track transaction size for in-between signatures
        let tx_size: usize = transactions.iter().map(|tx| {
            match tx {
                Transaction::Private(PrivateTransaction { encrypted_changes, .. }) => encrypted_changes.len(),
                Transaction::Trusting(TrustingTransaction { changes, .. }) => changes.len(),
            }
        }).sum();
        session_log.add_to_size_tracking(tx_size);
        
        // Update known state
        let tx_count = session_log.transaction_count() as u32;
        self.known_state.sessions.insert(session_id.to_string(), tx_count);
        
        // Check if we need an in-between signature
        if session_log.needs_inbetween_signature() {
            let idx = (session_log.transaction_count() - 1) as u32;
            session_log.record_inbetween_signature(idx, signature.to_string());
        }
        
        Ok(())
    }
    
    // === Session Queries ===
    
    pub fn get_session_ids(&self) -> Vec<String> {
        self.sessions.keys().cloned().collect()
    }
    
    pub fn get_transaction_count(&self, session_id: &str) -> Option<u32> {
        self.sessions.get(session_id)
            .map(|sl| sl.transaction_count() as u32)
    }
    
    pub fn get_transaction(&self, session_id: &str, tx_index: u32) -> Option<String> {
        self.sessions.get(session_id)
            .and_then(|sl| sl.get_transaction(tx_index as usize))
            .map(|s| s.to_string())
    }
    
    pub fn get_session_transactions(&self, session_id: &str, from_index: u32) -> Option<String> {
        let session_log = self.sessions.get(session_id)?;
        
        let count = session_log.transaction_count();
        let transactions: Vec<&str> = (from_index as usize..count)
            .filter_map(|i| session_log.get_transaction(i))
            .collect();
        
        // Use serde_json for safe JSON array construction
        serde_json::to_string(&transactions).ok()
    }
    
    pub fn get_last_signature(&self, session_id: &str) -> Option<String> {
        self.sessions.get(session_id)
            .and_then(|sl| sl.get_last_signature())
            .map(|s| s.to_string())
    }
    
    pub fn get_signature_after(&self, session_id: &str, tx_index: u32) -> Option<String> {
        self.sessions.get(session_id)
            .and_then(|sl| sl.get_signature_after(tx_index))
            .map(|s| s.to_string())
    }
    
    pub fn get_last_signature_checkpoint(&self, session_id: &str) -> Option<i32> {
        self.sessions.get(session_id)
            .map(|sl| sl.get_last_signature_checkpoint())
    }
    
    // === Known State ===
    
    pub fn get_known_state(&self) -> String {
        serde_json::to_string(&self.known_state).expect("known_state serialization should not fail")
    }
    
    pub fn get_known_state_with_streaming(&self) -> Option<String> {
        self.known_state_with_streaming.as_ref()
            .map(|ks| serde_json::to_string(ks).expect("known_state serialization should not fail"))
    }
    
    pub fn set_streaming_known_state(&mut self, streaming_json: &str) -> Result<(), SessionMapError> {
        if self.is_deleted {
            return Ok(());
        }
        
        let streaming: KnownStateSessions = serde_json::from_str(streaming_json)?;
        
        // Check if streaming state is subset of current known state
        let is_subset = streaming.iter().all(|(session_id, &count)| {
            self.known_state.sessions
                .get(session_id)
                .map(|&current| count <= current)
                .unwrap_or(false)
        });
        
        if is_subset {
            return Ok(());  // Already have this data
        }
        
        self.streaming_known_state = Some(streaming.clone());
        
        // Update known_state_with_streaming
        let mut combined = self.known_state.clone();
        for (session_id, count) in streaming {
            combined.sessions
                .entry(session_id)
                .and_modify(|c| *c = (*c).max(count))
                .or_insert(count);
        }
        self.known_state_with_streaming = Some(combined);
        
        Ok(())
    }
    
    // === Deletion ===
    
    pub fn mark_as_deleted(&mut self) {
        self.is_deleted = true;
        
        // Reset known state to only report delete sessions
        let mut new_known_state = KnownState {
            header: true,
            id: self.co_id.clone(),
            sessions: BTreeMap::new(),
        };
        
        // Only keep delete session counts in known state
        for (session_id, session_log) in &self.sessions {
            if is_delete_session_id(session_id) {
                new_known_state.sessions.insert(
                    session_id.clone(),
                    session_log.transaction_count() as u32,
                );
            }
        }
        
        self.known_state = new_known_state;
        self.known_state_with_streaming = None;
        self.streaming_known_state = None;
    }
    
    pub fn is_deleted(&self) -> bool {
        self.is_deleted
    }
    
    // === Decryption ===
    
    pub fn decrypt_transaction(
        &self,
        session_id: &str,
        tx_index: u32,
        key_secret: &str,
    ) -> Result<Option<String>, SessionMapError> {
        let session_log = self.sessions.get(session_id)
            .ok_or_else(|| SessionMapError::SessionNotFound(session_id.to_string()))?;
        
        let decrypted = session_log.decrypt_next_transaction_changes_json(tx_index as usize, key_secret);
        Ok(Some(decrypted))
    }
    
    pub fn decrypt_transaction_meta(
        &self,
        session_id: &str,
        tx_index: u32,
        key_secret: &str,
    ) -> Result<Option<String>, SessionMapError> {
        let session_log = self.sessions.get(session_id)
            .ok_or_else(|| SessionMapError::SessionNotFound(session_id.to_string()))?;
        
        Ok(session_log.decrypt_next_transaction_meta_json(tx_index as usize, key_secret))
    }
    
    // === Lifecycle ===
    
    pub fn free(self) {
        // Drop self - Rust will clean up all owned data
    }
}

// Helper function
fn is_delete_session_id(session_id: &str) -> bool {
    session_id.contains("_session_d") && session_id.ends_with('$')
}
```

### TypeScript Thin Wrapper (`SessionMap.ts`)

```typescript
// packages/cojson/src/coValueCore/SessionMap.ts

export class SessionMap {
  constructor(
    private readonly id: RawCoID,
    private readonly impl: SessionMapImpl,  // Rust-backed, from CryptoProvider
  ) {}

  // Header access
  get header(): CoValueHeader {
    return JSON.parse(this.impl.getHeader());
  }

  // Session accessors
  getTransactionCount(sessionID: SessionID): number | undefined {
    return this.impl.getTransactionCount(sessionID);
  }
  
  getTransaction(sessionID: SessionID, txIndex: number): Transaction | undefined {
    const json = this.impl.getTransaction(sessionID, txIndex);
    return json ? JSON.parse(json) : undefined;
  }
  
  getTransactions(sessionID: SessionID, fromIndex: number = 0): Transaction[] | undefined {
    const json = this.impl.getSessionTransactions(sessionID, fromIndex);
    return json ? JSON.parse(json) : undefined;
  }
  
  getLastSignature(sessionID: SessionID): Signature | undefined {
    return this.impl.getLastSignature(sessionID) as Signature | undefined;
  }
  
  getSignatureAfter(sessionID: SessionID, txIndex: number): Signature | undefined {
    return this.impl.getSignatureAfter(sessionID, txIndex) as Signature | undefined;
  }
  
  getLastSignatureCheckpoint(sessionID: SessionID): number | undefined {
    return this.impl.getLastSignatureCheckpoint(sessionID) ?? undefined;
  }

  addTransaction(
    sessionID: SessionID,
    signerID: SignerID | undefined,
    newTransactions: Transaction[],
    newSignature: Signature,
    skipVerify: boolean = false,
  ) {
    this.impl.addTransactions(
      sessionID,
      signerID ?? null,
      JSON.stringify(newTransactions),
      newSignature,
      skipVerify,
    );
  }

  makeNewPrivateTransaction(
    sessionID: SessionID,
    signerAgent: ControlledAccountOrAgent,
    changes: JsonValue[],
    keyID: KeyID,
    keySecret: KeySecret,
    meta: JsonObject | undefined,
    madeAt: number,
  ): { signature: Signature; transaction: Transaction } {
    const resultJson = this.impl.makeNewPrivateTransaction(
      sessionID,
      signerAgent.currentSignerID(),
      JSON.stringify(changes),
      keyID,
      keySecret,
      meta ? JSON.stringify(meta) : null,
      madeAt,
    );
    return JSON.parse(resultJson);
  }

  makeNewTrustingTransaction(
    sessionID: SessionID,
    signerAgent: ControlledAccountOrAgent,
    changes: JsonValue[],
    meta: JsonObject | undefined,
    madeAt: number,
  ): { signature: Signature; transaction: Transaction } {
    const resultJson = this.impl.makeNewTrustingTransaction(
      sessionID,
      signerAgent.currentSignerID(),
      JSON.stringify(changes),
      meta ? JSON.stringify(meta) : null,
      madeAt,
    );
    return JSON.parse(resultJson);
  }

  get knownState(): CoValueKnownState {
    return JSON.parse(this.impl.getKnownState());
  }

  get knownStateWithStreaming(): CoValueKnownState | undefined {
    const json = this.impl.getKnownStateWithStreaming();
    return json ? JSON.parse(json) : undefined;
  }

  setStreamingKnownState(streamingKnownState: KnownStateSessions) {
    this.impl.setStreamingKnownState(JSON.stringify(streamingKnownState));
  }

  markAsDeleted() {
    this.impl.markAsDeleted();
  }

  get isDeleted(): boolean {
    return this.impl.isDeleted();
  }

  decryptTransaction(
    sessionID: SessionID,
    txIndex: number,
    keySecret: KeySecret,
  ): JsonValue[] | undefined {
    const json = this.impl.decryptTransaction(sessionID, txIndex, keySecret);
    return json ? JSON.parse(json) : undefined;
  }

  decryptTransactionMeta(
    sessionID: SessionID,
    txIndex: number,
    keySecret: KeySecret,
  ): JsonObject | undefined {
    const json = this.impl.decryptTransactionMeta(sessionID, txIndex, keySecret);
    return json ? JSON.parse(json) : undefined;
  }

  // Iterator support for newContentSince
  getSessionIds(): SessionID[] {
    return this.impl.getSessionIds() as SessionID[];
  }

  get size(): number {
    return this.impl.getSessionIds().length;
  }

  // Lifecycle - call when VerifiedState is no longer needed
  free() {
    this.impl.free();
  }
}
```

### TypeScript: `VerifiedState` (MODIFIED)

```typescript
// packages/cojson/src/coValueCore/verifiedState.ts

export class VerifiedState {
  readonly id: RawCoID;
  readonly sessions: SessionMap;
  
  // Header is fetched from SessionMap (which owns the Rust impl)
  get header(): CoValueHeader {
    return this.sessions.header;
  }
  
  constructor(
    id: RawCoID,
    crypto: CryptoProvider,
    header: CoValueHeader,
  ) {
    this.id = id;
    
    // Create SessionMap with Rust impl (header is stored in Rust)
    const impl = crypto.createSessionMap(id, JSON.stringify(header));
    this.sessions = new SessionMap(id, impl);
  }
  
  // Delegates to SessionMap (calculated in Rust)
  getLastSignatureCheckpoint(sessionID: SessionID): number | undefined {
    return this.sessions.getLastSignatureCheckpoint(sessionID);
  }
  
  // newContentSince stays in TypeScript, reads from Rust via SessionMap
  newContentSince(knownState: CoValueKnownState | undefined): NewContentMessage[] | undefined {
    // Uses this.sessions.getSessionIds(), getTransactions(), getLastSignature(), etc.
    // Implementation stays largely the same, just uses Rust-backed data
  }
  
  // Lifecycle - call when done with this CoValue
  free() {
    this.sessions.free();
  }
}

// Example usage changes in CoValueCore:
// Before: this.verified.sessions.get(sessionID)?.transactions.length || 0
// After:  this.verified.sessions.getTransactionCount(sessionID)

// Before: this.verified?.sessions.get(txID.sessionID)?.transactions[txIndex]
// After:  this.verified?.sessions.getTransaction(txID.sessionID, txIndex)
```

## FFI Data Passing Strategy

**Approach: JSON Serialization**

All data crossing the FFI boundary uses JSON:

```typescript
// TypeScript side
sessionMap.impl.addTransactions(sessionId, signerId, JSON.stringify(transactions), signature, false);
```

```rust
// Rust side
let transactions: Vec<Transaction> = serde_json::from_str(&transactions_json)?;
```

**Why JSON:**
- Simple to implement and debug
- Works consistently across all platforms (NAPI, WASM, UniFFI)
- Matches existing Rust patterns
- Easy to inspect during development

**Future Optimization (if needed):**
- Native NAPI objects for hot paths
- `Buffer`/`TypedArray` for binary data
- Shared memory for WASM

## Implementation Plan

**Note**: Rust is the default and only implementation. No feature flags, no dual-write, no gradual rollout.

### Phase 1: Rust SessionMapImpl Implementation
1. Implement `SessionMapImpl` struct in `cojson-core/src/core/session_map.rs`
2. Extend `SessionLogInternal` with new fields (signature_after, tx_size tracking)
3. Add unit tests in Rust

### Phase 2: FFI Bindings
1. Add NAPI bindings for `SessionMapImpl`
2. Add WASM bindings
3. Add React Native (UniFFI) bindings
4. Test each platform independently

### Phase 3: TypeScript Integration
1. Add `createSessionMap()` to `CryptoProvider`
2. Replace `SessionMap.ts` with thin wrapper around `SessionMapImpl`
3. Update `VerifiedState` to create `SessionMap` with Rust impl
4. Remove old TypeScript SessionMap/SessionLog data storage
5. Verify all existing tests pass

### Phase 4: newContentSince (Deferred)
1. Keep `newContentSince` in TypeScript
2. It reads data from Rust via the thin wrapper
3. Future optimization: move algorithm to Rust if needed

## What Stays in TypeScript

These stay in TypeScript due to cross-CoValue dependencies:

- **`determineValidTransactions()`** - needs group state, time-travel, parent traversal
- **`getReadKey()`** - traverses group hierarchy, loads other CoValues
- **`roleOfInternal()`** - recursive parent group lookup
- **`atTime()` filtering** - complex time-travel views
- **`newContentSince()`** - deferred, currently uses Rust data via FFI

## Error Handling

### Rust Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum SessionMapError {
    #[error("Session not found: {0}")]
    SessionNotFound(String),
    
    #[error("Invalid transaction data: {0}")]
    InvalidTransaction(String),
    
    #[error("Signature verification failed: {0}")]
    SignatureVerification(String),
    
    #[error("Decryption failed: {0}")]
    Decryption(String),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    
    #[error("Cannot add to deleted CoValue: {0}")]
    DeletedCoValue(String),
}
```

## Testing Strategy

### Unit Tests (Rust)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    const TEST_HEADER: &str = r#"{"type":"comap","ruleset":{"type":"unsafeAllowAll"},"meta":null,"uniqueness":"test"}"#;
    
    #[test]
    fn test_session_map_add_transactions() {
        let mut session_map = SessionMapImpl::new("co_test", TEST_HEADER).unwrap();
        
        session_map.add_transactions(
            "session_1",
            Some("signer_1"),
            r#"[{"privacy":"trusting","madeAt":1234,"changes":"[]"}]"#,
            "sig_abc",
            true,  // skip verify for test
        ).unwrap();
        
        let known_state: KnownState = serde_json::from_str(
            &session_map.get_known_state()
        ).unwrap();
        assert_eq!(known_state.sessions.get("session_1"), Some(&1));
    }
    
    #[test]
    fn test_mark_as_deleted() {
        let mut session_map = SessionMapImpl::new("co_test", TEST_HEADER).unwrap();
        
        session_map.mark_as_deleted();
        assert!(session_map.is_deleted());
    }
    
    #[test]
    fn test_header_round_trip() {
        let session_map = SessionMapImpl::new("co_test", TEST_HEADER).unwrap();
        
        let header_json = session_map.get_header();
        // Should match the input (key ordering may differ but content is same)
        let header: CoValueHeader = serde_json::from_str(&header_json).unwrap();
        assert_eq!(header.co_type, "comap");
    }
}
```

### Integration Tests (TypeScript)

```typescript
describe('SessionMap (Rust-backed)', () => {
  it('should store and retrieve transactions', () => {
    const impl = crypto.createSessionMap(coId, JSON.stringify(header));
    const sessionMap = new SessionMap(coId, impl);
    
    sessionMap.addTransaction(
      sessionId,
      signerId,
      [{ privacy: 'trusting', madeAt: Date.now(), changes: '[]' }],
      signature,
    );
    
    expect(sessionMap.getTransactionCount(sessionId)).toBe(1);
  });
  
  it('should track known state correctly', () => {
    const impl = crypto.createSessionMap(coId, JSON.stringify(header));
    const sessionMap = new SessionMap(coId, impl);
    // ... add transactions ...
    
    expect(sessionMap.knownState.sessions[sessionId]).toBe(1);
  });
  
  it('should provide header access', () => {
    const impl = crypto.createSessionMap(coId, JSON.stringify(header));
    const sessionMap = new SessionMap(coId, impl);
    
    expect(sessionMap.header.type).toBe('comap');
  });
});
```

