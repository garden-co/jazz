# Rust CoValue Register - Design Document

## Overview

This document describes the architecture for moving `SessionMap` and `CoValueHeader` data structures to Rust. The focus is on **data structure ownership** - Rust owns the data, TypeScript becomes a thin orchestration layer.

### Design Goals

1. **Data ownership in Rust**: `SessionMap` and `CoValueHeader` live entirely in Rust
2. **TypeScript as orchestrator**: TS handles cross-CoValue logic (permissions, key lookup)
3. **Minimal FFI surface**: Batch APIs, JSON serialization for simplicity
4. **Active by default**: Rust implementation is the only implementation, no feature flags
5. **No performance regression**: Benchmarks must show improvement or parity

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
│  VerifiedState (thin wrapper)                                            │
│  ├── header → delegates to Rust Register                                │
│  ├── sessions → delegates to Rust SessionMap                            │
│  └── newContentSince() → reads data from Rust, builds messages          │
├─────────────────────────────────────────────────────────────────────────┤
│  SessionMap.ts (thin wrapper)                                            │
│  ├── All methods delegate to Rust                                       │
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
│                    Rust Layer (DATA OWNERSHIP)                           │
├─────────────────────────────────────────────────────────────────────────┤
│  CoValueRegister (singleton per LocalNode)                              │
│  ├── headers: HashMap<RawCoID, CoValueHeader>                           │
│  ├── session_maps: HashMap<RawCoID, SessionMap>                         │
│  └── Lifecycle: create, free, freeAll                                   │
├─────────────────────────────────────────────────────────────────────────┤
│  SessionMap (owns all session data for a CoValue)                       │
│  ├── sessions: HashMap<SessionID, SessionLog>                           │
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

The Register follows the same pattern as `SessionLogImpl` - created by the platform-specific crypto provider:

```
CryptoProvider (abstract)
├── createSessionLog() → SessionLogImpl     // Existing
├── createRegister() → RegisterImpl         // NEW
│
├── NapiCrypto (Node.js)
│   └── creates RegisterAdapter wrapping cojson-core-napi Register
├── WasmCrypto (Browser)
│   └── creates RegisterAdapter wrapping cojson-core-wasm Register
└── RNCrypto (React Native)
    └── creates RegisterAdapter wrapping cojson-core-rn Register
```

#### TypeScript: `RegisterImpl` Interface (NEW)

```typescript
// packages/cojson/src/crypto/crypto.ts

export interface RegisterImpl {
  // Lifecycle
  free(id: string): boolean;
  freeAll(): void;
  size(): number;
  
  // Header operations (all can throw RegisterError)
  setHeader(id: string, headerJson: string): void;
  getHeader(id: string): string | undefined;  // undefined if not found
  hasHeader(id: string): boolean;
  
  // Session map operations
  createSessionMap(id: string): void;
  hasSessionMap(id: string): boolean;
  
  // Transaction operations
  addTransactions(
    id: string,
    sessionId: string,
    signerId: string | null,
    transactionsJson: string,
    signature: string,
    skipVerify: boolean,
  ): void;
  
  makeNewPrivateTransaction(
    id: string,
    sessionId: string,
    signerId: string,
    changesJson: string,
    keyId: string,
    keySecret: string,
    metaJson: string | null,
    madeAt: number,
  ): string;  // Returns { signature, transaction } JSON
  
  makeNewTrustingTransaction(
    id: string,
    sessionId: string,
    signerId: string,
    changesJson: string,
    metaJson: string | null,
    madeAt: number,
  ): string;  // Returns { signature, transaction } JSON
  
  // Session queries - return undefined if not found (no exceptions for missing data)
  getSessionIds(id: string): string[];  // throws if coValue not found
  getTransactionCount(id: string, sessionId: string): number | undefined;
  getTransaction(id: string, sessionId: string, txIndex: number): string | undefined;
  getSessionTransactions(id: string, sessionId: string, fromIndex: number): string | undefined;
  getLastSignature(id: string, sessionId: string): string | undefined;
  getSignatureAfter(id: string, sessionId: string, txIndex: number): string | undefined;
  getLastSignatureCheckpoint(id: string, sessionId: string): number | undefined;
  
  // Known state
  getKnownState(id: string): string | undefined;
  getKnownStateWithStreaming(id: string): string | undefined;
  setStreamingKnownState(id: string, streamingJson: string): void;
  
  // Deletion
  markAsDeleted(id: string): void;
  isDeleted(id: string): boolean | undefined;  // undefined if not found
  
  // Decryption (throws if coValue/session not found)
  decryptTransaction(id: string, sessionId: string, txIndex: number, keySecret: string): string | undefined;
  decryptTransactionMeta(id: string, sessionId: string, txIndex: number, keySecret: string): string | undefined;
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
  
  // NEW: Create the register (singleton per crypto instance)
  abstract createRegister(): RegisterImpl;
}
```

#### Platform Implementations

```typescript
// packages/cojson/src/crypto/NapiCrypto.ts
import { Register } from "cojson-core-napi";

export class NapiCrypto extends CryptoProvider {
  private _register: RegisterImpl | undefined;
  
  createRegister(): RegisterImpl {
    if (!this._register) {
      this._register = new RegisterAdapter(new Register());
    }
    return this._register;
  }
}

class RegisterAdapter implements RegisterImpl {
  constructor(private readonly register: Register) {}
  
  setHeader(id: string, headerJson: string): void {
    this.register.setHeader(id, headerJson);
  }
  
  // ... implement all methods delegating to native Register ...
}
```

```typescript
// packages/cojson/src/crypto/WasmCrypto.ts
import { Register } from "cojson-core-wasm";

export class WasmCrypto extends CryptoProvider {
  createRegister(): RegisterImpl {
    // Same pattern as NapiCrypto
  }
}
```

```typescript
// packages/cojson/src/crypto/RNCrypto.ts
import { Register } from "cojson-core-rn";

export class RNCrypto extends CryptoProvider {
  createRegister(): RegisterImpl {
    // Same pattern as NapiCrypto
  }
}
```

#### Rust: `CoValueRegister`
- **Storage**: Owns all `CoValueHeader` and `SessionMap` instances
- **Lifecycle**: `create`, `free`, `freeAll` operations
- **Header access**: `set_header`, `get_header`, `has_header`
- **Session map access**: `get_session_map`, `create_session_map`
- **Exposed via**: NAPI, WASM, UniFFI bindings

#### Rust: `SessionMap`
- **Session storage**: HashMap of `SessionID` → `SessionLog`
- **Known state tracking**: Tracks transaction counts per session
- **Streaming state**: Handles `knownStateWithStreaming` for partial loads
- **Deletion handling**: `markAsDeleted`, filters non-delete sessions
- **Transaction operations**: `addTransaction`, `makeNewPrivateTransaction`, `makeNewTrustingTransaction`
- **Decryption**: `decryptTransaction`, `decryptTransactionMeta` (handled by SessionLog)

#### Rust: `SessionLog`
- **Transaction storage**: Vector of transactions
- **Signature tracking**: `lastSignature`, `signatureAfter` map
- **Size tracking**: For chunking in sync messages
- **Crypto operations**: `SessionLog` handles verification and decryption directly (unified)

#### TypeScript: `SessionMap.ts` (Thin Wrapper)
- Gets `RegisterImpl` from `CryptoProvider`
- Delegates all operations to Register via the interface
- Converts between TypeScript branded types and JSON strings
- Maintains API compatibility with existing code

#### TypeScript: `VerifiedState`
- Thin wrapper holding reference to Rust data via Register
- `newContentSince()` stays here for now - reads data from Rust, builds messages
- Orchestrates operations but doesn't own data

#### TypeScript: `LocalNode` (MODIFIED)
- Holds single `RegisterImpl` instance from crypto provider
- Passes register to `VerifiedState` / `SessionMap` when creating CoValues

```typescript
// packages/cojson/src/localNode.ts

export class LocalNode {
  readonly crypto: CryptoProvider;
  readonly register: RegisterImpl;
  
  constructor(crypto: CryptoProvider, ...) {
    this.crypto = crypto;
    this.register = crypto.createRegister();  // Singleton for this node
  }
}
```

## Data Models

### Stable Serialization (Critical)

TypeScript uses `stableStringify` which **sorts object keys alphabetically** before JSON serialization. This is critical because:

1. **CoValue IDs are computed from header hashes**: `idforHeader(header)` → `crypto.shortHash(header)` → `stableStringify(header)`
2. **The hash must be deterministic** - different key ordering = different hash = different ID

**Rust serialization requirements to match `stableStringify`:**

1. **Struct fields MUST be defined in alphabetical order** - serde serializes fields in definition order
2. **Use `BTreeMap` for any map/object** - ensures keys are serialized in sorted order
3. **Tagged enums**: The tag field (e.g., `"type"`, `"privacy"`) participates in alphabetical ordering

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

Internal storage structures (`CoValueRegister.headers`, `SessionMap.sessions`) can use `HashMap` since they're never hashed, only used for lookups.

### Rust Data Structures

```rust
// crates/cojson-core/src/core/register.rs

use std::collections::{HashMap, BTreeMap};

/// The central registry for all CoValue data
/// Note: HashMap is fine here - this is internal storage, never serialized for hashing
pub struct CoValueRegister {
    headers: HashMap<String, CoValueHeader>,
    session_maps: HashMap<String, SessionMap>,
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

/// RulesetDef - tagged enum, fields within variants in alphabetical order
/// Note: serde(tag = "type") adds "type" field, other fields must be alphabetically ordered
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RulesetDef {
    #[serde(rename = "group")]
    Group { 
        // "initialAdmin" comes before "type" alphabetically
        #[serde(rename = "initialAdmin")]
        initial_admin: String 
    },
    #[serde(rename = "ownedByGroup")]
    OwnedByGroup { 
        // "group" comes before "type" alphabetically
        group: String 
    },
    #[serde(rename = "unsafeAllowAll")]
    UnsafeAllowAll,
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

/// Session map holding all sessions for a CoValue
/// Internal storage uses HashMap (never serialized for hashing)
pub struct SessionMap {
    co_id: String,
    sessions: HashMap<String, SessionLogInternal>,  // Uses EXTENDED SessionLogInternal
    known_state: KnownState,
    known_state_with_streaming: Option<KnownState>,
    streaming_known_state: Option<KnownStateSessions>,
    is_deleted: bool,
}

/// SessionLog - EXTENDS existing SessionLogInternal from cojson-core/src/core/session_log.rs
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
/// Fields within variants in alphabetical order (tag "privacy" is added by serde)
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "privacy")]
pub enum Transaction {
    #[serde(rename = "private")]
    Private {
        // Alphabetical: encryptedChanges, keyUsed, madeAt, meta, (privacy is tag)
        #[serde(rename = "encryptedChanges")]
        encrypted_changes: String,
        #[serde(rename = "keyUsed")]
        key_used: String,
        #[serde(rename = "madeAt")]
        made_at: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        meta: Option<String>,
    },
    #[serde(rename = "trusting")]
    Trusting {
        // Alphabetical: changes, madeAt, meta, (privacy is tag)
        changes: String,
        #[serde(rename = "madeAt")]
        made_at: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        meta: Option<String>,
    },
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

### Rust Public API (`CoValueRegister`)

```rust
impl CoValueRegister {
    // === Lifecycle ===
    
    pub fn new() -> Self;
    pub fn with_capacity(capacity: usize) -> Self;
    pub fn free(&mut self, id: &str) -> bool;
    pub fn free_all(&mut self);
    pub fn size(&self) -> usize;
    
    // === Header Operations ===
    
    pub fn set_header(&mut self, id: &str, header_json: &str) -> Result<(), RegisterError>;
    pub fn get_header(&self, id: &str) -> Result<Option<String>, RegisterError>;
    pub fn has_header(&self, id: &str) -> bool;
    
    // === Session Map Operations ===
    
    pub fn create_session_map(&mut self, id: &str) -> Result<(), RegisterError>;
    pub fn has_session_map(&self, id: &str) -> bool;
    
    // === Known State ===
    
    pub fn get_known_state(&self, id: &str) -> Result<Option<String>, RegisterError>;
    pub fn get_known_state_with_streaming(&self, id: &str) -> Result<Option<String>, RegisterError>;
    pub fn set_streaming_known_state(&mut self, id: &str, streaming_json: &str) -> Result<(), RegisterError>;
}
```

### Rust Public API (`SessionMap` via Register)

```rust
impl CoValueRegister {
    // === Transaction Operations ===
    
    /// Add transactions to a session
    pub fn add_transactions(
        &mut self,
        id: &str,
        session_id: &str,
        signer_id: Option<&str>,
        transactions_json: &str,  // JSON array of Transaction
        signature: &str,
        skip_verify: bool,
    ) -> Result<(), RegisterError>;
    
    /// Create new private transaction (for local writes)
    pub fn make_new_private_transaction(
        &mut self,
        id: &str,
        session_id: &str,
        signer_id: &str,
        changes_json: &str,
        key_id: &str,
        key_secret: &str,
        meta_json: Option<&str>,
        made_at: u64,
    ) -> Result<String, RegisterError>;  // Returns { signature, transaction } JSON
    
    /// Create new trusting transaction (for local writes)
    pub fn make_new_trusting_transaction(
        &mut self,
        id: &str,
        session_id: &str,
        signer_id: &str,
        changes_json: &str,
        meta_json: Option<&str>,
        made_at: u64,
    ) -> Result<String, RegisterError>;  // Returns { signature, transaction } JSON
    
    // === Session Queries ===
    // All return Option to indicate "not found" - bindings convert to undefined
    
    /// Get all session IDs
    pub fn get_session_ids(&self, id: &str) -> Result<Vec<String>, RegisterError>;
    
    /// Get transaction count for a session (None if session not found)
    pub fn get_transaction_count(&self, id: &str, session_id: &str) -> Option<u32>;
    
    /// Get single transaction by index
    pub fn get_transaction(&self, id: &str, session_id: &str, tx_index: u32) -> Result<Option<String>, RegisterError>;
    
    /// Get transactions for a session from index (for newContentSince iteration)
    pub fn get_session_transactions(
        &self,
        id: &str,
        session_id: &str,
        from_index: u32,
    ) -> Result<Option<String>, RegisterError>;
    
    /// Get last signature for a session
    pub fn get_last_signature(&self, id: &str, session_id: &str) -> Option<String>;
    
    /// Get signature after specific transaction index
    pub fn get_signature_after(&self, id: &str, session_id: &str, tx_index: u32) -> Option<String>;
    
    /// Get the last signature checkpoint index (max index in signatureAfter map, or -1 if no checkpoints)
    /// Returns None if session not found
    pub fn get_last_signature_checkpoint(&self, id: &str, session_id: &str) -> Option<i32>;
    
    // === Deletion ===
    
    pub fn mark_as_deleted(&mut self, id: &str) -> Result<(), RegisterError>;
    pub fn is_deleted(&self, id: &str) -> Option<bool>;  // None if not found
    
    // === Decryption (key provided by TypeScript from group lookup) ===
    
    pub fn decrypt_transaction(
        &self,
        id: &str,
        session_id: &str,
        tx_index: u32,
        key_secret: &str,
    ) -> Result<Option<String>, RegisterError>;  // Returns decrypted changes JSON
    
    pub fn decrypt_transaction_meta(
        &self,
        id: &str,
        session_id: &str,
        tx_index: u32,
        key_secret: &str,
    ) -> Result<Option<String>, RegisterError>;  // Returns decrypted meta JSON
}
```

### Rust Implementation Examples

```rust
// crates/cojson-core/src/core/register.rs

impl CoValueRegister {
    pub fn new() -> Self {
        Self {
            headers: HashMap::new(),
            session_maps: HashMap::new(),
        }
    }
    
    // === Header Operations ===
    
    pub fn set_header(&mut self, id: &str, header_json: &str) -> Result<(), RegisterError> {
        if self.headers.contains_key(id) {
            return Err(RegisterError::HeaderExists(id.to_string()));
        }
        let header: CoValueHeader = serde_json::from_str(header_json)?;
        self.headers.insert(id.to_string(), header);
        Ok(())
    }
    
    pub fn get_header(&self, id: &str) -> Result<Option<String>, RegisterError> {
        match self.headers.get(id) {
            Some(header) => {
                let json = serde_json::to_string(header)?;
                Ok(Some(json))
            }
            None => Ok(None),
        }
    }
    
    pub fn has_header(&self, id: &str) -> bool {
        self.headers.contains_key(id)
    }
    
    // === Session Map Operations ===
    
    pub fn create_session_map(&mut self, id: &str) -> Result<(), RegisterError> {
        if self.session_maps.contains_key(id) {
            return Err(RegisterError::SessionMapExists(id.to_string()));
        }
        let session_map = SessionMap {
            co_id: id.to_string(),
            sessions: HashMap::new(),
            known_state: KnownState {
                header: true,
                id: id.to_string(),
                sessions: BTreeMap::new(),
            },
            known_state_with_streaming: None,
            streaming_known_state: None,
            is_deleted: false,
        };
        self.session_maps.insert(id.to_string(), session_map);
        Ok(())
    }
    
    pub fn has_session_map(&self, id: &str) -> bool {
        self.session_maps.contains_key(id)
    }
    
    // === Transaction Operations ===
    
    pub fn add_transactions(
        &mut self,
        id: &str,
        session_id: &str,
        signer_id: Option<&str>,
        transactions_json: &str,
        signature: &str,
        skip_verify: bool,
    ) -> Result<(), RegisterError> {
        let session_map = self.session_maps.get_mut(id)
            .ok_or_else(|| RegisterError::NotFound(id.to_string()))?;
        
        if session_map.is_deleted && !is_delete_session_id(session_id) {
            return Err(RegisterError::DeletedCoValue(id.to_string()));
        }
        
        let transactions: Vec<Transaction> = serde_json::from_str(transactions_json)?;
        
        // Get or create session log (uses existing SessionLogInternal)
        let session_log = session_map.sessions
            .entry(session_id.to_string())
            .or_insert_with(|| SessionLogInternal::new(id, session_id, signer_id));
        
        // Add transactions to staging area
        for tx in &transactions {
            match tx {
                Transaction::Private { encrypted_changes, key_used, made_at, meta } => {
                    session_log.add_existing_private_transaction(
                        encrypted_changes, key_used, *made_at, meta.as_deref()
                    );
                }
                Transaction::Trusting { changes, made_at, meta } => {
                    session_log.add_existing_trusting_transaction(
                        changes, *made_at, meta.as_deref()
                    );
                }
            }
        }
        
        // Commit transactions with signature verification
        // (commit_transactions already stores transactions and last_signature internally)
        session_log.commit_transactions(signature, skip_verify)?;
        
        // Track transaction size for in-between signatures (NEW method)
        let tx_size: usize = transactions.iter().map(|tx| {
            match tx {
                Transaction::Private { encrypted_changes, .. } => encrypted_changes.len(),
                Transaction::Trusting { changes, .. } => changes.len(),
            }
        }).sum();
        session_log.add_to_size_tracking(tx_size);
        
        // Update known state
        let tx_count = session_log.transaction_count() as u32;
        session_map.known_state.sessions.insert(session_id.to_string(), tx_count);
        
        // Check if we need an in-between signature (NEW method)
        if session_log.needs_inbetween_signature() {
            let idx = (session_log.transaction_count() - 1) as u32;
            session_log.record_inbetween_signature(idx, signature.to_string());
        }
        
        Ok(())
    }
    
    // === Session Queries ===
    // All return Option to indicate "not found" - bindings convert to undefined
    
    pub fn get_session_ids(&self, id: &str) -> Result<Vec<String>, RegisterError> {
        let session_map = self.session_maps.get(id)
            .ok_or_else(|| RegisterError::NotFound(id.to_string()))?;
        
        Ok(session_map.sessions.keys().cloned().collect())
    }
    
    pub fn get_transaction_count(&self, id: &str, session_id: &str) -> Option<u32> {
        self.session_maps.get(id)
            .and_then(|sm| sm.sessions.get(session_id))
            .map(|sl| sl.transaction_count() as u32)  // Use NEW method
    }
    
    pub fn get_transaction(
        &self,
        id: &str,
        session_id: &str,
        tx_index: u32,
    ) -> Result<Option<String>, RegisterError> {
        let session_map = match self.session_maps.get(id) {
            Some(sm) => sm,
            None => return Ok(None),
        };
        
        let session_log = match session_map.sessions.get(session_id) {
            Some(sl) => sl,
            None => return Ok(None),
        };
        
        // Use NEW method - returns JSON string directly
        match session_log.get_transaction(tx_index as usize) {
            Some(tx_json) => Ok(Some(tx_json.to_string())),
            None => Ok(None),
        }
    }
    
    pub fn get_session_transactions(
        &self,
        id: &str,
        session_id: &str,
        from_index: u32,
    ) -> Result<Option<String>, RegisterError> {
        let session_map = match self.session_maps.get(id) {
            Some(sm) => sm,
            None => return Ok(None),
        };
        
        let session_log = match session_map.sessions.get(session_id) {
            Some(sl) => sl,
            None => return Ok(None),
        };
        
        // Collect transactions from index using get_transaction (NEW method)
        let count = session_log.transaction_count();
        let transactions: Vec<String> = (from_index as usize..count)
            .filter_map(|i| session_log.get_transaction(i).map(|s| s.to_string()))
            .collect();
        
        let json = format!("[{}]", transactions.join(","));
        Ok(Some(json))
    }
    
    pub fn get_last_signature(&self, id: &str, session_id: &str) -> Option<String> {
        self.session_maps.get(id)
            .and_then(|sm| sm.sessions.get(session_id))
            .and_then(|sl| sl.get_last_signature().map(|s| s.to_string()))  // Use NEW method
    }
    
    pub fn get_signature_after(&self, id: &str, session_id: &str, tx_index: u32) -> Option<String> {
        self.session_maps.get(id)
            .and_then(|sm| sm.sessions.get(session_id))
            .and_then(|sl| sl.get_signature_after(tx_index).map(|s| s.to_string()))  // Use NEW method
    }
    
    pub fn get_last_signature_checkpoint(&self, id: &str, session_id: &str) -> Option<i32> {
        let session_log = self.session_maps.get(id)
            .and_then(|sm| sm.sessions.get(session_id))?;
        
        Some(session_log.get_last_signature_checkpoint())  // Use NEW method
    }
    
    // === Known State ===
    
    pub fn get_known_state(&self, id: &str) -> Result<Option<String>, RegisterError> {
        match self.session_maps.get(id) {
            Some(session_map) => {
                let json = serde_json::to_string(&session_map.known_state)?;
                Ok(Some(json))
            }
            None => Ok(None),
        }
    }
    
    pub fn get_known_state_with_streaming(&self, id: &str) -> Result<Option<String>, RegisterError> {
        let session_map = match self.session_maps.get(id) {
            Some(sm) => sm,
            None => return Ok(None),
        };
        
        match &session_map.known_state_with_streaming {
            Some(ks) => {
                let json = serde_json::to_string(ks)?;
                Ok(Some(json))
            }
            None => Ok(None),
        }
    }
    
    pub fn set_streaming_known_state(
        &mut self,
        id: &str,
        streaming_json: &str,
    ) -> Result<(), RegisterError> {
        let session_map = self.session_maps.get_mut(id)
            .ok_or_else(|| RegisterError::NotFound(id.to_string()))?;
        
        let streaming: KnownStateSessions = serde_json::from_str(streaming_json)?;
        
        // Check if streaming state is subset of current known state
        let is_subset = streaming.iter().all(|(session_id, &count)| {
            session_map.known_state.sessions
                .get(session_id)
                .map(|&current| count <= current)
                .unwrap_or(false)
        });
        
        if is_subset {
            return Ok(());  // Already have this data
        }
        
        session_map.streaming_known_state = Some(streaming.clone());
        
        // Update known_state_with_streaming
        let mut combined = session_map.known_state.clone();
        for (session_id, count) in streaming {
            combined.sessions
                .entry(session_id)
                .and_modify(|c| *c = (*c).max(count))
                .or_insert(count);
        }
        session_map.known_state_with_streaming = Some(combined);
        
        Ok(())
    }
    
    // === Deletion ===
    
    pub fn mark_as_deleted(&mut self, id: &str) -> Result<(), RegisterError> {
        let session_map = self.session_maps.get_mut(id)
            .ok_or_else(|| RegisterError::NotFound(id.to_string()))?;
        
        session_map.is_deleted = true;
        
        // Reset known state to only report delete sessions
        let mut new_known_state = KnownState {
            header: true,
            id: id.to_string(),
            sessions: BTreeMap::new(),
        };
        
        // Only keep delete session counts in known state
        for (session_id, session_log) in &session_map.sessions {
            if is_delete_session_id(session_id) {
                new_known_state.sessions.insert(
                    session_id.clone(),
                    session_log.transaction_count() as u32,  // Use NEW method
                );
            }
        }
        
        session_map.known_state = new_known_state;
        session_map.known_state_with_streaming = None;
        session_map.streaming_known_state = None;
        
        Ok(())
    }
    
    pub fn is_deleted(&self, id: &str) -> Option<bool> {
        self.session_maps.get(id).map(|sm| sm.is_deleted)
    }
    
    // === Decryption ===
    // Uses EXISTING SessionLogInternal methods (already implemented)
    
    pub fn decrypt_transaction(
        &self,
        id: &str,
        session_id: &str,
        tx_index: u32,
        key_secret: &str,
    ) -> Result<Option<String>, RegisterError> {
        let session_map = self.session_maps.get(id)
            .ok_or_else(|| RegisterError::NotFound(id.to_string()))?;
        
        let session_log = session_map.sessions.get(session_id)
            .ok_or_else(|| RegisterError::NotFound(format!("session {}:{}", id, session_id)))?;
        
        // Use EXISTING SessionLogInternal method
        let decrypted = session_log.decrypt_next_transaction_changes_json(tx_index as usize, key_secret);
        Ok(Some(decrypted))
    }
    
    pub fn decrypt_transaction_meta(
        &self,
        id: &str,
        session_id: &str,
        tx_index: u32,
        key_secret: &str,
    ) -> Result<Option<String>, RegisterError> {
        let session_map = self.session_maps.get(id)
            .ok_or_else(|| RegisterError::NotFound(id.to_string()))?;
        
        let session_log = session_map.sessions.get(session_id)
            .ok_or_else(|| RegisterError::NotFound(format!("session {}:{}", id, session_id)))?;
        
        // Use EXISTING SessionLogInternal method
        Ok(session_log.decrypt_next_transaction_meta_json(tx_index as usize, key_secret))
    }
    
    // === Lifecycle ===
    
    pub fn free(&mut self, id: &str) -> bool {
        let header_removed = self.headers.remove(id).is_some();
        let session_map_removed = self.session_maps.remove(id).is_some();
        header_removed || session_map_removed
    }
    
    pub fn free_all(&mut self) {
        self.headers.clear();
        self.session_maps.clear();
    }
    
    pub fn size(&self) -> usize {
        self.session_maps.len()
    }
}

// Helper functions
fn is_delete_session_id(session_id: &str) -> bool {
    session_id.contains("_session_d") && session_id.ends_with('$')
}

fn exceeds_recommended_size(size: usize) -> bool {
    size > 100_000  // 100KB threshold
}
```

### TypeScript Thin Wrapper (`SessionMap.ts`)

```typescript
// packages/cojson/src/coValueCore/SessionMap.ts

export class SessionMap {
  constructor(
    private readonly id: RawCoID,
    private readonly register: RegisterImpl,  // From CryptoProvider via LocalNode
  ) {
    register.createSessionMap(id);
  }

  // No get() returning full SessionLog - use specific accessors:
  
  getTransactionCount(sessionID: SessionID): number | undefined {
    return this.register.getTransactionCount(this.id, sessionID);
  }
  
  getTransaction(sessionID: SessionID, txIndex: number): Transaction | undefined {
    const json = this.register.getTransaction(this.id, sessionID, txIndex);
    return json ? JSON.parse(json) : undefined;
  }
  
  getTransactions(sessionID: SessionID, fromIndex: number = 0): Transaction[] | undefined {
    const json = this.register.getSessionTransactions(this.id, sessionID, fromIndex);
    return json ? JSON.parse(json) : undefined;
  }
  
  getLastSignature(sessionID: SessionID): Signature | undefined {
    return this.register.getLastSignature(this.id, sessionID) as Signature | undefined;
  }
  
  getSignatureAfter(sessionID: SessionID, txIndex: number): Signature | undefined {
    return this.register.getSignatureAfter(this.id, sessionID, txIndex) as Signature | undefined;
  }
  
  getLastSignatureCheckpoint(sessionID: SessionID): number | undefined {
    return this.register.getLastSignatureCheckpoint(this.id, sessionID);
  }

  addTransaction(
    sessionID: SessionID,
    signerID: SignerID | undefined,
    newTransactions: Transaction[],
    newSignature: Signature,
    skipVerify: boolean = false,
  ) {
    this.register.addTransactions(
      this.id,
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
    const resultJson = this.register.makeNewPrivateTransaction(
      this.id,
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
    const resultJson = this.register.makeNewTrustingTransaction(
      this.id,
      sessionID,
      signerAgent.currentSignerID(),
      JSON.stringify(changes),
      meta ? JSON.stringify(meta) : null,
      madeAt,
    );
    return JSON.parse(resultJson);
  }

  get knownState(): CoValueKnownState {
    return JSON.parse(this.register.getKnownState(this.id));
  }

  get knownStateWithStreaming(): CoValueKnownState | undefined {
    const json = this.register.getKnownStateWithStreaming(this.id);
    return json ? JSON.parse(json) : undefined;
  }

  setStreamingKnownState(streamingKnownState: KnownStateSessions) {
    this.register.setStreamingKnownState(this.id, JSON.stringify(streamingKnownState));
  }

  markAsDeleted() {
    this.register.markAsDeleted(this.id);
  }

  decryptTransaction(
    sessionID: SessionID,
    txIndex: number,
    keySecret: KeySecret,
  ): JsonValue[] | undefined {
    const json = this.register.decryptTransaction(this.id, sessionID, txIndex, keySecret);
    return json ? JSON.parse(json) : undefined;
  }

  decryptTransactionMeta(
    sessionID: SessionID,
    txIndex: number,
    keySecret: KeySecret,
  ): JsonObject | undefined {
    const json = this.register.decryptTransactionMeta(this.id, sessionID, txIndex, keySecret);
    return json ? JSON.parse(json) : undefined;
  }

  // Iterator support for newContentSince (stays in TS for now)
  *entries(): IterableIterator<[SessionID, SessionLog]> {
    const sessionIds = this.register.getSessionIds(this.id);
    for (const sessionId of sessionIds) {
      const session = this.get(sessionId as SessionID);
      if (session) {
        yield [sessionId as SessionID, session];
      }
    }
  }

  get size(): number {
    return this.register.getSessionIds(this.id).length;
  }
  
  // Note: clone() removed - VerifiedState.clone() is never called externally
}
```

### TypeScript: `VerifiedState` (MODIFIED)

```typescript
// packages/cojson/src/coValueCore/verifiedState.ts

export class VerifiedState {
  readonly id: RawCoID;
  readonly register: RegisterImpl;
  readonly sessions: SessionMap;
  
  // Header is fetched from Rust Register
  get header(): CoValueHeader {
    const json = this.register.getHeader(this.id);
    if (!json) throw new Error(`Header not found for ${this.id}`);
    return JSON.parse(json);
  }
  
  constructor(
    id: RawCoID,
    register: RegisterImpl,
    header: CoValueHeader,
  ) {
    this.id = id;
    this.register = register;
    
    // Store header in Rust
    register.setHeader(id, JSON.stringify(header));
    
    // Create SessionMap wrapper
    this.sessions = new SessionMap(id, register);
  }
  
  // Note: clone() removed - never called externally
  
  // Delegates to SessionMap (calculated in Rust)
  getLastSignatureCheckpoint(sessionID: SessionID): number {
    return this.sessions.getLastSignatureCheckpoint(sessionID);
  }
  
  // newContentSince stays in TypeScript, reads from Rust via SessionMap
  newContentSince(knownState: CoValueKnownState | undefined): NewContentMessage[] | undefined {
    // Uses this.sessions.entries(), getTransactions(), getLastSignature(), etc.
    // Implementation stays largely the same, just uses Rust-backed data
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
const result = register.addTransactions(id, sessionId, signerId, JSON.stringify(transactions), signature, false);
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

### Phase 1: Rust SessionMap Implementation
1. Implement `SessionMap` struct in `cojson-core/src/core/register.rs`
2. Implement unified `SessionLog` with data storage + crypto (signature verification, decryption)
3. Add unit tests in Rust

### Phase 2: FFI Bindings
1. Add NAPI bindings for all SessionMap operations
2. Add WASM bindings
3. Add React Native (UniFFI) bindings
4. Test each platform independently

### Phase 3: TypeScript Integration
1. Replace `SessionMap.ts` with thin wrapper that delegates to Rust
2. Update `VerifiedState` to use new SessionMap
3. Remove old TypeScript SessionMap implementation
4. Verify all existing tests pass

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
pub enum RegisterError {
    #[error("CoValue not found: {0}")]
    NotFound(String),
    
    #[error("Header already exists for: {0}")]
    HeaderExists(String),
    
    #[error("Session map already exists for: {0}")]
    SessionMapExists(String),
    
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
    
    #[test]
    fn test_session_map_add_transactions() {
        let mut register = CoValueRegister::new();
        register.set_header("co_test", /* header_json */).unwrap();
        register.create_session_map("co_test").unwrap();
        
        register.add_transactions(
            "co_test",
            "session_1",
            Some("signer_1"),
            r#"[{"privacy":"trusting","madeAt":1234,"changes":"[]"}]"#,
            "sig_abc",
            false,
        ).unwrap();
        
        let known_state: KnownState = serde_json::from_str(
            &register.get_known_state("co_test").unwrap()
        ).unwrap();
        assert_eq!(known_state.sessions.get("session_1"), Some(&1));
    }
    
    #[test]
    fn test_mark_as_deleted() {
        let mut register = CoValueRegister::new();
        // ... setup ...
        
        register.mark_as_deleted("co_test").unwrap();
        assert!(register.is_deleted("co_test"));
    }
}
```

### Integration Tests (TypeScript)

```typescript
describe('SessionMap (Rust-backed)', () => {
  it('should store and retrieve transactions', () => {
    const sessionMap = new SessionMap(coId, register);
    
    sessionMap.addTransaction(
      sessionId,
      signerId,
      [{ privacy: 'trusting', madeAt: Date.now(), changes: '[]' }],
      signature,
    );
    
    const session = sessionMap.get(sessionId);
    expect(session?.transactions).toHaveLength(1);
  });
  
  it('should track known state correctly', () => {
    const sessionMap = new SessionMap(coId, register);
    // ... add transactions ...
    
    expect(sessionMap.knownState.sessions[sessionId]).toBe(1);
  });
});
```

