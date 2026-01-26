# Codebase Knowledge: Jazz CoValue Architecture

This document captures architectural knowledge about the Jazz codebase, specifically the `cojson` package and its Rust integration, gathered during the Rust CoValue Register design and implementation process.

**Last Updated**: Implementation Phase 1-6 completed

---

## Project Structure

```
jazz/
├── packages/
│   └── cojson/                    # Core CRDT implementation (TypeScript)
│       └── src/
│           ├── coValueCore/       # Core CoValue infrastructure
│           ├── coValues/          # CoValue type implementations
│           ├── crypto/            # Crypto provider abstraction
│           └── storage/           # Storage backends
├── crates/
│   ├── cojson-core/              # Core Rust library
│   │   └── src/core/
│   │       ├── register.rs       # NEW: CoValueRegister implementation
│   │       ├── session_log.rs    # SessionLogInternal
│   │       ├── keys.rs           # Key types
│   │       ├── nonce.rs          # Nonce generation
│   │       ├── cache.rs          # Crypto cache
│   │       └── error.rs          # Error types
│   ├── cojson-core-napi/         # Node.js bindings (N-API)
│   │   └── src/
│   │       ├── lib.rs            # Main exports
│   │       └── register.rs       # NEW: Register NAPI bindings
│   ├── cojson-core-wasm/         # Browser bindings (WASM)
│   │   └── src/
│   │       ├── lib.rs            # Main exports
│   │       └── register.rs       # NEW: Register WASM bindings
│   └── cojson-core-rn/           # React Native bindings (UniFFI) - TODO
```

---

## Core Data Structures

### CoValueCore (`packages/cojson/src/coValueCore/coValueCore.ts`)
The central class managing a single CoValue instance.

**Key Properties:**
- `id: RawCoID` - Unique identifier (hash of header)
- `_verified: VerifiedState | null` - Verified content (header + sessions)
- `verifiedTransactions: VerifiedTransaction[]` - Parsed transaction list
- `dependencies: Set<RawCoID>` - CoValues this depends on (groups, accounts)
- `dependant: Set<RawCoID>` - CoValues that depend on this

**Key Methods:**
- `tryAddTransactions()` - Add transactions from sync/storage
- `makeTransaction()` - Create local transaction
- `getCurrentContent()` - Build RawCoValue view
- `getValidTransactions()` - Get validated, decrypted transactions
- `newContentSince()` - Generate sync messages

### VerifiedState (`packages/cojson/src/coValueCore/verifiedState.ts`)
Holds verified (signature-checked) content.

```typescript
class VerifiedState {
  readonly id: RawCoID;
  readonly header: CoValueHeader;
  readonly sessions: SessionMap;
}
```

### CoValueHeader
```typescript
type CoValueHeader = {
  type: "comap" | "colist" | "costream" | "coplaintext";
  ruleset: RulesetDef;
  meta: JsonObject | null;
  uniqueness: Uniqueness;
  createdAt?: string;
}

type RulesetDef =
  | { type: "group"; initialAdmin: string }
  | { type: "ownedByGroup"; group: RawCoID }
  | { type: "unsafeAllowAll" };

type Uniqueness = string | boolean | null | number (integer only) | { [key: string]: string };
```

### SessionMap (`packages/cojson/src/coValueCore/SessionMap.ts`)
Maps session IDs to transaction logs.

```typescript
class SessionMap {
  sessions: Map<SessionID, SessionLog>;
  knownState: CoValueKnownState;
  knownStateWithStreaming?: CoValueKnownState;
}

type SessionLog = {
  signerID?: SignerID;
  impl: SessionLogInternal;  // Rust implementation
  transactions: Transaction[];
  lastSignature: Signature | undefined;
  signatureAfter: { [txIdx: number]: Signature };
};
```

### Transaction Types
```typescript
type PrivateTransaction = {
  privacy: "private";
  madeAt: number;
  keyUsed: KeyID;
  encryptedChanges: Encrypted<JsonValue[]>;
  meta?: Encrypted<JsonObject>;
};

type TrustingTransaction = {
  privacy: "trusting";
  madeAt: number;
  changes: Stringified<JsonValue[]>;
  meta?: Stringified<JsonObject>;
};
```

---

## Rust Implementation (NEW - Completed)

### CoValueRegister (`crates/cojson-core/src/core/register.rs`)

Central registry for all CoValue data. This is the main new component that holds all `CoValueHeader` and `SessionMapInternal` instances in HashMaps keyed by `RawCoID`.

**Design Principle**: Move **algorithms** to Rust, not just data structures, to avoid per-item FFI overhead.

#### Data Structures

```rust
/// Central registry - no thread-safety needed (single-threaded JS runtime)
pub struct CoValueRegister {
    headers: HashMap<String, CoValueHeader>,
    session_maps: HashMap<String, SessionMapInternal>,
}

/// Header matching TypeScript format exactly
pub struct CoValueHeader {
    pub co_type: String,        // "comap" | "colist" | "costream" | "coplaintext"
    pub ruleset: RulesetDef,
    pub meta: Option<serde_json::Value>,
    pub uniqueness: Uniqueness,
    pub created_at: Option<String>,
}

/// Permission ruleset (tagged enum for JSON serialization)
pub enum RulesetDef {
    Group { initial_admin: String },
    OwnedByGroup { group: String },
    UnsafeAllowAll,
}

/// Uniqueness types (untagged enum - serializes directly)
pub enum Uniqueness {
    String(String),
    Bool(bool),
    Integer(i64),  // Note: floats NOT allowed
    Null,
    Object(HashMap<String, String>),
}

/// Known state for sync
pub struct KnownState {
    pub id: String,
    pub header: bool,
    pub sessions: HashMap<String, u32>,  // Session ID -> tx count
}

/// Internal session map (holds SessionLogInternal instances)
pub struct SessionMapInternal {
    co_id: String,
    sessions: HashMap<String, SessionLogInternal>,
    signer_ids: HashMap<String, Option<SignerID>>,
    known_state: KnownState,
    known_state_with_streaming: Option<KnownState>,
    is_deleted: bool,
}

/// Raw transaction data for batch queries
pub struct RawTransactionData {
    pub session_id: String,
    pub tx_index: u32,
    pub made_at: u64,
    pub privacy: String,
    pub changes_or_encrypted: String,
    pub meta_or_encrypted: Option<String>,
    pub key_used: Option<String>,
}

/// Content message for sync (output of get_content_since)
pub struct ContentMessage {
    pub id: String,
    pub header: Option<CoValueHeader>,
    pub new: HashMap<String, SessionContent>,
    pub expect_content_until: Option<HashMap<String, u32>>,
}

/// Session content in sync message
pub struct SessionContent {
    pub after: u32,
    pub transactions: Vec<RawTransactionData>,
    pub last_signature: String,
}
```

#### Public API

```rust
impl CoValueRegister {
    // === Lifecycle ===
    pub fn new() -> Self;
    pub fn with_capacity(capacity: usize) -> Self;
    pub fn free(&mut self, id: &str) -> bool;
    pub fn free_all(&mut self);
    pub fn size(&self) -> usize;
    
    // === Header Operations ===
    pub fn set_header(&mut self, id: &str, header: CoValueHeader) -> Result<(), RegisterError>;
    pub fn get_header(&self, id: &str) -> Option<&CoValueHeader>;
    pub fn get_header_cloned(&self, id: &str) -> Option<CoValueHeader>;
    pub fn has_header(&self, id: &str) -> bool;
    
    // === Session Map Operations ===
    pub fn create_session_map(&mut self, id: &str) -> Result<(), RegisterError>;
    pub fn get_or_create_session_map(&mut self, id: &str) -> &mut SessionMapInternal;
    pub fn get_session_tx_count(&self, id: &str, session_id: &str) -> u32;
    
    // === Transaction Operations ===
    pub fn add_transactions(
        &mut self,
        id: &str,
        session_id: &str,
        signer_id: Option<&str>,
        transactions: Vec<TransactionInput>,
        signature: &str,
        skip_verify: bool,
    ) -> Result<(), RegisterError>;
    
    // === Known State ===
    pub fn get_known_state(&self, id: &str) -> Result<KnownState, RegisterError>;
    pub fn get_known_state_with_streaming(&self, id: &str) -> Result<KnownState, RegisterError>;
    
    // === Batch Queries (MAIN PERFORMANCE WIN) ===
    pub fn get_raw_transactions(
        &self,
        id: &str,
        options: RawTransactionQueryOptions,
    ) -> Result<Vec<RawTransactionData>, RegisterError>;
    
    pub fn get_content_since(
        &self,
        id: &str,
        known_state: Option<&KnownState>,
    ) -> Result<Vec<ContentMessage>, RegisterError>;
    
    // === Decryption (JS provides key from group lookup) ===
    pub fn decrypt_transaction(
        &self,
        id: &str,
        session_id: &str,
        tx_index: u32,
        key_secret: &str,
    ) -> Result<String, RegisterError>;
    
    pub fn decrypt_transaction_meta(
        &self,
        id: &str,
        session_id: &str,
        tx_index: u32,
        key_secret: &str,
    ) -> Result<Option<String>, RegisterError>;
}
```

#### Error Types

```rust
pub enum RegisterError {
    NotFound(String),           // CoValue not found
    HeaderExists(String),       // Header already exists
    InvalidHeader(String),      // Invalid header data
    SessionMapExists(String),   // Session map already exists
    SessionNotFound(String, String),  // Session not found in CoValue
    SignatureVerification(String),
    Decryption(String),
    Serialization(serde_json::Error),
    Core(CoJsonCoreError),      // Underlying session log errors
}
```

### SessionLogInternal (`crates/cojson-core/src/core/session_log.rs`)
Pre-existing Rust implementation, handles:
- Transaction hashing (blake3)
- Signature verification (ed25519)
- Encryption/decryption (XSalsa20)
- Nonce generation

### FFI Bindings

#### NAPI (`crates/cojson-core-napi/src/register.rs`)
```rust
#[napi]
pub struct Register {
    inner: CoValueRegister,
}

#[napi]
impl Register {
    #[napi(constructor)]
    pub fn new() -> Self;
    
    #[napi(factory)]
    pub fn with_capacity(capacity: u32) -> Self;
    
    // All methods use JSON strings for data transfer
    #[napi]
    pub fn set_header(&mut self, id: String, header_json: String) -> napi::Result<()>;
    
    #[napi]
    pub fn get_content_since(
        &self,
        id: String,
        known_state_json: Option<String>,
    ) -> napi::Result<String>;  // Returns JSON array
    // ... etc
}
```

#### WASM (`crates/cojson-core-wasm/src/register.rs`)
```rust
#[wasm_bindgen]
pub struct Register {
    inner: CoValueRegister,
}

#[wasm_bindgen]
impl Register {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self;
    
    #[wasm_bindgen(js_name = withCapacity)]
    pub fn with_capacity(capacity: u32) -> Self;
    
    // Same API as NAPI, but using wasm_bindgen attributes
    // ...
}
```

---

## Test Coverage

### Rust Unit Tests (11 tests)
Location: `crates/cojson-core/src/core/register.rs`
- `test_register_lifecycle`
- `test_header_already_exists`
- `test_header_serialization_roundtrip`
- `test_ruleset_serialization`
- `test_uniqueness_serialization`
- `test_known_state`
- `test_session_map_internal`
- `test_free_all`
- `test_get_content_since_empty`
- `test_get_content_since_with_header_only`
- `test_get_content_since_with_known_header`

### NAPI Integration Tests (27 tests)
Location: `crates/cojson-core-napi/__test__/register.test.ts`
- Lifecycle: new, withCapacity, free, freeAll
- Header operations: setHeader, getHeader, hasHeader
- Session map operations: createSessionMap, getSessionTxCount
- Known state: getKnownState
- Content sync: getContentSince (empty, header only, known header, new transactions)
- Transactions: addTransactions, getRawTransactions with filters

### WASM Integration Tests (27 tests)
Location: `crates/cojson-core-wasm/__test__/register.test.ts`
- Same coverage as NAPI tests

---

## Transaction Processing Pipeline

### Flow: New Content Received
```
1. SyncManager.handleNewContent(msg, from)
   ↓
2. CoValueCore.tryAddTransactions(sessionID, txs, signature, skipVerify)
   ↓
3. SessionMap.addTransaction() 
   → SessionLogInternal.tryAdd() [Rust - signature verification]
   ↓
4. CoValueCore.processNewTransactions()
   ↓
5. CoValueCore.loadVerifiedTransactionsFromLogs()
   → Iterates sessions, creates VerifiedTransaction objects
   ↓
6. determineValidTransactions(coValue) [permissions.ts]
   → Checks group membership for each transaction
   ↓
7. decryptTransactionChangesAndMeta() for private transactions
   → Gets key from group, calls Rust decrypt
   ↓
8. RawCoValue.processNewTransactions()
   → Builds content view (CoMap/CoList/etc.)
```

### Flow: Generate Sync Message (NOW IN RUST)
```
// TypeScript calls:
register.getContentSince(id, knownStateJson)

// Rust executes entire algorithm:
1. Check if header needs to be included
2. For each session:
   - Determine which transactions are new (after known count)
   - Build SessionContent with transactions
3. Build ContentMessage with header (if needed) + new content
4. Return JSON array of ContentMessage
```

---

## Cross-CoValue Dependencies

### Operations That CANNOT Move to Rust (require group state)

| Operation | Why |
|-----------|-----|
| `determineValidTransactions()` | Needs to load owner group, traverse parents |
| `roleOfInternal()` | Recursive parent group traversal |
| `getReadKey()` | Key lookup through group hierarchy |
| `atTime()` filtering | Time-travel across group transactions |
| Delete permission check | Owner group admin status |

### Operations That CAN/DID Move to Rust (no cross-CoValue deps)

| Operation | Status |
|-----------|--------|
| `newContentSince()` | ✅ Implemented as `get_content_since()` |
| Signature verification | ✅ In SessionLogInternal |
| Transaction decryption (given key) | ✅ In Register |
| Known state tracking | ✅ In Register |
| Raw transaction queries | ✅ `get_raw_transactions()` |

---

## IDs and Types

```typescript
type RawCoID = `co_z${string}`;  // Hash of header
type SessionID = `${RawCoID}_session_z${string}`;
type AgentID = `sealer_z${string}`;
type RawAccountID = RawCoID;  // Account is also a CoValue
type KeyID = `key_z${string}`;
type KeySecret = `key_z${string}`;  // Private key material
type SignerID = `signer_z${string}`;
type Signature = `signature_z${string}`;
```

---

## Implementation Status

### Completed ✅
- [x] Core Rust `CoValueRegister` implementation
- [x] All data types (CoValueHeader, RulesetDef, Uniqueness, KnownState, etc.)
- [x] Header storage operations
- [x] Session map management
- [x] Transaction storage with signature verification
- [x] Known state tracking
- [x] `get_content_since()` algorithm (entire newContentSince in Rust)
- [x] `get_raw_transactions()` batch query
- [x] Decryption methods
- [x] NAPI bindings + tests (27 tests)
- [x] WASM bindings + tests (27 tests)
- [x] React Native (UniFFI) Rust implementation
- [x] **Register integrated into CryptoProvider** (same pattern as SessionLog)
  - `NapiCrypto.register` - NAPI Register instance
  - `WasmCrypto.register` - WASM Register instance  
  - `RNCrypto.register` - UniFFI Register instance (pending bindings)
- [x] `RegisterImpl` interface in `crypto.ts`
- [x] Types in `rustRegister.ts` (RustContentMessage, RawTransactionData, etc.)
- [x] Feature flags (`FEATURE_FLAGS.USE_RUST_REGISTER`)
- [x] LocalNode accesses register via `crypto.register`
- [x] Dual-write to Register in `CoValueCore.provideHeader()`
- [x] Dual-write to Register in `CoValueCore.tryAddTransactions()`
- [x] Conditional `newContentSince` using Rust implementation

### Pending
- [ ] React Native bindings regeneration (requires `pnpm build:rn` with native SDKs)
- [ ] React Native tests
- [ ] Integration tests comparing TypeScript vs Rust outputs
- [ ] Performance benchmarks (Phase 8)
- [ ] Final cleanup and migration (Phase 9)

---

## File Locations Quick Reference

| Concept | File |
|---------|------|
| **CryptoProvider (base)** | `packages/cojson/src/crypto/crypto.ts` |
| **NapiCrypto + Register** | `packages/cojson/src/crypto/NapiCrypto.ts` |
| **WasmCrypto + Register** | `packages/cojson/src/crypto/WasmCrypto.ts` |
| **RNCrypto + Register** | `packages/cojson/src/crypto/RNCrypto.ts` |
| **Register Types** | `packages/cojson/src/coValueCore/rustRegister.ts` |
| **Config (Feature Flags)** | `packages/cojson/src/config.ts` |
| CoValueCore | `packages/cojson/src/coValueCore/coValueCore.ts` |
| VerifiedState | `packages/cojson/src/coValueCore/verifiedState.ts` |
| SessionMap | `packages/cojson/src/coValueCore/SessionMap.ts` |
| Permissions | `packages/cojson/src/permissions.ts` |
| RawGroup | `packages/cojson/src/coValues/group.ts` |
| RawCoMap | `packages/cojson/src/coValues/coMap.ts` |
| LocalNode | `packages/cojson/src/localNode.ts` |
| Exports | `packages/cojson/src/exports.ts` |
| **Rust Register** | `crates/cojson-core/src/core/register.rs` |
| Rust SessionLog | `crates/cojson-core/src/core/session_log.rs` |
| Rust lib.rs | `crates/cojson-core/src/lib.rs` |
| **NAPI Register** | `crates/cojson-core-napi/src/register.rs` |
| NAPI lib.rs | `crates/cojson-core-napi/src/lib.rs` |
| **NAPI Tests** | `crates/cojson-core-napi/__test__/register.test.ts` |
| **WASM Register** | `crates/cojson-core-wasm/src/register.rs` |
| WASM lib.rs | `crates/cojson-core-wasm/src/lib.rs` |
| **WASM Tests** | `crates/cojson-core-wasm/__test__/register.test.ts` |
| **RN Register** | `crates/cojson-core-rn/rust/src/register.rs` |
| RN lib.rs | `crates/cojson-core-rn/rust/src/lib.rs` |
| RN Generated | `crates/cojson-core-rn/src/generated/cojson_core_rn.ts` (auto-generated)

---

## CryptoProvider Architecture

The `Register` is integrated into the `CryptoProvider` class, following the same pattern as `SessionLog`. This ensures:

1. **Consistent initialization**: Register is created when crypto provider is created
2. **Platform-specific bindings**: Each platform gets its own Register implementation
3. **Clean separation**: No separate provider/factory needed for Register

### Class Hierarchy

```typescript
// Base class in crypto.ts
abstract class CryptoProvider {
  register: RegisterImpl | undefined;  // NEW: Register instance
  abstract createSessionLog(...): SessionLogImpl;
  // ... other crypto methods
}

// Platform-specific implementations
class NapiCrypto extends CryptoProvider {
  constructor() {
    this.register = new Register();  // from cojson-core-napi
  }
}

class WasmCrypto extends CryptoProvider {
  constructor() {
    this.register = new Register();  // from cojson-core-wasm
  }
}

class RNCrypto extends CryptoProvider {
  constructor() {
    this.register = new Register();  // from cojson-core-rn (pending)
  }
}
```

### Access Pattern

```typescript
// LocalNode provides access to register via crypto
class LocalNode {
  get register(): RegisterImpl | undefined {
    return this.crypto.register;
  }
}

// CoValueCore uses node.register for dual-writes
class CoValueCore {
  private dualWriteHeaderToRegister(header: CoValueHeader) {
    const register = this.node.register;
    if (register) {
      register.setHeader(this.id, JSON.stringify(header));
    }
  }
}
```

### RegisterImpl Interface

```typescript
interface RegisterImpl {
  // Lifecycle
  size(): number;
  free(id: string): boolean;
  freeAll(): void;

  // Header operations
  setHeader(id: string, headerJson: string): void;
  getHeader(id: string): string | null | undefined;
  hasHeader(id: string): boolean;

  // Session map operations
  createSessionMap(id: string): void;
  addTransactions(...): void;

  // Key algorithm - runs entirely in Rust
  getContentSince(id: string, knownStateJson: string | null): string;
}
```

---

## Thread Safety Considerations

### Node.js / Browser (NAPI / WASM)
The JavaScript runtime is single-threaded. Implications:
- No need for `RwLock` or `Mutex` in Rust
- No concurrent access concerns
- Async operations use callbacks/promises, not threads
- Register uses simple `HashMap` without synchronization

### React Native (UniFFI)
React Native runs in a multi-threaded environment:
- Rust objects are wrapped in `Mutex<T>` for thread safety
- Methods acquire lock before accessing internal state
- `RegisterUniFFIError::LockError` handles lock acquisition failures
