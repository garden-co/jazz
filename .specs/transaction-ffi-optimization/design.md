# Transaction FFI Optimization - Design

## Overview

This design eliminates JSON serialization overhead by introducing FFI-compatible Transaction structs that can be passed directly from TypeScript to Rust. The key insight is that each binding technology (wasm-bindgen, napi-rs, uniffi) supports passing structured data, but they require different approaches:

- **WASM**: Uses `#[wasm_bindgen]` structs with constructor (instantiated via `new` in JS)
- **NAPI**: Uses `#[napi(object)]` for plain JS objects + factory function `createTransactionFfi`
- **Uniffi**: Uses `#[derive(uniffi::Record)]` + exported factory function `create_transaction_ffi`

Each binding layer defines its own FFI struct and converts **directly** to `PrivateTransaction` or `TrustingTransaction`. No intermediate types in `cojson-core`.

This implementation also standardizes the FFI transaction payload shape across all bindings:
- A single `changes` string is used for both privacy modes:
  - For `"private"` it contains the encrypted changes string (e.g. `"encrypted_U..."`)
  - For `"trusting"` it contains the stringified JSON changes
- `key_used`/`keyUsed` is **required** for `"private"` and **absent/undefined** for `"trusting"`
- `meta` remains optional for both privacy modes

**No shared helper file**: Each crypto adapter (`WasmCrypto.ts`, `NapiCrypto.ts`, `RNCrypto.ts`) defines its own inline conversion function, keeping platform-specific logic co-located.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        TypeScript Layer                              │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  Each crypto adapter defines its own conversion function:    │    │
│  │  - WasmCrypto.ts: toWasmFfiTransaction()                    │    │
│  │  - NapiCrypto.ts: toNapiFfiTransaction()                    │    │
│  │  - RNCrypto.ts:   toUniffiFfiTransaction()                  │    │
│  └─────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Platform Binding Layer                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐   │
│  │ cojson-core  │  │ cojson-core  │  │ cojson-core-rn           │   │
│  │ -wasm        │  │ -napi        │  │ (uniffi)                 │   │
│  │              │  │              │  │                          │   │
│  │ WasmFfiTx    │  │ NapiFfiTx    │  │ UniffiFfiTx              │   │
│  │ (constructor)│  │ (factory fn) │  │ (factory fn)             │   │
│  │     │        │  │     │        │  │     │                    │   │
│  │     ▼        │  │     ▼        │  │     ▼                    │   │
│  │ to_transac-  │  │ to_transac-  │  │ to_transaction()         │   │
│  │ tion()       │  │ tion()       │  │ (local fn)               │   │
│  │     │        │  │     │        │  │     │                    │   │
│  │     ▼        │  │     ▼        │  │     ▼                    │   │
│  │ Private or   │  │ Private or   │  │ Private or               │   │
│  │ Trusting Tx  │  │ Trusting Tx  │  │ Trusting Tx              │   │
│  └──────────────┘  └──────────────┘  └──────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        cojson-core                                   │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  try_add_transactions(Vec<Transaction>, signature, skip)    │    │
│  │  (accepts already-converted Transaction enum)                │    │
│  └─────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Updated SessionLogInternal (`cojson-core/src/core/session_log.rs`)

A new method accepts already-converted `Transaction` objects directly. The conversion from FFI types happens at each binding layer, not in cojson-core:

```rust
impl SessionLogInternal {
    /// Try to add transactions that are already converted to internal Transaction type.
    /// Each binding layer is responsible for converting their FFI types to Transaction.
    pub fn try_add_transactions(
        &mut self,
        transactions: Vec<Transaction>,
        new_signature: &Signature,
        skip_verify: bool,
    ) -> Result<(), CoJsonCoreError> {
        // Serialize to JSON for hashing (required for signature verification)
        let transactions_json: Vec<String> = transactions
            .iter()
            .map(|tx| serde_json::to_string(tx))
            .collect::<Result<Vec<_>, _>>()?;
        
        // Continue with existing logic...
        self.try_add_internal(transactions_json, new_signature, skip_verify)
    }
}
```

**Note:** No intermediate `FfiTransaction` type is needed in cojson-core. Each binding layer converts directly to `Transaction::Private(PrivateTransaction)` or `Transaction::Trusting(TrustingTransaction)`.

### 2. WASM Binding (`cojson-core-wasm/src/lib.rs`)

Uses `#[wasm_bindgen]` struct with constructor. JavaScript creates instances via `new WasmFfiTransaction(...)`:

```rust
/// WASM-compatible FFI Transaction struct.
/// Can be passed directly from JavaScript without JSON serialization.
#[wasm_bindgen(getter_with_clone)]
pub struct WasmFfiTransaction {
    /// "private" or "trusting"
    pub privacy: String,
    /// For private transactions: the key ID used for encryption
    pub key_used: Option<String>,
    /// Transaction payload:
    /// - for private transactions: the encrypted changes string (e.g., "encrypted_U...")
    /// - for trusting transactions: the stringified changes JSON
    pub changes: String,
    /// Timestamp when the transaction was made (milliseconds)
    pub made_at: u64,
    /// Optional meta (encrypted for private, stringified for trusting)
    pub meta: Option<String>,
}

#[wasm_bindgen]
impl WasmFfiTransaction {
    #[wasm_bindgen(constructor)]
    pub fn new(
        privacy: String,
        key_used: Option<String>,
        changes: String,
        made_at: u64,
        meta: Option<String>,
    ) -> WasmFfiTransaction {
        WasmFfiTransaction { privacy, key_used, changes, made_at, meta }
    }
}

/// Convert WasmFfiTransaction to internal Transaction type.
fn to_transaction(wasm: WasmFfiTransaction) -> Result<Transaction, CojsonCoreWasmError> {
    match wasm.privacy.as_str() {
        "private" => {
            let key_used = wasm.key_used.ok_or_else(|| {
                CojsonCoreWasmError::Js(JsValue::from_str("Missing key_used for private transaction"))
            })?;
            Ok(Transaction::Private(PrivateTransaction {
                encrypted_changes: Encrypted::new(wasm.changes),
                key_used: KeyID(key_used),
                made_at: Number::from(wasm.made_at),
                meta: wasm.meta.map(Encrypted::new),
                privacy: "private".to_string(),
            }))
        }
        "trusting" => Ok(Transaction::Trusting(TrustingTransaction {
            changes: wasm.changes,
            made_at: Number::from(wasm.made_at),
            meta: wasm.meta,
            privacy: "trusting".to_string(),
        })),
        _ => Err(CojsonCoreWasmError::Js(JsValue::from_str(&format!("Invalid privacy type: {}", wasm.privacy)))),
    }
}

#[wasm_bindgen]
impl SessionLog {
    /// FFI-optimized version of tryAdd that accepts typed transaction structs.
    #[wasm_bindgen(js_name = tryAddFfi)]
    pub fn try_add_ffi(
        &mut self,
        transactions: Vec<WasmFfiTransaction>,
        new_signature_str: String,
        skip_verify: bool,
    ) -> Result<(), CojsonCoreWasmError> {
        let new_signature = Signature(new_signature_str);
        let transactions: Vec<Transaction> = transactions
            .into_iter()
            .map(to_transaction)
            .collect::<Result<Vec<_>, _>>()?;
        self.internal.try_add_transactions(transactions, &new_signature, skip_verify)?;
        Ok(())
    }
}
```

### 3. NAPI Binding (`cojson-core-napi/src/lib.rs`)

Uses `#[napi(object)]` struct and an exported `createTransactionFfi` factory function. Uses `BigInt` for `made_at` to support full u64 range:

```rust
use napi::bindgen_prelude::BigInt;

#[napi(object)]
pub struct NapiFfiTransaction {
    /// "private" or "trusting"
    pub privacy: String,
    /// Transaction payload (encrypted for private, JSON for trusting)
    pub changes: String,
    /// For private transactions
    pub key_used: Option<String>,
    /// Timestamp (milliseconds) - BigInt for full u64 support
    pub made_at: BigInt,
    /// Optional meta (encrypted or stringified)
    pub meta: Option<String>,
}

#[napi(js_name = "createTransactionFfi")]
pub fn create_transaction(
    privacy: String,
    changes: String,
    key_used: Option<String>,
    made_at: BigInt,
    meta: Option<String>,
) -> napi::Result<NapiFfiTransaction> {
    Ok(NapiFfiTransaction {
        privacy,
        changes,
        key_used,
        made_at,
        meta,
    })
}

fn to_transaction(tx: NapiFfiTransaction) -> napi::Result<Transaction> {
    // Extract u64 from BigInt (returns (sign, value) tuple)
    let made_at = tx.made_at.get_u64().1;

    match tx.privacy.as_str() {
        "private" => {
            let key_used = tx.key_used.ok_or_else(|| {
                napi::Error::new(napi::Status::InvalidArg, "Missing key_used for private transaction")
            })?;
            Ok(Transaction::Private(PrivateTransaction {
                encrypted_changes: Encrypted::new(tx.changes),
                key_used: KeyID(key_used),
                made_at: Number::from(made_at),
                meta: tx.meta.map(Encrypted::new),
                privacy: "private".to_string(),
            }))
        }
        "trusting" => Ok(Transaction::Trusting(TrustingTransaction {
            changes: tx.changes,
            made_at: Number::from(made_at),
            meta: tx.meta,
            privacy: "trusting".to_string(),
        })),
        other => Err(napi::Error::new(napi::Status::InvalidArg, format!("Invalid privacy type: {other}"))),
    }
}

#[napi]
impl SessionLog {
    #[napi(js_name = "tryAddFfi")]
    pub fn try_add_ffi(
        &mut self,
        transactions: Vec<NapiFfiTransaction>,
        new_signature_str: String,
        skip_verify: bool,
    ) -> napi::Result<()> {
        let new_signature = Signature(new_signature_str);
        let transactions: Vec<Transaction> = transactions
            .into_iter()
            .map(to_transaction)
            .collect::<napi::Result<_>>()?;
        self.internal
            .try_add_transactions(transactions, &new_signature, skip_verify)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e.to_string()))?;
        Ok(())
    }
}
```

### 4. React Native/Uniffi Binding (`cojson-core-rn/rust/src/session_log.rs`)

Uses `#[derive(uniffi::Record)]` and an exported `create_transaction_ffi` factory function:

```rust
#[derive(uniffi::Record)]
pub struct UniffiFfiTransaction {
    /// "private" or "trusting"
    pub privacy: String,
    /// For private transactions
    pub key_used: Option<String>,
    /// Transaction payload:
    /// - for private transactions: the encrypted changes string (e.g., "encrypted_U...")
    /// - for trusting transactions: the stringified changes JSON
    pub changes: String,
    /// Timestamp (milliseconds)
    pub made_at: u64,
    /// Optional meta (encrypted or stringified)
    pub meta: Option<String>,
}

#[uniffi::export]
pub fn create_transaction_ffi(
    privacy: String,
    changes: String,
    key_used: Option<String>,
    made_at: u64,
    meta: Option<String>,
) -> UniffiFfiTransaction {
    UniffiFfiTransaction { privacy, changes, key_used, made_at, meta }
}

fn to_transaction(tx: UniffiFfiTransaction) -> Result<Transaction, SessionLogError> {
    match tx.privacy.as_str() {
        "private" => {
            let key_used = tx.key_used.ok_or_else(|| {
                SessionLogError::Generic("Missing key_used for private transaction".to_string())
            })?;
            Ok(Transaction::Private(PrivateTransaction {
                encrypted_changes: Encrypted::new(tx.changes),
                key_used: KeyID(key_used),
                made_at: Number::from(tx.made_at),
                meta: tx.meta.map(Encrypted::new),
                privacy: "private".to_string(),
            }))
        }
        "trusting" => Ok(Transaction::Trusting(TrustingTransaction {
            changes: tx.changes,
            made_at: Number::from(tx.made_at),
            meta: tx.meta,
            privacy: "trusting".to_string(),
        })),
        other => Err(SessionLogError::Generic(format!("Invalid privacy type: {other}"))),
    }
}

#[uniffi::export]
impl SessionLog {
    pub fn try_add_ffi(
        &self,
        transactions: Vec<UniffiFfiTransaction>,
        new_signature_str: String,
        skip_verify: bool,
    ) -> Result<(), SessionLogError> {
        let new_signature = Signature(new_signature_str);
        let transactions: Vec<Transaction> = transactions
            .into_iter()
            .map(to_transaction)
            .collect::<Result<Vec<_>, _>>()?;
        if let Ok(mut internal) = self.internal.lock() {
            internal.try_add_transactions(transactions, &new_signature, skip_verify).map_err(Into::into)
        } else {
            Err(SessionLogError::LockError)
        }
    }
}
```

### 5. TypeScript Crypto Adapters (Inline Conversion Functions)

Each crypto adapter defines its own inline conversion function. **No shared helper file.**

**WasmCrypto.ts:**
```typescript
import { WasmFfiTransaction } from "cojson-core-wasm";
import { Transaction } from "../coValueCore/verifiedState.js";

function toWasmFfiTransaction(tx: Transaction): WasmFfiTransaction {
  if (tx.privacy === "private") {
    return new WasmFfiTransaction(
      tx.privacy,
      tx.keyUsed,
      tx.encryptedChanges,
      BigInt(tx.madeAt),
      tx.meta,
    );
  }

  return new WasmFfiTransaction(
    tx.privacy,
    undefined,
    tx.changes,
    BigInt(tx.madeAt),
    tx.meta,
  );
}

class SessionLogAdapter {
  tryAdd(transactions: Transaction[], newSignature: Signature, skipVerify: boolean): void {
    this.sessionLog.tryAddFfi(
      transactions.map(toWasmFfiTransaction),
      newSignature,
      skipVerify,
    );
  }
}
```

**NapiCrypto.ts:**
```typescript
import { createTransactionFfi, NapiFfiTransaction } from "cojson-core-napi";
import { Transaction } from "../coValueCore/verifiedState.js";

function toNapiFfiTransaction(tx: Transaction): NapiFfiTransaction {
  if (tx.privacy === "private") {
    return createTransactionFfi(
      tx.privacy,
      tx.encryptedChanges,
      tx.keyUsed,
      BigInt(tx.madeAt),
      tx.meta,
    );
  }

  return createTransactionFfi(
    tx.privacy,
    tx.changes,
    undefined,
    BigInt(tx.madeAt),
    tx.meta,
  );
}

class SessionLogAdapter {
  tryAdd(transactions: Transaction[], newSignature: Signature, skipVerify: boolean): void {
    this.sessionLog.tryAddFfi(
      transactions.map(toNapiFfiTransaction),
      newSignature,
      skipVerify,
    );
  }
}
```

**RNCrypto.ts:**
```typescript
import { createTransactionFfi, UniffiFfiTransaction } from "cojson-core-rn";
import { Transaction } from "../coValueCore/verifiedState.js";

export function toUniffiFfiTransaction(tx: Transaction): UniffiFfiTransaction {
  if (tx.privacy === "private") {
    return createTransactionFfi(
      tx.privacy,
      tx.encryptedChanges,
      tx.keyUsed,
      BigInt(tx.madeAt),
      tx.meta,
    );
  }

  return createTransactionFfi(
    tx.privacy,
    tx.changes,
    undefined,
    BigInt(tx.madeAt),
    tx.meta,
  );
}

class SessionLogAdapter {
  tryAdd(transactions: Transaction[], newSignature: Signature, skipVerify: boolean): void {
    this.sessionLog.tryAddFfi(
      transactions.map(toUniffiFfiTransaction),
      newSignature,
      skipVerify,
    );
  }
}
```

## Data Models

### FFI Transaction Field Mapping

| TypeScript Field | Rust FFI Field | Target Rust Type | Notes |
|-----------------|----------------|------------------|-------|
| `privacy` | `privacy: String` | `Transaction::Private` or `Transaction::Trusting` | Discriminator |
| `keyUsed` | `key_used: Option<String>` | `PrivateTransaction.key_used.0` | Required for private |
| `changes` | `changes: String` | `PrivateTransaction.encrypted_changes.value` OR `TrustingTransaction.changes` | Required for both; meaning depends on `privacy` |
| `madeAt` | `made_at: u64/BigInt` | `*.made_at` as `Number` | bigint in JS, u64 in Rust |
| `meta` | `meta: Option<String>` | `*.meta` | Optional for both |

**Note:** TypeScript uses camelCase (`keyUsed`, `madeAt`), Rust uses snake_case (`key_used`, `made_at`). The binding generators handle the conversion automatically.

### Platform-Specific `madeAt` Handling

| Platform | Rust Type | JS Type | Notes |
|----------|-----------|---------|-------|
| WASM | `u64` | `bigint` | wasm-bindgen converts automatically |
| NAPI | `BigInt` | `bigint` | napi-rs BigInt wrapper |
| Uniffi | `u64` | `bigint` | uniffi-bindgen-react-native uses bigint |

### Direct Conversion Flow

```
TypeScript Transaction
        │
        ▼
Platform-specific conversion function:
  - WASM: new WasmFfiTransaction(...)
  - NAPI: createTransactionFfi(...)
  - RN:   createTransactionFfi(...)
        │
        ▼
FFI Object/Struct (WasmFfiTransaction / NapiFfiTransaction / UniffiFfiTransaction)
        │
        ▼ to_transaction(ffi_struct) -> Result<Transaction, Error>
        │ (local function in each binding crate)
        │
        ├──► Transaction::Private(PrivateTransaction { ... })
        │
        └──► Transaction::Trusting(TrustingTransaction { ... })
```

**Why a function instead of `TryFrom` trait?**

Rust's orphan rule prevents implementing a foreign trait (`TryFrom` from std) for a foreign type (`Transaction` from `cojson-core`). Since both the trait and the target type are from external crates, we must use a local function instead.

**Platform Differences:**
- **WASM**: Uses `WasmFfiTransaction` class with constructor (`new WasmFfiTransaction(...)`)
- **NAPI**: Uses `createTransactionFfi(...)` factory function returning `NapiFfiTransaction`
- **Uniffi**: Uses `createTransactionFfi(...)` factory function returning `UniffiFfiTransaction`

## Error Handling

Conversion errors are handled **at each binding layer** using platform-specific error types. No new error types are needed in `cojson-core`.

### 1. Missing Required Fields
When converting FFI struct to `Transaction`, missing required fields throw binding-specific errors:
- **WASM**: `CojsonCoreWasmError::Js(JsValue::from_str("Missing key_used..."))` (private only)
- **NAPI**: `napi::Error::new(napi::Status::InvalidArg, "Missing key_used...")` (private only)
- **Uniffi**: `SessionLogError::Generic("Missing key_used...".to_string())` (private only)

### 2. Invalid Privacy Type
If `privacy` is not "private" or "trusting":
- **WASM**: `CojsonCoreWasmError::Js(JsValue::from_str("Invalid privacy type: ..."))`
- **NAPI**: `napi::Error::new(napi::Status::InvalidArg, "Invalid privacy type: ...")`
- **Uniffi**: `SessionLogError::Generic("Invalid privacy type: ...".to_string())`

### 3. Core Errors
`SessionLogInternal::try_add_transactions` can still return `CoJsonCoreError` for:
- Signature verification failures
- JSON serialization errors (for hashing)
- Transaction size limit violations

These are converted to platform-specific errors at the binding layer.

### 4. Backward Compatibility
The existing JSON-based `tryAdd` method remains available, so existing code continues to work.

## Testing Strategy

### Unit Tests

1. **SessionLogInternal Tests** (`cojson-core/src/core/session_log.rs`)
   - Test `try_add_transactions` produces same results as `try_add` for equivalent inputs
   - Test signature verification works with `Transaction` objects directly
   - Test that JSON serialization for hashing is consistent

2. **Binding-Layer Conversion Tests** (in each binding crate)
   - Test `to_transaction` for private transactions
   - Test `to_transaction` for trusting transactions
   - Test error cases: missing required fields, invalid privacy type

### Integration Tests

1. **WASM Tests** (`cojson-core-wasm/__test__/`)
   - Test `tryAddFfi` with valid transactions
   - Test error handling for invalid transactions
   - Compare results with existing `tryAdd`

2. **NAPI Tests** (`cojson-core-napi/__test__/`)
   - Same test cases as WASM

3. **React Native Tests** (`cojson-core-rn/src/__tests__/`)
   - Same test cases, using uniffi bindings

### Performance Benchmarks

1. **Micro-benchmarks**
   - Compare `JSON.stringify` + `serde_json::from_str` vs direct FFI passing
   - Measure for single transaction and batch (10, 100, 1000 transactions)

2. **End-to-end benchmarks**
   - Measure `tryAdd` vs `tryAddFfi` latency in realistic scenarios
   - Memory allocation comparison

### TypeScript Tests

1. **Type Safety Tests**
   - Verify TypeScript compiler catches invalid FFI transaction structures
   - Test inline conversion functions in each crypto adapter

2. **Adapter Tests**
   - Verify `SessionLogAdapter.tryAdd` correctly uses FFI path
   - Regression tests to ensure existing behavior is preserved
