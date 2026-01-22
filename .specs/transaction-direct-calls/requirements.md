# Transaction Direct Calls - Requirements

## Introduction

This feature optimizes the `tryAdd` method by replacing JSON serialization with direct FFI calls. Instead of serializing transaction objects to JSON in JavaScript and parsing them in Rust, the `tryAdd` method now iterates over transactions and passes primitive parameters directly across the FFI boundary.

### Background

The previous `tryAdd` implementation required:
1. JavaScript to serialize each transaction to JSON (`JSON.stringify`)
2. Pass the JSON strings to Rust
3. Rust parses the JSON back into transaction objects

This introduced overhead from JSON serialization/deserialization at the FFI boundary.

### Solution

The optimized `tryAdd` implementation:
1. Iterates over transactions in TypeScript
2. For each transaction, calls `addExistingPrivateTransaction` or `addExistingTrustingTransaction` with primitive parameters
3. Calls `validateSignature` at the end (unless `skipVerify` is true)

This approach:
- Eliminates `JSON.stringify` overhead on the JS side
- Uses a staging area in Rust to maintain atomicity
- Is approximately **2.7x faster** than the previous approach

## User Stories

### US-1: Optimized tryAdd

**As a** developer using the Jazz sync system,  
**I want** the `tryAdd` method to be optimized for performance,  
**So that** syncing large batches of transactions is faster.

#### Acceptance Criteria (EARS Format)

- **When** `tryAdd` is called with a list of transactions,  
  **the system shall** iterate over each transaction and call the appropriate direct method.

- **When** a transaction has `privacy === "private"`,  
  **the system shall** call `addExistingPrivateTransaction` with the transaction's fields.

- **When** a transaction has `privacy === "trusting"`,  
  **the system shall** call `addExistingTrustingTransaction` with the transaction's fields.

- **When** `skipVerify` is false,  
  **the system shall** call `validateSignature` after adding all transactions.

- **When** `skipVerify` is true,  
  **the system shall** skip signature validation (for trusted sources).

### US-2: Atomicity

**As a** developer using the Jazz sync system,  
**I want** the `tryAdd` operation to be atomic,  
**So that** if signature validation fails, no transactions are committed.

#### Acceptance Criteria (EARS Format)

- **When** `validateSignature` succeeds,  
  **the system shall** commit all pending transactions to the session log.

- **When** `validateSignature` fails,  
  **the system shall** discard all pending transactions and throw an error.

- **When** an error occurs during transaction addition,  
  **the system shall** clear all pending transactions and propagate the error.

### US-3: Platform Support

**As a** developer using Jazz on different platforms (Node.js, Browser, React Native),  
**I want** the optimized `tryAdd` to work across all platforms,  
**So that** I get consistent performance improvements regardless of the platform.

#### Acceptance Criteria (EARS Format)

- **When** using the NAPI binding (Node.js),  
  **the system shall** use `BigInt` for `madeAt` timestamps.

- **When** using the WASM binding (Browser),  
  **the system shall** use `bigint` for `madeAt` timestamps.

- **When** using the Uniffi binding (React Native),  
  **the system shall** use `bigint` for `madeAt` timestamps.

## Non-Functional Requirements

### NFR-1: Performance

- The optimized `tryAdd` **shall** be at least **2x faster** than the JSON.stringify approach.

### NFR-2: Backward Compatibility

- The `tryAdd` method signature **shall** remain unchanged.
- Existing callers **shall** not require any modifications.

### NFR-3: Type Safety

- The `madeAt` timestamp **shall** use `bigint` to avoid floating-point precision issues.
