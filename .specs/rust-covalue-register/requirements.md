# Rust CoValue Register

## Introduction

This feature moves the `CoValueHeader` and `SessionMap` data structures from TypeScript to Rust, using a centralized "Register" pattern. Instead of exposing Rust objects directly to JavaScript via `#[napi(Object)]` or similar bindings (which can make memory management opaque), the Register holds all CoValue-related data in internal `HashMap`s keyed by `RawCoID`. JavaScript interacts with these structures by passing the CoValue ID, and explicit lifecycle methods (`create`, `free`) manage memory.

The goal is to:
1. Improve performance by moving computationally intensive data structures to Rust
2. Maintain clear ownership and memory management semantics
3. Provide a consistent API pattern across different platform bindings (NAPI, WASM, React Native)
4. Keep the JavaScript/TypeScript layer thin, delegating data storage to Rust

## User Stories

### US-1: Register Initialization
**As a** Jazz developer  
**I want** to initialize a CoValue Register when the LocalNode starts  
**So that** I have a centralized place to store CoValue headers and session maps in Rust

**Acceptance Criteria:**
- **When** a LocalNode is created, **the system shall** create a new Register instance
- **The system shall** allow creating a Register with an optional initial capacity hint
- **The system shall** use simple ownership semantics (no thread-safety primitives needed for single-threaded JS runtime)

### US-2: Header Registration
**As a** Jazz developer  
**I want** to store and retrieve `CoValueHeader` data in Rust  
**So that** header validation and storage is handled efficiently

**Acceptance Criteria:**
- **When** a new CoValue header is received, **the system shall** allow storing it in the Register by `RawCoID`
- **The system shall** validate the header structure before storage
- **The system shall** return an error if a header for the same `RawCoID` already exists (unless explicitly overwriting)
- **When** requested, **the system shall** return the header data for a given `RawCoID`
- **The system shall** return `null`/`None` if no header exists for the requested `RawCoID`
- **The system shall** support querying whether a header exists for a given `RawCoID`

### US-3: SessionMap Registration
**As a** Jazz developer  
**I want** to store and manage `SessionMap` data in Rust  
**So that** session management and transaction verification is handled efficiently

**Acceptance Criteria:**
- **When** a CoValue is created or loaded, **the system shall** allow creating a SessionMap entry in the Register
- **The system shall** associate each SessionMap with its parent `RawCoID`
- **When** requested, **the system shall** return references/handles to SessionMap data
- **The system shall** support all existing SessionMap operations through ID-based method calls:
  - Adding transactions (private and trusting)
  - Decrypting transaction changes and metadata
  - Managing known state
  - Cloning session data

### US-4: Memory Management via Free
**As a** Jazz developer  
**I want** explicit control over when CoValue data is freed from the Register  
**So that** I can prevent memory leaks and manage the lifecycle of CoValues

**Acceptance Criteria:**
- **The system shall** provide a `free(id: RawCoID)` method to remove all data for a CoValue
- **When** `free` is called, **the system shall** remove both the header and SessionMap for that ID
- **The system shall** handle `free` calls for non-existent IDs gracefully (no-op or return false)
- **The system shall** support a `freeAll()` method to clear all entries (for shutdown/cleanup)
- **The system shall** provide a `size()` or `count()` method to query the number of registered CoValues

### US-5: Cross-Platform Binding Support
**As a** Jazz developer  
**I want** the Register to work across all platform bindings (NAPI, WASM, React Native)  
**So that** the implementation is consistent regardless of runtime environment

**Acceptance Criteria:**
- **The system shall** implement the Register in `cojson-core` as the core Rust crate
- **The system shall** expose bindings in `cojson-core-napi` for Node.js
- **The system shall** expose bindings in `cojson-core-wasm` for browser/WASM
- **The system shall** expose bindings in `cojson-core-rn` for React Native
- **The system shall** use the same API shape across all bindings

### US-6: Integration with Existing SessionLog
**As a** Jazz developer  
**I want** the Register to integrate with the existing `SessionLog` implementation  
**So that** I can leverage existing Rust cryptographic operations

**Acceptance Criteria:**
- **The system shall** reuse `SessionLogInternal` from `cojson-core` for session-level operations
- **The system shall** maintain compatibility with existing `SessionLog` NAPI bindings during transition
- **The system shall** support gradual migration from the current TypeScript-based `SessionMap` to the Rust-based version

### US-7: Known State Management
**As a** Jazz developer  
**I want** to manage CoValue known state through the Register  
**So that** sync operations can query and update known state efficiently

**Acceptance Criteria:**
- **The system shall** track `knownState` (header presence + session transaction counts) per CoValue
- **The system shall** support `knownStateWithStreaming` for in-progress streaming operations
- **The system shall** provide methods to query immutable snapshots of known state
- **The system shall** invalidate and update known state caches when transactions are added

### US-8: Header Type Definitions
**As a** Jazz developer  
**I want** the Rust `CoValueHeader` to match the TypeScript definition  
**So that** data can be serialized/deserialized correctly between layers

**Acceptance Criteria:**
- **The system shall** support the following header fields:
  - `type`: CoValue type (e.g., "comap", "colist", "costream", "coPlainText")
  - `ruleset`: Permission rules definition (`{ type: "group" }` or `{ type: "ownedByGroup", group: RawCoID }`)
  - `meta`: Optional JSON object for metadata
  - `uniqueness`: Uniqueness value (string | boolean | null | object)
  - `createdAt`: Optional timestamp string
- **The system shall** serialize headers to JSON compatible with the TypeScript format
- **The system shall** validate uniqueness values according to existing rules:
  - Allowed: string, boolean, null, integer, object with string values
  - Rejected: arrays, floating-point numbers

### US-9: Batch Query APIs
**As a** Jazz developer  
**I want** batch/bulk APIs for querying transaction data  
**So that** I can retrieve all needed data in a single FFI call without per-item overhead

**Acceptance Criteria:**
- **The system shall** provide `getContentSince(id, knownState) -> ContentMessage[]` to generate sync messages in one call
- **The system shall** provide `getValidTransactionsSorted(id, options) ->  Transaction[]` to retrieve all valid transactions
- **The system shall** provide `getKnownState(id) -> KnownState` to retrieve the full known state
- **The system shall NOT** require iterating over sessions/transactions via individual FFI calls
- **When** algorithms require iteration (e.g., `newContentSince`), **the system shall** implement the entire algorithm in Rust and return the final result

### US-10: Performance Benchmarking
**As a** Jazz developer  
**I want** benchmarks that compare performance before and after the migration  
**So that** I can verify there is no performance regression

**Acceptance Criteria:**
- **The system shall** include benchmarks for:
  - Cold loading a CoValue with N transactions (N = 10, 100, 1000)
  - Processing new transactions (throughput: transactions/second)
  - Building content views (`getCurrentContent()` latency)
  - Generating sync messages (`newContentSince()` latency)
- **The system shall** run benchmarks on both the TypeScript baseline and Rust implementation
- **The system shall** fail CI if any benchmark shows >5% regression
- **The system shall** document measured improvements in the PR description

## Non-Functional Requirements

### NFR-1: No Performance Regression
**Critical**: This migration shall NOT introduce any performance regression. Moving data to Rust does not automatically improve performance—FFI overhead can make things significantly slower if not designed carefully.

#### FFI Overhead Constraints
- **The system shall NOT** require per-transaction FFI calls during iteration
- **The system shall** expose batch/bulk APIs that return complete result sets in a single FFI call
- **The system shall** minimize JS↔Rust boundary crossings for hot paths

#### Hot Path Performance Requirements
The following operations are performance-critical and shall not regress:

| Operation | Current Behavior | Requirement |
|-----------|------------------|-------------|
| `loadVerifiedTransactionsFromLogs()` | Iterates sessions/transactions in JS | Shall not require per-item FFI calls |
| `getValidTransactions()` | Returns array for JS iteration | Batch return, single FFI call |
| `newContentSince()` | Iterates to build content messages | Move entire algorithm to Rust OR keep in JS |
| `processNewTransactions()` (CoMap/CoList) | Iterates transactions for content building | Transaction data must be accessible without per-item FFI |
| `determineValidTransactions()` | Validates permissions per transaction | Batch validation in Rust acceptable |

#### API Design Principles
- **Prefer batch operations**: `getValidTransactionsSorted(id, options) -> Transaction[]` over `getTransaction(id, sessionId, idx)`
- **Move algorithms, not just data**: If data moves to Rust, the algorithms that iterate over it should also move
- **Cache in JS when needed**: For frequent iteration patterns, allow JS to hold cached copies of transaction data

#### Benchmarking Requirements
- **The system shall** include benchmarks comparing before/after performance for:
  - CoValue loading (cold start)
  - Transaction processing throughput
  - Content building (`getCurrentContent()`)
  - Sync message generation (`newContentSince()`)
- **The system shall** demonstrate no more than 5% regression in any benchmark
- **The system shall** show measurable improvement in at least one benchmark to justify the migration

### NFR-2: Performance Baselines
- Header lookups shall be O(1) average case
- SessionMap operations shall maintain current performance characteristics
- Memory overhead per CoValue shall not exceed 20% compared to the current TypeScript implementation

### NFR-3: Safety
- The Register shall prevent use-after-free scenarios
- The Register shall use simple ownership semantics (single-threaded JS runtime, no concurrent access)

### NFR-4: Backwards Compatibility
- The migration shall be incremental; the TypeScript layer shall continue to work during transition
- Existing tests shall pass without modification to their public API usage
