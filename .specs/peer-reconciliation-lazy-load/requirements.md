# Peer Reconciliation Lazy Load Optimization

## Introduction

When a client reconnects to a server peer, the `startPeerReconciliation` method iterates through all local CoValues and sends `LOAD` requests to synchronize state. Currently, for CoValues that are "unavailable" (lazy-loaded - known to the node but not loaded into memory), the system sends load requests with an **empty known state** (`sessions: {}`).

This is problematic because:
1. The server interprets an empty known state as "client has nothing"
2. The server sends **all content** for that CoValue
3. But the client may already have this data in local storage - it just hasn't loaded it into memory yet

This causes unnecessary bandwidth usage and server load, especially for applications with many lazy-loaded CoValues that reconnect frequently.

## User Stories

### US-1: Check local storage before sending load requests during reconciliation

> **As a** Jazz application user  
> **I want** the client to check local storage for lazy-loaded CoValues during peer reconciliation  
> **So that** reconnections don't cause unnecessary data transfer when I already have the data locally.

**Acceptance Criteria:**

- **When** peer reconciliation runs for an unavailable (lazy-loaded) CoValue:
  - The system **shall** first check local storage for the CoValue's known state.
- **If** the CoValue exists in local storage:
  - The system **shall** send the load request with the known state from storage (not empty).
- **If** the CoValue does not exist in local storage:
  - The system **shall** send the load request with empty known state (current behavior).
- **When** the CoValue is already available in memory:
  - The system **shall** behave as before (send current in-memory known state).

---

### US-2: Maintain dependency ordering during async storage checks

> **As a** Jazz developer  
> **I want** peer reconciliation to maintain correct dependency ordering even with async storage checks  
> **So that** CoValues are still synced in the correct order (dependencies before dependents).

**Acceptance Criteria:**

- **When** sending load requests during peer reconciliation:
  - The system **shall** preserve the dependency ordering (dependencies sent before dependents).
- **If** storage checks are async:
  - The system **shall** not block the entire reconciliation process.
  - The system **shall** ensure load requests are sent in dependency order within their category.

---

### US-3: Handle concurrent reconciliation and storage loads gracefully

> **As a** Jazz developer  
> **I want** the system to handle race conditions between reconciliation and ongoing storage operations  
> **So that** the sync state remains consistent.

**Acceptance Criteria:**

- **If** a CoValue becomes available in memory while waiting for storage check:
  - The system **shall** use the in-memory known state instead.
- **If** multiple peer reconnections happen concurrently:
  - The system **shall** not send duplicate or conflicting load requests.
- **When** storage check fails:
  - The system **shall** fall back to current behavior (empty known state) and log a warning.
