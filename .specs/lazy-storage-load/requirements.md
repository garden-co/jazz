# CoValue Lazy Loading Optimization

**Context**
We receive new content from a client for a CoValue that is not currently loaded in memory. This optimization reduces memory usage and improves response time for load requests, particularly beneficial for sync servers handling many CoValues.

## User Stories

### US-1: Lazy load on load action
> **As a** sync server operator
> **I want** the server to only load `knownState` when receiving load requests
> **So that** memory usage is reduced and response times are faster for CoValues the client already has.

**Acceptance Criteria:**
* When a load action is received for a CoValue that is not in memory:
    * The system shall first load only the `knownState` from storage.
* If the loaded `knownState` indicates the peer already has all content (no new content needed):
    * The system shall reply with a known message without loading the full CoValue.
* If the loaded `knownState` indicates the peer needs new content:
    * The system shall load the full CoValue from storage and send the new content.
* When the CoValue is already available in memory:
    * The system shall behave as before (use the in-memory state).

---

### US-2: Load from storage on handleNewContent
> **As a** sync server operator
> **I want** the server to load CoValues from storage when receiving content for CoValues not in memory
> **So that** the server can properly merge and forward content even after garbage collection.

**Acceptance Criteria:**
* When receiving new content via `handleNewContent` for a CoValue not in memory:
    * The system shall check storage for existing content before processing.
* If storage has the CoValue:
    * The system shall load it from storage before merging the new content.
* If storage does not have the CoValue:
    * The system shall process the new content as a fresh CoValue (current behavior).
* When the CoValue is already available in memory:
    * The system shall behave as before (merge directly).

---

### US-3: Storage API for knownState-only loading
> **As a** developer
> **I want** a storage API method to load only the `knownState` without loading transaction content
> **So that** the lazy loading optimization can be implemented efficiently.

**Acceptance Criteria:**
* The storage API shall provide a method to load only the `knownState` for a given CoValue ID.
* The method shall return the header presence and session counters without loading transaction data.
* The method shall work for both sync and async storage implementations.
* When the CoValue does not exist in storage:
    * The method shall indicate that it was not found.
