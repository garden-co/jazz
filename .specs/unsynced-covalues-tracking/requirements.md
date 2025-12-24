# Track Unsynced CoValues & Resume Sync

## Introduction

Currently, Jazz only resumes sync on CoValues that are loaded in memory. This creates a problem when users partially upload a CoValue graph, go offline, restart the app and then come back online. In this scenario, only the loaded parts of the CoValue graph resume syncing, leaving unloaded parts unsynced even though they may have pending changes.

This feature will implement automatic tracking and resumption of sync for all CoValues with pending changes, regardless of whether they are currently loaded in memory. The solution must work across all platforms (web, React Native, Node.js, cloud workers) and be performant enough to run in cloud environments.

Tracking unsynced CoValues also allows providing reactive APIs for tracking if CoValues have been synced. This includes refactoring `waitForSync` so that we don't need to create a subscription, and adding new subscription APIs for monitoring sync state at different levels of granularity: `CoValueCore.subscribeToSyncStatus` and `SyncManager.subscribeToSyncStatus`.

## User Stories

### US1: Track Unsynced CoValues
**As a** Jazz user  
**I want** Jazz to automatically track CoValues that have unsynced changes  
**So that** these CoValues can be synced even if they're not currently loaded in memory

**Acceptance Criteria:**
- [ ] When a CoValue has local changes that haven't been fully uploaded to at least one peer, it is tracked as unsynced
- [ ] When a CoValue becomes fully synced to all peers, it is removed from the unsynced tracking
- [ ] The tracking persists across app restarts using platform-appropriate storage

### US2: Resume Sync on App Start
**As a** Jazz user  
**I want** Jazz to automatically resume syncing all unsynced CoValues when the app starts  
**So that** all pending changes are eventually synced, even if the CoValues aren't loaded after going back online

**Acceptance Criteria:**
- [ ] On LocalNode initialization, all previously tracked unsynced CoValues are loaded and syncing is resumed
- [ ] The resumption happens asynchronously and doesn't block LocalNode initialization
- [ ] The resumption is efficient and doesn't cause performance issues when running in sync servers

### US3: Subscribe to CoValue Sync Status
**As a** Jazz user  
**I want** to subscribe to changes in a CoValue's sync status  
**So that** I can reactively monitor when a CoValue becomes synced

**Acceptance Criteria:**
- [ ] `CoValueCore.subscribeToSyncStatus(listener)` method subscribes to sync status changes
- [ ] The listener receives a boolean indicating if the CoValue is synced to all peers
- [ ] The method returns an unsubscribe function
- [ ] The subscription uses the unsynced CoValues tracking for efficient updates
- [ ] The listener is called immediately with the current sync status when subscribing

### US4: Subscribe to All CoValues Sync Status
**As a** Jazz user  
**I want** to subscribe to changes in whether all CoValues are synced  
**So that** I can reactively determine when all pending changes have been uploaded

**Acceptance Criteria:**
- [ ] `SyncManager.subscribeToSyncStatus(listener)` method subscribes to global sync status changes
- [ ] The listener receives a boolean indicating if all CoValues are synced
- [ ] The method returns an unsubscribe function
- [ ] The subscription uses the unsynced CoValues tracking for efficient implementation
- [ ] The subscription works correctly even when some CoValues are not loaded in memory
- [ ] The listener is called immediately with the current sync status when subscribing

### US5: Refactor waitForSync Without Subscriptions
**As a** Jazz developer  
**I want** `waitForSync` to work without creating subscriptions  
**So that** it's simpler and more efficient

**Acceptance Criteria:**
- [ ] `CoValueCore.waitForSync` is refactored to use unsynced CoValues tracking instead of CoValue subscriptions
- [ ] The method maintains backward compatibility with existing API
- [ ] Performance is improved compared to the subscription-based approach
