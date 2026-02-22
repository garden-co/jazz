# Implementation Tasks

## Tasks

- [x] 1. Extend sync message types with `SignatureMismatchErrorMessage` and wire it into the `SyncMessage` union in `/Users/guidodorsi/workspace/jazz/packages/cojson/src/sync.ts` (Design: Protocol Extension).
- [x] 2. Implement server-side signature mismatch response in `handleNewContent` failure path to emit authoritative `content: SessionNewContent[]` for the conflicting `sessionID` (Design: Server Flow).
- [x] 3. Add client-side handling for `SignatureMismatch` errors in sync message processing to trigger conflict recovery for `(coValueId, sessionID)` (Design: Client Recovery Coordinator).
- [x] 4. Implement authoritative content normalization logic (`SessionNewContent[]` continuity validation + canonical flattening + canonical `content` output) in a dedicated recovery utility/module (Design: Client Recovery Coordinator, Data Models).
- [x] 5. Add storage API contract for atomic session replacement (`replaceSessionHistory`) and implement it in both `/Users/guidodorsi/workspace/jazz/packages/cojson/src/storage/storageSync.ts` and `/Users/guidodorsi/workspace/jazz/packages/cojson/src/storage/storageAsync.ts` (Design: Local Storage Rewrite Contract).
- [ ] 6. Implement non-owner recovery path as explicit no-op: detect non-owner mismatch, record state/logging, and return without storage rewrite (Design: Non-owner Recovery).
- [ ] 7. Implement owner recovery path: snapshot local pre-replacement history, replace with normalized authoritative content, compute `base` and `localTail`, and call `coValue.addNewTransactions(localTail)` to re-append/sign local tail (Design: Owner Recovery).
- [ ] 8. Implement per-session recovery state tracking (`idle` → `awaiting-authoritative-payload` → `rewriting-storage` → `rebasing`/`completed`/`failed`) and retry behavior for transient failures (Design: Recovery State Machine).
- [ ] 9. Ensure recovery path integrates with sync state updates (errored/reset/retry) so peers do not get stuck in recurring signature mismatch loops (Design: Server Flow, Client Recovery Coordinator).
- [ ] 10. Add integration tests for server emission of `SignatureMismatch` with `content: SessionNewContent[]` (Design: Testing Strategy #1).
- [ ] 11. Add integration tests for non-owner no-op behavior and no-rebase guarantee (Design: Testing Strategy #2).
- [ ] 12. Add integration tests for owner tail-replay rebase (`replace` + `findCommonPrefixState` + `materializeDelta` + `coValue.addNewTransactions(localTail)`) (Design: Testing Strategy #3).
- [ ] 13. Add integration tests for post-recovery convergence and no repeated mismatch loop under streaming conditions (Design: Testing Strategy #4).
- [ ] 14. Add storage-level tests validating atomicity/idempotency of `replaceSessionHistory`, including interruption/restart scenarios (Design: Testing Strategy #5).
