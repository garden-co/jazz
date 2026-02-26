# Lightweight Subscription Delta Protocol (WASM <-> JS) — Tasks

1. Define shared lightweight delta wire types in Rust for binding serialization (`RowDelta`, `Added`, `Removed`, `Updated`) with `protocolVersion = 2`.  
   Ref: Design sections "New Wire Contract: RowDelta" and "Data Models".

2. Implement shared per-subscription index reconstruction in `jazz-tools` (`index_row_delta`) to compute pre/post window indices (`oldIndex`, `newIndex`) from `current_ids`, ensuring same-id updates are not detached/re-appended during reconstruction.  
   Ref: Design sections "Rust-side Delta Classification" and "Shared Serializer Shape Across Bindings".

3. Implement lightweight delta serialization in WASM subscribe callback:
   - emit `removed` without row payload
   - emit `updated` with optional `row` only for content changes
   - emit `added` with row payload  
     Ref: Design sections "New Wire Contract" and "Rust-side Delta Classification".

4. Wire WASM/NAPI/RN bindings to the shared `jazz-tools` index helper instead of runtime-local index implementations, preserving identical semantics across bindings.  
   Ref: Design sections "Rust-side Delta Classification" and "Shared Serializer Shape Across Bindings".

5. Implement lightweight delta serialization in NAPI subscribe callback with the same wire contract behavior as WASM.  
   Ref: Design sections "Shared Serializer Shape Across Bindings" and "Rust-side Delta Classification".

6. Update RN binding serializer to emit the exact `RowDelta` contract (field names and semantics aligned with WASM/NAPI), including optional `row` on `updated`.  
   Ref: Design sections "Shared Serializer Shape Across Bindings" and "Data Models".

7. Update TypeScript driver/runtime delta types to the new `RowDelta` shape and remove legacy heavy-delta assumptions.  
   Ref: Design sections "TypeScript runtime types" and "Compatibility policy".

8. Refactor `SubscriptionManager` to apply deltas via deterministic operation order (`removed` -> `updated` -> `added`) and handle move-only updates without re-transforming row values.  
   Ref: Design sections "TypeScript Delta Application Model" and "ASCII Scenarios".

9. Update/extend TS unit tests for move-only updates, removal shifts, mixed batch behavior, optional `updated.row`, and same-id update index stability.  
   Ref: Design sections "Testing Strategy" and "ASCII Scenarios".

10. Add/extend Rust binding tests to validate serializer output shape, index correctness, move-only compactness, and fallback threshold behavior for WASM, RN, and NAPI.  
    Ref: Design section "Unit Tests (Rust bindings)".

11. Update integration tests to assert that applying `RowDelta` sequences reconstructs the same final result as full query snapshots for ordered/limited subscriptions and mixed operations.  
    Ref: Design section "Integration Tests".

12. Remove remaining in-repo heavy-delta fixtures/usages and ensure all subscription paths (WASM, RN, NAPI) consistently emit/consume `protocolVersion: 2`.  
    Ref: Design sections "Rollout Plan" and "Compatibility policy".
