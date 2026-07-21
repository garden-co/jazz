# Edge Fate Authority Findings

## Normative Spec Notes

From `crates/jazz/SPEC/9_topology_edge.md`:

> Fate authority is a host-wired role, not a property inferred from data or node contents.

> mergeable fate authority | first upstream trusted edge; edge-final for edge-accepted mergeables (`INV-EDGE-8`)

> Edge acceptance is therefore not the same as global durability: only an observed `DurabilityTier::Global` means the write reached core/global durability (`INV-EDGE-11`, ch. 3).

From `crates/jazz/SPEC/3_transactions.md`:

> Fate authority is **structural**. A node acts as fate authority exactly when the host wires it as one: the core accept path for global authority, or the edge-authority ingest entry point for edge-decided mergeable fates.

> When an edge authority has already accepted a mergeable transaction, the core finalizes it by stamping the next `GlobalSeq` and `DurabilityTier::Global`; it does not re-judge write-policy authorization or the merge outcome (`INV-EDGE-8`).

From `crates/jazz/SPEC/8_sync_protocol.md`:

> Receiving a bare unfated commit unit is not authority. On a non-authority node, `apply_sync_message` stages or parks that commit unit as pending remote fate and waits for a `FateUpdate`; it must not accept the unit, assign global sequence, or create merge versions from it (`INV-TX-23`).

> View updates carry **accepted/settled state only** -- pending versions are visible only on the creating node and are never emitted to non-origin peers (`INV-SYNC-12`).

## Confirmed Failure

New black-box server-shell regression:

- `cargo test -p jazz-server --test edge_fate_authority edge_shell_does_not_report_global_or_serve_global_before_core_ack -j 2`
- Failing-before-fix exit code: `101`
- Failure: Alice's write satisfied `wait(DurabilityTier::Global)` after only the edge shell tick, before core was driven.

Current focused result:

- `cargo test -p jazz-server --test edge_fate_authority -j 2` exit code: `0`
- The edge reports Alice's upload as `Accepted` at `DurabilityTier::Edge`.
- Bob's pre-existing `Global` subscription on the same edge does not see Alice's row before core confirmation.
- The same upload through a `NodeRole::Core` shell still reports `Accepted`/`Global` immediately.

## Review Lesson

The first fix was over-broad: it used `PeerRole::EdgeClient` as the authority discriminator. That is wrong because `PeerState::for_author` is the compatibility spelling for `edge_client`, so ordinary client links on core servers also have `PeerRole::EdgeClient`. `crates/jazz-server/tests/cli_dry_run.rs::server_command_loads_published_schema_and_persists_ws_data_across_restart` correctly caught this by exercising a core-role durable server: client uploads there must keep the generic core authority path and settle at `Global`.

## Wiring Diff Summary

- `crates/jazz-server/src/lib.rs`
  - `NodeRole::Edge` session links now call the DB edge-authority subscriber admission helper.
  - `NodeRole::Core` session links continue to use the generic subscriber admission path.

- `crates/jazz/src/db.rs`
  - `CommitUnitIngestContext` now carries an `edge_authority` bit supplied by host/server wiring.
  - Subscriber `CommitUnit` handling calls edge ingest only when `edge_authority == true` and the peer is a client link.
  - Edge-client exclusive uploads are stored/forwarded as relay-pending units instead of edge-accepted units.
  - Outbox pruning now retains accepted edge-tier uploads until they reach `Global`.

- `crates/jazz/src/node/mod.rs`
  - `CommitUnitIngestContext` includes the host-wired `edge_authority` discriminator.

- `crates/jazz-server/tests/edge_fate_authority.rs`
  - Added the server-shell regression covering client A -> edge -> core and client B subscribed globally on the same edge.
  - Added the core-role discriminator test proving the same client upload on a core shell still reaches `Global`.

## 🔶 Candidates / Ambiguities

- 🔶 Core promotion propagation is still underspecified at the wire level. The spec says core finalizes edge-accepted mergeables with `GlobalSeq`/`Global`, but the current wire `CommitUnit` does not carry an explicit "edge-accepted" marker. The focused regression verifies the pre-core gating bug Nico reported; a fuller follow-up should pin the exact edge-to-core finalization signal and assert Alice/Bob observe global after core ack.
- 🔶 Remote `Edge`-tier subscription serving is still not enabled by the current DB sync contract: existing tests require edge/local live subscriptions to request global upstream coverage and require subscriber serving to reject non-global register-shape options. The "edge-tier subscriber sees pre-core data" assertion therefore remains a candidate for a later, explicit subscription-tier design change rather than part of this authority-wiring fix.

## Gate Table

| Gate | Result |
| --- | --- |
| `cargo test -p jazz-server --test edge_fate_authority edge_shell_does_not_report_global_or_serve_global_before_core_ack -j 2` before fix | failed, exit `101` |
| `cargo test -p jazz-server --test edge_fate_authority -j 2` | passed, exit `0` |
| `cargo test -p jazz-server -j 2` | passed, exit `0` |
| `cargo test -p jazz -j 2` | passed, exit `0` |
| `cargo test -p jazz --test four_tier -j 2` | passed, exit `0` |
| `cargo test -p jazz --test incremental_delivery_canary -j 2` | passed, exit `0` |
| `cargo test -p groove -j 2` | passed, exit `0` |
| `JAZZ_SEED_COUNT=100 cargo test -p jazz m3_maintained_one_shot_differential_oracle -j 2` | passed, exit `0` |
| `cargo check -p jazz-sim --benches -j 2` | passed, exit `0` |
| `cargo fmt -p jazz -p jazz-server` | passed, exit `0` |
| `cargo fmt --check -p jazz -p jazz-server` | passed, exit `0` |
