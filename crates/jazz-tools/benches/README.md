# Benchmark porting status

This directory contains both active Criterion benches and legacy benchmark source
material while the old `jazz-tools` engine is being replaced.

## Active benches

The active bench harness is the explicit `[[bench]]` list in
`crates/jazz-tools/Cargo.toml`:

- `observer_write_path`
- `direct_core_benchmark`
- `direct_authorization_scope_benchmark`
- `insert_benchmark`
- `update_benchmark`
- `subscription_benchmark`
- `memory_benchmark`

All active Criterion benches except `memory_benchmark` now exercise the new
`jazz_core`/`jazz` facade directly instead of going through the legacy
`jazz-tools::runtime_core::RuntimeCore` stack.

`memory_benchmark` still uses the legacy RuntimeCore helpers under
`benches/common/` because it reports internal SyncManager/QueryManager memory
breakdowns that the direct facade does not expose yet. Keep that split visible
while porting: direct-core benches are the target shape, and RuntimeCore benches
are compatibility/source-material benches until they are rewritten or retired.

Two direct ports intentionally measure the nearest direct-core semantics rather
than old helper behavior:

- `insert_benchmark` models team/folder authorization as a folder-access join
  policy instead of old `INHERITS SELECT VIA folder_id` session recursion.
- `subscription_benchmark` consumes one direct subscription delta per insert in
  the batch case; old RuntimeCore tick coalescing produced one larger callback.

## Intended next ports

Next ports should prioritize the inactive deep-internal benches before they are
re-enabled:

- `realistic_phase1`

Inactive legacy benches should remain in the tree as source material for
scenarios, scale factors, fixtures, and expected measurement coverage. Port them
by rebuilding the scenarios against the public direct-core API rather than by
reviving the old RuntimeCore internals.
