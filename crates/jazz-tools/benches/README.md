# Benchmark porting status

This directory contains both active Criterion benches and legacy benchmark source
material while the old `jazz-tools` engine is being replaced.

## Active benches

The active bench harness is the explicit `[[bench]]` list in
`crates/jazz-tools/Cargo.toml`:

- `observer_write_path`
- `direct_core_benchmark`
- `insert_benchmark`
- `update_benchmark`
- `subscription_benchmark`
- `memory_benchmark`

`direct_core_benchmark` is the first direct-core bench. It exercises the new
`jazz_core`/`jazz` facade directly instead of going through the legacy
`jazz-tools::runtime_core::RuntimeCore` stack.

The remaining active benches still use the legacy RuntimeCore helpers under
`benches/common/`. Keep that split visible while porting: direct-core benches are
the target shape, and RuntimeCore benches are compatibility/source-material
benches until they are rewritten or retired.

## Intended next ports

Next ports should prioritize the inactive deep-internal benches before they are
re-enabled:

- `server_authorization_scope_benchmark`
- `realistic_phase1`

Inactive legacy benches should remain in the tree as source material for
scenarios, scale factors, fixtures, and expected measurement coverage. Port them
by rebuilding the scenarios against the public direct-core API rather than by
reviving the old RuntimeCore internals.
