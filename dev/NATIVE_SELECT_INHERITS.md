# Native Select Inherits Findings

## Diff Summary

- `append_inherited_policy` now converts inherited policies to native core inherits with `query.inherits_operation(via_column, ...)` after validating the inherited FK and source policy.
- Select-specific expansion mode plumbing was removed from policy conversion.
- The old forward Select expansion helpers remain in place as dead fallback pending removal.

## Updated Tests And Fixtures

- `crates/jazz-tools/src/server/public_schema_convert.rs::preserves_unbounded_inherited_select_as_native_atom`: updated from expanded parent join assertions to native Select inherit assertions.
- `crates/jazz-tools/src/server/public_schema_convert.rs::preserves_inherited_select_branch_parent_policy_as_native_atom`: updated from expanded branch/join assertions to native Select inherit assertions.
- `crates/jazz-tools/src/server/public_schema_convert.rs::preserves_nested_inherited_select_branch_parent_policy_as_native_atom`: updated from expanded branch/join assertions to native Select inherit assertions.
- Fixtures: none changed. No schema-hash or catalogue fixture updates were required by the exercised repo conventions.

## Native Conversion Shapes

- No Select-inherits shape failed native conversion or validation during the requested gate set.

## Gates

| Gate | Exit Code |
| --- | --- |
| `cargo test -p jazz-tools --features test -j 2` | 0 |
| `cargo test -p jazz -j 2` | 0 |
| `cargo test -p jazz-server -j 2` | 0 |
| `JAZZ_SEED_COUNT=100 cargo test -p jazz m3_maintained_one_shot_differential_oracle -j 2` | 0 |
| `cargo test -p jazz --test incremental_delivery_canary -j 2` | 0 |
| `cargo fmt -p jazz-tools --check` | 0 |
| `cargo check -p jazz-sim --benches -j 2` | 0 |
| `cargo build --release -p jazz-tools --bin jazz-tools --features cli -j 2` | 0 |

Release binary: `target/release/jazz-tools`.
