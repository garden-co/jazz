# Archived benchmark source

This directory is source material, not an active Criterion benchmark harness.

`realistic_phase1.rs.txt` is the legacy Phase 1 realistic benchmark captured
before the old `jazz-tools::runtime_core::RuntimeCore` stack was retired from
active benchmarking. It imports deep internal modules such as `runtime_core`,
`storage`, `sync_manager`, and `row_histories`, so it is kept with a `.txt`
suffix to make Cargo and readers treat it as archival reference material.

When porting scenarios from this file, rebuild them against the public direct
core API and register the new bench explicitly in `crates/jazz-tools/Cargo.toml`.
