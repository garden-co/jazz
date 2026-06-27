# Archived benchmark notes

This directory no longer retains old `RuntimeCore`/`SchemaManager`/`SyncManager`
benchmark source. The active Criterion harnesses live one level up and are
registered explicitly in `crates/jazz-tools/Cargo.toml`.

When reintroducing old measurement intent, rebuild it against the public
direct-core API and register the new bench explicitly in
`crates/jazz-tools/Cargo.toml`.
