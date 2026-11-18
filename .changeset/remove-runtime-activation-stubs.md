---
"jazz-tools": patch
---

Rust: remove `CatalogueChange::activate` (a catalogue change now only puts rows and admits versions; a server already refused it), and the unused runtime schema-activation stubs from `jazz::db::Db` (`activate_catalogue_schema_for_test`, `author_schema_lineage_publication`, `publish_schema_with_lens`, `activate_schema_at_revision`) and the unused `protocol::CurrentWriteSchema`. A deployment's schema is installed when its store opens; no stored or wire bytes change.
