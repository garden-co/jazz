// Catalogue and migration-lens behavior, grouped by the durable boundary under test.

include!("snapshots.rs");
include!("lineage.rs");
include!("physical_storage.rs");
include!("replication.rs");
include!("winner_projection.rs");
include!("runtime_catalogue.rs");
include!("lens_projection.rs");
include!("enum_projection.rs");
include!("projected_reads.rs");
include!("wire_identity.rs");
