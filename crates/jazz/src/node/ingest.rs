//! Commit, fate, and sync-message ingestion for a storage-backed
//! node. This module owns mutation paths that validate incoming transactions,
//! apply authority fates, park/unpark causally blocked units, and write node
//! state into groove; read-only global derivations live in [`super::global_state`],
//! policy evaluation in [`super::policy`], and byte-level record construction in
//! [`super::codec`]. It is the node layer's write side below the `Db` facade and
//! protocol sync loop. Trusted catalogue snapshot activation lives in the
//! sibling [`super::catalogue_ingest`] module.

use super::*;
use crate::protocol::{CatalogueAck, LensOp, SchemaLineagePublication, VersionBundleRef};
use crate::protocol_limits::{
    commit_unit_limit_violation, validate_known_state_declaration, validate_shape_registration_size,
};
use crate::schema::ColumnSchema;

pub(super) const MAX_SCHEMA_LINEAGE_DECLARATIONS: usize = 4096;
pub(super) const MAX_SCHEMA_LINEAGE_NAME_BYTES: usize = 1024;
pub(super) const MAX_SCHEMA_LINEAGE_OPS: usize = 16_384;

fn authority_wall_clock_ms() -> Result<u64, Error> {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map_err(|_| Error::InvalidStoredValue("authority clock precedes Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| Error::InvalidStoredValue("authority clock exceeds u64 milliseconds"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CommitUnitParkMode {
    ingest_context: Option<CommitUnitIngestContext>,
    ingress_role: ParkedIngressRole,
}

impl Default for CommitUnitParkMode {
    fn default() -> Self {
        Self {
            ingest_context: None,
            ingress_role: ParkedIngressRole::Authority,
        }
    }
}

include!("ingest/catalogue.rs");
include!("ingest/commit_bundles.rs");
include!("ingest/fates.rs");
include!("ingest/view_updates.rs");
include!("ingest/validation.rs");

/// A sequence is the global-authority receipt. Peer payloads which pair it
/// with a weaker durability must be rejected before they can reach storage.
pub(super) fn validate_received_fate_update_global_time_durability(
    global_time: Option<GlobalTime>,
    durability: Option<DurabilityTier>,
) -> Result<(), Error> {
    if global_time.is_some() && durability != Some(DurabilityTier::Global) {
        return Err(Error::UnsupportedSyncMessage(
            "global timestamp requires Global durability",
        ));
    }
    Ok(())
}

/// View bundles are peer payloads too, including reset bundles eligible for
/// bulk persistence.
pub(super) fn validate_received_view_bundle_global_time_durability(
    global_time: Option<GlobalTime>,
    durability: DurabilityTier,
) -> Result<(), Error> {
    if global_time.is_some() && durability != DurabilityTier::Global {
        return Err(Error::MalformedViewUpdate(
            "global timestamp requires Global durability",
        ));
    }
    Ok(())
}

fn validate_transform_column(column: Option<&ColumnSchema>, transform: &str) -> Result<(), Error> {
    validate_registered_transform(transform)?;
    let Some(_) = column else {
        return Err(Error::InvalidCatalogueUpdate("transform column is unknown"));
    };
    Ok(())
}

fn fate_update_durability_claim(fate: &Fate, durability: DurabilityTier) -> Option<DurabilityTier> {
    match fate {
        Fate::Rejected(_) => None,
        Fate::Pending | Fate::Accepted => Some(durability),
    }
}

fn commit_unit_write_count_matches(tx: &Transaction, version_count: usize) -> bool {
    usize::try_from(tx.n_total_writes) == Ok(version_count)
}

/// A content row whose global current state was captured before a Core
/// decision, for the post-acceptance linear fold check.
pub(super) struct FoldCandidate {
    table: String,
    branch_key: BranchKey,
    row_uuid: RowUuid,
    previous: Option<VersionRow>,
}
