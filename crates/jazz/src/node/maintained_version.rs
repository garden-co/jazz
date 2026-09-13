//! A maintained proof names a native version; it is not another row store.

use super::codec::{VersionLayer, VersionRow};
use crate::ids::{NodeAlias, NodeUuid, PhysicalTableId, RowUuid, SchemaVersionAlias};
use crate::protocol::BranchKey;
use crate::time::TxTime;
use crate::tx::{DeletionEvent, TxId};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Only a resolver-owned native source may produce this reference. Its physical
/// table coordinate is bound at compilation, never recovered from a row UUID or
/// an ambiguous logical table label. These references are process-local.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct NativeVersionRef {
    pub(crate) physical_table: PhysicalTableId,
    pub(crate) table: groove::Intern<String>,
    pub(crate) branch: BranchKey,
    pub(crate) row: RowUuid,
    pub(crate) time: TxTime,
    pub(crate) node: NodeAlias,
    pub(crate) schema: SchemaVersionAlias,
    pub(crate) deletion: Option<DeletionEvent>,
}

impl NativeVersionRef {
    pub(crate) fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.table.len()
            + self
                .branch
                .values
                .iter()
                .map(|(name, value)| {
                    std::mem::size_of_val(&(name, value)) + name.len() + value.0.len()
                })
                .sum::<usize>()
    }
}

/// Materialized versions have no native-source proof. In particular, inline
/// synthetic merge output must retain its complete payload and byte identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MaintainedVersion {
    Native(Arc<NativeVersionRef>),
    Materialized(VersionRow),
}

impl From<VersionRow> for MaintainedVersion {
    fn from(row: VersionRow) -> Self {
        Self::Materialized(row)
    }
}

impl MaintainedVersion {
    pub(crate) fn table_intern(&self) -> groove::Intern<String> {
        match self {
            Self::Native(v) => v.table,
            Self::Materialized(v) => v.table,
        }
    }
    pub(crate) fn table(&self) -> &str {
        match self {
            Self::Native(v) => v.table.as_str(),
            Self::Materialized(v) => v.table(),
        }
    }
    pub(crate) fn row_uuid(&self) -> RowUuid {
        match self {
            Self::Native(v) => v.row,
            Self::Materialized(v) => v.row_uuid(),
        }
    }
    pub(crate) fn tx_time(&self) -> TxTime {
        match self {
            Self::Native(v) => v.time,
            Self::Materialized(v) => v.tx_time(),
        }
    }
    pub(crate) fn tx_node_alias(&self) -> NodeAlias {
        match self {
            Self::Native(v) => v.node,
            Self::Materialized(v) => v.tx_node_alias(),
        }
    }
    pub(crate) fn schema_version_alias(&self) -> SchemaVersionAlias {
        match self {
            Self::Native(v) => v.schema,
            Self::Materialized(v) => v.schema_version_alias(),
        }
    }
    pub(crate) fn branch_key(&self) -> &BranchKey {
        match self {
            Self::Native(v) => &v.branch,
            Self::Materialized(v) => v.branch_key(),
        }
    }
    pub(crate) fn deletion(&self) -> Option<DeletionEvent> {
        match self {
            Self::Native(v) => v.deletion,
            Self::Materialized(v) => v.deletion(),
        }
    }
    pub(super) fn layer(&self) -> VersionLayer {
        if self.deletion().is_some() {
            VersionLayer::Deletion
        } else {
            VersionLayer::Content
        }
    }
    pub(crate) fn tx_id(&self, aliases: &BTreeMap<NodeUuid, NodeAlias>) -> Option<TxId> {
        aliases.iter().find_map(|(node, alias)| {
            (*alias == self.tx_node_alias()).then(|| TxId::new(self.tx_time(), *node))
        })
    }
    pub(crate) fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::Native(v) => v.retained_bytes(),
                Self::Materialized(v) => v.table().len() + v.record.raw().len(),
            }
    }
}

impl<S: groove::storage::OrderedKvStorage> super::NodeState<S> {
    pub(crate) fn maintained_version_physical_table(
        &self,
        version: &MaintainedVersion,
    ) -> Result<PhysicalTableId, super::Error> {
        match version {
            MaintainedVersion::Native(v) => Ok(v.physical_table),
            MaintainedVersion::Materialized(v) => self.physical_table_id_for_version(v),
        }
    }

    pub(crate) async fn materialize_maintained_version(
        &mut self,
        version: &MaintainedVersion,
    ) -> Result<VersionRow, super::Error> {
        match version {
            MaintainedVersion::Materialized(v) => Ok(v.clone()),
            _ => self.resolve_maintained_version(version).await,
        }
    }

    pub(crate) fn maintained_version_tx_id(
        &self,
        version: &MaintainedVersion,
    ) -> Result<TxId, super::Error> {
        let node = self.node_for_alias(version.tx_node_alias()).ok_or(
            super::Error::InvalidStoredValue("native witness node alias missing"),
        )?;
        Ok(TxId::new(version.tx_time(), node))
    }

    pub(crate) async fn resolve_maintained_version(
        &mut self,
        version: &MaintainedVersion,
    ) -> Result<VersionRow, super::Error> {
        match version {
            MaintainedVersion::Materialized(row) => {
                self.canonical_history_version_for_maintained_witness(row)
                    .await
            }
            MaintainedVersion::Native(native) => {
                let tx = self.maintained_version_tx_id(version)?;
                let coordinate = super::codec::ParentCoordinate {
                    physical_table_id: native.physical_table,
                    branch_key: native.branch.clone(),
                    row_uuid: native.row,
                    layer: version.layer(),
                };
                let row = self
                    .query_exact_parent_version(tx, native.node, &coordinate)
                    .await?
                    .ok_or(super::Error::MaintainedViewMissingBundleWitness(
                        "native witness has no canonical history body",
                    ))?;
                if row.schema_version_alias() != native.schema || row.deletion() != native.deletion
                {
                    return Err(super::Error::InvalidStoredValue(
                        "native witness disagrees with canonical history",
                    ));
                }
                Ok(row)
            }
        }
    }
}
