//! Durable input closure for a non-suspending live IVM tick.
//!
//! Tick evaluation consumes ordinary table/index deltas from memory. Only
//! durable persist validation and recursive fallback reconstruction may read
//! storage. This module derives those reads from the retained graph without
//! executing any operator.

use super::*;
use crate::storage::async_ordered::OwnedScanRequest;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TickStorageRequirements {
    scans: Vec<OwnedScanRequest>,
}

impl TickStorageRequirements {
    fn insert(&mut self, scan: OwnedScanRequest) {
        if !self.scans.contains(&scan) {
            self.scans.push(scan);
        }
    }

    pub(crate) fn ensure_resident<S>(&self, storage: &S) -> Result<(), IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        for scan in &self.scans {
            storage.require_resident(
                &crate::storage::async_ordered::OwnedStorageOperation::Scan(scan.clone()),
            )?;
        }
        Ok(())
    }
}

impl IvmRuntime {
    pub(crate) fn tick_storage_requirements(
        &self,
    ) -> Result<TickStorageRequirements, IvmRuntimeError> {
        let retained = self.retained_node_ids();
        let mut requirements = TickStorageRequirements::default();
        for node in retained {
            let graph_node = self
                .graph
                .node(node)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
            match &graph_node.descriptor.operator {
                OpType::Persist(persist) => requirements.insert(OwnedScanRequest::prefix(
                    persist.storage.column_family.clone(),
                    persist.storage.key_prefix.clone(),
                )),
                OpType::Recursive(_) => {
                    let mut sources = HashSet::new();
                    collect_table_sources(&self.graph, node, &mut sources)?;
                    for source in sources {
                        let scan = match source.scan {
                            None => OwnedScanRequest::prefix(source.table, Vec::new()),
                            Some(scan) => match scan_bounds(&scan)? {
                                StaticScanBounds::Prefix(prefix) => {
                                    OwnedScanRequest::prefix(source.table, prefix)
                                }
                                StaticScanBounds::Range { start, end } => {
                                    OwnedScanRequest::range(source.table, start, end)
                                }
                            },
                        };
                        requirements.insert(scan);
                    }
                }
                _ => {}
            }
        }
        Ok(requirements)
    }
}
