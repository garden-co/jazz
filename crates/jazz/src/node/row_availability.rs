//! Core-owned current-row evaluation. Nothing here turns cache completeness into authority.
use super::*;
use crate::protocol::{
    CurrentRowCoordinate, CurrentRowOutcome, CurrentRowsReceipt, CurrentRowsRequest,
    PolicyBindingKey, VersionCarrier,
};

impl<S: OrderedKvStorage> NodeState<S> {
    /// The same host-only complete-policy-input capability used by scalar exits.
    /// History completeness and remote advertised roles never grant it.
    pub(crate) fn can_mint_current_row_receipts(&self) -> bool {
        self.authoritative_scalar_exit_refresh && self.client_relay_scope().is_none()
    }

    pub(crate) fn current_row_coordinate(
        &self,
        table: &str,
        row: RowUuid,
    ) -> Result<CurrentRowCoordinate, Error> {
        let schema = self.catalogue.current_schema_version_id;
        let physical_table = self
            .catalogue
            .physical_mappings
            .get(&schema)
            .and_then(|mapping| mapping.identities.tables.get(table))
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?
            .id;
        Ok(CurrentRowCoordinate {
            schema,
            table: table.to_owned(),
            physical_table,
            row,
        })
    }

    /// Caller holds the node owner lock throughout evaluation and receipt capture.
    pub(crate) async fn evaluate_current_rows(
        &mut self,
        request: &CurrentRowsRequest,
        context: PolicyBindingKey,
        core_epoch: u64,
        progress: u64,
    ) -> Result<CurrentRowsReceipt, Error> {
        let mut receipt = CurrentRowsReceipt {
            request_id: request.request_id,
            rows: request.rows.clone(),
            outcomes: vec![CurrentRowOutcome::Unknown; request.rows.len()],
            context: context.clone(),
            core: self.node_uuid(),
            core_epoch,
            claims_revision: self.session_claim_revision(context.identity),
            policy_epoch: self.active_catalogue_seq(),
            settled_through: self.committed_global_time(),
            authorization_progress: progress,
            version_carriers: Vec::new(),
        };
        if !self.can_mint_current_row_receipts() || !self.permissions_ready() {
            return Ok(receipt);
        }
        for (index, coordinate) in request.rows.iter().enumerate() {
            if self
                .current_row_coordinate(&coordinate.table, coordinate.row)
                .ok()
                .as_ref()
                != Some(coordinate)
            {
                continue;
            }
            if !self.table(&coordinate.table)?.branch_by.is_empty() {
                continue;
            }
            let (shape, binding) = self.whole_table_shape_binding(&coordinate.table)?;
            let rows = self
                .query_readable_current_row_including_deleted(
                    &shape,
                    &binding,
                    DurabilityTier::Global,
                    context.identity,
                    coordinate.row,
                )
                .await?;
            let Some(_row) = rows.iter().find(|row| row.row_uuid() == coordinate.row) else {
                receipt.outcomes[index] = CurrentRowOutcome::CurrentUnavailable;
                continue;
            };
            let table_id =
                self.physical_table_id_for_schema(coordinate.schema, &coordinate.table)?;
            // includeDeleted provenance may name the register event. Fetch
            // content and deletion winners independently, as ordinary views do.
            let Some(tx_id) = self
                .visible_global_layer_tx_id_for_physical_table_now(
                    table_id,
                    coordinate.row,
                    VersionLayer::Content,
                )
                .await
            else {
                continue;
            };
            let mut transactions = BTreeSet::from([tx_id]);
            if let Some(deletion_tx) = self
                .visible_global_layer_tx_id_for_physical_table_now(
                    table_id,
                    coordinate.row,
                    VersionLayer::Deletion,
                )
                .await
            {
                transactions.insert(deletion_tx);
            }
            let mut carriers = Vec::new();
            for tx in transactions {
                let versions = self
                    .query_versions_for_tx(tx)
                    .await?
                    .into_iter()
                    .filter(|version| {
                        version.row_uuid() == coordinate.row
                            && self.physical_table_id_for_version(version).ok() == Some(table_id)
                    })
                    .collect::<Vec<_>>();
                // Default root only: a branch-qualified row requires a future explicit contract.
                if versions.is_empty()
                    || versions
                        .iter()
                        .any(|version| !version.branch_key().values.is_empty())
                {
                    carriers.clear();
                    break;
                }
                let stored = self
                    .query_transaction(tx)
                    .await?
                    .ok_or(Error::MissingTransaction(tx))?;
                let mut bundle = self
                    .version_bundle_for_maintained_view_versions_with_tx(&stored, &versions)
                    .await?;
                bundle.scope = crate::protocol::VersionBundleScope::ViewScoped;
                carriers.push(VersionCarrier::Bundle(bundle));
            }
            if !carriers.is_empty() {
                receipt.outcomes[index] = CurrentRowOutcome::Readable;
                receipt.version_carriers.extend(carriers);
            }
        }
        Ok(receipt)
    }
}

impl<S: OrderedKvStorage> NodeState<S> {
    /// Validate the whole receipt against exact coordinates before ordinary
    /// repair ingestion. Missing results are never interpreted as unavailable.
    pub(crate) async fn ingest_current_rows_receipt(
        &mut self,
        receipt: &CurrentRowsReceipt,
    ) -> Result<(), Error> {
        let invalid = || {
            Error::MalformedViewUpdate("current row receipt does not match requested coordinates")
        };
        if receipt.rows.len() != receipt.outcomes.len() {
            return Err(invalid());
        }
        let bundles = crate::protocol::expand_version_carriers(&receipt.version_carriers)
            .map_err(|_| invalid())?;
        let mut covered = BTreeSet::new();
        let mut requests = Vec::new();
        for bundle in &bundles {
            if bundle.scope != crate::protocol::VersionBundleScope::ViewScoped
                || bundle.versions.is_empty()
                || bundle
                    .global_time
                    .is_none_or(|cut| cut > receipt.settled_through)
            {
                return Err(invalid());
            }
            for version in &bundle.versions {
                let physical = self
                    .catalogue
                    .physical_mappings
                    .get(&version.schema_version())
                    .and_then(|mapping| mapping.identities.tables.get(version.table()))
                    .map(|table| table.id)
                    .ok_or_else(invalid)?;
                let Some((index, coordinate)) = receipt.rows.iter().enumerate().find(|(_, row)| {
                    row.physical_table == physical && row.row == version.row_uuid()
                }) else {
                    return Err(invalid());
                };
                if receipt.outcomes[index] != CurrentRowOutcome::Readable
                    || !version.branch_key().values.is_empty()
                    || self
                        .current_row_coordinate(&coordinate.table, coordinate.row)
                        .ok()
                        .as_ref()
                        != Some(coordinate)
                {
                    return Err(invalid());
                }
                covered.insert(index);
                requests.push(crate::protocol::RowVersionRef::new(
                    coordinate.table.clone(),
                    coordinate.row,
                    bundle.tx.tx_id,
                ));
            }
        }
        if receipt.outcomes.iter().enumerate().any(|(index, outcome)| {
            *outcome == CurrentRowOutcome::Readable && !covered.contains(&index)
        }) {
            return Err(invalid());
        }
        self.apply_row_version_payloads_for_requests(&requests, bundles)
            .await?;
        Ok(())
    }
}
