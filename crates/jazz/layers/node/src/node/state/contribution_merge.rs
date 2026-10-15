impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// The one durable admission boundary for contribution provenance. Every
    /// path that can persist a transaction must pass through this before it
    /// can allocate aliases, stage large values, or mutate a batch.
    pub(super) fn admit_contribution_merge_for_storage(
        &self,
        tx: &Transaction,
    ) -> Result<Value, Error> {
        self.validate_contribution_merge_operation_identities(tx)?;
        self.contribution_merge_storage_value(tx.contribution_merge.as_ref())
    }

    /// Validate strategy-defined operation coordinates before a transaction can
    /// become durable.  Operation identity is not opaque provenance: its
    /// canonical spelling is part of merge deduplication, so a received or
    /// recovered record must be checked against the admitted schema rather
    /// than deferred until a later contribution calculation happens to read
    /// it.
    pub(super) fn validate_contribution_merge_operation_identities(
        &self,
        tx: &Transaction,
    ) -> Result<(), Error> {
        let Some(provenance) = &tx.contribution_merge else {
            return Ok(());
        };
        provenance.validate().map_err(|_| {
            Error::InvalidStoredValue("transaction contribution provenance must be canonical")
        })?;
        // Validate every branch key structurally against its *authored*
        // physical table before any durable codec calls `canonical_bytes`.
        // Raw input must become a malformed rejection, never an encoder panic.
        for intent in &provenance.branch_write_intents {
            let catalogue_schema = self
                .catalogue
                .catalogue_schemas
                .get(&intent.authored_schema)
                .ok_or(Error::InvalidStoredValue("branch write intent schema is unknown"))?;
            let mapping = self
                .catalogue
                .physical_mappings
                .get(&intent.authored_schema)
                .ok_or(Error::InvalidStoredValue("branch write intent physical mapping is missing"))?;
            let (table_name, _) = mapping
                .tables
                .iter()
                .find(|(_, table)| table.table_id == intent.physical_table_id)
                .ok_or(Error::InvalidStoredValue("branch write intent table is unknown"))?;
            let table = catalogue_schema
                .schema
                .tables
                .iter()
                .find(|table| &table.name == table_name)
                .ok_or(Error::InvalidStoredValue("branch write intent table schema is missing"))?;
            catalogue_schema
                .schema
                .validate_authored_branch_key(table, &intent.head)
                .map_err(Error::InvalidBranchKey)?;
            if let crate::tx::BranchWriteOperation::ViewUpdateCopy(evidence) = &intent.operation {
                if evidence.table != *table_name
                    || evidence.row_uuid != intent.row_uuid
                    || evidence.head != intent.head
                {
                    return Err(Error::InvalidStoredValue(
                        "branch write copy evidence is not bound to its intent",
                    ));
                }
                catalogue_schema
                    .schema
                    .validate_authored_branch_key(table, &evidence.head)
                    .map_err(Error::InvalidBranchKey)?;
                let base = match &evidence.base {
                    crate::tx::BranchViewCopyBase::Current(base) => base,
                    crate::tx::BranchViewCopyBase::Snapshot { branch, .. } => branch,
                };
                catalogue_schema
                    .schema
                    .validate_authored_branch_key(table, base)
                    .map_err(Error::InvalidBranchKey)?;
            }
        }
        // Linear history has no cross-branch contribution calculator, so
        // nothing legitimately produces contribution substitutions.
        if !provenance.substitutions.is_empty() {
            return Err(Error::InvalidStoredValue(
                "contribution substitutions require the removed contribution calculator",
            ));
        }
        Ok(())
    }
}
