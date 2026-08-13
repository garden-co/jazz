//! Trusted catalogue-snapshot activation.
//!
//! Planning remains with the catalogue mutation helpers in the parent module;
//! this module owns the fallible replacement-and-commit boundary and its cache
//! invalidation/parked-work follow-up.

use super::*;

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(crate) fn apply_trusted_catalogue_snapshot(
        &mut self,
        snapshot: crate::protocol::CatalogueSnapshot,
    ) -> Result<(), Error>
    where
        S: ReopenableStorage,
    {
        let plan = self.plan_trusted_catalogue_snapshot(snapshot)?;
        let runtime_semantics_changed = self.catalogue.schema != plan.catalogue.schema
            || self.catalogue.catalogue_schemas != plan.catalogue.catalogue_schemas
            || self.catalogue.catalogue_lenses != plan.catalogue.catalogue_lenses
            || self.catalogue.physical_mappings != plan.catalogue.physical_mappings
            || self.catalogue.current_write_schema != plan.catalogue.current_write_schema;
        let previous_catalogue = std::mem::replace(&mut self.catalogue, plan.catalogue.clone());
        if self.synchronize_physical_version_tables().is_err() {
            self.catalogue = previous_catalogue;
            self.catalogue_activation_failed = true;
            return Err(Error::CatalogueActivationFailed);
        }

        #[cfg(test)]
        if self.catalogue_activation_failpoint
            == Some(CatalogueActivationFailpoint::BeforeSnapshotActivationCommit)
        {
            self.catalogue_activation_failpoint = None;
            self.catalogue = previous_catalogue;
            self.catalogue_activation_failed = true;
            return Err(Error::CatalogueActivationFailed);
        }

        let mut batch = self.database.open_batch();
        for schema in plan.catalogue.catalogue_schemas.values() {
            batch.update(
                "jazz_catalogue",
                vec![
                    Value::Bytes(b"schema".to_vec()),
                    Value::Uuid(schema.id.0),
                    Value::Bytes(serde_json::to_vec(schema)?),
                ],
            );
        }
        for staged in &plan.activated_lineages {
            Self::write_active_schema_lineage_to_batch(&mut batch, staged)?;
        }
        for (schema_version, mapping) in &plan.catalogue.physical_mappings {
            let alias = plan.catalogue.schema_version_aliases[schema_version];
            Self::write_schema_version_mapping_to_batch(
                &mut batch,
                alias,
                *schema_version,
                mapping,
            )?;
        }
        if plan.catalogue.current_write_schema != previous_catalogue.current_write_schema {
            batch.update(
                "jazz_catalogue_pointer",
                vec![
                    Value::U64(plan.catalogue.current_write_schema.revision),
                    Value::Uuid(plan.catalogue.current_write_schema.schema.0),
                ],
            );
        }
        if self.database.commit_batch(batch).is_err() {
            self.catalogue = previous_catalogue;
            self.catalogue_activation_failed = true;
            return Err(Error::CatalogueActivationFailed);
        }

        self.catalogue.staged_lineages.clear();
        self.catalogue.pending_lineages.clear();
        self.catalogue.pending_write_pointers.clear();
        self.catalogue.lens_path_cache.clear();
        self.catalogue.compiled_lens_cache.clear();
        self.query.version_storage_sources_cache.clear();
        self.query.query_shape_cache.clear();
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
        if runtime_semantics_changed {
            self.groove_runtime_token = next_groove_runtime_token();
        }
        self.drain_parked_commit_units()?;
        self.drain_parked_relay_commit_units()?;
        self.drain_parked_shape_registrations()?;
        Ok(())
    }
}
