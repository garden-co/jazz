//! Trusted catalogue snapshot ingestion and planning.

use super::*;

struct PlannedCatalogueSnapshot {
    catalogue: SchemaCatalogue,
    activated_lineages: Vec<StagedSchemaLineage>,
}

fn next_schema_version_alias_in_catalogue(
    catalogue: &SchemaCatalogue,
) -> Result<SchemaVersionAlias, Error> {
    Ok(SchemaVersionAlias(
        catalogue
            .schema_version_aliases
            .values()
            .map(|alias| alias.0)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(Error::InvalidStoredValue("schema version alias exhausted"))?,
    ))
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    pub(crate) async fn apply_trusted_catalogue_snapshot(
        &mut self,
        snapshot: crate::protocol::CatalogueSnapshot,
    ) -> Result<PublicationOutcome<()>, Error>
    where
        S: ReopenableStorage,
    {
        let bootstrap_uninitialized =
            self.catalogue_bootstrap_state == CatalogueBootstrapState::Uninitialized;
        let plan = self.plan_trusted_catalogue_snapshot(snapshot)?;
        let catalogue_genesis = |catalogue: &SchemaCatalogue| {
            let lineage_targets = catalogue
                .active_lineages_by_target
                .keys()
                .copied()
                .collect::<BTreeSet<_>>();
            catalogue
                .catalogue_schemas
                .keys()
                .find(|schema| !lineage_targets.contains(schema))
                .copied()
                .ok_or(Error::InvalidStoredValue(
                    "catalogue genesis schema is missing",
                ))
        };
        let planned_genesis = catalogue_genesis(&plan.catalogue)?;
        let runtime_semantics_changed = self.catalogue.schema != plan.catalogue.schema
            || self.catalogue.catalogue_schemas != plan.catalogue.catalogue_schemas
            || self.catalogue.catalogue_lenses != plan.catalogue.catalogue_lenses
            || self.catalogue.physical_mappings != plan.catalogue.physical_mappings
            || self.catalogue.current_write_schema != plan.catalogue.current_write_schema;
        let previous_catalogue = std::mem::replace(&mut self.catalogue, plan.catalogue.clone());
        let previous_genesis = catalogue_genesis(&previous_catalogue)?;
        // Snapshot replay can install widened mappings after a persistent edge
        // restart. Rebuild the live projection registry as part of that
        // semantic transition, not after client traffic begins. An identical
        // trusted prefix is idempotent and must retain maintained/query state.
        if runtime_semantics_changed && self.rebuild_database_slot().await.is_err() {
            self.catalogue = previous_catalogue;
            self.catalogue_activation_failed = true;
            return Err(Error::CatalogueActivationFailed);
        }

        #[cfg(any(test, feature = "testing"))]
        if self.catalogue_activation_failpoint
            == Some(CatalogueActivationFailpoint::BeforeSnapshotActivationCommit)
        {
            self.catalogue_activation_failpoint = None;
            self.catalogue = previous_catalogue;
            self.catalogue_activation_failed = true;
            return Err(Error::CatalogueActivationFailed);
        }

        let mut batch = self.database.open_batch();
        // A snapshot's root is the authority's permanent genesis, rather than
        // its current write schema.  A pre-bootstrap receiver may have opened
        // against a descendant-shaped local schema; replace that provisional
        // marker atomically with the authority root before it can be recovered.
        if previous_genesis != planned_genesis {
            batch.delete(
                "jazz_catalogue",
                groove::db::PrimaryKeyValue::Composite(vec![
                    groove::db::PrimaryKeyValue::U64(codec::CatalogueRecordKind::Genesis.key()),
                    groove::db::PrimaryKeyValue::Uuid(previous_genesis.0),
                ]),
            );
        }
        if bootstrap_uninitialized
            || self.catalogue_bootstrap_marker
            || previous_genesis != planned_genesis
        {
            // This is intentionally in the same batch as every imported
            // schema, physical mapping, lineage activation, and write
            // pointer.  A dynamic edge must never recover an empty local
            // schema as a durable genesis if it crashes before the first
            // authority snapshot completes.  Existing bootstrap markers are
            // refreshed on every later trusted snapshot so a fresh process
            // can verify the exact durable pointer and catalogue high-water.
            batch.update(
                "jazz_catalogue",
                vec![
                    Value::U64(codec::CatalogueRecordKind::Genesis.key()),
                    Value::Uuid(planned_genesis.0),
                    Value::Bytes(Vec::new()),
                ],
            );
            let ready = CatalogueBootstrapReady {
                genesis: planned_genesis,
                current_write_schema: plan.catalogue.current_write_schema,
                active_catalogue_seq: plan.catalogue.active_catalogue_seq,
            };
            batch.update(
                "jazz_catalogue",
                vec![
                    Value::U64(codec::CatalogueRecordKind::BootstrapReady.key()),
                    Value::Uuid(ready.genesis.0),
                    Value::Bytes(codec::encode_catalogue_bootstrap_ready(&ready)),
                ],
            );
        }
        for schema in plan.catalogue.catalogue_schemas.values() {
            batch.update(
                "jazz_catalogue",
                vec![
                    Value::U64(codec::CatalogueRecordKind::Schema.key()),
                    Value::Uuid(schema.id.0),
                    Value::Bytes(codec::encode_catalogue_schema(schema)?),
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
        let persistence = async {
            let applied = self.database.apply_batch(batch).await?;
            let persisted = applied.persist().await;
            self.database.finish_persistence(persisted)?;
            Ok::<_, groove::db::Error>(())
        }
        .await;
        if persistence.is_err() {
            self.catalogue = previous_catalogue;
            self.catalogue_activation_failed = true;
            return Err(Error::CatalogueActivationFailed);
        }

        self.catalogue.staged_lineages.clear();
        self.catalogue.pending_lineages.clear();
        self.catalogue.pending_write_pointers.clear();
        self.catalogue.lens_path_cache.clear();
        self.catalogue.compiled_lens_cache.clear();
        self.catalogue.physical_write_plan_cache.clear();
        self.query.version_storage_sources_cache.clear();
        self.query.query_shape_cache.clear();
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
        self.catalogue_bootstrap_state = CatalogueBootstrapState::Ready;
        self.catalogue_bootstrap_marker |= bootstrap_uninitialized;
        if runtime_semantics_changed {
            self.groove_runtime_token = next_groove_runtime_token();
        }
        let drained = self.drain_parked_commit_units().await?;
        self.drain_parked_relay_commit_units().await?;
        self.drain_parked_shape_registrations()?;
        Ok(PublicationOutcome {
            value: (),
            publications: drained.publications,
            post_settlement_work: drained.post_settlement_work,
        })
    }

    fn plan_trusted_catalogue_snapshot(
        &self,
        snapshot: crate::protocol::CatalogueSnapshot,
    ) -> Result<PlannedCatalogueSnapshot, Error> {
        if !self.catalogue.pending_lineages.is_empty()
            || !self.catalogue.staged_lineages.is_empty()
            || !self.catalogue.pending_write_pointers.is_empty()
        {
            return Err(Error::InvalidCatalogueUpdate(
                "trusted catalogue snapshot conflicts with pending catalogue work",
            ));
        }

        let genesis_physical_identities = snapshot.genesis_physical_identities;
        let mut schemas = BTreeMap::new();
        for schema in snapshot.schemas {
            if schema.id != schema.schema.version_id() {
                return Err(Error::InvalidCatalogueUpdate(
                    "trusted catalogue snapshot schema id mismatch",
                ));
            }
            if schemas.insert(schema.id, schema).is_some() {
                return Err(Error::InvalidCatalogueUpdate(
                    "trusted catalogue snapshot repeats a schema id",
                ));
            }
        }

        let mut lineages = snapshot.lineages;
        lineages.sort_by_key(|(catalogue_seq, _)| *catalogue_seq);
        let lineage_targets = lineages
            .iter()
            .map(|(_, publication)| publication.schema.id)
            .collect::<BTreeSet<_>>();
        // §10.2 has one durable genesis and every other schema enters through
        // its lineage-defining bundle.  A snapshot is a complete authority
        // catalogue, not an opportunity to smuggle a dormant standalone
        // schema into a receiver's read/write surface.
        let genesis_ids = schemas
            .keys()
            .filter(|schema_id| !lineage_targets.contains(schema_id))
            .copied()
            .collect::<Vec<_>>();
        let [genesis_id] = genesis_ids.as_slice() else {
            return Err(Error::InvalidCatalogueUpdate(
                "trusted catalogue snapshot must contain exactly one genesis schema",
            ));
        };
        let bootstrap_genesis =
            if self.catalogue_bootstrap_state == CatalogueBootstrapState::Uninitialized {
                Some(
                    schemas
                        .get(genesis_id)
                        .cloned()
                        .ok_or(Error::InvalidCatalogueUpdate(
                            "trusted bootstrap snapshot omits genesis payload",
                        ))?,
                )
            } else {
                None
            };

        let local_schema_storage_anchor = (!matches!(
            self.catalogue_bootstrap_state,
            CatalogueBootstrapState::Uninitialized
        ))
        .then(|| {
            self.catalogue
                .schema_version_aliases
                .get(&self.catalogue.current_schema_version_id)
                .copied()
                .zip(
                    self.catalogue
                        .physical_mappings
                        .get(&self.catalogue.current_schema_version_id)
                        .cloned(),
                )
        })
        .flatten();

        let mut planned = match bootstrap_genesis {
            Some(genesis) => {
                let mut next_physical_table_id = 1;
                let mut next_physical_column_id = 1;
                let mapping = allocate_provisional_physical_mapping(
                    &genesis.schema,
                    genesis_physical_identities.clone(),
                    &mut next_physical_table_id,
                    &mut next_physical_column_id,
                )?;
                let genesis_id = genesis.id;
                SchemaCatalogue {
                    current_schema_version_id: genesis_id,
                    current_schema_version_alias: Some(SchemaVersionAlias(1)),
                    schema: genesis.schema.clone(),
                    schema_version_aliases: BTreeMap::from([(genesis_id, SchemaVersionAlias(1))]),
                    catalogue_schemas: BTreeMap::from([(genesis_id, genesis)]),
                    catalogue_lenses: BTreeMap::new(),
                    physical_mappings: BTreeMap::from([(genesis_id, mapping)]),
                    staged_lineages: BTreeMap::new(),
                    pending_lineages: BTreeMap::new(),
                    active_lineages_by_target: BTreeMap::new(),
                    active_catalogue_seq: 0,
                    pending_write_pointers: BTreeMap::new(),
                    next_physical_table_id,
                    next_physical_column_id,
                    lens_path_cache: BTreeMap::new(),
                    compiled_lens_cache: BTreeMap::new(),
                    physical_write_plan_cache: BTreeMap::new(),
                    current_write_schema: CurrentWriteSchema {
                        revision: 0,
                        schema: genesis_id,
                    },
                }
            }
            None => self.catalogue.clone(),
        };
        let mut activated_lineages = Vec::new();
        for schema in schemas.values() {
            if lineage_targets.contains(&schema.id)
                || planned.catalogue_schemas.contains_key(&schema.id)
            {
                continue;
            }
            let mapping = allocate_provisional_physical_mapping(
                &schema.schema,
                if schema.id == *genesis_id {
                    genesis_physical_identities.clone()
                } else {
                    return Err(Error::InvalidCatalogueUpdate(
                        "trusted catalogue snapshot schema has no identity publication",
                    ));
                },
                &mut planned.next_physical_table_id,
                &mut planned.next_physical_column_id,
            )?;
            let alias = next_schema_version_alias_in_catalogue(&planned)?;
            planned.catalogue_schemas.insert(schema.id, schema.clone());
            planned.physical_mappings.insert(schema.id, mapping);
            planned.schema_version_aliases.insert(schema.id, alias);
        }

        // A pre-bootstrap local node may already have durable rows under its
        // opening schema.  Its numeric aliases remain local, but its
        // provisional UUIDs are not an authority fact: install the exact
        // snapshot genesis manifest before validating descendant publications.
        // Otherwise a valid authority lineage would be compared to freshly
        // minted local UUIDs and rejected during snapshot planning.
        if let Some(mapping) = planned.physical_mappings.get_mut(genesis_id) {
            mapping.identities = genesis_physical_identities.clone();
        }

        for (catalogue_seq, publication) in lineages {
            Self::validate_schema_lineage_publication(&publication).map_err(|_| {
                Error::InvalidCatalogueUpdate(
                    "trusted catalogue snapshot contains invalid lineage identity",
                )
            })?;
            if let Some(existing) = planned
                .active_lineages_by_target
                .get(&publication.schema.id)
            {
                if existing.catalogue_seq != catalogue_seq || existing.publication != publication {
                    return Err(Error::InvalidCatalogueUpdate(
                        "trusted catalogue snapshot lineage conflicts with catalogue",
                    ));
                }
                continue;
            }
            if catalogue_seq != planned.active_catalogue_seq.saturating_add(1) {
                return Err(Error::InvalidCatalogueUpdate(
                    "trusted catalogue snapshot lineage sequence is not contiguous",
                ));
            }
            let source = planned
                .catalogue_schemas
                .get(&publication.lens.source)
                .ok_or(Error::InvalidCatalogueUpdate(
                    "trusted catalogue snapshot lineage source is missing",
                ))?;
            Self::validate_migration_lens_between(&publication.lens, source, &publication.schema)?;
            planned
                .physical_mappings
                .get(&publication.lens.source)
                .ok_or(Error::InvalidCatalogueUpdate(
                    "trusted catalogue snapshot source identities missing",
                ))?
                .identities
                .validate_evolution_to_with_history(
                    &source.schema,
                    &publication.physical_identities,
                    &publication.schema.schema,
                    &publication.lens,
                    planned
                        .physical_mappings
                        .values()
                        .map(|mapping| mapping.identities.clone())
                        .collect::<Vec<_>>(),
                )
                .map_err(Error::InvalidCatalogueUpdate)?;
            Self::validate_lineage_table_partition(
                &source.schema,
                &publication.schema.schema,
                &publication.lens,
                &publication.new_tables,
                &publication.dropped_tables,
            )?;
            let fresh = allocate_provisional_physical_mapping(
                &publication.schema.schema,
                publication.physical_identities.clone(),
                &mut planned.next_physical_table_id,
                &mut planned.next_physical_column_id,
            )?;
            let mapping = Self::reconcile_physical_mapping_for_lens_payload_in_catalogue(
                &planned,
                &publication.lens,
                &publication.schema,
                &fresh,
            )?;
            let staged = StagedSchemaLineage {
                catalogue_seq,
                publication: publication.clone(),
                alias: next_schema_version_alias_in_catalogue(&planned)?,
                mapping,
            };
            planned
                .catalogue_schemas
                .insert(publication.schema.id, publication.schema.clone());
            planned
                .catalogue_lenses
                .insert(publication.lens.id, publication.lens.clone());
            planned
                .schema_version_aliases
                .insert(publication.schema.id, staged.alias);
            planned
                .physical_mappings
                .insert(publication.schema.id, staged.mapping.clone());
            planned
                .active_lineages_by_target
                .insert(publication.schema.id, staged.clone());
            planned.active_catalogue_seq = catalogue_seq;
            activated_lineages.push(staged);
        }

        // Schema identity deliberately excludes policy and physical-index
        // declarations. Apply the sender's final payloads after lineage so
        // agreeing same-id metadata updates use the ordinary trusted path.
        for schema in schemas.into_values() {
            if !planned.catalogue_schemas.contains_key(&schema.id) {
                return Err(Error::InvalidCatalogueUpdate(
                    "trusted catalogue snapshot schema has no lineage",
                ));
            }
            planned.catalogue_schemas.insert(schema.id, schema);
        }

        // Schema aliases and physical ids are node-local. A client may have
        // authored durable work under its opening schema before the authority
        // snapshot arrives, so keep that schema's local storage identity and
        // reconcile the received lineage around it rather than orphaning the
        // pending rows by adopting a newly allocated alias/mapping.
        if let Some((local_alias, mut local_mapping)) = local_schema_storage_anchor {
            let anchor = planned.current_schema_version_id;
            let authority_identities = planned
                .physical_mappings
                .get(&anchor)
                .ok_or(Error::InvalidStoredValue(
                    "snapshot authority mapping missing for local anchor",
                ))?
                .identities
                .clone();
            authority_identities
                .validate_for_schema(
                    &planned
                        .catalogue_schemas
                        .get(&anchor)
                        .ok_or(Error::InvalidStoredValue(
                            "snapshot authority schema missing for local anchor",
                        ))?
                        .schema,
                )
                .map_err(Error::InvalidCatalogueUpdate)?;
            // Preserve only below-semantic-layer aliases. The authority
            // manifest is the global identity source of truth and replaces
            // any provisional UUIDs minted while the receiver was offline.
            local_mapping.identities = authority_identities;
            planned.schema_version_aliases.insert(anchor, local_alias);
            planned.physical_mappings.insert(anchor, local_mapping);

            let mut cursor = anchor;
            let mut visited = BTreeSet::new();
            while visited.insert(cursor) {
                let Some(lineage) = planned.active_lineages_by_target.get(&cursor).cloned() else {
                    break;
                };
                let source_schema = planned
                    .catalogue_schemas
                    .get(&lineage.publication.lens.source)
                    .ok_or(Error::InvalidStoredValue(
                        "snapshot source schema missing during local mapping reconciliation",
                    ))?;
                let target_schema = planned
                    .catalogue_schemas
                    .get(&lineage.publication.lens.target)
                    .ok_or(Error::InvalidStoredValue(
                        "snapshot target schema missing during local mapping reconciliation",
                    ))?;
                let provisional_source = planned
                    .physical_mappings
                    .get(&lineage.publication.lens.source)
                    .ok_or(Error::InvalidStoredValue(
                        "snapshot source mapping missing during local mapping reconciliation",
                    ))?;
                let target_mapping =
                    planned
                        .physical_mappings
                        .get(&cursor)
                        .ok_or(Error::InvalidStoredValue(
                            "snapshot target mapping missing during local mapping reconciliation",
                        ))?;
                let source_mapping = Self::reconcile_source_physical_mapping_for_lens_payload(
                    &lineage.publication.lens,
                    source_schema,
                    target_schema,
                    provisional_source,
                    target_mapping,
                )?;
                planned
                    .physical_mappings
                    .insert(lineage.publication.lens.source, source_mapping);
                cursor = lineage.publication.lens.source;
            }

            let mut ordered_lineages = planned
                .active_lineages_by_target
                .values()
                .cloned()
                .collect::<Vec<_>>();
            ordered_lineages.sort_by_key(|lineage| lineage.catalogue_seq);
            for lineage in ordered_lineages {
                let provisional_target = planned
                    .physical_mappings
                    .get(&lineage.publication.schema.id)
                    .ok_or(Error::InvalidStoredValue(
                        "snapshot target mapping missing during forward reconciliation",
                    ))?
                    .clone();
                let mapping = Self::reconcile_physical_mapping_for_lens_payload_in_catalogue(
                    &planned,
                    &lineage.publication.lens,
                    &lineage.publication.schema,
                    &provisional_target,
                )?;
                planned
                    .physical_mappings
                    .insert(lineage.publication.schema.id, mapping);
            }
            for lineage in planned.active_lineages_by_target.values_mut() {
                lineage.alias = planned.schema_version_aliases[&lineage.publication.schema.id];
                lineage.mapping = planned.physical_mappings[&lineage.publication.schema.id].clone();
            }
            for lineage in &mut activated_lineages {
                lineage.alias = planned.schema_version_aliases[&lineage.publication.schema.id];
                lineage.mapping = planned.physical_mappings[&lineage.publication.schema.id].clone();
            }
        }

        if snapshot.current_write_schema.revision < planned.current_write_schema.revision
            || (snapshot.current_write_schema.revision == planned.current_write_schema.revision
                && snapshot.current_write_schema != planned.current_write_schema)
            || !planned
                .catalogue_schemas
                .contains_key(&snapshot.current_write_schema.schema)
        {
            return Err(Error::InvalidCatalogueUpdate(
                "trusted catalogue snapshot conflicts at write-schema revision",
            ));
        }
        planned.current_write_schema = snapshot.current_write_schema;
        // `schema` is the active read-schema payload supplied when this node
        // was opened. Its policy metadata is intentionally outside the
        // structural schema id and may therefore be refreshed by a snapshot
        // even while the current write pointer names another schema.
        planned.schema = planned
            .catalogue_schemas
            .get(&planned.current_schema_version_id)
            .ok_or(Error::InvalidCatalogueUpdate(
                "trusted catalogue snapshot omits the active read schema",
            ))?
            .schema
            .clone();
        planned.current_schema_version_alias = planned
            .schema_version_aliases
            .get(&planned.current_schema_version_id)
            .copied();
        Ok(PlannedCatalogueSnapshot {
            catalogue: planned,
            activated_lineages,
        })
    }
}
