impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Commit a local mergeable write and leave its fate pending.
    pub async fn commit_mergeable(
        &mut self,
        commit: MergeableCommit,
    ) -> Result<PublishedTransaction, Error> {
        commit.validate()?;
        self.merge_commit_parent_times(std::slice::from_ref(&commit))?;
        let made_at = self.mint_tx_time(commit.now_ms)?;
        self.commit_mergeable_at(commit, made_at).await
    }

    /// Commit one local mergeable write under an admitted authored schema.
    ///
    /// Client database handles retain the schema they were opened with even
    /// when an authority later advances its separate current-write pointer.
    /// Their canonical versions must retain that authored schema so receivers
    /// can reconstruct through the ordered catalogue lineage.
    pub(crate) async fn commit_mergeable_in_schema(
        &mut self,
        schema_version: SchemaVersionId,
        commit: MergeableCommit,
    ) -> Result<PublishedTransaction, Error> {
        self.commit_mergeable_many_in_schema(schema_version, vec![commit])
            .await
    }

    /// Commit multiple local mergeable writes as one transaction.
    pub async fn commit_mergeable_many(
        &mut self,
        commits: Vec<MergeableCommit>,
    ) -> Result<PublishedTransaction, Error> {
        if commits.is_empty() {
            return Err(Error::InvalidMergeableCommit(
                "mergeable transaction requires at least one write",
            ));
        }
        for commit in &commits {
            commit.validate()?;
            if commit.effective_permission_subject() != commits[0].effective_permission_subject() {
                return Err(Error::InvalidMergeableCommit(
                    "mergeable transaction permission subjects must match",
                ));
            }
        }
        self.merge_commit_parent_times(&commits)?;
        let made_at = self.mint_tx_time(commits[0].now_ms)?;
        self.commit_mergeable_many_at(commits, made_at).await
    }

    /// Commit the already-calculated output of the high-level contribution
    /// merge helper as one ordinary mergeable transaction.
    pub(crate) async fn commit_calculated_merge_many(
        &mut self,
        commits: Vec<MergeableCommit>,
        provenance: ContributionMergeProvenance,
    ) -> Result<PublishedTransaction, Error> {
        self.require_catalogue_ready()?;
        provenance.validate().map_err(Error::InvalidMergeableCommit)?;
        if commits.is_empty() {
            return Err(Error::InvalidMergeableCommit(
                "calculated merge requires at least one write",
            ));
        }
        for commit in &commits {
            commit.validate()?;
            if commit.effective_permission_subject() != commits[0].effective_permission_subject() {
                return Err(Error::InvalidMergeableCommit(
                    "calculated merge permission subjects must match",
                ));
            }
        }
        let schema_version = self.catalogue.current_write_schema.schema;
        let mut emitted = BTreeSet::new();
        for commit in &commits {
            let table = self.table_in_schema(&commit.table, schema_version)?;
            let schema = &self
                .catalogue
                .catalogue_schemas
                .get(&schema_version)
                .ok_or(Error::InvalidStoredValue("current write schema missing"))?
                .schema;
            let (branch_key, _) = schema
                .project_branch_selector(&table, &commit.branch)
                .map_err(Error::InvalidBranchKey)?;
            let layer = if commit.deletion.is_some() {
                MergeAspect::Deletion
            } else {
                MergeAspect::Content
            };
            if layer == MergeAspect::Deletion {
                emitted.insert(ContributionCoordinate {
                    branch_key,
                    table: commit.table.clone(),
                    row_uuid: commit.row_uuid,
                    layer,
                    component: ContributionComponent::Register,
                });
            } else {
                let authored = commit
                    .authored_columns
                    .clone()
                    .unwrap_or_else(|| commit.cells.keys().cloned().collect());
                for column in authored {
                    let components = match table.merge_strategy(&column) {
                        MergeStrategy::Lww => vec![ContributionComponent::Column(column)],
                        MergeStrategy::Counter => {
                            vec![ContributionComponent::Operation(column.into_bytes())]
                        }
                        MergeStrategy::GSet => match commit.cells.get(&column) {
                            Some(Value::Array(elements)) => elements
                                .iter()
                                .map(|element| {
                                    postcard::to_allocvec(&(column.as_str(), element)).map(
                                        ContributionComponent::Operation,
                                    )
                                })
                                .collect::<Result<Vec<_>, _>>()
                                .map_err(|_| {
                                    Error::InvalidMergeableCommit(
                                        "g-set contribution operation must encode",
                                    )
                                })?,
                            _ => {
                                return Err(Error::InvalidMergeableCommit(
                                    "g-set calculated merge value must be an array",
                                ));
                            }
                        },
                    };
                    emitted.extend(components.into_iter().map(|component| ContributionCoordinate {
                        branch_key: branch_key.clone(),
                        table: commit.table.clone(),
                        row_uuid: commit.row_uuid,
                        layer,
                        component,
                    }));
                }
            }
        }
        if provenance
            .substitutions
            .iter()
            .any(|substitution| !emitted.contains(&substitution.target))
        {
            return Err(Error::InvalidMergeableCommit(
                "contribution substitution target was not emitted",
            ));
        }
        self.merge_commit_parent_times(&commits)?;
        let made_at = self.mint_tx_time(commits[0].now_ms)?;
        self.commit_mergeable_many_at_with_schema_versions_and_provenance(
            commits
                .into_iter()
                .map(|commit| (schema_version, commit))
                .collect(),
            made_at,
            Some(provenance),
        )
        .await
    }

    /// Commit local mergeable writes under one admitted authored schema.
    pub(crate) async fn commit_mergeable_many_in_schema(
        &mut self,
        schema_version: SchemaVersionId,
        commits: Vec<MergeableCommit>,
    ) -> Result<PublishedTransaction, Error> {
        self.require_catalogue_ready()?;
        if !self
            .catalogue
            .catalogue_schemas
            .contains_key(&schema_version)
        {
            return Err(Error::InvalidMergeableCommit(
                "authored schema version is not admitted",
            ));
        }
        if commits.is_empty() {
            return Err(Error::InvalidMergeableCommit(
                "mergeable transaction requires at least one write",
            ));
        }
        for commit in &commits {
            commit.validate()?;
            if commit.effective_permission_subject() != commits[0].effective_permission_subject() {
                return Err(Error::InvalidMergeableCommit(
                    "mergeable transaction permission subjects must match",
                ));
            }
        }
        self.merge_commit_parent_times(&commits)?;
        let made_at = self.mint_tx_time(commits[0].now_ms)?;
        self.commit_mergeable_many_at_with_schema_versions(
            commits
                .into_iter()
                .map(|commit| (schema_version, commit))
            .collect(),
            made_at,
        )
        .await
    }

    fn merge_commit_parent_times(&mut self, commits: &[MergeableCommit]) -> Result<(), Error> {
        for commit in commits {
            if !commit.parents.is_empty() {
                for parent in &commit.parents {
                    self.merge_tx_time(parent.time);
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn commit_mergeable_at(
        &mut self,
        commit: MergeableCommit,
        made_at: TxTime,
    ) -> Result<PublishedTransaction, Error> {
        self.commit_mergeable_many_at(vec![commit], made_at).await
    }

    async fn commit_mergeable_many_at(
        &mut self,
        commits: Vec<MergeableCommit>,
        made_at: TxTime,
    ) -> Result<PublishedTransaction, Error> {
        self.require_catalogue_ready()?;
        let write_schema_version = self.catalogue.current_write_schema.schema;
        let commits = commits
            .into_iter()
            .map(|commit| (write_schema_version, commit))
            .collect();
        self.commit_mergeable_many_at_with_schema_versions(commits, made_at)
            .await
    }

    pub(super) async fn commit_mergeable_many_at_with_schema_versions(
        &mut self,
        commits: Vec<(SchemaVersionId, MergeableCommit)>,
        made_at: TxTime,
    ) -> Result<PublishedTransaction, Error> {
        self.commit_mergeable_many_at_with_schema_versions_and_provenance(
            commits, made_at, None,
        )
        .await
    }

    pub(super) async fn commit_mergeable_many_at_with_schema_versions_and_provenance(
        &mut self,
        mut commits: Vec<(SchemaVersionId, MergeableCommit)>,
        made_at: TxTime,
        contribution_merge: Option<ContributionMergeProvenance>,
    ) -> Result<PublishedTransaction, Error> {
        // `*_at` is also used by trusted internal paths, so do not rely on the
        // public `commit_mergeable[_many]` wrapper to validate a public
        // provenance millisecond before any staging or batch mutation begins.
        for (_, commit) in &commits {
            commit.validate()?;
        }
        self.prepare_and_stage_large_commit_values(&mut commits).await?;
        let staged_ids = commits
            .iter()
            .flat_map(|(_, commit)| commit.staged_large_values.iter().copied())
            .collect::<BTreeSet<_>>();
        self.ensure_large_value_stages_current(&staged_ids).await?;
        let tx_id = TxId::new(made_at, self.node_uuid);
        let made_by = commits[0].1.made_by;
        let permission_subject = commits[0].1.effective_permission_subject();
        let user_metadata_json = commits[0].1.user_metadata_json.clone();
        let tx = Transaction {
            tx_id,
            kind: TxKind::Mergeable,
            n_total_writes: commits.len().try_into().map_err(|_| {
                Error::InvalidMergeableCommit("transaction write count exceeds u32")
            })?,
            made_by,
            permission_subject: commits[0].1.permission_subject,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json,
            contribution_merge,
        };
        let tx_node_alias = self.ensure_node_alias(tx_id.node).await?;
        let mut batch = self.database.open_batch();
        for (_, commit) in &commits {
            for staged_id in &commit.staged_large_values {
                batch.accept_large_value(*staged_id);
            }
        }
        batch.insert(
            "jazz_transactions",
            transaction_values(
                tx_node_alias,
                &tx,
                Fate::Pending,
                None,
                self.authored_commit_durability,
            ),
        );
        let mut stored_versions = Vec::new();
        let mut pending_parents = BTreeSet::new();
        let mut authored_content_rows = BTreeSet::new();
        for (write_schema_version, commit) in commits {
            let provenance_at = TxTime::from_physical_ms(commit.now_ms).map_err(|_| {
                Error::InvalidMergeableCommit(
                    "commit now_ms exceeds packed HLC physical-millisecond range",
                )
            })?;
            let schema_version_alias = self
                .ensure_schema_version_alias(write_schema_version)
                .await?;
            let table_schema = self.table_in_schema(&commit.table, write_schema_version)?;
            let schema = &self
                .catalogue
                .catalogue_schemas
                .get(&write_schema_version)
                .ok_or(Error::InvalidStoredValue("commit schema missing"))?
                .schema;
            let (branch_key, branch_cells) = schema
                .project_branch_selector(&table_schema, &commit.branch)
                .map_err(Error::InvalidBranchKey)?;
            let table_id = self.physical_table_id_for_schema(
                write_schema_version,
                &table_schema.name,
            )?;
            for parent in &commit.parents {
                let parent_versions = self.query_versions_for_tx(*parent).await?;
                let same_row = parent_versions.iter().filter(|version| {
                    version.row_uuid() == commit.row_uuid
                        && self.physical_table_id_for_version(version).ok() == Some(table_id)
                });
                if same_row.clone().next().is_some()
                    && !same_row.into_iter().any(|version| version.branch_key() == &branch_key)
                {
                    return Err(Error::InvalidMergeableCommit(
                        "version parent belongs to a different branch-local row",
                    ));
                }
            }
            let layer = VersionLayer::for_commit(&commit);
            let first_content_occurrence_in_batch = layer != VersionLayer::Content
                || authored_content_rows.insert((
                    table_id,
                    branch_key.clone(),
                    commit.row_uuid,
                ));
            let known_fresh_content_row = commit.known_fresh_row
                && layer == VersionLayer::Content
                && first_content_occurrence_in_batch;
            let previous_local_current = if known_fresh_content_row {
                None
            } else {
                self.query_local_layer_winner_in_branch(
                    &table_schema.name,
                    &branch_key,
                    commit.row_uuid,
                    layer,
                )
                .await?
            };
            let known_first_local_content_version =
                layer == VersionLayer::Content
                    && first_content_occurrence_in_batch
                    && (known_fresh_content_row || previous_local_current.is_none());
            let previous_current = match previous_local_current {
                Some(previous) => Some(previous),
                None if !known_fresh_content_row => {
                    self.query_global_layer_winner_in_branch(
                        &table_schema.name,
                        &branch_key,
                        commit.row_uuid,
                        layer,
                    )
                    .await?
                }
                None => None,
            };
            let creator_source = if let Some(previous) = previous_current.as_ref() {
                Some(previous.clone())
            } else if layer == VersionLayer::Deletion {
                match self.query_local_layer_winner_in_branch(
                    &table_schema.name,
                    &branch_key,
                    commit.row_uuid,
                    VersionLayer::Content,
                ).await? {
                    Some(previous) => Some(previous),
                    None => self.query_global_layer_winner_in_branch(
                        &table_schema.name,
                        &branch_key,
                        commit.row_uuid,
                        VersionLayer::Content,
                    ).await?,
                }
            } else {
                None
            };
            let (created_by, created_at) = creator_source
                .as_ref()
                .map(|version| (version.created_by(), version.created_at()))
                .unwrap_or((commit.made_by, provenance_at));

            let parents = if commit.parents.is_empty() {
                Vec::new()
            } else {
                commit.parents
            };
            let mut cells = commit.cells;
            for (column, value) in branch_cells {
                if let Some(authored) = cells.get(&column)
                    && authored != &value
                {
                    return Err(Error::InvalidMergeableCommit(
                        "branch column does not match exact branch key",
                    ));
                }
                cells.insert(column, value);
            }
            let authored_columns = Some(
                commit
                    .authored_columns
                    .clone()
                    .unwrap_or_else(|| cells.keys().cloned().collect()),
            );
            let history_descriptor = if commit.deletion.is_none() {
                Some(
                    self.prepared_physical_write_plan(
                        write_schema_version,
                        &table_schema.name,
                        PhysicalWriteTarget::History,
                    )?
                    .logical_descriptor,
                )
            } else {
                None
            };
            let stored = VersionRow::from_parts_with_schema_version(
                &table_schema,
                VersionRowParts {
                    table: commit.table,
                    branch_key,
                    row_uuid: commit.row_uuid,
                    tx_node_alias,
                    schema_version_alias,
                    tx_time: made_at,
                    parents,
                    created_by,
                    created_at,
                    updated_by: commit.made_by,
                    updated_at: provenance_at,
                    cells,
                    authored_columns,
                    deletion: commit.deletion,
                },
                (write_schema_version != self.catalogue.current_schema_version_id)
                    .then_some(write_schema_version),
                history_descriptor,
            )?;
            let previous_winner = if let Some(previous) = previous_current.as_ref() {
                Some((
                    previous,
                    self.version_tx_id(previous)?,
                    self.version_made_at(previous).await?,
                ))
            } else {
                None
            };
            let new_is_current =
                version_wins_over_open_winner(&stored, tx_id, made_at, previous_winner);
            let _ = (new_is_current, previous_current);
            let (history_table, groove_record) = self.version_storage_write_binding(&stored)?;
            batch.insert_raw(
                history_table.as_ref(),
                self.version_storage_primary_key(&stored)?,
                groove_record,
            );
            self.update_merge_heads_for_content_version(
                &mut batch,
                &stored,
                known_first_local_content_version,
            )
            .await?;
            self.write_ahead_current_insert(&mut batch, &stored)?;
            pending_parents.extend(stored.parents());
            stored_versions.push(stored);
        }
        for parent in pending_parents {
            if let Some(parent_alias) = self.node_aliases.get(&parent.node).copied() {
                batch.insert(
                    "jazz_pending_edges",
                    pending_edge_values(tx_node_alias, tx_id, parent_alias, parent),
                );
            }
        }
        let pending_child_edges = {
            let mut edges = Vec::new();
            for stored in &stored_versions {
                for parent in stored.parents() {
                    if self
                        .query_transaction(parent)
                        .await?
                        .is_none_or(|tx| matches!(tx.fate, Fate::Pending))
                    {
                        edges.push(parent);
                    }
                }
            }
            edges
        };
        let persistence = self.database.apply_batch(batch).await?;
        self.cache_tx_versions(tx_id, stored_versions.clone());
        if permission_subject != made_by {
            self.open_tx
                .local_permission_subjects
                .insert(tx_id, permission_subject);
        }
        for parent in pending_child_edges {
            self.rejections
                .child_txs_by_parent
                .entry(parent)
                .or_default()
                .insert(tx_id);
        }
        self.pending_persistence.insert(tx_id);
        Ok(PublishedTransaction { tx_id, persistence })
    }

    /// Lower oversized ordinary scalar cells through Groove and atomically
    /// stage their immutable nodes before row publication begins.
    async fn prepare_and_stage_large_commit_values(
        &mut self,
        commits: &mut [(SchemaVersionId, MergeableCommit)],
    ) -> Result<(), Error> {
        for (schema_version, commit) in commits.iter_mut() {
            let table_schema = self.table_in_schema(&commit.table, *schema_version)?;
            let inherited = if commit.cells.values().any(value_contains_indirect_descriptor) {
                self.current_physical_cells_in_branch_schema(
                    *schema_version,
                    &commit.table,
                    &commit.branch,
                    commit.row_uuid,
                )
                .await?
                .unwrap_or_default()
            } else {
                BTreeMap::new()
            };
            for (column, value) in commit.cells.iter_mut() {
                if value_contains_indirect_descriptor(value)
                    && inherited.get(column) == Some(value)
                {
                    commit.prepared_large_columns.insert(column.clone());
                    continue;
                }
                let semantic_kind = table_schema
                    .columns
                    .iter()
                    .find(|candidate| candidate.name == *column)
                    .map(|column| column.large_value_kind)
                    .unwrap_or(crate::schema::LargeValueSemanticKind::NotLarge);
                let Some(staged) = self
                    .prepare_and_stage_large_scalar(value, semantic_kind)
                    .await?
                else {
                    continue;
                };
                commit.staged_large_values.push(staged.id);
            }
        }
        Ok(())
    }

    /// Lower one top-level scalar cell, preserving its nullable wrapper. This
    /// is shared by mergeable and exclusive publication so neither write path
    /// can leak an oversized inline scalar onto the wire.
    pub(crate) async fn prepare_and_stage_large_scalar(
        &mut self,
        value: &mut Value,
        semantic_kind: crate::schema::LargeValueSemanticKind,
    ) -> Result<Option<groove::large_values::StagedLargeValue>, Error> {
        use groove::large_values::{INLINE_VALUE_MAX_BYTES, LargeValueKind};

        let candidate = match value {
            Value::String(text) if text.len() > INLINE_VALUE_MAX_BYTES => Some((
                match semantic_kind {
                    crate::schema::LargeValueSemanticKind::Json => LargeValueKind::Json,
                    _ => LargeValueKind::String,
                },
                text.as_bytes().to_vec(),
                false,
            )),
            Value::Bytes(bytes) if bytes.len() > INLINE_VALUE_MAX_BYTES => {
                Some((LargeValueKind::Bytes, bytes.clone(), false))
            }
            Value::Nullable(Some(inner)) => match inner.as_ref() {
                Value::String(text) if text.len() > INLINE_VALUE_MAX_BYTES => Some((
                    match semantic_kind {
                        crate::schema::LargeValueSemanticKind::Json => LargeValueKind::Json,
                        _ => LargeValueKind::String,
                    },
                    text.as_bytes().to_vec(),
                    true,
                )),
                Value::Bytes(bytes) if bytes.len() > INLINE_VALUE_MAX_BYTES => {
                    Some((LargeValueKind::Bytes, bytes.clone(), true))
                }
                _ => None,
            },
            _ => None,
        };
        let Some((kind, bytes, nullable)) = candidate else {
            return Ok(None);
        };
        let staged = self
            .database
            .prepare_and_stage_large_value(kind, &bytes)
            .await?;
        self.enforce_large_value_staging_policy(&staged).await?;
        let descriptor = Value::Large(staged.value_ref.clone());
        *value = if nullable {
            Value::Nullable(Some(Box::new(descriptor)))
        } else {
            descriptor
        };
        Ok(Some(staged))
    }

    /// Commit a local mergeable write and return its sync commit unit.
    pub async fn commit_mergeable_unit(
        &mut self,
        commit: MergeableCommit,
    ) -> Result<(PublishedTransaction, SyncMessage), Error> {
        let made_by = commit.made_by;
        let permission_subject = commit.permission_subject;
        let user_metadata_json = commit.user_metadata_json.clone();
        let published = self.commit_mergeable(commit).await?;
        let tx_id = published.tx_id;
        let tx = Transaction {
            tx_id,
            kind: TxKind::Mergeable,
            n_total_writes: 1,
            made_by,
            permission_subject,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json,
            contribution_merge: None,
        };
        let unit = self.resident_commit_unit(tx)?;
        Ok((published, unit))
    }

    pub(super) fn resident_commit_unit(&mut self, tx: Transaction) -> Result<SyncMessage, Error> {
        let versions = self
            .cached_tx_versions(tx.tx_id)
            .expect("newly published transaction retains its resident versions")
            .into_iter()
            .map(|row| self.version_record_from_row(&row))
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(SyncMessage::CommitUnit { tx, versions })
    }

    /// Settle a completed persistence receipt and release its storage boundary.
    pub fn settle_published_transaction(
        &mut self,
        tx_id: TxId,
        persistence: PersistedBatch,
    ) -> Result<(), Error> {
        self.database.finish_persistence(persistence)?;
        self.pending_persistence.remove(&tx_id);
        Ok(())
    }

    /// Persist and settle one resident transaction publication.
    pub async fn persist_and_settle_transaction(
        &mut self,
        published: PublishedTransaction,
    ) -> Result<TxId, Error> {
        let tx_id = published.tx_id;
        let persistence = published.persist().await;
        self.settle_published_transaction(tx_id, persistence)?;
        Ok(tx_id)
    }

    /// Persist and settle every resident publication attached to an outcome.
    pub async fn persist_and_settle_outcome<T>(
        &mut self,
        outcome: PublicationOutcome<T>,
    ) -> Result<T, Error>
    where
        S: ReopenableStorage,
    {
        let (value, mut publications, mut work) = outcome.into_parts();
        loop {
            for publication in publications {
                let persistence = publication.persist().await;
                self.settle_published_transaction(publication.tx_id(), persistence)?;
            }
            let Some(message) = work.pop_front() else {
                break;
            };
            let mut outcome = self.apply_sync_message(message).await?;
            // This is internal continuation work, not a reply to a remote
            // sender. Its resident publications and further continuations are
            // lifecycle-significant; its protocol response is not.
            publications = outcome.publications;
            work.append(&mut outcome.post_settlement_work);
        }
        Ok(value)
    }

    /// Rebuild the sync commit unit for an already-committed local transaction
    /// from its stored versions.
    ///
    /// Used by the `Db` sync surface to upload a client's local writes upstream
    /// on a connection. Unlike [`NodeState::commit_mergeable_unit`] this reads the
    /// stored versions, so the shipped
    /// unit matches what the author actually stored.
    pub async fn commit_unit_for(&mut self, tx_id: TxId) -> Result<SyncMessage, Error> {
        let tx = self
            .query_transaction(tx_id)
            .await?
            .ok_or(Error::MissingTransaction(tx_id))?
            .tx
            .clone();
        let versions = self
            .query_versions_for_tx(tx_id)
            .await?
            .into_iter()
            .map(|row| self.version_record_from_row(&row))
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(SyncMessage::CommitUnit { tx, versions })
    }

    /// Open an exclusive transaction over the current snapshot.
    pub fn visible_current_cells(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        Ok(crate::db::block_on(
            self.current_rows(table, DurabilityTier::Local),
        )?
            .into_iter()
            .find(|row| row.row_uuid() == row_uuid)
            .map(|row| {
                let table_schema = self.table(table).expect("table exists");
                table_schema
                    .columns
                    .iter()
                    .filter_map(|column| {
                        row.cell(table_schema, &column.name)
                            .map(|value| (column.name.clone(), value))
                    })
                    .collect()
            }))
    }

    /// Read one exact branch-local row for mutation preparation.
    pub async fn visible_current_cells_in_branch(
        &mut self,
        table: &str,
        branch: &BranchSelector,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        let schema_version = self.catalogue.current_write_schema.schema;
        self.visible_current_physical_cells_in_branch_schema(
            schema_version,
            table,
            branch,
            row_uuid,
        )
        .await
    }

    /// Read a branch-local winner projected into one registered schema while
    /// retaining indirect scalar descriptors rather than hydrating them.
    pub(crate) async fn visible_current_physical_cells_in_branch_schema(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        branch: &BranchSelector,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        let table_schema = self.table_in_schema(table, schema_version)?;
        let schema = &self
            .catalogue
            .catalogue_schemas
            .get(&schema_version)
            .ok_or(Error::InvalidStoredValue("registered read schema missing"))?
            .schema;
        let (branch_key, _) = schema
            .project_branch_selector(&table_schema, branch)
            .map_err(Error::InvalidBranchKey)?;
        let deletion = match self.query_local_layer_winner_in_branch(
            table,
            &branch_key,
            row_uuid,
            VersionLayer::Deletion,
        )
        .await?
        {
            Some(version) => Some(version),
            None => self.query_global_layer_winner_in_branch(
                table,
                &branch_key,
                row_uuid,
                VersionLayer::Deletion,
            )
            .await?,
        };
        if deletion.is_some_and(|version| version.deletion() == Some(DeletionEvent::Deleted)) {
            return Ok(None);
        }
        let content = match self.query_local_layer_winner_in_branch(
            table,
            &branch_key,
            row_uuid,
            VersionLayer::Content,
        )
        .await?
        {
            Some(version) => Some(version),
            None => self.query_global_layer_winner_in_branch(
                table,
                &branch_key,
                row_uuid,
                VersionLayer::Content,
            )
            .await?,
        };
        let Some(content) = content
        else {
            return Ok(None);
        };
        let authored_schema = self
            .schema_version_for_alias(content.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "current version schema alias must exist",
            ))?;
        let authored_table = self.table_in_schema(content.table(), authored_schema)?.clone();
        let mut cells = self.materialized_cells_for_version(&authored_table, &content)?;
        let Some(projected_table) =
            self.translate_cells(authored_schema, schema_version, content.table(), &mut cells)?
        else {
            return Ok(None);
        };
        if projected_table != table_schema.name {
            return Err(Error::InvalidStoredValue(
                "current version projects to an unexpected table",
            ));
        }
        Ok(Some(cells))
    }

    /// Return the exact local content parent for a branch-local row.
    pub async fn local_content_winner_tx_id_in_branch(
        &mut self,
        table: &str,
        branch: &BranchSelector,
        row_uuid: RowUuid,
    ) -> Result<Option<TxId>, Error> {
        self.local_layer_winner_tx_id_in_branch_selector(
            table,
            branch,
            row_uuid,
            VersionLayer::Content,
        )
        .await
    }

    /// Return the exact local deletion parent for a branch-local row.
    pub async fn local_deletion_winner_tx_id_in_branch(
        &mut self,
        table: &str,
        branch: &BranchSelector,
        row_uuid: RowUuid,
    ) -> Result<Option<TxId>, Error> {
        self.local_layer_winner_tx_id_in_branch_selector(
            table,
            branch,
            row_uuid,
            VersionLayer::Deletion,
        )
        .await
    }

    async fn local_layer_winner_tx_id_in_branch_selector(
        &mut self,
        table: &str,
        branch: &BranchSelector,
        row_uuid: RowUuid,
        layer: VersionLayer,
    ) -> Result<Option<TxId>, Error> {
        let schema_version = self.catalogue.current_write_schema.schema;
        let table_schema = self.table_in_schema(table, schema_version)?;
        let schema = &self
            .catalogue
            .catalogue_schemas
            .get(&schema_version)
            .ok_or(Error::InvalidStoredValue("current write schema missing"))?
            .schema;
        let (branch_key, _) = schema
            .project_branch_selector(&table_schema, branch)
            .map_err(Error::InvalidBranchKey)?;
        self.query_local_layer_winner_in_branch(table, &branch_key, row_uuid, layer)
            .await?
            .as_ref()
            .map(|version| self.version_tx_id(version))
            .transpose()
    }

    /// Return current rows at the requested durability tier.
    pub async fn current_rows(
        &mut self,
        table: &str,
        settled: DurabilityTier,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.require_catalogue_ready()?;
        let shape = crate::query::Query::from(table).validate(&self.catalogue.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        self.query_rows(&shape, &binding, settled).await
    }

    pub(crate) async fn local_content_winner_tx_id(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<TxId>, Error> {
        self.local_content_winner_tx_id_in_schema(
            self.catalogue.current_write_schema.schema,
            table,
            row_uuid,
        )
        .await
    }

    pub(crate) async fn local_content_winner_tx_id_in_schema(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<TxId>, Error> {
        let table_schema = self.table_in_schema(table, schema_version)?;
        Ok(self
            .local_current_content_row_candidate(&table_schema, row_uuid, schema_version)
            .await?
            .map(|(_, (time, node))| TxId::new(time, node)))
    }

    pub(crate) async fn local_deletion_winner_tx_id(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<TxId>, Error> {
        self.local_deletion_winner_tx_id_in_schema(
            self.catalogue.current_write_schema.schema,
            table,
            row_uuid,
        )
        .await
    }

    pub(crate) async fn local_deletion_winner_tx_id_in_schema(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<TxId>, Error> {
        let table_schema = self.table_in_schema(table, schema_version)?;
        Ok(self
            .local_current_deletion_candidate(&table_schema, row_uuid, schema_version)
            .await?
            .map(|(_, (time, node))| TxId::new(time, node)))
    }

    async fn rebuild_ahead_current_keys(&mut self) -> Result<(), Error> {
        #[cfg(feature = "testing")]
        {
            self.rebuild_ahead_current_keys_inner(None).await
        }
        #[cfg(not(feature = "testing"))]
        self.rebuild_ahead_current_keys_inner().await
    }

    #[cfg(feature = "testing")]
    async fn rebuild_ahead_current_keys_with_receipt(
        &mut self,
        receipt: &mut NodeOpenReceipt,
    ) -> Result<(), Error> {
        self.rebuild_ahead_current_keys_inner(Some(receipt)).await
    }

    async fn rebuild_ahead_current_keys_inner(
        &mut self,
        #[cfg(feature = "testing")] mut receipt: Option<&mut NodeOpenReceipt>,
    ) -> Result<(), Error> {
        self.ahead_current_keys.clear();
        let physical_table_ids = self
            .catalogue
            .physical_mappings
            .values()
            .flat_map(|mapping| mapping.tables.values().map(|table| table.table_id))
            .collect::<BTreeSet<_>>();
        for table_id in physical_table_ids {
            let content_rows = self
                .database
                .primary_key_scan_raw(&physical_ahead_current_table_name(table_id), &[])
                .await?
                .into_iter()
                .map(|raw| {
                    let record = raw.record();
                    Ok((
                        BranchKey::from_canonical_bytes(
                            record.get_bytes(GlobalCurrentRowRecord::FIELD_BRANCH_KEY_IDX)?,
                        )
                        .map_err(|_| Error::InvalidStoredValue("invalid ahead-current branch key"))?,
                        SchemaVersionAlias(
                            record.get_u64(GlobalCurrentRowRecord::FIELD_SCHEMA_VERSION_IDX)?,
                        ),
                        RowUuid(record.get_uuid(GlobalCurrentRowRecord::FIELD_ROW_UUID_IDX)?),
                        TxTime(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?),
                        NodeAlias(record.get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?),
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            for (branch_key, alias, row_uuid, tx_time, tx_node_alias) in content_rows {
                #[cfg(feature = "testing")]
                if let Some(receipt) = &mut receipt {
                    receipt.ahead_current_entries += 1;
                }
                self.insert_ahead_current_key(
                    self.logical_table_for_physical_alias(table_id, alias)?,
                    branch_key,
                    VersionLayer::Content,
                    row_uuid,
                    tx_time,
                    tx_node_alias,
                );
            }
            let deletion_rows = self
                .database
                .primary_key_scan_raw(&physical_register_ahead_current_table_name(table_id), &[])
                .await?
                .into_iter()
                .map(|raw| {
                    let record = raw.record();
                    Ok((
                        BranchKey::from_canonical_bytes(
                            record.get_bytes(
                                RegisterGlobalCurrentRowRecord::FIELD_BRANCH_KEY_IDX,
                            )?,
                        )
                        .map_err(|_| Error::InvalidStoredValue("invalid ahead-current branch key"))?,
                        SchemaVersionAlias(
                            record.get_u64(
                                RegisterGlobalCurrentRowRecord::FIELD_SCHEMA_VERSION_IDX,
                            )?,
                        ),
                        RowUuid(
                            record.get_uuid(RegisterGlobalCurrentRowRecord::FIELD_ROW_UUID_IDX)?,
                        ),
                        TxTime(record.get_u64(RegisterGlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?),
                        NodeAlias(
                            record.get_u64(RegisterGlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?,
                        ),
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            for (branch_key, alias, row_uuid, tx_time, tx_node_alias) in deletion_rows {
                #[cfg(feature = "testing")]
                if let Some(receipt) = &mut receipt {
                    receipt.ahead_current_entries += 1;
                }
                self.insert_ahead_current_key(
                    self.logical_table_for_physical_alias(table_id, alias)?,
                    branch_key,
                    VersionLayer::Deletion,
                    row_uuid,
                    tx_time,
                    tx_node_alias,
                );
            }
        }
        Ok(())
    }

    fn insert_ahead_current_key(
        &mut self,
        table: String,
        branch_key: BranchKey,
        layer: VersionLayer,
        row_uuid: RowUuid,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) {
        self.ahead_current_keys
            .insert((table, branch_key, layer, row_uuid, tx_time, tx_node_alias));
    }

    fn remove_ahead_current_key(
        &mut self,
        table: &str,
        branch_key: &BranchKey,
        layer: VersionLayer,
        row_uuid: RowUuid,
        tx_time: TxTime,
        tx_node_alias: NodeAlias,
    ) {
        self.ahead_current_keys.remove(&(
            table.to_owned(),
            branch_key.clone(),
            layer,
            row_uuid,
            tx_time,
            tx_node_alias,
        ));
    }

    pub(super) fn cached_tx_version_tables(&self, tx_id: TxId) -> Option<BTreeSet<String>> {
        self.query.tx_version_tables_cache.get(&tx_id).cloned()
    }

    pub(super) fn cached_tx_versions(&self, tx_id: TxId) -> Option<Vec<VersionRow>> {
        self.query.tx_versions_cache.get(&tx_id).cloned()
    }

    pub(super) fn cache_tx_version_tables(&mut self, tx_id: TxId, tables: BTreeSet<String>) {
        self.touch_tx_version_cache_entry(tx_id);
        self.query.tx_version_tables_cache.insert(tx_id, tables);
        self.bound_tx_version_cache();
    }

    pub(super) fn cache_tx_versions(&mut self, tx_id: TxId, versions: Vec<VersionRow>) {
        self.touch_tx_version_cache_entry(tx_id);
        self.query.tx_versions_cache.insert(tx_id, versions);
        self.bound_tx_version_cache();
    }

    fn touch_tx_version_cache_entry(&mut self, tx_id: TxId) {
        if self.query.tx_version_tables_cache_order_set.insert(tx_id) {
            self.query.tx_version_tables_cache_order.push_back(tx_id);
        }
    }

    fn bound_tx_version_cache(&mut self) {
        while self.query.tx_version_tables_cache.len() > TX_VERSION_TABLE_CACHE_MAX_ENTRIES
            || self.query.tx_versions_cache.len() > TX_VERSION_TABLE_CACHE_MAX_ENTRIES
        {
            let Some(oldest) = self.query.tx_version_tables_cache_order.pop_front() else {
                break;
            };
            if !self.query.tx_version_tables_cache_order_set.remove(&oldest) {
                continue;
            }
            self.query.tx_version_tables_cache.remove(&oldest);
            self.query.tx_versions_cache.remove(&oldest);
        }
    }

    pub(super) fn invalidate_tx_version_tables_cache(&mut self, tx_id: TxId) {
        self.query.tx_version_tables_cache.remove(&tx_id);
        self.query.tx_versions_cache.remove(&tx_id);
        self.query.tx_version_tables_cache_order_set.remove(&tx_id);
    }

    pub(super) fn invalidate_tx_version_table_names_cache(&mut self, tx_id: TxId) {
        self.query.tx_version_tables_cache.remove(&tx_id);
    }

    fn materialize_current_row(
        &mut self,
        _table: &TableSchema,
        row: CurrentRow,
    ) -> Result<CurrentRow, Error> {
        Ok(row)
    }

    fn current_row_from_materialized_version(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
    ) -> Result<CurrentRow, Error> {
        current_row_from_version_projection(table, version)
    }

    fn materialized_cells_for_version(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
    ) -> Result<BTreeMap<String, Value>, Error> {
        version.cells(table)
    }

    pub(crate) async fn local_current_row(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<CurrentRow>, Error> {
        self.local_current_row_in_schema(
            table,
            row_uuid,
            self.catalogue.current_write_schema.schema,
        )
        .await
    }

    /// Resolve an engine-owned indirect descriptor from the current physical
    /// row. Callers must perform ordinary Jazz row authorization before using
    /// this helper; the descriptor never crosses the public API boundary.
    pub(crate) async fn current_physical_cell_in_schema(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
        column: &str,
    ) -> Result<Option<Value>, Error> {
        Ok(self
            .current_physical_cells_in_schema(schema_version, table, row_uuid)
            .await?
            .and_then(|mut cells| cells.remove(column)))
    }

    pub(crate) async fn current_physical_cells_in_schema(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        self.current_physical_cells_in_branch_schema(
            schema_version,
            table,
            &BranchSelector::default(),
            row_uuid,
        )
        .await
    }

    async fn current_physical_cells_in_branch_schema(
        &mut self,
        schema_version: SchemaVersionId,
        table: &str,
        branch: &BranchSelector,
        row_uuid: RowUuid,
    ) -> Result<Option<BTreeMap<String, Value>>, Error> {
        self.visible_current_physical_cells_in_branch_schema(
            schema_version,
            table,
            branch,
            row_uuid,
        )
        .await
    }

    /// Seal a high-level partial-value update after verifying that every
    /// indirect descriptor is either the freshly staged target or an exact
    /// descriptor inherited from the current physical row.
    pub(crate) async fn seal_large_value_update(
        &mut self,
        mut commit: MergeableCommit,
        target_column: &str,
        staged: groove::large_values::StagedLargeValue,
        schema_version: SchemaVersionId,
    ) -> Result<MergeableCommit, Error> {
        let inherited = self
            .current_physical_cells_in_branch_schema(
                schema_version,
                &commit.table,
                &commit.branch,
                commit.row_uuid,
            )
            .await?
            .ok_or(Error::InvalidMergeableCommit(
                "partial large-value update target is not observed",
            ))?;
        for (column, value) in &commit.cells {
            if !value_contains_indirect_descriptor(value) {
                continue;
            }
            let valid = if column == target_column {
                let mut descriptors = Vec::new();
                collect_indirect_descriptors(value, &mut descriptors);
                descriptors.as_slice() == [staged.value_ref.clone()]
            } else {
                inherited.get(column) == Some(value)
            };
            if !valid {
                return Err(Error::InvalidMergeableCommit(
                    "partial large-value update contains an unverified descriptor",
                ));
            }
            commit.prepared_large_columns.insert(column.clone());
        }
        commit.staged_large_values.push(staged.id);
        Ok(commit)
    }

    pub(crate) async fn seal_inherited_large_values(
        &mut self,
        mut commit: MergeableCommit,
        schema_version: SchemaVersionId,
        allow_inherited_descriptors: bool,
    ) -> Result<MergeableCommit, Error> {
        if !commit.cells.values().any(value_contains_indirect_descriptor) {
            return Ok(commit);
        }
        if !allow_inherited_descriptors {
            return Err(Error::InvalidMergeableCommit(
                "complete row replacement contains an unverified large-value descriptor",
            ));
        }
        let inherited = self
            .current_physical_cells_in_branch_schema(
                schema_version,
                &commit.table,
                &commit.branch,
                commit.row_uuid,
            )
            .await?
            .unwrap_or_default();
        for (column, value) in &commit.cells {
            if value_contains_indirect_descriptor(value) {
                if inherited.get(column) != Some(value) {
                    return Err(Error::InvalidMergeableCommit(
                        "row update contains an unverified large-value descriptor",
                    ));
                }
                commit.prepared_large_columns.insert(column.clone());
            }
        }
        Ok(commit)
    }

    pub(crate) async fn local_current_row_in_schema(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        schema_version: SchemaVersionId,
    ) -> Result<Option<CurrentRow>, Error> {
        let table_schema = self.table_in_schema(table, schema_version)?;
        let content =
            self.local_current_content_row_candidate(&table_schema, row_uuid, schema_version)
                .await?;
        let deletion =
            self.local_current_deletion_candidate(&table_schema, row_uuid, schema_version)
                .await?;
        if let (Some((_, content_tx)), Some((deletion, deletion_tx))) = (&content, &deletion)
            && deletion_tx > content_tx
            && *deletion == DeletionEvent::Deleted
        {
            return Ok(None);
        }
        content
            .map(|(row, _)| self.materialize_current_row(&table_schema, row))
            .transpose()
    }

    async fn local_current_content_row_candidate(
        &mut self,
        table: &TableSchema,
        row_uuid: RowUuid,
        schema_version: SchemaVersionId,
    ) -> Result<Option<(CurrentRow, (TxTime, NodeUuid))>, Error> {
        let prefix = vec![groove::ivm::LiteralValue::from(Value::Uuid(row_uuid.0))];
        let global = self.physical_current_source_scan_graph(
            schema_version,
            &table.name,
            PhysicalCurrentClass::Global,
            groove::ivm::StaticScanSpec::Point(prefix.clone()),
        )?;
        let ahead = self.physical_current_source_scan_graph(
            schema_version,
            &table.name,
            PhysicalCurrentClass::Ahead,
            groove::ivm::StaticScanSpec::Prefix(prefix),
        )?;
        let result = self
            .database
            .query_graph(GraphBuilder::arg_max_by(
                GraphBuilder::union([global, ahead]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            ))
            .await
            .map_err(|error| Self::malformed_current_query_error(&table.name, row_uuid, error))?;
        let Some(delta) = result.deltas.into_iter().find(|delta| delta.weight > 0) else {
            return Ok(None);
        };
        let record = BorrowedRecord::new(&delta.record, &result.descriptor);
        let tx = self.current_record_sort_key(&table.name, row_uuid, record)?;
        Ok(Some((decode_current_row(table, record)?, tx)))
    }

    async fn local_current_deletion_candidate(
        &mut self,
        table: &TableSchema,
        row_uuid: RowUuid,
        schema_version: SchemaVersionId,
    ) -> Result<Option<(DeletionEvent, (TxTime, NodeUuid))>, Error> {
        let table_id = self.physical_table_id_for_schema(schema_version, &table.name)?;
        // Physical current keys lead with the branch key. Prepend the shared
        // (default) branch exactly as the canonical content-current lookup
        // does before applying the logical row-UUID point lookup.
        let point = groove::ivm::StaticScanSpec::Point(vec![
            groove::ivm::LiteralValue::from(Value::Uuid(row_uuid.0)),
        ]);
        let global = GraphBuilder::table_scan(
            physical_register_global_current_table_name(table_id),
            shared_branch_scan(Some(point.clone())),
        );
        let ahead = GraphBuilder::table_scan(
            physical_register_ahead_current_table_name(table_id),
            shared_branch_scan(Some(point)),
        );
        let result = self
            .database
            .query_graph(GraphBuilder::arg_max_by(
                GraphBuilder::union([global, ahead]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            ))
            .await
            .map_err(|error| Self::malformed_current_query_error(&table.name, row_uuid, error))?;
        let Some(delta) = result.deltas.into_iter().find(|delta| delta.weight > 0) else {
            return Ok(None);
        };
        let record = BorrowedRecord::new(&delta.record, &result.descriptor);
        Ok(Some((
            deletion_event_from_value(
                record.get_idx(RegisterGlobalCurrentRowRecord::FIELD__DELETION_IDX)?,
            )?,
            self.current_record_sort_key(&table.name, row_uuid, record)?,
        )))
    }

    fn current_record_sort_key(
        &self,
        table: &str,
        row_uuid: RowUuid,
        record: BorrowedRecord<'_>,
    ) -> Result<(TxTime, NodeUuid), Error> {
        let malformed = |source| {
            Error::MalformedCurrentRow(Box::new(MalformedCurrentRow {
                table: table.to_owned(),
                row_uuid,
                source,
            }))
        };
        let tx_time = TxTime(
            record
                .get_u64(GlobalCurrentRowRecord::FIELD_TX_TIME_IDX)
                .map_err(malformed)?,
        );
        let tx_node_alias = NodeAlias(
            record
                .get_u64(GlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)
                .map_err(malformed)?,
        );
        let tx_node = self
            .node_aliases
            .iter()
            .find_map(|(node, alias)| (*alias == tx_node_alias).then_some(*node))
            .ok_or(Error::InvalidStoredValue(
                "current row references unknown node alias",
            ))?;
        Ok((tx_time, tx_node))
    }

    fn malformed_current_query_error(
        table: &str,
        row_uuid: RowUuid,
        error: GrooveDbError,
    ) -> Error {
        let source = match error {
            GrooveDbError::RecordEncoding(source)
            | GrooveDbError::IvmRuntime(groove::ivm::IvmRuntimeError::RecordEncoding(source)) => {
                source
            }
            error => return Error::Groove(error),
        };
        Error::MalformedCurrentRow(Box::new(MalformedCurrentRow {
            table: table.to_owned(),
            row_uuid,
            source,
        }))
    }

}
