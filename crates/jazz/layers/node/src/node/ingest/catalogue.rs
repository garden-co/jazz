// Global restart-persistent metadata ceiling across every connected peer.
const MAX_PENDING_LARGE_VALUE_UPLOADS: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CatalogueActivationMode {
    ColdOpen,
    Live,
}

fn large_value_upload_is_rejected(error: &groove::db::Error) -> bool {
    matches!(
        error,
        groove::db::Error::InvalidLargeValueMetadata(_)
            | groove::db::Error::IvmRuntime(
                groove::ivm::runtime::IvmRuntimeError::LargeValue(_)
                    | groove::ivm::runtime::IvmRuntimeError::Chunk(
                        groove::chunks::ChunkError::Integrity
                    )
            )
    )
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Apply one sync message and return any outgoing sync messages.
    pub async fn apply_sync_message(
        &mut self,
        message: SyncMessage,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error>
    where
        S: ReopenableStorage,
    {
        self.apply_sync_message_with_ingest_context(message, None)
            .await
    }

    /// Apply a catalogue mutation from the trusted local administrative lane.
    pub async fn apply_trusted_catalogue_message(
        &mut self,
        message: SyncMessage,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error>
    where
        S: ReopenableStorage,
    {
        self.apply_sync_message_with_ingest_context(
            message,
            Some(CommitUnitIngestContext {
                identity: AuthorSubject::SYSTEM,
                trust: CommitUnitTrust::TrustedBackend,
                admitted_write_authorization: false,
                version_receipts_validated: false,
            }),
        )
        .await
    }

    /// Apply one sync message from a connection-authenticated upload path.
    pub fn apply_sync_message_with_ingest_context<'a>(
        &'a mut self,
        message: SyncMessage,
        ingest_context: Option<CommitUnitIngestContext>,
    ) -> std::pin::Pin<
        Box<dyn Future<Output = Result<PublicationOutcome<Vec<SyncMessage>>, Error>> + 'a>,
    >
    where
        S: ReopenableStorage,
    {
        if crate::node::is_catalogue_mutation(&message) {
            if let Err(error) = self.database.ensure_usable() {
                return Box::pin(async move { Err(error.into()) });
            }
            if self.database.has_unsettled_publications() {
                return Box::pin(async { Err(groove::db::Error::UnsettledPublications.into()) });
            }
        }
        // Dispatch the commit before constructing the general message future.
        // A commit's policy evaluation must not keep the inactive catalogue,
        // chunk-upload and other message arms on the executor's stack.
        if let SyncMessage::CommitUnit { tx, versions } = message {
            return Box::pin(async move {
                self.require_catalogue_ready()?;
                if self.catalogue_activation_failed {
                    return Err(Error::CatalogueActivationFailed);
                }
                if ingest_context.is_some() {
                    let descriptors = version_indirect_descriptors(&versions);
                    self.current_staged_ids_for_descriptors(&descriptors, true)
                        .await?;
                }
                let now_ms = if ingest_context.is_some() {
                    authority_wall_clock_ms()?
                } else {
                    tx.tx_id.time.physical_ms()
                };
                // Commit admission owns a large policy/storage state machine.
                // Keep it out of the catalogue dispatcher's inline state.
                Box::pin(self.ingest_commit_unit_with_context(tx, versions, now_ms, ingest_context))
                    .await
            });
        }
        Box::pin(async move {
            // Uninitialized clients and local relays must install a complete
            // trusted catalogue snapshot before accepting incremental traffic.
            self.require_catalogue_ready()?;
            if self.catalogue_activation_failed {
                return Err(Error::CatalogueActivationFailed);
            }
            match message {
                SyncMessage::Reserved30(retired) => match retired {},
                SyncMessage::ChunkUploadStart(start) => {
                    if !self.admit_large_value_ingress(
                        super::LARGE_VALUE_UPLOAD_START_INGRESS_CHARGE_BYTES,
                    ) {
                        return Ok(PublicationOutcome::settled(vec![
                            SyncMessage::ChunkUploadResult(crate::protocol::ChunkUploadResult {
                                value_ref: start.value_ref,
                                status: crate::protocol::ChunkUploadStatus::RateLimited,
                            }),
                        ]));
                    }
                    let progress = match self
                        .database
                        .begin_large_value_upload_with_pending_limit(
                            start.value_ref.clone(),
                            MAX_PENDING_LARGE_VALUE_UPLOADS,
                        )
                        .await
                    {
                        Ok(progress) => progress,
                        Err(groove::db::Error::PendingLargeValueUploadLimitExceeded { .. }) => {
                            return Ok(PublicationOutcome::settled(vec![
                                SyncMessage::ChunkUploadResult(
                                    crate::protocol::ChunkUploadResult {
                                        value_ref: start.value_ref,
                                        status: crate::protocol::ChunkUploadStatus::RateLimited,
                                    },
                                ),
                            ]));
                        }
                        Err(error) if large_value_upload_is_rejected(&error) => {
                            return Ok(PublicationOutcome::settled(vec![
                                SyncMessage::ChunkUploadResult(
                                    crate::protocol::ChunkUploadResult {
                                        value_ref: start.value_ref,
                                        status: crate::protocol::ChunkUploadStatus::Rejected,
                                    },
                                ),
                            ]));
                        }
                        Err(error) => return Err(error.into()),
                    };
                    let status = match progress {
                        groove::large_values::LargeValueUploadProgress::Missing(mut nodes) => {
                            nodes.truncate(64);
                            crate::protocol::ChunkUploadStatus::Need(nodes)
                        }
                        groove::large_values::LargeValueUploadProgress::Staged(_) => {
                            crate::protocol::ChunkUploadStatus::Staged
                        }
                    };
                    Ok(PublicationOutcome::settled(vec![
                        SyncMessage::ChunkUploadResult(crate::protocol::ChunkUploadResult {
                            value_ref: start.value_ref,
                            status,
                        }),
                    ]))
                }
                SyncMessage::ChunkUploadNodes(batch) => {
                    let upload_exists = self
                        .database
                        .pending_large_value_uploads()
                        .await?
                        .into_iter()
                        .any(|upload| upload.descriptor.as_ref() == Some(&batch.value_ref));
                    if !upload_exists {
                        return Ok(PublicationOutcome::settled(vec![
                            SyncMessage::ChunkUploadResult(crate::protocol::ChunkUploadResult {
                                value_ref: batch.value_ref,
                                status: crate::protocol::ChunkUploadStatus::Rejected,
                            }),
                        ]));
                    }
                    let accounting = batch.chunks.iter().try_fold(
                        groove::large_values::StagedLargeValueAccounting::default(),
                        |mut total, chunk| {
                            total.encoded_bytes = total
                                .encoded_bytes
                                .checked_add(u64::try_from(chunk.encoded.len()).map_err(|_| {
                                    Error::UnsupportedSyncMessage("chunk upload batch is too large")
                                })?)
                                .ok_or(Error::UnsupportedSyncMessage(
                                    "chunk upload accounting overflow",
                                ))?;
                            total.node_count = total.node_count.checked_add(1).ok_or(
                                Error::UnsupportedSyncMessage("chunk upload accounting overflow"),
                            )?;
                            Ok::<_, Error>(total)
                        },
                    )?;
                    if !self.admit_large_value_ingress(accounting.encoded_bytes) {
                        return Ok(PublicationOutcome::settled(vec![
                            SyncMessage::ChunkUploadResult(crate::protocol::ChunkUploadResult {
                                value_ref: batch.value_ref,
                                status: crate::protocol::ChunkUploadStatus::RateLimited,
                            }),
                        ]));
                    }
                    let progress = match self
                        .database
                        .continue_large_value_upload_if_current(
                            batch.value_ref.clone(),
                            batch.chunks,
                        )
                        .await
                    {
                        Ok(Some(progress)) => progress,
                        Ok(None) => {
                            return Ok(PublicationOutcome::settled(vec![
                                SyncMessage::ChunkUploadResult(
                                    crate::protocol::ChunkUploadResult {
                                        value_ref: batch.value_ref,
                                        status: crate::protocol::ChunkUploadStatus::Rejected,
                                    },
                                ),
                            ]));
                        }
                        Err(error) if large_value_upload_is_rejected(&error) => {
                            return Ok(PublicationOutcome::settled(vec![
                                SyncMessage::ChunkUploadResult(
                                    crate::protocol::ChunkUploadResult {
                                        value_ref: batch.value_ref,
                                        status: crate::protocol::ChunkUploadStatus::Rejected,
                                    },
                                ),
                            ]));
                        }
                        Err(error) => return Err(error.into()),
                    };
                    let status = match progress {
                        groove::large_values::LargeValueUploadProgress::Missing(mut nodes) => {
                            nodes.truncate(64);
                            crate::protocol::ChunkUploadStatus::Need(nodes)
                        }
                        groove::large_values::LargeValueUploadProgress::Staged(_) => {
                            crate::protocol::ChunkUploadStatus::Staged
                        }
                    };
                    Ok(PublicationOutcome::settled(vec![
                        SyncMessage::ChunkUploadResult(crate::protocol::ChunkUploadResult {
                            value_ref: batch.value_ref,
                            status,
                        }),
                    ]))
                }
                SyncMessage::ChunkUploadResult(_) => Err(Error::UnsupportedSyncMessage(
                    "chunk upload result requires peer link context",
                )),
                SyncMessage::SessionClaims { identity, claims } => {
                    if let Some(context) = ingest_context
                        && matches!(
                            context.trust,
                            CommitUnitTrust::TrustedBackend | CommitUnitTrust::TrustedAuthority
                        )
                    {
                        self.set_session_claims(identity, claims);
                    }
                    Ok(PublicationOutcome::settled(Vec::new()))
                }
                SyncMessage::CommitUnit { .. } => {
                    unreachable!("commit units dispatch before the general message future")
                }
                SyncMessage::FateUpdate {
                    tx_id,
                    fate,
                    global_time,
                    durability,
                } => {
                    validate_received_fate_update_global_time_durability(global_time, durability)?;
                    self.apply_fate_update(tx_id, fate, global_time, durability)
                        .await?;
                    self.drain_parked_commit_units().await
                }
                SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                    subscription,
                    settled_through,
                    version_carriers,
                    peer_payload_inventory,
                    supporting_rows: program_fact_adds,
                }) => {
                    self.apply_view_update(ViewUpdateParts {
                        wire_rows: Some(program_fact_adds),
                        subscription,
                        settled_through,
                        defer_settlement: false,
                        reset_input_set: true,
                        version_carriers,
                        peer_complete_tx_payload_refs: peer_payload_inventory.complete_tx_payloads,
                        authorization_progress: peer_payload_inventory.authorization_progress,
                        opening_pending: peer_payload_inventory.opening_pending,
                        result_member_adds: Vec::new(),
                        result_member_removes: Vec::new(),
                    })
                    .await?;
                    Ok(PublicationOutcome::settled(Vec::new()))
                }
                SyncMessage::RegisterShape {
                    shape_id,
                    ast,
                    opts,
                } => {
                    validate_shape_registration_size(&ast, &opts).map_err(|_| {
                        Error::UnsupportedSyncMessage("shape registration exceeds byte limit")
                    })?;
                    self.register_shape_with_options(shape_id, ast, opts)?;
                    Ok(PublicationOutcome::settled(Vec::new()))
                }
                SyncMessage::FetchRowVersions { .. } => Err(Error::UnsupportedSyncMessage(
                    "row-version repair fetch must be served by peer state",
                )),
                SyncMessage::RowVersionPayloads { .. } => Err(Error::UnsupportedSyncMessage(
                    "row-version repair payload requires outstanding request context",
                )),
                SyncMessage::CatalogueSnapshot(_) => Err(Error::UnsupportedSyncMessage(
                    "catalogue snapshot requires a trusted upstream link",
                )),
                SyncMessage::Subscribe(subscribe) => {
                    validate_known_state_declaration(&subscribe.known_state).map_err(|_| {
                        Error::UnsupportedSyncMessage("known-state declaration exceeds limit")
                    })?;
                    self.apply_subscribe(subscribe)?;
                    Ok(PublicationOutcome::settled(Vec::new()))
                }
                SyncMessage::SubscribeRejected { .. } => Err(Error::UnsupportedSyncMessage(
                    "subscription rejection requires subscription stream context",
                )),
                SyncMessage::Unsubscribe { subscription } => {
                    self.apply_unsubscribe(subscription);
                    Ok(PublicationOutcome::settled(Vec::new()))
                }
                SyncMessage::PublishSchema { author, schema } => {
                    self.apply_publish_schema(author, ingest_context, *schema)
                        .await
                }
                SyncMessage::PublishSchemaWithLens {
                    author,
                    catalogue_seq,
                    publication,
                } => {
                    self.apply_publish_schema_with_lens(
                        author,
                        ingest_context,
                        catalogue_seq,
                        *publication,
                    )
                    .await
                }
                SyncMessage::PublishLens { author, lens } => self
                    .apply_publish_lens(author, ingest_context, lens)
                    .await
                    .map(PublicationOutcome::settled),
                SyncMessage::CatalogueAck(_) => Ok(PublicationOutcome::settled(Vec::new())),
                SyncMessage::ChunkRequestBatch(_) | SyncMessage::ChunkResponseBatch(_) => Err(
                    Error::UnsupportedSyncMessage("chunk traffic requires peer link context"),
                ),
                SyncMessage::CurrentRowsRequest(_)
                | SyncMessage::CurrentRowsReceipt(_)
                | SyncMessage::CurrentRowsCancel { .. }
                | SyncMessage::PermissionAdviceRequest { .. }
                | SyncMessage::PermissionAdviceResponse { .. }
                | SyncMessage::AuthorizationScopeSubscribe { .. }
                | SyncMessage::AuthorizationScopeReceipt { .. }
                | SyncMessage::AuthorizationScopeIntent { .. }
                | SyncMessage::AuthorizationScopeView { .. }
                | SyncMessage::AuthorizationScopeAggregateReceipt { .. }
                | SyncMessage::AuthorizationScopeUnavailable { .. }
                | SyncMessage::AuthorizationScopeDecision { .. } => {
                    Err(Error::UnsupportedSyncMessage(
                        "permission advice requires authenticated link context",
                    ))
                }
            }
        })
    }

    async fn apply_publish_schema(
        &mut self,
        author: AuthorSubject,
        ingest_context: Option<CommitUnitIngestContext>,
        schema: SchemaVersion,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_admin(author, ingest_context)?;
        if schema.id != schema.schema.version_id() {
            return Err(Error::InvalidCatalogueUpdate(
                "schema id does not match schema payload",
            ));
        }
        if !self.catalogue.catalogue_schemas.contains_key(&schema.id) {
            return Err(Error::InvalidCatalogueUpdate(
                "non-genesis schema requires lineage publication",
            ));
        }
        let schema = SchemaVersion::new(schema.schema.without_permissions());
        self.catalogue
            .catalogue_schemas
            .insert(schema.id, schema.clone());
        self.query.version_storage_sources_cache.clear();
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
        self.query.policy_authorization_graph_replacements.clear();
        self.persist_catalogue_schema(&schema).await?;
        if schema.id == self.catalogue.active_schema.schema {
            let mut active = self.catalogue.active_schema.clone();
            active.compiled = schema.schema.with_permissions_from(&active.compiled);
            self.persist_active_schema(&active).await?;
            self.install_active_schema(active);
        } else if schema.id == self.catalogue.local_schema_version_id {
            self.catalogue.schema = schema.schema.with_permissions_from(&self.catalogue.schema);
        }
        self.ensure_provisional_physical_mapping(schema.id).await?;
        self.ensure_schema_version_alias(schema.id).await?;
        self.synchronize_physical_version_tables().await?;
        let mut outcome = self.drain_parked_commit_units().await?;
        self.drain_parked_relay_commit_units().await?;
        self.drain_parked_shape_registrations()?;
        outcome.value.insert(
            0,
            SyncMessage::CatalogueAck(CatalogueAck {
                revision: None,
                schema: Some(schema.id),
                lens: None,
                applied: true,
            }),
        );
        Ok(outcome)
    }

    async fn apply_publish_schema_with_lens(
        &mut self,
        author: AuthorSubject,
        ingest_context: Option<CommitUnitIngestContext>,
        catalogue_seq: u64,
        publication: SchemaLineagePublication,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_admin(author, ingest_context)?;
        Self::validate_schema_lineage_publication(&publication)?;
        if catalogue_seq == 0 {
            return Err(Error::InvalidCatalogueUpdate(
                "schema lineage catalogue sequence must be nonzero",
            ));
        }
        let schema = &publication.schema;
        let acknowledged_lens = publication
            .predecessors
            .first()
            .filter(|_| publication.predecessors.len() == 1)
            .map(|p| p.lens.id);
        if let Some(existing) = self.catalogue.active_lineages_by_target.get(&schema.id) {
            if existing.publication != publication || existing.catalogue_seq != catalogue_seq {
                return Err(Error::InvalidCatalogueUpdate(
                    "schema lineage publication conflicts with catalogue",
                ));
            }
            return Ok(PublicationOutcome::settled(vec![
                SyncMessage::CatalogueAck(CatalogueAck {
                    revision: None,
                    schema: Some(schema.id),
                    lens: acknowledged_lens,
                    applied: true,
                }),
            ]));
        }

        if catalogue_seq <= self.catalogue.active_catalogue_seq {
            return Err(Error::InvalidCatalogueUpdate(
                "schema lineage catalogue sequence conflicts with active catalogue",
            ));
        }
        if let Some(existing) = self.catalogue.pending_lineages.get(&catalogue_seq) {
            if existing.publication != publication {
                return Err(Error::InvalidCatalogueUpdate(
                    "schema lineage catalogue sequence conflict",
                ));
            }
        } else {
            if publication.predecessors.iter().all(|p| {
                self.catalogue
                    .catalogue_schemas
                    .contains_key(&p.lens.source)
            }) {
                Self::validate_publication_sources(
                    &publication,
                    &self.catalogue.catalogue_schemas,
                    &self.catalogue.physical_mappings,
                    self.physical_identity_history_for_candidate(
                        publication.schema.id,
                        Some(publication.id),
                    ),
                )?;
            }
            if self
                .catalogue
                .pending_lineages
                .values()
                .any(|pending| pending.publication.schema.id == schema.id)
                || self
                    .catalogue
                    .staged_lineages
                    .values()
                    .any(|staged| staged.publication.schema.id == schema.id)
            {
                return Err(Error::InvalidCatalogueUpdate(
                    "schema lineage target is already reserved",
                ));
            }
            let pending = PendingSchemaLineage {
                catalogue_seq,
                publication,
            };
            self.persist_pending_schema_lineage(&pending).await?;
            self.catalogue
                .pending_lineages
                .insert(catalogue_seq, pending);
        }
        self.drain_pending_schema_lineages().await
    }

    pub(super) async fn recover_pending_schema_lineages(&mut self) -> Result<(), Error>
    where
        S: ReopenableStorage,
    {
        self.activate_pending_schema_lineages(CatalogueActivationMode::ColdOpen)
            .await
            .map(|_| ())
    }

    pub(super) async fn drain_pending_schema_lineages(
        &mut self,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error>
    where
        S: ReopenableStorage,
    {
        self.activate_pending_schema_lineages(CatalogueActivationMode::Live)
            .await
    }

    async fn activate_pending_schema_lineages(
        &mut self,
        mode: CatalogueActivationMode,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error>
    where
        S: ReopenableStorage,
    {
        let mut outcome = PublicationOutcome::settled(Vec::new());
        loop {
            let next = self.catalogue.active_catalogue_seq.saturating_add(1);
            let Some(pending) = self.catalogue.pending_lineages.get(&next).cloned() else {
                break;
            };
            let publication = &pending.publication;
            if publication.predecessors.iter().any(|p| {
                !self
                    .catalogue
                    .catalogue_schemas
                    .contains_key(&p.lens.source)
            }) {
                break;
            }
            let validation = Self::validate_publication_sources(
                publication,
                &self.catalogue.catalogue_schemas,
                &self.catalogue.physical_mappings,
                self.physical_identity_history_for_candidate(
                    publication.schema.id,
                    Some(publication.id),
                ),
            )
            // A parked sibling was admitted against the catalogue prefix that
            // existed when it arrived. Reconcile it again only when its
            // sequence becomes active: an earlier sibling may have widened a
            // shared scalar registry to the u8 limit in the meantime.
            .and_then(|()| self.validate_pending_schema_lineage_physical_mapping(&pending));
            if validation.is_err() {
                self.remove_pending_schema_lineage(next, publication.id)
                    .await?;
                break;
            }
            let publication = pending.publication;
            if self
                .catalogue
                .active_lineages_by_target
                .contains_key(&publication.schema.id)
            {
                return Err(Error::InvalidCatalogueUpdate(
                    "schema lineage target already has an active bundle",
                ));
            }
            let staged = if let Some(staged) = self.catalogue.staged_lineages.get(&next) {
                if staged.publication != publication {
                    return Err(Error::InvalidCatalogueUpdate(
                        "staged schema lineage conflicts with pending bundle",
                    ));
                }
                staged.clone()
            } else {
                // Provisional identities are only candidates. Reconciliation can
                // replace them with identities inherited from the source schema,
                // so allocate against copies and commit only identities retained
                // by the durable staged mapping.
                let mut provisional_next_table_id = self.catalogue.next_physical_table_id;
                let mut provisional_next_column_id = self.catalogue.next_physical_column_id;
                let fresh = allocate_provisional_physical_mapping(
                    &publication.schema.schema,
                    publication.physical_identities.clone(),
                    &mut provisional_next_table_id,
                    &mut provisional_next_column_id,
                )?;
                let mapping =
                    Self::reconcile_publication_mapping(&self.catalogue, &publication, &fresh)?;
                let mut next_physical_table_id = self.catalogue.next_physical_table_id;
                let mut next_physical_column_id = self.catalogue.next_physical_column_id;
                for table in mapping.tables.values() {
                    next_physical_table_id = next_physical_table_id.max(
                        table
                            .table_id
                            .0
                            .checked_add(1)
                            .ok_or(Error::InvalidStoredValue("physical table id exhausted"))?,
                    );
                    for column in table.columns.values() {
                        next_physical_column_id =
                            next_physical_column_id.max(column.0.checked_add(1).ok_or(
                                Error::InvalidStoredValue("physical column id exhausted"),
                            )?);
                    }
                }
                let staged = StagedSchemaLineage {
                    catalogue_seq: next,
                    publication: publication.clone(),
                    alias: self.next_schema_version_alias()?,
                    mapping,
                };
                let mut candidate_mappings = self.catalogue.physical_mappings.clone();
                candidate_mappings.insert(staged.publication.schema.id, staged.mapping.clone());
                let mut candidate_aliases = self.catalogue.schema_version_aliases.clone();
                candidate_aliases.insert(staged.publication.schema.id, staged.alias);
                validate_scalar_enum_case_provenance(&candidate_mappings, &candidate_aliases)?;
                validate_payload_enum_case_provenance(&candidate_mappings, &candidate_aliases)?;
                self.persist_catalogue_schema_lineage(&staged).await?;
                self.catalogue.next_physical_table_id = next_physical_table_id;
                self.catalogue.next_physical_column_id = next_physical_column_id;
                self.catalogue.staged_lineages.insert(next, staged.clone());
                staged
            };

            // A new logical column widens the shared physical current-row
            // descriptor. Existing prepared/maintained graphs embed the old
            // fixed projection output, so they must be rebuilt after the
            // activation commits. A pure new variant over the same physical
            // columns remains safe to refresh in place.
            let widens_shared_current_descriptor = staged.mapping.tables.values().any(|target| {
                let existing_columns = self
                    .catalogue
                    .physical_mappings
                    .values()
                    .flat_map(|mapping| mapping.tables.values())
                    .filter(|existing| existing.table_id == target.table_id)
                    .flat_map(|existing| existing.columns.values().copied())
                    .collect::<BTreeSet<_>>();
                !existing_columns.is_empty()
                    && target
                        .columns
                        .values()
                        .any(|column| !existing_columns.contains(column))
            });

            #[cfg(any(test, feature = "testing"))]
            if self.catalogue_activation_failpoint
                == Some(CatalogueActivationFailpoint::AfterStaged)
            {
                self.catalogue_activation_failpoint = None;
                self.catalogue_activation_failed = true;
                return Err(Error::CatalogueActivationFailed);
            }

            self.install_staged_schema_lineage_in_memory(&staged);
            // A widened lineage adds new variant cases to existing physical
            // current tables. Install those cases in the live registry rather
            // than reopening the database: a reopen drops active history and
            // maintained-subscription receivers even when their output shape
            // remains compatible.
            if self.synchronize_physical_version_tables().await.is_err() {
                self.remove_staged_schema_lineage_from_memory(&staged);
                self.catalogue_activation_failed = true;
                return Err(Error::CatalogueActivationFailed);
            }
            // Closed raw receivers can otherwise make a cold server look live
            // until a later non-empty notification. Prune them explicitly;
            // then rebuild only when no observable subscription handle would
            // be disconnected. Live handles use the in-place cases above.
            if mode == CatalogueActivationMode::Live
                && self.database.prune_dropped_subscriptions().await.is_err()
            {
                self.remove_staged_schema_lineage_from_memory(&staged);
                self.catalogue_activation_failed = true;
                return Err(Error::CatalogueActivationFailed);
            }
            let rebuild_cold_runtime = mode == CatalogueActivationMode::ColdOpen
                || self.database.runtime_stats().active_subscriptions == 0;
            if rebuild_cold_runtime && self.rebuild_database_slot().await.is_err() {
                self.remove_staged_schema_lineage_from_memory(&staged);
                self.catalogue_activation_failed = true;
                return Err(Error::CatalogueActivationFailed);
            }
            if widens_shared_current_descriptor && !rebuild_cold_runtime {
                // Peer-serving and maintained caches are compiled against the
                // shared current-row descriptor too. Retire those handles now;
                // their owners rebuild from the new runtime token below. Raw
                // history subscriptions remain attached to the live Groove
                // registry and keep receiving compatible projected rows.
                self.invalidate_runtime_handles_after_database_rebuild();
            }
            #[cfg(any(test, feature = "testing"))]
            if self.catalogue_activation_failpoint
                == Some(CatalogueActivationFailpoint::AfterRegistration)
            {
                self.catalogue_activation_failpoint = None;
                self.remove_staged_schema_lineage_from_memory(&staged);
                self.catalogue_activation_failed = true;
                return Err(Error::CatalogueActivationFailed);
            }
            let mut batch = self.database.open_batch();
            Self::write_active_schema_lineage_to_batch(&mut batch, &staged)?;
            let persistence = async {
                let applied = self.database.apply_batch(batch).await?;
                let persisted = applied.persist().await;
                self.database.finish_persistence(persisted)?;
                Ok::<_, groove::db::Error>(())
            }
            .await;
            if persistence.is_err() {
                self.remove_staged_schema_lineage_from_memory(&staged);
                self.catalogue_activation_failed = true;
                return Err(Error::CatalogueActivationFailed);
            }
            self.catalogue.staged_lineages.remove(&next);
            self.catalogue.pending_lineages.remove(&next);
            self.catalogue
                .active_lineages_by_target
                .insert(staged.publication.schema.id, staged.clone());
            self.catalogue.active_catalogue_seq = next;
            if mode == CatalogueActivationMode::Live && widens_shared_current_descriptor {
                self.groove_runtime_token = next_groove_runtime_token();
            }
            if mode == CatalogueActivationMode::Live {
                outcome.value.push(SyncMessage::CatalogueAck(CatalogueAck {
                    revision: Some(next),
                    schema: Some(staged.publication.schema.id),
                    lens: staged
                        .publication
                        .predecessors
                        .first()
                        .filter(|_| staged.publication.predecessors.len() == 1)
                        .map(|p| p.lens.id),
                    applied: true,
                }));
                outcome.extend(self.drain_parked_commit_units().await?);
                self.drain_parked_relay_commit_units().await?;
                self.drain_parked_shape_registrations()?;
            }
        }
        Ok(outcome)
    }

    fn install_staged_schema_lineage_in_memory(&mut self, staged: &StagedSchemaLineage) {
        self.catalogue.catalogue_schemas.insert(
            staged.publication.schema.id,
            staged.publication.schema.clone(),
        );
        for predecessor in &staged.publication.predecessors {
            self.catalogue
                .catalogue_lenses
                .insert(predecessor.lens.id, predecessor.lens.clone());
        }
        self.catalogue
            .schema_version_aliases
            .insert(staged.publication.schema.id, staged.alias);
        self.catalogue
            .physical_mappings
            .insert(staged.publication.schema.id, staged.mapping.clone());
        self.catalogue.lens_path_cache.clear();
        self.catalogue.compiled_lens_cache.clear();
        self.catalogue.physical_write_plan_cache.clear();
        self.catalogue.physical_current_winner_projections.clear();
        self.query.version_storage_sources_cache.clear();
        self.query.query_shape_cache.clear();
        self.query.compiled_query_program_cache.clear();
        self.query.query_program_templates.clear();
        self.query.supported_query_program_requests.clear();
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
        self.query.policy_authorization_graph_replacements.clear();
    }

    fn remove_staged_schema_lineage_from_memory(&mut self, staged: &StagedSchemaLineage) {
        self.catalogue
            .catalogue_schemas
            .remove(&staged.publication.schema.id);
        for predecessor in &staged.publication.predecessors {
            self.catalogue.catalogue_lenses.remove(&predecessor.lens.id);
        }
        self.catalogue
            .schema_version_aliases
            .remove(&staged.publication.schema.id);
        self.catalogue
            .physical_mappings
            .remove(&staged.publication.schema.id);
        self.catalogue.lens_path_cache.clear();
        self.catalogue.compiled_lens_cache.clear();
        self.catalogue.physical_write_plan_cache.clear();
        self.catalogue.physical_current_winner_projections.clear();
        self.query.version_storage_sources_cache.clear();
        self.query.query_shape_cache.clear();
        self.query.compiled_query_program_cache.clear();
        self.query.query_program_templates.clear();
        self.query.supported_query_program_requests.clear();
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
        self.query.policy_authorization_graph_replacements.clear();
    }

    async fn apply_publish_lens(
        &mut self,
        author: AuthorSubject,
        ingest_context: Option<CommitUnitIngestContext>,
        lens: MigrationLens,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_admin(author, ingest_context)?;
        if lens.id != lens.content_id() {
            return Err(Error::InvalidCatalogueUpdate(
                "lens id does not match lens payload",
            ));
        }
        if !self.catalogue.catalogue_schemas.contains_key(&lens.source)
            || !self.catalogue.catalogue_schemas.contains_key(&lens.target)
        {
            return Err(Error::InvalidCatalogueUpdate("lens endpoint is unknown"));
        }
        self.validate_migration_lens(&lens)?;
        let installed = !self.catalogue.catalogue_lenses.contains_key(&lens.id);
        if installed {
            let candidate = self.reconcile_physical_mapping_for_lens(&lens)?;
            let authoritative = self.catalogue.physical_mappings.get(&lens.target).ok_or(
                Error::InvalidStoredValue("authoritative physical mapping missing"),
            )?;
            if candidate != *authoritative {
                return Err(Error::InvalidCatalogueUpdate(
                    "cross-lens conflicts with authoritative physical mapping",
                ));
            }
        }
        self.persist_catalogue_lens_with_physical_metadata(&lens, None)
            .await?;
        if installed {
            self.catalogue
                .catalogue_lenses
                .insert(lens.id, lens.clone());
        }
        self.catalogue.lens_path_cache.clear();
        self.catalogue.compiled_lens_cache.clear();
        self.query.version_storage_sources_cache.clear();
        self.catalogue.physical_current_winner_projections.clear();
        self.query.query_shape_cache.clear();
        self.query.compiled_query_program_cache.clear();
        self.query.query_program_templates.clear();
        self.query.supported_query_program_requests.clear();
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
        self.query.policy_authorization_graph_replacements.clear();
        // Both endpoint schemas are already Active and their agreeing physical
        // projection cases were registered during activation. A cross-lens adds
        // a catalogue path only; re-registering those cases is unnecessary and
        // Groove rejects it as a duplicate variant projection.
        Ok(vec![SyncMessage::CatalogueAck(CatalogueAck {
            revision: None,
            schema: None,
            lens: Some(lens.id),
            applied: true,
        })])
    }

    fn require_catalogue_admin(
        &self,
        _claimed_author: AuthorSubject,
        ingest_context: Option<CommitUnitIngestContext>,
    ) -> Result<(), Error> {
        if matches!(
            ingest_context,
            Some(context)
                if context.identity == AuthorSubject::SYSTEM
                    && matches!(context.trust, CommitUnitTrust::TrustedBackend | CommitUnitTrust::TrustedAuthority)
        ) {
            Ok(())
        } else {
            Err(Error::UnauthorizedCatalogueUpdate)
        }
    }

    fn validate_migration_lens(&self, lens: &MigrationLens) -> Result<(), Error> {
        let source = self
            .catalogue
            .catalogue_schemas
            .get(&lens.source)
            .ok_or(Error::InvalidCatalogueUpdate("lens endpoint is unknown"))?;
        let target = self
            .catalogue
            .catalogue_schemas
            .get(&lens.target)
            .ok_or(Error::InvalidCatalogueUpdate("lens endpoint is unknown"))?;
        Self::validate_migration_lens_between(lens, source, target)
    }

    pub(super) fn validate_migration_lens_between(
        lens: &MigrationLens,
        source: &SchemaVersion,
        target: &SchemaVersion,
    ) -> Result<(), Error> {
        crate::node::migration_validation::validate_migration_lens_between(lens, source, target)
    }

    pub(super) fn validate_lineage_table_partition(
        source: &JazzSchema,
        target: &JazzSchema,
        lens: &MigrationLens,
        new_tables: &[String],
        dropped_tables: &[String],
    ) -> Result<(), Error> {
        crate::node::migration_validation::validate_lineage_table_partition(
            source, target, lens, new_tables, dropped_tables,
        )
    }

    pub(super) fn validate_schema_lineage_publication_bounds(
        publication: &SchemaLineagePublication,
    ) -> Result<(), Error> {
        let declaration_count = publication
            .predecessors
            .iter()
            .map(|p| {
                p.lens
                    .table_lenses
                    .len()
                    .saturating_add(p.new_tables.len())
                    .saturating_add(p.dropped_tables.len())
            })
            .fold(publication.predecessors.len(), usize::saturating_add);
        let operation_count = publication
            .predecessors
            .iter()
            .flat_map(|p| &p.lens.table_lenses)
            .map(|table| table.ops.len())
            .fold(0usize, usize::saturating_add);
        let names_in_bounds = publication.predecessors.iter().all(|p| {
            p.new_tables
                .iter()
                .chain(&p.dropped_tables)
                .chain(
                    p.lens
                        .table_lenses
                        .iter()
                        .flat_map(|table| [&table.source_table, &table.target_table]),
                )
                .all(|name| !name.is_empty() && name.len() <= MAX_SCHEMA_LINEAGE_NAME_BYTES)
        });
        if declaration_count > MAX_SCHEMA_LINEAGE_DECLARATIONS
            || operation_count > MAX_SCHEMA_LINEAGE_OPS
            || !names_in_bounds
        {
            return Err(Error::InvalidCatalogueUpdate(
                "schema lineage publication exceeds structural limits",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_schema_lineage_publication(
        publication: &SchemaLineagePublication,
    ) -> Result<(), Error> {
        Self::validate_schema_lineage_publication_bounds(publication)?;
        if publication.id != publication.content_id() {
            return Err(Error::InvalidCatalogueUpdate(
                "schema lineage publication id mismatch",
            ));
        }
        publication
            .physical_identities
            .validate_for_schema(&publication.schema.schema)
            .map_err(Error::InvalidCatalogueUpdate)?;
        if publication.schema.id != publication.schema.schema.version_id() {
            return Err(Error::InvalidCatalogueUpdate(
                "schema id does not match schema payload",
            ));
        }
        if publication.predecessors.is_empty() {
            return Err(Error::InvalidCatalogueUpdate(
                "schema requires a predecessor",
            ));
        }
        let mut sources = BTreeSet::new();
        for predecessor in &publication.predecessors {
            let lens = &predecessor.lens;
            if !sources.insert(lens.source) || lens.source == publication.schema.id {
                return Err(Error::InvalidCatalogueUpdate(
                    "duplicate or self schema predecessor",
                ));
            }
            if lens.id != lens.content_id() {
                return Err(Error::InvalidCatalogueUpdate(
                    "lens id does not match lens payload",
                ));
            }
            if lens.target != publication.schema.id {
                return Err(Error::InvalidCatalogueUpdate(
                    "lineage lens target does not match schema",
                ));
            }
        }
        Ok(())
    }
}
