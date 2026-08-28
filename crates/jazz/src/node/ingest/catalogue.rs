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
                edge_authority: false,
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
        Box::pin(async move {
            // A dynamic edge has exactly one admissible pre-ready transition: the
            // authenticated upstream invokes `apply_trusted_catalogue_snapshot`
            // directly.  Incremental catalogue/data/branch traffic has no
            // authority lineage to validate against and must not leave durable
            // pending rows that poison a later reopen.
            self.require_catalogue_ready()?;
            if self.catalogue_activation_failed {
                return Err(Error::CatalogueActivationFailed);
            }
            match message {
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
                        && context.trust == CommitUnitTrust::TrustedBackend
                    {
                        self.set_session_claims(identity, claims);
                    }
                    Ok(PublicationOutcome::settled(Vec::new()))
                }
                SyncMessage::CommitUnit { tx, versions } => {
                    if ingest_context.is_some() {
                        let descriptors = version_indirect_descriptors(&versions);
                        self.current_staged_ids_for_descriptors(&descriptors, true)
                            .await?;
                    }
                    let now_ms = if ingest_context.is_some() {
                        web_time::SystemTime::now()
                            .duration_since(web_time::UNIX_EPOCH)
                            .map_err(|_| {
                                Error::InvalidStoredValue("authority clock precedes Unix epoch")
                            })?
                            .as_millis()
                            .try_into()
                            .map_err(|_| {
                                Error::InvalidStoredValue(
                                    "authority clock exceeds u64 milliseconds",
                                )
                            })?
                    } else {
                        tx.tx_id.time.physical_ms()
                    };
                    self.ingest_commit_unit_with_context(tx, versions, now_ms, ingest_context)
                        .await
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
                    reset_result_set,
                    version_carriers,
                    peer_payload_inventory,
                    result_member_adds,
                    result_member_removes,
                    terminal_operations,
                    program_fact_adds,
                    program_fact_removes,
                }) => {
                    self.apply_view_update(ViewUpdateParts {
                        subscription,
                        settled_through,
                        defer_settlement: false,
                        reset_result_set,
                        version_carriers,
                        peer_complete_tx_payload_refs: peer_payload_inventory.complete_tx_payloads,
                        authorization_progress: peer_payload_inventory.authorization_progress,
                        opening_pending: peer_payload_inventory.opening_pending,
                        result_member_adds,
                        result_member_removes,
                        terminal_operations,
                        program_fact_adds,
                        program_fact_removes,
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
                    self.register_shape(shape_id, ast)?;
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
                SyncMessage::SetCurrentWriteSchema { author, pointer } => self
                    .apply_set_current_write_schema(author, ingest_context, pointer)
                    .await
                    .map(PublicationOutcome::settled),
                SyncMessage::CatalogueAck(_) => Ok(PublicationOutcome::settled(Vec::new())),
                SyncMessage::ChunkRequestBatch(_) | SyncMessage::ChunkResponseBatch(_) => Err(
                    Error::UnsupportedSyncMessage("chunk traffic requires peer link context"),
                ),
                SyncMessage::PermissionAdviceRequest { .. }
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
        let active_schema_changed = schema.id == self.catalogue.current_write_schema.schema
            && self
                .catalogue
                .catalogue_schemas
                .get(&schema.id)
                .is_some_and(|current| current.schema != schema.schema);
        self.catalogue
            .catalogue_schemas
            .insert(schema.id, schema.clone());
        self.query.version_storage_sources_cache.clear();
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
        if schema.id == self.catalogue.current_schema_version_id {
            self.catalogue.schema = schema.schema.clone();
        }
        self.persist_catalogue_schema(&schema).await?;
        self.ensure_provisional_physical_mapping(schema.id).await?;
        self.ensure_schema_version_alias(schema.id).await?;
        self.synchronize_physical_version_tables().await?;
        if active_schema_changed {
            // Policy declarations are intentionally outside the schema version
            // identity. Invalidate maintained handles when that same-version
            // payload changes so live subscriptions rebuild their authorization
            // graph without reopening storage through the old catalogue row.
            self.groove_runtime_token = next_groove_runtime_token();
        }
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
        let lens = &publication.lens;
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
                    lens: Some(lens.id),
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
            if let Some(source) = self.catalogue.catalogue_schemas.get(&lens.source) {
                Self::validate_migration_lens_between(lens, source, schema)?;
                Self::validate_lineage_table_partition(
                    &source.schema,
                    &schema.schema,
                    lens,
                    &publication.new_tables,
                    &publication.dropped_tables,
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
            let publication = pending.publication;
            let Some(source) = self
                .catalogue
                .catalogue_schemas
                .get(&publication.lens.source)
                .cloned()
            else {
                break;
            };
            let validation = Self::validate_migration_lens_between(
                &publication.lens,
                &source,
                &publication.schema,
            )
            .and_then(|()| {
                Self::validate_lineage_table_partition(
                    &source.schema,
                    &publication.schema.schema,
                    &publication.lens,
                    &publication.new_tables,
                    &publication.dropped_tables,
                )
            });
            if validation.is_err() {
                self.remove_pending_schema_lineage(next, publication.id)
                    .await?;
                break;
            }
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
                let fresh = allocate_provisional_physical_mapping(
                    &publication.schema.schema,
                    publication.physical_identities.clone(),
                    &mut self.catalogue.next_physical_table_id,
                    &mut self.catalogue.next_physical_column_id,
                )?;
                let mapping = self.reconcile_physical_mapping_for_lens_payload(
                    &publication.lens,
                    &publication.schema,
                    &fresh,
                )?;
                let staged = StagedSchemaLineage {
                    catalogue_seq: next,
                    publication: publication.clone(),
                    alias: self.next_schema_version_alias()?,
                    mapping,
                };
                self.persist_catalogue_schema_lineage(&staged).await?;
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
                    lens: Some(staged.publication.lens.id),
                    applied: true,
                }));
                outcome.extend(self.drain_parked_commit_units().await?);
                self.drain_parked_relay_commit_units().await?;
                self.drain_parked_shape_registrations()?;
                outcome
                    .value
                    .extend(self.drain_pending_catalogue_pointers().await?);
            }
        }
        Ok(outcome)
    }

    fn install_staged_schema_lineage_in_memory(&mut self, staged: &StagedSchemaLineage) {
        self.catalogue.catalogue_schemas.insert(
            staged.publication.schema.id,
            staged.publication.schema.clone(),
        );
        self.catalogue
            .catalogue_lenses
            .insert(staged.publication.lens.id, staged.publication.lens.clone());
        self.catalogue
            .schema_version_aliases
            .insert(staged.publication.schema.id, staged.alias);
        self.catalogue
            .physical_mappings
            .insert(staged.publication.schema.id, staged.mapping.clone());
        self.catalogue.lens_path_cache.clear();
        self.catalogue.compiled_lens_cache.clear();
        self.catalogue.physical_write_plan_cache.clear();
        self.query.version_storage_sources_cache.clear();
        self.query.query_shape_cache.clear();
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
    }

    fn remove_staged_schema_lineage_from_memory(&mut self, staged: &StagedSchemaLineage) {
        self.catalogue
            .catalogue_schemas
            .remove(&staged.publication.schema.id);
        self.catalogue
            .catalogue_lenses
            .remove(&staged.publication.lens.id);
        self.catalogue
            .schema_version_aliases
            .remove(&staged.publication.schema.id);
        self.catalogue
            .physical_mappings
            .remove(&staged.publication.schema.id);
        self.catalogue.lens_path_cache.clear();
        self.catalogue.compiled_lens_cache.clear();
        self.catalogue.physical_write_plan_cache.clear();
        self.query.version_storage_sources_cache.clear();
        self.query.query_shape_cache.clear();
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
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
        self.query.query_shape_cache.clear();
        self.query.read_policy_authorization_request_cache.clear();
        self.query.policy_authorization_graph_cache.clear();
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

    async fn apply_set_current_write_schema(
        &mut self,
        author: AuthorSubject,
        ingest_context: Option<CommitUnitIngestContext>,
        pointer: CurrentWriteSchema,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_admin(author, ingest_context)?;
        if !self
            .catalogue
            .catalogue_schemas
            .contains_key(&pointer.schema)
        {
            self.persist_pending_catalogue_pointer(pointer).await?;
            self.catalogue
                .pending_write_pointers
                .insert(pointer.revision, pointer);
            return Ok(Vec::new());
        }
        Ok(vec![self.apply_active_catalogue_pointer(pointer).await?])
    }

    async fn apply_active_catalogue_pointer(
        &mut self,
        pointer: CurrentWriteSchema,
    ) -> Result<SyncMessage, Error> {
        let applied = pointer.revision > self.catalogue.current_write_schema.revision;
        if applied {
            self.catalogue.current_write_schema = pointer;
            self.persist_catalogue_pointer(pointer).await?;
            self.query.version_storage_sources_cache.clear();
            self.query.read_policy_authorization_request_cache.clear();
            self.query.policy_authorization_graph_cache.clear();
            let active_schema = self
                .catalogue
                .catalogue_schemas
                .get(&pointer.schema)
                .ok_or(Error::InvalidStoredValue(
                    "current write schema payload missing",
                ))?;
            if pointer.schema == self.catalogue.current_schema_version_id {
                self.catalogue.schema = active_schema.schema.clone();
            }
        }
        Ok(SyncMessage::CatalogueAck(CatalogueAck {
            revision: Some(pointer.revision),
            schema: Some(pointer.schema),
            lens: None,
            applied,
        }))
    }

    pub(super) async fn recover_pending_catalogue_pointers(&mut self) -> Result<(), Error> {
        self.apply_pending_catalogue_pointers(CatalogueActivationMode::ColdOpen)
            .await
            .map(|_| ())
    }

    pub(super) async fn drain_pending_catalogue_pointers(
        &mut self,
    ) -> Result<Vec<SyncMessage>, Error> {
        self.apply_pending_catalogue_pointers(CatalogueActivationMode::Live)
            .await
    }

    async fn apply_pending_catalogue_pointers(
        &mut self,
        mode: CatalogueActivationMode,
    ) -> Result<Vec<SyncMessage>, Error> {
        let ready = self
            .catalogue
            .pending_write_pointers
            .iter()
            .filter(|(_, pointer)| {
                self.catalogue
                    .catalogue_schemas
                    .contains_key(&pointer.schema)
            })
            .map(|(revision, pointer)| (*revision, *pointer))
            .collect::<Vec<_>>();
        let mut out = Vec::new();
        for (revision, pointer) in ready {
            let message = self.apply_active_catalogue_pointer(pointer).await?;
            if mode == CatalogueActivationMode::Live {
                out.push(message);
            }
            self.catalogue.pending_write_pointers.remove(&revision);
        }
        Ok(out)
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
                    && context.trust == CommitUnitTrust::TrustedBackend
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
        for table_lens in &lens.table_lenses {
            let source_table = source
                .schema
                .tables
                .iter()
                .find(|table| table.name == table_lens.source_table)
                .ok_or(Error::InvalidCatalogueUpdate("table lens is unknown"))?;
            let target_table = target
                .schema
                .tables
                .iter()
                .find(|table| table.name == table_lens.target_table)
                .ok_or(Error::InvalidCatalogueUpdate("table lens is unknown"))?;
            let target_bindings = target_table
                .branch_by
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            let mut branch_columns = source_table
                .branch_by
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            let mut columns = source_table
                .columns
                .iter()
                .cloned()
                .map(|column| (column.name.clone(), column))
                .collect::<BTreeMap<_, _>>();
            let mut saw_table_rename = source_table.name == target_table.name;
            for op in &table_lens.ops {
                match op {
                    LensOp::RenameTable { from, to } => {
                        if saw_table_rename
                            || from != &source_table.name
                            || to != &target_table.name
                        {
                            return Err(Error::InvalidCatalogueUpdate(
                                "table rename does not match lens endpoints",
                            ));
                        }
                        saw_table_rename = true;
                    }
                    LensOp::RenameColumn { from, to } => {
                        if columns.contains_key(to) {
                            return Err(Error::InvalidCatalogueUpdate(
                                "column rename collides with existing column",
                            ));
                        }
                        let mut column = columns.remove(from).ok_or(
                            Error::InvalidCatalogueUpdate("column rename source is unknown"),
                        )?;
                        column.name = to.clone();
                        columns.insert(to.clone(), column);
                        if branch_columns.remove(from) {
                            branch_columns.insert(to.clone());
                        }
                    }
                    LensOp::CopyColumn { from, to } => {
                        if columns.contains_key(to) {
                            return Err(Error::InvalidCatalogueUpdate(
                                "column copy collides with existing column",
                            ));
                        }
                        let mut column =
                            columns
                                .get(from)
                                .cloned()
                                .ok_or(Error::InvalidCatalogueUpdate(
                                    "column copy source is unknown",
                                ))?;
                        column.name = to.clone();
                        columns.insert(to.clone(), column);
                    }
                    LensOp::AddColumn { column, .. } => {
                        if columns.contains_key(column) {
                            return Err(Error::InvalidCatalogueUpdate(
                                "added column already exists",
                            ));
                        }
                        let target_column = target_table
                            .columns
                            .iter()
                            .find(|candidate| candidate.name == *column)
                            .cloned()
                            .ok_or(Error::InvalidCatalogueUpdate(
                                "added column is absent from target",
                            ))?;
                        columns.insert(column.clone(), target_column);
                        if target_bindings.contains(column) && columns[column].default.is_none() {
                            return Err(Error::InvalidCatalogueUpdate(
                                "added branch column requires a migration default",
                            ));
                        }
                    }
                    LensOp::DropColumn { column, .. } => {
                        if branch_columns.contains(column) {
                            return Err(Error::InvalidCatalogueUpdate(
                                "table branch columns cannot be removed",
                            ));
                        }
                        if columns.remove(column).is_none() {
                            return Err(Error::InvalidCatalogueUpdate(
                                "dropped column is absent from source",
                            ));
                        }
                    }
                    LensOp::TransformColumn { column, transform } => {
                        if branch_columns.contains(column) {
                            return Err(Error::InvalidCatalogueUpdate(
                                "branch column type and migration default are immutable",
                            ));
                        }
                        validate_transform_column(columns.get(column), transform)?;
                        let source_column =
                            columns.get(column).ok_or(Error::InvalidCatalogueUpdate(
                                "transformed column is absent from source",
                            ))?;
                        let target_column = target_table
                            .columns
                            .iter()
                            .find(|candidate| candidate.name == *column)
                            .ok_or(Error::InvalidCatalogueUpdate(
                                "transformed column is absent from target",
                            ))?;
                        if !physical_value_epoch_is_compatible(
                            &source_column.column_type,
                            &target_column.column_type,
                        ) || source_column.large_value_kind != target_column.large_value_kind {
                            return Err(Error::InvalidCatalogueUpdate(
                                "column transform changes physical value or large-value semantic kind",
                            ));
                        }
                        columns.insert(column.clone(), target_column.clone());
                    }
                    LensOp::RejectSourceDelta { .. } => {}
                }
            }
            if !saw_table_rename {
                return Err(Error::InvalidCatalogueUpdate(
                    "renamed table requires an explicit RenameTable operation",
                ));
            }
            if !branch_columns.is_subset(&target_bindings) {
                return Err(Error::InvalidCatalogueUpdate(
                    "table branch columns cannot be removed",
                ));
            }
            let target_columns = target_table
                .columns
                .iter()
                .cloned()
                .map(|column| (column.name.clone(), column))
                .collect::<BTreeMap<_, _>>();
            for branch_column in &branch_columns {
                let Some(source_column) = columns.get(branch_column) else {
                    continue;
                };
                let Some(target_column) = target_columns.get(branch_column) else {
                    continue;
                };
                if source_column.column_type != target_column.column_type
                    || source_column.default != target_column.default
                {
                    return Err(Error::InvalidCatalogueUpdate(
                        "branch column type and migration default are immutable",
                    ));
                }
            }
            if columns != target_columns {
                return Err(Error::InvalidCatalogueUpdate(
                    "lens operations do not reproduce target columns",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn validate_lineage_table_partition(
        source: &JazzSchema,
        target: &JazzSchema,
        lens: &MigrationLens,
        new_tables: &[String],
        dropped_tables: &[String],
    ) -> Result<(), Error> {
        let source_tables = source
            .tables
            .iter()
            .map(|table| table.name.clone())
            .collect::<BTreeSet<_>>();
        let target_tables = target
            .tables
            .iter()
            .map(|table| table.name.clone())
            .collect::<BTreeSet<_>>();
        let related_source = lens
            .table_lenses
            .iter()
            .map(|table| table.source_table.clone())
            .collect::<BTreeSet<_>>();
        let related_target = lens
            .table_lenses
            .iter()
            .map(|table| table.target_table.clone())
            .collect::<BTreeSet<_>>();
        let new = new_tables.iter().cloned().collect::<BTreeSet<_>>();
        let dropped = dropped_tables.iter().cloned().collect::<BTreeSet<_>>();
        if related_source.len() != lens.table_lenses.len()
            || related_target.len() != lens.table_lenses.len()
            || new.len() != new_tables.len()
            || dropped.len() != dropped_tables.len()
            || !related_source.is_disjoint(&dropped)
            || !related_target.is_disjoint(&new)
            || related_source
                .union(&dropped)
                .cloned()
                .collect::<BTreeSet<_>>()
                != source_tables
            || related_target.union(&new).cloned().collect::<BTreeSet<_>>() != target_tables
        {
            return Err(Error::InvalidCatalogueUpdate(
                "lineage table declarations do not partition schemas",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_schema_lineage_publication_bounds(
        publication: &SchemaLineagePublication,
    ) -> Result<(), Error> {
        let declaration_count = publication
            .lens
            .table_lenses
            .len()
            .saturating_add(publication.new_tables.len())
            .saturating_add(publication.dropped_tables.len());
        let operation_count = publication
            .lens
            .table_lenses
            .iter()
            .map(|table| table.ops.len())
            .sum::<usize>();
        let names_in_bounds = publication
            .new_tables
            .iter()
            .chain(&publication.dropped_tables)
            .chain(
                publication
                    .lens
                    .table_lenses
                    .iter()
                    .flat_map(|table| [&table.source_table, &table.target_table]),
            )
            .all(|name| !name.is_empty() && name.len() <= MAX_SCHEMA_LINEAGE_NAME_BYTES);
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
        if publication.schema.id != publication.schema.schema.version_id() {
            return Err(Error::InvalidCatalogueUpdate(
                "schema id does not match schema payload",
            ));
        }
        if publication.lens.id != publication.lens.content_id() {
            return Err(Error::InvalidCatalogueUpdate(
                "lens id does not match lens payload",
            ));
        }
        if publication.lens.target != publication.schema.id {
            return Err(Error::InvalidCatalogueUpdate(
                "lineage lens target does not match schema",
            ));
        }
        Ok(())
    }
}
