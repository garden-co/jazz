#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AuthorityCommitRequest {
    pub(super) tx: Transaction,
    pub(super) versions: Vec<VersionRecord>,
    pub(super) now_ms: u64,
    pub(super) ingest_context: Option<CommitUnitIngestContext>,
}

pub(crate) struct PreparedAuthorityCommit {
    request: AuthorityCommitRequest,
}

impl<S> NodeState<S>
where
    S: ResidentStorage,
{
    /// Acquire the durable-backed inputs that can affect authority validation.
    ///
    /// This phase may populate read/query caches, but it cannot advance a
    /// transaction, clock, IVM frontier, parking queue, or durable journal.
    /// Publication treats any later cold miss as an invariant violation.
    pub(crate) fn prepare_authority_commit(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
        ingest_context: Option<CommitUnitIngestContext>,
    ) -> Result<PreparedAuthorityCommit, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_ready()?;
        let versions = canonical_versions(versions);
        let tx_node_alias = self.prepare_node_alias(tx.tx_id.node)?;
        let mut will_reject = commit_unit_limit_violation(&versions).is_some()
            || !commit_unit_write_count_matches(&tx, versions.len())
            || self.malformed_authored_version_reason(&versions).is_some();
        if let Some(existing) = self.query_transaction(tx.tx_id)? {
            let _ = existing;
            let _ = self.query_versions_for_tx(tx.tx_id)?;
        }
        if commit_unit_limit_violation(&versions).is_none()
            && commit_unit_write_count_matches(&tx, versions.len())
            && self.malformed_authored_version_reason(&versions).is_none()
            && !matches!(
                tx.target_lineage,
                crate::tx::BranchLineage::Branch(branch_id)
                    if !self.branches.branches.contains_key(&branch_id)
            )
            && versions.iter().all(|version| {
                self.catalogue
                    .catalogue_schemas
                    .contains_key(&version.schema_version())
            })
        {
            let mut memo = IngestMemo::default();
            if self.missing_parent_refs_memo(&versions, &mut memo)?.is_empty() {
                will_reject |=
                    !self.commit_unit_satisfies_clock_condition(&tx, &versions, &mut memo)?;
                will_reject |=
                    tx.tx_id.time.physical_ms() > now_ms.saturating_add(SKEW_TOLERANCE_MS);
                will_reject |= self.cascade_root_for_versions(&versions).is_some();
                will_reject |= !self.commit_unit_satisfies_write_policies(
                    &tx,
                    &versions,
                    ingest_context,
                )?;
                if tx.kind == TxKind::Exclusive {
                    will_reject |= !self.validate_exclusive_commit_unit(&tx, &versions)?;
                }
                if tx.target_lineage == crate::tx::BranchLineage::Root {
                    let _ = self.merge_rows_for_versions(&versions)?;
                }
                for version in &versions {
                    let table = self.table_in_schema(version.table(), version.schema_version())?;
                    let layer = VersionLayer::for_record(version);
                    let _ = self.query_local_layer_winner(
                        &table.name,
                        version.row_uuid(),
                        layer,
                    )?;
                    let _ = self.query_global_layer_winner_in_schema(
                        version.schema_version(),
                        &table.name,
                        version.row_uuid(),
                        layer,
                    )?;
                    self.preload_global_change_slot(
                        version.schema_version(),
                        &table.name,
                        version.row_uuid(),
                        layer,
                        self.clock.next_global_seq,
                    )?;
                    if layer == VersionLayer::Content {
                        let table_id = self.physical_table_id_for_schema(
                            version.schema_version(),
                            &table.name,
                        )?;
                        let _ = self.read_merge_heads(table_id, version.row_uuid())?;
                    }
                    let ahead_table = self.physical_current_table_for_schema(
                        version.schema_version(),
                        &table.name,
                        layer,
                        PhysicalCurrentClass::Ahead,
                    )?;
                    let _ = self.database.primary_key_get_raw(
                        &ahead_table,
                        &[
                            Value::Uuid(version.row_uuid().0),
                            Value::U64(tx.tx_id.time.0),
                            Value::U64(tx_node_alias.0),
                        ],
                    )?;
                }
            }
        }
        if will_reject {
            self.prepare_rejection_cascade_inputs(tx.tx_id)?;
        }
        Ok(PreparedAuthorityCommit {
            request: AuthorityCommitRequest {
                tx,
                versions,
                now_ms,
                ingest_context,
            },
        })
    }

    pub(crate) fn publish_prepared_authority_commit(
        &mut self,
        prepared: PreparedAuthorityCommit,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        let AuthorityCommitRequest {
            tx,
            versions,
            now_ms,
            ingest_context,
        } = prepared.request;
        self.ingest_commit_unit_with_context(tx, versions, now_ms, ingest_context)
    }

    /// Ingest a commit unit as fate authority.
    pub fn ingest_commit_unit(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_ready()?;
        self.ingest_commit_unit_with_context(tx, versions, now_ms, None)
    }

    /// Ingest a commit unit as fate authority with an optional authenticated
    /// connection identity. SPEC/7 §7.2 evaluates policy against the connection
    /// subject; `made_by` is provenance unless the link is an untrusted session.
    pub fn ingest_commit_unit_with_context(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
        ingest_context: Option<CommitUnitIngestContext>,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_ready()?;
        if let Some(reason) = commit_unit_limit_violation(&versions) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(reason));
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if commit_unit_write_count_matches(&tx, versions.len())
            && let Some(reason) = self.malformed_authored_version_reason(&versions)
        {
            return self.reject_malformed_commit(tx, reason);
        }
        self.prepare_branch_target_partitions_if_ready(&tx, &versions)?;
        let mut updates = self.ingest_commit_unit_once(tx, versions, now_ms, ingest_context)?;
        updates.extend(self.drain_parked_commit_units()?);
        Ok(updates)
    }

    /// Ingest a mergeable commit unit as an edge authority.
    ///
    /// This applies the same structural and write-policy checks as the normal
    /// authority path, but records only edge durability: no global sequence is
    /// allocated until core later finalizes the edge-accepted unit.
    pub fn ingest_edge_authority_mergeable_commit_unit(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_ready()?;
        if commit_unit_limit_violation(&versions).is_none()
            && commit_unit_write_count_matches(&tx, versions.len())
            && let Some(reason) = self.malformed_authored_version_reason(&versions)
        {
            return self.reject_malformed_commit(tx, reason);
        }
        if commit_unit_limit_violation(&versions).is_none()
            && commit_unit_write_count_matches(&tx, versions.len())
        {
            self.prepare_branch_target_partitions_if_ready(&tx, &versions)?;
        }
        let mut updates =
            self.ingest_edge_authority_mergeable_commit_unit_once(tx, versions, now_ms, None)?;
        updates.extend(self.drain_parked_commit_units()?);
        Ok(updates)
    }

    /// Ingest a mergeable commit unit as an edge authority using an
    /// authenticated permission subject while preserving `made_by` provenance.
    pub fn ingest_edge_authority_mergeable_commit_unit_with_identity(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
        identity: AuthorId,
    ) -> Result<Vec<SyncMessage>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_ready()?;
        if commit_unit_limit_violation(&versions).is_none()
            && commit_unit_write_count_matches(&tx, versions.len())
            && let Some(reason) = self.malformed_authored_version_reason(&versions)
        {
            return self.reject_malformed_commit(tx, reason);
        }
        if commit_unit_limit_violation(&versions).is_none()
            && commit_unit_write_count_matches(&tx, versions.len())
        {
            self.prepare_branch_target_partitions_if_ready(&tx, &versions)?;
        }
        let ingest_context = Some(CommitUnitIngestContext {
            identity,
            trust: CommitUnitTrust::TrustedBackend,
            edge_authority: true,
        });
        let mut updates = self.ingest_edge_authority_mergeable_commit_unit_once(
            tx,
            versions,
            now_ms,
            ingest_context,
        )?;
        updates.extend(self.drain_parked_commit_units()?);
        Ok(updates)
    }

    /// Finalize a locally-authored pending mergeable commit as the global
    /// authority: assign the next global sequence and mark it Accepted/Global.
    ///
    /// This is the authority's self-acceptance of its own write — the path a
    /// `Core` `Db` takes when it commits through the facade (a client instead
    /// commits Pending/Local and learns its fate from upstream). It reuses the
    /// stored versions and does not re-run the
    /// authority validation the node already performed when it authored the
    /// commit. Idempotent: a non-pending transaction is left untouched.
    pub fn finalize_local_mergeable_commit(&mut self, tx_id: TxId) -> Result<(), Error> {
        self.require_catalogue_ready()?;
        let stored = self
            .query_transaction(tx_id)?
            .ok_or(Error::MissingTransaction(tx_id))?;
        if stored.tx.kind != TxKind::Mergeable {
            return Err(Error::UnsupportedCommitUnit(
                "self-finalize is mergeable-only",
            ));
        }
        if !matches!(stored.fate, Fate::Pending) {
            return Ok(());
        }
        let records = self
            .query_versions_for_tx(tx_id)?
            .into_iter()
            .map(|stored| self.version_record_from_row(&stored))
            .collect::<Result<Vec<_>, Error>>()?;
        let permission_subject = self
            .open_tx
            .local_permission_subjects
            .get(&tx_id)
            .copied()
            .unwrap_or(stored.tx.made_by);
        if !self.commit_unit_satisfies_write_policies(
            &Transaction {
                permission_subject: Some(permission_subject),
                ..stored.tx.clone()
            },
            &records,
            None,
        )? {
            let fate = Fate::Rejected(RejectionReason::AuthorizationDenied);
            self.ingest_rejected_transaction(stored.tx, fate)?;
            return Ok(());
        }
        let global_seq = self.clock.allocate_global_seq()?;
        self.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(global_seq),
            Some(DurabilityTier::Global),
        )?;
        if stored.tx.target_lineage == crate::tx::BranchLineage::Root {
            self.create_merge_versions_for(&records)?;
        }
        Ok(())
    }

    /// Finalize a locally-authored pending exclusive commit as the global
    /// authority, returning the accepted or rejected fate.
    ///
    /// Validation runs against the in-memory commit unit (`tx` + `versions`),
    /// NOT a re-query of the stored transaction: the stored transaction record
    /// does not persist `base_snapshot` or the read sets (they travel only on
    /// the commit unit), so re-querying would drop the §3.7 read evidence and
    /// spuriously reject. This mirrors the foreign authority path, which
    /// validates the arriving commit unit before it is ingested.
    pub fn finalize_local_exclusive_commit(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
    ) -> Result<Fate, Error> {
        self.require_catalogue_ready()?;
        let tx_id = tx.tx_id;
        if tx.kind != TxKind::Exclusive {
            return Err(Error::UnsupportedCommitUnit(
                "exclusive self-finalize requires an exclusive transaction",
            ));
        }
        let stored = self
            .query_transaction(tx_id)?
            .ok_or(Error::MissingTransaction(tx_id))?;
        if !matches!(stored.fate, Fate::Pending) {
            return Ok(stored.fate);
        }
        // Validate through the SAME authority path the core uses for an incoming
        // exclusive commit unit (§3.7): row/absent/predicate reads (INV-TX-16/17/18)
        // AND per-write first-committer-wins (INV-TX-20). Do not reimplement.
        if !self.validate_exclusive_commit_unit(&tx, &versions)? {
            let fate = Fate::Rejected(RejectionReason::ExclusiveConflict);
            self.ingest_rejected_transaction(tx, fate.clone())?;
            return Ok(fate);
        }
        let global_seq = self.clock.allocate_global_seq()?;
        self.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(global_seq),
            Some(DurabilityTier::Global),
        )?;
        if tx.target_lineage == crate::tx::BranchLineage::Root {
            self.create_merge_versions_for(&versions)?;
        }
        Ok(Fate::Accepted)
    }

    pub(super) fn finalize_edge_accepted_mergeable_commit_unit_once(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
    ) -> Result<Vec<SyncMessage>, Error> {
        let versions = canonical_versions(versions);
        let mut memo = IngestMemo::default();
        if tx.kind != TxKind::Mergeable {
            return Err(Error::UnsupportedCommitUnit(
                "edge-accepted finalization is mergeable-only",
            ));
        }
        if let Some(reason) = commit_unit_limit_violation(&versions) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(reason));
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if !commit_unit_write_count_matches(&tx, versions.len()) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(
                "commit unit version count does not match transaction n_total_writes".to_owned(),
            ));
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if let Some(reason) = self.malformed_authored_version_reason(&versions) {
            return self.reject_malformed_commit(tx, reason);
        }
        if let Some(existing) = self.query_transaction(tx.tx_id)? {
            let mut existing_versions = self
                .query_versions_for_tx(tx.tx_id)?
                .into_iter()
                .map(|stored| self.version_record_from_row(&stored))
                .collect::<Result<Vec<_>, Error>>()?;
            existing_versions.sort();
            if !known_transaction_payload_matches(&existing.tx, &tx)
                || existing_versions != versions
            {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            if matches!(existing.fate, Fate::Accepted)
                && existing.global_seq.is_some()
                && existing.durability >= DurabilityTier::Global
            {
                return Ok(vec![SyncMessage::FateUpdate {
                    tx_id: tx.tx_id,
                    fate: existing.fate.clone(),
                    global_seq: existing.global_seq,
                    durability: fate_update_durability_claim(&existing.fate, existing.durability),
                }]);
            }
            if matches!(existing.fate, Fate::Rejected(_)) {
                return Ok(vec![SyncMessage::FateUpdate {
                    tx_id: tx.tx_id,
                    fate: existing.fate.clone(),
                    global_seq: existing.global_seq,
                    durability: fate_update_durability_claim(&existing.fate, existing.durability),
                }]);
            }
        }
        if self.park_commit_unit_if_missing_branch_metadata_with_mode(
            &tx,
            &versions,
            now_ms,
            CommitUnitParkMode {
                ingress_role: ParkedIngressRole::EdgeAccepted,
                ..CommitUnitParkMode::default()
            },
        )? {
            return Ok(Vec::new());
        }
        if self.park_commit_unit_if_missing_schema_versions_with_mode(
            &tx,
            &versions,
            now_ms,
            CommitUnitParkMode {
                ingress_role: ParkedIngressRole::EdgeAccepted,
                ..CommitUnitParkMode::default()
            },
        )? {
            return Ok(Vec::new());
        }
        if self.park_commit_unit_if_missing_parents_with_mode(
            &tx,
            &versions,
            now_ms,
            &mut memo,
            CommitUnitParkMode {
                ingress_role: ParkedIngressRole::EdgeAccepted,
                ..CommitUnitParkMode::default()
            },
        )? {
            return Ok(Vec::new());
        }
        if !self.commit_unit_satisfies_clock_condition(&tx, &versions, &mut memo)? {
            let fate = Fate::Rejected(RejectionReason::CausalityViolation);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if tx.tx_id.time.physical_ms() > now_ms.saturating_add(SKEW_TOLERANCE_MS) {
            let fate = Fate::Rejected(RejectionReason::ClientClockTooFarAhead);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if let Some(root) = self.cascade_root_for_versions(&versions) {
            let fate = Fate::Rejected(RejectionReason::Cascade { root });
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            return Ok(vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }]);
        }
        let global_seq = self.clock.allocate_global_seq()?;
        let fate = Fate::Accepted;
        let durability = DurabilityTier::Global;
        let root_target = tx.target_lineage == crate::tx::BranchLineage::Root;
        let merge_rows = if root_target {
            self.merge_rows_for_versions(&versions)?
        } else {
            Vec::new()
        };
        self.ingest_known_transaction(
            tx.clone(),
            versions,
            fate.clone(),
            Some(global_seq),
            durability,
        )?;
        debug_assert_eq!(self.clock.applied_global_watermark, global_seq);
        if root_target {
            self.create_merge_versions_for_rows(merge_rows)?;
        }
        Ok(vec![SyncMessage::FateUpdate {
            tx_id: tx.tx_id,
            fate,
            global_seq: Some(global_seq),
            durability: Some(durability),
        }])
    }

    /// Ingest an unfated commit unit at a Local relay without assigning fate.
    pub fn ingest_relay_commit_unit(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
    ) -> Result<(), Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_ready()?;
        if commit_unit_limit_violation(&versions).is_some()
            || !commit_unit_write_count_matches(&tx, versions.len())
        {
            return Err(Error::UnsupportedCommitUnit("malformed relay commit unit"));
        }
        if self.malformed_authored_version_reason(&versions).is_some() {
            return Err(Error::UnsupportedCommitUnit("malformed relay commit unit"));
        }
        self.prepare_branch_target_partitions_if_ready(&tx, &versions)?;
        self.ingest_relay_commit_unit_once(tx, versions)?;
        self.drain_parked_relay_commit_units()?;
        Ok(())
    }

    pub(super) fn ingest_relay_commit_unit_once(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
    ) -> Result<(), Error> {
        if tx.kind != TxKind::Mergeable && tx.kind != TxKind::Exclusive {
            return Err(Error::UnsupportedCommitUnit("unsupported commit unit kind"));
        }
        let versions = canonical_versions(versions);
        if let Some(existing) = self.query_transaction(tx.tx_id)? {
            let mut existing_versions = self
                .query_versions_for_tx(tx.tx_id)?
                .into_iter()
                .map(|stored| self.version_record_from_row(&stored))
                .collect::<Result<Vec<_>, Error>>()?;
            existing_versions.sort();
            if !known_transaction_payload_matches(&existing.tx, &tx)
                || existing_versions != versions
            {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            return Ok(());
        }

        if !commit_unit_write_count_matches(&tx, versions.len()) {
            return Err(Error::UnsupportedCommitUnit(
                "commit unit version count does not match transaction n_total_writes",
            ));
        }
        let relay_mode = CommitUnitParkMode {
            ingress_role: ParkedIngressRole::Relay,
            ..CommitUnitParkMode::default()
        };
        if self.park_commit_unit_if_missing_branch_metadata_with_mode(
            &tx,
            &versions,
            u64::MAX - SKEW_TOLERANCE_MS,
            relay_mode,
        )? {
            return Ok(());
        }
        if self.park_commit_unit_if_missing_schema_versions_with_mode(
            &tx,
            &versions,
            u64::MAX - SKEW_TOLERANCE_MS,
            relay_mode,
        )? {
            return Ok(());
        }
        self.prepare_authored_schema_variants_for_commit(&versions)?;

        let mut memo = IngestMemo::default();
        if self.park_commit_unit_if_missing_parents_with_mode(
            &tx,
            &versions,
            u64::MAX - SKEW_TOLERANCE_MS,
            &mut memo,
            relay_mode,
        )? {
            return Ok(());
        }
        self.ingest_transaction_and_versions(
            tx,
            versions,
            Fate::Pending,
            None,
            DurabilityTier::Local,
        )
    }

    pub(super) fn ingest_commit_unit_once(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
        ingest_context: Option<CommitUnitIngestContext>,
    ) -> Result<Vec<SyncMessage>, Error> {
        let versions = canonical_versions(versions);
        let mut memo = IngestMemo::default();
        if !commit_unit_write_count_matches(&tx, versions.len()) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(
                "commit unit version count does not match transaction n_total_writes".to_owned(),
            ));
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if let Some(reason) = self.malformed_authored_version_reason(&versions) {
            return self.reject_malformed_commit(tx, reason);
        }
        if let Some(existing) = self.query_transaction(tx.tx_id)? {
            if tx.kind == TxKind::Exclusive || matches!(existing.fate, Fate::Rejected(_)) {
                if !known_transaction_payload_matches(&existing.tx, &tx) {
                    return Err(Error::ConflictingCommitUnit(tx.tx_id));
                }
                return Ok(vec![SyncMessage::FateUpdate {
                    tx_id: tx.tx_id,
                    fate: existing.fate.clone(),
                    global_seq: existing.global_seq,
                    durability: fate_update_durability_claim(&existing.fate, existing.durability),
                }]);
            }
            let mut existing_versions = self
                .query_versions_for_tx(tx.tx_id)?
                .into_iter()
                .map(|stored| self.version_record_from_row(&stored))
                .collect::<Result<Vec<_>, Error>>()?;
            existing_versions.sort();
            if !known_transaction_payload_matches(&existing.tx, &tx)
                || existing_versions != versions
            {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            if tx.kind == TxKind::Mergeable && matches!(existing.fate, Fate::Pending) {
                // Edge fate assignment can relay a mergeable unit as pending
                // before its permission scope settles, then re-enter authority
                // validation once that link-local subscription has hydrated.
            } else {
                return Ok(vec![SyncMessage::FateUpdate {
                    tx_id: tx.tx_id,
                    fate: existing.fate.clone(),
                    global_seq: existing.global_seq,
                    durability: fate_update_durability_claim(&existing.fate, existing.durability),
                }]);
            }
        }
        if self.park_commit_unit_if_missing_branch_metadata_with_mode(
            &tx,
            &versions,
            now_ms,
            CommitUnitParkMode {
                ingest_context,
                ..CommitUnitParkMode::default()
            },
        )? {
            return Ok(Vec::new());
        }
        if self.park_commit_unit_if_missing_schema_versions_with_mode(
            &tx,
            &versions,
            now_ms,
            CommitUnitParkMode {
                ingest_context,
                ..CommitUnitParkMode::default()
            },
        )? {
            return Ok(Vec::new());
        }
        self.prepare_authored_schema_variants_for_commit(&versions)?;
        if self.park_commit_unit_if_missing_parents_with_mode(
            &tx,
            &versions,
            now_ms,
            &mut memo,
            CommitUnitParkMode {
                ingest_context,
                ..CommitUnitParkMode::default()
            },
        )? {
            return Ok(Vec::new());
        }
        if !self.commit_unit_satisfies_clock_condition(&tx, &versions, &mut memo)? {
            let fate = Fate::Rejected(RejectionReason::CausalityViolation);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if tx.tx_id.time.physical_ms() > now_ms.saturating_add(SKEW_TOLERANCE_MS) {
            let fate = Fate::Rejected(RejectionReason::ClientClockTooFarAhead);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }

        if let Some(root) = self.cascade_root_for_versions(&versions) {
            let fate = Fate::Rejected(RejectionReason::Cascade { root });
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            return Ok(vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }]);
        }
        if !self.commit_unit_satisfies_write_policies(&tx, &versions, ingest_context)? {
            let fate = Fate::Rejected(RejectionReason::AuthorizationDenied);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if tx.kind == TxKind::Exclusive && !self.validate_exclusive_commit_unit(&tx, &versions)? {
            let fate = Fate::Rejected(RejectionReason::ExclusiveConflict);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            // This is a newly observed authority-side rejection. No stored
            // descendant can already point at it: descendants delivered before
            // the parent would park on the missing parent instead of entering
            // history. Later descendants will cascade when their parent state
            // is checked, so scanning all stored history here is redundant.
            return Ok(vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }]);
        }
        if tx.kind != TxKind::Mergeable && tx.kind != TxKind::Exclusive {
            return Err(Error::UnsupportedCommitUnit("unsupported commit unit kind"));
        }
        let global_seq = self.clock.allocate_global_seq()?;
        let fate = Fate::Accepted;
        let durability = DurabilityTier::Global;
        let root_target = tx.target_lineage == crate::tx::BranchLineage::Root;
        let merge_rows = if root_target {
            self.merge_rows_for_versions(&versions)?
        } else {
            Vec::new()
        };
        self.ingest_known_transaction(
            tx.clone(),
            versions,
            fate.clone(),
            Some(global_seq),
            durability,
        )?;
        debug_assert_eq!(self.clock.applied_global_watermark, global_seq);
        if root_target {
            self.create_merge_versions_for_rows(merge_rows)?;
        }
        Ok(vec![SyncMessage::FateUpdate {
            tx_id: tx.tx_id,
            fate,
            global_seq: Some(global_seq),
            durability: Some(durability),
        }])
    }

    pub(super) fn ingest_edge_authority_mergeable_commit_unit_once(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
        ingest_context: Option<CommitUnitIngestContext>,
    ) -> Result<Vec<SyncMessage>, Error> {
        let versions = canonical_versions(versions);
        let mut memo = IngestMemo::default();
        if tx.kind != TxKind::Mergeable {
            return Err(Error::UnsupportedCommitUnit(
                "edge authority only supports mergeable commit units",
            ));
        }
        if let Some(reason) = commit_unit_limit_violation(&versions) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(reason));
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if !commit_unit_write_count_matches(&tx, versions.len()) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(
                "commit unit version count does not match transaction n_total_writes".to_owned(),
            ));
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if let Some(reason) = self.malformed_authored_version_reason(&versions) {
            return self.reject_malformed_commit(tx, reason);
        }
        if let Some(existing) = self.query_transaction(tx.tx_id)? {
            let mut existing_versions = self
                .query_versions_for_tx(tx.tx_id)?
                .into_iter()
                .map(|stored| self.version_record_from_row(&stored))
                .collect::<Result<Vec<_>, Error>>()?;
            existing_versions.sort();
            if !known_transaction_payload_matches(&existing.tx, &tx)
                || existing_versions != versions
            {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            if !matches!(existing.fate, Fate::Pending) {
                return Ok(vec![SyncMessage::FateUpdate {
                    tx_id: tx.tx_id,
                    fate: existing.fate.clone(),
                    global_seq: existing.global_seq,
                    durability: fate_update_durability_claim(&existing.fate, existing.durability),
                }]);
            }
        }
        if self.park_commit_unit_if_missing_branch_metadata_with_mode(
            &tx,
            &versions,
            now_ms,
            CommitUnitParkMode {
                ingest_context,
                ingress_role: ParkedIngressRole::EdgeAuthority,
            },
        )? {
            return Ok(Vec::new());
        }
        if self.park_commit_unit_if_missing_schema_versions_with_mode(
            &tx,
            &versions,
            now_ms,
            CommitUnitParkMode {
                ingest_context,
                ingress_role: ParkedIngressRole::EdgeAuthority,
            },
        )? {
            return Ok(Vec::new());
        }
        self.prepare_authored_schema_variants_for_commit(&versions)?;
        if self.park_commit_unit_if_missing_parents_with_mode(
            &tx,
            &versions,
            now_ms,
            &mut memo,
            CommitUnitParkMode {
                ingest_context,
                ingress_role: ParkedIngressRole::EdgeAuthority,
            },
        )? {
            return Ok(Vec::new());
        }
        if !self.commit_unit_satisfies_clock_condition(&tx, &versions, &mut memo)? {
            let fate = Fate::Rejected(RejectionReason::CausalityViolation);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if tx.tx_id.time.physical_ms() > now_ms.saturating_add(SKEW_TOLERANCE_MS) {
            let fate = Fate::Rejected(RejectionReason::ClientClockTooFarAhead);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }
        if let Some(root) = self.cascade_root_for_versions(&versions) {
            let fate = Fate::Rejected(RejectionReason::Cascade { root });
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            return Ok(vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }]);
        }
        if !self.commit_unit_satisfies_write_policies(&tx, &versions, ingest_context)? {
            let fate = Fate::Rejected(RejectionReason::AuthorizationDenied);
            self.ingest_rejected_transaction(tx.clone(), fate.clone())?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_seq: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id)?);
            return Ok(updates);
        }

        let fate = Fate::Accepted;
        let durability = DurabilityTier::Edge;
        self.ingest_known_transaction(tx.clone(), versions, fate.clone(), None, durability)?;
        Ok(vec![SyncMessage::FateUpdate {
            tx_id: tx.tx_id,
            fate,
            global_seq: None,
            durability: Some(durability),
        }])
    }

    pub(super) fn ingest_known_transaction(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        fate: Fate,
        global_seq: Option<GlobalSeq>,
        durability: DurabilityTier,
    ) -> Result<(), Error> {
        self.require_catalogue_ready()?;
        debug_assert!(
            global_seq.is_none() || durability == DurabilityTier::Global,
            "a global sequence requires Global durability"
        );
        self.merge_tx_time(tx.tx_id.time);
        let versions = canonical_versions(versions);
        self.prepare_authored_schema_variants_for_commit(&versions)?;
        if let Some(existing) = self.query_transaction(tx.tx_id)? {
            let mut existing_versions = self
                .query_versions_for_tx(tx.tx_id)?
                .into_iter()
                .map(|stored| self.version_record_from_row(&stored))
                .collect::<Result<Vec<_>, Error>>()?;
            existing_versions.sort();
            if !known_transaction_payload_matches(&existing.tx, &tx) {
                return Err(Error::ConflictingCommitUnit(tx.tx_id));
            }
            let mut version_bundles = Vec::new();
            for version in versions {
                match existing_versions.iter().find(|existing| {
                    view_version_key_for_ingest(existing) == view_version_key_for_ingest(&version)
                }) {
                    Some(existing) if existing != &version => {
                        return Err(Error::ConflictingCommitUnit(tx.tx_id));
                    }
                    Some(_) => {}
                    None => version_bundles.push(version),
                }
            }
            if version_bundles.is_empty() {
                self.apply_fate_update(tx.tx_id, fate, global_seq, Some(durability))?;
                return Ok(());
            }
            return self.ingest_transaction_and_versions(
                tx,
                version_bundles,
                fate,
                global_seq,
                durability,
            );
        }
        self.ingest_transaction_and_versions(tx, versions, fate, global_seq, durability)
    }

    pub(super) fn stage_known_transaction(
        &mut self,
        batch: &mut DatabaseBatch,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        fate: Fate,
        global_seq: Option<GlobalSeq>,
        durability: DurabilityTier,
        staged_global_seqs: &mut Vec<GlobalSeq>,
    ) -> Result<(), Error> {
        debug_assert!(
            global_seq.is_none() || durability == DurabilityTier::Global,
            "a global sequence requires Global durability"
        );
        let versions = canonical_versions(versions);
        // This is entered by the batched ViewUpdate path, which has no
        // authority role and therefore cannot synthesize a fate. The caller
        // validates its entire frame before staging, and this is the central
        // storage-ingress backstop for other direct callers.
        self.validate_view_payload_versions(&versions)?;
        self.merge_tx_time(tx.tx_id.time);
        if self.query_transaction(tx.tx_id)?.is_some() {
            return self.ingest_known_transaction(tx, versions, fate, global_seq, durability);
        }
        let stored_versions = self.stage_transaction_and_versions_with_current_indexes(
            batch,
            tx.clone(),
            versions,
            fate.clone(),
            global_seq,
            durability,
            true,
        )?;
        self.finalize_staged_transaction_ingest(
            batch,
            tx.tx_id,
            &stored_versions,
            fate,
            global_seq,
            staged_global_seqs,
        )
    }

    pub(super) fn ingest_reset_view_bundle_refs_in_bulk(
        &mut self,
        bundles: &[VersionBundleRef<'_>],
    ) -> Result<BTreeSet<TxId>, Error> {
        let mut bundles_by_tx = BTreeMap::<TxId, Vec<VersionBundleRef<'_>>>::new();
        for bundle in bundles {
            validate_received_view_bundle_global_seq_durability(
                bundle.global_seq,
                bundle.durability,
            )?;
            bundles_by_tx
                .entry(bundle.tx.tx_id)
                .or_default()
                .push(*bundle);
        }
        let mut eligible = Vec::new();
        let mut loaded_tx_ids = BTreeSet::new();
        for (tx_id, tx_bundles) in bundles_by_tx {
            let first = tx_bundles[0];
            if tx_bundles.iter().any(|bundle| {
                bundle.tx != first.tx
                    || bundle.fate != first.fate
                    || bundle.global_seq != first.global_seq
                    || bundle.durability != first.durability
            }) {
                continue;
            }
            if *first.fate != Fate::Accepted {
                continue;
            }
            if first.global_seq.is_none() {
                continue;
            }
            if first.tx.kind != TxKind::Mergeable && first.tx.kind != TxKind::Exclusive {
                continue;
            }
            let mut unique_versions = BTreeMap::<
                (String, RowUuid, crate::ids::SchemaVersionId, bool),
                &VersionRecord,
            >::new();
            let mut duplicate_conflict = false;
            for bundle in &tx_bundles {
                for version in bundle.versions {
                    let key = (
                        version.table().to_owned(),
                        version.row_uuid(),
                        version.schema_version(),
                        version.deletion().is_some(),
                    );
                    match unique_versions.get(&key) {
                        Some(existing) if existing.record().raw() != version.record().raw() => {
                            duplicate_conflict = true;
                            break;
                        }
                        Some(_) => {}
                        None => {
                            unique_versions.insert(key, version);
                        }
                    }
                }
                if duplicate_conflict {
                    break;
                }
            }
            if duplicate_conflict {
                continue;
            }
            let version_count = unique_versions.len();
            if first.tx.kind == TxKind::Exclusive
                && usize::try_from(first.tx.n_total_writes).ok() != Some(version_count)
            {
                continue;
            }
            if self.query_transaction(tx_id)?.is_some() {
                continue;
            }
            let mut missing_refs = false;
            for bundle in &tx_bundles {
                if !self.missing_parent_refs(bundle.versions)?.is_empty() {
                    missing_refs = true;
                    break;
                }
            }
            if missing_refs {
                continue;
            }
            if loaded_tx_ids.insert(tx_id) {
                eligible.push(tx_bundles);
            }
        }
        if eligible.is_empty() {
            return Ok(loaded_tx_ids);
        }
        let eligible_versions = eligible
            .iter()
            .flat_map(|tx_bundles| tx_bundles.iter().flat_map(|bundle| bundle.versions))
            .cloned()
            .collect::<Vec<_>>();
        self.prepare_authored_schema_variants_for_commit(&eligible_versions)?;
        self.sync_metrics.receiver_bulk_ingest_commits += 1;
        self.sync_metrics.receiver_bulk_bundle_ingests += eligible.len() as u64;

        let mut batch = self.database.open_batch();
        let version_count = eligible
            .iter()
            .flatten()
            .map(|bundle| bundle.versions.len())
            .sum::<usize>();
        batch.reserve(eligible.len() + version_count.saturating_mul(2));
        let mut current_updates =
            BTreeMap::<(String, RowUuid, VersionLayer), (VersionRow, GlobalSeq)>::new();
        let mut content_versions = Vec::new();
        #[cfg(test)]
        let mut content_rows = BTreeSet::<(String, RowUuid)>::new();
        let mut applied_global_seqs = Vec::with_capacity(eligible.len());

        for tx_bundles in eligible {
            let first = tx_bundles[0];
            let tx = first.tx;
            let tx_node_alias = self.ensure_node_alias(tx.tx_id.node)?;
            let global_seq = first.global_seq.expect("checked above");
            applied_global_seqs.push(global_seq);
            batch.insert(
                "jazz_transactions",
                transaction_values(
                    tx_node_alias,
                    tx,
                    (*first.fate).clone(),
                    first.global_seq,
                    first.durability,
                ),
            );

            let mut unique_versions = BTreeMap::<
                (String, RowUuid, crate::ids::SchemaVersionId, bool),
                &VersionRecord,
            >::new();
            for bundle in &tx_bundles {
                for version in bundle.versions {
                    unique_versions
                        .entry((
                            version.table().to_owned(),
                            version.row_uuid(),
                            version.schema_version(),
                            version.deletion().is_some(),
                        ))
                        .or_insert(version);
                }
            }
            let mut versions = unique_versions.into_values().collect::<Vec<_>>();
            versions.sort();
            for version in versions {
                let author_schema = version.schema_version();
                let source_table_schema = self.table_in_schema(version.table(), author_schema)?;
                let schema_version_alias = self.ensure_schema_version_alias(author_schema)?;
                let stored = VersionRow::from_wire_with_schema_version(
                    &source_table_schema,
                    version,
                    tx_node_alias,
                    schema_version_alias,
                    tx.tx_id.time,
                    (author_schema != self.catalogue.current_schema_version_id)
                        .then_some(author_schema),
                )?;
                let (history_table, groove_record) = self.version_storage_write_binding(&stored)?;
                batch.insert_raw(
                    history_table.as_ref(),
                    self.version_storage_primary_key(&stored, BranchLineage::Root)?,
                    groove_record,
                );
                if stored.layer() == VersionLayer::Content {
                    content_versions.push(stored.clone());
                    #[cfg(test)]
                    content_rows.insert((stored.table().to_owned(), stored.row_uuid()));
                }

                let key = (stored.table().to_owned(), stored.row_uuid(), stored.layer());
                let existing_winner = current_updates.get(&key).map(|(previous, _)| {
                    (
                        previous,
                        self.version_tx_id(previous).expect("valid version tx id"),
                        previous.tx_time(),
                    )
                });
                if version_wins_over_open_winner(&stored, tx.tx_id, tx.tx_id.time, existing_winner)
                {
                    current_updates.insert(key, (stored, global_seq));
                }
            }
        }

        for (stored, global_seq) in current_updates.values() {
            self.write_global_current_update(&mut batch, stored, *global_seq)?;
        }
        self.write_merge_heads_for_bulk_content_versions(&mut batch, &content_versions)?;

        #[cfg(test)]
        let current_update_versions = current_updates
            .values()
            .map(|(stored, global_seq)| (stored.clone(), *global_seq))
            .collect::<Vec<_>>();
        for global_seq in &applied_global_seqs {
            self.clock.record_applied_global_seq(*global_seq);
        }
        self.commit_database_batch(batch)?;
        if let Some(tx_time) = loaded_tx_ids.iter().map(|tx_id| tx_id.time).max() {
            self.persist_storage_consistency_marker_through(tx_time)?;
        }
        #[cfg(test)]
        {
            if std::env::var_os("JAZZ_SKIP_BULK_INGEST_ASSERTS").is_none() {
                self.assert_merge_head_rows_match_history_for_test(&content_rows)?;
                self.assert_global_current_updates_match_history_for_test(
                    &current_update_versions,
                )?;
            }
        }
        for tx_id in &loaded_tx_ids {
            self.invalidate_tx_version_tables_cache(*tx_id);
        }
        Ok(loaded_tx_ids)
    }

}
