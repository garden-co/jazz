impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Ingest a commit unit as fate authority.
    pub async fn ingest_commit_unit(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_ready()?;
        self.ingest_commit_unit_with_context(tx, versions, now_ms, None).await
    }

    /// Ingest a commit unit as fate authority with an optional authenticated
    /// connection identity. SPEC/7 §7.2 evaluates policy against the connection
    /// subject; `made_by` is provenance unless the link is an untrusted session.
    pub async fn ingest_commit_unit_with_context(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
        ingest_context: Option<CommitUnitIngestContext>,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_ready()?;
        if let Some(reason) = commit_unit_limit_violation(&versions) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(reason));
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            let mut updates = PublicationOutcome::settled(vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }]);
            updates.value.extend(self.cascade_rejections_from(tx.tx_id).await?);
            return Ok(updates);
        }
        if commit_unit_write_count_matches(&tx, versions.len())
            && let Some(reason) = self.malformed_authored_version_reason(&versions)
        {
            return self
                .reject_malformed_commit(tx, reason)
                .await
                .map(PublicationOutcome::settled);
        }
        let clock_before_ingest = self.clock.clone();
        let mut updates = match self
            .ingest_commit_unit_once(tx, versions, now_ms, ingest_context)
            .await
        {
            Ok(updates) => updates,
            Err(error) => {
                self.restore_clock_after_failed_authority_ingest(clock_before_ingest);
                return Err(error);
            }
        };
        updates.extend(self.drain_parked_commit_units().await?);
        Ok(updates)
    }

    /// Undo speculative authority clock work after a rejected ingest without
    /// reusing a sequence that became durable before a later derived write
    /// failed. `ingest_commit_unit_once` records applied global progress only
    /// after the source transaction's canonical batch commits, so a changed
    /// progress set is the durable boundary: retain that state, discard any
    /// later speculative allocation, and resume immediately after the highest
    /// recovered sequence. If no global progress changed, the old full-clock
    /// restoration preserves retryability for a definitely-uncommitted batch.
    fn restore_clock_after_failed_authority_ingest(&mut self, before: Clock) {
        if self.clock.committed_global_time == before.committed_global_time
            && self.clock.applied_global_times_after_frontier == before.applied_global_times_after_frontier
        {
            self.clock = before;
            return;
        }

        let highest_recovered = self
            .clock
            .applied_global_times_after_frontier
            .iter()
            .next_back()
            .copied()
            .unwrap_or(self.clock.committed_global_time);
        self.clock.global_time_register = self
            .clock
            .global_time_register
            .max(before.global_time_register)
            .max(highest_recovered);
    }

    /// Ingest a mergeable commit unit as an edge authority.
    ///
    /// This applies the same structural and write-policy checks as the normal
    /// authority path, but records only edge durability: no global timestamp is
    /// allocated until core later finalizes the edge-accepted unit.
    pub async fn ingest_edge_authority_mergeable_commit_unit(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_ready()?;
        if commit_unit_limit_violation(&versions).is_none()
            && commit_unit_write_count_matches(&tx, versions.len())
            && let Some(reason) = self.malformed_authored_version_reason(&versions)
        {
            return self
                .reject_malformed_commit(tx, reason)
                .await
                .map(PublicationOutcome::settled);
        }
        let mut updates =
            self.ingest_edge_authority_mergeable_commit_unit_once(tx, versions, now_ms, None).await?;
        updates.extend(self.drain_parked_commit_units().await?);
        Ok(updates)
    }

    /// Ingest a mergeable commit unit as an edge authority using an
    /// authenticated permission subject while preserving `made_by` provenance.
    pub async fn ingest_edge_authority_mergeable_commit_unit_with_identity(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
        identity: AuthorSubject,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error>
    where
        S: ReopenableStorage,
    {
        self.require_catalogue_ready()?;
        if commit_unit_limit_violation(&versions).is_none()
            && commit_unit_write_count_matches(&tx, versions.len())
            && let Some(reason) = self.malformed_authored_version_reason(&versions)
        {
            return self
                .reject_malformed_commit(tx, reason)
                .await
                .map(PublicationOutcome::settled);
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
        ).await?;
        updates.extend(self.drain_parked_commit_units().await?);
        Ok(updates)
    }

    /// Finalize a locally-authored pending mergeable commit as the global
    /// authority: assign the next global timestamp and mark it Accepted/Global.
    ///
    /// This is the authority's self-acceptance of its own write — the path a
    /// `Core` `Db` takes when it commits through the facade (a client instead
    /// commits Pending/Local and learns its fate from upstream). It reuses the
    /// stored versions and does not re-run the
    /// authority validation the node already performed when it authored the
    /// commit. Idempotent: a non-pending transaction is left untouched.
    pub async fn finalize_local_mergeable_commit(
        &mut self,
        tx_id: TxId,
    ) -> Result<PublicationOutcome<()>, Error> {
        self.require_catalogue_ready()?;
        let stored = self
            .query_transaction(tx_id).await?
            .ok_or(Error::MissingTransaction(tx_id))?;
        if stored.tx.kind != TxKind::Mergeable {
            return Err(Error::UnsupportedCommitUnit(
                "self-finalize is mergeable-only",
            ));
        }
        if !matches!(stored.fate, Fate::Pending) {
            return Ok(PublicationOutcome::settled(()));
        }
        let records = self
            .query_versions_for_tx(tx_id).await?
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
        )
        .await?
        {
            let fate = Fate::Rejected(RejectionReason::AuthorizationDenied);
            self.ingest_rejected_transaction(stored.tx, fate).await?;
            return Ok(PublicationOutcome::settled(()));
        }
        let global_time = self
            .clock
            .allocate_global_time(tx_id.time.physical_ms())?;
        self.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(global_time),
            Some(DurabilityTier::Global),
        ).await?;
        let merges = self.create_merge_versions_for(&records).await?;
        Ok(PublicationOutcome {
            value: (),
            publications: merges.publications,
            post_settlement_work: merges.post_settlement_work,
        })
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
    pub async fn finalize_local_exclusive_commit(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
    ) -> Result<PublicationOutcome<Fate>, Error> {
        self.require_catalogue_ready()?;
        let tx_id = tx.tx_id;
        if tx.kind != TxKind::Exclusive {
            return Err(Error::UnsupportedCommitUnit(
                "exclusive self-finalize requires an exclusive transaction",
            ));
        }
        let stored = self
            .query_transaction(tx_id).await?
            .ok_or(Error::MissingTransaction(tx_id))?;
        if !matches!(stored.fate, Fate::Pending) {
            return Ok(PublicationOutcome::settled(stored.fate));
        }
        // Locally finalized exclusive commits bypass `ingest_commit_unit_once`,
        // so they must still take the common fate-policy path before their
        // optimistic local versions become globally accepted.
        if !self
            .commit_unit_satisfies_write_policies(&tx, &versions, None)
            .await?
        {
            let fate = Fate::Rejected(RejectionReason::AuthorizationDenied);
            self.ingest_rejected_transaction(tx, fate.clone()).await?;
            return Ok(PublicationOutcome::settled(fate));
        }
        // Validate through the SAME authority path the core uses for an incoming
        // exclusive commit unit (§3.7): row/absent/predicate reads (INV-TX-16/17/18)
        // AND per-write first-committer-wins (INV-TX-20). Do not reimplement.
        if !self.validate_exclusive_commit_unit(&tx, &versions).await? {
            let fate = Fate::Rejected(RejectionReason::ExclusiveConflict);
            self.ingest_rejected_transaction(tx, fate.clone()).await?;
            return Ok(PublicationOutcome::settled(fate));
        }
        let global_time = self
            .clock
            .allocate_global_time(tx_id.time.physical_ms())?;
        self.apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(global_time),
            Some(DurabilityTier::Global),
        ).await?;
        let merges = self.create_merge_versions_for(&versions).await?;
        Ok(PublicationOutcome {
            value: Fate::Accepted,
            publications: merges.publications,
            post_settlement_work: merges.post_settlement_work,
        })
    }

    pub(super) async fn finalize_edge_accepted_mergeable_commit_unit_once(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error> {
        let versions = canonical_versions(versions);
        let mut memo = IngestMemo::default();
        if tx.kind != TxKind::Mergeable {
            return Err(Error::UnsupportedCommitUnit(
                "edge-accepted finalization is mergeable-only",
            ));
        }
        if let Some(reason) = commit_unit_limit_violation(&versions) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(reason));
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id).await?);
            return Ok(PublicationOutcome::settled(updates));
        }
        if !commit_unit_write_count_matches(&tx, versions.len()) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(
                "commit unit version count does not match transaction n_total_writes".to_owned(),
            ));
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id).await?);
            return Ok(PublicationOutcome::settled(updates));
        }
        if let Some(reason) = self.malformed_authored_version_reason(&versions) {
            return self
                .reject_malformed_commit(tx, reason)
                .await
                .map(PublicationOutcome::settled);
        }
        if let Some(existing) = self.query_transaction(tx.tx_id).await? {
            let mut existing_versions = self
                .query_versions_for_tx(tx.tx_id).await?
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
                && existing.global_time.is_some()
                && existing.durability >= DurabilityTier::Global
            {
                return Ok(PublicationOutcome::settled(vec![SyncMessage::FateUpdate {
                    tx_id: tx.tx_id,
                    fate: existing.fate.clone(),
                    global_time: existing.global_time,
                    durability: fate_update_durability_claim(&existing.fate, existing.durability),
                }]));
            }
            if matches!(existing.fate, Fate::Rejected(_)) {
                return Ok(PublicationOutcome::settled(vec![SyncMessage::FateUpdate {
                    tx_id: tx.tx_id,
                    fate: existing.fate.clone(),
                    global_time: existing.global_time,
                    durability: fate_update_durability_claim(&existing.fate, existing.durability),
                }]));
            }
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
            return Ok(PublicationOutcome::settled(Vec::new()));
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
        ).await? {
            return Ok(PublicationOutcome::settled(Vec::new()));
        }
        if !self.commit_unit_satisfies_clock_condition(&tx, &versions, &mut memo).await? {
            let fate = Fate::Rejected(RejectionReason::CausalityViolation);
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id).await?);
            return Ok(PublicationOutcome::settled(updates));
        }
        if tx.tx_id.time.physical_ms() > now_ms.saturating_add(SKEW_TOLERANCE_MS) {
            let fate = Fate::Rejected(RejectionReason::ClientClockTooFarAhead);
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id).await?);
            return Ok(PublicationOutcome::settled(updates));
        }
        if let Some(root) = self.cascade_root_for_versions(&versions).await {
            let fate = Fate::Rejected(RejectionReason::Cascade { root });
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            return Ok(PublicationOutcome::settled(vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }]));
        }
        let authority_now_ms =
            GlobalTime::authority_now_ms(now_ms, tx.tx_id.time.physical_ms());
        let global_time = self.clock.allocate_global_time(authority_now_ms)?;
        let fate = Fate::Accepted;
        let durability = DurabilityTier::Global;
        let merge_rows = self.merge_rows_for_versions(&versions)?;
        self.ingest_known_transaction(
            tx.clone(),
            versions,
            fate.clone(),
            Some(global_time),
            durability,
        )
        .await?;
        debug_assert_eq!(self.clock.committed_global_time, global_time);
        let mut outcome = PublicationOutcome::settled(vec![SyncMessage::FateUpdate {
            tx_id: tx.tx_id,
            fate,
            global_time: Some(global_time),
            durability: Some(durability),
        }]);
        outcome.append_outcome(self.create_merge_versions_for_rows(merge_rows).await?);
        Ok(outcome)
    }

    /// Ingest an unfated commit unit at a Local relay without assigning fate.
    pub async fn ingest_relay_commit_unit(
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
        self.ingest_relay_commit_unit_once(tx, versions).await?;
        self.drain_parked_relay_commit_units().await?;
        Ok(())
    }

    pub(super) async fn ingest_relay_commit_unit_once(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
    ) -> Result<(), Error> {
        if tx.kind != TxKind::Mergeable && tx.kind != TxKind::Exclusive {
            return Err(Error::UnsupportedCommitUnit("unsupported commit unit kind"));
        }
        let versions = canonical_versions(versions);
        if let Some(existing) = self.query_transaction(tx.tx_id).await? {
            let mut existing_versions = self
                .query_versions_for_tx(tx.tx_id).await?
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
        if self.park_commit_unit_if_missing_schema_versions_with_mode(
            &tx,
            &versions,
            u64::MAX - SKEW_TOLERANCE_MS,
            relay_mode,
        )? {
            return Ok(());
        }
        self.prepare_authored_schema_variants_for_commit(&versions).await?;

        let mut memo = IngestMemo::default();
        if self.park_commit_unit_if_missing_parents_with_mode(
            &tx,
            &versions,
            u64::MAX - SKEW_TOLERANCE_MS,
            &mut memo,
            relay_mode,
        ).await? {
            return Ok(());
        }
        self.ingest_transaction_and_versions(
            tx,
            versions,
            Fate::Pending,
            None,
            DurabilityTier::Local,
        ).await
    }

    pub(super) async fn ingest_commit_unit_once(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
        ingest_context: Option<CommitUnitIngestContext>,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error> {
        let versions = canonical_versions(versions);
        let mut memo = IngestMemo::default();
        if !commit_unit_write_count_matches(&tx, versions.len()) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(
                "commit unit version count does not match transaction n_total_writes".to_owned(),
            ));
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id).await?);
            return Ok(PublicationOutcome::settled(updates));
        }
        if let Some(reason) = self.malformed_authored_version_reason(&versions) {
            return self
                .reject_malformed_commit(tx, reason)
                .await
                .map(PublicationOutcome::settled);
        }
        if let Some(existing) = self.query_transaction(tx.tx_id).await? {
            if tx.kind == TxKind::Exclusive || matches!(existing.fate, Fate::Rejected(_)) {
                if !known_transaction_payload_matches(&existing.tx, &tx) {
                    return Err(Error::ConflictingCommitUnit(tx.tx_id));
                }
                return Ok(PublicationOutcome::settled(vec![SyncMessage::FateUpdate {
                    tx_id: tx.tx_id,
                    fate: existing.fate.clone(),
                    global_time: existing.global_time,
                    durability: fate_update_durability_claim(&existing.fate, existing.durability),
                }]));
            }
            let mut existing_versions = self
                .query_versions_for_tx(tx.tx_id).await?
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
                return Ok(PublicationOutcome::settled(vec![SyncMessage::FateUpdate {
                    tx_id: tx.tx_id,
                    fate: existing.fate.clone(),
                    global_time: existing.global_time,
                    durability: fate_update_durability_claim(&existing.fate, existing.durability),
                }]));
            }
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
            return Ok(PublicationOutcome::settled(Vec::new()));
        }
        self.prepare_authored_schema_variants_for_commit(&versions).await?;
        if self.park_commit_unit_if_missing_parents_with_mode(
            &tx,
            &versions,
            now_ms,
            &mut memo,
            CommitUnitParkMode {
                ingest_context,
                ..CommitUnitParkMode::default()
            },
        ).await? {
            return Ok(PublicationOutcome::settled(Vec::new()));
        }
        if !self.commit_unit_satisfies_clock_condition(&tx, &versions, &mut memo).await? {
            let fate = Fate::Rejected(RejectionReason::CausalityViolation);
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id).await?);
            return Ok(PublicationOutcome::settled(updates));
        }
        if tx.tx_id.time.physical_ms() > now_ms.saturating_add(SKEW_TOLERANCE_MS) {
            let fate = Fate::Rejected(RejectionReason::ClientClockTooFarAhead);
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id).await?);
            return Ok(PublicationOutcome::settled(updates));
        }

        if let Some(root) = self.cascade_root_for_versions(&versions).await {
            let fate = Fate::Rejected(RejectionReason::Cascade { root });
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            return Ok(PublicationOutcome::settled(vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }]));
        }
        if !Box::pin(self.commit_unit_satisfies_write_policies(
            &tx,
            &versions,
            ingest_context,
        ))
        .await?
        {
            let fate = Fate::Rejected(RejectionReason::AuthorizationDenied);
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id).await?);
            return Ok(PublicationOutcome::settled(updates));
        }
        if tx.kind == TxKind::Exclusive
            && !self.validate_exclusive_commit_unit(&tx, &versions).await?
        {
            let fate = Fate::Rejected(RejectionReason::ExclusiveConflict);
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            // This is a newly observed authority-side rejection. No stored
            // descendant can already point at it: descendants delivered before
            // the parent would park on the missing parent instead of entering
            // history. Later descendants will cascade when their parent state
            // is checked, so scanning all stored history here is redundant.
            return Ok(PublicationOutcome::settled(vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }]));
        }
        if tx.kind != TxKind::Mergeable && tx.kind != TxKind::Exclusive {
            return Err(Error::UnsupportedCommitUnit("unsupported commit unit kind"));
        }
        let authority_now_ms =
            GlobalTime::authority_now_ms(now_ms, tx.tx_id.time.physical_ms());
        let global_time = self.clock.allocate_global_time(authority_now_ms)?;
        let fate = Fate::Accepted;
        let durability = DurabilityTier::Global;
        let merge_rows = self.merge_rows_for_versions(&versions)?;
        self.ingest_known_transaction(
            tx.clone(),
            versions,
            fate.clone(),
            Some(global_time),
            durability,
        )
        .await?;
        debug_assert_eq!(self.clock.committed_global_time, global_time);
        let mut outcome = PublicationOutcome::settled(vec![SyncMessage::FateUpdate {
            tx_id: tx.tx_id,
            fate,
            global_time: Some(global_time),
            durability: Some(durability),
        }]);
        outcome.append_outcome(self.create_merge_versions_for_rows(merge_rows).await?);
        Ok(outcome)
    }

    pub(super) async fn ingest_edge_authority_mergeable_commit_unit_once(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        now_ms: u64,
        ingest_context: Option<CommitUnitIngestContext>,
    ) -> Result<PublicationOutcome<Vec<SyncMessage>>, Error> {
        let versions = canonical_versions(versions);
        let mut memo = IngestMemo::default();
        if tx.kind != TxKind::Mergeable {
            return Err(Error::UnsupportedCommitUnit(
                "edge authority only supports mergeable commit units",
            ));
        }
        if let Some(reason) = commit_unit_limit_violation(&versions) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(reason));
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id).await?);
            return Ok(PublicationOutcome::settled(updates));
        }
        if !commit_unit_write_count_matches(&tx, versions.len()) {
            let fate = Fate::Rejected(RejectionReason::MalformedCommit(
                "commit unit version count does not match transaction n_total_writes".to_owned(),
            ));
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id).await?);
            return Ok(PublicationOutcome::settled(updates));
        }
        if let Some(reason) = self.malformed_authored_version_reason(&versions) {
            return self
                .reject_malformed_commit(tx, reason)
                .await
                .map(PublicationOutcome::settled);
        }
        if let Some(existing) = self.query_transaction(tx.tx_id).await? {
            let mut existing_versions = self
                .query_versions_for_tx(tx.tx_id).await?
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
                return Ok(PublicationOutcome::settled(vec![SyncMessage::FateUpdate {
                    tx_id: tx.tx_id,
                    fate: existing.fate.clone(),
                    global_time: existing.global_time,
                    durability: fate_update_durability_claim(&existing.fate, existing.durability),
                }]));
            }
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
            return Ok(PublicationOutcome::settled(Vec::new()));
        }
        self.prepare_authored_schema_variants_for_commit(&versions).await?;
        if self.park_commit_unit_if_missing_parents_with_mode(
            &tx,
            &versions,
            now_ms,
            &mut memo,
            CommitUnitParkMode {
                ingest_context,
                ingress_role: ParkedIngressRole::EdgeAuthority,
            },
        ).await? {
            return Ok(PublicationOutcome::settled(Vec::new()));
        }
        if !self.commit_unit_satisfies_clock_condition(&tx, &versions, &mut memo).await? {
            let fate = Fate::Rejected(RejectionReason::CausalityViolation);
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id).await?);
            return Ok(PublicationOutcome::settled(updates));
        }
        if tx.tx_id.time.physical_ms() > now_ms.saturating_add(SKEW_TOLERANCE_MS) {
            let fate = Fate::Rejected(RejectionReason::ClientClockTooFarAhead);
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id).await?);
            return Ok(PublicationOutcome::settled(updates));
        }
        if let Some(root) = self.cascade_root_for_versions(&versions).await {
            let fate = Fate::Rejected(RejectionReason::Cascade { root });
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            return Ok(PublicationOutcome::settled(vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }]));
        }
        if !self
            .commit_unit_satisfies_write_policies(&tx, &versions, ingest_context)
            .await?
        {
            let fate = Fate::Rejected(RejectionReason::AuthorizationDenied);
            self.ingest_rejected_transaction(tx.clone(), fate.clone()).await?;
            let mut updates = vec![SyncMessage::FateUpdate {
                tx_id: tx.tx_id,
                fate,
                global_time: None,
                durability: None,
            }];
            updates.extend(self.cascade_rejections_from(tx.tx_id).await?);
            return Ok(PublicationOutcome::settled(updates));
        }

        let fate = Fate::Accepted;
        let durability = DurabilityTier::Edge;
        self.ingest_known_transaction(tx.clone(), versions, fate.clone(), None, durability).await?;
        Ok(PublicationOutcome::settled(vec![SyncMessage::FateUpdate {
            tx_id: tx.tx_id,
            fate,
            global_time: None,
            durability: Some(durability),
        }]))
    }

    pub(super) async fn ingest_known_transaction(
        &mut self,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        fate: Fate,
        global_time: Option<GlobalTime>,
        durability: DurabilityTier,
    ) -> Result<(), Error> {
        self.require_catalogue_ready()?;
        debug_assert!(
            global_time.is_none() || durability == DurabilityTier::Global,
            "a global timestamp requires Global durability"
        );
        self.merge_tx_time(tx.tx_id.time);
        let versions = canonical_versions(versions);
        self.prepare_authored_schema_variants_for_commit(&versions).await?;
        if let Some(existing) = self.query_transaction(tx.tx_id).await? {
            let mut existing_versions = self
                .query_versions_for_tx(tx.tx_id).await?
                .into_iter()
                .map(|stored| self.version_record_from_row(&stored))
                .collect::<Result<Vec<_>, Error>>()?;
            existing_versions.sort();
            if !(known_transaction_payload_matches(&existing.tx, &tx)
                || existing.view_scoped_cardinality
                    && known_transaction_payload_matches_redacted_cardinality(&existing.tx, &tx))
            {
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
            if version_bundles.is_empty() && !existing.view_scoped_cardinality {
                self.apply_fate_update(tx.tx_id, fate, global_time, Some(durability))
                    .await?;
                return Ok(());
            }
            return self.ingest_transaction_and_versions(
                tx,
                version_bundles,
                fate,
                global_time,
                durability,
            ).await;
        }
        self.ingest_transaction_and_versions(tx, versions, fate, global_time, durability)
            .await
    }

    pub(super) async fn stage_known_transaction(
        &mut self,
        batch: &mut DatabaseBatch,
        tx: Transaction,
        versions: Vec<VersionRecord>,
        fate: Fate,
        global_time: Option<GlobalTime>,
        durability: DurabilityTier,
        staged_global_times: &mut Vec<GlobalTime>,
        staged_content_versions: &mut Vec<VersionRow>,
    ) -> Result<(), Error> {
        debug_assert!(
            global_time.is_none() || durability == DurabilityTier::Global,
            "a global timestamp requires Global durability"
        );
        let versions = canonical_versions(versions);
        // This is entered by the batched ViewUpdate path, which has no
        // authority role and therefore cannot synthesize a fate. The caller
        // validates its entire frame before staging, and this is the central
        // storage-ingress backstop for other direct callers.
        self.validate_view_payload_versions(&versions)?;
        self.merge_tx_time(tx.tx_id.time);
        if self.query_transaction(tx.tx_id).await?.is_some() {
            return self
                .ingest_known_transaction(tx, versions, fate, global_time, durability)
                .await;
        }
        let staged_versions = self.stage_transaction_and_versions_with_current_indexes(
            batch,
            tx.clone(),
            versions,
            fate.clone(),
            global_time,
            durability,
            true,
            false,
            Some(staged_content_versions),
        )
        .await?;
        self.finalize_staged_transaction_ingest(
            batch,
            fate,
            global_time,
            staged_global_times,
            &staged_versions,
        )
        .await
    }

    pub(super) async fn ingest_reset_view_bundle_refs_in_bulk(
        &mut self,
        bundles: &[VersionBundleRef<'_>],
        preflight_persisted_tx_ids: Option<&BTreeSet<TxId>>,
    ) -> Result<BTreeSet<TxId>, Error> {
        let mut bundles_by_tx = BTreeMap::<TxId, Vec<VersionBundleRef<'_>>>::new();
        for bundle in bundles {
            validate_received_view_bundle_global_time_durability(
                bundle.global_time,
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
            let view_scoped = first.scope == crate::protocol::VersionBundleScope::ViewScoped;
            // Each view-scoped bundle declares only its own redacted fragment
            // cardinality. Compare the immutable transaction identity here and
            // synthesize the receiver-local authorized cardinality only after
            // exact version deduplication below.
            let mut first_tx_identity = first.tx.clone();
            first_tx_identity.n_total_writes = 0;
            if tx_bundles.iter().any(|bundle| {
                let mut tx_identity = bundle.tx.clone();
                tx_identity.n_total_writes = 0;
                tx_identity != first_tx_identity
                    || (bundle.scope == crate::protocol::VersionBundleScope::ViewScoped)
                        != view_scoped
                    || bundle.fate != first.fate
                    || bundle.global_time != first.global_time
                    || bundle.durability != first.durability
            }) {
                continue;
            }
            if *first.fate != Fate::Accepted {
                continue;
            }
            if first.global_time.is_none() {
                continue;
            }
            if first.tx.kind != TxKind::Mergeable && first.tx.kind != TxKind::Exclusive {
                continue;
            }
            let mut unique_versions = BTreeMap::<
                (String, BranchKey, RowUuid, crate::ids::SchemaVersionId, bool),
                &VersionRecord,
            >::new();
            for bundle in &tx_bundles {
                for version in bundle.versions {
                    let key = (
                        version.table().to_owned(),
                        version.branch_key().clone(),
                        version.row_uuid(),
                        version.schema_version(),
                        version.deletion().is_some(),
                    );
                    match unique_versions.get(&key) {
                        Some(existing) if *existing != version => {
                            return Err(Error::ConflictingCommitUnit(tx_id));
                        }
                        Some(_) => {}
                        None => {
                            unique_versions.insert(key, version);
                        }
                    }
                }
            }
            let version_count = unique_versions.len();
            if first.tx.kind == TxKind::Exclusive
                && !view_scoped
                && usize::try_from(first.tx.n_total_writes).ok() != Some(version_count)
            {
                continue;
            }
            if preflight_persisted_tx_ids.is_some_and(|known| known.contains(&tx_id))
                || preflight_persisted_tx_ids.is_none()
                    && self.query_transaction(tx_id).await?.is_some()
            {
                continue;
            }
            let mut missing_refs = false;
            for bundle in &tx_bundles {
                if !self.missing_parent_refs(bundle.versions).await?.is_empty() {
                    missing_refs = true;
                    break;
                }
            }
            if missing_refs {
                continue;
            }
            if loaded_tx_ids.insert(tx_id) {
                let mut local_tx = first.tx.clone();
                if view_scoped {
                    local_tx.n_total_writes = version_count
                        .try_into()
                        .map_err(|_| Error::InvalidStoredValue("view payload is too large"))?;
                }
                eligible.push((tx_bundles, local_tx, view_scoped));
            }
        }
        if eligible.is_empty() {
            return Ok(loaded_tx_ids);
        }
        let eligible_versions = eligible
            .iter()
            .flat_map(|(tx_bundles, _, _)| {
                tx_bundles.iter().flat_map(|bundle| bundle.versions)
            })
            .cloned()
            .collect::<Vec<_>>();
        self.prepare_authored_schema_variants_for_commit(&eligible_versions).await?;
        self.sync_metrics.receiver_bulk_ingest_commits += 1;
        self.sync_metrics.receiver_bulk_bundle_ingests += eligible.len() as u64;

        let mut batch = self.database.open_batch();
        let version_count = eligible
            .iter()
            .flat_map(|(tx_bundles, _, _)| tx_bundles)
            .map(|bundle| bundle.versions.len())
            .sum::<usize>();
        batch.reserve(eligible.len() + version_count.saturating_mul(2));
        let mut current_updates = BTreeMap::<
            (String, BranchKey, RowUuid, VersionLayer),
            (VersionRow, GlobalTime),
        >::new();
        let mut content_versions = Vec::new();
        let mut content_rows =
            BTreeSet::<(PhysicalTableId, String, BranchKey, RowUuid)>::new();
        let mut applied_global_times = Vec::with_capacity(eligible.len());

        for (tx_bundles, local_tx, view_scoped) in eligible {
            let first = tx_bundles[0];
            let tx = &local_tx;
            let tx_node_alias = self.ensure_node_alias(tx.tx_id.node).await?;
            let global_time = first.global_time.expect("checked above");
            applied_global_times.push(global_time);
            let contribution_merge = self.contribution_merge_storage_value(
                tx.contribution_merge.as_ref(),
            )?;
            batch.insert(
                "jazz_transactions",
                // A reset may bulk-load only the view-authorized rows of an
                // exclusive transaction. Preserve that scope marker even when
                // the redacted write count equals this fragment's length, so a
                // later sibling view can extend the same local projection.
                transaction_values_with_cardinality_scope(
                    tx_node_alias,
                    tx,
                    (*first.fate).clone(),
                    first.global_time,
                    first.durability,
                    view_scoped,
                    contribution_merge,
                ),
            );

            let mut unique_versions = BTreeMap::<
                (String, BranchKey, RowUuid, crate::ids::SchemaVersionId, bool),
                &VersionRecord,
            >::new();
            for bundle in &tx_bundles {
                for version in bundle.versions {
                    unique_versions
                        .entry((
                            version.table().to_owned(),
                            version.branch_key().clone(),
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
                let schema_version_alias = self.ensure_schema_version_alias(author_schema).await?;
                let authored_column_ids = self.authored_column_ids_for_names(
                    author_schema,
                    version.table(),
                    version.authored_columns(),
                )?;
                let stored = VersionRow::from_wire_with_schema_version(
                    &source_table_schema,
                    version,
                    authored_column_ids,
                    tx_node_alias,
                    schema_version_alias,
                    tx.tx_id.time,
                    (author_schema != self.catalogue.current_schema_version_id)
                        .then_some(author_schema),
                )?;
                let (history_table, groove_record) = self.version_storage_write_binding(&stored)?;
                batch.insert_raw(
                    history_table.as_ref(),
                    self.version_storage_primary_key(&stored)?,
                    groove_record,
                );
                if stored.layer() == VersionLayer::Content {
                    content_versions.push(stored.clone());
                    content_rows.insert((
                        self.physical_table_id_for_version(&stored)?,
                        stored.table().to_owned(),
                        stored.branch_key().clone(),
                        stored.row_uuid(),
                    ));
                }

                let key = (
                    stored.table().to_owned(),
                    stored.branch_key().clone(),
                    stored.row_uuid(),
                    stored.layer(),
                );
                let existing_winner = current_updates.get(&key).map(|(previous, _)| {
                    (
                        previous,
                        self.version_tx_id(previous).expect("valid version tx id"),
                        previous.tx_time(),
                    )
                });
                if version_wins_over_open_winner(&stored, tx.tx_id, tx.tx_id.time, existing_winner)
                {
                    current_updates.insert(key, (stored, global_time));
                }
            }
        }

        for (stored, global_time) in current_updates.values() {
            self.write_global_current_update(&mut batch, stored, *global_time)?;
        }
        self.write_merge_heads_for_bulk_content_versions(&mut batch, &content_versions)
            .await?;

        #[cfg(test)]
        let current_update_versions = current_updates
            .values()
            .map(|(stored, global_time)| (stored.clone(), *global_time))
            .collect::<Vec<_>>();
        let applied = self.database.apply_batch(batch).await?;
        let persisted = applied.persist().await;
        self.database.finish_persistence(persisted)?;
        self.rebuild_merge_heads_after_history_commit(&content_rows)
            .await?;
        if let Some(tx_time) = loaded_tx_ids.iter().map(|tx_id| tx_id.time).max() {
            self.persist_storage_consistency_marker_through(tx_time).await?;
        }
        #[cfg(test)]
        {
            if std::env::var_os("JAZZ_SKIP_BULK_INGEST_ASSERTS").is_none() {
                for (_, table, branch_key, row_uuid) in &content_rows {
                    self.assert_merge_heads_match_history_in_branch_for_test(
                        table,
                        branch_key,
                        *row_uuid,
                    )
                    .await?;
                }
                self.assert_global_current_updates_match_history_for_test(
                    &current_update_versions,
                )
                .await?;
            }
        }
        for tx_id in &loaded_tx_ids {
            self.invalidate_tx_version_tables_cache(*tx_id);
        }
        for global_time in applied_global_times {
            self.record_applied_global_time(global_time);
        }
        Ok(loaded_tx_ids)
    }
}
