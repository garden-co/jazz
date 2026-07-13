//! View-update construction for subscribers and sync peers. This module owns
//! current-row and query-result bundle assembly, closure expansion, settled
//! canonical binding-view result-set/completeness state, and deduplicated
//! version shipping; per-peer shipped state lives in [`crate::peer`], policy
//! filtering in [`super::policy`], and query execution/planning in
//! [`super::query_eval`]. It sits on the node side of the protocol boundary and
//! emits [`crate::protocol::SyncMessage`] values.

use super::policy::ViewEvaluationContext;
use super::*;
use crate::ids::SchemaVersionId;
use crate::node::maintained_subscription_view::MaintainedSubscriptionView;
use crate::protocol::{
    KnownStateDeclaration, PeerPayloadInventory, ProgramFactEntry, ResultMemberEntry,
    RowVersionRef, VersionBundle, VersionRecord, build_version_carriers_from_singletons,
};

fn maintained_view_tx_versions_contain_winner(
    tx_versions: &[VersionRow],
    winner: &VersionRow,
) -> bool {
    tx_versions.iter().any(|candidate| {
        candidate.table() == winner.table()
            && candidate.row_uuid() == winner.row_uuid()
            && candidate.layer() == winner.layer()
            && candidate.deletion() == winner.deletion()
    })
}

fn maintained_view_find_content_witness<'a>(
    tx_versions: &'a [VersionRow],
    entry_table: &str,
    row_uuid: RowUuid,
) -> Option<&'a VersionRow> {
    tx_versions.iter().find(|version| {
        version.table() == entry_table
            && version.row_uuid() == row_uuid
            && version.deletion().is_none()
    })
}

fn merge_receiver_version_bundle(
    bundles: &mut BTreeMap<TxId, VersionBundle>,
    bundle: &VersionBundle,
) -> Result<(), Error> {
    let Some(existing) = bundles.get_mut(&bundle.tx.tx_id) else {
        bundles.insert(bundle.tx.tx_id, bundle.clone());
        return Ok(());
    };
    if existing.tx != bundle.tx
        || existing.fate != bundle.fate
        || existing.global_seq != bundle.global_seq
        || existing.durability != bundle.durability
    {
        return Err(Error::ConflictingCommitUnit(bundle.tx.tx_id));
    }
    let mut seen = existing
        .versions
        .iter()
        .map(|version| {
            (
                version_bundle_record_key(version),
                version.record().raw().to_vec(),
            )
        })
        .collect::<BTreeMap<_, Vec<u8>>>();
    for version in &bundle.versions {
        let key = version_bundle_record_key(version);
        match seen.get(&key) {
            Some(raw) if raw.as_slice() == version.record().raw() => {}
            Some(_) => return Err(Error::ConflictingCommitUnit(bundle.tx.tx_id)),
            None => {
                seen.insert(key, version.record().raw().to_vec());
                existing.versions.push(version.clone());
            }
        }
    }
    Ok(())
}

fn version_bundle_record_key(version: &VersionRecord) -> (String, RowUuid, SchemaVersionId, bool) {
    (
        version.table().to_owned(),
        version.row_uuid(),
        version.schema_version(),
        version.deletion().is_some(),
    )
}

fn content_row_members_for_bundle(
    members: &[ResultMemberEntry],
    context: &'static str,
) -> Result<Vec<ResultRowEntry>, Error> {
    members
        .iter()
        .filter(|member| member.as_row().is_some())
        .map(|member| {
            member.as_row().ok_or(Error::InvalidStoredValue(match member {
                ResultMemberEntry::Row(_) => context,
                ResultMemberEntry::Synthetic { .. } => {
                    "synthetic result members require typed payload facts before row bundle shipping"
                }
                ResultMemberEntry::PathTuple { .. } => {
                    "path tuple result members require typed payload facts before row bundle shipping"
                }
            }))
        })
        .collect()
}

fn relation_edge_version_rows_for_bundle(
    facts: &[ProgramFactEntry],
) -> BTreeSet<(String, RowUuid, TxId)> {
    facts
        .iter()
        .filter_map(|fact| match fact {
            ProgramFactEntry::RelationEdge(edge) => Some(edge),
            _ => None,
        })
        .flat_map(|edge| {
            [
                edge.source_version
                    .as_ref()
                    .map(|version| (edge.source_table.to_string(), edge.source_row, version.tx)),
                edge.target_version
                    .as_ref()
                    .map(|version| (edge.target_table.to_string(), edge.target_row, version.tx)),
            ]
        })
        .flatten()
        .collect()
}

pub(crate) struct MaintainedViewBundleInputs<'a> {
    pub(crate) subscription: SubscriptionKey,
    /// Peer inventory of transactions whose full row-version payload has
    /// already shipped on this link. Partial payload coverage is not recorded
    /// here, even when it is enough for a subscription-scoped exclusive result.
    pub(crate) peer_complete_tx_payloads: BTreeSet<TxId>,
    /// Optional fast known-state declaration for this served subscription.
    pub(crate) known_state: Option<KnownStateDeclaration>,
    /// Ship complete accepted exclusive transaction payloads so the receiver can
    /// use refreshed rows as a write base for later exclusive transactions.
    pub(crate) complete_exclusive_payloads: bool,
    pub(crate) previous_result_set: BTreeSet<TxId>,
    pub(crate) result_member_adds: Vec<ResultMemberEntry>,
    pub(crate) result_member_removes: Vec<ResultMemberEntry>,
    pub(crate) program_fact_adds: Vec<ProgramFactEntry>,
    pub(crate) program_fact_removes: Vec<ProgramFactEntry>,
    pub(crate) identity: AuthorId,
    pub(crate) tier: DurabilityTier,
    pub(crate) maintained_facts: &'a MaintainedSubscriptionView,
    pub(crate) allow_storage_witness_fallback: bool,
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Subscribe to the raw history storage table.
    pub fn subscribe_history(&mut self, table: &str) -> Result<Subscription, Error> {
        self.table(table)?;
        self.database
            .subscribe_query(select_all(&history_table_name(table)))
            .map_err(Error::Groove)
    }

    /// Build a current-row view update for a system-identity peer.
    #[cfg(test)]
    pub(crate) fn view_update_for_current_rows(
        &mut self,
        table: &str,
    ) -> Result<SyncMessage, Error> {
        let subscription = self.whole_table_subscription_key(table)?;
        self.view_update_for_current_rows_with_peer_payload_inventory(
            table,
            subscription,
            [],
            [],
            [],
            AuthorId::SYSTEM,
        )
    }

    /// Build a current-row view update using the peer's payload inventory.
    #[cfg(test)]
    pub(crate) fn view_update_for_current_rows_with_peer_payload_inventory(
        &mut self,
        table: &str,
        subscription: SubscriptionKey,
        peer_complete_tx_payloads: impl IntoIterator<Item = TxId>,
        previous_result_set: impl IntoIterator<Item = TxId>,
        previous_member_result_set: impl IntoIterator<Item = ResultMemberEntry>,
        identity: AuthorId,
    ) -> Result<SyncMessage, Error> {
        let (shape, binding) = self.whole_table_shape_binding(table)?;
        self.view_update_for_query_binding_with_peer_payload_inventory(
            &shape,
            &binding,
            subscription,
            peer_complete_tx_payloads,
            previous_result_set,
            previous_member_result_set,
            identity,
        )
    }

    /// Build a query-binding view update using the peer's payload inventory.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn view_update_for_query_binding_with_peer_payload_inventory(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        peer_complete_tx_payloads: impl IntoIterator<Item = TxId>,
        previous_result_set: impl IntoIterator<Item = TxId>,
        previous_member_result_set: impl IntoIterator<Item = ResultMemberEntry>,
        identity: AuthorId,
    ) -> Result<SyncMessage, Error> {
        self.seeded_maintained_view_update_for_query_binding_with_peer_payload_inventory(
            shape,
            binding,
            subscription,
            peer_complete_tx_payloads,
            previous_result_set,
            previous_member_result_set,
            identity,
        )
    }

    /// Build a cold maintained query-binding view update using the peer's
    /// payload inventory.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn seeded_maintained_view_update_for_query_binding_with_peer_payload_inventory(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        peer_complete_tx_payloads: impl IntoIterator<Item = TxId>,
        previous_result_set: impl IntoIterator<Item = TxId>,
        previous_member_result_set: impl IntoIterator<Item = ResultMemberEntry>,
        identity: AuthorId,
    ) -> Result<SyncMessage, Error> {
        self.seeded_maintained_view_update_for_query_binding_with_peer_payload_inventory_at_tier(
            shape,
            binding,
            subscription,
            peer_complete_tx_payloads,
            previous_result_set,
            previous_member_result_set,
            identity,
            DurabilityTier::Global,
        )
    }

    #[cfg(test)]
    pub(crate) fn seeded_maintained_view_update_for_query_binding_with_peer_payload_inventory_at_tier(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        peer_complete_tx_payloads: impl IntoIterator<Item = TxId>,
        previous_result_set: impl IntoIterator<Item = TxId>,
        previous_member_result_set: impl IntoIterator<Item = ResultMemberEntry>,
        identity: AuthorId,
        tier: DurabilityTier,
    ) -> Result<SyncMessage, Error> {
        let peer_complete_tx_payloads = peer_complete_tx_payloads
            .into_iter()
            .collect::<BTreeSet<_>>();
        let previous_result_set = previous_result_set.into_iter().collect::<BTreeSet<_>>();
        let previous_member_result_set = previous_member_result_set
            .into_iter()
            .collect::<BTreeSet<_>>();
        let (receiver, maintained, _terminal_schemas, transitions, tables) = self
            .open_seeded_maintained_subscription_view(
                shape,
                binding,
                identity,
                tier,
                &Default::default(),
            )?;
        debug_assert!(
            transitions.removes.is_empty(),
            "cold maintained snapshot emitted result removes"
        );
        let current_member_result_set = transitions
            .adds
            .into_iter()
            .filter(|member| {
                member
                    .table_name()
                    .is_some_and(|table| tables.contains_key(table))
            })
            .collect::<BTreeSet<_>>();
        let result_member_adds = current_member_result_set
            .difference(&previous_member_result_set)
            .cloned()
            .collect::<Vec<_>>();
        let result_member_removes = previous_member_result_set
            .difference(&current_member_result_set)
            .cloned()
            .collect::<Vec<_>>();
        let update = self.view_update_for_maintained_result_members(MaintainedViewBundleInputs {
            subscription,
            result_member_adds,
            result_member_removes,
            program_fact_adds: transitions.program_fact_adds,
            program_fact_removes: transitions.program_fact_removes,
            peer_complete_tx_payloads,
            known_state: None,
            complete_exclusive_payloads: false,
            previous_result_set,
            identity,
            tier,
            maintained_facts: &maintained,
            allow_storage_witness_fallback: false,
        });
        self.unsubscribe_groove_subscription(receiver.id());
        update
    }

    pub(crate) fn view_update_for_maintained_result_members(
        &mut self,
        inputs: MaintainedViewBundleInputs<'_>,
    ) -> Result<SyncMessage, Error> {
        let MaintainedViewBundleInputs {
            subscription,
            peer_complete_tx_payloads,
            known_state,
            complete_exclusive_payloads,
            previous_result_set: _previous_result_set,
            result_member_adds,
            result_member_removes,
            mut program_fact_adds,
            program_fact_removes,
            identity: _identity,
            tier: _tier,
            maintained_facts,
            allow_storage_witness_fallback,
        } = inputs;
        program_fact_adds.extend(maintained_facts.payload_facts_for_members(&result_member_adds));
        let program_fact_adds = program_fact_adds
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let program_fact_removes = program_fact_removes
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut context = ViewEvaluationContext::default();
        let row_result_adds = content_row_members_for_bundle(
            &result_member_adds,
            "real row result member is missing content transaction for bundle shipping",
        )?;
        let row_result_removes = content_row_members_for_bundle(
            &result_member_removes,
            "real row result member removal is missing content transaction for replacement shipping",
        )?;
        let mut tx_versions_cache = BTreeMap::<TxId, Vec<VersionRow>>::new();
        let known_state_position = match &known_state {
            Some(KnownStateDeclaration::Fast { position, .. }) => Some(*position),
            Some(KnownStateDeclaration::ExactVersionSet { .. }) | None => None,
        };
        let known_state_exact_refs = match &known_state {
            Some(KnownStateDeclaration::ExactVersionSet { versions }) => {
                versions.iter().cloned().collect::<BTreeSet<_>>()
            }
            Some(KnownStateDeclaration::Fast { .. }) | None => BTreeSet::new(),
        };
        let skipped_known_state_rows = result_member_adds
            .iter()
            .filter_map(|member| {
                let row = member.as_real_row()?;
                if let (Some(position), Some(declared)) =
                    (row.settle_position, known_state_position)
                    && position <= declared
                {
                    return Some((row.table.to_string(), row.row_uuid));
                }
                if let Some(tx_id) = row.content_tx {
                    let version_ref =
                        RowVersionRef::new(row.table.to_string(), row.row_uuid, tx_id);
                    if known_state_exact_refs.contains(&version_ref) {
                        return Some((row.table.to_string(), row.row_uuid));
                    }
                }
                None
            })
            .collect::<BTreeSet<_>>();
        let relation_edge_add_rows = relation_edge_version_rows_for_bundle(&program_fact_adds);
        let wanted_add_rows_by_tx = row_result_adds
            .iter()
            .map(|(table, row_uuid, tx_id)| (table.to_string(), *row_uuid, *tx_id))
            .chain(relation_edge_add_rows)
            .fold(
                BTreeMap::<TxId, BTreeSet<(String, RowUuid)>>::new(),
                |mut by_tx, (table, row_uuid, tx_id)| {
                    by_tx.entry(tx_id).or_default().insert((table, row_uuid));
                    by_tx
                },
            );
        self.preload_transaction_memo(wanted_add_rows_by_tx.keys().copied(), &mut context)?;
        let mut version_bundles = Vec::with_capacity(row_result_adds.len());
        let mut peer_payload_inventory_refs = Vec::new();
        let mut emitted_versions = BTreeSet::new();
        for (tx_id, wanted_rows) in &wanted_add_rows_by_tx {
            if peer_complete_tx_payloads.contains(tx_id) {
                peer_payload_inventory_refs.push(*tx_id);
                continue;
            }
            if !emitted_versions.insert(*tx_id) {
                continue;
            }
            let tx_versions = tx_versions_cache
                .entry(*tx_id)
                .or_insert_with(|| maintained_facts.versions_by_tx(*tx_id));
            let mut needs_storage_fallback = false;
            for (entry_table, row_uuid) in wanted_rows {
                if maintained_view_find_content_witness(tx_versions, entry_table, *row_uuid)
                    .is_none()
                {
                    let (content_winner, _) =
                        maintained_facts.replacement_for(entry_table, *row_uuid);
                    if let Some(content_winner) = content_winner {
                        if self.version_tx_id(&content_winner)? == *tx_id {
                            tx_versions.push(content_winner);
                        }
                    }
                }
                if maintained_view_find_content_witness(tx_versions, entry_table, *row_uuid)
                    .is_none()
                {
                    needs_storage_fallback = true;
                }
            }
            if needs_storage_fallback && allow_storage_witness_fallback {
                let stored_tx = self
                    .query_transaction_memo(*tx_id, &mut context)?
                    .ok_or(Error::MissingTransaction(*tx_id))?;
                let fallback_versions =
                    if complete_exclusive_payloads && stored_tx.tx.kind == TxKind::Exclusive {
                        self.query_versions_for_tx(*tx_id)?
                    } else {
                        self.query_versions_for_tx_rows_by_alias(
                            *tx_id,
                            stored_tx.node_alias,
                            wanted_rows,
                        )?
                    };
                tx_versions_cache.insert(*tx_id, fallback_versions);
            }
            let tx_versions = tx_versions_cache
                .get_mut(tx_id)
                .expect("tx versions cache entry must exist after fallback");
            if tx_versions.iter().any(|version| {
                version.deletion().is_none()
                    && wanted_rows.contains(&(version.table().to_owned(), version.row_uuid()))
            }) {
                let stored_tx = self
                    .query_transaction_memo(*tx_id, &mut context)?
                    .ok_or(Error::MissingTransaction(*tx_id))?;
                let filtered_tx_versions = tx_versions
                    .iter()
                    .filter(|version| {
                        complete_exclusive_payloads && stored_tx.tx.kind == TxKind::Exclusive
                            || wanted_rows
                                .contains(&(version.table().to_owned(), version.row_uuid()))
                    })
                    .filter(|version| {
                        !skipped_known_state_rows
                            .contains(&(version.table().to_owned(), version.row_uuid()))
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                if filtered_tx_versions.is_empty() {
                    continue;
                }
                let bundle = self.version_bundle_for_maintained_view_versions_with_tx(
                    &stored_tx,
                    &filtered_tx_versions,
                )?;
                version_bundles.push(bundle);
                record_maintained_view_stream_b_add_bundle();
                continue;
            }

            return Err(Error::MaintainedViewMissingBundleWitness(
                "add result row missing Stream B content witness",
            ));
        }
        for (entry_table, row_uuid, content_tx_id) in &row_result_adds {
            let (_, deletion_winner) = maintained_facts.replacement_for(entry_table, *row_uuid);
            let Some(version) = deletion_winner.as_ref() else {
                continue;
            };
            let tx_id = self.version_tx_id(version)?;
            if tx_id == *content_tx_id || !emitted_versions.insert(tx_id) {
                continue;
            }
            if peer_complete_tx_payloads.contains(&tx_id) {
                peer_payload_inventory_refs.push(tx_id);
                record_maintained_view_removal_stream_bundle();
            } else {
                let tx_versions = tx_versions_cache
                    .entry(tx_id)
                    .or_insert_with(|| maintained_facts.versions_by_tx(tx_id));
                if maintained_view_tx_versions_contain_winner(tx_versions, version) {
                    let stored_tx = self
                        .query_transaction_memo(tx_id, &mut context)?
                        .ok_or(Error::MissingTransaction(tx_id))?;
                    version_bundles.push(
                        self.version_bundle_for_maintained_view_versions_with_tx(
                            &stored_tx,
                            tx_versions,
                        )?,
                    );
                    record_maintained_view_removal_stream_bundle();
                } else {
                    return Err(Error::MaintainedViewMissingBundleWitness(
                        "add result row missing deletion replacement witness",
                    ));
                }
            }
        }
        for (entry_table, row_uuid, old_tx_id) in &row_result_removes {
            let (content_winner, deletion_winner) =
                maintained_facts.replacement_for(entry_table, *row_uuid);
            for (version, missing_witness) in [
                (
                    content_winner.as_ref(),
                    "removed result row missing content replacement witness",
                ),
                (
                    deletion_winner.as_ref(),
                    "removed result row missing deletion replacement witness",
                ),
            ] {
                let Some(version) = version else {
                    continue;
                };
                let tx_id = self.version_tx_id(version)?;
                if tx_id == *old_tx_id || emitted_versions.contains(&tx_id) {
                    continue;
                }
                if peer_complete_tx_payloads.contains(&tx_id) {
                    peer_payload_inventory_refs.push(tx_id);
                    record_maintained_view_removal_stream_bundle();
                    continue;
                }
                let tx_versions = tx_versions_cache
                    .entry(tx_id)
                    .or_insert_with(|| maintained_facts.versions_by_tx(tx_id));
                if !maintained_view_tx_versions_contain_winner(tx_versions, version) {
                    return Err(Error::MaintainedViewMissingBundleWitness(missing_witness));
                }
                emitted_versions.insert(tx_id);
                let stored_tx = self
                    .query_transaction_memo(tx_id, &mut context)?
                    .ok_or(Error::MissingTransaction(tx_id))?;
                version_bundles.push(self.version_bundle_for_maintained_view_versions_with_tx(
                    &stored_tx,
                    tx_versions,
                )?);
                record_maintained_view_removal_stream_bundle();
            }
        }
        for bundle in &mut version_bundles {
            if bundle.tx.kind != TxKind::Exclusive {
                continue;
            }
            if complete_exclusive_payloads {
                continue;
            }
            let Some(wanted_rows) = wanted_add_rows_by_tx.get(&bundle.tx.tx_id) else {
                continue;
            };
            bundle.versions.retain(|version| {
                version.deletion().is_some()
                    || wanted_rows.contains(&(version.table().to_owned(), version.row_uuid()))
            });
        }
        let version_carriers = build_version_carriers_from_singletons(version_bundles)
            .map_err(|_| Error::InvalidStoredValue("failed to build version-bundle run"))?;
        Ok(SyncMessage::ViewUpdate {
            subscription,
            settled_through: self.clock.applied_global_watermark,
            reset_result_set: false,
            version_carriers,
            version_bundles: Vec::new(),
            peer_payload_inventory: PeerPayloadInventory {
                complete_tx_payloads: peer_payload_inventory_refs,
            },
            result_member_adds: result_member_adds.into_iter().collect(),
            result_member_removes: result_member_removes.into_iter().collect(),
            program_fact_adds,
            program_fact_removes,
        })
    }

    /// Apply a downstream current-row view update.
    pub(super) fn apply_view_update(&mut self, update: ViewUpdateParts) -> Result<(), Error> {
        self.apply_view_update_inner(update, None)
    }

    pub(crate) fn apply_view_updates_in_batch(
        &mut self,
        updates: Vec<ViewUpdateParts>,
    ) -> Result<(), Error> {
        if updates.is_empty() {
            return Ok(());
        }
        let mut bundle_counts_by_tx = BTreeMap::<TxId, usize>::new();
        for update in &updates {
            for bundle in &update.version_bundles {
                *bundle_counts_by_tx.entry(bundle.tx.tx_id).or_default() += 1;
            }
        }
        let mut bulk_candidates = Vec::new();
        let mut initial_hydration_binding_views =
            self.query.initial_hydration_binding_views.clone();
        for update in &updates {
            let Ok(binding_view_key) = self.binding_view_key_for_subscription(update.subscription)
            else {
                continue;
            };
            if update.reset_result_set {
                initial_hydration_binding_views.insert(binding_view_key);
            }
            let in_initial_hydration = initial_hydration_binding_views.contains(&binding_view_key);
            if update.reset_result_set
                && update.peer_complete_tx_payload_refs.is_empty()
                && update.result_member_removes.is_empty()
                && update.version_bundles.iter().all(|bundle| {
                    bundle_counts_by_tx
                        .get(&bundle.tx.tx_id)
                        .is_some_and(|count| *count == 1)
                })
            {
                bulk_candidates.extend(update.version_bundles.iter().cloned());
            }
            if in_initial_hydration
                && update.version_bundles.is_empty()
                && (!update.reset_result_set || update.peer_complete_tx_payload_refs.is_empty())
            {
                initial_hydration_binding_views.remove(&binding_view_key);
            }
        }
        let bulk_loaded_tx_ids = self.ingest_reset_view_bundles_in_bulk(&bulk_candidates)?;
        let mut receiver_candidates = BTreeMap::<TxId, VersionBundle>::new();
        for update in &updates {
            for bundle in &update.version_bundles {
                if bulk_loaded_tx_ids.contains(&bundle.tx.tx_id) {
                    continue;
                }
                merge_receiver_version_bundle(&mut receiver_candidates, bundle)?;
            }
        }
        let mut receiver_batch = self.database.open_batch();
        let mut receiver_batch_tx_ids = BTreeSet::new();
        let mut receiver_batch_global_seqs = Vec::new();
        let mut receiver_batch_bundle_count = 0u64;
        for bundle in receiver_candidates.values() {
            let staged = self.stage_view_bundle(
                &mut receiver_batch,
                bundle,
                &mut receiver_batch_tx_ids,
                &mut receiver_batch_global_seqs,
            )?;
            if staged {
                receiver_batch_bundle_count += 1;
            }
        }
        if !receiver_batch.is_empty() {
            self.sync_metrics.receiver_bulk_ingest_commits += 1;
            self.sync_metrics.receiver_bulk_bundle_ingests += receiver_batch_bundle_count;
            self.database.commit_batch(receiver_batch)?;
            for tx_id in &receiver_batch_tx_ids {
                self.invalidate_tx_version_tables_cache(*tx_id);
            }
            for global_seq in receiver_batch_global_seqs {
                self.record_applied_global_seq(global_seq);
            }
            if let Some(tx_time) = receiver_batch_tx_ids.iter().map(|tx_id| tx_id.time).max() {
                self.persist_storage_consistency_marker_through(tx_time)?;
            }
        }
        let mut preloaded_tx_ids = bulk_loaded_tx_ids;
        preloaded_tx_ids.extend(receiver_batch_tx_ids);
        // Cross-subscription ordering within one receiver tick carries no
        // protocol semantics beyond per-link FIFO. Table writes are coalesced
        // above; per-subscription settled-state mutations still apply in
        // arrival order below.
        for update in updates {
            self.apply_view_update_inner(update, Some(&preloaded_tx_ids))?;
        }
        Ok(())
    }

    fn apply_view_update_inner(
        &mut self,
        update: ViewUpdateParts,
        preloaded_tx_ids: Option<&BTreeSet<TxId>>,
    ) -> Result<(), Error> {
        let ViewUpdateParts {
            subscription,
            settled_through,
            defer_settlement,
            reset_result_set,
            version_bundles,
            peer_complete_tx_payload_refs,
            result_member_adds,
            result_member_removes,
            program_fact_adds,
            program_fact_removes,
        } = update;
        let binding_view_key = match self.binding_view_key_for_subscription(subscription) {
            Ok(binding_view_key) => binding_view_key,
            Err(Error::InvalidStoredValue(
                "subscription referenced unregistered shape"
                | "subscription referenced unregistered binding",
            )) => {
                // Subscription teardown races in-flight traffic by design:
                // unsubscribe is asynchronous, so per-subscription messages
                // arriving after detach are normal protocol life, not
                // corruption. The receiver cannot distinguish late-detached
                // from never-registered keys, so both are benign drops.
                self.sync_metrics.dropped_detached_subscription_messages += 1;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if reset_result_set {
            self.query
                .initial_hydration_binding_views
                .insert(binding_view_key);
        }
        if defer_settlement {
            self.query
                .deferred_publication_binding_views
                .insert(binding_view_key);
        } else {
            self.query
                .deferred_publication_binding_views
                .remove(&binding_view_key);
        }
        let bulk_loaded_tx_ids = if let Some(preloaded) = preloaded_tx_ids {
            preloaded.clone()
        } else if reset_result_set
            && peer_complete_tx_payload_refs.is_empty()
            && result_member_removes.is_empty()
        {
            // A reset with bundles is a snapshot for this subscription even
            // when other subscriptions already advanced the node watermark.
            // Empty reset stamps stay orthogonal below: with no bundles there
            // is no payload to bulk ingest and the stamp must not clear shared
            // state that is already more settled.
            self.ingest_reset_view_bundles_in_bulk(&version_bundles)?
        } else {
            BTreeSet::new()
        };
        let row_result_adds = result_member_adds
            .iter()
            .filter_map(ResultMemberEntry::as_row)
            .collect::<Vec<_>>();
        let version_bundles_is_empty = version_bundles.is_empty();
        if bulk_loaded_tx_ids.len() != version_bundles.len() {
            for bundle in version_bundles {
                if bulk_loaded_tx_ids.contains(&bundle.tx.tx_id) {
                    continue;
                }
                self.sync_metrics.receiver_per_bundle_ingests += 1;
                self.ingest_view_bundle(bundle)?;
            }
        }
        let mut available_peer_complete_tx_payload_refs = Vec::new();
        for tx_id in peer_complete_tx_payload_refs.iter() {
            if bulk_loaded_tx_ids.contains(tx_id) {
                available_peer_complete_tx_payload_refs.push(*tx_id);
                continue;
            }
            if self.query_transaction(*tx_id)?.is_none() {
                self.record_peer_payload_inventory_missing_fallback();
                continue;
            }
            available_peer_complete_tx_payload_refs.push(*tx_id);
        }
        for tx_id in row_result_adds.iter().map(|(_, _, tx_id)| tx_id) {
            if bulk_loaded_tx_ids.contains(tx_id) {
                continue;
            }
            if self.query_transaction(*tx_id)?.is_none() {
                self.sync_metrics.parked_orphans += 1;
                return Err(Error::MissingTransaction(*tx_id));
            }
        }
        // Removals are self-sufficient: the removed version can be invisible
        // under the receiver's policy, so fetching its body is allowed to
        // return nothing. The row ref in the removal is enough to clear local
        // believed membership and advance coverage.
        self.validate_result_member_adds_are_witnessed(
            &available_peer_complete_tx_payload_refs,
            &row_result_adds,
        )?;
        let empty_reset = reset_result_set
            && version_bundles_is_empty
            && peer_complete_tx_payload_refs.is_empty()
            && result_member_adds.is_empty()
            && result_member_removes.is_empty()
            && program_fact_adds.is_empty()
            && program_fact_removes.is_empty();
        let reset_has_result_removes = !result_member_removes.is_empty();
        let persisted_member_adds = result_member_adds.clone();
        let persisted_member_removes = result_member_removes.clone();
        let persisted_fact_adds = program_fact_adds.clone();
        let persisted_fact_removes = program_fact_removes.clone();
        // A reset only replaces shared canonical state when it carries the
        // snapshot that will replace it. Empty resets from short-lived duplicate
        // usage subscriptions are coverage stamps; letting them clear non-empty
        // shared state makes later one-shot reads less settled than before.
        let preserve_existing_shared_state = empty_reset
            && self
                .query
                .settled_result_sets
                .get(&binding_view_key)
                .is_some_and(|members| !members.is_empty());
        let reset_cleared_shared_state = reset_result_set && !preserve_existing_shared_state;
        if reset_cleared_shared_state {
            self.clear_settled_result_view(binding_view_key);
            self.query.settled_program_facts.remove(&binding_view_key);
            self.query
                .settled_through_by_binding_view
                .remove(&binding_view_key);
        }
        if reset_result_set {
            self.query
                .settled_result_sets
                .entry(binding_view_key)
                .or_default();
        }
        let mut result_members_need_rewrite = false;
        let member_rewrite;
        let fact_rewrite;
        {
            for member in result_member_removes {
                if self.remove_settled_result_member_indexed(binding_view_key, &member) {
                    continue;
                }
                if let Some((removed_table, removed_row_uuid, _)) = member.as_row()
                    && self
                        .remove_settled_result_member_for_row_indexed(
                            binding_view_key,
                            removed_table,
                            removed_row_uuid,
                        )
                        .is_some()
                {
                    result_members_need_rewrite = true;
                }
            }
            for member in result_member_adds {
                if let Some((added_table, added_row_uuid, _)) = member.as_row() {
                    result_members_need_rewrite |= self
                        .remove_settled_result_member_for_row_indexed(
                            binding_view_key,
                            added_table,
                            added_row_uuid,
                        )
                        .is_some();
                }
                self.insert_settled_result_member_indexed(binding_view_key, member);
            }
            member_rewrite = if result_members_need_rewrite {
                Some(
                    self.query
                        .settled_result_sets
                        .get(&binding_view_key)
                        .cloned()
                        .unwrap_or_default(),
                )
            } else {
                None
            };

            let program_facts = self
                .query
                .settled_program_facts
                .entry(binding_view_key)
                .or_default();
            for fact in program_fact_removes {
                program_facts.remove(&fact);
            }
            program_facts.extend(program_fact_adds);
            fact_rewrite = None;
        }
        if !defer_settlement {
            self.query
                .settled_through_by_binding_view
                .insert(binding_view_key, settled_through);
            // Resume catch-up can use reset framing while also carrying removals
            // from the cursor diff. That shape is not a standalone server
            // snapshot for callback materialization: the receiver still needs
            // its normal maintained/local refresh path to fold the change into
            // the existing subscription state.
            if reset_result_set && !reset_has_result_removes && !preserve_existing_shared_state {
                self.query
                    .pending_authoritative_reset_binding_views
                    .insert(binding_view_key);
            }
        }
        // Diagnostic-only: the duplicate-content-version scan feeds a
        // debug_assert, so it is wasted work in release. Gate to debug builds.
        #[cfg(debug_assertions)]
        {
            let row_result_set = self
                .query
                .settled_result_sets
                .get(&binding_view_key)
                .into_iter()
                .flat_map(|members| members.iter())
                .filter_map(ResultMemberEntry::as_row)
                .collect::<BTreeSet<_>>();
            if let Some((table, row_uuid, first, second)) =
                duplicate_row_result_set(&row_result_set)
            {
                debug_assert!(
                    first == second,
                    "settled binding view {binding_view_key:?} has multiple content versions for {table}.{row_uuid:?}: {first:?} and {second:?}"
                );
            }
        }
        if !defer_settlement {
            self.persist_settled_result_state_delta(
                binding_view_key,
                reset_cleared_shared_state,
                &persisted_member_adds,
                &persisted_member_removes,
                member_rewrite.as_ref(),
                &persisted_fact_adds,
                &persisted_fact_removes,
                fact_rewrite.as_ref(),
            )?;
            self.persist_known_state_fact(binding_view_key, settled_through)?;
        }
        if self
            .query
            .initial_hydration_binding_views
            .contains(&binding_view_key)
            && version_bundles_is_empty
            && (!reset_result_set || peer_complete_tx_payload_refs.is_empty())
            && !defer_settlement
        {
            self.query
                .initial_hydration_binding_views
                .remove(&binding_view_key);
        }
        Ok(())
    }

    fn validate_result_member_adds_are_witnessed(
        &mut self,
        peer_complete_tx_payload_refs: &[TxId],
        result_member_adds: &[ResultRowEntry],
    ) -> Result<(), Error> {
        let peer_complete_tx_payload_refs = peer_complete_tx_payload_refs
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut partial_exclusive_keys = BTreeMap::<TxId, BTreeSet<(String, RowUuid)>>::new();
        for (table, row_uuid, tx_id) in result_member_adds {
            let Some(tx) = self.query_transaction(*tx_id)? else {
                continue;
            };
            if tx.tx.kind != TxKind::Exclusive || peer_complete_tx_payload_refs.contains(tx_id) {
                continue;
            }
            let keys = match partial_exclusive_keys.entry(*tx_id) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let keys = self
                        .query_versions_for_tx(*tx_id)?
                        .into_iter()
                        .filter(|version| version.deletion().is_none())
                        .map(|version| (version.table().to_owned(), version.row_uuid()))
                        .collect();
                    entry.insert(keys)
                }
            };
            if !keys.contains(&(table.to_string(), *row_uuid)) {
                return Err(Error::MalformedViewUpdate(
                    "exclusive result row add is not witnessed by partial payload",
                ));
            }
        }
        Ok(())
    }

    fn ingest_view_bundle(&mut self, bundle: VersionBundle) -> Result<(), Error> {
        if bundle.tx.kind != TxKind::Exclusive {
            return self.ingest_known_transaction(
                bundle.tx,
                bundle.versions,
                bundle.fate,
                bundle.global_seq,
                bundle.durability,
            );
        }
        let complete_len = usize::try_from(bundle.tx.n_total_writes).map_err(|_| {
            Error::InvalidStoredValue("exclusive transaction write count does not fit usize")
        })?;
        let tx_id = bundle.tx.tx_id;
        let mut stored_versions = if self.query_transaction(tx_id)?.is_some() {
            self.query_versions_for_tx(tx_id)?
                .iter()
                .map(|stored| self.version_record_from_row(stored))
                .collect::<Result<Vec<_>, Error>>()?
        } else {
            Vec::new()
        };
        let mut known_keys = stored_versions
            .iter()
            .map(view_version_key)
            .collect::<BTreeSet<_>>();
        known_keys.extend(bundle.versions.iter().map(view_version_key));
        if known_keys.len() > complete_len {
            return Err(Error::ConflictingCommitUnit(tx_id));
        }
        let is_tx_complete = known_keys.len() == complete_len;
        if is_tx_complete {
            let mut complete_versions = Vec::with_capacity(complete_len);
            let mut complete_keys = BTreeSet::new();
            for version in stored_versions
                .drain(..)
                .chain(bundle.versions.iter().cloned())
            {
                if complete_keys.insert(view_version_key(&version)) {
                    complete_versions.push(version);
                }
            }
            self.ingest_known_transaction(
                bundle.tx,
                complete_versions,
                bundle.fate.clone(),
                bundle.global_seq,
                bundle.durability,
            )?;
            if matches!(bundle.fate, Fate::Accepted) {
                self.apply_fate_update(
                    tx_id,
                    bundle.fate,
                    bundle.global_seq,
                    Some(bundle.durability),
                )?;
            }
            return Ok(());
        }
        self.ingest_transaction_fragment_without_current_indexes(
            bundle.tx,
            bundle.versions,
            bundle.fate,
            bundle.global_seq,
            bundle.durability,
        )
    }

    fn stage_view_bundle(
        &mut self,
        batch: &mut DatabaseBatch,
        bundle: &VersionBundle,
        staged_tx_ids: &mut BTreeSet<TxId>,
        staged_global_seqs: &mut Vec<GlobalSeq>,
    ) -> Result<bool, Error> {
        if bundle.tx.kind == TxKind::Exclusive {
            let complete_len = usize::try_from(bundle.tx.n_total_writes).map_err(|_| {
                Error::InvalidStoredValue("exclusive transaction write count does not fit usize")
            })?;
            if bundle.versions.len() != complete_len {
                return Ok(false);
            }
            if self.query_transaction(bundle.tx.tx_id)?.is_some() {
                return Ok(false);
            }
        }
        if !staged_tx_ids.insert(bundle.tx.tx_id) {
            return Ok(true);
        }
        self.stage_known_transaction(
            batch,
            bundle.tx.clone(),
            bundle.versions.clone(),
            bundle.fate.clone(),
            bundle.global_seq,
            bundle.durability,
            staged_global_seqs,
        )?;
        Ok(true)
    }

    pub(crate) fn whole_table_subscription_key(
        &self,
        table: &str,
    ) -> Result<SubscriptionKey, Error> {
        let (shape, binding) = self.whole_table_shape_binding(table)?;
        Ok(SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: Default::default(),
        })
    }

    pub(crate) fn whole_table_shape_binding(
        &self,
        table: &str,
    ) -> Result<(ValidatedQuery, Binding), Error> {
        let (schema, schema_version) = if self.table(table).is_ok() {
            (
                &self.catalogue.schema,
                self.catalogue.current_schema_version_id,
            )
        } else {
            let schema_version = self.catalogue.current_write_schema.schema;
            (
                &self
                    .catalogue
                    .catalogue_schemas
                    .get(&schema_version)
                    .ok_or(Error::InvalidStoredValue(
                        "current write schema payload missing",
                    ))?
                    .schema,
                schema_version,
            )
        };
        let shape = crate::query::Query::from(table)
            .validate_with_schema_version(schema, schema_version)?;
        let binding = shape.bind(BTreeMap::new())?;
        Ok((shape, binding))
    }

    pub(super) fn version_bundle_for_maintained_view_versions_with_tx(
        &mut self,
        stored_tx: &StoredTransaction,
        tx_versions: &[VersionRow],
    ) -> Result<VersionBundle, Error> {
        let Transaction {
            tx_id,
            kind,
            n_total_writes,
            made_by,
            permission_subject,
            base_snapshot,
            user_metadata_json,
            source_branch,
            merge_strategy,
            ..
        } = stored_tx.tx.clone();
        let tx_payload = Transaction {
            tx_id,
            kind,
            n_total_writes,
            made_by,
            permission_subject,
            base_snapshot,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json,
            source_branch,
            merge_strategy,
        };
        Ok(VersionBundle {
            tx: tx_payload,
            versions: tx_versions
                .iter()
                .map(|version| self.version_record_from_row(version))
                .collect::<Result<Vec<_>, Error>>()?,
            fate: stored_tx.fate.clone(),
            global_seq: stored_tx.global_seq,
            durability: stored_tx.durability,
        })
    }
}

fn view_version_key(version: &VersionRecord) -> (String, RowUuid, VersionLayer) {
    (
        version.table().to_owned(),
        version.row_uuid(),
        VersionLayer::for_record(version),
    )
}
