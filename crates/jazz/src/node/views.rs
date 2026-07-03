//! View-update construction for subscribers and sync peers. This module owns
//! current-row and query-result bundle assembly, closure expansion, settled
//! canonical binding-view result-set/completeness state, and deduplicated
//! version shipping; per-peer shipped state lives in [`crate::peer`], policy
//! filtering in [`super::policy`], and query execution/planning in
//! [`super::query_eval`]. It sits on the node side of the protocol boundary and
//! emits [`crate::protocol::SyncMessage`] values.

use super::policy::ViewEvaluationContext;
use super::*;
use crate::node::maintained_subscription_view::MaintainedSubscriptionView;
use crate::protocol::{
    KnownStateDeclaration, PeerPayloadInventory, ResultMemberEntry, RowVersionRef,
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

fn content_row_members_for_bundle(
    members: &[ResultMemberEntry],
    context: &'static str,
) -> Result<Vec<ResultRowEntry>, Error> {
    members
        .iter()
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
    pub(crate) identity: AuthorId,
    pub(crate) tier: DurabilityTier,
    pub(crate) maintained_facts: &'a MaintainedSubscriptionView,
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
            peer_complete_tx_payloads,
            known_state: None,
            complete_exclusive_payloads: false,
            previous_result_set,
            identity,
            tier,
            maintained_facts: &maintained,
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
            identity: _identity,
            tier: _tier,
            maintained_facts,
        } = inputs;
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
        let wanted_add_rows_by_tx = row_result_adds
            .iter()
            .map(|(table, row_uuid, tx_id)| (*tx_id, (table.to_string(), *row_uuid)))
            .fold(
                BTreeMap::<TxId, BTreeSet<(String, RowUuid)>>::new(),
                |mut by_tx, (tx_id, row)| {
                    by_tx.entry(tx_id).or_default().insert(row);
                    by_tx
                },
            );
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
            }
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
        Ok(SyncMessage::ViewUpdate {
            subscription,
            settled_through: self.clock.applied_global_watermark,
            reset_result_set: false,
            version_bundles,
            peer_payload_inventory: PeerPayloadInventory {
                complete_tx_payloads: peer_payload_inventory_refs,
            },
            result_member_adds: result_member_adds.into_iter().collect(),
            result_member_removes: result_member_removes.into_iter().collect(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        })
    }

    /// Apply a downstream current-row view update.
    pub(super) fn apply_view_update(&mut self, update: ViewUpdateParts) -> Result<(), Error> {
        let ViewUpdateParts {
            subscription,
            settled_through,
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
        let incoming_bundle_tx_ids = version_bundles
            .iter()
            .map(|bundle| bundle.tx.tx_id)
            .collect::<BTreeSet<_>>();
        let cold_bulk_loaded = reset_result_set
            && peer_complete_tx_payload_refs.is_empty()
            && result_member_removes.is_empty()
            && self.ingest_cold_view_bundles_if_empty(&version_bundles)?;
        let row_result_adds = result_member_adds
            .iter()
            .filter_map(ResultMemberEntry::as_row)
            .collect::<Vec<_>>();
        let version_bundles_is_empty = version_bundles.is_empty();
        if !cold_bulk_loaded {
            for bundle in version_bundles {
                self.ingest_view_bundle(bundle)?;
            }
        }
        if !cold_bulk_loaded {
            for tx_id in peer_complete_tx_payload_refs
                .iter()
                .chain(row_result_adds.iter().map(|(_, _, tx_id)| tx_id))
            {
                if incoming_bundle_tx_ids.contains(tx_id) {
                    continue;
                }
                if self.query_transaction(*tx_id)?.is_none() {
                    self.sync_metrics.parked_orphans += 1;
                    // M2 keeps peer state and receiver storage in memory only, and
                    // both are discarded on restart. Until either becomes durable,
                    // an unknown ref means the sender violated per-link ordering.
                    return Err(Error::MissingTransaction(*tx_id));
                }
            }
        }
        // Removals are self-sufficient: the removed version can be invisible
        // under the receiver's policy, so fetching its body is allowed to
        // return nothing. The row ref in the removal is enough to clear local
        // believed membership and advance coverage.
        self.validate_result_member_adds_are_witnessed(
            &peer_complete_tx_payload_refs,
            &row_result_adds,
        )?;
        let empty_reset = reset_result_set
            && version_bundles_is_empty
            && peer_complete_tx_payload_refs.is_empty()
            && result_member_adds.is_empty()
            && result_member_removes.is_empty()
            && program_fact_adds.is_empty()
            && program_fact_removes.is_empty();
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
        if reset_result_set && !preserve_existing_shared_state {
            self.query.settled_result_sets.remove(&binding_view_key);
            self.query.settled_program_facts.remove(&binding_view_key);
            self.query
                .settled_through_by_binding_view
                .remove(&binding_view_key);
        }
        let row_result_set = self
            .query
            .settled_result_sets
            .entry(binding_view_key)
            .or_default();
        for member in result_member_removes {
            if row_result_set.remove(&member) {
                continue;
            }
            if let Some((removed_table, removed_row_uuid, _)) = member.as_row() {
                row_result_set.retain(|existing| {
                    !matches!(
                        existing.as_row(),
                        Some((existing_table, existing_row_uuid, _))
                            if existing_table == removed_table
                                && existing_row_uuid == removed_row_uuid
                    )
                });
            }
        }
        row_result_set.extend(result_member_adds);
        let program_facts = self
            .query
            .settled_program_facts
            .entry(binding_view_key)
            .or_default();
        for fact in program_fact_removes {
            program_facts.remove(&fact);
        }
        program_facts.extend(program_fact_adds);
        self.query
            .settled_through_by_binding_view
            .insert(binding_view_key, settled_through);
        // Diagnostic-only: the duplicate-content-version scan feeds a
        // debug_assert, so it is wasted work in release. Gate to debug builds.
        #[cfg(debug_assertions)]
        {
            let row_result_set = row_result_set
                .iter()
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
        self.persist_known_state_fact(binding_view_key, settled_through)?;
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
        let shape = crate::query::Query::from(table).validate(&self.catalogue.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        Ok((shape, binding))
    }

    fn version_bundle_for_maintained_view_versions_with_tx(
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
