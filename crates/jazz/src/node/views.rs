//! View-update construction for subscribers and sync peers. This module owns
//! current-row and query-result bundle assembly, closure expansion, settled
//! canonical binding-view result-set/completeness state, and deduplicated
//! version shipping; per-peer shipped state lives in [`crate::peer`], policy
//! filtering in [`super::policy`], and query execution/planning in
//! [`super::query_eval`]. It sits on the node side of the protocol boundary and
//! emits [`crate::protocol::SyncMessage`] values.

use super::policy::ViewEvaluationContext;
use super::*;
use crate::protocol::{PeerPayloadInventory, ResultMemberEntry};

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

pub(crate) struct MaintainedViewBundleInputs<V, R> {
    pub(crate) subscription: SubscriptionKey,
    /// Peer inventory of transactions whose full row-version payload has
    /// already shipped on this link. Partial payload coverage is not recorded
    /// here, even when it is enough for a subscription-scoped exclusive result.
    pub(crate) peer_complete_tx_payloads: BTreeSet<TxId>,
    /// Ship complete accepted exclusive transaction payloads so the receiver can
    /// use refreshed rows as a write base for later exclusive transactions.
    pub(crate) complete_exclusive_payloads: bool,
    pub(crate) previous_result_set: BTreeSet<TxId>,
    pub(crate) result_member_adds: Vec<ResultMemberEntry>,
    pub(crate) result_member_removes: Vec<ResultMemberEntry>,
    pub(crate) identity: AuthorId,
    pub(crate) tier: DurabilityTier,
    pub(crate) versions_by_tx: V,
    pub(crate) replacement_for: R,
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
    pub fn view_update_for_current_rows(&mut self, table: &str) -> Result<SyncMessage, Error> {
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
    pub fn view_update_for_current_rows_with_peer_payload_inventory(
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
    #[allow(clippy::too_many_arguments)]
    pub fn view_update_for_query_binding_with_peer_payload_inventory(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        peer_complete_tx_payloads: impl IntoIterator<Item = TxId>,
        previous_result_set: impl IntoIterator<Item = TxId>,
        previous_member_result_set: impl IntoIterator<Item = ResultMemberEntry>,
        identity: AuthorId,
    ) -> Result<SyncMessage, Error> {
        self.cold_maintained_view_update_for_query_binding_with_peer_payload_inventory(
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
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn cold_maintained_view_update_for_query_binding_with_peer_payload_inventory(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        peer_complete_tx_payloads: impl IntoIterator<Item = TxId>,
        previous_result_set: impl IntoIterator<Item = TxId>,
        previous_member_result_set: impl IntoIterator<Item = ResultMemberEntry>,
        identity: AuthorId,
    ) -> Result<SyncMessage, Error> {
        self.cold_maintained_view_update_for_query_binding_with_peer_payload_inventory_at_tier(
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

    pub(crate) fn cold_maintained_view_update_for_query_binding_with_peer_payload_inventory_at_tier(
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
        let (receiver, maintained, _terminal_schemas, transitions, tables) =
            self.maintained_subscription_view_from_cold_snapshot(shape, binding, identity, tier)?;
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
        let update = self.view_update_for_query_result_delta_maintained_view_add_bundles(
            MaintainedViewBundleInputs {
                subscription,
                result_member_adds,
                result_member_removes,
                peer_complete_tx_payloads,
                complete_exclusive_payloads: false,
                previous_result_set,
                identity,
                tier,
                versions_by_tx: |tx_id| maintained.versions_by_tx(tx_id),
                replacement_for: |table: String, row_uuid| {
                    maintained.replacement_for(&table, row_uuid)
                },
            },
        );
        self.unsubscribe_groove_subscription(receiver.id());
        update
    }

    pub(crate) fn view_update_for_query_result_delta_maintained_view_add_bundles<V, R>(
        &mut self,
        inputs: MaintainedViewBundleInputs<V, R>,
    ) -> Result<SyncMessage, Error>
    where
        V: FnMut(TxId) -> Vec<VersionRow>,
        R: Fn(String, RowUuid) -> (Option<VersionRow>, Option<VersionRow>),
    {
        let MaintainedViewBundleInputs {
            subscription,
            peer_complete_tx_payloads,
            complete_exclusive_payloads,
            previous_result_set: _previous_result_set,
            result_member_adds,
            result_member_removes,
            identity,
            tier,
            mut versions_by_tx,
            replacement_for,
        } = inputs;
        let mut context = ViewEvaluationContext::for_policy_read_tier(tier);
        let row_result_adds = content_row_members_for_bundle(
            &result_member_adds,
            "real row result member is missing content transaction for bundle shipping",
        )?;
        let row_result_removes = content_row_members_for_bundle(
            &result_member_removes,
            "real row result member removal is missing content transaction for replacement shipping",
        )?;
        let mut tx_versions_cache = BTreeMap::<TxId, Vec<VersionRow>>::new();
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
                .or_insert_with(|| versions_by_tx(*tx_id));
            for (entry_table, row_uuid) in wanted_rows {
                if maintained_view_find_content_witness(tx_versions, entry_table, *row_uuid)
                    .is_none()
                {
                    let (content_winner, _) = replacement_for(entry_table.clone(), *row_uuid);
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
                let filtered_tx_versions = if complete_exclusive_payloads
                    && stored_tx.tx.kind == TxKind::Exclusive
                {
                    self.query_versions_for_tx_memo_cloned(*tx_id, &mut context)?
                } else {
                    tx_versions
                        .iter()
                        .filter(|version| {
                            wanted_rows.contains(&(version.table().to_owned(), version.row_uuid()))
                        })
                        .cloned()
                        .collect::<Vec<_>>()
                };
                version_bundles.push(
                    self.version_bundle_for_maintained_view_policy_readable_versions_with_tx(
                        &stored_tx,
                        &filtered_tx_versions,
                        identity,
                        &mut context,
                    )?,
                );
                record_maintained_view_stream_b_add_bundle();
                continue;
            }

            return Err(Error::MaintainedViewMissingBundleWitness(
                "add result row missing Stream B content witness",
            ));
        }
        for (entry_table, row_uuid, content_tx_id) in &row_result_adds {
            let (_, deletion_winner) = replacement_for(entry_table.to_string(), *row_uuid);
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
                    .or_insert_with(|| versions_by_tx(tx_id));
                if maintained_view_tx_versions_contain_winner(tx_versions, version) {
                    let stored_tx = self
                        .query_transaction_memo(tx_id, &mut context)?
                        .ok_or(Error::MissingTransaction(tx_id))?;
                    version_bundles.push(
                        self.version_bundle_for_maintained_view_policy_readable_versions_with_tx(
                            &stored_tx,
                            tx_versions,
                            identity,
                            &mut context,
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
                replacement_for(entry_table.to_string(), *row_uuid);
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
                    .or_insert_with(|| versions_by_tx(tx_id));
                if !maintained_view_tx_versions_contain_winner(tx_versions, version) {
                    return Err(Error::MaintainedViewMissingBundleWitness(missing_witness));
                }
                emitted_versions.insert(tx_id);
                let stored_tx = self
                    .query_transaction_memo(tx_id, &mut context)?
                    .ok_or(Error::MissingTransaction(tx_id))?;
                version_bundles.push(
                    self.version_bundle_for_maintained_view_policy_readable_versions_with_tx(
                        &stored_tx,
                        tx_versions,
                        identity,
                        &mut context,
                    )?,
                );
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
            reset_result_set,
            version_bundles,
            peer_complete_tx_payload_refs,
            result_member_adds,
            result_member_removes,
            program_fact_adds,
            program_fact_removes,
        } = update;
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
        let row_result_removes = result_member_removes
            .iter()
            .filter_map(ResultMemberEntry::as_row)
            .collect::<Vec<_>>();
        if !cold_bulk_loaded {
            for bundle in version_bundles {
                self.ingest_view_bundle(bundle)?;
            }
        }
        if !cold_bulk_loaded {
            for tx_id in peer_complete_tx_payload_refs
                .iter()
                .chain(row_result_adds.iter().map(|(_, _, tx_id)| tx_id))
                .chain(row_result_removes.iter().map(|(_, _, tx_id)| tx_id))
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
        self.validate_result_member_adds_are_witnessed(
            &peer_complete_tx_payload_refs,
            &row_result_adds,
        )?;
        let binding_view_key = self.binding_view_key_for_subscription(subscription)?;
        if reset_result_set {
            self.query.settled_result_sets.remove(&binding_view_key);
            self.query.settled_program_facts.remove(&binding_view_key);
        }
        let row_result_set = self
            .query
            .settled_result_sets
            .entry(binding_view_key)
            .or_default();
        for member in result_member_removes {
            row_result_set.remove(&member);
        }
        row_result_set.extend(result_member_adds);
        let mirrored_result_set = row_result_set
            .iter()
            .filter_map(ResultMemberEntry::as_row)
            .collect::<BTreeSet<_>>();
        let program_facts = self
            .query
            .settled_program_facts
            .entry(binding_view_key)
            .or_default();
        for fact in program_fact_removes {
            program_facts.remove(&fact);
        }
        program_facts.extend(program_fact_adds);
        // Diagnostic-only: the duplicate-content-version scan feeds a
        // debug_assert, so it is wasted work in release. Gate to debug builds.
        #[cfg(debug_assertions)]
        if let Some((table, row_uuid, first, second)) =
            duplicate_row_result_set(&mirrored_result_set)
        {
            debug_assert!(
                first == second,
                "settled binding view {binding_view_key:?} has multiple content versions for {table}.{row_uuid:?}: {first:?} and {second:?}"
            );
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

    pub(super) fn version_bundle_for_maintained_view_policy_readable_versions_with_tx(
        &mut self,
        stored_tx: &StoredTransaction,
        tx_versions: &[VersionRow],
        identity: AuthorId,
        context: &mut ViewEvaluationContext,
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
        let mut versions = Vec::with_capacity(tx_versions.len());
        for candidate in tx_versions {
            let table = self.table(candidate.table())?.clone();
            if !self.read_policy_allows_version_memo(&table, candidate, identity, context)?
                && !self.read_policy_allows_deletion_version_memo(
                    &table, candidate, identity, context,
                )?
            {
                continue;
            }
            versions.push(self.version_record_from_row(candidate)?);
        }
        Ok(VersionBundle {
            tx: tx_payload,
            versions,
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
