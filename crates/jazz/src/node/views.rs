//! View-update construction for subscribers and sync peers. This module owns
//! current-row and query-result bundle assembly, closure expansion, settled
//! subscription result-set/completeness state, and deduplicated version shipping; per-peer shipped
//! state lives in [`crate::peer`], policy filtering in [`super::policy`], and
//! query execution/planning in [`super::query_eval`]. It sits on the node side of
//! the protocol boundary and emits [`crate::protocol::SyncMessage`] values.

use super::policy::ViewEvaluationContext;
use super::query_eval::binding_for_shape;
use super::*;
use crate::protocol::PeerPayloadInventory;

#[derive(Default)]
struct ClosureExpansionMemo {
    current_rows: BTreeMap<(String, RowUuid), Option<CurrentRow>>,
    visible_current: BTreeMap<(String, RowUuid, DurabilityTier), Option<(CurrentRow, TxId)>>,
    join_rows_by_target: BTreeMap<JoinRowsKey, BTreeMap<RowUuid, Vec<CurrentRow>>>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct JoinRowsKey {
    shape_id: ShapeId,
    binding_id: BindingId,
    table: String,
    on_column: String,
    tier: DurabilityTier,
}

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
    pub(crate) result_row_adds: Vec<ResultRowEntry>,
    pub(crate) result_row_removes: Vec<ResultRowEntry>,
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
        previous_row_result_set: impl IntoIterator<Item = ResultRowEntry>,
        identity: AuthorId,
    ) -> Result<SyncMessage, Error> {
        let (shape, binding) = self.whole_table_shape_binding(table)?;
        self.view_update_for_query_binding_with_peer_payload_inventory(
            &shape,
            &binding,
            subscription,
            peer_complete_tx_payloads,
            previous_result_set,
            previous_row_result_set,
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
        previous_row_result_set: impl IntoIterator<Item = ResultRowEntry>,
        identity: AuthorId,
    ) -> Result<SyncMessage, Error> {
        self.view_update_for_query_binding_with_peer_payload_inventory_and_plan(
            shape,
            binding,
            subscription,
            peer_complete_tx_payloads,
            previous_result_set,
            previous_row_result_set,
            identity,
            None,
        )
    }

    /// Build a query-binding view update using the peer's payload inventory and an
    /// already-resolved plan for the link-policy-composed query.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn view_update_for_query_binding_with_peer_payload_inventory_and_plan(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        peer_complete_tx_payloads: impl IntoIterator<Item = TxId>,
        previous_result_set: impl IntoIterator<Item = TxId>,
        previous_row_result_set: impl IntoIterator<Item = ResultRowEntry>,
        identity: AuthorId,
        prepared_plan: Option<(&ValidatedQuery, &Binding, &PreparedQueryPlan)>,
    ) -> Result<SyncMessage, Error> {
        self.view_update_for_query_binding_with_peer_payload_inventory_and_plan_at_tier(
            shape,
            binding,
            subscription,
            peer_complete_tx_payloads,
            previous_result_set,
            previous_row_result_set,
            identity,
            prepared_plan,
            DurabilityTier::Global,
        )
    }

    pub(crate) fn view_update_for_query_binding_with_peer_payload_inventory_and_plan_at_tier(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        peer_complete_tx_payloads: impl IntoIterator<Item = TxId>,
        previous_result_set: impl IntoIterator<Item = TxId>,
        previous_row_result_set: impl IntoIterator<Item = ResultRowEntry>,
        identity: AuthorId,
        prepared_plan: Option<(&ValidatedQuery, &Binding, &PreparedQueryPlan)>,
        tier: DurabilityTier,
    ) -> Result<SyncMessage, Error> {
        let table_name = shape.query().table.clone();
        let peer_complete_tx_payloads = peer_complete_tx_payloads
            .into_iter()
            .collect::<BTreeSet<_>>();
        let _previous_result_set = previous_result_set.into_iter().collect::<BTreeSet<_>>();
        let previous_row_result_set = previous_row_result_set.into_iter().collect::<BTreeSet<_>>();
        let degenerate_whole_table = is_degenerate_whole_table(shape, binding);
        let mut context = ViewEvaluationContext::for_policy_read_tier(tier);
        let rows = if identity == AuthorId::SYSTEM
            && let Some((effective_shape, effective_binding, plan)) = prepared_plan
        {
            self.query_rows_with_prepared_plan_for_identity(
                effective_shape,
                effective_binding,
                tier,
                Some(plan),
                identity,
            )?
        } else {
            self.query_rows_for_link(shape, binding, tier, identity)?
        };
        let mut current_row_result_set = BTreeSet::new();
        let result_table = groove::Intern::new(table_name.clone());
        for row in rows {
            if let Some((time, alias)) = row.projected_tx_alias() {
                let node = self
                    .resolve_node_alias(alias)?
                    .ok_or(Error::InvalidStoredValue(
                        "query output tx node alias must exist",
                    ))?;
                current_row_result_set.insert((
                    result_table,
                    row.row_uuid(),
                    TxId::new(time, node),
                ));
            } else {
                self.add_visible_result_set_entry(
                    &mut current_row_result_set,
                    &table_name,
                    row.row_uuid(),
                )?;
            }
        }
        self.expand_query_closure(shape, binding, &mut current_row_result_set, tier)?;
        let root_result_entries = current_row_result_set
            .iter()
            .filter(|(entry_table, _, _)| entry_table.as_str() == table_name)
            .cloned()
            .collect::<BTreeSet<_>>();
        self.retain_policy_atomic_rows(&mut current_row_result_set, identity, &mut context)?;
        current_row_result_set.extend(root_result_entries.iter().cloned());
        for (entry_table, row_uuid, tx_id) in &current_row_result_set {
            if entry_table.as_str() == table_name
                && !root_result_entries.contains(&(*entry_table, *row_uuid, *tx_id))
            {
                debug_assert!(
                    self.result_set_entry_read_policy_allows_memo(
                        entry_table,
                        *row_uuid,
                        *tx_id,
                        identity,
                        &mut context,
                    )?,
                    "subscription emitted unreadable output row {entry_table}.{row_uuid:?}"
                );
            }
        }
        let wanted_rows_by_tx = current_row_result_set
            .iter()
            .filter(|entry| !previous_row_result_set.contains(*entry))
            .map(|(table, row_uuid, tx_id)| (*tx_id, (table.to_string(), *row_uuid)))
            .fold(
                BTreeMap::<TxId, BTreeSet<(String, RowUuid)>>::new(),
                |mut by_tx, (tx_id, row)| {
                    by_tx.entry(tx_id).or_default().insert(row);
                    by_tx
                },
            );
        let mut version_bundles = Vec::with_capacity(current_row_result_set.len());
        let mut peer_payload_inventory_refs = Vec::new();
        let mut witnessed_result_row_adds = BTreeSet::new();
        let mut emitted_versions = BTreeSet::new();
        for (entry_table, row_uuid, tx_id) in &current_row_result_set {
            if previous_row_result_set.contains(&(entry_table.clone(), *row_uuid, *tx_id)) {
                continue;
            }
            if peer_complete_tx_payloads.contains(tx_id) {
                peer_payload_inventory_refs.push(*tx_id);
                witnessed_result_row_adds.insert((entry_table.clone(), *row_uuid, *tx_id));
                continue;
            }
            if !emitted_versions.insert(*tx_id) {
                continue;
            }
            let table_schema = self.table(entry_table.as_str())?.clone();
            let tx_versions = self.query_versions_for_tx_memo_cloned(*tx_id, &mut context)?;
            let version = tx_versions
                .iter()
                .find(|version| {
                    version.table() == entry_table.as_str()
                        && version.row_uuid() == *row_uuid
                        && version.deletion().is_none()
                })
                .ok_or(Error::MissingTransaction(*tx_id))?;
            if self.read_policy_allows_version_memo(
                &table_schema,
                version,
                identity,
                &mut context,
            )? {
                let wanted_rows = wanted_rows_by_tx
                    .get(tx_id)
                    .ok_or(Error::MissingTransaction(*tx_id))?;
                let bundle_versions = tx_versions
                    .iter()
                    .filter(|version| {
                        wanted_rows.contains(&(version.table().to_owned(), version.row_uuid()))
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                version_bundles.push(self.version_bundle_for_view_memo_with_versions(
                    &table_schema,
                    version,
                    *tx_id,
                    &bundle_versions,
                    identity,
                    &mut context,
                )?);
                for (table, row_uuid) in wanted_rows {
                    witnessed_result_row_adds.insert((
                        groove::Intern::new(table.clone()),
                        *row_uuid,
                        *tx_id,
                    ));
                }
            }
        }
        for (entry_table, row_uuid, content_tx_id) in &current_row_result_set {
            let Some(version) = self.current_layer_winner_for_view_tier(
                entry_table,
                *row_uuid,
                VersionLayer::Deletion,
                tier,
            )?
            else {
                continue;
            };
            let tx_id = self.version_tx_id(&version)?;
            if tx_id == *content_tx_id || !emitted_versions.insert(tx_id) {
                continue;
            }
            let table_schema = self.table(entry_table.as_str())?.clone();
            if self.read_policy_allows_deletion_version_memo(
                &table_schema,
                &version,
                identity,
                &mut context,
            )? {
                if peer_complete_tx_payloads.contains(&tx_id) {
                    peer_payload_inventory_refs.push(tx_id);
                } else {
                    version_bundles.push(self.version_bundle_for_view_memo(
                        &table_schema,
                        &version,
                        identity,
                        &mut context,
                    )?);
                }
            }
        }
        if degenerate_whole_table {
            let table_schema = self.table(&table_name)?.clone();
            for version in self.current_deletion_register_versions_for_view(&table_name)? {
                let tx_id = self.version_tx_id(&version)?;
                if !self.transaction_read_policy_atomic_for_link_memo(
                    tx_id,
                    identity,
                    &mut context,
                )? {
                    continue;
                }
                if peer_complete_tx_payloads.contains(&tx_id) || !emitted_versions.insert(tx_id) {
                    continue;
                }
                if self.read_policy_allows_deletion_version_memo(
                    &table_schema,
                    &version,
                    identity,
                    &mut context,
                )? {
                    version_bundles.push(self.version_bundle_for_view_memo(
                        &table_schema,
                        &version,
                        identity,
                        &mut context,
                    )?);
                }
            }
        }
        let mut result_row_adds = Vec::with_capacity(current_row_result_set.len());
        result_row_adds.extend(
            current_row_result_set
                .difference(&previous_row_result_set)
                .filter(|entry| witnessed_result_row_adds.contains(*entry))
                .cloned(),
        );
        let mut result_row_removes = Vec::with_capacity(previous_row_result_set.len());
        result_row_removes.extend(
            previous_row_result_set
                .difference(&current_row_result_set)
                .cloned(),
        );
        for (entry_table, row_uuid, old_tx_id) in &result_row_removes {
            if let Some(version) =
                self.visible_global_content_version_now(entry_table.as_str(), *row_uuid)
            {
                let tx_id = self.version_tx_id(&version)?;
                if tx_id != *old_tx_id && !emitted_versions.contains(&tx_id) {
                    let table_schema = self.table(entry_table.as_str())?.clone();
                    if self.read_policy_allows_version_memo(
                        &table_schema,
                        &version,
                        identity,
                        &mut context,
                    )? {
                        if peer_complete_tx_payloads.contains(&tx_id) {
                            peer_payload_inventory_refs.push(tx_id);
                        } else {
                            emitted_versions.insert(tx_id);
                            version_bundles.push(self.version_bundle_for_view_memo(
                                &table_schema,
                                &version,
                                identity,
                                &mut context,
                            )?);
                        }
                    }
                }
            } else {
                let Some(version) =
                    self.query_global_layer_winner(entry_table, *row_uuid, VersionLayer::Deletion)?
                else {
                    continue;
                };
                let tx_id = self.version_tx_id(&version)?;
                if tx_id != *old_tx_id && !emitted_versions.contains(&tx_id) {
                    let table_schema = self.table(entry_table)?.clone();
                    if self.read_policy_allows_deletion_version_memo(
                        &table_schema,
                        &version,
                        identity,
                        &mut context,
                    )? {
                        if peer_complete_tx_payloads.contains(&tx_id) {
                            peer_payload_inventory_refs.push(tx_id);
                        } else {
                            emitted_versions.insert(tx_id);
                            version_bundles.push(self.version_bundle_for_view_memo(
                                &table_schema,
                                &version,
                                identity,
                                &mut context,
                            )?);
                        }
                    }
                }
            }
        }
        for bundle in &mut version_bundles {
            if bundle.tx.kind != TxKind::Exclusive {
                continue;
            }
            let Some(wanted_rows) = wanted_rows_by_tx.get(&bundle.tx.tx_id) else {
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
            result_row_adds,
            result_row_removes,
        })
    }

    /// Translate one query output delta record into the output-row result set entry.
    pub(crate) fn query_output_entry_from_delta(
        &mut self,
        shape: &ValidatedQuery,
        record: groove::records::BorrowedRecord<'_>,
    ) -> Result<ResultRowEntry, Error> {
        let table_name = shape.query().table.clone();
        let table = self.table(&table_name)?.clone();
        let row = decode_current_row(&table, record)?;
        if let Some((time, alias)) = row.projected_tx_alias() {
            let node = self
                .resolve_node_alias(alias)?
                .ok_or(Error::InvalidStoredValue(
                    "query output tx node alias must exist",
                ))?;
            Ok((
                groove::Intern::new(table_name),
                row.row_uuid(),
                TxId::new(time, node),
            ))
        } else {
            let tx_id = self
                .visible_global_content_tx_id_now(&table_name, row.row_uuid())
                .ok_or(Error::InvalidStoredValue("query output missing current tx"))?;
            Ok((groove::Intern::new(table_name), row.row_uuid(), tx_id))
        }
    }

    /// Translate one query output retraction record into the output-row result set
    /// entry. Retractions must carry their tx in the record itself: resolving
    /// against currently-visible state would name the wrong version (or none)
    /// for a row that just changed or vanished.
    pub(crate) fn query_output_entry_from_retraction(
        &mut self,
        shape: &ValidatedQuery,
        record: groove::records::BorrowedRecord<'_>,
    ) -> Result<ResultRowEntry, Error> {
        let table_name = shape.query().table.clone();
        let table = self.table(&table_name)?.clone();
        let row = decode_current_row(&table, record)?;
        let (time, alias) = row.projected_tx_alias().ok_or(Error::InvalidStoredValue(
            "query retraction record must project its tx",
        ))?;
        let node = self
            .resolve_node_alias(alias)?
            .ok_or(Error::InvalidStoredValue(
                "query output tx node alias must exist",
            ))?;
        Ok((
            groove::Intern::new(table_name),
            row.row_uuid(),
            TxId::new(time, node),
        ))
    }

    /// Compute the closure rows contributed by one output row.
    ///
    /// PeerState stores this per output-row entry. The map is bounded by the
    /// result set it already tracks for the subscription, and exists only so output
    /// removals can retract their exact closure contribution without full
    /// per-binding re-evaluation on the hot path.
    pub(crate) fn query_output_closure_contribution(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        output: ResultRowEntry,
        identity: AuthorId,
    ) -> Result<BTreeSet<ResultRowEntry>, Error> {
        let mut memo = ClosureExpansionMemo::default();
        let mut context = ViewEvaluationContext::default();
        self.query_output_closure_contribution_with_memo(
            shape,
            binding,
            output,
            identity,
            &mut memo,
            &mut context,
        )
    }

    pub(crate) fn query_output_closure_contributions(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        outputs: impl IntoIterator<Item = ResultRowEntry>,
        identity: AuthorId,
    ) -> Result<BTreeMap<ResultRowEntry, BTreeSet<ResultRowEntry>>, Error> {
        let mut memo = ClosureExpansionMemo::default();
        let mut context = ViewEvaluationContext::default();
        let mut contributions = BTreeMap::new();
        for output in outputs {
            let contribution = self.query_output_closure_contribution_with_memo(
                shape,
                binding,
                output,
                identity,
                &mut memo,
                &mut context,
            )?;
            contributions.insert(output, contribution);
        }
        Ok(contributions)
    }

    fn query_output_closure_contribution_with_memo(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        output: ResultRowEntry,
        identity: AuthorId,
        memo: &mut ClosureExpansionMemo,
        context: &mut ViewEvaluationContext,
    ) -> Result<BTreeSet<ResultRowEntry>, Error> {
        let mut set = BTreeSet::from([output]);
        let output_table = shape.query().table.clone();
        self.expand_reference_closure(
            &output_table,
            output.1,
            &shape.query().includes,
            &mut set,
            DurabilityTier::Global,
            memo,
        )?;
        self.expand_join_closure_for_output(
            shape,
            binding,
            output.1,
            &mut set,
            DurabilityTier::Global,
            memo,
        )?;
        self.retain_policy_atomic_rows(&mut set, identity, context)?;
        Ok(set)
    }

    /// Build a view update from already-computed result set adds/removes.
    pub(crate) fn view_update_for_query_result_delta(
        &mut self,
        subscription: SubscriptionKey,
        peer_complete_tx_payloads: impl IntoIterator<Item = TxId>,
        previous_result_set: impl IntoIterator<Item = TxId>,
        result_row_adds: Vec<ResultRowEntry>,
        result_row_removes: Vec<ResultRowEntry>,
        identity: AuthorId,
    ) -> Result<SyncMessage, Error> {
        let peer_complete_tx_payloads = peer_complete_tx_payloads
            .into_iter()
            .collect::<BTreeSet<_>>();
        let _previous_result_set = previous_result_set.into_iter().collect::<BTreeSet<_>>();
        let mut context = ViewEvaluationContext::default();
        let wanted_rows_by_tx = result_row_adds
            .iter()
            .map(|(table, row_uuid, tx_id)| (*tx_id, (table.to_string(), *row_uuid)))
            .fold(
                BTreeMap::<TxId, BTreeSet<(String, RowUuid)>>::new(),
                |mut by_tx, (tx_id, row)| {
                    by_tx.entry(tx_id).or_default().insert(row);
                    by_tx
                },
            );
        let mut version_bundles = Vec::with_capacity(result_row_adds.len());
        let mut peer_payload_inventory_refs = Vec::new();
        let mut emitted_versions = BTreeSet::new();
        for (entry_table, row_uuid, tx_id) in &result_row_adds {
            if peer_complete_tx_payloads.contains(tx_id) {
                peer_payload_inventory_refs.push(*tx_id);
                continue;
            }
            if !emitted_versions.insert(*tx_id) {
                continue;
            }
            let table_schema = self.table(entry_table.as_str())?.clone();
            let tx_versions = self.query_versions_for_tx_memo_cloned(*tx_id, &mut context)?;
            let version = tx_versions
                .iter()
                .find(|version| {
                    version.table() == entry_table.as_str()
                        && version.row_uuid() == *row_uuid
                        && version.deletion().is_none()
                })
                .ok_or(Error::MissingTransaction(*tx_id))?;
            if self.read_policy_allows_version_memo(
                &table_schema,
                version,
                identity,
                &mut context,
            )? {
                let wanted_rows = wanted_rows_by_tx
                    .get(tx_id)
                    .ok_or(Error::MissingTransaction(*tx_id))?;
                let bundle_versions = tx_versions
                    .iter()
                    .filter(|version| {
                        wanted_rows.contains(&(version.table().to_owned(), version.row_uuid()))
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                version_bundles.push(self.version_bundle_for_view_memo_with_versions(
                    &table_schema,
                    version,
                    *tx_id,
                    &bundle_versions,
                    identity,
                    &mut context,
                )?);
            }
        }
        for (entry_table, row_uuid, content_tx_id) in &result_row_adds {
            let Some(version) =
                self.query_global_layer_winner(entry_table, *row_uuid, VersionLayer::Deletion)?
            else {
                continue;
            };
            let tx_id = self.version_tx_id(&version)?;
            if tx_id == *content_tx_id || !emitted_versions.insert(tx_id) {
                continue;
            }
            let table_schema = self.table(entry_table)?.clone();
            if self.read_policy_allows_deletion_version_memo(
                &table_schema,
                &version,
                identity,
                &mut context,
            )? {
                if peer_complete_tx_payloads.contains(&tx_id) {
                    peer_payload_inventory_refs.push(tx_id);
                } else {
                    version_bundles.push(self.version_bundle_for_view_memo(
                        &table_schema,
                        &version,
                        identity,
                        &mut context,
                    )?);
                }
            }
        }
        for (entry_table, row_uuid, old_tx_id) in &result_row_removes {
            if let Some(version) = self.visible_global_content_version_now(entry_table, *row_uuid) {
                let tx_id = self.version_tx_id(&version)?;
                if tx_id != *old_tx_id && !emitted_versions.contains(&tx_id) {
                    let table_schema = self.table(entry_table)?.clone();
                    if self.read_policy_allows_version_memo(
                        &table_schema,
                        &version,
                        identity,
                        &mut context,
                    )? {
                        if peer_complete_tx_payloads.contains(&tx_id) {
                            peer_payload_inventory_refs.push(tx_id);
                        } else {
                            emitted_versions.insert(tx_id);
                            version_bundles.push(self.version_bundle_for_view_memo(
                                &table_schema,
                                &version,
                                identity,
                                &mut context,
                            )?);
                        }
                    }
                }
            } else {
                let Some(version) =
                    self.query_global_layer_winner(entry_table, *row_uuid, VersionLayer::Deletion)?
                else {
                    continue;
                };
                let tx_id = self.version_tx_id(&version)?;
                if tx_id != *old_tx_id && !emitted_versions.contains(&tx_id) {
                    let table_schema = self.table(entry_table)?.clone();
                    if self.read_policy_allows_deletion_version_memo(
                        &table_schema,
                        &version,
                        identity,
                        &mut context,
                    )? {
                        if peer_complete_tx_payloads.contains(&tx_id) {
                            peer_payload_inventory_refs.push(tx_id);
                        } else {
                            emitted_versions.insert(tx_id);
                            version_bundles.push(self.version_bundle_for_view_memo(
                                &table_schema,
                                &version,
                                identity,
                                &mut context,
                            )?);
                        }
                    }
                }
            }
        }
        for bundle in &mut version_bundles {
            if bundle.tx.kind != TxKind::Exclusive {
                continue;
            }
            let Some(wanted_rows) = wanted_rows_by_tx.get(&bundle.tx.tx_id) else {
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
            result_row_adds,
            result_row_removes,
        })
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
            result_row_adds,
            result_row_removes,
            identity,
            tier,
            mut versions_by_tx,
            replacement_for,
        } = inputs;
        let mut context = ViewEvaluationContext::for_policy_read_tier(tier);
        let mut tx_versions_cache = BTreeMap::<TxId, Vec<VersionRow>>::new();
        let wanted_add_rows_by_tx = result_row_adds
            .iter()
            .map(|(table, row_uuid, tx_id)| (*tx_id, (table.to_string(), *row_uuid)))
            .fold(
                BTreeMap::<TxId, BTreeSet<(String, RowUuid)>>::new(),
                |mut by_tx, (tx_id, row)| {
                    by_tx.entry(tx_id).or_default().insert(row);
                    by_tx
                },
            );
        let mut version_bundles = Vec::with_capacity(result_row_adds.len());
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

            record_maintained_view_add_bundle_fallback();
            let tx_versions = self.query_versions_for_tx_memo_cloned(*tx_id, &mut context)?;
            let version = tx_versions
                .iter()
                .find(|version| {
                    version.deletion().is_none()
                        && wanted_rows.contains(&(version.table().to_owned(), version.row_uuid()))
                })
                .ok_or(Error::MissingTransaction(*tx_id))?;
            let table_schema = self.table(version.table())?.clone();
            if self.read_policy_allows_version_memo(
                &table_schema,
                version,
                identity,
                &mut context,
            )? {
                let bundle_versions = if complete_exclusive_payloads {
                    tx_versions.clone()
                } else {
                    tx_versions
                        .iter()
                        .filter(|version| {
                            wanted_rows.contains(&(version.table().to_owned(), version.row_uuid()))
                        })
                        .cloned()
                        .collect::<Vec<_>>()
                };
                version_bundles.push(self.version_bundle_for_view_memo_with_versions(
                    &table_schema,
                    version,
                    *tx_id,
                    &bundle_versions,
                    identity,
                    &mut context,
                )?);
            }
        }
        for (entry_table, row_uuid, content_tx_id) in &result_row_adds {
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
                    record_maintained_view_removal_bundle_fallback();
                    let Some(version) = self.query_global_layer_winner(
                        entry_table,
                        *row_uuid,
                        VersionLayer::Deletion,
                    )?
                    else {
                        continue;
                    };
                    let table_schema = self.table(entry_table)?.clone();
                    if self.read_policy_allows_deletion_version_memo(
                        &table_schema,
                        &version,
                        identity,
                        &mut context,
                    )? {
                        version_bundles.push(self.version_bundle_for_view_memo(
                            &table_schema,
                            &version,
                            identity,
                            &mut context,
                        )?);
                    }
                }
            }
        }
        for (entry_table, row_uuid, old_tx_id) in &result_row_removes {
            let (content_winner, deletion_winner) =
                replacement_for(entry_table.to_string(), *row_uuid);
            if let Some(version) = content_winner.as_ref() {
                let tx_id = self.version_tx_id(version)?;
                if tx_id != *old_tx_id && !emitted_versions.contains(&tx_id) {
                    if peer_complete_tx_payloads.contains(&tx_id) {
                        peer_payload_inventory_refs.push(tx_id);
                        record_maintained_view_removal_stream_bundle();
                    } else {
                        let tx_versions = tx_versions_cache
                            .entry(tx_id)
                            .or_insert_with(|| versions_by_tx(tx_id));
                        if maintained_view_tx_versions_contain_winner(tx_versions, version) {
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
                        } else {
                            record_maintained_view_removal_bundle_fallback();
                            if let Some(version) =
                                self.visible_global_content_version_now(entry_table, *row_uuid)
                            {
                                let fallback_tx_id = self.version_tx_id(&version)?;
                                let table_schema = self.table(entry_table)?.clone();
                                if self.read_policy_allows_version_memo(
                                    &table_schema,
                                    &version,
                                    identity,
                                    &mut context,
                                )? {
                                    emitted_versions.insert(fallback_tx_id);
                                    version_bundles.push(self.version_bundle_for_view_memo(
                                        &table_schema,
                                        &version,
                                        identity,
                                        &mut context,
                                    )?);
                                }
                            }
                        }
                    }
                }
            } else if let Some(version) = deletion_winner.as_ref() {
                let tx_id = self.version_tx_id(version)?;
                if tx_id != *old_tx_id && !emitted_versions.contains(&tx_id) {
                    if peer_complete_tx_payloads.contains(&tx_id) {
                        peer_payload_inventory_refs.push(tx_id);
                        record_maintained_view_removal_stream_bundle();
                    } else {
                        let tx_versions = tx_versions_cache
                            .entry(tx_id)
                            .or_insert_with(|| versions_by_tx(tx_id));
                        if maintained_view_tx_versions_contain_winner(tx_versions, version) {
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
                        } else {
                            record_maintained_view_removal_bundle_fallback();
                            let Some(version) = self.query_global_layer_winner(
                                entry_table,
                                *row_uuid,
                                VersionLayer::Deletion,
                            )?
                            else {
                                continue;
                            };
                            let fallback_tx_id = self.version_tx_id(&version)?;
                            let table_schema = self.table(entry_table)?.clone();
                            if self.read_policy_allows_deletion_version_memo(
                                &table_schema,
                                &version,
                                identity,
                                &mut context,
                            )? {
                                emitted_versions.insert(fallback_tx_id);
                                version_bundles.push(self.version_bundle_for_view_memo(
                                    &table_schema,
                                    &version,
                                    identity,
                                    &mut context,
                                )?);
                            }
                        }
                    }
                }
            } else if let Some(version) =
                self.visible_global_content_version_now(entry_table, *row_uuid)
            {
                let tx_id = self.version_tx_id(&version)?;
                if tx_id != *old_tx_id && !emitted_versions.contains(&tx_id) {
                    if peer_complete_tx_payloads.contains(&tx_id) {
                        peer_payload_inventory_refs.push(tx_id);
                        record_maintained_view_removal_stream_bundle();
                    } else {
                        let table_schema = self.table(entry_table)?.clone();
                        if self.read_policy_allows_version_memo(
                            &table_schema,
                            &version,
                            identity,
                            &mut context,
                        )? {
                            emitted_versions.insert(tx_id);
                            version_bundles.push(self.version_bundle_for_view_memo(
                                &table_schema,
                                &version,
                                identity,
                                &mut context,
                            )?);
                            record_maintained_view_removal_bundle_fallback();
                        }
                    }
                }
            } else if let Some(version) =
                self.query_global_layer_winner(entry_table, *row_uuid, VersionLayer::Deletion)?
            {
                let tx_id = self.version_tx_id(&version)?;
                if tx_id != *old_tx_id && !emitted_versions.contains(&tx_id) {
                    if peer_complete_tx_payloads.contains(&tx_id) {
                        peer_payload_inventory_refs.push(tx_id);
                        record_maintained_view_removal_stream_bundle();
                    } else {
                        let table_schema = self.table(entry_table)?.clone();
                        if self.read_policy_allows_deletion_version_memo(
                            &table_schema,
                            &version,
                            identity,
                            &mut context,
                        )? {
                            emitted_versions.insert(tx_id);
                            version_bundles.push(self.version_bundle_for_view_memo(
                                &table_schema,
                                &version,
                                identity,
                                &mut context,
                            )?);
                            record_maintained_view_removal_bundle_fallback();
                        }
                    }
                }
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
            result_row_adds,
            result_row_removes,
        })
    }

    pub(crate) fn visible_exclusive_tx_result_entries_for_table(
        &mut self,
        table_name: &str,
        tx_id: TxId,
        identity: AuthorId,
    ) -> Result<BTreeSet<ResultRowEntry>, Error> {
        if self
            .query_transaction(tx_id)?
            .is_none_or(|tx| tx.tx.kind != TxKind::Exclusive)
        {
            return Ok(BTreeSet::new());
        }
        let table = self.table(table_name)?.clone();
        let mut entries = BTreeSet::new();
        for version in self.query_versions_for_tx(tx_id)? {
            if version.table() != table_name || version.deletion().is_some() {
                continue;
            }
            if self.visible_global_content_tx_id_now(table_name, version.row_uuid()) != Some(tx_id)
            {
                continue;
            }
            if self.read_policy_allows_version(&table, &version, identity)? {
                entries.insert((
                    groove::Intern::new(table_name.to_owned()),
                    version.row_uuid(),
                    tx_id,
                ));
            }
        }
        Ok(entries)
    }

    pub(crate) fn expand_maintained_view_result_rows(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        rows: impl IntoIterator<Item = ResultRowEntry>,
        identity: AuthorId,
        tier: DurabilityTier,
    ) -> Result<BTreeSet<ResultRowEntry>, Error> {
        let mut context = ViewEvaluationContext::for_policy_read_tier(tier);
        let mut set = rows.into_iter().collect::<BTreeSet<_>>();
        let root_table = shape.query().table.clone();
        let root_result_entries = set
            .iter()
            .filter(|(entry_table, _, _)| entry_table.as_str() == root_table)
            .cloned()
            .collect::<BTreeSet<_>>();
        self.expand_query_closure(shape, binding, &mut set, tier)?;
        self.retain_policy_atomic_rows(&mut set, identity, &mut context)?;
        set.extend(root_result_entries);
        Ok(set)
    }

    /// Apply a downstream current-row view update.
    pub(super) fn apply_view_update(&mut self, update: ViewUpdateParts) -> Result<(), Error> {
        let ViewUpdateParts {
            subscription,
            reset_result_set,
            version_bundles,
            peer_complete_tx_payload_refs,
            result_row_adds,
            result_row_removes,
        } = update;
        let incoming_bundle_tx_ids = version_bundles
            .iter()
            .map(|bundle| bundle.tx.tx_id)
            .collect::<BTreeSet<_>>();
        let cold_bulk_loaded = reset_result_set
            && peer_complete_tx_payload_refs.is_empty()
            && result_row_removes.is_empty()
            && self.ingest_cold_view_bundles_if_empty(&version_bundles)?;
        if !cold_bulk_loaded {
            for bundle in version_bundles {
                self.ingest_view_bundle(bundle)?;
            }
        }
        if !cold_bulk_loaded {
            for tx_id in peer_complete_tx_payload_refs
                .iter()
                .chain(result_row_adds.iter().map(|(_, _, tx_id)| tx_id))
                .chain(result_row_removes.iter().map(|(_, _, tx_id)| tx_id))
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
        self.validate_result_row_adds_are_witnessed(
            &peer_complete_tx_payload_refs,
            &result_row_adds,
        )?;
        if reset_result_set {
            self.query.settled_result_sets.remove(&subscription);
        }
        let canonical_subscription = self.canonical_subscription_for_usage(subscription)?;
        let mirrored_result_set = {
            let row_result_set = self
                .query
                .settled_result_sets
                .entry(subscription)
                .or_default();
            for member in result_row_removes {
                row_result_set.remove(&member);
            }
            row_result_set.extend(result_row_adds);
            row_result_set.clone()
        };
        if let Some(canonical_subscription) = canonical_subscription {
            if reset_result_set {
                self.query
                    .settled_result_sets
                    .remove(&canonical_subscription);
            }
            self.query
                .settled_result_sets
                .insert(canonical_subscription, mirrored_result_set.clone());
        }
        // Diagnostic-only: the duplicate-content-version scan feeds a
        // debug_assert, so it is wasted work in release. Gate to debug builds.
        #[cfg(debug_assertions)]
        if let Some((table, row_uuid, first, second)) =
            duplicate_row_result_set(&mirrored_result_set)
        {
            debug_assert!(
                first == second,
                "settled subscription {subscription:?} has multiple content versions for {table}.{row_uuid:?}: {first:?} and {second:?}"
            );
        }
        Ok(())
    }

    fn canonical_subscription_for_usage(
        &self,
        subscription: SubscriptionKey,
    ) -> Result<Option<SubscriptionKey>, Error> {
        let Some(shape) = self.query.registered_shapes.get(&subscription.shape_id) else {
            return Ok(None);
        };
        let Some(values) = self
            .query
            .registered_bindings
            .get(&subscription.shape_id)
            .and_then(|bindings| bindings.get(&subscription.binding_id))
        else {
            return Ok(None);
        };
        let value_map = shape
            .params()
            .keys()
            .cloned()
            .zip(values.iter().cloned())
            .collect::<BTreeMap<_, _>>();
        let binding = shape.bind(value_map)?;
        let canonical = SubscriptionKey {
            shape_id: subscription.shape_id,
            binding_id: binding.binding_id(),
        };
        Ok((canonical != subscription).then_some(canonical))
    }

    fn validate_result_row_adds_are_witnessed(
        &mut self,
        peer_complete_tx_payload_refs: &[TxId],
        result_row_adds: &[ResultRowEntry],
    ) -> Result<(), Error> {
        let peer_complete_tx_payload_refs = peer_complete_tx_payload_refs
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut partial_exclusive_keys = BTreeMap::<TxId, BTreeSet<(String, RowUuid)>>::new();
        for (table, row_uuid, tx_id) in result_row_adds {
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

    fn add_visible_result_set_entry(
        &mut self,
        set: &mut BTreeSet<ResultRowEntry>,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<(), Error> {
        if let Some(tx_id) = self.visible_global_content_tx_id_now(table, row_uuid) {
            set.insert((groove::Intern::new(table.to_owned()), row_uuid, tx_id));
            Ok(())
        } else {
            Err(Error::InvalidStoredValue(
                "closure row missing global winner",
            ))
        }
    }

    fn add_visible_result_set_entry_with_memo(
        &mut self,
        set: &mut BTreeSet<ResultRowEntry>,
        table: &str,
        row_uuid: RowUuid,
        tier: DurabilityTier,
        memo: &mut ClosureExpansionMemo,
    ) -> Result<(), Error> {
        if let Some(tx_id) =
            self.visible_content_tx_id_for_view_tier_with_memo(table, row_uuid, tier, memo)?
        {
            set.insert((groove::Intern::new(table.to_owned()), row_uuid, tx_id));
            Ok(())
        } else {
            Err(Error::InvalidStoredValue(
                "closure row missing global winner",
            ))
        }
    }

    fn add_optional_visible_result_set_entry_with_memo(
        &mut self,
        set: &mut BTreeSet<ResultRowEntry>,
        table: &str,
        row_uuid: RowUuid,
        tier: DurabilityTier,
        memo: &mut ClosureExpansionMemo,
    ) -> Result<(), Error> {
        if let Some(tx_id) =
            self.visible_content_tx_id_for_view_tier_with_memo(table, row_uuid, tier, memo)?
        {
            set.insert((groove::Intern::new(table.to_owned()), row_uuid, tx_id));
        }
        Ok(())
    }

    fn expand_query_closure(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        set: &mut BTreeSet<ResultRowEntry>,
        tier: DurabilityTier,
    ) -> Result<(), Error> {
        let mut memo = ClosureExpansionMemo::default();
        self.expand_query_closure_with_memo(shape, binding, set, tier, &mut memo)
    }

    fn expand_query_closure_with_memo(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        set: &mut BTreeSet<ResultRowEntry>,
        tier: DurabilityTier,
        memo: &mut ClosureExpansionMemo,
    ) -> Result<(), Error> {
        let output_table = shape.query().table.clone();
        let output_rows = set
            .iter()
            .filter(|(table, _, _)| table.as_str() == output_table)
            .map(|(_, row_uuid, _)| *row_uuid)
            .collect::<BTreeSet<_>>();
        for row_uuid in &output_rows {
            self.expand_reference_closure(
                &output_table,
                *row_uuid,
                &shape.query().includes,
                set,
                tier,
                memo,
            )?;
            self.expand_join_closure_for_output(shape, binding, *row_uuid, set, tier, memo)?;
        }
        Ok(())
    }

    fn expand_join_closure_for_output(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        output_row_uuid: RowUuid,
        set: &mut BTreeSet<ResultRowEntry>,
        tier: DurabilityTier,
        memo: &mut ClosureExpansionMemo,
    ) -> Result<(), Error> {
        let output_table = shape.query().table.clone();
        for join in &shape.query().joins {
            let mut join_query = crate::query::Query::from(&join.table);
            for predicate in &join.filters {
                join_query = join_query.filter(predicate.clone());
            }
            let join_shape = join_query.validate(&self.catalogue.schema)?;
            let join_binding = binding_for_shape(&join_shape, binding)?;
            let join_table = self.table(&join.table)?.clone();
            let output_table_schema = self.table(&output_table)?.clone();
            let target_row_uuid = if let Some(source_column) = &join.source_column {
                let Some(output_row) =
                    self.current_row_now_with_memo(&output_table, output_row_uuid, tier, memo)?
                else {
                    continue;
                };
                match output_row.cell(&output_table_schema, source_column) {
                    Some(Value::Uuid(uuid)) => RowUuid(uuid),
                    _ => continue,
                }
            } else {
                output_row_uuid
            };
            let rows_by_target = self.join_rows_by_target_with_memo(
                &join_shape,
                &join_binding,
                &join_table,
                join,
                tier,
                memo,
            )?;
            let join_rows = rows_by_target
                .get(&target_row_uuid)
                .cloned()
                .unwrap_or_default();
            for join_row in join_rows {
                self.add_visible_result_set_entry_with_memo(
                    set,
                    &join.table,
                    join_row.row_uuid(),
                    tier,
                    memo,
                )?;
                self.expand_reference_closure(
                    &join.table,
                    join_row.row_uuid(),
                    &[],
                    set,
                    tier,
                    memo,
                )?;
            }
        }
        Ok(())
    }

    fn expand_reference_closure(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        include_paths: &[crate::query::Include],
        set: &mut BTreeSet<ResultRowEntry>,
        tier: DurabilityTier,
        memo: &mut ClosureExpansionMemo,
    ) -> Result<(), Error> {
        let table = self.table(table_name)?.clone();
        let Some(row) = self.current_row_now_with_memo(table_name, row_uuid, tier, memo)? else {
            return Ok(());
        };
        if include_paths.is_empty() {
            for (column, target_table) in &table.references {
                if let Some(Value::Uuid(target)) = row.cell(&table, column) {
                    self.add_optional_visible_result_set_entry_with_memo(
                        set,
                        target_table,
                        RowUuid(target),
                        tier,
                        memo,
                    )?;
                }
            }
        }
        for include in include_paths {
            self.expand_include_path(&table, &row, include, set, tier, memo)?;
        }
        Ok(())
    }

    fn expand_include_path(
        &mut self,
        table: &TableSchema,
        row: &CurrentRow,
        include: &crate::query::Include,
        set: &mut BTreeSet<ResultRowEntry>,
        tier: DurabilityTier,
        memo: &mut ClosureExpansionMemo,
    ) -> Result<(), Error> {
        let mut current_table = table.clone();
        let mut current_row_uuid = row.row_uuid();
        let mut path_entries = Vec::new();
        let require_complete_path =
            include.require || include.join_mode == crate::query::JoinMode::Inner;
        for segment in include.path.split('.') {
            let Some(target_table) = current_table.references.get(segment).cloned() else {
                return Ok(());
            };
            let Some(current_row) =
                self.current_row_now_with_memo(&current_table.name, current_row_uuid, tier, memo)?
            else {
                return Ok(());
            };
            let Some(Value::Uuid(target_uuid)) = current_row.cell(&current_table, segment) else {
                return Ok(());
            };
            let target_row = RowUuid(target_uuid);
            if !require_complete_path {
                self.add_optional_visible_result_set_entry_with_memo(
                    set,
                    &target_table,
                    target_row,
                    tier,
                    memo,
                )?;
            } else if let Some(tx_id) = self.visible_content_tx_id_for_view_tier_with_memo(
                &target_table,
                target_row,
                tier,
                memo,
            )? {
                path_entries.push((groove::Intern::new(target_table.clone()), target_row, tx_id));
            } else {
                return Ok(());
            }
            current_table = self.table(&target_table)?.clone();
            current_row_uuid = target_row;
        }
        if require_complete_path {
            set.extend(path_entries);
        }
        Ok(())
    }

    fn current_row_now_with_memo(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        tier: DurabilityTier,
        memo: &mut ClosureExpansionMemo,
    ) -> Result<Option<CurrentRow>, Error> {
        let key = (table_name.to_owned(), row_uuid);
        if let Some(row) = memo.current_rows.get(&key) {
            return Ok(row.clone());
        }
        let row = self
            .visible_current_row_for_view_tier_with_memo(table_name, row_uuid, tier, memo)?
            .map(|(row, _)| row);
        memo.current_rows.insert(key, row.clone());
        Ok(row)
    }

    fn visible_content_tx_id_for_view_tier_with_memo(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        tier: DurabilityTier,
        memo: &mut ClosureExpansionMemo,
    ) -> Result<Option<TxId>, Error> {
        Ok(self
            .visible_current_row_for_view_tier_with_memo(table_name, row_uuid, tier, memo)?
            .map(|(_, tx_id)| tx_id))
    }

    fn visible_current_row_for_view_tier_with_memo(
        &mut self,
        table_name: &str,
        row_uuid: RowUuid,
        tier: DurabilityTier,
        memo: &mut ClosureExpansionMemo,
    ) -> Result<Option<(CurrentRow, TxId)>, Error> {
        let key = (table_name.to_owned(), row_uuid, tier);
        if let Some(row) = memo.visible_current.get(&key) {
            return Ok(row.clone());
        }

        let table = self.table(table_name)?.clone();
        if self
            .current_layer_winner_for_view_tier(table_name, row_uuid, VersionLayer::Deletion, tier)?
            .is_some_and(|version| version.deletion().is_some())
        {
            memo.visible_current.insert(key, None);
            return Ok(None);
        }
        let visible = self
            .current_layer_winner_for_view_tier(table_name, row_uuid, VersionLayer::Content, tier)?
            .map(|version| {
                let tx_id = self.version_tx_id(&version)?;
                let row = self.current_row_from_materialized_version(&table, &version)?;
                Ok::<_, Error>((row, tx_id))
            })
            .transpose()?;
        memo.visible_current.insert(key, visible.clone());
        Ok(visible)
    }

    fn join_rows_by_target_with_memo<'a>(
        &mut self,
        join_shape: &ValidatedQuery,
        join_binding: &Binding,
        join_table: &TableSchema,
        join: &crate::query::JoinVia,
        tier: DurabilityTier,
        memo: &'a mut ClosureExpansionMemo,
    ) -> Result<&'a BTreeMap<RowUuid, Vec<CurrentRow>>, Error> {
        let key = JoinRowsKey {
            shape_id: join_shape.shape_id(),
            binding_id: join_binding.binding_id(),
            table: join.table.clone(),
            on_column: join.on_column.clone(),
            tier,
        };
        if !memo.join_rows_by_target.contains_key(&key) {
            let mut rows_by_target: BTreeMap<RowUuid, Vec<CurrentRow>> = BTreeMap::new();
            for join_row in self.query_rows(join_shape, join_binding, tier)? {
                if let Some(Value::Uuid(uuid)) = join_row.cell(join_table, &join.on_column) {
                    rows_by_target
                        .entry(RowUuid(uuid))
                        .or_default()
                        .push(join_row);
                }
            }
            memo.join_rows_by_target.insert(key.clone(), rows_by_target);
        }
        Ok(memo
            .join_rows_by_target
            .get(&key)
            .expect("join rows memo populated"))
    }

    fn current_deletion_register_versions_for_view(
        &mut self,
        table: &str,
    ) -> Result<Vec<VersionRow>, Error> {
        let current_table = register_global_current_table_name(table);
        let rows = self
            .database
            .primary_key_scan_raw(&current_table, &[])?
            .into_iter()
            .map(|raw| raw.raw().to_vec())
            .collect::<Vec<_>>();
        let mut versions = Vec::with_capacity(rows.len());
        let descriptor = self.table(table)?.global_current_storage_tables()[1].record_schema();
        for raw in rows {
            let record = BorrowedRecord::new(&raw, &descriptor);
            let deletion = deletion_event_from_value(
                record.get_idx(RegisterGlobalCurrentRowRecord::FIELD__DELETION_IDX)?,
            )?;
            if deletion != DeletionEvent::Deleted {
                continue;
            }
            let row_uuid =
                RowUuid(record.get_uuid(RegisterGlobalCurrentRowRecord::FIELD_ROW_UUID_IDX)?);
            let tx_time =
                TxTime(record.get_u64(RegisterGlobalCurrentRowRecord::FIELD_TX_TIME_IDX)?);
            let tx_node_alias =
                NodeAlias(record.get_u64(RegisterGlobalCurrentRowRecord::FIELD_TX_NODE_ID_IDX)?);
            if let Some(version) = self.query_version_by_alias(
                table,
                row_uuid,
                VersionLayer::Deletion,
                tx_time,
                tx_node_alias,
            )? {
                versions.push(version);
            }
        }
        Ok(versions)
    }

    fn current_layer_winner_for_view_tier(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        layer: VersionLayer,
        tier: DurabilityTier,
    ) -> Result<Option<VersionRow>, Error> {
        match tier {
            DurabilityTier::Global => self.query_global_layer_winner(table, row_uuid, layer),
            DurabilityTier::Edge => self.current_layer_winner_for_ahead_row(
                table,
                row_uuid,
                layer,
                Some(DurabilityTier::Edge),
            ),
            DurabilityTier::None | DurabilityTier::Local => {
                self.current_layer_winner_for_ahead_row(table, row_uuid, layer, None)
            }
        }
    }

    pub(super) fn version_bundle_for_view_memo(
        &mut self,
        table: &TableSchema,
        version: &VersionRow,
        identity: AuthorId,
        context: &mut ViewEvaluationContext,
    ) -> Result<VersionBundle, Error> {
        let tx_id = self.version_tx_id(version)?;
        let tx_versions = self.query_versions_for_tx_memo_cloned(tx_id, context)?;
        self.version_bundle_for_view_memo_with_versions(
            table,
            version,
            tx_id,
            &tx_versions,
            identity,
            context,
        )
    }

    pub(super) fn version_bundle_for_view_memo_with_versions(
        &mut self,
        table: &TableSchema,
        _version: &VersionRow,
        tx_id: TxId,
        tx_versions: &[VersionRow],
        identity: AuthorId,
        context: &mut ViewEvaluationContext,
    ) -> Result<VersionBundle, Error> {
        let stored_tx = self
            .query_transaction(tx_id)?
            .ok_or(Error::MissingTransaction(tx_id))?;
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
        } = stored_tx.tx;
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
            if candidate.table() != table.name
                || self.read_policy_allows_version_memo(table, candidate, identity, context)?
                || self
                    .read_policy_allows_deletion_version_memo(table, candidate, identity, context)?
            {
                versions.push(self.version_record_from_row(candidate)?);
            }
        }
        Ok(VersionBundle {
            tx: tx_payload,
            versions,
            fate: stored_tx.fate.clone(),
            global_seq: stored_tx.global_seq,
            durability: stored_tx.durability,
        })
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

fn is_degenerate_whole_table(shape: &ValidatedQuery, binding: &Binding) -> bool {
    let query = shape.query();
    query.filters.is_empty()
        && query.joins.is_empty()
        && query.includes.is_empty()
        && binding.values().is_empty()
}

fn view_version_key(version: &VersionRecord) -> (String, RowUuid, VersionLayer) {
    (
        version.table().to_owned(),
        version.row_uuid(),
        VersionLayer::for_record(version),
    )
}
