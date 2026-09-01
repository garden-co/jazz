//! View-update construction for subscribers and sync peers. This module owns
//! current-row and query-result bundle assembly, closure expansion, settled
//! canonical binding-view result-set/completeness state, and deduplicated
//! version shipping; per-peer shipped state lives in [`crate::peer`], policy
//! filtering in [`super::policy`], and query execution/planning in
//! [`super::query_eval`]. It sits on the node side of the protocol boundary and
//! emits [`crate::protocol::SyncMessage`] values.

use super::ingest::validate_received_view_bundle_global_time_durability;
use super::policy::ViewEvaluationContext;
use super::*;
use crate::ids::SchemaVersionId;
use crate::node::maintained_subscription_view::MaintainedSubscriptionView;
use crate::node::query_engine::left_field;
use crate::protocol::{
    ContributingMembersEntry, KnownStateDeclaration, PeerPayloadInventory, ProgramFactEntry,
    RealRowMemberEntry, ResultMemberEntry, RowVersionRef, VersionBundle, VersionBundleRef,
    VersionCarrier, VersionRecord, build_version_carriers_from_singletons,
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

fn merge_receiver_version_bundle_ref(
    bundles: &mut BTreeMap<TxId, VersionBundle>,
    bundle: VersionBundleRef<'_>,
) -> Result<(), Error> {
    let incoming = canonical_receiver_bundle(bundle)?;
    let Some(existing) = bundles.get_mut(&bundle.tx.tx_id) else {
        bundles.insert(bundle.tx.tx_id, incoming);
        return Ok(());
    };
    let mut existing_tx_identity = existing.tx.clone();
    existing_tx_identity.n_total_writes = 0;
    let mut incoming_tx_identity = bundle.tx.clone();
    incoming_tx_identity.n_total_writes = 0;
    if existing_tx_identity != incoming_tx_identity
        || existing.fate != incoming.fate
        || existing.global_time != incoming.global_time
        || existing.durability != incoming.durability
    {
        return Err(Error::ConflictingCommitUnit(bundle.tx.tx_id));
    }
    let mut seen = existing
        .versions
        .iter()
        .cloned()
        .map(|version| (version_bundle_record_key(&version), version))
        .collect::<BTreeMap<_, VersionRecord>>();
    for version in &incoming.versions {
        let key = version_bundle_record_key(version);
        match seen.get(&key) {
            Some(existing) if existing == version => {}
            Some(_) => return Err(Error::ConflictingCommitUnit(bundle.tx.tx_id)),
            None => {
                seen.insert(key, version.clone());
            }
        }
    }
    use crate::protocol::VersionBundleScope::{CompleteTransaction, ViewScoped};
    match (existing.scope, incoming.scope) {
        (CompleteTransaction, CompleteTransaction) => {
            if existing.tx.n_total_writes != incoming.tx.n_total_writes
                || existing.versions != incoming.versions
            {
                return Err(Error::ConflictingCommitUnit(bundle.tx.tx_id));
            }
        }
        (CompleteTransaction, ViewScoped) => {
            validate_version_subset(&incoming.versions, &existing.versions, bundle.tx.tx_id)?;
        }
        (ViewScoped, CompleteTransaction) => {
            validate_version_subset(&existing.versions, &incoming.versions, bundle.tx.tx_id)?;
            *existing = incoming;
        }
        (ViewScoped, ViewScoped) => {
            existing.versions = seen.into_values().collect();
            existing.tx.n_total_writes = existing
                .versions
                .len()
                .try_into()
                .map_err(|_| Error::InvalidStoredValue("view payload is too large"))?;
        }
    }
    Ok(())
}

fn canonical_receiver_bundle(bundle: VersionBundleRef<'_>) -> Result<VersionBundle, Error> {
    let mut versions = BTreeMap::new();
    for version in bundle.versions {
        let key = version_bundle_record_key(version);
        match versions.get(&key) {
            Some(existing) if existing != version => {
                return Err(Error::ConflictingCommitUnit(bundle.tx.tx_id));
            }
            Some(_) => {}
            None => {
                versions.insert(key, version.clone());
            }
        }
    }
    let versions = versions.into_values().collect::<Vec<_>>();
    if usize::try_from(bundle.tx.n_total_writes).ok() != Some(versions.len()) {
        return Err(Error::ConflictingCommitUnit(bundle.tx.tx_id));
    }
    let mut owned = bundle.to_owned_bundle();
    owned.versions = versions;
    Ok(owned)
}

fn validate_version_subset(
    subset: &[VersionRecord],
    complete: &[VersionRecord],
    tx_id: TxId,
) -> Result<(), Error> {
    let complete = complete
        .iter()
        .map(|version| (version_bundle_record_key(version), version))
        .collect::<BTreeMap<_, _>>();
    for version in subset {
        if complete.get(&version_bundle_record_key(version)) != Some(&version) {
            return Err(Error::ConflictingCommitUnit(tx_id));
        }
    }
    Ok(())
}

fn version_bundle_refs_for_carriers(
    version_carriers: &[VersionCarrier],
) -> Result<Vec<VersionBundleRef<'_>>, Error> {
    let mut refs = Vec::with_capacity(version_carriers.len());
    for carrier in version_carriers {
        refs.extend(
            carrier
                .bundle_refs()
                .map_err(|_| Error::MalformedViewUpdate("malformed version-bundle run"))?,
        );
    }
    Ok(refs)
}

fn version_bundle_record_key(
    version: &VersionRecord,
) -> (String, BranchKey, RowUuid, SchemaVersionId, bool) {
    (
        version.table().to_owned(),
        version.branch_key().clone(),
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
                ResultMemberEntry::Row(_) | ResultMemberEntry::TypedRow { .. } => context,
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

/// Source vocabulary required to turn maintained flat-tuple membership into
/// canonical contributor facts. Construct this from the validated query rather
/// than independently from its result rows: an empty vocabulary on a flat join
/// produces a payload that renders once but cannot be reconstructed afterward.
pub(crate) struct FlatTupleSourceTables(Vec<String>);

impl FlatTupleSourceTables {
    pub(crate) fn for_query(shape: &ValidatedQuery) -> Self {
        Self(
            shape
                .query()
                .flat_join
                .as_ref()
                .map(|flat_join| {
                    flat_join
                        .sources
                        .iter()
                        .map(|source| source.table.clone())
                        .collect()
                })
                .unwrap_or_default(),
        )
    }

    pub(crate) fn as_slice(&self) -> &[String] {
        &self.0
    }
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
    /// Facts previously admitted on this subscription. Result-to-contributor
    /// admissions are retired with their result member, rather than becoming
    /// a durable general-history grant.
    pub(crate) previous_program_facts: BTreeSet<ProgramFactEntry>,
    /// Declared flat-join source table for each contributor position.
    pub(crate) flat_tuple_source_tables: FlatTupleSourceTables,
    pub(crate) result_member_adds: Vec<ResultMemberEntry>,
    pub(crate) result_member_removes: Vec<ResultMemberEntry>,
    pub(crate) program_fact_adds: Vec<ProgramFactEntry>,
    pub(crate) program_fact_removes: Vec<ProgramFactEntry>,
    pub(crate) identity: AuthorSubject,
    pub(crate) tier: DurabilityTier,
    /// Maintained fact and collector state. The current carrier consumes its
    /// membership/witness facts; the retained recursive app rows are available
    /// here for the structured carrier without changing this boundary.
    pub(crate) maintained_facts: &'a MaintainedSubscriptionView,
    pub(crate) allow_storage_witness_fallback: bool,
}

struct ViewBundlePreflight {
    bundles: BTreeMap<TxId, VersionBundle>,
    persisted_tx_ids: BTreeSet<TxId>,
}

/// Provenance of version rows selected for a maintained-view wire bundle.
/// Only exact immutable-store reads may skip witness canonicalization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaintainedBundleVersionSource {
    IvmWitness,
    ExactStorage,
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Recover the deletion-register winner currently visible at the
    /// subscription tier for an optimized scalar root. Result membership tells
    /// us that the row is visible, but not whether that visibility is due to a
    /// later `Restored` register event; the receiver must learn that event to
    /// keep its own register current for later ordinary reads.
    async fn storage_backed_maintained_deletion_winner(
        &mut self,
        table: &str,
        row_uuid: RowUuid,
        tier: DurabilityTier,
        context: &mut ViewEvaluationContext,
    ) -> Result<Option<VersionRow>, Error> {
        let table_id =
            self.physical_table_id_for_schema(self.catalogue.current_schema_version_id, table)?;
        let global = self
            .visible_global_layer_tx_id_for_physical_table_now(
                table_id,
                row_uuid,
                VersionLayer::Deletion,
            )
            .await;
        let tx_id = match tier {
            // Global current storage already represents the settled winner.
            DurabilityTier::Global => global,
            // Local reads select the greatest global/ahead register winner.
            DurabilityTier::Local => self.local_deletion_winner_tx_id(table, row_uuid).await?,
            // Edge filters ahead candidates before selecting a winner. A
            // later Local register event must not shadow an earlier
            // Edge-accepted event in that filter/argmax ordering.
            DurabilityTier::Edge => {
                self.edge_visible_deletion_winner_tx_id(table_id, table, row_uuid)
                    .await?
            }
            // No-tier reads do not have a settled maintained source.
            DurabilityTier::None => None,
        };
        let Some(tx_id) = tx_id else {
            return Ok(None);
        };
        let stored_tx = self
            .query_transaction_memo(tx_id, context)
            .await?
            .ok_or(Error::MissingTransaction(tx_id))?;
        let wanted_row = BTreeSet::from([(table.to_owned(), row_uuid)]);
        Ok(self
            .query_versions_for_tx_rows_by_alias(tx_id, stored_tx.node_alias, &wanted_row)
            .await?
            .into_iter()
            .find(|version| version.deletion().is_some()))
    }

    /// Mirror the Edge deletion source exactly: filter accepted
    /// Edge-or-Global ahead entries first, then select the winner with the
    /// settled global register.  This cannot be implemented by validating the
    /// raw Local argmax afterwards because that loses an older visible ahead
    /// row when a newer Local-only row shares the current key.
    async fn edge_visible_deletion_winner_tx_id(
        &mut self,
        table_id: PhysicalTableId,
        table: &str,
        row_uuid: RowUuid,
    ) -> Result<Option<TxId>, Error> {
        use groove::ivm::{LiteralValue, StaticScanSpec};

        let scan = StaticScanSpec::Point(vec![
            LiteralValue::from(Value::Bytes(BranchKey::default().canonical_bytes())),
            LiteralValue::from(Value::Uuid(row_uuid.0)),
        ]);
        let fields = ["row_uuid", "tx_time", "tx_node_id"]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let global = GraphBuilder::table_scan(
            physical_register_global_current_table_name(table_id),
            scan.clone(),
        )
        .project(fields.clone());
        let ahead =
            GraphBuilder::table_scan(physical_register_ahead_current_table_name(table_id), scan);
        let edge_ahead = GraphBuilder::join(
            ahead.project(fields.clone()),
            GraphBuilder::table("jazz_transactions")
                .filter(
                    PredicateExpr::And(vec![
                        PredicateExpr::eq("fate", Value::EnumTag(FateTag::Accepted as u8)),
                        PredicateExpr::Or(vec![
                            PredicateExpr::eq("durability", Value::EnumTag(2)),
                            PredicateExpr::eq("durability", Value::EnumTag(3)),
                        ])
                        .canonicalize(),
                    ])
                    .canonicalize(),
                )
                .project(["time", "node_id"]),
            ["tx_time", "tx_node_id"],
            ["time", "node_id"],
        )
        .project_fields(
            fields
                .into_iter()
                .map(|field| ProjectField::renamed(left_field(&field), field)),
        );
        let result = self
            .database
            .query_graph(GraphBuilder::arg_max_by(
                GraphBuilder::union([global, edge_ahead]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            ))
            .await
            .map_err(|error| Self::malformed_current_query_error(table, row_uuid, error))?;
        let Some(delta) = result.deltas.into_iter().find(|delta| delta.weight > 0) else {
            return Ok(None);
        };
        let record = BorrowedRecord::new(&delta.record, &result.descriptor);
        let time = TxTime(record.get_u64(1)?);
        let node_alias = NodeAlias(record.get_u64(2)?);
        let node = self
            .node_for_alias(node_alias)
            .ok_or(Error::InvalidStoredValue(
                "Edge deletion winner references an unknown node alias",
            ))?;
        Ok(Some(TxId::new(time, node)))
    }

    async fn preflight_view_bundle_conflicts(
        &mut self,
        bundles: &[VersionBundleRef<'_>],
    ) -> Result<ViewBundlePreflight, Error> {
        let mut merged = BTreeMap::<TxId, VersionBundle>::new();
        for bundle in bundles {
            merge_receiver_version_bundle_ref(&mut merged, *bundle)?;
        }
        let mut persisted_tx_ids = BTreeSet::new();
        for (tx_id, bundle) in &mut merged {
            let Some(stored) = self.query_transaction(*tx_id).await? else {
                continue;
            };
            persisted_tx_ids.insert(*tx_id);
            let mut stored_identity = stored.tx.clone();
            stored_identity.n_total_writes = 0;
            let mut incoming_identity = bundle.tx.clone();
            incoming_identity.n_total_writes = 0;
            if stored_identity != incoming_identity {
                return Err(Error::ConflictingCommitUnit(*tx_id));
            }
            let stored_versions = self.query_versions_for_tx(*tx_id).await?;
            let mut stored_by_key = BTreeMap::new();
            for stored_version in &stored_versions {
                let version = self.version_record_from_row(stored_version)?;
                stored_by_key.insert(version_bundle_record_key(&version), version);
            }
            let incoming_by_key = bundle
                .versions
                .iter()
                .cloned()
                .map(|version| (version_bundle_record_key(&version), version))
                .collect::<BTreeMap<_, _>>();
            for (key, incoming) in &incoming_by_key {
                if let Some(existing) = stored_by_key.get(key)
                    && existing != incoming
                {
                    return Err(Error::ConflictingCommitUnit(*tx_id));
                }
            }
            use crate::protocol::VersionBundleScope::{CompleteTransaction, ViewScoped};
            match (stored.view_scoped_cardinality, bundle.scope) {
                (false, CompleteTransaction) => {
                    if stored.tx.n_total_writes != bundle.tx.n_total_writes
                        || stored_by_key.len() != incoming_by_key.len()
                        || stored_by_key.keys().ne(incoming_by_key.keys())
                    {
                        return Err(Error::ConflictingCommitUnit(*tx_id));
                    }
                }
                (false, ViewScoped) => {
                    if incoming_by_key
                        .keys()
                        .any(|key| !stored_by_key.contains_key(key))
                    {
                        return Err(Error::ConflictingCommitUnit(*tx_id));
                    }
                }
                (true, CompleteTransaction) => {
                    if stored_by_key
                        .keys()
                        .any(|key| !incoming_by_key.contains_key(key))
                    {
                        return Err(Error::ConflictingCommitUnit(*tx_id));
                    }
                }
                (true, ViewScoped) => {
                    for (key, version) in stored_by_key {
                        if !incoming_by_key.contains_key(&key) {
                            bundle.versions.push(version);
                        }
                    }
                    bundle.versions.sort();
                    bundle.tx.n_total_writes = bundle
                        .versions
                        .len()
                        .try_into()
                        .map_err(|_| Error::InvalidStoredValue("view payload is too large"))?;
                }
            }
        }
        Ok(ViewBundlePreflight {
            bundles: merged,
            persisted_tx_ids,
        })
    }

    /// Subscribe to the raw history storage table.
    pub async fn subscribe_history(&mut self, table: &str) -> Result<Subscription, Error> {
        self.table(table)?;
        let schema_version = self.catalogue.current_schema_version_id;
        let source = self.physical_history_source_graph(schema_version, table)?;
        self.database
            .subscribe_one_sink(source)
            .await
            .map_err(Error::Groove)
    }

    /// Build a current-row view update for a system-identity peer.
    #[cfg(test)]
    pub(crate) async fn view_update_for_current_rows(
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
            AuthorSubject::SYSTEM,
        )
        .await
    }

    /// Build a current-row view update using the peer's payload inventory.
    #[cfg(test)]
    pub(crate) async fn view_update_for_current_rows_with_peer_payload_inventory(
        &mut self,
        table: &str,
        subscription: SubscriptionKey,
        peer_complete_tx_payloads: impl IntoIterator<Item = TxId>,
        previous_result_set: impl IntoIterator<Item = TxId>,
        previous_member_result_set: impl IntoIterator<Item = ResultMemberEntry>,
        identity: AuthorSubject,
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
        .await
    }

    /// Build a query-binding view update using the peer's payload inventory.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn view_update_for_query_binding_with_peer_payload_inventory(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        peer_complete_tx_payloads: impl IntoIterator<Item = TxId>,
        previous_result_set: impl IntoIterator<Item = TxId>,
        previous_member_result_set: impl IntoIterator<Item = ResultMemberEntry>,
        identity: AuthorSubject,
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
        .await
    }

    /// Build a cold maintained query-binding view update using the peer's
    /// payload inventory.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn seeded_maintained_view_update_for_query_binding_with_peer_payload_inventory(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        peer_complete_tx_payloads: impl IntoIterator<Item = TxId>,
        previous_result_set: impl IntoIterator<Item = TxId>,
        previous_member_result_set: impl IntoIterator<Item = ResultMemberEntry>,
        identity: AuthorSubject,
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
        .await
    }

    #[cfg(test)]
    pub(crate) async fn seeded_maintained_view_update_for_query_binding_with_peer_payload_inventory_at_tier(
        &mut self,
        shape: &ValidatedQuery,
        binding: &Binding,
        subscription: SubscriptionKey,
        peer_complete_tx_payloads: impl IntoIterator<Item = TxId>,
        previous_result_set: impl IntoIterator<Item = TxId>,
        previous_member_result_set: impl IntoIterator<Item = ResultMemberEntry>,
        identity: AuthorSubject,
        tier: DurabilityTier,
    ) -> Result<SyncMessage, Error> {
        let peer_complete_tx_payloads = peer_complete_tx_payloads
            .into_iter()
            .collect::<BTreeSet<_>>();
        let previous_result_set = previous_result_set.into_iter().collect::<BTreeSet<_>>();
        let previous_member_result_set = previous_member_result_set
            .into_iter()
            .collect::<BTreeSet<_>>();
        let (receiver, maintained, _terminal_schemas, transitions, tables, _incomplete) = self
            .open_seeded_maintained_subscription_view(
                shape,
                binding,
                identity,
                tier,
                &Default::default(),
            )
            .await?;
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
        let update = self
            .view_update_for_maintained_result_members(MaintainedViewBundleInputs {
                subscription,
                result_member_adds,
                result_member_removes,
                program_fact_adds: transitions.program_fact_adds,
                program_fact_removes: transitions.program_fact_removes,
                peer_complete_tx_payloads,
                known_state: None,
                complete_exclusive_payloads: false,
                previous_result_set,
                previous_program_facts: BTreeSet::new(),
                flat_tuple_source_tables: FlatTupleSourceTables::for_query(shape),
                identity,
                tier,
                maintained_facts: &maintained,
                allow_storage_witness_fallback: false,
            })
            .await;
        self.unsubscribe_groove_subscription(receiver.id());
        update
    }

    pub(crate) async fn view_update_for_maintained_result_members(
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
            mut program_fact_removes,
            previous_program_facts,
            flat_tuple_source_tables,
            identity: _identity,
            tier,
            maintained_facts,
            allow_storage_witness_fallback,
        } = inputs;
        // The storage-backed maintained subset deliberately omits the
        // source-wide terminal witnesses. Its result-member identity is still
        // exact, so Stream B must retrieve that immutable payload from the
        // authoritative node store when shipping a newly admitted member.
        // Keep callers' explicit fallback flag for the legacy cases that
        // already use it.
        let allow_storage_witness_fallback = allow_storage_witness_fallback
            || maintained_facts.uses_storage_backed_result_materialization();
        program_fact_adds.extend(maintained_facts.payload_facts_for_members(&result_member_adds));
        let tuple_source_versions = maintained_facts.tuple_source_versions_for_members(
            &maintained_facts.active_result_members(),
            flat_tuple_source_tables.as_slice(),
        );
        let mut desired_tuple_source_facts = BTreeMap::new();
        for (result, source_index, maintained_version) in &tuple_source_versions {
            let canonical = self
                .canonical_history_version_for_maintained_witness(&maintained_version)
                .await?;
            let tx = self.version_tx_id(&canonical)?;
            let schema_version = self
                .schema_version_for_alias(canonical.schema_version_alias())
                .ok_or(Error::InvalidStoredValue(
                    "flat tuple source witness schema version alias must exist",
                ))?;
            let mut contributor = RealRowMemberEntry::current_content((
                canonical.table.clone(),
                canonical.row_uuid(),
                tx,
            ));
            contributor.schema_version = Some(schema_version);
            let fact = ProgramFactEntry::ContributingMembers(ContributingMembersEntry {
                result: result.clone(),
                contributor: contributor.into(),
                batch: Some(tx),
                role: Some(format!("flat_tuple_source:{source_index}")),
            });
            desired_tuple_source_facts.insert(fact, maintained_version.clone());
        }
        let previous_tuple_source_facts = previous_program_facts
            .into_iter()
            .filter(|fact| {
                matches!(
                    fact,
                    ProgramFactEntry::ContributingMembers(contribution)
                        if contribution
                            .role
                            .as_deref()
                            .is_some_and(|role| role.starts_with("flat_tuple_source:"))
                )
            })
            .collect::<BTreeSet<_>>();
        let tuple_source_bundle_rows = desired_tuple_source_facts
            .iter()
            .filter(|(fact, _)| !previous_tuple_source_facts.contains(*fact))
            .map(|(_, version)| {
                Ok((
                    version.table().to_owned(),
                    version.row_uuid(),
                    self.version_tx_id(version)?,
                ))
            })
            .collect::<Result<BTreeSet<_>, Error>>()?;
        program_fact_adds.extend(
            desired_tuple_source_facts
                .keys()
                .filter(|fact| !previous_tuple_source_facts.contains(*fact))
                .cloned(),
        );
        program_fact_removes.extend(
            previous_tuple_source_facts
                .into_iter()
                .filter(|fact| !desired_tuple_source_facts.contains_key(fact)),
        );
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
            Some(
                KnownStateDeclaration::Fast { position, .. }
                | KnownStateDeclaration::FastWithAuthorizationProgress { position, .. },
            ) => Some(*position),
            Some(KnownStateDeclaration::ExactVersionSet { .. }) | None => None,
        };
        let known_state_exact_refs = match &known_state {
            Some(KnownStateDeclaration::ExactVersionSet { versions }) => {
                versions.iter().cloned().collect::<BTreeSet<_>>()
            }
            Some(
                KnownStateDeclaration::Fast { .. }
                | KnownStateDeclaration::FastWithAuthorizationProgress { .. },
            )
            | None => BTreeSet::new(),
        };
        // Exact-known declarations name canonical authored versions, while the
        // maintained source index is keyed by its read-schema table label.
        // Translate that declaration back to the source label used to select
        // bundle bodies (e.g. canonical `users` -> maintained `people`).
        let mut known_tuple_source_bundle_rows = BTreeSet::new();
        for (_, _, maintained_version) in &tuple_source_versions {
            let canonical = self
                .canonical_history_version_for_maintained_witness(maintained_version)
                .await?;
            let tx = self.version_tx_id(&canonical)?;
            if known_state_exact_refs.contains(&RowVersionRef::new(
                canonical.table().to_owned(),
                canonical.row_uuid(),
                tx,
            )) {
                known_tuple_source_bundle_rows.insert((
                    maintained_version.table().to_owned(),
                    maintained_version.row_uuid(),
                ));
            }
        }
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
            .chain(known_tuple_source_bundle_rows)
            .collect::<BTreeSet<_>>();
        let relation_edge_add_rows = relation_edge_version_rows_for_bundle(&program_fact_adds);
        let wanted_add_rows_by_tx = row_result_adds
            .iter()
            .map(|(table, row_uuid, tx_id)| (table.to_string(), *row_uuid, *tx_id))
            .chain(relation_edge_add_rows)
            // The contributor fact names canonical authored history; the
            // maintained index is keyed by the source graph's read-schema
            // label. Ship that exact retained witness and normalize it to
            // canonical history only at the wire boundary below.
            .chain(tuple_source_bundle_rows)
            .fold(
                BTreeMap::<TxId, BTreeSet<(String, RowUuid)>>::new(),
                |mut by_tx, (table, row_uuid, tx_id)| {
                    by_tx.entry(tx_id).or_default().insert((table, row_uuid));
                    by_tx
                },
            );
        self.preload_transaction_memo(wanted_add_rows_by_tx.keys().copied(), &mut context)
            .await?;
        let mut version_bundles = Vec::with_capacity(row_result_adds.len());
        let mut peer_payload_inventory_refs = Vec::new();
        let mut emitted_versions = BTreeSet::new();
        // A current result member names its content version, but its visible
        // existence can also depend on a deletion-register `Restored` event.
        // Remember the winner while assembling its content bundle so that a
        // content and restore event committed in the *same* transaction are
        // shipped together.  The later replacement pass only emits a
        // different transaction; once this transaction is marked emitted it
        // cannot repair a missing same-transaction register witness.
        let mut result_add_deletion_winners =
            BTreeMap::<(String, RowUuid), Option<VersionRow>>::new();
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
                    .query_transaction_memo(*tx_id, &mut context)
                    .await?
                    .ok_or(Error::MissingTransaction(*tx_id))?;
                let fallback_versions =
                    if complete_exclusive_payloads && stored_tx.tx.kind == TxKind::Exclusive {
                        self.query_versions_for_tx(*tx_id).await?
                    } else {
                        self.query_versions_for_tx_rows_by_alias(
                            *tx_id,
                            stored_tx.node_alias,
                            wanted_rows,
                        )
                        .await?
                    };
                tx_versions_cache.insert(*tx_id, fallback_versions);
            }
            let mut same_transaction_deletion_winners = Vec::new();
            for (entry_table, row_uuid) in wanted_rows {
                if !row_result_adds
                    .iter()
                    .any(|(table, result_row_uuid, content_tx_id)| {
                        table.as_str() == entry_table
                            && result_row_uuid == row_uuid
                            && content_tx_id == tx_id
                    })
                {
                    continue;
                }
                let winner_key = (entry_table.clone(), *row_uuid);
                let deletion_winner =
                    if let Some(winner) = result_add_deletion_winners.get(&winner_key) {
                        winner.clone()
                    } else {
                        let (_, retained_deletion_winner) =
                            maintained_facts.replacement_for(entry_table, *row_uuid);
                        let winner = match retained_deletion_winner {
                            Some(winner) => Some(winner),
                            None if allow_storage_witness_fallback => {
                                self.storage_backed_maintained_deletion_winner(
                                    entry_table,
                                    *row_uuid,
                                    tier,
                                    &mut context,
                                )
                                .await?
                            }
                            None => None,
                        };
                        result_add_deletion_winners.insert(winner_key, winner.clone());
                        winner
                    };
                if let Some(winner) = deletion_winner
                    && self.version_tx_id(&winner)? == *tx_id
                {
                    same_transaction_deletion_winners.push(winner);
                }
            }
            let tx_versions = tx_versions_cache
                .get_mut(tx_id)
                .expect("tx versions cache entry must exist after fallback");
            for winner in same_transaction_deletion_winners {
                if !maintained_view_tx_versions_contain_winner(tx_versions, &winner) {
                    tx_versions.push(winner);
                }
            }
            if tx_versions.iter().any(|version| {
                version.deletion().is_none()
                    && wanted_rows.contains(&(version.table().to_owned(), version.row_uuid()))
            }) {
                let stored_tx = self
                    .query_transaction_memo(*tx_id, &mut context)
                    .await?
                    .ok_or(Error::MissingTransaction(*tx_id))?;
                // A trusted writer link that explicitly requests complete
                // exclusive payloads must receive the whole cross-table
                // transaction, not one permanently redacted fragment per
                // table subscription. Identity-scoped links remain bounded
                // by their maintained policy witnesses even when the peer
                // preference is enabled.
                if complete_exclusive_payloads
                    && stored_tx.tx.kind == TxKind::Exclusive
                    && usize::try_from(stored_tx.tx.n_total_writes).ok() != Some(tx_versions.len())
                {
                    *tx_versions = self.query_versions_for_tx(*tx_id).await?;
                }
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
                // Storage-backed result membership loaded these exact
                // immutable history records by `(table, row, tx)` above. Do
                // not immediately reload them merely to canonicalize an IVM
                // witness: unlike a graph-projected witness, these are
                // already authored history rows. Keeping that distinction
                // here preserves the cold current-row O(1) read receipt.
                let bundle = if maintained_facts.uses_storage_backed_result_materialization()
                    && needs_storage_fallback
                    && self
                        .exact_storage_maintained_versions_are_unambiguous(&filtered_tx_versions)?
                {
                    self.version_bundle_for_exact_storage_maintained_view_versions_with_tx(
                        &stored_tx,
                        &filtered_tx_versions,
                    )
                    .await?
                } else {
                    self.version_bundle_for_maintained_view_versions_with_tx(
                        &stored_tx,
                        &filtered_tx_versions,
                    )
                    .await?
                };
                version_bundles.push(bundle);
                record_maintained_view_stream_b_add_bundle();
                continue;
            }

            return Err(Error::MaintainedViewMissingBundleWitness(
                "add result row missing Stream B content witness",
            ));
        }
        let mut replacement_winners_by_tx =
            BTreeMap::<TxId, Vec<(String, RowUuid, VersionRow, &'static str)>>::new();
        for (entry_table, row_uuid, content_tx_id) in &row_result_adds {
            let deletion_winner = if let Some(winner) =
                result_add_deletion_winners.get(&(entry_table.as_str().to_owned(), *row_uuid))
            {
                winner.clone()
            } else {
                let (_, retained_deletion_winner) =
                    maintained_facts.replacement_for(entry_table, *row_uuid);
                match retained_deletion_winner {
                    Some(winner) => Some(winner),
                    None if allow_storage_witness_fallback => {
                        self.storage_backed_maintained_deletion_winner(
                            entry_table,
                            *row_uuid,
                            tier,
                            &mut context,
                        )
                        .await?
                    }
                    None => None,
                }
            };
            let Some(version) = deletion_winner.as_ref() else {
                continue;
            };
            let tx_id = self.version_tx_id(version)?;
            if tx_id == *content_tx_id || emitted_versions.contains(&tx_id) {
                continue;
            }
            replacement_winners_by_tx.entry(tx_id).or_default().push((
                entry_table.to_string(),
                *row_uuid,
                version.clone(),
                "add result row missing deletion replacement witness",
            ));
        }
        for (entry_table, row_uuid, old_tx_id) in &row_result_removes {
            let (content_winner, retained_deletion_winner) =
                maintained_facts.replacement_for(entry_table, *row_uuid);
            let deletion_winner = match retained_deletion_winner {
                Some(winner) => Some(winner),
                None if allow_storage_witness_fallback => {
                    self.storage_backed_maintained_deletion_winner(
                        entry_table,
                        *row_uuid,
                        tier,
                        &mut context,
                    )
                    .await?
                }
                None => None,
            };
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
                replacement_winners_by_tx.entry(tx_id).or_default().push((
                    entry_table.to_string(),
                    *row_uuid,
                    version.clone(),
                    missing_witness,
                ));
            }
        }
        for (tx_id, winners) in replacement_winners_by_tx {
            if emitted_versions.contains(&tx_id) {
                continue;
            }
            if peer_complete_tx_payloads.contains(&tx_id) {
                emitted_versions.insert(tx_id);
                peer_payload_inventory_refs.push(tx_id);
                record_maintained_view_removal_stream_bundle();
                continue;
            }
            let wanted_rows = winners
                .iter()
                .map(|(table, row_uuid, _, _)| (table.clone(), *row_uuid))
                .collect::<BTreeSet<_>>();
            let tx_versions = tx_versions_cache
                .entry(tx_id)
                .or_insert_with(|| maintained_facts.versions_by_tx(tx_id));
            if winners.iter().any(|(_, _, winner, _)| {
                !maintained_view_tx_versions_contain_winner(tx_versions, winner)
            }) {
                if !allow_storage_witness_fallback {
                    let (_, _, _, missing_witness) = winners
                        .iter()
                        .find(|(_, _, winner, _)| {
                            !maintained_view_tx_versions_contain_winner(tx_versions, winner)
                        })
                        .expect("missing maintained witness must be present");
                    return Err(Error::MaintainedViewMissingBundleWitness(missing_witness));
                }
                let stored_tx = self
                    .query_transaction_memo(tx_id, &mut context)
                    .await?
                    .ok_or(Error::MissingTransaction(tx_id))?;
                let fallback_versions =
                    if complete_exclusive_payloads && stored_tx.tx.kind == TxKind::Exclusive {
                        self.query_versions_for_tx(tx_id).await?
                    } else {
                        self.query_versions_for_tx_rows_by_alias(
                            tx_id,
                            stored_tx.node_alias,
                            &wanted_rows,
                        )
                        .await?
                    };
                tx_versions_cache.insert(tx_id, fallback_versions);
            }
            let stored_tx = self
                .query_transaction_memo(tx_id, &mut context)
                .await?
                .ok_or(Error::MissingTransaction(tx_id))?;
            if complete_exclusive_payloads
                && stored_tx.tx.kind == TxKind::Exclusive
                && usize::try_from(stored_tx.tx.n_total_writes).ok()
                    != tx_versions_cache.get(&tx_id).map(Vec::len)
            {
                tx_versions_cache.insert(tx_id, self.query_versions_for_tx(tx_id).await?);
            }
            let tx_versions = tx_versions_cache
                .get(&tx_id)
                .expect("tx versions cache entry must exist after fallback");
            if let Some((_, _, _, missing_witness)) = winners.iter().find(|(_, _, winner, _)| {
                !maintained_view_tx_versions_contain_winner(tx_versions, winner)
            }) {
                return Err(Error::MaintainedViewMissingBundleWitness(missing_witness));
            }
            emitted_versions.insert(tx_id);
            let bundled_versions =
                if complete_exclusive_payloads && stored_tx.tx.kind == TxKind::Exclusive {
                    tx_versions.clone()
                } else {
                    tx_versions
                        .iter()
                        .filter(|version| {
                            wanted_rows.contains(&(version.table().to_owned(), version.row_uuid()))
                        })
                        .cloned()
                        .collect()
                };
            version_bundles.push(
                self.version_bundle_for_maintained_view_versions_with_tx(
                    &stored_tx,
                    &bundled_versions,
                )
                .await?,
            );
            record_maintained_view_removal_stream_bundle();
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
            bundle.tx.n_total_writes = bundle
                .versions
                .len()
                .try_into()
                .map_err(|_| Error::InvalidStoredValue("view payload is too large"))?;
            bundle.scope = crate::protocol::VersionBundleScope::ViewScoped;
        }
        let version_carriers = build_version_carriers_from_singletons(version_bundles)
            .map_err(|_| Error::InvalidStoredValue("failed to build version-bundle run"))?;
        Ok(SyncMessage::ViewUpdate(
            crate::protocol::ViewUpdatePayload {
                subscription,
                settled_through: self.clock.committed_global_time,
                reset_result_set: false,
                version_carriers,
                peer_payload_inventory: PeerPayloadInventory {
                    complete_tx_payloads: peer_payload_inventory_refs,
                    authorization_progress: None,
                    opening_pending: false,
                },
                result_member_adds: result_member_adds.into_iter().collect(),
                result_member_removes: result_member_removes.into_iter().collect(),
                terminal_operations: Vec::new(),
                program_fact_adds,
                program_fact_removes,
            },
        ))
    }

    /// Apply a downstream current-row view update.
    pub(super) async fn apply_view_update(&mut self, update: ViewUpdateParts) -> Result<(), Error> {
        self.validate_received_view_update_global_time_durability(&update)?;
        self.validate_view_update_payloads(std::slice::from_ref(&update))?;
        self.apply_view_update_inner(update, None).await
    }

    pub(crate) async fn apply_view_updates_in_batch(
        &mut self,
        updates: Vec<ViewUpdateParts>,
    ) -> Result<(), Error> {
        if updates.is_empty() {
            return Ok(());
        }
        for update in &updates {
            self.validate_received_view_update_global_time_durability(update)?;
        }
        // A receiver tick is one atomic protocol frame. Validate every row
        // descriptor before the first reset can change flush cadence, or a
        // preceding valid bundle can advance clocks, allocate aliases, or
        // stage history before a later malformed bundle rejects the frame.
        self.validate_view_update_payloads(&updates)?;
        let mut all_bundle_refs = Vec::new();
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
            let version_bundle_refs = version_bundle_refs_for_carriers(&update.version_carriers)?;
            all_bundle_refs.extend(version_bundle_refs.iter().copied());
            let in_initial_hydration = initial_hydration_binding_views.contains(&binding_view_key);
            if update.reset_result_set
                && update.peer_complete_tx_payload_refs.is_empty()
                && update.result_member_removes.is_empty()
            {
                bulk_candidates.extend(version_bundle_refs.iter().copied());
            }
            if in_initial_hydration
                && version_bundle_refs.is_empty()
                && (!update.reset_result_set || update.peer_complete_tx_payload_refs.is_empty())
            {
                initial_hydration_binding_views.remove(&binding_view_key);
            }
        }
        let preflight = self
            .preflight_view_bundle_conflicts(&all_bundle_refs)
            .await?;
        let bulk_candidate_tx_ids = bulk_candidates
            .iter()
            .map(|bundle| bundle.tx.tx_id)
            .collect::<BTreeSet<_>>();
        let bulk_candidate_bundles = preflight
            .bundles
            .iter()
            .filter(|(tx_id, bundle)| {
                bulk_candidate_tx_ids.contains(tx_id)
                    && bundle.scope == crate::protocol::VersionBundleScope::CompleteTransaction
            })
            .map(|(_, bundle)| bundle.clone())
            .collect::<Vec<_>>();
        let bulk_candidate_refs = bulk_candidate_bundles
            .iter()
            .map(VersionBundle::as_ref)
            .collect::<Vec<_>>();
        let bulk_loaded_tx_ids = self
            .ingest_reset_view_bundle_refs_in_bulk(
                &bulk_candidate_refs,
                Some(&preflight.persisted_tx_ids),
            )
            .await?;
        let mut receiver_candidates = preflight.bundles;
        if updates.iter().any(|update| update.reset_result_set) {
            self.begin_initial_sync_flush_cadence().await?;
        }
        for tx_id in &bulk_loaded_tx_ids {
            receiver_candidates.remove(tx_id);
        }
        let mut receiver_batch = self.database.open_batch();
        let mut receiver_batch_tx_ids = BTreeSet::new();
        let mut receiver_batch_global_times = Vec::new();
        let mut receiver_batch_content_versions = Vec::new();
        let mut receiver_batch_bundle_count = 0u64;
        for bundle in receiver_candidates.values() {
            let staged = self
                .stage_view_bundle(
                    &mut receiver_batch,
                    bundle,
                    &mut receiver_batch_tx_ids,
                    &mut receiver_batch_global_times,
                    &mut receiver_batch_content_versions,
                )
                .await?;
            if staged {
                receiver_batch_bundle_count += 1;
            }
        }
        self.write_merge_heads_for_bulk_content_versions(
            &mut receiver_batch,
            &receiver_batch_content_versions,
        )
        .await?;
        if !receiver_batch.is_empty() {
            self.sync_metrics.receiver_bulk_ingest_commits += 1;
            self.sync_metrics.receiver_bulk_bundle_ingests += receiver_batch_bundle_count;
            let applied = self.database.apply_batch(receiver_batch).await?;
            let persisted = applied.persist().await;
            self.database.finish_persistence(persisted)?;
            for tx_id in &receiver_batch_tx_ids {
                self.invalidate_tx_version_tables_cache(*tx_id);
            }
            for global_time in receiver_batch_global_times {
                self.record_applied_global_time(global_time);
            }
            self.settle_completed_parent_batch(&receiver_batch_tx_ids)
                .await?;
            if let Some(tx_time) = receiver_batch_tx_ids.iter().map(|tx_id| tx_id.time).max() {
                self.persist_storage_consistency_marker_through(tx_time)
                    .await?;
            }
        }
        let mut preloaded_tx_ids = bulk_loaded_tx_ids;
        preloaded_tx_ids.extend(receiver_batch_tx_ids);
        // Cross-subscription ordering within one receiver tick carries no
        // protocol semantics beyond per-link FIFO. Table writes are coalesced
        // above; per-subscription settled-state mutations still apply in
        // arrival order below.
        for update in updates {
            self.apply_view_update_inner(update, Some(&preloaded_tx_ids))
                .await?;
        }
        if self.initial_sync_flush_active && self.query.initial_hydration_binding_views.is_empty() {
            self.finish_initial_sync_flush_cadence().await?;
        }
        Ok(())
    }

    /// Validate all row bundles carried by a receiver frame without changing
    /// storage or in-memory receiver state.
    fn validate_view_update_payloads(&self, updates: &[ViewUpdateParts]) -> Result<(), Error> {
        for update in updates {
            for bundle in version_bundle_refs_for_carriers(&update.version_carriers)? {
                // This preflight runs before bulk-reset selection, alias
                // allocation, clock advancement, or receiver staging. The
                // shared transaction boundary keeps view payloads from being
                // a durable-ingress bypass for operation provenance.
                self.admit_contribution_merge_for_storage(bundle.tx)?;
                self.validate_view_payload_versions(bundle.versions)?;
            }
        }
        Ok(())
    }

    async fn apply_view_update_inner(
        &mut self,
        update: ViewUpdateParts,
        preloaded_tx_ids: Option<&BTreeSet<TxId>>,
    ) -> Result<(), Error> {
        let ViewUpdateParts {
            subscription,
            settled_through,
            defer_settlement,
            reset_result_set,
            version_carriers,
            peer_complete_tx_payload_refs,
            authorization_progress,
            opening_pending,
            result_member_adds,
            result_member_removes,
            terminal_operations,
            program_fact_adds,
            program_fact_removes,
        } = update;
        let synthetic_result_changed = result_member_adds
            .iter()
            .chain(&result_member_removes)
            .any(|member| matches!(member, ResultMemberEntry::Synthetic { .. }))
            || program_fact_adds
                .iter()
                .chain(&program_fact_removes)
                .any(|fact| {
                    matches!(
                        fact,
                        ProgramFactEntry::ResultPayload(payload)
                            if matches!(payload.member, ResultMemberEntry::Synthetic { .. })
                    )
                });
        let version_bundle_refs = version_bundle_refs_for_carriers(&version_carriers)?;
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
        let preflight = if preloaded_tx_ids.is_none() {
            Some(
                self.preflight_view_bundle_conflicts(&version_bundle_refs)
                    .await?,
            )
        } else {
            None
        };
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
            let preflight = preflight.as_ref().expect("direct update was preflighted");
            let complete_bundles = preflight
                .bundles
                .values()
                .filter(|bundle| {
                    bundle.scope == crate::protocol::VersionBundleScope::CompleteTransaction
                })
                .cloned()
                .collect::<Vec<_>>();
            let complete_refs = complete_bundles
                .iter()
                .map(VersionBundle::as_ref)
                .collect::<Vec<_>>();
            self.ingest_reset_view_bundle_refs_in_bulk(
                &complete_refs,
                Some(&preflight.persisted_tx_ids),
            )
            .await?
        } else {
            BTreeSet::new()
        };
        if reset_result_set {
            self.query
                .pending_terminal_operations_by_binding_view
                .remove(&binding_view_key);
            self.query
                .initial_hydration_binding_views
                .insert(binding_view_key);
        }
        if !terminal_operations.is_empty() {
            self.query
                .pending_terminal_operations_by_binding_view
                .entry(binding_view_key)
                .or_default()
                .extend(terminal_operations);
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
        let row_result_adds = result_member_adds
            .iter()
            .filter_map(ResultMemberEntry::as_row)
            .collect::<Vec<_>>();
        let version_bundles_is_empty = version_bundle_refs.is_empty();
        if let Some(preflight) = &preflight {
            for bundle in preflight.bundles.values() {
                if bulk_loaded_tx_ids.contains(&bundle.tx.tx_id) {
                    continue;
                }
                self.sync_metrics.receiver_per_bundle_ingests += 1;
                self.ingest_view_bundle(bundle.clone()).await?;
            }
        }
        let mut available_peer_complete_tx_payload_refs = Vec::new();
        for tx_id in peer_complete_tx_payload_refs.iter() {
            if bulk_loaded_tx_ids.contains(tx_id) {
                available_peer_complete_tx_payload_refs.push(*tx_id);
                continue;
            }
            if self.query_transaction(*tx_id).await?.is_none() {
                self.record_peer_payload_inventory_missing_fallback();
                continue;
            }
            available_peer_complete_tx_payload_refs.push(*tx_id);
        }
        for tx_id in row_result_adds.iter().map(|(_, _, tx_id)| tx_id) {
            if bulk_loaded_tx_ids.contains(tx_id) {
                continue;
            }
            if self.query_transaction(*tx_id).await?.is_none() {
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
        )
        .await?;
        let empty_reset = reset_result_set
            && version_bundles_is_empty
            && peer_complete_tx_payload_refs.is_empty()
            && result_member_adds.is_empty()
            && result_member_removes.is_empty()
            && program_fact_adds.is_empty()
            && program_fact_removes.is_empty();
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
                if let Some(occurrence_id) = member.output_occurrence_id()
                    && self
                        .remove_settled_result_member_for_occurrence_indexed(
                            binding_view_key,
                            occurrence_id,
                        )
                        .is_some()
                {
                    result_members_need_rewrite = true;
                }
            }
            for member in result_member_adds {
                if let Some(occurrence_id) = member.output_occurrence_id() {
                    result_members_need_rewrite |= self
                        .remove_settled_result_member_for_occurrence_indexed(
                            binding_view_key,
                            occurrence_id,
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
        if synthetic_result_changed
            && self
                .query
                .initial_hydration_binding_views
                .contains(&binding_view_key)
        {
            self.query
                .pending_authoritative_reset_binding_views
                .insert(binding_view_key);
        }
        if !defer_settlement {
            self.query
                .settled_through_by_binding_view
                .insert(binding_view_key, settled_through);
            // A reset is an authoritative membership rebuild, including when
            // it carries retractions. The public subscription materializes its
            // replacement snapshot below, rather than attempting to apply a
            // removal after the reset has cleared the cached result set.
            if reset_result_set && !preserve_existing_shared_state {
                self.query
                    .pending_authoritative_reset_binding_views
                    .insert(binding_view_key);
            }
        }
        // Diagnostic-only: the duplicate-content-version scan feeds a
        // debug_assert, so it is wasted work in release. Gate to debug builds.
        #[cfg(debug_assertions)]
        {
            if let Some((occurrence_id, first, second)) = self
                .query
                .settled_result_sets
                .get(&binding_view_key)
                .and_then(duplicate_output_occurrence_result_set)
            {
                debug_assert!(
                    first == second,
                    "settled binding view {binding_view_key:?} has multiple content versions for output occurrence {occurrence_id:?}: {first:?} and {second:?}"
                );
            }
        }
        if !defer_settlement {
            let authority_result_key = self.authority_result_key_for_subscription(subscription)?;
            self.persist_settled_result_state_delta_for_authority_result(
                authority_result_key.clone(),
                reset_cleared_shared_state,
                &persisted_member_adds,
                &persisted_member_removes,
                member_rewrite.as_ref(),
                &persisted_fact_adds,
                &persisted_fact_removes,
                fact_rewrite.as_ref(),
            )
            .await?;
            self.persist_known_state_fact_for_authority_result(
                authority_result_key,
                settled_through,
            )
            .await?;
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
        if let Some(progress) = authorization_progress {
            self.query
                .authorization_progress_by_binding_view
                .insert(binding_view_key, progress);
        }
        if opening_pending {
            self.query
                .pending_opening_binding_views
                .insert(binding_view_key);
        } else {
            self.query
                .pending_opening_binding_views
                .remove(&binding_view_key);
        }
        let generation = self
            .query
            .applied_view_update_generations
            .entry(binding_view_key)
            .or_default();
        *generation = generation.wrapping_add(1);
        Ok(())
    }

    async fn validate_result_member_adds_are_witnessed(
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
            let Some(tx) = self.query_transaction(*tx_id).await? else {
                continue;
            };
            if tx.tx.kind != TxKind::Exclusive || peer_complete_tx_payload_refs.contains(tx_id) {
                continue;
            }
            let keys = match partial_exclusive_keys.entry(*tx_id) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let keys = self
                        .query_versions_for_tx(*tx_id)
                        .await?
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

    async fn ingest_view_bundle(&mut self, bundle: VersionBundle) -> Result<(), Error> {
        validate_received_view_bundle_global_time_durability(
            bundle.global_time,
            bundle.durability,
        )?;
        if usize::try_from(bundle.tx.n_total_writes).ok() != Some(bundle.versions.len()) {
            return Err(Error::MalformedViewUpdate(
                "version bundle count does not match its declared scope payload",
            ));
        }
        if let Some(stored) = self.query_transaction(bundle.tx.tx_id).await? {
            let mut stored_identity = stored.tx;
            stored_identity.n_total_writes = 0;
            let mut incoming_identity = bundle.tx.clone();
            incoming_identity.n_total_writes = 0;
            if stored_identity != incoming_identity {
                return Err(Error::ConflictingCommitUnit(bundle.tx.tx_id));
            }
        }
        if bundle.tx.kind != TxKind::Exclusive {
            if bundle.scope == crate::protocol::VersionBundleScope::ViewScoped {
                return self
                    .ingest_view_scoped_transaction_with_current_indexes(
                        bundle.tx,
                        bundle.versions,
                        bundle.fate,
                        bundle.global_time,
                        bundle.durability,
                    )
                    .await;
            }
            return self
                .ingest_known_transaction(
                    bundle.tx,
                    bundle.versions,
                    bundle.fate,
                    bundle.global_time,
                    bundle.durability,
                )
                .await;
        }
        if bundle.scope == crate::protocol::VersionBundleScope::ViewScoped {
            let tx_id = bundle.tx.tx_id;
            let stored_tx = self.query_transaction(tx_id).await?;
            // A bulk reset installs its authorized fragment as locally current
            // so a relay can serve it onward. Further authorized siblings from
            // that same view-scoped transaction must extend that projection;
            // fragments first learned outside such a reset remain history-only.
            let extend_current_view = stored_tx
                .as_ref()
                .is_some_and(|stored| stored.view_scoped_cardinality);
            let mut known_keys = if stored_tx.is_some() {
                self.query_versions_for_tx(tx_id)
                    .await?
                    .iter()
                    .map(|stored| Ok(view_version_key(&self.version_record_from_row(stored)?)))
                    .collect::<Result<BTreeSet<_>, Error>>()?
            } else {
                BTreeSet::new()
            };
            known_keys.extend(bundle.versions.iter().map(view_version_key));
            let mut redacted_tx = bundle.tx;
            redacted_tx.n_total_writes = known_keys
                .len()
                .try_into()
                .map_err(|_| Error::InvalidStoredValue("view payload is too large"))?;
            return if extend_current_view {
                self.ingest_view_scoped_transaction_with_current_indexes(
                    redacted_tx,
                    bundle.versions,
                    bundle.fate,
                    bundle.global_time,
                    bundle.durability,
                )
                .await
            } else {
                self.ingest_transaction_fragment_without_current_indexes(
                    redacted_tx,
                    bundle.versions,
                    bundle.fate,
                    bundle.global_time,
                    bundle.durability,
                )
                .await
            };
        }
        let complete_len = usize::try_from(bundle.tx.n_total_writes).map_err(|_| {
            Error::InvalidStoredValue("exclusive transaction write count does not fit usize")
        })?;
        let tx_id = bundle.tx.tx_id;
        let mut stored_versions = if self.query_transaction(tx_id).await?.is_some() {
            self.query_versions_for_tx(tx_id)
                .await?
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
                bundle.global_time,
                bundle.durability,
            )
            .await?;
            if matches!(bundle.fate, Fate::Accepted) {
                // Ingesting only the previously missing versions replaces the
                // transaction-version cache with that subset. Reload the full
                // assembled transaction before applying its fate so every
                // earlier fragment receives a current index.
                self.invalidate_tx_version_tables_cache(tx_id);
                self.apply_fate_update(
                    tx_id,
                    bundle.fate,
                    bundle.global_time,
                    Some(bundle.durability),
                )
                .await?;
            }
            return Ok(());
        }
        self.ingest_transaction_fragment_without_current_indexes(
            bundle.tx,
            bundle.versions,
            bundle.fate,
            bundle.global_time,
            bundle.durability,
        )
        .await
    }

    async fn stage_view_bundle(
        &mut self,
        batch: &mut DatabaseBatch,
        bundle: &VersionBundle,
        staged_tx_ids: &mut BTreeSet<TxId>,
        staged_global_times: &mut Vec<GlobalTime>,
        staged_content_versions: &mut Vec<VersionRow>,
    ) -> Result<bool, Error> {
        validate_received_view_bundle_global_time_durability(
            bundle.global_time,
            bundle.durability,
        )?;
        if usize::try_from(bundle.tx.n_total_writes).ok() != Some(bundle.versions.len()) {
            return Err(Error::MalformedViewUpdate(
                "version bundle count does not match its declared scope payload",
            ));
        }
        if bundle.scope == crate::protocol::VersionBundleScope::ViewScoped {
            // A view-scoped exclusive fragment may extend a projection already
            // installed by an earlier reset. Its redacted cardinality is
            // deliberately not a whole-transaction completeness claim, so do
            // not take the complete-exclusive fast path below merely because
            // the transaction itself is already known.
            if !staged_tx_ids.insert(bundle.tx.tx_id) {
                return Ok(true);
            }
            self.stage_view_scoped_transaction_with_current_indexes(
                batch,
                bundle.tx.clone(),
                bundle.versions.clone(),
                bundle.fate.clone(),
                bundle.global_time,
                bundle.durability,
                staged_global_times,
                staged_content_versions,
            )
            .await?;
            return Ok(true);
        }
        if bundle.tx.kind == TxKind::Exclusive {
            let complete_len = usize::try_from(bundle.tx.n_total_writes).map_err(|_| {
                Error::InvalidStoredValue("exclusive transaction write count does not fit usize")
            })?;
            if bundle.versions.len() != complete_len {
                return Ok(false);
            }
            if self.query_transaction(bundle.tx.tx_id).await?.is_some() {
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
            bundle.global_time,
            bundle.durability,
            staged_global_times,
            staged_content_versions,
        )
        .await?;
        Ok(true)
    }

    fn validate_received_view_update_global_time_durability(
        &self,
        update: &ViewUpdateParts,
    ) -> Result<(), Error> {
        for bundle in version_bundle_refs_for_carriers(&update.version_carriers)? {
            validate_received_view_bundle_global_time_durability(
                bundle.global_time,
                bundle.durability,
            )?;
        }
        Ok(())
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

    pub(super) async fn version_bundle_for_maintained_view_versions_with_tx(
        &mut self,
        stored_tx: &StoredTransaction,
        tx_versions: &[VersionRow],
    ) -> Result<VersionBundle, Error> {
        self.version_bundle_for_maintained_view_versions_with_tx_and_source(
            stored_tx,
            tx_versions,
            MaintainedBundleVersionSource::IvmWitness,
        )
        .await
    }

    /// Build a maintained-view bundle from rows that were loaded directly
    /// from the immutable history store by their exact storage identities.
    ///
    /// This is intentionally narrower than the ordinary maintained-witness
    /// path: graph-projected versions still need canonicalization before
    /// serialization. Callers may use this only for the result of an exact
    /// `query_versions_for_tx_rows_by_alias` lookup, never for an IVM
    /// witness.
    async fn version_bundle_for_exact_storage_maintained_view_versions_with_tx(
        &mut self,
        stored_tx: &StoredTransaction,
        tx_versions: &[VersionRow],
    ) -> Result<VersionBundle, Error> {
        self.version_bundle_for_maintained_view_versions_with_tx_and_source(
            stored_tx,
            tx_versions,
            MaintainedBundleVersionSource::ExactStorage,
        )
        .await
    }

    /// Whether these direct immutable-store reads can be serialized without
    /// the normal maintained-witness canonicalization. Logical names can be
    /// reused by different physical tables across schema history, so an exact
    /// `(table, row, tx)` lookup alone is not enough: every row must resolve
    /// to the sole physical table ever named by that logical label. Otherwise
    /// use the ordinary fail-closed canonicalization path.
    fn exact_storage_maintained_versions_are_unambiguous(
        &self,
        versions: &[VersionRow],
    ) -> Result<bool, Error> {
        versions.iter().try_fold(true, |all_unambiguous, version| {
            let version_table_id = self.physical_table_id_for_version(version)?;
            let table_ids = self
                .catalogue
                .physical_mappings
                .values()
                .filter_map(|mapping| mapping.tables.get(version.table()))
                .map(|mapping| mapping.table_id)
                .collect::<BTreeSet<_>>();
            Ok(all_unambiguous && table_ids.len() == 1 && table_ids.contains(&version_table_id))
        })
    }

    async fn version_bundle_for_maintained_view_versions_with_tx_and_source(
        &mut self,
        stored_tx: &StoredTransaction,
        tx_versions: &[VersionRow],
        source: MaintainedBundleVersionSource,
    ) -> Result<VersionBundle, Error> {
        let Transaction {
            tx_id,
            kind,
            n_total_writes,
            made_by,
            permission_subject,
            base_snapshot,
            user_metadata_json,
            contribution_merge,
            ..
        } = stored_tx.tx.clone();
        // A structured result can reach the same immutable version through
        // more than one retained fact (for example, a root's nested relation
        // and the relation's sender witness). A wire bundle describes a set of
        // row versions, not those traversal paths: canonicalize it before
        // deriving its declared view-scoped cardinality.
        let mut versions = BTreeMap::new();
        for version in tx_versions {
            // A maintained terminal may use a current-row source projected for
            // query evaluation, but a VersionRecord is replicated history, not
            // query output. Resolve its identity back to the stored authored
            // row before crossing the wire boundary (INV-DATA-16/18,
            // INV-SYNC-16, and C.3's byte-fidelity rule).
            let canonical = if source == MaintainedBundleVersionSource::ExactStorage {
                version.clone()
            } else {
                self.canonical_history_version_for_maintained_witness(version)
                    .await?
            };
            let record = self.version_record_from_row(&canonical)?;
            let key = version_bundle_record_key(&record);
            match versions.get(&key) {
                Some(existing) if existing != &record => {
                    return Err(Error::ConflictingCommitUnit(tx_id));
                }
                Some(_) => {}
                None => {
                    versions.insert(key, record);
                }
            }
        }
        let versions = versions.into_values().collect::<Vec<_>>();
        let scope = if usize::try_from(n_total_writes).ok() == Some(versions.len()) {
            crate::protocol::VersionBundleScope::CompleteTransaction
        } else {
            crate::protocol::VersionBundleScope::ViewScoped
        };
        let tx_payload = Transaction {
            tx_id,
            kind,
            n_total_writes: match scope {
                crate::protocol::VersionBundleScope::CompleteTransaction => n_total_writes,
                crate::protocol::VersionBundleScope::ViewScoped => versions
                    .len()
                    .try_into()
                    .map_err(|_| Error::InvalidStoredValue("view payload is too large"))?,
            },
            made_by,
            permission_subject,
            base_snapshot,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json,
            contribution_merge,
        };
        Ok(VersionBundle {
            tx: tx_payload,
            versions,
            scope,
            fate: stored_tx.fate.clone(),
            global_time: stored_tx.global_time,
            durability: stored_tx.durability,
        })
    }

    /// Return the stored, authored history row for a maintained witness.
    ///
    /// The maintained graph may evaluate a schema-compatible current source,
    /// whereas a wire `VersionRecord` is always the complete immutable row
    /// version under its authored schema. This producer-side normalization is
    /// deliberately before serialization; receivers reject non-identical
    /// duplicate row versions rather than repairing them.
    pub(super) async fn canonical_history_version_for_maintained_witness(
        &mut self,
        version: &VersionRow,
    ) -> Result<VersionRow, Error> {
        // The maintained graph can call its projected result table by a name
        // that also existed in the authored schema.  Resolve that name once
        // through the active catalogue and require the same physical table
        // for every candidate below; a reused name is ambiguous and must fail
        // closed rather than selecting the first matching row key.
        let logical_candidates = self
            .catalogue
            .physical_mappings
            .values()
            .filter_map(|mapping| {
                mapping
                    .tables
                    .get(version.table())
                    .map(|mapping| mapping.table_id)
            })
            .collect::<BTreeSet<_>>();
        let witness_tx_id = self.version_tx_id(version)?;
        let physical_candidates = if logical_candidates.len() == 1 {
            logical_candidates
        } else {
            self.query_versions_for_tx(witness_tx_id)
                .await?
                .into_iter()
                .filter(|candidate| {
                    candidate.row_uuid() == version.row_uuid()
                        && candidate.layer() == version.layer()
                })
                .filter_map(|candidate| self.physical_table_id_for_version(&candidate).ok())
                .filter(|table_id| logical_candidates.contains(table_id))
                .collect::<BTreeSet<_>>()
        };
        let [projected_table_id] = physical_candidates.iter().copied().collect::<Vec<_>>()[..]
        else {
            return Err(Error::InvalidStoredValue(
                "maintained witness maps to zero or multiple physical tables",
            ));
        };
        let authored_schema = self
            .schema_version_for_alias(version.schema_version_alias())
            .ok_or(Error::InvalidStoredValue(
                "maintained witness schema version alias must exist",
            ))?;
        let authored_table = match self.table_in_schema(version.table(), authored_schema) {
            Ok(table) => table.clone(),
            Err(Error::TableNotFound(_)) => {
                // A current-query source may have been projected through a
                // later schema, so its logical name need not exist under the
                // authored alias carried by its immutable history identity.
                // Resolve it through the catalogue's unambiguous physical
                // table id, then reload the actual stored row for the same
                // physical table, row, transaction, and layer.  This is the
                // same identity boundary used for repair payloads; reused or
                // unknown logical names fail closed before history is read.
                if let Some(canonical) = self
                    .query_versions_for_tx(witness_tx_id)
                    .await?
                    .into_iter()
                    .find(|candidate| {
                        candidate.row_uuid() == version.row_uuid()
                            && candidate.layer() == version.layer()
                            && self
                                .physical_table_id_for_version(candidate)
                                .is_ok_and(|table_id| table_id == projected_table_id)
                    })
                {
                    return Ok(canonical);
                }
                return Err(Error::MaintainedViewMissingBundleWitness(
                    "maintained witness projection is missing its canonical history row",
                ));
            }
            Err(error) => return Err(error),
        };
        let authored_descriptor = if version.layer() == VersionLayer::Deletion {
            authored_table.register_storage_table().record_schema()
        } else {
            authored_table.history_storage_table().record_schema()
        };
        let has_authored_layout = version.record.descriptor() == &authored_descriptor;

        // A maintained witness is decoded from the current-query graph. Its
        // descriptor can be identical to history storage while selected-out
        // cells are still typed nulls, so first prefer its immutable stored
        // history identity even when the descriptors match.
        for storage_table in
            self.version_storage_sources_for_layer(version.table(), version.layer())?
        {
            let Some(canonical) = self
                .query_version_by_alias_with_storage_in_schema(
                    authored_schema,
                    version.table(),
                    &storage_table,
                    version.branch_key(),
                    version.row_uuid(),
                    version.tx_time(),
                    version.tx_node_alias(),
                )
                .await?
            else {
                continue;
            };
            if canonical.schema_version_alias() == version.schema_version_alias()
                && self.physical_table_id_for_version(&canonical)? == projected_table_id
            {
                return Ok(canonical);
            }
        }

        // Some maintained rows are legitimate materialized/synthetic versions
        // (for example, synthesized merge output) and therefore have no persisted
        // history identity to reload. They may cross the boundary only when
        // their authored descriptor is complete. `authored_columns` lets us
        // distinguish such a row from a query projection whose selected-out
        // authored cells were replaced by typed nulls.
        let has_complete_authored_payload = has_authored_layout
            && (version.layer() == VersionLayer::Deletion
                || match self.authored_columns_for_version(version)? {
                    Some(authored) => authored.iter().all(|column| {
                        version
                            .cell(&authored_table, column)
                            .is_ok_and(|value| value.is_some())
                    }),
                    // Legacy complete rows predate authored-column metadata.
                    None => true,
                });
        if has_complete_authored_payload {
            return Ok(version.clone());
        }
        Err(Error::MaintainedViewMissingBundleWitness(
            "maintained witness is missing its canonical history row",
        ))
    }
}

fn view_version_key(version: &VersionRecord) -> (String, BranchKey, RowUuid, VersionLayer) {
    (
        version.table().to_owned(),
        version.branch_key().clone(),
        version.row_uuid(),
        VersionLayer::for_record(version),
    )
}
