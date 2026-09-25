impl PeerState {
    fn record_outgoing_view_update_metadata(&mut self, update: &SyncMessage) {
        if let SyncMessage::ViewUpdate(view) = update
            && view.supporting_rows.is_snapshot()
            && !view.peer_payload_inventory.opening_pending
        {
            // A generic fresh snapshot supersedes any incremental publisher
            // baseline. The maintained path installs its prepared successor
            // after this metadata call; other paths rebuild once on next drain.
            let state = self
                .publication_states
                .entry(view.subscription)
                .or_default();
            state.supporting_revision = None;
        }
        let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            version_carriers,
            peer_payload_inventory,
            ..
        }) = update
        else {
            return;
        };

        let singleton_bundles = view_update_singleton_bundles(version_carriers);
        self.metrics.view_updates_out += 1;
        self.metrics.version_bundles_out += singleton_bundles.len() as u64;
        self.metrics.complete_tx_payload_refs_out +=
            peer_payload_inventory.complete_tx_payloads.len() as u64;

        self.metrics.duplicate_version_bundles_out += singleton_bundles
            .iter()
            .filter(|bundle| bundle_contains_complete_tx_payload(bundle))
            .filter(|bundle| self.shipped_complete_tx_payloads.contains(&bundle.tx.tx_id))
            .count() as u64;
    }

    /// Evaluate a client commit's write policies at the terminal authority,
    /// under the exact claims admitted for this connection. Each policy input
    /// is already at the authority cut in this node's own storage, so the
    /// evaluation reads it directly and keeps no per-connection support view.
    /// Cross-authority support (INV-SHARD-13) would have to be bound to the
    /// candidate's dependency closure; see #3794.
    pub async fn prove_terminal_commit_authorization<S>(
        &mut self,
        node: &mut NodeState<S>,
        writer: AuthorSubject,
        claims: BTreeMap<String, Value>,
        versions: &[VersionRecord],
        candidate_tx: &crate::tx::Transaction,
    ) -> Result<bool, Error>
    where
        S: OrderedKvStorage,
    {
        // SYSTEM is the trusted backend policy subject. Row-policy admission
        // already bypasses it: claim and join predicates have no SYSTEM
        // session to bind and are irrelevant to the bypass decision.
        if writer == AuthorSubject::SYSTEM {
            return Ok(true);
        }
        // The policy evaluation reads the active session scope. Keep the
        // immutable admitted snapshot installed for the entire proof; the
        // author-keyed compatibility map is neither sufficient nor safe for a
        // scope-isolated relay, and same-author sessions may differ. A
        // claim-only policy must not become an implicit grant.
        let mut node = node.scoped_active_session_claims(writer, claims);
        node.commit_unit_satisfies_write_policy(versions, writer, candidate_tx)
            .await
    }

    fn record_outgoing_view_update<S: OrderedKvStorage>(
        &mut self,
        _node: &NodeState<S>,
        _schema: crate::ids::SchemaVersionId,
        update: &SyncMessage,
    ) -> Result<(), Error> {
        self.record_outgoing_view_update_metadata(update);
        if let SyncMessage::ViewUpdate(view) = update {
            let state = self
                .publication_states
                .entry(view.subscription)
                .or_default();
            state.supporting_revision = Some(view.supporting_rows.revision());
            if let Some(maintained) = &mut state.maintained_subscription_view {
                maintained.maintained.acknowledge_peer_source_closure();
            }
        }
        Ok(())
    }

    fn apply_outgoing_view_delta(
        &mut self,
        subscription: SubscriptionKey,
        reset_input_set: bool,
        result_member_adds: &[ResultMemberEntry],
        result_member_removes: &[ResultMemberEntry],
    ) {
        let state = self.publication_states.entry(subscription).or_default();
        // This path records an independently supplied delta rather than the
        // maintained journal's exact successor. Re-establish its baseline if
        // this subscription later returns to maintained publication.
        if reset_input_set && let Some(view) = &mut state.maintained_subscription_view {
            view.maintained.forget_peer_source_closure_baseline();
        }
        if reset_input_set {
            state.supporting_revision = None;
            state.result_member_set.clear();
            state.member_index.clear();
        }
        for member in result_member_removes {
            state.result_member_set.remove(member);
            apply_contribution_remove(state, std::iter::once(member), &mut Vec::new());
        }
        for member in result_member_adds {
            state.result_member_set.insert(member.clone());
            apply_contribution_add(
                state,
                std::iter::once(member),
                &mut Vec::new(),
                &mut Vec::new(),
            );
        }
        // Diagnostic-only invariant check: detecting duplicate content versions
        // in the result set requires materializing and scanning it, which is
        // wasted work in release where the debug_assert compiles out. Gate the
        // whole scan to debug builds so it never runs on the release hot path
        // (this sat under the measured record_outgoing_view_update hotspot).
        #[cfg(debug_assertions)]
        {
            if let Some((row_key, first, second)) =
                duplicate_physical_row_result_set(&state.result_member_set)
            {
                debug_assert!(
                    first == second,
                    "peer subscription {subscription:?} has multiple content versions for physical output row {row_key:?}: {first:?} and {second:?}"
                );
            }
        }
    }
}
