//! Per-peer synchronization, repair, and resume machinery.
//!
//! A peer connection binds transport state to either an upstream authority or
//! a served subscriber. It applies and emits sync messages, tracks coverage,
//! performs bounded repair, and preserves authenticated reconnect state.

use super::node_runtime::{
    refresh_subscriptions_in, retire_relay_upstream_subscription,
    route_upstream_subscription_rejection, take_relay_upstream_subscription_owner,
};
use super::*;

/// Namespace for relay-owned usage-site subscription handles.
///
/// A relay may normalize multiple downstream coverage requests to the same
/// upstream read view. The resulting subscriptions still need distinct wire
/// handles: the upstream node deduplicates their work by [`CoverageKey`], while
/// retaining the independent ownership needed for correct unsubscribe behavior.
const RELAY_UPSTREAM_SUBSCRIPTION_NAMESPACE: uuid::Uuid =
    uuid::uuid!("ae3eb9f7-65cc-528d-8f3e-a772fb6f68fe");

fn relay_upstream_subscription_key(
    connection_epoch: u64,
    downstream: SubscriptionKey,
    upstream_read_view: ReadViewKey,
) -> SubscriptionKey {
    let identity = postcard::to_allocvec(&(connection_epoch, downstream, upstream_read_view))
        .expect("relay subscription identity is postcard encodable");
    SubscriptionKey {
        shape_id: downstream.shape_id,
        binding_id: BindingId(uuid::Uuid::new_v5(
            &RELAY_UPSTREAM_SUBSCRIPTION_NAMESPACE,
            &identity,
        )),
        read_view: upstream_read_view,
    }
}

async fn finish_peer_publication_outcome<S, T>(
    node: &SharedNodeState<S>,
    subscriptions: &SubscriptionList,
    active_authority_view_receipts: &ActiveAuthorityViewReceipts,
    outcome: PublicationOutcome<T>,
) -> Result<(T, usize), Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let (value, changed, _) = finish_peer_publication_outcome_with_refresh(
        node,
        subscriptions,
        active_authority_view_receipts,
        outcome,
        true,
    )
    .await?;
    Ok((value, changed))
}

async fn finish_peer_publication_outcome_with_refresh<S, T>(
    node: &SharedNodeState<S>,
    subscriptions: &SubscriptionList,
    active_authority_view_receipts: &ActiveAuthorityViewReceipts,
    outcome: PublicationOutcome<T>,
    refresh: bool,
) -> Result<(T, usize, bool), Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let PublicationOutcome {
        value,
        mut publications,
        mut post_settlement_work,
    } = outcome;
    let mut changed = 0;
    let mut published_any = false;
    loop {
        if !publications.is_empty() {
            published_any = true;
            if refresh {
                changed +=
                    refresh_subscriptions_in(node, subscriptions, active_authority_view_receipts)
                        .await?;
            }
            let mut persisted = Vec::with_capacity(publications.len());
            for publication in &publications {
                persisted.push((publication.tx_id(), publication.persist().await));
            }
            let mut state = node.lock().await;
            for (tx_id, persistence) in persisted {
                state.settle_published_transaction(tx_id, persistence)?;
            }
        }
        let Some(message) = post_settlement_work.pop_front() else {
            break;
        };
        let mut outcome = node
            .lock()
            .await
            .apply_sync_message_with_ingest_context(
                message,
                Some(CommitUnitIngestContext {
                    identity: AuthorSubject::SYSTEM,
                    trust: CommitUnitTrust::TrustedBackend,
                    edge_authority: false,
                }),
            )
            .await?;
        publications = outcome.publications;
        post_settlement_work.append(&mut outcome.post_settlement_work);
    }
    Ok((value, changed, published_any))
}

/// Dispatch one admitted subscriber message into node ingest.
///
/// This is deliberately one boxed suspension boundary at the peer/control-plane
/// seam. `PeerConnection::tick` otherwise contains the complete futures for
/// every message variant inline, and authority policy evaluation can exhaust a
/// normal test-thread stack before doing any recursive work.
fn dispatch_admitted_subscriber_message<'a, S>(
    node: &'a SharedNodeState<S>,
    peer: &'a mut PeerState,
    local_receiver: bool,
    ingest_context: CommitUnitIngestContext,
    admitted_upstream_authority: &'a Rc<RefCell<Option<AuthorityContext>>>,
    edge_fate_routes: &'a EdgeFateRoutes,
    local_fate_routes: &'a LocalFateRoutes,
    downstream_fates: &'a PendingDownstreamFates,
    message: SyncMessage,
) -> Pin<Box<dyn Future<Output = Result<PublicationOutcome<Vec<SyncMessage>>, Error>> + 'a>>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    Box::pin(async move {
        if let SyncMessage::CommitUnit { versions, .. } = &message
            && matches!(peer.role(), PeerRole::ClientLink { .. })
        {
            node.lock()
                .await
                .require_staged_large_values_for_versions(versions)
                .await?;
        }
        match message {
            SyncMessage::CommitUnit { tx, versions } if local_receiver => {
                let tx_id = tx.tx_id;
                register_local_fate_route(local_fate_routes, tx_id, downstream_fates);
                node.lock()
                    .await
                    .ingest_relay_commit_unit(tx, versions)
                    .await?;
                Ok(PublicationOutcome::settled(Vec::new()))
            }
            SyncMessage::CommitUnit { tx, versions }
                if ingest_context.edge_authority
                    && matches!(peer.role(), PeerRole::ClientLink { .. }) =>
            {
                if tx.kind != TxKind::Mergeable {
                    node.lock()
                        .await
                        .ingest_relay_commit_unit(tx, versions)
                        .await?;
                    return Ok(PublicationOutcome::settled(Vec::new()));
                }

                let tx_id = tx.tx_id;
                let route_registered =
                    if let Some(authority) = *admitted_upstream_authority.borrow() {
                        let mut routes = edge_fate_routes.borrow_mut();
                        prune_edge_fate_routes(&mut routes, Some(authority));
                        let route_count = routes.values().map(Vec::len).sum::<usize>();
                        let pending = routes.get(&tx_id);
                        let already_routed = pending.is_some_and(|pending| {
                            pending.iter().any(|route| {
                                route
                                    .authority
                                    .is_some_and(|route| route.same_admitted_link(authority))
                                    && route
                                        .queue
                                        .upgrade()
                                        .is_some_and(|queue| Rc::ptr_eq(&queue, downstream_fates))
                            })
                        });
                        if already_routed {
                            true
                        } else if route_count < MAX_EDGE_FATE_ROUTES {
                            let pending = routes.entry(tx_id).or_default();
                            if pending.len() < MAX_EDGE_FATE_ROUTES_PER_TX {
                                pending.push(EdgeFateRoute {
                                    authority: Some(authority),
                                    queue: Rc::downgrade(downstream_fates),
                                });
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        let mut routes = edge_fate_routes.borrow_mut();
                        prune_edge_fate_routes(&mut routes, None);
                        let already_routed = routes.get(&tx_id).is_some_and(|pending| {
                            pending.iter().any(|route| {
                                route.authority.is_none()
                                    && route
                                        .queue
                                        .upgrade()
                                        .is_some_and(|queue| Rc::ptr_eq(&queue, downstream_fates))
                            })
                        });
                        let route_count = routes.values().map(Vec::len).sum::<usize>();
                        if already_routed {
                            true
                        } else if route_count >= MAX_EDGE_FATE_ROUTES {
                            false
                        } else {
                            let pending = routes.entry(tx_id).or_default();
                            if pending.len() >= MAX_EDGE_FATE_ROUTES_PER_TX {
                                false
                            } else {
                                pending.push(EdgeFateRoute {
                                    authority: None,
                                    queue: Rc::downgrade(downstream_fates),
                                });
                                true
                            }
                        }
                    };

                if !route_registered {
                    return Ok(PublicationOutcome::settled(vec![SyncMessage::FateUpdate {
                        tx_id,
                        fate: Fate::Rejected(RejectionReason::MalformedCommit(
                            "no admitted authority route".to_owned(),
                        )),
                        global_time: None,
                        durability: None,
                    }]));
                }

                node.lock()
                    .await
                    .ingest_relay_commit_unit(tx, versions)
                    .await?;
                Ok(PublicationOutcome::settled(vec![SyncMessage::FateUpdate {
                    tx_id,
                    fate: Fate::Accepted,
                    global_time: None,
                    durability: Some(DurabilityTier::Edge),
                }]))
            }
            SyncMessage::CommitUnit { tx, versions }
                if tx.kind == TxKind::Mergeable
                    && matches!(peer.role(), PeerRole::ClientLink { .. }) =>
            {
                {
                    let mut node = node.lock().await;
                    peer.prove_terminal_commit_authorization(
                        &mut node,
                        ingest_context.identity,
                        &versions,
                        tx.tx_id,
                    )
                    .await?;
                }
                Ok(node
                    .lock()
                    .await
                    .apply_sync_message_with_ingest_context(
                        SyncMessage::CommitUnit { tx, versions },
                        Some(ingest_context),
                    )
                    .await?)
            }
            other => Ok(node
                .lock()
                .await
                .apply_sync_message_with_ingest_context(other, Some(ingest_context))
                .await?),
        }
    })
}

/// A live link between this `Db` and one peer, owned by the `Db`.
///
/// Two link shapes — a client/backend attached to an upstream, or a server
/// serving one subscriber under their identity. An edge is simply both at once
/// (one upstream connection plus many subscriber connections); edge authority
/// (relay/edge/core) stays below this facade in [`crate::peer`].
pub struct PeerConnection<S>
where
    S: OrderedKvStorage,
{
    pub(super) transport: Box<dyn Transport>,
    pub(super) staged_inbound: VecDeque<StagedInboundMessage>,
    pub(super) node: SharedNodeState<S>,
    pub(super) subscriptions: SubscriptionList,
    pub(super) upstream_subscription_owners: UpstreamSubscriptionOwners,
    pub(super) relay_upstream_subscription_owners: RelayUpstreamSubscriptionOwners,
    pub(super) pending_relay_subscription_rejections: PendingRelaySubscriptionRejections,
    pub(super) latest_coverage_subscriptions: LatestCoverageSubscriptions,
    pub(super) awaiting_initial_authority_coverage: AwaitingInitialAuthorityCoverage,
    pub(super) active_authority_view_receipts: ActiveAuthorityViewReceipts,
    pub(super) scheduler: SharedTickScheduler,
    pub(super) upload_retry_clock: SharedUploadRetryClock,
    pub(super) upstream_upload_destination: Option<UpstreamUploadDestination>,
    pub(super) large_value_upload_retry_deadlines: Rc<RefCell<BTreeMap<TxId, u64>>>,
    pub(super) write_state_waiters: WriteStateWaiters,
    pub(super) permission_advice_waiters: PermissionAdviceWaiters,
    pub(super) edge_fate_routes: EdgeFateRoutes,
    pub(super) local_fate_routes: LocalFateRoutes,
    pub(super) admitted_upstream_authority: Rc<RefCell<Option<AuthorityContext>>>,
    pub(super) downstream_fates: PendingDownstreamFates,
    pub(super) mutation_errors: SharedMutationErrors,
    pub(super) subscriber_dirty_epoch: Rc<Cell<u64>>,
    pub(super) observed_subscriber_dirty_epoch: Cell<u64>,
    pub(super) observed_session_claim_revision: Cell<u64>,
    /// Fresh non-resumable epoch binding authorization receipts to this link.
    pub(super) connection_epoch: u64,
    pub(super) startup_error: Option<Error>,
    /// Exact uploads whose applied fate made them globally settled or rejected.
    pub(super) released_outbox_tx_ids: Vec<TxId>,
    pub(super) link: ConnectionLink,
    pub(super) last_resume_bytes: Option<usize>,
    pub(super) auxiliary_pump: PeerIoPump,
}

pub(super) enum ConnectionLink {
    Upstream(UpstreamConnectionState),
    Subscriber(SubscriberConnectionState),
}

pub(super) struct UpstreamConnectionState {
    pub(super) local_receiver: bool,
    pub(super) pending: Vec<PendingUpstreamCommand>,
    pub(super) upstream_subscriptions: PendingUpstreamCommands,
    pub(super) announced_shapes: BTreeSet<ShapeRegistrationKey>,
    pub(super) sent_session_claim_revisions: BTreeMap<AuthorSubject, u64>,
    pub(super) outbox: Outbox,
    pub(super) uploaded: BTreeSet<TxId>,
    pub(super) large_value_uploads: LargeValueUploadQueues,
    pub(super) awaiting_large_value_uploads: BTreeMap<TxId, groove::large_values::LargeValueRef>,
    pub(super) failed_large_value_uploads: BTreeSet<TxId>,
    pub(super) pending_row_version_repairs: VecDeque<PendingRowVersionRepair>,
    pub(super) scope_view_cuts: BTreeMap<SubscriptionKey, crate::time::GlobalTime>,
    pub(super) scope_receipts: BTreeMap<SubscriptionKey, AuthorizationScopeReceipt>,
    pub(super) expected_scope_authority: Option<AuthorityContext>,
    pub(super) scope_lease_manager: AuthorizationScopeLeaseManager,
}

pub(super) struct PendingLargeValueUpload {
    value_ref: groove::large_values::LargeValueRef,
    requested: VecDeque<groove::large_values::NodeRef>,
    /// Nodes sent in the current batch, retained until the receiver accepts
    /// them so a rate-limited batch can be retried without restarting upload.
    in_flight: VecDeque<groove::large_values::NodeRef>,
    retry_not_before_ms: Option<u64>,
    started: bool,
}

pub(super) type LargeValueUploadQueues = BTreeMap<TxId, VecDeque<PendingLargeValueUpload>>;

/// Transfer one detached upstream's resumable upload state. A reply that was
/// in flight is no longer tied to a live transport, so restore its exact batch
/// to the requested frontier; a start awaiting its first frontier restarts
/// only after the shared admission deadline.
pub(super) fn take_reconnectable_large_value_uploads(
    uploads: &mut LargeValueUploadQueues,
    awaiting: &mut BTreeMap<TxId, groove::large_values::LargeValueRef>,
) -> LargeValueUploadQueues {
    let awaiting = std::mem::take(awaiting);
    let mut uploads = std::mem::take(uploads);
    for (tx_id, value_ref) in awaiting {
        let Some(upload) = uploads.get_mut(&tx_id).and_then(|uploads| {
            uploads
                .iter_mut()
                .find(|upload| upload.value_ref == value_ref)
        }) else {
            continue;
        };
        if upload.in_flight.is_empty() {
            upload.started = false;
        } else {
            while let Some(node) = upload.in_flight.pop_back() {
                upload.requested.push_front(node);
            }
        }
    }
    uploads
}

/// Merge independently detached links without duplicating a logical
/// transaction/value upload. Parallel links can carry the same outbox entry;
/// the first retained frontier is enough to resume that value once.
pub(super) fn merge_reconnectable_large_value_uploads(
    destination_uploads: &mut LargeValueUploadQueues,
    uploads: LargeValueUploadQueues,
) {
    for (tx_id, uploads) in uploads {
        let retained = destination_uploads.entry(tx_id).or_default();
        for upload in uploads {
            if retained
                .iter()
                .any(|existing| existing.value_ref == upload.value_ref)
            {
                continue;
            }
            retained.push_back(upload);
        }
    }
}

const RATE_LIMITED_UPLOAD_RETRY_DELAY_MS: u64 = 1_000;

fn collect_large_value_refs(value: &Value, refs: &mut Vec<groove::large_values::LargeValueRef>) {
    match value {
        Value::Large(value_ref) => {
            if !refs.contains(value_ref) {
                refs.push(value_ref.clone());
            }
        }
        Value::Tuple(values) | Value::Array(values) => {
            for value in values {
                collect_large_value_refs(value, refs);
            }
        }
        Value::Nullable(Some(value)) => collect_large_value_refs(value, refs),
        Value::Record(record) => {
            if let Ok(values) = record.to_values() {
                for value in values {
                    collect_large_value_refs(&value, refs);
                }
            }
        }
        Value::Enum(value) => {
            if let Ok(values) = value.record().to_values() {
                for value in values {
                    collect_large_value_refs(&value, refs);
                }
            }
        }
        _ => {}
    }
}

fn commit_unit_large_value_refs(unit: &SyncMessage) -> Vec<groove::large_values::LargeValueRef> {
    let SyncMessage::CommitUnit { versions, .. } = unit else {
        return Vec::new();
    };
    let mut refs = Vec::new();
    for version in versions {
        for position in 0..version.application_cell_count() {
            if let Some(value) = version.cell_at(position) {
                collect_large_value_refs(&value, &mut refs);
            }
        }
    }
    refs
}

pub(super) struct SubscriberConnectionState {
    pub(super) peer: PeerState,
    pub(super) ingest_context: CommitUnitIngestContext,
    pub(super) session_claims: BTreeMap<String, Value>,
    pub(super) session_claim_revision: u64,
    pub(super) local_receiver: bool,
    pub(super) outbox: Outbox,
    pub(super) upstream_subscriptions: PendingUpstreamCommands,
    pub(super) served: BTreeMap<SubscriptionKey, CoverageKey>,
    pub(super) coverage_groups: BTreeMap<CoverageKey, CoverageGroup>,
    pub(super) shape_registrations: BTreeMap<ShapeRegistrationKey, SubscriberShapeRegistration>,
    pub(super) deferred_subscribe_rejections: VecDeque<(SubscriptionKey, String)>,
    pub(super) served_current_rows: BTreeMap<SubscriptionKey, String>,
    pub(super) scope_purposes: BTreeMap<SubscriptionKey, AuthorizedScopePurpose>,
    pub(super) scope_aggregates:
        BTreeMap<crate::protocol::AuthorizationSupportScopeKey, AuthorityScopeAggregate>,
    pub(super) authority_scope_hydrations:
        BTreeMap<crate::protocol::AuthorizationSupportScopeKey, ServedAuthorizationScopeHydration>,
    pub(super) authority_scope_hydration_count: u64,
    pub(super) serve_dirty: bool,
}

pub(super) struct PendingRowVersionRepair {
    pub(super) requests: Vec<crate::protocol::RowVersionRef>,
    pub(super) update: SyncMessage,
    pub(super) authority_receipt_eligible: bool,
}

/// Per-connection resume state for a served subscriber.
///
/// Bindings keep this after a disconnect and pass it into
/// [`Node::accept_subscriber_with_resume`] for the reconnecting subscriber. It is
/// the facade handle for the peer-layer complete-tx payload inventory,
/// result-set cursor, and authenticated connection context. Resume must never
/// fall back to identity-global claim state because same-identity sessions can
/// legitimately hold different admission claims.
#[derive(Debug)]
pub struct ResumeCursor {
    pub(super) peer: PeerState,
    pub(super) ingest_context: CommitUnitIngestContext,
    pub(super) session_claims: BTreeMap<String, Value>,
    pub(super) session_claim_revision: u64,
}

impl<S> PeerConnection<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    pub(super) fn take_released_outbox_tx_ids(&mut self) -> Vec<TxId> {
        std::mem::take(&mut self.released_outbox_tx_ids)
    }

    pub(super) fn forget_released_outbox_tx_ids(&mut self, released: &HashSet<TxId>) {
        let ConnectionLink::Upstream(UpstreamConnectionState { uploaded, .. }) = &mut self.link
        else {
            return;
        };
        for tx_id in released {
            uploaded.remove(tx_id);
        }
    }

    /// Clone the binding-driven auxiliary I/O endpoint for this peer link.
    pub fn io_pump(&self) -> PeerIoPump {
        self.auxiliary_pump.clone()
    }
    /// Replace the claims authenticated by the host for this subscriber link.
    /// Wire peers cannot invoke this path; bindings use it only after their
    /// trusted authentication layer has accepted a refreshed session.
    pub fn update_authenticated_session_claims(&mut self, claims: BTreeMap<String, Value>) {
        let ConnectionLink::Subscriber(SubscriberConnectionState {
            session_claims,
            session_claim_revision,
            ..
        }) = &mut self.link
        else {
            return;
        };
        if *session_claims == claims {
            return;
        }
        *session_claims = claims;
        *session_claim_revision = session_claim_revision.saturating_add(1);
    }

    /// Bind the process-local query compiler to this subscriber's authenticated
    /// session immediately before it serves work for that subscriber. NodeState
    /// retains a cache keyed by identity, while several websocket sessions can
    /// legitimately share an identity with different claim maps.
    fn bind_subscriber_session_claims(&self) {
        let ConnectionLink::Subscriber(SubscriberConnectionState {
            ingest_context,
            session_claims,
            ..
        }) = &self.link
        else {
            return;
        };
        self.node
            .borrow_mut()
            .set_session_claims(ingest_context.identity, session_claims.clone());
    }

    fn subscriber_session_claim_revision(&self) -> u64 {
        let ConnectionLink::Subscriber(SubscriberConnectionState {
            session_claim_revision,
            ..
        }) = &self.link
        else {
            return 0;
        };
        *session_claim_revision
    }

    /// Rebuild this subscriber's maintained views if its process-local claims
    /// changed. Policy claim values are bound when a maintained view opens, so
    /// retaining the old view after a claim change would retain its authority.
    async fn rebind_subscriber_views_after_claim_change(&mut self) -> Result<bool, Error> {
        let connection_epoch = self.connection_epoch;
        let identity = match &self.link {
            ConnectionLink::Subscriber(SubscriberConnectionState { ingest_context, .. }) => {
                ingest_context.identity
            }
            ConnectionLink::Upstream(_) => return Ok(false),
        };
        self.bind_subscriber_session_claims();
        let current_revision = self.subscriber_session_claim_revision();
        if self.observed_session_claim_revision.get() == current_revision {
            return Ok(false);
        }

        let ConnectionLink::Subscriber(SubscriberConnectionState {
            peer,
            coverage_groups,
            served_current_rows,
            scope_purposes,
            scope_aggregates,
            ..
        }) = &mut self.link
        else {
            unreachable!("subscriber identity requires a subscriber link")
        };
        peer.advance_authorization_progress();
        let groups = coverage_groups
            .iter()
            .map(|(coverage, group)| {
                (
                    coverage.clone(),
                    group.shape.clone(),
                    group.binding.clone(),
                    group.subscribers.iter().copied().collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        for (coverage, shape, binding, subscribers) in groups {
            let maintained_subscription = SubscriptionKey {
                shape_id: coverage.shape_id,
                binding_id: coverage.binding_id,
                read_view: coverage.opts.read_view_key(),
            };
            let update = {
                let mut node = self.node.lock().await;
                peer.rehydrate_query_for_subscription_with_opts(
                    &mut node,
                    maintained_subscription,
                    &shape,
                    &binding,
                    coverage.opts,
                )
                .await?
            };
            let Some(update) = update else {
                continue;
            };
            for subscription in subscribers {
                let mut update = retarget_view_update(update.clone(), subscription);
                stamp_view_update_authorization_progress_from(
                    peer,
                    maintained_subscription,
                    &mut update,
                );
                let prior_scope = scope_purposes.get(&subscription).cloned();
                let refreshed_scope = prior_scope.as_ref().and_then(|prior| {
                    refresh_authorized_scope_purpose(
                        &self.node.borrow(),
                        identity,
                        subscription,
                        &shape,
                        &binding,
                        &prior,
                    )
                });
                if let Some(refreshed) = &refreshed_scope {
                    move_scope_aggregate_member(
                        scope_aggregates,
                        prior_scope.as_ref(),
                        refreshed,
                        subscription,
                    );
                    scope_purposes.insert(subscription, refreshed.clone());
                } else if let Some(prior) = scope_purposes.remove(&subscription) {
                    remove_scope_aggregate_member(scope_aggregates, &prior.key, subscription);
                }
                let receipt = refreshed_scope.as_ref().and_then(|purpose| {
                    aggregate_authorization_scope_receipt_for_view(
                        scope_aggregates,
                        &self.node.borrow(),
                        peer,
                        identity,
                        connection_epoch,
                        purpose,
                        &update,
                    )
                });
                send_with_sync_context(&self.node, peer, self.transport.as_mut(), update)?;
                if let Some((subscription, receipt)) = receipt {
                    self.transport
                        .send(SyncMessage::AuthorizationScopeReceipt {
                            subscription,
                            receipt,
                        })
                        .map_err(transport_error)?;
                }
            }
        }
        for table in served_current_rows.values() {
            let update = {
                let mut node = self.node.lock().await;
                peer.current_rows_update(&mut node, table).await?
            };
            send_with_sync_context(&self.node, peer, self.transport.as_mut(), update)?;
        }

        self.observed_session_claim_revision.set(current_revision);
        Ok(true)
    }

    /// Serve a whole-table current-row view to this subscriber immediately and
    /// refresh it on later ticks.
    pub async fn serve_current_rows(&mut self, table: &str) -> Result<(), Error> {
        self.tick().await?;
        let ConnectionLink::Subscriber(SubscriberConnectionState {
            peer,
            served_current_rows,
            ..
        }) = &mut self.link
        else {
            return Ok(());
        };
        let update = {
            let mut node = self.node.lock().await;
            peer.current_rows_update(&mut node, table).await?
        };
        self.last_resume_bytes = Some(serialized_sync_message_len(&update));
        let subscription = view_update_subscription(&update);
        send_sync_message_chunked(self.transport.as_mut(), update)?;
        if let Some(subscription) = subscription {
            served_current_rows.insert(subscription, table.to_owned());
        }
        if let ConnectionLink::Subscriber(SubscriberConnectionState { serve_dirty, .. }) =
            &mut self.link
        {
            *serve_dirty = true;
        }
        Ok(())
    }

    /// Return the serialized byte size of the latest resume/catch-up response
    /// sent by this connection.
    pub fn last_resume_bytes(&self) -> Option<usize> {
        self.last_resume_bytes
    }

    /// Return a receipt only after this connection applied its matching
    /// authorization-support view. A reconnect creates a new connection and
    /// therefore has no receipt to reuse.
    pub fn authorization_scope_receipt(
        &self,
        subscription: SubscriptionKey,
    ) -> Option<&AuthorizationScopeReceipt> {
        let ConnectionLink::Upstream(UpstreamConnectionState { scope_receipts, .. }) = &self.link
        else {
            return None;
        };
        scope_receipts.get(&subscription)
    }

    /// Extract this subscriber connection's resume cursor for a reconnect.
    pub fn take_resume_cursor(&mut self) -> Option<ResumeCursor> {
        let ConnectionLink::Subscriber(SubscriberConnectionState {
            peer,
            ingest_context,
            session_claims,
            session_claim_revision,
            ..
        }) = &mut self.link
        else {
            return None;
        };
        let replacement = PeerState::client_link(peer.link_identity());
        Some(ResumeCursor {
            peer: std::mem::replace(peer, replacement),
            ingest_context: *ingest_context,
            session_claims: std::mem::take(session_claims),
            session_claim_revision: *session_claim_revision,
        })
    }

    /// Rehydrate every view served on this subscriber link. This is used when
    /// an authority installs its first permissions head or replaces it with a
    /// tighter one: the client keeps the same subscription, but its visible
    /// membership must be recalculated immediately.
    pub(super) async fn rehydrate_subscriber_views(&mut self) -> Result<(), Error> {
        self.bind_subscriber_session_claims();
        let connection_epoch = self.connection_epoch;
        let ConnectionLink::Subscriber(SubscriberConnectionState {
            peer,
            coverage_groups,
            ingest_context,
            scope_purposes,
            scope_aggregates,
            serve_dirty,
            ..
        }) = &mut self.link
        else {
            return Ok(());
        };
        peer.advance_authorization_progress();
        let groups = coverage_groups
            .iter()
            .map(|(coverage, group)| {
                (
                    coverage.clone(),
                    group.shape.clone(),
                    group.binding.clone(),
                    group.subscribers.iter().copied().collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        for (coverage, shape, binding, subscribers) in groups {
            let group_subscription = SubscriptionKey {
                shape_id: coverage.shape_id,
                binding_id: coverage.binding_id,
                read_view: coverage.opts.read_view_key(),
            };
            let update = {
                let mut node = self.node.lock().await;
                peer.rehydrate_query_for_subscription_with_opts(
                    &mut node,
                    group_subscription,
                    &shape,
                    &binding,
                    coverage.opts.clone(),
                )
                .await?
            };
            let Some(update) = update else {
                *serve_dirty = true;
                continue;
            };
            for subscription in subscribers {
                let mut update = retarget_view_update(update.clone(), subscription);
                stamp_view_update_authorization_progress_from(
                    peer,
                    group_subscription,
                    &mut update,
                );
                self.last_resume_bytes = Some(serialized_sync_message_len(&update));
                let prior_scope = scope_purposes.get(&subscription).cloned();
                let refreshed_scope = prior_scope.as_ref().and_then(|prior| {
                    refresh_authorized_scope_purpose(
                        &self.node.borrow(),
                        ingest_context.identity,
                        subscription,
                        &shape,
                        &binding,
                        &prior,
                    )
                });
                if let Some(refreshed) = &refreshed_scope {
                    move_scope_aggregate_member(
                        scope_aggregates,
                        prior_scope.as_ref(),
                        refreshed,
                        subscription,
                    );
                    scope_purposes.insert(subscription, refreshed.clone());
                } else if let Some(prior) = scope_purposes.remove(&subscription) {
                    remove_scope_aggregate_member(scope_aggregates, &prior.key, subscription);
                }
                let receipt = refreshed_scope.as_ref().and_then(|purpose| {
                    aggregate_authorization_scope_receipt_for_view(
                        scope_aggregates,
                        &self.node.borrow(),
                        peer,
                        ingest_context.identity,
                        connection_epoch,
                        purpose,
                        &update,
                    )
                });
                send_with_sync_context(&self.node, peer, self.transport.as_mut(), update)?;
                if let Some((subscription, receipt)) = receipt {
                    self.transport
                        .send(SyncMessage::AuthorizationScopeReceipt {
                            subscription,
                            receipt,
                        })
                        .map_err(transport_error)?;
                }
            }
        }
        *serve_dirty = true;
        Ok(())
    }

    /// Preserve every frame currently staged on this link across an authority
    /// handoff, but make its ViewUpdates ineligible as the new authority's
    /// receipt. The next transport arrival is therefore the first receipt
    /// candidate after selection.
    pub(super) fn stage_inbound_without_authority_receipt(&mut self) {
        if let ConnectionLink::Upstream(UpstreamConnectionState {
            pending_row_version_repairs,
            ..
        }) = &mut self.link
        {
            for repair in pending_row_version_repairs {
                repair.authority_receipt_eligible = false;
            }
        }
        while let Some(message) = self.transport.try_recv() {
            self.staged_inbound.push_back(StagedInboundMessage {
                message,
                authority_receipt_eligible: false,
            });
        }
    }

    /// Service this connection once: drain inbound, apply, wake subscriptions, and
    /// flush pending outbound. Non-blocking; the binding calls it in its loop.
    pub async fn tick(&mut self) -> Result<DbTickStats, Error> {
        if let Some(error) = self.startup_error.take() {
            return Err(error);
        }
        let mut stats = DbTickStats::default();
        let connection_epoch = self.connection_epoch;
        self.observe_shared_subscriber_dirty_epoch();
        self.bind_subscriber_session_claims();
        self.rebind_subscriber_views_after_claim_change().await?;
        match &mut self.link {
            ConnectionLink::Upstream(UpstreamConnectionState {
                local_receiver,
                pending,
                upstream_subscriptions,
                announced_shapes,
                sent_session_claim_revisions,
                outbox,
                uploaded,
                large_value_uploads,
                awaiting_large_value_uploads,
                failed_large_value_uploads,
                pending_row_version_repairs,
                scope_view_cuts,
                scope_receipts,
                expected_scope_authority,
                scope_lease_manager,
            }) => {
                let stop = Box::pin(async {
                    let outbound_stop = Box::pin(async {
                        if let Some(message) = self.auxiliary_pump.take_outbound(64) {
                            if let Err(error) = self.transport.send(message.clone()) {
                                self.auxiliary_pump.restore_outbound(message);
                                if handle_transport_backpressure(
                                    &self.node,
                                    &self.scheduler,
                                    &error,
                                ) {
                                    return Ok(true);
                                }
                                return Err(transport_error(error));
                            }
                            self.auxiliary_pump.acknowledge_outbound(&message);
                        }
                        pending.extend(upstream_subscriptions.borrow_mut().drain(..));
                        let claims = self.node.borrow().session_claims_with_revisions();
                        for (identity, claims, revision) in claims {
                            if sent_session_claim_revisions
                                .get(&identity)
                                .is_some_and(|sent| *sent >= revision)
                            {
                                continue;
                            }
                            if let Err(error) = self
                                .transport
                                .send(SyncMessage::SessionClaims { identity, claims })
                            {
                                if handle_transport_backpressure(
                                    &self.node,
                                    &self.scheduler,
                                    &error,
                                ) {
                                    return Ok(true);
                                }
                                return Err(transport_error(error));
                            }
                            sent_session_claim_revisions.insert(identity, revision);
                        }
                        let pending_index = 0;
                        while pending_index < pending.len() {
                            match &pending[pending_index] {
                                PendingUpstreamCommand::Subscribe(pending_subscription) => {
                                    let shape = &pending_subscription.shape;
                                    let binding = &pending_subscription.binding;
                                    let registration_key = (
                                        shape.shape_id(),
                                        pending_subscription.opts.read_view_key(),
                                    );
                                    if announced_shapes.insert(registration_key) {
                                        let outcome = self
                                            .node
                                            .lock()
                                            .await
                                            .apply_sync_message(SyncMessage::RegisterShape {
                                                shape_id: shape.shape_id(),
                                                ast: ShapeAst::from_validated(shape),
                                                opts: RegisterShapeOptions::default(),
                                            })
                                            .await?;
                                        let (_, changed) = finish_peer_publication_outcome(
                                            &self.node,
                                            &self.subscriptions,
                                            &self.active_authority_view_receipts,
                                            outcome,
                                        )
                                        .await?;
                                        stats.subscription_events += changed;
                                        if let Err(error) =
                                            self.transport.send(SyncMessage::RegisterShape {
                                                shape_id: shape.shape_id(),
                                                ast: ShapeAst::from_validated(shape),
                                                opts: pending_subscription.opts.clone(),
                                            })
                                        {
                                            announced_shapes.remove(&registration_key);
                                            if handle_transport_backpressure(
                                                &self.node,
                                                &self.scheduler,
                                                &error,
                                            ) {
                                                return Ok(true);
                                            }
                                            return Err(transport_error(error));
                                        }
                                    }
                                    let values = binding_values_in_param_order(shape, binding);
                                    let known_state = self
                                        .node
                                        .lock()
                                        .await
                                        .known_state_declaration_for_subscription(
                                            shape,
                                            binding,
                                            pending_subscription.subscription,
                                            &values,
                                            pending_subscription.identity,
                                        )
                                        .await?;
                                    let subscribe = Subscribe {
                                        shape_id: shape.shape_id(),
                                        subscription: pending_subscription.subscription,
                                        values,
                                        known_state,
                                    };
                                    #[cfg(feature = "sync-autopsy")]
                                    sync_autopsy::record(format!(
                                        "upstream send subscribe {}",
                                        summarize_subscription_key(subscribe.subscription)
                                    ));
                                    let outcome = self
                                        .node
                                        .lock()
                                        .await
                                        .apply_sync_message(SyncMessage::Subscribe(
                                            subscribe.clone(),
                                        ))
                                        .await?;
                                    let (_, changed) = finish_peer_publication_outcome(
                                        &self.node,
                                        &self.subscriptions,
                                        &self.active_authority_view_receipts,
                                        outcome,
                                    )
                                    .await?;
                                    stats.subscription_events += changed;
                                    if let Err(error) =
                                        self.transport.send(SyncMessage::Subscribe(subscribe))
                                    {
                                        if handle_transport_backpressure(
                                            &self.node,
                                            &self.scheduler,
                                            &error,
                                        ) {
                                            return Ok(true);
                                        }
                                        return Err(transport_error(error));
                                    }
                                }
                                PendingUpstreamCommand::Unsubscribe(subscription) => {
                                    self.node.borrow_mut().apply_unsubscribe(*subscription);
                                    if let Err(error) =
                                        self.transport.send(SyncMessage::Unsubscribe {
                                            subscription: *subscription,
                                        })
                                    {
                                        if handle_transport_backpressure(
                                            &self.node,
                                            &self.scheduler,
                                            &error,
                                        ) {
                                            return Ok(true);
                                        }
                                        return Err(transport_error(error));
                                    }
                                }
                                PendingUpstreamCommand::AuthorizationScopeIntent {
                                    request_id,
                                    action,
                                } => {
                                    // An old or unauthenticated upstream must never receive a
                                    // downgraded preflight.  Resolve conservatively instead.
                                    if expected_scope_authority.is_none() {
                                        self.permission_advice_waiters
                                            .borrow_mut()
                                            .remove(request_id);
                                        scope_lease_manager.requests.remove(request_id);
                                    } else if self
                                        .permission_advice_waiters
                                        .borrow()
                                        .contains_key(request_id)
                                    {
                                        if let Some(existing) = scope_lease_manager
                                            .requests
                                            .values_mut()
                                            .find(|request| request.action == *action)
                                        {
                                            existing.waiters.insert(*request_id);
                                        } else {
                                            scope_lease_manager.requests.insert(
                                                *request_id,
                                                AuthorizationScopeLeaseRequest {
                                                    action: action.clone(),
                                                    waiters: BTreeSet::from([*request_id]),
                                                    key: None,
                                                    lease: None,
                                                    owner: None,
                                                    clause_count: None,
                                                    applied_clauses: BTreeMap::new(),
                                                },
                                            );
                                            if let Err(error) = self.transport.send(
                                                SyncMessage::AuthorizationScopeIntent {
                                                    request_id: *request_id,
                                                    action: action.clone(),
                                                },
                                            ) {
                                                if handle_transport_backpressure(
                                                    &self.node,
                                                    &self.scheduler,
                                                    &error,
                                                ) {
                                                    return Ok(true);
                                                }
                                                return Err(transport_error(error));
                                            }
                                        }
                                    }
                                }
                            }
                            pending.remove(pending_index);
                        }
                        // Upload locally-authored commits not yet shipped on this link.
                        let to_upload: Vec<(TxId, Option<SyncMessage>)> = {
                            let outbox = outbox.borrow();
                            let expected_missing = outbox.len().checked_sub(uploaded.len());
                            let mut suffix = outbox
                                .iter()
                                .rev()
                                .take_while(|pending| !uploaded.contains(&pending.tx_id))
                                .map(|pending| (pending.tx_id, pending.unit.clone()))
                                .collect::<Vec<_>>();
                            if expected_missing == Some(suffix.len()) {
                                suffix.reverse();
                                suffix
                            } else {
                                // Fate cleanup and authority handoff can leave
                                // holes in this link's uploaded set. Preserve
                                // the general set-difference behavior there.
                                outbox
                                    .iter()
                                    .filter(|pending| !uploaded.contains(&pending.tx_id))
                                    .map(|pending| (pending.tx_id, pending.unit.clone()))
                                    .collect()
                            }
                        };
                        for (tx_id, staged) in to_upload {
                            if failed_large_value_uploads.contains(&tx_id)
                                || awaiting_large_value_uploads.contains_key(&tx_id)
                                || !awaiting_large_value_uploads.is_empty()
                            {
                                continue;
                            }
                            let now_ms = self.upload_retry_clock.borrow().now_ms();
                            if let Some(deadline) = self
                                .large_value_upload_retry_deadlines
                                .borrow()
                                .get(&tx_id)
                                .copied()
                                && now_ms < deadline
                            {
                                continue;
                            }
                            self.large_value_upload_retry_deadlines
                                .borrow_mut()
                                .remove(&tx_id);
                            let unit = if let Some(unit) = staged {
                                unit
                            } else {
                                self.node.lock().await.commit_unit_for(tx_id).await?
                            };
                            if let std::collections::btree_map::Entry::Vacant(entry) =
                                large_value_uploads.entry(tx_id)
                            {
                                let refs = commit_unit_large_value_refs(&unit);
                                let mut uploads = VecDeque::new();
                                for value_ref in refs {
                                    uploads.push_back(PendingLargeValueUpload {
                                        value_ref,
                                        requested: VecDeque::new(),
                                        in_flight: VecDeque::new(),
                                        retry_not_before_ms: None,
                                        started: false,
                                    });
                                }
                                entry.insert(uploads);
                            }
                            let uploads = large_value_uploads
                                .get_mut(&tx_id)
                                .expect("initialized above");
                            if let Some(upload) = uploads.front_mut() {
                                let now_ms = self.upload_retry_clock.borrow().now_ms();
                                if upload.retry_not_before_ms.is_some_and(|deadline| now_ms < deadline) {
                                    continue;
                                }
                                upload.retry_not_before_ms = None;
                                let supplying_nodes = upload.started;
                                let mut supplied_count = 0_usize;
                                let message = if !upload.started {
                                    SyncMessage::ChunkUploadStart(
                                        crate::protocol::ChunkUploadStart {
                                            value_ref: upload.value_ref.clone(),
                                        },
                                    )
                                } else {
                                    let requested = upload
                                        .requested
                                        .iter()
                                        .take(64)
                                        .cloned()
                                        .collect::<Vec<_>>();
                                    if requested.is_empty() {
                                        continue;
                                    }
                                    let mut chunks = Vec::with_capacity(requested.len());
                                    for node_ref in requested {
                                        let (replica_role, source_node, result) = {
                                            let node = self.node.lock().await;
                                            let replica_role = if node.authored_commit_durability()
                                                == DurabilityTier::None
                                            {
                                                "non-durable-client"
                                            } else {
                                                "durable-relay"
                                            };
                                            let source_node = node.node_uuid();
                                            let result = node
                                                .local_chunk(
                                                    node_ref.locator,
                                                    node_ref.object_hash,
                                                )
                                                .await;
                                            (replica_role, source_node, result)
                                        };
                                        let encoded = result.map_err(|source| {
                                                crate::node::Error::LargeValueUploadChunkUnavailable {
                                                    context: large_value_upload_chunk_context(
                                                        tx_id,
                                                        &upload.value_ref,
                                                        &node_ref,
                                                        replica_role,
                                                        source_node,
                                                    ),
                                                    source,
                                                }
                                            })?;
                                        chunks.push(groove::large_values::StagedChunk {
                                            node_ref,
                                            encoded: encoded.to_vec(),
                                        });
                                    }
                                    supplied_count = chunks.len();
                                    SyncMessage::ChunkUploadNodes(
                                        crate::protocol::ChunkUploadNodes {
                                            value_ref: upload.value_ref.clone(),
                                            chunks,
                                        },
                                    )
                                };
                                if let Err(error) = self.transport.send(message) {
                                    if handle_transport_backpressure(
                                        &self.node,
                                        &self.scheduler,
                                        &error,
                                    ) {
                                        return Ok(true);
                                    }
                                    return Err(transport_error(error));
                                }
                                if supplying_nodes {
                                    for _ in 0..supplied_count {
                                        upload.in_flight.push_back(
                                            upload
                                                .requested
                                                .pop_front()
                                                .expect("sent nodes came from requested frontier"),
                                        );
                                    }
                                }
                                upload.started = true;
                                awaiting_large_value_uploads
                                    .insert(tx_id, upload.value_ref.clone());
                            }
                            if awaiting_large_value_uploads.contains_key(&tx_id)
                                || !uploads.is_empty()
                            {
                                continue;
                            }
                            if let Err(error) = send_with_local_sync_context(
                                &self.node,
                                self.transport.as_mut(),
                                unit,
                            ) {
                                if handle_db_backpressure(&self.node, &self.scheduler, &error) {
                                    return Ok(true);
                                }
                                return Err(error);
                            }
                            large_value_uploads.remove(&tx_id);
                            uploaded.insert(tx_id);
                        }
                        Ok::<bool, Error>(false)
                    })
                    .await?;
                    if outbound_stop {
                        return Ok(true);
                    }
                    let mut applied = false;
                    let mut publications = Vec::new();
                    let mut pending_view_updates = Vec::<PendingAuthorityViewUpdate>::new();
                    let mut pending_initial_coverage_clears = BTreeSet::<CoverageKey>::new();
                    while let Some(StagedInboundMessage {
                        message,
                        authority_receipt_eligible,
                    }) = self.staged_inbound.pop_front().or_else(|| {
                        self.transport
                            .try_recv()
                            .map(|message| StagedInboundMessage {
                                message,
                                authority_receipt_eligible: true,
                            })
                    }) {
                        let write_state_tx_id = write_state_update_tx_id(&message);
                        #[cfg(feature = "sync-autopsy")]
                        sync_autopsy::record(format!(
                            "upstream recv {}",
                            summarize_sync_message(&message)
                        ));
                        match message {
                            SyncMessage::ChunkResponseBatch(batch) => {
                                for response in batch.responses {
                                    self.auxiliary_pump.resolver.complete(response);
                                }
                                continue;
                            }
                            SyncMessage::ChunkRequestBatch(_) => {
                                drop_peer_request(&self.node);
                                continue;
                            }
                            SyncMessage::ChunkUploadResult(result) => {
                                let rate_limited = matches!(
                                    &result.status,
                                    crate::protocol::ChunkUploadStatus::RateLimited
                                );
                                let pending_tx = awaiting_large_value_uploads
                                    .iter()
                                    .find_map(|(tx_id, value_ref)| {
                                        (value_ref == &result.value_ref).then_some(*tx_id)
                                    });
                                match result.status {
                                    crate::protocol::ChunkUploadStatus::Need(nodes) => {
                                        if let Some(tx_id) = pending_tx {
                                            awaiting_large_value_uploads.remove(&tx_id);
                                            if let Some(upload) = large_value_uploads
                                                .get_mut(&tx_id)
                                                .and_then(|uploads| uploads.front_mut())
                                            {
                                                upload.in_flight.clear();
                                                upload.requested.extend(nodes);
                                            }
                                        }
                                    }
                                    crate::protocol::ChunkUploadStatus::Staged => {
                                        if let Some(tx_id) = pending_tx {
                                            awaiting_large_value_uploads.remove(&tx_id);
                                            if let Some(uploads) =
                                                large_value_uploads.get_mut(&tx_id)
                                            {
                                                if let Some(upload) = uploads.front_mut() {
                                                    upload.in_flight.clear();
                                                }
                                                uploads.pop_front();
                                            }
                                        }
                                    }
                                    crate::protocol::ChunkUploadStatus::RateLimited => {
                                        if let Some(tx_id) = pending_tx {
                                            awaiting_large_value_uploads.remove(&tx_id);
                                            if let Some(upload) = large_value_uploads
                                                .get_mut(&tx_id)
                                                .and_then(|uploads| uploads.front_mut())
                                            {
                                                while let Some(node) = upload.in_flight.pop_back() {
                                                    upload.requested.push_front(node);
                                                }
                                                let now_ms =
                                                    self.upload_retry_clock.borrow().now_ms();
                                                upload.retry_not_before_ms = Some(
                                                    now_ms.saturating_add(
                                                        RATE_LIMITED_UPLOAD_RETRY_DELAY_MS,
                                                    ),
                                                );
                                                self.large_value_upload_retry_deadlines
                                                    .borrow_mut()
                                                    .insert(
                                                        tx_id,
                                                        now_ms.saturating_add(
                                                            RATE_LIMITED_UPLOAD_RETRY_DELAY_MS,
                                                        ),
                                                    );
                                                if let Some(scheduler) = self.scheduler.borrow().as_ref() {
                                                    scheduler.schedule_tick_after(RATE_LIMITED_UPLOAD_RETRY_DELAY_MS);
                                                }
                                            }
                                        }
                                    }
                                    crate::protocol::ChunkUploadStatus::Rejected => {
                                        if let Some(tx_id) = pending_tx {
                                            awaiting_large_value_uploads.remove(&tx_id);
                                            failed_large_value_uploads.insert(tx_id);
                                            large_value_uploads.remove(&tx_id);
                                            self.large_value_upload_retry_deadlines
                                                .borrow_mut()
                                                .remove(&tx_id);
                                            outbox
                                                .borrow_mut()
                                                .retain(|pending| pending.tx_id != tx_id);
                                            self.staged_inbound.push_front(StagedInboundMessage {
                                                message: SyncMessage::FateUpdate {
                                                    tx_id,
                                                    fate: Fate::Rejected(
                                                        RejectionReason::MalformedCommit(
                                                            "large-value upload was not staged; upload again".to_owned(),
                                                        ),
                                                    ),
                                                    global_time: None,
                                                    durability: None,
                                                },
                                                authority_receipt_eligible: false,
                                            });
                                        }
                                    }
                                }
                                if !rate_limited {
                                    schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                }
                                continue;
                            }
                            SyncMessage::CatalogueSnapshot(snapshot) => {
                                if !pending_view_updates.is_empty() {
                                    apply_pending_authority_view_updates(
                                        &self.node,
                                        &mut pending_view_updates,
                                        &self.awaiting_initial_authority_coverage,
                                        &mut pending_initial_coverage_clears,
                                        &self.active_authority_view_receipts,
                                        self.connection_epoch,
                                    )
                                    .await?;
                                }
                                let outcome = self
                                    .node
                                    .lock()
                                    .await
                                    .apply_trusted_catalogue_snapshot(*snapshot)
                                    .await?;
                                publications.extend(outcome.publications);
                            }
                            SyncMessage::RowVersionPayloads { version_bundles } => {
                                if !pending_view_updates.is_empty() {
                                    apply_pending_authority_view_updates(
                                        &self.node,
                                        &mut pending_view_updates,
                                        &self.awaiting_initial_authority_coverage,
                                        &mut pending_initial_coverage_clears,
                                        &self.active_authority_view_receipts,
                                        self.connection_epoch,
                                    )
                                    .await?;
                                }
                                let Some(repair) = pending_row_version_repairs.pop_front() else {
                                    drop_peer_request(&self.node);
                                    continue;
                                };
                                {
                                    let mut node = self.node.lock().await;
                                    node.apply_row_version_payloads_for_requests(
                                        &repair.requests,
                                        version_bundles,
                                    )
                                    .await?;
                                }
                                let (subscription, settled_through) = match &repair.update {
                                    SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                                        subscription,
                                        settled_through,
                                        ..
                                    }) => (*subscription, *settled_through),
                                    _ => {
                                        unreachable!("row-version repair must retain a view update")
                                    }
                                };
                                stage_initial_coverage_clear_for_update(
                                    &repair.update,
                                    &self.latest_coverage_subscriptions,
                                    &mut pending_initial_coverage_clears,
                                );
                                push_view_update_message_for_receiver(
                                    &mut pending_view_updates,
                                    repair.update,
                                    repair.authority_receipt_eligible,
                                )?;
                                scope_view_cuts.insert(subscription, settled_through);
                            }
                            message @ SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                                subscription,
                                settled_through,
                                ..
                            }) => {
                                scope_receipts.remove(&subscription);
                                #[cfg(not(feature = "sync-autopsy"))]
                                let _ = subscription;
                                let missing = {
                                    let mut node = self.node.lock().await;
                                    node.missing_known_state_row_version_refs(&message).await?
                                };
                                if missing.is_empty() {
                                    stage_initial_coverage_clear_for_update(
                                        &message,
                                        &self.latest_coverage_subscriptions,
                                        &mut pending_initial_coverage_clears,
                                    );
                                    push_view_update_message_for_receiver(
                                        &mut pending_view_updates,
                                        message,
                                        authority_receipt_eligible,
                                    )?;
                                    scope_view_cuts.insert(subscription, settled_through);
                                    #[cfg(feature = "sync-autopsy")]
                                    sync_autopsy::record(format!(
                                        "upstream applied view update {}",
                                        summarize_subscription_key(subscription)
                                    ));
                                } else {
                                    #[cfg(feature = "sync-autopsy")]
                                    sync_autopsy::record(format!(
                                        "upstream queued repair {} missing={}",
                                        summarize_subscription_key(subscription),
                                        missing.len()
                                    ));
                                    self.transport
                                        .send(SyncMessage::FetchRowVersions {
                                            requests: missing.clone(),
                                        })
                                        .map_err(transport_error)?;
                                    pending_row_version_repairs.push_back(
                                        PendingRowVersionRepair {
                                            requests: missing,
                                            update: message,
                                            authority_receipt_eligible,
                                        },
                                    );
                                }
                            }
                            SyncMessage::SubscribeRejected {
                                subscription,
                                reason,
                            } => {
                                if let Some(owner) = take_relay_upstream_subscription_owner(
                                    &self.relay_upstream_subscription_owners,
                                    subscription,
                                ) {
                                    self.pending_relay_subscription_rejections
                                        .borrow_mut()
                                        .entry(owner.downstream_connection_epoch)
                                        .or_default()
                                        .push_back(RelaySubscriptionRejection {
                                            coverage: owner.coverage,
                                            downstream_subscriptions: owner
                                                .downstream_subscriptions,
                                            reason,
                                        });
                                    // The authority has already retired its
                                    // own attempt, but sending an explicit
                                    // unsubscribe makes reconnect/replay
                                    // convergence deterministic on peers that
                                    // retain rejected handles until told
                                    // otherwise.
                                    upstream_subscriptions.borrow_mut().push(
                                        PendingUpstreamCommand::Unsubscribe(subscription),
                                    );
                                    let next = self.subscriber_dirty_epoch.get().wrapping_add(1);
                                    self.subscriber_dirty_epoch.set(next);
                                    schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                } else {
                                    stats.subscription_events += route_upstream_subscription_rejection(
                                        &self.subscriptions,
                                        &self.upstream_subscription_owners,
                                        subscription,
                                        reason,
                                    );
                                }
                            }
                            SyncMessage::PermissionAdviceResponse { request_id, advice } => {
                                // Direct answers were the pre-Phase-3 protocol.
                                // They have no receipt-bound evidence and must
                                // never influence a modern client.
                                let _ = (request_id, advice);
                                drop_peer_request(&self.node);
                            }
                            SyncMessage::AuthorizationScopeView {
                                request_id,
                                key,
                                clause_index,
                                clause_count,
                                view,
                            } => {
                                let Some(expected) = expected_scope_authority else {
                                    drop_peer_request(&self.node);
                                    continue;
                                };
                                let subscription = view.subscription;
                                let settled_through = view.settled_through;
                                let authorization_progress = view
                                    .peer_payload_inventory
                                    .authorization_progress
                                    .unwrap_or_default();
                                if clause_count == 0
                                    || clause_index >= clause_count
                                    || key.subject != expected.link
                                {
                                    drop_peer_request(&self.node);
                                    continue;
                                }
                                let Some(prior) = scope_lease_manager.requests.get(&request_id)
                                else {
                                    // A cancelled intent cannot be revived by a
                                    // late/replayed authority view.
                                    continue;
                                };
                                if prior.key.as_ref().is_some_and(|known| known != &key)
                                    || prior
                                        .clause_count
                                        .is_some_and(|known| known != clause_count)
                                {
                                    drop_peer_request(&self.node);
                                    continue;
                                }
                                // The first authenticated view reveals the
                                // server-selected scope key.  Acquire here, before
                                // the aggregate receipt, so concurrent actions
                                // that compile to this same support scope share
                                // one registry lifecycle rather than racing after
                                // hydration has already completed.
                                let acquired = if prior.lease.is_none() {
                                    scope_lease_manager.registry.acquire(key.clone())
                                } else {
                                    None
                                };
                                let Some(request) =
                                    scope_lease_manager.requests.get_mut(&request_id)
                                else {
                                    continue;
                                };
                                request.key = Some(key);
                                request.clause_count = Some(clause_count);
                                if request.lease.is_none() {
                                    let Some((lease, acquisition)) = acquired else {
                                        self.permission_advice_waiters
                                            .borrow_mut()
                                            .remove(&request_id);
                                        scope_lease_manager.requests.remove(&request_id);
                                        continue;
                                    };
                                    request.owner = match acquisition {
                                        AuthorizationScopeAcquisition::Owner(owner) => Some(owner),
                                        AuthorizationScopeAcquisition::Waiting
                                        | AuthorizationScopeAcquisition::Proven => None,
                                    };
                                    request.lease = Some(lease);
                                }
                                let duplicate = request
                                    .applied_clauses
                                    .get(&clause_index)
                                    .is_some_and(|(prior, _, _)| *prior != subscription);
                                if duplicate {
                                    drop_peer_request(&self.node);
                                    continue;
                                }
                                // The envelope remains a normal ViewUpdate; retain
                                // existing batching/settlement semantics before a
                                // receipt can be accepted.
                                push_view_update_message_for_receiver(
                                    &mut pending_view_updates,
                                    view.into_view_update(),
                                    authority_receipt_eligible,
                                )?;
                                scope_view_cuts.insert(subscription, settled_through);
                                request.applied_clauses.insert(
                                    clause_index,
                                    (subscription, settled_through, authorization_progress),
                                );
                            }
                            SyncMessage::AuthorizationScopeAggregateReceipt {
                                request_id,
                                receipt,
                            } => {
                                // The authority's FIFO ordering says this receipt
                                // follows the views, but apply the queued views now
                                // so receipt admission is never merely queued.
                                if !pending_view_updates.is_empty() {
                                    apply_pending_authority_view_updates(
                                        &self.node,
                                        &mut pending_view_updates,
                                        &self.awaiting_initial_authority_coverage,
                                        &mut pending_initial_coverage_clears,
                                        &self.active_authority_view_receipts,
                                        self.connection_epoch,
                                    )
                                    .await?;
                                }
                                let Some(expected) = expected_scope_authority.as_mut() else {
                                    drop_peer_request(&self.node);
                                    continue;
                                };
                                let Some(request) =
                                    scope_lease_manager.requests.get_mut(&request_id)
                                else {
                                    continue;
                                };
                                // An empty compiled support set is a valid public
                                // policy proof.  It has no views by construction;
                                // the authority receipt itself supplies the
                                // server-chosen key and current context.
                                let clause_count = request.clause_count.unwrap_or(0);
                                let all_current = request.applied_clauses.len()
                                    == usize::from(clause_count)
                                    && request.applied_clauses.iter().all(
                                        |(index, (_, cut, _))| {
                                            *index < clause_count && *cut >= receipt.settled_through
                                        },
                                    );
                                let applied_cut = if clause_count == 0 {
                                    Some(receipt.settled_through)
                                } else {
                                    request
                                        .applied_clauses
                                        .values()
                                        .map(|(_, cut, _)| *cut)
                                        .min()
                                };
                                let observed = self.node.borrow();
                                let observed_claims = observed
                                    .session_claim_revision(expected.link);
                                let observed_policy = observed.active_catalogue_seq();
                                drop(observed);
                                // Context components are monotonic per admitted
                                // connection. A receipt may advance an otherwise
                                // opaque authority revision, but it can never
                                // decrease a component already admitted.
                                let applied_progress = request
                                    .applied_clauses
                                    .values()
                                    .map(|(_, _, progress)| *progress)
                                    .min()
                                    .unwrap_or_default();
                                let receipt_decreased = receipt.claims_revision
                                    < expected.claims_revision
                                    || receipt.policy_epoch < expected.policy_epoch
                                    || receipt.authorization_progress
                                        < expected.authorization_progress
                                    || receipt.settled_through.0 < expected.settled_through;
                                if observed_claims > expected.claims_revision {
                                    expected.claims_revision = observed_claims;
                                } else if observed_claims == 0 {
                                    expected.claims_revision = receipt.claims_revision;
                                }
                                if observed_policy > expected.policy_epoch {
                                    expected.policy_epoch = observed_policy;
                                } else if observed_policy == 0 {
                                    expected.policy_epoch = receipt.policy_epoch;
                                }
                                expected.authorization_progress =
                                    expected.authorization_progress.max(applied_progress);
                                expected.settled_through =
                                    expected.settled_through.max(receipt.settled_through.0);
                                let receipt_current =
                                    request.key.as_ref().is_some_and(|key| key == &receipt.key)
                                        && all_current
                                        && !receipt_decreased
                                        && authorization_scope_receipt_matches_transport_context(
                                            &receipt,
                                            *expected,
                                            applied_cut,
                                        );
                                if !receipt_current {
                                    // A claim/catalogue/progress transition can
                                    // race a just-completed hydration.  Retire its
                                    // lease and allocate a new opaque wire id so
                                    // old views/receipts cannot revive the
                                    // operation; retain the caller waiters and
                                    // reacquire under the observed context.
                                    let retry_id =
                                        PermissionAdviceRequestId(*uuid::Uuid::new_v4().as_bytes());
                                    let action = request.action.clone();
                                    let waiters = request.waiters.clone();
                                    scope_lease_manager.requests.remove(&request_id);
                                    scope_lease_manager.requests.insert(
                                        retry_id,
                                        AuthorizationScopeLeaseRequest {
                                            action: action.clone(),
                                            waiters,
                                            key: None,
                                            lease: None,
                                            owner: None,
                                            clause_count: None,
                                            applied_clauses: BTreeMap::new(),
                                        },
                                    );
                                    pending.push(
                                        PendingUpstreamCommand::AuthorizationScopeIntent {
                                            request_id: retry_id,
                                            action,
                                        },
                                    );
                                    drop_peer_request(&self.node);
                                    continue;
                                }
                                let admitted = match (request.lease.as_ref(), request.owner.take())
                                {
                                    (Some(lease), Some(owner)) => matches!(
                                        scope_lease_manager.registry.install(
                                            lease,
                                            owner,
                                            *expected,
                                            receipt.clone(),
                                        ),
                                        AuthorizationScopeInstall::Installed
                                    ),
                                    (Some(lease), None) => matches!(
                                        scope_lease_manager.registry.receipt(
                                            lease,
                                            *expected,
                                            receipt.authorization_progress,
                                            receipt.settled_through.0,
                                        ),
                                        AuthorizationScopeReadiness::Proven(_)
                                    ),
                                    (None, _) => false,
                                };
                                if !admitted {
                                    continue;
                                }
                                let action = request.action.clone();
                                let waiter_ids = request.waiters.clone();
                                scope_lease_manager.requests.remove(&request_id);
                                let advice = {
                                    let mut node = self.node.lock().await;
                                    evaluate_authoritative_permission_advice(
                                        &mut node,
                                        receipt.key.subject,
                                        action,
                                    )
                                    .await
                                };
                                for waiter_id in waiter_ids {
                                    if let Some(waiter) = self
                                        .permission_advice_waiters
                                        .borrow_mut()
                                        .remove(&waiter_id)
                                    {
                                        let _ = waiter.send(advice);
                                    }
                                }
                            }
                            SyncMessage::AuthorizationScopeUnavailable { request_id } => {
                                if let Some(request) =
                                    scope_lease_manager.requests.remove(&request_id)
                                {
                                    for waiter_id in request.waiters {
                                        if let Some(waiter) = self
                                            .permission_advice_waiters
                                            .borrow_mut()
                                            .remove(&waiter_id)
                                        {
                                            let _ = waiter.send(PermissionAdvice::Unknown);
                                        }
                                    }
                                }
                            }
                            SyncMessage::AuthorizationScopeDecision { request_id, advice } => {
                                if expected_scope_authority.is_none() {
                                    drop_peer_request(&self.node);
                                    continue;
                                }
                                if let Some(request) =
                                    scope_lease_manager.requests.remove(&request_id)
                                {
                                    for waiter_id in request.waiters {
                                        if let Some(waiter) = self
                                            .permission_advice_waiters
                                            .borrow_mut()
                                            .remove(&waiter_id)
                                        {
                                            let _ = waiter.send(advice);
                                        }
                                    }
                                }
                            }
                            SyncMessage::AuthorizationScopeReceipt {
                                subscription,
                                receipt,
                            } => {
                                let Some(expected) = expected_scope_authority else {
                                    drop_peer_request(&self.node);
                                    continue;
                                };
                                if !authorization_scope_receipt_matches_transport_context(
                                    &receipt,
                                    *expected,
                                    scope_view_cuts.get(&subscription).copied(),
                                ) {
                                    drop_peer_request(&self.node);
                                    continue;
                                }
                                scope_receipts.insert(subscription, receipt);
                            }
                            message => {
                                if let SyncMessage::FateUpdate { tx_id, .. } = &message {
                                    let admitted = *self.admitted_upstream_authority.borrow();
                                    // Gate fate before any NodeState mutation. A
                                    // parallel, stale, or featureless upstream is
                                    // not merely forbidden from forwarding an
                                    // edge route; it must not settle the routed
                                    // transaction's local state. Ordinary Core
                                    // client links have no edge route and retain
                                    // their normal fate transport.
                                    let routed = self.edge_fate_routes.borrow().contains_key(tx_id);
                                    if routed
                                        && !matches!(
                                            (admitted, *expected_scope_authority),
                                            (Some(admitted), Some(expected))
                                                if admitted.same_admitted_link(expected)
                                        )
                                    {
                                        drop_peer_request(&self.node);
                                        continue;
                                    }
                                }
                                let routed_fate = match &message {
                                    SyncMessage::FateUpdate { tx_id, .. } => {
                                        Some((*tx_id, message.clone()))
                                    }
                                    _ => None,
                                };
                                let released_outbox_tx_id = match &message {
                                    SyncMessage::FateUpdate {
                                        tx_id,
                                        fate,
                                        durability,
                                        ..
                                    } if matches!(fate, Fate::Rejected(_))
                                        || durability
                                            .is_some_and(|tier| tier >= DurabilityTier::Global) =>
                                    {
                                        Some(*tx_id)
                                    }
                                    _ => None,
                                };
                                if !pending_view_updates.is_empty() {
                                    apply_pending_authority_view_updates(
                                        &self.node,
                                        &mut pending_view_updates,
                                        &self.awaiting_initial_authority_coverage,
                                        &mut pending_initial_coverage_clears,
                                        &self.active_authority_view_receipts,
                                        self.connection_epoch,
                                    )
                                    .await?;
                                }
                                if *local_receiver {
                                    match message {
                                        SyncMessage::CommitUnit { tx, versions } => {
                                            self.node
                                                .lock()
                                                .await
                                                .ingest_relay_commit_unit(tx, versions)
                                                .await?;
                                        }
                                        other => {
                                            let outcome = self
                                                .node
                                                .lock()
                                                .await
                                                .apply_sync_message_with_ingest_context(other, None)
                                                .await?;
                                            publications.extend(outcome.publications);
                                        }
                                    }
                                } else {
                                    let outcome = self
                                        .node
                                        .lock()
                                        .await
                                        .apply_sync_message_with_ingest_context(message, None)
                                        .await?;
                                    publications.extend(outcome.publications);
                                }
                                if let Some((tx_id, fate)) = routed_fate {
                                    let authority = *expected_scope_authority;
                                    let mut routes = self.edge_fate_routes.borrow_mut();
                                    if let Some(pending) = routes.get_mut(&tx_id) {
                                        let mut remaining = Vec::new();
                                        for route in std::mem::take(pending) {
                                            let authority_matches = matches!(
                                                (route.authority, authority),
                                                (Some(route), Some(authority))
                                                    if route.same_admitted_link(authority)
                                            );
                                            let queue = route.queue.upgrade();
                                            if authority_matches {
                                                if let Some(queue) = queue {
                                                    queue.borrow_mut().push(fate.clone());
                                                }
                                            } else {
                                                remaining.push(route);
                                            }
                                        }
                                        if remaining.is_empty() {
                                            routes.remove(&tx_id);
                                        } else {
                                            *routes
                                                .get_mut(&tx_id)
                                                .expect("route remains present") = remaining;
                                        }
                                    }
                                    drop(routes);
                                    route_local_fate(&self.local_fate_routes, tx_id, &fate);
                                }
                                if let Some(tx_id) = released_outbox_tx_id {
                                    self.released_outbox_tx_ids.push(tx_id);
                                }
                            }
                        }
                        if let Some(tx_id) = write_state_tx_id {
                            handle_write_state_update(
                                &self.node,
                                &self.write_state_waiters,
                                &self.mutation_errors,
                                &self.scheduler,
                                tx_id,
                            );
                        }
                        applied = true;
                    }
                    if !pending_view_updates.is_empty() {
                        apply_pending_authority_view_updates(
                            &self.node,
                            &mut pending_view_updates,
                            &self.awaiting_initial_authority_coverage,
                            &mut pending_initial_coverage_clears,
                            &self.active_authority_view_receipts,
                            self.connection_epoch,
                        )
                        .await?;
                    }
                    if applied {
                        stats.subscription_events += refresh_subscriptions_in(
                            &self.node,
                            &self.subscriptions,
                            &self.active_authority_view_receipts,
                        )
                        .await?;
                        let mut persisted = Vec::with_capacity(publications.len());
                        for publication in &publications {
                            persisted.push((publication.tx_id(), publication.persist().await));
                        }
                        let mut node = self.node.lock().await;
                        for (tx_id, persistence) in persisted {
                            node.settle_published_transaction(tx_id, persistence)?;
                        }
                        let authoritative_reset_deferred = node.has_pending_authoritative_reset();
                        drop(node);
                        if authoritative_reset_deferred {
                            // The pre-persistence refresh deliberately retained
                            // this reset while its publication was ambiguous.
                            // Settlement makes it publishable; complete that
                            // lifecycle here instead of waiting for unrelated
                            // query or transport activity to wake subscribers.
                            stats.subscription_events += refresh_subscriptions_in(
                                &self.node,
                                &self.subscriptions,
                                &self.active_authority_view_receipts,
                            )
                            .await?;
                        }
                        stats.remote_sync_applied += 1;
                        let next = self.subscriber_dirty_epoch.get().wrapping_add(1);
                        self.subscriber_dirty_epoch.set(next);
                    }
                    Ok::<bool, Error>(false)
                })
                .await?;
                if stop {
                    return Ok(stats);
                }
            }
            ConnectionLink::Subscriber(SubscriberConnectionState {
                peer,
                ingest_context,
                session_claims: _,
                session_claim_revision: _,
                local_receiver,
                outbox,
                upstream_subscriptions,
                served,
                coverage_groups,
                shape_registrations,
                deferred_subscribe_rejections,
                served_current_rows,
                scope_purposes,
                scope_aggregates,
                authority_scope_hydrations,
                authority_scope_hydration_count,
                serve_dirty,
            }) => {
                let stop = Box::pin(async {
                // A trusted backend subscriber is an edge's normal upstream
                // link.  Unlike an application subscriber, it is entitled to
                // the authority catalogue and has no application subscription
                // that would otherwise cause a ViewUpdate to carry the
                // snapshot.  Announce it eagerly (and again only when its
                // fingerprint changes) so catalogue publication can propagate
                // Core -> peer edge before any client work starts.
                if ingest_context.trust.is_trusted() {
                    send_catalogue_snapshot_if_needed(&self.node, peer, self.transport.as_mut())?;
                }
                let mut applied_inbound = false;
                let mut scheduled_immediate = false;
                let mut sent_view_update = false;
                let mut needs_subscription_refresh = false;
                let relay_rejections = self
                    .pending_relay_subscription_rejections
                    .borrow_mut()
                    .remove(&connection_epoch)
                    .unwrap_or_default();
                for rejection in relay_rejections {
                    let active_subscriptions = coverage_groups
                        .get(&rejection.coverage)
                        .map(|group| {
                            group
                                .subscribers
                                .intersection(&rejection.downstream_subscriptions)
                                .copied()
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    // Forward the authority's reason to every active usage
                    // site before retiring the group. One coverage evaluator
                    // may have many downstream wire subscriptions.
                    for subscription in active_subscriptions {
                        self.transport
                            .send(SyncMessage::SubscribeRejected {
                                subscription,
                                reason: rejection.reason.clone(),
                            })
                            .map_err(transport_error)?;
                    }
                    if let Some(group) = coverage_groups.remove(&rejection.coverage) {
                        let group_subscription = SubscriptionKey {
                            shape_id: rejection.coverage.shape_id,
                            binding_id: rejection.coverage.binding_id,
                            read_view: rejection.coverage.opts.read_view_key(),
                        };
                        let mut node = self.node.borrow_mut();
                        // `group_subscription` owns the one shared maintained
                        // evaluator. The individual subscribers are still
                        // separately registered wire usage sites, including
                        // the case where a peer deliberately used noncanonical
                        // binding handles. Retire both layers: dropping only
                        // the group leaves the individual registrations and
                        // known-state declarations resident after rejection.
                        peer.forget_subscription_with_node(&mut node, group_subscription);
                        for subscription in group.subscribers {
                            node.apply_unsubscribe(subscription);
                            if subscription != group_subscription {
                                peer.forget_subscription(subscription);
                            }
                            served.remove(&subscription);
                            if let Some(purpose) = scope_purposes.remove(&subscription) {
                                remove_scope_aggregate_member(
                                    scope_aggregates,
                                    &purpose.key,
                                    subscription,
                                );
                            }
                        }
                    }
                }
                for fate in std::mem::take(&mut *self.downstream_fates.borrow_mut()) {
                    send_with_sync_context(&self.node, peer, self.transport.as_mut(), fate)?;
                }
                if let Some(message) = self.auxiliary_pump.take_outbound(64) {
                    if let Err(error) = self.transport.send(message.clone()) {
                        self.auxiliary_pump.restore_outbound(message);
                        return Err(transport_error(error));
                    }
                    self.auxiliary_pump.acknowledge_outbound(&message);
                }
                while let Some(message) = self.transport.try_recv() {
                    // Authorization support is authority-owned in Phase 3.
                    // A subscriber must never be able to smuggle a support
                    // purpose alongside its own shape/binding subscription.
                    let scope_purpose: Option<crate::protocol::AuthorizationScopePurpose> = None;
                    if subscriber_inbound_message_is_authority_only(&message, ingest_context.trust)
                    {
                        drop_peer_request(&self.node);
                        continue;
                    }
                    applied_inbound = true;
                    #[cfg(feature = "sync-autopsy")]
                    sync_autopsy::record(format!(
                        "subscriber recv {}",
                        summarize_sync_message(&message)
                    ));
                    match message {
                        SyncMessage::ChunkRequestBatch(batch) => {
                            let mut responses = Vec::new();
                            for request in batch.requests {
                                if self.auxiliary_pump.is_disconnected() {
                                    break;
                                }
                                let local = self
                                    .node
                                    .lock()
                                    .await
                                    .local_chunk(
                                        request.locator.clone(),
                                        groove::large_values::ContentHash(request.expected_hash),
                                    )
                                    .await;
                                if self.auxiliary_pump.is_disconnected() {
                                    break;
                                }
                                match local {
                                    Ok(bytes) => responses.push(ChunkResponseEntry {
                                        request_id: request.request_id,
                                        result: ChunkResponse::Found(bytes.to_vec()),
                                    }),
                                    Err(groove::chunks::ChunkStorageError::Unavailable) => self
                                        .auxiliary_pump
                                        .resolver
                                        .enqueue_relay(self.connection_epoch, request),
                                    Err(_) => responses.push(ChunkResponseEntry {
                                        request_id: request.request_id,
                                        result: ChunkResponse::Unavailable,
                                    }),
                                }
                            }
                            if !responses.is_empty()
                                && !self.auxiliary_pump.is_disconnected()
                            {
                                self.transport
                                    .send(SyncMessage::ChunkResponseBatch(ChunkResponseBatch {
                                        responses,
                                    }))
                                    .map_err(transport_error)?;
                            }
                            continue;
                        }
                        SyncMessage::ChunkResponseBatch(_) => {
                            drop_peer_request(&self.node);
                            continue;
                        }
                        SyncMessage::AuthorizationScopeIntent { request_id, action } => {
                            let admitted = self.transport.connection_session_context().is_some_and(
                                |context| {
                                    context.negotiated_features
                                        & crate::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS
                                        != 0
                                },
                            );
                            if !admitted {
                                drop_peer_request(&self.node);
                                continue;
                            }
                            serve_authorization_scope_intent(
                                &self.node,
                                peer,
                                self.transport.as_mut(),
                                ingest_context.identity,
                                connection_epoch,
                                request_id,
                                action,
                                ingest_context.trust,
                                authority_scope_hydrations,
                                authority_scope_hydration_count,
                            )
                            .await?;
                            continue;
                        }
                        // Legacy direct answers and caller-authored support
                        // subscriptions are deliberately fail-closed.
                        SyncMessage::PermissionAdviceRequest { .. }
                        | SyncMessage::PermissionAdviceResponse { .. }
                        | SyncMessage::AuthorizationScopeSubscribe { .. }
                        | SyncMessage::AuthorizationScopeReceipt { .. }
                        | SyncMessage::AuthorizationScopeView { .. }
                        | SyncMessage::AuthorizationScopeAggregateReceipt { .. }
                        | SyncMessage::AuthorizationScopeUnavailable { .. }
                        | SyncMessage::AuthorizationScopeDecision { .. } => {
                            drop_peer_request(&self.node);
                            continue;
                        }
                        SyncMessage::RegisterShape {
                            shape_id,
                            opts,
                            ast,
                        } => {
                            if let Err(message) =
                                validate_shape_registration_size(&ast, &opts)
                            {
                                // No stable subscription key exists before the
                                // read-view key is derived. Fail the peer link
                                // rather than inventing unnegotiated wire
                                // semantics or hashing attacker-sized options.
                                return Err(Error::new(ErrorCode::Protocol, message));
                            }
                            let read_view_key = opts.read_view_key();
                            let registration_key = (shape_id, read_view_key);
                            if let Err(error) = ensure_supported_register_shape_options(
                                &opts,
                                *local_receiver,
                                peer.role(),
                            ) {
                                shape_registrations.insert(
                                    registration_key,
                                    SubscriberShapeRegistration::RejectedUnsupportedCapability(
                                        error.message.clone(),
                                    ),
                                );
                                send_unsupported_shape_capability_rejection(
                                    &mut *self.transport,
                                    register_shape_rejection_subscription(
                                        shape_id,
                                        read_view_key,
                                    ),
                                    error.message,
                                )
                                .map_err(transport_error)?;
                                continue;
                            }
                            let shape_validation = {
                                let node = self.node.borrow();
                                validate_shape_ast_for_registration(&node, shape_id, &ast)
                            };
                            let shape = match shape_validation {
                                Ok(Some(shape)) => Some(shape),
                                Ok(None) => None,
                                Err(error) => {
                                    if is_server_shape_validation_failure(&error) {
                                        reject_server_subscription_failure(
                                            &mut *self.transport,
                                            register_shape_rejection_subscription(
                                                shape_id,
                                                read_view_key,
                                            ),
                                            &error,
                                        )
                                        .map_err(transport_error)?;
                                    } else {
                                        drop_peer_request(&self.node);
                                    }
                                    continue;
                                }
                            };
                            if let Some(shape) = &shape {
                                // Branch compilation may install an empty
                                // process-local sparse source. Defer that
                                // side-effecting preflight until Subscribe,
                                // where the authenticated branch gate is
                                // available.
                                if shape.params().is_empty() {
                                    let binding = shape.bind(BTreeMap::new()).map_err(Error::from);
                                    let binding = match binding {
                                        Ok(binding) => binding,
                                        Err(_) => {
                                            drop_peer_request(&self.node);
                                            continue;
                                        }
                                    };
                                    let supported = self
                                        .node
                                        .lock()
                                        .await
                                        .ensure_peer_maintained_subscription_view_supported(
                                            shape,
                                            &binding,
                                            opts.tier,
                                            subscriber_permission_subject(*ingest_context),
                                            &opts.read_view,
                                            QueryAuthorizationMode::TrustedServing,
                                        )
                                        .await;
                                    if let Err(crate::node::Error::QueryCapability(detail)) =
                                        supported
                                    {
                                        shape_registrations.insert(
                                            registration_key,
                                            SubscriberShapeRegistration::RejectedUnsupportedCapability(
                                                detail.clone(),
                                            ),
                                        );
                                        let subscription = SubscriptionKey {
                                            shape_id,
                                            binding_id: binding.binding_id(),
                                            read_view: read_view_key,
                                        };
                                        send_unsupported_shape_capability_rejection(
                                            &mut *self.transport,
                                            subscription,
                                            detail,
                                        )
                                        .map_err(transport_error)?;
                                        continue;
                                    } else if let Err(error) = supported {
                                        reject_server_subscription_failure(
                                            &mut *self.transport,
                                            SubscriptionKey {
                                                shape_id,
                                                binding_id: binding.binding_id(),
                                                read_view: read_view_key,
                                            },
                                            &error,
                                        )
                                        .map_err(transport_error)?;
                                        continue;
                                    }
                                }
                            }
                            let awaiting_catalogue_admission = shape.is_none();
                            if let Some(existing) = shape_registrations.get(&registration_key) {
                                match existing {
                                    SubscriberShapeRegistration::Registered(existing_opts)
                                    | SubscriberShapeRegistration::PendingCatalogueAdmission(
                                        existing_opts,
                                    ) if existing_opts != &opts => {
                                        drop_peer_request(&self.node);
                                        continue;
                                    }
                                    SubscriberShapeRegistration::RejectedUnsupportedCapability(
                                        detail,
                                    ) => {
                                        send_unsupported_shape_capability_rejection(
                                            &mut *self.transport,
                                            register_shape_rejection_subscription(
                                                shape_id,
                                                read_view_key,
                                            ),
                                            detail.clone(),
                                        )
                                        .map_err(transport_error)?;
                                        continue;
                                    }
                                    _ => {}
                                }
                            }
                            let rejection_subscription =
                                register_shape_rejection_subscription(shape_id, registration_key.1);
                            let register_result = {
                                self.node
                                    .lock()
                                    .await
                                    .apply_sync_message(SyncMessage::RegisterShape {
                                        shape_id,
                                        ast,
                                        opts: RegisterShapeOptions::default(),
                                    })
                                    .await
                            };
                            if let Err(error) = register_result {
                                reject_server_subscription_failure(
                                    &mut *self.transport,
                                    rejection_subscription,
                                    &error,
                                )
                                .map_err(transport_error)?;
                                continue;
                            }
                            let registration = if awaiting_catalogue_admission {
                                SubscriberShapeRegistration::PendingCatalogueAdmission(opts)
                            } else {
                                SubscriberShapeRegistration::Registered(opts)
                            };
                            shape_registrations.insert(registration_key, registration);
                        }
                        SyncMessage::Subscribe(subscribe) => {
                            // Subscription admission has a substantially larger async state
                            // machine than ordinary peer messages. Keep that state on the heap
                            // so a commit uploaded on this same connection does not carry the
                            // inactive Subscribe arm on a normal two-megabyte executor stack.
                            let should_continue = Box::pin(async {
                            if let Err(message) =
                                validate_known_state_declaration(&subscribe.known_state)
                            {
                                let _ = message;
                                drop_peer_request(&self.node);
                                return Ok::<bool, Error>(true);
                            }
                            let shape_id = subscribe.shape_id;
                            let subscription = subscribe.subscription;
                            let values = subscribe.values.clone();
                            let known_state = subscribe.known_state.clone();
                            let registration_key = (shape_id, subscription.read_view);
                            let Some(registration) =
                                shape_registrations.get(&registration_key).cloned()
                            else {
                                drop_peer_request(&self.node);
                                return Ok::<bool, Error>(true);
                            };
                            let pending_catalogue_admission = matches!(
                                &registration,
                                SubscriberShapeRegistration::PendingCatalogueAdmission(_)
                            );
                            let opts = match registration {
                                SubscriberShapeRegistration::RejectedUnsupportedCapability(
                                    detail,
                                ) => {
                                    // Keep the original permanent rejection, but let views
                                    // already served by this connection flush first. A rejected
                                    // shape must not starve unrelated subscriptions.
                                    deferred_subscribe_rejections.push_back((subscription, detail));
                                    return Ok::<bool, Error>(true);
                                }
                                SubscriberShapeRegistration::Registered(opts)
                                | SubscriberShapeRegistration::PendingCatalogueAdmission(opts) => {
                                    opts
                                }
                            };
                            let Some(shape) = self.node.borrow().registered_shape(shape_id) else {
                                if pending_catalogue_admission {
                                    self.transport
                                        .send(SyncMessage::SubscribeRejected {
                                            subscription,
                                            reason: SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission,
                                        })
                                        .map_err(transport_error)?;
                                } else {
                                    drop_peer_request(&self.node);
                                }
                                return Ok::<bool, Error>(true);
                            };
                            if pending_catalogue_admission {
                                shape_registrations.insert(
                                    registration_key,
                                    SubscriberShapeRegistration::Registered(opts.clone()),
                                );
                            }
                            let value_map = shape
                                .params()
                                .keys()
                                .cloned()
                                .zip(values)
                                .collect::<BTreeMap<_, _>>();
                            let binding = match shape.bind(value_map) {
                                Ok(binding) => binding,
                                Err(_) => {
                                    drop_peer_request(&self.node);
                                    return Ok::<bool, Error>(true);
                                }
                            };
                            if ensure_supported_register_shape_options(
                                &opts,
                                *local_receiver,
                                peer.role(),
                            )
                            .is_err()
                            {
                                drop_peer_request(&self.node);
                                return Ok::<bool, Error>(true);
                            }
                            let scope_purpose = if let Some(purpose) = scope_purpose {
                                let expected_result =
                                    self.node.borrow().authorization_support_scope(
                                        ingest_context.identity,
                                        &purpose.action,
                                    );
                                let expected = match expected_result {
                                    Ok(expected) => expected,
                                    Err(_) => {
                                        drop_peer_request(&self.node);
                                        return Ok::<bool, Error>(true);
                                    }
                                };
                                let exact_support = subscription.shape_id == shape.shape_id()
                                    && subscription.binding_id == binding.binding_id()
                                    && authorization_scope_support_options_match(
                                        &expected.options,
                                        &opts,
                                        subscription,
                                    )
                                    && expected.subscriptions.iter().any(
                                        |(expected_shape, expected_binding)| {
                                            expected_shape.shape_id() == shape.shape_id()
                                                && expected_binding.binding_id()
                                                    == binding.binding_id()
                                        },
                                    );
                                if !exact_support {
                                    drop_peer_request(&self.node);
                                    return Ok::<bool, Error>(true);
                                }
                                Some(AuthorizedScopePurpose {
                                    key: expected.key,
                                    operation: expected.operation,
                                    action: purpose.action,
                                    expected_support: expected
                                        .subscriptions
                                        .iter()
                                        .map(|(shape, binding)| {
                                            (shape.shape_id(), binding.binding_id())
                                        })
                                        .collect(),
                                })
                            } else {
                                None
                            };
                            if let Some(purpose) = &scope_purpose
                                && let Some(existing) = scope_purposes.get(&subscription)
                                && existing != purpose
                            {
                                drop_peer_request(&self.node);
                                return Ok::<bool, Error>(true);
                            }
                            let supported = self
                                .node
                                .lock()
                                .await
                                .ensure_peer_maintained_subscription_view_supported(
                                    &shape,
                                    &binding,
                                    opts.tier,
                                    subscriber_permission_subject(*ingest_context),
                                    &opts.read_view,
                                    QueryAuthorizationMode::TrustedServing,
                                )
                                .await;
                            if let Err(crate::node::Error::QueryCapability(detail)) = supported {
                                send_unsupported_shape_capability_rejection(
                                    &mut *self.transport,
                                    subscription,
                                    detail,
                                )
                                .map_err(transport_error)?;
                                return Ok::<bool, Error>(true);
                            } else if let Err(error) = supported {
                                reject_server_subscription_failure(
                                    &mut *self.transport,
                                    subscription,
                                    &error,
                                )
                                .map_err(transport_error)?;
                                return Ok::<bool, Error>(true);
                            }
                            let coverage = coverage_key(&shape, &binding, opts.clone());
                            let group_subscription = SubscriptionKey {
                                shape_id: coverage.shape_id,
                                binding_id: coverage.binding_id,
                                read_view: coverage.opts.read_view_key(),
                            };
                            let local_subscriber = *local_receiver;
                            let upstream_opts = if local_subscriber {
                                upstream_register_shape_options(
                                    opts.tier,
                                    opts.read_view.clone(),
                                    DurabilityTier::Global,
                                    opts.propagate_upstream,
                                )
                            } else {
                                opts.clone()
                            };
                            // This is an upstream usage-site handle, not the
                            // canonical binding id. Distinct downstream views
                            // can normalize to identical Global coverage, but
                            // each retains independent subscribe/unsubscribe
                            // ownership. The upstream node's CoverageKey groups
                            // them onto one evaluator without conflating their
                            // wire lifecycles.
                            let upstream_subscription = relay_upstream_subscription_key(
                                connection_epoch,
                                subscription,
                                upstream_opts.read_view_key(),
                            );
                            let first_subscriber = coverage_groups
                                .get(&coverage)
                                .is_none_or(|group| group.subscribers.is_empty());
                            let permissions_ready = subscriber_permissions_ready(
                                self.node.borrow().permissions_ready(),
                                ingest_context.trust,
                            );
                            let local_waiting_for_upstream_settlement = local_subscriber
                                && opts.propagate_upstream
                                && opts.tier > DurabilityTier::Local
                                && !self.node.borrow().has_settled_result_set(BindingViewKey {
                                    shape_id: shape.shape_id(),
                                    binding_id: binding.binding_id(),
                                    read_view: upstream_opts.read_view_key(),
                                });
                            let opening_pending = if !permissions_ready {
                                Some(SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                                    subscription,
                                    settled_through: self.node.borrow().committed_global_time(),
                                    reset_result_set: true,
                                    version_carriers: Vec::new(),
                                    version_bundles: Vec::new(),
                                    peer_payload_inventory: crate::protocol::PeerPayloadInventory {
                                        opening_pending: true,
                                        ..Default::default()
                                    },
                                    result_member_adds: Vec::new(),
                                    result_member_removes: Vec::new(),
                                    terminal_operations: Vec::new(),
                                    program_fact_adds: Vec::new(),
                                    program_fact_removes: Vec::new(),
                                }))
                            } else {
                                None
                            };
                            // Known state describes this concrete receiver, not
                            // the canonical coverage evaluator shared with its
                            // sibling usage subscriptions. Local nodes may still
                            // withhold delivery pending upstream settlement, but
                            // the cursor retains the same usage-site ownership.
                            peer.declare_known_state(subscription, known_state.clone());
                            let outcome = self
                                .node
                                .lock()
                                .await
                                .apply_sync_message(SyncMessage::Subscribe(subscribe))
                                .await?;
                            let (_, changed, published) = finish_peer_publication_outcome_with_refresh(
                                &self.node,
                                &self.subscriptions,
                                &self.active_authority_view_receipts,
                                outcome,
                                false,
                            )
                            .await?;
                            stats.subscription_events += changed;
                            needs_subscription_refresh |= published;
                            if let Some(purpose) = scope_purpose {
                                let aggregate = scope_aggregates
                                    .entry(purpose.key.clone())
                                    .or_insert_with(|| {
                                        AuthorityScopeAggregate::new(
                                            purpose.expected_support.clone(),
                                        )
                                    });
                                if aggregate.expected_support() != &purpose.expected_support
                                    || !aggregate.register(
                                        subscription,
                                        (shape.shape_id(), binding.binding_id()),
                                    )
                                {
                                    drop_peer_request(&self.node);
                                    return Ok::<bool, Error>(true);
                                }
                                scope_purposes.insert(subscription, purpose);
                            }
                            let group =
                                coverage_groups.entry(coverage.clone()).or_insert_with(|| {
                                    CoverageGroup {
                                        shape: shape.clone(),
                                        binding: binding.clone(),
                                        subscribers: BTreeSet::new(),
                                        pending_initial_subscribers: BTreeSet::new(),
                                        initialized: false,
                                        upstream_subscription,
                                        upstream_opts: upstream_opts.clone(),
                                        awaiting_upstream_settlement:
                                            local_waiting_for_upstream_settlement,
                                    }
                                });
                            group.subscribers.insert(subscription);
                            group.pending_initial_subscribers.insert(subscription);
                            if group.upstream_opts.propagate_upstream {
                                let owner = RelayUpstreamSubscriptionOwner {
                                    downstream_connection_epoch: connection_epoch,
                                    coverage: coverage.clone(),
                                    downstream_subscriptions: BTreeSet::from([subscription]),
                                };
                                self.relay_upstream_subscription_owners
                                    .borrow_mut()
                                    .entry(group.upstream_subscription)
                                    .and_modify(|existing| {
                                        debug_assert_eq!(
                                            existing.downstream_connection_epoch,
                                            connection_epoch,
                                            "relay upstream handle changed downstream owner"
                                        );
                                        debug_assert_eq!(
                                            existing.coverage, coverage,
                                            "relay upstream handle changed coverage group"
                                        );
                                        existing.downstream_subscriptions.insert(subscription);
                                    })
                                    .or_insert(owner);
                            }
                            served.insert(subscription, coverage);
                            if let Some(mut update) = opening_pending {
                                stamp_view_update_authorization_progress_from(
                                    peer,
                                    group_subscription,
                                    &mut update,
                                );
                                #[cfg(feature = "sync-autopsy")]
                                sync_autopsy::record(format!(
                                    "subscriber send rehydrate {}",
                                    summarize_sync_message(&update)
                                ));
                                self.last_resume_bytes = Some(serialized_sync_message_len(&update));
                                let receipt =
                                    scope_purposes.get(&subscription).and_then(|purpose| {
                                        aggregate_authorization_scope_receipt_for_view(
                                            scope_aggregates,
                                            &self.node.borrow(),
                                            peer,
                                            ingest_context.identity,
                                            connection_epoch,
                                            purpose,
                                            &update,
                                        )
                                    });
                                send_with_sync_context(
                                    &self.node,
                                    peer,
                                    self.transport.as_mut(),
                                    update,
                                )?;
                                if let Some((subscription, receipt)) = receipt {
                                    self.transport
                                        .send(SyncMessage::AuthorizationScopeReceipt {
                                            subscription,
                                            receipt,
                                        })
                                        .map_err(transport_error)?;
                                }
                                sent_view_update = true;
                            }
                            if first_subscriber && group.upstream_opts.propagate_upstream {
                                upstream_subscriptions.borrow_mut().push(
                                    PendingUpstreamCommand::Subscribe(
                                        PendingUpstreamSubscription {
                                            subscription: group.upstream_subscription,
                                            shape: shape.clone(),
                                            binding,
                                            opts: group.upstream_opts.clone(),
                                            identity: peer.link_identity(),
                                        },
                                    ),
                                );
                            }
                            schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                            scheduled_immediate = true;
                            Ok::<bool, Error>(false)
                            })
                            .await?;
                            if should_continue {
                                continue;
                            }
                        }
                        SyncMessage::Unsubscribe { subscription } => {
                            self.node.borrow_mut().apply_unsubscribe(subscription);
                            if let Some(purpose) = scope_purposes.remove(&subscription) {
                                remove_scope_aggregate_member(
                                    scope_aggregates,
                                    &purpose.key,
                                    subscription,
                                );
                            }
                            if let Some(coverage) = served.remove(&subscription) {
                                if let Some(group) = coverage_groups.get_mut(&coverage) {
                                    group.subscribers.remove(&subscription);
                                    group.pending_initial_subscribers.remove(&subscription);
                                    if group.upstream_opts.propagate_upstream {
                                        if let Some(owner) = self
                                            .relay_upstream_subscription_owners
                                            .borrow_mut()
                                            .get_mut(&group.upstream_subscription)
                                            && owner.downstream_connection_epoch == connection_epoch
                                            && owner.coverage == coverage
                                        {
                                            owner.downstream_subscriptions.remove(&subscription);
                                        }
                                    }
                                    if group.subscribers.is_empty() {
                                        let upstream_subscription = group.upstream_subscription;
                                        let propagated_upstream =
                                            group.upstream_opts.propagate_upstream;
                                        let group_subscription = SubscriptionKey {
                                            shape_id: coverage.shape_id,
                                            binding_id: coverage.binding_id,
                                            read_view: coverage.opts.read_view_key(),
                                        };
                                        // A coverage group owns a maintained Groove receiver.
                                        // Forgetting only the peer-side cursor leaves that
                                        // receiver dormant in the shared runtime; a later
                                        // re-open of the same logical coverage then races a
                                        // stale source subscription instead of observing the
                                        // current authority membership. Tear down both pieces
                                        // together when the final usage site goes away.
                                        peer.forget_subscription_with_node(
                                            &mut self.node.borrow_mut(),
                                            group_subscription,
                                        );
                                        coverage_groups.remove(&coverage);
                                        if propagated_upstream {
                                            if retire_relay_upstream_subscription(
                                                &self.relay_upstream_subscription_owners,
                                                upstream_subscription,
                                                connection_epoch,
                                                &coverage,
                                            )
                                            .is_some()
                                            {
                                                upstream_subscriptions.borrow_mut().push(
                                                    PendingUpstreamCommand::Unsubscribe(
                                                        upstream_subscription,
                                                    ),
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        SyncMessage::FetchRowVersions { requests } => {
                            if let Err(message) = validate_fetch_row_versions(&requests) {
                                let _ = message;
                                drop_peer_request(&self.node);
                                continue;
                            }
                            let responses = {
                                let mut node = self.node.lock().await;
                                peer.serve_row_versions(&mut node, &requests).await?
                            };
                            for response in responses {
                                send_with_sync_context(
                                    &self.node,
                                    peer,
                                    self.transport.as_mut(),
                                    response,
                                )?;
                            }
                        }
                        other => {
                            if matches!(other, SyncMessage::SessionClaims { .. })
                                && ingest_context.trust == CommitUnitTrust::Session
                            {
                                // Claims are fixed when the host admits or resumes this
                                // connection. A subscriber can otherwise self-assert a
                                // broader policy context after authentication.
                                drop_peer_request(&self.node);
                                continue;
                            }
                            let local_upload = match &other {
                                SyncMessage::CommitUnit { tx, .. } => {
                                    Some((tx.tx_id, other.clone()))
                                }
                                _ => None,
                            };
                            if let Some((tx_id, _)) = &local_upload
                                && !*local_receiver
                                && !subscriber_permissions_ready(
                                    self.node.borrow().permissions_ready(),
                                    ingest_context.trust,
                                )
                            {
                                let response = SyncMessage::FateUpdate {
                                    tx_id: *tx_id,
                                    fate: Fate::Rejected(RejectionReason::MalformedCommit(
                                        "permissions_head_missing: no published permissions head"
                                            .to_owned(),
                                    )),
                                    global_time: None,
                                    durability: None,
                                };
                                send_with_sync_context(
                                    &self.node,
                                    peer,
                                    self.transport.as_mut(),
                                    response,
                                )?;
                                continue;
                            }
                            let write_state_tx_id = write_state_update_tx_id(&other);
                            // RegisterShape (registers the shape ahead of its
                            // binding), plus the write-upload path: any
                            // responses (e.g. fate updates) flow back to the
                            // subscriber.
                            let outcome = dispatch_admitted_subscriber_message(
                                &self.node,
                                peer,
                                *local_receiver,
                                *ingest_context,
                                &self.admitted_upstream_authority,
                                &self.edge_fate_routes,
                                &self.local_fate_routes,
                                &self.downstream_fates,
                                other,
                            )
                            .await?;
                            let (responses, changed, published) = finish_peer_publication_outcome_with_refresh(
                                &self.node,
                                &self.subscriptions,
                                &self.active_authority_view_receipts,
                                outcome,
                                false,
                            )
                            .await?;
                            stats.subscription_events += changed;
                            needs_subscription_refresh |= published;
                            if let Some(tx_id) = write_state_tx_id {
                                handle_write_state_update(
                                    &self.node,
                                    &self.write_state_waiters,
                                    &self.mutation_errors,
                                    &self.scheduler,
                                    tx_id,
                                );
                            }
                            for response in responses {
                                send_with_sync_context(
                                    &self.node,
                                    peer,
                                    self.transport.as_mut(),
                                    response,
                                )?;
                            }
                            if let Some((tx_id, unit)) = local_upload {
                                let mut outbox = outbox.borrow_mut();
                                if outbox.push(PendingUpload {
                                        tx_id,
                                        unit: Some(unit),
                                    }) {
                                    schedule_tick_in(&self.scheduler, TickUrgency::Deferred);
                                }
                            }
                        }
                    }
                }
                queue_local_acknowledgements(&self.local_fate_routes, &self.node).await;
                for fate in std::mem::take(&mut *self.downstream_fates.borrow_mut()) {
                    send_with_sync_context(&self.node, peer, self.transport.as_mut(), fate)?;
                }
                if needs_subscription_refresh {
                    stats.subscription_events += refresh_subscriptions_in(
                        &self.node,
                        &self.subscriptions,
                        &self.active_authority_view_receipts,
                    )
                    .await?;
                }
                if applied_inbound && !scheduled_immediate {
                    schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                }
                if applied_inbound {
                    let next = self.subscriber_dirty_epoch.get().wrapping_add(1);
                    self.subscriber_dirty_epoch.set(next);
                    self.observed_subscriber_dirty_epoch.set(next);
                    *serve_dirty = true;
                    // Inbound admission, resident publication, and persistence
                    // form one owner-loop turn. Cold downstream view assembly
                    // is a separately scheduled turn so it cannot withhold the
                    // writer's durability receipt or later cancellation/flush
                    // traffic on this connection.
                    schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                    return Ok(true);
                }
                if *serve_dirty
                    && subscriber_permissions_ready(
                        self.node.borrow().permissions_ready(),
                        ingest_context.trust,
                    )
                {
                    let mut serve_again = false;
                    for (coverage, group) in coverage_groups.iter_mut() {
                        let group_subscription = SubscriptionKey {
                            shape_id: coverage.shape_id,
                            binding_id: coverage.binding_id,
                            read_view: coverage.opts.read_view_key(),
                        };
                        let settled_handoff = group.awaiting_upstream_settlement
                            && self.node.borrow().has_settled_result_set(BindingViewKey {
                                shape_id: group.shape.shape_id(),
                                binding_id: group.binding.binding_id(),
                                read_view: group.upstream_opts.read_view_key(),
                            });
                        if group.awaiting_upstream_settlement && !settled_handoff {
                            continue;
                        }
                        let pending_initial = std::mem::take(
                            &mut group.pending_initial_subscribers,
                        );
                        let serving_initial = !pending_initial.is_empty();
                        for subscription in pending_initial {
                            let update_result = {
                                let mut node = self.node.lock().await;
                                if group.initialized
                                    || peer.has_maintained_subscription(group_subscription)
                                {
                                    peer.rehydrate_query_for_subscription_from_maintained_subscription(
                                        &mut node,
                                        group_subscription,
                                        subscription,
                                        &group.shape,
                                    )
                                    .await
                                } else {
                                    peer.rehydrate_query_for_subscription_with_opts(
                                        &mut node,
                                        group_subscription,
                                        &group.shape,
                                        &group.binding,
                                        coverage.opts.clone(),
                                    )
                                    .await
                                    .map(|update| {
                                        update.map(|update| {
                                            retarget_view_update(update, subscription)
                                        })
                                    })
                                }
                            };
                            let mut update = match update_result {
                                Ok(Some(update)) => update,
                                Ok(None) => {
                                    group.pending_initial_subscribers.insert(subscription);
                                    serve_again = true;
                                    continue;
                                }
                                Err(crate::node::Error::QueryCapability(detail)) => {
                                    send_unsupported_shape_capability_rejection(
                                        &mut *self.transport,
                                        subscription,
                                        detail,
                                    )
                                    .map_err(transport_error)?;
                                    continue;
                                }
                                Err(error) => {
                                    reject_server_subscription_failure(
                                        &mut *self.transport,
                                        subscription,
                                        &error,
                                    )
                                    .map_err(transport_error)?;
                                    continue;
                                }
                            };
                            group.initialized = true;
                            stamp_view_update_authorization_progress_from(
                                peer,
                                group_subscription,
                                &mut update,
                            );
                            self.last_resume_bytes =
                                Some(serialized_sync_message_len(&update));
                            let receipt = scope_purposes.get(&subscription).and_then(|purpose| {
                                aggregate_authorization_scope_receipt_for_view(
                                    scope_aggregates,
                                    &self.node.borrow(),
                                    peer,
                                    ingest_context.identity,
                                    connection_epoch,
                                    purpose,
                                    &update,
                                )
                            });
                            send_with_sync_context(
                                &self.node,
                                peer,
                                self.transport.as_mut(),
                                update,
                            )?;
                            if let Some((subscription, receipt)) = receipt {
                                self.transport
                                    .send(SyncMessage::AuthorizationScopeReceipt {
                                        subscription,
                                        receipt,
                                    })
                                    .map_err(transport_error)?;
                            }
                            sent_view_update = true;
                        }
                        if serving_initial {
                            continue;
                        }
                        let update_result = {
                            let mut node = self.node.lock().await;
                            if settled_handoff {
                                peer.rehydrate_query_for_subscription_with_opts(
                                    &mut node,
                                    group_subscription,
                                    &group.shape,
                                    &group.binding,
                                    coverage.opts.clone(),
                                )
                                .await
                            } else {
                                peer.query_update_for_subscription_with_opts(
                                    &mut node,
                                    group_subscription,
                                    &group.shape,
                                    &group.binding,
                                    coverage.opts.clone(),
                                )
                                .await
                            }
                        };
                        let update = match update_result {
                            Ok(Some(update)) => update,
                            Ok(None) => {
                                serve_again = true;
                                continue;
                            }
                            Err(error) => {
                                for subscription in group.subscribers.iter().copied() {
                                    reject_server_subscription_failure(
                                        &mut *self.transport,
                                        subscription,
                                        &error,
                                    )
                                    .map_err(transport_error)?;
                                }
                                continue;
                            }
                        };
                        if settled_handoff {
                            group.awaiting_upstream_settlement = false;
                        }
                        if settled_handoff || !view_update_is_empty(&update) {
                            #[cfg(feature = "sync-autopsy")]
                            sync_autopsy::record(format!(
                                "subscriber generated group delta group={} update={}",
                                summarize_subscription_key(group_subscription),
                                summarize_sync_message(&update)
                            ));
                            for subscription in group.subscribers.iter().copied() {
                                let mut update = retarget_view_update(update.clone(), subscription);
                                stamp_view_update_authorization_progress_from(
                                    peer,
                                    group_subscription,
                                    &mut update,
                                );
                                let receipt =
                                    scope_purposes.get(&subscription).and_then(|purpose| {
                                        aggregate_authorization_scope_receipt_for_view(
                                            scope_aggregates,
                                            &self.node.borrow(),
                                            peer,
                                            ingest_context.identity,
                                            connection_epoch,
                                            purpose,
                                            &update,
                                        )
                                    });
                                #[cfg(feature = "sync-autopsy")]
                                sync_autopsy::record(format!(
                                    "subscriber send group delta {}",
                                    summarize_sync_message(&update)
                                ));
                                send_with_sync_context(
                                    &self.node,
                                    peer,
                                    self.transport.as_mut(),
                                    update,
                                )?;
                                if let Some((subscription, receipt)) = receipt {
                                    self.transport
                                        .send(SyncMessage::AuthorizationScopeReceipt {
                                            subscription,
                                            receipt,
                                        })
                                        .map_err(transport_error)?;
                                }
                                sent_view_update = true;
                            }
                        }
                    }
                    for table in served_current_rows.values() {
                        let update = {
                            let mut node = self.node.lock().await;
                            peer.current_rows_update(&mut node, table).await?
                        };
                        if !view_update_is_empty(&update) {
                            send_with_sync_context(
                                &self.node,
                                peer,
                                self.transport.as_mut(),
                                update,
                            )?;
                            sent_view_update = true;
                        }
                    }
                    *serve_dirty = serve_again;
                    if serve_again {
                        schedule_tick_in(&self.scheduler, TickUrgency::Deferred);
                    }
                }
                if sent_view_update {
                    while let Some((subscription, detail)) =
                        deferred_subscribe_rejections.pop_front()
                    {
                        send_unsupported_shape_capability_rejection(
                            &mut *self.transport,
                            subscription,
                            detail,
                        )
                        .map_err(transport_error)?;
                    }
                }
                    Ok::<bool, Error>(false)
                })
                .await?;
                if stop {
                    return Ok(stats);
                }
            }
        }
        Ok(stats)
    }
}

impl<S> PeerConnection<S>
where
    S: OrderedKvStorage,
{
    pub(super) fn mark_subscriber_dirty(&mut self) -> bool {
        if let ConnectionLink::Subscriber(SubscriberConnectionState { serve_dirty, .. }) =
            &mut self.link
        {
            *serve_dirty = true;
            self.observed_subscriber_dirty_epoch
                .set(self.subscriber_dirty_epoch.get());
            true
        } else {
            false
        }
    }

    fn observe_shared_subscriber_dirty_epoch(&mut self) {
        let epoch = self.subscriber_dirty_epoch.get();
        if self.observed_subscriber_dirty_epoch.get() == epoch {
            return;
        }
        self.observed_subscriber_dirty_epoch.set(epoch);
        if let ConnectionLink::Subscriber(SubscriberConnectionState { serve_dirty, .. }) =
            &mut self.link
        {
            *serve_dirty = true;
        }
    }

    pub(super) fn eviction_pins(&self) -> crate::peer::PeerEvictionPins {
        match &self.link {
            ConnectionLink::Subscriber(SubscriberConnectionState { peer, .. }) => {
                peer.eviction_pins()
            }
            ConnectionLink::Upstream(_) => crate::peer::PeerEvictionPins::default(),
        }
    }
}

pub(super) fn schedule_tick_in(scheduler: &SharedTickScheduler, urgency: TickUrgency) {
    if let Some(scheduler) = scheduler.borrow().as_ref() {
        scheduler.schedule_tick(urgency);
    }
}

fn serialized_sync_message_len(message: &SyncMessage) -> usize {
    #[cfg(feature = "cold-settle-attribution")]
    let started = Instant::now();
    let encoded = encode_sync_message(message);
    #[cfg(feature = "cold-settle-attribution")]
    crate::cold_settle_attribution::record_preflight_payload(
        started.elapsed().as_nanos() as u64,
        encoded.as_ref().map_or(0, Vec::len),
    );
    encoded.map_or(0, |bytes| bytes.len())
}

fn view_update_parts_from_message(message: SyncMessage) -> ViewUpdateParts {
    match message {
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through,
            reset_result_set,
            version_carriers,
            version_bundles,
            peer_payload_inventory,
            result_member_adds,
            result_member_removes,
            terminal_operations,
            program_fact_adds,
            program_fact_removes,
        }) => ViewUpdateParts {
            subscription,
            settled_through,
            defer_settlement: false,
            reset_result_set,
            version_carriers,
            version_bundles,
            peer_complete_tx_payload_refs: peer_payload_inventory.complete_tx_payloads,
            authorization_progress: peer_payload_inventory.authorization_progress,
            opening_pending: peer_payload_inventory.opening_pending,
            result_member_adds,
            result_member_removes,
            terminal_operations,
            program_fact_adds,
            program_fact_removes,
        },
        _ => unreachable!("expected view update message"),
    }
}

fn push_view_update_message_for_receiver(
    ready: &mut Vec<PendingAuthorityViewUpdate>,
    message: SyncMessage,
    authority_receipt_eligible: bool,
) -> Result<(), Error> {
    ready.push(PendingAuthorityViewUpdate {
        parts: view_update_parts_from_message(message),
        authority_receipt_eligible,
    });
    Ok(())
}

fn stage_initial_coverage_clear_for_update(
    update: &SyncMessage,
    latest: &LatestCoverageSubscriptions,
    clears: &mut BTreeSet<CoverageKey>,
) {
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        peer_payload_inventory,
        ..
    }) = update
    else {
        return;
    };
    if peer_payload_inventory.opening_pending {
        return;
    }
    if let Some(coverage) = latest
        .borrow()
        .iter()
        .find_map(|(coverage, current)| (*current == *subscription).then(|| coverage.clone()))
    {
        clears.insert(coverage);
    }
}

async fn apply_pending_authority_view_updates<S>(
    node: &SharedNodeState<S>,
    pending: &mut Vec<PendingAuthorityViewUpdate>,
    awaiting: &AwaitingInitialAuthorityCoverage,
    clears: &mut BTreeSet<CoverageKey>,
    active_authority_view_receipts: &ActiveAuthorityViewReceipts,
    connection_epoch: u64,
) -> Result<(), Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let confirmed_subscriptions = pending
        .iter()
        .filter(|update| update.authority_receipt_eligible && !update.parts.opening_pending)
        .map(|update| (update.parts.subscription, update.parts.settled_through))
        .collect::<Vec<_>>();
    let batch_cut = pending
        .iter()
        .map(|update| update.parts.settled_through)
        .max()
        .unwrap_or_default();
    let ineligible_cut = pending
        .iter()
        .filter(|update| !update.authority_receipt_eligible)
        .map(|update| update.parts.settled_through)
        .max();
    let node_ref = node.borrow();
    let confirmed_binding_views = confirmed_subscriptions
        .into_iter()
        .filter_map(|(subscription, settled_through)| {
            node_ref
                .binding_view_key_for_subscription(subscription)
                .ok()
                .map(|binding_view| (binding_view, settled_through))
        })
        .collect::<Vec<_>>();
    drop(node_ref);
    {
        let mut active_receipts = active_authority_view_receipts.borrow_mut();
        if let Some(receipts) = active_receipts.as_mut() {
            let invalidation_cut = if receipts.connection_epoch != connection_epoch {
                Some(batch_cut)
            } else {
                ineligible_cut
            };
            if let Some(invalidation_cut) = invalidation_cut {
                // A nonselected upstream update may recompute binding views beyond
                // the wire subscription it names. Fallback-staged updates are
                // likewise ineligible even after their link becomes selected.
                // Until that dependency closure is proven exact, no receipt
                // remains safe and later confirmation must reach this cut.
                receipts.binding_views.clear();
                receipts.confirmation_floor = receipts.confirmation_floor.max(invalidation_cut);
            }
        }
    }
    let updates = std::mem::take(pending)
        .into_iter()
        .map(|update| update.parts)
        .collect::<Vec<_>>();
    let mut node_ref = node.lock().await;
    node_ref.apply_view_updates_in_batch(updates).await?;
    drop(node_ref);
    if let Some(receipts) = active_authority_view_receipts.borrow_mut().as_mut()
        && receipts.connection_epoch == connection_epoch
    {
        receipts
            .binding_views
            .extend(confirmed_binding_views.into_iter().filter_map(
                |(binding_view, settled_through)| {
                    (settled_through >= receipts.confirmation_floor).then_some(binding_view)
                },
            ));
    }
    if !clears.is_empty() {
        let mut awaiting = awaiting.borrow_mut();
        for coverage in std::mem::take(clears) {
            awaiting.remove(&coverage);
        }
    }
    Ok(())
}

fn transport_error(error: TransportError) -> Error {
    match error {
        TransportError::Backpressure => {
            Error::new(ErrorCode::Backpressure, "transport backpressure")
        }
        TransportError::Failed(message) => Error::new(ErrorCode::Protocol, message),
    }
}

async fn evaluate_authoritative_permission_advice<S>(
    node: &mut NodeState<S>,
    identity: AuthorSubject,
    action: PermissionAdviceAction,
) -> PermissionAdvice
where
    S: OrderedKvStorage,
{
    let result = match action {
        PermissionAdviceAction::Insert { table, cells } => {
            let id_dependent = node.table(&table).map(|schema| {
                schema
                    .write_policies
                    .insert_check
                    .as_ref()
                    .is_some_and(query_root_filters_reference_id)
            });
            match id_dependent {
                Ok(true) => return PermissionAdvice::Unknown,
                Err(_) => return PermissionAdvice::Unknown,
                Ok(false) => {
                    node.dry_run_insert_allows(
                        MergeableCommit::new(table, RowUuid::from_bytes([0; 16]), 0)
                            .made_by(identity)
                            .permission_subject(identity)
                            .cells(cells),
                    )
                    .await
                }
            }
        }
        PermissionAdviceAction::Read { table, row } => {
            node.dry_run_read_current_allows(&table, row, identity)
                .await
        }
        PermissionAdviceAction::Update { table, row, patch } => {
            match node.current_rows(&table, DurabilityTier::Local).await {
                Ok(rows) if rows.iter().any(|current| current.row_uuid() == row) => {
                    node.dry_run_insert_allows(
                        MergeableCommit::new(table, row, 0)
                            .made_by(identity)
                            .permission_subject(identity)
                            .cells(patch),
                    )
                    .await
                }
                Ok(_) => Ok(false),
                Err(error) => Err(error),
            }
        }
        PermissionAdviceAction::Delete { table, row } => {
            node.dry_run_delete_current_allows(&table, row, identity)
                .await
        }
    };
    match result {
        Ok(true) => PermissionAdvice::Allowed,
        Ok(false) => PermissionAdvice::Denied,
        Err(_) => PermissionAdvice::Unknown,
    }
}

/// Compile and serve an authorization scope entirely at the serving authority.
///
/// This intentionally does not reuse the subscriber's shape registry or any
/// caller-provided subscription.  The authority allocates opaque usage-site
/// keys, registers canonical shapes in the receiver, and only then sends the
/// ordinary view updates in authority-scope envelopes.
async fn serve_authorization_scope_intent<S>(
    node: &SharedNodeState<S>,
    peer: &mut PeerState,
    transport: &mut dyn Transport,
    identity: AuthorSubject,
    connection_epoch: u64,
    request_id: PermissionAdviceRequestId,
    action: PermissionAdviceAction,
    trust: CommitUnitTrust,
    hydrations: &mut BTreeMap<
        crate::protocol::AuthorizationSupportScopeKey,
        ServedAuthorizationScopeHydration,
    >,
    hydration_count: &mut u64,
) -> Result<(), Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    if !node.borrow().is_history_complete()
        || !subscriber_permissions_ready(node.borrow().permissions_ready(), trust)
    {
        transport
            .send(SyncMessage::AuthorizationScopeUnavailable { request_id })
            .map_err(transport_error)?;
        return Ok(());
    }
    let scope = match node.borrow().authorization_support_scope(identity, &action) {
        Ok(scope) => scope,
        Err(_) => {
            transport
                .send(SyncMessage::AuthorizationScopeUnavailable { request_id })
                .map_err(transport_error)?;
            return Ok(());
        }
    };
    let mut seen_support = BTreeSet::new();
    let support_clauses = scope
        .subscriptions
        .iter()
        .filter(|(shape, binding)| seen_support.insert((shape.shape_id(), binding.binding_id())))
        .collect::<Vec<_>>();
    let clause_count = u16::try_from(support_clauses.len()).map_err(|_| {
        Error::new(
            ErrorCode::Protocol,
            "authorization scope has too many clauses",
        )
    })?;
    if clause_count == 0 {
        let advice = {
            let mut node = node.lock().await;
            evaluate_authoritative_permission_advice(&mut node, identity, action).await
        };
        transport
            .send(SyncMessage::AuthorizationScopeDecision { request_id, advice })
            .map_err(transport_error)?;
        return Ok(());
    }
    let current_claims_revision = node.borrow().session_claim_revision(identity);
    let current_policy_epoch = node.borrow().active_catalogue_seq();
    let current_cut = node.borrow().committed_global_time();
    // A cache entry is evidence, not a generic response cache.  Prune every
    // revision/cut mismatch before looking up this compiled support key.
    hydrations.retain(|_, hydration| {
        hydration.receipt.claims_revision == current_claims_revision
            && hydration.receipt.policy_epoch == current_policy_epoch
            && hydration.receipt.settled_through == current_cut
    });
    if let Some(hydration) = hydrations.get(&scope.key) {
        for (index, clause) in hydration.clauses.iter().enumerate() {
            transport
                .send(clause.register.clone())
                .map_err(transport_error)?;
            transport
                .send(clause.subscribe.clone())
                .map_err(transport_error)?;
            transport
                .send(SyncMessage::AuthorizationScopeView {
                    request_id,
                    key: scope.key.clone(),
                    clause_index: index as u16,
                    clause_count,
                    view: crate::protocol::ViewUpdatePayload::from_view_update(clause.view.clone())
                        .expect("authority scope clauses are view updates"),
                })
                .map_err(transport_error)?;
        }
        transport
            .send(SyncMessage::AuthorizationScopeAggregateReceipt {
                request_id,
                receipt: hydration.receipt.clone(),
            })
            .map_err(transport_error)?;
        for clause in &hydration.clauses {
            transport
                .send(SyncMessage::Unsubscribe {
                    subscription: clause.subscription,
                })
                .map_err(transport_error)?;
        }
        return Ok(());
    }
    *hydration_count = hydration_count.saturating_add(1);
    let mut aggregate = AuthorityScopeAggregate::new(
        support_clauses
            .iter()
            .map(|(shape, binding)| (shape.shape_id(), binding.binding_id()))
            .collect(),
    );
    let mut support_subscriptions = Vec::new();
    let mut served_clauses = Vec::new();
    for (index, (shape, binding)) in support_clauses.iter().enumerate() {
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            // This usage-site key belongs to the authority, and cannot be
            // inferred from the canonical binding identity.
            binding_id: crate::query::BindingId(uuid::Uuid::new_v4()),
            read_view: scope.options.read_view_key(),
        };
        if !aggregate.register(subscription, (shape.shape_id(), binding.binding_id())) {
            transport
                .send(SyncMessage::AuthorizationScopeUnavailable { request_id })
                .map_err(transport_error)?;
            return Ok(());
        }
        let supported = node
            .lock()
            .await
            .ensure_peer_maintained_subscription_view_supported(
                shape,
                binding,
                scope.options.tier,
                identity,
                &scope.options.read_view,
                QueryAuthorizationMode::TrustedServing,
            )
            .await;
        if supported.is_err() {
            transport
                .send(SyncMessage::AuthorizationScopeUnavailable { request_id })
                .map_err(transport_error)?;
            return Ok(());
        }
        let values = binding_values_in_param_order(shape, binding);
        let register = SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(shape),
            opts: scope.options.clone(),
        };
        transport.send(register.clone()).map_err(transport_error)?;
        let subscribe = SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values,
            known_state: None,
        });
        transport.send(subscribe.clone()).map_err(transport_error)?;
        peer.declare_known_state(subscription, None);
        let update = {
            let mut node = node.lock().await;
            peer.rehydrate_query_for_subscription_with_opts(
                &mut node,
                subscription,
                shape,
                binding,
                scope.options.clone(),
            )
            .await?
        };
        let Some(update) = update else {
            transport
                .send(SyncMessage::AuthorizationScopeUnavailable { request_id })
                .map_err(transport_error)?;
            return Ok(());
        };
        let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            settled_through: cut,
            ..
        }) = &update
        else {
            return Err(Error::new(
                ErrorCode::Protocol,
                "scope hydration was not a view update",
            ));
        };
        let progress = peer.authorization_progress_for_subscription(subscription);
        if aggregate.apply(subscription, *cut, progress).is_none()
            && index + 1 == support_clauses.len()
        {
            transport
                .send(SyncMessage::AuthorizationScopeUnavailable { request_id })
                .map_err(transport_error)?;
            return Ok(());
        }
        transport
            .send(SyncMessage::AuthorizationScopeView {
                request_id,
                key: scope.key.clone(),
                clause_index: index as u16,
                clause_count,
                view: crate::protocol::ViewUpdatePayload::from_view_update(update.clone())
                    .expect("scope hydration produces view updates"),
            })
            .map_err(transport_error)?;
        support_subscriptions.push(subscription);
        served_clauses.push(ServedAuthorizationScopeClause {
            subscription,
            register,
            subscribe,
            view: update,
        });
    }
    let Some((settled_through, authorization_progress)) = aggregate.bounds() else {
        transport
            .send(SyncMessage::AuthorizationScopeUnavailable { request_id })
            .map_err(transport_error)?;
        return Ok(());
    };
    let receipt = AuthorizationScopeReceipt {
        key: scope.key.clone(),
        authority: *node.borrow().node_uuid().as_bytes(),
        link: identity,
        authority_epoch: connection_epoch,
        claims_revision: current_claims_revision,
        policy_epoch: current_policy_epoch,
        settled_through,
        authorization_progress,
    };
    transport
        .send(SyncMessage::AuthorizationScopeAggregateReceipt {
            request_id,
            receipt: receipt.clone(),
        })
        .map_err(transport_error)?;
    if hydrations.len() < MAX_AUTHORIZATION_SCOPES {
        hydrations.insert(
            scope.key,
            ServedAuthorizationScopeHydration {
                clauses: served_clauses,
                receipt,
            },
        );
    }
    // Scope views are proof material, not application subscriptions.  Their
    // lifetime ends after the receipt; FIFO keeps the receiver's local
    // evaluation ahead of this cleanup.
    for subscription in support_subscriptions {
        transport
            .send(SyncMessage::Unsubscribe { subscription })
            .map_err(transport_error)?;
        peer.forget_subscription_with_node(&mut node.borrow_mut(), subscription);
    }
    Ok(())
}

fn authorization_scope_receipt_for_view<S>(
    node: &NodeState<S>,
    peer: &PeerState,
    link_identity: AuthorSubject,
    connection_epoch: u64,
    purpose: &AuthorizedScopePurpose,
    update: &SyncMessage,
) -> Option<(SubscriptionKey, AuthorizationScopeReceipt)>
where
    S: OrderedKvStorage,
{
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through,
        ..
    }) = update
    else {
        return None;
    };
    Some((
        *subscription,
        AuthorizationScopeReceipt {
            key: purpose.key.clone(),
            authority: *node.node_uuid().as_bytes(),
            link: link_identity,
            authority_epoch: connection_epoch,
            claims_revision: node.session_claim_revision(link_identity),
            policy_epoch: node.active_catalogue_seq(),
            settled_through: *settled_through,
            authorization_progress: peer.authorization_progress_for_subscription(*subscription),
        },
    ))
}

fn aggregate_authorization_scope_receipt_for_view<S>(
    aggregates: &mut BTreeMap<
        crate::protocol::AuthorizationSupportScopeKey,
        AuthorityScopeAggregate,
    >,
    node: &NodeState<S>,
    peer: &PeerState,
    link_identity: AuthorSubject,
    connection_epoch: u64,
    purpose: &AuthorizedScopePurpose,
    update: &SyncMessage,
) -> Option<(SubscriptionKey, AuthorizationScopeReceipt)>
where
    S: OrderedKvStorage,
{
    let (subscription, mut receipt) = authorization_scope_receipt_for_view(
        node,
        peer,
        link_identity,
        connection_epoch,
        purpose,
        update,
    )?;
    let aggregate = aggregates.get_mut(&purpose.key)?;
    if aggregate.expected_support() != &purpose.expected_support {
        return None;
    }
    let (settled_through, authorization_progress) = aggregate.apply(
        subscription,
        receipt.settled_through,
        receipt.authorization_progress,
    )?;
    receipt.settled_through = settled_through;
    receipt.authorization_progress = authorization_progress;
    Some((subscription, receipt))
}

/// Every support clause must be current in both dimensions.  They deliberately
/// have independent lower bounds: cuts and authorization generations do not
/// form a lexicographically ordered capability.
#[cfg(test)]
pub(super) fn aggregate_authorization_scope_bounds(
    applied: &BTreeMap<SubscriptionKey, (crate::time::GlobalTime, u64)>,
) -> Option<(crate::time::GlobalTime, u64)> {
    Some((
        applied
            .values()
            .map(|(settled, _)| *settled)
            .min_by_key(|settled| settled.0)?,
        applied.values().map(|(_, progress)| *progress).min()?,
    ))
}

/// Check a transported receipt against the authority identity authenticated by
/// this connection and the support view already applied locally.
pub(super) fn authorization_scope_receipt_matches_transport_context(
    receipt: &AuthorizationScopeReceipt,
    expected: AuthorityContext,
    applied_cut: Option<crate::time::GlobalTime>,
) -> bool {
    receipt.link == expected.link
        && receipt.link == receipt.key.subject
        && receipt.authority == expected.authority
        && receipt.authority_epoch == expected.connection_epoch
        && receipt.claims_revision == expected.claims_revision
        && receipt.policy_epoch == expected.policy_epoch
        && receipt.authorization_progress >= expected.authorization_progress
        && receipt.settled_through.0 >= expected.settled_through
        && applied_cut.is_some_and(|cut| cut >= receipt.settled_through)
}

/// Scope support is authority-current.  Keep this separate from generic shape
/// admission so a matching query identity cannot silently substitute a branch,
/// snapshot, or local-tier view for the support proof it is meant to hydrate.
pub(super) fn authorization_scope_support_options_match(
    expected: &RegisterShapeOptions,
    actual: &RegisterShapeOptions,
    subscription: SubscriptionKey,
) -> bool {
    actual == expected && subscription.read_view == expected.read_view_key()
}

fn move_scope_aggregate_member(
    aggregates: &mut BTreeMap<
        crate::protocol::AuthorizationSupportScopeKey,
        AuthorityScopeAggregate,
    >,
    prior: Option<&AuthorizedScopePurpose>,
    refreshed: &AuthorizedScopePurpose,
    subscription: SubscriptionKey,
) {
    if let Some(prior) = prior
        && prior.key != refreshed.key
        && let Some(previous) = aggregates.get_mut(&prior.key)
    {
        previous.forget(subscription);
        if previous.has_no_members() {
            aggregates.remove(&prior.key);
        }
    }
    let aggregate = aggregates
        .entry(refreshed.key.clone())
        .or_insert_with(|| AuthorityScopeAggregate::new(refreshed.expected_support.clone()));
    if aggregate.expected_support() == &refreshed.expected_support {
        // A changed scope identity must never reuse the old support cut.
        let _ = aggregate.register(
            subscription,
            (subscription.shape_id, subscription.binding_id),
        );
    }
}

/// Forget a support subscription when its authority-derived purpose ceases to
/// exist.  In particular, do not retain an applied cut across a policy/claims
/// transition that later returns to the same scope key.
pub(super) fn remove_scope_aggregate_member(
    aggregates: &mut BTreeMap<
        crate::protocol::AuthorizationSupportScopeKey,
        AuthorityScopeAggregate,
    >,
    key: &crate::protocol::AuthorizationSupportScopeKey,
    subscription: SubscriptionKey,
) {
    let empty = if let Some(aggregate) = aggregates.get_mut(key) {
        aggregate.forget(subscription);
        aggregate.has_no_members()
    } else {
        false
    };
    if empty {
        aggregates.remove(key);
    }
}

fn refresh_authorized_scope_purpose<S>(
    node: &NodeState<S>,
    link_identity: AuthorSubject,
    subscription: SubscriptionKey,
    shape: &ValidatedQuery,
    binding: &Binding,
    prior: &AuthorizedScopePurpose,
) -> Option<AuthorizedScopePurpose>
where
    S: OrderedKvStorage,
{
    let expected = node
        .authorization_support_scope(link_identity, &prior.action)
        .ok()?;
    let exact_support = subscription.shape_id == shape.shape_id()
        && subscription.binding_id == binding.binding_id()
        && subscription.read_view == expected.options.read_view_key()
        && expected
            .subscriptions
            .iter()
            .any(|(expected_shape, expected_binding)| {
                expected_shape.shape_id() == shape.shape_id()
                    && expected_binding.binding_id() == binding.binding_id()
            });
    exact_support.then_some(AuthorizedScopePurpose {
        key: expected.key,
        operation: expected.operation,
        action: prior.action.clone(),
        expected_support: expected
            .subscriptions
            .iter()
            .map(|(shape, binding)| (shape.shape_id(), binding.binding_id()))
            .collect(),
    })
}

fn query_root_filters_reference_id(query: &Query) -> bool {
    query.filters.iter().any(predicate_references_id)
        || !query.reachable.is_empty()
        || query.joins.iter().any(root_join_references_id)
        || query.policy_branches.iter().any(|branch| {
            branch.filters.iter().any(predicate_references_id)
                || !branch.reachable.is_empty()
                || branch.joins.iter().any(root_join_references_id)
        })
}

fn root_join_references_id(join: &crate::query::JoinVia) -> bool {
    join.source_column
        .as_deref()
        .is_none_or(|column| column == "id")
        || join
            .source_lookup
            .as_ref()
            .is_some_and(|lookup| lookup.row_id_source_column == "id")
        || join
            .correlated_filters
            .iter()
            .any(|correlation| correlation.source_column == "id")
}

fn predicate_references_id(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::All(items) | Predicate::Any(items) => items.iter().any(predicate_references_id),
        Predicate::Not(item) => predicate_references_id(item),
        Predicate::Eq(left, right)
        | Predicate::Ne(left, right)
        | Predicate::Gt(left, right)
        | Predicate::Gte(left, right)
        | Predicate::Lt(left, right)
        | Predicate::Lte(left, right)
        | Predicate::Contains(left, right) => operand_is_id(left) || operand_is_id(right),
        Predicate::In(operand, values) => {
            operand_is_id(operand) || values.iter().any(operand_is_id)
        }
        Predicate::IsNull(operand) => operand_is_id(operand),
        Predicate::EnumMatch {
            column, payload, ..
        } => column == "id" || predicate_references_id(payload),
    }
}

fn operand_is_id(operand: &Operand) -> bool {
    matches!(operand, Operand::Column(column) if column == "id")
}

fn drop_peer_request<S>(node: &SharedNodeState<S>)
where
    S: OrderedKvStorage,
{
    node.borrow_mut().record_dropped_peer_request();
}

fn handle_transport_backpressure<S>(
    node: &SharedNodeState<S>,
    scheduler: &SharedTickScheduler,
    error: &TransportError,
) -> bool
where
    S: OrderedKvStorage,
{
    match error {
        TransportError::Backpressure => {
            node.borrow_mut().record_transport_backpressure_retry();
            schedule_tick_in(scheduler, TickUrgency::Deferred);
            true
        }
        TransportError::Failed(_) => false,
    }
}

fn handle_db_backpressure<S>(
    node: &SharedNodeState<S>,
    scheduler: &SharedTickScheduler,
    error: &Error,
) -> bool
where
    S: OrderedKvStorage,
{
    if error.code == ErrorCode::Backpressure {
        node.borrow_mut().record_transport_backpressure_retry();
        schedule_tick_in(scheduler, TickUrgency::Deferred);
        true
    } else {
        false
    }
}

#[cfg(feature = "sync-autopsy")]
fn summarize_subscription_key(subscription: SubscriptionKey) -> String {
    format!(
        "shape={} binding={} read_view={}",
        subscription.shape_id.0, subscription.binding_id.0, subscription.read_view.id
    )
}

#[cfg(feature = "sync-autopsy")]
fn summarize_sync_message(message: &SyncMessage) -> String {
    match message {
        SyncMessage::RegisterShape { shape_id, opts, .. } => {
            format!(
                "RegisterShape shape={} read_view={}",
                shape_id.0,
                opts.read_view_key().id
            )
        }
        SyncMessage::Subscribe(subscribe) => {
            format!(
                "Subscribe {} values={} known_state={}",
                summarize_subscription_key(subscribe.subscription),
                subscribe.values.len(),
                subscribe.known_state.is_some()
            )
        }
        SyncMessage::Unsubscribe { subscription } => {
            format!("Unsubscribe {}", summarize_subscription_key(*subscription))
        }
        SyncMessage::SubscribeRejected {
            subscription,
            reason,
        } => format!(
            "SubscribeRejected {} reason={reason:?}",
            summarize_subscription_key(*subscription)
        ),
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through,
            reset_result_set,
            version_carriers,
            version_bundles,
            peer_payload_inventory,
            result_member_adds,
            result_member_removes,
            program_fact_adds,
            program_fact_removes,
            terminal_operations,
        }) => format!(
            "ViewUpdate {} settled={} reset={} bundles={} inventory={} adds={} removes={} fact_adds={} fact_removes={} terminal_ops={}",
            summarize_subscription_key(*subscription),
            settled_through.0,
            reset_result_set,
            version_bundles.len()
                + expand_version_carriers(version_carriers)
                    .map(|bundles| bundles.len())
                    .unwrap_or_default(),
            peer_payload_inventory.complete_tx_payloads.len(),
            result_member_adds.len(),
            result_member_removes.len(),
            program_fact_adds.len(),
            program_fact_removes.len(),
            terminal_operations.len()
        ),
        SyncMessage::CommitUnit { tx, .. } => format!("CommitUnit tx={:?}", tx.tx_id),
        SyncMessage::FateUpdate { tx_id, fate, .. } => {
            format!("FateUpdate tx={tx_id:?} fate={fate:?}")
        }
        SyncMessage::FetchRowVersions { requests } => {
            format!("FetchRowVersions requests={}", requests.len())
        }
        SyncMessage::RowVersionPayloads { version_bundles } => {
            format!("RowVersionPayloads bundles={}", version_bundles.len())
        }
        SyncMessage::PermissionAdviceRequest { request_id, action } => {
            let (kind, table) = match action {
                PermissionAdviceAction::Insert { table, .. } => ("insert", table),
                PermissionAdviceAction::Read { table, .. } => ("read", table),
                PermissionAdviceAction::Update { table, .. } => ("update", table),
                PermissionAdviceAction::Delete { table, .. } => ("delete", table),
            };
            format!("PermissionAdviceRequest id={request_id:?} action={kind} table={table}")
        }
        SyncMessage::PermissionAdviceResponse { request_id, advice } => {
            format!("PermissionAdviceResponse id={request_id:?} advice={advice:?}")
        }
        other => format!("{other:?}"),
    }
}

fn send_with_sync_context<S>(
    node: &SharedNodeState<S>,
    peer: &mut PeerState,
    transport: &mut dyn Transport,
    message: SyncMessage,
) -> Result<(), Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    send_catalogue_snapshot_if_needed(node, peer, transport)?;
    let mut message = message;
    if let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        peer_payload_inventory,
        ..
    }) = &mut message
    {
        peer_payload_inventory
            .authorization_progress
            .get_or_insert_with(|| peer.authorization_progress_for_subscription(*subscription));
    }
    #[cfg(feature = "sync-autopsy")]
    sync_autopsy::record(format!(
        "transport send {}",
        summarize_sync_message(&message)
    ));
    send_sync_message_chunked(transport, message)
}

/// Send an authority catalogue snapshot exactly once per peer fingerprint.
/// Trusted edge links have no application subscription during bootstrap, so
/// catalogue propagation must not depend on a later ViewUpdate or fate.
fn send_catalogue_snapshot_if_needed<S>(
    node: &SharedNodeState<S>,
    peer: &mut PeerState,
    transport: &mut dyn Transport,
) -> Result<(), Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let snapshot = node.borrow().catalogue_snapshot()?;
    let catalogue_fingerprint = *blake3::hash(
        &serde_json::to_vec(&snapshot).expect("catalogue snapshot serialization is infallible"),
    )
    .as_bytes();
    if peer.needs_catalogue_snapshot(catalogue_fingerprint) {
        transport
            .send(SyncMessage::CatalogueSnapshot(Box::new(snapshot)))
            .map_err(transport_error)?;
        peer.mark_catalogue_snapshot_announced(catalogue_fingerprint);
    }
    Ok(())
}

fn send_sync_message_chunked(
    transport: &mut dyn Transport,
    message: SyncMessage,
) -> Result<(), Error> {
    transport.send(message).map_err(transport_error)
}

fn send_with_local_sync_context<S>(
    node: &SharedNodeState<S>,
    transport: &mut dyn Transport,
    message: SyncMessage,
) -> Result<(), Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let _ = node;
    #[cfg(feature = "sync-autopsy")]
    sync_autopsy::record(format!(
        "transport send {}",
        summarize_sync_message(&message)
    ));
    transport.send(message).map_err(transport_error)
}

fn view_update_subscription(message: &SyncMessage) -> Option<SubscriptionKey> {
    match message {
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { subscription, .. }) => {
            Some(*subscription)
        }
        _ => None,
    }
}

fn stamp_view_update_authorization_progress_from(
    peer: &PeerState,
    source_subscription: SubscriptionKey,
    message: &mut SyncMessage,
) {
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        peer_payload_inventory,
        ..
    }) = message
    else {
        return;
    };
    let source_progress = peer.authorization_progress_for_subscription(source_subscription);
    if source_subscription != *subscription
        && source_progress == peer.authorization_progress_for_subscription(*subscription)
    {
        return;
    }
    peer_payload_inventory.authorization_progress = Some(source_progress);
}

fn retarget_view_update(mut message: SyncMessage, target: SubscriptionKey) -> SyncMessage {
    if let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { subscription, .. }) =
        &mut message
    {
        *subscription = target;
    }
    message
}

fn write_state_update_tx_id(message: &SyncMessage) -> Option<TxId> {
    match message {
        SyncMessage::FateUpdate { tx_id, .. } => Some(*tx_id),
        _ => None,
    }
}

fn notify_write_state_waiters(waiters: &WriteStateWaiters, tx_id: TxId) -> bool {
    let Some(waiters) = waiters.borrow_mut().remove(&tx_id) else {
        return false;
    };
    let mut handled_mutation_error = false;
    for waiter in waiters {
        match waiter.notify {
            WriteStateWaiterNotify::Future(sender) => {
                if sender.send(()).is_ok() {
                    handled_mutation_error = true;
                }
            }
            WriteStateWaiterNotify::Callback(callback) => {
                callback();
                handled_mutation_error = true;
            }
        }
    }
    handled_mutation_error
}

fn handle_write_state_update<S>(
    node: &SharedNodeState<S>,
    waiters: &WriteStateWaiters,
    mutation_errors: &SharedMutationErrors,
    scheduler: &SharedTickScheduler,
    tx_id: TxId,
) where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let handled_by_waiter = notify_write_state_waiters(waiters, tx_id);
    let rejected = node.borrow().rejected_transaction(tx_id);
    let Some(rejected) = rejected else {
        return;
    };

    if handled_by_waiter {
        mutation_errors.borrow_mut().pending.remove(&tx_id);
        if let Err(error) = crate::db::block_on(node.borrow_mut().discard_rejection(tx_id)) {
            tracing::warn!(?tx_id, %error, "failed to acknowledge waited mutation error");
        }
        return;
    }

    let should_schedule = {
        let mut state = mutation_errors.borrow_mut();
        state
            .pending
            .entry(tx_id)
            .or_insert_with(|| mutation_error_event(rejected));
        state.callback.is_some()
    };
    if should_schedule {
        schedule_tick_in(scheduler, TickUrgency::Immediate);
    }
}

pub(super) fn take_pending_mutation_error_delivery(
    mutation_errors: &SharedMutationErrors,
) -> Option<(MutationErrorCallback, BTreeMap<TxId, MutationErrorEvent>)> {
    let mut state = mutation_errors.borrow_mut();
    let callback = state.callback.clone()?;
    if state.pending.is_empty() {
        return None;
    }
    Some((callback, std::mem::take(&mut state.pending)))
}

pub(super) fn mutation_error_event(rejected: crate::tx::RejectedTransaction) -> MutationErrorEvent {
    let tx_id = rejected.tx_id();
    let transaction_id = TransactionId::from_committed_tx(tx_id);
    let (code, reason) = mutation_error_details(&rejected.reason());
    MutationErrorEvent {
        code: code.clone(),
        reason: reason.clone(),
        transaction: LocalTransactionRecord {
            transaction_id,
            kind: rejected.kind().into(),
            sealed: true,
            latest_settlement: TransactionFate::Rejected {
                transaction_id,
                code,
                reason,
            },
        },
    }
}

fn mutation_error_details(reason: &RejectionReason) -> (String, String) {
    match reason {
        RejectionReason::ClientClockTooFarAhead => (
            "client_clock_too_far_ahead".to_owned(),
            "Client clock is too far ahead".to_owned(),
        ),
        RejectionReason::AuthorizationDenied => (
            "permission_denied".to_owned(),
            "Write rejected by server authorization".to_owned(),
        ),
        RejectionReason::ExclusiveConflict => (
            "exclusive_conflict".to_owned(),
            "Exclusive transaction conflicted with another write".to_owned(),
        ),
        RejectionReason::CausalityViolation => (
            "causality_violation".to_owned(),
            "Transaction violated causal ordering".to_owned(),
        ),
        RejectionReason::Cascade { root } => (
            "cascade_rejected".to_owned(),
            format!("Transaction was rejected because ancestor {root:?} was rejected"),
        ),
        RejectionReason::MalformedCommit(reason) => (
            "write_rejected".to_owned(),
            format!("Malformed transaction: {reason}"),
        ),
    }
}

/// Bindings carry values positionally; the shape orders them by param name.
fn binding_values_in_param_order(shape: &ValidatedQuery, binding: &Binding) -> Vec<Value> {
    shape
        .params()
        .keys()
        .map(|name| {
            binding
                .values()
                .get(name)
                .cloned()
                .expect("binding is missing a shape parameter value")
        })
        .collect()
}

/// Describe an unavailable locally-owned upload chunk without disclosing its
/// retrieval capability. A locator grants exact chunk retrieval, so the
/// diagnostic carries a stable fingerprint rather than raw locator bytes.
fn large_value_upload_chunk_context(
    tx_id: TxId,
    value_ref: &groove::large_values::LargeValueRef,
    node_ref: &groove::large_values::NodeRef,
    replica_role: &str,
    source_node: NodeUuid,
) -> String {
    format!(
        "role={replica_role} source_node={source_node:?} transaction={tx_id:?} root_hash={} root_locator={} chunk_hash={} chunk_locator={}",
        hex::encode(value_ref.root.object_hash.0),
        chunk_locator_fingerprint(value_ref.root.locator),
        hex::encode(node_ref.object_hash.0),
        chunk_locator_fingerprint(node_ref.locator),
    )
}

fn chunk_locator_fingerprint(locator: groove::large_values::Locator) -> String {
    blake3::hash(locator.as_bytes()).to_hex()[..16].to_owned()
}

/// A `ViewUpdate` that carries no version, result-set, or program-fact change —
/// nothing to ship to the subscriber this tick.
pub(super) fn view_update_is_empty(message: &SyncMessage) -> bool {
    match message {
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            reset_result_set,
            version_carriers,
            version_bundles,
            peer_payload_inventory,
            result_member_adds,
            result_member_removes,
            program_fact_adds,
            program_fact_removes,
            ..
        }) => {
            !reset_result_set
                && version_carriers.is_empty()
                && version_bundles.is_empty()
                && peer_payload_inventory.complete_tx_payloads.is_empty()
                && result_member_adds.is_empty()
                && result_member_removes.is_empty()
                && program_fact_adds.is_empty()
                && program_fact_removes.is_empty()
        }
        _ => false,
    }
}
