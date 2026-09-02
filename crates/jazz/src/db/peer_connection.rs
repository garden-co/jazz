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
use crate::protocol::expand_version_carriers;
pub(super) fn route_subscription_refresh_failure(
    subscriptions: &SubscriptionList,
    error: &Error,
) -> usize {
    eprintln!("jazz subscription refresh failed: {error}");
    let mut delivered = 0;
    for state in subscriptions.borrow().iter().filter_map(Weak::upgrade) {
        let state = state.borrow();
        if state.closed.get() {
            continue;
        }
        let event = SubscriptionEvent::Rejected {
            reason: SubscribeRejectReason::ServerFailure {
                code: SubscribeServerFailureCode::Internal,
            },
        };
        if state.sender.unbounded_send(event).is_ok() {
            delivered += 1;
        }
    }
    delivered
}

/// Namespace for relay-owned usage-site subscription handles.
///
/// A relay may normalize multiple downstream coverage requests to the same
/// upstream read view. The resulting subscriptions still need distinct wire
/// handles: the upstream node deduplicates their work by [`CoverageKey`], while
/// retaining the independent ownership needed for correct unsubscribe behavior.
const RELAY_UPSTREAM_SUBSCRIPTION_NAMESPACE: uuid::Uuid =
    uuid::uuid!("ae3eb9f7-65cc-528d-8f3e-a772fb6f68fe");

/// Namespace for the maintained receiver owned by one policy-partitioned
/// coverage group. The ordinary canonical binding id remains stable for direct
/// coverage; relayed policy snapshots need a distinct runtime receiver even
/// when their query bindings are identical.
const COVERAGE_GROUP_SUBSCRIPTION_NAMESPACE: uuid::Uuid =
    uuid::uuid!("19fdc830-2dd8-5876-ae31-a8f526512ac5");

/// Wall-clock time used exclusively for authority admission checks.
///
/// This must not use `UploadRetryClock`: that clock is deliberately monotonic
/// and process-relative so retry backoff is unaffected by wall-clock changes,
/// whereas transaction HLC physical components are Unix milliseconds.
fn authority_admission_now_ms() -> Result<u64, Error> {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map_err(|_| Error::new(ErrorCode::Protocol, "authority clock precedes Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| {
            Error::new(
                ErrorCode::Protocol,
                "authority clock exceeds u64 milliseconds",
            )
        })
}

fn relay_upstream_subscription_key(
    connection_epoch: u64,
    downstream: SubscriptionKey,
    upstream_read_view: ReadViewKey,
    policy_binding: &(AuthorSubject, BTreeMap<String, Value>),
) -> SubscriptionKey {
    // An upstream relay usage site is admitted under this exact snapshot. A
    // direct downstream claim refresh must therefore get a fresh opaque handle
    // rather than reusing a wire subscription whose authority has retained the
    // old delegated context.
    let identity = postcard::to_allocvec(&(
        connection_epoch,
        downstream,
        upstream_read_view,
        policy_binding,
    ))
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

pub(crate) fn coverage_group_subscription_key(coverage: &CoverageKey) -> SubscriptionKey {
    let binding_id = coverage
        .policy_binding
        .as_ref()
        .map_or(coverage.binding_id, |policy| {
            let identity = postcard::to_allocvec(&(coverage.binding_id, policy))
                .expect("coverage policy identity is postcard encodable");
            BindingId(uuid::Uuid::new_v5(
                &COVERAGE_GROUP_SUBSCRIPTION_NAMESPACE,
                &identity,
            ))
        });
    SubscriptionKey {
        shape_id: coverage.shape_id,
        binding_id,
        read_view: coverage.opts.read_view_key(),
    }
}

async fn finish_peer_publication_outcome<S, T>(
    node: &SharedNodeState<S>,
    subscriptions: &SubscriptionList,
    active_authority_view_receipts: &ActiveAuthorityViewReceipts,
    progress_waker: Option<&Waker>,
    outcome: PublicationOutcome<T>,
) -> Result<(T, usize), Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let (value, changed, _) = finish_peer_publication_outcome_with_refresh(
        node,
        subscriptions,
        active_authority_view_receipts,
        progress_waker,
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
    progress_waker: Option<&Waker>,
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

            let mut persisted = Vec::with_capacity(publications.len());
            for publication in &publications {
                persisted.push((publication.tx_id(), publication.persist().await));
            }
            let mut state = node.lock().await;
            for (tx_id, persistence) in persisted {
                state.settle_published_transaction(tx_id, persistence)?;
            }
            drop(state);
            if refresh {
                changed += match refresh_subscriptions_in(
                    node,
                    subscriptions,
                    active_authority_view_receipts,
                    progress_waker,
                )
                .await
                {
                    Ok(changed) => changed,
                    Err(error) => route_subscription_refresh_failure(subscriptions, &error),
                };
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
                    admitted_write_authorization: false,
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
pub(super) fn dispatch_admitted_subscriber_message<'a, S>(
    node: &'a SharedNodeState<S>,
    peer: &'a mut PeerState,
    local_receiver: bool,
    ingest_context: CommitUnitIngestContext,
    session_claim_binding: (AuthorSubject, BTreeMap<String, Value>),
    admitted_upstream_authority: &'a Rc<RefCell<Option<AuthorityContext>>>,
    edge_fate_routes: &'a EdgeFateRoutes,
    local_fate_routes: &'a LocalFateRoutes,
    downstream_fates: &'a PendingDownstreamFates,
    maintenance_now_ms: u64,
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
                let mut state = node.lock().await;
                let same_scope_author = state.client_relay_scope().is_some_and(|scope| {
                    scope.admits_session(session_claim_binding.0)
                        && tx.made_by == session_claim_binding.0
                });
                state
                    .ingest_relay_commit_unit(tx.clone(), versions.clone())
                    .await?;
                if same_scope_author {
                    state
                        .record_scope_relay_authored_pending_versions(
                            &tx,
                            &versions,
                            session_claim_binding.0,
                        )
                        .await?;
                }
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
                let identity = EdgeFateCommitIdentity::new(&tx, &versions);
                let route_registered = if let Some(authority) =
                    *admitted_upstream_authority.borrow()
                {
                    let mut routes = edge_fate_routes.borrow_mut();
                    prune_edge_fate_routes(&mut routes, Some(authority));
                    let route_count = routes
                        .values()
                        .map(|obligation| obligation.routes.len())
                        .sum::<usize>();
                    let existing = routes.get(&tx_id);
                    if existing.is_some_and(|obligation| !obligation.identity.matches(&identity)) {
                        return Err(crate::node::Error::ConflictingCommitUnit(tx_id).into());
                    }
                    let already_routed = existing.is_some_and(|obligation| {
                        obligation.routes.iter().any(|route| {
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
                        let obligation =
                            routes.entry(tx_id).or_insert_with(|| EdgeFateObligation {
                                identity: identity.clone(),
                                routes: Vec::new(),
                            });
                        if obligation.routes.len() < MAX_EDGE_FATE_ROUTES_PER_TX {
                            obligation.routes.push(EdgeFateRoute {
                                authority: Some(authority),
                                queue: Rc::downgrade(downstream_fates),
                                edge_acknowledged: false,
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
                    let existing = routes.get(&tx_id);
                    if existing.is_some_and(|obligation| !obligation.identity.matches(&identity)) {
                        return Err(crate::node::Error::ConflictingCommitUnit(tx_id).into());
                    }
                    let already_routed = existing.is_some_and(|obligation| {
                        obligation.routes.iter().any(|route| {
                            route.authority.is_none()
                                && route
                                    .queue
                                    .upgrade()
                                    .is_some_and(|queue| Rc::ptr_eq(&queue, downstream_fates))
                        })
                    });
                    let route_count = routes
                        .values()
                        .map(|obligation| obligation.routes.len())
                        .sum::<usize>();
                    if already_routed {
                        true
                    } else if route_count >= MAX_EDGE_FATE_ROUTES {
                        false
                    } else {
                        let obligation =
                            routes.entry(tx_id).or_insert_with(|| EdgeFateObligation {
                                identity: identity.clone(),
                                routes: Vec::new(),
                            });
                        if obligation.routes.len() >= MAX_EDGE_FATE_ROUTES_PER_TX {
                            false
                        } else {
                            obligation.routes.push(EdgeFateRoute {
                                authority: None,
                                queue: Rc::downgrade(downstream_fates),
                                edge_acknowledged: false,
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

                let authority_now_ms = authority_admission_now_ms()?;
                let mut node = node.lock().await;
                let outcome = peer
                    .ingest_edge_mergeable_commit_unit(
                        &mut node,
                        tx,
                        versions,
                        maintenance_now_ms,
                        authority_now_ms,
                        session_claim_binding.1,
                    )
                    .await
                    .map_err(Error::from)?;
                let (responses, publications, post_settlement_work) = outcome.into_parts();
                let mut direct_responses = Vec::new();
                for response in responses {
                    if matches!(response, SyncMessage::FateUpdate { .. }) {
                        route_edge_admission_fate(edge_fate_routes, tx_id, &response);
                    } else {
                        direct_responses.push(response);
                    }
                }
                Ok(PublicationOutcome {
                    value: direct_responses,
                    publications,
                    post_settlement_work,
                })
            }
            SyncMessage::CommitUnit { tx, versions }
                if tx.kind == TxKind::Mergeable
                    && (matches!(peer.role(), PeerRole::ClientLink { .. })
                        || peer.role() == PeerRole::Relay) =>
            {
                // Terminal authorization belongs to the immutable session
                // selected at request admission, never to the relay's
                // subjectless transport identity. A scope-isolated relay has
                // already been checked against its server-issued one-binding
                // capability; a multiplexed relay has passed the corresponding
                // transport admission check for this request.
                let permission_subject = match ingest_context.trust {
                    CommitUnitTrust::Session => ingest_context.identity,
                    CommitUnitTrust::Relay => session_claim_binding.0,
                    CommitUnitTrust::TrustedBackend => tx.permission_subject.unwrap_or(tx.made_by),
                    CommitUnitTrust::TrustedAdmin => ingest_context.identity,
                };
                let admitted_write_authorization = {
                    let mut node = node.lock().await;
                    peer.prove_terminal_commit_authorization(
                        &mut node,
                        permission_subject,
                        session_claim_binding.1,
                        &versions,
                        tx.tx_id,
                    )
                    .await?
                };
                Ok(node
                    .lock()
                    .await
                    .apply_sync_message_with_ingest_context(
                        SyncMessage::CommitUnit { tx, versions },
                        Some(CommitUnitIngestContext {
                            admitted_write_authorization,
                            ..ingest_context
                        }),
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
    pub(super) query_coverage_registrations: QueryCoverageRegistrations,
    pub(super) active_authority_view_receipts: ActiveAuthorityViewReceipts,
    pub(super) coverage_refresh_generations: CoverageRefreshGenerations,
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
    pub(super) browser_relay_recovered_tx_ids: Rc<RefCell<BTreeSet<TxId>>>,
    pub(super) subscriber_dirty_epoch: Rc<Cell<u64>>,
    #[cfg(any(test, feature = "testing"))]
    pub(super) fail_next_subscription_refresh: Cell<bool>,
    pub(super) observed_subscriber_dirty_epoch: Cell<u64>,
    pub(super) observed_session_claim_revision: Cell<u64>,
    /// Fresh non-resumable epoch binding authorization receipts to this link.
    pub(super) connection_epoch: u64,
    pub(super) startup_error: Option<Error>,
    /// Exact uploads whose applied fate made them globally settled or rejected.
    pub(super) released_outbox_tx_ids: Vec<TxId>,
    /// One ordinary-wire chunk reply held at its semantic producer boundary.
    /// Dedicated auxiliary chunk traffic uses `PeerIoPump`'s bounded
    /// take/restore queue; this covers the legacy ordinary-wire responder.
    pub(super) pending_chunk_response: Option<ChunkResponseBatch>,
    /// Subscriber control/rejection replies retained until byte admission.
    ///
    /// This is deliberately not a transport-level queue. Every entry is an
    /// already-bounded registration, relay rejection, or maintained
    /// subscription outcome; while it is nonempty the subscriber stops
    /// consuming inbound work, and entries leave only after logical wire
    /// admission. A permanently stalled peer therefore cannot manufacture an
    /// independent unbounded response backlog.
    pub(super) pending_control_responses: VecDeque<PendingSubscriberControlResponse>,
    pub(super) link: ConnectionLink,
    pub(super) last_resume_bytes: Option<usize>,
    pub(super) auxiliary_pump: PeerIoPump,
}

/// A connection-owned response that has been produced but not yet admitted by
/// the bounded wire adapter.
///
/// Most control protocol frames intentionally bypass `send_with_sync_context`:
/// unlike a replicated payload, they must not opportunistically inject a
/// catalogue snapshot ahead of a rejection or proof receipt. Repair payloads
/// retain the sync context because their normal send path carries its
/// per-peer bookkeeping.
#[derive(Clone)]
pub(super) enum PendingSubscriberControlResponse {
    Direct(SyncMessage),
    WithSyncContext(SyncMessage),
    AuthorizationScopeSequence(PendingAuthorizationScopeSequence),
}

/// Lazily emits one authorization-scope proof sequence. The clauses are the
/// authority's already-derived semantic hydration state; retaining this plan
/// avoids copying it into a second, expanded wire-message queue when a bounded
/// adapter is full between any two proof frames.
#[derive(Clone)]
pub(super) struct PendingAuthorizationScopeSequence {
    request_id: PermissionAdviceRequestId,
    key: crate::protocol::AuthorizationSupportScopeKey,
    hydration: ServedAuthorizationScopeHydration,
    next_step: usize,
}

impl PendingAuthorizationScopeSequence {
    fn next_message(&self) -> Option<SyncMessage> {
        let clause_count = self.hydration.clauses.len();
        let proof_steps = clause_count.checked_mul(3)?;
        if self.next_step < proof_steps {
            let clause = &self.hydration.clauses[self.next_step / 3];
            return Some(match self.next_step % 3 {
                0 => clause.register.clone(),
                1 => clause.subscribe.clone(),
                2 => SyncMessage::AuthorizationScopeView {
                    request_id: self.request_id,
                    key: self.key.clone(),
                    clause_index: (self.next_step / 3) as u16,
                    clause_count: clause_count as u16,
                    view: crate::protocol::ViewUpdatePayload::from_view_update(clause.view.clone())
                        .expect("authority scope clauses are view updates"),
                },
                _ => unreachable!("modulo three has only three cases"),
            });
        }
        if self.next_step == proof_steps {
            return Some(SyncMessage::AuthorizationScopeAggregateReceipt {
                request_id: self.request_id,
                receipt: self.hydration.receipt.clone(),
            });
        }
        let unsubscribe_index = self.next_step.checked_sub(proof_steps + 1)?;
        self.hydration
            .clauses
            .get(unsubscribe_index)
            .map(|clause| SyncMessage::Unsubscribe {
                subscription: clause.subscription,
            })
    }

    fn advance(&mut self) {
        self.next_step = self.next_step.saturating_add(1);
    }
}

impl PendingSubscriberControlResponse {
    fn direct(message: SyncMessage) -> Self {
        Self::Direct(message)
    }

    fn with_sync_context(message: SyncMessage) -> Self {
        Self::WithSyncContext(message)
    }

    #[cfg(test)]
    pub(super) fn message(&self) -> &SyncMessage {
        match self {
            Self::Direct(message) | Self::WithSyncContext(message) => message,
            Self::AuthorizationScopeSequence(_) => {
                panic!("scope sequences generate messages lazily")
            }
        }
    }
}

fn queue_direct_control(
    pending: &mut VecDeque<PendingSubscriberControlResponse>,
    message: SyncMessage,
) {
    pending.push_back(PendingSubscriberControlResponse::direct(message));
}

fn queue_sync_context_control(
    pending: &mut VecDeque<PendingSubscriberControlResponse>,
    message: SyncMessage,
) {
    pending.push_back(PendingSubscriberControlResponse::with_sync_context(message));
}

fn queue_authorization_scope_sequence(
    pending: &mut VecDeque<PendingSubscriberControlResponse>,
    request_id: PermissionAdviceRequestId,
    key: crate::protocol::AuthorizationSupportScopeKey,
    hydration: ServedAuthorizationScopeHydration,
) {
    pending.push_back(
        PendingSubscriberControlResponse::AuthorizationScopeSequence(
            PendingAuthorizationScopeSequence {
                request_id,
                key,
                hydration,
                next_step: 0,
            },
        ),
    );
}

macro_rules! flush_subscriber_controls_or_stop {
    ($connection:expr, $peer:expr) => {
        if !flush_pending_control_responses(
            &$connection.node,
            $peer,
            $connection.transport.as_mut(),
            &mut $connection.pending_control_responses,
            &$connection.scheduler,
        )? {
            return Ok(true);
        }
    };
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
    /// Exact repair fetches whose byte admission has not happened yet.
    /// Kept separately from the paired repair payload so a bounded wire
    /// adapter cannot lose the one-shot request between detecting a missing
    /// version and recording the ViewUpdate that needs it.
    pub(super) pending_row_version_fetches: VecDeque<PendingRowVersionFetch>,
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
    pub(super) deferred_subscribe_rejections: VecDeque<PendingSubscriberControlResponse>,
    pub(super) served_current_rows: BTreeMap<SubscriptionKey, ServedCurrentRows>,
    pub(super) scope_purposes: BTreeMap<SubscriptionKey, AuthorizedScopePurpose>,
    pub(super) scope_aggregates:
        BTreeMap<crate::protocol::AuthorizationSupportScopeKey, AuthorityScopeAggregate>,
    pub(super) authority_scope_hydrations:
        BTreeMap<crate::protocol::AuthorizationSupportScopeKey, ServedAuthorizationScopeHydration>,
    pub(super) authority_scope_hydration_count: u64,
    pub(super) serve_dirty: bool,
}

/// A whole-table current-row view has the same immutable admission binding as
/// an ordinary subscription. Keep its provenance explicit: a delegated
/// current-row view must not be rewritten when the outer connection refreshes.
#[derive(Debug)]
pub(super) struct ServedCurrentRows {
    pub(super) table: String,
    pub(super) policy_binding: (AuthorSubject, BTreeMap<String, groove::records::Value>),
    pub(super) policy_binding_origin: CoveragePolicyBindingOrigin,
}

pub(super) struct PendingRowVersionRepair {
    pub(super) requests: Vec<crate::protocol::RowVersionRef>,
    pub(super) update: SyncMessage,
    pub(super) authority_receipt_eligible: bool,
}

/// One repair request remains bound to the exact policy snapshot that made
/// its source view update visible. It must never be coalesced with another
/// subscriber's request merely because the row-version references coincide.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct PendingRowVersionFetch {
    pub(super) requests: Vec<crate::protocol::RowVersionRef>,
    pub(super) policy_binding: (AuthorSubject, BTreeMap<String, groove::records::Value>),
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

impl ResumeCursor {
    /// Resume attaches the saved peer to a new physical transport. Preserve
    /// the server-authenticated scope binding while replacing its old
    /// per-attachment admission capability.
    #[cfg(feature = "runtime")]
    pub(crate) fn refresh_scope_relay_admission_epoch(&mut self) -> bool {
        self.peer.refresh_scope_relay_admission_epoch()
    }
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
            peer,
            session_claims,
            session_claim_revision,
            ..
        }) = &mut self.link
        else {
            return;
        };
        if peer.admitted_scope_relay_binding().is_some() {
            // A scope relay receives a fresh immutable capability only through
            // a new server-authenticated connection. Do not turn this generic
            // host refresh hook into a mutable capability update.
            return;
        }
        if *session_claims == claims {
            return;
        }
        *session_claims = claims;
        *session_claim_revision = session_claim_revision.saturating_add(1);
    }

    /// Return the claims admitted for this subscriber session. They are scoped
    /// while the node lock is held instead of being installed under the shared
    /// author identity.
    fn subscriber_session_claim_binding(&self) -> Option<(AuthorSubject, BTreeMap<String, Value>)> {
        let ConnectionLink::Subscriber(SubscriberConnectionState {
            peer,
            ingest_context,
            session_claims,
            ..
        }) = &self.link
        else {
            return None;
        };
        if let Some(binding) = peer.admitted_scope_relay_binding() {
            return Some((binding.identity, binding.claims.clone()));
        }
        Some((ingest_context.identity, session_claims.clone()))
    }

    /// Keep the legacy admission map available to write-policy evaluation and
    /// upstream propagation. Read compilation uses the scoped context above,
    /// so this author-keyed compatibility state cannot select another live
    /// session's maintained view.
    fn bind_subscriber_session_claims(&self) {
        if matches!(
            &self.link,
            ConnectionLink::Subscriber(SubscriberConnectionState { peer, .. })
                if peer.admitted_scope_relay_binding().is_some()
        ) {
            // All scope-relay policy work takes the immutable binding through
            // `scoped_active_session_claims`; do not duplicate it into the
            // author-keyed mutable compatibility map.
            return;
        }
        let Some((identity, claims)) = self.subscriber_session_claim_binding() else {
            return;
        };
        self.node.borrow_mut().set_session_claims(identity, claims);
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
    async fn rebind_subscriber_views_after_claim_change(
        &mut self,
        progress_waker: Option<&std::task::Waker>,
    ) -> Result<bool, Error> {
        let connection_epoch = self.connection_epoch;
        let identity = match &self.link {
            ConnectionLink::Subscriber(SubscriberConnectionState { ingest_context, .. }) => {
                ingest_context.identity
            }
            ConnectionLink::Upstream(_) => return Ok(false),
        };
        let session_claim_binding = self.subscriber_session_claim_binding();
        let current_revision = self.subscriber_session_claim_revision();
        if self.observed_session_claim_revision.get() == current_revision {
            return Ok(false);
        }

        let ConnectionLink::Subscriber(SubscriberConnectionState {
            peer,
            served,
            coverage_groups,
            upstream_subscriptions,
            served_current_rows,
            scope_purposes,
            scope_aggregates,
            serve_dirty,
            ..
        }) = &mut self.link
        else {
            unreachable!("subscriber identity requires a subscriber link")
        };
        peer.advance_authorization_progress();
        let refreshed_direct_binding = session_claim_binding
            .as_ref()
            .expect("subscriber claims")
            .clone();
        // `CoverageKey` is also the maintained receiver's stable identity. A
        // relay key includes its admitted policy snapshot, so a direct claim
        // refresh must replace that key rather than merely changing mutable
        // state under the old key. Otherwise the owner loop can read a
        // settled result set that still denotes the old claims.
        let coverage_replacements = coverage_groups
            .iter()
            .filter(|(_, group)| {
                group.policy_binding_origin == CoveragePolicyBindingOrigin::DirectAdmitted
            })
            .map(|(coverage, _)| {
                let mut refreshed = coverage.clone();
                if let Some(policy) = &mut refreshed.policy_binding {
                    policy.identity = refreshed_direct_binding.0;
                    policy.canonical_claims = crate::protocol::CanonicalPolicyClaims::new(
                        refreshed_direct_binding.1.clone(),
                    );
                }
                (coverage.clone(), refreshed)
            })
            .filter(|(old, refreshed)| old != refreshed)
            .collect::<Vec<_>>();
        let replaced_coverage_by_new = coverage_replacements
            .iter()
            .map(|(old, refreshed)| (refreshed.clone(), old.clone()))
            .collect::<BTreeMap<_, _>>();
        let stale_maintained_subscriptions = coverage_replacements
            .iter()
            .map(|(old, _)| coverage_group_subscription_key(old))
            .collect::<BTreeSet<_>>();
        for (old, refreshed) in &coverage_replacements {
            if coverage_groups.contains_key(refreshed) {
                return Err(Error::new(
                    ErrorCode::Protocol,
                    "claim refresh would merge direct and existing relay coverage",
                ));
            }
            let group = coverage_groups
                .remove(old)
                .expect("coverage key came from coverage_groups");
            coverage_groups.insert(refreshed.clone(), group);
            for coverage in served.values_mut() {
                if coverage == old {
                    *coverage = refreshed.clone();
                }
            }
        }
        // Coverage-key replacement changes the maintained receiver key. The
        // new key has never been opened; retire the old key explicitly before
        // scheduling a fresh group. Otherwise its PeerState cursor and
        // Groove subscription remain live under the old policy snapshot.
        for stale_subscription in stale_maintained_subscriptions {
            let mut node = self.node.borrow_mut();
            node.apply_unsubscribe(stale_subscription);
            peer.forget_subscription_with_node(&mut node, stale_subscription);
        }
        // A direct group can have already propagated its old snapshot to an
        // upstream authority. Replacing only the local maintained receiver
        // would make a broadened refresh permanently miss remote rows (and
        // would let a later repair continue under the old policy). Retire that
        // one usage site and allocate a fresh opaque handle whose identity
        // includes the new immutable delegated snapshot.
        let mut upstream_replacements = Vec::new();
        let mut refreshed_authority_sources = BTreeMap::new();
        let groups = coverage_groups
            .iter_mut()
            .map(|(coverage, group)| {
                // The connection's authenticated snapshot owns only direct
                // usage sites. A trusted relay may carry delegated sessions
                // whose subject happens to equal the connection subject (and
                // may even be SYSTEM), so identity equality is not provenance.
                if group.policy_binding_origin == CoveragePolicyBindingOrigin::DirectAdmitted {
                    group.policy_binding = refreshed_direct_binding.clone();
                    if group.upstream_opts.binding_source == BindingSource::RelayAuthoritySession
                        && group.upstream_opts.propagate_upstream
                        && let Some(downstream_subscription) = group.subscribers.first().copied()
                    {
                        let old_upstream_subscription = group.upstream_subscription;
                        let fresh_upstream_subscription = relay_upstream_subscription_key(
                            connection_epoch,
                            downstream_subscription,
                            group.upstream_opts.read_view_key(),
                            &refreshed_direct_binding,
                        );
                        if old_upstream_subscription != fresh_upstream_subscription {
                            group.upstream_subscription = fresh_upstream_subscription;
                            debug_assert_eq!(
                                group.authority_result_subscription,
                                old_upstream_subscription,
                                "only a non-authoritative relay refreshes an upstream authority source"
                            );
                            group.authority_result_subscription = fresh_upstream_subscription;
                            refreshed_authority_sources.insert(
                                coverage.clone(),
                                crate::protocol::AuthorityResultKey::policy_scoped(
                                    BindingViewKey {
                                        shape_id: group.shape.shape_id(),
                                        binding_id: group.binding.binding_id(),
                                        read_view: group.upstream_opts.read_view_key(),
                                    },
                                    crate::protocol::PolicyBindingKey::from_canonical_parts(
                                        refreshed_direct_binding.0,
                                        refreshed_direct_binding.1.clone(),
                                    ),
                                ),
                            );
                            group.awaiting_upstream_settlement = true;
                            // Do not let the next owner-loop pass rehydrate a
                            // subscriber from the old result set. Once the
                            // replacement has settled, it must publish a
                            // fresh reset (including an empty reset on a
                            // revocation) from the new upstream usage.
                            group.initialized = false;
                            group.pending_initial_subscribers = group.subscribers.clone();
                            upstream_replacements.push((
                                coverage.clone(),
                                replaced_coverage_by_new
                                    .get(coverage)
                                    .cloned()
                                    .unwrap_or_else(|| coverage.clone()),
                                old_upstream_subscription,
                                fresh_upstream_subscription,
                                group.shape.clone(),
                                group.binding.clone(),
                                group.upstream_opts.clone(),
                                group.subscribers.clone(),
                            ));
                        }
                    }
                }
                (
                    coverage.clone(),
                    group.shape.clone(),
                    group.binding.clone(),
                    group.policy_binding.clone(),
                    group.policy_binding_origin,
                    group.subscribers.iter().copied().collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let refreshed_upstream_usage = !upstream_replacements.is_empty();
        let deferred_rehydrates = upstream_replacements
            .iter()
            .map(|(coverage, ..)| coverage.clone())
            .collect::<BTreeSet<_>>();
        for (
            coverage,
            old_coverage,
            old_upstream_subscription,
            fresh_upstream_subscription,
            shape,
            binding,
            opts,
            downstream_subscriptions,
        ) in upstream_replacements
        {
            // A direct coverage key is not itself policy-partitioned, so a
            // claim refresh can replace U without changing the group key.
            // In that case the old receiver would otherwise remain attached
            // to B1 and later publish its stale membership as B2. Retire the
            // group-owned receiver explicitly; it will reopen only after the
            // fresh exact U settles.
            let maintained_subscription = coverage_group_subscription_key(&coverage);
            {
                let mut node = self.node.borrow_mut();
                node.apply_unsubscribe(maintained_subscription);
                peer.forget_subscription_with_node(&mut node, maintained_subscription);
            }
            let old_owner = retire_relay_upstream_subscription(
                &self.relay_upstream_subscription_owners,
                old_upstream_subscription,
                connection_epoch,
                &old_coverage,
            );
            // Withdraw the old local source before this subscriber's owner
            // loop can observe its already-settled BindingViewKey again. The
            // queued wire unsubscribe remains responsible for the remote
            // receiver; `apply_unsubscribe` is idempotent when the old open
            // had not left the pending queue yet.
            self.node
                .borrow_mut()
                .apply_unsubscribe(old_upstream_subscription);
            let mut pending = upstream_subscriptions.borrow_mut();
            // If admission had not reached the upstream owner yet, removing
            // the retained old open is sufficient. Otherwise its local and
            // remote receiver both need the normal unsubscribe lifecycle.
            let old_open_was_pending = pending.iter().any(|command| {
                matches!(
                    command,
                    PendingUpstreamCommand::Subscribe(subscription)
                        if subscription.subscription == old_upstream_subscription
                )
            });
            pending.retain(|command| {
                !matches!(
                    command,
                    PendingUpstreamCommand::Subscribe(subscription)
                        if subscription.subscription == old_upstream_subscription
                )
            });
            if old_owner.is_some() && !old_open_was_pending {
                pending.push(PendingUpstreamCommand::Unsubscribe(
                    old_upstream_subscription,
                ));
            }
            self.relay_upstream_subscription_owners.borrow_mut().insert(
                fresh_upstream_subscription,
                RelayUpstreamSubscriptionOwner {
                    downstream_connection_epoch: connection_epoch,
                    coverage,
                    policy_binding: refreshed_direct_binding.clone(),
                    downstream_subscriptions,
                },
            );
            pending.push(PendingUpstreamCommand::Subscribe(
                PendingUpstreamSubscription {
                    subscription: fresh_upstream_subscription,
                    shape,
                    binding,
                    opts,
                    identity: refreshed_direct_binding.0,
                    policy_binding: Some(refreshed_direct_binding.clone()),
                },
            ));
        }
        if refreshed_upstream_usage {
            *serve_dirty = true;
            schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
        }
        let mut rebind_pending = false;
        for (coverage, shape, binding, policy_binding, binding_origin, subscribers) in groups {
            let maintained_subscription = coverage_group_subscription_key(&coverage);
            peer.set_subscription_policy_binding(maintained_subscription, policy_binding);
            if binding_origin == CoveragePolicyBindingOrigin::DirectAdmitted {
                // Update every concrete usage site before reopening the shared
                // evaluator. A cold rehydrate can yield, but no later repair
                // or resumed delta may retain the stale direct snapshot.
                for subscription in &subscribers {
                    peer.set_subscription_policy_binding(
                        *subscription,
                        refreshed_direct_binding.clone(),
                    );
                }
            }
            if deferred_rehydrates.contains(&coverage) {
                let source = refreshed_authority_sources
                    .get(&coverage)
                    .expect("every deferred relay refresh selects a new exact U source")
                    .clone();
                peer.set_subscription_authority_result_source(
                    maintained_subscription,
                    source.clone(),
                );
                peer.set_subscription_awaiting_selected_authority_source(
                    maintained_subscription,
                    true,
                );
                for subscription in &subscribers {
                    peer.set_subscription_authority_result_source(*subscription, source.clone());
                }
                // The old maintained group was fed by the old upstream usage
                // and may still contain rows now forbidden by the refreshed
                // session. Tear it down rather than rehydrating from that
                // stale source. The normal owner loop will open a fresh reset
                // only after the new usage site has settled upstream.
                continue;
            }
            let update = {
                let mut node = self.node.lock().await;
                let mut node = node.scoped_active_session_claims(
                    session_claim_binding.as_ref().expect("subscriber claims").0,
                    session_claim_binding
                        .as_ref()
                        .expect("subscriber claims")
                        .1
                        .clone(),
                );
                peer.rehydrate_query_for_subscription_with_opts_and_waker(
                    &mut node,
                    maintained_subscription,
                    &shape,
                    &binding,
                    coverage.opts,
                    progress_waker,
                )
                .await?
            };
            let Some(update) = update else {
                // A cold maintained view may yield while storage/runtime work
                // is pending. Do not acknowledge the claim revision until we
                // have actually replaced every direct usage site's result
                // set: otherwise the old authorized membership survives
                // forever after this one failed attempt.
                rebind_pending = true;
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
                let refreshed_scope = (binding_origin
                    == CoveragePolicyBindingOrigin::DirectAdmitted)
                    .then(|| {
                        prior_scope.as_ref().and_then(|prior| {
                            refresh_authorized_scope_purpose(
                                &self.node.borrow(),
                                identity,
                                &session_claim_binding.as_ref().expect("subscriber claims").1,
                                subscription,
                                &shape,
                                &binding,
                                &prior,
                            )
                        })
                    })
                    .flatten();
                if let Some(refreshed) = &refreshed_scope {
                    move_scope_aggregate_member(
                        scope_aggregates,
                        prior_scope.as_ref(),
                        refreshed,
                        subscription,
                    );
                    scope_purposes.insert(subscription, refreshed.clone());
                } else if binding_origin == CoveragePolicyBindingOrigin::DirectAdmitted
                    && let Some(prior) = scope_purposes.remove(&subscription)
                {
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
                send_subscriber_with_sync_context(
                    &self.node,
                    peer,
                    self.transport.as_mut(),
                    &self.local_fate_routes,
                    &self.downstream_fates,
                    update,
                )?;
                if let Some((subscription, receipt)) = receipt {
                    queue_direct_control(
                        &mut self.pending_control_responses,
                        SyncMessage::AuthorizationScopeReceipt {
                            subscription,
                            receipt,
                        },
                    );
                }
            }
        }
        for served_current_rows in served_current_rows.values_mut() {
            if served_current_rows.policy_binding_origin
                != CoveragePolicyBindingOrigin::DirectAdmitted
            {
                continue;
            }
            served_current_rows.policy_binding = refreshed_direct_binding.clone();
            let subscription = self
                .node
                .borrow()
                .whole_table_subscription_key(&served_current_rows.table)?;
            peer.set_subscription_policy_binding(subscription, refreshed_direct_binding.clone());
            let update = {
                let mut node = self.node.lock().await;
                let mut node = node.scoped_active_session_claims(
                    refreshed_direct_binding.0,
                    refreshed_direct_binding.1.clone(),
                );
                // `current_rows_update` deliberately retains its maintained
                // receiver for ordinary deltas. A claim refresh changes its
                // immutable policy input, so drop it and send a full reset
                // under the newly admitted snapshot instead.
                peer.forget_subscription_with_node(&mut node, subscription);
                peer.reset_current_rows(&mut node, &served_current_rows.table)
                    .await?
            };
            send_subscriber_with_sync_context(
                &self.node,
                peer,
                self.transport.as_mut(),
                &self.local_fate_routes,
                &self.downstream_fates,
                update,
            )?;
        }

        if rebind_pending {
            schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
            return Ok(true);
        }
        self.observed_session_claim_revision.set(current_revision);
        Ok(true)
    }

    /// Serve a whole-table current-row view to this subscriber immediately and
    /// refresh it on later ticks.
    pub async fn serve_current_rows(&mut self, table: &str) -> Result<(), Error> {
        self.tick().await?;
        // This is an owner-loop admission path, not a standalone peer helper.
        // Capture the exact claims accepted for this connection before opening
        // its maintained view; the author-keyed NodeState cache is only a
        // compatibility input and may already contain a sibling session's
        // claims for the same subject.
        let policy_binding = self
            .subscriber_session_claim_binding()
            .expect("subscriber claims");
        let ConnectionLink::Subscriber(SubscriberConnectionState {
            peer,
            served,
            served_current_rows,
            ..
        }) = &mut self.link
        else {
            return Ok(());
        };
        let subscription = self.node.borrow().whole_table_subscription_key(table)?;
        if let Some(existing) = served_current_rows.get(&subscription) {
            if existing.table == table {
                return Ok(());
            }
            return Err(Error::new(
                ErrorCode::Protocol,
                "whole-table subscription key is already owned by another current-row view",
            ));
        }
        if served.contains_key(&subscription) {
            return Err(Error::new(
                ErrorCode::Protocol,
                "whole-table subscription key is already owned by an ordinary subscription",
            ));
        }
        peer.set_subscription_policy_binding(subscription, policy_binding.clone());
        let update = {
            let mut node = self.node.lock().await;
            let mut node =
                node.scoped_active_session_claims(policy_binding.0, policy_binding.1.clone());
            peer.current_rows_update(&mut node, table).await?
        };
        self.last_resume_bytes = Some(serialized_sync_message_len(&update));
        debug_assert_eq!(view_update_subscription(&update), Some(subscription));
        send_sync_message_chunked(self.transport.as_mut(), update)?;
        served_current_rows.insert(
            subscription,
            ServedCurrentRows {
                table: table.to_owned(),
                policy_binding,
                policy_binding_origin: CoveragePolicyBindingOrigin::DirectAdmitted,
            },
        );
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

    #[cfg(test)]
    pub(crate) fn scope_relay_admission_epoch_for_test(&self) -> Option<u64> {
        let ConnectionLink::Subscriber(SubscriberConnectionState { peer, .. }) = &self.link else {
            return None;
        };
        peer.scope_relay_admission_epoch_for_test()
    }

    #[cfg(test)]
    pub(crate) fn scope_relay_binding_for_test(
        &self,
    ) -> Option<(AuthorSubject, BTreeMap<String, Value>)> {
        let ConnectionLink::Subscriber(SubscriberConnectionState { peer, .. }) = &self.link else {
            return None;
        };
        peer.scope_relay_binding_for_test()
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
        let replacement = match peer.role() {
            PeerRole::Relay => PeerState::relay(),
            PeerRole::ClientLink { identity } => PeerState::client_link(identity),
        };
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
        let progress_waker = self
            .scheduler
            .borrow()
            .as_ref()
            .and_then(|scheduler| scheduler.query_runtime_waker());
        let session_claim_binding = self.subscriber_session_claim_binding();
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
                    group.policy_binding.clone(),
                    group.authority_result_subscription,
                    group.awaiting_upstream_settlement,
                    group.subscribers.iter().copied().collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        for (
            coverage,
            shape,
            binding,
            policy_binding,
            authority_result_subscription,
            awaiting_upstream_settlement,
            subscribers,
        ) in groups
        {
            let group_subscription = coverage_group_subscription_key(&coverage);
            peer.set_subscription_policy_binding(group_subscription, policy_binding);
            let scope_relay = self.node.borrow().client_relay_scope().is_some();
            if awaiting_upstream_settlement {
                let authority_result_source = self
                    .node
                    .borrow()
                    .authority_result_key_for_subscription(authority_result_subscription)
                    .ok();
                let Some(authority_result_source) = authority_result_source else {
                    // A strict scope relay may not rehydrate from its overlay
                    // or guess a sibling's receipt. Its separately admitted U
                    // source must be registered first.
                    *serve_dirty = true;
                    continue;
                };
                if scope_relay
                    && !self
                        .node
                        .borrow()
                        .has_settled_authority_result(&authority_result_source)
                {
                    // Recovery cannot turn an unsettled strict relay read into
                    // a definitive empty result. Wait for the selected U.
                    *serve_dirty = true;
                    continue;
                }
                peer.set_subscription_authority_result_source(
                    group_subscription,
                    authority_result_source,
                );
                peer.set_subscription_awaiting_selected_authority_source(
                    group_subscription,
                    scope_relay,
                );
            } else {
                peer.set_subscription_awaiting_selected_authority_source(group_subscription, false);
            }
            let update = {
                let mut node = self.node.lock().await;
                let mut node = node.scoped_active_session_claims(
                    session_claim_binding.as_ref().expect("subscriber claims").0,
                    session_claim_binding
                        .as_ref()
                        .expect("subscriber claims")
                        .1
                        .clone(),
                );
                peer.rehydrate_query_for_subscription_with_opts_and_waker(
                    &mut node,
                    group_subscription,
                    &shape,
                    &binding,
                    coverage.opts.clone(),
                    progress_waker.as_ref(),
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
                        &session_claim_binding.as_ref().expect("subscriber claims").1,
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
                send_subscriber_with_sync_context(
                    &self.node,
                    peer,
                    self.transport.as_mut(),
                    &self.local_fate_routes,
                    &self.downstream_fates,
                    update,
                )?;
                if let Some((subscription, receipt)) = receipt {
                    queue_direct_control(
                        &mut self.pending_control_responses,
                        SyncMessage::AuthorizationScopeReceipt {
                            subscription,
                            receipt,
                        },
                    );
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
        let progress_waker = self
            .scheduler
            .borrow()
            .as_ref()
            .and_then(|scheduler| scheduler.query_runtime_waker());
        let connection_epoch = self.connection_epoch;
        // The host-admitted scope-isolated worker owns one immutable foreground
        // session and may forward that exact binding upstream. A generic
        // multiplexed relay has no per-binding admission capability and must
        // forward rather than select a user binding. Raw wire input cannot
        // enable either path.
        let permits_delegated_sessions = self.transport.permits_delegated_sessions()
            || self.node.borrow().client_relay_scope().is_some();
        self.observe_shared_subscriber_dirty_epoch();
        let session_claim_binding = self.subscriber_session_claim_binding();
        self.bind_subscriber_session_claims();
        self.rebind_subscriber_views_after_claim_change(progress_waker.as_ref())
            .await?;
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
                pending_row_version_fetches,
                pending_row_version_repairs,
                scope_view_cuts,
                scope_receipts,
                expected_scope_authority,
                scope_lease_manager,
            }) => {
                let stop = Box::pin(async {
                    let outbound_stop = Box::pin(async {
                        if let Some(request) = pending_row_version_fetches.front().cloned() {
                            let delegated_session = (permits_delegated_sessions
                                && request.policy_binding.0 != AuthorSubject::SYSTEM)
                                .then_some(crate::protocol::DelegatedSessionBinding {
                                    identity: request.policy_binding.0,
                                    claims: request.policy_binding.1,
                                });
                            if let Err(error) = self
                                .transport
                                .send(SyncMessage::FetchRowVersions {
                                    requests: request.requests,
                                    delegated_session,
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
                            pending_row_version_fetches.pop_front();
                        }
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
                        if permits_delegated_sessions {
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
                        }
                        let pending_index = 0;
                        while pending_index < pending.len() {
                            match &mut pending[pending_index] {
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
                                            progress_waker.as_ref(),
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
                                            if permits_delegated_sessions {
                                                pending_subscription.policy_binding.as_ref()
                                            } else {
                                                None
                                            },
                                        )
                                        .await?;
                                    let subscribe = Subscribe {
                                        shape_id: shape.shape_id(),
                                        subscription: pending_subscription.subscription,
                                        values,
                                        known_state,
                                        delegated_session: permits_delegated_sessions
                                            .then(|| pending_subscription.policy_binding.clone())
                                            .flatten()
                                            .map(|(identity, claims)| {
                                                crate::protocol::DelegatedSessionBinding {
                                                    identity,
                                                    claims,
                                                }
                                            }),
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
                                        progress_waker.as_ref(),
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
                                    announced_shapes.remove(&(
                                        subscription.shape_id,
                                        subscription.read_view,
                                    ));
                                    // Local finalization may already have
                                    // applied this retirement. Reapplying is
                                    // idempotent, and the command remains in
                                    // `pending` until the send succeeds.
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
                                    session_claim_binding: pending_session_claim_binding,
                                } => {
                                    // An old or unauthenticated upstream must never receive a
                                    // downgraded preflight.  Resolve conservatively instead.
                                    if expected_scope_authority.is_none() {
                                        if let Some(request) =
                                            scope_lease_manager.requests.remove(request_id)
                                        {
                                            let mut waiters = self.permission_advice_waiters.borrow_mut();
                                            for waiter_id in request.waiters {
                                                waiters.remove(&waiter_id);
                                            }
                                        } else {
                                            self.permission_advice_waiters
                                                .borrow_mut()
                                                .remove(request_id);
                                        }
                                    } else {
                                        let has_live_waiter = scope_lease_manager
                                            .requests
                                            .get(request_id)
                                            .map(|request| {
                                                let waiters = self.permission_advice_waiters.borrow();
                                                request
                                                    .waiters
                                                    .iter()
                                                    .any(|waiter_id| waiters.contains_key(waiter_id))
                                            })
                                            .unwrap_or_else(|| {
                                                self.permission_advice_waiters
                                                    .borrow()
                                                    .contains_key(request_id)
                                            });
                                        if !has_live_waiter {
                                            scope_lease_manager.requests.remove(request_id);
                                            pending.remove(pending_index);
                                            continue;
                                        }
                                        let Some(expected) = expected_scope_authority else {
                                            continue;
                                        };
                                        let session_claim_binding = pending_session_claim_binding
                                            .clone()
                                            .or_else(|| {
                                                scope_lease_manager
                                                    .requests
                                                    .get(request_id)
                                                    .map(|request| {
                                                        request.session_claim_binding.clone()
                                                    })
                                            })
                                            .unwrap_or_else(|| {
                                                let identity = expected.link;
                                                let claims = self
                                                    .node
                                                    .borrow()
                                                    .session_claims_with_revisions()
                                                    .into_iter()
                                                    .find_map(|(subject, claims, _)| {
                                                        (subject == identity).then_some(claims)
                                                    })
                                                    .unwrap_or_default();
                                                (identity, claims)
                                            });
                                        // Allocation establishes the immutable
                                        // request binding before the first wire
                                        // send. A backpressured command remains
                                        // in this queue, so it must carry the
                                        // same binding on a later turn.
                                        *pending_session_claim_binding =
                                            Some(session_claim_binding.clone());
                                        let claims_still_bound = self
                                            .node
                                            .borrow()
                                            .session_claims_with_revisions()
                                            .into_iter()
                                            .find_map(|(identity, claims, _)| {
                                                (identity == session_claim_binding.0)
                                                    .then_some(claims)
                                            });
                                        let claims_still_bound = claims_still_bound
                                            .unwrap_or_default()
                                            == session_claim_binding.1;
                                        if !claims_still_bound {
                                            if let Some(request) =
                                                scope_lease_manager.requests.remove(request_id)
                                            {
                                                for waiter_id in request.waiters {
                                                    if let Some(waiter) = self
                                                        .permission_advice_waiters
                                                        .borrow_mut()
                                                        .remove(&waiter_id)
                                                    {
                                                        let _ =
                                                            waiter.send(PermissionAdvice::Unknown);
                                                    }
                                                }
                                            } else if let Some(waiter) = self
                                                .permission_advice_waiters
                                                .borrow_mut()
                                                .remove(request_id)
                                            {
                                                let _ = waiter.send(PermissionAdvice::Unknown);
                                            }
                                            pending.remove(pending_index);
                                            continue;
                                        }
                                        let existing = scope_lease_manager
                                            .requests
                                            .iter()
                                            .find(|(_, request)| {
                                                request.action == *action
                                                    && request.session_claim_binding
                                                        == session_claim_binding
                                            })
                                            .map(|(wire_request_id, request)| {
                                                (*wire_request_id, request.intent_sent)
                                            });
                                        if let Some((wire_request_id, intent_sent)) = existing {
                                            let request = scope_lease_manager
                                                .requests
                                                .get_mut(&wire_request_id)
                                                .expect("authorization scope request still exists");
                                            request.waiters.insert(*request_id);
                                            if !intent_sent {
                                                if let Err(error) = self.transport.send(
                                                    SyncMessage::AuthorizationScopeIntent {
                                                        request_id: wire_request_id,
                                                        action: request.action.clone(),
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
                                                request.intent_sent = true;
                                            }
                                        } else {
                                            scope_lease_manager.requests.insert(
                                                *request_id,
                                                AuthorizationScopeLeaseRequest {
                                                    action: action.clone(),
                                                    session_claim_binding,
                                                    waiters: BTreeSet::from([*request_id]),
                                                    intent_sent: false,
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
                                            scope_lease_manager
                                                .requests
                                                .get_mut(request_id)
                                                .expect("inserted authorization scope request")
                                                .intent_sent = true;
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
                                        &self.subscriptions,
                                        &mut pending_view_updates,
                                        &self.awaiting_initial_authority_coverage,
                                        &mut pending_initial_coverage_clears,
                                        &self.query_coverage_registrations,
                                        &self.active_authority_view_receipts,
                                        &self.coverage_refresh_generations,
                                        &self.subscriber_dirty_epoch,
                                        &self.scheduler,
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
                                        &self.subscriptions,
                                        &mut pending_view_updates,
                                        &self.awaiting_initial_authority_coverage,
                                        &mut pending_initial_coverage_clears,
                                        &self.query_coverage_registrations,
                                        &self.active_authority_view_receipts,
                                        &self.coverage_refresh_generations,
                                        &self.subscriber_dirty_epoch,
                                        &self.scheduler,
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
                                    let applied_bundles = node.apply_row_version_payloads_for_requests(
                                        &repair.requests,
                                        version_bundles,
                                    )
                                    .await?;
                                    // Only the still-selected authority receipt can later be
                                    // served to this durable foreground scope without a fresh
                                    // policy check. A stale/fallback repair may populate the
                                    // local cache, but never grants durable disclosure authority.
                                    node.record_scope_relay_authoritative_repair_payloads(
                                        &applied_bundles,
                                        repair.authority_receipt_eligible,
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
                                    let policy_binding = self
                                        .relay_upstream_subscription_owners
                                        .borrow()
                                        .get(&subscription)
                                        .map(|owner| owner.policy_binding.clone())
                                        .ok_or_else(|| Error::new(
                                            ErrorCode::Protocol,
                                            "row-version repair lost its subscription policy binding",
                                        ))?;
                                    pending_row_version_fetches.push_back(PendingRowVersionFetch {
                                        requests: missing.clone(),
                                        policy_binding,
                                    });
                                    pending_row_version_repairs.push_back(
                                        PendingRowVersionRepair {
                                            requests: missing,
                                            update: message,
                                            authority_receipt_eligible,
                                        },
                                    );
                                    schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                    return Ok(true);
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
                                            policy_binding: owner.policy_binding,
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
                                let Some((session_claim_binding, key_mismatch, needs_acquire)) = scope_lease_manager
                                    .requests
                                    .get(&request_id)
                                    .map(|prior| {
                                        (
                                            prior.session_claim_binding.clone(),
                                            prior.key.as_ref().is_some_and(|known| known != &key)
                                                || prior
                                                    .clause_count
                                                    .is_some_and(|known| known != clause_count),
                                            prior.lease.is_none(),
                                        )
                                    })
                                else {
                                    // A cancelled intent cannot be revived by a
                                    // late/replayed authority view.
                                    continue;
                                };
                                let claims_still_bound = self
                                    .node
                                    .borrow()
                                    .session_claims_with_revisions()
                                    .into_iter()
                                    .find_map(|(identity, claims, _)| {
                                        (identity == session_claim_binding.0).then_some(claims)
                                    })
                                    .unwrap_or_default()
                                    == session_claim_binding.1;
                                if !claims_still_bound {
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
                                    continue;
                                }
                                if key_mismatch {
                                    drop_peer_request(&self.node);
                                    continue;
                                }
                                // The first authenticated view reveals the
                                // server-selected scope key.  Acquire here, before
                                // the aggregate receipt, so concurrent actions
                                // that compile to this same support scope share
                                // one registry lifecycle rather than racing after
                                // hydration has already completed.
                                let acquired = if needs_acquire {
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
                                // Reject a receipt before applying any queued
                                // scope views when this request's admitted
                                // claims have changed. A B-scoped proof must
                                // never materialize support for an A request.
                                let claims_still_bound = scope_lease_manager
                                    .requests
                                    .get(&request_id)
                                    .is_none_or(|request| {
                                        self.node
                                            .borrow()
                                            .session_claims_with_revisions()
                                            .into_iter()
                                            .find_map(|(identity, claims, _)| {
                                                (identity == request.session_claim_binding.0)
                                                    .then_some(claims)
                                            })
                                            .unwrap_or_default()
                                            == request.session_claim_binding.1
                                    });
                                if !claims_still_bound {
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
                                    continue;
                                }
                                // The authority's FIFO ordering says this receipt
                                // follows the views, but apply the queued views now
                                // so receipt admission is never merely queued.
                                if !pending_view_updates.is_empty() {
                                    apply_pending_authority_view_updates(
                                        &self.node,
                                        &self.subscriptions,
                                        &mut pending_view_updates,
                                        &self.awaiting_initial_authority_coverage,
                                        &mut pending_initial_coverage_clears,
                                        &self.query_coverage_registrations,
                                        &self.active_authority_view_receipts,
                                        &self.coverage_refresh_generations,
                                        &self.subscriber_dirty_epoch,
                                        &self.scheduler,
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
                                    let claims_still_bound = self
                                        .node
                                        .borrow()
                                        .session_claims_with_revisions()
                                        .into_iter()
                                        .find_map(|(identity, claims, _)| {
                                            (identity == request.session_claim_binding.0)
                                                .then_some(claims)
                                        })
                                        .unwrap_or_default()
                                        == request.session_claim_binding.1;
                                    if !claims_still_bound {
                                        let waiter_ids = request.waiters.clone();
                                        scope_lease_manager.requests.remove(&request_id);
                                        for waiter_id in waiter_ids {
                                            if let Some(waiter) = self
                                                .permission_advice_waiters
                                                .borrow_mut()
                                                .remove(&waiter_id)
                                            {
                                                let _ = waiter.send(PermissionAdvice::Unknown);
                                            }
                                        }
                                        continue;
                                    }
                                    // A claim/catalogue/progress transition can
                                    // race a just-completed hydration.  Retire its
                                    // lease and allocate a new opaque wire id so
                                    // old views/receipts cannot revive the
                                    // operation; retain the caller waiters and
                                    // reacquire under the observed context.
                                    let retry_id =
                                        PermissionAdviceRequestId(*uuid::Uuid::new_v4().as_bytes());
                                    let action = request.action.clone();
                                    let session_claim_binding = request.session_claim_binding.clone();
                                    let waiters = request.waiters.clone();
                                    scope_lease_manager.requests.remove(&request_id);
                                    scope_lease_manager.requests.insert(
                                        retry_id,
                                        AuthorizationScopeLeaseRequest {
                                            action: action.clone(),
                                            session_claim_binding: session_claim_binding.clone(),
                                            waiters,
                                            intent_sent: false,
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
                                            session_claim_binding: Some(session_claim_binding),
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
                                let session_claim_binding = request.session_claim_binding.clone();
                                let waiter_ids = request.waiters.clone();
                                scope_lease_manager.requests.remove(&request_id);
                                let advice = {
                                    let mut node = self.node.lock().await;
                                    let mut node = node.scoped_active_session_claims(
                                        session_claim_binding.0,
                                        session_claim_binding.1,
                                    );
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
                                let admitted = *self.admitted_upstream_authority.borrow();
                                let current_authority_receipt_eligible =
                                    authority_receipt_eligible
                                        && expected_scope_authority.is_some_and(|expected| {
                                            admitted.is_some_and(|admitted| {
                                                admitted.same_admitted_link(expected)
                                            })
                                        });
                                let routed_fate = matches!(
                                    &message,
                                    SyncMessage::FateUpdate { tx_id, .. }
                                        if self.edge_fate_routes.borrow().contains_key(tx_id)
                                );
                                // The Edge outbox is tied to an authenticated
                                // selected authority. Ordinary direct uploads
                                // keep their legacy featureless-link receipt
                                // behavior, which has no routed Edge fate to
                                // discharge.
                                let outbox_release_receipt_eligible =
                                    current_authority_receipt_eligible
                                        || (!routed_fate
                                            && authority_receipt_eligible
                                            && expected_scope_authority.is_none());
                                if let SyncMessage::FateUpdate { tx_id: _, .. } = &message {
                                    // Authenticated authority links must match the
                                    // currently admitted connection, not merely
                                    // deliver a direct, unstaged frame.
                                    // Gate fate before any NodeState mutation. A
                                    // parallel, stale, or featureless upstream is
                                    // not merely forbidden from forwarding an
                                    // edge route; it must not settle the routed
                                    // transaction's local state. Ordinary Core
                                    // client links have no edge route and retain
                                    // their normal fate transport.
                                    if routed_fate && !current_authority_receipt_eligible {
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
                                        global_time,
                                        durability,
                                        ..
                                    } if outbox_release_receipt_eligible
                                        && (matches!(fate, Fate::Rejected(_))
                                            || (matches!(fate, Fate::Accepted)
                                                && global_time.is_some()
                                                && durability.is_some_and(|tier| {
                                                    tier >= DurabilityTier::Global
                                                }))) =>
                                    {
                                        Some(*tx_id)
                                    }
                                    _ => None,
                                };
                                if !pending_view_updates.is_empty() {
                                    apply_pending_authority_view_updates(
                                        &self.node,
                                        &self.subscriptions,
                                        &mut pending_view_updates,
                                        &self.awaiting_initial_authority_coverage,
                                        &mut pending_initial_coverage_clears,
                                        &self.query_coverage_registrations,
                                        &self.active_authority_view_receipts,
                                        &self.coverage_refresh_generations,
                                        &self.subscriber_dirty_epoch,
                                        &self.scheduler,
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
                                    if let Some(obligation) = routes.get_mut(&tx_id) {
                                        let mut remaining = Vec::new();
                                        for route in std::mem::take(&mut obligation.routes) {
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
                                            let obligation = routes
                                                .get_mut(&tx_id)
                                                .expect("route remains present");
                                            obligation.routes = remaining;
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
                                &self.browser_relay_recovered_tx_ids,
                                &self.scheduler,
                                tx_id,
                            );
                        }
                        applied = true;
                    }
                    if !pending_view_updates.is_empty() {
                        apply_pending_authority_view_updates(
                            &self.node,
                            &self.subscriptions,
                            &mut pending_view_updates,
                            &self.awaiting_initial_authority_coverage,
                            &mut pending_initial_coverage_clears,
                            &self.query_coverage_registrations,
                            &self.active_authority_view_receipts,
                            &self.coverage_refresh_generations,
                            &self.subscriber_dirty_epoch,
                            &self.scheduler,
                            self.connection_epoch,
                        )
                        .await?;
                    }
                    if applied {

                        let mut persisted = Vec::with_capacity(publications.len());
                        for publication in &publications {
                            persisted.push((publication.tx_id(), publication.persist().await));
                        }
                        let mut node = self.node.lock().await;
                        for (tx_id, persistence) in persisted {
                            node.settle_published_transaction(tx_id, persistence)?;
                        }
                        drop(node);
                        // Durable application is complete at this boundary. A
                        // refresh failure belongs to the resident subscriptions;
                        // returning it would discard this tick's progress receipt
                        // and make the already-consumed batch eligible for replay.
                        let refresh_result = {
                            #[cfg(any(test, feature = "testing"))]
                            {
                                if self.fail_next_subscription_refresh.replace(false) {
                                    Err(Error::new(
                                        ErrorCode::Protocol,
                                        "injected subscription refresh failure",
                                    ))
                                } else {
                                    refresh_subscriptions_in(
                                        &self.node,
                                        &self.subscriptions,
                                        &self.active_authority_view_receipts,
                                        progress_waker.as_ref(),
                                    )
                                    .await
                                }
                            }
                            #[cfg(not(any(test, feature = "testing")))]
                            {
                                refresh_subscriptions_in(
                                    &self.node,
                                    &self.subscriptions,
                                    &self.active_authority_view_receipts,
                                    progress_waker.as_ref(),
                                )
                                .await
                            }
                        };
                        stats.subscription_events += match refresh_result {
                            Ok(changed) => changed,
                            Err(error) => {
                                route_subscription_refresh_failure(&self.subscriptions, &error)
                            }
                        };
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
                let mut scheduled_follow_up = false;
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
                    // may have many downstream wire subscriptions. The relay
                    // rejection already owns this bounded recipient set, so
                    // move its responses to the semantic control queue rather
                    // than asking the byte transport to retain another
                    // logical message after its one admitted backlog.
                    for subscription in active_subscriptions {
                        queue_direct_control(&mut self.pending_control_responses,
                            SyncMessage::SubscribeRejected {
                                subscription,
                                reason: rejection.reason.clone(),
                            },
                        );
                    }
                    if let Some(group) = coverage_groups.remove(&rejection.coverage) {
                        let group_subscription = coverage_group_subscription_key(&rejection.coverage);
                        let mut node = self.node.borrow_mut();
                        // `group_subscription` owns the one shared maintained
                        // evaluator. The individual subscribers are still
                        // separately registered wire usage sites, including
                        // the case where a peer deliberately used noncanonical
                        // binding handles. Retire both layers: dropping only
                        // the group leaves the individual registrations and
                        // known-state declarations resident after rejection.
                        // The peer facade may not have created publication
                        // state yet for an opening group, but registration is
                        // already policy-scoped in the node. Retire that
                        // exact group usage first; a bare wire unsubscribe is
                        // intentionally ambiguous across reader scopes.
                        node.apply_unsubscribe_with_admitted_policy_binding(
                            group_subscription,
                            crate::protocol::PolicyBindingKey::from_canonical_parts(
                                rejection.policy_binding.0,
                                rejection.policy_binding.1.clone(),
                            ),
                        );
                        peer.forget_subscription_with_node(&mut node, group_subscription);
                        for subscription in group.subscribers {
                            node.apply_unsubscribe_with_admitted_policy_binding(
                                subscription,
                                crate::protocol::PolicyBindingKey::from_canonical_parts(
                                    rejection.policy_binding.0,
                                    rejection.policy_binding.1.clone(),
                                ),
                            );
                            if subscription != group_subscription {
                                peer.forget_subscription_with_node(&mut node, subscription);
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
                if !flush_downstream_fates(
                    &self.node,
                    peer,
                    self.transport.as_mut(),
                    &self.downstream_fates,
                    &self.scheduler,
                )? {
                    return Ok(true);
                }
                if !flush_pending_chunk_response(
                    self.transport.as_mut(),
                    &mut self.pending_chunk_response,
                    &self.scheduler,
                )? {
                    return Ok(true);
                }
                if !flush_pending_control_responses(
                    &self.node,
                    peer,
                    self.transport.as_mut(),
                    &mut self.pending_control_responses,
                    &self.scheduler,
                )? {
                    return Ok(true);
                }
                if let Some(message) = self.auxiliary_pump.take_outbound(64) {
                    if let Err(error) = self.transport.send(message.clone()) {
                        self.auxiliary_pump.restore_outbound(message);
                        if handle_transport_backpressure(&self.node, &self.scheduler, &error) {
                            return Ok(true);
                        }
                        return Err(transport_error(error));
                    }
                    self.auxiliary_pump.acknowledge_outbound(&message);
                }
                // `SyncMessage` contains several large wire payload variants.
                // Keep an inbound message heap-owned while this async serving
                // turn awaits policy and storage work; matching it by value
                // below moves only the selected payload out of the box instead
                // of reserving every variant in the tick future's stack frame.
                while let Some(message) = self.transport.try_recv().map(Box::new) {
                    // Authorization support is authority-owned in Phase 3.
                    // A subscriber must never be able to smuggle a support
                    // purpose alongside its own shape/binding subscription.
                    let scope_purpose: Option<crate::protocol::AuthorizationScopePurpose> = None;
                    if subscriber_inbound_message_is_authority_only(
                        &message,
                        *ingest_context,
                        peer,
                    )
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
                    match *message {
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
                                debug_assert!(
                                    self.pending_chunk_response.is_none(),
                                    "subscriber drains a retained chunk response before reading another request"
                                );
                                self.pending_chunk_response = Some(ChunkResponseBatch { responses });
                                schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                return Ok(true);
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
                                &mut self.pending_control_responses,
                                ingest_context.identity,
                                session_claim_binding
                                    .as_ref()
                                    .expect("subscriber claims")
                                    .1
                                    .clone(),
                                connection_epoch,
                                request_id,
                                action,
                                ingest_context.trust,
                                authority_scope_hydrations,
                                authority_scope_hydration_count,
                                progress_waker.as_ref(),
                            )
                            .await?;
                            if !self.pending_control_responses.is_empty() {
                                schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                flush_subscriber_controls_or_stop!(self, peer);
                            }
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
                            // Shape admission carries the parsed AST and may
                            // compile policy dependencies. Keep that inactive
                            // state out of ordinary commit-serving turns.
                            let should_continue = Box::pin(async {
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
                            if !shape_registrations.contains_key(&registration_key)
                                && shape_registrations.len()
                                    >= MAX_SHAPE_REGISTRATIONS_PER_PEER
                            {
                                let error = crate::node::Error::UnsupportedSyncMessage(
                                    "peer shape registration limit exceeded",
                                );
                                queue_direct_control(
                                    &mut self.pending_control_responses,
                                    server_subscription_failure_rejection_message(
                                        register_shape_rejection_subscription(
                                            shape_id,
                                            read_view_key,
                                        ),
                                        &error,
                                    ),
                                );
                                schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                flush_subscriber_controls_or_stop!(self, peer);
                                return Ok(true);
                            }
                            if let Err(error) = ensure_supported_register_shape_options(
                                &opts,
                                *local_receiver,
                                peer.role(),
                                delegated_session_capability(*ingest_context, peer.role()),
                            ) {
                                shape_registrations.insert(
                                    registration_key,
                                    SubscriberShapeRegistration::RejectedUnsupportedCapability(
                                        error.message.clone(),
                                    ),
                                );
                                queue_direct_control(&mut self.pending_control_responses,
                                    unsupported_shape_capability_rejection_message(
                                        register_shape_rejection_subscription(
                                            shape_id,
                                            read_view_key,
                                        ),
                                        error.message,
                                    ),
                                );
                                schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                flush_subscriber_controls_or_stop!(self, peer);
                                return Ok(true);
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
                                        queue_direct_control(&mut self.pending_control_responses,
                                            server_subscription_failure_rejection_message(
                                                register_shape_rejection_subscription(
                                                    shape_id,
                                                    read_view_key,
                                                ),
                                                &error,
                                            ),
                                        );
                                        schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                        flush_subscriber_controls_or_stop!(self, peer);
                                        return Ok(true);
                                    } else {
                                        drop_peer_request(&self.node);
                                    }
                                    return Ok::<bool, Error>(true);
                                }
                            };
                            if let Some(shape) = &shape {
                                // Branch compilation may install an empty
                                // process-local sparse source. Defer that
                                // side-effecting preflight until Subscribe,
                                // where the authenticated branch gate is
                                // available.
                                if shape.params().is_empty()
                                    && let Some(permission_subject) =
                                        subscriber_permission_subject(*ingest_context)
                                {
                                    let binding = shape.bind(BTreeMap::new()).map_err(Error::from);
                                    let binding = match binding {
                                        Ok(binding) => binding,
                                        Err(_) => {
                                            drop_peer_request(&self.node);
                                            return Ok::<bool, Error>(true);
                                        }
                                    };
                                    let supported = {
                                        let mut node = self.node.lock().await;
                                        let mut node = node.scoped_active_session_claims(
                                            session_claim_binding.as_ref().expect("subscriber claims").0,
                                            session_claim_binding.as_ref().expect("subscriber claims").1.clone(),
                                        );
                                        node.ensure_peer_maintained_subscription_view_supported(
                                            shape,
                                            &binding,
                                            opts.tier,
                                            permission_subject,
                                            &opts.read_view,
                                            QueryAuthorizationMode::TrustedServing,
                                        )
                                        .await
                                    };
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
                                        queue_direct_control(&mut self.pending_control_responses,
                                            unsupported_shape_capability_rejection_message(
                                                subscription,
                                                detail,
                                            ),
                                        );
                                        schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                        flush_subscriber_controls_or_stop!(self, peer);
                                        return Ok(true);
                                    } else if let Err(error) = supported {
                                        queue_direct_control(&mut self.pending_control_responses,
                                            server_subscription_failure_rejection_message(
                                                SubscriptionKey {
                                                    shape_id,
                                                    binding_id: binding.binding_id(),
                                                    read_view: read_view_key,
                                                },
                                                &error,
                                            ),
                                        );
                                        schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                        flush_subscriber_controls_or_stop!(self, peer);
                                        return Ok(true);
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
                                        return Ok::<bool, Error>(true);
                                    }
                                    SubscriberShapeRegistration::RejectedUnsupportedCapability(
                                        detail,
                                    ) => {
                                        queue_direct_control(&mut self.pending_control_responses,
                                            unsupported_shape_capability_rejection_message(
                                                register_shape_rejection_subscription(
                                                    shape_id,
                                                    read_view_key,
                                                ),
                                                detail.clone(),
                                            ),
                                        );
                                        schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                        flush_subscriber_controls_or_stop!(self, peer);
                                        return Ok(true);
                                    }
                                    _ => {}
                                }
                            }
                            let rejection_subscription =
                                register_shape_rejection_subscription(shape_id, registration_key.1);
                            let register_result = self
                                .node
                                .lock()
                                .await
                                .register_shape_for_peer(connection_epoch, shape_id, ast);
                            if let Err(error) = register_result {
                                queue_direct_control(&mut self.pending_control_responses,
                                    server_subscription_failure_rejection_message(
                                        rejection_subscription,
                                        &error,
                                    ),
                                );
                                schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                flush_subscriber_controls_or_stop!(self, peer);
                                return Ok(true);
                            }
                            let registration = if awaiting_catalogue_admission {
                                SubscriberShapeRegistration::PendingCatalogueAdmission(opts)
                            } else {
                                SubscriberShapeRegistration::Registered(opts)
                            };
                            shape_registrations.insert(registration_key, registration);
                            Ok::<bool, Error>(false)
                            })
                            .await?;
                            if should_continue {
                                continue;
                            }
                        }
                        SyncMessage::Subscribe(subscribe) => {
                            // Subscription admission has a substantially larger async state
                            // machine than ordinary peer messages. Keep that state on the heap
                            // so a commit uploaded on this same connection does not carry the
                            // inactive Subscribe arm on a normal two-megabyte executor stack.
                            let should_continue = Box::pin(async {
                            let subscription_has_delegated_session = subscribe.delegated_session.is_some();
                            let session_claim_binding = admitted_request_policy_binding(
                                *ingest_context,
                                peer,
                                session_claim_binding.clone(),
                                subscribe.delegated_session.clone(),
                            );
                            if session_claim_binding.is_none() {
                                // A relay's transport is not a user. It may
                                // only carry the topology-assigned immutable
                                // session snapshot for this request.
                                drop_peer_request(&self.node);
                                return Ok::<bool, Error>(true);
                            }
                            if let Err(message) =
                                validate_known_state_declaration(&subscribe.known_state)
                            {
                                let _ = message;
                                drop_peer_request(&self.node);
                                return Ok::<bool, Error>(true);
                            }
                            let shape_id = subscribe.shape_id;
                            let subscription = subscribe.subscription;
                            if shape_id != subscription.shape_id {
                                drop_peer_request(&self.node);
                                return Ok::<bool, Error>(true);
                            }
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
                                    queue_direct_control(
                                        deferred_subscribe_rejections,
                                        unsupported_shape_capability_rejection_message(
                                            subscription,
                                            detail,
                                        ),
                                    );
                                    // This subscribe has been handled. The outer receive loop
                                    // owns iteration; returning its continue signal preserves
                                    // the deferred-rejection ordering without trying to jump out
                                    // of this heap-pinned async admission block.
                                    return Ok::<bool, Error>(true);
                                }
                                SubscriberShapeRegistration::Registered(opts)
                                | SubscriberShapeRegistration::PendingCatalogueAdmission(opts) => {
                                    opts
                                }
                            };
                            let Some(shape) = self.node.borrow().registered_shape(shape_id) else {
                                if pending_catalogue_admission {
                                    queue_direct_control(&mut self.pending_control_responses,
                                        SyncMessage::SubscribeRejected {
                                            subscription,
                                            reason: SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission,
                                        },
                                    );
                                    schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                    flush_subscriber_controls_or_stop!(self, peer);
                                    return Ok(true);
                                } else {
                                    drop_peer_request(&self.node);
                                }
                                return Ok::<bool, Error>(true);
                            };
                            if values.len() != shape.params().len() {
                                drop_peer_request(&self.node);
                                return Ok::<bool, Error>(true);
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
                                delegated_session_capability(*ingest_context, peer.role()),
                            )
                            .is_err()
                            {
                                drop_peer_request(&self.node);
                                return Ok::<bool, Error>(true);
                            }
                            let subscription_policy_binding = session_claim_binding
                                .as_ref()
                                .expect("subscriber claims")
                                .clone();
                            let mut coverage = coverage_key(&shape, &binding, opts.clone());
                            // A coverage group is an authority evaluation
                            // namespace, not merely a transport optimization.
                            // Every admitted reader gets its own immutable
                            // policy scope, including a direct subscriber on a
                            // single authenticated connection. Otherwise two
                            // direct readers with the same public query can
                            // reuse one maintained authority result.
                            coverage.policy_binding = Some(
                                crate::protocol::PolicyBindingKey::from_canonical_parts(
                                    subscription_policy_binding.0,
                                    subscription_policy_binding.1.clone(),
                                ),
                            );
                            if served_current_rows.contains_key(&subscription) {
                                drop_peer_request(&self.node);
                                return Ok::<bool, Error>(true);
                            }
                            if let Some(existing_coverage) = served.get(&subscription)
                                && existing_coverage != &coverage
                            {
                                drop_peer_request(&self.node);
                                return Ok::<bool, Error>(true);
                            }
                            if pending_catalogue_admission {
                                shape_registrations.insert(
                                    registration_key,
                                    SubscriberShapeRegistration::Registered(opts.clone()),
                                );
                            }
                            let scope_purpose = if let Some(purpose) = scope_purpose {
                                let expected_result = self
                                    .node
                                    .borrow()
                                    .authorization_support_scope_for_session(
                                        ingest_context.identity,
                                        Some(&session_claim_binding
                                            .as_ref()
                                            .expect("subscriber claims")
                                            .1),
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
                            let supported = {
                                let mut node = self.node.lock().await;
                                let mut node = node.scoped_active_session_claims(
                                    session_claim_binding.as_ref().expect("subscriber claims").0,
                                    session_claim_binding.as_ref().expect("subscriber claims").1.clone(),
                                );
                                node.ensure_peer_maintained_subscription_view_supported(
                                    &shape,
                                    &binding,
                                    opts.tier,
                                    subscription_policy_binding.0,
                                    &opts.read_view,
                                    QueryAuthorizationMode::TrustedServing,
                                )
                                .await
                            };
                            if let Err(crate::node::Error::QueryCapability(detail)) = supported {
                                queue_direct_control(&mut self.pending_control_responses,
                                    unsupported_shape_capability_rejection_message(
                                        subscription,
                                        detail,
                                    ),
                                );
                                schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                flush_subscriber_controls_or_stop!(self, peer);
                                return Ok(true);
                            } else if let Err(error) = supported {
                                queue_direct_control(&mut self.pending_control_responses,
                                    server_subscription_failure_rejection_message(subscription, &error),
                                );
                                schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                flush_subscriber_controls_or_stop!(self, peer);
                                return Ok(true);
                            }
                            let group_subscription = coverage_group_subscription_key(&coverage);
                            let local_subscriber = *local_receiver;
                            let scope_relay = self.node.borrow().client_relay_scope().is_some();
                            let upstream_opts = if local_subscriber || scope_relay {
                                let mut opts = upstream_register_shape_options(
                                    opts.tier,
                                    opts.read_view.clone(),
                                    DurabilityTier::Global,
                                    opts.propagate_upstream,
                                );
                                if self.node.borrow().client_relay_scope().is_some() {
                                    opts.binding_source = BindingSource::RelayAuthoritySession;
                                }
                                opts
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
                                &subscription_policy_binding,
                            );
                            // This is a topology-role distinction, not a
                            // transport-direction distinction.  The browser
                            // worker receives its foreground subscriber on a
                            // `local_receiver = false` link, yet it is still
                            // a non-authoritative scope relay: only its
                            // separately registered upstream usage can
                            // receive a live authority result.  A serving
                            // authority, by contrast, evaluates the incoming
                            // downstream usage itself and therefore owns D.
                            let propagates_to_selected_authority = opts.propagate_upstream
                                && (local_subscriber || scope_relay);
                            let waits_for_selected_authority = propagates_to_selected_authority
                                && opts.tier > DurabilityTier::Local;
                            let authority_result_subscription = if propagates_to_selected_authority {
                                upstream_subscription
                            } else {
                                // The concrete wire subscription is only a
                                // publication handle. The shared server-side
                                // evaluator belongs to the coverage group,
                                // whose opaque handle incorporates the
                                // admitted policy scope. Registering the
                                // public handle here would let two direct
                                // readers with an identical query overwrite
                                // each other's NodeState usage entry.
                                group_subscription
                            };
                            let first_subscriber = coverage_groups
                                .get(&coverage)
                                .is_none_or(|group| group.subscribers.is_empty());
                            let permissions_ready = subscriber_permissions_ready(
                                self.node.borrow().permissions_ready(),
                                ingest_context.trust,
                            );
                            let opening_pending = if !permissions_ready {
                                Some(SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                                    subscription,
                                    settled_through: self.node.borrow().committed_global_time(),
                                    reset_result_set: true,
                                    version_carriers: Vec::new(),
                                    peer_payload_inventory: crate::protocol::PeerPayloadInventory {
                                        opening_pending: true,
                                        ..Default::default()
                                    },
                                    result_member_adds: Vec::new(),
                                    result_member_removes: Vec::new(),
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
                            peer.set_subscription_policy_binding(
                                subscription,
                                subscription_policy_binding.clone(),
                            );
                            // The group key owns the maintained evaluator;
                            // install the same admitted snapshot before any
                            // owner-loop rehydrate or delta can touch it.
                            peer.set_subscription_policy_binding(
                                group_subscription,
                                subscription_policy_binding.clone(),
                            );
                            let outcome = {
                                let mut node = self.node.lock().await;
                                node.apply_subscribe_with_admitted_policy_binding(
                                    Subscribe {
                                        subscription: group_subscription,
                                        ..subscribe
                                    },
                                    crate::protocol::PolicyBindingKey::from_canonical_parts(
                                        subscription_policy_binding.0,
                                        subscription_policy_binding.1.clone(),
                                    ),
                                )?;
                                crate::node::PublicationOutcome::settled(Vec::<SyncMessage>::new())
                            };
                            let upstream_binding_view = BindingViewKey {
                                shape_id: shape.shape_id(),
                                binding_id: binding.binding_id(),
                                read_view: upstream_opts.read_view_key(),
                            };
                            let selected_authority_result_key =
                                (scope_relay && propagates_to_selected_authority).then(|| {
                                crate::protocol::AuthorityResultKey::policy_scoped(
                                    upstream_binding_view,
                                    crate::protocol::PolicyBindingKey::from_canonical_parts(
                                        subscription_policy_binding.0,
                                        subscription_policy_binding.1.clone(),
                                    ),
                                )
                            });
                            // Record the exact source on the concrete D
                            // usage before any recovery/publication path can
                            // emit a frame. The U wire registration is queued
                            // later in this owner-loop turn, but its canonical
                            // authority key is already determined by the
                            // immutable admitted binding. This lets the send
                            // boundary preserve opening provenance instead of
                            // accidentally publishing D's empty local overlay
                            // as a final strict result.
                            if scope_relay && propagates_to_selected_authority {
                                let selected_authority_result_key = selected_authority_result_key
                                    .clone()
                                    .expect("scope relay selects an exact authority result");
                                // The coverage-group subscription owns the
                                // maintained receiver. Install U on that
                                // owner before any publication path can open
                                // it; D only receives the group's retargeted
                                // ViewUpdates and must not be mistaken for
                                // the maintained receiver's source.
                                peer.set_subscription_authority_result_source(
                                    group_subscription,
                                    selected_authority_result_key.clone(),
                                );
                                peer.set_subscription_awaiting_selected_authority_source(
                                    group_subscription,
                                    waits_for_selected_authority,
                                );
                                peer.set_subscription_authority_result_source(
                                    subscription,
                                    selected_authority_result_key,
                                );
                            }
                            if local_subscriber
                                && upstream_opts.binding_source
                                    == BindingSource::RelayAuthoritySession
                            {
                                // Settled authority membership survives a
                                // RocksDB reopen, while its wire registration
                                // does not. Do not let that ownerless result
                                // settle this fresh Edge usage site before the
                                // new relay authority registration receives a
                                // current reset from upstream.
                                self.node
                                    .borrow_mut()
                                    .invalidate_ownerless_settled_result_view(upstream_binding_view);
                            }
                            let (_, changed, published) = finish_peer_publication_outcome_with_refresh(
                                &self.node,
                                &self.subscriptions,
                                &self.active_authority_view_receipts,
                                progress_waker.as_ref(),
                                outcome,
                                false,
                            )
                            .await?;
                            stats.subscription_events += changed;
                            needs_subscription_refresh |= published;
                            // A strict child of a non-authoritative relay
                            // cannot open its maintained receiver against the
                            // local overlay: it must wait until the exact
                            // upstream authority receipt selected above has
                            // settled. `local_receiver` describes the link,
                            // not the topology role, so a browser scope relay
                            // needs this too even though its foreground link
                            // is a normal subscriber connection. A serving
                            // authority selects D and evaluates it locally,
                            // so it deliberately does not take this handoff
                            // path.
                            let waiting_for_selected_authority_settlement =
                                waits_for_selected_authority
                                    && {
                                        // This usage site owns a distinct
                                        // receipt. A same-shaped sibling must
                                        // neither settle nor block it.
                                        let node = self.node.borrow();
                                        let selected = selected_authority_result_key
                                            .as_ref()
                                            .cloned()
                                            .or_else(|| {
                                                node.authority_result_key_for_subscription(
                                                    authority_result_subscription,
                                                )
                                                .ok()
                                            });
                                        !selected.is_some_and(|key| {
                                            node.has_settled_authority_result(&key)
                                        })
                                    };
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
                                        policy_binding: subscription_policy_binding.clone(),
                                        policy_binding_origin: if subscription_has_delegated_session {
                                            CoveragePolicyBindingOrigin::Delegated
                                        } else {
                                            CoveragePolicyBindingOrigin::DirectAdmitted
                                        },
                                        subscribers: BTreeSet::new(),
                                        pending_initial_subscribers: BTreeSet::new(),
                                        initialized: false,
                                        authority_result_subscription,
                                        upstream_subscription,
                                        upstream_opts: upstream_opts.clone(),
                                        awaiting_upstream_settlement:
                                            waiting_for_selected_authority_settlement,
                                    }
                                });
                            group.subscribers.insert(subscription);
                            group.pending_initial_subscribers.insert(subscription);
                            if let Some(selected) = selected_authority_result_key {
                                // Keep the policy-scoped U source selected at
                                // admission. A later owner-loop lookup must
                                // not collapse it to an unscoped local cache.
                                peer.set_subscription_authority_result_source(
                                    group_subscription,
                                    selected,
                                );
                            }
                            if group.upstream_opts.propagate_upstream {
                                let owner = RelayUpstreamSubscriptionOwner {
                                    downstream_connection_epoch: connection_epoch,
                                    coverage: coverage.clone(),
                                    policy_binding: group.policy_binding.clone(),
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
                                send_subscriber_with_sync_context(
                                    &self.node,
                                    peer,
                                    self.transport.as_mut(),
                                    &self.local_fate_routes,
                                    &self.downstream_fates,
                                    update,
                                )?;
                                if let Some((subscription, receipt)) = receipt {
                                    queue_direct_control(&mut self.pending_control_responses,
                                        SyncMessage::AuthorizationScopeReceipt {
                                            subscription,
                                            receipt,
                                        },
                                    );
                                    schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                    return Ok(true);
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
                                            // The relay's transport identity is SYSTEM/backend,
                                            // but each multiplexed subscription carries the
                                            // caller's admitted policy context.
                                            identity: group.policy_binding.0,
                                            policy_binding: Some(group.policy_binding.clone()),
                                        },
                                    ),
                                );
                            }
                            // Opening a subscription creates work that may
                            // require cold storage. Let the current transport
                            // owner return first, so other frames (notably
                            // local write receipts) can be ingressed before
                            // initial hydration is attempted.
                            schedule_tick_in(&self.scheduler, TickUrgency::AfterCurrentTurn);
                            scheduled_follow_up = true;
                            Ok::<bool, Error>(false)
                            })
                            .await?;
                            if should_continue {
                                continue;
                            }
                        }
                        SyncMessage::Unsubscribe { subscription } => {
                            let admitted_policy_binding = peer
                                .subscription_policy_binding(subscription)
                                .map(|(identity, claims)| {
                                    crate::protocol::PolicyBindingKey::from_canonical_parts(
                                        identity, claims,
                                    )
                                });
                            let mut node = self.node.borrow_mut();
                            if let Some(policy_binding) = admitted_policy_binding {
                                node.apply_unsubscribe_with_admitted_policy_binding(
                                    subscription,
                                    policy_binding,
                                );
                            } else {
                                node.apply_unsubscribe(subscription);
                            }
                            drop(node);
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
                                        let group_subscription = coverage_group_subscription_key(&coverage);
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
                                                // The relay owns both the
                                                // local exact authority
                                                // receipt and its wire usage
                                                // site. Retiring only the
                                                // remote wire handle leaks a
                                                // settled receipt until a
                                                // later lifecycle sweep.
                                                self.node
                                                    .borrow_mut()
                                                    .apply_unsubscribe(upstream_subscription);
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
                            let registration_key =
                                (subscription.shape_id, subscription.read_view);
                            let registration_still_served = served.keys().any(|active| {
                                active.shape_id == subscription.shape_id
                                    && active.read_view == subscription.read_view
                            });
                            if !registration_still_served
                                && shape_registrations
                                    .remove(&registration_key)
                                    .is_some_and(|registration| registration.owns_node_shape())
                                && !shape_registrations.iter().any(
                                    |((shape_id, _), registration)| {
                                        *shape_id == subscription.shape_id
                                            && registration.owns_node_shape()
                                    },
                                )
                            {
                                self.node.borrow_mut().release_shape_for_peer(
                                    connection_epoch,
                                    subscription.shape_id,
                                );
                            }
                        }
                        SyncMessage::FetchRowVersions {
                            requests,
                            delegated_session,
                        } => {
                            if let Err(message) = validate_fetch_row_versions(&requests) {
                                let _ = message;
                                drop_peer_request(&self.node);
                                continue;
                            }
                            let repair_context = if *local_receiver {
                                // This path is exclusively for a direct page
                                // (or native foreground) client link. The
                                // worker's upstream capability chose what
                                // entered the ledger; the foreground hop only
                                // proves it is the same live durable subject.
                                // `local_receiver` alone also describes local
                                // generic relays, which must never turn cache
                                // contents into scope-ledger authority.
                                let PeerRole::ClientLink { identity: peer_identity } = peer.role()
                                else {
                                    drop_peer_request(&self.node);
                                    continue;
                                };
                                let Some((session_identity, _)) = session_claim_binding.as_ref()
                                else {
                                    drop_peer_request(&self.node);
                                    continue;
                                };
                                let scope_matches = self
                                    .node
                                    .borrow()
                                    .client_relay_scope()
                                    .is_some_and(|scope| {
                                        scope.admits_session(peer_identity)
                                            && peer_identity == *session_identity
                                    });
                                if delegated_session.is_some() || !scope_matches {
                                    drop_peer_request(&self.node);
                                    continue;
                                }
                                crate::peer::RepairServingContext::ScopeIsolatedClientRelay
                            } else {
                                let repair_policy_binding = admitted_request_policy_binding(
                                    *ingest_context,
                                    peer,
                                    session_claim_binding.clone(),
                                    delegated_session,
                                );
                                let Some(repair_policy_binding) = repair_policy_binding else {
                                    drop_peer_request(&self.node);
                                    continue;
                                };
                                crate::peer::RepairServingContext::Authority {
                                    policy_binding: repair_policy_binding,
                                }
                            };
                            let responses = {
                                let mut node = self.node.lock().await;
                                peer.serve_row_versions(
                                    &mut node,
                                    &requests,
                                    repair_context,
                                )
                                .await?
                            };
                            for response in responses {
                                queue_sync_context_control(
                                    &mut self.pending_control_responses,
                                    response,
                                );
                            }
                            if !self.pending_control_responses.is_empty() {
                                schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                return Ok(true);
                            }
                        }
                        other => {
                            // Upload admission has its own policy/persistence
                            // awaits. Keep it as a separately boxed future so
                            // the subscriber owner-loop does not retain those
                            // compiler states while serving unrelated control
                            // messages or maintained-view updates.
                            let should_continue = Box::pin(async {
                            if matches!(other, SyncMessage::SessionClaims { .. })
                                && matches!(
                                    ingest_context.trust,
                                    CommitUnitTrust::Session | CommitUnitTrust::Relay
                                )
                            {
                                // Claims are fixed at host admission. A session cannot
                                // broaden itself, and a subjectless relay cannot mutate
                                // the node-wide claim cache; delegated bindings are
                                // request-local and topology-admitted instead.
                                drop_peer_request(&self.node);
                                return Ok::<bool, Error>(true);
                            }
                            let edge_client_upload = matches!(
                                &other,
                                SyncMessage::CommitUnit { tx, .. } if tx.kind == TxKind::Mergeable
                            ) && ingest_context.edge_authority
                                && matches!(peer.role(), PeerRole::ClientLink { .. });
                            let edge_upload = edge_client_upload.then(|| match &other {
                                SyncMessage::CommitUnit { tx, .. } => (tx.tx_id, other.clone()),
                                _ => unreachable!("edge upload was matched as a commit unit"),
                            });
                            let local_upload = match &other {
                                SyncMessage::CommitUnit { tx, .. } => {
                                    (!edge_client_upload).then(|| (tx.tx_id, other.clone()))
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
                                self.downstream_fates.borrow_mut().push(response);
                                return Ok::<bool, Error>(true);
                            }
                            let write_state_tx_id = write_state_update_tx_id(&other);
                            // RegisterShape (registers the shape ahead of its
                            // binding), plus the write-upload path: any
                            // responses (e.g. fate updates) flow back to the
                            // subscriber.
                            let maintenance_now_ms = self.upload_retry_clock.borrow().now_ms();
                            let outcome = dispatch_admitted_subscriber_message(
                                &self.node,
                                peer,
                                *local_receiver,
                                *ingest_context,
                                session_claim_binding.clone().expect(
                                    "subscriber dispatch has an admitted immutable session binding",
                                ),
                                &self.admitted_upstream_authority,
                                &self.edge_fate_routes,
                                &self.local_fate_routes,
                                &self.downstream_fates,
                                maintenance_now_ms,
                                other,
                            )
                            .await?;
                            let (responses, changed, published) = finish_peer_publication_outcome_with_refresh(
                                &self.node,
                                &self.subscriptions,
                                &self.active_authority_view_receipts,
                                progress_waker.as_ref(),
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
                                    &self.browser_relay_recovered_tx_ids,
                                    &self.scheduler,
                                    tx_id,
                                );
                            }
                            for response in responses {
                                if matches!(response, SyncMessage::FateUpdate { .. }) {
                                    self.downstream_fates.borrow_mut().push(response);
                                } else {
                                    send_with_sync_context(
                                        &self.node,
                                        peer,
                                        self.transport.as_mut(),
                                        response,
                                    )?;
                                }
                            }
                            if let Some((tx_id, unit)) = edge_upload {
                                let admitted = self
                                    .node
                                    .lock()
                                    .await
                                    .transaction_state(tx_id)
                                    .await
                                    .is_some_and(|(fate, _, durability)| {
                                        fate == Fate::Accepted
                                            && durability >= DurabilityTier::Edge
                                    });
                                if admitted {
                                    if queue_pending_upload_in(&outbox, tx_id, Some(unit)) {
                                        schedule_tick_in(&self.scheduler, TickUrgency::Deferred);
                                    }
                                }
                            }
                            if let Some((tx_id, unit)) = local_upload
                                && queue_pending_upload_in(&outbox, tx_id, Some(unit))
                            {
                                schedule_tick_in(&self.scheduler, TickUrgency::Deferred);
                            }
                            Ok::<bool, Error>(false)
                            })
                            .await?;
                            if should_continue {
                                continue;
                            }
                        }
                    }
                }
                // Keep post-ingress publication/refresh work in its own heap
                // future. It contains the maintained-view serving graph and
                // does not need to inflate the inbound message dispatcher.
                return Box::pin(async {
                // A client upload arriving before its action-specific support
                // view settles is retained by `PeerState`, not optimistically
                // inserted into edge history.  Drive that state on every
                // served-connection turn: the receipt/view may have settled
                // immediately before or immediately after the original
                // registration, and all pending commits must make fair
                // progress without requiring unrelated inbound traffic.
                if ingest_context.edge_authority
                    && matches!(peer.role(), PeerRole::ClientLink { .. })
                {
                    let now_ms = self.upload_retry_clock.borrow().now_ms();
                    let outcome = {
                        let mut node = self.node.lock().await;
                        peer.drain_deferred_edge_fates(&mut node, now_ms)
                        .await
                        .map_err(Error::from)?
                    };
                    let (responses, changed, published) =
                        finish_peer_publication_outcome_with_refresh(
                            &self.node,
                            &self.subscriptions,
                            &self.active_authority_view_receipts,
                            progress_waker.as_ref(),
                            outcome,
                            false,
                        )
                        .await?;
                    stats.subscription_events += changed;
                    needs_subscription_refresh |= published;
                    let admitted = responses
                        .iter()
                        .filter_map(|response| match response {
                            SyncMessage::FateUpdate {
                                tx_id,
                                fate: Fate::Accepted,
                                durability: Some(durability),
                                ..
                            } if *durability >= DurabilityTier::Edge => Some(*tx_id),
                            _ => None,
                        })
                        .collect::<BTreeSet<_>>();
                    for response in responses {
                        if let SyncMessage::FateUpdate { tx_id, .. } = &response {
                            route_edge_admission_fate(
                                &self.edge_fate_routes,
                                *tx_id,
                                &response,
                            );
                        } else {
                            send_with_sync_context(
                                &self.node,
                                peer,
                                self.transport.as_mut(),
                                response,
                            )?;
                        }
                    }
                    for tx_id in admitted {
                        let unit = self.node.lock().await.commit_unit_for(tx_id).await?;
                        if queue_pending_upload_in(&outbox, tx_id, Some(unit)) {
                            schedule_tick_in(&self.scheduler, TickUrgency::Deferred);
                        }
                    }
                }
                queue_local_acknowledgements(&self.local_fate_routes, &self.node).await;
                if !flush_downstream_fates(
                    &self.node,
                    peer,
                    self.transport.as_mut(),
                    &self.downstream_fates,
                    &self.scheduler,
                )? {
                    return Ok(true);
                }
                if needs_subscription_refresh {
                    stats.subscription_events += match refresh_subscriptions_in(
                        &self.node,
                        &self.subscriptions,
                        &self.active_authority_view_receipts,
                        progress_waker.as_ref(),
                    )
                    .await
                    {
                        Ok(changed) => changed,
                        Err(error) => {
                            route_subscription_refresh_failure(&self.subscriptions, &error)
                        }
                    };
                }
                if applied_inbound && !scheduled_follow_up {
                    schedule_tick_in(&self.scheduler, TickUrgency::AfterCurrentTurn);
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
                    schedule_tick_in(&self.scheduler, TickUrgency::AfterCurrentTurn);
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
                        let group_subscription = coverage_group_subscription_key(coverage);
                        peer.set_subscription_policy_binding(
                            group_subscription,
                            group.policy_binding.clone(),
                        );
                        // The maintained receiver is addressed by the
                        // policy-partitioned coverage-group key. Its
                        // membership source is the locally admitted
                        // downstream usage on an authority, or the separate
                        // upstream usage only on a non-authoritative relay.
                        // Keep that exact association even for groups that
                        // did not have to wait on this turn; strict relay
                        // materialization always needs it.
                        // Admission selected the canonical authority result
                        // for this coverage group.  In particular, a scoped
                        // client relay's U carries the immutable delegated
                        // policy binding.  Do not re-derive it from the wire
                        // subscription here: that can collapse a scoped U
                        // into a sibling/unscoped cache entry between
                        // registration and first publication.
                        let upstream_authority_result_key = peer
                            .subscription_authority_result_source(group_subscription)
                            .cloned()
                            .or_else(|| {
                                self.node
                                    .borrow()
                                    .authority_result_key_for_subscription(
                                        group.authority_result_subscription,
                                    )
                                    .ok()
                            });
                        let upstream_authority_is_settled = {
                            let node = self.node.borrow();
                            upstream_authority_result_key
                                .as_ref()
                                .is_some_and(|key| node.has_settled_authority_result(key))
                        };
                        let settled_handoff =
                            group.awaiting_upstream_settlement && upstream_authority_is_settled;
                        if group.awaiting_upstream_settlement && !settled_handoff {
                            continue;
                        }
                        if let Some(authority_result_key) = upstream_authority_result_key {
                            peer.set_subscription_authority_result_source(
                                group_subscription,
                                authority_result_key,
                            );
                        }
                        let pending_initial =
                            std::mem::take(&mut group.pending_initial_subscribers);
                        let serving_initial = !pending_initial.is_empty();
                        if serving_initial {
                            let mut established_subscribers = group
                                .subscribers
                                .difference(&pending_initial)
                                .copied()
                                .collect::<BTreeSet<_>>();
                            for subscription in pending_initial {
                            let cloning_existing = group.initialized
                                || peer.has_maintained_subscription(group_subscription);
                            let reconciled = if cloning_existing {
                                let result = {
                                    let mut node = self.node.lock().await;
                                    let mut node = node.scoped_active_session_claims(
                                        session_claim_binding.as_ref().expect("subscriber claims").0,
                                        session_claim_binding
                                            .as_ref()
                                            .expect("subscriber claims")
                                            .1
                                            .clone(),
                                    );
                                    peer.reconcile_maintained_subscription_for_clone(
                                        &mut node,
                                        group_subscription,
                                        &group.shape,
                                        &group.binding,
                                        &coverage.opts,
                                        progress_waker.as_ref(),
                                    )
                                    .await
                                };
                                let reconciled = match result {
                                    Ok(Some(reconciled)) => reconciled,
                                    Ok(None) => {
                                        group.pending_initial_subscribers.insert(subscription);
                                        serve_again = true;
                                        continue;
                                    }
                                    Err(crate::node::Error::QueryCapability(detail)) => {
                                        rollback_rejected_subscriber_admission(
                                            &self.node,
                                            peer,
                                            served,
                                            coverage_groups,
                                            scope_purposes,
                                            scope_aggregates,
                                            &self.relay_upstream_subscription_owners,
                                            upstream_subscriptions,
                                            connection_epoch,
                                            subscription,
                                        );
                                        queue_direct_control(
                                            &mut self.pending_control_responses,
                                            unsupported_shape_capability_rejection_message(
                                                subscription,
                                                detail,
                                            ),
                                        );
                                        schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                        return Ok(true);
                                    }
                                    Err(error) => {
                                        rollback_rejected_subscriber_admission(
                                            &self.node,
                                            peer,
                                            served,
                                            coverage_groups,
                                            scope_purposes,
                                            scope_aggregates,
                                            &self.relay_upstream_subscription_owners,
                                            upstream_subscriptions,
                                            connection_epoch,
                                            subscription,
                                        );
                                        queue_direct_control(
                                            &mut self.pending_control_responses,
                                            server_subscription_failure_rejection_message(
                                                subscription,
                                                &error,
                                            ),
                                        );
                                        schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                        return Ok(true);
                                    }
                                };
                                Some(reconciled)
                            } else {
                                None
                            };
                            if let Some(canonical_update) = reconciled
                                .as_ref()
                                .and_then(|reconciled| reconciled.canonical_update.as_ref())
                            {
                                // Reconciliation has already advanced the canonical
                                // maintained state. Publish that durable transition to
                                // every established usage before any fallible reset
                                // assembly for the new usage can fail.
                                for sibling in established_subscribers.iter().copied() {
                                    let mut sibling_update =
                                        retarget_view_update(canonical_update.clone(), sibling);
                                    stamp_view_update_authorization_progress_from(
                                        peer,
                                        group_subscription,
                                        &mut sibling_update,
                                    );
                                    let receipt =
                                        scope_purposes.get(&sibling).and_then(|purpose| {
                                            aggregate_authorization_scope_receipt_for_view(
                                                scope_aggregates,
                                                &self.node.borrow(),
                                                peer,
                                                ingest_context.identity,
                                                connection_epoch,
                                                purpose,
                                                &sibling_update,
                                            )
                                        });
                                    send_subscriber_with_sync_context(
                                        &self.node,
                                        peer,
                                        self.transport.as_mut(),
                                        &self.local_fate_routes,
                                        &self.downstream_fates,
                                        sibling_update,
                                    )?;
                                    if let Some((subscription, receipt)) = receipt {
                                        queue_direct_control(
                                            &mut self.pending_control_responses,
                                            SyncMessage::AuthorizationScopeReceipt {
                                                subscription,
                                                receipt,
                                            },
                                        );
                                    }
                                    sent_view_update = true;
                                }
                            }
                            let update_result = if let Some(reconciled) = reconciled {
                                let mut node = self.node.lock().await;
                                let mut node = node.scoped_active_session_claims(
                                    session_claim_binding.as_ref().expect("subscriber claims").0,
                                    session_claim_binding
                                        .as_ref()
                                        .expect("subscriber claims")
                                        .1
                                        .clone(),
                                );
                                peer
                                    .rehydrate_query_for_subscription_from_reconciled_maintained_subscription(
                                        &mut node,
                                        group_subscription,
                                        subscription,
                                        &group.shape,
                                        reconciled,
                                    )
                                .await
                                .map(Some)
                            } else {
                                let mut node = self.node.lock().await;
                                let mut node = node.scoped_active_session_claims(
                                    session_claim_binding.as_ref().expect("subscriber claims").0,
                                    session_claim_binding
                                        .as_ref()
                                            .expect("subscriber claims")
                                            .1
                                            .clone(),
                                );
                                peer.rehydrate_query_for_subscription_with_opts_and_waker(
                                    &mut node,
                                    group_subscription,
                                    &group.shape,
                                    &group.binding,
                                    coverage.opts.clone(),
                                    progress_waker.as_ref(),
                                )
                                .await
                                .map(|update| {
                                    update.map(|update| retarget_view_update(update, subscription))
                                })
                            };
                            let mut update = match update_result {
                                Ok(Some(update)) => update,
                                Ok(None) => {
                                    group.pending_initial_subscribers.insert(subscription);
                                    serve_again = true;
                                    continue;
                                }
                                Err(crate::node::Error::QueryCapability(detail)) => {
                                    rollback_rejected_subscriber_admission(
                                        &self.node,
                                        peer,
                                        served,
                                        coverage_groups,
                                        scope_purposes,
                                        scope_aggregates,
                                        &self.relay_upstream_subscription_owners,
                                        upstream_subscriptions,
                                        connection_epoch,
                                        subscription,
                                    );
                                    queue_direct_control(&mut self.pending_control_responses,
                                        unsupported_shape_capability_rejection_message(
                                            subscription,
                                            detail,
                                        ),
                                    );
                                    schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                    return Ok(true);
                                }
                                Err(error) => {
                                    rollback_rejected_subscriber_admission(
                                        &self.node,
                                        peer,
                                        served,
                                        coverage_groups,
                                        scope_purposes,
                                        scope_aggregates,
                                        &self.relay_upstream_subscription_owners,
                                        upstream_subscriptions,
                                        connection_epoch,
                                        subscription,
                                    );
                                    queue_direct_control(&mut self.pending_control_responses,
                                        server_subscription_failure_rejection_message(
                                            subscription,
                                            &error,
                                        ),
                                    );
                                    schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                    return Ok(true);
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
                            send_subscriber_with_sync_context(
                                &self.node,
                                peer,
                                self.transport.as_mut(),
                                &self.local_fate_routes,
                                &self.downstream_fates,
                                update,
                            )?;
                            if let Some((subscription, receipt)) = receipt {
                                queue_direct_control(&mut self.pending_control_responses,
                                    SyncMessage::AuthorizationScopeReceipt {
                                        subscription,
                                        receipt,
                                    },
                                );
                                schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                return Ok(true);
                            }
                            sent_view_update = true;
                            established_subscribers.insert(subscription);
                        }
                        }
                        if serving_initial {
                            continue;
                        }
                        let update_result = {
                            let mut node = self.node.lock().await;
                            let mut node = node.scoped_active_session_claims(
                                session_claim_binding.as_ref().expect("subscriber claims").0,
                                session_claim_binding.as_ref().expect("subscriber claims").1.clone(),
                            );
                            if settled_handoff && peer.has_maintained_subscription(group_subscription) {
                                // A cold handoff opens the maintained view on
                                // its first turn. Retrying that full rehydrate
                                // would discard the just-opened receiver each
                                // time, so resume the existing view's initial
                                // delta. That delta is not an authority
                                // successor closure: the exact upstream reset
                                // has already been installed/forwarded. In
                                // particular, never relabel an empty local
                                // terminal tick as `reset_result_set`, because
                                // a receiver would correctly reject its absent
                                // ProgramSourceCoverage manifest.
                                peer.query_update_for_subscription_with_opts_and_waker(
                                    &mut node,
                                    group_subscription,
                                    &group.shape,
                                    &group.binding,
                                    coverage.opts.clone(),
                                    progress_waker.as_ref(),
                                )
                                .await
                            } else if settled_handoff {
                                peer.rehydrate_query_for_subscription_with_opts_and_waker(
                                    &mut node,
                                    group_subscription,
                                    &group.shape,
                                    &group.binding,
                                    coverage.opts.clone(),
                                    progress_waker.as_ref(),
                                )
                                .await
                            } else {
                                peer.query_update_for_subscription_with_opts_and_waker(
                                    &mut node,
                                    group_subscription,
                                    &group.shape,
                                    &group.binding,
                                    coverage.opts.clone(),
                                    progress_waker.as_ref(),
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
                                    queue_direct_control(&mut self.pending_control_responses,
                                        server_subscription_failure_rejection_message(
                                            subscription,
                                            &error,
                                        ),
                                    );
                                }
                                schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                return Ok(true);
                            }
                        };
                        if settled_handoff {
                            group.awaiting_upstream_settlement = false;
                            peer.set_subscription_awaiting_selected_authority_source(
                                group_subscription,
                                false,
                            );
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
                                send_subscriber_with_sync_context(
                                    &self.node,
                                    peer,
                                    self.transport.as_mut(),
                                    &self.local_fate_routes,
                                    &self.downstream_fates,
                                    update,
                                )?;
                                if let Some((subscription, receipt)) = receipt {
                                    queue_direct_control(&mut self.pending_control_responses,
                                        SyncMessage::AuthorizationScopeReceipt {
                                            subscription,
                                            receipt,
                                        },
                                    );
                                    schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                                    return Ok(true);
                                }
                                sent_view_update = true;
                            }
                        }
                    }
                    for served_current_rows in served_current_rows.values() {
                        let update = {
                            let mut node = self.node.lock().await;
                            peer.current_rows_update(&mut node, &served_current_rows.table)
                                .await?
                        };
                        if !view_update_is_empty(&update) {
                            send_subscriber_with_sync_context(
                                &self.node,
                                peer,
                                self.transport.as_mut(),
                                &self.local_fate_routes,
                                &self.downstream_fates,
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
                    while let Some(response) = deferred_subscribe_rejections.pop_front() {
                        self.pending_control_responses.push_back(response);
                    }
                    if !flush_pending_control_responses(
                        &self.node,
                        peer,
                        self.transport.as_mut(),
                        &mut self.pending_control_responses,
                        &self.scheduler,
                    )? {
                        return Ok(true);
                    }
                }
                    Ok::<bool, Error>(false)
                })
                .await;
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
            peer_payload_inventory,
            result_member_adds,
            result_member_removes,
            program_fact_adds,
            program_fact_removes,
        }) => ViewUpdateParts {
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
    subscriptions: &SubscriptionList,
    pending: &mut Vec<PendingAuthorityViewUpdate>,
    awaiting: &AwaitingInitialAuthorityCoverage,
    clears: &mut BTreeSet<CoverageKey>,
    query_coverage_registrations: &QueryCoverageRegistrations,
    active_authority_view_receipts: &ActiveAuthorityViewReceipts,
    coverage_refresh_generations: &CoverageRefreshGenerations,
    subscriber_dirty_epoch: &Rc<Cell<u64>>,
    scheduler: &SharedTickScheduler,
    connection_epoch: u64,
) -> Result<(), Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    // A replacement authority selection is connection-scoped. Frames which
    // were already queued on an older transport retain their transport-local
    // `authority_receipt_eligible` bit, so combine it with the currently
    // selected connection here before those frames can touch a shared
    // authority result slot.
    let authority_link_selected = active_authority_view_receipts
        .borrow()
        .as_ref()
        .is_none_or(|receipts| receipts.connection_epoch == connection_epoch);
    let frame_is_selected = |update: &PendingAuthorityViewUpdate| {
        authority_link_selected && update.authority_receipt_eligible
    };
    let confirmed_subscriptions = pending
        .iter()
        .filter(|update| frame_is_selected(update) && !update.parts.opening_pending)
        .map(|update| (update.parts.subscription, update.parts.settled_through))
        .collect::<Vec<_>>();
    let batch_cut = pending
        .iter()
        .map(|update| update.parts.settled_through)
        .max()
        .unwrap_or_default();
    let ineligible_cut = pending
        .iter()
        .filter(|update| !frame_is_selected(update))
        .map(|update| update.parts.settled_through)
        .max();
    let publishing_subscriptions = pending
        .iter()
        .filter(|update| frame_is_selected(update))
        .map(|update| update.parts.subscription)
        .collect::<BTreeSet<_>>();
    let node_ref = node.borrow();
    let relay_authority_session_owner = node_ref.client_relay_scope().is_some();
    let confirmed_binding_views = confirmed_subscriptions
        .iter()
        .filter_map(|(subscription, settled_through)| {
            node_ref
                .binding_view_key_for_subscription(*subscription)
                .ok()
                .map(|binding_view| (*subscription, binding_view, *settled_through))
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
                receipts.subscriptions.clear();
                receipts.confirmation_floor = receipts.confirmation_floor.max(invalidation_cut);
                // Keep the receiver-local terminal state intact, but make the
                // receipt demotion visible before this stale authority frame
                // can publish any ordinary update. A replacement connection
                // must prove a fresh exact closure for every subscription.
                demote_authority_receipt_subscriptions(subscriptions, &publishing_subscriptions);
            }
        }
    }
    // Record concrete authoritative payloads only after their normal batch is
    // accepted. This is intentionally independent of live result membership:
    // a later removal governs future delivery, not retained bytes.
    let ledger_bundles = if relay_authority_session_owner {
        pending
            .iter()
            .filter(|update| frame_is_selected(update))
            .flat_map(|update| {
                expand_version_carriers(&update.parts.version_carriers).unwrap_or_default()
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    // A frame from a nonselected upstream is only evidence that the selected
    // receipt is no longer current. It is not an input to that receipt's
    // receiver-local graph: applying its source closure here would mutate the
    // selected authority slot, then make the selected successor look like a
    // duplicate incremental witness. The selected link must publish its own
    // exact predecessor→successor closure. This also keeps a stale authority
    // from changing the locally derived result during receipt demotion.
    let unselected_carriers = pending
        .iter()
        .filter(|update| !frame_is_selected(update))
        .flat_map(|update| update.parts.version_carriers.iter().cloned())
        .collect::<Vec<_>>();
    let updates = std::mem::take(pending)
        .into_iter()
        .filter(|update| frame_is_selected(update))
        .map(|update| update.parts)
        .collect::<Vec<_>>();
    if !unselected_carriers.is_empty() {
        node.lock()
            .await
            .ingest_unselected_authority_view_bundles(&unselected_carriers)
            .await?;
    }
    if !updates.is_empty() {
        let mut node_ref = node.lock().await;
        node_ref.apply_view_updates_in_batch(updates).await?;
        node_ref
            .record_scope_relay_authoritative_bundles(&ledger_bundles)
            .await?;
    }
    if relay_authority_session_owner {
        // A relay authority view is input to every locally served browser
        // Edge child. Advance the shared generation only after the validated
        // batch commits, so a later tab is rehydrated from the worker's
        // resident authority membership without unrelated upstream traffic.
        let next = subscriber_dirty_epoch.get().wrapping_add(1);
        subscriber_dirty_epoch.set(next);
        schedule_tick_in(scheduler, TickUrgency::Immediate);
    }
    if let Some(receipts) = active_authority_view_receipts.borrow_mut().as_mut()
        && receipts.connection_epoch == connection_epoch
    {
        let registrations = query_coverage_registrations.borrow();
        for (subscription, binding_view, settled_through) in confirmed_binding_views {
            if settled_through < receipts.confirmation_floor {
                continue;
            }
            // Public streams are not query-coverage attachments, but their
            // binding view is still the exact receipt required to settle the
            // receiver-local graph. Coverage registrations retain only their
            // own ownership accounting below.
            receipts.binding_views.insert(binding_view);
            if registrations.contains_key(&subscription) {
                receipts.subscriptions.insert(subscription);
            }
        }
    }
    if !clears.is_empty() {
        let mut awaiting = awaiting.borrow_mut();
        let mut refreshes = coverage_refresh_generations.borrow_mut();
        for coverage in std::mem::take(clears) {
            awaiting.remove(&coverage);
            if relay_authority_session_owner {
                // Coalescing is valid only while this relay authority reply
                // remains outstanding. A later tab must request a receipt of
                // its own rather than inheriting an obsolete refresh marker.
                refreshes.remove(&coverage);
            }
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
    pending_control_responses: &mut VecDeque<PendingSubscriberControlResponse>,
    identity: AuthorSubject,
    session_claims: BTreeMap<String, Value>,
    connection_epoch: u64,
    request_id: PermissionAdviceRequestId,
    action: PermissionAdviceAction,
    trust: CommitUnitTrust,
    hydrations: &mut BTreeMap<
        crate::protocol::AuthorizationSupportScopeKey,
        ServedAuthorizationScopeHydration,
    >,
    hydration_count: &mut u64,
    progress_waker: Option<&std::task::Waker>,
) -> Result<(), Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    if !node.borrow().is_history_complete()
        || !subscriber_permissions_ready(node.borrow().permissions_ready(), trust)
    {
        queue_direct_control(
            pending_control_responses,
            SyncMessage::AuthorizationScopeUnavailable { request_id },
        );
        return Ok(());
    }
    let scope = match node.borrow().authorization_support_scope_for_session(
        identity,
        Some(&session_claims),
        &action,
    ) {
        Ok(scope) => scope,
        Err(_) => {
            queue_direct_control(
                pending_control_responses,
                SyncMessage::AuthorizationScopeUnavailable { request_id },
            );
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
            let mut node = node.scoped_active_session_claims(identity, session_claims.clone());
            evaluate_authoritative_permission_advice(&mut node, identity, action).await
        };
        queue_direct_control(
            pending_control_responses,
            SyncMessage::AuthorizationScopeDecision { request_id, advice },
        );
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
        queue_authorization_scope_sequence(
            pending_control_responses,
            request_id,
            scope.key.clone(),
            hydration.clone(),
        );
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
            queue_direct_control(
                pending_control_responses,
                SyncMessage::AuthorizationScopeUnavailable { request_id },
            );
            return Ok(());
        }
        let supported = {
            let mut node = node.lock().await;
            let mut node = node.scoped_active_session_claims(identity, session_claims.clone());
            node.ensure_peer_maintained_subscription_view_supported(
                shape,
                binding,
                scope.options.tier,
                identity,
                &scope.options.read_view,
                QueryAuthorizationMode::TrustedServing,
            )
            .await
        };
        if supported.is_err() {
            queue_direct_control(
                pending_control_responses,
                SyncMessage::AuthorizationScopeUnavailable { request_id },
            );
            return Ok(());
        }
        let values = binding_values_in_param_order(shape, binding);
        let register = SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(shape),
            opts: scope.options.clone(),
        };
        let subscribe = SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values,
            known_state: None,
            delegated_session: None,
        });
        peer.declare_known_state(subscription, None);
        // Authority scope support has no wire Subscribe admission: this
        // opaque usage site is allocated locally for the request currently
        // authenticated on this link. Bind that exact admission snapshot
        // before the owner-loop rehydrate opens a maintained view. In
        // particular, do not fall back to the authority transport identity
        // (normally SYSTEM for a trusted backend link).
        peer.set_subscription_policy_binding(subscription, (identity, session_claims.clone()));
        let update = {
            let mut node = node.lock().await;
            let mut node = node.scoped_active_session_claims(identity, session_claims.clone());
            peer.rehydrate_query_for_subscription_with_opts_and_waker(
                &mut node,
                subscription,
                shape,
                binding,
                scope.options.clone(),
                progress_waker,
            )
            .await?
        };
        let Some(update) = update else {
            queue_direct_control(
                pending_control_responses,
                SyncMessage::AuthorizationScopeUnavailable { request_id },
            );
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
            queue_direct_control(
                pending_control_responses,
                SyncMessage::AuthorizationScopeUnavailable { request_id },
            );
            return Ok(());
        }
        support_subscriptions.push(subscription);
        served_clauses.push(ServedAuthorizationScopeClause {
            subscription,
            register,
            subscribe,
            view: update,
        });
    }
    let Some((settled_through, authorization_progress)) = aggregate.bounds() else {
        queue_direct_control(
            pending_control_responses,
            SyncMessage::AuthorizationScopeUnavailable { request_id },
        );
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
    let hydration = ServedAuthorizationScopeHydration {
        clauses: served_clauses,
        receipt,
    };
    if hydrations.len() < MAX_AUTHORIZATION_SCOPES {
        hydrations.insert(scope.key.clone(), hydration.clone());
    }
    // Scope views are proof material, not application subscriptions.  Their
    // lifetime ends after the receipt; FIFO keeps the receiver's local
    // evaluation ahead of this cleanup.
    for subscription in support_subscriptions {
        peer.forget_subscription_with_node(&mut node.borrow_mut(), subscription);
    }
    queue_authorization_scope_sequence(pending_control_responses, request_id, scope.key, hydration);
    Ok(())
}

pub(super) fn authorization_progress_for_view_receipt(
    peer_payload_inventory: &crate::protocol::PeerPayloadInventory,
    usage_site_progress: u64,
) -> u64 {
    peer_payload_inventory
        .authorization_progress
        .unwrap_or(usage_site_progress)
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
        peer_payload_inventory,
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
            authorization_progress: authorization_progress_for_view_receipt(
                peer_payload_inventory,
                peer.authorization_progress_for_subscription(*subscription),
            ),
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

/// Undo a served usage-site admission that failed before its opening reset was
/// accepted. A coverage group owns shared canonical state, so preserve it for
/// siblings while removing every per-usage registration. If this was the last
/// usage, retire the group and cancel (or withdraw) its upstream ownership too.
fn rollback_rejected_subscriber_admission<S>(
    node: &SharedNodeState<S>,
    peer: &mut PeerState,
    served: &mut BTreeMap<SubscriptionKey, CoverageKey>,
    coverage_groups: &mut BTreeMap<CoverageKey, CoverageGroup>,
    scope_purposes: &mut BTreeMap<SubscriptionKey, AuthorizedScopePurpose>,
    scope_aggregates: &mut BTreeMap<
        crate::protocol::AuthorizationSupportScopeKey,
        AuthorityScopeAggregate,
    >,
    relay_upstream_subscription_owners: &RelayUpstreamSubscriptionOwners,
    upstream_subscriptions: &PendingUpstreamCommands,
    connection_epoch: u64,
    subscription: SubscriptionKey,
) where
    S: OrderedKvStorage,
{
    let Some(coverage) = served.remove(&subscription) else {
        return;
    };
    if let Some(purpose) = scope_purposes.remove(&subscription) {
        remove_scope_aggregate_member(scope_aggregates, &purpose.key, subscription);
    }

    let Some(group) = coverage_groups.get_mut(&coverage) else {
        // Admission always installs the group before `served`; avoid retaining
        // the usage-site state if an earlier invariant violation broke that
        // ordering.
        let mut node = node.borrow_mut();
        node.apply_unsubscribe(subscription);
        peer.forget_subscription(subscription);
        return;
    };
    group.subscribers.remove(&subscription);
    group.pending_initial_subscribers.remove(&subscription);
    if group.upstream_opts.propagate_upstream {
        if let Some(owner) = relay_upstream_subscription_owners
            .borrow_mut()
            .get_mut(&group.upstream_subscription)
            && owner.downstream_connection_epoch == connection_epoch
            && owner.coverage == coverage
        {
            owner.downstream_subscriptions.remove(&subscription);
        }
    }
    let retire_group = group.subscribers.is_empty();
    let upstream = retire_group.then_some((
        group.upstream_subscription,
        group.upstream_opts.propagate_upstream,
    ));

    let mut node = node.borrow_mut();
    node.apply_unsubscribe(subscription);
    peer.forget_subscription(subscription);
    if !retire_group {
        return;
    }

    peer.forget_subscription_with_node(&mut node, coverage_group_subscription_key(&coverage));
    coverage_groups.remove(&coverage);
    let Some((upstream_subscription, propagated_upstream)) = upstream else {
        return;
    };
    if !propagated_upstream
        || retire_relay_upstream_subscription(
            relay_upstream_subscription_owners,
            upstream_subscription,
            connection_epoch,
            &coverage,
        )
        .is_none()
    {
        return;
    }
    let mut pending = upstream_subscriptions.borrow_mut();
    let open_was_pending = pending.iter().any(|command| {
        matches!(
            command,
            PendingUpstreamCommand::Subscribe(open)
                if open.subscription == upstream_subscription
        )
    });
    pending.retain(|command| {
        !matches!(
            command,
            PendingUpstreamCommand::Subscribe(open)
                if open.subscription == upstream_subscription
        )
    });
    if !open_was_pending {
        pending.push(PendingUpstreamCommand::Unsubscribe(upstream_subscription));
    }
}

fn refresh_authorized_scope_purpose<S>(
    node: &NodeState<S>,
    link_identity: AuthorSubject,
    session_claims: &BTreeMap<String, Value>,
    subscription: SubscriptionKey,
    shape: &ValidatedQuery,
    binding: &Binding,
    prior: &AuthorizedScopePurpose,
) -> Option<AuthorizedScopePurpose>
where
    S: OrderedKvStorage,
{
    let expected = node
        .authorization_support_scope_for_session(link_identity, Some(session_claims), &prior.action)
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
            peer_payload_inventory,
            result_member_adds,
            result_member_removes,
            program_fact_adds,
            program_fact_removes,
        }) => format!(
            "ViewUpdate {} settled={} reset={} bundles={} inventory={} adds={} removes={} fact_adds={} fact_removes={}",
            summarize_subscription_key(*subscription),
            settled_through.0,
            reset_result_set,
            expand_version_carriers(version_carriers)
                .map(|bundles| bundles.len())
                .unwrap_or_default(),
            peer_payload_inventory.complete_tx_payloads.len(),
            result_member_adds.len(),
            result_member_removes.len(),
            program_fact_adds.len(),
            program_fact_removes.len()
        ),
        SyncMessage::CommitUnit { tx, .. } => format!("CommitUnit tx={:?}", tx.tx_id),
        SyncMessage::FateUpdate { tx_id, fate, .. } => {
            format!("FateUpdate tx={tx_id:?} fate={fate:?}")
        }
        SyncMessage::FetchRowVersions { requests, .. } => {
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

pub(super) fn send_subscriber_with_sync_context<S>(
    node: &SharedNodeState<S>,
    peer: &mut PeerState,
    transport: &mut dyn Transport,
    local_fate_routes: &LocalFateRoutes,
    downstream_fates: &PendingDownstreamFates,
    mut message: SyncMessage,
) -> Result<(), Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    if let SyncMessage::ViewUpdate(payload) = &mut message
        && node.borrow().client_relay_scope().is_some()
    {
        let source = peer
            .subscription_authority_result_source(payload.subscription)
            .cloned();
        let source_settled = source
            .as_ref()
            .is_some_and(|source| node.borrow().has_settled_authority_result(source));
        if source.is_some() && !source_settled {
            // This D belongs to a non-authoritative scope relay. Its selected U
            // source exists conceptually at admission but has not delivered a
            // live authority reset yet. Every route to the foreground—including
            // recovery/rehydration—must retain that fact; an ordinary empty reset
            // would otherwise complete a strict Edge read and release U before
            // the authority reply can arrive.
            payload.peer_payload_inventory.opening_pending = true;
        }
    }
    let mut pending_tx_ids = BTreeSet::new();
    if let SyncMessage::ViewUpdate(payload) = &message {
        for carrier in &payload.version_carriers {
            for bundle in carrier
                .bundle_refs()
                .map_err(|_| Error::new(ErrorCode::Protocol, "malformed version-bundle run"))?
            {
                if matches!(bundle.fate, Fate::Pending) {
                    pending_tx_ids.insert(bundle.tx.tx_id);
                }
            }
        }
    }

    send_with_sync_context(node, peer, transport, message)?;
    for tx_id in pending_tx_ids {
        register_local_fate_observer(local_fate_routes, tx_id, downstream_fates);
    }
    Ok(())
}

/// Deliver terminal/local fate updates in FIFO order without letting a bounded
/// byte transport turn an already-produced settlement into a dropped message.
///
/// The wire adapter retains at most the one logical message it has already
/// accepted. If that backlog is full, this queue keeps the *unaccepted* fate
/// at its semantic producer boundary and the scheduler retries after the
/// binding wakes for transport capacity. We remove only after `send` accepts
/// the logical message, so a retry neither duplicates a sent fate nor loses a
/// later fate behind it.
fn flush_downstream_fates<S>(
    node: &SharedNodeState<S>,
    peer: &mut PeerState,
    transport: &mut dyn Transport,
    fates: &PendingDownstreamFates,
    scheduler: &SharedTickScheduler,
) -> Result<bool, Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    loop {
        let Some(fate) = fates.borrow().first().cloned() else {
            return Ok(true);
        };
        match send_with_sync_context(node, peer, transport, fate) {
            Ok(()) => {
                fates.borrow_mut().remove(0);
            }
            Err(error) if error.code == ErrorCode::Backpressure => {
                schedule_tick_in(scheduler, TickUrgency::Deferred);
                return Ok(false);
            }
            Err(error) => return Err(error),
        }
    }
}

/// Retry the one ordinary-wire chunk response whose byte admission was refused.
/// The auxiliary lane has its own bounded take/restore queue; this covers the
/// legacy canonical-wire request path without giving a stalled peer an
/// unbounded second response buffer.
fn flush_pending_chunk_response(
    transport: &mut dyn Transport,
    pending: &mut Option<ChunkResponseBatch>,
    scheduler: &SharedTickScheduler,
) -> Result<bool, Error> {
    let Some(response) = pending.take() else {
        return Ok(true);
    };
    match transport.send(SyncMessage::ChunkResponseBatch(response.clone())) {
        Ok(()) => Ok(true),
        Err(TransportError::Backpressure) => {
            *pending = Some(response);
            schedule_tick_in(scheduler, TickUrgency::Deferred);
            Ok(false)
        }
        Err(error) => {
            *pending = Some(response);
            Err(transport_error(error))
        }
    }
}

/// Retry a subscriber-control reply such as `SubscribeRejected`. These replies
/// are generated while consuming a one-shot inbound registration, so treating
/// a rejected byte admission as a completed reply would otherwise leave the
/// requester waiting forever. One retained message bounds a stalled link; the
/// subscriber tick stops as soon as it creates one.
fn flush_pending_control_responses<S>(
    node: &SharedNodeState<S>,
    peer: &mut PeerState,
    transport: &mut dyn Transport,
    pending: &mut VecDeque<PendingSubscriberControlResponse>,
    scheduler: &SharedTickScheduler,
) -> Result<bool, Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    loop {
        let Some(response) = pending.front() else {
            return Ok(true);
        };
        let send_result = match response {
            PendingSubscriberControlResponse::Direct(response) => {
                transport.send(response.clone()).map_err(transport_error)
            }
            PendingSubscriberControlResponse::WithSyncContext(response) => {
                send_with_sync_context(node, peer, transport, response.clone())
            }
            PendingSubscriberControlResponse::AuthorizationScopeSequence(sequence) => {
                let Some(response) = sequence.next_message() else {
                    pending.pop_front();
                    continue;
                };
                transport.send(response).map_err(transport_error)
            }
        };
        match send_result {
            Ok(()) => {
                let finished = match pending.front_mut() {
                    Some(PendingSubscriberControlResponse::AuthorizationScopeSequence(
                        sequence,
                    )) => {
                        sequence.advance();
                        sequence.next_message().is_none()
                    }
                    Some(PendingSubscriberControlResponse::Direct(_))
                    | Some(PendingSubscriberControlResponse::WithSyncContext(_)) => true,
                    None => unreachable!("accepted control operation remains queued"),
                };
                if finished {
                    pending.pop_front();
                }
            }
            Err(error) if error.code == ErrorCode::Backpressure => {
                schedule_tick_in(scheduler, TickUrgency::Deferred);
                return Ok(false);
            }
            Err(error) => return Err(error),
        }
    }
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
        }
    }
    handled_mutation_error
}

fn handle_write_state_update<S>(
    node: &SharedNodeState<S>,
    waiters: &WriteStateWaiters,
    mutation_errors: &SharedMutationErrors,
    browser_relay_recovered_tx_ids: &Rc<RefCell<BTreeSet<TxId>>>,
    scheduler: &SharedTickScheduler,
    tx_id: TxId,
) where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let handled_by_waiter = notify_write_state_waiters(waiters, tx_id);
    // Extract the owned rejection before deciding how to report it. Keeping a
    // `LocalMutex` guard in an `if let` scrutinee spans the entire body, which
    // used to reenter the suspended node when an acknowledged rejection was
    // discarded below.
    let rejected = node.borrow().rejected_transaction(tx_id);
    if let Some(rejected) = rejected {
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
        return;
    }

    // A restarted browser relay re-uploads durable foreground commits whose
    // TxId node belongs to the former, non-durable foreground runtime. Their
    // row-version payload is deliberately not retained as this worker's
    // rejection state (INV-TX-9), but an attached foreground runtime still
    // needs one live notification when that exact replayed transaction is
    // rejected. This ownership set is process-local and populated only by the
    // browser relay recovery path, so it cannot turn arbitrary foreign
    // history into callbacks or survive an app-less worker interval.
    let Some(record) = browser_relay_recovered_tx_ids
        .borrow()
        .contains(&tx_id)
        .then(|| crate::db::block_on(node.borrow_mut().transaction_record(tx_id)))
        .flatten()
    else {
        return;
    };

    let terminal = matches!(record.fate, Fate::Rejected(_))
        || matches!(record.fate, Fate::Accepted)
            && record.global_time.is_some()
            && record.durability >= DurabilityTier::Global;
    if !terminal || !browser_relay_recovered_tx_ids.borrow_mut().remove(&tx_id) {
        return;
    }
    if handled_by_waiter {
        return;
    }
    let Fate::Rejected(reason) = record.fate else {
        return;
    };

    let should_schedule = {
        let mut state = mutation_errors.borrow_mut();
        state
            .pending
            .entry(tx_id)
            .or_insert_with(|| mutation_error_event_for(tx_id, record.kind, &reason));
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
    mutation_error_event_for(tx_id, rejected.kind(), &rejected.reason())
}

fn mutation_error_event_for(
    tx_id: TxId,
    kind: TxKind,
    rejection: &RejectionReason,
) -> MutationErrorEvent {
    let transaction_id = TransactionId::from_committed_tx(tx_id);
    let (code, reason) = mutation_error_details(rejection);
    MutationErrorEvent {
        code: code.clone(),
        reason: reason.clone(),
        transaction: LocalTransactionRecord {
            transaction_id,
            kind: kind.into(),
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
            peer_payload_inventory,
            result_member_adds,
            result_member_removes,
            program_fact_adds,
            program_fact_removes,
            ..
        }) => {
            !reset_result_set
                && version_carriers.is_empty()
                && peer_payload_inventory.complete_tx_payloads.is_empty()
                && result_member_adds.is_empty()
                && result_member_removes.is_empty()
                && program_fact_adds.is_empty()
                && program_fact_removes.is_empty()
        }
        _ => false,
    }
}
