//! Bounded first-page reads routed to the selected serving authority.
use super::peer_connection::{ConnectionLink, PeerConnection, transport_error};
use super::*;
use crate::protocol::{
    CurrentRowsReceipt, PolicyBindingKey, RemoteReadRequest, RemoteReadResponse,
};

const MAX_PENDING: usize = 64;
pub(super) const MAX_QUERY_BYTES: usize = 32 * 1024;
pub(super) const MAX_RESULT_BYTES: usize = 1024 * 1024;

pub(super) type SharedRemoteReads = Rc<RefCell<RemoteReadRouter>>;

pub(super) struct RemoteReadRoute {
    pub request: RemoteReadRequest,
    pub context: PolicyBindingKey,
    pub upstream: Option<AuthorityContext>,
    pub downstream: Option<(u64, PermissionAdviceRequestId)>,
    pub sender: Option<oneshot::Sender<Option<Vec<u8>>>>,
}

#[derive(Default)]
pub(super) struct RemoteReadRouter {
    pub routes: BTreeMap<PermissionAdviceRequestId, RemoteReadRoute>,
    pub responses: BTreeMap<u64, VecDeque<RemoteReadResponse>>,
    pub cancels: VecDeque<(AuthorityContext, PermissionAdviceRequestId)>,
}

impl RemoteReadRouter {
    pub fn admit(&mut self, route: RemoteReadRoute) -> bool {
        if self.routes.len()
            + self.cancels.len()
            + self.responses.values().map(VecDeque::len).sum::<usize>()
            >= MAX_PENDING
        {
            return false;
        }
        self.routes.insert(route.request.request_id, route);
        true
    }

    /// A local requester receives rows only after their receipt was ingested;
    /// a relay forwards both, rebinding the receipt to the downstream nonce.
    pub fn finish(&mut self, id: PermissionAdviceRequestId, result: Option<RemoteReadResult>) {
        let Some(mut route) = self.routes.remove(&id) else {
            return;
        };
        if let Some((epoch, downstream_id)) = route.downstream {
            let (rows, receipt) = match result {
                Some((rows, mut receipt)) => {
                    receipt.request_id = downstream_id;
                    (Some(rows), Some(receipt))
                }
                None => (None, None),
            };
            self.responses
                .entry(epoch)
                .or_default()
                .push_back(RemoteReadResponse {
                    request_id: downstream_id,
                    rows,
                    receipt,
                });
        } else if let Some(sender) = route.sender.take() {
            let _ = sender.send(result.map(|(rows, _)| rows));
        }
    }

    pub fn cancel_downstream(&mut self, epoch: u64, id: PermissionAdviceRequestId) {
        let ids = self
            .routes
            .iter()
            .filter_map(|(key, route)| (route.downstream == Some((epoch, id))).then_some(*key))
            .collect::<Vec<_>>();
        for key in ids {
            if let Some(route) = self.routes.remove(&key)
                && let Some(upstream) = route.upstream
            {
                self.cancels.push_back((upstream, key));
            }
        }
    }

    pub fn disconnect(&mut self, epoch: u64) {
        let ids = self
            .routes
            .iter()
            .filter_map(|(id, route)| {
                (route
                    .upstream
                    .is_some_and(|upstream| upstream.connection_id == epoch)
                    || route
                        .downstream
                        .is_some_and(|(downstream, _)| downstream == epoch))
                .then_some(*id)
            })
            .collect::<Vec<_>>();
        for id in ids {
            if self.routes.get(&id).is_some_and(|route| {
                route
                    .downstream
                    .is_some_and(|(downstream, _)| downstream == epoch)
            }) {
                if let Some(route) = self.routes.remove(&id)
                    && let Some(upstream) = route.upstream
                {
                    self.cancels.push_back((upstream, id));
                }
            } else {
                self.finish(id, None);
            }
        }
        self.responses.remove(&epoch);
        self.cancels
            .retain(|(upstream, _)| upstream.connection_id != epoch);
    }
}

pub(super) fn valid_request(request: &RemoteReadRequest) -> bool {
    !request.query.is_empty() && request.query.len() <= MAX_QUERY_BYTES
}

/// Encoded rows plus the Core receipt that installs exactly those rows.
pub(super) type RemoteReadResult = (Vec<u8>, CurrentRowsReceipt);

/// A serving Core owns both query evaluation and final row hydration. A relay
/// never evaluates under its own incomplete policy inputs. The receipt is
/// captured under the same owner lock and scoped claims as the query, so its
/// carriers are the current versions of exactly the returned rows.
pub(super) async fn evaluate_remote_read<S: OrderedKvStorage>(
    node: &SharedNodeState<S>,
    request: &RemoteReadRequest,
    identity: AuthorSubject,
    claims: BTreeMap<String, Value>,
    core_epoch: u64,
    progress: u64,
) -> Option<RemoteReadResult> {
    let query: Query = crate::wire::decode_postcard_exact(&request.query).ok()?;
    if !matches!(query.limit, Some(1..=1000))
        || query.relation.is_some()
        || !query.array_subqueries.is_empty()
    {
        return None;
    }
    let mut owner = node.lock().await;
    if owner.current_write_schema().ok()?.schema != request.schema {
        return None;
    }
    let schema = owner
        .catalogue_schemas()
        .get(&request.schema)?
        .schema
        .clone();
    let shape = query
        .validate_with_schema_version(&schema, request.schema)
        .ok()?;
    let binding = shape.bind(BTreeMap::new()).ok()?;
    let context = PolicyBindingKey::from_canonical_parts(identity, claims.clone());
    let mut scoped = owner.scoped_active_session_claims(identity, claims);
    let mut rows = scoped
        .query_rows_with_prepared_plan_for_identity(
            &shape,
            &binding,
            DurabilityTier::Global,
            None,
            identity,
        )
        .await
        .ok()?;
    scoped.hydrate_current_rows(&mut rows).await.ok()?;
    let receipt = scoped
        .readable_current_rows_receipt(
            request.request_id,
            rows.iter().map(|row| (row.table(), row.row_uuid())),
            context,
            core_epoch,
            progress,
        )
        .await
        .ok()??;
    let bytes = crate::binding_codec::encode_rows(&rows).ok()?;
    within_result_budget(&bytes, &receipt).then_some((bytes, receipt))
}

/// The byte budget covers the page and its ingestible carriers together.
fn within_result_budget(rows: &[u8], receipt: &CurrentRowsReceipt) -> bool {
    rows.len() <= MAX_RESULT_BYTES
        && postcard::experimental::serialized_size(receipt)
            .is_ok_and(|size| rows.len() + size <= MAX_RESULT_BYTES)
}

impl<S: OrderedKvStorage + ReopenableStorage + 'static> Node<S> {
    pub(crate) fn request_remote_read(
        &self,
        query: Vec<u8>,
        schema: SchemaVersionId,
        identity: AuthorSubject,
    ) -> impl Future<Output = Option<Vec<u8>>> + use<S> {
        let id = PermissionAdviceRequestId(*uuid::Uuid::new_v4().as_bytes());
        let (sender, receiver) = oneshot::channel();
        let request = RemoteReadRequest {
            request_id: id,
            query,
            schema,
            delegated_session: None,
        };
        if valid_request(&request) && self.admitted_upstream_authority.borrow().is_some() {
            let claims = self.node.borrow().session_claims_for(identity);
            self.remote_reads.borrow_mut().admit(RemoteReadRoute {
                request,
                context: PolicyBindingKey::from_canonical_parts(identity, claims),
                upstream: None,
                downstream: None,
                sender: Some(sender),
            });
        }
        schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
        let guard = RemoteReadGuard {
            id,
            router: Rc::clone(&self.remote_reads),
            scheduler: Rc::clone(&self.scheduler),
        };
        async move {
            let result = receiver.await.unwrap_or(None);
            drop(guard);
            result
        }
    }
}

struct RemoteReadGuard {
    id: PermissionAdviceRequestId,
    router: SharedRemoteReads,
    scheduler: SharedTickScheduler,
}

impl Drop for RemoteReadGuard {
    fn drop(&mut self) {
        let mut router = self.router.borrow_mut();
        if let Some(route) = router.routes.remove(&self.id)
            && let Some(upstream) = route.upstream
        {
            router.cancels.push_back((upstream, self.id));
        }
        schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
    }
}

impl<S: OrderedKvStorage + ReopenableStorage + 'static> PeerConnection<S> {
    fn send_remote_read(&mut self, message: SyncMessage) -> Result<bool, Error> {
        match self.transport.send(message) {
            Ok(()) => Ok(true),
            Err(error)
                if super::peer_connection::handle_transport_backpressure(
                    &self.node,
                    &self.scheduler,
                    &error,
                ) =>
            {
                Ok(false)
            }
            Err(error) => Err(transport_error(error)),
        }
    }

    pub(super) fn pump_remote_reads(&mut self) -> Result<(), Error> {
        if matches!(self.link, ConnectionLink::Subscriber(_)) {
            loop {
                let response = self
                    .remote_reads
                    .borrow()
                    .responses
                    .get(&self.connection_epoch)
                    .and_then(|queue| queue.front())
                    .cloned();
                let Some(response) = response else {
                    break;
                };
                // Forwarded carriers keep the repair-payload sync context.
                super::peer_connection::queue_sync_context_control(
                    &mut self.pending_control_responses,
                    SyncMessage::RemoteReadResponse(response),
                );
                self.remote_reads
                    .borrow_mut()
                    .responses
                    .get_mut(&self.connection_epoch)
                    .unwrap()
                    .pop_front();
            }
            return Ok(());
        }
        let ConnectionLink::Upstream(state) = &self.link else {
            return Ok(());
        };
        let Some(expected) = state.expected_scope_authority else {
            return Ok(());
        };
        if !self
            .transport
            .connection_session_context()
            .is_some_and(|context| {
                context.negotiated_features & crate::wire::FEATURE_REMOTE_READ_RESULTS != 0
            })
        {
            let ids = self
                .remote_reads
                .borrow()
                .routes
                .keys()
                .copied()
                .collect::<Vec<_>>();
            for id in ids {
                self.remote_reads.borrow_mut().finish(id, None);
            }
            return Ok(());
        }
        if !self
            .admitted_upstream_authority
            .borrow()
            .is_some_and(|selected| selected.same_admitted_link(expected))
        {
            return Ok(());
        }
        let stale = self
            .remote_reads
            .borrow()
            .routes
            .iter()
            .filter_map(|(id, route)| {
                route
                    .upstream
                    .is_some_and(|sent| !sent.same_admitted_link(expected))
                    .then_some(*id)
            })
            .collect::<Vec<_>>();
        for id in stale {
            self.remote_reads.borrow_mut().finish(id, None);
        }
        let ids = self
            .remote_reads
            .borrow()
            .routes
            .iter()
            .filter_map(|(id, route)| route.upstream.is_none().then_some(*id))
            .collect::<Vec<_>>();
        for id in ids {
            let request = {
                let router = self.remote_reads.borrow();
                let route = router.routes.get(&id).unwrap();
                let mut request = route.request.clone();
                let scoped_session = self
                    .node
                    .borrow()
                    .client_relay_scope()
                    .is_some_and(|scope| scope.admits_session(route.context.identity));
                if self.transport.permits_delegated_sessions() || scoped_session {
                    request.delegated_session = Some(crate::protocol::DelegatedSessionBinding {
                        identity: route.context.identity,
                        claims: route.context.claims().clone(),
                    });
                } else if route.downstream.is_some() || route.context.identity != expected.link {
                    drop(router);
                    self.remote_reads.borrow_mut().finish(id, None);
                    continue;
                }
                request
            };
            if !self.send_remote_read(SyncMessage::RemoteReadRequest(request))? {
                return Ok(());
            }
            if let Some(route) = self.remote_reads.borrow_mut().routes.get_mut(&id) {
                route.upstream = Some(expected);
            }
        }
        loop {
            let cancellation = self
                .remote_reads
                .borrow()
                .cancels
                .iter()
                .position(|(upstream, _)| upstream.same_admitted_link(expected));
            let Some(index) = cancellation else {
                break;
            };
            let (_, id) = self.remote_reads.borrow().cancels[index];
            if !self.send_remote_read(SyncMessage::RemoteReadCancel { request_id: id })? {
                return Ok(());
            }
            self.remote_reads.borrow_mut().cancels.remove(index);
        }
        Ok(())
    }
}

/// Rows reach a local requester (or a relay's downstream) only after the Core
/// receipt for exactly those rows passed the same freshness checks as a
/// current-row receipt and was installed through ordinary ingestion. A missing
/// or invalid receipt yields no result, so the caller takes the coverage path
/// instead of returning rows its local store does not hold.
pub(super) async fn receive_remote_read<S: OrderedKvStorage + ReopenableStorage + 'static>(
    node: &SharedNodeState<S>,
    router: &SharedRemoteReads,
    selected: Option<AuthorityContext>,
    expected: Option<AuthorityContext>,
    eligible: bool,
    response: RemoteReadResponse,
) {
    let Some(expected) = expected else {
        return;
    };
    let id = response.request_id;
    let identity = router.borrow().routes.get(&id).and_then(|route| {
        (eligible
            && selected.is_some_and(|selected| selected.same_admitted_link(expected))
            && route
                .upstream
                .is_some_and(|sent| sent.same_admitted_link(expected)))
        .then_some(route.context.identity)
    });
    let Some(identity) = identity else {
        return;
    };
    let result = match (response.rows, response.receipt) {
        (Some(rows), Some(receipt))
            if receipt.request_id == id
                // The Core evaluated under the link's admitted binding; a
                // client may not hold that exact claims snapshot locally, so
                // only the subject is bound here. Claim changes during the
                // read are rejected by the caller's claims-revision check.
                && receipt.context.identity == identity
                && within_result_budget(&rows, &receipt) =>
        {
            ingest_remote_read_receipt(node, &receipt)
                .await
                .then_some((rows, receipt))
        }
        _ => None,
    };
    router.borrow_mut().finish(id, result);
}

async fn ingest_remote_read_receipt<S: OrderedKvStorage + ReopenableStorage + 'static>(
    node: &SharedNodeState<S>,
    receipt: &CurrentRowsReceipt,
) -> bool {
    if receipt.core.0.is_nil()
        || receipt.core_epoch == 0
        || receipt.authorization_progress == 0
        || receipt.outcomes.len() != receipt.rows.len()
        || receipt
            .outcomes
            .iter()
            .any(|outcome| *outcome != crate::protocol::CurrentRowOutcome::Readable)
    {
        return false;
    }
    let mut node = node.lock().await;
    if receipt.rows.iter().any(|row| {
        node.current_row_coordinate(&row.table, row.row)
            .ok()
            .as_ref()
            != Some(row)
    }) || node.active_catalogue_seq() > receipt.policy_epoch
        || node.committed_global_time() > receipt.settled_through
    {
        return false;
    }
    node.ingest_current_rows_receipt(receipt).await.is_ok()
}
