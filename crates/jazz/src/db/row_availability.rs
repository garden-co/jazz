//! Bounded, ephemeral known-row routing. A serving Edge is trusted by its client;
//! it proxies only replies correlated to its selected authenticated upstream.
use super::peer_connection::{ConnectionLink, PeerConnection, transport_error};
use super::*;
use crate::protocol::{
    CurrentRowOutcome, CurrentRowsReceipt, CurrentRowsRequest, PolicyBindingKey,
};

const MAX_PENDING: usize = 64;
pub(super) type SharedCurrentRows = Rc<RefCell<CurrentRowsRouter>>;

#[allow(dead_code)] // Internal hook remains dormant until source-filter integration.
pub(crate) enum CurrentRowsResult {
    /// Carriers have passed normal ingestion before this outcome is exposed.
    Applied(CurrentRowsReceipt),
    Unknown,
}

pub(super) struct CurrentRowsRoute {
    pub request: CurrentRowsRequest,
    pub context: PolicyBindingKey,
    pub upstream: Option<AuthorityContext>,
    pub downstream: Option<(u64, PermissionAdviceRequestId)>,
    pub sender: Option<oneshot::Sender<CurrentRowsResult>>,
}

#[derive(Default)]
pub(super) struct CurrentRowsRouter {
    pub routes: BTreeMap<PermissionAdviceRequestId, CurrentRowsRoute>,
    pub responses: BTreeMap<u64, VecDeque<CurrentRowsReceipt>>,
    pub cancels: VecDeque<(AuthorityContext, PermissionAdviceRequestId)>,
    pub progress: BTreeMap<u64, u64>,
    /// Floors are per exact context and Core epoch, never global policy state.
    /// Retain floors while an older admitted request could still return.
    pub floors: BTreeMap<(NodeUuid, u64, PolicyBindingKey), CurrentRowsFloor>,
}

pub(super) struct CurrentRowsFloor {
    cut: GlobalTime,
    progress: u64,
    policy: u64,
    /// Requests already in flight when this evidence was accepted. A future
    /// nonce cannot revive these replies; durable row watermarks live elsewhere.
    protected_requests: BTreeSet<PermissionAdviceRequestId>,
}

impl CurrentRowsRouter {
    pub fn admit(&mut self, route: CurrentRowsRoute) -> bool {
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

    pub fn finish(&mut self, id: PermissionAdviceRequestId, receipt: Option<CurrentRowsReceipt>) {
        let Some(mut route) = self.routes.remove(&id) else {
            return;
        };
        if let Some((epoch, downstream_id)) = route.downstream {
            let mut receipt =
                receipt.unwrap_or_else(|| unknown_receipt(&route.request, route.context));
            receipt.request_id = downstream_id;
            self.responses.entry(epoch).or_default().push_back(receipt);
        } else if let Some(sender) = route.sender.take() {
            let _ =
                sender.send(receipt.map_or(CurrentRowsResult::Unknown, CurrentRowsResult::Applied));
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
        self.progress.remove(&epoch);
        self.cancels
            .retain(|(upstream, _)| upstream.connection_id != epoch);
        // Bounded ephemeral evidence; a disconnect never grants replay authority.
        // Retain monotonic floors: unrelated downstream disconnects must not
        // make an older outstanding reply current again.
    }
}

pub(super) fn valid_request(request: &CurrentRowsRequest) -> bool {
    !request.rows.is_empty()
        && request.rows.len() <= crate::protocol::MAX_CURRENT_ROWS
        && request
            .rows
            .iter()
            .all(|row| !row.table.is_empty() && row.table.len() <= 256)
        && request
            .rows
            .iter()
            .enumerate()
            .all(|(index, row)| !request.rows[..index].contains(row))
}

pub(super) fn unknown_receipt(
    request: &CurrentRowsRequest,
    context: PolicyBindingKey,
) -> CurrentRowsReceipt {
    CurrentRowsReceipt {
        request_id: request.request_id,
        rows: request.rows.clone(),
        outcomes: vec![CurrentRowOutcome::Unknown; request.rows.len()],
        context,
        core: NodeUuid(uuid::Uuid::nil()),
        core_epoch: 0,
        claims_revision: 0,
        policy_epoch: 0,
        settled_through: GlobalTime(0),
        authorization_progress: 0,
        version_carriers: Vec::new(),
    }
}

impl<S: OrderedKvStorage + ReopenableStorage + 'static> Node<S> {
    /// Internal pilot hook. Caller must provide the immutable owner policy key;
    /// no unavailable-source filter is changed by this exchange.
    #[allow(dead_code)] // Internal pilot entry point, exercised by transport tests.
    pub(crate) fn request_current_rows(
        &self,
        rows: Vec<crate::protocol::CurrentRowCoordinate>,
        context: PolicyBindingKey,
    ) -> impl Future<Output = CurrentRowsResult> + use<S> {
        request_current_rows_for_owner(
            Rc::clone(&self.current_rows),
            Rc::clone(&self.scheduler),
            self.admitted_upstream_authority.borrow().is_some(),
            rows,
            context,
        )
    }
}

/// Owned requester shared by local operations and partial-Edge repair owners.
pub(super) fn request_current_rows_for_owner(
    router: SharedCurrentRows,
    scheduler: SharedTickScheduler,
    has_upstream: bool,
    rows: Vec<crate::protocol::CurrentRowCoordinate>,
    context: PolicyBindingKey,
) -> impl Future<Output = CurrentRowsResult> + use<> {
    let id = PermissionAdviceRequestId(*uuid::Uuid::new_v4().as_bytes());
    let (sender, receiver) = oneshot::channel();
    let request = CurrentRowsRequest {
        request_id: id,
        rows,
        delegated_session: None,
    };
    if valid_request(&request) && has_upstream {
        router.borrow_mut().admit(CurrentRowsRoute {
            request,
            context,
            upstream: None,
            downstream: None,
            sender: Some(sender),
        });
    }
    schedule_tick_in(&scheduler, TickUrgency::Immediate);
    let guard = CurrentRowsGuard {
        id,
        router: Rc::clone(&router),
        scheduler: Rc::clone(&scheduler),
    };
    async move {
        let result = receiver.await.unwrap_or(CurrentRowsResult::Unknown);
        drop(guard);
        result
    }
}

#[allow(dead_code)]
struct CurrentRowsGuard {
    id: PermissionAdviceRequestId,
    router: SharedCurrentRows,
    scheduler: SharedTickScheduler,
}
impl Drop for CurrentRowsGuard {
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
    fn send_current_rows(&mut self, message: SyncMessage) -> Result<bool, Error> {
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
    pub(super) fn pump_current_rows(&mut self) -> Result<(), Error> {
        if matches!(self.link, ConnectionLink::Subscriber(_)) {
            loop {
                let response = self
                    .current_rows
                    .borrow()
                    .responses
                    .get(&self.connection_epoch)
                    .and_then(|queue| queue.front())
                    .cloned();
                let Some(response) = response else {
                    break;
                };
                super::peer_connection::queue_sync_context_control(
                    &mut self.pending_control_responses,
                    SyncMessage::CurrentRowsReceipt(response),
                );
                self.current_rows
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
            .admitted_upstream_authority
            .borrow()
            .is_some_and(|selected| selected.same_admitted_link(expected))
        {
            return Ok(());
        }
        let stale = self
            .current_rows
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
            self.current_rows.borrow_mut().finish(id, None);
        }
        let ids = self
            .current_rows
            .borrow()
            .routes
            .iter()
            .filter_map(|(id, route)| route.upstream.is_none().then_some(*id))
            .collect::<Vec<_>>();
        for id in ids {
            let request = {
                let router = self.current_rows.borrow();
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
                    self.current_rows.borrow_mut().finish(id, None);
                    continue;
                }
                request
            };
            if !self.send_current_rows(SyncMessage::CurrentRowsRequest(request))? {
                return Ok(());
            }
            if let Some(route) = self.current_rows.borrow_mut().routes.get_mut(&id) {
                route.upstream = Some(expected);
            }
        }
        loop {
            let cancellation = self
                .current_rows
                .borrow()
                .cancels
                .iter()
                .position(|(upstream, _)| upstream.same_admitted_link(expected));
            let Some(index) = cancellation else {
                break;
            };
            let (_, id) = self.current_rows.borrow().cancels[index];
            if !self.send_current_rows(SyncMessage::CurrentRowsCancel { request_id: id })? {
                return Ok(());
            }
            self.current_rows.borrow_mut().cancels.remove(index);
        }
        Ok(())
    }
}

pub(super) async fn receive_current_rows<S: OrderedKvStorage + ReopenableStorage + 'static>(
    node: &SharedNodeState<S>,
    router: &SharedCurrentRows,
    selected: Option<AuthorityContext>,
    expected: Option<AuthorityContext>,
    eligible: bool,
    receipt: CurrentRowsReceipt,
) -> Result<(), Error> {
    let Some(expected) = expected else {
        return Ok(());
    };
    let id = receipt.request_id;
    let valid = {
        let router = router.borrow();
        router.routes.get(&id).is_some_and(|route| {
            eligible
                && selected.is_some_and(|selected| selected.same_admitted_link(expected))
                && route
                    .upstream
                    .is_some_and(|sent| sent.same_admitted_link(expected))
                && receipt.rows == route.request.rows
                && receipt.context == route.context
                && receipt.outcomes.len() == receipt.rows.len()
        })
    };
    if !valid {
        return Ok(());
    }
    if receipt
        .outcomes
        .iter()
        .all(|outcome| *outcome == CurrentRowOutcome::Unknown)
    {
        if receipt.version_carriers.is_empty() {
            router.borrow_mut().finish(id, None);
        }
        return Ok(());
    }
    if receipt.core.0.is_nil() || receipt.core_epoch == 0 || receipt.authorization_progress == 0 {
        return Ok(());
    }
    let key = (receipt.core, receipt.core_epoch, receipt.context.clone());
    if router.borrow().floors.get(&key).is_some_and(|floor| {
        receipt.settled_through < floor.cut
            || receipt.authorization_progress < floor.progress
            || receipt.policy_epoch < floor.policy
    }) {
        router.borrow_mut().finish(id, None);
        return Ok(());
    }
    {
        let mut router = router.borrow_mut();
        if router.floors.len() >= MAX_PENDING && !router.floors.contains_key(&key) {
            let live = router.routes.keys().copied().collect::<BTreeSet<_>>();
            router.floors.retain(|_, floor| {
                floor
                    .protected_requests
                    .iter()
                    .any(|request| live.contains(request))
            });
            if router.floors.len() >= MAX_PENDING {
                router.finish(id, None);
                return Ok(());
            }
        }
    }
    {
        let mut node = node.lock().await;
        if receipt.rows.iter().any(|row| {
            node.current_row_coordinate(&row.table, row.row)
                .ok()
                .as_ref()
                != Some(row)
        }) {
            router.borrow_mut().finish(id, None);
            return Ok(());
        }
        if node.active_catalogue_seq() > receipt.policy_epoch
            || node.committed_global_time() > receipt.settled_through
        {
            router.borrow_mut().finish(id, None);
            return Ok(());
        }
        node.ingest_current_rows_receipt(&receipt).await?;
    }
    let mut router = router.borrow_mut();
    // At most one floor per outstanding route budget. Existing epoch/context
    // entries remain useful, but arbitrary completed requests cannot grow memory.
    let protected_requests = router
        .routes
        .iter()
        .filter_map(|(request_id, route)| {
            (*request_id != id
                && route.context == receipt.context
                && route
                    .upstream
                    .is_some_and(|upstream| upstream.same_admitted_link(expected)))
            .then_some(*request_id)
        })
        .collect();
    router.floors.insert(
        key,
        CurrentRowsFloor {
            cut: receipt.settled_through,
            progress: receipt.authorization_progress,
            policy: receipt.policy_epoch,
            protected_requests,
        },
    );
    router.finish(id, Some(receipt));
    Ok(())
}
