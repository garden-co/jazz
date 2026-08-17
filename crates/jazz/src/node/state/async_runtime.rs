/// Pollable, query-driven node construction over one resident working set.
///
/// Each attempt runs as an isolated resident-storage transaction. A missing
/// durable input discards that attempt, admits exactly the requested input, and
/// retries. The first successful transaction becomes the node's real resident
/// view, and metadata it created is durably committed before the node becomes
/// ready.
#[doc(hidden)]
pub struct PollableNodeOpen {
    node_uuid: NodeUuid,
    schema: JazzSchema,
    history_complete: bool,
    cache: groove::storage::DemandLoadedStorage,
    persistence: Option<Box<dyn groove::storage::pollable::PollableOrderedKvStorage>>,
    acquisition: groove::db::StorageAcquisition,
    phase: NodeOpenPhase,
}

enum NodeOpenPhase {
    Acquiring,
    Finalizing {
        request: groove::storage::pollable::OwnedStorageRequest,
        node: NodeState<groove::storage::DemandLoadedStorage>,
        cache: groove::storage::DemandLoadedStorage,
    },
    Complete,
}

/// Ready Jazz node together with the durable session that supplies cold inputs
/// and persists resident mutations.
///
/// This is the durable owner produced by [`PollableNodeOpen`]. Keeping these
/// resources together prevents a ready resident node from outliving the async
/// storage session whose ordering and cancellation state it depends on.
#[doc(hidden)]
pub struct DemandDrivenNode {
    node: NodeState<groove::storage::DemandLoadedStorage>,
    cache: groove::storage::DemandLoadedStorage,
    persistence: Box<dyn groove::storage::pollable::PollableOrderedKvStorage>,
    acquisition: groove::db::StorageAcquisition,
    pending_persistence:
        std::collections::VecDeque<groove::storage::pollable::OwnedStorageRequest>,
    persistence_failed: bool,
}

impl DemandDrivenNode {
    /// Access the synchronously resident Jazz core.
    pub fn resident(&self) -> &NodeState<groove::storage::DemandLoadedStorage> {
        &self.node
    }

    /// Run one resident local operation and retain every resulting durable
    /// batch in FIFO order. Local IVM effects are observable before this method
    /// returns; durable publication is driven separately by
    /// [`Self::poll_persistence`].
    pub fn commit_local<T>(
        &mut self,
        operation: impl FnOnce(
            &mut NodeState<groove::storage::DemandLoadedStorage>,
        ) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.ensure_persistence_usable()?;
        let result = operation(&mut self.node);
        if result.is_ok() {
            self.collect_persistence_unit();
        } else {
            self.discard_failed_operation_writes();
        }
        result
    }

    /// Poll a restartable query or subscription operation. It may suspend only
    /// while acquiring a missing durable input; once ready, evaluation runs on
    /// the same resident node used by local writes.
    pub fn poll_query<T>(
        &mut self,
        context: &mut std::task::Context<'_>,
        mut operation: impl FnMut(
            &mut NodeState<groove::storage::DemandLoadedStorage>,
        ) -> Result<T, Error>,
    ) -> std::task::Poll<Result<T, Error>> {
        if let Err(error) = self.ensure_persistence_usable() {
            return std::task::Poll::Ready(Err(error));
        }
        let result = self.acquisition.poll(
            self.persistence.as_mut(),
            &self.cache,
            context,
            || operation(&mut self.node),
            missing_node_open_input,
        );
        match &result {
            std::task::Poll::Ready(Ok(_)) => self.collect_persistence_unit(),
            std::task::Poll::Ready(Err(_)) => self.discard_failed_operation_writes(),
            std::task::Poll::Pending => {}
        }
        result
    }

    /// Poll the oldest durable batch. Later batches cannot overtake it.
    pub fn poll_persistence(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        if let Err(error) = self.ensure_persistence_usable() {
            return std::task::Poll::Ready(Err(error));
        }
        self.collect_persistence_unit();
        loop {
            let Some(request) = self.pending_persistence.front() else {
                return std::task::Poll::Ready(Ok(()));
            };
            match self.persistence.poll_request(request, context) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(Ok(
                    groove::storage::pollable::OwnedStorageResponse::Committed,
                )) => {
                    self.pending_persistence.pop_front();
                }
                std::task::Poll::Ready(Ok(response)) => {
                    let error = Error::Storage(groove::storage::Error::Backend {
                        backend: "demand-driven-node",
                        message: format!("node commit returned unexpected response {response:?}"),
                    });
                    self.fail_persistence();
                    return std::task::Poll::Ready(Err(error));
                }
                std::task::Poll::Ready(Err(error)) => {
                    self.fail_persistence();
                    return std::task::Poll::Ready(Err(error.into()));
                }
            }
        }
    }

    fn collect_persistence_unit(&mut self) {
        let operations = self.cache.take_pending_writes();
        if !operations.is_empty() {
            self.pending_persistence.push_back(
                groove::storage::pollable::OwnedStorageRequest::new(
                    groove::storage::pollable::OwnedStorageOperation::Commit(operations),
                ),
            );
        }
    }

    fn discard_failed_operation_writes(&mut self) {
        if !self.cache.take_pending_writes().is_empty() {
            // An operation that emitted a durable batch before failing has an
            // ambiguous resident outcome. Publish none of it and prevent this
            // runtime from accepting dependent work until a clean reopen.
            self.fail_persistence();
        }
    }

    fn ensure_persistence_usable(&self) -> Result<(), Error> {
        if self.persistence_failed {
            Err(Error::Groove(groove::db::Error::DatabasePoisoned))
        } else {
            Ok(())
        }
    }

    fn fail_persistence(&mut self) {
        self.persistence_failed = true;
        for request in self.pending_persistence.drain(..) {
            let _ = self.persistence.cancel_request(request.id());
        }
    }
}

impl Drop for DemandDrivenNode {
    fn drop(&mut self) {
        let _ = self.acquisition.cancel(self.persistence.as_mut());
        for request in self.pending_persistence.drain(..) {
            let _ = self.persistence.cancel_request(request.id());
        }
    }
}

impl PollableNodeOpen {
    fn finish(
        &mut self,
        mut node: NodeState<groove::storage::DemandLoadedStorage>,
        cache: groove::storage::DemandLoadedStorage,
    ) -> DemandDrivenNode {
        // Durable completion belongs to this wrapper, not the synchronous
        // NodeState call stack. Authored rows therefore enter the resident
        // view at None and gain stronger durability only through the normal
        // acknowledged sync/fate path.
        node.set_non_durable_client();
        DemandDrivenNode {
            node,
            cache,
            persistence: self
                .persistence
                .take()
                .expect("ready node takes its persistence session"),
            acquisition: groove::db::StorageAcquisition::default(),
            pending_persistence: std::collections::VecDeque::new(),
            persistence_failed: false,
        }
    }

    #[doc(hidden)]
    pub fn new(
        node_uuid: NodeUuid,
        schema: JazzSchema,
        persistence: Box<dyn groove::storage::pollable::PollableOrderedKvStorage>,
    ) -> Self {
        Self::with_history_complete(node_uuid, schema, persistence, false)
    }

    #[doc(hidden)]
    pub fn new_history_complete(
        node_uuid: NodeUuid,
        schema: JazzSchema,
        persistence: Box<dyn groove::storage::pollable::PollableOrderedKvStorage>,
    ) -> Self {
        Self::with_history_complete(node_uuid, schema, persistence, true)
    }

    fn with_history_complete(
        node_uuid: NodeUuid,
        schema: JazzSchema,
        persistence: Box<dyn groove::storage::pollable::PollableOrderedKvStorage>,
        history_complete: bool,
    ) -> Self {
        let column_families = schema.column_families();
        let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
        Self {
            node_uuid,
            schema,
            history_complete,
            cache: groove::storage::DemandLoadedStorage::new(&refs),
            persistence: Some(persistence),
            acquisition: groove::db::StorageAcquisition::default(),
            phase: NodeOpenPhase::Acquiring,
        }
    }

    #[doc(hidden)]
    pub fn poll(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<DemandDrivenNode, Error>> {
        loop {
            if let NodeOpenPhase::Finalizing { request, .. } = &self.phase {
                match self
                    .persistence
                    .as_mut()
                    .expect("active node opening owns persistence")
                    .poll_request(request, context)
                {
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                    std::task::Poll::Ready(Ok(
                        groove::storage::pollable::OwnedStorageResponse::Committed,
                    )) => {
                        let NodeOpenPhase::Finalizing { node, cache, .. } = std::mem::replace(
                            &mut self.phase,
                            NodeOpenPhase::Complete,
                        ) else {
                            unreachable!("finalizing phase was just matched")
                        };
                        return std::task::Poll::Ready(Ok(self.finish(node, cache)));
                    }
                    std::task::Poll::Ready(Ok(response)) => {
                        self.phase = NodeOpenPhase::Complete;
                        return std::task::Poll::Ready(Err(Error::Storage(
                            groove::storage::Error::Backend {
                                backend: "pollable-node-open",
                                message: format!(
                                    "node-open commit returned unexpected response {response:?}"
                                ),
                            },
                        )));
                    }
                    std::task::Poll::Ready(Err(error)) => {
                        self.phase = NodeOpenPhase::Complete;
                        return std::task::Poll::Ready(Err(error.into()));
                    }
                }
            }
            assert!(
                matches!(self.phase, NodeOpenPhase::Acquiring),
                "completed node opening cannot be polled again"
            );
            let node_uuid = self.node_uuid;
            let schema = self.schema.clone();
            let history_complete = self.history_complete;
            match self.acquisition.poll(
                self.persistence
                    .as_mut()
                    .expect("active node opening owns persistence")
                    .as_mut(),
                &self.cache,
                context,
                || {
                    let transaction = self.cache.begin_transaction()?;
                    let node = construct_pollable_node(
                        node_uuid,
                        schema.clone(),
                        history_complete,
                        transaction.clone(),
                    )?;
                    Ok((node, transaction))
                },
                missing_node_open_input,
            ) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(Ok((node, transaction))) => {
                    let writes = transaction.take_pending_writes();
                    if writes.is_empty() {
                        self.phase = NodeOpenPhase::Complete;
                        return std::task::Poll::Ready(Ok(self.finish(node, transaction)));
                    }
                    self.phase = NodeOpenPhase::Finalizing {
                        request: groove::storage::pollable::OwnedStorageRequest::new(
                            groove::storage::pollable::OwnedStorageOperation::Commit(writes),
                        ),
                        node,
                        cache: transaction,
                    };
                }
                std::task::Poll::Ready(Err(error)) => {
                    self.phase = NodeOpenPhase::Complete;
                    return std::task::Poll::Ready(Err(error));
                }
            }
        }
    }
}

impl Drop for PollableNodeOpen {
    fn drop(&mut self) {
        let Some(persistence) = self.persistence.as_mut() else {
            return;
        };
        let _ = self.acquisition.cancel(persistence.as_mut());
        if let NodeOpenPhase::Finalizing { request, .. } = &self.phase {
            let _ = persistence.cancel_request(request.id());
        }
    }
}

fn construct_pollable_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
    history_complete: bool,
    cache: groove::storage::DemandLoadedStorage,
) -> Result<NodeState<groove::storage::DemandLoadedStorage>, Error> {
    if history_complete {
        NodeState::new_history_complete(node_uuid, schema, cache)
    } else {
        NodeState::new(node_uuid, schema, cache)
    }
}

fn missing_node_open_input(
    error: Error,
) -> Result<groove::storage::pollable::OwnedStorageOperation, Error> {
    match error {
        Error::Storage(groove::storage::Error::NotResident { request }) => Ok(*request),
        Error::Groove(groove::db::Error::Storage(error)) => match *error {
            groove::storage::Error::NotResident { request } => Ok(*request),
            error => Err(Error::Storage(error)),
        },
        Error::Groove(groove::db::Error::IvmRuntime(
            groove::ivm::IvmRuntimeError::Storage(groove::storage::Error::NotResident { request }),
        )) => Ok(*request),
        error => Err(error),
    }
}
