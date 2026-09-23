//! Foreground handles for the ordinary core write and streaming APIs.
//! Commands contain binding data only; core owns mutation, upload, and fate semantics.
use super::*;
use jazz::db::{StreamingMutationKind, StreamingValueUpload, WriteHandle};
use std::cell::{Cell, RefCell};

type Writes = Rc<RefCell<BTreeMap<TransactionId, Rc<WriteHandle<MemoryStorage>>>>>;

pub(super) struct MutationHandles {
    pub(super) writes: Writes,
    /// Admitted direct writes the owner has not applied yet, in FIFO order.
    unapplied: RefCell<VecDeque<Rc<WriteHandle<MemoryStorage>>>>,
    uploads: Rc<RefCell<BTreeMap<u64, Rc<StreamingUploadSlot>>>>,
    errors: Rc<RefCell<Vec<jazz::db::MutationErrorEvent>>>,
}

impl MutationHandles {
    #[cfg(test)]
    pub(super) fn upload_count_for_test(&self) -> usize {
        self.uploads.borrow().len()
    }

    pub(super) fn new(db: &Db<MemoryStorage>) -> Self {
        let errors = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&errors);
        db.on_mutation_error(Rc::new(move |event| {
            captured.borrow_mut().push(event.clone())
        }));
        Self {
            writes: Rc::new(RefCell::new(BTreeMap::new())),
            unapplied: RefCell::new(VecDeque::new()),
            uploads: Rc::new(RefCell::new(BTreeMap::new())),
            errors,
        }
    }

    pub(super) fn close(&mut self, db: &Db<MemoryStorage>) -> Result<(), RelayError> {
        db.clear_mutation_error_callback();
        // Unfinished uploads exist solely in this foreground's MemoryStorage.
        // Closing drops its pending operations and Db; no unfinished scalar is
        // published to the persistent relay. Never await a node lock here.
        self.uploads.borrow_mut().clear();
        self.writes.borrow_mut().clear();
        self.unapplied.borrow_mut().clear();
        self.errors.borrow_mut().clear();
        Ok(())
    }

    /// Forget admitted writes the owner has applied (or failed).
    pub(super) fn retire_applied(&self) {
        self.unapplied
            .borrow_mut()
            .retain(|write| write.is_queued_unapplied());
    }

    /// Whether one of this foreground's own admitted writes to `row` has not
    /// been applied yet, so the resident row state cannot decide its outcome.
    fn row_has_unapplied_write(&self, row: RowUuid) -> bool {
        self.retire_applied();
        self.unapplied
            .borrow()
            .iter()
            .any(|write| write.row_uuid() == row)
    }

    pub(super) fn has_errors(&self) -> bool {
        !self.errors.borrow().is_empty()
    }
}

struct StreamingUploadSlot {
    pending: LocalMutex<Option<StreamingMutation>>,
    closing: Cell<bool>,
}

struct StreamingMutation {
    table: String,
    row_id: RowUuid,
    cells: BTreeMap<String, Value>,
    column: String,
    mutation: StreamingMutationKind,
    options: ForegroundMutationOptions,
    upload: StreamingValueUpload,
}

fn poll_write_state_once(
    future: impl Future<Output = Result<jazz::db::WriteState, jazz::db::Error>>,
) -> Result<jazz::db::WriteState, RelayError> {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result.map_err(RelayError::Db),
        Poll::Pending => Err(RelayError::ForegroundCommand(
            "write state is temporarily busy; retry after the next native turn".into(),
        )),
    }
}

fn register_write(writes: &Writes, write: WriteHandle<MemoryStorage>) -> TransactionId {
    let id = TransactionId::from_committed_tx(write.mergeable_tx_id());
    writes.borrow_mut().insert(id, Rc::new(write));
    id
}

pub(super) fn parse_mutation_options(
    mutation: ForegroundMutationKind,
    json: &str,
) -> Result<ForegroundMutationOptions, RelayError> {
    let value: serde_json::Value = serde_json::from_str(json).map_err(|error| {
        RelayError::ForegroundCommand(format!("invalid mutation options: {error}"))
    })?;
    let invalid = match mutation {
        ForegroundMutationKind::Insert | ForegroundMutationKind::Restore => ["head", "base"]
            .into_iter()
            .find(|key| value.get(key).is_some()),
        _ => value.get("branch").map(|_| "branch"),
    };
    if let Some(key) = invalid {
        return Err(RelayError::ForegroundCommand(format!(
            "mutation option `{key}` is not supported for {mutation:?}"
        )));
    }
    serde_json::from_value(value).map_err(|error| {
        RelayError::ForegroundCommand(format!("invalid mutation options: {error}"))
    })
}

impl RelayWorker {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn direct_foreground_mutation(
        &mut self,
        client: u64,
        mutation: ForegroundMutationKind,
        table: String,
        row_id: Option<[u8; 16]>,
        cells: Vec<u8>,
        options_json: String,
    ) -> Result<(TransactionId, RowUuid), RelayError> {
        let options = parse_mutation_options(mutation, &options_json)?;
        let cells = if matches!(mutation, ForegroundMutationKind::Delete) {
            Default::default()
        } else {
            decode_foreground_cells(&cells)?
        };
        let exact_target = options
            .branch
            .clone()
            .map(jazz::db::ExactWriteTarget::Branch)
            .unwrap_or_default();
        let target = match options.head {
            Some(head) => jazz::db::WriteTarget::BranchView {
                head,
                base: options.base,
            },
            None if options.base.is_none() => Default::default(),
            None => {
                return Err(RelayError::ForegroundCommand(
                    "branch view base requires a head selector".into(),
                ));
            }
        };
        let updated_at_ms = options.updated_at_ms;
        let row_id = row_id.map(RowUuid::from_bytes);
        let client_id = client;
        let client = self.foreground_client_mut(client)?;
        // Synchronous class: failures the resident state already decides.
        // Everything discovered while applying reaches the write handle.
        let precheck = match mutation {
            ForegroundMutationKind::Update => jazz::db::ResidentMutationPrecheck::Update(&cells),
            ForegroundMutationKind::Upsert => jazz::db::ResidentMutationPrecheck::Upsert,
            ForegroundMutationKind::Delete => jazz::db::ResidentMutationPrecheck::Delete,
            ForegroundMutationKind::Insert | ForegroundMutationKind::Restore => {
                jazz::db::ResidentMutationPrecheck::Other
            }
        };
        let resident_row = row_id.filter(|row| {
            matches!(target, jazz::db::WriteTarget::Root)
                && !client.mutations.row_has_unapplied_write(*row)
        });
        client
            .db
            .precheck_resident_mutation(&table, resident_row, precheck)
            .map_err(RelayError::Db)?;
        self.ensure_mutation_operation_capacity(client_id)?;
        let client = self.foreground_client_mut(client_id)?;
        let write = match mutation {
            ForegroundMutationKind::Insert => client.db.enqueue_insert(
                table,
                cells,
                jazz::db::InsertOptions {
                    row_id,
                    target: exact_target,
                    updated_at_ms,
                    ..Default::default()
                },
            ),
            mutation => {
                let row_id = row_id.ok_or_else(|| {
                    RelayError::ForegroundCommand("mutation requires row id".into())
                })?;
                match mutation {
                    ForegroundMutationKind::Update => client.db.enqueue_update(
                        table,
                        row_id,
                        cells,
                        UpdateOptions {
                            target,
                            updated_at_ms,
                            ..Default::default()
                        },
                    ),
                    ForegroundMutationKind::Upsert => {
                        if options.branch.is_some() {
                            return Err(RelayError::ForegroundCommand(
                                "upsert option `branch` is not supported; use `head`".into(),
                            ));
                        }
                        client.db.enqueue_upsert(
                            table,
                            row_id,
                            cells,
                            UpsertOptions {
                                target,
                                updated_at_ms,
                                ..Default::default()
                            },
                        )
                    }
                    ForegroundMutationKind::Delete => client.db.enqueue_delete(
                        table,
                        row_id,
                        DeleteOptions {
                            target,
                            updated_at_ms,
                            ..Default::default()
                        },
                    ),
                    ForegroundMutationKind::Restore => client.db.enqueue_restore(
                        table,
                        row_id,
                        Some(cells),
                        jazz::db::RestoreOptions {
                            target: exact_target,
                            updated_at_ms,
                            ..Default::default()
                        },
                    ),
                    ForegroundMutationKind::Insert => unreachable!(),
                }
            }
        }
        .map_err(RelayError::Db)?;
        // Applying, IVM and relay pumping happen in the owner drive turn that
        // follows this command's reply, not while the JS caller waits.
        let row_id = write.row_uuid();
        let id = register_write(&client.mutations.writes, write);
        let registered = Rc::clone(&client.mutations.writes.borrow()[&id]);
        client
            .mutations
            .unapplied
            .borrow_mut()
            .push_back(registered);
        self.drive.request(0);
        Ok((id, row_id))
    }

    /// Bound this foreground's pending mutation work. Streaming operations are
    /// bounded by their retained futures; direct mutations by the core owner
    /// queue behind them. At the direct-mutation cap, admission applies
    /// backpressure by applying queued work inline (the calling JS turn pays
    /// for the excess of a burst) and rejects, admitting nothing, only when
    /// the queue cannot make progress.
    fn ensure_mutation_operation_capacity(&self, client: u64) -> Result<(), RelayError> {
        let client = self.foreground_client(client)?;
        if client.pending_operations.len() + client.mutation_cleanups.len()
            >= NATIVE_RELAY_FOREGROUND_PENDING_MAX
        {
            return Err(RelayError::ForegroundCommand(
                "foreground operation capacity exceeded".into(),
            ));
        }
        let mut polls = 0;
        while client.db.queued_mutation_count() >= NATIVE_RELAY_DIRECT_MUTATION_QUEUE_MAX {
            if polls == NATIVE_RELAY_DIRECT_MUTATION_BACKPRESSURE_POLLS {
                return Err(RelayError::ForegroundCommand(
                    "backpressure: direct mutation queue is full; retry after the next native turn"
                        .into(),
                ));
            }
            client.db.drive_queued_mutation_once();
            polls += 1;
        }
        client.mutations.retire_applied();
        Ok(())
    }

    pub(super) fn foreground_write_state(
        &self,
        client: u64,
        public_id: [u8; 16],
    ) -> Result<String, RelayError> {
        let client = self.foreground_client(client)?;
        let writes = client.mutations.writes.borrow();
        let write = writes
            .iter()
            .find(|(id, _)| id.as_bytes() == &public_id)
            .map(|(_, write)| write);
        let state = match write {
            Some(write) => poll_write_state_once(write.write_state()),
            None => {
                let tx_id = client
                    .committed_transactions
                    .iter()
                    .find(|(id, _)| id.as_bytes() == &public_id)
                    .map(|(_, tx_id)| *tx_id)
                    .ok_or_else(|| {
                        RelayError::ForegroundCommand("unknown foreground write".into())
                    })?;
                poll_write_state_once(client.db.write_state_async(tx_id))
            }
        }?;
        serde_json::to_string(&state)
            .map_err(|error| RelayError::ForegroundCommand(error.to_string()))
    }

    pub(super) fn drain_foreground_mutation_errors(
        &self,
        client: u64,
    ) -> Result<String, RelayError> {
        let client = self.foreground_client(client)?;
        let events = std::mem::take(&mut *client.mutations.errors.borrow_mut());
        serde_json::to_string(&events)
            .map_err(|error| RelayError::ForegroundCommand(error.to_string()))
    }

    #[allow(clippy::too_many_arguments)] // Versioned flat command envelope.
    pub(super) fn begin_foreground_streaming_mutation(
        &mut self,
        client: u64,
        mutation: ForegroundMutationKind,
        table: String,
        row_id: [u8; 16],
        cells: Vec<u8>,
        column: String,
        options_json: String,
    ) -> Result<u64, RelayError> {
        let mutation = match mutation {
            ForegroundMutationKind::Insert => StreamingMutationKind::Insert,
            ForegroundMutationKind::Update => StreamingMutationKind::Update,
            ForegroundMutationKind::Upsert => StreamingMutationKind::Upsert,
            _ => {
                return Err(RelayError::ForegroundCommand(
                    "streaming mutation must be insert, update, or upsert".into(),
                ));
            }
        };
        let options: ForegroundMutationOptions =
            serde_json::from_str(&options_json).map_err(|error| {
                RelayError::ForegroundCommand(format!("invalid streaming options: {error}"))
            })?;
        if options.branch.is_some() || (options.base.is_some() && options.head.is_none()) {
            return Err(RelayError::ForegroundCommand(
                "streaming branch view requires head and optional base".into(),
            ));
        }
        let cells = decode_foreground_cells(&cells)?;
        let client = self.foreground_client_mut(client)?;
        if client.mutations.uploads.borrow().len() >= NATIVE_RELAY_FOREGROUND_TRANSACTION_MAX {
            return Err(RelayError::ForegroundCommand(
                "foreground streaming upload capacity exceeded".into(),
            ));
        }
        let upload = client
            .db
            .begin_streaming_value_upload(&table, &cells, &column)
            .map_err(RelayError::Db)?;
        let handle = Self::next_foreground_handle(client)?;
        client.mutations.uploads.borrow_mut().insert(
            handle,
            Rc::new(StreamingUploadSlot {
                closing: Cell::new(false),
                pending: LocalMutex::new(Some(StreamingMutation {
                    table,
                    row_id: RowUuid::from_bytes(row_id),
                    cells,
                    column,
                    mutation,
                    options,
                    upload,
                })),
            }),
        );
        Ok(handle)
    }

    pub(super) fn push_foreground_streaming_mutation(
        &mut self,
        client: u64,
        handle: u64,
        chunk: Vec<u8>,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        if chunk.len() > 64 * 1024 {
            return Err(RelayError::ForegroundCommand(
                "streaming chunks must fit the 64 KiB host window".into(),
            ));
        }
        self.ensure_mutation_operation_capacity(client)?;
        let (db, slot) = {
            let client = self.foreground_client_mut(client)?;
            let slot = client
                .mutations
                .uploads
                .borrow()
                .get(&handle)
                .cloned()
                .filter(|slot| !slot.closing.get())
                .ok_or_else(|| {
                    RelayError::ForegroundCommand("streaming mutation is closed".into())
                })?;
            (Rc::clone(&client.db), slot)
        };
        let future: ForegroundOperationFuture = Box::pin(async move {
            let mut pending = slot.pending.lock().await;
            let pending = pending.as_mut().ok_or_else(|| {
                RelayError::ForegroundCommand("streaming mutation is closed".into())
            })?;
            db.push_streaming_value_upload(&mut pending.upload, &chunk)
                .await
                .map_err(RelayError::Db)?;
            Ok(ForegroundOperationResult::StreamingMutationPushed)
        });
        self.start_foreground_operation(client, None, future)
    }

    pub(super) fn finish_foreground_streaming_mutation(
        &mut self,
        client: u64,
        handle: u64,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        self.ensure_mutation_operation_capacity(client)?;
        let (db, writes, uploads, slot) = {
            let client = self.foreground_client_mut(client)?;
            let slot = client
                .mutations
                .uploads
                .borrow()
                .get(&handle)
                .cloned()
                .filter(|slot| !slot.closing.replace(true))
                .ok_or_else(|| {
                    RelayError::ForegroundCommand("streaming mutation is closed".into())
                })?;
            (
                Rc::clone(&client.db),
                Rc::clone(&client.mutations.writes),
                Rc::clone(&client.mutations.uploads),
                slot,
            )
        };
        let future: ForegroundOperationFuture = Box::pin(async move {
            let pending = slot.pending.lock().await.take().ok_or_else(|| {
                RelayError::ForegroundCommand("streaming mutation is closed".into())
            })?;
            let result = db
                .finish_streaming_value_upload(
                    pending.upload,
                    pending.mutation,
                    &pending.table,
                    pending.row_id,
                    pending.cells,
                    &pending.column,
                    jazz::db::WriteIdentity::Database,
                    pending.options.updated_at_ms,
                    pending.options.head,
                    pending.options.base,
                )
                .await;
            uploads.borrow_mut().remove(&handle);
            let write = result.map_err(RelayError::Db)?;
            Ok(ForegroundOperationResult::TransactionCommitted(
                register_write(&writes, write),
            ))
        });
        let result = self.start_foreground_operation(client, None, future)?;
        if let ForegroundOperationPoll::Pending { operation } = result {
            self.foreground_client_mut(client)?
                .pending_operations
                .get_mut(&operation)
                .expect("new pending mutation")
                .finish_on_cancel = true;
        }
        Ok(result)
    }

    pub(super) fn abort_foreground_streaming_mutation(
        &mut self,
        client: u64,
        handle: u64,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        self.ensure_mutation_operation_capacity(client)?;
        let (db, uploads, slot) = {
            let client = self.foreground_client_mut(client)?;
            let slot = client
                .mutations
                .uploads
                .borrow()
                .get(&handle)
                .cloned()
                .filter(|slot| !slot.closing.replace(true));
            (
                Rc::clone(&client.db),
                Rc::clone(&client.mutations.uploads),
                slot,
            )
        };
        let future: ForegroundOperationFuture = Box::pin(async move {
            let Some(slot) = slot else {
                return Ok(ForegroundOperationResult::StreamingMutationAborted(false));
            };
            let pending = slot.pending.lock().await.take();
            let result = match pending {
                Some(pending) => db.abort_streaming_value_upload(pending.upload).await,
                None => Ok(()),
            };
            uploads.borrow_mut().remove(&handle);
            result.map_err(RelayError::Db)?;
            Ok(ForegroundOperationResult::StreamingMutationAborted(true))
        });
        let result = self.start_foreground_operation(client, None, future)?;
        if let ForegroundOperationPoll::Pending { operation } = result {
            self.foreground_client_mut(client)?
                .pending_operations
                .get_mut(&operation)
                .expect("new pending mutation")
                .finish_on_cancel = true;
        }
        Ok(result)
    }

    pub(super) fn update_foreground_large_values(
        &mut self,
        client: u64,
        table: String,
        row_id: [u8; 16],
        patch: Vec<u8>,
        descriptors_json: String,
        updated_at_ms: Option<u64>,
    ) -> Result<TransactionId, RelayError> {
        let patch = decode_foreground_cells(&patch)?;
        let descriptors = serde_json::from_str(&descriptors_json).map_err(|error| {
            RelayError::ForegroundCommand(format!(
                "invalid partial-value update descriptor: {error}"
            ))
        })?;
        let client = self.foreground_client_mut(client)?;
        let write = client
            .db
            .enqueue_large_value_update(
                table,
                RowUuid::from_bytes(row_id),
                patch,
                descriptors,
                updated_at_ms,
            )
            .map_err(RelayError::Db)?;
        client.db.drive_queued_mutation_once();
        if let Some(error) = client
            .db
            .take_queued_mutation_failure(write.mergeable_tx_id())
        {
            return Err(RelayError::Db(error));
        }
        Ok(register_write(&client.mutations.writes, write))
    }
}

impl NativeRelayClient {
    pub(super) fn execute_mutation_command(
        &self,
        command: ForegroundDbCommandRequest,
    ) -> Result<ForegroundDbCommandResponse, RelayError> {
        let id = self.id;
        self.relay.run(move |worker| {
            use ForegroundDbCommandRequest as Request;
            use ForegroundDbCommandResponse as Response;
            Ok(match command {
                Request::DirectMutation {
                    mutation,
                    table,
                    row_id,
                    cells,
                    options_json,
                } => {
                    let (tx_id, row_id) = worker.direct_foreground_mutation(
                        id,
                        mutation,
                        table,
                        row_id,
                        cells,
                        options_json,
                    )?;
                    Response::MutationCommitted {
                        tx_id: *tx_id.as_bytes(),
                        row_id: *row_id.as_bytes(),
                    }
                }
                Request::WriteState { tx_id } => Response::WriteState {
                    state_json: worker.foreground_write_state(id, tx_id)?,
                },
                Request::DrainMutationErrors => Response::MutationErrors {
                    events_json: worker.drain_foreground_mutation_errors(id)?,
                },
                Request::BeginStreamingMutation {
                    mutation,
                    table,
                    row_id,
                    cells,
                    column,
                    options_json,
                } => Response::StreamingMutationOpened {
                    upload: worker.begin_foreground_streaming_mutation(
                        id,
                        mutation,
                        table,
                        row_id,
                        cells,
                        column,
                        options_json,
                    )?,
                },
                Request::PushStreamingMutation { upload, chunk } => foreground_operation_response(
                    worker.push_foreground_streaming_mutation(id, upload, chunk)?,
                ),
                Request::FinishStreamingMutation { upload } => foreground_operation_response(
                    worker.finish_foreground_streaming_mutation(id, upload)?,
                ),
                Request::AbortStreamingMutation { upload } => foreground_operation_response(
                    worker.abort_foreground_streaming_mutation(id, upload)?,
                ),
                Request::UpdateLargeValues {
                    table,
                    row_id,
                    patch,
                    descriptors_json,
                    updated_at_ms,
                } => Response::TransactionCommitted {
                    tx_id: *worker
                        .update_foreground_large_values(
                            id,
                            table,
                            row_id,
                            patch,
                            descriptors_json,
                            updated_at_ms,
                        )?
                        .as_bytes(),
                },
                _ => {
                    return Err(RelayError::ForegroundCommand(
                        "not a mutation command".into(),
                    ));
                }
            })
        })
    }
}
