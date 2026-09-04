//! Foreground handles for the ordinary core write and streaming APIs.
//! Commands contain binding data only; core owns mutation, upload, and fate semantics.
use super::*;
use jazz::db::{StreamingMutationKind, StreamingValueUpload, WriteHandle};
use std::cell::RefCell;

type Writes = Rc<RefCell<BTreeMap<TransactionId, Rc<WriteHandle<MemoryStorage>>>>>;

pub(super) struct MutationHandles {
    pub(super) writes: Writes,
    uploads: BTreeMap<u64, StreamingMutation>,
    errors: Rc<RefCell<Vec<jazz::db::MutationErrorEvent>>>,
}

impl MutationHandles {
    pub(super) fn new(db: &Db<MemoryStorage>) -> Self {
        let errors = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&errors);
        db.on_mutation_error(Rc::new(move |event| {
            captured.borrow_mut().push(event.clone())
        }));
        Self {
            writes: Rc::new(RefCell::new(BTreeMap::new())),
            uploads: BTreeMap::new(),
            errors,
        }
    }

    pub(super) fn close(&mut self, db: &Db<MemoryStorage>) -> Result<(), RelayError> {
        db.clear_mutation_error_callback();
        let mut first_error = None;
        for (_, pending) in std::mem::take(&mut self.uploads) {
            if let Err(error) = block_on(db.abort_streaming_value_upload(pending.upload)) {
                first_error.get_or_insert(error);
            }
        }
        self.writes.borrow_mut().clear();
        self.errors.borrow_mut().clear();
        first_error.map_or(Ok(()), |error| Err(RelayError::Db(error)))
    }
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

fn register_write(writes: &Writes, write: WriteHandle<MemoryStorage>) -> TransactionId {
    let id = TransactionId::from_committed_tx(write.mergeable_tx_id());
    writes.borrow_mut().insert(id, Rc::new(write));
    id
}

impl RelayWorker {
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
            Some(write) => block_on(write.write_state()),
            None => {
                let tx_id = client
                    .committed_transactions
                    .iter()
                    .find(|(id, _)| id.as_bytes() == &public_id)
                    .map(|(_, tx_id)| *tx_id)
                    .ok_or_else(|| {
                        RelayError::ForegroundCommand("unknown foreground write".into())
                    })?;
                client.db.write_state(tx_id)
            }
        }
        .map_err(RelayError::Db)?;
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
        if client.mutations.uploads.len() >= NATIVE_RELAY_FOREGROUND_TRANSACTION_MAX {
            return Err(RelayError::ForegroundCommand(
                "foreground streaming upload capacity exceeded".into(),
            ));
        }
        let upload = client
            .db
            .begin_streaming_value_upload(&table, &cells, &column)
            .map_err(RelayError::Db)?;
        let handle = Self::next_foreground_handle(client)?;
        client.mutations.uploads.insert(
            handle,
            StreamingMutation {
                table,
                row_id: RowUuid::from_bytes(row_id),
                cells,
                column,
                mutation,
                options,
                upload,
            },
        );
        Ok(handle)
    }

    pub(super) fn push_foreground_streaming_mutation(
        &mut self,
        client: u64,
        handle: u64,
        chunk: Vec<u8>,
    ) -> Result<(), RelayError> {
        if chunk.len() > 64 * 1024 {
            return Err(RelayError::ForegroundCommand(
                "streaming chunks must fit the 64 KiB host window".into(),
            ));
        }
        let client = self.foreground_client_mut(client)?;
        let pending =
            client.mutations.uploads.get_mut(&handle).ok_or_else(|| {
                RelayError::ForegroundCommand("streaming mutation is closed".into())
            })?;
        block_on(
            client
                .db
                .push_streaming_value_upload(&mut pending.upload, &chunk),
        )
        .map_err(RelayError::Db)
    }

    pub(super) fn finish_foreground_streaming_mutation(
        &mut self,
        client: u64,
        handle: u64,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let (db, writes, pending) = {
            let client = self.foreground_client_mut(client)?;
            let pending = client.mutations.uploads.remove(&handle).ok_or_else(|| {
                RelayError::ForegroundCommand("streaming mutation is closed".into())
            })?;
            (
                Rc::clone(&client.db),
                Rc::clone(&client.mutations.writes),
                pending,
            )
        };
        let future: ForegroundOperationFuture = Box::pin(async move {
            let write = db
                .finish_streaming_value_upload(
                    pending.upload,
                    pending.mutation,
                    &pending.table,
                    pending.row_id,
                    pending.cells,
                    &pending.column,
                    None,
                    pending.options.updated_at_ms,
                    pending.options.head,
                    pending.options.base,
                    None,
                )
                .await
                .map_err(RelayError::Db)?;
            Ok(ForegroundOperationResult::TransactionCommitted(
                register_write(&writes, write),
            ))
        });
        self.start_foreground_operation(client, None, future)
    }

    pub(super) fn abort_foreground_streaming_mutation(
        &mut self,
        client: u64,
        handle: u64,
    ) -> Result<bool, RelayError> {
        let client = self.foreground_client_mut(client)?;
        let Some(pending) = client.mutations.uploads.remove(&handle) else {
            return Ok(false);
        };
        block_on(client.db.abort_streaming_value_upload(pending.upload)).map_err(RelayError::Db)?;
        Ok(true)
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
                Request::PushStreamingMutation { upload, chunk } => {
                    worker.push_foreground_streaming_mutation(id, upload, chunk)?;
                    Response::StreamingMutationPushed
                }
                Request::FinishStreamingMutation { upload } => foreground_operation_response(
                    worker.finish_foreground_streaming_mutation(id, upload)?,
                ),
                Request::AbortStreamingMutation { upload } => Response::StreamingMutationAborted {
                    aborted: worker.abort_foreground_streaming_mutation(id, upload)?,
                },
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
