//! Test conveniences for the shared SQLite host's actual command surface.
//! They replace removed private entry points; no operation is emulated here.
use super::*;
use jazz::tools::TransactionId;

#[derive(Debug)]
pub(super) enum ForegroundOperationResult {
    PermissionAdvice(ForegroundPermissionAdvice),
    Rows(Vec<u8>),
    SubscriptionEvents(Vec<ForegroundSubscriptionEvent>),
    TransactionSettled(TransactionId),
}
#[derive(Debug)]
pub(super) enum ForegroundOperationPoll {
    Pending {
        operation: u64,
    },
    Ready(ForegroundOperationResult),
    Error {
        code: Option<String>,
        reason: String,
    },
}
fn polled(response: ForegroundDbCommandResponse) -> ForegroundOperationPoll {
    use ForegroundDbCommandResponse as R;
    use ForegroundOperationResult as V;
    let value = match response {
        R::Pending { operation } => return ForegroundOperationPoll::Pending { operation },
        R::OperationError { reason } => {
            return ForegroundOperationPoll::Error { code: None, reason };
        }
        R::CodedOperationError { code, reason } => {
            return ForegroundOperationPoll::Error {
                code: Some(code),
                reason,
            };
        }
        R::Rows { rows } => V::Rows(rows),
        R::SubscriptionEvents { events } => V::SubscriptionEvents(events),
        R::PermissionAdvice { advice } => V::PermissionAdvice(advice),
        R::TransactionSettled { tx_id } => V::TransactionSettled(TransactionId(tx_id)),
        other => panic!("unexpected polled fixture response: {other:?}"),
    };
    ForegroundOperationPoll::Ready(value)
}
fn required(
    response: ForegroundDbCommandResponse,
) -> Result<ForegroundDbCommandResponse, RelayError> {
    match response {
        ForegroundDbCommandResponse::OperationError { reason } => {
            Err(RelayError::ForegroundCommand(reason))
        }
        ForegroundDbCommandResponse::CodedOperationError { code, reason } => {
            Err(RelayError::ForegroundCommand(format!("{code}: {reason}")))
        }
        response => Ok(response),
    }
}
fn read_options(opts: ReadOpts) -> String {
    let mut value =
        serde_json::json!({ "tier": format!("{:?}", opts.tier), "read_view": opts.read_view });
    if let jazz::db::FirstLoad::WaitForRemote { timeout_ms } = opts.first_load {
        value["first_load_remote_wait_ms"] = timeout_ms.into();
    }
    value.to_string()
}

impl NativeRelayClient {
    pub(super) fn start_foreground_read(
        &self,
        query: Vec<u8>,
        options_json: String,
        transaction: Option<u64>,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let response = self.execute(ForegroundDbCommandRequest::All {
            query,
            options_json,
            transaction,
        })?;
        Ok(polled(response))
    }
    pub(super) fn poll_foreground_operation(
        &self,
        operation: u64,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let response = self.execute(ForegroundDbCommandRequest::Poll { operation })?;
        Ok(polled(response))
    }
    pub(super) fn request_foreground_permission_advice(
        &self,
        action: ForegroundPermissionAdviceAction,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let response = self.execute(ForegroundDbCommandRequest::PermissionAdvice { action })?;
        Ok(polled(response))
    }
    pub(super) fn initialization_command(
        &self,
        action: InitializationAction,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let response =
            self.execute(ForegroundDbCommandRequest::InitializationV1 { version: 1, action })?;
        Ok(polled(required(response)?))
    }
    pub(super) fn wait_for_foreground_transaction(
        &self,
        tx_id: [u8; 16],
        tier: CoreDurabilityTier,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let response = self.execute(ForegroundDbCommandRequest::WaitForTransaction {
            tx_id,
            tier: format!("{tier:?}"),
            observe_only: false,
        })?;
        Ok(polled(response))
    }

    pub(super) fn drain_foreground_subscription(
        &self,
        subscription: u64,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let response =
            self.execute(ForegroundDbCommandRequest::DrainSubscription { subscription })?;
        Ok(polled(response))
    }
    pub(super) fn cancel_foreground_operation(&self, operation: u64) -> Result<bool, RelayError> {
        let response = self.execute(ForegroundDbCommandRequest::Cancel { operation })?;
        match required(response)? {
            ForegroundDbCommandResponse::Cancelled { cancelled } => Ok(cancelled),
            other => panic!("unexpected fixture response: {other:?}"),
        }
    }

    pub(super) fn subscribe_foreground_query_with_options(
        &self,
        query: Vec<u8>,
        opts: ReadOpts,
    ) -> Result<u64, RelayError> {
        let response = self.execute(ForegroundDbCommandRequest::Subscribe {
            query,
            options_json: read_options(opts),
        })?;
        match required(response)? {
            ForegroundDbCommandResponse::Subscribed { subscription } => Ok(subscription),
            other => panic!("unexpected fixture response: {other:?}"),
        }
    }
    pub(super) fn begin_foreground_transaction(
        &self,
        kind: ForegroundTransactionKind,
    ) -> Result<u64, RelayError> {
        let response = self.execute(ForegroundDbCommandRequest::BeginTransaction { kind })?;
        match required(response)? {
            ForegroundDbCommandResponse::TransactionOpened { transaction } => Ok(transaction),
            other => panic!("unexpected fixture response: {other:?}"),
        }
    }
    pub(super) fn insert_foreground_transaction(
        &self,
        transaction: u64,
        table: String,
        cells: Vec<u8>,
        row_id: Option<[u8; 16]>,
    ) -> Result<RowUuid, RelayError> {
        let response = self.execute(ForegroundDbCommandRequest::Insert {
            transaction,
            table,
            cells,
            row_id,
        })?;
        match required(response)? {
            ForegroundDbCommandResponse::Inserted { row_id } => Ok(RowUuid::from_bytes(row_id)),
            other => panic!("unexpected fixture response: {other:?}"),
        }
    }
    pub(super) fn update_foreground_transaction(
        &self,
        transaction: u64,
        table: String,
        row_id: [u8; 16],
        patch: Vec<u8>,
    ) -> Result<(), RelayError> {
        let response = self.execute(ForegroundDbCommandRequest::Update {
            transaction,
            table,
            row_id,
            patch,
        })?;
        match required(response)? {
            ForegroundDbCommandResponse::MutationStaged => Ok(()),
            other => panic!("unexpected fixture response: {other:?}"),
        }
    }

    pub(super) fn commit_foreground_transaction(
        &self,
        transaction: u64,
    ) -> Result<TransactionId, RelayError> {
        let response =
            self.execute(ForegroundDbCommandRequest::CommitTransaction { transaction })?;
        match required(response)? {
            ForegroundDbCommandResponse::TransactionCommitted { tx_id } => Ok(TransactionId(tx_id)),
            other => panic!("unexpected fixture response: {other:?}"),
        }
    }
    pub(super) fn rollback_foreground_transaction(
        &self,
        transaction: u64,
    ) -> Result<bool, RelayError> {
        let response =
            self.execute(ForegroundDbCommandRequest::RollbackTransaction { transaction })?;
        match required(response)? {
            ForegroundDbCommandResponse::TransactionRolledBack { rolled_back } => Ok(rolled_back),
            other => panic!("unexpected fixture response: {other:?}"),
        }
    }
    pub(super) fn local_current_foreground_row(
        &self,
        table: String,
        row_id: [u8; 16],
    ) -> Result<Vec<u8>, RelayError> {
        let response =
            self.execute(ForegroundDbCommandRequest::LocalCurrentRow { table, row_id })?;
        match required(response)? {
            ForegroundDbCommandResponse::Rows { rows } => Ok(rows),
            other => panic!("unexpected fixture response: {other:?}"),
        }
    }

    pub(super) fn direct_foreground_mutation(
        &self,
        mutation: ForegroundMutationKind,
        table: String,
        row_id: Option<[u8; 16]>,
        cells: Vec<u8>,
        options_json: String,
    ) -> Result<(TransactionId, RowUuid), RelayError> {
        let response = self.execute(ForegroundDbCommandRequest::DirectMutation {
            mutation,
            table,
            row_id,
            cells,
            options_json,
        })?;
        match required(response)? {
            ForegroundDbCommandResponse::MutationCommitted { tx_id, row_id } => {
                Ok((TransactionId(tx_id), RowUuid::from_bytes(row_id)))
            }
            other => panic!("unexpected fixture response: {other:?}"),
        }
    }
    pub(super) fn execute_mutation_command(
        &self,
        command: ForegroundDbCommandRequest,
    ) -> Result<ForegroundDbCommandResponse, RelayError> {
        self.execute(command)
    }
}

impl RelayWorker {
    pub(super) fn start_foreground_read(
        &mut self,
        client: u64,
        query: Vec<u8>,
        options_json: String,
        transaction: Option<u64>,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let response = self.execute_foreground(
            client,
            ForegroundDbCommandRequest::All {
                query,
                options_json,
                transaction,
            },
        )?;
        Ok(polled(response))
    }
    pub(super) fn poll_foreground_operation(
        &mut self,
        client: u64,
        operation: u64,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let response =
            self.execute_foreground(client, ForegroundDbCommandRequest::Poll { operation })?;
        Ok(polled(response))
    }
    pub(super) fn request_foreground_permission_advice(
        &mut self,
        client: u64,
        action: ForegroundPermissionAdviceAction,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let response = self.execute_foreground(
            client,
            ForegroundDbCommandRequest::PermissionAdvice { action },
        )?;
        Ok(polled(response))
    }

    pub(super) fn drain_foreground_subscription(
        &mut self,
        client: u64,
        subscription: u64,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let response = self.execute_foreground(
            client,
            ForegroundDbCommandRequest::DrainSubscription { subscription },
        )?;
        Ok(polled(response))
    }
    pub(super) fn cancel_foreground_operation(
        &mut self,
        client: u64,
        operation: u64,
    ) -> Result<bool, RelayError> {
        let response =
            self.execute_foreground(client, ForegroundDbCommandRequest::Cancel { operation })?;
        match required(response)? {
            ForegroundDbCommandResponse::Cancelled { cancelled } => Ok(cancelled),
            other => panic!("unexpected fixture response: {other:?}"),
        }
    }
    pub(super) fn close_foreground_subscription(
        &mut self,
        client: u64,
        subscription: u64,
    ) -> Result<bool, RelayError> {
        let response = self.execute_foreground(
            client,
            ForegroundDbCommandRequest::Unsubscribe { subscription },
        )?;
        match required(response)? {
            ForegroundDbCommandResponse::Unsubscribed { closed } => Ok(closed),
            other => panic!("unexpected fixture response: {other:?}"),
        }
    }
    pub(super) fn subscribe_foreground_query_with_options(
        &mut self,
        client: u64,
        query: Vec<u8>,
        opts: ReadOpts,
    ) -> Result<u64, RelayError> {
        let response = self.execute_foreground(
            client,
            ForegroundDbCommandRequest::Subscribe {
                query,
                options_json: read_options(opts),
            },
        )?;
        match required(response)? {
            ForegroundDbCommandResponse::Subscribed { subscription } => Ok(subscription),
            other => panic!("unexpected fixture response: {other:?}"),
        }
    }
    pub(super) fn begin_foreground_transaction(
        &mut self,
        client: u64,
        kind: ForegroundTransactionKind,
    ) -> Result<u64, RelayError> {
        let response = self.execute_foreground(
            client,
            ForegroundDbCommandRequest::BeginTransaction { kind },
        )?;
        match required(response)? {
            ForegroundDbCommandResponse::TransactionOpened { transaction } => Ok(transaction),
            other => panic!("unexpected fixture response: {other:?}"),
        }
    }
    pub(super) fn insert_foreground_transaction(
        &mut self,
        client: u64,
        transaction: u64,
        table: String,
        cells: Vec<u8>,
        row_id: Option<[u8; 16]>,
    ) -> Result<RowUuid, RelayError> {
        let response = self.execute_foreground(
            client,
            ForegroundDbCommandRequest::Insert {
                transaction,
                table,
                cells,
                row_id,
            },
        )?;
        match required(response)? {
            ForegroundDbCommandResponse::Inserted { row_id } => Ok(RowUuid::from_bytes(row_id)),
            other => panic!("unexpected fixture response: {other:?}"),
        }
    }

    pub(super) fn rollback_foreground_transaction(
        &mut self,
        client: u64,
        transaction: u64,
    ) -> Result<bool, RelayError> {
        let response = self.execute_foreground(
            client,
            ForegroundDbCommandRequest::RollbackTransaction { transaction },
        )?;
        match required(response)? {
            ForegroundDbCommandResponse::TransactionRolledBack { rolled_back } => Ok(rolled_back),
            other => panic!("unexpected fixture response: {other:?}"),
        }
    }

    pub(super) fn direct_foreground_mutation(
        &mut self,
        client: u64,
        mutation: ForegroundMutationKind,
        table: String,
        row_id: Option<[u8; 16]>,
        cells: Vec<u8>,
        options_json: String,
    ) -> Result<(TransactionId, RowUuid), RelayError> {
        let response = self.execute_foreground(
            client,
            ForegroundDbCommandRequest::DirectMutation {
                mutation,
                table,
                row_id,
                cells,
                options_json,
            },
        )?;
        match required(response)? {
            ForegroundDbCommandResponse::MutationCommitted { tx_id, row_id } => {
                Ok((TransactionId(tx_id), RowUuid::from_bytes(row_id)))
            }
            other => panic!("unexpected fixture response: {other:?}"),
        }
    }
}

impl NativeRelayClient {
    pub(super) fn stage_foreground_mutation(
        &self,
        transaction: u64,
        mutation: ForegroundMutationKind,
        table: String,
        row_id: Option<[u8; 16]>,
        cells: Vec<u8>,
        options_json: String,
    ) -> Result<(), RelayError> {
        match required(self.execute(ForegroundDbCommandRequest::StageMutation {
            transaction,
            mutation,
            table,
            row_id,
            cells,
            options_json,
        })?)? {
            ForegroundDbCommandResponse::MutationStaged
            | ForegroundDbCommandResponse::Inserted { .. } => Ok(()),
            other => panic!("unexpected stage response: {other:?}"),
        }
    }
}
impl RelayWorker {
    pub(super) fn foreground_write_state(
        &mut self,
        client: u64,
        tx_id: [u8; 16],
    ) -> Result<String, RelayError> {
        match required(
            self.execute_foreground(client, ForegroundDbCommandRequest::WriteState { tx_id })?,
        )? {
            ForegroundDbCommandResponse::WriteState { state_json } => Ok(state_json),
            other => panic!("unexpected write state: {other:?}"),
        }
    }
    pub(super) fn retire_socket_for_test(&mut self, generation: u64) -> Result<bool, RelayError> {
        let current = self
            .upstream
            .as_ref()
            .is_some_and(|upstream| upstream.generation == generation);
        self.retire_socket_upstream(generation);
        Ok(current)
    }
    pub(super) fn has_drive_work_for_test(&self) -> bool {
        self.redrive
            || self.persistent_dirty.get()
            || self.clients.values().any(|client| client.dirty.get())
    }
}
