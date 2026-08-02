use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;

use crate::db::{CommitUnitTrust, DbIdentity, Transport};
use crate::groove::records::Value;
use crate::ids::{AuthorId, NodeUuid, RowUuid, SchemaVersionId};
use crate::node::{EdgeCacheBudget, OpenTxId, OpenTxLargeValueCell};
use crate::protocol::MigrationLens;
use crate::query::Query;
use crate::schema::{JazzSchema, TableSchema};
use crate::serving::{
    AbiBytes, InMemoryServerShell, InMemoryServerShellConfig, NodeRole, ServerSession,
    StorageConfig,
};
use crate::tools::server::shutdown::ActivePostgresQueryGuard;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch};

const MAX_ADMITTED_POSTGRES_OWNER_JOBS: usize = 8;
const MAX_POSTGRES_TRANSACTION_ROWS: usize = 1_000;
const MAX_POSTGRES_TRANSACTION_CELL_BYTES: usize = 1024 * 1024;

/// Sendable handle for the thread that owns the in-memory server shell.
///
/// The underlying `InMemoryServerShell` is intentionally kept on one OS thread
/// because it currently stores its DB, sessions, and transports behind
/// `Rc<RefCell<...>>`. Axum request/websocket tasks can clone this handle, but
/// all shell access is serialized onto that owner thread.
#[derive(Clone)]
pub(crate) struct ServerShellHandle {
    jobs: mpsc::Sender<ServerShellJob>,
    activity_tx: watch::Sender<u64>,
    postgres_job_slots: Arc<Semaphore>,
    next_postgres_transaction_id: Arc<AtomicU64>,
}

type ServerShellJob = Box<dyn FnOnce(&mut ServerShellOwner) + Send + 'static>;

struct ServerShellOwner {
    shell: InMemoryServerShell,
    postgres_transactions: BTreeMap<u64, PostgresOwnedTransaction>,
}

struct PostgresOwnedTransaction {
    open_tx_id: OpenTxId,
    schema_version: SchemaVersionId,
    staged_rows: usize,
    staged_cell_bytes: usize,
    inserted_rows: BTreeSet<(String, RowUuid)>,
}

/// RAII lease for a PostgreSQL transaction retained by the shell owner thread.
/// Dropping the lease queues a rollback, including when an async request is
/// cancelled after the owner thread opened the Jazz transaction.
pub(crate) struct PostgresOpenTransaction {
    shell: ServerShellHandle,
    owner_id: u64,
    schema_version: Option<SchemaVersionId>,
    active: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PostgresRow {
    pub(crate) values: Vec<Option<Value>>,
}

pub(crate) struct PostgresQueryResult {
    pub(crate) table: TableSchema,
    pub(crate) rows: Vec<PostgresRow>,
    pub(crate) response_permit: OwnedSemaphorePermit,
}

pub(crate) enum PostgresMutation {
    Insert {
        rows: Vec<(RowUuid, BTreeMap<String, Value>)>,
    },
    Update {
        query: Query,
        bindings: BTreeMap<String, Value>,
        patch: BTreeMap<String, Value>,
    },
    Delete {
        row: RowUuid,
    },
}

pub(crate) struct PostgresMutationResult {
    pub(crate) affected_rows: usize,
    pub(crate) rows: Vec<RowUuid>,
}

impl PostgresOpenTransaction {
    pub(crate) fn schema_version(&self) -> SchemaVersionId {
        self.schema_version
            .expect("a returned PostgreSQL transaction has a pinned schema")
    }

    pub(crate) async fn commit(mut self) -> Result<(), String> {
        let activity_tx = self.shell.activity_tx.clone();
        let result = self
            .shell
            .finish_postgres_transaction(self.owner_id, true)
            .await;
        if result.is_ok() {
            self.active = false;
            notify_shell_activity(&activity_tx);
        }
        result
    }

    pub(crate) async fn rollback(mut self) -> Result<(), String> {
        let result = self
            .shell
            .finish_postgres_transaction(self.owner_id, false)
            .await;
        if result.is_ok() {
            self.active = false;
        }
        result
    }
}

impl Drop for PostgresOpenTransaction {
    fn drop(&mut self) {
        if self.active {
            self.shell
                .queue_postgres_transaction_rollback(self.owner_id);
        }
    }
}

impl ServerShellHandle {
    pub(crate) fn start_with_storage(
        schema: JazzSchema,
        storage_config: StorageConfig,
    ) -> Result<Self, String> {
        Self::start_with_storage_config_and_permissions(
            schema,
            storage_config,
            NodeRole::Core,
            None,
            false,
        )
    }

    pub(crate) fn start_with_storage_config(
        schema: JazzSchema,
        storage_config: StorageConfig,
        role: NodeRole,
        edge_cache_budget: Option<EdgeCacheBudget>,
    ) -> Result<Self, String> {
        Self::start_with_storage_config_and_permissions(
            schema,
            storage_config,
            role,
            edge_cache_budget,
            true,
        )
    }

    fn start_with_storage_config_and_permissions(
        schema: JazzSchema,
        storage_config: StorageConfig,
        role: NodeRole,
        edge_cache_budget: Option<EdgeCacheBudget>,
        permissions_ready: bool,
    ) -> Result<Self, String> {
        let (jobs, receiver) = mpsc::channel::<ServerShellJob>();
        let (started_tx, started_rx) = mpsc::channel();
        let (activity_tx, _) = watch::channel(0_u64);

        thread::Builder::new()
            .name("jazz-server-shell".to_owned())
            .spawn(move || {
                let config = InMemoryServerShellConfig::new(
                    schema,
                    DbIdentity {
                        node: NodeUuid::from_bytes([0x5e; 16]),
                        author: AuthorId::SYSTEM,
                    },
                )
                .with_row_id_seed(0x5e)
                .with_runtime_schema_bootstrap()
                .with_role(role);
                let config = match edge_cache_budget {
                    Some(budget) => config.with_edge_cache_budget(budget),
                    None => config,
                };
                let shell = match InMemoryServerShell::start_with_storage(config, storage_config) {
                    Ok(mut shell) => {
                        if !permissions_ready && let Err(error) = shell.set_permissions_ready(false)
                        {
                            let _ = started_tx.send(Err(error.to_string()));
                            return;
                        }
                        let _ = started_tx.send(Ok(()));
                        shell
                    }
                    Err(error) => {
                        let _ = started_tx.send(Err(error.to_string()));
                        return;
                    }
                };

                let mut owner = ServerShellOwner {
                    shell,
                    postgres_transactions: BTreeMap::new(),
                };
                while let Ok(job) = receiver.recv() {
                    job(&mut owner);
                }
            })
            .map_err(|error| format!("failed to spawn server shell thread: {error}"))?;

        started_rx
            .recv()
            .map_err(|_| "server shell thread exited before startup".to_owned())??;
        Ok(Self {
            jobs,
            activity_tx,
            postgres_job_slots: Arc::new(Semaphore::new(MAX_ADMITTED_POSTGRES_OWNER_JOBS)),
            next_postgres_transaction_id: Arc::new(AtomicU64::new(1)),
        })
    }

    pub(crate) fn subscribe_activity(&self) -> watch::Receiver<u64> {
        self.activity_tx.subscribe()
    }

    pub(crate) async fn open(
        &self,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
    ) -> Result<ServerSession, String> {
        self.run(move |shell| {
            shell
                .accept_subscriber_session_with_claims_and_trust(identity, claims, trust)
                .map_err(|error| error.to_string())
        })
        .await
    }

    pub(crate) async fn publish_catalogue_schema(
        &self,
        schema: JazzSchema,
    ) -> Result<SchemaVersionId, String> {
        self.run(move |shell| {
            shell
                .publish_catalogue_schema(schema)
                .map_err(|error| error.to_string())
        })
        .await
    }

    pub(crate) async fn publish_lens(&self, lens: MigrationLens) -> Result<(), String> {
        self.run(move |shell| {
            shell
                .publish_runtime_lens(lens)
                .map_err(|error| error.to_string())
        })
        .await
    }

    pub(crate) async fn publish_permissions_schema(
        &self,
        schema: JazzSchema,
    ) -> Result<SchemaVersionId, String> {
        let activity_tx = self.activity_tx.clone();
        let result = self
            .run(move |shell| {
                shell
                    .publish_permissions_schema(schema)
                    .map_err(|error| error.to_string())
            })
            .await;
        if result.is_ok() {
            notify_shell_activity(&activity_tx);
        }
        result
    }

    pub(crate) async fn postgres_schema(&self) -> Result<JazzSchema, String> {
        self.run_cancelable(move |shell| {
            shell
                .current_runtime_schema()
                .map_err(|error| error.to_string())
        })
        .await
    }

    pub(crate) async fn postgres_begin_transaction(
        &self,
    ) -> Result<PostgresOpenTransaction, String> {
        let owner_id = self
            .next_postgres_transaction_id
            .fetch_add(1, Ordering::Relaxed);
        let mut transaction = PostgresOpenTransaction {
            shell: self.clone(),
            owner_id,
            schema_version: None,
            active: true,
        };
        let schema_version = self
            .run_owner_cancelable(move |owner| {
                let schema = owner
                    .shell
                    .current_runtime_schema()
                    .map_err(|error| error.to_string())?;
                let open_tx_id = owner
                    .shell
                    .begin_exclusive_as_system()
                    .map_err(|error| error.to_string())?;
                owner.postgres_transactions.insert(
                    owner_id,
                    PostgresOwnedTransaction {
                        open_tx_id,
                        schema_version: schema.version_id(),
                        staged_rows: 0,
                        staged_cell_bytes: 0,
                        inserted_rows: BTreeSet::new(),
                    },
                );
                Ok(schema.version_id())
            })
            .await?;
        transaction.schema_version = Some(schema_version);
        Ok(transaction)
    }

    pub(crate) async fn postgres_query(
        &self,
        query: Query,
        expected_schema_version: SchemaVersionId,
        columns: Vec<String>,
        bindings: BTreeMap<String, Value>,
        database_job_permit: OwnedSemaphorePermit,
        response_permit: OwnedSemaphorePermit,
        query_guard: ActivePostgresQueryGuard,
        max_response_bytes: usize,
    ) -> Result<PostgresQueryResult, String> {
        self.run_cancelable(move |shell| {
            let _database_job_permit = database_job_permit;
            let _query_guard = query_guard;
            let schema = shell
                .current_runtime_schema()
                .map_err(|error| error.to_string())?;
            if schema.version_id() != expected_schema_version {
                return Err("PostgreSQL schema changed while planning the query; retry".to_owned());
            }
            let current_rows = shell
                .read_as_system(&query, bindings)
                .map_err(|error| error.to_string())?;
            build_postgres_query_result(
                shell,
                &schema,
                &query.table,
                columns,
                current_rows,
                None,
                response_permit,
                max_response_bytes,
            )
        })
        .await
    }

    pub(crate) async fn postgres_query_in_transaction(
        &self,
        transaction: &PostgresOpenTransaction,
        query: Query,
        expected_schema_version: SchemaVersionId,
        columns: Vec<String>,
        bindings: BTreeMap<String, Value>,
        database_job_permit: OwnedSemaphorePermit,
        response_permit: OwnedSemaphorePermit,
        query_guard: ActivePostgresQueryGuard,
        max_response_bytes: usize,
    ) -> Result<PostgresQueryResult, String> {
        let owner_id = transaction.owner_id;
        self.run_owner_cancelable(move |owner| {
            let _database_job_permit = database_job_permit;
            let _query_guard = query_guard;
            let (open_tx_id, pinned_schema_version) = owner
                .postgres_transactions
                .get(&owner_id)
                .map(|transaction| (transaction.open_tx_id, transaction.schema_version))
                .ok_or_else(|| "PostgreSQL transaction is no longer open".to_owned())?;
            let schema = owner
                .shell
                .current_runtime_schema()
                .map_err(|error| error.to_string())?;
            if schema.version_id() != pinned_schema_version
                || expected_schema_version != pinned_schema_version
            {
                return Err("PostgreSQL schema changed during the transaction; retry".to_owned());
            }
            let current_rows = owner
                .shell
                .read_exclusive_as_system(open_tx_id, &query, bindings)
                .map_err(|error| error.to_string())?;
            build_postgres_query_result(
                &owner.shell,
                &schema,
                &query.table,
                columns,
                current_rows,
                Some(open_tx_id),
                response_permit,
                max_response_bytes,
            )
        })
        .await
    }

    pub(crate) async fn postgres_mutate(
        &self,
        table_name: String,
        expected_schema_version: SchemaVersionId,
        mutation: PostgresMutation,
        database_job_permit: OwnedSemaphorePermit,
        query_guard: ActivePostgresQueryGuard,
    ) -> Result<PostgresMutationResult, String> {
        let activity_tx = self.activity_tx.clone();
        let result = self
            .run_cancelable(move |shell| {
                let _database_job_permit = database_job_permit;
                let _query_guard = query_guard;
                let schema = shell
                    .current_runtime_schema()
                    .map_err(|error| error.to_string())?;
                if schema.version_id() != expected_schema_version {
                    return Err(
                        "PostgreSQL schema changed while planning the mutation; retry".to_owned(),
                    );
                }
                if !schema.tables.iter().any(|table| table.name == table_name) {
                    return Err(format!("unknown table {table_name}"));
                }
                let open_tx_id = shell
                    .begin_exclusive_as_system()
                    .map_err(|error| error.to_string())?;
                let mut transaction = PostgresOwnedTransaction {
                    open_tx_id,
                    schema_version: expected_schema_version,
                    staged_rows: 0,
                    staged_cell_bytes: 0,
                    inserted_rows: BTreeSet::new(),
                };
                let result =
                    stage_postgres_mutation(shell, &mut transaction, &table_name, mutation);
                let result = match result {
                    Ok(result) => result,
                    Err(error) => {
                        let _ = shell.rollback_exclusive_as_system(open_tx_id);
                        return Err(error);
                    }
                };
                if transaction.staged_rows == 0 {
                    shell
                        .rollback_exclusive_as_system(open_tx_id)
                        .map_err(|error| error.to_string())?;
                } else {
                    shell
                        .commit_exclusive_settled_as_system(open_tx_id)
                        .map_err(|error| error.to_string())?;
                }
                Ok(result)
            })
            .await;
        if result.is_ok() {
            notify_shell_activity(&activity_tx);
        }
        result
    }

    pub(crate) async fn postgres_mutate_in_transaction(
        &self,
        transaction: &PostgresOpenTransaction,
        table_name: String,
        expected_schema_version: SchemaVersionId,
        mutation: PostgresMutation,
        database_job_permit: OwnedSemaphorePermit,
        query_guard: ActivePostgresQueryGuard,
    ) -> Result<PostgresMutationResult, String> {
        let owner_id = transaction.owner_id;
        self.run_owner_cancelable(move |owner| {
            let _database_job_permit = database_job_permit;
            let _query_guard = query_guard;
            let schema = owner
                .shell
                .current_runtime_schema()
                .map_err(|error| error.to_string())?;
            let ServerShellOwner {
                shell,
                postgres_transactions,
            } = owner;
            let transaction = postgres_transactions
                .get_mut(&owner_id)
                .ok_or_else(|| "PostgreSQL transaction is no longer open".to_owned())?;
            if schema.version_id() != transaction.schema_version
                || expected_schema_version != transaction.schema_version
            {
                return Err("PostgreSQL schema changed during the transaction; retry".to_owned());
            }
            if !schema.tables.iter().any(|table| table.name == table_name) {
                return Err(format!("unknown table {table_name}"));
            }
            stage_postgres_mutation(shell, transaction, &table_name, mutation)
        })
        .await
    }

    pub(crate) async fn receive_tick_take(
        &self,
        session: ServerSession,
        frames: Vec<AbiBytes>,
    ) -> Result<Vec<AbiBytes>, String> {
        let activity_tx = self.activity_tx.clone();
        self.run(move |shell| {
            let result = shell
                .receive_frames(session, frames)
                .and_then(|()| shell.tick())
                .and_then(|()| shell.take_frames(session))
                .map_err(|error| error.to_string());
            if result.is_ok() {
                notify_shell_activity(&activity_tx);
            }
            result
        })
        .await
    }

    pub(crate) async fn tick_take(&self, session: ServerSession) -> Result<Vec<AbiBytes>, String> {
        let activity_tx = self.activity_tx.clone();
        self.run(move |shell| {
            let result = shell
                .tick()
                .and_then(|()| shell.take_frames(session))
                .map_err(|error| error.to_string());
            // Progress-based re-arm: a tick that yielded frames may have more
            // behind it (large resets span many ticks), so schedule another.
            // Empty ticks do NOT re-arm — that unconditional re-arm was the
            // consolidation-spin feeder. One notification must never buy an
            // unbounded loop, and delivery must never stall mid-reset; frames
            // produced is exactly the signal that separates the two.
            if let Ok(frames) = &result
                && !frames.is_empty()
            {
                notify_shell_activity(&activity_tx);
            }
            result
        })
        .await
    }

    pub(crate) async fn connect_upstream(
        &self,
        transport: Box<dyn Transport + Send>,
    ) -> Result<(), String> {
        let activity_tx = self.activity_tx.clone();
        self.run(move |shell| {
            let result = shell
                .connect_upstream(transport)
                .map_err(|error| error.to_string());
            if result.is_ok() {
                notify_shell_activity(&activity_tx);
            }
            result
        })
        .await
    }

    pub(crate) fn notify_activity(&self) {
        notify_shell_activity(&self.activity_tx);
    }

    async fn finish_postgres_transaction(&self, owner_id: u64, commit: bool) -> Result<(), String> {
        self.run_owner_cancelable(move |owner| {
            let transaction = owner
                .postgres_transactions
                .remove(&owner_id)
                .ok_or_else(|| "PostgreSQL transaction is no longer open".to_owned())?;
            if !commit || transaction.staged_rows == 0 {
                return owner
                    .shell
                    .rollback_exclusive_as_system(transaction.open_tx_id)
                    .map_err(|error| error.to_string());
            }
            let schema = match owner.shell.current_runtime_schema() {
                Ok(schema) => schema,
                Err(error) => {
                    let _ = owner
                        .shell
                        .rollback_exclusive_as_system(transaction.open_tx_id);
                    return Err(error.to_string());
                }
            };
            if schema.version_id() != transaction.schema_version {
                let _ = owner
                    .shell
                    .rollback_exclusive_as_system(transaction.open_tx_id);
                return Err("PostgreSQL schema changed during the transaction; retry".to_owned());
            }
            owner
                .shell
                .commit_exclusive_settled_as_system(transaction.open_tx_id)
                .map_err(|error| error.to_string())
        })
        .await
    }

    fn queue_postgres_transaction_rollback(&self, owner_id: u64) {
        let _ = self.jobs.send(Box::new(move |owner| {
            let Some(transaction) = owner.postgres_transactions.remove(&owner_id) else {
                return;
            };
            let _ = owner
                .shell
                .rollback_exclusive_as_system(transaction.open_tx_id);
        }));
    }

    pub(crate) fn close(&self, session: ServerSession) {
        let _ = self.jobs.send(Box::new(move |owner| {
            let _ = owner.shell.close_session(session);
        }));
    }

    async fn run<T>(
        &self,
        run_on_shell: impl FnOnce(&mut InMemoryServerShell) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String>
    where
        T: Send + 'static,
    {
        self.run_owner_inner(move |owner| run_on_shell(&mut owner.shell), false)
            .await
    }

    async fn run_cancelable<T>(
        &self,
        run_on_shell: impl FnOnce(&mut InMemoryServerShell) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String>
    where
        T: Send + 'static,
    {
        self.run_owner_cancelable(move |owner| run_on_shell(&mut owner.shell))
            .await
    }

    async fn run_owner_cancelable<T>(
        &self,
        run_on_owner: impl FnOnce(&mut ServerShellOwner) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String>
    where
        T: Send + 'static,
    {
        // The owner thread uses an unbounded std channel for ordinary server
        // work. Bound PostgreSQL admission before enqueueing and move the
        // permit into the queued closure. If the client disconnects or sends
        // CancelRequest, its future can disappear without freeing a slot for
        // another closure until the abandoned job is actually skipped.
        let job_permit = self
            .postgres_job_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| "PostgreSQL owner-job admission is closed".to_owned())?;
        self.run_owner_inner(
            move |owner| {
                let _job_permit = job_permit;
                run_on_owner(owner)
            },
            true,
        )
        .await
    }

    async fn run_owner_inner<T>(
        &self,
        run_on_owner: impl FnOnce(&mut ServerShellOwner) -> Result<T, String> + Send + 'static,
        skip_if_response_closed: bool,
    ) -> Result<T, String>
    where
        T: Send + 'static,
    {
        let (reply, response) = oneshot::channel();
        self.jobs
            .send(Box::new(move |owner| {
                if skip_if_response_closed && reply.is_closed() {
                    return;
                }
                let _ = reply.send(run_on_owner(owner));
            }))
            .map_err(|_| "server shell thread is not running".to_owned())?;
        response
            .await
            .map_err(|_| "server shell thread dropped response".to_owned())?
    }
}

fn stage_postgres_mutation(
    shell: &mut InMemoryServerShell,
    transaction: &mut PostgresOwnedTransaction,
    table_name: &str,
    mutation: PostgresMutation,
) -> Result<PostgresMutationResult, String> {
    match mutation {
        PostgresMutation::Insert { rows } => {
            for (row, _) in &rows {
                if transaction
                    .inserted_rows
                    .contains(&(table_name.to_owned(), *row))
                {
                    return Err(format!(
                        "encoding error: object already exists in transaction: {}",
                        row.0
                    ));
                }
            }
            let cell_bytes = rows.iter().try_fold(0_usize, |total, (_, cells)| {
                postgres_cells_size(cells).and_then(|bytes| total.checked_add(bytes))
            });
            reserve_postgres_transaction_capacity(transaction, rows.len(), cell_bytes)?;
            let inserted = shell
                .insert_many_exclusive_as_system(transaction.open_tx_id, table_name, rows)
                .map_err(|error| error.to_string())?;
            transaction.staged_rows += inserted.len();
            transaction.staged_cell_bytes += cell_bytes.expect("capacity check rejected overflow");
            transaction.inserted_rows.extend(
                inserted
                    .iter()
                    .copied()
                    .map(|row| (table_name.to_owned(), row)),
            );
            Ok(PostgresMutationResult {
                affected_rows: inserted.len(),
                rows: inserted,
            })
        }
        PostgresMutation::Update {
            query,
            bindings,
            patch,
        } => {
            let current_rows = shell
                .read_exclusive_as_system(transaction.open_tx_id, &query, bindings)
                .map_err(|error| error.to_string())?;
            let rows = current_rows
                .into_iter()
                .map(|row| row.row_uuid())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let patch_bytes = postgres_cells_size(&patch);
            let cell_bytes = patch_bytes.and_then(|bytes| bytes.checked_mul(rows.len()));
            reserve_postgres_transaction_capacity(transaction, rows.len(), cell_bytes)?;
            for row in &rows {
                let updated = shell
                    .update_exclusive_as_system(
                        transaction.open_tx_id,
                        table_name,
                        *row,
                        patch.clone(),
                    )
                    .map_err(|error| error.to_string())?;
                if !updated {
                    return Err(format!(
                        "row {} disappeared from the PostgreSQL transaction snapshot",
                        row.0
                    ));
                }
            }
            transaction.staged_rows += rows.len();
            transaction.staged_cell_bytes += cell_bytes.expect("capacity check rejected overflow");
            Ok(PostgresMutationResult {
                affected_rows: rows.len(),
                rows,
            })
        }
        PostgresMutation::Delete { row } => {
            reserve_postgres_transaction_capacity(transaction, 1, Some(0))?;
            let deleted = shell
                .delete_exclusive_as_system(transaction.open_tx_id, table_name, row)
                .map_err(|error| error.to_string())?;
            if deleted {
                transaction.staged_rows += 1;
            }
            Ok(PostgresMutationResult {
                affected_rows: usize::from(deleted),
                rows: deleted.then_some(row).into_iter().collect(),
            })
        }
    }
}

fn reserve_postgres_transaction_capacity(
    transaction: &PostgresOwnedTransaction,
    additional_rows: usize,
    additional_cell_bytes: Option<usize>,
) -> Result<(), String> {
    if transaction
        .staged_rows
        .checked_add(additional_rows)
        .is_none_or(|rows| rows > MAX_POSTGRES_TRANSACTION_ROWS)
    {
        return Err(format!(
            "PostgreSQL transaction cannot affect more than {MAX_POSTGRES_TRANSACTION_ROWS} rows"
        ));
    }
    if additional_cell_bytes
        .and_then(|bytes| transaction.staged_cell_bytes.checked_add(bytes))
        .is_none_or(|bytes| bytes > MAX_POSTGRES_TRANSACTION_CELL_BYTES)
    {
        return Err(format!(
            "PostgreSQL transaction cell payload exceeds {MAX_POSTGRES_TRANSACTION_CELL_BYTES} bytes"
        ));
    }
    Ok(())
}

fn postgres_cells_size(cells: &BTreeMap<String, Value>) -> Option<usize> {
    cells.values().try_fold(0_usize, |total, value| {
        total.checked_add(postgres_value_size(value))
    })
}

fn build_postgres_query_result(
    shell: &InMemoryServerShell,
    schema: &JazzSchema,
    table_name: &str,
    columns: Vec<String>,
    current_rows: Vec<crate::node::CurrentRow>,
    exclusive_tx_id: Option<OpenTxId>,
    response_permit: OwnedSemaphorePermit,
    max_response_bytes: usize,
) -> Result<PostgresQueryResult, String> {
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == table_name)
        .cloned()
        .ok_or_else(|| format!("unknown table {table_name}"))?;
    let mut rows = Vec::with_capacity(current_rows.len());
    let mut response_bytes = 0_usize;
    for row in current_rows {
        let row_container_bytes = std::mem::size_of::<PostgresRow>().saturating_add(
            columns
                .len()
                .saturating_mul(std::mem::size_of::<Option<Value>>()),
        );
        response_bytes = response_bytes
            .checked_add(row_container_bytes)
            .ok_or_else(|| "PostgreSQL response exceeds configured byte limit".to_owned())?;
        if response_bytes > max_response_bytes {
            return Err(format!(
                "PostgreSQL response exceeds configured {max_response_bytes}-byte limit"
            ));
        }
        let mut values = Vec::with_capacity(columns.len());
        for column_name in &columns {
            if column_name == "id" {
                values.push(Some(Value::Uuid(row.row_uuid().0)));
                continue;
            }

            let column = table
                .columns
                .iter()
                .find(|column| column.name == *column_name)
                .ok_or_else(|| format!("unknown column {}.{}", table.name, column_name))?;
            let mut value = row.cell(&table, column_name);
            if column.large_value.is_some() {
                let resolved = match exclusive_tx_id {
                    Some(open_tx_id) => shell
                        .resolve_exclusive_large_value_as_system(
                            open_tx_id,
                            &table.name,
                            row.row_uuid(),
                            column_name,
                        )
                        .map_err(|error| error.to_string())?,
                    None => match value.take() {
                        Some(Value::Bytes(handle)) => {
                            Some(OpenTxLargeValueCell::SnapshotHandle(handle))
                        }
                        None => None,
                        Some(_) => {
                            return Err(format!(
                                "large-value column {}.{} has an invalid value",
                                table.name, column_name
                            ));
                        }
                    },
                };
                let Some(resolved) = resolved else {
                    values.push(None);
                    continue;
                };
                let handle = match resolved {
                    OpenTxLargeValueCell::Authored(bytes) => {
                        value = Some(Value::Bytes(bytes));
                        if let Some(value) = &value {
                            response_bytes = response_bytes
                                .checked_add(postgres_value_size(value))
                                .ok_or_else(|| {
                                    "PostgreSQL response exceeds configured byte limit".to_owned()
                                })?;
                        }
                        if response_bytes > max_response_bytes {
                            return Err(format!(
                                "PostgreSQL response exceeds configured {max_response_bytes}-byte limit"
                            ));
                        }
                        values.push(value);
                        continue;
                    }
                    OpenTxLargeValueCell::SnapshotHandle(handle) => handle,
                };
                let declared_len = crate::node::large_value_handle_len(&handle)
                    .map_err(|error| error.to_string())?;
                let declared_len = usize::try_from(declared_len).unwrap_or(usize::MAX);
                if response_bytes.saturating_add(declared_len) > max_response_bytes {
                    return Err(format!(
                        "PostgreSQL response exceeds configured {max_response_bytes}-byte limit"
                    ));
                }
                value = Some(Value::Bytes(
                    shell
                        .hydrate_large_value_handle(&handle)
                        .map_err(|error| error.to_string())?,
                ));
            }
            if let Some(value) = &value {
                response_bytes = response_bytes
                    .checked_add(postgres_value_size(value))
                    .ok_or_else(|| {
                        "PostgreSQL response exceeds configured byte limit".to_owned()
                    })?;
                if response_bytes > max_response_bytes {
                    return Err(format!(
                        "PostgreSQL response exceeds configured {max_response_bytes}-byte limit"
                    ));
                }
            }
            values.push(value);
        }
        rows.push(PostgresRow { values });
    }
    Ok(PostgresQueryResult {
        table,
        rows,
        response_permit,
    })
}

fn postgres_value_size(value: &Value) -> usize {
    match value {
        Value::U8(_) | Value::Bool(_) | Value::Enum(_) => 1,
        Value::U16(_) => 2,
        Value::U32(_) | Value::I32(_) => 4,
        Value::U64(_) | Value::I64(_) | Value::F64(_) => 8,
        Value::Uuid(_) => 16,
        Value::String(value) => value.len(),
        Value::Bytes(value) => value.len(),
        Value::Tuple(values) | Value::Array(values) => values
            .len()
            .saturating_mul(std::mem::size_of::<Value>())
            .saturating_add(values.iter().fold(0_usize, |total, value| {
                total.saturating_add(postgres_value_size(value))
            })),
        Value::Nullable(Some(value)) => {
            std::mem::size_of::<Value>().saturating_add(postgres_value_size(value))
        }
        Value::Nullable(None) => 0,
    }
}

fn notify_shell_activity(activity_tx: &watch::Sender<u64>) {
    activity_tx.send_modify(|version| {
        *version = version.wrapping_add(1);
    });
}
