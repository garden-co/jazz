use std::collections::BTreeMap;
use std::sync::{Arc, mpsc};
use std::thread;

use crate::db::{CommitUnitTrust, DbIdentity, Transport};
use crate::groove::records::Value;
use crate::ids::{AuthorId, NodeUuid, RowUuid, SchemaVersionId};
use crate::node::EdgeCacheBudget;
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
}

type ServerShellJob = Box<dyn FnOnce(&mut InMemoryServerShell) + Send + 'static>;

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
        row: RowUuid,
        cells: BTreeMap<String, Value>,
    },
    Update {
        row: RowUuid,
        patch: BTreeMap<String, Value>,
    },
    Delete {
        row: RowUuid,
    },
}

pub(crate) struct PostgresMutationResult {
    pub(crate) affected_rows: usize,
    pub(crate) row: Option<RowUuid>,
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

                let mut shell = shell;
                while let Ok(job) = receiver.recv() {
                    job(&mut shell);
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
            let table = schema
                .tables
                .iter()
                .find(|table| table.name == query.table)
                .cloned()
                .ok_or_else(|| format!("unknown table {}", query.table))?;
            let current_rows = shell
                .read_as_system(&query, bindings)
                .map_err(|error| error.to_string())?;
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
                    .ok_or_else(|| {
                        "PostgreSQL response exceeds configured byte limit".to_owned()
                    })?;
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
                    if column.large_value.is_some()
                        && let Some(Value::Bytes(handle)) = value.as_ref()
                    {
                        let declared_len = crate::node::large_value_handle_len(handle)
                            .map_err(|error| error.to_string())?;
                        let declared_len = usize::try_from(declared_len).unwrap_or(usize::MAX);
                        if response_bytes.saturating_add(declared_len) > max_response_bytes {
                            return Err(format!(
                                "PostgreSQL response exceeds configured {max_response_bytes}-byte limit"
                            ));
                        }
                        value = Some(Value::Bytes(
                            shell
                                .hydrate_large_value_handle(handle)
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
                match mutation {
                    PostgresMutation::Insert { row, cells } => {
                        let row = shell
                            .insert_settled_as_system(&table_name, row, cells)
                            .map_err(|error| error.to_string())?;
                        Ok(PostgresMutationResult {
                            affected_rows: 1,
                            row: Some(row),
                        })
                    }
                    PostgresMutation::Update { row, patch } => {
                        let updated = shell
                            .update_settled_as_system(&table_name, row, patch)
                            .map_err(|error| error.to_string())?;
                        Ok(PostgresMutationResult {
                            affected_rows: usize::from(updated),
                            row: updated.then_some(row),
                        })
                    }
                    PostgresMutation::Delete { row } => {
                        let deleted = shell
                            .delete_settled_as_system(&table_name, row)
                            .map_err(|error| error.to_string())?;
                        Ok(PostgresMutationResult {
                            affected_rows: usize::from(deleted),
                            row: deleted.then_some(row),
                        })
                    }
                }
            })
            .await;
        if result.is_ok() {
            notify_shell_activity(&activity_tx);
        }
        result
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

    pub(crate) fn close(&self, session: ServerSession) {
        let _ = self.jobs.send(Box::new(move |shell| {
            let _ = shell.close_session(session);
        }));
    }

    async fn run<T>(
        &self,
        run_on_shell: impl FnOnce(&mut InMemoryServerShell) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String>
    where
        T: Send + 'static,
    {
        self.run_inner(run_on_shell, false).await
    }

    async fn run_cancelable<T>(
        &self,
        run_on_shell: impl FnOnce(&mut InMemoryServerShell) -> Result<T, String> + Send + 'static,
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
        self.run_inner(
            move |shell| {
                let _job_permit = job_permit;
                run_on_shell(shell)
            },
            true,
        )
        .await
    }

    async fn run_inner<T>(
        &self,
        run_on_shell: impl FnOnce(&mut InMemoryServerShell) -> Result<T, String> + Send + 'static,
        skip_if_response_closed: bool,
    ) -> Result<T, String>
    where
        T: Send + 'static,
    {
        let (reply, response) = oneshot::channel();
        self.jobs
            .send(Box::new(move |shell| {
                if skip_if_response_closed && reply.is_closed() {
                    return;
                }
                let _ = reply.send(run_on_shell(shell));
            }))
            .map_err(|_| "server shell thread is not running".to_owned())?;
        response
            .await
            .map_err(|_| "server shell thread dropped response".to_owned())?
    }
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
