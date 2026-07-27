use std::collections::BTreeMap;
use std::sync::mpsc;
use std::thread;

use jazz::db::{CommitUnitTrust, DbIdentity, Transport};
use jazz::groove::records::Value;
use jazz::ids::{AuthorId, NodeUuid, SchemaVersionId};
use jazz::node::EdgeCacheBudget;
use jazz::schema::JazzSchema;
use jazz_server::{
    AbiBytes, InMemoryServerShell, InMemoryServerShellConfig, NodeRole, ServerSession,
    StorageConfig,
};
use tokio::sync::{oneshot, watch};

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
}

type ServerShellJob = Box<dyn FnOnce(&mut InMemoryServerShell) + Send + 'static>;

impl ServerShellHandle {
    pub(crate) fn start_with_storage(
        schema: JazzSchema,
        storage_config: StorageConfig,
    ) -> Result<Self, String> {
        Self::start_with_storage_config(schema, storage_config, NodeRole::Core, None)
    }

    pub(crate) fn start_with_storage_config(
        schema: JazzSchema,
        storage_config: StorageConfig,
        role: NodeRole,
        edge_cache_budget: Option<EdgeCacheBudget>,
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
                    Ok(shell) => {
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
        Ok(Self { jobs, activity_tx })
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

    pub(crate) async fn publish_schema(
        &self,
        schema: JazzSchema,
    ) -> Result<SchemaVersionId, String> {
        self.run(move |shell| {
            shell
                .publish_runtime_schema(schema)
                .map_err(|error| error.to_string())
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
        let (reply, response) = oneshot::channel();
        self.jobs
            .send(Box::new(move |shell| {
                let _ = reply.send(run_on_shell(shell));
            }))
            .map_err(|_| "server shell thread is not running".to_owned())?;
        response
            .await
            .map_err(|_| "server shell thread dropped response".to_owned())?
    }
}

fn notify_shell_activity(activity_tx: &watch::Sender<u64>) {
    activity_tx.send_modify(|version| {
        *version = version.wrapping_add(1);
    });
}
