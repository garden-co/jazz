use std::collections::BTreeMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::db::{CommitUnitTrust, ConnectionSessionContext, DbIdentity, Transport};
use crate::groove::records::Value;
use crate::groove::storage::StorageFactory;
use crate::ids::{AuthorId, NodeUuid, SchemaVersionId};
use crate::node::EdgeCacheBudget;
use crate::protocol::{MigrationLens, SyncMessage};
use crate::schema::JazzSchema;
use crate::serving::{
    AbiBytes, InMemoryServerShell, InMemoryServerShellConfig, NodeRole, ServerSession,
    StorageConfig,
};
use crate::wire::{WireFrame, decode_frame, decode_sync_message};
use tokio::sync::{mpsc as tokio_mpsc, oneshot, watch};

/// Sendable handle for the thread that owns the in-memory server shell.
///
/// The underlying `InMemoryServerShell` is intentionally kept on one OS thread
/// because it currently stores its DB, sessions, and transports behind
/// `Rc<RefCell<...>>`. Axum request/websocket tasks can clone this handle, but
/// all shell access is serialized onto that owner thread. Shutdown explicitly
/// joins the owner before its surrounding server storage lifecycle completes.
/// This ensures native RocksDB resources are destroyed on their owner thread.
#[derive(Clone)]
pub struct ServerRuntimeHandle {
    inner: Arc<ServerShellInner>,
}

/// Opaque activity subscription for a server runtime.
pub struct ServerRuntimeActivity {
    receiver: watch::Receiver<u64>,
}

impl ServerRuntimeActivity {
    /// Wait until runtime work may have produced outbound frames.
    pub async fn changed(&mut self) -> Result<(), String> {
        self.receiver
            .changed()
            .await
            .map_err(|_| "server runtime activity channel closed".to_owned())
    }
}

/// Opaque stream of per-tick outbound frame batches.
pub struct ServerRuntimeFrameStream {
    receiver: tokio_mpsc::UnboundedReceiver<Result<Vec<AbiBytes>, String>>,
}

impl ServerRuntimeFrameStream {
    /// Receive the next completed tick's outbound frames.
    pub async fn recv(&mut self) -> Option<Result<Vec<AbiBytes>, String>> {
        self.receiver.recv().await
    }
}

struct ServerShellInner {
    jobs: Mutex<Option<mpsc::Sender<ServerShellCommand>>>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
    activity_tx: watch::Sender<u64>,
}

impl Drop for ServerShellInner {
    fn drop(&mut self) {
        // `BuiltServer::shutdown` is the public, async lifecycle boundary and
        // drains request work before reaching this point. This fallback covers
        // direct builder use that is simply dropped: without it, dropping the
        // last sender merely asks the owner thread to exit and a caller can
        // race a reopen of the same RocksDB path before that exit completes.
        // There can be no remaining `ServerRuntimeHandle` when this runs, so no
        // public operation can be waiting for an owner-thread reply.
        let _ = shutdown_blocking(self);
    }
}

type ServerShellJob = Box<dyn FnOnce(&mut InMemoryServerShell) + Send + 'static>;

enum ServerShellCommand {
    Run(ServerShellJob),
    Shutdown(mpsc::Sender<()>),
}

impl ServerRuntimeHandle {
    /// Reopen an already bootstrapped dynamic edge. A blank store returns
    /// `None` so its owner can run the authenticated bootstrap exchange.
    pub fn try_start_dynamic_edge_from_storage(
        storage_config: StorageConfig,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        edge_cache_budget: Option<EdgeCacheBudget>,
    ) -> Result<Option<Self>, String> {
        let (jobs, receiver) = mpsc::channel::<ServerShellCommand>();
        let (started_tx, started_rx) = mpsc::channel();
        let (activity_tx, _) = watch::channel(0_u64);
        let join = thread::Builder::new()
            .name("jazz-server-shell".to_owned())
            .spawn(move || {
                let shell = match InMemoryServerShell::try_start_dynamic_edge_from_storage(
                    DbIdentity {
                        node: NodeUuid::from_bytes([0x5e; 16]),
                        author: AuthorId::SYSTEM,
                    },
                    storage_config,
                    storage_factory,
                    edge_cache_budget,
                ) {
                    Ok(Some(shell)) => {
                        let _ = started_tx.send(Ok(true));
                        shell
                    }
                    Ok(None) => {
                        let _ = started_tx.send(Ok(false));
                        return;
                    }
                    Err(error) => {
                        let _ = started_tx.send(Err(error.to_string()));
                        return;
                    }
                };
                let mut shell = shell;
                while let Ok(command) = receiver.recv() {
                    match command {
                        ServerShellCommand::Run(job) => job(&mut shell),
                        ServerShellCommand::Shutdown(stopped) => {
                            drop(shell);
                            let _ = stopped.send(());
                            return;
                        }
                    }
                }
            })
            .map_err(|error| format!("failed to spawn server shell thread: {error}"))?;
        let started = started_rx
            .recv()
            .map_err(|_| "server shell thread exited before dynamic reopen".to_owned())??;
        Ok(started.then_some(Self {
            inner: Arc::new(ServerShellInner {
                jobs: Mutex::new(Some(jobs)),
                join: Mutex::new(Some(join)),
                activity_tx,
            }),
        }))
    }

    /// Construct a ready edge shell only after an authenticated bootstrap
    /// snapshot has been durably adopted. The owner thread is not published to
    /// downstream routes until this returns successfully.
    pub fn start_dynamic_edge_with_catalogue_snapshot(
        storage_config: StorageConfig,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        edge_cache_budget: Option<EdgeCacheBudget>,
        snapshot: crate::protocol::CatalogueSnapshot,
    ) -> Result<Self, String> {
        let (jobs, receiver) = mpsc::channel::<ServerShellCommand>();
        let (started_tx, started_rx) = mpsc::channel();
        let (activity_tx, _) = watch::channel(0_u64);

        let join = thread::Builder::new()
            .name("jazz-server-shell".to_owned())
            .spawn(move || {
                let shell = match InMemoryServerShell::start_dynamic_edge_with_catalogue_snapshot(
                    DbIdentity {
                        node: NodeUuid::from_bytes([0x5e; 16]),
                        author: AuthorId::SYSTEM,
                    },
                    storage_config,
                    storage_factory,
                    edge_cache_budget,
                    snapshot,
                ) {
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
                while let Ok(command) = receiver.recv() {
                    match command {
                        ServerShellCommand::Run(job) => job(&mut shell),
                        ServerShellCommand::Shutdown(stopped) => {
                            drop(shell);
                            let _ = stopped.send(());
                            return;
                        }
                    }
                }
            })
            .map_err(|error| format!("failed to spawn server shell thread: {error}"))?;

        started_rx
            .recv()
            .map_err(|_| "server shell thread exited before dynamic bootstrap".to_owned())??;
        Ok(Self {
            inner: Arc::new(ServerShellInner {
                jobs: Mutex::new(Some(jobs)),
                join: Mutex::new(Some(join)),
                activity_tx,
            }),
        })
    }

    /// Encode the trusted catalogue through the negotiated wire format.
    pub async fn encoded_trusted_catalogue_snapshot(
        &self,
        protocol_version: u16,
        features: crate::wire::WireFeatures,
    ) -> Result<Vec<AbiBytes>, String> {
        self.run(move |shell| {
            shell
                .encoded_trusted_catalogue_snapshot(protocol_version, features)
                .map_err(|error| error.to_string())
        })
        .await
    }

    /// Replace an already persisted edge's authority catalogue through the
    /// same authenticated snapshot path used at first bootstrap. The snapshot
    /// adoption rebuilds the local physical projection registry before this
    /// call returns, so callers may safely make the edge externally ready.
    pub async fn apply_trusted_catalogue_snapshot(
        &self,
        snapshot: crate::protocol::CatalogueSnapshot,
    ) -> Result<(), String> {
        self.run(move |shell| {
            shell
                .apply_trusted_catalogue_snapshot(snapshot)
                .map_err(|error| error.to_string())
        })
        .await
    }

    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub async fn set_catalogue_activation_failpoint(
        &self,
        failpoint: crate::node::CatalogueActivationFailpoint,
    ) -> Result<(), String> {
        self.run(move |shell| {
            shell.set_catalogue_activation_failpoint(failpoint);
            Ok(())
        })
        .await
    }

    #[doc(hidden)]
    pub async fn trusted_catalogue_snapshot_for_test(
        &self,
    ) -> Result<crate::protocol::CatalogueSnapshot, String> {
        self.run(move |shell| {
            shell
                .trusted_catalogue_snapshot()
                .map_err(|error| error.to_string())
        })
        .await
    }
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub async fn runtime_catalogue_contains(
        &self,
        schema: SchemaVersionId,
        lens: crate::ids::MigrationLensId,
    ) -> Result<(bool, bool), String> {
        self.run(move |shell| Ok(shell.runtime_catalogue_contains(schema, lens)))
            .await
    }

    /// Start a core runtime over the selected storage configuration.
    pub fn start_with_storage(
        schema: JazzSchema,
        storage_config: StorageConfig,
        storage_factory: Option<Arc<dyn StorageFactory>>,
    ) -> Result<Self, String> {
        Self::start_with_storage_config_and_permissions(
            schema,
            storage_config,
            storage_factory,
            NodeRole::Core,
            None,
            false,
        )
    }

    /// Start a runtime with an explicit role and optional Edge cache budget.
    pub fn start_with_storage_config(
        schema: JazzSchema,
        storage_config: StorageConfig,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        role: NodeRole,
        edge_cache_budget: Option<EdgeCacheBudget>,
    ) -> Result<Self, String> {
        Self::start_with_storage_config_and_permissions(
            schema,
            storage_config,
            storage_factory,
            role,
            edge_cache_budget,
            true,
        )
    }

    fn start_with_storage_config_and_permissions(
        schema: JazzSchema,
        storage_config: StorageConfig,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        role: NodeRole,
        edge_cache_budget: Option<EdgeCacheBudget>,
        permissions_ready: bool,
    ) -> Result<Self, String> {
        let (jobs, receiver) = mpsc::channel::<ServerShellCommand>();
        let (started_tx, started_rx) = mpsc::channel();
        let (activity_tx, _) = watch::channel(0_u64);

        let join = thread::Builder::new()
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
                let config = match storage_factory {
                    Some(factory) => config.with_storage_factory(factory),
                    None => config,
                };
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
                while let Ok(command) = receiver.recv() {
                    match command {
                        ServerShellCommand::Run(job) => job(&mut shell),
                        ServerShellCommand::Shutdown(stopped) => {
                            // The shell owns the native storage, so complete
                            // its destruction before acknowledging shutdown.
                            drop(shell);
                            let _ = stopped.send(());
                            return;
                        }
                    }
                }
            })
            .map_err(|error| format!("failed to spawn server shell thread: {error}"))?;

        started_rx
            .recv()
            .map_err(|_| "server shell thread exited before startup".to_owned())??;
        Ok(Self {
            inner: Arc::new(ServerShellInner {
                jobs: Mutex::new(Some(jobs)),
                join: Mutex::new(Some(join)),
                activity_tx,
            }),
        })
    }

    /// Subscribe to runtime activity that may make outbound session frames available.
    pub fn subscribe_activity(&self) -> ServerRuntimeActivity {
        ServerRuntimeActivity {
            receiver: self.inner.activity_tx.subscribe(),
        }
    }

    /// Admit an authenticated session into the semantic runtime.
    pub async fn open_with_session_context(
        &self,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
        negotiated_features: crate::wire::WireFeatures,
        session_context: Option<ConnectionSessionContext>,
    ) -> Result<ServerSession, String> {
        self.run(move |shell| {
            shell
                .accept_subscriber_session_with_claims_and_trust_and_context(
                    identity,
                    claims,
                    trust,
                    negotiated_features,
                    session_context,
                )
                .map_err(|error| error.to_string())
        })
        .await
    }

    /// Publish a validated schema and optional migration lens to the runtime.
    pub async fn publish_schema_with_lens(
        &self,
        schema: JazzSchema,
        lens: MigrationLens,
        new_tables: Vec<String>,
        dropped_tables: Vec<String>,
    ) -> Result<SchemaVersionId, String> {
        let activity_tx = self.inner.activity_tx.clone();
        let result = self
            .run(move |shell| {
                shell
                    .publish_runtime_schema_with_lens(schema, lens, new_tables, dropped_tables)
                    .map_err(|error| error.to_string())
            })
            .await;
        if result.is_ok() {
            notify_shell_activity(&activity_tx);
        }
        result
    }

    /// Compile and publish the permissions source selected by the catalogue
    /// shell.
    pub async fn publish_permissions_source(
        &self,
        schema: crate::tools::Schema,
        lineage_source: SchemaVersionId,
    ) -> Result<SchemaVersionId, String> {
        let activity_tx = self.inner.activity_tx.clone();
        let result = self
            .run(move |shell| {
                let schema =
                    crate::schema::JazzSchema::new(&schema).map_err(|error| error.to_string())?;
                shell
                    .publish_permissions_schema(schema, lineage_source)
                    .map_err(|error| error.to_string())
            })
            .await;
        if result.is_ok() {
            notify_shell_activity(&activity_tx);
        }
        result
    }

    /// Service one WebSocket message as ordered frame-sized shell ticks.
    ///
    /// Results become available as each frame has completed its tick while the
    /// shell thread keeps ingesting later frames. This is intentionally a
    /// streaming operation rather than one large `run` job so a route can
    /// publish a durability fate as soon as it is true.
    /// Apply an inbound frame batch and stream every immediately available response.
    pub fn receive_tick_stream(
        &self,
        session: ServerSession,
        frames: Vec<AbiBytes>,
    ) -> Result<ServerRuntimeFrameStream, String> {
        let activity_tx = self.inner.activity_tx.clone();
        let (outbound_tx, outbound_rx) = tokio_mpsc::unbounded_channel();
        self.send(ServerShellCommand::Run(Box::new(move |shell| {
            for frame in frames {
                let phase = inbound_frame_phase(&frame);
                let result = shell
                    .receive_frames(session, [frame])
                    .map_err(|error| format!("server receive {phase}: {error}"))
                    .and_then(|()| {
                        shell
                            .tick()
                            .map_err(|error| format!("server tick after {phase}: {error}"))
                    })
                    .and_then(|()| {
                        shell
                            .take_frames(session)
                            .map_err(|error| format!("server drain after {phase}: {error}"))
                    });
                let keep_streaming = result.is_ok();
                if outbound_tx.send(result).is_err() {
                    return;
                }
                if !keep_streaming {
                    return;
                }
                notify_shell_activity(&activity_tx);
            }
        })))?;
        Ok(ServerRuntimeFrameStream {
            receiver: outbound_rx,
        })
    }

    /// Tick the runtime once and return pending frames for one session.
    pub async fn tick_take(&self, session: ServerSession) -> Result<Vec<AbiBytes>, String> {
        let activity_tx = self.inner.activity_tx.clone();
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

    /// Attach a negotiated upstream transport to an edge runtime.
    pub async fn connect_upstream(
        &self,
        transport: Box<dyn Transport + Send>,
    ) -> Result<(), String> {
        let activity_tx = self.inner.activity_tx.clone();
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

    /// Wake shell tasks after an adapter stages inbound work.
    pub fn notify_activity(&self) {
        notify_shell_activity(&self.inner.activity_tx);
    }

    /// Close a semantic session without exposing runtime storage or peer state.
    pub fn close(&self, session: ServerSession) {
        let _ = self.send(ServerShellCommand::Run(Box::new(move |shell| {
            let _ = shell.close_session(session);
        })));
    }

    /// Retire the job sender, then wait until the owner has dropped the shell
    /// and its storage. It is safe for multiple shutdown paths to call this.
    /// Stop and join the runtime owner thread.
    pub async fn shutdown(&self) -> Result<(), String> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || shutdown_blocking(&inner))
            .await
            .map_err(|error| format!("server shell shutdown task failed: {error}"))?
    }

    fn send(&self, command: ServerShellCommand) -> Result<(), String> {
        self.inner
            .jobs
            .lock()
            .map_err(|_| "server shell jobs mutex poisoned".to_owned())?
            .as_ref()
            .ok_or_else(|| "server shell is shut down".to_owned())?
            .send(command)
            .map_err(|_| "server shell thread is not running".to_owned())
    }

    async fn run<T>(
        &self,
        run_on_shell: impl FnOnce(&mut InMemoryServerShell) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String>
    where
        T: Send + 'static,
    {
        let (reply, response) = oneshot::channel();
        self.send(ServerShellCommand::Run(Box::new(move |shell| {
            let _ = reply.send(run_on_shell(shell));
        })))?;
        response
            .await
            .map_err(|_| "server shell thread dropped response".to_owned())?
    }
}

fn shutdown_blocking(inner: &ServerShellInner) -> Result<(), String> {
    let sender = inner
        .jobs
        .lock()
        .map_err(|_| "server shell jobs mutex poisoned".to_owned())?
        .take();
    let Some(sender) = sender else {
        return Ok(());
    };

    let (stopped_tx, stopped_rx) = mpsc::channel();
    sender
        .send(ServerShellCommand::Shutdown(stopped_tx))
        .map_err(|_| "server shell thread is not running".to_owned())?;
    drop(sender);
    stopped_rx
        .recv()
        .map_err(|_| "server shell thread exited before storage shutdown".to_owned())?;

    if let Some(join) = inner
        .join
        .lock()
        .map_err(|_| "server shell join mutex poisoned".to_owned())?
        .take()
    {
        join.join()
            .map_err(|_| "server shell thread panicked during shutdown".to_owned())?;
    }
    Ok(())
}

fn inbound_frame_phase(frame: &[u8]) -> String {
    let Ok(frame) = decode_frame(frame) else {
        return "malformed wire frame".to_owned();
    };
    let WireFrame::Message(envelope) = frame else {
        return match frame {
            WireFrame::Hello(_) => "wire hello".to_owned(),
            WireFrame::Error(_) => "wire error".to_owned(),
            WireFrame::MessageFragment(_) => "wire message fragment".to_owned(),
            WireFrame::Message(_) => unreachable!("message handled above"),
        };
    };
    match decode_sync_message(&envelope.payload) {
        Ok(message) => sync_message_name(&message).to_owned(),
        Err(_) => "malformed SyncMessage".to_owned(),
    }
}

fn sync_message_name(message: &SyncMessage) -> &'static str {
    // This deliberately names only the protocol variant. Never format the
    // message itself here: claims and row payloads must not escape through a
    // transport diagnostic.
    match message {
        SyncMessage::BranchMetadata(_) => "BranchMetadata",
        SyncMessage::FetchBranchMetadata { .. } => "FetchBranchMetadata",
        SyncMessage::SessionClaims { .. } => "SessionClaims",
        SyncMessage::CommitUnit { .. } => "CommitUnit",
        SyncMessage::FateUpdate { .. } => "FateUpdate",
        SyncMessage::RegisterShape { .. } => "RegisterShape",
        SyncMessage::Subscribe(_) => "Subscribe",
        SyncMessage::SubscribeRejected { .. } => "SubscribeRejected",
        SyncMessage::Unsubscribe { .. } => "Unsubscribe",
        SyncMessage::PublishSchema { .. } => "PublishSchema",
        SyncMessage::PublishSchemaWithLens { .. } => "PublishSchemaWithLens",
        SyncMessage::PublishLens { .. } => "PublishLens",
        SyncMessage::SetCurrentWriteSchema { .. } => "SetCurrentWriteSchema",
        SyncMessage::CatalogueAck(_) => "CatalogueAck",
        SyncMessage::ViewUpdate { .. } => "ViewUpdate",
        SyncMessage::FetchRowVersions { .. } => "FetchRowVersions",
        SyncMessage::RowVersionPayloads { .. } => "RowVersionPayloads",
        SyncMessage::CatalogueSnapshot(_) => "CatalogueSnapshot",
        SyncMessage::PermissionAdviceRequest { .. } => "PermissionAdviceRequest",
        SyncMessage::PermissionAdviceResponse { .. } => "PermissionAdviceResponse",
        SyncMessage::AuthorizationScopeSubscribe { .. } => "AuthorizationScopeSubscribe",
        SyncMessage::AuthorizationScopeReceipt { .. } => "AuthorizationScopeReceipt",
        SyncMessage::AuthorizationScopeIntent { .. } => "AuthorizationScopeIntent",
        SyncMessage::AuthorizationScopeView { .. } => "AuthorizationScopeView",
        SyncMessage::AuthorizationScopeAggregateReceipt { .. } => {
            "AuthorizationScopeAggregateReceipt"
        }
        SyncMessage::AuthorizationScopeUnavailable { .. } => "AuthorizationScopeUnavailable",
        SyncMessage::AuthorizationScopeDecision { .. } => "AuthorizationScopeDecision",
    }
}

fn notify_shell_activity(activity_tx: &watch::Sender<u64>) {
    activity_tx.send_modify(|version| {
        *version = version.wrapping_add(1);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ReadViewKey, Subscribe, SubscriptionKey};
    use crate::query::{BindingId, ShapeId};
    use crate::wire::{WireEnvelope, encode_frame, encode_sync_message};

    fn encode_message(message: SyncMessage) -> Vec<u8> {
        let payload = encode_sync_message(&message).unwrap();
        encode_frame(&WireFrame::Message(WireEnvelope::new(0, 0, payload))).unwrap()
    }

    #[test]
    fn inbound_frame_phase_labels_semantic_transport_work() {
        let encoded = encode_message(SyncMessage::SessionClaims {
            identity: AuthorId::from_bytes([7; 16]),
            claims: BTreeMap::new(),
        });

        assert_eq!(inbound_frame_phase(&encoded), "SessionClaims");
        let shape_id = ShapeId(uuid::Uuid::from_bytes([3; 16]));
        let subscribe = encode_message(SyncMessage::Subscribe(Subscribe {
            shape_id,
            subscription: SubscriptionKey {
                shape_id,
                binding_id: BindingId(uuid::Uuid::from_bytes([4; 16])),
                read_view: ReadViewKey::default(),
            },
            values: Vec::new(),
            known_state: None,
        }));
        assert_eq!(inbound_frame_phase(&subscribe), "Subscribe");
        assert_eq!(inbound_frame_phase(&[0xff]), "malformed wire frame");
    }
}
