use std::collections::BTreeMap;
use std::sync::mpsc;
use std::thread;

use jazz::db::{CommitUnitTrust, DbIdentity};
use jazz::groove::records::Value;
use jazz::ids::{AuthorId, NodeUuid, SchemaVersionId};
use jazz::schema::JazzSchema;
use jazz_server::{
    AbiBytes, InMemoryServerShell, InMemoryServerShellConfig, ServerSession, StorageConfig,
};
use tokio::sync::{oneshot, watch};

/// Sendable handle for the thread that owns the local jazz_core engine shell.
///
/// The underlying `InMemoryServerShell` is intentionally kept on one OS thread
/// because it currently stores its DB, sessions, and transports behind
/// `Rc<RefCell<...>>`. Axum request/websocket tasks can clone this handle, but
/// all direct shell work is serialized onto the local owner thread below.
#[derive(Clone)]
pub(crate) struct LocalEngineHandle {
    commands: mpsc::Sender<LocalEngineCommand>,
    activity_tx: watch::Sender<u64>,
}

enum LocalEngineCommand {
    Open {
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
        reply: oneshot::Sender<Result<ServerSession, String>>,
    },
    PublishSchema {
        schema: Box<JazzSchema>,
        reply: oneshot::Sender<Result<SchemaVersionId, String>>,
    },
    ReceiveTickTake {
        session: ServerSession,
        frames: Vec<AbiBytes>,
        reply: oneshot::Sender<Result<Vec<AbiBytes>, String>>,
    },
    TickTake {
        session: ServerSession,
        reply: oneshot::Sender<Result<Vec<AbiBytes>, String>>,
    },
    Close {
        session: ServerSession,
    },
}

struct LocalEngineOwner {
    shell: InMemoryServerShell,
    activity_tx: watch::Sender<u64>,
}

impl LocalEngineHandle {
    pub(crate) fn start_with_storage(
        schema: JazzSchema,
        storage_config: StorageConfig,
    ) -> Result<Self, String> {
        let (commands, receiver) = mpsc::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let (activity_tx, _) = watch::channel(0_u64);
        let engine_activity_tx = activity_tx.clone();

        thread::Builder::new()
            .name("jazz-local-engine".to_owned())
            .spawn(move || {
                let config = InMemoryServerShellConfig::new(
                    schema,
                    DbIdentity {
                        node: NodeUuid::from_bytes([0x5e; 16]),
                        author: AuthorId::SYSTEM,
                    },
                )
                .with_row_id_seed(0x5e);
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

                let mut owner = LocalEngineOwner {
                    shell,
                    activity_tx: engine_activity_tx,
                };

                while let Ok(command) = receiver.recv() {
                    owner.handle(command);
                }
            })
            .map_err(|error| format!("failed to spawn local engine thread: {error}"))?;

        started_rx
            .recv()
            .map_err(|_| "local engine thread exited before startup".to_owned())??;
        Ok(Self {
            commands,
            activity_tx,
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
        let (reply, response) = oneshot::channel();
        self.commands
            .send(LocalEngineCommand::Open {
                identity,
                claims,
                trust,
                reply,
            })
            .map_err(|_| "local engine thread is not running".to_owned())?;
        response
            .await
            .map_err(|_| "local engine thread dropped open response".to_owned())?
    }

    pub(crate) async fn publish_schema(
        &self,
        schema: JazzSchema,
    ) -> Result<SchemaVersionId, String> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(LocalEngineCommand::PublishSchema {
                schema: Box::new(schema),
                reply,
            })
            .map_err(|_| "local engine thread is not running".to_owned())?;
        response
            .await
            .map_err(|_| "local engine thread dropped schema publish response".to_owned())?
    }

    pub(crate) async fn receive_tick_take(
        &self,
        session: ServerSession,
        frames: Vec<AbiBytes>,
    ) -> Result<Vec<AbiBytes>, String> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(LocalEngineCommand::ReceiveTickTake {
                session,
                frames,
                reply,
            })
            .map_err(|_| "local engine thread is not running".to_owned())?;
        response
            .await
            .map_err(|_| "local engine thread dropped receive response".to_owned())?
    }

    pub(crate) async fn tick_take(&self, session: ServerSession) -> Result<Vec<AbiBytes>, String> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(LocalEngineCommand::TickTake { session, reply })
            .map_err(|_| "local engine thread is not running".to_owned())?;
        response
            .await
            .map_err(|_| "local engine thread dropped tick response".to_owned())?
    }

    pub(crate) fn close(&self, session: ServerSession) {
        let _ = self.commands.send(LocalEngineCommand::Close { session });
    }
}

impl LocalEngineOwner {
    fn handle(&mut self, command: LocalEngineCommand) {
        match command {
            LocalEngineCommand::Open {
                identity,
                claims,
                trust,
                reply,
            } => {
                let _ = reply.send(self.open(identity, claims, trust));
            }
            LocalEngineCommand::PublishSchema { schema, reply } => {
                let _ = reply.send(self.publish_schema(*schema));
            }
            LocalEngineCommand::ReceiveTickTake {
                session,
                frames,
                reply,
            } => {
                let result = self.receive_tick_take(session, frames);
                if result.is_ok() {
                    notify_engine_activity(&self.activity_tx);
                }
                let _ = reply.send(result);
            }
            LocalEngineCommand::TickTake { session, reply } => {
                let _ = reply.send(self.tick_take(session));
            }
            LocalEngineCommand::Close { session } => {
                let _ = self.shell.close_session(session);
            }
        }
    }

    fn open(
        &mut self,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
    ) -> Result<ServerSession, String> {
        self.shell
            .accept_subscriber_session_with_claims_and_trust(identity, claims, trust)
            .map_err(|error| error.to_string())
    }

    fn publish_schema(&mut self, schema: JazzSchema) -> Result<SchemaVersionId, String> {
        self.shell
            .publish_runtime_schema(schema)
            .map_err(|error| error.to_string())
    }

    fn receive_tick_take(
        &mut self,
        session: ServerSession,
        frames: Vec<AbiBytes>,
    ) -> Result<Vec<AbiBytes>, String> {
        self.shell
            .receive_frames(session, frames)
            .and_then(|()| self.shell.tick())
            .and_then(|()| self.shell.take_frames(session))
            .map_err(|error| error.to_string())
    }

    fn tick_take(&mut self, session: ServerSession) -> Result<Vec<AbiBytes>, String> {
        self.shell
            .tick()
            .and_then(|()| self.shell.take_frames(session))
            .map_err(|error| error.to_string())
    }
}

fn notify_engine_activity(activity_tx: &watch::Sender<u64>) {
    activity_tx.send_modify(|version| {
        *version = version.wrapping_add(1);
    });
}
