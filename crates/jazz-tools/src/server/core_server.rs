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

#[derive(Clone)]
pub(crate) struct CoreServer {
    commands: mpsc::Sender<CoreCommand>,
    activity_tx: watch::Sender<u64>,
}

enum CoreCommand {
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

impl CoreServer {
    pub(crate) fn start_with_storage(
        schema: JazzSchema,
        storage_config: StorageConfig,
    ) -> Result<Self, String> {
        let (commands, receiver) = mpsc::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let (activity_tx, _) = watch::channel(0_u64);
        let core_activity_tx = activity_tx.clone();

        thread::Builder::new()
            .name("jazz-core-server".to_owned())
            .spawn(move || {
                let config = InMemoryServerShellConfig::new(
                    schema,
                    DbIdentity {
                        node: NodeUuid::from_bytes([0x5e; 16]),
                        author: AuthorId::SYSTEM,
                    },
                )
                .with_row_id_seed(0x5e);
                let shell = InMemoryServerShell::start_with_storage(config, storage_config);
                let mut shell = match shell {
                    Ok(shell) => {
                        let _ = started_tx.send(Ok(()));
                        shell
                    }
                    Err(error) => {
                        let _ = started_tx.send(Err(error.to_string()));
                        return;
                    }
                };

                while let Ok(command) = receiver.recv() {
                    match command {
                        CoreCommand::Open {
                            identity,
                            claims,
                            trust,
                            reply,
                        } => {
                            let result = shell
                                .accept_subscriber_session_with_claims_and_trust(
                                    identity, claims, trust,
                                )
                                .map_err(|error| error.to_string());
                            let _ = reply.send(result);
                        }
                        CoreCommand::PublishSchema { schema, reply } => {
                            let result = shell
                                .publish_runtime_schema(*schema)
                                .map_err(|error| error.to_string());
                            let _ = reply.send(result);
                        }
                        CoreCommand::ReceiveTickTake {
                            session,
                            frames,
                            reply,
                        } => {
                            let result = shell
                                .receive_frames(session, frames)
                                .and_then(|()| shell.tick())
                                .and_then(|()| shell.take_frames(session))
                                .map_err(|error| error.to_string());
                            if result.is_ok() {
                                notify_core_activity(&core_activity_tx);
                            }
                            let _ = reply.send(result);
                        }
                        CoreCommand::TickTake { session, reply } => {
                            let result = shell
                                .tick()
                                .and_then(|()| shell.take_frames(session))
                                .map_err(|error| error.to_string());
                            let _ = reply.send(result);
                        }
                        CoreCommand::Close { session } => {
                            let _ = shell.close_session(session);
                        }
                    }
                }
            })
            .map_err(|error| format!("failed to spawn core server thread: {error}"))?;

        started_rx
            .recv()
            .map_err(|_| "core server thread exited before startup".to_owned())??;
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
            .send(CoreCommand::Open {
                identity,
                claims,
                trust,
                reply,
            })
            .map_err(|_| "core server thread is not running".to_owned())?;
        response
            .await
            .map_err(|_| "core server thread dropped open response".to_owned())?
    }

    pub(crate) async fn publish_schema(
        &self,
        schema: JazzSchema,
    ) -> Result<SchemaVersionId, String> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(CoreCommand::PublishSchema {
                schema: Box::new(schema),
                reply,
            })
            .map_err(|_| "core server thread is not running".to_owned())?;
        response
            .await
            .map_err(|_| "core server thread dropped schema publish response".to_owned())?
    }

    pub(crate) async fn receive_tick_take(
        &self,
        session: ServerSession,
        frames: Vec<AbiBytes>,
    ) -> Result<Vec<AbiBytes>, String> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(CoreCommand::ReceiveTickTake {
                session,
                frames,
                reply,
            })
            .map_err(|_| "core server thread is not running".to_owned())?;
        response
            .await
            .map_err(|_| "core server thread dropped receive response".to_owned())?
    }

    pub(crate) async fn tick_take(&self, session: ServerSession) -> Result<Vec<AbiBytes>, String> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(CoreCommand::TickTake { session, reply })
            .map_err(|_| "core server thread is not running".to_owned())?;
        response
            .await
            .map_err(|_| "core server thread dropped tick response".to_owned())?
    }

    pub(crate) fn close(&self, session: ServerSession) {
        let _ = self.commands.send(CoreCommand::Close { session });
    }
}

fn notify_core_activity(activity_tx: &watch::Sender<u64>) {
    activity_tx.send_modify(|version| {
        *version = version.wrapping_add(1);
    });
}
