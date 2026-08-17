use std::collections::HashMap;
use std::thread;

use jazz::tools::{
    AppContext, BatchId, DurabilityTier, JazzClient, JazzError, ObjectId, Query, Value,
};
use tokio::sync::{mpsc, oneshot};

async fn connect_native(context: AppContext) -> jazz::tools::Result<JazzClient> {
    JazzClient::connect_with_native_transport(
        context,
        std::sync::Arc::new(jazz_native_transport::NativeWebSocketConnector),
    )
    .await
}

#[derive(Clone)]
pub struct TodoClient {
    tx: mpsc::UnboundedSender<ClientCommand>,
}

enum ClientCommand {
    Query {
        query: Query,
        durability_tier: Option<DurabilityTier>,
        reply: oneshot::Sender<jazz::tools::Result<Vec<(ObjectId, Vec<Value>)>>>,
    },
    Insert {
        table: String,
        values: HashMap<String, Value>,
        reply: oneshot::Sender<jazz::tools::Result<(ObjectId, Vec<Value>, Option<BatchId>)>>,
    },
    Update {
        object_id: ObjectId,
        updates: Vec<(String, Value)>,
        reply: oneshot::Sender<jazz::tools::Result<Option<BatchId>>>,
    },
    Delete {
        object_id: ObjectId,
        reply: oneshot::Sender<jazz::tools::Result<Option<BatchId>>>,
    },
}

impl TodoClient {
    pub async fn connect(context: AppContext) -> jazz::tools::Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();

        thread::Builder::new()
            .name("todo-jazz-client".to_string())
            .spawn(move || run_client_worker(context, rx, ready_tx))
            .map_err(JazzError::Io)?;

        ready_rx.recv().map_err(|_| JazzError::ChannelClosed)??;
        Ok(Self { tx })
    }

    pub async fn query(
        &self,
        query: Query,
        durability_tier: Option<DurabilityTier>,
    ) -> jazz::tools::Result<Vec<(ObjectId, Vec<Value>)>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(ClientCommand::Query {
                query,
                durability_tier,
                reply,
            })
            .map_err(|_| JazzError::ChannelClosed)?;
        rx.await.map_err(|_| JazzError::ChannelClosed)?
    }

    pub async fn insert(
        &self,
        table: &str,
        values: HashMap<String, Value>,
    ) -> jazz::tools::Result<(ObjectId, Vec<Value>, Option<BatchId>)> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(ClientCommand::Insert {
                table: table.to_string(),
                values,
                reply,
            })
            .map_err(|_| JazzError::ChannelClosed)?;
        rx.await.map_err(|_| JazzError::ChannelClosed)?
    }

    pub async fn update(
        &self,
        object_id: ObjectId,
        updates: Vec<(String, Value)>,
    ) -> jazz::tools::Result<Option<BatchId>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(ClientCommand::Update {
                object_id,
                updates,
                reply,
            })
            .map_err(|_| JazzError::ChannelClosed)?;
        rx.await.map_err(|_| JazzError::ChannelClosed)?
    }

    pub async fn delete(&self, object_id: ObjectId) -> jazz::tools::Result<Option<BatchId>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(ClientCommand::Delete { object_id, reply })
            .map_err(|_| JazzError::ChannelClosed)?;
        rx.await.map_err(|_| JazzError::ChannelClosed)?
    }
}

fn run_client_worker(
    context: AppContext,
    mut rx: mpsc::UnboundedReceiver<ClientCommand>,
    ready_tx: std::sync::mpsc::Sender<jazz::tools::Result<()>>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = ready_tx.send(Err(JazzError::Connection(format!(
                "failed to start client runtime: {error}"
            ))));
            return;
        }
    };

    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async move {
        let client = match connect_native(context).await {
            Ok(client) => {
                let _ = ready_tx.send(Ok(()));
                client
            }
            Err(error) => {
                let _ = ready_tx.send(Err(error));
                return;
            }
        };

        while let Some(command) = rx.recv().await {
            match command {
                ClientCommand::Query {
                    query,
                    durability_tier,
                    reply,
                } => {
                    let _ = reply.send(client.query(query, durability_tier).await);
                }
                ClientCommand::Insert {
                    table,
                    values,
                    reply,
                } => {
                    let _ = reply.send(client.insert(&table, values));
                }
                ClientCommand::Update {
                    object_id,
                    updates,
                    reply,
                } => {
                    let _ = reply.send(client.update(object_id, updates));
                }
                ClientCommand::Delete { object_id, reply } => {
                    let _ = reply.send(client.delete(object_id));
                }
            }
        }
    });
}
