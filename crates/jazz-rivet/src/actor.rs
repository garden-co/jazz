//! Actor-owned execution. A request owns a reply receiver, never the task that
//! runs SQL. Keep the Tokio runtime alive until `close` completes.

use crate::Store;
use anyhow::{Result, ensure};
use jazz_sql::HostTable;
use sqlite_embedded::vfs::SqliteTransportHandle;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot, watch};

type Reply<T> = oneshot::Sender<Result<T>>;
type Completion = Option<Result<(), Arc<str>>>;

enum Command {
    Put {
        table: HostTable,
        key: Vec<u8>,
        value: Vec<u8>,
        reply: Reply<Option<Vec<u8>>>,
    },
    Get {
        table: HostTable,
        key: Vec<u8>,
        reply: Reply<Option<Vec<u8>>>,
    },
    Close,
}

/// A bounded mailbox for one actor's database. Admission is the synchronous
/// enqueue after capacity is acquired. Cancelling before that point admits
/// nothing; afterwards the owner finishes the command even without a waiter.
#[derive(Clone)]
pub struct ActorStore {
    commands: mpsc::Sender<Command>,
    accepting: Arc<Mutex<bool>>,
    completion: watch::Receiver<Completion>,
    runtime: tokio::runtime::Handle,
}

impl ActorStore {
    pub async fn open(
        transport: SqliteTransportHandle,
        actor_id: String,
        generation: u64,
        queue_capacity: usize,
    ) -> Result<Self> {
        ensure!(queue_capacity > 0, "actor queue capacity must be positive");
        let store = Store::open(transport, actor_id, generation).await?;
        let (commands, receiver) = mpsc::channel(queue_capacity);
        let (completed, completion) = watch::channel(None);
        let runtime = tokio::runtime::Handle::current();
        // Deliberately no request-owned JoinHandle or AbortHandle. The receiver
        // also drains when all senders disappear, then closes the database.
        runtime.spawn(async move {
            let result = run(store, receiver)
                .await
                .map_err(|e| Arc::from(e.to_string()));
            completed.send_replace(Some(result));
        });
        Ok(Self {
            commands,
            accepting: Arc::new(Mutex::new(true)),
            completion,
            runtime,
        })
    }

    async fn request<T>(&self, make: impl FnOnce(Reply<T>) -> Command) -> Result<T> {
        ensure!(*self.accepting.lock().unwrap(), "actor store is closing");
        let permit = self
            .commands
            .reserve()
            .await
            .map_err(|_| anyhow::anyhow!("actor store owner stopped"))?;
        let (reply, answer) = oneshot::channel();
        // Copy request data only after obtaining bounded queue capacity, and
        // outside the admission lock. No await separates the final check/send.
        let command = make(reply);
        {
            let accepting = self.accepting.lock().unwrap();
            ensure!(*accepting, "actor store is closing");
            permit.send(command);
        }
        answer
            .await
            .map_err(|_| anyhow::anyhow!("actor store owner stopped before replying"))?
    }

    pub async fn put_if_absent(
        &self,
        table: HostTable,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        self.request(|reply| Command::Put {
            table,
            key: key.to_vec(),
            value: value.to_vec(),
            reply,
        })
        .await
    }

    pub async fn get(&self, table: HostTable, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.request(|reply| Command::Get {
            table,
            key: key.to_vec(),
            reply,
        })
        .await
    }

    /// Stop admission, finish all admitted work, then close SQLite. Multiple
    /// callers share completion. Dropping a shutdown waiter does not stop drain.
    pub async fn close(&self) -> Result<()> {
        {
            let mut accepting = self.accepting.lock().unwrap();
            if *accepting {
                *accepting = false;
                let commands = self.commands.clone();
                // Sending Close can wait for capacity; this wait belongs to
                // the actor too, so cancellation cannot strand it half closed.
                self.runtime.spawn(async move {
                    let _ = commands.send(Command::Close).await;
                });
            }
        }
        let mut completion = self.completion.clone();
        loop {
            if let Some(result) = completion.borrow_and_update().clone() {
                return result.map_err(|error| anyhow::anyhow!(error.to_string()));
            }
            completion.changed().await.map_err(|_| {
                anyhow::anyhow!("actor store owner stopped without closing storage")
            })?;
        }
    }
}

async fn run(store: Store, mut commands: mpsc::Receiver<Command>) -> Result<()> {
    while let Some(command) = commands.recv().await {
        match command {
            Command::Put {
                table,
                key,
                value,
                reply,
            } => {
                let result = store.put_if_absent(table, &key, &value).await;
                // A missing receiver is not a reason to skip or undo the write.
                let _ = reply.send(result);
            }
            Command::Get { table, key, reply } => {
                let _ = reply.send(store.get(table, &key).await);
            }
            Command::Close => commands.close(),
        }
    }
    store.close().await
}
