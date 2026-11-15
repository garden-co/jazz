//! The app's link and store: disconnecting and reconnecting, deleting the
//! local store, and the writes the server refused that nothing waited on
//! (TS's `disconnect`, `reconnect`, `deleteClientStorage`, `onMutationError`).

use std::cell::RefCell;
use std::collections::HashSet;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use jazz::tools::{JazzClient, MutationError, TransactionId};
use tokio::sync::mpsc;

use crate::error::Result;
use crate::handle::Jazz;

/// How often refusals are collected from the client.
const POLL: Duration = Duration::from_millis(50);
/// Refusals kept for a listener that hasn't subscribed yet.
const PENDING_MAX: usize = 1024;

#[derive(Default)]
struct Reports {
    /// Commits a durable wait is waiting on: their refusals are its.
    claimed: HashSet<TransactionId>,
    listeners: Vec<mpsc::UnboundedSender<MutationError>>,
    /// Refusals no listener has taken yet, oldest first.
    pending: Vec<MutationError>,
}

thread_local! {
    // On the client's thread: jobs and the reporter share it.
    static REPORTS: RefCell<Reports> = RefCell::default();
}

pub(crate) fn claim(written: TransactionId) {
    REPORTS.with(|reports| reports.borrow_mut().claimed.insert(written));
}

pub(crate) fn unclaim(written: TransactionId) {
    REPORTS.with(|reports| reports.borrow_mut().claimed.remove(&written));
}

/// Runs on the client's thread for its lifetime: hands each refusal no
/// durable wait claimed to the listeners, or keeps it until one comes.
pub(crate) async fn report_mutation_errors(client: JazzClient) {
    loop {
        tokio::time::sleep(POLL).await;
        let errors = client.take_mutation_errors();
        if errors.is_empty() {
            continue;
        }
        REPORTS.with(|reports| {
            let mut reports = reports.borrow_mut();
            for error in errors {
                if reports.claimed.remove(&error.transaction) {
                    continue;
                }
                reports.listeners.retain(|listener| !listener.is_closed());
                if reports.listeners.is_empty() {
                    if reports.pending.len() == PENDING_MAX {
                        reports.pending.remove(0);
                    }
                    reports.pending.push(error);
                } else {
                    for listener in &reports.listeners {
                        let _ = listener.send(error.clone());
                    }
                }
            }
        });
    }
}

/// Refused writes nothing waited on, as they arrive: see
/// [`Jazz::mutation_errors`].
pub struct MutationErrors {
    receiver: mpsc::UnboundedReceiver<MutationError>,
    _jazz: Jazz,
}

impl Stream for MutationErrors {
    type Item = MutationError;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl Jazz {
    /// The writes the server refused that no [`Jazz::durable`] wait handled,
    /// as TS's `onMutationError` reports them: a plain write returns once
    /// it is committed locally, so its refusal comes here. Refusals that
    /// arrived before the first call are delivered to it first. Without a
    /// server nothing is refused, and the stream stays empty.
    pub async fn mutation_errors(&self) -> Result<MutationErrors> {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.with_client(move |_| async move {
            REPORTS.with(|reports| {
                let mut reports = reports.borrow_mut();
                for error in std::mem::take(&mut reports.pending) {
                    let _ = sender.send(error);
                }
                reports.listeners.push(sender);
            });
            Ok(())
        })
        .await?;
        Ok(MutationErrors {
            receiver,
            _jazz: self.clone(),
        })
    }

    /// Leaves the server: local reads and writes go on, and sync resumes
    /// with [`Jazz::reconnect`] (TS's `disconnect`).
    pub async fn disconnect(&self) -> Result<()> {
        self.with_client(|client| async move {
            client.disconnect();
            Ok(())
        })
        .await
    }

    /// Resumes sync after [`Jazz::disconnect`], once the link is attached
    /// (TS's `reconnect`).
    pub async fn reconnect(&self) -> Result<()> {
        self.with_client(|client| async move {
            client.reconnect().await?;
            Ok(())
        })
        .await
    }

    /// Stops the client and deletes its local store (TS's
    /// `deleteClientStorage`): the next open of the app starts empty and
    /// syncs again. Writes the server hasn't accepted yet are lost. Nothing
    /// to delete for [`crate::Storage::Memory`].
    pub async fn delete_client_storage(&self) -> Result<()> {
        self.shutdown().await?;
        let Some(dir) = &self.info.storage_dir else {
            return Ok(());
        };
        for file in [
            "jazz.sqlite",
            "jazz.sqlite-wal",
            "jazz.sqlite-shm",
            "client_id",
        ] {
            match std::fs::remove_file(dir.join(file)) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(jazz::tools::JazzError::Io(error).into()),
            }
        }
        Ok(())
    }
}
