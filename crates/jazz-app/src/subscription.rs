//! Subscriptions: a `Send` [`Stream`] of typed deltas.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use jazz::tools::{
    ObjectId, OrderedRowDelta, ResultKey, SubscriptionStream, SubscriptionStreamItem,
};
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::handle::Jazz;
use crate::table::{self, Implementations, Table};

/// A change to a subscribed query's rows. Apply its parts in order:
/// `removed`, then `updated`, then `added` (see [`Rows::apply`]).
#[derive(Debug, Clone, PartialEq)]
pub struct Delta<T> {
    pub removed: Vec<Removed>,
    pub updated: Vec<Updated<T>>,
    pub added: Vec<Added<T>>,
    /// The server has not yet confirmed what the query matches: the rows
    /// so far are local.
    pub pending: bool,
    /// The subscription fell too far behind to be told every change: this
    /// delta removes every row held and adds the current ones, standing
    /// for the changes it missed. [`Rows::apply`] handles it like any
    /// other.
    pub coalesced: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Added<T> {
    pub index: usize,
    pub row: T,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Updated<T> {
    pub id: ObjectId,
    pub old_index: usize,
    pub new_index: usize,
    /// The row's new value; `None` when it only moved.
    pub row: Option<T>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Removed {
    pub id: ObjectId,
    pub index: usize,
}

/// A subscribed query's rows, kept in order by applying its deltas.
#[derive(Debug, Clone, PartialEq)]
pub struct Rows<T> {
    rows: Vec<T>,
}

impl<T> Default for Rows<T> {
    fn default() -> Self {
        Self { rows: Vec::new() }
    }
}

impl<T: Table> Rows<T> {
    pub fn apply(&mut self, delta: Delta<T>) {
        for removed in delta.removed {
            self.rows.retain(|row| row.id() != removed.id);
        }
        for updated in delta.updated {
            let held = self.rows.iter().position(|row| row.id() == updated.id);
            let row = match (held, updated.row) {
                (Some(at), Some(row)) => {
                    self.rows.remove(at);
                    row
                }
                (Some(at), None) => self.rows.remove(at),
                (None, Some(row)) => row,
                (None, None) => continue,
            };
            let at = updated.new_index.min(self.rows.len());
            self.rows.insert(at, row);
        }
        for added in delta.added {
            let at = added.index.min(self.rows.len());
            self.rows.insert(at, added.row);
        }
    }

    pub fn as_slice(&self) -> &[T] {
        &self.rows
    }

    pub fn into_vec(self) -> Vec<T> {
        self.rows
    }
}

/// A subscription's changes. It holds the app open while it lives, and
/// ends its query on the server when dropped.
pub struct Subscription<T> {
    receiver: mpsc::Receiver<Result<Delta<T>>>,
    _jazz: Jazz,
}

impl<T> Subscription<T> {
    pub(crate) fn new(receiver: mpsc::Receiver<Result<Delta<T>>>, jazz: Jazz) -> Self {
        Self {
            receiver,
            _jazz: jazz,
        }
    }
}

impl<T> Stream for Subscription<T> {
    type Item = Result<Delta<T>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

/// Deltas a [`Subscription`] holds before its forwarder waits for the
/// subscriber to read.
pub(crate) const SUBSCRIPTION_ITEMS: usize = 16;

/// Runs on the client's thread: forwards `stream`'s deltas, typed, until
/// either side ends.
pub(crate) async fn forward<T: Table>(
    mut stream: SubscriptionStream,
    sender: mpsc::Sender<Result<Delta<T>>>,
    implementations: Implementations,
) {
    loop {
        let item = tokio::select! {
            item = stream.next() => item,
            () = sender.closed() => return,
        };
        let delta = match item {
            None => return,
            Some(SubscriptionStreamItem::Delta(delta)) => typed::<T>(delta, &implementations),
            Some(SubscriptionStreamItem::Rejected { reason }) => {
                Err(Error::Rejected(format!("{reason:?}")))
            }
        };
        let failed = delta.is_err();
        // Waits while the subscriber's stream is full: deltas then wait
        // in the core, which coalesces a backlog past its limit.
        if sender.send(delta).await.is_err() || failed {
            return;
        }
    }
}

fn row_id<T: Table>(key: &ResultKey) -> Result<ObjectId> {
    key.row_id().ok_or_else(|| Error::Decode {
        table: T::NAME,
        message: "a joined result is not a row".into(),
    })
}

fn typed<T: Table>(delta: OrderedRowDelta, implementations: &Implementations) -> Result<Delta<T>> {
    Ok(Delta {
        removed: delta
            .removed
            .into_iter()
            .map(|removed| {
                Ok(Removed {
                    id: row_id::<T>(&removed.id)?,
                    index: removed.index,
                })
            })
            .collect::<Result<_>>()?,
        updated: delta
            .updated
            .into_iter()
            .map(|updated| {
                let id = row_id::<T>(&updated.id)?;
                Ok(Updated {
                    id,
                    old_index: updated.old_index,
                    new_index: updated.new_index,
                    row: updated
                        .row
                        .map(|row| table::decode(id, row.fields, implementations))
                        .transpose()?,
                })
            })
            .collect::<Result<_>>()?,
        added: delta
            .added
            .into_iter()
            .map(|added| {
                Ok(Added {
                    index: added.index,
                    row: table::decode(row_id::<T>(&added.id)?, added.row.fields, implementations)?,
                })
            })
            .collect::<Result<_>>()?,
        pending: delta.pending,
        coalesced: delta.coalesced,
    })
}
