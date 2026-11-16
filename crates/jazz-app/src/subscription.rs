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
use crate::include::Row;
use crate::table::{self, Implementations, Table};

/// A change to a subscribed query's rows. Apply its parts in order:
/// `removed`, then `updated`, then `added` (see [`Rows::apply`]).
///
/// Each part names the result it changes by its `key`. A row can be
/// several results: a history read has one per version of a row, and a
/// union one per arm that matches the row, all sharing the row's id.
#[derive(Debug, Clone, PartialEq)]
pub struct Delta<T> {
    pub removed: Vec<Removed>,
    pub updated: Vec<Updated<T>>,
    pub added: Vec<Added<T>>,
    /// The server has not yet confirmed what the query matches: the rows
    /// so far are local.
    pub pending: bool,
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Added<T> {
    /// The result this is.
    pub key: ResultKey,
    pub index: usize,
    pub row: T,
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Updated<T> {
    /// The result that changed.
    pub key: ResultKey,
    pub id: ObjectId,
    pub old_index: usize,
    pub new_index: usize,
    /// The row's new value; `None` when it only moved.
    pub row: Option<T>,
    /// Per column named in [`crate::ReadOptions::splices`], the edits that
    /// turn the value the previous delta showed into this one, in order.
    /// A column without an entry has none to offer: read its new value.
    pub splices: Vec<(String, Vec<ValueSplice>)>,
}

/// One edit of a large value in a subscription update: replace `delete`
/// bytes at byte offset `at` with `insert` (UTF-8 for text). Text edits
/// also carry the same edit in UTF-16 code units (`at_utf16`,
/// `delete_utf16`), for hosts whose strings count those (Swift, Kotlin,
/// JavaScript).
pub use jazz::db::LargeValueDeltaSplice as ValueSplice;

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Removed {
    /// The result that went.
    pub key: ResultKey,
    pub id: ObjectId,
    pub index: usize,
}

/// A subscribed query's rows, kept in order by applying its deltas.
#[derive(Debug, Clone, PartialEq)]
pub struct Rows<T> {
    rows: Vec<T>,
    /// Each row's result, in the same order.
    keys: Vec<ResultKey>,
}

impl<T> Default for Rows<T> {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            keys: Vec::new(),
        }
    }
}

impl<T: Row> Rows<T> {
    pub fn apply(&mut self, delta: Delta<T>) {
        for removed in delta.removed {
            if let Some(at) = self.position(&removed.key) {
                self.rows.remove(at);
                self.keys.remove(at);
            }
        }
        for updated in delta.updated {
            let row = match (self.position(&updated.key), updated.row) {
                (Some(at), Some(row)) => {
                    self.rows.remove(at);
                    self.keys.remove(at);
                    row
                }
                (Some(at), None) => {
                    self.keys.remove(at);
                    self.rows.remove(at)
                }
                (None, Some(row)) => row,
                (None, None) => continue,
            };
            let at = updated.new_index.min(self.rows.len());
            self.rows.insert(at, row);
            self.keys.insert(at, updated.key);
        }
        for added in delta.added {
            let at = added.index.min(self.rows.len());
            self.rows.insert(at, added.row);
            self.keys.insert(at, added.key);
        }
    }

    fn position(&self, key: &ResultKey) -> Option<usize> {
        self.keys.iter().position(|held| held == key)
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
    receiver: mpsc::UnboundedReceiver<Result<Delta<T>>>,
    _jazz: Jazz,
}

impl<T> Subscription<T> {
    pub(crate) fn new(receiver: mpsc::UnboundedReceiver<Result<Delta<T>>>, jazz: Jazz) -> Self {
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

/// Runs on the client's thread: forwards `stream`'s deltas, typed, until
/// either side ends.
pub(crate) async fn forward<T: Row>(
    mut stream: SubscriptionStream,
    sender: mpsc::UnboundedSender<Result<Delta<T>>>,
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
        if sender.send(delta).is_err() || failed {
            return;
        }
    }
}

fn row_id<T: Row>(key: &ResultKey) -> Result<ObjectId> {
    key.row_id().ok_or_else(|| Error::Decode {
        table: <T::Table as Table>::NAME,
        message: "a joined result is not a row".into(),
    })
}

fn typed<T: Row>(delta: OrderedRowDelta, implementations: &Implementations) -> Result<Delta<T>> {
    Ok(Delta {
        removed: delta
            .removed
            .into_iter()
            .map(|removed| {
                Ok(Removed {
                    id: row_id::<T>(&removed.id)?,
                    key: removed.id,
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
                    key: updated.id,
                    id,
                    old_index: updated.old_index,
                    new_index: updated.new_index,
                    row: updated
                        .row
                        .map(|row| table::decode(id, row.fields, implementations))
                        .transpose()?,
                    splices: updated.splices,
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
                    key: added.id,
                })
            })
            .collect::<Result<_>>()?,
        pending: delta.pending,
    })
}
