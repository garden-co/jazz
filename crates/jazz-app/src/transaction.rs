//! Transactions: writes that commit together, and reads that see the
//! transaction's snapshot.

use std::cell::Cell;
use std::future::Future;
use std::rc::Rc;

use jazz::tools::{JazzClient, ObjectId, OpenTransactionId, ReadTier};

use futures_util::FutureExt as _;

use crate::error::Result;
use crate::handle::{Jazz, decode_result, insert};
use crate::query::Query;
use crate::table::{Implementations, Insert, Table, Update};

/// A transaction in progress, handed to [`Jazz::transaction`]'s closure.
///
/// It is serializable: its reads see the data as of its start, and the
/// server applies its writes only if nothing it read or wrote changed in
/// the meantime. Staged writes are not visible to reads outside it until it
/// commits. Clone it freely within the closure; it lives on the client's
/// thread, so it is not `Send`.
#[derive(Clone)]
pub struct Transaction {
    client: JazzClient,
    id: OpenTransactionId,
    wrote: Rc<Cell<bool>>,
    implementations: Implementations,
}

impl Transaction {
    pub(crate) fn client(&self) -> &JazzClient {
        &self.client
    }

    /// Marks the transaction as having written, so it commits.
    pub(crate) fn wrote(&self) {
        self.wrote.set(true);
    }

    pub fn insert<N: Insert>(&self, new: N) -> Result<N::Row> {
        let (row, _) = insert(&self.client, new, &self.implementations)?;
        self.wrote.set(true);
        Ok(row)
    }

    pub fn update<U: Update>(&self, id: ObjectId, update: U) -> Result<()> {
        let cells = update.into_cells(&self.implementations)?;
        self.client.update(<U::Row as Table>::NAME, id, cells)?;
        self.wrote.set(true);
        Ok(())
    }

    /// Writes the row with this id, as [`Jazz::upsert`]; returns it as the
    /// transaction now sees it.
    pub async fn upsert<N: Insert>(&self, id: ObjectId, new: N) -> Result<N::Row> {
        let cells = new.into_cells(&self.implementations)?;
        self.client
            .upsert(<N::Row as Table>::NAME, *id.uuid(), cells)?;
        self.wrote.set(true);
        self.read_back(id).await
    }

    pub fn delete<T: Table>(&self, id: ObjectId) -> Result<()> {
        self.client.delete(T::NAME, id)?;
        self.wrote.set(true);
        Ok(())
    }

    /// Brings back a deleted row, as [`Jazz::restore`].
    pub async fn restore<T: Table>(&self, id: ObjectId) -> Result<T> {
        self.client.restore(T::NAME, id, None)?;
        self.wrote.set(true);
        self.read_back(id).await
    }

    /// Brings back a deleted row with `update`'s columns replaced.
    pub async fn restore_with<U: Update>(&self, id: ObjectId, update: U) -> Result<U::Row> {
        let cells = update.into_cells(&self.implementations)?;
        self.client
            .restore(<U::Row as Table>::NAME, id, Some(cells))?;
        self.wrote.set(true);
        self.read_back(id).await
    }

    async fn read_back<T: Table>(&self, id: ObjectId) -> Result<T> {
        self.get::<T>(id)
            .await?
            .ok_or_else(|| crate::Error::Decode {
                table: T::NAME,
                message: format!("row {} is not there after writing it", id.uuid()),
            })
    }

    /// Every row `query` matches, as the transaction sees them.
    pub async fn all<T: Table>(&self, query: Query<T>) -> Result<Vec<T>> {
        let rows = self
            .client
            .query(query.into_untyped(), ReadTier::LocalFirst)
            .await?;
        rows.into_iter()
            .map(|row| decode_result(row, &self.implementations))
            .collect()
    }

    pub async fn one<T: Table>(&self, query: Query<T>) -> Result<Option<T>> {
        Ok(self.all(query.limit(1)).await?.into_iter().next())
    }

    pub async fn get<T: Table>(&self, id: ObjectId) -> Result<Option<T>> {
        self.one(Query::<T>::all().id(id)).await
    }
}

impl Jazz {
    /// Runs `body` in a serializable transaction. It is committed if `body`
    /// returns `Ok` (and wrote anything), rolled back if it returns `Err`.
    /// On a [`Jazz::durable`] handle the call returns once the commit
    /// reached that tier. A body that panics rolls back and fails with
    /// [`crate::Error::Panicked`]. Dropping the returned future doesn't
    /// cancel the transaction: it runs to its end on the client's thread.
    ///
    /// ```ignore
    /// let moved = jazz.transaction(move |tx| async move {
    ///     let task = tx.get::<Task>(id).await?.ok_or(MyError::Gone)?;
    ///     tx.update(id, TaskUpdate { status: Some(next), ..Default::default() })?;
    ///     Ok(task)
    /// }).await?;
    /// ```
    pub async fn transaction<F, Fut, R>(&self, body: F) -> Result<R>
    where
        F: FnOnce(Transaction) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R>> + 'static,
        R: Send + 'static,
    {
        let implementations = self.info.implementations.clone();
        let settle = self.settler()?;
        self.with_client(move |client| async move {
            let open = client.begin_transaction()?;
            let transaction = Transaction {
                client: open.client().clone(),
                id: open.transaction_id(),
                wrote: Rc::new(Cell::new(false)),
                implementations,
            };
            let wrote = transaction.wrote.clone();
            let scoped = transaction.client.clone();
            let id = transaction.id;
            // The body runs inside the guard, so a panic rolls back too.
            let outcome = std::panic::AssertUnwindSafe(async move { body(transaction).await })
                .catch_unwind()
                .await
                .unwrap_or_else(|panic| {
                    Err(crate::Error::Panicked(crate::handle::panic_message(
                        &*panic,
                    )))
                });
            match outcome {
                Ok(result) if wrote.get() => {
                    let committed = scoped.commit_transaction(id)?;
                    settle.settle(&client, Some(committed)).await?;
                    Ok(result)
                }
                Ok(result) => {
                    scoped.rollback_transaction(id)?;
                    Ok(result)
                }
                Err(error) => {
                    // The body's error is the one to report; a rollback
                    // that fails too adds nothing the caller can act on.
                    let _ = scoped.rollback_transaction(id);
                    Err(error)
                }
            }
        })
        .await
    }
}
