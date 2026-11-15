//! Transactions: writes that commit together, and reads that see the
//! transaction's snapshot.

use std::cell::Cell;
use std::future::Future;
use std::rc::Rc;

use jazz::tools::{JazzClient, ObjectId, OpenTransactionId, ReadTier};

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

    pub fn upsert<N: Insert>(&self, id: ObjectId, new: N) -> Result<()> {
        let cells = new.into_cells(&self.implementations)?;
        self.client
            .upsert(<N::Row as Table>::NAME, *id.uuid(), cells)?;
        self.wrote.set(true);
        Ok(())
    }

    pub fn delete<T: Table>(&self, id: ObjectId) -> Result<()> {
        self.client.delete(T::NAME, id)?;
        self.wrote.set(true);
        Ok(())
    }

    pub fn restore<T: Table>(&self, id: ObjectId) -> Result<()> {
        self.client.restore(T::NAME, id, None)?;
        self.wrote.set(true);
        Ok(())
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
    /// reached that tier.
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
        let settle = self.settler();
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
            match body(transaction).await {
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
                    scoped.rollback_transaction(id)?;
                    Err(error)
                }
            }
        })
        .await
    }
}
