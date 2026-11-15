//! Durability: how far a write has gone before the call returns, and
//! waiting for the writes made so far to reach the server.

use jazz::tools::{DurabilityTier, JazzClient, ObjectId, TransactionId};

use crate::error::{Error, Result};
use crate::handle::Jazz;
use crate::table::{Insert, Table};

/// What a write does once committed locally: on a [`Jazz::durable`]
/// handle, wait until it reached that tier.
pub(crate) struct Settler {
    durability: Option<DurabilityTier>,
}

impl Settler {
    pub(crate) async fn settle(
        &self,
        client: &JazzClient,
        written: Option<TransactionId>,
    ) -> Result<()> {
        match (self.durability, written) {
            (Some(tier), Some(written)) => Ok(client.wait_for_transaction(written, tier).await?),
            _ => Ok(()),
        }
    }
}

impl Jazz {
    /// The settler for a write about to be made: refused before writing
    /// when its tier is out of reach.
    pub(crate) fn settler(&self) -> Result<Settler> {
        if let Some(tier) = self.durability
            && tier != DurabilityTier::Local
            && self.info.serverless
        {
            return Err(Error::Config(format!(
                "{tier:?} durability is unreachable: the app runs without a server"
            )));
        }
        Ok(Settler {
            durability: self.durability,
        })
    }

    /// This handle, with writes that return only once they reached `tier`:
    /// `jazz.durable(DurabilityTier::GlobalServer).insert(…)` returns once
    /// the server accepted the row, or fails if it rejected it (a
    /// permission refused, say).
    pub fn durable(&self, tier: DurabilityTier) -> Jazz {
        Jazz {
            commands: self.commands.clone(),
            info: self.info.clone(),
            durability: Some(tier),
        }
    }

    /// Waits until every write committed so far (through any handle of
    /// this app) reached the server, failing if the server rejected one, as
    /// TS's `waitForSync` does. Without a server there is nothing to wait
    /// for.
    pub async fn wait_for_sync(&self) -> Result<()> {
        self.with_client(|client| async move { Ok(client.wait_for_sync().await?) })
            .await
    }

    /// Writes the row with this id: inserts it if there is none, otherwise
    /// replaces the columns `new` sets. Returns the row as stored.
    pub async fn upsert<N: Insert>(&self, id: ObjectId, new: N) -> Result<N::Row> {
        let implementations = self.info.implementations.clone();
        let settle = self.settler()?;
        self.with_client(move |client| async move {
            let cells = new.into_cells(&implementations)?;
            let written = client.upsert(<N::Row as Table>::NAME, *id.uuid(), cells)?;
            settle.settle(&client, written).await?;
            read_back::<N::Row>(&client, id, &implementations).await
        })
        .await
    }

    /// Brings back the deleted row of `T` with this id, as it was.
    pub async fn restore<T: Table>(&self, id: ObjectId) -> Result<T> {
        self.restore_cells::<T>(id, None).await
    }

    /// Brings back the deleted row with this id, with `new`'s values. A
    /// restore replaces the whole row, so it takes the full insert, as TS's
    /// `restore(table, id, data)` does.
    pub async fn restore_with<N: Insert>(&self, id: ObjectId, new: N) -> Result<N::Row> {
        let cells = new.into_cells(&self.info.implementations)?;
        self.restore_cells::<N::Row>(id, Some(cells.into_iter().collect()))
            .await
    }

    async fn restore_cells<T: Table>(
        &self,
        id: ObjectId,
        cells: Option<Vec<(String, jazz::tools::Value)>>,
    ) -> Result<T> {
        let implementations = self.info.implementations.clone();
        let settle = self.settler()?;
        self.with_client(move |client| async move {
            let written = client.restore(T::NAME, id, cells)?;
            settle.settle(&client, written).await?;
            read_back::<T>(&client, id, &implementations).await
        })
        .await
    }
}

pub(crate) async fn read_back<T: Table>(
    client: &JazzClient,
    id: ObjectId,
    implementations: &crate::table::Implementations,
) -> Result<T> {
    let query = crate::query::Query::<T>::all().id(id).into_untyped();
    let row = client
        .query(query, jazz::tools::ReadTier::LocalFirst)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| Error::Decode {
            table: T::NAME,
            message: format!("row {} is not there after writing it", id.uuid()),
        })?;
    crate::handle::decode_result(row, implementations)
}
