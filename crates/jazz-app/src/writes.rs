//! Durability: how far a write has gone before the call returns, and
//! waiting for the writes made so far to reach the server.

use jazz::tools::{DurabilityTier, JazzClient, ObjectId, TransactionId};

use crate::error::{Error, Result};
use crate::handle::Jazz;
use crate::table::{Insert, Table};

/// What a write does once committed locally: wait for its tier, or note
/// it for [`Jazz::wait_for_sync`].
pub(crate) struct Settler {
    durability: Option<DurabilityTier>,
    info: std::sync::Arc<crate::handle::Info>,
}

impl Settler {
    pub(crate) async fn settle(
        &self,
        client: &JazzClient,
        written: Option<TransactionId>,
    ) -> Result<()> {
        let Some(written) = written else {
            return Ok(());
        };
        match self.durability {
            Some(tier) => wait(client, written, tier, self.info.serverless).await,
            None => {
                if !self.info.serverless {
                    self.info.pending.lock().unwrap().push(written);
                }
                Ok(())
            }
        }
    }
}

async fn wait(
    client: &JazzClient,
    written: TransactionId,
    tier: DurabilityTier,
    serverless: bool,
) -> Result<()> {
    if serverless && tier != DurabilityTier::Local {
        return Err(Error::Config(format!(
            "{tier:?} durability is unreachable: the app runs without a server"
        )));
    }
    Ok(client.wait_for_transaction(written, tier).await?)
}

impl Jazz {
    pub(crate) fn settler(&self) -> Settler {
        Settler {
            durability: self.durability,
            info: self.info.clone(),
        }
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

    /// Waits until every write made through this app's handles so far has
    /// reached the server, failing with the first the server rejected.
    /// Without a server there is nothing to wait for.
    pub async fn wait_for_sync(&self) -> Result<()> {
        let pending = std::mem::take(&mut *self.info.pending.lock().unwrap());
        if pending.is_empty() {
            return Ok(());
        }
        let serverless = self.info.serverless;
        self.with_client(move |client| async move {
            for written in pending {
                wait(&client, written, DurabilityTier::GlobalServer, serverless).await?;
            }
            Ok(())
        })
        .await
    }

    /// Writes the row with this id: inserts it if there is none, otherwise
    /// replaces the columns `new` sets. Returns the row as stored.
    pub async fn upsert<N: Insert>(&self, id: ObjectId, new: N) -> Result<N::Row> {
        let implementations = self.info.implementations.clone();
        let settle = self.settler();
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
        let implementations = self.info.implementations.clone();
        let settle = self.settler();
        self.with_client(move |client| async move {
            let written = client.restore(T::NAME, id, None)?;
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
