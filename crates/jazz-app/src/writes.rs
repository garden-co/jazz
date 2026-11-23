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
    /// A settler that only commits locally.
    pub(crate) fn local() -> Settler {
        Settler { durability: None }
    }

    pub(crate) async fn settle(
        &self,
        client: &JazzClient,
        written: Option<TransactionId>,
    ) -> Result<()> {
        if written.is_some() {
            note_committed();
        }
        match (self.durability, written) {
            (Some(tier), Some(written)) => {
                // This wait handles the write's refusal: it isn't also a
                // mutation error, unless its caller stops waiting first.
                crate::lifecycle::claim(written);
                let waited = client.observe_transaction(written, tier).await;
                if waited
                    .as_ref()
                    .is_err_and(|error| error.is_transaction_rejection())
                {
                    // Whether it was handled is known once the reply is
                    // sent: see `Heard`.
                    Heard::later(client, written);
                } else {
                    // A wait that ended otherwise (the driver stopped, the
                    // write unknown) handled nothing, so a refusal is a
                    // mutation error.
                    crate::lifecycle::unclaim(written);
                }
                Ok(waited?)
            }
            _ => Ok(()),
        }
    }
}

tokio::task_local! {
    static REFUSALS: Heard;
}

/// The refusals a job's durable waits reported, settled once its reply is
/// sent: a caller that heard them handled them, while one that stopped
/// waiting (its call dropped, as `tokio::time::timeout` drops it) loses no
/// bookkeeping. Its refusals go to `mutation_errors()` and stay for the
/// next `wait_for_sync()`, as a plain write's do.
#[derive(Clone, Default)]
pub(crate) struct Heard(std::rc::Rc<CallState>);

#[derive(Default)]
pub(crate) struct CallState {
    refusals: std::cell::RefCell<Vec<(JazzClient, TransactionId)>>,
    /// The call committed a write locally.
    committed: std::cell::Cell<bool>,
}

/// The call running now committed a write locally: cut short, it is
/// `StillQueued`. For writes not made through [`Settler`] (the bindings'
/// own). Not public API.
#[doc(hidden)]
pub fn note_committed() {
    let _ = REFUSALS.try_with(|heard| heard.0.committed.set(true));
}

impl Heard {
    /// Runs `job` with its refusals collected here.
    pub(crate) async fn collect<T>(&self, job: impl std::future::Future<Output = T>) -> T {
        REFUSALS.scope(self.clone(), job).await
    }

    /// `written`'s refusal is decided by whether the reply reaches its
    /// caller; outside a job, it is heard at once.
    fn later(client: &JazzClient, written: TransactionId) {
        let kept = REFUSALS.try_with(|heard| {
            heard
                .0
                .refusals
                .borrow_mut()
                .push((client.clone(), written));
        });
        if kept.is_err() {
            crate::lifecycle::heard(client, written);
        }
    }

    /// Whether the call committed a write locally.
    pub(crate) fn committed(&self) -> bool {
        self.0.committed.get()
    }

    /// The job's reply was sent: `heard` if its caller was still there.
    pub(crate) fn replied(&self, heard: bool) {
        for (client, written) in self.0.refusals.borrow_mut().drain(..) {
            if heard {
                crate::lifecycle::heard(&client, written);
            } else {
                crate::lifecycle::unclaim(written);
            }
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
            durability: Some(tier),
            ..self.clone()
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
    let query = crate::query::Query::<T>::all().id(id).into_untyped()?;
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
