//! Repair of the rows a refused or lost creation left under its key
//! (`E2EE_PROTOCOL.md` §3.2).
//!
//! A creation whose root did not become the space's root leaves this device
//! with a key no other member will use, and with rows it wrote under it,
//! which every other member reports `invalid-ciphertext`. The device keeps
//! that key for good (it still opens those rows), obtains the space's real
//! key by delivery, and then rewrites the cells that still name the losing
//! epoch under the real key, in serializable batches that read the rows
//! they rewrite: a concurrent edit, move or delete, or a second repairer,
//! changes a row the batch read and the batch conflicts and starts again
//! from a fresh read, so nothing is ever overwritten.
//!
//! The rewrite is an ordinary update of the cell's plaintext, which the
//! hooks seal under the newest key of the chain's root with a fresh
//! equality token; which cells are still under the losing key is what the
//! read of that batch reports (the hooks note every cell that opened under a
//! refused creation's key). A repair that cannot proceed (no real key yet,
//! rows the authority has not accepted yet) does nothing and is tried again
//! at the next natural moment.

use std::collections::BTreeMap;

use jazz::db::{Db, ErrorCode, ReadOpts, ReadTier, Reason};
use jazz::e2ee::{E2eeRefusal, SpaceAddress};
use jazz::ids::RowUuid;
use jazz::query::{Operand, Predicate, Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::OpenTransactionId;
use jazz_e2ee::protocol::content::{KeyMark, WriteChoice};
use jazz_e2ee::protocol::space::members::DELIVERY_BATCH;

use super::{ATTEMPTS, protocol_address};
use crate::client::E2eeClient;
use crate::error::ClientError;
use crate::state::{Collector, Repairable};

/// Rows one repair transaction reads and rewrites (protocol §3.2).
const REPAIR_BATCH: usize = DELIVERY_BATCH;

/// Rounds of finding and rewriting one space gets in a call: a bound, so a
/// server that keeps changing the rows cannot keep a repair busy forever.
const ROUNDS: usize = 20;

/// The collector of one repair read: registered for the duration of the
/// read (the hooks note into every open collector), and removed when the read
/// is done, however it ends.
struct Collecting<'a> {
    shared: &'a crate::state::Shared,
    id: uuid::Uuid,
    notes: Collector,
}

impl<'a> Collecting<'a> {
    fn start(shared: &'a crate::state::Shared) -> Self {
        let id = uuid::Uuid::new_v4();
        let notes = Collector::default();
        shared.collectors.borrow_mut().insert(id, notes.clone());
        Self { shared, id, notes }
    }

    fn take(&self) -> Repairable {
        std::mem::take(&mut *self.notes.borrow_mut())
    }
}

impl Drop for Collecting<'_> {
    fn drop(&mut self) {
        self.shared.collectors.borrow_mut().remove(&self.id);
    }
}

impl E2eeClient {
    /// Whether this device holds a key of `space` that is `refused`.
    pub(crate) fn holds_refused_key(&self, space: &SpaceAddress) -> bool {
        let Ok(address) = protocol_address(space) else {
            return false;
        };
        self.shared
            .keys
            .borrow()
            .held
            .roots_of(&address)
            .iter()
            .any(|held| held.held().any(|(_, mark)| *mark == KeyMark::Refused))
    }

    /// Rewrites, under the space's real key, the cells of `space` that this
    /// device wrote under the key of a refused or lost creation (protocol
    /// §3.2). It reads this device's own store to find the rows (including
    /// its own writes not yet accepted), then rewrites them in serializable
    /// transactions of at most 100 rows that each read the rows they
    /// rewrite; only cells that still name the losing epoch are written. It
    /// waits, returning what it has written so far, while the device holds no
    /// real key of the space, while the authority has not accepted a row
    /// yet, or while it is offline. If the authority refuses the rewrite (the
    /// device may not update the rows it wrote: the application's rules
    /// decide) the natural moments stop repairing the space for the rest of the
    /// run, and this call, which always attempts, returns the refusal. How
    /// many rows it rewrote.
    pub async fn repair_space(&self, db: &Db, space: &SpaceAddress) -> Result<usize, ClientError> {
        self.ensure_open()?;
        if !self.holds_refused_key(space) {
            return Ok(0);
        }
        let address = protocol_address(space)?;
        let mut written = 0;
        for _ in 0..ROUNDS {
            // Without a real key the rewrite would be refused: wait.
            let ready = {
                let keys = self.shared.keys.borrow();
                matches!(keys.held.write_key(&address, false), WriteChoice::Key(_))
            };
            if !ready || !self.shared.keys.borrow().online {
                return Ok(written);
            }
            // A rewrite seals like any write: a space whose mark this runtime
            // has not evaluated is read first, and rotated if a removed device
            // may hold its version (protocol §3.5).
            let unevaluated = self.shared.keys.borrow().needs_evaluation(&address);
            if unevaluated {
                self.make_ready(db, space, None, None).await?;
            }
            let found = self.repairable_rows(db, space).await?;
            if found.is_empty() {
                return Ok(written);
            }
            let mut progressed = false;
            for (table, rows) in found {
                for batch in rows.chunks(REPAIR_BATCH) {
                    match self.repair_batch(db, space, &table, batch).await {
                        Ok(count) => {
                            written += count;
                            progressed |= count > 0;
                        }
                        // Rows not accepted yet, or a key still to come:
                        // the next natural moment.
                        Err(ClientError::Database(error))
                            if error.code == ErrorCode::NotObserved
                                || E2eeRefusal::of(&error).is_some() =>
                        {
                            return Ok(written);
                        }
                        // The authority refuses the rewrite: this device may
                        // not update the rows it wrote. The natural moments
                        // stop trying while the client runs; an explicit
                        // call attempts again.
                        Err(ClientError::Database(error))
                            if error.reason == Some(Reason::AuthorizationDenied) =>
                        {
                            self.shared
                                .keys
                                .borrow_mut()
                                .repair_refused
                                .insert(address.clone());
                            return Err(ClientError::Database(error));
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            if !progressed {
                return Ok(written);
            }
        }
        Ok(written)
    }

    /// [`E2eeClient::repair_space`] for the natural moments: a space whose
    /// repair the authority refused is left alone while the client runs.
    pub(crate) async fn repair_auto(
        &self,
        db: &Db,
        space: &SpaceAddress,
    ) -> Result<usize, ClientError> {
        let address = protocol_address(space)?;
        if self.shared.keys.borrow().repair_refused.contains(&address) {
            return Ok(0);
        }
        self.repair_space(db, space).await
    }

    /// The query for the rows of `table` that belong to `space`.
    fn rows_of_space(&self, table: &str, space: &SpaceAddress) -> Result<Query, ClientError> {
        let plan = self
            .shared
            .plans
            .get(table)
            .ok_or_else(|| ClientError::Unsupported(format!("`{table}` is not encrypted")))?;
        let column = if plan.self_space() {
            "id"
        } else {
            plan.entry.space.as_str()
        };
        let id = uuid::Uuid::parse_str(&space.identifier).map_err(|_| {
            ClientError::Unsupported("a space identifier that is not a UUID".to_owned())
        })?;
        Ok(Query::from(table).filter(eq(col(column), lit(Value::Uuid(id)))))
    }

    /// The rows of `space` with a cell under a refused creation's key, as
    /// this device's own store shows them: a read that opens every cell it
    /// can, so the hooks note the ones under that key, table by table.
    async fn repairable_rows(
        &self,
        db: &Db,
        space: &SpaceAddress,
    ) -> Result<BTreeMap<String, Vec<RowUuid>>, ClientError> {
        let mut found: BTreeMap<String, Vec<RowUuid>> = BTreeMap::new();
        for table in self.tables_of(space) {
            let collector = Collecting::start(&self.shared);
            let prepared = db.prepare_query(&self.rows_of_space(&table, space)?)?;
            db.all(&prepared, ReadOpts::for_read_tier(ReadTier::LocalOnly))
                .await?;
            let noted = collector.take();
            let mut rows: Vec<RowUuid> = noted
                .into_iter()
                .filter(|((noted_table, _), (noted_space, _))| {
                    *noted_table == table && noted_space == space
                })
                .map(|((_, row), _)| row)
                .collect();
            rows.sort();
            if !rows.is_empty() {
                found.insert(table, rows);
            }
        }
        Ok(found)
    }

    /// One repair transaction: reads `rows` of `table` (the rows are in its
    /// read set), and rewrites the cells that still name the losing epoch.
    /// A conflict reads again. How many rows it rewrote.
    async fn repair_batch(
        &self,
        db: &Db,
        space: &SpaceAddress,
        table: &str,
        rows: &[RowUuid],
    ) -> Result<usize, ClientError> {
        crate::account::serializable(db, ATTEMPTS, async |tx| {
            self.repair_step(db, tx, space, table, rows).await
        })
        .await
    }

    async fn repair_step(
        &self,
        db: &Db,
        tx: OpenTransactionId,
        space: &SpaceAddress,
        table: &str,
        rows: &[RowUuid],
    ) -> Result<usize, ClientError> {
        let ids: Vec<Operand> = rows
            .iter()
            .map(|row| Operand::Literal(Value::Uuid(row.0)))
            .collect();
        let collector = Collecting::start(&self.shared);
        let query = self
            .rows_of_space(table, space)?
            .filter(Predicate::In(Operand::Column("id".to_owned()), ids));
        let prepared = db.prepare_query(&query)?;
        let read = db.tx_ref(tx).ledger_read(&prepared).await?;
        let noted = collector.take();
        let mut rewritten = 0;
        for (row, _) in &read.rows {
            let uuid = row.row_uuid();
            let Some((_, columns)) = noted.get(&(table.to_owned(), uuid)) else {
                continue;
            };
            let mut cells = BTreeMap::new();
            for column in columns {
                if let Some(value) = row.cell(column) {
                    cells.insert(column.clone(), value);
                }
            }
            if cells.is_empty() {
                continue;
            }
            db.tx_ref(tx)
                .update(table, uuid, cells, Default::default())
                .await?;
            rewritten += 1;
        }
        if rewritten > 0 {
            self.publish(db, tx).await?;
        }
        Ok(rewritten)
    }
}
