//! The reads and the transaction fate the client's encryption runtime
//! needs beside the hooks (E2EE protocol §0, "What Jazz provides", items 3
//! and 6; §5.4): ledger reads with accepted positions and a common cut,
//! provisional reads over this store's own pending writes, and the durable
//! fate of one of its commits.

use super::{BatchKind, Db, Error, ErrorCode, PreparedQuery, ReadOpts, ReadSpec, ReadTier, TxRef};
use crate::ids::NodeUuid;
use crate::values::CurrentRow;

/// A ledger read: rows of one cut, each with the authority position it was
/// accepted at.
#[derive(Clone, Debug, PartialEq)]
pub struct LedgerRead {
    /// Each row with its accepted position, never past `through`.
    pub rows: Vec<(CurrentRow, u64)>,
    /// The cut every row was read at: the transaction's snapshot.
    pub through: u64,
}

/// Where a provisional read's row stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Provisional {
    /// Accepted by the authority, at this position.
    Accepted(u64),
    /// Written by one of this store's own commits, still pending: the
    /// commit's transaction coordinate, never an accepted position.
    Own(u64),
}

/// A provisional read (protocol §0 item 6): what this store holds accepted
/// through `accepted_through`, and its own pending rows. Accepted rows
/// come first by position, then own rows by transaction.
#[derive(Clone, Debug, PartialEq)]
pub struct ProvisionalRead {
    /// The writer whose pending rows the read includes: this store.
    pub owner: NodeUuid,
    /// The authority position this store holds every accepted row
    /// through.
    pub accepted_through: u64,
    pub rows: Vec<(CurrentRow, Provisional)>,
}

/// The fate of one of this store's commits, as the store keeps it across
/// restarts.
#[derive(Clone, Debug)]
pub enum TransactionFate {
    /// Not settled yet: it waits to be uploaded or for the authority's
    /// answer.
    Pending,
    /// The authority accepted it, at this position.
    Accepted(u64),
    /// The authority refused it: why, while the store keeps the refusal
    /// ([`Db::track_upstream_rejections`]), else `None`.
    Refused(Option<Error>),
    /// The store knows of no such commit: never made, made before the store
    /// first connected upstream, or forgotten.
    Unknown,
}

impl TxRef<'_> {
    /// A ledger read of `prepared` in this serializable transaction: every
    /// row at the transaction's snapshot with its accepted position, and
    /// the snapshot as the common cut. A row not yet accepted (this store's
    /// own pending write) fails the read rather than be left out or given a
    /// position it does not have; so does any row the encryption hooks
    /// cannot open. The read joins the transaction's read set.
    pub async fn ledger_read(&self, prepared: &PreparedQuery) -> Result<LedgerRead, Error> {
        self.db.require_upstream("a ledger read")?;
        let serializable = matches!(
            self.db
                .inner
                .batches
                .borrow()
                .get(&self.id)
                .map(|batch| &batch.kind),
            Some(BatchKind::Serializable { .. })
        );
        if !serializable {
            return Err(Error::new(
                ErrorCode::Query,
                "a ledger read runs in a serializable transaction",
            ));
        }
        // Boxed: the read's state stays off the stack of the runtime that
        // polls it, whose own futures are deep already.
        let rows = Box::pin(self.all_prepared(prepared)).await?;
        let through = match self
            .db
            .inner
            .batches
            .borrow()
            .get(&self.id)
            .map(|batch| &batch.kind)
        {
            Some(BatchKind::Serializable { snapshot, .. }) => snapshot.seq,
            _ => return Err(Error::new(ErrorCode::Query, "the transaction is not open")),
        };
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let at = row.seq();
            if !at.accepted() || at.seq > through {
                return Err(Error::new(
                    ErrorCode::NotObserved,
                    format!(
                        "a ledger read of `{}` found a row not accepted at its cut",
                        prepared.query().table
                    ),
                ));
            }
            out.push((row, at.seq));
        }
        Ok(LedgerRead { rows: out, through })
    }
}

impl Db {
    /// A provisional read of `prepared` (protocol §0 item 6): the rows this
    /// store holds accepted, and its own pending writes, each marked which.
    /// It reads one table: joins, traversals, inheritance and relations are
    /// refused, as is a store that has never had an upstream. Rows the
    /// encryption hooks cannot open fail the read.
    pub async fn provisional_read(
        &self,
        prepared: &PreparedQuery,
    ) -> Result<ProvisionalRead, Error> {
        self.require_upstream("a provisional read")?;
        let query = prepared.query();
        if !query.joins.is_empty()
            || !query.reachable.is_empty()
            || !query.inherits.is_empty()
            || !query.includes.is_empty()
            || !query.array_subqueries.is_empty()
            || query.flat_join.is_some()
            || query.relation.is_some()
        {
            return Err(Error::new(
                ErrorCode::Query,
                format!(
                    "a provisional read of `{}` reads that table alone: joins, traversals and relations are not supported",
                    query.table
                ),
            ));
        }
        let _operation = self.write_operation().await;
        let inner = self.inner.without_overlay();
        let read = ReadSpec::of(&ReadOpts::for_read_tier(ReadTier::LocalOnly), inner).await?;
        let rows = Box::pin(inner.run_for_reader(prepared, None, &read, true)).await?;
        let accepted_through = inner.client().map_or(0, |client| client.known);
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let at = row.seq();
            let writer = row
                .writer
                .as_deref()
                .and_then(crate::db::sync::writer_of_bytes);
            let mark = if at.accepted() {
                Provisional::Accepted(at.seq)
            } else if let Some(tx) = inner.own_seq_at((at, writer)) {
                Provisional::Own(tx)
            } else {
                return Err(Error::new(
                    ErrorCode::NotObserved,
                    format!(
                        "a provisional read of `{}` found a pending row of another writer",
                        query.table
                    ),
                ));
            };
            out.push((row, mark));
        }
        // Accepted first by position, then own rows by transaction; a row's
        // id keeps one order among rows of one position.
        out.sort_by_key(|(row, mark)| (*mark, row.row_uuid()));
        Ok(ProvisionalRead {
            owner: inner.identity.node,
            accepted_through,
            rows: out,
        })
    }

    /// Refuse `what` on a store that has never had an upstream: its
    /// positions are its own, not the authority's.
    fn require_upstream(&self, what: &str) -> Result<(), Error> {
        if self.is_client() {
            return Ok(());
        }
        Err(Error::new(
            ErrorCode::NotObserved,
            format!("{what} needs the authority's positions: this store has never had an upstream"),
        ))
    }

    /// The fate of this store's own commit `tx` (a [`super::TxReceipt`] or
    /// a write handle's transaction), as kept across restarts.
    pub async fn transaction_fate(&self, tx: u64) -> Result<TransactionFate, Error> {
        let _operation = self.write_operation().await;
        let inner = self.inner.without_overlay();
        let me = *inner.identity.node.as_bytes();
        match inner.settled(me, tx).await? {
            Some(Some(at)) => return Ok(TransactionFate::Accepted(at.seq)),
            Some(None) => return Ok(TransactionFate::Refused(inner.rejected_error(tx).await?)),
            None => {}
        }
        Ok(if inner.unsettled_own().contains(&tx) {
            TransactionFate::Pending
        } else {
            TransactionFate::Unknown
        })
    }
}
