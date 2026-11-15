//! History reads: the accepted versions of a table's rows, as TypeScript's
//! `app.$history.<table>` reads them.
//!
//! A history query reads `history.<table>`: each accepted content version
//! of a row, the current one and those it superseded, with the transaction
//! that wrote it and when the authority accepted it. Bound it to a row
//! ([`Query::id`]) or a list of rows (`id().is_in(…)`); filters, order and
//! window then apply to the versions.
//!
//! ```ignore
//! // Query<Version<Todo>>, oldest version first
//! let versions = jazz
//!     .all(todos::history().id(todo.id).order_by_accepted_at(Order::Asc))
//!     .await?;
//! for version in versions {
//!     println!("{} at {:?}: {}", version.transaction_id, version.accepted_at, version.row.title);
//! }
//! ```
//!
//! History reads cannot include, hop, gather or union, as in TypeScript;
//! subscribing to them is not supported yet.

use std::ops::{Bound, RangeBounds};

use jazz::schema::history::{ACCEPTED_AT_COLUMN, TRANSACTION_ID_COLUMN};
use jazz::tools::{ObjectId, TransactionId};

use crate::error::Result;
use crate::include::Row;
use crate::query::{Order, Query, table};
use crate::table::{Fields, Table};
use crate::untyped;
use crate::value::Timestamp;

/// One accepted version of a row of `T`: its values then, the transaction
/// that wrote them, and when the authority accepted it.
#[derive(Debug, Clone, PartialEq)]
pub struct Version<T> {
    pub row: T,
    /// The transaction that wrote this version, as a write reports it.
    pub transaction_id: TransactionId,
    /// When the authority accepted the version.
    pub accepted_at: Timestamp,
}

impl<T: Table> Row for Version<T> {
    type Table = T;

    fn row_id(&self) -> ObjectId {
        self.row.id()
    }

    fn decode_fields(fields: &mut Fields<'_>) -> Result<Self> {
        let row = T::decode(fields)?;
        let value = fields.take_raw(TRANSACTION_ID_COLUMN)?;
        let transaction_id = untyped::transaction_id(&value)
            .ok_or_else(|| fields.error(format!("{TRANSACTION_ID_COLUMN} holds {value:?}")))?;
        let value = fields.take_raw(ACCEPTED_AT_COLUMN)?;
        let accepted_at = untyped::accepted_at(&value)
            .ok_or_else(|| fields.error(format!("{ACCEPTED_AT_COLUMN} holds {value:?}")))?;
        Ok(Self {
            row,
            transaction_id,
            accepted_at,
        })
    }

    fn value_names() -> Vec<String> {
        let mut names = T::value_names();
        names.extend(untyped::HISTORY_FIELDS.map(str::to_owned));
        names
    }
}

impl<T: Table> Query<Version<T>> {
    /// Every accepted version of `T`'s rows. Bound it to rows with
    /// [`Query::id`] or an `id().is_in(…)` filter before reading it.
    pub fn history() -> Self {
        Self::wrap(untyped::Query::history(table::<T>()))
    }

    /// Only versions the authority accepted in `range` (of milliseconds
    /// since the Unix epoch): `since..`, `from..to`, `..=until`.
    pub fn accepted(self, range: impl RangeBounds<Timestamp>) -> Self {
        let bound = |bound: Bound<&Timestamp>| bound.map(|at| at.0);
        let (from, to) = (bound(range.start_bound()), bound(range.end_bound()));
        self.map(|inner| inner.accepted(from, to))
    }

    /// Orders the versions by when they were accepted: before or after the
    /// columns ordered so far.
    pub fn order_by_accepted_at(self, order: Order) -> Self {
        self.map(|inner| inner.order_by_accepted_at(order.direction()))
    }
}
