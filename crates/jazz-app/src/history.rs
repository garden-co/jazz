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

use jazz::query::{self as q, OrderDirection, Predicate};
use jazz::records::Value as CoreValue;
use jazz::schema::history::{ACCEPTED_AT_COLUMN, HISTORY_PREFIX, TRANSACTION_ID_COLUMN};
use jazz::time::GlobalTime;
use jazz::tools::{ObjectId, TransactionId, Value};

use crate::error::Result;
use crate::include::Row;
use crate::query::{Order, Query};
use crate::table::{Fields, Table};
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
        let transaction_id = match fields.take_raw(TRANSACTION_ID_COLUMN)? {
            Value::Row { values, .. } => match values.as_slice() {
                [Value::Timestamp(time), Value::Uuid(node)] => {
                    TransactionId::from_committed_tx(jazz::tx::TxId::new(
                        jazz::time::TxTime(*time),
                        jazz::ids::NodeUuid(*node.uuid()),
                    ))
                }
                _ => return Err(fields.error(format!("{TRANSACTION_ID_COLUMN} holds {values:?}"))),
            },
            other => return Err(fields.error(format!("{TRANSACTION_ID_COLUMN} holds {other:?}"))),
        };
        let accepted_at = match fields.take_raw(ACCEPTED_AT_COLUMN)? {
            Value::Timestamp(hlc) => Timestamp(GlobalTime(hlc).physical_ms()),
            other => return Err(fields.error(format!("{ACCEPTED_AT_COLUMN} holds {other:?}"))),
        };
        Ok(Self {
            row,
            transaction_id,
            accepted_at,
        })
    }

    fn value_names() -> Vec<String> {
        let mut names = T::value_names();
        names.extend([TRANSACTION_ID_COLUMN, ACCEPTED_AT_COLUMN].map(str::to_owned));
        names
    }
}

impl<T: Table> Query<Version<T>> {
    /// Every accepted version of `T`'s rows. Bound it to rows with
    /// [`Query::id`] or an `id().is_in(…)` filter before reading it.
    pub fn history() -> Self {
        Self::from_parts(q::Query::from(format!("{HISTORY_PREFIX}{}", T::NAME)))
    }

    /// Only versions the authority accepted in `range` (of milliseconds
    /// since the Unix epoch): `since..`, `from..to`, `..=until`.
    pub fn accepted(mut self, range: impl RangeBounds<Timestamp>) -> Self {
        // An acceptance time is an HLC: milliseconds over a logical
        // counter. Every HLC of a millisecond lies from its first on to
        // the next millisecond's first.
        let first = match range.start_bound() {
            Bound::Included(at) => Some(at.0),
            Bound::Excluded(at) => Some(at.0.saturating_add(1)),
            Bound::Unbounded => None,
        };
        let end = match range.end_bound() {
            Bound::Included(at) => Some(at.0.saturating_add(1)),
            Bound::Excluded(at) => Some(at.0),
            Bound::Unbounded => None,
        };
        let column = || q::col(ACCEPTED_AT_COLUMN);
        let at = |hlc: u64| q::lit(CoreValue::U64(hlc));
        if let Some(first) = first {
            self.query.filters.push(match hlc(first) {
                Some(first) => q::gte(column(), at(first)),
                // Past the last time there is: no version.
                None => Predicate::Any(Vec::new()),
            });
        }
        // Past the last time there is, every version is before it.
        if let Some(end) = end.and_then(hlc) {
            self.query.filters.push(q::lt(column(), at(end)));
        }
        self
    }

    /// Orders the versions by when they were accepted: before or after the
    /// columns ordered so far.
    pub fn order_by_accepted_at(mut self, order: Order) -> Self {
        self.query.order_by.push(q::OrderBy {
            column: ACCEPTED_AT_COLUMN.to_owned(),
            direction: match order {
                Order::Asc => OrderDirection::Asc,
                Order::Desc => OrderDirection::Desc,
            },
        });
        self
    }
}

/// The first HLC of millisecond `ms`; `None` past the last one there is.
fn hlc(ms: u64) -> Option<u64> {
    GlobalTime::new(ms, 0).map(|time| time.0)
}
