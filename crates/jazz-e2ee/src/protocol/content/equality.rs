//! Equality-filtered queries (`E2EE_PROTOCOL.md` §4.5, H4).
//!
//! An encrypted predicate must be `eq` (a complete enum `match` is an `eq`
//! of the whole value) on a column with an equality index, together with a
//! plaintext `eq` on the space column. Tags are per key version, so a query
//! computes one token per held version and the physical predicate becomes
//! `__e2ee_eq_<column> IN tokens`; candidates are decrypted and compared
//! before limit and offset apply. Offline no tokens are computed. Online,
//! a space with no held version cannot be queried by index: planning
//! reports the missing key rather than an empty `IN ()`. Other shapes are
//! refused, never answered by a scan.

use crate::agility::EqualityTagger;
use crate::records::{self, EqualityIdentity, EqualityValue};

use super::super::domain::SharedDomain;
use super::super::records::space::SpaceAddress;
use super::held::HeldKeys;
use super::{ContentError, ContentResult, EncryptedColumn};

/// A query shape encrypted columns do not support (§4.5, H4). Each names
/// the operation refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnsupportedQuery {
    /// `eq` on an encrypted column without an equality index.
    UnindexedEquality,
}

impl UnsupportedQuery {
    pub const fn name(self) -> &'static str {
        match self {
            Self::UnindexedEquality => "eq on an encrypted column without an equality index",
        }
    }
}

impl From<UnsupportedQuery> for ContentError {
    fn from(value: UnsupportedQuery) -> Self {
        Self::UnsupportedQuery(value)
    }
}

/// The table queried, in one space: the shared domain `Dₛ`, the space of
/// the query's plaintext `eq` on the space column, and Jazz's catalogue
/// identity of the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableTarget<'a> {
    pub domain: &'a SharedDomain,
    pub space: &'a SpaceAddress,
    pub table_id: &'a str,
}

impl TableTarget<'_> {
    pub(crate) fn equality<'c>(
        &'c self,
        column_id: &'c str,
        epoch_id: &'c str,
    ) -> EqualityIdentity<'c> {
        EqualityIdentity {
            application: self.domain.as_str(),
            scope_id: &self.space.scope_id,
            identifier: &self.space.identifier,
            table_id: self.table_id,
            column_id,
            epoch_id,
        }
    }
}

/// The tokens one encrypted predicate matches: one per held version, in
/// `__e2ee_eq_<column> IN tokens`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTokens {
    /// Distinct tokens, ascending.
    pub tokens: Vec<Vec<u8>>,
    /// The held epochs the tokens were computed for, ascending: the
    /// version set the query was planned with. When it changes during the
    /// query, the query is planned again (§4.5).
    pub epochs: Vec<String>,
    /// The chain the device last read has versions it does not hold: the
    /// result may miss their rows, and the space is reported once
    /// (`key-pending` for a member, `key-unavailable` otherwise, §4.5).
    pub incomplete: bool,
}

/// How one encrypted predicate is executed (H4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryPlan {
    Tokens(QueryTokens),
    /// Offline: no tokens; every candidate of the space is decrypted and
    /// compared.
    Offline,
    /// Online with no held version of the space: acquire within the read's
    /// budget and plan again (§4.5, "Cold queries").
    NeedsKey,
}

/// Plans `eq(column, value)` in `table` (§4.5): one token per held version
/// of the space, of any mark, computed with `tagger` (the selected equality
/// mechanism). A column without an equality index is refused; a value of
/// another type than the column's is an invalid input.
pub fn plan_equality(
    tagger: &dyn EqualityTagger,
    held: &HeldKeys,
    table: &TableTarget<'_>,
    column: &EncryptedColumn<'_>,
    value: &EqualityValue<'_>,
    online: bool,
) -> ContentResult<QueryPlan> {
    let index = column.equality.ok_or(UnsupportedQuery::UnindexedEquality)?;
    index.encode(value)?;
    if !online {
        return Ok(QueryPlan::Offline);
    }
    let versions = held.versions_of(table.space);
    if versions.is_empty() {
        return Ok(QueryPlan::NeedsKey);
    }
    let mut tokens = Vec::with_capacity(versions.len());
    let mut epochs = Vec::with_capacity(versions.len());
    for (epoch_id, key) in versions {
        tokens.push(records::equality_token_of(
            tagger,
            key,
            &table.equality(column.column_id, epoch_id),
            column.cell_type,
            &index,
            value,
        )?);
        epochs.push(epoch_id.to_owned());
    }
    tokens.sort();
    tokens.dedup();
    epochs.sort();
    epochs.dedup();
    Ok(QueryPlan::Tokens(QueryTokens {
        tokens,
        epochs,
        incomplete: held.incomplete(table.space),
    }))
}
