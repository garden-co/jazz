//! History relations: `history.<table>` lists the accepted content versions
//! of rows named by id, current and superseded, with their transaction
//! identity and acceptance time.
//!
//! Versions come from the table and its `__h` table. A version is listed
//! when its commit wrote content (`_st` without
//! [`crate::values::STATE_NO_CONTENT`]; deletes and restores without
//! content are not versions), it is no merged text write's value as sent
//! ([`crate::values::STATE_AS_SENT`]) and the authority accepted it (`_seq`
//! below [`crate::db::LOCAL_BASE`]: a replica's pending writes are not). Access
//! follows the current row: its read policy, in its branch, gates every
//! version of it, and a deleted current row hides its history unless the
//! read includes deleted rows.
//!
//! `$transactionId` is the `(tx, node)` the writer committed under, as
//! `_jazz_transactions` records it for an upload. The authority's own commits
//! are not recorded there: they are `(seq, authority node)`. A replica
//! lists only versions whose transaction it knows. `$acceptedAt` is the
//! authority's stamp for the commit (ms, distinct per commit) as an HLC
//! with a zero logical counter.

use crate::schema::history::{ACCEPTED_AT_COLUMN, HistoryMetadataColumn, TRANSACTION_ID_COLUMN};

use super::*;

/// Bits below an HLC's physical milliseconds (its logical counter).
const HLC_COUNTER_BITS: u32 = 18;

impl Ctx<'_> {
    /// A `history.<source>` read. See the module docs.
    pub(super) fn compile_history(
        mut self,
        query: &Query,
        source: &str,
        include_deleted: bool,
    ) -> Result<Compiled, Error> {
        if !query.joins.is_empty()
            || !query.includes.is_empty()
            || !query.array_subqueries.is_empty()
            || !query.reachable.is_empty()
        {
            return Err(history_unsupported("joins or relationships"));
        }
        if let Some(aggregate) = &query.aggregate {
            let metadata = |column: &str| {
                HistoryMetadataColumn::ALL
                    .iter()
                    .any(|m| m.name() == column)
            };
            let reads_metadata = aggregate.group_by.as_deref().is_some_and(metadata)
                || aggregate
                    .aggregates
                    .iter()
                    .any(|output| output.column.as_deref().is_some_and(metadata));
            if reads_metadata {
                return Err(history_unsupported("aggregates over history metadata"));
            }
        }
        let Some(bound) = query.filters.iter().find_map(bounding_id) else {
            return Err(history_unsupported(
                "history without an id equality or IN constraint",
            ));
        };
        let names_transaction = |predicate: &Predicate| {
            predicate.any_operand(&mut |operand| {
                matches!(operand, Operand::Column(column) if column == TRANSACTION_ID_COLUMN)
            })
        };
        if query.filters.iter().any(names_transaction) {
            return Err(history_unsupported(
                "user-provided $transactionId predicates",
            ));
        }
        if query
            .order_by
            .iter()
            .any(|order| order.column == TRANSACTION_ID_COLUMN)
        {
            return Err(history_unsupported("ordering by $transactionId"));
        }
        let (source, versions) =
            self.history_source(source, HistoryBound::Filter(bound), include_deleted)?;
        let versions = &versions;
        let projection = projection(versions, query.select.as_deref())?;

        let alias = "t";
        let mut wheres = Vec::new();
        for filter in &query.filters {
            wheres.push(self.predicate(filter, versions, alias)?);
        }
        if let Some(aggregate) = &query.aggregate {
            // Every listed version contributes, current or superseded.
            return self.compile_aggregate(query, aggregate, versions, &source, alias, &wheres);
        }
        let order = self.order_sql(versions, alias, &query.order_by)?;
        // Versions of one row share its id: their seq orders them.
        let tie = match query.order_by.last().map(|key| key.direction) {
            Some(OrderDirection::Desc) => "DESC",
            _ => "ASC",
        };
        let order = format!("{order}, {alias}._seq {tie}");
        let columns = projection
            .iter()
            .map(|(name, _)| Ok(format!(", {}", column_sql(versions, alias, name)?)))
            .collect::<Result<String, Error>>()?;
        let limit = self.bind(Sql::Integer(query.limit.map_or(-1, |l| l as i64)));
        let offset = self.bind(Sql::Integer(query.offset as i64));
        let statement = |order: &str| {
            format!(
                "SELECT {alias}.id, {alias}._seq, {alias}._st, {alias}.{} \
                 {columns} FROM {source} AS {alias} WHERE {} ORDER BY {order} LIMIT {limit} \
                 OFFSET {offset}",
                quote(TRANSACTION_ID_COLUMN),
                and(&wheres),
            )
        };
        Ok(Compiled {
            sql: statement(&order),
            params: self.params,
            projection,
            tables: self.tables,
            aggregate: false,
            joined: 0,
            versioned: true,
            union: false,
            partial: Vec::new(),
        })
    }
    /// The versions of the rows of `source` that `bound` names, as a SQL
    /// source with the table's columns plus the history metadata, and the
    /// table they read as. Every version is gated by its current row's
    /// read policy in its branch (and liveness, unless `include_deleted`).
    pub(super) fn history_source(
        &mut self,
        source: &str,
        bound: HistoryBound<'_>,
        include_deleted: bool,
    ) -> Result<(String, Table), Error> {
        if self.cut.is_some() && !self.settled_only {
            return Err(history_unsupported("history at a snapshot"));
        }
        if self.view.as_ref().is_some_and(|view| view.base.is_some()) {
            return Err(history_unsupported("history with a branch base"));
        }
        let table = self.table(source)?;
        let versions = versions_table(table);
        let mut stored = String::new();
        for column in table.stored() {
            stored.push_str(&format!(", {}", quote(&column.stored)));
        }
        let (current_bound, history_bound) = match bound {
            HistoryBound::Filter(bound) => (
                self.predicate(bound, &versions, "c")?,
                self.predicate(bound, &versions, "h")?,
            ),
            HistoryBound::Ids(ids) => (format!("c.id IN ({ids})"), format!("h.id IN ({ids})")),
        };
        let name = quote(&table.stored);
        let history = quote(&format!("{}__h", table.stored));
        let mut gate = vec!["a.id = v.id".to_owned(), "a._b = v._b".to_owned()];
        let mut keep = vec![
            format!("(v._st & {}) = 0", STATE_NO_CONTENT | STATE_AS_SENT),
            format!("v._seq < {}", crate::db::LOCAL_BASE),
        ];
        if let Some(view) = &self.view
            && !table.branch_by.is_empty()
        {
            let head = self.bind(Sql::Blob(table.selector_key(&view.head)?));
            keep.push(format!("v._b = {head}"));
        }
        if !include_deleted {
            gate.push(format!("(a._st & {STATE_DELETED}) = 0"));
        }
        if let Some(session) = self.policy {
            gate.push(self.read_policy(table, "a", session)?);
        }
        keep.push(format!(
            "EXISTS (SELECT 1 FROM {name} AS a WHERE {})",
            and(&gate)
        ));
        let recorded = "(SELECT unhex(printf('%016x', s.tx) || hex(n.node)) \
                        FROM _jazz_transactions AS s JOIN _jazz_nodes AS n ON n.n = s.n \
                        WHERE s.seq = v._seq)";
        let transaction = match self.history_origin {
            Some(node) => format!(
                "COALESCE({recorded}, unhex(printf('%016x', v._seq) || '{}'))",
                hex(&node)
            ),
            None => {
                keep.push(format!("{recorded} IS NOT NULL"));
                recorded.to_owned()
            }
        };
        let accepted =
            format!("(COALESCE(v.\"$updatedAt\", v.\"$createdAt\") << {HLC_COUNTER_BITS})");
        let mut deletion = String::new();
        for column in ["$deletedAt", "$deletedBy"] {
            deletion.push_str(&format!(
                ", (SELECT {} FROM {name} AS a WHERE a.id = v.id AND a._b = v._b) AS {}",
                deletion_sql(table, "a", column),
                quote(column)
            ));
        }
        let source = format!(
            "(SELECT v.id, v._b, v._seq, 0 AS _st{v_stored}, {transaction} AS {}, \
             {accepted} AS {}{deletion} FROM (SELECT id, _b, _seq, _st{stored} FROM {name} AS c \
             WHERE {current_bound} UNION ALL SELECT id, _b, _seq, _st{stored} FROM {history} AS h \
             WHERE {history_bound}) AS v WHERE {})",
            quote(TRANSACTION_ID_COLUMN),
            quote(ACCEPTED_AT_COLUMN),
            and(&keep),
            v_stored = stored.replace(", ", ", v."),
        );

        Ok((source, versions))
    }
}

/// The table a history source of `table` reads as: its columns plus the
/// history metadata, under the source's name, each version carrying its
/// current row's deletion.
pub(super) fn versions_table(table: &Table) -> Table {
    let mut versions = table.clone();
    for metadata in HistoryMetadataColumn::ALL {
        let ty = match metadata {
            HistoryMetadataColumn::TransactionId => crate::tx::TxId::value_type(),
            HistoryMetadataColumn::AcceptedAt => ValueType::U64,
        };
        versions.columns.push(crate::values::Column {
            name: metadata.name().to_owned(),
            stored: metadata.name().to_owned(),
            ty,
            default: None,
            json: false,
            json_type: None,
            merge: Default::default(),
        });
    }
    // Every version reads its current row's deletion (see
    // `deletion_sql`), carried as columns of the version.
    for column in ["$deletedAt", "$deletedBy"] {
        versions.stored_provenance.push(crate::values::Column {
            name: column.to_owned(),
            stored: column.to_owned(),
            ty: ValueType::Nullable(Box::new(ValueType::U64)),
            default: None,
            json: false,
            json_type: None,
            merge: Default::default(),
        });
    }
    versions
}

/// What bounds a history source to named rows.
pub(super) enum HistoryBound<'p> {
    /// A filter `id = v` or `id IN (...)` on the source ([`bounding_id`]).
    Filter(&'p Predicate),
    /// A SQL query of the row ids.
    Ids(String),
}

/// The filter that bounds a history read to supplied row ids: `id = v` or
/// `id IN (...)`, at the top level or inside a conjunction.
pub(super) fn bounding_id(predicate: &Predicate) -> Option<&Predicate> {
    let supplied = |value: &Operand| matches!(value, Operand::Literal(_) | Operand::Param(_));
    match predicate {
        Predicate::All(predicates) => predicates.iter().find_map(bounding_id),
        // Validation canonicalizes an empty IN list to false.
        Predicate::Any(predicates) if predicates.is_empty() => Some(predicate),
        Predicate::Eq(Operand::Column(column), value)
        | Predicate::Eq(value, Operand::Column(column))
            if column == "id" && supplied(value) =>
        {
            Some(predicate)
        }
        Predicate::In(Operand::Column(column), values)
            if column == "id" && values.iter().all(supplied) =>
        {
            Some(predicate)
        }
        _ => None,
    }
}

/// The error for a history shape that is not supported yet.
fn history_unsupported(pattern: &str) -> Error {
    query_error(format!(
        "unsupported relation query: Reading {pattern} is not supported in history queries yet"
    ))
}
