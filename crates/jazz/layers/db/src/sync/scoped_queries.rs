//! Bounded query witnesses for request-scoped existential traversals.
//!
//! Every lookup starts from a returned row and a declared scalar UUID index.
//! The authority evaluates the witness's read policy with the immutable request
//! claims. Exact versions then join the existing scoped evidence receipt, so a
//! trusted relay never reconstructs this request from its current shared cache.

use super::*;
use crate::query::{JoinTarget, JoinVia};
use crate::records::ValueType;
use crate::values::{Schema, Table};

fn uuid_column(table: &Table, column: &str) -> bool {
    column == "id"
        || table.column(column).is_some_and(|column| match &column.ty {
            ValueType::Uuid => true,
            ValueType::Nullable(inner) => matches!(inner.as_ref(), ValueType::Uuid),
            _ => false,
        })
}

/// Resolve and bound every traversed source before executing even an empty read.
pub(super) fn tables<'a>(
    schema: &'a Schema,
    root: &'a Table,
    joins: &[JoinVia],
) -> Result<Vec<&'a Table>, Error> {
    let mut tables = vec![root];
    let mut pending = vec![(root, joins)];
    while let Some((parent, joins)) = pending.pop() {
        for join in joins {
            let target = schema.table(&join.table)?;
            let source = join.source_column.as_deref().unwrap_or("id");
            if join.target == JoinTarget::Uncorrelated
                || join.source_lookup.is_some()
                || !parent.branch_by.is_empty()
                || !target.branch_by.is_empty()
                || !uuid_column(parent, source)
                || !uuid_column(target, &join.on_column)
                || (join.on_column != "id"
                    && !target
                        .indexes
                        .iter()
                        .any(|index| index.first().is_some_and(|first| first == &join.on_column)))
            {
                return Err(unsupported(
                    "scoped relation reads without an indexed scalar UUID correlation",
                ));
            }
            if tables.len() >= scoped_reads::MAX_EVIDENCE {
                return Err(unsupported(
                    "scoped relation query exceeds its bounded work limit",
                ));
            }
            tables.push(target);
            pending.push((target, &join.nested_joins));
        }
    }
    Ok(tables)
}

impl Inner {
    pub(super) fn scoped_query_images(
        &self,
        prepared: &PreparedQuery,
        identity: Option<AuthorSubject>,
        roots: &[Image],
        through: u64,
        budget: &mut usize,
    ) -> Result<Vec<Image>, Error> {
        let schema = self.effective_schema(prepared.schema.as_ref().unwrap_or(&self.owner));
        let root = schema.table(&prepared.query.table)?;
        tables(&schema, root, &prepared.query.joins)?;
        let mut pending = roots
            .iter()
            .map(|image| (root, &prepared.query.joins[..], image.clone()))
            .collect::<Vec<_>>();
        let mut images = Vec::new();
        while let Some((parent, joins, image)) = pending.pop() {
            for join in joins {
                *budget = budget.checked_sub(1).ok_or_else(|| {
                    unsupported("scoped relation evidence exceeds its bounded work limit")
                })?;
                let source = parent.stored_name(join.source_column.as_deref().unwrap_or("id"));
                let Some(Sql::Blob(value)) = image_cell(&image, source) else {
                    continue;
                };
                let id: [u8; 16] = value
                    .try_into()
                    .map_err(|_| Error::new(ErrorCode::Storage, "invalid scoped relation UUID"))?;
                let target = schema.table(&join.table)?;
                let mut lookup = prepared.clone();
                lookup.query = Query::from(join.table.clone()).filter(crate::query::eq(
                    crate::query::col(&join.on_column),
                    crate::query::lit(Value::Uuid(RowUuid::from_bytes(id).0)),
                ));
                lookup.query.limit = Some(*budget + 1);
                let read = ReadSpec {
                    cut: (through != self.seq).then(|| Cut::at(through)),
                    permission_cut: Some(through),
                    ..ReadSpec::default()
                };
                let rows =
                    self.with_candidate_sql_budget(budget, || self.run(&lookup, identity, &read))?;
                *budget = budget.checked_sub(rows.len()).ok_or_else(|| {
                    unsupported("scoped relation evidence exceeds its bounded work limit")
                })?;
                for row in rows {
                    let coordinate = ScopedReadRowV1 {
                        table: target.stored.clone(),
                        id: *row.id.as_bytes(),
                        key: Vec::new(),
                        seq: row.seq,
                    };
                    for found in self.scoped_evidence_image(&coordinate)? {
                        if !join.nested_joins.is_empty() {
                            pending.push((target, &join.nested_joins, found.clone()));
                        }
                        images.push(found);
                    }
                }
            }
        }
        Ok(images)
    }
}
