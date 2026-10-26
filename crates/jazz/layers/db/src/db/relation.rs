//! The relation facade's ordinary row set and retained output projection.

use std::collections::BTreeSet;

use crate::options::Error;
use crate::query::{Query, RelationProjectExpr, RelationRowIdRef};
use crate::values::{Schema, query_error, unsupported};

#[derive(Clone, Debug)]
pub(crate) struct OutputColumn {
    pub source: String,
    pub alias: String,
}

pub(crate) fn is_union(query: &Query) -> bool {
    query
        .relation
        .as_ref()
        .is_some_and(|relation| crate::query::relation_union_parts(&relation.rel).is_some())
}

/// Sources and names of a non-UNION relation's terminal projection. Row
/// identity stays separate even when the projection gives it another name.
pub(crate) fn columns(query: &Query) -> Result<Option<Vec<OutputColumn>>, Error> {
    let Some(relation) = &query.relation else {
        return Ok(None);
    };
    if is_union(query) {
        return Ok(None);
    }
    let (scope, columns) = crate::query::relation_output_projection(relation)
        .map_err(|error| query_error(error.to_string()))?;
    crate::query::reject_reserved_relation_aliases(&columns)
        .map_err(|error| query_error(error.to_string()))?;
    columns
        .into_iter()
        .map(|column| {
            let source = match column.expr {
                RelationProjectExpr::RowId(RelationRowIdRef::Current) => "id".to_owned(),
                RelationProjectExpr::Column(reference)
                    if reference.scope.as_deref() == Some(&scope) =>
                {
                    reference.column
                }
                _ => return Err(unsupported("relation projections outside the output scope")),
            };
            Ok(OutputColumn {
                source,
                alias: column.alias,
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// Accept the bare facade and its canonical public-normalizer envelope.
/// Keep renamed output in the existing AST: it must survive query transport
/// and serializable content evidence, not just local row materialization.
pub(crate) fn normalize(query: &Query, schema: &Schema) -> Result<Option<Query>, Error> {
    let Some(relation) = &query.relation else {
        return Ok(None);
    };
    let relation_error = |error: crate::query::QueryError| query_error(error.to_string());
    let mut resolved = crate::query::relation_query_to_query(relation).map_err(relation_error)?;
    if resolved.table != query.table {
        return Err(query_error(
            "relation query output table does not match its Query envelope",
        ));
    }
    let ordinary = |query: &Query| {
        let mut query = query.clone();
        query.relation = None;
        query.array_subqueries.clear();
        query.select = None;
        query
    };
    let envelope = ordinary(query);
    if envelope != Query::from(query.table.clone()) && envelope != ordinary(&resolved) {
        return Err(query_error(
            "relation query ordinary clauses must match its normalized relation",
        ));
    }
    if is_union(query) {
        return Ok(None);
    }
    let mut retained = false;
    if let Some(projected) =
        crate::query::relation_output_projection_if_present(relation).map_err(relation_error)?
    {
        let (scope, _) =
            crate::query::relation_output_projection(relation).map_err(relation_error)?;
        let table = schema.table(&resolved.table)?;
        let mut names = BTreeSet::new();
        let identity = projected.iter().all(|column| {
            let same = match &column.expr {
                RelationProjectExpr::RowId(RelationRowIdRef::Current) => column.alias == "id",
                RelationProjectExpr::Column(reference) => {
                    reference.scope.as_deref() == Some(&scope) && reference.column == column.alias
                }
                RelationProjectExpr::RowId(_) => false,
            };
            if same && column.alias != "id" {
                names.insert(column.alias.as_str());
            }
            same
        });
        let full = identity
            && names.len() == table.columns.len()
            && table
                .columns
                .iter()
                .all(|column| names.contains(column.name.as_str()));
        if !full {
            // This is the model's retained-projection contract: the relation
            // owns its output, so a second presentation cannot replace it.
            if query.select.is_some() || !query.array_subqueries.is_empty() {
                return Err(unsupported(
                    "select or nested arrays over a relation projection that renames or narrows its output",
                ));
            }
            crate::query::reject_reserved_relation_aliases(&projected).map_err(relation_error)?;
            if identity {
                resolved.select = Some(
                    projected
                        .iter()
                        .filter(|column| column.alias != "id")
                        .map(|column| column.alias.clone())
                        .collect(),
                );
            } else {
                retained = true;
            }
        }
    }
    resolved.array_subqueries = query.array_subqueries.clone();
    if query.select.is_some() {
        resolved.select = query.select.clone();
    }
    if !retained {
        resolved.relation = None;
    }
    Ok(Some(resolved))
}

/// Hydration needs the source cells; each reader applies its own aliases.
/// Malformed input is left intact for ordinary validation to reject. Only
/// validated, non-UNION retained projections can lose their AST here.
pub(crate) fn input(query: &Query) -> std::borrow::Cow<'_, Query> {
    let Ok(Some(columns)) = columns(query) else {
        return std::borrow::Cow::Borrowed(query);
    };
    let mut input = query.clone();
    input.relation = None;
    let mut seen = BTreeSet::new();
    input.select = Some(
        columns
            .into_iter()
            .filter_map(|column| seen.insert(column.source.clone()).then_some(column.source))
            .collect(),
    );
    std::borrow::Cow::Owned(input)
}
