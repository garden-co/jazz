//! Narrowed exclusive reads of the sources a query reads beyond its root
//! (garden-co/jazz#3694).
//!
//! An exclusive transaction records every table a query reads beyond its root
//! so the authority can re-check it. A whole-table read is always safe, but it
//! downloads and proves every row the reader can see there, and any write to
//! that table conflicts the transaction. Most joined, included and correlated
//! sources correlate back to the root through key equalities, so they are
//! recorded instead as the rows the query could have consulted: the source
//! table under its own filters, restricted to rows that correlate with a
//! parent row passing the parent's own filters, up to the root.
//!
//! That restriction is an ordinary query: the source table with a reverse
//! `JoinVia` chain back to the root. It depends only on the query and its
//! binding, never on the rows read, so a partial node can hydrate it before
//! reading and the authority re-runs it as the reader like any other
//! predicate read. A row added, changed or removed there conflicts exactly
//! when it correlates with a row the query could have consulted, including a
//! correlated row that was absent. The parent filters kept on the chain only
//! ever drop conjuncts, so the read covers a superset of the rows the engine
//! joined. Sources whose shape has no such restriction keep the whole-table
//! read.

use super::*;
use crate::query::JoinCorrelation;

#[cfg(any(test, feature = "testing"))]
thread_local! {
    static NARROWING_DISABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Record every source beyond a query's root as a read of its whole table, as
/// before narrowed reads existed, so a benchmark can compare both in one
/// build. Applies to the current thread.
#[cfg(any(test, feature = "testing"))]
#[doc(hidden)]
pub fn set_exclusive_source_narrowing_for_test(enabled: bool) {
    NARROWING_DISABLED.with(|disabled| disabled.set(!enabled));
}

/// A narrowed read of one non-root source: a query the transaction runs and
/// records in place of a read of the source's whole table.
#[derive(Clone)]
pub(in crate::node) struct NarrowedSourceRead {
    pub(in crate::node) shape: ValidatedQuery,
    pub(in crate::node) binding: Binding,
}

/// A row a narrowed source correlates with, and how that row is itself
/// constrained back to the query root.
#[derive(Clone)]
struct CorrelationParent {
    table: String,
    filters: Vec<Predicate>,
    /// The parent's own reverse join towards the root; `None` at the root.
    up: Option<JoinVia>,
}

/// Keys of one correlation between a child source and its parent: columns,
/// or `"id"` for the row id.
struct Correlation {
    child_key: String,
    parent_key: String,
    /// Additional `(child column, parent column)` equalities.
    extra: Vec<(String, String)>,
}

impl CorrelationParent {
    /// The join from a child row to the parent rows it correlates with.
    fn reverse_join(&self, correlation: Correlation) -> JoinVia {
        JoinVia {
            table: self.table.clone(),
            target: if correlation.parent_key == "id" {
                JoinTarget::RowId
            } else {
                JoinTarget::Column
            },
            on_column: correlation.parent_key,
            source_column: Some(correlation.child_key),
            source_lookup: None,
            correlated_filters: correlation
                .extra
                .into_iter()
                .map(|(child, parent)| JoinCorrelation {
                    join_column: parent,
                    source_column: child,
                })
                .collect(),
            filters: self.filters.clone(),
            nested_joins: self.up.clone().into_iter().collect(),
        }
    }

    /// The narrowed read of a child source of this parent, and the parent the
    /// child's own children correlate with.
    fn child(
        &self,
        table: &str,
        filters: Vec<Predicate>,
        correlation: Option<Correlation>,
    ) -> (JazzQuery, CorrelationParent) {
        let up = correlation.map(|correlation| self.reverse_join(correlation));
        let mut query = JazzQuery::from(table);
        query.filters = filters.clone();
        query.joins = up.clone().into_iter().collect();
        (
            query,
            CorrelationParent {
                table: table.to_owned(),
                filters,
                up,
            },
        )
    }
}

/// `predicate` with every binding parameter replaced by its bound value, so a
/// narrowed read needs no binding of its own. `None` if a value is missing.
fn bind_predicate(predicate: &Predicate, values: &BTreeMap<String, Value>) -> Option<Predicate> {
    let operand = |operand: &Operand| -> Option<Operand> {
        Some(match operand {
            Operand::Param(name) => Operand::Literal(values.get(name)?.clone()),
            other => other.clone(),
        })
    };
    let all = |predicates: &[Predicate]| -> Option<Vec<Predicate>> {
        predicates
            .iter()
            .map(|predicate| bind_predicate(predicate, values))
            .collect()
    };
    Some(match predicate {
        Predicate::All(predicates) => Predicate::All(all(predicates)?),
        Predicate::Any(predicates) => Predicate::Any(all(predicates)?),
        Predicate::Not(inner) => Predicate::Not(Box::new(bind_predicate(inner, values)?)),
        Predicate::Eq(left, right) => Predicate::Eq(operand(left)?, operand(right)?),
        Predicate::Ne(left, right) => Predicate::Ne(operand(left)?, operand(right)?),
        Predicate::In(left, list) => Predicate::In(
            operand(left)?,
            list.iter().map(operand).collect::<Option<Vec<_>>>()?,
        ),
        Predicate::Gt(left, right) => Predicate::Gt(operand(left)?, operand(right)?),
        Predicate::Gte(left, right) => Predicate::Gte(operand(left)?, operand(right)?),
        Predicate::Lt(left, right) => Predicate::Lt(operand(left)?, operand(right)?),
        Predicate::Lte(left, right) => Predicate::Lte(operand(left)?, operand(right)?),
        Predicate::Contains(left, right) => Predicate::Contains(operand(left)?, operand(right)?),
        Predicate::EnumMatch {
            column,
            case,
            payload,
        } => Predicate::EnumMatch {
            column: column.clone(),
            case: case.clone(),
            payload: Box::new(bind_predicate(payload, values)?),
        },
        Predicate::IsNull(inner) => Predicate::IsNull(operand(inner)?),
    })
}

fn bind_predicates(
    predicates: &[Predicate],
    values: &BTreeMap<String, Value>,
) -> Option<Vec<Predicate>> {
    predicates
        .iter()
        .map(|predicate| bind_predicate(predicate, values))
        .collect()
}

/// Whether `column_type` holds a single row reference. Reference arrays match
/// by membership, which a reverse equality join cannot express.
fn is_scalar_reference(column_type: &ColumnType) -> bool {
    match column_type {
        ColumnType::Uuid => true,
        ColumnType::Nullable(inner) => matches!(inner.as_ref(), ColumnType::Uuid),
        _ => false,
    }
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// The narrowed read for each non-root source of `shape` that correlates
    /// back to its root, keyed by the source it replaces. A source missing
    /// here keeps its whole-table read. Shapes whose root rows are not a
    /// subset of the rows its own filters select (policy branches, flat joins
    /// and relation trees) narrow nothing.
    pub(in crate::node) fn narrowed_source_reads(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<BTreeMap<SourceId, NarrowedSourceRead>, Error> {
        #[cfg(any(test, feature = "testing"))]
        if NARROWING_DISABLED.with(std::cell::Cell::get) {
            return Ok(BTreeMap::new());
        }
        let query = shape.query();
        let mut candidates = Vec::<(SourceId, JazzQuery)>::new();
        if !query.policy_branches.is_empty()
            || query.flat_join.is_some()
            || query.relation.is_some()
        {
            return Ok(BTreeMap::new());
        }
        let values = binding.values();
        let Some(root_filters) = bind_predicates(&query.filters, values) else {
            return Ok(BTreeMap::new());
        };
        let root = CorrelationParent {
            table: query.table.clone(),
            filters: root_filters,
            up: None,
        };

        for (index, join) in query.joins.iter().enumerate() {
            Self::narrow_join(
                &root,
                join,
                &format!("join_via:{index}"),
                values,
                &mut candidates,
            );
        }

        let schema_version = shape.schema_version();
        let root_schema = self.table_in_schema_ref(&query.table, schema_version)?;
        let explicit_root_segments = query
            .includes
            .iter()
            .filter_map(|include| include.path.split('.').next())
            .collect::<BTreeSet<_>>();
        for (column, target) in &root_schema.references {
            if explicit_root_segments.contains(column.as_str())
                || !self.column_is_scalar_reference(&query.table, column, schema_version)
            {
                continue;
            }
            let (narrowed, _) = root.child(
                target,
                Vec::new(),
                Some(Correlation {
                    child_key: "id".to_owned(),
                    parent_key: column.clone(),
                    extra: Vec::new(),
                }),
            );
            candidates.push((implicit_reference_source_id(target, column), narrowed));
        }
        for (include_index, include) in query.includes.iter().enumerate() {
            let mut parent = root.clone();
            for (segment_index, segment) in include.path.split('.').enumerate() {
                let Some(target) = self
                    .table_in_schema_ref(&parent.table, schema_version)?
                    .references
                    .get(segment)
                    .cloned()
                else {
                    break;
                };
                if !self.column_is_scalar_reference(&parent.table, segment, schema_version) {
                    break;
                }
                let (narrowed, child) = parent.child(
                    &target,
                    Vec::new(),
                    Some(Correlation {
                        child_key: "id".to_owned(),
                        parent_key: segment.to_owned(),
                        extra: Vec::new(),
                    }),
                );
                candidates.push((
                    include_auxiliary_source_id(target, include_index, segment_index),
                    narrowed,
                ));
                parent = child;
            }
        }

        let root_source = root_source_id(&query.table);
        for (index, subquery) in query.array_subqueries.iter().enumerate() {
            Self::narrow_array_subquery(
                &root,
                &root_source,
                subquery,
                &[index],
                values,
                &mut candidates,
            );
        }

        let schema = &self
            .catalogue
            .catalogue_schemas
            .get(&schema_version)
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?
            .schema;
        let mut reads = BTreeMap::new();
        for (source, narrowed) in candidates {
            // A shape the query model rejects keeps its whole-table read.
            let Ok(shape) = narrowed.validate(schema) else {
                continue;
            };
            let Ok(binding) = shape.bind(BTreeMap::new()) else {
                continue;
            };
            reads.insert(source, NarrowedSourceRead { shape, binding });
        }
        Ok(reads)
    }

    fn column_is_scalar_reference(
        &self,
        table: &str,
        column: &str,
        schema_version: SchemaVersionId,
    ) -> bool {
        self.table_in_schema_ref(table, schema_version)
            .ok()
            .and_then(|table| {
                table
                    .columns
                    .iter()
                    .find(|candidate| candidate.name == column)
            })
            .is_some_and(|column| is_scalar_reference(&column.column_type))
    }

    /// Narrow a `JoinVia` source at `path` and its nested joins.
    fn narrow_join(
        parent: &CorrelationParent,
        join: &JoinVia,
        path: &str,
        values: &BTreeMap<String, Value>,
        candidates: &mut Vec<(SourceId, JazzQuery)>,
    ) {
        // A lookup join correlates through a third table; keep its source
        // and everything below it whole.
        if join.source_lookup.is_some() {
            return;
        }
        let Some(filters) = bind_predicates(&join.filters, values) else {
            return;
        };
        let correlation = (join.target != JoinTarget::Uncorrelated).then(|| Correlation {
            child_key: if join.target == JoinTarget::RowId {
                "id".to_owned()
            } else {
                join.on_column.clone()
            },
            parent_key: join
                .source_column
                .clone()
                .unwrap_or_else(|| "id".to_owned()),
            extra: join
                .correlated_filters
                .iter()
                .map(|correlation| {
                    (
                        correlation.join_column.clone(),
                        correlation.source_column.clone(),
                    )
                })
                .collect(),
        });
        let (narrowed, child) = parent.child(&join.table, filters, correlation);
        candidates.push((nested_join_source_id(join, path), narrowed));
        for (index, nested) in join.nested_joins.iter().enumerate() {
            Self::narrow_join(
                &child,
                nested,
                &format!("{path}:nested:{index}"),
                values,
                candidates,
            );
        }
    }

    /// Narrow a correlated array source and its nested arrays.
    fn narrow_array_subquery(
        parent: &CorrelationParent,
        owner: &SourceId,
        subquery: &ArraySubquery,
        path: &[usize],
        values: &BTreeMap<String, Value>,
        candidates: &mut Vec<(SourceId, JazzQuery)>,
    ) {
        let Some(filters) = bind_predicates(&subquery.filters, values) else {
            return;
        };
        let source = correlated_child_source_id(owner, subquery, path);
        let (narrowed, child) = parent.child(
            &subquery.table,
            filters,
            Some(Correlation {
                child_key: subquery.inner_column.clone(),
                parent_key: subquery.outer_column.clone(),
                extra: Vec::new(),
            }),
        );
        candidates.push((source.clone(), narrowed));
        for (index, nested) in subquery.nested_arrays.iter().enumerate() {
            let mut nested_path = path.to_vec();
            nested_path.push(index);
            Self::narrow_array_subquery(&child, &source, nested, &nested_path, values, candidates);
        }
    }
}
