//! Narrowed exclusive reads of the sources a query reads beyond its root
//! (garden-co/jazz#3694).
//!
//! An exclusive transaction records every table a query reads beyond its root
//! so the authority can re-check it. A whole-table read is always safe, but it
//! downloads and proves every row the reader can see there, and any write to
//! that table conflicts the transaction. Joined, included and correlated
//! sources correlate back to the root through key equalities or
//! reference-array membership, so they are recorded instead as the rows the
//! query could have consulted: the source
//! table under its own filters, restricted to rows that correlate with a
//! parent row passing the parent's own filters, up to the root.
//!
//! That restriction is an ordinary query: the source table with a reverse
//! `JoinVia` chain back to the root, or nested correlated arrays requiring a
//! parent row when a hop matches by membership. It depends only on the query and its
//! binding, never on the rows read, so a partial node can hydrate it before
//! reading and the authority re-runs it as the reader like any other
//! predicate read. A row added, changed or removed there conflicts exactly
//! when it correlates with a row the query could have consulted, including a
//! correlated row that was absent. The parent filters kept on the chain only
//! ever drop conjuncts, so the read covers a superset of the rows the engine
//! joined.
//!
//! Implicit root references are sync payload that never decides a query's
//! result, so they record no read. Any other source with no narrowed read is
//! not read at all: the query fails with
//! [`Error::UnsupportedExclusiveRead`] naming the read pattern, rather than
//! falling back to a read of the whole table, which does not scale.

use super::*;
use crate::query::{InheritsOperation, JoinCorrelation};

/// A narrowed read of one non-root source: the query the transaction runs
/// and records as its read of that source.
#[derive(Clone)]
pub(in crate::node) struct NarrowedSourceRead {
    pub(in crate::node) shape: ValidatedQuery,
    pub(in crate::node) binding: Binding,
}

/// How an exclusive transaction query reads the sources beyond its root.
#[derive(Clone, Default)]
pub(in crate::node) struct ExclusiveSourceReads {
    /// The narrowed read of each source that decides the result.
    pub(in crate::node) reads: BTreeMap<SourceId, NarrowedSourceRead>,
    /// Implicit root reference sources: sync payload that never decides the
    /// result, so it records no read.
    pub(in crate::node) payload: BTreeSet<SourceId>,
}

/// The narrowed reads of a query's non-root sources, and a description of
/// the read pattern of each source that has none.
#[derive(Default)]
pub(in crate::node) struct NarrowedSources {
    pub(in crate::node) reads: BTreeMap<SourceId, NarrowedSourceRead>,
    unsupported: BTreeMap<SourceId, String>,
    /// Set when no source of the query narrows.
    unsupported_query: Option<String>,
}

impl NarrowedSources {
    fn unsupported_query(pattern: String) -> Self {
        Self {
            unsupported_query: Some(pattern),
            ..Self::default()
        }
    }

    /// The read pattern of `source`, which has no narrowed read.
    fn unsupported_pattern(&self, source: &SourceId, root: &str) -> String {
        self.unsupported
            .get(source)
            .or(self.unsupported_query.as_ref())
            .cloned()
            .unwrap_or_else(|| {
                format!(
                    "`{}` as source `{}` of a query on `{root}`",
                    source.table,
                    describe_source_path(source),
                )
            })
    }
}

fn describe_source_path(source: &SourceId) -> String {
    use crate::node::query_engine::SourceRole;
    source
        .path
        .components
        .iter()
        .map(|component| match component {
            SourceRole::Root => "root".to_owned(),
            SourceRole::Alias(name)
            | SourceRole::RecursiveSeed(name)
            | SourceRole::RecursiveStep(name)
            | SourceRole::CorrelatedChild(name)
            | SourceRole::Policy(name) => name.clone(),
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// A row a narrowed source correlates with, and how that row is itself
/// constrained back to the query root.
#[derive(Clone)]
struct CorrelationParent {
    table: String,
    filters: Vec<Predicate>,
    /// The hops from this row up to the root; empty at the root.
    links: Vec<Link>,
}

/// One hop from a row towards the query root: the parent rows it correlates
/// with.
#[derive(Clone)]
struct Link {
    parent_table: String,
    parent_filters: Vec<Predicate>,
    correlation: Correlation,
}

/// Keys of one correlation between a child source and its parent: columns,
/// or `"id"` for the row id.
#[derive(Clone)]
struct Correlation {
    child_key: String,
    parent_key: String,
    /// Additional `(child column, parent column)` equalities.
    extra: Vec<(String, String)>,
    /// Whether a key holds an array of references, which correlates by
    /// membership rather than equality.
    membership: bool,
}

impl CorrelationParent {
    /// The narrowed read of a child source of this parent, and the parent the
    /// child's own children correlate with. `None` if the hops back to the
    /// root cannot be expressed as one query.
    fn child(
        &self,
        table: &str,
        filters: Vec<Predicate>,
        correlation: Option<Correlation>,
    ) -> (Option<JazzQuery>, CorrelationParent) {
        let links = match correlation {
            Some(correlation) => std::iter::once(Link {
                parent_table: self.table.clone(),
                parent_filters: self.filters.clone(),
                correlation,
            })
            .chain(self.links.iter().cloned())
            .collect(),
            None => Vec::new(),
        };
        let parent = CorrelationParent {
            table: table.to_owned(),
            filters,
            links,
        };
        (parent.narrowed_read(), parent)
    }

    /// This source under its own filters, restricted to rows that correlate
    /// with a parent row passing the parent's own filters, up to the root.
    /// Equality hops become a reverse join chain. A chain with a membership
    /// hop becomes nested correlated arrays that require at least one parent
    /// row, which match array references by membership; they carry no
    /// additional equalities.
    fn narrowed_read(&self) -> Option<JazzQuery> {
        let mut query = JazzQuery::from(self.table.as_str());
        query.filters = self.filters.clone();
        if self.links.iter().any(|link| link.correlation.membership) {
            let mut up: Option<ArraySubquery> = None;
            for link in self.links.iter().rev() {
                let correlation = &link.correlation;
                if !correlation.extra.is_empty() {
                    return None;
                }
                let mut hop = ArraySubquery::new(
                    "narrowed_parent",
                    link.parent_table.as_str(),
                    correlation.parent_key.as_str(),
                    correlation.child_key.as_str(),
                );
                hop.filters = link.parent_filters.clone();
                hop.requirement = ArraySubqueryRequirement::AtLeastOne;
                hop.nested_arrays = up.into_iter().collect();
                up = Some(hop);
            }
            query.array_subqueries = up.into_iter().collect();
        } else {
            let mut up: Option<JoinVia> = None;
            for link in self.links.iter().rev() {
                let correlation = &link.correlation;
                up = Some(JoinVia {
                    source_mode: crate::query::CandidateSourceMode::AcceptedOnly,
                    table: link.parent_table.clone(),
                    target: if correlation.parent_key == "id" {
                        JoinTarget::RowId
                    } else {
                        JoinTarget::Column
                    },
                    on_column: correlation.parent_key.clone(),
                    source_column: Some(correlation.child_key.clone()),
                    source_lookup: None,
                    correlated_filters: correlation
                        .extra
                        .iter()
                        .map(|(child, parent)| JoinCorrelation {
                            join_column: parent.clone(),
                            source_column: child.clone(),
                        })
                        .collect(),
                    filters: link.parent_filters.clone(),
                    nested_joins: up.into_iter().collect(),
                });
            }
            query.joins = up.into_iter().collect();
        }
        Some(query)
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

/// Whether `column_type` holds an array, whose correlation matches by
/// membership.
fn is_array(column_type: &ColumnType) -> bool {
    match column_type {
        ColumnType::Array(_) => true,
        ColumnType::Nullable(inner) => is_array(inner),
        _ => false,
    }
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// The narrowed read of every source of `shape` beyond its root that
    /// decides its result, keyed by source, for an exclusive transaction
    /// query, and the implicit root reference sources, which are sync payload
    /// and need no read. Fails with [`Error::UnsupportedExclusiveRead`]
    /// naming the read pattern of the first source that has no narrowed
    /// read. `include_deleted` reads narrow nothing: a narrowed read
    /// correlates only with the root rows that are visible.
    pub(in crate::node) fn exclusive_source_reads(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        include_deleted: bool,
    ) -> Result<ExclusiveSourceReads, Error> {
        use crate::node::query_engine::{ClosurePath, RowSetExpr, SourceRole};
        let normalized = self.normalized_row_set_shape(shape, binding)?;
        let payload = normalized
            .closure_paths
            .iter()
            .filter_map(|path| match path {
                ClosurePath::ImplicitRootReference { segment, .. } => Some(segment.target.clone()),
                ClosurePath::ExplicitInclude { .. } => None,
            })
            .collect::<BTreeSet<_>>();
        let sources = normalized
            .nodes
            .values()
            .filter_map(|node| match node {
                RowSetExpr::Source { source, .. } => Some(source),
                _ => None,
            })
            .chain(&normalized.auxiliary_sources)
            .filter(|source| {
                source.path.components != [SourceRole::Root] && !payload.contains(*source)
            })
            .collect::<BTreeSet<_>>();
        let Some(first) = sources.first() else {
            return Ok(ExclusiveSourceReads {
                reads: BTreeMap::new(),
                payload,
            });
        };
        let root = &shape.query().table;
        let narrowed = if include_deleted {
            NarrowedSources::unsupported_query(format!(
                "deleted `{root}` rows together with `{}`",
                first.table
            ))
        } else {
            self.narrowed_source_reads(shape, binding)?
        };
        if let Some(source) = sources
            .iter()
            .find(|source| !narrowed.reads.contains_key(source))
        {
            return Err(Error::UnsupportedExclusiveRead(
                narrowed.unsupported_pattern(source, root),
            ));
        }
        Ok(ExclusiveSourceReads {
            reads: narrowed.reads,
            payload,
        })
    }

    /// The narrowed read for each non-root source of `shape` that correlates
    /// back to its root, keyed by the source it replaces, and the read
    /// pattern of each source that does not. Shapes whose root rows are not
    /// a subset of the rows its own filters select (policy branches, flat
    /// joins and retained relation trees) narrow nothing.
    fn narrowed_source_reads(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<NarrowedSources, Error> {
        let query = shape.query();
        let root_table = &query.table;
        if let Some(relation) = &query.relation {
            return Ok(NarrowedSources::unsupported_query(
                if crate::query::relation_union_parts(&relation.rel).is_some() {
                    format!("a union of relations over `{root_table}`")
                } else {
                    format!("a relation over `{root_table}` that projects selected columns")
                },
            ));
        }
        if let Some(flat_join) = &query.flat_join {
            let tables = flat_join
                .sources
                .iter()
                .map(|source| format!("`{}`", source.table))
                .collect::<Vec<_>>()
                .join(", ");
            return Ok(NarrowedSources::unsupported_query(format!(
                "a flat join of `{root_table}` with {tables}"
            )));
        }
        if !query.policy_branches.is_empty() {
            return Ok(NarrowedSources::unsupported_query(format!(
                "`{root_table}` through policy branches"
            )));
        }
        if let Some(reachable) = query.reachable.first() {
            return Ok(NarrowedSources::unsupported_query(format!(
                "`{root_table}` through a recursive traversal of `{}`",
                reachable.edge_table
            )));
        }
        if let Some(inherits) = query.inherits.first() {
            let parent = self
                .table_in_schema_ref(root_table, shape.schema_version())?
                .references
                .get(&inherits.parent_column)
                .cloned()
                .unwrap_or_else(|| inherits.parent_column.clone());
            let access = match inherits.operation {
                InheritsOperation::Select => "read",
                InheritsOperation::Insert => "insert",
                InheritsOperation::Update => "update",
                InheritsOperation::Delete => "delete",
            };
            return Ok(NarrowedSources::unsupported_query(format!(
                "`{root_table}` inheriting {access} access from `{parent}`"
            )));
        }
        let values = binding.values();
        let Some(root_filters) = bind_predicates(&query.filters, values) else {
            return Ok(NarrowedSources::unsupported_query(format!(
                "`{root_table}` with a filter whose parameter is not bound"
            )));
        };
        let root = CorrelationParent {
            table: query.table.clone(),
            filters: root_filters,
            links: Vec::new(),
        };
        let mut candidates = Vec::<(SourceId, JazzQuery)>::new();
        let mut narrowed = NarrowedSources::default();

        let schema_version = shape.schema_version();
        for (index, join) in query.joins.iter().enumerate() {
            self.narrow_join(
                &root,
                join,
                &format!("join_via:{index}"),
                schema_version,
                values,
                &mut candidates,
                &mut narrowed.unsupported,
            );
        }

        // Implicit root references are sync payload: they never decide
        // which rows a query returns, so they need no read
        // (`Self::exclusive_source_reads`).
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
                let correlation =
                    self.correlation(&target, "id", &parent.table, segment, schema_version);
                let (read, child) = parent.child(&target, Vec::new(), Some(correlation));
                push_candidate(
                    include_auxiliary_source_id(target, include_index, segment_index),
                    read,
                    &mut candidates,
                    &mut narrowed.unsupported,
                );
                parent = child;
            }
        }

        let root_source = root_source_id(&query.table);
        for (index, subquery) in query.array_subqueries.iter().enumerate() {
            self.narrow_array_subquery(
                &root,
                &root_source,
                subquery,
                &[index],
                schema_version,
                values,
                &mut candidates,
                &mut narrowed.unsupported,
            );
        }

        let schema = &self
            .catalogue
            .catalogue_schemas
            .get(&schema_version)
            .ok_or(Error::InvalidStoredValue("query schema version is unknown"))?
            .schema;
        for (source, read) in candidates {
            let bound = read
                .validate(schema)
                .and_then(|shape| shape.bind(BTreeMap::new()).map(|binding| (shape, binding)));
            match bound {
                Ok((shape, binding)) => {
                    narrowed
                        .reads
                        .insert(source, NarrowedSourceRead { shape, binding });
                }
                Err(error) => {
                    let pattern = format!(
                        "`{}` correlated with `{root_table}` ({error})",
                        source.table
                    );
                    narrowed.unsupported.insert(source, pattern);
                }
            }
        }
        Ok(narrowed)
    }

    /// The correlation of `child_table.child_key` with
    /// `parent_table.parent_key`, which matches by membership when either
    /// holds an array.
    fn correlation(
        &self,
        child_table: &str,
        child_key: &str,
        parent_table: &str,
        parent_key: &str,
        schema_version: SchemaVersionId,
    ) -> Correlation {
        Correlation {
            child_key: child_key.to_owned(),
            parent_key: parent_key.to_owned(),
            extra: Vec::new(),
            membership: self.column_is_array(child_table, child_key, schema_version)
                || self.column_is_array(parent_table, parent_key, schema_version),
        }
    }

    fn column_is_array(&self, table: &str, column: &str, schema_version: SchemaVersionId) -> bool {
        self.table_in_schema_ref(table, schema_version)
            .ok()
            .and_then(|table| {
                table
                    .columns
                    .iter()
                    .find(|candidate| candidate.name == column)
            })
            .is_some_and(|column| is_array(&column.column_type))
    }

    /// Narrow a `JoinVia` source at `path` and its nested joins.
    #[allow(clippy::too_many_arguments)]
    fn narrow_join(
        &self,
        parent: &CorrelationParent,
        join: &JoinVia,
        path: &str,
        schema_version: SchemaVersionId,
        values: &BTreeMap<String, Value>,
        candidates: &mut Vec<(SourceId, JazzQuery)>,
        unsupported: &mut BTreeMap<SourceId, String>,
    ) {
        // A lookup join correlates through a third table, which a reverse
        // join cannot express; so does everything below it.
        if join.source_lookup.is_some() {
            Self::decline_join(
                join,
                path,
                &format!(
                    "`{}` through a lookup join from `{}`",
                    join.table, parent.table
                ),
                unsupported,
            );
            return;
        }
        let Some(filters) = bind_predicates(&join.filters, values) else {
            Self::decline_join(
                join,
                path,
                &format!(
                    "`{}` joined to `{}` with a filter whose parameter is not bound",
                    join.table, parent.table
                ),
                unsupported,
            );
            return;
        };
        let correlation = (join.target != JoinTarget::Uncorrelated).then(|| {
            let child_key = if join.target == JoinTarget::RowId {
                "id"
            } else {
                join.on_column.as_str()
            };
            let parent_key = join.source_column.as_deref().unwrap_or("id");
            let mut correlation = self.correlation(
                &join.table,
                child_key,
                &parent.table,
                parent_key,
                schema_version,
            );
            correlation.extra = join
                .correlated_filters
                .iter()
                .map(|correlation| {
                    (
                        correlation.join_column.clone(),
                        correlation.source_column.clone(),
                    )
                })
                .collect();
            correlation
        });
        let (read, child) = parent.child(&join.table, filters, correlation);
        push_candidate(
            nested_join_source_id(join, path),
            read,
            candidates,
            unsupported,
        );
        for (index, nested) in join.nested_joins.iter().enumerate() {
            self.narrow_join(
                &child,
                nested,
                &format!("{path}:nested:{index}"),
                schema_version,
                values,
                candidates,
                unsupported,
            );
        }
    }

    /// Note `pattern` as the read of the join source at `path` and every
    /// source nested below it.
    fn decline_join(
        join: &JoinVia,
        path: &str,
        pattern: &str,
        unsupported: &mut BTreeMap<SourceId, String>,
    ) {
        unsupported.insert(nested_join_source_id(join, path), pattern.to_owned());
        for (index, nested) in join.nested_joins.iter().enumerate() {
            Self::decline_join(
                nested,
                &format!("{path}:nested:{index}"),
                pattern,
                unsupported,
            );
        }
    }

    /// Narrow a correlated array source and its nested arrays.
    #[allow(clippy::too_many_arguments)]
    fn narrow_array_subquery(
        &self,
        parent: &CorrelationParent,
        owner: &SourceId,
        subquery: &ArraySubquery,
        path: &[usize],
        schema_version: SchemaVersionId,
        values: &BTreeMap<String, Value>,
        candidates: &mut Vec<(SourceId, JazzQuery)>,
        unsupported: &mut BTreeMap<SourceId, String>,
    ) {
        let source = correlated_child_source_id(owner, subquery, path);
        let Some(filters) = bind_predicates(&subquery.filters, values) else {
            unsupported.insert(
                source,
                format!(
                    "`{}` related to `{}` with a filter whose parameter is not bound",
                    subquery.table, parent.table
                ),
            );
            return;
        };
        let correlation = self.correlation(
            &subquery.table,
            &subquery.inner_column,
            &parent.table,
            &subquery.outer_column,
            schema_version,
        );
        let (read, child) = parent.child(&subquery.table, filters, Some(correlation));
        push_candidate(source.clone(), read, candidates, unsupported);
        for (index, nested) in subquery.nested_arrays.iter().enumerate() {
            let mut nested_path = path.to_vec();
            nested_path.push(index);
            self.narrow_array_subquery(
                &child,
                &source,
                nested,
                &nested_path,
                schema_version,
                values,
                candidates,
                unsupported,
            );
        }
    }
}

/// Add the narrowed read of `source`, or note why it has none.
fn push_candidate(
    source: SourceId,
    read: Option<JazzQuery>,
    candidates: &mut Vec<(SourceId, JazzQuery)>,
    unsupported: &mut BTreeMap<SourceId, String>,
) {
    match read {
        Some(read) => candidates.push((source, read)),
        None => {
            let pattern = format!(
                "`{}` through a reference array together with additional join keys",
                source.table
            );
            unsupported.insert(source, pattern);
        }
    }
}
