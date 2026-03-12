use ahash::AHashSet;

use crate::commit::CommitId;
use crate::query_manager::encoding::{decode_column, encode_row};
use crate::query_manager::relation_ir::{ProjectColumn, ProjectExpr, RowIdRef};
use crate::query_manager::types::{
    ColumnDescriptor, ColumnName, ColumnType, RowDescriptor, Tuple, TupleDelta, TupleDescriptor,
    TupleElement, Value,
};

use super::RowNode;

#[derive(Debug, Clone)]
enum ProjectionSource {
    Column { global_index: usize },
    RowId { element_index: usize },
}

#[derive(Debug, Clone)]
struct ProjectionField {
    output_column: ColumnDescriptor,
    source: ProjectionSource,
}

/// Project node for column selection.
///
/// Transforms fully materialized tuples into a single output row with the
/// requested projection shape. This supports both the legacy "select these
/// column names" path and precise relation-IR projections with aliases/scopes.
#[derive(Debug)]
pub struct ProjectNode {
    input_tuple_descriptor: TupleDescriptor,
    output_descriptor: RowDescriptor,
    output_tuple_descriptor: TupleDescriptor,
    projection_fields: Vec<ProjectionField>,
    current_tuples: AHashSet<Tuple>,
    dirty: bool,
}

impl ProjectNode {
    /// Create a new project node from a single input row descriptor.
    pub fn new(input_descriptor: RowDescriptor, select_columns: &[&str]) -> Self {
        let input_tuple_descriptor =
            TupleDescriptor::single_with_materialization("", input_descriptor, true);
        Self::with_tuple_descriptor(input_tuple_descriptor, select_columns)
    }

    /// Create a new project node from a tuple descriptor and unqualified column names.
    pub fn with_tuple_descriptor(
        input_tuple_descriptor: TupleDescriptor,
        select_columns: &[&str],
    ) -> Self {
        let combined_descriptor = input_tuple_descriptor.combined_descriptor();
        let projection_fields = select_columns
            .iter()
            .filter_map(|col_name| {
                let src_idx = combined_descriptor.column_index(col_name)?;
                let col = combined_descriptor.columns[src_idx].clone();
                Some(ProjectionField {
                    output_column: col,
                    source: ProjectionSource::Column {
                        global_index: src_idx,
                    },
                })
            })
            .collect();

        Self::from_projection_fields(input_tuple_descriptor, projection_fields)
    }

    /// Create a new project node from explicit projected expressions.
    pub fn with_project_columns(
        input_tuple_descriptor: TupleDescriptor,
        project_columns: &[ProjectColumn],
    ) -> Option<Self> {
        let mut projection_fields = Vec::with_capacity(project_columns.len());
        for column in project_columns {
            let Some(source) = (match &column.expr {
                ProjectExpr::Column(column_ref) if column_ref.column == "_id" => {
                    Some(ProjectionSource::RowId {
                        element_index: resolve_row_id_element(
                            &input_tuple_descriptor,
                            column_ref.scope.as_deref(),
                        )?,
                    })
                }
                ProjectExpr::Column(column_ref) => {
                    let global_index = if let Some(scope) = column_ref.scope.as_deref() {
                        input_tuple_descriptor.qualified_column_index(scope, &column_ref.column)
                    } else {
                        input_tuple_descriptor.column_index(&column_ref.column)
                    };
                    global_index.map(|global_index| ProjectionSource::Column { global_index })
                }
                ProjectExpr::RowId(RowIdRef::Current) => {
                    resolve_row_id_element(&input_tuple_descriptor, None)
                        .map(|element_index| ProjectionSource::RowId { element_index })
                }
                ProjectExpr::RowId(_) => None,
            }) else {
                continue;
            };

            let output_column = match &source {
                ProjectionSource::Column { global_index } => {
                    let (element_index, local_index) =
                        input_tuple_descriptor.resolve_column(*global_index)?;
                    let source_column = &input_tuple_descriptor
                        .element(element_index)?
                        .descriptor
                        .columns[local_index];
                    ColumnDescriptor {
                        name: ColumnName::new(column.alias.clone()),
                        column_type: source_column.column_type.clone(),
                        nullable: source_column.nullable,
                        references: source_column.references,
                    }
                }
                ProjectionSource::RowId { .. } => ColumnDescriptor {
                    name: ColumnName::new(column.alias.clone()),
                    column_type: ColumnType::Uuid,
                    nullable: false,
                    references: None,
                },
            };

            projection_fields.push(ProjectionField {
                output_column,
                source,
            });
        }

        if projection_fields.is_empty() && !project_columns.is_empty() {
            return None;
        }

        Some(Self::from_projection_fields(
            input_tuple_descriptor,
            projection_fields,
        ))
    }

    fn from_projection_fields(
        input_tuple_descriptor: TupleDescriptor,
        projection_fields: Vec<ProjectionField>,
    ) -> Self {
        let output_descriptor = RowDescriptor::new(
            projection_fields
                .iter()
                .map(|field| field.output_column.clone())
                .collect(),
        );
        let output_tuple_descriptor =
            TupleDescriptor::single_with_materialization("", output_descriptor.clone(), true);

        Self {
            input_tuple_descriptor,
            output_descriptor,
            output_tuple_descriptor,
            projection_fields,
            current_tuples: AHashSet::new(),
            dirty: true,
        }
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_tuple_descriptor
    }

    fn projected_value(&self, tuple: &Tuple, source: &ProjectionSource) -> Option<Value> {
        match source {
            ProjectionSource::Column { global_index } => {
                let (element_index, local_index) =
                    self.input_tuple_descriptor.resolve_column(*global_index)?;
                let element = tuple.get(element_index)?;
                let descriptor = &self
                    .input_tuple_descriptor
                    .element(element_index)?
                    .descriptor;
                decode_column(descriptor, element.content()?, local_index).ok()
            }
            ProjectionSource::RowId { element_index } => {
                Some(Value::Uuid(tuple.get(*element_index)?.id()))
            }
        }
    }

    /// Project a single tuple to the output row shape.
    pub(in crate::query_manager) fn project_tuple(&self, tuple: &Tuple) -> Option<Tuple> {
        let values: Option<Vec<_>> = self
            .projection_fields
            .iter()
            .map(|field| self.projected_value(tuple, &field.source))
            .collect();
        let projected_content = encode_row(&self.output_descriptor, &values?).ok()?;
        let id = tuple.first_id()?;
        let commit_id = tuple
            .iter()
            .find_map(TupleElement::commit_id)
            .unwrap_or(CommitId([0; 32]));

        Some(
            Tuple::new(vec![TupleElement::Row {
                id,
                content: projected_content,
                commit_id,
            }])
            .with_provenance(tuple.provenance().clone()),
        )
    }
}

fn resolve_row_id_element(
    input_tuple_descriptor: &TupleDescriptor,
    scope: Option<&str>,
) -> Option<usize> {
    if let Some(scope) = scope {
        for index in 0..input_tuple_descriptor.element_count() {
            if input_tuple_descriptor.element(index)?.table == scope {
                return Some(index);
            }
        }
        return None;
    }

    (input_tuple_descriptor.element_count() == 1).then_some(0)
}

impl RowNode for ProjectNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.output_descriptor
    }

    fn process(&mut self, input: TupleDelta) -> TupleDelta {
        let mut result = TupleDelta::new();

        for tuple in input.removed {
            if let Some(projected) = self.project_tuple(&tuple) {
                self.current_tuples.remove(&projected);
                result.removed.push(projected);
            }
        }

        for tuple in input.added {
            if let Some(projected) = self.project_tuple(&tuple) {
                self.current_tuples.insert(projected.clone());
                result.added.push(projected);
            }
        }

        for (old_tuple, new_tuple) in input.updated {
            if let (Some(old_projected), Some(new_projected)) = (
                self.project_tuple(&old_tuple),
                self.project_tuple(&new_tuple),
            ) {
                self.current_tuples.remove(&old_projected);
                self.current_tuples.insert(new_projected.clone());
                result.updated.push((old_projected, new_projected));
            }
        }

        self.dirty = false;
        result
    }

    fn current_tuples(&self) -> &AHashSet<Tuple> {
        &self.current_tuples
    }

    fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::CommitId;
    use crate::object::ObjectId;
    use crate::query_manager::encoding::{decode_row, encode_row};
    use crate::query_manager::relation_ir::ColumnRef;
    use crate::query_manager::types::Value;

    fn test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("email", ColumnType::Text),
            ColumnDescriptor::new("age", ColumnType::Integer),
        ])
    }

    fn make_tuple(id: ObjectId, values: &[Value]) -> Tuple {
        let descriptor = test_descriptor();
        let data = encode_row(&descriptor, values).unwrap();
        Tuple::new(vec![TupleElement::Row {
            id,
            content: data,
            commit_id: CommitId([0; 32]),
        }])
    }

    #[test]
    fn project_selects_columns() {
        let descriptor = test_descriptor();
        let mut node = ProjectNode::new(descriptor, &["name", "age"]);

        let id1 = ObjectId::new();
        let tuple1 = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Text("alice@example.com".into()),
                Value::Integer(30),
            ],
        );

        let delta = TupleDelta {
            added: vec![tuple1],
            removed: vec![],
            moved: vec![],
            updated: vec![],
        };

        let result = node.process(delta);

        assert_eq!(result.added.len(), 1);

        let projected = &result.added[0];
        let row = projected.to_single_row().unwrap();
        let values = decode_row(node.output_descriptor(), &row.data).unwrap();

        assert_eq!(values.len(), 2);
        assert_eq!(values[0], Value::Text("Alice".into()));
        assert_eq!(values[1], Value::Integer(30));
    }

    #[test]
    fn project_output_descriptor_has_selected_columns() {
        let descriptor = test_descriptor();
        let node = ProjectNode::new(descriptor, &["name", "age"]);

        let output = node.output_descriptor();
        assert_eq!(output.columns.len(), 2);
        assert_eq!(output.columns[0].name, "name");
        assert_eq!(output.columns[1].name, "age");
    }

    #[test]
    fn project_preserves_tuple_identity() {
        let descriptor = test_descriptor();
        let mut node = ProjectNode::new(descriptor, &["name"]);

        let id1 = ObjectId::new();
        let tuple1 = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Text("alice@example.com".into()),
                Value::Integer(30),
            ],
        );

        let delta = TupleDelta {
            added: vec![tuple1],
            removed: vec![],
            moved: vec![],
            updated: vec![],
        };

        node.process(delta);

        assert_eq!(node.current_tuples().len(), 1);
        let projected = node.current_tuples().iter().next().unwrap();
        assert_eq!(projected.ids()[0], id1);
    }

    #[test]
    fn project_handles_updates() {
        let descriptor = test_descriptor();
        let mut node = ProjectNode::new(descriptor, &["name", "age"]);

        let id1 = ObjectId::new();
        let old_tuple = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Text("alice@example.com".into()),
                Value::Integer(30),
            ],
        );
        let new_tuple = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Text("alice@newmail.com".into()),
                Value::Integer(31),
            ],
        );

        node.process(TupleDelta {
            added: vec![old_tuple.clone()],
            removed: vec![],
            moved: vec![],
            updated: vec![],
        });

        let result = node.process(TupleDelta {
            added: vec![],
            removed: vec![],
            moved: vec![],
            updated: vec![(old_tuple, new_tuple)],
        });

        assert_eq!(result.updated.len(), 1);

        let (_, new_projected) = &result.updated[0];
        let row = new_projected.to_single_row().unwrap();
        let values = decode_row(node.output_descriptor(), &row.data).unwrap();
        assert_eq!(values[1], Value::Integer(31));
    }

    #[test]
    fn project_ignores_unknown_columns() {
        let descriptor = test_descriptor();
        let node = ProjectNode::new(descriptor, &["name", "nonexistent", "age"]);

        let output = node.output_descriptor();
        assert_eq!(output.columns.len(), 2);
        assert_eq!(output.columns[0].name, "name");
        assert_eq!(output.columns[1].name, "age");
    }

    #[test]
    fn precise_project_uses_scopes_and_aliases() {
        let users = RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]);
        let posts = RowDescriptor::new(vec![ColumnDescriptor::new("title", ColumnType::Text)]);
        let input = TupleDescriptor::from_tables(&[
            ("u".to_string(), users.clone()),
            ("p".to_string(), posts.clone()),
        ])
        .with_all_materialized();
        let node = ProjectNode::with_project_columns(
            input,
            &[
                ProjectColumn {
                    alias: "author_name".into(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("u", "name")),
                },
                ProjectColumn {
                    alias: "post_title".into(),
                    expr: ProjectExpr::Column(ColumnRef::scoped("p", "title")),
                },
            ],
        )
        .expect("precise projection should build");

        assert_eq!(node.output_descriptor().columns.len(), 2);
        assert_eq!(node.output_descriptor().columns[0].name, "author_name");
        assert_eq!(node.output_descriptor().columns[1].name, "post_title");

        let user_id = ObjectId::new();
        let post_id = ObjectId::new();
        let user_row = encode_row(&users, &[Value::Text("Alice".into())]).unwrap();
        let post_row = encode_row(&posts, &[Value::Text("Hello".into())]).unwrap();
        let tuple = Tuple::new(vec![
            TupleElement::Row {
                id: user_id,
                content: user_row,
                commit_id: CommitId([1; 32]),
            },
            TupleElement::Row {
                id: post_id,
                content: post_row,
                commit_id: CommitId([2; 32]),
            },
        ]);

        let result = node
            .project_tuple(&tuple)
            .expect("tuple should project to a single row");
        let row = result
            .to_single_row()
            .expect("projected tuple should be a row");
        let values = decode_row(node.output_descriptor(), &row.data).unwrap();

        assert_eq!(row.id, user_id);
        assert_eq!(
            values,
            vec![Value::Text("Alice".into()), Value::Text("Hello".into())]
        );
    }

    #[test]
    fn precise_project_can_expose_scoped_row_id() {
        let users = RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]);
        let input =
            TupleDescriptor::from_tables(&[("u".to_string(), users)]).with_all_materialized();
        let node = ProjectNode::with_project_columns(
            input,
            &[ProjectColumn {
                alias: "row_id".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("u", "_id")),
            }],
        )
        .expect("row id projection should build");

        assert_eq!(
            node.output_descriptor().columns[0].column_type,
            ColumnType::Uuid
        );
        assert_eq!(node.output_descriptor().columns[0].references, None);
        assert_eq!(node.output_descriptor().columns[0].name, "row_id");
    }
}
