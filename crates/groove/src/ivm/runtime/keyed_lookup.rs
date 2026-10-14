//! Keyed join inputs: read a stored table by the other side's join keys
//! instead of arranging all of it.
//!
//! A join arranges both inputs, so a join against a base table holds the
//! whole table in memory even when only a handful of its rows can ever
//! match. When one input is a stored table seen through key-preserving
//! operators and the join key is the table's primary key (after its static
//! scan prefix), that input is rewritten to the same operators over a
//! [`LookupOp`] probed by the other input's keys. The table rows then stay in
//! storage, where the storage engine's cache is the only memory they take,
//! and the join arranges only rows that can match.
//!
//! The rewrite is exact for the join: it only drops table rows whose key is
//! not live on the other side, and those rows never match. Each operator it
//! looks through therefore has to commute with "keep only rows with a live
//! key": row-wise operators do, a union does branch by branch, and a
//! per-group winner does when the key is part of its group.
//!
//! It only pays when the keys are few. A lookup probes storage once per key
//! and holds the rows it found, so keys drawn from a whole table would cost
//! one probe per row and hold the matched rows twice, against one scan and
//! one arrangement. The keys side therefore has to be narrow
//! ([`IvmRuntime::narrow_relation`]): every row of it is reached through an
//! index probe, a point or bounded read, a binding, inline rows, a filter
//! pinning a field to one UUID or another keyed lookup. Without cardinality statistics this is the structural
//! stand-in for "the keys side is small".

use super::*;

/// Which inputs of a join may be read by the other input's keys.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum KeyedJoinKind {
    /// Inner join or semi-join: rows of either side without a partner never
    /// reach the output, so either side may be narrowed.
    EitherSide,
    /// Anti-join: every left row matters, so only the right side may be
    /// narrowed to the left side's keys.
    RightSideOnly,
}

impl IvmRuntime {
    /// Rewrite a join's arrangement inputs so a side that is a stored table
    /// read by key is probed by the other side's keys instead of arranged
    /// whole. The right side is preferred: Jazz lowers the public relation on
    /// the left and the related relation on the right. Returns the (possibly
    /// unchanged) arrangement inputs.
    pub(super) fn keyed_join_inputs(
        &mut self,
        kind: KeyedJoinKind,
        left: NodeId,
        right: NodeId,
    ) -> (NodeId, NodeId) {
        if let Some(right_keyed) = self.keyed_arrangement(right, left) {
            self.keyed_join_inputs_rewritten += 1;
            return (left, right_keyed);
        }
        if kind == KeyedJoinKind::EitherSide
            && let Some(left_keyed) = self.keyed_arrangement(left, right)
        {
            self.keyed_join_inputs_rewritten += 1;
            return (left_keyed, right);
        }
        (left, right)
    }

    /// The keyed rewrite for an installed join node's operator, if any.
    pub(super) fn keyed_join_kind(operator: &OpType) -> Option<KeyedJoinKind> {
        match operator {
            OpType::Join(join) | OpType::SemiJoin(join)
                if join.kind == JoinOpKind::Inner && join.residual_predicate.is_none() =>
            {
                Some(KeyedJoinKind::EitherSide)
            }
            OpType::AntiJoin(join)
                if join.kind == JoinOpKind::Inner && join.residual_predicate.is_none() =>
            {
                Some(KeyedJoinKind::RightSideOnly)
            }
            _ => None,
        }
    }

    /// `target`'s arrangement over its input narrowed to the keys of `keys`'
    /// arrangement, when the input can be read by key.
    fn keyed_arrangement(&mut self, target: NodeId, keys: NodeId) -> Option<NodeId> {
        let (target_op, target_input) = self.arrangement_parts(target)?;
        let (keys_op, keys_input) = self.arrangement_parts(keys)?;
        if target_op.fields.is_empty() || target_op.fields.len() != keys_op.fields.len() {
            return None;
        }
        if !self.narrow_relation(keys_input, &mut HashMap::default()) {
            return None;
        }
        let target_desc = self.record_output(target_input)?;
        let key_desc = self.record_output(keys_input)?;
        let target_fields = target_op
            .fields
            .iter()
            .map(|field| resolve_field_name(&target_desc, field))
            .collect::<Option<Vec<_>>>()?;
        let key_fields = keys_op
            .fields
            .iter()
            .map(|field| resolve_field_name(&key_desc, field))
            .collect::<Option<Vec<_>>>()?;
        let keyed = self.keyed_input(
            target_input,
            &target_fields,
            &KeySource {
                node: keys_input,
                descriptor: key_desc,
                fields: key_fields,
            },
        )?;
        Some(self.add_arrangement_node(keyed, target_desc, target_op.fields, target_op.comparison))
    }

    /// Whether `node` holds few rows: every row is reached through a
    /// narrowing read (an index probe, a point or bounded table read, a
    /// binding, inline rows, or a keyed lookup by narrow keys) or passed a
    /// filter pinning a field to one UUID. An inner join or semi-join is narrow
    /// when either input is, since its rows are selected by that input's
    /// keys; an anti-join when its left input is; a union when every branch
    /// is. Table scans under at most a static prefix, and anything this does
    /// not recognise, are not narrow.
    fn narrow_relation(&self, node: NodeId, memo: &mut HashMap<NodeId, bool>) -> bool {
        if let Some(narrow) = memo.get(&node) {
            return *narrow;
        }
        let Some(graph_node) = self.graph.node(node) else {
            return false;
        };
        let inputs = &graph_node.descriptor.inputs;
        let narrow = match &graph_node.descriptor.operator {
            OpType::TableSource(source) => matches!(
                source.scan,
                Some(
                    StaticScanSpec::Point(_)
                        | StaticScanSpec::PrefixLimit { .. }
                        | StaticScanSpec::ReversePrefixLimit { .. }
                )
            ),
            OpType::IndexSource(index) => index.scan.is_some(),
            OpType::BindingSource(_) | OpType::InlineRecords(_) => true,
            OpType::Lookup(_) => inputs
                .first()
                .is_some_and(|keys| self.narrow_relation(*keys, memo)),
            OpType::Join(_) | OpType::SemiJoin(_) => inputs
                .iter()
                .any(|input| self.narrow_relation(*input, memo)),
            OpType::AntiJoin(_) => inputs
                .first()
                .is_some_and(|left| self.narrow_relation(*left, memo)),
            OpType::Union => {
                !inputs.is_empty()
                    && inputs
                        .iter()
                        .all(|input| self.narrow_relation(*input, memo))
            }
            OpType::Filter(filter) if selective_predicate(&filter.predicate) => true,
            OpType::Arrange(_)
            | OpType::Filter(_)
            | OpType::MapProject(_)
            | OpType::Distinct
            | OpType::Negate
            | OpType::UnwrapNullable(_)
            | OpType::VariantProject(_)
            | OpType::Unnest(_)
            | OpType::ArgMaxBy(_)
            | OpType::ArgMinBy(_)
            | OpType::TopBy(_)
            | OpType::Aggregate(_) => inputs.len() == 1 && self.narrow_relation(inputs[0], memo),
            _ => false,
        };
        memo.insert(node, narrow);
        narrow
    }

    fn arrangement_parts(&self, arrangement: NodeId) -> Option<(ArrangeOp, NodeId)> {
        let descriptor = &self.graph.node(arrangement)?.descriptor;
        let OpType::Arrange(op) = &descriptor.operator else {
            return None;
        };
        Some((op.clone(), *descriptor.inputs.first()?))
    }

    fn record_output(&self, node: NodeId) -> Option<RecordDescriptor> {
        match self.graph.node(node)?.descriptor.output {
            NodeOutput::Records(descriptor) => Some(descriptor),
            NodeOutput::Arrangement(_) => None,
        }
    }

    /// `node` narrowed to rows whose `fields` equal some live key of `keys`,
    /// or `None` when no stored table below it can be read by those fields.
    /// Rows with a live key are exactly those of `node`.
    fn keyed_input(&mut self, node: NodeId, fields: &[usize], keys: &KeySource) -> Option<NodeId> {
        let descriptor = self.graph.node(node)?.descriptor.clone();
        let output = descriptor.output.records();
        let keyed_inputs = match &descriptor.operator {
            OpType::TableSource(source) => {
                return self.keyed_table_source(source, output, fields, keys);
            }
            // Row-wise: a row's key, and whether it passes, are its own.
            OpType::Filter(_) | OpType::Distinct | OpType::Negate | OpType::UnwrapNullable(_) => {
                let [input] = descriptor.inputs.as_slice() else {
                    return None;
                };
                vec![self.keyed_input(*input, fields, keys)?]
            }
            OpType::MapProject(project) => {
                let [input] = descriptor.inputs.as_slice() else {
                    return None;
                };
                let input_desc = self.record_output(*input)?;
                // Each key field must be a plain copy of an input field, so
                // the input's key is the output's key.
                let input_fields = fields
                    .iter()
                    .map(|field| match &project.expressions.get(*field)?.expression {
                        ProjectExpr::Field(source) => resolve_field_ref(&input_desc, source).ok(),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()?;
                vec![self.keyed_input(*input, &input_fields, keys)?]
            }
            // A winner per group stays the winner when whole groups are
            // kept or dropped, which holds when the key lies in the group.
            OpType::ArgMaxBy(ArgMaxByOp {
                group_field_indices,
                ..
            })
            | OpType::ArgMinBy(ArgMinByOp {
                group_field_indices,
                ..
            }) => {
                if !fields
                    .iter()
                    .all(|field| group_field_indices.contains(field))
                {
                    return None;
                }
                // The winner is chosen over an arrangement of the input.
                let [input] = descriptor.inputs.as_slice() else {
                    return None;
                };
                let (arrange, arranged) = self.arrangement_parts(*input)?;
                let arranged_desc = self.record_output(arranged)?;
                let keyed = self.keyed_input(arranged, fields, keys)?;
                vec![self.add_arrangement_node(
                    keyed,
                    arranged_desc,
                    arrange.fields,
                    arrange.comparison,
                )]
            }
            // Union inputs share the output descriptor. A branch that cannot
            // be read by key stays whole, which is still exact.
            OpType::Union => {
                let mut any = false;
                let inputs = descriptor
                    .inputs
                    .iter()
                    .map(|input| match self.keyed_input(*input, fields, keys) {
                        Some(keyed) => {
                            any = true;
                            keyed
                        }
                        None => *input,
                    })
                    .collect::<Vec<_>>();
                if !any {
                    return None;
                }
                inputs
            }
            // A semi- or anti-join keeps or drops each left row on its own;
            // its output is the left input's.
            OpType::SemiJoin(_) | OpType::AntiJoin(_) => {
                let [left, right] = descriptor.inputs.as_slice() else {
                    return None;
                };
                let (left_op, left_input) = self.arrangement_parts(*left)?;
                let left_desc = self.record_output(left_input)?;
                let keyed = self.keyed_input(left_input, fields, keys)?;
                let left =
                    self.add_arrangement_node(keyed, left_desc, left_op.fields, left_op.comparison);
                vec![left, *right]
            }
            _ => return None,
        };
        Some(self.keyed_node(descriptor.operator, keyed_inputs, output))
    }

    fn keyed_table_source(
        &mut self,
        source: &TableSourceOp,
        output: RecordDescriptor,
        fields: &[usize],
        keys: &KeySource,
    ) -> Option<NodeId> {
        let prefix_len = match &source.scan {
            None => 0,
            Some(StaticScanSpec::Prefix(values)) => values.len(),
            // A point already reads one key; bounded and ordered pages are
            // not plain prefixes.
            Some(_) => return None,
        };
        let table = self.schema.table(&source.table)?;
        let primary_key = primary_key_field_indices(table, &output).ok()?;
        // The probe is the storage encoding of the key values, so the join
        // fields must be the primary-key columns right after the scan
        // prefix, in order, and each key must have its column's exact type.
        if primary_key.get(prefix_len..prefix_len + fields.len())? != fields {
            return None;
        }
        let types_match = fields.iter().zip(&keys.fields).all(|(column, key)| {
            output.fields()[*column].value_type == keys.descriptor.fields()[*key].value_type
        });
        if !types_match {
            return None;
        }
        Some(self.keyed_node(
            OpType::Lookup(LookupOp {
                source: source.clone(),
                key_descriptor: keys.descriptor,
                key_fields: keys.fields.clone(),
            }),
            vec![keys.node],
            output,
        ))
    }

    fn keyed_node(
        &mut self,
        operator: OpType,
        inputs: Vec<NodeId>,
        output: RecordDescriptor,
    ) -> NodeId {
        self.logical_nodes_requested += 1;
        let node = self.graph.dedup_node(
            NodeDescriptor::new(operator, inputs, output),
            NodeDurability::Ephemeral,
        );
        self.initialize_node_runtime(node);
        node
    }
}

/// The relation whose live keys select table rows.
struct KeySource {
    node: NodeId,
    descriptor: RecordDescriptor,
    fields: Vec<usize>,
}

/// Whether rows passing `predicate` are few whatever the input: it pins a
/// field to one identifier. Only UUIDs count; an equality on a bool, enum,
/// number or string may still select most of a table.
fn selective_predicate(predicate: &PredicateExpr) -> bool {
    match predicate {
        PredicateExpr::Eq { value, .. } => identifier_literal(value),
        PredicateExpr::And(predicates) => predicates.iter().any(selective_predicate),
        _ => false,
    }
}

fn identifier_literal(value: &LiteralValue) -> bool {
    match value {
        LiteralValue::Uuid(_) => true,
        LiteralValue::Nullable(Some(value)) => identifier_literal(value),
        _ => false,
    }
}
