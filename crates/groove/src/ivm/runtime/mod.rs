//! Synchronous IVM graph runtime and tick-loop narrative.
//!
//! This module owns executable state for hash-consed graphs: subscriptions,
//! prepared-shape bindings, durable index nodes, per-operator state, reusable
//! join arrangements, recursive state, and per-tick memoization. The reading
//! order is tick-loop first: start at [`IvmRuntime::tick_with_params`], then
//! follow [`TickEvaluator::update_node`] to operator evaluation. Subscription
//! setup, graph insertion, retainers, and GC live after that narrative. Query
//! lowering lives in [`crate::ivm::planner`], graph identity in
//! [`crate::ivm::graph`], and storage mechanics in [`crate::storage`].

use bytes::{Bytes, BytesMut};
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{
    Arc, OnceLock,
    mpsc::{self, Receiver, RecvError, Sender, TryRecvError},
};

use rustc_hash::FxHashMap as HashMap;

use crate::ivm::{
    AggregateExpr, AggregateFunction, AggregateOp, ArgMaxByOp, ArgMinByOp, BindingSourceOp,
    CollectByBuilder, CollectByField, CollectByMode, CollectByOp, CollectByProjection,
    CollectBySlot, CollectBySlotBuilder, DurableStorage, FieldRef, FilterOp, FrontierName,
    FrontierSourceOp, GraphBuilder, IndexByOp, IndexSourceOp, InlineRecordsOp, IvmGraph, JoinOp,
    JoinOpKind, LiteralValue, MAX_COLLECT_BY_TREE_DEPTH, MapProjectOp, NodeDescriptor,
    NodeDurability, NodeId, OpType, PersistOp, PlanExpr, PredicateExpr, ProjectExpr, ProjectField,
    ProjectionExpr, RecursiveEnumRemaps, RecursiveOp, Retainer, StaticScanSpec, TableSourceOp,
    TopByDirection, TopByLimit, TopByOp, TopByOrderField, UnnestOp, UnwrapNullableOp,
    ValueComparison, VariantProjectOp, VariantProjectionTarget,
};
use crate::records::{
    self, BorrowedRecord, EnumSchema, EnumValue, OwnedRecord, RawProjectionField,
    RawProjectionScratch, RecordDescriptor, Value, ValueType,
};
use crate::schema::{DatabaseSchema, IndexSchema, TableSchema};
use crate::storage::{
    OrderedKvStorage, OwnedWriteOperation, RecordStore, StagedWriteOverlay, StagedWriteState,
};
use thiserror::Error;

mod join;
mod persist;
mod recursion;

use join::{AntiJoinState, ArrangementState, JoinState, touched_join_keys};
use persist::apply_persist_delta;
use recursion::{
    RecursiveState, hydrate_recursive_arrangements, recompute_recursive, recursive_delta,
    recursive_read_tables, snapshot_table_deltas,
};

const DEFAULT_SINK: &str = "__default";
const EVAL_MEMO_MAX_ENTRIES: usize = 8192;
const EVAL_MEMO_MAX_BYTES: usize = 128 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct VariantProjectionKey {
    table: String,
    target: VariantProjectionTarget,
}

#[derive(Clone, Debug)]
pub(super) struct VariantProjection {
    output: RecordDescriptor,
    cases: HashMap<u32, VariantProjectionCase>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum VariantProjectionCase {
    Project {
        source: RecordDescriptor,
        project: MapProjectOp,
        raw_projection: Option<Arc<[RawProjectionField]>>,
    },
    Ignore {
        source: RecordDescriptor,
    },
    Enum {
        source: RecordDescriptor,
        tag: u32,
        payload: RecordDescriptor,
        project: MapProjectOp,
        raw_projection: Option<Arc<[RawProjectionField]>>,
    },
}

impl VariantProjectionCase {
    fn source(&self) -> RecordDescriptor {
        match self {
            Self::Project { source, .. } | Self::Ignore { source } | Self::Enum { source, .. } => {
                *source
            }
        }
    }
}

// These maps are keyed by local runtime/schema/graph metadata produced after
// validation. Wire-facing or otherwise adversarial-input maps must keep the
// standard hasher; this alias is intentionally scoped to the IVM runtime.

/// Stateful executor for deduplicated IVM graphs and subscriptions.
#[derive(Clone, Debug)]
pub struct IvmRuntime {
    schema: DatabaseSchema,
    table_descriptors: HashMap<String, RecordDescriptor>,
    /// Append-only, fixed-output projection families for heterogeneous table
    /// sources. Cases are runtime input metadata rather than graph identity.
    variant_projections: HashMap<VariantProjectionKey, VariantProjection>,
    graph: IvmGraph,
    multisink_subscriptions: HashMap<SubscriptionId, MultisinkSubscriptionState>,
    prepared_shapes: HashMap<PreparedShapeId, RoutedMultisinkShapeState>,
    auto_direct_families: HashMap<AutoDirectFamilyKey, PreparedShapeId>,
    binding_sources: HashMap<String, BindingSourceState>,
    /// Binding retractions discovered while routing notifications cannot tick
    /// recursively; the next public tick drains them before user deltas run.
    pending_binding_retractions: Vec<BindingDelta>,
    /// Persistent operator state keyed by scope and node. This survives ticks;
    /// see [`EvalMemoKey`] for per-evaluation caching.
    operator_states: HashMap<OperatorStateKey, OperatorState>,
    /// Reusable indexed multisets for join inputs. These are keyed by input
    /// fragment, key fields, descriptor, and scope so similar queries can share
    /// expensive context-independent arrangements.
    arrangement_states: HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    /// Input-owned memoization for pure node evaluation results. Entries are
    /// keyed by node/scope/context inputs and validated against per-input
    /// frontier counters before reuse; operator state remains owned separately.
    eval_memo: HashMap<EvalMemoKey, EvalMemoEntry>,
    table_frontiers: HashMap<String, u64>,
    binding_frontiers: HashMap<String, u64>,
    memo_use_clock: u64,
    eval_memo_bytes: usize,
    hydration_memo_hits: u64,
    hydration_memo_computes: u64,
    hydration_memo_computed_nodes: HashSet<NodeId>,
    /// Retainers and GC age live outside operator state so stateless leaf nodes
    /// can be retained without allocating fake operator state.
    node_meta: HashMap<NodeId, NodeRuntimeMeta>,
    current_tick: u64,
    next_subscription_id: u64,
    next_shape_id: u64,
    logical_nodes_requested: u64,
    auto_direct_family_enabled: bool,
    collect_tick_runtime_stats: bool,
}

impl IvmRuntime {
    pub fn new(schema: DatabaseSchema) -> Result<Self, IvmRuntimeError> {
        let table_descriptors = schema
            .tables
            .iter()
            .filter(|table| !table.has_variants())
            .map(|table| (table.name.clone(), table.record_schema()))
            .collect();
        let mut runtime = Self {
            schema,
            table_descriptors,
            variant_projections: HashMap::default(),
            graph: IvmGraph::new(),
            multisink_subscriptions: HashMap::default(),
            operator_states: HashMap::default(),
            arrangement_states: HashMap::default(),
            eval_memo: HashMap::default(),
            table_frontiers: HashMap::default(),
            binding_frontiers: HashMap::default(),
            memo_use_clock: 0,
            eval_memo_bytes: 0,
            hydration_memo_hits: 0,
            hydration_memo_computes: 0,
            hydration_memo_computed_nodes: HashSet::default(),
            node_meta: HashMap::default(),
            current_tick: 0,
            next_subscription_id: 1,
            next_shape_id: 1,
            logical_nodes_requested: 0,
            auto_direct_family_enabled: true,
            collect_tick_runtime_stats: false,
            prepared_shapes: HashMap::default(),
            auto_direct_families: HashMap::default(),
            binding_sources: HashMap::default(),
            pending_binding_retractions: Vec::new(),
        };
        runtime.define_schema_index_variant_projections()?;
        runtime.add_dedup_schema_indices()?;
        Ok(runtime)
    }

    pub fn set_tick_runtime_stats_enabled(&mut self, enabled: bool) {
        self.collect_tick_runtime_stats = enabled;
    }

    pub fn graph(&self) -> &IvmGraph {
        &self.graph
    }

    pub fn set_auto_direct_family_enabled(&mut self, enabled: bool) {
        self.auto_direct_family_enabled = enabled;
    }

    pub fn schema(&self) -> &DatabaseSchema {
        &self.schema
    }

    pub fn table(&self, table: &str) -> Option<&TableSchema> {
        self.schema.table(table)
    }

    pub fn table_descriptor(&self, table: &str) -> Option<&RecordDescriptor> {
        self.table_descriptors.get(table)
    }

    pub(crate) fn register_table(&mut self, table: TableSchema) -> Result<(), IvmRuntimeError> {
        if self.schema.table(&table.name).is_some() {
            return Err(IvmRuntimeError::TableAlreadyExists(table.name));
        }
        if !table.has_variants() {
            self.table_descriptors
                .insert(table.name.clone(), table.record_schema());
        }
        self.schema.tables.push(table);
        self.define_schema_index_variant_projections()?;
        self.add_dedup_schema_indices()?;
        Ok(())
    }

    pub(crate) fn register_table_variant_with_columns(
        &mut self,
        table: &str,
        columns: Vec<crate::schema::ColumnSchema>,
        variant_tag: crate::schema::TableVariant,
    ) -> Result<(), IvmRuntimeError> {
        let table_schema = self
            .schema
            .tables
            .iter_mut()
            .find(|candidate| candidate.name == table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.to_owned()))?;
        if table_schema.variant(variant_tag.tag).is_some() {
            return Err(IvmRuntimeError::DuplicateTableVariant {
                table: table.to_owned(),
                version: u64::from(variant_tag.tag),
            });
        }
        for column in columns {
            if table_schema
                .columns
                .iter()
                .any(|existing| existing.name == column.name)
            {
                return Err(IvmRuntimeError::TableFieldAlreadyExists {
                    table: table.to_owned(),
                    field: column.name,
                });
            }
            table_schema.columns.push(column);
        }
        let version = variant_tag.tag;
        for field in &variant_tag.payload_fields {
            field
                .value_type
                .collect_variant_registries(&mut table_schema.value_variant_registries);
        }
        table_schema.variants.push(variant_tag);
        let table_schema = table_schema.clone();
        for index in &table_schema.indices {
            self.register_schema_index_variant_case(&table_schema, index, version)?;
        }
        self.invalidate_table_inputs(table);
        Ok(())
    }

    pub(crate) fn evolve_table_variant_registries(
        &mut self,
        table: &str,
        columns: &[crate::schema::ColumnSchema],
    ) -> Result<(), IvmRuntimeError> {
        let table_schema = self
            .schema
            .tables
            .iter_mut()
            .find(|candidate| candidate.name == table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.to_owned()))?;
        for desired in columns {
            let Some(existing) = table_schema
                .columns
                .iter_mut()
                .find(|column| column.name == desired.name)
            else {
                continue;
            };
            if !existing
                .column_type
                .registry_compatible_with(&desired.column_type)
            {
                return Err(IvmRuntimeError::TableFieldAlreadyExists {
                    table: table.to_owned(),
                    field: desired.name.clone(),
                });
            }
            existing.column_type = desired.column_type.clone();
            desired
                .column_type
                .collect_variant_registries(&mut table_schema.value_variant_registries);
        }
        self.invalidate_table_inputs(table);
        Ok(())
    }

    pub(crate) fn register_table_index<S>(
        &mut self,
        table: &str,
        index: IndexSchema,
        storage: &S,
    ) -> Result<(), IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let table_position = self
            .schema
            .tables
            .iter()
            .position(|candidate| candidate.name == table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.to_owned()))?;
        self.schema.tables[table_position]
            .indices
            .push(index.clone());
        let table_schema = self.schema.tables[table_position].clone();
        if table_schema.has_variants() {
            let target = VariantProjectionTarget::SchemaIndex(index.name.clone());
            self.define_variant_projection_target(
                table,
                target,
                schema_index_input_descriptor(&table_schema, &index)?,
            )?;
            for variant_tag in &table_schema.variants {
                self.register_schema_index_variant_case(&table_schema, &index, variant_tag.tag)?;
            }
        }
        let persist = self.add_dedup_schema_index(&table_schema, &index)?;
        self.invalidate_table_inputs(table);
        self.hydration_snapshot(persist, storage, HydrationMode::Ordinary)?;
        Ok(())
    }

    pub(crate) fn define_variant_projection(
        &mut self,
        table: &str,
        target: &str,
        output: RecordDescriptor,
    ) -> Result<(), IvmRuntimeError> {
        self.define_variant_projection_target(
            table,
            VariantProjectionTarget::Named(target.to_owned()),
            output,
        )
    }

    fn define_variant_projection_target(
        &mut self,
        table: &str,
        target: VariantProjectionTarget,
        output: RecordDescriptor,
    ) -> Result<(), IvmRuntimeError> {
        if self.schema.table(table).is_none() {
            return Err(IvmRuntimeError::TableNotFound(table.to_owned()));
        }
        let key = VariantProjectionKey {
            table: table.to_owned(),
            target: target.clone(),
        };
        match self.variant_projections.entry(key) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(VariantProjection {
                    output,
                    cases: HashMap::default(),
                });
            }
            std::collections::hash_map::Entry::Occupied(mut entry)
                if record_descriptors_registry_compatible(&entry.get().output, &output) =>
            {
                entry.get_mut().output = output;
            }
            std::collections::hash_map::Entry::Occupied(_) => {
                return Err(IvmRuntimeError::VariantProjectionOutputMismatch {
                    table: table.to_owned(),
                    target: variant_projection_target_name(&target).to_owned(),
                });
            }
        }
        Ok(())
    }

    pub(crate) fn register_variant_projection_case(
        &mut self,
        table: &str,
        target: &str,
        variant_tag: u32,
        fields: &[ProjectField],
    ) -> Result<(), IvmRuntimeError> {
        self.register_variant_projection_target_case(
            table,
            VariantProjectionTarget::Named(target.to_owned()),
            variant_tag,
            Some(fields),
        )
    }

    /// Append a physical source case that constructs one stable logical enum
    /// value. The outer projection descriptor stays fixed; the selected enum
    /// case owns the dense payload descriptor used for this source layout.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn register_variant_projection_enum_case(
        &mut self,
        table: &str,
        target: &str,
        variant_tag: u32,
        enum_field: &str,
        enum_schema: &EnumSchema,
        enum_case: &str,
        fields: &[ProjectField],
    ) -> Result<(), IvmRuntimeError> {
        let source = self
            .schema
            .table(table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.to_owned()))?
            .record_schema_for_variant(variant_tag)
            .ok_or_else(|| IvmRuntimeError::UnknownTableVariant {
                table: table.to_owned(),
                version: u64::from(variant_tag),
            })?;
        let key = VariantProjectionKey {
            table: table.to_owned(),
            target: VariantProjectionTarget::Named(target.to_owned()),
        };
        let projection = self.variant_projections.get_mut(&key).ok_or_else(|| {
            IvmRuntimeError::VariantProjectionNotFound {
                table: table.to_owned(),
                target: target.to_owned(),
            }
        })?;
        let output_fields = projection.output.fields();
        let Some(union_idx) = projection.output.field_index(enum_field) else {
            return Err(IvmRuntimeError::VariantProjectionEnumFieldNotFound {
                table: table.to_owned(),
                target: target.to_owned(),
                field: enum_field.to_owned(),
            });
        };
        if output_fields.len() != 1 {
            return Err(
                IvmRuntimeError::VariantProjectionEnumOutputMustBeSingleField {
                    table: table.to_owned(),
                    target: target.to_owned(),
                },
            );
        }
        let ValueType::Enum(output_schema) = &output_fields[union_idx].value_type else {
            return Err(IvmRuntimeError::VariantProjectionEnumFieldTypeMismatch {
                table: table.to_owned(),
                target: target.to_owned(),
                field: enum_field.to_owned(),
            });
        };
        if output_schema.as_ref() != enum_schema {
            return Err(IvmRuntimeError::VariantProjectionEnumSchemaMismatch {
                table: table.to_owned(),
                target: target.to_owned(),
                field: enum_field.to_owned(),
            });
        }
        let tag = enum_schema.tag(enum_case)?;
        let payload = enum_schema.case(tag)?.payload;
        let projected = project_descriptor(&source, fields)?;
        if projected != payload {
            return Err(IvmRuntimeError::VariantProjectionEnumPayloadMismatch {
                table: table.to_owned(),
                target: target.to_owned(),
                case: enum_case.to_owned(),
            });
        }
        let mapping = fields
            .iter()
            .filter_map(|field| {
                field.source().map(|source_field| {
                    resolve_field_ref(&source, source_field).map(|idx| (0, idx))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let project = MapProjectOp {
            expressions: fields
                .iter()
                .map(|field| {
                    project_field_expr(&source, field).map(|expression| ProjectionExpr {
                        expression,
                        output_name: Some(field.output_name.clone()),
                    })
                })
                .collect::<Result<Vec<_>, IvmRuntimeError>>()?,
            mapping,
        };
        let raw_projection = raw_projection_fields(&project, &source, payload)?
            .map(Arc::<[RawProjectionField]>::from);
        let case = VariantProjectionCase::Enum {
            source,
            tag,
            payload,
            project,
            raw_projection,
        };
        match projection.cases.entry(variant_tag) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(case);
            }
            std::collections::hash_map::Entry::Occupied(entry) if entry.get() == &case => {}
            std::collections::hash_map::Entry::Occupied(_) => {
                return Err(IvmRuntimeError::VariantProjectionCaseAlreadyRegistered {
                    table: table.to_owned(),
                    target: target.to_owned(),
                    version: u64::from(variant_tag),
                });
            }
        }
        self.invalidate_table_inputs(table);
        Ok(())
    }

    pub(crate) fn register_variant_projection_ignore_case(
        &mut self,
        table: &str,
        target: &str,
        variant_tag: u32,
    ) -> Result<(), IvmRuntimeError> {
        self.register_variant_projection_target_case(
            table,
            VariantProjectionTarget::Named(target.to_owned()),
            variant_tag,
            None,
        )
    }

    pub(crate) fn project_variant_record(
        &self,
        table: &str,
        target: &str,
        record: &records::VariantRecord,
    ) -> Result<Option<OwnedRecord>, IvmRuntimeError> {
        let key = VariantProjectionKey {
            table: table.to_owned(),
            target: VariantProjectionTarget::Named(target.to_owned()),
        };
        let projection = self.variant_projections.get(&key).ok_or_else(|| {
            IvmRuntimeError::VariantProjectionNotFound {
                table: table.to_owned(),
                target: target.to_owned(),
            }
        })?;
        let case = projection.cases.get(&record.variant_tag()).ok_or_else(|| {
            IvmRuntimeError::VariantProjectionCaseNotFound {
                table: table.to_owned(),
                target: target.to_owned(),
                version: u64::from(record.variant_tag()),
            }
        })?;
        if !case
            .source()
            .registry_compatible_with(record.record().descriptor())
        {
            return Err(IvmRuntimeError::VariantProjectionSourceMismatch {
                table: table.to_owned(),
                target: target.to_owned(),
                version: u64::from(record.variant_tag()),
            });
        }
        let input = RecordDeltas {
            descriptor: *record.record().descriptor(),
            deltas: vec![RecordDelta {
                record: Bytes::copy_from_slice(record.record().raw()),
                weight: 1,
            }],
        };
        let projected = match case {
            VariantProjectionCase::Project {
                project,
                raw_projection,
                ..
            } => NodeState::update_map_project(
                project,
                projection.output,
                &input,
                raw_projection.as_deref(),
            )?,
            VariantProjectionCase::Enum {
                tag,
                payload,
                project,
                raw_projection,
                ..
            } => NodeState::update_variant_enum_project(
                *tag,
                *payload,
                project,
                projection.output,
                &input,
                raw_projection.as_deref(),
            )?,
            VariantProjectionCase::Ignore { .. } => return Ok(None),
        };
        let record = projected
            .deltas
            .into_iter()
            .next()
            .ok_or(IvmRuntimeError::GraphOutputMismatch)?;
        Ok(Some(OwnedRecord::new(
            record.record.to_vec(),
            projection.output,
        )))
    }

    fn register_variant_projection_target_case(
        &mut self,
        table: &str,
        target: VariantProjectionTarget,
        variant_tag: u32,
        fields: Option<&[ProjectField]>,
    ) -> Result<(), IvmRuntimeError> {
        let source = self
            .schema
            .table(table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.to_owned()))?
            .record_schema_for_variant(variant_tag)
            .ok_or_else(|| IvmRuntimeError::UnknownTableVariant {
                table: table.to_owned(),
                version: u64::from(variant_tag),
            })?;
        let key = VariantProjectionKey {
            table: table.to_owned(),
            target: target.clone(),
        };
        let projection = self.variant_projections.get_mut(&key).ok_or_else(|| {
            IvmRuntimeError::VariantProjectionNotFound {
                table: table.to_owned(),
                target: variant_projection_target_name(&target).to_owned(),
            }
        })?;
        let case = if let Some(fields) = fields {
            let projected = project_descriptor(&source, fields)?;
            // A recursive enum projector is an explicit descriptor boundary:
            // its source and output may intentionally name different enum
            // registries. The operation validates and re-encodes the values
            // against the fixed output descriptor at execution time, so the
            // usual raw-descriptor compatibility shortcut is inapplicable.
            let recursive_enum_boundary = fields
                .iter()
                .any(|field| matches!(field.expression, ProjectExpr::RecursiveEnumRemap { .. }));
            if !recursive_enum_boundary
                && !record_descriptors_registry_compatible(&projected, &projection.output)
            {
                return Err(IvmRuntimeError::VariantProjectionOutputMismatch {
                    table: table.to_owned(),
                    target: variant_projection_target_name(&target).to_owned(),
                });
            }
            let mapping = fields
                .iter()
                .filter_map(|field| {
                    field.source().map(|source_field| {
                        resolve_field_ref(&source, source_field).map(|idx| (0, idx))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let project = MapProjectOp {
                expressions: fields
                    .iter()
                    .map(|field| {
                        project_field_expr(&source, field).map(|expression| ProjectionExpr {
                            expression,
                            output_name: Some(field.output_name.clone()),
                        })
                    })
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?,
                mapping,
            };
            let raw_projection = raw_projection_fields(&project, &source, projection.output)?
                .map(Arc::<[RawProjectionField]>::from);
            VariantProjectionCase::Project {
                source,
                project,
                raw_projection,
            }
        } else {
            VariantProjectionCase::Ignore { source }
        };
        match projection.cases.entry(variant_tag) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(case);
            }
            std::collections::hash_map::Entry::Occupied(entry) if entry.get() == &case => {}
            std::collections::hash_map::Entry::Occupied(_) => {
                return Err(IvmRuntimeError::VariantProjectionCaseAlreadyRegistered {
                    table: table.to_owned(),
                    target: variant_projection_target_name(&target).to_owned(),
                    version: u64::from(variant_tag),
                });
            }
        }
        self.invalidate_table_inputs(table);
        Ok(())
    }

    fn define_schema_index_variant_projections(&mut self) -> Result<(), IvmRuntimeError> {
        for table in self.schema.tables.clone() {
            if !table.has_variants() {
                continue;
            }
            for index in &table.indices {
                let target = VariantProjectionTarget::SchemaIndex(index.name.clone());
                self.define_variant_projection_target(
                    &table.name,
                    target,
                    schema_index_input_descriptor(&table, index)?,
                )?;
                for variant_tag in &table.variants {
                    self.register_schema_index_variant_case(&table, index, variant_tag.tag)?;
                }
            }
        }
        Ok(())
    }

    fn register_schema_index_variant_case(
        &mut self,
        table: &TableSchema,
        index: &IndexSchema,
        variant_tag: u32,
    ) -> Result<(), IvmRuntimeError> {
        let version =
            table
                .variant(variant_tag)
                .ok_or_else(|| IvmRuntimeError::UnknownTableVariant {
                    table: table.name.clone(),
                    version: u64::from(variant_tag),
                })?;
        let fields = schema_index_input_fields(table, index)?
            .into_iter()
            .map(|shared| {
                version
                    .payload_name_for_shared(&shared)
                    .map(|local| ProjectField::renamed(local, shared))
            })
            .collect::<Option<Vec<_>>>();
        self.register_variant_projection_target_case(
            &table.name,
            VariantProjectionTarget::SchemaIndex(index.name.clone()),
            variant_tag,
            fields.as_deref(),
        )
    }

    fn invalidate_table_inputs(&mut self, table: &str) {
        *self.table_frontiers.entry(table.to_owned()).or_default() += 1;
        self.eval_memo.retain(|key, _| {
            self.node_meta
                .get(&key.node)
                .and_then(|meta| meta.input_signature.as_ref())
                .is_none_or(|signature| {
                    !signature.tables.iter().any(|candidate| candidate == table)
                })
        });
        self.eval_memo_bytes = self
            .eval_memo
            .values()
            .map(|entry| entry.payload_bytes)
            .sum();
    }

    pub fn index(&self, table: &str, index_name: &str) -> Option<&IndexSchema> {
        self.table(table)?
            .indices
            .iter()
            .find(|index| index.name == index_name)
    }

    pub fn direct_record_store(
        &self,
        store: &str,
    ) -> Option<&crate::schema::DirectRecordStoreSchema> {
        self.schema.direct_record_store(store)
    }

    pub fn query_snapshot<S>(
        &mut self,
        graph: GraphBuilder,
        storage: &S,
    ) -> Result<RecordDeltas, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        self.flush_pending_binding_retractions(storage)?;
        if builder_contains_binding_source(&graph) {
            return Err(IvmRuntimeError::BindingSourceRequiresPrepare);
        }
        self.logical_nodes_requested += count_builder_nodes(&graph) as u64;
        let CompiledNode {
            output,
            node: output_node,
            ..
        } = self.add_dedup_graph(&graph)?;
        let records = self.hydration_snapshot(output_node, storage, HydrationMode::Ordinary);
        for node in self.gc_ephemeral_nodes(0) {
            self.remove_node_runtime(node);
        }
        self.prune_unreferenced_arrangements();
        let records = records?;
        if !records.descriptor.registry_compatible_with(&output) {
            return Err(IvmRuntimeError::GraphOutputMismatch);
        }
        Ok(records)
    }

    pub fn query_snapshots<I, K, S>(
        &mut self,
        sinks: I,
        storage: &S,
    ) -> Result<MultisinkDeltas, IvmRuntimeError>
    where
        I: IntoIterator<Item = (K, GraphBuilder)>,
        K: Into<String>,
        S: OrderedKvStorage,
    {
        self.flush_pending_binding_retractions(storage)?;
        let sinks = sinks
            .into_iter()
            .map(|(sink, graph)| (sink.into(), graph))
            .collect::<Vec<_>>();
        if sinks.is_empty() {
            return Err(IvmRuntimeError::EmptyMultisinkSubscription);
        }
        let mut sink_names = HashSet::new();
        for (sink, graph) in &sinks {
            if !sink_names.insert(sink.clone()) {
                return Err(IvmRuntimeError::DuplicateMultisinkSink(sink.clone()));
            }
            if builder_contains_binding_source(graph) {
                return Err(IvmRuntimeError::MultisinkSinkRequiresPrepare(sink.clone()));
            }
        }
        self.logical_nodes_requested += sinks
            .iter()
            .map(|(_, graph)| count_builder_nodes(graph))
            .sum::<usize>() as u64;
        let mut outputs = BTreeMap::new();
        for (sink, graph) in sinks {
            outputs.insert(sink, self.add_dedup_graph(&graph)?);
        }
        let snapshots = self.hydration_snapshots(&outputs, storage, HydrationMode::Ordinary);
        for node in self.gc_ephemeral_nodes(0) {
            self.remove_node_runtime(node);
        }
        self.prune_unreferenced_arrangements();
        snapshots
    }

    pub fn tick<S>(
        &mut self,
        table_deltas: Vec<TableDelta>,
        storage: &S,
    ) -> Result<TickMetrics, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        self.tick_with_params(table_deltas, Vec::new(), storage)
    }

    fn flush_pending_binding_retractions<S>(&mut self, storage: &S) -> Result<(), IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        if !self.pending_binding_retractions.is_empty() {
            // Unsubscribe may queue routed binding retractions for the next
            // runtime tick. Snapshot hydration also needs a binding snapshot,
            // so it must first bring queued retractions into arranged state;
            // otherwise the snapshot could observe a binding as live while
            // its retraction is already committed to the lifecycle queue.
            self.tick_with_params(Vec::new(), Vec::new(), storage)?;
        }
        Ok(())
    }

    pub(crate) fn tick_staged<S>(
        &mut self,
        table_deltas: Vec<TableDelta>,
        storage: &S,
        staged_writes: &mut Vec<OwnedWriteOperation>,
    ) -> Result<TickMetrics, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let staged_overlay = RefCell::new(StagedWriteState::from(std::mem::take(staged_writes)));
        let overlay = StagedWriteOverlay::new(storage, &staged_overlay);
        let tick = self.tick_with_params(table_deltas, Vec::new(), &overlay);
        overlay.drain_into(staged_writes);
        tick
    }

    fn tick_with_params<S>(
        &mut self,
        table_deltas: Vec<TableDelta>,
        mut binding_deltas: Vec<BindingDelta>,
        storage: &S,
    ) -> Result<TickMetrics, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        if !self.pending_binding_retractions.is_empty() {
            let mut pending = std::mem::take(&mut self.pending_binding_retractions);
            pending.extend(binding_deltas);
            binding_deltas = pending;
        }
        let current_tick = self.advance_tick();
        self.bump_input_frontiers(&table_deltas, &binding_deltas);
        let table_delta_records = table_deltas
            .iter()
            .map(|delta| delta.deltas.len())
            .sum::<usize>();
        self.tick_durable_nodes(&table_deltas, current_tick, storage)?;
        let mut dropped_subscriptions = Vec::new();
        let mut metrics = TickMetrics {
            tick: current_tick,
            table_delta_records,
            ..TickMetrics::default()
        };
        let binding_snapshots = self.binding_snapshot_deltas();
        let mut retained_roots = self
            .node_meta
            .iter()
            .filter(|(node, meta)| {
                !meta.retainers.is_empty()
                    && self
                        .graph
                        .node(**node)
                        .is_some_and(|node| !node.is_durable())
            })
            .map(|(node, _)| *node)
            .collect::<Vec<_>>();
        retained_roots.sort_unstable();
        let mut evaluator = TickEvaluator {
            schema: &self.schema,
            graph: &self.graph,
            variant_projections: &self.variant_projections,
            table_deltas: &table_deltas,
            binding_deltas: &binding_deltas,
            binding_snapshots: &binding_snapshots,
            current_tick,
            operator_states: &mut self.operator_states,
            arrangement_states: &mut self.arrangement_states,
            eval_memo: &mut self.eval_memo,
            eval_memo_bytes: &mut self.eval_memo_bytes,
            table_frontiers: &self.table_frontiers,
            binding_frontiers: &self.binding_frontiers,
            memo_use_clock: &mut self.memo_use_clock,
            node_meta: &mut self.node_meta,
            storage: Some(storage),
            context: EvalContext::root(),
            metrics: &mut metrics,
            terminal_deltas: HashMap::default(),
            root_ordering_windows: HashMap::default(),
        };

        for (subscription_id, subscription) in &self.multisink_subscriptions {
            let mut sinks = BTreeMap::new();
            let mut terminal_sinks = BTreeMap::new();
            for (sink, output) in &subscription.outputs {
                let records = evaluator.update_node(output.node)?;
                if !records.deltas.is_empty()
                    && !records.descriptor.registry_compatible_with(&output.output)
                {
                    return Err(IvmRuntimeError::GraphOutputMismatch);
                }
                let structured = evaluator.output_is_structured_collect_by(output.node)?;
                let public_root = evaluator.output_has_public_root(output.node)?;
                let terminal_owned = output.root_ordering_node.is_some() || structured;
                let records = records.as_ref().clone();
                if terminal_owned {
                    let terminal = if structured {
                        if let Some(terminal) =
                            evaluator.take_terminal_deltas_for_output(output.node)?
                        {
                            Some(terminal)
                        } else if !public_root && !records.is_empty() {
                            Some(terminal_deltas_from_record_deltas(&records)?)
                        } else {
                            None
                        }
                    } else if !records.is_empty() {
                        // The ordering node can be above a wider source row.
                        // It contributes only root positions; terminal payloads
                        // must be encoded from the public sink descriptor.
                        Some(terminal_deltas_from_record_deltas(&records)?)
                    } else {
                        None
                    };
                    if let Some(mut terminal) = terminal {
                        if let Some(root_ordering_node) = output.root_ordering_node {
                            evaluator.apply_root_ordering(root_ordering_node, &mut terminal)?;
                        }
                        if !terminal.is_empty() {
                            terminal_sinks.insert(sink.clone(), terminal);
                        }
                    }
                }
                if !records.is_empty() {
                    // Keep the rendered terminal record available for legacy
                    // single-sink consumers and hydration. Structured carriers
                    // select `terminal_sinks` for incremental delivery.
                    sinks.insert(sink.clone(), records);
                }
            }
            let records = MultisinkDeltas {
                sinks,
                terminal_sinks,
            };
            if !records.is_empty() {
                evaluator.metrics.notifications_sent += 1;
                evaluator.metrics.notification_records += multisink_deltas_record_count(&records);
                evaluator.metrics.notification_encoded_bytes +=
                    multisink_deltas_encoded_bytes(&records);
            }
            let queued = QueuedMultisinkDeltas::new(records);
            if !queued.deltas.is_empty() && subscription.sender.send(queued).is_err() {
                dropped_subscriptions.push(*subscription_id);
            }
        }
        // Retained roots are background maintenance. Active subscriptions must
        // see the tick's deltas before retained-only roots can advance shared
        // recursive/operator state.
        for node in retained_roots {
            evaluator.update_node(node)?;
        }
        drop(evaluator);
        self.operator_states
            .retain(|key, _| key.scope == ScopeId::root());

        for subscription_id in dropped_subscriptions {
            self.unsubscribe(subscription_id);
        }
        debug_assert!(self.retained_recursive_nodes_are_current(current_tick));
        self.evict_eval_memo();
        metrics.runtime_stats = if self.collect_tick_runtime_stats {
            self.stats()
        } else {
            self.cheap_stats()
        };
        Ok(metrics)
    }

    fn bump_input_frontiers(
        &mut self,
        table_deltas: &[TableDelta],
        binding_deltas: &[BindingDelta],
    ) {
        let mut changed_tables = Vec::new();
        for delta in table_deltas.iter().filter(|delta| !delta.deltas.is_empty()) {
            *self.table_frontiers.entry(delta.table.clone()).or_default() += 1;
            changed_tables.push(delta.table.as_str());
        }
        let mut changed_bindings = Vec::new();
        for delta in binding_deltas
            .iter()
            .filter(|delta| !delta.deltas.is_empty())
        {
            *self
                .binding_frontiers
                .entry(delta.shape.clone())
                .or_default() += 1;
            changed_bindings.push(delta.shape.as_str());
        }
        if changed_tables.is_empty() && changed_bindings.is_empty() {
            return;
        }
        for meta in self.node_meta.values_mut() {
            let Some(signature) = meta.input_signature.as_ref() else {
                continue;
            };
            let table_changed = changed_tables.iter().any(|changed| {
                signature
                    .tables
                    .iter()
                    .any(|table| table.as_str() == *changed)
            });
            let binding_changed = changed_bindings.iter().any(|changed| {
                signature
                    .bindings
                    .iter()
                    .any(|binding| binding.as_str() == *changed)
            });
            if table_changed || binding_changed {
                meta.input_generation = meta.input_generation.wrapping_add(1);
            }
        }
    }

    fn evict_eval_memo(&mut self) {
        if self.eval_memo.keys().any(|key| key.tick_epoch.is_some()) {
            let mut retained_bytes = 0usize;
            self.eval_memo.retain(|key, entry| {
                let keep = key.tick_epoch.is_none();
                if keep {
                    retained_bytes = retained_bytes.saturating_add(entry.payload_bytes);
                }
                keep
            });
            self.eval_memo_bytes = retained_bytes;
        }
        if self.eval_memo.len() <= EVAL_MEMO_MAX_ENTRIES
            && self.eval_memo_bytes <= EVAL_MEMO_MAX_BYTES
        {
            return;
        }
        let mut entries = self
            .eval_memo
            .iter()
            .map(|(key, entry)| (key.clone(), entry.last_used))
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(_, last_used)| *last_used);
        for (key, _) in entries {
            if self.eval_memo.len() <= EVAL_MEMO_MAX_ENTRIES
                && self.eval_memo_bytes <= EVAL_MEMO_MAX_BYTES
            {
                break;
            }
            if let Some(entry) = self.eval_memo.remove(&key) {
                self.eval_memo_bytes = self.eval_memo_bytes.saturating_sub(entry.payload_bytes);
            }
        }
    }

    #[cfg(test)]
    fn recompute_eval_memo_bytes(&mut self) {
        self.eval_memo_bytes = self
            .eval_memo
            .values()
            .map(|entry| entry.payload_bytes)
            .sum();
    }

    #[cfg(test)]
    fn evict_eval_memo_for_tests(&mut self, max_entries: usize, max_bytes: usize) {
        self.eval_memo.retain(|key, _| key.tick_epoch.is_none());
        self.recompute_eval_memo_bytes();
        let mut entries = self
            .eval_memo
            .iter()
            .map(|(key, entry)| (key.clone(), entry.last_used))
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(_, last_used)| *last_used);
        for (key, _) in entries {
            if self.eval_memo.len() <= max_entries && self.eval_memo_bytes <= max_bytes {
                break;
            }
            if let Some(entry) = self.eval_memo.remove(&key) {
                self.eval_memo_bytes = self.eval_memo_bytes.saturating_sub(entry.payload_bytes);
            }
        }
    }

    fn queued_multisink_deltas(&self, deltas: MultisinkDeltas) -> QueuedMultisinkDeltas {
        QueuedMultisinkDeltas::new(deltas)
    }

    fn hydration_snapshot<S>(
        &mut self,
        output_node: NodeId,
        storage: &S,
        mode: HydrationMode,
    ) -> Result<RecordDeltas, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let table_deltas = snapshot_table_deltas(&self.schema, &self.graph, storage, output_node)?;
        let binding_snapshots = self.binding_snapshot_deltas();
        let context = match mode {
            HydrationMode::Ordinary => EvalContext::root_snapshot(),
            // Subscription hydration must rebuild arrangements for aggregate
            // outputs, while ordinary snapshot hydration intentionally only
            // probes them. Keep that distinction at this policy boundary so
            // both paths share the same frontier and memo lifecycle.
            HydrationMode::Subscription if self.output_depends_on_aggregate(output_node)? => {
                EvalContext::root_subscription_snapshot()
            }
            HydrationMode::Subscription => EvalContext::root_snapshot(),
        };
        let mut metrics = TickMetrics::default();
        let mut evaluator = TickEvaluator {
            schema: &self.schema,
            graph: &self.graph,
            variant_projections: &self.variant_projections,
            table_deltas: &table_deltas,
            binding_deltas: &[],
            binding_snapshots: &binding_snapshots,
            current_tick: self.current_tick,
            operator_states: &mut self.operator_states,
            arrangement_states: &mut self.arrangement_states,
            // Snapshot hydration is evaluated at the runtime's current
            // logical frontier. If a canonical fragment has already been
            // hydrated at this frontier, reusing its memoized output is an
            // attach/probe operation, not an accumulation over stale state:
            // any table or binding change that could invalidate it advances the
            // input frontier counters stored with each memo entry.
            eval_memo: &mut self.eval_memo,
            eval_memo_bytes: &mut self.eval_memo_bytes,
            table_frontiers: &self.table_frontiers,
            binding_frontiers: &self.binding_frontiers,
            memo_use_clock: &mut self.memo_use_clock,
            node_meta: &mut self.node_meta,
            storage: Some(storage),
            context,
            metrics: &mut metrics,
            terminal_deltas: HashMap::default(),
            root_ordering_windows: HashMap::default(),
        };
        let records = evaluator
            .update_node(output_node)
            .map(|records| records.as_ref().clone());
        self.record_hydration_memo_metrics(&metrics);
        self.evict_eval_memo();
        records
    }

    fn hydration_snapshots<S>(
        &mut self,
        outputs: &BTreeMap<String, CompiledNode>,
        storage: &S,
        mode: HydrationMode,
    ) -> Result<MultisinkDeltas, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let mut sinks = BTreeMap::new();
        for (sink, output) in outputs {
            let ordering = output
                .root_ordering_node
                .map(|node| self.hydration_snapshot(node, storage, mode))
                .transpose()?;
            let mut records = self.hydration_snapshot(output.node, storage, mode)?;
            if !records.descriptor.registry_compatible_with(&output.output) {
                return Err(IvmRuntimeError::GraphOutputMismatch);
            }
            if let Some(ordering) = &ordering {
                order_terminal_snapshot(&mut records, ordering)?;
            }
            sinks.insert(sink.clone(), records);
        }
        Ok(MultisinkDeltas {
            sinks,
            terminal_sinks: BTreeMap::new(),
        })
    }

    fn hydration_snapshots_for_subscription<S>(
        &mut self,
        outputs: &BTreeMap<String, CompiledNode>,
        storage: &S,
    ) -> Result<MultisinkDeltas, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        self.hydration_snapshots(outputs, storage, HydrationMode::Subscription)
    }

    fn output_depends_on_aggregate(&self, output_node: NodeId) -> Result<bool, IvmRuntimeError> {
        let mut ancestors = HashSet::new();
        self.graph.mark_ancestors(output_node, &mut ancestors);
        for ancestor in ancestors {
            let graph_node = self
                .graph
                .node(ancestor)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(ancestor))?;
            if matches!(graph_node.descriptor.operator, OpType::Aggregate(_)) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn tick_durable_nodes<S>(
        &mut self,
        table_deltas: &[TableDelta],
        current_tick: u64,
        storage: &S,
    ) -> Result<(), IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let durable_nodes = self
            .retained_node_ids()
            .into_iter()
            .filter(|node| self.graph.node(*node).is_some_and(|node| node.is_durable()))
            .collect::<Vec<_>>();
        let binding_snapshots = self.binding_snapshot_deltas();
        let mut metrics = TickMetrics::default();
        let mut evaluator = TickEvaluator {
            schema: &self.schema,
            graph: &self.graph,
            variant_projections: &self.variant_projections,
            table_deltas,
            binding_deltas: &[],
            binding_snapshots: &binding_snapshots,
            current_tick,
            operator_states: &mut self.operator_states,
            arrangement_states: &mut self.arrangement_states,
            eval_memo: &mut self.eval_memo,
            eval_memo_bytes: &mut self.eval_memo_bytes,
            table_frontiers: &self.table_frontiers,
            binding_frontiers: &self.binding_frontiers,
            memo_use_clock: &mut self.memo_use_clock,
            node_meta: &mut self.node_meta,
            storage: Some(storage),
            context: EvalContext::root(),
            metrics: &mut metrics,
            terminal_deltas: HashMap::default(),
            root_ordering_windows: HashMap::default(),
        };

        for node in durable_nodes {
            evaluator.update_node(node)?;
        }

        Ok(())
    }

    pub fn subscribe_one_sink(
        &mut self,
        graph: GraphBuilder,
        storage: &impl OrderedKvStorage,
    ) -> Result<Subscription, IvmRuntimeError> {
        if builder_contains_binding_source(&graph) {
            return Err(IvmRuntimeError::BindingSourceRequiresPrepare);
        }
        if self.auto_direct_family_enabled
            && let Some(plan) = self.plan_auto_direct_family(&graph)?
        {
            let shape_id = if let Some(shape_id) = self.auto_direct_families.get(&plan.key).copied()
            {
                shape_id
            } else {
                let shape = self.prepare_one_sink(
                    plan.graph.clone(),
                    plan.shape.clone(),
                    plan.binding_descriptor,
                    [plan.binding_field.clone()],
                    storage,
                )?;
                if let Some(state) = self.prepared_shapes.get_mut(&shape.id()) {
                    state.auto_family_key = Some(plan.key.clone());
                    if let Some(terminal) = state.terminals.get_mut(DEFAULT_SINK) {
                        terminal.terminal.public_fields = plan.public_fields.clone();
                    }
                }
                self.auto_direct_families
                    .insert(plan.key.clone(), shape.id());
                shape.id()
            };
            return self.bind_shape_one_sink(shape_id, &[plan.binding_value], storage);
        }
        let multisink = self.subscribe([(DEFAULT_SINK, graph)], storage)?;
        self.single_sink_subscription(multisink, DEFAULT_SINK)
    }

    pub fn subscribe<I, K, S>(
        &mut self,
        sinks: I,
        storage: &S,
    ) -> Result<MultisinkSubscription, IvmRuntimeError>
    where
        I: IntoIterator<Item = (K, GraphBuilder)>,
        K: Into<String>,
        S: OrderedKvStorage,
    {
        self.flush_pending_binding_retractions(storage)?;
        let sinks = sinks
            .into_iter()
            .map(|(sink, graph)| (sink.into(), graph))
            .collect::<Vec<_>>();
        if sinks.is_empty() {
            return Err(IvmRuntimeError::EmptyMultisinkSubscription);
        }
        let mut sink_names = HashSet::new();
        for (sink, _) in &sinks {
            if !sink_names.insert(sink.clone()) {
                return Err(IvmRuntimeError::DuplicateMultisinkSink(sink.clone()));
            }
        }
        if let Some((sink, _)) = sinks
            .iter()
            .find(|(_, graph)| builder_contains_binding_source(graph))
        {
            return Err(IvmRuntimeError::MultisinkSinkRequiresPrepare(sink.clone()));
        }
        self.logical_nodes_requested += sinks
            .iter()
            .map(|(_, graph)| count_builder_nodes(graph))
            .sum::<usize>() as u64;
        let mut outputs = BTreeMap::new();
        for (sink, graph) in sinks {
            let compiled = self.add_dedup_graph(&graph)?;
            outputs.insert(sink, compiled);
        }
        let subscription_id = self.next_subscription_id();
        let (sender, receiver) = mpsc::channel();
        for output in outputs.values() {
            self.retain_as_subscription(subscription_id, output.node);
        }
        self.multisink_subscriptions.insert(
            subscription_id,
            MultisinkSubscriptionState {
                sender,
                outputs: outputs.clone(),
                target: MultisinkSubscriptionTarget::Direct,
            },
        );
        let initial = match self.hydration_snapshots_for_subscription(&outputs, storage) {
            Ok(initial) => initial,
            Err(error) => {
                self.unsubscribe(subscription_id);
                return Err(error);
            }
        };
        let queued = self.queued_multisink_deltas(initial);
        let sent = self
            .multisink_subscriptions
            .get(&subscription_id)
            .is_some_and(|subscription| subscription.sender.send(queued).is_ok());
        if !sent {
            self.unsubscribe(subscription_id);
        }
        Ok(MultisinkSubscription {
            id: subscription_id,
            receiver,
        })
    }

    pub fn prepare<I, S>(
        &mut self,
        terminals: I,
        binding_source_shape: impl Into<String>,
        binding_descriptor: RecordDescriptor,
        storage: &S,
    ) -> Result<PreparedShape, IvmRuntimeError>
    where
        I: IntoIterator<Item = RoutedMultisinkTerminal>,
        S: OrderedKvStorage,
    {
        self.flush_pending_binding_retractions(storage)?;
        let terminals = terminals.into_iter().collect::<Vec<_>>();
        if terminals.is_empty() {
            return Err(IvmRuntimeError::EmptyMultisinkSubscription);
        }
        let mut sink_names = HashSet::new();
        for terminal in &terminals {
            if !sink_names.insert(terminal.sink.clone()) {
                return Err(IvmRuntimeError::DuplicateMultisinkSink(
                    terminal.sink.clone(),
                ));
            }
            if terminal.route_fields.len() > binding_descriptor.fields().len() {
                return Err(IvmRuntimeError::RoutedMultisinkRouteArityMismatch {
                    sink: terminal.sink.clone(),
                    expected: binding_descriptor.fields().len(),
                    actual: terminal.route_fields.len(),
                });
            }
            if terminal.route_value_indices.len() != terminal.route_fields.len() {
                return Err(IvmRuntimeError::RoutedMultisinkRouteArityMismatch {
                    sink: terminal.sink.clone(),
                    expected: terminal.route_fields.len(),
                    actual: terminal.route_value_indices.len(),
                });
            }
            if let Some(index) = terminal
                .route_value_indices
                .iter()
                .find(|index| **index >= binding_descriptor.fields().len())
            {
                return Err(IvmRuntimeError::GraphFieldIndexOutOfBounds(*index));
            }
            let output = self.infer_builder_output(&terminal.graph)?;
            for field in terminal.route_fields.iter().chain(&terminal.public_fields) {
                if output.field_index(field).is_none() {
                    return Err(IvmRuntimeError::GraphFieldNotFound(field.clone()));
                }
            }
        }
        self.logical_nodes_requested += terminals
            .iter()
            .map(|terminal| count_builder_nodes(&terminal.graph))
            .sum::<usize>() as u64;
        let shape = binding_source_shape.into();
        let shape_id = self.next_shape_id();
        match self.binding_sources.entry(shape.clone()) {
            std::collections::hash_map::Entry::Occupied(existing)
                if existing.get().descriptor != binding_descriptor =>
            {
                return Err(IvmRuntimeError::BindingSourceDescriptorMismatch(shape));
            }
            std::collections::hash_map::Entry::Occupied(_) => {}
            std::collections::hash_map::Entry::Vacant(vacant) => {
                vacant.insert(BindingSourceState {
                    descriptor: binding_descriptor,
                    refcounts: HashMap::default(),
                });
            }
        }
        let mut terminal_states = BTreeMap::new();
        for terminal in terminals {
            let output = self.add_dedup_graph(&terminal.graph)?;
            self.add_retainer(
                output.node,
                Retainer::PreparedShape(shape_id.retainer_key()),
            );
            terminal_states.insert(
                terminal.sink.clone(),
                RoutedMultisinkTerminalState { terminal, output },
            );
        }
        self.prepared_shapes.insert(
            shape_id,
            RoutedMultisinkShapeState {
                shape,
                binding_descriptor,
                terminals: terminal_states,
                auto_family_key: None,
            },
        );
        Ok(PreparedShape { id: shape_id })
    }

    pub fn bind_shape<S>(
        &mut self,
        shape_id: PreparedShapeId,
        binding_values: &[Value],
        storage: &S,
    ) -> Result<MultisinkSubscription, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        self.bind_shape_with_public_fields(shape_id, binding_values, BTreeMap::new(), storage)
    }

    fn bind_shape_with_public_fields<S>(
        &mut self,
        shape_id: PreparedShapeId,
        binding_values: &[Value],
        public_fields: BTreeMap<String, Vec<String>>,
        storage: &S,
    ) -> Result<MultisinkSubscription, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        self.flush_pending_binding_retractions(storage)?;
        let shape = self
            .prepared_shapes
            .get(&shape_id)
            .ok_or(IvmRuntimeError::PreparedShapeNotFound(shape_id))?
            .clone();
        let binding_record = shape.binding_descriptor.create(binding_values)?;
        let binding_key = BindingKey(binding_record);
        let mut outputs = BTreeMap::new();
        self.logical_nodes_requested += shape
            .terminals
            .values()
            .map(|terminal| count_builder_nodes(&terminal.terminal.graph) + 2)
            .sum::<usize>() as u64;
        for (sink, terminal) in &shape.terminals {
            let mut terminal = terminal.terminal.clone();
            if let Some(fields) = public_fields.get(sink) {
                terminal.public_fields = fields.clone();
            }
            let graph = bound_routed_multisink_graph(&terminal, binding_values);
            let output = self.add_dedup_graph(&graph)?;
            outputs.insert(sink.clone(), output);
        }
        let subscription_id = self.next_subscription_id();
        for output in outputs.values() {
            self.retain_as_subscription(subscription_id, output.node);
        }
        let binding_delta = match self.add_binding_ref(shape_id, binding_key.clone()) {
            Ok(delta) => delta,
            Err(error) => {
                self.remove_multisink_retainers(subscription_id, &outputs);
                return Err(error);
            }
        };
        if !binding_delta.deltas.is_empty()
            && let Err(error) = self.tick_with_params(Vec::new(), vec![binding_delta], storage)
        {
            self.remove_multisink_retainers(subscription_id, &outputs);
            let _ = self.remove_binding_ref(shape_id, &binding_key);
            return Err(error);
        }
        let (sender, receiver) = mpsc::channel();
        self.multisink_subscriptions.insert(
            subscription_id,
            MultisinkSubscriptionState {
                sender,
                outputs: outputs.clone(),
                target: MultisinkSubscriptionTarget::RoutedShape {
                    shape_id,
                    binding_key: binding_key.clone(),
                },
            },
        );
        let initial = match self.hydration_snapshots_for_subscription(&outputs, storage) {
            Ok(initial) => initial,
            Err(error) => {
                self.unsubscribe(subscription_id);
                return Err(error);
            }
        };
        let queued = self.queued_multisink_deltas(initial);
        let sent = self
            .multisink_subscriptions
            .get(&subscription_id)
            .is_some_and(|subscription| subscription.sender.send(queued).is_ok());
        if !sent {
            self.unsubscribe(subscription_id);
        }
        Ok(MultisinkSubscription {
            id: subscription_id,
            receiver,
        })
    }

    pub fn prepare_one_sink(
        &mut self,
        graph: GraphBuilder,
        binding_source_shape: impl Into<String>,
        binding_descriptor: RecordDescriptor,
        output_key_fields: impl IntoIterator<Item = impl Into<String>>,
        storage: &impl OrderedKvStorage,
    ) -> Result<PreparedShape, IvmRuntimeError> {
        // One-sink sugar: the ordinary prepared-shape API is represented as a
        // routed multisink shape with a single default terminal.
        let output = self.infer_builder_output(&graph)?;
        let route_fields = output_key_fields
            .into_iter()
            .map(|field| {
                let field = field.into();
                output
                    .field_index(&field)
                    .ok_or_else(|| IvmRuntimeError::ShapeKeyFieldNotFound(field.clone()))?;
                Ok::<_, IvmRuntimeError>(field)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let public_fields = descriptor_field_names(&output)?;
        self.prepare(
            [RoutedMultisinkTerminal::new(
                DEFAULT_SINK,
                graph,
                route_fields,
                public_fields,
            )],
            binding_source_shape,
            binding_descriptor,
            storage,
        )
    }

    pub fn prepare_one_sink_with_routing(
        &mut self,
        output_graph: GraphBuilder,
        routing_graph: GraphBuilder,
        binding_source_shape: impl Into<String>,
        binding_descriptor: RecordDescriptor,
        routing_key_fields: impl IntoIterator<Item = impl Into<String>>,
        storage: &impl OrderedKvStorage,
    ) -> Result<PreparedShape, IvmRuntimeError> {
        // One-sink sugar for callers that want to describe a clean public
        // output separately from the route-carrying terminal graph.
        let output = self.infer_builder_output(&output_graph)?;
        let routing_output = self.infer_builder_output(&routing_graph)?;
        validate_public_output_fields(&routing_output, &output)?;
        let route_fields = routing_key_fields
            .into_iter()
            .map(|field| {
                let field = field.into();
                routing_output
                    .field_index(&field)
                    .ok_or_else(|| IvmRuntimeError::ShapeKeyFieldNotFound(field.clone()))?;
                Ok::<_, IvmRuntimeError>(field)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let public_fields = descriptor_field_names(&output)?;
        self.prepare(
            [RoutedMultisinkTerminal::new(
                DEFAULT_SINK,
                routing_graph,
                route_fields,
                public_fields,
            )],
            binding_source_shape,
            binding_descriptor,
            storage,
        )
    }

    pub fn bind_shape_one_sink<S>(
        &mut self,
        shape_id: PreparedShapeId,
        binding_values: &[Value],
        storage: &S,
    ) -> Result<Subscription, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let multisink = self.bind_shape(shape_id, binding_values, storage)?;
        self.single_sink_subscription(multisink, DEFAULT_SINK)
    }

    pub(crate) fn bind_shape_one_sink_with_output<S>(
        &mut self,
        shape_id: PreparedShapeId,
        binding_values: &[Value],
        public_output: RecordDescriptor,
        storage: &S,
    ) -> Result<Subscription, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        validate_public_output_for_shape(
            self.prepared_shapes
                .get(&shape_id)
                .ok_or(IvmRuntimeError::PreparedShapeNotFound(shape_id))?,
            DEFAULT_SINK,
            &public_output,
        )?;
        let public_fields = descriptor_field_names(&public_output)?;
        let multisink = self.bind_shape_with_public_fields(
            shape_id,
            binding_values,
            [(DEFAULT_SINK.to_owned(), public_fields)].into(),
            storage,
        )?;
        self.single_sink_subscription(multisink, DEFAULT_SINK)
    }

    pub fn unsubscribe(&mut self, subscription_id: SubscriptionId) -> bool {
        if let Some(subscription) = self.multisink_subscriptions.remove(&subscription_id) {
            let removed = self.remove_multisink_retainers(subscription_id, &subscription.outputs);
            if let MultisinkSubscriptionTarget::RoutedShape {
                shape_id,
                binding_key,
            } = subscription.target
                && let Some(param_delta) = self.remove_binding_ref(shape_id, &binding_key)
                && !param_delta.deltas.is_empty()
            {
                self.pending_binding_retractions.push(param_delta);
                self.remove_unreferenced_auto_family(shape_id);
            }
            return removed;
        }

        false
    }

    pub fn unsubscribe_with_storage<S>(
        &mut self,
        subscription_id: SubscriptionId,
        storage: &S,
    ) -> Result<bool, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        if let Some(subscription) = self.multisink_subscriptions.remove(&subscription_id) {
            let removed = self.remove_multisink_retainers(subscription_id, &subscription.outputs);
            if let MultisinkSubscriptionTarget::RoutedShape {
                shape_id,
                binding_key,
            } = subscription.target
                && let Some(param_delta) = self.remove_binding_ref(shape_id, &binding_key)
                && !param_delta.deltas.is_empty()
            {
                self.tick_with_params(Vec::new(), vec![param_delta], storage)?;
                self.remove_unreferenced_auto_family(shape_id);
            }
            return Ok(removed);
        }

        Ok(false)
    }

    pub fn add_dedup_schema_indices(&mut self) -> Result<(), IvmRuntimeError> {
        for table in self.schema.tables.clone() {
            for index in &table.indices {
                self.add_dedup_schema_index(&table, index)?;
            }
        }
        Ok(())
    }

    pub fn subscription_output_node(&self, subscription_id: SubscriptionId) -> Option<NodeId> {
        let subscription = self.multisink_subscriptions.get(&subscription_id)?;
        if subscription.outputs.len() != 1 {
            return None;
        }
        subscription
            .outputs
            .values()
            .next()
            .map(|output| output.node)
    }

    pub fn subscription_output(
        &self,
        subscription_id: SubscriptionId,
    ) -> Option<&RecordDescriptor> {
        let subscription = self.multisink_subscriptions.get(&subscription_id)?;
        if subscription.outputs.len() != 1 {
            return None;
        }
        subscription
            .outputs
            .values()
            .next()
            .map(|output| &output.output)
    }

    fn single_sink_subscription(
        &self,
        inner: MultisinkSubscription,
        sink: &str,
    ) -> Result<Subscription, IvmRuntimeError> {
        let output = self
            .subscription_output(inner.id())
            .copied()
            .ok_or(IvmRuntimeError::GraphOutputMismatch)?;
        Ok(Subscription {
            inner,
            sink: sink.to_owned(),
            output,
        })
    }

    fn plan_auto_direct_family(
        &self,
        graph: &GraphBuilder,
    ) -> Result<Option<AutoDirectFamilyPlan>, IvmRuntimeError> {
        if builder_contains_recursive(graph) {
            return Ok(None);
        }
        let original_output = self.infer_builder_output(graph)?;
        let binding_field = auto_direct_binding_field(graph, &original_output, self)?;
        let Some(lifted) = lift_literal_filter(self, graph, &binding_field)? else {
            return Ok(None);
        };
        let shape_seed = "__auto_direct_shape".to_owned();
        let graph = replace_binding_shape(lifted.graph, &shape_seed);
        let shape_output = self.infer_builder_output(&graph)?;
        validate_public_output_fields(&shape_output, &original_output)?;
        let public_fields = descriptor_field_names(&original_output)?;
        let mut hasher = DefaultHasher::new();
        graph.hash(&mut hasher);
        let shape = format!("auto_direct_{:016x}", hasher.finish());
        let graph = replace_binding_shape(graph, &shape);
        if shape_output.field_index(&binding_field).is_none() {
            return Ok(None);
        }
        let binding_descriptor = RecordDescriptor::new([(
            binding_field.clone(),
            lifted
                .value
                .value_type()
                .ok_or(IvmRuntimeError::UnsupportedOperator)?,
        )]);
        let key = AutoDirectFamilyKey {
            graph: graph.clone(),
            binding_descriptor,
            binding_field: binding_field.clone(),
            public_fields: public_fields.clone(),
        };
        Ok(Some(AutoDirectFamilyPlan {
            key,
            graph,
            shape,
            binding_descriptor,
            binding_field,
            binding_value: lifted.value.to_value(),
            public_fields,
        }))
    }

    fn infer_builder_output(
        &self,
        graph: &GraphBuilder,
    ) -> Result<RecordDescriptor, IvmRuntimeError> {
        let mut output_memo = HashMap::default();
        self.infer_builder_output_cached(graph, &mut output_memo)
    }

    fn infer_builder_output_cached(
        &self,
        graph: &GraphBuilder,
        output_memo: &mut HashMap<usize, RecordDescriptor>,
    ) -> Result<RecordDescriptor, IvmRuntimeError> {
        let memo_key = graph as *const GraphBuilder as usize;
        if let Some(output) = output_memo.get(&memo_key) {
            return Ok(*output);
        }
        let output = self.infer_builder_output_uncached(graph, output_memo)?;
        output_memo.insert(memo_key, output);
        Ok(output)
    }

    fn infer_builder_output_uncached(
        &self,
        graph: &GraphBuilder,
        output_memo: &mut HashMap<usize, RecordDescriptor>,
    ) -> Result<RecordDescriptor, IvmRuntimeError> {
        match graph {
            GraphBuilder::Table {
                table,
                variant_projection,
                ..
            } => {
                if let Some(target) = variant_projection {
                    return self
                        .variant_projections
                        .get(&VariantProjectionKey {
                            table: table.clone(),
                            target: VariantProjectionTarget::Named(target.clone()),
                        })
                        .map(|projection| projection.output)
                        .ok_or_else(|| IvmRuntimeError::VariantProjectionNotFound {
                            table: table.clone(),
                            target: target.clone(),
                        });
                }
                let table_schema = self
                    .schema
                    .table(table)
                    .ok_or_else(|| IvmRuntimeError::TableNotFound(table.clone()))?;
                if table_schema.has_variants() {
                    return Err(IvmRuntimeError::VariantProjectionRequired(table.clone()));
                }
                Ok(table_schema.record_schema())
            }
            GraphBuilder::InlineRecords { output, .. } => Ok(*output),
            GraphBuilder::Index { .. } => Ok(index_record_descriptor()),
            GraphBuilder::FrontierSource { output, .. }
            | GraphBuilder::BindingSource { output, .. } => Ok(*output),
            GraphBuilder::Filter { input, .. }
            | GraphBuilder::ArgMaxBy { input, .. }
            | GraphBuilder::ArgMinBy { input, .. }
            | GraphBuilder::TopBy { input, .. } => {
                self.infer_builder_output_cached(input, output_memo)
            }
            GraphBuilder::CollectBy { input, collect, .. } => {
                let input = self.infer_builder_output_cached(input, output_memo)?;
                match collect.mode {
                    CollectByMode::Root => {
                        collect_by_root_descriptor(&input, &collect.parent_fields)
                    }
                    CollectByMode::Collect if collect.slots.is_empty() => collect_by_descriptor(
                        &input,
                        &collect.parent_fields,
                        &collect.child_fields,
                        &collect.collection_field,
                    ),
                    CollectByMode::Collect => {
                        collect_by_tree_descriptor(&input, &collect.parent_fields, &collect.slots)
                    }
                    CollectByMode::Expand if collect.slots.is_empty() => {
                        collect_by_expand_descriptor(&input, &collect.tuple_fields)
                    }
                    CollectByMode::Expand => Err(IvmRuntimeError::InvalidCollectBy(
                        "expand mode does not accept recursive collection slots".into(),
                    )),
                }
            }
            GraphBuilder::Aggregate {
                input,
                group_cols,
                aggregates,
            } => {
                let input = self.infer_builder_output_cached(input, output_memo)?;
                aggregate_descriptor(&input, group_cols, aggregates)
            }
            GraphBuilder::UnwrapNullable { input, field } => {
                let input = self.infer_builder_output_cached(input, output_memo)?;
                let field_idx = resolve_field_ref(&input, field)?;
                unwrap_nullable_descriptor(&input, field_idx)
            }
            GraphBuilder::Unnest {
                input,
                array_field,
                element_field,
            } => {
                let input = self.infer_builder_output_cached(input, output_memo)?;
                let field_idx = resolve_field_ref(&input, array_field)?;
                unnest_descriptor(&input, field_idx, element_field)
            }
            GraphBuilder::VariantProject { input, field, case } => {
                let input = self.infer_builder_output_cached(input, output_memo)?;
                variant_project_descriptor(&input, field, case)
            }
            GraphBuilder::Project { input, fields } => {
                let input = self.infer_builder_output_cached(input, output_memo)?;
                project_descriptor(&input, fields)
            }
            GraphBuilder::Union { inputs } => {
                let mut output: Option<RecordDescriptor> = None;
                for input in inputs {
                    let next = self.infer_builder_output_cached(input, output_memo)?;
                    if let Some(output) = output {
                        if !output.registry_compatible_with(&next) {
                            return Err(IvmRuntimeError::GraphOutputMismatch);
                        }
                    } else {
                        output = Some(next);
                    }
                }
                Ok(output.unwrap_or_default())
            }
            GraphBuilder::Join { left, right, .. } => {
                let left = self.infer_builder_output_cached(left, output_memo)?;
                let right = self.infer_builder_output_cached(right, output_memo)?;
                Ok(join_descriptor(&left, &right))
            }
            GraphBuilder::SemiJoin { left, .. } => {
                self.infer_builder_output_cached(left, output_memo)
            }
            GraphBuilder::AntiJoin { left, .. } => {
                self.infer_builder_output_cached(left, output_memo)
            }
            GraphBuilder::Recursive { seed, step, .. } => {
                let seed = self.infer_builder_output_cached(seed, output_memo)?;
                let step = self.infer_builder_output_cached(step, output_memo)?;
                if !seed.registry_compatible_with(&step) {
                    return Err(IvmRuntimeError::GraphOutputMismatch);
                }
                Ok(seed)
            }
        }
    }

    fn next_subscription_id(&mut self) -> SubscriptionId {
        let id = SubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;
        id
    }

    fn next_shape_id(&mut self) -> PreparedShapeId {
        let id = PreparedShapeId(self.next_shape_id);
        self.next_shape_id += 1;
        id
    }

    fn add_binding_ref(
        &mut self,
        shape_id: PreparedShapeId,
        binding: BindingKey,
    ) -> Result<BindingDelta, IvmRuntimeError> {
        let shape = self.binding_source_shape_name(shape_id)?;
        self.add_binding_ref_for_shape(&shape, binding)
    }

    fn add_binding_ref_for_shape(
        &mut self,
        shape: &str,
        binding: BindingKey,
    ) -> Result<BindingDelta, IvmRuntimeError> {
        let source = self
            .binding_sources
            .get_mut(shape)
            .ok_or_else(|| IvmRuntimeError::BindingSourceNotFound(shape.to_owned()))?;
        let count = source.refcounts.entry(binding.clone()).or_default();
        *count += 1;
        Ok(BindingDelta {
            shape: shape.to_owned(),
            descriptor: source.descriptor,
            deltas: if *count == 1 {
                vec![RecordDelta {
                    record: binding.0.into(),
                    weight: 1,
                }]
            } else {
                Vec::new()
            },
        })
    }

    fn remove_binding_ref(
        &mut self,
        shape_id: PreparedShapeId,
        binding: &BindingKey,
    ) -> Option<BindingDelta> {
        let shape = self.binding_source_shape_name(shape_id).ok()?;
        self.remove_binding_ref_for_shape(&shape, binding)
    }

    fn remove_binding_ref_for_shape(
        &mut self,
        shape: &str,
        binding: &BindingKey,
    ) -> Option<BindingDelta> {
        let source = self.binding_sources.get_mut(shape)?;
        let count = source.refcounts.get_mut(binding)?;
        *count -= 1;
        if *count > 0 {
            return Some(BindingDelta {
                shape: shape.to_owned(),
                descriptor: source.descriptor,
                deltas: Vec::new(),
            });
        }
        source.refcounts.remove(binding);
        Some(BindingDelta {
            shape: shape.to_owned(),
            descriptor: source.descriptor,
            deltas: vec![RecordDelta {
                record: binding.0.clone().into(),
                weight: -1,
            }],
        })
    }

    fn binding_source_shape_name(
        &self,
        shape_id: PreparedShapeId,
    ) -> Result<String, IvmRuntimeError> {
        if let Some(shape) = self.prepared_shapes.get(&shape_id) {
            return Ok(shape.shape.clone());
        }
        Err(IvmRuntimeError::PreparedShapeNotFound(shape_id))
    }

    fn binding_snapshot_deltas(&self) -> HashMap<String, RecordDeltas> {
        debug_assert!(
            self.pending_binding_retractions.is_empty(),
            "binding snapshots must not race queued binding retractions"
        );
        self.binding_sources
            .iter()
            .map(|(shape, source)| {
                (
                    shape.clone(),
                    RecordDeltas {
                        descriptor: source.descriptor,
                        deltas: source
                            .refcounts
                            .keys()
                            .map(|binding| RecordDelta {
                                record: binding.0.clone().into(),
                                weight: 1,
                            })
                            .collect(),
                    },
                )
            })
            .collect()
    }

    fn remove_unreferenced_auto_family(&mut self, shape_id: PreparedShapeId) {
        let Some(shape) = self.prepared_shapes.get(&shape_id) else {
            return;
        };
        let Some(key) = shape.auto_family_key.clone() else {
            return;
        };
        if self
            .multisink_subscriptions
            .values()
            .any(|subscription| matches!(subscription.target, MultisinkSubscriptionTarget::RoutedShape { shape_id: active, .. } if active == shape_id))
        {
            return;
        }
        let shape_name = shape.shape.clone();
        let output_nodes = shape
            .terminals
            .values()
            .map(|terminal| terminal.output.node)
            .collect::<Vec<_>>();
        self.prepared_shapes.remove(&shape_id);
        self.binding_sources.remove(&shape_name);
        self.auto_direct_families.remove(&key);
        for output_node in output_nodes {
            self.remove_retainer(
                output_node,
                &Retainer::PreparedShape(shape_id.retainer_key()),
            );
        }
        for node in self.gc_ephemeral_nodes(0) {
            self.remove_node_runtime(node);
        }
        self.prune_unreferenced_arrangements();
    }

    fn advance_tick(&mut self) -> u64 {
        self.current_tick += 1;
        self.current_tick
    }

    pub fn retained_node_ids(&self) -> HashSet<NodeId> {
        let mut retained = HashSet::new();
        let roots = self
            .graph
            .nodes()
            .values()
            .filter(|node| {
                node.is_durable()
                    || self
                        .node_meta
                        .get(&node.id)
                        .is_some_and(|meta| !meta.retainers.is_empty())
            })
            .map(|node| node.id)
            .collect::<Vec<_>>();

        for root in roots {
            self.graph.mark_ancestors(root, &mut retained);
        }

        retained
    }

    pub fn stats(&self) -> RuntimeStats {
        let mut stats = self.cheap_stats();
        for arrangement in self.arrangement_states.values() {
            stats.arrangement_rows += arrangement.value().row_count();
            stats.arrangement_encoded_bytes += arrangement.value().encoded_bytes();
        }
        for state in self.operator_states.values() {
            let OperatorState::Recursive(recursive) = state else {
                continue;
            };
            stats.recursive_state_count += 1;
            stats.recursive_accumulated_rows += recursive.value().accumulated_row_count();
            stats.recursive_accumulated_encoded_bytes +=
                recursive.value().accumulated_encoded_bytes();
        }
        stats
    }

    fn cheap_stats(&self) -> RuntimeStats {
        RuntimeStats {
            graph_nodes: self.graph.nodes().len(),
            active_subscriptions: self.multisink_subscriptions.len(),
            active_prepared_shapes: self.prepared_shapes.len(),
            active_shape_params: self
                .binding_sources
                .values()
                .map(|source| source.refcounts.len())
                .sum(),
            arrangement_count: self.arrangement_states.len(),
            eval_memo_entries: self.eval_memo.len(),
            hydration_memo_entries: self
                .eval_memo
                .keys()
                .filter(|key| key.tick_epoch.is_none())
                .count(),
            eval_memo_bytes: self
                .eval_memo
                .values()
                .map(|entry| entry.payload_bytes)
                .sum(),
            hydration_memo_hits: self.hydration_memo_hits,
            hydration_memo_computes: self.hydration_memo_computes,
            hydration_memo_distinct_computed_nodes: self.hydration_memo_computed_nodes.len(),
            logical_nodes_requested: self.logical_nodes_requested,
            deduped_graph_nodes: self.graph.nodes().len(),
            ..RuntimeStats::default()
        }
    }

    fn record_hydration_memo_metrics(&mut self, metrics: &TickMetrics) {
        self.hydration_memo_hits += metrics.hydration_memo_hits;
        self.hydration_memo_computes += metrics.hydration_memo_computes;
        self.hydration_memo_computed_nodes
            .extend(metrics.hydration_memo_computed_nodes.iter().copied());
    }

    fn add_retainer(&mut self, id: NodeId, retainer: Retainer) -> bool {
        if self.graph.node(id).is_none() {
            return false;
        }
        let meta = self.node_meta.entry(id).or_default();
        meta.last_used_tick = self.current_tick;
        meta.retainers.insert(retainer)
    }

    fn retain_as_subscription(
        &mut self,
        subscription_id: SubscriptionId,
        output_node: NodeId,
    ) -> bool {
        self.add_retainer(
            output_node,
            Retainer::Subscription(subscription_id.retainer_key()),
        )
    }

    fn remove_multisink_retainers(
        &mut self,
        subscription_id: SubscriptionId,
        outputs: &BTreeMap<String, CompiledNode>,
    ) -> bool {
        let mut removed = false;
        for output in outputs.values() {
            removed |= self.remove_retainer(
                output.node,
                &Retainer::Subscription(subscription_id.retainer_key()),
            );
        }
        for node in self.gc_ephemeral_nodes(0) {
            self.remove_node_runtime(node);
        }
        self.prune_unreferenced_arrangements();
        removed
    }

    fn remove_retainer(&mut self, id: NodeId, retainer: &Retainer) -> bool {
        self.node_meta
            .get_mut(&id)
            .map(|meta| meta.retainers.remove(retainer))
            .unwrap_or(false)
    }

    fn gc_ephemeral_nodes(&mut self, ttl_ticks: u64) -> Vec<NodeId> {
        let retained = self.retained_node_ids();
        let remove_before_tick = self.current_tick.saturating_sub(ttl_ticks);
        let removable = self
            .graph
            .nodes()
            .values()
            .filter(|node| {
                !node.is_durable()
                    && !retained.contains(&node.id)
                    && self
                        .node_meta
                        .get(&node.id)
                        .is_none_or(|meta| meta.last_used_tick <= remove_before_tick)
            })
            .map(|node| node.id)
            .collect::<Vec<_>>();

        for id in &removable {
            self.graph.remove_node(*id);
        }

        removable
    }

    fn remove_node_runtime(&mut self, node: NodeId) {
        self.operator_states.retain(|key, _| key.node != node);
        self.arrangement_states.retain(|key, _| key.input != node);
        self.eval_memo.retain(|key, _| key.node != node);
        self.node_meta.remove(&node);
    }

    fn prune_unreferenced_arrangements(&mut self) {
        let mut referenced = HashSet::new();
        for node in self.graph.nodes().values() {
            match &node.descriptor.operator {
                OpType::Join(join) | OpType::SemiJoin(join) | OpType::AntiJoin(join) => {
                    if let [left, right] = node.descriptor.inputs.as_slice() {
                        referenced.insert(ArrangementKey {
                            scope: ScopeId::root(),
                            input: *left,
                            fields: Arc::from(plan_expr_names(&join.left_key)),
                            descriptor: join.left_descriptor,
                            comparison: join.comparison,
                        });
                        referenced.insert(ArrangementKey {
                            scope: ScopeId::root(),
                            input: *right,
                            fields: Arc::from(plan_expr_names(&join.right_key)),
                            descriptor: join.right_descriptor,
                            comparison: join.comparison,
                        });
                    }
                }
                OpType::ArgMaxBy(arg_by) => {
                    if let [input] = node.descriptor.inputs.as_slice() {
                        referenced.insert(ArrangementKey {
                            scope: ScopeId::root(),
                            input: *input,
                            fields: Arc::from(arg_by.group_fields.clone()),
                            descriptor: node.descriptor.output,
                            comparison: ValueComparison::Exact,
                        });
                    }
                }
                OpType::ArgMinBy(arg_by) => {
                    if let [input] = node.descriptor.inputs.as_slice() {
                        referenced.insert(ArrangementKey {
                            scope: ScopeId::root(),
                            input: *input,
                            fields: Arc::from(arg_by.group_fields.clone()),
                            descriptor: node.descriptor.output,
                            comparison: ValueComparison::Exact,
                        });
                    }
                }
                OpType::TopBy(top_by) => {
                    if let [input] = node.descriptor.inputs.as_slice() {
                        referenced.insert(ArrangementKey {
                            scope: ScopeId::root(),
                            input: *input,
                            fields: Arc::from(top_by.group_fields.clone()),
                            descriptor: node.descriptor.output,
                            comparison: ValueComparison::Exact,
                        });
                    }
                }
                OpType::CollectBy(collect_by) => {
                    if let [input] = node.descriptor.inputs.as_slice()
                        && let Some(input_node) = self.graph.node(*input)
                    {
                        referenced.insert(ArrangementKey {
                            scope: ScopeId::root(),
                            input: *input,
                            fields: Arc::from(collect_by.group_fields.clone()),
                            descriptor: input_node.descriptor.output,
                            comparison: ValueComparison::Exact,
                        });
                    }
                }
                OpType::Aggregate(aggregate) => {
                    if let [input] = node.descriptor.inputs.as_slice()
                        && let Some(input_node) = self.graph.node(*input)
                    {
                        referenced.insert(ArrangementKey {
                            scope: ScopeId::root(),
                            input: *input,
                            fields: Arc::from(plan_expr_names(&aggregate.group_key)),
                            descriptor: input_node.descriptor.output,
                            comparison: ValueComparison::Exact,
                        });
                    }
                }
                _ => {}
            }
        }
        self.arrangement_states.retain(|key, _| {
            referenced.iter().any(|referenced| {
                referenced.input == key.input
                    && referenced.fields == key.fields
                    && referenced.descriptor == key.descriptor
            })
        });
    }

    fn retained_recursive_nodes_are_current(&self, current_tick: u64) -> bool {
        let retained = self.retained_node_ids();
        self.operator_states.iter().all(|(key, state)| {
            !retained.contains(&key.node)
                || !matches!(state, OperatorState::Recursive(_))
                || matches!(
                    state,
                    OperatorState::Recursive(recursive) if recursive.as_of() == Some(Tick(current_tick))
                )
        })
    }

    fn initialize_node_runtime(&mut self, node: NodeId) {
        self.node_meta.entry(node).or_default();
        let Some(graph_node) = self.graph.node(node) else {
            return;
        };
        let operator = &graph_node.descriptor.operator;
        let operator_state = operator_state_for(operator);
        if !matches!(operator_state, OperatorState::Stateless) {
            self.operator_states
                .entry(OperatorStateKey {
                    scope: ScopeId::root(),
                    node,
                })
                .or_insert(operator_state);
        }
    }

    fn add_dedup_graph(&mut self, graph: &GraphBuilder) -> Result<CompiledNode, IvmRuntimeError> {
        validate_collect_by_terminality(graph)?;
        let mut output_memo = HashMap::default();
        self.add_dedup_graph_cached(graph, &mut output_memo)
    }

    fn add_dedup_graph_cached(
        &mut self,
        graph: &GraphBuilder,
        output_memo: &mut HashMap<usize, RecordDescriptor>,
    ) -> Result<CompiledNode, IvmRuntimeError> {
        let inferred_output = self.infer_builder_output_cached(graph, output_memo)?;
        match graph {
            GraphBuilder::Table { .. }
            | GraphBuilder::InlineRecords { .. }
            | GraphBuilder::Index { .. }
            | GraphBuilder::FrontierSource { .. }
            | GraphBuilder::BindingSource { .. }
            | GraphBuilder::Recursive { .. }
            | GraphBuilder::CollectBy { .. } => {
                self.add_dedup_source_graph(graph, inferred_output, output_memo)
            }
            GraphBuilder::ArgMaxBy { .. }
            | GraphBuilder::ArgMinBy { .. }
            | GraphBuilder::TopBy { .. } => {
                self.add_dedup_ordering_graph(graph, inferred_output, output_memo)
            }
            GraphBuilder::Aggregate { .. }
            | GraphBuilder::Filter { .. }
            | GraphBuilder::Project { .. }
            | GraphBuilder::UnwrapNullable { .. }
            | GraphBuilder::Unnest { .. }
            | GraphBuilder::VariantProject { .. }
            | GraphBuilder::Union { .. } => {
                self.add_dedup_unary_graph(graph, inferred_output, output_memo)
            }
            GraphBuilder::Join { .. }
            | GraphBuilder::SemiJoin { .. }
            | GraphBuilder::AntiJoin { .. } => {
                self.add_dedup_join_graph(graph, inferred_output, output_memo)
            }
        }
    }

    #[inline(never)]
    fn add_dedup_source_graph(
        &mut self,
        graph: &GraphBuilder,
        inferred_output: RecordDescriptor,
        output_memo: &mut HashMap<usize, RecordDescriptor>,
    ) -> Result<CompiledNode, IvmRuntimeError> {
        match graph {
            GraphBuilder::Table {
                table,
                scan,
                variant_projection,
            } => {
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::TableSource(TableSourceOp {
                            table: table.clone(),
                            scan: scan.clone(),
                            variant_projection: variant_projection
                                .clone()
                                .map(VariantProjectionTarget::Named),
                        }),
                        [],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: None,
                })
            }
            GraphBuilder::InlineRecords { output, records } => {
                if !inferred_output.registry_compatible_with(output) {
                    return Err(IvmRuntimeError::GraphOutputMismatch);
                }
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::InlineRecords(InlineRecordsOp {
                            records: records.clone(),
                        }),
                        [],
                        *output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output: *output,
                    node,
                    root_ordering_node: None,
                })
            }
            GraphBuilder::Index { table, index, scan } => {
                let table = self
                    .schema
                    .table(table)
                    .ok_or_else(|| IvmRuntimeError::TableNotFound(table.clone()))?
                    .clone();
                let index = table
                    .indices
                    .iter()
                    .find(|candidate| candidate.name == *index)
                    .ok_or_else(|| IvmRuntimeError::IndexNotFound(index.clone()))?
                    .clone();
                let source = self.index_source_op(&table, &index, scan.clone())?;
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(OpType::IndexSource(source), [], output),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: None,
                })
            }
            GraphBuilder::FrontierSource { binding, output } => {
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::FrontierSource(FrontierSourceOp {
                            binding: binding.clone(),
                        }),
                        [],
                        *output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output: inferred_output,
                    node,
                    root_ordering_node: None,
                })
            }
            GraphBuilder::BindingSource { shape, output } => {
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::BindingSource(BindingSourceOp {
                            shape: shape.clone(),
                        }),
                        [],
                        *output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output: inferred_output,
                    node,
                    root_ordering_node: None,
                })
            }
            GraphBuilder::Recursive {
                seed,
                step,
                frontier,
                max_iters,
            } => {
                if builder_contains_recursive(seed) || builder_contains_recursive(step) {
                    return Err(IvmRuntimeError::UnsupportedNestedRecursion);
                }
                let compiled_seed = self.add_dedup_graph_cached(seed, output_memo)?;
                let compiled_step = self.add_dedup_graph_cached(step, output_memo)?;
                if !compiled_seed
                    .output
                    .registry_compatible_with(&compiled_step.output)
                    || !compiled_seed
                        .output
                        .registry_compatible_with(&inferred_output)
                {
                    return Err(IvmRuntimeError::GraphOutputMismatch);
                }
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::Recursive(RecursiveOp {
                            frontier: frontier.clone(),
                            max_iters: *max_iters,
                            read_tables: recursive_read_tables(
                                &self.graph,
                                compiled_seed.node,
                                compiled_step.node,
                            )?,
                        }),
                        [compiled_seed.node, compiled_step.node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: None,
                })
            }
            GraphBuilder::CollectBy { input, collect } => {
                self.add_collect_by_graph(input, collect, inferred_output, output_memo)
            }
            _ => unreachable!("dispatcher routes only source graph builders here"),
        }
    }

    #[inline(never)]
    fn add_dedup_ordering_graph(
        &mut self,
        graph: &GraphBuilder,
        inferred_output: RecordDescriptor,
        output_memo: &mut HashMap<usize, RecordDescriptor>,
    ) -> Result<CompiledNode, IvmRuntimeError> {
        match graph {
            GraphBuilder::ArgMaxBy {
                input,
                group_cols,
                order_cols,
            } => {
                let compiled_input = self.add_dedup_graph_cached(input, output_memo)?;
                let output = inferred_output;
                let group_field_indices = group_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_field_indices = order_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let primary_key_field_indices =
                    if let GraphBuilder::Table { table, .. } = input.as_ref() {
                        let table_schema = self
                            .schema
                            .table(table)
                            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.clone()))?
                            .clone();
                        let primary_key = table_schema
                            .primary_key
                            .as_ref()
                            .ok_or_else(|| IvmRuntimeError::MissingPrimaryKey(table.clone()))?;
                        let primary_key_field_indices = primary_key
                            .columns
                            .iter()
                            .map(|column| {
                                output.field_index(&column.column).ok_or_else(|| {
                                    IvmRuntimeError::GraphFieldNotFound(column.column.clone())
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        validate_arg_by_primary_key_indices(
                            "arg_max_by",
                            &table_schema,
                            &group_field_indices,
                            &order_field_indices,
                            &primary_key_field_indices,
                        )?;
                        primary_key_field_indices
                    } else {
                        group_field_indices
                            .iter()
                            .chain(&order_field_indices)
                            .copied()
                            .collect()
                    };
                let group_field_names = group_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_field_names = order_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::ArgMaxBy(ArgMaxByOp {
                            group_fields: group_field_names,
                            order_fields: order_field_names,
                            group_field_indices,
                            primary_key_field_indices,
                        }),
                        [compiled_input.node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_input.root_ordering_node,
                })
            }
            GraphBuilder::ArgMinBy {
                input,
                group_cols,
                order_cols,
            } => {
                let compiled_input = self.add_dedup_graph_cached(input, output_memo)?;
                let output = inferred_output;
                let group_field_indices = group_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_field_indices = order_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let primary_key_field_indices =
                    if let GraphBuilder::Table { table, .. } = input.as_ref() {
                        let table_schema = self
                            .schema
                            .table(table)
                            .ok_or_else(|| IvmRuntimeError::TableNotFound(table.clone()))?
                            .clone();
                        let primary_key = table_schema
                            .primary_key
                            .as_ref()
                            .ok_or_else(|| IvmRuntimeError::MissingPrimaryKey(table.clone()))?;
                        let primary_key_field_indices = primary_key
                            .columns
                            .iter()
                            .map(|column| {
                                output.field_index(&column.column).ok_or_else(|| {
                                    IvmRuntimeError::GraphFieldNotFound(column.column.clone())
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        validate_arg_by_primary_key_indices(
                            "arg_min_by",
                            &table_schema,
                            &group_field_indices,
                            &order_field_indices,
                            &primary_key_field_indices,
                        )?;
                        primary_key_field_indices
                    } else {
                        group_field_indices
                            .iter()
                            .chain(&order_field_indices)
                            .copied()
                            .collect()
                    };
                let group_field_names = group_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_field_names = order_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::ArgMinBy(ArgMinByOp {
                            group_fields: group_field_names,
                            order_fields: order_field_names,
                            group_field_indices,
                            primary_key_field_indices,
                        }),
                        [compiled_input.node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_input.root_ordering_node,
                })
            }
            GraphBuilder::TopBy {
                input,
                group_cols,
                order_cols,
                tie_cols,
                offset,
                limit,
            } => {
                let compiled_input = self.add_dedup_graph_cached(input, output_memo)?;
                let output = inferred_output;
                let group_field_indices = group_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_field_indices = order_cols
                    .iter()
                    .map(|order| resolve_field_ref(&output, &order.field))
                    .collect::<Result<Vec<_>, _>>()?;
                let tie_field_indices = tie_cols
                    .iter()
                    .map(|field| resolve_field_ref(&output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let group_field_names = group_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let order_fields = order_cols
                    .iter()
                    .zip(&order_field_indices)
                    .map(|(order, field_idx)| {
                        Ok(TopByOrderField {
                            field: field_name_at(&output, *field_idx)?,
                            direction: order.direction,
                        })
                    })
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let tie_field_names = tie_field_indices
                    .iter()
                    .map(|field| field_name_at(&output, *field))
                    .collect::<Result<Vec<_>, _>>()?;
                let sort_field_indices = order_field_indices
                    .iter()
                    .chain(&tie_field_indices)
                    .copied()
                    .collect::<Vec<_>>();
                let sort_directions = order_cols
                    .iter()
                    .map(|order| order.direction)
                    .chain(std::iter::repeat_n(
                        TopByDirection::Asc,
                        tie_field_indices.len(),
                    ))
                    .collect::<Vec<_>>();
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::TopBy(TopByOp {
                            group_fields: group_field_names,
                            group_field_indices,
                            order_fields,
                            tie_fields: tie_field_names,
                            sort_field_indices,
                            sort_directions,
                            offset: *offset,
                            limit: *limit,
                        }),
                        [compiled_input.node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: Some(node),
                })
            }
            _ => unreachable!("dispatcher routes only ordering graph builders here"),
        }
    }

    #[inline(never)]
    fn add_dedup_unary_graph(
        &mut self,
        graph: &GraphBuilder,
        inferred_output: RecordDescriptor,
        output_memo: &mut HashMap<usize, RecordDescriptor>,
    ) -> Result<CompiledNode, IvmRuntimeError> {
        match graph {
            GraphBuilder::Aggregate {
                input,
                group_cols,
                aggregates,
            } => {
                let compiled_input = self.add_dedup_graph_cached(input, output_memo)?;
                let input_node = compiled_input.node;
                let input_output = compiled_input.output;
                let output = inferred_output;
                let group_field_indices = group_cols
                    .iter()
                    .map(|field| resolve_field_ref(&input_output, field))
                    .collect::<Result<Vec<_>, _>>()?;
                let group_key = group_field_indices
                    .iter()
                    .map(|field| Ok(PlanExpr::Field(field_name_at(&input_output, *field)?)))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let aggregates = aggregates
                    .iter()
                    .map(|aggregate| resolve_aggregate_expr(&input_output, aggregate))
                    .collect::<Result<Vec<_>, _>>()?;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::Aggregate(AggregateOp {
                            group_key,
                            group_field_indices,
                            aggregates,
                        }),
                        [input_node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_input.root_ordering_node,
                })
            }
            GraphBuilder::Filter {
                input,
                predicate,
                comparison,
            } => {
                let compiled_input = self.add_dedup_graph_cached(input, output_memo)?;
                let input_node = compiled_input.node;
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::Filter(FilterOp {
                            predicate: predicate.clone(),
                            comparison: *comparison,
                        }),
                        [input_node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_input.root_ordering_node,
                })
            }
            GraphBuilder::Project { input, fields } => {
                let compiled_input = self.add_dedup_graph_cached(input, output_memo)?;
                let input_node = compiled_input.node;
                let input_output = compiled_input.output;
                let output = inferred_output;
                let mapping = fields
                    .iter()
                    .filter_map(|field| {
                        field.source().map(|source| {
                            resolve_field_ref(&input_output, source).map(|idx| (0, idx))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::MapProject(MapProjectOp {
                            expressions: fields
                                .iter()
                                .map(|field| {
                                    project_field_expr(&input_output, field).map(|expression| {
                                        ProjectionExpr {
                                            expression,
                                            output_name: Some(field.output_name.clone()),
                                        }
                                    })
                                })
                                .collect::<Result<Vec<_>, IvmRuntimeError>>()?,
                            mapping,
                        }),
                        [input_node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_input.root_ordering_node,
                })
            }
            GraphBuilder::UnwrapNullable { input, field } => {
                let compiled_input = self.add_dedup_graph_cached(input, output_memo)?;
                let input_node = compiled_input.node;
                let input_output = compiled_input.output;
                let field_idx = resolve_field_ref(&input_output, field)?;
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::UnwrapNullable(UnwrapNullableOp {
                            field: field_ref_name(&input_output, field)?,
                            field_idx,
                        }),
                        [input_node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_input.root_ordering_node,
                })
            }
            GraphBuilder::Unnest {
                input,
                array_field,
                element_field,
            } => {
                let compiled_input = self.add_dedup_graph_cached(input, output_memo)?;
                let input_node = compiled_input.node;
                let input_output = compiled_input.output;
                let array_field_idx = resolve_field_ref(&input_output, array_field)?;
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::Unnest(UnnestOp {
                            array_field: field_ref_name(&input_output, array_field)?,
                            array_field_idx,
                            element_field: element_field.clone(),
                        }),
                        [input_node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_input.root_ordering_node,
                })
            }
            GraphBuilder::VariantProject { input, field, case } => {
                let compiled_input = self.add_dedup_graph_cached(input, output_memo)?;
                let input_node = compiled_input.node;
                let input_output = compiled_input.output;
                let field_idx = resolve_field_ref(&input_output, field)?;
                let ValueType::Enum(schema) = &input_output.fields()[field_idx].value_type else {
                    return Err(IvmRuntimeError::VariantProjectFieldTypeMismatch {
                        field: field_ref_name(&input_output, field)?,
                    });
                };
                let tag = schema.tag(case)?;
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(
                        OpType::VariantProject(VariantProjectOp {
                            field: field_ref_name(&input_output, field)?,
                            field_idx,
                            tag,
                        }),
                        [input_node],
                        output,
                    ),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_input.root_ordering_node,
                })
            }
            GraphBuilder::Union { inputs } => {
                let mut input_nodes = Vec::with_capacity(inputs.len());
                let mut public_root_ordering = None;
                for input in inputs {
                    let compiled_input = self.add_dedup_graph_cached(input, output_memo)?;
                    if input_nodes.is_empty() {
                        // Structured lowering places the public/root anchor
                        // first; later arms carry association rows and may
                        // contain their own nested TopBy nodes.
                        public_root_ordering = compiled_input.root_ordering_node;
                    }
                    let input_node = compiled_input.node;
                    let input_output = compiled_input.output;
                    if !inferred_output.registry_compatible_with(&input_output) {
                        return Err(IvmRuntimeError::GraphOutputMismatch);
                    }
                    input_nodes.push(input_node);
                }
                let output = inferred_output;
                let node = self.graph.dedup_node(
                    NodeDescriptor::new(OpType::Union, input_nodes, output),
                    NodeDurability::Ephemeral,
                );
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: public_root_ordering,
                })
            }
            _ => unreachable!("dispatcher routes only unary graph builders here"),
        }
    }

    #[inline(never)]
    fn add_dedup_join_graph(
        &mut self,
        graph: &GraphBuilder,
        inferred_output: RecordDescriptor,
        output_memo: &mut HashMap<usize, RecordDescriptor>,
    ) -> Result<CompiledNode, IvmRuntimeError> {
        match graph {
            GraphBuilder::Join {
                left,
                right,
                left_on,
                right_on,
                comparison,
            } => {
                let compiled_left = self.add_dedup_graph_cached(left, output_memo)?;
                let compiled_right = self.add_dedup_graph_cached(right, output_memo)?;
                let output = inferred_output;
                let left_descriptor = compiled_left.output;
                let right_descriptor = compiled_right.output;
                let left_key = left_on
                    .iter()
                    .map(|field| field_ref_name(&left_descriptor, field).map(PlanExpr::field))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let right_key = right_on
                    .iter()
                    .map(|field| field_ref_name(&right_descriptor, field).map(PlanExpr::field))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let node_descriptor = NodeDescriptor::new(
                    OpType::Join(JoinOp {
                        kind: JoinOpKind::Inner,
                        left_key,
                        right_key,
                        left_descriptor,
                        right_descriptor,
                        residual_predicate: None,
                        comparison: *comparison,
                    }),
                    [compiled_left.node, compiled_right.node],
                    output,
                );
                let node = self
                    .graph
                    .dedup_node(node_descriptor, NodeDurability::Ephemeral);
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    // Jazz lowers the public/root relation on the left and
                    // nested relation inputs on the right. Prefer the public
                    // side so a nested TopBy cannot become root ordering.
                    root_ordering_node: compiled_left
                        .root_ordering_node
                        .or(compiled_right.root_ordering_node),
                })
            }
            GraphBuilder::SemiJoin {
                left,
                right,
                left_on,
                right_on,
                comparison,
            } => {
                let compiled_left = self.add_dedup_graph_cached(left, output_memo)?;
                let compiled_right = self.add_dedup_graph_cached(right, output_memo)?;
                let output = inferred_output;
                let left_descriptor = compiled_left.output;
                let right_descriptor = compiled_right.output;
                let left_key = left_on
                    .iter()
                    .map(|field| field_ref_name(&left_descriptor, field).map(PlanExpr::field))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let right_key = right_on
                    .iter()
                    .map(|field| field_ref_name(&right_descriptor, field).map(PlanExpr::field))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let node_descriptor = NodeDescriptor::new(
                    OpType::SemiJoin(JoinOp {
                        kind: JoinOpKind::Inner,
                        left_key,
                        right_key,
                        left_descriptor,
                        right_descriptor,
                        residual_predicate: None,
                        comparison: *comparison,
                    }),
                    [compiled_left.node, compiled_right.node],
                    output,
                );
                let node = self
                    .graph
                    .dedup_node(node_descriptor, NodeDurability::Ephemeral);
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_left
                        .root_ordering_node
                        .or(compiled_right.root_ordering_node),
                })
            }
            GraphBuilder::AntiJoin {
                left,
                right,
                left_on,
                right_on,
                comparison,
            } => {
                let compiled_left = self.add_dedup_graph_cached(left, output_memo)?;
                let compiled_right = self.add_dedup_graph_cached(right, output_memo)?;
                let output = inferred_output;
                let left_descriptor = compiled_left.output;
                let right_descriptor = compiled_right.output;
                let left_key = left_on
                    .iter()
                    .map(|field| field_ref_name(&left_descriptor, field).map(PlanExpr::field))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let right_key = right_on
                    .iter()
                    .map(|field| field_ref_name(&right_descriptor, field).map(PlanExpr::field))
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                let node_descriptor = NodeDescriptor::new(
                    OpType::AntiJoin(JoinOp {
                        kind: JoinOpKind::Inner,
                        left_key,
                        right_key,
                        left_descriptor,
                        right_descriptor,
                        residual_predicate: None,
                        comparison: *comparison,
                    }),
                    [compiled_left.node, compiled_right.node],
                    output,
                );
                let node = self
                    .graph
                    .dedup_node(node_descriptor, NodeDurability::Ephemeral);
                self.initialize_node_runtime(node);
                Ok(CompiledNode {
                    output,
                    node,
                    root_ordering_node: compiled_left
                        .root_ordering_node
                        .or(compiled_right.root_ordering_node),
                })
            }
            _ => unreachable!("dispatcher routes only join graph builders here"),
        }
    }

    fn add_collect_by_graph(
        &mut self,
        input: &GraphBuilder,
        collect: &CollectByBuilder,
        output: RecordDescriptor,
        output_memo: &mut HashMap<usize, RecordDescriptor>,
    ) -> Result<CompiledNode, IvmRuntimeError> {
        let compiled_input = self.add_dedup_graph_cached(input, output_memo)?;
        let input_output = compiled_input.output;
        let group_field_indices = collect
            .group_cols
            .iter()
            .map(|field| resolve_field_ref(&input_output, field))
            .collect::<Result<Vec<_>, _>>()?;
        let parent_fields = collect_by_projections(&input_output, &collect.parent_fields)?;
        if matches!(collect.mode, CollectByMode::Collect | CollectByMode::Root)
            && (!collect.slots.is_empty() || collect.mode == CollectByMode::Root)
        {
            if parent_fields.is_empty() {
                return Err(IvmRuntimeError::InvalidCollectBy(
                    "tree collect requires a parent projection".into(),
                ));
            }
            validate_collect_by_key_types(&input_output, &group_field_indices)?;
            let slots = collect_by_slots(&input_output, &collect.slots, &group_field_indices, 1)?;
            let order_field_indices = if collect.order_cols.is_empty() {
                group_field_indices.clone()
            } else {
                collect
                    .order_cols
                    .iter()
                    .map(|order| resolve_field_ref(&input_output, &order.field))
                    .collect::<Result<Vec<_>, _>>()?
            };
            let tie_field_indices = if collect.tie_cols.is_empty() {
                group_field_indices.clone()
            } else {
                collect
                    .tie_cols
                    .iter()
                    .map(|field| resolve_field_ref(&input_output, field))
                    .collect::<Result<Vec<_>, _>>()?
            };
            validate_collect_by_key_types(&input_output, &order_field_indices)?;
            validate_collect_by_key_types(&input_output, &tie_field_indices)?;
            let group_fields = group_field_indices
                .iter()
                .map(|field| field_name_at(&input_output, *field))
                .collect::<Result<Vec<_>, _>>()?;
            let node = self.graph.dedup_node(
                NodeDescriptor::new(
                    OpType::CollectBy(Box::new(CollectByOp {
                        mode: collect.mode,
                        group_fields,
                        group_field_indices,
                        parent_fields,
                        child_fields: Vec::new(),
                        child_descriptor: RecordDescriptor::new(Vec::<(String, ValueType)>::new()),
                        collection_field: String::new(),
                        collection_field_index: 0,
                        slots,
                        tuple_fields: Vec::new(),
                        occurrence_id_fields: Vec::new(),
                        occurrence_id_field_indices: Vec::new(),
                        order_fields: order_field_indices
                            .iter()
                            .enumerate()
                            .map(|(index, field_idx)| {
                                Ok(TopByOrderField {
                                    field: field_name_at(&input_output, *field_idx)?,
                                    direction: collect
                                        .order_cols
                                        .get(index)
                                        .map_or(TopByDirection::Asc, |order| order.direction),
                                })
                            })
                            .collect::<Result<Vec<_>, IvmRuntimeError>>()?,
                        tie_fields: tie_field_indices
                            .iter()
                            .map(|field| field_name_at(&input_output, *field))
                            .collect::<Result<Vec<_>, _>>()?,
                        sort_field_indices: order_field_indices
                            .iter()
                            .chain(&tie_field_indices)
                            .copied()
                            .collect(),
                        sort_directions: order_field_indices
                            .iter()
                            .enumerate()
                            .map(|(index, _)| {
                                collect
                                    .order_cols
                                    .get(index)
                                    .map_or(TopByDirection::Asc, |order| order.direction)
                            })
                            .chain(std::iter::repeat_n(
                                TopByDirection::Asc,
                                tie_field_indices.len(),
                            ))
                            .collect(),
                        offset: collect.offset,
                        limit: collect.limit,
                    })),
                    [compiled_input.node],
                    output,
                ),
                NodeDurability::Ephemeral,
            );
            self.initialize_node_runtime(node);
            return Ok(CompiledNode {
                output,
                node,
                root_ordering_node: compiled_input.root_ordering_node,
            });
        }
        let child_fields = collect_by_projections(&input_output, &collect.child_fields)?;
        let tuple_fields = collect_by_projections(&input_output, &collect.tuple_fields)?;
        let occurrence_id_field_indices = collect
            .occurrence_id_cols
            .iter()
            .map(|field| resolve_field_ref(&input_output, field))
            .collect::<Result<Vec<_>, _>>()?;
        // Parent values must be determined by the group, rather than by
        // whichever child happens to be selected.
        if collect.mode == CollectByMode::Collect
            && parent_fields
                .iter()
                .any(|field| !group_field_indices.contains(&field.field_idx))
        {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "parent projection fields must be grouping fields".into(),
            ));
        }
        validate_collect_by_key_types(&input_output, &group_field_indices)?;
        validate_collect_by_key_types(&input_output, &occurrence_id_field_indices)?;
        let order_field_indices = collect
            .order_cols
            .iter()
            .map(|order| resolve_field_ref(&input_output, &order.field))
            .collect::<Result<Vec<_>, _>>()?;
        let tie_field_indices = collect
            .tie_cols
            .iter()
            .map(|field| resolve_field_ref(&input_output, field))
            .collect::<Result<Vec<_>, _>>()?;
        if order_field_indices.is_empty() || tie_field_indices.is_empty() {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "order and tie fields must both be complete and non-empty".into(),
            ));
        }
        validate_collect_by_key_types(&input_output, &order_field_indices)?;
        validate_collect_by_key_types(&input_output, &tie_field_indices)?;
        let group_fields = group_field_indices
            .iter()
            .map(|field| field_name_at(&input_output, *field))
            .collect::<Result<Vec<_>, _>>()?;
        let order_fields = collect
            .order_cols
            .iter()
            .zip(&order_field_indices)
            .map(|(order, field_idx)| {
                Ok(TopByOrderField {
                    field: field_name_at(&input_output, *field_idx)?,
                    direction: order.direction,
                })
            })
            .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
        let tie_fields = tie_field_indices
            .iter()
            .map(|field| field_name_at(&input_output, *field))
            .collect::<Result<Vec<_>, _>>()?;
        let sort_field_indices = order_field_indices
            .iter()
            .chain(&tie_field_indices)
            .copied()
            .collect::<Vec<_>>();
        let sort_directions = collect
            .order_cols
            .iter()
            .map(|order| order.direction)
            .chain(std::iter::repeat_n(
                TopByDirection::Asc,
                tie_field_indices.len(),
            ))
            .collect::<Vec<_>>();
        let child_descriptor = RecordDescriptor::new(child_fields.iter().map(|field| {
            let value_type = input_output.fields()[field.field_idx].value_type.clone();
            (field.output_name.clone(), value_type)
        }));
        if collect.mode == CollectByMode::Expand
            && (tuple_fields.is_empty() || occurrence_id_field_indices.is_empty())
        {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "expand mode requires a tuple projection and ordered occurrence-id fields".into(),
            ));
        }
        let collection_field_index = parent_fields.len();
        let node = self.graph.dedup_node(
            NodeDescriptor::new(
                OpType::CollectBy(Box::new(CollectByOp {
                    mode: collect.mode,
                    group_fields,
                    group_field_indices,
                    parent_fields,
                    child_fields,
                    child_descriptor,
                    collection_field: collect.collection_field.clone(),
                    collection_field_index,
                    slots: Vec::new(),
                    tuple_fields,
                    occurrence_id_fields: occurrence_id_field_indices
                        .iter()
                        .map(|field| field_name_at(&input_output, *field))
                        .collect::<Result<Vec<_>, _>>()?,
                    occurrence_id_field_indices,
                    order_fields,
                    tie_fields,
                    sort_field_indices,
                    sort_directions,
                    offset: collect.offset,
                    limit: collect.limit,
                })),
                [compiled_input.node],
                output,
            ),
            NodeDurability::Ephemeral,
        );
        self.initialize_node_runtime(node);
        Ok(CompiledNode {
            output,
            node,
            // CollectBy changes representation, not the identity/order of
            // public roots selected upstream.
            root_ordering_node: compiled_input.root_ordering_node,
        })
    }

    fn add_dedup_schema_index(
        &mut self,
        table: &TableSchema,
        index: &IndexSchema,
    ) -> Result<NodeId, IvmRuntimeError> {
        self.logical_nodes_requested += 3;
        let (table_descriptor, variant_projection) = if table.has_variants() {
            (
                schema_index_input_descriptor(table, index)?,
                Some(VariantProjectionTarget::SchemaIndex(index.name.clone())),
            )
        } else {
            (table.record_schema(), None)
        };
        let input = self.graph.dedup_node(
            NodeDescriptor::new(
                OpType::TableSource(TableSourceOp {
                    table: table.name.clone(),
                    scan: None,
                    variant_projection,
                }),
                [],
                table_descriptor,
            ),
            NodeDurability::Ephemeral,
        );
        self.initialize_node_runtime(input);

        let CompiledNode {
            output: index_descriptor,
            node: index_by,
            ..
        } = self.add_dedup_index_by_from_input(table, index, input, table_descriptor, None)?;

        let storage = DurableStorage {
            column_family: "indices".to_owned(),
            key_prefix: durable_index_key_prefix(&table.name, &index.name),
        };
        let persist = self.graph.dedup_node(
            NodeDescriptor::new(
                OpType::Persist(PersistOp {
                    name: index.name.clone(),
                    storage: storage.clone(),
                    key_fields: vec![0],
                    unique: index.unique,
                }),
                [index_by],
                index_descriptor,
            ),
            NodeDurability::Durable { storage },
        );
        self.add_retainer(
            persist,
            Retainer::DurableSchemaObject(format!("{}.{}", table.name, index.name)),
        );
        self.initialize_node_runtime(persist);

        Ok(persist)
    }

    fn index_source_op(
        &self,
        table: &TableSchema,
        index: &IndexSchema,
        scan: Option<StaticScanSpec>,
    ) -> Result<IndexSourceOp, IvmRuntimeError> {
        let (table_descriptor, variant_projection) = if table.has_variants() {
            (
                schema_index_input_descriptor(table, index)?,
                Some(VariantProjectionTarget::SchemaIndex(index.name.clone())),
            )
        } else {
            (table.record_schema(), None)
        };
        let key_fields = index
            .columns
            .iter()
            .map(|column| {
                table_descriptor
                    .field_index(column)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(column.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let primary_key = table
            .primary_key
            .as_ref()
            .ok_or_else(|| IvmRuntimeError::MissingPrimaryKey(table.name.clone()))?;
        let value_fields = primary_key
            .columns
            .iter()
            .map(|column| {
                table_descriptor
                    .field_index(&column.column)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(column.column.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let index_key_covers_primary_key = primary_key
            .columns
            .iter()
            .all(|primary_key_column| index.columns.contains(&primary_key_column.column));

        Ok(IndexSourceOp {
            table: table.name.clone(),
            index: index.name.clone(),
            input_descriptor: table_descriptor,
            variant_projection,
            key_fields,
            value_fields,
            unique: index.unique || index_key_covers_primary_key,
            append_value_to_key: !index.unique && !index_key_covers_primary_key,
            store_value: index.unique && !index_key_covers_primary_key,
            scan,
        })
    }

    fn add_dedup_index_by_from_input(
        &mut self,
        table: &TableSchema,
        index: &IndexSchema,
        input: NodeId,
        table_descriptor: RecordDescriptor,
        scan: Option<StaticScanSpec>,
    ) -> Result<CompiledNode, IvmRuntimeError> {
        let index_descriptor = index_record_descriptor();
        let key_fields = index
            .columns
            .iter()
            .map(|column| {
                table_descriptor
                    .field_index(column)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(column.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let primary_key = table
            .primary_key
            .as_ref()
            .ok_or_else(|| IvmRuntimeError::MissingPrimaryKey(table.name.clone()))?;
        let primary_key_fields = primary_key
            .columns
            .iter()
            .map(|column| {
                table_descriptor
                    .field_index(&column.column)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(column.column.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let index_key_covers_primary_key = primary_key
            .columns
            .iter()
            .all(|primary_key_column| index.columns.contains(&primary_key_column.column));

        let node = self.graph.dedup_node(
            NodeDescriptor::new(
                OpType::IndexBy(IndexByOp {
                    key_expressions: index
                        .columns
                        .iter()
                        .map(|column| PlanExpr::field(column.clone()))
                        .collect(),
                    value_expressions: primary_key
                        .columns
                        .iter()
                        .map(|column| PlanExpr::field(column.column.clone()))
                        .collect(),
                    explicit_index: Some(index.clone()),
                    key_fields,
                    value_fields: primary_key_fields,
                    unique: index.unique || index_key_covers_primary_key,
                    append_value_to_key: !index.unique && !index_key_covers_primary_key,
                    store_value: index.unique && !index_key_covers_primary_key,
                    scan,
                }),
                [input],
                index_descriptor,
            ),
            NodeDurability::Ephemeral,
        );
        self.initialize_node_runtime(node);
        Ok(CompiledNode {
            output: index_descriptor,
            node,
            root_ordering_node: None,
        })
    }
}

fn record_descriptors_registry_compatible(
    left: &RecordDescriptor,
    right: &RecordDescriptor,
) -> bool {
    left.registry_compatible_with(right)
}

/// Point-in-time runtime counters for benchmark and diagnostics reporting.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeStats {
    pub graph_nodes: usize,
    pub active_subscriptions: usize,
    pub active_prepared_shapes: usize,
    pub active_shape_params: usize,
    pub arrangement_count: usize,
    pub eval_memo_entries: usize,
    pub eval_memo_bytes: usize,
    pub hydration_memo_entries: usize,
    pub hydration_memo_hits: u64,
    pub hydration_memo_computes: u64,
    pub hydration_memo_distinct_computed_nodes: usize,
    pub arrangement_rows: usize,
    pub arrangement_encoded_bytes: usize,
    pub recursive_state_count: usize,
    pub recursive_accumulated_rows: usize,
    pub recursive_accumulated_encoded_bytes: usize,
    pub logical_nodes_requested: u64,
    pub deduped_graph_nodes: usize,
}

impl RuntimeStats {
    pub fn dedupe_ratio(&self) -> f64 {
        if self.logical_nodes_requested == 0 {
            return 1.0;
        }
        self.deduped_graph_nodes as f64 / self.logical_nodes_requested as f64
    }
}

/// Metrics produced by one runtime tick.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TickMetrics {
    pub tick: u64,
    pub table_delta_records: usize,
    pub records_processed: usize,
    pub recursive_recomputes: usize,
    pub hydration_memo_hits: u64,
    pub hydration_memo_computes: u64,
    pub hydration_memo_computed_nodes: HashSet<NodeId>,
    pub notifications_sent: usize,
    pub notification_records: usize,
    pub notification_encoded_bytes: usize,
    pub runtime_stats: RuntimeStats,
}

/// Recursive scope path used to namespace context-dependent state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(super) struct ScopePath(Vec<NodeId>);

impl ScopePath {
    fn root() -> Self {
        Self(Vec::new())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct ScopeId(crate::Intern<ScopePath>);

impl ScopeId {
    fn root() -> Self {
        static ROOT: OnceLock<ScopeId> = OnceLock::new();
        *ROOT.get_or_init(|| Self(crate::Intern::new(ScopePath::root())))
    }

    pub(super) fn child(self, recursive_node: NodeId) -> Self {
        let mut scope = self.0.0.clone();
        scope.push(recursive_node);
        Self(crate::Intern::new(ScopePath(scope)))
    }
}

impl Default for ScopeId {
    fn default() -> Self {
        Self::root()
    }
}

/// Key for operator state that must survive across ticks.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct OperatorStateKey {
    /// Empty for normal query execution; nested recursive scopes append their
    /// recursive node ids here.
    scope: ScopeId,
    node: NodeId,
}

/// Key for a reusable join-side arrangement.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ArrangementKey {
    /// Context-independent inputs use the root scope and can be shared across
    /// unrelated subscriptions.
    scope: ScopeId,
    /// The graph fragment whose records are arranged.
    input: NodeId,
    fields: Arc<[String]>,
    descriptor: RecordDescriptor,
    comparison: ValueComparison,
}

/// Database tick plus recursive sub-tick for scoped arrangement freshness.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) struct SubTick {
    tick: u64,
    sub_tick: u64,
}

/// Logical database tick for state whose contents are only root-tick scoped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) struct Tick(u64);

/// Runtime state together with the logical time its contents reflect.
///
/// Keeping the "as of" time outside operator-specific state makes freshness
/// checks visible at shared-state access sites instead of burying them inside
/// joins, recursion, or future stateful operators.
#[derive(Clone, Debug)]
pub(super) struct AsOf<T, S> {
    value: T,
    as_of: Option<S>,
}

impl<T, S> AsOf<T, S> {
    fn new(value: T) -> Self {
        Self { value, as_of: None }
    }

    pub(super) fn value(&self) -> &T {
        &self.value
    }

    pub(super) fn value_mut(&mut self) -> &mut T {
        &mut self.value
    }

    pub(super) fn as_of(&self) -> Option<S>
    where
        S: Copy,
    {
        self.as_of
    }
}

impl<T, S> AsOf<T, S>
where
    S: Copy + Ord + std::fmt::Debug,
{
    pub(super) fn value_at(&self, expected: S) -> Result<&T, IvmRuntimeError> {
        if self.as_of == Some(expected) {
            return Ok(&self.value);
        }
        Err(IvmRuntimeError::StaleRuntimeState {
            expected: format!("{expected:?}"),
            actual: self.as_of.map(|actual| format!("{actual:?}")),
        })
    }

    pub(super) fn mark_forward_as_of(&mut self, next: S) -> Result<(), IvmRuntimeError> {
        if self.as_of.is_some_and(|current| current > next) {
            return Err(IvmRuntimeError::OutOfOrderRuntimeState {
                current: format!("{:?}", self.as_of.expect("checked above")),
                next: format!("{next:?}"),
            });
        }
        self.as_of = Some(next);
        Ok(())
    }

    pub(super) fn replace_as_of_at_least(&mut self, next: S) {
        if self.as_of.is_none_or(|current| current <= next) {
            self.as_of = Some(next);
        }
    }
}

impl<T: Default, S> Default for AsOf<T, S> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

/// Whether an arrangement should consume a delta or be rebuilt from a snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum ArrangementUpdateMode {
    #[default]
    Accumulate,
    Replace,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum EvalMode {
    #[default]
    Tick,
    Hydrate,
}

/// Hydration consumers that need aggregate arrangements rebuilt opt into the
/// subscription policy; all other snapshot consumers retain probe semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HydrationMode {
    Ordinary,
    Subscription,
}

/// Key for one cached node evaluation within a logical tick.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EvalMemoKey {
    scope: ScopeId,
    node: NodeId,
    input_signature_hash: u64,
    /// Tick-mode results are deltas and are only reusable inside one public
    /// tick. Hydration results are snapshots, so their key omits the tick and
    /// validity is owned by the input frontier vector stored on the entry.
    tick_epoch: Option<u64>,
    /// Recursive sub-ticks intentionally affect memoization, not operator
    /// state identity.
    sub_tick: u64,
    context_digest: u64,
}

#[derive(Clone, Debug)]
struct EvalMemoEntry {
    records: Arc<RecordDeltas>,
    input_watermark: u64,
    payload_bytes: usize,
    last_used: u64,
}

impl EvalMemoEntry {
    fn new(
        records: Arc<RecordDeltas>,
        input_watermark: u64,
        payload_bytes: usize,
        last_used: u64,
    ) -> Self {
        Self {
            records,
            input_watermark,
            payload_bytes,
            last_used,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct NodeInputSignature {
    tables: Arc<[String]>,
    bindings: Arc<[String]>,
    frontier_bindings: Arc<[FrontierName]>,
    hash: u64,
}

impl NodeInputSignature {
    fn from_sets(
        tables: BTreeSet<String>,
        bindings: BTreeSet<String>,
        frontier_bindings: BTreeSet<FrontierName>,
    ) -> Self {
        let tables = tables.into_iter().collect::<Arc<[_]>>();
        let bindings = bindings.into_iter().collect::<Arc<[_]>>();
        let frontier_bindings = frontier_bindings.into_iter().collect::<Arc<[_]>>();
        let mut hasher = DefaultHasher::new();
        tables.hash(&mut hasher);
        bindings.hash(&mut hasher);
        frontier_bindings.hash(&mut hasher);
        let hash = hasher.finish();
        Self {
            tables,
            bindings,
            frontier_bindings,
            hash,
        }
    }
}

/// Current scoped inputs and logical time for node evaluation.
#[derive(Clone, Debug, Default)]
struct EvalContext {
    /// Current operator-state namespace.
    scope: ScopeId,
    /// Logical time within a recursive fixed-point evaluation.
    sub_tick: u64,
    /// FrontierSource bindings, currently used for recursive frontiers.
    bindings: HashMap<FrontierName, RecordDeltas>,
    binding_digests: HashMap<FrontierName, u64>,
    /// Hydrate preparation rebuilds arrangements instead of layering onto them.
    arrangement_update_mode: ArrangementUpdateMode,
    eval_mode: EvalMode,
    hydrate_arrangements: bool,
}

impl EvalContext {
    fn root() -> Self {
        Self {
            scope: ScopeId::root(),
            sub_tick: 0,
            bindings: HashMap::default(),
            binding_digests: HashMap::default(),
            arrangement_update_mode: ArrangementUpdateMode::Accumulate,
            eval_mode: EvalMode::Tick,
            hydrate_arrangements: false,
        }
    }

    fn root_snapshot() -> Self {
        Self {
            scope: ScopeId::root(),
            sub_tick: 0,
            bindings: HashMap::default(),
            binding_digests: HashMap::default(),
            arrangement_update_mode: ArrangementUpdateMode::Replace,
            eval_mode: EvalMode::Hydrate,
            hydrate_arrangements: false,
        }
    }

    fn root_subscription_snapshot() -> Self {
        Self {
            scope: ScopeId::root(),
            sub_tick: 0,
            bindings: HashMap::default(),
            binding_digests: HashMap::default(),
            arrangement_update_mode: ArrangementUpdateMode::Replace,
            eval_mode: EvalMode::Hydrate,
            hydrate_arrangements: true,
        }
    }

    pub(super) fn with_binding(
        scope: ScopeId,
        sub_tick: u64,
        binding: FrontierName,
        deltas: RecordDeltas,
    ) -> Self {
        let mut bindings = HashMap::default();
        let digest = record_deltas_digest(&deltas);
        bindings.insert(binding.clone(), deltas);
        let mut binding_digests = HashMap::default();
        binding_digests.insert(binding, digest);
        Self {
            scope,
            sub_tick,
            bindings,
            binding_digests,
            arrangement_update_mode: ArrangementUpdateMode::Accumulate,
            eval_mode: EvalMode::Tick,
            hydrate_arrangements: false,
        }
    }

    pub(super) fn with_binding_and_arrangement_mode(
        scope: ScopeId,
        sub_tick: u64,
        binding: FrontierName,
        deltas: RecordDeltas,
        arrangement_update_mode: ArrangementUpdateMode,
    ) -> Self {
        let mut bindings = HashMap::default();
        let digest = record_deltas_digest(&deltas);
        bindings.insert(binding.clone(), deltas);
        let mut binding_digests = HashMap::default();
        binding_digests.insert(binding, digest);
        Self {
            scope,
            sub_tick,
            bindings,
            binding_digests,
            arrangement_update_mode,
            eval_mode: EvalMode::Tick,
            hydrate_arrangements: false,
        }
    }
}

/// Stable handle returned to callers for subscription management.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriptionId(u64);

impl SubscriptionId {
    fn retainer_key(self) -> String {
        self.0.to_string()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PreparedShapeId(u64);

impl PreparedShapeId {
    fn retainer_key(self) -> String {
        self.0.to_string()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedShape {
    id: PreparedShapeId,
}

impl PreparedShape {
    pub fn id(&self) -> PreparedShapeId {
        self.id
    }
}

/// One prepared multisink terminal.
///
/// The terminal graph is the route-carrying graph Groove maintains: it includes
/// hidden route fields plus any columns that a sink may expose publicly. Binding
/// appends a route filter and public projection for each sink.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RoutedMultisinkTerminal {
    pub sink: String,
    pub graph: GraphBuilder,
    pub route_fields: Vec<String>,
    /// Binding descriptor positions paired with `route_fields`.
    pub route_value_indices: Vec<usize>,
    pub public_fields: Vec<String>,
}

impl RoutedMultisinkTerminal {
    pub fn new(
        sink: impl Into<String>,
        graph: GraphBuilder,
        route_fields: impl IntoIterator<Item = impl Into<String>>,
        public_fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let route_fields = route_fields.into_iter().map(Into::into).collect::<Vec<_>>();
        let route_value_indices = (0..route_fields.len()).collect();
        Self {
            sink: sink.into(),
            graph,
            route_fields,
            route_value_indices,
            public_fields: public_fields.into_iter().map(Into::into).collect(),
        }
    }

    /// Select non-prefix binding values for the terminal's route predicates.
    pub fn with_route_value_indices(
        mut self,
        route_value_indices: impl IntoIterator<Item = usize>,
    ) -> Self {
        self.route_value_indices = route_value_indices.into_iter().collect();
        self
    }
}

/// Receiving end of a one-sink live query subscription.
///
/// This is a convenience wrapper around [`MultisinkSubscription`] for callers
/// that only asked for one output. The runtime delivery path is still multisink.
#[derive(Debug)]
pub struct Subscription {
    inner: MultisinkSubscription,
    sink: String,
    output: RecordDescriptor,
}

impl Subscription {
    pub fn id(&self) -> SubscriptionId {
        self.inner.id()
    }

    pub fn recv(&self) -> Result<RecordDeltas, RecvError> {
        self.inner
            .recv()
            .map(|deltas| self.extract_sink_deltas(deltas))
    }

    pub fn try_recv(&self) -> Result<RecordDeltas, TryRecvError> {
        self.inner
            .try_recv()
            .map(|deltas| self.extract_sink_deltas(deltas))
    }

    fn extract_sink_deltas(&self, mut deltas: MultisinkDeltas) -> RecordDeltas {
        deltas
            .sinks
            .remove(&self.sink)
            .unwrap_or_else(|| RecordDeltas::empty(self.output))
    }
}

/// Deltas grouped by named output sink for one multisink graph subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultisinkDeltas {
    pub sinks: BTreeMap<String, RecordDeltas>,
    /// Structured terminal operations are a subscription-boundary output.
    /// Relational operators never consume this map.
    pub terminal_sinks: BTreeMap<String, TerminalDeltas>,
}

#[derive(Debug)]
struct QueuedMultisinkDeltas {
    // Explicit fragment-output drain channel: once a tick or hydration computes
    // subscription output, this queue owns delivery until the receiver drains
    // it. Eval memo is only a recompute cache and may be evicted independently.
    deltas: MultisinkDeltas,
}

impl QueuedMultisinkDeltas {
    fn new(deltas: MultisinkDeltas) -> Self {
        Self { deltas }
    }
}

impl MultisinkDeltas {
    pub fn is_empty(&self) -> bool {
        self.sinks.values().all(RecordDeltas::is_empty)
            && self.terminal_sinks.values().all(TerminalDeltas::is_empty)
    }

    pub fn get(&self, sink: &str) -> Option<&RecordDeltas> {
        self.sinks.get(sink)
    }
}

/// Incremental edits to a materialized terminal tree. Paths alternate public
/// collection fields and stable descendant keys, starting below `root_key`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TerminalDeltas {
    pub operations: Vec<TerminalOperation>,
}

impl TerminalDeltas {
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TerminalOperation {
    pub root_key: Vec<u8>,
    pub path: Vec<TerminalPathSegment>,
    pub edit: TerminalEdit,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum TerminalPathSegment {
    Collection(String),
    Key(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum TerminalEdit {
    Insert {
        index: usize,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Update {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Remove {
        key: Vec<u8>,
    },
    Move {
        key: Vec<u8>,
        index: usize,
    },
}

fn terminal_deltas_from_record_deltas(
    deltas: &RecordDeltas,
) -> Result<TerminalDeltas, IvmRuntimeError> {
    let mut before = BTreeMap::<Vec<u8>, OwnedRecord>::new();
    let mut after = BTreeMap::<Vec<u8>, OwnedRecord>::new();
    for delta in &deltas.deltas {
        let key = encoded_record_key_part(deltas.descriptor, delta.raw(), &[0])?;
        let record = OwnedRecord::new(delta.raw().to_vec(), deltas.descriptor);
        if delta.weight < 0 {
            before.insert(key, record);
        } else if delta.weight > 0 {
            after.insert(key, record);
        }
    }

    let mut keys = BTreeSet::new();
    keys.extend(before.keys().cloned());
    keys.extend(after.keys().cloned());
    let mut operations = Vec::new();
    for key in keys {
        match (before.get(&key), after.get(&key)) {
            (None, Some(record)) => operations.push(TerminalOperation {
                root_key: key.clone(),
                path: Vec::new(),
                edit: TerminalEdit::Insert {
                    index: 0,
                    key: key.clone(),
                    value: record.raw().to_vec(),
                },
            }),
            (Some(_), None) => operations.push(TerminalOperation {
                root_key: key.clone(),
                path: Vec::new(),
                edit: TerminalEdit::Remove { key: key.clone() },
            }),
            (Some(before), Some(after)) => {
                diff_terminal_record(&key, Vec::new(), before, after, &mut operations)?
            }
            (None, None) => unreachable!("terminal key came from before or after"),
        }
    }
    Ok(TerminalDeltas { operations })
}

fn order_terminal_snapshot(
    terminal: &mut RecordDeltas,
    ordering: &RecordDeltas,
) -> Result<(), IvmRuntimeError> {
    let mut positions = BTreeMap::new();
    for (index, delta) in ordering
        .deltas
        .iter()
        .filter(|delta| delta.weight > 0)
        .enumerate()
    {
        let key = encoded_record_key_part(ordering.descriptor, delta.raw(), &[0])?;
        positions.entry(key).or_insert(index);
    }
    let mut keyed = terminal
        .deltas
        .drain(..)
        .map(|delta| {
            let key = encoded_record_key_part(terminal.descriptor, delta.raw(), &[0])?;
            let position = positions.get(&key).copied().unwrap_or(usize::MAX);
            Ok((position, key, delta))
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
    keyed.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    terminal.deltas = keyed.into_iter().map(|(_, _, delta)| delta).collect();
    Ok(())
}

fn diff_terminal_record(
    root_key: &[u8],
    path: Vec<TerminalPathSegment>,
    before: &OwnedRecord,
    after: &OwnedRecord,
    operations: &mut Vec<TerminalOperation>,
) -> Result<(), IvmRuntimeError> {
    let before_values = before
        .to_values()
        .map_err(IvmRuntimeError::RecordEncoding)?;
    let after_values = after.to_values().map_err(IvmRuntimeError::RecordEncoding)?;
    let mut scalar_changed = false;
    for (index, (before_value, after_value)) in before_values.iter().zip(&after_values).enumerate()
    {
        let descriptor_field = after
            .descriptor()
            .fields()
            .get(index)
            .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(index))?;
        match (before_value, after_value) {
            (Value::Array(before_children), Value::Array(after_children))
                if matches!(
                    &descriptor_field.value_type,
                    ValueType::Array(inner) if matches!(inner.as_ref(), ValueType::Record(_))
                ) =>
            {
                let field = descriptor_field
                    .name
                    .clone()
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
                let mut child_path = path.clone();
                child_path.push(TerminalPathSegment::Collection(field));
                diff_terminal_collection(
                    root_key,
                    child_path,
                    before_children,
                    after_children,
                    operations,
                )?;
            }
            _ if before_value != after_value => scalar_changed = true,
            _ => {}
        }
    }
    if scalar_changed {
        let key = encoded_record_key_part(*after.descriptor(), after.raw(), &[0])?;
        operations.push(TerminalOperation {
            root_key: root_key.to_vec(),
            path,
            edit: TerminalEdit::Update {
                key,
                value: after.raw().to_vec(),
            },
        });
    }
    Ok(())
}

fn diff_terminal_collection(
    root_key: &[u8],
    path: Vec<TerminalPathSegment>,
    before_values: &[Value],
    after_values: &[Value],
    operations: &mut Vec<TerminalOperation>,
) -> Result<(), IvmRuntimeError> {
    let children =
        |values: &[Value]| -> Result<BTreeMap<Vec<u8>, (usize, OwnedRecord)>, IvmRuntimeError> {
            let mut children = BTreeMap::new();
            for (index, value) in values.iter().enumerate() {
                let Value::Record(record) = value else {
                    return Err(IvmRuntimeError::InvalidCollectBy(
                        "structured terminal arrays must contain records".to_owned(),
                    ));
                };
                let key = encoded_record_key_part(*record.descriptor(), record.raw(), &[0])?;
                if children.insert(key, (index, record.clone())).is_some() {
                    return Err(IvmRuntimeError::DuplicateCollectByOccurrenceId);
                }
            }
            Ok(children)
        };
    let before = children(before_values)?;
    let after = children(after_values)?;
    let mut keys = BTreeSet::new();
    keys.extend(before.keys().cloned());
    keys.extend(after.keys().cloned());
    for key in keys {
        match (before.get(&key), after.get(&key)) {
            (None, Some((index, record))) => operations.push(TerminalOperation {
                root_key: root_key.to_vec(),
                path: path.clone(),
                edit: TerminalEdit::Insert {
                    index: *index,
                    key: key.clone(),
                    value: record.raw().to_vec(),
                },
            }),
            (Some(_), None) => operations.push(TerminalOperation {
                root_key: root_key.to_vec(),
                path: path.clone(),
                edit: TerminalEdit::Remove { key: key.clone() },
            }),
            (Some((before_index, before_record)), Some((after_index, after_record))) => {
                if before_index != after_index {
                    operations.push(TerminalOperation {
                        root_key: root_key.to_vec(),
                        path: path.clone(),
                        edit: TerminalEdit::Move {
                            key: key.clone(),
                            index: *after_index,
                        },
                    });
                }
                let mut descendant_path = path.clone();
                descendant_path.push(TerminalPathSegment::Key(key));
                diff_terminal_record(
                    root_key,
                    descendant_path,
                    before_record,
                    after_record,
                    operations,
                )?;
            }
            (None, None) => unreachable!("child key came from before or after"),
        }
    }
    Ok(())
}

/// Receiving end of a live multisink graph subscription.
#[derive(Debug)]
pub struct MultisinkSubscription {
    id: SubscriptionId,
    receiver: Receiver<QueuedMultisinkDeltas>,
}

impl MultisinkSubscription {
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    pub fn recv(&self) -> Result<MultisinkDeltas, RecvError> {
        self.receiver.recv().map(|queued| queued.deltas)
    }

    pub fn try_recv(&self) -> Result<MultisinkDeltas, TryRecvError> {
        self.receiver.try_recv().map(|queued| queued.deltas)
    }
}

impl PredicateExpr {
    fn matches(
        &self,
        record: BorrowedRecord<'_>,
        comparison: ValueComparison,
    ) -> Result<bool, IvmRuntimeError> {
        match self {
            Self::Eq { field, value } => {
                compare_record_field(record, field, value, |ord| ord.is_eq(), comparison)
            }
            Self::Neq { field, value } => {
                compare_record_field(record, field, value, |ord| !ord.is_eq(), comparison)
            }
            Self::Contains { field, value } => {
                contains_record_field(record, field, value, comparison)
            }
            Self::EqField { field, value_field } => {
                compare_record_fields(record, field, value_field, |ord| ord.is_eq(), comparison)
            }
            Self::ContainsField {
                field,
                needle_field,
            } => contains_record_field_value(record, field, needle_field, comparison),
            Self::NeqField { field, value_field } => {
                compare_record_fields(record, field, value_field, |ord| !ord.is_eq(), comparison)
            }
            Self::Gt { field, value } => {
                compare_record_field(record, field, value, |ord| ord.is_gt(), comparison)
            }
            Self::GtEq { field, value } => {
                compare_record_field(record, field, value, |ord| ord.is_ge(), comparison)
            }
            Self::Lt { field, value } => {
                compare_record_field(record, field, value, |ord| ord.is_lt(), comparison)
            }
            Self::LtEq { field, value } => {
                compare_record_field(record, field, value, |ord| ord.is_le(), comparison)
            }
            Self::IsNull { field } => Ok(is_sql_null_value(&record.get(field)?)),
            Self::IsNotNull { field } => Ok(!is_sql_null_value(&record.get(field)?)),
            Self::And(predicates) => predicates
                .iter()
                .map(|predicate| predicate.matches(record, comparison))
                .try_fold(true, |acc, matches| matches.map(|matches| acc && matches)),
            Self::Or(predicates) => predicates
                .iter()
                .map(|predicate| predicate.matches(record, comparison))
                .try_fold(false, |acc, matches| matches.map(|matches| acc || matches)),
        }
    }
}

/// Deltas for one base table in a committed batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableDelta {
    pub table: String,
    /// Table-local discriminator selecting the descriptor for every encoded
    /// payload in this homogeneous delta group.
    pub variant_tag: u32,
    pub descriptor: RecordDescriptor,
    pub deltas: Vec<RecordDelta>,
}

/// Weighted change to one encoded record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordDelta {
    pub record: Bytes,
    pub weight: i64,
}

impl RecordDelta {
    pub fn raw(&self) -> &[u8] {
        &self.record
    }

    pub fn borrowed<'a>(&'a self, descriptor: &'a RecordDescriptor) -> BorrowedRecord<'a> {
        BorrowedRecord::new(&self.record, descriptor)
    }
}

#[derive(Clone, Debug)]
struct MultisinkSubscriptionState {
    sender: Sender<QueuedMultisinkDeltas>,
    outputs: BTreeMap<String, CompiledNode>,
    target: MultisinkSubscriptionTarget,
}

#[derive(Clone, Debug)]
enum MultisinkSubscriptionTarget {
    Direct,
    RoutedShape {
        shape_id: PreparedShapeId,
        binding_key: BindingKey,
    },
}

#[derive(Clone, Debug)]
struct RoutedMultisinkShapeState {
    shape: String,
    binding_descriptor: RecordDescriptor,
    terminals: BTreeMap<String, RoutedMultisinkTerminalState>,
    auto_family_key: Option<AutoDirectFamilyKey>,
}

#[derive(Clone, Debug)]
struct RoutedMultisinkTerminalState {
    terminal: RoutedMultisinkTerminal,
    output: CompiledNode,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct BindingKey(Vec<u8>);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct AutoDirectFamilyKey {
    graph: GraphBuilder,
    binding_descriptor: RecordDescriptor,
    binding_field: String,
    public_fields: Vec<String>,
}

struct AutoDirectFamilyPlan {
    key: AutoDirectFamilyKey,
    graph: GraphBuilder,
    shape: String,
    binding_descriptor: RecordDescriptor,
    binding_field: String,
    binding_value: Value,
    public_fields: Vec<String>,
}

#[derive(Clone, Debug)]
struct BindingSourceState {
    descriptor: RecordDescriptor,
    refcounts: HashMap<BindingKey, usize>,
}

#[derive(Clone, Debug)]
pub(super) struct BindingDelta {
    shape: String,
    descriptor: RecordDescriptor,
    deltas: Vec<RecordDelta>,
}

/// Result of lowering a graph-builder fragment into the deduplicated graph.
#[derive(Clone, Debug)]
struct CompiledNode {
    output: RecordDescriptor,
    node: NodeId,
    /// The `TopBy` node that defines ordering of public terminal roots.
    ///
    /// This is explicit lowering metadata: walking graph ancestors is
    /// ambiguous once a structured plan contains independently ordered nested
    /// collections.
    root_ordering_node: Option<NodeId>,
}

/// Descriptor plus a batch of weighted encoded record changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordDeltas {
    pub descriptor: RecordDescriptor,
    pub deltas: Vec<RecordDelta>,
}

impl RecordDeltas {
    fn empty(descriptor: RecordDescriptor) -> Self {
        Self {
            descriptor,
            deltas: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.deltas.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (BorrowedRecord<'_>, i64)> {
        self.deltas
            .iter()
            .map(|delta| (delta.borrowed(&self.descriptor), delta.weight))
    }

    pub fn to_values(&self) -> Result<Vec<(Vec<Value>, i64)>, records::Error> {
        self.iter()
            .map(|(record, weight)| record.to_values().map(|values| (values, weight)))
            .collect()
    }
}

fn record_deltas_encoded_bytes(deltas: &RecordDeltas) -> usize {
    deltas.deltas.iter().map(|delta| delta.record.len()).sum()
}

fn multisink_deltas_record_count(deltas: &MultisinkDeltas) -> usize {
    deltas
        .sinks
        .values()
        .map(|records| records.deltas.len())
        .sum()
}

fn multisink_deltas_encoded_bytes(deltas: &MultisinkDeltas) -> usize {
    deltas.sinks.values().map(record_deltas_encoded_bytes).sum()
}

fn descriptor_field_names(descriptor: &RecordDescriptor) -> Result<Vec<String>, IvmRuntimeError> {
    descriptor
        .fields()
        .iter()
        .map(|field| {
            field
                .name
                .clone()
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))
        })
        .collect()
}

pub(super) fn record_store_for_table<'a, S>(
    storage: &'a S,
    table: &'a TableSchema,
    descriptor: &'a RecordDescriptor,
) -> RecordStore<'a, S>
where
    S: OrderedKvStorage,
{
    RecordStore::new(storage, &table.name, descriptor)
}

fn validate_public_output_fields(
    source: &RecordDescriptor,
    public_output: &RecordDescriptor,
) -> Result<(), IvmRuntimeError> {
    for field in public_output.fields() {
        let name = field
            .name
            .as_ref()
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
        let index = source
            .field_index(name)
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(name.clone()))?;
        let source_field = source
            .fields()
            .get(index)
            .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(index))?;
        if !source_field
            .value_type
            .registry_compatible_with(&field.value_type)
        {
            return Err(IvmRuntimeError::GraphOutputMismatch);
        }
    }
    Ok(())
}

fn validate_public_output_for_shape(
    shape: &RoutedMultisinkShapeState,
    sink: &str,
    public_output: &RecordDescriptor,
) -> Result<(), IvmRuntimeError> {
    let terminal = shape
        .terminals
        .get(sink)
        .ok_or_else(|| IvmRuntimeError::DuplicateMultisinkSink(sink.to_owned()))?;
    validate_public_output_fields(&terminal.output.output, public_output)
}

fn bound_routed_multisink_graph(
    terminal: &RoutedMultisinkTerminal,
    binding_values: &[Value],
) -> GraphBuilder {
    let predicates = terminal
        .route_fields
        .iter()
        .zip(&terminal.route_value_indices)
        .map(|(field, index)| route_predicate(field, &binding_values[*index]))
        .collect::<Vec<_>>();
    let predicate = match predicates.as_slice() {
        [] => None,
        [predicate] => Some(predicate.clone()),
        _ => Some(PredicateExpr::And(predicates).canonicalize()),
    };
    if let GraphBuilder::CollectBy { input, collect } = &terminal.graph {
        // CollectBy is terminal-only. Route its flat input before rendering and
        // remove hidden route columns from the collector's own projection,
        // rather than appending filter/project consumers after the collector.
        let mut collect = collect.as_ref().clone();
        collect
            .parent_fields
            .retain(|field| terminal.public_fields.contains(&field.output_name));
        collect
            .tuple_fields
            .retain(|field| terminal.public_fields.contains(&field.output_name));
        let input = predicate
            .map(|predicate| input.as_ref().clone().filter(predicate))
            .unwrap_or_else(|| input.as_ref().clone());
        return GraphBuilder::CollectBy {
            input: Box::new(input),
            collect: Box::new(collect),
        };
    }
    let graph = predicate
        .map(|predicate| terminal.graph.clone().filter(predicate))
        .unwrap_or_else(|| terminal.graph.clone());
    graph.project(terminal.public_fields.clone())
}

fn route_predicate(field: &str, value: &Value) -> PredicateExpr {
    match value {
        Value::Nullable(None) => PredicateExpr::is_null(field),
        value => PredicateExpr::eq(field.to_owned(), value.clone()),
    }
}

fn count_builder_nodes(graph: &GraphBuilder) -> usize {
    match graph {
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::BindingSource { .. } => 1,
        GraphBuilder::Recursive { seed, step, .. } => {
            1 + count_builder_nodes(seed) + count_builder_nodes(step)
        }
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::Unnest { input, .. }
        | GraphBuilder::VariantProject { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. }
        | GraphBuilder::CollectBy { input, .. }
        | GraphBuilder::Aggregate { input, .. } => 1 + count_builder_nodes(input),
        GraphBuilder::Union { inputs } => 1 + inputs.iter().map(count_builder_nodes).sum::<usize>(),
        GraphBuilder::Join { left, right, .. }
        | GraphBuilder::SemiJoin { left, right, .. }
        | GraphBuilder::AntiJoin { left, right, .. } => {
            1 + count_builder_nodes(left) + count_builder_nodes(right)
        }
    }
}

fn builder_contains_binding_source(graph: &GraphBuilder) -> bool {
    match graph {
        GraphBuilder::BindingSource { .. } => true,
        GraphBuilder::Recursive { seed, step, .. } => {
            builder_contains_binding_source(seed) || builder_contains_binding_source(step)
        }
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::Unnest { input, .. }
        | GraphBuilder::VariantProject { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. }
        | GraphBuilder::CollectBy { input, .. }
        | GraphBuilder::Aggregate { input, .. } => builder_contains_binding_source(input),
        GraphBuilder::Union { inputs } => inputs.iter().any(builder_contains_binding_source),
        GraphBuilder::Join { left, right, .. }
        | GraphBuilder::SemiJoin { left, right, .. }
        | GraphBuilder::AntiJoin { left, right, .. } => {
            builder_contains_binding_source(left) || builder_contains_binding_source(right)
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. } => false,
    }
}

#[derive(Clone)]
struct LiftedLiteralFilter {
    graph: GraphBuilder,
    value: LiteralValue,
}

const AUTO_DIRECT_BINDING_PREFIX: &str = "\0groove.auto_direct.binding.";

fn auto_direct_binding_field(
    graph: &GraphBuilder,
    output: &RecordDescriptor,
    runtime: &IvmRuntime,
) -> Result<String, IvmRuntimeError> {
    let mut occupied = HashSet::new();
    collect_builder_field_names(graph, runtime, &mut occupied)?;
    occupied.extend(
        output
            .fields()
            .iter()
            .filter_map(|field| field.name.as_ref().cloned()),
    );
    for index in 0.. {
        let candidate = format!("{AUTO_DIRECT_BINDING_PREFIX}{index}");
        if !occupied.contains(&candidate) {
            return Ok(candidate);
        }
    }
    unreachable!("unbounded hidden binding field search should always find a free name")
}

fn collect_builder_field_names(
    graph: &GraphBuilder,
    runtime: &IvmRuntime,
    occupied: &mut HashSet<String>,
) -> Result<(), IvmRuntimeError> {
    let output = runtime.infer_builder_output(graph)?;
    occupied.extend(
        output
            .fields()
            .iter()
            .filter_map(|field| field.name.as_ref().cloned()),
    );
    match graph {
        GraphBuilder::Recursive { seed, step, .. } => {
            collect_builder_field_names(seed, runtime, occupied)?;
            collect_builder_field_names(step, runtime, occupied)?;
        }
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::Unnest { input, .. }
        | GraphBuilder::VariantProject { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. }
        | GraphBuilder::CollectBy { input, .. }
        | GraphBuilder::Aggregate { input, .. } => {
            collect_builder_field_names(input, runtime, occupied)?;
        }
        GraphBuilder::Union { inputs } => {
            for input in inputs {
                collect_builder_field_names(input, runtime, occupied)?;
            }
        }
        GraphBuilder::Join { left, right, .. }
        | GraphBuilder::SemiJoin { left, right, .. }
        | GraphBuilder::AntiJoin { left, right, .. } => {
            collect_builder_field_names(left, runtime, occupied)?;
            collect_builder_field_names(right, runtime, occupied)?;
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::BindingSource { .. } => {}
    }
    Ok(())
}

fn lift_literal_filter(
    runtime: &IvmRuntime,
    graph: &GraphBuilder,
    binding_field: &str,
) -> Result<Option<LiftedLiteralFilter>, IvmRuntimeError> {
    match graph {
        GraphBuilder::Filter {
            input,
            predicate,
            comparison,
        } => {
            if let PredicateExpr::Eq { field, value } = predicate {
                let joined =
                    literal_filter_binding_join((**input).clone(), field, value, binding_field)?;
                let input_output = runtime.infer_builder_output(input)?;
                let mut fields = input_output
                    .fields()
                    .iter()
                    .filter_map(|field| {
                        let name = field.name.clone()?;
                        Some(ProjectField::renamed(format!("left.{name}"), name))
                    })
                    .collect::<Vec<_>>();
                fields.push(ProjectField::renamed(
                    format!("right.{binding_field}"),
                    binding_field.to_owned(),
                ));
                return Ok(Some(LiftedLiteralFilter {
                    graph: joined.project_fields(fields),
                    value: value.clone(),
                }));
            }
            if let Some(lifted) = lift_literal_filter(runtime, input, binding_field)? {
                return Ok(Some(LiftedLiteralFilter {
                    graph: GraphBuilder::Filter {
                        input: Box::new(lifted.graph),
                        predicate: predicate.clone(),
                        comparison: *comparison,
                    },
                    value: lifted.value,
                }));
            }
            Ok(None)
        }
        GraphBuilder::Project { input, fields } => {
            if let GraphBuilder::Join {
                left,
                right,
                left_on,
                right_on,
                comparison,
            } = input.as_ref()
            {
                if let Some(lifted) = lift_literal_filter(runtime, left, binding_field)? {
                    let joined = GraphBuilder::Join {
                        left: Box::new(lifted.graph),
                        right: right.clone(),
                        left_on: left_on.clone(),
                        right_on: right_on.clone(),
                        comparison: *comparison,
                    };
                    let mut fields =
                        project_fields_against_rewritten_input(runtime, input, &joined, fields)?;
                    append_binding_project_field(
                        &mut fields,
                        binding_field,
                        binding_project_source(&joined, binding_field),
                    );
                    return Ok(Some(LiftedLiteralFilter {
                        graph: joined.project_fields(fields),
                        value: lifted.value,
                    }));
                }
                if let Some(lifted) = lift_literal_filter(runtime, right, binding_field)? {
                    let joined = GraphBuilder::Join {
                        left: left.clone(),
                        right: Box::new(lifted.graph),
                        left_on: left_on.clone(),
                        right_on: right_on.clone(),
                        comparison: *comparison,
                    };
                    let mut fields =
                        project_fields_against_rewritten_input(runtime, input, &joined, fields)?;
                    append_binding_project_field(
                        &mut fields,
                        binding_field,
                        binding_project_source(&joined, binding_field),
                    );
                    return Ok(Some(LiftedLiteralFilter {
                        graph: joined.project_fields(fields),
                        value: lifted.value,
                    }));
                }
            }
            if let GraphBuilder::Filter {
                input: filtered_input,
                predicate: PredicateExpr::Eq { field, value },
                ..
            } = input.as_ref()
            {
                let joined = literal_filter_binding_join(
                    (**filtered_input).clone(),
                    field,
                    value,
                    binding_field,
                )?;
                let input_output = runtime.infer_builder_output(filtered_input)?;
                let mut fields = fields
                    .iter()
                    .map(|field| match &field.expression {
                        ProjectExpr::Field(source) => {
                            let source =
                                project_source_from_joined_filter_input(&input_output, source)?;
                            Ok(ProjectField::renamed(source, field.output_name.clone()))
                        }
                        ProjectExpr::Literal(value) => Ok(ProjectField::literal(
                            field.output_name.clone(),
                            value.clone(),
                        )),
                        ProjectExpr::TypedLiteral { value, value_type } => {
                            Ok(ProjectField::literal_typed(
                                field.output_name.clone(),
                                value.clone(),
                                value_type.clone(),
                            ))
                        }
                        ProjectExpr::Null(value_type) => Ok(ProjectField::null_typed(
                            field.output_name.clone(),
                            value_type.clone(),
                        )),
                        ProjectExpr::Nullable(source) => {
                            let source =
                                project_source_from_joined_filter_input(&input_output, source)?;
                            Ok(ProjectField::nullable(source, field.output_name.clone()))
                        }
                        ProjectExpr::NullableFlat(source) => {
                            let source =
                                project_source_from_joined_filter_input(&input_output, source)?;
                            Ok(ProjectField::nullable_flat(
                                source,
                                field.output_name.clone(),
                            ))
                        }
                        ProjectExpr::EnumTagRemap { source, tags } => {
                            let source =
                                project_source_from_joined_filter_input(&input_output, source)?;
                            Ok(ProjectField::enum_tag_remap(
                                source,
                                field.output_name.clone(),
                                tags.clone(),
                            ))
                        }
                        ProjectExpr::EnumRemap { source, tags } => {
                            let source =
                                project_source_from_joined_filter_input(&input_output, source)?;
                            Ok(ProjectField::enum_remap(
                                source,
                                field.output_name.clone(),
                                tags.clone(),
                            ))
                        }
                        ProjectExpr::RecursiveEnumRemap {
                            source,
                            target,
                            remaps,
                        } => {
                            let source =
                                project_source_from_joined_filter_input(&input_output, source)?;
                            Ok(ProjectField::recursive_enum_remap(
                                source,
                                field.output_name.clone(),
                                target.clone(),
                                remaps.clone(),
                            ))
                        }
                    })
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
                fields.push(ProjectField::renamed(
                    format!("right.{binding_field}"),
                    binding_field.to_owned(),
                ));
                return Ok(Some(LiftedLiteralFilter {
                    graph: joined.project_fields(fields),
                    value: value.clone(),
                }));
            }
            let Some(lifted) = lift_literal_filter(runtime, input, binding_field)? else {
                return Ok(None);
            };
            let mut fields = fields.clone();
            append_binding_project_field(
                &mut fields,
                binding_field,
                binding_project_source(&lifted.graph, binding_field),
            );
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::Project {
                    input: Box::new(lifted.graph),
                    fields,
                },
                value: lifted.value,
            }))
        }
        GraphBuilder::Join {
            left,
            right,
            left_on,
            right_on,
            comparison,
        } => {
            if let Some(lifted) = lift_literal_filter(runtime, left, binding_field)? {
                let original_output = runtime.infer_builder_output(graph)?;
                let joined = GraphBuilder::Join {
                    left: Box::new(lifted.graph),
                    right: right.clone(),
                    left_on: left_on.clone(),
                    right_on: right_on.clone(),
                    comparison: *comparison,
                };
                return Ok(Some(LiftedLiteralFilter {
                    graph: project_to_output_with_binding(
                        runtime,
                        joined,
                        &original_output,
                        binding_field,
                    )?,
                    value: lifted.value,
                }));
            }
            if let Some(lifted) = lift_literal_filter(runtime, right, binding_field)? {
                let original_output = runtime.infer_builder_output(graph)?;
                let joined = GraphBuilder::Join {
                    left: left.clone(),
                    right: Box::new(lifted.graph),
                    left_on: left_on.clone(),
                    right_on: right_on.clone(),
                    comparison: *comparison,
                };
                return Ok(Some(LiftedLiteralFilter {
                    graph: project_to_output_with_binding(
                        runtime,
                        joined,
                        &original_output,
                        binding_field,
                    )?,
                    value: lifted.value,
                }));
            }
            Ok(None)
        }
        GraphBuilder::AntiJoin {
            left,
            right,
            left_on,
            right_on,
            comparison,
        } => {
            let Some(lifted) = lift_literal_filter(runtime, left, binding_field)? else {
                return Ok(None);
            };
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::AntiJoin {
                    left: Box::new(lifted.graph),
                    right: right.clone(),
                    left_on: left_on.clone(),
                    right_on: right_on.clone(),
                    comparison: *comparison,
                },
                value: lifted.value,
            }))
        }
        GraphBuilder::SemiJoin {
            left,
            right,
            left_on,
            right_on,
            comparison,
        } => {
            let Some(lifted) = lift_literal_filter(runtime, left, binding_field)? else {
                return Ok(None);
            };
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::SemiJoin {
                    left: Box::new(lifted.graph),
                    right: right.clone(),
                    left_on: left_on.clone(),
                    right_on: right_on.clone(),
                    comparison: *comparison,
                },
                value: lifted.value,
            }))
        }
        GraphBuilder::Recursive { .. } => Ok(None),
        GraphBuilder::Union { .. } | GraphBuilder::VariantProject { .. } => Ok(None),
        GraphBuilder::UnwrapNullable { input, field } => {
            let Some(lifted) = lift_literal_filter(runtime, input, binding_field)? else {
                return Ok(None);
            };
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::UnwrapNullable {
                    input: Box::new(lifted.graph),
                    field: field.clone(),
                },
                value: lifted.value,
            }))
        }
        GraphBuilder::Unnest {
            input,
            array_field,
            element_field,
        } => {
            let Some(lifted) = lift_literal_filter(runtime, input, binding_field)? else {
                return Ok(None);
            };
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::Unnest {
                    input: Box::new(lifted.graph),
                    array_field: array_field.clone(),
                    element_field: element_field.clone(),
                },
                value: lifted.value,
            }))
        }
        GraphBuilder::ArgMaxBy {
            input,
            group_cols,
            order_cols,
        } => {
            let Some(lifted) = lift_literal_filter(runtime, input, binding_field)? else {
                return Ok(None);
            };
            let mut group_cols = group_cols.clone();
            group_cols.push(FieldRef::name(binding_field));
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::ArgMaxBy {
                    input: Box::new(lifted.graph),
                    group_cols,
                    order_cols: order_cols.clone(),
                },
                value: lifted.value,
            }))
        }
        GraphBuilder::ArgMinBy {
            input,
            group_cols,
            order_cols,
        } => {
            let Some(lifted) = lift_literal_filter(runtime, input, binding_field)? else {
                return Ok(None);
            };
            let mut group_cols = group_cols.clone();
            group_cols.push(FieldRef::name(binding_field));
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::ArgMinBy {
                    input: Box::new(lifted.graph),
                    group_cols,
                    order_cols: order_cols.clone(),
                },
                value: lifted.value,
            }))
        }
        GraphBuilder::TopBy {
            input,
            group_cols,
            order_cols,
            tie_cols,
            offset,
            limit,
        } => {
            let Some(lifted) = lift_literal_filter(runtime, input, binding_field)? else {
                return Ok(None);
            };
            let mut group_cols = group_cols.clone();
            group_cols.push(FieldRef::name(binding_field));
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::TopBy {
                    input: Box::new(lifted.graph),
                    group_cols,
                    order_cols: order_cols.clone(),
                    tie_cols: tie_cols.clone(),
                    offset: *offset,
                    limit: *limit,
                },
                value: lifted.value,
            }))
        }
        GraphBuilder::CollectBy { .. } => Ok(None),
        GraphBuilder::Aggregate {
            input,
            group_cols,
            aggregates,
        } => {
            let Some(lifted) = lift_literal_filter(runtime, input, binding_field)? else {
                return Ok(None);
            };
            let mut group_cols = group_cols.clone();
            group_cols.push(FieldRef::name(binding_field));
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::Aggregate {
                    input: Box::new(lifted.graph),
                    group_cols,
                    aggregates: aggregates.clone(),
                },
                value: lifted.value,
            }))
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::BindingSource { .. } => Ok(None),
    }
}

fn literal_filter_binding_join(
    input: GraphBuilder,
    field: &str,
    value: &LiteralValue,
    binding_field: &str,
) -> Result<GraphBuilder, IvmRuntimeError> {
    let value_type = value
        .value_type()
        .ok_or(IvmRuntimeError::UnsupportedOperator)?;
    let binding = GraphBuilder::binding_source(
        "__auto_direct_shape",
        RecordDescriptor::new([(binding_field.to_owned(), value_type)]),
    );
    Ok(GraphBuilder::join(
        input,
        binding,
        [field.to_owned()],
        [binding_field.to_owned()],
    ))
}

fn project_source_from_joined_filter_input(
    input_output: &RecordDescriptor,
    source: &FieldRef,
) -> Result<String, IvmRuntimeError> {
    Ok(format!("left.{}", field_ref_name(input_output, source)?))
}

fn project_fields_against_rewritten_input(
    runtime: &IvmRuntime,
    original_input: &GraphBuilder,
    rewritten_input: &GraphBuilder,
    fields: &[ProjectField],
) -> Result<Vec<ProjectField>, IvmRuntimeError> {
    let original_output = runtime.infer_builder_output(original_input)?;
    let rewritten_output = runtime.infer_builder_output(rewritten_input)?;
    fields
        .iter()
        .map(|field| {
            let (field_ref, nullable_projection) = match &field.expression {
                ProjectExpr::Field(field_ref) => (field_ref, None),
                ProjectExpr::Nullable(field_ref) => (field_ref, Some(false)),
                ProjectExpr::NullableFlat(field_ref) => (field_ref, Some(true)),
                ProjectExpr::EnumTagRemap { source, tags } => {
                    let source = field_ref_name(&original_output, source)?;
                    if rewritten_output.field_index(&source).is_none() {
                        return Err(IvmRuntimeError::GraphFieldNotFound(source));
                    }
                    return Ok(ProjectField::enum_tag_remap(
                        source,
                        field.output_name.clone(),
                        tags.clone(),
                    ));
                }
                ProjectExpr::EnumRemap { source, tags } => {
                    let source = field_ref_name(&original_output, source)?;
                    if rewritten_output.field_index(&source).is_none() {
                        return Err(IvmRuntimeError::GraphFieldNotFound(source));
                    }
                    return Ok(ProjectField::enum_remap(
                        source,
                        field.output_name.clone(),
                        tags.clone(),
                    ));
                }
                ProjectExpr::RecursiveEnumRemap {
                    source,
                    target,
                    remaps,
                } => {
                    let source = field_ref_name(&original_output, source)?;
                    if rewritten_output.field_index(&source).is_none() {
                        return Err(IvmRuntimeError::GraphFieldNotFound(source));
                    }
                    return Ok(ProjectField::recursive_enum_remap(
                        source,
                        field.output_name.clone(),
                        target.clone(),
                        remaps.clone(),
                    ));
                }
                ProjectExpr::Literal(_)
                | ProjectExpr::TypedLiteral { .. }
                | ProjectExpr::Null(_) => return Ok(field.clone()),
            };
            let source = field_ref_name(&original_output, field_ref)?;
            if rewritten_output.field_index(&source).is_none() {
                return Err(IvmRuntimeError::GraphFieldNotFound(source));
            }
            match nullable_projection {
                None => Ok(ProjectField::renamed(source, field.output_name.clone())),
                Some(false) => Ok(ProjectField::nullable(source, field.output_name.clone())),
                Some(true) => Ok(ProjectField::nullable_flat(
                    source,
                    field.output_name.clone(),
                )),
            }
        })
        .collect()
}

fn project_to_output_with_binding(
    runtime: &IvmRuntime,
    graph: GraphBuilder,
    original_output: &RecordDescriptor,
    binding_field: &str,
) -> Result<GraphBuilder, IvmRuntimeError> {
    let lifted_output = runtime.infer_builder_output(&graph)?;
    let mut fields = original_output
        .fields()
        .iter()
        .map(|field| {
            let name = field
                .name
                .clone()
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
            if lifted_output.field_index(&name).is_none() {
                return Err(IvmRuntimeError::GraphFieldNotFound(name));
            }
            Ok(ProjectField::renamed(name.clone(), name))
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
    append_binding_project_field(
        &mut fields,
        binding_field,
        binding_project_source(&graph, binding_field),
    );
    Ok(GraphBuilder::Project {
        input: Box::new(graph),
        fields,
    })
}

fn append_binding_project_field(
    fields: &mut Vec<ProjectField>,
    binding_field: &str,
    source: String,
) {
    if !fields
        .iter()
        .any(|field| field.output_name == binding_field)
    {
        fields.push(ProjectField::renamed(source, binding_field.to_owned()));
    }
}

fn binding_project_source(input: &GraphBuilder, binding_field: &str) -> String {
    match input {
        GraphBuilder::Join { left, right, .. }
        | GraphBuilder::SemiJoin { left, right, .. }
        | GraphBuilder::AntiJoin { left, right, .. } => {
            if graph_outputs_binding(left, binding_field) {
                format!("left.{binding_field}")
            } else if graph_outputs_binding(right, binding_field) {
                format!("right.{binding_field}")
            } else {
                binding_field.to_owned()
            }
        }
        _ => binding_field.to_owned(),
    }
}

fn graph_outputs_binding(graph: &GraphBuilder, binding_field: &str) -> bool {
    match graph {
        GraphBuilder::BindingSource { output, .. }
        | GraphBuilder::FrontierSource { output, .. }
        | GraphBuilder::InlineRecords { output, .. } => output.field_index(binding_field).is_some(),
        GraphBuilder::Project { fields, .. } => fields
            .iter()
            .any(|field| field.output_name == binding_field),
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::Unnest { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. }
        | GraphBuilder::CollectBy { input, .. }
        | GraphBuilder::Aggregate { input, .. } => graph_outputs_binding(input, binding_field),
        GraphBuilder::Recursive { seed, .. } => graph_outputs_binding(seed, binding_field),
        GraphBuilder::Join { left, right, .. }
        | GraphBuilder::SemiJoin { left, right, .. }
        | GraphBuilder::AntiJoin { left, right, .. } => {
            graph_outputs_binding(left, binding_field)
                || graph_outputs_binding(right, binding_field)
        }
        GraphBuilder::Union { inputs } => inputs
            .iter()
            .any(|input| graph_outputs_binding(input, binding_field)),
        GraphBuilder::Table { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::VariantProject { .. } => false,
    }
}

#[allow(dead_code)]
fn propagate_binding_through_frontier(
    graph: &GraphBuilder,
    frontier: &FrontierName,
    binding_field: &str,
    binding_type: ValueType,
) -> Option<GraphBuilder> {
    match graph {
        GraphBuilder::FrontierSource { binding, output } if binding == frontier => {
            let fields = output
                .fields()
                .iter()
                .filter_map(|field| Some((field.name.clone()?, field.value_type.clone())));
            let fields = if output.field_index(binding_field).is_some() {
                fields.collect::<Vec<_>>()
            } else {
                fields
                    .chain([(binding_field.to_owned(), binding_type.clone())])
                    .collect::<Vec<_>>()
            };
            Some(GraphBuilder::frontier_source(
                binding.0.clone(),
                RecordDescriptor::new(fields),
            ))
        }
        GraphBuilder::Filter {
            input,
            predicate,
            comparison,
        } => {
            let input =
                propagate_binding_through_frontier(input, frontier, binding_field, binding_type)?;
            Some(GraphBuilder::Filter {
                input: Box::new(input),
                predicate: predicate.clone(),
                comparison: *comparison,
            })
        }
        GraphBuilder::Project { input, fields } => {
            let input =
                propagate_binding_through_frontier(input, frontier, binding_field, binding_type)?;
            let mut fields = fields.clone();
            append_binding_project_field(
                &mut fields,
                binding_field,
                binding_project_source(&input, binding_field),
            );
            Some(GraphBuilder::Project {
                input: Box::new(input),
                fields,
            })
        }
        GraphBuilder::UnwrapNullable { input, field } => {
            let input =
                propagate_binding_through_frontier(input, frontier, binding_field, binding_type)?;
            Some(GraphBuilder::UnwrapNullable {
                input: Box::new(input),
                field: field.clone(),
            })
        }
        GraphBuilder::Unnest {
            input,
            array_field,
            element_field,
        } => {
            let input =
                propagate_binding_through_frontier(input, frontier, binding_field, binding_type)?;
            Some(GraphBuilder::Unnest {
                input: Box::new(input),
                array_field: array_field.clone(),
                element_field: element_field.clone(),
            })
        }
        GraphBuilder::Join {
            left,
            right,
            left_on,
            right_on,
            comparison,
        } => {
            let left = propagate_binding_through_frontier(
                left,
                frontier,
                binding_field,
                binding_type.clone(),
            )
            .unwrap_or_else(|| (**left).clone());
            let right =
                propagate_binding_through_frontier(right, frontier, binding_field, binding_type)
                    .unwrap_or_else(|| (**right).clone());
            Some(GraphBuilder::Join {
                left: Box::new(left),
                right: Box::new(right),
                left_on: left_on.clone(),
                right_on: right_on.clone(),
                comparison: *comparison,
            })
        }
        GraphBuilder::SemiJoin {
            left,
            right,
            left_on,
            right_on,
            comparison,
        } => {
            let left =
                propagate_binding_through_frontier(left, frontier, binding_field, binding_type)?;
            Some(GraphBuilder::SemiJoin {
                left: Box::new(left),
                right: right.clone(),
                left_on: left_on.clone(),
                right_on: right_on.clone(),
                comparison: *comparison,
            })
        }
        GraphBuilder::AntiJoin {
            left,
            right,
            left_on,
            right_on,
            comparison,
        } => {
            let left =
                propagate_binding_through_frontier(left, frontier, binding_field, binding_type)?;
            Some(GraphBuilder::AntiJoin {
                left: Box::new(left),
                right: right.clone(),
                left_on: left_on.clone(),
                right_on: right_on.clone(),
                comparison: *comparison,
            })
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::BindingSource { .. }
        | GraphBuilder::Recursive { .. }
        | GraphBuilder::ArgMaxBy { .. }
        | GraphBuilder::ArgMinBy { .. }
        | GraphBuilder::TopBy { .. }
        | GraphBuilder::CollectBy { .. }
        | GraphBuilder::Aggregate { .. }
        | GraphBuilder::Union { .. }
        | GraphBuilder::VariantProject { .. } => None,
    }
}

fn replace_binding_shape(graph: GraphBuilder, shape: &str) -> GraphBuilder {
    match graph {
        GraphBuilder::BindingSource { output, .. } => GraphBuilder::binding_source(shape, output),
        GraphBuilder::Recursive {
            seed,
            step,
            frontier,
            max_iters,
        } => GraphBuilder::Recursive {
            seed: Box::new(replace_binding_shape(*seed, shape)),
            step: Box::new(replace_binding_shape(*step, shape)),
            frontier,
            max_iters,
        },
        GraphBuilder::Filter {
            input,
            predicate,
            comparison,
        } => GraphBuilder::Filter {
            input: Box::new(replace_binding_shape(*input, shape)),
            predicate,
            comparison,
        },
        GraphBuilder::Project { input, fields } => GraphBuilder::Project {
            input: Box::new(replace_binding_shape(*input, shape)),
            fields,
        },
        GraphBuilder::UnwrapNullable { input, field } => GraphBuilder::UnwrapNullable {
            input: Box::new(replace_binding_shape(*input, shape)),
            field,
        },
        GraphBuilder::Unnest {
            input,
            array_field,
            element_field,
        } => GraphBuilder::Unnest {
            input: Box::new(replace_binding_shape(*input, shape)),
            array_field,
            element_field,
        },
        GraphBuilder::ArgMaxBy {
            input,
            group_cols,
            order_cols,
        } => GraphBuilder::ArgMaxBy {
            input: Box::new(replace_binding_shape(*input, shape)),
            group_cols,
            order_cols,
        },
        GraphBuilder::ArgMinBy {
            input,
            group_cols,
            order_cols,
        } => GraphBuilder::ArgMinBy {
            input: Box::new(replace_binding_shape(*input, shape)),
            group_cols,
            order_cols,
        },
        GraphBuilder::TopBy {
            input,
            group_cols,
            order_cols,
            tie_cols,
            offset,
            limit,
        } => GraphBuilder::TopBy {
            input: Box::new(replace_binding_shape(*input, shape)),
            group_cols,
            order_cols,
            tie_cols,
            offset,
            limit,
        },
        GraphBuilder::Aggregate {
            input,
            group_cols,
            aggregates,
        } => GraphBuilder::Aggregate {
            input: Box::new(replace_binding_shape(*input, shape)),
            group_cols,
            aggregates,
        },
        GraphBuilder::Union { inputs } => GraphBuilder::Union {
            inputs: inputs
                .into_iter()
                .map(|input| replace_binding_shape(input, shape))
                .collect(),
        },
        GraphBuilder::Join {
            left,
            right,
            left_on,
            right_on,
            comparison,
        } => GraphBuilder::Join {
            left: Box::new(replace_binding_shape(*left, shape)),
            right: Box::new(replace_binding_shape(*right, shape)),
            left_on,
            right_on,
            comparison,
        },
        GraphBuilder::SemiJoin {
            left,
            right,
            left_on,
            right_on,
            comparison,
        } => GraphBuilder::SemiJoin {
            left: Box::new(replace_binding_shape(*left, shape)),
            right: Box::new(replace_binding_shape(*right, shape)),
            left_on,
            right_on,
            comparison,
        },
        GraphBuilder::AntiJoin {
            left,
            right,
            left_on,
            right_on,
            comparison,
        } => GraphBuilder::AntiJoin {
            left: Box::new(replace_binding_shape(*left, shape)),
            right: Box::new(replace_binding_shape(*right, shape)),
            left_on,
            right_on,
            comparison,
        },
        graph => graph,
    }
}

/// Retention and GC metadata for a deduplicated graph node.
#[derive(Clone, Debug, Default)]
struct NodeRuntimeMeta {
    retainers: HashSet<Retainer>,
    last_used_tick: u64,
    depends_on_context: Option<bool>,
    input_signature: Option<Arc<NodeInputSignature>>,
    input_generation: u64,
    raw_projection_fields: Option<Option<Arc<[RawProjectionField]>>>,
    join_left_fields: Option<Arc<[String]>>,
    join_right_fields: Option<Arc<[String]>>,
    join_output_mapping: Option<Arc<[(usize, usize)]>>,
    aggregate_group_fields: Option<Arc<[String]>>,
}

/// Namespace for stateless operator helper methods.
struct NodeState;

impl NodeState {
    fn update_table_source(
        input: &TableSourceOp,
        schema: &DatabaseSchema,
        variant_projections: &HashMap<VariantProjectionKey, VariantProjection>,
        output_desc: &RecordDescriptor,
        table_deltas: &[TableDelta],
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let table_schema = schema
            .table(&input.table)
            .ok_or_else(|| IvmRuntimeError::TableNotFound(input.table.clone()))?;
        let primary_key_fields = input
            .scan
            .as_ref()
            .map(|_| primary_key_field_indices(table_schema, output_desc))
            .transpose()?
            .unwrap_or_default();
        let mut deltas = Vec::new();
        let projection = input
            .variant_projection
            .as_ref()
            .map(|target| {
                variant_projections
                    .get(&VariantProjectionKey {
                        table: input.table.clone(),
                        target: target.clone(),
                    })
                    .ok_or_else(|| IvmRuntimeError::VariantProjectionNotFound {
                        table: input.table.clone(),
                        target: variant_projection_target_name(target).to_owned(),
                    })
            })
            .transpose()?;
        for delta in table_deltas
            .iter()
            .filter(|delta| delta.table == input.table)
        {
            let projected;
            let source_deltas = if let Some(projection) = projection {
                if !projection.output.registry_compatible_with(output_desc) {
                    return Err(IvmRuntimeError::GraphOutputMismatch);
                }
                let case = projection.cases.get(&delta.variant_tag).ok_or_else(|| {
                    IvmRuntimeError::VariantProjectionCaseNotFound {
                        table: input.table.clone(),
                        target: input
                            .variant_projection
                            .as_ref()
                            .map(variant_projection_target_name)
                            .unwrap_or_default()
                            .to_owned(),
                        version: u64::from(delta.variant_tag),
                    }
                })?;
                if !case.source().registry_compatible_with(&delta.descriptor) {
                    return Err(IvmRuntimeError::VariantProjectionSourceMismatch {
                        table: input.table.clone(),
                        target: input
                            .variant_projection
                            .as_ref()
                            .map(variant_projection_target_name)
                            .unwrap_or_default()
                            .to_owned(),
                        version: u64::from(delta.variant_tag),
                    });
                }
                match case {
                    VariantProjectionCase::Project {
                        project,
                        raw_projection,
                        ..
                    } => {
                        projected = Self::update_map_project(
                            project,
                            *output_desc,
                            &RecordDeltas {
                                descriptor: delta.descriptor,
                                deltas: delta.deltas.clone(),
                            },
                            raw_projection.as_deref(),
                        )?;
                        &projected.deltas
                    }
                    VariantProjectionCase::Enum {
                        tag,
                        payload,
                        project,
                        raw_projection,
                        ..
                    } => {
                        projected = Self::update_variant_enum_project(
                            *tag,
                            *payload,
                            project,
                            *output_desc,
                            &RecordDeltas {
                                descriptor: delta.descriptor,
                                deltas: delta.deltas.clone(),
                            },
                            raw_projection.as_deref(),
                        )?;
                        &projected.deltas
                    }
                    VariantProjectionCase::Ignore { .. } => continue,
                }
            } else {
                if !delta.descriptor.registry_compatible_with(output_desc) {
                    return Err(IvmRuntimeError::GraphOutputMismatch);
                }
                &delta.deltas
            };
            for record_delta in source_deltas {
                if let Some(scan) = &input.scan {
                    let key = primary_key_value_bytes(
                        output_desc,
                        record_delta.raw(),
                        &primary_key_fields,
                    )?;
                    if !key_matches_static_scan(&key, scan)? {
                        continue;
                    }
                }
                deltas.push(record_delta.clone());
            }
        }
        Ok(RecordDeltas {
            descriptor: *output_desc,
            deltas,
        })
    }

    fn update_index_source<S>(
        input: &IndexSourceOp,
        schema: &DatabaseSchema,
        variant_projections: &HashMap<VariantProjectionKey, VariantProjection>,
        output_desc: &RecordDescriptor,
        table_deltas: &[TableDelta],
        storage: Option<&S>,
        eval_mode: EvalMode,
    ) -> Result<RecordDeltas, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        if eval_mode == EvalMode::Hydrate {
            let storage = storage.ok_or(IvmRuntimeError::StorageUnavailable)?;
            let store = RecordStore::new(storage, "indices", output_desc);
            let mut deltas = Vec::new();
            let mut visit = |_: &[u8], record: &[u8]| {
                deltas.push(RecordDelta {
                    record: Bytes::copy_from_slice(record),
                    weight: 1,
                });
                Ok(())
            };
            match persisted_index_scan_bounds(&input.table, &input.index, input.scan.as_ref())? {
                StaticScanBounds::Prefix(prefix) => store.scan_prefix(&prefix, &mut visit)?,
                StaticScanBounds::Range { start, end } => {
                    if start < end {
                        store.scan_range(&start, &end, &mut visit)?;
                    }
                }
            }
            return Ok(RecordDeltas {
                descriptor: *output_desc,
                deltas,
            });
        }

        let index_by = IndexByOp {
            key_expressions: Vec::new(),
            value_expressions: Vec::new(),
            explicit_index: None,
            key_fields: input.key_fields.clone(),
            value_fields: input.value_fields.clone(),
            unique: input.unique,
            append_value_to_key: input.append_value_to_key,
            store_value: input.store_value,
            scan: input.scan.clone(),
        };
        let source = Self::update_table_source(
            &TableSourceOp {
                table: input.table.clone(),
                scan: None,
                variant_projection: input.variant_projection.clone(),
            },
            schema,
            variant_projections,
            &input.input_descriptor,
            table_deltas,
        )?;
        let deltas = apply_index_by(&index_by, &input.input_descriptor, &source.deltas)?;
        Ok(RecordDeltas {
            descriptor: *output_desc,
            deltas,
        })
    }

    fn update_binding_source(
        input: &BindingSourceOp,
        output_desc: &RecordDescriptor,
        binding_deltas: &[BindingDelta],
        binding_snapshots: &HashMap<String, RecordDeltas>,
        mode: ArrangementUpdateMode,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        if mode == ArrangementUpdateMode::Replace {
            let Some(snapshot) = binding_snapshots.get(&input.shape) else {
                return Ok(RecordDeltas::empty(*output_desc));
            };
            return project_binding_source_deltas(snapshot, output_desc);
        }
        let mut deltas = Vec::new();
        for delta in binding_deltas
            .iter()
            .filter(|delta| delta.shape == input.shape)
        {
            deltas.extend(
                project_binding_source_deltas(
                    &RecordDeltas {
                        descriptor: delta.descriptor,
                        deltas: delta.deltas.clone(),
                    },
                    output_desc,
                )?
                .deltas,
            );
        }
        Ok(RecordDeltas {
            descriptor: *output_desc,
            deltas,
        })
    }

    fn update_filter(
        filter: &FilterOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let predicate = &filter.predicate;
        let mut deltas = Vec::new();
        for delta in &input.deltas {
            if predicate.matches(delta.borrowed(&input.descriptor), filter.comparison)? {
                deltas.push(delta.clone());
            }
        }
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_map_project(
        project: &MapProjectOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
        raw_projection: Option<&[RawProjectionField]>,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let estimated_output_bytes = input
            .deltas
            .iter()
            .map(|delta| delta.record.len())
            .sum::<usize>();
        let mut output = BytesMut::with_capacity(estimated_output_bytes);
        let mut spans = Vec::with_capacity(input.deltas.len());
        let mut raw_projection_scratch = RawProjectionScratch::default();
        for delta in &input.deltas {
            let span = if let Some(fields) = raw_projection {
                output_desc
                    .project_raw_fields_into(
                        &input.descriptor,
                        delta.raw(),
                        fields,
                        &mut output,
                        &mut raw_projection_scratch,
                    )
                    .map_err(IvmRuntimeError::RecordEncoding)?
            } else {
                let start = output.len();
                let record = project_record(
                    &project.expressions,
                    &project.mapping,
                    output_desc,
                    &input.descriptor,
                    delta.raw(),
                )?;
                output.extend_from_slice(&record);
                start..output.len()
            };
            spans.push((span, delta.weight));
        }
        let output = output.freeze();
        let deltas = spans
            .into_iter()
            .map(|(span, weight)| RecordDelta {
                record: output.slice(span),
                weight,
            })
            .collect();
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_variant_enum_project(
        tag: u32,
        payload_desc: RecordDescriptor,
        project: &MapProjectOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
        raw_projection: Option<&[RawProjectionField]>,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let payloads = Self::update_map_project(project, payload_desc, input, raw_projection)?;
        let mut deltas = Vec::with_capacity(payloads.deltas.len());
        for payload in payloads.deltas {
            let value = Value::Enum(EnumValue::new(
                tag,
                OwnedRecord::new(payload.record.to_vec(), payload_desc),
            ));
            deltas.push(RecordDelta {
                record: output_desc.create(&[value])?.into(),
                weight: payload.weight,
            });
        }
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_variant_project(
        variant_project: &VariantProjectOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut deltas = Vec::new();
        for delta in &input.deltas {
            let value = delta
                .borrowed(&input.descriptor)
                .get_idx(variant_project.field_idx)?;
            let Value::Enum(value) = value else {
                return Err(IvmRuntimeError::VariantProjectFieldTypeMismatch {
                    field: variant_project.field.clone(),
                });
            };
            if value.tag() == variant_project.tag {
                if *value.record().descriptor() != output_desc {
                    return Err(IvmRuntimeError::VariantProjectPayloadMismatch {
                        field: variant_project.field.clone(),
                    });
                }
                deltas.push(RecordDelta {
                    record: Bytes::copy_from_slice(value.record().raw()),
                    weight: delta.weight,
                });
            }
        }
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_unwrap_nullable(
        unwrap: &UnwrapNullableOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let estimated_output_bytes = input
            .deltas
            .iter()
            .map(|delta| delta.record.len())
            .sum::<usize>();
        let mut output = BytesMut::with_capacity(estimated_output_bytes);
        let mut spans = Vec::new();
        let mut scratch = RawProjectionScratch::default();
        for delta in &input.deltas {
            if let Some(span) = output_desc
                .unwrap_nullable_field_into(
                    &input.descriptor,
                    delta.raw(),
                    unwrap.field_idx,
                    &mut output,
                    &mut scratch,
                )
                .map_err(IvmRuntimeError::RecordEncoding)?
            {
                spans.push((span, delta.weight));
            }
        }
        let output = output.freeze();
        let deltas = spans
            .into_iter()
            .map(|(span, weight)| RecordDelta {
                record: output.slice(span),
                weight,
            })
            .collect();
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_unnest(
        unnest: &UnnestOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut deltas = Vec::new();
        for delta in &input.deltas {
            let values = delta
                .borrowed(&input.descriptor)
                .to_values()
                .map_err(IvmRuntimeError::RecordEncoding)?;
            let Some(value) = values.get(unnest.array_field_idx) else {
                return Err(IvmRuntimeError::GraphFieldIndexOutOfBounds(
                    unnest.array_field_idx,
                ));
            };
            let Value::Array(elements) = value else {
                return Err(IvmRuntimeError::UnsupportedOperator);
            };
            for element in elements {
                let mut output_values = values.clone();
                output_values.push(element.clone());
                deltas.push(RecordDelta {
                    record: output_desc.create(&output_values)?.into(),
                    weight: delta.weight,
                });
            }
        }
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_union(
        output_desc: RecordDescriptor,
        inputs: Vec<Arc<RecordDeltas>>,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut deltas = Vec::new();
        for input in inputs {
            if input.deltas.is_empty() {
                continue;
            }
            if !output_desc.registry_compatible_with(&input.descriptor) {
                return Err(IvmRuntimeError::GraphOutputMismatch);
            }
            deltas.extend(input.deltas.iter().cloned());
        }
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_index_by(
        index_by: &IndexByOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let deltas = apply_index_by(index_by, &input.descriptor, &input.deltas)?;
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_persist(
        persist: &PersistOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
        storage: &impl OrderedKvStorage,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let trace = std::env::var_os("GROOVE_TRACE_INDEX_BY").is_some() && !input.deltas.is_empty();
        let start = trace.then(std::time::Instant::now);
        let result = apply_persist_delta(
            storage,
            &persist.storage,
            &persist.key_fields,
            persist.unique,
            input,
        );
        if trace {
            eprintln!(
                "GROOVE_TRACE_PERSIST storage={} input={} unique={} key_fields={:?} elapsed_ms={:.3}",
                String::from_utf8_lossy(&persist.storage.key_prefix).replace('\0', "."),
                input.deltas.len(),
                persist.unique,
                persist.key_fields,
                start.expect("trace start").elapsed().as_secs_f64() * 1000.0
            );
        }
        result?;
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas: input.deltas.clone(),
        })
    }
}

#[derive(Clone, Debug)]
enum OperatorState {
    Stateless,
    Join(JoinState),
    SemiJoin(AntiJoinState),
    AntiJoin(AntiJoinState),
    Recursive(AsOf<RecursiveState, Tick>),
    CollectBy(CollectByIncrementalState),
}

#[derive(Clone, Debug, Default)]
struct CollectByIncrementalState {
    groups: CollectByGroups,
    roots: BTreeMap<CollectByOrderKey, i64>,
}

type CollectByOrderKey = (Vec<TopBySortPart>, Bytes);
type CollectByGroups = BTreeMap<Vec<u8>, BTreeMap<CollectByOrderKey, i64>>;

fn operator_state_for(operator: &OpType) -> OperatorState {
    match operator {
        OpType::Join(_) => OperatorState::Join(JoinState),
        OpType::SemiJoin(_) => OperatorState::SemiJoin(AntiJoinState),
        OpType::AntiJoin(_) => OperatorState::AntiJoin(AntiJoinState),
        OpType::Recursive(_) => OperatorState::Recursive(AsOf::new(RecursiveState::default())),
        OpType::CollectBy(_) => OperatorState::CollectBy(CollectByIncrementalState::default()),
        _ => OperatorState::Stateless,
    }
}

fn plan_expr_names(expressions: &[PlanExpr]) -> Vec<String> {
    expressions
        .iter()
        .filter_map(|expr| match expr {
            PlanExpr::Field(name)
            | PlanExpr::Nullable(name)
            | PlanExpr::NullableFlat(name)
            | PlanExpr::EnumTagRemap { field: name, .. }
            | PlanExpr::EnumRemap { field: name, .. }
            | PlanExpr::RecursiveEnumRemap { field: name, .. } => Some(name.clone()),
            PlanExpr::Literal(_) | PlanExpr::Null(_) => None,
        })
        .collect()
}

fn record_deltas_digest(deltas: &RecordDeltas) -> u64 {
    let mut hasher = DefaultHasher::new();
    deltas.descriptor.hash(&mut hasher);
    for delta in &deltas.deltas {
        delta.weight.hash(&mut hasher);
        delta.record.hash(&mut hasher);
    }
    hasher.finish()
}

fn builder_contains_recursive(graph: &GraphBuilder) -> bool {
    match graph {
        GraphBuilder::Recursive { .. } => true,
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::Unnest { input, .. }
        | GraphBuilder::VariantProject { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. }
        | GraphBuilder::CollectBy { input, .. }
        | GraphBuilder::Aggregate { input, .. } => builder_contains_recursive(input),
        GraphBuilder::Union { inputs } => inputs.iter().any(builder_contains_recursive),
        GraphBuilder::Join { left, right, .. }
        | GraphBuilder::SemiJoin { left, right, .. }
        | GraphBuilder::AntiJoin { left, right, .. } => {
            builder_contains_recursive(left) || builder_contains_recursive(right)
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::BindingSource { .. } => false,
    }
}

fn validate_arg_by_primary_key_indices(
    op_name: &str,
    table: &TableSchema,
    group_fields: &[usize],
    order_fields: &[usize],
    primary_key_fields: &[usize],
) -> Result<(), IvmRuntimeError> {
    let expected = group_fields
        .iter()
        .chain(order_fields.iter())
        .copied()
        .collect::<Vec<_>>();
    if primary_key_fields == expected {
        Ok(())
    } else {
        Err(IvmRuntimeError::UnsupportedArgMaxBy(format!(
            "{op_name} v1 requires primary key for {} to equal group_cols + order_cols",
            table.name
        )))
    }
}

/// Single-tick evaluator over a deduplicated graph.
#[derive(Clone, Debug, Default)]
struct RootOrderingWindows {
    before: BTreeMap<Vec<u8>, usize>,
    after: BTreeMap<Vec<u8>, usize>,
}

struct TickEvaluator<'a, S> {
    schema: &'a DatabaseSchema,
    graph: &'a IvmGraph,
    variant_projections: &'a HashMap<VariantProjectionKey, VariantProjection>,
    table_deltas: &'a [TableDelta],
    binding_deltas: &'a [BindingDelta],
    binding_snapshots: &'a HashMap<String, RecordDeltas>,
    current_tick: u64,
    operator_states: &'a mut HashMap<OperatorStateKey, OperatorState>,
    arrangement_states: &'a mut HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    eval_memo: &'a mut HashMap<EvalMemoKey, EvalMemoEntry>,
    eval_memo_bytes: &'a mut usize,
    table_frontiers: &'a HashMap<String, u64>,
    binding_frontiers: &'a HashMap<String, u64>,
    memo_use_clock: &'a mut u64,
    node_meta: &'a mut HashMap<NodeId, NodeRuntimeMeta>,
    storage: Option<&'a S>,
    context: EvalContext,
    metrics: &'a mut TickMetrics,
    terminal_deltas: HashMap<NodeId, TerminalDeltas>,
    /// Exact pre/post windows captured by TopBy evaluation for terminal
    /// ordering. Kept per node so nested collection ordering cannot be
    /// confused with public root ordering.
    root_ordering_windows: HashMap<NodeId, RootOrderingWindows>,
}

/// Borrowed runtime pieces used by recursive evaluation to run child graphs.
/// This avoids giving recursion ownership of the whole [`IvmRuntime`].
pub(super) struct GraphRuntimeView<'a, S> {
    pub(super) schema: &'a DatabaseSchema,
    pub(super) graph: &'a IvmGraph,
    pub(super) variant_projections: &'a HashMap<VariantProjectionKey, VariantProjection>,
    pub(super) table_deltas: &'a [TableDelta],
    pub(super) binding_deltas: &'a [BindingDelta],
    pub(super) binding_snapshots: &'a HashMap<String, RecordDeltas>,
    pub(super) current_tick: u64,
    operator_states: &'a mut HashMap<OperatorStateKey, OperatorState>,
    arrangement_states: &'a mut HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    eval_memo: &'a mut HashMap<EvalMemoKey, EvalMemoEntry>,
    eval_memo_bytes: &'a mut usize,
    table_frontiers: &'a HashMap<String, u64>,
    binding_frontiers: &'a HashMap<String, u64>,
    memo_use_clock: &'a mut u64,
    node_meta: &'a mut HashMap<NodeId, NodeRuntimeMeta>,
    pub(super) storage: &'a S,
    pub(super) scope: ScopeId,
    pub(super) metrics: &'a mut TickMetrics,
}

#[allow(clippy::too_many_arguments)]
fn graph_runtime_view<'a, S>(
    schema: &'a DatabaseSchema,
    graph: &'a IvmGraph,
    variant_projections: &'a HashMap<VariantProjectionKey, VariantProjection>,
    table_deltas: &'a [TableDelta],
    binding_deltas: &'a [BindingDelta],
    binding_snapshots: &'a HashMap<String, RecordDeltas>,
    current_tick: u64,
    operator_states: &'a mut HashMap<OperatorStateKey, OperatorState>,
    arrangement_states: &'a mut HashMap<ArrangementKey, AsOf<ArrangementState, SubTick>>,
    eval_memo: &'a mut HashMap<EvalMemoKey, EvalMemoEntry>,
    eval_memo_bytes: &'a mut usize,
    table_frontiers: &'a HashMap<String, u64>,
    binding_frontiers: &'a HashMap<String, u64>,
    memo_use_clock: &'a mut u64,
    node_meta: &'a mut HashMap<NodeId, NodeRuntimeMeta>,
    storage: &'a S,
    scope: ScopeId,
    metrics: &'a mut TickMetrics,
) -> GraphRuntimeView<'a, S> {
    GraphRuntimeView {
        schema,
        graph,
        variant_projections,
        table_deltas,
        binding_deltas,
        binding_snapshots,
        current_tick,
        operator_states,
        arrangement_states,
        eval_memo,
        eval_memo_bytes,
        table_frontiers,
        binding_frontiers,
        memo_use_clock,
        node_meta,
        storage,
        scope,
        metrics,
    }
}

impl<'a, S> GraphRuntimeView<'a, S>
where
    S: OrderedKvStorage,
{
    pub(super) fn eval_with_binding(
        &mut self,
        sub_tick: u64,
        binding: FrontierName,
        deltas: RecordDeltas,
        node: NodeId,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut evaluator = TickEvaluator {
            schema: self.schema,
            graph: self.graph,
            variant_projections: self.variant_projections,
            table_deltas: self.table_deltas,
            binding_deltas: self.binding_deltas,
            binding_snapshots: self.binding_snapshots,
            current_tick: self.current_tick,
            operator_states: self.operator_states,
            arrangement_states: self.arrangement_states,
            eval_memo: self.eval_memo,
            eval_memo_bytes: self.eval_memo_bytes,
            table_frontiers: self.table_frontiers,
            binding_frontiers: self.binding_frontiers,
            memo_use_clock: self.memo_use_clock,
            node_meta: self.node_meta,
            storage: Some(self.storage),
            context: EvalContext::with_binding(self.scope, sub_tick, binding, deltas),
            metrics: self.metrics,
            terminal_deltas: HashMap::default(),
            root_ordering_windows: HashMap::default(),
        };
        evaluator
            .update_node(node)
            .map(|records| records.as_ref().clone())
    }

    pub(super) fn eval_with_binding_and_table_deltas(
        &mut self,
        table_deltas: &[TableDelta],
        sub_tick: u64,
        binding: FrontierName,
        deltas: RecordDeltas,
        node: NodeId,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut isolated_memo = HashMap::default();
        let mut isolated_memo_bytes = 0usize;
        let mut evaluator = TickEvaluator {
            schema: self.schema,
            graph: self.graph,
            variant_projections: self.variant_projections,
            table_deltas,
            binding_deltas: self.binding_deltas,
            binding_snapshots: self.binding_snapshots,
            current_tick: self.current_tick,
            operator_states: self.operator_states,
            arrangement_states: self.arrangement_states,
            eval_memo: &mut isolated_memo,
            eval_memo_bytes: &mut isolated_memo_bytes,
            table_frontiers: self.table_frontiers,
            binding_frontiers: self.binding_frontiers,
            memo_use_clock: self.memo_use_clock,
            node_meta: self.node_meta,
            storage: Some(self.storage),
            context: EvalContext::with_binding_and_arrangement_mode(
                self.scope,
                sub_tick,
                binding,
                deltas,
                ArrangementUpdateMode::Replace,
            ),
            metrics: self.metrics,
            terminal_deltas: HashMap::default(),
            root_ordering_windows: HashMap::default(),
        };
        evaluator
            .update_node(node)
            .map(|records| records.as_ref().clone())
    }

    pub(super) fn clear_operator_state_for_scope(&mut self) {
        self.operator_states
            .retain(|key, _| key.scope != self.scope);
    }

    pub(super) fn eval_root(&mut self, node: NodeId) -> Result<RecordDeltas, IvmRuntimeError> {
        let mut evaluator = TickEvaluator {
            schema: self.schema,
            graph: self.graph,
            variant_projections: self.variant_projections,
            table_deltas: self.table_deltas,
            binding_deltas: self.binding_deltas,
            binding_snapshots: self.binding_snapshots,
            current_tick: self.current_tick,
            operator_states: self.operator_states,
            arrangement_states: self.arrangement_states,
            eval_memo: self.eval_memo,
            eval_memo_bytes: self.eval_memo_bytes,
            table_frontiers: self.table_frontiers,
            binding_frontiers: self.binding_frontiers,
            memo_use_clock: self.memo_use_clock,
            node_meta: self.node_meta,
            storage: Some(self.storage),
            context: EvalContext {
                scope: self.scope,
                sub_tick: 0,
                bindings: HashMap::default(),
                binding_digests: HashMap::default(),
                arrangement_update_mode: ArrangementUpdateMode::Accumulate,
                eval_mode: EvalMode::Tick,
                hydrate_arrangements: false,
            },
            metrics: self.metrics,
            terminal_deltas: HashMap::default(),
            root_ordering_windows: HashMap::default(),
        };
        evaluator
            .update_node(node)
            .map(|records| records.as_ref().clone())
    }
}

impl<S> TickEvaluator<'_, S>
where
    S: OrderedKvStorage,
{
    fn apply_root_ordering(
        &self,
        ordering_node: NodeId,
        terminal: &mut TerminalDeltas,
    ) -> Result<(), IvmRuntimeError> {
        let Some(windows) = self.root_ordering_windows.get(&ordering_node) else {
            return Ok(());
        };
        apply_root_ordering_operations(&windows.before, &windows.after, terminal);
        Ok(())
    }

    fn take_terminal_deltas_for_output(
        &mut self,
        node: NodeId,
    ) -> Result<Option<TerminalDeltas>, IvmRuntimeError> {
        let mut pending = vec![node];
        let mut seen = HashSet::new();
        let mut fallback = None;
        let mut has_public_root = false;
        while let Some(node) = pending.pop() {
            if !seen.insert(node) {
                continue;
            }
            let graph_node = self
                .graph
                .node(node)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
            let is_public_root = matches!(
                &graph_node.descriptor.operator,
                OpType::CollectBy(collect_by) if collect_by.mode == CollectByMode::Root
            );
            has_public_root |= is_public_root;
            if self.terminal_deltas.contains_key(&node) {
                if is_public_root {
                    return Ok(self.terminal_deltas.remove(&node));
                }
                fallback.get_or_insert(node);
            }
            pending.extend(graph_node.descriptor.inputs.iter().copied());
        }
        Ok((!has_public_root)
            .then_some(fallback)
            .flatten()
            .and_then(|node| self.terminal_deltas.remove(&node)))
    }

    fn output_is_structured_collect_by(&self, node: NodeId) -> Result<bool, IvmRuntimeError> {
        let mut pending = vec![node];
        let mut seen = HashSet::new();
        while let Some(node) = pending.pop() {
            if !seen.insert(node) {
                continue;
            }
            let node = self
                .graph
                .node(node)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
            match &node.descriptor.operator {
                OpType::CollectBy(collect_by) => {
                    return Ok(matches!(
                        collect_by.mode,
                        CollectByMode::Collect | CollectByMode::Root
                    ));
                }
                _ => pending.extend(node.descriptor.inputs.iter().copied()),
            }
        }
        Ok(false)
    }

    fn output_has_public_root(&self, node: NodeId) -> Result<bool, IvmRuntimeError> {
        let mut pending = vec![node];
        let mut seen = HashSet::new();
        while let Some(node) = pending.pop() {
            if !seen.insert(node) {
                continue;
            }
            let node = self
                .graph
                .node(node)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
            if matches!(
                &node.descriptor.operator,
                OpType::CollectBy(collect_by) if collect_by.mode == CollectByMode::Root
            ) {
                return Ok(true);
            }
            pending.extend(node.descriptor.inputs.iter().copied());
        }
        Ok(false)
    }

    fn node_depends_on_aggregate(&self, node: NodeId) -> Result<bool, IvmRuntimeError> {
        let mut ancestors = HashSet::new();
        self.graph.mark_ancestors(node, &mut ancestors);
        for ancestor in ancestors {
            let graph_node = self
                .graph
                .node(ancestor)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(ancestor))?;
            if matches!(graph_node.descriptor.operator, OpType::Aggregate(_)) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn aggregate_arrangements_are_current(
        &mut self,
        node: NodeId,
    ) -> Result<bool, IvmRuntimeError> {
        self.aggregate_arrangements_are_current_inner(node, &mut HashSet::new())
    }

    fn aggregate_arrangements_are_current_inner(
        &mut self,
        node: NodeId,
        seen: &mut HashSet<NodeId>,
    ) -> Result<bool, IvmRuntimeError> {
        if !seen.insert(node) {
            return Ok(true);
        }
        let graph_node = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
        let operator = graph_node.descriptor.operator.clone();
        let inputs = graph_node.descriptor.inputs.clone();
        if let OpType::Aggregate(aggregate) = operator {
            let [input] = inputs.as_slice() else {
                return Err(IvmRuntimeError::GraphInputArityMismatch(node));
            };
            let input_desc = self
                .graph
                .node(*input)
                .ok_or(IvmRuntimeError::GraphNodeNotFound(*input))?
                .descriptor
                .output;
            let group_fields = self.aggregate_group_fields(node, &aggregate);
            let arrangement_key =
                self.arrangement_key(*input, input_desc, group_fields, ValueComparison::Exact)?;
            if self
                .arrangement_states
                .get(&arrangement_key)
                .and_then(AsOf::as_of)
                != Some(self.arrangement_sub_tick(&arrangement_key))
            {
                return Ok(false);
            }
        }
        for input in inputs {
            if !self.aggregate_arrangements_are_current_inner(input, seen)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn update_node(&mut self, node: NodeId) -> Result<Arc<RecordDeltas>, IvmRuntimeError> {
        let graph_node = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
        let signature = self.input_signature(node)?;
        let memo_key = self.memo_key(node, &signature)?;
        let current_watermark = self.input_generation(node);
        let requires_state_rebuild = (self.context.hydrate_arrangements
            && self.node_depends_on_aggregate(node)?
            && !self.aggregate_arrangements_are_current(node)?)
            || (self.context.eval_mode == EvalMode::Tick
                && self.context.arrangement_update_mode == ArrangementUpdateMode::Replace);
        if !requires_state_rebuild
            && let Some(entry) = self.eval_memo.get_mut(&memo_key)
            && entry.input_watermark == current_watermark
        {
            *self.memo_use_clock += 1;
            entry.last_used = *self.memo_use_clock;
            if self.context.eval_mode == EvalMode::Hydrate {
                self.metrics.hydration_memo_hits += 1;
            }
            return Ok(Arc::clone(&entry.records));
        }

        if self.context.eval_mode == EvalMode::Hydrate {
            self.metrics.hydration_memo_computes += 1;
            self.metrics.hydration_memo_computed_nodes.insert(node);
        }

        let output_desc = graph_node.descriptor.output;
        if self.context.sub_tick > 1 && !self.depends_on_context(node)? {
            let result = Arc::new(RecordDeltas::empty(output_desc));
            *self.memo_use_clock += 1;
            if let Some(previous) = self.eval_memo.insert(
                memo_key,
                EvalMemoEntry::new(
                    Arc::clone(&result),
                    current_watermark,
                    0,
                    *self.memo_use_clock,
                ),
            ) {
                *self.eval_memo_bytes = self.eval_memo_bytes.saturating_sub(previous.payload_bytes);
            }
            return Ok(result);
        }
        let result = match &graph_node.descriptor.operator {
            OpType::TableSource(input) => NodeState::update_table_source(
                input,
                self.schema,
                self.variant_projections,
                &output_desc,
                self.table_deltas,
            ),
            OpType::IndexSource(input) => NodeState::update_index_source(
                input,
                self.schema,
                self.variant_projections,
                &output_desc,
                self.table_deltas,
                self.storage,
                self.context.eval_mode,
            ),
            OpType::InlineRecords(inline) if self.context.eval_mode == EvalMode::Hydrate => {
                Ok(RecordDeltas {
                    descriptor: output_desc,
                    deltas: inline
                        .records
                        .iter()
                        .cloned()
                        .map(|record| RecordDelta {
                            record: record.into(),
                            weight: 1,
                        })
                        .collect(),
                })
            }
            OpType::InlineRecords(_) => Ok(RecordDeltas::empty(output_desc)),
            OpType::BindingSource(input) => NodeState::update_binding_source(
                input,
                &output_desc,
                self.binding_deltas,
                self.binding_snapshots,
                self.context.arrangement_update_mode,
            ),
            OpType::FrontierSource(frontier_source) => {
                self.frontier_source(frontier_source, &graph_node.descriptor.output)
            }
            OpType::Filter(filter) => {
                let input = self.update_unary_input(graph_node, node)?;
                NodeState::update_filter(filter, output_desc, &input)
            }
            OpType::MapProject(project) => {
                let input = self.update_unary_input(graph_node, node)?;
                let raw_projection =
                    self.raw_projection_fields(node, project, &input.descriptor, output_desc)?;
                let result = NodeState::update_map_project(
                    project,
                    output_desc,
                    &input,
                    raw_projection.as_deref(),
                );
                #[cfg(feature = "cold-settle-attribution")]
                if let Ok(output) = &result {
                    crate::cold_settle_attribution::record_map(
                        self.context.eval_mode == EvalMode::Hydrate,
                        self.depends_on_dominant_child(node)?,
                        input.deltas.len(),
                        output.deltas.len(),
                    );
                }
                result
            }
            OpType::UnwrapNullable(unwrap) => {
                let input = self.update_unary_input(graph_node, node)?;
                NodeState::update_unwrap_nullable(unwrap, output_desc, &input)
            }
            OpType::Unnest(unnest) => {
                let input = self.update_unary_input(graph_node, node)?;
                NodeState::update_unnest(unnest, output_desc, &input)
            }
            OpType::VariantProject(variant_project) => {
                let input = self.update_unary_input(graph_node, node)?;
                NodeState::update_variant_project(variant_project, output_desc, &input)
            }
            OpType::ArgMaxBy(arg_max_by) => {
                let input = self.update_unary_input(graph_node, node)?;
                self.update_arg_by(
                    node,
                    ArgBySpec {
                        group_fields: &arg_max_by.group_fields,
                        group_field_indices: &arg_max_by.group_field_indices,
                        primary_key_field_indices: &arg_max_by.primary_key_field_indices,
                        direction: ArgByDirection::Max,
                    },
                    output_desc,
                    &input,
                )
            }
            OpType::ArgMinBy(arg_min_by) => {
                let input = self.update_unary_input(graph_node, node)?;
                self.update_arg_by(
                    node,
                    ArgBySpec {
                        group_fields: &arg_min_by.group_fields,
                        group_field_indices: &arg_min_by.group_field_indices,
                        primary_key_field_indices: &arg_min_by.primary_key_field_indices,
                        direction: ArgByDirection::Min,
                    },
                    output_desc,
                    &input,
                )
            }
            OpType::TopBy(top_by) => {
                let input = self.update_unary_input(graph_node, node)?;
                self.update_top_by(node, top_by, output_desc, &input)
            }
            OpType::CollectBy(collect_by) => {
                let input = self.update_unary_input(graph_node, node)?;
                self.update_collect_by(node, collect_by, output_desc, &input)
            }
            OpType::Aggregate(aggregate) => {
                let input = self.update_unary_input(graph_node, node)?;
                self.update_aggregate(node, aggregate, output_desc, &input)
            }
            OpType::IndexBy(index_by) => {
                let input = self.update_unary_input(graph_node, node)?;
                let trace = std::env::var_os("GROOVE_TRACE_INDEX_BY").is_some();
                let start = trace.then(std::time::Instant::now);
                let input_len = input.deltas.len();
                let result = NodeState::update_index_by(index_by, output_desc, &input);
                if trace && input_len > 0 {
                    let output_len = result
                        .as_ref()
                        .map(|records| records.deltas.len())
                        .unwrap_or(0);
                    let index_name = index_by
                        .explicit_index
                        .as_ref()
                        .map(|index| index.name.as_str())
                        .unwrap_or("<derived>");
                    let key_fields = index_by
                        .key_expressions
                        .iter()
                        .map(|expr| format!("{expr:?}"))
                        .collect::<Vec<_>>()
                        .join(",");
                    eprintln!(
                        "GROOVE_TRACE_INDEX_BY node={node:?} index={index_name} input={input_len} output={output_len} unique={} append_value_to_key={} store_value={} scan={} key_fields=[{}] elapsed_ms={:.3}",
                        index_by.unique,
                        index_by.append_value_to_key,
                        index_by.store_value,
                        index_by.scan.is_some(),
                        key_fields,
                        start.expect("trace start").elapsed().as_secs_f64() * 1000.0
                    );
                }
                result
            }
            OpType::Union => {
                let inputs = graph_node
                    .descriptor
                    .inputs
                    .iter()
                    .map(|input| self.update_node(*input))
                    .collect::<Result<Vec<_>, _>>()?;
                NodeState::update_union(output_desc, inputs)
            }
            OpType::Join(join) => {
                let [left_input, right_input] = graph_node.descriptor.inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                let left = self.update_node(*left_input)?;
                let right = self.update_node(*right_input)?;
                self.update_join(
                    node,
                    join,
                    output_desc,
                    *left_input,
                    *right_input,
                    &left.deltas,
                    &right.deltas,
                )
            }
            OpType::SemiJoin(join) => {
                let [left_input, right_input] = graph_node.descriptor.inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                let left = self.update_node(*left_input)?;
                let right = self.update_node(*right_input)?;
                self.update_semi_join(
                    node,
                    join,
                    output_desc,
                    *left_input,
                    *right_input,
                    &left.deltas,
                    &right.deltas,
                )
            }
            OpType::AntiJoin(join) => {
                let [left_input, right_input] = graph_node.descriptor.inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                let left = self.update_node(*left_input)?;
                let right = self.update_node(*right_input)?;
                self.update_anti_join(
                    node,
                    join,
                    output_desc,
                    *left_input,
                    *right_input,
                    &left.deltas,
                    &right.deltas,
                )
            }
            OpType::Recursive(recursive) => {
                let [seed, step] = graph_node.descriptor.inputs.as_slice() else {
                    return Err(IvmRuntimeError::GraphInputArityMismatch(node));
                };
                self.update_recursive(node, recursive, output_desc, *seed, *step)
            }
            OpType::Persist(persist) => {
                let storage = self.storage.ok_or(IvmRuntimeError::StorageUnavailable)?;
                let input = self.update_unary_input(graph_node, node)?;
                NodeState::update_persist(persist, output_desc, &input, storage)
            }
            _ => Err(IvmRuntimeError::UnsupportedOperator),
        }?;
        self.metrics.records_processed += result.deltas.len();
        let result = Arc::new(result);
        let payload_bytes = record_deltas_encoded_bytes(&result);
        *self.memo_use_clock += 1;
        if let Some(previous) = self.eval_memo.insert(
            memo_key,
            EvalMemoEntry::new(
                Arc::clone(&result),
                current_watermark,
                payload_bytes,
                *self.memo_use_clock,
            ),
        ) {
            *self.eval_memo_bytes = self.eval_memo_bytes.saturating_sub(previous.payload_bytes);
        }
        *self.eval_memo_bytes = self.eval_memo_bytes.saturating_add(payload_bytes);
        Ok(result)
    }

    fn memo_key(
        &mut self,
        node: NodeId,
        signature: &NodeInputSignature,
    ) -> Result<EvalMemoKey, IvmRuntimeError> {
        Ok(EvalMemoKey {
            scope: if self.context.scope == ScopeId::root() {
                self.operator_scope(node)?
            } else {
                self.context.scope
            },
            node,
            input_signature_hash: signature.hash,
            tick_epoch: match self.context.eval_mode {
                EvalMode::Tick => Some(self.current_tick),
                EvalMode::Hydrate => None,
            },
            sub_tick: self.context.sub_tick,
            context_digest: self.context_digest(signature),
        })
    }

    fn input_generation(&self, node: NodeId) -> u64 {
        self.node_meta
            .get(&node)
            .map(|meta| meta.input_generation)
            .unwrap_or_default()
    }

    fn context_digest(&self, signature: &NodeInputSignature) -> u64 {
        if signature.frontier_bindings.is_empty() {
            return 0;
        }
        let mut hasher = DefaultHasher::new();
        for binding in signature.frontier_bindings.iter() {
            binding.hash(&mut hasher);
            self.context
                .binding_digests
                .get(binding)
                .copied()
                .unwrap_or_default()
                .hash(&mut hasher);
        }
        hasher.finish()
    }

    fn operator_key(&mut self, node: NodeId) -> Result<OperatorStateKey, IvmRuntimeError> {
        Ok(OperatorStateKey {
            scope: self.operator_scope(node)?,
            node,
        })
    }

    fn operator_scope(&mut self, node: NodeId) -> Result<ScopeId, IvmRuntimeError> {
        // Recursive step evaluation must be isolated per recursive node even
        // for context-independent table/index inputs. Sibling recursive nodes
        // can evaluate the same base-table delta in one outer tick; sharing
        // root-scoped child operator state would let the first sibling advance
        // the table side and make later siblings miss the same positive edge.
        // Scoped child operator state is tick-local and is cleared before the
        // public tick exits.
        if self.context.scope != ScopeId::root() {
            return Ok(self.context.scope);
        }
        // Only fragments downstream of FrontierSource are scoped. Base table
        // arrangements stay global and can be reused by unrelated queries.
        if self.depends_on_context(node)? {
            Ok(self.context.scope)
        } else {
            Ok(ScopeId::root())
        }
    }

    fn depends_on_context(&mut self, node: NodeId) -> Result<bool, IvmRuntimeError> {
        Ok(!self.input_signature(node)?.frontier_bindings.is_empty())
    }

    fn input_signature(
        &mut self,
        node: NodeId,
    ) -> Result<Arc<NodeInputSignature>, IvmRuntimeError> {
        self.input_signature_inner(node, &mut HashSet::new())
    }

    fn input_signature_inner(
        &mut self,
        node: NodeId,
        seen: &mut HashSet<NodeId>,
    ) -> Result<Arc<NodeInputSignature>, IvmRuntimeError> {
        if let Some(signature) = self
            .node_meta
            .get(&node)
            .and_then(|meta| meta.input_signature.clone())
        {
            return Ok(signature);
        }
        if !seen.insert(node) {
            return Ok(Arc::new(NodeInputSignature::default()));
        }
        let graph_node = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
        let operator = graph_node.descriptor.operator.clone();
        let inputs = graph_node.descriptor.inputs.clone();
        let mut tables = BTreeSet::new();
        let mut bindings = BTreeSet::new();
        let mut frontier_bindings = BTreeSet::new();
        match operator {
            OpType::TableSource(input) => {
                tables.insert(input.table);
            }
            OpType::IndexSource(input) => {
                tables.insert(input.table);
            }
            OpType::BindingSource(input) => {
                bindings.insert(input.shape);
            }
            OpType::FrontierSource(input) => {
                frontier_bindings.insert(input.binding);
            }
            _ => {}
        };
        for input in inputs {
            let child = self.input_signature_inner(input, seen)?;
            tables.extend(child.tables.iter().cloned());
            bindings.extend(child.bindings.iter().cloned());
            frontier_bindings.extend(child.frontier_bindings.iter().cloned());
        }
        let signature = Arc::new(NodeInputSignature::from_sets(
            tables,
            bindings,
            frontier_bindings,
        ));
        let depends_on_context = !signature.frontier_bindings.is_empty();
        let meta = self.node_meta.entry(node).or_default();
        meta.depends_on_context = Some(depends_on_context);
        meta.input_signature = Some(Arc::clone(&signature));
        Ok(signature)
    }

    fn raw_projection_fields(
        &mut self,
        node: NodeId,
        project: &MapProjectOp,
        input_desc: &RecordDescriptor,
        output_desc: RecordDescriptor,
    ) -> Result<Option<Arc<[RawProjectionField]>>, IvmRuntimeError> {
        if let Some(cached) = self
            .node_meta
            .get(&node)
            .and_then(|meta| meta.raw_projection_fields.clone())
        {
            return Ok(cached);
        }

        let resolved = raw_projection_fields(project, input_desc, output_desc)?.map(Arc::from);
        self.node_meta
            .entry(node)
            .or_default()
            .raw_projection_fields = Some(resolved.clone());
        Ok(resolved)
    }

    fn frontier_source(
        &self,
        frontier_source: &FrontierSourceOp,
        output: &RecordDescriptor,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let deltas = self
            .context
            .bindings
            .get(&frontier_source.binding)
            .cloned()
            .unwrap_or_else(|| RecordDeltas::empty(*output));
        if !deltas.descriptor.registry_compatible_with(output) {
            return Err(IvmRuntimeError::GraphOutputMismatch);
        }
        Ok(deltas)
    }

    #[cfg(feature = "cold-settle-attribution")]
    fn depends_on_dominant_child(&self, node: NodeId) -> Result<bool, IvmRuntimeError> {
        let graph_node = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?;
        if matches!(
            &graph_node.descriptor.operator,
            OpType::TableSource(source) if source.table == "res_l_child_3"
        ) {
            return Ok(true);
        }
        // Policy lowering can replace the direct table source with an indexed
        // source. The anonymous child shape is unique in this benchmark, so
        // retain the tag through that lowering as well.
        if ["parent_id", "value_text", "value_json"]
            .into_iter()
            .all(|field| {
                graph_node
                    .descriptor
                    .output
                    .fields()
                    .iter()
                    .any(|candidate| candidate.name.as_deref() == Some(field))
            })
        {
            return Ok(true);
        }
        graph_node
            .descriptor
            .inputs
            .iter()
            .copied()
            .map(|input| self.depends_on_dominant_child(input))
            .collect::<Result<Vec<_>, _>>()
            .map(|dependencies| dependencies.into_iter().any(|dependency| dependency))
    }

    #[allow(clippy::too_many_arguments)]
    fn update_join(
        &mut self,
        node: NodeId,
        join: &JoinOp,
        output_desc: RecordDescriptor,
        left_input: NodeId,
        right_input: NodeId,
        left_delta: &[RecordDelta],
        right_delta: &[RecordDelta],
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let operator_key = self.operator_key(node)?;
        let operator = self
            .operator_states
            .entry(operator_key)
            .or_insert_with(|| operator_state_for(&OpType::Join(join.clone())));
        let OperatorState::Join(join_state) = operator else {
            return Err(IvmRuntimeError::NodeStateOperatorMismatch(node));
        };
        let join_state = join_state.clone();
        let (left_on, right_on) = self.join_field_names(node, join);
        let output_mapping = self.join_output_mapping(
            node,
            join.left_descriptor,
            join.right_descriptor,
            output_desc,
        )?;
        let left_key =
            self.arrangement_key(left_input, join.left_descriptor, left_on, join.comparison)?;
        let right_key = self.arrangement_key(
            right_input,
            join.right_descriptor,
            right_on,
            join.comparison,
        )?;
        let mut left_arrangement = self
            .arrangement_states
            .remove(&left_key)
            .unwrap_or_default();
        // Pull arrangements out while applying so both sides can be mutated
        // without aliasing the arrangement map.
        let shared_arrangement_keys = if left_key == right_key {
            let mut keys = touched_join_keys(
                &join.left_descriptor,
                &left_key.fields,
                left_delta,
                join.comparison,
            )?;
            keys.extend(touched_join_keys(
                &join.right_descriptor,
                &right_key.fields,
                right_delta,
                join.comparison,
            )?);
            // `replace_keys` consumes each replacement bucket. A key can be
            // present in both sides' deltas, so pass every touched key once.
            keys.sort_unstable();
            keys.dedup();
            Some(keys)
        } else {
            None
        };
        let mut right_arrangement = if let Some(keys) = &shared_arrangement_keys {
            AsOf {
                value: left_arrangement.value().clone_keys(keys.iter()),
                as_of: left_arrangement.as_of(),
            }
        } else {
            self.arrangement_states
                .remove(&right_key)
                .unwrap_or_default()
        };
        let deltas = join_state.apply(
            &mut left_arrangement,
            &mut right_arrangement,
            &join.left_descriptor,
            &join.right_descriptor,
            &output_desc,
            &output_mapping,
            &left_key.fields,
            &right_key.fields,
            join.comparison,
            left_delta,
            right_delta,
            self.arrangement_sub_tick(&left_key),
            self.arrangement_sub_tick(&right_key),
            self.context.arrangement_update_mode,
        )?;
        if let Some(keys) = shared_arrangement_keys {
            left_arrangement
                .value_mut()
                .replace_keys(keys.iter(), right_arrangement.value().clone());
        } else {
            self.arrangement_states.insert(right_key, right_arrangement);
        }
        self.arrangement_states.insert(left_key, left_arrangement);
        #[cfg(feature = "cold-settle-attribution")]
        crate::cold_settle_attribution::record_join(
            self.context.eval_mode == EvalMode::Hydrate,
            self.depends_on_dominant_child(node)?,
            left_delta.len(),
            right_delta.len(),
            deltas.len(),
        );
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn update_anti_join(
        &mut self,
        node: NodeId,
        join: &JoinOp,
        output_desc: RecordDescriptor,
        left_input: NodeId,
        right_input: NodeId,
        left_delta: &[RecordDelta],
        right_delta: &[RecordDelta],
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let operator_key = self.operator_key(node)?;
        let operator = self
            .operator_states
            .entry(operator_key)
            .or_insert_with(|| operator_state_for(&OpType::AntiJoin(join.clone())));
        let OperatorState::AntiJoin(join_state) = operator else {
            return Err(IvmRuntimeError::NodeStateOperatorMismatch(node));
        };
        let join_state = join_state.clone();
        let (left_on, right_on) = self.join_field_names(node, join);
        let left_key =
            self.arrangement_key(left_input, join.left_descriptor, left_on, join.comparison)?;
        let right_key = self.arrangement_key(
            right_input,
            join.right_descriptor,
            right_on,
            join.comparison,
        )?;
        let mut left_arrangement = self
            .arrangement_states
            .remove(&left_key)
            .unwrap_or_default();
        let mut right_arrangement = if left_key == right_key {
            left_arrangement.clone()
        } else {
            self.arrangement_states
                .remove(&right_key)
                .unwrap_or_default()
        };
        let deltas = join_state.apply(
            &mut left_arrangement,
            &mut right_arrangement,
            &join.left_descriptor,
            &join.right_descriptor,
            &output_desc,
            &left_key.fields,
            &right_key.fields,
            join.comparison,
            left_delta,
            right_delta,
            self.arrangement_sub_tick(&left_key),
            self.arrangement_sub_tick(&right_key),
            self.context.arrangement_update_mode,
        )?;
        if left_key == right_key {
            left_arrangement = right_arrangement;
        } else {
            self.arrangement_states.insert(right_key, right_arrangement);
        }
        self.arrangement_states.insert(left_key, left_arrangement);
        #[cfg(feature = "cold-settle-attribution")]
        crate::cold_settle_attribution::record_join(
            self.context.eval_mode == EvalMode::Hydrate,
            self.depends_on_dominant_child(node)?,
            left_delta.len(),
            right_delta.len(),
            deltas.len(),
        );
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn update_semi_join(
        &mut self,
        node: NodeId,
        join: &JoinOp,
        output_desc: RecordDescriptor,
        left_input: NodeId,
        right_input: NodeId,
        left_delta: &[RecordDelta],
        right_delta: &[RecordDelta],
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let operator_key = self.operator_key(node)?;
        let operator = self
            .operator_states
            .entry(operator_key)
            .or_insert_with(|| operator_state_for(&OpType::SemiJoin(join.clone())));
        let OperatorState::SemiJoin(join_state) = operator else {
            return Err(IvmRuntimeError::NodeStateOperatorMismatch(node));
        };
        let join_state = join_state.clone();
        let (left_on, right_on) = self.join_field_names(node, join);
        let left_key =
            self.arrangement_key(left_input, join.left_descriptor, left_on, join.comparison)?;
        let right_key = self.arrangement_key(
            right_input,
            join.right_descriptor,
            right_on,
            join.comparison,
        )?;
        let mut left_arrangement = self
            .arrangement_states
            .remove(&left_key)
            .unwrap_or_default();
        let mut right_arrangement = if left_key == right_key {
            left_arrangement.clone()
        } else {
            self.arrangement_states
                .remove(&right_key)
                .unwrap_or_default()
        };
        let deltas = join_state.apply_semi(
            &mut left_arrangement,
            &mut right_arrangement,
            join.left_descriptor,
            join.right_descriptor,
            &output_desc,
            &left_key.fields,
            &right_key.fields,
            join.comparison,
            left_delta,
            right_delta,
            self.arrangement_sub_tick(&left_key),
            self.arrangement_sub_tick(&right_key),
            self.context.arrangement_update_mode,
        )?;
        if left_key == right_key {
            left_arrangement = right_arrangement;
        } else {
            self.arrangement_states.insert(right_key, right_arrangement);
        }
        self.arrangement_states.insert(left_key, left_arrangement);
        #[cfg(feature = "cold-settle-attribution")]
        crate::cold_settle_attribution::record_join(
            self.context.eval_mode == EvalMode::Hydrate,
            self.depends_on_dominant_child(node)?,
            left_delta.len(),
            right_delta.len(),
            deltas.len(),
        );
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_arg_by(
        &mut self,
        node: NodeId,
        spec: ArgBySpec<'_>,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        if input.deltas.is_empty() {
            return Ok(RecordDeltas::empty(output_desc));
        }
        let [input_node] = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?
            .descriptor
            .inputs
            .as_slice()
        else {
            return Err(IvmRuntimeError::GraphInputArityMismatch(node));
        };
        let arrangement_key = self.arrangement_key(
            *input_node,
            output_desc,
            Arc::from(spec.group_fields.to_vec()),
            ValueComparison::Exact,
        )?;
        let sub_tick = self.arrangement_sub_tick(&arrangement_key);
        let mut arrangement = self
            .arrangement_states
            .remove(&arrangement_key)
            .unwrap_or_default();
        let should_apply_arrangement = self.context.arrangement_update_mode
            == ArrangementUpdateMode::Replace
            || arrangement.as_of() != Some(sub_tick);
        if should_apply_arrangement {
            let replace_within_same_tick = self.context.arrangement_update_mode
                == ArrangementUpdateMode::Replace
                && arrangement
                    .as_of()
                    .is_some_and(|current| current.tick == sub_tick.tick);
            if !replace_within_same_tick
                && arrangement
                    .as_of()
                    .is_some_and(|current| current > sub_tick)
            {
                return Err(IvmRuntimeError::OutOfOrderRuntimeState {
                    current: format!("{:?}", arrangement.as_of().expect("checked above")),
                    next: format!("{sub_tick:?}"),
                });
            }
            arrangement.value_mut().apply_record_deltas(
                output_desc,
                spec.group_fields,
                &input.deltas,
                self.context.arrangement_update_mode,
            )?;
            if replace_within_same_tick {
                arrangement.replace_as_of_at_least(sub_tick);
            } else {
                arrangement.mark_forward_as_of(sub_tick)?;
            }
        }
        let mut touched_groups = BTreeMap::<Vec<u8>, Vec<RecordDelta>>::new();
        for delta in &input.deltas {
            let group_key =
                encoded_arrangement_key_part(output_desc, delta.raw(), spec.group_field_indices)?;
            touched_groups
                .entry(group_key)
                .or_default()
                .push(delta.clone());
        }

        let mut output = Vec::new();
        for (group_prefix, group_deltas) in touched_groups {
            let after_records = arrangement.value().records_for_key(&group_prefix);
            let after = arg_by_winner_from_records(
                output_desc,
                spec.primary_key_field_indices,
                after_records.clone(),
                spec.direction,
            )?;
            let before = arg_by_winner_before_from_deltas(
                output_desc,
                spec.primary_key_field_indices,
                after_records,
                group_deltas,
                spec.direction,
            )?;
            if before == after {
                continue;
            }
            if let Some((_, record)) = before {
                output.push(RecordDelta { record, weight: -1 });
            }
            if let Some((_, record)) = after {
                output.push(RecordDelta { record, weight: 1 });
            }
        }
        self.arrangement_states.insert(arrangement_key, arrangement);

        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas: output,
        })
    }

    fn update_top_by(
        &mut self,
        node: NodeId,
        top_by: &TopByOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        if input.deltas.is_empty() || top_by.limit == TopByLimit::Finite(0) {
            return Ok(RecordDeltas::empty(output_desc));
        }
        let [input_node] = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?
            .descriptor
            .inputs
            .as_slice()
        else {
            return Err(IvmRuntimeError::GraphInputArityMismatch(node));
        };
        let arrangement_key = self.arrangement_key(
            *input_node,
            output_desc,
            Arc::from(top_by.group_fields.clone()),
            ValueComparison::Exact,
        )?;
        let sub_tick = self.arrangement_sub_tick(&arrangement_key);
        let mut arrangement = self
            .arrangement_states
            .remove(&arrangement_key)
            .unwrap_or_default();
        let should_apply_arrangement = self.context.arrangement_update_mode
            == ArrangementUpdateMode::Replace
            || arrangement.as_of() != Some(sub_tick);
        if should_apply_arrangement {
            let replace_within_same_tick = self.context.arrangement_update_mode
                == ArrangementUpdateMode::Replace
                && arrangement
                    .as_of()
                    .is_some_and(|current| current.tick == sub_tick.tick);
            if !replace_within_same_tick
                && arrangement
                    .as_of()
                    .is_some_and(|current| current > sub_tick)
            {
                return Err(IvmRuntimeError::OutOfOrderRuntimeState {
                    current: format!("{:?}", arrangement.as_of().expect("checked above")),
                    next: format!("{sub_tick:?}"),
                });
            }
            arrangement.value_mut().apply_record_deltas(
                output_desc,
                &top_by.group_fields,
                &input.deltas,
                self.context.arrangement_update_mode,
            )?;
            if replace_within_same_tick {
                arrangement.replace_as_of_at_least(sub_tick);
            } else {
                arrangement.mark_forward_as_of(sub_tick)?;
            }
        }

        let mut touched_groups = BTreeMap::<Vec<u8>, Vec<RecordDelta>>::new();
        for delta in &input.deltas {
            let group_key =
                encoded_record_key_part(output_desc, delta.raw(), &top_by.group_field_indices)?;
            touched_groups
                .entry(group_key)
                .or_default()
                .push(delta.clone());
        }

        let mut output = Vec::new();
        for (group_prefix, group_deltas) in touched_groups {
            let after_records = arrangement.value().records_for_key(&group_prefix);
            let after = top_by_window_from_records(output_desc, after_records.clone(), top_by)?;
            let before =
                top_by_window_before_from_deltas(output_desc, after_records, group_deltas, top_by)?;
            let windows = self.root_ordering_windows.entry(node).or_default();
            extend_root_window_positions(output_desc, &before, &mut windows.before)?;
            extend_root_window_positions(output_desc, &after, &mut windows.after)?;
            output.extend(diff_record_windows(before, after));
        }
        self.arrangement_states.insert(arrangement_key, arrangement);

        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas: output,
        })
    }

    /// Render touched flat groups as complete parents. This intentionally uses
    /// a root-scope arrangement keyed by the collector input; a collector is
    /// structurally terminal, so it can never become state in a recursive step
    /// or inherit a recursive sub-tick work bound.
    fn update_collect_by(
        &mut self,
        node: NodeId,
        collect_by: &CollectByOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        if input.deltas.is_empty() || collect_by.limit == TopByLimit::Finite(0) {
            return Ok(RecordDeltas::empty(output_desc));
        }
        let direct_tree_slot = match collect_by.slots.as_slice() {
            [] if collect_by.limit == TopByLimit::Unbounded => None,
            [slot] if slot.slots.is_empty() && slot.limit == TopByLimit::Unbounded => Some(slot),
            _ => None,
        };
        if collect_by.mode == CollectByMode::Root
            || (collect_by.mode == CollectByMode::Collect
                && (collect_by.slots.is_empty() || direct_tree_slot.is_some())
                && (collect_by.limit == TopByLimit::Unbounded || direct_tree_slot.is_some()))
        {
            let operator_key = self.operator_key(node)?;
            let mut operator = self
                .operator_states
                .remove(&operator_key)
                .unwrap_or_else(|| OperatorState::CollectBy(CollectByIncrementalState::default()));
            let OperatorState::CollectBy(state) = &mut operator else {
                return Err(IvmRuntimeError::NodeStateOperatorMismatch(node));
            };
            let operations = update_unbounded_collect_by_terminal_state(
                input.descriptor,
                output_desc,
                collect_by,
                direct_tree_slot,
                state,
                &input.deltas,
                self.context.eval_mode == EvalMode::Tick,
            )?;
            self.operator_states.insert(operator_key, operator);
            if self.context.eval_mode == EvalMode::Tick {
                if !operations.is_empty() {
                    self.terminal_deltas
                        .insert(node, TerminalDeltas { operations });
                }
                return Ok(RecordDeltas::empty(output_desc));
            }
        }
        let [input_node] = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?
            .descriptor
            .inputs
            .as_slice()
        else {
            return Err(IvmRuntimeError::GraphInputArityMismatch(node));
        };
        let input_desc = input.descriptor;
        let arrangement_key = self.arrangement_key(
            *input_node,
            input_desc,
            Arc::from(collect_by.group_fields.clone()),
            ValueComparison::Exact,
        )?;
        // Structural validation permits only terminal filter/projection
        // adapters above CollectBy, so a non-root evaluation scope here is a
        // routed terminal adapter, never recursive relational state.
        let sub_tick = self.arrangement_sub_tick(&arrangement_key);
        let mut arrangement = self
            .arrangement_states
            .remove(&arrangement_key)
            .unwrap_or_default();
        let should_apply_arrangement = self.context.arrangement_update_mode
            == ArrangementUpdateMode::Replace
            || arrangement.as_of() != Some(sub_tick);
        if should_apply_arrangement {
            let replace_within_same_tick = self.context.arrangement_update_mode
                == ArrangementUpdateMode::Replace
                && arrangement
                    .as_of()
                    .is_some_and(|current| current.tick == sub_tick.tick);
            if !replace_within_same_tick
                && arrangement
                    .as_of()
                    .is_some_and(|current| current > sub_tick)
            {
                return Err(IvmRuntimeError::OutOfOrderRuntimeState {
                    current: format!("{:?}", arrangement.as_of().expect("checked above")),
                    next: format!("{sub_tick:?}"),
                });
            }
            arrangement.value_mut().apply_record_deltas(
                input_desc,
                &collect_by.group_fields,
                &input.deltas,
                self.context.arrangement_update_mode,
            )?;
            if replace_within_same_tick {
                arrangement.replace_as_of_at_least(sub_tick);
            } else {
                arrangement.mark_forward_as_of(sub_tick)?;
            }
        }

        let mut touched_groups = BTreeMap::<Vec<u8>, Vec<RecordDelta>>::new();
        for delta in &input.deltas {
            let group_key =
                encoded_record_key_part(input_desc, delta.raw(), &collect_by.group_field_indices)?;
            touched_groups
                .entry(group_key)
                .or_default()
                .push(delta.clone());
        }

        let mut output = Vec::new();
        for (group_prefix, group_deltas) in touched_groups {
            let after_records = arrangement.value().records_for_key(&group_prefix);
            let before_records = records_before_deltas(after_records.clone(), &group_deltas);
            match collect_by.mode {
                CollectByMode::Collect | CollectByMode::Root => {
                    let render = |records: &[(Bytes, i64)]| {
                        if collect_by.mode == CollectByMode::Root {
                            collect_by_root_from_records(
                                input_desc,
                                output_desc,
                                collect_by,
                                records,
                            )
                        } else if collect_by.slots.is_empty() {
                            collect_by_parent_from_records(
                                input_desc,
                                output_desc,
                                collect_by,
                                records,
                            )
                        } else {
                            collect_by_tree_parent_from_records(
                                input_desc,
                                output_desc,
                                collect_by,
                                records,
                            )
                        }
                    };
                    let before = render(&before_records)?;
                    let after = render(&after_records)?;
                    if before == after {
                        continue;
                    }
                    if let Some(record) = before {
                        output.push(RecordDelta { record, weight: -1 });
                    }
                    if let Some(record) = after {
                        output.push(RecordDelta { record, weight: 1 });
                    }
                }
                CollectByMode::Expand => {
                    let before = collect_by_expanded_window(
                        input_desc,
                        output_desc,
                        collect_by,
                        &before_records,
                    )?;
                    let after = collect_by_expanded_window(
                        input_desc,
                        output_desc,
                        collect_by,
                        &after_records,
                    )?;
                    let mut occurrences = BTreeSet::new();
                    occurrences.extend(before.keys().cloned());
                    occurrences.extend(after.keys().cloned());
                    for occurrence in occurrences {
                        match (before.get(&occurrence), after.get(&occurrence)) {
                            (Some(before), Some(after)) if before == after => {}
                            (Some(before), Some(after)) => {
                                output.push(RecordDelta {
                                    record: before.clone(),
                                    weight: -1,
                                });
                                output.push(RecordDelta {
                                    record: after.clone(),
                                    weight: 1,
                                });
                            }
                            (Some(before), None) => output.push(RecordDelta {
                                record: before.clone(),
                                weight: -1,
                            }),
                            (None, Some(after)) => output.push(RecordDelta {
                                record: after.clone(),
                                weight: 1,
                            }),
                            (None, None) => unreachable!("occurrence came from a selected window"),
                        }
                    }
                }
            }
        }
        self.arrangement_states.insert(arrangement_key, arrangement);
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas: output,
        })
    }

    fn update_aggregate(
        &mut self,
        node: NodeId,
        aggregate: &AggregateOp,
        output_desc: RecordDescriptor,
        input: &RecordDeltas,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        if input.deltas.is_empty() {
            if self.context.eval_mode == EvalMode::Hydrate && aggregate.group_key.is_empty() {
                let record =
                    aggregate_row_from_records(input.descriptor, output_desc, aggregate, &[])?
                        .ok_or(IvmRuntimeError::UnsupportedOperator)?;
                return Ok(RecordDeltas {
                    descriptor: output_desc,
                    deltas: vec![RecordDelta { record, weight: 1 }],
                });
            }
            return Ok(RecordDeltas::empty(output_desc));
        }
        let [input_node] = self
            .graph
            .node(node)
            .ok_or(IvmRuntimeError::GraphNodeNotFound(node))?
            .descriptor
            .inputs
            .as_slice()
        else {
            return Err(IvmRuntimeError::GraphInputArityMismatch(node));
        };
        let input_desc = input.descriptor;
        let group_fields = self.aggregate_group_fields(node, aggregate);
        if self.context.eval_mode == EvalMode::Hydrate {
            let mut groups = BTreeMap::<Vec<u8>, Vec<(Bytes, i64)>>::new();
            for delta in &input.deltas {
                let group_key = encoded_record_key_part(
                    input_desc,
                    delta.raw(),
                    &aggregate.group_field_indices,
                )?;
                groups
                    .entry(group_key)
                    .or_default()
                    .push((delta.record.clone(), delta.weight));
            }
            if self.context.hydrate_arrangements {
                let arrangement_key = self.arrangement_key(
                    *input_node,
                    input_desc,
                    group_fields.clone(),
                    ValueComparison::Exact,
                )?;
                let mut arrangement = AsOf::<ArrangementState, SubTick>::default();
                arrangement.value_mut().apply_record_deltas(
                    input_desc,
                    group_fields.as_ref(),
                    &input.deltas,
                    ArrangementUpdateMode::Replace,
                )?;
                arrangement.mark_forward_as_of(self.arrangement_sub_tick(&arrangement_key))?;
                self.arrangement_states.insert(arrangement_key, arrangement);
            }
            let mut output = Vec::new();
            for records in groups.values() {
                if let Some(record) =
                    aggregate_row_from_records(input_desc, output_desc, aggregate, records)?
                {
                    output.push(RecordDelta { record, weight: 1 });
                }
            }
            return Ok(RecordDeltas {
                descriptor: output_desc,
                deltas: output,
            });
        }
        let arrangement_key = self.arrangement_key(
            *input_node,
            input_desc,
            group_fields.clone(),
            ValueComparison::Exact,
        )?;
        let sub_tick = self.arrangement_sub_tick(&arrangement_key);
        let mut touched_groups = BTreeMap::<Vec<u8>, Vec<RecordDelta>>::new();
        for delta in &input.deltas {
            let group_key =
                encoded_record_key_part(input_desc, delta.raw(), &aggregate.group_field_indices)?;
            touched_groups
                .entry(group_key)
                .or_default()
                .push(delta.clone());
        }
        let current_arrangement = self.arrangement_states.get(&arrangement_key);
        let current_as_of = current_arrangement.and_then(AsOf::as_of);
        let should_apply_arrangement = self.context.arrangement_update_mode
            == ArrangementUpdateMode::Replace
            || current_as_of != Some(sub_tick);
        let replace_within_same_tick = self.context.arrangement_update_mode
            == ArrangementUpdateMode::Replace
            && current_as_of.is_some_and(|current| current.tick == sub_tick.tick);
        if !replace_within_same_tick && current_as_of.is_some_and(|current| current > sub_tick) {
            return Err(IvmRuntimeError::OutOfOrderRuntimeState {
                current: format!("{:?}", current_as_of.expect("checked above")),
                next: format!("{sub_tick:?}"),
            });
        }
        let before_groups = if self.context.arrangement_update_mode
            == ArrangementUpdateMode::Replace
            || !should_apply_arrangement
        {
            BTreeMap::new()
        } else {
            touched_groups
                .keys()
                .map(|group| {
                    (
                        group.clone(),
                        current_arrangement
                            .map(|arrangement| arrangement.value().records_for_key(group))
                            .unwrap_or_default(),
                    )
                })
                .collect::<BTreeMap<_, _>>()
        };
        let mut staged_arrangement =
            if self.context.arrangement_update_mode == ArrangementUpdateMode::Replace {
                ArrangementState::default()
            } else {
                current_arrangement
                    .map(|arrangement| arrangement.value().clone_keys(touched_groups.keys()))
                    .unwrap_or_default()
            };
        if should_apply_arrangement {
            staged_arrangement.apply_record_deltas(
                input_desc,
                group_fields.as_ref(),
                &input.deltas,
                self.context.arrangement_update_mode,
            )?;
        }

        let mut output = Vec::new();
        for group_prefix in touched_groups.keys() {
            let after_records = staged_arrangement.records_for_key(group_prefix);
            let after =
                aggregate_row_from_records(input_desc, output_desc, aggregate, &after_records)?;
            let before_records = if let Some(records) = before_groups
                .get(group_prefix)
                .filter(|records| !records.is_empty())
            {
                records.clone()
            } else {
                records_before_from_deltas(
                    after_records,
                    touched_groups
                        .get(group_prefix)
                        .cloned()
                        .unwrap_or_default(),
                )
            };
            let before =
                aggregate_row_from_records(input_desc, output_desc, aggregate, &before_records)?;
            if before == after {
                continue;
            }
            if let Some(record) = before {
                output.push(RecordDelta { record, weight: -1 });
            }
            if let Some(record) = after {
                output.push(RecordDelta { record, weight: 1 });
            }
        }
        if should_apply_arrangement {
            match self.context.arrangement_update_mode {
                ArrangementUpdateMode::Accumulate => {
                    let arrangement = self.arrangement_states.entry(arrangement_key).or_default();
                    arrangement.mark_forward_as_of(sub_tick)?;
                    arrangement
                        .value_mut()
                        .replace_keys(touched_groups.keys(), staged_arrangement);
                }
                ArrangementUpdateMode::Replace => {
                    let mut arrangement = AsOf {
                        value: staged_arrangement,
                        as_of: current_as_of,
                    };
                    if replace_within_same_tick {
                        arrangement.replace_as_of_at_least(sub_tick);
                    } else {
                        arrangement.mark_forward_as_of(sub_tick)?;
                    }
                    self.arrangement_states.insert(arrangement_key, arrangement);
                }
            }
        }

        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas: consolidate_deltas(output),
        })
    }

    fn arrangement_key(
        &mut self,
        input: NodeId,
        descriptor: RecordDescriptor,
        fields: Arc<[String]>,
        comparison: ValueComparison,
    ) -> Result<ArrangementKey, IvmRuntimeError> {
        Ok(ArrangementKey {
            scope: self.operator_scope(input)?,
            input,
            fields,
            descriptor,
            comparison,
        })
    }

    fn join_field_names(&mut self, node: NodeId, join: &JoinOp) -> (Arc<[String]>, Arc<[String]>) {
        let meta = self.node_meta.entry(node).or_default();
        let left = meta
            .join_left_fields
            .get_or_insert_with(|| Arc::from(plan_expr_names(&join.left_key)))
            .clone();
        let right = meta
            .join_right_fields
            .get_or_insert_with(|| Arc::from(plan_expr_names(&join.right_key)))
            .clone();
        (left, right)
    }

    fn join_output_mapping(
        &mut self,
        node: NodeId,
        left_descriptor: RecordDescriptor,
        right_descriptor: RecordDescriptor,
        output_descriptor: RecordDescriptor,
    ) -> Result<Arc<[(usize, usize)]>, IvmRuntimeError> {
        if let Some(mapping) = &self.node_meta.entry(node).or_default().join_output_mapping {
            return Ok(mapping.clone());
        }
        let mapping = output_descriptor
            .fields()
            .iter()
            .map(|field| {
                let name = field
                    .name
                    .as_deref()
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
                if let Some(name) = name.strip_prefix("left.") {
                    let field_idx = left_descriptor
                        .field_index(name)
                        .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(name.to_owned()))?;
                    Ok((0, field_idx))
                } else if let Some(name) = name.strip_prefix("right.") {
                    let field_idx = right_descriptor
                        .field_index(name)
                        .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(name.to_owned()))?;
                    Ok((1, field_idx))
                } else {
                    Err(IvmRuntimeError::GraphFieldNotFound(name.to_owned()))
                }
            })
            .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
        let mapping = Arc::<[(usize, usize)]>::from(mapping);
        self.node_meta.entry(node).or_default().join_output_mapping = Some(mapping.clone());
        Ok(mapping)
    }

    fn aggregate_group_fields(&mut self, node: NodeId, aggregate: &AggregateOp) -> Arc<[String]> {
        self.node_meta
            .entry(node)
            .or_default()
            .aggregate_group_fields
            .get_or_insert_with(|| Arc::from(plan_expr_names(&aggregate.group_key)))
            .clone()
    }

    fn arrangement_sub_tick(&self, key: &ArrangementKey) -> SubTick {
        SubTick {
            tick: self.current_tick,
            // Root-scope arrangements represent table time, not recursive
            // evaluator time. A recursive step at sub_tick 1 and a sibling
            // non-recursive join must therefore share the same root SubTick.
            sub_tick: if key.scope == ScopeId::root() {
                0
            } else {
                self.context.sub_tick
            },
        }
    }

    fn update_recursive(
        &mut self,
        node: NodeId,
        recursive: &RecursiveOp,
        output_desc: RecordDescriptor,
        seed: NodeId,
        step: NodeId,
    ) -> Result<RecordDeltas, IvmRuntimeError> {
        let storage = self.storage.ok_or(IvmRuntimeError::StorageUnavailable)?;
        let operator_key = self.operator_key(node)?;
        // Recursive child evaluation may touch the same state maps. Remove only
        // this recursive node's state; child operator state stays available.
        let mut operator = self
            .operator_states
            .remove(&operator_key)
            .unwrap_or_else(|| OperatorState::Recursive(AsOf::new(RecursiveState::default())));
        let OperatorState::Recursive(recursive_as_of) = &mut operator else {
            return Err(IvmRuntimeError::NodeStateOperatorMismatch(node));
        };
        if self.context.eval_mode == EvalMode::Hydrate {
            if recursive_as_of.value().step_arrangements_hydrated()
                && recursive_as_of.as_of() == Some(Tick(self.current_tick))
            {
                let deltas = recursive_as_of
                    .value_at(Tick(self.current_tick))?
                    .accumulated_deltas();
                self.operator_states.insert(operator_key, operator);
                return Ok(RecordDeltas {
                    descriptor: output_desc,
                    deltas,
                });
            }
            let scope = self.context.scope.child(node);
            let next = recompute_recursive(
                self.schema,
                self.graph,
                self.variant_projections,
                node,
                recursive,
                output_desc,
                step,
                storage,
                self.binding_snapshots,
                self.current_tick,
                scope,
            )?;
            recursive_as_of.value_mut().replace_with(next);
            let accumulated = RecordDeltas {
                descriptor: output_desc,
                deltas: recursive_as_of.value().accumulated_deltas(),
            };
            let mut runtime = graph_runtime_view(
                self.schema,
                self.graph,
                self.variant_projections,
                self.table_deltas,
                self.binding_deltas,
                self.binding_snapshots,
                self.current_tick,
                self.operator_states,
                self.arrangement_states,
                self.eval_memo,
                self.eval_memo_bytes,
                self.table_frontiers,
                self.binding_frontiers,
                self.memo_use_clock,
                self.node_meta,
                storage,
                scope,
                self.metrics,
            );
            hydrate_recursive_arrangements(&mut runtime, recursive, step, accumulated.clone())?;
            recursive_as_of
                .value_mut()
                .mark_step_arrangements_hydrated();
            recursive_as_of.mark_forward_as_of(Tick(self.current_tick))?;
            self.operator_states.insert(operator_key, operator);
            return Ok(accumulated);
        }
        let deltas = recursive_delta(
            recursive_as_of.value_mut(),
            graph_runtime_view(
                self.schema,
                self.graph,
                self.variant_projections,
                self.table_deltas,
                self.binding_deltas,
                self.binding_snapshots,
                self.current_tick,
                self.operator_states,
                self.arrangement_states,
                self.eval_memo,
                self.eval_memo_bytes,
                self.table_frontiers,
                self.binding_frontiers,
                self.memo_use_clock,
                self.node_meta,
                storage,
                self.context.scope.child(node),
                self.metrics,
            ),
            node,
            recursive,
            output_desc,
            seed,
            step,
        )?;
        recursive_as_of.mark_forward_as_of(Tick(self.current_tick))?;
        self.operator_states.insert(operator_key, operator);
        Ok(RecordDeltas {
            descriptor: output_desc,
            deltas,
        })
    }

    fn update_unary_input(
        &mut self,
        graph_node: &crate::ivm::GraphNode,
        node: NodeId,
    ) -> Result<Arc<RecordDeltas>, IvmRuntimeError> {
        let input = *graph_node
            .descriptor
            .inputs
            .first()
            .ok_or(IvmRuntimeError::GraphInputMissing(node))?;
        self.update_node(input)
    }
}

fn extend_root_window_positions(
    descriptor: RecordDescriptor,
    window: &[WindowedRecord],
    positions: &mut BTreeMap<Vec<u8>, usize>,
) -> Result<(), IvmRuntimeError> {
    let mut index = 0usize;
    for (record, copies) in window {
        let key = encoded_record_key_part(descriptor, record, &[0])?;
        positions.entry(key).or_insert(index);
        index = index.saturating_add(usize::try_from(*copies).unwrap_or(usize::MAX));
    }
    Ok(())
}

fn apply_root_ordering_operations(
    before: &BTreeMap<Vec<u8>, usize>,
    after: &BTreeMap<Vec<u8>, usize>,
    terminal: &mut TerminalDeltas,
) {
    let mut current = before
        .iter()
        .map(|(key, index)| (*index, key.clone()))
        .collect::<Vec<_>>();
    current.sort_by_key(|(index, _)| *index);
    let mut current = current.into_iter().map(|(_, key)| key).collect::<Vec<_>>();
    for operation in &mut terminal.operations {
        if !operation.path.is_empty() {
            continue;
        }
        match &mut operation.edit {
            TerminalEdit::Insert { index, key, .. } => {
                if let Some(actual) = after.get(key) {
                    *index = *actual;
                }
                if let Some(existing) = current.iter().position(|candidate| candidate == key) {
                    current.remove(existing);
                }
                current.insert((*index).min(current.len()), key.clone());
            }
            TerminalEdit::Remove { key } => {
                if let Some(existing) = current.iter().position(|candidate| candidate == key) {
                    current.remove(existing);
                }
            }
            TerminalEdit::Update { .. } | TerminalEdit::Move { .. } => {}
        }
    }

    // Payload/nested edits are applied first. Positional edits follow, so a
    // consumer never observes a move targeting a root that is not present.
    let mut desired = after
        .iter()
        .map(|(key, index)| (*index, key.clone()))
        .collect::<Vec<_>>();
    desired.sort_by_key(|(index, _)| *index);
    for (after_index, key) in desired {
        if current.get(after_index) != Some(&key)
            && let Some(existing) = current.iter().position(|candidate| candidate == &key)
        {
            current.remove(existing);
            current.insert(after_index.min(current.len()), key.clone());
            terminal.operations.push(TerminalOperation {
                root_key: key.clone(),
                path: Vec::new(),
                edit: TerminalEdit::Move {
                    key,
                    index: after_index,
                },
            });
        }
    }
}

fn project_descriptor(
    input: &RecordDescriptor,
    fields: &[crate::ivm::ProjectField],
) -> Result<RecordDescriptor, IvmRuntimeError> {
    fields
        .iter()
        .map(|project_field| {
            let value_type = match &project_field.expression {
                ProjectExpr::Field(source) => {
                    let source_idx = resolve_field_ref(input, source)?;
                    input
                        .fields()
                        .get(source_idx)
                        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(source_idx))?
                        .value_type
                        .clone()
                }
                ProjectExpr::Literal(value) => value
                    .value_type()
                    .ok_or(IvmRuntimeError::UnsupportedOperator)?,
                ProjectExpr::TypedLiteral { value_type, .. } => value_type.clone(),
                ProjectExpr::Null(value_type) => value_type.clone(),
                ProjectExpr::Nullable(source) => {
                    let source_idx = resolve_field_ref(input, source)?;
                    let inner = input
                        .fields()
                        .get(source_idx)
                        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(source_idx))?
                        .value_type
                        .clone();
                    ValueType::Nullable(Box::new(inner))
                }
                ProjectExpr::NullableFlat(source) => {
                    let source_idx = resolve_field_ref(input, source)?;
                    let inner = input
                        .fields()
                        .get(source_idx)
                        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(source_idx))?
                        .value_type
                        .clone();
                    match inner {
                        ValueType::Nullable(_) => inner,
                        other => ValueType::Nullable(Box::new(other)),
                    }
                }
                ProjectExpr::EnumTagRemap { source, .. } => {
                    let source_idx = resolve_field_ref(input, source)?;
                    input
                        .fields()
                        .get(source_idx)
                        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(source_idx))?
                        .value_type
                        .clone()
                }
                ProjectExpr::EnumRemap { source, .. } => {
                    let source_idx = resolve_field_ref(input, source)?;
                    input
                        .fields()
                        .get(source_idx)
                        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(source_idx))?
                        .value_type
                        .clone()
                }
                ProjectExpr::RecursiveEnumRemap { target, .. } => target.clone(),
            };
            Ok((project_field.output_name.clone(), value_type))
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()
        .map(RecordDescriptor::new)
}

fn collect_by_descriptor(
    input: &RecordDescriptor,
    parent_fields: &[CollectByField],
    child_fields: &[CollectByField],
    collection_field: &str,
) -> Result<RecordDescriptor, IvmRuntimeError> {
    if parent_fields.is_empty() || child_fields.is_empty() || collection_field.is_empty() {
        return Err(IvmRuntimeError::InvalidCollectBy(
            "parent projection, child projection, and collection slot are required".into(),
        ));
    }
    let mut names = HashSet::new();
    let mut output = Vec::with_capacity(parent_fields.len() + 1);
    for field in parent_fields {
        if !names.insert(field.output_name.clone()) || field.output_name == collection_field {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "output field names must be unique".into(),
            ));
        }
        let index = resolve_field_ref(input, &field.field)?;
        output.push((
            field.output_name.clone(),
            collect_by_field_value_type(input, index, field)?,
        ));
    }
    let mut child_names = HashSet::new();
    let mut child = Vec::with_capacity(child_fields.len());
    for field in child_fields {
        if !child_names.insert(field.output_name.clone()) {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "child output field names must be unique".into(),
            ));
        }
        let index = resolve_field_ref(input, &field.field)?;
        child.push((
            field.output_name.clone(),
            collect_by_field_value_type(input, index, field)?,
        ));
    }
    output.push((
        collection_field.to_owned(),
        ValueType::Array(Box::new(ValueType::Record(Box::new(
            RecordDescriptor::new(child),
        )))),
    ));
    Ok(RecordDescriptor::new(output))
}

fn collect_by_root_descriptor(
    input: &RecordDescriptor,
    parent_fields: &[CollectByField],
) -> Result<RecordDescriptor, IvmRuntimeError> {
    if parent_fields.is_empty() {
        return Err(IvmRuntimeError::InvalidCollectBy(
            "root collect requires a parent projection".into(),
        ));
    }
    parent_fields
        .iter()
        .map(|field| {
            let index = resolve_field_ref(input, &field.field)?;
            Ok((
                field.output_name.clone(),
                collect_by_field_value_type(input, index, field)?,
            ))
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()
        .map(RecordDescriptor::new)
}

fn collect_by_tree_descriptor(
    input: &RecordDescriptor,
    parent_fields: &[CollectByField],
    slots: &[CollectBySlotBuilder],
) -> Result<RecordDescriptor, IvmRuntimeError> {
    if parent_fields.is_empty() || slots.is_empty() {
        return Err(IvmRuntimeError::InvalidCollectBy(
            "tree collect requires a parent projection and at least one collection slot".into(),
        ));
    }
    let mut names = HashSet::new();
    let mut output = Vec::with_capacity(parent_fields.len() + slots.len());
    for field in parent_fields {
        if !names.insert(field.output_name.clone()) {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "output field names must be unique".into(),
            ));
        }
        let index = resolve_field_ref(input, &field.field)?;
        output.push((
            field.output_name.clone(),
            collect_by_field_value_type(input, index, field)?,
        ));
    }
    for slot in slots {
        if !names.insert(slot.collection_field.clone()) {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "output field names must be unique".into(),
            ));
        }
        output.push((
            slot.collection_field.clone(),
            ValueType::Array(Box::new(ValueType::Record(Box::new(
                collect_by_slot_descriptor(input, slot, 1)?,
            )))),
        ));
    }
    Ok(RecordDescriptor::new(output))
}

fn collect_by_slot_descriptor(
    input: &RecordDescriptor,
    slot: &CollectBySlotBuilder,
    depth: usize,
) -> Result<RecordDescriptor, IvmRuntimeError> {
    if depth > MAX_COLLECT_BY_TREE_DEPTH {
        return Err(IvmRuntimeError::InvalidCollectBy(format!(
            "tree collect depth exceeds MAX_COLLECT_BY_TREE_DEPTH ({MAX_COLLECT_BY_TREE_DEPTH})"
        )));
    }
    if slot.group_cols.is_empty()
        || slot.child_fields.is_empty()
        || slot.collection_field.is_empty()
        || slot.order_cols.is_empty()
        || slot.tie_cols.is_empty()
    {
        return Err(IvmRuntimeError::InvalidCollectBy(
            "each tree collection slot requires group, child, name, order, and tie fields".into(),
        ));
    }
    let mut names = HashSet::new();
    let mut output = Vec::with_capacity(slot.child_fields.len() + slot.slots.len());
    for field in &slot.child_fields {
        if !names.insert(field.output_name.clone()) {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "child output field names must be unique".into(),
            ));
        }
        let index = resolve_field_ref(input, &field.field)?;
        output.push((
            field.output_name.clone(),
            collect_by_field_value_type(input, index, field)?,
        ));
    }
    for nested in &slot.slots {
        if !names.insert(nested.collection_field.clone()) {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "child output field names must be unique".into(),
            ));
        }
        output.push((
            nested.collection_field.clone(),
            ValueType::Array(Box::new(ValueType::Record(Box::new(
                collect_by_slot_descriptor(input, nested, depth + 1)?,
            )))),
        ));
    }
    Ok(RecordDescriptor::new(output))
}

fn collect_by_slots(
    input: &RecordDescriptor,
    builders: &[CollectBySlotBuilder],
    owner_indices: &[usize],
    depth: usize,
) -> Result<Vec<CollectBySlot>, IvmRuntimeError> {
    if depth > MAX_COLLECT_BY_TREE_DEPTH {
        return Err(IvmRuntimeError::InvalidCollectBy(format!(
            "tree collect depth exceeds MAX_COLLECT_BY_TREE_DEPTH ({MAX_COLLECT_BY_TREE_DEPTH})"
        )));
    }
    let mut names = HashSet::new();
    builders
        .iter()
        .enumerate()
        .map(|(slot_index, builder)| {
            if !names.insert(builder.collection_field.clone()) {
                return Err(IvmRuntimeError::InvalidCollectBy(
                    "sibling collection slot names must be unique".into(),
                ));
            }
            let group_field_indices = builder
                .group_cols
                .iter()
                .map(|field| resolve_field_ref(input, field))
                .collect::<Result<Vec<_>, _>>()?;
            if group_field_indices.is_empty()
                || group_field_indices
                    .iter()
                    .any(|field| !owner_indices.contains(field))
            {
                return Err(IvmRuntimeError::InvalidCollectBy(
                    "a slot group must be available on its owning record".into(),
                ));
            }
            validate_collect_by_key_types(input, &group_field_indices)?;
            let child_fields = collect_by_projections(input, &builder.child_fields)?;
            let order_field_indices = builder
                .order_cols
                .iter()
                .map(|order| resolve_field_ref(input, &order.field))
                .collect::<Result<Vec<_>, _>>()?;
            let tie_field_indices = builder
                .tie_cols
                .iter()
                .map(|field| resolve_field_ref(input, field))
                .collect::<Result<Vec<_>, _>>()?;
            let presence_field_index = builder
                .presence_col
                .as_ref()
                .map(|field| resolve_field_ref(input, field))
                .transpose()?;
            if let Some(index) = presence_field_index
                && input.fields()[index].value_type != ValueType::Bool
            {
                return Err(IvmRuntimeError::InvalidCollectBy(
                    "a tree collection presence field must be boolean".into(),
                ));
            }
            if order_field_indices.is_empty() || tie_field_indices.is_empty() {
                return Err(IvmRuntimeError::InvalidCollectBy(
                    "order and tie fields must both be complete and non-empty".into(),
                ));
            }
            validate_collect_by_key_types(input, &order_field_indices)?;
            validate_collect_by_key_types(input, &tie_field_indices)?;
            let child_descriptor = collect_by_slot_descriptor(input, builder, depth)?;
            let child_indices = child_fields
                .iter()
                .map(|field| field.field_idx)
                .collect::<Vec<_>>();
            let slots = collect_by_slots(input, &builder.slots, &child_indices, depth + 1)?;
            Ok(CollectBySlot {
                group_fields: group_field_indices
                    .iter()
                    .map(|field| field_name_at(input, *field))
                    .collect::<Result<Vec<_>, _>>()?,
                group_field_indices,
                child_fields,
                child_descriptor,
                collection_field: builder.collection_field.clone(),
                collection_field_index: builder.child_fields.len() + slot_index,
                slots,
                order_fields: builder
                    .order_cols
                    .iter()
                    .zip(&order_field_indices)
                    .map(|(order, field)| {
                        Ok(TopByOrderField {
                            field: field_name_at(input, *field)?,
                            direction: order.direction,
                        })
                    })
                    .collect::<Result<Vec<_>, IvmRuntimeError>>()?,
                tie_fields: tie_field_indices
                    .iter()
                    .map(|field| field_name_at(input, *field))
                    .collect::<Result<Vec<_>, _>>()?,
                presence_field_index,
                sort_field_indices: order_field_indices
                    .iter()
                    .chain(&tie_field_indices)
                    .copied()
                    .collect(),
                sort_directions: builder
                    .order_cols
                    .iter()
                    .map(|order| order.direction)
                    .chain(std::iter::repeat_n(
                        TopByDirection::Asc,
                        tie_field_indices.len(),
                    ))
                    .collect(),
                offset: builder.offset,
                limit: builder.limit,
            })
        })
        .collect()
}

fn collect_by_expand_descriptor(
    input: &RecordDescriptor,
    tuple_fields: &[CollectByField],
) -> Result<RecordDescriptor, IvmRuntimeError> {
    if tuple_fields.is_empty() {
        return Err(IvmRuntimeError::InvalidCollectBy(
            "expand mode requires a non-empty tuple projection".into(),
        ));
    }
    let mut names = HashSet::new();
    tuple_fields
        .iter()
        .map(|field| {
            if !names.insert(field.output_name.clone()) {
                return Err(IvmRuntimeError::InvalidCollectBy(
                    "expand tuple output field names must be unique".into(),
                ));
            }
            let index = resolve_field_ref(input, &field.field)?;
            Ok((
                field.output_name.clone(),
                collect_by_field_value_type(input, index, field)?,
            ))
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()
        .map(RecordDescriptor::new)
}

fn collect_by_projections(
    input: &RecordDescriptor,
    fields: &[CollectByField],
) -> Result<Vec<CollectByProjection>, IvmRuntimeError> {
    fields
        .iter()
        .map(|field| {
            Ok(CollectByProjection {
                field: field_ref_name(input, &field.field)?,
                field_idx: resolve_field_ref(input, &field.field)?,
                output_name: field.output_name.clone(),
                unwrap_nullable: field.unwrap_nullable,
            })
        })
        .collect()
}

fn collect_by_field_value_type(
    input: &RecordDescriptor,
    index: usize,
    field: &CollectByField,
) -> Result<ValueType, IvmRuntimeError> {
    let value_type = input.fields()[index].value_type.clone();
    if !field.unwrap_nullable {
        return Ok(value_type);
    }
    Ok(match value_type {
        ValueType::Nullable(inner) => *inner,
        value_type => value_type,
    })
}

fn validate_collect_by_key_types(
    input: &RecordDescriptor,
    indices: &[usize],
) -> Result<(), IvmRuntimeError> {
    fn ordered_scalar(value_type: &ValueType) -> bool {
        match value_type {
            ValueType::Nullable(inner) => ordered_scalar(inner),
            ValueType::U8
            | ValueType::U16
            | ValueType::U32
            | ValueType::U64
            | ValueType::I32
            | ValueType::I64
            | ValueType::F64
            | ValueType::Bool
            | ValueType::String
            | ValueType::Bytes
            | ValueType::Uuid
            | ValueType::EnumTag(_) => true,
            _ => false,
        }
    }
    for &index in indices {
        let value_type = &input
            .fields()
            .get(index)
            .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(index))?
            .value_type;
        let scalar = ordered_scalar(value_type);
        if value_type.contains_record() || !scalar {
            return Err(IvmRuntimeError::InvalidCollectBy(
                "group, order, and tie fields must be scalar ordered values without records".into(),
            ));
        }
    }
    Ok(())
}

fn validate_collect_by_terminality(graph: &GraphBuilder) -> Result<(), IvmRuntimeError> {
    fn contains_collect_by(graph: &GraphBuilder) -> bool {
        match graph {
            GraphBuilder::CollectBy { .. } => true,
            GraphBuilder::Recursive { seed, step, .. } => {
                contains_collect_by(seed) || contains_collect_by(step)
            }
            GraphBuilder::Filter { input, .. }
            | GraphBuilder::Project { input, .. }
            | GraphBuilder::UnwrapNullable { input, .. }
            | GraphBuilder::Unnest { input, .. }
            | GraphBuilder::VariantProject { input, .. }
            | GraphBuilder::ArgMaxBy { input, .. }
            | GraphBuilder::ArgMinBy { input, .. }
            | GraphBuilder::TopBy { input, .. }
            | GraphBuilder::Aggregate { input, .. } => contains_collect_by(input),
            GraphBuilder::Union { inputs } => inputs.iter().any(contains_collect_by),
            GraphBuilder::Join { left, right, .. }
            | GraphBuilder::SemiJoin { left, right, .. }
            | GraphBuilder::AntiJoin { left, right, .. } => {
                contains_collect_by(left) || contains_collect_by(right)
            }
            GraphBuilder::Table { .. }
            | GraphBuilder::InlineRecords { .. }
            | GraphBuilder::Index { .. }
            | GraphBuilder::FrontierSource { .. }
            | GraphBuilder::BindingSource { .. } => false,
        }
    }

    match graph {
        GraphBuilder::CollectBy { input, .. } => {
            if contains_collect_by(input) {
                return Err(IvmRuntimeError::CollectByMustBeTerminal);
            }
            validate_collect_by_terminality(input)
        }
        GraphBuilder::Recursive { seed, step, .. } => {
            if contains_collect_by(seed) || contains_collect_by(step) {
                return Err(IvmRuntimeError::CollectByMustBeTerminal);
            }
            validate_collect_by_terminality(seed)?;
            validate_collect_by_terminality(step)
        }
        GraphBuilder::Filter { input, .. } | GraphBuilder::Project { input, .. } => {
            validate_collect_by_terminality(input)
        }
        GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::Unnest { input, .. }
        | GraphBuilder::VariantProject { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. }
        | GraphBuilder::Aggregate { input, .. } => {
            if contains_collect_by(input) {
                return Err(IvmRuntimeError::CollectByMustBeTerminal);
            }
            validate_collect_by_terminality(input)
        }
        GraphBuilder::Union { inputs } => {
            if inputs.iter().any(contains_collect_by) {
                return Err(IvmRuntimeError::CollectByMustBeTerminal);
            }
            for input in inputs {
                validate_collect_by_terminality(input)?;
            }
            Ok(())
        }
        GraphBuilder::Join { left, right, .. }
        | GraphBuilder::SemiJoin { left, right, .. }
        | GraphBuilder::AntiJoin { left, right, .. } => {
            if contains_collect_by(left) || contains_collect_by(right) {
                return Err(IvmRuntimeError::CollectByMustBeTerminal);
            }
            validate_collect_by_terminality(left)?;
            validate_collect_by_terminality(right)
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::BindingSource { .. } => Ok(()),
    }
}

fn project_field_expr(
    input: &RecordDescriptor,
    field: &ProjectField,
) -> Result<PlanExpr, IvmRuntimeError> {
    match &field.expression {
        ProjectExpr::Field(source) => Ok(PlanExpr::field(field_ref_name(input, source)?)),
        ProjectExpr::Literal(value) => Ok(PlanExpr::literal(value.clone())),
        ProjectExpr::TypedLiteral { value, .. } => Ok(PlanExpr::literal(value.clone())),
        ProjectExpr::Null(value_type) => Ok(PlanExpr::null(value_type.clone())),
        ProjectExpr::Nullable(source) => Ok(PlanExpr::nullable(field_ref_name(input, source)?)),
        ProjectExpr::NullableFlat(source) => {
            Ok(PlanExpr::nullable_flat(field_ref_name(input, source)?))
        }
        ProjectExpr::EnumTagRemap { source, tags } => Ok(PlanExpr::EnumTagRemap {
            field: field_ref_name(input, source)?,
            tags: tags.clone(),
        }),
        ProjectExpr::EnumRemap { source, tags } => Ok(PlanExpr::EnumRemap {
            field: field_ref_name(input, source)?,
            tags: tags.clone(),
        }),
        ProjectExpr::RecursiveEnumRemap { source, remaps, .. } => {
            Ok(PlanExpr::RecursiveEnumRemap {
                field: field_ref_name(input, source)?,
                remaps: remaps.clone(),
            })
        }
    }
}

fn project_record(
    expressions: &[ProjectionExpr],
    mapping: &[(usize, usize)],
    output_desc: RecordDescriptor,
    input_desc: &RecordDescriptor,
    input_record: &[u8],
) -> Result<Vec<u8>, IvmRuntimeError> {
    if projection_uses_raw_copy(expressions, mapping, output_desc) {
        return Ok(output_desc.project_record_raw(
            std::slice::from_ref(input_desc),
            &[input_record],
            mapping,
        )?);
    }

    let input = BorrowedRecord::new(input_record, input_desc);
    let mut values = Vec::with_capacity(expressions.len());
    for expr in expressions {
        values.push(match &expr.expression {
            PlanExpr::Field(field) => input.get(field)?.clone(),
            PlanExpr::Literal(value) => value.to_value(),
            PlanExpr::Null(_) => Value::Nullable(None),
            PlanExpr::Nullable(field) => Value::Nullable(Some(Box::new(input.get(field)?.clone()))),
            PlanExpr::NullableFlat(field) => {
                let value = input.get(field)?.clone();
                if matches!(value, Value::Nullable(_)) {
                    value
                } else {
                    Value::Nullable(Some(Box::new(value)))
                }
            }
            PlanExpr::EnumTagRemap { field, tags } => {
                remap_enum_tag(input.get(field)?.clone(), tags)?
            }
            PlanExpr::EnumRemap { field, tags } => remap_enum(input.get(field)?.clone(), tags)?,
            PlanExpr::RecursiveEnumRemap { field, remaps } => {
                let source_idx = input_desc
                    .field_index(field)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.clone()))?;
                let output_idx = output_desc
                    .field_index(expr.output_name.as_deref().ok_or_else(|| {
                        IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned())
                    })?)
                    .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.clone()))?;
                remap_recursive_enum_value(
                    input.get(field)?.clone(),
                    &input_desc.fields()[source_idx].value_type,
                    &output_desc.fields()[output_idx].value_type,
                    remaps,
                    "root",
                )?
            }
        });
    }
    Ok(output_desc.create(&values)?)
}

fn remap_enum_tag(value: Value, tags: &[Option<u8>]) -> Result<Value, IvmRuntimeError> {
    match value {
        Value::EnumTag(tag) => tags
            .get(usize::from(tag))
            .and_then(|tag| *tag)
            .map(Value::EnumTag)
            .ok_or(IvmRuntimeError::EnumTagProjectionAbsent { tag }),
        Value::Nullable(None) => Ok(Value::Nullable(None)),
        Value::Nullable(Some(value)) => Ok(Value::Nullable(Some(Box::new(remap_enum_tag(
            *value, tags,
        )?)))),
        _ => Err(IvmRuntimeError::EnumTagProjectionNonEnum),
    }
}

fn remap_enum(value: Value, tags: &[Option<u32>]) -> Result<Value, IvmRuntimeError> {
    match value {
        Value::Enum(value) => {
            let tag = value.tag();
            let mapped = tags
                .get(
                    usize::try_from(tag)
                        .map_err(|_| IvmRuntimeError::EnumProjectionAbsent { tag })?,
                )
                .and_then(|tag| *tag)
                .ok_or(IvmRuntimeError::EnumProjectionAbsent { tag })?;
            Ok(Value::Enum(crate::records::EnumValue::new(
                mapped,
                value.into_record(),
            )))
        }
        Value::Nullable(None) => Ok(Value::Nullable(None)),
        Value::Nullable(Some(value)) => {
            Ok(Value::Nullable(Some(Box::new(remap_enum(*value, tags)?))))
        }
        _ => Err(IvmRuntimeError::EnumProjectionNonEnum),
    }
}

/// Re-encode an arbitrary user value across two descriptors whose enum
/// registries use different compact tags.  This is intentionally a value
/// operation rather than a byte splice: descriptor equality does not imply
/// that a physical enum tag has the same meaning in the authored descriptor.
fn remap_recursive_enum_value(
    value: Value,
    source: &ValueType,
    target: &ValueType,
    remaps: &RecursiveEnumRemaps,
    path: &str,
) -> Result<Value, IvmRuntimeError> {
    match (value, source, target) {
        (Value::EnumTag(tag), ValueType::EnumTag(_), ValueType::EnumTag(_)) => remaps
            .scalar
            .get(path)
            .and_then(|tags| tags.get(usize::from(tag)))
            .and_then(|tag| *tag)
            .map(Value::EnumTag)
            .ok_or(IvmRuntimeError::EnumTagProjectionAbsent { tag }),
        (Value::Nullable(None), ValueType::Nullable(_), ValueType::Nullable(_)) => {
            Ok(Value::Nullable(None))
        }
        (
            Value::Nullable(Some(value)),
            ValueType::Nullable(source),
            ValueType::Nullable(target),
        ) => Ok(Value::Nullable(Some(Box::new(remap_recursive_enum_value(
            *value,
            source,
            target,
            remaps,
            &format!("{path}/nullable"),
        )?)))),
        (Value::Array(values), ValueType::Array(source), ValueType::Array(target)) => {
            Ok(Value::Array(
                values
                    .into_iter()
                    .map(|value| {
                        remap_recursive_enum_value(
                            value,
                            source,
                            target,
                            remaps,
                            &format!("{path}/array"),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        (Value::Tuple(values), ValueType::Tuple(source), ValueType::Tuple(target))
            if values.len() == source.len() && source.len() == target.len() =>
        {
            Ok(Value::Tuple(
                values
                    .into_iter()
                    .zip(source.iter().zip(target))
                    .enumerate()
                    .map(|(index, (value, (source, target)))| {
                        remap_recursive_enum_value(
                            value,
                            source,
                            target,
                            remaps,
                            &format!("{path}/tuple/{index}"),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        (Value::Record(record), ValueType::Record(source), ValueType::Record(target))
            if source.fields().len() == target.fields().len() =>
        {
            let values = record.to_values()?;
            let values = values
                .into_iter()
                .zip(source.fields().iter().zip(target.fields()))
                .map(|(value, (source, target))| {
                    let name = source.name.as_deref().ok_or_else(|| {
                        IvmRuntimeError::RecursiveEnumProjectionDescriptorMismatch {
                            path: path.to_owned(),
                        }
                    })?;
                    remap_recursive_enum_value(
                        value,
                        &source.value_type,
                        &target.value_type,
                        remaps,
                        &format!("{path}/record/{name}"),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::Record(OwnedRecord::new(
                target.create(&values)?,
                **target,
            )))
        }
        (Value::Enum(value), ValueType::Enum(source), ValueType::Enum(target)) => {
            let source_tag = value.tag();
            let target_tag = remaps
                .payload
                .get(path)
                .and_then(|tags| tags.get(usize::try_from(source_tag).ok()?))
                .and_then(|tag| *tag)
                .ok_or(IvmRuntimeError::EnumProjectionAbsent { tag: source_tag })?;
            let source_case = source.case(source_tag)?;
            let target_case = target.case(target_tag)?;
            if source_case.payload.fields().len() != target_case.payload.fields().len() {
                return Err(IvmRuntimeError::RecursiveEnumProjectionDescriptorMismatch {
                    path: path.to_owned(),
                });
            }
            let values = value.record().to_values()?;
            let values = values
                .into_iter()
                .zip(
                    source_case
                        .payload
                        .fields()
                        .iter()
                        .zip(target_case.payload.fields()),
                )
                .map(|(value, (source, target))| {
                    let name = source.name.as_deref().ok_or_else(|| {
                        IvmRuntimeError::RecursiveEnumProjectionDescriptorMismatch {
                            path: path.to_owned(),
                        }
                    })?;
                    remap_recursive_enum_value(
                        value,
                        &source.value_type,
                        &target.value_type,
                        remaps,
                        &format!("{path}/case/{source_tag}/{name}"),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::Enum(EnumValue::new(
                target_tag,
                OwnedRecord::new(target_case.payload.create(&values)?, target_case.payload),
            )))
        }
        (value, source, target) if source == target => Ok(value),
        _ => Err(IvmRuntimeError::RecursiveEnumProjectionDescriptorMismatch {
            path: path.to_owned(),
        }),
    }
}

fn projection_uses_raw_copy(
    expressions: &[ProjectionExpr],
    mapping: &[(usize, usize)],
    output_desc: RecordDescriptor,
) -> bool {
    if expressions.is_empty() {
        // Legacy/validation-only path: normal lowering always fills expressions.
        return mapping.len() == output_desc.fields().len();
    }
    expressions.len() == output_desc.fields().len()
        && mapping.len() == output_desc.fields().len()
        && expressions
            .iter()
            .all(|expr| matches!(expr.expression, PlanExpr::Field(_)))
}

fn raw_projection_fields(
    project: &MapProjectOp,
    input_desc: &RecordDescriptor,
    output_desc: RecordDescriptor,
) -> Result<Option<Vec<RawProjectionField>>, IvmRuntimeError> {
    if project.expressions.is_empty() || project.expressions.len() != output_desc.fields().len() {
        return Ok(None);
    }

    let fields = project
        .expressions
        .iter()
        .map(|expr| match &expr.expression {
            PlanExpr::Field(field) => input_desc
                .field_index(field)
                .map(|source_idx| RawProjectionField::Copy { source_idx }),
            PlanExpr::Nullable(field) => input_desc
                .field_index(field)
                .map(|source_idx| RawProjectionField::WrapNullable { source_idx }),
            PlanExpr::NullableFlat(field) => input_desc
                .field_index(field)
                .map(|source_idx| RawProjectionField::FlattenNullable { source_idx }),
            PlanExpr::Null(_) => Some(RawProjectionField::Encoded {
                bytes: encode_projection_field_value(
                    output_desc,
                    expr.output_name.as_deref(),
                    Value::Nullable(None),
                )
                .ok()?,
            }),
            PlanExpr::Literal(value) => Some(RawProjectionField::Encoded {
                bytes: encode_projection_field_value(
                    output_desc,
                    expr.output_name.as_deref(),
                    value.to_value(),
                )
                .ok()?,
            }),
            PlanExpr::EnumTagRemap { .. }
            | PlanExpr::EnumRemap { .. }
            | PlanExpr::RecursiveEnumRemap { .. } => None,
        })
        .collect::<Option<Vec<_>>>();
    Ok(fields)
}

fn encode_projection_field_value(
    output_desc: RecordDescriptor,
    output_name: Option<&str>,
    value: Value,
) -> Result<Vec<u8>, IvmRuntimeError> {
    let field_idx = if let Some(output_name) = output_name {
        output_desc
            .field_index(output_name)
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(output_name.to_owned()))?
    } else {
        return Err(IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()));
    };
    let field = output_desc
        .fields()
        .get(field_idx)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field_idx))?;
    records::encode_single_field_value(&value, &field.value_type).map_err(Into::into)
}

fn resolve_field_ref(
    descriptor: &RecordDescriptor,
    field: &FieldRef,
) -> Result<usize, IvmRuntimeError> {
    match field {
        FieldRef::Name(name) => descriptor
            .field_index(name)
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(name.clone())),
        FieldRef::Resolved(index) => {
            if *index < descriptor.fields().len() {
                Ok(*index)
            } else {
                Err(IvmRuntimeError::GraphFieldIndexOutOfBounds(*index))
            }
        }
    }
}

fn field_ref_name(
    descriptor: &RecordDescriptor,
    field: &FieldRef,
) -> Result<String, IvmRuntimeError> {
    match field {
        FieldRef::Name(name) => Ok(name.clone()),
        FieldRef::Resolved(index) => field_name_at(descriptor, *index),
    }
}

fn field_name_at(descriptor: &RecordDescriptor, index: usize) -> Result<String, IvmRuntimeError> {
    let field = descriptor
        .fields()
        .get(index)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(index))?;
    field
        .name
        .clone()
        .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(format!("#{index}")))
}

fn unwrap_nullable_descriptor(
    input: &RecordDescriptor,
    field_idx: usize,
) -> Result<RecordDescriptor, IvmRuntimeError> {
    input
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let value_type = if idx == field_idx {
                match &field.value_type {
                    ValueType::Nullable(inner) => (**inner).clone(),
                    other => other.clone(),
                }
            } else {
                field.value_type.clone()
            };
            Ok((field.name.clone().unwrap_or_default(), value_type))
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()
        .map(RecordDescriptor::new)
}

fn unnest_descriptor(
    input: &RecordDescriptor,
    array_field_idx: usize,
    element_field: &str,
) -> Result<RecordDescriptor, IvmRuntimeError> {
    let array_field = input
        .fields()
        .get(array_field_idx)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(array_field_idx))?;
    let ValueType::Array(element_type) = &array_field.value_type else {
        return Err(IvmRuntimeError::UnsupportedOperator);
    };
    let mut fields = input
        .fields()
        .iter()
        .map(|field| {
            (
                field.name.clone().unwrap_or_default(),
                field.value_type.clone(),
            )
        })
        .collect::<Vec<_>>();
    fields.push((element_field.to_owned(), (**element_type).clone()));
    Ok(RecordDescriptor::new(fields))
}

fn variant_project_descriptor(
    input: &RecordDescriptor,
    field: &FieldRef,
    case: &str,
) -> Result<RecordDescriptor, IvmRuntimeError> {
    let field_idx = resolve_field_ref(input, field)?;
    let enum_field = input
        .fields()
        .get(field_idx)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field_idx))?;
    let ValueType::Enum(schema) = &enum_field.value_type else {
        return Err(IvmRuntimeError::VariantProjectFieldTypeMismatch {
            field: field_ref_name(input, field)?,
        });
    };
    Ok(schema.case(schema.tag(case)?)?.payload)
}

fn aggregate_descriptor(
    input: &RecordDescriptor,
    group_cols: &[FieldRef],
    aggregates: &[AggregateExpr],
) -> Result<RecordDescriptor, IvmRuntimeError> {
    let mut fields = Vec::new();
    for group_col in group_cols {
        let field_idx = resolve_field_ref(input, group_col)?;
        let field = input
            .fields()
            .get(field_idx)
            .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field_idx))?;
        fields.push((field_ref_name(input, group_col)?, field.value_type.clone()));
    }
    for (index, aggregate) in aggregates.iter().enumerate() {
        let name = aggregate
            .output_name
            .clone()
            .unwrap_or_else(|| format!("aggregate_{index}"));
        fields.push((name, aggregate_output_type(input, aggregate)?));
    }
    Ok(RecordDescriptor::new(fields))
}

fn aggregate_output_type(
    input: &RecordDescriptor,
    aggregate: &AggregateExpr,
) -> Result<ValueType, IvmRuntimeError> {
    Ok(match aggregate.function {
        AggregateFunction::Count => ValueType::U64,
        AggregateFunction::Avg => ValueType::Nullable(Box::new(ValueType::F64)),
        AggregateFunction::Sum => {
            let value_type = aggregate_expr_value_type(input, aggregate)?;
            match non_nullable_type(&value_type) {
                ValueType::U8
                | ValueType::U16
                | ValueType::U32
                | ValueType::U64
                | ValueType::I32
                | ValueType::I64
                | ValueType::F64 => nullable_type(&value_type),
                _ => return Err(IvmRuntimeError::UnsupportedOperator),
            }
        }
        AggregateFunction::Min | AggregateFunction::Max => {
            let value_type = aggregate_expr_value_type(input, aggregate)?;
            nullable_type(&value_type)
        }
    })
}

fn aggregate_expr_value_type(
    input: &RecordDescriptor,
    aggregate: &AggregateExpr,
) -> Result<ValueType, IvmRuntimeError> {
    let Some(expr) = &aggregate.expression else {
        return Err(IvmRuntimeError::UnsupportedOperator);
    };
    match expr {
        PlanExpr::Field(field)
        | PlanExpr::Nullable(field)
        | PlanExpr::NullableFlat(field)
        | PlanExpr::EnumTagRemap { field, .. }
        | PlanExpr::EnumRemap { field, .. }
        | PlanExpr::RecursiveEnumRemap { field, .. } => {
            let field_idx = input
                .field_index(field)
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.clone()))?;
            Ok(input
                .fields()
                .get(field_idx)
                .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field_idx))?
                .value_type
                .clone())
        }
        PlanExpr::Literal(literal) => literal
            .value_type()
            .ok_or(IvmRuntimeError::UnsupportedOperator),
        PlanExpr::Null(value_type) => Ok(value_type.clone()),
    }
}

fn non_nullable_type(value_type: &ValueType) -> &ValueType {
    match value_type {
        ValueType::Nullable(inner) => inner,
        other => other,
    }
}

/// Used for converting non-nullable column types to nullable column types when aggregating.
fn nullable_type(value_type: &ValueType) -> ValueType {
    ValueType::Nullable(Box::new(non_nullable_type(value_type).clone()))
}

fn index_record_descriptor() -> RecordDescriptor {
    static DESCRIPTOR: std::sync::OnceLock<RecordDescriptor> = std::sync::OnceLock::new();
    *DESCRIPTOR.get_or_init(|| {
        RecordDescriptor::new([("key", ValueType::Bytes), ("value", ValueType::Bytes)])
    })
}

fn variant_projection_target_name(target: &VariantProjectionTarget) -> &str {
    match target {
        VariantProjectionTarget::Named(name) | VariantProjectionTarget::SchemaIndex(name) => name,
    }
}

fn schema_index_input_fields(
    table: &TableSchema,
    index: &IndexSchema,
) -> Result<Vec<String>, IvmRuntimeError> {
    let primary_key = table
        .primary_key
        .as_ref()
        .ok_or_else(|| IvmRuntimeError::MissingPrimaryKey(table.name.clone()))?;
    let catalogue = table.record_schema();
    let mut fields = Vec::new();
    for field in index
        .columns
        .iter()
        .chain(primary_key.columns.iter().map(|column| &column.column))
    {
        if catalogue.field_index(field).is_none() {
            return Err(IvmRuntimeError::GraphFieldNotFound(field.clone()));
        }
        if !fields.contains(field) {
            fields.push(field.clone());
        }
    }
    Ok(fields)
}

fn schema_index_input_descriptor(
    table: &TableSchema,
    index: &IndexSchema,
) -> Result<RecordDescriptor, IvmRuntimeError> {
    let catalogue = table.record_schema();
    let fields = schema_index_input_fields(table, index)?
        .into_iter()
        .map(ProjectField::named)
        .collect::<Vec<_>>();
    project_descriptor(&catalogue, &fields)
}

fn apply_index_by(
    index_by: &IndexByOp,
    input_descriptor: &RecordDescriptor,
    input_deltas: &[RecordDelta],
) -> Result<Vec<RecordDelta>, IvmRuntimeError> {
    let mut deltas = Vec::new();
    let scalar_key_fields = index_key_fields_are_scalar(index_by, input_descriptor)?;
    for delta in input_deltas {
        let value = if index_by.store_value {
            primary_key_value_bytes(input_descriptor, delta.raw(), &index_by.value_fields)?
        } else {
            Vec::new()
        };
        if scalar_key_fields {
            let key = scalar_index_key(index_by, input_descriptor, delta.raw())?;
            if let Some(scan) = &index_by.scan
                && !key_matches_static_scan(&key, scan)?
            {
                continue;
            }
            deltas.push(RecordDelta {
                record: index_record_descriptor()
                    .create(&[Value::Bytes(key), Value::Bytes(value)])?
                    .into(),
                weight: delta.weight,
            });
            continue;
        }

        let keys = index_keys(index_by, input_descriptor, delta.raw())?;
        for key in keys {
            if let Some(scan) = &index_by.scan
                && !key_matches_static_scan(&key, scan)?
            {
                continue;
            }
            deltas.push(RecordDelta {
                record: index_record_descriptor()
                    .create(&[Value::Bytes(key), Value::Bytes(value.clone())])?
                    .into(),
                weight: delta.weight,
            });
        }
    }
    Ok(deltas)
}

fn index_key_fields_are_scalar(
    index_by: &IndexByOp,
    input_descriptor: &RecordDescriptor,
) -> Result<bool, IvmRuntimeError> {
    for field_idx in &index_by.key_fields {
        let field = input_descriptor
            .fields()
            .get(*field_idx)
            .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(*field_idx))?;
        match &field.value_type {
            ValueType::Array(_) => return Ok(false),
            ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::Array(_)) => {
                return Ok(false);
            }
            _ => {}
        }
    }
    Ok(true)
}

fn scalar_index_key(
    index_by: &IndexByOp,
    input_descriptor: &RecordDescriptor,
    record: &[u8],
) -> Result<Vec<u8>, IvmRuntimeError> {
    let mut key = Vec::new();
    for field_idx in &index_by.key_fields {
        encode_record_field_key_part(&mut key, input_descriptor, record, *field_idx)?;
    }
    if index_by.append_value_to_key {
        let value = primary_key_value_bytes(input_descriptor, record, &index_by.value_fields)?;
        key.push(0xff);
        key.extend(value);
    }
    Ok(key)
}

fn index_keys(
    index_by: &IndexByOp,
    input_descriptor: &RecordDescriptor,
    record: &[u8],
) -> Result<Vec<Vec<u8>>, IvmRuntimeError> {
    let mut keys = vec![Vec::new()];
    let mut seen = HashSet::new();

    for field_idx in &index_by.key_fields {
        let parts = record_field_key_parts(input_descriptor, record, *field_idx)?;
        if parts.is_empty() {
            return Ok(Vec::new());
        }

        let mut next_keys = Vec::with_capacity(keys.len() * parts.len());
        for key in &keys {
            for part in &parts {
                let mut next = key.clone();
                next.extend(part);
                if seen.insert(next.clone()) {
                    next_keys.push(next);
                }
            }
        }
        keys = next_keys;
        seen.clear();
    }
    if index_by.append_value_to_key {
        let value = primary_key_value_bytes(input_descriptor, record, &index_by.value_fields)?;
        // Non-unique indices append the primary key so equal index values remain
        // distinct and ordered for range scans.
        for key in &mut keys {
            key.push(0xff);
            key.extend(&value);
        }
    }
    Ok(keys)
}

pub(super) enum StaticScanBounds {
    Prefix(Vec<u8>),
    Range { start: Vec<u8>, end: Vec<u8> },
}

pub(super) fn scan_bounds(scan: &StaticScanSpec) -> Result<StaticScanBounds, IvmRuntimeError> {
    match scan {
        StaticScanSpec::Point(values) | StaticScanSpec::Prefix(values) => {
            Ok(StaticScanBounds::Prefix(static_scan_key(values)?))
        }
        StaticScanSpec::Range { start, end } => Ok(StaticScanBounds::Range {
            start: static_scan_key(start)?,
            end: static_scan_key(end)?,
        }),
    }
}

fn static_scan_key(values: &[LiteralValue]) -> Result<Vec<u8>, IvmRuntimeError> {
    let mut key = Vec::new();
    for value in values {
        encode_key_part(&mut key, &value.to_value())?;
    }
    Ok(key)
}

fn key_matches_static_scan(key: &[u8], scan: &StaticScanSpec) -> Result<bool, IvmRuntimeError> {
    Ok(match scan_bounds(scan)? {
        StaticScanBounds::Prefix(prefix) => key.starts_with(&prefix),
        StaticScanBounds::Range { start, end } => start.as_slice() <= key && key < end.as_slice(),
    })
}

fn persisted_index_scan_bounds(
    table: &str,
    index: &str,
    scan: Option<&StaticScanSpec>,
) -> Result<StaticScanBounds, IvmRuntimeError> {
    let base = durable_index_key_prefix(table, index);
    let wrap_prefix = |logical_key: Vec<u8>| {
        let mut storage_key = base.clone();
        if !logical_key.is_empty() {
            storage_key.push(7);
            encode_ordered_bytes_without_terminal(&mut storage_key, &logical_key);
        }
        storage_key
    };
    Ok(match scan {
        None => StaticScanBounds::Prefix(base),
        Some(StaticScanSpec::Point(values) | StaticScanSpec::Prefix(values)) => {
            StaticScanBounds::Prefix(wrap_prefix(static_scan_key(values)?))
        }
        Some(StaticScanSpec::Range { start, end }) => StaticScanBounds::Range {
            start: wrap_prefix(static_scan_key(start)?),
            end: wrap_prefix(static_scan_key(end)?),
        },
    })
}

pub(crate) fn durable_index_key_prefix(table: &str, index: &str) -> Vec<u8> {
    let mut prefix = Vec::new();
    // NUL separators keep table/index names prefix-decodable without escaping.
    prefix.extend(table.as_bytes());
    prefix.push(0);
    prefix.extend(index.as_bytes());
    prefix.push(0);
    prefix
}

fn encode_ordered_bytes_without_terminal(key: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            key.extend([0, 0xff]);
        } else {
            key.push(*byte);
        }
    }
}

fn primary_key_value_bytes(
    descriptor: &RecordDescriptor,
    record: &[u8],
    primary_key_field_indices: &[usize],
) -> Result<Vec<u8>, IvmRuntimeError> {
    let mut bytes = Vec::new();
    for primary_key_field_idx in primary_key_field_indices {
        encode_record_field_key_part(&mut bytes, descriptor, record, *primary_key_field_idx)?;
    }
    Ok(bytes)
}

pub(super) fn primary_key_field_indices(
    table: &TableSchema,
    descriptor: &RecordDescriptor,
) -> Result<Vec<usize>, IvmRuntimeError> {
    let primary_key = table
        .primary_key
        .as_ref()
        .ok_or_else(|| IvmRuntimeError::MissingPrimaryKey(table.name.clone()))?;
    primary_key
        .columns
        .iter()
        .map(|column| {
            descriptor
                .field_index(&column.column)
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(column.column.clone()))
        })
        .collect()
}

fn encode_record_field_key_part(
    key: &mut Vec<u8>,
    descriptor: &RecordDescriptor,
    record: &[u8],
    field_idx: usize,
) -> Result<(), IvmRuntimeError> {
    let field = descriptor
        .fields()
        .get(field_idx)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field_idx))?;
    let borrowed = descriptor.bind(record);
    match &field.value_type {
        ValueType::U8 => {
            key.push(0);
            key.push(borrowed.get_u8(field_idx)?);
            Ok(())
        }
        ValueType::U32 => {
            key.push(2);
            key.extend(borrowed.get_u32(field_idx)?.to_be_bytes());
            Ok(())
        }
        ValueType::U64 => {
            key.push(3);
            key.extend(borrowed.get_u64(field_idx)?.to_be_bytes());
            Ok(())
        }
        ValueType::I32 => {
            key.push(14);
            key.extend(order_preserving_i32_bits(borrowed.get_i32(field_idx)?).to_be_bytes());
            Ok(())
        }
        ValueType::I64 => {
            key.push(13);
            key.extend(order_preserving_i64_bits(borrowed.get_i64(field_idx)?).to_be_bytes());
            Ok(())
        }
        ValueType::F64 => {
            let value = borrowed.get_f64(field_idx)?;
            if value.is_nan() {
                return Err(IvmRuntimeError::RecordEncoding(
                    records::Error::InvalidF64NaN,
                ));
            }
            key.push(4);
            key.extend(order_preserving_f64_bits(value).to_be_bytes());
            Ok(())
        }
        ValueType::Bool => {
            key.push(5);
            key.push(u8::from(borrowed.get_bool(field_idx)?));
            Ok(())
        }
        ValueType::String => {
            key.push(6);
            encode_ordered_bytes(key, borrowed.get_str(field_idx)?.as_bytes());
            Ok(())
        }
        ValueType::Bytes => {
            key.push(7);
            encode_ordered_bytes(key, borrowed.get_bytes(field_idx)?);
            Ok(())
        }
        ValueType::Uuid => {
            key.push(10);
            key.extend_from_slice(borrowed.get_uuid(field_idx)?.as_bytes());
            Ok(())
        }
        ValueType::EnumTag(_) => {
            let value = borrowed.get_enum(field_idx)?;
            encode_key_part(key, &Value::U8(value))
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::U64) => {
            match borrowed.get_nullable_u64(field_idx)? {
                Some(value) => {
                    key.push(9);
                    key.push(3);
                    key.extend(value.to_be_bytes());
                }
                None => key.push(8),
            }
            Ok(())
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::I64) => {
            match borrowed.get_nullable_i64(field_idx)? {
                Some(value) => {
                    key.push(9);
                    key.push(13);
                    key.extend(order_preserving_i64_bits(value).to_be_bytes());
                }
                None => key.push(8),
            }
            Ok(())
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::I32) => {
            match borrowed.get_nullable_i32(field_idx)? {
                Some(value) => {
                    key.push(9);
                    key.push(14);
                    key.extend(order_preserving_i32_bits(value).to_be_bytes());
                }
                None => key.push(8),
            }
            Ok(())
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::F64) => {
            match borrowed.get_nullable_f64(field_idx)? {
                Some(value) => {
                    if value.is_nan() {
                        return Err(IvmRuntimeError::RecordEncoding(
                            records::Error::InvalidF64NaN,
                        ));
                    }
                    key.push(9);
                    key.push(4);
                    key.extend(order_preserving_f64_bits(value).to_be_bytes());
                }
                None => key.push(8),
            }
            Ok(())
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::String) => {
            match borrowed.get_nullable_string(field_idx)? {
                Some(value) => {
                    key.push(9);
                    key.push(6);
                    encode_ordered_bytes(key, value.as_bytes());
                }
                None => key.push(8),
            }
            Ok(())
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::Bytes) => {
            match borrowed.get_nullable_bytes(field_idx)? {
                Some(value) => {
                    key.push(9);
                    key.push(7);
                    encode_ordered_bytes(key, value);
                }
                None => key.push(8),
            }
            Ok(())
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::Uuid) => {
            match borrowed.get_nullable_uuid(field_idx)? {
                Some(value) => {
                    key.push(9);
                    key.push(10);
                    key.extend_from_slice(value.as_bytes());
                }
                None => key.push(8),
            }
            Ok(())
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::EnumTag(_)) => {
            match borrowed.get_nullable_enum(field_idx)? {
                Some(value) => {
                    encode_key_part(key, &Value::Nullable(Some(Box::new(Value::U8(value)))))
                }
                None => encode_key_part(key, &Value::Nullable(None)),
            }
        }
        _ => {
            let field_name = field
                .name
                .as_deref()
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
            let value = descriptor.get(record, field_name)?;
            encode_key_part(key, &value)
        }
    }
}

fn record_field_key_parts(
    descriptor: &RecordDescriptor,
    record: &[u8],
    field_idx: usize,
) -> Result<Vec<Vec<u8>>, IvmRuntimeError> {
    let field = descriptor
        .fields()
        .get(field_idx)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field_idx))?;
    match &field.value_type {
        ValueType::Array(_) => {
            let field_name = field
                .name
                .as_deref()
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
            let Value::Array(values) = descriptor.get(record, field_name)? else {
                return Err(IvmRuntimeError::GraphFieldNotFound(field_name.to_owned()));
            };
            values
                .into_iter()
                .map(|value| {
                    let mut key = Vec::new();
                    encode_key_part(&mut key, &value)?;
                    Ok(key)
                })
                .collect()
        }
        ValueType::Nullable(inner) if matches!(inner.as_ref(), ValueType::Array(_)) => {
            let field_name = field
                .name
                .as_deref()
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
            match descriptor.get(record, field_name)? {
                Value::Nullable(None) => {
                    let mut key = Vec::new();
                    encode_key_part(&mut key, &Value::Nullable(None))?;
                    Ok(vec![key])
                }
                Value::Nullable(Some(value)) => match *value {
                    Value::Array(values) => values
                        .into_iter()
                        .map(|value| {
                            let mut key = Vec::new();
                            encode_key_part(&mut key, &Value::Nullable(Some(Box::new(value))))?;
                            Ok(key)
                        })
                        .collect(),
                    value => {
                        let mut key = Vec::new();
                        encode_key_part(&mut key, &Value::Nullable(Some(Box::new(value))))?;
                        Ok(vec![key])
                    }
                },
                value => {
                    let mut key = Vec::new();
                    encode_key_part(&mut key, &value)?;
                    Ok(vec![key])
                }
            }
        }
        _ => {
            let mut key = Vec::new();
            encode_record_field_key_part(&mut key, descriptor, record, field_idx)?;
            Ok(vec![key])
        }
    }
}

fn compare_record_field(
    record: BorrowedRecord<'_>,
    field: &str,
    value: &LiteralValue,
    predicate: impl FnOnce(std::cmp::Ordering) -> bool,
    comparison: ValueComparison,
) -> Result<bool, IvmRuntimeError> {
    let field_idx = record
        .descriptor()
        .field_index(field)
        .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.to_owned()))?;
    match record_field_literal_ordering(record, field_idx, value)? {
        FieldLiteralOrdering::Compared(ordering) => return Ok(predicate(ordering)),
        FieldLiteralOrdering::SqlNull => return Ok(false),
        FieldLiteralOrdering::Unsupported => {}
    }
    let value = value.to_value();
    let actual = record.get(field)?;
    Ok(compare_values_sql(&actual, &value, comparison).is_some_and(predicate))
}

enum FieldLiteralOrdering {
    Compared(std::cmp::Ordering),
    SqlNull,
    Unsupported,
}

fn record_field_literal_ordering(
    record: BorrowedRecord<'_>,
    field_idx: usize,
    value: &LiteralValue,
) -> Result<FieldLiteralOrdering, IvmRuntimeError> {
    let field = record.field(field_idx)?;
    match (&field.value_type, value) {
        (ValueType::U8, LiteralValue::U8(expected)) => {
            Ok(ordering(&record.get_u8(field_idx)?, expected))
        }
        (ValueType::U32, LiteralValue::U32(expected)) => {
            Ok(ordering(&record.get_u32(field_idx)?, expected))
        }
        (ValueType::U64, LiteralValue::U64(expected)) => {
            Ok(ordering(&record.get_u64(field_idx)?, expected))
        }
        (ValueType::I32, LiteralValue::I32(expected)) => {
            Ok(ordering(&record.get_i32(field_idx)?, expected))
        }
        (ValueType::I64, LiteralValue::I64(expected)) => {
            Ok(ordering(&record.get_i64(field_idx)?, expected))
        }
        (ValueType::F64, LiteralValue::F64(expected)) => {
            let expected = f64::from_bits(*expected);
            Ok(record
                .get_f64(field_idx)?
                .partial_cmp(&expected)
                .map(FieldLiteralOrdering::Compared)
                .unwrap_or(FieldLiteralOrdering::SqlNull))
        }
        (ValueType::Bool, LiteralValue::Bool(expected)) => {
            Ok(ordering(&record.get_bool(field_idx)?, expected))
        }
        (ValueType::String, LiteralValue::String(expected)) => {
            Ok(ordering(record.get_str(field_idx)?, expected.as_str()))
        }
        (ValueType::Bytes, LiteralValue::Bytes(expected)) => {
            Ok(ordering(record.get_bytes(field_idx)?, expected.as_slice()))
        }
        (ValueType::Uuid, LiteralValue::Uuid(expected)) => Ok(record
            .get_uuid(field_idx)?
            .as_bytes()
            .partial_cmp(expected.as_bytes())
            .map(FieldLiteralOrdering::Compared)
            .unwrap_or(FieldLiteralOrdering::SqlNull)),
        (ValueType::EnumTag(_), LiteralValue::EnumTag(expected)) => {
            Ok(ordering(&record.get_enum(field_idx)?, expected))
        }
        (ValueType::Nullable(inner), LiteralValue::Nullable(Some(expected))) => {
            nullable_record_field_literal_ordering(record, field_idx, inner, expected)
        }
        (ValueType::Nullable(_), LiteralValue::Nullable(None)) => Ok(FieldLiteralOrdering::SqlNull),
        _ => Ok(FieldLiteralOrdering::Unsupported),
    }
}

fn nullable_record_field_literal_ordering(
    record: BorrowedRecord<'_>,
    field_idx: usize,
    inner: &ValueType,
    expected: &LiteralValue,
) -> Result<FieldLiteralOrdering, IvmRuntimeError> {
    match (inner, expected) {
        (ValueType::U64, LiteralValue::U64(expected)) => Ok(record
            .get_nullable_u64(field_idx)?
            .map(|actual| ordering(&actual, expected))
            .unwrap_or(FieldLiteralOrdering::SqlNull)),
        (ValueType::I64, LiteralValue::I64(expected)) => Ok(record
            .get_nullable_i64(field_idx)?
            .map(|actual| ordering(&actual, expected))
            .unwrap_or(FieldLiteralOrdering::SqlNull)),
        (ValueType::I32, LiteralValue::I32(expected)) => Ok(record
            .get_nullable_i32(field_idx)?
            .map(|actual| ordering(&actual, expected))
            .unwrap_or(FieldLiteralOrdering::SqlNull)),
        (ValueType::F64, LiteralValue::F64(expected)) => {
            let expected = f64::from_bits(*expected);
            Ok(record
                .get_nullable_f64(field_idx)?
                .and_then(|actual| actual.partial_cmp(&expected))
                .map(FieldLiteralOrdering::Compared)
                .unwrap_or(FieldLiteralOrdering::SqlNull))
        }
        (ValueType::String, LiteralValue::String(expected)) => Ok(record
            .get_nullable_string(field_idx)?
            .map(|actual| ordering(actual, expected.as_str()))
            .unwrap_or(FieldLiteralOrdering::SqlNull)),
        (ValueType::Bytes, LiteralValue::Bytes(expected)) => Ok(record
            .get_nullable_bytes(field_idx)?
            .map(|actual| ordering(actual, expected.as_slice()))
            .unwrap_or(FieldLiteralOrdering::SqlNull)),
        (ValueType::Uuid, LiteralValue::Uuid(expected)) => Ok(record
            .get_nullable_uuid(field_idx)?
            .and_then(|actual| actual.as_bytes().partial_cmp(expected.as_bytes()))
            .map(FieldLiteralOrdering::Compared)
            .unwrap_or(FieldLiteralOrdering::SqlNull)),
        (ValueType::EnumTag(_), LiteralValue::EnumTag(expected)) => Ok(record
            .get_nullable_enum(field_idx)?
            .map(|actual| ordering(&actual, expected))
            .unwrap_or(FieldLiteralOrdering::SqlNull)),
        _ => Ok(FieldLiteralOrdering::Unsupported),
    }
}

fn ordering<T: PartialOrd + ?Sized>(actual: &T, expected: &T) -> FieldLiteralOrdering {
    actual
        .partial_cmp(expected)
        .map(FieldLiteralOrdering::Compared)
        .unwrap_or(FieldLiteralOrdering::SqlNull)
}

fn compare_record_fields(
    record: BorrowedRecord<'_>,
    field: &str,
    value_field: &str,
    predicate: impl FnOnce(std::cmp::Ordering) -> bool,
    comparison: ValueComparison,
) -> Result<bool, IvmRuntimeError> {
    let left = record.get(field)?;
    let right = record.get(value_field)?;
    Ok(compare_values_sql(&left, &right, comparison).is_some_and(predicate))
}

fn contains_record_field(
    record: BorrowedRecord<'_>,
    field: &str,
    value: &LiteralValue,
    comparison: ValueComparison,
) -> Result<bool, IvmRuntimeError> {
    let needle = value.to_value();
    let haystack = record.get(field)?;
    Ok(value_contains_sql(&haystack, &needle, comparison))
}

fn contains_record_field_value(
    record: BorrowedRecord<'_>,
    field: &str,
    needle_field: &str,
    comparison: ValueComparison,
) -> Result<bool, IvmRuntimeError> {
    let haystack = record.get(field)?;
    let needle = record.get(needle_field)?;
    Ok(value_contains_sql(&haystack, &needle, comparison))
}

fn value_contains_sql(left: &Value, right: &Value, comparison: ValueComparison) -> bool {
    match (left, right) {
        (Value::Nullable(None), _) | (_, Value::Nullable(None)) => false,
        (Value::Nullable(Some(left)), right) => value_contains_sql(left, right, comparison),
        (left, Value::Nullable(Some(right))) => value_contains_sql(left, right, comparison),
        (Value::String(left), Value::String(right)) => left.contains(right),
        (Value::Array(values), right) => values.iter().any(|value| {
            compare_values_sql(value, right, comparison).is_some_and(std::cmp::Ordering::is_eq)
        }),
        _ => false,
    }
}

fn compare_values_sql(
    left: &Value,
    right: &Value,
    comparison: ValueComparison,
) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Nullable(None), _) | (_, Value::Nullable(None)) => None,
        (Value::Nullable(Some(left)), right) => compare_values_sql(left, right, comparison),
        (left, Value::Nullable(Some(right))) => compare_values_sql(left, right, comparison),
        _ => compare_values(left, right, comparison),
    }
}

fn is_sql_null_value(value: &Value) -> bool {
    match value {
        Value::Nullable(None) => true,
        Value::Nullable(Some(value)) => is_sql_null_value(value),
        _ => false,
    }
}

fn compare_values(
    left: &Value,
    right: &Value,
    comparison: ValueComparison,
) -> Option<std::cmp::Ordering> {
    if matches!(comparison, ValueComparison::Policy)
        && let (Some(left), Some(right)) = (integer_value(left), integer_value(right))
    {
        // i128 represents every supported integer exactly, including U64
        // values above i64::MAX. Floats deliberately do not participate: a
        // numeric-width match must never turn into lossy integer/float equality.
        return left.partial_cmp(&right);
    }
    match (left, right) {
        (Value::U8(left), Value::U8(right)) => left.partial_cmp(right),
        (Value::U16(left), Value::U16(right)) => left.partial_cmp(right),
        (Value::U32(left), Value::U32(right)) => left.partial_cmp(right),
        (Value::U64(left), Value::U64(right)) => left.partial_cmp(right),
        (Value::I32(left), Value::I32(right)) => left.partial_cmp(right),
        (Value::I64(left), Value::I64(right)) => left.partial_cmp(right),
        (Value::F64(left), Value::F64(right)) => left.partial_cmp(right),
        (Value::Bool(left), Value::Bool(right)) => left.partial_cmp(right),
        (Value::EnumTag(left), Value::EnumTag(right)) => left.partial_cmp(right),
        (Value::String(left), Value::String(right)) => left.partial_cmp(right),
        (Value::Bytes(left), Value::Bytes(right)) => left.partial_cmp(right),
        (Value::Uuid(left), Value::Uuid(right)) => left.as_bytes().partial_cmp(right.as_bytes()),
        (Value::Tuple(left), Value::Tuple(right)) => left
            .iter()
            .zip(right)
            .map(|(left, right)| compare_values(left, right, comparison))
            .find(|ordering| !matches!(ordering, Some(std::cmp::Ordering::Equal)))
            .unwrap_or_else(|| left.len().partial_cmp(&right.len())),
        (Value::Array(left), Value::Array(right)) => left
            .iter()
            .zip(right)
            .map(|(left, right)| compare_values(left, right, comparison))
            .find(|ordering| !matches!(ordering, Some(std::cmp::Ordering::Equal)))
            .unwrap_or_else(|| left.len().partial_cmp(&right.len())),
        _ => None,
    }
}

fn integer_value(value: &Value) -> Option<i128> {
    match value {
        Value::U8(value) => Some(i128::from(*value)),
        Value::U16(value) => Some(i128::from(*value)),
        Value::U32(value) => Some(i128::from(*value)),
        Value::U64(value) => Some(i128::from(*value)),
        Value::I32(value) => Some(i128::from(*value)),
        Value::I64(value) => Some(i128::from(*value)),
        _ => None,
    }
}

fn join_descriptor(left: &RecordDescriptor, right: &RecordDescriptor) -> RecordDescriptor {
    let fields = left
        .fields()
        .iter()
        .filter_map(|field| {
            Some((
                format!("left.{}", field.name.as_ref()?),
                field.value_type.clone(),
            ))
        })
        .chain(right.fields().iter().filter_map(|field| {
            Some((
                format!("right.{}", field.name.as_ref()?),
                field.value_type.clone(),
            ))
        }))
        .collect::<Vec<_>>();

    RecordDescriptor::new(fields)
}

pub(crate) fn encode_key_part(key: &mut Vec<u8>, value: &Value) -> Result<(), IvmRuntimeError> {
    // Type tags make composite keys unambiguous. Payload bytes are chosen to
    // preserve natural ordering in RocksDB's lexicographic iterator order.
    match value {
        Value::U8(value) => {
            key.push(0);
            key.push(*value);
        }
        Value::U16(value) => {
            key.push(1);
            key.extend(value.to_be_bytes());
        }
        Value::U32(value) => {
            key.push(2);
            key.extend(value.to_be_bytes());
        }
        Value::U64(value) => {
            key.push(3);
            key.extend(value.to_be_bytes());
        }
        Value::I32(value) => {
            key.push(14);
            key.extend(order_preserving_i32_bits(*value).to_be_bytes());
        }
        Value::I64(value) => {
            key.push(13);
            key.extend(order_preserving_i64_bits(*value).to_be_bytes());
        }
        Value::F64(value) => {
            if value.is_nan() {
                return Err(IvmRuntimeError::RecordEncoding(
                    records::Error::InvalidF64NaN,
                ));
            }
            key.push(4);
            key.extend(order_preserving_f64_bits(*value).to_be_bytes());
        }
        Value::Bool(value) => {
            key.push(5);
            key.push(u8::from(*value));
        }
        Value::String(value) => {
            key.push(6);
            encode_ordered_bytes(key, value.as_bytes());
        }
        Value::Bytes(value) => {
            key.push(7);
            encode_ordered_bytes(key, value);
        }
        Value::Uuid(value) => {
            key.push(10);
            key.extend_from_slice(value.as_bytes());
        }
        Value::Tuple(values) => {
            key.push(11);
            for value in values {
                encode_key_part(key, value)?;
            }
        }
        Value::EnumTag(value) => {
            key.push(0);
            key.push(*value);
        }
        Value::Nullable(None) => {
            key.push(8);
        }
        Value::Nullable(Some(value)) => {
            key.push(9);
            encode_key_part(key, value)?;
        }
        Value::Array(_) | Value::Record(_) | Value::Enum(_) => {
            return Err(IvmRuntimeError::UnsupportedJoinKey);
        }
    }
    Ok(())
}

fn order_preserving_f64_bits(value: f64) -> u64 {
    let bits = value.to_bits();
    // Flip positive signs and invert negatives; the resulting unsigned integer
    // sorts like IEEE numeric order for non-NaN values.
    if bits & (1 << 63) == 0 {
        bits ^ (1 << 63)
    } else {
        !bits
    }
}

fn order_preserving_i64_bits(value: i64) -> u64 {
    (value as u64) ^ (1_u64 << 63)
}

fn order_preserving_i32_bits(value: i32) -> u32 {
    (value as u32) ^ (1_u32 << 31)
}

fn encode_ordered_bytes(key: &mut Vec<u8>, value: &[u8]) {
    // 0x00 terminates the byte string; embedded NULs are escaped as 00 ff.
    for byte in value {
        if *byte == 0 {
            key.extend([0, 0xff]);
        } else {
            key.push(*byte);
        }
    }
    key.extend([0, 0]);
}

pub(super) fn project_binding_source_deltas(
    input: &RecordDeltas,
    output_desc: &RecordDescriptor,
) -> Result<RecordDeltas, IvmRuntimeError> {
    if input.descriptor == *output_desc {
        return Ok(input.clone());
    }
    let mapping = output_desc
        .fields()
        .iter()
        .map(|field| {
            let name = field
                .name
                .as_ref()
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound("<unnamed>".to_owned()))?;
            input
                .descriptor
                .field_index(name)
                .map(|index| (0, index))
                .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(name.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let deltas = input
        .deltas
        .iter()
        .map(|delta| {
            Ok(RecordDelta {
                record: output_desc
                    .project_record_raw(
                        std::slice::from_ref(&input.descriptor),
                        &[delta.raw()],
                        &mapping,
                    )?
                    .into(),
                weight: delta.weight,
            })
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
    Ok(RecordDeltas {
        descriptor: *output_desc,
        deltas,
    })
}

fn consolidate_deltas(deltas: Vec<RecordDelta>) -> Vec<RecordDelta> {
    if deltas.len() <= 1 {
        return deltas;
    }
    let mut deltas = deltas;
    deltas.sort_unstable_by(|left, right| left.record.cmp(&right.record));
    let mut consolidated = Vec::<RecordDelta>::with_capacity(deltas.len());
    for delta in deltas {
        if let Some(last) = consolidated.last_mut()
            && last.record == delta.record
        {
            last.weight += delta.weight;
            continue;
        }
        consolidated.push(delta);
    }
    consolidated
        .into_iter()
        .filter(|delta| delta.weight != 0)
        .collect()
}

fn resolve_aggregate_expr(
    input: &RecordDescriptor,
    aggregate: &AggregateExpr,
) -> Result<AggregateExpr, IvmRuntimeError> {
    let expression = match &aggregate.expression {
        Some(PlanExpr::Field(field)) => Some(PlanExpr::Field(resolve_field_name(input, field)?)),
        Some(PlanExpr::Nullable(field)) => {
            Some(PlanExpr::Nullable(resolve_field_name(input, field)?))
        }
        Some(PlanExpr::NullableFlat(field)) => {
            Some(PlanExpr::NullableFlat(resolve_field_name(input, field)?))
        }
        Some(PlanExpr::EnumTagRemap { field, tags }) => Some(PlanExpr::EnumTagRemap {
            field: resolve_field_name(input, field)?,
            tags: tags.clone(),
        }),
        Some(PlanExpr::EnumRemap { field, tags }) => Some(PlanExpr::EnumRemap {
            field: resolve_field_name(input, field)?,
            tags: tags.clone(),
        }),
        Some(PlanExpr::RecursiveEnumRemap { field, remaps }) => {
            Some(PlanExpr::RecursiveEnumRemap {
                field: resolve_field_name(input, field)?,
                remaps: remaps.clone(),
            })
        }
        Some(PlanExpr::Literal(_)) | Some(PlanExpr::Null(_)) | None => aggregate.expression.clone(),
    };
    Ok(AggregateExpr {
        function: aggregate.function.clone(),
        expression,
        distinct: aggregate.distinct,
        output_name: aggregate.output_name.clone(),
    })
}

fn resolve_field_name(input: &RecordDescriptor, field: &str) -> Result<String, IvmRuntimeError> {
    let field_idx = input
        .field_index(field)
        .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(field.to_owned()))?;
    field_name_at(input, field_idx)
}

fn records_before_from_deltas(
    after_records: Vec<(Bytes, i64)>,
    deltas: Vec<RecordDelta>,
) -> Vec<(Bytes, i64)> {
    let mut records = BTreeMap::<Bytes, (Bytes, i64)>::new();
    for (record, weight) in after_records {
        records.insert(record.clone(), (record, weight));
    }
    for delta in deltas {
        let entry = records
            .entry(delta.record.clone())
            .or_insert_with(|| (delta.record.clone(), 0));
        entry.1 -= delta.weight;
    }
    records
        .into_iter()
        .filter_map(|(_, (record, weight))| (weight > 0).then_some((record, weight)))
        .collect()
}

fn aggregate_row_from_records(
    input_desc: RecordDescriptor,
    output_desc: RecordDescriptor,
    aggregate: &AggregateOp,
    records: &[(Bytes, i64)],
) -> Result<Option<Bytes>, IvmRuntimeError> {
    let mut positive = Vec::new();
    let mut total_weight = 0_i64;
    for (record, weight) in records {
        if *weight < 0 {
            return Err(IvmRuntimeError::UnsupportedOperator);
        }
        if *weight > 0 {
            total_weight += *weight;
            positive.push((record.as_ref(), *weight));
        }
    }
    if total_weight == 0 && !aggregate.group_key.is_empty() {
        return Ok(None);
    }

    let mut values = Vec::new();
    if let Some((first, _)) = positive.first() {
        let first = BorrowedRecord::new(first, &input_desc);
        for group_expr in &aggregate.group_key {
            values.push(evaluate_aggregate_expr(&first, group_expr)?);
        }
    }
    for aggregate_expr in &aggregate.aggregates {
        values.push(evaluate_aggregate(records, input_desc, aggregate_expr)?);
    }
    output_desc
        .create(&values)
        .map(Bytes::from)
        .map(Some)
        .map_err(IvmRuntimeError::RecordEncoding)
}

fn evaluate_aggregate(
    records: &[(Bytes, i64)],
    input_desc: RecordDescriptor,
    aggregate: &AggregateExpr,
) -> Result<Value, IvmRuntimeError> {
    if aggregate.distinct {
        return Err(IvmRuntimeError::UnsupportedOperator);
    }
    match aggregate.function {
        AggregateFunction::Count => {
            let mut count = 0_u64;
            for (record, weight) in records {
                if *weight <= 0 {
                    continue;
                }
                if let Some(expr) = &aggregate.expression {
                    let value =
                        evaluate_aggregate_expr(&BorrowedRecord::new(record, &input_desc), expr)?;
                    if is_null_value(&value) {
                        continue;
                    }
                }
                count = count
                    .checked_add(
                        u64::try_from(*weight).map_err(|_| IvmRuntimeError::UnsupportedOperator)?,
                    )
                    .ok_or(IvmRuntimeError::UnsupportedOperator)?;
            }
            Ok(Value::U64(count))
        }
        AggregateFunction::Sum => {
            aggregate_sum(records, input_desc, aggregate).map(nullable_aggregate_value)
        }
        AggregateFunction::Avg => {
            aggregate_avg(records, input_desc, aggregate).map(nullable_aggregate_value)
        }
        AggregateFunction::Min | AggregateFunction::Max => {
            aggregate_extremum(records, input_desc, aggregate).map(nullable_aggregate_value)
        }
    }
}

fn aggregate_sum(
    records: &[(Bytes, i64)],
    input_desc: RecordDescriptor,
    aggregate: &AggregateExpr,
) -> Result<Option<Value>, IvmRuntimeError> {
    let Some(expr) = &aggregate.expression else {
        return Err(IvmRuntimeError::UnsupportedOperator);
    };
    let mut kind = None;
    let mut u64_sum = 0_u64;
    let mut i64_sum = 0_i64;
    let mut f64_sum = 0_f64;
    for (record, weight) in records {
        if *weight <= 0 {
            continue;
        }
        let value = evaluate_aggregate_expr(&BorrowedRecord::new(record, &input_desc), expr)?;
        let Some(value) = unwrap_nullable_value(value) else {
            continue;
        };
        match value {
            Value::U8(value) => {
                kind.get_or_insert(ValueType::U8);
                u64_sum = add_weighted_u64(u64_sum, u64::from(value), *weight)?;
            }
            Value::U16(value) => {
                kind.get_or_insert(ValueType::U16);
                u64_sum = add_weighted_u64(u64_sum, u64::from(value), *weight)?;
            }
            Value::U32(value) => {
                kind.get_or_insert(ValueType::U32);
                u64_sum = add_weighted_u64(u64_sum, u64::from(value), *weight)?;
            }
            Value::U64(value) => {
                kind.get_or_insert(ValueType::U64);
                u64_sum = add_weighted_u64(u64_sum, value, *weight)?;
            }
            Value::I32(value) => {
                kind.get_or_insert(ValueType::I32);
                i64_sum = add_weighted_i64(i64_sum, i64::from(value), *weight)?;
            }
            Value::I64(value) => {
                kind.get_or_insert(ValueType::I64);
                i64_sum = add_weighted_i64(i64_sum, value, *weight)?;
            }
            Value::F64(value) => {
                kind.get_or_insert(ValueType::F64);
                f64_sum += value * (*weight as f64);
            }
            _ => return Err(IvmRuntimeError::UnsupportedOperator),
        }
    }
    match kind {
        None => Ok(None),
        Some(ValueType::U8) => u8::try_from(u64_sum)
            .map(Value::U8)
            .map(Some)
            .map_err(|_| IvmRuntimeError::AggregateOverflow),
        Some(ValueType::U16) => u16::try_from(u64_sum)
            .map(Value::U16)
            .map(Some)
            .map_err(|_| IvmRuntimeError::AggregateOverflow),
        Some(ValueType::U32) => u32::try_from(u64_sum)
            .map(Value::U32)
            .map(Some)
            .map_err(|_| IvmRuntimeError::AggregateOverflow),
        Some(ValueType::U64) => Ok(Some(Value::U64(u64_sum))),
        Some(ValueType::I32) => i32::try_from(i64_sum)
            .map(Value::I32)
            .map(Some)
            .map_err(|_| IvmRuntimeError::AggregateOverflow),
        Some(ValueType::F64) => Ok(Some(Value::F64(f64_sum))),
        Some(ValueType::I64) => Ok(Some(Value::I64(i64_sum))),
        Some(_) => Err(IvmRuntimeError::UnsupportedOperator),
    }
}

fn aggregate_avg(
    records: &[(Bytes, i64)],
    input_desc: RecordDescriptor,
    aggregate: &AggregateExpr,
) -> Result<Option<Value>, IvmRuntimeError> {
    let Some(expr) = &aggregate.expression else {
        return Err(IvmRuntimeError::UnsupportedOperator);
    };
    let mut sum = 0_f64;
    let mut count = 0_i64;
    for (record, weight) in records {
        if *weight <= 0 {
            continue;
        }
        let value = evaluate_aggregate_expr(&BorrowedRecord::new(record, &input_desc), expr)?;
        let Some(value) = unwrap_nullable_value(value) else {
            continue;
        };
        let numeric = numeric_value_as_f64(&value)?;
        sum += numeric * (*weight as f64);
        count += *weight;
    }
    if count <= 0 {
        return Ok(None);
    }
    Ok(Some(Value::F64(sum / (count as f64))))
}

fn aggregate_extremum(
    records: &[(Bytes, i64)],
    input_desc: RecordDescriptor,
    aggregate: &AggregateExpr,
) -> Result<Option<Value>, IvmRuntimeError> {
    let Some(expr) = &aggregate.expression else {
        return Err(IvmRuntimeError::UnsupportedOperator);
    };
    let mut best: Option<(Vec<u8>, Bytes, Value)> = None;
    for (record, weight) in records {
        if *weight <= 0 {
            continue;
        }
        let value = evaluate_aggregate_expr(&BorrowedRecord::new(record, &input_desc), expr)?;
        let Some(value) = unwrap_nullable_value(value) else {
            continue;
        };
        let mut value_key = Vec::new();
        encode_key_part(&mut value_key, &value)?;
        let replaces =
            best.as_ref()
                .is_none_or(|(best_key, best_record, _)| match aggregate.function {
                    AggregateFunction::Min => {
                        value_key < *best_key || (value_key == *best_key && record < best_record)
                    }
                    AggregateFunction::Max => {
                        value_key > *best_key || (value_key == *best_key && record < best_record)
                    }
                    _ => false,
                });
        if replaces {
            best = Some((value_key, record.clone(), value));
        }
    }
    Ok(best.map(|(_, _, value)| value))
}

fn add_weighted_u64(current: u64, value: u64, weight: i64) -> Result<u64, IvmRuntimeError> {
    let weight = u64::try_from(weight).map_err(|_| IvmRuntimeError::AggregateOverflow)?;
    current
        .checked_add(
            value
                .checked_mul(weight)
                .ok_or(IvmRuntimeError::AggregateOverflow)?,
        )
        .ok_or(IvmRuntimeError::AggregateOverflow)
}

fn add_weighted_i64(current: i64, value: i64, weight: i64) -> Result<i64, IvmRuntimeError> {
    current
        .checked_add(
            value
                .checked_mul(weight)
                .ok_or(IvmRuntimeError::AggregateOverflow)?,
        )
        .ok_or(IvmRuntimeError::AggregateOverflow)
}

fn numeric_value_as_f64(value: &Value) -> Result<f64, IvmRuntimeError> {
    match value {
        Value::U8(value) => Ok(f64::from(*value)),
        Value::U16(value) => Ok(f64::from(*value)),
        Value::U32(value) => Ok(f64::from(*value)),
        Value::U64(value) => Ok(*value as f64),
        Value::I32(value) => Ok(f64::from(*value)),
        Value::I64(value) => Ok(*value as f64),
        Value::F64(value) => Ok(*value),
        _ => Err(IvmRuntimeError::UnsupportedOperator),
    }
}

fn nullable_aggregate_value(value: Option<Value>) -> Value {
    Value::Nullable(value.map(Box::new))
}

fn unwrap_nullable_value(value: Value) -> Option<Value> {
    match value {
        Value::Nullable(None) => None,
        Value::Nullable(Some(value)) => Some(*value),
        value => Some(value),
    }
}

fn is_null_value(value: &Value) -> bool {
    matches!(value, Value::Nullable(None))
}

fn evaluate_aggregate_expr(
    record: &BorrowedRecord<'_>,
    expr: &PlanExpr,
) -> Result<Value, IvmRuntimeError> {
    match expr {
        PlanExpr::Field(field) | PlanExpr::Nullable(field) | PlanExpr::NullableFlat(field) => {
            record.get(field).map_err(IvmRuntimeError::RecordEncoding)
        }
        PlanExpr::EnumTagRemap { field, tags } => remap_enum_tag(record.get(field)?, tags),
        PlanExpr::EnumRemap { field, tags } => remap_enum(record.get(field)?, tags),
        // Aggregates do not carry an output descriptor, so there is no
        // well-defined target boundary for a recursive re-encoding.
        PlanExpr::RecursiveEnumRemap { .. } => Err(IvmRuntimeError::UnsupportedOperator),
        PlanExpr::Literal(literal) => Ok(literal.to_value()),
        PlanExpr::Null(value_type) => Ok(Value::Nullable(match value_type {
            ValueType::Nullable(_) => None,
            _ => None,
        })),
    }
}

type SourceRecord = (Vec<u8>, Bytes);
type WindowedRecord = (Bytes, i64);

#[derive(Clone, Debug)]
struct TopBySortPart {
    key: Value,
    direction: TopByDirection,
}

impl PartialEq for TopBySortPart {
    fn eq(&self, other: &Self) -> bool {
        self.direction == other.direction
            && compare_values_sql(&self.key, &other.key, ValueComparison::Exact)
                == Some(std::cmp::Ordering::Equal)
    }
}

impl Eq for TopBySortPart {}

impl Ord for TopBySortPart {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.direction {
            TopByDirection::Asc => {
                compare_values_sql(&self.key, &other.key, ValueComparison::Exact)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
            TopByDirection::Desc => {
                compare_values_sql(&other.key, &self.key, ValueComparison::Exact)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
        }
    }
}

impl PartialOrd for TopBySortPart {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy)]
enum ArgByDirection {
    Min,
    Max,
}

struct ArgBySpec<'a> {
    group_fields: &'a [String],
    group_field_indices: &'a [usize],
    primary_key_field_indices: &'a [usize],
    direction: ArgByDirection,
}

fn arg_by_winner_from_records(
    descriptor: RecordDescriptor,
    primary_key_field_indices: &[usize],
    records: Vec<(Bytes, i64)>,
    direction: ArgByDirection,
) -> Result<Option<SourceRecord>, IvmRuntimeError> {
    let mut winner = None;
    for (record, weight) in records {
        if weight <= 0 {
            continue;
        }
        let key = encoded_record_key_part(descriptor, &record, primary_key_field_indices)?;
        let replaces =
            winner
                .as_ref()
                .is_none_or(|(winner_key, _): &SourceRecord| match direction {
                    ArgByDirection::Min => key < *winner_key,
                    ArgByDirection::Max => key > *winner_key,
                });
        if replaces {
            winner = Some((key, record));
        }
    }
    Ok(winner)
}

fn arg_by_winner_before_from_deltas(
    descriptor: RecordDescriptor,
    primary_key_field_indices: &[usize],
    after_records: Vec<(Bytes, i64)>,
    deltas: Vec<RecordDelta>,
    direction: ArgByDirection,
) -> Result<Option<SourceRecord>, IvmRuntimeError> {
    let mut records = BTreeMap::<Vec<u8>, (Bytes, i64)>::new();
    for (record, weight) in after_records {
        let key = encoded_record_key_part(descriptor, &record, primary_key_field_indices)?;
        records.insert(key, (record, weight));
    }
    for delta in deltas {
        let key = encoded_record_key_part(descriptor, delta.raw(), primary_key_field_indices)?;
        let entry = records
            .entry(key)
            .or_insert_with(|| (delta.record.clone(), 0));
        entry.1 -= delta.weight;
    }
    let mut positive = records
        .into_iter()
        .filter_map(|(key, (record, weight))| (weight > 0).then_some((key, record)));
    Ok(match direction {
        ArgByDirection::Min => positive.next(),
        ArgByDirection::Max => positive.next_back(),
    })
}

fn top_by_window_from_records(
    descriptor: RecordDescriptor,
    records: Vec<(Bytes, i64)>,
    top_by: &TopByOp,
) -> Result<Vec<WindowedRecord>, IvmRuntimeError> {
    let mut ranked = Vec::new();
    for (record, weight) in records {
        if weight > 0 {
            ranked.push((
                top_by_sort_key(descriptor, &record, top_by)?,
                record,
                weight,
            ));
        }
    }
    // Full record bytes are the final tie-breaker (INV-QUERY-23); the total
    // order must not depend on arrangement iteration order.
    ranked.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

    // Bag semantics (INV-QUERY-24/25): a record with multiplicity m occupies m
    // ordinals, and offset/limit consume copies, not distinct records.
    let mut window = Vec::new();
    let mut to_skip = top_by.offset;
    let mut remaining = match top_by.limit {
        TopByLimit::Finite(limit) => Some(limit),
        TopByLimit::Unbounded => None,
    };
    for (_, record, weight) in ranked {
        if remaining == Some(0) {
            break;
        }
        let copies = weight as u64;
        let available = copies.saturating_sub(to_skip);
        to_skip = to_skip.saturating_sub(copies);
        let taken = remaining.map_or(available, |remaining| available.min(remaining));
        if taken > 0 {
            window.push((
                record,
                i64::try_from(taken).expect("taken copies cannot exceed positive record weight"),
            ));
            if let Some(remaining) = &mut remaining {
                *remaining -= taken;
            }
        }
    }
    Ok(window)
}

fn top_by_window_before_from_deltas(
    descriptor: RecordDescriptor,
    after_records: Vec<(Bytes, i64)>,
    deltas: Vec<RecordDelta>,
    top_by: &TopByOp,
) -> Result<Vec<WindowedRecord>, IvmRuntimeError> {
    // Reconstruct the pre-tick multiset keyed by record bytes — the same
    // identity the arrangement consolidates by. Keying by sort key would
    // collapse distinct records that tie through (order_cols, tie_cols).
    let mut records = BTreeMap::<Bytes, i64>::new();
    for (record, weight) in after_records {
        *records.entry(record).or_default() += weight;
    }
    for delta in deltas {
        *records.entry(delta.record.clone()).or_default() -= delta.weight;
    }
    top_by_window_from_records(descriptor, records.into_iter().collect(), top_by)
}

fn records_before_deltas(
    after_records: Vec<(Bytes, i64)>,
    deltas: &[RecordDelta],
) -> Vec<(Bytes, i64)> {
    let mut records = BTreeMap::<Bytes, i64>::new();
    for (record, weight) in after_records {
        *records.entry(record).or_default() += weight;
    }
    for delta in deltas {
        *records.entry(delta.record.clone()).or_default() -= delta.weight;
    }
    records.into_iter().collect()
}

fn collect_by_projection_value_type(
    input_desc: RecordDescriptor,
    field: &CollectByProjection,
) -> Result<ValueType, IvmRuntimeError> {
    let value_type = input_desc
        .fields()
        .get(field.field_idx)
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field.field_idx))?
        .value_type
        .clone();
    if !field.unwrap_nullable {
        return Ok(value_type);
    }
    Ok(match value_type {
        ValueType::Nullable(inner) => *inner,
        value_type => value_type,
    })
}

fn collect_by_output_value(output_type: &ValueType, value: Value) -> Value {
    match (output_type, value) {
        (ValueType::Nullable(_), value @ Value::Nullable(_)) => value,
        (ValueType::Nullable(_), value) => Value::Nullable(Some(Box::new(value))),
        (_, value) => value,
    }
}

fn collect_by_projected_value(
    values: &[Value],
    field: &CollectByProjection,
) -> Result<Value, IvmRuntimeError> {
    let value = values
        .get(field.field_idx)
        .cloned()
        .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(field.field_idx))?;
    if !field.unwrap_nullable {
        return Ok(value);
    }
    match value {
        Value::Nullable(Some(value)) => Ok(*value),
        // A present child can legitimately carry an application NULL. Keep
        // it saturated at NULL; descriptor validation will reject this value
        // when the requested output field is non-nullable.
        Value::Nullable(None) => Ok(Value::Nullable(None)),
        value => Ok(value),
    }
}

fn update_unbounded_collect_by_terminal_state(
    input_desc: RecordDescriptor,
    output_desc: RecordDescriptor,
    collect_by: &CollectByOp,
    direct_tree_slot: Option<&CollectBySlot>,
    state: &mut CollectByIncrementalState,
    deltas: &[RecordDelta],
    emit: bool,
) -> Result<Vec<TerminalOperation>, IvmRuntimeError> {
    if collect_by.mode == CollectByMode::Root {
        return update_collect_by_root_terminal_state(
            input_desc,
            output_desc,
            collect_by,
            state,
            deltas,
            emit,
        );
    }
    if !emit {
        state.groups.clear();
        state.roots.clear();
    }
    let root_groups_before = direct_tree_slot
        .is_none()
        .then(|| state.groups.keys().cloned().collect::<BTreeSet<_>>());
    let mut operations = Vec::new();
    for delta in deltas {
        let group_key =
            encoded_record_key_part(input_desc, delta.raw(), &collect_by.group_field_indices)?;
        if let Some(presence_field) = direct_tree_slot.and_then(|slot| slot.presence_field_index)
            && !BorrowedRecord::new(delta.raw(), &input_desc).get_bool(presence_field)?
        {
            let state_key = (
                collect_by_sort_key(input_desc, delta.raw(), collect_by)?,
                delta.record.clone(),
            );
            let before_weight = state.roots.get(&state_key).copied().unwrap_or_default();
            let after_weight = before_weight + delta.weight;
            if after_weight == 0 {
                state.roots.remove(&state_key);
            } else {
                state.roots.insert(state_key.clone(), after_weight);
            }
            if !emit || (before_weight > 0) == (after_weight > 0) {
                continue;
            }
            if after_weight > 0 {
                let index = state
                    .roots
                    .range(..state_key)
                    .filter(|(_, weight)| **weight > 0)
                    .count();
                let record = collect_by_tree_parent_from_records(
                    input_desc,
                    output_desc,
                    collect_by,
                    &[(delta.record.clone(), 1)],
                )?
                .ok_or_else(|| {
                    IvmRuntimeError::InvalidCollectBy(
                        "root anchor did not render a terminal row".to_owned(),
                    )
                })?;
                operations.push(TerminalOperation {
                    root_key: group_key.clone(),
                    path: Vec::new(),
                    edit: TerminalEdit::Insert {
                        index,
                        key: group_key,
                        value: record.to_vec(),
                    },
                });
            } else {
                operations.push(TerminalOperation {
                    root_key: group_key.clone(),
                    path: Vec::new(),
                    edit: TerminalEdit::Remove { key: group_key },
                });
            }
            continue;
        }
        let (sort_field_indices, sort_directions) = direct_tree_slot.map_or(
            (
                collect_by.sort_field_indices.as_slice(),
                collect_by.sort_directions.as_slice(),
            ),
            |slot| {
                (
                    slot.sort_field_indices.as_slice(),
                    slot.sort_directions.as_slice(),
                )
            },
        );
        let sort_key = collect_by_sort_key_for_fields(
            input_desc,
            delta.raw(),
            sort_field_indices,
            sort_directions,
        )?;
        let state_key = (sort_key, delta.record.clone());
        let group = state.groups.entry(group_key.clone()).or_default();
        let before_weight = group.get(&state_key).copied().unwrap_or_default();
        let before_index = (before_weight > 0).then(|| {
            group
                .range(..state_key.clone())
                .filter(|(_, weight)| **weight > 0)
                .count()
        });
        let after_weight = before_weight + delta.weight;
        if after_weight == 0 {
            group.remove(&state_key);
        } else {
            group.insert(state_key.clone(), after_weight);
        }
        if !emit || (before_weight > 0) == (after_weight > 0) {
            continue;
        }
        let source_values = BorrowedRecord::new(delta.raw(), &input_desc)
            .to_values()
            .map_err(IvmRuntimeError::RecordEncoding)?;
        let child_fields = direct_tree_slot
            .map(|slot| slot.child_fields.as_slice())
            .unwrap_or(collect_by.child_fields.as_slice());
        let child_descriptor = direct_tree_slot
            .map(|slot| slot.child_descriptor)
            .unwrap_or(collect_by.child_descriptor);
        let collection_field = direct_tree_slot
            .map(|slot| slot.collection_field.as_str())
            .unwrap_or(collect_by.collection_field.as_str());
        let child_values = child_fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                Ok::<Value, IvmRuntimeError>(collect_by_output_value(
                    &child_descriptor.fields()[index].value_type,
                    collect_by_projected_value(&source_values, field)?,
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let child_record: Vec<u8> = child_descriptor.create(&child_values)?;
        let child_key = encoded_record_key_part(child_descriptor, &child_record, &[0])?;
        let path = vec![TerminalPathSegment::Collection(collection_field.to_owned())];
        if after_weight > 0 {
            let index = group
                .range(..state_key)
                .filter(|(_, weight)| **weight > 0)
                .count();
            operations.push(TerminalOperation {
                root_key: group_key,
                path,
                edit: TerminalEdit::Insert {
                    index,
                    key: child_key,
                    value: child_record,
                },
            });
        } else {
            operations.push(TerminalOperation {
                root_key: group_key,
                path,
                edit: TerminalEdit::Remove { key: child_key },
            });
            debug_assert!(before_index.is_some());
        }
    }
    state.groups.retain(|_, group| !group.is_empty());
    if let Some(root_groups_before) = root_groups_before {
        let root_groups_after = state.groups.keys().cloned().collect::<BTreeSet<_>>();
        operations.retain(|operation| {
            root_groups_before.contains(&operation.root_key)
                && root_groups_after.contains(&operation.root_key)
        });
        for root_key in root_groups_before.difference(&root_groups_after) {
            operations.push(TerminalOperation {
                root_key: root_key.clone(),
                path: Vec::new(),
                edit: TerminalEdit::Remove {
                    key: root_key.clone(),
                },
            });
        }
        for root_key in root_groups_after.difference(&root_groups_before) {
            let group = state
                .groups
                .get(root_key)
                .expect("root key came from collect state");
            let records = group
                .iter()
                .map(|((_, record), weight)| (record.clone(), *weight))
                .collect::<Vec<_>>();
            let record =
                collect_by_parent_from_records(input_desc, output_desc, collect_by, &records)?
                    .ok_or_else(|| {
                        IvmRuntimeError::InvalidCollectBy(
                            "new collect root did not render a terminal row".to_owned(),
                        )
                    })?;
            let index = root_groups_after.range(..root_key.clone()).count();
            operations.push(TerminalOperation {
                root_key: root_key.clone(),
                path: Vec::new(),
                edit: TerminalEdit::Insert {
                    index,
                    key: root_key.clone(),
                    value: record.to_vec(),
                },
            });
        }
    }
    Ok(operations)
}

fn update_collect_by_root_terminal_state(
    input_desc: RecordDescriptor,
    output_desc: RecordDescriptor,
    collect_by: &CollectByOp,
    state: &mut CollectByIncrementalState,
    deltas: &[RecordDelta],
    emit: bool,
) -> Result<Vec<TerminalOperation>, IvmRuntimeError> {
    if !emit {
        state.groups.clear();
        state.roots.clear();
    }
    let mut before = BTreeMap::<Vec<u8>, Option<Bytes>>::new();
    for delta in deltas {
        let group_key =
            encoded_record_key_part(input_desc, delta.raw(), &collect_by.group_field_indices)?;
        if emit && !before.contains_key(&group_key) {
            let rendered = state.groups.get(&group_key).map_or(Ok(None), |group| {
                let records = group
                    .iter()
                    .map(|((_, record), weight)| (record.clone(), *weight))
                    .collect::<Vec<_>>();
                collect_by_root_from_records(input_desc, output_desc, collect_by, &records)
            })?;
            before.insert(group_key.clone(), rendered);
        }
        let sort_key = collect_by_sort_key(input_desc, delta.raw(), collect_by)?;
        let state_key = (sort_key, delta.record.clone());
        let group = state.groups.entry(group_key).or_default();
        let weight = group.get(&state_key).copied().unwrap_or_default() + delta.weight;
        if weight == 0 {
            group.remove(&state_key);
        } else {
            group.insert(state_key, weight);
        }
    }
    state.groups.retain(|_, group| !group.is_empty());
    if !emit {
        return Ok(Vec::new());
    }

    let mut operations = Vec::new();
    for (root_key, before_record) in before {
        let after_record = state.groups.get(&root_key).map_or(Ok(None), |group| {
            let records = group
                .iter()
                .map(|((_, record), weight)| (record.clone(), *weight))
                .collect::<Vec<_>>();
            collect_by_root_from_records(input_desc, output_desc, collect_by, &records)
        })?;
        if before_record == after_record {
            continue;
        }
        let edit = match (before_record, after_record) {
            (None, Some(record)) => TerminalEdit::Insert {
                index: state
                    .groups
                    .keys()
                    .take_while(|key| *key < &root_key)
                    .count(),
                key: root_key.clone(),
                value: record.to_vec(),
            },
            (Some(_), Some(record)) => TerminalEdit::Update {
                key: root_key.clone(),
                value: record.to_vec(),
            },
            (Some(_), None) => TerminalEdit::Remove {
                key: root_key.clone(),
            },
            (None, None) => continue,
        };
        operations.push(TerminalOperation {
            root_key,
            path: Vec::new(),
            edit,
        });
    }
    Ok(operations)
}

fn collect_by_root_from_records(
    input_desc: RecordDescriptor,
    output_desc: RecordDescriptor,
    collect_by: &CollectByOp,
    records: &[(Bytes, i64)],
) -> Result<Option<Bytes>, IvmRuntimeError> {
    let Some((parent_record, _)) = records.iter().find(|(_, weight)| *weight > 0) else {
        return Ok(None);
    };
    let parent_values = BorrowedRecord::new(parent_record, &input_desc)
        .to_values()
        .map_err(|error| {
            IvmRuntimeError::InvalidCollectBy(format!("root input decode failed: {error}"))
        })?;
    let values = collect_by
        .parent_fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let value = collect_by_projected_value(&parent_values, field)?;
            let output_type = &output_desc.fields()[index].value_type;
            Ok::<Value, IvmRuntimeError>(collect_by_output_value(output_type, value))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let record = output_desc.create(&values).map_err(|error| {
        IvmRuntimeError::InvalidCollectBy(format!("root record render failed: {error}"))
    })?;
    Ok(Some(record.into()))
}

fn collect_by_parent_from_records(
    input_desc: RecordDescriptor,
    output_desc: RecordDescriptor,
    collect_by: &CollectByOp,
    records: &[(Bytes, i64)],
) -> Result<Option<Bytes>, IvmRuntimeError> {
    let Some((parent_record, _)) = records.iter().find(|(_, weight)| *weight > 0) else {
        return Ok(None);
    };
    let parent_values = BorrowedRecord::new(parent_record, &input_desc)
        .to_values()
        .map_err(IvmRuntimeError::RecordEncoding)?;
    let mut values = collect_by
        .parent_fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let value = collect_by_projected_value(&parent_values, field)?;
            let output_type = &output_desc.fields()[index].value_type;
            Ok::<Value, IvmRuntimeError>(collect_by_output_value(output_type, value))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let window = collect_by_window_from_records(input_desc, records, collect_by)?;
    let mut children = Vec::new();
    for (record, copies) in window {
        let source_values = BorrowedRecord::new(&record, &input_desc)
            .to_values()
            .map_err(IvmRuntimeError::RecordEncoding)?;
        let child_values = collect_by
            .child_fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                Ok::<Value, IvmRuntimeError>(collect_by_output_value(
                    &collect_by.child_descriptor.fields()[index].value_type,
                    collect_by_projected_value(&source_values, field)?,
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let child = OwnedRecord::new(
            collect_by.child_descriptor.create(&child_values)?,
            collect_by.child_descriptor,
        );
        for _ in 0..copies {
            children.push(Value::Record(child.clone()));
        }
    }
    values.push(Value::Array(children));
    Ok(Some(output_desc.create(&values)?.into()))
}

fn collect_by_tree_parent_from_records(
    input_desc: RecordDescriptor,
    output_desc: RecordDescriptor,
    collect_by: &CollectByOp,
    records: &[(Bytes, i64)],
) -> Result<Option<Bytes>, IvmRuntimeError> {
    let Some((parent_record, _)) = records.iter().find(|(_, weight)| *weight > 0) else {
        return Ok(None);
    };
    let parent_values = BorrowedRecord::new(parent_record, &input_desc)
        .to_values()
        .map_err(IvmRuntimeError::RecordEncoding)?;
    let mut values = collect_by
        .parent_fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            Ok::<Value, IvmRuntimeError>(collect_by_output_value(
                &output_desc.fields()[index].value_type,
                collect_by_projected_value(&parent_values, field)?,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    values.extend(render_collect_by_slots(
        input_desc,
        records,
        parent_record,
        &collect_by.slots,
    )?);
    Ok(Some(output_desc.create(&values)?.into()))
}

fn render_collect_by_slots(
    input_desc: RecordDescriptor,
    records: &[(Bytes, i64)],
    owner_record: &Bytes,
    slots: &[CollectBySlot],
) -> Result<Vec<Value>, IvmRuntimeError> {
    slots
        .iter()
        .map(|slot| {
            if slot.limit == TopByLimit::Finite(0) {
                return Ok(Value::Array(Vec::new()));
            }
            let owner_key =
                encoded_record_key_part(input_desc, owner_record, &slot.group_field_indices)?;
            // A nested flat association can repeat its owning child once for
            // every grandchild. Collapse those repetitions by the rendered
            // child projection before applying this slot's order/window.
            let child_key_fields = slot
                .child_fields
                .iter()
                .map(|field| {
                    Ok((
                        field.output_name.clone(),
                        collect_by_projection_value_type(input_desc, field)?,
                    ))
                })
                .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
            let child_key_descriptor = RecordDescriptor::new(child_key_fields);
            let mut candidates = BTreeMap::<Bytes, Bytes>::new();
            for (record, weight) in records {
                if *weight <= 0
                    || encoded_record_key_part(input_desc, record, &slot.group_field_indices)?
                        != owner_key
                {
                    continue;
                }
                if let Some(presence_field_index) = slot.presence_field_index
                    && !BorrowedRecord::new(record, &input_desc).get_bool(presence_field_index)?
                {
                    continue;
                }
                let source_values = BorrowedRecord::new(record, &input_desc)
                    .to_values()
                    .map_err(IvmRuntimeError::RecordEncoding)?;
                let child_values = slot
                    .child_fields
                    .iter()
                    .map(|field| collect_by_projected_value(&source_values, field))
                    .collect::<Result<Vec<_>, _>>()?;
                candidates
                    .entry(child_key_descriptor.create(&child_values)?.into())
                    .or_insert_with(|| record.clone());
            }
            let selected = collect_by_slot_window_from_records(
                input_desc,
                candidates.into_values().collect(),
                slot,
            )?;
            let mut children = Vec::with_capacity(selected.len());
            for record in selected {
                let source_values = BorrowedRecord::new(&record, &input_desc)
                    .to_values()
                    .map_err(IvmRuntimeError::RecordEncoding)?;
                let mut child_values = slot
                    .child_fields
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        Ok::<Value, IvmRuntimeError>(collect_by_output_value(
                            &slot.child_descriptor.fields()[index].value_type,
                            collect_by_projected_value(&source_values, field)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                child_values.extend(render_collect_by_slots(
                    input_desc,
                    records,
                    &record,
                    &slot.slots,
                )?);
                children.push(Value::Record(OwnedRecord::new(
                    slot.child_descriptor.create(&child_values)?,
                    slot.child_descriptor,
                )));
            }
            Ok(Value::Array(children))
        })
        .collect()
}

fn collect_by_slot_window_from_records(
    descriptor: RecordDescriptor,
    records: Vec<Bytes>,
    slot: &CollectBySlot,
) -> Result<Vec<Bytes>, IvmRuntimeError> {
    let mut ranked = records
        .into_iter()
        .map(|record| {
            Ok((
                collect_by_sort_key_for_fields(
                    descriptor,
                    &record,
                    &slot.sort_field_indices,
                    &slot.sort_directions,
                )?,
                record,
            ))
        })
        .collect::<Result<Vec<_>, IvmRuntimeError>>()?;
    ranked.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let skip = usize::try_from(slot.offset).unwrap_or(usize::MAX);
    let take = match slot.limit {
        TopByLimit::Finite(limit) => usize::try_from(limit).unwrap_or(usize::MAX),
        TopByLimit::Unbounded => usize::MAX,
    };
    Ok(ranked
        .into_iter()
        .skip(skip)
        .take(take)
        .map(|(_, record)| record)
        .collect())
}

/// Render the selected expand window keyed by its ordered contributing source
/// ids. Keeping the key separate from the rendered bytes is what lets expand
/// suppress only a byte-equal occurrence without collapsing equal tuples from
/// different joins.
fn collect_by_expanded_window(
    input_desc: RecordDescriptor,
    output_desc: RecordDescriptor,
    collect_by: &CollectByOp,
    records: &[(Bytes, i64)],
) -> Result<BTreeMap<Vec<u8>, Bytes>, IvmRuntimeError> {
    let window = collect_by_window_from_records(input_desc, records, collect_by)?;
    let mut expanded = BTreeMap::new();
    for (record, copies) in window {
        let occurrence =
            encoded_record_key_part(input_desc, &record, &collect_by.occurrence_id_field_indices)?;
        // The complete typed occurrence carrier must distinguish every
        // derivation. A residual weighted duplicate is therefore malformed;
        // do not silently turn that ambiguity into one row.
        if copies != 1 || expanded.contains_key(&occurrence) {
            return Err(IvmRuntimeError::DuplicateCollectByOccurrenceId);
        }
        let values = BorrowedRecord::new(&record, &input_desc)
            .to_values()
            .map_err(IvmRuntimeError::RecordEncoding)?;
        let tuple = collect_by
            .tuple_fields
            .iter()
            .map(|field| collect_by_projected_value(&values, field))
            .collect::<Result<Vec<_>, _>>()?;
        expanded.insert(occurrence, output_desc.create(&tuple)?.into());
    }
    Ok(expanded)
}

fn collect_by_window_from_records(
    descriptor: RecordDescriptor,
    records: &[(Bytes, i64)],
    collect_by: &CollectByOp,
) -> Result<Vec<WindowedRecord>, IvmRuntimeError> {
    let mut ranked = Vec::new();
    for (record, weight) in records {
        if *weight > 0 {
            ranked.push((
                collect_by_sort_key(descriptor, record, collect_by)?,
                record.clone(),
                *weight,
            ));
        }
    }
    ranked.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let mut window = Vec::new();
    let mut to_skip = collect_by.offset;
    let mut remaining = match collect_by.limit {
        TopByLimit::Finite(limit) => Some(limit),
        TopByLimit::Unbounded => None,
    };
    for (_, record, weight) in ranked {
        if remaining == Some(0) {
            break;
        }
        let copies = weight as u64;
        let available = copies.saturating_sub(to_skip);
        to_skip = to_skip.saturating_sub(copies);
        let taken = remaining.map_or(available, |remaining| available.min(remaining));
        if taken > 0 {
            window.push((
                record,
                i64::try_from(taken).expect("window copies cannot exceed positive input weight"),
            ));
            if let Some(remaining) = &mut remaining {
                *remaining -= taken;
            }
        }
    }
    Ok(window)
}

fn collect_by_sort_key(
    descriptor: RecordDescriptor,
    record: &[u8],
    collect_by: &CollectByOp,
) -> Result<Vec<TopBySortPart>, IvmRuntimeError> {
    collect_by_sort_key_for_fields(
        descriptor,
        record,
        &collect_by.sort_field_indices,
        &collect_by.sort_directions,
    )
}

fn collect_by_sort_key_for_fields(
    descriptor: RecordDescriptor,
    record: &[u8],
    sort_field_indices: &[usize],
    sort_directions: &[TopByDirection],
) -> Result<Vec<TopBySortPart>, IvmRuntimeError> {
    let values = BorrowedRecord::new(record, &descriptor)
        .to_values()
        .map_err(IvmRuntimeError::RecordEncoding)?;
    sort_field_indices
        .iter()
        .zip(sort_directions)
        .map(|(field_idx, direction)| {
            Ok(TopBySortPart {
                key: values
                    .get(*field_idx)
                    .cloned()
                    .ok_or(IvmRuntimeError::GraphFieldIndexOutOfBounds(*field_idx))?,
                direction: *direction,
            })
        })
        .collect()
}

fn top_by_sort_key(
    descriptor: RecordDescriptor,
    record: &[u8],
    top_by: &TopByOp,
) -> Result<Vec<TopBySortPart>, IvmRuntimeError> {
    collect_by_sort_key_for_fields(
        descriptor,
        record,
        &top_by.sort_field_indices,
        &top_by.sort_directions,
    )
}

fn diff_record_windows(
    before: Vec<WindowedRecord>,
    after: Vec<WindowedRecord>,
) -> Vec<RecordDelta> {
    let mut weights = BTreeMap::<Bytes, i64>::new();
    for (record, copies) in &before {
        *weights.entry(record.clone()).or_default() -= *copies;
    }
    for (record, copies) in &after {
        *weights.entry(record.clone()).or_default() += *copies;
    }
    let mut deltas = Vec::new();
    for (record, _) in before {
        if weights.get(&record).is_some_and(|weight| *weight < 0)
            && let Some(weight) = weights.remove(&record)
        {
            deltas.push(RecordDelta { record, weight });
        }
    }
    for (record, _) in after {
        if weights.get(&record).is_some_and(|weight| *weight > 0)
            && let Some(weight) = weights.remove(&record)
        {
            deltas.push(RecordDelta { record, weight });
        }
    }
    debug_assert!(weights.values().all(|weight| *weight == 0));
    deltas
}

pub(super) fn encoded_record_key_part(
    descriptor: RecordDescriptor,
    record: &[u8],
    field_indices: &[usize],
) -> Result<Vec<u8>, IvmRuntimeError> {
    let mut key = Vec::new();
    for field_idx in field_indices {
        let value = descriptor.get_idx(record, *field_idx)?;
        encode_runtime_primary_key_part(&mut key, &value)?;
    }
    Ok(key)
}

fn encoded_arrangement_key_part(
    descriptor: RecordDescriptor,
    record: &[u8],
    field_indices: &[usize],
) -> Result<Vec<u8>, IvmRuntimeError> {
    let mut key = Vec::new();
    for field_idx in field_indices {
        encode_key_part(&mut key, &descriptor.get_idx(record, *field_idx)?)?;
    }
    Ok(key)
}

fn encode_runtime_primary_key_part(
    key: &mut Vec<u8>,
    value: &Value,
) -> Result<(), IvmRuntimeError> {
    match value {
        Value::U8(value) => {
            key.push(0);
            key.push(*value);
        }
        Value::U16(value) => {
            key.push(1);
            key.extend(value.to_be_bytes());
        }
        Value::U32(value) => {
            key.push(2);
            key.extend(value.to_be_bytes());
        }
        Value::U64(value) => {
            key.push(3);
            key.extend(value.to_be_bytes());
        }
        Value::I32(value) => {
            key.push(14);
            key.extend(order_preserving_i32_bits(*value).to_be_bytes());
        }
        Value::I64(value) => {
            key.push(13);
            key.extend(order_preserving_i64_bits(*value).to_be_bytes());
        }
        Value::F64(value) => {
            key.push(4);
            key.extend(ordered_f64_key(*value).to_be_bytes());
        }
        Value::Bool(value) => {
            key.push(5);
            key.push(u8::from(*value));
        }
        Value::String(value) => {
            key.push(6);
            encode_runtime_ordered_bytes(key, value.as_bytes());
        }
        Value::EnumTag(value) => {
            key.push(0);
            key.push(*value);
        }
        Value::Bytes(value) => {
            key.push(7);
            encode_runtime_ordered_bytes(key, value);
        }
        Value::Uuid(value) => {
            key.push(10);
            key.extend_from_slice(value.as_bytes());
        }
        Value::Tuple(values) => {
            key.push(11);
            for value in values {
                encode_runtime_primary_key_part(key, value)?;
            }
        }
        Value::Nullable(None) => {
            key.push(12);
            key.push(0);
        }
        Value::Nullable(Some(value)) => {
            key.push(12);
            key.push(1);
            encode_runtime_primary_key_part(key, value)?;
        }
        Value::Array(_) | Value::Record(_) | Value::Enum(_) => {
            return Err(IvmRuntimeError::UnsupportedJoinKey);
        }
    }
    Ok(())
}

fn ordered_f64_key(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits & (1 << 63) == 0 {
        bits ^ (1 << 63)
    } else {
        !bits
    }
}

fn encode_runtime_ordered_bytes(key: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            key.extend([0, 0xff]);
        } else {
            key.push(*byte);
        }
    }
    key.push(0);
    key.push(0);
}

#[derive(Debug, Error)]
pub enum IvmRuntimeError {
    #[error("aggregate result exceeds its declared numeric width")]
    AggregateOverflow,
    #[error("graph field not found: {0}")]
    GraphFieldNotFound(String),
    #[error("graph field index out of bounds: {0}")]
    GraphFieldIndexOutOfBounds(usize),
    #[error("graph node has unexpected input arity: {0:?}")]
    GraphInputArityMismatch(NodeId),
    #[error("graph node is missing input: {0:?}")]
    GraphInputMissing(NodeId),
    #[error("graph node not found: {0:?}")]
    GraphNodeNotFound(NodeId),
    #[error("graph output descriptors do not match")]
    GraphOutputMismatch,
    #[error("enum tag {tag} is absent from this projection target")]
    EnumTagProjectionAbsent { tag: u8 },
    #[error("enum tag projection requires an enum value")]
    EnumTagProjectionNonEnum,
    #[error("payload enum tag {tag} is absent from this projection target")]
    EnumProjectionAbsent { tag: u32 },
    #[error("payload enum projection requires an enum value")]
    EnumProjectionNonEnum,
    #[error("index not found: {0}")]
    IndexNotFound(String),
    #[error("join key arity mismatch: left={left}, right={right}")]
    JoinKeyArityMismatch { left: usize, right: usize },
    #[error("shape key field not found: {0}")]
    ShapeKeyFieldNotFound(String),
    #[error("table has no primary key: {0}")]
    MissingPrimaryKey(String),
    #[error("runtime node state missing: {0:?}")]
    NodeStateMissing(NodeId),
    #[error("runtime node state operator mismatch: {0:?}")]
    NodeStateOperatorMismatch(NodeId),
    #[error("runtime state advanced out of order: current={current}, next={next}")]
    OutOfOrderRuntimeState { current: String, next: String },
    #[error("persist node expected key/value bytes")]
    PersistRecordMismatch,
    #[error("binding sources can only be evaluated through prepared shapes")]
    BindingSourceRequiresPrepare,
    #[error("multisink subscription must have at least one sink")]
    EmptyMultisinkSubscription,
    #[error("multisink sink already exists: {0}")]
    DuplicateMultisinkSink(String),
    #[error("multisink sink requires prepare because it contains a binding source: {0}")]
    MultisinkSinkRequiresPrepare(String),
    #[error("routed multisink sink {sink} has {actual} route fields, expected {expected}")]
    RoutedMultisinkRouteArityMismatch {
        sink: String,
        expected: usize,
        actual: usize,
    },
    #[error("binding source not found: {0}")]
    BindingSourceNotFound(String),
    #[error("binding source descriptor mismatch: {0}")]
    BindingSourceDescriptorMismatch(String),
    #[error("duplicate schema version {version} for table {table}")]
    DuplicateTableVariant { table: String, version: u64 },
    #[error(transparent)]
    RecordEncoding(#[from] records::Error),
    #[error("recursive node {node:?} exceeded iteration limit {max_iters}")]
    RecursiveIterationLimit { node: NodeId, max_iters: usize },
    #[error(transparent)]
    Storage(#[from] crate::storage::Error),
    #[error("storage unavailable for durable node")]
    StorageUnavailable,
    #[error("subscription shape not found: {0:?}")]
    PreparedShapeNotFound(PreparedShapeId),
    #[error("runtime state is stale: expected={expected}, actual={actual:?}")]
    StaleRuntimeState {
        expected: String,
        actual: Option<String>,
    },
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),
    #[error("field already exists in the live catalogue: {table}.{field}")]
    TableFieldAlreadyExists { table: String, field: String },
    #[error("unknown schema version {version} for table {table}")]
    UnknownTableVariant { table: String, version: u64 },
    #[error("variant projection not found: {table}.{target}")]
    VariantProjectionNotFound { table: String, target: String },
    #[error("schema-variant table requires a fixed-output projection: {0}")]
    VariantProjectionRequired(String),
    #[error("variant projection output descriptor mismatch: {table}.{target}")]
    VariantProjectionOutputMismatch { table: String, target: String },
    #[error(
        "variant projection case already registered with different semantics: {table}.{target} version {version}"
    )]
    VariantProjectionCaseAlreadyRegistered {
        table: String,
        target: String,
        version: u64,
    },
    #[error("variant projection case not found: {table}.{target} version {version}")]
    VariantProjectionCaseNotFound {
        table: String,
        target: String,
        version: u64,
    },
    #[error("variant projection source descriptor mismatch: {table}.{target} version {version}")]
    VariantProjectionSourceMismatch {
        table: String,
        target: String,
        version: u64,
    },
    #[error("variant projection enum field not found: {table}.{target}.{field}")]
    VariantProjectionEnumFieldNotFound {
        table: String,
        target: String,
        field: String,
    },
    #[error("variant projection enum output must contain exactly one field: {table}.{target}")]
    VariantProjectionEnumOutputMustBeSingleField { table: String, target: String },
    #[error("variant projection field is not an enum: {table}.{target}.{field}")]
    VariantProjectionEnumFieldTypeMismatch {
        table: String,
        target: String,
        field: String,
    },
    #[error("variant projection enum schema does not match fixed output: {table}.{target}.{field}")]
    VariantProjectionEnumSchemaMismatch {
        table: String,
        target: String,
        field: String,
    },
    #[error("variant projection payload does not match enum case: {table}.{target}.{case}")]
    VariantProjectionEnumPayloadMismatch {
        table: String,
        target: String,
        case: String,
    },
    #[error("recursive enum projection descriptor mismatch at {path}")]
    RecursiveEnumProjectionDescriptorMismatch { path: String },
    #[error("enum match field is not an enum: {field}")]
    VariantProjectFieldTypeMismatch { field: String },
    #[error("enum match payload descriptor does not match its declared case: {field}")]
    VariantProjectPayloadMismatch { field: String },
    #[error("unique index violation: {index}")]
    UniqueIndexViolation { index: String },
    #[error("unsupported join key")]
    UnsupportedJoinKey,
    #[error("non-monotone recursive delta reached positive-only incremental recursion")]
    UnsupportedNonMonotoneRecursion,
    #[error("nested recursive graphs are not supported in v0")]
    UnsupportedNestedRecursion,
    #[error("unsupported arg_max_by graph: {0}")]
    UnsupportedArgMaxBy(String),
    #[error("collect_by is terminal-only and cannot feed another graph node")]
    CollectByMustBeTerminal,
    #[error("invalid collect_by descriptor: {0}")]
    InvalidCollectBy(String),
    #[error("collect_by expand encountered duplicate output occurrence source ids")]
    DuplicateCollectByOccurrenceId,
    #[error("unsupported operator")]
    UnsupportedOperator,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{
        ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey,
    };
    use crate::storage::{RecordStore, RocksDbStorage};

    #[test]
    fn payload_enum_remap_changes_only_the_case_tag() {
        let descriptor = RecordDescriptor::new([("value", ValueType::String)]);
        let value = Value::Enum(
            EnumValue::create(2, descriptor, &[Value::String("later".to_owned())]).unwrap(),
        );
        let remapped = remap_enum(value, &[Some(0), Some(1), Some(3)]).unwrap();
        let Value::Enum(remapped) = remapped else {
            panic!("expected payload enum");
        };
        assert_eq!(remapped.tag(), 3);
        assert_eq!(
            remapped.record().to_values().unwrap(),
            vec![Value::String("later".to_owned())]
        );
    }

    #[test]
    fn recursive_enum_projection_reencodes_nullable_array_tuple_and_record_occurrences() {
        let physical = ValueType::EnumTag(
            records::ScalarEnumSchema::new("physical", ["draft", "snoozed", "archived"]).unwrap(),
        );
        let authored = ValueType::EnumTag(
            records::ScalarEnumSchema::new("authored", ["draft", "archived"]).unwrap(),
        );
        let source = ValueType::Tuple(vec![
            ValueType::Nullable(Box::new(ValueType::Array(Box::new(physical.clone())))),
            ValueType::Record(Box::new(RecordDescriptor::new([("state", physical)]))),
        ]);
        let target = ValueType::Tuple(vec![
            ValueType::Nullable(Box::new(ValueType::Array(Box::new(authored.clone())))),
            ValueType::Record(Box::new(RecordDescriptor::new([("state", authored)]))),
        ]);
        let remaps = RecursiveEnumRemaps {
            scalar: BTreeMap::from([
                (
                    "root/tuple/0/nullable/array".to_owned(),
                    vec![Some(0), None, Some(1)],
                ),
                (
                    "root/tuple/1/record/state".to_owned(),
                    vec![Some(0), None, Some(1)],
                ),
            ]),
            payload: BTreeMap::new(),
        };
        let record = RecordDescriptor::new([(
            "state",
            ValueType::EnumTag(
                records::ScalarEnumSchema::new("physical", ["draft", "snoozed", "archived"])
                    .unwrap(),
            ),
        )]);
        let value = Value::Tuple(vec![
            Value::Nullable(Some(Box::new(Value::Array(vec![Value::EnumTag(2)])))),
            Value::Record(OwnedRecord::new(
                record.create(&[Value::EnumTag(2)]).unwrap(),
                record,
            )),
        ]);
        let projected =
            remap_recursive_enum_value(value, &source, &target, &remaps, "root").unwrap();
        let Value::Tuple(values) = projected else {
            panic!("tuple expected")
        };
        assert_eq!(
            values[0],
            Value::Nullable(Some(Box::new(Value::Array(vec![Value::EnumTag(1)]))))
        );
        let Value::Record(record) = &values[1] else {
            panic!("record expected")
        };
        assert_eq!(record.to_values().unwrap(), vec![Value::EnumTag(1)]);
    }

    #[test]
    fn recursive_enum_projection_remaps_payload_cases_and_nested_payload_enums() {
        let physical_scalar = ValueType::EnumTag(
            records::ScalarEnumSchema::new("physical", ["draft", "snoozed", "archived"]).unwrap(),
        );
        let authored_scalar = ValueType::EnumTag(
            records::ScalarEnumSchema::new("authored", ["draft", "archived"]).unwrap(),
        );
        let physical_payload = ValueType::Enum(Box::new(
            EnumSchema::new(
                "physical-payload",
                [
                    records::EnumCase::new(
                        "case-a",
                        RecordDescriptor::new([("state", physical_scalar.clone())]),
                    ),
                    records::EnumCase::new(
                        "case-b",
                        RecordDescriptor::new(Vec::<(String, ValueType)>::new()),
                    ),
                ],
            )
            .unwrap(),
        ));
        let authored_payload = ValueType::Enum(Box::new(
            EnumSchema::new(
                "authored-payload",
                [records::EnumCase::new(
                    "case-a",
                    RecordDescriptor::new([("state", authored_scalar)]),
                )],
            )
            .unwrap(),
        ));
        let source = ValueType::Record(Box::new(RecordDescriptor::new([(
            "nested",
            physical_payload.clone(),
        )])));
        let target = ValueType::Record(Box::new(RecordDescriptor::new([(
            "nested",
            authored_payload.clone(),
        )])));
        let remaps = RecursiveEnumRemaps {
            scalar: BTreeMap::from([(
                "root/record/nested/case/0/state".to_owned(),
                vec![Some(0), None, Some(1)],
            )]),
            payload: BTreeMap::from([("root/record/nested".to_owned(), vec![Some(0), None])]),
        };
        let ValueType::Enum(payload_schema) = physical_payload else {
            panic!("payload expected")
        };
        let payload = payload_schema.case(0).unwrap().payload.clone();
        let nested = Value::Enum(EnumValue::create(0, payload, &[Value::EnumTag(2)]).unwrap());
        let ValueType::Record(source_record) = &source else {
            panic!("record expected")
        };
        let value = Value::Record(OwnedRecord::new(
            source_record.create(&[nested]).unwrap(),
            (**source_record).clone(),
        ));
        let projected =
            remap_recursive_enum_value(value, &source, &target, &remaps, "root").unwrap();
        let Value::Record(record) = projected else {
            panic!("record expected")
        };
        let Value::Enum(nested) = record.to_values().unwrap().pop().unwrap() else {
            panic!("enum expected")
        };
        assert_eq!(nested.tag(), 0);
        assert_eq!(
            nested.record().to_values().unwrap(),
            vec![Value::EnumTag(1)]
        );

        // A physical case which the target schema cannot name must fail rather
        // than becoming a default case or silently removing the row.
        let unknown = Value::Enum(
            EnumValue::create(
                1,
                RecordDescriptor::new(Vec::<(String, ValueType)>::new()),
                &[],
            )
            .unwrap(),
        );
        let value = Value::Record(OwnedRecord::new(
            source_record.create(&[unknown]).unwrap(),
            (**source_record).clone(),
        ));
        assert!(matches!(
            remap_recursive_enum_value(value, &source, &target, &remaps, "root"),
            Err(IvmRuntimeError::EnumProjectionAbsent { tag: 1 })
        ));
    }

    #[test]
    fn collect_by_terminal_records_preserve_nullable_descriptor_wrappers() {
        let uuid = uuid::Uuid::from_bytes([7; 16]);

        assert_eq!(
            collect_by_output_value(
                &ValueType::Nullable(Box::new(ValueType::I32)),
                Value::I32(3),
            ),
            Value::Nullable(Some(Box::new(Value::I32(3))))
        );
        assert_eq!(
            collect_by_output_value(
                &ValueType::Nullable(Box::new(ValueType::Uuid)),
                Value::Uuid(uuid),
            ),
            Value::Nullable(Some(Box::new(Value::Uuid(uuid))))
        );
        assert_eq!(
            collect_by_output_value(
                &ValueType::Nullable(Box::new(ValueType::I32)),
                Value::Nullable(None),
            ),
            Value::Nullable(None)
        );
    }

    #[test]
    fn root_ordering_rewrites_insert_indices_and_emits_moves_after_payload_edits() {
        // Internal coverage is intentional: the public browser matrix proves
        // end-to-end ordering, while this pins the terminal operation protocol
        // ordering that is not otherwise observable through the public API.
        let key = |byte| vec![byte];
        let before = BTreeMap::from([(key(1), 0), (key(2), 1), (key(3), 2)]);
        let after = BTreeMap::from([(key(3), 0), (key(4), 1), (key(1), 2), (key(2), 3)]);
        let mut terminal = TerminalDeltas {
            operations: vec![
                TerminalOperation {
                    root_key: key(2),
                    path: Vec::new(),
                    edit: TerminalEdit::Update {
                        key: key(2),
                        value: vec![22],
                    },
                },
                TerminalOperation {
                    root_key: key(4),
                    path: Vec::new(),
                    edit: TerminalEdit::Insert {
                        index: 0,
                        key: key(4),
                        value: vec![44],
                    },
                },
            ],
        };

        apply_root_ordering_operations(&before, &after, &mut terminal);

        assert!(matches!(
            terminal.operations[1].edit,
            TerminalEdit::Insert { index: 1, .. }
        ));
        assert!(matches!(
            terminal.operations[0].edit,
            TerminalEdit::Update { .. }
        ));
        assert_eq!(
            terminal.operations[2..]
                .iter()
                .map(|operation| match &operation.edit {
                    TerminalEdit::Move { key, index } => (key.clone(), *index),
                    edit => panic!("expected root move after payload edits, got {edit:?}"),
                })
                .collect::<Vec<_>>(),
            vec![(key(3), 0), (key(4), 1)]
        );
    }

    fn albums_schema() -> DatabaseSchema {
        DatabaseSchema::new([TableSchema::new(
            "albums",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
    }

    fn indexed_albums_schema() -> DatabaseSchema {
        DatabaseSchema::new([TableSchema::new(
            "albums",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
        .with_index(IndexSchema::new("albums_by_title", ["title"]))])
    }

    fn albums_artists_schema() -> DatabaseSchema {
        DatabaseSchema::new([
            TableSchema::new(
                "albums",
                [
                    ColumnSchema::new("id", ColumnType::U64),
                    ColumnSchema::new("artist_id", ColumnType::U64),
                    ColumnSchema::new("title", ColumnType::String),
                ],
            )
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
            TableSchema::new(
                "artists",
                [
                    ColumnSchema::new("id", ColumnType::U64),
                    ColumnSchema::new("name", ColumnType::String),
                ],
            )
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        ])
    }

    fn edges_schema() -> DatabaseSchema {
        DatabaseSchema::new([TableSchema::new(
            "edges",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("src", ColumnType::U64),
                ColumnSchema::new("dst", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
    }

    fn reach_descriptor() -> RecordDescriptor {
        RecordDescriptor::new([
            ("src", ColumnType::U64.clone()),
            ("dst", ColumnType::U64.clone()),
        ])
    }

    #[test]
    fn top_by_distinguishes_finite_max_from_unbounded_limit() {
        // Direct helper coverage is intentional: constructing more than
        // u64::MAX derivations through public tables is not feasible, while
        // synthetic weights exercise the semantic boundary without expanding
        // multiplicity into individual records.
        let descriptor = RecordDescriptor::new([("id", ValueType::U64)]);
        let records = [1, 2, 3]
            .into_iter()
            .map(|id| {
                (
                    Bytes::from(descriptor.create(&[Value::U64(id)]).unwrap()),
                    i64::MAX,
                )
            })
            .collect::<Vec<_>>();
        let top_by = |limit| TopByOp {
            group_fields: Vec::new(),
            group_field_indices: Vec::new(),
            order_fields: vec![TopByOrderField {
                field: "id".to_owned(),
                direction: TopByDirection::Asc,
            }],
            tie_fields: Vec::new(),
            sort_field_indices: vec![0],
            sort_directions: vec![TopByDirection::Asc],
            offset: 0,
            limit,
        };

        let finite = top_by_window_from_records(
            descriptor,
            records.clone(),
            &top_by(TopByLimit::Finite(u64::MAX)),
        )
        .unwrap();
        assert_eq!(
            finite
                .into_iter()
                .map(|(_, weight)| weight)
                .collect::<Vec<_>>(),
            [i64::MAX, i64::MAX, 1]
        );

        let unbounded =
            top_by_window_from_records(descriptor, records, &top_by(TopByLimit::Unbounded))
                .unwrap();
        assert_eq!(
            unbounded
                .into_iter()
                .map(|(_, weight)| weight)
                .collect::<Vec<_>>(),
            [i64::MAX, i64::MAX, i64::MAX]
        );
    }

    fn recursive_reach_graph() -> GraphBuilder {
        let seed = GraphBuilder::table("edges").project(["src", "dst"]);
        let edge_pairs = GraphBuilder::table("edges").project(["src", "dst"]);
        let frontier = GraphBuilder::frontier_source("frontier", reach_descriptor());
        let step = GraphBuilder::join(frontier, edge_pairs, ["dst"], ["src"]).project_fields([
            crate::ivm::ProjectField::renamed("left.src", "src"),
            crate::ivm::ProjectField::renamed("right.dst", "dst"),
        ]);
        GraphBuilder::recursive(seed, step, "frontier", 16)
    }

    fn recursive_reach_from_graph(src: u64) -> GraphBuilder {
        let seed = GraphBuilder::table("edges")
            .filter(PredicateExpr::eq("src", Value::U64(src)))
            .project(["src", "dst"]);
        let edge_pairs = GraphBuilder::table("edges").project(["src", "dst"]);
        let frontier = GraphBuilder::frontier_source("frontier", reach_descriptor());
        let step = GraphBuilder::join(frontier, edge_pairs, ["dst"], ["src"]).project_fields([
            crate::ivm::ProjectField::renamed("left.src", "src"),
            crate::ivm::ProjectField::renamed("right.dst", "dst"),
        ]);
        GraphBuilder::recursive(seed, step, "frontier", 16)
    }

    fn recursive_reach_from_with_union_step_graph(src: u64) -> GraphBuilder {
        let seed = GraphBuilder::table("edges")
            .filter(PredicateExpr::eq("src", Value::U64(src)))
            .project(["src", "dst"]);
        let edge_pairs = GraphBuilder::table("edges").project(["src", "dst"]);
        let frontier = GraphBuilder::frontier_source("frontier", reach_descriptor());
        let expanded = GraphBuilder::join(frontier.clone(), edge_pairs, ["dst"], ["src"])
            .project_fields([
                crate::ivm::ProjectField::renamed("left.src", "src"),
                crate::ivm::ProjectField::renamed("right.dst", "dst"),
            ]);
        let step = GraphBuilder::union([frontier, expanded]);
        GraphBuilder::recursive(seed, step, "frontier", 16)
    }

    fn assert_auto_family_matches_direct_with_prepared_count(
        schema: DatabaseSchema,
        families: &[GraphBuilder],
        table_deltas: Vec<TableDelta>,
        storage_familied: &impl OrderedKvStorage,
        storage_direct: &impl OrderedKvStorage,
        expected_prepared_shapes: usize,
    ) {
        let mut familied = IvmRuntime::new(schema.clone()).unwrap();
        let mut direct = IvmRuntime::new(schema).unwrap();
        direct.set_auto_direct_family_enabled(false);

        let familied_subscriptions = families
            .iter()
            .cloned()
            .map(|graph| {
                familied
                    .subscribe_one_sink(graph, storage_familied)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let direct_subscriptions = families
            .iter()
            .cloned()
            .map(|graph| direct.subscribe_one_sink(graph, storage_direct).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(familied.prepared_shapes.len(), expected_prepared_shapes);
        for (familied_subscription, direct_subscription) in familied_subscriptions
            .iter()
            .zip(direct_subscriptions.iter())
        {
            assert_eq!(
                familied_subscription.recv().unwrap(),
                direct_subscription.recv().unwrap()
            );
        }

        familied
            .tick(table_deltas.clone(), storage_familied)
            .expect("familied tick");
        direct
            .tick(table_deltas, storage_direct)
            .expect("direct tick");
        for (familied_subscription, direct_subscription) in familied_subscriptions
            .iter()
            .zip(direct_subscriptions.iter())
        {
            match (
                familied_subscription.try_recv(),
                direct_subscription.try_recv(),
            ) {
                (Ok(familied), Ok(direct)) => assert_eq!(familied, direct),
                (Err(TryRecvError::Empty), Err(TryRecvError::Empty)) => {}
                (familied, direct) => {
                    panic!("familied/direct notification mismatch: {familied:?} != {direct:?}");
                }
            }
        }
    }

    fn assert_auto_family_matches_direct(
        schema: DatabaseSchema,
        families: &[GraphBuilder],
        table_deltas: Vec<TableDelta>,
        storage_familied: &impl OrderedKvStorage,
        storage_direct: &impl OrderedKvStorage,
    ) {
        assert_auto_family_matches_direct_with_prepared_count(
            schema,
            families,
            table_deltas,
            storage_familied,
            storage_direct,
            1,
        );
    }

    #[test]
    fn direct_literal_subscriptions_share_auto_family_and_keep_direct_output() {
        let schema = albums_schema();
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let storage = crate::storage::MemoryStorage::new(&["albums"]);
        let first = runtime
            .subscribe_one_sink(
                GraphBuilder::table("albums")
                    .filter(PredicateExpr::eq("id", Value::U64(1)))
                    .project(["title"]),
                &storage,
            )
            .unwrap();
        let second = runtime
            .subscribe_one_sink(
                GraphBuilder::table("albums")
                    .filter(PredicateExpr::eq("id", Value::U64(2)))
                    .project(["title"]),
                &storage,
            )
            .unwrap();

        assert!(first.recv().unwrap().is_empty());
        assert!(second.recv().unwrap().is_empty());
        assert_eq!(runtime.prepared_shapes.len(), 1);
        assert_eq!(
            runtime
                .binding_sources
                .values()
                .next()
                .unwrap()
                .refcounts
                .len(),
            2
        );

        let albums = schema.table("albums").unwrap().record_schema();
        runtime
            .tick(
                vec![TableDelta {
                    variant_tag: 0,
                    table: "albums".to_owned(),
                    descriptor: albums,
                    deltas: vec![
                        RecordDelta {
                            record: albums
                                .create(&[Value::U64(1), Value::String("one".to_owned())])
                                .unwrap()
                                .into(),
                            weight: 1,
                        },
                        RecordDelta {
                            record: albums
                                .create(&[Value::U64(2), Value::String("two".to_owned())])
                                .unwrap()
                                .into(),
                            weight: 1,
                        },
                    ],
                }],
                &storage,
            )
            .unwrap();

        assert_eq!(
            first.recv().unwrap().to_values().unwrap(),
            vec![(vec![Value::String("one".to_owned())], 1)]
        );
        assert_eq!(
            second.recv().unwrap().to_values().unwrap(),
            vec![(vec![Value::String("two".to_owned())], 1)]
        );
    }

    #[test]
    fn hydration_memo_survives_empty_ticks_without_replaying_deltas() {
        let schema = albums_schema();
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let storage = crate::storage::MemoryStorage::new(&["albums"]);
        let subscription = runtime
            .subscribe_one_sink(GraphBuilder::table("albums"), &storage)
            .unwrap();
        assert!(subscription.recv().unwrap().is_empty());
        assert!(runtime.eval_memo.keys().any(|key| key.tick_epoch.is_none()));

        runtime.tick(Vec::new(), &storage).unwrap();
        assert!(subscription.try_recv().is_err());
        assert!(runtime.eval_memo.keys().any(|key| key.tick_epoch.is_none()));

        let albums = schema.table("albums").unwrap().record_schema();
        let row = albums
            .create(&[Value::U64(1), Value::String("Blue Train".to_owned())])
            .unwrap();
        runtime
            .tick(
                vec![TableDelta {
                    variant_tag: 0,
                    table: "albums".to_owned(),
                    descriptor: albums,
                    deltas: vec![RecordDelta {
                        record: row.into(),
                        weight: 1,
                    }],
                }],
                &storage,
            )
            .unwrap();
        assert_eq!(subscription.recv().unwrap().deltas.len(), 1);

        runtime.tick(Vec::new(), &storage).unwrap();
        assert!(subscription.try_recv().is_err());
    }

    fn write_two_album_rows(storage: &impl OrderedKvStorage, albums: &RecordDescriptor) {
        let store = RecordStore::new(storage, "albums", albums);
        let first = albums
            .create(&[Value::U64(1), Value::String("one".to_owned())])
            .unwrap();
        let second = albums
            .create(&[Value::U64(2), Value::String("two".to_owned())])
            .unwrap();
        let first = crate::records::encode_variant_record(0, &first);
        let second = crate::records::encode_variant_record(0, &second);
        store
            .write_many(&[store.set(b"1", &first), store.set(b"2", &second)])
            .unwrap();
    }

    fn album_count_graph() -> GraphBuilder {
        GraphBuilder::aggregate(
            GraphBuilder::table("albums"),
            Vec::<String>::new(),
            [AggregateExpr {
                function: AggregateFunction::Count,
                expression: None,
                distinct: false,
                output_name: Some("count".to_owned()),
            }],
        )
    }

    #[test]
    fn aggregate_subscription_hydration_reuses_current_shared_arrangements() {
        let schema = albums_schema();
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
        let albums = schema.table("albums").unwrap().record_schema();
        write_two_album_rows(&storage, &albums);

        let first = runtime
            .subscribe_one_sink(album_count_graph(), &storage)
            .unwrap();
        let first_snapshot = first.recv().unwrap();
        assert_eq!(
            first_snapshot.to_values().unwrap(),
            vec![(vec![Value::U64(2)], 1)]
        );
        let after_first = runtime.stats();
        assert!(after_first.hydration_memo_computes > 0);

        let mut fresh_runtime = IvmRuntime::new(schema).unwrap();
        let fresh = fresh_runtime
            .subscribe_one_sink(album_count_graph(), &storage)
            .unwrap()
            .recv()
            .unwrap();

        let second = runtime
            .subscribe_one_sink(album_count_graph(), &storage)
            .unwrap();
        let reused = second.recv().unwrap();
        let after_second = runtime.stats();

        assert_eq!(reused, fresh);
        assert_eq!(
            after_second.hydration_memo_computes, after_first.hydration_memo_computes,
            "second identical subscriber should reuse the hydrated aggregate output"
        );
        assert!(
            after_second.hydration_memo_hits > after_first.hydration_memo_hits,
            "second identical subscriber should record a hydration memo hit"
        );
    }

    #[test]
    fn one_shot_aggregate_hydration_does_not_satisfy_subscription_arrangement_seed() {
        let schema = albums_schema();
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
        let albums = schema.table("albums").unwrap().record_schema();
        write_two_album_rows(&storage, &albums);

        let one_shot = runtime
            .query_snapshot(album_count_graph(), &storage)
            .unwrap();
        assert_eq!(
            one_shot.to_values().unwrap(),
            vec![(vec![Value::U64(2)], 1)]
        );
        let after_one_shot = runtime.stats();
        assert_eq!(after_one_shot.arrangement_count, 0);

        let subscription = runtime
            .subscribe_one_sink(album_count_graph(), &storage)
            .unwrap();
        let snapshot = subscription.recv().unwrap();
        let after_subscribe = runtime.stats();

        assert_eq!(snapshot, one_shot);
        assert!(
            after_subscribe.hydration_memo_computes > after_one_shot.hydration_memo_computes,
            "subscription hydration must rebuild when a one-shot memo has no current arrangement"
        );
        assert_eq!(after_subscribe.arrangement_count, 1);
    }

    #[test]
    fn pending_subscription_drains_match_unbounded_when_eval_memo_is_evicted_before_drain() {
        let schema = albums_schema();
        let albums = schema.table("albums").unwrap().record_schema();

        let run = |evict_before_drain: bool| {
            let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
            let storage = crate::storage::MemoryStorage::new(&["albums"]);
            let subscription = runtime
                .subscribe_one_sink(GraphBuilder::table("albums"), &storage)
                .unwrap();
            assert!(subscription.recv().unwrap().is_empty());

            let row = albums
                .create(&[Value::U64(1), Value::String("Blue Train".to_owned())])
                .unwrap();
            runtime
                .tick(
                    vec![TableDelta {
                        variant_tag: 0,
                        table: "albums".to_owned(),
                        descriptor: albums,
                        deltas: vec![RecordDelta {
                            record: row.into(),
                            weight: 1,
                        }],
                    }],
                    &storage,
                )
                .unwrap();

            if evict_before_drain {
                runtime.evict_eval_memo_for_tests(0, 0);
                assert!(
                    runtime.eval_memo.is_empty(),
                    "the eval memo is a pure cache and may be fully evicted while subscription output is pending"
                );
            }

            let delivered = subscription.recv().unwrap();

            if evict_before_drain {
                runtime.evict_eval_memo_for_tests(0, 0);
                assert!(
                    runtime.eval_memo.is_empty(),
                    "draining subscription output must not depend on eval memo entries"
                );
            }

            delivered
        };

        assert_eq!(run(true), run(false));
    }

    #[test]
    fn memo_context_digest_distinguishes_frontier_binding_values() {
        let descriptor = reach_descriptor();
        let left = RecordDeltas {
            descriptor,
            deltas: vec![RecordDelta {
                record: descriptor
                    .create(&[Value::U64(1), Value::U64(2)])
                    .unwrap()
                    .into(),
                weight: 1,
            }],
        };
        let right = RecordDeltas {
            descriptor,
            deltas: vec![RecordDelta {
                record: descriptor
                    .create(&[Value::U64(1), Value::U64(3)])
                    .unwrap()
                    .into(),
                weight: 1,
            }],
        };

        assert_ne!(record_deltas_digest(&left), record_deltas_digest(&right));
        assert_eq!(record_deltas_digest(&left), record_deltas_digest(&left));
    }

    #[test]
    fn project_emits_copied_literal_and_null_columns() {
        let schema = albums_schema();
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let storage = crate::storage::MemoryStorage::new(&["albums"]);
        let subscription = runtime
            .subscribe_one_sink(
                GraphBuilder::table("albums").project_fields([
                    ProjectField::renamed("id", "id"),
                    ProjectField::literal(
                        "event_kind",
                        LiteralValue::String("result_content".to_owned()),
                    ),
                    ProjectField::null_typed(
                        "missing_title",
                        ValueType::Nullable(Box::new(ValueType::String)),
                    ),
                ]),
                &storage,
            )
            .unwrap();

        assert!(subscription.recv().unwrap().is_empty());
        assert!(runtime.graph.nodes().values().any(|node| {
            let OpType::MapProject(project) = &node.descriptor.operator else {
                return false;
            };
            project.mapping == vec![(0, 0)]
                && project.expressions.len() == 3
                && !projection_uses_raw_copy(
                    &project.expressions,
                    &project.mapping,
                    node.descriptor.output,
                )
        }));

        let albums = schema.table("albums").unwrap().record_schema();
        runtime
            .tick(
                vec![TableDelta {
                    variant_tag: 0,
                    table: "albums".to_owned(),
                    descriptor: albums,
                    deltas: vec![
                        RecordDelta {
                            record: albums
                                .create(&[Value::U64(1), Value::String("one".to_owned())])
                                .unwrap()
                                .into(),
                            weight: 2,
                        },
                        RecordDelta {
                            record: albums
                                .create(&[Value::U64(2), Value::String("two".to_owned())])
                                .unwrap()
                                .into(),
                            weight: -3,
                        },
                    ],
                }],
                &storage,
            )
            .unwrap();

        assert_eq!(
            subscription.recv().unwrap().to_values().unwrap(),
            vec![
                (
                    vec![
                        Value::U64(1),
                        Value::String("result_content".to_owned()),
                        Value::Nullable(None),
                    ],
                    2,
                ),
                (
                    vec![
                        Value::U64(2),
                        Value::String("result_content".to_owned()),
                        Value::Nullable(None),
                    ],
                    -3,
                ),
            ]
        );
    }

    #[test]
    fn project_typed_literal_preserves_nested_nullable_null_type() {
        let schema = albums_schema();
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let storage = crate::storage::MemoryStorage::new(&["albums"]);
        let subscription = runtime
            .subscribe_one_sink(
                GraphBuilder::table("albums").project_fields([
                    ProjectField::renamed("id", "id"),
                    ProjectField::literal_typed(
                        "default_value",
                        LiteralValue::Nullable(Some(Box::new(LiteralValue::Nullable(None)))),
                        ValueType::Nullable(Box::new(ValueType::Nullable(Box::new(
                            ValueType::String,
                        )))),
                    ),
                ]),
                &storage,
            )
            .unwrap();

        assert!(subscription.recv().unwrap().is_empty());
        let albums = schema.table("albums").unwrap().record_schema();
        runtime
            .tick(
                vec![TableDelta {
                    variant_tag: 0,
                    table: "albums".to_owned(),
                    descriptor: albums,
                    deltas: vec![RecordDelta {
                        record: albums
                            .create(&[Value::U64(1), Value::String("one".to_owned())])
                            .unwrap()
                            .into(),
                        weight: 1,
                    }],
                }],
                &storage,
            )
            .unwrap();

        assert_eq!(
            subscription.recv().unwrap().to_values().unwrap(),
            vec![(
                vec![
                    Value::U64(1),
                    Value::Nullable(Some(Box::new(Value::Nullable(None)))),
                ],
                1,
            )],
        );
    }

    #[test]
    fn cold_project_hydration_materializes_literal_and_typed_null_columns() {
        let schema = albums_schema();
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let storage = crate::storage::MemoryStorage::new(&["albums"]);
        let albums = schema.table("albums").unwrap().record_schema();
        let store = RecordStore::new(&storage, "albums", &albums);
        let first = albums
            .create(&[Value::U64(1), Value::String("one".to_owned())])
            .unwrap();
        let second = albums
            .create(&[Value::U64(2), Value::String("two".to_owned())])
            .unwrap();
        let first = crate::records::encode_variant_record(0, &first);
        let second = crate::records::encode_variant_record(0, &second);

        store
            .write_many(&[store.set(b"1", &first), store.set(b"2", &second)])
            .unwrap();

        let subscription = runtime
            .subscribe_one_sink(
                GraphBuilder::table("albums").project_fields([
                    ProjectField::renamed("id", "id"),
                    ProjectField::literal("event_kind", LiteralValue::String("cold".to_owned())),
                    ProjectField::null_typed(
                        "missing_title",
                        ValueType::Nullable(Box::new(ValueType::String)),
                    ),
                ]),
                &storage,
            )
            .unwrap();

        let mut initial = subscription.recv().unwrap().to_values().unwrap();
        initial.sort_by_key(|(values, _)| {
            let Value::U64(id) = values[0] else {
                unreachable!()
            };
            id
        });
        assert_eq!(
            initial,
            vec![
                (
                    vec![
                        Value::U64(1),
                        Value::String("cold".to_owned()),
                        Value::Nullable(None),
                    ],
                    1,
                ),
                (
                    vec![
                        Value::U64(2),
                        Value::String("cold".to_owned()),
                        Value::Nullable(None),
                    ],
                    1,
                ),
            ]
        );
    }

    #[test]
    fn pure_copy_project_lowers_with_full_fast_mapping() {
        let schema = albums_schema();
        let mut runtime = IvmRuntime::new(schema).unwrap();
        let storage = crate::storage::MemoryStorage::new(&["albums"]);
        let subscription = runtime
            .subscribe_one_sink(
                GraphBuilder::table("albums").project(["id", "title"]),
                &storage,
            )
            .unwrap();

        assert!(subscription.recv().unwrap().is_empty());
        assert!(runtime.graph.nodes().values().any(|node| {
            let OpType::MapProject(project) = &node.descriptor.operator else {
                return false;
            };
            project.mapping == vec![(0, 0), (0, 1)]
                && project.expressions.len() == 2
                && project
                    .expressions
                    .iter()
                    .all(|expr| matches!(expr.expression, PlanExpr::Field(_)))
        }));
    }

    #[test]
    fn auto_family_hidden_field_does_not_collide_with_user_column() {
        let schema = DatabaseSchema::new([TableSchema::new(
            "records",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("__auto_binding_0", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let storage = crate::storage::MemoryStorage::new(&["records"]);
        let first = runtime
            .subscribe_one_sink(
                GraphBuilder::table("records")
                    .filter(PredicateExpr::eq("id", Value::U64(1)))
                    .project(["__auto_binding_0"]),
                &storage,
            )
            .unwrap();
        let second = runtime
            .subscribe_one_sink(
                GraphBuilder::table("records")
                    .filter(PredicateExpr::eq("id", Value::U64(2)))
                    .project(["__auto_binding_0"]),
                &storage,
            )
            .unwrap();
        assert!(first.recv().unwrap().is_empty());
        assert!(second.recv().unwrap().is_empty());
        assert_eq!(runtime.prepared_shapes.len(), 1);

        let descriptor = schema.table("records").unwrap().record_schema();
        runtime
            .tick(
                vec![TableDelta {
                    variant_tag: 0,
                    table: "records".to_owned(),
                    descriptor,
                    deltas: vec![
                        RecordDelta {
                            record: descriptor
                                .create(&[Value::U64(1), Value::String("visible-one".to_owned())])
                                .unwrap()
                                .into(),
                            weight: 1,
                        },
                        RecordDelta {
                            record: descriptor
                                .create(&[Value::U64(2), Value::String("visible-two".to_owned())])
                                .unwrap()
                                .into(),
                            weight: 1,
                        },
                    ],
                }],
                &storage,
            )
            .unwrap();

        assert_eq!(
            first.recv().unwrap().to_values().unwrap(),
            vec![(vec![Value::String("visible-one".to_owned())], 1)]
        );
        assert_eq!(
            second.recv().unwrap().to_values().unwrap(),
            vec![(vec![Value::String("visible-two".to_owned())], 1)]
        );
    }

    #[test]
    fn auto_family_multi_join_is_byte_identical_to_direct_path() {
        let schema = DatabaseSchema::new([
            TableSchema::new(
                "albums",
                [
                    ColumnSchema::new("id", ColumnType::U64),
                    ColumnSchema::new("artist_id", ColumnType::U64),
                    ColumnSchema::new("title", ColumnType::String),
                ],
            )
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
            TableSchema::new(
                "artists",
                [
                    ColumnSchema::new("id", ColumnType::U64),
                    ColumnSchema::new("name", ColumnType::String),
                ],
            )
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
            TableSchema::new(
                "labels",
                [
                    ColumnSchema::new("id", ColumnType::U64),
                    ColumnSchema::new("artist_id", ColumnType::U64),
                    ColumnSchema::new("label", ColumnType::String),
                ],
            )
            .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        ]);
        let graph = |artist_id| {
            let albums = GraphBuilder::table("albums")
                .filter(PredicateExpr::eq("artist_id", Value::U64(artist_id)));
            let album_artists = GraphBuilder::join(
                albums,
                GraphBuilder::table("artists"),
                ["artist_id"],
                ["id"],
            )
            .project_fields([
                ProjectField::renamed("left.id", "album_id"),
                ProjectField::renamed("left.artist_id", "artist_id"),
                ProjectField::renamed("left.title", "title"),
                ProjectField::renamed("right.name", "artist"),
            ]);
            GraphBuilder::join(
                album_artists,
                GraphBuilder::table("labels"),
                ["artist_id"],
                ["artist_id"],
            )
            .project_fields([
                ProjectField::renamed("left.title", "title"),
                ProjectField::renamed("left.artist", "artist"),
                ProjectField::renamed("right.label", "label"),
            ])
        };
        let albums = schema.table("albums").unwrap().record_schema();
        let artists = schema.table("artists").unwrap().record_schema();
        let labels = schema.table("labels").unwrap().record_schema();
        let deltas = vec![
            TableDelta {
                variant_tag: 0,
                table: "artists".to_owned(),
                descriptor: artists,
                deltas: vec![RecordDelta {
                    record: artists
                        .create(&[Value::U64(7), Value::String("Alice".to_owned())])
                        .unwrap()
                        .into(),
                    weight: 1,
                }],
            },
            TableDelta {
                variant_tag: 0,
                table: "labels".to_owned(),
                descriptor: labels,
                deltas: vec![RecordDelta {
                    record: labels
                        .create(&[
                            Value::U64(70),
                            Value::U64(7),
                            Value::String("Impulse".to_owned()),
                        ])
                        .unwrap()
                        .into(),
                    weight: 1,
                }],
            },
            TableDelta {
                variant_tag: 0,
                table: "albums".to_owned(),
                descriptor: albums,
                deltas: vec![RecordDelta {
                    record: albums
                        .create(&[
                            Value::U64(700),
                            Value::U64(7),
                            Value::String("Journey".to_owned()),
                        ])
                        .unwrap()
                        .into(),
                    weight: 1,
                }],
            },
        ];
        let familied_storage = crate::storage::MemoryStorage::new(&["albums", "artists", "labels"]);
        let direct_storage = crate::storage::MemoryStorage::new(&["albums", "artists", "labels"]);
        assert_auto_family_matches_direct(
            schema,
            &[graph(7), graph(8)],
            deltas,
            &familied_storage,
            &direct_storage,
        );
    }

    #[test]
    fn auto_family_recursive_shape_falls_back_to_byte_identical_direct_path() {
        let schema = edges_schema();
        let edges = schema.table("edges").unwrap().record_schema();
        let deltas = vec![TableDelta {
            variant_tag: 0,
            table: "edges".to_owned(),
            descriptor: edges,
            deltas: vec![
                RecordDelta {
                    record: edges
                        .create(&[Value::U64(1), Value::U64(1), Value::U64(2)])
                        .unwrap()
                        .into(),
                    weight: 1,
                },
                RecordDelta {
                    record: edges
                        .create(&[Value::U64(2), Value::U64(2), Value::U64(3)])
                        .unwrap()
                        .into(),
                    weight: 1,
                },
                RecordDelta {
                    record: edges
                        .create(&[Value::U64(3), Value::U64(9), Value::U64(10)])
                        .unwrap()
                        .into(),
                    weight: 1,
                },
            ],
        }];
        let familied_storage = crate::storage::MemoryStorage::new(&["edges"]);
        let direct_storage = crate::storage::MemoryStorage::new(&["edges"]);
        assert_auto_family_matches_direct_with_prepared_count(
            schema,
            &[recursive_reach_from_graph(1), recursive_reach_from_graph(9)],
            deltas,
            &familied_storage,
            &direct_storage,
            0,
        );
    }

    #[test]
    fn auto_family_arg_max_by_shape_is_byte_identical_to_direct_path() {
        let schema = DatabaseSchema::new([TableSchema::new(
            "scores",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("group_id", ColumnType::U64),
                ColumnSchema::new("score", ColumnType::U64),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
        let graph = |group_id| {
            GraphBuilder::arg_max_by(
                GraphBuilder::table("scores")
                    .filter(PredicateExpr::eq("group_id", Value::U64(group_id))),
                ["group_id"],
                ["score"],
            )
            .project(["id", "group_id", "score"])
        };
        let scores = schema.table("scores").unwrap().record_schema();
        let deltas = vec![TableDelta {
            variant_tag: 0,
            table: "scores".to_owned(),
            descriptor: scores,
            deltas: vec![
                RecordDelta {
                    record: scores
                        .create(&[Value::U64(1), Value::U64(1), Value::U64(10)])
                        .unwrap()
                        .into(),
                    weight: 1,
                },
                RecordDelta {
                    record: scores
                        .create(&[Value::U64(2), Value::U64(1), Value::U64(20)])
                        .unwrap()
                        .into(),
                    weight: 1,
                },
                RecordDelta {
                    record: scores
                        .create(&[Value::U64(3), Value::U64(2), Value::U64(15)])
                        .unwrap()
                        .into(),
                    weight: 1,
                },
            ],
        }];
        let familied_storage = crate::storage::MemoryStorage::new(&["scores"]);
        let direct_storage = crate::storage::MemoryStorage::new(&["scores"]);
        assert_auto_family_matches_direct(
            schema,
            &[graph(1), graph(2)],
            deltas,
            &familied_storage,
            &direct_storage,
        );
    }

    #[test]
    fn auto_family_excluded_recursive_shape_falls_back_to_direct_path() {
        let schema = edges_schema();
        let mut runtime = IvmRuntime::new(schema).unwrap();
        let storage = crate::storage::MemoryStorage::new(&["edges"]);
        let first = runtime
            .subscribe_one_sink(recursive_reach_from_with_union_step_graph(1), &storage)
            .unwrap();
        let second = runtime
            .subscribe_one_sink(recursive_reach_from_with_union_step_graph(9), &storage)
            .unwrap();

        assert!(first.recv().unwrap().is_empty());
        assert!(second.recv().unwrap().is_empty());
        assert!(runtime.prepared_shapes.is_empty());
        assert!(matches!(
            runtime
                .multisink_subscriptions
                .get(&first.id())
                .unwrap()
                .target,
            MultisinkSubscriptionTarget::Direct
        ));
        assert!(matches!(
            runtime
                .multisink_subscriptions
                .get(&second.id())
                .unwrap()
                .target,
            MultisinkSubscriptionTarget::Direct
        ));
    }

    #[test]
    fn subscription_retainers_keep_output_ancestors_alive() {
        let schema = albums_schema();
        let mut runtime = IvmRuntime::new(schema).unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
        let subscription = runtime
            .subscribe_one_sink(
                GraphBuilder::table("albums")
                    .filter(PredicateExpr::gt("id", Value::U64(10)))
                    .project(["title"]),
                &storage,
            )
            .unwrap();
        let output = runtime.subscription_output_node(subscription.id()).unwrap();
        let retained = runtime.retained_node_ids();

        assert_eq!(retained.len(), 3);
        assert!(retained.contains(&output));
        assert!(runtime.graph().node(output).is_some());
    }

    #[test]
    fn unsubscribe_eagerly_collects_unretained_ephemeral_nodes_and_state() {
        let schema = albums_schema();
        let mut runtime = IvmRuntime::new(schema).unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
        let subscription = runtime
            .subscribe_one_sink(GraphBuilder::table("albums"), &storage)
            .unwrap();
        let output = runtime.subscription_output_node(subscription.id()).unwrap();

        assert_eq!(runtime.retained_node_ids().len(), 1);
        assert!(
            runtime
                .node_meta
                .get(&output)
                .is_some_and(|meta| !meta.retainers.is_empty())
        );

        assert!(runtime.unsubscribe(subscription.id()));

        assert!(runtime.retained_node_ids().is_empty());
        assert!(runtime.graph().node(output).is_none());
        assert!(!runtime.node_meta.contains_key(&output));
    }

    #[test]
    fn identical_subscriptions_share_one_node_with_multiple_retainers() {
        let schema = albums_schema();
        let mut runtime = IvmRuntime::new(schema).unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["albums"]).unwrap();
        let graph = || {
            GraphBuilder::table("albums")
                .filter(PredicateExpr::gt("id", Value::U64(10)))
                .project(["title"])
        };

        let first = runtime.subscribe_one_sink(graph(), &storage).unwrap();
        let second = runtime.subscribe_one_sink(graph(), &storage).unwrap();
        let output = runtime.subscription_output_node(first.id()).unwrap();

        assert_eq!(Some(output), runtime.subscription_output_node(second.id()));
        assert_eq!(
            runtime
                .node_meta
                .get(&output)
                .map(|meta| meta.retainers.len()),
            Some(2)
        );

        assert!(runtime.unsubscribe(first.id()));
        assert!(runtime.graph().node(output).is_some());
        assert_eq!(
            runtime
                .node_meta
                .get(&output)
                .map(|meta| meta.retainers.len()),
            Some(1)
        );

        assert!(runtime.unsubscribe(second.id()));
        assert!(runtime.graph().node(output).is_none());
        assert!(!runtime.node_meta.contains_key(&output));
    }

    #[test]
    fn durable_schema_nodes_are_runtime_retainer_roots() {
        let schema = indexed_albums_schema();
        let runtime = IvmRuntime::new(schema).unwrap();
        let retained = runtime.retained_node_ids();
        let durable_nodes = retained
            .iter()
            .copied()
            .filter(|node| {
                runtime
                    .graph()
                    .node(*node)
                    .is_some_and(|node| node.is_durable())
            })
            .collect::<Vec<_>>();

        assert_eq!(durable_nodes.len(), 1);
        assert_eq!(retained.len(), 3);
    }

    #[test]
    fn unsupported_query_operator_variants_are_not_executable() {
        let schema = albums_schema();
        let storage = crate::storage::MemoryStorage::new(&["albums"]);
        let mut runtime = IvmRuntime::new(schema).unwrap();
        let input = runtime
            .add_dedup_graph(&GraphBuilder::table("albums"))
            .unwrap()
            .node;
        let output = *runtime.table_descriptor("albums").unwrap();

        let unsupported = [OpType::Distinct, OpType::Negate];

        for operator in unsupported {
            let node = runtime.graph.dedup_node(
                NodeDescriptor::new(operator, [input], output),
                NodeDurability::Ephemeral,
            );
            assert!(matches!(
                runtime.hydration_snapshot(node, &storage, HydrationMode::Ordinary),
                Err(IvmRuntimeError::UnsupportedOperator)
            ));
        }
    }

    #[test]
    fn stale_as_of_state_rejects_wrong_or_backward_logical_time() {
        let mut state = AsOf::<usize, SubTick>::new(7);

        assert!(matches!(
            state.value_at(SubTick {
                tick: 0,
                sub_tick: 0
            }),
            Err(IvmRuntimeError::StaleRuntimeState { .. })
        ));
        state
            .mark_forward_as_of(SubTick {
                tick: 0,
                sub_tick: 2,
            })
            .unwrap();
        assert_eq!(
            *state
                .value_at(SubTick {
                    tick: 0,
                    sub_tick: 2,
                })
                .unwrap(),
            7
        );
        assert!(matches!(
            state.value_at(SubTick {
                tick: 0,
                sub_tick: 1
            }),
            Err(IvmRuntimeError::StaleRuntimeState { .. })
        ));
        assert!(matches!(
            state.mark_forward_as_of(SubTick {
                tick: 0,
                sub_tick: 1
            }),
            Err(IvmRuntimeError::OutOfOrderRuntimeState { .. })
        ));
    }

    #[test]
    fn similar_join_subscriptions_share_context_independent_base_arrangements() {
        let schema = albums_artists_schema();
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["albums", "artists"]).unwrap();
        let _first = runtime
            .subscribe_one_sink(
                GraphBuilder::join(
                    GraphBuilder::table("albums"),
                    GraphBuilder::table("artists"),
                    ["artist_id"],
                    ["id"],
                ),
                &storage,
            )
            .unwrap();
        let _second = runtime
            .subscribe_one_sink(
                GraphBuilder::join(
                    GraphBuilder::table("albums").filter(PredicateExpr::gt("id", Value::U64(0))),
                    GraphBuilder::table("artists"),
                    ["artist_id"],
                    ["id"],
                ),
                &storage,
            )
            .unwrap();

        let albums = schema.table("albums").unwrap().record_schema();
        let artists = schema.table("artists").unwrap().record_schema();
        runtime
            .tick(
                vec![
                    TableDelta {
                        variant_tag: 0,
                        table: "albums".to_owned(),
                        descriptor: albums,
                        deltas: vec![RecordDelta {
                            record: albums
                                .create(&[
                                    Value::U64(7),
                                    Value::U64(11),
                                    Value::String("Blue Train".to_owned()),
                                ])
                                .unwrap()
                                .into(),
                            weight: 1,
                        }],
                    },
                    TableDelta {
                        variant_tag: 0,
                        table: "artists".to_owned(),
                        descriptor: artists,
                        deltas: vec![RecordDelta {
                            record: artists
                                .create(&[
                                    Value::U64(11),
                                    Value::String("John Coltrane".to_owned()),
                                ])
                                .unwrap()
                                .into(),
                            weight: 1,
                        }],
                    },
                ],
                &storage,
            )
            .unwrap();

        let artist_arrangements = runtime
            .arrangement_states
            .keys()
            .filter(|key| {
                key.scope == ScopeId::root()
                    && key.descriptor == artists
                    && key.fields.as_ref() == ["id"]
            })
            .count();

        assert_eq!(artist_arrangements, 1);
        let stats = runtime.stats();
        assert_eq!(stats.arrangement_count, 3);
        assert!(stats.arrangement_rows >= 2);
        assert!(stats.arrangement_encoded_bytes > 0);
        assert!(stats.logical_nodes_requested > stats.deduped_graph_nodes as u64);
        assert!(stats.dedupe_ratio() < 1.0);
    }

    #[test]
    fn recursive_recompute_reuses_graph_nodes_without_persisting_contextual_child_state() {
        let schema = edges_schema();
        let mut runtime = IvmRuntime::new(schema.clone()).unwrap();
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = RocksDbStorage::open(temp_dir.path(), &["edges"]).unwrap();
        let first = runtime
            .subscribe_one_sink(recursive_reach_graph(), &storage)
            .unwrap();
        let second = runtime
            .subscribe_one_sink(recursive_reach_graph(), &storage)
            .unwrap();

        assert_eq!(
            runtime.subscription_output_node(first.id()),
            runtime.subscription_output_node(second.id())
        );

        let edges = schema.table("edges").unwrap().record_schema();
        let table_delta = TableDelta {
            variant_tag: 0,
            table: "edges".to_owned(),
            descriptor: edges,
            deltas: vec![
                RecordDelta {
                    record: edges
                        .create(&[Value::U64(1), Value::U64(1), Value::U64(2)])
                        .unwrap()
                        .into(),
                    weight: 1,
                },
                RecordDelta {
                    record: edges
                        .create(&[Value::U64(2), Value::U64(2), Value::U64(3)])
                        .unwrap()
                        .into(),
                    weight: 1,
                },
            ],
        };
        runtime.tick(vec![table_delta], &storage).unwrap();

        assert!(
            runtime
                .operator_states
                .keys()
                .all(|key| key.scope == ScopeId::root()),
            "recursive recomputation should not leave per-context child state in runtime"
        );
    }

    #[test]
    fn key_encoding_preserves_value_order_for_index_range_scans() {
        let mut encoded = [
            Value::U64(1),
            Value::U64(256),
            Value::String("aa".to_owned()),
            Value::String("b".to_owned()),
            Value::F64(f64::NEG_INFINITY),
            Value::F64(-1.0),
            Value::F64(-0.0),
            Value::F64(0.0),
            Value::F64(1.0),
            Value::F64(f64::INFINITY),
            Value::Bytes(b"a\0b".to_vec()),
            Value::Bytes(b"a\0c".to_vec()),
        ]
        .into_iter()
        .map(|value| {
            let mut key = Vec::new();
            encode_key_part(&mut key, &value).unwrap();
            (value, key)
        })
        .collect::<Vec<_>>();

        encoded.sort_by(|left, right| left.1.cmp(&right.1));

        assert_eq!(
            encoded
                .into_iter()
                .map(|(value, _)| value)
                .collect::<Vec<_>>(),
            [
                Value::U64(1),
                Value::U64(256),
                Value::F64(f64::NEG_INFINITY),
                Value::F64(-1.0),
                Value::F64(-0.0),
                Value::F64(0.0),
                Value::F64(1.0),
                Value::F64(f64::INFINITY),
                Value::String("aa".to_owned()),
                Value::String("b".to_owned()),
                Value::Bytes(b"a\0b".to_vec()),
                Value::Bytes(b"a\0c".to_vec()),
            ]
        );
        let mut key = Vec::new();
        assert!(matches!(
            encode_key_part(&mut key, &Value::F64(f64::NAN)),
            Err(IvmRuntimeError::RecordEncoding(
                records::Error::InvalidF64NaN
            ))
        ));
    }

    #[test]
    fn record_values_canonicalize_delta_identity_and_are_rejected_as_arrangement_keys() {
        // This is deliberately a runtime-level test: consolidation identifies
        // records by their encoded bytes, so the relevant observable is the
        // delta batch produced by the maintained runtime rather than a record
        // field accessor alone.
        let child = RecordDescriptor::new([("id", ValueType::U64)]);
        let first = records::OwnedRecord::new(child.create(&[Value::U64(7)]).unwrap(), child);
        let second = records::OwnedRecord::new(child.create(&[Value::U64(7)]).unwrap(), child);
        let descriptor = RecordDescriptor::new([("child", ValueType::Record(Box::new(child)))]);
        let first_parent = descriptor.create(&[Value::Record(first)]).unwrap();
        let second_parent = descriptor.create(&[Value::Record(second)]).unwrap();

        assert_eq!(first_parent, second_parent);
        assert!(
            consolidate_deltas(vec![
                RecordDelta {
                    record: first_parent.into(),
                    weight: 1,
                },
                RecordDelta {
                    record: second_parent.into(),
                    weight: -1,
                },
            ])
            .is_empty()
        );

        let record = descriptor
            .create(&[Value::Record(records::OwnedRecord::new(
                child.create(&[Value::U64(7)]).unwrap(),
                child,
            ))])
            .unwrap();
        assert!(matches!(
            encoded_record_key_part(descriptor, &record, &[0]),
            Err(IvmRuntimeError::UnsupportedJoinKey)
        ));
    }
}
