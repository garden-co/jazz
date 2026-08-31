//! Prepared shapes, routed bindings, and subscription delivery state.

use super::evaluation_session::EvaluationInputs;
use super::*;
use crate::storage::OwnedStorage;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// Stable handle returned to callers for subscription management.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriptionId(pub(super) u64);

/// Monotone identity of one resident database publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PublicationId(pub u64);

/// Incremental terminal output together with the publication that produced it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicationUpdate<T> {
    pub publication: Option<PublicationId>,
    pub deltas: T,
}

/// One low-level subscription outcome. An error is terminal for this session.
#[derive(Clone, Debug)]
pub enum SubscriptionEvent<T> {
    Update(PublicationUpdate<T>),
    Error(SubscriptionError),
}

/// Shared evaluation failure which permanently ended a low-level subscription.
#[derive(Clone, Debug)]
pub enum SubscriptionError {
    Evaluation(Arc<IvmRuntimeError>),
    Ended,
}

impl SubscriptionError {
    pub(super) fn new(error: Arc<IvmRuntimeError>) -> Self {
        Self::Evaluation(error)
    }

    pub fn source_error(&self) -> Option<&IvmRuntimeError> {
        match self {
            Self::Evaluation(error) => Some(error.as_ref()),
            Self::Ended => None,
        }
    }
}

impl std::fmt::Display for SubscriptionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Evaluation(error) => write!(formatter, "subscription evaluation failed: {error}"),
            Self::Ended => formatter.write_str("subscription ended"),
        }
    }
}

impl std::error::Error for SubscriptionError {}

impl SubscriptionId {
    pub(super) fn retainer_key(self) -> String {
        self.0.to_string()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PreparedShapeId(pub(super) u64);

impl PreparedShapeId {
    pub(super) fn retainer_key(self) -> String {
        self.0.to_string()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedShape {
    pub(super) id: PreparedShapeId,
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
    pub(super) inner: MultisinkSubscription,
    pub(super) sink: String,
    pub(super) output: RecordDescriptor,
}

impl Subscription {
    pub fn id(&self) -> SubscriptionId {
        self.inner.id()
    }

    pub fn recv(&self) -> Result<RecordDeltas, RecvError> {
        if let Some(initial) = self.take_initial() {
            return Ok(initial);
        }
        self.inner
            .recv()
            .map(|deltas| self.extract_sink_deltas(deltas))
    }

    pub fn recv_with_publication(&self) -> Result<PublicationUpdate<RecordDeltas>, RecvError> {
        self.inner
            .recv_with_publication()
            .map(|update| PublicationUpdate {
                publication: update.publication,
                deltas: self.extract_sink_deltas(update.deltas),
            })
    }

    pub fn try_recv(&self) -> Result<RecordDeltas, TryRecvError> {
        if let Some(initial) = self.take_initial() {
            return Ok(initial);
        }
        self.inner
            .try_recv()
            .map(|deltas| self.extract_sink_deltas(deltas))
    }

    pub fn poll_next(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<RecordDeltas, RecvError>> {
        self.poll_next_with_publication(cx)
            .map(|result| result.map(|update| update.deltas))
    }

    pub fn poll_next_with_publication(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<PublicationUpdate<RecordDeltas>, RecvError>> {
        self.inner.poll_next_with_publication(cx).map(|result| {
            result.map(|update| PublicationUpdate {
                publication: update.publication,
                deltas: self.extract_sink_deltas(update.deltas),
            })
        })
    }

    pub fn poll_next_event(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<SubscriptionEvent<RecordDeltas>> {
        self.inner.poll_next_event(cx).map(|event| match event {
            SubscriptionEvent::Update(update) => SubscriptionEvent::Update(PublicationUpdate {
                publication: update.publication,
                deltas: self.extract_sink_deltas(update.deltas),
            }),
            SubscriptionEvent::Error(error) => SubscriptionEvent::Error(error),
        })
    }

    pub fn try_recv_with_publication(
        &self,
    ) -> Result<PublicationUpdate<RecordDeltas>, TryRecvError> {
        self.inner
            .try_recv_with_publication()
            .map(|update| PublicationUpdate {
                publication: update.publication,
                deltas: self.extract_sink_deltas(update.deltas),
            })
    }

    /// Take the complete value captured when this terminal session opened.
    ///
    /// Once this returns `Some`, every later value received from this session
    /// is an incremental delta relative to that initial value and its preceding
    /// deltas.
    pub fn take_initial(&self) -> Option<RecordDeltas> {
        self.inner
            .take_initial()
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

#[derive(Clone, Debug)]
pub(super) struct QueuedMultisinkDeltas {
    // Explicit fragment-output drain channel: once a tick computes incremental
    // subscription output, this queue owns delivery until the receiver drains
    // it. The initial snapshot is owned separately by MultisinkSubscription.
    // Eval memo is only a recompute cache and may be evicted independently.
    pub(super) deltas: MultisinkDeltas,
    pub(super) publication: Option<PublicationId>,
}

impl QueuedMultisinkDeltas {
    pub(super) fn new(deltas: MultisinkDeltas) -> Self {
        Self {
            deltas,
            publication: None,
        }
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

/// Receiving end of a live multisink graph subscription.
#[derive(Debug)]
pub struct MultisinkSubscription {
    pub(super) id: SubscriptionId,
    pub(super) initial: Arc<Mutex<Option<MultisinkDeltas>>>,
    pub(super) receiver: Receiver<Result<QueuedMultisinkDeltas, SubscriptionError>>,
    pub(super) waiter: Arc<Mutex<Option<std::task::Waker>>>,
    pub(super) _receiver_liveness: Arc<()>,
}

impl MultisinkSubscription {
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    pub fn recv(&self) -> Result<MultisinkDeltas, RecvError> {
        if let Some(initial) = self.take_initial() {
            return Ok(initial);
        }
        match self.receiver.recv()? {
            Ok(queued) => Ok(queued.deltas),
            Err(_) => Err(RecvError),
        }
    }

    pub fn recv_with_publication(&self) -> Result<PublicationUpdate<MultisinkDeltas>, RecvError> {
        if let Some(initial) = self.take_initial() {
            return Ok(PublicationUpdate {
                publication: None,
                deltas: initial,
            });
        }
        match self.receiver.recv()? {
            Ok(queued) => Ok(PublicationUpdate {
                publication: queued.publication,
                deltas: queued.deltas,
            }),
            Err(_) => Err(RecvError),
        }
    }

    pub fn try_recv(&self) -> Result<MultisinkDeltas, TryRecvError> {
        if let Some(initial) = self.take_initial() {
            return Ok(initial);
        }
        match self.receiver.try_recv()? {
            Ok(queued) => Ok(queued.deltas),
            Err(_) => Err(TryRecvError::Disconnected),
        }
    }

    pub fn poll_next(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<MultisinkDeltas, RecvError>> {
        self.poll_next_with_publication(cx)
            .map(|result| result.map(|update| update.deltas))
    }

    pub fn poll_next_with_publication(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<PublicationUpdate<MultisinkDeltas>, RecvError>> {
        self.poll_next_event(cx).map(|event| match event {
            SubscriptionEvent::Update(update) => Ok(update),
            SubscriptionEvent::Error(_) => Err(RecvError),
        })
    }

    pub fn poll_next_event(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<SubscriptionEvent<MultisinkDeltas>> {
        if let Some(initial) = self.take_initial() {
            return std::task::Poll::Ready(SubscriptionEvent::Update(PublicationUpdate {
                publication: None,
                deltas: initial,
            }));
        }
        match self.receiver.try_recv() {
            Ok(Ok(queued)) => {
                return std::task::Poll::Ready(SubscriptionEvent::Update(PublicationUpdate {
                    publication: queued.publication,
                    deltas: queued.deltas,
                }));
            }
            Ok(Err(error)) => return std::task::Poll::Ready(SubscriptionEvent::Error(error)),
            Err(TryRecvError::Disconnected) => {
                return std::task::Poll::Ready(SubscriptionEvent::Error(SubscriptionError::Ended));
            }
            Err(TryRecvError::Empty) => {}
        }
        *self
            .waiter
            .lock()
            .expect("subscription waiter mutex poisoned") = Some(cx.waker().clone());
        match self.receiver.try_recv() {
            Ok(Ok(queued)) => {
                std::task::Poll::Ready(SubscriptionEvent::Update(PublicationUpdate {
                    publication: queued.publication,
                    deltas: queued.deltas,
                }))
            }
            Ok(Err(error)) => std::task::Poll::Ready(SubscriptionEvent::Error(error)),
            Err(TryRecvError::Disconnected) => {
                std::task::Poll::Ready(SubscriptionEvent::Error(SubscriptionError::Ended))
            }
            Err(TryRecvError::Empty) => std::task::Poll::Pending,
        }
    }

    pub fn try_recv_with_publication(
        &self,
    ) -> Result<PublicationUpdate<MultisinkDeltas>, TryRecvError> {
        if let Some(initial) = self.take_initial() {
            return Ok(PublicationUpdate {
                publication: None,
                deltas: initial,
            });
        }
        match self.receiver.try_recv()? {
            Ok(queued) => Ok(PublicationUpdate {
                publication: queued.publication,
                deltas: queued.deltas,
            }),
            Err(_) => Err(TryRecvError::Disconnected),
        }
    }

    /// Take the complete multisink value captured when this terminal session
    /// opened. The receiver contains only later incremental deltas.
    pub fn take_initial(&self) -> Option<MultisinkDeltas> {
        self.initial
            .lock()
            .expect("subscription initial snapshot mutex poisoned")
            .take()
    }
}

impl PredicateExpr {
    pub(super) fn supports_indirect_literal_attempt(&self) -> bool {
        matches!(
            self,
            Self::Eq { .. }
                | Self::Neq { .. }
                | Self::Gt { .. }
                | Self::GtEq { .. }
                | Self::Lt { .. }
                | Self::LtEq { .. }
        )
    }

    pub(super) fn matches_indirect_literal_attempt(
        &self,
        record: BorrowedRecord<'_>,
        inputs: &mut EvaluationInputs,
    ) -> Result<Option<bool>, IvmRuntimeError> {
        let (field, literal, predicate): (&str, &LiteralValue, fn(std::cmp::Ordering) -> bool) =
            match self {
                Self::Eq { field, value } => (field, value, std::cmp::Ordering::is_eq),
                Self::Neq { field, value } => (field, value, |ordering| !ordering.is_eq()),
                Self::Gt { field, value } => (field, value, std::cmp::Ordering::is_gt),
                Self::GtEq { field, value } => (field, value, std::cmp::Ordering::is_ge),
                Self::Lt { field, value } => (field, value, std::cmp::Ordering::is_lt),
                Self::LtEq { field, value } => (field, value, std::cmp::Ordering::is_le),
                _ => return Ok(None),
            };
        let actual = record.get(field)?;
        let actual = match actual {
            Value::Nullable(Some(value)) => *value,
            value => value,
        };
        let Value::Large(large) = actual else {
            return Ok(None);
        };
        let literal = match literal.to_value() {
            Value::Nullable(Some(value)) => *value,
            value => value,
        };
        let inline = match (&large.kind, literal) {
            (crate::large_values::LargeValueKind::Bytes, Value::Bytes(bytes)) => bytes,
            (
                crate::large_values::LargeValueKind::String
                | crate::large_values::LargeValueKind::Json,
                Value::String(text),
            ) => text.into_bytes(),
            _ => return Ok(Some(false)),
        };
        crate::large_values::compare_inline_attempt(&large, &inline, inputs)
            .map(predicate)
            .map(Some)
    }

    pub(super) fn referenced_fields(&self, output: &mut BTreeSet<String>) {
        match self {
            Self::Eq { field, .. }
            | Self::Neq { field, .. }
            | Self::Contains { field, .. }
            | Self::Gt { field, .. }
            | Self::GtEq { field, .. }
            | Self::Lt { field, .. }
            | Self::LtEq { field, .. }
            | Self::IsNull { field }
            | Self::IsNotNull { field }
            | Self::EnumMatch { field, .. } => {
                output.insert(field.clone());
            }
            Self::EqField { field, value_field }
            | Self::NeqField { field, value_field }
            | Self::ContainsField {
                field,
                needle_field: value_field,
            } => {
                output.insert(field.clone());
                output.insert(value_field.clone());
            }
            Self::And(predicates) | Self::Or(predicates) => {
                for predicate in predicates {
                    predicate.referenced_fields(output);
                }
            }
        }
    }

    pub(super) fn matches(
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
            Self::EnumMatch {
                field,
                case_tag,
                payload,
            } => {
                let value = record.get(field)?;
                let value = match value {
                    Value::Nullable(Some(value)) => *value,
                    value => value,
                };
                match value {
                    Value::Enum(value) if value.tag() == *case_tag => {
                        payload.matches(value.record().borrowed(), comparison)
                    }
                    // Wrong arms and NULL never match. This is intentionally
                    // fail-closed so cross-case updates produce ordinary
                    // removal/insertion deltas through the existing filter
                    // operator.
                    Value::Enum(_) | Value::Nullable(None) => Ok(false),
                    _ => Ok(false),
                }
            }
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
pub(super) struct SubscriptionSender {
    sender: Arc<Mutex<Option<SubscriptionChannelSender>>>,
    waiter: Arc<Mutex<Option<std::task::Waker>>>,
}

type SubscriptionChannelSender = Sender<Result<QueuedMultisinkDeltas, SubscriptionError>>;

impl SubscriptionSender {
    pub(super) fn send(
        &self,
        queued: QueuedMultisinkDeltas,
    ) -> Result<(), std::sync::mpsc::SendError<QueuedMultisinkDeltas>> {
        let sender = self
            .sender
            .lock()
            .expect("subscription sender mutex poisoned");
        let Some(sender) = sender.as_ref() else {
            return Err(std::sync::mpsc::SendError(queued));
        };
        sender.send(Ok(queued)).map_err(|error| {
            std::sync::mpsc::SendError(error.0.expect("update send retains update"))
        })?;
        if let Some(waiter) = self
            .waiter
            .lock()
            .expect("subscription waiter mutex poisoned")
            .take()
        {
            waiter.wake();
        }
        Ok(())
    }

    pub(super) fn fail(&self, error: SubscriptionError) {
        if let Some(sender) = self
            .sender
            .lock()
            .expect("subscription sender mutex poisoned")
            .take()
        {
            let _ = sender.send(Err(error));
        }
        if let Some(waiter) = self
            .waiter
            .lock()
            .expect("subscription waiter mutex poisoned")
            .take()
        {
            waiter.wake();
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct MultisinkSubscriptionState {
    pub(super) sender: SubscriptionSender,
    pub(super) receiver_liveness: Weak<()>,
    pub(super) outputs: BTreeMap<String, CompiledNode>,
    pub(super) target: MultisinkSubscriptionTarget,
    pub(super) failed: bool,
}

#[derive(Clone, Debug)]
pub(super) enum MultisinkSubscriptionTarget {
    Direct,
    RoutedShape {
        shape_id: PreparedShapeId,
        binding_key: BindingKey,
    },
}

#[derive(Clone, Debug)]
pub(super) struct RoutedMultisinkShapeState {
    pub(super) shape: String,
    pub(super) binding_descriptor: RecordDescriptor,
    pub(super) terminals: BTreeMap<String, RoutedMultisinkTerminalState>,
    pub(super) auto_family_key: Option<AutoDirectFamilyKey>,
}

#[derive(Clone, Debug)]
pub(super) struct RoutedMultisinkTerminalState {
    pub(super) terminal: RoutedMultisinkTerminal,
    pub(super) output: CompiledNode,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct BindingKey(pub(super) Vec<u8>);

#[derive(Clone)]
pub(super) struct AutoDirectFamilyKey {
    pub(super) graph: GraphBuilder,
    pub(super) binding_descriptor: RecordDescriptor,
    pub(super) binding_field: String,
    pub(super) public_fields: Vec<String>,
}

impl std::fmt::Debug for AutoDirectFamilyKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AutoDirectFamilyKey")
            .field("graph_fingerprint", &graph_builder_fingerprint(&self.graph))
            .field("binding_descriptor", &self.binding_descriptor)
            .field("binding_field", &self.binding_field)
            .field("public_fields", &self.public_fields)
            .finish()
    }
}

impl PartialEq for AutoDirectFamilyKey {
    fn eq(&self, other: &Self) -> bool {
        self.binding_descriptor == other.binding_descriptor
            && self.binding_field == other.binding_field
            && self.public_fields == other.public_fields
            && graph_builders_equal(&self.graph, &other.graph)
    }
}

impl Eq for AutoDirectFamilyKey {}

impl Hash for AutoDirectFamilyKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        graph_builder_fingerprint(&self.graph).hash(state);
        self.binding_descriptor.hash(state);
        self.binding_field.hash(state);
        self.public_fields.hash(state);
    }
}

/// Bounded structural fingerprint for auto-direct family lookup. Hash-map
/// collisions are resolved by [`graph_builders_equal`], so this hash never
/// carries semantic identity by itself.
fn graph_builder_fingerprint(graph: &GraphBuilder) -> u64 {
    let mut hashes = HashMap::<*const GraphBuilder, u64>::default();
    macro_rules! child {
        ($child:expr) => {
            *hashes
                .get(&($child.as_ref() as *const GraphBuilder))
                .expect("postorder visits graph children before their parent")
        };
    }
    for node in graph.postorder() {
        let node_ptr = node as *const GraphBuilder;
        if hashes.contains_key(&node_ptr) {
            continue;
        }
        let mut hasher = DefaultHasher::new();
        std::mem::discriminant(node).hash(&mut hasher);
        match node {
            GraphBuilder::Table {
                table,
                scan,
                variant_projection,
            } => {
                table.hash(&mut hasher);
                scan.hash(&mut hasher);
                variant_projection.hash(&mut hasher);
            }
            GraphBuilder::InlineRecords { output, records } => {
                output.hash(&mut hasher);
                records.hash(&mut hasher);
            }
            GraphBuilder::Index {
                table,
                index,
                scan,
                intersections,
                row_projection,
            } => {
                table.hash(&mut hasher);
                index.hash(&mut hasher);
                scan.hash(&mut hasher);
                intersections.hash(&mut hasher);
                row_projection.hash(&mut hasher);
            }
            GraphBuilder::FrontierSource { binding, output } => {
                binding.hash(&mut hasher);
                output.hash(&mut hasher);
            }
            GraphBuilder::BindingSource { shape, output } => {
                shape.hash(&mut hasher);
                output.hash(&mut hasher);
            }
            GraphBuilder::Recursive {
                seed,
                step,
                frontier,
                max_iters,
                truncate_at_max_iters,
            } => {
                child!(seed).hash(&mut hasher);
                child!(step).hash(&mut hasher);
                frontier.hash(&mut hasher);
                max_iters.hash(&mut hasher);
                truncate_at_max_iters.hash(&mut hasher);
            }
            GraphBuilder::Filter {
                input,
                predicate,
                comparison,
            } => {
                child!(input).hash(&mut hasher);
                predicate.hash(&mut hasher);
                comparison.hash(&mut hasher);
            }
            GraphBuilder::UnwrapNullable { input, field } => {
                child!(input).hash(&mut hasher);
                field.hash(&mut hasher);
            }
            GraphBuilder::Unnest {
                input,
                array_field,
                element_field,
            } => {
                child!(input).hash(&mut hasher);
                array_field.hash(&mut hasher);
                element_field.hash(&mut hasher);
            }
            GraphBuilder::VariantProject { input, field, case } => {
                child!(input).hash(&mut hasher);
                field.hash(&mut hasher);
                case.hash(&mut hasher);
            }
            GraphBuilder::Project { input, fields } => {
                child!(input).hash(&mut hasher);
                fields.hash(&mut hasher);
            }
            GraphBuilder::StreamingChecksum {
                input,
                field,
                output_field,
                window_bytes,
                max_bytes_per_turn,
            } => {
                child!(input).hash(&mut hasher);
                field.hash(&mut hasher);
                output_field.hash(&mut hasher);
                window_bytes.hash(&mut hasher);
                max_bytes_per_turn.hash(&mut hasher);
            }
            GraphBuilder::Union { inputs } => {
                for input in inputs {
                    child!(input).hash(&mut hasher);
                }
            }
            GraphBuilder::Join {
                left,
                right,
                left_on,
                right_on,
                comparison,
            }
            | GraphBuilder::SemiJoin {
                left,
                right,
                left_on,
                right_on,
                comparison,
            }
            | GraphBuilder::AntiJoin {
                left,
                right,
                left_on,
                right_on,
                comparison,
            } => {
                child!(left).hash(&mut hasher);
                child!(right).hash(&mut hasher);
                left_on.hash(&mut hasher);
                right_on.hash(&mut hasher);
                comparison.hash(&mut hasher);
            }
            GraphBuilder::ArgMaxBy {
                input,
                group_cols,
                order_cols,
            }
            | GraphBuilder::ArgMinBy {
                input,
                group_cols,
                order_cols,
            } => {
                child!(input).hash(&mut hasher);
                group_cols.hash(&mut hasher);
                order_cols.hash(&mut hasher);
            }
            GraphBuilder::TopBy {
                input,
                group_cols,
                order_cols,
                tie_cols,
                offset,
                limit,
            } => {
                child!(input).hash(&mut hasher);
                group_cols.hash(&mut hasher);
                order_cols.hash(&mut hasher);
                tie_cols.hash(&mut hasher);
                offset.hash(&mut hasher);
                limit.hash(&mut hasher);
            }
            GraphBuilder::CollectBy { input, collect } => {
                child!(input).hash(&mut hasher);
                collect.hash(&mut hasher);
            }
            GraphBuilder::Aggregate {
                input,
                group_cols,
                aggregates,
            } => {
                child!(input).hash(&mut hasher);
                group_cols.hash(&mut hasher);
                aggregates.hash(&mut hasher);
            }
        }
        hashes.insert(node_ptr, hasher.finish());
    }
    *hashes
        .get(&std::ptr::from_ref(graph))
        .expect("postorder includes the graph root")
}

/// Exact, nonrecursive equality check paired with the bounded family hash.
fn graph_builders_equal(left: &GraphBuilder, right: &GraphBuilder) -> bool {
    let mut pending = vec![(left, right)];
    while let Some((left, right)) = pending.pop() {
        match (left, right) {
            (
                GraphBuilder::Table {
                    table: a,
                    scan: b,
                    variant_projection: c,
                },
                GraphBuilder::Table {
                    table: x,
                    scan: y,
                    variant_projection: z,
                },
            ) if a == x && b == y && c == z => {}
            (
                GraphBuilder::InlineRecords {
                    output: a,
                    records: b,
                },
                GraphBuilder::InlineRecords {
                    output: x,
                    records: y,
                },
            ) if a == x && b == y => {}
            (
                GraphBuilder::Index {
                    table: a,
                    index: b,
                    scan: c,
                    intersections: d,
                    row_projection: e,
                },
                GraphBuilder::Index {
                    table: x,
                    index: y,
                    scan: z,
                    intersections: w,
                    row_projection: v,
                },
            ) if a == x && b == y && c == z && d == w && e == v => {}
            (
                GraphBuilder::FrontierSource {
                    binding: a,
                    output: b,
                },
                GraphBuilder::FrontierSource {
                    binding: x,
                    output: y,
                },
            ) if a == x && b == y => {}
            (
                GraphBuilder::BindingSource {
                    shape: a,
                    output: b,
                },
                GraphBuilder::BindingSource {
                    shape: x,
                    output: y,
                },
            ) if a == x && b == y => {}
            (
                GraphBuilder::Recursive {
                    seed: a,
                    step: b,
                    frontier: c,
                    max_iters: d,
                    truncate_at_max_iters: e,
                },
                GraphBuilder::Recursive {
                    seed: x,
                    step: y,
                    frontier: z,
                    max_iters: w,
                    truncate_at_max_iters: v,
                },
            ) if c == z && d == w && e == v => {
                pending.extend([(a.as_ref(), x.as_ref()), (b.as_ref(), y.as_ref())])
            }
            (
                GraphBuilder::Filter {
                    input: a,
                    predicate: b,
                    comparison: c,
                },
                GraphBuilder::Filter {
                    input: x,
                    predicate: y,
                    comparison: z,
                },
            ) if b == y && c == z => pending.push((a, x)),
            (
                GraphBuilder::UnwrapNullable { input: a, field: b },
                GraphBuilder::UnwrapNullable { input: x, field: y },
            ) if b == y => pending.push((a, x)),
            (
                GraphBuilder::Unnest {
                    input: a,
                    array_field: b,
                    element_field: c,
                },
                GraphBuilder::Unnest {
                    input: x,
                    array_field: y,
                    element_field: z,
                },
            ) if b == y && c == z => pending.push((a, x)),
            (
                GraphBuilder::VariantProject {
                    input: a,
                    field: b,
                    case: c,
                },
                GraphBuilder::VariantProject {
                    input: x,
                    field: y,
                    case: z,
                },
            ) if b == y && c == z => pending.push((a, x)),
            (
                GraphBuilder::Project {
                    input: a,
                    fields: b,
                },
                GraphBuilder::Project {
                    input: x,
                    fields: y,
                },
            ) if b == y => pending.push((a, x)),
            (
                GraphBuilder::StreamingChecksum {
                    input: a,
                    field: b,
                    output_field: c,
                    window_bytes: d,
                    max_bytes_per_turn: e,
                },
                GraphBuilder::StreamingChecksum {
                    input: x,
                    field: y,
                    output_field: z,
                    window_bytes: w,
                    max_bytes_per_turn: v,
                },
            ) if b == y && c == z && d == w && e == v => pending.push((a, x)),
            (GraphBuilder::Union { inputs: a }, GraphBuilder::Union { inputs: x })
                if a.len() == x.len() =>
            {
                pending.extend(a.iter().zip(x).map(|(a, x)| (a.as_ref(), x.as_ref())))
            }
            (
                GraphBuilder::Join {
                    left: a,
                    right: b,
                    left_on: c,
                    right_on: d,
                    comparison: e,
                },
                GraphBuilder::Join {
                    left: x,
                    right: y,
                    left_on: z,
                    right_on: w,
                    comparison: v,
                },
            ) if c == z && d == w && e == v => {
                pending.extend([(a.as_ref(), x.as_ref()), (b.as_ref(), y.as_ref())])
            }
            (
                GraphBuilder::SemiJoin {
                    left: a,
                    right: b,
                    left_on: c,
                    right_on: d,
                    comparison: e,
                },
                GraphBuilder::SemiJoin {
                    left: x,
                    right: y,
                    left_on: z,
                    right_on: w,
                    comparison: v,
                },
            ) if c == z && d == w && e == v => {
                pending.extend([(a.as_ref(), x.as_ref()), (b.as_ref(), y.as_ref())])
            }
            (
                GraphBuilder::AntiJoin {
                    left: a,
                    right: b,
                    left_on: c,
                    right_on: d,
                    comparison: e,
                },
                GraphBuilder::AntiJoin {
                    left: x,
                    right: y,
                    left_on: z,
                    right_on: w,
                    comparison: v,
                },
            ) if c == z && d == w && e == v => {
                pending.extend([(a.as_ref(), x.as_ref()), (b.as_ref(), y.as_ref())])
            }
            (
                GraphBuilder::ArgMaxBy {
                    input: a,
                    group_cols: b,
                    order_cols: c,
                },
                GraphBuilder::ArgMaxBy {
                    input: x,
                    group_cols: y,
                    order_cols: z,
                },
            ) if b == y && c == z => pending.push((a, x)),
            (
                GraphBuilder::ArgMinBy {
                    input: a,
                    group_cols: b,
                    order_cols: c,
                },
                GraphBuilder::ArgMinBy {
                    input: x,
                    group_cols: y,
                    order_cols: z,
                },
            ) if b == y && c == z => pending.push((a, x)),
            (
                GraphBuilder::TopBy {
                    input: a,
                    group_cols: b,
                    order_cols: c,
                    tie_cols: d,
                    offset: e,
                    limit: f,
                },
                GraphBuilder::TopBy {
                    input: x,
                    group_cols: y,
                    order_cols: z,
                    tie_cols: w,
                    offset: v,
                    limit: u,
                },
            ) if b == y && c == z && d == w && e == v && f == u => pending.push((a, x)),
            (
                GraphBuilder::CollectBy {
                    input: a,
                    collect: b,
                },
                GraphBuilder::CollectBy {
                    input: x,
                    collect: y,
                },
            ) if b == y => pending.push((a, x)),
            (
                GraphBuilder::Aggregate {
                    input: a,
                    group_cols: b,
                    aggregates: c,
                },
                GraphBuilder::Aggregate {
                    input: x,
                    group_cols: y,
                    aggregates: z,
                },
            ) if b == y && c == z => pending.push((a, x)),
            _ => return false,
        }
    }
    true
}

struct AutoDirectFamilyPlan {
    pub(super) key: AutoDirectFamilyKey,
    pub(super) graph: GraphBuilder,
    pub(super) shape: String,
    pub(super) binding_descriptor: RecordDescriptor,
    pub(super) binding_field: String,
    pub(super) binding_value: Value,
    pub(super) public_fields: Vec<String>,
}

#[derive(Clone, Debug)]
pub(super) struct BindingSourceState {
    pub(super) descriptor: RecordDescriptor,
    pub(super) refcounts: HashMap<BindingKey, usize>,
}

#[derive(Clone, Debug)]
pub(super) struct BindingDelta {
    pub(super) shape: String,
    pub(super) descriptor: RecordDescriptor,
    pub(super) deltas: Vec<RecordDelta>,
}

/// Result of lowering a graph-builder fragment into the deduplicated graph.
#[derive(Clone, Debug)]
pub(super) struct CompiledNode {
    pub(super) output: RecordDescriptor,
    pub(super) node: NodeId,
    /// The `TopBy` node that defines ordering of public terminal roots.
    ///
    /// This is explicit lowering metadata: walking graph ancestors is
    /// ambiguous once a structured plan contains independently ordered nested
    /// collections.
    pub(super) root_ordering_node: Option<NodeId>,
}

/// Descriptor plus a batch of weighted encoded record changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordDeltas {
    pub descriptor: RecordDescriptor,
    pub deltas: Vec<RecordDelta>,
}

impl RecordDeltas {
    pub(super) fn empty(descriptor: RecordDescriptor) -> Self {
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

pub(super) fn record_deltas_encoded_bytes(deltas: &RecordDeltas) -> usize {
    deltas.deltas.iter().map(|delta| delta.record.len()).sum()
}

pub(super) fn multisink_deltas_record_count(deltas: &MultisinkDeltas) -> usize {
    deltas
        .sinks
        .values()
        .map(|records| records.deltas.len())
        .sum()
}

pub(super) fn multisink_deltas_encoded_bytes(deltas: &MultisinkDeltas) -> usize {
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
    S: OrderedKvStorage + ?Sized,
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
            input: Arc::new(input),
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

pub(super) fn count_builder_nodes(graph: &GraphBuilder) -> usize {
    graph.postorder().len()
}

pub(super) fn builder_contains_binding_source(graph: &GraphBuilder) -> bool {
    graph
        .postorder()
        .iter()
        .any(|node| matches!(node, GraphBuilder::BindingSource { .. }))
}

#[derive(Clone)]
struct LiftedLiteralFilter {
    pub(super) graph: GraphBuilder,
    pub(super) value: LiteralValue,
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
    for node in graph.postorder() {
        let output = runtime.infer_builder_output(node)?;
        occupied.extend(
            output
                .fields()
                .iter()
                .filter_map(|field| field.name.as_ref().cloned()),
        );
    }
    Ok(())
}

fn lift_literal_filter(
    runtime: &IvmRuntime,
    graph: &GraphBuilder,
    binding_field: &str,
) -> Result<Option<LiftedLiteralFilter>, IvmRuntimeError> {
    let mut lifted = HashMap::<*const GraphBuilder, Option<LiftedLiteralFilter>>::default();
    for node in graph.postorder() {
        let node_ptr = node as *const GraphBuilder;
        if lifted.contains_key(&node_ptr) {
            continue;
        }
        let result = lift_literal_filter_node(runtime, node, binding_field, &lifted)?;
        lifted.insert(node_ptr, result);
    }
    Ok(lifted
        .remove(&std::ptr::from_ref(graph))
        .expect("postorder includes the graph root"))
}

fn lift_literal_filter_node(
    runtime: &IvmRuntime,
    graph: &GraphBuilder,
    binding_field: &str,
    lifted_children: &HashMap<*const GraphBuilder, Option<LiftedLiteralFilter>>,
) -> Result<Option<LiftedLiteralFilter>, IvmRuntimeError> {
    macro_rules! lifted_child {
        ($child:expr) => {
            lifted_children
                .get(&($child.as_ref() as *const GraphBuilder))
                .expect("postorder visits graph children before their parent")
                .clone()
        };
    }
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
            if let Some(lifted) = lifted_child!(input) {
                return Ok(Some(LiftedLiteralFilter {
                    graph: GraphBuilder::Filter {
                        input: Arc::new(lifted.graph),
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
                if let Some(lifted) = lifted_child!(left) {
                    let joined = GraphBuilder::Join {
                        left: Arc::new(lifted.graph),
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
                if let Some(lifted) = lifted_child!(right) {
                    let joined = GraphBuilder::Join {
                        left: left.clone(),
                        right: Arc::new(lifted.graph),
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
                            omit_unrepresentable,
                        } => {
                            let source =
                                project_source_from_joined_filter_input(&input_output, source)?;
                            Ok(if *omit_unrepresentable {
                                ProjectField::recursive_enum_remap_omitting_unrepresentable(
                                    source,
                                    field.output_name.clone(),
                                    target.clone(),
                                    remaps.clone(),
                                )
                            } else {
                                ProjectField::recursive_enum_remap(
                                    source,
                                    field.output_name.clone(),
                                    target.clone(),
                                    remaps.clone(),
                                )
                            })
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
            let Some(lifted) = lifted_child!(input) else {
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
                    input: Arc::new(lifted.graph),
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
            if let Some(lifted) = lifted_child!(left) {
                let original_output = runtime.infer_builder_output(graph)?;
                let joined = GraphBuilder::Join {
                    left: Arc::new(lifted.graph),
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
            if let Some(lifted) = lifted_child!(right) {
                let original_output = runtime.infer_builder_output(graph)?;
                let joined = GraphBuilder::Join {
                    left: left.clone(),
                    right: Arc::new(lifted.graph),
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
            let Some(lifted) = lifted_child!(left) else {
                return Ok(None);
            };
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::AntiJoin {
                    left: Arc::new(lifted.graph),
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
            let Some(lifted) = lifted_child!(left) else {
                return Ok(None);
            };
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::SemiJoin {
                    left: Arc::new(lifted.graph),
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
            let Some(lifted) = lifted_child!(input) else {
                return Ok(None);
            };
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::UnwrapNullable {
                    input: Arc::new(lifted.graph),
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
            let Some(lifted) = lifted_child!(input) else {
                return Ok(None);
            };
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::Unnest {
                    input: Arc::new(lifted.graph),
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
            let Some(lifted) = lifted_child!(input) else {
                return Ok(None);
            };
            let mut group_cols = group_cols.clone();
            group_cols.push(FieldRef::name(binding_field));
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::ArgMaxBy {
                    input: Arc::new(lifted.graph),
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
            let Some(lifted) = lifted_child!(input) else {
                return Ok(None);
            };
            let mut group_cols = group_cols.clone();
            group_cols.push(FieldRef::name(binding_field));
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::ArgMinBy {
                    input: Arc::new(lifted.graph),
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
            let Some(lifted) = lifted_child!(input) else {
                return Ok(None);
            };
            let mut group_cols = group_cols.clone();
            group_cols.push(FieldRef::name(binding_field));
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::TopBy {
                    input: Arc::new(lifted.graph),
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
            let Some(lifted) = lifted_child!(input) else {
                return Ok(None);
            };
            let mut group_cols = group_cols.clone();
            group_cols.push(FieldRef::name(binding_field));
            Ok(Some(LiftedLiteralFilter {
                graph: GraphBuilder::Aggregate {
                    input: Arc::new(lifted.graph),
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
        | GraphBuilder::BindingSource { .. }
        | GraphBuilder::StreamingChecksum { .. } => Ok(None),
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
                    omit_unrepresentable,
                } => {
                    let source = field_ref_name(&original_output, source)?;
                    if rewritten_output.field_index(&source).is_none() {
                        return Err(IvmRuntimeError::GraphFieldNotFound(source));
                    }
                    return Ok(if *omit_unrepresentable {
                        ProjectField::recursive_enum_remap_omitting_unrepresentable(
                            source,
                            field.output_name.clone(),
                            target.clone(),
                            remaps.clone(),
                        )
                    } else {
                        ProjectField::recursive_enum_remap(
                            source,
                            field.output_name.clone(),
                            target.clone(),
                            remaps.clone(),
                        )
                    });
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
        input: Arc::new(graph),
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
    let mut outputs = HashMap::<*const GraphBuilder, bool>::default();
    macro_rules! child {
        ($child:expr) => {
            *outputs
                .get(&($child.as_ref() as *const GraphBuilder))
                .expect("postorder visits graph children before their parent")
        };
    }
    for node in graph.postorder() {
        let node_ptr = node as *const GraphBuilder;
        if outputs.contains_key(&node_ptr) {
            continue;
        }
        let output = match node {
            GraphBuilder::BindingSource { output, .. }
            | GraphBuilder::FrontierSource { output, .. }
            | GraphBuilder::InlineRecords { output, .. } => {
                output.field_index(binding_field).is_some()
            }
            GraphBuilder::Project { fields, .. } => fields
                .iter()
                .any(|field| field.output_name == binding_field),
            GraphBuilder::StreamingChecksum {
                input,
                field,
                output_field,
                ..
            } => {
                output_field == binding_field
                    || (!matches!(field, FieldRef::Name(name) if name == binding_field)
                        && child!(input))
            }
            GraphBuilder::Filter { input, .. }
            | GraphBuilder::UnwrapNullable { input, .. }
            | GraphBuilder::Unnest { input, .. }
            | GraphBuilder::ArgMaxBy { input, .. }
            | GraphBuilder::ArgMinBy { input, .. }
            | GraphBuilder::TopBy { input, .. }
            | GraphBuilder::CollectBy { input, .. }
            | GraphBuilder::Aggregate { input, .. } => child!(input),
            GraphBuilder::Recursive { seed, .. } => child!(seed),
            GraphBuilder::Join { left, right, .. }
            | GraphBuilder::SemiJoin { left, right, .. }
            | GraphBuilder::AntiJoin { left, right, .. } => child!(left) || child!(right),
            GraphBuilder::Union { inputs } => inputs.iter().any(|input| child!(input)),
            GraphBuilder::Table { .. }
            | GraphBuilder::Index { .. }
            | GraphBuilder::VariantProject { .. } => false,
        };
        outputs.insert(node_ptr, output);
    }
    *outputs
        .get(&std::ptr::from_ref(graph))
        .expect("postorder includes the graph root")
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
                input: Arc::new(input),
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
                input: Arc::new(input),
                fields,
            })
        }
        GraphBuilder::UnwrapNullable { input, field } => {
            let input =
                propagate_binding_through_frontier(input, frontier, binding_field, binding_type)?;
            Some(GraphBuilder::UnwrapNullable {
                input: Arc::new(input),
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
                input: Arc::new(input),
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
                left: Arc::new(left),
                right: Arc::new(right),
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
                left: Arc::new(left),
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
                left: Arc::new(left),
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
        | GraphBuilder::StreamingChecksum { .. }
        | GraphBuilder::VariantProject { .. } => None,
    }
}

fn replace_binding_shape(graph: &GraphBuilder, shape: &str) -> GraphBuilder {
    // Auto-direct planning can build a long chain of otherwise ordinary
    // operators. Rewriting the binding source must therefore be bounded just
    // like compilation: transform the owned graph in iterative postorder
    // rather than recursing once per operator.
    let mut rewritten = HashMap::<*const GraphBuilder, Arc<GraphBuilder>>::default();
    macro_rules! child {
        ($child:expr) => {{
            let child = $child;
            rewritten
                .get(&(child.as_ref() as *const GraphBuilder))
                .expect("postorder rewrites every graph child before its parent")
                .clone()
        }};
    }

    for node in graph.postorder() {
        let node_ptr = node as *const GraphBuilder;
        if rewritten.contains_key(&node_ptr) {
            continue;
        }
        let replacement = match node {
            GraphBuilder::BindingSource { output, .. } => {
                GraphBuilder::binding_source(shape, *output)
            }
            GraphBuilder::Recursive {
                seed,
                step,
                frontier,
                max_iters,
                truncate_at_max_iters,
            } => GraphBuilder::Recursive {
                seed: child!(seed),
                step: child!(step),
                frontier: frontier.clone(),
                max_iters: *max_iters,
                truncate_at_max_iters: *truncate_at_max_iters,
            },
            GraphBuilder::Filter {
                input,
                predicate,
                comparison,
            } => GraphBuilder::Filter {
                input: child!(input),
                predicate: predicate.clone(),
                comparison: *comparison,
            },
            GraphBuilder::Project { input, fields } => GraphBuilder::Project {
                input: child!(input),
                fields: fields.clone(),
            },
            GraphBuilder::UnwrapNullable { input, field } => GraphBuilder::UnwrapNullable {
                input: child!(input),
                field: field.clone(),
            },
            GraphBuilder::Unnest {
                input,
                array_field,
                element_field,
            } => GraphBuilder::Unnest {
                input: child!(input),
                array_field: array_field.clone(),
                element_field: element_field.clone(),
            },
            GraphBuilder::ArgMaxBy {
                input,
                group_cols,
                order_cols,
            } => GraphBuilder::ArgMaxBy {
                input: child!(input),
                group_cols: group_cols.clone(),
                order_cols: order_cols.clone(),
            },
            GraphBuilder::ArgMinBy {
                input,
                group_cols,
                order_cols,
            } => GraphBuilder::ArgMinBy {
                input: child!(input),
                group_cols: group_cols.clone(),
                order_cols: order_cols.clone(),
            },
            GraphBuilder::TopBy {
                input,
                group_cols,
                order_cols,
                tie_cols,
                offset,
                limit,
            } => GraphBuilder::TopBy {
                input: child!(input),
                group_cols: group_cols.clone(),
                order_cols: order_cols.clone(),
                tie_cols: tie_cols.clone(),
                offset: *offset,
                limit: *limit,
            },
            GraphBuilder::Aggregate {
                input,
                group_cols,
                aggregates,
            } => GraphBuilder::Aggregate {
                input: child!(input),
                group_cols: group_cols.clone(),
                aggregates: aggregates.clone(),
            },
            GraphBuilder::Union { inputs } => GraphBuilder::Union {
                inputs: inputs.iter().map(|input| child!(input)).collect(),
            },
            GraphBuilder::Join {
                left,
                right,
                left_on,
                right_on,
                comparison,
            } => GraphBuilder::Join {
                left: child!(left),
                right: child!(right),
                left_on: left_on.clone(),
                right_on: right_on.clone(),
                comparison: *comparison,
            },
            GraphBuilder::SemiJoin {
                left,
                right,
                left_on,
                right_on,
                comparison,
            } => GraphBuilder::SemiJoin {
                left: child!(left),
                right: child!(right),
                left_on: left_on.clone(),
                right_on: right_on.clone(),
                comparison: *comparison,
            },
            GraphBuilder::AntiJoin {
                left,
                right,
                left_on,
                right_on,
                comparison,
            } => GraphBuilder::AntiJoin {
                left: child!(left),
                right: child!(right),
                left_on: left_on.clone(),
                right_on: right_on.clone(),
                comparison: *comparison,
            },
            // Preserve the former behavior for operators auto-direct does not
            // rewrite through: their enclosing graph stays byte-for-byte
            // equivalent, even if a child happens to contain a binding.
            node => node.clone(),
        };
        rewritten.insert(node_ptr, Arc::new(replacement));
    }

    Arc::try_unwrap(
        rewritten
            .remove(&std::ptr::from_ref(graph))
            .expect("postorder includes the graph root"),
    )
    .expect("rewritten graph root has no parent")
}

impl IvmRuntime {
    pub async fn subscribe_one_sink<S>(
        &mut self,
        graph: GraphBuilder,
        storage: &Rc<S>,
    ) -> Result<Subscription, IvmRuntimeError>
    where
        S: OrderedKvStorage + 'static,
    {
        self.subscribe_one_sink_with_waker(graph, storage, None)
            .await
    }

    /// Internal direct-subscription opening with an optional continuation
    /// waker. Runtime owners use the multi-sink entrypoint; this exists for
    /// the database convenience API to drain only its resident continuation
    /// chain without losing a cold-storage wake.
    pub(crate) async fn subscribe_one_sink_with_waker<S>(
        &mut self,
        graph: GraphBuilder,
        storage: &Rc<S>,
        progress_waker: Option<&Waker>,
    ) -> Result<Subscription, IvmRuntimeError>
    where
        S: OrderedKvStorage + 'static,
    {
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
                let shape = self
                    .prepare_one_sink(
                        plan.graph.clone(),
                        plan.shape.clone(),
                        plan.binding_descriptor,
                        [plan.binding_field.clone()],
                        storage.as_ref(),
                    )
                    .await?;
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
            return self.bind_shape_one_sink_with_waker(
                shape_id,
                &[plan.binding_value],
                storage,
                progress_waker,
            );
        }
        let multisink = self.subscribe_staged(vec![(DEFAULT_SINK.to_owned(), graph)], storage)?;
        let subscription = self.single_sink_subscription(multisink, DEFAULT_SINK)?;
        self.poll_ready_subscription_work_now_with_waker(progress_waker)?;
        Ok(subscription)
    }

    pub fn subscribe<I, K, S>(
        &mut self,
        sinks: I,
        storage: &Rc<S>,
    ) -> Result<MultisinkSubscription, IvmRuntimeError>
    where
        I: IntoIterator<Item = (K, GraphBuilder)>,
        K: Into<String>,
        S: OrderedKvStorage + 'static,
    {
        self.subscribe_with_waker(sinks, storage, None)
    }

    /// Internal owner-loop counterpart to [`Self::subscribe`].
    pub(crate) fn subscribe_with_waker<I, K, S>(
        &mut self,
        sinks: I,
        storage: &Rc<S>,
        progress_waker: Option<&Waker>,
    ) -> Result<MultisinkSubscription, IvmRuntimeError>
    where
        I: IntoIterator<Item = (K, GraphBuilder)>,
        K: Into<String>,
        S: OrderedKvStorage + 'static,
    {
        let sinks = sinks
            .into_iter()
            .map(|(sink, graph)| (sink.into(), graph))
            .collect::<Vec<_>>();
        let subscription = self.subscribe_staged(sinks, storage)?;
        self.poll_ready_subscription_work_now_with_waker(progress_waker)?;
        Ok(subscription)
    }

    /// Publish all subscription work that can complete from resident inputs
    /// before returning control to the caller. Cold inputs remain queued for
    /// the runtime owner to resume when storage wakes them.
    fn poll_ready_subscription_work_now_with_waker(
        &mut self,
        progress_waker: Option<&Waker>,
    ) -> Result<(), IvmRuntimeError> {
        let mut cx = Context::from_waker(progress_waker.unwrap_or(Waker::noop()));
        match self.poll_pending_incremental(&mut cx) {
            Poll::Ready(result) => result,
            Poll::Pending if progress_waker.is_some() => Ok(()),
            Poll::Pending => {
                // A direct opening owns every explicitly retained CPU slice,
                // including one queued behind an older cold hydration. Do not
                // infer that ownership from a wake: scan the runtime's
                // per-evaluation continuation state and leave all storage
                // requests untouched for a later owner.
                while self.has_resident_continuation() {
                    match self.poll_resident_incremental(&mut cx) {
                        Poll::Ready(result) => return result,
                        Poll::Pending => {}
                    }
                }
                Ok(())
            }
        }
    }

    fn subscribe_staged<S>(
        &mut self,
        sinks: Vec<(String, GraphBuilder)>,
        storage: &Rc<S>,
    ) -> Result<MultisinkSubscription, IvmRuntimeError>
    where
        S: OrderedKvStorage + 'static,
    {
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
        let subscription_id = self.next_subscription_id();
        let (sender, receiver) = mpsc::channel();
        let waiter = Arc::new(Mutex::new(None));
        let sender = SubscriptionSender {
            sender: Arc::new(Mutex::new(Some(sender))),
            waiter: Arc::clone(&waiter),
        };
        let receiver_liveness = Arc::new(());
        // Compilation may add nodes to the shared hash-consed graph, while
        // hydration keeps its mutable evaluator state operation-local. The
        // ephemeral guard collects only unretained additions if hydration is
        // cancelled or fails. On success, adding the subscription retainers
        // before releasing the guard atomically promotes those additions into
        // live graph state without cloning unrelated runtime state.
        let outputs = {
            let mut install = super::graph_lifecycle::EphemeralGraphInstall::new(self);
            let runtime = install.runtime();
            runtime.logical_nodes_requested += sinks
                .iter()
                .map(|(_, graph)| count_builder_nodes(graph))
                .sum::<usize>() as u64;
            let mut outputs = BTreeMap::new();
            for (sink, graph) in sinks {
                let compiled = runtime.add_dedup_graph(&graph)?;
                outputs.insert(sink, compiled);
            }
            for output in outputs.values() {
                runtime.retain_as_subscription(subscription_id, output.node);
            }
            install.commit();
            outputs
        };
        self.multisink_subscriptions.insert(
            subscription_id,
            MultisinkSubscriptionState {
                sender,
                receiver_liveness: Arc::downgrade(&receiver_liveness),
                outputs: outputs.clone(),
                target: MultisinkSubscriptionTarget::Direct,
                failed: false,
            },
        );
        self.index_subscription_outputs(subscription_id, &outputs);
        let initial = Arc::new(Mutex::new(None));
        self.enqueue_subscription_hydration(
            subscription_id,
            outputs,
            OwnedStorage::new(Rc::clone(storage)),
            None,
            None,
            Arc::clone(&initial),
        )?;
        Ok(MultisinkSubscription {
            id: subscription_id,
            initial,
            receiver,
            waiter,
            _receiver_liveness: receiver_liveness,
        })
    }

    pub async fn prepare<I, S>(
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
        self.flush_pending_binding_retractions(storage).await?;
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
        storage: &Rc<S>,
    ) -> Result<MultisinkSubscription, IvmRuntimeError>
    where
        S: OrderedKvStorage + 'static,
    {
        self.bind_shape_with_public_fields(shape_id, binding_values, BTreeMap::new(), storage, None)
    }

    /// Internal owner-loop counterpart to [`Self::bind_shape`].
    pub(crate) fn bind_shape_with_waker<S>(
        &mut self,
        shape_id: PreparedShapeId,
        binding_values: &[Value],
        storage: &Rc<S>,
        progress_waker: Option<&Waker>,
    ) -> Result<MultisinkSubscription, IvmRuntimeError>
    where
        S: OrderedKvStorage + 'static,
    {
        self.bind_shape_with_public_fields(
            shape_id,
            binding_values,
            BTreeMap::new(),
            storage,
            progress_waker,
        )
    }

    fn bind_shape_with_public_fields<S>(
        &mut self,
        shape_id: PreparedShapeId,
        binding_values: &[Value],
        public_fields: BTreeMap<String, Vec<String>>,
        storage: &Rc<S>,
        progress_waker: Option<&Waker>,
    ) -> Result<MultisinkSubscription, IvmRuntimeError>
    where
        S: OrderedKvStorage + 'static,
    {
        let subscription = self.bind_shape_with_public_fields_staged(
            shape_id,
            binding_values,
            public_fields,
            storage,
        )?;
        self.poll_ready_subscription_work_now_with_waker(progress_waker)?;
        Ok(subscription)
    }

    fn bind_shape_with_public_fields_staged<S>(
        &mut self,
        shape_id: PreparedShapeId,
        binding_values: &[Value],
        public_fields: BTreeMap<String, Vec<String>>,
        storage: &Rc<S>,
    ) -> Result<MultisinkSubscription, IvmRuntimeError>
    where
        S: OrderedKvStorage + 'static,
    {
        let shape = self
            .prepared_shapes
            .get(&shape_id)
            .ok_or(IvmRuntimeError::PreparedShapeNotFound(shape_id))?
            .clone();
        let binding_record = shape.binding_descriptor.create(binding_values)?;
        let binding_key = BindingKey(binding_record);
        let subscription_id = self.next_subscription_id();
        let (outputs, binding_snapshots) = {
            let mut install = super::graph_lifecycle::EphemeralGraphInstall::new(self);
            let runtime = install.runtime();
            runtime.logical_nodes_requested += shape
                .terminals
                .values()
                .map(|terminal| count_builder_nodes(&terminal.terminal.graph) + 2)
                .sum::<usize>() as u64;
            let mut outputs = BTreeMap::new();
            for (sink, terminal) in &shape.terminals {
                let mut terminal = terminal.terminal.clone();
                if let Some(fields) = public_fields.get(sink) {
                    terminal.public_fields = fields.clone();
                }
                let graph = bound_routed_multisink_graph(&terminal, binding_values);
                let output = runtime.add_dedup_graph(&graph)?;
                outputs.insert(sink.clone(), output);
            }
            let binding_shape = runtime.binding_source_shape_name(shape_id)?;
            let cancelled_retraction =
                runtime.cancel_pending_binding_retraction(&binding_shape, &binding_key);
            let binding_delta = runtime.provisional_binding_delta(shape_id, &binding_key)?;
            let mut binding_snapshots = runtime.binding_snapshot_deltas();
            let snapshot = binding_snapshots
                .entry(binding_delta.shape.clone())
                .or_insert_with(|| RecordDeltas {
                    descriptor: binding_delta.descriptor,
                    deltas: Vec::new(),
                });
            for delta in &binding_delta.deltas {
                if delta.weight > 0
                    && !snapshot
                        .deltas
                        .iter()
                        .any(|existing| existing.record == delta.record)
                {
                    snapshot.deltas.push(delta.clone());
                }
            }
            let installed_delta = runtime.add_binding_ref(shape_id, binding_key.clone())?;
            debug_assert_eq!(installed_delta.deltas, binding_delta.deltas);
            if !cancelled_retraction {
                runtime.bump_input_frontiers(&[], std::slice::from_ref(&installed_delta));
            }
            for output in outputs.values() {
                runtime.retain_as_subscription(subscription_id, output.node);
            }
            install.commit();
            (outputs, binding_snapshots)
        };
        let (sender, receiver) = mpsc::channel();
        let waiter = Arc::new(Mutex::new(None));
        let sender = SubscriptionSender {
            sender: Arc::new(Mutex::new(Some(sender))),
            waiter: Arc::clone(&waiter),
        };
        let receiver_liveness = Arc::new(());
        self.multisink_subscriptions.insert(
            subscription_id,
            MultisinkSubscriptionState {
                sender,
                receiver_liveness: Arc::downgrade(&receiver_liveness),
                outputs: outputs.clone(),
                target: MultisinkSubscriptionTarget::RoutedShape {
                    shape_id,
                    binding_key: binding_key.clone(),
                },
                failed: false,
            },
        );
        self.index_subscription_outputs(subscription_id, &outputs);
        let initial = Arc::new(Mutex::new(None));
        self.enqueue_subscription_hydration(
            subscription_id,
            outputs,
            OwnedStorage::new(Rc::clone(storage)),
            Some(binding_snapshots),
            Some(&shape.shape),
            Arc::clone(&initial),
        )?;
        Ok(MultisinkSubscription {
            id: subscription_id,
            initial,
            receiver,
            waiter,
            _receiver_liveness: receiver_liveness,
        })
    }

    pub async fn prepare_one_sink(
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
        .await
    }

    pub async fn prepare_one_sink_with_routing(
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
        .await
    }

    pub fn bind_shape_one_sink<S>(
        &mut self,
        shape_id: PreparedShapeId,
        binding_values: &[Value],
        storage: &Rc<S>,
    ) -> Result<Subscription, IvmRuntimeError>
    where
        S: OrderedKvStorage + 'static,
    {
        self.bind_shape_one_sink_with_waker(shape_id, binding_values, storage, None)
    }

    pub(crate) fn bind_shape_one_sink_with_waker<S>(
        &mut self,
        shape_id: PreparedShapeId,
        binding_values: &[Value],
        storage: &Rc<S>,
        progress_waker: Option<&Waker>,
    ) -> Result<Subscription, IvmRuntimeError>
    where
        S: OrderedKvStorage + 'static,
    {
        let multisink = self.bind_shape_with_public_fields_staged(
            shape_id,
            binding_values,
            BTreeMap::new(),
            storage,
        )?;
        let subscription = self.single_sink_subscription(multisink, DEFAULT_SINK)?;
        self.poll_ready_subscription_work_now_with_waker(progress_waker)?;
        Ok(subscription)
    }

    pub(crate) fn bind_shape_one_sink_with_output_and_waker<S>(
        &mut self,
        shape_id: PreparedShapeId,
        binding_values: &[Value],
        public_output: RecordDescriptor,
        storage: &Rc<S>,
        progress_waker: Option<&Waker>,
    ) -> Result<Subscription, IvmRuntimeError>
    where
        S: OrderedKvStorage + 'static,
    {
        validate_public_output_for_shape(
            self.prepared_shapes
                .get(&shape_id)
                .ok_or(IvmRuntimeError::PreparedShapeNotFound(shape_id))?,
            DEFAULT_SINK,
            &public_output,
        )?;
        let public_fields = descriptor_field_names(&public_output)?;
        let multisink = self.bind_shape_with_public_fields_staged(
            shape_id,
            binding_values,
            [(DEFAULT_SINK.to_owned(), public_fields)].into(),
            storage,
        )?;
        let subscription = self.single_sink_subscription(multisink, DEFAULT_SINK)?;
        self.poll_ready_subscription_work_now_with_waker(progress_waker)?;
        Ok(subscription)
    }

    fn index_subscription_outputs(
        &mut self,
        subscription_id: SubscriptionId,
        outputs: &BTreeMap<String, CompiledNode>,
    ) {
        for output in outputs.values() {
            self.subscriptions_by_output_node
                .entry(output.node)
                .or_default()
                .insert(subscription_id);
        }
    }

    fn unindex_subscription_outputs(
        &mut self,
        subscription_id: SubscriptionId,
        outputs: &BTreeMap<String, CompiledNode>,
    ) {
        for output in outputs.values() {
            let remove_node = self
                .subscriptions_by_output_node
                .get_mut(&output.node)
                .is_some_and(|subscriptions| {
                    subscriptions.remove(&subscription_id);
                    subscriptions.is_empty()
                });
            if remove_node {
                self.subscriptions_by_output_node.remove(&output.node);
            }
        }
    }

    pub fn unsubscribe(&mut self, subscription_id: SubscriptionId) -> bool {
        if let Some(subscription) = self.multisink_subscriptions.remove(&subscription_id) {
            self.unindex_subscription_outputs(subscription_id, &subscription.outputs);
            let removed = self.remove_multisink_retainers(subscription_id, &subscription.outputs);
            self.cancel_pending_subscription_hydration(subscription_id);
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

    pub async fn unsubscribe_with_storage<S>(
        &mut self,
        subscription_id: SubscriptionId,
        storage: &S,
    ) -> Result<bool, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        if let Some(subscription) = self.multisink_subscriptions.remove(&subscription_id) {
            self.unindex_subscription_outputs(subscription_id, &subscription.outputs);
            let removed = self.remove_multisink_retainers(subscription_id, &subscription.outputs);
            self.cancel_pending_subscription_hydration(subscription_id);
            if let MultisinkSubscriptionTarget::RoutedShape {
                shape_id,
                binding_key,
            } = subscription.target
                && let Some(param_delta) = self.remove_binding_ref(shape_id, &binding_key)
                && !param_delta.deltas.is_empty()
            {
                self.tick_with_params(
                    Vec::new(),
                    vec![param_delta],
                    OwnedStorage::new(Rc::new(storage)),
                    None,
                )
                .await?;
                self.remove_unreferenced_auto_family(shape_id);
            }
            return Ok(removed);
        }

        Ok(false)
    }

    /// Remove a caller-owned prepared shape after all of its bindings have
    /// been unsubscribed. Binding-source entries are intentionally retained:
    /// multiple prepared shapes may share the same source descriptor, while
    /// their graph retainers are owned by this exact shape id.
    pub fn retire_prepared_shape(
        &mut self,
        shape_id: PreparedShapeId,
    ) -> Result<(), IvmRuntimeError> {
        if self.multisink_subscriptions.values().any(|subscription| {
            matches!(
                subscription.target,
                MultisinkSubscriptionTarget::RoutedShape { shape_id: active, .. } if active == shape_id
            )
        }) {
            return Err(IvmRuntimeError::PreparedShapeHasActiveBindings(shape_id));
        }
        let shape = self
            .prepared_shapes
            .remove(&shape_id)
            .ok_or(IvmRuntimeError::PreparedShapeNotFound(shape_id))?;
        for output_node in shape
            .terminals
            .values()
            .map(|terminal| terminal.output.node)
        {
            self.remove_retainer(
                output_node,
                &Retainer::PreparedShape(shape_id.retainer_key()),
            );
        }
        for node in self.gc_ephemeral_nodes(0) {
            self.remove_node_runtime(node);
        }
        Ok(())
    }

    pub(crate) async fn prune_dropped_subscriptions_with_storage<S>(
        &mut self,
        storage: &S,
    ) -> Result<usize, IvmRuntimeError>
    where
        S: OrderedKvStorage,
    {
        let dropped = self
            .multisink_subscriptions
            .iter()
            .filter_map(|(id, state)| state.receiver_liveness.upgrade().is_none().then_some(*id))
            .collect::<Vec<_>>();
        for id in &dropped {
            self.unsubscribe_with_storage(*id, storage).await?;
        }
        Ok(dropped.len())
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
        let graph = replace_binding_shape(&lifted.graph, &shape_seed);
        let shape_output = self.infer_builder_output(&graph)?;
        validate_public_output_fields(&shape_output, &original_output)?;
        let public_fields = descriptor_field_names(&original_output)?;
        let shape = format!("auto_direct_{:016x}", graph_builder_fingerprint(&graph));
        let graph = replace_binding_shape(&graph, &shape);
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

    pub(super) fn infer_builder_output_cached(
        &self,
        graph: &GraphBuilder,
        output_memo: &mut HashMap<usize, RecordDescriptor>,
    ) -> Result<RecordDescriptor, IvmRuntimeError> {
        let memo_key = graph as *const GraphBuilder as usize;
        if let Some(output) = output_memo.get(&memo_key) {
            return Ok(*output);
        }
        // A legal policy graph can be deeply nested (for example a recursive
        // inheritance policy after its public result and routing projections
        // have been attached). Infer every child before its parent explicitly
        // so descriptor inference does not consume the server owner's stack.
        for builder in graph.postorder() {
            let key = builder as *const GraphBuilder as usize;
            if output_memo.contains_key(&key) {
                continue;
            }
            let output = self.infer_builder_output_uncached(builder, output_memo)?;
            output_memo.insert(key, output);
        }
        output_memo
            .get(&memo_key)
            .copied()
            .ok_or(IvmRuntimeError::UnsupportedOperator)
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
            GraphBuilder::Index {
                table,
                row_projection: Some(target),
                ..
            } => self
                .variant_projections
                .get(&VariantProjectionKey {
                    table: table.clone(),
                    target: VariantProjectionTarget::Named(target.clone()),
                })
                .map(|projection| projection.output)
                .ok_or_else(|| IvmRuntimeError::VariantProjectionNotFound {
                    table: table.clone(),
                    target: target.clone(),
                }),
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
            GraphBuilder::StreamingChecksum {
                input,
                field,
                output_field,
                window_bytes,
                max_bytes_per_turn,
            } => {
                if *window_bytes == 0 || *max_bytes_per_turn == 0 {
                    return Err(IvmRuntimeError::InvalidStreamingChecksumBudget);
                }
                let input = self.infer_builder_output_cached(input, output_memo)?;
                let field_idx = resolve_field_ref(&input, field)?;
                match input.fields()[field_idx].value_type {
                    ValueType::String | ValueType::Bytes => {}
                    _ => return Err(IvmRuntimeError::StreamingChecksumTypeMismatch),
                }
                Ok(RecordDescriptor::new(
                    input
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(index, descriptor_field)| {
                            let name = if index == field_idx {
                                output_field.clone()
                            } else {
                                descriptor_field.name.clone().unwrap_or_default()
                            };
                            let value_type = if index == field_idx {
                                ValueType::Bytes
                            } else {
                                descriptor_field.value_type.clone()
                            };
                            (name, value_type)
                        }),
                ))
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

    pub(super) fn next_subscription_id(&mut self) -> SubscriptionId {
        let id = SubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;
        id
    }

    pub(super) fn next_shape_id(&mut self) -> PreparedShapeId {
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

    /// Reacquiring the last reference before its queued retraction is
    /// evaluated leaves the evaluator's binding resident. Cancel that
    /// lifecycle transition instead of emitting a redundant remove/add pair.
    fn cancel_pending_binding_retraction(&mut self, shape: &str, binding: &BindingKey) -> bool {
        let mut cancelled = false;
        for pending in &mut self.pending_binding_retractions {
            if pending.shape != shape {
                continue;
            }
            pending.deltas.retain(|delta| {
                let matches = delta.weight < 0 && delta.record.as_ref() == binding.0.as_slice();
                cancelled |= matches;
                !matches
            });
        }
        self.pending_binding_retractions
            .retain(|pending| !pending.deltas.is_empty());
        cancelled
    }

    fn provisional_binding_delta(
        &self,
        shape_id: PreparedShapeId,
        binding: &BindingKey,
    ) -> Result<BindingDelta, IvmRuntimeError> {
        let shape = self.binding_source_shape_name(shape_id)?;
        let source = self
            .binding_sources
            .get(&shape)
            .ok_or_else(|| IvmRuntimeError::BindingSourceNotFound(shape.clone()))?;
        Ok(BindingDelta {
            shape,
            descriptor: source.descriptor,
            deltas: if source.refcounts.contains_key(binding) {
                Vec::new()
            } else {
                vec![RecordDelta {
                    record: binding.0.clone().into(),
                    weight: 1,
                }]
            },
        })
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

    pub(super) fn binding_snapshot_deltas(&self) -> HashMap<String, RecordDeltas> {
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
    }
}

#[cfg(test)]
mod bounded_graph_traversal_tests {
    use super::*;

    #[test]
    fn deep_binding_shape_rewrite_and_family_keying_fit_a_two_mib_stack() {
        std::thread::Builder::new()
            .name("deep-binding-shape-rewrite".to_owned())
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                const DEPTH: usize = 4_096;
                let descriptor = RecordDescriptor::new([("id", ValueType::U64)]);
                let mut graph = GraphBuilder::binding_source("old_shape", descriptor);
                for _ in 0..DEPTH {
                    graph = graph.project(["id"]);
                }

                assert_eq!(count_builder_nodes(&graph), DEPTH + 1);
                assert!(builder_contains_binding_source(&graph));
                let rewritten = replace_binding_shape(&graph, "new_shape");
                assert_eq!(rewritten.postorder().len(), DEPTH + 1);
                assert!(graph_builders_equal(&rewritten, &rewritten.clone()));
                assert_eq!(
                    graph_builder_fingerprint(&rewritten),
                    graph_builder_fingerprint(&rewritten.clone())
                );

                let mut leaf = &rewritten;
                for _ in 0..DEPTH {
                    let GraphBuilder::Project { input, .. } = leaf else {
                        panic!("rewrite must preserve the unary project chain");
                    };
                    leaf = input;
                }
                assert!(matches!(leaf, GraphBuilder::BindingSource { shape, .. } if shape == "new_shape"));

                // Arc child destruction is intentionally independent of this
                // traversal receipt; do not make its stack behavior mask a
                // regression in bounded rewrite/keying.
                std::mem::forget(graph);
                std::mem::forget(rewritten);
            })
            .expect("spawn normal-stack traversal test")
            .join()
            .expect("normal-stack traversal test must not overflow");
    }
}
