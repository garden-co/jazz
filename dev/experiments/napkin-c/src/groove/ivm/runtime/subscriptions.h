/* groove::ivm::runtime::subscriptions — types owned by this module. */
#ifndef GROOVE_IVM_RUNTIME_SUBSCRIPTIONS_H
#define GROOVE_IVM_RUNTIME_SUBSCRIPTIONS_H

#include "jz_rt.h"
#include "groove/_ext_groove-ivm-rt-a.h"

/* RecordDelta { record: Bytes, weight: i64 } */
typedef struct groove_RecordDelta { jz_bytes record; i64 weight; } groove_RecordDelta;
typedef JZ_VEC(groove_RecordDelta) groove_RecordDeltaVec;
/* RecordDeltas { descriptor, deltas }; Arc<RecordDeltas> = jz_rc payload */
typedef struct groove_RecordDeltas { groove_RecordDescriptor descriptor; groove_RecordDeltaVec deltas; } groove_RecordDeltas;

static inline jz_slice groove_RecordDelta_raw(const groove_RecordDelta *d) { return jz_bytes_slice(&d->record); }
static inline groove_BorrowedRecord groove_RecordDelta_borrowed(const groove_RecordDelta *d, groove_RecordDescriptor desc) {
    groove_BorrowedRecord r = { d->record.ptr, d->record.len, desc }; return r;
}
static inline groove_RecordDeltas groove_RecordDeltas_empty(groove_RecordDescriptor d) { groove_RecordDeltas r = { d, {0} }; return r; }

void groove__ivm__runtime__subscriptions__RecordDelta__clone(void *dst, const void *src);
void groove__ivm__runtime__subscriptions__RecordDelta__drop(void *d);
bool groove__ivm__runtime__subscriptions__RecordDelta__eq(const groove_RecordDelta *a, const groove_RecordDelta *b);
void groove__ivm__runtime__subscriptions__RecordDeltas__clone(groove_RecordDeltas *dst, const groove_RecordDeltas *src);
void groove__ivm__runtime__subscriptions__RecordDeltas__drop(groove_RecordDeltas *d);
bool groove__ivm__runtime__subscriptions__RecordDeltas__eq(const groove_RecordDeltas *a, const groove_RecordDeltas *b);
bool groove__ivm__runtime__subscriptions__RecordDeltas__is_empty(const groove_RecordDeltas *d);
jz_err groove__ivm__runtime__subscriptions__RecordDeltas__to_values(const groove_RecordDeltas *d, void *out /* Vec<(Vec<Value>, i64)> */);
/* Arc<RecordDeltas> helpers */
groove_RecordDeltas *groove__ivm__runtime__subscriptions__RecordDeltas__arc_new(groove_RecordDeltas value);
void groove__ivm__runtime__subscriptions__RecordDeltas__arc_drop(void *rc);
usize groove__ivm__runtime__subscriptions__record_deltas_encoded_bytes(const groove_RecordDeltas *d);

/* TableDelta */
typedef struct groove_TableDelta { jz_str table; u32 variant_tag; groove_RecordDescriptor descriptor; groove_RecordDeltaVec deltas; } groove_TableDelta;
typedef JZ_VEC(groove_TableDelta) groove_TableDeltaVec;
void groove__ivm__runtime__subscriptions__TableDelta__clone(void *dst, const void *src);
void groove__ivm__runtime__subscriptions__TableDelta__drop(void *d);

/* BindingKey(Vec<u8>) */
typedef struct groove_BindingKey { jz_bytes bytes; } groove_BindingKey;
/* BindingDelta */
typedef struct groove_BindingDelta { groove_BindingSourceKey key; groove_RecordDescriptor descriptor; groove_RecordDeltaVec deltas; bool initializes_snapshot; } groove_BindingDelta;
typedef JZ_VEC(groove_BindingDelta) groove_BindingDeltaVec;
void groove__ivm__runtime__subscriptions__BindingDelta__clone(void *dst, const void *src);
void groove__ivm__runtime__subscriptions__BindingDelta__drop(void *d);

/* BindingSnapshots = HashMap<BindingSourceKey, Arc<RecordDeltas>> (rc payload jz_hmap) */
typedef jz_hmap groove_BindingSnapshots;
typedef struct groove_BindingSourceState { groove_RecordDescriptor descriptor; jz_hmap refcounts; /* BindingKey -> usize */ bool initialized; } groove_BindingSourceState;
typedef struct groove_BindingSources { jz_hmap states; groove_BindingSnapshots *snapshot; jz_hmap dirty; } groove_BindingSources;

/* CompiledNode */
typedef struct groove_CompiledNode { groove_RecordDescriptor output; groove_NodeId node; bool has_root_ordering_node; groove_NodeId root_ordering_node; } groove_CompiledNode;

/* ids */
typedef struct groove_SubscriptionId { u64 runtime_namespace; u64 sequence; } groove_SubscriptionId;
typedef struct groove_PublicationId { u64 v; } groove_PublicationId;
typedef struct groove_PreparedShapeId { u64 v; } groove_PreparedShapeId;

/* key_encoding::PredicateRecord as a vtable (generic over BorrowedRecord / RoutedRecord in Rust) */
enum { GROOVE_FLO_COMPARED, GROOVE_FLO_SQL_NULL, GROOVE_FLO_UNSUPPORTED };
typedef struct groove_FieldLiteralOrdering { u32 tag; int ordering; } groove_FieldLiteralOrdering;
typedef struct groove_PredicateRecordVt {
    jz_err (*value)(const void *self, const jz_str *field, groove_Value *out);
    jz_err (*literal_ordering)(const void *self, const jz_str *field, const groove_LiteralValue *value, groove_FieldLiteralOrdering *out);
} groove_PredicateRecordVt;
typedef struct groove_PredicateRecord { const groove_PredicateRecordVt *vt; const void *self; } groove_PredicateRecord;
extern const groove_PredicateRecordVt groove__ivm__runtime__key_encoding__BorrowedRecord__PredicateRecord__VT;
typedef bool (*groove_OrdPred)(int ordering);
jz_err groove__ivm__runtime__key_encoding__compare_record_field(groove_PredicateRecord r, const jz_str *field, const groove_LiteralValue *value, groove_OrdPred pred, u32 comparison, bool *out);
jz_err groove__ivm__runtime__key_encoding__compare_record_fields(groove_PredicateRecord r, const jz_str *field, const jz_str *value_field, groove_OrdPred pred, u32 comparison, bool *out);
jz_err groove__ivm__runtime__key_encoding__contains_record_field(groove_PredicateRecord r, const jz_str *field, const groove_LiteralValue *value, u32 comparison, bool *out);
jz_err groove__ivm__runtime__key_encoding__contains_record_field_value(groove_PredicateRecord r, const jz_str *field, const jz_str *needle_field, u32 comparison, bool *out);
bool groove__ivm__runtime__key_encoding__is_sql_null_value(const groove_Value *v);

/* PredicateExpr impl (defined in subscriptions.rs) */
bool groove__ivm__runtime__subscriptions__PredicateExpr__supports_indirect_literal_attempt(const groove_PredicateExpr *p);
jz_err groove__ivm__runtime__subscriptions__PredicateExpr__matches_indirect_literal_attempt(const groove_PredicateExpr *p, groove_BorrowedRecord record, groove_EvaluationInputs *inputs, i8 *out /* -1 None */);
void groove__ivm__runtime__subscriptions__PredicateExpr__referenced_fields(const groove_PredicateExpr *p, jz_btree *out /* BTreeSet<String> */);
jz_err groove__ivm__runtime__subscriptions__PredicateExpr__matches(const groove_PredicateExpr *p, groove_PredicateRecord record, u32 comparison, bool *out);
jz_err groove__ivm__runtime__subscriptions__PredicateExpr__matches_borrowed(const groove_PredicateExpr *p, groove_BorrowedRecord record, u32 comparison, bool *out);

/* shared btree/hmap descriptors of this unit (defined in subscriptions.c) */
extern const jz_btree_desc groove_ivm_rt_a_desc_str_set;     /* BTreeSet<String> */
extern const jz_btree_desc groove_ivm_rt_a_desc_bytes_set;   /* BTreeSet<Vec<u8>> */
extern const jz_hmap_desc groove_ivm_rt_a_hdesc_nodeid_set;  /* HashSet<NodeId> */
extern const jz_hmap_desc groove_ivm_rt_a_hdesc_bytes_set;   /* HashSet<Vec<u8>> */
u64 groove_ivm_rt_a_hash_nodeid(const void *k);
bool groove_ivm_rt_a_eq_nodeid(const void *a, const void *b);
u64 groove_ivm_rt_a_hash_bytes(const void *k);
bool groove_ivm_rt_a_eq_bytes(const void *a, const void *b);
void groove_ivm_rt_a_drop_bytes(void *p);
void groove_ivm_rt_a_drop_str(void *p);
void groove_ivm_rt_a_clone_bytes(void *dst, const void *src);
void groove_ivm_rt_a_clone_str(void *dst, const void *src);
static inline bool groove_NodeId_eq(groove_NodeId a, groove_NodeId b) { return a.v == b.v; }

/* IvmRuntimeError constructors (Display text of mod.rs) */
static inline jz_err groove_err_node_not_found(groove_NodeId n) { return jz_err_fmt(GROOVE_IVM_ERR_GRAPH_NODE_NOT_FOUND, "graph node not found: NodeId(%llu)", (unsigned long long)n.v); }
static inline jz_err groove_err_arity(groove_NodeId n) { return jz_err_fmt(GROOVE_IVM_ERR_GRAPH_INPUT_ARITY_MISMATCH, "graph node has unexpected input arity: NodeId(%llu)", (unsigned long long)n.v); }
static inline jz_err groove_err_input_missing(groove_NodeId n) { return jz_err_fmt(GROOVE_IVM_ERR_GRAPH_INPUT_MISSING, "graph node is missing input: NodeId(%llu)", (unsigned long long)n.v); }
static inline jz_err groove_err_state_mismatch(groove_NodeId n) { return jz_err_fmt(GROOVE_IVM_ERR_NODE_STATE_OPERATOR_MISMATCH, "runtime node state operator mismatch: NodeId(%llu)", (unsigned long long)n.v); }
static inline jz_err groove_err_field_not_found(const char *p, u32 n) { return jz_err_fmt(GROOVE_IVM_ERR_GRAPH_FIELD_NOT_FOUND, "graph field not found: %.*s", (int)n, p); }
static inline jz_err groove_err_field_oob(usize i) { return jz_err_fmt(GROOVE_IVM_ERR_GRAPH_FIELD_INDEX_OUT_OF_BOUNDS, "graph field index out of bounds: %zu", i); }
static inline jz_err groove_err_unsupported_operator(void) { return jz_err_new(GROOVE_IVM_ERR_UNSUPPORTED_OPERATOR, "unsupported operator"); }
static inline jz_err groove_err_output_mismatch(void) { return jz_err_new(GROOVE_IVM_ERR_GRAPH_OUTPUT_MISMATCH, "graph output descriptors do not match"); }
static inline jz_err groove_err_invalid_collect_by(const char *msg) { return jz_err_fmt(GROOVE_IVM_ERR_INVALID_COLLECT_BY, "invalid collect_by descriptor: %s", msg); }
static inline jz_err groove_err_evaluation_blocked(void) { return jz_err_new(GROOVE_IVM_ERR_EVALUATION_BLOCKED, "evaluation blocked on a non-resident storage input"); }
static inline jz_err groove_err_table_not_found(const jz_str *t) { return jz_err_fmt(GROOVE_IVM_ERR_TABLE_NOT_FOUND, "table not found: %.*s", (int)t->len, t->ptr); }
static inline jz_err groove_err_unsupported_lookup(const char *what) { return jz_err_fmt(GROOVE_IVM_ERR_UNSUPPORTED_LOOKUP, "unsupported keyed lookup: %s", what); }
jz_err groove_ivm_rt_a_err_out_of_order(groove_SubTick current, groove_SubTick next, bool is_tick);
void groove_ivm_rt_a_subtick_debug(jz_str *out, groove_SubTick t, bool is_tick);


/* ======================================================================
 * subscriptions.rs: lifetimes, delivery, prepared shapes
 * ==================================================================== */
enum { GROOVE_SL_FIRST_RESULT, GROOVE_SL_RETAINED };
typedef u32 groove_SubscriptionLifetime;
groove_Retainer groove__ivm__runtime__subscriptions__SubscriptionLifetime__retainer(groove_SubscriptionLifetime self, groove_SubscriptionId id);

enum { GROOVE_RIV_MATERIALIZE, GROOVE_RIV_PHYSICAL, GROOVE_RIV_PHYSICAL_FIELDS };
typedef struct groove_RootIndirectValues { u32 tag; jz_btree *physical; /* Arc<BTreeSet<String>> rc payload */ } groove_RootIndirectValues;
void groove__ivm__runtime__subscriptions__RootIndirectValues__clone(groove_RootIndirectValues *dst, const groove_RootIndirectValues *src);
void groove__ivm__runtime__subscriptions__RootIndirectValues__drop(groove_RootIndirectValues *r);
bool groove__ivm__runtime__subscriptions__RootIndirectValues__eq(const groove_RootIndirectValues *a, const groove_RootIndirectValues *b);
bool groove__ivm__runtime__subscriptions__RootIndirectValues__materialized_field_indices(const groove_RootIndirectValues *self, groove_RecordDescriptor d, groove_UsizeVec *out);

void groove__ivm__runtime__subscriptions__SubscriptionId__retainer_key(groove_SubscriptionId id, jz_str *out);
void groove__ivm__runtime__subscriptions__PreparedShapeId__retainer_key(groove_PreparedShapeId id, jz_str *out);
typedef struct groove_PreparedShape { groove_PreparedShapeId id; } groove_PreparedShape;

/* SubscriptionError { Evaluation(Arc<IvmRuntimeError>), Ended } */
enum { GROOVE_SE_EVALUATION, GROOVE_SE_ENDED };
typedef struct groove_SubscriptionError { u32 tag; jz_err *error; /* rc payload */ } groove_SubscriptionError;
groove_SubscriptionError groove__ivm__runtime__subscriptions__SubscriptionError__new(jz_err *rc_error);
const jz_err *groove__ivm__runtime__subscriptions__SubscriptionError__source_error(const groove_SubscriptionError *e);
void groove__ivm__runtime__subscriptions__SubscriptionError__fmt(const groove_SubscriptionError *e, jz_str *out);
void groove__ivm__runtime__subscriptions__SubscriptionError__clone(groove_SubscriptionError *dst, const groove_SubscriptionError *src);
void groove__ivm__runtime__subscriptions__SubscriptionError__drop(groove_SubscriptionError *e);

/* RoutedMultisinkTerminal */
typedef struct groove_RoutedMultisinkTerminal {
    jz_str sink; groove_GraphBuilder graph;
    groove_StrVec route_fields; groove_UsizeVec route_value_indices; groove_StrVec public_fields;
} groove_RoutedMultisinkTerminal;
typedef JZ_VEC(groove_RoutedMultisinkTerminal) groove_RoutedMultisinkTerminalVec;
void groove__ivm__runtime__subscriptions__RoutedMultisinkTerminal__new(jz_str sink, groove_GraphBuilder graph, groove_StrVec route_fields, groove_StrVec public_fields, groove_RoutedMultisinkTerminal *out);
void groove__ivm__runtime__subscriptions__RoutedMultisinkTerminal__with_route_value_indices(groove_RoutedMultisinkTerminal *self, groove_UsizeVec indices);
void groove__ivm__runtime__subscriptions__RoutedMultisinkTerminal__clone(void *dst, const void *src);
void groove__ivm__runtime__subscriptions__RoutedMultisinkTerminal__drop(void *t);
bool groove__ivm__runtime__subscriptions__RoutedMultisinkTerminal__eq(const groove_RoutedMultisinkTerminal *a, const groove_RoutedMultisinkTerminal *b);
u64 groove__ivm__runtime__subscriptions__RoutedMultisinkTerminal__hash(const groove_RoutedMultisinkTerminal *t, u64 h);

/* MultisinkDeltas { sinks: BTreeMap<String, RecordDeltas>, terminal_sinks: BTreeMap<String, TerminalDeltas> } */
typedef struct groove_MultisinkDeltas { jz_btree sinks; jz_btree terminal_sinks; } groove_MultisinkDeltas;
extern const jz_btree_desc groove_ivm_rt_a_desc_sink_record_deltas;
extern const jz_btree_desc groove_ivm_rt_a_desc_sink_terminal_deltas;
void groove__ivm__runtime__subscriptions__MultisinkDeltas__default(groove_MultisinkDeltas *d);
void groove__ivm__runtime__subscriptions__MultisinkDeltas__clone(groove_MultisinkDeltas *dst, const groove_MultisinkDeltas *src);
void groove__ivm__runtime__subscriptions__MultisinkDeltas__drop(groove_MultisinkDeltas *d);
bool groove__ivm__runtime__subscriptions__MultisinkDeltas__is_empty(const groove_MultisinkDeltas *d);
const groove_RecordDeltas *groove__ivm__runtime__subscriptions__MultisinkDeltas__get(const groove_MultisinkDeltas *d, const char *sink, u32 len);
usize groove__ivm__runtime__subscriptions__multisink_deltas_record_count(const groove_MultisinkDeltas *d);
usize groove__ivm__runtime__subscriptions__multisink_deltas_encoded_bytes(const groove_MultisinkDeltas *d);

typedef struct groove_QueuedMultisinkDeltas { groove_MultisinkDeltas deltas; bool has_publication; groove_PublicationId publication; } groove_QueuedMultisinkDeltas;
groove_QueuedMultisinkDeltas groove__ivm__runtime__subscriptions__QueuedMultisinkDeltas__new(groove_MultisinkDeltas deltas);
void groove__ivm__runtime__subscriptions__QueuedMultisinkDeltas__drop(groove_QueuedMultisinkDeltas *q);
void groove__ivm__runtime__subscriptions__QueuedMultisinkDeltas__clone(groove_QueuedMultisinkDeltas *dst, const groove_QueuedMultisinkDeltas *src);

/* channel item: Result<QueuedMultisinkDeltas, SubscriptionError> */
typedef struct groove_SubscriptionItem { bool ok; groove_QueuedMultisinkDeltas queued; groove_SubscriptionError error; } groove_SubscriptionItem;
/* Arc<Mutex<Option<Sender>>> / Arc<Mutex<Option<Waker>>> / Arc<Mutex<Option<MultisinkDeltas>>> / Arc<()> as rc cells */
typedef struct groove_SenderCell { bool some; jz_chan *chan; } groove_SenderCell;
typedef struct groove_WaiterCell { bool some; jz_waker waker; } groove_WaiterCell;
typedef struct groove_InitialCell { bool some; groove_MultisinkDeltas value; } groove_InitialCell;
typedef struct groove_LivenessCell { bool alive; } groove_LivenessCell; /* Weak::upgrade() == alive */
groove_InitialCell *groove_ivm_rt_a_initial_cell_new(void);

typedef struct groove_SubscriptionSender { groove_SenderCell *sender; groove_WaiterCell *waiter; } groove_SubscriptionSender;
bool groove__ivm__runtime__subscriptions__SubscriptionSender__send(const groove_SubscriptionSender *self, groove_QueuedMultisinkDeltas queued, groove_QueuedMultisinkDeltas *returned /* SendError */);
void groove__ivm__runtime__subscriptions__SubscriptionSender__fail(const groove_SubscriptionSender *self, groove_SubscriptionError error);
void groove__ivm__runtime__subscriptions__SubscriptionSender__clone(groove_SubscriptionSender *dst, const groove_SubscriptionSender *src);
void groove__ivm__runtime__subscriptions__SubscriptionSender__drop(groove_SubscriptionSender *s);

typedef struct groove_MultisinkSubscription {
    groove_SubscriptionId id; groove_InitialCell *initial; jz_chan *receiver; groove_WaiterCell *waiter; groove_LivenessCell *receiver_liveness;
} groove_MultisinkSubscription;
typedef struct groove_PublicationUpdate { bool has_publication; groove_PublicationId publication; groove_MultisinkDeltas deltas; } groove_PublicationUpdate;
enum { GROOVE_SEV_UPDATE, GROOVE_SEV_ERROR };
typedef struct groove_SubscriptionEvent { u32 tag; groove_PublicationUpdate update; groove_SubscriptionError error; } groove_SubscriptionEvent;
enum { GROOVE_TRY_RECV_OK, GROOVE_TRY_RECV_EMPTY, GROOVE_TRY_RECV_DISCONNECTED };
void groove__ivm__runtime__subscriptions__MultisinkSubscription__drop(groove_MultisinkSubscription *s);
bool groove__ivm__runtime__subscriptions__MultisinkSubscription__take_initial(const groove_MultisinkSubscription *s, groove_MultisinkDeltas *out);
u32 groove__ivm__runtime__subscriptions__MultisinkSubscription__try_recv(const groove_MultisinkSubscription *s, groove_MultisinkDeltas *out);
u32 groove__ivm__runtime__subscriptions__MultisinkSubscription__try_recv_with_publication(const groove_MultisinkSubscription *s, groove_PublicationUpdate *out);
u32 groove__ivm__runtime__subscriptions__MultisinkSubscription__poll_next_event(const groove_MultisinkSubscription *s, const jz_waker *cx, groove_SubscriptionEvent *out); /* 1 ready, GROOVE_POLL_PENDING */
u32 groove__ivm__runtime__subscriptions__MultisinkSubscription__poll_next_with_publication(const groove_MultisinkSubscription *s, const jz_waker *cx, bool *ok, groove_PublicationUpdate *out);

/* MultisinkSubscriptionTarget */
enum { GROOVE_MST_DIRECT, GROOVE_MST_ROUTED_SHAPE };
typedef struct groove_MultisinkSubscriptionTarget { u32 tag; groove_PreparedShapeId shape_id; groove_BindingKey binding_key; groove_NodeIdVec route_barriers; } groove_MultisinkSubscriptionTarget;
extern const jz_btree_desc groove_ivm_rt_a_desc_sink_compiled_node; /* BTreeMap<String, CompiledNode> */
typedef struct groove_MultisinkSubscriptionState {
    groove_SubscriptionSender sender;
    groove_LivenessCell *receiver_liveness;  /* Weak<()> */
    jz_btree outputs;                        /* String -> CompiledNode */
    groove_MultisinkSubscriptionTarget target;
    bool failed;
    groove_RootIndirectValues root_indirect_values;
} groove_MultisinkSubscriptionState;
void groove__ivm__runtime__subscriptions__MultisinkSubscriptionState__drop(void *s);
void groove__ivm__runtime__subscriptions__MultisinkSubscriptionState__clone(void *dst, const void *src);
static inline bool groove_LivenessCell_upgrade(const groove_LivenessCell *w) { return w && w->alive; }

typedef struct groove_AutoDirectFamilyKey { groove_GraphBuilder graph; groove_RecordDescriptor binding_descriptor; jz_str binding_field; groove_StrVec public_fields; } groove_AutoDirectFamilyKey;
bool groove__ivm__runtime__subscriptions__AutoDirectFamilyKey__eq(const void *a, const void *b);
u64 groove__ivm__runtime__subscriptions__AutoDirectFamilyKey__hash(const void *k);
void groove__ivm__runtime__subscriptions__AutoDirectFamilyKey__clone(void *dst, const void *src);
void groove__ivm__runtime__subscriptions__AutoDirectFamilyKey__drop(void *k);

/* SharedShapeKey = (String, Vec<RoutedMultisinkTerminal>) */
typedef struct groove_SharedShapeKey { jz_str shape; groove_RoutedMultisinkTerminalVec terminals; } groove_SharedShapeKey;
bool groove__ivm__runtime__subscriptions__SharedShapeKey__eq(const void *a, const void *b);
u64 groove__ivm__runtime__subscriptions__SharedShapeKey__hash(const void *k);
void groove__ivm__runtime__subscriptions__SharedShapeKey__clone(void *dst, const void *src);
void groove__ivm__runtime__subscriptions__SharedShapeKey__drop(void *k);

typedef struct groove_RoutedMultisinkTerminalState { groove_RoutedMultisinkTerminal terminal; groove_CompiledNode output; } groove_RoutedMultisinkTerminalState;
extern const jz_btree_desc groove_ivm_rt_a_desc_sink_terminal_state;
typedef struct groove_RoutedMultisinkShapeState {
    jz_str shape; groove_RecordDescriptor binding_descriptor;
    jz_btree terminals; /* String -> RoutedMultisinkTerminalState */
    bool has_auto_family_key; groove_AutoDirectFamilyKey auto_family_key;
    bool has_shared_key; groove_SharedShapeKey shared_key;
    bool retire_when_unbound;
} groove_RoutedMultisinkShapeState;
void groove__ivm__runtime__subscriptions__RoutedMultisinkShapeState__clone(void *dst, const void *src);
void groove__ivm__runtime__subscriptions__RoutedMultisinkShapeState__drop(void *s);

typedef struct groove_LiveAttach { groove_BindingSourceKey binding_source; groove_BindingKey binding_key; jz_hmap borrowed; /* HashSet<NodeId> */ } groove_LiveAttach;
void groove__ivm__runtime__subscriptions__LiveAttach__drop(groove_LiveAttach *l);

typedef struct groove_InputSourceReplacement { groove_InputSourceId id; groove_RecordDescriptor descriptor; JZ_VEC(jz_bytes) records; } groove_InputSourceReplacement;
typedef struct groove_InputSourceDelta { groove_InputSourceId id; groove_RecordDescriptor descriptor; JZ_VEC(jz_bytes) adds, removes; } groove_InputSourceDelta;

/* BindingSources API */
const groove_BindingSourceState *groove__ivm__runtime__subscriptions__BindingSources__get(const groove_BindingSources *b, const groove_BindingSourceKey *key);
bool groove__ivm__runtime__subscriptions__BindingSources__contains_key(const groove_BindingSources *b, const groove_BindingSourceKey *key);
groove_BindingSourceState *groove__ivm__runtime__subscriptions__BindingSources__get_mut(groove_BindingSources *b, const groove_BindingSourceKey *key);
groove_BindingSourceState *groove__ivm__runtime__subscriptions__BindingSources__entry(groove_BindingSources *b, groove_BindingSourceKey key /* moved */, bool *inserted);
bool groove__ivm__runtime__subscriptions__BindingSources__remove(groove_BindingSources *b, const groove_BindingSourceKey *key, groove_BindingSourceState *out);
groove_BindingSnapshots *groove__ivm__runtime__subscriptions__BindingSources__snapshot(groove_BindingSources *b);
void groove__ivm__runtime__subscriptions__BindingSourceState__drop(void *s);
extern const jz_hmap_desc groove_ivm_rt_a_hdesc_binding_snapshots; /* BindingSourceKey -> Arc<RecordDeltas> */
extern const jz_hmap_desc groove_ivm_rt_a_hdesc_binding_refcounts; /* BindingKey -> usize */

/* free functions */
u64 groove__ivm__runtime__subscriptions__graph_builder_fingerprint(const groove_GraphBuilder *g);
typedef struct groove_GraphBuilderEqHook { i8 (*call)(void *ctx, const groove_GraphBuilder *a, const groove_GraphBuilder *b); void *ctx; } groove_GraphBuilderEqHook; /* -1 None, 0 false, 1 true */
bool groove__ivm__runtime__subscriptions__graph_builders_equal_with(const groove_GraphBuilder *a, const groove_GraphBuilder *b, groove_GraphBuilderEqHook hook);
bool groove__ivm__runtime__subscriptions__graph_builders_equal(const groove_GraphBuilder *a, const groove_GraphBuilder *b);
usize groove__ivm__runtime__subscriptions__count_builder_nodes(const groove_GraphBuilder *g);
bool groove__ivm__runtime__subscriptions__builder_contains_binding_source(const groove_GraphBuilder *g);
void groove__ivm__runtime__subscriptions__strip_bound_route_fields_from_slot(groove_CollectBySlotBuilder *slot, const groove_StrVec *route_fields);

/* ======================================================================
 * IvmRuntime (mod.rs) mirror, as this unit sees it
 * ==================================================================== */
struct groove_PendingIncrementalState;
struct groove_IvmRuntime {
    groove_DatabaseSchema schema;
    void *chunk_provider;              /* OwnedChunkProvider */
    jz_hmap table_storage_descriptors; /* String -> RecordDescriptor */
    jz_hmap table_descriptors;         /* String -> RecordDescriptor */
    jz_hmap variant_descriptors;       /* String -> HashMap<u32, RecordDescriptor> */
    jz_hmap variant_projections;       /* VariantProjectionKey -> VariantProjection */
    groove_IvmGraph *graph;
    void *projection_plans;            /* RefCell<ProjectionPlans> */
    void *compilation_cache;
    jz_hmap multisink_subscriptions;   /* SubscriptionId -> MultisinkSubscriptionState */
    jz_hmap subscriptions_by_output_node; /* NodeId -> HashSet<SubscriptionId> */
    struct groove_PendingIncrementalState *pending_incremental; /* PendingIncrementalEvaluation(Rc<RefCell<..>>) */
    bool pending_incremental_polling;
    bool has_owner_progress_waker; jz_waker owner_progress_waker;
    bool has_owner_fanout_waker; jz_waker owner_fanout_caller; jz_waker owner_fanout_waker;
    bool ephemeral_graph_gc_pending;
    jz_hmap gc_candidates;             /* HashSet<NodeId> */
    jz_hmap prepared_shapes;           /* PreparedShapeId -> RoutedMultisinkShapeState */
    jz_hmap auto_direct_families;      /* AutoDirectFamilyKey -> PreparedShapeId */
    jz_hmap shared_prepared_shapes;    /* SharedShapeKey -> PreparedShapeId */
    groove_BindingSources binding_sources;
    u64 input_source_runtime_namespace;
    u64 next_input_source_id;
    groove_BindingDeltaVec pending_binding_retractions;
    u64 live_attaches;
    jz_btree deferred_notifications;   /* PublicationId -> Vec<(SubscriptionId, QueuedMultisinkDeltas)> */
    jz_btree durable_notification_publications; /* BTreeSet<PublicationId> */
    jz_btree completed_deferred_publications;   /* BTreeSet<PublicationId> */
    bool *persistence_indeterminate;   /* Rc<Cell<bool>> */
    void *index_ids;                   /* Rc<RefCell<IndexIdRegistry>> */
    jz_hmap operator_states;           /* OperatorStateKey -> OperatorState */
    jz_hmap arrangement_states;        /* ArrangementKey -> AsOf<ArrangementState, SubTick> */
    jz_hmap arrangement_keys_by_input; /* NodeId -> HashSet<ArrangementKey> */
    jz_hmap eval_memo_keys_by_node;    /* NodeId -> HashSet<EvalMemoKey> */
    groove_EvaluationMemo *eval_memo;
    jz_hmap table_frontiers;           /* String -> u64 */
    jz_hmap binding_frontiers;         /* BindingSourceKey -> u64 */
    u64 memo_use_clock;
    usize eval_memo_bytes;
    u64 hydration_memo_hits;
    u64 hydration_memo_computes;
    jz_hmap hydration_memo_computed_nodes; /* HashSet<NodeId> */
    jz_hmap node_meta;                 /* NodeId -> NodeRuntimeMeta */
    u64 current_tick;
    u64 next_subscription_id;
    u64 next_shape_id;
    u64 logical_nodes_requested;
    bool auto_direct_family_enabled;
    bool plain_output_root_positions;
    bool collect_tick_runtime_stats;
};
extern const jz_hmap_desc groove_ivm_rt_a_hdesc_multisink_subscriptions;
extern const jz_hmap_desc groove_ivm_rt_a_hdesc_subscription_id_set;
extern const jz_hmap_desc groove_ivm_rt_a_hdesc_prepared_shapes;
u64 groove_ivm_rt_a_hash_subscription_id(const void *k);
bool groove_ivm_rt_a_eq_subscription_id(const void *a, const void *b);

/* IvmRuntime methods defined in subscriptions.c */
#define SUBS(name) groove__ivm__runtime__subscriptions__IvmRuntime__##name
jz_err SUBS(apply_input_source_deltas)(groove_IvmRuntime *self, groove_InputSourceDelta *deltas, u32 n, groove_OrderedKvStorage *storage, groove_TickMetrics *out);
jz_err SUBS(apply_input_source_deltas_detaching_cold)(groove_IvmRuntime *self, groove_InputSourceDelta *deltas, u32 n, groove_OrderedKvStorage *storage, groove_TickMetrics *out);
jz_err SUBS(replace_input_sources)(groove_IvmRuntime *self, groove_InputSourceReplacement *r, u32 n, groove_OrderedKvStorage *storage, groove_TickMetrics *out);
jz_err SUBS(replace_input_sources_detaching_cold)(groove_IvmRuntime *self, groove_InputSourceReplacement *r, u32 n, groove_OrderedKvStorage *storage, groove_TickMetrics *out);
jz_err SUBS(retire_input_sources)(groove_IvmRuntime *self, const groove_InputSourceId *ids, u32 n, groove_OrderedKvStorage *storage, groove_TickMetrics *out);
typedef struct groove_SinkGraph { jz_str sink; groove_GraphBuilder graph; } groove_SinkGraph;
jz_err SUBS(subscribe_with_lifetime)(groove_IvmRuntime *self, groove_SinkGraph *sinks, u32 n, groove_OrderedKvStorage *storage, groove_SubscriptionLifetime lifetime, groove_RootIndirectValues root, const jz_waker *progress_waker, groove_MultisinkSubscription *out);
jz_err SUBS(subscribe_with_waker)(groove_IvmRuntime *self, groove_SinkGraph *sinks, u32 n, groove_OrderedKvStorage *storage, const jz_waker *progress_waker, groove_MultisinkSubscription *out);
jz_err SUBS(subscribe)(groove_IvmRuntime *self, groove_SinkGraph *sinks, u32 n, groove_OrderedKvStorage *storage, groove_MultisinkSubscription *out);
jz_err SUBS(prepare)(groove_IvmRuntime *self, groove_RoutedMultisinkTerminal *terminals, u32 n, jz_str shape, groove_RecordDescriptor binding_descriptor, groove_OrderedKvStorage *storage, groove_PreparedShape *out);
jz_err SUBS(prepare_shared)(groove_IvmRuntime *self, groove_RoutedMultisinkTerminal *terminals, u32 n, jz_str shape, groove_RecordDescriptor binding_descriptor, groove_OrderedKvStorage *storage, groove_PreparedShape *out);
void SUBS(release_shared_prepared_shape)(groove_IvmRuntime *self, groove_PreparedShapeId id);
void SUBS(transfer_prepared_shape_to_bindings)(groove_IvmRuntime *self, groove_PreparedShapeId id);
jz_err SUBS(bind_shape)(groove_IvmRuntime *self, groove_PreparedShapeId id, const groove_Value *values, u32 n, groove_OrderedKvStorage *storage, groove_MultisinkSubscription *out);
jz_err SUBS(bind_shape_with_lifetime)(groove_IvmRuntime *self, groove_PreparedShapeId id, const groove_Value *values, u32 n, groove_OrderedKvStorage *storage, groove_SubscriptionLifetime lifetime, groove_RootIndirectValues root, const jz_waker *progress_waker, groove_LiveAttach *live /* nullable, moved */, groove_MultisinkSubscription *out);
void SUBS(index_subscription_outputs)(groove_IvmRuntime *self, groove_SubscriptionId id, const jz_btree *outputs);
void SUBS(unindex_subscription_outputs)(groove_IvmRuntime *self, groove_SubscriptionId id, const jz_btree *outputs);
bool SUBS(unsubscribe)(groove_IvmRuntime *self, groove_SubscriptionId id);
jz_err SUBS(unsubscribe_with_storage)(groove_IvmRuntime *self, groove_SubscriptionId id, groove_OrderedKvStorage *storage, bool *out);
jz_err SUBS(retire_prepared_shape)(groove_IvmRuntime *self, groove_PreparedShapeId id);
jz_err SUBS(prune_dropped_subscriptions_with_storage)(groove_IvmRuntime *self, groove_OrderedKvStorage *storage, usize *out);
jz_err SUBS(add_dedup_schema_indices)(groove_IvmRuntime *self);
bool SUBS(subscription_output_node)(const groove_IvmRuntime *self, groove_SubscriptionId id, groove_NodeId *out);
const groove_RecordDescriptor *SUBS(subscription_output)(const groove_IvmRuntime *self, groove_SubscriptionId id);
jz_err SUBS(infer_builder_output)(const groove_IvmRuntime *self, const groove_GraphBuilder *g, groove_RecordDescriptor *out);
jz_err SUBS(infer_builder_output_cached)(const groove_IvmRuntime *self, const groove_GraphBuilder *g, jz_hmap *memo, groove_RecordDescriptor *out);
jz_err SUBS(infer_builder_output_uncached)(const groove_IvmRuntime *self, const groove_GraphBuilder *g, jz_hmap *memo, groove_RecordDescriptor *out);
groove_SubscriptionId SUBS(next_subscription_id)(groove_IvmRuntime *self);
groove_PreparedShapeId SUBS(next_shape_id)(groove_IvmRuntime *self);
jz_err SUBS(prepare_live_attach)(groove_IvmRuntime *self, groove_PreparedShapeId id, const groove_Value *values, u32 n, groove_OrderedKvStorage *storage, bool *some, groove_LiveAttach *out);
bool SUBS(prepared_binding_source_is_bound)(const groove_IvmRuntime *self, const char *shape, u32 len);
groove_BindingSnapshots *SUBS(binding_snapshot_deltas)(groove_IvmRuntime *self);

#endif
