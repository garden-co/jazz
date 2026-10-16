/* groove::ivm::runtime::evaluator (+ evaluator/{kernels,lookup}) */
#ifndef GROOVE_IVM_RUNTIME_EVALUATOR_H
#define GROOVE_IVM_RUNTIME_EVALUATOR_H

#include "subscriptions.h"

/* Poll::Pending of a cooperative yield, propagated like an error through the
 * synchronous port (the driver re-polls; operator state carries progress). */
#define GROOVE_POLL_PENDING 0x1fffu
static inline jz_err groove_poll_pending(void) { jz_err e = { GROOVE_POLL_PENDING, NULL }; return e; }

/* CollectByOrderKey = (Vec<TopBySortPart>, Bytes) */
typedef struct groove_CollectByOrderKey { JZ_VEC(groove_TopBySortPart) parts; jz_bytes record; } groove_CollectByOrderKey;
int groove__ivm__runtime__evaluator__CollectByOrderKey__cmp(const void *a, const void *b, void *ctx);
void groove__ivm__runtime__evaluator__CollectByOrderKey__clone(void *dst, const void *src);
void groove__ivm__runtime__evaluator__CollectByOrderKey__drop(void *k);
bool groove__ivm__runtime__evaluator__CollectByOrderKey__eq(const groove_CollectByOrderKey *a, const groove_CollectByOrderKey *b);
extern const jz_btree_desc groove_ivm_rt_a_desc_order_key_set;      /* BTreeSet<CollectByOrderKey> */
extern const jz_btree_desc groove_ivm_rt_a_desc_order_key_i64;      /* BTreeMap<CollectByOrderKey, i64> */

/* Option<i64> overlay value */
typedef struct groove_OptI64 { bool some; i64 v; } groove_OptI64;

/* CollectByGroup { base: Rc<CountedMap<CollectByOrderKey>>, overlay: Rc<BTreeMap<CollectByOrderKey, Option<i64>>> } */
typedef struct groove_CollectByGroup { groove_CountedMap *base; jz_btree *overlay; } groove_CollectByGroup;
typedef struct groove_OptCollectByGroup { bool some; groove_CollectByGroup g; } groove_OptCollectByGroup;
/* SparseGroups { base: Rc<BTreeMap<Vec<u8>, CollectByGroup>>, overlay: Rc<BTreeMap<Vec<u8>, Option<CollectByGroup>>> } */
typedef struct groove_SparseGroups { jz_btree *base; jz_btree *overlay; } groove_SparseGroups;

void groove__ivm__runtime__evaluator__CollectByGroup__default(groove_CollectByGroup *g);
void groove__ivm__runtime__evaluator__CollectByGroup__clone(void *dst, const void *src);
void groove__ivm__runtime__evaluator__CollectByGroup__drop(void *g);
const i64 *groove__ivm__runtime__evaluator__CollectByGroup__get(const groove_CollectByGroup *g, const groove_CollectByOrderKey *key);
void groove__ivm__runtime__evaluator__CollectByGroup__set(groove_CollectByGroup *g, groove_CollectByOrderKey key, i64 weight);
usize groove__ivm__runtime__evaluator__CollectByGroup__count_before(const groove_CollectByGroup *g, const groove_CollectByOrderKey *key);
void groove__ivm__runtime__evaluator__CollectByGroup__count_before_each(const groove_CollectByGroup *g, const groove_CollectByOrderKey *const *keys, u32 n, groove_UsizeVec *out);
bool groove__ivm__runtime__evaluator__CollectByGroup__is_empty(const groove_CollectByGroup *g);
void groove__ivm__runtime__evaluator__CollectByGroup__commit_overlay(groove_CollectByGroup *g);
typedef struct groove_CollectByGroupIter { groove_CountedMapIter base; bool base_peeked; const void *bk; const i64 *bw; jz_btree_iter overlay; bool ov_peeked; void *ok; void *ov; } groove_CollectByGroupIter;
void groove__ivm__runtime__evaluator__CollectByGroup__iter(const groove_CollectByGroup *g, groove_CollectByGroupIter *it);
bool groove__ivm__runtime__evaluator__CollectByGroupIter__Iterator__next(groove_CollectByGroupIter *it, const groove_CollectByOrderKey **key, const i64 **weight);

void groove__ivm__runtime__evaluator__SparseGroups__default(groove_SparseGroups *s);
void groove__ivm__runtime__evaluator__SparseGroups__clone(groove_SparseGroups *dst, const groove_SparseGroups *src);
void groove__ivm__runtime__evaluator__SparseGroups__drop(groove_SparseGroups *s);
const groove_CollectByGroup *groove__ivm__runtime__evaluator__SparseGroups__get(const groove_SparseGroups *s, const u8 *key, u32 len);
groove_CollectByGroup *groove__ivm__runtime__evaluator__SparseGroups__get_or_default(groove_SparseGroups *s, jz_bytes key);
void groove__ivm__runtime__evaluator__SparseGroups__remove_empty_touched_groups(groove_SparseGroups *s, jz_bytes *keys, u32 n); /* consumes keys */
void groove__ivm__runtime__evaluator__SparseGroups__clear(groove_SparseGroups *s);
void groove__ivm__runtime__evaluator__SparseGroups__count_before_each(const groove_SparseGroups *s, const jz_slice *keys, u32 n, groove_UsizeVec *out);
void groove__ivm__runtime__evaluator__SparseGroups__commit_overlay(groove_SparseGroups *s);

typedef struct groove_TopByIncrementalState { groove_SparseGroups groups; } groove_TopByIncrementalState;
void groove__ivm__runtime__evaluator__TopByIncrementalState__commit_overlays(groove_TopByIncrementalState *s);

typedef struct groove_CollectByIncrementalPayload {
    groove_SparseGroups groups;
    jz_btree roots;                      /* BTreeMap<CollectByOrderKey, i64> */
    groove_RankIndex *emitted_root_order; /* Rc<RankIndex<CollectByOrderKey, Vec<u8>>> */
    groove_RankIndex *emitted_root_keys;  /* Rc<RankIndex<Vec<u8>, ()>> */
} groove_CollectByIncrementalPayload;
typedef struct groove_CollectByIncrementalState { groove_CollectByIncrementalPayload *payload; /* Rc */ } groove_CollectByIncrementalState;
void groove__ivm__runtime__evaluator__CollectByIncrementalState__default(groove_CollectByIncrementalState *s);
groove_CollectByIncrementalPayload *groove__ivm__runtime__evaluator__CollectByIncrementalState__deref_mut(groove_CollectByIncrementalState *s);

typedef struct groove_PendingStreamingChecksum {
    groove_RecordDeltas *input; /* Arc */
    usize next_delta;
    groove_StreamingChecksum *current; /* Option */
    groove_RecordDeltaVec output;
} groove_PendingStreamingChecksum;
typedef struct groove_StreamingChecksumOperatorState { bool has_pending; groove_PendingStreamingChecksum pending; } groove_StreamingChecksumOperatorState;

/* lookup.rs: LookupState(Rc<LookupStateContents>) */
typedef struct groove_LookupStateContents { jz_hmap keys; /* Vec<u8> -> i64 */ jz_hmap rows; /* Vec<u8> -> BTreeMap<Vec<u8>, Bytes> */ } groove_LookupStateContents;
typedef struct groove_LookupState { groove_LookupStateContents *rc; } groove_LookupState;
void groove__ivm__runtime__evaluator__lookup__LookupState__default(groove_LookupState *s);
void groove__ivm__runtime__evaluator__lookup__LookupState__drop(groove_LookupState *s);

/* AsOf instantiations */
typedef struct groove_AsOfSparseGroups { groove_SparseGroups value; groove_AsOfStamp as_of; } groove_AsOfSparseGroups;
typedef struct groove_AsOfTopBy { groove_TopByIncrementalState value; groove_AsOfStamp as_of; } groove_AsOfTopBy;
typedef struct groove_AsOfRecursive { groove_RecursiveState *value; groove_AsOfStamp as_of; } groove_AsOfRecursive;
typedef struct groove_AsOfLookup { groove_LookupState value; groove_AsOfStamp as_of; } groove_AsOfLookup;

enum groove_OperatorStateTag {
    GROOVE_OS_STATELESS, GROOVE_OS_JOIN, GROOVE_OS_SEMI_JOIN, GROOVE_OS_ANTI_JOIN, GROOVE_OS_ARG_BY,
    GROOVE_OS_TOP_BY, GROOVE_OS_RECURSIVE, GROOVE_OS_COLLECT_BY, GROOVE_OS_STREAMING_CHECKSUM, GROOVE_OS_LOOKUP,
};
typedef struct groove_OperatorState {
    u32 tag;
    union {
        groove_SemiJoinState *semi_join;
        groove_AntiJoinState *anti_join;
        groove_AsOfSparseGroups arg_by;
        groove_AsOfTopBy top_by;
        groove_AsOfRecursive recursive;
        groove_CollectByIncrementalState collect_by;
        groove_StreamingChecksumOperatorState *streaming_checksum; /* Box */
        groove_AsOfLookup lookup;
    } v;
} groove_OperatorState;
void groove__ivm__runtime__evaluator__OperatorState__clone(void *dst, const void *src);
void groove__ivm__runtime__evaluator__OperatorState__drop(void *s);
void groove__ivm__runtime__evaluator__operator_state_for(const groove_OpType *op, groove_OperatorState *out);

/* misc free fns */
void groove__ivm__runtime__evaluator__plan_expr_names(const groove_PlanExpr *exprs, u32 n, groove_StrVec *out);
u64 groove__ivm__runtime__evaluator__record_deltas_digest(const groove_RecordDeltas *d);
bool groove__ivm__runtime__evaluator__builder_contains_recursive(const groove_GraphBuilder *g);
jz_err groove__ivm__runtime__evaluator__validate_arg_by_primary_key_indices(const char *op_name, const groove_TableSchema *table, const usize *group, u32 ng, const usize *order, u32 no, const usize *pk, u32 npk);

/* TerminalLineage */
typedef struct groove_LineageCandidate { groove_NodeId node; bool is_public_root; } groove_LineageCandidate;
typedef struct groove_TerminalLineage { JZ_VEC(groove_LineageCandidate) candidates; bool has_public_root; } groove_TerminalLineage;

/* RootOrderingWindows */
typedef struct groove_GroupTransition { groove_CollectByGroup before, after; jz_btree touched; /* BTreeSet<CollectByOrderKey> */ bool touched_complete; } groove_GroupTransition;
typedef struct groove_RootOrderingWindows {
    jz_hmap groups; /* Vec<u8> -> GroupTransition */
    bool has_descriptor; groove_RecordDescriptor descriptor;
    groove_UsizeVec identity;
    bool has_top_by; groove_TopByOp top_by;
} groove_RootOrderingWindows;
typedef struct groove_IndexedRecord { usize index; jz_bytes record; } groove_IndexedRecord;
typedef JZ_VEC(groove_IndexedRecord) groove_IndexedRecordVec;
typedef struct groove_ChangedWindowRows { groove_IndexedRecordVec removed, added; } groove_ChangedWindowRows;
void groove__ivm__runtime__evaluator__RootOrderingWindows__default(groove_RootOrderingWindows *w);
void groove__ivm__runtime__evaluator__RootOrderingWindows__drop(void *w);
typedef void (*groove_BeforeGroupFn)(void *ctx, groove_CollectByGroup *out);
void groove__ivm__runtime__evaluator__RootOrderingWindows__record(groove_RootOrderingWindows *w, groove_RecordDescriptor descriptor, const groove_TopByOp *top_by, const u8 *group, u32 group_len, groove_BeforeGroupFn before, void *before_ctx, const groove_CollectByGroup *after, groove_CollectByOrderKey *touched, u32 n_touched, bool touched_complete);

/* NodeMemoLookup / FrameInputs */
typedef struct groove_NodeMemoLookup { groove_EvalMemoKey key; u64 input_watermark; bool depends_on_context; } groove_NodeMemoLookup;
typedef struct groove_FrameInputs { const usize *slots; u32 len; } groove_FrameInputs;

typedef struct groove_PendingUnaryBatch {
    groove_NodeMemoLookup lookup;
    groove_RecordDeltas *input; /* Arc */
    usize next;
    groove_RecordDeltas output;
    groove_PreparedProjection *projection; /* Option<Arc> */
} groove_PendingUnaryBatch;
void groove__ivm__runtime__evaluator__PendingUnaryBatch__drop(void *b);

typedef JZ_VEC(groove_RecordDeltas *) groove_ArcDeltasVec; /* SmallVec<[Arc<RecordDeltas>; 2]> */

typedef struct groove_TickEvaluator {
    const groove_DatabaseSchema *schema;
    const groove_IvmGraph *graph;
    const jz_hmap *variant_projections;
    const groove_TableDelta *table_deltas; u32 n_table_deltas;
    const groove_BindingDelta *binding_deltas; u32 n_binding_deltas;
    const groove_BindingSnapshots *binding_snapshots;
    u64 current_tick;
    jz_hmap *operator_states;          /* OperatorStateKey -> OperatorState */
    jz_hmap *arrangement_states;       /* ArrangementKey -> AsOf<ArrangementState, SubTick> */
    jz_hmap *arrangement_keys_by_input; /* NodeId -> HashSet<ArrangementKey> */
    groove_EvaluationMemo *eval_memo;
    usize *eval_memo_bytes;
    const jz_hmap *table_frontiers;
    const jz_hmap *binding_frontiers;
    u64 *memo_use_clock;
    jz_hmap *node_meta;                /* NodeId -> NodeRuntimeMeta */
    const groove_OrderedKvStorage *storage; /* Option<&dyn> */
    groove_EvaluationInputs *evaluation_inputs; /* Option<&mut> */
    groove_EvalContext context;
    groove_TickMetrics *metrics;
    jz_hmap terminal_deltas;           /* NodeId -> TerminalDeltas */
    jz_hmap root_ordering_windows;     /* NodeId -> RootOrderingWindows */
} groove_TickEvaluator;
void groove__ivm__runtime__evaluator__TickEvaluator__init_maps(groove_TickEvaluator *e);
void groove__ivm__runtime__evaluator__TickEvaluator__drop(groove_TickEvaluator *e);

typedef struct groove_GraphRuntimeView {
    const groove_DatabaseSchema *schema;
    const groove_IvmGraph *graph;
    const jz_hmap *variant_projections;
    const groove_TableDelta *table_deltas; u32 n_table_deltas;
    const groove_BindingDelta *binding_deltas; u32 n_binding_deltas;
    const groove_BindingSnapshots *binding_snapshots;
    u64 current_tick;
    jz_hmap *operator_states;
    jz_hmap *arrangement_states;
    jz_hmap *arrangement_keys_by_input;
    groove_EvaluationMemo *eval_memo;
    usize *eval_memo_bytes;
    const jz_hmap *table_frontiers;
    const jz_hmap *binding_frontiers;
    u64 *memo_use_clock;
    jz_hmap *node_meta;
    const groove_OrderedKvStorage *storage;
    groove_EvaluationInputs *evaluation_inputs;
    groove_ScopeId scope;
    groove_TickMetrics *metrics;
} groove_GraphRuntimeView;

extern const jz_hmap_desc groove_ivm_rt_a_hdesc_terminal_deltas;    /* NodeId -> TerminalDeltas */
extern const jz_hmap_desc groove_ivm_rt_a_hdesc_root_windows;       /* NodeId -> RootOrderingWindows */
extern const jz_hmap_desc groove_ivm_rt_a_hdesc_pending_unary;      /* NodeId -> PendingUnaryBatch */
extern const jz_hmap_desc groove_ivm_rt_a_hdesc_operator_states;    /* OperatorStateKey -> OperatorState */

/* GraphRuntimeView */
jz_err groove__ivm__runtime__evaluator__GraphRuntimeView__eval_with_binding(groove_GraphRuntimeView *v, u64 sub_tick, groove_FrontierName binding, groove_RecordDeltas deltas, groove_NodeId node, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__evaluator__GraphRuntimeView__eval_with_binding_and_table_deltas(groove_GraphRuntimeView *v, const groove_TableDelta *td, u32 ntd, u64 sub_tick, groove_FrontierName binding, groove_RecordDeltas deltas, groove_NodeId node, groove_RecordDeltas *out);
void groove__ivm__runtime__evaluator__GraphRuntimeView__clear_operator_state_for_scope(groove_GraphRuntimeView *v);
jz_err groove__ivm__runtime__evaluator__GraphRuntimeView__eval_root(groove_GraphRuntimeView *v, groove_NodeId node, groove_RecordDeltas *out);

/* TickEvaluator (evaluator.rs) */
jz_err groove__ivm__runtime__evaluator__TickEvaluator__poll_ready_node(groove_TickEvaluator *e, groove_NodeId node, jz_hmap *pending, groove_FrameInputs frame_inputs, groove_RecordDeltas **out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__update_subgraph(groove_TickEvaluator *e, groove_NodeId root, groove_RecordDeltas **out);
struct groove_RootIdentity;
jz_err groove__ivm__runtime__evaluator__TickEvaluator__apply_root_ordering(groove_TickEvaluator *e, groove_NodeId ordering_node, groove_RecordDescriptor root_descriptor, const struct groove_RootIdentity *identity, const jz_btree *groups, groove_TerminalDeltas *terminal);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__terminal_delta_node_for_output(groove_TickEvaluator *e, groove_NodeId node, bool *has, groove_NodeId *out);
bool groove__ivm__runtime__evaluator__TickEvaluator__terminal_deltas_for_consumer(groove_TickEvaluator *e, groove_NodeId node, bool last_consumer, groove_TerminalDeltas *out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__output_is_structured_collect_by(groove_TickEvaluator *e, groove_NodeId node, bool *out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__output_has_public_root(groove_TickEvaluator *e, groove_NodeId node, bool *out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__update_node(groove_TickEvaluator *e, groove_NodeId node, groove_RecordDeltas **out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__prepare_memo_lookup(groove_TickEvaluator *e, groove_NodeId node, groove_NodeMemoLookup *out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__cached_node_records(groove_TickEvaluator *e, const groove_NodeMemoLookup *lookup, groove_RecordDeltas **out /* NULL = None */);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__resolve_register_inputs(groove_TickEvaluator *e, groove_FrameInputs inputs, bool *some, groove_ArcDeltasVec *out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__update_ready_node(groove_TickEvaluator *e, groove_NodeId node, groove_RecordDeltas **out);
groove_RecordDeltas *groove__ivm__runtime__evaluator__TickEvaluator__memoize_result(groove_TickEvaluator *e, const groove_NodeMemoLookup *lookup, groove_RecordDeltas result);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__compute_node(groove_TickEvaluator *e, groove_NodeId node, groove_NodeMemoLookup lookup, groove_RecordDeltas **out);
groove_EvalMemoKey groove__ivm__runtime__evaluator__TickEvaluator__memo_key(const groove_TickEvaluator *e, groove_NodeId node, const groove_NodeInputSignature *sig);
u64 groove__ivm__runtime__evaluator__TickEvaluator__input_generation(const groove_TickEvaluator *e, groove_NodeId node);
u64 groove__ivm__runtime__evaluator__TickEvaluator__context_digest(const groove_TickEvaluator *e, const groove_NodeInputSignature *sig);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__input_signature(groove_TickEvaluator *e, groove_NodeId node, groove_NodeInputSignature **out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__raw_projection_fields(groove_TickEvaluator *e, groove_NodeId node, const groove_MapProjectOp *project, groove_RecordDescriptor input_desc, groove_RecordDescriptor output_desc, groove_PreparedProjection **out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__frontier_source(const groove_TickEvaluator *e, const groove_FrontierSourceOp *fs, groove_RecordDescriptor output, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__join_output_projection(groove_TickEvaluator *e, groove_NodeId node, groove_RecordDescriptor l, groove_RecordDescriptor r, groove_RecordDescriptor o, groove_PreparedRecordCopy **out);
groove_StrList *groove__ivm__runtime__evaluator__TickEvaluator__aggregate_group_fields(groove_TickEvaluator *e, groove_NodeId node, const groove_AggregateOp *agg);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__materialize_indirect_input(groove_TickEvaluator *e, groove_RecordDeltas *input, groove_RecordDeltas **out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__materialize_indirect_field_indices(groove_TickEvaluator *e, groove_RecordDeltas *input, const usize *idx, u32 n, groove_RecordDeltas **out);

/* evaluator-internal entry points shared with kernels.c / lookup.c */
jz_err groove__ivm__runtime__evaluator__TickEvaluator__compute_filter(groove_TickEvaluator *e, groove_NodeId node, const groove_FilterOp *filter, groove_RecordDescriptor output_desc, groove_RecordDeltas *input, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__compute_projection(groove_TickEvaluator *e, groove_NodeId node, const groove_MapProjectOp *project, groove_RecordDescriptor output_desc, const groove_RecordDeltas *input, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__materialize_indirect_fields(groove_TickEvaluator *e, groove_RecordDeltas *input, const jz_btree *fields, groove_RecordDeltas **out);
bool groove__ivm__runtime__evaluator__TickEvaluator__arrangement_needs_index(const groove_TickEvaluator *e, groove_NodeId node);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__arrangement_key(groove_TickEvaluator *e, groove_NodeId input, groove_RecordDescriptor d, const jz_str *fields, u32 nfields, u32 comparison, groove_ArrangementKey *out);
groove_SubTick groove__ivm__runtime__evaluator__TickEvaluator__arrangement_sub_tick(const groove_TickEvaluator *e, const groove_ArrangementKey *key);
void groove__ivm__runtime__evaluator__TickEvaluator__insert_arrangement(groove_TickEvaluator *e, groove_ArrangementKey key, groove_AsOfArrangement state);
groove_OperatorStateKey groove__ivm__runtime__evaluator__TickEvaluator__operator_key(const groove_TickEvaluator *e, groove_NodeId node);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__update_join(groove_TickEvaluator *e, groove_NodeId node, const groove_JoinOp *join, groove_RecordDescriptor output_desc, groove_NodeId l, groove_NodeId r, groove_RecordDeltas *ld, groove_RecordDeltas *rd, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__update_threshold_join(groove_TickEvaluator *e, groove_NodeId node, const groove_JoinOp *join, groove_RecordDescriptor output_desc, groove_NodeId l, groove_NodeId r, groove_RecordDeltas *ld, groove_RecordDeltas *rd, bool semi, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__update_semi_join(groove_TickEvaluator *e, groove_NodeId node, const groove_JoinOp *join, groove_RecordDescriptor output_desc, groove_NodeId l, groove_NodeId r, groove_RecordDeltas *ld, groove_RecordDeltas *rd, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__update_anti_join(groove_TickEvaluator *e, groove_NodeId node, const groove_JoinOp *join, groove_RecordDescriptor output_desc, groove_NodeId l, groove_NodeId r, groove_RecordDeltas *ld, groove_RecordDeltas *rd, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__update_arg_by(groove_TickEvaluator *e, groove_NodeId node, groove_ArgBySpec spec, groove_RecordDescriptor output_desc, const groove_RecordDeltas *input, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__update_top_by(groove_TickEvaluator *e, groove_NodeId node, const groove_TopByOp *top_by, groove_RecordDescriptor output_desc, const groove_RecordDeltas *input, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__update_collect_by(groove_TickEvaluator *e, groove_NodeId node, const groove_CollectByOp *cb, groove_RecordDescriptor output_desc, const groove_RecordDeltas *input, const groove_RecordDeltas *canonical, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__update_aggregate(groove_TickEvaluator *e, groove_NodeId node, const groove_AggregateOp *agg, groove_RecordDescriptor output_desc, const groove_RecordDeltas *input, const groove_RecordDeltas *canonical, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__update_recursive_step_witness(groove_TickEvaluator *e, groove_NodeId recursive, groove_RecordDescriptor output_desc, groove_RecordDeltas *out);
void groove__ivm__runtime__evaluator__plan_expr_fields(const groove_PlanExpr *exprs, u32 n, jz_btree *out);
/* kernels.rs */
jz_err groove__ivm__runtime__evaluator__kernels__TickEvaluator__compute_batch(groove_TickEvaluator *e, groove_NodeId node, groove_RecordDeltas *const *inputs, u32 n, groove_RecordDeltas *out);
/* lookup.rs */
jz_err groove__ivm__runtime__evaluator__lookup__TickEvaluator__update_lookup(groove_TickEvaluator *e, groove_NodeId node, const groove_LookupOp *lookup, groove_RecordDescriptor output_desc, groove_RecordDeltas *input, groove_RecordDeltas *out);

/* helper: hmap entry into node_meta with Default */
groove_NodeRuntimeMeta *groove_ivm_rt_a_node_meta_entry(jz_hmap *node_meta, groove_NodeId node);

/* jz_rc payloads used for Arc<[String]> */
groove_StrList *groove_ivm_rt_a_strlist_from_vec(groove_StrVec v);

#endif
