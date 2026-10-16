/* groove::ivm::runtime: shared declarations of unit groove-ivm-rt-b
 * (mod.rs, state.rs and the files of this unit). */
#ifndef GROOVE_IVM_RUNTIME_MOD_H
#define GROOVE_IVM_RUNTIME_MOD_H
#include "jz_rt.h"
#include "../../_ext_groove-ivm-rt-b.h"

/* ---------------- IvmRuntimeError (mod.rs) ---------------- */
enum groove_IvmRuntimeErrorCode {
    GROOVE_IVM_ERR_BASE = 0x0600,
    GROOVE_IVM_PersistenceOutcomeIndeterminate,
    GROOVE_IVM_AggregateOverflow,
    GROOVE_IVM_GraphFieldNotFound,
    GROOVE_IVM_AmbiguousGraphFieldReference,
    GROOVE_IVM_GraphFieldIndexOutOfBounds,
    GROOVE_IVM_GraphInputArityMismatch,
    GROOVE_IVM_GraphInputMissing,
    GROOVE_IVM_GraphNodeNotFound,
    GROOVE_IVM_GraphCycle,
    GROOVE_IVM_GraphOutputMismatch,
    GROOVE_IVM_EnumTagProjectionAbsent,
    GROOVE_IVM_EnumTagProjectionNonEnum,
    GROOVE_IVM_EnumProjectionAbsent,
    GROOVE_IVM_EnumProjectionNonEnum,
    GROOVE_IVM_IndexNotFound,
    GROOVE_IVM_InvalidPersistedIndex,
    GROOVE_IVM_UnsupportedIndexIntersectionScan,
    GROOVE_IVM_UnsupportedIndexCandidateFilter,
    GROOVE_IVM_UnsupportedLookup,
    GROOVE_IVM_JoinKeyArityMismatch,
    GROOVE_IVM_ShapeKeyFieldNotFound,
    GROOVE_IVM_MissingPrimaryKey,
    GROOVE_IVM_NodeStateMissing,
    GROOVE_IVM_NodeStateOperatorMismatch,
    GROOVE_IVM_OutOfOrderRuntimeState,
    GROOVE_IVM_PersistRecordMismatch,
    GROOVE_IVM_BindingSourceRequiresPrepare,
    GROOVE_IVM_EmptyMultisinkSubscription,
    GROOVE_IVM_DuplicateMultisinkSink,
    GROOVE_IVM_MultisinkSinkRequiresPrepare,
    GROOVE_IVM_RoutedMultisinkRouteArityMismatch,
    GROOVE_IVM_BindingSourceNotFound,
    GROOVE_IVM_BindingSourceDescriptorMismatch,
    GROOVE_IVM_ForeignInputSource,
    GROOVE_IVM_InputSourceRetired,
    GROOVE_IVM_DuplicateTableVariant,
    GROOVE_IVM_RecordEncoding,
    GROOVE_IVM_RecursiveIterationLimit,
    GROOVE_IVM_Storage,
    GROOVE_IVM_Chunk,
    GROOVE_IVM_LargeValue,
    GROOVE_IVM_StorageUnavailable,
    GROOVE_IVM_EvaluationBlocked,
    GROOVE_IVM_InvalidStreamingChecksumBudget,
    GROOVE_IVM_StreamingChecksumTypeMismatch,
    GROOVE_IVM_PreparedShapeNotFound,
    GROOVE_IVM_PreparedShapeHasActiveBindings,
    GROOVE_IVM_StaleRuntimeState,
    GROOVE_IVM_TableNotFound,
    GROOVE_IVM_TableAlreadyExists,
    GROOVE_IVM_TableFieldAlreadyExists,
    GROOVE_IVM_UnknownTableVariant,
    GROOVE_IVM_VariantProjectionNotFound,
    GROOVE_IVM_VariantProjectionRequired,
    GROOVE_IVM_VariantProjectionOutputMismatch,
    GROOVE_IVM_VariantProjectionCaseAlreadyRegistered,
    GROOVE_IVM_VariantProjectionCaseNotFound,
    GROOVE_IVM_VariantProjectionSourceMismatch,
    GROOVE_IVM_VariantProjectionEnumFieldNotFound,
    GROOVE_IVM_VariantProjectionEnumOutputMustBeSingleField,
    GROOVE_IVM_VariantProjectionEnumFieldTypeMismatch,
    GROOVE_IVM_VariantProjectionEnumSchemaMismatch,
    GROOVE_IVM_VariantProjectionEnumPayloadMismatch,
    GROOVE_IVM_RecursiveEnumProjectionDescriptorMismatch,
    GROOVE_IVM_VariantProjectFieldTypeMismatch,
    GROOVE_IVM_VariantProjectPayloadMismatch,
    GROOVE_IVM_UniqueIndexViolation,
    GROOVE_IVM_UnsupportedJoinKey,
    GROOVE_IVM_UnsupportedNonMonotoneRecursion,
    GROOVE_IVM_UnsupportedNestedRecursion,
    GROOVE_IVM_UnsupportedArgMaxBy,
    GROOVE_IVM_CollectByMustBeTerminal,
    GROOVE_IVM_InvalidCollectBy,
    GROOVE_IVM_InvalidTopBy,
    GROOVE_IVM_DuplicateCollectByOccurrenceId,
    GROOVE_IVM_UnsupportedOperator,
};
/* Constructors carrying the thiserror Display text (mod.c). */
jz_err groove__ivm__runtime__IvmRuntimeError__simple(u32 code);
jz_err groove__ivm__runtime__IvmRuntimeError__with_str(u32 code, const char *p, u32 n);
jz_err groove__ivm__runtime__IvmRuntimeError__with_node(u32 code, groove_NodeId node);
jz_err groove__ivm__runtime__IvmRuntimeError__with_usize(u32 code, u64 v);
jz_err groove__ivm__runtime__IvmRuntimeError__table_target(u32 code, const jz_str *table, const char *target, u32 tlen);
jz_err groove__ivm__runtime__IvmRuntimeError__table_target_version(u32 code, const jz_str *table, const char *target, u32 tlen, u64 version);
jz_err groove__ivm__runtime__IvmRuntimeError__table_target_field(u32 code, const jz_str *table, const char *target, u32 tlen, const char *field, u32 flen);
jz_err groove__ivm__runtime__IvmRuntimeError__join_arity(u64 left, u64 right);
jz_err groove__ivm__runtime__IvmRuntimeError__recursive_limit(groove_NodeId node, u64 max_iters);
jz_err groove__ivm__runtime__IvmRuntimeError__stale(jz_str expected, bool has_actual, jz_str actual);
jz_err groove__ivm__runtime__IvmRuntimeError__out_of_order(jz_str current, jz_str next);
#define GROOVE_IVM_ERR(code) groove__ivm__runtime__IvmRuntimeError__simple(code)
#define GROOVE_IVM_ERR_S(code, s) groove__ivm__runtime__IvmRuntimeError__with_str((code), (s)->ptr, (s)->len)
#define GROOVE_IVM_ERR_C(code, cs) groove__ivm__runtime__IvmRuntimeError__with_str((code), (cs), (u32)strlen(cs))
#define GROOVE_IVM_ERR_N(code, n) groove__ivm__runtime__IvmRuntimeError__with_node((code), (n))

/* ---------------- state.rs ---------------- */
typedef struct groove_ExecutionLayoutStats { u64 builds, hits; } groove_ExecutionLayoutStats;
typedef struct groove_RuntimeStats {
    u32 graph_nodes, active_subscriptions, active_prepared_shapes, active_shape_params, arrangement_count;
    u32 eval_memo_entries, eval_memo_bytes, hydration_memo_entries;
    u64 hydration_memo_hits, hydration_memo_computes;
    u32 hydration_memo_distinct_computed_nodes, arrangement_rows, arrangement_encoded_bytes;
    u32 recursive_state_count, recursive_accumulated_rows, recursive_accumulated_encoded_bytes;
    u64 logical_nodes_requested;
    u32 deduped_graph_nodes;
} groove_RuntimeStats;
f64 groove__ivm__runtime__state__RuntimeStats__dedupe_ratio(const groove_RuntimeStats *s);
typedef struct groove_TickMetrics {
    u64 tick;
    u32 table_delta_records, records_processed, nodes_evaluated, subscriptions_considered;
    u32 root_ordering_position_records, root_ordering_position_records_skipped, top_by_delta_membership_records;
    u32 recursive_recomputes;
    u64 hydration_memo_hits, hydration_memo_computes;
    jz_hmap hydration_memo_computed_nodes;
    u32 notifications_sent, notification_records, notification_encoded_bytes;
    groove_RuntimeStats runtime_stats;
} groove_TickMetrics;
void groove_TickMetrics_drop(groove_TickMetrics *m);
void groove_TickMetrics_clone(groove_TickMetrics *dst, const groove_TickMetrics *src);

typedef struct groove_ScopePath { groove_NodeIdVec nodes; } groove_ScopePath;
typedef const groove_ScopePath *groove_ScopeId; /* Intern<ScopePath> */
groove_ScopeId groove__ivm__runtime__state__ScopeId__root(void);
groove_ScopeId groove__ivm__runtime__state__ScopeId__child(groove_ScopeId self, groove_NodeId recursive_node);
bool groove__ivm__runtime__state__ScopeId__outermost(groove_ScopeId self, groove_NodeId *out);

typedef struct groove_OperatorStateKey { groove_ScopeId scope; groove_NodeId node; } groove_OperatorStateKey;
typedef struct groove_ArrangementKey { groove_ScopeId scope; groove_NodeId input; } groove_ArrangementKey;
u64 groove_ScopedKey_hash(const void *k);   /* shared by OperatorStateKey and ArrangementKey */
bool groove_ScopedKey_eq(const void *a, const void *b);
typedef struct groove_SubTick { u64 tick; u64 sub_tick; } groove_SubTick;
int groove_SubTick_cmp(groove_SubTick a, groove_SubTick b);
/* AsOf<T, S>: one C copy. S is SubTick or Tick (Tick uses sub_tick = 0). */
typedef enum { groove_AsOfKind_SubTick, groove_AsOfKind_Tick } groove_AsOfKind;
typedef struct groove_AsOfStamp { bool has; groove_SubTick at; u8 kind; } groove_AsOfStamp;
jz_err groove__ivm__runtime__state__AsOf__value_at(const groove_AsOfStamp *self, groove_SubTick expected);
jz_err groove__ivm__runtime__state__AsOf__mark_forward_as_of(groove_AsOfStamp *self, groove_SubTick next);
void groove__ivm__runtime__state__AsOf__replace_as_of_at_least(groove_AsOfStamp *self, groove_SubTick next);

typedef enum { groove_ArrangementUpdateMode_Accumulate, groove_ArrangementUpdateMode_Replace } groove_ArrangementUpdateMode;
typedef enum { groove_EvalMode_Tick, groove_EvalMode_Hydrate } groove_EvalMode;
typedef enum { groove_HydrationMode_Ordinary, groove_HydrationMode_Subscription } groove_HydrationMode;

typedef struct groove_EvalMemoKey {
    groove_ScopeId scope; groove_NodeId node; u64 input_signature_hash;
    bool has_tick_epoch; u64 tick_epoch; u64 sub_tick; u64 context_digest;
} groove_EvalMemoKey;
u64 groove_EvalMemoKey_hash(const void *k);
bool groove_EvalMemoKey_eq(const void *a, const void *b);
typedef struct groove_EvalMemoEntry { groove_RecordDeltas *records; /* Arc */ u64 input_watermark; u32 payload_bytes; u64 last_used; } groove_EvalMemoEntry;
void groove_EvalMemoEntry_drop(void *e);
void groove_EvalMemoEntry_clone(void *dst, const void *src);
groove_EvalMemoEntry groove__ivm__runtime__state__EvalMemoEntry__new(groove_RecordDeltas *records, u64 input_watermark, u32 payload_bytes, u64 last_used);

typedef struct groove_NodeInputSignature {
    jz_str *tables; u32 tables_len;                       /* Arc<[String]> */
    groove_BindingSourceKey *bindings; u32 bindings_len;  /* Arc<[BindingSourceKey]> */
    groove_FrontierName *frontier_bindings; u32 frontier_bindings_len;
    u64 hash;
} groove_NodeInputSignature;
void groove__ivm__runtime__state__NodeInputSignature__from_sets(jz_btree *tables, jz_btree *bindings, jz_btree *frontier_bindings, groove_NodeInputSignature *out);
void groove_NodeInputSignature_drop(groove_NodeInputSignature *s);

typedef struct groove_EvalContext {
    groove_ScopeId scope; u64 sub_tick;
    jz_hmap bindings;         /* FrontierName -> RecordDeltas */
    jz_hmap binding_digests;  /* FrontierName -> u64 */
    groove_ArrangementUpdateMode arrangement_update_mode;
    groove_EvalMode eval_mode;
    bool hydrate_arrangements;
} groove_EvalContext;
void groove__ivm__runtime__state__EvalContext__root(groove_EvalContext *out);
void groove__ivm__runtime__state__EvalContext__root_snapshot(groove_EvalContext *out);
void groove__ivm__runtime__state__EvalContext__root_subscription_snapshot(groove_EvalContext *out);
void groove__ivm__runtime__state__EvalContext__with_binding(groove_ScopeId scope, u64 sub_tick, groove_FrontierName binding, groove_RecordDeltas deltas, groove_EvalContext *out);
void groove__ivm__runtime__state__EvalContext__with_binding_and_arrangement_mode(groove_ScopeId scope, u64 sub_tick, groove_FrontierName binding, groove_RecordDeltas deltas, groove_ArrangementUpdateMode mode, groove_EvalContext *out);
void groove_EvalContext_drop(groove_EvalContext *c);
void groove_EvalContext_clone(groove_EvalContext *dst, const groove_EvalContext *src);

/* shared hmap/btree descriptors (state.c) */
extern const jz_hmap_desc groove_ivm_rt_NODE_SET;       /* HashSet<NodeId> */
extern const jz_hmap_desc groove_ivm_rt_NODE_U64_MAP;   /* HashMap<NodeId, u64> */
extern const jz_hmap_desc groove_ivm_rt_STR_U64_MAP;    /* HashMap<String, u64> */
extern const jz_hmap_desc groove_ivm_rt_STR_DESC_MAP;   /* HashMap<String, RecordDescriptor> */
extern const jz_hmap_desc groove_ivm_rt_FRONTIER_DELTAS_MAP;
extern const jz_hmap_desc groove_ivm_rt_FRONTIER_U64_MAP;
extern const jz_hmap_desc groove_ivm_rt_BINDING_U64_MAP;
extern const jz_btree_desc groove_ivm_rt_STR_SET;       /* BTreeSet<String> */
extern const jz_btree_desc groove_ivm_rt_BINDING_SET;   /* BTreeSet<BindingSourceKey> */
extern const jz_btree_desc groove_ivm_rt_FRONTIER_SET;  /* BTreeSet<FrontierName> */
extern const jz_btree_desc groove_ivm_rt_BYTES_SET;     /* BTreeSet<Vec<u8>> */
extern const jz_btree_desc groove_ivm_rt_BYTES_I64_MAP; /* BTreeMap<Vec<u8>, i64> */
u64 groove_ivm_rt_str_hash_v(const void *k);
bool groove_ivm_rt_str_eq_v(const void *a, const void *b);
void groove_ivm_rt_str_drop_v(void *s);
void groove_ivm_rt_bytes_drop_v(void *b);
u64 groove_ivm_rt_bytes_hash_v(const void *k);
bool groove_ivm_rt_bytes_eq_v(const void *a, const void *b);
void groove_ivm_rt_hmap_drop_v(void *m);

/* ---------------- counted_map.rs ---------------- */
typedef struct groove_CountedMapKeyDesc { u32 size; jz_cmp_fn cmp; void (*drop)(void *); void (*clone)(void *, const void *); } groove_CountedMapKeyDesc;
typedef struct groove_CountedMapChunk { jz_vec entries; /* (K, i64) packed */ u32 positive; } groove_CountedMapChunk;
typedef struct groove_CountedMap {
    JZ_VEC(groove_CountedMapChunk) chunks;
    JZ_VEC(u32) positive_tree;
    u32 len;
    const groove_CountedMapKeyDesc *kd;
} groove_CountedMap;
void groove__ivm__runtime__counted_map__CountedMap__default(groove_CountedMap *m, const groove_CountedMapKeyDesc *kd);
void groove_CountedMap_drop(groove_CountedMap *m);
void groove_CountedMap_clone(groove_CountedMap *dst, const groove_CountedMap *src);
const i64 *groove__ivm__runtime__counted_map__CountedMap__get(const groove_CountedMap *m, const void *key);
bool groove__ivm__runtime__counted_map__CountedMap__insert(groove_CountedMap *m, const void *key, i64 weight, i64 *old);
bool groove__ivm__runtime__counted_map__CountedMap__remove(groove_CountedMap *m, const void *key, i64 *removed);
u32 groove__ivm__runtime__counted_map__CountedMap__positive_before(const groove_CountedMap *m, const void *key);
typedef struct groove_CountedMapIter { const groove_CountedMap *m; u32 chunk; u32 idx; } groove_CountedMapIter;
void groove__ivm__runtime__counted_map__CountedMap__iter(const groove_CountedMap *m, groove_CountedMapIter *it);
bool groove__ivm__runtime__counted_map__Iter__next(groove_CountedMapIter *it, const void **key, const i64 **weight);
void groove__ivm__runtime__counted_map__CountedMap__from_iter(groove_CountedMap *m, const groove_CountedMapKeyDesc *kd, void *pairs, u32 n);

/* ---------------- rank_index.rs ---------------- */
typedef struct groove_RankIndexDesc { u32 key_size, val_size; jz_cmp_fn cmp; void (*key_drop)(void *); void (*val_drop)(void *); } groove_RankIndexDesc;
typedef struct groove_RankNode groove_RankNode;  /* Rc<Node> */
typedef struct groove_RankIndex { groove_RankNode *root; const groove_RankIndexDesc *d; } groove_RankIndex;
void groove__ivm__runtime__rank_index__RankIndex__default(groove_RankIndex *r, const groove_RankIndexDesc *d);
void groove_RankIndex_clone(groove_RankIndex *dst, const groove_RankIndex *src);
void groove_RankIndex_drop(groove_RankIndex *r);
void groove__ivm__runtime__rank_index__RankIndex__insert(groove_RankIndex *r, const void *key, const void *value);
void groove__ivm__runtime__rank_index__RankIndex__remove(groove_RankIndex *r, const void *key);
bool groove__ivm__runtime__rank_index__RankIndex__contains(const groove_RankIndex *r, const void *key);
u32 groove__ivm__runtime__rank_index__RankIndex__rank(const groove_RankIndex *r, const void *key);

/* ---------------- evaluation_memo.rs ---------------- */
typedef struct groove_EvalMemoSlot { bool some; groove_EvalMemoKey key; groove_EvalMemoEntry entry; } groove_EvalMemoSlot;
typedef struct groove_EvaluationMemo {
    groove_ExecutionLayout *layout; /* Option<Arc<ExecutionLayout>> */
    JZ_VEC(groove_EvalMemoSlot) slots;
    u32 occupied;
    jz_hmap overflow; /* EvalMemoKey -> EvalMemoEntry */
    u32 tick_entries;
} groove_EvaluationMemo;
void groove__ivm__runtime__evaluation_memo__EvaluationMemo__default(groove_EvaluationMemo *m);
void groove_EvaluationMemo_drop(groove_EvaluationMemo *m);
void groove_EvaluationMemo_clone(groove_EvaluationMemo *dst, const groove_EvaluationMemo *src);
void groove__ivm__runtime__evaluation_memo__EvaluationMemo__for_layout(groove_EvaluationMemo *m, groove_ExecutionLayout *layout);
void groove__ivm__runtime__evaluation_memo__EvaluationMemo__set_layout(groove_EvaluationMemo *m, groove_ExecutionLayout *layout);
const groove_EvalMemoSlot *groove__ivm__runtime__evaluation_memo__EvaluationMemo__slot(const groove_EvaluationMemo *m, u32 slot);
groove_EvalMemoSlot *groove__ivm__runtime__evaluation_memo__EvaluationMemo__slot_mut(groove_EvaluationMemo *m, u32 slot);
const groove_EvalMemoEntry *groove__ivm__runtime__evaluation_memo__EvaluationMemo__get(const groove_EvaluationMemo *m, const groove_EvalMemoKey *key);
groove_EvalMemoEntry *groove__ivm__runtime__evaluation_memo__EvaluationMemo__get_mut(groove_EvaluationMemo *m, const groove_EvalMemoKey *key);
bool groove__ivm__runtime__evaluation_memo__EvaluationMemo__insert(groove_EvaluationMemo *m, groove_EvalMemoKey key, groove_EvalMemoEntry entry, groove_EvalMemoEntry *previous);
bool groove__ivm__runtime__evaluation_memo__EvaluationMemo__remove(groove_EvaluationMemo *m, const groove_EvalMemoKey *key, groove_EvalMemoEntry *removed);
void groove__ivm__runtime__evaluation_memo__EvaluationMemo__retain(groove_EvaluationMemo *m, bool (*keep)(const groove_EvalMemoKey *, groove_EvalMemoEntry *, void *), void *ctx);
u32 groove__ivm__runtime__evaluation_memo__EvaluationMemo__tick_entries(const groove_EvaluationMemo *m);
u32 groove__ivm__runtime__evaluation_memo__EvaluationMemo__len(const groove_EvaluationMemo *m);
typedef struct groove_EvalMemoIter { const groove_EvaluationMemo *m; u32 slot; jz_hmap_iter oit; } groove_EvalMemoIter;
void groove__ivm__runtime__evaluation_memo__EvaluationMemo__iter(const groove_EvaluationMemo *m, groove_EvalMemoIter *it);
bool groove__ivm__runtime__evaluation_memo__EvalMemoIter__next(groove_EvalMemoIter *it, const groove_EvalMemoKey **key, const groove_EvalMemoEntry **entry);

/* ---------------- compilation_cache.rs ---------------- */
typedef struct groove_CompiledInput { groove_CompiledNode compiled; groove_RecordDescriptor output; } groove_CompiledInput;
typedef struct groove_CompilationCacheEntry {
    groove_GraphBuilder *owner; /* Weak<GraphBuilder> */
    JZ_VEC(groove_CompiledInput) inputs;
    groove_RecordDescriptor inferred;
    groove_CompiledNode compiled;
    u64 logical_nodes;
} groove_CompilationCacheEntry;
typedef struct groove_CompilationCache { jz_hmap slots; /* usize -> Entry */ } groove_CompilationCache;
void groove__ivm__runtime__compilation_cache__CompilationCache__default(groove_CompilationCache *c);
void groove_CompilationCache_drop(groove_CompilationCache *c);
bool groove__ivm__runtime__compilation_cache__CompilationCache__get(groove_CompilationCache *c, groove_GraphBuilder *owner,
    const jz_hmap *compiled, const jz_hmap *outputs, const groove_IvmGraph *graph,
    groove_CompiledNode *out_compiled, groove_RecordDescriptor *out_inferred, u64 *out_logical);
void groove__ivm__runtime__compilation_cache__CompilationCache__insert(groove_CompilationCache *c, groove_GraphBuilder *owner,
    const jz_hmap *compiled, const jz_hmap *outputs, groove_CompiledNode result, groove_RecordDescriptor inferred, u64 logical_nodes);
typedef struct groove_CompilationStep { const groove_GraphBuilder *graph; groove_GraphBuilder *owner; } groove_CompilationStep;
typedef JZ_VEC(groove_CompilationStep) groove_CompilationStepVec;
void groove__ivm__runtime__compilation_cache__compilation_order(const groove_GraphBuilder *graph, groove_CompilationStepVec *out);
extern const jz_hmap_desc groove_ivm_rt_USIZE_COMPILED_MAP; /* HashMap<usize, CompiledNode> */
extern const jz_hmap_desc groove_ivm_rt_USIZE_DESC_MAP;     /* HashMap<usize, RecordDescriptor> */

/* ---------------- typed_projection.rs ---------------- */
typedef struct groove_ProjectionPlan {
    groove_RecordDescriptor input;
    groove_ProjectFieldVec fields;
    groove_RecordDescriptor output;
    bool expressions_set; groove_ProjectionExprVec expressions; /* OnceCell */
} groove_ProjectionPlan;
typedef struct groove_ProjectionPlans { JZ_VEC(groove_ProjectionPlan *) slots; /* Option<Rc<ProjectionPlan>> */ } groove_ProjectionPlans;
void groove__ivm__runtime__typed_projection__ProjectionPlans__default(groove_ProjectionPlans *p);
void groove_ProjectionPlans_drop(groove_ProjectionPlans *p);
void groove_ProjectionPlans_clone(groove_ProjectionPlans *dst, const groove_ProjectionPlans *src);
jz_err groove__ivm__runtime__typed_projection__ProjectionPlan__expressions(groove_ProjectionPlan *plan, const groove_ProjectionExprVec **out);
jz_err groove__ivm__runtime__typed_projection__ProjectionPlans__get(groove_ProjectionPlans *p, groove_RecordDescriptor input, const groove_ProjectField *fields, u32 n, groove_ProjectionPlan **out);

/* ---------------- IvmRuntime (mod.rs) ---------------- */
typedef struct groove_IvmRuntime {
    groove_DatabaseSchema schema;
    groove_OwnedChunkProvider chunk_provider;
    jz_hmap table_storage_descriptors; /* String -> RecordDescriptor */
    jz_hmap table_descriptors;         /* String -> RecordDescriptor */
    jz_hmap variant_descriptors;       /* String -> HashMap<u32, RecordDescriptor> */
    jz_hmap variant_projections;       /* VariantProjectionKey -> VariantProjection */
    groove_IvmGraph *graph;
    groove_ProjectionPlans projection_plans; /* RefCell */
    groove_CompilationCache compilation_cache;
    jz_hmap multisink_subscriptions;   /* SubscriptionId -> MultisinkSubscriptionState */
    jz_hmap subscriptions_by_output_node; /* NodeId -> HashSet<SubscriptionId> */
    groove_PendingIncrementalEvaluation pending_incremental;
    bool pending_incremental_polling;
    bool has_owner_progress_waker; jz_waker owner_progress_waker;
    bool has_owner_fanout_waker; jz_waker owner_fanout_waker[2];
    bool ephemeral_graph_gc_pending;
    jz_hmap gc_candidates;             /* HashSet<NodeId> */
    jz_hmap prepared_shapes;           /* PreparedShapeId -> RoutedMultisinkShapeState */
    jz_hmap auto_direct_families;      /* AutoDirectFamilyKey -> PreparedShapeId */
    jz_hmap shared_prepared_shapes;    /* SharedShapeKey -> PreparedShapeId */
    groove_BindingSources binding_sources;
    u64 input_source_runtime_namespace;
    u64 next_input_source_id;
    JZ_VEC(groove_BindingDelta) pending_binding_retractions;
    u64 live_attaches;
    jz_hmap deferred_notifications;    /* PublicationId -> Vec<(SubscriptionId, QueuedMultisinkDeltas)> */
    jz_hmap durable_notification_publications;
    jz_hmap completed_deferred_publications;
    bool *persistence_indeterminate;   /* Rc<Cell<bool>> */
    struct groove_IndexIdRegistry *index_ids; /* Rc<RefCell<IndexIdRegistry>> */
    jz_hmap operator_states;           /* OperatorStateKey -> OperatorState */
    jz_hmap arrangement_states;        /* ArrangementKey -> AsOf<ArrangementState, SubTick> */
    jz_hmap arrangement_keys_by_input; /* NodeId -> HashSet<ArrangementKey> */
    jz_hmap eval_memo_keys_by_node;    /* NodeId -> HashSet<EvalMemoKey> */
    groove_EvaluationMemo eval_memo;
    jz_hmap table_frontiers;           /* String -> u64 */
    jz_hmap binding_frontiers;         /* BindingSourceKey -> u64 */
    u64 memo_use_clock;
    u32 eval_memo_bytes;
    u64 hydration_memo_hits;
    u64 hydration_memo_computes;
    jz_hmap hydration_memo_computed_nodes;
    jz_hmap node_meta;                 /* NodeId -> NodeRuntimeMeta */
    u64 current_tick;
    u64 next_subscription_id;
    u64 next_shape_id;
    u64 logical_nodes_requested;
    bool auto_direct_family_enabled;
    bool plain_output_root_positions;
    bool collect_tick_runtime_stats;
} groove_IvmRuntime;

jz_err groove__ivm__runtime__typed_projection__IvmRuntime__projection_plan(groove_IvmRuntime *self, groove_RecordDescriptor input, const groove_ProjectField *fields, u32 n, groove_ProjectionPlan **out);

/* ---------------- index_ids.rs ---------------- */
typedef struct groove_StrPair { jz_str a; jz_str b; } groove_StrPair;
typedef struct groove_IndexIdEntry { u32 id; jz_bytes definition; } groove_IndexIdEntry;
typedef struct groove_IndexIdRegistry {
    jz_btree entries;      /* (String, String) -> IndexIdEntry */
    jz_btree names;        /* u32 -> (String, String) */
    u32 max_id;
    jz_btree unpersisted;  /* BTreeSet<(String, String)> */
    bool marker_persisted;
} groove_IndexIdRegistry;
typedef struct groove_PendingIndexEntry { groove_StrPair key; u32 id; } groove_PendingIndexEntry;
typedef struct groove_PendingIndexIds { groove_WriteOpVec operations; bool marker; JZ_VEC(groove_PendingIndexEntry) entries; } groove_PendingIndexIds;
typedef groove_IndexIdRegistry *groove_SharedIndexIds; /* Rc<RefCell<IndexIdRegistry>> (jz_rc payload) */
extern const char groove__ivm__runtime__index_ids__INDICES_CF[];
groove_SharedIndexIds groove__ivm__runtime__index_ids__SharedIndexIds__default(void);
void groove__ivm__runtime__index_ids__IndexIdRegistry__default(groove_IndexIdRegistry *r);
void groove_IndexIdRegistry_drop(void *r);
bool groove__ivm__runtime__index_ids__PendingIndexIds__is_empty(const groove_PendingIndexIds *p);
const groove_WriteOpVec *groove__ivm__runtime__index_ids__PendingIndexIds__operations(const groove_PendingIndexIds *p);
void groove_PendingIndexIds_drop(groove_PendingIndexIds *p);
jz_err groove__ivm__runtime__index_ids__IndexIdRegistry__load(groove_Storage storage, groove_IndexIdRegistry *out);
u32 groove__ivm__runtime__index_ids__IndexIdRegistry__resolve(groove_IndexIdRegistry *r, const char *table, u32 table_len, const groove_IndexSchema *index);
void groove__ivm__runtime__index_ids__IndexIdRegistry__retire(groove_IndexIdRegistry *r, const char *table, u32 table_len, const jz_str *index);
bool groove__ivm__runtime__index_ids__IndexIdRegistry__names(const groove_IndexIdRegistry *r, u32 id, const jz_str **table, const jz_str **index);
void groove__ivm__runtime__index_ids__IndexIdRegistry__pending(const groove_IndexIdRegistry *r, groove_PendingIndexIds *out);
void groove__ivm__runtime__index_ids__IndexIdRegistry__mark_persisted(groove_IndexIdRegistry *r, const groove_PendingIndexIds *p);
jz_bytes groove__ivm__runtime__index_ids__durable_index_key_prefix(u32 id);
bool groove__ivm__runtime__index_ids__split_durable_index_key(jz_slice key, u32 *id, jz_slice *rest);

/* ---------------- key_encoding.rs ---------------- */
#define GROOVE_ORD_NONE 2 /* Option<Ordering>::None; otherwise -1/0/1 */
typedef enum { groove_FLO_Compared, groove_FLO_SqlNull, groove_FLO_Unsupported } groove_FieldLiteralOrderingTag;
typedef struct groove_FieldLiteralOrdering { u8 tag; i8 ordering; } groove_FieldLiteralOrdering;
/* trait PredicateRecord: Copy -> vtable + by-value context */
typedef struct groove_PredicateRecordVt {
    jz_err (*value)(const void *self, const char *field, u32 len, groove_Value *out);
    jz_err (*literal_ordering)(const void *self, const char *field, u32 len, const groove_LiteralValue *value, groove_FieldLiteralOrdering *out);
} groove_PredicateRecordVt;
typedef struct groove_PredicateRecord { const groove_PredicateRecordVt *vt; const void *self; } groove_PredicateRecord;
extern const groove_PredicateRecordVt groove__ivm__runtime__key_encoding__BorrowedRecord__PredicateRecord__VT;
typedef bool (*groove_OrderingPredicate)(int ordering);
jz_err groove__ivm__runtime__key_encoding__primary_key_value_bytes(groove_RecordDescriptor d, jz_slice record, const u32 *pk, u32 n, jz_bytes *out);
jz_err groove__ivm__runtime__key_encoding__primary_key_field_indices(const groove_TableSchema *table, groove_RecordDescriptor d, groove_U32Vec *out);
jz_err groove__ivm__runtime__key_encoding__encode_record_field_key_part(jz_bytes *key, groove_RecordDescriptor d, jz_slice record, u32 field_idx);
jz_err groove__ivm__runtime__key_encoding__record_field_key_parts(groove_RecordDescriptor d, jz_slice record, u32 field_idx, groove_BytesVec *out);
bool groove__ivm__runtime__key_encoding__expands_to_key_parts(const groove_ValueType *t);
void groove__ivm__runtime__key_encoding__key_parts(groove_Value value, groove_ValueVec *out);
jz_err groove__ivm__runtime__key_encoding__named_field_value(groove_RecordDescriptor d, jz_slice record, u32 field_idx, groove_Value *out);
typedef jz_err (*groove_KeyEncodeFn)(jz_bytes *key, const groove_Value *v, void *ctx);
typedef jz_err (*groove_FieldValueFn)(u32 i, groove_Value *out, void *ctx);
jz_err groove__ivm__runtime__key_encoding__composite_keys(jz_bytes prefix, u32 nfields, groove_FieldValueFn field_value, void *fctx, groove_KeyEncodeFn encode, void *ectx, groove_BytesVec *out);
jz_err groove__ivm__runtime__key_encoding__compare_record_field(groove_PredicateRecord record, const char *field, u32 flen, const groove_LiteralValue *value, groove_OrderingPredicate predicate, groove_ValueComparison comparison, bool *out);
jz_err groove__ivm__runtime__key_encoding__record_field_literal_ordering(const groove_BorrowedRecord *record, u32 field_idx, const groove_LiteralValue *value, groove_FieldLiteralOrdering *out);
groove_FieldLiteralOrdering groove__ivm__runtime__key_encoding__ordering_int(int c);
jz_err groove__ivm__runtime__key_encoding__compare_record_fields(groove_PredicateRecord record, const char *field, u32 flen, const char *value_field, u32 vlen, groove_OrderingPredicate predicate, groove_ValueComparison comparison, bool *out);
jz_err groove__ivm__runtime__key_encoding__contains_record_field(groove_PredicateRecord record, const char *field, u32 flen, const groove_LiteralValue *value, groove_ValueComparison comparison, bool *out);
jz_err groove__ivm__runtime__key_encoding__contains_record_field_value(groove_PredicateRecord record, const char *field, u32 flen, const char *needle, u32 nlen, groove_ValueComparison comparison, bool *out);
int groove__ivm__runtime__key_encoding__compare_values_sql(const groove_Value *left, const groove_Value *right, groove_ValueComparison comparison);
bool groove__ivm__runtime__key_encoding__is_sql_null_value(const groove_Value *v);
bool groove__ivm__runtime__key_encoding__integer_value(const groove_Value *v, __int128 *out);
groove_RecordDescriptor groove__ivm__runtime__key_encoding__join_descriptor(groove_RecordDescriptor left, groove_RecordDescriptor right);
jz_err groove__ivm__runtime__key_encoding__encode_key_part(jz_bytes *key, const groove_Value *v);
jz_err groove__ivm__runtime__key_encoding__encode_scalar_key_part(jz_bytes *key, const groove_Value *v);
u64 groove__ivm__runtime__key_encoding__order_preserving_i64_bits(i64 v);
u32 groove__ivm__runtime__key_encoding__order_preserving_i32_bits(i32 v);
void groove__ivm__runtime__key_encoding__encode_ordered_bytes(jz_bytes *key, const u8 *p, u32 n);
jz_err groove__ivm__runtime__key_encoding__project_binding_source_deltas(const groove_RecordDeltas *input, groove_RecordDescriptor output, groove_RecordDeltas *out);
void groove__ivm__runtime__key_encoding__consolidate_deltas(groove_RecordDeltaVec *deltas);

/* ---------------- terminal.rs ---------------- */
typedef enum { groove_TerminalPathSegment_Collection, groove_TerminalPathSegment_Key } groove_TerminalPathSegmentTag;
typedef struct groove_TerminalPathSegment { u8 tag; jz_str collection; jz_bytes key; } groove_TerminalPathSegment;
typedef JZ_VEC(groove_TerminalPathSegment) groove_TerminalPath;
typedef enum { groove_TerminalEdit_Insert, groove_TerminalEdit_Update, groove_TerminalEdit_Remove, groove_TerminalEdit_Move } groove_TerminalEditTag;
typedef struct groove_TerminalEdit { u8 tag; u32 index; jz_bytes key; jz_bytes value; } groove_TerminalEdit;
typedef struct groove_TerminalOperation { groove_RecordDescriptor root_descriptor; jz_bytes root_key; groove_TerminalPath path; groove_TerminalEdit edit; } groove_TerminalOperation;
typedef JZ_VEC(groove_TerminalOperation) groove_TerminalOperationVec;
typedef struct groove_TerminalDeltas { groove_TerminalOperationVec operations; } groove_TerminalDeltas;
void groove_TerminalOperation_drop(void *op);
void groove_TerminalDeltas_drop(groove_TerminalDeltas *d);
jz_bytes groove__ivm__runtime__terminal__terminal_occurrence_key(jz_bytes row_key, u64 occurrence);
bool groove__ivm__runtime__terminal__TerminalDeltas__is_empty(const groove_TerminalDeltas *d);
jz_err groove__ivm__runtime__terminal__terminal_deltas_from_record_deltas(const groove_RecordDeltas *deltas, groove_TerminalDeltas *out);
jz_err groove__ivm__runtime__terminal__terminal_deltas_keyed_by_identity(const groove_RecordDeltas *deltas, const groove_RecordDeltas *physical, const u32 *key_fields, u32 n, groove_TerminalDeltas *out);
jz_err groove__ivm__runtime__terminal__order_terminal_snapshot(groove_RecordDeltas *terminal, const groove_RecordDeltas *ordering);

/* windows.rs key helpers used by terminal.rs */
jz_err groove__ivm__runtime__windows__encoded_record_key_part(groove_RecordDescriptor d, const u8 *raw, u32 len, const usize *idx, u32 n, jz_bytes *out);
jz_err groove__ivm__runtime__windows__encoded_identity_key_part(groove_RecordDescriptor d, const u8 *raw, u32 len, const usize *idx, u32 n, jz_bytes *out);

/* ---------------- aggregate.rs ---------------- */
/* (Bytes, i64) record/weight pairs share RecordDelta's layout */
void groove_AggregateExpr_clone(groove_AggregateExpr *dst, const groove_AggregateExpr *src);
void groove_AggregateExpr_drop(groove_AggregateExpr *e);
jz_err groove__ivm__runtime__aggregate__resolve_aggregate_expr(groove_RecordDescriptor input, const groove_AggregateExpr *aggregate, groove_AggregateExpr *out);
void groove__ivm__runtime__aggregate__records_before_from_deltas(groove_RecordDeltaVec after_records, groove_RecordDeltaVec deltas, groove_RecordDeltaVec *out);
jz_err groove__ivm__runtime__aggregate__aggregate_row_from_records(groove_RecordDescriptor input_desc, groove_RecordDescriptor output_desc, const groove_AggregateOp *aggregate, const groove_RecordDelta *records, u32 n, bool *some, jz_bytes *out);
/* windows.rs */
void groove__ivm__runtime__windows__records_before_deltas(groove_RecordDeltaVec after_records, const groove_RecordDelta *deltas, u32 n, groove_RecordDeltaVec *out);

/* ---------------- persist.rs ---------------- */
jz_err groove__ivm__runtime__persist__apply_persist_delta(groove_Storage storage, const groove_DurableStorage *durable, const u32 *key_fields, u32 nkeys, bool unique, const groove_RecordDeltas *delta);
jz_err groove__ivm__runtime__persist__apply_persist_delta_with(groove_Storage storage, const groove_DurableStorage *durable, const u32 *key_fields, u32 nkeys, bool unique, const groove_RecordDeltas *delta, groove_WriteOpVec leading);
jz_err groove__ivm__runtime__persist__index_record_from_storage(u32 prefix_len, jz_slice key, jz_bytes value, jz_bytes *out);

/* ---------------- evaluator.rs mirror (unit groove-ivm-rt-a) ---------------- */
typedef struct groove_EvalContext_ext { void *scope; u64 sub_tick; jz_hmap bindings; jz_hmap binding_digests; u8 arrangement_update_mode; u8 eval_mode; bool hydrate_arrangements; } groove_EvalContext_ext;
typedef struct groove_NodeMemoLookup { groove_EvalMemoKey key; u64 input_watermark; bool depends_on_context; } groove_NodeMemoLookup;
typedef struct groove_FrameInputs { const u32 *slots; u32 len; } groove_FrameInputs;
typedef struct groove_TickEvaluator {
    const groove_DatabaseSchema *schema;
    const groove_IvmGraph *graph;
    const jz_hmap *variant_projections;
    const groove_TableDelta *table_deltas; u32 table_deltas_len;
    const groove_BindingDelta *binding_deltas; u32 binding_deltas_len;
    const jz_hmap *binding_snapshots;
    u64 current_tick;
    jz_hmap *operator_states;
    jz_hmap *arrangement_states;
    jz_hmap *arrangement_keys_by_input;
    groove_EvaluationMemo *eval_memo;
    u32 *eval_memo_bytes;
    const jz_hmap *table_frontiers;
    const jz_hmap *binding_frontiers;
    u64 *memo_use_clock;
    jz_hmap *node_meta;
    bool has_storage; groove_Storage storage;
    groove_EvaluationInputs *evaluation_inputs;
    groove_EvalContext context;
    groove_TickMetrics *metrics;
    jz_hmap terminal_deltas;
    jz_hmap root_ordering_windows;
} groove_TickEvaluator;
jz_err groove__ivm__runtime__evaluator__TickEvaluator__input_signature(groove_TickEvaluator *te, groove_NodeId node, groove_NodeInputSignature **out /* Arc */);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__prepare_memo_lookup(groove_TickEvaluator *te, groove_NodeId node, groove_NodeMemoLookup *out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__cached_node_records(groove_TickEvaluator *te, const groove_NodeMemoLookup *lookup, groove_RecordDeltas **out /* NULL = None */);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__resolve_register_inputs(groove_TickEvaluator *te, groove_FrameInputs inputs, bool *some, JZ_VEC(groove_RecordDeltas *) *out);
groove_RecordDeltas *groove__ivm__runtime__evaluator__TickEvaluator__memoize_result(groove_TickEvaluator *te, const groove_NodeMemoLookup *lookup, groove_RecordDeltas result);
/* compute_node returns a boxed future; polled synchronously: returns false while pending */
bool groove__ivm__runtime__evaluator__TickEvaluator__compute_node_poll(groove_TickEvaluator *te, groove_NodeId node, groove_NodeMemoLookup lookup, jz_err *err, groove_RecordDeltas **out);
jz_err groove__ivm__runtime__evaluator__TickEvaluator__raw_projection_fields(groove_TickEvaluator *te, groove_NodeId node, const groove_MapProjectOp *project, groove_RecordDescriptor input, groove_RecordDescriptor output, groove_PreparedProjection **out /* NULL = None */);

/* ---------------- pipeline/fields.rs ---------------- */
typedef enum { groove_Origin_Source, groove_Origin_Constant } groove_OriginTag;
typedef struct groove_RouteField {
    u8 origin_tag;
    JZ_VEC(groove_DescIdx) path;  /* Source */
    jz_bytes *constant;            /* Constant: Arc<[u8]> (jz_rc payload) */
    u32 present_wrappers;
} groove_RouteField;
typedef struct groove_FieldRoutes {
    groove_RecordDescriptor source;
    groove_RecordDescriptor descriptor;
    groove_RouteField *fields; u32 fields_len; /* Arc<[Field]> (jz_rc payload) */
    bool reuses_input;
} groove_FieldRoutes;
typedef struct groove_RoutedRecord { const groove_FieldRoutes *routes; jz_slice raw; } groove_RoutedRecord;
void groove_FieldRoutes_clone(groove_FieldRoutes *dst, const groove_FieldRoutes *src);
void groove_FieldRoutes_drop(groove_FieldRoutes *r);
jz_err groove__ivm__runtime__pipeline__fields__Field__bytes(const groove_RouteField *f, jz_slice raw, jz_slice *out);
void groove__ivm__runtime__pipeline__fields__FieldRoutes__identity(groove_RecordDescriptor descriptor, groove_FieldRoutes *out);
bool groove__ivm__runtime__pipeline__fields__FieldRoutes__compose(const groove_FieldRoutes *self, groove_RecordDescriptor descriptor, const groove_PreparedProjection *projection, groove_FieldRoutes *out);
jz_err groove__ivm__runtime__pipeline__fields__FieldRoutes__append(const groove_FieldRoutes *self, jz_slice raw, jz_bytes *output, u32 *start, u32 *end);
groove_RoutedRecord groove__ivm__runtime__pipeline__fields__FieldRoutes__record(const groove_FieldRoutes *self, jz_slice raw);
extern const groove_PredicateRecordVt groove__ivm__runtime__pipeline__fields__RoutedRecord__PredicateRecord__VT;

/* ---------------- operator_updates.rs: NodeRuntimeMeta ---------------- */
typedef struct groove_PreparedPipeline groove_PreparedPipeline;
typedef struct groove_StrList { u32 len; jz_str *items; } groove_StrList;          /* Arc<[String]> payload */
typedef struct groove_NodeIdList { u32 len; groove_NodeId *items; } groove_NodeIdList; /* Arc<[NodeId]> payload */
typedef struct groove_NodeRuntimeMeta {
    jz_hmap retainers;  /* HashSet<Retainer> */
    u64 last_used_tick;
    i8 depends_on_context;            /* Option<bool>: -1 None */
    i8 has_hydration_state_ancestor;  /* Option<bool> */
    groove_NodeIdList *readiness_frontier;     /* Option<Arc<[NodeId]>> */
    void *terminal_lineage;                    /* Option<Arc<TerminalLineage>> (rt-a) */
    groove_NodeInputSignature *input_signature; /* Option<Arc<..>> */
    u64 input_generation;
    bool has_raw_projection_fields; groove_PreparedProjection *raw_projection_fields; /* Option<Option<Arc<..>>> */
    groove_PreparedPipeline *pipeline;  /* Option<Arc<PreparedPipeline>> */
    groove_StrList *join_left_fields, *join_right_fields;
    groove_PreparedRecordCopy *join_output;
    groove_StrList *aggregate_group_fields;
} groove_NodeRuntimeMeta;
void groove__ivm__runtime__operator_updates__NodeRuntimeMeta__default(groove_NodeRuntimeMeta *m);
void groove_NodeRuntimeMeta_drop(void *m);
void groove_NodeRuntimeMeta_clone(void *dst, const void *src);
extern const jz_hmap_desc groove_ivm_rt_NODE_META_MAP; /* HashMap<NodeId, NodeRuntimeMeta> */
groove_NodeRuntimeMeta *groove_ivm_rt_node_meta_entry(jz_hmap *node_meta, groove_NodeId node); /* entry().or_default() */
jz_err groove__ivm__runtime__operator_updates__NodeState__project_row_into(const groove_MapProjectOp *project, groove_RecordDescriptor output_desc, groove_RecordDescriptor input_desc,
    jz_slice raw, const groove_PreparedProjection *raw_projection, bool omit_unrepresentable_enum_rows, jz_bytes *output, bool *some, u32 *start, u32 *end);

/* ---------------- pipeline.rs ---------------- */
typedef struct groove_PendingPipeline groove_PendingPipeline;
bool groove__ivm__runtime__pipeline__supports_node(const groove_IvmGraph *graph, groove_NodeId id);
void groove_PendingPipeline_drop_v(void *p);
extern const jz_hmap_desc groove_ivm_rt_PENDING_PIPELINE_MAP; /* HashMap<NodeId, PendingPipeline> */
/* Poll<Result<Arc<RecordDeltas>>>: returns false while pending */
bool groove__ivm__runtime__pipeline__TickEvaluator__poll_pipeline(groove_TickEvaluator *te, const groove_NodeId *nodes, u32 nnodes, jz_hmap *pending,
    groove_FrameInputs frame_inputs, jz_err *err, groove_RecordDeltas **out);

/* mod.rs */
jz_bytes groove__ivm__runtime__freeze_batch_buffer(jz_bytes buffer);

/* ---------------- evaluation_session.rs ---------------- */
typedef enum {
    groove_SRK_Get, groove_SRK_ScanRange, groove_SRK_ScanPrefix, groove_SRK_ScanPrefixLimit,
    groove_SRK_IndexedRowsPrefix, groove_SRK_IndexedRowsPrefixLimit, groove_SRK_IndexedRowsIntersection,
    groove_SRK_IndexedRowsCandidateFilter, groove_SRK_IndexedRowsRange,
} groove_StorageRequestKeyTag;
typedef struct groove_IndexPrefixPair { jz_str index; jz_bytes prefix; } groove_IndexPrefixPair;
typedef struct groove_StorageRequestKey {
    u8 tag;
    jz_str family;   /* family, or table for IndexedRows* */
    jz_str index;
    jz_bytes a;      /* key / start / prefix */
    jz_bytes b;      /* end */
    u32 max_items; bool reversed;
    JZ_VEC(groove_IndexPrefixPair) intersections;
    jz_str candidate_table, candidate_index; jz_bytes candidate_prefix;
    jz_str source_column, candidate_column;
} groove_StorageRequestKey;
void groove_StorageRequestKey_clone(groove_StorageRequestKey *dst, const groove_StorageRequestKey *src);
void groove_StorageRequestKey_drop(groove_StorageRequestKey *k);
int groove_StorageRequestKey_cmp(const groove_StorageRequestKey *a, const groove_StorageRequestKey *b);
typedef enum { groove_ERK_Storage, groove_ERK_Chunk } groove_EvaluationRequestKeyTag;
typedef struct groove_EvaluationRequestKey { u8 tag; groove_StorageRequestKey storage; groove_ChunkRequest chunk; } groove_EvaluationRequestKey;
typedef enum { groove_SRO_Value, groove_SRO_Rows } groove_StorageRequestOutputTag;
typedef enum { groove_LCB_Direct, groove_LCB_Leased } groove_LoadedChunkBytesTag;
typedef struct groove_LoadedChunk { u8 tag; jz_bytes direct; groove_ChunkLease lease; } groove_LoadedChunk;
typedef enum { groove_ERO_Storage, groove_ERO_Chunk } groove_EvaluationRequestOutputTag;
typedef struct groove_EvaluationRequestOutput {
    u8 tag; u8 storage_tag;
    bool has_value; jz_bytes value; /* Value(Option<Vec<u8>>) */
    groove_KvPairVec rows;          /* Rows */
    groove_LoadedChunk chunk;
} groove_EvaluationRequestOutput;
const jz_bytes *groove__ivm__runtime__evaluation_session__LoadedChunk__bytes(const groove_LoadedChunk *c);
bool groove__ivm__runtime__evaluation_session__LoadedChunk__verifies_object_hash(const groove_LoadedChunk *c, const u8 expected[32]);
struct groove_EvaluationInputs {
    jz_btree loaded;       /* EvaluationRequestKey -> EvaluationRequestOutput */
    jz_btree missing;      /* BTreeSet<EvaluationRequestKey> */
    jz_btree chunk_owners; /* EvaluationRequestKey -> BTreeSet<NodeId> */
    bool has_chunk_scope; groove_NodeId chunk_scope;
};
typedef JZ_VEC(groove_ChunkRequest) groove_ChunkRequestVec;
void groove__ivm__runtime__evaluation_session__EvaluationInputs__default(groove_EvaluationInputs *in);
void groove_EvaluationInputs_drop(groove_EvaluationInputs *in);
void groove__ivm__runtime__evaluation_session__EvaluationInputs__set_chunk_scope(groove_EvaluationInputs *in, bool has, groove_NodeId scope);
void groove__ivm__runtime__evaluation_session__EvaluationInputs__release_chunks_owned_by(groove_EvaluationInputs *in, groove_NodeId owner);
void groove__ivm__runtime__evaluation_session__EvaluationInputs__take_missing_chunks(groove_EvaluationInputs *in, groove_ChunkRequestVec *out);
void groove__ivm__runtime__evaluation_session__EvaluationInputs__install_chunk(groove_EvaluationInputs *in, groove_ChunkRequest request, jz_bytes bytes);
void groove__ivm__runtime__evaluation_session__EvaluationInputs__install_chunk_from_provider(groove_EvaluationInputs *in, groove_ChunkRequest request, groove_ChunkLease bytes);
jz_err groove__ivm__runtime__evaluation_session__EvaluationInputs__chunk(groove_EvaluationInputs *in, groove_ChunkRequest request, const jz_bytes **out);
jz_err groove__ivm__runtime__evaluation_session__EvaluationInputs__loaded_chunk(groove_EvaluationInputs *in, groove_ChunkRequest request, const groove_LoadedChunk **out);
jz_err groove__ivm__runtime__evaluation_session__EvaluationInputs__rows(groove_EvaluationInputs *in, groove_StorageRequestKey key, const groove_KvPairVec **out);
void groove__ivm__runtime__evaluation_session__EvaluationInputs__install(groove_EvaluationInputs *in, jz_btree *ready);
void groove__ivm__runtime__evaluation_session__EvaluationInputs__take_missing(groove_EvaluationInputs *in, jz_btree *out);
void groove__ivm__runtime__evaluation_session__EvaluationInputs__hand_off_chunks(groove_EvaluationInputs *in, groove_EvaluationInputs *successor);
typedef struct groove_EvaluationRequestFailure { jz_err error; bool publication_metadata_durability; } groove_EvaluationRequestFailure;
typedef struct groove_EvaluationRequests {
    jz_btree pending; /* EvaluationRequestKey -> PendingRequest */
    jz_btree ready;   /* EvaluationRequestKey -> Result<Output, Failure> */
} groove_EvaluationRequests;
void groove__ivm__runtime__evaluation_session__EvaluationRequests__new(groove_EvaluationRequests *r);
void groove_EvaluationRequests_drop(groove_EvaluationRequests *r);
bool groove__ivm__runtime__evaluation_session__EvaluationRequests__request(groove_EvaluationRequests *r, groove_EvaluationRequestKey key,
    groove_Storage storage, const groove_OwnedChunkProvider *chunks, const groove_DatabaseSchema *schema);
u32 groove__ivm__runtime__evaluation_session__EvaluationRequests__poll(groove_EvaluationRequests *r);
u32 groove__ivm__runtime__evaluation_session__EvaluationRequests__poll_eager_retry(groove_EvaluationRequests *r);
bool groove__ivm__runtime__evaluation_session__EvaluationRequests__has_pending(const groove_EvaluationRequests *r);
void groove__ivm__runtime__evaluation_session__EvaluationRequests__hand_off_chunks(groove_EvaluationRequests *r, groove_EvaluationRequests *successor);
bool groove__ivm__runtime__evaluation_session__EvaluationRequests__has_pending_chunk(const groove_EvaluationRequests *r);
/* Err: *failed_key / *failure set and false returned */
bool groove__ivm__runtime__evaluation_session__EvaluationRequests__drain_ready(groove_EvaluationRequests *r, jz_btree *out,
    groove_EvaluationRequestKey *failed_key, groove_EvaluationRequestFailure *failure);
extern const jz_btree_desc groove_ivm_rt_REQUEST_OUTPUT_MAP; /* BTreeMap<EvaluationRequestKey, EvaluationRequestOutput> */
extern const jz_btree_desc groove_ivm_rt_REQUEST_KEY_SET;

/* ---------------- mod.rs: variant projections ---------------- */
typedef struct groove_VariantProjectionKey { jz_str table; groove_VariantProjectionTarget target; } groove_VariantProjectionKey;
typedef enum { groove_VPC_Project, groove_VPC_Ignore, groove_VPC_Enum } groove_VariantProjectionCaseTag;
typedef struct groove_VariantProjectionCase {
    u8 tag;
    groove_RecordDescriptor source;
    groove_MapProjectOp project;                 /* Project / Enum */
    groove_PreparedProjection *raw_projection;   /* Option<Arc<..>> */
    bool omit_unrepresentable_enum_rows;         /* Project */
    u32 enum_tag; groove_RecordDescriptor payload; /* Enum */
} groove_VariantProjectionCase;
typedef struct groove_VariantProjection { groove_RecordDescriptor output; jz_hmap cases; /* u32 -> VariantProjectionCase */ } groove_VariantProjection;
extern const jz_hmap_desc groove_ivm_rt_VARIANT_PROJECTIONS_MAP; /* VariantProjectionKey -> VariantProjection */
extern const jz_hmap_desc groove_ivm_rt_VARIANT_CASES_MAP;       /* u32 -> VariantProjectionCase */
groove_RecordDescriptor groove__ivm__runtime__VariantProjectionCase__source(const groove_VariantProjectionCase *c);
bool groove__ivm__runtime__VariantProjectionCase__can_refresh_registries_to(const groove_VariantProjectionCase *c, const groove_VariantProjectionCase *next);
void groove_VariantProjectionCase_clone(groove_VariantProjectionCase *dst, const groove_VariantProjectionCase *src);
void groove_VariantProjectionCase_drop(groove_VariantProjectionCase *c);
bool groove_VariantProjectionCase_eq(const groove_VariantProjectionCase *a, const groove_VariantProjectionCase *b);
void groove_VariantProjection_clone(groove_VariantProjection *dst, const groove_VariantProjection *src);
void groove_VariantProjection_drop(groove_VariantProjection *p);

/* ---------------- operator_updates.rs: NodeState ---------------- */
typedef JZ_VEC(groove_RecordDelta) groove_RecordDeltaVecB;
jz_err groove__ivm__runtime__operator_updates__NodeState__table_source_request(const groove_TableSourceOp *input, bool *some, groove_StorageRequestKey *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__index_source_request(const groove_IndexSourceOp *input, bool *some, groove_StorageRequestKey *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__group_source_rows(const groove_TableSchema *t, const groove_KvPairVec *rows, groove_TableDeltaVec *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_table_source_from_inputs(const groove_TableSourceOp *op, const groove_DatabaseSchema *s, const jz_hmap *vp, groove_RecordDescriptor out_desc, groove_EvaluationInputs *in, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_index_source_from_inputs(const groove_IndexSourceOp *op, const groove_DatabaseSchema *s, const jz_hmap *vp, groove_RecordDescriptor out_desc, groove_EvaluationInputs *in, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_table_source(const groove_TableSourceOp *op, const groove_DatabaseSchema *s, const jz_hmap *vp, groove_RecordDescriptor out_desc, const groove_TableDelta *td, u32 ntd, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_index_source(const groove_IndexSourceOp *op, const groove_DatabaseSchema *s, const jz_hmap *vp, groove_RecordDescriptor out_desc, const groove_TableDelta *td, u32 ntd, const groove_Storage *storage, u32 eval_mode, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_binding_source(const groove_BindingSourceOp *op, groove_RecordDescriptor out_desc, const groove_BindingDelta *bd, u32 nbd, const jz_hmap *snapshots, u32 mode, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_filter(const groove_FilterOp *f, groove_RecordDescriptor out_desc, const groove_RecordDeltas *input, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_map_project(const groove_MapProjectOp *p, groove_RecordDescriptor out_desc, const groove_RecordDeltas *input, const groove_PreparedProjection *raw, bool omit, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_map_project_slice(const groove_MapProjectOp *p, groove_RecordDescriptor out_desc, groove_RecordDescriptor in_desc, const groove_RecordDelta *input, u32 n, const groove_PreparedProjection *raw, bool omit, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_variant_enum_project(u32 tag, groove_RecordDescriptor payload_desc, const groove_MapProjectOp *p, groove_RecordDescriptor out_desc, const groove_RecordDeltas *input, const groove_PreparedProjection *raw, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_variant_project(const groove_VariantProjectOp *op, groove_RecordDescriptor out_desc, const groove_RecordDeltas *input, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_unwrap_nullable(const groove_UnwrapNullableOp *op, groove_RecordDescriptor out_desc, const groove_RecordDeltas *input, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_unnest(const groove_UnnestOp *op, groove_RecordDescriptor out_desc, const groove_RecordDeltas *input, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_union(groove_RecordDescriptor out_desc, groove_RecordDeltas **inputs, u32 n, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_index_by(const groove_IndexByOp *op, groove_RecordDescriptor out_desc, const groove_RecordDeltas *input, groove_RecordDeltas *out);

/* ---------------- join.rs (types used across files) ---------------- */
typedef struct groove_ArrangementState groove_ArrangementState;  /* heap */
typedef struct groove_AsOfArrangement { groove_ArrangementState *value; groove_AsOfStamp as_of; } groove_AsOfArrangement;
void groove__ivm__runtime__join__ArrangementState__drop(groove_ArrangementState *s);

/* ---------------- mod.rs: IvmRuntime ---------------- */
extern const jz_hmap_desc groove_ivm_rt_U64_SET;
extern const jz_hmap_desc groove_ivm_rt_U32_DESC_MAP;
extern const jz_hmap_desc groove_ivm_rt_STR_VARIANT_DESCS_MAP;
extern const jz_hmap_desc groove_ivm_rt_OPERATOR_STATES_MAP;
extern const jz_hmap_desc groove_ivm_rt_ARRANGEMENT_STATES_MAP;
extern const jz_hmap_desc groove_ivm_rt_ARRANGEMENT_KEY_SET;
extern const jz_hmap_desc groove_ivm_rt_NODE_KEYSET_MAP;
extern const jz_hmap_desc groove_ivm_rt_EVAL_MEMO_KEY_SET;
jz_err groove__ivm__runtime__IvmRuntimeError__route_arity(const jz_str *sink, u64 expected, u64 actual);
void groove__ivm__runtime__IvmRuntime__insert_retained_eval_memo(groove_IvmRuntime *self, groove_EvalMemoKey key, groove_EvalMemoEntry entry);
void groove__ivm__runtime__IvmRuntime__remove_retained_eval_memo(groove_IvmRuntime *self, const groove_EvalMemoKey *key);
void groove__ivm__runtime__IvmRuntime__remove_retained_eval_memos_for_nodes(groove_IvmRuntime *self, const jz_hmap *nodes);
jz_err groove__ivm__runtime__IvmRuntime__new(groove_DatabaseSchema schema, groove_IvmRuntime **out);
jz_err groove__ivm__runtime__IvmRuntime__new_with_index_ids(groove_DatabaseSchema schema, groove_SharedIndexIds index_ids, groove_IvmRuntime **out);
void groove_IvmRuntime_drop(groove_IvmRuntime *rt);
groove_InputSourceId groove__ivm__runtime__IvmRuntime__allocate_input_source(groove_IvmRuntime *self, groove_RecordDescriptor descriptor);
const groove_RecordDescriptor *groove__ivm__runtime__IvmRuntime__table_descriptor(const groove_IvmRuntime *self, const char *table, u32 len);
const groove_RecordDescriptor *groove__ivm__runtime__IvmRuntime__table_storage_descriptor(const groove_IvmRuntime *self, const char *table, u32 len);
const groove_RecordDescriptor *groove__ivm__runtime__IvmRuntime__record_descriptor(const groove_IvmRuntime *self, const char *table, u32 len, u32 variant_tag);
u32 groove__ivm__runtime__IvmRuntime__index_storage_id(const groove_IvmRuntime *self, const char *table, u32 len, const groove_IndexSchema *index);
/* schema.rs */
jz_err groove__ivm__runtime__schema__IvmRuntime__define_schema_index_variant_projections(groove_IvmRuntime *self);
jz_err groove__ivm__runtime__subscriptions__IvmRuntime__add_dedup_schema_indices(groove_IvmRuntime *self);

/* ---------------- join.rs ---------------- */
typedef JZ_VEC(groove_UsizePair) groove_UsizePairVec;
typedef struct groove_ThresholdJoinState groove_ThresholdJoinState;
typedef groove_ThresholdJoinState groove_SemiJoinState;  /* ThresholdJoinState<true> */
typedef groove_ThresholdJoinState groove_AntiJoinState;  /* ThresholdJoinState<false> */
typedef struct groove_ArrangementTransition { const groove_AsOfArrangement *state; groove_SubTick expected; } groove_ArrangementTransition;
typedef struct groove_JoinInput { const groove_RecordDelta *records; u32 n; const groove_RecordDeltas *snapshot; /* Option<&Arc> */ } groove_JoinInput;
typedef struct groove_RecordWeight { jz_bytes record; i64 weight; } groove_RecordWeight;
typedef JZ_VEC(groove_RecordWeight) groove_RecordWeightVec;
groove_ArrangementState *groove__ivm__runtime__join__ArrangementState__default(void);
groove_ArrangementState *groove__ivm__runtime__join__ArrangementState__clone(const groove_ArrangementState *s);
groove_SemiJoinState *groove__ivm__runtime__join__SemiJoinState__default(void);
groove_SemiJoinState *groove__ivm__runtime__join__SemiJoinState__clone(const groove_SemiJoinState *s);
void groove__ivm__runtime__join__SemiJoinState__drop(groove_SemiJoinState *s);
groove_AntiJoinState *groove__ivm__runtime__join__AntiJoinState__default(void);
groove_AntiJoinState *groove__ivm__runtime__join__AntiJoinState__clone(const groove_AntiJoinState *s);
void groove__ivm__runtime__join__AntiJoinState__drop(groove_AntiJoinState *s);
groove_ArrangementTransition groove__ivm__runtime__join__ArrangementTransition__at(const groove_AsOfArrangement *state, groove_SubTick expected);
jz_err groove__ivm__runtime__join__touched_join_keys(groove_RecordDescriptor d, const jz_str *fields, u32 nfields, const groove_RecordDelta *deltas, u32 n, groove_ValueComparison comparison, groove_BytesVec *out);
jz_err groove__ivm__runtime__join__JoinState__evaluate_prepared(groove_ArrangementTransition left, groove_ArrangementTransition right, const groove_PreparedRecordCopy *projection, u32 mode, groove_RecordDeltaVec *out);
jz_err groove__ivm__runtime__join__threshold_snapshot(const groove_RecordDeltas *left, const groove_RecordDeltas *right, const jz_str *left_keys, u32 nl, const jz_str *right_keys, u32 nr, groove_ValueComparison comparison, bool semi, groove_RecordDeltaVec *out);
void groove__ivm__runtime__join__ThresholdJoinState__commit_published_overlay(groove_ThresholdJoinState *s);
void groove__ivm__runtime__join__SemiJoinState__evaluate(groove_SemiJoinState *s, groove_ArrangementTransition l, groove_ArrangementTransition r, u32 mode, groove_RecordDeltaVec *out);
void groove__ivm__runtime__join__AntiJoinState__evaluate(groove_AntiJoinState *s, groove_ArrangementTransition l, groove_ArrangementTransition r, u32 mode, groove_RecordDeltaVec *out);
void groove__ivm__runtime__join__ArrangementState__release_changes(groove_ArrangementState *s);
void groove__ivm__runtime__join__ArrangementState__commit_overlay(groove_ArrangementState *s);
usize groove__ivm__runtime__join__ArrangementState__row_count(const groove_ArrangementState *s);
usize groove__ivm__runtime__join__ArrangementState__encoded_bytes(const groove_ArrangementState *s);
jz_err groove__ivm__runtime__join__ArrangementState__apply_record_deltas(groove_ArrangementState *s, groove_RecordDescriptor d, const jz_str *fields, u32 nfields, const groove_RecordDelta *deltas, u32 n, u32 mode);
void groove__ivm__runtime__join__ArrangementState__records_for_key(const groove_ArrangementState *s, const u8 *key, u32 len, groove_RecordWeightVec *out);
jz_err groove__ivm__runtime__join__prepare_arrangement(groove_AsOfArrangement *state, groove_RecordDescriptor d, const jz_str *fields, u32 nfields, u32 comparison, groove_JoinInput input, groove_SubTick stamp, u32 mode);
jz_err groove__ivm__runtime__join__join_keys(groove_RecordDescriptor d, jz_slice record, const jz_str *fields, u32 nfields, groove_ValueComparison comparison, groove_BytesVec *out);
jz_err groove__ivm__runtime__join__create_join_record(groove_RecordDescriptor l, jz_slice lr, groove_RecordDescriptor r, jz_slice rr, groove_RecordDescriptor o, jz_bytes *out);
jz_err groove__ivm__runtime__join__join_output_mapping(groove_RecordDescriptor l, groove_RecordDescriptor r, groove_RecordDescriptor o, groove_UsizePairVec *out);

/* ---------------- windows.rs ---------------- */
typedef enum {
    groove_TBK_Null, groove_TBK_U8, groove_TBK_U16, groove_TBK_U32, groove_TBK_U64, groove_TBK_U48, groove_TBK_I32,
    groove_TBK_I64, groove_TBK_F64, groove_TBK_Bool, groove_TBK_String, groove_TBK_Bytes, groove_TBK_Uuid, groove_TBK_EnumTag,
} groove_TopBySortKeyTag;
typedef struct groove_TopBySortPart {
    u32 key_tag;
    union { u64 u; i64 i; bool b; jz_str s; jz_bytes bytes; u8 uuid[16]; } key;
    u32 direction;
} groove_TopBySortPart;
typedef JZ_VEC(groove_TopBySortPart) groove_TopBySortPartVec;
int groove__ivm__runtime__windows__TopBySortPart__Ord__cmp(const groove_TopBySortPart *a, const groove_TopBySortPart *b);
void groove__ivm__runtime__windows__TopBySortPart__clone(groove_TopBySortPart *dst, const groove_TopBySortPart *src);
void groove__ivm__runtime__windows__TopBySortPart__drop(groove_TopBySortPart *p);
enum { GROOVE_ARGBY_MIN, GROOVE_ARGBY_MAX };
typedef struct groove_ArgBySpec { const usize *group_field_indices; u32 n_group; const usize *comparison_field_indices; u32 n_comparison; u32 direction; } groove_ArgBySpec;
typedef struct groove_WindowedRecord { jz_bytes record; i64 weight; } groove_WindowedRecord;
typedef JZ_VEC(groove_WindowedRecord) groove_WindowedRecordVec;

/* evaluator.rs (unit groove-ivm-rt-a) mirror: ordered CollectBy groups */
typedef struct groove_CollectByOrderKey { groove_TopBySortPartVec parts; jz_bytes record; } groove_CollectByOrderKey;
typedef JZ_VEC(groove_CollectByOrderKey) groove_CollectByOrderKeyVec;
int groove__ivm__runtime__evaluator__CollectByOrderKey__cmp(const void *a, const void *b, void *ctx);
void groove__ivm__runtime__evaluator__CollectByOrderKey__clone(void *dst, const void *src);
void groove__ivm__runtime__evaluator__CollectByOrderKey__drop(void *k);
bool groove__ivm__runtime__evaluator__CollectByOrderKey__eq(const groove_CollectByOrderKey *a, const groove_CollectByOrderKey *b);
extern const jz_btree_desc groove_ivm_rt_a_desc_order_key_i64;
typedef struct groove_CollectByGroup { groove_CountedMap *base; jz_btree *overlay; } groove_CollectByGroup;
typedef struct groove_SparseGroups { jz_btree *base; jz_btree *overlay; } groove_SparseGroups;
const i64 *groove__ivm__runtime__evaluator__CollectByGroup__get(const groove_CollectByGroup *g, const groove_CollectByOrderKey *key);
void groove__ivm__runtime__evaluator__CollectByGroup__set(groove_CollectByGroup *g, groove_CollectByOrderKey key, i64 weight);
usize groove__ivm__runtime__evaluator__CollectByGroup__count_before(const groove_CollectByGroup *g, const groove_CollectByOrderKey *key);
typedef struct groove_CollectByGroupIter { groove_CountedMapIter base; bool base_peeked; const void *bk; const i64 *bw; jz_btree_iter overlay; bool ov_peeked; void *ok; void *ov; } groove_CollectByGroupIter;
void groove__ivm__runtime__evaluator__CollectByGroup__iter(const groove_CollectByGroup *g, groove_CollectByGroupIter *it);
bool groove__ivm__runtime__evaluator__CollectByGroupIter__Iterator__next(groove_CollectByGroupIter *it, const groove_CollectByOrderKey **key, const i64 **weight);
const groove_CollectByGroup *groove__ivm__runtime__evaluator__SparseGroups__get(const groove_SparseGroups *s, const u8 *key, u32 len);
groove_CollectByGroup *groove__ivm__runtime__evaluator__SparseGroups__get_or_default(groove_SparseGroups *s, jz_bytes key);
void groove__ivm__runtime__evaluator__SparseGroups__remove_empty_touched_groups(groove_SparseGroups *s, jz_bytes *keys, u32 n);
void groove__ivm__runtime__evaluator__SparseGroups__clear(groove_SparseGroups *s);
typedef JZ_VEC(usize) groove_UsizeVecB;
void groove__ivm__runtime__evaluator__SparseGroups__count_before_each(const groove_SparseGroups *s, const jz_slice *keys, u32 n, groove_UsizeVecB *out);
typedef struct groove_CollectByIncrementalPayload {
    groove_SparseGroups groups;
    jz_btree roots;                       /* BTreeMap<CollectByOrderKey, i64> */
    groove_RankIndex *emitted_root_order; /* Rc<RankIndex<CollectByOrderKey, Vec<u8>>> */
    groove_RankIndex *emitted_root_keys;  /* Rc<RankIndex<Vec<u8>, ()>> */
} groove_CollectByIncrementalPayload;
typedef struct groove_CollectByIncrementalState { groove_CollectByIncrementalPayload *payload; /* Rc */ } groove_CollectByIncrementalState;
groove_CollectByIncrementalPayload *groove__ivm__runtime__evaluator__CollectByIncrementalState__deref_mut(groove_CollectByIncrementalState *s);
groove_RankIndex *groove__ivm__runtime__rank_index__RankIndex__new_order(void);
groove_RankIndex *groove__ivm__runtime__rank_index__RankIndex__new_keys(void);

bool groove__ivm__runtime__windows__arg_by_candidate_replaces(jz_slice candidate_key, jz_slice candidate_record, jz_slice winner_key, jz_slice winner_record, u32 direction);
jz_err groove__ivm__runtime__windows__arg_by_order_key(groove_RecordDescriptor d, const groove_RecordDelta *delta, const groove_ArgBySpec *spec, groove_CollectByOrderKey *out);
bool groove__ivm__runtime__windows__arg_by_ordered_winner(const groove_CollectByGroup *g, jz_bytes *out);
void groove__ivm__runtime__windows__top_by_window_from_ordered_group(const groove_CollectByGroup *g, const groove_TopByOp *top_by, groove_WindowedRecordVec *out);
void groove__ivm__runtime__windows__collect_by_output_value(const groove_ValueType *output_type, groove_Value value, groove_Value *out);
jz_err groove__ivm__runtime__windows__update_unbounded_collect_by_terminal_state(groove_RecordDescriptor in_desc, groove_RecordDescriptor out_desc, const groove_CollectByOp *cb,
    const groove_CollectBySlot *direct_tree_slot, groove_CollectByIncrementalState *state, const groove_RecordDelta *d, u32 n, bool emit, groove_TerminalOperationVec *out);
jz_err groove__ivm__runtime__windows__collect_by_root_from_records(groove_RecordDescriptor in_desc, groove_RecordDescriptor out_desc, const groove_CollectByOp *cb, const groove_RecordWeight *r, u32 n, bool *some, jz_bytes *out);
jz_err groove__ivm__runtime__windows__collect_by_parent_from_records(groove_RecordDescriptor in_desc, groove_RecordDescriptor out_desc, const groove_CollectByOp *cb, const groove_RecordWeight *r, u32 n, bool *some, jz_bytes *out);
jz_err groove__ivm__runtime__windows__collect_by_tree_parent_from_records(groove_RecordDescriptor in_desc, groove_RecordDescriptor out_desc, const groove_CollectByOp *cb, const groove_RecordWeight *r, u32 n, bool *some, jz_bytes *out);
jz_err groove__ivm__runtime__windows__collect_by_expanded_window(groove_RecordDescriptor in_desc, groove_RecordDescriptor out_desc, const groove_CollectByOp *cb, const groove_RecordWeight *r, u32 n, jz_btree *out);
jz_err groove__ivm__runtime__windows__top_by_sort_key(groove_RecordDescriptor d, const u8 *raw, u32 len, const groove_TopByOp *top_by, groove_TopBySortPartVec *out);
void groove__ivm__runtime__windows__diff_record_windows(groove_WindowedRecordVec before, groove_WindowedRecordVec after, groove_RecordDeltaVec *out);
jz_err groove__ivm__runtime__windows__update_unbounded_top_by_group_touching(groove_RecordDescriptor d, const groove_TopByOp *top_by, groove_CollectByGroup *group,
    const groove_RecordDelta *input, u32 n, groove_CollectByOrderKeyVec *touched /* nullable */, groove_RecordDeltaVec *out);
jz_err groove__ivm__runtime__windows__encoded_arrangement_key_part(groove_RecordDescriptor d, const u8 *raw, u32 len, const usize *idx, u32 n, jz_bytes *out);


/* ---------------- recursion.rs ---------------- */
extern const jz_hmap_desc groove_ivm_rt_BYTES_I64_HMAP; /* HashMap<Bytes, i64> */
typedef struct groove_RecursiveState groove_RecursiveState; /* heap */
typedef struct groove_RecursiveNodes { groove_NodeId seed, step; bool has_step_witness; groove_NodeId step_witness; } groove_RecursiveNodes;
enum { GROOVE_HRP_YIELD, GROOVE_HRP_READY_FOR_ARRANGEMENT_HYDRATION };
enum { GROOVE_RDP_YIELD, GROOVE_RDP_READY };
typedef struct groove_HydrationRecomputeContext {
    const groove_DatabaseSchema *schema; const groove_IvmGraph *graph; const jz_hmap *variant_projections;
    groove_EvaluationInputs *inputs; const groove_TableDelta *table_deltas; u32 n_table_deltas; bool has_table_deltas;
    const groove_Storage *storage; const jz_hmap *binding_snapshots; groove_ScopeId scope; u64 input_generation;
} groove_HydrationRecomputeContext;
/* evaluator.rs GraphRuntimeView mirror (unit groove-ivm-rt-a) */
typedef struct groove_GraphRuntimeView {
    const groove_DatabaseSchema *schema;
    const groove_IvmGraph *graph;
    const jz_hmap *variant_projections;
    const groove_TableDelta *table_deltas; u32 n_table_deltas;
    const groove_BindingDelta *binding_deltas; u32 n_binding_deltas;
    const jz_hmap *binding_snapshots;
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
    const groove_Storage *storage;
    groove_EvaluationInputs *evaluation_inputs;
    groove_ScopeId scope;
    groove_TickMetrics *metrics;
} groove_GraphRuntimeView;
jz_err groove__ivm__runtime__evaluator__GraphRuntimeView__eval_with_binding(groove_GraphRuntimeView *v, u64 sub_tick, groove_FrontierName binding, groove_RecordDeltas deltas, groove_NodeId node, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__evaluator__GraphRuntimeView__eval_with_binding_and_table_deltas(groove_GraphRuntimeView *v, const groove_TableDelta *td, u32 ntd, u64 sub_tick, groove_FrontierName binding, groove_RecordDeltas deltas, groove_NodeId node, groove_RecordDeltas *out);
void groove__ivm__runtime__evaluator__GraphRuntimeView__clear_operator_state_for_scope(groove_GraphRuntimeView *v);
jz_err groove__ivm__runtime__evaluator__GraphRuntimeView__eval_root(groove_GraphRuntimeView *v, groove_NodeId node, groove_RecordDeltas *out);
void groove__ivm__runtime__evaluator__plan_expr_names(const groove_PlanExpr *exprs, u32 n, groove_StrVec *out);
/* subscriptions.rs mirror */
groove_RecordStore groove__ivm__runtime__subscriptions__record_store_for_table(const groove_Storage *storage, const groove_TableSchema *t, const groove_RecordDescriptor *d);

groove_RecursiveState *groove__ivm__runtime__recursion__RecursiveState__default(void);
groove_RecursiveState *groove__ivm__runtime__recursion__RecursiveState__clone(const groove_RecursiveState *s);
void groove__ivm__runtime__recursion__RecursiveState__drop(groove_RecursiveState *s);
bool groove__ivm__runtime__recursion__RecursiveState__is_empty(const groove_RecursiveState *s);
bool groove__ivm__runtime__recursion__RecursiveState__step_arrangements_hydrated(const groove_RecursiveState *s);
usize groove__ivm__runtime__recursion__RecursiveState__accumulated_row_count(const groove_RecursiveState *s);
usize groove__ivm__runtime__recursion__RecursiveState__accumulated_encoded_bytes(const groove_RecursiveState *s);
void groove__ivm__runtime__recursion__RecursiveState__mark_step_arrangements_hydrated(groove_RecursiveState *s);
bool groove__ivm__runtime__recursion__RecursiveState__hydrated_input_generation(const groove_RecursiveState *s, u64 *out);
void groove__ivm__runtime__recursion__RecursiveState__mark_hydrated_input_generation(groove_RecursiveState *s, u64 g);
bool groove__ivm__runtime__recursion__RecursiveState__has_pending_hydration(const groove_RecursiveState *s);
bool groove__ivm__runtime__recursion__RecursiveState__has_pending_hydration_for(const groove_RecursiveState *s, u64 g);
void groove__ivm__runtime__recursion__RecursiveState__pending_hydration_accumulated_deltas(const groove_RecursiveState *s, groove_RecordDescriptor d, groove_RecordDeltas *out);
void groove__ivm__runtime__recursion__RecursiveState__finish_hydration_recompute(groove_RecursiveState *s, jz_hmap *next, jz_hmap *step_witness);
void groove__ivm__runtime__recursion__RecursiveState__accumulated_deltas(const groove_RecursiveState *s, groove_RecordDeltaVec *out);
void groove__ivm__runtime__recursion__RecursiveState__step_witness_deltas(const groove_RecursiveState *s, groove_RecordDeltaVec *out);
void groove__ivm__runtime__recursion__RecursiveState__step_witness_accumulated_deltas(const groove_RecursiveState *s, groove_RecordDeltaVec *out);
void groove__ivm__runtime__recursion__RecursiveState__replace_step_witness_with(groove_RecursiveState *s, jz_hmap next);
void groove__ivm__runtime__recursion__RecursiveState__commit_staged_positive(groove_RecursiveState *s);
void groove__ivm__runtime__recursion__RecursiveState__replace_with(groove_RecursiveState *s, jz_hmap next, groove_RecordDeltaVec *out /* nullable */);
jz_err groove__ivm__runtime__recursion__resume_inputs_hydration_recompute(groove_RecursiveState *s, groove_HydrationRecomputeContext ctx, groove_NodeId node, const groove_RecursiveOp *rec, groove_RecordDescriptor out_desc, groove_RecursiveNodes nodes, u32 *progress);
jz_err groove__ivm__runtime__recursion__recursive_delta(groove_RecursiveState *s, groove_GraphRuntimeView *runtime, groove_NodeId node, const groove_RecursiveOp *rec, groove_RecordDescriptor out_desc, groove_RecursiveNodes nodes, u32 *progress, groove_RecordDeltaVec *out);
jz_err groove__ivm__runtime__recursion__snapshot_requirement(const groove_IvmGraph *g, groove_NodeId node, groove_RecursiveNodes nodes, const groove_TableDelta *td, u32 ntd, const groove_BindingDelta *bd, u32 nbd, const groove_RecursiveState *state, bool *some, groove_NodeId *out);
jz_err groove__ivm__runtime__recursion__require_snapshot_inputs(const groove_IvmGraph *g, groove_EvaluationInputs *in, groove_NodeId root);
jz_err groove__ivm__runtime__recursion__hydrate_recursive_arrangements(groove_GraphRuntimeView *runtime, const groove_RecursiveOp *rec, groove_NodeId step, groove_RecordDeltas accumulated);
jz_err groove__ivm__runtime__recursion__snapshot_table_deltas(const groove_DatabaseSchema *schema, const groove_IvmGraph *g, const groove_Storage *storage, groove_NodeId root, groove_TableDeltaVec *out);
jz_err groove__ivm__runtime__recursion__recursive_read_tables(const groove_IvmGraph *g, groove_NodeId seed, groove_NodeId step, const groove_NodeId *step_witness /* nullable */, groove_StrVec *out);
jz_err groove__ivm__runtime__recursion__hydrated_arg_by_winners(const groove_RecordDeltas *input, groove_RecordDescriptor out_desc, const usize *g, u32 ng, const usize *c, u32 nc, u32 direction, groove_RecordDeltas *out);


/* ---------------- compilation.rs ---------------- */
typedef JZ_VEC(groove_CollectByProjection) groove_CollectByProjectionVec;
typedef JZ_VEC(groove_CollectBySlot) groove_CollectBySlotVec;
/* ivm::graph CollectByBuilder mirror (unit groove-ivm) */
typedef struct groove_CollectByField { groove_FieldRef field; jz_str output_name; bool unwrap_nullable; } groove_CollectByField;
typedef struct groove_CollectBySlotBuilder groove_CollectBySlotBuilder;
struct groove_CollectBySlotBuilder {
    JZ_VEC(groove_FieldRef) group_cols, owner_key_cols;
    JZ_VEC(groove_CollectByField) child_fields;
    jz_str collection_field;
    JZ_VEC(groove_CollectBySlotBuilder) slots;
    JZ_VEC(groove_TopByOrder) order_cols;
    JZ_VEC(groove_FieldRef) tie_cols;
    bool has_presence_col; groove_FieldRef presence_col;
    bool has_reference_array_col; groove_FieldRef reference_array_col;
    bool reference_order; u64 offset; groove_TopByLimit limit;
};
struct groove_CollectByBuilder {
    u32 mode;
    JZ_VEC(groove_FieldRef) group_cols;
    JZ_VEC(groove_CollectByField) parent_fields, child_fields;
    jz_str collection_field;
    JZ_VEC(groove_CollectBySlotBuilder) slots;
    JZ_VEC(groove_CollectByField) tuple_fields;
    JZ_VEC(groove_FieldRef) occurrence_id_cols;
    JZ_VEC(groove_TopByOrder) order_cols;
    JZ_VEC(groove_FieldRef) tie_cols;
    u64 offset; groove_TopByLimit limit;
};
jz_err groove__ivm__runtime__record_projection__collect_by_slots(groove_RecordDescriptor input, const groove_CollectBySlotBuilder *b, u32 nb, const usize *owner, u32 nowner, usize depth, groove_CollectBySlotVec *out);
jz_err groove__ivm__runtime__record_projection__collect_by_projections(groove_RecordDescriptor input, const groove_CollectByField *fields, u32 n, groove_CollectByProjectionVec *out);
jz_err groove__ivm__runtime__evaluator__validate_arg_by_primary_key_indices(const char *op_name, const groove_TableSchema *table, const usize *group, u32 ng, const usize *order, u32 no, const usize *pk, u32 npk);
jz_err groove__ivm__runtime__subscriptions__IvmRuntime__infer_builder_output_cached(const groove_IvmRuntime *self, const groove_GraphBuilder *g, jz_hmap *memo, groove_RecordDescriptor *out);
jz_err groove__ivm__runtime__subscriptions__IvmRuntime__infer_builder_output_uncached(const groove_IvmRuntime *self, const groove_GraphBuilder *g, jz_hmap *memo, groove_RecordDescriptor *out);
jz_err groove__ivm__runtime__compilation__IvmRuntime__add_dedup_graph(groove_IvmRuntime *self, const groove_GraphBuilder *g, groove_CompiledNode *out);
jz_err groove__ivm__runtime__compilation__IvmRuntime__add_dedup_template_graph(groove_IvmRuntime *self, const groove_GraphBuilder *g, groove_CompiledNode *out);
jz_err groove__ivm__runtime__compilation__IvmRuntime__add_dedup_schema_index(groove_IvmRuntime *self, const groove_TableSchema *t, const groove_IndexSchema *i, groove_NodeId *out /* nullable */);
/* typed_template.rs */
jz_err groove__ivm__runtime__typed_template__IvmRuntime__install_typed_template(groove_IvmRuntime *self, const groove_TypedGraphTemplate *program,
    const groove_CompiledNode *inputs, u32 n, groove_GraphBuilder *const *graphs, u32 ngraphs,
    const groove_PredicateExpr *predicates, u32 np, const groove_TemplateScalarArgument *scalars, u32 ns, groove_CompiledNode *out);
/* graph_lifecycle.rs */
void groove__ivm__runtime__graph_lifecycle__IvmRuntime__initialize_node_runtime(groove_IvmRuntime *self, groove_NodeId node);
bool groove__ivm__runtime__graph_lifecycle__IvmRuntime__add_retainer(groove_IvmRuntime *self, groove_NodeId id, groove_Retainer retainer);


/* ---------------- graph_lifecycle.rs ---------------- */
typedef struct groove_SubscriptionId { u64 runtime_namespace; u64 sequence; } groove_SubscriptionId;
void groove__ivm__runtime__subscriptions__SubscriptionId__retainer_key(groove_SubscriptionId id, jz_str *out);
bool groove__ivm__runtime__runtime_tick__PendingIncrementalEvaluation__is_pending(const groove_PendingIncrementalEvaluation *p);
void groove__ivm__runtime__runtime_tick__PendingIncrementalEvaluation__registered_nodes(const groove_PendingIncrementalEvaluation *p, jz_hmap *out /* HashSet<NodeId> */);
usize groove__ivm__runtime__join__ArrangementState__row_count(const groove_ArrangementState *s);
usize groove__ivm__runtime__join__ArrangementState__encoded_bytes(const groove_ArrangementState *s);
typedef struct groove_EphemeralGraphInstall { groove_IvmRuntime *runtime; bool committed; } groove_EphemeralGraphInstall;
groove_EphemeralGraphInstall groove__ivm__runtime__graph_lifecycle__EphemeralGraphInstall__new(groove_IvmRuntime *runtime);
groove_IvmRuntime *groove__ivm__runtime__graph_lifecycle__EphemeralGraphInstall__runtime(groove_EphemeralGraphInstall *g);
void groove__ivm__runtime__graph_lifecycle__EphemeralGraphInstall__commit(groove_EphemeralGraphInstall *g);
void groove_EphemeralGraphInstall_drop(groove_EphemeralGraphInstall *g);
groove_ExecutionLayoutStats groove__ivm__runtime__graph_lifecycle__IvmRuntime__execution_layout_stats(const groove_IvmRuntime *self);
void groove__ivm__runtime__graph_lifecycle__IvmRuntime__retained_node_ids(const groove_IvmRuntime *self, jz_hmap *out);
groove_RuntimeStats groove__ivm__runtime__graph_lifecycle__IvmRuntime__stats(const groove_IvmRuntime *self);
groove_RuntimeStats groove__ivm__runtime__graph_lifecycle__IvmRuntime__cheap_stats(const groove_IvmRuntime *self);
void groove__ivm__runtime__graph_lifecycle__IvmRuntime__record_hydration_memo_metrics(groove_IvmRuntime *self, const groove_TickMetrics *metrics);
bool groove__ivm__runtime__graph_lifecycle__IvmRuntime__remove_multisink_retainers(groove_IvmRuntime *self, groove_SubscriptionId id, const jz_btree *outputs /* String -> CompiledNode */);
bool groove__ivm__runtime__graph_lifecycle__IvmRuntime__remove_retainer(groove_IvmRuntime *self, groove_NodeId id, const groove_Retainer *retainer);
void groove__ivm__runtime__graph_lifecycle__IvmRuntime__collect_unretained_ephemeral_nodes(groove_IvmRuntime *self);
bool groove__ivm__runtime__graph_lifecycle__IvmRuntime__affected_recursive_nodes_are_current(const groove_IvmRuntime *self, const jz_hmap *affected, u64 current_tick);


/* ---------------- typed_template.rs: mirrors of groove-ivm template/graph items ---------------- */
enum { GROOVE_TNREF_INPUT_B, GROOVE_TNREF_NODE_B };
typedef struct groove_TemplateNodeRefB { u8 tag; u32 index; } groove_TemplateNodeRefB;
typedef JZ_VEC(groove_TemplateNodeRefB) groove_TemplateNodeRefVecB;
typedef struct groove_TemplateNodeB { groove_NodeDescriptor descriptor; groove_TemplateNodeRefVecB inputs; bool has_predicate; u32 predicate; } groove_TemplateNodeB;
typedef JZ_VEC(groove_TemplateNodeB) groove_TemplateNodeVecB;
typedef JZ_VEC(groove_RecordDescriptor) groove_RecordDescriptorVecB;
typedef JZ_VEC(groove_PredicateExpr) groove_PredicateExprVecB;
/* Layout of groove-ivm's struct groove_TypedGraphTemplate (template.h), mirrored. */
typedef struct groove_TypedGraphTemplateB {
    u64 identity; groove_TemplateNodeVecB nodes; groove_RecordDescriptorVecB inputs; groove_RecordDescriptor output;
    groove_TemplateNodeRefB root; bool has_ordering; groove_TemplateNodeRefB ordering; bool terminal;
    groove_GraphBuilder fallback; groove_PredicateExprVecB predicate_markers;
} groove_TypedGraphTemplateB;
typedef JZ_VEC(usize) groove_UsizeVecT;
typedef struct groove_TemplateSliceB { groove_UsizeVecT inputs, predicates; } groove_TemplateSliceB;
typedef struct groove_CompiledForestEntryB { groove_TypedGraphTemplateB *template_; groove_TemplateSliceB slice; } groove_CompiledForestEntryB;
typedef struct groove_CompiledForestB { u32 len; groove_CompiledForestEntryB items[]; } groove_CompiledForestB; /* jz_rc payload */
typedef JZ_VEC(groove_GraphBuilder) groove_GraphBuilderValVec; /* Vec<GraphBuilder> */
typedef struct groove_TemplateCacheEntryB { u64 key; groove_GraphBuilderValVec graphs; groove_CompiledForestB *forest; } groove_TemplateCacheEntryB;
typedef struct groove_TypedGraphTemplateCacheB { jz_deque entries; u64 compilations, reuses; } groove_TypedGraphTemplateCacheB;
typedef struct groove_TemplateGraphInputB { groove_GraphBuilder *graph; groove_RecordDescriptor output; } groove_TemplateGraphInputB;
typedef JZ_VEC(groove_TemplateGraphInputB) groove_TemplateGraphInputVecB;
groove_TemplateGraphInputB groove__ivm__template__TemplateGraphInput__with_output_contract(groove_GraphBuilder graph, groove_RecordDescriptor output);
void groove__ivm__template__TemplateGraphInputVec__drop(groove_TemplateGraphInputVecB *v);
jz_err groove__ivm__template__bind_template_graphs_inner(const groove_GraphBuilder *graphs, u32 n, const groove_TemplateGraphInputB *inputs, u32 ni,
                                                         bool descend_bound, const void *arguments /* Option<&TemplateArguments> */, groove_GraphBuilderValVec *out);
void groove__ivm__template__TypedGraphTemplate__drop(groove_TypedGraphTemplate *v);
jz_err groove__ivm__template__TypedGraphTemplate__bind_declarative_with_arguments(const groove_TypedGraphTemplate *self, groove_GraphBuilder *const *inputs, u32 ni,
    const groove_PredicateExpr *predicates, u32 np, const groove_TemplateScalarArgument *scalars, u32 ns, groove_GraphBuilder *out);
jz_err groove__ivm__template__arguments__bind_project_expression(groove_ProjectExpr *expression, const groove_TemplateScalarArgument *arguments, u32 n);
void groove__ivm__template__CompiledForest__from_vec(groove_CompiledForestEntryB *items, u32 n, groove_CompiledForestB **out);
void groove__ivm__template__TemplateCacheEntry__drop_v(void *e);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__input_source(groove_InputSourceId id, groove_RecordDescriptor output);
void groove__ivm__graph__GraphBuilder__clone(groove_GraphBuilder *dst, const groove_GraphBuilder *src);
void groove__ivm__graph__GraphBuilder__drop(groove_GraphBuilder *v);
void groove__ivm__graph__GraphBuilder__drop_v(void *v);
void groove__ivm__graph__GraphBuilder__map_inputs(const groove_GraphBuilder *self, groove_GraphBuilder *(*map)(groove_GraphBuilder *const *input, void *ctx), void *ctx, groove_GraphBuilder *out);
groove_NodeId groove__ivm__graph__IvmGraph__dedup_node_parts(groove_IvmGraph *self, const groove_OpType *op, groove_NodeIdVec inputs, groove_NodeOutput output, groove_NodeDurability durability);
bool groove__ivm__runtime__subscriptions__graph_builders_equal(const groove_GraphBuilder *a, const groove_GraphBuilder *b);
groove_DatabaseSchema groove__schema__DatabaseSchema__new_empty(void);
jz_err groove__ivm__runtime__typed_template__TypedGraphTemplateCache__compile(groove_TypedGraphTemplateCacheB *self, const groove_GraphBuilder *graphs, u32 n, groove_GraphBuilderValVec *out);
typedef jz_err (*groove_DescribeFn)(void *ctx, const groove_GraphBuilder *g, groove_RecordDescriptor *out);
jz_err groove__ivm__runtime__typed_template__TypedGraphTemplateCache__compile_with_sources(groove_TypedGraphTemplateCacheB *self, const groove_GraphBuilder *graphs, u32 n, groove_DescribeFn describe, void *ctx, groove_GraphBuilderValVec *out);

void groove_IvmRuntime_clone(groove_IvmRuntime *dst, const groove_IvmRuntime *src);
const groove_TableSchema *groove__ivm__runtime__IvmRuntime__table(const groove_IvmRuntime *self, const char *table, u32 len);


/* ---------------- schema.rs: mirrors of sibling-unit items ---------------- */
enum { GROOVE_RIV_MATERIALIZE_B, GROOVE_RIV_PHYSICAL_B, GROOVE_RIV_PHYSICAL_FIELDS_B };
typedef struct groove_RootIndirectValuesB { u32 tag; jz_btree *physical; } groove_RootIndirectValuesB;
typedef struct groove_MultisinkDeltasB { jz_btree sinks; jz_btree terminal_sinks; } groove_MultisinkDeltasB;
extern const jz_btree_desc groove_ivm_rt_STR_COMPILED_MAP; /* BTreeMap<String, CompiledNode> */
jz_err groove__ivm__runtime__runtime_tick__IvmRuntime__flush_pending_binding_retractions(groove_IvmRuntime *self, groove_Storage *storage);
jz_err groove__ivm__runtime__runtime_tick__IvmRuntime__hydration_snapshot(groove_IvmRuntime *self, groove_NodeId output_node, groove_Storage *storage, u32 mode, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__runtime_tick__IvmRuntime__hydration_snapshot_with_root_values(groove_IvmRuntime *self, groove_NodeId output_node, groove_Storage *storage, u32 mode,
    groove_RootIndirectValuesB root_indirect_values, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__runtime_tick__IvmRuntime__hydration_snapshots(groove_IvmRuntime *self, const jz_btree *outputs, groove_Storage *storage, u32 mode, groove_MultisinkDeltasB *out);
usize groove__ivm__runtime__subscriptions__count_builder_nodes(const groove_GraphBuilder *g);
bool groove__ivm__runtime__subscriptions__builder_contains_binding_source(const groove_GraphBuilder *g);
void groove__records__values__ValueType__collect_variant_registries(const groove_ValueType *t, jz_btree *output);
bool groove__records__values__ValueType__can_evolve_registry_to(const groove_ValueType *a, const groove_ValueType *b);
const jz_str *groove__schema__TableVariant__payload_name_for_shared(const groove_TableVariant *v, const jz_str *shared);
groove_ProjectField groove__ivm__graph__ProjectField__renamed(jz_str source_name, jz_str output_name);
jz_err groove__records__values__EnumSchema__case(const groove_EnumSchema *s, u32 tag, const groove_EnumCase **out);
groove_OwnedRecord groove__records__OwnedRecord__new(jz_bytes raw, groove_RecordDescriptor d);
typedef struct groove_VariantRecordB { u32 variant_tag; groove_OwnedRecord record; } groove_VariantRecordB;
typedef struct groove_DirectRecordStoreSchema groove_DirectRecordStoreSchema;
const groove_DirectRecordStoreSchema *groove__schema__DatabaseSchema__direct_record_store(const groove_DatabaseSchema *s, const char *name, u32 len);
void groove_ColumnSchema_drop(groove_ColumnSchema *c);
void groove_TableVariant_drop(groove_TableVariant *v);
void groove_PendingIndexIds_drop(groove_PendingIndexIds *p);
void groove_MultisinkDeltas_drop(groove_MultisinkDeltasB *d);
/* schema.rs (this unit) */
bool groove__ivm__runtime__IvmRuntime__declared_index_repair_is_idle(const groove_IvmRuntime *self);
jz_err groove__ivm__runtime__IvmRuntime__rebuild_declared_indexes(groove_IvmRuntime *self, groove_Storage *storage);
jz_err groove__ivm__runtime__IvmRuntime__register_table(groove_IvmRuntime *self, groove_TableSchema table);
jz_err groove__ivm__runtime__IvmRuntime__register_table_variant_with_columns(groove_IvmRuntime *self, const char *table, u32 len, groove_ColumnSchema *columns, u32 ncols, groove_TableVariant variant_tag);
jz_err groove__ivm__runtime__IvmRuntime__evolve_table_variant_registries(groove_IvmRuntime *self, const char *table, u32 len, const groove_ColumnSchema *columns, u32 n);
jz_err groove__ivm__runtime__IvmRuntime__register_table_index(groove_IvmRuntime *self, const char *table, u32 len, groove_IndexSchema index, groove_Storage *storage);
jz_err groove__ivm__runtime__IvmRuntime__define_variant_projection(groove_IvmRuntime *self, const char *table, u32 len, const char *target, u32 tlen, groove_RecordDescriptor output);
jz_err groove__ivm__runtime__IvmRuntime__register_variant_projection_case(groove_IvmRuntime *self, const char *table, u32 len, const char *target, u32 tlen, u32 variant_tag, const groove_ProjectField *fields, u32 n);
jz_err groove__ivm__runtime__IvmRuntime__refresh_variant_projection_case_for_registry_evolution(groove_IvmRuntime *self, const char *table, u32 len, const char *target, u32 tlen, u32 variant_tag, const groove_ProjectField *fields, u32 n);
jz_err groove__ivm__runtime__IvmRuntime__register_variant_projection_case_omitting_unrepresentable_enums(groove_IvmRuntime *self, const char *table, u32 len, const char *target, u32 tlen, u32 variant_tag, const groove_ProjectField *fields, u32 n);
jz_err groove__ivm__runtime__IvmRuntime__register_variant_projection_enum_case(groove_IvmRuntime *self, const char *table, u32 len, const char *target, u32 tlen, u32 variant_tag,
    const char *enum_field, u32 flen, const groove_EnumSchema *enum_schema, const char *enum_case, u32 clen, const groove_ProjectField *fields, u32 n);
jz_err groove__ivm__runtime__IvmRuntime__register_variant_projection_ignore_case(groove_IvmRuntime *self, const char *table, u32 len, const char *target, u32 tlen, u32 variant_tag);
jz_err groove__ivm__runtime__IvmRuntime__project_variant_record(const groove_IvmRuntime *self, const char *table, u32 len, const char *target, u32 tlen, const groove_VariantRecordB *record, bool *some, groove_OwnedRecord *out);
void groove__ivm__runtime__schema__IvmRuntime__invalidate_table_inputs(groove_IvmRuntime *self, const char *table, u32 len);
const groove_IndexSchema *groove__ivm__runtime__IvmRuntime__index(const groove_IvmRuntime *self, const char *table, u32 len, const char *index, u32 ilen);
const groove_DirectRecordStoreSchema *groove__ivm__runtime__IvmRuntime__direct_record_store(const groove_IvmRuntime *self, const char *store, u32 len);
jz_err groove__ivm__runtime__IvmRuntime__query_snapshot(groove_IvmRuntime *self, groove_GraphBuilder graph, groove_Storage *storage, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__IvmRuntime__query_snapshot_with_root_values(groove_IvmRuntime *self, groove_GraphBuilder graph, groove_Storage *storage, groove_RootIndirectValuesB root_indirect_values, groove_RecordDeltas *out);
jz_err groove__ivm__runtime__IvmRuntime__query_snapshots(groove_IvmRuntime *self, const jz_str *names, groove_GraphBuilder *graphs, u32 n, groove_Storage *storage, groove_MultisinkDeltasB *out);

#endif
