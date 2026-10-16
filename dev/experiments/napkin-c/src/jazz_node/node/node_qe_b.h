/* node_qe_b.h: types and cross-file prototypes owned by unit node-qe-b
 * (jazz_node::node::query_eval::{authorization, maintained_views, ...} and
 * the node/state files include!d into jazz_node::node). */
#ifndef JZ_NODE_QE_B_H
#define JZ_NODE_QE_B_H
#include "jz_rt.h"
#include "jazz_node/_ext_node-qe-b.h"

/* ---- query_eval::local_authority_reconciliation ---- */
typedef struct jazz_node_LocalAuthorityReconciliation {
    bool has_source; jazz_protocol_AuthorityResultKey source;
    u64 generation;
} jazz_node_LocalAuthorityReconciliation;
bool jazz_node__node__query_eval__local_authority_reconciliation__LocalAuthorityReconciliation__is_due(const jazz_node_LocalAuthorityReconciliation *self, const jazz_protocol_AuthorityResultKey *source, u64 generation);
void jazz_node__node__query_eval__local_authority_reconciliation__LocalAuthorityReconciliation__replace_source(jazz_node_LocalAuthorityReconciliation *self, jazz_protocol_AuthorityResultKey source, u64 generation);

/* ---- query_eval::policy_factoring ---- */
typedef JZ_VEC(jazz_model_PolicyBranch) jazz_model_PolicyBranchVec;
typedef JZ_VEC(jazz_model_PolicyBranchVec) jazz_model_PolicyBranchVecVec;
bool jazz_node__node__query_eval__policy_factoring__factor_policy_branches(const jazz_model_PolicyBranch *branches, u32 n, jazz_model_PolicyBranchVecVec *out);

/* ---- state/durable.rs (jazz_node::node) used across files ---- */
bool jazz_node__node__NodeState__current_row_tx_id(jazz_node_NodeState *self, const jazz_node_CurrentRow *row, jazz_model_TxId *out);

/* ---- query_eval::subscription_watermarks ---- */
bool jazz_node__node__query_eval__subscription_watermarks__NodeState__watermark_catch_up_view(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape);
jz_err jazz_node__node__query_eval__subscription_watermarks__NodeState__purge_subscription_watermarks(jazz_node_NodeState *self);
jz_err jazz_node__node__query_eval__subscription_watermarks__NodeState__persist_subscription_watermark(jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key, jazz_types_GlobalTime settled_through, const u8 revision[16]);
jz_err jazz_node__node__query_eval__subscription_watermarks__NodeState__restore_subscription_watermark(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, const jazz_protocol_AuthorityResultKey *key, jazz_types_AuthorSubject identity, bool *out);

/* ---- query_eval::bindings ---- */
typedef enum { JZN_PCBM_STRICT = 0, JZN_PCBM_FAIL_CLOSED_AUTHORIZATION_SUPPORT = 1 } jazz_node_PreparedClaimBindingMode;

/* ---- query_eval::prepared_bindings ---- */
jz_str jazz_node__node__query_eval__prepared_bindings__query_binding_value_signature(const jazz_model_Binding *binding);
jz_str jazz_node__node__query_eval__prepared_bindings__policy_plan_cache_signature(const jazz_model_Binding *binding, jazz_types_AuthorSubject identity, const jz_str *claims_scope);
jz_str jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_prepared_params(const jazz_node_PreparedQueryParam *params, u32 n);
jz_str jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_parts(const jz_btree *param_types, const jz_btree *claim_params);
bool jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_parts_if_needed(const jz_btree *param_types, const jz_btree *claim_params, jz_str *out);
bool jazz_node__node__query_eval__prepared_bindings__authorization_binding_source_shape(const jazz_model_ValidatedQuery *shape, const jz_btree *extra_user_params, const jz_btree *claim_params, jz_str *out);
jz_err jazz_node__node__query_eval__prepared_bindings__binding_values_for_plan(const jazz_model_Binding *binding, const jazz_node_PreparedQueryParam *params, u32 n, const jazz_engine_PolicyContext *policy, jazz_node_PreparedClaimBindingMode mode, groove_ValueVec *out);
jz_err jazz_node__node__query_eval__prepared_bindings__prepared_claim_value(const jazz_engine_ClaimPath *path, const jazz_engine_PolicyContext *policy, bool *has, groove_Value *out);
void jazz_node__node__query_eval__prepared_bindings__coerce_binding_values_for_shape(const jazz_model_ValidatedQuery *shape, jz_btree *values);
/* shared btree descriptors (defined in prepared_bindings.c) */
extern const jz_btree_desc jazz_node_qeb_STR_COLUMN_TYPE_DESC;  /* String -> ColumnType */
extern const jz_btree_desc jazz_node_qeb_STR_CLAIM_PARAM_DESC;  /* String -> ProgramClaimParam */
extern const jz_btree_desc jazz_node_qeb_STR_VALUE_DESC;        /* String -> Value */
extern const jz_btree_desc jazz_node_qeb_STR_SET_DESC;          /* BTreeSet<String> */
extern const jz_btree_desc jazz_node_qeb_STR_STR_DESC;          /* String -> String */
void jazz_node_qeb_str_drop(void *s);
void jazz_node_qeb_str_clone(void *dst, const void *src);
void jazz_node_qeb_value_clone(void *dst, const void *src);
void jazz_node_qeb_column_type_clone(void *dst, const void *src);

/* ---- query_eval::query_read_sets ---- */
jazz_engine_RequestedReadSet jazz_node__node__query_eval__query_read_sets__current_query_read_set(const jazz_engine_NormalizedRowSetShape *shape, jazz_types_SchemaVersionId read_schema, jazz_types_SchemaVersionId policy_schema, jazz_model_DurabilityTier tier, const jazz_protocol_BindingViewKey *settled_binding_view, const jazz_protocol_AuthorityResultKey *settled_authority_result_key, bool settled_current_default);
jazz_engine_RequestedReadSet jazz_node__node__query_eval__query_read_sets__historical_query_read_set(const jazz_engine_NormalizedRowSetShape *shape, jazz_types_SchemaVersionId schema_version, jazz_types_GlobalTime position);
jazz_engine_RequestedReadSet jazz_node__node__query_eval__query_read_sets__snapshot_query_read_set(const jazz_engine_NormalizedRowSetShape *shape, jazz_types_SchemaVersionId schema_version, const jazz_model_Snapshot *snapshot);
jazz_engine_RequestedReadSet jazz_node__node__query_eval__query_read_sets__tx_query_read_set(const jazz_engine_NormalizedRowSetShape *shape, jazz_types_SchemaVersionId schema_version, jazz_model_OpenTransactionId tx_id, const jazz_model_Snapshot *snapshot);
jz_err jazz_node__node__query_eval__query_read_sets__query_read_set_for_read_view(const jazz_engine_NormalizedRowSetShape *shape, jazz_types_SchemaVersionId read_schema, jazz_types_SchemaVersionId policy_schema, jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view, const jazz_protocol_BindingViewKey *settled_binding_view, const jazz_protocol_AuthorityResultKey *settled_authority_result_key, const jazz_model_JazzSchema *schema, jazz_engine_RequestedReadSet *out);

/* ---- query_eval::query_result_rows ---- */
int jazz_node__node__query_eval__query_result_rows__compare_optional_values(const groove_Value *left, const groove_Value *right);
/* sort_aggregate_rows<T>: one copy; `row` and `tie_break` are the Rust closures. */
typedef struct jazz_node_AggregateSortVt {
    const jazz_node_CurrentRow *(*row)(const void *item, void *ctx);
    int (*tie_break)(const void *a, const void *b, void *ctx);
    void *ctx;
} jazz_node_AggregateSortVt;
jz_err jazz_node__node__query_eval__query_result_rows__sort_aggregate_rows(const jazz_model_Query *query, void *rows, u32 n, usize elem, const jazz_node_AggregateSortVt *vt);
jz_err jazz_node__node__query_eval__query_result_rows__aggregate_result_table(const jazz_model_Query *query, const jazz_model_TableSchema *source_table, jazz_model_TableSchema *out);
jz_err jazz_node__node__query_eval__query_result_rows__aggregate_output_schema(const jazz_engine_AppRowSchema *output, const jazz_engine_AggregateResultSchema **out);
jz_err jazz_node__node__query_eval__query_result_rows__aggregate_record_field_index(const groove_BorrowedRecord *record, const groove_DescriptorField *field, u32 *out);
jz_err jazz_node__node__query_eval__query_result_rows__aggregate_query_row_uuid(const jazz_engine_AppRowSchema *output, const groove_BorrowedRecord *record, jazz_types_RowUuid *out);

/* ---- query_eval::unavailable_inputs ---- */
typedef struct jazz_node_LocalUnavailableInput {
    bool has_id; groove_InputSourceId id;
    u64 runtime_token;
    jz_btree rows; /* BTreeSet<RowUuid> */
} jazz_node_LocalUnavailableInput;
typedef struct jazz_node_ScopeTableKey { jazz_protocol_PolicyBindingKey scope; jazz_types_GlobalPhysicalTableId table; } jazz_node_ScopeTableKey;
typedef struct jazz_node_ScopeTableRowKey { jazz_protocol_PolicyBindingKey scope; jazz_types_GlobalPhysicalTableId table; jazz_types_RowUuid row; } jazz_node_ScopeTableRowKey;
typedef struct jazz_node_UnavailableRowChange { jazz_types_GlobalPhysicalTableId table; jazz_types_RowUuid row; bool unavailable; } jazz_node_UnavailableRowChange;
extern const jz_btree_desc jazz_node_qeb_UUID_SET_DESC;
void jazz_node_LocalUnavailableInput_init(jazz_node_LocalUnavailableInput *i);
bool jazz_node__node__query_eval__unavailable_inputs__local_unavailable_policy_binding(const jazz_engine_QueryProgramRequest *request, jazz_protocol_PolicyBindingKey *out);
bool jazz_node__node__query_eval__unavailable_inputs__is_current_app_source(const jazz_engine_SourceExpr *source);
bool jazz_node__node__query_eval__unavailable_inputs__NodeState__local_read_policy_binding(const jazz_node_NodeState *self, jazz_types_AuthorSubject identity, jazz_protocol_PolicyBindingKey *out);
bool jazz_node__node__query_eval__unavailable_inputs__NodeState__is_local_row_unavailable(const jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *scope, jazz_types_GlobalPhysicalTableId table, jazz_types_RowUuid row);
jz_err jazz_node__node__query_eval__unavailable_inputs__NodeState__local_availability_table_id(const jazz_node_NodeState *self, jazz_types_SchemaVersionId schema, const jz_str *table, jazz_types_GlobalPhysicalTableId *out);
jz_err jazz_node__node__query_eval__unavailable_inputs__NodeState__set_local_row_unavailable(jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *scope, const jz_str *table, jazz_types_RowUuid row, bool unavailable, bool *out);
jz_err jazz_node__node__query_eval__unavailable_inputs__NodeState__update_local_unavailable_rows(jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *scope, const jazz_node_UnavailableRowChange *rows, u32 n, bool *out);
jz_err jazz_node__node__query_eval__unavailable_inputs__NodeState__require_local_availability_context_capacity(const jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *scope);
jz_err jazz_node__node__query_eval__unavailable_inputs__NodeState__retire_local_availability_scope_inputs(jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *scope);
jz_err jazz_node__node__query_eval__unavailable_inputs__NodeState__exclude_local_unavailable_graph(jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *scope, jazz_types_SchemaVersionId schema, const jazz_engine_SourceRequest *source, groove_GraphBuilder *graph, bool pending_ahead, groove_GraphBuilder **out);

/* ---- query_eval::authorization (cross-file) ---- */
void jazz_node__node__query_eval__authorization__NodeState__query_program_policy_context(const jazz_node_NodeState *self, jazz_types_AuthorSubject identity, jazz_engine_PolicyContext *out);

/* ---- query_eval::local_availability_receipts ---- */
typedef struct jazz_node_LocalAvailabilityWatermark {
    jazz_types_NodeUuid core;
    u64 core_epoch, claims_revision, policy_epoch;
    jazz_types_GlobalTime settled_through;
    u64 authorization_progress;
} jazz_node_LocalAvailabilityWatermark;
typedef enum { JZN_LRA_READABLE = 0, JZN_LRA_CURRENT_UNAVAILABLE = 1 } jazz_node_LocalRowAvailability;
typedef struct jazz_node_LocalAvailabilityRecord { jazz_node_LocalRowAvailability status; jazz_node_LocalAvailabilityWatermark watermark; } jazz_node_LocalAvailabilityRecord;
typedef struct jazz_node_LocalAvailabilityOutcome { jazz_types_GlobalPhysicalTableId table; jazz_types_RowUuid row; jazz_node_LocalRowAvailability status; } jazz_node_LocalAvailabilityOutcome;
typedef struct jazz_node_CoreEpoch { jazz_types_NodeUuid core; u64 epoch; } jazz_node_CoreEpoch;
jz_err jazz_node__node__query_eval__local_availability_receipts__NodeState__activate_local_availability_authority(jazz_node_NodeState *self, jazz_protocol_PolicyBindingKey scope, jazz_types_NodeUuid core, u64 epoch);
jz_err jazz_node__node__query_eval__local_availability_receipts__NodeState__apply_verified_local_row_availability(jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *scope, jazz_node_LocalAvailabilityWatermark watermark, const jazz_node_LocalAvailabilityOutcome *outcomes, u32 n, bool *out);
jz_err jazz_node__node__query_eval__local_availability_receipts__NodeState__apply_local_row_availability_after_persist(jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *scope, jazz_node_LocalAvailabilityWatermark watermark, const jazz_node_LocalAvailabilityOutcome *outcomes, u32 n, void (*after_persist)(void *), void *after_ctx, bool *out);
jz_err jazz_node__node__query_eval__local_availability_receipts__NodeState__recover_local_availability_records(jazz_node_NodeState *self);

/* ---- state/read_payload.rs ---- */
jz_err jazz_node__node__NodeState__table_in_schema_ref(const jazz_node_NodeState *self, const jz_str *table, jazz_types_SchemaVersionId schema_version, const jazz_model_TableSchema **out);

/* ---- query_eval::narrowed_reads ---- */
typedef struct jazz_node_NarrowedSourceRead { jazz_model_ValidatedQuery shape; jazz_model_Binding binding; } jazz_node_NarrowedSourceRead;
typedef struct jazz_node_ExclusiveSourceReads { jz_btree reads; /* SourceId -> NarrowedSourceRead */ jz_btree payload; /* BTreeSet<SourceId> */ } jazz_node_ExclusiveSourceReads;
void jazz_node_NarrowedSourceRead_drop(void *r);
jz_err jazz_node__node__query_eval__narrowed_reads__NodeState__exclusive_source_reads(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, bool include_deleted, jazz_node_ExclusiveSourceReads *out);

/* ---- shared small helpers (static inline, one copy per TU like a C codebase's util.h) ---- */
static inline void jz_str_vec_drop(jz_str_vec *v) { for (u32 i = 0; i < v->len; i++) jz_str_drop(&v->ptr[i]); jz_free(v->ptr); v->ptr = NULL; v->len = v->cap = 0; }
/* Debug formatting of string collections ({:?}) */
void jazz_node_qeb_debug_str_set(jz_str *out, const jz_btree *set);
void jazz_node_qeb_debug_str_vec(jz_str *out, const jz_str *v, u32 n);
void jazz_node_qeb_debug_str(jz_str *out, const jz_str *s);

/* ---- query_eval::route_discipline ---- */
enum { JZN_ROUTE_VIOLATION = 0x01f0 };
jz_btree jazz_node__node__query_eval__route_discipline__binding_slot_routes(const jazz_engine_ParameterDomain *domain);
typedef struct jazz_node_RouteTerminal { const char *sink; u32 sink_len; const groove_GraphBuilder *graph; jz_btree delivered; } jazz_node_RouteTerminal;
typedef jz_err (*jazz_node_RouteInferFn)(const groove_GraphBuilder *g, groove_RecordDescriptor **out, void *ctx);
jz_err jazz_node__node__query_eval__route_discipline__check_route_discipline(jazz_node_RouteTerminal *terminals, u32 n, const jz_btree *slot_routes, jazz_node_RouteInferFn infer, void *infer_ctx);
groove_GraphBuilder *jazz_node__node__query_eval__route_discipline__isolate_binding_sources(const groove_GraphBuilder *graph, const jz_str *suffix, jz_hmap *rewritten);
void jazz_node__node__query_eval__route_discipline__NodeState__scope_program_binding_source(const jazz_node_NodeState *self, jazz_engine_QueryProgram *program);
jz_err jazz_node__node__query_eval__route_discipline__NodeState__check_program_route_discipline(const jazz_node_NodeState *self, const jazz_engine_QueryProgram *program);
extern const jz_hmap_desc jazz_node_qeb_REWRITTEN_DESC;

/* ---- state/lifecycle.rs ---- */
bool jazz_node__node__NodeState__active_session_claim_scope_key(const jazz_node_NodeState *self, jazz_types_AuthorSubject identity, jz_str *out);

/* ---- query_eval::bindings ---- */
typedef enum { JZN_PBM_INLINE_ALL_REACHABLE_SEEDS = 0, JZN_PBM_RETAIN_ALL_PARAMS = 1 } jazz_node_ParamBindingMode;
jazz_model_Query jazz_node__node__query_eval__bindings__authorization_query_from_read_policy(const jazz_model_TableSchema *table);
jazz_model_JoinVia jazz_node__node__query_eval__bindings__rewrite_claim_join_for_binding(jazz_model_JoinVia join, const jz_btree *claims);
jazz_model_Predicate jazz_node__node__query_eval__bindings__rewrite_claim_predicate_for_binding(jazz_model_Predicate predicate, const jz_btree *claims);
void jazz_node__node__query_eval__bindings__default_permission_scope_claim_values(jazz_types_AuthorSubject writer, jz_btree *out);
void jazz_node__node__query_eval__bindings__default_policy_claim_values(jazz_types_AuthorSubject writer, jz_btree *out);
void jazz_node__node__query_eval__bindings__bind_scope_claim_operands(jazz_model_Query *query, const jz_btree *claim_values, jz_btree *binding_values);
jz_err jazz_node__node__query_eval__bindings__disambiguate_policy_claim_params(jazz_model_Query *query, const jazz_model_JazzSchema *schema, jz_btree *binding_values, jz_btree *out);
jz_err jazz_node__node__query_eval__bindings__disambiguate_policy_claim_params_with_outer_slots(jazz_model_Query *query, const jazz_model_JazzSchema *schema, jz_btree *binding_values, const jz_btree *outer_slots, jz_btree *out);
jz_str jazz_node__node__query_eval__bindings__typed_claim_param_alias(const jz_str *name, const groove_ColumnType *ty);
u32 jazz_node__node__query_eval__bindings__ParamBindingMode__cache_key(jazz_node_ParamBindingMode mode);
jz_str jazz_node__node__query_eval__bindings__binding_user_params_cache_key(const jz_btree *params);
jz_str jazz_node__node__query_eval__bindings__binding_claim_params_cache_key(const jz_btree *params);
jz_err jazz_node__node__query_eval__bindings__bind_query_params_with_mode(const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, const jazz_model_JazzSchema *schema, jazz_node_ParamBindingMode mode, jazz_model_ValidatedQuery *out);
jz_err jazz_node__node__query_eval__bindings__inline_snapshot_bind_filter_literals(const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, const jazz_model_JazzSchema *schema, jazz_model_ValidatedQuery *out);
void jazz_node__node__query_eval__bindings__retarget_binding_value_sources(jazz_engine_NormalizedRowSetShape *shape, const jz_str *binding_source_shape);
jz_btree jazz_node__node__query_eval__bindings__binding_claim_params_for_shape(const jazz_engine_NormalizedRowSetShape *shape, const jz_btree *param_types);
jz_btree jazz_node__node__query_eval__bindings__normalized_source_tables(const jazz_engine_NormalizedRowSetShape *shape);
jz_err jazz_node__node__query_eval__bindings__collect_reachable_seed_claim_params(const jazz_model_JazzSchema *schema, const jazz_model_Query *query, jz_btree *params);
jazz_engine_ProgramBinding jazz_node__node__query_eval__bindings__NodeState__program_binding_for_shape(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, const jz_str *source_shape, jz_btree extra_user_params, jz_btree claim_params);
jz_err jazz_node__node__query_eval__bindings__NodeState__program_binding_for_shape_and_policy(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, const jz_str *source_shape, jz_btree extra_user_params, jz_btree claim_params, const jazz_engine_PolicyContext *policy, jazz_engine_ProgramBinding *out);
jz_err jazz_node__node__query_eval__bindings__NodeState__program_binding_for_shape_and_policy_with_prepared_claim_mode(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, const jz_str *source_shape, jz_btree extra_user_params, jz_btree claim_params, const jazz_engine_PolicyContext *policy, jazz_node_PreparedClaimBindingMode mode, jazz_engine_ProgramBinding *out);


/* state/lifecycle.rs (used by subscriptions) */
void jazz_node__node__NodeState__clear_settled_result_view(jazz_node_NodeState *self, jazz_protocol_AuthorityResultKey authority_result_key);
void jazz_node__node__NodeState__retire_authority_result_view(jazz_node_NodeState *self, jazz_protocol_AuthorityResultKey authority_result_key);

/* query_eval/subscriptions.rs */
jz_err jazz_node__node__query_eval__subscriptions__NodeState__register_shape_with_options(jazz_node_NodeState *self, jazz_model_ShapeId shape_id, jazz_protocol_ShapeAst ast, jazz_protocol_RegisterShapeOptions opts);
jz_err jazz_node__node__query_eval__subscriptions__NodeState__register_shape_for_peer_with_options(jazz_node_NodeState *self, u64 peer, jazz_model_ShapeId shape_id, jazz_protocol_ShapeAst ast, jazz_protocol_RegisterShapeOptions opts);
jz_err jazz_node__node__query_eval__subscriptions__NodeState__validate_shape_ast_for_registration(const jazz_node_NodeState *self, jazz_model_ShapeId shape_id, const jazz_protocol_ShapeAst *ast, bool *has, jazz_model_ValidatedQuery *out);
jz_err jazz_node__node__query_eval__subscriptions__NodeState__drain_parked_shape_registrations(jazz_node_NodeState *self);
void jazz_node__node__query_eval__subscriptions__NodeState__release_shape_for_peer(jazz_node_NodeState *self, u64 peer, jazz_model_ShapeId shape_id);
void jazz_node__node__query_eval__subscriptions__NodeState__release_shapes_for_peer(jazz_node_NodeState *self, u64 peer);
jz_err jazz_node__node__query_eval__subscriptions__NodeState__register_query_subscription_for_peer(jazz_node_NodeState *self, u64 publication_owner, jazz_model_ShapeId shape_id, jazz_protocol_ShapeAst ast, jazz_protocol_RegisterShapeOptions opts, jazz_protocol_Subscribe subscribe, jazz_protocol_PolicyBindingKey policy_binding);
void jazz_node__node__query_eval__subscriptions__NodeState__release_query_subscription_for_peer(jazz_node_NodeState *self, u64 publication_owner, jazz_protocol_SubscriptionKey subscription);
void jazz_node__node__query_eval__subscriptions__NodeState__release_query_subscription_for_peer_with_admitted_policy_binding(jazz_node_NodeState *self, u64 publication_owner, jazz_protocol_SubscriptionKey subscription, jazz_protocol_PolicyBindingKey policy_binding);
jz_err jazz_node__node__query_eval__subscriptions__NodeState__apply_subscribe(jazz_node_NodeState *self, jazz_protocol_Subscribe subscribe);
jz_err jazz_node__node__query_eval__subscriptions__NodeState__apply_subscribe_with_admitted_policy_binding(jazz_node_NodeState *self, jazz_protocol_Subscribe subscribe, jazz_protocol_PolicyBindingKey policy_binding);
bool jazz_node__node__query_eval__subscriptions__NodeState__holds_supporting_revision(const jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, const u8 revision[16]);
void jazz_node__node__query_eval__subscriptions__NodeState__forget_supporting_revision(jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription);
void jazz_node__node__query_eval__subscriptions__NodeState__forget_declared_known_state(jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription);
jz_err jazz_node__node__query_eval__subscriptions__NodeState__authority_result_key_for_subscription(const jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, jazz_protocol_AuthorityResultKey *out);
bool jazz_node__node__query_eval__subscriptions__NodeState__unique_authority_result_key_for_binding_view(const jazz_node_NodeState *self, jazz_protocol_BindingViewKey binding_view, jazz_protocol_AuthorityResultKey *out);
void jazz_node__node__query_eval__subscriptions__NodeState__apply_unsubscribe(jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription);
void jazz_node__node__query_eval__subscriptions__NodeState__apply_unsubscribe_with_admitted_policy_binding(jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, jazz_protocol_PolicyBindingKey policy_binding);
bool jazz_node__node__query_eval__subscriptions__NodeState__invalidate_ownerless_settled_result_view(jazz_node_NodeState *self, jazz_protocol_BindingViewKey binding_view_key);
void jazz_node__node__query_eval__subscriptions__NodeState__invalidate_authority_result_settlement(jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key);
bool jazz_node__node__query_eval__subscriptions__NodeState__has_settled_authority_result(const jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key);
jz_err jazz_node__node__query_eval__subscriptions__NodeState__selected_deletion_witnesses(jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key, jazz_types_SchemaVersionId schema, jz_btree *out);
jz_btree jazz_node__node__query_eval__subscriptions__NodeState__scalar_authority_input_rows(const jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key, const jz_str *table);
u64 jazz_node__node__query_eval__subscriptions__NodeState__applied_authority_result_generation(const jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key);
jz_err jazz_node__node__query_eval__subscriptions__NodeState__allocate_authoritative_reset_generation(jazz_node_NodeState *self, u64 *out);
bool jazz_node__node__query_eval__subscriptions__NodeState__authority_source_closure_generation(const jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key, u64 *out);
jz_btree jazz_node__node__query_eval__subscriptions__NodeState__snapshot_pending_authoritative_resets(const jazz_node_NodeState *self);
bool jazz_node__node__query_eval__subscriptions__NodeState__acknowledge_authoritative_reset(jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key, u64 generation);
bool jazz_node__node__query_eval__subscriptions__NodeState__publication_deferred_for_authority_result(const jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key);
bool jazz_node__node__query_eval__subscriptions__NodeState__opening_pending_for_authority_result(const jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key);
bool jazz_node__node__query_eval__subscriptions__NodeState__settled_through_for_authority_result(const jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key, jazz_types_GlobalTime *out);
typedef struct jazz_node_PolicyBindingParts { jazz_types_AuthorSubject identity; const jz_btree *claims; } jazz_node_PolicyBindingParts;
jz_err jazz_node__node__query_eval__subscriptions__NodeState__known_state_declaration_for_subscription(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_protocol_SubscriptionKey subscription, const groove_Value *values, u32 nvalues, jazz_types_AuthorSubject identity, const jazz_node_PolicyBindingParts *policy_binding, bool *has, jazz_protocol_KnownStateDeclaration *out);
jz_err jazz_node__node__query_eval__subscriptions__NodeState__binding_view_key_for_subscription(const jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, jazz_protocol_BindingViewKey *out);
const jazz_node_RegisteredBinding *jazz_node__node__query_eval__subscriptions__NodeState__unique_registered_binding_for_subscription(const jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription);


/* ---- query_eval::maintained_views ---- */
typedef struct jazz_node_CoveredInputSource {
    groove_InputSourceId id;
    const jz_str *table;                 /* Intern<String> */
    groove_RecordDescriptor *descriptor;
} jazz_node_CoveredInputSource;
void jazz_node_CoveredInputSource_drop(void *s);
/* Runtime-local state for the exact, policy-scoped source closure installed
 * into one receiver graph. */
typedef struct jazz_node_CoveredInputReceiver {
    jz_btree sources;                    /* GPTId -> CoveredInputSource */
    jazz_protocol_ReadViewSpec read_view;
    jazz_node_LocalAuthorityReconciliation local_authority;
    bool has_installed_closure; jazz_protocol_AuthorityResultKey installed_closure_key; u64 installed_closure_generation;
    bool has_installed_generation; u64 installed_generation;
    jz_btree installed_records;          /* GPTId -> BTreeMap<SupportingRow, Vec<u8>> */
} jazz_node_CoveredInputReceiver;
jazz_node_CoveredInputReceiver jazz_node__node__query_eval__maintained_views__CoveredInputReceiver__new(jz_btree sources, jazz_protocol_ReadViewSpec read_view);
bool jazz_node__node__query_eval__maintained_views__CoveredInputReceiver__is_empty(const jazz_node_CoveredInputReceiver *self);
void jazz_node_CoveredInputReceiver_drop(void *r);
/* A plan retained solely to keep a maintained subscription graph alive. */
typedef struct jazz_node_SubscriptionPreparedPlan {
    jazz_node_PreparedQueryPlanHandle plan;
    jazz_engine_QueryAuthorizationMode authorization_mode;
} jazz_node_SubscriptionPreparedPlan;
typedef struct jazz_node_LocalMaintainedViewSubscription {
    groove_MultisinkSubscription *subscription;
    bool has_retained_prepared_plan; jazz_node_SubscriptionPreparedPlan retained_prepared_plan;
    jazz_node_MaintainedSubscriptionView *maintained;
    jazz_node_MaintainedTerminalSchemas *terminal_schemas;
    jz_btree tables;                     /* String -> TableSchema */
    jazz_model_Query result_query;
    jz_str result_table;
    jazz_types_SchemaVersionId result_schema_version;
    bool has_result_select; jz_str_vec result_select;
    bool has_result_relation_projection; jazz_model_RelationProjectColumnVec result_relation_projection;
    bool has_result_relation_projections; jz_btree result_relation_projections; /* String -> Vec<RelationProjectColumn> */
    jz_btree result_set;                 /* BTreeSet<ResultMemberEntry> */
    jz_btree result_payloads;            /* ResultMemberEntry -> ResultMemberPayloadEntry */
    jz_btree program_facts;              /* BTreeSet<ProgramFactEntry> */
    jazz_types_OutputOccurrenceIdVec root_occurrence_ids;
    bool initial_received;
    jazz_node_CoveredInputReceiver covered_input_receiver;
} jazz_node_LocalMaintainedViewSubscription;
void jazz_node_LocalMaintainedViewSubscription_drop(void *l);
enum { JZN_LMVSU_FLAT = 0, JZN_LMVSU_ORDERED_WINDOW = 1, JZN_LMVSU_STRUCTURED = 2 };
typedef struct { jazz_types_OutputOccurrenceId occurrence; jazz_node_CurrentRow row; } jazz_node_OccurrenceRow;
typedef struct jazz_node_LocalMaintainedViewSubscriptionUpdate {
    u32 tag;
    JZ_VEC(jazz_node_OccurrenceRow) added;          /* Flat */
    jazz_types_OutputOccurrenceIdVec removed;      /* Flat */
    jazz_node_RelationSnapshot snapshot;           /* OrderedWindow */
    jazz_types_OutputOccurrenceIdVec occurrence_ids; /* OrderedWindow */
    groove_TerminalOperationVec terminal_operations; /* Structured */
} jazz_node_LocalMaintainedViewSubscriptionUpdate;
void jazz_node_LocalMaintainedViewSubscriptionUpdate_drop(void *u);
#define MV(name) jazz_node__node__query_eval__maintained_views__##name
jz_err MV(LocalMaintainedViewSubscription__decoded_terminal_records)(const jazz_node_LocalMaintainedViewSubscription *self, jz_btree *out);
const jazz_node_TerminalRootLayout *MV(LocalMaintainedViewSubscription__terminal_root_layout)(const jazz_node_LocalMaintainedViewSubscription *self);
bool MV(LocalMaintainedViewSubscription__has_root_collector)(const jazz_node_LocalMaintainedViewSubscription *self);
bool MV(LocalMaintainedViewSubscription__initial_snapshot_received)(const jazz_node_LocalMaintainedViewSubscription *self);
jz_err MV(LocalMaintainedViewSubscription__relation_projection_for_member)(const jazz_node_LocalMaintainedViewSubscription *self, const jazz_node_ResultMemberEntry *member, const jazz_model_RelationProjectColumnVec **out);
jz_err MV(LocalMaintainedViewSubscription__relation_projection_for_occurrence)(const jazz_node_LocalMaintainedViewSubscription *self, const jazz_types_OutputOccurrenceId *occurrence, const jazz_model_RelationProjectColumnVec **out);
u64 MV(LocalMaintainedViewSubscription__subscription_id)(const jazz_node_LocalMaintainedViewSubscription *self);
bool MV(LocalMaintainedViewSubscription__uses_logical_tables)(const jazz_node_LocalMaintainedViewSubscription *self, const jz_hmap *changed_logical_tables);
const jazz_types_OutputOccurrenceIdVec *MV(LocalMaintainedViewSubscription__root_occurrence_ids)(const jazz_node_LocalMaintainedViewSubscription *self);
bool MV(LocalMaintainedViewSubscription__has_covered_input_sources)(const jazz_node_LocalMaintainedViewSubscription *self);
bool MV(LocalMaintainedViewSubscription__has_installed_covered_closure)(const jazz_node_LocalMaintainedViewSubscription *self, const jazz_protocol_AuthorityResultKey *key, u64 generation);
jz_err MV(NodeState__open_maintained_view_subscription_in_authorization_mode)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_types_AuthorSubject identity, jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view, const jazz_node_SubscriptionPreparedPlan *retained, jazz_engine_QueryAuthorizationMode mode, jazz_node_LocalMaintainedViewSubscription *local_out, jazz_node_RelationSnapshot *snapshot_out);
jz_err MV(NodeState__open_maintained_view_subscription_in_authorization_mode_with_waker)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_types_AuthorSubject identity, jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view, const jazz_node_SubscriptionPreparedPlan *retained, jazz_engine_QueryAuthorizationMode mode, bool pending_overlay, const void *progress_waker, jazz_node_LocalMaintainedViewSubscription *local_out, jazz_node_RelationSnapshot *snapshot_out);
jz_err MV(NodeState__drain_local_maintained_view_subscription)(jazz_node_NodeState *self, jazz_node_LocalMaintainedViewSubscription *local, const jazz_protocol_AuthorityResultKey *key, bool *has, jazz_node_LocalMaintainedViewSubscriptionUpdate *out);
jz_err MV(NodeState__drain_local_maintained_view_subscription_with_waker)(jazz_node_NodeState *self, jazz_node_LocalMaintainedViewSubscription *local, const jazz_protocol_AuthorityResultKey *key, const void *waker, bool *has, jazz_node_LocalMaintainedViewSubscriptionUpdate *out);
jz_err MV(NodeState__drain_local_maintained_view_subscription_preserving_rows_with_waker)(jazz_node_NodeState *self, jazz_node_LocalMaintainedViewSubscription *local, const jazz_protocol_AuthorityResultKey *key, const jz_btree *preserved_row_keys, const void *waker, bool *has, jazz_node_LocalMaintainedViewSubscriptionUpdate *out, bool *suppressed);
jz_err MV(NodeState__drain_local_maintained_view_subscription_state_with_waker)(jazz_node_NodeState *self, jazz_node_LocalMaintainedViewSubscription *local, const jazz_protocol_AuthorityResultKey *key, const void *waker, bool *out);
jz_err MV(NodeState__install_opened_local_covered_receiver)(jazz_node_NodeState *self, jazz_node_LocalMaintainedViewSubscription *local, const jazz_protocol_AuthorityResultKey *key, const void *waker, bool *has, jazz_node_ResultTransitions *out);
bool MV(NodeState__local_maintained_authority_reconciliation_due)(const jazz_node_NodeState *self, const jazz_node_LocalMaintainedViewSubscription *local, const jazz_protocol_AuthorityResultKey *key);
bool MV(NodeState__covered_input_receiver_reconciliation_due)(const jazz_node_NodeState *self, const jazz_node_CoveredInputReceiver *receiver, const jazz_protocol_AuthorityResultKey *key);
bool MV(NodeState__covered_receiver_evaluation_pending)(const jazz_node_NodeState *self, const jazz_node_LocalMaintainedViewSubscription *local);
jz_err MV(NodeState__replace_local_maintained_covered_inputs)(jazz_node_NodeState *self, jazz_node_LocalMaintainedViewSubscription *local, const jazz_protocol_AuthorityResultKey *key, bool *out);
jz_err MV(NodeState__replace_covered_input_receiver)(jazz_node_NodeState *self, jazz_node_CoveredInputReceiver *receiver, jazz_types_SchemaVersionId schema, const jazz_protocol_AuthorityResultKey *key, bool *out);
jz_err MV(NodeState__apply_local_maintained_view_transitions_inner)(jazz_node_NodeState *self, jazz_node_LocalMaintainedViewSubscription *local, jazz_node_ResultTransitions transitions, bool materialize_update, jazz_node_LocalMaintainedViewSubscriptionUpdate *out);

/* ---- query_eval::materialization ---- */
#define MAT(name) jazz_node__node__query_eval__materialization__##name
typedef struct jazz_node_RelationSnapshotWindow { usize offset; bool has_limit; usize limit; } jazz_node_RelationSnapshotWindow;
typedef struct jazz_node_LocalMaintainedRelationSnapshot { jazz_node_RelationSnapshot snapshot; jazz_types_OutputOccurrenceIdVec root_occurrence_ids; } jazz_node_LocalMaintainedRelationSnapshot;
jz_err MAT(NodeState__project_relation_edge_through_read_schema)(jazz_node_NodeState *self, const jazz_protocol_RelationEdgeEntry *edge, jazz_types_SchemaVersionId read_schema, jazz_node_RelationEdge *out);
jz_err MAT(NodeState__resolve_relation_edge_version)(jazz_node_NodeState *self, const jz_str *table, jazz_types_RowUuid row, const jazz_protocol_RowVersionRefEntry *version_ref, jazz_node_VersionRow *out);
jz_err MAT(NodeState__resolve_relation_terminal_version)(jazz_node_NodeState *self, const jz_str *emitted_table, jazz_types_RowUuid row, const jazz_protocol_RowVersionRefEntry *version_ref, jazz_types_SchemaVersionId read_schema, jazz_node_VersionRow *out);
jz_err MAT(NodeState__materialize_historical_query_rows)(jazz_node_NodeState *self, jazz_model_TableSchema table, const groove_RecordDeltas *deltas, jazz_node_CurrentRowVec *out);
jz_err MAT(NodeState__materialize_include_deleted_query_rows)(jazz_node_NodeState *self, jazz_model_TableSchema table, const groove_RecordDeltas *deltas, jazz_node_CurrentRowVec *out);
jz_err MAT(NodeState__materialize_inline_current_query_rows)(jazz_node_NodeState *self, const jazz_model_TableSchema *table, const groove_RecordDeltas *deltas, jazz_node_CurrentRowVec *out);
jz_err MAT(NodeState__materialize_aggregate_query_rows)(jazz_node_NodeState *self, const jazz_model_Query *query, const jazz_engine_AppRowSchema *output, const groove_RecordDeltas *deltas, jazz_node_CurrentRowVec *out);
jz_err MAT(NodeState__materialize_local_maintained_view_result_member)(jazz_node_NodeState *self, const jazz_node_LocalMaintainedViewSubscription *local, const jazz_node_ResultMemberEntry *member, bool *has, jazz_node_CurrentRow *out);
jz_err MAT(NodeState__projected_current_row_from_materialized_version_in_read_schema)(jazz_node_NodeState *self, jazz_types_SchemaVersionId read_schema, const jazz_node_VersionRow *version, bool *has, jazz_node_CurrentRow *out);
jz_err MAT(NodeState__current_row_from_result_payload)(jazz_node_NodeState *self, const jazz_model_TableSchema *table, const jazz_protocol_ResultMemberPayloadEntry *payload, const jazz_node_ResultMembershipSchema *schema, jazz_node_CurrentRow *out);
jz_err MAT(NodeState__materialize_relation_snapshot_from_query_engine)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_protocol_ReadViewSpec *read_view, const groove_MultisinkDeltas *snapshots, const jazz_engine_QueryProgram *program, jazz_node_RelationSnapshot *out);
jz_err MAT(NodeState__materialize_relation_edge_target_row)(jazz_node_NodeState *self, const jazz_protocol_ReadViewSpec *read_view, jazz_types_SchemaVersionId read_schema, const jz_str *table, jazz_types_RowUuid row, jazz_types_TxTime time, jazz_types_NodeAlias node, jazz_node_CurrentRow *out);
void MAT(NodeState__relation_snapshot_no_order_windows)(const jazz_model_ArraySubquery *subqueries, u32 n, jz_btree *windows);
jz_err MAT(NodeState__bind_app_row_schema_fields)(jazz_node_CurrentRow *row, const jazz_engine_AppRowSchema *schema);
jz_err MAT(NodeState__bind_current_row_columns_in_schema)(const jazz_node_NodeState *self, jazz_types_SchemaVersionId schema, jazz_node_CurrentRow *row);
jz_err MAT(NodeState__finish_engine_query_rows_in_schema)(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_types_SchemaVersionId schema, jazz_node_CurrentRowVec *rows);
jz_err MAT(NodeState__query_output_table)(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_types_SchemaVersionId schema, jazz_model_TableSchema *out);
jz_err MAT(NodeState__sort_query_rows_with_occurrences)(const jazz_model_Query *query, const jazz_model_TableSchema *table, jazz_node_CurrentRowVec *rows, jazz_types_OutputOccurrenceIdVec *occurrences);
jz_err MAT(NodeState__apply_aggregate_window_with_occurrences)(const jazz_model_Query *query, jazz_node_CurrentRowVec *rows, jazz_types_OutputOccurrenceIdVec *occurrences);
jz_err MAT(NodeState__apply_query_order_in_schema)(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_types_SchemaVersionId schema, jazz_node_CurrentRow *rows, u32 n);
jz_err MAT(NodeState__apply_projection_in_schema)(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_types_SchemaVersionId schema, jazz_node_CurrentRow *rows, u32 n);
jz_err MAT(NodeState__relation_snapshot_has_materialized_required_cells)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_node_RelationSnapshot *snapshot, bool *out);
jz_err MAT(NodeState__materialize_local_maintained_relation_snapshot_with_occurrences)(jazz_node_NodeState *self, const jazz_node_LocalMaintainedViewSubscription *local, jazz_node_LocalMaintainedRelationSnapshot *out);


/* ---- query_eval::authorization ---- */
#define AUTHZ(name) jazz_node__node__query_eval__authorization__##name
typedef struct { jazz_model_ValidatedQuery shape; jazz_model_Binding binding; } jazz_node_ShapeBinding;
typedef JZ_VEC(jazz_node_ShapeBinding) jazz_node_ShapeBindingVec;
/* Exact, action-specific policy support compiled for a hypothetical operation. */
typedef struct jazz_node_AuthorizationSupportScope {
    jazz_protocol_AuthorizationSupportScopeKey key;
    jazz_protocol_AuthorizationOperationKey operation;
    jazz_protocol_RegisterShapeOptions options;
    jazz_node_ShapeBindingVec subscriptions;
} jazz_node_AuthorizationSupportScope;
void jazz_node_AuthorizationSupportScope_drop(void *s);
jz_err AUTHZ(NodeState__read_policy_query_allows_open_tx_row)(jazz_node_NodeState *self, jazz_model_OpenTransactionId tx_id, const jazz_model_Query *policy, jazz_types_SchemaVersionId policy_schema_version, jazz_types_RowUuid row_uuid, jazz_types_AuthorSubject identity, bool *out);
jz_err AUTHZ(NodeState__policy_authorization_row_id_graph)(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, jazz_node_PolicyAuthorizationGraph *out);
jz_err AUTHZ(NodeState__point_policy_authorization_row_id_graph)(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, jz_btree access_paths, bool has_bounded, jazz_engine_SourceId bounded_source, groove_GraphBuilder *bounded_graph, jazz_node_PolicyAuthorizationGraph *out);
jz_err AUTHZ(NodeState__write_policy_query_allows_current_row)(jazz_node_NodeState *self, const jazz_model_Query *policy, jazz_types_RowUuid row_uuid, jazz_types_AuthorSubject identity, bool *out);
jz_err AUTHZ(NodeState__write_policy_query_allows_candidate)(jazz_node_NodeState *self, const jazz_model_TableSchema *table, const jazz_model_Query *policy, jazz_types_RowUuid row_uuid, const jz_btree *cells, jazz_types_AuthorSubject identity, bool insert_candidate, bool *out);
jz_err AUTHZ(NodeState__write_policy_query_allows_candidate_for_schema)(jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jazz_model_TableSchema *table, const jazz_model_Query *policy, jazz_types_RowUuid row_uuid, const jz_btree *cells, jazz_types_AuthorSubject identity, bool insert_candidate, bool *out);
jz_err AUTHZ(NodeState__write_policy_query_allows_candidate_with_provenance_for_schema)(jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jazz_model_TableSchema *table, const jazz_model_Query *policy, jazz_types_RowUuid row_uuid, const jz_btree *cells, jazz_types_AuthorSubject identity, bool insert_candidate, jazz_model_RowProvenance provenance, bool *out);
jz_err AUTHZ(NodeState__write_policy_query_allows_candidate_over_transaction)(jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jazz_model_TableSchema *table, const jazz_model_Query *policy, jazz_types_RowUuid row_uuid, const jz_btree *cells, jazz_types_AuthorSubject identity, bool insert_candidate, jazz_model_RowProvenance provenance, const jazz_node_TransactionWriteOverlay *overlay, bool *out);
jz_err AUTHZ(NodeState__read_policy_query_allows_candidate_with_provenance_for_schema)(jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jazz_model_TableSchema *table, const jazz_model_Query *policy, jazz_types_RowUuid row_uuid, const jz_btree *cells, jazz_types_AuthorSubject identity, jazz_model_RowProvenance provenance, bool *out);
jz_err AUTHZ(NodeState__query_rows)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_model_DurabilityTier tier, jazz_node_CurrentRowVec *out);
jz_err AUTHZ(NodeState__compose_policy_filtered_current_source_graph)(jazz_node_NodeState *self, jz_err policy_request_err, jazz_engine_QueryProgramRequest policy_request, groove_GraphBuilder *base, const jz_str *output_fields, u32 n, jazz_node_PolicyAuthorizationGraph *out);
jz_err AUTHZ(NodeState__table_read_policy_authorization_request)(jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jz_str *table_name, jazz_types_AuthorSubject identity, jazz_node_ParamBindingMode mode, jazz_model_DurabilityTier tier, const jz_str *binding_source_shape, jz_btree binding_user_params, jz_btree binding_claim_params, jazz_engine_QueryProgramRequest *out);
jz_err AUTHZ(NodeState__table_read_policy_authorization_request_at)(const jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jz_str *table_name, jazz_types_AuthorSubject identity, jazz_node_ParamBindingMode mode, jazz_types_GlobalTime position, const jz_str *binding_source_shape, jz_btree binding_user_params, jz_btree binding_claim_params, jazz_engine_QueryProgramRequest *out);
jz_err AUTHZ(NodeState__table_read_policy_authorization_request_for_include_deleted)(jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jz_str *table_name, jazz_types_AuthorSubject identity, jazz_model_DurabilityTier tier, const jz_str *binding_source_shape, jz_btree binding_user_params, jz_btree binding_claim_params, jazz_engine_QueryProgramRequest *out);
jz_err AUTHZ(NodeState__maintained_view_content_current_with_version)(const jazz_node_NodeState *self, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, groove_GraphBuilder **out);
jz_err AUTHZ(NodeState__authorization_support_scope)(const jazz_node_NodeState *self, jazz_types_AuthorSubject writer, const jazz_protocol_PermissionAdviceAction *action, jazz_node_AuthorizationSupportScope *out);
jz_err AUTHZ(NodeState__authorization_support_scope_for_session)(const jazz_node_NodeState *self, jazz_types_AuthorSubject writer, const jz_btree *claims, const jazz_protocol_PermissionAdviceAction *action, jazz_node_AuthorizationSupportScope *out);
void AUTHZ(permission_scope_claim_values)(jazz_types_AuthorSubject writer, const jz_btree *claims, jz_btree *out);


/* ======================= node/state (state/ files) ======================= */
/* contribution_merge.rs */
jz_err jazz_node__node__NodeState__admit_contribution_merge_for_storage(const jazz_node_NodeState *self, const jazz_model_Transaction *tx, groove_Value *out);
jz_err jazz_node__node__NodeState__validate_contribution_merge_operation_identities(const jazz_node_NodeState *self, const jazz_model_Transaction *tx);
/* read_payload.rs */
jz_err jazz_node__node__NodeState__table(const jazz_node_NodeState *self, const jz_str *table, const jazz_model_TableSchema **out);
jz_err jazz_node__node__NodeState__table_in_schema(const jazz_node_NodeState *self, const jz_str *table, jazz_types_SchemaVersionId v, jazz_model_TableSchema *out);
jz_err jazz_node__node__NodeState__table_in_schema_ref(const jazz_node_NodeState *self, const jz_str *table, jazz_types_SchemaVersionId v, const jazz_model_TableSchema **out);
bool jazz_node__node__NodeState__shortest_lens_path_cached(jazz_node_NodeState *self, jazz_types_SchemaVersionId source, jazz_types_SchemaVersionId target, jazz_node_LensStepVec *out);
bool jazz_node__node__NodeState__shortest_lens_path(const jazz_node_NodeState *self, jazz_types_SchemaVersionId source, jazz_types_SchemaVersionId target, jazz_node_LensStepVec *out);
jz_err jazz_node__node__NodeState__compiled_lens_path(jazz_node_NodeState *self, jazz_types_SchemaVersionId source, jazz_types_SchemaVersionId target, const jz_str *table, bool *has_path, jazz_node_CompiledLensPath *out);
bool jazz_node__node__NodeState__node_for_alias(const jazz_node_NodeState *self, jazz_types_NodeAlias alias, jazz_types_NodeUuid *out);
jz_err jazz_node__node__NodeState__resolve_node_alias(jazz_node_NodeState *self, jazz_types_NodeAlias alias, bool *has, jazz_types_NodeUuid *out);
jz_err jazz_node__node__NodeState__version_tx_id(const jazz_node_NodeState *self, const jazz_node_VersionRow *version, jazz_model_TxId *out);
jz_err jazz_node__node__NodeState__version_record_from_row(jazz_node_NodeState *self, const jazz_node_VersionRow *version, jazz_protocol_VersionRecord **out);
jz_err jazz_node__node__NodeState__apply_row_version_payloads_for_requests(jazz_node_NodeState *self, const jazz_protocol_RowVersionRef *requests, u32 n_requests,
    jazz_protocol_VersionBundleVec version_bundles, jazz_protocol_VersionBundleVec *out);
jz_err jazz_node__node__NodeState__missing_known_state_row_version_refs(jazz_node_NodeState *self, const jazz_protocol_SyncMessage *message, jazz_protocol_RowVersionRefVec *out);
jz_err jazz_node__node__NodeState__inline_version_bundle_covers(jazz_node_NodeState *self, const jazz_protocol_RowVersionRef *request, jazz_types_SchemaVersionId result_schema_version,
    const jazz_protocol_RowVersionRefEntry *version, const jz_btree *incoming_versions, bool *out);
jz_err jazz_node__node__NodeState__local_supporting_row_version(jazz_node_NodeState *self, const jazz_protocol_SupportingRow *row, jazz_types_SchemaVersionId read_schema, const jz_str *table,
    bool *has, jazz_node_VersionRow *out);
jz_err jazz_node__node__NodeState__mint_tx_time(jazz_node_NodeState *self, u64 now_ms, jazz_types_TxTime *out);
void jazz_node__node__NodeState__merge_tx_time(jazz_node_NodeState *self, jazz_types_TxTime observed);
/* catalogue.rs (subset referenced before catalogue.c) */
bool jazz_node__node__NodeState__schema_version_for_alias(const jazz_node_NodeState *self, jazz_types_SchemaVersionAlias alias, jazz_types_SchemaVersionId *out);

/* durable.rs */
const jazz_node_SyncMetrics *jazz_node__node__NodeState__sync_metrics(const jazz_node_NodeState *self);
void jazz_node__node__NodeState__record_dropped_peer_request(jazz_node_NodeState *self);
void jazz_node__node__NodeState__record_transport_backpressure_retry(jazz_node_NodeState *self);
void jazz_node__node__NodeState__record_authoritative_reset_missing_payload_fallback(jazz_node_NodeState *self);
void jazz_node__node__NodeState__record_peer_payload_inventory_missing_fallback(jazz_node_NodeState *self);
const void *jazz_node__node__NodeState__query_engine_read_metrics(const jazz_node_NodeState *self);
void jazz_node__node__NodeState__reset_query_engine_read_metrics(jazz_node_NodeState *self);
const jz_btree *jazz_node__node__NodeState__catalogue_schemas(const jazz_node_NodeState *self);
const jazz_model_JazzSchema *jazz_node__node__NodeState__schema_with_active_permissions(const jazz_node_NodeState *self, jazz_types_SchemaVersionId id);
u64 jazz_node__node__NodeState__active_catalogue_seq(const jazz_node_NodeState *self);
const jz_btree *jazz_node__node__NodeState__catalogue_lenses(const jazz_node_NodeState *self);
u32 jazz_node__node__NodeState__catalogue_bootstrap_state(const jazz_node_NodeState *self);
jz_err jazz_node__node__NodeState__try_current_write_schema(const jazz_node_NodeState *self, jazz_protocol_CurrentWriteSchema *out);
jz_err jazz_node__node__NodeState__try_current_schema(const jazz_node_NodeState *self, const jazz_model_JazzSchema **out);
jz_err jazz_node__node__NodeState__require_catalogue_ready(const jazz_node_NodeState *self);
jz_err jazz_node__node__NodeState__current_write_schema(const jazz_node_NodeState *self, jazz_protocol_CurrentWriteSchema *out);
jz_err jazz_node__node__NodeState__catalogue_snapshot(const jazz_node_NodeState *self, jazz_protocol_CatalogueSnapshot *out);
jazz_node_HistoricalRead jazz_node__node__NodeState__at(jazz_node_NodeState *self, jazz_types_GlobalTime position);
jz_err jazz_node__node__NodeState__at_time(jazz_node_NodeState *self, jazz_types_TxTime time, jazz_node_HistoricalRead *out);
bool jazz_node__node__NodeState__is_history_complete_for(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, jazz_types_GlobalTime position);
bool jazz_node__node__NodeState__is_history_complete(const jazz_node_NodeState *self);
jz_err jazz_node__node__NodeState__subscription_current_rows(jazz_node_NodeState *self, const jz_str *table, jazz_model_DurabilityTier settled, jazz_node_CurrentRowVec *out);
bool jazz_node__node__NodeState__transaction_state(jazz_node_NodeState *self, jazz_model_TxId tx_id, jazz_model_Fate *fate, bool *has_global_time, jazz_types_GlobalTime *global_time, jazz_model_DurabilityTier *durability);
bool jazz_node__node__NodeState__transaction_record(jazz_node_NodeState *self, jazz_model_TxId tx_id, jazz_node_TransactionRecord *out);
jz_err jazz_node__node__NodeState__pending_transaction_ids_for(jazz_node_NodeState *self, jazz_types_NodeUuid node, jazz_types_AuthorSubject author, jazz_model_TxIdVec *out);
jz_err jazz_node__node__NodeState__pending_transaction_ids_for_author(jazz_node_NodeState *self, jazz_types_AuthorSubject author, jazz_model_TxIdVec *out);
jz_err jazz_node__node__NodeState__synchronizing_transaction_ids_for_author(jazz_node_NodeState *self, jazz_types_AuthorSubject author, jazz_model_TxIdVec *out);
jz_err jazz_node__node__NodeState__synchronizing_transaction_ids_for_node(jazz_node_NodeState *self, jazz_types_NodeUuid node, jazz_model_TxIdVec *out);
jz_err jazz_node__node__NodeState__below_global_transaction_ids(jazz_node_NodeState *self, bool has_author, jazz_types_AuthorSubject author, bool include_missing_authority_timestamp, jazz_model_TxIdVec *out);
jz_err jazz_node__node__NodeState__pending_transaction_scan_for(jazz_node_NodeState *self, jazz_types_NodeUuid node, jazz_types_AuthorSubject author, jazz_node_PendingTransactionScan *out);
jz_err jazz_node__node__NodeState__row_provenance(jazz_node_NodeState *self, const jazz_node_CurrentRow *row, bool *has, jazz_model_RowProvenance *out);
bool jazz_node__node__NodeState__current_row_tx_id(jazz_node_NodeState *self, const jazz_node_CurrentRow *row, jazz_model_TxId *out);
void jazz_node__node__NodeState__invalidate_subscription_scopes(jazz_node_NodeState *self);
jz_err jazz_node__node__NodeState__persist_policy_binding_directory(const jazz_node_NodeState *self, const jazz_protocol_PolicyBindingKey *policy);
jz_err jazz_node__node__NodeState__close(jazz_node_NodeState *self);
void jazz_node__node__NodeState__rejected_transactions(const jazz_node_NodeState *self, jazz_model_TxIdVec *out);
jazz_model_RejectedTransaction *jazz_node__node__NodeState__rejected_transaction(const jazz_node_NodeState *self, jazz_model_TxId tx_id);
jz_err jazz_node__node__NodeState__discard_rejection(jazz_node_NodeState *self, jazz_model_TxId tx_id);
jz_err jazz_node__node__NodeState__row_history(jazz_node_NodeState *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_node_HistoryEntryVec *out);
groove_Database *jazz_node__node__NodeState__into_database(jazz_node_NodeState *self);
bool jazz_node__node__NodeState__unsubscribe_groove_subscription(jazz_node_NodeState *self, groove_SubscriptionId subscription_id);
jz_err jazz_node__node__NodeState__drive_query_runtime(jazz_node_NodeState *self);
jz_err jazz_node__node__NodeState__drive_ready_query_runtime(jazz_node_NodeState *self);
jz_err jazz_node__node__NodeState__drive_ready_query_runtime_with_waker(jazz_node_NodeState *self, const jz_waker *progress_waker);
bool jazz_node__node__NodeState__has_pending_query_runtime(const jazz_node_NodeState *self);
bool jazz_node__node__NodeState__subscription_has_pending_query_runtime(const jazz_node_NodeState *self, groove_SubscriptionId subscription);
bool jazz_node__node__NodeState__subscription_has_pending_query_evaluation(const jazz_node_NodeState *self, groove_SubscriptionId subscription);
jz_err jazz_node__node__NodeState__set_initial_sync_flush_cadence(jazz_node_NodeState *self, u32 every);
jz_err jazz_node__node__NodeState__begin_initial_sync_flush_cadence(jazz_node_NodeState *self);
jz_err jazz_node__node__NodeState__finish_initial_sync_flush_cadence(jazz_node_NodeState *self);
jz_err jazz_node__node__NodeState__defer_catalogue_for_persistence(const jazz_node_NodeState *self, const jz_waker *waker, bool *out);
u64 jazz_node__node__NodeState__physical_identity_generation(const jazz_node_NodeState *self);
u64 jazz_node__node__NodeState__groove_runtime_token(const jazz_node_NodeState *self);
const groove_CommitMetrics *jazz_node__node__NodeState__last_commit_metrics(const jazz_node_NodeState *self);
const groove_TickMetrics *jazz_node__node__NodeState__last_tick_metrics(const jazz_node_NodeState *self);
groove_StorageReadMetrics jazz_node__node__NodeState__storage_read_metrics(const jazz_node_NodeState *self);
void jazz_node__node__NodeState__reset_storage_read_metrics(const jazz_node_NodeState *self);
groove_StorageReadMetrics jazz_node__node__NodeState__take_storage_read_metrics(const jazz_node_NodeState *self);
bool jazz_node__node__durable_author_matches(jazz_types_AuthorSubject requested, jazz_types_AuthorSubject stored);
/* commit.rs / catalogue.rs (forward) */
jz_err jazz_node__node__NodeState__current_rows(jazz_node_NodeState *self, const jz_str *table, jazz_model_DurabilityTier settled, jazz_node_CurrentRowVec *out);
jz_err jazz_node__node__NodeState__apply_and_persist_node_batch(jazz_node_NodeState *self, groove_DatabaseBatch *batch);

/* ---- state/catalogue.rs ---- */
jz_err jazz_node__node__NodeState__apply_node_batch(jazz_node_NodeState *self, groove_DatabaseBatch *batch, groove_AppliedBatch **out); /* state/commit.rs */
bool jazz_node__node__lineage_is_descendant(const jz_btree *parents, jazz_types_SchemaVersionId candidate, jazz_types_SchemaVersionId schema);
jazz_protocol_PhysicalIdentityManifestVec jazz_node__node__NodeState__physical_identity_history_for_candidate(const jazz_node_NodeState *self,
    jazz_types_SchemaVersionId candidate_schema, bool has_omit, jazz_types_SchemaLineagePublicationId omit);
jz_err jazz_node__node__NodeState__author_schema_lineage_publication(const jazz_node_NodeState *self, jazz_protocol_SchemaVersion schema, jazz_protocol_MigrationLens lens,
    jz_str_vec new_tables, jz_str_vec dropped_tables, jazz_protocol_SchemaLineagePublication *out);
jz_err jazz_node__node__NodeState__author_schema_from_predecessors(const jazz_node_NodeState *self, jazz_protocol_SchemaVersion schema,
    jazz_protocol_SchemaPredecessorVec predecessors, jazz_protocol_SchemaLineagePublication *out);
jz_err jazz_node__node__NodeState__predecessor_sources(const jazz_protocol_SchemaPredecessorVec *predecessors, const jz_btree *schemas, const jz_btree *mappings, jz_btree *out);
jz_err jazz_node__node__NodeState__validate_publication_sources(const jazz_protocol_SchemaLineagePublication *publication, const jz_btree *schemas,
    const jz_btree *mappings, jazz_protocol_PhysicalIdentityManifestVec history);
jz_err jazz_node__node__NodeState__reconcile_publication_mapping(const jazz_node_SchemaCatalogue *catalogue, const jazz_protocol_SchemaLineagePublication *publication,
    const jazz_node_SchemaPhysicalMapping *provisional, jazz_node_SchemaPhysicalMapping *out);
jz_err jazz_node__node__NodeState__persist_catalogue_schema(jazz_node_NodeState *self, const jazz_protocol_SchemaVersion *schema);
jz_err jazz_node__node__NodeState__hydrate_enum_case_mapping(jazz_node_NodeState *self, jazz_types_SchemaVersionId schema_version);
jz_err jazz_node__node__NodeState__ensure_provisional_physical_mapping(jazz_node_NodeState *self, jazz_types_SchemaVersionId schema_version);
jz_err jazz_node__node__NodeState__next_schema_version_alias(const jazz_node_NodeState *self, jazz_types_SchemaVersionAlias *out);
jz_err jazz_node__node__NodeState__allocate_physical_table_id(jazz_node_NodeState *self, jazz_types_PhysicalTableId *out);
jz_err jazz_node__node__NodeState__allocate_physical_column_id(jazz_node_NodeState *self, jazz_types_PhysicalColumnId *out);
jz_err jazz_node__node__NodeState__validate_pending_schema_lineage_physical_mapping(const jazz_node_NodeState *self, const jazz_node_PendingSchemaLineage *pending);
jz_err jazz_node__node__NodeState__validate_candidate_scalar_enum_capacity(const jazz_node_SchemaCatalogue *catalogue, const jazz_node_SchemaPhysicalMapping *candidate);
jz_err jazz_node__node__NodeState__reconcile_physical_mapping_for_lens(jazz_node_NodeState *self, const jazz_protocol_MigrationLens *lens, jazz_node_SchemaPhysicalMapping *out);
jz_err jazz_node__node__NodeState__reconcile_physical_mapping_for_lens_payload(const jazz_node_NodeState *self, const jazz_protocol_MigrationLens *lens,
    const jazz_protocol_SchemaVersion *target_schema_version, const jazz_node_SchemaPhysicalMapping *provisional_target_mapping, jazz_node_SchemaPhysicalMapping *out);
jz_err jazz_node__node__NodeState__reconcile_physical_mapping_for_lens_payload_in_catalogue(const jazz_node_SchemaCatalogue *catalogue,
    const jazz_protocol_MigrationLens *lens, const jazz_protocol_SchemaVersion *target_schema_version,
    const jazz_node_SchemaPhysicalMapping *provisional_target_mapping, jazz_node_SchemaPhysicalMapping *out);
jz_err jazz_node__node__NodeState__reconcile_source_physical_mapping_for_lens_payload(const jazz_protocol_MigrationLens *lens,
    const jazz_protocol_SchemaVersion *source_schema_version, const jazz_protocol_SchemaVersion *target_schema_version,
    const jazz_node_SchemaPhysicalMapping *provisional_source_mapping, const jazz_node_SchemaPhysicalMapping *target_mapping, jazz_node_SchemaPhysicalMapping *out);
jz_err jazz_node__node__NodeState__persist_catalogue_schema_lineage(jazz_node_NodeState *self, const jazz_node_StagedSchemaLineage *staged);
jz_err jazz_node__node__NodeState__persist_pending_schema_lineage(jazz_node_NodeState *self, const jazz_node_PendingSchemaLineage *pending);
jz_err jazz_node__node__NodeState__remove_pending_schema_lineage(jazz_node_NodeState *self, u64 catalogue_seq, jazz_types_SchemaLineagePublicationId publication_id);
jz_err jazz_node__node__NodeState__write_active_schema_lineage_to_batch(groove_DatabaseBatch *batch, const jazz_node_StagedSchemaLineage *staged);
jz_err jazz_node__node__NodeState__persist_catalogue_lens_with_physical_metadata(jazz_node_NodeState *self, const jazz_protocol_MigrationLens *lens, const jazz_node_SchemaPhysicalMapping *mapping);
jz_err jazz_node__node__NodeState__write_schema_version_mapping_to_batch(groove_DatabaseBatch *batch, jazz_types_SchemaVersionAlias alias,
    jazz_types_SchemaVersionId schema_version, const jazz_node_SchemaPhysicalMapping *mapping);
jz_err jazz_node__node__NodeState__write_active_schema_to_batch(groove_DatabaseBatch *batch, const jazz_node_ActiveSchema *active);
jz_err jazz_node__node__NodeState__write_active_schema_bootstrap_to_batch(const jazz_node_NodeState *self, groove_DatabaseBatch *batch, const jazz_node_ActiveSchema *active);
jz_err jazz_node__node__NodeState__persist_active_schema(jazz_node_NodeState *self, const jazz_node_ActiveSchema *active);
jz_err jazz_node__node__NodeState__activate_schema(jazz_node_NodeState *self, u64 revision, jazz_model_JazzSchema compiled);
jz_err jazz_node__node__NodeState__validate_schema_activation(const jazz_node_NodeState *self, u64 revision, jazz_model_JazzSchema compiled);
jz_err jazz_node__node__NodeState__prepare_active_schema(const jazz_node_NodeState *self, u64 revision, jazz_model_JazzSchema compiled, jazz_node_ActiveSchema *out);
void jazz_node__node__NodeState__install_active_schema(jazz_node_NodeState *self, jazz_node_ActiveSchema active);
jz_err jazz_node__node__NodeState__ensure_node_alias(jazz_node_NodeState *self, jazz_types_NodeUuid node_uuid, jazz_types_NodeAlias *out);
jz_err jazz_node__node__NodeState__ensure_schema_version_alias(jazz_node_NodeState *self, jazz_types_SchemaVersionId schema_version_id, jazz_types_SchemaVersionAlias *out);

/* ---- state/commit.rs ---- */
jz_err jazz_node__node__NodeState__ensure_large_value_stages_current(jazz_node_NodeState *self, const jz_btree *ids); /* state/lifecycle.rs */
jz_err jazz_node__node__NodeState__enforce_large_value_staging_policy(jazz_node_NodeState *self, const groove_StagedLargeValue *staged); /* state/lifecycle.rs */
jz_err jazz_node__node__NodeState__commit_mergeable(jazz_node_NodeState *self, jazz_node_MergeableCommit commit, jazz_node_PublishedTransaction *out);
jz_err jazz_node__node__NodeState__commit_mergeable_in_schema(jazz_node_NodeState *self, jazz_types_SchemaVersionId schema_version,
    jazz_node_MergeableCommit commit, jazz_node_PublishedTransaction *out);
jz_err jazz_node__node__NodeState__commit_mergeable_many(jazz_node_NodeState *self, jazz_node_MergeableCommitVec commits, jazz_node_PublishedTransaction *out);
jz_err jazz_node__node__NodeState__commit_mergeable_many_in_schema(jazz_node_NodeState *self, jazz_types_SchemaVersionId schema_version,
    jazz_node_MergeableCommitVec commits, jazz_node_PublishedTransaction *out);
jz_err jazz_node__node__NodeState__commit_mergeable_many_in_schema_at(jazz_node_NodeState *self, jazz_types_SchemaVersionId schema_version,
    jazz_node_MergeableCommitVec commits, jazz_model_TxId reserved, jazz_node_PublishedTransaction *out);
jz_err jazz_node__node__NodeState__commit_mergeable_at(jazz_node_NodeState *self, jazz_node_MergeableCommit commit, jazz_types_TxTime made_at, jazz_node_PublishedTransaction *out);
jz_err jazz_node__node__NodeState__commit_mergeable_many_at(jazz_node_NodeState *self, jazz_node_MergeableCommitVec commits, jazz_types_TxTime made_at, jazz_node_PublishedTransaction *out);
jz_err jazz_node__node__NodeState__commit_mergeable_many_at_with_schema_versions(jazz_node_NodeState *self, jazz_node_SchemaCommitVec commits,
    jazz_types_TxTime made_at, jazz_node_PublishedTransaction *out);
jz_err jazz_node__node__NodeState__commit_mergeable_many_at_with_schema_versions_and_provenance(jazz_node_NodeState *self, jazz_node_SchemaCommitVec commits,
    jazz_types_TxTime made_at, jazz_model_ContributionMergeProvenance *contribution_merge_in /* NULL = None, moved */, jazz_node_PublishedTransaction *out);
jz_err jazz_node__node__NodeState__prepare_and_stage_large_commit_values(jazz_node_NodeState *self, jazz_node_SchemaCommitVec *commits);
jz_err jazz_node__node__NodeState__prepare_and_stage_large_scalar(jazz_node_NodeState *self, groove_Value *value, u32 semantic_kind,
    bool *some, groove_StagedLargeValue *out);
jz_err jazz_node__node__NodeState__commit_mergeable_unit(jazz_node_NodeState *self, jazz_node_MergeableCommit commit,
    jazz_node_PublishedTransaction *published, jazz_protocol_SyncMessage **unit);
jz_err jazz_node__node__NodeState__resident_commit_unit(jazz_node_NodeState *self, jazz_model_TxId tx_id, jazz_protocol_SyncMessage **out);
jz_err jazz_node__node__NodeState__settle_published_transaction(jazz_node_NodeState *self, jazz_model_TxId tx_id, groove_PersistedBatch *persistence);
jz_err jazz_node__node__NodeState__persist_and_settle_transaction(jazz_node_NodeState *self, jazz_node_PublishedTransaction published, jazz_model_TxId *out);
jz_err jazz_node__node__NodeState__persist_and_settle_outcome(jazz_node_NodeState *self, jazz_node_PublicationOutcome outcome, void **out);
jz_err jazz_node__node__NodeState__commit_unit_for(jazz_node_NodeState *self, jazz_model_TxId tx_id, jazz_protocol_SyncMessage **out);
jz_err jazz_node__node__NodeState__visible_current_cells(jazz_node_NodeState *self, const jz_str *table, jazz_types_RowUuid row_uuid, bool *has, jz_btree *out);
jz_err jazz_node__node__NodeState__visible_current_cells_in_branch(jazz_node_NodeState *self, const jz_str *table, const jazz_model_BranchSelector *branch,
    jazz_types_RowUuid row_uuid, bool *has, jz_btree *out);
jz_err jazz_node__node__NodeState__visible_current_physical_cells_in_branch_schema(jazz_node_NodeState *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, const jazz_model_BranchSelector *branch, jazz_types_RowUuid row_uuid, bool *has, jz_btree *out);
jz_err jazz_node__node__NodeState__visible_current_physical_cells_and_winner_in_branch_schema(jazz_node_NodeState *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, const jazz_model_BranchSelector *branch, jazz_types_RowUuid row_uuid, bool *has, jz_btree *cells_out, jazz_model_TxId *tx_out);
jz_err jazz_node__node__NodeState__local_content_winner_tx_id_in_branch(jazz_node_NodeState *self, const jz_str *table, const jazz_model_BranchSelector *branch,
    jazz_types_RowUuid row_uuid, bool *has, jazz_model_TxId *out);
jz_err jazz_node__node__NodeState__local_deletion_winner_tx_id_in_branch(jazz_node_NodeState *self, const jz_str *table, const jazz_model_BranchSelector *branch,
    jazz_types_RowUuid row_uuid, bool *has, jazz_model_TxId *out);
jz_err jazz_node__node__NodeState__local_winner_in_branch_selector(jazz_node_NodeState *self, const jz_str *table, const jazz_model_BranchSelector *branch,
    jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out);
jz_err jazz_node__node__NodeState__local_content_winner_tx_id(jazz_node_NodeState *self, const jz_str *table, jazz_types_RowUuid row_uuid, bool *has, jazz_model_TxId *out);
jz_err jazz_node__node__NodeState__local_content_winner_tx_id_in_schema(jazz_node_NodeState *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, jazz_types_RowUuid row_uuid, bool *has, jazz_model_TxId *out);
jz_err jazz_node__node__NodeState__local_deletion_winner_tx_id(jazz_node_NodeState *self, const jz_str *table, jazz_types_RowUuid row_uuid, bool *has, jazz_model_TxId *out);
jz_err jazz_node__node__NodeState__local_deletion_winner_tx_id_in_schema(jazz_node_NodeState *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, jazz_types_RowUuid row_uuid, bool *has, jazz_model_TxId *out);
jz_err jazz_node__node__NodeState__rebuild_ahead_current_keys(jazz_node_NodeState *self);
void jazz_node__node__NodeState__mark_tx_touched_row(jazz_node_NodeState *self, jazz_types_PhysicalTableId table_id, const jazz_node_VersionRow *version);
jz_err jazz_node__node__NodeState__load_tx_touched_rows(const jazz_node_NodeState *self, const groove_DatabaseBatch *batch /* NULL = None */,
    jazz_types_TxTime tx_time, jazz_types_NodeAlias node_alias, jazz_node_TouchedRows *out);
bool jazz_node__node__NodeState__cached_tx_versions(const jazz_node_NodeState *self, jazz_model_TxId tx_id, jazz_node_VersionRowVec *out);
void jazz_node__node__NodeState__cache_tx_versions(jazz_node_NodeState *self, jazz_model_TxId tx_id, jazz_node_VersionRowVec versions);
void jazz_node__node__NodeState__invalidate_tx_version_tables_cache(jazz_node_NodeState *self, jazz_model_TxId tx_id);
jz_err jazz_node__node__NodeState__current_row_from_materialized_version(jazz_node_NodeState *self, const jazz_model_TableSchema *table,
    const jazz_node_VersionRow *version, jazz_node_CurrentRow *out);
jz_err jazz_node__node__NodeState__materialized_cells_for_version(const jazz_node_NodeState *self, const jazz_model_TableSchema *table,
    const jazz_node_VersionRow *version, jz_btree *out);
jz_err jazz_node__node__NodeState__local_current_row(jazz_node_NodeState *self, const jz_str *table, jazz_types_RowUuid row_uuid, bool *has, jazz_node_CurrentRow *out);
jz_err jazz_node__node__NodeState__current_physical_cell_in_schema(jazz_node_NodeState *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, jazz_types_RowUuid row_uuid, const jz_str *column, bool *has, groove_Value *out);
jz_err jazz_node__node__NodeState__current_physical_cells_in_schema(jazz_node_NodeState *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, jazz_types_RowUuid row_uuid, bool *has, jz_btree *out);
jz_err jazz_node__node__NodeState__current_physical_cells_and_winner_in_schema(jazz_node_NodeState *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, jazz_types_RowUuid row_uuid, bool *has, jz_btree *cells, jazz_model_TxId *winner);
jz_err jazz_node__node__NodeState__current_physical_cells_in_branch_schema(jazz_node_NodeState *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, const jazz_model_BranchSelector *branch, jazz_types_RowUuid row_uuid, bool *has, jz_btree *out);
jz_err jazz_node__node__NodeState__seal_large_value_updates(jazz_node_NodeState *self, jazz_node_MergeableCommit commit,
    const jazz_node_NamedStagedLargeValue *staged, u32 n_staged, jazz_types_SchemaVersionId schema_version, jazz_node_MergeableCommit *out);
jz_err jazz_node__node__NodeState__seal_inherited_large_values(jazz_node_NodeState *self, jazz_node_MergeableCommit commit,
    jazz_types_SchemaVersionId schema_version, bool allow_inherited_descriptors, jazz_node_MergeableCommit *out);
jz_err jazz_node__node__NodeState__local_current_row_in_schema(jazz_node_NodeState *self, const jz_str *table, jazz_types_RowUuid row_uuid,
    jazz_types_SchemaVersionId schema_version, bool *has, jazz_node_CurrentRow *out);
jz_err jazz_node__node__NodeState__local_current_content_row_candidate(jazz_node_NodeState *self, const jazz_model_TableSchema *table, jazz_types_RowUuid row_uuid,
    jazz_types_SchemaVersionId schema_version, bool *found, jazz_node_CurrentRow *row_out, jazz_types_TxTime *tx_time, jazz_types_NodeUuid *tx_node, bool *deleted_out);
jz_err jazz_node__node__NodeState__current_record_sort_key(const jazz_node_NodeState *self, const jz_str *table, jazz_types_RowUuid row_uuid,
    const groove_BorrowedRecord *record, jazz_types_TxTime *tx_time, jazz_types_NodeUuid *tx_node);
jz_err jazz_node__node__NodeState__malformed_current_query_error(const jz_str *table, jazz_types_RowUuid row_uuid, jz_err error);

/* ---- state/lifecycle.rs ---- */
void jazz_node__node__ActiveSessionClaimsScope__drop(jazz_node_ActiveSessionClaimsScope *scope);
jz_err jazz_node__node__NodeState__new(jazz_types_NodeUuid node_uuid, jazz_model_JazzSchema schema, jazz_node_BoxedStorage *storage, jazz_node_NodeState **out);
jz_err jazz_node__node__NodeState__new_client(jazz_types_NodeUuid node_uuid, jazz_model_JazzSchema requested_schema,
    jazz_node_BoxedStorage *storage, bool history_complete, jazz_node_NodeState **out);
jz_err jazz_node__node__NodeState__new_catalogue_uninitialized(jazz_types_NodeUuid node_uuid, jazz_node_BoxedStorage *storage, jazz_node_NodeState **out);
jz_err jazz_node__node__NodeState__discover_durable_catalogue_genesis(jazz_node_BoxedStorage *storage,
    jazz_node_BoxedStorage **storage_out, bool *has_schema, jazz_model_JazzSchema *schema_out);
jz_err jazz_node__node__NodeState__select_durable_reopen_schema(jazz_node_BoxedStorage *storage, jazz_model_JazzSchema requested,
    jazz_node_BoxedStorage **storage_out, jazz_model_JazzSchema *schema_out);
jz_err jazz_node__node__NodeState__new_history_complete(jazz_types_NodeUuid node_uuid, jazz_model_JazzSchema schema, jazz_node_BoxedStorage *storage, jazz_node_NodeState **out);
jz_err jazz_node__node__NodeState__reopen_in_place(jazz_node_NodeState *self, jazz_node_NodeState **out);
jz_err jazz_node__node__NodeState__new_with_history_complete(jazz_types_NodeUuid node_uuid, jazz_model_JazzSchema schema,
    jazz_node_BoxedStorage *storage, bool history_complete, jazz_node_NodeState **out);
jz_err jazz_node__node__NodeState__new_with_options(jazz_types_NodeUuid node_uuid, jazz_model_JazzSchema schema,
    jazz_node_BoxedStorage *storage, bool history_complete, jazz_node_NodeState **out);
jz_err jazz_node__node__NodeState__new_with_options_inner(jazz_types_NodeUuid node_uuid, jazz_model_JazzSchema schema,
    jazz_node_BoxedStorage *storage, bool history_complete, u32 catalogue_bootstrap_state, jazz_node_NodeState **out);
jz_err jazz_node__node__NodeState__open_full_database(const jazz_model_JazzSchema *schema, const jz_btree *catalogue_schemas,
    const jz_btree *schema_version_aliases, const jz_btree *physical_mappings, jazz_node_BoxedStorage *storage, groove_Database **out);
jazz_types_GlobalTime jazz_node__node__NodeState__committed_global_time(const jazz_node_NodeState *self);
jazz_types_NodeUuid jazz_node__node__NodeState__node_uuid(const jazz_node_NodeState *self);
jazz_types_TxTime jazz_node__node__NodeState__tx_time_high_water(const jazz_node_NodeState *self);
jazz_types_TxTime *jazz_node__node__NodeState__tx_time_reservation_clock(const jazz_node_NodeState *self);
void jazz_node__node__NodeState__seed_tx_time_high_water(jazz_node_NodeState *self, jazz_types_TxTime high_water);
jazz_model_DurabilityTier jazz_node__node__NodeState__authored_commit_durability(const jazz_node_NodeState *self);
void jazz_node__node__NodeState__set_non_durable_client(jazz_node_NodeState *self);
void jazz_node__node__NodeState__enable_authoritative_scalar_exit_refresh(jazz_node_NodeState *self);
jz_err jazz_node__node__NodeState__configure_scope_isolated_client_relay(jazz_node_NodeState *self, jazz_node_ClientRelayScope *scope);
const jazz_node_ClientRelayScope *jazz_node__node__NodeState__client_relay_scope(const jazz_node_NodeState *self);
void jazz_node__node__NodeState__set_session_claims(jazz_node_NodeState *self, jazz_types_AuthorSubject identity, jz_btree claims);
jazz_node_ActiveSessionClaimsScope jazz_node__node__NodeState__scoped_active_session_claims(jazz_node_NodeState *self, jazz_types_AuthorSubject identity, jz_btree claims);
jazz_node_ActiveSessionClaimsScope jazz_node__node__NodeState__scoped_optional_session_claims(jazz_node_NodeState *self, jazz_types_AuthorSubject identity,
    bool has_claims, jz_btree claims);
u64 jazz_node__node__NodeState__session_claim_revision(const jazz_node_NodeState *self, jazz_types_AuthorSubject identity);
jazz_node_SessionClaimsRevisionVec jazz_node__node__NodeState__session_claims_with_revisions(const jazz_node_NodeState *self);
void jazz_node__node__NodeState__session_claims_for(const jazz_node_NodeState *self, jazz_types_AuthorSubject identity, jz_btree *out);
void jazz_node__node__NodeState__set_permissions_ready(jazz_node_NodeState *self, bool ready);
bool jazz_node__node__NodeState__permissions_ready(const jazz_node_NodeState *self);
void jazz_node__node__NodeState__set_chunk_storage(jazz_node_NodeState *self, void *storage /* Rc<dyn ChunkStorage> */);
bool *jazz_node__node__NodeState__detach_covered_chunk_waits_handle(const jazz_node_NodeState *self);
void jazz_node__node__NodeState__set_missing_chunk_resolver(jazz_node_NodeState *self, void *resolver);
jz_err jazz_node__node__NodeState__local_chunk(const jazz_node_NodeState *self, groove_Locator locator, groove_ContentHash expected_hash, jz_bytes *out);
void *jazz_node__node__NodeState__local_chunk_reader_handle(const jazz_node_NodeState *self);
void jazz_node__node__NodeState__set_large_value_staging_policy(jazz_node_NodeState *self, jazz_node_LargeValueStagingPolicy policy);
bool jazz_node__node__NodeState__admit_large_value_ingress(jazz_node_NodeState *self, u64 encoded_bytes);
jz_err jazz_node__node__NodeState__current_staged_ids_for_descriptors(jazz_node_NodeState *self, const groove_LargeValueRefVec *descriptors,
    bool require_every_descriptor, groove_StagedLargeValueIdVec *out);
jz_err jazz_node__node__NodeState__require_staged_large_values_for_versions(jazz_node_NodeState *self, const jazz_protocol_VersionRecordVec *versions);
jz_err jazz_node__node__NodeState__evict_expired_staged_large_values(jazz_node_NodeState *self, usize *out);
jz_err jazz_node__node__NodeState__staged_large_value_ids(jazz_node_NodeState *self, groove_StagedLargeValueIdVec *out);
jz_err jazz_node__node__NodeState__evict_staged_large_value(jazz_node_NodeState *self, groove_StagedLargeValueId id, bool *out);
jz_err jazz_node__node__NodeState__stage_large_value_chunk_batch(jazz_node_NodeState *self, groove_StagedLargeValueId upload_id, u32 kind, groove_StagedChunkVec chunks);
jz_err jazz_node__node__NodeState__begin_streaming_large_value_upload(jazz_node_NodeState *self, groove_StagedLargeValueId upload_id, u32 kind);
jz_err jazz_node__node__NodeState__evict_pending_large_value_upload(jazz_node_NodeState *self, groove_StagedLargeValueId upload_id);
jz_err jazz_node__node__NodeState__finalize_large_value_upload(jazz_node_NodeState *self, groove_StagedLargeValueId upload_id, groove_LargeValueRef *value_ref,
    groove_StagedLargeValue *out);
jz_err jazz_node__node__NodeState__consolidate_and_stage_large_value(jazz_node_NodeState *self, groove_LargeValueRef *value, groove_StagedLargeValue *out);
jz_err jazz_node__node__NodeState__append_and_stage_large_value(jazz_node_NodeState *self, groove_LargeValueRef *value, jz_bytes bytes, groove_StagedLargeValue *out);
jz_err jazz_node__node__NodeState__edit_and_stage_large_value(jazz_node_NodeState *self, groove_LargeValueRef *value, u64 offset, u64 delete_length,
    jz_bytes insert_bytes, groove_StagedLargeValue *out);
jz_err jazz_node__node__NodeState__read_large_value_range(const jazz_node_NodeState *self, const groove_LargeValueRef *value, u64 start, u64 end, jz_bytes *out);
jz_err jazz_node__node__NodeState__read_large_text_utf16_range(const jazz_node_NodeState *self, const groove_LargeValueRef *value, u64 start, u64 end, jz_str *out);
jz_err jazz_node__node__NodeState__large_text_utf16_offset_to_byte(const jazz_node_NodeState *self, const groove_LargeValueRef *value, u64 offset, u64 *out);
jz_err jazz_node__node__NodeState__read_large_json_pointer(const jazz_node_NodeState *self, const groove_LargeValueRef *value, const jz_str *pointer,
    bool *some, jz_json *out);
jz_err jazz_node__node__NodeState__hydrate_large_value_cells(const jazz_node_NodeState *self, jz_btree *cells);
jz_err jazz_node__node__NodeState__hydrate_current_rows(const jazz_node_NodeState *self, jazz_node_CurrentRow *rows, u32 n);
jz_err jazz_node__node__NodeState__hydrate_encoded_record(const jazz_node_NodeState *self, const groove_RecordDescriptor *descriptor, jz_bytes *raw);
jz_err jazz_node__node__NodeState__rebuild_database_slot(jazz_node_NodeState *self);
void jazz_node__node__NodeState__invalidate_runtime_handles_after_database_rebuild(jazz_node_NodeState *self);
jz_err jazz_node__node__NodeState__open_catalogue_stage(jazz_model_JazzSchema schema, jazz_node_BoxedStorage *storage,
    u32 catalogue_bootstrap_state, jazz_node_CatalogueOpenState *out);
jz_err jazz_node__node__NodeState__validate_durable_staged_lineage(const jazz_node_StagedSchemaLineage *staged, const jz_btree *catalogue_schemas);
jz_err jazz_node__node__NodeState__validate_durable_physical_identity_bindings(const jz_btree *schemas, const jz_btree *mappings,
    const jz_btree *staged, const jz_btree *active, const jz_btree *pending, bool has_genesis, jazz_types_SchemaVersionId genesis, u32 bootstrap_state);

#endif
