/* lowering.h: types of jazz_node::node::query_eval::lowering (unit node-qe-a). */
#ifndef JZ_NODE_QE_LOWERING_H
#define JZ_NODE_QE_LOWERING_H
#include "jz_rt.h"
#include "jazz_node/_ext_node-qe-a.h"
#include "jazz_node/node/query_eval/read_sources.h"

/* ---- jazz_engine lowered-program mirrors (constructed / matched here) ---- */
typedef struct jazz_engine_ContentVersionFields { jz_str tx_time_field, tx_node_field; } jazz_engine_ContentVersionFields;
typedef struct jazz_engine_VersionIdentityFields {
    jz_str table_field, row_field, tx_time_field, tx_node_field;
    bool has_batch_id_field; jz_str batch_id_field;
    bool has_branch_or_prefix_field; jz_str branch_or_prefix_field;
    bool has_row_digest_field; jz_str row_digest_field;
    jz_str schema_field, layer_field;
} jazz_engine_VersionIdentityFields;
enum { JAZZ_RMV_CONTENT, JAZZ_RMV_CONTENT_OR_DELETION };
typedef struct jazz_engine_ResultMembershipVersionSchema {
    u32 tag; jazz_engine_ContentVersionFields content; jazz_engine_VersionIdentityFields deletion; jz_str deletion_state_field;
} jazz_engine_ResultMembershipVersionSchema;
typedef struct jazz_engine_QueryEngineRowRefSchema { jz_str source_field, table_field, row_field; } jazz_engine_QueryEngineRowRefSchema;
typedef struct jazz_engine_VersionedRowRefSchema {
    jazz_engine_QueryEngineRowRefSchema row; bool has_branch_or_prefix_field; jz_str branch_or_prefix_field;
    jazz_engine_ResultMembershipVersionSchema *version; /* Option */
} jazz_engine_VersionedRowRefSchema;
typedef struct jazz_engine_VersionWitnessSchema {
    jazz_engine_SourceId source; groove_RecordDescriptor descriptor; jazz_engine_VersionIdentityFields identity;
    jz_str created_by_field, created_at_field, updated_by_field, updated_at_field, authored_columns_field, deletion_field;
    jz_btree user_fields; /* String -> String */
} jazz_engine_VersionWitnessSchema;
typedef struct jazz_engine_VersionWitnessSchemas {
    jz_str role_field; jazz_engine_VersionWitnessSchema *content, *deletion; jz_btree routing_param_fields;
} jazz_engine_VersionWitnessSchemas;
typedef struct jazz_engine_AuthorizedRowsSchema { jz_str row_field; jz_btree routing_param_fields; } jazz_engine_AuthorizedRowsSchema;
typedef struct jazz_engine_PayloadField { jz_str name; groove_ValueType ty; } jazz_engine_PayloadField;
typedef struct jazz_engine_ResultMembershipSchema {
    jz_str table_field, row_field; jz_vec_str occurrence_id_fields; jz_btree occurrence_union_arm_fields; /* u32 -> String */
    JZ_VEC(jazz_engine_PayloadField) payload_fields; jz_btree payload_publication_fields;
    bool has_branch_or_prefix_field; jz_str branch_or_prefix_field;
    jazz_engine_ResultMembershipVersionSchema version;
    bool has_settle_position_field; jz_str settle_position_field;
    jz_btree routing_param_fields;
} jazz_engine_ResultMembershipSchema;
typedef struct jazz_engine_SyntheticRowSchema { jz_str table_field, row_field; } jazz_engine_SyntheticRowSchema;
typedef struct jazz_engine_AggregateResultFactSchema {
    jazz_engine_SyntheticRowSchema synthetic; groove_DescriptorFieldVec group_key_fields, value_fields; jz_btree routing_param_fields;
} jazz_engine_AggregateResultFactSchema;
typedef struct jazz_engine_RelationEdgesSchema {
    jazz_engine_VersionedRowRefSchema source, target; jz_str path_field, kind_field;
    bool has_depth_field; jz_str depth_field; bool has_edge_id_field; jz_str edge_id_field;
    bool has_branch_field; jz_str branch_field; bool has_role_field; jz_str role_field;
    bool has_order_field; jz_str order_field; bool has_hole_state_field; jz_str hole_state_field;
} jazz_engine_RelationEdgesSchema;
typedef struct jazz_engine_RoutedFactSchema { jazz_engine_SourceId source; bool complete; jz_btree routing_param_fields; } jazz_engine_RoutedFactSchema;
enum { JAZZ_PFS_AUTHORIZED_ROWS, JAZZ_PFS_RESULT_MEMBERSHIP, JAZZ_PFS_AGGREGATE_RESULT, JAZZ_PFS_RELATION_EDGES,
       JAZZ_PFS_PATH_CORRELATION_COVERAGE, JAZZ_PFS_PROGRAM_SOURCE_COVERAGE, JAZZ_PFS_READ_FRONTIER_SETTLED,
       JAZZ_PFS_COMPLETE_TX_PAYLOAD_COVERAGE, JAZZ_PFS_VIEW_COMPLETE_EXCLUSIVE_COVERAGE, JAZZ_PFS_VERSION_WITNESSES,
       JAZZ_PFS_REPLACEMENT_WITNESSES, JAZZ_PFS_POLICY_DECISION, JAZZ_PFS_POLICY_WITNESSES, JAZZ_PFS_CONTRIBUTING_MEMBERS,
       JAZZ_PFS_PREDICATE_READS, JAZZ_PFS_PREDICATE_OUTPUT_SET, JAZZ_PFS_POINT_READS };
typedef struct jazz_engine_ProgramFactSchema {
    u32 tag;
    jazz_engine_AuthorizedRowsSchema authorized_rows;
    jazz_engine_ResultMembershipSchema result_membership;
    jazz_engine_AggregateResultFactSchema aggregate_result;
    jazz_engine_RelationEdgesSchema relation_edges;
    jazz_engine_VersionWitnessSchemas witnesses;     /* VersionWitnesses / ReplacementWitnesses */
    jazz_engine_RoutedFactSchema routed;              /* ProgramSourceCoverage / ReadFrontierSettled */
    void *other;
} jazz_engine_ProgramFactSchema;
typedef struct jazz_engine_ProgramFactOutput { jazz_engine_ProgramFactKey key; u32 terminal; jazz_engine_ProgramFactSchema schema; } jazz_engine_ProgramFactOutput;
typedef struct jazz_engine_AppRowSchema { groove_RecordDescriptor descriptor; jz_vec_str hidden_fields; void *publication; } jazz_engine_AppRowSchema;
void jazz_engine_AppRowSchema_clone(jazz_engine_AppRowSchema *dst, const jazz_engine_AppRowSchema *src);
void jazz_engine_AppRowSchema_drop(jazz_engine_AppRowSchema *s);
enum { JAZZ_OTS_APP_ROWS, JAZZ_OTS_FACT };
typedef struct jazz_engine_OutputTerminalSchema { u32 tag; jazz_engine_AppRowSchema rows; jazz_engine_ProgramFactOutput fact; } jazz_engine_OutputTerminalSchema;
typedef struct jazz_engine_ProgramOutputSchemas { JZ_VEC(jazz_engine_OutputTerminalSchema) row_set; } jazz_engine_ProgramOutputSchemas;
typedef struct jazz_engine_LoweredTerminal { jz_str sink; groove_GraphBuilder *graph; jazz_engine_OutputTerminalSchema output; } jazz_engine_LoweredTerminal;
typedef struct jazz_engine_ParameterDomain { jz_btree user_params; /* String -> ColumnType */ jz_btree claim_params; /* String -> ProgramClaimParam */ jz_btree routing_params; /* set */ } jazz_engine_ParameterDomain;
typedef struct jazz_engine_LoweredProgram {
    jazz_engine_ProgramOutputSchemas output;
    JZ_VEC(jazz_engine_LoweredTerminal) terminals;
    groove_GraphBuilder *internal_app_rows_graph; /* Option */
    jazz_engine_ParameterDomain parameters;
    jz_btree targeted_refresh_tables; bool targeted_refresh_uncertain;
} jazz_engine_LoweredProgram;
struct jazz_engine_QueryProgram { jazz_engine_QueryProgramRequest request; jazz_engine_LoweredProgram lowered; void *explain; };
void jazz_engine_QueryProgram_clone(jazz_engine_QueryProgram *dst, const jazz_engine_QueryProgram *src);
void jazz_engine_QueryProgram_drop(void *p);
/* LoweredGraph::execution_terminals(): borrowed pointers in execution order */
const jazz_engine_LoweredTerminal **jazz_engine__query_engine__lowering__LoweredProgram__execution_terminals(const jazz_engine_LoweredProgram *l, u32 *n);
jz_str jazz_engine__query_engine__fields__route_param_field(jz_slice param);
extern const char JAZZ_APP_ROWS_SINK[];
typedef struct jazz_engine_QueryProgramCompilation jazz_engine_QueryProgramCompilation;
/* analyze: Err(report) is returned as its Debug string in *report */
bool jazz_engine__query_engine__QueryProgramCompilation__analyze(jazz_engine_QueryProgramRequest request, jazz_engine_QueryProgramCompilation **out, jz_str *report);
const jazz_engine_QueryProgramRequest *jazz_engine__query_engine__QueryProgramCompilation__request(const jazz_engine_QueryProgramCompilation *c);
const jazz_engine_SourceRequest *jazz_engine__query_engine__QueryProgramCompilation__sources(const jazz_engine_QueryProgramCompilation *c, u32 *n);
typedef struct jazz_engine_PreparedSources jazz_engine_PreparedSources;
typedef struct jazz_engine_CapabilityReport jazz_engine_CapabilityReport;
void jazz_engine_CapabilityReport_debug(jz_str *out, const jazz_engine_CapabilityReport *r);
void jazz_engine_CapabilityReport_free(jazz_engine_CapabilityReport *r);
bool jazz_engine__query_engine__prepare_query_program_sources(const jazz_engine_QueryProgramCompilation *c, void *resolver, const jazz_engine_SourceGraphPreparerVtbl *vt,
    jazz_engine_PreparedSources **sources, void **explain, jazz_engine_CapabilityReport **report);
typedef jz_err (*jazz_engine_describe_input_fn)(void *ctx, const groove_GraphBuilder *g, groove_RecordDescriptor *out);
bool jazz_engine__query_engine__QueryProgramTemplateCache__lower(void *cache, jazz_engine_QueryProgramCompilation *c, jazz_engine_PreparedSources *sources, void *explain,
    jazz_engine_describe_input_fn describe, void *ctx, jazz_engine_QueryProgram *out, jazz_engine_CapabilityReport **report);

/* ---- node types owned elsewhere but constructed here ---- */
enum { JZN_PQPS_USER = 0, JZN_PQPS_CLAIM = 1 };
typedef struct jazz_node_PreparedQueryParam { jz_str name; groove_ColumnType ty; u32 source_tag; jazz_engine_ClaimPath claim; } jazz_node_PreparedQueryParam;
typedef JZ_VEC(jazz_node_PreparedQueryParam) jazz_node_PreparedQueryParamVec;
void jazz_node_PreparedQueryParam_drop(void *p);
enum { JZN_PQP_GRAPH = 0, JZN_PQP_PREPARED = 1, JZN_PQP_PEER_MAINTAINED_MARKER = 2 };
struct jazz_node_PreparedQueryPlan {
    u32 tag; groove_GraphBuilder *graph; u64 shape; jazz_node_PreparedQueryParamVec params; jazz_engine_AppRowSchema output;
};
const jazz_engine_AppRowSchema *jazz_node__node__PreparedQueryPlan__app_row_schema(const jazz_node_PreparedQueryPlan *p);
typedef enum { JZN_PCBM_STRICT = 0, JZN_PCBM_FAIL_CLOSED_AUTHORIZATION_SUPPORT = 1 } jazz_node_PreparedClaimBindingMode;
jz_err jazz_node__node__query_eval__prepared_bindings__binding_values_for_plan(const jazz_model_Binding *binding, const jazz_node_PreparedQueryParam *params, u32 n,
    const jazz_engine_PolicyContext *policy, jazz_node_PreparedClaimBindingMode mode, groove_ValueVec *out);
jz_str jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_prepared_params(const jazz_node_PreparedQueryParam *params, u32 n);
bool jazz_node__node__query_eval__unavailable_inputs__local_unavailable_policy_binding(const jazz_engine_QueryProgramRequest *request, jazz_protocol_PolicyBindingKey *out);
void jazz_node__node__query_eval__route_discipline__NodeState__scope_program_binding_source(const jazz_node_NodeState *self, jazz_engine_QueryProgram *program);
jz_err jazz_node__node__query_eval__authorization__NodeState__policy_authorization_row_id_graph(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, jazz_node_PolicyAuthorizationGraph *out);
jz_err jazz_node__node__query_eval__authorization__NodeState__point_policy_authorization_row_id_graph(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, jz_btree access_paths,
    bool has_bounded_register, jazz_engine_SourceId bounded_source, groove_GraphBuilder *bounded_register, jazz_node_PolicyAuthorizationGraph *out);
bool jazz_types__debug_env__covered_input_trace(void);

/* groove::db::Database (concrete) */
typedef struct groove_RoutedMultisinkTerminal {
    jz_str sink; groove_GraphBuilder *graph; jz_vec_str route_fields; jz_vec_str public_fields; JZ_VEC(u32) route_value_indices;
} groove_RoutedMultisinkTerminal;
typedef JZ_VEC(groove_RoutedMultisinkTerminal) groove_RoutedMultisinkTerminalVec;
typedef struct groove_MultisinkSubscription groove_MultisinkSubscription;
typedef struct groove_RecordDeltas groove_RecordDeltas;
typedef struct groove_MultisinkDeltas groove_MultisinkDeltas;
typedef struct groove_Sink { jz_str sink; groove_GraphBuilder *graph; } groove_Sink;
typedef enum { GROOVE_SL_RETAINED, GROOVE_SL_FIRST_RESULT } groove_SubscriptionLifetime;
jz_err groove__db__Database__prepare(groove_Database *db, groove_RoutedMultisinkTerminalVec terminals, jz_str binding_source_shape, groove_RecordDescriptor binding_descriptor, u64 *shape);
jz_err groove__db__Database__prepare_shared(groove_Database *db, groove_RoutedMultisinkTerminalVec terminals, jz_str binding_source_shape, groove_RecordDescriptor binding_descriptor, u64 *shape);
jz_err groove__db__Database__bind_shape_with_lifetime_and_root_values(groove_Database *db, u64 shape, const groove_ValueVec *values, groove_SubscriptionLifetime lifetime,
    groove_RootIndirectValues root, void *waker, groove_MultisinkSubscription **out);
jz_err groove__db__Database__subscribe_with_lifetime_and_root_values(groove_Database *db, groove_Sink *sinks, u32 n, groove_SubscriptionLifetime lifetime, groove_RootIndirectValues root,
    void *waker, groove_MultisinkSubscription **out);
void groove__db__Database__unsubscribe(groove_Database *db, u64 subscription_id);
u64 groove__ivm__MultisinkSubscription__id(const groove_MultisinkSubscription *s);
void groove__db__Database__release_shared_prepared_shape(groove_Database *db, u64 shape);
jz_err groove__db__Database__retire_prepared_shape(groove_Database *db, u64 shape);
/* poll the subscription to completion of its initial update, driving Database progress */
enum { GROOVE_SE_UPDATE, GROOVE_SE_ERROR };
jz_err groove__db__Database__poll_initial_event(groove_Database *db, groove_MultisinkSubscription *s, u32 *tag, groove_MultisinkDeltas **deltas, jz_err *event_error);
jz_err groove__db__Database__describe_template_input(groove_Database *db, const groove_GraphBuilder *g, groove_RecordDescriptor *out);

/* ---- this file ---- */
struct jazz_node_PolicyAuthorizationGraph {
    groove_GraphBuilder *graph;
    jz_btree route_fields;  /* BTreeSet<String> */
    jz_btree access_paths;  /* SourceId -> CurrentAccessPath */
};
void jazz_node_PolicyAuthorizationGraph_drop(void *g);
void jazz_node_PolicyAuthorizationGraph_clone(void *dst, const void *src);
extern const jz_btree_desc jazz_node_PolicyAuthorizationGraphCache_desc; /* String -> PolicyAuthorizationGraph */
extern const jz_btree_desc jazz_node_PolicyAuthorizationGraphReplacements_desc; /* String -> Vec<ScopedPolicyAuthorizationGraphReplacement> */

typedef struct jazz_node_SupportedQueryProgram { u8 fingerprint[32]; jazz_engine_QueryProgram *program; /* Option */ } jazz_node_SupportedQueryProgram;
void jazz_node_SupportedQueryProgram_drop(void *p);
typedef struct jazz_node_ScopedPolicyAuthorizationGraphReplacement { bool has_previous; jazz_node_PolicyAuthorizationGraph previous; void *lease; /* Weak<()> */ } jazz_node_ScopedPolicyAuthorizationGraphReplacement;

#define JAZZ_ADMISSION_HANDOFF_MAX_PROGRAMS 128u

jz_err jazz_node__node__query_eval__lowering__app_row_terminal_route_eligible_fields(const jazz_engine_ProgramOutputSchemas *o, jz_vec_str *out);
jz_err jazz_node__node__query_eval__lowering__materialization_app_row_schema(const jazz_node_PreparedQueryPlan *plan, const jazz_engine_QueryProgram *program, jazz_engine_AppRowSchema *out);
jz_err jazz_node__node__query_eval__lowering__lowered_terminal_graph(const jazz_engine_QueryProgram *program, jz_slice sink, groove_GraphBuilder **out);
jz_err jazz_node__node__query_eval__lowering__lowered_app_rows_graph(const jazz_engine_QueryProgram *program, groove_GraphBuilder **out);
jz_err jazz_node__node__query_eval__lowering__lowered_materialization_app_rows_graph(const jazz_engine_QueryProgram *program, groove_GraphBuilder **out);
groove_Sink *jazz_node__node__query_eval__lowering__lowered_program_sinks(const jazz_engine_QueryProgram *program, u32 *n);
jazz_node_PreparedQueryParamVec jazz_node__node__query_eval__lowering__prepared_params_from_domain(const jazz_engine_ParameterDomain *parameters);
jz_vec_str jazz_node__node__query_eval__lowering__prepared_route_param_names(const jazz_engine_ParameterDomain *d);
jz_vec_str jazz_node__node__query_eval__lowering__terminal_route_fields(const jz_str *route_params, u32 nparams, const jz_str *eligible, u32 neligible);
jz_err jazz_node__node__query_eval__lowering__terminal_route_eligible_fields(const jazz_engine_OutputTerminalSchema *t, jz_vec_str *out);
jz_err jazz_node__node__query_eval__lowering__fact_public_fields(const jazz_engine_ProgramFactSchema *schema, jz_vec_str *out);
jz_str jazz_node__node__query_eval__lowering__policy_authorization_graph_cache_key(const jazz_engine_QueryProgramRequest *r);
jz_btree jazz_node__node__query_eval__lowering__output_routing_fields_for_query_eval(const jazz_engine_ProgramFactOutput *output);
jz_err jazz_node__node__query_eval__lowering__descriptor_field_names(groove_RecordDescriptor d, jz_vec_str *out);
jz_vec_str jazz_node__node__query_eval__lowering__versioned_row_ref_fields(const jazz_engine_VersionedRowRefSchema *schema);

jz_err jazz_node__node__query_eval__lowering__NodeState__ensure_query_program_request_supported(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, jz_btree access_paths);
jz_err jazz_node__node__query_eval__lowering__NodeState__compile_query_program_request(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, jazz_engine_QueryProgram *out);
jz_err jazz_node__node__query_eval__lowering__NodeState__compile_query_program_request_with_access_paths(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, jz_btree access_paths, jazz_engine_QueryProgram *out);
jz_err jazz_node__node__query_eval__lowering__NodeState__compile_query_program_request_with_inline_sources_and_access_paths(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request,
    jz_btree inline_sources, jz_btree access_paths, jazz_engine_QueryProgram *out);
jz_err jazz_node__node__query_eval__lowering__NodeState__compile_query_program_request_with_inline_sources_access_paths_and_covered_inputs(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request,
    jz_btree inline_sources, jz_btree access_paths, jz_btree covered_input_sources, jz_btree covered_input_descriptors, jazz_engine_QueryProgram *out);
jz_err jazz_node__node__query_eval__lowering__NodeState__compile_query_program_request_with_inline_sources_and_transaction_overlay(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request,
    jz_btree inline_sources, jz_btree access_paths, jazz_node_TransactionWriteOverlay transaction_overlay, jazz_engine_QueryProgram *out);
jz_err jazz_node__node__query_eval__lowering__NodeState__compile_query_program_request_with_shared_access_paths(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request,
    jz_btree access_paths, bool has_bounded, jazz_engine_SourceId bounded_source, groove_GraphBuilder *bounded_register, jazz_engine_QueryProgram *out);
jz_err jazz_node__node__query_eval__lowering__NodeState__compile_query_program_request_with_bounded_deletion_register(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request,
    jz_btree access_paths, jazz_engine_SourceId bounded_source, groove_GraphBuilder *bounded_register, jazz_engine_QueryProgram *out);
void jazz_node__node__query_eval__lowering__NodeState__restore_expired_policy_compilation_state(jazz_node_NodeState *self);
bool jazz_node__node__query_eval__lowering__NodeState__policy_authorization_graph_cache_get(jazz_node_NodeState *self, jz_slice cache_key, jazz_node_PolicyAuthorizationGraph *out);
jz_err jazz_node__node__query_eval__lowering__NodeState__prepared_query_plan_from_program(jazz_node_NodeState *self, const jazz_engine_QueryProgram *program,
    const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_node_PreparedQueryPlan *out);
jz_err jazz_node__node__query_eval__lowering__NodeState__subscribe_lowered_program_with_root_values(jazz_node_NodeState *self, jazz_engine_QueryProgram program, const jazz_model_Binding *binding,
    jz_str binding_source_shape, jazz_node_PreparedClaimBindingMode mode, void *progress_waker, groove_RootIndirectValues root_indirect_values, groove_MultisinkSubscription **out);
jz_err jazz_node__node__query_eval__lowering__NodeState__hydrate_lowered_program_once(jazz_node_NodeState *self, jazz_engine_QueryProgram program, const jazz_model_Binding *binding,
    groove_RootIndirectValues root_indirect_values, groove_RecordDeltas **out);
/* query_eval.rs */
jz_err jazz_node__node__query_eval__take_required_sink_deltas(groove_MultisinkDeltas *deltas, jz_slice sink, groove_RecordDeltas **out);
#endif
