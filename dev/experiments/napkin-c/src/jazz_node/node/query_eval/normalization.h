/* normalization.h: jazz_node::node::query_eval::normalization (unit node-qe-a). */
#ifndef JZ_NODE_QE_NORMALIZATION_H
#define JZ_NODE_QE_NORMALIZATION_H
#include "jz_rt.h"
#include "jazz_node/_ext_node-qe-a.h"
#include "jazz_node/node/query_eval/read_sources.h"

groove_StaticScanSpec jazz_node__node__query_eval__normalization__static_scan_for_prefix(groove_ValueVec prefix, u32 full_key_len);
jazz_engine_SourceId jazz_node__node__query_eval__normalization__root_source_id(const char *table, u32 len);
jz_err jazz_node__node__query_eval__normalization__root_literal_equalities(const jazz_model_Query *query, const jazz_model_Binding *binding, jz_btree *out);
jz_err jazz_node__node__query_eval__normalization__literal_equalities_for_filters(const jazz_model_PredicateVec *filters, const jazz_model_Binding *binding, jz_btree *out);
bool jazz_node__node__query_eval__normalization__select_current_access_path(const jazz_model_TableSchema *table, const jz_btree *equalities, jazz_node_CurrentAccessPath *out);
bool jazz_node__node__query_eval__normalization__select_composite_leading_equality_access_path(const jazz_model_TableSchema *table, const jz_btree *equalities, jz_slice covered, jazz_node_CurrentAccessPath *out);
jazz_engine_SourceId jazz_node__node__query_eval__normalization__reachable_edge_source_id(const jazz_model_ReachableVia *reachable, jz_slice reachable_id);
jazz_engine_SourceId jazz_node__node__query_eval__normalization__reachable_access_source_id(const jazz_model_ReachableVia *reachable, jz_slice reachable_id);
jazz_engine_SourceId jazz_node__node__query_eval__normalization__reachable_seed_source_id(const jazz_model_ReachableSeed *seed, jz_slice reachable_id);
jz_err jazz_node__node__query_eval__normalization__NodeState__normalized_row_set_shape(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_engine_NormalizedRowSetShape *out);
groove_Value jazz_node__node__query_eval__normalization__physical_current_index_value(const jazz_model_TableSchema *table, jz_slice column, groove_Value value);
jazz_engine_SourceId jazz_node__node__query_eval__normalization__nested_join_source_id(const jazz_model_JoinVia *join, const jz_str *path);
jazz_engine_SourceId jazz_node__node__query_eval__normalization__include_auxiliary_source_id(jz_str table, u32 include_index, u32 segment_index);
jazz_engine_SourceId jazz_node__node__query_eval__normalization__correlated_child_source_id(const jazz_engine_SourceId *owner, const jazz_model_ArraySubquery *sub, const u32 *path, u32 n);
jazz_engine_SourceId jazz_node__node__query_eval__normalization__implicit_reference_source_id(jz_slice table, jz_slice column);
jz_err jazz_node__node__query_eval__normalization__operand_column_type(const jazz_model_RuntimeSchema *schema, const jazz_engine_SourceId *source, const jazz_model_Operand *op, bool *has, groove_ColumnType *out);
jz_err jazz_node__node__query_eval__normalization__contains_needle_type(const jazz_model_RuntimeSchema *schema, const jazz_engine_SourceId *source, const jazz_model_Operand *op, bool *has, groove_ColumnType *out);
groove_Value jazz_node__node__query_eval__normalization__coerce_literal_for_column_type(groove_Value v, const groove_ColumnType *ty);
jz_err jazz_node__node__query_eval__normalization__table_schema(const jazz_model_RuntimeSchema *schema, jz_slice table, const jazz_model_TableSchema **out);
jz_err jazz_node__node__query_eval__normalization__schema_column_type(const jazz_model_RuntimeSchema *schema, const jz_str *table, const jz_str *column, groove_ColumnType *out);
jazz_engine_NormalizedValueRef jazz_node__node__query_eval__normalization__source_column_value(const jazz_engine_SourceId *source, jz_slice column, jazz_model_JoinTarget target);
jz_err jazz_node__node__query_eval__normalization__required_field_idx(groove_RecordDescriptor descriptor, jz_slice field, u32 *out);
/* CurrentQueryProgramOutput (query_eval.rs) */
enum { JZN_CQPO_APP_ROWS, JZN_CQPO_POLICY_PREDICATE, JZN_CQPO_AUTHORIZED_ROWS, JZN_CQPO_RELATION_SNAPSHOT, JZN_CQPO_MAINTAINED_VIEW };
jz_err jazz_node__node__query_eval__normalization__current_query_output_request(u32 output, const jazz_model_Query *query, jazz_engine_RowSetOutputRequest *out);
bool jazz_node__node__query_eval__normalization__storage_backed_maintained_view_eligible(const jazz_model_Query *query, jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view, const jazz_engine_NormalizedRowSetShape *normalized);
jz_err jazz_node__node__query_eval__normalization__normalize_enum_payload_predicate(groove_RecordDescriptor descriptor, const jazz_engine_SourceId *source, const jazz_model_Predicate *predicate, jazz_engine_PredicateExpr *out);
jz_err jazz_node__node__query_eval__normalization__NodeState__collect_policy_dependency_claim_params(const jazz_node_NodeState *self, const jazz_model_RuntimeSchema *schema, const jazz_engine_PolicyContext *policy, const jazz_engine_NormalizedRowSetShape *input, jz_btree *params);
jz_err jazz_node__node__query_eval__normalization__NodeState__normalized_include_deleted_row_set_shape(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_engine_NormalizedRowSetShape *out);
#endif
