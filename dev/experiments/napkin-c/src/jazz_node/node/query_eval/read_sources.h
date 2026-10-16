/* read_sources.h: types of jazz_node::node::query_eval::read_sources (unit node-qe-a). */
#ifndef JZ_NODE_QE_READ_SOURCES_H
#define JZ_NODE_QE_READ_SOURCES_H
#include "jz_rt.h"
#include "jazz_node/_ext_node-qe-a.h"

/* Result<_, SourceResolutionError>: NULL is Ok, otherwise the boxed error. */
typedef jazz_engine_SourceResolutionError *jz_sre;

/* CurrentIndexCandidateFilter */
typedef struct jazz_node_CurrentIndexCandidateFilter {
    jz_str table; jz_str column; bool has_order_column; jz_str order_column; groove_ValueVec prefix; jz_str source_column;
} jazz_node_CurrentIndexCandidateFilter;
void jazz_node_CurrentIndexCandidateFilter_drop(jazz_node_CurrentIndexCandidateFilter *f);
void jazz_node_CurrentIndexCandidateFilter_clone(jazz_node_CurrentIndexCandidateFilter *dst, const jazz_node_CurrentIndexCandidateFilter *src);
bool jazz_node_CurrentIndexCandidateFilter_eq(const jazz_node_CurrentIndexCandidateFilter *a, const jazz_node_CurrentIndexCandidateFilter *b);

/* (String, Vec<Value>) */
typedef struct jazz_node_IndexIntersection { jz_str column; groove_ValueVec values; } jazz_node_IndexIntersection;
typedef JZ_VEC(jazz_node_IndexIntersection) jazz_node_IndexIntersectionVec;
void jazz_node_IndexIntersection_drop(void *p);
void jazz_node_IndexIntersection_clone(void *dst, const void *src);

enum { JAZZ_CAP_PRIMARY_KEY, JAZZ_CAP_PRIMARY_KEY_PAGE, JAZZ_CAP_INDEX };
typedef struct jazz_node_CurrentAccessPath {
    u32 tag;
    groove_ValueVec prefix;            /* PrimaryKey(prefix) / Index.prefix */
    u32 cap;                           /* PrimaryKeyPage */
    jz_str column; bool has_order_column; jz_str order_column; bool reverse;
    jazz_node_IndexIntersectionVec intersections;
    jazz_node_CurrentIndexCandidateFilter *candidate_filter;
    bool maintained; bool has_source_limit; u32 source_limit;
} jazz_node_CurrentAccessPath;
void jazz_node_CurrentAccessPath_drop(void *p);
void jazz_node_CurrentAccessPath_clone(void *dst, const void *src);
bool jazz_node_CurrentAccessPath_eq(const jazz_node_CurrentAccessPath *a, const jazz_node_CurrentAccessPath *b);
void jazz_node_CurrentAccessPath_debug(jz_str *out, const jazz_node_CurrentAccessPath *p);
/* BTreeMap<SourceId, CurrentAccessPath> */
extern const jz_btree_desc jazz_node_AccessPaths_desc;
void jazz_node_AccessPaths_debug(jz_str *out, const jz_btree *paths);
void jazz_node_AccessPaths_clone(jz_btree *dst, const jz_btree *src);

typedef enum { JAZZ_HL_FIRST_RESULT, JAZZ_HL_RETAINED } jazz_node_HydrationLifetime;
typedef enum { JAZZ_APS_ORDINARY, JAZZ_APS_COMPOSITE_EQUALITY } jazz_node_AccessPathSelector;

/* TransactionOverlayTable: Arc'd, Mutex-protected caches */
typedef struct jazz_node_OverlayRecord { jazz_types_RowUuid row; jz_bytes record; } jazz_node_OverlayRecord;
typedef JZ_VEC(jazz_node_OverlayRecord) jazz_node_OverlayRecordVec; /* Arc<Vec<(RowUuid, Vec<u8>)>> payload */
typedef struct jazz_node_OverlayCacheEntry { groove_RecordDescriptor descriptor; jazz_node_OverlayRecordVec *records; /* rc */ } jazz_node_OverlayCacheEntry;
typedef struct jazz_node_OverlayCache { bool poisoned; JZ_VEC(jazz_node_OverlayCacheEntry) entries; } jazz_node_OverlayCache;
typedef struct jazz_node_TransactionOverlayTable {
    jz_btree rows; /* RowUuid -> Option<CurrentRow> (jazz_node_CurrentRow *) */
    jazz_node_OverlayCache live_records;
    jazz_node_OverlayCache written_records;
} jazz_node_TransactionOverlayTable;
typedef struct jazz_node_OverlayTableKey { jazz_types_SchemaVersionId schema; jz_str table; } jazz_node_OverlayTableKey;
typedef struct jazz_node_OverlayReads { bool poisoned; jz_btree set; /* set<(SchemaVersionId, String)> */ } jazz_node_OverlayReads;
typedef struct jazz_node_TransactionWriteOverlay {
    jz_btree *tables;           /* Arc<BTreeMap<(SchemaVersionId, String), Arc<TransactionOverlayTable>>> */
    bool has_excluded; jazz_types_SchemaVersionId excluded_schema; jz_str excluded_table; jazz_types_RowUuid excluded_row;
    jazz_node_OverlayReads *reads; /* Option<Arc<Mutex<..>>> */
    jazz_model_DurabilityTier evidence_tier;
} jazz_node_TransactionWriteOverlay;
void jazz_node_TransactionWriteOverlay_drop(jazz_node_TransactionWriteOverlay *o);
void jazz_node_TransactionWriteOverlay_clone(jazz_node_TransactionWriteOverlay *dst, const jazz_node_TransactionWriteOverlay *src);
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__Default__default(void);
jazz_node_TransactionOverlayTable *jazz_node__node__query_eval__read_sources__TransactionOverlayTable__new(jz_btree rows);
u32 jazz_node__node__query_eval__read_sources__TransactionOverlayTable__len(const jazz_node_TransactionOverlayTable *t);
bool jazz_node__node__query_eval__read_sources__TransactionOverlayTable__is_empty(const jazz_node_TransactionOverlayTable *t);
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__from_tables(jz_btree *tables);
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__accepted_state(void);
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__committed_view(const jazz_node_TransactionWriteOverlay *o);
jazz_model_DurabilityTier jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__evidence_tier(const jazz_node_TransactionWriteOverlay *o);
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__excluding(const jazz_node_TransactionWriteOverlay *o, jazz_types_SchemaVersionId schema, jz_slice table, jazz_types_RowUuid row);
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__recording(const jazz_node_TransactionWriteOverlay *o, jazz_node_OverlayReads *reads);
bool jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__is_active(const jazz_node_TransactionWriteOverlay *o);
u32 jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__rows_in(const jazz_node_TransactionWriteOverlay *o, const jz_btree *tables);

typedef struct jazz_node_CurrentSourceGraph { groove_GraphBuilder *graph; groove_RecordDescriptor descriptor; jz_btree metadata; } jazz_node_CurrentSourceGraph;

/* BranchLayerAccess */
enum { JAZZ_BLA_FULL, JAZZ_BLA_PRIMARY_KEY, JAZZ_BLA_INDEX };
typedef struct jazz_node_BranchLayerAccess {
    u32 tag; groove_ValueVec prefix; jz_str column; bool has_order_column; jz_str order_column;
    jazz_node_IndexIntersectionVec intersections; bool maintained;
} jazz_node_BranchLayerAccess;
void jazz_node_BranchLayerAccess_drop(jazz_node_BranchLayerAccess *a);

/* JazzSourceGraphPreparer<'a, S> (S erased: the node carries the storage vtable) */
typedef struct jazz_node_JazzSourceGraphPreparer {
    bool has_local_unavailable_scope; jazz_protocol_PolicyBindingKey local_unavailable_scope;
    jazz_node_NodeState *node;
    const jazz_engine_ReadView *read_view;
    jz_btree inline_sources;               /* SourceId -> Vec<CurrentRow> */
    jz_btree covered_input_sources;        /* SourceId -> GraphBuilder* */
    jz_btree covered_input_descriptors;    /* SourceId -> RecordDescriptor */
    jz_btree access_paths;                 /* SourceId -> CurrentAccessPath */
    bool has_bounded_deletion_register; jazz_engine_SourceId bounded_source; groove_GraphBuilder *bounded_register;
    bool count_access_path_metrics;
    jz_btree current_projection_targets;   /* SourceId -> String */
    bool policy_subplan;
    jazz_node_TransactionWriteOverlay transaction_overlay;
} jazz_node_JazzSourceGraphPreparer;
void jazz_node_JazzSourceGraphPreparer_drop(jazz_node_JazzSourceGraphPreparer *p);
extern const jazz_engine_SourceGraphPreparerVtbl jazz_node_JazzSourceGraphPreparer_SourceGraphPreparer_vtbl;
extern const jz_btree_desc jazz_node_SourceGraphs_desc;      /* SourceId -> GraphBuilder* */
extern const jz_btree_desc jazz_node_SourceDescriptors_desc; /* SourceId -> RecordDescriptor */
extern const jz_btree_desc jazz_node_SourceStrings_desc;     /* SourceId -> String */
extern const jz_btree_desc jazz_node_StrSet_desc;            /* BTreeSet<String> */
extern const jz_btree_desc jazz_node_StrStr_desc;            /* BTreeMap<String,String> */
extern const jz_btree_desc jazz_node_StrValue_desc;          /* BTreeMap<String,Value> */

/* trait impl entry (SourceGraphPreparer::prepare_source_graph) */
jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__SourceGraphPreparer__prepare_source_graph(
    jazz_node_JazzSourceGraphPreparer *self, const jazz_engine_SourceRequest *request, jazz_engine_ResolvedSource *out);
jz_err jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__policy_dependency_request(
    jazz_node_JazzSourceGraphPreparer *self, const jazz_engine_SourceRequest *request, bool *some, jazz_engine_QueryProgramRequest *out);
jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__current_projection_target(
    jazz_node_JazzSourceGraphPreparer *self, const jazz_engine_SourceRequest *request, const jazz_model_TableSchema *table, jz_str *out);
jz_btree jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__current_projection_required_fields(
    const jazz_node_JazzSourceGraphPreparer *self, const jazz_engine_SourceRequest *request, const jazz_model_TableSchema *table);
bool jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__can_use_bounded_historical_source(const jazz_node_JazzSourceGraphPreparer *self, jz_slice table);
bool jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__needs_projected_current_source(jazz_node_JazzSourceGraphPreparer *self, jz_slice table);

/* free functions shared with sibling modules */
groove_ProjectFieldVec jazz_node__node__query_eval__read_sources__register_storage_fields_for_query_engine(jz_slice prefix);
jz_vec_str jazz_node__node__query_eval__read_sources__register_storage_field_names(void);
bool jazz_node__node__query_eval__read_sources__capability_trace_enabled(void);
void jazz_node__node__query_eval__read_sources__trace_capability_compile(jazz_types_NodeUuid node_uuid, bool has_alias, u64 node_alias, const jazz_engine_QueryProgramRequest *request, const void *program, const void *report);
groove_RecordDescriptor jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields(const jazz_model_TableSchema *table, const jz_btree *metadata);
groove_RecordDescriptor jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields_for_current_storage(const jazz_model_TableSchema *table, const jz_btree *metadata);
jz_vec_str jazz_node__node__query_eval__read_sources__current_row_fields(const jazz_model_TableSchema *table);
jz_vec_str jazz_node__node__query_eval__read_sources__global_current_storage_fields(const jazz_model_TableSchema *table, bool include_deleted, bool include_global_time);
groove_GraphBuilder *jazz_node__node__query_eval__read_sources__empty_authorized_row_id_graph(void);
jz_btree jazz_node__node__query_eval__read_sources__inline_source_metadata(const jazz_engine_SourceRequirements *requirements, const char *branch_witness_field);
jz_btree jazz_node__node__query_eval__read_sources__covered_input_source_metadata(const jazz_engine_SourceRequirements *req, const jazz_model_TableSchema *table);
jz_err jazz_node__node__query_eval__read_sources__covered_input_record(const jazz_model_TableSchema *table, groove_RecordDescriptor descriptor, const jazz_node_CurrentRow *row, u32 schema_version_alias, const jazz_model_BranchKey *source_branch, jz_bytes *out);
jz_vec_str jazz_node__node__query_eval__read_sources__maintained_view_history_storage_field_names(const jazz_model_TableSchema *table);

/* NodeState impl (access paths) */
jz_err jazz_node__node__query_eval__read_sources__NodeState__query_program_access_paths(const jazz_node_NodeState *self, const jazz_engine_QueryProgramRequest *request, bool allow_secondary_indexes, jz_btree *out);
jz_err jazz_node__node__query_eval__read_sources__NodeState__one_shot_access_paths(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_model_DurabilityTier tier, jz_btree *out);
jz_err jazz_node__node__query_eval__read_sources__NodeState__current_query_hydration_access_paths(const jazz_node_NodeState *self, const jazz_engine_QueryProgramRequest *request, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_node_HydrationLifetime lifetime, jz_btree *out);
jz_err jazz_node__node__query_eval__read_sources__NodeState__current_query_primary_key_access_paths(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jz_btree *out);

#endif
