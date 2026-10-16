/* physical.h: jazz_node::node::physical (physical.rs + physical/{..}.rs, which
 * Rust include!s into one module). Types owned by the node-core-a unit. */
#ifndef JZ_NODE_PHYSICAL_H
#define JZ_NODE_PHYSICAL_H
#include "jz_rt.h"
#include "../_ext_node-core-a.h"

#define JZN_S(s) (int)(s).len, (s).ptr

/* ---- physical/catalogue.rs types ---- */
typedef struct jazz_node_GlobalScalarEnumCaseId {
    jazz_types_GlobalPhysicalEnumVariantId id;
    jazz_types_SchemaVersionId introducing_schema;
    u8 introducing_ordinal;
} jazz_node_GlobalScalarEnumCaseId;
typedef struct jazz_node_GlobalEnumCaseId {
    jazz_types_GlobalPhysicalEnumVariantId id;
    jazz_types_SchemaVersionId introducing_schema;
    u32 introducing_ordinal;
} jazz_node_GlobalEnumCaseId;
typedef JZ_VEC(jazz_node_GlobalScalarEnumCaseId) jazz_node_GlobalScalarEnumCaseIdVec;
typedef JZ_VEC(jazz_node_GlobalEnumCaseId) jazz_node_GlobalEnumCaseIdVec;

typedef struct jazz_node_PhysicalVariantCase {
    u32 tag;
    jz_btree fields; /* BTreeSet<String> */
} jazz_node_PhysicalVariantCase;
typedef JZ_VEC(jazz_node_PhysicalVariantCase) jazz_node_PhysicalVariantCaseVec;

typedef struct jazz_node_TablePhysicalMapping {
    jazz_types_PhysicalTableId table_id;
    jz_btree columns;                     /* String -> PhysicalColumnId */
    jazz_node_PhysicalVariantCaseVec variant_cases;
    jz_btree scalar_enum_cases;           /* PhysicalColumnId -> Vec<GlobalScalarEnumCaseId> */
    jz_btree payload_enum_cases;          /* PhysicalColumnId -> Vec<GlobalEnumCaseId> */
    jz_btree nested_scalar_enum_cases;    /* PhysicalColumnId -> BTreeMap<String, Vec<GlobalScalarEnumCaseId>> */
    jz_btree nested_payload_enum_cases;   /* PhysicalColumnId -> BTreeMap<String, Vec<GlobalEnumCaseId>> */
} jazz_node_TablePhysicalMapping;

struct jazz_node_SchemaPhysicalMapping {
    jazz_protocol_PhysicalIdentityManifest identities;
    jz_btree tables; /* String -> TablePhysicalMapping */
};

typedef struct jazz_node_PhysicalHistoryBinding {
    jz_str storage_table;
    groove_RecordDescriptor descriptor;
} jazz_node_PhysicalHistoryBinding;

/* CurrentWinnerCellProjection */
enum { JZN_CWCP_FIELD, JZN_CWCP_LITERAL, JZN_CWCP_NULL };
typedef struct jazz_node_CurrentWinnerCellProjection {
    u32 tag;
    jz_str name;
    jazz_types_PhysicalColumnId column_id;
    groove_ValueType column_type;
    groove_Value literal;
} jazz_node_CurrentWinnerCellProjection;

typedef groove_RecursiveEnumRemaps jazz_node_EnumOccurrenceRemaps;
typedef JZ_VEC(jazz_node_EnumOccurrenceRemaps) jazz_node_EnumOccurrenceRemapsVec;

/* ---- physical.rs types ---- */
typedef enum { JZN_PWT_HISTORY = 0, JZN_PWT_GLOBAL_CURRENT = 1, JZN_PWT_AHEAD_CURRENT = 2 } jazz_node_PhysicalWriteTarget;
typedef enum { JZN_PCC_GLOBAL = 0, JZN_PCC_AHEAD = 1 } jazz_node_PhysicalCurrentClass;
typedef enum { JZN_CPS_HISTORY = 0, JZN_CPS_CURRENT = 1 } jazz_node_ContentProjectionShape;
enum {
    JZN_PWF_COPY, JZN_PWF_DECODE, JZN_PWF_ENUM, JZN_PWF_CREATED_AT_MILLIS, JZN_PWF_UPDATED_AT_MILLIS,
    JZN_PWF_UPDATED_BY, JZN_PWF_HISTORY_UPDATED_BY, JZN_PWF_GLOBAL_TIME
};
typedef struct jazz_node_PhysicalWriteField { u32 tag; u32 source; u32 column; } jazz_node_PhysicalWriteField;

struct jazz_node_PreparedPhysicalWritePlan {
    jz_str storage_table;
    jazz_model_TableSchema *source_table;   /* Arc<TableSchema> */
    groove_RecordDescriptor logical_descriptor;
    groove_RecordDescriptor physical_descriptor;
    groove_RecordDescriptor history_descriptor;
    JZ_VEC(jazz_node_PhysicalWriteField) write_fields;
    jazz_node_EnumOccurrenceRemapsVec enum_remaps;
};

/* ---- btree descriptors shared by the physical module ---- */
extern const jz_btree_desc jzn_desc_str_set;            /* BTreeSet<String> */
extern const jz_btree_desc jzn_desc_str_u64;            /* String -> u64 */
extern const jz_btree_desc jzn_desc_u64_set;            /* BTreeSet<u64> */
extern const jz_btree_desc jzn_desc_u64_podvec;         /* u64 -> Vec<POD> */
extern const jz_btree_desc jzn_desc_str_podvec;         /* String -> Vec<POD> */
extern const jz_btree_desc jzn_desc_u64_strpodvec_map;  /* u64 -> BTreeMap<String, Vec<POD>> */
extern const jz_btree_desc jzn_desc_str_table_mapping;  /* String -> TablePhysicalMapping */
extern const jz_btree_desc jzn_desc_uuid_schema_mapping;/* SchemaVersionId -> SchemaPhysicalMapping */
extern const jz_btree_desc jzn_desc_gse_set;            /* BTreeSet<GlobalScalarEnumCaseId> (by id) */
extern const jz_btree_desc jzn_desc_ge_set;             /* BTreeSet<GlobalEnumCaseId> (by id) */
extern const jz_btree_desc jzn_desc_u32_uuid;           /* u32 -> SchemaVersionId */
extern const jz_btree_desc jzn_desc_u32_strvec;         /* u32 -> Vec<String> */
extern const jz_btree_desc jzn_desc_str_cwcp;           /* String -> CurrentWinnerCellProjection */
extern const jz_btree_desc jzn_desc_str_str;            /* String -> String */
extern const jz_btree_desc jzn_desc_u64_str;            /* u64 -> String */

void jazz_node_PhysicalVariantCase_clone(void *dst, const void *src);
void jazz_node_PhysicalVariantCase_drop(void *p);
void jazz_node_TablePhysicalMapping_clone(void *dst, const void *src);
void jazz_node_TablePhysicalMapping_drop(void *p);
void jazz_node_TablePhysicalMapping_init(jazz_node_TablePhysicalMapping *m, jazz_types_PhysicalTableId id);
void jazz_node_SchemaPhysicalMapping_clone(void *dst, const void *src);
void jazz_node_SchemaPhysicalMapping_drop(void *p);
bool jazz_node_SchemaPhysicalMapping_eq(const jazz_node_SchemaPhysicalMapping *a, const jazz_node_SchemaPhysicalMapping *b);
void jazz_node_PreparedPhysicalWritePlan_drop(void *p);
void jazz_node_CurrentWinnerCellProjection_clone(void *dst, const void *src);
void jazz_node_CurrentWinnerCellProjection_drop(void *p);
void jzn_str_vec_drop(void *v);
void jzn_str_vec_clone(void *dst, const void *src);
void jzn_pod_vec_drop(void *v);
int jazz_node_GlobalScalarEnumCaseId_cmp(const void *a, const void *b, void *ctx);
int jazz_node_GlobalEnumCaseId_cmp(const void *a, const void *b, void *ctx);

/* ---- physical/catalogue.rs ---- */
jz_err jazz_node__node__physical__groove_variant_tag(jazz_types_SchemaVersionAlias alias, u32 *out);
bool jazz_node__node__physical__physical_mapping_has_enum_boundary(const jazz_node_TablePhysicalMapping *m, jazz_types_PhysicalColumnId c);
bool jazz_node__node__physical__value_type_has_enum_boundary(const groove_ValueType *t);
jz_err jazz_node__node__physical__bootstrap_copy_enum_remaps(const groove_ValueType *source, const groove_ValueType *target,
    const jz_str *path, jazz_node_EnumOccurrenceRemaps *remaps);
jz_str jazz_node__node__physical__physical_scalar_enum_case_name(const jazz_node_GlobalScalarEnumCaseId *c);
jz_str jazz_node__node__physical__physical_enum_case_name(const jazz_node_GlobalEnumCaseId *c);
int jazz_node__node__physical__compare_scalar_enum_cases(const jz_btree *aliases, const jazz_node_GlobalScalarEnumCaseId *l, const jazz_node_GlobalScalarEnumCaseId *r);
int jazz_node__node__physical__compare_global_enum_cases(const jz_btree *aliases, const jazz_node_GlobalEnumCaseId *l, const jazz_node_GlobalEnumCaseId *r);
jz_err jazz_node__node__physical__physical_scalar_enum_schema(jazz_types_PhysicalColumnId c, const jazz_node_GlobalScalarEnumCaseId *cases, u32 n, groove_ScalarEnumSchema *out);
jz_err jazz_node__node__physical__physical_nested_enum_value_type(const groove_ValueType *t, const jz_str *path,
    const jz_btree *scalar_registries, const jz_btree *payload_registries, const jz_btree *payload_layouts,
    jazz_types_PhysicalColumnId column_id, groove_ValueType *out);
jz_err jazz_node__node__physical__collect_nested_payload_enum_layouts(const groove_ValueType *t, const jz_str *path,
    const jz_btree *identities, jz_btree *output);
jz_str jazz_node__node__physical__physical_history_table_name(jazz_types_PhysicalTableId id);
bool jazz_node__node__physical__split_physical_table_name(jz_slice name, jazz_types_PhysicalTableId *id, jz_slice *suffix);
bool jazz_node__node__physical__physical_version_table_id(jz_slice name, jazz_types_PhysicalTableId *out);
jz_str jazz_node__node__physical__physical_global_current_table_name(jazz_types_PhysicalTableId id);
jz_str jazz_node__node__physical__physical_ahead_current_table_name(jazz_types_PhysicalTableId id);
jz_str jazz_node__node__physical__physical_rejected_versions_table_name(jazz_types_PhysicalTableId id);
jz_str jazz_node__node__physical__physical_current_index_name(jazz_types_PhysicalColumnId id);
jz_str jazz_node__node__physical__physical_current_composite_index_name(const jazz_types_PhysicalColumnId *ids, u32 n);
jz_str jazz_node__node__physical__physical_user_column_field(jazz_types_PhysicalColumnId id);
jz_str jazz_node__node__physical__physical_history_projection_target(jazz_types_SchemaVersionAlias a, const jz_str *table);
jz_str jazz_node__node__physical__physical_current_projection_target(jazz_types_SchemaVersionAlias a, const jz_str *table);
/* layout key types used by the layout maps */
typedef struct jazz_node_PathCaseKey { jz_str path; jazz_node_GlobalEnumCaseId identity; } jazz_node_PathCaseKey;
extern const jz_btree_desc jzn_desc_pathcase_desc;  /* (String, GlobalEnumCaseId) -> RecordDescriptor */

/* ---- physical.rs (names.rs) ---- */
jz_str jazz_node__node__physical__physical_current_winner_projection_target(jazz_types_PhysicalTableId id, const jz_str *fields, u32 n);
jz_str jazz_node__node__physical__physical_current_projection_target_for_enum_columns(jazz_types_SchemaVersionAlias a, const jz_str *table, const jz_btree *enum_columns);
groove_RecordDescriptor jazz_node__node__physical__authored_history_projection_descriptor(const jazz_model_TableSchema *t);

/* ---- physical/bindings.rs ---- */
jz_err jazz_node__node__physical__allocate_provisional_physical_mapping(const jazz_model_JazzSchema *schema,
    jazz_protocol_PhysicalIdentityManifest identities, u64 *next_table_id, u64 *next_column_id, jazz_node_SchemaPhysicalMapping *out);
jz_err jazz_node__node__physical__allocate_physical_variant_cases(jz_btree *mappings, const jz_btree *aliases,
    jazz_types_SchemaVersionId schema_version, const jz_str *logical_table, jz_btree fields, jazz_node_PhysicalVariantCaseVec *out);
jz_err jazz_node__node__physical__validate_physical_variant_cases(const jz_btree *mappings, const jz_btree *aliases);
jz_err jazz_node__node__physical__validate_physical_mapping_registries(const jz_btree *mappings, const jz_btree *aliases);
jz_err jazz_node__node__physical__validate_payload_enum_case_provenance(const jz_btree *mappings, const jz_btree *aliases);
jz_err jazz_node__node__physical__validate_scalar_enum_case_provenance(const jz_btree *mappings, const jz_btree *aliases);
jz_err jazz_node__node__physical__physical_history_source_table(const jz_btree *schemas, const jz_btree *aliases,
    const jz_btree *mappings, jazz_types_SchemaVersionId v, const jz_str *table, jz_str *out);
jz_err jazz_node__node__physical__physical_history_storage_table(const jz_btree *mappings, jazz_types_SchemaVersionId v, const jz_str *table, jz_str *out);
jz_err jazz_node__node__physical__physical_current_source_table(const jz_btree *schemas, const jz_btree *mappings,
    jazz_types_SchemaVersionId v, const jz_str *table, jazz_node_PhysicalCurrentClass cls, jz_str *out);
jz_err jazz_node__node__physical__physical_current_storage_table(const jz_btree *mappings, jazz_types_SchemaVersionId v,
    const jz_str *table, jazz_node_PhysicalCurrentClass cls, jz_str *out);
jz_err jazz_node__node__physical__physical_rejected_version_binding(const jz_btree *schemas, const jz_btree *mappings,
    jazz_types_SchemaVersionId v, const jz_str *table, jazz_node_PhysicalHistoryBinding *out);

/* ---- physical/enum_lookup.rs ---- */
jz_err jazz_node__node__physical__NodeState__physical_scalar_enum_cases(const jazz_node_NodeState *s,
    jazz_types_PhysicalTableId t, jazz_types_PhysicalColumnId c, jazz_node_GlobalScalarEnumCaseIdVec *out);
jz_err jazz_node__node__physical__NodeState__physical_payload_enum_cases(const jazz_node_NodeState *s,
    jazz_types_PhysicalTableId t, jazz_types_PhysicalColumnId c, jazz_node_GlobalEnumCaseIdVec *out);
jz_err jazz_node__node__physical__NodeState__physical_nested_scalar_enum_cases(const jazz_node_NodeState *s,
    jazz_types_PhysicalTableId t, jazz_types_PhysicalColumnId c, const jz_str *path, jazz_node_GlobalScalarEnumCaseIdVec *out);
jz_err jazz_node__node__physical__NodeState__physical_nested_payload_enum_cases(const jazz_node_NodeState *s,
    jazz_types_PhysicalTableId t, jazz_types_PhysicalColumnId c, const jz_str *path, jazz_node_GlobalEnumCaseIdVec *out);
jz_err jazz_node__node__physical__NodeState__physical_to_authored_enum_remaps(const jazz_node_NodeState *s,
    const jazz_node_TablePhysicalMapping *target, jazz_types_PhysicalColumnId c, jazz_node_EnumOccurrenceRemaps *out);
jz_err jazz_node__node__physical__NodeState__physical_copy_enum_remaps(const jazz_node_NodeState *s,
    const jazz_node_TablePhysicalMapping *source_mapping, jazz_types_PhysicalColumnId source_column,
    const jazz_node_TablePhysicalMapping *target_mapping, jazz_types_PhysicalColumnId target_column,
    const groove_ValueType *source_type, const groove_ValueType *target_type, jazz_node_EnumOccurrenceRemaps *out);

/* ---- physical/descriptors.rs ---- */
jz_err jazz_node__node__physical__widened_projection_descriptor(groove_RecordDescriptor logical, const jz_str *names, u32 n,
    const groove_TableSchema *physical, groove_RecordDescriptor *out);
jz_err jazz_node__node__physical__physical_write_descriptor(groove_RecordDescriptor logical, const jz_str *names, u32 n,
    const groove_TableSchema *physical, groove_RecordDescriptor *out);
jz_err jazz_node__node__physical__remap_nested_enum_value(groove_Value value, const groove_ValueType *authored,
    const groove_ValueType *physical, const jazz_node_EnumOccurrenceRemaps *remaps, const jz_str *path, groove_Value *out);
groove_ValueType jazz_node__node__physical__widen_projection_value_type(const groove_ValueType *logical, const groove_ValueType *physical);
jz_err jazz_node__node__physical__physical_version_storage_tables(const jz_btree *schemas, const jz_btree *aliases,
    const jz_btree *mappings, groove_TableSchemaVec *out);
groove_ValueType jazz_node__node__physical__physical_storage_value_type(const jazz_model_ColumnSchema *c);
jz_err jazz_node__node__physical__merge_physical_value_type(const groove_ValueType *a, const groove_ValueType *b, groove_ValueType *out);
jz_err jazz_node__node__physical__physical_current_descriptor(const jazz_model_TableSchema *t, const jazz_node_TablePhysicalMapping *m, groove_RecordDescriptor *out);
jz_err jazz_node__node__physical__physical_rejected_version_descriptor(const jazz_model_TableSchema *t, const jazz_node_TablePhysicalMapping *m, groove_RecordDescriptor *out);
jz_err jazz_node__node__physical__physical_history_field_names(const jazz_model_TableSchema *t, const jazz_node_TablePhysicalMapping *m, jzn_StrVec *out);
jz_err jazz_node__node__physical__physical_history_field_names_for_case(const jazz_model_TableSchema *t, const jazz_node_TablePhysicalMapping *m, const jz_btree *present, jzn_StrVec *out);
jz_err jazz_node__node__physical__physical_current_field_names(const jazz_model_TableSchema *t, const jazz_node_TablePhysicalMapping *m, jzn_StrVec *out);
jz_err jazz_node__node__physical__physical_current_field_names_for_case(const jazz_model_TableSchema *t, const jazz_node_TablePhysicalMapping *m, const jz_btree *present, jzn_StrVec *out);
jz_err jazz_node__node__physical__physical_rejected_version_field_names(const jazz_model_TableSchema *t, const jazz_node_TablePhysicalMapping *m, jzn_StrVec *out);
jz_err jazz_node__node__physical__physical_rejected_version_field_names_for_case(const jazz_model_TableSchema *t, const jazz_node_TablePhysicalMapping *m, const jz_btree *present, jzn_StrVec *out);

/* ---- physical/projections.rs ---- */
groove_StaticScanSpec jazz_node__node__physical__shared_branch_scan(const groove_StaticScanSpec *scan /* NULL = None, moved */);
groove_StaticScanSpec jazz_node__node__physical__branch_scan(const jazz_model_BranchKey *k, const groove_StaticScanSpec *scan);
jz_err jazz_node__node__physical__NodeState__physical_table_id_for_schema(jazz_node_NodeState *s, jazz_types_SchemaVersionId v, const jz_str *table, jazz_types_PhysicalTableId *out);
jz_err jazz_node__node__physical__NodeState__physical_table_id_for_version(jazz_node_NodeState *s, const jazz_node_VersionRow *v, jazz_types_PhysicalTableId *out);
jz_err jazz_node__node__physical__NodeState__physical_current_table_for_schema(jazz_node_NodeState *s, jazz_types_SchemaVersionId v, const jz_str *table, jazz_node_PhysicalCurrentClass c, jz_str *out);
void jazz_node__node__physical__NodeState__physical_table_ids(const jazz_node_NodeState *s, jz_btree *out);
jz_err jazz_node__node__physical__NodeState__logical_table_for_physical_alias(const jazz_node_NodeState *s, jazz_types_PhysicalTableId t, jazz_types_SchemaVersionAlias a, jz_str *out);
jz_err jazz_node__node__physical__NodeState__ensure_physical_current_projection_for_enum_columns(jazz_node_NodeState *s,
    jazz_types_SchemaVersionId v, const jz_str *table, const jz_btree *required, jz_str *out);
jz_err jazz_node__node__physical__NodeState__ensure_physical_current_winner_projection(jazz_node_NodeState *s,
    jazz_types_SchemaVersionId v, const jz_str *table, jz_str *target_out, jzn_StrVec *fields_out);
jz_err jazz_node__node__physical__NodeState__physical_current_post_winner_projection_fields(jazz_node_NodeState *s,
    jazz_types_SchemaVersionId v, const jz_str *table, const jz_btree *required, groove_ProjectFieldVec *out);
jz_err jazz_node__node__physical__NodeState__synchronize_physical_version_tables(jazz_node_NodeState *s);
void jazz_node__node__physical__NodeState__logical_table_names_for_storage_tables(const jazz_node_NodeState *s, const jz_hmap *changed_tables, jz_hmap *out);
extern const jz_hmap_desc jzn_hdesc_str_set;
extern const jz_btree_desc jzn_desc_uuid_winner_map; /* physical_current_winner_projections */
jz_err jazz_node__node__physical__NodeState__physical_current_source_graph(const jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *table, jazz_node_PhysicalCurrentClass cls, groove_GraphBuilder **out);
jz_err jazz_node__node__physical__NodeState__physical_current_marker_source_graph(jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *table, jazz_node_PhysicalCurrentClass cls, const jazz_model_BranchKey *branch_key, const groove_StaticScanSpec *scan, groove_GraphBuilder **out);
jz_err jazz_node__node__physical__NodeState__physical_global_marker_index_graph(jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *table, jz_str index, groove_StaticScanSpec scan, groove_GraphBuilder **out);
jz_err jazz_node__node__physical__NodeState__physical_global_marker_page_graph(jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *table, groove_StaticScanSpec scan, groove_GraphBuilder **out);
jz_err jazz_node__node__physical__NodeState__physical_current_source_graph_with_projection_target(const jazz_node_NodeState *s,
    jazz_types_SchemaVersionId v, const jz_str *table, jazz_node_PhysicalCurrentClass cls, jz_str target, groove_GraphBuilder **out);
jz_err jazz_node__node__physical__NodeState__physical_current_branch_source_graph_with_projection_target(const jazz_node_NodeState *s,
    jazz_types_SchemaVersionId v, const jz_str *table, jazz_node_PhysicalCurrentClass cls, jz_str target,
    const jazz_model_BranchKey *branch_key, const groove_StaticScanSpec *scan, groove_GraphBuilder **out);
jz_err jazz_node__node__physical__NodeState__physical_current_source_scan_graph(const jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *table, jazz_node_PhysicalCurrentClass cls, groove_StaticScanSpec scan, groove_GraphBuilder **out);
jz_err jazz_node__node__physical__NodeState__physical_current_source_scan_graph_with_projection_target(const jazz_node_NodeState *s,
    jazz_types_SchemaVersionId v, const jz_str *table, jazz_node_PhysicalCurrentClass cls, jz_str target, groove_StaticScanSpec scan, groove_GraphBuilder **out);
jz_err jazz_node__node__physical__NodeState__physical_history_source_graph(const jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *table, groove_GraphBuilder **out);
jz_err jazz_node__node__physical__NodeState__physical_history_projection_case_for_enum_columns(jazz_node_NodeState *s,
    jazz_types_SchemaVersionId source_schema, const jz_str *source_table, const jazz_node_TablePhysicalMapping *source_mapping,
    jazz_types_SchemaVersionId target_schema, const jz_str *target_table, const jz_btree *present, const jz_btree *required, bool *some, groove_ProjectFieldVec *out);
jz_err jazz_node__node__physical__NodeState__physical_current_projection_case_for_enum_columns(jazz_node_NodeState *s,
    jazz_types_SchemaVersionId source_schema, const jz_str *source_table, const jazz_node_TablePhysicalMapping *source_mapping,
    jazz_types_SchemaVersionId target_schema, const jz_str *target_table, const jz_btree *present, const jz_btree *required, bool *some, groove_ProjectFieldVec *out);

/* ---- physical/storage.rs ---- */
jz_err jazz_node__node__physical__NodeState__authored_column_ids_for_names(const jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *table, const jz_btree *columns, bool *some, jz_btree *out);
jz_err jazz_node__node__physical__NodeState__authored_column_names_for_ids(const jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *table, const jz_btree *columns, bool *some, jz_btree *out);
jz_err jazz_node__node__physical__NodeState__physical_cell_to_authored(jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *table, u32 column, groove_Value value, groove_Value *out);
jz_err jazz_node__node__physical__NodeState__contribution_table_id_for_storage(const jazz_node_NodeState *s, const jz_str *table, jazz_types_PhysicalTableId *out);
jz_err jazz_node__node__physical__NodeState__contribution_table_name_for_storage(const jazz_node_NodeState *s, jazz_types_PhysicalTableId t, jz_str *out);
jz_err jazz_node__node__physical__NodeState__contribution_column_id_for_storage(const jazz_node_NodeState *s, const jz_str *table, const jz_str *column, jazz_types_PhysicalColumnId *out);
jz_err jazz_node__node__physical__NodeState__contribution_column_name_for_storage(const jazz_node_NodeState *s, const jz_str *table, jazz_types_PhysicalColumnId c, jz_str *out);
jz_err jazz_node__node__physical__NodeState__authored_columns_for_version(jazz_node_NodeState *s, const jazz_node_VersionRow *v, bool *some, jz_btree *out);
jz_err jazz_node__node__physical__NodeState__prepared_physical_write_plan(jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *table, jazz_node_PhysicalWriteTarget target, jazz_node_PreparedPhysicalWritePlan **out);
jz_err jazz_node__node__physical__NodeState__version_storage_table_for_row(jazz_node_NodeState *s, const jazz_node_VersionRow *v, groove_InternStr *out);
jz_err jazz_node__node__physical__NodeState__version_storage_write_binding(jazz_node_NodeState *s, const jazz_node_VersionRow *v,
    jazz_types_AuthorSubject tx_author, groove_InternStr *table_out, groove_ValidatedVariantRecord *record_out);
jz_err jazz_node__node__physical__NodeState__encode_physical_version_record(const jazz_node_NodeState *s, const jazz_node_PreparedPhysicalWritePlan *plan,
    const jazz_node_VersionRow *v, bool has_global_time, jazz_types_GlobalTime global_time, bool has_author, jazz_types_AuthorSubject tx_author,
    groove_ValidatedVariantRecord *out);
jz_err jazz_node__node__physical__NodeState__rejected_version_storage_write_binding(const jazz_node_NodeState *s, const jazz_node_VersionRow *v,
    const groove_OwnedRecord *logical, groove_InternStr *table_out, groove_VariantRecord *record_out);

#endif
