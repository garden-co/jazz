/* _ext_node-qe-a.h: mirror declarations of types/functions owned by OTHER units,
 * as used by unit node-qe-a (jazz_node::node::query_eval{,::read_sources,::normalization,::lowering}).
 * Struct layouts roughly follow the Rust fields that this unit touches; types only
 * passed by pointer are opaque. Never include another unit's header. */
#ifndef JZ_EXT_NODE_QE_A_H
#define JZ_EXT_NODE_QE_A_H
#include "jz_rt.h"

typedef JZ_VEC(jz_str) jz_vec_str;
typedef JZ_VEC(u8) jz_vec_u8;
typedef JZ_VEC(jz_bytes) jz_vec_bytes;

/* ======================================================================
 * groove
 * ====================================================================== */
typedef struct groove_RecordDescriptor_s groove_RecordDescriptor_s;
typedef struct groove_ValueType { u32 tag; void *payload; } groove_ValueType; /* == groove::schema::ColumnType */
typedef groove_ValueType groove_ColumnType;
enum { GROOVE_VT_U8, GROOVE_VT_U16, GROOVE_VT_U32, GROOVE_VT_U64, GROOVE_VT_I32, GROOVE_VT_I64, GROOVE_VT_F64,
       GROOVE_VT_BOOL, GROOVE_VT_STRING, GROOVE_VT_BYTES, GROOVE_VT_INTERNAL, GROOVE_VT_UUID, GROOVE_VT_ENUM_TAG,
       GROOVE_VT_TUPLE, GROOVE_VT_ARRAY, GROOVE_VT_NULLABLE, GROOVE_VT_RECORD, GROOVE_VT_ENUM, GROOVE_VT_U48 };
void groove_ValueType_drop(groove_ValueType *t);
void groove_ValueType_clone(groove_ValueType *dst, const groove_ValueType *src);
bool groove_ValueType_eq(const groove_ValueType *a, const groove_ValueType *b);
groove_ValueType groove_ValueType_simple(u32 tag);
groove_ValueType groove_ValueType_nullable(groove_ValueType inner);
groove_ValueType groove_ValueType_array(groove_ValueType inner);
const groove_ValueType *groove_ValueType_inner(const groove_ValueType *t); /* Nullable/Array payload */
typedef struct groove_EnumSchema groove_EnumSchema;
const groove_EnumSchema *groove_ValueType_as_enum(const groove_ValueType *t);
bool groove__records__values__EnumSchema__case_by_name(const groove_EnumSchema *e, jz_slice name, u32 *tag, const struct groove_RecordDescriptor_s **payload);

typedef struct groove_Value groove_Value;
typedef JZ_VEC(groove_Value) groove_ValueVec;
enum { GROOVE_V_U8, GROOVE_V_U16, GROOVE_V_U32, GROOVE_V_U64, GROOVE_V_F64, GROOVE_V_BOOL, GROOVE_V_STRING,
       GROOVE_V_BYTES, GROOVE_V_LARGE, GROOVE_V_UUID, GROOVE_V_ENUM_TAG, GROOVE_V_TUPLE, GROOVE_V_ARRAY,
       GROOVE_V_NULLABLE, GROOVE_V_I64, GROOVE_V_I32, GROOVE_V_RECORD, GROOVE_V_ENUM, GROOVE_V_U48 };
struct groove_Value {
    u32 tag;
    union { u64 u; i64 i; f64 f; bool b; jz_str s; jz_bytes bytes; jz_uuid uuid; groove_ValueVec vec;
            groove_Value *nullable; void *large; void *record; } v;
};
void groove_Value_drop(groove_Value *v);
void groove_Value_clone(groove_Value *dst, const groove_Value *src);
bool groove_Value_eq(const groove_Value *a, const groove_Value *b);
int groove_Value_partial_cmp(const groove_Value *a, const groove_Value *b, bool *some);
void groove_Value_debug(jz_str *out, const groove_Value *v);
void groove_Value_drop_v(void *v);
void groove_Value_clone_v(void *dst, const void *src);
static inline groove_Value groove_Value_u64(u64 x) { groove_Value v; v.tag = GROOVE_V_U64; v.v.u = x; return v; }
static inline groove_Value groove_Value_bytes(jz_bytes b) { groove_Value v; v.tag = GROOVE_V_BYTES; v.v.bytes = b; return v; }
static inline groove_Value groove_Value_uuid(jz_uuid u) { groove_Value v; v.tag = GROOVE_V_UUID; v.v.uuid = u; return v; }
static inline groove_Value groove_Value_string(jz_str s) { groove_Value v; v.tag = GROOVE_V_STRING; v.v.s = s; return v; }
static inline groove_Value groove_Value_bool(bool b) { groove_Value v; v.tag = GROOVE_V_BOOL; v.v.b = b; return v; }
static inline groove_Value groove_Value_null(void) { groove_Value v; v.tag = GROOVE_V_NULLABLE; v.v.nullable = NULL; return v; }
groove_Value groove_Value_some(groove_Value inner);
bool groove__records__values__Value__matches_type(const groove_Value *v, const groove_ValueType *t);

/* records */
typedef struct groove_RecordDescriptor_s groove_RecordDescriptor_s;
typedef const groove_RecordDescriptor_s *groove_RecordDescriptor; /* Intern<RecordDescriptorData>: pointer-eq */
typedef struct groove_DescriptorField {
    jz_str name; bool has_name; groove_ValueType value_type;
    u32 identity_kind; jz_str identity_name; u32 identity_slot; /* FieldIdentity */
} groove_DescriptorField;
typedef JZ_VEC(groove_DescriptorField) groove_DescriptorFieldVec;
void groove_DescriptorField_drop(void *f);
void groove_DescriptorField_clone(void *dst, const void *src);
groove_DescriptorField groove__records__DescriptorField__new(jz_str name, groove_ValueType vt);
groove_DescriptorField groove__records__DescriptorField__with_identity_name(groove_DescriptorField f, jz_str identity);
groove_DescriptorField groove__records__DescriptorField__with_identity_hidden(groove_DescriptorField f);
groove_DescriptorField groove__records__DescriptorField__nullable(groove_DescriptorField f);
const jz_str *groove__records__DescriptorField__logical_name(const groove_DescriptorField *f);
enum { GROOVE_FID_NONE, GROOVE_FID_NAME, GROOVE_FID_SLOT, GROOVE_FID_NAMED_SLOT, GROOVE_FID_HIDDEN };
groove_RecordDescriptor groove__records__RecordDescriptor__new(groove_DescriptorFieldVec fields);
groove_RecordDescriptor groove__records__RecordDescriptor__from_logical_fields(groove_DescriptorFieldVec fields);
const groove_DescriptorFieldVec *groove__records__RecordDescriptor__fields(groove_RecordDescriptor d);
bool groove__records__RecordDescriptor__field_index(groove_RecordDescriptor d, jz_slice name, u32 *idx);
jz_err groove__records__RecordDescriptor__create(groove_RecordDescriptor d, const groove_Value *values, u32 n, jz_bytes *out);
u32 groove__records__RecordDescriptor__len(groove_RecordDescriptor d);
typedef struct groove_BorrowedRecord { const u8 *raw; u32 len; groove_RecordDescriptor descriptor; } groove_BorrowedRecord;
typedef struct groove_OwnedRecord { jz_bytes raw; groove_RecordDescriptor descriptor; } groove_OwnedRecord;
void groove_OwnedRecord_drop(void *r);
void groove_OwnedRecord_clone(void *dst, const void *src);
jz_err groove__records__BorrowedRecord__get(const groove_BorrowedRecord *r, u32 idx, groove_Value *out);
jz_err groove__records__BorrowedRecord__get_uuid(const groove_BorrowedRecord *r, u32 idx, jz_uuid *out);
jz_err groove__records__BorrowedRecord__get_u64(const groove_BorrowedRecord *r, u32 idx, u64 *out);
jz_err groove__records__BorrowedRecord__get_nullable_u64(const groove_BorrowedRecord *r, u32 idx, bool *some, u64 *out);
jz_err groove__records__BorrowedRecord__get_bytes(const groove_BorrowedRecord *r, u32 idx, jz_slice *out);
jz_err groove__records__BorrowedRecord__get_by_name(const groove_BorrowedRecord *r, jz_slice name, groove_Value *out);
jz_err groove__records__BorrowedRecord__values(const groove_BorrowedRecord *r, groove_ValueVec *out);
groove_BorrowedRecord groove__records__OwnedRecord__as_borrowed(const groove_OwnedRecord *r);

/* ivm ops */
typedef struct groove_LiteralValue groove_LiteralValue; /* opaque, heap */
groove_LiteralValue *groove__ivm__op_types__LiteralValue__from_value(groove_Value v);
void groove_LiteralValue_free(groove_LiteralValue *l);
groove_LiteralValue *groove_LiteralValue_clone(const groove_LiteralValue *l);
typedef JZ_VEC(groove_LiteralValue *) groove_LiteralVec;
void groove_LiteralVec_drop(groove_LiteralVec *v);
enum { GROOVE_SCAN_POINT, GROOVE_SCAN_PREFIX, GROOVE_SCAN_PREFIX_LIMIT, GROOVE_SCAN_REVERSE_PREFIX_LIMIT, GROOVE_SCAN_RANGE };
typedef struct groove_StaticScanSpec { u32 tag; groove_LiteralVec prefix; groove_LiteralVec end; u32 max_items; } groove_StaticScanSpec;
void groove_StaticScanSpec_drop(groove_StaticScanSpec *s);
void groove_StaticScanSpec_clone(groove_StaticScanSpec *dst, const groove_StaticScanSpec *src);

typedef struct groove_PredicateExpr groove_PredicateExpr; /* groove::ivm::op_types::PredicateExpr, heap */
groove_PredicateExpr *groove__ivm__op_types__PredicateExpr__is_not_null(jz_slice field);
groove_PredicateExpr *groove__ivm__op_types__PredicateExpr__is_null(jz_slice field);
groove_PredicateExpr *groove__ivm__op_types__PredicateExpr__eq(jz_slice field, groove_LiteralValue *v);
groove_PredicateExpr *groove__ivm__op_types__PredicateExpr__neq(jz_slice field, groove_LiteralValue *v);
groove_PredicateExpr *groove__ivm__op_types__PredicateExpr__eq_field(jz_slice field, jz_slice value_field);
groove_PredicateExpr *groove__ivm__op_types__PredicateExpr__and(groove_PredicateExpr **items, u32 n);
groove_PredicateExpr *groove__ivm__op_types__PredicateExpr__or(groove_PredicateExpr **items, u32 n);
void groove_PredicateExpr_free(groove_PredicateExpr *p);

typedef struct groove_ProjectField groove_ProjectField; /* heap */
typedef JZ_VEC(groove_ProjectField *) groove_ProjectFieldVec;
groove_ProjectField *groove__ivm__graph__ProjectField__named(jz_slice name);
groove_ProjectField *groove__ivm__graph__ProjectField__renamed(jz_slice src, jz_slice out);
groove_ProjectField *groove__ivm__graph__ProjectField__nullable(jz_slice src, jz_slice out);
groove_ProjectField *groove__ivm__graph__ProjectField__literal(jz_slice out, groove_LiteralValue *v);
groove_ProjectField *groove__ivm__graph__ProjectField__literal_typed(jz_slice out, groove_LiteralValue *v, groove_ValueType t);
groove_ProjectField *groove__ivm__graph__ProjectField__null_typed(jz_slice out, groove_ValueType t);
groove_ProjectField *groove__ivm__graph__ProjectField__named_with_identity(jz_slice name, jz_str identity);
groove_ProjectField *groove__ivm__graph__ProjectField__renamed_with_identity(jz_slice src, jz_slice out, jz_str identity);
groove_ProjectField *groove__ivm__graph__ProjectField__enum_remap(jz_slice src, jz_slice out, const void *remap);
void groove_ProjectFieldVec_drop(groove_ProjectFieldVec *v);
groove_ProjectFieldVec groove_ProjectFieldVec_clone(const groove_ProjectFieldVec *v);

typedef struct groove_GraphBuilder groove_GraphBuilder; /* groove::ivm::graph::GraphBuilder, heap (consumed by builder calls) */
typedef struct groove_InputSourceId { u64 id; } groove_InputSourceId;
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__table(jz_slice table);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__table_scan(jz_slice table, groove_StaticScanSpec scan);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__variant_source(jz_slice table, jz_slice target);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__variant_source_scan(jz_slice table, jz_slice target, groove_StaticScanSpec scan);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__inline_records(groove_RecordDescriptor d, jz_vec_bytes records);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__input_source(groove_InputSourceId id, groove_RecordDescriptor d);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__index_scan(jz_slice table, jz_slice index, groove_StaticScanSpec scan);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__variant_index_scan(jz_slice table, jz_slice index, groove_StaticScanSpec scan, jz_slice target);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__variant_index_intersection_scan(jz_slice table, jz_slice index, groove_StaticScanSpec scan, void *intersections, u32 n, jz_slice target);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__variant_index_candidate_scan(jz_slice table, jz_slice index, groove_StaticScanSpec scan, void *candidate, jz_slice target);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__binding_source(jz_slice shape, groove_RecordDescriptor d);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__union(groove_GraphBuilder **inputs, u32 n);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__join(groove_GraphBuilder *l, groove_GraphBuilder *r, const jz_str *lon, u32 nl, const jz_str *ron, u32 nr);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__semi_join(groove_GraphBuilder *l, groove_GraphBuilder *r, const jz_str *lon, u32 nl, const jz_str *ron, u32 nr);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__anti_join(groove_GraphBuilder *l, groove_GraphBuilder *r, const jz_str *lon, u32 nl, const jz_str *ron, u32 nr);
/* lookup(right, keys, keys_on) -> Option<GraphBuilder>: NULL when `right` is not a table source (right is consumed either way) */
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__lookup(groove_GraphBuilder *right, groove_GraphBuilder *keys, const jz_str *keys_on, u32 n);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__arg_max_by(groove_GraphBuilder *in, const jz_str *group, u32 ng, const jz_str *order, u32 no);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__filter(groove_GraphBuilder *in, groove_PredicateExpr *p);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__project(groove_GraphBuilder *in, const jz_str *fields, u32 n);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__project_cstrs(groove_GraphBuilder *in, const char *const *fields, u32 n);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__project_fields(groove_GraphBuilder *in, groove_ProjectFieldVec fields);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__unwrap_nullable(groove_GraphBuilder *in, jz_slice field);
groove_GraphBuilder *groove_GraphBuilder_clone(const groove_GraphBuilder *g);
void groove_GraphBuilder_free(groove_GraphBuilder *g);
bool groove_GraphBuilder_eq(const groove_GraphBuilder *a, const groove_GraphBuilder *b);
void groove_GraphBuilder_free_v(void *pp);   /* drop for a slot holding groove_GraphBuilder* */
void groove_GraphBuilder_clone_v(void *dst, const void *src);
jz_err groove__ivm__graph__GraphBuilder__output_descriptor(const groove_GraphBuilder *g, groove_RecordDescriptor *out);

typedef struct groove_RecordDeltas { JZ_VEC(groove_OwnedRecord) added; JZ_VEC(groove_OwnedRecord) removed; groove_RecordDescriptor descriptor; } groove_RecordDeltas;
void groove_RecordDeltas_drop(void *d);
typedef struct groove_MultisinkDeltas { jz_btree sinks; /* jz_str -> groove_RecordDeltas */ } groove_MultisinkDeltas;
void groove_MultisinkDeltas_drop(groove_MultisinkDeltas *d);
enum { GROOVE_RIV_MATERIALIZE, GROOVE_RIV_PHYSICAL, GROOVE_RIV_PHYSICAL_FIELDS };
typedef struct groove_RootIndirectValues { u32 tag; jz_btree *physical_fields; /* Arc<BTreeSet<String>> (jz_rc payload) */ } groove_RootIndirectValues;
typedef struct groove_PreparedShapeId { u64 id; } groove_PreparedShapeId;
typedef struct groove_RoutedMultisinkTerminal groove_RoutedMultisinkTerminal;
typedef struct groove_MultisinkSubscription groove_MultisinkSubscription;

/* groove schema */
typedef struct groove_TableSchema groove_TableSchema;
typedef struct groove_TableSchema_view { groove_RecordDescriptor record; } groove_TableSchema_view;
groove_RecordDescriptor groove__schema__TableSchema__record_schema(const groove_TableSchema *t);

/* groove db */
typedef struct groove_DbError groove_DbError;
typedef struct groove_RawRecord { jz_bytes key; jz_bytes value; groove_RecordDescriptor descriptor; } groove_RawRecord;
groove_BorrowedRecord groove__db__RawRecord__record(const groove_RawRecord *r);
void groove_RawRecord_drop(void *r);
typedef JZ_VEC(groove_RawRecord) groove_RawRecordVec;
typedef struct groove_GraphRow { groove_OwnedRecord row; i64 weight; } groove_GraphRow;
typedef JZ_VEC(groove_GraphRow) groove_GraphRows;
void groove_GraphRows_drop(groove_GraphRows *r);
typedef struct jazz_node_DatabaseSlot jazz_node_DatabaseSlot;
/* The storage-generic Database<S> is reached through a vtable (S: OrderedKvStorage). */
typedef struct groove_StorageVtbl {
    jz_err (*primary_key_scan_raw)(void *self, jz_slice table, const groove_Value *prefix, u32 n, groove_RawRecordVec *out);
    jz_err (*primary_key_scan_range_raw)(void *self, jz_slice table, const groove_Value *lo, u32 nlo, const groove_Value *hi, u32 nhi, groove_RawRecordVec *out);
    jz_err (*primary_key_last_raw)(void *self, jz_slice table, const groove_Value *prefix, u32 n, bool *some, groove_RawRecord *out);
    jz_err (*query_graph)(void *self, groove_GraphBuilder *g, groove_GraphRows *out);
    jz_err (*table_schema)(void *self, jz_slice table, const groove_TableSchema **out);
    jz_err (*index_prefix_scan_raw)(void *self, jz_slice table, jz_slice index, const groove_Value *prefix, u32 n, u32 limit, bool reverse, groove_RawRecordVec *out);
    jz_err (*index_prefix_entries_up_to)(void *self, jz_slice table, jz_slice index, const groove_Value *prefix, u32 n, u32 max_items, u32 *out);
} groove_StorageVtbl;
typedef struct groove_Database { const groove_StorageVtbl *vt; void *self; } groove_Database;

/* ======================================================================
 * jazz_types
 * ====================================================================== */
typedef struct jazz_types_SchemaVersionId { jz_uuid u; } jazz_types_SchemaVersionId;
typedef struct jazz_types_RowUuid { jz_uuid u; } jazz_types_RowUuid;
typedef struct jazz_types_NodeUuid { jz_uuid u; } jazz_types_NodeUuid;
typedef struct jazz_types_PhysicalColumnId { jz_uuid u; } jazz_types_PhysicalColumnId;
typedef struct jazz_types_PhysicalTableId { jz_uuid u; } jazz_types_PhysicalTableId;
typedef struct jazz_types_SchemaFamilyId { jz_uuid u; } jazz_types_SchemaFamilyId;
typedef struct jazz_types_TxTime { u64 t; } jazz_types_TxTime;
typedef struct jazz_types_GlobalTime { u64 t; } jazz_types_GlobalTime;
typedef struct jazz_types_TxId { jazz_types_TxTime time; jazz_types_NodeUuid node; } jazz_types_TxId;
typedef struct jazz_types_ObjectId { jz_uuid u; } jazz_types_ObjectId;
typedef struct jazz_types_OutputOccurrenceId { JZ_VEC(jazz_types_ObjectId) sources; } jazz_types_OutputOccurrenceId;
void jazz_types_OutputOccurrenceId_drop(jazz_types_OutputOccurrenceId *o);
bool jazz_types_OutputOccurrenceId_eq(const jazz_types_OutputOccurrenceId *a, const jazz_types_OutputOccurrenceId *b);
jazz_types_OutputOccurrenceId jazz_types__object__OutputOccurrenceId__single_source(jazz_types_ObjectId id);
/* AuthorSubject (copy) */
typedef struct jazz_types_AuthorSubject { u32 tag; jz_uuid id; } jazz_types_AuthorSubject;
enum { JAZZ_AUTHOR_SYSTEM, JAZZ_AUTHOR_ACCOUNT, JAZZ_AUTHOR_SESSION, JAZZ_AUTHOR_ANONYMOUS };
bool jazz_types_AuthorSubject_eq(const jazz_types_AuthorSubject *a, const jazz_types_AuthorSubject *b);
jz_uuid jazz_types__ids__AuthorSubject__as_uuid(jazz_types_AuthorSubject a);
bool jazz_types__debug_env__covered_input_trace(void);
bool jazz_types__debug_env__capability_trace(void);
bool jazz_types__debug_env__query_profile(void);

/* ======================================================================
 * jazz_model
 * ====================================================================== */
typedef enum { JAZZ_TIER_NONE = 0, JAZZ_TIER_LOCAL = 1, JAZZ_TIER_EDGE = 2, JAZZ_TIER_GLOBAL = 3 } jazz_model_DurabilityTier;
typedef struct jazz_model_ShapeId { jz_uuid u; } jazz_model_ShapeId;
typedef struct jazz_model_BindingId { jz_uuid u; } jazz_model_BindingId;
typedef struct jazz_model_BranchKey { jz_bytes canonical; } jazz_model_BranchKey;
void jazz_model_BranchKey_drop(jazz_model_BranchKey *b);
void jazz_model_BranchKey_clone(jazz_model_BranchKey *d, const jazz_model_BranchKey *s);
bool jazz_model_BranchKey_eq(const jazz_model_BranchKey *a, const jazz_model_BranchKey *b);
jazz_model_BranchKey jazz_model__model__branch__BranchKey__default(void);
jz_bytes jazz_model__model__branch__BranchKey__canonical_bytes(const jazz_model_BranchKey *b);
typedef struct jazz_model_OpenTransactionId { jz_uuid u; } jazz_model_OpenTransactionId;
typedef struct jazz_model_Snapshot { jz_bytes bytes; } jazz_model_Snapshot;
typedef struct jazz_model_SnapshotRef { jz_bytes bytes; } jazz_model_SnapshotRef;

/* query AST (jazz_model::query) */
typedef struct jazz_model_Operand { u32 tag; jz_str name; groove_Value literal; } jazz_model_Operand;
enum { JAZZ_OPERAND_COLUMN, JAZZ_OPERAND_PARAM, JAZZ_OPERAND_CLAIM, JAZZ_OPERAND_LITERAL };
typedef JZ_VEC(jazz_model_Operand) jazz_model_OperandVec;
typedef struct jazz_model_Predicate jazz_model_Predicate;
typedef JZ_VEC(jazz_model_Predicate) jazz_model_PredicateVec;
enum { JAZZ_PRED_ALL, JAZZ_PRED_ANY, JAZZ_PRED_NOT, JAZZ_PRED_EQ, JAZZ_PRED_NE, JAZZ_PRED_IN, JAZZ_PRED_GT, JAZZ_PRED_GTE,
       JAZZ_PRED_LT, JAZZ_PRED_LTE, JAZZ_PRED_CONTAINS, JAZZ_PRED_ENUM_MATCH, JAZZ_PRED_IS_NULL };
struct jazz_model_Predicate {
    u32 tag;
    jazz_model_PredicateVec children;          /* All / Any */
    jazz_model_Predicate *inner;               /* Not / EnumMatch payload */
    jazz_model_Operand left, right;            /* binary, IsNull(left) */
    jazz_model_OperandVec options;             /* In */
    jz_str column, enum_case;                  /* EnumMatch */
};
void jazz_model_Predicate_drop(void *p);
void jazz_model_Predicate_clone(void *dst, const void *src);
void jazz_model_Operand_drop(void *o);
void jazz_model_Operand_clone(void *dst, const void *src);
typedef enum { JAZZ_ORDER_ASC, JAZZ_ORDER_DESC } jazz_model_OrderDirection;
typedef struct jazz_model_OrderBy { jz_str column; jazz_model_OrderDirection direction; } jazz_model_OrderBy;
typedef enum { JAZZ_JOIN_TARGET_COLUMN, JAZZ_JOIN_TARGET_ROW_ID, JAZZ_JOIN_TARGET_UNCORRELATED } jazz_model_JoinTarget;
typedef struct jazz_model_JoinCorrelation { jz_str join_column, source_column; } jazz_model_JoinCorrelation;
typedef struct jazz_model_JoinSourceLookup { jz_str table, row_id_source_column, value_column; } jazz_model_JoinSourceLookup;
typedef struct jazz_model_JoinVia jazz_model_JoinVia;
typedef JZ_VEC(jazz_model_JoinVia) jazz_model_JoinViaVec;
struct jazz_model_JoinVia {
    jz_str table, on_column; jazz_model_JoinTarget target;
    bool has_source_column; jz_str source_column;
    bool has_source_lookup; jazz_model_JoinSourceLookup source_lookup;
    JZ_VEC(jazz_model_JoinCorrelation) correlated_filters;
    jazz_model_PredicateVec filters;
    jazz_model_JoinViaVec nested_joins;
};
void jazz_model_JoinVia_drop(void *j);
void jazz_model_JoinVia_clone(void *dst, const void *src);
typedef struct jazz_model_RecursionBound { u32 tag; /*0 Fixpoint 1 MaxDepth*/ u32 max_depth; } jazz_model_RecursionBound;
typedef struct jazz_model_ReachableSeed { jz_str table; bool has_user_column; jz_str user_column; bool has_user_claim; jz_str user_claim; jz_str team_column; jazz_model_PredicateVec filters; } jazz_model_ReachableSeed;
typedef struct jazz_model_ReachableVia {
    jz_str access_table, access_row_column, access_team_column; jazz_model_JoinTarget access_team_target;
    jazz_model_Operand from; jazz_model_PredicateVec access_filters;
    jz_str edge_table, edge_member_column, edge_parent_column; jazz_model_PredicateVec edge_filters;
    jazz_model_RecursionBound bound; bool has_seed; jazz_model_ReachableSeed seed;
} jazz_model_ReachableVia;
typedef enum { JAZZ_INHERITS_SELECT, JAZZ_INHERITS_INSERT, JAZZ_INHERITS_UPDATE, JAZZ_INHERITS_DELETE } jazz_model_InheritsOperation;
typedef struct jazz_model_InheritsVia { jz_str parent_column; jazz_model_InheritsOperation operation; bool has_max_depth; u32 max_depth; } jazz_model_InheritsVia;
typedef enum { JAZZ_INCLUDE_INNER, JAZZ_INCLUDE_HOLES } jazz_model_JoinMode;
typedef struct jazz_model_Include { jz_str path; jazz_model_JoinMode join_mode; bool require; } jazz_model_Include;
typedef enum { JAZZ_ASQ_OPTIONAL, JAZZ_ASQ_AT_LEAST_ONE, JAZZ_ASQ_MATCH } jazz_model_ArraySubqueryRequirement;
typedef struct jazz_model_ArraySubquery jazz_model_ArraySubquery;
struct jazz_model_ArraySubquery {
    jz_str column_name, table, inner_column, outer_column; jazz_model_PredicateVec filters;
    bool has_select; jz_vec_str select; JZ_VEC(jazz_model_OrderBy) order_by;
    bool has_limit; u32 limit; u32 offset; jazz_model_ArraySubqueryRequirement requirement;
    JZ_VEC(jazz_model_ArraySubquery) nested_arrays;
};
typedef enum { JAZZ_AGG_COUNT, JAZZ_AGG_SUM, JAZZ_AGG_AVG, JAZZ_AGG_MIN, JAZZ_AGG_MAX } jazz_model_AggregateFunction;
typedef struct jazz_model_Aggregate { jazz_model_AggregateFunction function; bool has_column; jz_str column; jz_str alias; } jazz_model_Aggregate;
typedef struct jazz_model_AggregateQuery { JZ_VEC(jazz_model_Aggregate) aggregates; bool has_group_by; jz_str group_by; } jazz_model_AggregateQuery;
typedef struct jazz_model_FlatJoinSource { jz_str table; bool has_alias; jz_str alias; jz_str on_left, on_right; } jazz_model_FlatJoinSource;
typedef struct jazz_model_FlatJoin { bool has_root_alias; jz_str root_alias; JZ_VEC(jazz_model_FlatJoinSource) sources; } jazz_model_FlatJoin;
typedef struct jazz_model_PolicyBranch { jazz_model_PredicateVec filters; jazz_model_JoinViaVec joins; JZ_VEC(jazz_model_ReachableVia) reachable; JZ_VEC(jazz_model_InheritsVia) inherits; } jazz_model_PolicyBranch;
typedef struct jazz_model_RelationExpr jazz_model_RelationExpr; /* opaque */
typedef struct jazz_model_RelationQuery { jazz_model_RelationExpr *rel; } jazz_model_RelationQuery;
typedef struct jazz_model_Query {
    jz_str table; jazz_model_PredicateVec filters; jazz_model_JoinViaVec joins;
    jazz_model_FlatJoin *flat_join; /* Option<FlatJoin> */
    JZ_VEC(jazz_model_PolicyBranch) policy_branches;
    JZ_VEC(jazz_model_ReachableVia) reachable; JZ_VEC(jazz_model_InheritsVia) inherits;
    JZ_VEC(jazz_model_Include) includes; JZ_VEC(jazz_model_ArraySubquery) array_subqueries;
    bool has_select; jz_vec_str select; JZ_VEC(jazz_model_OrderBy) order_by;
    jazz_model_AggregateQuery *aggregate; bool has_limit; u32 limit; u32 offset;
    jazz_model_RelationQuery *relation;
} jazz_model_Query;
void jazz_model_Query_drop(jazz_model_Query *q);
void jazz_model_Query_clone(jazz_model_Query *dst, const jazz_model_Query *src);
typedef struct jazz_model_ValidatedQuery {
    jazz_model_Query query; jazz_types_SchemaVersionId schema_version;
    jz_btree params; /* String -> ColumnType */ jz_bytes canonical; jazz_model_ShapeId shape_id;
} jazz_model_ValidatedQuery;
void jazz_model_ValidatedQuery_drop(void *q);
void jazz_model_ValidatedQuery_clone(void *dst, const void *src);
typedef struct jazz_model_Binding { jz_btree values; /* String -> Value */ jz_bytes canonical; } jazz_model_Binding;
void jazz_model_Binding_drop(void *b);
void jazz_model_Binding_clone(void *dst, const void *src);
jazz_model_BindingId jazz_model__query__validation__Binding__binding_id(const jazz_model_Binding *b);
typedef struct jazz_model_QueryError jazz_model_QueryError;
/* QueryError -> node Error::Query: returned as a jz_err with code JZN_E_Query */
jz_err jazz_model__query__validation__ValidatedQuery__bind(const jazz_model_ValidatedQuery *q, jz_btree values, jazz_model_Binding *out);
jz_err jazz_model__query__ast__Query__validate_with_schema_version(const jazz_model_Query *q, const void *schema, jazz_types_SchemaVersionId v, jazz_model_ValidatedQuery *out);
jz_err jazz_model__query__relation_query_to_query(const jazz_model_RelationQuery *r, jazz_model_Query *out);
jz_err jazz_model__query__binding_id_for_values(const jz_btree *values, jazz_model_BindingId *out);
JZ_VEC(jz_str) jazz_model__query__operand_claim_path(jz_slice name);

/* schema (jazz_model::schema) */
typedef struct jazz_model_ColumnSchema { jz_str name; groove_ColumnType column_type; u32 large_value_kind; groove_Value *default_value; } jazz_model_ColumnSchema;
typedef struct jazz_model_WritePolicies { jazz_model_Query *insert_check, *update_using, *update_check, *delete_using; } jazz_model_WritePolicies;
typedef struct jazz_model_TableSchema {
    jz_str name; JZ_VEC(jazz_model_ColumnSchema) columns; jz_vec_str branch_by;
    jz_btree references; /* String -> String */ jazz_model_Query *read_policy;
    jazz_model_WritePolicies write_policies; jz_btree indexed_columns; /* set<String> */
    jz_btree composite_indexes; /* set<Vec<String>> */ jz_btree merge_strategies;
} jazz_model_TableSchema;
void jazz_model_TableSchema_drop(void *t);
void jazz_model_TableSchema_clone(void *dst, const void *src);
const jazz_model_ColumnSchema *jazz_model__schema__TableSchema__column(const jazz_model_TableSchema *t, jz_slice name);
typedef struct jazz_model_RuntimeSchema jazz_model_RuntimeSchema;
struct jazz_model_RuntimeSchema { JZ_VEC(jazz_model_TableSchema) tables; };
const jazz_model_TableSchema *jazz_model__schema__RuntimeSchema__table(const jazz_model_RuntimeSchema *s, jz_slice name);
jz_str jazz_model__schema__app_storage_column_name(jz_slice column);

/* ======================================================================
 * jazz_protocol
 * ====================================================================== */
typedef struct jazz_protocol_BindingViewKey { jazz_model_ShapeId shape_id; jazz_model_BindingId binding_id; u32 tier; jz_bytes read_view; } jazz_protocol_BindingViewKey;
void jazz_protocol_BindingViewKey_drop(void *k);
void jazz_protocol_BindingViewKey_clone(void *dst, const void *src);
bool jazz_protocol_BindingViewKey_eq(const jazz_protocol_BindingViewKey *a, const jazz_protocol_BindingViewKey *b);
typedef struct jazz_protocol_SubscriptionKey { jazz_model_ShapeId shape_id; jazz_model_BindingId binding_id; u64 seq; } jazz_protocol_SubscriptionKey;
typedef struct jazz_protocol_PolicyBindingKey { jz_bytes bytes; } jazz_protocol_PolicyBindingKey;
void jazz_protocol_PolicyBindingKey_drop(void *k);
void jazz_protocol_PolicyBindingKey_clone(void *dst, const void *src);
typedef struct jazz_protocol_AuthorityResultKey { jz_bytes bytes; } jazz_protocol_AuthorityResultKey;
enum { JAZZ_RVS_CURRENT, JAZZ_RVS_BRANCH_VIEW, JAZZ_RVS_HISTORY, JAZZ_RVS_SNAPSHOT, JAZZ_RVS_OPEN_TX };
typedef struct jazz_protocol_ReadViewSourceSpec { u32 tag; jazz_model_BranchKey head; jazz_model_BranchKey *base; jazz_types_GlobalTime position; jazz_model_Snapshot snapshot; jazz_model_OpenTransactionId tx; } jazz_protocol_ReadViewSourceSpec;
typedef struct jazz_protocol_ReadViewSpec { jazz_protocol_ReadViewSourceSpec source; bool has_lens; jz_bytes lens; } jazz_protocol_ReadViewSpec;
jazz_protocol_ReadViewSpec jazz_protocol_ReadViewSpec_default(void);
void jazz_protocol_ReadViewSpec_drop(jazz_protocol_ReadViewSpec *r);
void jazz_protocol_ReadViewSpec_clone(jazz_protocol_ReadViewSpec *d, const jazz_protocol_ReadViewSpec *s);
bool jazz_protocol_ReadViewSpec_eq(const jazz_protocol_ReadViewSpec *a, const jazz_protocol_ReadViewSpec *b);
typedef struct jazz_protocol_RegisterShapeOptions { jazz_model_DurabilityTier tier; jazz_protocol_ReadViewSpec read_view; bool include_deleted; } jazz_protocol_RegisterShapeOptions;
enum { JAZZ_RML_CONTENT, JAZZ_RML_DELETION };
typedef struct jazz_protocol_RowVersionRefEntry { jazz_types_TxId tx; bool has_schema_version; jazz_types_SchemaVersionId schema_version; u32 layer; bool has_batch; jazz_types_TxId batch; jz_bytes *branch_or_prefix; jz_bytes *row_digest; } jazz_protocol_RowVersionRefEntry;
enum { JAZZ_RME_ROW, JAZZ_RME_SYNTHETIC };
typedef struct jazz_protocol_ResultMemberEntry {
    u32 tag; jz_str table; jazz_types_RowUuid row_uuid; jazz_protocol_RowVersionRefEntry version;
    jz_bytes row; /* Synthetic row key */ jazz_types_OutputOccurrenceId *occurrence;
} jazz_protocol_ResultMemberEntry;
void jazz_protocol_ResultMemberEntry_drop(void *m);
void jazz_protocol_ResultMemberEntry_clone(void *dst, const void *src);
bool jazz_protocol_ResultMemberEntry_eq(const jazz_protocol_ResultMemberEntry *a, const jazz_protocol_ResultMemberEntry *b);
int jazz_protocol_ResultMemberEntry_cmp_v(const void *a, const void *b, void *ctx);
const jz_str *jazz_protocol__protocol__ResultMemberEntry__table_name(const jazz_protocol_ResultMemberEntry *m);
bool jazz_protocol__protocol__ResultMemberEntry__output_occurrence_id(const jazz_protocol_ResultMemberEntry *m, jazz_types_OutputOccurrenceId *out);
typedef struct jazz_protocol_ResultMemberPayloadEntry jazz_protocol_ResultMemberPayloadEntry;
void jazz_protocol_ResultMemberPayloadEntry_drop(void *p);
typedef struct jazz_protocol_ShapeAst jazz_protocol_ShapeAst;
typedef struct jazz_protocol_Subscribe jazz_protocol_Subscribe;
typedef struct jazz_protocol_ProgramFactEntry jazz_protocol_ProgramFactEntry;
typedef struct jazz_protocol_KnownStateDeclaration jazz_protocol_KnownStateDeclaration;
#define JAZZ_PROTOCOL_MAX_KNOWN_STATE_EXACT_REFS 4096u

/* ======================================================================
 * jazz_engine::query_engine (IR + resolver contract)
 * ====================================================================== */
enum { JAZZ_SR_ROOT, JAZZ_SR_ALIAS, JAZZ_SR_RECURSIVE_SEED, JAZZ_SR_RECURSIVE_STEP, JAZZ_SR_CORRELATED_CHILD, JAZZ_SR_POLICY };
typedef struct jazz_engine_SourceRole { u32 tag; jz_str name; } jazz_engine_SourceRole;
typedef struct jazz_engine_SourcePath { JZ_VEC(jazz_engine_SourceRole) components; } jazz_engine_SourcePath;
typedef struct jazz_engine_SourceId { jz_str table; jazz_engine_SourcePath path; } jazz_engine_SourceId;
void jazz_engine_SourceId_drop(void *s);
void jazz_engine_SourceId_clone(void *dst, const void *src);
int jazz_engine_SourceId_cmp(const void *a, const void *b, void *ctx);
bool jazz_engine_SourceId_eq(const jazz_engine_SourceId *a, const jazz_engine_SourceId *b);
void jazz_engine_SourceId_debug(jz_str *out, const jazz_engine_SourceId *s);
extern const jz_btree_desc jazz_engine_SourceId_set_desc; /* BTreeSet<SourceId> */
typedef struct jazz_engine_RowSetNodeId { jz_str s; } jazz_engine_RowSetNodeId;
typedef struct jazz_engine_FrontierId { jz_str s; } jazz_engine_FrontierId;
typedef struct jazz_engine_ClaimPath { jz_vec_str segments; } jazz_engine_ClaimPath;
void jazz_engine_ClaimPath_drop(void *c);
void jazz_engine_ClaimPath_clone(void *dst, const void *src);
typedef enum { JAZZ_PROV_CREATED_AT, JAZZ_PROV_CREATED_BY, JAZZ_PROV_UPDATED_AT, JAZZ_PROV_UPDATED_BY } jazz_engine_ProvenanceField;
enum { JAZZ_NVR_SOURCE_FIELD, JAZZ_NVR_PARAM, JAZZ_NVR_CLAIM, JAZZ_NVR_FRONTIER_COLUMN, JAZZ_NVR_ROW_ID, JAZZ_NVR_PROVENANCE, JAZZ_NVR_LITERAL, JAZZ_NVR_TEMPLATE_LITERAL };
enum { JAZZ_ROWID_SOURCE, JAZZ_ROWID_FRONTIER };
typedef struct jazz_engine_NormalizedValueRef {
    u32 tag;
    jazz_engine_SourceId source;      /* SourceField / Provenance / RowId(Source) */
    jz_str field;                     /* SourceField / FrontierColumn / Param name */
    jazz_engine_ClaimPath claim;      /* Claim */
    jazz_engine_FrontierId frontier;  /* FrontierColumn / RowId(Frontier) */
    u32 row_id_kind;                  /* RowId */
    jazz_engine_ProvenanceField provenance;
    jz_bytes literal;                 /* Literal */
    u32 template_slot;
} jazz_engine_NormalizedValueRef;
typedef JZ_VEC(jazz_engine_NormalizedValueRef) jazz_engine_ValueRefVec;
void jazz_engine_NormalizedValueRef_drop(void *v);
void jazz_engine_NormalizedValueRef_clone(void *dst, const void *src);
bool jazz_engine_NormalizedValueRef_eq(const jazz_engine_NormalizedValueRef *a, const jazz_engine_NormalizedValueRef *b);
int jazz_engine_NormalizedValueRef_cmp(const void *a, const void *b, void *ctx);
typedef enum { JAZZ_CMP_EQ, JAZZ_CMP_NE, JAZZ_CMP_LT, JAZZ_CMP_LTE, JAZZ_CMP_GT, JAZZ_CMP_GTE } jazz_engine_ComparisonOp;
enum { JAZZ_NP_TRUE, JAZZ_NP_FALSE, JAZZ_NP_COMPARE, JAZZ_NP_IN, JAZZ_NP_ARRAY_CONTAINS, JAZZ_NP_TEXT_CONTAINS,
       JAZZ_NP_IS_NULL, JAZZ_NP_IS_NOT_NULL, JAZZ_NP_ENUM_MATCH, JAZZ_NP_AND, JAZZ_NP_OR, JAZZ_NP_NOT };
typedef struct jazz_engine_PredicateExpr jazz_engine_PredicateExpr;
typedef JZ_VEC(jazz_engine_PredicateExpr) jazz_engine_PredicateVec;
struct jazz_engine_PredicateExpr {
    u32 tag;
    jazz_engine_NormalizedValueRef left, right; /* Compare(left,right) / In value=left / contains value=left needle=right / IsNull(left) / EnumMatch value=left */
    jazz_engine_ComparisonOp op;
    jazz_engine_ValueRefVec options;
    u32 case_tag;
    jazz_engine_PredicateExpr *inner;           /* EnumMatch payload / Not */
    jazz_engine_PredicateVec children;          /* And / Or */
};
void jazz_engine_PredicateExpr_drop(void *p);
void jazz_engine_PredicateExpr_clone(void *dst, const void *src);
bool jazz_engine_PredicateExpr_eq(const jazz_engine_PredicateExpr *a, const jazz_engine_PredicateExpr *b);
typedef enum { JAZZ_VIS_VISIBLE, JAZZ_VIS_INCLUDE_DELETED } jazz_engine_RowVisibility;
typedef enum { JAZZ_JM_INNER, JAZZ_JM_SEMI, JAZZ_JM_NULL_EXTEND } jazz_engine_JoinMode;
typedef enum { JAZZ_CR_OPTIONAL, JAZZ_CR_AT_LEAST_ONE, JAZZ_CR_MATCH } jazz_engine_CorrelationRequirement;
typedef enum { JAZZ_SORT_ASC, JAZZ_SORT_DESC } jazz_engine_SortDirection;
typedef struct jazz_engine_OrderKey { jazz_engine_NormalizedValueRef value; jazz_engine_SortDirection direction; } jazz_engine_OrderKey;
typedef struct jazz_engine_TypedOutputField { jz_str name; groove_ColumnType ty; } jazz_engine_TypedOutputField;
typedef enum { JAZZ_NAGG_COUNT, JAZZ_NAGG_SUM, JAZZ_NAGG_AVG, JAZZ_NAGG_MIN, JAZZ_NAGG_MAX } jazz_engine_AggregateFunction;
typedef struct jazz_engine_AggregateExpr { jazz_engine_TypedOutputField output; jazz_engine_AggregateFunction function; bool has_input; jazz_engine_NormalizedValueRef input; } jazz_engine_AggregateExpr;
typedef struct jazz_engine_ValueSourceColumn { jz_str name; jazz_engine_NormalizedValueRef value; groove_ColumnType ty; } jazz_engine_ValueSourceColumn;
typedef enum { JAZZ_VSM_BINDING, JAZZ_VSM_INLINE } jazz_engine_ValueSourceMode;
typedef struct jazz_engine_RowProjection { jazz_engine_TypedOutputField output; jazz_engine_NormalizedValueRef value; } jazz_engine_RowProjection;
typedef struct jazz_engine_UnionInput { jazz_engine_RowSetNodeId node; jz_str label; } jazz_engine_UnionInput;
typedef struct jazz_engine_ProgramPathId { jazz_engine_SourceId owner, child; } jazz_engine_ProgramPathId;
typedef struct jazz_engine_RecursionBound { u32 tag; u32 max_depth; } jazz_engine_RecursionBound;
enum { JAZZ_RSE_SOURCE, JAZZ_RSE_VALUE_SOURCE, JAZZ_RSE_FRONTIER_SOURCE, JAZZ_RSE_FILTER, JAZZ_RSE_JOIN, JAZZ_RSE_RECURSIVE,
       JAZZ_RSE_UNION, JAZZ_RSE_DISTINCT, JAZZ_RSE_PROJECT, JAZZ_RSE_CORRELATED_PATH, JAZZ_RSE_ORDER_BY, JAZZ_RSE_SLICE, JAZZ_RSE_AGGREGATE };
typedef struct jazz_engine_RowSetExpr {
    u32 tag;
    jazz_engine_SourceId source; jazz_engine_RowVisibility visibility;     /* Source */
    jz_str shape; JZ_VEC(jazz_engine_ValueSourceColumn) columns; jazz_engine_ValueSourceMode mode; /* ValueSource/FrontierSource */
    jazz_engine_FrontierId frontier;
    jazz_engine_RowSetNodeId input, right, step;                         /* Filter/Join(left=input)/Recursive(seed=input) ... */
    jazz_engine_PredicateExpr predicate;                                 /* Filter.predicate / Join.on / CorrelatedPath.correlation */
    jazz_engine_JoinMode join_mode;
    jazz_engine_NormalizedValueRef frontier_key; jazz_engine_ValueRefVec keys; /* Recursive dedupe / Distinct keys / Slice partition_by / Aggregate group_by */
    jazz_engine_RecursionBound bound;
    JZ_VEC(jazz_engine_UnionInput) inputs;
    JZ_VEC(jazz_engine_RowProjection) projections;
    jazz_engine_ProgramPathId path; jazz_engine_CorrelationRequirement requirement;
    JZ_VEC(jazz_engine_OrderKey) order_keys;
    bool has_limit; u32 limit; u32 offset; jazz_engine_ValueRefVec tie_breaker; bool has_rank_output; jazz_engine_TypedOutputField rank_output;
    JZ_VEC(jazz_engine_AggregateExpr) outputs;
} jazz_engine_RowSetExpr;
void jazz_engine_RowSetExpr_drop(void *e);
void jazz_engine_RowSetExpr_clone(void *dst, const void *src);
typedef struct jazz_engine_ClosurePathSegment { jazz_engine_SourceId parent, target; jz_str source_field; } jazz_engine_ClosurePathSegment;
typedef enum { JAZZ_CRG_REQUIRED, JAZZ_CRG_INNER } jazz_engine_ClosureRootGate;
typedef struct jazz_engine_ClosurePath { u32 tag; /*0 ImplicitRootReference 1 ExplicitInclude*/ jz_str id; JZ_VEC(jazz_engine_ClosurePathSegment) segments; bool has_root_gate; jazz_engine_ClosureRootGate root_gate; } jazz_engine_ClosurePath;
typedef struct jazz_engine_JoinContribution { bool has_parent; jazz_engine_SourceId parent; jz_str id; jazz_engine_SourceId source; jazz_engine_RowSetNodeId input; jazz_engine_PredicateExpr membership; } jazz_engine_JoinContribution;
typedef struct jazz_engine_InheritedContribution { jz_str id; jazz_engine_SourceId source; jazz_engine_RowSetNodeId input; jazz_engine_PredicateExpr membership; } jazz_engine_InheritedContribution;
typedef struct jazz_engine_ReachableContribution { jz_str id; jazz_engine_SourceId access_source, edge_source; jazz_engine_RowSetNodeId access_input; jz_str root_ref_field; } jazz_engine_ReachableContribution;
typedef struct jazz_engine_NormalizedShapeIdentity { jazz_model_ShapeId shape_id; jz_bytes canonical; } jazz_engine_NormalizedShapeIdentity;
enum { JAZZ_RRR_SOURCE, JAZZ_RRR_SOURCE_ALTERNATIVES, JAZZ_RRR_PROJECTED };
typedef struct jazz_engine_ResultRowRef { u32 tag; jazz_engine_SourceId source; jz_btree alternatives; jazz_engine_NormalizedValueRef row; bool has_discriminator; jazz_engine_NormalizedValueRef discriminator; } jazz_engine_ResultRowRef;
typedef struct jazz_engine_SyntheticIdentitySpec { jz_str table; jz_vec_str key_columns, revision_columns; } jazz_engine_SyntheticIdentitySpec;
enum { JAZZ_RID_REAL_ROW, JAZZ_RID_SYNTHETIC_TUPLE, JAZZ_RID_PATH_TUPLE };
typedef struct jazz_engine_ResultId { u32 tag; jz_str table; jazz_engine_ResultRowRef row; jazz_engine_SyntheticIdentitySpec identity; jazz_engine_ProgramPathId path; jazz_engine_ValueRefVec revision; } jazz_engine_ResultId;
void jazz_engine_ResultId_drop(void *r);
void jazz_engine_ResultId_clone(void *dst, const void *src);
typedef struct jazz_engine_NormalizedRowSetShape {
    jazz_engine_NormalizedShapeIdentity identity; jazz_engine_RowSetNodeId root; jazz_engine_ResultId result;
    jz_btree auxiliary_sources; /* set<SourceId> */
    JZ_VEC(jazz_engine_ClosurePath) closure_paths; JZ_VEC(jazz_engine_JoinContribution) join_contributions;
    JZ_VEC(jazz_engine_InheritedContribution) inherited_contributions; JZ_VEC(jazz_engine_ReachableContribution) reachable_contributions;
    jz_btree nodes; /* RowSetNodeId -> RowSetExpr */
} jazz_engine_NormalizedRowSetShape;
void jazz_engine_NormalizedRowSetShape_drop(void *s);
void jazz_engine_NormalizedRowSetShape_clone(void *dst, const void *src);
extern const jz_btree_desc jazz_engine_RowSetNodes_desc; /* RowSetNodeId(jz_str) -> RowSetExpr */
typedef struct jazz_engine_ProgramClaimParam { jazz_engine_ClaimPath path; groove_ColumnType ty; } jazz_engine_ProgramClaimParam;
typedef struct jazz_engine_ProgramBinding {
    jazz_model_BindingId id; bool has_source_shape; jz_str source_shape;
    jz_btree extra_user_params, param_types; /* String -> ColumnType */
    jz_btree claim_params; /* String -> ProgramClaimParam */ jz_btree values; /* String -> Value */
} jazz_engine_ProgramBinding;
void jazz_engine_ProgramBinding_drop(void *b);
typedef struct jazz_engine_RowSetProgramInput { jazz_engine_NormalizedRowSetShape shape; jazz_engine_ProgramBinding binding; } jazz_engine_RowSetProgramInput;
typedef enum { JAZZ_QAM_TRUSTED_SERVING, JAZZ_QAM_CLIENT_LOCAL } jazz_engine_QueryAuthorizationMode;
typedef enum { JAZZ_PDR_READ, JAZZ_PDR_WRITE } jazz_engine_PolicyDecisionRole;
typedef enum { JAZZ_PEM_PERMISSIVE_LOCAL, JAZZ_PEM_ENFORCING } jazz_engine_PolicyEnforcementMode;
enum { JAZZ_PC_SYSTEM, JAZZ_PC_IDENTITY, JAZZ_PC_AUTHORIZATION_SUBPLAN };
typedef struct jazz_engine_PolicyContext {
    u32 tag; jazz_engine_SourceId protected_source; jazz_engine_PolicyDecisionRole role; jazz_engine_PolicyEnforcementMode mode;
    jazz_types_AuthorSubject permission_subject; jz_btree claims; bool has_attribution; jazz_types_AuthorSubject attribution;
} jazz_engine_PolicyContext;
void jazz_engine_PolicyContext_drop(void *p);
void jazz_engine_PolicyContext_clone(void *dst, const void *src);
/* read views */
enum { JAZZ_SFS_CURRENT, JAZZ_SFS_EXPLICIT };
enum { JAZZ_SSS_COMPATIBLE, JAZZ_SSS_SINGLE, JAZZ_SSS_EXPLICIT };
enum { JAZZ_LENS_CANONICAL, JAZZ_LENS_FINGERPRINT };
typedef struct jazz_engine_SchemaProjection {
    u32 schema_family; jazz_types_SchemaFamilyId family_id;
    u32 storage; jazz_types_SchemaVersionId single; jz_btree explicit_versions;
    u32 lens; jz_bytes lens_fingerprint;
} jazz_engine_SchemaProjection;
void jazz_engine_SchemaProjection_drop(void *p);
void jazz_engine_SchemaProjection_clone(void *dst, const void *src);
enum { JAZZ_OVL_PENDING_LOCAL, JAZZ_OVL_DIRECT_BATCH, JAZZ_OVL_ACCEPTED_TX, JAZZ_OVL_OPEN_TX };
typedef struct jazz_engine_OverlayRef { u32 tag; jz_bytes batch; jazz_types_TxId tx; jazz_model_OpenTransactionId open_tx; } jazz_engine_OverlayRef;
typedef struct jazz_engine_OverlayStack { JZ_VEC(jazz_engine_OverlayRef) entries; } jazz_engine_OverlayStack;
enum { JAZZ_BVB_CURRENT, JAZZ_BVB_SNAPSHOT };
typedef struct jazz_engine_BranchViewSourceBase { u32 tag; jazz_model_BranchKey branch; jazz_model_SnapshotRef snapshot; } jazz_engine_BranchViewSourceBase;
void jazz_engine_BranchViewSourceBase_clone(void *dst, const void *src);
void jazz_engine_BranchViewSourceBase_drop(void *p);
enum { JAZZ_DS_CURRENT, JAZZ_DS_BRANCH };
typedef struct jazz_engine_DataSource { u32 tag; jazz_model_BranchKey branch; } jazz_engine_DataSource;
enum { JAZZ_SE_VISIBLE_CURRENT, JAZZ_SE_BRANCH_VIEW, JAZZ_SE_HISTORY_CUT, JAZZ_SE_SNAPSHOT_REF, JAZZ_SE_SETTLED_BINDING_VIEW,
       JAZZ_SE_WITH_OVERLAYS, JAZZ_SE_LENS_PROJECT, JAZZ_SE_MERGE };
typedef struct jazz_engine_SourceExpr jazz_engine_SourceExpr;
struct jazz_engine_SourceExpr {
    u32 tag;
    jazz_engine_SchemaProjection projection;
    jazz_engine_DataSource data;
    jazz_model_DurabilityTier tier;
    jazz_model_BranchKey head; jazz_engine_BranchViewSourceBase *base;
    jazz_types_GlobalTime position;
    jazz_model_Snapshot snapshot;
    bool current_default; jazz_protocol_BindingViewKey binding_view; jazz_protocol_AuthorityResultKey *authority_result_key;
    jazz_engine_SourceExpr *input; jazz_engine_OverlayStack overlays;
    JZ_VEC(jazz_engine_SourceExpr) merge_inputs; u32 merge_mode;
};
void jazz_engine_SourceExpr_drop(void *e);
void jazz_engine_SourceExpr_clone(void *dst, const void *src);
bool jazz_engine__query_engine__read__SourceExpr__current_tier(const jazz_engine_SourceExpr *e, jazz_model_DurabilityTier *out);
extern const jz_btree_desc jazz_engine_SourceGraph_desc; /* SourceId -> SourceExpr */
typedef struct jazz_engine_ReadView { jazz_types_SchemaVersionId read_schema, policy_schema; jz_btree sources; /* SourceId -> SourceExpr */ } jazz_engine_ReadView;
void jazz_engine_ReadView_drop(void *r);
void jazz_engine_ReadView_clone(void *dst, const void *src);
bool jazz_engine__query_engine__read__ReadView__source_current_tier(const jazz_engine_ReadView *r, const jazz_engine_SourceId *s, jazz_model_DurabilityTier *out);
typedef struct jazz_engine_RequestedReadSet { jazz_engine_ReadView primary; jz_btree fact_reads; } jazz_engine_RequestedReadSet;
/* outputs */
typedef enum { JAZZ_FP_ALL, JAZZ_FP_FIELDS } jazz_engine_FieldProjection_tag;
typedef struct jazz_engine_FieldProjection { u32 tag; jz_btree fields; } jazz_engine_FieldProjection;
typedef enum { JAZZ_PCARD_ONE, JAZZ_PCARD_MANY } jazz_engine_PathCardinality;
typedef enum { JAZZ_PHP_KEEP, JAZZ_PHP_DROP } jazz_engine_PathHolePolicy;
typedef struct jazz_engine_AppPathProjection jazz_engine_AppPathProjection;
struct jazz_engine_AppPathProjection { jazz_engine_ProgramPathId path; jz_str field; jazz_engine_PathCardinality cardinality; jazz_engine_FieldProjection fields; JZ_VEC(jazz_engine_AppPathProjection) children; jazz_engine_PathHolePolicy hole_policy; };
typedef struct jazz_engine_AppProjectionTree { jazz_engine_FieldProjection fields; JZ_VEC(jazz_engine_AppPathProjection) paths; } jazz_engine_AppProjectionTree;
enum { JAZZ_PP_SHAPE_DEFAULT, JAZZ_PP_RELATION, JAZZ_PP_TREE };
typedef struct jazz_engine_PayloadProjection { u32 tag; void *relation_columns; jazz_engine_AppProjectionTree tree; } jazz_engine_PayloadProjection;
typedef struct jazz_engine_AppRowOutputRequest { bool public_terminal; jazz_engine_PayloadProjection projection; } jazz_engine_AppRowOutputRequest;
enum { JAZZ_PFK_AUTHORIZED_ROWS, JAZZ_PFK_RESULT_MEMBERSHIP, JAZZ_PFK_RELATION_EDGES, JAZZ_PFK_PATH_CORRELATION_COVERAGE,
       JAZZ_PFK_PROGRAM_SOURCE_COVERAGE, JAZZ_PFK_READ_FRONTIER_SETTLED, JAZZ_PFK_COMPLETE_TX_PAYLOAD_COVERAGE,
       JAZZ_PFK_VIEW_COMPLETE_EXCLUSIVE_COVERAGE, JAZZ_PFK_VERSION_WITNESSES, JAZZ_PFK_REPLACEMENT_WITNESSES,
       JAZZ_PFK_POLICY_DECISION, JAZZ_PFK_POLICY_WITNESSES, JAZZ_PFK_CONTRIBUTING_MEMBERS, JAZZ_PFK_PREDICATE_READS,
       JAZZ_PFK_PREDICATE_OUTPUT_SET, JAZZ_PFK_POINT_READS };
typedef struct jazz_engine_ProgramFactKey { u32 tag; void *payload; bool present; u32 role; } jazz_engine_ProgramFactKey;
extern const jz_btree_desc jazz_engine_ProgramFactKey_set_desc;
typedef struct jazz_engine_RowSetOutputRequest { bool has_app_rows; jazz_engine_AppRowOutputRequest app_rows; jz_btree facts; } jazz_engine_RowSetOutputRequest;
typedef struct jazz_engine_QueryProgramRequest {
    jazz_engine_QueryAuthorizationMode authorization_mode; jazz_engine_RequestedReadSet reads; jazz_engine_PolicyContext policy;
    jazz_engine_RowSetProgramInput input; jazz_engine_RowSetOutputRequest output;
} jazz_engine_QueryProgramRequest;
void jazz_engine_QueryProgramRequest_drop(void *r);
void jazz_engine_QueryProgramRequest_clone(void *dst, const void *src);
void jazz_engine_QueryProgramRequest_debug(jz_str *out, const jazz_engine_QueryProgramRequest *r);
bool jazz_engine_QueryProgramRequest_eq(const jazz_engine_QueryProgramRequest *a, const jazz_engine_QueryProgramRequest *b);
/* resolver contract */
typedef struct jazz_engine_PolicyAuthorizationPlan {
    jazz_engine_SourceId protected_source; jazz_engine_PolicyDecisionRole role; jz_str protected_row_field;
    bool has_binding_source_shape; jz_str binding_source_shape; jz_btree binding_user_params; jz_btree binding_claim_params;
} jazz_engine_PolicyAuthorizationPlan;
enum { JAZZ_SAR_SYSTEM, JAZZ_SAR_POLICY_FILTERED, JAZZ_SAR_POLICY_PROOF };
typedef struct jazz_engine_SourceAuthorizationRequest { u32 tag; jazz_types_AuthorSubject permission_subject; jazz_engine_PolicyAuthorizationPlan plan; } jazz_engine_SourceAuthorizationRequest;
void jazz_engine_SourceAuthorizationRequest_drop(void *r);
void jazz_engine_SourceAuthorizationRequest_clone(void *dst, const void *src);
enum { JAZZ_SMR_VERSION_WITNESSES, JAZZ_SMR_SETTLE_POSITION, JAZZ_SMR_VERSION_PAYLOADS, JAZZ_SMR_DELETION_MARKERS,
       JAZZ_SMR_BATCH_MEMBERSHIP, JAZZ_SMR_COVERAGE, JAZZ_SMR_VALIDATION_READS, JAZZ_SMR_POLICY_WITNESSES,
       JAZZ_SMR_PROVENANCE, JAZZ_SMR_AUTHOR_PATH };
typedef struct jazz_engine_SourceMetadataRequirement { u32 tag; jazz_engine_ProvenanceField provenance; jz_str author_path; } jazz_engine_SourceMetadataRequirement;
int jazz_engine_SourceMetadataRequirement_cmp(const void *a, const void *b, void *ctx);
void jazz_engine_SourceMetadataRequirement_drop(void *r);
void jazz_engine_SourceMetadataRequirement_clone(void *dst, const void *src);
extern const jz_btree_desc jazz_engine_MetadataReq_set_desc;   /* set<SourceMetadataRequirement> */
extern const jz_btree_desc jazz_engine_MetadataFields_desc;    /* SourceMetadataRequirement -> SourceMetadataFields */
enum { JAZZ_FR_NONE, JAZZ_FR_ALL, JAZZ_FR_FIELDS };
typedef struct jazz_engine_FieldRequirement { u32 tag; jz_btree fields; } jazz_engine_FieldRequirement;
typedef struct jazz_engine_SourceRequirements { jazz_engine_FieldRequirement app_fields; jz_btree metadata; } jazz_engine_SourceRequirements;
void jazz_engine_SourceRequirements_clone(void *dst, const void *src);
typedef struct jazz_engine_SourceRequest { jazz_engine_SourceId source; jazz_engine_RowVisibility visibility; jazz_engine_SourceAuthorizationRequest authorization; jazz_engine_SourceRequirements requirements; } jazz_engine_SourceRequest;
void jazz_engine_SourceRequest_drop(void *r);
void jazz_engine_SourceRequest_clone(void *dst, const void *src);
void jazz_engine_SourceRequest_debug(jz_str *out, const jazz_engine_SourceRequest *r);
enum { JAZZ_SMF_VERSION_WITNESSES, JAZZ_SMF_SETTLE_POSITION, JAZZ_SMF_DELETION_MARKERS, JAZZ_SMF_BATCH_MEMBERSHIP,
       JAZZ_SMF_COVERAGE, JAZZ_SMF_VALIDATION_READS, JAZZ_SMF_POLICY_WITNESSES, JAZZ_SMF_PROVENANCE };
typedef struct jazz_engine_SourceMetadataFields {
    u32 tag; jz_str f0, f1, f2, f3; bool has_opt0, has_opt1; jz_str opt0, opt1;
} jazz_engine_SourceMetadataFields;
void jazz_engine_SourceMetadataFields_drop(void *f);
void jazz_engine_SourceMetadataFields_clone(void *dst, const void *src);
typedef struct jazz_engine_SourceRowShape { jazz_engine_SourceId source; groove_RecordDescriptor descriptor; jz_str row_uuid_field; jz_btree metadata; } jazz_engine_SourceRowShape;
typedef struct jazz_engine_ContentVersionSource { groove_GraphBuilder *graph; jz_str row_uuid_field; } jazz_engine_ContentVersionSource;
typedef struct jazz_engine_DeletionRegisterSource { groove_GraphBuilder *graph; jz_str row_uuid_field; } jazz_engine_DeletionRegisterSource;
typedef struct jazz_engine_AuthorizedDeletionPreimage { groove_GraphBuilder *graph; jz_btree routing_fields; } jazz_engine_AuthorizedDeletionPreimage;
typedef struct jazz_engine_ResolvedSource {
    jz_btree stored_column_ids; /* String -> PhysicalColumnId */
    jazz_model_TableSchema *table_schema; /* Arc */
    groove_GraphBuilder *graph; jazz_engine_SourceRowShape row_shape; jz_btree routing_fields; bool requires_result_payload;
    bool has_content_version; jazz_engine_ContentVersionSource content_version;
    bool has_deletion_register; jazz_engine_DeletionRegisterSource deletion_register;
    bool has_authorized_deletion_preimage; jazz_engine_AuthorizedDeletionPreimage authorized_deletion_preimage;
} jazz_engine_ResolvedSource;
void jazz_engine_ResolvedSource_drop(void *r);
enum { JAZZ_GAP_LOCAL_AVAILABILITY_INPUT, JAZZ_GAP_POLICY_PROOF_CYCLE, JAZZ_GAP_HISTORICAL_STORAGE_CUT, JAZZ_GAP_SNAPSHOT_REF,
       JAZZ_GAP_SCHEMA_PROJECTION, JAZZ_GAP_BRANCH_OVERLAY, JAZZ_GAP_TRANSACTION_READ_OVERLAY, JAZZ_GAP_COVERAGE };
typedef struct jazz_engine_SourceGap { u32 tag; jz_str table; u32 depth; } jazz_engine_SourceGap;
typedef struct jazz_engine_SourceResolutionError { jazz_engine_SourceRequest *request; jazz_engine_SourceGap gap; } jazz_engine_SourceResolutionError;
void jazz_engine_SourceResolutionError_drop(jazz_engine_SourceResolutionError *e);
/* Result<_, SourceResolutionError> encoding: jz_err code JZ_SRE_CODE with the boxed error in a side slot */
enum { JZ_SRE_CODE = 0x5e00 };
typedef struct jazz_engine_SourceGraphPreparerVtbl {
    jz_err (*prepare_source_graph)(void *self, const jazz_engine_SourceRequest *req, jazz_engine_ResolvedSource *out, jazz_engine_SourceResolutionError *err);
} jazz_engine_SourceGraphPreparerVtbl;
typedef struct jazz_engine_CapabilityReport { JZ_VEC(void *) gaps; void *explain; } jazz_engine_CapabilityReport;
void jazz_engine_CapabilityReport_debug(jz_str *out, const jazz_engine_CapabilityReport *r);
void jazz_engine_CapabilityReport_free(jazz_engine_CapabilityReport *r);
/* lowering / compiled programs (opaque) */
typedef struct jazz_engine_QueryProgram jazz_engine_QueryProgram;
typedef struct jazz_engine_QueryProgramCompilation jazz_engine_QueryProgramCompilation;
typedef struct jazz_engine_ProgramOutputSchemas jazz_engine_ProgramOutputSchemas;
typedef struct jazz_engine_AppRowSchema jazz_engine_AppRowSchema;
typedef struct jazz_engine_OutputTerminalSchema jazz_engine_OutputTerminalSchema;
typedef struct jazz_engine_ResultMembershipVersionSchema jazz_engine_ResultMembershipVersionSchema;
typedef struct jazz_engine_VersionIdentityFields jazz_engine_VersionIdentityFields;
typedef struct jazz_engine_VersionedRowRefSchema jazz_engine_VersionedRowRefSchema;
typedef struct jazz_engine_RowRefSchema jazz_engine_RowRefSchema;
/* query_program_source_requests(&request) -> CapabilityResult<Vec<SourceRequest>> */
jz_err jazz_engine__query_engine__lowering__query_program_source_requests(const jazz_engine_QueryProgramRequest *r, JZ_VEC(jazz_engine_SourceRequest) *out, jazz_engine_CapabilityReport **report);
/* field naming helpers */
jz_str jazz_engine__query_engine__fields__user_column_field(jz_slice column);
jz_str jazz_engine__query_engine__fields__table_user_column_field(jz_slice table, jz_slice column);
jz_str jazz_engine__query_engine__fields__left_field(jz_slice field);
jz_str jazz_engine__query_engine__fields__right_field(jz_slice field);
jz_str jazz_engine__query_engine__fields__route_param_field(jz_slice param);
jz_str jazz_engine__query_engine__fields__claim_param_field(const jazz_engine_ClaimPath *path);
bool jazz_engine__query_engine__fields__claim_path_from_param_field(jz_slice field, jazz_engine_ClaimPath *out);
jz_str jazz_engine__query_engine__fields__aggregate_output_column(jz_slice output);
jz_str jazz_engine__query_engine__fields__aggregate_output_field(jz_slice output);
jz_vec_str jazz_engine__query_engine__fields__current_row_field_names(const jazz_model_TableSchema *t);
bool jazz_engine__query_engine__lowering__authorized_deletion_preimage_source_request(const jazz_engine_SourceRequest *s, jazz_engine_SourceRequest *out);

/* ======================================================================
 * jazz_node (other units)
 * ====================================================================== */
/* node::Error codes (mirror of jazz_node::node::Error discriminants) */
enum {
    JZN_E_Groove = 1, JZN_E_ChunkStorage, JZN_E_LargeValueUploadChunkUnavailable, JZN_E_LargeValue, JZN_E_LargeValueReachability,
    JZN_E_LargeValueIngressRateLimited, JZN_E_LargeValueStageExpired, JZN_E_Record, JZN_E_MalformedCurrentRow, JZN_E_Storage,
    JZN_E_ClockOverflow, JZN_E_Query, JZN_E_QueryLowering, JZN_E_QueryCapability, JZN_E_AuthorizationSupportMissingClaim,
    JZN_E_PolicyProofCycle, JZN_E_TableNotFound, JZN_E_UnsupportedColumnType, JZN_E_InvalidMergeableCommit,
    JZN_E_UnadmittedWriteAuthor, JZN_E_InvalidBranchKey, JZN_E_UnsupportedExclusiveRead, JZN_E_TransactionConflict,
    JZN_E_InvalidStoredValue, JZN_E_InvalidAuthoritySourceClosure, JZN_E_MissingTransaction, JZN_E_MalformedViewUpdate,
    JZN_E_MaintainedViewMissingBundleWitness, JZN_E_MissingOpenBatch, JZN_E_DuplicateOpenBatch,
    JZN_E_OpenTransactionIdentityMismatch, JZN_E_NonMonotoneState, JZN_E_ConflictingCommitUnit, JZN_E_ConflictingFate,
    JZN_E_UnsupportedCommitUnit, JZN_E_UnsupportedSyncMessage, JZN_E_UnauthorizedCatalogueUpdate, JZN_E_InvalidCatalogueUpdate,
    JZN_E_CatalogueActivationFailed, JZN_E_CatalogueUninitialized, JZN_E_CatalogueCodec, JZN_E_HistoricalReadRequiresServer,
    JZN_E_AuthorizationDenied, JZN_E_SubscriptionClosed,
};
#define JZN_INVALID_STORED(msg) jz_err_fmt(JZN_E_InvalidStoredValue, "invalid stored value: %s", (msg))
#define JZN_QUERY_LOWERING(fmt, ...) jz_err_fmt(JZN_E_QueryLowering, "query lowering failed: " fmt, __VA_ARGS__)
#define JZN_QUERY_CAPABILITY(fmt, ...) jz_err_fmt(JZN_E_QueryCapability, "query capability unsupported: " fmt, __VA_ARGS__)
bool jazz_node__node__is_unrepresentable_enum_projection(const jz_err *e);
/* Error::Groove(e) wraps; is pass-through here */
static inline jz_err jzn_groove(jz_err e) { if (e.code) e.code = JZN_E_Groove; return e; }

typedef struct jazz_node_CurrentRow {
    jazz_types_RowUuid row_uuid; JZ_VEC(groove_Value) values; jazz_types_TxId tx;
    jazz_types_AuthorSubject created_by, updated_by; u64 created_at, updated_at;
    bool has_branch; jazz_model_BranchKey branch; bool deleted;
    bool has_occurrence; jazz_types_OutputOccurrenceId occurrence;
} jazz_node_CurrentRow;
typedef JZ_VEC(jazz_node_CurrentRow) jazz_node_CurrentRowVec;
void jazz_node_CurrentRow_drop(void *r);
void jazz_node_CurrentRow_clone(void *dst, const void *src);
void jazz_node_CurrentRowVec_drop(jazz_node_CurrentRowVec *v);
void jazz_node_CurrentRowVec_clone(jazz_node_CurrentRowVec *dst, const jazz_node_CurrentRowVec *src);
extern const jz_btree_desc jazz_node_CurrentRowsBySource_desc; /* SourceId -> Vec<CurrentRow> */

/* catalogue */
typedef struct jazz_node_PhysicalTableMapping { jazz_types_PhysicalTableId table_id; jz_btree columns; /* String -> PhysicalColumnId */ } jazz_node_PhysicalTableMapping;
typedef struct jazz_node_PhysicalMapping { jz_btree tables; /* String -> PhysicalTableMapping */ } jazz_node_PhysicalMapping;
typedef struct jazz_node_CatalogueSchema { jazz_model_RuntimeSchema *schema; } jazz_node_CatalogueSchema;
typedef struct jazz_node_SchemaCatalogue {
    jz_btree physical_mappings; /* SchemaVersionId -> PhysicalMapping */
    jz_btree catalogue_schemas; /* SchemaVersionId -> CatalogueSchema */
    jazz_types_SchemaVersionId local_schema_version_id;
} jazz_node_SchemaCatalogue;
typedef struct jazz_node_PreparedQueryPlan jazz_node_PreparedQueryPlan;
typedef struct jazz_node_PolicyProofStackEntry { jz_str table; void *lease; /* Weak<()> */ } jazz_node_PolicyProofStackEntry;
typedef struct jazz_node_QueryServing {
    jz_btree watermark_restore_seen, persisted_watermarks; bool watermarks_invalidated;
    jz_btree local_availability_records, local_availability_authorities, local_unavailable_inputs;
    jz_btree query_shape_cache;                       /* (ShapeId, tier, String) -> PreparedQueryPlanHandle */
    jz_btree read_policy_authorization_request_cache; /* key -> QueryProgramRequest */
    jz_btree compiled_query_program_cache;            /* String -> Arc<QueryProgram> */
    void *query_program_templates;                    /* QueryProgramTemplateCache */
    jz_deque supported_query_program_requests;        /* VecDeque<SupportedQueryProgram> */
    jz_btree policy_authorization_graph_cache;        /* String -> PolicyAuthorizationGraph */
    jz_btree policy_authorization_graph_replacements; /* String -> Vec<ScopedPolicyAuthorizationGraphReplacement> */
    JZ_VEC(jazz_node_PolicyProofStackEntry) policy_proof_stack;
    jz_btree tx_versions_cache; jz_deque tx_version_tables_cache_order; jz_btree tx_version_tables_cache_order_set;
    jz_btree version_storage_sources_cache;
    jz_btree registered_shapes;                       /* ShapeId -> ValidatedQuery */
    jz_btree registered_shape_options, peer_shape_owners, locally_registered_shapes, outbound_shape_owners, outbound_binding_owners;
    jz_btree registered_bindings, authority_results, applied_view_update_generations, retained_root_window_sources;
} jazz_node_QueryServing;
typedef struct jazz_node_QueryEngineReadMetrics {
    u64 source_full_scans, source_primary_key_scans, source_index_probes, source_global_time_range_scans,
        source_branch_register_full_scans, local_mask_count_fallbacks, policy_authorized_source_joins,
        source_index_candidate_filters, source_capped_scans;
} jazz_node_QueryEngineReadMetrics;
typedef struct jazz_node_NodeState {
    jazz_types_NodeUuid node_uuid;
    jazz_node_SchemaCatalogue catalogue;
    jazz_node_QueryServing query;
    groove_Database database;
    u64 groove_runtime_token;
    u64 physical_identity_generation;
    bool history_complete;
    jazz_model_DurabilityTier authored_commit_durability;
    void *relay_authority_session_owner;
    jz_hmap client_local_literal_shapes;
    void *open_tx;
    jazz_node_QueryEngineReadMetrics query_engine_read_metrics;
    void *ahead_current_keys;
    bool has_self_node_alias; u64 self_node_alias;
} jazz_node_NodeState;

/* NodeState methods implemented by other units */
jz_err jazz_node__node__NodeState__table_in_schema(jazz_node_NodeState *n, jz_slice table, jazz_types_SchemaVersionId v, jazz_model_TableSchema *out);
jz_err jazz_node__node__physical__NodeState__scope_physical_table(jazz_node_NodeState *n, jazz_types_SchemaVersionId v, jz_slice table, jazz_types_PhysicalTableId *out);
jz_err jazz_node__node__NodeState__ensure_schema_version_alias(jazz_node_NodeState *n, jazz_types_SchemaVersionId v, u32 *out);
jz_str jazz_node__node__physical__physical_global_current_table_name(jazz_types_PhysicalTableId id);
jz_str jazz_node__node__physical__physical_local_current_table_name(jazz_types_PhysicalTableId id);
jz_str jazz_node__node__physical__physical_current_index_name(jazz_types_PhysicalTableId id, jz_slice column);
jz_str jazz_node__node__physical__physical_current_composite_index_name(jazz_types_PhysicalTableId id, jz_slice a, jz_slice b);
jz_err jazz_node__node__physical__NodeState__ensure_physical_current_projection_for_enum_columns(jazz_node_NodeState *n, jazz_types_SchemaVersionId v, jz_slice table, const jz_btree *cols, jz_str *out);
jz_err jazz_node__node__physical__NodeState__physical_global_marker_page_graph(jazz_node_NodeState *n, jazz_types_SchemaVersionId v, jz_slice table, groove_StaticScanSpec scan, groove_GraphBuilder **out);

/* ---- more jazz_node externals (read_sources) ---- */

typedef enum { JAZZ_PCC_GLOBAL, JAZZ_PCC_AHEAD, JAZZ_PCC_LOCAL_DELTA, JAZZ_PCC_EDGE } jazz_node_PhysicalCurrentClass;
typedef enum { JAZZ_PBM_INLINE_ALL_REACHABLE_SEEDS, JAZZ_PBM_RETAIN_ALL_PARAMS } jazz_node_ParamBindingMode;
typedef struct jazz_node_PolicyAuthorizationGraph jazz_node_PolicyAuthorizationGraph; /* defined in lowering.h */
/* refs: Ref = Local | Remote(Origin|Edge) */
typedef struct jazz_node_Ref { u32 tag; u32 remote; } jazz_node_Ref;
enum { JAZZ_REF_LOCAL, JAZZ_REF_REMOTE };
enum { JAZZ_REMOTE_ORIGIN, JAZZ_REMOTE_EDGE };
const jazz_node_Ref *jazz_node__node__refs__Ref__stack(jazz_model_DurabilityTier tier, u32 *n);
jazz_node_PhysicalCurrentClass jazz_node__node__refs__Ref__physical_class(jazz_node_Ref r);
bool jazz_node_Ref_eq(jazz_node_Ref a, jazz_node_Ref b);
/* layer_stack */
typedef struct jazz_node_Layer jazz_node_Layer; /* heap */
jazz_node_Layer *jazz_node__node__layer_stack__Layer__new(groove_GraphBuilder *rows);
jazz_node_Layer *jazz_node__node__layer_stack__Layer__masked_by_presence(groove_GraphBuilder *rows, groove_GraphBuilder *presence);
groove_GraphBuilder *jazz_node__node__layer_stack__compose(jazz_node_Layer **layers, u32 n);
groove_GraphBuilder *jazz_node__node__layer_stack__exposed(groove_GraphBuilder *rows, jazz_node_Layer *const *above, u32 n);
void jazz_node_Layer_free(jazz_node_Layer *l);
/* try_ref_layers(tier, |r| -> Result<Layer, E>) */
typedef void *(*jazz_node_ref_layer_fn)(void *ctx, jazz_node_Ref r, bool *failed);
typedef JZ_VEC(jazz_node_Layer *) jazz_node_LayerVec;
jazz_node_LayerVec jazz_node__node__layer_stack__try_ref_layers(jazz_model_DurabilityTier tier, jazz_node_ref_layer_fn f, void *ctx, bool *failed);

/* query_eval::unavailable_inputs (unit node-qe-b) */
bool jazz_node__node__query_eval__unavailable_inputs__is_current_app_source(const jazz_engine_SourceExpr *e);
jz_err jazz_node__node__query_eval__unavailable_inputs__NodeState__exclude_local_unavailable_graph(jazz_node_NodeState *n, const jazz_protocol_PolicyBindingKey *scope, jazz_types_SchemaVersionId schema, const jazz_engine_SourceRequest *req, groove_GraphBuilder *graph, bool pending_ahead, groove_GraphBuilder **out);
/* query_eval::authorization (unit node-qe-b) */
jz_err jazz_node__node__query_eval__authorization__NodeState__table_read_policy_authorization_request(jazz_node_NodeState *n, jazz_types_SchemaVersionId policy_schema, jz_slice table, jazz_types_AuthorSubject identity, jazz_node_ParamBindingMode mode, jazz_model_DurabilityTier tier, bool has_shape, jz_str shape, jz_btree user_params, jz_btree claim_params, jazz_engine_QueryProgramRequest *out);
jz_err jazz_node__node__query_eval__authorization__NodeState__table_read_policy_authorization_request_at(jazz_node_NodeState *n, jazz_types_SchemaVersionId policy_schema, jz_slice table, jazz_types_AuthorSubject identity, jazz_node_ParamBindingMode mode, jazz_types_GlobalTime position, bool has_shape, jz_str shape, jz_btree user_params, jz_btree claim_params, jazz_engine_QueryProgramRequest *out);
jz_err jazz_node__node__query_eval__authorization__NodeState__table_read_policy_authorization_request_for_include_deleted(jazz_node_NodeState *n, jazz_types_SchemaVersionId policy_schema, jz_slice table, jazz_types_AuthorSubject identity, jazz_model_DurabilityTier tier, bool has_shape, jz_str shape, jz_btree user_params, jz_btree claim_params, jazz_engine_QueryProgramRequest *out);
/* policy_request: Result<QueryProgramRequest, Error> passed as (err, value) */
jz_err jazz_node__node__query_eval__authorization__NodeState__compose_policy_filtered_current_source_graph(jazz_node_NodeState *n, jz_err policy_request_err, jazz_engine_QueryProgramRequest *policy_request, groove_GraphBuilder *base, const jz_str *output_fields, u32 n_fields, jazz_node_PolicyAuthorizationGraph *out);
/* source_resolution */
typedef JZ_VEC(jazz_model_BranchKey) jazz_model_BranchKeyVec;
extern const jz_btree_desc jazz_node_BranchKeySet_desc;
jz_err jazz_node__node__source_resolution__NodeState__equivalent_stored_branch_keys(jazz_node_NodeState *n, jz_slice table, jazz_types_SchemaVersionId schema, const jazz_model_BranchKey *key, jz_btree *out);
jz_err jazz_node__node__source_resolution__NodeState__branch_snapshot_rows_for_schema(jazz_node_NodeState *n, jz_slice table, jazz_types_SchemaVersionId schema, const jazz_model_BranchKey *head, const jazz_model_BranchKey *base, const jazz_model_SnapshotRef *snapshot, jazz_node_CurrentRowVec *rows, jazz_node_CurrentRowVec *deleted);
jz_err jazz_node__node__source_resolution__NodeState__branch_view_rows_for_schema(jazz_node_NodeState *n, jz_slice table, jazz_types_SchemaVersionId schema, jazz_model_DurabilityTier tier, const jazz_model_BranchKey *head, const jazz_engine_BranchViewSourceBase *base, jazz_node_CurrentRowVec *out);
jz_err jazz_node__node__source_resolution__NodeState__projected_snapshot_current_rows(jazz_node_NodeState *n, jz_slice table, jazz_types_SchemaVersionId schema, const jazz_model_Snapshot *snapshot, jazz_node_CurrentRowVec *out);
jz_err jazz_node__node__source_resolution__NodeState__projected_historical_current_rows(jazz_node_NodeState *n, jz_slice table, jazz_types_SchemaVersionId schema, jazz_types_GlobalTime position, jazz_node_CurrentRowVec *out);
/* open_tx */
jz_err jazz_node__node__open_tx__NodeState__claim_tx_source_read(jazz_node_NodeState *n, jazz_model_OpenTransactionId tx, const jazz_engine_SourceId *source);
jz_err jazz_node__node__open_tx__NodeState__tx_current_rows_in_schema_with_options(jazz_node_NodeState *n, jazz_model_OpenTransactionId tx, jazz_types_SchemaVersionId schema, jz_slice table, bool include_deleted, jazz_node_CurrentRowVec *out);
bool jazz_node__node__CurrentRow__is_deleted(const jazz_node_CurrentRow *r);
/* physical::projections */
jz_err jazz_node__node__physical__projections__NodeState__physical_current_source_graph_with_projection_target(jazz_node_NodeState *n, jazz_types_SchemaVersionId schema, jz_slice table, jazz_node_PhysicalCurrentClass cls, jz_str target, groove_GraphBuilder **out);
jz_err jazz_node__node__physical__projections__NodeState__physical_current_source_scan_graph_with_projection_target(jazz_node_NodeState *n, jazz_types_SchemaVersionId schema, jz_slice table, jazz_node_PhysicalCurrentClass cls, jz_str target, groove_StaticScanSpec scan, groove_GraphBuilder **out);
jz_err jazz_node__node__physical__projections__NodeState__physical_current_branch_source_graph_with_projection_target(jazz_node_NodeState *n, jazz_types_SchemaVersionId schema, jz_slice table, jazz_node_PhysicalCurrentClass cls, jz_str target, const jazz_model_BranchKey *key, const groove_StaticScanSpec *scan, groove_GraphBuilder **out);
jz_err jazz_node__node__physical__projections__NodeState__physical_current_marker_source_graph(const jazz_node_NodeState *n, jazz_types_SchemaVersionId schema, jz_slice table, jazz_node_PhysicalCurrentClass cls, const jazz_model_BranchKey *key, const groove_StaticScanSpec *scan, groove_GraphBuilder **out);
jz_err jazz_node__node__physical__projections__NodeState__ensure_physical_current_winner_projection(jazz_node_NodeState *n, jazz_types_SchemaVersionId schema, jz_slice table, jz_str *target, jz_vec_str *fields);
jz_err jazz_node__node__physical__projections__NodeState__physical_current_post_winner_projection_fields(jazz_node_NodeState *n, jazz_types_SchemaVersionId schema, jz_slice table, const jz_btree *required, groove_ProjectFieldVec *out);
jz_err jazz_node__node__physical__NodeState__physical_table_id_for_schema(const jazz_node_NodeState *n, jazz_types_SchemaVersionId schema, jz_slice table, jazz_types_PhysicalTableId *out);
jz_str jazz_node__node__codec__ahead_current_table_name(jz_slice table);
groove_PredicateExpr *jazz_node__node__codec__not_deleted_predicate(void);
jz_str jazz_node__node__codec__global_current_table_name(jz_slice table);
jz_str jazz_node__node__physical__local_current_table_name(jz_slice table);
jz_str jazz_node__node__physical__deletion_register_table_name(jz_slice table, jazz_model_DurabilityTier tier);
u32 jazz_node__node__AheadCurrentKeys__branch_rows(const void *keys, jazz_types_PhysicalTableId table, const jazz_model_BranchKey *key);
jz_err groove__db__Database__graph_output_descriptor(groove_Database *db, const groove_GraphBuilder *g, groove_RecordDescriptor *out);
void jazz_types__debug_env__eprint(jz_str line);


/* ---- more externals (read_sources tail) ---- */
typedef struct jazz_node_RowProvenance { jazz_types_AuthorSubject created_by; u64 created_at; jazz_types_AuthorSubject updated_by; u64 updated_at; } jazz_node_RowProvenance;
bool jazz_node__node__CurrentRow__cell(const jazz_node_CurrentRow *r, const jazz_model_TableSchema *t, jz_slice column, groove_Value *out);
jz_err jazz_node__node__CurrentRow__provenance(const jazz_node_CurrentRow *r, bool *some, jazz_node_RowProvenance *out);
bool jazz_node__node__CurrentRow__projected_tx_alias(const jazz_node_CurrentRow *r, u64 *tx_time, u64 *node_alias);
jz_err jazz_node__node__query_eval__prepared_bindings__prepared_claim_value(const jazz_engine_ClaimPath *path, const jazz_engine_PolicyContext *policy, bool *has, groove_Value *out);
jz_vec_str jazz_model__schema__TableSchema__global_current_indexed_columns(const jazz_model_TableSchema *t); /* BTreeSet<String> in order */
jz_err groove__records__Value__from_postcard(jz_slice bytes, groove_Value *out);
bool groove__records__values__Value__record_get(const groove_Value *record, jz_slice member, groove_Value *out);
typedef struct groove_IndexScan { jz_str index; groove_StaticScanSpec scan; } groove_IndexScan; /* (String, StaticScanSpec) */
typedef JZ_VEC(groove_IndexScan) groove_IndexScanVec;
typedef struct groove_IndexCandidateFilter { jz_str table; jz_str index; groove_StaticScanSpec scan; jz_str source_column; jz_str candidate_column; } groove_IndexCandidateFilter;
jz_err jazz_node__node__physical__NodeState__physical_current_table_for_schema(const jazz_node_NodeState *n, jazz_types_SchemaVersionId v, jz_slice table, jazz_node_PhysicalCurrentClass cls, jz_str *out);
jz_str jazz_node__node__physical__physical_user_column_field(jazz_types_PhysicalColumnId id);
jz_str jazz_node__node__physical__physical_current_composite_index_name_ids(const jazz_types_PhysicalColumnId *ids, u32 n);
jz_str jazz_node__node__physical__physical_current_index_name_id(jazz_types_PhysicalColumnId id);
jazz_engine_ProgramBinding jazz_node__node__query_eval__bindings__NodeState__program_binding_for_shape(const jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, const jz_str *source_shape, jz_btree extra_user_params, jz_btree claim_params);
jazz_engine_RequestedReadSet jazz_node__node__query_eval__query_read_sets__current_query_read_set(const jazz_engine_NormalizedRowSetShape *shape, jazz_types_SchemaVersionId read_schema, jazz_types_SchemaVersionId policy_schema, jazz_model_DurabilityTier tier, const jazz_protocol_BindingViewKey *settled_binding_view, const jazz_protocol_AuthorityResultKey *settled_authority_result_key, bool settled_current_default);
void jazz_engine_RequestedReadSet_drop(void *r);
extern const jz_btree_desc jazz_engine_ColumnTypes_desc;  /* String -> ColumnType */
extern const jz_btree_desc jazz_engine_ProgramClaimParams_desc; /* String -> ProgramClaimParam */

void jazz_model_Snapshot_clone(void *dst, const void *src);
void jazz_model_Snapshot_drop(void *s);
void jazz_engine_OverlayStack_clone(void *dst, const void *src);
void jazz_engine_OverlayStack_drop(void *s);
groove_ProjectField *groove__ivm__graph__ProjectField__record_field(jz_slice root, const char *const *path, u32 n, jz_str name);
groove_ProjectField *groove__ivm__graph__ProjectField__literal_with_identity(jz_slice out, groove_LiteralValue *v, jz_str identity);
bool jazz_types__ids__AuthorSubject__metadata_path(jz_slice column, jz_slice *root, const char *const **path, u32 *n);
const groove_ValueType *jazz_types__ids__AuthorSubject__metadata_type(jz_slice column);
groove_ValueType jazz_types__ids__RowAuthor__value_type(void);
jz_err jazz_types__ids__RowAuthor__from_subject_value(jazz_types_AuthorSubject a, groove_Value *out);
typedef struct jazz_model_EncodedBranchValue { jz_bytes bytes; } jazz_model_EncodedBranchValue;
typedef struct jazz_model_BranchKeyValue { jz_str column; jazz_model_EncodedBranchValue value; } jazz_model_BranchKeyValue;
const jazz_model_BranchKeyValue *jazz_model__branch__BranchKey__values(const jazz_model_BranchKey *k, u32 *n);
jz_err jazz_model__branch__EncodedBranchValue__decode(const jazz_model_EncodedBranchValue *v, groove_Value *out);
const groove_TableSchema *jazz_model__schema__TableSchema__global_current_storage_table(const jazz_model_TableSchema *t);
bool jazz_model__schema__TableSchema__has_any_policy(const jazz_model_TableSchema *t);
groove_ValueType jazz_node__node__codec__current_row_column_type(const jazz_model_ColumnSchema *column);
groove_GraphBuilder *jazz_node__node__codec__visible_current_graph(const jazz_model_TableSchema *table, jazz_model_DurabilityTier settled);
jz_err jazz_node__node__query_eval__authorization__NodeState__maintained_view_content_current_with_version(jazz_node_NodeState *n, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, groove_GraphBuilder **out);
/* Error::PolicyProofCycle { table, depth } payload accessor */
bool jazz_node__Error__as_policy_proof_cycle(const jz_err *e, jz_str *table, u32 *depth);
jz_err jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(const jazz_node_NodeState *n, jz_slice table, jazz_types_SchemaVersionId schema, const jazz_model_TableSchema **out);

/* query_eval.rs (this unit) entry used by read_sources */
jz_err jazz_node__node__query_eval__NodeState__bounded_historical_current_rows(jazz_node_NodeState *n, jz_slice table, jazz_types_GlobalTime position, jazz_node_CurrentRowVec *out);

/* ---- more externals (normalization) ---- */
void jazz_model_RelationQuery_clone(jazz_model_RelationQuery *dst, const jazz_model_RelationExpr *rel);
typedef struct jazz_model_RelationColumnRef { bool has_scope; jz_str scope; jz_str column; } jazz_model_RelationColumnRef;
enum { JAZZ_RPE_COLUMN, JAZZ_RPE_ROW_ID };
enum { JAZZ_RRID_CURRENT, JAZZ_RRID_OUTER, JAZZ_RRID_FRONTIER };
typedef struct jazz_model_RelationProjectColumn { jz_str alias; u32 expr_tag; u32 row_id; jazz_model_RelationColumnRef column; } jazz_model_RelationProjectColumn;
typedef JZ_VEC(jazz_model_RelationProjectColumn) jazz_model_RelationProjectColumnVec;
void jazz_model_RelationProjectColumnVec_drop(jazz_model_RelationProjectColumnVec *v);
typedef struct jazz_model_RelationUnionArm { jz_str label; jazz_model_RelationExpr *input; } jazz_model_RelationUnionArm;
typedef struct jazz_model_RelationOrderTerm { jazz_model_RelationColumnRef column; jazz_model_OrderDirection direction; } jazz_model_RelationOrderTerm;
typedef struct jazz_model_RelationUnionParts {
    JZ_VEC(jazz_model_RelationUnionArm) inputs; bool has_order_by; JZ_VEC(jazz_model_RelationOrderTerm) order_by;
    bool has_limit; u32 limit; bool has_offset; u32 offset;
} jazz_model_RelationUnionParts;
void jazz_model_RelationUnionParts_drop(jazz_model_RelationUnionParts *p);
bool jazz_model__query__relation_union_parts(const jazz_model_RelationExpr *e, jazz_model_RelationUnionParts *out);
jz_err jazz_model__query__relation_output_projection(const jazz_model_RelationQuery *r, jz_str *scope, jazz_model_RelationProjectColumnVec *out);
jz_err jazz_model__query__relation_output_projection_if_present(const jazz_model_RelationQuery *r, bool *has, jazz_model_RelationProjectColumnVec *out);
jz_err jazz_model__query__relation_scope(const jazz_model_RelationColumnRef *c, jz_str *out);
jz_err jazz_model__query__flat_join_predicate_sources(const jazz_model_Predicate *p, jz_btree *out);
jz_err jazz_model__query__unqualify_flat_join_predicate(const jazz_model_Predicate *p, jz_slice scope, jazz_model_Predicate *out);
jz_err jazz_model__query__validate_union_arm_with_schema_version(const jazz_model_Query *q, const jazz_model_RuntimeSchema *schema, jazz_types_SchemaVersionId v, jazz_model_ValidatedQuery *out);
void jazz_model_Predicate_serialize(jz_pc_w *w, const jazz_model_Predicate *p);
void jazz_model_JoinVia_serialize(jz_pc_w *w, const jazz_model_JoinVia *j);
void jazz_model_ReachableVia_serialize(jz_pc_w *w, const jazz_model_ReachableVia *r);
void jazz_model_InheritsVia_serialize(jz_pc_w *w, const jazz_model_InheritsVia *i);
jz_err groove__records__Value__to_postcard(const groove_Value *v, jz_bytes *out);
void groove_ValueType_debug(jz_str *out, const groove_ValueType *t);
const jazz_model_RuntimeSchema *jazz_node__node__state__catalogue__SchemaCatalogue__schema_for_version(const jazz_node_SchemaCatalogue *c, jazz_types_SchemaVersionId v);
/* query_eval::bindings / policy_factoring / prepared_bindings (unit node-qe-b) */
typedef JZ_VEC(jazz_model_PolicyBranch) jazz_model_PolicyBranchVec;
typedef JZ_VEC(jazz_model_PolicyBranchVec) jazz_model_PolicyBranchVecVec;
void jazz_model_PolicyBranchVecVec_drop(jazz_model_PolicyBranchVecVec *v);
bool jazz_node__node__query_eval__policy_factoring__factor_policy_branches(const jazz_model_PolicyBranch *branches, u32 n, jazz_model_PolicyBranchVecVec *out);
jazz_model_Query jazz_node__node__query_eval__bindings__authorization_query_from_read_policy(const jazz_model_TableSchema *table);
void jazz_node__node__query_eval__bindings__bind_scope_claim_operands(jazz_model_Query *query, const jz_btree *claim_values, jz_btree *binding_values);
jz_err jazz_node__node__query_eval__bindings__disambiguate_policy_claim_params(jazz_model_Query *query, const jazz_model_RuntimeSchema *schema, jz_btree *binding_values, jz_btree *out);
void jazz_node__node__query_eval__bindings__retarget_binding_value_sources(jazz_engine_NormalizedRowSetShape *shape, const jz_str *binding_source_shape);
jz_btree jazz_node__node__query_eval__bindings__binding_claim_params_for_shape(const jazz_engine_NormalizedRowSetShape *shape, const jz_btree *param_types);
jz_btree jazz_node__node__query_eval__bindings__normalized_source_tables(const jazz_engine_NormalizedRowSetShape *shape);
jz_str jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_parts(const jz_btree *param_types, const jz_btree *claim_params);

/* ---- more externals (query_eval.rs, part A) ---- */
/* catalogue fields read by query_eval.rs (SchemaCatalogue.schema / active_schema) */
const jazz_model_RuntimeSchema *jazz_node__node__state__catalogue__SchemaCatalogue__schema(const jazz_node_SchemaCatalogue *c);
jazz_types_SchemaVersionId jazz_node__node__state__catalogue__SchemaCatalogue__active_schema_version(const jazz_node_SchemaCatalogue *c);
const jazz_model_RuntimeSchema *jazz_node__node__state__catalogue__SchemaCatalogue__active_compiled(const jazz_node_SchemaCatalogue *c);
extern const u32 jazz_node__node__TransactionRowRecord__FIELD_GLOBAL_TIME_IDX;
bool jazz_types_SchemaVersionId_eq(jazz_types_SchemaVersionId a, jazz_types_SchemaVersionId b);
bool jazz_model_ShapeId_eq(jazz_model_ShapeId a, jazz_model_ShapeId b);
void jazz_types_AuthorSubject_debug(jz_str *out, jazz_types_AuthorSubject a);
void jazz_engine_PolicyContext_debug(jz_str *out, const jazz_engine_PolicyContext *p);
void jazz_engine_CapabilityReport_debug(jz_str *out, const jazz_engine_CapabilityReport *r);
void jazz_engine_CapabilityReport_free(jazz_engine_CapabilityReport *r);
bool jazz_protocol__protocol__ReadViewSpec__is_default(const jazz_protocol_ReadViewSpec *r);
void jazz_engine_SourceRequest_drop(void *r);
void jazz_engine_ProgramFactKey_drop(void *k);
/* registry (node/mod.rs) */
typedef struct jazz_node_RegisteredBinding {
    groove_ValueVec values; jazz_protocol_RegisterShapeOptions options;
    jazz_types_AuthorSubject compiler_identity; jazz_protocol_BindingViewKey binding_view_key;
} jazz_node_RegisteredBinding;
const jazz_node_RegisteredBinding *jazz_node__node__query_eval__subscriptions__NodeState__unique_registered_binding_for_subscription(const jazz_node_NodeState *n, const jazz_protocol_SubscriptionKey *k);
extern const jz_btree_desc jazz_node_CompiledScopeTables_desc; /* PhysicalTableId -> Arc<str> */
extern const jz_btree_desc jazz_model_StrColumnType_desc;     /* String -> ColumnType */
int jazz_types_PhysicalTableId_cmp(const void *a, const void *b, void *ctx);
/* authorization / policy / lifecycle */
jazz_engine_PolicyContext jazz_node__node__query_eval__authorization__NodeState__query_program_policy_context(const jazz_node_NodeState *n, jazz_types_AuthorSubject identity);
jazz_types_SchemaVersionId jazz_node__node__policy__NodeState__read_policy_schema_for_table_name(const jazz_node_NodeState *n, jz_slice table, jazz_types_SchemaVersionId schema, const jazz_engine_NormalizedRowSetShape *shape);
bool jazz_node__node__state__lifecycle__NodeState__active_session_claim_scope_key(const jazz_node_NodeState *n, jazz_types_AuthorSubject identity, jz_str *out);
/* query_read_sets / bindings / prepared_bindings (unit node-qe-b) */
jazz_engine_RequestedReadSet jazz_node__node__query_eval__query_read_sets__historical_query_read_set(const jazz_engine_NormalizedRowSetShape *shape, jazz_types_SchemaVersionId schema_version, jazz_types_GlobalTime position);
jazz_engine_RequestedReadSet jazz_node__node__query_eval__query_read_sets__snapshot_query_read_set(const jazz_engine_NormalizedRowSetShape *shape, jazz_types_SchemaVersionId schema_version, const jazz_model_Snapshot *snapshot);
jazz_engine_RequestedReadSet jazz_node__node__query_eval__query_read_sets__tx_query_read_set(const jazz_engine_NormalizedRowSetShape *shape, jazz_types_SchemaVersionId schema_version, jazz_model_OpenTransactionId tx_id, const jazz_model_Snapshot *snapshot);
jz_err jazz_node__node__query_eval__query_read_sets__query_read_set_for_read_view(const jazz_engine_NormalizedRowSetShape *shape, jazz_types_SchemaVersionId read_schema, jazz_types_SchemaVersionId policy_schema, jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view, const jazz_protocol_BindingViewKey *settled_binding_view, const jazz_protocol_AuthorityResultKey *settled_authority_result_key, const jazz_model_RuntimeSchema *schema, jazz_engine_RequestedReadSet *out);
bool jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_parts_if_needed(const jz_btree *param_types, const jz_btree *claim_params, jz_str *out);
jz_err jazz_node__node__query_eval__bindings__inline_snapshot_bind_filter_literals(const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, const jazz_model_RuntimeSchema *schema, jazz_model_ValidatedQuery *out);
jz_err jazz_node__node__query_eval__bindings__NodeState__program_binding_for_shape_and_policy_with_prepared_claim_mode(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, const jz_str *source_shape, jz_btree extra_user_params, jz_btree claim_params, const jazz_engine_PolicyContext *policy, u32 mode, jazz_engine_ProgramBinding *out);
/* open_tx */
typedef struct jazz_node_OpenTransaction { jazz_model_Snapshot base_snapshot; } jazz_node_OpenTransaction;
jz_err jazz_node__node__open_tx__NodeState__open_tx(const jazz_node_NodeState *n, jazz_model_OpenTransactionId id, const jazz_node_OpenTransaction **out);
/* physical projections */
jz_err jazz_node__node__physical__projections__NodeState__physical_global_marker_index_graph(jazz_node_NodeState *n, jazz_types_SchemaVersionId v, jz_slice table, jz_str index, groove_StaticScanSpec scan, groove_GraphBuilder **out);
jz_str jazz_node__node__physical__physical_current_composite_index_name_ids(const jazz_types_PhysicalColumnId *ids, u32 n);
groove_LiteralVec groove_LiteralVec_new(void);
void groove_LiteralVec_push(groove_LiteralVec *v, groove_LiteralValue *l);
/* maintained_views (unit node-qe-b) */
typedef struct jazz_node_CoveredInputSource { groove_InputSourceId id; groove_RecordDescriptor descriptor; jz_str table; } jazz_node_CoveredInputSource;
extern const jz_btree_desc jazz_node_CoveredInputSources_desc; /* GlobalPhysicalTableId -> CoveredInputSource */
typedef struct jazz_node_CoveredInputReceiver jazz_node_CoveredInputReceiver; /* heap */
jazz_node_CoveredInputReceiver *jazz_node__node__query_eval__maintained_views__CoveredInputReceiver__new(jz_btree sources, jazz_protocol_ReadViewSpec read_view);
const jz_btree *jazz_node__node__query_eval__maintained_views__CoveredInputReceiver__sources(const jazz_node_CoveredInputReceiver *r);
void jazz_node_CoveredInputReceiver_free(jazz_node_CoveredInputReceiver *r);
jz_err jazz_node__node__query_eval__maintained_views__NodeState__replace_covered_input_receiver(jazz_node_NodeState *n, jazz_node_CoveredInputReceiver *r, jazz_types_SchemaVersionId schema, const jazz_protocol_AuthorityResultKey *key, bool *installed);
groove_InputSourceId groove__db__Database__allocate_input_source(groove_Database *db, groove_RecordDescriptor d);
jz_err groove__db__Database__retire_input_sources(groove_Database *db, const groove_InputSourceId *ids, u32 n);

/* ---- more externals (query_eval.rs, part B) ---- */
typedef struct jazz_node_QueryReadProfile {
    u64 resolve_view, compile_program, select_plan, execute_plan, decode_materialize, finish_rows, apply_projection, total; /* Duration (ms) */
} jazz_node_QueryReadProfile;
typedef struct groove_RecordDeltasIter { const groove_RecordDeltas *d; u32 i; } groove_RecordDeltasIter;
bool groove__ivm__RecordDeltas__next(groove_RecordDeltasIter *it, groove_BorrowedRecord *record, i64 *weight);
bool jazz_node__node__query_eval__subscriptions__NodeState__unique_authority_result_key_for_binding_view(const jazz_node_NodeState *n, const jazz_protocol_BindingViewKey *k, jazz_protocol_AuthorityResultKey *out);
void jazz_protocol_AuthorityResultKey_drop(void *k);
typedef struct jazz_node_RetainedRootWindowSource jazz_node_RetainedRootWindowSource; /* heap */
jazz_node_RetainedRootWindowSource *jazz_node__node__RetainedRootWindowSource__for_shape(const jazz_model_ValidatedQuery *shape);
bool jazz_node__node__RetainedRootWindowSource__is_bounded(const jazz_node_RetainedRootWindowSource *s);
void jazz_node__node__RetainedRootWindowSource__relative_window_for(const jazz_node_RetainedRootWindowSource *s, const jazz_model_ValidatedQuery *target, u32 *offset, bool *has_limit, u32 *limit);
void jazz_node_RetainedRootWindowSource_free(jazz_node_RetainedRootWindowSource *s);
jazz_protocol_RegisterShapeOptions jazz_protocol_RegisterShapeOptions_default(void);
void jazz_protocol_RegisterShapeOptions_drop(jazz_protocol_RegisterShapeOptions *o);
jz_bytes jazz_protocol__protocol__RegisterShapeOptions__read_view_key(const jazz_protocol_RegisterShapeOptions *o);
jazz_protocol_BindingViewKey jazz_protocol__protocol__BindingViewKey__new(jazz_model_ShapeId shape, jazz_model_BindingId binding, jz_bytes read_view_key);
/* query_eval::materialization (unit node-qe-b) */
jz_err jazz_node__node__query_eval__materialization__NodeState__materialize_aggregate_query_rows(jazz_node_NodeState *self, const jazz_model_Query *query, const jazz_engine_AppRowSchema *output, const groove_RecordDeltas *deltas, jazz_node_CurrentRowVec *out);
jz_err jazz_node__node__query_eval__materialization__NodeState__bind_app_row_schema_fields(jazz_node_CurrentRow *row, const jazz_engine_AppRowSchema *schema);
jz_err jazz_node__node__query_eval__materialization__NodeState__finish_engine_query_rows_in_schema(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_types_SchemaVersionId schema_version, jazz_node_CurrentRowVec *rows);
jz_err jazz_node__node__query_eval__materialization__NodeState__query_output_table(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_types_SchemaVersionId schema_version, jazz_model_TableSchema *out);
jz_err jazz_node__node__query_eval__materialization__NodeState__apply_projection_in_schema(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_types_SchemaVersionId schema_version, jazz_node_CurrentRow *rows, u32 n);
jazz_node_CurrentRow jazz_node__node__CurrentRow__new(const jz_str *table, groove_OwnedRecord record);
jz_err jazz_node__node__codec__decode_current_row(const jazz_model_TableSchema *table, const groove_BorrowedRecord *record, jazz_node_CurrentRow *out);
jz_err jazz_node__node__state__commit__NodeState__materialize_current_row(jazz_node_NodeState *self, const jazz_model_TableSchema *table, jazz_node_CurrentRow row, jazz_node_CurrentRow *out);
bool jazz_node__node__NodeState__is_history_complete(const jazz_node_NodeState *n);
jz_err jazz_node__node__NodeState__require_catalogue_ready(const jazz_node_NodeState *n);

/* ---- more externals (query_eval.rs, part C) ---- */
void jazz_node_PreparedQueryPlan_drop(jazz_node_PreparedQueryPlan *p);
jz_err groove__db__Database__query_graph_deltas(groove_Database *db, groove_GraphBuilder *g, groove_RecordDeltas *out);
jz_err groove__db__Database__query_graph_with_root_values(groove_Database *db, groove_GraphBuilder *g, groove_RootIndirectValues root, groove_RecordDeltas *out);
typedef struct groove_Sink groove_Sink;
jz_err groove__db__Database__query_graphs(groove_Database *db, groove_Sink *sinks, u32 n, groove_MultisinkDeltas *out);
jz_err jazz_node__node__query_eval__materialization__NodeState__materialize_historical_query_rows(jazz_node_NodeState *self, jazz_model_TableSchema table, const groove_RecordDeltas *deltas, jazz_node_CurrentRowVec *out);
jz_err jazz_node__node__query_eval__materialization__NodeState__materialize_include_deleted_query_rows(jazz_node_NodeState *self, jazz_model_TableSchema table, const groove_RecordDeltas *deltas, jazz_node_CurrentRowVec *out);
jz_err jazz_node__node__query_eval__materialization__NodeState__materialize_inline_current_query_rows(jazz_node_NodeState *self, const jazz_model_TableSchema *table, const groove_RecordDeltas *deltas, jazz_node_CurrentRowVec *out);
typedef struct jazz_node_RelationEdge jazz_node_RelationEdge;
typedef struct jazz_node_RelationSnapshot { usize root_count; jazz_node_CurrentRowVec rows; JZ_VEC(jazz_node_RelationEdge *) edges; } jazz_node_RelationSnapshot;
jz_err jazz_node__node__query_eval__materialization__NodeState__materialize_relation_snapshot_from_query_engine(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_protocol_ReadViewSpec *read_view, const groove_MultisinkDeltas *snapshots, const jazz_engine_QueryProgram *program, jazz_node_RelationSnapshot *out);
jz_err jazz_node__node__NodeState__table(const jazz_node_NodeState *n, jz_slice table, const jazz_model_TableSchema **out);
extern const u32 jazz_node__node__GlobalCurrentRowRecord__FIELD_ROW_UUID_IDX, jazz_node__node__GlobalCurrentRowRecord__FIELD_GLOBAL_TIME_IDX,
    jazz_node__node__GlobalCurrentRowRecord__FIELD_TX_TIME_IDX, jazz_node__node__GlobalCurrentRowRecord__FIELD_TX_NODE_ID_IDX;
typedef struct jazz_node_VersionRow jazz_node_VersionRow; /* heap */
typedef JZ_VEC(jazz_node_VersionRow *) jazz_node_VersionRowVec;
void jazz_node_VersionRow_free(jazz_node_VersionRow *v);
void jazz_node_VersionRowVec_drop(jazz_node_VersionRowVec *v);
bool jazz_node__node__VersionRow__is_deleted(const jazz_node_VersionRow *v);
jazz_types_RowUuid jazz_node__node__VersionRow__row_uuid(const jazz_node_VersionRow *v);
jz_err jazz_node__node__views__NodeState__query_version_by_alias(jazz_node_NodeState *n, jz_slice table, jazz_types_RowUuid row, jazz_types_TxTime time, u64 alias, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_row_versions_in_branch(jazz_node_NodeState *n, jz_slice table, const jazz_model_BranchKey *branch, jazz_types_RowUuid row, jazz_node_VersionRowVec *out);
jz_err jazz_node__node__NodeState__version_tx_id(const jazz_node_NodeState *n, const jazz_node_VersionRow *v, jazz_types_TxId *out);
jz_err jazz_node__node__currency__NodeState__query_transaction_state(jazz_node_NodeState *n, jazz_types_TxId tx, bool *found, u32 *fate, bool *has_global_time, jazz_types_GlobalTime *global_time, jazz_model_DurabilityTier *tier);
jz_err jazz_node__node__state__read_payload__NodeState__current_row_from_materialized_version(jazz_node_NodeState *n, const jazz_model_TableSchema *table, const jazz_node_VersionRow *v, jazz_node_CurrentRow *out);
void jazz_node__node__sort_current_rows(jazz_node_CurrentRow *rows, u32 n);
jz_err jazz_node__node__physical__NodeState__physical_table_id_for_version(const jazz_node_NodeState *n, const jazz_node_VersionRow *v, jazz_types_PhysicalTableId *out);
jz_err jazz_node__node__query_eval__bindings__bind_query_params_with_mode(const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, const jazz_model_RuntimeSchema *schema, jazz_node_ParamBindingMode mode, jazz_model_ValidatedQuery *out);

/* ---- more externals (query_eval.rs, part D) ---- */
bool jazz_model_Query_eq(const jazz_model_Query *a, const jazz_model_Query *b);
typedef JZ_VEC(jazz_model_Query) jazz_model_QueryVec;
typedef JZ_VEC(jazz_types_OutputOccurrenceId) jazz_types_OutputOccurrenceIdVec;
typedef JZ_VEC(jazz_model_OrderBy) jazz_model_OrderByVec;
bool jazz_model__query__relation_union_presentation_order(const jazz_model_RelationQuery *relation, jazz_model_OrderByVec *out);
void jazz_model_OrderByVec_drop(jazz_model_OrderByVec *v);
jz_err jazz_node__node__query_eval__materialization__NodeState__sort_query_rows_with_occurrences(const jazz_model_Query *query, const jazz_model_TableSchema *table, jazz_node_CurrentRowVec *rows, jazz_types_OutputOccurrenceIdVec *occurrence_ids);
const jz_str *jazz_node__node__CurrentRow__table(const jazz_node_CurrentRow *r);
/* open transactions */
enum { JAZZ_OTK_EXCLUSIVE, JAZZ_OTK_MERGEABLE };
u32 jazz_node__node__OpenTransaction__kind(const jazz_node_OpenTransaction *t, bool *has_permission_subject, jazz_types_AuthorSubject *permission_subject);
bool jazz_node__node__OpenTransaction__source_narrowing_recording(const jazz_node_OpenTransaction *t);
void jazz_node__node__OpenTransaction__set_source_narrowing_recording(jazz_node_OpenTransaction *t, bool recording);
bool jazz_node__node__OpenTransaction__narrowed_predicate_reads_contains(const jazz_node_OpenTransaction *t, jazz_model_ShapeId shape, jazz_model_BindingId binding);
void jazz_node__node__OpenTransaction__narrowed_predicate_reads_insert(jazz_node_OpenTransaction *t, jazz_model_ShapeId shape, jazz_model_BindingId binding);
typedef struct jazz_model_PredicateRead { jz_str table; jazz_model_ShapeId shape_id; jazz_model_Query shape; jazz_model_BindingId binding_id; jz_btree binding_values; } jazz_model_PredicateRead;
void jazz_node__node__OpenTransaction__push_predicate_read(jazz_node_OpenTransaction *t, jazz_model_PredicateRead read);
jz_err jazz_node__node__open_tx__NodeState__open_tx_mut(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_node_OpenTransaction **out);
jz_err jazz_node__node__open_tx__NodeState__transaction_is_exclusive(const jazz_node_NodeState *n, jazz_model_OpenTransactionId id, bool *out);
typedef struct jazz_node_SourceNarrowing jazz_node_SourceNarrowing; /* heap */
typedef struct jazz_node_NarrowedSourceRead { jazz_model_ValidatedQuery shape; jazz_model_Binding binding; } jazz_node_NarrowedSourceRead;
typedef JZ_VEC(jazz_node_NarrowedSourceRead) jazz_node_NarrowedSourceReadVec;
void jazz_node_NarrowedSourceRead_drop(void *r);
typedef struct jazz_node_ExclusiveSourceReads { jz_btree reads; /* SourceId -> NarrowedSourceRead */ } jazz_node_ExclusiveSourceReads;
jazz_node_ExclusiveSourceReads jazz_node_ExclusiveSourceReads_default(void);
void jazz_node_ExclusiveSourceReads_drop(jazz_node_ExclusiveSourceReads *r);
jz_err jazz_node__node__query_eval__narrowed_reads__NodeState__exclusive_source_reads(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, bool include_deleted, jazz_node_ExclusiveSourceReads *out);
jz_err jazz_node__node__open_tx__NodeState__offer_tx_narrowed_source_reads(jazz_node_NodeState *n, jazz_model_OpenTransactionId tx, jazz_node_ExclusiveSourceReads reads, jazz_node_SourceNarrowing **previous);
jz_err jazz_node__node__open_tx__NodeState__withdraw_tx_narrowed_source_reads(jazz_node_NodeState *n, jazz_model_OpenTransactionId tx, jazz_node_SourceNarrowing *previous, jazz_node_NarrowedSourceReadVec *out);
jz_err jazz_node__node__open_tx__NodeState__record_tx_query_row_reads(jazz_node_NodeState *n, jazz_model_OpenTransactionId tx, jazz_types_SchemaVersionId schema, jz_slice table, const jazz_types_RowUuid *rows, u32 nrows);
bool jazz_model__query__builders__Query__aggregate_input(const jazz_model_Query *q, jazz_model_Query *out);
jz_err jazz_model__query__ast__Query__validate(const jazz_model_Query *q, const jazz_model_RuntimeSchema *schema, jazz_model_ValidatedQuery *out);
jazz_types_RowUuid jazz_node__node__CurrentRow__row_uuid(const jazz_node_CurrentRow *r);
u64 jazz_node__node__state__lifecycle__NodeState__session_claim_revision(const jazz_node_NodeState *n, jazz_types_AuthorSubject identity);
jz_str jazz_node__node__query_eval__prepared_bindings__policy_plan_cache_signature(const jazz_model_Binding *binding, jazz_types_AuthorSubject identity, const jz_str *claims_scope);
jz_str jazz_node__node__query_eval__prepared_bindings__query_binding_value_signature(const jazz_model_Binding *binding);
/* QueryServing.query_shape_cache key: (ShapeId, DurabilityTier, String) -> Arc<PreparedQueryPlan> (jz_rc payload) */
typedef struct jazz_node_QueryShapeCacheKey { jazz_model_ShapeId shape; jazz_model_DurabilityTier tier; jz_str signature; } jazz_node_QueryShapeCacheKey;
void jazz_node_PreparedQueryPlan_drop_v(void *p);

/* ---- more externals (query_eval.rs, part E: maintained views) ---- */
typedef struct jazz_node_MaintainedTerminalSchemas jazz_node_MaintainedTerminalSchemas; /* heap */
void jazz_node_MaintainedTerminalSchemas_free(jazz_node_MaintainedTerminalSchemas *s);
/* maintained_subscription_view::ResultTransitions */
typedef struct jazz_node_ResultTransitions {
    JZ_VEC(jazz_protocol_ResultMemberEntry) adds, removes;
    JZ_VEC(jazz_protocol_ResultMemberPayloadEntry *) result_payload_adds;
    JZ_VEC(jazz_protocol_ResultMemberEntry) result_payload_removes;
    JZ_VEC(jazz_protocol_ProgramFactEntry *) program_fact_adds, program_fact_removes;
    JZ_VEC(void *) terminal_operations;
    bool supporting_changed;
} jazz_node_ResultTransitions;
void jazz_node_ResultTransitions_drop(jazz_node_ResultTransitions *t);
void jazz_node_ResultTransitions_clone(jazz_node_ResultTransitions *dst, const jazz_node_ResultTransitions *src);
/* MaintainedSubscriptionView: public fields this unit writes, then the rest */
typedef struct jazz_node_MaintainedSubscriptionView {
    jz_btree physical_tables; /* Arc<str> -> GlobalPhysicalTableId */
    jz_btree targeted_refresh_tables; bool targeted_refresh_uncertain;
    void *rest;
} jazz_node_MaintainedSubscriptionView;
jazz_node_MaintainedSubscriptionView *jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__default(void);
void jazz_node_MaintainedSubscriptionView_free(jazz_node_MaintainedSubscriptionView *v);
extern const jz_btree_desc jazz_node_PhysicalTablesByName_desc; /* Arc<str> -> GlobalPhysicalTableId */
jazz_node_MaintainedTerminalSchemas *jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__terminal_schemas_for_program(const jazz_engine_QueryProgram *program);
void jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__set_read_view(jazz_node_MaintainedSubscriptionView *v, jz_bytes read_view_key);
void jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__set_witness_table_names(jazz_node_MaintainedSubscriptionView *v, jz_btree names);
void jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__enable_storage_backed_result_materialization(jazz_node_MaintainedSubscriptionView *v);
void jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__enable_inline_content_branch_key(jazz_node_MaintainedSubscriptionView *v, const jazz_model_BranchKey *key);
jz_err jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__apply_multisink_deltas(jazz_node_MaintainedSubscriptionView *v, groove_MultisinkDeltas *deltas,
    const jazz_node_MaintainedTerminalSchemas *schemas, const jz_btree *tables, const void *node_aliases, jazz_node_ResultTransitions *out);
void jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__hold_client_local_literal_token(jazz_node_MaintainedSubscriptionView *v, void *token);
bool jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__replace_selected_deletion_witnesses(jazz_node_MaintainedSubscriptionView *v, void *witnesses);
extern const jz_btree_desc jazz_model_TableSchemaByName_desc; /* String -> TableSchema */
struct jazz_engine_LoweredProgram;
const jz_btree *jazz_engine__query_engine__LoweredProgram__maintained_terminal_tables(const struct jazz_engine_LoweredProgram *l);
const jz_btree *jazz_engine__query_engine__QueryProgram__source_descriptors(const jazz_engine_QueryProgram *p);
void jazz_engine_SourceId_debug(jz_str *out, const jazz_engine_SourceId *s);
void jazz_protocol_BindingViewKey_debug(jz_str *out, const jazz_protocol_BindingViewKey *k);
void jazz_protocol_AuthorityResultKey_debug(jz_str *out, const jazz_protocol_AuthorityResultKey *k);
void jazz_protocol_AuthorityResultKey_clone(void *dst, const void *src);
jazz_protocol_BindingViewKey jazz_protocol__protocol__AuthorityResultKey__binding_view(const jazz_protocol_AuthorityResultKey *k);
/* groove subscription polling (one copy, Context built from an optional Waker) */
enum { GROOVE_POLL_PENDING, GROOVE_POLL_UPDATE, GROOVE_POLL_ERROR };
u32 groove__ivm__MultisinkSubscription__poll_next_event(groove_MultisinkSubscription *s, void *waker, groove_MultisinkDeltas *deltas, jz_err *error);
void groove_MultisinkSubscription_free(groove_MultisinkSubscription *s);
jz_err groove__db__Database__bind_shape(groove_Database *db, u64 shape, const groove_Value *values, u32 n, groove_MultisinkSubscription **out);
jz_err groove__db__Database__bind_shape_with_root_values(groove_Database *db, u64 shape, const groove_Value *values, u32 n, groove_RootIndirectValues root, groove_MultisinkSubscription **out);
bool groove__db__Database__prepared_binding_source_is_bound(const groove_Database *db, const jz_str *source_shape);
/* catalogue identities */
typedef struct jazz_node_PhysicalTableIdentity { jz_uuid id; jz_btree columns; } jazz_node_PhysicalTableIdentity;
const jz_btree *jazz_node__node__PhysicalMapping__identity_tables(const jazz_node_PhysicalMapping *m); /* String -> PhysicalTableIdentity */
const jz_btree *jazz_node__node__state__catalogue__SchemaCatalogue__schema_version_aliases(const jazz_node_SchemaCatalogue *c); /* SchemaVersionId -> u32 */
const void *jazz_node__node__NodeState__node_aliases(const jazz_node_NodeState *n);
bool jazz_node__node__NodeState__client_relay_scope(const jazz_node_NodeState *n);
/* client_local_literal_shapes: String -> (Weak<()>, Binding) */
typedef struct jazz_node_LiteralShapeEntry { void *token; jazz_model_Binding binding; } jazz_node_LiteralShapeEntry;
bool jazz_model_Binding_eq(const jazz_model_Binding *a, const jazz_model_Binding *b);
/* maintained_views::LocalMaintainedViewSubscription (unit node-qe-b) */
typedef struct jazz_node_LocalMaintainedViewSubscription {
    groove_MultisinkSubscription *subscription; void *retained_prepared_plan;
    jazz_node_MaintainedSubscriptionView *maintained; jazz_node_MaintainedTerminalSchemas *terminal_schemas; jz_btree tables;
    jazz_model_Query result_query; jz_str result_table; jazz_types_SchemaVersionId result_schema_version;
    bool has_result_select; jz_vec_str result_select;
    bool has_result_relation_projection; jazz_model_RelationProjectColumnVec result_relation_projection;
    bool has_result_relation_projections; JZ_VEC(jazz_model_RelationProjectColumnVec) result_relation_projections;
    jz_btree result_set, result_payloads, program_facts; jazz_types_OutputOccurrenceIdVec root_occurrence_ids;
    bool initial_received; jazz_node_CoveredInputReceiver *covered_input_receiver;
} jazz_node_LocalMaintainedViewSubscription;
extern const jz_btree_desc jazz_protocol_ResultMemberSet_desc, jazz_protocol_ResultPayloads_desc, jazz_protocol_ProgramFactSet_desc;
jz_err jazz_model__query__relation_union_leaf_projections(const jazz_model_RelationQuery *r, void *out);
jz_err jazz_node__node__query_eval__maintained_views__NodeState__apply_local_maintained_view_transitions_inner(jazz_node_NodeState *n, jazz_node_LocalMaintainedViewSubscription *local, jazz_node_ResultTransitions transitions, bool publish, void *out);
bool jazz_node__node__query_eval__maintained_views__CoveredInputReceiver__is_empty(const jazz_node_CoveredInputReceiver *r);
jz_err jazz_node__node__query_eval__maintained_views__NodeState__install_opened_local_covered_receiver(jazz_node_NodeState *n, jazz_node_LocalMaintainedViewSubscription *local, const jazz_protocol_AuthorityResultKey *key, void *waker, bool *has, jazz_node_ResultTransitions *extra);
jz_err jazz_node__node__query_eval__maintained_views__NodeState__selected_deletion_witnesses(jazz_node_NodeState *n, const jazz_protocol_AuthorityResultKey *key, jazz_types_SchemaVersionId schema, void **out);
/* result row helpers (free functions at the end of query_eval.rs) */
typedef struct jazz_node_CurrentRowPublicationField { u32 tag; jz_str name; u32 visibility; } jazz_node_CurrentRowPublicationField;
enum { JAZZ_CRPF_STORED_COLUMN, JAZZ_CRPF_RESULT_FIELD };
enum { JAZZ_CRRV_APPLICATION_CELL, JAZZ_CRRV_HIDDEN_METADATA };
typedef JZ_VEC(jazz_node_CurrentRowPublicationField) jazz_node_CurrentRowPublicationFieldVec;
void jazz_node_CurrentRowPublicationField_clone(void *dst, const void *src);
void jazz_node_CurrentRowPublicationField_drop(void *p);
const jz_str *jazz_node__node__CurrentRowPublicationField__application_name(const jazz_node_CurrentRowPublicationField *f);
const jazz_node_CurrentRowPublicationFieldVec *jazz_node__node__CurrentRow__publication_fields(const jazz_node_CurrentRow *r);
jazz_node_CurrentRow jazz_node__node__CurrentRow__new_with_publication_fields(const jz_str *table, groove_OwnedRecord record, jazz_node_CurrentRowPublicationFieldVec fields);
typedef bool (*jazz_node_column_binding_fn)(void *ctx, const jz_str *name, u32 *index);
jz_err jazz_node__node__CurrentRow__project_with_column_binding(const jazz_node_CurrentRow *r, const jazz_model_TableSchema *t, const jz_vec_str *columns, jazz_node_column_binding_fn f, void *ctx, jazz_node_CurrentRow *out);
jz_err jazz_node__node__CurrentRow__project(const jazz_node_CurrentRow *r, const jazz_model_TableSchema *t, const jz_vec_str *columns, jazz_node_CurrentRow *out);
bool jazz_node__node__CurrentRow__raw_field(const jazz_node_CurrentRow *r, jz_slice column, groove_Value *out);
jz_slice jazz_node__node__CurrentRow__raw(const jazz_node_CurrentRow *r);
const jz_str *jazz_node__node__VersionRow__table(const jazz_node_VersionRow *v);
typedef struct jazz_engine_AggregateResultSchema { groove_DescriptorFieldVec group_key_fields, value_fields; } jazz_engine_AggregateResultSchema;
jz_err jazz_node__node__query_eval__query_result_rows__aggregate_output_schema(const jazz_engine_AppRowSchema *output, const jazz_engine_AggregateResultSchema **out);
jz_err jazz_node__node__query_eval__query_result_rows__aggregate_record_field_index(const groove_BorrowedRecord *record, const groove_DescriptorField *field, u32 *out);
const jazz_node_CurrentRowPublicationField *jazz_engine__query_engine__AppRowSchema__publication_field(const jazz_engine_AppRowSchema *s, const jz_str *name);
groove_DescriptorField groove__records__DescriptorField__with_identity_name_owned(groove_DescriptorField f, jz_str identity);
groove_RecordDescriptor groove__records__RecordDescriptor__new_with_fields(groove_DescriptorFieldVec fields);
jz_err groove__records__BorrowedRecord__get_idx(const groove_BorrowedRecord *r, u32 idx, groove_Value *out);
bool jazz_node__node__NodeState__is_history_complete_for(const jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape, jazz_types_GlobalTime position);
typedef struct jazz_node_HistoricalRead { jazz_node_NodeState *node; jazz_types_GlobalTime position; } jazz_node_HistoricalRead;
#endif
