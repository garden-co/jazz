/* _ext_node-qe-b.h: mirror declarations of types and functions owned by OTHER
 * units (groove, jazz_types, jazz_model, jazz_protocol, jazz_engine and the
 * rest of jazz_node) that unit node-qe-b uses. Layouts are approximate (the
 * Rust field order, rough sizes); opaque where we only pass pointers.
 * Function names follow PORTING.md so the link resolves against the owners. */
#ifndef JZ_EXT_NODE_QE_B_H
#define JZ_EXT_NODE_QE_B_H
#include "jz_rt.h"
typedef JZ_VEC(jz_str) jz_str_vec;

/* ======================= jazz_types ids / time ======================= */
typedef jz_uuid jazz_types_NodeUuid;
typedef jz_uuid jazz_types_SchemaVersionId;
typedef jz_uuid jazz_types_GlobalPhysicalTableId;
typedef jz_uuid jazz_types_RowUuid;
typedef jz_uuid jazz_types_LensId;
typedef jz_uuid jazz_model_ShapeId;
typedef jz_uuid jazz_model_BindingId;
typedef u64 jazz_types_NodeAlias;
typedef u64 jazz_types_SchemaVersionAlias;
typedef u64 jazz_types_PhysicalTableId;
typedef u64 jazz_types_PhysicalColumnId;
typedef u64 jazz_types_GlobalTime;
typedef u64 jazz_types_TxTime;

enum { JZ_AUTHOR_SYSTEM = 0, JZ_AUTHOR_SYSTEM_AT = 1, JZ_AUTHOR_AUTHENTICATED = 2 };
typedef struct jazz_types_AuthorSubject { u32 tag; const void *author; /* Intern<InternedAuthor> */ } jazz_types_AuthorSubject;
static inline bool jazz_types_AuthorSubject_eq(jazz_types_AuthorSubject a, jazz_types_AuthorSubject b) { return a.tag == b.tag && a.author == b.author; }
static inline bool jazz_types_AuthorSubject_is_system(jazz_types_AuthorSubject a) { return a.tag == JZ_AUTHOR_SYSTEM; }
void jazz_types_AuthorSubject_debug(jz_str *out, const jazz_types_AuthorSubject *s);
int jazz_types_AuthorSubject_cmp(const void *a, const void *b, void *ctx);
jz_err jazz_types__ids__AuthorSubject__from_canonical(jz_slice s, jazz_types_AuthorSubject *out);
jz_str jazz_types__ids__AuthorSubject__canonical(const jazz_types_AuthorSubject *s);
jz_err jazz_types__time__GlobalTime__tick(jazz_types_GlobalTime prev, u64 now_ms, jazz_types_GlobalTime *out);
jz_err jazz_types__time__TxTime__tick_after(jazz_types_TxTime t, jazz_types_TxTime *out);
u64 jazz_types__time__TxTime__physical_ms(jazz_types_TxTime t);

typedef enum { JZ_DURABILITY_NONE = 0, JZ_DURABILITY_LOCAL, JZ_DURABILITY_EDGE, JZ_DURABILITY_GLOBAL } jazz_model_DurabilityTier;
u8 jazz_model__tx__DurabilityTier__discriminant(jazz_model_DurabilityTier t);
typedef enum { JZ_FATE_PENDING = 0, JZ_FATE_ACCEPTED, JZ_FATE_REJECTED } jazz_model_FateTag;

typedef struct jazz_model_TxId { jazz_types_TxTime time; jazz_types_NodeUuid node; } jazz_model_TxId;
int jazz_model_TxId_cmp(const void *a, const void *b, void *ctx);
bool jazz_model_TxId_eq(const jazz_model_TxId *a, const jazz_model_TxId *b);
void jazz_model_TxId_debug(jz_str *out, const jazz_model_TxId *t);
typedef jz_uuid jazz_model_OpenTransactionId;
typedef jz_uuid jazz_model_BatchId;

typedef struct jazz_model_Snapshot {
    jazz_types_NodeUuid owner;
    jazz_types_GlobalTime global_base;
    jazz_types_TxTime local_base;
    JZ_VEC(jazz_model_TxId) dots;
} jazz_model_Snapshot;
void jazz_model_Snapshot_clone(jazz_model_Snapshot *dst, const jazz_model_Snapshot *src);
void jazz_model_Snapshot_drop(jazz_model_Snapshot *s);

/* ======================= groove values / records ======================= */
typedef struct groove_Value groove_Value;
typedef JZ_VEC(groove_Value) groove_ValueVec;
enum {
    GV_U8, GV_U16, GV_U32, GV_U64, GV_F64, GV_BOOL, GV_STRING, GV_BYTES, GV_LARGE, GV_UUID,
    GV_ENUM_TAG, GV_TUPLE, GV_ARRAY, GV_NULLABLE, GV_I64, GV_I32, GV_RECORD, GV_ENUM, GV_U48
};
struct groove_Value {
    u32 tag;
    union {
        u8 u8_; u16 u16_; u32 u32_; u64 u64_; f64 f64_; bool b; i64 i64_; i32 i32_;
        jz_str s; jz_bytes bytes; jz_uuid uuid; u8 enum_tag;
        void *large;                /* Box<LargeValueRef> */
        groove_ValueVec items;      /* Tuple / Array */
        groove_Value *nullable;     /* Option<Box<Value>>, NULL = None */
        void *record;               /* OwnedRecord */
        struct { u32 tag; void *record; } enum_;
    } v;
};
void groove_Value_clone(groove_Value *dst, const groove_Value *src);
void groove_Value_drop(void *v);
bool groove_Value_eq(const groove_Value *a, const groove_Value *b);
int groove_Value_cmp(const void *a, const void *b, void *ctx);
void groove_Value_debug(jz_str *out, const groove_Value *v);
static inline groove_Value gv_u8(u8 x) { groove_Value v; v.tag = GV_U8; v.v.u8_ = x; return v; }
static inline groove_Value gv_u64(u64 x) { groove_Value v; v.tag = GV_U64; v.v.u64_ = x; return v; }
static inline groove_Value gv_bool(bool x) { groove_Value v; v.tag = GV_BOOL; v.v.b = x; return v; }
static inline groove_Value gv_uuid(jz_uuid x) { groove_Value v; v.tag = GV_UUID; v.v.uuid = x; return v; }
static inline groove_Value gv_bytes(jz_bytes x) { groove_Value v; v.tag = GV_BYTES; v.v.bytes = x; return v; }
static inline groove_Value gv_str(jz_str x) { groove_Value v; v.tag = GV_STRING; v.v.s = x; return v; }
static inline groove_Value gv_enum_tag(u8 x) { groove_Value v; v.tag = GV_ENUM_TAG; v.v.enum_tag = x; return v; }

typedef struct groove_ValueType { u32 tag; void *payload; } groove_ValueType;
enum { GVT_U8, GVT_U16, GVT_U32, GVT_U64, GVT_I32, GVT_I64, GVT_F64, GVT_BOOL, GVT_STRING, GVT_BYTES,
       GVT_INTERNAL, GVT_UUID, GVT_ENUM_TAG, GVT_TUPLE, GVT_ARRAY, GVT_NULLABLE, GVT_RECORD, GVT_ENUM, GVT_U48 };
void groove_ValueType_clone(groove_ValueType *dst, const groove_ValueType *src);
void groove_ValueType_drop(void *t);

/* groove::schema::ColumnType */
typedef struct groove_ColumnType { u32 tag; struct groove_ColumnType *inner; void *payload; } groove_ColumnType;
enum { GCT_U8, GCT_U16, GCT_U32, GCT_U64, GCT_I32, GCT_I64, GCT_F64, GCT_BOOL, GCT_STRING, GCT_BYTES,
       GCT_UUID, GCT_ENUM, GCT_TUPLE, GCT_ARRAY, GCT_NULLABLE, GCT_RECORD, GCT_JSON, GCT_U48 };
void groove_ColumnType_clone(groove_ColumnType *dst, const groove_ColumnType *src);
void groove_ColumnType_drop(void *t);
void groove_ColumnType_debug(jz_str *out, const groove_ColumnType *t);
bool groove_ColumnType_eq(const groove_ColumnType *a, const groove_ColumnType *b);

typedef struct groove_RecordDescriptor groove_RecordDescriptor; /* Arc-backed; pass by pointer */
typedef struct groove_DescriptorField {
    bool has_name; jz_str name;
    groove_ValueType value_type;
    bool has_identity; jz_bytes identity;
} groove_DescriptorField;
typedef struct groove_FieldSpec { const char *name; groove_ValueType ty; } groove_FieldSpec;
groove_RecordDescriptor *groove__records__descriptor__RecordDescriptor__new(const groove_FieldSpec *fields, u32 n);
groove_RecordDescriptor *groove_RecordDescriptor_clone(const groove_RecordDescriptor *d);
void groove_RecordDescriptor_drop(groove_RecordDescriptor *d);
jz_err groove__records__descriptor__RecordDescriptor__create(const groove_RecordDescriptor *d, const groove_Value *values, u32 n, jz_bytes *out);
bool groove__records__descriptor__RecordDescriptor__field_index_by_identity(const groove_RecordDescriptor *d, const jz_bytes *identity, u32 *out);
const groove_DescriptorField *groove__records__descriptor__RecordDescriptor__fields(const groove_RecordDescriptor *d, u32 *n);
u32 groove__records__descriptor__RecordDescriptor__field_count(const groove_RecordDescriptor *d);

/* OwnedRecord / BorrowedRecord / Record<'_> */
typedef struct groove_OwnedRecord { groove_RecordDescriptor *descriptor; jz_bytes bytes; } groove_OwnedRecord;
typedef struct groove_BorrowedRecord { const groove_RecordDescriptor *descriptor; jz_slice bytes; } groove_BorrowedRecord;
typedef groove_BorrowedRecord groove_Record;
void groove_OwnedRecord_clone(groove_OwnedRecord *dst, const groove_OwnedRecord *src);
void groove_OwnedRecord_drop(void *r);
groove_BorrowedRecord groove__records__record__OwnedRecord__borrowed(const groove_OwnedRecord *r);
jz_err groove__records__record__BorrowedRecord__get_idx(const groove_BorrowedRecord *r, u32 idx, groove_Value *out);
jz_err groove__records__record__BorrowedRecord__to_values(const groove_BorrowedRecord *r, groove_ValueVec *out);
jz_err groove__records__record__BorrowedRecord__get(const groove_BorrowedRecord *r, const char *field, groove_Value *out);
jz_err groove__records__record__OwnedRecord__new(groove_RecordDescriptor *d, const groove_Value *values, u32 n, groove_OwnedRecord *out);

/* ======================= groove db facade ======================= */
typedef struct groove_Database groove_Database;
typedef struct groove_DirectRecordStore { const groove_Database *db; jz_str name; const void *schema; } groove_DirectRecordStore;
typedef struct groove_DirectRecordEntry { groove_ValueVec key; groove_OwnedRecord value; } groove_DirectRecordEntry;
typedef JZ_VEC(groove_DirectRecordEntry) groove_DirectRecordEntryVec;
enum { GDRSW_SET = 0, GDRSW_DELETE = 1 };
typedef struct groove_DirectRecordStoreWrite { u32 tag; groove_ValueVec key; groove_ValueVec value; } groove_DirectRecordStoreWrite;
typedef JZ_VEC(groove_DirectRecordStoreWrite) groove_DirectRecordStoreWriteVec;
void groove_DirectRecordEntry_drop(void *e);
void groove_DirectRecordStoreWrite_drop(void *w);
jz_err groove__db__facade__Database__direct_record_store(const groove_Database *db, const char *name, groove_DirectRecordStore *out);
jz_err groove__db__storage_helpers__DirectRecordStore__prefix_entries(const groove_DirectRecordStore *s, const groove_Value *prefix, u32 n, groove_DirectRecordEntryVec *out);
jz_err groove__db__storage_helpers__DirectRecordStore__write_many(const groove_DirectRecordStore *s, const groove_DirectRecordStoreWrite *w, u32 n);
jz_err groove__db__storage_helpers__DirectRecordStore__set(const groove_DirectRecordStore *s, const groove_Value *key, u32 nkey, const groove_Value *value, u32 nvalue);
jz_err groove__db__storage_helpers__DirectRecordStore__get(const groove_DirectRecordStore *s, const groove_Value *key, u32 nkey, bool *found, groove_OwnedRecord *out);
jz_err groove__db__storage_helpers__DirectRecordStore__delete(const groove_DirectRecordStore *s, const groove_Value *key, u32 nkey);
void groove_DirectRecordStore_drop(groove_DirectRecordStore *s);
typedef struct groove_HostApplicationGuard { const groove_Database *db; bool armed; } groove_HostApplicationGuard;
jz_err groove__db__facade__Database__guard_host_application(const groove_Database *db, groove_HostApplicationGuard *out);
void groove__db__HostApplicationGuard__complete(groove_HostApplicationGuard *g);
void groove_HostApplicationGuard_drop(groove_HostApplicationGuard *g);

/* IVM input sources */
typedef struct groove_InputSourceId { u64 runtime_namespace; u64 local; } groove_InputSourceId;
typedef struct groove_InputSourceDelta {
    groove_InputSourceId id; groove_RecordDescriptor *descriptor;
    JZ_VEC(jz_bytes) adds; JZ_VEC(jz_bytes) removes;
} groove_InputSourceDelta;
typedef struct groove_InputSourceReplacement {
    groove_InputSourceId id; groove_RecordDescriptor *descriptor; JZ_VEC(jz_bytes) records;
} groove_InputSourceReplacement;
void groove_InputSourceDelta_drop(void *d);
void groove_InputSourceReplacement_drop(void *d);
groove_InputSourceId groove__db__query__Database__allocate_input_source(groove_Database *db, groove_RecordDescriptor *d);
jz_err groove__db__query__Database__apply_input_source_deltas(groove_Database *db, groove_InputSourceDelta *deltas, u32 n);
jz_err groove__db__query__Database__replace_input_sources(groove_Database *db, groove_InputSourceReplacement *r, u32 n);
jz_err groove__db__query__Database__retire_input_sources(groove_Database *db, const groove_InputSourceId *ids, u32 n);

/* IVM graph builder (value type in Rust; heap handle here) */
typedef struct groove_GraphBuilder groove_GraphBuilder;
typedef struct groove_PredicateExpr groove_PredicateExpr;
enum { GFR_NAME = 0, GFR_STORED_NAME = 1, GFR_RESOLVED = 2 };
typedef struct groove_FieldRef { u32 tag; jz_str name; u32 index; } groove_FieldRef;
typedef JZ_VEC(groove_FieldRef) groove_FieldRefVec;
enum { GPE_TEMPLATE_ARGUMENT = 0, GPE_FIELD, GPE_RECORD_FIELD, GPE_LITERAL, GPE_TYPED_LITERAL, GPE_NULL, GPE_NULLABLE, GPE_NULLABLE_FLAT, GPE_ENUM_TAG_REMAP, GPE_OTHER };
typedef struct groove_ProjectExpr { u32 tag; groove_FieldRef field; void *rest; } groove_ProjectExpr;
typedef struct groove_ProjectField { groove_ProjectExpr expression; jz_str output_name; void *output_identity; } groove_ProjectField;
typedef JZ_VEC(groove_GraphBuilder *) groove_GraphBuilderPtrVec;
enum {
    GGB_TYPED_TEMPLATE = 0, GGB_TEMPLATE_INPUT, GGB_TABLE, GGB_INLINE_RECORDS, GGB_INPUT_SOURCE, GGB_INDEX,
    GGB_FRONTIER_SOURCE, GGB_BINDING_SOURCE, GGB_RECURSIVE, GGB_RECURSIVE_STEP_WITNESS, GGB_FILTER,
    GGB_UNWRAP_NULLABLE, GGB_UNNEST, GGB_VARIANT_PROJECT, GGB_PROJECT, GGB_STREAMING_CHECKSUM, GGB_UNION,
    GGB_JOIN, GGB_SEMI_JOIN, GGB_ANTI_JOIN, GGB_LOOKUP, GGB_ARG_MAX_BY, GGB_ARG_MIN_BY, GGB_TOP_BY,
    GGB_COLLECT_BY, GGB_AGGREGATE
};
enum { GTBL_UNBOUNDED = 0, GTBL_LIMIT = 1 };
typedef struct groove_CollectByBuilder groove_CollectByBuilder;
struct groove_CollectByBuilder { groove_FieldRefVec group_cols; JZ_VEC(groove_CollectByBuilder) slots; void *rest; };
/* groove::ivm::GraphBuilder, flattened: children are Arc<GraphBuilder> (jz_rc payloads). */
struct groove_GraphBuilder {
    u32 tag;
    groove_GraphBuilder *input;                 /* Filter..Aggregate; Lookup.keys; TemplateInput.input (Option) */
    groove_GraphBuilder *left, *right;          /* Join / SemiJoin / AntiJoin */
    groove_GraphBuilder *seed, *step, *step_witness; /* Recursive */
    groove_GraphBuilder *recursive;             /* RecursiveStepWitness */
    groove_GraphBuilderPtrVec inputs;           /* TypedTemplate / Union */
    groove_FieldRefVec left_on, right_on;       /* joins */
    groove_FieldRefVec group_cols;              /* ArgMaxBy / ArgMinBy / TopBy / Aggregate */
    groove_FieldRefVec keys_on;                 /* Lookup */
    JZ_VEC(groove_ProjectField) fields;         /* Project */
    jz_str shape;                               /* BindingSource */
    jz_str table;                               /* Table / Index / Lookup */
    groove_RecordDescriptor *output;            /* BindingSource / InputSource / InlineRecords / TemplateInput / FrontierSource */
    u64 offset; u32 limit_tag; u64 limit;       /* TopBy */
    groove_CollectByBuilder *collect;           /* CollectBy */
    void *program; JZ_VEC(void *) predicates; void *scalars; /* TypedTemplate */
    void *rest;
};
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__input_source(groove_InputSourceId id, groove_RecordDescriptor *d);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__table(const char *table);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__filter(groove_GraphBuilder *g, groove_PredicateExpr *p);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__project(groove_GraphBuilder *g, const char *const *fields, u32 n);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__project_fields(groove_GraphBuilder *g, groove_ProjectField *fields, u32 n);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__join(groove_GraphBuilder *l, groove_GraphBuilder *r, const char *const *lk, u32 nl, const char *const *rk, u32 nr);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__anti_join(groove_GraphBuilder *l, groove_GraphBuilder *r, const char *const *lk, u32 nl, const char *const *rk, u32 nr);
groove_GraphBuilder *groove_GraphBuilder_clone(const groove_GraphBuilder *g); /* derive(Clone): new node, children Arc-cloned */
void groove_GraphBuilder_drop(groove_GraphBuilder *g);
u64 groove_GraphBuilder_hash_into(const groove_GraphBuilder *g, u64 state);
void groove_GraphBuilder_debug(jz_str *out, const groove_GraphBuilder *g);
jz_err groove__db__query__Database__graph_output_descriptor(const groove_Database *db, const groove_GraphBuilder *g, groove_RecordDescriptor **out);
jz_err groove__ivm__template__TypedGraphTemplate__bind_declarative_with_arguments(const void *program, const groove_GraphBuilderPtrVec *inputs, const void *predicates, const void *scalars, groove_GraphBuilder **out);
void groove_FieldRef_debug(jz_str *out, const groove_FieldRef *f);
groove_PredicateExpr *groove__ivm__graph__PredicateExpr__eq(const char *field, groove_Value v);
groove_PredicateExpr *groove__ivm__graph__PredicateExpr__and(groove_PredicateExpr **parts, u32 n);
groove_PredicateExpr *groove__ivm__graph__PredicateExpr__canonicalize(groove_PredicateExpr *p);
void groove_PredicateExpr_drop(groove_PredicateExpr *p);
groove_ProjectField groove__ivm__graph__ProjectField__renamed(jz_str source, const char *name);
jz_str jazz_engine__query_engine__fields__route_param_field(const jz_str *param);
bool jazz_types__debug_env__covered_input_trace(void);
jz_str jazz_engine__query_engine__fields__left_field(const char *field);

/* ======================= jazz_model query / schema ======================= */
enum { JZ_OPERAND_COLUMN = 0, JZ_OPERAND_PARAM, JZ_OPERAND_CLAIM, JZ_OPERAND_LITERAL };
typedef struct jazz_model_Operand { u32 tag; jz_str name; groove_Value literal; } jazz_model_Operand;
typedef JZ_VEC(jazz_model_Operand) jazz_model_OperandVec;
void jazz_model_Operand_drop(void *o);
void jazz_model_Operand_clone(jazz_model_Operand *dst, const jazz_model_Operand *src);
enum { JZ_PRED_ALL = 0, JZ_PRED_ANY, JZ_PRED_NOT, JZ_PRED_EQ, JZ_PRED_NE, JZ_PRED_IN, JZ_PRED_GT, JZ_PRED_GTE,
       JZ_PRED_LT, JZ_PRED_LTE, JZ_PRED_CONTAINS, JZ_PRED_ENUM_MATCH, JZ_PRED_IS_NULL };
typedef struct jazz_model_Predicate jazz_model_Predicate;
typedef JZ_VEC(jazz_model_Predicate) jazz_model_PredicateVec;
struct jazz_model_Predicate {
    u32 tag;
    jazz_model_PredicateVec children;     /* All / Any */
    jazz_model_Predicate *inner;          /* Not / EnumMatch.payload (Box) */
    jazz_model_Operand left, right;       /* binary; In.left; IsNull */
    jazz_model_OperandVec values;         /* In */
    jz_str column, case_;                 /* EnumMatch */
};
typedef enum { JZ_JT_COLUMN = 0, JZ_JT_ROW_ID = 1, JZ_JT_UNCORRELATED = 2 } jazz_model_JoinTarget;
typedef struct jazz_model_JoinCorrelation { jz_str join_column; jz_str source_column; } jazz_model_JoinCorrelation;
typedef struct jazz_model_JoinSourceLookup { jz_str table; jz_str row_id_source_column; jz_str value_column; } jazz_model_JoinSourceLookup;
typedef struct jazz_model_JoinVia jazz_model_JoinVia;
struct jazz_model_JoinVia {
    jz_str table;
    jz_str on_column;
    jazz_model_JoinTarget target;
    bool has_source_column; jz_str source_column;
    jazz_model_JoinSourceLookup *source_lookup;           /* Option */
    JZ_VEC(jazz_model_JoinCorrelation) correlated_filters;
    jazz_model_PredicateVec filters;
    JZ_VEC(jazz_model_JoinVia) nested_joins;
};
typedef struct jazz_model_ReachableSeed { jz_str table; bool has_user_column; jz_str user_column; bool has_user_claim; jz_str user_claim; jz_str team_column; jazz_model_PredicateVec filters; } jazz_model_ReachableSeed;
typedef struct jazz_model_ReachableVia {
    jz_str access_table, access_row_column, access_team_column;
    jazz_model_JoinTarget access_team_target;
    jazz_model_Operand from;
    jazz_model_PredicateVec access_filters;
    jz_str edge_table, edge_member_column, edge_parent_column;
    jazz_model_PredicateVec edge_filters;
    jazz_model_ReachableSeed *seed;    /* Option */
    void *rest;
} jazz_model_ReachableVia;
typedef enum { JZ_INH_SELECT = 0, JZ_INH_INSERT, JZ_INH_UPDATE, JZ_INH_DELETE } jazz_model_InheritsOperation;
typedef struct jazz_model_InheritsVia { jz_str parent_column; jazz_model_InheritsOperation operation; bool has_max_depth; u32 max_depth; } jazz_model_InheritsVia;
#define JZ_DECL_DERIVES(T) \
    void T##_clone(T *dst, const T *src); void T##_drop(void *p); bool T##_eq(const T *a, const T *b); \
    void T##_pc_encode(jz_pc_w *w, const T *v);
JZ_DECL_DERIVES(jazz_model_Predicate)
JZ_DECL_DERIVES(jazz_model_JoinVia)
JZ_DECL_DERIVES(jazz_model_ReachableVia)
JZ_DECL_DERIVES(jazz_model_InheritsVia)

typedef struct jazz_model_PolicyBranch {
    jazz_model_PredicateVec filters;
    JZ_VEC(jazz_model_JoinVia) joins;
    JZ_VEC(jazz_model_ReachableVia) reachable;
    JZ_VEC(jazz_model_InheritsVia) inherits;
} jazz_model_PolicyBranch;
void jazz_model_PolicyBranch_drop(void *b);
void jazz_model_PolicyBranch_clone(jazz_model_PolicyBranch *dst, const jazz_model_PolicyBranch *src);

typedef enum { JZ_ORDER_ASC = 0, JZ_ORDER_DESC = 1 } jazz_model_OrderDirection;
typedef struct jazz_model_OrderBy { jz_str column; jazz_model_OrderDirection direction; } jazz_model_OrderBy;
typedef enum { JZ_AGG_COUNT, JZ_AGG_SUM, JZ_AGG_AVG, JZ_AGG_MIN, JZ_AGG_MAX } jazz_model_AggregateFunction;
typedef struct jazz_model_Aggregate { jazz_model_AggregateFunction function; bool has_column; jz_str column; jz_str alias; } jazz_model_Aggregate;
typedef struct jazz_model_AggregateQuery { bool has_group_by; jz_str group_by; JZ_VEC(jazz_model_Aggregate) aggregates; } jazz_model_AggregateQuery;
typedef struct jazz_model_Include { jz_str path; u32 join_mode; bool require; } jazz_model_Include;
typedef enum { JZ_ASR_ANY = 0, JZ_ASR_AT_LEAST_ONE = 1 } jazz_model_ArraySubqueryRequirement;
typedef struct jazz_model_ArraySubquery jazz_model_ArraySubquery;
struct jazz_model_ArraySubquery {
    jz_str column_name, table, inner_column, outer_column;
    jazz_model_PredicateVec filters;
    bool has_select; jz_str_vec select;
    JZ_VEC(jazz_model_OrderBy) order_by;
    bool has_limit; u32 limit;
    u32 offset;
    jazz_model_ArraySubqueryRequirement requirement;
    JZ_VEC(jazz_model_ArraySubquery) nested_arrays;
};
jazz_model_ArraySubquery jazz_model__query__policy_and_request_contracts__ArraySubquery__new(const char *column_name, const jz_str *table, const jz_str *inner_column, const jz_str *outer_column);
void jazz_model_ArraySubquery_drop(void *a);
typedef struct jazz_model_FlatJoinSource { jz_str table; bool has_alias; jz_str alias; void *on; } jazz_model_FlatJoinSource;
typedef struct jazz_model_FlatJoin { bool has_root_alias; jz_str root_alias; JZ_VEC(jazz_model_FlatJoinSource) sources; } jazz_model_FlatJoin;
typedef struct jazz_model_RelationQuery { void *rel; void *rest; } jazz_model_RelationQuery;
typedef struct jazz_model_RelationUnionParts { void *arms; bool has_limit; u32 limit; bool has_offset; u32 offset; } jazz_model_RelationUnionParts;
const jazz_model_RelationUnionParts *jazz_model__query__relation_resolution__relation_union_parts(const void *rel); /* Option<&UnionParts> */
typedef struct jazz_model_Query {
    jz_str table;
    jazz_model_PredicateVec filters;
    JZ_VEC(jazz_model_JoinVia) joins;
    jazz_model_FlatJoin *flat_join;                   /* Option<FlatJoin> */
    JZ_VEC(jazz_model_PolicyBranch) policy_branches;
    JZ_VEC(jazz_model_ReachableVia) reachable;
    JZ_VEC(jazz_model_InheritsVia) inherits;
    JZ_VEC(jazz_model_Include) includes;
    JZ_VEC(jazz_model_ArraySubquery) array_subqueries;
    bool has_select; jz_str_vec select;
    JZ_VEC(jazz_model_OrderBy) order_by;
    jazz_model_AggregateQuery *aggregate;             /* Option<Box<..>> */
    bool has_limit; u32 limit;
    u32 offset;
    jazz_model_RelationQuery *relation;               /* Option<RelationQuery> */
} jazz_model_Query;
void jazz_model_Query_clone(jazz_model_Query *dst, const jazz_model_Query *src);
void jazz_model_Query_drop(void *q);
bool jazz_model_Query_eq(const jazz_model_Query *a, const jazz_model_Query *b);
void jazz_model_Query_debug(jz_str *out, const jazz_model_Query *q);
void jazz_model_Query_pc_encode(jz_pc_w *w, const jazz_model_Query *q);

typedef struct jazz_model_ValidatedQuery {
    jazz_model_Query query;
    jazz_types_SchemaVersionId schema_version;
    jz_btree params;    /* String -> ColumnType */
    jz_bytes canonical;
    jazz_model_ShapeId shape_id;
} jazz_model_ValidatedQuery;
void jazz_model_ValidatedQuery_clone(jazz_model_ValidatedQuery *dst, const jazz_model_ValidatedQuery *src);
void jazz_model_ValidatedQuery_drop(void *q);
typedef struct jazz_model_Binding { jz_btree values; /* String -> Value */ jz_bytes canonical; } jazz_model_Binding;
void jazz_model_Binding_clone(jazz_model_Binding *dst, const jazz_model_Binding *src);
void jazz_model_Binding_drop(void *b);
jazz_model_BindingId jazz_model__query__validation__Binding__binding_id(const jazz_model_Binding *b);
jz_err jazz_model__query__validation__Binding__new(const jazz_model_ValidatedQuery *shape, jz_btree *values, jazz_model_Binding *out);
bool jazz_model__query__author_claim_path_key(const jz_str *path, u32 n, jz_str *out);
void jazz_model__model__policy_claims__author_policy_claims(jazz_types_AuthorSubject s, jz_btree *out);
bool jazz_model__model__policy_claims__policy_claim_at_path(const jz_btree *claims, const jz_str *path, u32 n, groove_Value *out);
/* jazz_node::node::default_policy_claim_values (mod.rs) */
void jazz_node__node__default_policy_claim_values(jazz_types_AuthorSubject s, jz_btree *out);

/* schema */
typedef struct jazz_model_ColumnSchema { jz_str name; groove_ColumnType column_type; u32 large_value_kind; groove_Value *default_; } jazz_model_ColumnSchema;
void jazz_model_ColumnSchema_drop(void *c);
jazz_model_ColumnSchema jazz_model__schema__ColumnSchema__new(const char *name, u32 nlen, groove_ColumnType ty);
typedef struct jazz_model_TableSchema {
    jz_str name;
    JZ_VEC(jazz_model_ColumnSchema) columns;
    jz_str_vec branch_by;
    jz_btree references;            /* String -> String */
    jazz_model_Query *read_policy;   /* Option<Query> */
    JZ_VEC(void) write_policies;    /* (WriteOp, Query) */
    jz_btree indexed_columns;
    jz_btree composite_indexes;
    jz_btree merge_strategies;
} jazz_model_TableSchema;
void jazz_model_TableSchema_drop(void *t);
void jazz_model_TableSchema_clone(jazz_model_TableSchema *dst, const jazz_model_TableSchema *src);
jazz_model_TableSchema jazz_model__schema__TableSchema__new(const jz_str *name, jazz_model_ColumnSchema *cols, u32 n);
typedef struct jazz_model_JazzSchema {
    void *public_schema;
    JZ_VEC(jazz_model_TableSchema) tables; /* runtime.tables */
} jazz_model_JazzSchema;
void jazz_model_JazzSchema_clone(jazz_model_JazzSchema *dst, const jazz_model_JazzSchema *src);
void jazz_model_JazzSchema_drop(void *s);
jazz_types_SchemaVersionId jazz_model__schema__JazzSchema__version_id(const jazz_model_JazzSchema *s);
bool jazz_model_JazzSchema_eq(const jazz_model_JazzSchema *a, const jazz_model_JazzSchema *b);

/* jazz_model BranchKey */
typedef struct jazz_model_BranchKey { JZ_VEC(void) values; /* (String, BranchColumnValue) */ } jazz_model_BranchKey;
void jazz_model_BranchKey_clone(jazz_model_BranchKey *dst, const jazz_model_BranchKey *src);
void jazz_model_BranchKey_drop(void *b);
jz_bytes jazz_model__model__branch__BranchKey__canonical_bytes(const jazz_model_BranchKey *b);

/* ======================= jazz_protocol ======================= */
typedef struct jazz_protocol_ReadViewKey { jz_uuid id; } jazz_protocol_ReadViewKey;
typedef struct jazz_protocol_CanonicalPolicyClaims { jz_btree claims; jz_bytes comparison_key; } jazz_protocol_CanonicalPolicyClaims;
typedef struct jazz_protocol_PolicyBindingKey { jazz_types_AuthorSubject identity; jazz_protocol_CanonicalPolicyClaims canonical_claims; } jazz_protocol_PolicyBindingKey;
void jazz_protocol_PolicyBindingKey_clone(jazz_protocol_PolicyBindingKey *dst, const jazz_protocol_PolicyBindingKey *src);
void jazz_protocol_PolicyBindingKey_drop(void *k);
bool jazz_protocol_PolicyBindingKey_eq(const jazz_protocol_PolicyBindingKey *a, const jazz_protocol_PolicyBindingKey *b);
int jazz_protocol_PolicyBindingKey_cmp(const void *a, const void *b, void *ctx);
void jazz_protocol__protocol__PolicyBindingKey__directory_digest(const jazz_protocol_PolicyBindingKey *k, u8 out[32]);
jz_err jazz_protocol__protocol__PolicyBindingKey__from_directory_value(jazz_types_AuthorSubject identity, groove_Value v, jazz_protocol_PolicyBindingKey *out);
jazz_protocol_CanonicalPolicyClaims jazz_protocol__protocol__CanonicalPolicyClaims__new(jz_btree claims);
typedef struct jazz_protocol_BindingViewKey { jazz_model_ShapeId shape_id; jazz_model_BindingId binding_id; jazz_protocol_ReadViewKey read_view; } jazz_protocol_BindingViewKey;
int jazz_protocol_BindingViewKey_cmp(const void *a, const void *b, void *ctx);
typedef struct jazz_protocol_SubscriptionKey { jazz_model_ShapeId shape_id; jazz_model_BindingId binding_id; jazz_protocol_ReadViewKey read_view; } jazz_protocol_SubscriptionKey;
typedef struct jazz_protocol_AuthorityResultKey {
    jazz_protocol_BindingViewKey binding_view;
    bool has_policy_binding; jazz_protocol_PolicyBindingKey policy_binding;
} jazz_protocol_AuthorityResultKey;
void jazz_protocol_AuthorityResultKey_clone(jazz_protocol_AuthorityResultKey *dst, const jazz_protocol_AuthorityResultKey *src);
void jazz_protocol_AuthorityResultKey_drop(void *k);
bool jazz_protocol_AuthorityResultKey_eq(const jazz_protocol_AuthorityResultKey *a, const jazz_protocol_AuthorityResultKey *b);
int jazz_protocol_AuthorityResultKey_cmp(const void *a, const void *b, void *ctx);

typedef enum { JZ_LAYER_CONTENT = 0, JZ_LAYER_DELETION, JZ_LAYER_CONTENT_OR_DELETION } jazz_protocol_ResultRowLayer;
typedef struct jazz_protocol_RowVersionRefEntry {
    jazz_model_TxId tx;
    bool has_schema_version; jazz_types_SchemaVersionId schema_version;
    jazz_protocol_ResultRowLayer layer;
    bool has_batch; jazz_model_TxId batch;
    bool has_branch_or_prefix; jz_bytes branch_or_prefix;
    bool has_row_digest; jz_bytes row_digest;
} jazz_protocol_RowVersionRefEntry;
typedef struct jazz_protocol_SupportingRow {
    jazz_types_GlobalPhysicalTableId physical_table;
    const jz_str *version_table; /* Intern<String> */
    jazz_types_RowUuid row;
    jazz_protocol_RowVersionRefEntry version;
} jazz_protocol_SupportingRow;
void jazz_protocol_SupportingRow_drop(void *r);
void jazz_protocol_SupportingRow_clone(jazz_protocol_SupportingRow *dst, const jazz_protocol_SupportingRow *src);
int jazz_protocol_SupportingRow_cmp(const void *a, const void *b, void *ctx);
const jz_str *groove__intern_string(const jz_str *s); /* Intern<String>::from */

typedef struct jazz_protocol_ReadViewSpec jazz_protocol_ReadViewSpec;
enum { JZ_RVS_CURRENT = 0, JZ_RVS_SNAPSHOT = 1, JZ_RVS_BRANCH_VIEW = 2 };
typedef struct jazz_protocol_ReadViewSourceSpec {
    u32 tag;
    jazz_model_Snapshot snapshot;      /* Snapshot { snapshot } */
    void *head;                        /* BranchView { head, base } */
    void *base;                        /* Option<..>, NULL = None */
} jazz_protocol_ReadViewSourceSpec;
struct jazz_protocol_ReadViewSpec { jazz_protocol_ReadViewSourceSpec source; };

/* ======================= jazz_engine query_engine ======================= */
enum { JZ_SR_ROOT = 0, JZ_SR_ALIAS, JZ_SR_RECURSIVE_SEED, JZ_SR_RECURSIVE_STEP, JZ_SR_CORRELATED_CHILD, JZ_SR_POLICY };
typedef struct jazz_engine_SourceRole { u32 tag; jz_str name; } jazz_engine_SourceRole;
typedef struct jazz_engine_SourcePath { JZ_VEC(jazz_engine_SourceRole) components; } jazz_engine_SourcePath;
typedef struct jazz_engine_SourceId { jz_str table; jazz_engine_SourcePath path; } jazz_engine_SourceId;
void jazz_engine_SourceId_clone(jazz_engine_SourceId *dst, const jazz_engine_SourceId *src);
void jazz_engine_SourceId_drop(void *s);
int jazz_engine_SourceId_cmp(const void *a, const void *b, void *ctx);
extern const jz_btree_desc jazz_engine_SOURCE_ID_SET_DESC;

enum { JZ_PC_SYSTEM = 0, JZ_PC_IDENTITY = 1, JZ_PC_AUTHORIZATION_SUBPLAN = 2 };
typedef enum { JZ_PEM_PERMISSIVE_LOCAL = 0, JZ_PEM_ENFORCING = 1 } jazz_engine_PolicyEnforcementMode;
typedef struct jazz_engine_PolicyContext {
    u32 tag;
    jazz_engine_SourceId protected_source;   /* AuthorizationSubplan */
    u32 role;                                /* AuthorizationSubplan */
    jazz_engine_PolicyEnforcementMode mode;
    jazz_types_AuthorSubject permission_subject;
    jz_btree claims;                         /* String -> Value */
    bool has_attribution; jazz_types_AuthorSubject attribution;
} jazz_engine_PolicyContext;
void jazz_engine_PolicyContext_clone(jazz_engine_PolicyContext *dst, const jazz_engine_PolicyContext *src);
void jazz_engine_PolicyContext_drop(void *p);
bool jazz_engine_PolicyContext_eq(const jazz_engine_PolicyContext *a, const jazz_engine_PolicyContext *b);

typedef struct jazz_engine_ClaimPath { jz_str_vec segs; } jazz_engine_ClaimPath;
typedef struct jazz_engine_ProgramClaimParam { jazz_engine_ClaimPath path; groove_ColumnType ty; } jazz_engine_ProgramClaimParam;
void jazz_engine_ProgramClaimParam_drop(void *p);
void jazz_engine_ProgramClaimParam_clone(jazz_engine_ProgramClaimParam *dst, const jazz_engine_ProgramClaimParam *src);

typedef enum { JZ_QAM_TRUSTED_SERVING = 0, JZ_QAM_CLIENT_LOCAL = 1 } jazz_engine_QueryAuthorizationMode;

/* SourceExpr<RequestedSourceStage> */
enum { JZ_SFS_CURRENT = 0, JZ_SFS_EXPLICIT = 1 };
enum { JZ_SSS_COMPATIBLE = 0, JZ_SSS_SINGLE = 1, JZ_SSS_EXPLICIT = 2 };
enum { JZ_LENS_CANONICAL = 0, JZ_LENS_FINGERPRINT = 1 };
typedef struct jazz_engine_SchemaProjection {
    u32 family_tag; jz_uuid family;
    u32 storage_tag; jazz_types_SchemaVersionId single; jz_btree explicit_set;
    u32 lens_tag; jz_bytes fingerprint;
} jazz_engine_SchemaProjection;
void jazz_engine_SchemaProjection_clone(jazz_engine_SchemaProjection *dst, const jazz_engine_SchemaProjection *src);
void jazz_engine_SchemaProjection_drop(void *p);
enum { JZ_DS_CURRENT = 0, JZ_DS_BRANCH = 1 };
typedef struct jazz_engine_DataSource { u32 tag; jazz_model_BranchKey branch; } jazz_engine_DataSource;
enum { JZ_OVL_PENDING_LOCAL = 0, JZ_OVL_DIRECT_BATCH, JZ_OVL_ACCEPTED_TX, JZ_OVL_OPEN_TX };
typedef struct jazz_engine_OverlayRef { u32 tag; jazz_model_BatchId batch; jazz_model_TxId tx; jazz_model_OpenTransactionId open; } jazz_engine_OverlayRef;
typedef struct jazz_engine_OverlayStack { JZ_VEC(jazz_engine_OverlayRef) entries; } jazz_engine_OverlayStack;
enum {
    JZ_SE_VISIBLE_CURRENT = 0, JZ_SE_BRANCH_VIEW, JZ_SE_HISTORY_CUT, JZ_SE_SNAPSHOT_REF,
    JZ_SE_SETTLED_BINDING_VIEW, JZ_SE_WITH_OVERLAYS, JZ_SE_LENS_PROJECT, JZ_SE_MERGE
};
typedef struct jazz_engine_SourceExpr jazz_engine_SourceExpr;
struct jazz_engine_SourceExpr {
    u32 tag;
    jazz_engine_SchemaProjection projection;
    jazz_engine_DataSource data;
    jazz_model_DurabilityTier tier;
    jazz_model_BranchKey head;                 /* BranchView */
    void *base;                                /* Option<BranchViewSourceBase> */
    jazz_types_GlobalTime position;            /* HistoryCut */
    jazz_model_Snapshot snapshot;              /* SnapshotRef */
    bool current_default;                      /* SettledBindingView */
    jazz_protocol_BindingViewKey binding_view;
    bool has_authority_result_key; jazz_protocol_AuthorityResultKey authority_result_key;
    jazz_engine_SourceExpr *input;             /* WithOverlays / LensProject */
    jazz_engine_OverlayStack overlays;
    JZ_VEC(jazz_engine_SourceExpr) inputs;     /* Merge */
    u32 merge_mode;
};
void jazz_engine_SourceExpr_clone(jazz_engine_SourceExpr *dst, const jazz_engine_SourceExpr *src);
void jazz_engine_SourceExpr_drop(void *e);
extern const jz_btree_desc jazz_engine_SOURCE_GRAPH_DESC; /* SourceId -> SourceExpr */
typedef struct jazz_engine_ReadView {
    jazz_types_SchemaVersionId read_schema;
    jazz_types_SchemaVersionId policy_schema;
    jz_btree sources; /* SourceGraph */
} jazz_engine_ReadView;
typedef struct jazz_engine_QueryReadSet { jazz_engine_ReadView primary; jz_btree fact_reads; } jazz_engine_QueryReadSet;
typedef jazz_engine_QueryReadSet jazz_engine_RequestedReadSet;
void jazz_engine_QueryReadSet_drop(void *r);
extern const jz_btree_desc jazz_engine_FACT_READS_DESC;
jazz_engine_QueryReadSet jazz_engine__query_engine__read__QueryReadSet__primary(jazz_engine_ReadView primary);

enum { JZ_CP_IMPLICIT_ROOT_REFERENCE = 0, JZ_CP_EXPLICIT_INCLUDE = 1 };
typedef struct jazz_engine_ClosurePathSegment { jazz_engine_SourceId parent; jazz_engine_SourceId target; jz_str source_field; } jazz_engine_ClosurePathSegment;
typedef struct jazz_engine_ClosurePath { u32 tag; jz_str id; jazz_engine_ClosurePathSegment segment; void *rest; } jazz_engine_ClosurePath;
typedef struct jazz_engine_RowSetNodeId { jz_str s; } jazz_engine_RowSetNodeId;
enum { JZ_NVR_SOURCE_FIELD = 0, JZ_NVR_PARAM = 1, JZ_NVR_CLAIM = 2 };
typedef struct jazz_engine_NormalizedValueRef { u32 tag; jazz_engine_SourceId source; jz_str field; /* field or param */ jazz_engine_ClaimPath claim; } jazz_engine_NormalizedValueRef;
typedef JZ_VEC(jazz_engine_NormalizedValueRef) jazz_engine_NormalizedValueRefVec;
typedef struct jazz_engine_TypedOutputField { jz_str name; groove_ColumnType ty; } jazz_engine_TypedOutputField;
typedef struct jazz_engine_ValueSourceColumn { jz_str name; jazz_engine_NormalizedValueRef value; groove_ColumnType ty; } jazz_engine_ValueSourceColumn;
typedef struct jazz_engine_RowProjection { jazz_engine_TypedOutputField output; jazz_engine_NormalizedValueRef value; } jazz_engine_RowProjection;
typedef struct jazz_engine_OrderKey { jazz_engine_NormalizedValueRef value; u32 direction; } jazz_engine_OrderKey;
typedef struct jazz_engine_AggregateExpr { jazz_engine_TypedOutputField output; u32 function; bool has_input; jazz_engine_NormalizedValueRef input; } jazz_engine_AggregateExpr;
typedef struct jazz_engine_PredicateExpr jazz_engine_PredicateExpr; /* normalized predicate (opaque) */
jz_err jazz_engine__query_engine__input__PredicateExpr__try_for_each_value(const jazz_engine_PredicateExpr *p, jz_err (*f)(const jazz_engine_NormalizedValueRef *v, void *ctx), void *ctx);
enum { JZ_RSE_SOURCE = 0, JZ_RSE_VALUE_SOURCE, JZ_RSE_FRONTIER_SOURCE, JZ_RSE_FILTER, JZ_RSE_JOIN, JZ_RSE_RECURSIVE,
       JZ_RSE_UNION, JZ_RSE_DISTINCT, JZ_RSE_PROJECT, JZ_RSE_CORRELATED_PATH_PROJECTION, JZ_RSE_ORDER_BY, JZ_RSE_SLICE, JZ_RSE_AGGREGATE };
enum { JZ_VSM_BINDING = 0, JZ_VSM_INLINE = 1 };
typedef struct jazz_engine_RowSetExpr {
    u32 tag;
    jazz_engine_SourceId source; u32 visibility;                     /* Source */
    jz_str vs_shape; JZ_VEC(jazz_engine_ValueSourceColumn) columns; u32 vs_mode; /* ValueSource / FrontierSource */
    jazz_engine_PredicateExpr *predicate;                            /* Filter.predicate / Join.on / CorrelatedPathProjection.correlation */
    jazz_engine_NormalizedValueRef frontier_key; jazz_engine_NormalizedValueRefVec dedupe_keys; /* RecursiveRelation */
    JZ_VEC(jazz_engine_RowProjection) projections;                   /* Project.columns */
    jazz_engine_NormalizedValueRefVec keys;                          /* Distinct.keys */
    JZ_VEC(jazz_engine_OrderKey) order_keys;                         /* OrderBy.keys */
    jazz_engine_NormalizedValueRefVec partition_by, tie_breaker;     /* Slice */
    jazz_engine_NormalizedValueRefVec group_by; JZ_VEC(jazz_engine_AggregateExpr) outputs; /* Aggregate */
    void *rest;
} jazz_engine_RowSetExpr;
typedef struct jazz_engine_NormalizedRowSetShape {
    jazz_model_ShapeId shape_id; jz_bytes canonical;   /* identity */
    jazz_engine_RowSetNodeId root;
    jz_str result;
    jz_btree auxiliary_sources;       /* BTreeSet<SourceId> */
    JZ_VEC(struct jazz_engine_ClosurePath) closure_paths;
    JZ_VEC(void) join_contributions;
    JZ_VEC(void) inherited_contributions;
    JZ_VEC(void) reachable_contributions;
    jz_btree nodes;                   /* RowSetNodeId -> RowSetExpr */
} jazz_engine_NormalizedRowSetShape;

typedef struct jazz_engine_ProgramBinding {
    jazz_model_BindingId id;
    bool has_source_shape; jz_str source_shape;
    jz_btree extra_user_params; jz_btree param_types; jz_btree claim_params; jz_btree values;
} jazz_engine_ProgramBinding;
typedef struct jazz_engine_QueryProgramRequest {
    jazz_engine_QueryAuthorizationMode authorization_mode;
    jazz_engine_RequestedReadSet reads;
    jazz_engine_PolicyContext policy;
    jazz_engine_NormalizedRowSetShape shape;    /* input.shape */
    jazz_engine_ProgramBinding binding;         /* input.binding */
    void *output;                               /* RowSetOutputRequest */
} jazz_engine_QueryProgramRequest;
void jazz_engine_QueryProgramRequest_clone(jazz_engine_QueryProgramRequest *dst, const jazz_engine_QueryProgramRequest *src);
void jazz_engine_QueryProgramRequest_drop(void *r);

typedef struct jazz_engine_SourceRequest { jazz_engine_SourceId source; jazz_engine_SourceExpr expr; void *extra; } jazz_engine_SourceRequest;

/* AppRowSchema / terminals */
enum { JZ_ART_ROWS = 0, JZ_ART_AGGREGATE = 1 };
typedef struct jazz_engine_AggregateResultSchema { JZ_VEC(groove_DescriptorField) group_key_fields; JZ_VEC(groove_DescriptorField) aggregate_fields; } jazz_engine_AggregateResultSchema;
typedef struct jazz_engine_AppRowSchema { u32 terminal_tag; jazz_engine_AggregateResultSchema aggregate; void *rows; jz_btree publication_fields; /* String -> CurrentRowPublicationField */ } jazz_engine_AppRowSchema;

groove_Value jazz_engine__query_engine__coerce_prepared_binding_value(groove_Value v, const groove_ColumnType *ty);

/* ======================= jazz_node (other units) ======================= */
/* node::Error codes (thiserror enum in node/mod.rs) */
enum {
    JZN_E_Groove = 0x0100, JZN_E_ChunkStorage, JZN_E_LargeValueUploadChunkUnavailable, JZN_E_LargeValue,
    JZN_E_LargeValueReachability, JZN_E_LargeValueIngressRateLimited, JZN_E_LargeValueStageExpired,
    JZN_E_Record, JZN_E_MalformedCurrentRow, JZN_E_Storage, JZN_E_ClockOverflow, JZN_E_Query,
    JZN_E_QueryLowering, JZN_E_QueryCapability, JZN_E_AuthorizationSupportMissingClaim, JZN_E_PolicyProofCycle,
    JZN_E_TableNotFound, JZN_E_UnsupportedColumnType, JZN_E_InvalidMergeableCommit, JZN_E_UnadmittedWriteAuthor,
    JZN_E_InvalidBranchKey, JZN_E_UnsupportedExclusiveRead, JZN_E_TransactionConflict, JZN_E_InvalidStoredValue,
    JZN_E_InvalidAuthoritySourceClosure, JZN_E_MissingTransaction, JZN_E_MalformedViewUpdate,
    JZN_E_MaintainedViewMissingBundleWitness, JZN_E_MissingOpenBatch, JZN_E_DuplicateOpenBatch,
    JZN_E_OpenTransactionIdentityMismatch, JZN_E_NonMonotoneState, JZN_E_ConflictingCommitUnit,
    JZN_E_ConflictingFate, JZN_E_UnsupportedCommitUnit, JZN_E_UnsupportedSyncMessage,
    JZN_E_UnauthorizedCatalogueUpdate, JZN_E_InvalidCatalogueUpdate, JZN_E_CatalogueActivationFailed,
    JZN_E_CatalogueUninitialized, JZN_E_CatalogueCodec, JZN_E_HistoricalReadRequiresServer,
    JZN_E_AuthorizationDenied, JZN_E_SubscriptionClosed,
};
/* jazz_model QueryError codes used directly */
enum { JZM_QE_MissingParam = 0x0200, JZM_QE_UnknownTable, JZM_QE_UnknownColumn, JZM_QE_Other };

#define JZN_INVALID_STORED(msg) jz_err_fmt(JZN_E_InvalidStoredValue, "invalid stored value: %s", (msg))
#define JZN_MALFORMED_VIEW(msg) jz_err_fmt(JZN_E_MalformedViewUpdate, "malformed view update: %s", (msg))
#define JZN_INVALID_CATALOGUE(msg) jz_err_fmt(JZN_E_InvalidCatalogueUpdate, "invalid catalogue update: %s", (msg))
#define JZN_NON_MONOTONE(msg) jz_err_fmt(JZN_E_NonMonotoneState, "non-monotone state update: %s", (msg))
#define JZN_UNSUPPORTED_COMMIT(msg) jz_err_fmt(JZN_E_UnsupportedCommitUnit, "unsupported commit unit: %s", (msg))
#define JZN_INVALID_MERGEABLE(msg) jz_err_fmt(JZN_E_InvalidMergeableCommit, "invalid mergeable commit: %s", (msg))
#define JZN_MISSING_BUNDLE_WITNESS(msg) jz_err_fmt(JZN_E_MaintainedViewMissingBundleWitness, "maintained subscription view missing bundle witness: %s", (msg))
jz_err jazz_node__node__Error_query_lowering(jz_str msg);   /* Error::QueryLowering(String) */
jz_err jazz_node__node__Error_query_capability(jz_str msg); /* Error::QueryCapability(String) */
bool jazz_node__node__is_unrepresentable_enum_projection(const jz_err *e);

/* node::CurrentRow */
typedef struct jazz_node_CurrentRowPublicationField jazz_node_CurrentRowPublicationField;
typedef struct jazz_node_CurrentRow {
    const jz_str *table;                     /* Intern<String> */
    groove_OwnedRecord *record;              /* Arc<OwnedRecord> */
    bool deleted;
    void *publication_fields;                /* Arc<Vec<..>> */
} jazz_node_CurrentRow;
typedef JZ_VEC(jazz_node_CurrentRow) jazz_node_CurrentRowVec;
void jazz_node_CurrentRow_clone(jazz_node_CurrentRow *dst, const jazz_node_CurrentRow *src);
void jazz_node_CurrentRow_drop(void *r);
bool jazz_node_CurrentRow_eq(const jazz_node_CurrentRow *a, const jazz_node_CurrentRow *b);
bool jazz_node__node__CurrentRow__application_column_index_by_name(const jazz_node_CurrentRow *r, const char *name, u32 nlen, u32 *out);
jazz_types_RowUuid jazz_node__node__CurrentRow__row_uuid(const jazz_node_CurrentRow *r);
jazz_node_CurrentRow jazz_node__node__CurrentRow__new(const jz_str *table, groove_OwnedRecord record);
const jz_str *jazz_node__node__CurrentRow__table(const jazz_node_CurrentRow *r);

/* node::CoveredInputCoordinate / AuthorityResultState */
typedef struct jazz_node_CoveredInputCoordinate {
    jazz_types_GlobalPhysicalTableId physical_table;
    jazz_types_RowUuid row;
    jazz_protocol_ResultRowLayer layer;
    jz_bytes branch;
} jazz_node_CoveredInputCoordinate;
jazz_node_CoveredInputCoordinate jazz_node__node__CoveredInputCoordinate__from(const jazz_protocol_SupportingRow *r);
extern const jz_btree_desc jazz_node_COVERED_INPUTS_DESC; /* CoveredInputCoordinate -> SupportingRow */
enum { JZN_ASC_PENDING = 0, JZN_ASC_CLAIMED = 1 };
typedef struct jazz_node_AuthoritySourceClosure { u32 tag; u64 generation; } jazz_node_AuthoritySourceClosure;
typedef struct jazz_node_AuthorityResultState {
    u64 applied_view_update_generation;
    jazz_node_AuthoritySourceClosure source_closure;
    JZ_VEC(void) source_incrementals;
    bool live_settled;
    bool has_supporting_revision; u8 supporting_revision[16];
    jz_btree covered_input_versions;
    jz_btree *compiled_covered_input_sources;
    bool has_settled_through; jazz_types_GlobalTime settled_through;
    bool has_authorization_progress; u64 authorization_progress;
    bool known_state_declared, initial_hydration, deferred_publication;
    bool has_pending_authoritative_reset; u64 pending_authoritative_reset;
    bool pending_opening;
} jazz_node_AuthorityResultState;
void jazz_node_AuthorityResultState_init(jazz_node_AuthorityResultState *s); /* Default */

/* node::physical SchemaPhysicalMapping (only the identities.tables map is read here) */
typedef struct jazz_protocol_PhysicalColumnIdentity { jz_uuid id; jz_btree enum_variants; /* String -> Vec<Uuid> */ } jazz_protocol_PhysicalColumnIdentity;
typedef struct jazz_node_PhysicalTableIdentity { jazz_types_GlobalPhysicalTableId id; jz_btree columns; /* String -> PhysicalColumnIdentity */ } jazz_node_PhysicalTableIdentity;
typedef struct jazz_protocol_PhysicalIdentityManifest { jz_btree tables; /* String -> PhysicalTableIdentity */ } jazz_protocol_PhysicalIdentityManifest;
void jazz_protocol_PhysicalIdentityManifest_clone(jazz_protocol_PhysicalIdentityManifest *dst, const jazz_protocol_PhysicalIdentityManifest *src);
void jazz_protocol_PhysicalIdentityManifest_drop(void *m);
bool jazz_protocol_PhysicalIdentityManifest_eq(const jazz_protocol_PhysicalIdentityManifest *a, const jazz_protocol_PhysicalIdentityManifest *b);
typedef JZ_VEC(jz_uuid) jz_uuid_vec;
void jazz_node_PhysicalTableIdentity_clone(jazz_node_PhysicalTableIdentity *dst, const jazz_node_PhysicalTableIdentity *src);
void jazz_node_PhysicalTableIdentity_drop(void *t);
typedef JZ_VEC(jazz_protocol_PhysicalIdentityManifest) jazz_protocol_PhysicalIdentityManifestVec;
void jazz_protocol_PhysicalIdentityManifestVec_drop(jazz_protocol_PhysicalIdentityManifestVec *v);
/* node::physical catalogue case identities */
typedef jz_uuid jazz_types_GlobalPhysicalEnumVariantId;
typedef struct jazz_node_GlobalScalarEnumCaseId { jazz_types_GlobalPhysicalEnumVariantId id; jz_uuid introducing_schema; u8 introducing_ordinal; } jazz_node_GlobalScalarEnumCaseId;
typedef struct jazz_node_GlobalEnumCaseId { jazz_types_GlobalPhysicalEnumVariantId id; jz_uuid introducing_schema; u32 introducing_ordinal; } jazz_node_GlobalEnumCaseId;
typedef JZ_VEC(jazz_node_GlobalScalarEnumCaseId) jazz_node_GlobalScalarEnumCaseIdVec;
typedef JZ_VEC(jazz_node_GlobalEnumCaseId) jazz_node_GlobalEnumCaseIdVec;
jazz_node_GlobalScalarEnumCaseIdVec jazz_node_GlobalScalarEnumCaseIdVec_clone(const jazz_node_GlobalScalarEnumCaseIdVec *v);
jazz_node_GlobalEnumCaseIdVec jazz_node_GlobalEnumCaseIdVec_clone(const jazz_node_GlobalEnumCaseIdVec *v);
extern const jz_btree_desc jazz_node_SCALAR_CASE_SET_DESC;      /* BTreeSet<GlobalScalarEnumCaseId> */
extern const jz_btree_desc jazz_node_NESTED_CASES_BY_PATH_DESC; /* String -> Vec<GlobalScalarEnumCaseId> (or GlobalEnumCaseId) */
/* node::physical TablePhysicalMapping */
typedef struct jazz_node_PhysicalVariantCase { u32 tag; jz_btree fields; } jazz_node_PhysicalVariantCase;
typedef JZ_VEC(jazz_node_PhysicalVariantCase) jazz_node_PhysicalVariantCaseVec;
jazz_node_PhysicalVariantCaseVec jazz_node_PhysicalVariantCaseVec_clone(const jazz_node_PhysicalVariantCaseVec *v);
void jazz_node_PhysicalVariantCaseVec_drop(jazz_node_PhysicalVariantCaseVec *v);
typedef struct jazz_node_PhysicalTableMapping {
    jazz_types_PhysicalTableId table_id;
    jz_btree columns;                         /* String -> PhysicalColumnId */
    jazz_node_PhysicalVariantCaseVec variant_cases;
    jz_btree scalar_enum_cases;               /* PhysicalColumnId -> Vec<GlobalScalarEnumCaseId> */
    jz_btree payload_enum_cases;              /* PhysicalColumnId -> Vec<GlobalPayloadEnumCaseId> */
    jz_btree nested_scalar_enum_cases;        /* PhysicalColumnId -> BTreeMap<String, Vec<..>> */
    jz_btree nested_payload_enum_cases;       /* PhysicalColumnId -> BTreeMap<String, Vec<..>> */
} jazz_node_PhysicalTableMapping;
void jazz_node_PhysicalTableMapping_clone(jazz_node_PhysicalTableMapping *dst, const jazz_node_PhysicalTableMapping *src);
void jazz_node_PhysicalTableMapping_drop(void *t);
typedef struct jazz_node_SchemaPhysicalMapping {
    jazz_protocol_PhysicalIdentityManifest identities;
    jz_btree tables;               /* String -> PhysicalTableMapping */
} jazz_node_SchemaPhysicalMapping;
void jazz_node_SchemaPhysicalMapping_clone(jazz_node_SchemaPhysicalMapping *dst, const jazz_node_SchemaPhysicalMapping *src);
void jazz_node_SchemaPhysicalMapping_drop(void *m);
bool jazz_node_SchemaPhysicalMapping_eq(const jazz_node_SchemaPhysicalMapping *a, const jazz_node_SchemaPhysicalMapping *b);

/* node::SchemaCatalogue */
typedef struct jazz_node_ActiveSchema { jazz_types_SchemaVersionId schema; u64 revision; jazz_model_JazzSchema compiled; } jazz_node_ActiveSchema;
typedef struct jazz_node_SchemaCatalogue {
    jazz_types_SchemaVersionId local_schema_version_id;
    bool has_local_schema_version_alias; jazz_types_SchemaVersionAlias local_schema_version_alias;
    jazz_model_JazzSchema schema;
    jz_btree schema_version_aliases;     /* SchemaVersionId -> SchemaVersionAlias */
    jz_btree catalogue_schemas;          /* SchemaVersionId -> SchemaVersion */
    jz_btree catalogue_lenses;           /* MigrationLensId -> MigrationLens */
    jz_btree physical_mappings;          /* SchemaVersionId -> SchemaPhysicalMapping */
    jz_btree physical_current_winner_projections;
    jz_btree staged_lineages, pending_lineages, active_lineages_by_target;
    u64 active_catalogue_seq, next_physical_table_id, next_physical_column_id;
    jz_btree lens_path_cache, compiled_lens_cache, physical_write_plan_cache;
    jazz_node_ActiveSchema active_schema;
} jazz_node_SchemaCatalogue;

/* node::QueryServing */
typedef struct jazz_node_QueryServing {
    jz_btree watermark_restore_seen;            /* BTreeSet<AuthorityResultKey> */
    jz_btree persisted_watermarks;              /* AuthorityResultKey -> (GlobalTime,[u8;16]) */
    bool watermarks_invalidated;
    jz_btree local_availability_records;        /* (PolicyBindingKey, GPTId, RowUuid) -> LocalAvailabilityRecord */
    jz_btree local_availability_authorities;    /* PolicyBindingKey -> (NodeUuid, u64) */
    jz_btree local_unavailable_inputs;          /* (PolicyBindingKey, GPTId) -> LocalUnavailableInput */
    jz_btree query_shape_cache;
    jz_btree read_policy_authorization_request_cache;
    jz_btree compiled_query_program_cache;
    void *query_program_templates;              /* QueryProgramTemplateCache */
    jz_deque supported_query_program_requests;
    jz_btree policy_authorization_graph_cache;
    jz_btree policy_authorization_graph_replacements;
    struct { struct jazz_node_PolicyProofStackEntry *ptr; u32 len, cap; } policy_proof_stack;
    jz_btree tx_versions_cache;
    jz_deque tx_version_tables_cache_order;
    jz_btree tx_version_tables_cache_order_set;
    jz_btree version_storage_sources_cache;
    jz_btree registered_shapes;                 /* ShapeId -> ValidatedQuery */
    jz_btree registered_shape_options;
    jz_btree peer_shape_owners;
    jz_btree locally_registered_shapes;
    jz_btree outbound_shape_owners;
    jz_btree outbound_binding_owners;
    jz_btree registered_bindings;
    jz_btree authority_results;                 /* AuthorityResultKey -> AuthorityResultState */
    jz_btree applied_view_update_generations;
    jz_btree retained_root_window_sources;
} jazz_node_QueryServing;
void jazz_engine__query_engine__QueryProgramTemplateCache__clear(void *c);
extern const jz_btree_desc jazz_node_AUTHORITY_RESULT_KEY_SET_DESC;

typedef struct jazz_node_Clock {
    jazz_types_TxTime tx_time;
    jazz_types_TxTime *reservation_high_water; /* Rc<Cell<TxTime>> (rc payload) */
    jazz_types_GlobalTime global_time_register;
    jz_btree locally_minted_global_times;
    jazz_types_GlobalTime committed_global_time;
    jz_btree applied_global_times_after_frontier;
    jz_btree frontier_dots;
} jazz_node_Clock;

/* node::SyncMetrics */
typedef struct jazz_node_SyncMetrics {
    u64 parked_orphans, parked_orphans_resolved, parked_catalogue_orphans, parked_catalogue_orphans_resolved;
    u64 dropped_malformed_relay_commit_units, parked_catalogue_shapes, parked_catalogue_shapes_resolved;
    u64 dropped_detached_subscription_messages, dropped_peer_request_messages, transport_backpressure_retries;
    u64 receiver_bulk_bundle_ingests, receiver_per_bundle_ingests, receiver_bulk_ingest_commits;
    u64 receiver_history_table_probes, receiver_history_rebuild_rows_avoided;
    u64 authoritative_reset_missing_payload_fallbacks, peer_payload_inventory_missing_fallbacks;
} jazz_node_SyncMetrics;

/* node::Parking (in-memory delivery state) */
typedef struct jazz_node_Parking {
    jz_btree parked_shape_registrations, parked_binding_deltas, parked_commit_units, parked_catalogue_commit_units;
    u64 newest_ingest_ms;
} jazz_node_Parking;
void jazz_node_Parking_clone(jazz_node_Parking *dst, const jazz_node_Parking *src);
void jazz_node_Parking_drop(jazz_node_Parking *p);
void jazz_node_Parking_default(jazz_node_Parking *p);

/* node::NodeState<S> (generic S erased: storage lives behind the groove Database) */
typedef struct jazz_node_NodeState {
    jazz_types_NodeUuid node_uuid;
    bool has_self_node_alias; jazz_types_NodeAlias self_node_alias;
    jazz_node_SchemaCatalogue catalogue;
    u32 catalogue_bootstrap_state;   /* 0 Uninitialized, 1 Ready */
    bool catalogue_bootstrap_marker;
    jazz_node_Clock clock;
    jazz_node_Parking parking;
    jazz_node_QueryServing query;
    struct {
        jz_btree open_transactions, closed_batches, local_permission_subjects; /* .., .., TxId -> AuthorSubject */
        JZ_VEC(void) released_large_values;
        jz_btree pending_foreign_transactions;
    } open_tx;
    struct { jz_btree rejected_transactions; /* TxId -> RejectedTransaction* */ jz_btree pending_cascade_parents; jz_btree pending_cascade_children; } rejections;
    groove_Database *database;       /* DatabaseSlot derefs to Database */
    void *local_chunk_reader;
    void *chunk_resolver;
    void *detach_covered_chunk_waits;
    struct { u64 incoming_bytes_per_window, window_ms, max_age_ms; } large_value_staging_policy;
    struct { u64 window_started_ms, admitted_bytes; } large_value_ingress;
    void *content_runtime_provider;
    u64 groove_runtime_token;
    u64 physical_identity_generation;
    u64 next_authoritative_reset_generation;
    bool history_complete;
    bool authoritative_scalar_exit_refresh;
    jz_hmap client_local_literal_shapes;
    jazz_model_DurabilityTier authored_commit_durability;
    void *relay_authority_session_owner;
    jz_btree pending_persistence;
    void *node_aliases;
    bool has_absent_node_alias; jazz_types_NodeUuid absent_node_alias;
    void *ahead_current_keys;
    jz_btree tx_touched_dirty;
    jz_btree history_tx_authors;
    jz_hmap history_tx_seqs;
    bool minting_global_time;
    jazz_node_SyncMetrics sync_metrics;
    void *query_engine_read_metrics;
    jz_btree session_claims;
    jz_btree session_claim_revisions;
    bool has_active_session_claims; jazz_types_AuthorSubject active_session_identity; jz_btree active_session_claims;
    bool permissions_ready;
    bool catalogue_activation_failed;
    bool has_initial_sync_flush_cadence; u32 initial_sync_flush_cadence;
    bool initial_sync_flush_active;
    bool initial_sync_flush_completed;
} jazz_node_NodeState;

/* NodeState methods owned by other units (node/mod.rs, views.rs, query_eval.rs, ...) */
jz_err jazz_node__node__views__NodeState__scope_physical_table(jazz_node_NodeState *self, jazz_types_SchemaVersionId schema, const jz_str *table, jazz_types_GlobalPhysicalTableId *out);
jz_err jazz_node__node__query_eval__NodeState__query_rows_for_link(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_node_CurrentRowVec *out);
jz_err jazz_node__node__NodeState__table(const jazz_node_NodeState *self, const jz_str *name, const jazz_model_TableSchema **out);
bool jazz_node__peer__publication__row_local_membership(const jazz_model_Query *q);
bool jazz_node__peer__publication__query_uses_claims(const jazz_model_Query *q);

/* node::layer_stack */
jz_err jazz_node__node__layer_stack__branch_view_stack(const jazz_model_JazzSchema *schema, const jazz_model_TableSchema *table, const void *head, const void *base, jazz_model_BranchKey *head_out, void **base_out);

/* node::codec */
jz_err jazz_node__node__codec__runtime_result_identity_bytes(const groove_Value *v, const groove_ValueType *ty, jz_bytes *out);

/* node::query_eval (unit node-qe-a) helpers re-exported to submodules via `use super::*` */
jz_str jazz_node__node__query_eval__aggregate_output_column(const jz_str *alias);
enum { JZ_RME_ROW = 0, JZ_RME_SYNTHETIC = 1 };
typedef struct jazz_node_ResultMemberEntry { u32 tag; jz_str table; jz_bytes row; jz_bytes replacement; } jazz_node_ResultMemberEntry;
jz_err jazz_node__node__query_eval__aggregate_result_member_row_uuid(const jazz_node_ResultMemberEntry *e, jazz_types_RowUuid *out);
jz_bytes jazz_node__node__SyntheticReplacementToken__from_encoded_record(jz_bytes b);

/* schema store names (jazz_model::schema) */
extern const char *const jazz_model__schema__SUBSCRIPTION_WATERMARKS_STORE;
extern const char *const jazz_model__schema__LOCAL_ROW_AVAILABILITY_STORE;
extern const char *const jazz_model__schema__AUTHORITY_POLICY_BINDINGS_STORE;
groove_RecordDescriptor *jazz_model__schema__local_availability_record_descriptor(void);

/* protocol limits / authorization scope */
#define JZ_MAX_KNOWN_STATE_EXACT_REFS 1024u
#define JZ_MAX_AUTHORIZATION_SCOPES 256u

/* node::PreparedQueryParam / PreparedQueryPlan (node/mod.rs) */
enum { JZN_PQPS_USER = 0, JZN_PQPS_CLAIM = 1 };
typedef struct jazz_node_PreparedQueryParam {
    jz_str name;
    groove_ColumnType ty;
    u32 source_tag; jazz_engine_ClaimPath claim;
} jazz_node_PreparedQueryParam;
void jazz_node_PreparedQueryParam_drop(void *p);
void jazz_node_PreparedQueryParam_clone(jazz_node_PreparedQueryParam *dst, const jazz_node_PreparedQueryParam *src);
enum { JZN_PQP_GRAPH = 0, JZN_PQP_PREPARED = 1, JZN_PQP_PEER_MAINTAINED_MARKER = 2 };
typedef struct jazz_node_PreparedQueryPlan {
    u32 tag;
    groove_GraphBuilder *graph;                       /* Graph */
    u64 shape;                                        /* Prepared: PreparedShapeId */
    JZ_VEC(jazz_node_PreparedQueryParam) params;      /* Prepared */
    jazz_engine_AppRowSchema output;
} jazz_node_PreparedQueryPlan;
typedef jazz_node_PreparedQueryPlan *jazz_node_PreparedQueryPlanHandle; /* Arc */
void jazz_node_PreparedQueryPlan_drop(void *p);

/* query_eval::lowering (unit node-qe-a) */
typedef struct jazz_node_SupportedQueryProgram jazz_node_SupportedQueryProgram;
void jazz_node_SupportedQueryProgram_drop(void *p);

/* jazz_model query builders / validation */
jazz_model_Query jazz_model__query__builders__Query__from(const char *table, u32 len);
jz_err jazz_model__query__builders__Query__validate(const jazz_model_Query *q, const jazz_model_JazzSchema *schema, jazz_model_ValidatedQuery *out);
jz_err jazz_model__query__validation__ValidatedQuery__bind(const jazz_model_ValidatedQuery *q, jz_btree values, jazz_model_Binding *out);
void jazz_model__query__policy_and_request_contracts__Predicate__for_each_operand_mut(jazz_model_Predicate *p, void (*f)(jazz_model_Operand *op, void *ctx), void *ctx);
void jazz_model_QueryError_display(jz_str *out, const jz_err *e);
/* protocol SchemaVersion */
typedef struct jazz_protocol_SchemaVersion { jazz_types_SchemaVersionId id; jazz_model_JazzSchema schema; } jazz_protocol_SchemaVersion;
/* query_eval::normalization (unit node-qe-a) */
jz_err jazz_node__node__query_eval__normalization__NodeState__normalized_row_set_shape(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_engine_NormalizedRowSetShape *out);
void jazz_engine_NormalizedRowSetShape_drop(void *s);
jazz_engine_SourceId jazz_node__node__query_eval__normalization__root_source_id(const char *table, u32 len);
jazz_engine_SourceId jazz_node__node__query_eval__normalization__nested_join_source_id(const jazz_model_JoinVia *join, const jz_str *path);
jazz_engine_SourceId jazz_node__node__query_eval__normalization__include_auxiliary_source_id(jz_str table, u32 include_index, u32 segment_index);
jazz_engine_SourceId jazz_node__node__query_eval__normalization__correlated_child_source_id(const jazz_engine_SourceId *owner, const jazz_model_ArraySubquery *sub, const u32 *path, u32 n);

/* jazz_engine::query_engine::lowering::{QueryProgram, LoweredGraph} */
typedef struct jazz_engine_ParameterDomain { jz_btree user_params; jz_btree claim_params; jz_btree routing_params; } jazz_engine_ParameterDomain;
typedef struct jazz_engine_OutputTerminalSchema { u32 tag; void *payload; } jazz_engine_OutputTerminalSchema;
typedef struct jazz_engine_LoweredTerminal { jz_str sink; groove_GraphBuilder *graph; jazz_engine_OutputTerminalSchema output; } jazz_engine_LoweredTerminal;
typedef struct jazz_engine_ProgramOutputSchemas { void *payload; } jazz_engine_ProgramOutputSchemas;
typedef struct jazz_engine_LoweredGraph {
    JZ_VEC(jazz_engine_LoweredTerminal) terminals;
    jz_btree shared_witness_sinks;
    groove_GraphBuilder *internal_app_rows_graph; /* Option */
    jazz_engine_ParameterDomain parameters;
    jazz_engine_ProgramOutputSchemas output;
} jazz_engine_LoweredGraph;
typedef struct jazz_engine_QueryProgram {
    jazz_engine_QueryProgramRequest request;
    jazz_engine_LoweredGraph lowered;
    jz_btree source_descriptors;
    jz_btree covered_input_source_descriptors;
    void *explain;
} jazz_engine_QueryProgram;
void jazz_engine_QueryProgram_drop(void *p);
/* query_eval::lowering (unit node-qe-a) */
jz_str_vec jazz_node__node__query_eval__lowering__prepared_route_param_names(const jazz_engine_ParameterDomain *d);
jz_err jazz_node__node__query_eval__lowering__terminal_route_eligible_fields(const jazz_engine_OutputTerminalSchema *t, jz_str_vec *out);
jz_err jazz_node__node__query_eval__lowering__app_row_terminal_route_eligible_fields(const jazz_engine_ProgramOutputSchemas *o, jz_str_vec *out);
jz_str_vec jazz_node__node__query_eval__lowering__terminal_route_fields(const jz_str *route_params, u32 nparams, const jz_str *eligible, u32 neligible);
void jazz_node_Error_debug(jz_str *out, const jz_err *e);
void groove_Error_debug(jz_str *out, const jz_err *e);
void jazz_model_BindingId_debug(jz_str *out, const jazz_model_BindingId *id);

/* jazz_model query helpers used by bindings.rs */
void jazz_model__query__policy_and_request_contracts__Query__for_each_condition_mut(jazz_model_Query *q, void (*f)(jazz_model_Predicate *p, void *ctx), void *ctx);
bool jazz_model__query__policy_and_request_contracts__Predicate__any_operand(const jazz_model_Predicate *p, bool (*f)(const jazz_model_Operand *op, void *ctx), void *ctx);
jz_str_vec jazz_model__query__policy_and_request_contracts__operand_claim_path(const jz_str *name);
jz_err jazz_model__query__builders__Query__validate_runtime(const jazz_model_Query *q, const jazz_model_JazzSchema *schema, jazz_model_ValidatedQuery *out);
jz_err jazz_model__query__builders__Query__validate_with_schema_version(const jazz_model_Query *q, const jazz_model_JazzSchema *schema, jazz_types_SchemaVersionId v, jazz_model_ValidatedQuery *out);
jz_err jazz_model__query__validation__flat_join_predicate_sources(const jazz_model_Predicate *p, jz_btree *out);
jz_err jazz_model__query__validation__unqualify_flat_join_predicate(const jazz_model_Predicate *p, const jz_str *scope, jazz_model_Predicate *out);
jz_err jazz_model__query__validation__qualify_flat_join_source_predicate(jazz_model_Predicate p, const jz_str *scope, jazz_model_Predicate *out);
jz_err jazz_model__query__validation__binding_id_for_values(const jz_btree *values, jazz_model_BindingId *out);
/* jazz_engine fields */
jz_str jazz_engine__query_engine__fields__claim_param_field(const jazz_engine_ClaimPath *path);
bool jazz_engine__query_engine__fields__claim_path_from_param_field(const jz_str *field, jazz_engine_ClaimPath *out);
void jazz_engine_ClaimPath_drop(jazz_engine_ClaimPath *p);
bool jazz_engine_ClaimPath_eq(const jazz_engine_ClaimPath *a, const jazz_engine_ClaimPath *b);
/* query_eval::normalization (unit node-qe-a) */
jz_err jazz_node__node__query_eval__normalization__operand_column_type(const jazz_model_JazzSchema *schema, const jazz_engine_SourceId *source, const jazz_model_Operand *op, bool *has, groove_ColumnType *out);
jz_err jazz_node__node__query_eval__normalization__contains_needle_type(const jazz_model_JazzSchema *schema, const jazz_engine_SourceId *source, const jazz_model_Operand *op, bool *has, groove_ColumnType *out);
groove_Value jazz_node__node__query_eval__normalization__coerce_literal_for_column_type(groove_Value v, const groove_ColumnType *ty);
jz_err jazz_node__node__query_eval__normalization__schema_column_type(const jazz_model_JazzSchema *schema, const jz_str *table, const jz_str *column, groove_ColumnType *out);


/* ===== jazz_protocol subscription wire types (subscriptions.rs) ===== */
#define JZ_SHAPE_AST_VERSION 0u
enum { JZ_SHAPE_BODY_QUERY = 0, JZ_SHAPE_BODY_RELATION = 1 };
typedef struct jazz_protocol_ShapeAst {
    u32 version;
    jazz_types_SchemaVersionId schema_version;
    u32 body_tag; jazz_model_Query query; jazz_model_RelationQuery relation;
} jazz_protocol_ShapeAst;
void jazz_protocol_ShapeAst_clone(jazz_protocol_ShapeAst *dst, const jazz_protocol_ShapeAst *src);
void jazz_protocol_ShapeAst_drop(void *a);
bool jazz_protocol_ShapeAst_eq(const jazz_protocol_ShapeAst *a, const jazz_protocol_ShapeAst *b);
enum { JZ_BINDING_SOURCE_ORDINARY = 0, JZ_BINDING_SOURCE_RELAY_AUTHORITY_SESSION = 1 };
typedef struct jazz_protocol_RegisterShapeOptions {
    jazz_model_DurabilityTier tier;
    jazz_protocol_ReadViewSpec read_view;
    u32 binding_source;
} jazz_protocol_RegisterShapeOptions;
jazz_protocol_RegisterShapeOptions jazz_protocol_RegisterShapeOptions_default(void);
void jazz_protocol_RegisterShapeOptions_clone(jazz_protocol_RegisterShapeOptions *dst, const jazz_protocol_RegisterShapeOptions *src);
void jazz_protocol_RegisterShapeOptions_drop(void *o);
bool jazz_protocol_RegisterShapeOptions_eq(const jazz_protocol_RegisterShapeOptions *a, const jazz_protocol_RegisterShapeOptions *b);
jazz_protocol_ReadViewKey jazz_protocol__protocol__RegisterShapeOptions__read_view_key(const jazz_protocol_RegisterShapeOptions *o);
static inline bool jazz_protocol_ReadViewKey_eq(jazz_protocol_ReadViewKey a, jazz_protocol_ReadViewKey b) { return jz_uuid_eq(&a.id, &b.id); }
static inline jazz_protocol_ReadViewKey jazz_protocol_ReadViewKey_default(void) { jazz_protocol_ReadViewKey k; memset(&k, 0, sizeof k); return k; }
static inline bool jazz_protocol_SubscriptionKey_eq(const jazz_protocol_SubscriptionKey *a, const jazz_protocol_SubscriptionKey *b) {
    return jz_uuid_eq(&a->shape_id, &b->shape_id) && jz_uuid_eq(&a->binding_id, &b->binding_id) && jazz_protocol_ReadViewKey_eq(a->read_view, b->read_view);
}
int jazz_protocol_SubscriptionKey_cmp(const void *a, const void *b, void *ctx);
static inline bool jazz_protocol_BindingViewKey_eq(const jazz_protocol_BindingViewKey *a, const jazz_protocol_BindingViewKey *b) {
    return jz_uuid_eq(&a->shape_id, &b->shape_id) && jz_uuid_eq(&a->binding_id, &b->binding_id) && jazz_protocol_ReadViewKey_eq(a->read_view, b->read_view);
}
typedef struct jazz_protocol_DelegatedSessionBinding { jazz_types_AuthorSubject identity; jz_btree claims; /* String -> Value */ } jazz_protocol_DelegatedSessionBinding;
enum { JZ_KSD_FAST = 0, JZ_KSD_FAST_WITH_AUTHORIZATION_PROGRESS = 1, JZ_KSD_RESERVED2 = 2, JZ_KSD_WATERMARK = 3 };
enum { JZ_KSC_FAST_CURRENT_MEMBERSHIP = 0 };
typedef struct jazz_protocol_KnownStateDeclaration {
    u32 tag;
    u32 completeness;
    jazz_types_GlobalTime position;
    bool has_authorization_progress; u64 authorization_progress;
    u8 supporting_revision[16];
} jazz_protocol_KnownStateDeclaration;
typedef struct jazz_protocol_Subscribe {
    jazz_model_ShapeId shape_id;
    jazz_protocol_SubscriptionKey subscription;
    groove_ValueVec values;
    bool has_known_state; jazz_protocol_KnownStateDeclaration known_state;
    bool has_delegated_session; jazz_protocol_DelegatedSessionBinding delegated_session;
} jazz_protocol_Subscribe;
void jazz_protocol_Subscribe_drop(void *s);
jazz_protocol_PolicyBindingKey jazz_protocol__protocol__PolicyBindingKey__from_delegated_session(const jazz_protocol_DelegatedSessionBinding *s);
jazz_protocol_PolicyBindingKey jazz_protocol__protocol__PolicyBindingKey__from_canonical_parts(jazz_types_AuthorSubject identity, jz_btree claims);
jazz_protocol_AuthorityResultKey jazz_protocol__protocol__AuthorityResultKey__unscoped(jazz_protocol_BindingViewKey bv);
jazz_protocol_AuthorityResultKey jazz_protocol__protocol__AuthorityResultKey__policy_scoped(jazz_protocol_BindingViewKey bv, jazz_protocol_PolicyBindingKey p);
jazz_protocol_BindingViewKey jazz_protocol__protocol__BindingViewKey__from_canonical_subscription_key(jazz_protocol_SubscriptionKey s);
#define JZ_MAX_SHAPE_REGISTRATIONS_PER_PEER 1024u
#define JZ_MAX_RETAINED_PEER_SHAPES 1024u
jz_err jazz_model__query__relation_resolution__relation_query_to_query(const jazz_model_RelationQuery *r, jazz_model_Query *out);
jazz_model_ShapeId jazz_model__query__validation__ValidatedQuery__shape_id(const jazz_model_ValidatedQuery *q);

/* node::{ParkedSubscription, RegisteredBinding, RegisteredBindingUsageKey, RetainedRootWindowSource} (node/mod.rs) */
typedef struct jazz_node_ParkedSubscription {
    jazz_protocol_Subscribe subscribe;
    bool has_policy_binding; jazz_protocol_PolicyBindingKey policy_binding;
} jazz_node_ParkedSubscription;
typedef JZ_VEC(jazz_node_ParkedSubscription) jazz_node_ParkedSubscriptionVec;
void jazz_node_ParkedSubscription_drop(void *p);
extern const jz_btree_desc jazz_node_PARKED_BINDING_DELTAS_DESC; /* ShapeId -> Vec<ParkedSubscription> */
extern const jz_btree_desc jazz_node_PARKED_SHAPE_REGISTRATIONS_DESC; /* ShapeId -> ShapeAst */
typedef struct jazz_node_RegisteredBinding {
    groove_ValueVec values;
    jazz_protocol_ReadViewKey read_view;
    jazz_protocol_BindingViewKey binding_view_key;
    jazz_protocol_AuthorityResultKey authority_result_key;
    jazz_protocol_RegisterShapeOptions options;
    jazz_types_AuthorSubject compiler_identity;
} jazz_node_RegisteredBinding;
void jazz_node_RegisteredBinding_clone(jazz_node_RegisteredBinding *dst, const jazz_node_RegisteredBinding *src);
void jazz_node_RegisteredBinding_drop(void *b);
typedef struct jazz_node_RegisteredBindingUsageKey {
    jazz_model_BindingId binding_id;
    jazz_protocol_ReadViewKey read_view;
    bool has_policy_binding; jazz_protocol_PolicyBindingKey policy_binding;
} jazz_node_RegisteredBindingUsageKey;
void jazz_node_RegisteredBindingUsageKey_clone(jazz_node_RegisteredBindingUsageKey *dst, const jazz_node_RegisteredBindingUsageKey *src);
void jazz_node_RegisteredBindingUsageKey_drop(void *k);
extern const jz_btree_desc jazz_node_REGISTERED_BINDINGS_DESC;      /* ShapeId -> BTreeMap<UsageKey, RegisteredBinding> */
extern const jz_btree_desc jazz_node_REGISTERED_BINDING_MAP_DESC;   /* UsageKey -> RegisteredBinding */
extern const jz_btree_desc jazz_node_REGISTERED_SHAPE_OPTIONS_DESC; /* (ShapeId, ReadViewKey) -> RegisterShapeOptions */
extern const jz_btree_desc jazz_node_REGISTERED_SHAPES_DESC;        /* ShapeId -> ValidatedQuery */
extern const jz_btree_desc jazz_node_PEER_SHAPE_OWNERS_DESC;        /* ShapeId -> BTreeSet<u64> */
extern const jz_btree_desc jazz_node_OUTBOUND_SHAPE_OWNERS_DESC;    /* ShapeId -> BTreeSet<(u64, SubscriptionKey)> */
extern const jz_btree_desc jazz_node_OUTBOUND_BINDING_OWNERS_DESC;  /* (SubscriptionKey, PolicyBindingKey) -> BTreeSet<u64> */
extern const jz_btree_desc jazz_node_U64_SET_DESC;
extern const jz_btree_desc jazz_node_OWNER_SUBSCRIPTION_SET_DESC;   /* (u64, SubscriptionKey) */
extern const jz_btree_desc jazz_node_AUTHORITY_RESULTS_DESC;        /* AuthorityResultKey -> AuthorityResultState */
extern const jz_btree_desc jazz_node_RETAINED_ROOT_WINDOW_SOURCES_DESC; /* AuthorityResultKey -> RetainedRootWindowSource */
typedef struct jazz_node_ShapeReadViewKey { jazz_model_ShapeId shape_id; jazz_protocol_ReadViewKey read_view; } jazz_node_ShapeReadViewKey;
typedef struct jazz_node_OwnerSubscription { u64 owner; jazz_protocol_SubscriptionKey subscription; } jazz_node_OwnerSubscription;
typedef struct jazz_node_SubscriptionPolicyKey { jazz_protocol_SubscriptionKey subscription; jazz_protocol_PolicyBindingKey policy_binding; } jazz_node_SubscriptionPolicyKey;
typedef struct jazz_node_RetainedRootWindowSource jazz_node_RetainedRootWindowSource; /* opaque, by value in a btree slot */
typedef struct { u8 bytes[256]; } jazz_node_RetainedRootWindowSourceSlot;
void jazz_node__node__RetainedRootWindowSource__for_shape(const jazz_model_ValidatedQuery *shape, jazz_node_RetainedRootWindowSourceSlot *out);
bool jazz_node__node__RetainedRootWindowSource__is_bounded(const jazz_node_RetainedRootWindowSourceSlot *s);
void jazz_node_RetainedRootWindowSource_drop(void *s);
/* node::codec::VersionRow (opaque by value) */
typedef struct { u8 bytes[128]; } jazz_node_VersionRow;
extern const jz_btree_desc jazz_node_SUPPORTING_ROW_VERSION_ROW_DESC; /* SupportingRow -> VersionRow */
/* node::views */
jz_err jazz_node__node__views__NodeState__covered_input_version(jazz_node_NodeState *self, const jazz_protocol_SupportingRow *input, jazz_types_SchemaVersionId schema, bool *has, jazz_node_VersionRow *out);
jz_err jazz_node__node__views__NodeState__whole_table_subscription_key(const jazz_node_NodeState *self, const jz_str *table, jazz_protocol_SubscriptionKey *out);
jz_err jazz_node__node__Error_missing_transaction(jazz_model_TxId tx); /* Error::MissingTransaction */
void jazz_protocol_KnownStateDeclaration_debug(jz_str *out, const jazz_protocol_KnownStateDeclaration *k);
void jazz_protocol_DelegatedSessionBinding_debug(jz_str *out, const jazz_protocol_DelegatedSessionBinding *d, bool has);
void jazz_protocol_BindingViewKey_debug(jz_str *out, const jazz_protocol_BindingViewKey *k);
void jazz_protocol_AuthorityResultKey_debug(jz_str *out, const jazz_protocol_AuthorityResultKey *k);
extern const jz_btree_desc jazz_node_AUTHORITY_RESULT_GENERATION_DESC; /* AuthorityResultKey -> u64 */
extern const jz_btree_desc jazz_node_ROW_UUID_SET_DESC;

#define JZN_UNSUPPORTED_SYNC(msg) jz_err_fmt(JZN_E_UnsupportedSyncMessage, "unsupported sync message: %s", (msg))


/* ===== maintained_views.rs / materialization.rs external surface ===== */
/* jazz_types::object::OutputOccurrenceId (opaque, owned) */
typedef struct jazz_types_OutputOccurrenceId { void *repr; } jazz_types_OutputOccurrenceId;
typedef JZ_VEC(jazz_types_OutputOccurrenceId) jazz_types_OutputOccurrenceIdVec;
void jazz_types_OutputOccurrenceId_clone(jazz_types_OutputOccurrenceId *dst, const jazz_types_OutputOccurrenceId *src);
void jazz_types_OutputOccurrenceId_drop(void *o);
int jazz_types_OutputOccurrenceId_cmp(const void *a, const void *b, void *ctx);
void jazz_types_OutputOccurrenceId_debug(jz_str *out, const jazz_types_OutputOccurrenceId *o);
typedef struct jazz_types_UnionArm { u32 position; jz_str label; } jazz_types_UnionArm;
const jazz_types_UnionArm *jazz_types__object__OutputOccurrenceId__union_arms(const jazz_types_OutputOccurrenceId *o, u32 *n);
jazz_types_OutputOccurrenceId jazz_types__object__OutputOccurrenceId__single_source(jz_uuid object_id);

/* jazz_protocol::ResultMemberEntry: the shared mirror above is extended with
 * the real-row view. Opaque beyond `as_row`. */
bool jazz_protocol__protocol__ResultMemberEntry__as_row(const jazz_node_ResultMemberEntry *m, const jz_str **table, jazz_types_RowUuid *row, jazz_model_TxId *tx);
bool jazz_protocol__protocol__ResultMemberEntry__output_occurrence_id(const jazz_node_ResultMemberEntry *m, jazz_types_OutputOccurrenceId *out);
void jazz_protocol_ResultMemberEntry_clone(jazz_node_ResultMemberEntry *dst, const jazz_node_ResultMemberEntry *src);
void jazz_protocol_ResultMemberEntry_drop(void *m);
int jazz_protocol_ResultMemberEntry_cmp(const void *a, const void *b, void *ctx);
typedef JZ_VEC(jazz_node_ResultMemberEntry) jazz_protocol_ResultMemberEntryVec;
typedef struct jazz_protocol_ResultMemberPayloadEntry { jazz_node_ResultMemberEntry member; jz_bytes descriptor; jz_bytes record; } jazz_protocol_ResultMemberPayloadEntry;
void jazz_protocol_ResultMemberPayloadEntry_clone(jazz_protocol_ResultMemberPayloadEntry *dst, const jazz_protocol_ResultMemberPayloadEntry *src);
void jazz_protocol_ResultMemberPayloadEntry_drop(void *p);
bool jazz_protocol_ResultMemberPayloadEntry_eq(const jazz_protocol_ResultMemberPayloadEntry *a, const jazz_protocol_ResultMemberPayloadEntry *b);
typedef struct { jazz_node_ResultMemberEntry member; jazz_protocol_ResultMemberPayloadEntry payload; } jazz_protocol_MemberPayloadPair;
typedef JZ_VEC(jazz_protocol_MemberPayloadPair) jazz_protocol_MemberPayloadPairVec;
typedef struct jazz_protocol_RelationEdgeEntry {
    jz_str path;
    const jz_str *source_table; jazz_types_RowUuid source_row;
    const jz_str *target_table; jazz_types_RowUuid target_row;
    bool has_kind; u32 kind;
    bool has_source_version; jazz_protocol_RowVersionRefEntry source_version;
    bool has_target_version; jazz_protocol_RowVersionRefEntry target_version;
} jazz_protocol_RelationEdgeEntry;
enum { JZ_PFE_RESULT_PAYLOAD = 0, JZ_PFE_RELATION_EDGE = 1 /* other variants carried opaquely */ };
typedef struct jazz_protocol_ProgramFactEntry {
    u32 tag;
    jazz_protocol_ResultMemberPayloadEntry result_payload;
    jazz_protocol_RelationEdgeEntry relation_edge;
    void *other;
} jazz_protocol_ProgramFactEntry;
typedef JZ_VEC(jazz_protocol_ProgramFactEntry) jazz_protocol_ProgramFactEntryVec;
void jazz_protocol_ProgramFactEntry_clone(jazz_protocol_ProgramFactEntry *dst, const jazz_protocol_ProgramFactEntry *src);
void jazz_protocol_ProgramFactEntry_drop(void *f);
int jazz_protocol_ProgramFactEntry_cmp(const void *a, const void *b, void *ctx);
extern const jz_btree_desc jazz_protocol_RESULT_MEMBER_SET_DESC;          /* BTreeSet<ResultMemberEntry> */
extern const jz_btree_desc jazz_protocol_RESULT_MEMBER_PAYLOAD_DESC;      /* ResultMemberEntry -> ResultMemberPayloadEntry */
extern const jz_btree_desc jazz_protocol_PROGRAM_FACT_SET_DESC;           /* BTreeSet<ProgramFactEntry> */

/* node::{RelationEdge, RelationSnapshot} (node/mod.rs) */
typedef struct jazz_node_RelationEdge {
    jz_str source_table; jazz_types_RowUuid source_row;
    jz_str relation;
    jz_str target_table; jazz_types_RowUuid target_row;
} jazz_node_RelationEdge;
void jazz_node_RelationEdge_drop(void *e);
typedef JZ_VEC(jazz_node_RelationEdge) jazz_node_RelationEdgeVec;
typedef struct jazz_node_RelationSnapshot { usize root_count; jazz_node_CurrentRowVec rows; jazz_node_RelationEdgeVec edges; } jazz_node_RelationSnapshot;
void jazz_node_RelationSnapshot_drop(void *s);

/* jazz_engine::CurrentRowPublicationField; CurrentRow.publication_fields is an
 * Arc (jz_rc payload) of this vec. */
enum { JZ_CRPF_RESULT_FIELD = 0, JZ_CRPF_UNRESOLVED_SOURCE_CELL = 1, JZ_CRPF_STORED_COLUMN = 2 };
enum { JZ_CRRV_PUBLIC = 0, JZ_CRRV_HIDDEN_METADATA = 1 };
typedef struct jazz_engine_CurrentRowPublicationField { u32 tag; jz_str name; u32 visibility; jazz_types_PhysicalColumnId id; } jazz_engine_CurrentRowPublicationField;
typedef JZ_VEC(jazz_engine_CurrentRowPublicationField) jazz_engine_PublicationFieldVec;
void jazz_engine_CurrentRowPublicationField_clone(jazz_engine_CurrentRowPublicationField *dst, const jazz_engine_CurrentRowPublicationField *src);
void jazz_engine_PublicationFieldVec_drop(void *v);
void jazz_engine_PublicationFieldVec_clone(void *dst, const void *src);
/* node::physical PhysicalTableMapping.columns: String -> PhysicalColumnId */


/* CurrentRow helpers (node/mod.rs) */
jz_err jazz_node__node__CurrentRow__project(const jazz_node_CurrentRow *row, const jazz_model_TableSchema *table, const jz_str *columns, u32 n, jazz_node_CurrentRow *out);
typedef struct jazz_model_RelationProjectColumn { jz_str alias; void *expr; } jazz_model_RelationProjectColumn;
typedef JZ_VEC(jazz_model_RelationProjectColumn) jazz_model_RelationProjectColumnVec;
void jazz_model_RelationProjectColumnVec_drop(void *v);
void jazz_model_RelationProjectColumnVec_clone(void *dst, const void *src);
jz_err jazz_node__node__CurrentRow__project_relation(const jazz_node_CurrentRow *row, const jazz_model_TableSchema *table, const jazz_model_RelationProjectColumnVec *columns, jazz_node_CurrentRow *out);
jazz_node_CurrentRow jazz_node__node__CurrentRow__into_deleted(jazz_node_CurrentRow row);
const groove_Value *jazz_node__node__CurrentRow__raw_field(const jazz_node_CurrentRow *row, const jz_str *name);
jazz_node_CurrentRow jazz_node__node__CurrentRow__new_with_publication_fields(const jz_str *table, groove_OwnedRecord record, void *publication_fields);
jz_err jazz_node__node__decode_current_row(const jazz_model_TableSchema *table, const groove_BorrowedRecord *record, jazz_node_CurrentRow *out);
u32 jazz_node__node__current_row_fields_len(const jazz_model_TableSchema *table); /* current_row_fields(&table).len() */
bool jazz_node__node__current_row_has_required_subscription_cells(const jazz_node_CurrentRow *row, const jazz_model_TableSchema *table, const jz_str_vec *projection);
int jazz_node__node__default_query_row_order(const jazz_node_CurrentRow *a, const jazz_node_CurrentRow *b);
void jazz_node__node__sort_query_default_rows(jazz_node_CurrentRow *rows, u32 n);
void jazz_node__node__sort_current_rows(jazz_node_CurrentRow *rows, u32 n);
const groove_Value *jazz_node__node__query_order_value(const jazz_node_CurrentRow *row, const jazz_model_TableSchema *table, const jz_str *column, groove_Value *scratch);
void jazz_node__node__apply_query_window(const jazz_model_Query *query, jazz_node_CurrentRowVec *rows);
int jazz_node__node__record_raw_cmp(const jazz_node_CurrentRow *a, const jazz_node_CurrentRow *b); /* record.raw().cmp() */

/* groove ivm deltas */
typedef struct groove_RecordDeltas groove_RecordDeltas;   /* iter() over (Record, weight) */
typedef struct groove_RecordDeltasIter { const groove_RecordDeltas *d; u32 i; } groove_RecordDeltasIter;
bool groove__ivm__RecordDeltas__next(groove_RecordDeltasIter *it, groove_BorrowedRecord *record, i64 *weight);
const groove_RecordDescriptor *groove__ivm__RecordDeltas__descriptor(const groove_RecordDeltas *d);
typedef struct groove_MultisinkDeltas groove_MultisinkDeltas;
const groove_RecordDeltas *groove__ivm__MultisinkDeltas__get(const groove_MultisinkDeltas *d, const char *sink);
jz_err groove__records__record__BorrowedRecord__get_bool(const groove_BorrowedRecord *r, u32 idx, bool *out);
jz_err groove__records__record__BorrowedRecord__get_str(const groove_BorrowedRecord *r, u32 idx, jz_slice *out);
jz_err groove__records__record__BorrowedRecord__get_uuid(const groove_BorrowedRecord *r, u32 idx, jz_uuid *out);
jz_err groove__records__record__BorrowedRecord__get_u64(const groove_BorrowedRecord *r, u32 idx, u64 *out);
extern const char *const jazz_engine__query_engine__JAZZ_APP_ROWS_SINK;
typedef struct groove_TerminalOperation { void *repr; } groove_TerminalOperation;
typedef JZ_VEC(groove_TerminalOperation) groove_TerminalOperationVec;
void groove_TerminalOperation_clone(groove_TerminalOperation *dst, const groove_TerminalOperation *src);
void groove_TerminalOperation_drop(void *t);
typedef struct groove_MultisinkSubscription groove_MultisinkSubscription;
typedef struct groove_MultisinkBatch { u32 sinks_len; u32 terminal_sinks_len; void *repr; } groove_MultisinkBatch;
enum { GROOVE_TRY_RECV_OK = 0, GROOVE_TRY_RECV_EMPTY = 1, GROOVE_TRY_RECV_DISCONNECTED = 2 };
u32 groove__ivm__MultisinkSubscription__try_recv(groove_MultisinkSubscription *s, groove_MultisinkBatch *out);
u64 groove__ivm__MultisinkSubscription__id(const groove_MultisinkSubscription *s);
void groove_MultisinkSubscription_drop(groove_MultisinkSubscription *s);
typedef struct groove_InputSourceMetrics { u64 tick, records_processed, notifications_sent, notification_records; } groove_InputSourceMetrics;
jz_err groove__db__query__Database__apply_input_source_deltas_detaching_cold(groove_Database *db, groove_InputSourceDelta *deltas, u32 n, groove_InputSourceMetrics *m);
jz_err groove__db__query__Database__apply_input_source_deltas_metrics(groove_Database *db, groove_InputSourceDelta *deltas, u32 n, groove_InputSourceMetrics *m);
jz_err groove__db__query__Database__replace_input_sources_detaching_cold(groove_Database *db, groove_InputSourceReplacement *r, u32 n, groove_InputSourceMetrics *m);
jz_err groove__db__query__Database__replace_input_sources_metrics(groove_Database *db, groove_InputSourceReplacement *r, u32 n, groove_InputSourceMetrics *m);
jz_err jazz_node__node__Error_groove(jz_err e); /* Error::Groove */

/* node::maintained_subscription_view (other unit) */
typedef struct jazz_node_MaintainedSubscriptionView {
    bool targeted_refresh_uncertain;
    jz_btree targeted_refresh_tables;   /* BTreeSet<String> */
    bool has_compiled_authorization_mode; jazz_engine_QueryAuthorizationMode compiled_authorization_mode;
    void *rest;
} jazz_node_MaintainedSubscriptionView;
typedef struct jazz_node_TerminalRootLayout { bool root_union_arm; void *rest; } jazz_node_TerminalRootLayout;
typedef struct jazz_node_MaintainedTerminalSchemas jazz_node_MaintainedTerminalSchemas;
typedef struct jazz_node_ResultMembershipSchema jazz_node_ResultMembershipSchema;
typedef struct jazz_node_ResultTransitions {
    bool authoritative_membership_changed;
    jz_btree authoritative_member_adds;     /* BTreeSet<ResultMemberEntry> */
    jazz_protocol_ResultMemberEntryVec adds, removes;
    jazz_protocol_MemberPayloadPairVec result_payload_adds;
    jazz_protocol_ResultMemberEntryVec result_payload_removes;
    jazz_protocol_ProgramFactEntryVec program_fact_adds, program_fact_removes;
    bool supporting_changed;
    groove_TerminalOperationVec terminal_operations;
    usize observed_result_delta_batches;
    void *rest;
} jazz_node_ResultTransitions;
void jazz_node_ResultTransitions_default(jazz_node_ResultTransitions *t);
void jazz_node_ResultTransitions_clone(jazz_node_ResultTransitions *dst, const jazz_node_ResultTransitions *src);
void jazz_node_ResultTransitions_drop(void *t);
typedef JZ_VEC(jazz_node_VersionRow) jazz_node_VersionRowVec;
void jazz_node_VersionRowVec_drop(void *v);
bool jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__uses_storage_backed_result_materialization(const jazz_node_MaintainedSubscriptionView *m);
jazz_node_VersionRowVec jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__versions_by_tx(const jazz_node_MaintainedSubscriptionView *m, jazz_model_TxId tx);
bool jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__replacement_for(const jazz_node_MaintainedSubscriptionView *m, const jz_str *table, jazz_types_RowUuid row, jazz_node_VersionRow *content_winner);
const jz_btree *jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__decoded_terminal_records(const jazz_node_MaintainedSubscriptionView *m); /* terminal key -> TerminalRecordState */
jz_err jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__apply_multisink_deltas(jazz_node_MaintainedSubscriptionView *m, groove_MultisinkBatch deltas, const jazz_node_MaintainedTerminalSchemas *schemas, const jz_btree *tables, const void *node_aliases, jazz_node_ResultTransitions *out);
typedef struct { jz_bytes terminal_key; groove_OwnedRecord record; } jazz_node_TerminalKeyRecord;
typedef JZ_VEC(jazz_node_TerminalKeyRecord) jazz_node_TerminalKeyRecordVec;
jz_err jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__structured_app_rows_by_terminal_key(const jazz_node_MaintainedSubscriptionView *m, jazz_node_TerminalKeyRecordVec *out);
void jazz_node_MaintainedSubscriptionView_drop(jazz_node_MaintainedSubscriptionView *m);
const jazz_node_TerminalRootLayout *jazz_node__node__MaintainedTerminalSchemas__terminal_root_layout(const jazz_node_MaintainedTerminalSchemas *s);
bool jazz_node__node__MaintainedTerminalSchemas__has_root_collector(const jazz_node_MaintainedTerminalSchemas *s);
jz_err jazz_node__node__MaintainedTerminalSchemas__direct_app_row_schema(const jazz_node_MaintainedTerminalSchemas *s, const jazz_engine_AppRowSchema **out);
jz_err jazz_node__node__MaintainedTerminalSchemas__aggregate_app_row_schema(const jazz_node_MaintainedTerminalSchemas *s, const jazz_engine_AppRowSchema **out);
jz_err jazz_node__node__MaintainedTerminalSchemas__current_payload_schema(const jazz_node_MaintainedTerminalSchemas *s, const jazz_node_ResultMembershipSchema **out);
void jazz_node_MaintainedTerminalSchemas_drop(jazz_node_MaintainedTerminalSchemas *s);
jz_err jazz_node__node__terminal_root__terminal_root_occurrence_id_with_root_union(const jz_bytes *key, bool root_union_arm, jazz_types_OutputOccurrenceId *out);
void *jazz_node__node__terminal_root__terminal_root_publication_fields(const jazz_node_TerminalRootLayout *layout); /* Arc<Vec<PublicationField>> */
typedef struct jazz_node_TerminalRecordState jazz_node_TerminalRecordState;
void *jazz_node_TerminalRecordState_clone_box(const void *s);
extern const jz_btree_desc jazz_node_OCCURRENCE_TERMINAL_RECORD_DESC; /* OutputOccurrenceId -> TerminalRecordState* */

/* VersionRow accessors (node::codec) */
jazz_types_SchemaVersionAlias jazz_node__node__codec__VersionRow__schema_version_alias(const jazz_node_VersionRow *v);
const jz_str *jazz_node__node__codec__VersionRow__table(const jazz_node_VersionRow *v);
jazz_types_RowUuid jazz_node__node__codec__VersionRow__row_uuid(const jazz_node_VersionRow *v);
bool jazz_node__node__codec__VersionRow__is_deleted(const jazz_node_VersionRow *v);
jazz_model_BranchKey jazz_node__node__codec__VersionRow__branch_key(const jazz_node_VersionRow *v);
void jazz_node_VersionRow_clone(jazz_node_VersionRow *dst, const jazz_node_VersionRow *src);
void jazz_node_VersionRow_drop(void *v);

/* NodeState methods owned by other units used by materialization/maintained_views */
bool jazz_node__node__NodeState__schema_version_for_alias(const jazz_node_NodeState *self, jazz_types_SchemaVersionAlias alias, jazz_types_SchemaVersionId *out);
jz_err jazz_node__node__lenses__NodeState__translate_cells(jazz_node_NodeState *self, jazz_types_SchemaVersionId from, jazz_types_SchemaVersionId to, const jz_str *table, jz_btree *cells, bool *has, jz_str *out);
jz_err jazz_node__node__views__NodeState__query_version_by_alias(jazz_node_NodeState *self, const jz_str *table, jazz_types_RowUuid row, jazz_types_TxTime time, jazz_types_NodeAlias alias, bool *has, jazz_node_VersionRow *out);
jz_err jazz_node__node__views__NodeState__query_versions_for_tx(jazz_node_NodeState *self, jazz_model_TxId tx, jazz_node_VersionRowVec *out);
typedef struct jazz_node_StoredTransaction jazz_node_StoredTransaction; /* defined with the state mirrors below */
jz_err jazz_node__node__views__NodeState__query_transaction(jazz_node_NodeState *self, jazz_model_TxId tx, bool *has, jazz_node_StoredTransaction *out);
void jazz_node_StoredTransaction_drop(void *t);
typedef struct { jz_str table; jazz_types_RowUuid row; } jazz_node_TableRowKey;
extern const jz_btree_desc jazz_node_TABLE_ROW_SET_DESC; /* BTreeSet<(String, RowUuid)> */
jz_err jazz_node__node__views__NodeState__query_versions_for_tx_rows_by_alias(jazz_node_NodeState *self, jazz_model_TxId tx, jazz_types_NodeAlias alias, const jz_btree *rows, jazz_node_VersionRowVec *out);
bool jazz_node__node__views__NodeState__cached_tx_versions(jazz_node_NodeState *self, jazz_model_TxId tx, jazz_node_VersionRowVec *out);
bool jazz_node__node__NodeAliases__get(const void *aliases, const jazz_types_NodeUuid *node, jazz_types_NodeAlias *out);
bool jazz_node__node__NodeAliases__node_for_alias(const void *aliases, jazz_types_NodeAlias alias, jazz_types_NodeUuid *out);
jz_err jazz_node__node__views__NodeState__materialized_cells_for_version(jazz_node_NodeState *self, const jazz_model_TableSchema *table, const jazz_node_VersionRow *v, jz_btree *cells);
jz_err jazz_node__node__current_row_from_materialized_cells(const jazz_model_TableSchema *table, const jazz_node_VersionRow *v, const jz_btree *cells, jazz_node_CurrentRow *out);
jz_err jazz_node__node__views__NodeState__materialize_current_row(jazz_node_NodeState *self, const jazz_model_TableSchema *table, jazz_node_CurrentRow row, jazz_node_CurrentRow *out);
jz_err jazz_node__node__NodeState__table_in_schema(const jazz_node_NodeState *self, const jz_str *table, jazz_types_SchemaVersionId schema, jazz_model_TableSchema *out);
jz_err jazz_node__node__views__NodeState__maintained_witness_for_result_member(jazz_node_NodeState *self, const jazz_node_VersionRowVec *versions, jazz_types_SchemaVersionId schema, const jz_str *table, jazz_types_RowUuid row, const jazz_node_VersionRow **out);
const jazz_node_VersionRow *jazz_node__node__query_eval__local_maintained_view_content_witness(const jazz_node_VersionRowVec *versions, const jz_str *table, jazz_types_RowUuid row);
jz_err jazz_node__node__views__NodeState__version_tx_id(const jazz_node_NodeState *self, const jazz_node_VersionRow *v, jazz_model_TxId *out);
jz_err jazz_node__node__descriptor_roles__decode_aggregate_payload_record(const jazz_protocol_ResultMemberPayloadEntry *p, const jazz_engine_AggregateResultSchema *schema, groove_OwnedRecord *out);
jz_err jazz_node__node__descriptor_roles__decode_current_payload_record(const jz_str *table, const jazz_protocol_ResultMemberPayloadEntry *p, const jazz_node_ResultMembershipSchema *schema, jazz_node_CurrentRow *out);
jz_err jazz_node__node__query_eval__aggregate_current_row_from_record(const jazz_model_Query *query, const jazz_engine_AppRowSchema *output, jazz_types_RowUuid row_uuid, const groove_BorrowedRecord *record, jazz_node_CurrentRow *out);
jz_err jazz_node__node__query_eval__required_field_idx(const groove_RecordDescriptor *d, const char *name, u32 *out);
jz_err jazz_node__node__query_eval__materialization_app_row_schema(const void *plan, const jazz_engine_QueryProgram *program, jazz_engine_AppRowSchema *out);
bool jazz_node__node__query_eval__is_public_aggregate_result_member(const jazz_node_ResultMemberEntry *m, const jz_str *table, bool aggregate);
bool jazz_node__node__query_eval__is_public_result_member(const jazz_node_ResultMemberEntry *m, const jz_str *table, bool aggregate);
jz_err jazz_node__node__query_eval__public_result_member_occurrence_id(const jazz_node_ResultMemberEntry *m, const jz_str *table, bool aggregate, bool *has, jazz_types_OutputOccurrenceId *out);
jz_err jazz_node__node__query_eval__replace_stale_authoritative_occurrence_member(jz_btree *result_set, jz_btree *result_payloads, const jz_btree *authoritative_member_adds, const jazz_node_ResultMemberEntry *member, const jz_str *table, bool aggregate);
jz_err jazz_node__node__query_eval__NodeState__apply_query_order_with_occurrences(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_node_CurrentRowVec *rows, jazz_types_OutputOccurrenceIdVec *occurrences);
jz_err jazz_node__node__query_eval__NodeState__apply_projection(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_node_CurrentRowVec *rows);
jz_err jazz_node__node__query_eval__read_sources__covered_input_record(const jazz_model_TableSchema *table, const groove_RecordDescriptor *d, const jazz_node_CurrentRow *row, jazz_types_SchemaVersionAlias alias, const jazz_model_BranchKey *branch, jz_bytes *out);
bool jazz_node__node__query_eval__NodeState__client_settled_binding_view_key_for_query(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view, jazz_protocol_BindingViewKey *out);
bool jazz_node__node__NodeState__subscription_has_pending_query_evaluation(const jazz_node_NodeState *self, u64 subscription_id);
bool jazz_node__node__NodeState__detaches_covered_chunk_waits(const jazz_node_NodeState *self);
jz_err jazz_node__node__views__NodeState__materialize_branch_view_winners(jazz_node_NodeState *self, const jz_str *table, jazz_types_SchemaVersionId schema, const jazz_model_BranchKey *head, jz_btree a, jz_btree b, jz_btree versions, jz_btree d, jazz_node_CurrentRowVec *out);
extern const jz_btree_desc jazz_node_ROW_UUID_VERSION_ROW_DESC;
jz_err jazz_model__schema__JazzSchema__project_branch_view_selector(const jazz_model_JazzSchema *s, const jazz_model_TableSchema *table, const void *head, jazz_model_BranchKey *out);
typedef struct jazz_node_AuthoritySourceIncremental {
    u64 predecessor_generation, generation;
    JZ_VEC(jazz_protocol_SupportingRow) adds, removes;
} jazz_node_AuthoritySourceIncremental;
void jazz_node_AuthoritySourceIncremental_drop(void *i);
jz_err jazz_node__node__coalesce_authority_source_incrementals(const void *incrementals, u64 from, u64 to, jazz_node_AuthoritySourceIncremental *out);
jz_err jazz_model__query__relation_output_projection_if_present(const jazz_model_RelationQuery *r, bool *has, jazz_model_RelationProjectColumnVec *out);
jz_err jazz_model__query__relation_union_leaf_projections(const jazz_model_RelationQuery *r, jz_btree *out); /* String -> Vec<RelationProjectColumn> */
bool jazz_model__query__relation_union_presentation_order(const jazz_model_RelationQuery *r);
jz_err jazz_node__node__api_error__from_node_error(jz_err e);
extern const jz_btree_desc jazz_node_STR_RELATION_PROJECTION_DESC;
extern const jz_btree_desc jazz_node_STR_TABLE_SCHEMA_DESC;
extern const jz_btree_desc jazz_node_GPT_COVERED_INPUT_SOURCE_DESC;
extern const jz_btree_desc jazz_node_GPT_INSTALLED_RECORDS_DESC;   /* GPTId -> BTreeMap<SupportingRow, Vec<u8>> */
extern const jz_btree_desc jazz_node_SUPPORTING_ROW_BYTES_DESC;    /* SupportingRow -> Vec<u8> */
extern const jz_btree_desc jazz_node_TX_VERSIONS_DESC;             /* TxId -> Vec<VersionRow> */
extern const jz_btree_desc jazz_node_TX_ID_SET_DESC;
/* node::subscription_open (other unit) */
typedef struct jazz_node_SeededMaintainedOpen {
    groove_MultisinkSubscription *subscription;
    jazz_node_MaintainedSubscriptionView *maintained;
    jazz_node_MaintainedTerminalSchemas *terminal_schemas;
    jazz_node_ResultTransitions transitions;
    jz_btree tables;
    bool initial_received;
    jz_btree covered_input_sources;   /* GPTId -> CoveredInputSource */
} jazz_node_SeededMaintainedOpen;
jz_err jazz_node__node__NodeState__open_seeded_maintained_subscription_view_in_authorization_mode(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_types_AuthorSubject identity, jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view, jazz_protocol_ReadViewKey read_view_key, jazz_engine_QueryAuthorizationMode mode, const jazz_protocol_BindingViewKey *settled_binding_view, const jazz_protocol_AuthorityResultKey *settled_authority_result_key, u32 claim_mode, bool pending_overlay, const void *progress_waker, jazz_node_SeededMaintainedOpen *out);
void jazz_protocol_ReadViewSpec_clone(jazz_protocol_ReadViewSpec *dst, const jazz_protocol_ReadViewSpec *src);
void jazz_protocol_ReadViewSpec_drop(void *s);


/* ===== authorization.rs external surface ===== */
/* node::PolicyProofStackEntry { table, lease: Weak<()> } */
typedef struct jazz_node_PolicyProofStackEntry { jz_str table; void *lease; /* Weak<()> */ } jazz_node_PolicyProofStackEntry;
/* jazz_protocol permission advice */
enum { JZ_PAA_INSERT = 0, JZ_PAA_READ = 1, JZ_PAA_UPDATE = 2, JZ_PAA_DELETE = 3 };
typedef struct jazz_protocol_PermissionAdviceAction {
    u32 tag;
    jz_str table;
    jazz_types_RowUuid row;          /* Read / Update / Delete */
    jz_btree cells;                  /* Insert: String -> Value; Update: patch */
} jazz_protocol_PermissionAdviceAction;
void jazz_protocol_PermissionAdviceAction_pc_encode(jz_pc_w *w, const jazz_protocol_PermissionAdviceAction *a);
typedef enum { JZ_ASO_READ = 0, JZ_ASO_INSERT = 1, JZ_ASO_UPDATE = 2, JZ_ASO_DELETE = 3 } jazz_protocol_AuthorizationScopeOperation;
typedef struct jazz_protocol_AuthorizationSupportScopeKey {
    u8 support_shape_digest[32];
    jazz_types_AuthorSubject subject;
    u8 claims_digest[32];
    u8 policy_digest[32];
} jazz_protocol_AuthorizationSupportScopeKey;
typedef struct jazz_protocol_AuthorizationOperationKey {
    jazz_protocol_AuthorizationScopeOperation operation;
    jz_str table;
    bool has_row; jazz_types_RowUuid row;
    u8 candidate_digest[32];
} jazz_protocol_AuthorizationOperationKey;
void jazz_protocol_RegisterShapeOptions_pc_encode(jz_pc_w *w, const jazz_protocol_RegisterShapeOptions *o);
void jazz_protocol_ClaimMap_pc_encode(jz_pc_w *w, const jz_btree *claims); /* BTreeMap<String, Value> */
/* jazz_model write policies + query builders */
typedef struct jazz_model_WritePolicies {
    jazz_model_Query *insert_check, *update_using, *update_check, *delete_using; /* Option<Query> */
} jazz_model_WritePolicies;
const jazz_model_WritePolicies *jazz_model__schema__TableSchema__write_policies(const jazz_model_TableSchema *t);
jazz_model_Predicate jazz_model__query__builders__eq(jazz_model_Operand left, jazz_model_Operand right);
jazz_model_Operand jazz_model__query__builders__col(const char *name);
jazz_model_Operand jazz_model__query__builders__lit(groove_Value v);
jazz_model_Query jazz_model__query__builders__Query__filter(jazz_model_Query q, jazz_model_Predicate p);
const jazz_model_JazzSchema *jazz_node__node__SchemaCatalogue__schema_for_version(const jazz_node_SchemaCatalogue *c, jazz_types_SchemaVersionId v);
bool jazz_model_TableSchema_eq(const jazz_model_TableSchema *a, const jazz_model_TableSchema *b);
/* engine output */
typedef enum { JZ_PDR_READ = 0, JZ_PDR_WRITE = 1 } jazz_engine_PolicyDecisionRole;
typedef enum { JZ_CQPO_POLICY_PREDICATE = 0, JZ_CQPO_AUTHORIZED_ROWS = 1 } jazz_node_CurrentQueryProgramOutput;
jz_err jazz_node__node__query_eval__lowering__current_query_output_request(jazz_node_CurrentQueryProgramOutput kind, const jazz_model_Query *query, void **out);
/* query_eval::read_sources (unit node-qe-a) */
enum { JZN_CAP_PRIMARY_KEY = 0, JZN_CAP_PRIMARY_KEY_PAGE = 1, JZN_CAP_INDEX = 2 };
typedef struct jazz_node_CurrentAccessPath { u32 tag; groove_ValueVec values; usize cap; jz_str column; void *rest; } jazz_node_CurrentAccessPath;
void jazz_node_CurrentAccessPath_drop(void *p);
extern const jz_btree_desc jazz_node_ACCESS_PATHS_DESC; /* SourceId -> CurrentAccessPath */
typedef struct jazz_node_TransactionWriteOverlay { void *tables; bool has_excluded; void *excluded; void *reads; jazz_model_DurabilityTier evidence_tier; } jazz_node_TransactionWriteOverlay;
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__default(void);
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__accepted_state(void);
jazz_model_DurabilityTier jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__evidence_tier(const jazz_node_TransactionWriteOverlay *o);
bool jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__is_active(const jazz_node_TransactionWriteOverlay *o);
void jazz_node_TransactionWriteOverlay_clone(jazz_node_TransactionWriteOverlay *dst, const jazz_node_TransactionWriteOverlay *src);
void jazz_node_TransactionWriteOverlay_drop(void *o);
/* query_eval::lowering (unit node-qe-a) */
typedef struct jazz_node_PolicyAuthorizationGraph {
    groove_GraphBuilder *graph;
    jz_btree route_fields;   /* BTreeSet<String> */
    jz_btree access_paths;   /* SourceId -> CurrentAccessPath */
} jazz_node_PolicyAuthorizationGraph;
void jazz_node_PolicyAuthorizationGraph_clone(jazz_node_PolicyAuthorizationGraph *dst, const jazz_node_PolicyAuthorizationGraph *src);
void jazz_node_PolicyAuthorizationGraph_drop(void *g);
extern const jz_btree_desc jazz_node_POLICY_AUTHORIZATION_GRAPH_CACHE_DESC; /* PolicyAuthorizationGraphCacheKey -> PolicyAuthorizationGraph */
typedef struct jazz_node_PolicyAuthorizationGraphCacheKey { jz_bytes key; } jazz_node_PolicyAuthorizationGraphCacheKey;
jazz_node_PolicyAuthorizationGraphCacheKey jazz_node__node__query_eval__lowering__policy_authorization_graph_cache_key(const jazz_engine_QueryProgramRequest *r);
bool jazz_node__node__query_eval__lowering__NodeState__policy_authorization_graph_cache_get(jazz_node_NodeState *self, const jazz_node_PolicyAuthorizationGraphCacheKey *k, jazz_node_PolicyAuthorizationGraph *out);
void jazz_node__node__query_eval__lowering__NodeState__restore_expired_policy_compilation_state(jazz_node_NodeState *self);
jz_err jazz_node__node__query_eval__lowering__NodeState__query_program_access_paths(jazz_node_NodeState *self, const jazz_engine_QueryProgramRequest *r, bool claims, jz_btree *out);
jz_err jazz_node__node__query_eval__lowering__NodeState__compile_query_program_request_with_access_paths(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest r, jz_btree access_paths, jazz_engine_QueryProgram **out);
jz_err jazz_node__node__query_eval__lowering__NodeState__compile_query_program_request_with_shared_access_paths(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest r, jz_btree access_paths, bool has_bounded, jazz_engine_SourceId bounded_source, groove_GraphBuilder *bounded_graph, jazz_engine_QueryProgram **out);
jz_err jazz_node__node__query_eval__lowering__NodeState__compile_query_program_request_with_inline_sources_and_access_paths(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest r, jz_btree inline_sources, jz_btree access_paths, jazz_engine_QueryProgram **out);
jz_err jazz_node__node__query_eval__lowering__NodeState__compile_query_program_request_with_inline_sources_and_transaction_overlay(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest r, jz_btree inline_sources, jz_btree access_paths, jazz_node_TransactionWriteOverlay overlay, jazz_engine_QueryProgram **out);
jz_err jazz_node__node__query_eval__lowering__lowered_terminal_graph(const jazz_engine_QueryProgram *p, const char *sink, groove_GraphBuilder **out);
jz_btree jazz_node__node__query_eval__lowering__output_routing_fields_for_query_eval(const void *fact_schema);
jz_err jazz_node__node__query_eval__lowering__NodeState__prepared_query_plan_from_program(jazz_node_NodeState *self, const jazz_engine_QueryProgram *p, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_node_PreparedQueryPlan *out);
jz_err jazz_node__node__query_eval__lowering__NodeState__current_query_primary_key_access_paths(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jz_btree *out);
groove_GraphBuilder *jazz_node__node__query_eval__lowering__empty_authorized_row_id_graph(void);
extern const jz_btree_desc jazz_node_INLINE_SOURCES_DESC; /* SourceId -> Vec<CurrentRow> */
enum { JZ_OTS_FACT = 1 };
/* query_eval (unit node-qe-a) */
jz_err jazz_node__node__query_eval__NodeState__query_rows_with_prepared_plan(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_model_DurabilityTier tier, const void *plan, jazz_node_CurrentRowVec *out);
jz_err jazz_node__node__query_eval__take_required_sink_deltas(void *deltas, const char *sink, groove_RecordDeltas **out);
jz_err jazz_node__node__query_eval__normalization__NodeState__normalized_include_deleted_row_set_shape(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_engine_NormalizedRowSetShape *out);
/* node/mod.rs and friends */
jz_err jazz_node__node__NodeState__require_catalogue_ready(const jazz_node_NodeState *self);
typedef struct jazz_node_OpenTransaction { jazz_model_Snapshot base_snapshot; void *rest; } jazz_node_OpenTransaction;
jz_err jazz_node__node__NodeState__open_tx(const jazz_node_NodeState *self, jazz_model_OpenTransactionId id, const jazz_node_OpenTransaction **out);
jz_err groove__db__query__Database__query_graph(groove_Database *db, groove_GraphBuilder *g, groove_RecordDeltas **out);
void groove_RecordDeltas_drop(groove_RecordDeltas *d);
jz_err jazz_node__node__prepared__NodeState__bind_shape_snapshot(jazz_node_NodeState *self, u64 shape, const groove_ValueVec *values, void **out);
typedef struct jazz_model_RowProvenance { jazz_types_AuthorSubject created_by; u64 created_at; jazz_types_AuthorSubject updated_by; u64 updated_at; } jazz_model_RowProvenance;
jz_err jazz_node__node__current_row_from_cells_with_explicit_provenance(const jazz_model_TableSchema *table, jazz_types_RowUuid row, const jz_btree *cells, jazz_model_RowProvenance provenance, const void *extra, jazz_node_CurrentRow *out);
jz_str_vec jazz_node__node__global_current_storage_fields(const jazz_model_TableSchema *table, bool a, bool b);
groove_PredicateExpr *jazz_node__node__not_deleted_predicate(void);
jz_err jazz_node__node__physical__NodeState__physical_current_source_graph(const jazz_node_NodeState *self, jazz_types_SchemaVersionId schema, const jz_str *table, u32 physical_class, groove_GraphBuilder **out);
/* node::layer_stack */
typedef struct jazz_node_LayerRef jazz_node_LayerRef;
u32 jazz_node__node__layer_stack__LayerRef__physical_class(const jazz_node_LayerRef *r);
typedef struct jazz_node_Layer { groove_GraphBuilder *graph; void *rest; } jazz_node_Layer;
jazz_node_Layer jazz_node__node__layer_stack__Layer__new(groove_GraphBuilder *rows);
typedef JZ_VEC(jazz_node_Layer) jazz_node_LayerVec;
jz_err jazz_node__node__layer_stack__try_ref_layers(jazz_model_DurabilityTier tier, jz_err (*f)(const jazz_node_LayerRef *r, void *ctx, jazz_node_Layer *out), void *ctx, jazz_node_LayerVec *out);
groove_GraphBuilder *jazz_node__node__layer_stack__compose(jazz_node_LayerVec layers);
/* groove graph builders used here */
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__arg_max_by(groove_GraphBuilder *input, const char *const *group, u32 ng, const char *const *keys, u32 nk);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__binding_source(const jz_str *shape, groove_RecordDescriptor *d);
groove_FieldRef groove__ivm__graph__FieldRef__stored_name(jz_str name);
void groove_ProjectField_clone(groove_ProjectField *dst, const groove_ProjectField *src);
void groove_ProjectField_drop(void *f);
jz_str jazz_engine__query_engine__fields__right_field(const jz_str *name);
typedef struct groove_FieldSpecNamed { jz_str name; groove_ColumnType ty; } groove_FieldSpecNamed;
groove_RecordDescriptor *groove__records__descriptor__RecordDescriptor__new_named(const groove_FieldSpecNamed *fields, u32 n);

/* node::ReadPolicyAuthorizationRequestCacheKey (node/mod.rs) */
typedef struct jazz_node_ReadPolicyAuthorizationRequestCacheKey {
    jazz_types_SchemaVersionId policy_schema_version;
    jz_str table_name;
    jazz_types_AuthorSubject identity;
    u32 param_binding_mode;
    jazz_model_DurabilityTier tier;
    bool has_binding_source_shape; jz_str binding_source_shape;
    jz_str binding_user_params, binding_claim_params, active_session_claims;
    bool include_deleted_root;
} jazz_node_ReadPolicyAuthorizationRequestCacheKey;
void jazz_node_ReadPolicyAuthorizationRequestCacheKey_drop(void *k);


/* ======================= node/state mirrors (node-qe-b, state/ files) ======================= */
#define JZN_TABLE_NOT_FOUND(name) jz_err_fmt(JZN_E_TableNotFound, "table not found: %.*s", (int)(name)->len, (name)->ptr)
#define JZN_INVALID_BRANCH_KEY(e) jz_err_context((e), "invalid branch key")
jz_err jazz_node__node__Error_invalid_branch_key(jz_err model_error); /* map_err(Error::InvalidBranchKey) */
jz_err jazz_node__node__Error_invalid_branch_key_str(const char *msg);
#define JZN_LARGE_VALUE_INGRESS_RATE_LIMITED() jz_err_new(JZN_E_LargeValueIngressRateLimited, "large-value upload rate limit exceeded")
#define JZN_LARGE_VALUE_STAGE_EXPIRED() jz_err_new(JZN_E_LargeValueStageExpired, "large-value staging root expired; upload again")
#define JZN_CATALOGUE_UNINITIALIZED() jz_err_new(JZN_E_CatalogueUninitialized, "catalogue bootstrap is not ready")
#define JZN_CATALOGUE_ACTIVATION_FAILED() jz_err_new(JZN_E_CatalogueActivationFailed, "catalogue activation failed; reopen required")
#define JZN_UNADMITTED_WRITE_AUTHOR() jz_err_new(JZN_E_UnadmittedWriteAuthor, "durable writes require an admitted account author")
jz_err jazz_node__node__Error_groove(jz_err groove_error);            /* Error::Groove (transparent) */
jz_err jazz_node__node__Error_query(jz_err query_error);              /* Error::Query(Box<..>) */

/* jazz_types ids */
typedef jz_uuid jazz_types_MigrationLensId;
typedef jz_uuid jazz_types_SchemaLineagePublicationId;
jazz_types_AuthorSubject jazz_types__ids__AuthorSubject__system_at(jazz_types_NodeUuid node);
extern const jazz_types_AuthorSubject jazz_types_AuthorSubject_SYSTEM;
typedef struct jazz_types_RowAuthor { u32 kind; const void *author; } jazz_types_RowAuthor;
jz_err jazz_types__ids__RowAuthor__from_session(jazz_types_AuthorSubject s, jazz_types_NodeUuid node, jazz_types_RowAuthor *out);
jz_err jazz_types__ids__RowAuthor__from_record(const groove_OwnedRecord *r, jazz_types_RowAuthor *out);
jazz_types_AuthorSubject jazz_types__ids__RowAuthor__as_author_subject(const jazz_types_RowAuthor *a);
jz_err jazz_types__time__TxTime__tick(jazz_types_TxTime prev, u64 now_ms, jazz_types_TxTime *out);
jz_err jazz_types__time__TxTime__from_physical_ms(u64 ms, jazz_types_TxTime *out);
u64 jazz_types__time__TxTime__sort_key(jazz_types_TxTime t, jazz_types_NodeUuid node); /* (u128 truncated) */

/* jazz_model tx */
typedef enum { JZ_TXKIND_MERGEABLE = 0, JZ_TXKIND_EXCLUSIVE } jazz_model_TxKind;
typedef struct jazz_model_Fate { u32 tag; void *rejection; } jazz_model_Fate; /* JZ_FATE_* */
void jazz_model_Fate_clone(jazz_model_Fate *dst, const jazz_model_Fate *src);
void jazz_model_Fate_drop(jazz_model_Fate *f);
enum { JZ_DELETION_DELETED = 0, JZ_DELETION_RESTORED = 1 };
jz_err jazz_model__tx__deletion_event_from_value(groove_Value v, u8 *out);
typedef struct jazz_model_BranchSelector { jz_btree values; /* String -> Value */ } jazz_model_BranchSelector;
void jazz_model_BranchSelector_clone(jazz_model_BranchSelector *dst, const jazz_model_BranchSelector *src);
void jazz_model_BranchSelector_drop(void *b);
bool jazz_model_BranchSelector_eq(const jazz_model_BranchSelector *a, const jazz_model_BranchSelector *b);
void jazz_model_BranchSelector_default(jazz_model_BranchSelector *b);
void jazz_model_BranchKey_default(jazz_model_BranchKey *b);
int jazz_model_BranchKey_cmp(const void *a, const void *b, void *ctx);
bool jazz_model_BranchKey_eq(const jazz_model_BranchKey *a, const jazz_model_BranchKey *b);
enum { JZ_BVCB_CURRENT = 0, JZ_BVCB_SNAPSHOT = 1 };
typedef struct jazz_model_BranchViewCopyBase { u32 tag; jazz_model_BranchKey branch; void *snapshot; } jazz_model_BranchViewCopyBase;
typedef struct jazz_model_BranchViewCopyEvidence {
    u8 version; jazz_model_BranchKey head; jazz_model_BranchViewCopyBase base;
    jz_str table; jazz_types_RowUuid row_uuid; jazz_model_TxId source_version;
} jazz_model_BranchViewCopyEvidence;
enum { JZ_BWO_EXACT_HEAD_INSERT = 0, JZ_BWO_EXACT_HEAD_UPDATE = 1, JZ_BWO_VIEW_UPDATE_COPY = 2 };
typedef struct jazz_model_BranchWriteOperation { u32 tag; jazz_model_BranchViewCopyEvidence evidence; } jazz_model_BranchWriteOperation;
typedef struct jazz_model_BranchWriteIntent {
    u8 version;
    jazz_types_PhysicalTableId physical_table_id;
    jazz_types_SchemaVersionId authored_schema;
    jazz_types_RowUuid row_uuid;
    jazz_model_BranchKey head;
    jazz_model_BranchWriteOperation operation;
} jazz_model_BranchWriteIntent;
typedef JZ_VEC(jazz_model_BranchWriteIntent) jazz_model_BranchWriteIntentVec;
typedef struct jazz_model_ContributionMergeProvenance {
    jazz_model_BranchKey source, target;
    JZ_VEC(void) substitutions;
    JZ_VEC(jazz_model_BranchViewCopyEvidence) branch_view_copies;
    jazz_model_BranchWriteIntentVec branch_write_intents;
} jazz_model_ContributionMergeProvenance;
jz_err jazz_model__tx__ContributionMergeProvenance__validate(const jazz_model_ContributionMergeProvenance *p);
void jazz_model_ContributionMergeProvenance_drop(jazz_model_ContributionMergeProvenance *p);
typedef struct jazz_model_Transaction {
    jazz_model_TxId tx_id;
    jazz_model_TxKind kind;
    u32 n_total_writes;
    jazz_types_AuthorSubject made_by;
    bool has_permission_subject; jazz_types_AuthorSubject permission_subject;
    bool has_base_snapshot; jazz_model_Snapshot base_snapshot;
    void *row_read_set, *absent_read_set, *predicate_read_set; /* Option<Vec<..>>, NULL = None */
    bool has_user_metadata_json; jz_str user_metadata_json;
    bool has_contribution_merge; jazz_model_ContributionMergeProvenance contribution_merge;
} jazz_model_Transaction;
void jazz_model_Transaction_clone(jazz_model_Transaction *dst, const jazz_model_Transaction *src);
void jazz_model_Transaction_drop(void *t);
jazz_model_Transaction jazz_node__node__transaction_without_permission_subject(const jazz_model_Transaction *t);

/* jazz_model schema helpers used by state/ */
jz_err jazz_model__schema__JazzSchema__validate_authored_branch_key(const jazz_model_JazzSchema *s, const jazz_model_TableSchema *t, const jazz_model_BranchKey *k);
jz_err jazz_model__schema__JazzSchema__project_branch_selector(const jazz_model_JazzSchema *s, const jazz_model_TableSchema *t, const jazz_model_BranchSelector *sel, jazz_model_BranchKey *key_out, jz_btree *cells_out);
jazz_types_SchemaVersionId jazz_model__schema__JazzSchema__version_id(const jazz_model_JazzSchema *s);
jazz_model_JazzSchema jazz_model__schema__JazzSchema__without_permissions(const jazz_model_JazzSchema *s);
jazz_model_JazzSchema jazz_model__schema__JazzSchema__empty(void);
bool jazz_model__schema__JazzSchema__has_same_lineage_structure(const jazz_model_JazzSchema *a, const jazz_model_JazzSchema *b);
bool jazz_model_JazzSchema_eq(const jazz_model_JazzSchema *a, const jazz_model_JazzSchema *b);
typedef struct groove_DbSchema { JZ_VEC(void) tables; } groove_DbSchema; /* groove::db::Schema */
groove_DbSchema jazz_model__schema__JazzSchema__lower_to_groove(const jazz_model_JazzSchema *s);
groove_DbSchema jazz_model__schema__JazzSchema__lower_catalogue_meta_to_groove(const jazz_model_JazzSchema *s);
groove_Value jazz_model__schema__ColumnSchema__storage_value(const jazz_model_ColumnSchema *c, groove_Value v);
bool groove__records__ValueType__as_enum_tag(const groove_ColumnType *t, u32 *variant_count);
bool groove__records__ValueType__as_enum(const groove_ColumnType *t, u32 *case_count);
enum { JZ_LVSK_NOT_LARGE = 0, JZ_LVSK_STRING, JZ_LVSK_BYTES, JZ_LVSK_JSON };
extern const char *const jazz_model__schema__DELETION_COLUMN_NAME;

/* protocol SchemaVersion / lenses / lineage */
void jazz_protocol_SchemaVersion_clone(jazz_protocol_SchemaVersion *dst, const jazz_protocol_SchemaVersion *src);
void jazz_protocol_SchemaVersion_drop(void *s);
jazz_protocol_SchemaVersion jazz_protocol__protocol__SchemaVersion__new(jazz_model_JazzSchema schema);
int jazz_protocol_SchemaVersion_id_cmp(const void *a, const void *b, void *ctx);
enum { JZ_LENSOP_RENAME_TABLE = 0, JZ_LENSOP_RENAME_COLUMN, JZ_LENSOP_COPY_COLUMN, JZ_LENSOP_ADD_COLUMN,
       JZ_LENSOP_DROP_COLUMN, JZ_LENSOP_TRANSFORM_COLUMN, JZ_LENSOP_REJECT_SOURCE_DELTA };
typedef struct jazz_protocol_LensOp { u32 tag; jz_str from, to; jz_str column; groove_Value value; jz_str text; } jazz_protocol_LensOp;
typedef struct jazz_protocol_TableLens { jz_str source_table, target_table; JZ_VEC(jazz_protocol_LensOp) ops; } jazz_protocol_TableLens;
typedef struct jazz_protocol_MigrationLens {
    jazz_types_MigrationLensId id;
    jazz_types_SchemaVersionId source, target;
    JZ_VEC(jazz_protocol_TableLens) table_lenses;
} jazz_protocol_MigrationLens;
void jazz_protocol_MigrationLens_clone(jazz_protocol_MigrationLens *dst, const jazz_protocol_MigrationLens *src);
void jazz_protocol_MigrationLens_drop(void *l);
jazz_types_MigrationLensId jazz_protocol__protocol__MigrationLens__content_id(const jazz_protocol_MigrationLens *l);
typedef struct jazz_protocol_SchemaPredecessor { jazz_protocol_MigrationLens lens; jz_str_vec new_tables, dropped_tables; } jazz_protocol_SchemaPredecessor;
typedef JZ_VEC(jazz_protocol_SchemaPredecessor) jazz_protocol_SchemaPredecessorVec;
typedef struct jazz_protocol_SchemaLineagePublication {
    jazz_types_SchemaLineagePublicationId id;
    jazz_protocol_SchemaVersion schema;
    jazz_protocol_SchemaPredecessorVec predecessors;
    jazz_protocol_PhysicalIdentityManifest physical_identities;
} jazz_protocol_SchemaLineagePublication;
void jazz_protocol_SchemaLineagePublication_clone(jazz_protocol_SchemaLineagePublication *dst, const jazz_protocol_SchemaLineagePublication *src);
void jazz_protocol_SchemaLineagePublication_drop(void *p);
jazz_types_SchemaLineagePublicationId jazz_protocol__protocol__SchemaLineagePublication__content_id(const jazz_protocol_SchemaLineagePublication *p);
/* author_from_predecessors / validate_predecessor_identities return Err(&'static str) -> InvalidCatalogueUpdate */
const char *jazz_protocol__protocol__SchemaLineagePublication__author_from_predecessors(jazz_protocol_SchemaVersion schema, jazz_protocol_SchemaPredecessorVec predecessors,
    const jz_btree *sources /* SchemaVersionId -> (JazzSchema, PhysicalIdentityManifest) */, jazz_protocol_PhysicalIdentityManifestVec history, jazz_protocol_SchemaLineagePublication *out);
const char *jazz_protocol__protocol__SchemaLineagePublication__validate_predecessor_identities(const jazz_protocol_SchemaLineagePublication *p, const jz_btree *sources, jazz_protocol_PhysicalIdentityManifestVec history);
jazz_protocol_PhysicalIdentityManifest jazz_protocol__protocol__PhysicalIdentityManifest__allocate(const jazz_model_JazzSchema *s);
jz_err jazz_protocol__protocol__PhysicalIdentityManifest__validate_for_schema(const jazz_protocol_PhysicalIdentityManifest *m, const jazz_model_JazzSchema *s);
typedef struct jazz_protocol_CurrentWriteSchema { u64 revision; jazz_types_SchemaVersionId schema; } jazz_protocol_CurrentWriteSchema;
typedef jazz_protocol_CurrentWriteSchema jazz_node_CurrentWriteSchema;
typedef struct jazz_protocol_CatalogueSnapshot {
    jazz_protocol_PhysicalIdentityManifest genesis_physical_identities;
    JZ_VEC(jazz_protocol_SchemaVersion) schemas;
    JZ_VEC(void) lineages; /* (u64, SchemaLineagePublication) */
    jazz_protocol_CurrentWriteSchema current_write_schema;
} jazz_protocol_CatalogueSnapshot;
typedef struct jazz_protocol_CatalogueLineage { u64 catalogue_seq; jazz_protocol_SchemaLineagePublication publication; } jazz_protocol_CatalogueLineage;

/* node catalogue lineage records (node/mod.rs) */
typedef struct jazz_node_StagedSchemaLineage {
    u64 catalogue_seq;
    jazz_protocol_SchemaLineagePublication publication;
    jazz_types_SchemaVersionAlias alias;
    jazz_node_SchemaPhysicalMapping mapping;
} jazz_node_StagedSchemaLineage;
void jazz_node_StagedSchemaLineage_clone(jazz_node_StagedSchemaLineage *dst, const jazz_node_StagedSchemaLineage *src);
void jazz_node_StagedSchemaLineage_drop(void *s);
typedef struct jazz_node_PendingSchemaLineage { u64 catalogue_seq; jazz_protocol_SchemaLineagePublication publication; } jazz_node_PendingSchemaLineage;
void jazz_node_PendingSchemaLineage_drop(void *s);
typedef struct jazz_node_SchemaLineageActivation { jazz_types_SchemaLineagePublicationId id; u64 catalogue_seq; } jazz_node_SchemaLineageActivation;
typedef struct jazz_node_CatalogueBootstrapReady { jazz_types_SchemaVersionId genesis; jazz_protocol_CurrentWriteSchema current_write_schema; u64 active_catalogue_seq; } jazz_node_CatalogueBootstrapReady;
typedef struct jazz_node_CatalogueOpenState {
    void *storage; /* BoxedStorage */
    jz_btree schemas, lenses, schema_version_aliases, physical_mappings;
    jz_btree staged_lineages, pending_lineages, active_lineages_by_target;
    u64 active_catalogue_seq, next_physical_table_id, next_physical_column_id;
    bool catalogue_bootstrap_marker;
    jazz_node_ActiveSchema active_schema;
} jazz_node_CatalogueOpenState;
typedef struct jazz_node_LensPathCacheKey { jazz_types_SchemaVersionId source, target; } jazz_node_LensPathCacheKey;
typedef struct jazz_node_CompiledLensCacheKey { jazz_types_SchemaVersionId source, target; jz_str table; } jazz_node_CompiledLensCacheKey;
enum { JZN_LENS_FORWARD = 0, JZN_LENS_REVERSE = 1 };
typedef struct jazz_node_LensStep { jazz_types_MigrationLensId lens; u32 direction; } jazz_node_LensStep;
typedef JZ_VEC(jazz_node_LensStep) jazz_node_LensStepVec;
typedef struct jazz_node_LensPathCacheEntry { bool has; jazz_node_LensStepVec path; } jazz_node_LensPathCacheEntry; /* Option<Vec<..>> */
typedef struct jazz_node_CompiledLensOp { u32 tag; jz_str from, to; groove_Value default_; } jazz_node_CompiledLensOp;
typedef JZ_VEC(jazz_node_CompiledLensOp) jazz_node_CompiledLensOpVec;
typedef struct jazz_node_CompiledLensPath { jz_str target_table; jazz_node_CompiledLensOpVec ops; } jazz_node_CompiledLensPath;
typedef struct jazz_node_CompiledLensCacheEntry { bool has; jazz_node_CompiledLensPath path; } jazz_node_CompiledLensCacheEntry;
void jazz_node_CompiledLensPath_clone(jazz_node_CompiledLensPath *dst, const jazz_node_CompiledLensPath *src);
void jazz_node_CompiledLensPath_drop(void *p);
jz_err jazz_node__node__push_compiled_forward_lens_op(const jazz_protocol_LensOp *op, jazz_node_CompiledLensOpVec *ops);
jz_err jazz_node__node__push_compiled_reverse_lens_op(const jazz_protocol_LensOp *op, jazz_node_CompiledLensOpVec *ops);
jazz_protocol_CurrentWriteSchema jazz_node__node__ActiveSchema__wire_pointer(const jazz_node_ActiveSchema *a);
jz_err jazz_node__node__ActiveSchema__new(jazz_protocol_CurrentWriteSchema pointer, jazz_model_JazzSchema compiled, jazz_node_ActiveSchema *out);
bool jazz_node_ActiveSchema_eq(const jazz_node_ActiveSchema *a, const jazz_node_ActiveSchema *b);
void jazz_node_ActiveSchema_drop(jazz_node_ActiveSchema *a);
bool jazz_node__node__ActiveSchema__same_authorization_source(const jazz_node_ActiveSchema *a, const jazz_node_ActiveSchema *b, const jazz_node_SchemaPhysicalMapping *ma, const jazz_node_SchemaPhysicalMapping *mb);
u64 jazz_node__node__next_groove_runtime_token(void);

/* descs for catalogue maps (other units own the comparators) */
extern const jz_btree_desc jazz_node_SCHEMA_VERSIONS_DESC;        /* SchemaVersionId -> SchemaVersion */
extern const jz_btree_desc jazz_node_LENSES_DESC;                 /* MigrationLensId -> MigrationLens */
extern const jz_btree_desc jazz_node_SCHEMA_ALIASES_DESC;         /* SchemaVersionId -> SchemaVersionAlias */
extern const jz_btree_desc jazz_node_PHYSICAL_MAPPINGS_DESC;      /* SchemaVersionId -> SchemaPhysicalMapping */
extern const jz_btree_desc jazz_node_STAGED_LINEAGES_DESC;        /* u64 -> StagedSchemaLineage */
extern const jz_btree_desc jazz_node_PENDING_LINEAGES_DESC;       /* u64 -> PendingSchemaLineage */
extern const jz_btree_desc jazz_node_ACTIVE_LINEAGES_DESC;        /* SchemaVersionId -> StagedSchemaLineage */
extern const jz_btree_desc jazz_node_LENS_PATH_CACHE_DESC;        /* LensPathCacheKey -> LensPathCacheEntry */
extern const jz_btree_desc jazz_node_COMPILED_LENS_CACHE_DESC;    /* CompiledLensCacheKey -> CompiledLensCacheEntry */
extern const jz_btree_desc jazz_node_PHYSICAL_TABLE_MAPPINGS_DESC;/* String -> PhysicalTableMapping */
extern const jz_btree_desc jazz_node_COLUMN_IDS_DESC;             /* String -> PhysicalColumnId */
extern const jz_btree_desc jazz_node_COLUMN_CASES_DESC;           /* PhysicalColumnId -> Vec<Uuid> */
extern const jz_btree_desc jazz_node_NESTED_COLUMN_CASES_DESC;    /* PhysicalColumnId -> BTreeMap<String, Vec<Uuid>> */
extern const jz_btree_desc jazz_node_TX_ID_SET_DESC;              /* BTreeSet<TxId> */
extern const jz_btree_desc jazz_node_UUID_SET_DESC;               /* BTreeSet<Uuid> */
extern const jz_btree_desc jazz_node_U64_SET_DESC;                /* BTreeSet<u64> */
extern const jz_btree_desc jazz_node_STRING_SET_DESC;             /* BTreeSet<String> */
extern const jz_btree_desc jazz_node_STRING_VALUE_MAP_DESC;       /* String -> Value */


/* --- groove raw rows / records (primary_storage) used by state/ --- */
typedef struct groove_DatabaseBatch groove_DatabaseBatch;
typedef struct groove_RawRow groove_RawRow; /* owning */
typedef JZ_VEC(groove_RawRow *) groove_RawRowVec;
const groove_OwnedRecord *groove__db__RawRow__record(const groove_RawRow *r);
jz_slice groove__db__RawRow__key(const groove_RawRow *r);
void groove__db__RawRow__drop(groove_RawRow *r);
jz_err groove__db__primary_storage__Database__primary_key_scan_raw(groove_Database *db, const jz_str *table, const groove_Value *prefix, u32 n, groove_RawRowVec *out);
jz_err groove__db__primary_storage__Database__primary_key_get_raw(groove_Database *db, const jz_str *table, const groove_Value *key, u32 n, groove_RawRow **out);
jz_err groove__db__primary_storage__Database__primary_key_get_raw_in_batch(groove_Database *db, const groove_DatabaseBatch *b, const jz_str *table, const groove_Value *key, u32 n, groove_RawRow **out);
jz_err groove__db__primary_storage__Database__index_scan_raw(groove_Database *db, const jz_str *table, const char *index, const groove_Value *key, u32 n, groove_RawRowVec *out);
jz_err groove__records__OwnedRecord__get_u64(const groove_OwnedRecord *r, u32 idx, u64 *out);
jz_err groove__records__OwnedRecord__get_uuid(const groove_OwnedRecord *r, u32 idx, jz_uuid *out);
jz_err groove__records__OwnedRecord__get_enum(const groove_OwnedRecord *r, u32 idx, u8 *out);
jz_err groove__records__OwnedRecord__get_bytes(const groove_OwnedRecord *r, u32 idx, jz_slice *out);
jz_err groove__records__OwnedRecord__get_record(const groove_OwnedRecord *r, u32 idx, groove_OwnedRecord *out);
jz_err groove__records__OwnedRecord__get_idx(const groove_OwnedRecord *r, u32 idx, groove_Value *out);
jz_err groove__records__BorrowedRecord__get_nullable_enum(const groove_BorrowedRecord *r, u32 idx, bool *some, u8 *tag);
static inline void groove_RawRowVec_drop(groove_RawRowVec *v) { for (u32 i = 0; i < v->len; ++i) groove__db__RawRow__drop(v->ptr[i]); jz_free(v->ptr); v->ptr = NULL; v->len = v->cap = 0; }
/* record field indices (node::codec generated *RowRecord::FIELD_*_IDX) */
enum {
    JZN_NODE_ALIAS_ROW_FIELD_ID_IDX = 0, JZN_NODE_ALIAS_ROW_FIELD_UUID_IDX = 1,
    JZN_TX_ROW_FIELD_TIME_IDX = 0, JZN_TX_ROW_FIELD_NODE_ID_IDX = 1, JZN_TX_ROW_FIELD_MADE_BY_IDX = 4,
    JZN_TX_ROW_FIELD_FATE_IDX = 7, JZN_TX_ROW_FIELD_DURABILITY_IDX = 9,
    JZN_REJECTED_VERSION_ROW_FIELD_TX_TIME_IDX = 0, JZN_REJECTED_VERSION_ROW_FIELD_TX_NODE_ID_IDX = 1,
    JZN_CATALOGUE_ROW_FIELD_KIND_IDX = 0, JZN_CATALOGUE_ROW_FIELD_ID_IDX = 1, JZN_CATALOGUE_ROW_FIELD_PAYLOAD_IDX = 2,
    JZN_SCHEMA_VERSION_ALIAS_ROW_FIELD_ID_IDX = 0, JZN_SCHEMA_VERSION_ALIAS_ROW_FIELD_UUID_IDX = 1, JZN_SCHEMA_VERSION_ALIAS_ROW_FIELD_PHYSICAL_MAPPING_IDX = 2,
    JZN_HISTORY_ROW_FIELD_TX_TIME_IDX = 2, JZN_HISTORY_ROW_FIELD_TX_NODE_ID_IDX = 3,
    JZN_GLOBAL_CURRENT_ROW_FIELD_TX_TIME_IDX = 2, JZN_GLOBAL_CURRENT_ROW_FIELD_TX_NODE_ID_IDX = 3,
};
jz_err jazz_node__node__Error_record(jz_err record_error);        /* Error::Record (transparent) */
jz_err jazz_node__node__Error_non_canonical_record(void);         /* groove::records::Error::NonCanonicalRecord */
void jazz_node__node__NodeAliases__insert(void *aliases, jazz_types_NodeUuid node, jazz_types_NodeAlias alias);
u64 jazz_node__node__NodeAliases__max_alias(const void *aliases);
bool jazz_node__node__NodeAliases__contains_key(const void *aliases, const jazz_types_NodeUuid *node);
jazz_types_NodeAlias jazz_node__node__codec__VersionRow__tx_node_alias(const jazz_node_VersionRow *v);
jazz_types_TxTime jazz_node__node__codec__VersionRow__tx_time(const jazz_node_VersionRow *v);

/* --- protocol VersionRecord / VersionBundle / RowVersionRef / SyncMessage --- */
typedef struct jazz_protocol_VersionRecord jazz_protocol_VersionRecord; /* boxed */
typedef JZ_VEC(jazz_protocol_VersionRecord *) jazz_protocol_VersionRecordVec;
void jazz_protocol_VersionRecord_drop(jazz_protocol_VersionRecord *v);
jazz_protocol_VersionRecordVec jazz_protocol_VersionRecordVec_clone(const jazz_protocol_VersionRecordVec *v);
void jazz_protocol_VersionRecordVec_drop(jazz_protocol_VersionRecordVec *v);
jazz_types_SchemaVersionId jazz_protocol__protocol__VersionRecord__schema_version(const jazz_protocol_VersionRecord *v);
const jz_str *jazz_protocol__protocol__VersionRecord__table(const jazz_protocol_VersionRecord *v);
jazz_types_RowUuid jazz_protocol__protocol__VersionRecord__row_uuid(const jazz_protocol_VersionRecord *v);
jazz_model_BranchKey jazz_protocol__protocol__VersionRecord__branch_key(const jazz_protocol_VersionRecord *v);
jz_err jazz_protocol__protocol__VersionRecord__from_stored(const jazz_node_VersionRow *v, const jazz_model_TableSchema *table, jazz_types_SchemaVersionId schema, const jz_btree *authored_columns, jazz_protocol_VersionRecord **out);
jz_err jazz_protocol__protocol__validate_version_records(const jazz_protocol_VersionRecordVec *v);
enum { JZ_VBS_VIEW_SCOPED = 0, JZ_VBS_COMPLETE_TRANSACTION = 1 };
typedef struct jazz_protocol_VersionBundle {
    jazz_model_Transaction tx;
    jazz_protocol_VersionRecordVec versions;
    u32 scope;
    jazz_model_Fate fate;
    bool has_global_time; jazz_types_GlobalTime global_time;
    jazz_model_DurabilityTier durability;
} jazz_protocol_VersionBundle;
typedef JZ_VEC(jazz_protocol_VersionBundle) jazz_protocol_VersionBundleVec;
void jazz_protocol_VersionBundle_drop(jazz_protocol_VersionBundle *b);
typedef struct jazz_protocol_RowVersionRef { const jz_str *table; jazz_types_RowUuid row_uuid; jazz_types_TxTime tx_time; jazz_types_NodeUuid tx_node_id; } jazz_protocol_RowVersionRef;
typedef JZ_VEC(jazz_protocol_RowVersionRef) jazz_protocol_RowVersionRefVec;
int jazz_protocol_RowVersionRef_cmp(const void *a, const void *b, void *ctx);
jazz_protocol_RowVersionRef jazz_protocol__protocol__RowVersionRef__new(const jz_str *table, jazz_types_RowUuid row, jazz_model_TxId tx);
jazz_model_TxId jazz_protocol__protocol__RowVersionRef__tx_id(const jazz_protocol_RowVersionRef *r);
typedef struct jazz_protocol_SyncMessage jazz_protocol_SyncMessage;
enum { JZ_SYNC_COMMIT_UNIT = 0, JZ_SYNC_VIEW_UPDATE = 1, JZ_SYNC_OTHER = 0xff };
u32 jazz_protocol__protocol__SyncMessage__tag(const jazz_protocol_SyncMessage *m);
typedef struct jazz_protocol_ProgramFacts jazz_protocol_ProgramFacts;
typedef JZ_VEC(const jazz_protocol_SupportingRow *) jazz_protocol_SupportingRowVec;
jazz_protocol_SupportingRowVec jazz_protocol__protocol__ProgramFacts__added_rows(const jazz_protocol_ProgramFacts *f);
typedef struct jazz_protocol_ViewUpdatePayload {
    jazz_protocol_SubscriptionKey subscription;
    void *version_carriers;                      /* Vec<VersionCarrier> */
    const jazz_protocol_ProgramFacts *supporting_rows; /* boxed ProgramFacts */
    struct { bool opening_pending; } peer_payload_inventory;
} jazz_protocol_ViewUpdatePayload;
const jazz_protocol_ViewUpdatePayload *jazz_protocol__protocol__SyncMessage__view_update(const jazz_protocol_SyncMessage *m);
jz_err jazz_protocol__protocol__expand_version_carriers(const void *carriers, jazz_protocol_VersionBundleVec *out);
jazz_protocol_SyncMessage *jazz_protocol__protocol__SyncMessage__commit_unit(jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions);
extern const jz_btree_desc jazz_node_UUID_PTR_MAP_DESC; /* Uuid -> const T* (borrowed) */

/* --- other units' NodeState methods used by state/ --- */
bool jazz_node__node__query_eval__NodeState__registered_shape(const jazz_node_NodeState *self, jazz_model_ShapeId id, jazz_model_ValidatedQuery *out);
jz_err jazz_node__node__currency__NodeState__transaction_exists(const jazz_node_NodeState *self, jazz_model_TxId tx, bool *out);
jz_err jazz_node__node__physical__NodeState__physical_table_id_for_schema(jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jz_str *table, jazz_types_PhysicalTableId *out);
jz_err jazz_node__node__physical__NodeState__authored_columns_for_version(jazz_node_NodeState *self, const jazz_node_VersionRow *v, bool *some, jz_btree *out);
jz_err jazz_node__node__physical__NodeState__contribution_merge_storage_value(const jazz_node_NodeState *self, const jazz_model_ContributionMergeProvenance *p, groove_Value *out);
jz_err jazz_node__node__ingest__validate_received_view_bundle_global_time_durability(bool has_gt, jazz_types_GlobalTime gt, jazz_model_DurabilityTier d);
jz_err jazz_node__node__ingest__NodeState__validate_view_payload_versions(const jazz_node_NodeState *self, const jazz_protocol_VersionRecordVec *versions);
jz_err jazz_node__node__ingest__NodeState__ingest_known_transaction(jazz_node_NodeState *self, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions,
    jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt, u32 durability);
jz_err jazz_node__node__ingest__NodeState__ingest_view_scoped_transaction_with_current_indexes(jazz_node_NodeState *self, jazz_model_Transaction tx,
    jazz_protocol_VersionRecordVec versions, jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt, u32 durability);


/* --- durable.rs dependencies --- */
enum { JZN_CBS_UNINITIALIZED = 0, JZN_CBS_READY = 1 }; /* CatalogueBootstrapState */
typedef JZ_VEC(jazz_model_TxId) jazz_model_TxIdVec;
typedef struct jz_waker jz_waker;                 /* std::task::Waker */
typedef u64 groove_SubscriptionId;
typedef struct groove_CommitMetrics groove_CommitMetrics;
typedef struct groove_TickMetrics groove_TickMetrics;
typedef struct groove_StorageReadMetrics { u64 reads, bytes, scans; } groove_StorageReadMetrics;
typedef struct jazz_node_HistoricalRead { jazz_node_NodeState *node; jazz_types_GlobalTime position; } jazz_node_HistoricalRead;
typedef struct jazz_node_PendingTransactionScan { jazz_model_TxIdVec tx_ids; u64 records_visited, full_transactions_decoded; } jazz_node_PendingTransactionScan;
typedef struct jazz_node_TransactionRecord jazz_node_TransactionRecord; /* returned by value; opaque body */
struct jazz_node_TransactionRecord { jazz_model_TxId tx_id; void *body; };
typedef struct jazz_node_HistoryEntry jazz_node_HistoryEntry;
typedef JZ_VEC(jazz_node_HistoryEntry *) jazz_node_HistoryEntryVec;
jazz_model_TxId jazz_node__node__HistoryEntry__tx_id(const jazz_node_HistoryEntry *e);
void jazz_node__node__HistoryEntry__drop(jazz_node_HistoryEntry *e);
void jazz_types__time__TxTime__sort_key128(jazz_types_TxTime t, jazz_types_NodeUuid node, u64 *hi, u64 *lo);
typedef struct jazz_model_RejectedTransaction jazz_model_RejectedTransaction;
jazz_model_RejectedTransaction *jazz_model__tx__RejectedTransaction__clone(const jazz_model_RejectedTransaction *t);
void jazz_model__tx__RejectedTransaction__drop(jazz_model_RejectedTransaction *t);
struct jazz_node_StoredTransaction {
    jazz_model_Transaction tx;
    jazz_types_NodeAlias node_alias;
    jazz_model_Fate fate;
    bool has_global_time; jazz_types_GlobalTime global_time;
    u32 durability;
    bool is_partial;
};
jazz_node_TransactionRecord jazz_node__node__codec__StoredTransaction__to_record(const jazz_node_StoredTransaction *t);
jz_err jazz_node__node__codec__durability_from_discriminant(u8 d, jazz_model_DurabilityTier *out);
typedef struct jazz_node_PrimaryKeyValue { u32 tag; u64 u64_; jz_uuid uuid; JZ_VEC(struct jazz_node_PrimaryKeyValue) parts; } jazz_node_PrimaryKeyValue;
jazz_node_PrimaryKeyValue jazz_node__node__codec__rejected_transaction_primary_key(jazz_types_NodeAlias alias, jazz_model_TxId tx);
jz_err jazz_node__node__codec__rejected_version_primary_key_from_record(const groove_OwnedRecord *r, jazz_node_PrimaryKeyValue *out);
jazz_node_HistoryEntry *jazz_node__node__codec__VersionRow__to_history_entry(const jazz_node_VersionRow *v, const jazz_node_StoredTransaction *tx, bool local_current, bool global_current);
void jazz_node__node__codec__VersionRow__free(jazz_node_VersionRow *v); /* drop a boxed Option<VersionRow> */
jz_str jazz_node__node__physical__physical_rejected_versions_table_name(jazz_types_PhysicalTableId id);
void jazz_node__node__physical__NodeState__physical_table_ids(const jazz_node_NodeState *self, jz_btree *out); /* BTreeSet<PhysicalTableId> */
jz_err jazz_node__node__currency__NodeState__query_transaction(jazz_node_NodeState *self, jazz_model_TxId tx, bool *found, jazz_node_StoredTransaction *out);
jz_err jazz_node__node__currency__NodeState__query_transaction_state(jazz_node_NodeState *self, jazz_model_TxId tx_id, bool *found, jazz_model_Fate *fate, bool *has_global_time, jazz_types_GlobalTime *global_time, jazz_model_DurabilityTier *durability);
jz_err jazz_node__node__currency__NodeState__query_row_versions(jazz_node_NodeState *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_node_VersionRowVec *out);
jz_err jazz_node__node__currency__NodeState__query_versions_for_tx(jazz_node_NodeState *self, jazz_model_TxId tx, jazz_node_VersionRowVec *out);
jz_err jazz_node__node__currency__NodeState__query_local_winner_in_branch(jazz_node_NodeState *self, const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_global_winner_in_branch(jazz_node_NodeState *self, const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_local_view_winner_in_branch(jazz_node_NodeState *self, const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out);
void jazz_node__node__QueryEngineReadMetrics__reset_default(void *metrics);
jz_err jazz_node__node__query_eval__NodeState__resolve_time_travel_position(jazz_node_NodeState *self, jazz_types_TxTime time, jazz_types_GlobalTime *out);
jz_err jazz_node__node__query_eval__NodeState__query_rows_with_prepared_plan_for_identity(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *q,
    const jazz_model_Binding *b, u32 durability, const void *opts, jazz_types_AuthorSubject identity, jazz_node_CurrentRowVec *out);
jz_err jazz_node__node__query_eval__NodeState__query_rows_for_client(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_node_CurrentRowVec *out);
jazz_model_Query jazz_model__query__Query__from_table(const jz_str *table);
jz_err jazz_model__query__Query__validate(const jazz_model_Query *q, const jazz_model_JazzSchema *schema, jazz_model_ValidatedQuery *out);
jz_err jazz_model__query__ValidatedQuery__bind(const jazz_model_ValidatedQuery *q, jz_btree params, jazz_model_Binding *out);
jz_err jazz_node__node__CurrentRow__provenance(const jazz_node_CurrentRow *r, bool *has, jazz_model_RowProvenance *out);
bool jazz_node__node__CurrentRow__projected_tx_alias(const jazz_node_CurrentRow *r, jazz_types_TxTime *time, jazz_types_NodeAlias *alias);
void jazz_node_AuthorityResultState_drop_fields(jazz_node_AuthorityResultState *s);
jz_err jazz_protocol__protocol__PolicyBindingKey__directory_value(const jazz_protocol_PolicyBindingKey *k, groove_Value *out);
jz_err jazz_protocol__protocol__PolicyBindingKey__from_directory_value(jazz_types_AuthorSubject identity, groove_Value value, jazz_protocol_PolicyBindingKey *out);
/* groove Database facade */
jz_err groove__db__facade__Database__ensure_usable(const groove_Database *db);
jz_err groove__db__facade__Database__close(groove_Database *db);
jz_err groove__db__commit__Database__flush(groove_Database *db);
jz_err groove__db__facade__Database__set_write_flush_cadence(groove_Database *db, u32 every);
jz_err groove__db__facade__Database__flush_write_boundary(groove_Database *db);
jz_err groove__db__facade__Database__wait_for_publication_settlement(const groove_Database *db, const jz_waker *w, bool *out);
groove_DatabaseBatch *groove__db__facade__Database__open_batch(groove_Database *db);
void groove__db__batch__DatabaseBatch__delete(groove_DatabaseBatch *b, const jz_str *table, jazz_node_PrimaryKeyValue key);
void groove__db__batch__DatabaseBatch__insert(groove_DatabaseBatch *b, const jz_str *table, groove_ValueVec values);
void groove__db__batch__DatabaseBatch__update(groove_DatabaseBatch *b, const jz_str *table, groove_ValueVec values);
void groove__db__batch__DatabaseBatch__drop(groove_DatabaseBatch *b);
bool groove__db__query__Database__unsubscribe(groove_Database *db, groove_SubscriptionId id);
jz_err groove__db__query__Database__drive_progress(groove_Database *db);
jz_err groove__db__query__Database__drive_ready_progress_with_waker(groove_Database *db, const jz_waker *w);
bool groove__db__query__Database__has_pending_progress(const groove_Database *db);
bool groove__db__query__Database__subscription_has_pending_progress(const groove_Database *db, groove_SubscriptionId id);
bool groove__db__query__Database__subscription_has_pending_evaluation(const groove_Database *db, groove_SubscriptionId id);
const groove_CommitMetrics *groove__db__commit__Database__last_commit_metrics(const groove_Database *db);
const groove_TickMetrics *groove__db__commit__Database__last_tick_metrics(const groove_Database *db);
groove_StorageReadMetrics groove__db__commit__Database__storage_read_metrics(const groove_Database *db);
void groove__db__commit__Database__reset_storage_read_metrics(const groove_Database *db);
groove_StorageReadMetrics groove__db__commit__Database__take_storage_read_metrics(const groove_Database *db);
groove_Database *jazz_node__node__database_slot__DatabaseSlot__into_inner(groove_Database *slot);

/* ---- state/catalogue.rs support ---- */
/* groove::db commit pipeline (opaque here) */
typedef struct groove_AppliedBatch groove_AppliedBatch;
typedef struct groove_PersistedBatch groove_PersistedBatch;
groove_PersistedBatch *groove__db__commit__Database__persist_with_progress(groove_Database *db, const groove_AppliedBatch *applied);
jz_err groove__db__commit__Database__finish_persistence(groove_Database *db, groove_PersistedBatch *persisted);
void groove__db__commit__AppliedBatch__drop(groove_AppliedBatch *b);
/* (JazzSchema, PhysicalIdentityManifest) value of predecessor_sources */
typedef struct jazz_node_PredecessorSource { jazz_model_JazzSchema schema; jazz_protocol_PhysicalIdentityManifest identities; } jazz_node_PredecessorSource;
void jazz_node_PredecessorSource_drop(void *p);
extern const jz_btree_desc jazz_node_PREDECESSOR_SOURCES_DESC; /* SchemaVersionId -> PredecessorSource */
/* codec */
enum {
    JZN_CRK_GENESIS = 0, JZN_CRK_SCHEMA, JZN_CRK_LENS, JZN_CRK_SCHEMA_LINEAGE_STAGED, JZN_CRK_SCHEMA_LINEAGE_PENDING,
    JZN_CRK_SCHEMA_LINEAGE_ACTIVE, JZN_CRK_WRITE_POINTER_PENDING, JZN_CRK_BOOTSTRAP_READY, JZN_CRK_ACTIVE_SCHEMA
};
u64 jazz_node__node__codec__CatalogueRecordKind__key(u32 kind);
jz_err jazz_node__node__codec__encode_physical_mapping(const jazz_node_SchemaPhysicalMapping *m, jz_bytes *out);
jz_err jazz_node__node__codec__encode_catalogue_schema(const jazz_protocol_SchemaVersion *s, jz_bytes *out);
void jazz_node__node__codec__encode_catalogue_bootstrap_ready(const jazz_node_CatalogueBootstrapReady *r, jz_bytes *out);
void jazz_node__node__codec__encode_catalogue_lineage_activation(jazz_node_SchemaLineageActivation a, jz_bytes *out);
void jazz_node__node__codec__encode_catalogue_lens(const jazz_protocol_MigrationLens *l, jz_bytes *out);
jz_err jazz_node__node__codec__encode_catalogue_staged_lineage(const jazz_node_StagedSchemaLineage *s, jz_bytes *out);
jz_err jazz_node__node__codec__encode_catalogue_pending_lineage(const jazz_node_PendingSchemaLineage *p, jz_bytes *out);
jz_err jazz_node__node__codec__encode_active_schema(const jazz_node_ActiveSchema *a, jz_bytes *out);
enum { JZN_PKV_U64 = 0, JZN_PKV_UUID, JZN_PKV_COMPOSITE };
/* node/mod.rs free helpers */
jz_err jazz_node__node__install_enum_case_ids(jazz_node_GlobalScalarEnumCaseIdVec *slot, const jz_uuid_vec *ids, jazz_types_SchemaVersionId schema);
jz_err jazz_node__node__install_payload_enum_case_ids(jazz_node_GlobalEnumCaseIdVec *slot, const jz_uuid_vec *ids, jazz_types_SchemaVersionId schema);
jz_err jazz_node__node__hydrate_nested_scalar_enum_cases(const groove_ColumnType *t, const jz_btree *enum_variants,
    jazz_types_SchemaVersionId schema, const char *identity_path, const char *path, jz_btree *out);
jz_err jazz_node__node__hydrate_nested_payload_enum_cases(const groove_ColumnType *t, const jz_btree *enum_variants,
    jazz_types_SchemaVersionId schema, const char *identity_path, const char *path, jz_btree *out);
/* node::physical bindings */
jz_err jazz_node__node__physical__allocate_provisional_physical_mapping(const jazz_model_JazzSchema *schema,
    jazz_protocol_PhysicalIdentityManifest identities, u64 *next_table_id, u64 *next_column_id, jazz_node_SchemaPhysicalMapping *out);
jz_err jazz_node__node__physical__allocate_physical_variant_cases(jz_btree *mappings, const jz_btree *aliases,
    jazz_types_SchemaVersionId schema_version, const jz_str *logical_table, jz_btree fields, jazz_node_PhysicalVariantCaseVec *out);
jz_err jazz_node__node__physical__validate_payload_enum_case_provenance(const jz_btree *mappings, const jz_btree *aliases);
jz_err jazz_node__node__physical__validate_scalar_enum_case_provenance(const jz_btree *mappings, const jz_btree *aliases);
/* node::ingest migration validation */
jz_err jazz_node__node__ingest__NodeState__validate_migration_lens_between(const jazz_protocol_MigrationLens *lens,
    const jazz_protocol_SchemaVersion *source, const jazz_protocol_SchemaVersion *target);
jz_err jazz_node__node__ingest__NodeState__validate_lineage_table_partition(const jazz_model_JazzSchema *source, const jazz_model_JazzSchema *target,
    const jazz_protocol_MigrationLens *lens, const jz_str_vec *new_tables, const jz_str_vec *dropped_tables);
/* jazz_model::schema */
bool jazz_model__schema__physical_column_epoch_is_compatible(const jazz_model_TableSchema *source_table, jz_slice source_column_name,
    const jazz_model_TableSchema *target_table, jz_slice target_column_name);
void jazz_node__node__QueryServing__clear_supported_query_program_requests(jazz_node_QueryServing *q);

/* ---- state/commit.rs support ---- */
/* groove::large_values */
typedef u64 groove_StagedLargeValueId;
typedef JZ_VEC(groove_StagedLargeValueId) groove_StagedLargeValueIdVec;
typedef struct groove_LargeValueRef groove_LargeValueRef;           /* opaque, boxed */
typedef JZ_VEC(groove_LargeValueRef *) groove_LargeValueRefVec;
groove_LargeValueRef *groove_LargeValueRef_clone(const groove_LargeValueRef *r);
bool groove_LargeValueRef_eq(const groove_LargeValueRef *a, const groove_LargeValueRef *b);
void groove_LargeValueRefVec_drop(groove_LargeValueRefVec *v);
typedef struct groove_StagedLargeValue { groove_StagedLargeValueId id; groove_LargeValueRef *value_ref; } groove_StagedLargeValue;
enum { GROOVE_LVK_STRING = 0, GROOVE_LVK_BYTES = 1, GROOVE_LVK_JSON = 2 };
#define GROOVE_INLINE_VALUE_MAX_BYTES (64u * 1024u)
jz_err groove__db__large_values__Database__prepare_and_stage_large_value(groove_Database *db, u32 kind, jz_slice bytes, groove_StagedLargeValue *out);
enum { JZ_LVSK_NOT_LARGE = 0, JZ_LVSK_BYTES, JZ_LVSK_STRING, JZ_LVSK_JSON }; /* jazz_model::schema::LargeValueSemanticKind */
static inline groove_Value gv_large(groove_LargeValueRef *r) { groove_Value v; v.tag = GV_LARGE; v.v.large = r; return v; }
static inline groove_Value gv_nullable(groove_Value *inner) { groove_Value v; v.tag = GV_NULLABLE; v.v.nullable = inner; return v; }

/* node::MergeableCommit (node/commit.rs) */
typedef u8 jazz_model_DeletionEvent; /* JZ_DELETION_* */
typedef struct jazz_node_MergeableCommit {
    jz_str table;
    jazz_types_RowUuid row_uuid;
    jazz_model_BranchSelector branch;
    jazz_types_AuthorSubject made_by;
    bool has_permission_subject; jazz_types_AuthorSubject permission_subject;
    u64 now_ms;
    jz_btree cells;                                     /* String -> Value */
    bool has_authored_columns; jz_btree authored_columns; /* BTreeSet<String> */
    bool has_deletion; jazz_model_DeletionEvent deletion;
    bool has_user_metadata_json; jz_str user_metadata_json;
    jz_btree prepared_large_columns;                    /* BTreeSet<String> */
    groove_StagedLargeValueIdVec staged_large_values;
    bool known_fresh_row;
} jazz_node_MergeableCommit;
jz_err jazz_node__node__MergeableCommit__validate(const jazz_node_MergeableCommit *c);
jazz_types_AuthorSubject jazz_node__node__MergeableCommit__effective_permission_subject(const jazz_node_MergeableCommit *c);
void jazz_node__node__MergeableCommit__drop(void *c);
typedef JZ_VEC(jazz_node_MergeableCommit) jazz_node_MergeableCommitVec;
typedef struct jazz_node_SchemaCommit { jazz_types_SchemaVersionId schema; jazz_node_MergeableCommit commit; } jazz_node_SchemaCommit;
typedef JZ_VEC(jazz_node_SchemaCommit) jazz_node_SchemaCommitVec;
bool jazz_node__node__value_contains_indirect_descriptor(const groove_Value *v);
void jazz_node__node__collect_indirect_descriptors(const groove_Value *v, groove_LargeValueRefVec *out);
#define JZN_DELETION_COLUMN_NAME "_deletion"
#define JZN_TX_TOUCHED_ROWS_TABLE "jazz_tx_touched_rows"
#define JZN_TX_VERSION_TABLE_CACHE_MAX_ENTRIES 4096u

/* node::PublishedTransaction / PublicationOutcome<T> (node/mod.rs) */
typedef struct jazz_node_PublishedTransaction { jazz_model_TxId tx_id; groove_AppliedBatch *persistence; } jazz_node_PublishedTransaction;
typedef JZ_VEC(jazz_node_PublishedTransaction) jazz_node_PublishedTransactionVec;
typedef struct jazz_node_PublicationOutcome {
    void *value;                                   /* T (generic, erased) */
    jazz_node_PublishedTransactionVec publications;
    jz_deque post_settlement_work;                 /* VecDeque<SyncMessage*> */
} jazz_node_PublicationOutcome;
jz_err jazz_node__node__ingest__catalogue__NodeState__apply_sync_message(jazz_node_NodeState *self, jazz_protocol_SyncMessage *message, jazz_node_PublicationOutcome *out);

/* jazz_protocol::RowBase */
typedef struct jazz_protocol_RowBase { bool has_pending; jazz_model_TxId pending; bool has_seq; jazz_types_GlobalTime seq; } jazz_protocol_RowBase;
/* node::codec::VersionRowParts */
typedef struct jazz_node_VersionRowParts {
    jz_str table;
    jazz_model_BranchKey branch_key;
    jazz_types_RowUuid row_uuid;
    jazz_types_NodeAlias tx_node_alias;
    jazz_types_SchemaVersionAlias schema_version_alias;
    jazz_types_TxTime tx_time;
    jazz_types_AuthorSubject created_by;
    jazz_types_TxTime created_at;
    jazz_types_AuthorSubject updated_by;
    jazz_types_TxTime updated_at;
    jz_btree cells;                    /* String -> Value */
    bool has_authored_columns; jz_btree authored_columns; /* BTreeSet<PhysicalColumnId> */
    bool has_deletion; jazz_model_DeletionEvent deletion;
    jazz_types_GlobalTime seq;
    jazz_protocol_RowBase base;
    jz_bytes counter_signs;
} jazz_node_VersionRowParts;
jz_err jazz_node__node__codec__VersionRow__from_parts_with_schema_version(const jazz_model_TableSchema *t, jazz_node_VersionRowParts parts,
    bool has_schema_version, jazz_types_SchemaVersionId schema_version, const groove_RecordDescriptor *history_descriptor /* NULL = None */, jazz_node_VersionRow *out);
jz_err jazz_node__node__codec__VersionRow__cells(const jazz_node_VersionRow *v, const jazz_model_TableSchema *t, jz_btree *out);
bool jazz_node__node__codec__VersionRow__deletion(const jazz_node_VersionRow *v, jazz_model_DeletionEvent *out);
jazz_types_AuthorSubject jazz_node__node__codec__VersionRow__created_by(const jazz_node_VersionRow *v);
jazz_types_TxTime jazz_node__node__codec__VersionRow__created_at(const jazz_node_VersionRow *v);
jz_err jazz_node__node__codec__current_row_from_version_projection(const jazz_model_TableSchema *t, const jazz_node_VersionRow *v, jazz_node_CurrentRow *out);
jz_err jazz_node__node__codec__transaction_values(jazz_types_NodeAlias alias, const jazz_model_Transaction *tx, const jazz_model_Fate *fate,
    bool has_global_time, jazz_types_GlobalTime global_time, jazz_model_DurabilityTier durability, groove_Value contribution_merge, groove_ValueVec *out);
jz_bytes jazz_model__branch__BranchKey__canonical_bytes(const jazz_model_BranchKey *k);
jz_err jazz_types__time__TxTime__from_physical_ms(u64 ms, jazz_types_TxTime *out);
/* jazz_model BranchWriteIntent ordering (physical_table_id, authored_schema, row_uuid, head) */
int jazz_model_BranchWriteIntent_sort_cmp(const void *a, const void *b, void *ctx);

/* node::codec::TouchedRows */
typedef struct jazz_node_TouchedRows { jz_btree map; /* (PhysicalTableId, Vec<u8>) -> BTreeSet<RowUuid> */ } jazz_node_TouchedRows;
void jazz_node__node__codec__TouchedRows__default(jazz_node_TouchedRows *t);
void jazz_node__node__codec__TouchedRows__insert(jazz_node_TouchedRows *t, jazz_types_PhysicalTableId table, jz_bytes branch_key, jazz_types_RowUuid row);
void jazz_node__node__codec__TouchedRows__extend(jazz_node_TouchedRows *t, const jazz_node_TouchedRows *other);
bool jazz_node__node__codec__TouchedRows__eq(const jazz_node_TouchedRows *a, const jazz_node_TouchedRows *b);
void jazz_node__node__codec__TouchedRows__clone(jazz_node_TouchedRows *dst, const jazz_node_TouchedRows *src);
void jazz_node__node__codec__TouchedRows__drop(void *t);
jz_err jazz_node__node__codec__TouchedRows__from_list_record(const groove_BorrowedRecord *r, jazz_node_TouchedRows *out);
jz_err jazz_node__node__codec__TouchedRows__list_record_values(const jazz_node_TouchedRows *t, jazz_types_TxTime tx_time, jazz_types_NodeAlias alias, groove_ValueVec *out);
typedef struct jazz_node_TxTouchedKey { jazz_types_TxTime tx_time; jazz_types_NodeAlias node_alias; } jazz_node_TxTouchedKey;
extern const jz_btree_desc jazz_node_TX_TOUCHED_DIRTY_DESC; /* (TxTime, NodeAlias) -> TouchedRows */
/* node::AheadCurrentKeys (NodeState.ahead_current_keys) */
void jazz_node__node__AheadCurrentKeys__clear(void *keys);
void jazz_node__node__AheadCurrentKeys__insert(void *keys, jazz_types_PhysicalTableId table, jz_bytes key, jazz_model_TxId tx);
/* node::CachedTransactionVersions */
typedef struct jazz_node_CachedTransactionVersions { jazz_node_VersionRowVec versions; } jazz_node_CachedTransactionVersions;
jazz_node_CachedTransactionVersions jazz_node__node__CachedTransactionVersions__new(jazz_node_VersionRowVec versions);
void jazz_node_CachedTransactionVersions_drop(void *c);

/* groove batch / storage / records */
typedef struct groove_InternStr { const jz_str *s; } groove_InternStr;
typedef struct groove_ValidatedVariantRecord groove_ValidatedVariantRecord;
struct groove_ValidatedVariantRecord { void *repr; };
void groove__db__batch__DatabaseBatch__accept_large_value(groove_DatabaseBatch *b, groove_StagedLargeValueId id);
void groove__db__batch__DatabaseBatch__insert_raw(groove_DatabaseBatch *b, const jz_str *table, jazz_node_PrimaryKeyValue key, groove_ValidatedVariantRecord record);
jz_err groove__db__commit__Database__apply_batch(groove_Database *db, groove_DatabaseBatch *b, groove_AppliedBatch **out);
bool groove__records__descriptor__RecordDescriptor__field_index(const groove_RecordDescriptor *d, const char *name, u32 *out);
/* GrooveDbError::RecordEncoding(e) | IvmRuntime(RecordEncoding(e)) -> Some(e) */
bool groove__db__Error__record_encoding_source(const jz_err *e, jz_err *source);
jz_err groove__db__Error__record_encoding(jz_err source); /* GrooveDbError::RecordEncoding */
jz_err jazz_node__node__Error_malformed_current_row(const jz_str *table, jazz_types_RowUuid row_uuid, jz_err source);
/* groove::ivm::StaticScanSpec */
enum { GROOVE_SSS_FULL = 0, GROOVE_SSS_PREFIX = 1, GROOVE_SSS_POINT = 2 };
typedef struct groove_StaticScanSpec { u32 tag; groove_ValueVec values; /* LiteralValue::from(Value) */ } groove_StaticScanSpec;
bool jazz_node__node__layer_stack__LayerRef__is_local(const jazz_node_LayerRef *r); /* Ref::Local vs Ref::Remote(Origin) */

/* physical / ingest / merge_ops / lifecycle callees of commit.rs */
typedef enum { JZN_PWT_HISTORY = 0, JZN_PWT_GLOBAL_CURRENT = 1, JZN_PWT_AHEAD_CURRENT = 2 } jazz_node_PhysicalWriteTarget;
typedef struct jazz_node_PreparedPhysicalWritePlan jazz_node_PreparedPhysicalWritePlan;
const groove_RecordDescriptor *jazz_node__node__physical__PreparedPhysicalWritePlan__logical_descriptor(const jazz_node_PreparedPhysicalWritePlan *p);
jz_err jazz_node__node__physical__NodeState__prepared_physical_write_plan(jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *table, jazz_node_PhysicalWriteTarget target, jazz_node_PreparedPhysicalWritePlan **out);
jz_err jazz_node__node__physical__NodeState__authored_column_ids_for_names(const jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *table, const jz_btree *columns, bool *some, jz_btree *out);
jz_err jazz_node__node__physical__NodeState__version_storage_write_binding(jazz_node_NodeState *s, const jazz_node_VersionRow *v,
    jazz_types_AuthorSubject tx_author, groove_InternStr *table_out, groove_ValidatedVariantRecord *record_out);
jz_err jazz_node__node__physical__NodeState__version_storage_primary_key(const jazz_node_NodeState *s, const jazz_node_VersionRow *v, jazz_node_PrimaryKeyValue *out);
jz_err jazz_node__node__physical__NodeState__physical_current_source_scan_graph(const jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *table, u32 cls, groove_StaticScanSpec scan, groove_GraphBuilder **out);
jz_str jazz_node__node__physical__physical_ahead_current_table_name(jazz_types_PhysicalTableId id);
jz_err jazz_node__node__ingest__NodeState__write_ahead_current_insert(jazz_node_NodeState *s, groove_DatabaseBatch *batch, const jazz_node_VersionRow *v);
jz_err jazz_node__node__ingest__NodeState__newest_own_pending_write_in_batch(jazz_node_NodeState *s, const groove_DatabaseBatch *batch, const jz_str *table,
    const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row, bool *some, jazz_model_TxId *out);
jz_err jazz_node__node__ingest__NodeState__global_current_seq_in_batch(jazz_node_NodeState *s, const groove_DatabaseBatch *batch, jazz_types_SchemaVersionId v,
    const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row, bool *has, jazz_types_GlobalTime *out);
jz_err jazz_node__node__merge_ops__split_merge_ops(const jazz_model_TableSchema *table, const jz_btree *authored_columns, const jz_btree *base_cells,
    jz_btree *cells, bool *has_image, jz_btree *image_cells, jz_bytes *counter_signs);
jz_err jazz_node__node__source_resolution__NodeState__translate_cells(jazz_node_NodeState *self, jazz_types_SchemaVersionId source, jazz_types_SchemaVersionId target,
    const jz_str *table, jz_btree *cells, bool *some, jz_str *out_table);
bool jazz_node__node__CurrentRow__cell(const jazz_node_CurrentRow *row, const jazz_model_TableSchema *table, const jz_str *name, groove_Value *out);
bool jazz_node__node__NodeAliases__node_for_alias(const void *aliases, jazz_types_NodeAlias alias, jazz_types_NodeUuid *out);

void groove_LargeValueRef_drop(groove_LargeValueRef *r);
void jazz_node__node__codec__StoredTransaction__drop(jazz_node_StoredTransaction *t);
void jazz_protocol_SyncMessage_free(jazz_protocol_SyncMessage *m);
void jazz_protocol_SyncMessageVec_free_boxed(void *v); /* Box<Vec<SyncMessage>> as PublicationOutcome<Vec<SyncMessage>>::value */

#endif
