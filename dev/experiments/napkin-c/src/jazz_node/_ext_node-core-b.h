/* _ext_node-core-b.h: mirror declarations of types/functions owned by OTHER
 * units (groove, jazz_types, jazz_model, jazz_protocol, jazz_engine and the
 * other jazz_node units: codec, physical, query_eval, state, ingest,
 * maintained_subscription_view). Layouts are approximate. */
#ifndef JZ_EXT_NODE_CORE_B_H
#define JZ_EXT_NODE_CORE_B_H
#include "jz_rt.h"
/* named vec types (anonymous JZ_VEC structs are not assignment-compatible across declarations) */
#define JZ_NVEC(name, T) typedef struct name##_s { T *ptr; u32 len; u32 cap; } name
JZ_NVEC(jzn_strvec, jz_str);
JZ_NVEC(jzn_u64vec, u64);
JZ_NVEC(jzn_uuidvec, jz_uuid);
JZ_NVEC(jzn_ptrvec, void *);
struct jazz_node_NodeState;
struct jazz_node_CurrentRow; struct jazz_node_CompiledLensPath;
struct jazz_node_CurrentRowVec_;
struct jazz_node_NodeAliases;
struct jazz_node_ClientRelayScope;

/* ======================= jazz_types ======================= */
typedef u64 jazz_types_TxTime;
typedef u64 jazz_types_GlobalTime;
typedef jz_uuid jazz_types_NodeUuid;
typedef u64 jazz_types_NodeAlias;
typedef u64 jazz_types_SchemaVersionAlias;
typedef u64 jazz_types_PhysicalTableId;
typedef u64 jazz_types_PhysicalColumnId;
typedef jz_uuid jazz_types_GlobalPhysicalTableId;
typedef jz_uuid jazz_types_GlobalPhysicalColumnId;
typedef jz_uuid jazz_types_GlobalPhysicalEnumVariantId;
typedef jz_uuid jazz_types_SchemaVersionId;
typedef jz_uuid jazz_types_SchemaFamilyId;
typedef jz_uuid jazz_types_MigrationLensId;
typedef jz_uuid jazz_types_RowUuid;
typedef jz_uuid jazz_types_SchemaLineagePublicationId;

/* AuthorSubject: System | SystemAt(Intern) | Authenticated(Intern) */
enum { JAZZ_AUTHOR_SYSTEM = 0, JAZZ_AUTHOR_SYSTEM_AT = 1, JAZZ_AUTHOR_AUTHENTICATED = 2 };
typedef struct jazz_types_AuthorSubject { u32 tag; const void *interned; } jazz_types_AuthorSubject;
typedef struct jazz_types_RowAuthor { u32 tag; const void *interned; } jazz_types_RowAuthor;
static inline bool jazz_types_AuthorSubject_eq(const jazz_types_AuthorSubject *a, const jazz_types_AuthorSubject *b) {
    return a->tag == b->tag && a->interned == b->interned;
}
jz_slice jazz_types__ids__AuthorSubject__canonical(const jazz_types_AuthorSubject *s);
int jazz_types__ids__AuthorSubject__cmp(const void *a, const void *b, void *ctx);
jz_err jazz_types__ids__RowAuthor__from_persisted_subject(jazz_types_AuthorSubject s, jazz_types_RowAuthor *out);
jazz_types_AuthorSubject jazz_types__ids__RowAuthor__as_author_subject(jazz_types_RowAuthor a);

jz_err jazz_types__time__GlobalTime__tick(jazz_types_GlobalTime reg, u64 now_ms, jazz_types_GlobalTime *out);
u64 jazz_types__time__GlobalTime__physical_ms(jazz_types_GlobalTime t);
jz_err jazz_types__time__TxTime__tick_after(jazz_types_TxTime t, jazz_types_TxTime *out);
jz_err jazz_types__time__TxTime__from_physical_ms(u64 ms, jazz_types_TxTime *out);
u64 jazz_types__time__TxTime__physical_ms(jazz_types_TxTime t);
enum { JAZZ_TYPES_ERR_HLC_OVERFLOW = 0x5101 };

/* ======================= groove ======================= */
typedef struct groove_LargeValueRef groove_LargeValueRef;
typedef struct groove_OwnedRecord groove_OwnedRecord;
typedef struct groove_RecordDescriptor groove_RecordDescriptor;
typedef struct groove_Value groove_Value;
JZ_NVEC(groove_ValueVec, groove_Value);
enum {
    GROOVE_V_U8, GROOVE_V_U16, GROOVE_V_U32, GROOVE_V_U64, GROOVE_V_F64, GROOVE_V_BOOL,
    GROOVE_V_STRING, GROOVE_V_BYTES, GROOVE_V_LARGE, GROOVE_V_UUID, GROOVE_V_ENUM_TAG,
    GROOVE_V_TUPLE, GROOVE_V_ARRAY, GROOVE_V_NULLABLE, GROOVE_V_I64, GROOVE_V_I32,
    GROOVE_V_RECORD, GROOVE_V_ENUM, GROOVE_V_U48
};
struct groove_Value {
    u32 tag;
    union {
        u64 u; i64 i; f64 f; bool b; jz_str s; jz_bytes bytes; jz_uuid uuid;
        groove_LargeValueRef *large;
        groove_ValueVec vals;
        groove_Value *nullable; /* NULL = None */
        groove_OwnedRecord *record;
        struct { u32 tag; groove_OwnedRecord *record; } en;
    } v;
};
void groove__records__values__Value__clone(void *dst, const void *src);
void groove__records__values__Value__drop(void *v);
bool groove__records__values__Value__eq(const groove_Value *a, const groove_Value *b);
bool groove__records__values__Value__eq_v(const void *a, const void *b);
int groove__records__values__Value__cmp(const void *a, const void *b, void *ctx);
void groove__records__values__Value__serialize(jz_pc_w *w, const groove_Value *v);
void groove__records__values__Value__debug(jz_str *out, const groove_Value *v);
jz_err groove__records__values__Value__deserialize(jz_pc_r *r, groove_Value *out);

typedef struct groove_ValueType groove_ValueType;
typedef struct groove_ScalarEnumSchema { JZ_VEC(jz_str) variants; } groove_ScalarEnumSchema;
typedef struct groove_EnumCaseLayout groove_EnumCaseLayout;
typedef struct groove_EnumSchema { JZ_VEC(groove_EnumCaseLayout *) cases; } groove_EnumSchema;
enum {
    GROOVE_VT_U8, GROOVE_VT_U16, GROOVE_VT_U32, GROOVE_VT_U64, GROOVE_VT_I32, GROOVE_VT_I64,
    GROOVE_VT_F64, GROOVE_VT_BOOL, GROOVE_VT_STRING, GROOVE_VT_BYTES, GROOVE_VT_INTERNAL,
    GROOVE_VT_UUID, GROOVE_VT_ENUM_TAG, GROOVE_VT_TUPLE, GROOVE_VT_ARRAY, GROOVE_VT_NULLABLE,
    GROOVE_VT_RECORD, GROOVE_VT_ENUM, GROOVE_VT_U48
};
struct groove_ValueType {
    u32 tag;
    union {
        groove_ScalarEnumSchema *enum_tag;
        JZ_VEC(groove_ValueType) tuple;
        groove_ValueType *inner;            /* Array / Nullable */
        groove_RecordDescriptor *record;
        groove_EnumSchema *enm;
        u32 internal;
    } v;
};
void groove__records__values__ValueType__clone(void *dst, const void *src);
void groove__records__values__ValueType__drop(void *v);
bool groove__records__values__ValueType__eq(const groove_ValueType *a, const groove_ValueType *b);
groove_RecordDescriptor *groove__records__EnumCaseLayout__payload(const groove_EnumCaseLayout *c);

typedef struct groove_DescriptorField {
    bool has_name; jz_str name;
    groove_ValueType value_type;
    u32 identity_tag; jz_str identity_name;
} groove_DescriptorField;
JZ_NVEC(groove_DescriptorFieldVec, groove_DescriptorField);
void groove__records__DescriptorField__new(groove_DescriptorField *out, jz_str name, groove_ValueType ty);
void groove__records__DescriptorField__with_identity_name(groove_DescriptorField *f, jz_str name);
void groove__records__DescriptorField__drop(void *f);
const groove_DescriptorFieldVec *groove__records__RecordDescriptor__fields(const groove_RecordDescriptor *d);
bool groove__records__RecordDescriptor__field_index(const groove_RecordDescriptor *d, const char *name, u32 *out);
groove_RecordDescriptor *groove__records__RecordDescriptor__new_with_fields(groove_DescriptorFieldVec fields);
jz_err groove__records__RecordDescriptor__create(const groove_RecordDescriptor *d, const groove_Value *vals, u32 n, jz_bytes *out);
groove_RecordDescriptor *groove__records__RecordDescriptor__clone_box(const groove_RecordDescriptor *d);
void groove__records__RecordDescriptor__drop_box(groove_RecordDescriptor *d);

groove_OwnedRecord *groove__records__OwnedRecord__new(jz_bytes raw, groove_RecordDescriptor *d);
const groove_RecordDescriptor *groove__records__OwnedRecord__descriptor(const groove_OwnedRecord *r);
jz_slice groove__records__OwnedRecord__raw(const groove_OwnedRecord *r);
jz_err groove__records__OwnedRecord__to_values(const groove_OwnedRecord *r, groove_ValueVec *out);
jz_err groove__records__BorrowedRecord__get_idx(const groove_OwnedRecord *r, u32 idx, groove_Value *out);
jz_err groove__records__BorrowedRecord__get_uuid(const groove_OwnedRecord *r, u32 idx, jz_uuid *out);
jz_err groove__records__BorrowedRecord__get_u64(const groove_OwnedRecord *r, u32 idx, u64 *out);
jz_err groove__records__BorrowedRecord__get_record(const groove_OwnedRecord *r, u32 idx, groove_OwnedRecord **out);
void groove__records__OwnedRecord__drop(groove_OwnedRecord *r);
bool groove__records__OwnedRecord__eq(const groove_OwnedRecord *a, const groove_OwnedRecord *b);
struct jazz_engine_CurrentRowPublicationField;
bool jazz_engine__query_engine__publication__CurrentRowPublicationField__PartialEq__eq(const struct jazz_engine_CurrentRowPublicationField *a, const struct jazz_engine_CurrentRowPublicationField *b);
enum {
    GROOVE_REC_ERR_NON_CANONICAL = 0x6101, GROOVE_REC_ERR_INVALID_ENUM_DISCRIMINANT,
    GROOVE_REC_ERR_UNKNOWN_ENUM_TAG
};

bool groove__large_values__LargeValueRef__eq(const groove_LargeValueRef *a, const groove_LargeValueRef *b);
groove_LargeValueRef *groove__large_values__LargeValueRef__clone(const groove_LargeValueRef *a);
void groove__large_values__LargeValueRef__drop(groove_LargeValueRef *a);
#define GROOVE_INLINE_VALUE_MAX_BYTES 1024u
typedef struct groove_StagedLargeValueId { jz_uuid id; } groove_StagedLargeValueId;
typedef struct groove_StagedLargeValue { groove_StagedLargeValueId id; groove_LargeValueRef *value_ref; } groove_StagedLargeValue;

/* groove::db */
typedef struct groove_Database groove_Database;
typedef struct groove_BoxedStorage groove_BoxedStorage;
typedef struct groove_AppliedBatch groove_AppliedBatch;
typedef struct groove_PersistedBatch groove_PersistedBatch;
typedef struct groove_GraphBuilder groove_GraphBuilder;
typedef struct groove_DatabaseBatch groove_DatabaseBatch;
typedef struct groove_Subscription groove_Subscription;
typedef u64 groove_PreparedShapeId;
typedef struct groove_LocalChunkReader groove_LocalChunkReader;
typedef struct groove_OwnedChunkProvider groove_OwnedChunkProvider;
typedef struct groove_MissingChunkResolverVt groove_MissingChunkResolverVt;
typedef struct groove_ColumnType groove_ColumnType;
typedef struct groove_StorageReadMetrics groove_StorageReadMetrics;
const jz_str *groove__db__AppliedBatch__changed_tables(const groove_AppliedBatch *b, u32 *n);
void groove__db__AppliedBatch__persist(const groove_AppliedBatch *b, groove_PersistedBatch **out);
void groove__db__Database__drop(groove_Database *d);

/* groove::Intern<String> */
typedef const jz_str *groove_InternStr;
groove_InternStr groove__Intern__new_str(jz_str s);


/* ======================= jazz_model ======================= */
typedef struct jazz_model_TxId { jazz_types_TxTime time; jazz_types_NodeUuid node; } jazz_model_TxId;
static inline int jazz_model_TxId_cmp(const jazz_model_TxId *a, const jazz_model_TxId *b) {
    if (a->time != b->time) return a->time < b->time ? -1 : 1;
    return jz_uuid_cmp(&a->node, &b->node);
}
static inline bool jazz_model_TxId_eq(const jazz_model_TxId *a, const jazz_model_TxId *b) { return jazz_model_TxId_cmp(a, b) == 0; }
void jazz_model__tx__TxId__debug(jz_str *out, const jazz_model_TxId *t);
typedef enum { JAZZ_DUR_NONE, JAZZ_DUR_LOCAL, JAZZ_DUR_GLOBAL } jazz_model_DurabilityTier;
typedef enum { JAZZ_TXKIND_EXCLUSIVE, JAZZ_TXKIND_MERGEABLE } jazz_model_TxKind;
typedef enum { JAZZ_DEL_DELETED, JAZZ_DEL_RESTORED } jazz_model_DeletionEvent;
typedef enum { JAZZ_ASPECT_CONTENT, JAZZ_ASPECT_DELETION } jazz_model_MergeAspect;
typedef struct jazz_model_RejectionReason jazz_model_RejectionReason;
typedef struct jazz_model_Fate { u32 tag; jazz_model_RejectionReason *reason; } jazz_model_Fate; /* Pending|Accepted|Rejected */
enum { JAZZ_FATE_PENDING, JAZZ_FATE_ACCEPTED, JAZZ_FATE_REJECTED };
typedef struct jazz_model_BranchKey jazz_model_BranchKey;
typedef struct jazz_model_BranchSelector jazz_model_BranchSelector;
typedef struct jazz_model_Transaction jazz_model_Transaction;
typedef struct jazz_model_TransactionRecord jazz_model_TransactionRecord;
typedef struct jazz_model_Snapshot jazz_model_Snapshot;
typedef struct jazz_model_RejectedTransaction jazz_model_RejectedTransaction;
typedef struct jazz_model_RowRead { jz_str table; jazz_types_RowUuid row; jazz_model_TxId proof; } jazz_model_RowRead;
typedef struct jazz_model_PredicateRead jazz_model_PredicateRead;
typedef struct jazz_model_AbsentRead jazz_model_AbsentRead;
typedef jz_uuid jazz_model_OpenTransactionId;
/* jazz_model::tx read-set mirrors (exclusive_read_evidence) */
JZ_NVEC(jazz_model_TxIdVec, jazz_model_TxId);
struct jazz_model_Snapshot { jazz_types_NodeUuid owner; jazz_types_GlobalTime global_base; jazz_types_TxTime local_base; jazz_model_TxIdVec dots; };
struct jazz_model_AbsentRead { jz_str table; jazz_types_RowUuid row_uuid; };
struct jazz_model_PredicateRead { jz_str table; jz_uuid shape_id; struct jazz_model_Query *shape; jz_uuid binding_id; jz_btree binding_values; /* String -> Value */ };
JZ_NVEC(jazz_model_RowReadVec, jazz_model_RowRead);
JZ_NVEC(jazz_model_AbsentReadVec, struct jazz_model_AbsentRead);
JZ_NVEC(jazz_model_PredicateReadVec, struct jazz_model_PredicateRead);
jz_err jazz_model__tx__Snapshot__exclusive_base(jazz_types_NodeUuid owner, jazz_types_GlobalTime g, jazz_types_TxTime l, jazz_model_TxIdVec dots, jazz_model_Snapshot *out);
bool jazz_model__tx__Snapshot__eq(const jazz_model_Snapshot *a, const jazz_model_Snapshot *b);
void jazz_model__tx__Snapshot__drop(jazz_model_Snapshot *s);
bool jazz_model__tx__RowReadVec__eq(const jazz_model_RowReadVec *a, const jazz_model_RowReadVec *b);
bool jazz_model__tx__AbsentReadVec__eq(const jazz_model_AbsentReadVec *a, const jazz_model_AbsentReadVec *b);
bool jazz_model__tx__PredicateReadVec__eq(const jazz_model_PredicateReadVec *a, const jazz_model_PredicateReadVec *b);
void jazz_model__tx__RowRead__drop(void *p);
void jazz_model__tx__AbsentRead__drop(void *p);
void jazz_model__tx__PredicateRead__drop(void *p);
/* Option<...> accessors on Transaction: NULL = None */
const jazz_model_Snapshot *jazz_model__tx__Transaction__base_snapshot(const jazz_model_Transaction *t);
const jazz_model_RowReadVec *jazz_model__tx__Transaction__row_read_set(const jazz_model_Transaction *t);
const jazz_model_AbsentReadVec *jazz_model__tx__Transaction__absent_read_set(const jazz_model_Transaction *t);
const jazz_model_PredicateReadVec *jazz_model__tx__Transaction__predicate_read_set(const jazz_model_Transaction *t);
/* jazz_model::query */
void jazz_model__query__Query__Serialize__serialize(jz_pc_w *w, const struct jazz_model_Query *q);
jz_err jazz_model__query__Query__Deserialize__deserialize(jz_pc_r *r, struct jazz_model_Query **out);
void jazz_model__query__Query__drop(struct jazz_model_Query *q);
jz_err jazz_model__query__canonical_binding_bytes_for_values(const jz_btree *values, jz_bytes *out);
jz_err jazz_model__query__binding_values_from_canonical_bytes(jz_slice bytes, jz_btree *out);
extern const jz_uuid jazz_model__query__QUERY_NAMESPACE;
/* groove::records::Record (borrowed bytes + descriptor) */
groove_RecordDescriptor *groove__records__RecordDescriptor__new(const char *const *names, const groove_ValueType *types, u32 n);
groove_ValueType groove__records__values__ValueType__array_of(groove_ValueType inner);
jz_err groove__records__Record__to_values(jz_slice bytes, const groove_RecordDescriptor *d, groove_ValueVec *out);
typedef struct jazz_model_Query jazz_model_Query;
/* ---- jazz_model::query AST mirror (layout as in jazz_model/jazz_model.h) ---- */
#ifndef JZN_MODEL_QUERY_AST
#define JZN_MODEL_QUERY_AST
typedef JZ_VEC(jz_str) jazz_model_StrVec;
typedef struct jazz_model_OptStr { bool some; jz_str s; } jazz_model_OptStr;
enum { JAZZ_RPE_CMP, JAZZ_RPE_IS_NULL, JAZZ_RPE_IS_NOT_NULL, JAZZ_RPE_IN, JAZZ_RPE_CONTAINS, JAZZ_RPE_ENUM_MATCH,
       JAZZ_RPE_AND, JAZZ_RPE_OR, JAZZ_RPE_NOT, JAZZ_RPE_TRUE, JAZZ_RPE_FALSE };
enum { JAZZ_JOIN_INNER = 0, JAZZ_JOIN_LEFT = 1 };
enum { JAZZ_ROWID_CURRENT = 0, JAZZ_ROWID_OUTER = 1, JAZZ_ROWID_FRONTIER = 2 };
enum { JAZZ_KEYREF_COLUMN = 0, JAZZ_KEYREF_ROW_ID = 1 };
enum { JAZZ_BOUND_FIXPOINT = 0, JAZZ_BOUND_MAX_DEPTH = 1 };
enum { JAZZ_OPERAND_COLUMN = 0, JAZZ_OPERAND_PARAM, JAZZ_OPERAND_CLAIM, JAZZ_OPERAND_LITERAL };
typedef struct jazz_model_Operand { u8 tag; jz_str name; groove_Value literal; } jazz_model_Operand;
typedef JZ_VEC(jazz_model_Operand) jazz_model_OperandVec;

enum {
    JAZZ_PRED_ALL = 0, JAZZ_PRED_ANY, JAZZ_PRED_NOT, JAZZ_PRED_EQ, JAZZ_PRED_NE, JAZZ_PRED_IN, JAZZ_PRED_GT,
    JAZZ_PRED_GTE, JAZZ_PRED_LT, JAZZ_PRED_LTE, JAZZ_PRED_CONTAINS, JAZZ_PRED_ENUM_MATCH, JAZZ_PRED_IS_NULL,
    JAZZ_PRED_COUNT
};
typedef struct jazz_model_Predicate jazz_model_Predicate;
typedef JZ_VEC(jazz_model_Predicate) jazz_model_PredicateVec;
struct jazz_model_Predicate {
    u8 tag;
    jazz_model_PredicateVec children;  /* All / Any */
    jazz_model_Predicate *inner;       /* Not / EnumMatch payload */
    jazz_model_Operand left, right;    /* binary ops; IsNull uses left */
    jazz_model_OperandVec values;      /* In */
    jz_str column, case_;              /* EnumMatch */
};

enum { JAZZ_JOIN_MODE_INNER = 0, JAZZ_JOIN_MODE_HOLES = 1 };
typedef struct jazz_model_Include { jz_str path; u8 join_mode; bool require; } jazz_model_Include;
typedef JZ_VEC(jazz_model_Include) jazz_model_IncludeVec;

enum { JAZZ_ORDER_ASC = 0, JAZZ_ORDER_DESC = 1 };
typedef struct jazz_model_OrderBy { jz_str column; u8 direction; } jazz_model_OrderBy;
typedef JZ_VEC(jazz_model_OrderBy) jazz_model_OrderByVec;

enum { JAZZ_ASQ_OPTIONAL = 0, JAZZ_ASQ_AT_LEAST_ONE, JAZZ_ASQ_MATCH_CORRELATION_CARDINALITY };
typedef struct jazz_model_ArraySubquery jazz_model_ArraySubquery;
typedef JZ_VEC(jazz_model_ArraySubquery) jazz_model_ArraySubqueryVec;
struct jazz_model_ArraySubquery {
    jz_str column_name, table, inner_column, outer_column;
    jazz_model_PredicateVec filters;
    bool has_select; jazz_model_StrVec select;
    jazz_model_OrderByVec order_by;
    bool has_limit; u64 limit;
    u64 offset;
    u8 requirement;
    jazz_model_ArraySubqueryVec nested_arrays;
};

enum { JAZZ_AGG_COUNT = 0, JAZZ_AGG_SUM, JAZZ_AGG_AVG, JAZZ_AGG_MIN, JAZZ_AGG_MAX };
typedef struct jazz_model_Aggregate { u8 function; jazz_model_OptStr column; jz_str alias; } jazz_model_Aggregate;
typedef JZ_VEC(jazz_model_Aggregate) jazz_model_AggregateVec;
typedef struct jazz_model_AggregateQuery { jazz_model_AggregateVec aggregates; jazz_model_OptStr group_by; } jazz_model_AggregateQuery;

enum { JAZZ_JOIN_TARGET_COLUMN = 0, JAZZ_JOIN_TARGET_ROW_ID, JAZZ_JOIN_TARGET_UNCORRELATED };
typedef struct jazz_model_JoinCorrelation { jz_str join_column, source_column; } jazz_model_JoinCorrelation;
typedef JZ_VEC(jazz_model_JoinCorrelation) jazz_model_JoinCorrelationVec;
typedef struct jazz_model_JoinSourceLookup { jz_str table, row_id_source_column, value_column; } jazz_model_JoinSourceLookup;
typedef struct jazz_model_JoinVia jazz_model_JoinVia;
typedef JZ_VEC(jazz_model_JoinVia) jazz_model_JoinViaVec;
struct jazz_model_JoinVia {
    jz_str table, on_column;
    u8 target;
    jazz_model_OptStr source_column;
    jazz_model_JoinSourceLookup *source_lookup; /* NULL = None */
    jazz_model_JoinCorrelationVec correlated_filters;
    jazz_model_PredicateVec filters;
    jazz_model_JoinViaVec nested_joins;
};

/* RecursionBound tags: JAZZ_BOUND_FIXPOINT / JAZZ_BOUND_MAX_DEPTH (shared with relation_ir) */
typedef struct jazz_model_RecursionBound { u8 tag; u64 max_depth; } jazz_model_RecursionBound;

typedef struct jazz_model_ReachableSeed {
    jz_str table; jazz_model_OptStr user_column, user_claim; jz_str team_column; jazz_model_PredicateVec filters;
} jazz_model_ReachableSeed;
typedef struct jazz_model_ReachableVia {
    jz_str access_table, access_row_column, access_team_column;
    u8 access_team_target;
    jazz_model_Operand from;
    jazz_model_PredicateVec access_filters;
    jz_str edge_table, edge_member_column, edge_parent_column;
    jazz_model_PredicateVec edge_filters;
    jazz_model_RecursionBound bound;
    jazz_model_ReachableSeed *seed; /* NULL = None */
} jazz_model_ReachableVia;
typedef JZ_VEC(jazz_model_ReachableVia) jazz_model_ReachableViaVec;

enum { JAZZ_INHERITS_SELECT = 0, JAZZ_INHERITS_INSERT, JAZZ_INHERITS_UPDATE, JAZZ_INHERITS_DELETE };
typedef struct jazz_model_InheritsVia { jz_str parent_column; u8 operation; bool has_max_depth; u64 max_depth; } jazz_model_InheritsVia;
typedef JZ_VEC(jazz_model_InheritsVia) jazz_model_InheritsViaVec;

typedef struct jazz_model_PolicyBranch {
    jazz_model_PredicateVec filters; jazz_model_JoinViaVec joins;
    jazz_model_ReachableViaVec reachable; jazz_model_InheritsViaVec inherits;
} jazz_model_PolicyBranch;
typedef JZ_VEC(jazz_model_PolicyBranch) jazz_model_PolicyBranchVec;

typedef struct jazz_model_FlatJoinOn { jz_str left, right; } jazz_model_FlatJoinOn;
typedef struct jazz_model_FlatJoinSource { jz_str table; jazz_model_OptStr alias; jazz_model_FlatJoinOn on; } jazz_model_FlatJoinSource;
typedef JZ_VEC(jazz_model_FlatJoinSource) jazz_model_FlatJoinSourceVec;
typedef struct jazz_model_FlatJoin { jazz_model_OptStr root_alias; jazz_model_FlatJoinSourceVec sources; } jazz_model_FlatJoin;

/* relation facade (query::RelationExpr and friends) */
typedef struct jazz_model_RelationColumnRef { jazz_model_OptStr scope; jz_str column; } jazz_model_RelationColumnRef;
enum { JAZZ_RVAL_LITERAL = 0, JAZZ_RVAL_PARAM, JAZZ_RVAL_SESSION_REF, JAZZ_RVAL_OUTER_COLUMN, JAZZ_RVAL_FRONTIER_COLUMN, JAZZ_RVAL_ROW_ID };
typedef struct jazz_model_RelationValueRef {
    u8 tag;
    jz_json *literal;                  /* Literal(serde_json::Value) */
    jz_str param;                      /* Param */
    jazz_model_StrVec session_ref;     /* SessionRef */
    jazz_model_RelationColumnRef column; /* OuterColumn / FrontierColumn */
    u8 row_id;                         /* RowId (JAZZ_ROWID_*) */
} jazz_model_RelationValueRef;
typedef JZ_VEC(jazz_model_RelationValueRef) jazz_model_RelationValueRefVec;
typedef struct jazz_model_RelationPredicate jazz_model_RelationPredicate;
typedef JZ_VEC(jazz_model_RelationPredicate) jazz_model_RelationPredicateVec;
struct jazz_model_RelationPredicate {
    u8 tag;                              /* JAZZ_RPE_* (same variant order as relation_ir) */
    jazz_model_RelationColumnRef left;   /* Cmp/In/Contains left; IsNull/IsNotNull/EnumMatch column */
    u8 op;                               /* JAZZ_PCMP_* */
    jazz_model_RelationValueRef right;
    jazz_model_RelationValueRefVec values;
    jz_str case_;
    jazz_model_RelationPredicate *inner; /* EnumMatch payload / Not */
    jazz_model_RelationPredicateVec items; /* And / Or */
};
typedef struct jazz_model_RelationKeyRef { u8 tag; jazz_model_RelationColumnRef column; u8 row_id; } jazz_model_RelationKeyRef;
typedef JZ_VEC(jazz_model_RelationKeyRef) jazz_model_RelationKeyRefVec;
typedef jazz_model_RelationKeyRef jazz_model_RelationProjectExpr;
typedef struct jazz_model_RelationProjectColumn { jz_str alias; jazz_model_RelationProjectExpr expr; } jazz_model_RelationProjectColumn;
typedef JZ_VEC(jazz_model_RelationProjectColumn) jazz_model_RelationProjectColumnVec;
typedef struct jazz_model_RelationJoinCondition { jazz_model_RelationColumnRef left, right; } jazz_model_RelationJoinCondition;
typedef JZ_VEC(jazz_model_RelationJoinCondition) jazz_model_RelationJoinConditionVec;
typedef struct jazz_model_RelationOrderBy { jazz_model_RelationColumnRef column; u8 direction; } jazz_model_RelationOrderBy;
typedef JZ_VEC(jazz_model_RelationOrderBy) jazz_model_RelationOrderByVec;
enum {
    JAZZ_REXPR_TABLE_SCAN = 0, JAZZ_REXPR_FILTER, JAZZ_REXPR_UNION, JAZZ_REXPR_JOIN, JAZZ_REXPR_PROJECT,
    JAZZ_REXPR_GATHER, JAZZ_REXPR_DISTINCT, JAZZ_REXPR_ORDER_BY, JAZZ_REXPR_OFFSET, JAZZ_REXPR_LIMIT
};
typedef struct jazz_model_RelationExpr jazz_model_RelationExpr;
typedef struct jazz_model_RelationUnionArm jazz_model_RelationUnionArm;
typedef JZ_VEC(jazz_model_RelationUnionArm) jazz_model_RelationUnionArmVec;
struct jazz_model_RelationExpr {
    u8 tag;
    jz_str table; jazz_model_OptStr alias;          /* TableScan */
    jazz_model_RelationExpr *input;                 /* Filter/Project/Distinct/OrderBy/Offset/Limit input; Join left; Gather seed */
    jazz_model_RelationExpr *right;                 /* Join right; Gather step */
    jazz_model_RelationPredicate predicate;         /* Filter */
    jazz_model_RelationUnionArmVec inputs;          /* Union */
    jazz_model_RelationJoinConditionVec on;         /* Join */
    u8 join_kind;                                   /* Join */
    jazz_model_RelationProjectColumnVec columns;    /* Project */
    jazz_model_RelationKeyRef frontier_key;         /* Gather */
    jazz_model_RecursionBound bound;                /* Gather */
    jazz_model_RelationKeyRefVec keys;              /* Gather dedupe_key / Distinct key */
    jazz_model_RelationOrderByVec terms;            /* OrderBy */
    u64 n;                                          /* Offset / Limit */
};
struct jazz_model_RelationUnionArm { jz_str label; jazz_model_RelationExpr input; };
typedef struct jazz_model_RelationQuery { jazz_model_RelationExpr rel; } jazz_model_RelationQuery;

struct jazz_model_Query {
    jz_str table;
    jazz_model_PredicateVec filters;
    jazz_model_JoinViaVec joins;
    jazz_model_FlatJoin *flat_join;             /* NULL = None */
    jazz_model_PolicyBranchVec policy_branches;
    jazz_model_ReachableViaVec reachable;
    jazz_model_InheritsViaVec inherits;
    jazz_model_IncludeVec includes;
    jazz_model_ArraySubqueryVec array_subqueries;
    bool has_select; jazz_model_StrVec select;
    jazz_model_OrderByVec order_by;
    jazz_model_AggregateQuery *aggregate;       /* NULL = None */
    bool has_limit; u64 limit;
    u64 offset;
    jazz_model_RelationQuery *relation;         /* NULL = None */
};
typedef JZ_VEC(jazz_model_Query) jazz_model_QueryVec;
#endif
typedef struct jazz_model_ValidatedQuery jazz_model_ValidatedQuery;
typedef struct jazz_model_Binding jazz_model_Binding;
typedef struct jazz_model_QueryError jazz_model_QueryError;
typedef jz_uuid jazz_model_ShapeId;
typedef jz_uuid jazz_model_BindingId;
enum { JAZZ_LVK_NOT_LARGE = 0, JAZZ_LVK_BYTES = 1, JAZZ_LVK_STRING = 2, JAZZ_LVK_JSON = 3 };
typedef struct jazz_model_ColumnSchema {
    jz_str name;
    groove_ValueType column_type;
    u32 large_value_kind;
    bool has_default; groove_Value deflt;
} jazz_model_ColumnSchema;
void jazz_model__schema__ColumnSchema__clone(void *dst, const void *src);
void jazz_model__schema__ColumnSchema__drop(void *p);
bool jazz_model__schema__ColumnSchema__eq(const void *a, const void *b);
bool jazz_model__schema__physical_value_epoch_is_compatible(const groove_ValueType *a, const groove_ValueType *b);
/* WritePolicies { insert_check, update_using, update_check, delete_using }; iter() yields the Some ones in that order */
typedef struct jazz_model_WritePolicies {
    jazz_model_Query *insert_check, *update_using, *update_check, *delete_using; /* NULL = None */
} jazz_model_WritePolicies;
typedef struct jazz_model_TableSchema {
    jz_str name;
    JZ_VEC(jazz_model_ColumnSchema) columns;
    JZ_VEC(jz_str) branch_by;
    jz_btree references;
    jazz_model_Query *read_policy; /* Option */
    jazz_model_WritePolicies write_policies;
    jz_btree indexed_columns;
    jz_btree composite_indexes;
    jz_btree merge_strategies;
} jazz_model_TableSchema;
typedef enum { JAZZ_MERGE_LWW, JAZZ_MERGE_COUNTER, JAZZ_MERGE_GSET } jazz_model_MergeStrategy;
jazz_model_MergeStrategy jazz_model__schema__TableSchema__merge_strategy(const jazz_model_TableSchema *t, const jz_str *column);
#define JAZZ_COUNTER_SIGNS_FIELD "counter_signs"
groove_RecordDescriptor *groove__records__RecordDescriptor__new_single(const char *name, groove_ValueType ty);
typedef struct jazz_model_JazzSchema jazz_model_JazzSchema;
JZ_NVEC(jazz_model_TableSchemaVec, jazz_model_TableSchema);
const jazz_model_TableSchemaVec *jazz_model__schema__JazzSchema__tables(const jazz_model_JazzSchema *s);
const jazz_model_TableSchema *jazz_model__schema__JazzSchema__table(const jazz_model_JazzSchema *s, const jz_str *name);
jazz_types_SchemaVersionId jazz_model__schema__JazzSchema__version_id(const jazz_model_JazzSchema *s);
jazz_model_JazzSchema *jazz_model__schema__JazzSchema__clone(const jazz_model_JazzSchema *s);
void jazz_model__schema__JazzSchema__drop(jazz_model_JazzSchema *s);
bool jazz_model__query__Query__eq(const jazz_model_Query *a, const jazz_model_Query *b);
jazz_model_Query *jazz_model__query__Query__from_table(const jz_str *table);
jazz_model_Query *jazz_model__query__Query__filter_any_empty(jazz_model_Query *q);
jazz_model_Query *jazz_model__query__Query__clone(const jazz_model_Query *q);
void jazz_model__query__Query__drop(jazz_model_Query *q);
typedef struct jazz_model_ColumnTransformSemantics { bool bijective; bool canonical_equality_preserving; } jazz_model_ColumnTransformSemantics;
const jazz_model_ColumnTransformSemantics *jazz_model__schema__registered_column_transform(const jz_str *name);

/* ======================= jazz_protocol ======================= */
typedef struct jazz_protocol_ReadViewKey { jz_uuid id; } jazz_protocol_ReadViewKey;
typedef struct jazz_protocol_SubscriptionKey { jazz_model_ShapeId shape_id; jazz_model_BindingId binding_id; jazz_protocol_ReadViewKey read_view; } jazz_protocol_SubscriptionKey;
typedef jazz_protocol_SubscriptionKey jazz_protocol_BindingViewKey;
static inline int jazz_protocol_SubscriptionKey_cmp(const jazz_protocol_SubscriptionKey *a, const jazz_protocol_SubscriptionKey *b) { return memcmp(a, b, sizeof *a); }
void jazz_protocol__protocol__SubscriptionKey__debug(jz_str *out, const jazz_protocol_SubscriptionKey *k);
typedef struct jazz_protocol_PolicyBindingKey jazz_protocol_PolicyBindingKey;
int jazz_protocol__protocol__PolicyBindingKey__cmp(const jazz_protocol_PolicyBindingKey *a, const jazz_protocol_PolicyBindingKey *b);
jazz_protocol_PolicyBindingKey *jazz_protocol__protocol__PolicyBindingKey__clone(const jazz_protocol_PolicyBindingKey *k);
void jazz_protocol__protocol__PolicyBindingKey__drop(jazz_protocol_PolicyBindingKey *k);
const jazz_types_AuthorSubject *jazz_protocol__protocol__PolicyBindingKey__identity(const jazz_protocol_PolicyBindingKey *k);
typedef struct jazz_protocol_AuthorityResultKey { jazz_protocol_BindingViewKey binding_view; jazz_protocol_PolicyBindingKey *policy_binding; } jazz_protocol_AuthorityResultKey;
int jazz_protocol__protocol__AuthorityResultKey__cmp(const void *a, const void *b, void *ctx);
void jazz_protocol__protocol__AuthorityResultKey__clone(void *dst, const void *src);
void jazz_protocol__protocol__AuthorityResultKey__drop(void *p);
typedef enum { JAZZ_LAYER_CONTENT, JAZZ_LAYER_DELETION, JAZZ_LAYER_CONTENT_OR_DELETION } jazz_protocol_ResultRowLayer;
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
    groove_InternStr version_table;
    jazz_types_RowUuid row;
    jazz_protocol_RowVersionRefEntry version;
} jazz_protocol_SupportingRow;
int jazz_protocol__protocol__SupportingRow__cmp(const void *a, const void *b, void *ctx);
void jazz_protocol__protocol__SupportingRow__clone(void *dst, const void *src);
void jazz_protocol__protocol__SupportingRow__drop(void *p);
JZ_NVEC(jazz_protocol_SupportingRowVec, jazz_protocol_SupportingRow);
typedef struct jazz_protocol_SupportingRowsUpdate jazz_protocol_SupportingRowsUpdate;
const jazz_protocol_SupportingRow *jazz_protocol__protocol__SupportingRowsUpdate__added_rows(const jazz_protocol_SupportingRowsUpdate *u, u32 *n);
const jazz_protocol_SupportingRow *jazz_protocol__protocol__SupportingRowsUpdate__removed_rows(const jazz_protocol_SupportingRowsUpdate *u, u32 *n);
typedef struct jazz_protocol_ResultMemberEntry { u32 tag; void *p; } jazz_protocol_ResultMemberEntry;
typedef struct jazz_protocol_VersionRecord jazz_protocol_VersionRecord;
typedef struct jazz_protocol_VersionCarrier jazz_protocol_VersionCarrier;
typedef struct jazz_protocol_VersionBundle {
    jazz_model_Transaction *tx;
    JZ_VEC(jazz_protocol_VersionRecord *) versions;
    u32 scope;
    jazz_model_Fate fate;
    bool has_global_time; jazz_types_GlobalTime global_time;
    jazz_model_DurabilityTier durability;
} jazz_protocol_VersionBundle;
u32 jazz_protocol__protocol__VersionRecord__application_cell_count(const jazz_protocol_VersionRecord *v);
bool jazz_protocol__protocol__VersionRecord__cell_at(const jazz_protocol_VersionRecord *v, u32 pos, groove_Value *out);
typedef struct jazz_protocol_SchemaVersion { jazz_types_SchemaVersionId id; jazz_model_JazzSchema *schema; } jazz_protocol_SchemaVersion;
typedef struct jazz_protocol_CurrentWriteSchema { u64 revision; jazz_types_SchemaVersionId schema; } jazz_protocol_CurrentWriteSchema;
typedef struct jazz_protocol_SchemaLineagePublication jazz_protocol_SchemaLineagePublication;
typedef struct jazz_protocol_PhysicalIdentityManifest jazz_protocol_PhysicalIdentityManifest;
typedef struct jazz_protocol_ShapeAst jazz_protocol_ShapeAst;
typedef struct jazz_protocol_Subscribe jazz_protocol_Subscribe;
typedef struct jazz_protocol_RegisterShapeOptions jazz_protocol_RegisterShapeOptions;
typedef struct jazz_protocol_KnownStateDeclaration jazz_protocol_KnownStateDeclaration;
typedef struct jazz_protocol_ProgramSourceId jazz_protocol_ProgramSourceId;
typedef struct jazz_protocol_SyncMessage jazz_protocol_SyncMessage;
enum { /* SyncMessage tags used by this unit */
    JAZZ_SM_CATALOGUE_SNAPSHOT = 1, JAZZ_SM_PUBLISH_SCHEMA, JAZZ_SM_PUBLISH_SCHEMA_WITH_LENS, JAZZ_SM_PUBLISH_LENS
};
u32 jazz_protocol__protocol__SyncMessage__tag(const jazz_protocol_SyncMessage *m);
enum { JAZZ_LENSOP_RENAME_TABLE, JAZZ_LENSOP_RENAME_COLUMN, JAZZ_LENSOP_COPY_COLUMN, JAZZ_LENSOP_ADD_COLUMN,
       JAZZ_LENSOP_DROP_COLUMN, JAZZ_LENSOP_TRANSFORM_COLUMN, JAZZ_LENSOP_REJECT_SOURCE_DELTA };
typedef struct jazz_protocol_LensOp {
    u32 tag;
    jz_str a;  /* from / column / reason */
    jz_str b;  /* to / transform */
    groove_Value value; /* default / backwards_default */
} jazz_protocol_LensOp;
typedef struct jazz_protocol_TableLens { jz_str source_table; jz_str target_table; JZ_VEC(jazz_protocol_LensOp) ops; } jazz_protocol_TableLens;
typedef struct jazz_protocol_MigrationLens {
    jazz_types_MigrationLensId id;
    jazz_types_SchemaVersionId source, target;
    JZ_VEC(jazz_protocol_TableLens) table_lenses;
} jazz_protocol_MigrationLens;
void jazz_protocol__protocol__MigrationLens__clone(jazz_protocol_MigrationLens *dst, const jazz_protocol_MigrationLens *src);
void jazz_protocol__protocol__MigrationLens__drop(jazz_protocol_MigrationLens *l);
bool jazz_protocol__protocol__MigrationLens__eq(const jazz_protocol_MigrationLens *a, const jazz_protocol_MigrationLens *b);

/* ======================= jazz_engine::query_engine ======================= */
typedef enum { JAZZ_ROLE_PHYSICAL_COLUMN, JAZZ_ROLE_LOGICAL_FIELD } jazz_engine_CurrentRowBindingRole;
JZ_NVEC(jazz_engine_CurrentRowBindingRoleVec, jazz_engine_CurrentRowBindingRole);
typedef enum { JAZZ_VIS_APPLICATION_CELL, JAZZ_VIS_PUBLIC_PROVENANCE, JAZZ_VIS_HIDDEN_METADATA } jazz_engine_CurrentRowResultVisibility;
enum { JAZZ_PUBF_STORED_COLUMN, JAZZ_PUBF_RESULT_FIELD, JAZZ_PUBF_UNRESOLVED_SOURCE_CELL };
typedef struct jazz_engine_CurrentRowPublicationField {
    u32 tag;
    jazz_types_PhysicalColumnId id;
    jz_str name; /* output_name / name */
    jazz_engine_CurrentRowResultVisibility visibility;
} jazz_engine_CurrentRowPublicationField;
JZ_NVEC(jazz_engine_CurrentRowPublicationFieldVec, jazz_engine_CurrentRowPublicationField);
jazz_engine_CurrentRowResultVisibility jazz_engine__query_engine__publication__CurrentRowResultVisibility__current_row_metadata(const jz_str *name);
jazz_engine_CurrentRowBindingRole jazz_engine__query_engine__publication__CurrentRowPublicationField__role(const jazz_engine_CurrentRowPublicationField *f);
bool jazz_engine__query_engine__publication__CurrentRowPublicationField__application_name(const jazz_engine_CurrentRowPublicationField *f, jz_slice *out);
void jazz_engine__query_engine__publication__CurrentRowPublicationField__clone(void *dst, const void *src);
void jazz_engine__query_engine__publication__CurrentRowPublicationField__drop(void *p);
bool jazz_engine__query_engine__descriptor_public_name(const groove_DescriptorField *f, jz_slice *out);
jz_str jazz_engine__query_engine__user_column_field(const jz_str *column);
#define JAZZ_USER_COLUMN_PREFIX "user_"

/* ======================= jazz_node::node::physical (other unit) ======================= */
typedef struct jazz_node_GlobalScalarEnumCaseId { jazz_types_GlobalPhysicalEnumVariantId id; jazz_types_SchemaVersionId introducing_schema; u8 introducing_ordinal; } jazz_node_GlobalScalarEnumCaseId;
typedef struct jazz_node_GlobalEnumCaseId { jazz_types_GlobalPhysicalEnumVariantId id; jazz_types_SchemaVersionId introducing_schema; u32 introducing_ordinal; } jazz_node_GlobalEnumCaseId;
JZ_NVEC(jazz_node_GlobalScalarEnumCaseIdVec, jazz_node_GlobalScalarEnumCaseId);
JZ_NVEC(jazz_node_GlobalEnumCaseIdVec, jazz_node_GlobalEnumCaseId);
typedef struct jazz_node_TablePhysicalMapping {
    jazz_types_PhysicalTableId table_id;
    jz_btree columns;                  /* String -> PhysicalColumnId */
    JZ_VEC(struct jazz_node_PhysicalVariantCase *) variant_cases;
    jz_btree scalar_enum_cases;        /* PhysicalColumnId -> Vec<GlobalScalarEnumCaseId> */
    jz_btree payload_enum_cases;       /* PhysicalColumnId -> Vec<GlobalEnumCaseId> */
    jz_btree nested_scalar_enum_cases; /* PhysicalColumnId -> BTreeMap<String, Vec<..>> */
    jz_btree nested_payload_enum_cases;
} jazz_node_TablePhysicalMapping;
typedef struct jazz_node_SchemaPhysicalMapping {
    jazz_protocol_PhysicalIdentityManifest *identities;
    jz_btree tables;                   /* String -> TablePhysicalMapping */
} jazz_node_SchemaPhysicalMapping;
typedef enum { JZN_PHYS_CLASS_GLOBAL, JZN_PHYS_CLASS_AHEAD, JZN_PHYS_CLASS_HISTORY } jazz_node_PhysicalCurrentClass;
jz_slice jazz_node__node__codec__global_current_key_branch_prefix(jz_slice key);
void jazz_node__node__codec__global_current_branch_key_prefix(const jazz_model_BranchKey *b, jz_bytes *out);

/* groove::ivm terminal operations */
typedef struct groove_TerminalPathSegment { u32 tag; jz_str name; jz_bytes key; } groove_TerminalPathSegment; /* Collection(name) | Key(bytes) */
enum { GROOVE_TPS_COLLECTION, GROOVE_TPS_KEY };
enum { GROOVE_TE_INSERT, GROOVE_TE_UPDATE, GROOVE_TE_REMOVE, GROOVE_TE_MOVE };
typedef struct groove_TerminalEdit { u32 tag; jz_bytes key; jz_bytes value; usize index; } groove_TerminalEdit;
typedef struct groove_TerminalOperation {
    groove_RecordDescriptor *root_descriptor;
    JZ_VEC(groove_TerminalPathSegment) path;
    groove_TerminalEdit edit;
} groove_TerminalOperation;
void groove__ivm__terminal_occurrence_key(jz_bytes row_key, u64 occurrence, jz_bytes *out);
bool groove__records__RecordDescriptor__eq(const groove_RecordDescriptor *a, const groove_RecordDescriptor *b);
jz_err groove__records__BorrowedRecord__to_values(const groove_OwnedRecord *r, groove_ValueVec *out);
groove_OwnedRecord *groove__records__OwnedRecord__clone(const groove_OwnedRecord *r);
void groove__records__Error__Display__fmt(const jz_err *e, jz_str *out);
usize jazz_postcard_serialized_size_values(const groove_ValueVec *v);

/* groove::ivm::graph::GraphBuilder (by pointer, owning) */
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__clone(const groove_GraphBuilder *g);
void groove__ivm__graph__GraphBuilder__drop(groove_GraphBuilder *g);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__project(groove_GraphBuilder *g, const char *const *cols, u32 n);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__union(groove_GraphBuilder **inputs, u32 n);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__anti_join(groove_GraphBuilder *left, groove_GraphBuilder *right,
    const char *const *lcols, u32 nl, const char *const *rcols, u32 nr);

/* jazz_engine::query_engine::read::BranchViewSourceBase */
typedef struct jazz_engine_BranchViewSourceBase { u32 tag; jazz_model_BranchKey *key; void *snapshot; } jazz_engine_BranchViewSourceBase; /* Current | Snapshot */
typedef struct jazz_protocol_BranchViewBase { u32 tag; jazz_model_BranchSelector *branch; void *snapshot; } jazz_protocol_BranchViewBase;   /* Current(sel) | Snapshot{branch,snapshot} */
bool jazz_model__model__branch__BranchKey__eq(const jazz_model_BranchKey *a, const jazz_model_BranchKey *b);
void jazz_model__model__branch__BranchKey__drop(jazz_model_BranchKey *k);
jazz_model_BranchKey *jazz_model__model__branch__BranchKey__default(void);
jazz_model_BranchKey *jazz_model__model__branch__BranchKey__clone(const jazz_model_BranchKey *k);
void jazz_model__model__branch__BranchKey__canonical_bytes(const jazz_model_BranchKey *k, jz_bytes *out);
bool jazz_model__model__branch__BranchKey__values_is_empty(const jazz_model_BranchKey *k);
int jazz_model__model__branch__BranchKey__cmp(const jazz_model_BranchKey *a, const jazz_model_BranchKey *b);
jz_err jazz_model__schema__RuntimeSchema__project_branch_view_selector(const jazz_model_JazzSchema *s, const jazz_model_TableSchema *t,
    const jazz_model_BranchSelector *sel, jazz_model_BranchKey **out, jz_str *err_msg);
void *jazz_model__tx__SnapshotRef__clone(const void *s);

/* ======================= jazz_node::node::codec (other unit) ======================= */
typedef struct jazz_node_VersionRow jazz_node_VersionRow;          /* opaque, owning pointer */
JZ_NVEC(jazz_node_VersionRowVec, jazz_node_VersionRow *);
jazz_node_VersionRow *jazz_node__node__codec__VersionRow__clone(const jazz_node_VersionRow *v);
void jazz_node__node__codec__VersionRow__drop(jazz_node_VersionRow *v);
void jazz_node__node__codec__VersionRow__drop_v(void *pv);
jazz_types_RowUuid jazz_node__node__codec__VersionRow__row_uuid(const jazz_node_VersionRow *v);
jazz_types_TxTime jazz_node__node__codec__VersionRow__tx_time(const jazz_node_VersionRow *v);
jazz_types_NodeAlias jazz_node__node__codec__VersionRow__tx_node_alias(const jazz_node_VersionRow *v);
const jz_str *jazz_node__node__codec__VersionRow__table(const jazz_node_VersionRow *v);
jazz_types_SchemaVersionAlias jazz_node__node__codec__VersionRow__schema_version_alias(const jazz_node_VersionRow *v);
const jazz_model_BranchKey *jazz_node__node__codec__VersionRow__branch_key(const jazz_node_VersionRow *v);
jazz_model_MergeAspect jazz_node__node__codec__VersionRow__aspect(const jazz_node_VersionRow *v);
bool jazz_node__node__codec__VersionRow__deletion(const jazz_node_VersionRow *v, jazz_model_DeletionEvent *out);
const groove_OwnedRecord *jazz_node__node__codec__VersionRow__record(const jazz_node_VersionRow *v);
bool jazz_node__node__codec__VersionRow__parent(const jazz_node_VersionRow *v, jazz_model_TxId *out);
bool jazz_node__node__codec__VersionRow__global_time(const jazz_node_VersionRow *v, jazz_types_GlobalTime *out);
jz_err jazz_node__node__codec__version_tx_id_from_aliases(const jazz_node_VersionRow *v, const struct jazz_node_NodeAliases *aliases, jazz_model_TxId *out);
jz_err jazz_node__node__codec__history_primary_key(const jazz_node_VersionRow *v, groove_ValueVec *out);
jz_err jazz_node__node__codec__deletion_event_from_value(groove_Value v, jazz_model_DeletionEvent *out);
jz_str jazz_node__node__physical__physical_global_current_table_name(jazz_types_PhysicalTableId t);
jz_str jazz_node__node__physical__physical_history_table_name(jazz_types_PhysicalTableId t);
jz_str jazz_node__node__physical__physical_rejected_versions_table_name(jazz_types_PhysicalTableId t);
jz_str jazz_node__node__physical__physical_ahead_current_table_name(jazz_types_PhysicalTableId t);
enum { /* GlobalCurrentRowRecord field indexes */
    JZN_GCR_FIELD_BRANCH_KEY_IDX = 0, JZN_GCR_FIELD_ROW_UUID_IDX = 1, JZN_GCR_FIELD_TX_TIME_IDX = 2,
    JZN_GCR_FIELD_TX_NODE_ID_IDX = 3, JZN_GCR_FIELD_SCHEMA_VERSION_IDX = 4, JZN_GCR_FIELD_CREATED_BY_IDX = 5,
    JZN_GCR_FIELD_CREATED_AT_IDX = 6, JZN_GCR_FIELD_UPDATED_BY_IDX = 7, JZN_GCR_FIELD_UPDATED_AT_IDX = 8,
/* jazz_model::schema field-name constants */
#define JAZZ_SEQ_FIELD "seq"
#define JAZZ_BASE_SEQ_FIELD "base_seq"
#define JAZZ_BASE_PENDING_FIELD "base_pending"
    JZN_GCR_FIELD_GLOBAL_TIME_IDX = 9, JZN_GCR_FIELD__DELETION_IDX = 10,
    JZN_HRR_FIELD_BRANCH_KEY_IDX = 0, JZN_HRR_FIELD_ROW_UUID_IDX = 1, JZN_HRR_FIELD_TX_TIME_IDX = 2,
    JZN_HRR_FIELD_TX_NODE_ID_IDX = 3, JZN_HRR_FIELD_SCHEMA_VERSION_IDX = 4, JZN_HRR_FIELD_CREATED_BY_IDX = 5,
    JZN_HRR_FIELD_CREATED_AT_IDX = 6, JZN_HRR_FIELD_UPDATED_BY_IDX = 7, JZN_HRR_FIELD_UPDATED_AT_IDX = 8,
    JZN_HRR_FIELD__DELETION_IDX = 9
};
enum { /* codec record layouts: field indexes */
    JZN_TXR_FIELD_TIME_IDX = 0, JZN_TXR_FIELD_NODE_ID_IDX = 1, JZN_TXR_FIELD_KIND_IDX = 2, JZN_TXR_FIELD_N_TOTAL_WRITES_IDX = 3,
    JZN_TXR_FIELD_MADE_BY_IDX = 4, JZN_TXR_FIELD_BASE_SNAPSHOT_IDX = 5, JZN_TXR_FIELD_ROW_READ_SET_IDX = 6, JZN_TXR_FIELD_ABSENT_READ_SET_IDX = 7,
    JZN_TXR_FIELD_PREDICATE_READ_SET_IDX = 8, JZN_TXR_FIELD_USER_METADATA_IDX = 9, JZN_TXR_FIELD_CONTRIBUTION_MERGE_IDX = 10,
    JZN_TXR_FIELD_PERMISSION_SUBJECT_IDX = 11, JZN_TXR_FIELD_IS_PARTIAL_IDX = 12, JZN_TXR_FIELD_FATE_IDX = 13,
    JZN_TXR_FIELD_GLOBAL_TIME_IDX = 14, JZN_TXR_FIELD_REJECTION_REASON_IDX = 15, JZN_TXR_FIELD_DURABILITY_IDX = 16,
    JZN_NAR_FIELD_ID_IDX = 0, JZN_NAR_FIELD_UUID_IDX = 1,
    JZN_RTR_FIELD_TIME_IDX = 0, JZN_RTR_FIELD_NODE_ID_IDX = 1,
    JZN_RVR_FIELD_TX_TIME_IDX = 0, JZN_RVR_FIELD_TX_NODE_ID_IDX = 1
};
jz_err jazz_node__node__codec__durability_from_discriminant(u8 d, jazz_model_DurabilityTier *out);
jz_err jazz_node__node__codec__fate_from_encoded_fields(const groove_OwnedRecord *r, jazz_model_Fate *out);
typedef struct jazz_node_StoredTransaction {
    jazz_model_Transaction *tx;
    jazz_types_NodeAlias node_alias;
    jazz_model_Fate fate;
    bool has_global_time; jazz_types_GlobalTime global_time;
    jazz_model_DurabilityTier durability;
    bool is_partial;
} jazz_node_StoredTransaction;
void jazz_node__node__codec__StoredTransaction__drop(jazz_node_StoredTransaction *t);
jz_err jazz_node__node__codec__StoredTransaction__reconcile_fate(jazz_node_StoredTransaction *t, jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt, bool has_dur, jazz_model_DurabilityTier dur);
jazz_model_TxId jazz_model__tx__Transaction__tx_id(const jazz_model_Transaction *t);
jazz_model_TxKind jazz_model__tx__Transaction__kind(const jazz_model_Transaction *t);
const jazz_types_AuthorSubject *jazz_model__tx__Transaction__made_by(const jazz_model_Transaction *t);
jazz_model_Transaction *jazz_model__tx__Transaction__clone(const jazz_model_Transaction *t);
void jazz_model__tx__Transaction__drop(jazz_model_Transaction *t);

/* groove raw rows (primary_storage) */
typedef struct groove_RawRow groove_RawRow; /* owning */
JZ_NVEC(groove_RawRowVec, groove_RawRow *);
JZ_NVEC(groove_OwnedRecordVec, groove_OwnedRecord *);
const groove_OwnedRecord *groove__db__RawRow__record(const groove_RawRow *r);
groove_OwnedRecord *groove__db__RawRow__owned_record(groove_RawRow *r);
u32 groove__db__RawRow__variant_tag(const groove_RawRow *r);
void groove__db__RawRow__drop(groove_RawRow *r);
jz_err groove__db__primary_storage__Database__primary_key_get_raw(groove_Database *db, const jz_str *table, const groove_Value *key, u32 n, groove_RawRow **out);
jz_err groove__db__primary_storage__Database__primary_key_scan_raw(groove_Database *db, const jz_str *table, const groove_Value *prefix, u32 n, groove_RawRowVec *out);
jz_err groove__db__primary_storage__Database__index_scan_range_raw(groove_Database *db, const jz_str *table, const char *index,
    const groove_Value *lo, u32 nlo, const groove_Value *hi, u32 nhi, groove_RawRowVec *out);
groove_DatabaseBatch *groove__db__facade__Database__open_batch(groove_Database *db);
void groove__db__batch__DatabaseBatch__delete(groove_DatabaseBatch *b, const jz_str *table, groove_ValueVec key);
void groove__db__batch__DatabaseBatch__drop(groove_DatabaseBatch *b);
jz_err groove__records__BorrowedRecord__get_nullable_enum(const groove_OwnedRecord *r, u32 idx, bool *some, u8 *tag);
jz_err groove__records__BorrowedRecord__get_nullable_u64(const groove_OwnedRecord *r, u32 idx, bool *some, u64 *out);
jz_err groove__db__primary_storage__Database__primary_key_last_raw(groove_Database *db, const jz_str *table, const groove_Value *prefix, u32 n, groove_RawRow **out);
jz_err groove__db__primary_storage__Database__index_scan_raw(groove_Database *db, const jz_str *table, const char *index, const groove_Value *key, u32 n, groove_RawRowVec *out);
jz_slice groove__db__RawRow__raw(const groove_RawRow *r);
jz_err groove__records__BorrowedRecord__get_enum(const groove_OwnedRecord *r, u32 idx, u8 *out);
#define JAZZ_GLOBAL_CURRENT_BY_SEQ_INDEX "by_seq"

/* ======================= NodeState methods defined by other units ======================= */
void jazz_node__node__NodeState__merge_tx_time(struct jazz_node_NodeState *s, jazz_types_TxTime observed);
const struct jazz_node_ClientRelayScope *jazz_node__node__NodeState__client_relay_scope(const struct jazz_node_NodeState *s);
jazz_types_NodeUuid jazz_node__node__NodeState__node_uuid(const struct jazz_node_NodeState *s);
u64 jazz_node__node__NodeState__session_claim_revision(const struct jazz_node_NodeState *s, jazz_types_AuthorSubject id);
u64 jazz_node__node__NodeState__active_catalogue_seq(const struct jazz_node_NodeState *s);
jazz_types_GlobalTime jazz_node__node__NodeState__committed_global_time(const struct jazz_node_NodeState *s);
bool jazz_node__node__NodeState__permissions_ready(const struct jazz_node_NodeState *s);
jz_err jazz_node__node__NodeState__table(const struct jazz_node_NodeState *s, const jz_str *name, const jazz_model_TableSchema **out);
jz_err jazz_node__node__NodeState__table_in_schema(struct jazz_node_NodeState *s, const jz_str *table, jazz_types_SchemaVersionId v, jazz_model_TableSchema **out);
jz_err jazz_node__node__NodeState__table_in_schema_ref(const struct jazz_node_NodeState *s, const jz_str *table, jazz_types_SchemaVersionId v, const jazz_model_TableSchema **out);
jz_err jazz_node__node__NodeState__version_tx_id(const struct jazz_node_NodeState *s, const jazz_node_VersionRow *v, jazz_model_TxId *out);
bool jazz_node__node__NodeState__node_for_alias(const struct jazz_node_NodeState *s, jazz_types_NodeAlias a, jazz_types_NodeUuid *out);
bool jazz_node__node__NodeState__schema_version_for_alias(const struct jazz_node_NodeState *s, jazz_types_SchemaVersionAlias a, jazz_types_SchemaVersionId *out);
void jazz_node__node__NodeState__invalidate_subscription_scopes(struct jazz_node_NodeState *s);
void jazz_node__node__NodeState__invalidate_tx_version_tables_cache(struct jazz_node_NodeState *s, jazz_model_TxId tx);
jz_err jazz_node__node__NodeState__apply_and_persist_node_batch(struct jazz_node_NodeState *s, groove_DatabaseBatch *b);
jz_err jazz_node__node__NodeState__apply_row_version_payloads_for_requests(struct jazz_node_NodeState *s, const void *requests, u32 n, void *bundles);
jz_err jazz_node__node__physical__NodeState__physical_table_id_for_schema(const struct jazz_node_NodeState *s, jazz_types_SchemaVersionId v, const jz_str *table, jazz_types_PhysicalTableId *out);
jz_err jazz_node__node__physical__NodeState__version_storage_table_for_row(const struct jazz_node_NodeState *s, const jazz_node_VersionRow *v, jz_str *out);
jz_err jazz_node__node__physical__NodeState__physical_current_table_for_schema(const struct jazz_node_NodeState *s, jazz_types_SchemaVersionId v, const jz_str *table, jazz_node_PhysicalCurrentClass c, jz_str *out);
jz_err jazz_node__node__query_eval__NodeState__query_readable_current_row_including_deleted(struct jazz_node_NodeState *s, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *b, jazz_model_DurabilityTier tier, jazz_types_AuthorSubject id, jazz_types_RowUuid row, struct jazz_node_CurrentRowVec_ *out);
void jazz_node__node__CurrentRow__drop(void *p);
void jazz_node__node__physical__NodeState__physical_table_ids(const struct jazz_node_NodeState *s, jzn_u64vec *out);
jz_err jazz_node__node__physical__NodeState__logical_table_for_physical_alias(const struct jazz_node_NodeState *s, jazz_types_PhysicalTableId t, jazz_types_SchemaVersionAlias a, jz_str *out);
groove_RecordDescriptor *jazz_model__schema__TableSchema__rejected_versions_record_schema(const jazz_model_TableSchema *t);
typedef struct jazz_model_RejectedVersion jazz_model_RejectedVersion;
jazz_model_RejectedVersion *jazz_model__tx__RejectedVersion__new(jz_str table, groove_OwnedRecord *rec);
const jz_str *jazz_model__tx__RejectedVersion__table(const jazz_model_RejectedVersion *v);
jazz_types_RowUuid jazz_model__tx__RejectedVersion__row_uuid(const jazz_model_RejectedVersion *v);
bool jazz_model__tx__RejectedVersion__has_deletion(const jazz_model_RejectedVersion *v);
jazz_model_RejectedTransaction *jazz_model__tx__RejectedTransaction__new(jazz_model_TxId tx, groove_OwnedRecord *rec, void *versions);
jz_err jazz_node__node__query_eval__subscription_watermarks__NodeState__purge_subscription_watermarks(struct jazz_node_NodeState *s);
jz_err jazz_node__node__ingest__NodeState__merged_global_post_image(struct jazz_node_NodeState *s, const groove_DatabaseBatch *b, jazz_types_SchemaVersionId authored,
    const jazz_model_TableSchema *t, const jazz_node_VersionRow *v, jazz_model_TxId tx, jazz_types_GlobalTime gt, bool minting, jazz_node_VersionRow **out);

/* protocol: current-row receipts */
typedef struct jazz_protocol_CurrentRowCoordinate { jazz_types_SchemaVersionId schema; jz_str table; jazz_types_GlobalPhysicalTableId physical_table; jazz_types_RowUuid row; } jazz_protocol_CurrentRowCoordinate;
typedef enum { JAZZ_CRO_READABLE, JAZZ_CRO_CURRENT_UNAVAILABLE, JAZZ_CRO_UNKNOWN } jazz_protocol_CurrentRowOutcome;
JZ_NVEC(jazz_protocol_CurrentRowCoordinateVec, jazz_protocol_CurrentRowCoordinate);
typedef struct jazz_protocol_CurrentRowsRequest { u64 request_id; jazz_protocol_CurrentRowCoordinateVec rows; void *delegated_session; } jazz_protocol_CurrentRowsRequest;
typedef struct jazz_protocol_CurrentRowsReceipt {
    u64 request_id;
    jazz_protocol_CurrentRowCoordinateVec rows;
    JZ_VEC(jazz_protocol_CurrentRowOutcome) outcomes;
    jazz_protocol_PolicyBindingKey *context;
    jazz_types_NodeUuid core;
    u64 core_epoch, claims_revision, policy_epoch;
    jazz_types_GlobalTime settled_through;
    u64 authorization_progress;
    JZ_VEC(jazz_protocol_VersionCarrier *) version_carriers;
} jazz_protocol_CurrentRowsReceipt;
void jazz_protocol__protocol__CurrentRowCoordinate__clone(void *dst, const void *src);
bool jazz_protocol__protocol__CurrentRowCoordinate__eq(const jazz_protocol_CurrentRowCoordinate *a, const jazz_protocol_CurrentRowCoordinate *b);
void jazz_protocol__protocol__CurrentRowCoordinate__drop(void *p);
bool jazz_protocol__protocol__PhysicalIdentityManifest__table_id(const jazz_protocol_PhysicalIdentityManifest *m, const jz_str *table, jazz_types_GlobalPhysicalTableId *out);
JZ_NVEC(jazz_protocol_VersionBundleVec, jazz_protocol_VersionBundle);
jz_err jazz_protocol__protocol__expand_version_carriers(jazz_protocol_VersionCarrier *const *carriers, u32 n, jazz_protocol_VersionBundleVec *out);
void jazz_protocol__protocol__VersionBundle__drop(void *b);
jazz_protocol_VersionCarrier *jazz_protocol__protocol__VersionCarrier__bundle(jazz_protocol_VersionBundle b);
void jazz_protocol__protocol__VersionCarrier__drop(jazz_protocol_VersionCarrier *c);
enum { JAZZ_BUNDLE_SCOPE_COMPLETE, JAZZ_BUNDLE_SCOPE_VIEW_SCOPED };
jazz_types_SchemaVersionId jazz_protocol__protocol__VersionRecord__schema_version(const jazz_protocol_VersionRecord *v);
const jz_str *jazz_protocol__protocol__VersionRecord__table(const jazz_protocol_VersionRecord *v);
jazz_types_RowUuid jazz_protocol__protocol__VersionRecord__row_uuid(const jazz_protocol_VersionRecord *v);
const jazz_model_BranchKey *jazz_protocol__protocol__VersionRecord__branch_key(const jazz_protocol_VersionRecord *v);
typedef struct jazz_protocol_RowVersionRef { jz_str table; jazz_types_RowUuid row; jazz_model_TxId tx; } jazz_protocol_RowVersionRef;
jazz_protocol_RowVersionRef jazz_protocol__protocol__RowVersionRef__new(jz_str table, jazz_types_RowUuid row, jazz_model_TxId tx);
jazz_protocol_SupportingRowsUpdate *jazz_protocol__protocol__SupportingRowsUpdate__catch_up(const u8 predecessor[16], const u8 revision[16],
    jazz_protocol_SupportingRowVec changed, jazz_protocol_SupportingRowVec left);

/* ======================= descriptor_roles external mirrors ======================= */
typedef struct groove_BorrowedRecord groove_BorrowedRecord;
typedef struct groove_Record groove_Record;
typedef struct groove_RecordProjector groove_RecordProjector;
typedef struct jz_usize_pair { u32 a, b; } jz_usize_pair;
JZ_NVEC(jz_usize_pair_vec, jz_usize_pair);
const groove_RecordDescriptor *groove__records__BorrowedRecord__descriptor(const groove_BorrowedRecord *r);
jz_err groove__records__BorrowedRecord__get_idx_borrowed(const groove_BorrowedRecord *r, u32 idx, groove_Value *out);
bool groove__records__RecordDescriptor__field_index_by_identity(const groove_RecordDescriptor *d, const groove_DescriptorField *identity_of, u32 *out);
jz_err groove__records__RecordDescriptor__bind_to_values(const groove_RecordDescriptor *d, jz_slice raw, groove_ValueVec *out); /* bind(raw).to_values() */
jz_err groove__records__values__encode_persisted_record_descriptor(const groove_RecordDescriptor *d, jz_bytes *out);
jz_err groove__records__values__decode_persisted_record_descriptor(jz_slice bytes, groove_RecordDescriptor **out);
void groove__records__DescriptorField__clone(void *dst, const void *src);
jz_err groove__records__RecordProjector__new_registry_rebound(groove_RecordDescriptor *source, groove_RecordDescriptor *target, jz_usize_pair_vec mapping, groove_RecordProjector **out);
jz_err groove__records__RecordProjector__project(const groove_RecordProjector *p, const groove_BorrowedRecord *r, groove_OwnedRecord **out);
jz_bytes groove__records__OwnedRecord__into_raw(groove_OwnedRecord *r);
void groove__records__RecordProjector__drop(groove_RecordProjector *p);
jz_err jazz_node__node__codec__runtime_result_identity_bytes(const groove_Value *v, const groove_ValueType *ty, jz_bytes *out);

typedef struct jazz_engine_SyntheticResultMembershipSchema jazz_engine_SyntheticResultMembershipSchema;
typedef struct jazz_engine_AggregateResultSchema {
    jazz_engine_SyntheticResultMembershipSchema *synthetic;
    groove_DescriptorFieldVec group_key_fields;
    jzn_strvec group_names;
    groove_DescriptorFieldVec value_fields;
    jzn_strvec value_names;
    jz_btree routing_param_fields;
} jazz_engine_AggregateResultSchema;
typedef struct jazz_engine_TypedOutputField { jz_str name; groove_ValueType ty; } jazz_engine_TypedOutputField;
JZ_NVEC(jazz_engine_TypedOutputFieldVec, jazz_engine_TypedOutputField);
typedef struct jazz_engine_ResultMembershipSchema {
    jz_str table_field;
    jz_str row_field;
    jzn_strvec occurrence_id_fields;
    jz_btree occurrence_union_arm_fields; /* usize -> String */
    jazz_engine_TypedOutputFieldVec payload_fields;
    jz_btree payload_publication_fields;  /* String -> CurrentRowPublicationField */
    bool has_branch_or_prefix_field; jz_str branch_or_prefix_field;
} jazz_engine_ResultMembershipSchema;

typedef struct jazz_protocol_ResultMemberPayloadEntry {
    jazz_protocol_ResultMemberEntry member;
    jz_bytes descriptor;
    jz_bytes record;
} jazz_protocol_ResultMemberPayloadEntry;
enum { JAZZ_RME_ROW, JAZZ_RME_SYNTHETIC, JAZZ_RME_PATH_TUPLE };
typedef struct jazz_types_OutputOccurrenceId jazz_types_OutputOccurrenceId;
typedef struct jazz_types_SourceRef jazz_types_SourceRef;
typedef struct jazz_protocol_SyntheticReplacementToken jazz_protocol_SyntheticReplacementToken;
typedef struct jazz_types_UnionArm { u32 position; jz_str arm; } jazz_types_UnionArm;
/* as_row(): Some((table, row_uuid)) */
bool jazz_protocol__protocol__ResultMemberEntry__as_row(const jazz_protocol_ResultMemberEntry *m, const jz_str **table, jazz_types_RowUuid *row);
/* NULL = None; caller owns result */
jazz_types_OutputOccurrenceId *jazz_protocol__protocol__ResultMemberEntry__output_occurrence_id(const jazz_protocol_ResultMemberEntry *m);
void jazz_types__object__OutputOccurrenceId__drop(jazz_types_OutputOccurrenceId *o);
const jazz_types_SourceRef *jazz_types__object__OutputOccurrenceId__joined_sources(const jazz_types_OutputOccurrenceId *o, u32 *n);
const jazz_types_SourceRef *jazz_types__object__OutputOccurrenceId__root_source(const jazz_types_OutputOccurrenceId *o);
const jazz_types_UnionArm *jazz_types__object__OutputOccurrenceId__union_arms(const jazz_types_OutputOccurrenceId *o, u32 *n);
const jz_uuid *jazz_types__object__SourceRef__uuid(const jazz_types_SourceRef *s, u32 idx);
/* Synthetic { table, row, replacement } */
bool jazz_protocol__protocol__ResultMemberEntry__as_synthetic(const jazz_protocol_ResultMemberEntry *m, const jz_bytes **row, const jazz_protocol_SyntheticReplacementToken **replacement);
jz_slice jazz_protocol__protocol__SyntheticReplacementToken__encoded_record(const jazz_protocol_SyntheticReplacementToken *t);

/* ======================= source_resolution external mirrors ======================= */
typedef struct jazz_model_BranchColumnValue { jz_bytes bytes; } jazz_model_BranchColumnValue;
typedef struct jazz_model_BranchKeyEntry { jz_str name; jazz_model_BranchColumnValue value; } jazz_model_BranchKeyEntry;
JZ_NVEC(jazz_model_BranchKeyEntryVec, jazz_model_BranchKeyEntry);
struct jazz_model_BranchKey { jazz_model_BranchKeyEntryVec values; };
jz_err jazz_model__model__branch__BranchColumnValue__encode_typed(const groove_Value *value, const groove_ValueType *t, jazz_model_BranchColumnValue *out);
jz_err jazz_model__model__branch__BranchColumnValue__decode(const jazz_model_BranchColumnValue *v, groove_Value *out);
void jazz_model__model__branch__BranchColumnValue__clone(void *dst, const void *src);
bool jazz_model__model__branch__BranchColumnValue__eq(const void *a, const void *b);
void jazz_model__model__branch__BranchColumnValue__drop(void *p);
void jazz_model__model__branch__BranchKeyEntry__drop(void *p);
typedef struct jazz_model_SnapshotRef {
    jazz_types_NodeUuid owner; jazz_types_GlobalTime global_base; jazz_types_TxTime local_base; jazz_model_TxIdVec dots;
} jazz_model_SnapshotRef;
enum { JAZZ_BVCB_CURRENT, JAZZ_BVCB_SNAPSHOT };
typedef struct jazz_model_BranchViewCopyBase { u32 tag; jazz_model_BranchKey *branch; jazz_model_SnapshotRef *snapshot; } jazz_model_BranchViewCopyBase;
typedef struct jazz_model_BranchViewCopyEvidence {
    u32 version;
    jazz_model_BranchKey *head;
    jazz_model_BranchViewCopyBase base;
    jz_str table;
    jazz_types_RowUuid row_uuid;
    jazz_model_TxId source_version;
} jazz_model_BranchViewCopyEvidence;
enum { JAZZ_BVB_CURRENT, JAZZ_BVB_SNAPSHOT };
jz_err jazz_node__node__codec__current_row_from_materialized_cells_with_layer_provenance(const jazz_model_TableSchema *table, const jazz_node_VersionRow *content,
    const jazz_node_VersionRow *created, const jazz_node_VersionRow *updated, const jz_btree *cells, struct jazz_node_CurrentRow *out);
void jazz_node__node__codec__sort_current_rows(struct jazz_node_CurrentRow *rows, u32 n);
jz_err jazz_node__node__NodeState__materialized_cells_for_version(const struct jazz_node_NodeState *s, const jazz_model_TableSchema *table, const jazz_node_VersionRow *v, jz_btree *out);
struct jazz_node_CompiledLensPath;
jz_err jazz_node__node__NodeState__compiled_lens_path(struct jazz_node_NodeState *s, jazz_types_SchemaVersionId from, jazz_types_SchemaVersionId to,
    const jz_str *table, bool *some, struct jazz_node_CompiledLensPath *out);

/* ======================= catalogue_ingest external mirrors ======================= */
struct jazz_protocol_PhysicalIdentityManifest { jz_btree tables; /* String -> PhysicalTableIdentity */ };
typedef struct jazz_protocol_PhysicalTableIdentity { jazz_types_GlobalPhysicalTableId id; jz_btree columns; /* String -> PhysicalColumnIdentity */ } jazz_protocol_PhysicalTableIdentity;
typedef struct jazz_protocol_PhysicalColumnIdentity { jazz_types_GlobalPhysicalColumnId id; jz_btree enum_variants; /* String -> jzn_uuidvec */ } jazz_protocol_PhysicalColumnIdentity;
jazz_protocol_PhysicalIdentityManifest *jazz_protocol__protocol__PhysicalIdentityManifest__clone(const jazz_protocol_PhysicalIdentityManifest *m);
bool jazz_protocol__protocol__PhysicalIdentityManifest__eq(const jazz_protocol_PhysicalIdentityManifest *a, const jazz_protocol_PhysicalIdentityManifest *b);
void jazz_protocol__protocol__PhysicalIdentityManifest__drop(jazz_protocol_PhysicalIdentityManifest *m);
jz_err jazz_protocol__protocol__PhysicalIdentityManifest__validate_for_schema(const jazz_protocol_PhysicalIdentityManifest *m, const jazz_model_JazzSchema *schema, jz_str *err_msg);
typedef struct jazz_protocol_SchemaPredecessor { jazz_protocol_MigrationLens lens; jzn_strvec new_tables; jzn_strvec dropped_tables; } jazz_protocol_SchemaPredecessor;
JZ_NVEC(jazz_protocol_SchemaPredecessorVec, jazz_protocol_SchemaPredecessor);
struct jazz_protocol_SchemaLineagePublication {
    jazz_types_SchemaLineagePublicationId id;
    jazz_protocol_SchemaVersion schema;
    jazz_protocol_SchemaPredecessorVec predecessors;
    jazz_protocol_PhysicalIdentityManifest *physical_identities;
};
void jazz_protocol__protocol__SchemaLineagePublication__clone(jazz_protocol_SchemaLineagePublication *dst, const jazz_protocol_SchemaLineagePublication *src);
bool jazz_protocol__protocol__SchemaLineagePublication__eq(const jazz_protocol_SchemaLineagePublication *a, const jazz_protocol_SchemaLineagePublication *b);
void jazz_protocol__protocol__SchemaLineagePublication__drop(jazz_protocol_SchemaLineagePublication *p);
JZ_NVEC(jazz_protocol_SchemaVersionVec, jazz_protocol_SchemaVersion);
typedef struct jazz_protocol_SeqPublication { u64 catalogue_seq; jazz_protocol_SchemaLineagePublication publication; } jazz_protocol_SeqPublication;
JZ_NVEC(jazz_protocol_SeqPublicationVec, jazz_protocol_SeqPublication);
typedef struct jazz_protocol_CatalogueSnapshot {
    jazz_protocol_PhysicalIdentityManifest *genesis_physical_identities;
    jazz_protocol_SchemaVersionVec schemas;
    jazz_protocol_SeqPublicationVec lineages;
    jazz_protocol_CurrentWriteSchema current_write_schema;
} jazz_protocol_CatalogueSnapshot;
void jazz_protocol__protocol__SchemaVersion__clone(jazz_protocol_SchemaVersion *dst, const jazz_protocol_SchemaVersion *src);
bool jazz_protocol__protocol__SchemaVersion__eq(const jazz_protocol_SchemaVersion *a, const jazz_protocol_SchemaVersion *b);
void jazz_protocol__protocol__SchemaVersion__drop(jazz_protocol_SchemaVersion *v);
jazz_protocol_SchemaVersion jazz_protocol__protocol__SchemaVersion__new(jazz_model_JazzSchema *schema);
jazz_types_SchemaVersionId jazz_model__schema__RuntimeSchema__version_id(const jazz_model_JazzSchema *s);
bool jazz_model__schema__JazzSchema__eq(const jazz_model_JazzSchema *a, const jazz_model_JazzSchema *b);
jazz_model_JazzSchema *jazz_model__schema__JazzSchema__clone(const jazz_model_JazzSchema *s);
jazz_model_JazzSchema *jazz_model__schema__JazzSchema__without_permissions(const jazz_model_JazzSchema *s);
void jazz_node__node__physical__SchemaPhysicalMapping__clone(jazz_node_SchemaPhysicalMapping *dst, const jazz_node_SchemaPhysicalMapping *src);
bool jazz_node__node__physical__SchemaPhysicalMapping__eq(const jazz_node_SchemaPhysicalMapping *a, const jazz_node_SchemaPhysicalMapping *b);
void jazz_node__node__physical__SchemaPhysicalMapping__drop(void *m);
bool jazz_node__node__physical__TablePhysicalMapping__eq_v(const void *a, const void *b);
jz_err jazz_node__node__physical__allocate_provisional_physical_mapping(const jazz_model_JazzSchema *schema, jazz_protocol_PhysicalIdentityManifest *identities,
    u64 *next_physical_table_id, u64 *next_physical_column_id, jazz_node_SchemaPhysicalMapping *out);
jz_err jazz_node__node__physical__validate_scalar_enum_case_provenance(const jz_btree *mappings, const jz_btree *aliases);
jz_err jazz_node__node__physical__validate_payload_enum_case_provenance(const jz_btree *mappings, const jz_btree *aliases);
/* groove db facade */
typedef struct groove_RuntimeRegistryCheckpoint groove_RuntimeRegistryCheckpoint;
jz_err groove__db__facade__Database__ensure_usable(const groove_Database *db);
bool groove__db__facade__Database__has_unsettled_publications(const groove_Database *db);
groove_RuntimeRegistryCheckpoint *groove__db__schema_admission__Database__runtime_registry_checkpoint(groove_Database *db);
void groove__db__schema_admission__Database__restore_runtime_registry(groove_Database *db, groove_RuntimeRegistryCheckpoint *c);
jz_err groove__db__commit__Database__apply_batch(groove_Database *db, groove_DatabaseBatch *b, groove_AppliedBatch **out);
typedef struct groove_PersistOutcome groove_PersistOutcome;
groove_PersistOutcome *groove__db__commit__Database__persist_with_progress(groove_Database *db, const groove_AppliedBatch *applied);
jz_err groove__db__commit__Database__finish_persistence(groove_Database *db, groove_PersistOutcome *o);
void groove__db__AppliedBatch__drop(groove_AppliedBatch *b);
enum { GROOVE_DB_E_UNSETTLED_PUBLICATIONS = 0x6a10 };
/* state/catalogue.rs and other units' NodeState methods */
void jazz_node__node__put_catalogue(groove_DatabaseBatch *b, u32 kind, jz_uuid id, jz_bytes payload);
void jazz_node__node__delete_catalogue(groove_DatabaseBatch *b, u32 kind, jz_uuid id);
enum { JZN_CAT_GENESIS, JZN_CAT_SCHEMA, JZN_CAT_LENS, JZN_CAT_LINEAGE_STAGED, JZN_CAT_LINEAGE_PENDING, JZN_CAT_LINEAGE_ACTIVE, JZN_CAT_WRITE_POINTER_PENDING, JZN_CAT_BOOTSTRAP_READY };
jz_err jazz_node__node__codec__encode_catalogue_schema(const jazz_protocol_SchemaVersion *s, jz_bytes *out);
jz_err jazz_node__node__NodeState__rebuild_database_slot(struct jazz_node_NodeState *s);
jz_err jazz_node__node__physical__NodeState__synchronize_physical_version_tables(struct jazz_node_NodeState *s);
void jazz_node__node__NodeState__invalidate_subscription_scopes(struct jazz_node_NodeState *s);
jz_err jazz_node__node__query_eval__subscriptions__NodeState__drain_parked_shape_registrations(struct jazz_node_NodeState *s);
jz_err jazz_node__node__ingest__NodeState__validate_schema_lineage_publication(const jazz_protocol_SchemaLineagePublication *p);
jz_err jazz_node__node__NodeState__validate_publication_sources(const jazz_protocol_SchemaLineagePublication *p, const jz_btree *schemas, const jz_btree *mappings,
    jzn_ptrvec manifests /* Vec<PhysicalIdentityManifest> */);
jz_err jazz_node__node__NodeState__reconcile_source_physical_mapping_for_lens_payload(const jazz_protocol_MigrationLens *lens, const jazz_protocol_SchemaVersion *source,
    const jazz_protocol_SchemaVersion *target, const jazz_node_SchemaPhysicalMapping *provisional_source, const jazz_node_SchemaPhysicalMapping *target_mapping,
    jazz_node_SchemaPhysicalMapping *out);
jz_err jazz_node__node__NodeState__validate_durable_physical_identity_bindings(const jz_btree *schemas, const jz_btree *mappings, const jz_btree *staged,
    const jz_btree *active, const jz_btree *pending, const jazz_types_SchemaVersionId *genesis, u32 bootstrap_state);
void jazz_engine__query_engine__QueryProgramTemplateCache__clear(void *c);
void jazz_node__node__query_eval__SupportedQueryProgram__drop(void *p);

/* ======================= currency external mirrors ======================= */
typedef struct groove_TableSchema groove_TableSchema;
jz_err groove__db__schema_admission__Database__table_schema(const groove_Database *db, const jz_str *table, const groove_TableSchema **out);
const groove_RecordDescriptor *groove__schema__TableSchema__record_schema_for_variant(const groove_TableSchema *t, u32 variant_tag);
typedef jz_err (*groove_encode_field_fn)(u32 index, jz_bytes *output, void *ctx);
jz_err groove__records__RecordDescriptor__create_with_encoded_fields(const groove_RecordDescriptor *d, u32 capacity, groove_encode_field_fn f, void *ctx, jz_bytes *out);
jz_err groove__records__RecordDescriptor__encode_field_into(const groove_RecordDescriptor *d, u32 index, const groove_Value *v, jz_bytes *output);
jz_err groove__records__RecordDescriptor__field_span(const groove_RecordDescriptor *d, jz_slice raw, u32 index, u32 *start, u32 *end);
jz_err groove__records__BorrowedRecord__get_bytes(const groove_OwnedRecord *r, u32 idx, jz_slice *out);
jz_err groove__records__BorrowedRecord__get_u32(const groove_OwnedRecord *r, u32 idx, u32 *out);
jz_err groove__records__BorrowedRecord__get_bool(const groove_OwnedRecord *r, u32 idx, bool *out);
jz_err groove__records__BorrowedRecord__get_nullable_string(const groove_OwnedRecord *r, u32 idx, bool *some, jz_slice *out);
jz_err groove__records__BorrowedRecord__get_nullable_record(const groove_OwnedRecord *r, u32 idx, groove_OwnedRecord **out); /* NULL = None */
jz_err groove__records__RecordField__read_option_author_subject(const groove_OwnedRecord *r, u32 idx, bool *some, jazz_types_AuthorSubject *out);
groove_OwnedRecord *groove__records__OwnedRecord__clone(const groove_OwnedRecord *r);
void groove__records__OwnedRecord__drop(groove_OwnedRecord *r);
jz_err groove__db__primary_storage__Database__primary_key_get_raw_in_batch(groove_Database *db, const groove_DatabaseBatch *b, const jz_str *table,
    const groove_Value *key, u32 n, groove_RawRow **out);
jz_err groove__db__primary_storage__Database__primary_key_scan_range_raw(groove_Database *db, const jz_str *table, const groove_Value *lo, u32 nlo,
    const groove_Value *hi, u32 nhi, groove_RawRowVec *out);
jz_err jazz_types__time__TxTime__from_physical_ms(u64 ms, jazz_types_TxTime *out);
jz_err jazz_types__ids__RowAuthor__from_record(const groove_OwnedRecord *r, jazz_types_RowAuthor *out);
groove_ValueType jazz_types__ids__RowAuthor__value_type(void);
jz_err jazz_model__schema__RuntimeSchema__decode_persisted_branch_key(const jazz_model_TableSchema *t, jz_slice bytes, jazz_model_BranchKey **out);
jazz_model_Transaction *jazz_model__tx__Transaction__new(jazz_model_TxId tx_id, jazz_model_TxKind kind, u32 n_total_writes, jazz_types_AuthorSubject made_by,
    bool has_permission_subject, jazz_types_AuthorSubject permission_subject, bool has_base_snapshot, jazz_model_Snapshot base_snapshot,
    const jazz_model_RowReadVec *row_read_set, const jazz_model_AbsentReadVec *absent_read_set, const jazz_model_PredicateReadVec *predicate_read_set,
    const jz_slice *user_metadata_json, void *contribution_merge);
/* codec (other unit) */
jazz_node_VersionRow *jazz_node__node__codec__VersionRow__from_parts(jz_str table, jazz_model_BranchKey *branch_key, groove_OwnedRecord *record);
jz_err jazz_node__node__codec__VersionRow__updated_by_is_implicit(const jazz_node_VersionRow *v, bool *out);
jz_err jazz_node__node__codec__VersionRow__validate_canonical(const jazz_node_VersionRow *v);
jz_err jazz_node__node__codec__VersionRow__seq(const jazz_node_VersionRow *v, jazz_types_GlobalTime *out);
void jazz_node__node__codec__history_seq_candidates(bool has_global_time, jazz_types_GlobalTime global_time, jzn_u64vec *out);
void jazz_node__node__codec__history_key_values(jz_slice branch_key, jazz_types_RowUuid row, jazz_types_GlobalTime seq, jazz_types_TxTime tx_time,
    jazz_types_NodeAlias alias, groove_ValueVec *out);
jz_err jazz_node__node__codec__row_author_value(jazz_types_AuthorSubject author, groove_Value *out);
jz_err jazz_node__node__codec__tx_kind_from_discriminant(u8 v, jazz_model_TxKind *out);
jz_err jazz_node__node__physical__NodeState__contribution_merge_from_storage_record(const struct jazz_node_NodeState *s, groove_OwnedRecord *r, void **out);
jz_err jazz_node__node__NodeState__validate_contribution_merge_operation_identities(const struct jazz_node_NodeState *s, const jazz_model_Transaction *tx);
bool jazz_node__node__physical__physical_version_table_id(const jz_str *storage_table, jazz_types_PhysicalTableId *out);
jz_str jazz_node__node__physical__physical_history_table_name(jazz_types_PhysicalTableId t);
typedef struct jazz_node_TouchedRow { jazz_types_PhysicalTableId table_id; jz_bytes branch_key; jazz_types_RowUuid row_uuid; } jazz_node_TouchedRow;
JZ_NVEC(jazz_node_TouchedRowVec, jazz_node_TouchedRow);
jz_err jazz_node__node__NodeState__load_tx_touched_rows(const struct jazz_node_NodeState *s, const groove_DatabaseBatch *b, jazz_types_TxTime t,
    jazz_types_NodeAlias alias, jazz_node_TouchedRowVec *out);
bool jazz_node__node__NodeState__cached_tx_versions(struct jazz_node_NodeState *s, jazz_model_TxId tx, jazz_node_VersionRowVec *out);

/* ======================= policy external mirrors ======================= */
struct jazz_node_RowProvenance; struct jazz_node_MergeableCommit; struct jazz_node_CurrentRowVec_;
#define JZN_AUTHOR_IS_SYSTEM(a) ((a).tag == JAZZ_AUTHOR_SYSTEM)
extern const jazz_types_AuthorSubject jazz_types__ids__AuthorSubject__SYSTEM;
jz_err jazz_types__ids__RowAuthor__from_session(jazz_types_AuthorSubject s, jazz_types_NodeUuid node, jazz_types_RowAuthor *out);
/* DeletionEvent as an Option discriminant (0 = None) */
enum { JZN_DELETION_NONE = 0, JZN_DELETION_DELETED = 1, JZN_DELETION_RESTORED = 2 };
u32 jazz_protocol__protocol__VersionRecord__deletion(const jazz_protocol_VersionRecord *v);
bool jazz_protocol__protocol__VersionRecord__deletes_row(const jazz_protocol_VersionRecord *v);
bool jazz_protocol__protocol__VersionRecord__optional_cell_at(const jazz_protocol_VersionRecord *v, u32 column, groove_Value *out);
const jz_btree *jazz_protocol__protocol__VersionRecord__authored_columns(const jazz_protocol_VersionRecord *v); /* NULL = None */
jazz_types_AuthorSubject jazz_protocol__protocol__VersionRecord__created_by(const jazz_protocol_VersionRecord *v);
jazz_types_AuthorSubject jazz_protocol__protocol__VersionRecord__updated_by(const jazz_protocol_VersionRecord *v);
u64 jazz_protocol__protocol__VersionRecord__created_at_ms(const jazz_protocol_VersionRecord *v);
u64 jazz_protocol__protocol__VersionRecord__updated_at_ms(const jazz_protocol_VersionRecord *v);
void jazz_protocol__protocol__VersionRecord__drop(jazz_protocol_VersionRecord *v);
jz_err jazz_node__node__codec__VersionRecordFromNode__from_commit(const struct jazz_node_MergeableCommit *c, const jazz_model_TableSchema *t,
    jazz_types_SchemaVersionId v, jazz_protocol_VersionRecord **out);
jazz_types_TxTime jazz_node__node__codec__VersionRow__created_at(const jazz_node_VersionRow *v);
jazz_types_TxTime jazz_node__node__codec__VersionRow__updated_at(const jazz_node_VersionRow *v);
jazz_types_AuthorSubject jazz_node__node__codec__VersionRow__created_by(const jazz_node_VersionRow *v);
jazz_types_AuthorSubject jazz_node__node__codec__VersionRow__updated_by(const jazz_node_VersionRow *v);
jz_err jazz_node__node__codec__VersionRow__cells(const jazz_node_VersionRow *v, const jazz_model_TableSchema *t, jz_btree *out);
jz_err jazz_node__node__codec__current_row_from_cells_with_explicit_provenance(const jazz_model_TableSchema *t, jazz_types_RowUuid row,
    const jz_btree *cells, struct jazz_node_RowProvenance provenance, bool has_projected, jazz_types_TxTime tx_time, jazz_types_NodeAlias alias,
    struct jazz_node_CurrentRow *out);
void jazz_node__node__codec__StoredTransaction__clone(jazz_node_StoredTransaction *dst, const jazz_node_StoredTransaction *src);
jz_err jazz_node__node__NodeState__version_record_from_row(struct jazz_node_NodeState *s, const jazz_node_VersionRow *v, jazz_protocol_VersionRecord **out);
jz_err jazz_node__node__NodeState__version_tx_id(const struct jazz_node_NodeState *s, const jazz_node_VersionRow *v, jazz_model_TxId *out);
bool jazz_node__node__NodeState__schema_version_for_alias(const struct jazz_node_NodeState *s, jazz_types_SchemaVersionAlias alias, jazz_types_SchemaVersionId *out);
void jazz_model__schema__TableSchema__clone(jazz_model_TableSchema *dst, const jazz_model_TableSchema *src);
void jazz_model__schema__TableSchema__drop(jazz_model_TableSchema *t);
/* query::Query construction / validation */
jazz_model_Query jazz_model__query__Query__from(jz_str table, jazz_model_OptStr alias);
void jazz_model__query__Query__drop_v(void *q);
jz_err jazz_model__query__Query__validate_with_schema_version(const jazz_model_Query *q, const jazz_model_JazzSchema *schema,
    jazz_types_SchemaVersionId v, jazz_model_ValidatedQuery **out);
jz_err jazz_model__query__ValidatedQuery__bind(const jazz_model_ValidatedQuery *v, jz_btree values, jazz_model_Binding **out);
void jazz_model__query__ValidatedQuery__drop(jazz_model_ValidatedQuery *v);
void jazz_model__query__Binding__drop(jazz_model_Binding *b);
jz_err jazz_node__node__query_eval__NodeState__query_row_visibility_for_link_physical_row(struct jazz_node_NodeState *s, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *b, jazz_model_DurabilityTier tier, jazz_types_AuthorSubject id, jazz_types_RowUuid row, struct jazz_node_CurrentRowVec_ *out);
/* query_engine::NormalizedRowSetShape (only the members policy reads; full layout in jazz_engine/query_engine/input.h) */
typedef struct jazz_engine_SourceRole { u8 tag; jz_str name; } jazz_engine_SourceRole;
typedef JZ_VEC(jazz_engine_SourceRole) jazz_engine_SourceRoleVec;
typedef struct jazz_engine_SourceId { jz_str table; jazz_engine_SourceRoleVec path; } jazz_engine_SourceId;
enum { JAZZ_ENGINE_EXPR_SOURCE = 0 };
typedef struct jazz_engine_RowSetExpr {
    u8 tag;
    union { struct { jazz_engine_SourceId source; u8 visibility; } source; /* other variants elided */ } u;
} jazz_engine_RowSetExpr;
typedef struct jazz_engine_NormalizedRowSetShape jazz_engine_NormalizedRowSetShape;
const jz_btree *jazz_engine__query_engine__input__NormalizedRowSetShape__nodes(const jazz_engine_NormalizedRowSetShape *s);             /* RowSetNodeId -> RowSetExpr */
const jz_btree *jazz_engine__query_engine__input__NormalizedRowSetShape__auxiliary_sources(const jazz_engine_NormalizedRowSetShape *s); /* set<SourceId> */
/* query_eval::read_sources::TransactionWriteOverlay / TransactionOverlayTable */
typedef struct jazz_node_TransactionOverlayTable jazz_node_TransactionOverlayTable;
typedef struct jazz_node_OverlayTableKey { jazz_types_SchemaVersionId schema; jz_str table; } jazz_node_OverlayTableKey;
typedef struct jazz_node_OverlayReads { bool poisoned; jz_btree set; /* set<(SchemaVersionId, String)> */ } jazz_node_OverlayReads;
typedef struct jazz_node_TransactionWriteOverlay {
    jz_btree *tables;           /* Arc<BTreeMap<(SchemaVersionId, String), Arc<TransactionOverlayTable>>> */
    bool has_excluded; jazz_types_SchemaVersionId excluded_schema; jz_str excluded_table; jazz_types_RowUuid excluded_row;
    jazz_node_OverlayReads *reads; /* Option<Arc<Mutex<..>>> */
    jazz_model_DurabilityTier evidence_tier;
} jazz_node_TransactionWriteOverlay;
void jazz_node_TransactionWriteOverlay_drop(jazz_node_TransactionWriteOverlay *o);
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__Default__default(void);
jazz_node_TransactionOverlayTable *jazz_node__node__query_eval__read_sources__TransactionOverlayTable__new(jz_btree rows);
void jazz_node__node__query_eval__read_sources__TransactionOverlayTable__release(jazz_node_TransactionOverlayTable *t);
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__from_tables(jz_btree *tables);
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__accepted_state(void);
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__committed_view(const jazz_node_TransactionWriteOverlay *o);
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__excluding(const jazz_node_TransactionWriteOverlay *o, jazz_types_SchemaVersionId schema, jz_slice table, jazz_types_RowUuid row);
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__recording(const jazz_node_TransactionWriteOverlay *o, jazz_node_OverlayReads *reads);
u32 jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__rows_in(const jazz_node_TransactionWriteOverlay *o, const jz_btree *tables);
/* query_eval::authorization / bindings */
jazz_model_Query jazz_node__node__query_eval__bindings__authorization_query_from_read_policy(const jazz_model_TableSchema *table);
jz_err jazz_node__node__query_eval__authorization__NodeState__write_policy_query_allows_current_row(struct jazz_node_NodeState *self, const jazz_model_Query *policy, jazz_types_RowUuid row_uuid, jazz_types_AuthorSubject identity, bool *out);
jz_err jazz_node__node__query_eval__authorization__NodeState__write_policy_query_allows_candidate_with_provenance_for_schema(struct jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jazz_model_TableSchema *table, const jazz_model_Query *policy, jazz_types_RowUuid row_uuid, const jz_btree *cells, jazz_types_AuthorSubject identity, bool insert_candidate, struct jazz_node_RowProvenance provenance, bool *out);
jz_err jazz_node__node__query_eval__authorization__NodeState__write_policy_query_allows_candidate_over_transaction(struct jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jazz_model_TableSchema *table, const jazz_model_Query *policy, jazz_types_RowUuid row_uuid, const jz_btree *cells, jazz_types_AuthorSubject identity, bool insert_candidate, struct jazz_node_RowProvenance provenance, const jazz_node_TransactionWriteOverlay *overlay, bool *out);
jz_err jazz_node__node__query_eval__authorization__NodeState__read_policy_query_allows_candidate_with_provenance_for_schema(struct jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jazz_model_TableSchema *table, const jazz_model_Query *policy, jazz_types_RowUuid row_uuid, const jz_btree *cells, jazz_types_AuthorSubject identity, struct jazz_node_RowProvenance provenance, bool *out);

/* ======================= open_tx external mirrors ======================= */
struct jazz_model_BranchSelector { jz_btree values; /* String -> BranchColumnValue */ };
jazz_model_BranchSelector *jazz_model__model__branch__BranchSelector__default(void);
jazz_model_BranchSelector *jazz_model__model__branch__BranchSelector__clone(const jazz_model_BranchSelector *s);
bool jazz_model__model__branch__BranchSelector__eq(const jazz_model_BranchSelector *a, const jazz_model_BranchSelector *b);
int jazz_model__model__branch__BranchSelector__cmp(const jazz_model_BranchSelector *a, const jazz_model_BranchSelector *b);
void jazz_model__model__branch__BranchSelector__drop(jazz_model_BranchSelector *s);
jazz_model_BranchKey *jazz_model__model__branch__BranchKey__default(void);
jazz_model_BranchKey *jazz_model__model__branch__BranchKey__clone(const jazz_model_BranchKey *k);
void jazz_model__tx__BranchViewCopyEvidence__clone(jazz_model_BranchViewCopyEvidence *dst, const jazz_model_BranchViewCopyEvidence *src);
bool jazz_model__tx__BranchViewCopyEvidence__eq(const jazz_model_BranchViewCopyEvidence *a, const jazz_model_BranchViewCopyEvidence *b);
void jazz_model__tx__BranchViewCopyEvidence__drop(jazz_model_BranchViewCopyEvidence *e);
void jazz_model__tx__Snapshot__clone(jazz_model_Snapshot *dst, const jazz_model_Snapshot *src);
void jazz_model__tx__RowRead__clone(void *dst, const void *src);
void jazz_model__tx__AbsentRead__clone(void *dst, const void *src);
void jazz_model__tx__PredicateRead__clone(void *dst, const void *src);
/* BranchWriteIntent / BranchWriteOperation / ContributionMergeProvenance */
enum { JAZZ_BWO_EXACT_HEAD_INSERT, JAZZ_BWO_EXACT_HEAD_UPDATE, JAZZ_BWO_VIEW_UPDATE_COPY };
typedef struct jazz_model_BranchWriteOperation { u32 tag; jazz_model_BranchViewCopyEvidence copy; /* ViewUpdateCopy */ } jazz_model_BranchWriteOperation;
typedef struct jazz_model_BranchWriteIntent {
    u8 version; jazz_types_PhysicalTableId physical_table_id; jazz_types_SchemaVersionId authored_schema;
    jazz_types_RowUuid row_uuid; jazz_model_BranchKey *head; jazz_model_BranchWriteOperation operation;
} jazz_model_BranchWriteIntent;
JZ_NVEC(jazz_model_BranchWriteIntentVec, jazz_model_BranchWriteIntent);
JZ_NVEC(jazz_model_BranchViewCopyEvidenceVec, jazz_model_BranchViewCopyEvidence);
bool jazz_model__tx__BranchWriteOperation__eq(const jazz_model_BranchWriteOperation *a, const jazz_model_BranchWriteOperation *b);
void jazz_model__tx__BranchWriteIntent__drop(void *p);
typedef struct jazz_model_ContributionSubstitution jazz_model_ContributionSubstitution;
JZ_NVEC(jazz_model_ContributionSubstitutionVec, jazz_model_ContributionSubstitution);
typedef struct jazz_model_ContributionMergeProvenance {
    jazz_model_BranchKey *source, *target;
    jazz_model_ContributionSubstitutionVec substitutions;
    jazz_model_BranchViewCopyEvidenceVec branch_view_copies;
    jazz_model_BranchWriteIntentVec branch_write_intents;
} jazz_model_ContributionMergeProvenance;
/* groove large values */
#define GROOVE_INLINE_VALUE_MAX_BYTES 1024u
void groove__large_values__StagedLargeValue__clone(void *dst, const void *src);
void groove__large_values__StagedLargeValue__drop(void *p);
/* query::ValidatedQuery / Binding accessors */
jz_err jazz_model__query__Query__validate(const jazz_model_Query *q, const jazz_model_JazzSchema *schema, jazz_model_ValidatedQuery **out);
jz_uuid jazz_model__query__ValidatedQuery__shape_id(const jazz_model_ValidatedQuery *v);
const jazz_model_Query *jazz_model__query__ValidatedQuery__query(const jazz_model_ValidatedQuery *v);
jz_uuid jazz_model__query__Binding__binding_id(const jazz_model_Binding *b);
const jz_btree *jazz_model__query__Binding__values(const jazz_model_Binding *b);
/* query_eval::narrowed_reads */
typedef struct jazz_node_NarrowedSourceRead { jazz_model_ValidatedQuery *shape; jazz_model_Binding *binding; } jazz_node_NarrowedSourceRead;
typedef struct jazz_node_ExclusiveSourceReads { jz_btree reads; /* SourceId -> NarrowedSourceRead */ jz_btree payload; /* BTreeSet<SourceId> */ } jazz_node_ExclusiveSourceReads;
void jazz_node_NarrowedSourceRead_clone(void *dst, const void *src);
void jazz_node_NarrowedSourceRead_drop(void *r);
extern const jz_btree_desc jazz_engine_SourceId_narrowed_read_desc; /* SourceId -> NarrowedSourceRead */
extern const jz_btree_desc jazz_engine_SourceId_set_desc;           /* BTreeSet<SourceId> */
void jazz_engine__query_engine__input__SourceId__clone(void *dst, const void *src);
/* NodeState methods in state (other units) */
jz_err jazz_node__node__NodeState__require_catalogue_ready(const struct jazz_node_NodeState *s);
jazz_types_TxTime jazz_node__node__NodeState__tx_time_high_water(const struct jazz_node_NodeState *s);
jz_err jazz_node__node__NodeState__mint_tx_time(struct jazz_node_NodeState *s, u64 now_ms, jazz_types_TxTime *out);
jz_err jazz_node__node__NodeState__ensure_large_value_stages_current(const struct jazz_node_NodeState *s, const jz_btree *ids /* set<StagedLargeValueId> */);
jz_err jazz_node__node__NodeState__prepare_and_stage_large_scalar(struct jazz_node_NodeState *s, groove_Value *value, u8 semantic_kind,
    bool *has_staged, groove_StagedLargeValue *out);
jz_err jazz_node__node__NodeState__evict_staged_large_value(const struct jazz_node_NodeState *s, groove_StagedLargeValueId id, bool *out);
jz_err jazz_node__node__NodeState__visible_current_cells_in_branch(struct jazz_node_NodeState *s, const jz_str *table, const jazz_model_BranchSelector *branch,
    jazz_types_RowUuid row_uuid, bool *some, jz_btree *out);
jz_err jazz_node__node__physical__NodeState__physical_table_id_for_schema(const struct jazz_node_NodeState *s, jazz_types_SchemaVersionId v, const jz_str *table,
    jazz_types_PhysicalTableId *out);
jz_err jazz_node__node__ingest__NodeState__predicate_read_is_degenerate_whole_table(struct jazz_node_NodeState *s, const jazz_model_PredicateRead *p, bool *out);
jz_err jazz_node__node__ingest__NodeState__shape_predicate_outputs_differ(struct jazz_node_NodeState *s, const jazz_model_PredicateRead *p,
    const jazz_model_Snapshot *base, const jazz_model_Snapshot *current /* NULL = None */, bool *out);
/* codec positional helpers: Vec<Option<Value>> is an array of Value* (NULL = None) */
bool jazz_node__node__codec__current_version_index(jazz_node_VersionRow *const *versions, const usize *candidates, u32 n,
    const struct jazz_node_NodeAliases *aliases, usize *out);
jz_err jazz_node__node__codec__positional_cells_from_map(const jazz_model_TableSchema *t, const jz_btree *cells, groove_ValueVec *out);
void jazz_node__node__codec__cells_from_positional(const jazz_model_TableSchema *t, const groove_Value *const *cells, u32 n, jz_btree *out);
jz_err jazz_node__node__codec__current_row_from_positional_cells(const jazz_model_TableSchema *t, jazz_types_RowUuid row,
    const groove_ValueVec *cells, struct jazz_node_CurrentRow *out);
jz_err jazz_protocol__protocol__VersionRecord__encode(const jazz_model_TableSchema *table, jazz_types_SchemaVersionId sv, jazz_types_RowUuid row_uuid,
    jazz_types_AuthorSubject created_by, u64 created_at_ms, jazz_types_AuthorSubject updated_by, u64 updated_at_ms,
    const groove_ValueVec *cells_by_position, bool has_deletion, jazz_model_DeletionEvent deletion, jazz_protocol_VersionRecord **out);
jazz_protocol_VersionRecord *jazz_protocol__protocol__VersionRecord__clone(const jazz_protocol_VersionRecord *v);
jazz_model_Transaction *jazz_model__tx__Transaction__clone(const jazz_model_Transaction *t);
void jazz_model__tx__Transaction__drop(jazz_model_Transaction *t);
/* SyncMessage::CommitUnit { tx, versions } (moves both) */
jazz_protocol_SyncMessage *jazz_protocol__protocol__SyncMessage__CommitUnit(jazz_model_Transaction *tx, jazz_protocol_VersionRecord **versions, u32 n);
/* groove index scan */
jz_err groove__db__primary_storage__Database__index_scan_raw(groove_Database *db, const jz_str *table, const char *index,
    const groove_Value *prefix, u32 n, groove_RawRowVec *out);


/* ======================= views external mirrors ======================= */
JZ_NVEC(jazz_protocol_VersionRecordVec, jazz_protocol_VersionRecord *);
enum { JAZZ_SRU_SNAPSHOT = 0, JAZZ_SRU_DELTA = 1, JAZZ_SRU_CATCH_UP = 2 };
/* Snapshot{revision, rows=added} | Delta{predecessor, revision, adds=added, removes=removed} | CatchUp{predecessor, revision, changed=added, left=removed} */
struct jazz_protocol_SupportingRowsUpdate {
    u32 tag;
    u8 predecessor[16];
    u8 revision[16];
    jazz_protocol_SupportingRowVec added;
    jazz_protocol_SupportingRowVec removed;
};
void jazz_protocol__protocol__SupportingRowsUpdate__snapshot(jazz_protocol_SupportingRowsUpdate *out, jazz_protocol_SupportingRowVec rows);
void jazz_protocol__protocol__SupportingRowsUpdate__drop(jazz_protocol_SupportingRowsUpdate *u);
bool jazz_protocol__protocol__SupportingRow__is_wire_valid(const jazz_protocol_SupportingRow *r);
bool jazz_protocol__protocol__SupportingRow__eq(const jazz_protocol_SupportingRow *a, const jazz_protocol_SupportingRow *b);
enum { JAZZ_KSD_FAST = 0, JAZZ_KSD_FAST_WITH_AUTH = 1, JAZZ_KSD_RESERVED2 = 2, JAZZ_KSD_WATERMARK = 3 };
struct jazz_protocol_KnownStateDeclaration {
    u32 tag; u32 completeness; jazz_types_GlobalTime position;
    bool has_authorization_progress; u64 authorization_progress;
    u8 supporting_revision[16];
};
/* VersionBundleRef<'a>: Copy view over a bundle */
typedef struct jazz_protocol_VersionBundleRef {
    const jazz_model_Transaction *tx;
    jazz_protocol_VersionRecord *const *versions; u32 n_versions;
    u32 scope;
    const jazz_model_Fate *fate;
    bool has_global_time; jazz_types_GlobalTime global_time;
    jazz_model_DurabilityTier durability;
} jazz_protocol_VersionBundleRef;
JZ_NVEC(jazz_protocol_VersionBundleRefVec, jazz_protocol_VersionBundleRef);
JZ_NVEC(jazz_protocol_VersionCarrierPtrVec, jazz_protocol_VersionCarrier *);
jz_err jazz_protocol__protocol__VersionCarrier__bundle_refs(const jazz_protocol_VersionCarrier *c, jazz_protocol_VersionBundleRefVec *out);
void jazz_protocol__protocol__VersionBundleRef__to_owned_bundle(jazz_protocol_VersionBundleRef r, jazz_protocol_VersionBundle *out);
jazz_protocol_VersionBundleRef jazz_protocol__protocol__VersionBundle__as_ref(const jazz_protocol_VersionBundle *b);
void jazz_protocol__protocol__VersionBundle__clone(void *dst, const void *src);
jz_err jazz_protocol__protocol__build_version_carriers_from_singletons(jazz_protocol_VersionBundleVec bundles, jazz_protocol_VersionCarrierPtrVec *out);
typedef struct jazz_protocol_PeerPayloadInventory {
    jazz_model_TxIdVec complete_tx_payloads;
    bool has_authorization_progress; u64 authorization_progress;
    bool opening_pending;
} jazz_protocol_PeerPayloadInventory;
typedef struct jazz_protocol_ViewUpdatePayload {
    jazz_protocol_SubscriptionKey subscription;
    jazz_types_GlobalTime settled_through;
    jazz_protocol_VersionCarrierPtrVec version_carriers;
    jazz_protocol_PeerPayloadInventory peer_payload_inventory;
    jazz_protocol_SupportingRowsUpdate supporting_rows;
} jazz_protocol_ViewUpdatePayload;
jazz_protocol_SyncMessage *jazz_protocol__protocol__SyncMessage__ViewUpdate(jazz_protocol_ViewUpdatePayload p);
bool jazz_protocol__protocol__VersionRecord__eq(const jazz_protocol_VersionRecord *a, const jazz_protocol_VersionRecord *b);
int jazz_protocol__protocol__VersionRecord__cmp_ptr(const void *a, const void *b, void *ctx); /* on VersionRecord* elements (Ord) */
enum { JAZZ_RME_TYPED_ROW = 3 };
typedef struct jazz_protocol_RealRowMemberEntry jazz_protocol_RealRowMemberEntry;
/* as_row(): Some((table, row_uuid, content tx)) */
bool jazz_protocol__protocol__ResultMemberEntry__as_row_entry(const jazz_protocol_ResultMemberEntry *m, const jz_str **table, jazz_types_RowUuid *row, jazz_model_TxId *tx);
const jazz_protocol_RealRowMemberEntry *jazz_protocol__protocol__ResultMemberEntry__as_real_row(const jazz_protocol_ResultMemberEntry *m);
const jz_str *jazz_protocol__protocol__RealRowMemberEntry__table(const jazz_protocol_RealRowMemberEntry *r);
jazz_types_RowUuid jazz_protocol__protocol__RealRowMemberEntry__row_uuid(const jazz_protocol_RealRowMemberEntry *r);
bool jazz_protocol__protocol__RealRowMemberEntry__settle_position(const jazz_protocol_RealRowMemberEntry *r, jazz_types_GlobalTime *out);
const jazz_types_OutputOccurrenceId *jazz_protocol__protocol__RealRowMemberEntry__occurrence_id(const jazz_protocol_RealRowMemberEntry *r); /* NULL = None */
/* model */
u32 jazz_model__tx__Transaction__n_total_writes(const jazz_model_Transaction *t);
void jazz_model__tx__Transaction__set_n_total_writes(jazz_model_Transaction *t, u32 n);
bool jazz_model__tx__Transaction__eq(const jazz_model_Transaction *a, const jazz_model_Transaction *b);
const jz_str *jazz_model__tx__Transaction__user_metadata_json(const jazz_model_Transaction *t); /* NULL = None */
void *jazz_model__tx__Transaction__contribution_merge_clone(const jazz_model_Transaction *t);  /* NULL = None */
bool jazz_model__tx__Fate__eq(const jazz_model_Fate *a, const jazz_model_Fate *b);
void jazz_model__tx__Fate__clone(jazz_model_Fate *dst, const jazz_model_Fate *src);
jz_err jazz_model__model__branch__BranchKey__from_canonical_bytes(jz_slice bytes, jazz_model_BranchKey **out);
jazz_types_SchemaVersionId jazz_model__query__ValidatedQuery__schema_version(const jazz_model_ValidatedQuery *v);
/* groove */
bool groove__db__facade__DatabaseBatch__is_empty(const groove_DatabaseBatch *b);
jz_err groove__db__facade__Database__subscribe_one_sink(groove_Database *db, groove_GraphBuilder *source, groove_Subscription **out);
/* node::maintained_subscription_view (other unit) */
typedef struct jazz_node_MaintainedSubscriptionView jazz_node_MaintainedSubscriptionView;
const jz_btree *jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__physical_tables(const jazz_node_MaintainedSubscriptionView *m); /* String -> GlobalPhysicalTableId */
jazz_protocol_SupportingRowVec jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__supporting_rows_cloned(const jazz_node_MaintainedSubscriptionView *m);
bool jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__uses_storage_backed_result_materialization(const jazz_node_MaintainedSubscriptionView *m);
jazz_node_VersionRowVec jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__versions_by_tx(const jazz_node_MaintainedSubscriptionView *m, jazz_model_TxId tx);
/* (content winner, deletion winner), each owned, NULL = None */
void jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__replacement_for(const jazz_node_MaintainedSubscriptionView *m, const jz_str *table,
    jazz_types_RowUuid row, jazz_node_VersionRow **content, jazz_node_VersionRow **deletion);
/* node (other units) */
bool jazz_node__node__codec__VersionRow__is_deleted(const jazz_node_VersionRow *v);
jazz_types_SchemaVersionId jazz_node__node__codec__VersionRow__schema_version(const jazz_node_VersionRow *v);
jz_err jazz_node__node__codec__VersionRow__cell(const jazz_node_VersionRow *v, const jazz_model_TableSchema *t, const jz_str *column, bool *some, groove_Value *out);
const groove_RecordDescriptor *jazz_model__schema__TableSchema__history_storage_record_schema(const jazz_model_TableSchema *t);
#define JAZZ_DELETION_COLUMN_NAME "$deletion"
bool jazz_node__node__codec__known_transaction_payload_matches(const jazz_model_Transaction *a, const jazz_model_Transaction *b);
jazz_model_Transaction *jazz_node__node__codec__transaction_without_permission_subject(const jazz_model_Transaction *tx);
jz_err jazz_node__node__ingest__validate_received_view_bundle_global_time_durability(bool has_gt, jazz_types_GlobalTime gt, jazz_model_DurabilityTier durability);
JZ_NVEC(jazz_node_GlobalTimeTxVec, struct jazz_node_GlobalTimeTx);
struct jazz_node_GlobalTimeTx { jazz_types_GlobalTime gt; jazz_model_TxId tx; };
JZ_NVEC(jazz_node_RejectedTransactionVec, jazz_model_RejectedTransaction *);
jazz_model_TxId jazz_model__tx__RejectedTransaction__tx_id(const jazz_model_RejectedTransaction *r);
struct jazz_node_NodeState;
jz_err jazz_node__node__ingest__NodeState__ingest_known_transaction(struct jazz_node_NodeState *s, jazz_model_Transaction *tx, jazz_protocol_VersionRecordVec versions,
    jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt, jazz_model_DurabilityTier durability);
jz_err jazz_node__node__ingest__NodeState__ingest_transaction_fragment_without_current_indexes(struct jazz_node_NodeState *s, jazz_model_Transaction *tx,
    jazz_protocol_VersionRecordVec versions, jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt, jazz_model_DurabilityTier durability);
jz_err jazz_node__node__ingest__NodeState__ingest_view_scoped_transaction_with_current_indexes(struct jazz_node_NodeState *s, jazz_model_Transaction *tx,
    jazz_protocol_VersionRecordVec versions, jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt, jazz_model_DurabilityTier durability);
jz_err jazz_node__node__ingest__NodeState__stage_known_transaction(struct jazz_node_NodeState *s, groove_DatabaseBatch *batch, jazz_model_Transaction *tx,
    jazz_protocol_VersionRecordVec versions, jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt, jazz_model_DurabilityTier durability,
    jazz_node_GlobalTimeTxVec *staged_global_times, jazz_node_VersionRowVec *staged_content_versions, jazz_node_RejectedTransactionVec *staged_rejections);
jz_err jazz_node__node__ingest__NodeState__stage_view_scoped_transaction_with_current_indexes(struct jazz_node_NodeState *s, groove_DatabaseBatch *batch,
    jazz_model_Transaction *tx, jazz_protocol_VersionRecordVec versions, jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt, jazz_model_DurabilityTier durability,
    jazz_node_GlobalTimeTxVec *staged_global_times, jazz_node_VersionRowVec *staged_content_versions, jazz_node_RejectedTransactionVec *staged_rejections);
jz_err jazz_node__node__ingest__NodeState__ingest_reset_view_bundle_refs_in_bulk(struct jazz_node_NodeState *s, const jazz_protocol_VersionBundleRef *bundles, u32 n,
    const jz_btree *preflight_persisted_tx_ids /* NULL = None */, jz_btree *out /* set<TxId> */);
jz_err jazz_node__node__ingest__NodeState__validate_view_payload_versions_prepared(const struct jazz_node_NodeState *s, jazz_protocol_VersionRecord *const *versions, u32 n,
    jz_btree *descriptors);
extern const jz_btree_desc jazz_node__node__ingest__PREPARED_DESCRIPTORS_DESC;
jz_err jazz_node__node__state__catalogue__NodeState__ensure_node_alias(struct jazz_node_NodeState *s, jazz_types_NodeUuid node, jazz_types_NodeAlias *out);
jz_err jazz_node__node__state__catalogue__NodeState__ensure_schema_version_alias(struct jazz_node_NodeState *s, jazz_types_SchemaVersionId v, jazz_types_SchemaVersionAlias *out);
jz_err jazz_node__node__state__commit__NodeState__apply_node_batch(struct jazz_node_NodeState *s, groove_DatabaseBatch *b, groove_AppliedBatch **out);
jz_err jazz_node__node__NodeState__admit_contribution_merge_for_storage(const struct jazz_node_NodeState *s, const jazz_model_Transaction *tx, groove_Value *out);
jz_err jazz_node__node__query_eval__subscriptions__NodeState__authority_result_key_for_subscription(const struct jazz_node_NodeState *s, jazz_protocol_SubscriptionKey sub, jazz_protocol_AuthorityResultKey *out);
jz_err jazz_node__node__query_eval__subscriptions__NodeState__binding_view_key_for_subscription(const struct jazz_node_NodeState *s, jazz_protocol_SubscriptionKey sub, jazz_protocol_BindingViewKey *out);
jz_err jazz_node__node__query_eval__subscriptions__NodeState__allocate_authoritative_reset_generation(struct jazz_node_NodeState *s, u64 *out);
void jazz_node__node__NodeState__clear_settled_result_view(struct jazz_node_NodeState *s, jazz_protocol_AuthorityResultKey key);
void jazz_node__node__NodeState__record_peer_payload_inventory_missing_fallback(struct jazz_node_NodeState *s);
jz_err jazz_node__node__NodeState__begin_initial_sync_flush_cadence(struct jazz_node_NodeState *s);
jz_err jazz_node__node__NodeState__finish_initial_sync_flush_cadence(struct jazz_node_NodeState *s);
bool jazz_node__node__query_eval__subscription_watermarks__NodeState__watermark_catch_up_view(const struct jazz_node_NodeState *s, const jazz_model_ValidatedQuery *shape);
jz_err jazz_node__node__query_eval__subscription_watermarks__NodeState__persist_subscription_watermark(struct jazz_node_NodeState *s, const jazz_protocol_AuthorityResultKey *key,
    jazz_types_GlobalTime settled_through, const u8 revision[16]);
const jazz_model_ValidatedQuery *jazz_node__node__query_eval__NodeState__registered_shape(const struct jazz_node_NodeState *s, jazz_model_ShapeId shape_id); /* NULL = None */
jz_err jazz_node__node__query_eval__NodeState__compiled_covered_input_sources_for_subscription(const struct jazz_node_NodeState *s, jazz_protocol_SubscriptionKey sub, jz_btree *out);
jz_err jazz_node__node__query_eval__NodeState__query_rows_for_link_physical_row(struct jazz_node_NodeState *s, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_types_RowUuid row_uuid, struct jazz_node_CurrentRowVec_ *out);
bool jazz_node__node__NodeState__current_row_tx_id(struct jazz_node_NodeState *s, const struct jazz_node_CurrentRow *row, jazz_model_TxId *out);
jz_err jazz_node__node__NodeState__local_deletion_winner_tx_id(struct jazz_node_NodeState *s, const jz_str *table, jazz_types_RowUuid row, bool *has, jazz_model_TxId *out);
jz_err jazz_node__node__physical__NodeState__physical_table_id_for_version(struct jazz_node_NodeState *s, const jazz_node_VersionRow *v, jazz_types_PhysicalTableId *out);
jz_err jazz_node__node__physical__NodeState__physical_history_source_graph(const struct jazz_node_NodeState *s, jazz_types_SchemaVersionId v,
    const jz_str *table, groove_GraphBuilder **out);
jz_err jazz_node__node__physical__NodeState__authored_columns_for_version(struct jazz_node_NodeState *s, const jazz_node_VersionRow *v, bool *some, jz_btree *out);
bool jazz_node__debug_env__covered_input_trace(void);
void jazz_protocol__protocol__ResultMemberEntry__drop(void *p);
void jazz_node__debug_env__eprintln(const jz_str *line);

#endif
