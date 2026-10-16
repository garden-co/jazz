/* jazz_model.h: types and functions of the jazz_model crate
 * (crates/jazz/layers/model/src), napkin C port. */
#ifndef JAZZ_MODEL_H
#define JAZZ_MODEL_H
#include "jz_rt.h"
#include "_ext_model.h"

/* ============================================================ common */
typedef JZ_VEC(jz_str) jazz_model_StrVec;
typedef struct jazz_model_OptStr { bool some; jz_str s; } jazz_model_OptStr;
typedef JZ_VEC(u8) jazz_model_U8Vec;

/* error code space for this crate */
enum {
    JAZZ_MODEL_ERR_BASE = 0x2000,
    JAZZ_MODEL_ERR_SERDE,            /* serde custom / invalid type errors */
    JAZZ_MODEL_ERR_STRING,           /* Result<_, String> */
    JAZZ_MODEL_ERR_SCHEMA_CONVERSION,/* SchemaConversionError */
    JAZZ_MODEL_ERR_BRANCH_CODEC,     /* BranchCodecError (+variant) */
    JAZZ_MODEL_ERR_BRANCH_CODEC_ENVELOPE,
    JAZZ_MODEL_ERR_BRANCH_CODEC_UNSUPPORTED,
    JAZZ_MODEL_ERR_BRANCH_CODEC_MISMATCH,
    JAZZ_MODEL_ERR_BRANCH_CODEC_GROOVE,
    JAZZ_MODEL_ERR_JSON_SCHEMA_INVALID,
    JAZZ_MODEL_ERR_JSON_SCHEMA_MISMATCH,
    JAZZ_MODEL_ERR_JSON_SCHEMA_UNAVAILABLE,
    JAZZ_MODEL_ERR_POLICY_LIMIT,
    JAZZ_MODEL_ERR_RELATION_WIRE_TOO_LARGE,
    JAZZ_MODEL_ERR_RELATION_WIRE_POSTCARD,
    JAZZ_MODEL_ERR_RELATION_WIRE_NON_FINITE,
    JAZZ_MODEL_ERR_RELATION_WIRE_INVALID,
    JAZZ_MODEL_ERR_HEX,
    JAZZ_MODEL_ERR_QUERY = 0x2100,   /* + QueryError variant index */
};

/* ============================================================ public_api::types::schema */
typedef const jz_str *jazz_model_TableName;  /* Intern<String> */
typedef const jz_str *jazz_model_ColumnName; /* Intern<String> */
typedef JZ_VEC(jazz_model_ColumnName) jazz_model_ColumnNameVec;
typedef JZ_VEC(jazz_model_ColumnNameVec) jazz_model_ColumnNameVecVec;

jazz_model_TableName jazz_model__model__public_api__types__schema__TableName__new(jz_str name);
jazz_model_TableName jazz_model__model__public_api__types__schema__TableName__from(const char *p, u32 n);
jazz_model_ColumnName jazz_model__model__public_api__types__schema__ColumnName__new(jz_str name);
jazz_model_ColumnName jazz_model__model__public_api__types__schema__ColumnName__from(const char *p, u32 n);
int jazz_model__model__public_api__types__schema__TableName__Ord__cmp(const void *a, const void *b, void *ctx);
void jazz_model__model__public_api__types__schema__TableName__Debug__fmt(jz_str *out, jazz_model_TableName n);
void jazz_model__model__public_api__types__schema__ColumnName__Debug__fmt(jz_str *out, jazz_model_ColumnName n);

typedef struct jazz_model_public_ColumnType jazz_model_public_ColumnType;
typedef struct jazz_model_ColumnDescriptor jazz_model_ColumnDescriptor;
typedef struct jazz_model_RowDescriptor jazz_model_RowDescriptor;
typedef struct jazz_model_public_Value jazz_model_public_Value;
typedef JZ_VEC(jazz_model_ColumnDescriptor) jazz_model_ColumnDescriptorVec;

typedef struct jazz_model_EnumCaseDescriptor { jz_str name; jazz_model_ColumnDescriptorVec fields; } jazz_model_EnumCaseDescriptor;
typedef JZ_VEC(jazz_model_EnumCaseDescriptor) jazz_model_EnumCaseDescriptorVec;

enum {
    JAZZ_CT_INTEGER, JAZZ_CT_BIGINT, JAZZ_CT_BOOLEAN, JAZZ_CT_TEXT, JAZZ_CT_ENUM, JAZZ_CT_SCALAR_ENUM,
    JAZZ_CT_CATALOGUE_ENUM_PAYLOAD, JAZZ_CT_ENUM_PAYLOAD, JAZZ_CT_TIMESTAMP, JAZZ_CT_DOUBLE, JAZZ_CT_UUID,
    JAZZ_CT_TRANSACTION_ID, JAZZ_CT_BYTEA, JAZZ_CT_JSON, JAZZ_CT_ARRAY, JAZZ_CT_ROW, JAZZ_CT_COUNT
};
struct jazz_model_public_ColumnType {
    u8 tag;
    jz_str name;                               /* ScalarEnum / CatalogueEnumPayload */
    jazz_model_StrVec variants;                /* Enum / ScalarEnum */
    jazz_model_EnumCaseDescriptorVec cases;    /* CatalogueEnumPayload / EnumPayload */
    jz_json *schema;                           /* Json: Option<serde_json::Value> */
    jazz_model_public_ColumnType *element;     /* Array: Box<ColumnType> */
    jazz_model_RowDescriptor *columns;         /* Row: Box<RowDescriptor> */
};

enum { JAZZ_MERGE_COUNTER = 0, JAZZ_MERGE_GSET = 1 };
struct jazz_model_ColumnDescriptor {
    jazz_model_ColumnName name;
    jazz_model_public_ColumnType column_type;
    bool nullable;
    jazz_model_TableName references;     /* NULL = None */
    jazz_model_public_Value *default_;   /* NULL = None */
    bool has_merge_strategy; u8 merge_strategy;
};
struct jazz_model_RowDescriptor { jazz_model_ColumnDescriptorVec columns; };

/* ============================================================ public_api::types::value */
enum {
    JAZZ_PV_INTEGER, JAZZ_PV_BIGINT, JAZZ_PV_DOUBLE, JAZZ_PV_BOOLEAN, JAZZ_PV_TEXT, JAZZ_PV_TIMESTAMP,
    JAZZ_PV_UUID, JAZZ_PV_TRANSACTION_ID, JAZZ_PV_BYTEA, JAZZ_PV_ARRAY, JAZZ_PV_ROW, JAZZ_PV_ENUM, JAZZ_PV_NULL,
    JAZZ_PV_COUNT
};
typedef JZ_VEC(jazz_model_public_Value) jazz_model_public_ValueVec;
struct jazz_model_public_Value {
    u8 tag;
    union {
        i32 integer; i64 bigint; f64 dbl; bool boolean; jz_str text; u64 timestamp;
        jazz_types_ObjectId uuid; u8 txid[16]; jz_bytes bytea;
        jazz_model_public_ValueVec array;
        struct { jazz_types_ObjectId id; /* NULL = None */ jazz_model_public_ValueVec values; } row;
        struct { jz_str case_; jazz_model_public_ValueVec values; } enum_;
    } u;
};

/* ============================================================ public_api::relation_ir */
typedef struct jazz_model_RelColumnRef { jazz_model_OptStr scope; jz_str column; } jazz_model_RelColumnRef;
enum { JAZZ_ROWID_CURRENT = 0, JAZZ_ROWID_OUTER = 1, JAZZ_ROWID_FRONTIER = 2 };
enum { JAZZ_RVR_LITERAL = 0, JAZZ_RVR_SESSION_REF, JAZZ_RVR_OUTER_COLUMN, JAZZ_RVR_ROW_ID };
typedef struct jazz_model_RelValueRef {
    u8 tag;
    jazz_model_public_Value literal;
    jazz_model_StrVec session_ref;
    jazz_model_RelColumnRef outer_column;
    u8 row_id;
} jazz_model_RelValueRef;
typedef JZ_VEC(jazz_model_RelValueRef) jazz_model_RelValueRefVec;
enum { JAZZ_PCMP_EQ, JAZZ_PCMP_NE, JAZZ_PCMP_LT, JAZZ_PCMP_LE, JAZZ_PCMP_GT, JAZZ_PCMP_GE };
enum {
    JAZZ_RPE_CMP, JAZZ_RPE_IS_NULL, JAZZ_RPE_IS_NOT_NULL, JAZZ_RPE_IN, JAZZ_RPE_CONTAINS, JAZZ_RPE_ENUM_MATCH,
    JAZZ_RPE_AND, JAZZ_RPE_OR, JAZZ_RPE_NOT, JAZZ_RPE_TRUE, JAZZ_RPE_FALSE
};
typedef struct jazz_model_RelPredicateExpr jazz_model_RelPredicateExpr;
typedef JZ_VEC(jazz_model_RelPredicateExpr) jazz_model_RelPredicateExprVec;
struct jazz_model_RelPredicateExpr {
    u8 tag;
    jazz_model_RelColumnRef left;   /* Cmp/In/Contains left; IsNull/IsNotNull/EnumMatch column */
    u8 op;
    jazz_model_RelValueRef right;   /* Cmp / Contains */
    jazz_model_RelValueRefVec values; /* In */
    jz_str case_;                   /* EnumMatch */
    jazz_model_RelPredicateExpr *inner; /* EnumMatch payload / Not */
    jazz_model_RelPredicateExprVec items; /* And / Or */
};
enum { JAZZ_KEYREF_COLUMN = 0, JAZZ_KEYREF_ROW_ID = 1 };
typedef struct jazz_model_RelKeyRef { u8 tag; jazz_model_RelColumnRef column; u8 row_id; } jazz_model_RelKeyRef;
typedef JZ_VEC(jazz_model_RelKeyRef) jazz_model_RelKeyRefVec;
typedef jazz_model_RelKeyRef jazz_model_RelProjectExpr; /* same shape: Column(ColumnRef) | RowId(RowIdRef) */
typedef struct jazz_model_RelProjectColumn { jz_str alias; jazz_model_RelProjectExpr expr; } jazz_model_RelProjectColumn;
typedef JZ_VEC(jazz_model_RelProjectColumn) jazz_model_RelProjectColumnVec;
typedef struct jazz_model_RelJoinCondition { jazz_model_RelColumnRef left, right; } jazz_model_RelJoinCondition;
typedef JZ_VEC(jazz_model_RelJoinCondition) jazz_model_RelJoinConditionVec;
enum { JAZZ_JOIN_INNER = 0, JAZZ_JOIN_LEFT = 1 };
enum { JAZZ_BOUND_FIXPOINT = 0, JAZZ_BOUND_MAX_DEPTH = 1 };
typedef struct jazz_model_RelRecursionBound { u8 tag; u32 max_depth; } jazz_model_RelRecursionBound;
enum { JAZZ_REL_TABLE_SCAN, JAZZ_REL_FILTER, JAZZ_REL_UNION, JAZZ_REL_JOIN, JAZZ_REL_PROJECT, JAZZ_REL_GATHER };
typedef struct jazz_model_RelExpr jazz_model_RelExpr;
typedef struct jazz_model_RelUnionArm jazz_model_RelUnionArm;
typedef JZ_VEC(jazz_model_RelUnionArm) jazz_model_RelUnionArmVec;
struct jazz_model_RelExpr {
    u8 tag;
    jazz_model_TableName table; jazz_model_OptStr alias;  /* TableScan */
    jazz_model_RelExpr *input;                            /* Filter / Project input; Join left; Gather seed */
    jazz_model_RelExpr *right;                            /* Join right; Gather step */
    jazz_model_RelPredicateExpr predicate;                /* Filter */
    jazz_model_RelUnionArmVec inputs;                     /* Union */
    jazz_model_RelJoinConditionVec on; u8 join_kind;      /* Join */
    jazz_model_RelProjectColumnVec columns;               /* Project */
    jazz_model_RelKeyRef frontier_key; jazz_model_RelRecursionBound bound; jazz_model_RelKeyRefVec dedupe_key; /* Gather */
};
struct jazz_model_RelUnionArm { jz_str label; jazz_model_RelExpr input; };

/* ============================================================ public_api::policy */
enum { JAZZ_CMP_EQ, JAZZ_CMP_NE, JAZZ_CMP_LT, JAZZ_CMP_LE, JAZZ_CMP_GT, JAZZ_CMP_GE };
enum { JAZZ_OP_SELECT, JAZZ_OP_INSERT, JAZZ_OP_UPDATE, JAZZ_OP_DELETE };
enum { JAZZ_POLVAL_LITERAL = 0, JAZZ_POLVAL_SESSION_REF = 1 };
typedef struct jazz_model_PolicyValue { u8 tag; jazz_model_public_Value literal; jazz_model_StrVec path; } jazz_model_PolicyValue;
typedef JZ_VEC(jazz_model_PolicyValue) jazz_model_PolicyValueVec;
enum {
    JAZZ_PE_CMP, JAZZ_PE_SESSION_CMP, JAZZ_PE_IS_NULL, JAZZ_PE_SESSION_IS_NULL, JAZZ_PE_IS_NOT_NULL,
    JAZZ_PE_SESSION_IS_NOT_NULL, JAZZ_PE_CONTAINS, JAZZ_PE_SESSION_CONTAINS, JAZZ_PE_IN, JAZZ_PE_IN_LIST,
    JAZZ_PE_SESSION_IN_LIST, JAZZ_PE_EXISTS, JAZZ_PE_EXISTS_REL, JAZZ_PE_INHERITS, JAZZ_PE_INHERITS_REFERENCING,
    JAZZ_PE_AND, JAZZ_PE_OR, JAZZ_PE_NOT, JAZZ_PE_TRUE, JAZZ_PE_FALSE, JAZZ_PE_COUNT
};
typedef struct jazz_model_PolicyExpr jazz_model_PolicyExpr;
typedef JZ_VEC(jazz_model_PolicyExpr) jazz_model_PolicyExprVec;
struct jazz_model_PolicyExpr {
    u8 tag;
    jz_str column;               /* Cmp, IsNull, IsNotNull, Contains, In, InList; via_column for Inherits* */
    jz_str table;                /* Exists table; InheritsReferencing source_table */
    jazz_model_StrVec path;      /* Session*; In session_path */
    u8 op;                       /* Cmp / SessionCmp: CmpOp; Inherits*: Operation */
    jazz_model_PolicyValue pvalue;          /* Cmp / Contains */
    jazz_model_public_Value value;          /* SessionCmp / SessionContains */
    jazz_model_PolicyValueVec pvalues;      /* InList */
    jazz_model_public_ValueVec values;      /* SessionInList */
    jazz_model_PolicyExpr *inner;           /* Exists condition / Not */
    jazz_model_RelExpr rel;                 /* ExistsRel */
    bool has_max_depth; u32 max_depth;      /* Inherits* */
    jazz_model_PolicyExprVec exprs;         /* And / Or */
};

/* ============================================================ public_api::types::policy */
typedef struct jazz_model_OperationPolicy { jazz_model_PolicyExpr *using_; jazz_model_PolicyExpr *with_check; } jazz_model_OperationPolicy;
typedef struct jazz_model_TablePolicies { jazz_model_OperationPolicy select, insert, update, delete_; } jazz_model_TablePolicies;

/* ============================================================ public_api::types::schema (tables) */
typedef struct jazz_model_public_TableSchema {
    jazz_model_RowDescriptor columns;
    bool has_indexed_columns; jazz_model_ColumnNameVec indexed_columns;
    jazz_model_ColumnNameVecVec composite_indexes;
    jazz_model_TablePolicies policies;
    jazz_model_ColumnNameVec branch_by;
} jazz_model_public_TableSchema;
typedef struct jazz_model_public_Schema { jz_btree tables; /* TableName -> public_TableSchema */ } jazz_model_public_Schema;
extern const jz_btree_desc jazz_model__PUBLIC_SCHEMA_TABLES_DESC;

/* ============================================================ public_api::types::row */
typedef struct jazz_model_QueryResultField { jz_str name; jazz_model_public_Value value; } jazz_model_QueryResultField;
typedef JZ_VEC(jazz_model_QueryResultField) jazz_model_QueryResultFieldVec;
typedef struct jazz_model_QueryResult { jazz_types_ResultKey key; jazz_model_QueryResultFieldVec fields; } jazz_model_QueryResult;
typedef struct jazz_model_RowBytes { u8 *arc; u32 len; } jazz_model_RowBytes; /* Arc<[u8]>: jz_rc payload */

/* ---------- prototypes: public_api::types::schema ---------- */
void jazz_model__model__public_api__types__schema__ColumnType__drop(jazz_model_public_ColumnType *t);
void jazz_model__model__public_api__types__schema__ColumnType__Clone__clone(jazz_model_public_ColumnType *dst, const jazz_model_public_ColumnType *src);
bool jazz_model__model__public_api__types__schema__ColumnType__PartialEq__eq(const jazz_model_public_ColumnType *a, const jazz_model_public_ColumnType *b);
void jazz_model__model__public_api__types__schema__ColumnType__Debug__fmt(jz_str *out, const jazz_model_public_ColumnType *t);
void jazz_model__model__public_api__types__schema__ColumnType__Serialize__serialize(jz_json_w *w, const jazz_model_public_ColumnType *t);
jz_err jazz_model__model__public_api__types__schema__ColumnType__Deserialize__deserialize(const jz_json *j, jazz_model_public_ColumnType *out);
jazz_model_public_ColumnType jazz_model__model__public_api__types__schema__ColumnType__simple(u8 tag);
const jazz_model_public_ColumnType *jazz_model__model__public_api__types__schema__ColumnType__element_type(const jazz_model_public_ColumnType *t);
const jazz_model_RowDescriptor *jazz_model__model__public_api__types__schema__ColumnType__row_descriptor(const jazz_model_public_ColumnType *t);
void jazz_model__model__public_api__types__schema__EnumCaseDescriptor__drop(jazz_model_EnumCaseDescriptor *c);
void jazz_model__model__public_api__types__schema__ColumnDescriptor__drop(jazz_model_ColumnDescriptor *c);
void jazz_model__model__public_api__types__schema__ColumnDescriptor__Clone__clone(jazz_model_ColumnDescriptor *dst, const jazz_model_ColumnDescriptor *src);
bool jazz_model__model__public_api__types__schema__ColumnDescriptor__PartialEq__eq(const jazz_model_ColumnDescriptor *a, const jazz_model_ColumnDescriptor *b);
void jazz_model__model__public_api__types__schema__ColumnDescriptor__Serialize__serialize(jz_json_w *w, const jazz_model_ColumnDescriptor *c);
jz_err jazz_model__model__public_api__types__schema__ColumnDescriptor__Deserialize__deserialize(const jz_json *j, jazz_model_ColumnDescriptor *out);
jazz_model_ColumnDescriptor jazz_model__model__public_api__types__schema__ColumnDescriptor__new(jazz_model_ColumnName name, jazz_model_public_ColumnType t);
jz_err jazz_model__model__public_api__types__schema__ColumnDescriptor__validate_merge_strategy(const jazz_model_ColumnDescriptor *c);
void jazz_model__model__public_api__types__schema__RowDescriptor__drop(jazz_model_RowDescriptor *r);
void jazz_model__model__public_api__types__schema__RowDescriptor__Clone__clone(jazz_model_RowDescriptor *dst, const jazz_model_RowDescriptor *src);
bool jazz_model__model__public_api__types__schema__RowDescriptor__PartialEq__eq(const jazz_model_RowDescriptor *a, const jazz_model_RowDescriptor *b);
bool jazz_model__model__public_api__types__schema__RowDescriptor__column_index(const jazz_model_RowDescriptor *r, jz_slice name, u32 *out);
const jazz_model_ColumnDescriptor *jazz_model__model__public_api__types__schema__RowDescriptor__column(const jazz_model_RowDescriptor *r, jz_slice name);
void jazz_model__model__public_api__types__schema__TableSchema__drop(jazz_model_public_TableSchema *t);
void jazz_model__model__public_api__types__schema__TableSchema__drop_v(void *t);
void jazz_model__model__public_api__types__schema__TableSchema__Clone__clone(jazz_model_public_TableSchema *dst, const jazz_model_public_TableSchema *src);
void jazz_model__model__public_api__types__schema__TableSchema__Clone__clone_v(void *dst, const void *src);
bool jazz_model__model__public_api__types__schema__TableSchema__PartialEq__eq(const jazz_model_public_TableSchema *a, const jazz_model_public_TableSchema *b);
bool jazz_model__model__public_api__types__schema__TableSchema__PartialEq__eq_v(const void *a, const void *b);
void jazz_model__model__public_api__types__schema__TableSchema__Serialize__serialize(jz_json_w *w, const jazz_model_public_TableSchema *t);
jz_err jazz_model__model__public_api__types__schema__TableSchema__Deserialize__deserialize(const jz_json *j, jazz_model_public_TableSchema *out);
jazz_model_public_TableSchema jazz_model__model__public_api__types__schema__TableSchema__new(jazz_model_RowDescriptor columns);
bool jazz_model__model__public_api__types__schema__TableSchema__is_indexed_column(const jazz_model_public_TableSchema *t, jz_slice column);
typedef JZ_VEC(jz_slice) jazz_model_SliceVec;
typedef JZ_VEC(jazz_model_SliceVec) jazz_model_SliceVecVec;
void jazz_model__model__public_api__types__schema__canonical_composite_index_order(const jazz_model_ColumnNameVecVec *indexes, jazz_model_SliceVecVec *out);
void jazz_model__model__public_api__types__schema__SliceVecVec__drop(jazz_model_SliceVecVec *v);
bool jazz_model__model__public_api__types__schema__table_policies_are_default(const jazz_model_TablePolicies *p);
void jazz_model__model__public_api__types__schema__Schema__new(jazz_model_public_Schema *s);
void jazz_model__model__public_api__types__schema__Schema__drop(jazz_model_public_Schema *s);
void jazz_model__model__public_api__types__schema__Schema__Clone__clone(jazz_model_public_Schema *dst, const jazz_model_public_Schema *src);
bool jazz_model__model__public_api__types__schema__Schema__PartialEq__eq(const jazz_model_public_Schema *a, const jazz_model_public_Schema *b);
void jazz_model__model__public_api__types__schema__Schema__Serialize__serialize(jz_json_w *w, const jazz_model_public_Schema *s);
jz_err jazz_model__model__public_api__types__schema__Schema__Deserialize__deserialize(const jz_json *j, jazz_model_public_Schema *out);
void jazz_model__model__public_api__types__schema__ColumnNameVec__drop(jazz_model_ColumnNameVec *v);
void jazz_model__model__public_api__types__schema__ColumnNameVecVec__drop(jazz_model_ColumnNameVecVec *v);

/* ---------- prototypes: public_api::types::value ---------- */
void jazz_model__model__public_api__types__value__Value__drop(jazz_model_public_Value *v);
void jazz_model__model__public_api__types__value__Value__drop_v(void *v);
void jazz_model__model__public_api__types__value__Value__Clone__clone(jazz_model_public_Value *dst, const jazz_model_public_Value *src);
void jazz_model__model__public_api__types__value__Value__Clone__clone_v(void *dst, const void *src);
bool jazz_model__model__public_api__types__value__Value__PartialEq__eq(const jazz_model_public_Value *a, const jazz_model_public_Value *b);
void jazz_model__model__public_api__types__value__Value__Debug__fmt(jz_str *out, const jazz_model_public_Value *v);
jz_err jazz_model__model__public_api__types__value__Value__Serialize__serialize(jz_json_w *w, const jazz_model_public_Value *v);
jz_err jazz_model__model__public_api__types__value__Value__Deserialize__deserialize(const jz_json *j, jazz_model_public_Value *out);
jz_err jazz_model__model__public_api__types__value__Value__Serialize__serialize_postcard(jz_pc_w *w, const jazz_model_public_Value *v);
jz_err jazz_model__model__public_api__types__value__Value__Deserialize__deserialize_postcard(jz_pc_r *r, jazz_model_public_Value *out);
bool jazz_model__model__public_api__types__value__Value__column_type(const jazz_model_public_Value *v, jazz_model_public_ColumnType *out);
void jazz_model__model__public_api__types__value__ValueVec__drop(jazz_model_public_ValueVec *v);
void jazz_model__model__public_api__types__value__ValueVec__clone(jazz_model_public_ValueVec *dst, const jazz_model_public_ValueVec *src);
bool jazz_model__model__public_api__types__value__ValueVec__eq(const jazz_model_public_ValueVec *a, const jazz_model_public_ValueVec *b);
static inline jazz_model_public_Value jazz_model_public_Value_null(void) { jazz_model_public_Value v; memset(&v, 0, sizeof v); v.tag = JAZZ_PV_NULL; return v; }

/* ---------- prototypes: public_api::types::policy ---------- */
void jazz_model__model__public_api__types__policy__OperationPolicy__drop(jazz_model_OperationPolicy *p);
void jazz_model__model__public_api__types__policy__TablePolicies__drop(jazz_model_TablePolicies *p);
void jazz_model__model__public_api__types__policy__TablePolicies__Clone__clone(jazz_model_TablePolicies *dst, const jazz_model_TablePolicies *src);
bool jazz_model__model__public_api__types__policy__TablePolicies__PartialEq__eq(const jazz_model_TablePolicies *a, const jazz_model_TablePolicies *b);
void jazz_model__model__public_api__types__policy__TablePolicies__Serialize__serialize(jz_json_w *w, const jazz_model_TablePolicies *p);
jz_err jazz_model__model__public_api__types__policy__TablePolicies__Deserialize__deserialize(const jz_json *j, jazz_model_TablePolicies *out);
bool jazz_model__model__public_api__types__policy__TablePolicies__has_any_explicit_policy(const jazz_model_TablePolicies *p);
bool jazz_model__model__public_api__types__policy__TablePolicies__has_explicit_update_policy(const jazz_model_TablePolicies *p);

/* ---------- prototypes: public_api::types::row ---------- */
void jazz_model__model__public_api__types__row__QueryResultField__drop(jazz_model_QueryResultField *f);
void jazz_model__model__public_api__types__row__QueryResult__drop(jazz_model_QueryResult *r);
const jazz_model_public_Value *jazz_model__model__public_api__types__row__QueryResult__get(const jazz_model_QueryResult *r, jz_slice name);
void jazz_model__model__public_api__types__row__QueryResult__into_values(jazz_model_QueryResult *r, jazz_model_public_ValueVec *out);
jazz_model_RowBytes jazz_model__model__public_api__types__row__RowBytes__from_vec(jz_bytes v);
void jazz_model__model__public_api__types__row__RowBytes__drop(jazz_model_RowBytes *b);
void jazz_model__model__public_api__types__row__RowBytes__Debug__fmt(jz_str *out, const jazz_model_RowBytes *b);
void jazz_model__model__public_api__types__row__RowBytes__Serialize__serialize(jz_pc_w *w, const jazz_model_RowBytes *b);
jz_err jazz_model__model__public_api__types__row__RowBytes__Deserialize__deserialize(jz_pc_r *r, jazz_model_RowBytes *out);

/* ---------- prototypes: public_api::relation_ir ---------- */
void jazz_model__model__public_api__relation_ir__ColumnRef__drop(jazz_model_RelColumnRef *c);
void jazz_model__model__public_api__relation_ir__ColumnRef__Clone__clone(jazz_model_RelColumnRef *dst, const jazz_model_RelColumnRef *src);
bool jazz_model__model__public_api__relation_ir__ColumnRef__PartialEq__eq(const jazz_model_RelColumnRef *a, const jazz_model_RelColumnRef *b);
void jazz_model__model__public_api__relation_ir__ColumnRef__Debug__fmt(jz_str *out, const jazz_model_RelColumnRef *c);
jazz_model_RelColumnRef jazz_model__model__public_api__relation_ir__ColumnRef__unscoped(jz_str column);
void jazz_model__model__public_api__relation_ir__ValueRef__drop(jazz_model_RelValueRef *v);
void jazz_model__model__public_api__relation_ir__ValueRef__Clone__clone(jazz_model_RelValueRef *dst, const jazz_model_RelValueRef *src);
bool jazz_model__model__public_api__relation_ir__ValueRef__PartialEq__eq(const jazz_model_RelValueRef *a, const jazz_model_RelValueRef *b);
void jazz_model__model__public_api__relation_ir__PredicateExpr__drop(jazz_model_RelPredicateExpr *p);
void jazz_model__model__public_api__relation_ir__PredicateExpr__Clone__clone(jazz_model_RelPredicateExpr *dst, const jazz_model_RelPredicateExpr *src);
bool jazz_model__model__public_api__relation_ir__PredicateExpr__PartialEq__eq(const jazz_model_RelPredicateExpr *a, const jazz_model_RelPredicateExpr *b);
void jazz_model__model__public_api__relation_ir__PredicateExpr__Debug__fmt(jz_str *out, const jazz_model_RelPredicateExpr *p);
void jazz_model__model__public_api__relation_ir__KeyRef__Debug__fmt(jz_str *out, const jazz_model_RelKeyRef *k);
void jazz_model__model__public_api__relation_ir__KeyRef__drop(jazz_model_RelKeyRef *k);
void jazz_model__model__public_api__relation_ir__KeyRef__Clone__clone(jazz_model_RelKeyRef *dst, const jazz_model_RelKeyRef *src);
bool jazz_model__model__public_api__relation_ir__KeyRef__PartialEq__eq(const jazz_model_RelKeyRef *a, const jazz_model_RelKeyRef *b);
void jazz_model__model__public_api__relation_ir__RecursionBound__Debug__fmt(jz_str *out, const jazz_model_RelRecursionBound *b);
void jazz_model__model__public_api__relation_ir__RelExpr__drop(jazz_model_RelExpr *e);
void jazz_model__model__public_api__relation_ir__RelExpr__Clone__clone(jazz_model_RelExpr *dst, const jazz_model_RelExpr *src);
bool jazz_model__model__public_api__relation_ir__RelExpr__PartialEq__eq(const jazz_model_RelExpr *a, const jazz_model_RelExpr *b);
jz_err jazz_model__model__public_api__relation_ir__RelExpr__Serialize__serialize(jz_json_w *w, const jazz_model_RelExpr *e);
jz_err jazz_model__model__public_api__relation_ir__RelExpr__Deserialize__deserialize(const jz_json *j, jazz_model_RelExpr *out);
jz_err jazz_model__model__public_api__relation_ir__PredicateExpr__Serialize__serialize(jz_json_w *w, const jazz_model_RelPredicateExpr *p);
jz_err jazz_model__model__public_api__relation_ir__PredicateExpr__Deserialize__deserialize(const jz_json *j, jazz_model_RelPredicateExpr *out);

/* ---------- prototypes: public_api::policy ---------- */
void jazz_model__model__public_api__policy__CmpOp__Debug__fmt(jz_str *out, u8 op);
void jazz_model__model__public_api__policy__Operation__Debug__fmt(jz_str *out, u8 op);
void jazz_model__model__public_api__policy__Operation__Display__fmt(jz_str *out, u8 op);
void jazz_model__model__public_api__policy__PolicyValue__drop(jazz_model_PolicyValue *v);
void jazz_model__model__public_api__policy__PolicyValue__Clone__clone(jazz_model_PolicyValue *dst, const jazz_model_PolicyValue *src);
bool jazz_model__model__public_api__policy__PolicyValue__PartialEq__eq(const jazz_model_PolicyValue *a, const jazz_model_PolicyValue *b);
jz_err jazz_model__model__public_api__policy__PolicyValue__Serialize__serialize(jz_json_w *w, const jazz_model_PolicyValue *v);
jz_err jazz_model__model__public_api__policy__PolicyValue__Deserialize__deserialize(const jz_json *j, jazz_model_PolicyValue *out);
void jazz_model__model__public_api__policy__PolicyExpr__drop(jazz_model_PolicyExpr *e);
void jazz_model__model__public_api__policy__PolicyExpr__drop_v(void *e);
void jazz_model__model__public_api__policy__PolicyExpr__Clone__clone(jazz_model_PolicyExpr *dst, const jazz_model_PolicyExpr *src);
bool jazz_model__model__public_api__policy__PolicyExpr__PartialEq__eq(const jazz_model_PolicyExpr *a, const jazz_model_PolicyExpr *b);
void jazz_model__model__public_api__policy__PolicyExpr__Debug__fmt(jz_str *out, const jazz_model_PolicyExpr *e);
jz_err jazz_model__model__public_api__policy__PolicyExpr__Serialize__serialize(jz_json_w *w, const jazz_model_PolicyExpr *e);
jz_err jazz_model__model__public_api__policy__PolicyExpr__Deserialize__deserialize(const jz_json *j, jazz_model_PolicyExpr *out);
jazz_model_PolicyExpr jazz_model__model__public_api__policy__PolicyExpr__simple(u8 tag);
jazz_model_PolicyExpr jazz_model__model__public_api__policy__PolicyExpr__and(jazz_model_PolicyExprVec exprs);
jazz_model_PolicyExpr jazz_model__model__public_api__policy__PolicyExpr__or(jazz_model_PolicyExprVec exprs);
jazz_model_PolicyExpr jazz_model__model__public_api__policy__PolicyExpr__not(jazz_model_PolicyExpr expr);

/* ---------- lib.c shared helpers ---------- */
void jazz_model__StrVec__drop(jazz_model_StrVec *v);
void jazz_model__StrVec__clone(jazz_model_StrVec *dst, const jazz_model_StrVec *src);
bool jazz_model__StrVec__eq(const jazz_model_StrVec *a, const jazz_model_StrVec *b);
void jazz_model__StrVec__Debug__fmt(jz_str *out, const jazz_model_StrVec *v);
void jazz_model__StrVec__serialize_json(jz_json_w *w, const jazz_model_StrVec *v);
jz_err jazz_model__StrVec__deserialize_json(const jz_json *j, jazz_model_StrVec *out);
void jazz_model__StrVec__serialize_pc(jz_pc_w *w, const jazz_model_StrVec *v);
jz_err jazz_model__StrVec__deserialize_pc(jz_pc_r *r, jazz_model_StrVec *out);
void jazz_model__OptStr__drop(jazz_model_OptStr *s);
jazz_model_OptStr jazz_model__OptStr__clone(const jazz_model_OptStr *s);
bool jazz_model__OptStr__eq(const jazz_model_OptStr *a, const jazz_model_OptStr *b);
void jazz_model__str_drop_v(void *s);
void jazz_model__str_clone_v(void *dst, const void *src);
void jazz_model__json_w_str(jz_json_w *w, const jz_str *s);
void jazz_model__json_w_cstr(jz_json_w *w, const char *s);
void jazz_model__debug_str(jz_str *out, const char *p, u32 n);   /* {:?} of a &str */
jz_err jazz_model__serde_err(const char *fmt, ...);
jz_err jazz_model__json_missing_field(const char *field);
jz_err jazz_model__json_unknown_variant(const jz_str *name, const char *const *names, u32 n);
jz_err jazz_model__json_deny_unknown_fields(const jz_json *obj, const char *const *fields, u32 n);
jz_err jazz_model__json_internal_tag(const jz_json *v, const char *tag_field, const char *const *names, u32 n, u32 *idx);
const jz_str *jazz_model__json_str(const jz_json *v);
jz_err jazz_model__json_expect_str(const jz_json *v, jz_str *out);
jz_err jazz_model__json_expect_arr(const jz_json *v, const char *what);
jz_err jazz_model__json_expect_bool(const jz_json *v, bool *out);
jz_err jazz_model__json_expect_u64(const jz_json *v, u64 *out);
jz_err jazz_model__json_expect_i64(const jz_json *v, i64 *out);
jz_err jazz_model__json_expect_f64(const jz_json *v, f64 *out);
jz_err jazz_model__json_unit_variant(const jz_json *v, const char *const *names, u32 n, u32 *idx);
void jazz_model__json_w_unit_variant(jz_json_w *w, const char *name);
jz_str jazz_model__str_from_slice(jz_slice s);
bool jazz_model__slice_eq(jz_slice a, jz_slice b);
bool jazz_model__slice_eq_cstr(jz_slice a, const char *s);
bool jazz_model__str_starts_with(jz_slice s, const char *prefix);
int jazz_model__cmp_str_ptr(const void *a, const void *b, void *ctx); /* compares const jz_str * */
extern const jz_btree_desc jazz_model__STR_SET_DESC;     /* BTreeSet<String> */
extern const jz_btree_desc jazz_model__STR_U32_DESC;     /* BTreeMap<String, u32/usize> */
extern const jz_btree_desc jazz_model__STRPTR_SET_DESC;  /* BTreeSet<&str> / BTreeSet<Intern<String>> by content */

/* ============================================================ model::metadata */
extern const char jazz_model__model__metadata__SYSTEM_PRINCIPAL_ID[];
typedef struct jazz_model_RowProvenance { jz_str created_by; u64 created_at; jz_str updated_by; u64 updated_at; } jazz_model_RowProvenance;
void jazz_model__model__metadata__RowProvenance__drop(jazz_model_RowProvenance *p);
void jazz_model__model__metadata__RowProvenance__Clone__clone(jazz_model_RowProvenance *dst, const jazz_model_RowProvenance *src);
bool jazz_model__model__metadata__RowProvenance__PartialEq__eq(const jazz_model_RowProvenance *a, const jazz_model_RowProvenance *b);

/* ============================================================ tx::TxId (used by model::transaction) */
typedef struct jazz_model_TxId { jazz_types_TxTime time; jazz_types_NodeUuid node; } jazz_model_TxId;

/* ============================================================ model::transaction */
typedef struct jazz_model_OpenTransactionId { u8 b[16]; } jazz_model_OpenTransactionId;
typedef struct jazz_model_TransactionId { u8 b[16]; } jazz_model_TransactionId;
jazz_model_OpenTransactionId jazz_model__model__transaction__OpenTransactionId__new(void);
void jazz_model__model__transaction__OpenTransactionId__Display__fmt(jz_str *out, const jazz_model_OpenTransactionId *id);
void jazz_model__model__transaction__OpenTransactionId__Debug__fmt(jz_str *out, const jazz_model_OpenTransactionId *id);
jz_err jazz_model__model__transaction__OpenTransactionId__FromStr__from_str(jz_slice raw, jazz_model_OpenTransactionId *out);
void jazz_model__model__transaction__OpenTransactionId__Serialize__serialize(jz_pc_w *w, const jazz_model_OpenTransactionId *id);
jz_err jazz_model__model__transaction__OpenTransactionId__Deserialize__deserialize(jz_pc_r *r, jazz_model_OpenTransactionId *out);
void jazz_model__model__transaction__OpenTransactionId__Serialize__serialize_json(jz_json_w *w, const jazz_model_OpenTransactionId *id);
jz_err jazz_model__model__transaction__OpenTransactionId__Deserialize__deserialize_json(const jz_json *j, jazz_model_OpenTransactionId *out);
void jazz_model__model__transaction__TransactionId__Display__fmt(jz_str *out, const jazz_model_TransactionId *id);
void jazz_model__model__transaction__TransactionId__Debug__fmt(jz_str *out, const jazz_model_TransactionId *id);
jz_err jazz_model__model__transaction__TransactionId__FromStr__from_str(jz_slice raw, jazz_model_TransactionId *out);
void jazz_model__model__transaction__TransactionId__Serialize__serialize(jz_pc_w *w, const jazz_model_TransactionId *id);
jz_err jazz_model__model__transaction__TransactionId__Deserialize__deserialize(jz_pc_r *r, jazz_model_TransactionId *out);
void jazz_model__model__transaction__TransactionId__Serialize__serialize_json(jz_json_w *w, const jazz_model_TransactionId *id);
jz_err jazz_model__model__transaction__TransactionId__Deserialize__deserialize_json(const jz_json *j, jazz_model_TransactionId *out);
jazz_model_TransactionId jazz_model__model__transaction__TransactionId__from_committed_tx(jazz_model_TxId tx_id);
int jazz_model__model__transaction__TransactionId__Ord__cmp(const void *a, const void *b, void *ctx);

/* ============================================================ public_api::types::row (Row / RowDelta) */
typedef struct jazz_model_Row {
    jazz_types_ResultKey id;
    jazz_model_RowBytes data;
    jazz_model_TransactionId transaction_id;
    jazz_model_RowProvenance provenance;
} jazz_model_Row;
typedef JZ_VEC(jazz_model_Row) jazz_model_RowVec;
typedef struct jazz_model_RowPair { jazz_model_Row old_row, new_row; } jazz_model_RowPair;
typedef struct jazz_model_RowDelta {
    jazz_model_RowVec added, removed;
    JZ_VEC(jazz_types_ResultKey) moved;
    JZ_VEC(jazz_model_RowPair) updated;
} jazz_model_RowDelta;
typedef struct jazz_model_OrderedAdded { jazz_types_ResultKey id; u32 index; jazz_model_Row row; } jazz_model_OrderedAdded;
typedef struct jazz_model_OrderedRemoved { jazz_types_ResultKey id; u32 index; } jazz_model_OrderedRemoved;
typedef struct jazz_model_OrderedUpdated { jazz_types_ResultKey id; u32 old_index, new_index; bool has_row; jazz_model_Row row; } jazz_model_OrderedUpdated;
typedef struct jazz_model_OrderedRowDelta {
    JZ_VEC(jazz_model_OrderedAdded) added;
    JZ_VEC(jazz_model_OrderedRemoved) removed;
    JZ_VEC(jazz_model_OrderedUpdated) updated;
    bool pending;
} jazz_model_OrderedRowDelta;
jazz_model_Row jazz_model__model__public_api__types__row__Row__new(jazz_types_ResultKey id, jazz_model_RowBytes data, jazz_model_TransactionId tx, jazz_model_RowProvenance provenance);
void jazz_model__model__public_api__types__row__Row__drop(jazz_model_Row *r);
void jazz_model__model__public_api__types__row__Row__Clone__clone(jazz_model_Row *dst, const jazz_model_Row *src);
bool jazz_model__model__public_api__types__row__Row__PartialEq__eq(const jazz_model_Row *a, const jazz_model_Row *b);
void jazz_model__model__public_api__types__row__RowDelta__drop(jazz_model_RowDelta *d);
bool jazz_model__model__public_api__types__row__RowDelta__is_empty(const jazz_model_RowDelta *d);
void jazz_model__model__public_api__types__row__OrderedRowDelta__drop(jazz_model_OrderedRowDelta *d);
bool jazz_model__model__public_api__types__row__OrderedRowDelta__is_empty(const jazz_model_OrderedRowDelta *d);

/* ============================================================ model::branch */
typedef struct jazz_model_BranchColumnValue { jz_bytes bytes; } jazz_model_BranchColumnValue;
typedef struct jazz_model_BranchSelector { jz_btree values; /* String -> BranchColumnValue */ } jazz_model_BranchSelector;
typedef struct jazz_model_BranchKeyEntry { jz_str name; jazz_model_BranchColumnValue value; } jazz_model_BranchKeyEntry;
typedef struct jazz_model_BranchKey { JZ_VEC(jazz_model_BranchKeyEntry) values; } jazz_model_BranchKey;
typedef JZ_VEC(jazz_model_TxId) jazz_model_TxIdVec;
typedef struct jazz_model_SnapshotRef {
    jazz_types_NodeUuid owner;
    jazz_types_GlobalTime global_base;
    jazz_types_TxTime local_base;
    jazz_model_TxIdVec dots;
} jazz_model_SnapshotRef;
enum { JAZZ_BVB_CURRENT = 0, JAZZ_BVB_SNAPSHOT = 1 };
typedef struct jazz_model_BranchViewBase { u8 tag; jazz_model_BranchSelector branch; jazz_model_SnapshotRef snapshot; } jazz_model_BranchViewBase;
extern const jz_btree_desc jazz_model__model__branch__SELECTOR_VALUES_DESC;
extern const jz_btree_desc jazz_model__model__branch__BRANCH_KEY_SET_DESC;

void jazz_model__model__branch__BranchCodecError__Display__fmt(jz_str *out, const jz_err *e);
void jazz_model__model__branch__BranchCodecError__Debug__fmt(jz_str *out, const jz_err *e);
void jazz_model__model__branch__BranchSelector__new(jazz_model_BranchSelector *s);
void jazz_model__model__branch__BranchSelector__drop(jazz_model_BranchSelector *s);
void jazz_model__model__branch__BranchSelector__Clone__clone(jazz_model_BranchSelector *dst, const jazz_model_BranchSelector *src);
int jazz_model__model__branch__BranchSelector__Ord__cmp(const jazz_model_BranchSelector *a, const jazz_model_BranchSelector *b);
bool jazz_model__model__branch__BranchSelector__PartialEq__eq(const jazz_model_BranchSelector *a, const jazz_model_BranchSelector *b);
void jazz_model__model__branch__BranchSelector__Serialize__serialize(jz_pc_w *w, const jazz_model_BranchSelector *s);
jz_err jazz_model__model__branch__BranchSelector__Deserialize__deserialize(jz_pc_r *r, jazz_model_BranchSelector *out);
void jazz_model__model__branch__BranchSelector__Debug__fmt(jz_str *out, const jazz_model_BranchSelector *s);
jazz_model_BranchColumnValue jazz_model__model__branch__BranchColumnValue__From_Value__from(groove_Value value);
jz_err jazz_model__model__branch__BranchColumnValue__encode_typed(const groove_Value *value, const groove_ValueType *t, jazz_model_BranchColumnValue *out);
jz_err jazz_model__model__branch__BranchColumnValue__decode(const jazz_model_BranchColumnValue *v, groove_Value *out);
jz_err jazz_model__model__branch__BranchColumnValue__decode_as(const jazz_model_BranchColumnValue *v, const groove_ValueType *t, groove_Value *out);
void jazz_model__model__branch__BranchColumnValue__Serialize__serialize(jz_pc_w *w, const jazz_model_BranchColumnValue *v);
jz_err jazz_model__model__branch__BranchColumnValue__Deserialize__deserialize(jz_pc_r *r, jazz_model_BranchColumnValue *out);
void jazz_model__model__branch__BranchKey__drop(jazz_model_BranchKey *k);
void jazz_model__model__branch__BranchKey__drop_v(void *k);
void jazz_model__model__branch__BranchKey__Clone__clone(jazz_model_BranchKey *dst, const jazz_model_BranchKey *src);
void jazz_model__model__branch__BranchKey__Clone__clone_v(void *dst, const void *src);
int jazz_model__model__branch__BranchKey__Ord__cmp(const void *a, const void *b, void *ctx);
bool jazz_model__model__branch__BranchKey__PartialEq__eq(const jazz_model_BranchKey *a, const jazz_model_BranchKey *b);
u64 jazz_model__model__branch__BranchKey__Hash__hash(const void *k);
void jazz_model__model__branch__BranchKey__Debug__fmt(jz_str *out, const jazz_model_BranchKey *k);
jz_err jazz_model__model__branch__BranchKey__try_canonical_bytes(const jazz_model_BranchKey *k, jz_bytes *out);
jz_bytes jazz_model__model__branch__BranchKey__canonical_bytes(const jazz_model_BranchKey *k);
jz_err jazz_model__model__branch__BranchKey__from_canonical_bytes(jz_slice bytes, jazz_model_BranchKey *out);
bool jazz_model__model__branch__BranchKey__is_canonical(const jazz_model_BranchKey *k);
void jazz_model__model__branch__BranchKey__Serialize__serialize(jz_pc_w *w, const jazz_model_BranchKey *k);
jz_err jazz_model__model__branch__BranchKey__Deserialize__deserialize(jz_pc_r *r, jazz_model_BranchKey *out);
void jazz_model__model__branch__SnapshotRef__drop(jazz_model_SnapshotRef *s);
void jazz_model__model__branch__SnapshotRef__Clone__clone(jazz_model_SnapshotRef *dst, const jazz_model_SnapshotRef *src);
bool jazz_model__model__branch__SnapshotRef__PartialEq__eq(const jazz_model_SnapshotRef *a, const jazz_model_SnapshotRef *b);
int jazz_model__model__branch__SnapshotRef__Ord__cmp(const jazz_model_SnapshotRef *a, const jazz_model_SnapshotRef *b);
void jazz_model__model__branch__SnapshotRef__Serialize__serialize(jz_pc_w *w, const jazz_model_SnapshotRef *s);
jz_err jazz_model__model__branch__SnapshotRef__Deserialize__deserialize(jz_pc_r *r, jazz_model_SnapshotRef *out);
void jazz_model__model__branch__BranchViewBase__drop(jazz_model_BranchViewBase *b);
void jazz_model__model__branch__BranchViewBase__Clone__clone(jazz_model_BranchViewBase *dst, const jazz_model_BranchViewBase *src);
bool jazz_model__model__branch__BranchViewBase__PartialEq__eq(const jazz_model_BranchViewBase *a, const jazz_model_BranchViewBase *b);
void jazz_model__model__branch__BranchViewBase__Serialize__serialize(jz_pc_w *w, const jazz_model_BranchViewBase *b);
jz_err jazz_model__model__branch__BranchViewBase__Deserialize__deserialize(jz_pc_r *r, jazz_model_BranchViewBase *out);
void jazz_model__tx__TxId__Serialize__serialize(jz_pc_w *w, const jazz_model_TxId *id);
jz_err jazz_model__tx__TxId__Deserialize__deserialize(jz_pc_r *r, jazz_model_TxId *out);
int jazz_model__tx__TxId__Ord__cmp(const void *a, const void *b, void *ctx);

/* ============================================================ model::json_schema */
/* dyn HostCompiledJsonSchema: validate(instance_json) -> Ok(None) | Ok(Some(reason)) | Err(error) */
typedef struct jazz_model_HostCompiledJsonSchema {
    jz_err (*validate)(void *self, jz_slice instance_json, bool *mismatch, jz_str *reason);
    void (*drop)(void *self);
    void *self;
} jazz_model_HostCompiledJsonSchema;
/* dyn HostJsonSchemaValidator: compile(schema_json) -> Result<Box<dyn HostCompiledJsonSchema>, String> */
typedef struct jazz_model_HostJsonSchemaValidator {
    jz_err (*compile)(void *self, jz_slice schema_json, jazz_model_HostCompiledJsonSchema *out);
    void (*drop)(void *self);
    void *self;
} jazz_model_HostJsonSchemaValidator;
void jazz_model__model__json_schema__install_host_validator(jazz_model_HostJsonSchemaValidator validator);
jz_err jazz_model__model__json_schema__check_schema(const jz_json *schema);
jz_err jazz_model__model__json_schema__validate(const jz_json *schema, const jz_json *instance);

/* ============================================================ model::public_schema */
jz_err jazz_model__model__public_schema__validate_json_value(const jazz_model_public_Value *value, const jazz_model_public_ColumnType *t, jz_slice path);
jz_err jazz_model__model__public_schema__validate_json_schemas(const jazz_model_public_ColumnType *t, jz_slice path);

/* ============================================================ model::policy_claims */
extern const jz_btree_desc jazz_model__STR_GVALUE_DESC; /* BTreeMap<String, groove Value> */
enum { JAZZ_NUMERIC_CLAIM_JAVASCRIPT = 0, JAZZ_NUMERIC_CLAIM_EXACT_JSON = 1 };
enum { JAZZ_AUTH_MODE_EXTERNAL = 0, JAZZ_AUTH_MODE_LOCAL_FIRST = 1, JAZZ_AUTH_MODE_ANONYMOUS = 2 };
#define JAZZ_MAX_SAFE_JS_INTEGER 9007199254740991ull
void jazz_model__model__policy_claims__canonical_policy_binding_claims(const jazz_types_AuthorSubject *author, jz_btree *provider_claims_moved, jz_btree *out);
void jazz_model__model__policy_claims__author_policy_claims(jazz_types_AuthorSubject author, jz_btree *out);
jz_err jazz_model__model__policy_claims__json_number_to_policy_claim(const jz_json *number, u8 origin, groove_Value *out);
jz_err jazz_model__model__policy_claims__json_value_to_policy_claim(const jz_json *value, u8 origin, bool *some, groove_Value *out);
bool jazz_model__model__policy_claims__policy_claim_at_path(const jz_btree *claims, const jazz_model_StrVec *path, groove_Value *out);

/* query claim keys (query/policy_and_request_contracts.rs) */
jz_str jazz_model__query__provider_claim_key(jz_slice name);
bool jazz_model__query__author_claim_path_key(const jz_str *path, u32 n, jz_str *out);

/* ============================================================ model::policy_directory */
jz_err jazz_model__model__policy_directory__policy_binding_directory_claims_value(const jz_btree *claims, groove_Value *out);
jz_err jazz_model__model__policy_directory__policy_binding_directory_claims_from_value(groove_Value value, jz_btree *out);
groove_RecordDescriptor jazz_model__model__policy_directory__policy_directory_descriptor(void);
jz_err jazz_model__model__policy_directory__policy_directory_payload(u8 presence, groove_Value claims, groove_Value *out);
groove_ValueType jazz_model__model__policy_directory__policy_binding_directory_claims_value_type(void);

/* ============================================================ query (forward decls used by tx) */
typedef struct jazz_model_Query jazz_model_Query;
typedef struct jazz_model_ShapeId { jz_uuid id; } jazz_model_ShapeId;
typedef struct jazz_model_BindingId { jz_uuid id; } jazz_model_BindingId;
jz_err jazz_model__query__Query__Serialize__serialize(jz_pc_w *w, const jazz_model_Query *q);
jz_err jazz_model__query__Query__Deserialize__deserialize(jz_pc_r *r, jazz_model_Query *out);
void jazz_model__query__Query__drop(jazz_model_Query *q);
void jazz_model__query__Query__Clone__clone(jazz_model_Query *dst, const jazz_model_Query *src);
bool jazz_model__query__Query__PartialEq__eq(const jazz_model_Query *a, const jazz_model_Query *b);
jazz_model_Query *jazz_model__query__Query__box_new(void);

/* ============================================================ tx */
enum { JAZZ_TX_KIND_MERGEABLE = 0, JAZZ_TX_KIND_EXCLUSIVE = 1 };
enum { JAZZ_DURABILITY_NONE = 0, JAZZ_DURABILITY_LOCAL = 1, JAZZ_DURABILITY_GLOBAL = 2 };
enum { JAZZ_MERGE_ASPECT_CONTENT = 0, JAZZ_MERGE_ASPECT_DELETION = 1 };
enum { JAZZ_DELETION_DELETED = 0, JAZZ_DELETION_RESTORED = 1 };
enum {
    JAZZ_REJECT_CLIENT_CLOCK_TOO_FAR_AHEAD = 0, JAZZ_REJECT_AUTHORIZATION_DENIED, JAZZ_REJECT_EXCLUSIVE_CONFLICT,
    JAZZ_REJECT_CAUSALITY_VIOLATION, JAZZ_REJECT_CASCADE, JAZZ_REJECT_MALFORMED_COMMIT
};
typedef struct jazz_model_RejectionReason { u8 tag; jazz_model_TxId root; jz_str message; } jazz_model_RejectionReason;
enum { JAZZ_FATE_PENDING = 0, JAZZ_FATE_ACCEPTED = 1, JAZZ_FATE_REJECTED = 2 };
typedef struct jazz_model_Fate { u8 tag; jazz_model_RejectionReason reason; } jazz_model_Fate;
enum { JAZZ_COMPONENT_COLUMN = 0, JAZZ_COMPONENT_OPERATION = 1, JAZZ_COMPONENT_REGISTER = 2 };
typedef struct jazz_model_ContributionComponent { u8 tag; jz_str column; jz_bytes identity; } jazz_model_ContributionComponent;
typedef struct jazz_model_ContributionCoordinate {
    jazz_model_BranchKey branch_key;
    jz_str table;
    jazz_types_RowUuid row_uuid;
    u8 layer;
    jazz_model_ContributionComponent component;
} jazz_model_ContributionCoordinate;
typedef struct jazz_model_ContributionDot { jazz_model_TxId tx_id; jazz_model_ContributionCoordinate coordinate; } jazz_model_ContributionDot;
typedef JZ_VEC(jazz_model_ContributionDot) jazz_model_ContributionDotVec;
typedef struct jazz_model_ContributionSubstitution { jazz_model_ContributionCoordinate target; jazz_model_ContributionDotVec sources; } jazz_model_ContributionSubstitution;
typedef JZ_VEC(jazz_model_ContributionSubstitution) jazz_model_ContributionSubstitutionVec;
enum { JAZZ_COPY_BASE_CURRENT = 0, JAZZ_COPY_BASE_SNAPSHOT = 1 };
typedef struct jazz_model_BranchViewCopyBase { u8 tag; jazz_model_BranchKey branch; jazz_model_SnapshotRef snapshot; } jazz_model_BranchViewCopyBase;
typedef struct jazz_model_BranchViewCopyEvidence {
    u8 version;
    jazz_model_BranchKey head;
    jazz_model_BranchViewCopyBase base;
    jz_str table;
    jazz_types_RowUuid row_uuid;
    jazz_model_TxId source_version;
} jazz_model_BranchViewCopyEvidence;
typedef JZ_VEC(jazz_model_BranchViewCopyEvidence) jazz_model_BranchViewCopyEvidenceVec;
enum { JAZZ_BWO_EXACT_HEAD_INSERT = 0, JAZZ_BWO_EXACT_HEAD_UPDATE = 1, JAZZ_BWO_VIEW_UPDATE_COPY = 2 };
typedef struct jazz_model_BranchWriteOperation { u8 tag; jazz_model_BranchViewCopyEvidence evidence; } jazz_model_BranchWriteOperation;
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
    jazz_model_ContributionSubstitutionVec substitutions;
    jazz_model_BranchViewCopyEvidenceVec branch_view_copies;
    jazz_model_BranchWriteIntentVec branch_write_intents;
} jazz_model_ContributionMergeProvenance;
typedef jazz_model_SnapshotRef jazz_model_Snapshot; /* identical field layout */
typedef struct jazz_model_RowRead { jz_str table; jazz_types_RowUuid row_uuid; jazz_model_TxId version; } jazz_model_RowRead;
typedef struct jazz_model_AbsentRead { jz_str table; jazz_types_RowUuid row_uuid; } jazz_model_AbsentRead;
typedef struct jazz_model_PredicateRead {
    jz_str table;
    jazz_model_ShapeId shape_id;
    jazz_model_Query *shape;
    jazz_model_BindingId binding_id;
    jz_btree binding_values; /* String -> groove Value */
} jazz_model_PredicateRead;
typedef struct jazz_model_Transaction {
    jazz_model_TxId tx_id;
    u8 kind;
    u32 n_total_writes;
    jazz_types_AuthorSubject made_by;
    bool has_permission_subject; jazz_types_AuthorSubject permission_subject;
    bool has_base_snapshot; jazz_model_Snapshot base_snapshot;
    bool has_row_read_set; JZ_VEC(jazz_model_RowRead) row_read_set;
    bool has_absent_read_set; JZ_VEC(jazz_model_AbsentRead) absent_read_set;
    bool has_predicate_read_set; JZ_VEC(jazz_model_PredicateRead) predicate_read_set;
    jazz_model_OptStr user_metadata_json;
    jazz_model_ContributionMergeProvenance *contribution_merge; /* NULL = None */
} jazz_model_Transaction;
typedef struct jazz_model_TransactionRecord {
    jazz_model_TxId tx_id;
    jazz_types_AuthorSubject made_by;
    u8 kind;
    u32 n_total_writes;
    jazz_model_Fate fate;
    bool has_global_time; jazz_types_GlobalTime global_time;
    u8 durability;
    jazz_model_OptStr user_metadata_json;
    jazz_model_ContributionMergeProvenance *contribution_merge;
} jazz_model_TransactionRecord;
typedef struct jazz_model_HistoryEntry {
    jz_str table;
    groove_OwnedRecord version;
    jazz_model_TransactionRecord transaction;
    bool is_locally_current, is_globally_current;
} jazz_model_HistoryEntry;
typedef struct jazz_model_RejectedVersion { jz_str table; groove_OwnedRecord record; } jazz_model_RejectedVersion;
typedef struct jazz_model_RejectedTransaction { jazz_model_TxId tx_id; groove_OwnedRecord record; JZ_VEC(jazz_model_RejectedVersion) versions; } jazz_model_RejectedTransaction;

void jazz_model__tx__TxId__Debug__fmt(jz_str *out, const jazz_model_TxId *id);
bool jazz_model__tx__TxId__PartialEq__eq(const jazz_model_TxId *a, const jazz_model_TxId *b);
u64 jazz_model__tx__TxId__Hash__hash(const void *id);
bool jazz_model__tx__TxId__eq_v(const void *a, const void *b);
extern const jz_btree_desc jazz_model__tx__TXID_SET_DESC;
void jazz_model__tx__TxKind__Serialize__serialize(jz_pc_w *w, u8 kind);
jz_err jazz_model__tx__TxKind__Deserialize__deserialize(jz_pc_r *r, u8 *out);
void jazz_model__tx__DurabilityTier__Debug__fmt(jz_str *out, u8 tier);
void jazz_model__tx__DurabilityTier__Serialize__serialize(jz_pc_w *w, u8 tier);
jz_err jazz_model__tx__DurabilityTier__Deserialize__deserialize(jz_pc_r *r, u8 *out);
void jazz_model__tx__RejectionReason__drop(jazz_model_RejectionReason *r);
void jazz_model__tx__RejectionReason__Clone__clone(jazz_model_RejectionReason *dst, const jazz_model_RejectionReason *src);
bool jazz_model__tx__RejectionReason__PartialEq__eq(const jazz_model_RejectionReason *a, const jazz_model_RejectionReason *b);
void jazz_model__tx__RejectionReason__Debug__fmt(jz_str *out, const jazz_model_RejectionReason *r);
groove_ValueType jazz_model__tx__RejectionReason__storage_value_type(void);
jz_err jazz_model__tx__RejectionReason__to_storage_value(const jazz_model_RejectionReason *r, groove_Value *out);
jz_err jazz_model__tx__RejectionReason__from_storage_value(groove_Value value, jazz_model_RejectionReason *out);
void jazz_model__tx__Fate__drop(jazz_model_Fate *f);
void jazz_model__tx__Fate__Clone__clone(jazz_model_Fate *dst, const jazz_model_Fate *src);
bool jazz_model__tx__Fate__PartialEq__eq(const jazz_model_Fate *a, const jazz_model_Fate *b);
void jazz_model__tx__Fate__Serialize__serialize(jz_pc_w *w, const jazz_model_Fate *f);
jz_err jazz_model__tx__Fate__Deserialize__deserialize(jz_pc_r *r, jazz_model_Fate *out);
void jazz_model__tx__BranchViewCopyEvidence__drop(jazz_model_BranchViewCopyEvidence *e);
void jazz_model__tx__BranchViewCopyEvidence__Clone__clone(jazz_model_BranchViewCopyEvidence *dst, const jazz_model_BranchViewCopyEvidence *src);
bool jazz_model__tx__BranchViewCopyEvidence__PartialEq__eq(const jazz_model_BranchViewCopyEvidence *a, const jazz_model_BranchViewCopyEvidence *b);
void jazz_model__tx__BranchViewCopyEvidence__Serialize__serialize(jz_pc_w *w, const jazz_model_BranchViewCopyEvidence *e);
jz_err jazz_model__tx__BranchViewCopyEvidence__Deserialize__deserialize(jz_pc_r *r, jazz_model_BranchViewCopyEvidence *out);
int jazz_model__tx__ContributionCoordinate__Ord__cmp(const jazz_model_ContributionCoordinate *a, const jazz_model_ContributionCoordinate *b);
bool jazz_model__tx__ContributionCoordinate__PartialEq__eq(const jazz_model_ContributionCoordinate *a, const jazz_model_ContributionCoordinate *b);
void jazz_model__tx__ContributionMergeProvenance__drop(jazz_model_ContributionMergeProvenance *p);
void jazz_model__tx__ContributionMergeProvenance__Clone__clone(jazz_model_ContributionMergeProvenance *dst, const jazz_model_ContributionMergeProvenance *src);
bool jazz_model__tx__ContributionMergeProvenance__PartialEq__eq(const jazz_model_ContributionMergeProvenance *a, const jazz_model_ContributionMergeProvenance *b);
jz_err jazz_model__tx__ContributionMergeProvenance__canonical(jazz_model_BranchKey source, jazz_model_BranchKey target, jazz_model_ContributionSubstitutionVec substitutions, jazz_model_ContributionMergeProvenance *out);
void jazz_model__tx__ContributionMergeProvenance__branch_view_copy(jazz_model_BranchViewCopyEvidence evidence, jazz_model_ContributionMergeProvenance *out);
jz_err jazz_model__tx__ContributionMergeProvenance__validate(const jazz_model_ContributionMergeProvenance *p);
void jazz_model__tx__Transaction__drop(jazz_model_Transaction *t);
void jazz_model__tx__Transaction__Clone__clone(jazz_model_Transaction *dst, const jazz_model_Transaction *src);
bool jazz_model__tx__Transaction__PartialEq__eq(const jazz_model_Transaction *a, const jazz_model_Transaction *b);
jz_err jazz_model__tx__Transaction__Serialize__serialize(jz_pc_w *w, const jazz_model_Transaction *t);
jz_err jazz_model__tx__Transaction__Deserialize__deserialize(jz_pc_r *r, jazz_model_Transaction *out);
void jazz_model__tx__TransactionRecord__drop(jazz_model_TransactionRecord *t);
void jazz_model__tx__HistoryEntry__drop(jazz_model_HistoryEntry *h);
void jazz_model__tx__RejectedTransaction__drop(jazz_model_RejectedTransaction *t);
void jazz_model__tx__RejectedVersion__drop(jazz_model_RejectedVersion *v);
jz_str jazz_model__tx__RejectedVersion__table(const jazz_model_RejectedVersion *v);
jazz_types_RowUuid jazz_model__tx__RejectedVersion__row_uuid(const jazz_model_RejectedVersion *v);
bool jazz_model__tx__RejectedVersion__deletion(const jazz_model_RejectedVersion *v, u8 *out);


/* ============================================================ query (ast / policy_and_request_contracts) */
#define JAZZ_QUERY_NAMESPACE_STR "5d39e9ed-88f3-5b58-b8db-8786b02f5d2f"
extern const jz_uuid jazz_model__query__QUERY_NAMESPACE;
typedef JZ_VEC(u64) jazz_model_U64Vec;

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

/* drop / clone / eq (derive impls) */
#define JAZZ_QUERY_DERIVES(T, N) \
    void jazz_model__query__##N##__drop(T *v); \
    void jazz_model__query__##N##__drop_v(void *v); \
    void jazz_model__query__##N##__Clone__clone(T *dst, const T *src); \
    void jazz_model__query__##N##__Clone__clone_v(void *dst, const void *src); \
    bool jazz_model__query__##N##__PartialEq__eq(const T *a, const T *b);
JAZZ_QUERY_DERIVES(jazz_model_Operand, Operand)
JAZZ_QUERY_DERIVES(jazz_model_Predicate, Predicate)
JAZZ_QUERY_DERIVES(jazz_model_Include, Include)
JAZZ_QUERY_DERIVES(jazz_model_OrderBy, OrderBy)
JAZZ_QUERY_DERIVES(jazz_model_ArraySubquery, ArraySubquery)
JAZZ_QUERY_DERIVES(jazz_model_Aggregate, Aggregate)
JAZZ_QUERY_DERIVES(jazz_model_AggregateQuery, AggregateQuery)
JAZZ_QUERY_DERIVES(jazz_model_JoinCorrelation, JoinCorrelation)
JAZZ_QUERY_DERIVES(jazz_model_JoinSourceLookup, JoinSourceLookup)
JAZZ_QUERY_DERIVES(jazz_model_JoinVia, JoinVia)
JAZZ_QUERY_DERIVES(jazz_model_ReachableSeed, ReachableSeed)
JAZZ_QUERY_DERIVES(jazz_model_ReachableVia, ReachableVia)
JAZZ_QUERY_DERIVES(jazz_model_InheritsVia, InheritsVia)
JAZZ_QUERY_DERIVES(jazz_model_PolicyBranch, PolicyBranch)
JAZZ_QUERY_DERIVES(jazz_model_FlatJoinSource, FlatJoinSource)
JAZZ_QUERY_DERIVES(jazz_model_FlatJoin, FlatJoin)
JAZZ_QUERY_DERIVES(jazz_model_RelationColumnRef, RelationColumnRef)
JAZZ_QUERY_DERIVES(jazz_model_RelationValueRef, RelationValueRef)
JAZZ_QUERY_DERIVES(jazz_model_RelationPredicate, RelationPredicate)
JAZZ_QUERY_DERIVES(jazz_model_RelationKeyRef, RelationKeyRef)
JAZZ_QUERY_DERIVES(jazz_model_RelationProjectColumn, RelationProjectColumn)
JAZZ_QUERY_DERIVES(jazz_model_RelationJoinCondition, RelationJoinCondition)
JAZZ_QUERY_DERIVES(jazz_model_RelationOrderBy, RelationOrderBy)
JAZZ_QUERY_DERIVES(jazz_model_RelationUnionArm, RelationUnionArm)
JAZZ_QUERY_DERIVES(jazz_model_RelationExpr, RelationExpr)
JAZZ_QUERY_DERIVES(jazz_model_RelationQuery, RelationQuery)
void jazz_model__query__Query__drop_v(void *v);
void jazz_model__query__Query__Clone__clone_v(void *dst, const void *src);
int jazz_model__query__RelationColumnRef__Ord__cmp(const jazz_model_RelationColumnRef *a, const jazz_model_RelationColumnRef *b);
jazz_model_Predicate *jazz_model__query__Predicate__box(jazz_model_Predicate p);
jazz_model_RelationExpr *jazz_model__query__RelationExpr__box(jazz_model_RelationExpr e);
jazz_model_RecursionBound jazz_model__query__RecursionBound__default_max_depth(void);
u64 jazz_model__query__RecursionBound__depth_steps(jazz_model_RecursionBound b);

/* serde: Postcard (non-human) and JSON (human-readable serialize) */
void jazz_model__query__Operand__Serialize__serialize(jz_pc_w *w, const jazz_model_Operand *v);
jz_err jazz_model__query__Operand__Deserialize__deserialize(jz_pc_r *r, jazz_model_Operand *out);
void jazz_model__query__Operand__Serialize__serialize_json(jz_json_w *w, const jazz_model_Operand *v);
void jazz_model__query__Predicate__Serialize__serialize(jz_pc_w *w, const jazz_model_Predicate *v);
jz_err jazz_model__query__Predicate__Deserialize__deserialize(jz_pc_r *r, jazz_model_Predicate *out);
void jazz_model__query__Predicate__Serialize__serialize_json(jz_json_w *w, const jazz_model_Predicate *v);
void jazz_model__query__JoinVia__Serialize__serialize(jz_pc_w *w, const jazz_model_JoinVia *v);
jz_err jazz_model__query__JoinVia__Deserialize__deserialize(jz_pc_r *r, jazz_model_JoinVia *out);
void jazz_model__query__JoinVia__Serialize__serialize_json(jz_json_w *w, const jazz_model_JoinVia *v);
void jazz_model__query__Query__Serialize__serialize_json(jz_json_w *w, const jazz_model_Query *q);
void jazz_model__query__RelationQuery__Serialize__serialize_json(jz_json_w *w, const jazz_model_RelationQuery *q);

/* policy_limits */
#define JAZZ_MAX_POLICY_EXPRESSION_DEPTH 256u
#define JAZZ_MAX_POLICY_EXPRESSION_NODES 16384u
typedef struct jazz_model_PolicyExpressionLimitError { const char *limit; u64 max; u64 actual; } jazz_model_PolicyExpressionLimitError;
typedef struct jazz_model_PolicyExpressionBudget { u64 nodes; } jazz_model_PolicyExpressionBudget;
void jazz_model__query__policy_limits__PolicyExpressionLimitError__Display__fmt(jz_str *out, const jazz_model_PolicyExpressionLimitError *e);
bool jazz_model__query__policy_limits__PolicyExpressionBudget__enter(jazz_model_PolicyExpressionBudget *b, u64 depth, jazz_model_PolicyExpressionLimitError *err);
bool jazz_model__query__policy_limits__PolicyExpressionBudget__reserve(const jazz_model_PolicyExpressionBudget *b, u64 children, jazz_model_PolicyExpressionLimitError *err);
jz_err jazz_model__query__policy_limits__PolicyExpressionLimitError__into_err(const jazz_model_PolicyExpressionLimitError *e);

/* relation_codec */
#define JAZZ_MAX_RELATION_DEPTH 128u
jz_err jazz_model__query__relation_query_wire__serialize(jz_pc_w *w, const jazz_model_RelationQuery *value /* NULL = None */);
jz_err jazz_model__query__relation_query_wire__deserialize(jz_pc_r *r, jazz_model_RelationQuery **out);
jz_err jazz_model__query__relation_query_wire__serialize_required(jz_pc_w *w, const jazz_model_RelationQuery *value);
jz_err jazz_model__query__relation_query_wire__deserialize_required(jz_pc_r *r, jazz_model_RelationQuery *out);
jz_err jazz_model__query__encode_relation_query_postcard(const jazz_model_RelationQuery *value, jz_bytes *out);
jz_err jazz_model__query__decode_relation_query_postcard(jz_slice bytes, jazz_model_RelationQuery *out);

/* policy_and_request_contracts */
#define JAZZ_PROVIDER_CLAIM_PREFIX "\0claims:"
#define JAZZ_PROVIDER_CLAIM_PREFIX_LEN 8u
void jazz_model__query__PolicyBranch__alternatives_from_query(jazz_model_Query query, jazz_model_PolicyBranchVec *out);
jazz_model_ArraySubquery jazz_model__query__ArraySubquery__new(jz_str column_name, jz_str table, jz_str inner_column, jz_str outer_column);
typedef void (*jazz_model_OperandMutFn)(jazz_model_Operand *op, void *ctx);
typedef bool (*jazz_model_OperandPredFn)(const jazz_model_Operand *op, void *ctx);
typedef void (*jazz_model_PredicateMutFn)(jazz_model_Predicate *p, void *ctx);
void jazz_model__query__Predicate__for_each_operand_mut(jazz_model_Predicate *p, jazz_model_OperandMutFn f, void *ctx);
bool jazz_model__query__Predicate__any_operand(const jazz_model_Predicate *p, jazz_model_OperandPredFn f, void *ctx);
void jazz_model__query__Query__for_each_condition_mut(jazz_model_Query *q, jazz_model_PredicateMutFn f, void *ctx);
void jazz_model__query__JoinVia__for_each_condition_mut(jazz_model_JoinVia *j, jazz_model_PredicateMutFn f, void *ctx);
jz_str jazz_model__query__provider_claim_operand_key(jz_slice name);
jz_str jazz_model__query__provider_claim_path_operand_key(const jz_str *segments, u32 n);
void jazz_model__query__operand_claim_path(jz_slice name, jazz_model_StrVec *out);
void jazz_model__query__ShapeId__Debug__fmt(jz_str *out, const jazz_model_ShapeId *id);
void jazz_model__query__BindingId__Debug__fmt(jz_str *out, const jazz_model_BindingId *id);

/* combinators + builders (shipped subset) */
jazz_model_Operand jazz_model__query__col(jz_str name);
jazz_model_Operand jazz_model__query__param(jz_str name);
jazz_model_Operand jazz_model__query__claim(jz_str name);
jazz_model_Operand jazz_model__query__lit(groove_Value value);
jazz_model_Predicate jazz_model__query__binary(u8 tag, jazz_model_Operand left, jazz_model_Operand right);
jazz_model_Predicate jazz_model__query__all_of(jazz_model_PredicateVec predicates);
jazz_model_Predicate jazz_model__query__any_of(jazz_model_PredicateVec predicates);
jazz_model_Predicate jazz_model__query__not(jazz_model_Predicate predicate);
jazz_model_Predicate jazz_model__query__in_list(jazz_model_Operand left, jazz_model_OperandVec values);
jazz_model_Predicate jazz_model__query__is_null(jazz_model_Operand operand);
jazz_model_Query jazz_model__query__Query__from(jz_str table, jazz_model_OptStr alias);
bool jazz_model__query__Query__aggregate_input(const jazz_model_Query *q, jazz_model_Query *out);
void jazz_model__query__Query__filter(jazz_model_Query *q, jazz_model_Predicate predicate);
void jazz_model__query__Query__join_via_row_id(jazz_model_Query *q, jz_str table, jz_str source_column, jazz_model_PredicateVec filters);


#define JAZZ_QUERY_SERDE(T, N) \
    void jazz_model__query__##N##__Serialize__serialize(jz_pc_w *w, const T *v); \
    jz_err jazz_model__query__##N##__Deserialize__deserialize(jz_pc_r *r, T *out); \
    void jazz_model__query__##N##__Serialize__serialize_json(jz_json_w *w, const T *v);
JAZZ_QUERY_SERDE(jazz_model_PolicyBranch, PolicyBranch)
JAZZ_QUERY_SERDE(jazz_model_ReachableVia, ReachableVia)
JAZZ_QUERY_SERDE(jazz_model_ReachableSeed, ReachableSeed)
JAZZ_QUERY_SERDE(jazz_model_InheritsVia, InheritsVia)
JAZZ_QUERY_SERDE(jazz_model_Include, Include)
JAZZ_QUERY_SERDE(jazz_model_ArraySubquery, ArraySubquery)
JAZZ_QUERY_SERDE(jazz_model_OrderBy, OrderBy)
JAZZ_QUERY_SERDE(jazz_model_AggregateQuery, AggregateQuery)
JAZZ_QUERY_SERDE(jazz_model_Aggregate, Aggregate)
JAZZ_QUERY_SERDE(jazz_model_JoinCorrelation, JoinCorrelation)
JAZZ_QUERY_SERDE(jazz_model_JoinSourceLookup, JoinSourceLookup)
JAZZ_QUERY_SERDE(jazz_model_RecursionBound, RecursionBound)
JAZZ_QUERY_SERDE(jazz_model_FlatJoin, FlatJoin)
JAZZ_QUERY_SERDE(jazz_model_FlatJoinSource, FlatJoinSource)
JAZZ_QUERY_SERDE(jazz_model_FlatJoinOn, FlatJoinOn)

/* shared serde helpers (lib.c) */
void jazz_model__json_w_value(jz_json_w *w, const jz_json *v);   /* embed a serde_json::Value */
void jazz_model__OptStr__serialize_pc(jz_pc_w *w, const jazz_model_OptStr *s);
jz_err jazz_model__OptStr__deserialize_pc(jz_pc_r *r, jazz_model_OptStr *out);
void jazz_model__OptStr__serialize_json(jz_json_w *w, const jazz_model_OptStr *s);
void jazz_model__OptU64__serialize_pc(jz_pc_w *w, bool some, u64 v);
jz_err jazz_model__OptU64__deserialize_pc(jz_pc_r *r, bool *some, u64 *v);
void jazz_model__OptU64__serialize_json(jz_json_w *w, bool some, u64 v);
#define JAZZ_VEC_EQ(a, b, eq) ({ bool _r = (a)->len == (b)->len; \
    for (u32 _i = 0; _r && _i < (a)->len; _i++) _r = eq(&(a)->ptr[_i], &(b)->ptr[_i]); _r; })
/* push a zeroed slot and return it typed */
#define JAZZ_VEC_SLOT(v) ({ void *_s = jz_vec_push_slot(&(v), sizeof(*(v).ptr)); memset(_s, 0, sizeof(*(v).ptr)); (__typeof__((v).ptr))_s; })


/* ============================================================ schema (crate::schema) */
extern const jz_uuid jazz_model__schema__SCHEMA_VERSION_NAMESPACE;
#define JAZZ_AUTHORITY_POLICY_BINDINGS_STORE "jazz_authority_policy_bindings"
#define JAZZ_SUBSCRIPTION_WATERMARKS_STORE "jazz_subscription_watermarks_v1"
#define JAZZ_LOCAL_ROW_AVAILABILITY_STORE "jazz_local_row_availability_v1"
#define JAZZ_SUPERSEDED_WRITES_STORE "jazz_superseded_writes_v1"
#define JAZZ_SEQ_FIELD "seq"
#define JAZZ_BASE_SEQ_FIELD "base_seq"
#define JAZZ_BASE_PENDING_FIELD "base_pending"
#define JAZZ_COUNTER_SIGNS_FIELD "counter_signs"
#define JAZZ_APP_COLUMN_PREFIX "_app_"
#define JAZZ_GLOBAL_CURRENT_BY_SEQ_INDEX "by_seq"

enum { JAZZ_MERGE_STRATEGY_LWW = 0, JAZZ_MERGE_STRATEGY_COUNTER = 1, JAZZ_MERGE_STRATEGY_GSET = 2 };
enum { JAZZ_LVK_NOT_LARGE = 0, JAZZ_LVK_BYTES = 1, JAZZ_LVK_STRING = 2, JAZZ_LVK_JSON = 3 };
typedef struct jazz_model_ColumnTransformSemantics { bool bijective; bool canonical_equality_preserving; } jazz_model_ColumnTransformSemantics;

typedef struct jazz_model_ColumnSchema {
    jz_str name;
    groove_ValueType column_type;
    u8 large_value_kind;
    bool has_default; groove_Value default_;
} jazz_model_ColumnSchema;
typedef JZ_VEC(jazz_model_ColumnSchema) jazz_model_ColumnSchemaVec;

typedef struct jazz_model_WritePolicies {
    jazz_model_Query *insert_check, *update_using, *update_check, *delete_using; /* NULL = None */
} jazz_model_WritePolicies;

typedef struct jazz_model_TableSchema {
    jz_str name;
    jazz_model_ColumnSchemaVec columns;
    jazz_model_StrVec branch_by;
    jz_btree references;             /* String -> String */
    jazz_model_Query *read_policy;   /* NULL = None */
    jazz_model_WritePolicies write_policies;
    jz_btree indexed_columns;        /* BTreeSet<String> */
    jz_btree composite_indexes;      /* BTreeSet<Vec<String>> */
    jz_btree merge_strategies;       /* String -> u8 */
} jazz_model_TableSchema;
typedef JZ_VEC(jazz_model_TableSchema) jazz_model_TableSchemaVec;
typedef struct jazz_model_RuntimeSchema { jazz_model_TableSchemaVec tables; } jazz_model_RuntimeSchema;
typedef struct jazz_model_JazzSchema { jazz_model_public_Schema public_schema; jazz_model_RuntimeSchema runtime; } jazz_model_JazzSchema;

extern const jz_btree_desc jazz_model__schema__STR_STR_DESC;      /* BTreeMap<String, String> */
extern const jz_btree_desc jazz_model__schema__STRVEC_SET_DESC;   /* BTreeSet<Vec<String>> */
extern const jz_btree_desc jazz_model__schema__STR_MERGE_DESC;    /* BTreeMap<String, MergeStrategy> */

void jazz_model__schema__ColumnSchema__drop(jazz_model_ColumnSchema *c);
void jazz_model__schema__ColumnSchema__Clone__clone(jazz_model_ColumnSchema *d, const jazz_model_ColumnSchema *s);
bool jazz_model__schema__ColumnSchema__PartialEq__eq(const jazz_model_ColumnSchema *a, const jazz_model_ColumnSchema *b);
jazz_model_ColumnSchema jazz_model__schema__ColumnSchema__new(jz_str name, groove_ValueType column_type);
void jazz_model__schema__ColumnSchema__with_default(jazz_model_ColumnSchema *c, groove_Value value);
bool jazz_model__schema__ColumnSchema__is_nullable_json(const jazz_model_ColumnSchema *c);
groove_Value jazz_model__schema__ColumnSchema__storage_value(const jazz_model_ColumnSchema *c, groove_Value value);
jazz_model_ColumnSchema jazz_model__schema__ColumnSchema__From_groove__from(groove_ColumnSchema column);
groove_ValueType jazz_model__schema__storage_column_type(const jazz_model_ColumnSchema *c);
bool jazz_model__schema__registered_column_transform(jz_slice key, jazz_model_ColumnTransformSemantics *out);
bool jazz_model__schema__contains_internal_storage_type(const groove_ValueType *t);
u8 jazz_model__schema__large_value_kind_for_type(const groove_ValueType *t);

void jazz_model__schema__WritePolicies__drop(jazz_model_WritePolicies *p);
void jazz_model__schema__WritePolicies__Clone__clone(jazz_model_WritePolicies *d, const jazz_model_WritePolicies *s);
bool jazz_model__schema__WritePolicies__PartialEq__eq(const jazz_model_WritePolicies *a, const jazz_model_WritePolicies *b);
jazz_model_WritePolicies jazz_model__schema__WritePolicies__legacy(jazz_model_Query *policy);
u32 jazz_model__schema__WritePolicies__iter(const jazz_model_WritePolicies *p, const char *labels[4], const jazz_model_Query *policies[4]);
jazz_model_Query *jazz_model__schema__WritePolicies__any(const jazz_model_WritePolicies *p);
bool jazz_model__schema__WritePolicies__has_any(const jazz_model_WritePolicies *p);

void jazz_model__schema__TableSchema__drop(jazz_model_TableSchema *t);
void jazz_model__schema__TableSchema__drop_v(void *t);
void jazz_model__schema__TableSchema__Clone__clone(jazz_model_TableSchema *d, const jazz_model_TableSchema *s);
void jazz_model__schema__TableSchema__Clone__clone_v(void *d, const void *s);
bool jazz_model__schema__TableSchema__PartialEq__eq(const jazz_model_TableSchema *a, const jazz_model_TableSchema *b);
jazz_model_TableSchema jazz_model__schema__TableSchema__new(jz_str name, jazz_model_ColumnSchemaVec columns);
bool jazz_model__schema__TableSchema__has_any_policy(const jazz_model_TableSchema *t);
void jazz_model__schema__TableSchema__with_branch_column(jazz_model_TableSchema *t, jz_str column);
u8 jazz_model__schema__TableSchema__merge_strategy(const jazz_model_TableSchema *t, jz_slice column);
const jazz_model_ColumnSchema *jazz_model__schema__TableSchema__column(const jazz_model_TableSchema *t, jz_slice name);
groove_TableSchema jazz_model__schema__TableSchema__rejected_versions_storage_table(const jazz_model_TableSchema *t);
groove_TableSchema jazz_model__schema__TableSchema__history_storage_table(const jazz_model_TableSchema *t);
groove_TableSchema jazz_model__schema__TableSchema__authored_history_storage_table(const jazz_model_TableSchema *t);
groove_TableSchema jazz_model__schema__TableSchema__global_current_storage_table(const jazz_model_TableSchema *t);
groove_TableSchema jazz_model__schema__TableSchema__ahead_current_storage_table(const jazz_model_TableSchema *t);
void jazz_model__schema__TableSchema__global_current_indexed_columns(const jazz_model_TableSchema *t, jz_btree *out);
groove_RecordDescriptor jazz_model__schema__TableSchema__wire_record_descriptor(const jazz_model_TableSchema *t);

void jazz_model__schema__RuntimeSchema__drop(jazz_model_RuntimeSchema *s);
void jazz_model__schema__RuntimeSchema__Clone__clone(jazz_model_RuntimeSchema *d, const jazz_model_RuntimeSchema *s);
bool jazz_model__schema__RuntimeSchema__PartialEq__eq(const jazz_model_RuntimeSchema *a, const jazz_model_RuntimeSchema *b);
const jazz_model_TableSchema *jazz_model__schema__RuntimeSchema__table(const jazz_model_RuntimeSchema *s, jz_slice name);
jz_err jazz_model__schema__RuntimeSchema__project_branch_selector(const jazz_model_RuntimeSchema *s, const jazz_model_TableSchema *table,
    const jazz_model_BranchSelector *selector, jazz_model_BranchKey *key_out, jz_btree *cells_out);
jz_err jazz_model__schema__RuntimeSchema__project_branch_view_selector(const jazz_model_RuntimeSchema *s, const jazz_model_TableSchema *table,
    const jazz_model_BranchSelector *selector, jazz_model_BranchKey *key_out, jz_btree *cells_out);
jz_err jazz_model__schema__RuntimeSchema__validate_authored_branch_key(const jazz_model_RuntimeSchema *s, const jazz_model_TableSchema *table,
    const jazz_model_BranchKey *key, jz_btree *cells_out);
jz_err jazz_model__schema__RuntimeSchema__decode_persisted_branch_key(const jazz_model_TableSchema *table, jz_slice bytes, jazz_model_BranchKey *out);
jz_err jazz_model__schema__RuntimeSchema__validate_branch_key_for_table(const jazz_model_TableSchema *table, const jazz_model_BranchKey *key, jz_btree *cells_out);
groove_DatabaseSchema jazz_model__schema__RuntimeSchema__lower_to_groove(const jazz_model_RuntimeSchema *s);
groove_DatabaseSchema jazz_model__schema__RuntimeSchema__lower_catalogue_meta_to_groove(const jazz_model_RuntimeSchema *s);
void jazz_model__schema__RuntimeSchema__column_families(const jazz_model_RuntimeSchema *s, jazz_model_StrVec *out);
void jazz_model__schema__RuntimeSchema__physical_column_families(const jazz_model_RuntimeSchema *s, jazz_model_StrVec *out);
void jazz_model__schema__RuntimeSchema__storage_tables(const jazz_model_RuntimeSchema *s, groove_TableSchemaVec *out);
void jazz_model__schema__RuntimeSchema__catalogue_meta_storage_tables(const jazz_model_RuntimeSchema *s, groove_TableSchemaVec *out);
jz_bytes jazz_model__schema__RuntimeSchema__canonical_bytes(const jazz_model_RuntimeSchema *s);
jazz_types_SchemaVersionId jazz_model__schema__RuntimeSchema__version_id(const jazz_model_RuntimeSchema *s);

void jazz_model__schema__JazzSchema__drop(jazz_model_JazzSchema *s);
void jazz_model__schema__JazzSchema__Clone__clone(jazz_model_JazzSchema *d, const jazz_model_JazzSchema *s);
bool jazz_model__schema__JazzSchema__PartialEq__eq(const jazz_model_JazzSchema *a, const jazz_model_JazzSchema *b);
void jazz_model__schema__JazzSchema__empty(jazz_model_JazzSchema *out);
jz_err jazz_model__schema__JazzSchema__new(const jazz_model_public_Schema *schema, jazz_model_JazzSchema *out);
void jazz_model__schema__JazzSchema__without_permissions(const jazz_model_JazzSchema *s, jazz_model_JazzSchema *out);
bool jazz_model__schema__JazzSchema__has_same_lineage_structure(const jazz_model_JazzSchema *a, const jazz_model_JazzSchema *b);
void jazz_model__schema__JazzSchema__with_permissions_from(const jazz_model_JazzSchema *s, const jazz_model_JazzSchema *selected, jazz_model_JazzSchema *out);
const jazz_model_public_Schema *jazz_model__schema__JazzSchema__public_schema(const jazz_model_JazzSchema *s);
const jazz_model_TableSchemaVec *jazz_model__schema__JazzSchema__tables(const jazz_model_JazzSchema *s);
jazz_model_JazzSchema jazz_model__schema__JazzSchema__from_runtime(jazz_model_public_Schema public_schema, jazz_model_RuntimeSchema runtime);
const jazz_model_RuntimeSchema *jazz_model__schema__JazzSchema__runtime(const jazz_model_JazzSchema *s);

groove_RecordDescriptor jazz_model__schema__subscription_watermark_key_descriptor(void);
groove_RecordDescriptor jazz_model__schema__subscription_watermark_value_descriptor(void);
groove_RecordDescriptor jazz_model__schema__local_availability_record_descriptor(void);
groove_ValueType jazz_model__schema__contribution_merge_storage_type(void);
jz_str jazz_model__schema__app_storage_column_name(jz_slice column);
jz_str jazz_model__schema__global_current_index_name(jz_slice column);
jz_str jazz_model__schema__global_current_composite_index_name(const jazz_model_StrVec *columns);
bool jazz_model__schema__physical_column_epoch_is_compatible(const jazz_model_TableSchema *source_table, jz_slice source_column_name,
    const jazz_model_TableSchema *target_table, jz_slice target_column_name);
bool jazz_model__schema__physical_value_epoch_is_compatible(const groove_ValueType *source, const groove_ValueType *target);

/* model::public_schema_convert entry used by JazzSchema::new */
jz_err jazz_model__model__public_schema_convert__convert_public_schema(const jazz_model_public_Schema *schema, jazz_model_JazzSchema *out);
jz_err jazz_model__model__public_schema_convert__decode_public_schema_json(jz_slice bytes, jazz_model_JazzSchema *out);


/* ============================================================ query (validation / relation_resolution / canonical_request) */
enum {
    JAZZ_QE_RESERVED_AGGREGATE_ALIAS = 0, JAZZ_QE_AGGREGATE_OUTPUT_NAME_COLLISION, JAZZ_QE_UNSUPPORTED_FLAT_JOIN_COMBINATION,
    JAZZ_QE_UNKNOWN_TABLE, JAZZ_QE_UNKNOWN_COLUMN, JAZZ_QE_OPERAND_TYPE_MISMATCH, JAZZ_QE_CONTAINS_NEEDLE_TYPE_MISMATCH,
    JAZZ_QE_IN_CANDIDATE_TYPE_MISMATCH, JAZZ_QE_CLAIM_TYPE_MISMATCH, JAZZ_QE_UNSUPPORTED_RELATION_QUERY,
    JAZZ_QE_PARAM_TYPE_CONFLICT, JAZZ_QE_MISSING_PARAM, JAZZ_QE_UNKNOWN_PARAM, JAZZ_QE_PARAM_TYPE_MISMATCH,
    JAZZ_QE_JOIN_NOT_REF_COMPATIBLE, JAZZ_QE_BAD_INCLUDE_PATH, JAZZ_QE_UNSUPPORTED_MAGIC_COLUMN, JAZZ_QE_UNSUPPORTED_AUTHOR_ORDERING
};
/* QueryError: code JAZZ_MODEL_ERR_QUERY + variant; message is the thiserror Display text */
jz_err jazz_model__query__QueryError__new(u32 variant, const char *fmt, ...);
bool jazz_model__query__QueryError__is(const jz_err *e, u32 variant);

typedef struct jazz_model_ValidatedQuery {
    jazz_model_Query query;
    jazz_types_SchemaVersionId schema_version;
    jz_btree params;            /* String -> groove ColumnType */
    jz_bytes canonical;
    jazz_model_ShapeId shape_id;
} jazz_model_ValidatedQuery;
typedef struct jazz_model_Binding { jz_btree values; /* String -> groove Value */ jz_bytes canonical; } jazz_model_Binding;
extern const jz_btree_desc jazz_model__query__PARAM_TYPES_DESC; /* BTreeMap<String, ColumnType> */
void jazz_model__query__ValidatedQuery__drop(jazz_model_ValidatedQuery *v);
void jazz_model__query__ValidatedQuery__Clone__clone(jazz_model_ValidatedQuery *d, const jazz_model_ValidatedQuery *s);
jz_err jazz_model__query__ValidatedQuery__bind(const jazz_model_ValidatedQuery *v, jz_btree values, jazz_model_Binding *out);
void jazz_model__query__Binding__drop(jazz_model_Binding *b);
jazz_model_BindingId jazz_model__query__Binding__binding_id(const jazz_model_Binding *b);
jz_err jazz_model__query__canonical_binding_bytes_for_values(const jz_btree *values, jz_bytes *out);
jz_err jazz_model__query__binding_values_from_canonical_bytes(jz_slice bytes, jz_btree *out);
jz_err jazz_model__query__binding_id_for_values(const jz_btree *values, jazz_model_BindingId *out);
jz_err jazz_model__query__validate_query(const jazz_model_Query *q, const jazz_model_RuntimeSchema *schema, jazz_model_ValidatedQuery *out);
jz_err jazz_model__query__validate_query_with_schema_version(const jazz_model_Query *q, const jazz_model_RuntimeSchema *schema,
    jazz_types_SchemaVersionId version, jazz_model_ValidatedQuery *out);
jz_err jazz_model__query__validate_union_arm_with_schema_version(const jazz_model_Query *q, const jazz_model_RuntimeSchema *schema,
    jazz_types_SchemaVersionId version, jazz_model_ValidatedQuery *out);
jz_err jazz_model__query__Query__validate(const jazz_model_Query *q, const jazz_model_JazzSchema *schema, jazz_model_ValidatedQuery *out);
jz_err jazz_model__query__Query__validate_runtime(const jazz_model_Query *q, const jazz_model_RuntimeSchema *schema, jazz_model_ValidatedQuery *out);
jz_err jazz_model__query__Query__validate_with_schema_version(const jazz_model_Query *q, const jazz_model_RuntimeSchema *schema,
    jazz_types_SchemaVersionId version, jazz_model_ValidatedQuery *out);

/* relation_resolution */
typedef struct jazz_model_RelationUnionParts {
    jazz_model_RelationUnionArmVec inputs;
    bool has_order_by; jazz_model_RelationOrderByVec order_by;
    bool has_offset; u64 offset;
    bool has_limit; u64 limit;
} jazz_model_RelationUnionParts;
void jazz_model__query__RelationUnionParts__drop(jazz_model_RelationUnionParts *p);
bool jazz_model__query__relation_union_parts(const jazz_model_RelationExpr *expr, jazz_model_RelationUnionParts *out);
jz_err jazz_model__query__relation_query_to_query(const jazz_model_RelationQuery *query, jazz_model_Query *out);
jz_err jazz_model__query__relation_output_projection(const jazz_model_RelationQuery *relation, jz_str *scope, jazz_model_RelationProjectColumnVec *columns);
jz_err jazz_model__query__relation_output_projection_if_present(const jazz_model_RelationQuery *relation, bool *some, jazz_model_RelationProjectColumnVec *columns);
jz_err jazz_model__query__relation_union_leaf_projections(const jazz_model_RelationQuery *relation, jz_btree *out);
extern const jz_btree_desc jazz_model__query__STR_PROJECTION_DESC; /* BTreeMap<String, Vec<RelationProjectColumn>> */
jz_str jazz_model__query__compose_union_arm_path(jz_slice prefix, jz_slice label);
jz_err jazz_model__query__validate_relation_projection_aliases(const jazz_model_RelationProjectColumnVec *columns);
jz_err jazz_model__query__reject_reserved_relation_aliases(const jazz_model_RelationProjectColumnVec *columns);
bool jazz_model__query__relation_union_presentation_order(const jazz_model_RelationQuery *relation, jazz_model_OrderByVec *out);
jz_err jazz_model__query__relation_scope(const jazz_model_RelationColumnRef *column, jz_str *out);
jz_err jazz_model__query__json_value_to_record_value(const jz_json *value, groove_Value *out);

/* canonical_request */
void jazz_model__query__normalize_query(const jazz_model_Query *q, jazz_model_Query *out);
jz_err jazz_model__query__value_type(const groove_Value *value, groove_ValueType *out);
bool jazz_model__query__value_matches_type(const groove_Value *value, const groove_ValueType *column_type);
jz_err jazz_model__query__validate_binding_value(const groove_Value *value);
/* validation helpers shared with canonical_request */
jz_err jazz_model__query__schema_table(const jazz_model_RuntimeSchema *schema, jz_slice name, const jazz_model_TableSchema **out);
jz_err jazz_model__query__planner_column_type(const jazz_model_TableSchema *table, jz_slice column, const groove_ValueType **out);
jz_err jazz_model__query__flat_join_predicate_sources(const jazz_model_Predicate *predicate, jz_btree *out /* BTreeSet<String> */);
jz_err jazz_model__query__unqualify_flat_join_predicate(const jazz_model_Predicate *predicate, jz_slice expected_scope, jazz_model_Predicate *out);
jz_err jazz_model__query__canonical_query_bytes_for_schema(const jazz_model_Query *q, const jazz_model_RuntimeSchema *schema, jz_bytes *out);
jz_err jazz_model__query__canonical_binding_bytes(const jz_btree *values, jz_bytes *out);

#endif
