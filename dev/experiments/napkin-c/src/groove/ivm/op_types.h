/* op_types.h: operator descriptors carried by IVM graph nodes
 * (crates/groove/src/ivm/op_types.rs) plus the graph.rs payload types they
 * embed (FieldRef, ProjectExpr, DurableStorage, IndexCandidateFilter). */
#ifndef GROOVE_IVM_OP_TYPES_H
#define GROOVE_IVM_OP_TYPES_H
#include "jz_rt.h"
#include "../_ext_groove-ivm-db.h"

typedef JZ_VEC(u32) groove_UsizeVec;
/* groove_StrVec: declared in _ext_groove-ivm-db.h */
typedef JZ_VEC(jz_bytes) groove_BytesVec;
typedef struct groove_OptU8 { bool some; u8 v; } groove_OptU8;
typedef struct groove_OptU32 { bool some; u32 v; } groove_OptU32;
typedef struct groove_OptStr { bool some; jz_str v; } groove_OptStr;
typedef JZ_VEC(groove_OptU8) groove_OptU8Vec;
typedef JZ_VEC(groove_OptU32) groove_OptU32Vec;
typedef JZ_VEC(groove_OptStr) groove_OptStrVec;

/* ---- shared small-container helpers (defined in op_types.c) ---- */
void groove__ivm__strvec_clone(groove_StrVec *dst, const groove_StrVec *src);
void groove__ivm__strvec_drop(groove_StrVec *v);
bool groove__ivm__strvec_eq(const groove_StrVec *a, const groove_StrVec *b);
void groove__ivm__strvec_hash(const groove_StrVec *v, groove_Hasher *h);
void groove__ivm__usizevec_clone(groove_UsizeVec *dst, const groove_UsizeVec *src);
bool groove__ivm__usizevec_eq(const groove_UsizeVec *a, const groove_UsizeVec *b);
void groove__ivm__usizevec_hash(const groove_UsizeVec *v, groove_Hasher *h);
void groove__ivm__bytesvec_clone(groove_BytesVec *dst, const groove_BytesVec *src);
void groove__ivm__bytesvec_drop(groove_BytesVec *v);
bool groove__ivm__bytesvec_eq(const groove_BytesVec *a, const groove_BytesVec *b);
void groove__ivm__bytesvec_hash(const groove_BytesVec *v, groove_Hasher *h);
void groove__ivm__jz_str_drop_v(void *s);
void groove__ivm__jz_str_clone_v(void *dst, const void *src);
void groove__ivm__jz_bytes_drop_v(void *b);
extern const jz_btree_desc groove__ivm__STRSET_DESC; /* BTreeSet<String> */

/* ---- LiteralValue ---- */
enum {
    GROOVE_LIT_U8, GROOVE_LIT_U16, GROOVE_LIT_U32, GROOVE_LIT_U64, GROOVE_LIT_U48, GROOVE_LIT_I64,
    GROOVE_LIT_I32, GROOVE_LIT_F64, GROOVE_LIT_BOOL, GROOVE_LIT_ENUM_TAG, GROOVE_LIT_STRING,
    GROOVE_LIT_BYTES, GROOVE_LIT_UUID, GROOVE_LIT_TUPLE, GROOVE_LIT_ARRAY, GROOVE_LIT_NULLABLE,
    GROOVE_LIT_NATIVE_RECORD, GROOVE_LIT_RECORD
};
typedef struct groove_LiteralValue groove_LiteralValue;
typedef JZ_VEC(groove_LiteralValue) groove_LiteralVec;
struct groove_LiteralValue {
    u8 tag;
    union {
        u8 u8_; u16 u16_; u32 u32_; u64 u64_; i64 i64_; i32 i32_; bool b;
        jz_str s; jz_bytes bytes; jz_uuid uuid;
        groove_LiteralVec items;          /* Tuple / Array */
        groove_LiteralValue *nullable;    /* Nullable(Option<Box<_>>): NULL = None */
        struct { jz_bytes descriptor; jz_bytes raw; } native;
    } u;
};
void groove__ivm__op_types__LiteralValue__Clone__clone(groove_LiteralValue *dst, const groove_LiteralValue *src);
void groove__ivm__op_types__LiteralValue__drop(groove_LiteralValue *v);
bool groove__ivm__op_types__LiteralValue__PartialEq__eq(const groove_LiteralValue *a, const groove_LiteralValue *b);
int groove__ivm__op_types__LiteralValue__PartialOrd__partial_cmp(const groove_LiteralValue *a, const groove_LiteralValue *b);
void groove__ivm__op_types__LiteralValue__Hash__hash(const groove_LiteralValue *v, groove_Hasher *h);
void groove__ivm__op_types__LiteralValue__Debug__fmt(jz_str *out, const groove_LiteralValue *v);
void groove__ivm__op_types__LiteralValue__From_Value__from(groove_LiteralValue *out, groove_Value *value);
bool groove__ivm__op_types__LiteralValue__value_type(const groove_LiteralValue *v, groove_ValueType *out);
void groove__ivm__op_types__LiteralValue__to_value(const groove_LiteralValue *v, groove_Value *out);
void groove__ivm__literalvec_clone(groove_LiteralVec *dst, const groove_LiteralVec *src);
void groove__ivm__literalvec_drop(groove_LiteralVec *v);
bool groove__ivm__literalvec_eq(const groove_LiteralVec *a, const groove_LiteralVec *b);
void groove__ivm__literalvec_hash(const groove_LiteralVec *v, groove_Hasher *h);
void groove__ivm__literalvec_debug(jz_str *out, const groove_LiteralVec *v);

/* ---- StaticScanSpec ---- */
enum { GROOVE_SCAN_POINT, GROOVE_SCAN_PREFIX, GROOVE_SCAN_PREFIX_LIMIT, GROOVE_SCAN_REVERSE_PREFIX_LIMIT, GROOVE_SCAN_RANGE };
typedef struct groove_StaticScanSpec {
    u8 tag;
    groove_LiteralVec values;  /* Point/Prefix values, *Limit prefix, Range start */
    groove_LiteralVec end;     /* Range end */
    u32 max_items;             /* *Limit */
} groove_StaticScanSpec;
void groove__ivm__op_types__StaticScanSpec__Clone__clone(groove_StaticScanSpec *dst, const groove_StaticScanSpec *src);
void groove__ivm__op_types__StaticScanSpec__drop(groove_StaticScanSpec *v);
bool groove__ivm__op_types__StaticScanSpec__PartialEq__eq(const groove_StaticScanSpec *a, const groove_StaticScanSpec *b);
void groove__ivm__op_types__StaticScanSpec__Hash__hash(const groove_StaticScanSpec *v, groove_Hasher *h);
void groove__ivm__op_types__StaticScanSpec__Debug__fmt(jz_str *out, const groove_StaticScanSpec *v);

/* ---- VariantProjectionTarget ---- */
enum { GROOVE_VPT_NAMED, GROOVE_VPT_SCHEMA_INDEX };
typedef struct groove_VariantProjectionTarget { u8 tag; jz_str name; } groove_VariantProjectionTarget;

/* ---- FieldRef (graph.rs) ---- */
enum { GROOVE_FIELDREF_NAME, GROOVE_FIELDREF_STORED_NAME, GROOVE_FIELDREF_RESOLVED };
typedef struct groove_FieldRef { u8 tag; jz_str name; u32 index; } groove_FieldRef;
typedef JZ_VEC(groove_FieldRef) groove_FieldRefVec;
void groove__ivm__graph__FieldRef__Clone__clone(groove_FieldRef *dst, const groove_FieldRef *src);
void groove__ivm__graph__FieldRef__drop(groove_FieldRef *v);
bool groove__ivm__graph__FieldRef__PartialEq__eq(const groove_FieldRef *a, const groove_FieldRef *b);
void groove__ivm__graph__FieldRef__Hash__hash(const groove_FieldRef *v, groove_Hasher *h);
void groove__ivm__graph__FieldRef__Debug__fmt(jz_str *out, const groove_FieldRef *v);
groove_FieldRef groove__ivm__graph__FieldRef__name(jz_str name);
groove_FieldRef groove__ivm__graph__FieldRef__resolved(u32 index);
groove_FieldRef groove__ivm__graph__FieldRef__stored_name(jz_str name);
jz_str groove__ivm__graph__FieldRef__display_name(const groove_FieldRef *f);

/* ---- DurableStorage (graph.rs) ---- */
typedef struct groove_DurableStorage { jz_str column_family; jz_bytes key_prefix; jz_str name; } groove_DurableStorage;
void groove__ivm__graph__DurableStorage__Clone__clone(groove_DurableStorage *dst, const groove_DurableStorage *src);
void groove__ivm__graph__DurableStorage__drop(groove_DurableStorage *v);
bool groove__ivm__graph__DurableStorage__PartialEq__eq(const groove_DurableStorage *a, const groove_DurableStorage *b);
void groove__ivm__graph__DurableStorage__Hash__hash(const groove_DurableStorage *v, groove_Hasher *h);
void groove__ivm__graph__DurableStorage__Debug__fmt(jz_str *out, const groove_DurableStorage *v);

/* ---- IndexCandidateFilter (graph.rs) ---- */
typedef struct groove_IndexCandidateFilter {
    jz_str table, index; groove_StaticScanSpec scan; jz_str source_column, candidate_column;
} groove_IndexCandidateFilter;
void groove__ivm__graph__IndexCandidateFilter__Clone__clone(groove_IndexCandidateFilter *dst, const groove_IndexCandidateFilter *src);
void groove__ivm__graph__IndexCandidateFilter__drop(groove_IndexCandidateFilter *v);
bool groove__ivm__graph__IndexCandidateFilter__PartialEq__eq(const groove_IndexCandidateFilter *a, const groove_IndexCandidateFilter *b);
void groove__ivm__graph__IndexCandidateFilter__Hash__hash(const groove_IndexCandidateFilter *v, groove_Hasher *h);
void groove__ivm__graph__IndexCandidateFilter__Debug__fmt(jz_str *out, const groove_IndexCandidateFilter *v);

/* ---- RecursiveEnumRemaps: BTreeMap<String, Vec<Option<_>>> x3 ---- */
typedef struct groove_RecursiveEnumRemaps { jz_btree scalar, payload, payload_children; } groove_RecursiveEnumRemaps;
void groove__ivm__op_types__RecursiveEnumRemaps__Default__default(groove_RecursiveEnumRemaps *v);
void groove__ivm__op_types__RecursiveEnumRemaps__Clone__clone(groove_RecursiveEnumRemaps *dst, const groove_RecursiveEnumRemaps *src);
void groove__ivm__op_types__RecursiveEnumRemaps__drop(groove_RecursiveEnumRemaps *v);
bool groove__ivm__op_types__RecursiveEnumRemaps__PartialEq__eq(const groove_RecursiveEnumRemaps *a, const groove_RecursiveEnumRemaps *b);
void groove__ivm__op_types__RecursiveEnumRemaps__Hash__hash(const groove_RecursiveEnumRemaps *v, groove_Hasher *h);
void groove__ivm__op_types__RecursiveEnumRemaps__Debug__fmt(jz_str *out, const groove_RecursiveEnumRemaps *v);

/* ---- ProjectExpr (graph.rs) ---- */
enum {
    GROOVE_PEXPR_TEMPLATE_ARGUMENT, GROOVE_PEXPR_FIELD, GROOVE_PEXPR_RECORD_FIELD, GROOVE_PEXPR_LITERAL,
    GROOVE_PEXPR_TYPED_LITERAL, GROOVE_PEXPR_NULL, GROOVE_PEXPR_NULLABLE, GROOVE_PEXPR_NULLABLE_FLAT,
    GROOVE_PEXPR_ENUM_TAG_REMAP, GROOVE_PEXPR_ENUM_REMAP, GROOVE_PEXPR_RECURSIVE_ENUM_REMAP
};
typedef struct groove_ProjectExpr {
    u8 tag;
    union {
        struct { u32 slot; groove_ValueType value_type; } template_argument;
        groove_FieldRef field;   /* Field / Nullable / NullableFlat */
        struct { groove_FieldRef source; groove_StrVec path; } record_field;
        groove_LiteralValue literal;
        struct { groove_LiteralValue value; groove_ValueType value_type; } typed_literal;
        groove_ValueType null_type;
        struct { groove_FieldRef source; groove_OptU8Vec tags; } enum_tag_remap;
        struct { groove_FieldRef source; groove_OptU32Vec tags; } enum_remap;
        struct { groove_FieldRef source; groove_ValueType target; groove_RecursiveEnumRemaps remaps; bool omit_unrepresentable; } recursive;
    } u;
} groove_ProjectExpr;
void groove__ivm__graph__ProjectExpr__Clone__clone(groove_ProjectExpr *dst, const groove_ProjectExpr *src);
void groove__ivm__graph__ProjectExpr__drop(groove_ProjectExpr *v);
bool groove__ivm__graph__ProjectExpr__PartialEq__eq(const groove_ProjectExpr *a, const groove_ProjectExpr *b);
void groove__ivm__graph__ProjectExpr__Hash__hash(const groove_ProjectExpr *v, groove_Hasher *h);
void groove__ivm__graph__ProjectExpr__Debug__fmt(jz_str *out, const groove_ProjectExpr *v);

/* ---- PredicateExpr ---- */
enum {
    GROOVE_PRED_TEMPLATE_ARGUMENT, GROOVE_PRED_EQ, GROOVE_PRED_NEQ, GROOVE_PRED_CONTAINS, GROOVE_PRED_EQ_FIELD,
    GROOVE_PRED_CONTAINS_FIELD, GROOVE_PRED_NEQ_FIELD, GROOVE_PRED_GT, GROOVE_PRED_GT_EQ, GROOVE_PRED_LT,
    GROOVE_PRED_LT_EQ, GROOVE_PRED_IS_NULL, GROOVE_PRED_IS_NOT_NULL, GROOVE_PRED_ENUM_MATCH, GROOVE_PRED_AND,
    GROOVE_PRED_OR
};
typedef struct groove_PredicateExpr groove_PredicateExpr;
typedef JZ_VEC(groove_PredicateExpr) groove_PredicateVec;
struct groove_PredicateExpr {
    u8 tag;
    union {
        struct { u32 slot; groove_StrVec fields; } template_argument;
        struct { jz_str field; groove_LiteralValue value; } lit;     /* Eq Neq Contains Gt GtEq Lt LtEq */
        struct { jz_str field; jz_str other; } fields;              /* EqField ContainsField NeqField */
        struct { jz_str field; } null;                              /* IsNull IsNotNull */
        struct { jz_str field; u32 case_tag; groove_PredicateExpr *payload; } enum_match;
        groove_PredicateVec children;                               /* And Or */
    } u;
};
void groove__ivm__op_types__PredicateExpr__Clone__clone(groove_PredicateExpr *dst, const groove_PredicateExpr *src);
void groove__ivm__op_types__PredicateExpr__drop(groove_PredicateExpr *v);
void groove__ivm__op_types__PredicateExpr__drop_v(void *v);
bool groove__ivm__op_types__PredicateExpr__PartialEq__eq(const groove_PredicateExpr *a, const groove_PredicateExpr *b);
int groove__ivm__op_types__PredicateExpr__PartialOrd__partial_cmp(const groove_PredicateExpr *a, const groove_PredicateExpr *b);
void groove__ivm__op_types__PredicateExpr__Hash__hash(const groove_PredicateExpr *v, groove_Hasher *h);
void groove__ivm__op_types__PredicateExpr__Debug__fmt(jz_str *out, const groove_PredicateExpr *v);
bool groove__ivm__op_types__PredicateExpr__accepts_template_argument(const groove_PredicateExpr *self, const groove_PredicateExpr *argument);
bool groove__ivm__op_types__PredicateExpr__has_template_arguments(const groove_PredicateExpr *self);
void groove__ivm__op_types__PredicateExpr__canonicalize(groove_PredicateExpr *out, groove_PredicateExpr self);
groove_PredicateExpr groove__ivm__op_types__PredicateExpr__eq(jz_str field, groove_Value value);
groove_PredicateExpr groove__ivm__op_types__PredicateExpr__gt(jz_str field, groove_Value value);
groove_PredicateExpr groove__ivm__op_types__PredicateExpr__is_null(jz_str field);
groove_PredicateExpr groove__ivm__op_types__PredicateExpr__is_not_null(jz_str field);
enum { GROOVE_PK_EQ, GROOVE_PK_NEQ, GROOVE_PK_GT, GROOVE_PK_GT_EQ, GROOVE_PK_LT, GROOVE_PK_LT_EQ };
groove_PredicateExpr groove__ivm__op_types__PredicateExpr__from_field_literal(u8 kind, jz_str field, groove_LiteralValue value);
u8 groove__ivm__op_types__PredicateKind__reversed(u8 kind);
/* owned by ivm::runtime::subscriptions (another unit) */
void groove__ivm__runtime__subscriptions__PredicateExpr__referenced_fields(const groove_PredicateExpr *self, jz_btree *output);

/* ---- PlanExpr ---- */
enum {
    GROOVE_PLAN_FIELD, GROOVE_PLAN_LITERAL, GROOVE_PLAN_NULL, GROOVE_PLAN_NULLABLE, GROOVE_PLAN_NULLABLE_FLAT,
    GROOVE_PLAN_ENUM_TAG_REMAP, GROOVE_PLAN_ENUM_REMAP, GROOVE_PLAN_RECURSIVE_ENUM_REMAP
};
typedef struct groove_PlanExpr {
    u8 tag;
    union {
        jz_str field;   /* Field / Nullable / NullableFlat */
        groove_LiteralValue literal;
        groove_ValueType null_type;
        struct { jz_str field; groove_OptU8Vec tags; } enum_tag_remap;
        struct { jz_str field; groove_OptU32Vec tags; } enum_remap;
        struct { jz_str field; groove_RecursiveEnumRemaps remaps; bool omit_unrepresentable; } recursive;
    } u;
} groove_PlanExpr;
typedef JZ_VEC(groove_PlanExpr) groove_PlanExprVec;
void groove__ivm__op_types__PlanExpr__Clone__clone(groove_PlanExpr *dst, const groove_PlanExpr *src);
void groove__ivm__op_types__PlanExpr__drop(groove_PlanExpr *v);
bool groove__ivm__op_types__PlanExpr__PartialEq__eq(const groove_PlanExpr *a, const groove_PlanExpr *b);
void groove__ivm__op_types__PlanExpr__Hash__hash(const groove_PlanExpr *v, groove_Hasher *h);
void groove__ivm__op_types__PlanExpr__Debug__fmt(jz_str *out, const groove_PlanExpr *v);
groove_PlanExpr groove__ivm__op_types__PlanExpr__field(jz_str name);
groove_PlanExpr groove__ivm__op_types__PlanExpr__literal(groove_LiteralValue v);
groove_PlanExpr groove__ivm__op_types__PlanExpr__null(groove_ValueType t);
groove_PlanExpr groove__ivm__op_types__PlanExpr__nullable(jz_str name);
groove_PlanExpr groove__ivm__op_types__PlanExpr__nullable_flat(jz_str name);

/* ---- operator structs ---- */
enum { GROOVE_CMP_EXACT, GROOVE_CMP_POLICY };   /* ValueComparison */

typedef struct groove_TableSourceOp {
    jz_str table;
    bool has_scan; groove_StaticScanSpec scan;
    bool has_variant_projection; groove_VariantProjectionTarget variant_projection;
} groove_TableSourceOp;
typedef struct groove_LookupOp { groove_TableSourceOp source; groove_RecordDescriptor key_descriptor; groove_UsizeVec key_fields; } groove_LookupOp;
typedef struct groove_IndexSourceDurableIds { u32 index; JZ_VEC(u32) intersections; bool has_candidate; u32 candidate; } groove_IndexSourceDurableIds;
typedef struct groove_ScanIntersection { jz_str index; groove_StaticScanSpec scan; } groove_ScanIntersection;
typedef JZ_VEC(groove_ScanIntersection) groove_ScanIntersectionVec;
typedef struct groove_IndexSourceOp {
    jz_str table, index;
    groove_IndexSourceDurableIds *durable_ids; /* Box */
    groove_ScanIntersectionVec intersections;
    bool has_candidate_filter; groove_IndexCandidateFilter candidate_filter;
    groove_RecordDescriptor input_descriptor;
    bool has_variant_projection; groove_VariantProjectionTarget variant_projection;
    bool has_row_projection; groove_VariantProjectionTarget row_projection;
    groove_UsizeVec key_fields, value_fields;
    bool unique, append_value_to_key, store_value;
    bool has_scan; groove_StaticScanSpec scan;
} groove_IndexSourceOp;
typedef struct groove_InlineRecordsOp { groove_BytesVec records; } groove_InlineRecordsOp;
typedef struct groove_FrontierSourceOp { jz_str binding; } groove_FrontierSourceOp;
enum { GROOVE_BSK_PREPARED, GROOVE_BSK_RUNTIME_INPUT };
typedef struct groove_BindingSourceKey { u8 tag; jz_str prepared; u64 runtime_namespace, local; } groove_BindingSourceKey;
typedef struct groove_BindingSourceOp { groove_BindingSourceKey key; } groove_BindingSourceOp;
typedef struct groove_PersistOp { jz_str name; groove_DurableStorage storage; groove_UsizeVec key_fields; bool unique; } groove_PersistOp;
typedef struct groove_FilterOp { groove_PredicateExpr predicate; u8 comparison; } groove_FilterOp;
typedef struct groove_ProjectionExpr {
    groove_ProjectExpr expression;
    bool has_output_name; jz_str output_name;
    groove_FieldIdentity output_identity;
} groove_ProjectionExpr;
typedef struct groove_UsizePair { u32 a, b; } groove_UsizePair;
typedef struct groove_MapProjectOp { JZ_VEC(groove_ProjectionExpr) expressions; JZ_VEC(groove_UsizePair) mapping; } groove_MapProjectOp;
typedef struct groove_StreamingChecksumOp { jz_str field; u32 field_idx; jz_str output_field; u32 window_bytes, max_bytes_per_turn; } groove_StreamingChecksumOp;
typedef struct groove_UnwrapNullableOp { jz_str field; u32 field_idx; } groove_UnwrapNullableOp;
typedef struct groove_UnnestOp { jz_str array_field; u32 array_field_idx; jz_str element_field; } groove_UnnestOp;
typedef struct groove_VariantProjectOp { jz_str field; u32 field_idx; u32 tag; } groove_VariantProjectOp;
typedef struct groove_IndexByOp {
    groove_PlanExprVec key_expressions, value_expressions;
    bool has_explicit_index; groove_IndexSchema explicit_index;
    groove_UsizeVec key_fields, value_fields;
    bool unique, append_value_to_key, store_value;
    bool has_scan; groove_StaticScanSpec scan;
} groove_IndexByOp;
enum { GROOVE_JOIN_INNER, GROOVE_JOIN_LEFT, GROOVE_JOIN_RIGHT, GROOVE_JOIN_FULL };
typedef struct groove_JoinOp {
    u8 kind;
    groove_PlanExprVec left_key, right_key;
    groove_RecordDescriptor left_descriptor, right_descriptor;
    bool has_residual_predicate; groove_PlanExpr residual_predicate;
    u8 comparison;
} groove_JoinOp;
typedef struct groove_RecursiveOp {
    jz_str frontier; u32 max_iters; bool truncate_at_max_iters; groove_StrVec read_tables;
    bool has_step_witness_output; groove_RecordDescriptor step_witness_output;
} groove_RecursiveOp;
/* ArgMaxByOp and ArgMinByOp have identical payloads: one C struct. */
typedef struct groove_ArgExtremumByOp {
    groove_StrVec group_fields, order_fields;
    groove_UsizeVec group_field_indices, comparison_field_indices;
} groove_ArgExtremumByOp;
typedef groove_ArgExtremumByOp groove_ArgMaxByOp;
typedef groove_ArgExtremumByOp groove_ArgMinByOp;
enum { GROOVE_TOPBY_FINITE, GROOVE_TOPBY_UNBOUNDED };
typedef struct groove_TopByLimit { u8 tag; u64 n; } groove_TopByLimit;
enum { GROOVE_DIR_ASC, GROOVE_DIR_DESC };
typedef struct groove_TopByOrderField { jz_str field; u8 direction; } groove_TopByOrderField;
typedef JZ_VEC(groove_TopByOrderField) groove_TopByOrderFieldVec;
typedef JZ_VEC(u8) groove_DirectionVec;
typedef struct groove_TopByOp {
    groove_StrVec group_fields; groove_UsizeVec group_field_indices;
    groove_TopByOrderFieldVec order_fields; groove_StrVec tie_fields;
    groove_UsizeVec sort_field_indices; groove_DirectionVec sort_directions;
    u64 offset; groove_TopByLimit limit;
} groove_TopByOp;
typedef struct groove_CollectByProjection { jz_str field; u32 field_idx; jz_str output_name; bool unwrap_nullable; } groove_CollectByProjection;
typedef JZ_VEC(groove_CollectByProjection) groove_CollectByProjectionVec;
typedef struct groove_CollectBySlot groove_CollectBySlot;
typedef JZ_VEC(groove_CollectBySlot) groove_CollectBySlotVec;
struct groove_CollectBySlot {
    groove_StrVec group_fields; groove_UsizeVec group_field_indices;
    groove_CollectByProjectionVec child_fields; groove_RecordDescriptor child_descriptor;
    jz_str collection_field; u32 collection_field_index;
    groove_CollectBySlotVec slots;
    groove_TopByOrderFieldVec order_fields; groove_StrVec tie_fields;
    bool has_presence_field_index; u32 presence_field_index;
    bool has_reference_array_field_index; u32 reference_array_field_index;
    bool reference_order;
    groove_UsizeVec sort_field_indices; groove_DirectionVec sort_directions;
    u64 offset; groove_TopByLimit limit;
};
enum { GROOVE_COLLECT_MODE_COLLECT, GROOVE_COLLECT_MODE_ROOT, GROOVE_COLLECT_MODE_EXPAND };
typedef struct groove_CollectByOp {
    u8 mode;
    groove_StrVec group_fields; groove_UsizeVec group_field_indices;
    groove_CollectByProjectionVec parent_fields, child_fields;
    groove_RecordDescriptor child_descriptor;
    jz_str collection_field; u32 collection_field_index;
    groove_CollectBySlotVec slots;
    groove_CollectByProjectionVec tuple_fields;
    groove_StrVec occurrence_id_fields; groove_UsizeVec occurrence_id_field_indices;
    groove_TopByOrderFieldVec order_fields; groove_StrVec tie_fields;
    groove_UsizeVec sort_field_indices; groove_DirectionVec sort_directions;
    u64 offset; groove_TopByLimit limit;
} groove_CollectByOp;
enum { GROOVE_AGG_COUNT, GROOVE_AGG_SUM, GROOVE_AGG_AVG, GROOVE_AGG_MIN, GROOVE_AGG_MAX };
typedef struct groove_AggregateExpr {
    u8 function;
    bool has_expression; groove_PlanExpr expression;
    bool distinct;
    bool has_output_name; jz_str output_name;
    bool has_output_identity; groove_FieldIdentity output_identity;
} groove_AggregateExpr;
typedef JZ_VEC(groove_AggregateExpr) groove_AggregateExprVec;
typedef struct groove_AggregateOp { groove_PlanExprVec group_key; groove_UsizeVec group_field_indices; groove_AggregateExprVec aggregates; } groove_AggregateOp;

/* derive impls for operator structs (one set each, op_types.c) */
#define GROOVE_OP_DERIVES(T, path) \
    void path##__Clone__clone(T *dst, const T *src); \
    void path##__drop(T *v); \
    bool path##__PartialEq__eq(const T *a, const T *b); \
    void path##__Hash__hash(const T *v, groove_Hasher *h); \
    void path##__Debug__fmt(jz_str *out, const T *v);
GROOVE_OP_DERIVES(groove_TableSourceOp, groove__ivm__op_types__TableSourceOp)
GROOVE_OP_DERIVES(groove_LookupOp, groove__ivm__op_types__LookupOp)
GROOVE_OP_DERIVES(groove_IndexSourceOp, groove__ivm__op_types__IndexSourceOp)
GROOVE_OP_DERIVES(groove_InlineRecordsOp, groove__ivm__op_types__InlineRecordsOp)
GROOVE_OP_DERIVES(groove_BindingSourceKey, groove__ivm__op_types__BindingSourceKey)
GROOVE_OP_DERIVES(groove_PersistOp, groove__ivm__op_types__PersistOp)
GROOVE_OP_DERIVES(groove_FilterOp, groove__ivm__op_types__FilterOp)
GROOVE_OP_DERIVES(groove_ProjectionExpr, groove__ivm__op_types__ProjectionExpr)
GROOVE_OP_DERIVES(groove_MapProjectOp, groove__ivm__op_types__MapProjectOp)
GROOVE_OP_DERIVES(groove_StreamingChecksumOp, groove__ivm__op_types__StreamingChecksumOp)
GROOVE_OP_DERIVES(groove_UnwrapNullableOp, groove__ivm__op_types__UnwrapNullableOp)
GROOVE_OP_DERIVES(groove_UnnestOp, groove__ivm__op_types__UnnestOp)
GROOVE_OP_DERIVES(groove_VariantProjectOp, groove__ivm__op_types__VariantProjectOp)
GROOVE_OP_DERIVES(groove_IndexByOp, groove__ivm__op_types__IndexByOp)
GROOVE_OP_DERIVES(groove_JoinOp, groove__ivm__op_types__JoinOp)
GROOVE_OP_DERIVES(groove_RecursiveOp, groove__ivm__op_types__RecursiveOp)
GROOVE_OP_DERIVES(groove_ArgExtremumByOp, groove__ivm__op_types__ArgMaxByOp)
GROOVE_OP_DERIVES(groove_TopByOp, groove__ivm__op_types__TopByOp)
GROOVE_OP_DERIVES(groove_CollectBySlot, groove__ivm__op_types__CollectBySlot)
GROOVE_OP_DERIVES(groove_CollectByOp, groove__ivm__op_types__CollectByOp)
GROOVE_OP_DERIVES(groove_AggregateExpr, groove__ivm__op_types__AggregateExpr)
GROOVE_OP_DERIVES(groove_AggregateOp, groove__ivm__op_types__AggregateOp)

void groove__ivm__op_types__BindingSourceKey__prepared(groove_BindingSourceKey *out, jz_str shape);
const jz_str *groove__ivm__op_types__BindingSourceKey__prepared_name(const groove_BindingSourceKey *k);
int groove__ivm__op_types__BindingSourceKey__Ord__cmp(const groove_BindingSourceKey *a, const groove_BindingSourceKey *b);
u64 groove__ivm__op_types__BindingSourceKey__hash_v(const void *k);
bool groove__ivm__op_types__BindingSourceKey__eq_v(const void *a, const void *b);
void groove__ivm__op_types__BindingSourceKey__drop_v(void *k);
void groove__ivm__op_types__CollectByOp__read_field_indices(const groove_CollectByOp *self, groove_UsizeVec *out);
void groove__ivm__op_types__TopByLimit__Debug__fmt(jz_str *out, const groove_TopByLimit *v);
void groove__ivm__op_types__TopByOrderFieldVec__Debug__fmt(jz_str *out, const groove_TopByOrderFieldVec *v);
void groove__ivm__op_types__RecursiveEnumRemaps__hash_helper(const groove_RecursiveEnumRemaps *v, groove_Hasher *h);
void groove__ivm__op_types__optu8vec_clone(groove_OptU8Vec *dst, const groove_OptU8Vec *src);
void groove__ivm__op_types__optu32vec_clone(groove_OptU32Vec *dst, const groove_OptU32Vec *src);
bool groove__ivm__op_types__optu8vec_eq(const groove_OptU8Vec *a, const groove_OptU8Vec *b);
bool groove__ivm__op_types__optu32vec_eq(const groove_OptU32Vec *a, const groove_OptU32Vec *b);
void groove__ivm__op_types__optu8vec_hash(const groove_OptU8Vec *v, groove_Hasher *h);
void groove__ivm__op_types__optu32vec_hash(const groove_OptU32Vec *v, groove_Hasher *h);
void groove__ivm__op_types__optu8vec_debug(jz_str *out, const groove_OptU8Vec *v);
void groove__ivm__op_types__optu32vec_debug(jz_str *out, const groove_OptU32Vec *v);


/* shared small helpers (op_types.c), used by graph.c and template/ */
void groove__ivm__op_types__dbg_bool(jz_str *out, bool b);
void groove__ivm__op_types__dbg_opt_str(jz_str *out, bool some, const jz_str *s);
void groove__ivm__op_types__dbg_dir(jz_str *out, u8 d);
void groove__ivm__op_types__dbg_cmp(jz_str *out, u8 c);
void groove__ivm__op_types__opt_str_clone(bool some, const jz_str *src, jz_str *dst);
void groove__ivm__op_types__hash_opt_str(groove_Hasher *h, bool some, const jz_str *s);
bool groove__ivm__op_types__opt_str_eq(bool sa, const jz_str *a, bool sb, const jz_str *b);
void groove__ivm__op_types__scan_opt_clone(bool has, const groove_StaticScanSpec *src, groove_StaticScanSpec *dst);
bool groove__ivm__op_types__scan_opt_eq(bool ha, const groove_StaticScanSpec *a, bool hb, const groove_StaticScanSpec *b);
void groove__ivm__op_types__scan_opt_hash(groove_Hasher *h, bool has, const groove_StaticScanSpec *s);
void groove__ivm__op_types__scan_opt_debug(jz_str *out, bool has, const groove_StaticScanSpec *s);
void groove__ivm__op_types__intersection_clone_v(void *dst, const void *src);
void groove__ivm__op_types__intersection_drop_v(void *p);

void groove__ivm__op_types__predvec_clone(groove_PredicateVec *dst, const groove_PredicateVec *src);
void groove__ivm__op_types__limit_hash(groove_Hasher *h, const groove_TopByLimit *l);
bool groove__ivm__op_types__limit_eq(const groove_TopByLimit *a, const groove_TopByLimit *b);
void groove__ivm__op_types__agg_expr_clone_v(void *d, const void *s);
void groove__ivm__op_types__agg_expr_drop_v(void *p);

#endif
