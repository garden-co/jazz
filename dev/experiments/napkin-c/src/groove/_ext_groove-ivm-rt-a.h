/* Mirror declarations for unit groove-ivm-rt-a (ivm/runtime/{subscriptions,
 * runtime_tick,evaluator,record_projection}). Everything here is OWNED BY
 * OTHER UNITS (records, schema, storage, ivm graph/op_types, the rest of
 * ivm/runtime, large_values, chunks). Layouts roughly follow the Rust fields;
 * only what this unit touches is spelled out. */
#ifndef GROOVE_EXT_IVM_RT_A_H
#define GROOVE_EXT_IVM_RT_A_H

#include "jz_rt.h"
typedef long isize;

/* ======================================================================
 * crate::records
 * ==================================================================== */
typedef struct groove_RecordDescriptorData groove_RecordDescriptorData;
/* RecordDescriptor(Intern<RecordDescriptorData>): Copy, compared by pointer */
typedef const groove_RecordDescriptorData *groove_RecordDescriptor;

typedef struct groove_ValueType groove_ValueType;
typedef struct groove_EnumSchema groove_EnumSchema;
typedef struct groove_ScalarEnumSchema groove_ScalarEnumSchema;
enum groove_ValueTypeTag {
    GROOVE_VT_U8, GROOVE_VT_U16, GROOVE_VT_U32, GROOVE_VT_U64, GROOVE_VT_I32, GROOVE_VT_I64,
    GROOVE_VT_F64, GROOVE_VT_BOOL, GROOVE_VT_STRING, GROOVE_VT_BYTES, GROOVE_VT_INTERNAL,
    GROOVE_VT_UUID, GROOVE_VT_ENUM_TAG, GROOVE_VT_TUPLE, GROOVE_VT_ARRAY, GROOVE_VT_NULLABLE,
    GROOVE_VT_RECORD, GROOVE_VT_ENUM, GROOVE_VT_U48,
};
struct groove_ValueType {
    u32 tag;
    union {
        void *internal;
        groove_ScalarEnumSchema *enum_tag;
        JZ_VEC(groove_ValueType) tuple;
        groove_ValueType *boxed;              /* Array, Nullable */
        groove_RecordDescriptor *record;      /* Box<RecordDescriptor> */
        groove_EnumSchema *enum_schema;       /* Box<EnumSchema> */
    } v;
};

typedef struct groove_FieldIdentity { u32 tag; jz_str name; u64 slot; } groove_FieldIdentity;

typedef struct groove_DescriptorField {
    bool has_name; jz_str name;
    bool has_identity; groove_FieldIdentity identity;
    groove_ValueType value_type;
} groove_DescriptorField;

struct groove_RecordDescriptorData {
    JZ_VEC(groove_DescriptorField) fields;
    /* layout: private to records */
};

typedef struct groove_OwnedRecord { jz_bytes raw; groove_RecordDescriptor descriptor; } groove_OwnedRecord;
typedef struct groove_EnumValue { u32 tag; groove_OwnedRecord record; } groove_EnumValue;
typedef struct groove_EnumCase { jz_str name; groove_RecordDescriptor payload; } groove_EnumCase;

typedef struct groove_Value groove_Value;
enum groove_ValueTag {
    GROOVE_V_U8, GROOVE_V_U16, GROOVE_V_U32, GROOVE_V_U64, GROOVE_V_F64, GROOVE_V_BOOL,
    GROOVE_V_STRING, GROOVE_V_BYTES, GROOVE_V_LARGE, GROOVE_V_UUID, GROOVE_V_ENUM_TAG,
    GROOVE_V_TUPLE, GROOVE_V_ARRAY, GROOVE_V_NULLABLE, GROOVE_V_I64, GROOVE_V_I32,
    GROOVE_V_RECORD, GROOVE_V_ENUM, GROOVE_V_U48,
};
typedef struct groove_LargeValueRef groove_LargeValueRef;
struct groove_Value {
    u32 tag;
    union {
        u8 u8; u16 u16; u32 u32; u64 u64; f64 f64; bool b; i64 i64; i32 i32;
        jz_str s; jz_bytes bytes; groove_LargeValueRef *large; jz_uuid uuid; u8 enum_tag;
        JZ_VEC(groove_Value) list;      /* Tuple, Array */
        groove_Value *nullable;         /* NULL = None */
        groove_OwnedRecord record;
        groove_EnumValue enm;
    } v;
};
typedef JZ_VEC(groove_Value) groove_ValueVec;

typedef struct groove_BorrowedRecord { const u8 *raw; u32 len; groove_RecordDescriptor descriptor; } groove_BorrowedRecord;

typedef struct groove_PreparedProjection groove_PreparedProjection;
typedef struct groove_PreparedRecordCopy groove_PreparedRecordCopy;
enum { GROOVE_RPF_COPY, GROOVE_RPF_WRAP_NULLABLE, GROOVE_RPF_NESTED, GROOVE_RPF_ENCODED, GROOVE_RPF_ERROR, GROOVE_RPF_EVALUATE };
typedef struct groove_DescIdx { groove_RecordDescriptor descriptor; usize index; } groove_DescIdx;
typedef struct groove_RawProjectionField {
    u32 tag;
    union {
        usize source_idx;
        JZ_VEC(groove_DescIdx) path;
        jz_bytes bytes;
        jz_err error;
    } v;
} groove_RawProjectionField;
typedef struct groove_NamedType { jz_str name; groove_ValueType value_type; } groove_NamedType;

/* records::Error discriminants used here */
enum { GROOVE_RECORDS_ERR_FIELD_INDEX_OOB = 0x0201, GROOVE_RECORDS_ERR_TYPE_MISMATCH = 0x0202 };

void groove__records__values__Value__drop(groove_Value *v);
void groove__records__values__Value__clone(groove_Value *dst, const groove_Value *src);
bool groove__records__values__Value__eq(const groove_Value *a, const groove_Value *b);
void groove__records__values__Value__debug(jz_str *out, const groove_Value *v);
void groove__records__values__ValueType__clone(groove_ValueType *dst, const groove_ValueType *src);
void groove__records__values__ValueType__drop(groove_ValueType *v);
bool groove__records__values__ValueType__eq(const groove_ValueType *a, const groove_ValueType *b);
void groove__records__values__ValueType__debug(jz_str *out, const groove_ValueType *v);
bool groove__records__values__ValueType__may_contain_stored_scalar(const groove_ValueType *v);
bool groove__records__values__ValueType__contains_record(const groove_ValueType *v);
const groove_ValueType *groove__records__values__ValueType__non_nullable(const groove_ValueType *v);
bool groove__records__values__ValueType__is_array_fk(const groove_ValueType *v);
jz_err groove__records__values__EnumSchema__case(const groove_EnumSchema *s, u32 tag, const groove_EnumCase **out);
jz_err groove__records__values__EnumSchema__tag(const groove_EnumSchema *s, const jz_str *name, u32 *out);
bool groove__records__collect_by_ordered_scalar(const groove_ValueType *v);
jz_err groove__records__encode_single_field_value(const groove_Value *v, const groove_ValueType *t, jz_bytes *out);
void groove__records__FieldIdentity__clone(groove_FieldIdentity *dst, const groove_FieldIdentity *src);
void groove__records__DescriptorField__drop(groove_DescriptorField *f);
const char *groove__records__DescriptorField__logical_name(const groove_DescriptorField *f, u32 *len);

bool groove__records__RecordDescriptor__field_index(groove_RecordDescriptor d, const char *name, u32 len, usize *out);
jz_err groove__records__RecordDescriptor__create(groove_RecordDescriptor d, const groove_Value *values, u32 n, jz_bytes *out);
groove_RecordDescriptor groove__records__RecordDescriptor__new(groove_NamedType *fields, u32 n); /* consumes */
groove_RecordDescriptor groove__records__RecordDescriptor__new_with_fields(groove_DescriptorField *fields, u32 n); /* consumes */
groove_RecordDescriptor groove__records__RecordDescriptor__default(void);
bool groove__records__RecordDescriptor__registry_compatible_with(groove_RecordDescriptor a, groove_RecordDescriptor b);
bool groove__records__RecordDescriptor__can_evolve_registry_to(groove_RecordDescriptor a, groove_RecordDescriptor b);
jz_err groove__records__RecordDescriptor__fields_contain_indirect_values(groove_RecordDescriptor d, const u8 *raw, u32 len, const usize *idx, u32 n, bool *out);
jz_err groove__records__RecordDescriptor__project_record_raw(groove_RecordDescriptor out_desc, const groove_RecordDescriptor *inputs, const jz_slice *records, u32 n, const usize (*mapping)[2], u32 nmap, jz_bytes *out);
jz_err groove__records__RecordDescriptor__get_idx(groove_RecordDescriptor d, const u8 *raw, u32 len, usize idx, groove_Value *out);
u64 groove__records__RecordDescriptor__hash(groove_RecordDescriptor d);
void groove__records__RecordDescriptor__debug(jz_str *out, groove_RecordDescriptor d);
jz_err groove__records__BorrowedRecord__get_idx(const groove_BorrowedRecord *r, usize idx, groove_Value *out);
jz_err groove__records__BorrowedRecord__to_values(const groove_BorrowedRecord *r, groove_ValueVec *out);
jz_err groove__records__OwnedRecord__get(const groove_OwnedRecord *r, const jz_str *name, groove_Value *out);
jz_err groove__records__OwnedRecord__to_values(const groove_OwnedRecord *r, groove_ValueVec *out);
groove_PreparedProjection *groove__records__PreparedProjection__new(groove_RecordDescriptor in, groove_RecordDescriptor out, groove_RawProjectionField *fields, u32 n); /* consumes, rc */
void groove__records__PreparedProjection__release(groove_PreparedProjection *p);
jz_err groove__records__PreparedRecordCopy__new(const groove_RecordDescriptor *inputs, u32 n, groove_RecordDescriptor out, const void *mapping, u32 nmap, groove_PreparedRecordCopy **res); /* rc */

/* ======================================================================
 * crate::schema
 * ==================================================================== */
typedef struct groove_ColumnSchema { jz_str name; void *column_type; } groove_ColumnSchema;
typedef struct groove_PrimaryKeyColumn { jz_str column; u32 key_type; } groove_PrimaryKeyColumn;
typedef struct groove_PrimaryKey { JZ_VEC(groove_PrimaryKeyColumn) columns; bool generated; } groove_PrimaryKey;
typedef struct groove_IndexSchema { jz_str name; JZ_VEC(jz_str) columns; bool unique; } groove_IndexSchema;
typedef struct groove_TableVariant { u32 tag; JZ_VEC(jz_str) fields; jz_vec payload_fields; } groove_TableVariant;
typedef struct groove_TableSchema {
    jz_str name;
    u64 variant_registry_id;
    jz_btree value_variant_registries;
    JZ_VEC(groove_ColumnSchema) columns;
    bool has_primary_key; groove_PrimaryKey primary_key;
    JZ_VEC(groove_IndexSchema) indices;
    jz_vec foreign_keys;
    JZ_VEC(groove_TableVariant) variants;
} groove_TableSchema;
typedef struct groove_DatabaseSchema { JZ_VEC(groove_TableSchema) tables; jz_vec direct_record_stores; } groove_DatabaseSchema;
const groove_TableSchema *groove__schema__DatabaseSchema__table(const groove_DatabaseSchema *s, const char *name, u32 len);
groove_RecordDescriptor groove__schema__TableSchema__record_schema(const groove_TableSchema *t);
bool groove__schema__TableSchema__has_variants(const groove_TableSchema *t);
void groove__schema__IndexSchema__clone(groove_IndexSchema *dst, const groove_IndexSchema *src);
void groove__schema__IndexSchema__drop(groove_IndexSchema *s);

/* ======================================================================
 * crate::ivm::op_types / graph
 * ==================================================================== */
typedef struct groove_NodeId { u64 v; } groove_NodeId;
typedef JZ_VEC(groove_NodeId) groove_NodeIdVec;
typedef struct groove_FrontierName { jz_str name; } groove_FrontierName;
enum { GROOVE_BSK_PREPARED, GROOVE_BSK_RUNTIME_INPUT };
typedef struct groove_BindingSourceKey { u32 tag; jz_str prepared; u64 runtime_namespace; u64 local; } groove_BindingSourceKey;
typedef struct groove_InputSourceId { u64 runtime_namespace; u64 local; } groove_InputSourceId;

typedef struct groove_LiteralValue groove_LiteralValue;
struct groove_LiteralValue {
    u32 tag;
    union { u64 u; i64 i; bool b; jz_str s; jz_bytes bytes; jz_uuid uuid; JZ_VEC(groove_LiteralValue) list; groove_LiteralValue *nullable; struct { jz_bytes descriptor, raw; } native; } v;
};
void groove__ivm__op_types__LiteralValue__to_value(const groove_LiteralValue *l, groove_Value *out);
bool groove__ivm__op_types__LiteralValue__value_type(const groove_LiteralValue *l, groove_ValueType *out);
void groove__ivm__op_types__LiteralValue__from_value(groove_Value v, groove_LiteralValue *out);
void groove__ivm__op_types__LiteralValue__clone(groove_LiteralValue *dst, const groove_LiteralValue *src);
void groove__ivm__op_types__LiteralValue__drop(groove_LiteralValue *l);
int groove__ivm__op_types__LiteralValue__cmp(const groove_LiteralValue *a, const groove_LiteralValue *b);
u64 groove__ivm__op_types__LiteralValue__hash(const groove_LiteralValue *l, u64 h);

enum { GROOVE_SCAN_POINT, GROOVE_SCAN_PREFIX, GROOVE_SCAN_PREFIX_LIMIT, GROOVE_SCAN_REVERSE_PREFIX_LIMIT, GROOVE_SCAN_RANGE };
typedef JZ_VEC(groove_LiteralValue) groove_LiteralVec;
typedef JZ_VEC(groove_ValueType) groove_ValueTypeVec;
typedef struct groove_StaticScanSpec { u32 tag; groove_LiteralVec prefix; usize max_items; groove_LiteralVec end; } groove_StaticScanSpec; /* Range: prefix=start */

enum { GROOVE_VPT_NAMED, GROOVE_VPT_SCHEMA_INDEX };
typedef struct groove_VariantProjectionTarget { u32 tag; jz_str name; } groove_VariantProjectionTarget;

enum { GROOVE_FR_NAME, GROOVE_FR_STORED_NAME, GROOVE_FR_RESOLVED };
typedef struct groove_FieldRef { u32 tag; jz_str name; usize index; } groove_FieldRef;
jz_str groove__ivm__graph__FieldRef__display_name(const groove_FieldRef *f);
void groove__ivm__graph__FieldRef__clone(groove_FieldRef *dst, const groove_FieldRef *src);
bool groove__ivm__graph__FieldRef__eq(const groove_FieldRef *a, const groove_FieldRef *b);

typedef struct groove_RecursiveEnumRemaps { jz_btree scalar, payload, payload_children; } groove_RecursiveEnumRemaps;

enum { GROOVE_PE_FIELD, GROOVE_PE_LITERAL, GROOVE_PE_NULL, GROOVE_PE_NULLABLE, GROOVE_PE_NULLABLE_FLAT, GROOVE_PE_ENUM_TAG_REMAP, GROOVE_PE_ENUM_REMAP, GROOVE_PE_RECURSIVE_ENUM_REMAP };
typedef struct groove_PlanExpr {
    u32 tag;
    jz_str field;
    groove_LiteralValue literal;
    groove_ValueType null_type;
    JZ_VEC(i32) tags;                /* Option<u8>/Option<u32> encoded, -1 = None */
    groove_RecursiveEnumRemaps remaps;
    bool omit_unrepresentable;
} groove_PlanExpr;
typedef JZ_VEC(groove_PlanExpr) groove_PlanExprVec;
void groove__ivm__op_types__PlanExpr__clone(groove_PlanExpr *dst, const groove_PlanExpr *src);
void groove__ivm__op_types__PlanExpr__drop(groove_PlanExpr *p);
void groove__ivm__op_types__PlanExpr__debug(jz_str *out, const groove_PlanExpr *p);

/* ProjectExpr (ivm::graph) */
enum { GROOVE_PX_TEMPLATE_ARGUMENT, GROOVE_PX_FIELD, GROOVE_PX_RECORD_FIELD, GROOVE_PX_LITERAL, GROOVE_PX_TYPED_LITERAL, GROOVE_PX_NULL, GROOVE_PX_NULLABLE, GROOVE_PX_NULLABLE_FLAT, GROOVE_PX_ENUM_TAG_REMAP, GROOVE_PX_ENUM_REMAP, GROOVE_PX_RECURSIVE_ENUM_REMAP };
typedef struct groove_ProjectExpr {
    u32 tag;
    u32 slot;
    groove_FieldRef source;
    JZ_VEC(jz_str) path;
    groove_LiteralValue value;
    groove_ValueType value_type;   /* TemplateArgument/TypedLiteral/Null/RecursiveEnumRemap target */
    JZ_VEC(i32) tags;
    groove_RecursiveEnumRemaps remaps;
    bool omit_unrepresentable;
} groove_ProjectExpr;
void groove__ivm__graph__ProjectExpr__clone(groove_ProjectExpr *dst, const groove_ProjectExpr *src);
void groove__ivm__graph__ProjectExpr__drop(groove_ProjectExpr *p);
bool groove__ivm__graph__ProjectExpr__eq(const groove_ProjectExpr *a, const groove_ProjectExpr *b);
u64 groove__ivm__graph__ProjectExpr__hash(const groove_ProjectExpr *p, u64 h);

typedef struct groove_ProjectField { groove_ProjectExpr expression; jz_str output_name; groove_FieldIdentity output_identity; } groove_ProjectField;
void groove__ivm__graph__ProjectField__named(groove_ProjectField *out, jz_str name);
void groove__ivm__graph__ProjectField__drop(groove_ProjectField *p);
typedef struct groove_ProjectionExpr { groove_ProjectExpr expression; bool has_output_name; jz_str output_name; groove_FieldIdentity output_identity; } groove_ProjectionExpr;

/* PredicateExpr (ivm::op_types); its matcher impl lives in subscriptions.rs (this unit) */
enum {
    GROOVE_PRED_TEMPLATE_ARGUMENT, GROOVE_PRED_EQ, GROOVE_PRED_NEQ, GROOVE_PRED_CONTAINS, GROOVE_PRED_EQ_FIELD,
    GROOVE_PRED_CONTAINS_FIELD, GROOVE_PRED_NEQ_FIELD, GROOVE_PRED_GT, GROOVE_PRED_GT_EQ, GROOVE_PRED_LT,
    GROOVE_PRED_LT_EQ, GROOVE_PRED_IS_NULL, GROOVE_PRED_IS_NOT_NULL, GROOVE_PRED_ENUM_MATCH, GROOVE_PRED_AND, GROOVE_PRED_OR,
};
typedef struct groove_PredicateExpr groove_PredicateExpr;
struct groove_PredicateExpr {
    u32 tag;
    u32 slot;                        /* TemplateArgument */
    u32 case_tag;                    /* EnumMatch */
    JZ_VEC(jz_str) fields;           /* TemplateArgument */
    jz_str field;
    jz_str other_field;              /* value_field / needle_field */
    groove_LiteralValue value;
    groove_PredicateExpr *payload;   /* EnumMatch */
    JZ_VEC(groove_PredicateExpr) list; /* And / Or */
};
void groove__ivm__op_types__PredicateExpr__clone(groove_PredicateExpr *dst, const groove_PredicateExpr *src);
void groove__ivm__op_types__PredicateExpr__drop(groove_PredicateExpr *p);
bool groove__ivm__op_types__PredicateExpr__eq(const groove_PredicateExpr *a, const groove_PredicateExpr *b);
u64 groove__ivm__op_types__PredicateExpr__hash(const groove_PredicateExpr *p, u64 h);

enum { GROOVE_VC_EXACT, GROOVE_VC_POLICY };
enum { GROOVE_DIR_ASC, GROOVE_DIR_DESC };
enum { GROOVE_LIMIT_FINITE, GROOVE_LIMIT_UNBOUNDED };
typedef struct groove_TopByLimit { u32 tag; u64 n; } groove_TopByLimit;
static inline bool groove_TopByLimit_eq(groove_TopByLimit a, groove_TopByLimit b) { return a.tag == b.tag && (a.tag != GROOVE_LIMIT_FINITE || a.n == b.n); }
enum { GROOVE_CBM_COLLECT, GROOVE_CBM_ROOT, GROOVE_CBM_EXPAND };
typedef JZ_VEC(usize) groove_UsizeVec;
typedef JZ_VEC(jz_str) groove_StrVec;
typedef JZ_VEC(u8) groove_DirVec;

typedef struct groove_TableSourceOp { jz_str table; bool has_scan; groove_StaticScanSpec scan; bool has_variant_projection; groove_VariantProjectionTarget variant_projection; } groove_TableSourceOp;
void groove__ivm__op_types__TableSourceOp__clone(groove_TableSourceOp *dst, const groove_TableSourceOp *src);
void groove__ivm__op_types__TableSourceOp__drop(groove_TableSourceOp *op);
typedef struct groove_LookupOp { groove_TableSourceOp source; groove_RecordDescriptor key_descriptor; groove_UsizeVec key_fields; } groove_LookupOp;
typedef struct groove_IndexSourceOp {
    jz_str table, index; void *durable_ids; jz_vec intersections; void *candidate_filter;
    groove_RecordDescriptor input_descriptor;
    bool has_variant_projection; groove_VariantProjectionTarget variant_projection;
    bool has_row_projection; groove_VariantProjectionTarget row_projection;
    groove_UsizeVec key_fields, value_fields; bool unique, append_value_to_key, store_value;
    bool has_scan; groove_StaticScanSpec scan;
} groove_IndexSourceOp;
typedef struct groove_InlineRecordsOp { JZ_VEC(jz_bytes) records; } groove_InlineRecordsOp;
typedef struct groove_FrontierSourceOp { groove_FrontierName binding; } groove_FrontierSourceOp;
typedef struct groove_BindingSourceOp { groove_BindingSourceKey key; } groove_BindingSourceOp;
typedef struct groove_ArrangeOp { groove_StrVec fields; u32 comparison; } groove_ArrangeOp;
typedef struct groove_ArgByOp { groove_StrVec group_fields, order_fields; groove_UsizeVec group_field_indices, comparison_field_indices; } groove_ArgByOp;
typedef struct groove_TopByOrderField { jz_str field; u32 direction; } groove_TopByOrderField;
typedef struct groove_TopByOp {
    groove_StrVec group_fields; groove_UsizeVec group_field_indices;
    JZ_VEC(groove_TopByOrderField) order_fields; groove_StrVec tie_fields;
    groove_UsizeVec sort_field_indices; groove_DirVec sort_directions;
    u64 offset; groove_TopByLimit limit;
} groove_TopByOp;
void groove__ivm__op_types__TopByOp__clone(groove_TopByOp *dst, const groove_TopByOp *src);
void groove__ivm__op_types__TopByOp__drop(groove_TopByOp *op);
typedef struct groove_CollectByProjection { jz_str field; usize field_idx; jz_str output_name; bool unwrap_nullable; } groove_CollectByProjection;
typedef JZ_VEC(groove_CollectByProjection) groove_CollectByProjectionVec;
typedef struct groove_CollectBySlot groove_CollectBySlot;
typedef JZ_VEC(groove_CollectBySlot) groove_CollectBySlotVec;
struct groove_CollectBySlot {
    groove_StrVec group_fields; groove_UsizeVec group_field_indices;
    groove_CollectByProjectionVec child_fields; groove_RecordDescriptor child_descriptor;
    jz_str collection_field; usize collection_field_index;
    groove_CollectBySlotVec slots;
    JZ_VEC(groove_TopByOrderField) order_fields; groove_StrVec tie_fields;
    bool has_presence_field_index; usize presence_field_index;
    bool has_reference_array_field_index; usize reference_array_field_index;
    bool reference_order;
    groove_UsizeVec sort_field_indices; groove_DirVec sort_directions;
    u64 offset; groove_TopByLimit limit;
};
typedef struct groove_CollectByOp {
    u32 mode;
    groove_StrVec group_fields; groove_UsizeVec group_field_indices;
    groove_CollectByProjectionVec parent_fields, child_fields;
    groove_RecordDescriptor child_descriptor;
    jz_str collection_field; usize collection_field_index;
    groove_CollectBySlotVec slots;
    groove_CollectByProjectionVec tuple_fields;
    groove_StrVec occurrence_id_fields; groove_UsizeVec occurrence_id_field_indices;
    JZ_VEC(groove_TopByOrderField) order_fields; groove_StrVec tie_fields;
    groove_UsizeVec sort_field_indices; groove_DirVec sort_directions;
    u64 offset; groove_TopByLimit limit;
} groove_CollectByOp;
void groove__ivm__op_types__CollectByOp__read_field_indices(const groove_CollectByOp *op, groove_UsizeVec *out);
typedef struct groove_RecursiveOp { groove_FrontierName frontier; usize max_iters; bool truncate_at_max_iters; groove_StrVec read_tables; bool has_step_witness_output; groove_RecordDescriptor step_witness_output; } groove_RecursiveOp;
typedef struct groove_DurableStorage { jz_str column_family; jz_bytes key_prefix; jz_str name; } groove_DurableStorage;
typedef struct groove_PersistOp { jz_str name; groove_DurableStorage storage; groove_UsizeVec key_fields; bool unique; } groove_PersistOp;
typedef struct groove_FilterOp { groove_PredicateExpr predicate; u32 comparison; } groove_FilterOp;
typedef struct groove_UsizePair { usize v[2]; } groove_UsizePair;
typedef JZ_VEC(groove_UsizePair) groove_UsizePairVec;
typedef struct groove_MapProjectOp { JZ_VEC(groove_ProjectionExpr) expressions; groove_UsizePairVec mapping; } groove_MapProjectOp;
bool groove__ivm__op_types__MapProjectOp__eq(const groove_MapProjectOp *a, const groove_MapProjectOp *b);
typedef struct groove_StreamingChecksumOp { jz_str field; usize field_idx; jz_str output_field; usize window_bytes, max_bytes_per_turn; } groove_StreamingChecksumOp;
typedef struct groove_UnwrapNullableOp { jz_str field; usize field_idx; } groove_UnwrapNullableOp;
typedef struct groove_UnnestOp { jz_str array_field; usize array_field_idx; jz_str element_field; } groove_UnnestOp;
typedef struct groove_VariantProjectOp { jz_str field; usize field_idx; u32 tag; } groove_VariantProjectOp;
typedef struct groove_IndexByOp {
    groove_PlanExprVec key_expressions, value_expressions;
    bool has_explicit_index; groove_IndexSchema explicit_index;
    groove_UsizeVec key_fields, value_fields; bool unique, append_value_to_key, store_value;
    bool has_scan; groove_StaticScanSpec scan;
} groove_IndexByOp;
typedef struct groove_JoinOp { u32 kind; groove_PlanExprVec left_key, right_key; groove_RecordDescriptor left_descriptor, right_descriptor; bool has_residual; groove_PlanExpr residual_predicate; u32 comparison; } groove_JoinOp;
enum { GROOVE_AGG_COUNT, GROOVE_AGG_SUM, GROOVE_AGG_AVG, GROOVE_AGG_MIN, GROOVE_AGG_MAX };
typedef struct groove_AggregateExpr { u32 function; bool has_expression; groove_PlanExpr expression; bool distinct; bool has_output_name; jz_str output_name; bool has_output_identity; groove_FieldIdentity output_identity; } groove_AggregateExpr;
typedef struct groove_AggregateOp { groove_PlanExprVec group_key; groove_UsizeVec group_field_indices; JZ_VEC(groove_AggregateExpr) aggregates; } groove_AggregateOp;

enum groove_OpTag {
    GROOVE_OP_TABLE_SOURCE, GROOVE_OP_INDEX_SOURCE, GROOVE_OP_INLINE_RECORDS, GROOVE_OP_FRONTIER_SOURCE,
    GROOVE_OP_BINDING_SOURCE, GROOVE_OP_ARRANGE, GROOVE_OP_ARG_MAX_BY, GROOVE_OP_ARG_MIN_BY, GROOVE_OP_TOP_BY,
    GROOVE_OP_COLLECT_BY, GROOVE_OP_RECURSIVE, GROOVE_OP_RECURSIVE_STEP_WITNESS, GROOVE_OP_PERSIST,
    GROOVE_OP_FILTER, GROOVE_OP_MAP_PROJECT, GROOVE_OP_STREAMING_CHECKSUM, GROOVE_OP_UNWRAP_NULLABLE,
    GROOVE_OP_UNNEST, GROOVE_OP_VARIANT_PROJECT, GROOVE_OP_INDEX_BY, GROOVE_OP_JOIN, GROOVE_OP_SEMI_JOIN,
    GROOVE_OP_ANTI_JOIN, GROOVE_OP_LOOKUP, GROOVE_OP_UNION, GROOVE_OP_NEGATE, GROOVE_OP_DISTINCT, GROOVE_OP_AGGREGATE,
};
typedef struct groove_OpType {
    u32 tag;
    union {
        groove_TableSourceOp table_source; groove_IndexSourceOp index_source; groove_InlineRecordsOp inline_records;
        groove_FrontierSourceOp frontier_source; groove_BindingSourceOp binding_source; groove_ArrangeOp arrange;
        groove_ArgByOp arg_by; groove_TopByOp top_by; groove_CollectByOp *collect_by; groove_RecursiveOp recursive;
        groove_PersistOp persist; groove_FilterOp filter; groove_MapProjectOp map_project;
        groove_StreamingChecksumOp streaming_checksum; groove_UnwrapNullableOp unwrap_nullable; groove_UnnestOp unnest;
        groove_VariantProjectOp variant_project; groove_IndexByOp index_by; groove_JoinOp join; groove_LookupOp lookup;
        groove_AggregateOp aggregate;
    } v;
} groove_OpType;
void groove__ivm__graph__OpType__clone(groove_OpType *dst, const groove_OpType *src);
void groove__ivm__graph__OpType__drop(groove_OpType *op);

enum { GROOVE_NO_RECORDS, GROOVE_NO_ARRANGEMENT };
typedef struct groove_NodeOutput { u32 tag; groove_RecordDescriptor records; } groove_NodeOutput;
static inline groove_RecordDescriptor groove_NodeOutput_records(groove_NodeOutput o) { return o.records; }
typedef struct groove_NodeDescriptor { groove_OpType op; groove_NodeIdVec inputs; groove_NodeOutput output; } groove_NodeDescriptor;
typedef struct groove_NodeDurability { u32 tag; groove_DurableStorage storage; } groove_NodeDurability;
typedef struct groove_GraphNode { groove_NodeId id; groove_NodeDescriptor descriptor; groove_NodeDurability durability; jz_hmap children; } groove_GraphNode;
typedef struct groove_IvmGraph groove_IvmGraph;
const groove_GraphNode *groove__ivm__graph__IvmGraph__node(const groove_IvmGraph *g, groove_NodeId id);
void groove__ivm__graph__IvmGraph__mark_ancestors(const groove_IvmGraph *g, groove_NodeId id, jz_hmap *out);
usize groove__ivm__graph__IvmGraph__len(const groove_IvmGraph *g);

enum { GROOVE_RET_SUBSCRIPTION, GROOVE_RET_HYDRATION, GROOVE_RET_PREPARED_SHAPE, GROOVE_RET_DURABLE_SCHEMA_OBJECT };
typedef struct groove_Retainer { u32 tag; jz_str key; } groove_Retainer;

/* GraphBuilder (ivm::graph) — Arc<GraphBuilder> children are rc pointers */
typedef struct groove_GraphBuilder groove_GraphBuilder;
typedef struct groove_TopByOrder { groove_FieldRef field; u32 direction; } groove_TopByOrder;
typedef struct groove_CollectByField { groove_FieldRef field; jz_str output_name; bool unwrap_nullable; } groove_CollectByField;
typedef struct groove_CollectBySlotBuilder groove_CollectBySlotBuilder;
struct groove_CollectBySlotBuilder {
    JZ_VEC(groove_FieldRef) group_cols, owner_key_cols;
    JZ_VEC(groove_CollectByField) child_fields;
    jz_str collection_field;
    JZ_VEC(groove_CollectBySlotBuilder) slots;
    JZ_VEC(groove_TopByOrder) order_cols;
    JZ_VEC(groove_FieldRef) tie_cols;
    bool has_presence_col; groove_FieldRef presence_col;
    bool has_reference_array_col; groove_FieldRef reference_array_col;
    bool reference_order; u64 offset; groove_TopByLimit limit;
};
typedef struct groove_CollectByBuilder {
    u32 mode;
    JZ_VEC(groove_FieldRef) group_cols;
    JZ_VEC(groove_CollectByField) parent_fields, child_fields;
    jz_str collection_field;
    JZ_VEC(groove_CollectBySlotBuilder) slots;
    JZ_VEC(groove_CollectByField) tuple_fields;
    JZ_VEC(groove_FieldRef) occurrence_id_cols;
    JZ_VEC(groove_TopByOrder) order_cols;
    JZ_VEC(groove_FieldRef) tie_cols;
    u64 offset; groove_TopByLimit limit;
} groove_CollectByBuilder;
typedef struct groove_TypedGraphTemplate groove_TypedGraphTemplate;
bool groove__ivm__template__TypedGraphTemplate__terminal(const groove_TypedGraphTemplate *t);
enum groove_GbTag {
    GROOVE_GB_TYPED_TEMPLATE, GROOVE_GB_TEMPLATE_INPUT, GROOVE_GB_TABLE, GROOVE_GB_INLINE_RECORDS,
    GROOVE_GB_INPUT_SOURCE, GROOVE_GB_INDEX, GROOVE_GB_FRONTIER_SOURCE, GROOVE_GB_BINDING_SOURCE,
    GROOVE_GB_RECURSIVE, GROOVE_GB_RECURSIVE_STEP_WITNESS, GROOVE_GB_FILTER, GROOVE_GB_UNWRAP_NULLABLE,
    GROOVE_GB_UNNEST, GROOVE_GB_VARIANT_PROJECT, GROOVE_GB_PROJECT, GROOVE_GB_STREAMING_CHECKSUM,
    GROOVE_GB_UNION, GROOVE_GB_JOIN, GROOVE_GB_SEMI_JOIN, GROOVE_GB_ANTI_JOIN, GROOVE_GB_LOOKUP,
    GROOVE_GB_ARG_MAX_BY, GROOVE_GB_ARG_MIN_BY, GROOVE_GB_TOP_BY, GROOVE_GB_COLLECT_BY, GROOVE_GB_AGGREGATE,
};
struct groove_GraphBuilder {
    u32 tag;
    /* TypedTemplate */
    groove_TypedGraphTemplate *program;
    JZ_VEC(groove_GraphBuilder *) inputs;  /* TypedTemplate, Union */
    JZ_VEC(groove_PredicateExpr) predicates;
    void *scalars; u32 scalars_len;
    /* single-input operators / TemplateInput (nullable) / Lookup keys / RecursiveStepWitness */
    groove_GraphBuilder *input;
    /* Join family */
    groove_GraphBuilder *left, *right;
    JZ_VEC(groove_FieldRef) left_on, right_on;
    /* Recursive */
    groove_GraphBuilder *seed, *step, *step_witness;
    groove_FrontierName frontier; usize max_iters; bool truncate_at_max_iters;
    /* sources */
    u32 slot;
    groove_RecordDescriptor output;
    jz_str table, index, shape;
    bool has_scan; groove_StaticScanSpec scan;
    jz_vec intersections; void *candidate_filter;
    bool has_variant_projection; jz_str variant_projection; /* also row_projection */
    JZ_VEC(jz_bytes) records;
    groove_InputSourceId id;
    groove_FrontierName binding;
    /* unary payloads */
    groove_PredicateExpr predicate; u32 comparison;
    groove_FieldRef field; jz_str element_field; jz_str case_name; jz_str output_field;
    usize window_bytes, max_bytes_per_turn;
    JZ_VEC(groove_ProjectField) fields;
    JZ_VEC(groove_FieldRef) keys_on, group_cols, order_cols, tie_cols_ref;
    JZ_VEC(groove_TopByOrder) top_order_cols;
    u64 offset; groove_TopByLimit limit;
    groove_CollectByBuilder *collect;
    JZ_VEC(groove_AggregateExpr) aggregates;
};
typedef JZ_VEC(const groove_GraphBuilder *) groove_GraphBuilderRefVec;
void groove__ivm__graph__GraphBuilder__postorder(const groove_GraphBuilder *g, groove_GraphBuilderRefVec *out);

/* ======================================================================
 * crate::ivm::runtime (unit groove-ivm-rt-b)
 * ==================================================================== */
/* mod.rs: IvmRuntimeError discriminants (Display texts are spelled at each raise site) */
enum groove_IvmRuntimeErrorCode {
    GROOVE_IVM_ERR_PERSISTENCE_OUTCOME_INDETERMINATE = 0x1101, GROOVE_IVM_ERR_AGGREGATE_OVERFLOW,
    GROOVE_IVM_ERR_GRAPH_FIELD_NOT_FOUND, GROOVE_IVM_ERR_AMBIGUOUS_GRAPH_FIELD_REFERENCE,
    GROOVE_IVM_ERR_GRAPH_FIELD_INDEX_OUT_OF_BOUNDS, GROOVE_IVM_ERR_GRAPH_INPUT_ARITY_MISMATCH,
    GROOVE_IVM_ERR_GRAPH_INPUT_MISSING, GROOVE_IVM_ERR_GRAPH_NODE_NOT_FOUND, GROOVE_IVM_ERR_GRAPH_CYCLE,
    GROOVE_IVM_ERR_GRAPH_OUTPUT_MISMATCH, GROOVE_IVM_ERR_ENUM_TAG_PROJECTION_ABSENT,
    GROOVE_IVM_ERR_ENUM_TAG_PROJECTION_NON_ENUM, GROOVE_IVM_ERR_ENUM_PROJECTION_ABSENT,
    GROOVE_IVM_ERR_ENUM_PROJECTION_NON_ENUM, GROOVE_IVM_ERR_INDEX_NOT_FOUND, GROOVE_IVM_ERR_INVALID_PERSISTED_INDEX,
    GROOVE_IVM_ERR_UNSUPPORTED_INDEX_INTERSECTION_SCAN, GROOVE_IVM_ERR_UNSUPPORTED_INDEX_CANDIDATE_FILTER,
    GROOVE_IVM_ERR_UNSUPPORTED_LOOKUP, GROOVE_IVM_ERR_JOIN_KEY_ARITY_MISMATCH, GROOVE_IVM_ERR_SHAPE_KEY_FIELD_NOT_FOUND,
    GROOVE_IVM_ERR_MISSING_PRIMARY_KEY, GROOVE_IVM_ERR_NODE_STATE_MISSING, GROOVE_IVM_ERR_NODE_STATE_OPERATOR_MISMATCH,
    GROOVE_IVM_ERR_OUT_OF_ORDER_RUNTIME_STATE, GROOVE_IVM_ERR_PERSIST_RECORD_MISMATCH,
    GROOVE_IVM_ERR_BINDING_SOURCE_REQUIRES_PREPARE, GROOVE_IVM_ERR_EMPTY_MULTISINK_SUBSCRIPTION,
    GROOVE_IVM_ERR_DUPLICATE_MULTISINK_SINK, GROOVE_IVM_ERR_MULTISINK_SINK_REQUIRES_PREPARE,
    GROOVE_IVM_ERR_ROUTED_MULTISINK_ROUTE_ARITY_MISMATCH, GROOVE_IVM_ERR_BINDING_SOURCE_NOT_FOUND,
    GROOVE_IVM_ERR_BINDING_SOURCE_DESCRIPTOR_MISMATCH, GROOVE_IVM_ERR_FOREIGN_INPUT_SOURCE,
    GROOVE_IVM_ERR_INPUT_SOURCE_RETIRED, GROOVE_IVM_ERR_DUPLICATE_TABLE_VARIANT, GROOVE_IVM_ERR_RECORD_ENCODING,
    GROOVE_IVM_ERR_RECURSIVE_ITERATION_LIMIT, GROOVE_IVM_ERR_STORAGE, GROOVE_IVM_ERR_CHUNK, GROOVE_IVM_ERR_LARGE_VALUE,
    GROOVE_IVM_ERR_STORAGE_UNAVAILABLE, GROOVE_IVM_ERR_EVALUATION_BLOCKED, GROOVE_IVM_ERR_INVALID_STREAMING_CHECKSUM_BUDGET,
    GROOVE_IVM_ERR_STREAMING_CHECKSUM_TYPE_MISMATCH, GROOVE_IVM_ERR_PREPARED_SHAPE_NOT_FOUND,
    GROOVE_IVM_ERR_PREPARED_SHAPE_HAS_ACTIVE_BINDINGS, GROOVE_IVM_ERR_STALE_RUNTIME_STATE, GROOVE_IVM_ERR_TABLE_NOT_FOUND,
    GROOVE_IVM_ERR_TABLE_ALREADY_EXISTS, GROOVE_IVM_ERR_TABLE_FIELD_ALREADY_EXISTS, GROOVE_IVM_ERR_UNKNOWN_TABLE_VARIANT,
    GROOVE_IVM_ERR_VARIANT_PROJECTION_NOT_FOUND, GROOVE_IVM_ERR_VARIANT_PROJECTION_REQUIRED,
    GROOVE_IVM_ERR_RECURSIVE_ENUM_PROJECTION_DESCRIPTOR_MISMATCH = 0x1150,
    GROOVE_IVM_ERR_VARIANT_PROJECT_FIELD_TYPE_MISMATCH, GROOVE_IVM_ERR_VARIANT_PROJECT_PAYLOAD_MISMATCH,
    GROOVE_IVM_ERR_UNIQUE_INDEX_VIOLATION, GROOVE_IVM_ERR_UNSUPPORTED_JOIN_KEY, GROOVE_IVM_ERR_UNSUPPORTED_NON_MONOTONE_RECURSION,
    GROOVE_IVM_ERR_UNSUPPORTED_NESTED_RECURSION, GROOVE_IVM_ERR_UNSUPPORTED_ARG_MAX_BY, GROOVE_IVM_ERR_COLLECT_BY_MUST_BE_TERMINAL,
    GROOVE_IVM_ERR_INVALID_COLLECT_BY, GROOVE_IVM_ERR_INVALID_TOP_BY, GROOVE_IVM_ERR_DUPLICATE_COLLECT_BY_OCCURRENCE_ID,
    GROOVE_IVM_ERR_UNSUPPORTED_OPERATOR,
};
jz_err groove__ivm__runtime__IvmRuntimeError__node_err(u32 code, groove_NodeId node); /* "...: NodeId(n)" */

/* state.rs */
typedef struct groove_ScopePath groove_ScopePath;
typedef const groove_ScopePath *groove_ScopeId;   /* Intern<ScopePath> */
groove_ScopeId groove__ivm__runtime__state__ScopeId__root(void);
groove_ScopeId groove__ivm__runtime__state__ScopeId__child(groove_ScopeId s, groove_NodeId node);
bool groove__ivm__runtime__state__ScopeId__outermost(groove_ScopeId s, groove_NodeId *out);
typedef struct groove_OperatorStateKey { groove_ScopeId scope; groove_NodeId node; } groove_OperatorStateKey;
typedef struct groove_ArrangementKey { groove_ScopeId scope; groove_NodeId input; } groove_ArrangementKey;
typedef struct groove_SubTick { u64 tick; u64 sub_tick; } groove_SubTick;
static inline int groove_SubTick_cmp(groove_SubTick a, groove_SubTick b) { return a.tick != b.tick ? (a.tick < b.tick ? -1 : 1) : (a.sub_tick < b.sub_tick ? -1 : a.sub_tick > b.sub_tick); }
/* AsOf<T, S>: as_of stamp; Tick is stored as SubTick{tick, 0} with is_tick */
typedef struct groove_AsOfStamp { bool has; groove_SubTick at; } groove_AsOfStamp;
jz_err groove__ivm__runtime__state__AsOf__mark_forward_as_of(groove_AsOfStamp *st, groove_SubTick next, bool is_tick);
jz_err groove__ivm__runtime__state__AsOf__value_at(const groove_AsOfStamp *st, groove_SubTick expected, bool is_tick);
void groove__ivm__runtime__state__AsOf__replace_as_of_at_least(groove_AsOfStamp *st, groove_SubTick next);
enum { GROOVE_AUM_ACCUMULATE, GROOVE_AUM_REPLACE };
enum { GROOVE_EVAL_TICK, GROOVE_EVAL_HYDRATE };
enum { GROOVE_HYDRATION_ORDINARY, GROOVE_HYDRATION_SUBSCRIPTION };
typedef struct groove_EvalMemoKey {
    groove_ScopeId scope; groove_NodeId node; u64 input_signature_hash;
    bool has_tick_epoch; u64 tick_epoch; u64 sub_tick; u64 context_digest;
} groove_EvalMemoKey;
struct groove_RecordDeltas;
typedef struct groove_EvalMemoEntry { struct groove_RecordDeltas *records; u64 input_watermark; usize payload_bytes; u64 last_used; } groove_EvalMemoEntry;
typedef struct groove_StrList { u32 len; jz_str *items; } groove_StrList;          /* Arc<[String]> payload */
typedef struct groove_NodeIdList { u32 len; groove_NodeId *items; } groove_NodeIdList; /* Arc<[NodeId]> payload */
typedef struct groove_NodeInputSignature {
    JZ_VEC(jz_str) tables; JZ_VEC(groove_BindingSourceKey) bindings; JZ_VEC(groove_FrontierName) frontier_bindings; u64 hash;
} groove_NodeInputSignature;
groove_NodeInputSignature *groove__ivm__runtime__state__NodeInputSignature__from_sets(jz_btree *tables, jz_btree *bindings, jz_btree *frontier_bindings); /* rc, consumes */
groove_NodeInputSignature *groove__ivm__runtime__state__NodeInputSignature__default(void); /* rc */
typedef struct groove_RuntimeStats {
    usize graph_nodes, active_subscriptions, active_prepared_shapes, active_shape_params, arrangement_count,
          eval_memo_entries, eval_memo_bytes, hydration_memo_entries;
    u64 hydration_memo_hits, hydration_memo_computes;
    usize hydration_memo_distinct_computed_nodes, arrangement_rows, arrangement_encoded_bytes, recursive_state_count,
          recursive_accumulated_rows, recursive_accumulated_encoded_bytes;
    u64 logical_nodes_requested; usize deduped_graph_nodes;
} groove_RuntimeStats;
typedef struct groove_TickMetrics {
    u64 tick; usize table_delta_records, records_processed, nodes_evaluated, subscriptions_considered,
    root_ordering_position_records, root_ordering_position_records_skipped, top_by_delta_membership_records,
    recursive_recomputes;
    u64 hydration_memo_hits, hydration_memo_computes;
    jz_hmap hydration_memo_computed_nodes; /* HashSet<NodeId> */
    usize notifications_sent, notification_records, notification_encoded_bytes;
    groove_RuntimeStats runtime_stats;
} groove_TickMetrics;
void groove__ivm__runtime__state__TickMetrics__default(groove_TickMetrics *m);
void groove__ivm__runtime__state__TickMetrics__drop(groove_TickMetrics *m);
typedef struct groove_EvalContext {
    groove_ScopeId scope; u64 sub_tick;
    jz_hmap bindings;        /* FrontierName -> RecordDeltas */
    jz_hmap binding_digests; /* FrontierName -> u64 */
    u32 arrangement_update_mode; u32 eval_mode; bool hydrate_arrangements;
} groove_EvalContext;
void groove__ivm__runtime__state__EvalContext__default(groove_EvalContext *c);
void groove__ivm__runtime__state__EvalContext__root_snapshot(groove_EvalContext *c);
void groove__ivm__runtime__state__EvalContext__root_subscription_snapshot(groove_EvalContext *c);
void groove__ivm__runtime__state__EvalContext__with_binding(groove_EvalContext *c, groove_ScopeId scope, u64 sub_tick, groove_FrontierName binding, struct groove_RecordDeltas deltas);
void groove__ivm__runtime__state__EvalContext__with_binding_and_arrangement_mode(groove_EvalContext *c, groove_ScopeId scope, u64 sub_tick, groove_FrontierName binding, struct groove_RecordDeltas deltas, u32 mode);
void groove__ivm__runtime__state__EvalContext__drop(groove_EvalContext *c);
u64 groove_hash_ScopeId(groove_ScopeId s);
u64 groove_hash_FrontierName(const groove_FrontierName *f, u64 h);
u64 groove_hash_BindingSourceKey(const groove_BindingSourceKey *k, u64 h);

/* evaluation_memo.rs */
typedef struct groove_EvaluationMemo groove_EvaluationMemo;
groove_EvaluationMemo *groove__ivm__runtime__evaluation_memo__EvaluationMemo__default(void);
void groove__ivm__runtime__evaluation_memo__EvaluationMemo__drop(groove_EvaluationMemo *m);
bool groove__ivm__runtime__evaluation_memo__EvaluationMemo__slot(const groove_EvaluationMemo *m, usize slot, const groove_EvalMemoKey **key, groove_EvalMemoEntry **entry);
groove_EvalMemoEntry *groove__ivm__runtime__evaluation_memo__EvaluationMemo__get(const groove_EvaluationMemo *m, const groove_EvalMemoKey *key);
bool groove__ivm__runtime__evaluation_memo__EvaluationMemo__insert(groove_EvaluationMemo *m, const groove_EvalMemoKey *key, groove_EvalMemoEntry entry, groove_EvalMemoEntry *previous);
bool groove__ivm__runtime__evaluation_memo__EvaluationMemo__remove(groove_EvaluationMemo *m, const groove_EvalMemoKey *key, groove_EvalMemoEntry *out);
void groove__ivm__runtime__evaluation_memo__EvaluationMemo__retain(groove_EvaluationMemo *m, bool (*keep)(const groove_EvalMemoKey *, groove_EvalMemoEntry *, void *), void *ctx);
usize groove__ivm__runtime__evaluation_memo__EvaluationMemo__tick_entries(const groove_EvaluationMemo *m);
usize groove__ivm__runtime__evaluation_memo__EvaluationMemo__len(const groove_EvaluationMemo *m);
void groove__ivm__runtime__evaluation_memo__EvaluationMemo__set_layout(groove_EvaluationMemo *m, void *layout);
typedef struct groove_EvaluationMemoIter { const groove_EvaluationMemo *m; u32 phase; u32 i; jz_hmap_iter it; } groove_EvaluationMemoIter;
void groove__ivm__runtime__evaluation_memo__EvaluationMemo__iter(const groove_EvaluationMemo *m, groove_EvaluationMemoIter *it);
bool groove__ivm__runtime__evaluation_memo__EvaluationMemoIter__next(groove_EvaluationMemoIter *it, const groove_EvalMemoKey **key, groove_EvalMemoEntry **entry);
void groove__ivm__runtime__evaluation_memo__EvaluationMemo__extend_from(groove_EvaluationMemo *m, groove_EvaluationMemo *src);
void groove__ivm__runtime__state__EvalMemoEntry__drop(groove_EvalMemoEntry *e);

/* counted_map.rs (type-erased over K with a btree desc) */
typedef struct groove_CountedMap { jz_vec chunks; JZ_VEC(usize) positive_tree; usize len; const jz_btree_desc *d; } groove_CountedMap;
typedef struct groove_CountedMapIter { const groove_CountedMap *m; u32 chunk; u32 idx; } groove_CountedMapIter;
void groove__ivm__runtime__counted_map__CountedMap__default(groove_CountedMap *m, const jz_btree_desc *d);
void groove__ivm__runtime__counted_map__CountedMap__clone(groove_CountedMap *dst, const groove_CountedMap *src, void (*kclone)(void *, const void *));
void groove__ivm__runtime__counted_map__CountedMap__drop(groove_CountedMap *m);
const i64 *groove__ivm__runtime__counted_map__CountedMap__get(const groove_CountedMap *m, const void *key);
bool groove__ivm__runtime__counted_map__CountedMap__insert(groove_CountedMap *m, const void *key, i64 weight, i64 *old);
bool groove__ivm__runtime__counted_map__CountedMap__remove(groove_CountedMap *m, const void *key, i64 *old);
usize groove__ivm__runtime__counted_map__CountedMap__positive_before(const groove_CountedMap *m, const void *key);
void groove__ivm__runtime__counted_map__CountedMap__iter(const groove_CountedMap *m, groove_CountedMapIter *it);
bool groove__ivm__runtime__counted_map__Iter__next(groove_CountedMapIter *it, const void **key, const i64 **weight);

/* rank_index.rs */
typedef struct groove_RankIndex groove_RankIndex;
groove_RankIndex *groove__ivm__runtime__rank_index__RankIndex__new_order(void);  /* Rc<RankIndex<CollectByOrderKey, Vec<u8>>> */
groove_RankIndex *groove__ivm__runtime__rank_index__RankIndex__new_keys(void);   /* Rc<RankIndex<Vec<u8>, ()>> */

/* windows.rs */
typedef struct groove_TopBySortPart {
    u32 key_tag;
    union { u64 u; i64 i; bool b; jz_str s; jz_bytes bytes; u8 uuid[16]; } key;
    u32 direction;
} groove_TopBySortPart;
int groove__ivm__runtime__windows__TopBySortPart__Ord__cmp(const groove_TopBySortPart *a, const groove_TopBySortPart *b);
void groove__ivm__runtime__windows__TopBySortPart__clone(groove_TopBySortPart *dst, const groove_TopBySortPart *src);
void groove__ivm__runtime__windows__TopBySortPart__drop(groove_TopBySortPart *p);
enum { GROOVE_ARGBY_MIN, GROOVE_ARGBY_MAX };
typedef struct groove_ArgBySpec { const usize *group_field_indices; u32 n_group; const usize *comparison_field_indices; u32 n_comparison; u32 direction; } groove_ArgBySpec;
typedef struct groove_WindowedRecord { jz_bytes record; i64 weight; } groove_WindowedRecord;
typedef JZ_VEC(groove_WindowedRecord) groove_WindowedRecordVec;

/* operator_updates.rs */
struct groove_TerminalLineage;
typedef struct groove_NodeRuntimeMeta {
    jz_hmap retainers;
    u64 last_used_tick;
    i8 depends_on_context;            /* -1 None, 0/1 */
    i8 has_hydration_state_ancestor;  /* -1 None, 0/1 */
    groove_NodeIdList *readiness_frontier;         /* Option<Arc<[NodeId]>> */
    struct groove_TerminalLineage *terminal_lineage; /* Option<Arc<..>> */
    groove_NodeInputSignature *input_signature;    /* Option<Arc<..>> */
    u64 input_generation;
    bool has_raw_projection_fields; groove_PreparedProjection *raw_projection_fields; /* Option<Option<Arc>> */
    void *pipeline;
    groove_StrList *join_left_fields, *join_right_fields;
    groove_PreparedRecordCopy *join_output;
    groove_StrList *aggregate_group_fields;
} groove_NodeRuntimeMeta;
void groove__ivm__runtime__operator_updates__NodeRuntimeMeta__default(groove_NodeRuntimeMeta *m);

/* join.rs */
typedef struct groove_ArrangementState groove_ArrangementState;  /* opaque, heap */
typedef struct groove_AsOfArrangement { groove_ArrangementState *value; groove_AsOfStamp as_of; } groove_AsOfArrangement;
typedef struct groove_SemiJoinState groove_SemiJoinState;
typedef struct groove_AntiJoinState groove_AntiJoinState;
groove_ArrangementState *groove__ivm__runtime__join__ArrangementState__default(void);
groove_ArrangementState *groove__ivm__runtime__join__ArrangementState__clone(const groove_ArrangementState *s);
void groove__ivm__runtime__join__ArrangementState__drop(groove_ArrangementState *s);
groove_SemiJoinState *groove__ivm__runtime__join__SemiJoinState__default(void);
groove_SemiJoinState *groove__ivm__runtime__join__SemiJoinState__clone(const groove_SemiJoinState *s);
void groove__ivm__runtime__join__SemiJoinState__drop(groove_SemiJoinState *s);
groove_AntiJoinState *groove__ivm__runtime__join__AntiJoinState__default(void);
groove_AntiJoinState *groove__ivm__runtime__join__AntiJoinState__clone(const groove_AntiJoinState *s);
void groove__ivm__runtime__join__AntiJoinState__drop(groove_AntiJoinState *s);
typedef struct groove_ArrangementTransition { const groove_AsOfArrangement *state; groove_SubTick expected; } groove_ArrangementTransition;
typedef JZ_VEC(struct groove_RecordDelta) groove_RecordDeltaVec_ext;
typedef struct groove_RecordWeight { jz_bytes record; i64 weight; } groove_RecordWeight;
typedef JZ_VEC(groove_RecordWeight) groove_RecordWeightVec;

/* recursion.rs */
typedef struct groove_RecursiveState groove_RecursiveState;  /* opaque, heap */
groove_RecursiveState *groove__ivm__runtime__recursion__RecursiveState__default(void);
groove_RecursiveState *groove__ivm__runtime__recursion__RecursiveState__clone(const groove_RecursiveState *s);
void groove__ivm__runtime__recursion__RecursiveState__drop(groove_RecursiveState *s);
bool groove__ivm__runtime__recursion__RecursiveState__has_pending_hydration(const groove_RecursiveState *s);
bool groove__ivm__runtime__recursion__RecursiveState__step_arrangements_hydrated(const groove_RecursiveState *s);
bool groove__ivm__runtime__recursion__RecursiveState__hydrated_input_generation(const groove_RecursiveState *s, u64 *out);
void groove__ivm__runtime__recursion__RecursiveState__mark_step_arrangements_hydrated(groove_RecursiveState *s);
void groove__ivm__runtime__recursion__RecursiveState__mark_hydrated_input_generation(groove_RecursiveState *s, u64 g);
usize groove__ivm__runtime__recursion__RecursiveState__accumulated_row_count(const groove_RecursiveState *s);
usize groove__ivm__runtime__recursion__RecursiveState__accumulated_encoded_bytes(const groove_RecursiveState *s);
bool groove__ivm__runtime__recursion__RecursiveState__is_empty(const groove_RecursiveState *s);
typedef struct groove_RecursiveNodes { groove_NodeId seed, step; bool has_step_witness; groove_NodeId step_witness; } groove_RecursiveNodes;

/* evaluation_session.rs */
typedef struct groove_EvaluationInputs groove_EvaluationInputs;
void groove__ivm__runtime__evaluation_session__EvaluationInputs__set_chunk_scope(groove_EvaluationInputs *in, bool has, groove_NodeId node);
void groove__ivm__runtime__evaluation_session__EvaluationInputs__release_chunks_owned_by(groove_EvaluationInputs *in, groove_NodeId owner);
enum { GROOVE_SRK_SCAN_PREFIX, GROOVE_SRK_SCAN_PREFIX_LIMIT, GROOVE_SRK_SCAN_RANGE, GROOVE_SRK_GET };
typedef struct groove_StorageRequestKey { u32 tag; jz_str family; jz_bytes prefix; jz_bytes end; usize max_items; bool reversed; } groove_StorageRequestKey;
typedef struct groove_KvPair { jz_bytes key; jz_bytes value; } groove_KvPair;
typedef JZ_VEC(groove_KvPair) groove_KvPairVec;
jz_err groove__ivm__runtime__evaluation_session__EvaluationInputs__rows(groove_EvaluationInputs *in, groove_StorageRequestKey key, const groove_KvPairVec **out);

/* terminal.rs */
enum { GROOVE_TPS_COLLECTION, GROOVE_TPS_KEY };
typedef struct groove_TerminalPathSegment { u32 tag; jz_str collection; jz_bytes key; } groove_TerminalPathSegment;
enum { GROOVE_TE_INSERT, GROOVE_TE_UPDATE, GROOVE_TE_REMOVE, GROOVE_TE_MOVE };
typedef struct groove_TerminalEdit { u32 tag; usize index; jz_bytes key; jz_bytes value; } groove_TerminalEdit;
typedef struct groove_TerminalOperation {
    groove_RecordDescriptor root_descriptor; jz_bytes root_key;
    JZ_VEC(groove_TerminalPathSegment) path; groove_TerminalEdit edit;
} groove_TerminalOperation;
typedef JZ_VEC(groove_TerminalOperation) groove_TerminalOperationVec;
typedef struct groove_TerminalDeltas { groove_TerminalOperationVec operations; } groove_TerminalDeltas;
void groove__ivm__runtime__terminal__TerminalDeltas__clone(groove_TerminalDeltas *dst, const groove_TerminalDeltas *src);
void groove__ivm__runtime__terminal__TerminalDeltas__drop(groove_TerminalDeltas *t);
bool groove__ivm__runtime__terminal__TerminalDeltas__eq(const groove_TerminalDeltas *a, const groove_TerminalDeltas *b);

/* key_encoding.rs */
jz_err groove__ivm__runtime__key_encoding__primary_key_field_indices(const groove_TableSchema *t, groove_RecordDescriptor d, groove_UsizeVec *out);
jz_err groove__ivm__runtime__key_encoding__encode_record_field_key_part(jz_bytes *key, groove_RecordDescriptor d, const u8 *raw, u32 len, usize field);
jz_err groove__ivm__runtime__key_encoding__primary_key_value_bytes(groove_RecordDescriptor d, const u8 *raw, u32 len, const usize *fields, u32 n, jz_bytes *out);
jz_err groove__ivm__runtime__key_encoding__record_field_key_parts(groove_RecordDescriptor d, const u8 *raw, u32 len, usize field, JZ_VEC(jz_bytes) *out);
jz_err groove__ivm__runtime__key_encoding__encode_key_part(jz_bytes *key, const groove_Value *v);
void groove__ivm__runtime__key_encoding__consolidate_deltas(void *deltas_vec /* Vec<RecordDelta> in/out */);

/* index_ids.rs */
void groove__ivm__runtime__index_ids__durable_index_key_prefix(u32 id, jz_bytes *out);

/* large_values */
typedef struct groove_StreamingChecksum groove_StreamingChecksum;
typedef struct groove_ByteRange { u64 start, end; } groove_ByteRange;
jz_err groove__large_values__StreamingChecksum__new(groove_LargeValueRef *value, usize window_bytes, usize max_bytes_per_turn, groove_StreamingChecksum **out);
groove_StreamingChecksum *groove__large_values__StreamingChecksum__clone(const groove_StreamingChecksum *s);
void groove__large_values__StreamingChecksum__drop(groove_StreamingChecksum *s);
u64 groove__large_values__StreamingChecksum__remaining_bytes(const groove_StreamingChecksum *s);
bool groove__large_values__StreamingChecksum__next_range(const groove_StreamingChecksum *s, groove_ByteRange *out);
const groove_LargeValueRef *groove__large_values__StreamingChecksum__value(const groove_StreamingChecksum *s);
jz_err groove__large_values__StreamingChecksum__consume_window(groove_StreamingChecksum *s, const jz_bytes *bytes, bool *should_yield);
jz_err groove__large_values__StreamingChecksum__record_yield(groove_StreamingChecksum *s);
jz_err groove__large_values__StreamingChecksum__finish(groove_StreamingChecksum *s, u8 out[32]); /* consumes */
jz_err groove__large_values__byte_range_attempt(const groove_LargeValueRef *value, groove_ByteRange range, groove_EvaluationInputs *in, jz_bytes *out);
jz_err groove__large_values__materialize_record_attempt(groove_RecordDescriptor d, const u8 *raw, u32 len, groove_EvaluationInputs *in, jz_bytes *out);
jz_err groove__large_values__materialize_record_fields_attempt(groove_RecordDescriptor d, const u8 *raw, u32 len, const usize *idx, u32 n, groove_EvaluationInputs *in, jz_bytes *out);

/* storage */
typedef struct groove_OrderedKvStorage groove_OrderedKvStorage; /* dyn: vtable owned by storage unit */

/* ======================================================================
 * Part 2: rt-b functions over this unit's types (forward-declared structs)
 * ==================================================================== */
struct groove_RecordDeltas; struct groove_RecordDelta; struct groove_TableDelta; struct groove_BindingDelta;
struct groove_CollectByGroup; struct groove_CollectByIncrementalState; struct groove_CollectByOrderKey;
struct groove_GraphRuntimeView; struct groove_CollectBySlot;
/* operator_updates.rs: NodeState */
jz_err groove__ivm__runtime__operator_updates__NodeState__update_map_project(const groove_MapProjectOp *p, groove_RecordDescriptor out_desc, const struct groove_RecordDeltas *input, const groove_PreparedProjection *raw, bool flag, struct groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_map_project_slice(const groove_MapProjectOp *p, groove_RecordDescriptor out_desc, groove_RecordDescriptor in_desc, const struct groove_RecordDelta *input, u32 n, const groove_PreparedProjection *raw, bool flag, struct groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_filter(const groove_FilterOp *f, groove_RecordDescriptor out_desc, const struct groove_RecordDeltas *input, struct groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_table_source_from_inputs(const groove_TableSourceOp *op, const groove_DatabaseSchema *s, const jz_hmap *vp, groove_RecordDescriptor out_desc, groove_EvaluationInputs *in, struct groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_table_source(const groove_TableSourceOp *op, const groove_DatabaseSchema *s, const jz_hmap *vp, groove_RecordDescriptor out_desc, const struct groove_TableDelta *td, u32 ntd, struct groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_index_source_from_inputs(const groove_IndexSourceOp *op, const groove_DatabaseSchema *s, const jz_hmap *vp, groove_RecordDescriptor out_desc, groove_EvaluationInputs *in, struct groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_index_source(const groove_IndexSourceOp *op, const groove_DatabaseSchema *s, const jz_hmap *vp, groove_RecordDescriptor out_desc, const struct groove_TableDelta *td, u32 ntd, const groove_OrderedKvStorage *storage, u32 eval_mode, struct groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_binding_source(const groove_BindingSourceOp *op, groove_RecordDescriptor out_desc, const struct groove_BindingDelta *bd, u32 nbd, const jz_hmap *snapshots, u32 mode, struct groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_unwrap_nullable(const groove_UnwrapNullableOp *op, groove_RecordDescriptor out_desc, const struct groove_RecordDeltas *input, struct groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_unnest(const groove_UnnestOp *op, groove_RecordDescriptor out_desc, const struct groove_RecordDeltas *input, struct groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_variant_project(const groove_VariantProjectOp *op, groove_RecordDescriptor out_desc, const struct groove_RecordDeltas *input, struct groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_index_by(const groove_IndexByOp *op, groove_RecordDescriptor out_desc, const struct groove_RecordDeltas *input, struct groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__update_union(groove_RecordDescriptor out_desc, struct groove_RecordDeltas **inputs, u32 n, struct groove_RecordDeltas *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__group_source_rows(const groove_TableSchema *t, const groove_KvPairVec *rows, JZ_VEC(struct groove_TableDelta) *out);
/* windows.rs */
void groove__ivm__runtime__windows__top_by_window_from_ordered_group(const struct groove_CollectByGroup *g /* nullable */, const groove_TopByOp *top_by, groove_WindowedRecordVec *out);
void groove__ivm__runtime__windows__records_before_deltas(groove_RecordWeightVec after, const struct groove_RecordDelta *d, u32 n, groove_RecordWeightVec *out);
jz_err groove__ivm__runtime__windows__update_unbounded_collect_by_terminal_state(groove_RecordDescriptor in_desc, groove_RecordDescriptor out_desc, const groove_CollectByOp *cb, const struct groove_CollectBySlot *direct_tree_slot, struct groove_CollectByIncrementalState *state, const struct groove_RecordDelta *d, u32 n, bool emit, groove_TerminalOperationVec *out);
jz_err groove__ivm__runtime__windows__collect_by_root_from_records(groove_RecordDescriptor in_desc, groove_RecordDescriptor out_desc, const groove_CollectByOp *cb, const groove_RecordWeight *r, u32 n, bool *some, jz_bytes *out);
jz_err groove__ivm__runtime__windows__collect_by_parent_from_records(groove_RecordDescriptor in_desc, groove_RecordDescriptor out_desc, const groove_CollectByOp *cb, const groove_RecordWeight *r, u32 n, bool *some, jz_bytes *out);
jz_err groove__ivm__runtime__windows__collect_by_tree_parent_from_records(groove_RecordDescriptor in_desc, groove_RecordDescriptor out_desc, const groove_CollectByOp *cb, const groove_RecordWeight *r, u32 n, bool *some, jz_bytes *out);
jz_err groove__ivm__runtime__windows__collect_by_expanded_window(groove_RecordDescriptor in_desc, groove_RecordDescriptor out_desc, const groove_CollectByOp *cb, const groove_RecordWeight *r, u32 n, jz_btree *out /* Vec<u8> -> Bytes */);
jz_err groove__ivm__runtime__windows__top_by_sort_key(groove_RecordDescriptor d, const u8 *raw, u32 len, const groove_TopByOp *top_by, void *out /* Vec<TopBySortPart> */);
void groove__ivm__runtime__windows__diff_record_windows(groove_WindowedRecordVec before, groove_WindowedRecordVec after, void *out /* Vec<RecordDelta>, appended */);
jz_err groove__ivm__runtime__windows__update_unbounded_top_by_group_touching(groove_RecordDescriptor d, const groove_TopByOp *top_by, struct groove_CollectByGroup *group, const struct groove_RecordDelta *input, u32 n, void *touched /* nullable Vec<CollectByOrderKey> */, void *out /* Vec<RecordDelta> */);
jz_err groove__ivm__runtime__windows__encoded_record_key_part(groove_RecordDescriptor d, const u8 *raw, u32 len, const usize *idx, u32 n, jz_bytes *out);
jz_err groove__ivm__runtime__windows__encoded_identity_key_part(groove_RecordDescriptor d, const u8 *raw, u32 len, const usize *idx, u32 n, jz_bytes *out);
jz_err groove__ivm__runtime__windows__encoded_arrangement_key_part(groove_RecordDescriptor d, const u8 *raw, u32 len, const usize *idx, u32 n, jz_bytes *out);
jz_err groove__ivm__runtime__windows__arg_by_order_key(groove_RecordDescriptor d, const struct groove_RecordDelta *delta, const groove_ArgBySpec *spec, struct groove_CollectByOrderKey *out);
bool groove__ivm__runtime__windows__arg_by_ordered_winner(const struct groove_CollectByGroup *g, jz_bytes *out);
/* join.rs */
jz_err groove__ivm__runtime__join__JoinState__evaluate_prepared(groove_ArrangementTransition l, groove_ArrangementTransition r, const groove_PreparedRecordCopy *proj, u32 mode, void *out /* Vec<RecordDelta> */);
void groove__ivm__runtime__join__SemiJoinState__evaluate(groove_SemiJoinState *s, groove_ArrangementTransition l, groove_ArrangementTransition r, u32 mode, void *out);
void groove__ivm__runtime__join__AntiJoinState__evaluate(groove_AntiJoinState *s, groove_ArrangementTransition l, groove_ArrangementTransition r, u32 mode, void *out);
jz_err groove__ivm__runtime__join__join_output_mapping(groove_RecordDescriptor l, groove_RecordDescriptor r, groove_RecordDescriptor o, groove_UsizePairVec *out);
typedef struct groove_JoinInput { const struct groove_RecordDelta *records; u32 n; const struct groove_RecordDeltas *snapshot; } groove_JoinInput;
jz_err groove__ivm__runtime__join__prepare_arrangement(groove_AsOfArrangement *state, groove_RecordDescriptor d, const jz_str *fields, u32 nfields, u32 comparison, groove_JoinInput input, groove_SubTick stamp, u32 mode);
jz_err groove__ivm__runtime__join__ArrangementState__apply_record_deltas(groove_ArrangementState *s, groove_RecordDescriptor d, const jz_str *fields, u32 nfields, const struct groove_RecordDelta *deltas, u32 n, u32 mode);
void groove__ivm__runtime__join__ArrangementState__records_for_key(const groove_ArrangementState *s, const u8 *key, u32 len, groove_RecordWeightVec *out);
usize groove__ivm__runtime__join__ArrangementState__row_count(const groove_ArrangementState *s);
usize groove__ivm__runtime__join__ArrangementState__encoded_bytes(const groove_ArrangementState *s);
/* aggregate.rs */
jz_err groove__ivm__runtime__aggregate__aggregate_row_from_records(groove_RecordDescriptor in_desc, groove_RecordDescriptor out_desc, const groove_AggregateOp *agg, const groove_RecordWeight *r, u32 n, bool *some, jz_bytes *out);
void groove__ivm__runtime__aggregate__records_before_from_deltas(groove_RecordWeightVec after, void *deltas /* Vec<RecordDelta>, consumed */, groove_RecordWeightVec *out);
/* recursion.rs */
jz_err groove__ivm__runtime__recursion__hydrated_arg_by_winners(const struct groove_RecordDeltas *input, groove_RecordDescriptor out_desc, const usize *g, u32 ng, const usize *c, u32 nc, u32 direction, struct groove_RecordDeltas *out);
jz_err groove__ivm__runtime__recursion__snapshot_requirement(const groove_IvmGraph *g, groove_NodeId node, groove_RecursiveNodes nodes, const struct groove_TableDelta *td, u32 ntd, const struct groove_BindingDelta *bd, u32 nbd, const groove_RecursiveState *state, bool *some, groove_NodeId *out);
jz_err groove__ivm__runtime__recursion__require_snapshot_inputs(const groove_IvmGraph *g, groove_EvaluationInputs *in, groove_NodeId root);
enum { GROOVE_HRP_YIELD, GROOVE_HRP_READY_FOR_ARRANGEMENT_HYDRATION };
typedef struct groove_HydrationRecomputeContext {
    const groove_DatabaseSchema *schema; const groove_IvmGraph *graph; const jz_hmap *variant_projections;
    groove_EvaluationInputs *inputs; const struct groove_TableDelta *table_deltas; u32 n_table_deltas; bool has_table_deltas;
    const groove_OrderedKvStorage *storage; const jz_hmap *binding_snapshots; groove_ScopeId scope; u64 input_generation;
} groove_HydrationRecomputeContext;
jz_err groove__ivm__runtime__recursion__resume_inputs_hydration_recompute(groove_RecursiveState *s, groove_HydrationRecomputeContext ctx, groove_NodeId node, const groove_RecursiveOp *rec, groove_RecordDescriptor out_desc, groove_RecursiveNodes nodes, u32 *progress);
enum { GROOVE_RDP_YIELD, GROOVE_RDP_READY };
jz_err groove__ivm__runtime__recursion__recursive_delta(groove_RecursiveState *s, struct groove_GraphRuntimeView *runtime, groove_NodeId node, const groove_RecursiveOp *rec, groove_RecordDescriptor out_desc, groove_RecursiveNodes nodes, u32 *progress, void *out /* Vec<RecordDelta> */);
jz_err groove__ivm__runtime__recursion__hydrate_recursive_arrangements(struct groove_GraphRuntimeView *runtime, const groove_RecursiveOp *rec, groove_NodeId step, struct groove_RecordDeltas accumulated);
void groove__ivm__runtime__recursion__RecursiveState__pending_hydration_accumulated_deltas(const groove_RecursiveState *s, groove_RecordDescriptor d, struct groove_RecordDeltas *out);
void groove__ivm__runtime__recursion__RecursiveState__finish_hydration_recompute(groove_RecursiveState *s, jz_hmap *next, jz_hmap *step_witness);
void groove__ivm__runtime__recursion__RecursiveState__replace_with(groove_RecursiveState *s, jz_hmap next, void *out_deltas /* nullable */);
void groove__ivm__runtime__recursion__RecursiveState__replace_step_witness_with(groove_RecursiveState *s, jz_hmap next);
void groove__ivm__runtime__recursion__RecursiveState__accumulated_deltas(const groove_RecursiveState *s, void *out);
void groove__ivm__runtime__recursion__RecursiveState__step_witness_deltas(const groove_RecursiveState *s, void *out);
void groove__ivm__runtime__recursion__RecursiveState__step_witness_accumulated_deltas(const groove_RecursiveState *s, void *out);


/* ======================================================================
 * Part 3: declarations used by subscriptions.c / runtime_tick.c
 * ==================================================================== */
/* records / large_values */
enum { GROOVE_LVK_BYTES, GROOVE_LVK_STRING, GROOVE_LVK_JSON };
u32 groove__large_values__LargeValueRef__kind(const groove_LargeValueRef *l);
jz_err groove__large_values__compare_inline_attempt(const groove_LargeValueRef *l, const u8 *inline_bytes, u32 n, groove_EvaluationInputs *in, int *ordering);
bool groove__records__RecordDescriptor__field_index_by_identity(groove_RecordDescriptor d, const groove_FieldIdentity *id, usize *out);
bool groove__records__ValueType__registry_compatible_with(const groove_ValueType *a, const groove_ValueType *b);
bool groove__records__values__EnumValue__tag_is(const groove_EnumValue *e, u32 tag);
void groove__records__values__ValueVec__drop(groove_ValueVec *v);
bool groove__records__FieldIdentity__eq(const groove_FieldIdentity *a, const groove_FieldIdentity *b);
void groove__records__FieldIdentity__drop(groove_FieldIdentity *f);
groove_LiteralValue groove__ivm__op_types__LiteralValue__from_value_ref(const groove_Value *v);
void groove__ivm__op_types__LiteralValue__to_value(const groove_LiteralValue *l, groove_Value *out);
bool groove__ivm__op_types__LiteralValue__value_type(const groove_LiteralValue *l, groove_ValueType *out);

/* ivm::op_types::PredicateExpr constructors */
void groove__ivm__op_types__PredicateExpr__is_null(jz_str field, groove_PredicateExpr *out);
void groove__ivm__op_types__PredicateExpr__eq_literal(jz_str field, groove_LiteralValue value, groove_PredicateExpr *out);
void groove__ivm__op_types__PredicateExpr__canonicalize(groove_PredicateExpr p /* consumed */, groove_PredicateExpr *out);

/* ivm::graph::GraphBuilder (value type; children are rc payloads) */
void groove__ivm__graph__GraphBuilder__clone(groove_GraphBuilder *dst, const groove_GraphBuilder *src);
void groove__ivm__graph__GraphBuilder__drop(groove_GraphBuilder *g);
void groove__ivm__graph__GraphBuilder__rc_drop(void *g);
bool groove__ivm__graph__GraphBuilder__eq(const groove_GraphBuilder *a, const groove_GraphBuilder *b);
u64 groove__ivm__graph__GraphBuilder__hash(const groove_GraphBuilder *g, u64 h);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__arc_new(groove_GraphBuilder value /* moved */);
void groove__ivm__graph__GraphBuilder__filter(groove_GraphBuilder self /* moved */, groove_PredicateExpr predicate, groove_GraphBuilder *out);
void groove__ivm__graph__GraphBuilder__project_fields(groove_GraphBuilder self /* moved */, groove_ProjectField *fields, u32 n /* moved */, groove_GraphBuilder *out);
void groove__ivm__graph__FieldRef__stored_name(jz_str name, groove_FieldRef *out);
void groove__ivm__graph__FieldRef__drop(groove_FieldRef *f);
u64 groove__ivm__graph__FieldRef__hash(const groove_FieldRef *f, u64 h);
u64 groove__ivm__graph__ProjectField__hash(const groove_ProjectField *f, u64 h);
bool groove__ivm__graph__ProjectField__eq(const groove_ProjectField *a, const groove_ProjectField *b);
void groove__ivm__graph__ProjectField__clone(groove_ProjectField *dst, const groove_ProjectField *src);
u64 groove__ivm__graph__TopByOrder__hash(const groove_TopByOrder *o, u64 h);
bool groove__ivm__graph__TopByOrder__eq(const groove_TopByOrder *a, const groove_TopByOrder *b);
u64 groove__ivm__graph__CollectByBuilder__hash(const groove_CollectByBuilder *c, u64 h);
bool groove__ivm__graph__CollectByBuilder__eq(const groove_CollectByBuilder *a, const groove_CollectByBuilder *b);
groove_CollectByBuilder *groove__ivm__graph__CollectByBuilder__box_clone(const groove_CollectByBuilder *c);
u64 groove__ivm__graph__AggregateExpr__hash(const groove_AggregateExpr *a, u64 h);
bool groove__ivm__graph__AggregateExpr__eq(const groove_AggregateExpr *a, const groove_AggregateExpr *b);
u64 groove__ivm__op_types__StaticScanSpec__hash(const groove_StaticScanSpec *s, u64 h);
bool groove__ivm__op_types__StaticScanSpec__eq(const groove_StaticScanSpec *a, const groove_StaticScanSpec *b);
u64 groove__ivm__graph__IndexIntersections__hash(const jz_vec *v, u64 h);
bool groove__ivm__graph__IndexIntersections__eq(const jz_vec *a, const jz_vec *b);
u64 groove__ivm__graph__IndexCandidateFilter__hash(const void *f, u64 h);
bool groove__ivm__graph__IndexCandidateFilter__eq(const void *a, const void *b);
u64 groove__ivm__template__TypedGraphTemplate__hash(const groove_TypedGraphTemplate *t, u64 h);
bool groove__ivm__template__TypedGraphTemplate__eq(const groove_TypedGraphTemplate *a, const groove_TypedGraphTemplate *b);
u64 groove__ivm__template__TemplateScalars__hash(const void *s, u32 n, u64 h);
bool groove__ivm__template__TemplateScalars__eq(const void *a, u32 na, const void *b, u32 nb);
u32 groove__ivm__template__TypedGraphTemplate__predicate_marker_count(const groove_TypedGraphTemplate *t);
u32 groove__ivm__template__TypedGraphTemplate__input_count(const groove_TypedGraphTemplate *t);
groove_RecordDescriptor groove__ivm__template__TypedGraphTemplate__input(const groove_TypedGraphTemplate *t, u32 i);
groove_RecordDescriptor groove__ivm__template__TypedGraphTemplate__output(const groove_TypedGraphTemplate *t);

/* ivm::graph::IvmGraph route barriers + routes.rs */
typedef struct groove_RouteIndex groove_RouteIndex;
typedef struct groove_RouteTable {
    groove_UsizeVec field_indices; groove_ValueTypeVec field_types;
    jz_hmap by_key;              /* HashMap<Vec<u8>, HashSet<NodeId>> */
    jz_hmap root_ordering_nodes; /* HashSet<NodeId> */
} groove_RouteTable;
const groove_RouteIndex *groove__ivm__graph__IvmGraph__routes(const groove_IvmGraph *g);
const groove_RouteTable *groove__ivm__routes__RouteIndex__table(const groove_RouteIndex *r, groove_NodeId shared);
void groove__ivm__graph__IvmGraph__add_route_barrier(groove_IvmGraph *g, groove_NodeId shared, groove_NodeId barrier, groove_UsizeVec field_indices, groove_ValueTypeVec field_types, jz_bytes key, bool has_root_ordering, groove_NodeId root_ordering_node);
void groove__ivm__graph__IvmGraph__release_route_barrier(groove_IvmGraph *g, groove_NodeId barrier);
void groove__ivm__graph__IvmGraph__affected_nodes_through_routes(const groove_IvmGraph *g, const jz_str *tables, u32 nt, const groove_BindingSourceKey *bindings, u32 nb, groove_NodeIdVec *out);
bool groove__ivm__routes__is_routable_type(const groove_ValueType *t);
bool groove__ivm__routes__encode_route_key(const groove_Value *values, u32 n, const groove_ValueType *types, u32 nt, jz_bytes *out);

/* op_types ids */
void groove__ivm__op_types__BindingSourceKey__prepared(const char *shape, u32 len, groove_BindingSourceKey *out);
void groove__ivm__op_types__BindingSourceKey__clone(groove_BindingSourceKey *dst, const groove_BindingSourceKey *src);
void groove__ivm__op_types__BindingSourceKey__drop(groove_BindingSourceKey *k);
bool groove__ivm__op_types__BindingSourceKey__eq(const groove_BindingSourceKey *a, const groove_BindingSourceKey *b);
bool groove__ivm__op_types__InputSourceId__belongs_to(groove_InputSourceId id, u64 ns);
bool groove__ivm__op_types__InputSourceId__was_allocated_by(groove_InputSourceId id, u64 ns, u64 next);
void groove__ivm__op_types__InputSourceId__binding_key(groove_InputSourceId id, groove_BindingSourceKey *out);
void groove__ivm__op_types__InputSourceId__diagnostic_name(groove_InputSourceId id, jz_str *out);
int groove__ivm__op_types__InputSourceId__cmp(const void *a, const void *b);

/* runtime/mod.rs variant projections: HashMap<VariantProjectionKey, VariantProjection> */
typedef struct groove_VariantProjectionKey { jz_str table; groove_VariantProjectionTarget target; } groove_VariantProjectionKey;
typedef struct groove_VariantProjection { groove_RecordDescriptor output; /* ... */ } groove_VariantProjection;

/* storage: Rc<dyn OrderedKvStorage> handle (OwnedStorage) */
typedef struct groove_OwnedStorage { groove_OrderedKvStorage *storage; /* rc */ } groove_OwnedStorage;
groove_OwnedStorage groove__storage__OwnedStorage__new(groove_OrderedKvStorage *rc_storage);
void groove__storage__OwnedStorage__drop(groove_OwnedStorage *s);


/* rt-b IvmRuntime methods used by this unit (IvmRuntime mirror lives in subscriptions.h) */
typedef struct groove_IvmRuntime groove_IvmRuntime;
struct groove_CompiledNode; struct groove_SubscriptionId;
typedef struct groove_ProjectionPlan groove_ProjectionPlan;
jz_err groove__ivm__runtime__typed_projection__IvmRuntime__projection_plan(groove_IvmRuntime *self, groove_RecordDescriptor input, const groove_ProjectField *fields, u32 n, groove_ProjectionPlan **out);
groove_RecordDescriptor groove__ivm__runtime__typed_projection__ProjectionPlan__output(const groove_ProjectionPlan *p);
void groove__ivm__runtime__typed_projection__ProjectionPlan__release(groove_ProjectionPlan *p);
jz_err groove__ivm__runtime__compilation__IvmRuntime__add_dedup_graph(groove_IvmRuntime *self, const groove_GraphBuilder *g, struct groove_CompiledNode *out);
jz_err groove__ivm__runtime__compilation__IvmRuntime__add_dedup_schema_index(groove_IvmRuntime *self, const groove_TableSchema *t, const groove_IndexSchema *i);
bool groove__ivm__runtime__graph_lifecycle__IvmRuntime__add_retainer(groove_IvmRuntime *self, groove_NodeId id, groove_Retainer retainer);
bool groove__ivm__runtime__graph_lifecycle__IvmRuntime__remove_retainer(groove_IvmRuntime *self, groove_NodeId id, const groove_Retainer *retainer);
void groove__ivm__runtime__graph_lifecycle__IvmRuntime__collect_unretained_ephemeral_nodes(groove_IvmRuntime *self);
bool groove__ivm__runtime__graph_lifecycle__IvmRuntime__remove_multisink_retainers(groove_IvmRuntime *self, struct groove_SubscriptionId id, const jz_btree *outputs);
groove_RecordDescriptor groove__ivm__runtime__key_encoding__join_descriptor(groove_RecordDescriptor l, groove_RecordDescriptor r);


/* ======================================================================
 * Part 4: dependencies of runtime_tick.rs
 * ==================================================================== */
struct groove_RecordDeltas; struct groove_TickEvaluator; struct groove_FrameInputs; struct groove_TableDelta;
/* execution_layout.rs (graph unit): Arc<ExecutionLayout> is a jz_rc payload */
typedef struct groove_OptU32 { bool some; u32 v; } groove_OptU32;
typedef JZ_VEC(groove_OptU32) groove_OptU32Vec;
typedef struct groove_ExecutionLayout {
    groove_NodeIdVec nodes;
    jz_hmap slots; /* NodeId -> usize */
    groove_NodeIdVec roots; /* sorted */
    groove_UsizeVec input_counts, input_offsets, input_slots, source_slots;
    groove_OptU32Vec pipeline_predecessors;
    groove_UsizeVec dependent_offsets, dependent_slots;
} groove_ExecutionLayout;
const u32 *groove__ivm__execution_layout__ExecutionLayout__dependents(const groove_ExecutionLayout *self, u32 slot, u32 *len);
const u32 *groove__ivm__execution_layout__ExecutionLayout__inputs(const groove_ExecutionLayout *self, u32 slot, u32 *len);
/* graph.rs / activation.rs */
typedef JZ_VEC(groove_BindingSourceKey) groove_BindingSourceKeyVec;
typedef struct groove_ActivationPlan {
    groove_NodeIdVec sources;
    jz_hmap *affected; jz_hmap *relevant; /* Arc<HashSet<NodeId>> rc payloads */
    groove_ExecutionLayout *layout;
    groove_NodeIdVec durable, ephemeral;
    groove_StrVec tables; groove_BindingSourceKeyVec bindings;
    groove_NodeIdVec routed;
} groove_ActivationPlan; /* Arc<ActivationPlan>: jz_rc payload */
bool groove__ivm__graph__IvmGraph__execution_layout(const groove_IvmGraph *self, const groove_NodeId *roots, u32 n, groove_ExecutionLayout **out, groove_NodeId *err);
void groove__ivm__graph__IvmGraph__downstream_through_routes(const groove_IvmGraph *self, const groove_NodeId *roots, u32 n, jz_hmap *out);
jz_hmap *groove__ivm__graph__IvmGraph__affected_nodes(const groove_IvmGraph *self, const jz_str *tables, u32 nt, const groove_BindingSourceKey *bindings, u32 nb);
bool groove__ivm__graph__IvmGraph__activation_plan(const groove_IvmGraph *self, const jz_str *tables, u32 nt, const groove_BindingSourceKey *bindings, u32 nb, groove_ActivationPlan **out, groove_NodeId *err);
bool groove__ivm__graph__GraphNode__is_durable(const groove_GraphNode *node);
extern const jz_hmap_desc groove__ivm__NODESET_DESC;
jz_hmap *groove__ivm__nodeset_arc_new(jz_hmap set);
/* routes.rs */
bool groove__ivm__routes__RouteTable__key_of_record(const groove_RouteTable *self, const groove_BorrowedRecord *record, jz_bytes *out);
void groove__ivm__routes__RouteTable__barriers(const groove_RouteTable *self, groove_NodeIdVec *out);

/* evaluation_session.rs (rt-b) */
typedef struct groove_ChunkRequestMirror { u8 object_hash[32]; u64 offset, len; } groove_ChunkRequestMirror;
enum { GROOVE_ERK_STORAGE, GROOVE_ERK_CHUNK };
typedef struct groove_EvaluationRequestKey { u8 tag; groove_StorageRequestKey storage; groove_ChunkRequestMirror chunk; } groove_EvaluationRequestKey;
void groove__ivm__runtime__evaluation_session__EvaluationRequestKey__clone(groove_EvaluationRequestKey *dst, const groove_EvaluationRequestKey *src);
extern const jz_btree_desc groove_ivm_rt_REQUEST_OUTPUT_MAP; /* BTreeMap<EvaluationRequestKey, EvaluationRequestOutput> */
extern const jz_btree_desc groove_ivm_rt_REQUEST_KEY_SET;    /* BTreeSet<EvaluationRequestKey> */
struct groove_EvaluationInputs {
    jz_btree loaded; jz_btree missing; jz_btree chunk_owners;
    bool has_chunk_scope; groove_NodeId chunk_scope;
};
void groove__ivm__runtime__evaluation_session__EvaluationInputs__default(groove_EvaluationInputs *in);
void groove_EvaluationInputs_drop(groove_EvaluationInputs *in);
void groove__ivm__runtime__evaluation_session__EvaluationInputs__install(groove_EvaluationInputs *in, jz_btree *ready);
void groove__ivm__runtime__evaluation_session__EvaluationInputs__take_missing(groove_EvaluationInputs *in, jz_btree *out);
void groove__ivm__runtime__evaluation_session__EvaluationInputs__hand_off_chunks(groove_EvaluationInputs *in, groove_EvaluationInputs *successor);
typedef struct groove_EvaluationRequestFailure { jz_err error; bool publication_metadata_durability; } groove_EvaluationRequestFailure;
typedef struct groove_EvaluationRequests { jz_btree pending; jz_btree ready; } groove_EvaluationRequests;
void groove__ivm__runtime__evaluation_session__EvaluationRequests__new(groove_EvaluationRequests *r);
void groove_EvaluationRequests_drop(groove_EvaluationRequests *r);
bool groove__ivm__runtime__evaluation_session__EvaluationRequests__request(groove_EvaluationRequests *r, groove_EvaluationRequestKey key,
    const groove_OrderedKvStorage *storage, const void *chunks /* Option<&OwnedChunkProvider> */, const groove_DatabaseSchema *schema);
u32 groove__ivm__runtime__evaluation_session__EvaluationRequests__poll(groove_EvaluationRequests *r);
u32 groove__ivm__runtime__evaluation_session__EvaluationRequests__poll_eager_retry(groove_EvaluationRequests *r);
bool groove__ivm__runtime__evaluation_session__EvaluationRequests__has_pending(const groove_EvaluationRequests *r);
void groove__ivm__runtime__evaluation_session__EvaluationRequests__hand_off_chunks(groove_EvaluationRequests *r, groove_EvaluationRequests *successor);
bool groove__ivm__runtime__evaluation_session__EvaluationRequests__has_pending_chunk(const groove_EvaluationRequests *r);
bool groove__ivm__runtime__evaluation_session__EvaluationRequests__drain_ready(groove_EvaluationRequests *r, jz_btree *out,
    groove_EvaluationRequestKey *failed_key, groove_EvaluationRequestFailure *failure);
/* operator_updates.rs */
jz_err groove__ivm__runtime__operator_updates__NodeState__table_source_request(const groove_TableSourceOp *input, bool *some, groove_StorageRequestKey *out);
jz_err groove__ivm__runtime__operator_updates__NodeState__index_source_request(const groove_IndexSourceOp *input, bool *some, groove_StorageRequestKey *out);
void groove_NodeRuntimeMeta_drop(void *m);
void groove_NodeRuntimeMeta_clone(void *dst, const void *src);
extern const jz_hmap_desc groove_ivm_rt_NODE_META_MAP;           /* NodeId -> NodeRuntimeMeta */
/* mod.rs maps */
extern const jz_hmap_desc groove_ivm_rt_ARRANGEMENT_STATES_MAP;  /* ArrangementKey -> AsOf<ArrangementState, SubTick> */
extern const jz_hmap_desc groove_ivm_rt_ARRANGEMENT_KEY_SET;     /* HashSet<ArrangementKey> */
extern const jz_hmap_desc groove_ivm_rt_NODE_KEYSET_MAP;         /* NodeId -> HashSet<ArrangementKey> */
extern const jz_hmap_desc groove_ivm_rt_EVAL_MEMO_KEY_SET;       /* HashSet<EvalMemoKey> */
void groove__ivm__runtime__IvmRuntime__insert_retained_eval_memo(groove_IvmRuntime *self, groove_EvalMemoKey key, groove_EvalMemoEntry entry);
void groove__ivm__runtime__IvmRuntime__remove_retained_eval_memo(groove_IvmRuntime *self, const groove_EvalMemoKey *key);
void groove__ivm__runtime__IvmRuntime__remove_retained_eval_memos_for_nodes(groove_IvmRuntime *self, const jz_hmap *nodes);
/* graph_lifecycle.rs */
void groove__ivm__runtime__graph_lifecycle__IvmRuntime__stats(const groove_IvmRuntime *self, groove_RuntimeStats *out);
void groove__ivm__runtime__graph_lifecycle__IvmRuntime__cheap_stats(const groove_IvmRuntime *self, groove_RuntimeStats *out);
void groove__ivm__runtime__graph_lifecycle__IvmRuntime__record_hydration_memo_metrics(groove_IvmRuntime *self, const groove_TickMetrics *metrics);
/* join.rs / recursion.rs / state.rs */
void groove__ivm__runtime__join__AsOfArrangement__clone(void *dst, const void *src);
void groove__ivm__runtime__join__ArrangementState__release_changes(groove_ArrangementState *s);
void groove__ivm__runtime__join__ArrangementState__commit_overlay(groove_ArrangementState *s);
void groove__ivm__runtime__join__SemiJoinState__commit_published_overlay(groove_SemiJoinState *s);
void groove__ivm__runtime__join__AntiJoinState__commit_published_overlay(groove_AntiJoinState *s);
void groove__ivm__runtime__recursion__RecursiveState__commit_staged_positive(groove_RecursiveState *s);
void groove__ivm__runtime__state__EvalContext__root(groove_EvalContext *c);
/* evaluation_memo.rs */
groove_EvaluationMemo *groove__ivm__runtime__evaluation_memo__EvaluationMemo__for_layout(groove_ExecutionLayout *layout /* rc, cloned */);
/* pipeline.rs: Poll<Result<Arc<RecordDeltas>>>; false while pending */
extern const jz_hmap_desc groove_ivm_rt_PENDING_PIPELINE_MAP; /* NodeId -> PendingPipeline */
bool groove__ivm__runtime__pipeline__TickEvaluator__poll_pipeline(struct groove_TickEvaluator *te, const groove_NodeId *nodes, u32 nnodes, jz_hmap *pending,
    struct groove_FrameInputs frame_inputs, jz_err *err, struct groove_RecordDeltas **out);
/* persist.rs */
jz_err groove__ivm__runtime__persist__apply_persist_delta(const groove_OrderedKvStorage *storage, const groove_DurableStorage *durable, const usize *key_fields, u32 nkeys, bool unique, const struct groove_RecordDeltas *delta);
/* terminal.rs */
bool groove__ivm__runtime__terminal__TerminalDeltas__is_empty(const groove_TerminalDeltas *d);
jz_err groove__ivm__runtime__terminal__terminal_deltas_from_record_deltas(const struct groove_RecordDeltas *deltas, groove_TerminalDeltas *out);
jz_err groove__ivm__runtime__terminal__terminal_deltas_keyed_by_identity(const struct groove_RecordDeltas *deltas, const struct groove_RecordDeltas *physical, const usize *key_fields, u32 n, groove_TerminalDeltas *out);
jz_err groove__ivm__runtime__terminal__order_terminal_snapshot(struct groove_RecordDeltas *terminal, const struct groove_RecordDeltas *ordering);
/* large_values.rs: fields NULL = None */
jz_err groove__large_values__materialize_record_borrowed_attempt(groove_RecordDescriptor descriptor, const u8 *raw, u32 n, const usize *fields, u32 nfields, groove_EvaluationInputs *inputs, bool *owned, jz_bytes *out);
/* storage */
typedef struct groove_StagedWriteState groove_StagedWriteState; /* Rc<RefCell<..>>: jz_rc payload */
groove_StagedWriteState *groove__storage__StagedWriteState__default(void);
bool groove__storage__StagedWriteState__is_empty(const groove_StagedWriteState *s);
typedef struct groove_OwnedWriteOperationVec { void *ptr; u32 len; u32 cap; } groove_OwnedWriteOperationVec;
groove_OwnedWriteOperationVec groove__storage__StagedWriteState__into_operations(groove_StagedWriteState *s); /* mem::take(..).into_operations() */
enum { GROOVE_WMO_COMMITTED, GROOVE_WMO_UNCOMMITTED, GROOVE_WMO_POSSIBLY_COMMITTED };
u32 groove__storage__OrderedKvStorage__write_many_outcome(const groove_OrderedKvStorage *s, groove_OwnedWriteOperationVec operations, jz_err *error);
groove_OrderedKvStorage *groove__storage__StagedWriteOverlay__new(const groove_OrderedKvStorage *base, groove_StagedWriteState *state);
void groove__storage__StagedWriteOverlay__drop(groove_OrderedKvStorage *overlay);
groove_OwnedStorage groove__storage__OwnedStorage__clone(const groove_OwnedStorage *s);
/* chunks.rs */
void *groove__chunks__OwnedChunkProvider__with_install_observer(const void *provider, void *observer, void *failures);
void groove__chunks__OwnedChunkProvider__drop(void *provider);

#endif
