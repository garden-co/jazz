/* _ext_engine.h: mirror declarations of the types and functions that the
 * engine unit (crates/jazz/layers/engine) uses from OTHER units: groove
 * (records, ivm graph / op types / templates), jazz_model (schema, query,
 * tx, branch, policy claims), jazz_protocol and jazz_types.
 * Layouts are "roughly like the Rust fields"; names follow PORTING.md. */
#ifndef JAZZ_ENGINE_EXT_ENGINE_H
#define JAZZ_ENGINE_EXT_ENGINE_H
#include "jz_rt.h"

typedef JZ_VEC(jz_str) jazz_engine_StrVec;
typedef JZ_VEC(u32) jazz_engine_U32Vec;

/* ============================== groove::records ============================== */
typedef struct groove_RecordDescriptorData groove_RecordDescriptorData;
typedef const groove_RecordDescriptorData *groove_RecordDescriptor; /* Intern handle: Copy, eq by pointer */
typedef struct groove_ScalarEnumSchema { JZ_VEC(jz_str) variants; u64 registry_id; } groove_ScalarEnumSchema;
typedef struct groove_EnumSchema groove_EnumSchema;
typedef struct groove_LargeValueRef groove_LargeValueRef;

enum {
    GROOVE_VT_U8, GROOVE_VT_U16, GROOVE_VT_U32, GROOVE_VT_U64, GROOVE_VT_I32, GROOVE_VT_I64,
    GROOVE_VT_F64, GROOVE_VT_BOOL, GROOVE_VT_STRING, GROOVE_VT_BYTES, GROOVE_VT_INTERNAL,
    GROOVE_VT_UUID, GROOVE_VT_ENUM_TAG, GROOVE_VT_TUPLE, GROOVE_VT_ARRAY, GROOVE_VT_NULLABLE,
    GROOVE_VT_RECORD, GROOVE_VT_ENUM, GROOVE_VT_U48
};
typedef struct groove_ValueType groove_ValueType;
struct groove_ValueType {
    u8 tag;
    union {
        struct { u8 repr; u8 kind; } internal;
        groove_ScalarEnumSchema *enum_tag;
        JZ_VEC(groove_ValueType) tuple;
        groove_ValueType *inner;          /* Array / Nullable: Box<ValueType> */
        groove_RecordDescriptor *record;  /* Box<RecordDescriptor> */
        groove_EnumSchema *enum_schema;
    } u;
};
typedef groove_ValueType groove_ColumnType; /* groove::schema::ColumnType = ValueType */
typedef JZ_VEC(groove_ValueType) groove_ValueTypeVec;
void groove__records__values__ValueType__Clone__clone(groove_ValueType *dst, const groove_ValueType *src);
void groove__records__values__ValueType__drop(groove_ValueType *v);
bool groove__records__values__ValueType__PartialEq__eq(const groove_ValueType *a, const groove_ValueType *b);
int groove__records__values__ValueType__Ord__cmp(const groove_ValueType *a, const groove_ValueType *b);
void groove__records__values__ValueType__Debug__fmt(jz_str *out, const groove_ValueType *v);
const groove_ValueType *groove__records__values__ValueType__non_nullable(const groove_ValueType *v);
bool groove__records__values__ValueType__is_array_fk(const groove_ValueType *v);
bool groove__records__values__collect_by_ordered_scalar(const groove_ValueType *v);
jz_err groove__records__values__ScalarEnumSchema__discriminant(const groove_ScalarEnumSchema *s, const jz_str *variant, u8 *out);
static inline groove_ValueType groove_vt_scalar(u8 tag) { groove_ValueType t; memset(&t, 0, sizeof t); t.tag = tag; return t; }
static inline groove_ValueType groove_vt_boxed(u8 tag, groove_ValueType inner) {
    groove_ValueType t; memset(&t, 0, sizeof t); t.tag = tag;
    t.u.inner = (groove_ValueType *)jz_memdup(&inner, sizeof inner); return t;
}
static inline groove_ValueType groove_vt_clone(const groove_ValueType *v) { groove_ValueType t; groove__records__values__ValueType__Clone__clone(&t, v); return t; }

enum { GROOVE_FIELD_ID_NAME, GROOVE_FIELD_ID_SLOT, GROOVE_FIELD_ID_NAMED_SLOT };
typedef struct groove_FieldIdentity { u8 tag; jz_str name; u64 slot; } groove_FieldIdentity;
void groove__records__FieldIdentity__Clone__clone(groove_FieldIdentity *dst, const groove_FieldIdentity *src);
void groove__records__FieldIdentity__drop(groove_FieldIdentity *v);
bool groove__records__FieldIdentity__PartialEq__eq(const groove_FieldIdentity *a, const groove_FieldIdentity *b);

typedef struct groove_DescriptorField {
    bool has_name; jz_str name;
    bool has_identity; groove_FieldIdentity identity;
    groove_ValueType value_type;
} groove_DescriptorField;
typedef JZ_VEC(groove_DescriptorField) groove_DescriptorFieldVec;
void groove__records__DescriptorField__Clone__clone(groove_DescriptorField *dst, const groove_DescriptorField *src);
void groove__records__DescriptorField__drop(groove_DescriptorField *f);
bool groove__records__DescriptorField__PartialEq__eq(const groove_DescriptorField *a, const groove_DescriptorField *b);
/* logical_name(): NULL = None */
const jz_str *groove__records__DescriptorField__logical_name(const groove_DescriptorField *f);

const groove_DescriptorField *groove__records__RecordDescriptor__fields(groove_RecordDescriptor d, u32 *len);
bool groove__records__RecordDescriptor__field_index(groove_RecordDescriptor d, const char *p, u32 n, u32 *out);
/* new(fields): takes ownership of names and types */
groove_RecordDescriptor groove__records__RecordDescriptor__new(jz_str *names, groove_ValueType *types, u32 n);
groove_RecordDescriptor groove__records__RecordDescriptor__new_with_fields(groove_DescriptorField *fields, u32 n);
void groove__records__RecordDescriptor__Debug__fmt(jz_str *out, groove_RecordDescriptor d);

enum {
    GROOVE_V_U8, GROOVE_V_U16, GROOVE_V_U32, GROOVE_V_U64, GROOVE_V_F64, GROOVE_V_BOOL,
    GROOVE_V_STRING, GROOVE_V_BYTES, GROOVE_V_LARGE, GROOVE_V_UUID, GROOVE_V_ENUM_TAG,
    GROOVE_V_TUPLE, GROOVE_V_ARRAY, GROOVE_V_NULLABLE, GROOVE_V_I64, GROOVE_V_I32,
    GROOVE_V_RECORD, GROOVE_V_ENUM, GROOVE_V_U48
};
typedef struct groove_OwnedRecord { jz_bytes raw; groove_RecordDescriptor descriptor; } groove_OwnedRecord;
typedef struct groove_EnumValue { u32 tag; groove_OwnedRecord record; } groove_EnumValue;
typedef struct groove_Value groove_Value;
typedef JZ_VEC(groove_Value) groove_ValueVec;
struct groove_Value {
    u8 tag;
    union {
        u8 u8_; u16 u16_; u32 u32_; u64 u64_; f64 f64_; bool b; i64 i64_; i32 i32_;
        jz_str s; jz_bytes bytes; groove_LargeValueRef *large; jz_uuid uuid; u8 enum_tag;
        groove_ValueVec items;
        groove_Value *nullable; /* NULL = None */
        groove_OwnedRecord record;
        groove_EnumValue enum_;
    } u;
};
void groove__records__values__Value__Clone__clone(groove_Value *dst, const groove_Value *src);
void groove__records__values__Value__drop(groove_Value *v);
void groove__records__values__Value__drop_v(void *v);
void groove__records__values__Value__clone_v(void *dst, const void *src);
bool groove__records__values__Value__PartialEq__eq(const groove_Value *a, const groove_Value *b);
bool groove__records__values__Value__eq_v(const void *a, const void *b);
void groove__records__values__Value__Debug__fmt(jz_str *out, const groove_Value *v);
jz_err groove__records__values__Value__Deserialize__postcard(jz_pc_r *r, groove_Value *out);
jz_err groove__records__RecordDescriptor__create(groove_RecordDescriptor d, const groove_Value *values, u32 n, jz_bytes *out);

/* ============================== groove::ivm op types ============================== */
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
        u8 u8_; u16 u16_; u32 u32_; u64 u64_; i64 i64_; i32 i32_; f64 f64_; bool b;
        jz_str s; jz_bytes bytes; jz_uuid uuid;
        groove_LiteralVec items;
        groove_LiteralValue *nullable;
        struct { jz_bytes descriptor; jz_bytes raw; } native;
    } u;
};
void groove__ivm__op_types__LiteralValue__Clone__clone(groove_LiteralValue *dst, const groove_LiteralValue *src);
void groove__ivm__op_types__LiteralValue__drop(groove_LiteralValue *v);
bool groove__ivm__op_types__LiteralValue__PartialEq__eq(const groove_LiteralValue *a, const groove_LiteralValue *b);
void groove__ivm__op_types__LiteralValue__Debug__fmt(jz_str *out, const groove_LiteralValue *v);
int groove__ivm__op_types__LiteralValue__PartialOrd__partial_cmp(const groove_LiteralValue *a, const groove_LiteralValue *b); /* -1/0/1; 2 = None */
void groove__ivm__op_types__LiteralValue__From_Value__from(groove_LiteralValue *out, groove_Value *value);
/* value_type(): false = None */
bool groove__ivm__op_types__LiteralValue__value_type(const groove_LiteralValue *v, groove_ValueType *out);

enum { GROOVE_FIELDREF_NAME, GROOVE_FIELDREF_STORED_NAME, GROOVE_FIELDREF_RESOLVED };
typedef struct groove_FieldRef { u8 tag; jz_str name; u32 index; } groove_FieldRef;
typedef JZ_VEC(groove_FieldRef) groove_FieldRefVec;
void groove__ivm__graph__FieldRef__Clone__clone(groove_FieldRef *dst, const groove_FieldRef *src);
void groove__ivm__graph__FieldRef__drop(groove_FieldRef *v);
bool groove__ivm__graph__FieldRef__PartialEq__eq(const groove_FieldRef *a, const groove_FieldRef *b);
void groove__ivm__graph__FieldRef__Debug__fmt(jz_str *out, const groove_FieldRef *v);
groove_FieldRef groove__ivm__graph__FieldRef__name(jz_str name);
groove_FieldRef groove__ivm__graph__FieldRef__stored_name(jz_str name);
groove_FieldRef groove__ivm__graph__FieldRef__resolved(u32 index);
jz_str groove__ivm__graph__FieldRef__display_name(const groove_FieldRef *f);

enum {
    GROOVE_PEXPR_TEMPLATE_ARGUMENT, GROOVE_PEXPR_FIELD, GROOVE_PEXPR_RECORD_FIELD, GROOVE_PEXPR_LITERAL,
    GROOVE_PEXPR_TYPED_LITERAL, GROOVE_PEXPR_NULL, GROOVE_PEXPR_NULLABLE, GROOVE_PEXPR_NULLABLE_FLAT,
    GROOVE_PEXPR_ENUM_TAG_REMAP, GROOVE_PEXPR_ENUM_REMAP, GROOVE_PEXPR_RECURSIVE_ENUM_REMAP
};
typedef struct groove_ProjectExpr {
    u8 tag;
    union {
        struct { u32 slot; groove_ValueType value_type; } template_argument;
        groove_FieldRef field; /* Field / Nullable / NullableFlat */
        struct { groove_FieldRef source; jazz_engine_StrVec path; } record_field;
        groove_LiteralValue literal;
        struct { groove_LiteralValue value; groove_ValueType value_type; } typed_literal;
        groove_ValueType null_type;
        struct { groove_FieldRef source; void *rest; } remap; /* EnumTagRemap / EnumRemap / RecursiveEnumRemap */
    } u;
} groove_ProjectExpr;
typedef struct groove_ProjectField {
    groove_ProjectExpr expression;
    jz_str output_name;
    groove_FieldIdentity output_identity;
} groove_ProjectField;
typedef JZ_VEC(groove_ProjectField) groove_ProjectFieldVec;
void groove__ivm__graph__ProjectExpr__drop(groove_ProjectExpr *e);
void groove__ivm__graph__ProjectField__Clone__clone(groove_ProjectField *dst, const groove_ProjectField *src);
void groove__ivm__graph__ProjectField__drop(groove_ProjectField *f);
void groove__ivm__graph__ProjectField__drop_v(void *f);
bool groove__ivm__graph__ProjectField__PartialEq__eq(const groove_ProjectField *a, const groove_ProjectField *b);
bool groove__ivm__graph__ProjectExpr__PartialEq__eq(const groove_ProjectExpr *a, const groove_ProjectExpr *b);
/* ProjectField::source(): NULL = None */
const groove_FieldRef *groove__ivm__graph__ProjectField__source(const groove_ProjectField *f);
groove_ProjectField groove__ivm__graph__ProjectField__named(jz_str name);
groove_ProjectField groove__ivm__graph__ProjectField__named_with_identity(jz_str name, groove_FieldIdentity id);
groove_ProjectField groove__ivm__graph__ProjectField__renamed(jz_str source, jz_str output);
groove_ProjectField groove__ivm__graph__ProjectField__renamed_resolved(u32 source_idx, jz_str output);
groove_ProjectField groove__ivm__graph__ProjectField__renamed_with_identity(jz_str source, jz_str output, groove_FieldIdentity id);
groove_ProjectField groove__ivm__graph__ProjectField__literal(jz_str output, groove_LiteralValue value);
groove_ProjectField groove__ivm__graph__ProjectField__literal_typed(jz_str output, groove_LiteralValue value, groove_ValueType t);
groove_ProjectField groove__ivm__graph__ProjectField__null_typed(jz_str output, groove_ValueType t);
groove_ProjectField groove__ivm__graph__ProjectField__nullable(jz_str source, jz_str output);
groove_ProjectField groove__ivm__graph__ProjectField__nullable_flat(jz_str source, jz_str output);

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
        struct { u32 slot; jazz_engine_StrVec fields; } template_argument;
        struct { jz_str field; groove_LiteralValue value; } lit;
        struct { jz_str field; jz_str other; } fields;
        struct { jz_str field; } null;
        struct { jz_str field; u32 case_tag; groove_PredicateExpr *payload; } enum_match;
        groove_PredicateVec children;
    } u;
};
void groove__ivm__op_types__PredicateExpr__Clone__clone(groove_PredicateExpr *dst, const groove_PredicateExpr *src);
void groove__ivm__op_types__PredicateExpr__drop(groove_PredicateExpr *v);
void groove__ivm__op_types__PredicateExpr__drop_v(void *v);
void groove__ivm__op_types__PredicateExpr__clone_v(void *dst, const void *src);
bool groove__ivm__op_types__PredicateExpr__PartialEq__eq(const groove_PredicateExpr *a, const groove_PredicateExpr *b);
void groove__ivm__op_types__PredicateExpr__Debug__fmt(jz_str *out, const groove_PredicateExpr *v);
void groove__ivm__op_types__PredicateExpr__canonicalize(groove_PredicateExpr *out, groove_PredicateExpr self);
enum { GROOVE_PK_EQ, GROOVE_PK_NEQ, GROOVE_PK_GT, GROOVE_PK_GT_EQ, GROOVE_PK_LT, GROOVE_PK_LT_EQ };
groove_PredicateExpr groove__ivm__op_types__PredicateExpr__from_field_literal(u8 kind, jz_str field, groove_LiteralValue value);
u8 groove__ivm__op_types__PredicateKind__reversed(u8 kind);

enum { GROOVE_CMP_EXACT, GROOVE_CMP_POLICY }; /* ValueComparison */
enum { GROOVE_TOPBY_FINITE, GROOVE_TOPBY_UNBOUNDED };
typedef struct groove_TopByLimit { u8 tag; u64 n; } groove_TopByLimit;
enum { GROOVE_DIR_ASC, GROOVE_DIR_DESC };
typedef struct groove_TopByOrder { groove_FieldRef field; u8 direction; } groove_TopByOrder;
typedef JZ_VEC(groove_TopByOrder) groove_TopByOrderVec;
groove_TopByOrder groove__ivm__graph__TopByOrder__asc(jz_str field);
groove_TopByOrder groove__ivm__graph__TopByOrder__desc(jz_str field);
void groove__ivm__graph__TopByOrder__drop_v(void *o);
void groove__ivm__graph__TopByOrder__clone_v(void *dst, const void *src);

enum { GROOVE_AGG_COUNT, GROOVE_AGG_SUM, GROOVE_AGG_AVG, GROOVE_AGG_MIN, GROOVE_AGG_MAX };
typedef struct groove_PlanExpr { u8 tag; jz_str field; void *rest; } groove_PlanExpr;
groove_PlanExpr groove__ivm__op_types__PlanExpr__field(jz_str name);
typedef struct groove_AggregateExpr {
    u8 function;
    bool has_expression; groove_PlanExpr expression;
    bool distinct;
    bool has_output_name; jz_str output_name;
    bool has_output_identity; groove_FieldIdentity output_identity;
} groove_AggregateExpr;
typedef JZ_VEC(groove_AggregateExpr) groove_AggregateExprVec;
void groove__ivm__op_types__AggregateExpr__drop_v(void *a);

typedef struct groove_CollectByField { groove_FieldRef field; jz_str output_name; bool unwrap_nullable; } groove_CollectByField;
typedef JZ_VEC(groove_CollectByField) groove_CollectByFieldVec;
groove_CollectByField groove__ivm__graph__CollectByField__renamed(jz_str field, jz_str output);
groove_CollectByField groove__ivm__graph__CollectByField__renamed_unwrap_nullable(jz_str field, jz_str output);
void groove__ivm__graph__CollectByField__drop_v(void *f);
typedef struct groove_CollectBySlotBuilder groove_CollectBySlotBuilder;
typedef JZ_VEC(groove_CollectBySlotBuilder) groove_CollectBySlotBuilderVec;
struct groove_CollectBySlotBuilder {
    groove_FieldRefVec group_cols, owner_key_cols;
    groove_CollectByFieldVec child_fields;
    jz_str collection_field;
    groove_CollectBySlotBuilderVec slots;
    groove_TopByOrderVec order_cols;
    groove_FieldRefVec tie_cols;
    bool has_presence_col; groove_FieldRef presence_col;
    bool has_reference_array_col; groove_FieldRef reference_array_col;
    bool reference_order;
    u64 offset; groove_TopByLimit limit;
};
groove_CollectBySlotBuilder groove__ivm__graph__CollectBySlotBuilder__new(jazz_engine_StrVec group_cols, groove_CollectByFieldVec child_fields,
    jz_str collection_field, groove_CollectBySlotBuilderVec slots, groove_TopByOrderVec order_cols, jazz_engine_StrVec tie_cols,
    u64 offset, groove_TopByLimit limit);
void groove__ivm__graph__CollectBySlotBuilder__with_presence_col(groove_CollectBySlotBuilder *b, jz_str presence_col);
void groove__ivm__graph__CollectBySlotBuilder__with_owner_key_cols(groove_CollectBySlotBuilder *b, jazz_engine_StrVec cols);
void groove__ivm__graph__CollectBySlotBuilder__drop_v(void *b);
enum { GROOVE_MAX_COLLECT_BY_TREE_DEPTH = 16 };
enum { GROOVE_COLLECT_MODE_COLLECT, GROOVE_COLLECT_MODE_ROOT, GROOVE_COLLECT_MODE_EXPAND };
typedef struct groove_CollectByBuilder {
    u8 mode;
    groove_FieldRefVec group_cols;
    groove_CollectByFieldVec parent_fields, child_fields;
    jz_str collection_field;
    groove_CollectBySlotBuilderVec slots;
    groove_CollectByFieldVec tuple_fields;
    groove_FieldRefVec occurrence_id_cols;
    groove_TopByOrderVec order_cols;
    groove_FieldRefVec tie_cols;
    u64 offset; groove_TopByLimit limit;
} groove_CollectByBuilder;

/* ============================== groove::ivm::graph::GraphBuilder ==============================
 * Arc<GraphBuilder> and owned GraphBuilder values are both represented as a jz_rc payload
 * pointer. Builder constructors consume (take ownership of) their GraphBuilder arguments. */
typedef struct groove_GraphBuilder groove_GraphBuilder;
typedef JZ_VEC(groove_GraphBuilder *) groove_GraphBuilderVec;
typedef struct groove_TypedGraphTemplate groove_TypedGraphTemplate;
typedef struct groove_TemplateScalarArgument groove_TemplateScalarArgument;
typedef struct groove_StaticScanSpec groove_StaticScanSpec;
typedef struct groove_InputSourceId { u64 runtime_namespace, local; } groove_InputSourceId;
typedef enum {
    GROOVE_GB_TYPED_TEMPLATE, GROOVE_GB_TEMPLATE_INPUT, GROOVE_GB_TABLE, GROOVE_GB_INLINE_RECORDS, GROOVE_GB_INPUT_SOURCE,
    GROOVE_GB_INDEX, GROOVE_GB_FRONTIER_SOURCE, GROOVE_GB_BINDING_SOURCE, GROOVE_GB_RECURSIVE, GROOVE_GB_RECURSIVE_STEP_WITNESS,
    GROOVE_GB_FILTER, GROOVE_GB_UNWRAP_NULLABLE, GROOVE_GB_UNNEST, GROOVE_GB_VARIANT_PROJECT, GROOVE_GB_PROJECT,
    GROOVE_GB_STREAMING_CHECKSUM, GROOVE_GB_UNION, GROOVE_GB_JOIN, GROOVE_GB_SEMI_JOIN, GROOVE_GB_ANTI_JOIN, GROOVE_GB_LOOKUP,
    GROOVE_GB_ARG_MAX_BY, GROOVE_GB_ARG_MIN_BY, GROOVE_GB_TOP_BY, GROOVE_GB_COLLECT_BY, GROOVE_GB_AGGREGATE,
} groove_GraphBuilderTag;
struct groove_GraphBuilder {
    u8 tag;
    union {
        struct { groove_TypedGraphTemplate *program; groove_GraphBuilderVec inputs; groove_PredicateVec predicates; groove_TemplateScalarArgument *scalars; u32 scalars_len; } typed_template;
        struct { u32 slot; groove_RecordDescriptor output; groove_GraphBuilder *input; } template_input;
        struct { jz_str table; groove_StaticScanSpec *scan; bool has_variant_projection; jz_str variant_projection; } table;
        struct { groove_RecordDescriptor output; JZ_VEC(jz_bytes) records; } inline_records;
        struct { groove_InputSourceId id; groove_RecordDescriptor output; } input_source;
        struct { jz_str table, index; void *rest; } index;
        struct { jz_str binding; groove_RecordDescriptor output; } frontier_source;
        struct { jz_str shape; groove_RecordDescriptor output; } binding_source;
        struct { groove_GraphBuilder *seed, *step, *step_witness; jz_str frontier; u32 max_iters; bool truncate_at_max_iters; } recursive;
        struct { groove_GraphBuilder *recursive; } recursive_step_witness;
        struct { groove_GraphBuilder *input; groove_PredicateExpr predicate; u8 comparison; } filter;
        struct { groove_GraphBuilder *input; groove_FieldRef field; } unwrap_nullable;
        struct { groove_GraphBuilder *input; groove_FieldRef array_field; jz_str element_field; } unnest;
        struct { groove_GraphBuilder *input; groove_FieldRef field; jz_str case_; } variant_project;
        struct { groove_GraphBuilder *input; groove_ProjectFieldVec fields; } project;
        struct { groove_GraphBuilder *input; groove_FieldRef field; jz_str output_field; u32 window_bytes, max_bytes_per_turn; } streaming_checksum;
        struct { groove_GraphBuilderVec inputs; } union_;
        struct { groove_GraphBuilder *left, *right; groove_FieldRefVec left_on, right_on; u8 comparison; } join; /* Join/SemiJoin/AntiJoin */
        struct { groove_GraphBuilder *keys; groove_FieldRefVec keys_on; jz_str table; groove_StaticScanSpec *scan; bool has_variant_projection; jz_str variant_projection; } lookup;
        struct { groove_GraphBuilder *input; groove_FieldRefVec group_cols, order_cols; } arg_by; /* ArgMaxBy / ArgMinBy */
        struct { groove_GraphBuilder *input; groove_FieldRefVec group_cols; groove_TopByOrderVec order_cols; groove_FieldRefVec tie_cols; u64 offset; groove_TopByLimit limit; } top_by;
        struct { groove_GraphBuilder *input; groove_CollectByBuilder *collect; } collect_by;
        struct { groove_GraphBuilder *input; groove_FieldRefVec group_cols; groove_AggregateExprVec aggregates; } aggregate;
    } u;
};
static inline groove_GraphBuilder *groove_gb_clone(groove_GraphBuilder *g) { return (groove_GraphBuilder *)jz_rc_clone(g); }
static inline void groove_gb_drop(groove_GraphBuilder *g) { if (g) jz_rc_release(g); }
void groove__ivm__graph__GraphBuilder__drop_v(void *g); /* drops a groove_GraphBuilder* slot */
void groove__ivm__graph__GraphBuilder__drop_in_place(void *node); /* jz_rc drop fn of a GraphBuilder payload (enum-literal construction) */
void groove__ivm__graph__GraphBuilder__clone_v(void *dst, const void *src);
bool groove__ivm__graph__GraphBuilder__PartialEq__eq(const groove_GraphBuilder *a, const groove_GraphBuilder *b);
void groove__ivm__graph__GraphBuilder__Debug__fmt(jz_str *out, const groove_GraphBuilder *g);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__table(jz_str table);
jz_err groove__ivm__graph__GraphBuilder__values(groove_RecordDescriptor output, const groove_ValueVec *rows, u32 nrows, groove_GraphBuilder **out);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__inline_records(groove_RecordDescriptor output, JZ_VEC(jz_bytes) records);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__frontier_source(jz_str binding, groove_RecordDescriptor output);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__binding_source(jz_str shape, groove_RecordDescriptor output);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__recursive_with_step_witness(groove_GraphBuilder *seed, groove_GraphBuilder *step,
    groove_GraphBuilder *step_witness, jz_str frontier, u32 max_iters, bool truncate_at_max_iters);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__recursive_step_witness(groove_GraphBuilder *recursive);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__union(groove_GraphBuilderVec inputs);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__join(groove_GraphBuilder *l, groove_GraphBuilder *r, jazz_engine_StrVec left_on, jazz_engine_StrVec right_on);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__policy_join(groove_GraphBuilder *l, groove_GraphBuilder *r, jazz_engine_StrVec left_on, jazz_engine_StrVec right_on);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__semi_join(groove_GraphBuilder *l, groove_GraphBuilder *r, jazz_engine_StrVec left_on, jazz_engine_StrVec right_on);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__anti_join(groove_GraphBuilder *l, groove_GraphBuilder *r, jazz_engine_StrVec left_on, jazz_engine_StrVec right_on);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__arg_max_by(groove_GraphBuilder *in, jazz_engine_StrVec group_cols, jazz_engine_StrVec order_cols);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__arg_min_by(groove_GraphBuilder *in, jazz_engine_StrVec group_cols, jazz_engine_StrVec order_cols);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__top_by(groove_GraphBuilder *in, jazz_engine_StrVec group_cols, groove_TopByOrderVec order_cols,
    jazz_engine_StrVec tie_cols, u64 offset, groove_TopByLimit limit);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__collect_by_tree_ordered(groove_GraphBuilder *in, jazz_engine_StrVec group_cols,
    groove_CollectByFieldVec parent_fields, groove_CollectBySlotBuilderVec slots, groove_TopByOrderVec order_cols,
    jazz_engine_StrVec tie_cols, u64 offset, groove_TopByLimit limit);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__collect_root_ordered(groove_GraphBuilder *in, jazz_engine_StrVec group_cols,
    groove_CollectByFieldVec parent_fields, groove_TopByOrderVec order_cols, jazz_engine_StrVec tie_cols, u64 offset, groove_TopByLimit limit);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__aggregate(groove_GraphBuilder *in, jazz_engine_StrVec group_cols, groove_AggregateExprVec aggregates);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__filter(groove_GraphBuilder *self, groove_PredicateExpr predicate);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__policy_filter(groove_GraphBuilder *self, groove_PredicateExpr predicate);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__unwrap_nullable(groove_GraphBuilder *self, jz_str field);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__unnest(groove_GraphBuilder *self, jz_str array_field, jz_str element_field);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__project(groove_GraphBuilder *self, jazz_engine_StrVec fields);
groove_GraphBuilder *groove__ivm__graph__GraphBuilder__project_fields(groove_GraphBuilder *self, groove_ProjectFieldVec fields);
groove_GraphBuilder *groove__ivm__graph__join_chains__GraphBuilder__narrow_projected_join_chain(groove_GraphBuilder *self);

/* ============================== groove::ivm templates ============================== */
typedef struct groove_TemplateGraphInput { groove_GraphBuilder *graph; groove_RecordDescriptor output; } groove_TemplateGraphInput;
typedef JZ_VEC(groove_TemplateGraphInput) groove_TemplateGraphInputVec;
void groove__ivm__template__TemplateGraphInput__drop_v(void *p);
jz_err groove__ivm__template__TemplateGraphInput__new(groove_GraphBuilder *graph, groove_RecordDescriptor output, groove_TemplateGraphInput *out);
groove_RecordDescriptor groove__ivm__template__TypedGraphTemplate__output_descriptor(const groove_TypedGraphTemplate *t);
typedef struct groove_TemplateScalarArgumentVec { groove_TemplateScalarArgument *ptr; u32 len; u32 cap; } groove_TemplateScalarArgumentVec;
jz_err groove__ivm__template__arguments__TemplateScalarArgument__new(groove_Value value, groove_ValueType value_type, groove_TemplateScalarArgumentVec *push_into);
void groove__ivm__template__TemplateScalarArgumentVec__drop(groove_TemplateScalarArgumentVec *v);
/* describe callback for match/split_template_sources */
typedef jz_err (*groove_TemplateDescribeFn)(groove_GraphBuilder *graph, groove_TemplateGraphInput *out, void *ctx);
/* Result<Option<Vec<TemplateGraphInput>>, E>: *matched = false is None */
jz_err groove__ivm__template__sources__match_template_sources(groove_GraphBuilder *const *graphs, u32 ngraphs, groove_GraphBuilder *const *blueprints,
    u32 nblueprints, groove_TemplateDescribeFn describe, void *ctx, bool *matched, groove_TemplateGraphInputVec *out);
jz_err groove__ivm__template__sources__split_template_sources(groove_GraphBuilder *const *graphs, u32 ngraphs, groove_TemplateDescribeFn describe,
    void *ctx, groove_GraphBuilderVec *blueprints, groove_TemplateGraphInputVec *inputs);
jz_err groove__ivm__template__bind_template_program(groove_GraphBuilder *const *graphs, u32 ngraphs, const groove_TemplateGraphInput *inputs,
    u32 ninputs, const groove_PredicateExpr *predicates, u32 npredicates, groove_TemplateScalarArgumentVec scalars, groove_GraphBuilderVec *out);
typedef struct groove_TypedGraphTemplateCache groove_TypedGraphTemplateCache;
groove_TypedGraphTemplateCache *groove__ivm__runtime__typed_template__TypedGraphTemplateCache__default(void);
void groove__ivm__runtime__typed_template__TypedGraphTemplateCache__clear(groove_TypedGraphTemplateCache *c);
void groove__ivm__runtime__typed_template__TypedGraphTemplateCache__drop(groove_TypedGraphTemplateCache *c);
jz_err groove__ivm__runtime__typed_template__TypedGraphTemplateCache__compile(groove_TypedGraphTemplateCache *c, groove_GraphBuilder *const *graphs,
    u32 n, groove_GraphBuilderVec *out);
void groove__ivm__runtime__IvmRuntimeError__Display__fmt(jz_str *out, const jz_err *e);

/* ============================== jazz_types ============================== */
typedef struct jazz_types_SchemaVersionId { jz_uuid u; } jazz_types_SchemaVersionId;
typedef struct jazz_types_SchemaFamilyId { jz_uuid u; } jazz_types_SchemaFamilyId;
typedef struct jazz_types_PhysicalColumnId { jz_uuid u; } jazz_types_PhysicalColumnId;
typedef struct jazz_types_GlobalTime { u64 t; } jazz_types_GlobalTime;
/* AuthorSubject: Copy enum; opaque 32 byte payload */
typedef struct jazz_types_AuthorSubject { u8 tag; u8 b[32]; } jazz_types_AuthorSubject;
/* AuthorSubject::metadata_path(path) -> Option<(root, AuthorSubjectPath)>; false = None */
typedef struct jazz_types_AuthorMetadataPath { const char *root; u8 path; } jazz_types_AuthorMetadataPath;
bool jazz_types__ids__AuthorSubject__metadata_path(jz_slice path, jazz_types_AuthorMetadataPath *out);
void jazz_types__ids__AuthorSubject__to_value(const jazz_types_AuthorSubject *s, groove_Value *out);
void jazz_types__ids__AuthorSubject__Debug__fmt(jz_str *out, const jazz_types_AuthorSubject *s);
bool jazz_types__ids__AuthorSubject__PartialEq__eq(const jazz_types_AuthorSubject *a, const jazz_types_AuthorSubject *b);
bool jazz_types__debug_env__covered_input_trace(void);
bool jazz_types__debug_env__query_template_trace(void);
void jazz_types__ids__uuid_newtype__Debug__fmt(jz_str *out, const char *name, const jz_uuid *u);

/* ============================== jazz_model ============================== */
typedef struct jazz_model_ShapeId { jz_uuid u; } jazz_model_ShapeId;
typedef struct jazz_model_BindingId { jz_uuid u; } jazz_model_BindingId;
typedef struct jazz_model_TxId { u64 time; jz_uuid node; } jazz_model_TxId;
typedef enum { JAZZ_DUR_NONE = 0, JAZZ_DUR_LOCAL = 1, JAZZ_DUR_GLOBAL = 2 } jazz_model_DurabilityTier;
void jazz_model__tx__TxId__Debug__fmt(jz_str *out, const jazz_model_TxId *t);
typedef struct jazz_model_Snapshot jazz_model_Snapshot; /* opaque, boxed */
void jazz_model__tx__Snapshot__Clone__clone(jazz_model_Snapshot **dst, const jazz_model_Snapshot *src);
void jazz_model__tx__Snapshot__drop(jazz_model_Snapshot *s);
bool jazz_model__tx__Snapshot__PartialEq__eq(const jazz_model_Snapshot *a, const jazz_model_Snapshot *b);
void jazz_model__tx__Snapshot__Debug__fmt(jz_str *out, const jazz_model_Snapshot *s);
typedef struct jazz_model_OpenTransactionId { jz_uuid u; } jazz_model_OpenTransactionId;
void jazz_model__model__transaction__OpenTransactionId__Debug__fmt(jz_str *out, const jazz_model_OpenTransactionId *t);

/* RecursionBound { Fixpoint, MaxDepth(usize) } */
enum { JAZZ_MODEL_RECURSION_FIXPOINT, JAZZ_MODEL_RECURSION_MAX_DEPTH };
typedef struct jazz_model_RecursionBound { u8 tag; u32 max_depth; } jazz_model_RecursionBound;

/* RelationProjectColumn { alias, expr } */
enum { JAZZ_MODEL_RPE_ROW_ID, JAZZ_MODEL_RPE_COLUMN };
enum { JAZZ_MODEL_RRI_CURRENT, JAZZ_MODEL_RRI_OUTER, JAZZ_MODEL_RRI_FRONTIER };
typedef struct jazz_model_RelationProjectColumn {
    jz_str alias;
    u8 expr_tag; u8 row_id; jz_str column_scope; jz_str column;
} jazz_model_RelationProjectColumn;
typedef JZ_VEC(jazz_model_RelationProjectColumn) jazz_model_RelationProjectColumnVec;
void jazz_model__query__RelationProjectColumn__Clone__clone(jazz_model_RelationProjectColumn *dst, const jazz_model_RelationProjectColumn *src);
void jazz_model__query__RelationProjectColumn__drop(jazz_model_RelationProjectColumn *c);
bool jazz_model__query__RelationProjectColumn__PartialEq__eq(const jazz_model_RelationProjectColumn *a, const jazz_model_RelationProjectColumn *b);
int jazz_model__query__RelationProjectColumn__Ord__cmp(const jazz_model_RelationProjectColumn *a, const jazz_model_RelationProjectColumn *b);
void jazz_model__query__RelationProjectColumn__Debug__fmt(jz_str *out, const jazz_model_RelationProjectColumn *c);
jz_str jazz_model__query__compose_union_arm_path(const jz_str *parent, const jz_str *label);
/* author_claim_path_key(path) -> Option<String>: false = None */
bool jazz_model__query__author_claim_path_key(const jz_str *segments, u32 n, jz_str *out);

/* schema */
typedef struct jazz_model_ColumnSchema {
    jz_str name;
    groove_ValueType column_type;
    u8 large_value_kind;
    bool has_default; groove_Value default_;
} jazz_model_ColumnSchema;
typedef struct jazz_model_TableSchema {
    jz_str name;
    JZ_VEC(jazz_model_ColumnSchema) columns;
    void *rest;
} jazz_model_TableSchema;
void jazz_model__schema__TableSchema__Clone__clone(jazz_model_TableSchema *dst, const jazz_model_TableSchema *src);
void jazz_model__schema__TableSchema__drop(jazz_model_TableSchema *t);
bool jazz_model__schema__TableSchema__PartialEq__eq(const jazz_model_TableSchema *a, const jazz_model_TableSchema *b);
void jazz_model__schema__TableSchema__Debug__fmt(jz_str *out, const jazz_model_TableSchema *t);
jz_str jazz_model__schema__app_storage_column_name(const jz_str *column);
extern const char jazz_model__schema__APP_COLUMN_PREFIX[]; /* "_app_" */
#define JAZZ_APP_COLUMN_PREFIX "_app_"

/* model::policy_claims */
typedef struct jazz_model_PolicyClaims jazz_model_PolicyClaims; /* opaque json-like claims */
/* author_policy_claims(author) -> BTreeMap<String, Value>; the callee initializes *out */
void jazz_model__model__policy_claims__author_policy_claims(jazz_types_AuthorSubject author, jz_btree *out);
/* policy_claim_at_path(claims: &BTreeMap<String, Value>, path) -> Option<Value>; false = None */
bool jazz_model__model__policy_claims__policy_claim_at_path(const jz_btree *claims, const jz_str *segments, u32 n, groove_Value *out);

/* branch (protocol re-exports) */
typedef struct jazz_model_BranchKey jazz_model_BranchKey;
typedef struct jazz_model_SnapshotRef jazz_model_SnapshotRef;
void jazz_model__model__branch__BranchKey__Clone__clone(jazz_model_BranchKey **dst, const jazz_model_BranchKey *src);
void jazz_model__model__branch__BranchKey__drop(jazz_model_BranchKey *k);
bool jazz_model__model__branch__BranchKey__PartialEq__eq(const jazz_model_BranchKey *a, const jazz_model_BranchKey *b);
void jazz_model__model__branch__BranchKey__Debug__fmt(jz_str *out, const jazz_model_BranchKey *k);
void jazz_model__model__branch__SnapshotRef__Clone__clone(jazz_model_SnapshotRef **dst, const jazz_model_SnapshotRef *src);
void jazz_model__model__branch__SnapshotRef__drop(jazz_model_SnapshotRef *s);
bool jazz_model__model__branch__SnapshotRef__PartialEq__eq(const jazz_model_SnapshotRef *a, const jazz_model_SnapshotRef *b);
void jazz_model__model__branch__SnapshotRef__Debug__fmt(jz_str *out, const jazz_model_SnapshotRef *s);

/* ============================== jazz_protocol ============================== */
/* ProgramSourceRole: same variants as engine SourceRole */
typedef struct jazz_protocol_ProgramSourceId { const jz_str *table; JZ_VEC(u8) path; } jazz_protocol_ProgramSourceId; /* table: Intern<String> */
typedef struct jazz_protocol_BindingViewKey { jz_uuid shape; jz_uuid binding; } jazz_protocol_BindingViewKey;
typedef struct jazz_protocol_AuthorityResultKey { jz_uuid a; jz_uuid b; u64 c; } jazz_protocol_AuthorityResultKey;
void jazz_protocol__protocol__BindingViewKey__Debug__fmt(jz_str *out, const jazz_protocol_BindingViewKey *k);
void jazz_protocol__protocol__AuthorityResultKey__Debug__fmt(jz_str *out, const jazz_protocol_AuthorityResultKey *k);
const jz_str *jazz_protocol__intern_str(const jz_str *s); /* Intern<String>::new */

#endif
