/* groove::records::values + the core record types (crates/groove/src/records/values.rs,
 * plus the type definitions of records/mod.rs that values.rs is mutually recursive with).
 *
 * This is the canonical C definition of groove's Value / ValueType algebra. Other units
 * mirror these layouts in their _ext_*.h headers.
 *
 * Layout notes:
 *  - RecordDescriptor is Intern<RecordDescriptorData>: a Copy pointer to the interned data,
 *    compared and hashed by pointer.
 *  - usize is u32 on wasm32, so lengths and indices are u32.
 *  - Box<T> is a heap pointer, except Box<RecordDescriptor>, which is stored as the interned
 *    pointer itself (the Box only wraps an 8-byte handle).
 */
#ifndef GROOVE_RECORDS_VALUES_H
#define GROOVE_RECORDS_VALUES_H

#include "jz_rt.h"

/* ---------------------------------------------------------------- errors */
/* records::Error discriminants (thiserror). LargeValue(transparent) carries the
 * large_values::Error code unchanged (GROOVE_LV_E_*). */
enum {
    GROOVE_REC_E_BASE = 0x1000,
    GROOVE_REC_E_ArityMismatch,
    GROOVE_REC_E_FieldNotFound,
    GROOVE_REC_E_FieldIndexOutOfBounds,
    GROOVE_REC_E_InvalidBool,
    GROOVE_REC_E_InvalidNullFlag,
    GROOVE_REC_E_EnumTooManyVariants,
    GROOVE_REC_E_DuplicateEnumVariantName,
    GROOVE_REC_E_InvalidEnumDiscriminant,
    GROOVE_REC_E_UnknownEnumVariant,
    GROOVE_REC_E_InvalidOffset,
    GROOVE_REC_E_InvalidSchemaVersionHeader,
    GROOVE_REC_E_InvalidEnumHeader,
    GROOVE_REC_E_EnumTooManyCases,
    GROOVE_REC_E_DuplicateEnumCaseName,
    GROOVE_REC_E_UnknownEnumCase,
    GROOVE_REC_E_UnknownEnumTag,
    GROOVE_REC_E_VariantTagOutOfRange,
    GROOVE_REC_E_NonCanonicalRecord,
    GROOVE_REC_E_InvalidTupleMember,
    GROOVE_REC_E_InvalidUtf8,
    GROOVE_REC_E_InvalidF64NaN,
    GROOVE_REC_E_U48OutOfRange,
    GROOVE_REC_E_LengthOverflow,
    GROOVE_REC_E_TypeMismatch,
    GROOVE_REC_E_ProjectDuplicateTarget,
    GROOVE_REC_E_ProjectMissingTarget,
    GROOVE_REC_E_ProjectTypeMismatch,
    GROOVE_REC_E_ProjectSourceDescriptorMismatch,
    GROOVE_REC_E_UnexpectedEof,
    GROOVE_REC_E_END
};

/* ---------------------------------------------------------------- core types */
typedef struct groove_RecordDescriptorData groove_RecordDescriptorData;
typedef const groove_RecordDescriptorData *groove_RecordDescriptor;
typedef struct groove_LargeValueRef groove_LargeValueRef; /* large_values.h */

/* large_values::LargeValueKind (serde order) */
typedef enum { GROOVE_LVK_BYTES = 0, GROOVE_LVK_STRING = 1, GROOVE_LVK_JSON = 2 } groove_LargeValueKind;

/* InternalValueType(InternalValueTypeRepr) */
enum { GROOVE_IVT_RAW_STRING = 0, GROOVE_IVT_RAW_BYTES = 1, GROOVE_IVT_STORED_SCALAR = 2 };
typedef struct groove_InternalValueType { u8 repr; u8 kind; /* groove_LargeValueKind for STORED_SCALAR */ } groove_InternalValueType;

typedef JZ_VEC(jz_str) groove_StrVec;

typedef struct groove_ScalarEnumSchema {
    u64 registry_id;
    jz_str name;
    groove_StrVec variants;
} groove_ScalarEnumSchema;

typedef struct groove_EnumCase { jz_str name; groove_RecordDescriptor payload; } groove_EnumCase;
typedef JZ_VEC(groove_EnumCase) groove_EnumCaseVec;
typedef struct groove_EnumSchema {
    u64 registry_id;
    jz_str name;
    groove_EnumCaseVec cases;
} groove_EnumSchema;

/* ValueType discriminants (serde variant order) */
enum {
    GROOVE_VT_U8, GROOVE_VT_U16, GROOVE_VT_U32, GROOVE_VT_U64, GROOVE_VT_I32, GROOVE_VT_I64,
    GROOVE_VT_F64, GROOVE_VT_BOOL, GROOVE_VT_STRING, GROOVE_VT_BYTES, GROOVE_VT_INTERNAL,
    GROOVE_VT_UUID, GROOVE_VT_ENUM_TAG, GROOVE_VT_TUPLE, GROOVE_VT_ARRAY, GROOVE_VT_NULLABLE,
    GROOVE_VT_RECORD, GROOVE_VT_ENUM, GROOVE_VT_U48,
    GROOVE_VT_COUNT
};
typedef struct groove_ValueType groove_ValueType;
typedef JZ_VEC(groove_ValueType) groove_ValueTypeVec;
struct groove_ValueType {
    u8 tag;
    union {
        groove_InternalValueType internal;
        groove_ScalarEnumSchema enum_tag;
        groove_ValueTypeVec tuple;
        groove_ValueType *inner;          /* Array / Nullable: Box<ValueType> */
        groove_RecordDescriptor record;   /* Record(Box<RecordDescriptor>) */
        groove_EnumSchema *enum_;         /* Enum(Box<EnumSchema>) */
    } u;
};

/* FieldIdentity */
enum { GROOVE_FID_NAME = 0, GROOVE_FID_SLOT = 1, GROOVE_FID_NAMED_SLOT = 2 };
typedef struct groove_FieldIdentity {
    u8 tag;
    jz_str name; /* Name / NamedSlot.name */
    u64 slot;    /* Slot / NamedSlot.slot */
} groove_FieldIdentity;

typedef struct groove_DescriptorField {
    bool has_name; jz_str name;
    bool has_identity; groove_FieldIdentity identity;
    groove_ValueType value_type;
} groove_DescriptorField;
typedef JZ_VEC(groove_DescriptorField) groove_DescriptorFieldVec;

/* FieldLayout { Static { offset, width } | Variable { variable_idx } } */
typedef struct groove_FieldLayout { bool is_static; u32 offset; u32 width; u32 variable_idx; } groove_FieldLayout;
typedef struct groove_RecordLayout {
    JZ_VEC(groove_FieldLayout) fields;
    JZ_VEC(u32) logical_by_physical;
    u32 fixed_size;
    u32 variable_count;
} groove_RecordLayout;
struct groove_RecordDescriptorData {
    groove_DescriptorFieldVec fields;
    groove_RecordLayout layout;
};

typedef struct groove_OwnedRecord { jz_bytes raw; groove_RecordDescriptor descriptor; } groove_OwnedRecord;
typedef struct groove_BorrowedRecord { jz_slice raw; groove_RecordDescriptor descriptor; } groove_BorrowedRecord;
typedef struct groove_EnumValue { u32 tag; groove_OwnedRecord record; } groove_EnumValue;

/* Value discriminants (serde variant order) */
enum {
    GROOVE_V_U8, GROOVE_V_U16, GROOVE_V_U32, GROOVE_V_U64, GROOVE_V_F64, GROOVE_V_BOOL,
    GROOVE_V_STRING, GROOVE_V_BYTES, GROOVE_V_LARGE, GROOVE_V_UUID, GROOVE_V_ENUM_TAG,
    GROOVE_V_TUPLE, GROOVE_V_ARRAY, GROOVE_V_NULLABLE, GROOVE_V_I64, GROOVE_V_I32,
    GROOVE_V_RECORD, GROOVE_V_ENUM, GROOVE_V_U48,
    GROOVE_V_COUNT
};
typedef struct groove_Value groove_Value;
typedef JZ_VEC(groove_Value) groove_ValueVec;
struct groove_Value {
    u8 tag;
    union {
        u8 u8_; u16 u16_; u32 u32_; u64 u64_; f64 f64_; bool b; i64 i64_; i32 i32_;
        jz_str s; jz_bytes bytes; groove_LargeValueRef *large; jz_uuid uuid; u8 enum_tag;
        groove_ValueVec items;      /* Tuple / Array */
        groove_Value *nullable;     /* Nullable: NULL = None */
        groove_OwnedRecord record;
        groove_EnumValue enum_;
    } u;
};

/* VariantRegistry { EnumTag { variants } | Enum { cases } } */
enum { GROOVE_VR_ENUM_TAG = 0, GROOVE_VR_ENUM = 1 };
typedef struct groove_VariantRegistry { u8 tag; groove_StrVec names; } groove_VariantRegistry;

/* SystemVariantRegistry(u64) */
typedef struct groove_SystemVariantRegistry { u64 id; } groove_SystemVariantRegistry;

#define GROOVE_SYSTEM_REGISTRY_MARKER ((u64)1 << 63)
#define GROOVE_U48_MAX ((((u64)1) << 48) - 1)

/* ---------------------------------------------------------------- values.rs API */
u64 groove__records__values__variant_registry_id_for_path(const char *p, u32 n);

/* Value: derives Clone, Debug, PartialEq, Serialize, Deserialize */
void groove__records__values__Value__drop(groove_Value *v);
void groove__records__values__Value__drop_v(void *v);
void groove__records__values__Value__Clone__clone(groove_Value *dst, const groove_Value *src);
void groove__records__values__Value__Clone__clone_v(void *dst, const void *src);
bool groove__records__values__Value__PartialEq__eq(const groove_Value *a, const groove_Value *b);
void groove__records__values__Value__Debug__fmt(jz_str *out, const groove_Value *v);
void groove__records__values__Value__Serialize__serialize(jz_pc_w *w, const groove_Value *v);
jz_err groove__records__values__Value__Deserialize__deserialize(jz_pc_r *r, groove_Value *out);
void groove__records__values__Value__Serialize__serialize_json(jz_json_w *w, const groove_Value *v);
jz_err groove__records__values__Value__Deserialize__deserialize_json(const jz_json *j, groove_Value *out);
void groove__records__values__ValueVec__drop(groove_ValueVec *v);

/* Value::from(..) helpers (From impls are inlined constructors) */
groove_Value groove__records__values__Value__from_u64(u64 v);
groove_Value groove__records__values__Value__from_str(const char *p, u32 n);
groove_Value groove__records__values__Value__from_bytes(const u8 *p, u32 n);
groove_Value groove__records__values__Value__from_option(groove_Value *some_or_null);

/* EnumValue */
groove_EnumValue groove__records__values__EnumValue__new(u32 tag, groove_OwnedRecord record);
jz_err groove__records__values__EnumValue__create(u32 tag, groove_RecordDescriptor descriptor, const groove_Value *values, u32 n, groove_EnumValue *out);
u64 groove__records__values__EnumValue__Hash__hash(const groove_EnumValue *v, u64 h);

/* ScalarEnumSchema */
jz_err groove__records__values__ScalarEnumSchema__new(jz_str name, groove_StrVec variants, groove_ScalarEnumSchema *out);
jz_err groove__records__values__ScalarEnumSchema__from_parts(u64 registry_id, jz_str name, groove_StrVec variants, groove_ScalarEnumSchema *out);
void groove__records__values__ScalarEnumSchema__with_registry_id(groove_ScalarEnumSchema *s, u64 registry_id);
void groove__records__values__ScalarEnumSchema__with_system_registry(groove_ScalarEnumSchema *s, groove_SystemVariantRegistry r);
jz_err groove__records__values__ScalarEnumSchema__discriminant(const groove_ScalarEnumSchema *s, const char *p, u32 n, u8 *out);
jz_err groove__records__values__ScalarEnumSchema__variant(const groove_ScalarEnumSchema *s, u8 discriminant, const jz_str **out);
void groove__records__values__ScalarEnumSchema__drop(groove_ScalarEnumSchema *s);
void groove__records__values__ScalarEnumSchema__Clone__clone(groove_ScalarEnumSchema *dst, const groove_ScalarEnumSchema *src);
bool groove__records__values__ScalarEnumSchema__PartialEq__eq(const groove_ScalarEnumSchema *a, const groove_ScalarEnumSchema *b);
groove_SystemVariantRegistry groove__records__values__SystemVariantRegistry__deletion_state(void);

/* EnumSchema / EnumCase */
groove_EnumCase groove__records__values__EnumCase__new(jz_str name, groove_RecordDescriptor payload);
jz_err groove__records__values__EnumSchema__new(jz_str name, groove_EnumCaseVec cases, groove_EnumSchema *out);
void groove__records__values__EnumSchema__with_registry_id(groove_EnumSchema *s, u64 registry_id);
jz_err groove__records__values__EnumSchema__case(const groove_EnumSchema *s, u32 tag, const groove_EnumCase **out);
jz_err groove__records__values__EnumSchema__tag(const groove_EnumSchema *s, const char *p, u32 n, u32 *out);
void groove__records__values__EnumSchema__drop(groove_EnumSchema *s);
void groove__records__values__EnumSchema__Clone__clone(groove_EnumSchema *dst, const groove_EnumSchema *src);
bool groove__records__values__EnumSchema__PartialEq__eq(const groove_EnumSchema *a, const groove_EnumSchema *b);

/* VariantRegistry */
void groove__records__values__VariantRegistry__drop(groove_VariantRegistry *r);
void groove__records__values__VariantRegistry__drop_v(void *r);
void groove__records__values__VariantRegistry__Clone__clone(groove_VariantRegistry *dst, const groove_VariantRegistry *src);
bool groove__records__values__VariantRegistry__PartialEq__eq(const groove_VariantRegistry *a, const groove_VariantRegistry *b);
u64 groove__records__values__VariantRegistry__Hash__hash(const groove_VariantRegistry *r, u64 h);
void groove__records__values__VariantRegistry__Debug__fmt(jz_str *out, const groove_VariantRegistry *r);
void groove__records__values__VariantRegistry__Serialize__serialize(jz_pc_w *w, const groove_VariantRegistry *r);
jz_err groove__records__values__VariantRegistry__Deserialize__deserialize(jz_pc_r *r, groove_VariantRegistry *out);
extern const jz_btree_desc groove__records__values__VARIANT_REGISTRY_MAP; /* BTreeMap<u64, VariantRegistry> */

/* ValueType: derives Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize */
void groove__records__values__ValueType__drop(groove_ValueType *t);
void groove__records__values__ValueType__drop_v(void *t);
void groove__records__values__ValueType__Clone__clone(groove_ValueType *dst, const groove_ValueType *src);
void groove__records__values__ValueType__Clone__clone_v(void *dst, const void *src);
bool groove__records__values__ValueType__PartialEq__eq(const groove_ValueType *a, const groove_ValueType *b);
u64 groove__records__values__ValueType__Hash__hash(const groove_ValueType *t, u64 h);
void groove__records__values__ValueType__Debug__fmt(jz_str *out, const groove_ValueType *t);
void groove__records__values__ValueType__Serialize__serialize(jz_pc_w *w, const groove_ValueType *t);
jz_err groove__records__values__ValueType__Deserialize__deserialize(jz_pc_r *r, groove_ValueType *out);
void groove__records__values__ValueType__Serialize__serialize_json(jz_json_w *w, const groove_ValueType *t);
jz_err groove__records__values__ValueType__Deserialize__deserialize_json(const jz_json *j, groove_ValueType *out);

groove_ValueType groove__records__values__ValueType__scalar(u8 tag);
const groove_ValueType *groove__records__values__ValueType__non_nullable(const groove_ValueType *t);
bool groove__records__values__ValueType__is_array_fk(const groove_ValueType *t);
bool groove__records__values__ValueType__is_internal_storage_type(const groove_ValueType *t);
groove_ValueType groove__records__values__ValueType__raw_string(void);
groove_ValueType groove__records__values__ValueType__raw_bytes(void);
groove_ValueType groove__records__values__ValueType__stored_scalar(groove_LargeValueKind kind);
bool groove__records__values__ValueType__may_contain_stored_scalar(const groove_ValueType *t);
u32 groove__records__values__ValueType__variant_registry_occurrence_count(const groove_ValueType *t);
bool groove__records__values__ValueType__registry_compatible_with(const groove_ValueType *a, const groove_ValueType *b);
bool groove__records__values__ValueType__registry_rebound_layout_compatible_with(const groove_ValueType *a, const groove_ValueType *b);
bool groove__records__values__ValueType__can_evolve_registry_to(const groove_ValueType *a, const groove_ValueType *b);
void groove__records__values__ValueType__collect_variant_registries(const groove_ValueType *t, jz_btree *output);
void groove__records__values__ValueType__stamp_variant_registries(groove_ValueType *t, const char *p, u32 n);
void groove__records__values__ValueType__rebind_variant_registries(groove_ValueType *t, const char *p, u32 n);
void groove__records__values__ValueType__assign_variant_registries(groove_ValueType *t, const char *p, u32 n, bool replace);
groove_ValueType groove__records__values__ValueType__nullable(groove_ValueType t);
groove_ValueType groove__records__values__ValueType__array_of(groove_ValueType t);
bool groove__records__values__ValueType__contains_record(const groove_ValueType *t);
bool groove__records__values__ValueType__fixed_size(const groove_ValueType *t, u32 *out);
bool groove__records__values__ValueType__is_fixed_size(const groove_ValueType *t);
bool groove__records__values__collect_by_ordered_scalar(const groove_ValueType *t);

/* descriptor codecs */
jz_err groove__records__values__encode_record_descriptor(groove_RecordDescriptor d, jz_bytes *out);
jz_err groove__records__values__decode_record_descriptor(const u8 *p, u32 n, groove_RecordDescriptor *out);
jz_err groove__records__values__encode_persisted_record_descriptor(groove_RecordDescriptor d, jz_bytes *out);
jz_err groove__records__values__decode_persisted_record_descriptor(const u8 *p, u32 n, groove_RecordDescriptor *out);

/* u48 */
jz_err groove__records__values__u48_be_bytes(u64 v, u8 out[6]);
u64 groove__records__values__u48_from_le_bytes(const u8 b[6]);
u64 groove__records__values__u48_from_be_bytes(const u8 b[6]);

/* value codecs (pub(super)/pub(crate)) */
jz_err groove__records__values__encode_value(const groove_Value *v, const groove_ValueType *t, jz_bytes *out);
jz_err groove__records__values__encode_value_into(jz_bytes *bytes, const groove_Value *v, const groove_ValueType *t);
jz_err groove__records__values__encode_fixed_value(jz_bytes *bytes, const groove_Value *v, const groove_ValueType *t);
jz_err groove__records__values__decode_value(const u8 *p, u32 n, const groove_ValueType *t, groove_Value *out);
jz_err groove__records__values__validate_value(const u8 *p, u32 n, const groove_ValueType *t);
jz_err groove__records__values__validate_canonical_value(const u8 *p, u32 n, const groove_ValueType *t);
jz_err groove__records__values__ensure_value_type(const groove_Value *v, const groove_ValueType *t);
jz_err groove__records__values__validate_schema_value_type(const groove_ValueType *t);

/* visit_encoded_indirect_values(bytes, type, visitor) -> Result<bool> */
typedef jz_err (*groove_IndirectVisitor)(void *ctx, const u8 *p, u32 n, const groove_ValueType *t, bool *stop);
jz_err groove__records__values__visit_encoded_indirect_values(const u8 *p, u32 n, const groove_ValueType *t, groove_IndirectVisitor visit, void *ctx, bool *out);

void groove__records__values__write_u32(jz_bytes *bytes, u32 v);
jz_err groove__records__values__checked_add(u32 a, u32 b, u32 *out);
jz_err groove__records__values__usize_to_u32(u64 v, u32 *out);

/* records::Error helpers */
jz_err groove__records__Error__clone(const jz_err *e);
bool groove__records__Error__PartialEq__eq(const jz_err *a, const jz_err *b);
jz_err groove__records__Error__type_mismatch(const groove_ValueType *expected);
jz_err groove__records__Error__invalid_tuple_member(const groove_ValueType *member);
jz_err groove__records__Error__field_index_out_of_bounds(u32 index, u32 len);
jz_err groove__records__Error__arity_mismatch(u32 expected, u32 actual);
jz_err groove__records__Error__simple(u32 code);

#endif
