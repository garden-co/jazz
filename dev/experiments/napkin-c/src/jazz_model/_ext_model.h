/* _ext_model.h: mirror declarations of the groove and jazz_types items the
 * jazz_model unit uses. Layouts follow the canonical groove/jazz_types C
 * definitions (copied, not included, per PORTING.md). */
#ifndef JAZZ_MODEL_EXT_MODEL_H
#define JAZZ_MODEL_EXT_MODEL_H
#include "jz_rt.h"

/* ======================= groove::records (layouts) ======================= */
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

/* ======================= groove::records::values (API used) ======================= */
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

/* ======================= groove::records (mod.rs API used) ======================= */
groove_RecordDescriptor groove__records__RecordDescriptor__new(const jz_str *names, const groove_ValueType *types, u32 n); /* moves in */
groove_RecordDescriptor groove__records__RecordDescriptor__new_with_fields(groove_DescriptorFieldVec fields);
const groove_DescriptorField *groove__records__RecordDescriptor__fields(groove_RecordDescriptor d, u32 *n);
bool groove__records__RecordDescriptor__field_index(groove_RecordDescriptor d, jz_slice name, u32 *out);
jz_err groove__records__RecordDescriptor__create(groove_RecordDescriptor d, const groove_Value *values, u32 n, jz_bytes *out);
jz_err groove__records__RecordDescriptor__get_idx(groove_RecordDescriptor d, jz_slice record, u32 idx, groove_Value *out);
groove_RecordDescriptor groove__records__RecordDescriptor__Default__default(void);
void groove__records__RecordDescriptor__Serialize__serialize(jz_pc_w *w, groove_RecordDescriptor d);
groove_DescriptorField groove__records__DescriptorField__new(jz_str name, groove_ValueType t);
groove_OwnedRecord groove__records__OwnedRecord__new(jz_bytes raw, groove_RecordDescriptor d);
jz_err groove__records__OwnedRecord__get_idx(const groove_OwnedRecord *r, u32 idx, groove_Value *out);
jz_err groove__records__OwnedRecord__to_values(const groove_OwnedRecord *r, groove_ValueVec *out);
void groove__records__OwnedRecord__drop(groove_OwnedRecord *r);
void groove__records__OwnedRecord__Clone__clone(groove_OwnedRecord *dst, const groove_OwnedRecord *src);
jz_err groove__records__BorrowedRecord__get_idx(groove_BorrowedRecord r, u32 idx, groove_Value *out);
jz_err groove__records__BorrowedRecord__get_u64(groove_BorrowedRecord r, u32 idx, u64 *out);
jz_err groove__records__BorrowedRecord__get_u8(groove_BorrowedRecord r, u32 idx, u8 *out);
jz_err groove__records__BorrowedRecord__get_str(groove_BorrowedRecord r, u32 idx, jz_slice *out);
jz_err groove__records__BorrowedRecord__get_bytes(groove_BorrowedRecord r, u32 idx, jz_slice *out);
jz_err groove__records__BorrowedRecord__get_uuid(groove_BorrowedRecord r, u32 idx, jz_uuid *out);
jz_err groove__records__BorrowedRecord__to_values(groove_BorrowedRecord r, groove_ValueVec *out);
void groove__records__Error__Display__fmt(jz_str *out, const jz_err *e);

/* ======================= jazz_types (mirrors) ======================= */
enum { JAZZ_TYPES_ERR_PC_BAD_ENCODING_M = 0x010b };
typedef jz_uuid jazz_types_NodeUuid;
typedef jz_uuid jazz_types_SchemaVersionId;
typedef u64 jazz_types_PhysicalTableId;
typedef jz_uuid jazz_types_RowUuid;
typedef jz_uuid jazz_types_AccountId;
typedef u64 jazz_types_TxTime;
typedef u64 jazz_types_GlobalTime;
typedef const jz_uuid *jazz_types_ObjectId; /* Intern<Uuid> */
typedef struct jazz_types_InternedAuthor jazz_types_InternedAuthor;
enum { JAZZ_AUTHOR_SYSTEM = 0, JAZZ_AUTHOR_SYSTEM_AT = 1, JAZZ_AUTHOR_AUTHENTICATED = 2 };
typedef struct jazz_types_AuthorSubject { u32 tag; const jazz_types_InternedAuthor *interned; } jazz_types_AuthorSubject;
enum { JAZZ_ROW_AUTHOR_SYSTEM_AT = 0, JAZZ_ROW_AUTHOR_ACCOUNT = 1 };
typedef struct jazz_types_RowAuthor { u32 tag; const jazz_types_InternedAuthor *interned; } jazz_types_RowAuthor;
typedef struct jazz_types_UnionArm { u32 position; jz_str label; } jazz_types_UnionArm;
typedef struct jazz_types_OutputOccurrenceId {
    jazz_types_ObjectId root;
    JZ_VEC(jazz_types_ObjectId) joined;
    JZ_VEC(jazz_types_UnionArm) union_arms;
} jazz_types_OutputOccurrenceId;
typedef struct jazz_types_ResultKey { jazz_types_OutputOccurrenceId occ; } jazz_types_ResultKey;

extern const char jazz_types__ids__AuthorSubject__SYSTEM_ISSUER[];
extern const char jazz_types__ids__AuthorSubject__LOCAL_FIRST_ISSUER[];
extern const char jazz_types__ids__AuthorSubject__ANONYMOUS_ISSUER[];
jz_slice jazz_types__ids__AuthorSubject__canonical(const jazz_types_AuthorSubject *s);
void jazz_types__ids__AuthorSubject__principal_parts(const jazz_types_AuthorSubject *s, jz_str *issuer, jz_str *subject);
bool jazz_types__ids__AuthorSubject__account_id(const jazz_types_AuthorSubject *s, jazz_types_AccountId *out);
jz_err jazz_types__ids__AuthorSubject__from_canonical(jz_slice canonical, jazz_types_AuthorSubject *out);
void jazz_types__ids__AuthorSubject__Serialize__serialize(jz_pc_w *w, const jazz_types_AuthorSubject *s);
jz_err jazz_types__ids__AuthorSubject__Deserialize__deserialize(jz_pc_r *r, jazz_types_AuthorSubject *out);
groove_ValueType jazz_types__ids__RowAuthor__value_type(void);
groove_Value jazz_types__ids__RowAuthor__to_value(jazz_types_RowAuthor a);
jz_err jazz_types__ids__RowAuthor__from_value(groove_Value value, jazz_types_RowAuthor *out);
jazz_types_AuthorSubject jazz_types__ids__RowAuthor__as_author_subject(jazz_types_RowAuthor a);
bool jazz_types__account_registry__AccountId__is_system(jazz_types_AccountId a);
jazz_types_ObjectId jazz_types__object__ObjectId__from_uuid(const jz_uuid *u);
int jazz_types__object__ObjectId__Ord__cmp(jazz_types_ObjectId a, jazz_types_ObjectId b);
void jazz_types__object__ObjectId__Debug__fmt(jz_str *out, jazz_types_ObjectId id);
void jazz_types__object__ObjectId__Display__fmt(jz_str *out, jazz_types_ObjectId id);
void jazz_types__object__ObjectId__Serialize__serialize(jz_pc_w *w, jazz_types_ObjectId id);
jz_err jazz_types__object__ObjectId__Deserialize__deserialize(jz_pc_r *r, jazz_types_ObjectId *out);
void jazz_types__object__OutputOccurrenceId__drop(jazz_types_OutputOccurrenceId *o);
void jazz_types__object__OutputOccurrenceId__Clone__clone(jazz_types_OutputOccurrenceId *dst, const jazz_types_OutputOccurrenceId *src);
bool jazz_types__object__OutputOccurrenceId__PartialEq__eq(const jazz_types_OutputOccurrenceId *a, const jazz_types_OutputOccurrenceId *b);
void jazz_types__time__TxTime__Serialize__serialize(jz_pc_w *w, jazz_types_TxTime t);
jz_err jazz_types__time__TxTime__Deserialize__deserialize(jz_pc_r *r, jazz_types_TxTime *out);
typedef struct jazz_types_PcCodec {
    jz_err (*decode)(jz_pc_r *r, void *out);
    void (*encode)(jz_pc_w *w, const void *value);
    void (*drop)(void *value);
} jazz_types_PcCodec;
jz_err jazz_types__postcard_exact__decode_postcard_exact(const jazz_types_PcCodec *codec, jz_slice bytes, void *out);
void jazz_types__time__GlobalTime__Serialize__serialize(jz_pc_w *w, jazz_types_GlobalTime t);
jz_err jazz_types__time__GlobalTime__Deserialize__deserialize(jz_pc_r *r, jazz_types_GlobalTime *out);
void jazz_types__time__TxTime__Debug__fmt(jz_str *out, jazz_types_TxTime t);

jz_err groove__records__OwnedRecord__get(const groove_OwnedRecord *r, jz_slice field_name, groove_Value *out);
jz_err jazz_types__ids__RowAuthor__from_persisted_subject(jazz_types_AuthorSubject s, jazz_types_RowAuthor *out);
groove_Value jazz_types__ids__AuthorSubject__to_value(jazz_types_AuthorSubject s);


/* ======================= groove::schema (layouts, API used) ======================= */
typedef struct groove_IndexSchema { jz_str name; JZ_VEC(jz_str) columns; bool unique; } groove_IndexSchema;
typedef struct groove_TableVariantField { jz_str name; groove_ValueType value_type; bool has_shared; jz_str shared_column; } groove_TableVariantField;
typedef struct groove_TableVariant { u32 tag; JZ_VEC(jz_str) fields; JZ_VEC(groove_TableVariantField) payload_fields; } groove_TableVariant;
typedef struct groove_ColumnSchema { jz_str name; groove_ValueType column_type; } groove_ColumnSchema;
typedef JZ_VEC(groove_ColumnSchema) groove_ColumnSchemaVec;
/* PrimaryKeyType { Integer(IntegerKeyType{U8,U16,U32,U64}) | Bool | String | Bytes | Uuid } flattened */
enum { GROOVE_PKT_U8 = 0, GROOVE_PKT_U16, GROOVE_PKT_U32, GROOVE_PKT_U64, GROOVE_PKT_BOOL, GROOVE_PKT_STRING, GROOVE_PKT_BYTES, GROOVE_PKT_UUID };
typedef struct groove_PrimaryKeyColumn { jz_str column; u8 key_type; } groove_PrimaryKeyColumn;
typedef JZ_VEC(groove_PrimaryKeyColumn) groove_PrimaryKeyColumnVec;
typedef struct groove_PrimaryKey { groove_PrimaryKeyColumnVec columns; bool generated; } groove_PrimaryKey;
typedef struct groove_TableSchema {
    jz_str name;
    u64 variant_registry_id;
    jz_btree value_variant_registries;
    groove_ColumnSchemaVec columns;
    bool has_primary_key; groove_PrimaryKey primary_key;
    JZ_VEC(groove_IndexSchema) indices;
    jz_vec foreign_keys;
    JZ_VEC(groove_TableVariant) variants;
} groove_TableSchema;
typedef JZ_VEC(groove_TableSchema) groove_TableSchemaVec;
typedef struct groove_NamedValueType { jz_str name; groove_ValueType value_type; } groove_NamedValueType;
typedef struct groove_DirectRecordStoreSchema {
    jz_str name; JZ_VEC(groove_NamedValueType) key; JZ_VEC(groove_NamedValueType) value;
} groove_DirectRecordStoreSchema;
typedef struct groove_DatabaseSchema { groove_TableSchemaVec tables; JZ_VEC(groove_DirectRecordStoreSchema) direct_record_stores; } groove_DatabaseSchema;

groove_ColumnSchema groove__schema__ColumnSchema__new(jz_str name, groove_ValueType column_type);
groove_TableSchema groove__schema__TableSchema__new(jz_str name, groove_ColumnSchema *columns, u32 n);
void groove__schema__TableSchema__with_primary_key(groove_TableSchema *t, groove_PrimaryKey pk);
void groove__schema__TableSchema__with_index(groove_TableSchema *t, groove_IndexSchema index);
void groove__schema__TableSchema__drop(groove_TableSchema *t);
groove_PrimaryKey groove__schema__PrimaryKey__new(jz_str column, u8 integer_type);
groove_PrimaryKey groove__schema__PrimaryKey__composite(groove_PrimaryKeyColumn *columns, u32 n);
groove_PrimaryKeyColumn groove__schema__PrimaryKeyColumn__integer(jz_str column, u8 integer_type);
groove_PrimaryKeyColumn groove__schema__PrimaryKeyColumn__bytes(jz_str column);
groove_PrimaryKeyColumn groove__schema__PrimaryKeyColumn__uuid(jz_str column);
groove_IndexSchema groove__schema__IndexSchema__new(jz_str name, jz_str *columns, u32 n);
groove_DatabaseSchema groove__schema__DatabaseSchema__new(groove_TableSchemaVec tables);
void groove__schema__DatabaseSchema__with_direct_record_store(groove_DatabaseSchema *s, groove_DirectRecordStoreSchema store);
groove_DirectRecordStoreSchema groove__schema__DirectRecordStoreSchema__new(jz_str name, groove_RecordDescriptor key, groove_RecordDescriptor value);
typedef JZ_VEC(jz_slice) groove_SliceVec;
void groove__schema__DatabaseSchema__column_families(const groove_DatabaseSchema *s, groove_SliceVec *out);
void groove__schema__DatabaseSchema__drop(groove_DatabaseSchema *s);
/* groove::storage::StorageLayout */
enum { GROOVE_STORAGE_LAYOUT_IDENTITY = 0, GROOVE_STORAGE_LAYOUT_JAZZ_CLASS_V2 = 1 };
void groove__storage__StorageLayout__physical_column_families(u8 layout, const jz_slice *logical, u32 n, JZ_VEC(jz_str) *out);
/* groove::large_values */
groove_ValueType groove__large_values__physical_storage_value_type(groove_LargeValueKind kind);
/* records */

groove_ValueType jazz_types__ids__AuthorSubject__value_type(void);

/* jazz_types::ids::AuthorSubject::metadata_type: NULL = None */
const groove_ValueType *jazz_types__ids__AuthorSubject__metadata_type(jz_slice column);
/* groove::large_values::LargeValueRef leading fields (layout prefix of the groove struct) */
typedef struct groove_LargeValueRefHead { u8 kind; u8 format_version; u8 logical_hash[32]; } groove_LargeValueRefHead;
/* RecordDescriptor is an interned Copy handle: eq/clone by pointer */
#define GROOVE_RECORD_DESCRIPTOR_EQ(a, b) ((a) == (b))

#endif
