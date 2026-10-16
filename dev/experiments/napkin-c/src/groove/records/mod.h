/* groove::records (crates/groove/src/records/mod.rs). Type definitions live in values.h. */
#ifndef GROOVE_RECORDS_MOD_H
#define GROOVE_RECORDS_MOD_H

#include "jz_rt.h"
#include "values.h"

#define GROOVE_MAX_VARIANT_TAG_LEN 5

/* (usize, usize) mapping pair */
typedef struct groove_IdxPair { u32 a, b; } groove_IdxPair;
typedef struct groove_Span { u32 start, end; } groove_Span;

jz_err groove__records__encode_single_field_value(const groove_Value *v, const groove_ValueType *t, jz_bytes *out);
jz_err groove__records__decode_single_field_value(const u8 *p, u32 n, const groove_ValueType *t, groove_Value *out);

/* RecordDescriptor */
groove_RecordDescriptor groove__records__RecordDescriptor__new(const jz_str *names, groove_ValueType *types, u32 n);
groove_RecordDescriptor groove__records__RecordDescriptor__new_cstr(const char *const *names, groove_ValueType *types, u32 n);
groove_RecordDescriptor groove__records__RecordDescriptor__new_with_fields(groove_DescriptorField *fields, u32 n);
groove_RecordDescriptor groove__records__RecordDescriptor__from_logical_fields(groove_DescriptorFieldVec fields);
groove_RecordDescriptor groove__records__RecordDescriptor__Default__default(void);
const groove_DescriptorField *groove__records__RecordDescriptor__fields(groove_RecordDescriptor d, u32 *len);
bool groove__records__RecordDescriptor__registry_compatible_with(groove_RecordDescriptor a, groove_RecordDescriptor b);
bool groove__records__RecordDescriptor__can_evolve_registry_to(groove_RecordDescriptor a, groove_RecordDescriptor b);
bool groove__records__RecordDescriptor__field_index(groove_RecordDescriptor d, const char *p, u32 n, u32 *out);
bool groove__records__RecordDescriptor__field_index_by_identity(groove_RecordDescriptor d, const groove_FieldIdentity *id, u32 *out);
jz_err groove__records__RecordDescriptor__create(groove_RecordDescriptor d, const groove_Value *values, u32 n, jz_bytes *out);
jz_err groove__records__RecordDescriptor__project(const groove_RecordDescriptor *srcs, const jz_slice *recs, u32 nsrc, u32 nrecs,
                                                 const groove_IdxPair *mapping, u32 nmap, groove_RecordDescriptor *out_d, jz_bytes *out_rec);
jz_err groove__records__RecordDescriptor__project_record(groove_RecordDescriptor d, const groove_RecordDescriptor *srcs, const jz_slice *recs,
                                                        u32 nsrc, u32 nrecs, const groove_IdxPair *mapping, u32 nmap, jz_bytes *out);
jz_err groove__records__RecordDescriptor__project_record_raw(groove_RecordDescriptor d, const groove_RecordDescriptor *srcs, const jz_slice *recs,
                                                            u32 nsrc, u32 nrecs, const groove_IdxPair *mapping, u32 nmap, jz_bytes *out);
groove_BorrowedRecord groove__records__RecordDescriptor__bind(groove_RecordDescriptor d, const u8 *p, u32 n);
jz_err groove__records__RecordDescriptor__get(groove_RecordDescriptor d, const u8 *rec, u32 n, const char *name, u32 nlen, groove_Value *out);
jz_err groove__records__RecordDescriptor__get_idx(groove_RecordDescriptor d, const u8 *rec, u32 n, u32 idx, groove_Value *out);
jz_err groove__records__RecordDescriptor__field_span(groove_RecordDescriptor d, const u8 *rec, u32 n, u32 idx, groove_Span *out);
jz_err groove__records__RecordDescriptor__fields_contain_indirect_values(groove_RecordDescriptor d, const u8 *rec, u32 n, const u32 *idx, u32 nidx, bool *out);
typedef bool (*groove_LargeRefVisitor)(void *ctx, const groove_LargeValueRef *r);
jz_err groove__records__RecordDescriptor__visit_large_value_refs(groove_RecordDescriptor d, const u8 *rec, u32 n, groove_LargeRefVisitor visit, void *ctx, bool *out);
/* visit_encoded_indirect_fields: indices == NULL means 0..fields.len() */
jz_err groove__records__RecordDescriptor__visit_encoded_indirect_fields(groove_RecordDescriptor d, const u8 *rec, u32 n, const u32 *idx, u32 nidx,
                                                                       groove_IndirectVisitor visit, void *ctx, bool *out);
/* append callback: FnMut(usize, &mut Vec<u8>) -> Result<(), E> */
typedef jz_err (*groove_FieldAppendFn)(void *ctx, u32 index, jz_bytes *output);
jz_err groove__records__RecordDescriptor__create_with_encoded_fields(groove_RecordDescriptor d, u32 capacity, groove_FieldAppendFn append, void *ctx, jz_bytes *out);
jz_err groove__records__RecordDescriptor__encode_field_into(groove_RecordDescriptor d, u32 index, const groove_Value *v, jz_bytes *output);
u32 groove__records__RecordDescriptor__fixed_size(groove_RecordDescriptor d);
u32 groove__records__RecordDescriptor__variable_count(groove_RecordDescriptor d);
u64 groove__records__RecordDescriptor__Hash__hash(groove_RecordDescriptor d, u64 h);
void groove__records__RecordDescriptor__Debug__fmt(jz_str *out, groove_RecordDescriptor d);
void groove__records__RecordDescriptor__Serialize__serialize(jz_pc_w *w, groove_RecordDescriptor d);
jz_err groove__records__RecordDescriptor__Deserialize__deserialize(jz_pc_r *r, groove_RecordDescriptor *out);
void groove__records__RecordDescriptor__Serialize__serialize_json(jz_json_w *w, groove_RecordDescriptor d);
jz_err groove__records__RecordDescriptor__Deserialize__deserialize_json(const jz_json *j, groove_RecordDescriptor *out);

/* RawProjectionField */
enum { GROOVE_RPF_COPY, GROOVE_RPF_WRAP_NULLABLE, GROOVE_RPF_NESTED, GROOVE_RPF_ERROR, GROOVE_RPF_EVALUATE, GROOVE_RPF_ENCODED };
typedef struct groove_NestedStep { groove_RecordDescriptor descriptor; u32 index; } groove_NestedStep;
typedef struct groove_RawProjectionField {
    u8 tag;
    u32 source_idx;                       /* Copy / WrapNullable */
    JZ_VEC(groove_NestedStep) path;       /* Nested */
    jz_err error;                         /* Error */
    jz_bytes bytes;                       /* Encoded */
} groove_RawProjectionField;
typedef JZ_VEC(groove_RawProjectionField) groove_RawProjectionFieldVec;
void groove__records__RawProjectionField__drop(groove_RawProjectionField *f);
void groove__records__RawProjectionField__Clone__clone(groove_RawProjectionField *dst, const groove_RawProjectionField *src);
bool groove__records__RawProjectionField__PartialEq__eq(const groove_RawProjectionField *a, const groove_RawProjectionField *b);

typedef struct groove_PreparedProjection { groove_RawProjectionFieldVec fields; bool reuses_input; } groove_PreparedProjection;
groove_PreparedProjection groove__records__PreparedProjection__new(groove_RecordDescriptor source, groove_RecordDescriptor target, groove_RawProjectionFieldVec fields);
void groove__records__PreparedProjection__drop(groove_PreparedProjection *p);

typedef struct groove_SourceUse { groove_RecordDescriptor descriptor; bool used; } groove_SourceUse;
typedef struct groove_CopyField { u32 source_idx; groove_FieldLayout layout; } groove_CopyField;
typedef struct groove_PreparedRecordCopy {
    JZ_VEC(groove_SourceUse) sources;
    groove_RecordDescriptor target;
    JZ_VEC(groove_CopyField) fields;
} groove_PreparedRecordCopy;
jz_err groove__records__PreparedRecordCopy__new(const groove_RecordDescriptor *sources, u32 nsources, groove_RecordDescriptor target,
                                                const groove_IdxPair *mapping, u32 nmap, groove_PreparedRecordCopy *out);
jz_err groove__records__PreparedRecordCopy__project_into(const groove_PreparedRecordCopy *c, const jz_slice *records, u32 n, jz_bytes *output, groove_Span *out);
void groove__records__PreparedRecordCopy__drop(groove_PreparedRecordCopy *c);

/* RawProjectionScratch */
typedef struct groove_RawProjectedBytes { bool generated; u32 start, end; } groove_RawProjectedBytes;
typedef struct groove_RawProjectionScratch { JZ_VEC(groove_RawProjectedBytes) variable_fields; jz_bytes generated; } groove_RawProjectionScratch;
void groove__records__RawProjectionScratch__drop(groove_RawProjectionScratch *s);

/* RecordProjector */
typedef struct groove_RecordProjector { groove_RecordDescriptor source, target; JZ_VEC(u32) target_to_source; } groove_RecordProjector;
jz_err groove__records__RecordProjector__new(groove_RecordDescriptor source, groove_RecordDescriptor target, const groove_IdxPair *mapping, u32 n, groove_RecordProjector *out);
jz_err groove__records__RecordProjector__new_registry_rebound(groove_RecordDescriptor source, groove_RecordDescriptor target, const groove_IdxPair *mapping, u32 n, groove_RecordProjector *out);
jz_err groove__records__RecordProjector__project(const groove_RecordProjector *p, groove_BorrowedRecord record, groove_OwnedRecord *out);
void groove__records__RecordProjector__drop(groove_RecordProjector *p);

/* evaluate callback for project_raw_fields_into */
typedef jz_err (*groove_EvaluateFn)(void *ctx, u32 target_idx, jz_bytes *output);
jz_err groove__records__RecordDescriptor__project_raw_fields_into(groove_RecordDescriptor d, groove_RecordDescriptor source, const u8 *rec, u32 n,
                                                                 const groove_RawProjectionField *fields, jz_bytes *output,
                                                                 groove_EvaluateFn evaluate, void *ctx, groove_Span *out);
jz_err groove__records__RecordDescriptor__write_projected_fields_into(groove_RecordDescriptor d, jz_bytes *output, groove_FieldAppendFn append, void *ctx, groove_Span *out);
jz_err groove__records__RecordDescriptor__unwrap_nullable_field_into(groove_RecordDescriptor d, groove_RecordDescriptor source, const u8 *rec, u32 n, u32 field_idx,
                                                                    jz_bytes *output, groove_RawProjectionScratch *scratch, bool *has, groove_Span *out);

/* BorrowedRecord */
groove_BorrowedRecord groove__records__BorrowedRecord__new(const u8 *p, u32 n, groove_RecordDescriptor d);
jz_err groove__records__BorrowedRecord__get(const groove_BorrowedRecord *r, const char *name, u32 nlen, groove_Value *out);
jz_err groove__records__BorrowedRecord__get_idx(const groove_BorrowedRecord *r, u32 idx, groove_Value *out);
jz_err groove__records__BorrowedRecord__get_record(const groove_BorrowedRecord *r, u32 idx, groove_BorrowedRecord *out);
jz_err groove__records__BorrowedRecord__to_values(const groove_BorrowedRecord *r, groove_ValueVec *out);
jz_err groove__records__BorrowedRecord__validate(const groove_BorrowedRecord *r);
jz_err groove__records__BorrowedRecord__validate_canonical(const groove_BorrowedRecord *r);
jz_err groove__records__BorrowedRecord__get_u64(const groove_BorrowedRecord *r, u32 idx, u64 *out);
jz_err groove__records__BorrowedRecord__get_i64(const groove_BorrowedRecord *r, u32 idx, i64 *out);
jz_err groove__records__BorrowedRecord__get_i32(const groove_BorrowedRecord *r, u32 idx, i32 *out);
jz_err groove__records__BorrowedRecord__get_f64(const groove_BorrowedRecord *r, u32 idx, f64 *out);
jz_err groove__records__BorrowedRecord__get_u48(const groove_BorrowedRecord *r, u32 idx, u64 *out);
jz_err groove__records__BorrowedRecord__get_nullable_u48(const groove_BorrowedRecord *r, u32 idx, bool *has, u64 *out);
jz_err groove__records__BorrowedRecord__get_u32(const groove_BorrowedRecord *r, u32 idx, u32 *out);
jz_err groove__records__BorrowedRecord__get_u8(const groove_BorrowedRecord *r, u32 idx, u8 *out);
jz_err groove__records__BorrowedRecord__get_bool(const groove_BorrowedRecord *r, u32 idx, bool *out);
jz_err groove__records__BorrowedRecord__get_enum(const groove_BorrowedRecord *r, u32 idx, u8 *out);
jz_err groove__records__BorrowedRecord__get_enum_name(const groove_BorrowedRecord *r, u32 idx, const jz_str **out);
jz_err groove__records__BorrowedRecord__get_bytes(const groove_BorrowedRecord *r, u32 idx, jz_slice *out);
jz_err groove__records__BorrowedRecord__get_uuid(const groove_BorrowedRecord *r, u32 idx, jz_uuid *out);
jz_err groove__records__BorrowedRecord__get_str(const groove_BorrowedRecord *r, u32 idx, jz_slice *out);
jz_err groove__records__BorrowedRecord__get_nullable_u64(const groove_BorrowedRecord *r, u32 idx, bool *has, u64 *out);
jz_err groove__records__BorrowedRecord__get_nullable_i64(const groove_BorrowedRecord *r, u32 idx, bool *has, i64 *out);
jz_err groove__records__BorrowedRecord__get_nullable_i32(const groove_BorrowedRecord *r, u32 idx, bool *has, i32 *out);
jz_err groove__records__BorrowedRecord__get_nullable_f64(const groove_BorrowedRecord *r, u32 idx, bool *has, f64 *out);
jz_err groove__records__BorrowedRecord__get_nullable_enum(const groove_BorrowedRecord *r, u32 idx, bool *has, u8 *out);
jz_err groove__records__BorrowedRecord__get_nullable_string(const groove_BorrowedRecord *r, u32 idx, bool *has, jz_slice *out);
jz_err groove__records__BorrowedRecord__get_nullable_bytes(const groove_BorrowedRecord *r, u32 idx, bool *has, jz_slice *out);
jz_err groove__records__BorrowedRecord__get_nullable_uuid(const groove_BorrowedRecord *r, u32 idx, bool *has, jz_uuid *out);
jz_err groove__records__BorrowedRecord__get_array_element(const groove_BorrowedRecord *r, u32 field_idx, u32 element_idx, groove_Value *out);
jz_err groove__records__BorrowedRecord__field(const groove_BorrowedRecord *r, u32 idx, const groove_DescriptorField **out);
jz_err groove__records__BorrowedRecord__field_bytes_unchecked(const groove_BorrowedRecord *r, u32 idx, jz_slice *out);
/* (crate-visible for macros.c) */
jz_err groove__records__BorrowedRecord__field_bytes(const groove_BorrowedRecord *r, u32 idx, const groove_ValueType *expected, jz_slice *out);
jz_err groove__records__BorrowedRecord__nullable_field_bytes(const groove_BorrowedRecord *r, u32 idx, const groove_ValueType *inner, jz_slice *out);

/* FieldIdentity / DescriptorField */
void groove__records__FieldIdentity__drop(groove_FieldIdentity *f);
void groove__records__FieldIdentity__Clone__clone(groove_FieldIdentity *dst, const groove_FieldIdentity *src);
bool groove__records__FieldIdentity__PartialEq__eq(const groove_FieldIdentity *a, const groove_FieldIdentity *b);
u64 groove__records__FieldIdentity__Hash__hash(const groove_FieldIdentity *f, u64 h);
void groove__records__FieldIdentity__Debug__fmt(jz_str *out, const groove_FieldIdentity *f);
void groove__records__FieldIdentity__Serialize__serialize(jz_pc_w *w, const groove_FieldIdentity *f);
jz_err groove__records__FieldIdentity__Deserialize__deserialize(jz_pc_r *r, groove_FieldIdentity *out);
void groove__records__FieldIdentity__Serialize__serialize_json(jz_json_w *w, const groove_FieldIdentity *f);
jz_err groove__records__FieldIdentity__Deserialize__deserialize_json(const jz_json *j, groove_FieldIdentity *out);

groove_DescriptorField groove__records__DescriptorField__new(jz_str name, groove_ValueType value_type);
void groove__records__DescriptorField__with_identity(groove_DescriptorField *f, groove_FieldIdentity identity);
bool groove__records__DescriptorField__logical_name(const groove_DescriptorField *f, jz_slice *out);
void groove__records__DescriptorField__drop(groove_DescriptorField *f);
void groove__records__DescriptorField__drop_v(void *f);
void groove__records__DescriptorField__Clone__clone(groove_DescriptorField *dst, const groove_DescriptorField *src);
void groove__records__DescriptorField__Clone__clone_v(void *dst, const void *src);
bool groove__records__DescriptorField__PartialEq__eq(const groove_DescriptorField *a, const groove_DescriptorField *b);
u64 groove__records__DescriptorField__Hash__hash(const groove_DescriptorField *f, u64 h);
void groove__records__DescriptorField__Debug__fmt(jz_str *out, const groove_DescriptorField *f);
void groove__records__DescriptorField__Serialize__serialize(jz_pc_w *w, const groove_DescriptorField *f);
jz_err groove__records__DescriptorField__Deserialize__deserialize(jz_pc_r *r, groove_DescriptorField *out);
void groove__records__DescriptorField__Serialize__serialize_json(jz_json_w *w, const groove_DescriptorField *f);
jz_err groove__records__DescriptorField__Deserialize__deserialize_json(const jz_json *j, groove_DescriptorField *out);

/* OwnedRecord / VariantRecord */
groove_OwnedRecord groove__records__OwnedRecord__new(jz_bytes raw, groove_RecordDescriptor d);
groove_BorrowedRecord groove__records__OwnedRecord__borrowed(const groove_OwnedRecord *r);
jz_err groove__records__OwnedRecord__get(const groove_OwnedRecord *r, const char *name, u32 nlen, groove_Value *out);
jz_err groove__records__OwnedRecord__get_idx(const groove_OwnedRecord *r, u32 idx, groove_Value *out);
jz_err groove__records__OwnedRecord__to_values(const groove_OwnedRecord *r, groove_ValueVec *out);
jz_err groove__records__OwnedRecord__validate(const groove_OwnedRecord *r);
void groove__records__OwnedRecord__drop(groove_OwnedRecord *r);
void groove__records__OwnedRecord__Clone__clone(groove_OwnedRecord *dst, const groove_OwnedRecord *src);
bool groove__records__OwnedRecord__PartialEq__eq(const groove_OwnedRecord *a, const groove_OwnedRecord *b);
u64 groove__records__OwnedRecord__Hash__hash(const groove_OwnedRecord *r, u64 h);
void groove__records__OwnedRecord__Debug__fmt(jz_str *out, const groove_OwnedRecord *r);
void groove__records__OwnedRecord__Serialize__serialize(jz_pc_w *w, const groove_OwnedRecord *r);
jz_err groove__records__OwnedRecord__Deserialize__deserialize(jz_pc_r *r, groove_OwnedRecord *out);
void groove__records__OwnedRecord__Serialize__serialize_json(jz_json_w *w, const groove_OwnedRecord *r);
jz_err groove__records__OwnedRecord__Deserialize__deserialize_json(const jz_json *j, groove_OwnedRecord *out);

typedef struct groove_VariantRecord { u32 variant_tag; groove_OwnedRecord record; } groove_VariantRecord;
typedef struct groove_ValidatedVariantRecord { groove_VariantRecord inner; } groove_ValidatedVariantRecord;
groove_VariantRecord groove__records__VariantRecord__new(u32 tag, groove_OwnedRecord record);
jz_err groove__records__VariantRecord__create(u32 tag, groove_RecordDescriptor d, const groove_Value *values, u32 n, groove_VariantRecord *out);
jz_bytes groove__records__VariantRecord__into_stored_bytes(groove_VariantRecord r);
jz_err groove__records__ValidatedVariantRecord__create_with_encoded_fields(u32 tag, groove_RecordDescriptor d, u32 capacity, groove_FieldAppendFn append, void *ctx, groove_ValidatedVariantRecord *out);
jz_err groove__records__ValidatedVariantRecord__create(u32 tag, groove_RecordDescriptor d, const groove_Value *values, u32 n, groove_ValidatedVariantRecord *out);
jz_bytes groove__records__encode_variant_record(u32 tag, const u8 *payload, u32 n);
void groove__records__append_variant_record(jz_bytes *stored, u32 tag, const u8 *payload, u32 n);
jz_err groove__records__split_variant_record(const u8 *stored, u32 n, u32 *tag, jz_slice *payload);

/* Record<'a> (owned bytes, borrowed descriptor) */
typedef struct groove_Record { jz_bytes raw; const groove_RecordDescriptor *descriptor; } groove_Record;
groove_Record groove__records__RecordDescriptor__bind_owned(const groove_RecordDescriptor *d, jz_bytes raw);
jz_err groove__records__Record__get(const groove_Record *r, const char *name, u32 nlen, groove_Value *out);
jz_err groove__records__Record__get_idx(const groove_Record *r, u32 idx, groove_Value *out);
jz_err groove__records__Record__to_values(const groove_Record *r, groove_ValueVec *out);

#endif
