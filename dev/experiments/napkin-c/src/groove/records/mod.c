/* groove::records (crates/groove/src/records/mod.rs)
 *
 * Compact binary record descriptors, layout, and encoded row access. Fixed-width
 * fields come first, then a u32 offset table for all but the last variable field,
 * then variable payloads. Multi-byte scalars and offsets are little-endian. */
#include "jz_rt.h"
#include "values.h"
#include "mod.h"
#include "../large_values.h"

#define V(name) groove__records__values__##name

static jz_err unexpected_eof(void) { return groove__records__Error__simple(GROOVE_REC_E_UnexpectedEof); }
static jz_err invalid_offset(void) { return groove__records__Error__simple(GROOVE_REC_E_InvalidOffset); }
static jz_err oob(u32 index, u32 len) { return groove__records__Error__field_index_out_of_bounds(index, len); }
static inline u32 get_le32(const u8 *p) { return (u32)p[0] | ((u32)p[1] << 8) | ((u32)p[2] << 16) | ((u32)p[3] << 24); }
static inline void put_le32(u8 *p, u32 v) { p[0] = (u8)v; p[1] = (u8)(v >> 8); p[2] = (u8)(v >> 16); p[3] = (u8)(v >> 24); }
static inline u32 sat_sub1(u32 v) { return v ? v - 1 : 0; }

static void bytes_resize_zero(jz_bytes *b, u32 len) {
    if (len > b->len) {
        jz_vec_reserve(b, len - b->len, 1);
        memset(b->ptr + b->len, 0, len - b->len);
    }
    b->len = len;
}

/* ===================================================================== single field */

jz_err groove__records__encode_single_field_value(const groove_Value *v, const groove_ValueType *t, jz_bytes *out) {
    return V(encode_value)(v, t, out);
}

jz_err groove__records__decode_single_field_value(const u8 *p, u32 n, const groove_ValueType *t, groove_Value *out) {
    return V(decode_value)(p, n, t, out);
}

/* ===================================================================== FieldIdentity */

void groove__records__FieldIdentity__drop(groove_FieldIdentity *f) {
    if (f->tag != GROOVE_FID_SLOT) jz_str_drop(&f->name);
}

void groove__records__FieldIdentity__Clone__clone(groove_FieldIdentity *dst, const groove_FieldIdentity *src) {
    dst->tag = src->tag;
    dst->slot = src->slot;
    if (src->tag != GROOVE_FID_SLOT) dst->name = jz_str_clone(&src->name);
    else memset(&dst->name, 0, sizeof(dst->name));
}

bool groove__records__FieldIdentity__PartialEq__eq(const groove_FieldIdentity *a, const groove_FieldIdentity *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case GROOVE_FID_NAME: return jz_str_eq(&a->name, &b->name);
    case GROOVE_FID_SLOT: return a->slot == b->slot;
    default: return jz_str_eq(&a->name, &b->name) && a->slot == b->slot;
    }
}

u64 groove__records__FieldIdentity__Hash__hash(const groove_FieldIdentity *f, u64 h) {
    h = jz_hash_combine(h, f->tag);
    if (f->tag != GROOVE_FID_SLOT) h = jz_hash_combine(h, jz_str_hash(&f->name));
    if (f->tag != GROOVE_FID_NAME) h = jz_hash_combine(h, f->slot);
    return h;
}

void groove__records__FieldIdentity__Debug__fmt(jz_str *out, const groove_FieldIdentity *f) {
    switch (f->tag) {
    case GROOVE_FID_NAME: jz_str_appendf(out, "Name(\"%.*s\")", (int)f->name.len, f->name.ptr); break;
    case GROOVE_FID_SLOT: jz_str_appendf(out, "Slot(%llu)", (unsigned long long)f->slot); break;
    default: jz_str_appendf(out, "NamedSlot { name: \"%.*s\", slot: %llu }", (int)f->name.len, f->name.ptr, (unsigned long long)f->slot); break;
    }
}

void groove__records__FieldIdentity__Serialize__serialize(jz_pc_w *w, const groove_FieldIdentity *f) {
    jz_pc_variant(w, f->tag);
    if (f->tag != GROOVE_FID_SLOT) jz_pc_str(w, &f->name);
    if (f->tag != GROOVE_FID_NAME) jz_pc_varint(w, f->slot);
}

jz_err groove__records__FieldIdentity__Deserialize__deserialize(jz_pc_r *r, groove_FieldIdentity *out) {
    u32 idx;
    memset(out, 0, sizeof(*out));
    JZ_TRY(jz_pc_r_variant(r, &idx, 3));
    out->tag = (u8)idx;
    if (idx != GROOVE_FID_SLOT) JZ_TRY(jz_pc_r_str(r, &out->name));
    if (idx != GROOVE_FID_NAME) {
        jz_err e = jz_pc_r_varint(r, &out->slot);
        if (e.code) { groove__records__FieldIdentity__drop(out); return e; }
    }
    return JZ_OK;
}

static const char *const FIELD_IDENTITY_NAMES[3] = {"Name", "Slot", "NamedSlot"};

void groove__records__FieldIdentity__Serialize__serialize_json(jz_json_w *w, const groove_FieldIdentity *f) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, FIELD_IDENTITY_NAMES[f->tag]);
    switch (f->tag) {
    case GROOVE_FID_NAME: jz_jw_str(w, f->name.ptr, f->name.len); break;
    case GROOVE_FID_SLOT: jz_jw_u64(w, f->slot); break;
    default:
        jz_jw_begin_obj(w);
        jz_jw_key(w, "name");
        jz_jw_str(w, f->name.ptr, f->name.len);
        jz_jw_key(w, "slot");
        jz_jw_u64(w, f->slot);
        jz_jw_end_obj(w);
        break;
    }
    jz_jw_end_obj(w);
}

jz_err groove__records__FieldIdentity__Deserialize__deserialize_json(const jz_json *j, groove_FieldIdentity *out) {
    u32 idx;
    const jz_json *p = NULL;
    memset(out, 0, sizeof(*out));
    JZ_TRY(jz_json_variant(j, FIELD_IDENTITY_NAMES, 3, &idx, &p));
    out->tag = (u8)idx;
    switch (idx) {
    case GROOVE_FID_NAME: {
        const jz_str *s = p ? jz_json_as_str(p) : NULL;
        if (!s) return jz_err_new(JZ_JSON_ERR_TYPE, "invalid type: expected a string");
        out->name = jz_str_clone(s);
        return JZ_OK;
    }
    case GROOVE_FID_SLOT:
        if (!p || !jz_json_as_u64(p, &out->slot)) return jz_err_new(JZ_JSON_ERR_TYPE, "invalid type: expected u64");
        return JZ_OK;
    default:
        JZ_TRY(jz_json_expect_obj(p, "struct variant FieldIdentity::NamedSlot"));
        JZ_TRY(jz_json_field_str(p, "name", &out->name));
        jz_err e = jz_json_field_u64(p, "slot", &out->slot);
        if (e.code) jz_str_drop(&out->name);
        return e;
    }
}

/* ===================================================================== DescriptorField */

groove_DescriptorField groove__records__DescriptorField__new(jz_str name, groove_ValueType value_type) {
    groove_DescriptorField f;
    memset(&f, 0, sizeof(f));
    f.has_name = true;
    f.name = name;
    f.value_type = value_type;
    return f;
}

void groove__records__DescriptorField__with_identity(groove_DescriptorField *f, groove_FieldIdentity identity) {
    if (f->has_identity) groove__records__FieldIdentity__drop(&f->identity);
    f->has_identity = true;
    f->identity = identity;
}

bool groove__records__DescriptorField__logical_name(const groove_DescriptorField *f, jz_slice *out) {
    if (f->has_identity && (f->identity.tag == GROOVE_FID_NAME || f->identity.tag == GROOVE_FID_NAMED_SLOT)) {
        *out = jz_str_slice(&f->identity.name);
        return true;
    }
    if (f->has_name) {
        *out = jz_str_slice(&f->name);
        return true;
    }
    return false;
}

static void with_default_identity(groove_DescriptorField *f) {
    if (!f->has_identity && f->has_name) {
        f->has_identity = true;
        f->identity.tag = GROOVE_FID_NAME;
        f->identity.name = jz_str_clone(&f->name);
        f->identity.slot = 0;
    }
}

void groove__records__DescriptorField__drop(groove_DescriptorField *f) {
    if (f->has_name) jz_str_drop(&f->name);
    if (f->has_identity) groove__records__FieldIdentity__drop(&f->identity);
    V(ValueType__drop)(&f->value_type);
}
void groove__records__DescriptorField__drop_v(void *f) { groove__records__DescriptorField__drop(f); }

void groove__records__DescriptorField__Clone__clone(groove_DescriptorField *dst, const groove_DescriptorField *src) {
    memset(dst, 0, sizeof(*dst));
    dst->has_name = src->has_name;
    if (src->has_name) dst->name = jz_str_clone(&src->name);
    dst->has_identity = src->has_identity;
    if (src->has_identity) groove__records__FieldIdentity__Clone__clone(&dst->identity, &src->identity);
    V(ValueType__Clone__clone)(&dst->value_type, &src->value_type);
}
void groove__records__DescriptorField__Clone__clone_v(void *dst, const void *src) { groove__records__DescriptorField__Clone__clone(dst, src); }

static bool opt_str_eq(bool ha, const jz_str *a, bool hb, const jz_str *b) { return ha == hb && (!ha || jz_str_eq(a, b)); }

bool groove__records__DescriptorField__PartialEq__eq(const groove_DescriptorField *a, const groove_DescriptorField *b) {
    if (!opt_str_eq(a->has_name, &a->name, b->has_name, &b->name)) return false;
    if (a->has_identity != b->has_identity) return false;
    if (a->has_identity && !groove__records__FieldIdentity__PartialEq__eq(&a->identity, &b->identity)) return false;
    return V(ValueType__PartialEq__eq)(&a->value_type, &b->value_type);
}

u64 groove__records__DescriptorField__Hash__hash(const groove_DescriptorField *f, u64 h) {
    h = jz_hash_combine(h, f->has_name);
    if (f->has_name) h = jz_hash_combine(h, jz_str_hash(&f->name));
    h = jz_hash_combine(h, f->has_identity);
    if (f->has_identity) h = groove__records__FieldIdentity__Hash__hash(&f->identity, h);
    return V(ValueType__Hash__hash)(&f->value_type, h);
}

void groove__records__DescriptorField__Debug__fmt(jz_str *out, const groove_DescriptorField *f) {
    jz_str_push_cstr(out, "DescriptorField { name: ");
    if (f->has_name) jz_str_appendf(out, "Some(\"%.*s\")", (int)f->name.len, f->name.ptr);
    else jz_str_push_cstr(out, "None");
    jz_str_push_cstr(out, ", identity: ");
    if (f->has_identity) {
        jz_str_push_cstr(out, "Some(");
        groove__records__FieldIdentity__Debug__fmt(out, &f->identity);
        jz_str_push_char(out, ')');
    } else {
        jz_str_push_cstr(out, "None");
    }
    jz_str_push_cstr(out, ", value_type: ");
    V(ValueType__Debug__fmt)(out, &f->value_type);
    jz_str_push_cstr(out, " }");
}

void groove__records__DescriptorField__Serialize__serialize(jz_pc_w *w, const groove_DescriptorField *f) {
    jz_pc_option(w, f->has_name);
    if (f->has_name) jz_pc_str(w, &f->name);
    /* skip_serializing_if = "Option::is_none" (postcard writes the field only when present) */
    if (f->has_identity) {
        jz_pc_option(w, true);
        groove__records__FieldIdentity__Serialize__serialize(w, &f->identity);
    }
    V(ValueType__Serialize__serialize)(w, &f->value_type);
}

jz_err groove__records__DescriptorField__Deserialize__deserialize(jz_pc_r *r, groove_DescriptorField *out) {
    memset(out, 0, sizeof(*out));
    JZ_TRY(jz_pc_r_option(r, &out->has_name));
    if (out->has_name) JZ_TRY(jz_pc_r_str(r, &out->name));
    jz_err e = jz_pc_r_option(r, &out->has_identity);
    if (!e.code && out->has_identity) e = groove__records__FieldIdentity__Deserialize__deserialize(r, &out->identity);
    if (!e.code) e = V(ValueType__Deserialize__deserialize)(r, &out->value_type);
    if (e.code) {
        if (out->has_name) jz_str_drop(&out->name);
        if (out->has_identity) groove__records__FieldIdentity__drop(&out->identity);
    }
    return e;
}

void groove__records__DescriptorField__Serialize__serialize_json(jz_json_w *w, const groove_DescriptorField *f) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "name");
    if (f->has_name) jz_jw_str(w, f->name.ptr, f->name.len);
    else jz_jw_null(w);
    if (f->has_identity) {
        jz_jw_key(w, "identity");
        groove__records__FieldIdentity__Serialize__serialize_json(w, &f->identity);
    }
    jz_jw_key(w, "value_type");
    V(ValueType__Serialize__serialize_json)(w, &f->value_type);
    jz_jw_end_obj(w);
}

jz_err groove__records__DescriptorField__Deserialize__deserialize_json(const jz_json *j, groove_DescriptorField *out) {
    memset(out, 0, sizeof(*out));
    JZ_TRY(jz_json_expect_obj(j, "struct DescriptorField"));
    const jz_json *name = jz_json_obj_get(j, "name");
    if (name && name->kind != JZ_JSON_NULL) {
        const jz_str *s = jz_json_as_str(name);
        if (!s) return jz_err_new(JZ_JSON_ERR_TYPE, "invalid type: expected a string");
        out->has_name = true;
        out->name = jz_str_clone(s);
    }
    jz_err e = JZ_OK;
    const jz_json *identity = jz_json_obj_get(j, "identity"); /* #[serde(default)] */
    if (identity && identity->kind != JZ_JSON_NULL) {
        out->has_identity = true;
        e = groove__records__FieldIdentity__Deserialize__deserialize_json(identity, &out->identity);
        if (e.code) out->has_identity = false;
    }
    if (!e.code) {
        const jz_json *vt = jz_json_obj_get(j, "value_type");
        e = vt ? V(ValueType__Deserialize__deserialize_json)(vt, &out->value_type)
               : jz_err_new(JZ_JSON_ERR_MISSING_FIELD, "missing field `value_type`");
    }
    if (e.code) {
        if (out->has_name) jz_str_drop(&out->name);
        if (out->has_identity) groove__records__FieldIdentity__drop(&out->identity);
    }
    return e;
}

/* ===================================================================== RecordDescriptorData (interned) */

static jz_interner *DESCRIPTOR_POOL;

static u64 descriptor_data_hash(const void *p) {
    const groove_RecordDescriptorData *d = p;
    u64 h = jz_hash_u64(d->fields.len);
    for (u32 i = 0; i < d->fields.len; i++) h = groove__records__DescriptorField__Hash__hash(&d->fields.ptr[i], h);
    h = jz_hash_combine(h, d->layout.fields.len);
    for (u32 i = 0; i < d->layout.fields.len; i++) {
        const groove_FieldLayout *l = &d->layout.fields.ptr[i];
        h = jz_hash_combine(h, l->is_static);
        h = l->is_static ? jz_hash_combine(jz_hash_combine(h, l->offset), l->width) : jz_hash_combine(h, l->variable_idx);
    }
    h = jz_hash_combine(h, jz_hash_bytes(d->layout.logical_by_physical.ptr, d->layout.logical_by_physical.len * sizeof(u32)));
    h = jz_hash_combine(h, d->layout.fixed_size);
    return jz_hash_combine(h, d->layout.variable_count);
}

static bool layout_eq(const groove_FieldLayout *a, const groove_FieldLayout *b) {
    if (a->is_static != b->is_static) return false;
    return a->is_static ? (a->offset == b->offset && a->width == b->width) : a->variable_idx == b->variable_idx;
}

static bool descriptor_data_eq(const void *pa, const void *pb) {
    const groove_RecordDescriptorData *a = pa, *b = pb;
    if (a->fields.len != b->fields.len || a->layout.fields.len != b->layout.fields.len ||
        a->layout.logical_by_physical.len != b->layout.logical_by_physical.len ||
        a->layout.fixed_size != b->layout.fixed_size || a->layout.variable_count != b->layout.variable_count)
        return false;
    for (u32 i = 0; i < a->fields.len; i++)
        if (!groove__records__DescriptorField__PartialEq__eq(&a->fields.ptr[i], &b->fields.ptr[i])) return false;
    for (u32 i = 0; i < a->layout.fields.len; i++)
        if (!layout_eq(&a->layout.fields.ptr[i], &b->layout.fields.ptr[i])) return false;
    return memcmp(a->layout.logical_by_physical.ptr, b->layout.logical_by_physical.ptr, a->layout.logical_by_physical.len * sizeof(u32)) == 0;
}

static void descriptor_data_drop(groove_RecordDescriptorData *d) {
    jz_vec_drop(&d->fields, sizeof(groove_DescriptorField), groove__records__DescriptorField__drop_v);
    jz_vec_drop(&d->layout.fields, sizeof(groove_FieldLayout), NULL);
    jz_vec_drop(&d->layout.logical_by_physical, sizeof(u32), NULL);
}

groove_RecordDescriptor groove__records__RecordDescriptor__from_logical_fields(groove_DescriptorFieldVec fields) {
    for (u32 i = 0; i < fields.len; i++) with_default_identity(&fields.ptr[i]);
    for (u32 i = 0; i < fields.len; i++) {
        jz_err e = V(validate_schema_value_type)(&fields.ptr[i].value_type);
        if (e.code) {
            jz_str msg = {0};
            const groove_DescriptorField *f = &fields.ptr[i];
            if (f->has_name) jz_str_appendf(&msg, "invalid record field Some(\"%.*s\"): ", (int)f->name.len, f->name.ptr);
            else jz_str_push_cstr(&msg, "invalid record field None: ");
            if (e.msg) jz_str_push(&msg, e.msg->ptr, e.msg->len);
            jz_str_push_char(&msg, 0);
            jz_panic(msg.ptr);
        }
    }
    groove_RecordDescriptorData data;
    memset(&data, 0, sizeof(data));
    jz_vec_reserve(&data.layout.fields, fields.len, sizeof(groove_FieldLayout));
    for (u32 i = 0; i < fields.len; i++) {
        groove_FieldLayout l = {false, 0, 0, UINT32_MAX};
        jz_vec_push(&data.layout.fields, &l, sizeof(l));
    }
    jz_vec_reserve(&data.layout.logical_by_physical, fields.len, sizeof(u32));
    u32 fixed_size = 0, variable_count = 0;
    /* partition(is_fixed_size): fixed fields first, then variable, each in logical order */
    for (int pass = 0; pass < 2; pass++) {
        for (u32 logical_idx = 0; logical_idx < fields.len; logical_idx++) {
            u32 width;
            bool fixed = V(ValueType__fixed_size)(&fields.ptr[logical_idx].value_type, &width);
            if (fixed != (pass == 0)) continue;
            JZ_VEC_PUSH(data.layout.logical_by_physical, logical_idx);
            groove_FieldLayout *l = &data.layout.fields.ptr[logical_idx];
            if (fixed) {
                u32 offset = fixed_size;
                if (fixed_size > UINT32_MAX - width) jz_panic("record descriptor fixed layout exceeds usize");
                fixed_size += width;
                l->is_static = true;
                l->offset = offset;
                l->width = width;
            } else {
                l->is_static = false;
                l->variable_idx = variable_count;
                variable_count += 1;
            }
        }
    }
    data.fields = fields;
    data.layout.fixed_size = fixed_size;
    data.layout.variable_count = variable_count;
    bool inserted;
    const groove_RecordDescriptorData *interned =
        jz_intern_ex(&DESCRIPTOR_POOL, &data, sizeof(data), descriptor_data_hash, descriptor_data_eq, &inserted);
    if (!inserted) descriptor_data_drop(&data);
    return interned;
}

groove_RecordDescriptor groove__records__RecordDescriptor__Default__default(void) {
    groove_DescriptorFieldVec empty = {0};
    return groove__records__RecordDescriptor__from_logical_fields(empty);
}

groove_RecordDescriptor groove__records__RecordDescriptor__new(const jz_str *names, groove_ValueType *types, u32 n) {
    groove_DescriptorFieldVec fields = {0};
    jz_vec_reserve(&fields, n, sizeof(groove_DescriptorField));
    for (u32 i = 0; i < n; i++) {
        groove_DescriptorField f = groove__records__DescriptorField__new(jz_str_clone(&names[i]), types[i]);
        jz_vec_push(&fields, &f, sizeof(f));
    }
    return groove__records__RecordDescriptor__from_logical_fields(fields);
}

groove_RecordDescriptor groove__records__RecordDescriptor__new_cstr(const char *const *names, groove_ValueType *types, u32 n) {
    groove_DescriptorFieldVec fields = {0};
    jz_vec_reserve(&fields, n, sizeof(groove_DescriptorField));
    for (u32 i = 0; i < n; i++) {
        groove_DescriptorField f = groove__records__DescriptorField__new(jz_str_from(names[i]), types[i]);
        jz_vec_push(&fields, &f, sizeof(f));
    }
    return groove__records__RecordDescriptor__from_logical_fields(fields);
}

groove_RecordDescriptor groove__records__RecordDescriptor__new_with_fields(groove_DescriptorField *fields, u32 n) {
    groove_DescriptorFieldVec v = {0};
    jz_vec_extend(&v, fields, n, sizeof(groove_DescriptorField));
    return groove__records__RecordDescriptor__from_logical_fields(v);
}

const groove_DescriptorField *groove__records__RecordDescriptor__fields(groove_RecordDescriptor d, u32 *len) {
    *len = d->fields.len;
    return d->fields.ptr;
}

u32 groove__records__RecordDescriptor__fixed_size(groove_RecordDescriptor d) { return d->layout.fixed_size; }
u32 groove__records__RecordDescriptor__variable_count(groove_RecordDescriptor d) { return d->layout.variable_count; }

u64 groove__records__RecordDescriptor__Hash__hash(groove_RecordDescriptor d, u64 h) { return jz_hash_combine(h, (u64)(uintptr_t)d); }

void groove__records__RecordDescriptor__Debug__fmt(jz_str *out, groove_RecordDescriptor d) {
    jz_str_push_cstr(out, "RecordDescriptor(RecordDescriptorData { fields: [");
    for (u32 i = 0; i < d->fields.len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        groove__records__DescriptorField__Debug__fmt(out, &d->fields.ptr[i]);
    }
    jz_str_push_cstr(out, "], layout: RecordLayout { fields: [");
    for (u32 i = 0; i < d->layout.fields.len; i++) {
        const groove_FieldLayout *l = &d->layout.fields.ptr[i];
        if (i) jz_str_push_cstr(out, ", ");
        if (l->is_static) jz_str_appendf(out, "Static { offset: %u, width: %u }", l->offset, l->width);
        else jz_str_appendf(out, "Variable { variable_idx: %u }", l->variable_idx);
    }
    jz_str_push_cstr(out, "], logical_by_physical: [");
    for (u32 i = 0; i < d->layout.logical_by_physical.len; i++) jz_str_appendf(out, i ? ", %u" : "%u", d->layout.logical_by_physical.ptr[i]);
    jz_str_appendf(out, "], fixed_size: %u, variable_count: %u } })", d->layout.fixed_size, d->layout.variable_count);
}

/* Serialize as Vec<DescriptorField>; Deserialize via from_logical_fields. */
void groove__records__RecordDescriptor__Serialize__serialize(jz_pc_w *w, groove_RecordDescriptor d) {
    jz_pc_seq(w, d->fields.len);
    for (u32 i = 0; i < d->fields.len; i++) groove__records__DescriptorField__Serialize__serialize(w, &d->fields.ptr[i]);
}

jz_err groove__records__RecordDescriptor__Deserialize__deserialize(jz_pc_r *r, groove_RecordDescriptor *out) {
    u32 n;
    JZ_TRY(jz_pc_r_enter(r));
    jz_err e = jz_pc_r_seq(r, &n);
    groove_DescriptorFieldVec fields = {0};
    for (u32 i = 0; !e.code && i < n; i++) {
        groove_DescriptorField f;
        e = groove__records__DescriptorField__Deserialize__deserialize(r, &f);
        if (!e.code) jz_vec_push(&fields, &f, sizeof(f));
    }
    jz_pc_r_leave(r);
    if (e.code) {
        jz_vec_drop(&fields, sizeof(groove_DescriptorField), groove__records__DescriptorField__drop_v);
        return e;
    }
    *out = groove__records__RecordDescriptor__from_logical_fields(fields);
    return JZ_OK;
}

void groove__records__RecordDescriptor__Serialize__serialize_json(jz_json_w *w, groove_RecordDescriptor d) {
    jz_jw_begin_arr(w);
    for (u32 i = 0; i < d->fields.len; i++) groove__records__DescriptorField__Serialize__serialize_json(w, &d->fields.ptr[i]);
    jz_jw_end_arr(w);
}

jz_err groove__records__RecordDescriptor__Deserialize__deserialize_json(const jz_json *j, groove_RecordDescriptor *out) {
    if (!j || j->kind != JZ_JSON_ARR) return jz_err_new(JZ_JSON_ERR_TYPE, "invalid type: expected a sequence");
    groove_DescriptorFieldVec fields = {0};
    for (u32 i = 0; i < j->v.arr.len; i++) {
        groove_DescriptorField f;
        jz_err e = groove__records__DescriptorField__Deserialize__deserialize_json(j->v.arr.ptr[i], &f);
        if (e.code) {
            jz_vec_drop(&fields, sizeof(groove_DescriptorField), groove__records__DescriptorField__drop_v);
            return e;
        }
        jz_vec_push(&fields, &f, sizeof(f));
    }
    *out = groove__records__RecordDescriptor__from_logical_fields(fields);
    return JZ_OK;
}

/* ===================================================================== RecordDescriptor methods */

bool groove__records__RecordDescriptor__registry_compatible_with(groove_RecordDescriptor a, groove_RecordDescriptor b) {
    if (a->fields.len != b->fields.len) return false;
    for (u32 i = 0; i < a->fields.len; i++) {
        const groove_DescriptorField *x = &a->fields.ptr[i], *y = &b->fields.ptr[i];
        if (!opt_str_eq(x->has_name, &x->name, y->has_name, &y->name)) return false;
        if (!V(ValueType__registry_compatible_with)(&x->value_type, &y->value_type)) return false;
    }
    return true;
}

bool groove__records__RecordDescriptor__can_evolve_registry_to(groove_RecordDescriptor a, groove_RecordDescriptor b) {
    if (a->fields.len != b->fields.len) return false;
    for (u32 i = 0; i < a->fields.len; i++) {
        const groove_DescriptorField *x = &a->fields.ptr[i], *y = &b->fields.ptr[i];
        if (!opt_str_eq(x->has_name, &x->name, y->has_name, &y->name)) return false;
        if (!V(ValueType__can_evolve_registry_to)(&x->value_type, &y->value_type)) return false;
    }
    return true;
}

bool groove__records__RecordDescriptor__field_index(groove_RecordDescriptor d, const char *p, u32 n, u32 *out) {
    for (u32 i = 0; i < d->fields.len; i++) {
        jz_slice name;
        if (groove__records__DescriptorField__logical_name(&d->fields.ptr[i], &name) && name.len == n && memcmp(name.ptr, p, n) == 0) {
            *out = i;
            return true;
        }
    }
    return false;
}

bool groove__records__RecordDescriptor__field_index_by_identity(groove_RecordDescriptor d, const groove_FieldIdentity *id, u32 *out) {
    for (u32 i = 0; i < d->fields.len; i++) {
        if (d->fields.ptr[i].has_identity && groove__records__FieldIdentity__PartialEq__eq(&d->fields.ptr[i].identity, id)) {
            *out = i;
            return true;
        }
    }
    return false;
}

typedef struct CreateCtx { groove_RecordDescriptor d; const groove_Value *values; } CreateCtx;
static jz_err create_append(void *ctx, u32 index, jz_bytes *output) {
    CreateCtx *c = ctx;
    return groove__records__RecordDescriptor__encode_field_into(c->d, index, &c->values[index], output);
}

jz_err groove__records__RecordDescriptor__create(groove_RecordDescriptor d, const groove_Value *values, u32 n, jz_bytes *out) {
    if (d->fields.len != n) return groove__records__Error__arity_mismatch(d->fields.len, n);
    u32 capacity = d->layout.fixed_size + sat_sub1(d->layout.variable_count) * 4;
    CreateCtx ctx = {d, values};
    return groove__records__RecordDescriptor__create_with_encoded_fields(d, capacity, create_append, &ctx, out);
}

static jz_err project_fields_and_values(const groove_RecordDescriptor *srcs, const jz_slice *recs, u32 nsrc, u32 nrecs,
                                        const groove_IdxPair *mapping, u32 nmap, groove_DescriptorFieldVec *out_fields, groove_ValueVec *out_values);

jz_err groove__records__RecordDescriptor__project(const groove_RecordDescriptor *srcs, const jz_slice *recs, u32 nsrc, u32 nrecs,
                                                 const groove_IdxPair *mapping, u32 nmap, groove_RecordDescriptor *out_d, jz_bytes *out_rec) {
    groove_DescriptorFieldVec fields;
    groove_ValueVec values;
    JZ_TRY(project_fields_and_values(srcs, recs, nsrc, nrecs, mapping, nmap, &fields, &values));
    groove_RecordDescriptor descriptor = groove__records__RecordDescriptor__from_logical_fields(fields);
    jz_err e = groove__records__RecordDescriptor__create(descriptor, values.ptr, values.len, out_rec);
    V(ValueVec__drop)(&values);
    if (e.code) return e;
    *out_d = descriptor;
    return JZ_OK;
}

jz_err groove__records__RecordDescriptor__project_record(groove_RecordDescriptor d, const groove_RecordDescriptor *srcs, const jz_slice *recs,
                                                        u32 nsrc, u32 nrecs, const groove_IdxPair *mapping, u32 nmap, jz_bytes *out) {
    groove_DescriptorFieldVec fields;
    groove_ValueVec values;
    JZ_TRY(project_fields_and_values(srcs, recs, nsrc, nrecs, mapping, nmap, &fields, &values));
    jz_vec_drop(&fields, sizeof(groove_DescriptorField), groove__records__DescriptorField__drop_v);
    jz_err e = groove__records__RecordDescriptor__create(d, values.ptr, values.len, out);
    V(ValueVec__drop)(&values);
    return e;
}

jz_err groove__records__RecordDescriptor__project_record_raw(groove_RecordDescriptor d, const groove_RecordDescriptor *srcs, const jz_slice *recs,
                                                            u32 nsrc, u32 nrecs, const groove_IdxPair *mapping, u32 nmap, jz_bytes *out) {
    if (d->fields.len != nmap) return groove__records__Error__arity_mismatch(d->fields.len, nmap);
    if (nsrc != nrecs) return groove__records__Error__arity_mismatch(nsrc, nrecs);
    u32 fixed_size = d->layout.fixed_size;
    u32 variable_count = d->layout.variable_count;
    u32 offset_table_size = sat_sub1(variable_count) * 4;
    jz_bytes record = {0};
    jz_vec_reserve(&record, fixed_size + offset_table_size, 1);
    JZ_VEC(jz_slice) variable_values = {0};
    jz_vec_reserve(&variable_values, variable_count, sizeof(jz_slice));
    jz_err e = JZ_OK;
    for (u32 k = 0; k < d->layout.logical_by_physical.len; k++) {
        u32 logical_idx = d->layout.logical_by_physical.ptr[k];
        u32 sdi = mapping[logical_idx].a, sfi = mapping[logical_idx].b;
        if (sdi >= nsrc) { e = oob(sdi, nsrc); goto fail; }
        if (sdi >= nrecs) { e = oob(sdi, nrecs); goto fail; }
        groove_RecordDescriptor sd = srcs[sdi];
        jz_slice srec = recs[sdi];
        if (sfi >= sd->fields.len) { e = oob(sfi, sd->fields.len); goto fail; }
        const groove_DescriptorField *output_field = &d->fields.ptr[logical_idx];
        if (!V(ValueType__PartialEq__eq)(&sd->fields.ptr[sfi].value_type, &output_field->value_type)) {
            e = groove__records__Error__type_mismatch(&output_field->value_type);
            goto fail;
        }
        groove_Span span;
        e = groove__records__RecordDescriptor__field_span(sd, srec.ptr, srec.len, sfi, &span);
        if (e.code) goto fail;
        jz_slice encoded = {srec.ptr + span.start, span.end - span.start};
        const groove_FieldLayout *l = &d->layout.fields.ptr[logical_idx];
        if (l->is_static) {
            if (encoded.len != l->width) { e = invalid_offset(); goto fail; }
            jz_bytes_push(&record, encoded.ptr, encoded.len);
        } else {
            JZ_VEC_PUSH(variable_values, encoded);
        }
    }
    u32 next_offset = fixed_size + offset_table_size;
    for (u32 i = 0; i < sat_sub1(variable_values.len); i++) {
        e = V(checked_add)(next_offset, variable_values.ptr[i].len, &next_offset);
        if (e.code) goto fail;
        V(write_u32)(&record, next_offset);
    }
    for (u32 i = 0; i < variable_values.len; i++) jz_bytes_push(&record, variable_values.ptr[i].ptr, variable_values.ptr[i].len);
    jz_vec_drop(&variable_values, sizeof(jz_slice), NULL);
    *out = record;
    return JZ_OK;
fail:
    jz_vec_drop(&variable_values, sizeof(jz_slice), NULL);
    jz_bytes_drop(&record);
    return e;
}

groove_BorrowedRecord groove__records__RecordDescriptor__bind(groove_RecordDescriptor d, const u8 *p, u32 n) {
    return groove__records__BorrowedRecord__new(p, n, d);
}

groove_Record groove__records__RecordDescriptor__bind_owned(const groove_RecordDescriptor *d, jz_bytes raw) {
    groove_Record r = {raw, d};
    return r;
}

jz_err groove__records__RecordDescriptor__get(groove_RecordDescriptor d, const u8 *rec, u32 n, const char *name, u32 nlen, groove_Value *out) {
    u32 idx;
    if (!groove__records__RecordDescriptor__field_index(d, name, nlen, &idx))
        return jz_err_fmt(GROOVE_REC_E_FieldNotFound, "field not found: %.*s", (int)nlen, name);
    return groove__records__RecordDescriptor__get_idx(d, rec, n, idx, out);
}

jz_err groove__records__RecordDescriptor__get_idx(groove_RecordDescriptor d, const u8 *rec, u32 n, u32 idx, groove_Value *out) {
    if (idx >= d->fields.len) return oob(idx, d->fields.len);
    groove_Span span;
    JZ_TRY(groove__records__RecordDescriptor__field_span(d, rec, n, idx, &span));
    return V(decode_value)(rec + span.start, span.end - span.start, &d->fields.ptr[idx].value_type, out);
}

static jz_err validate_record_header(const u8 *rec, u32 n, groove_RecordDescriptor d);
static jz_err record_value_span_for_layout(const u8 *rec, u32 n, groove_RecordDescriptor d, const groove_FieldLayout *l, groove_Span *out);

static jz_err record_value_span(const u8 *rec, u32 n, groove_RecordDescriptor d, u32 idx, groove_Span *out) {
    if (idx >= d->layout.fields.len) return oob(idx, d->fields.len);
    JZ_TRY(validate_record_header(rec, n, d));
    return record_value_span_for_layout(rec, n, d, &d->layout.fields.ptr[idx], out);
}

jz_err groove__records__RecordDescriptor__field_span(groove_RecordDescriptor d, const u8 *rec, u32 n, u32 idx, groove_Span *out) {
    return record_value_span(rec, n, d, idx, out);
}

static jz_err true_visitor(void *ctx, const u8 *p, u32 n, const groove_ValueType *t, bool *stop) {
    (void)ctx; (void)p; (void)n; (void)t;
    *stop = true;
    return JZ_OK;
}

jz_err groove__records__RecordDescriptor__fields_contain_indirect_values(groove_RecordDescriptor d, const u8 *rec, u32 n, const u32 *idx, u32 nidx, bool *out) {
    return groove__records__RecordDescriptor__visit_encoded_indirect_fields(d, rec, n, idx, nidx, true_visitor, NULL, out);
}

typedef struct LargeRefCtx { groove_LargeRefVisitor visit; void *ctx; } LargeRefCtx;
static jz_err large_ref_visitor(void *ctx, const u8 *p, u32 n, const groove_ValueType *t, bool *stop) {
    LargeRefCtx *c = ctx;
    groove_Value v;
    JZ_TRY(V(decode_value)(p, n, t, &v));
    if (v.tag != GROOVE_V_LARGE) jz_panic("indirect traversal visits only the indirect scalar arm");
    *stop = c->visit(c->ctx, v.u.large);
    V(Value__drop)(&v);
    return JZ_OK;
}

jz_err groove__records__RecordDescriptor__visit_large_value_refs(groove_RecordDescriptor d, const u8 *rec, u32 n, groove_LargeRefVisitor visit, void *ctx, bool *out) {
    LargeRefCtx c = {visit, ctx};
    return groove__records__RecordDescriptor__visit_encoded_indirect_fields(d, rec, n, NULL, 0, large_ref_visitor, &c, out);
}

jz_err groove__records__RecordDescriptor__visit_encoded_indirect_fields(groove_RecordDescriptor d, const u8 *rec, u32 n, const u32 *idx, u32 nidx,
                                                                       groove_IndirectVisitor visit, void *ctx, bool *out) {
    u32 count = idx ? nidx : d->fields.len;
    for (u32 k = 0; k < count; k++) {
        u32 index = idx ? idx[k] : k;
        if (index >= d->fields.len) return oob(index, d->fields.len);
        const groove_DescriptorField *field = &d->fields.ptr[index];
        if (V(ValueType__may_contain_stored_scalar)(&field->value_type)) {
            groove_Span span;
            JZ_TRY(groove__records__RecordDescriptor__field_span(d, rec, n, index, &span));
            bool hit;
            JZ_TRY(V(visit_encoded_indirect_values)(rec + span.start, span.end - span.start, &field->value_type, visit, ctx, &hit));
            if (hit) { *out = true; return JZ_OK; }
        }
    }
    *out = false;
    return JZ_OK;
}

jz_err groove__records__RecordDescriptor__create_with_encoded_fields(groove_RecordDescriptor d, u32 capacity, groove_FieldAppendFn append, void *ctx, jz_bytes *out) {
    jz_bytes output = {0};
    jz_vec_reserve(&output, capacity, 1);
    jz_err e = JZ_OK;
    for (u32 k = 0; k < d->layout.logical_by_physical.len; k++) {
        u32 index = d->layout.logical_by_physical.ptr[k];
        if (d->layout.fields.ptr[index].is_static) {
            e = append(ctx, index, &output);
            if (e.code) goto fail;
        }
    }
    u32 fixed_size = d->layout.fixed_size, variable_count = d->layout.variable_count, header;
    e = V(checked_add)(fixed_size, sat_sub1(variable_count) * 4, &header);
    if (e.code) goto fail;
    bytes_resize_zero(&output, header);
    for (u32 k = 0; k < d->layout.logical_by_physical.len; k++) {
        u32 index = d->layout.logical_by_physical.ptr[k];
        const groove_FieldLayout *l = &d->layout.fields.ptr[index];
        if (l->is_static) continue;
        e = append(ctx, index, &output);
        if (e.code) goto fail;
        if (l->variable_idx + 1 < variable_count) {
            u32 end;
            e = V(usize_to_u32)(output.len, &end);
            if (e.code) goto fail;
            put_le32(output.ptr + fixed_size + l->variable_idx * 4, end);
        }
    }
    *out = output;
    return JZ_OK;
fail:
    jz_bytes_drop(&output);
    return e;
}

jz_err groove__records__RecordDescriptor__encode_field_into(groove_RecordDescriptor d, u32 index, const groove_Value *v, jz_bytes *output) {
    if (index >= d->fields.len) return oob(index, d->fields.len);
    const groove_DescriptorField *field = &d->fields.ptr[index];
    JZ_TRY(V(ensure_value_type)(v, &field->value_type));
    if (V(ValueType__is_fixed_size)(&field->value_type)) return V(encode_fixed_value)(output, v, &field->value_type);
    u32 start = output->len;
    jz_err e = V(encode_value_into)(output, v, &field->value_type);
    if (e.code) output->len = start; /* the allocating variable-field path appended nothing on error */
    return e;
}

/* ===================================================================== PreparedRecordCopy */

jz_err groove__records__PreparedRecordCopy__new(const groove_RecordDescriptor *sources, u32 nsources, groove_RecordDescriptor target,
                                                const groove_IdxPair *mapping, u32 nmap, groove_PreparedRecordCopy *out) {
    if (target->fields.len != nmap) return groove__records__Error__arity_mismatch(target->fields.len, nmap);
    groove_PreparedRecordCopy c;
    memset(&c, 0, sizeof(c));
    jz_vec_reserve(&c.sources, nsources, sizeof(groove_SourceUse));
    for (u32 i = 0; i < nsources; i++) {
        groove_SourceUse s = {sources[i], false};
        jz_vec_push(&c.sources, &s, sizeof(s));
    }
    jz_vec_reserve(&c.fields, nmap, sizeof(groove_CopyField));
    u32 source_count = c.sources.len;
    for (u32 target_idx = 0; target_idx < nmap; target_idx++) {
        u32 source_idx = mapping[target_idx].a, field_idx = mapping[target_idx].b;
        jz_err e = JZ_OK;
        if (source_idx >= source_count) e = oob(source_idx, source_count);
        groove_SourceUse *s = e.code ? NULL : &c.sources.ptr[source_idx];
        if (!e.code && field_idx >= s->descriptor->fields.len) e = oob(field_idx, s->descriptor->fields.len);
        if (!e.code && !V(ValueType__PartialEq__eq)(&s->descriptor->fields.ptr[field_idx].value_type, &target->fields.ptr[target_idx].value_type))
            e = groove__records__Error__type_mismatch(&target->fields.ptr[target_idx].value_type);
        if (e.code) {
            groove__records__PreparedRecordCopy__drop(&c);
            return e;
        }
        s->used = true;
        groove_CopyField f = {source_idx, s->descriptor->layout.fields.ptr[field_idx]};
        jz_vec_push(&c.fields, &f, sizeof(f));
    }
    c.target = target;
    *out = c;
    return JZ_OK;
}

void groove__records__PreparedRecordCopy__drop(groove_PreparedRecordCopy *c) {
    jz_vec_drop(&c->sources, sizeof(groove_SourceUse), NULL);
    jz_vec_drop(&c->fields, sizeof(groove_CopyField), NULL);
}

typedef struct CopyCtx { const groove_PreparedRecordCopy *c; const jz_slice *records; } CopyCtx;
static jz_err copy_append(void *ctx, u32 target_idx, jz_bytes *output) {
    CopyCtx *cc = ctx;
    const groove_CopyField *f = &cc->c->fields.ptr[target_idx];
    jz_slice record = cc->records[f->source_idx];
    groove_Span span;
    JZ_TRY(record_value_span_for_layout(record.ptr, record.len, cc->c->sources.ptr[f->source_idx].descriptor, &f->layout, &span));
    jz_bytes_push(output, record.ptr + span.start, span.end - span.start);
    return JZ_OK;
}

jz_err groove__records__PreparedRecordCopy__project_into(const groove_PreparedRecordCopy *c, const jz_slice *records, u32 n, jz_bytes *output, groove_Span *out) {
    if (n != c->sources.len) return groove__records__Error__arity_mismatch(c->sources.len, n);
    for (u32 i = 0; i < n; i++)
        if (c->sources.ptr[i].used) JZ_TRY(validate_record_header(records[i].ptr, records[i].len, c->sources.ptr[i].descriptor));
    u32 start = output->len;
    CopyCtx ctx = {c, records};
    jz_err e = groove__records__RecordDescriptor__write_projected_fields_into(c->target, output, copy_append, &ctx, out);
    if (e.code) output->len = start;
    return e;
}

/* ===================================================================== PreparedProjection / RawProjectionField */

void groove__records__RawProjectionField__drop(groove_RawProjectionField *f) {
    switch (f->tag) {
    case GROOVE_RPF_NESTED: jz_vec_drop(&f->path, sizeof(groove_NestedStep), NULL); break;
    case GROOVE_RPF_ERROR: jz_err_drop(&f->error); break;
    case GROOVE_RPF_ENCODED: jz_bytes_drop(&f->bytes); break;
    default: break;
    }
}

void groove__records__RawProjectionField__Clone__clone(groove_RawProjectionField *dst, const groove_RawProjectionField *src) {
    memset(dst, 0, sizeof(*dst));
    dst->tag = src->tag;
    dst->source_idx = src->source_idx;
    switch (src->tag) {
    case GROOVE_RPF_NESTED: jz_vec_clone(&dst->path, &src->path, sizeof(groove_NestedStep), NULL); break;
    case GROOVE_RPF_ERROR: dst->error = groove__records__Error__clone(&src->error); break;
    case GROOVE_RPF_ENCODED: dst->bytes = jz_bytes_clone(&src->bytes); break;
    default: break;
    }
}

bool groove__records__RawProjectionField__PartialEq__eq(const groove_RawProjectionField *a, const groove_RawProjectionField *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case GROOVE_RPF_COPY:
    case GROOVE_RPF_WRAP_NULLABLE: return a->source_idx == b->source_idx;
    case GROOVE_RPF_NESTED:
        if (a->path.len != b->path.len) return false;
        for (u32 i = 0; i < a->path.len; i++)
            if (a->path.ptr[i].descriptor != b->path.ptr[i].descriptor || a->path.ptr[i].index != b->path.ptr[i].index) return false;
        return true;
    case GROOVE_RPF_ERROR: return groove__records__Error__PartialEq__eq(&a->error, &b->error);
    case GROOVE_RPF_ENCODED: return jz_bytes_eq(&a->bytes, &b->bytes);
    default: return true;
    }
}

static void rpf_drop_v(void *f) { groove__records__RawProjectionField__drop(f); }

groove_PreparedProjection groove__records__PreparedProjection__new(groove_RecordDescriptor source, groove_RecordDescriptor target, groove_RawProjectionFieldVec fields) {
    bool reuses_input = source->fields.len == target->fields.len && fields.len == target->fields.len &&
                        source->layout.fixed_size == target->layout.fixed_size &&
                        source->layout.variable_count == target->layout.variable_count;
    for (u32 target_idx = 0; reuses_input && target_idx < fields.len; target_idx++) {
        const groove_RawProjectionField *f = &fields.ptr[target_idx];
        if (f->tag != GROOVE_RPF_COPY) { reuses_input = false; break; }
        if (f->source_idx >= source->fields.len) { reuses_input = false; break; }
        reuses_input = V(ValueType__PartialEq__eq)(&source->fields.ptr[f->source_idx].value_type, &target->fields.ptr[target_idx].value_type) &&
                       layout_eq(&source->layout.fields.ptr[f->source_idx], &target->layout.fields.ptr[target_idx]);
    }
    groove_PreparedProjection p = {fields, reuses_input};
    return p;
}

void groove__records__PreparedProjection__drop(groove_PreparedProjection *p) {
    jz_vec_drop(&p->fields, sizeof(groove_RawProjectionField), rpf_drop_v);
}

void groove__records__RawProjectionScratch__drop(groove_RawProjectionScratch *s) {
    jz_vec_drop(&s->variable_fields, sizeof(groove_RawProjectedBytes), NULL);
    jz_bytes_drop(&s->generated);
}

/* ===================================================================== RecordProjector */

static jz_err projector_new_inner(groove_RecordDescriptor source, groove_RecordDescriptor target, const groove_IdxPair *mapping, u32 n,
                                  bool allow_registry_rebinding, groove_RecordProjector *out) {
    u32 tlen = target->fields.len;
    u32 *target_to_source = jz_alloc((tlen ? tlen : 1) * sizeof(u32));
    bool *has = jz_zalloc(tlen ? tlen : 1);
    jz_err e = JZ_OK;
    for (u32 k = 0; k < n && !e.code; k++) {
        u32 source_idx = mapping[k].a, target_idx = mapping[k].b;
        if (source_idx >= source->fields.len) { e = oob(source_idx, source->fields.len); break; }
        if (target_idx >= tlen) { e = oob(target_idx, tlen); break; }
        if (has[target_idx]) {
            e = jz_err_fmt(GROOVE_REC_E_ProjectDuplicateTarget, "projection target field %u was mapped more than once", target_idx);
            break;
        }
        const groove_ValueType *st = &source->fields.ptr[source_idx].value_type;
        const groove_ValueType *tt = &target->fields.ptr[target_idx].value_type;
        if (!V(ValueType__PartialEq__eq)(st, tt) && !(allow_registry_rebinding && V(ValueType__registry_rebound_layout_compatible_with)(st, tt))) {
            jz_str sd = {0}, td = {0};
            V(ValueType__Debug__fmt)(&sd, st);
            V(ValueType__Debug__fmt)(&td, tt);
            e = jz_err_fmt(GROOVE_REC_E_ProjectTypeMismatch,
                           "projection source field %u type %.*s does not match target field %u type %.*s",
                           source_idx, (int)sd.len, sd.ptr, target_idx, (int)td.len, td.ptr);
            jz_str_drop(&sd);
            jz_str_drop(&td);
            break;
        }
        target_to_source[target_idx] = source_idx;
        has[target_idx] = true;
    }
    for (u32 target_idx = 0; !e.code && target_idx < tlen; target_idx++)
        if (!has[target_idx]) e = jz_err_fmt(GROOVE_REC_E_ProjectMissingTarget, "projection target field %u has no source field", target_idx);
    jz_free(has);
    if (e.code) {
        jz_free(target_to_source);
        return e;
    }
    out->source = source;
    out->target = target;
    out->target_to_source.ptr = target_to_source;
    out->target_to_source.len = tlen;
    out->target_to_source.cap = tlen;
    return JZ_OK;
}

jz_err groove__records__RecordProjector__new(groove_RecordDescriptor source, groove_RecordDescriptor target, const groove_IdxPair *mapping, u32 n, groove_RecordProjector *out) {
    return projector_new_inner(source, target, mapping, n, false, out);
}

jz_err groove__records__RecordProjector__new_registry_rebound(groove_RecordDescriptor source, groove_RecordDescriptor target, const groove_IdxPair *mapping, u32 n, groove_RecordProjector *out) {
    return projector_new_inner(source, target, mapping, n, true, out);
}

void groove__records__RecordProjector__drop(groove_RecordProjector *p) { jz_vec_drop(&p->target_to_source, sizeof(u32), NULL); }

jz_err groove__records__RecordProjector__project(const groove_RecordProjector *p, groove_BorrowedRecord record, groove_OwnedRecord *out) {
    if (record.descriptor != p->source) return groove__records__Error__simple(GROOVE_REC_E_ProjectSourceDescriptorMismatch);
    groove_RecordDescriptor target = p->target;
    u32 fixed_size = target->layout.fixed_size, variable_count = target->layout.variable_count;
    u32 offset_table_size = sat_sub1(variable_count) * 4, cap;
    JZ_TRY(V(checked_add)(fixed_size, offset_table_size, &cap));
    jz_bytes raw = {0};
    jz_vec_reserve(&raw, cap, 1);
    jz_err e = JZ_OK;
    groove_Span span;
    for (u32 k = 0; k < target->layout.logical_by_physical.len; k++) {
        u32 ti = target->layout.logical_by_physical.ptr[k];
        u32 si = p->target_to_source.ptr[ti];
        e = groove__records__RecordDescriptor__field_span(p->source, record.raw.ptr, record.raw.len, si, &span);
        if (e.code) goto fail;
        if (target->layout.fields.ptr[ti].is_static) jz_bytes_push(&raw, record.raw.ptr + span.start, span.end - span.start);
    }
    u32 next_offset = fixed_size + offset_table_size;
    if (variable_count > 1) {
        for (u32 k = 0; k < target->layout.logical_by_physical.len; k++) {
            u32 ti = target->layout.logical_by_physical.ptr[k];
            const groove_FieldLayout *l = &target->layout.fields.ptr[ti];
            if (l->is_static) continue;
            if (l->variable_idx + 1 == variable_count) break;
            e = groove__records__RecordDescriptor__field_span(p->source, record.raw.ptr, record.raw.len, p->target_to_source.ptr[ti], &span);
            if (e.code) goto fail;
            e = V(checked_add)(next_offset, span.end - span.start, &next_offset);
            if (e.code) goto fail;
            V(write_u32)(&raw, next_offset);
        }
    }
    for (u32 k = 0; k < target->layout.logical_by_physical.len; k++) {
        u32 ti = target->layout.logical_by_physical.ptr[k];
        if (target->layout.fields.ptr[ti].is_static) continue;
        e = groove__records__RecordDescriptor__field_span(p->source, record.raw.ptr, record.raw.len, p->target_to_source.ptr[ti], &span);
        if (e.code) goto fail;
        jz_bytes_push(&raw, record.raw.ptr + span.start, span.end - span.start);
    }
    *out = groove__records__OwnedRecord__new(raw, target);
    return JZ_OK;
fail:
    jz_bytes_drop(&raw);
    return e;
}

/* ===================================================================== raw projection execution */

typedef struct RawFieldsCtx {
    groove_RecordDescriptor source;
    const u8 *rec; u32 n;
    const groove_RawProjectionField *fields;
    groove_EvaluateFn evaluate; void *ectx;
} RawFieldsCtx;

static jz_err append_projected_field(void *ctx, u32 target_idx, jz_bytes *output) {
    RawFieldsCtx *c = ctx;
    const groove_RawProjectionField *field = &c->fields[target_idx];
    groove_Span span;
    switch (field->tag) {
    case GROOVE_RPF_COPY:
        JZ_TRY(groove__records__RecordDescriptor__field_span(c->source, c->rec, c->n, field->source_idx, &span));
        jz_bytes_push(output, c->rec + span.start, span.end - span.start);
        return JZ_OK;
    case GROOVE_RPF_WRAP_NULLABLE:
        jz_bytes_push_u8(output, 1);
        JZ_TRY(groove__records__RecordDescriptor__field_span(c->source, c->rec, c->n, field->source_idx, &span));
        jz_bytes_push(output, c->rec + span.start, span.end - span.start);
        return JZ_OK;
    case GROOVE_RPF_NESTED: {
        const u8 *bytes = c->rec;
        u32 len = c->n;
        for (u32 i = 0; i < field->path.len; i++) {
            JZ_TRY(groove__records__RecordDescriptor__field_span(field->path.ptr[i].descriptor, bytes, len, field->path.ptr[i].index, &span));
            bytes += span.start;
            len = span.end - span.start;
        }
        jz_bytes_push(output, bytes, len);
        return JZ_OK;
    }
    case GROOVE_RPF_ENCODED: jz_bytes_push(output, field->bytes.ptr, field->bytes.len); return JZ_OK;
    case GROOVE_RPF_EVALUATE: return c->evaluate(c->ectx, target_idx, output);
    case GROOVE_RPF_ERROR: return groove__records__Error__clone(&field->error);
    default: JZ_UNREACHABLE();
    }
}

jz_err groove__records__RecordDescriptor__project_raw_fields_into(groove_RecordDescriptor d, groove_RecordDescriptor source, const u8 *rec, u32 n,
                                                                 const groove_RawProjectionField *fields, jz_bytes *output,
                                                                 groove_EvaluateFn evaluate, void *ctx, groove_Span *out) {
    RawFieldsCtx c = {source, rec, n, fields, evaluate, ctx};
    return groove__records__RecordDescriptor__write_projected_fields_into(d, output, append_projected_field, &c, out);
}

jz_err groove__records__RecordDescriptor__write_projected_fields_into(groove_RecordDescriptor d, jz_bytes *output, groove_FieldAppendFn append, void *ctx, groove_Span *out) {
    u32 start = output->len;
    u32 fixed_size = d->layout.fixed_size, variable_count = d->layout.variable_count;
    u32 offset_table_size = sat_sub1(variable_count) * 4;
    for (u32 k = 0; k < d->layout.logical_by_physical.len; k++) {
        u32 ti = d->layout.logical_by_physical.ptr[k];
        if (d->layout.fields.ptr[ti].is_static) JZ_TRY(append(ctx, ti, output));
    }
    bytes_resize_zero(output, output->len + offset_table_size);
    for (u32 k = 0; k < d->layout.logical_by_physical.len; k++) {
        u32 ti = d->layout.logical_by_physical.ptr[k];
        const groove_FieldLayout *l = &d->layout.fields.ptr[ti];
        if (l->is_static) continue;
        JZ_TRY(append(ctx, ti, output));
        if (l->variable_idx + 1 < variable_count) {
            u32 end;
            JZ_TRY(V(usize_to_u32)(output->len - start, &end));
            put_le32(output->ptr + start + fixed_size + l->variable_idx * 4, end);
        }
    }
    out->start = start;
    out->end = output->len;
    return JZ_OK;
}

static bool any_nonzero(const u8 *p, u32 n) {
    for (u32 i = 0; i < n; i++) if (p[i]) return true;
    return false;
}

static jz_err nullable_present_payload(const u8 *rec, groove_Span span, const groove_ValueType *inner, bool *has, groove_Span *out) {
    const u8 *bytes = rec + span.start;
    u32 n = span.end - span.start;
    if (n == 0) return unexpected_eof();
    u8 flag = bytes[0];
    u32 size;
    switch (flag) {
    case 0:
        if (V(ValueType__fixed_size)(inner, &size)) {
            if (any_nonzero(bytes + 1, n - 1)) return invalid_offset();
        } else if (n - 1 != 0) {
            return invalid_offset();
        }
        *has = false;
        return JZ_OK;
    case 1:
        *has = true;
        out->start = span.start + 1;
        out->end = span.end;
        return JZ_OK;
    default: return jz_err_fmt(GROOVE_REC_E_InvalidNullFlag, "invalid nullable flag byte: %u", (unsigned)flag);
    }
}

static u32 projected_len(const groove_RawProjectedBytes *b) { return b->end - b->start; }

jz_err groove__records__RecordDescriptor__unwrap_nullable_field_into(groove_RecordDescriptor d, groove_RecordDescriptor source, const u8 *rec, u32 n, u32 field_idx,
                                                                    jz_bytes *output, groove_RawProjectionScratch *scratch, bool *has, groove_Span *out) {
    if (d->fields.len != source->fields.len) return groove__records__Error__arity_mismatch(d->fields.len, source->fields.len);
    if (field_idx >= source->fields.len) return oob(field_idx, source->fields.len);
    if (field_idx >= d->fields.len) return oob(field_idx, d->fields.len);
    const groove_DescriptorField *source_field = &source->fields.ptr[field_idx];
    const groove_DescriptorField *target_field = &d->fields.ptr[field_idx];
    const groove_ValueType *unwrap_inner = NULL;
    if (source_field->value_type.tag == GROOVE_VT_NULLABLE) {
        if (!V(ValueType__PartialEq__eq)(source_field->value_type.u.inner, &target_field->value_type))
            return groove__records__Error__type_mismatch(&target_field->value_type);
        unwrap_inner = source_field->value_type.u.inner;
    } else if (!V(ValueType__PartialEq__eq)(&source_field->value_type, &target_field->value_type)) {
        return groove__records__Error__type_mismatch(&target_field->value_type);
    }

    scratch->variable_fields.len = 0;
    scratch->generated.len = 0;
    u32 start = output->len;
    u32 fixed_size = d->layout.fixed_size, variable_count = d->layout.variable_count;
    u32 offset_table_size = sat_sub1(variable_count) * 4;
    jz_vec_reserve(output, fixed_size + offset_table_size, 1);

    for (u32 k = 0; k < d->layout.logical_by_physical.len; k++) {
        u32 ti = d->layout.logical_by_physical.ptr[k];
        groove_RawProjectedBytes bytes = {false, 0, 0};
        groove_Span span;
        if (ti == field_idx) {
            if (unwrap_inner) {
                JZ_TRY(groove__records__RecordDescriptor__field_span(source, rec, n, field_idx, &span));
                bool present;
                groove_Span payload;
                JZ_TRY(nullable_present_payload(rec, span, unwrap_inner, &present, &payload));
                if (present) {
                    bytes.start = payload.start;
                    bytes.end = payload.end;
                } else if (target_field->value_type.tag == GROOVE_VT_NULLABLE) {
                    groove_Value null_value = {.tag = GROOVE_V_NULLABLE};
                    jz_bytes encoded;
                    JZ_TRY(V(encode_value)(&null_value, &target_field->value_type, &encoded));
                    u32 gstart = scratch->generated.len;
                    jz_bytes_push(&scratch->generated, encoded.ptr, encoded.len);
                    jz_bytes_drop(&encoded);
                    bytes.generated = true;
                    bytes.start = gstart;
                    bytes.end = scratch->generated.len;
                } else {
                    output->len = start;
                    scratch->variable_fields.len = 0;
                    scratch->generated.len = 0;
                    *has = false;
                    return JZ_OK;
                }
            } else {
                JZ_TRY(groove__records__RecordDescriptor__field_span(source, rec, n, field_idx, &span));
                bytes.start = span.start;
                bytes.end = span.end;
            }
        } else {
            if (ti >= source->fields.len) return oob(ti, source->fields.len);
            if (ti >= d->fields.len) return oob(ti, d->fields.len);
            if (!V(ValueType__PartialEq__eq)(&source->fields.ptr[ti].value_type, &d->fields.ptr[ti].value_type))
                return groove__records__Error__type_mismatch(&d->fields.ptr[ti].value_type);
            JZ_TRY(groove__records__RecordDescriptor__field_span(source, rec, n, ti, &span));
            bytes.start = span.start;
            bytes.end = span.end;
        }
        if (!d->layout.fields.ptr[ti].is_static) {
            jz_vec_push(&scratch->variable_fields, &bytes, sizeof(bytes));
        } else if (bytes.generated) {
            jz_bytes_push(output, scratch->generated.ptr + bytes.start, projected_len(&bytes));
        } else {
            jz_bytes_push(output, rec + bytes.start, projected_len(&bytes));
        }
    }
    u32 next_offset = fixed_size + offset_table_size;
    for (u32 i = 0; i < sat_sub1(scratch->variable_fields.len); i++) {
        JZ_TRY(V(checked_add)(next_offset, projected_len(&scratch->variable_fields.ptr[i]), &next_offset));
        u32 v;
        JZ_TRY(V(usize_to_u32)(next_offset, &v));
        V(write_u32)(output, v);
    }
    for (u32 i = 0; i < scratch->variable_fields.len; i++) {
        const groove_RawProjectedBytes *b = &scratch->variable_fields.ptr[i];
        jz_bytes_push(output, (b->generated ? scratch->generated.ptr : rec) + b->start, projected_len(b));
    }
    *has = true;
    out->start = start;
    out->end = output->len;
    return JZ_OK;
}

/* ===================================================================== BorrowedRecord */

groove_BorrowedRecord groove__records__BorrowedRecord__new(const u8 *p, u32 n, groove_RecordDescriptor d) {
    groove_BorrowedRecord r = {{p, n}, d};
    return r;
}

jz_err groove__records__BorrowedRecord__get(const groove_BorrowedRecord *r, const char *name, u32 nlen, groove_Value *out) {
    return groove__records__RecordDescriptor__get(r->descriptor, r->raw.ptr, r->raw.len, name, nlen, out);
}

jz_err groove__records__BorrowedRecord__get_idx(const groove_BorrowedRecord *r, u32 idx, groove_Value *out) {
    return groove__records__RecordDescriptor__get_idx(r->descriptor, r->raw.ptr, r->raw.len, idx, out);
}

jz_err groove__records__BorrowedRecord__get_record(const groove_BorrowedRecord *r, u32 idx, groove_BorrowedRecord *out) {
    groove_RecordDescriptor d = r->descriptor;
    if (idx >= d->fields.len) return oob(idx, d->fields.len);
    const groove_DescriptorField *field = &d->fields.ptr[idx];
    if (field->value_type.tag != GROOVE_VT_RECORD) return groove__records__Error__type_mismatch(&field->value_type);
    groove_Span span;
    JZ_TRY(groove__records__RecordDescriptor__field_span(d, r->raw.ptr, r->raw.len, idx, &span));
    *out = groove__records__BorrowedRecord__new(r->raw.ptr + span.start, span.end - span.start, field->value_type.u.record);
    return JZ_OK;
}

static jz_err record_value_spans(const u8 *rec, u32 n, groove_RecordDescriptor d, groove_Span **out) {
    JZ_TRY(validate_record_header(rec, n, d));
    groove_Span *spans = jz_alloc((d->layout.fields.len ? d->layout.fields.len : 1) * sizeof(groove_Span));
    for (u32 i = 0; i < d->layout.fields.len; i++) {
        jz_err e = record_value_span_for_layout(rec, n, d, &d->layout.fields.ptr[i], &spans[i]);
        if (e.code) { jz_free(spans); return e; }
    }
    *out = spans;
    return JZ_OK;
}

jz_err groove__records__BorrowedRecord__to_values(const groove_BorrowedRecord *r, groove_ValueVec *out) {
    groove_Span *spans;
    JZ_TRY(record_value_spans(r->raw.ptr, r->raw.len, r->descriptor, &spans));
    groove_ValueVec values = {0};
    jz_vec_reserve(&values, r->descriptor->fields.len, sizeof(groove_Value));
    jz_err e = JZ_OK;
    for (u32 i = 0; i < r->descriptor->fields.len; i++) {
        groove_Value v;
        e = V(decode_value)(r->raw.ptr + spans[i].start, spans[i].end - spans[i].start, &r->descriptor->fields.ptr[i].value_type, &v);
        if (e.code) break;
        jz_vec_push(&values, &v, sizeof(v));
    }
    jz_free(spans);
    if (e.code) { V(ValueVec__drop)(&values); return e; }
    *out = values;
    return JZ_OK;
}

static jz_err borrowed_validate_inner(const groove_BorrowedRecord *r, bool canonical) {
    groove_RecordDescriptor d = r->descriptor;
    JZ_TRY(validate_record_header(r->raw.ptr, r->raw.len, d));
    for (u32 i = 0; i < d->fields.len; i++) {
        groove_Span span;
        JZ_TRY(record_value_span_for_layout(r->raw.ptr, r->raw.len, d, &d->layout.fields.ptr[i], &span));
        const u8 *p = r->raw.ptr + span.start;
        u32 n = span.end - span.start;
        if (canonical) JZ_TRY(V(validate_canonical_value)(p, n, &d->fields.ptr[i].value_type));
        else JZ_TRY(V(validate_value)(p, n, &d->fields.ptr[i].value_type));
    }
    return JZ_OK;
}

jz_err groove__records__BorrowedRecord__validate(const groove_BorrowedRecord *r) { return borrowed_validate_inner(r, false); }
jz_err groove__records__BorrowedRecord__validate_canonical(const groove_BorrowedRecord *r) { return borrowed_validate_inner(r, true); }

static jz_err read_exact_array(jz_slice b, u32 want) {
    if (b.len == want) return JZ_OK;
    return b.len < want ? unexpected_eof() : invalid_offset();
}

static inline u64 le64(const u8 *p) { u64 v = 0; for (int i = 7; i >= 0; i--) v = (v << 8) | p[i]; return v; }

jz_err groove__records__BorrowedRecord__field_bytes(const groove_BorrowedRecord *r, u32 idx, const groove_ValueType *expected, jz_slice *out) {
    groove_RecordDescriptor d = r->descriptor;
    if (idx >= d->fields.len) return oob(idx, d->fields.len);
    if (!V(ValueType__PartialEq__eq)(&d->fields.ptr[idx].value_type, expected)) return groove__records__Error__type_mismatch(expected);
    groove_Span span;
    JZ_TRY(groove__records__RecordDescriptor__field_span(d, r->raw.ptr, r->raw.len, idx, &span));
    out->ptr = r->raw.ptr + span.start;
    out->len = span.end - span.start;
    return JZ_OK;
}

static jz_err field_bytes_scalar(const groove_BorrowedRecord *r, u32 idx, u8 tag, jz_slice *out) {
    groove_ValueType t = V(ValueType__scalar)(tag);
    return groove__records__BorrowedRecord__field_bytes(r, idx, &t, out);
}

jz_err groove__records__BorrowedRecord__get_u64(const groove_BorrowedRecord *r, u32 idx, u64 *out) {
    jz_slice b;
    JZ_TRY(field_bytes_scalar(r, idx, GROOVE_VT_U64, &b));
    JZ_TRY(read_exact_array(b, 8));
    *out = le64(b.ptr);
    return JZ_OK;
}

jz_err groove__records__BorrowedRecord__get_i64(const groove_BorrowedRecord *r, u32 idx, i64 *out) {
    jz_slice b;
    JZ_TRY(field_bytes_scalar(r, idx, GROOVE_VT_I64, &b));
    JZ_TRY(read_exact_array(b, 8));
    *out = (i64)le64(b.ptr);
    return JZ_OK;
}

jz_err groove__records__BorrowedRecord__get_i32(const groove_BorrowedRecord *r, u32 idx, i32 *out) {
    jz_slice b;
    JZ_TRY(field_bytes_scalar(r, idx, GROOVE_VT_I32, &b));
    JZ_TRY(read_exact_array(b, 4));
    *out = (i32)get_le32(b.ptr);
    return JZ_OK;
}

static jz_err f64_from(jz_slice b, f64 *out) {
    JZ_TRY(read_exact_array(b, 8));
    u64 bits = le64(b.ptr);
    memcpy(out, &bits, 8);
    if (*out != *out) return groove__records__Error__simple(GROOVE_REC_E_InvalidF64NaN);
    return JZ_OK;
}

jz_err groove__records__BorrowedRecord__get_f64(const groove_BorrowedRecord *r, u32 idx, f64 *out) {
    jz_slice b;
    JZ_TRY(field_bytes_scalar(r, idx, GROOVE_VT_F64, &b));
    return f64_from(b, out);
}

jz_err groove__records__BorrowedRecord__get_u48(const groove_BorrowedRecord *r, u32 idx, u64 *out) {
    jz_slice b;
    JZ_TRY(field_bytes_scalar(r, idx, GROOVE_VT_U48, &b));
    JZ_TRY(read_exact_array(b, 6));
    *out = V(u48_from_le_bytes)(b.ptr);
    return JZ_OK;
}

jz_err groove__records__BorrowedRecord__get_u32(const groove_BorrowedRecord *r, u32 idx, u32 *out) {
    jz_slice b;
    JZ_TRY(field_bytes_scalar(r, idx, GROOVE_VT_U32, &b));
    JZ_TRY(read_exact_array(b, 4));
    *out = get_le32(b.ptr);
    return JZ_OK;
}

jz_err groove__records__BorrowedRecord__get_u8(const groove_BorrowedRecord *r, u32 idx, u8 *out) {
    jz_slice b;
    JZ_TRY(field_bytes_scalar(r, idx, GROOVE_VT_U8, &b));
    if (b.len == 0) return unexpected_eof();
    *out = b.ptr[0];
    return JZ_OK;
}

jz_err groove__records__BorrowedRecord__get_bool(const groove_BorrowedRecord *r, u32 idx, bool *out) {
    jz_slice b;
    JZ_TRY(field_bytes_scalar(r, idx, GROOVE_VT_BOOL, &b));
    if (b.len == 0) return unexpected_eof();
    switch (b.ptr[0]) {
    case 0: *out = false; return JZ_OK;
    case 1: *out = true; return JZ_OK;
    default: return jz_err_fmt(GROOVE_REC_E_InvalidBool, "invalid boolean byte: %u", (unsigned)b.ptr[0]);
    }
}

jz_err groove__records__BorrowedRecord__get_enum(const groove_BorrowedRecord *r, u32 idx, u8 *out) {
    groove_RecordDescriptor d = r->descriptor;
    if (idx >= d->fields.len) return oob(idx, d->fields.len);
    if (d->fields.ptr[idx].value_type.tag != GROOVE_VT_ENUM_TAG) {
        groove_ValueType u8t = V(ValueType__scalar)(GROOVE_VT_U8);
        return groove__records__Error__type_mismatch(&u8t);
    }
    groove_Span span;
    JZ_TRY(groove__records__RecordDescriptor__field_span(d, r->raw.ptr, r->raw.len, idx, &span));
    jz_slice b = {r->raw.ptr + span.start, span.end - span.start};
    JZ_TRY(read_exact_array(b, 1));
    *out = b.ptr[0];
    return JZ_OK;
}

jz_err groove__records__BorrowedRecord__get_enum_name(const groove_BorrowedRecord *r, u32 idx, const jz_str **out) {
    groove_RecordDescriptor d = r->descriptor;
    if (idx >= d->fields.len) return oob(idx, d->fields.len);
    const groove_ValueType *t = &d->fields.ptr[idx].value_type;
    if (t->tag != GROOVE_VT_ENUM_TAG) {
        groove_ValueType u8t = V(ValueType__scalar)(GROOVE_VT_U8);
        return groove__records__Error__type_mismatch(&u8t);
    }
    u8 disc;
    JZ_TRY(groove__records__BorrowedRecord__get_enum(r, idx, &disc));
    return V(ScalarEnumSchema__variant)(&t->u.enum_tag, disc, out);
}

jz_err groove__records__BorrowedRecord__get_bytes(const groove_BorrowedRecord *r, u32 idx, jz_slice *out) {
    jz_slice b;
    JZ_TRY(field_bytes_scalar(r, idx, GROOVE_VT_BYTES, &b));
    return groove__large_values__inline_scalar_bytes(GROOVE_LVK_BYTES, b.ptr, b.len, out);
}

jz_err groove__records__BorrowedRecord__get_uuid(const groove_BorrowedRecord *r, u32 idx, jz_uuid *out) {
    jz_slice b;
    JZ_TRY(field_bytes_scalar(r, idx, GROOVE_VT_UUID, &b));
    JZ_TRY(read_exact_array(b, 16));
    memcpy(out->b, b.ptr, 16);
    return JZ_OK;
}

static jz_err inline_str(jz_slice b, jz_slice *out) {
    JZ_TRY(groove__large_values__inline_scalar_bytes(GROOVE_LVK_STRING, b.ptr, b.len, out));
    if (!jz_utf8_valid(out->ptr, out->len)) return groove__records__Error__simple(GROOVE_REC_E_InvalidUtf8);
    return JZ_OK;
}

jz_err groove__records__BorrowedRecord__get_str(const groove_BorrowedRecord *r, u32 idx, jz_slice *out) {
    jz_slice b;
    JZ_TRY(field_bytes_scalar(r, idx, GROOVE_VT_STRING, &b));
    return inline_str(b, out);
}

jz_err groove__records__BorrowedRecord__nullable_field_bytes(const groove_BorrowedRecord *r, u32 idx, const groove_ValueType *inner, jz_slice *out) {
    groove_RecordDescriptor d = r->descriptor;
    if (idx >= d->fields.len) return oob(idx, d->fields.len);
    const groove_ValueType *t = &d->fields.ptr[idx].value_type;
    if (!(t->tag == GROOVE_VT_NULLABLE && V(ValueType__PartialEq__eq)(t->u.inner, inner))) {
        groove_ValueType expected;
        groove_ValueType inner_clone;
        V(ValueType__Clone__clone)(&inner_clone, inner);
        expected = V(ValueType__nullable)(inner_clone);
        jz_err e = groove__records__Error__type_mismatch(&expected);
        V(ValueType__drop)(&expected);
        return e;
    }
    groove_Span span;
    JZ_TRY(groove__records__RecordDescriptor__field_span(d, r->raw.ptr, r->raw.len, idx, &span));
    out->ptr = r->raw.ptr + span.start;
    out->len = span.end - span.start;
    return JZ_OK;
}

/* nullable_field<T>: one copy with a decode callback writing into `out`. */
typedef jz_err (*nullable_decode_fn)(jz_slice payload, void *out);
static jz_err nullable_field(const groove_BorrowedRecord *r, u32 idx, u8 inner_tag, nullable_decode_fn decode, bool *has, void *out) {
    groove_ValueType inner = V(ValueType__scalar)(inner_tag);
    jz_slice bytes;
    JZ_TRY(groove__records__BorrowedRecord__nullable_field_bytes(r, idx, &inner, &bytes));
    if (bytes.len == 0) return unexpected_eof();
    u8 flag = bytes.ptr[0];
    jz_slice payload = {bytes.ptr + 1, bytes.len - 1};
    switch (flag) {
    case 0:
        if (r->descriptor->layout.fields.ptr[idx].is_static) {
            if (any_nonzero(payload.ptr, payload.len)) return invalid_offset();
        } else if (payload.len != 0) {
            return invalid_offset();
        }
        *has = false;
        return JZ_OK;
    case 1:
        JZ_TRY(decode(payload, out));
        *has = true;
        return JZ_OK;
    default: return jz_err_fmt(GROOVE_REC_E_InvalidNullFlag, "invalid nullable flag byte: %u", (unsigned)flag);
    }
}

static jz_err dec_u48(jz_slice p, void *out) { JZ_TRY(read_exact_array(p, 6)); *(u64 *)out = V(u48_from_le_bytes)(p.ptr); return JZ_OK; }
static jz_err dec_u64(jz_slice p, void *out) { JZ_TRY(read_exact_array(p, 8)); *(u64 *)out = le64(p.ptr); return JZ_OK; }
static jz_err dec_i64(jz_slice p, void *out) { JZ_TRY(read_exact_array(p, 8)); *(i64 *)out = (i64)le64(p.ptr); return JZ_OK; }
static jz_err dec_i32(jz_slice p, void *out) { JZ_TRY(read_exact_array(p, 4)); *(i32 *)out = (i32)get_le32(p.ptr); return JZ_OK; }
static jz_err dec_f64(jz_slice p, void *out) { return f64_from(p, out); }
static jz_err dec_str(jz_slice p, void *out) { return inline_str(p, out); }
static jz_err dec_bytes(jz_slice p, void *out) { return groove__large_values__inline_scalar_bytes(GROOVE_LVK_BYTES, p.ptr, p.len, out); }
static jz_err dec_uuid(jz_slice p, void *out) { JZ_TRY(read_exact_array(p, 16)); memcpy(((jz_uuid *)out)->b, p.ptr, 16); return JZ_OK; }

jz_err groove__records__BorrowedRecord__get_nullable_u48(const groove_BorrowedRecord *r, u32 idx, bool *has, u64 *out) { return nullable_field(r, idx, GROOVE_VT_U48, dec_u48, has, out); }
jz_err groove__records__BorrowedRecord__get_nullable_u64(const groove_BorrowedRecord *r, u32 idx, bool *has, u64 *out) { return nullable_field(r, idx, GROOVE_VT_U64, dec_u64, has, out); }
jz_err groove__records__BorrowedRecord__get_nullable_i64(const groove_BorrowedRecord *r, u32 idx, bool *has, i64 *out) { return nullable_field(r, idx, GROOVE_VT_I64, dec_i64, has, out); }
jz_err groove__records__BorrowedRecord__get_nullable_i32(const groove_BorrowedRecord *r, u32 idx, bool *has, i32 *out) { return nullable_field(r, idx, GROOVE_VT_I32, dec_i32, has, out); }
jz_err groove__records__BorrowedRecord__get_nullable_f64(const groove_BorrowedRecord *r, u32 idx, bool *has, f64 *out) { return nullable_field(r, idx, GROOVE_VT_F64, dec_f64, has, out); }
jz_err groove__records__BorrowedRecord__get_nullable_string(const groove_BorrowedRecord *r, u32 idx, bool *has, jz_slice *out) { return nullable_field(r, idx, GROOVE_VT_STRING, dec_str, has, out); }
jz_err groove__records__BorrowedRecord__get_nullable_bytes(const groove_BorrowedRecord *r, u32 idx, bool *has, jz_slice *out) { return nullable_field(r, idx, GROOVE_VT_BYTES, dec_bytes, has, out); }
jz_err groove__records__BorrowedRecord__get_nullable_uuid(const groove_BorrowedRecord *r, u32 idx, bool *has, jz_uuid *out) { return nullable_field(r, idx, GROOVE_VT_UUID, dec_uuid, has, out); }

static jz_err nullable_u8_type_err(void) {
    groove_ValueType expected = V(ValueType__nullable)(V(ValueType__scalar)(GROOVE_VT_U8));
    jz_err e = groove__records__Error__type_mismatch(&expected);
    V(ValueType__drop)(&expected);
    return e;
}

jz_err groove__records__BorrowedRecord__get_nullable_enum(const groove_BorrowedRecord *r, u32 idx, bool *has, u8 *out) {
    groove_RecordDescriptor d = r->descriptor;
    if (idx >= d->fields.len) return oob(idx, d->fields.len);
    const groove_ValueType *t = &d->fields.ptr[idx].value_type;
    if (t->tag != GROOVE_VT_NULLABLE) return nullable_u8_type_err();
    if (t->u.inner->tag != GROOVE_VT_ENUM_TAG) return nullable_u8_type_err();
    groove_Span span;
    JZ_TRY(groove__records__RecordDescriptor__field_span(d, r->raw.ptr, r->raw.len, idx, &span));
    const u8 *bytes = r->raw.ptr + span.start;
    u32 n = span.end - span.start;
    if (n == 0) return unexpected_eof();
    jz_slice payload = {bytes + 1, n - 1};
    switch (bytes[0]) {
    case 0:
        if (any_nonzero(payload.ptr, payload.len)) return invalid_offset();
        *has = false;
        return JZ_OK;
    case 1:
        JZ_TRY(read_exact_array(payload, 1));
        *has = true;
        *out = payload.ptr[0];
        return JZ_OK;
    default: return jz_err_fmt(GROOVE_REC_E_InvalidNullFlag, "invalid nullable flag byte: %u", (unsigned)bytes[0]);
    }
}

jz_err groove__records__BorrowedRecord__get_array_element(const groove_BorrowedRecord *r, u32 field_idx, u32 element_idx, groove_Value *out) {
    groove_RecordDescriptor d = r->descriptor;
    if (field_idx >= d->fields.len) return oob(field_idx, d->fields.len);
    const groove_ValueType *t = &d->fields.ptr[field_idx].value_type;
    if (t->tag != GROOVE_VT_ARRAY) {
        groove_ValueType expected = V(ValueType__array_of)(V(ValueType__scalar)(GROOVE_VT_BYTES));
        jz_err e = groove__records__Error__type_mismatch(&expected);
        V(ValueType__drop)(&expected);
        return e;
    }
    const groove_ValueType *element_type = t->u.inner;
    u32 element_size;
    if (!V(ValueType__fixed_size)(element_type, &element_size)) return groove__records__Error__invalid_tuple_member(element_type);
    groove_Span span;
    JZ_TRY(groove__records__RecordDescriptor__field_span(d, r->raw.ptr, r->raw.len, field_idx, &span));
    const u8 *array = r->raw.ptr + span.start;
    u32 alen = span.end - span.start;
    if (alen % element_size != 0) return unexpected_eof();
    u32 len = alen / element_size;
    if (element_idx >= len) return oob(element_idx, len);
    u64 start64 = (u64)element_idx * element_size;
    if (start64 > UINT32_MAX) return groove__records__Error__simple(GROOVE_REC_E_LengthOverflow);
    u32 start = (u32)start64, end;
    JZ_TRY(V(checked_add)(start, element_size, &end));
    if (end > alen) return unexpected_eof();
    return V(decode_value)(array + start, element_size, element_type, out);
}

jz_err groove__records__BorrowedRecord__field(const groove_BorrowedRecord *r, u32 idx, const groove_DescriptorField **out) {
    if (idx >= r->descriptor->fields.len) return oob(idx, r->descriptor->fields.len);
    *out = &r->descriptor->fields.ptr[idx];
    return JZ_OK;
}

jz_err groove__records__BorrowedRecord__field_bytes_unchecked(const groove_BorrowedRecord *r, u32 idx, jz_slice *out) {
    const groove_DescriptorField *unused;
    JZ_TRY(groove__records__BorrowedRecord__field(r, idx, &unused));
    groove_Span span;
    JZ_TRY(groove__records__RecordDescriptor__field_span(r->descriptor, r->raw.ptr, r->raw.len, idx, &span));
    out->ptr = r->raw.ptr + span.start;
    out->len = span.end - span.start;
    return JZ_OK;
}

/* ===================================================================== projection helpers */

static jz_err project_fields_and_values(const groove_RecordDescriptor *srcs, const jz_slice *recs, u32 nsrc, u32 nrecs,
                                        const groove_IdxPair *mapping, u32 nmap, groove_DescriptorFieldVec *out_fields, groove_ValueVec *out_values) {
    if (nsrc != nrecs) return groove__records__Error__arity_mismatch(nsrc, nrecs);
    groove_DescriptorFieldVec sel_fields = {0};
    groove_ValueVec sel_values = {0};
    jz_vec_reserve(&sel_fields, nmap, sizeof(groove_DescriptorField));
    jz_vec_reserve(&sel_values, nmap, sizeof(groove_Value));
    jz_err e = JZ_OK;
    for (u32 k = 0; k < nmap; k++) {
        u32 di = mapping[k].a, fi = mapping[k].b;
        if (di >= nsrc) { e = oob(di, nsrc); break; }
        groove_RecordDescriptor d = srcs[di];
        if (fi >= d->fields.len) { e = oob(fi, d->fields.len); break; }
        groove_Value v;
        e = groove__records__RecordDescriptor__get_idx(d, recs[di].ptr, recs[di].len, fi, &v);
        if (e.code) break;
        groove_DescriptorField f;
        groove__records__DescriptorField__Clone__clone(&f, &d->fields.ptr[fi]);
        jz_vec_push(&sel_fields, &f, sizeof(f));
        jz_vec_push(&sel_values, &v, sizeof(v));
    }
    if (e.code) {
        jz_vec_drop(&sel_fields, sizeof(groove_DescriptorField), groove__records__DescriptorField__drop_v);
        V(ValueVec__drop)(&sel_values);
        return e;
    }
    /* fields/values are cloned out of `selected` (Rust clones each pair once more) */
    groove_DescriptorFieldVec fields = {0};
    groove_ValueVec values = {0};
    jz_vec_clone(&fields, &sel_fields, sizeof(groove_DescriptorField), groove__records__DescriptorField__Clone__clone_v);
    jz_vec_clone(&values, &sel_values, sizeof(groove_Value), V(Value__Clone__clone_v));
    jz_vec_drop(&sel_fields, sizeof(groove_DescriptorField), groove__records__DescriptorField__drop_v);
    V(ValueVec__drop)(&sel_values);
    groove_RecordDescriptor descriptor = groove__records__RecordDescriptor__from_logical_fields(fields);
    jz_vec_clone(out_fields, &descriptor->fields, sizeof(groove_DescriptorField), groove__records__DescriptorField__Clone__clone_v);
    *out_values = values;
    return JZ_OK;
}

static jz_err validate_record_header(const u8 *rec, u32 n, groove_RecordDescriptor d) {
    u32 fixed_size = d->layout.fixed_size, variable_count = d->layout.variable_count;
    u32 variable_start;
    JZ_TRY(V(checked_add)(fixed_size, sat_sub1(variable_count) * 4, &variable_start));
    (void)rec;
    if (n < variable_start) return unexpected_eof();
    if (variable_count == 0 && n != fixed_size) return invalid_offset();
    return JZ_OK;
}

static jz_err read_u32_at(const u8 *p, u32 n, u32 offset, u32 *out) {
    u32 end;
    JZ_TRY(V(checked_add)(offset, 4, &end));
    if (end > n) return unexpected_eof();
    *out = get_le32(p + offset);
    return JZ_OK;
}

static jz_err record_value_span_for_layout(const u8 *rec, u32 n, groove_RecordDescriptor d, const groove_FieldLayout *l, groove_Span *out) {
    if (l->is_static) {
        u32 end;
        JZ_TRY(V(checked_add)(l->offset, l->width, &end));
        if (n < end) return unexpected_eof();
        out->start = l->offset;
        out->end = end;
        return JZ_OK;
    }
    u32 fixed_size = d->layout.fixed_size, variable_count = d->layout.variable_count;
    u32 variable_start, start, end;
    JZ_TRY(V(checked_add)(fixed_size, sat_sub1(variable_count) * 4, &variable_start));
    if (l->variable_idx == 0) start = variable_start;
    else JZ_TRY(read_u32_at(rec, n, fixed_size + (l->variable_idx - 1) * 4, &start));
    if (l->variable_idx + 1 == variable_count) end = n;
    else JZ_TRY(read_u32_at(rec, n, fixed_size + l->variable_idx * 4, &end));
    if (start < variable_start || end < start || end > n) return invalid_offset();
    out->start = start;
    out->end = end;
    return JZ_OK;
}

/* ===================================================================== variant records */

groove_VariantRecord groove__records__VariantRecord__new(u32 tag, groove_OwnedRecord record) {
    groove_VariantRecord r = {tag, record};
    return r;
}

jz_err groove__records__VariantRecord__create(u32 tag, groove_RecordDescriptor d, const groove_Value *values, u32 n, groove_VariantRecord *out) {
    jz_bytes raw;
    JZ_TRY(groove__records__RecordDescriptor__create(d, values, n, &raw));
    *out = groove__records__VariantRecord__new(tag, groove__records__OwnedRecord__new(raw, d));
    return JZ_OK;
}

jz_bytes groove__records__VariantRecord__into_stored_bytes(groove_VariantRecord r) {
    jz_bytes out = groove__records__encode_variant_record(r.variant_tag, r.record.raw.ptr, r.record.raw.len);
    jz_bytes_drop(&r.record.raw);
    return out;
}

jz_err groove__records__ValidatedVariantRecord__create_with_encoded_fields(u32 tag, groove_RecordDescriptor d, u32 capacity, groove_FieldAppendFn append, void *ctx, groove_ValidatedVariantRecord *out) {
    jz_bytes raw;
    JZ_TRY(groove__records__RecordDescriptor__create_with_encoded_fields(d, capacity, append, ctx, &raw));
    out->inner = groove__records__VariantRecord__new(tag, groove__records__OwnedRecord__new(raw, d));
    return JZ_OK;
}

jz_err groove__records__ValidatedVariantRecord__create(u32 tag, groove_RecordDescriptor d, const groove_Value *values, u32 n, groove_ValidatedVariantRecord *out) {
    jz_bytes raw;
    JZ_TRY(groove__records__RecordDescriptor__create(d, values, n, &raw));
    out->inner = groove__records__VariantRecord__new(tag, groove__records__OwnedRecord__new(raw, d));
    return JZ_OK;
}

static void put_canonical_u32_varint(jz_bytes *out, u32 value) {
    while (value >= 0x80) {
        jz_bytes_push_u8(out, (u8)value | 0x80);
        value >>= 7;
    }
    jz_bytes_push_u8(out, (u8)value);
}

jz_bytes groove__records__encode_variant_record(u32 tag, const u8 *payload, u32 n) {
    jz_bytes stored = {0};
    jz_vec_reserve(&stored, GROOVE_MAX_VARIANT_TAG_LEN + n, 1);
    groove__records__append_variant_record(&stored, tag, payload, n);
    return stored;
}

void groove__records__append_variant_record(jz_bytes *stored, u32 tag, const u8 *payload, u32 n) {
    put_canonical_u32_varint(stored, tag);
    jz_bytes_push(stored, payload, n);
}

static jz_err read_canonical_u32_varint(const u8 *input, u32 n, u32 *value_out, u32 *len_out) {
    u32 value = 0;
    for (u32 index = 0; index < GROOVE_MAX_VARIANT_TAG_LEN; index++) {
        if (index >= n) return groove__records__Error__simple(GROOVE_REC_E_InvalidSchemaVersionHeader);
        u8 byte = input[index];
        u32 payload = byte & 0x7f;
        if (index == GROOVE_MAX_VARIANT_TAG_LEN - 1 && payload > 0x0f) return groove__records__Error__simple(GROOVE_REC_E_InvalidSchemaVersionHeader);
        value |= payload << (index * 7);
        if ((byte & 0x80) == 0) {
            if (index > 0 && payload == 0) return groove__records__Error__simple(GROOVE_REC_E_InvalidSchemaVersionHeader);
            *value_out = value;
            *len_out = index + 1;
            return JZ_OK;
        }
    }
    return groove__records__Error__simple(GROOVE_REC_E_InvalidSchemaVersionHeader);
}

jz_err groove__records__split_variant_record(const u8 *stored, u32 n, u32 *tag, jz_slice *payload) {
    u32 header_len;
    JZ_TRY(read_canonical_u32_varint(stored, n, tag, &header_len));
    payload->ptr = stored + header_len;
    payload->len = n - header_len;
    return JZ_OK;
}

/* ===================================================================== OwnedRecord / Record */

groove_OwnedRecord groove__records__OwnedRecord__new(jz_bytes raw, groove_RecordDescriptor d) {
    groove_OwnedRecord r = {raw, d};
    return r;
}

groove_BorrowedRecord groove__records__OwnedRecord__borrowed(const groove_OwnedRecord *r) {
    return groove__records__BorrowedRecord__new(r->raw.ptr, r->raw.len, r->descriptor);
}

jz_err groove__records__OwnedRecord__get(const groove_OwnedRecord *r, const char *name, u32 nlen, groove_Value *out) {
    groove_BorrowedRecord b = groove__records__OwnedRecord__borrowed(r);
    return groove__records__BorrowedRecord__get(&b, name, nlen, out);
}

jz_err groove__records__OwnedRecord__get_idx(const groove_OwnedRecord *r, u32 idx, groove_Value *out) {
    groove_BorrowedRecord b = groove__records__OwnedRecord__borrowed(r);
    return groove__records__BorrowedRecord__get_idx(&b, idx, out);
}

jz_err groove__records__OwnedRecord__to_values(const groove_OwnedRecord *r, groove_ValueVec *out) {
    groove_BorrowedRecord b = groove__records__OwnedRecord__borrowed(r);
    return groove__records__BorrowedRecord__to_values(&b, out);
}

jz_err groove__records__OwnedRecord__validate(const groove_OwnedRecord *r) {
    groove_BorrowedRecord b = groove__records__OwnedRecord__borrowed(r);
    return groove__records__BorrowedRecord__validate(&b);
}

void groove__records__OwnedRecord__drop(groove_OwnedRecord *r) { jz_bytes_drop(&r->raw); }

void groove__records__OwnedRecord__Clone__clone(groove_OwnedRecord *dst, const groove_OwnedRecord *src) {
    dst->raw = jz_bytes_clone(&src->raw);
    dst->descriptor = src->descriptor;
}

bool groove__records__OwnedRecord__PartialEq__eq(const groove_OwnedRecord *a, const groove_OwnedRecord *b) {
    return jz_bytes_eq(&a->raw, &b->raw) && a->descriptor == b->descriptor;
}

u64 groove__records__OwnedRecord__Hash__hash(const groove_OwnedRecord *r, u64 h) {
    h = jz_hash_combine(h, jz_hash_bytes(r->raw.ptr, r->raw.len));
    return groove__records__RecordDescriptor__Hash__hash(r->descriptor, h);
}

void groove__records__OwnedRecord__Debug__fmt(jz_str *out, const groove_OwnedRecord *r) {
    jz_str_push_cstr(out, "OwnedRecord { raw: [");
    for (u32 i = 0; i < r->raw.len; i++) jz_str_appendf(out, i ? ", %u" : "%u", (unsigned)r->raw.ptr[i]);
    jz_str_push_cstr(out, "], descriptor: ");
    groove__records__RecordDescriptor__Debug__fmt(out, r->descriptor);
    jz_str_push_cstr(out, " }");
}

/* OwnedRecordSerde { descriptor, raw } */
void groove__records__OwnedRecord__Serialize__serialize(jz_pc_w *w, const groove_OwnedRecord *r) {
    groove__records__RecordDescriptor__Serialize__serialize(w, r->descriptor);
    jz_pc_bytes(w, r->raw.ptr, r->raw.len);
}

jz_err groove__records__OwnedRecord__Deserialize__deserialize(jz_pc_r *r, groove_OwnedRecord *out) {
    JZ_TRY(groove__records__RecordDescriptor__Deserialize__deserialize(r, &out->descriptor));
    return jz_pc_r_bytes(r, &out->raw);
}

void groove__records__OwnedRecord__Serialize__serialize_json(jz_json_w *w, const groove_OwnedRecord *r) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "descriptor");
    groove__records__RecordDescriptor__Serialize__serialize_json(w, r->descriptor);
    jz_jw_key(w, "raw");
    jz_jw_begin_arr(w);
    for (u32 i = 0; i < r->raw.len; i++) jz_jw_u64(w, r->raw.ptr[i]);
    jz_jw_end_arr(w);
    jz_jw_end_obj(w);
}

jz_err groove__records__OwnedRecord__Deserialize__deserialize_json(const jz_json *j, groove_OwnedRecord *out) {
    JZ_TRY(jz_json_expect_obj(j, "struct OwnedRecordSerdeOwned"));
    const jz_json *d = jz_json_obj_get(j, "descriptor");
    if (!d) return jz_err_new(JZ_JSON_ERR_MISSING_FIELD, "missing field `descriptor`");
    JZ_TRY(groove__records__RecordDescriptor__Deserialize__deserialize_json(d, &out->descriptor));
    const jz_json *raw = jz_json_obj_get(j, "raw");
    if (!raw) return jz_err_new(JZ_JSON_ERR_MISSING_FIELD, "missing field `raw`");
    if (raw->kind != JZ_JSON_ARR) return jz_err_new(JZ_JSON_ERR_TYPE, "invalid type: expected a sequence");
    memset(&out->raw, 0, sizeof(out->raw));
    for (u32 i = 0; i < raw->v.arr.len; i++) {
        u64 b;
        if (!jz_json_as_u64(raw->v.arr.ptr[i], &b) || b > 0xff) {
            jz_bytes_drop(&out->raw);
            return jz_err_new(JZ_JSON_ERR_TYPE, "invalid value: expected u8");
        }
        jz_bytes_push_u8(&out->raw, (u8)b);
    }
    return JZ_OK;
}

jz_err groove__records__Record__get(const groove_Record *r, const char *name, u32 nlen, groove_Value *out) {
    return groove__records__RecordDescriptor__get(*r->descriptor, r->raw.ptr, r->raw.len, name, nlen, out);
}

jz_err groove__records__Record__get_idx(const groove_Record *r, u32 idx, groove_Value *out) {
    return groove__records__RecordDescriptor__get_idx(*r->descriptor, r->raw.ptr, r->raw.len, idx, out);
}

jz_err groove__records__Record__to_values(const groove_Record *r, groove_ValueVec *out) {
    groove_BorrowedRecord b = groove__records__BorrowedRecord__new(r->raw.ptr, r->raw.len, *r->descriptor);
    return groove__records__BorrowedRecord__to_values(&b, out);
}
