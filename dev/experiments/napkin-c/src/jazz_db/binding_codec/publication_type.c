/* publication_type.c: port of crates/jazz/layers/db/src/binding_codec/publication_type.rs */
#include "publication_type.h"

void jazz_db__binding_codec__publication_type__serialize(const groove_ValueType *value, jz_pc_w *w) {
    jazz_db__binding_codec__publication_type__NativeValueType__Serialize__serialize(value, w);
}

static void jazz_db__binding_codec__publication_type__NativeEnumSchema__Serialize__serialize(const groove_EnumSchema *schema, jz_pc_w *w) {
    /* struct Schema { registry_id, name, cases: Vec<Case { name, payload }> } */
    jz_pc_varint(w, schema->registry_id);
    jz_pc_str(w, &schema->name);
    jz_pc_seq(w, schema->cases.len);
    JZ_VEC_FOR(groove_EnumCase, c, schema->cases) {
        jz_pc_str(w, &c->name);
        jazz_db__binding_codec__publication_type__NativeDescriptor__Serialize__serialize(c->payload, w);
    }
}

void jazz_db__binding_codec__publication_type__NativeValueType__Serialize__serialize(const groove_ValueType *v, jz_pc_w *w) {
    switch (v->tag) {
    case GROOVE_VT_TUPLE:
        jz_pc_variant(w, 13);
        jz_pc_seq(w, v->v.tuple.len);
        JZ_VEC_FOR(groove_ValueType, m, v->v.tuple) jazz_db__binding_codec__publication_type__NativeValueType__Serialize__serialize(m, w);
        break;
    case GROOVE_VT_ARRAY:
        jz_pc_variant(w, 14);
        jazz_db__binding_codec__publication_type__NativeValueType__Serialize__serialize(v->v.inner, w);
        break;
    case GROOVE_VT_NULLABLE:
        jz_pc_variant(w, 15);
        jazz_db__binding_codec__publication_type__NativeValueType__Serialize__serialize(v->v.inner, w);
        break;
    case GROOVE_VT_RECORD:
        jz_pc_variant(w, 16);
        jazz_db__binding_codec__publication_type__NativeDescriptor__Serialize__serialize(v->v.record, w);
        break;
    case GROOVE_VT_ENUM:
        jz_pc_variant(w, 17);
        jazz_db__binding_codec__publication_type__NativeEnumSchema__Serialize__serialize(v->v.enum_schema, w);
        break;
    default:
        groove__records__values__ValueType__Serialize__serialize(v, w);
        break;
    }
}

void jazz_db__binding_codec__publication_type__NativeDescriptor__Serialize__serialize(const groove_RecordDescriptor *d, jz_pc_w *w) {
    u32 n;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(d, &n);
    jz_pc_seq(w, n);
    for (u32 i = 0; i < n; i++) {
        jz_pc_option(w, fields[i].has_name);
        if (fields[i].has_name) jz_pc_str(w, &fields[i].name);
        jazz_db__binding_codec__publication_type__NativeValueType__Serialize__serialize(&fields[i].value_type, w);
    }
}

/* take::<T>: postcard::take_from_bytes(...).map_err(|e| e.to_string()) per concrete T */
static jz_err take_err(jz_err e) {
    if (JZ_IS_ERR(e)) return jz_err_context(e, "take");
    return e;
}
static jz_err jazz_db__binding_codec__publication_type__take_usize(jz_pc_r *input, u64 *out) { return take_err(jz_pc_r_varint(input, out)); }
static jz_err jazz_db__binding_codec__publication_type__take_u32(jz_pc_r *input, u32 *out) { return take_err(jz_pc_r_u32(input, out)); }
static jz_err jazz_db__binding_codec__publication_type__take_string(jz_pc_r *input, jz_str *out) { return take_err(jz_pc_r_str(input, out)); }
static jz_err jazz_db__binding_codec__publication_type__take_option_string(jz_pc_r *input, bool *some, jz_str *out) {
    JZ_TRY(take_err(jz_pc_r_option(input, some)));
    if (*some) return take_err(jz_pc_r_str(input, out));
    return JZ_OK;
}

static jz_err jazz_db__binding_codec__publication_type__count(jz_pc_r *input, u64 budget, u64 *out) {
    u64 count;
    JZ_TRY(jazz_db__binding_codec__publication_type__take_usize(input, &count));
    if (count > budget || count > (u64)(input->end - input->p))
        return jz_err_new(0x201, "binding descriptor node limit exceeded");
    *out = count;
    return JZ_OK;
}

static jz_err jazz_db__binding_codec__publication_type__read_type(jz_pc_r *input, u64 *budget, u32 depth, groove_ValueType *out);

static void field_drop(void *p) {
    groove_DescriptorField *f = p;
    jz_str_drop(&f->name);
    jz_str_drop(&f->identity_name);
    groove__records__values__ValueType__drop(&f->value_type);
}

static jz_err jazz_db__binding_codec__publication_type__read_fields(jz_pc_r *input, u64 *budget, u32 depth, const groove_RecordDescriptor **out) {
    u64 count;
    JZ_TRY(jazz_db__binding_codec__publication_type__count(input, *budget, &count));
    JZ_VEC(groove_DescriptorField) fields = {0};
    jz_vec_reserve(&fields, (u32)count, sizeof(groove_DescriptorField));
    for (u64 i = 0; i < count; i++) {
        groove_DescriptorField f;
        memset(&f, 0, sizeof f);
        jz_err e = jazz_db__binding_codec__publication_type__take_option_string(input, &f.has_name, &f.name);
        if (!JZ_IS_ERR(e)) e = jazz_db__binding_codec__publication_type__read_type(input, budget, depth, &f.value_type);
        if (JZ_IS_ERR(e)) {
            jz_str_drop(&f.name);
            jz_vec_drop(&fields, sizeof(groove_DescriptorField), field_drop);
            return e;
        }
        if (f.has_name) {
            f.has_identity = true;
            f.identity_tag = 0; /* FieldIdentity::Name */
            f.identity_name = jz_str_clone(&f.name);
        }
        jz_vec_push(&fields, &f, sizeof f);
    }
    *out = groove__records__RecordDescriptor__new_with_fields(fields.ptr, fields.len);
    jz_free(fields.ptr);
    return JZ_OK;
}

static jz_err jazz_db__binding_codec__publication_type__read_type(jz_pc_r *input, u64 *budget, u32 depth, groove_ValueType *out) {
    if (*budget == 0 || depth >= 128) return jz_err_new(0x201, "binding descriptor node or depth limit exceeded");
    *budget -= 1;
    jz_pc_r start = *input;
    u32 tag;
    JZ_TRY(jazz_db__binding_codec__publication_type__take_u32(input, &tag));
    memset(out, 0, sizeof *out);
    switch (tag) {
    case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7: case 8: case 9: case 10: case 11: case 12:
        *input = start;
        return take_err(groove__records__values__ValueType__Deserialize__deserialize(input, out));
    case 13: {
        u64 count;
        JZ_TRY(jazz_db__binding_codec__publication_type__count(input, *budget, &count));
        out->tag = GROOVE_VT_TUPLE;
        jz_vec_reserve(&out->v.tuple, (u32)count, sizeof(groove_ValueType));
        for (u64 i = 0; i < count; i++) {
            groove_ValueType m;
            jz_err e = jazz_db__binding_codec__publication_type__read_type(input, budget, depth + 1, &m);
            if (JZ_IS_ERR(e)) { groove__records__values__ValueType__drop(out); return e; }
            jz_vec_push(&out->v.tuple, &m, sizeof m);
        }
        return JZ_OK;
    }
    case 14:
    case 15: {
        groove_ValueType *inner = JZ_NEW(groove_ValueType);
        jz_err e = jazz_db__binding_codec__publication_type__read_type(input, budget, depth + 1, inner);
        if (JZ_IS_ERR(e)) { jz_free(inner); return e; }
        out->tag = tag == 14 ? GROOVE_VT_ARRAY : GROOVE_VT_NULLABLE;
        out->v.inner = inner;
        return JZ_OK;
    }
    case 16: {
        const groove_RecordDescriptor *d;
        JZ_TRY(jazz_db__binding_codec__publication_type__read_fields(input, budget, depth + 1, &d));
        out->tag = GROOVE_VT_RECORD;
        out->v.record = (groove_RecordDescriptor *)d;
        return JZ_OK;
    }
    case 17: {
        u64 registry_id;
        jz_str name = {0};
        u64 count;
        JZ_TRY(jazz_db__binding_codec__publication_type__take_usize(input, &registry_id));
        JZ_TRY(jazz_db__binding_codec__publication_type__take_string(input, &name));
        jz_err e = jazz_db__binding_codec__publication_type__count(input, *budget, &count);
        if (JZ_IS_ERR(e)) { jz_str_drop(&name); return e; }
        JZ_VEC(groove_EnumCase) cases = {0};
        jz_vec_reserve(&cases, (u32)count, sizeof(groove_EnumCase));
        for (u64 i = 0; i < count; i++) {
            if (*budget == 0) { e = jz_err_new(0x201, "binding descriptor node limit exceeded"); goto fail; }
            *budget -= 1;
            jz_str case_name = {0};
            e = jazz_db__binding_codec__publication_type__take_string(input, &case_name);
            if (JZ_IS_ERR(e)) goto fail;
            const groove_RecordDescriptor *payload;
            e = jazz_db__binding_codec__publication_type__read_fields(input, budget, depth + 1, &payload);
            if (JZ_IS_ERR(e)) { jz_str_drop(&case_name); goto fail; }
            groove_EnumCase c = groove__records__values__EnumCase__new(case_name, payload);
            jz_vec_push(&cases, &c, sizeof c);
        }
        {
            groove_EnumSchema *schema;
            e = groove__records__values__EnumSchema__new(name, cases.ptr, cases.len, &schema);
            jz_free(cases.ptr);
            if (JZ_IS_ERR(e)) return e; /* error.to_string() */
            schema->registry_id = registry_id; /* with_registry_id */
            out->tag = GROOVE_VT_ENUM;
            out->v.enum_schema = schema;
            return JZ_OK;
        }
    fail:
        for (u32 j = 0; j < cases.len; j++) jz_str_drop(&cases.ptr[j].name);
        jz_free(cases.ptr);
        jz_str_drop(&name);
        return e;
    }
    default:
        return jz_err_new(0x201, "unknown binding value type");
    }
}

jz_err jazz_db__binding_codec__publication_type__read_descriptor(jz_pc_r *input, const groove_RecordDescriptor **out) {
    u64 budget = 1024;
    return jazz_db__binding_codec__publication_type__read_fields(input, &budget, 0, out);
}
