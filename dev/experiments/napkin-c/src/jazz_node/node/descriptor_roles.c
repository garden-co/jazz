/* Port of crates/jazz/layers/node/src/node/descriptor_roles.rs */
#include <stdio.h>
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

static const u8 RESULT_DESCRIPTOR_MAGIC[5] = { 'J', 'R', 'P', 'D', 0x01 };

typedef enum { JZN_ROLE_CURRENT = 0, JZN_ROLE_AGGREGATE = 1 } jazz_node_ResultDescriptorRole;

typedef struct jazz_node_PreparedAggregatePayload {
    jz_bytes descriptor;
    jz_bytes record;
    jz_bytes row_identity;
    jz_bytes replacement_identity;
} jazz_node_PreparedAggregatePayload;

typedef struct jazz_node_CurrentPayloadEncodePlan {
    groove_RecordProjector *projector;
    jz_bytes descriptor;
} jazz_node_CurrentPayloadEncodePlan;

static void value_drop_v(void *p) { groove__records__values__Value__drop(p); }
static void values_drop(groove_ValueVec *v) { jz_vec_drop(v, sizeof(groove_Value), value_drop_v); }
static void fields_drop(groove_DescriptorFieldVec *v) { jz_vec_drop(v, sizeof(groove_DescriptorField), groove__records__DescriptorField__drop); }
static void pubs_drop(jazz_engine_CurrentRowPublicationFieldVec *v) {
    jz_vec_drop(v, sizeof(jazz_engine_CurrentRowPublicationField), jazz_engine__query_engine__publication__CurrentRowPublicationField__drop);
}
static void push_field(groove_DescriptorFieldVec *fields, jz_str name, groove_ValueType ty) {
    groove_DescriptorField f;
    groove__records__DescriptorField__new(&f, name, ty);
    JZ_VEC_PUSH(*fields, f);
}
static groove_ValueType vt(u32 tag) { groove_ValueType t = { tag }; return t; }
static groove_ValueType vt_clone(const groove_ValueType *t) { groove_ValueType o; groove__records__values__ValueType__clone(&o, t); return o; }
static bool slice_eq(jz_slice a, jz_slice b) { return jz_slice_cmp(a, b) == 0; }
static bool value_is_uuid(const groove_Value *v, const jz_uuid *u) { return v->tag == GROOVE_V_UUID && jz_uuid_eq(&v->v.uuid, u); }
static jz_err invalid(const char *m) { return JZN_INVALID_STORED(m); }

static jz_err validate_role_fields(jazz_node_ResultDescriptorRole role, const groove_RecordDescriptor *descriptor);

jz_err jazz_node__node__descriptor_roles__encode_result_descriptor(jazz_node_ResultDescriptorRole role, const groove_RecordDescriptor *descriptor, jz_bytes *out) {
    JZ_TRY(validate_role_fields(role, descriptor));
    jz_bytes bytes = jz_bytes_from(RESULT_DESCRIPTOR_MAGIC, 5);
    jz_bytes_push_u8(&bytes, (u8)role);
    jz_bytes persisted = { 0 };
    jz_err e = groove__records__values__encode_persisted_record_descriptor(descriptor, &persisted);
    if (e.code) { jz_bytes_drop(&bytes); return e; }
    jz_bytes_push(&bytes, persisted.ptr, persisted.len);
    jz_bytes_drop(&persisted);
    *out = bytes;
    return JZ_OK;
}

/* name.splitn(3, '/') -> (tag, ordinal, logical_name); false when fewer than 3 parts. */
static bool split3(const jz_str *name, jz_slice *tag, jz_slice *ordinal, jz_slice *rest) {
    const u8 *p = (const u8 *)name->ptr, *end = p + name->len;
    const u8 *a = memchr(p, '/', name->len);
    if (!a) return false;
    const u8 *b = memchr(a + 1, '/', (usize)(end - (a + 1)));
    if (!b) return false;
    *tag = jz_slice_of(p, (u32)(a - p));
    *ordinal = jz_slice_of(a + 1, (u32)(b - (a + 1)));
    *rest = jz_slice_of(b + 1, (u32)(end - (b + 1)));
    return true;
}
static bool slice_is(jz_slice s, const char *lit) { return s.len == strlen(lit) && memcmp(s.ptr, lit, s.len) == 0; }
/* ordinal.parse::<usize>() */
static bool parse_usize(jz_slice s, u32 *out) {
    if (s.len == 0) return false;
    u64 v = 0;
    u32 i = 0;
    if (s.ptr[0] == '+') { i = 1; if (s.len == 1) return false; }
    for (; i < s.len; i++) {
        if (s.ptr[i] < '0' || s.ptr[i] > '9') return false;
        v = v * 10 + (u64)(s.ptr[i] - '0');
        if (v > 0xffffffffu) return false;
    }
    *out = (u32)v;
    return true;
}

static jz_err validate_role_fields(jazz_node_ResultDescriptorRole role, const groove_RecordDescriptor *descriptor) {
    const char *msg = "result descriptor field roles are not canonical";
    u32 group_ordinal = 0, value_ordinal = 0, occurrence_ordinal = 1;
    bool values_started = false;
    bool has_last_union = false;
    u32 last_union_position = 0;
    const groove_DescriptorFieldVec *all = groove__records__RecordDescriptor__fields(descriptor);
    const groove_DescriptorField *fields = all->ptr;
    u32 nfields = all->len;
    if (role == JZN_ROLE_CURRENT) {
        if (nfields == 0) return invalid(msg);
        const groove_DescriptorField *first = &fields[0];
        if (!first->has_name || !jz_str_eq_cstr(&first->name, "metadata/row_uuid") || first->value_type.tag != GROOVE_VT_UUID)
            return invalid(msg);
        fields++;
        nfields--;
    }
    for (u32 current_ordinal = 0; current_ordinal < nfields; current_ordinal++) {
        const groove_DescriptorField *field = &fields[current_ordinal];
        if (!field->has_name) return invalid(msg);
        jz_slice tag, ordinal, logical_name;
        if (!split3(&field->name, &tag, &ordinal, &logical_name)) return invalid(msg);
        (void)logical_name;
        u32 expected;
        if (role == JZN_ROLE_CURRENT
            && (slice_is(tag, "source") || slice_is(tag, "result") || slice_is(tag, "provenance") || slice_is(tag, "metadata"))
            && !values_started) {
            expected = current_ordinal;
        } else if (role == JZN_ROLE_CURRENT && slice_is(tag, "occurrence") && !has_last_union) {
            if (field->value_type.tag != GROOVE_VT_UUID) return invalid(msg);
            values_started = true;
            expected = occurrence_ordinal++;
        } else if (role == JZN_ROLE_CURRENT && slice_is(tag, "occurrence_union")) {
            u32 position;
            if (!parse_usize(ordinal, &position)) return invalid(msg);
            if (field->value_type.tag != GROOVE_VT_STRING || position >= occurrence_ordinal
                || (has_last_union && position <= last_union_position))
                return invalid(msg);
            has_last_union = true;
            last_union_position = position;
            expected = position;
        } else if (role == JZN_ROLE_AGGREGATE && slice_is(tag, "group") && !values_started) {
            expected = group_ordinal++;
        } else if (role == JZN_ROLE_AGGREGATE && slice_is(tag, "value")) {
            values_started = true;
            expected = value_ordinal++;
        } else {
            return invalid(msg);
        }
        char buf[16];
        int n = snprintf(buf, sizeof buf, "%u", expected);
        if (ordinal.len != (u32)n || memcmp(ordinal.ptr, buf, (usize)n) != 0) return invalid(msg);
    }
    return JZ_OK;
}

/* decode(encode(descriptor)): canonicalize through the persisted codec. Consumes `descriptor`. */
static jz_err persisted_roundtrip(groove_RecordDescriptor *descriptor, groove_RecordDescriptor **out) {
    jz_bytes enc = { 0 };
    jz_err e = groove__records__values__encode_persisted_record_descriptor(descriptor, &enc);
    groove__records__RecordDescriptor__drop_box(descriptor);
    if (e.code) return e;
    e = groove__records__values__decode_persisted_record_descriptor(jz_bytes_slice(&enc), out);
    jz_bytes_drop(&enc);
    return e;
}

jz_err jazz_node__node__descriptor_roles__aggregate_role_schema(const jazz_engine_AggregateResultSchema *schema, groove_RecordDescriptor **out) {
    if (schema->group_names.len != schema->group_key_fields.len || schema->value_names.len != schema->value_fields.len)
        return invalid("aggregate role schema has inconsistent identity counts");
    groove_DescriptorFieldVec fields = { 0 };
    struct { const char *role; const jzn_strvec *names; const groove_DescriptorFieldVec *source; } parts[2] = {
        { "group", &schema->group_names, &schema->group_key_fields },
        { "value", &schema->value_names, &schema->value_fields },
    };
    for (int k = 0; k < 2; k++) {
        u32 n = parts[k].names->len < parts[k].source->len ? parts[k].names->len : parts[k].source->len;
        for (u32 ordinal = 0; ordinal < n; ordinal++) {
            const jz_str *name = &parts[k].names->ptr[ordinal];
            jz_str fname = jz_format("%s/%u/%.*s", parts[k].role, ordinal, (int)name->len, name->ptr);
            push_field(&fields, fname, vt_clone(&parts[k].source->ptr[ordinal].value_type));
        }
    }
    groove_RecordDescriptor *descriptor = groove__records__RecordDescriptor__new_with_fields(fields);
    return persisted_roundtrip(descriptor, out);
}

jz_err jazz_node__node__descriptor_roles__aggregate_row_identity(const jazz_engine_AggregateResultSchema *schema, const groove_RecordDescriptor *canonical,
                                                                 const groove_ValueVec *values, jz_bytes *out) {
    switch (schema->group_key_fields.len) {
    case 0: {
        groove_Value g = { GROOVE_V_STRING };
        g.v.s = jz_str_from("global");
        groove_ValueType st = vt(GROOVE_VT_STRING);
        jz_err e = jazz_node__node__codec__runtime_result_identity_bytes(&g, &st, out);
        jz_str_drop(&g.v.s);
        return e;
    }
    case 1:
        return jazz_node__node__codec__runtime_result_identity_bytes(&values->ptr[0], &groove__records__RecordDescriptor__fields(canonical)->ptr[0].value_type, out);
    default:
        return invalid("aggregate payload has unsupported group identity");
    }
}

static void hash_len_prefixed(jz_blake3_hasher *h, jz_slice s) {
    u64 n = s.len;
    u8 le[8];
    for (int i = 0; i < 8; i++) le[i] = (u8)(n >> (8 * i));
    jz_blake3_update(h, le, 8);
    jz_blake3_update(h, s.ptr, s.len);
}

jz_err jazz_node__node__descriptor_roles__aggregate_replacement_identity(const jazz_engine_AggregateResultSchema *schema, const groove_RecordDescriptor *canonical,
                                                                         const groove_ValueVec *values, jz_slice descriptor, jz_slice record, jz_bytes *out) {
    switch (schema->value_fields.len) {
    case 0: {
        groove_Value g = { GROOVE_V_STRING };
        g.v.s = jz_str_from("empty");
        groove_ValueType st = vt(GROOVE_VT_STRING);
        jz_err e = jazz_node__node__codec__runtime_result_identity_bytes(&g, &st, out);
        jz_str_drop(&g.v.s);
        return e;
    }
    case 1: {
        u32 index = schema->group_key_fields.len;
        return jazz_node__node__codec__runtime_result_identity_bytes(&values->ptr[index], &groove__records__RecordDescriptor__fields(canonical)->ptr[index].value_type, out);
    }
    default: {
        jz_blake3_hasher *h = jz_blake3_new();
        static const char tag[] = "jazz aggregate replacement v1";
        jz_blake3_update(h, (const u8 *)tag, sizeof tag - 1);
        hash_len_prefixed(h, descriptor);
        hash_len_prefixed(h, record);
        u8 digest[32];
        jz_blake3_finalize(h, digest);
        jz_blake3_free(h);
        groove_Value b = { GROOVE_V_BYTES };
        b.v.bytes = jz_bytes_from(digest, 32);
        groove_ValueType bt = vt(GROOVE_VT_BYTES);
        jz_err e = jazz_node__node__codec__runtime_result_identity_bytes(&b, &bt, out);
        jz_bytes_drop(&b.v.bytes);
        return e;
    }
    }
}

/* canonical.bind(raw).to_values() and canonical.create(values) == raw */
static jz_err canonical_values(const groove_RecordDescriptor *canonical, jz_slice raw, const char *noncanonical, groove_ValueVec *out) {
    groove_ValueVec values = { 0 };
    JZ_TRY(groove__records__RecordDescriptor__bind_to_values(canonical, raw, &values));
    jz_bytes again = { 0 };
    jz_err e = groove__records__RecordDescriptor__create(canonical, values.ptr, values.len, &again);
    if (e.code) { values_drop(&values); return e; }
    bool same = slice_eq(jz_bytes_slice(&again), raw);
    jz_bytes_drop(&again);
    if (!same) { values_drop(&values); return invalid(noncanonical); }
    *out = values;
    return JZ_OK;
}

void jazz_node__node__descriptor_roles__PreparedAggregatePayload__drop(jazz_node_PreparedAggregatePayload *p) {
    jz_bytes_drop(&p->descriptor); jz_bytes_drop(&p->record); jz_bytes_drop(&p->row_identity); jz_bytes_drop(&p->replacement_identity);
}

jz_err jazz_node__node__descriptor_roles__encode_aggregate_payload_record(const groove_BorrowedRecord *record, const jazz_engine_AggregateResultSchema *schema,
                                                                          jazz_node_PreparedAggregatePayload *out) {
    groove_DescriptorFieldVec runtime_fields = { 0 };
    groove_ValueVec values = { 0 };
    jz_err e = JZ_OK;
    const groove_RecordDescriptor *rd = groove__records__BorrowedRecord__descriptor(record);
    const groove_DescriptorFieldVec *sets[2] = { &schema->group_key_fields, &schema->value_fields };
    for (int k = 0; k < 2; k++) {
        for (u32 i = 0; i < sets[k]->len; i++) {
            const groove_DescriptorField *field = &sets[k]->ptr[i];
            if (field->identity_tag == 0) { e = invalid("aggregate role field has no execution binding"); goto fail; }
            u32 index;
            if (!groove__records__RecordDescriptor__field_index_by_identity(rd, field, &index)) {
                e = invalid("aggregate record is missing its compiled role field");
                goto fail;
            }
            if (!groove__records__values__ValueType__eq(&groove__records__RecordDescriptor__fields(rd)->ptr[index].value_type, &field->value_type)) {
                e = invalid("aggregate role field type differs from compiler schema");
                goto fail;
            }
            groove_Value v;
            e = groove__records__BorrowedRecord__get_idx_borrowed(record, index, &v);
            if (e.code) goto fail;
            JZ_VEC_PUSH(values, v);
            groove_DescriptorField c;
            groove__records__DescriptorField__clone(&c, field);
            JZ_VEC_PUSH(runtime_fields, c);
        }
    }
    groove_RecordDescriptor *runtime = groove__records__RecordDescriptor__new_with_fields(runtime_fields);
    runtime_fields = (groove_DescriptorFieldVec){ 0 };
    jz_bytes raw = { 0 };
    e = groove__records__RecordDescriptor__create(runtime, values.ptr, values.len, &raw);
    groove__records__RecordDescriptor__drop_box(runtime);
    values_drop(&values);
    if (e.code) return e;
    groove_RecordDescriptor *canonical = NULL;
    e = jazz_node__node__descriptor_roles__aggregate_role_schema(schema, &canonical);
    if (e.code) { jz_bytes_drop(&raw); return e; }
    groove_ValueVec cvals = { 0 };
    jazz_node_PreparedAggregatePayload p = { 0 };
    e = canonical_values(canonical, jz_bytes_slice(&raw), "aggregate payload row is not canonical", &cvals);
    if (e.code) goto out;
    e = jazz_node__node__descriptor_roles__encode_result_descriptor(JZN_ROLE_AGGREGATE, canonical, &p.descriptor);
    if (e.code) goto out;
    e = jazz_node__node__descriptor_roles__aggregate_row_identity(schema, canonical, &cvals, &p.row_identity);
    if (e.code) goto out;
    e = jazz_node__node__descriptor_roles__aggregate_replacement_identity(schema, canonical, &cvals, jz_bytes_slice(&p.descriptor), jz_bytes_slice(&raw), &p.replacement_identity);
    if (e.code) goto out;
    p.record = raw;
    raw = (jz_bytes){ 0 };
    *out = p;
    p = (jazz_node_PreparedAggregatePayload){ 0 };
out:
    jazz_node__node__descriptor_roles__PreparedAggregatePayload__drop(&p);
    jz_bytes_drop(&raw);
    values_drop(&cvals);
    groove__records__RecordDescriptor__drop_box(canonical);
    return e;
fail:
    fields_drop(&runtime_fields);
    values_drop(&values);
    return e;
}

jz_err jazz_node__node__descriptor_roles__decode_aggregate_payload_record(const jazz_protocol_ResultMemberPayloadEntry *payload, const jazz_engine_AggregateResultSchema *schema,
                                                                          groove_OwnedRecord **out) {
    groove_RecordDescriptor *canonical = NULL;
    JZ_TRY(jazz_node__node__descriptor_roles__aggregate_role_schema(schema, &canonical));
    jz_bytes enc = { 0 }, row_id = { 0 }, repl_id = { 0 };
    groove_ValueVec values = { 0 };
    jz_err e = jazz_node__node__descriptor_roles__encode_result_descriptor(JZN_ROLE_AGGREGATE, canonical, &enc);
    if (e.code) goto out;
    if (!jz_bytes_eq(&enc, &payload->descriptor)) { e = invalid("aggregate payload role schema differs from compiled output"); goto out; }
    e = canonical_values(canonical, jz_bytes_slice(&payload->record), "aggregate payload row is not canonical", &values);
    if (e.code) goto out;
    const jz_bytes *row;
    const jazz_protocol_SyntheticReplacementToken *replacement;
    if (!jazz_protocol__protocol__ResultMemberEntry__as_synthetic(&payload->member, &row, &replacement)) {
        e = invalid("aggregate payload has no synthetic member identity");
        goto out;
    }
    e = jazz_node__node__descriptor_roles__aggregate_row_identity(schema, canonical, &values, &row_id);
    if (e.code) goto out;
    if (!jz_bytes_eq(&row_id, row)) { e = invalid("aggregate payload differs from its member identity"); goto out; }
    e = jazz_node__node__descriptor_roles__aggregate_replacement_identity(schema, canonical, &values, jz_bytes_slice(&payload->descriptor),
                                                                          jz_bytes_slice(&payload->record), &repl_id);
    if (e.code) goto out;
    if (!slice_eq(jz_bytes_slice(&repl_id), jazz_protocol__protocol__SyntheticReplacementToken__encoded_record(replacement))) {
        e = invalid("aggregate payload differs from its member identity");
        goto out;
    }
    {
        groove_DescriptorFieldVec fields = { 0 };
        const groove_DescriptorFieldVec *sets[2] = { &schema->group_key_fields, &schema->value_fields };
        for (int k = 0; k < 2; k++)
            for (u32 i = 0; i < sets[k]->len; i++) {
                groove_DescriptorField c;
                groove__records__DescriptorField__clone(&c, &sets[k]->ptr[i]);
                JZ_VEC_PUSH(fields, c);
            }
        groove_RecordDescriptor *runtime = groove__records__RecordDescriptor__new_with_fields(fields);
        *out = groove__records__OwnedRecord__new(jz_bytes_clone(&payload->record), runtime);
    }
out:
    jz_bytes_drop(&enc); jz_bytes_drop(&row_id); jz_bytes_drop(&repl_id);
    values_drop(&values);
    groove__records__RecordDescriptor__drop_box(canonical);
    return e;
}

static int cmp_strp(const void *a, const void *b, void *c) { (void)c; return jz_str_cmp(*(const jz_str *const *)a, *(const jz_str *const *)b); }
static const jz_btree_desc desc_strp_set = { sizeof(const jz_str *), 0, cmp_strp, NULL, NULL };

jz_err jazz_node__node__descriptor_roles__current_role_schema(const jazz_engine_ResultMembershipSchema *schema, groove_RecordDescriptor **out) {
    jz_btree unique_sources;
    jz_btree_init(&unique_sources, &desc_strp_set);
    for (u32 i = 0; i < schema->occurrence_id_fields.len; i++) {
        const jz_str *p = &schema->occurrence_id_fields.ptr[i];
        jz_btree_insert(&unique_sources, &p, NULL, NULL);
    }
    bool bad_union = false;
    {
        const u32 *pos; const jz_str *nm;
        JZ_BTREE_FOR(&schema->occurrence_union_arm_fields, pos, nm) {
            (void)nm;
            if (*pos >= schema->occurrence_id_fields.len) { bad_union = true; break; }
        }
    }
    usize unique = unique_sources.len;
    jz_btree_drop(&unique_sources);
    if (schema->occurrence_id_fields.len == 0 || !jz_str_eq(&schema->occurrence_id_fields.ptr[0], &schema->row_field)
        || unique != schema->occurrence_id_fields.len || bad_union)
        return invalid("result occurrence source declarations are invalid");

    groove_DescriptorFieldVec fields = { 0 };
    push_field(&fields, jz_str_from("metadata/row_uuid"), vt(GROOVE_VT_UUID));
    u32 ordinal = 0;
    for (u32 i = 0; i < schema->payload_fields.len; i++) {
        const jazz_engine_TypedOutputField *field = &schema->payload_fields.ptr[i];
        if (jz_str_eq(&field->name, &schema->row_field)) continue;
        const jazz_engine_CurrentRowPublicationField *binding = jz_btree_get(&schema->payload_publication_fields, &field->name);
        if (!binding) { fields_drop(&fields); return invalid("result payload field has no publication identity"); }
        const char *role;
        switch (binding->tag) {
        case JAZZ_PUBF_STORED_COLUMN:
        case JAZZ_PUBF_UNRESOLVED_SOURCE_CELL: role = "source"; break;
        default:
            role = binding->visibility == JAZZ_VIS_APPLICATION_CELL ? "result"
                 : binding->visibility == JAZZ_VIS_PUBLIC_PROVENANCE ? "provenance" : "metadata";
        }
        push_field(&fields, jz_format("%s/%u/%.*s", role, ordinal, (int)binding->name.len, binding->name.ptr), vt_clone(&field->ty));
        ordinal++;
    }
    for (u32 o = 1; o < schema->occurrence_id_fields.len; o++) {
        const jz_str *name = &schema->occurrence_id_fields.ptr[o];
        push_field(&fields, jz_format("occurrence/%u/%.*s", o, (int)name->len, name->ptr), vt(GROOVE_VT_UUID));
    }
    const u32 *posp; const jz_str *name;
    JZ_BTREE_FOR(&schema->occurrence_union_arm_fields, posp, name) {
        u32 position = *posp;
        push_field(&fields, jz_format("occurrence_union/%u/%.*s", position, (int)name->len, name->ptr), vt(GROOVE_VT_STRING));
    }
    groove_RecordDescriptor *descriptor = groove__records__RecordDescriptor__new_with_fields(fields);
    return persisted_roundtrip(descriptor, out);
}

/* Consumes `descriptor`. */
jz_err jazz_node__node__descriptor_roles__CurrentPayloadEncodePlan__new(groove_RecordDescriptor *descriptor, const jazz_engine_ResultMembershipSchema *schema,
                                                                        jazz_node_CurrentPayloadEncodePlan *out) {
    /* selected = once(row_field) ++ payload (≠ row_field) ++ occurrence_id_fields.skip(1) ++ union arm names */
    JZ_VEC(const jz_str *) selected = { 0 };
    JZ_VEC_PUSH(selected, &schema->row_field);
    for (u32 i = 0; i < schema->payload_fields.len; i++)
        if (!jz_str_eq(&schema->payload_fields.ptr[i].name, &schema->row_field)) JZ_VEC_PUSH(selected, (const jz_str *)&schema->payload_fields.ptr[i].name);
    for (u32 i = 1; i < schema->occurrence_id_fields.len; i++) JZ_VEC_PUSH(selected, (const jz_str *)&schema->occurrence_id_fields.ptr[i]);
    {
        const u32 *pos; const jz_str *nm;
        JZ_BTREE_FOR(&schema->occurrence_union_arm_fields, pos, nm) { (void)pos; JZ_VEC_PUSH(selected, nm); }
    }

    jz_usize_pair_vec mapping = { 0 };
    groove_DescriptorFieldVec runtime_fields = { 0 };
    groove_RecordDescriptor *canonical = NULL;
    jz_err e = JZ_OK;
    const groove_DescriptorFieldVec *dfields = groove__records__RecordDescriptor__fields(descriptor);
    for (u32 s = 0; s < selected.len; s++) {
        const jz_str *name = selected.ptr[s];
        u32 index = 0;
        bool found = false;
        for (u32 i = 0; i < dfields->len; i++)
            if (dfields->ptr[i].has_name && jz_str_eq(&dfields->ptr[i].name, name)) { index = i; found = true; break; }
        if (!found) { e = invalid("result payload is missing its declared carrier"); goto fail; }
        for (u32 i = index + 1; i < dfields->len; i++)
            if (dfields->ptr[i].has_name && jz_str_eq(&dfields->ptr[i].name, name)) { e = invalid("result payload carrier binding is ambiguous"); goto fail; }
        jz_usize_pair m = { index, mapping.len };
        JZ_VEC_PUSH(mapping, m);
        groove_DescriptorField c;
        groove__records__DescriptorField__clone(&c, &dfields->ptr[index]);
        JZ_VEC_PUSH(runtime_fields, c);
    }
    {
        groove_RecordDescriptor *runtime = groove__records__RecordDescriptor__new_with_fields(runtime_fields);
        runtime_fields = (groove_DescriptorFieldVec){ 0 };
        e = jazz_node__node__descriptor_roles__current_role_schema(schema, &canonical);
        if (e.code) { groove__records__RecordDescriptor__drop_box(runtime); goto fail; }
        const groove_DescriptorFieldVec *rf = groove__records__RecordDescriptor__fields(runtime);
        const groove_DescriptorFieldVec *cf = groove__records__RecordDescriptor__fields(canonical);
        u32 n = rf->len < cf->len ? rf->len : cf->len;
        for (u32 i = 0; i < n && !e.code; i++) {
            static const char *const vname[] = { "value" };
            groove_ValueType st = vt_clone(&rf->ptr[i].value_type), tt = vt_clone(&cf->ptr[i].value_type);
            groove_RecordDescriptor *source_type = groove__records__RecordDescriptor__new(vname, &st, 1);
            groove_RecordDescriptor *target_type = groove__records__RecordDescriptor__new(vname, &tt, 1);
            jz_bytes a = { 0 }, b = { 0 };
            e = groove__records__values__encode_persisted_record_descriptor(source_type, &a);
            if (!e.code) e = groove__records__values__encode_persisted_record_descriptor(target_type, &b);
            if (!e.code && !jz_bytes_eq(&a, &b)) e = invalid("result payload type differs from its publication schema");
            jz_bytes_drop(&a); jz_bytes_drop(&b);
            groove__records__RecordDescriptor__drop_box(source_type);
            groove__records__RecordDescriptor__drop_box(target_type);
        }
        groove__records__RecordDescriptor__drop_box(runtime);
        if (e.code) goto fail;
    }
    {
        jazz_node_CurrentPayloadEncodePlan plan = { 0 };
        e = jazz_node__node__descriptor_roles__encode_result_descriptor(JZN_ROLE_CURRENT, canonical, &plan.descriptor);
        if (e.code) goto fail;
        e = groove__records__RecordProjector__new_registry_rebound(descriptor, canonical, mapping, &plan.projector);
        descriptor = NULL; canonical = NULL; mapping = (jz_usize_pair_vec){ 0 };
        if (e.code) { jz_bytes_drop(&plan.descriptor); goto fail; }
        *out = plan;
        jz_vec_drop(&selected, sizeof(const jz_str *), NULL);
        return JZ_OK;
    }
fail:
    jz_vec_drop(&selected, sizeof(const jz_str *), NULL);
    jz_vec_drop(&mapping, sizeof(jz_usize_pair), NULL);
    fields_drop(&runtime_fields);
    if (canonical) groove__records__RecordDescriptor__drop_box(canonical);
    if (descriptor) groove__records__RecordDescriptor__drop_box(descriptor);
    return e;
}

jz_err jazz_node__node__descriptor_roles__CurrentPayloadEncodePlan__encode(const jazz_node_CurrentPayloadEncodePlan *self, const groove_BorrowedRecord *record,
                                                                           jz_bytes *descriptor_out, jz_bytes *record_out) {
    groove_OwnedRecord *projected = NULL;
    JZ_TRY(groove__records__RecordProjector__project(self->projector, record, &projected));
    *descriptor_out = jz_bytes_clone(&self->descriptor);
    *record_out = groove__records__OwnedRecord__into_raw(projected);
    return JZ_OK;
}

void jazz_node__node__descriptor_roles__CurrentPayloadEncodePlan__drop(jazz_node_CurrentPayloadEncodePlan *p) {
    groove__records__RecordProjector__drop(p->projector);
    jz_bytes_drop(&p->descriptor);
}

static jazz_engine_CurrentRowPublicationField hidden_metadata(jz_str name) {
    jazz_engine_CurrentRowPublicationField f;
    memset(&f, 0, sizeof f);
    f.tag = JAZZ_PUBF_RESULT_FIELD;
    f.name = name;
    f.visibility = JAZZ_VIS_HIDDEN_METADATA;
    return f;
}

jz_err jazz_node__node__descriptor_roles__decode_current_payload_record(const jz_str *table, const jazz_protocol_ResultMemberPayloadEntry *payload,
                                                                        const jazz_engine_ResultMembershipSchema *schema, jazz_node_CurrentRow *out) {
    groove_RecordDescriptor *canonical = NULL;
    JZ_TRY(jazz_node__node__descriptor_roles__current_role_schema(schema, &canonical));
    jz_bytes enc = { 0 };
    groove_ValueVec values = { 0 };
    groove_DescriptorFieldVec fields = { 0 };
    jazz_engine_CurrentRowPublicationFieldVec publications = { 0 };
    jazz_types_OutputOccurrenceId *member_occurrence = NULL;
    jz_err e = jazz_node__node__descriptor_roles__encode_result_descriptor(JZN_ROLE_CURRENT, canonical, &enc);
    if (e.code) goto out;
    if (!jz_bytes_eq(&enc, &payload->descriptor)) { e = invalid("result payload role schema differs from compiled output"); goto out; }
    e = canonical_values(canonical, jz_bytes_slice(&payload->record), "result payload row is not canonical", &values);
    if (e.code) goto out;
    {
        const jz_str *mtable;
        jazz_types_RowUuid mrow;
        if (!jazz_protocol__protocol__ResultMemberEntry__as_row(&payload->member, &mtable, &mrow) || !value_is_uuid(&values.ptr[0], &mrow)) {
            e = invalid("result payload differs from its member row identity");
            goto out;
        }
    }
    push_field(&fields, jz_str_from("row_uuid"), vt(GROOVE_VT_UUID));
    JZ_VEC_PUSH(publications, hidden_metadata(jz_str_from("row_uuid")));
    for (u32 i = 0; i < schema->payload_fields.len; i++) {
        const jazz_engine_TypedOutputField *field = &schema->payload_fields.ptr[i];
        if (jz_str_eq(&field->name, &schema->row_field)) continue;
        push_field(&fields, jz_str_clone(&field->name), vt_clone(&field->ty));
        const jazz_engine_CurrentRowPublicationField *p = jz_btree_get(&schema->payload_publication_fields, &field->name);
        if (!p) { e = invalid("result payload field has no publication identity"); goto out; }
        jazz_engine_CurrentRowPublicationField c;
        jazz_engine__query_engine__publication__CurrentRowPublicationField__clone(&c, p);
        JZ_VEC_PUSH(publications, c);
    }
    member_occurrence = jazz_protocol__protocol__ResultMemberEntry__output_occurrence_id(&payload->member);
    {
        u32 njoined = 0, narms = 0;
        if (member_occurrence) {
            jazz_types__object__OutputOccurrenceId__joined_sources(member_occurrence, &njoined);
            jazz_types__object__OutputOccurrenceId__union_arms(member_occurrence, &narms);
        }
        bool mismatch = (member_occurrence ? njoined : 0) + 1 != schema->occurrence_id_fields.len;
        if (!mismatch && member_occurrence) {
            const jz_uuid *root = jazz_types__object__SourceRef__uuid(jazz_types__object__OutputOccurrenceId__root_source(member_occurrence), 0);
            mismatch = !value_is_uuid(&values.ptr[0], root) || narms != schema->occurrence_union_arm_fields.len;
        }
        if (mismatch) { e = invalid("result occurrence differs from compiled sources"); goto out; }
    }
    if (schema->occurrence_id_fields.len > 1) {
        jazz_types_OutputOccurrenceId *occurrence = jazz_protocol__protocol__ResultMemberEntry__output_occurrence_id(&payload->member);
        if (!occurrence) { e = invalid("joined result payload has no occurrence identity"); goto out; }
        u32 njoined, narms;
        const jazz_types_SourceRef *joined = jazz_types__object__OutputOccurrenceId__joined_sources(occurrence, &njoined);
        const jazz_types_UnionArm *arms = jazz_types__object__OutputOccurrenceId__union_arms(occurrence, &narms);
        if (njoined + 1 != schema->occurrence_id_fields.len || narms != schema->occurrence_union_arm_fields.len) {
            jazz_types__object__OutputOccurrenceId__drop(occurrence);
            e = invalid("result occurrence differs from compiled sources");
            goto out;
        }
        for (u32 k = 0; k + 1 < schema->occurrence_id_fields.len && k < njoined; k++) {
            const jz_str *name = &schema->occurrence_id_fields.ptr[k + 1];
            const jz_uuid *src = jazz_types__object__SourceRef__uuid(joined, k);
            if (!value_is_uuid(&values.ptr[fields.len], src)) {
                jazz_types__object__OutputOccurrenceId__drop(occurrence);
                e = invalid("result payload differs from its joined source identity");
                goto out;
            }
            push_field(&fields, jz_str_clone(name), vt(GROOVE_VT_UUID));
            JZ_VEC_PUSH(publications, hidden_metadata(jz_str_clone(name)));
        }
        u32 a = 0;
        const u32 *posp; const jz_str *name;
        JZ_BTREE_FOR(&schema->occurrence_union_arm_fields, posp, name) {
            if (a >= narms) break;
            u32 position = *posp;
            const jazz_types_UnionArm *arm = &arms[a++];
            const groove_Value *v = &values.ptr[fields.len];
            if (position != arm->position || v->tag != GROOVE_V_STRING || !jz_str_eq(&v->v.s, &arm->arm)) {
                jazz_types__object__OutputOccurrenceId__drop(occurrence);
                e = invalid("result payload differs from its union discriminator");
                goto out;
            }
            push_field(&fields, jz_str_clone(name), vt(GROOVE_VT_STRING));
            JZ_VEC_PUSH(publications, hidden_metadata(jz_str_clone(name)));
        }
        jazz_types__object__OutputOccurrenceId__drop(occurrence);
    }
    {
        groove_RecordDescriptor *descriptor = groove__records__RecordDescriptor__new_with_fields(fields);
        fields = (groove_DescriptorFieldVec){ 0 };
        jz_bytes raw = { 0 };
        e = groove__records__RecordDescriptor__create(descriptor, values.ptr, values.len, &raw);
        if (e.code) { groove__records__RecordDescriptor__drop_box(descriptor); goto out; }
        *out = jazz_node__node__CurrentRow__new_with_publication_fields(jz_str_clone(table), groove__records__OwnedRecord__new(raw, descriptor), publications);
        publications = (jazz_engine_CurrentRowPublicationFieldVec){ 0 };
    }
out:
    if (member_occurrence) jazz_types__object__OutputOccurrenceId__drop(member_occurrence);
    pubs_drop(&publications);
    fields_drop(&fields);
    values_drop(&values);
    jz_bytes_drop(&enc);
    groove__records__RecordDescriptor__drop_box(canonical);
    return e;
}
