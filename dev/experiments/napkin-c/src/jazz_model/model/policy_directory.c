/* model/policy_directory.rs: durable Groove encoding of policy claims. */
#include "jazz_model/jazz_model.h"

#define PD(x) jazz_model__model__policy_directory__##x
#define GV(x) groove__records__values__Value__##x
#define GVT(x) groove__records__values__ValueType__##x

#define POLICY_CLAIM_DIRECTORY_MAX_NODES 1024u
enum {
    POLICY_CLAIM_U8 = 0, POLICY_CLAIM_U16, POLICY_CLAIM_U32, POLICY_CLAIM_U64, POLICY_CLAIM_I32, POLICY_CLAIM_I64,
    POLICY_CLAIM_F64, POLICY_CLAIM_BOOL, POLICY_CLAIM_STRING, POLICY_CLAIM_BYTES, POLICY_CLAIM_UUID,
    POLICY_CLAIM_ENUM_TAG, POLICY_CLAIM_NULL, POLICY_CLAIM_TUPLE, POLICY_CLAIM_ARRAY, POLICY_CLAIM_NULLABLE,
};
static const char *const KIND_NAMES[16] = {
    "u8", "u16", "u32", "u64", "i32", "i64", "f64", "bool", "string", "bytes", "uuid", "enum_tag", "null", "tuple", "array", "nullable",
};

static jz_err str_err(const char *msg) { return jz_err_new(JAZZ_MODEL_ERR_STRING, msg); }

static jz_err str_err_with(const char *prefix, jz_err e) {
    jz_str m = {0};
    groove__records__Error__Display__fmt(&m, &e);
    jz_err r = jz_err_fmt(JAZZ_MODEL_ERR_STRING, "%s: %.*s", prefix, (int)m.len, m.ptr);
    jz_str_drop(&m);
    jz_err_drop(&e);
    return r;
}

static groove_ValueType jazz_model__model__policy_directory__policy_claim_kind_type(void) {
    groove_StrVec variants = {0};
    for (u32 i = 0; i < 16; i++) JZ_VEC_PUSH(variants, jz_str_from(KIND_NAMES[i]));
    groove_ValueType t;
    memset(&t, 0, sizeof t);
    t.tag = GROOVE_VT_ENUM_TAG;
    jz_err e = groove__records__values__ScalarEnumSchema__new(jz_str_from("jazz.internal.policy_claim_directory_node_kind.v1"), variants, &t.u.enum_tag);
    if (e.code) jz_panic("fixed policy-claim node kinds are valid");
    return t;
}

static groove_RecordDescriptor single_value_descriptor(u8 vt) {
    jz_str name = jz_str_from("value");
    groove_ValueType t = GVT(scalar)(vt);
    return groove__records__RecordDescriptor__new(&name, &t, 1);
}

static bool SCHEMA_INIT;
static groove_EnumSchema SCHEMA;

static const groove_EnumSchema *jazz_model__model__policy_directory__policy_claim_value_schema(void) {
    if (SCHEMA_INIT) return &SCHEMA;
    static const u8 scalar_types[12] = {
        GROOVE_VT_U8, GROOVE_VT_U16, GROOVE_VT_U32, GROOVE_VT_U64, GROOVE_VT_I32, GROOVE_VT_I64,
        GROOVE_VT_F64, GROOVE_VT_BOOL, GROOVE_VT_STRING, GROOVE_VT_BYTES, GROOVE_VT_UUID, GROOVE_VT_U8,
    };
    groove_EnumCaseVec cases = {0};
    for (u32 i = 0; i < 12; i++)
        JZ_VEC_PUSH(cases, groove__records__values__EnumCase__new(jz_str_from(KIND_NAMES[i]), single_value_descriptor(scalar_types[i])));
    for (u32 i = 12; i < 16; i++)
        JZ_VEC_PUSH(cases, groove__records__values__EnumCase__new(jz_str_from(KIND_NAMES[i]), groove__records__RecordDescriptor__new(NULL, NULL, 0)));
    jz_err e = groove__records__values__EnumSchema__new(jz_str_from("jazz.internal.policy_claim_directory_value.v1"), cases, &SCHEMA);
    if (e.code) jz_panic("fixed policy-claim value enum is valid");
    SCHEMA_INIT = true;
    return &SCHEMA;
}

static bool DESCRIPTOR_INIT;
static groove_RecordDescriptor DESCRIPTOR;

static groove_RecordDescriptor jazz_model__model__policy_directory__policy_claim_node_descriptor(void) {
    if (DESCRIPTOR_INIT) return DESCRIPTOR;
    jz_str names[4] = {jz_str_from("name"), jz_str_from("kind"), jz_str_from("children"), jz_str_from("value")};
    groove_ValueType types[4];
    types[0] = GVT(nullable)(GVT(scalar)(GROOVE_VT_STRING));
    types[1] = jazz_model__model__policy_directory__policy_claim_kind_type();
    types[2] = GVT(scalar)(GROOVE_VT_U32);
    memset(&types[3], 0, sizeof types[3]);
    types[3].tag = GROOVE_VT_ENUM;
    types[3].u.enum_ = JZ_NEW(groove_EnumSchema);
    groove__records__values__EnumSchema__Clone__clone(types[3].u.enum_, jazz_model__model__policy_directory__policy_claim_value_schema());
    DESCRIPTOR = groove__records__RecordDescriptor__new(names, types, 4);
    DESCRIPTOR_INIT = true;
    return DESCRIPTOR;
}

groove_RecordDescriptor PD(policy_directory_descriptor)(void) {
    jz_str names[2] = {jz_str_from("derived_v1"), jz_str_from("claims_v1")};
    groove_ValueType types[2];
    types[0] = GVT(scalar)(GROOVE_VT_U8);
    groove_ValueType rec;
    memset(&rec, 0, sizeof rec);
    rec.tag = GROOVE_VT_RECORD;
    rec.u.record = jazz_model__model__policy_directory__policy_claim_node_descriptor();
    types[1] = GVT(array_of)(rec);
    return groove__records__RecordDescriptor__new(names, types, 2);
}

jz_err PD(policy_directory_payload)(u8 presence, groove_Value claims, groove_Value *out) {
    groove_RecordDescriptor d = PD(policy_directory_descriptor)();
    groove_Value values[2];
    memset(&values[0], 0, sizeof values[0]);
    values[0].tag = GROOVE_V_U8;
    values[0].u.u8_ = presence;
    values[1] = claims;
    jz_bytes raw = {0};
    jz_err e = groove__records__RecordDescriptor__create(d, values, 2, &raw);
    GV(drop)(&values[1]);
    if (e.code) {
        jz_str m = {0};
        groove__records__Error__Display__fmt(&m, &e);
        jz_err_drop(&e);
        jz_err r = jz_err_fmt(JAZZ_MODEL_ERR_STRING, "%.*s", (int)m.len, m.ptr);
        jz_str_drop(&m);
        return r;
    }
    memset(out, 0, sizeof *out);
    out->tag = GROOVE_V_RECORD;
    out->u.record = groove__records__OwnedRecord__new(raw, d);
    return JZ_OK;
}

groove_ValueType PD(policy_binding_directory_claims_value_type)(void) {
    groove_ValueType t;
    memset(&t, 0, sizeof t);
    t.tag = GROOVE_VT_RECORD;
    t.u.record = PD(policy_directory_descriptor)();
    return t;
}

static jz_err jazz_model__model__policy_directory__policy_claim_scalar(u32 tag, groove_Value value, groove_EnumValue *out) {
    const groove_EnumCase *c;
    if (groove__records__values__EnumSchema__case(jazz_model__model__policy_directory__policy_claim_value_schema(), tag, &c).code)
        jz_panic("fixed tag");
    jz_err e = groove__records__values__EnumValue__create(tag, c->payload, &value, 1, out);
    GV(drop)(&value);
    if (e.code) return str_err_with("policy binding directory scalar is invalid", e);
    return JZ_OK;
}

static jz_err jazz_model__model__policy_directory__policy_claim_container(u32 tag, groove_EnumValue *out) {
    const groove_EnumCase *c;
    if (groove__records__values__EnumSchema__case(jazz_model__model__policy_directory__policy_claim_value_schema(), tag, &c).code)
        jz_panic("fixed tag");
    jz_err e = groove__records__values__EnumValue__create(tag, c->payload, NULL, 0, out);
    if (e.code) return str_err_with("policy binding directory container is invalid", e);
    return JZ_OK;
}

static jz_err jazz_model__model__policy_directory__encode_policy_claim_node(groove_ValueVec *nodes, const jz_str *name, const groove_Value *value) {
    if (nodes->len >= POLICY_CLAIM_DIRECTORY_MAX_NODES) return str_err("policy binding directory claims exceed node limit");
    u32 kind;
    groove_EnumValue enum_value;
    const groove_Value *children = NULL;
    u32 child_count = 0;
    bool scalar = true;
    switch (value->tag) {
    case GROOVE_V_U8: kind = POLICY_CLAIM_U8; break;
    case GROOVE_V_U16: kind = POLICY_CLAIM_U16; break;
    case GROOVE_V_U32: kind = POLICY_CLAIM_U32; break;
    case GROOVE_V_U64: kind = POLICY_CLAIM_U64; break;
    case GROOVE_V_I32: kind = POLICY_CLAIM_I32; break;
    case GROOVE_V_I64: kind = POLICY_CLAIM_I64; break;
    case GROOVE_V_F64: kind = POLICY_CLAIM_F64; break;
    case GROOVE_V_BOOL: kind = POLICY_CLAIM_BOOL; break;
    case GROOVE_V_STRING: kind = POLICY_CLAIM_STRING; break;
    case GROOVE_V_BYTES: kind = POLICY_CLAIM_BYTES; break;
    case GROOVE_V_UUID: kind = POLICY_CLAIM_UUID; break;
    case GROOVE_V_ENUM_TAG: kind = POLICY_CLAIM_ENUM_TAG; break;
    case GROOVE_V_NULLABLE:
        scalar = false;
        if (value->u.nullable) { kind = POLICY_CLAIM_NULLABLE; children = value->u.nullable; child_count = 1; }
        else kind = POLICY_CLAIM_NULL;
        break;
    case GROOVE_V_TUPLE: scalar = false; kind = POLICY_CLAIM_TUPLE; children = value->u.items.ptr; child_count = value->u.items.len; break;
    case GROOVE_V_ARRAY: scalar = false; kind = POLICY_CLAIM_ARRAY; children = value->u.items.ptr; child_count = value->u.items.len; break;
    default: return str_err("policy binding directory does not admit engine-owned claim values");
    }
    if (scalar) {
        groove_Value v;
        if (kind == POLICY_CLAIM_ENUM_TAG) {
            memset(&v, 0, sizeof v);
            v.tag = GROOVE_V_U8;
            v.u.u8_ = value->u.enum_tag;
        } else GV(Clone__clone)(&v, value);
        JZ_TRY(jazz_model__model__policy_directory__policy_claim_scalar(kind, v, &enum_value));
    } else {
        JZ_TRY(jazz_model__model__policy_directory__policy_claim_container(kind, &enum_value));
    }
    groove_RecordDescriptor d = jazz_model__model__policy_directory__policy_claim_node_descriptor();
    groove_Value fields[4];
    memset(fields, 0, sizeof fields);
    fields[0].tag = GROOVE_V_NULLABLE;
    if (name) {
        fields[0].u.nullable = JZ_NEW(groove_Value);
        fields[0].u.nullable->tag = GROOVE_V_STRING;
        fields[0].u.nullable->u.s = jz_str_clone(name);
    }
    fields[1].tag = GROOVE_V_ENUM_TAG;
    fields[1].u.enum_tag = (u8)kind;
    fields[2].tag = GROOVE_V_U32;
    fields[2].u.u32_ = child_count;
    fields[3].tag = GROOVE_V_ENUM;
    fields[3].u.enum_ = enum_value;
    jz_bytes raw = {0};
    jz_err e = groove__records__RecordDescriptor__create(d, fields, 4, &raw);
    GV(drop)(&fields[0]);
    GV(drop)(&fields[3]);
    if (e.code) return str_err_with("policy binding directory claim node is invalid", e);
    groove_Value node;
    memset(&node, 0, sizeof node);
    node.tag = GROOVE_V_RECORD;
    node.u.record = groove__records__OwnedRecord__new(raw, d);
    JZ_VEC_PUSH(*nodes, node);
    for (u32 i = 0; i < child_count; i++)
        JZ_TRY(jazz_model__model__policy_directory__encode_policy_claim_node(nodes, NULL, &children[i]));
    return JZ_OK;
}

jz_err PD(policy_binding_directory_claims_value)(const jz_btree *claims, groove_Value *out) {
    groove_ValueVec nodes = {0};
    jz_str *k;
    groove_Value *v;
    JZ_BTREE_FOR(claims, k, v) {
        jz_err e = jazz_model__model__policy_directory__encode_policy_claim_node(&nodes, k, v);
        if (e.code) { groove__records__values__ValueVec__drop(&nodes); return e; }
    }
    memset(out, 0, sizeof *out);
    out->tag = GROOVE_V_ARRAY;
    out->u.items = nodes;
    return JZ_OK;
}

static jz_err jazz_model__model__policy_directory__decode_policy_claim_node(const groove_ValueVec *nodes, u32 *cursor, bool root, jazz_model_OptStr *name_out, groove_Value *value_out) {
    if (*cursor >= nodes->len) return str_err("policy binding directory claim tree ended early");
    const groove_Value *node = &nodes->ptr[*cursor];
    *cursor += 1;
    if (node->tag != GROOVE_V_RECORD) return str_err("policy binding directory node must be a record");
    if (node->u.record.descriptor != jazz_model__model__policy_directory__policy_claim_node_descriptor())
        return str_err("policy binding directory node has unexpected descriptor");
    groove_ValueVec values = {0};
    jz_err e = groove__records__OwnedRecord__to_values(&node->u.record, &values);
    if (e.code) return str_err_with("policy binding directory node cannot decode", e);
    jz_err result = JZ_OK;
    groove_ValueVec payload = {0};
    jazz_model_OptStr name = {false, {0}};
    if (values.len != 4 || values.ptr[1].tag != GROOVE_V_ENUM_TAG || values.ptr[2].tag != GROOVE_V_U32 || values.ptr[3].tag != GROOVE_V_ENUM) {
        result = str_err("policy binding directory node has invalid fields");
        goto done;
    }
    const groove_Value *n = &values.ptr[0];
    if (n->tag != GROOVE_V_NULLABLE) { result = str_err("policy binding directory name must be nullable string"); goto done; }
    if (n->u.nullable) {
        if (n->u.nullable->tag != GROOVE_V_STRING) { result = str_err("policy binding directory name must be string"); goto done; }
        name.some = true;
        name.s = jz_str_clone(&n->u.nullable->u.s);
    }
    if (root != name.some) {
        result = str_err(root ? "policy binding directory root claim is unnamed" : "policy binding directory child claim is named");
        goto done;
    }
    u32 expected_tag = values.ptr[1].u.enum_tag;
    const groove_EnumValue *enum_value = &values.ptr[3].u.enum_;
    if (enum_value->tag != expected_tag || expected_tag > POLICY_CLAIM_NULLABLE) {
        result = str_err("policy binding directory kind and value disagree");
        goto done;
    }
    e = groove__records__OwnedRecord__to_values(&enum_value->record, &payload);
    if (e.code) { result = str_err_with("policy binding directory value cannot decode", e); goto done; }
    u32 child_count = values.ptr[2].u.u32_;
    groove_Value value;
    memset(&value, 0, sizeof value);
    if (expected_tag <= POLICY_CLAIM_ENUM_TAG) {
        if (child_count != 0 || payload.len != 1) {
            result = str_err("policy binding directory scalar has invalid children or payload");
            goto done;
        }
        GV(Clone__clone)(&value, &payload.ptr[0]);
        if (expected_tag == POLICY_CLAIM_ENUM_TAG) {
            if (value.tag != GROOVE_V_U8) {
                GV(drop)(&value);
                result = str_err("policy binding directory enum tag must be u8");
                goto done;
            }
            u8 t = value.u.u8_;
            value.tag = GROOVE_V_ENUM_TAG;
            value.u.enum_tag = t;
        }
    } else if (expected_tag == POLICY_CLAIM_NULL) {
        if (child_count != 0 || payload.len != 0) { result = str_err("policy binding directory null has payload or children"); goto done; }
        value.tag = GROOVE_V_NULLABLE;
        value.u.nullable = NULL;
    } else if (expected_tag == POLICY_CLAIM_TUPLE || expected_tag == POLICY_CLAIM_ARRAY || expected_tag == POLICY_CLAIM_NULLABLE) {
        if (payload.len != 0) { result = str_err("policy binding directory container has payload"); goto done; }
        if (expected_tag == POLICY_CLAIM_NULLABLE && child_count != 1) {
            result = str_err("policy binding directory nullable must have one child");
            goto done;
        }
        groove_ValueVec children = {0};
        jz_vec_reserve(&children, child_count, sizeof(groove_Value));
        for (u32 i = 0; i < child_count; i++) {
            jazz_model_OptStr ignored;
            groove_Value child;
            e = jazz_model__model__policy_directory__decode_policy_claim_node(nodes, cursor, false, &ignored, &child);
            if (e.code) { groove__records__values__ValueVec__drop(&children); result = e; goto done; }
            jazz_model__OptStr__drop(&ignored);
            JZ_VEC_PUSH(children, child);
        }
        if (expected_tag == POLICY_CLAIM_TUPLE) { value.tag = GROOVE_V_TUPLE; value.u.items = children; }
        else if (expected_tag == POLICY_CLAIM_ARRAY) { value.tag = GROOVE_V_ARRAY; value.u.items = children; }
        else {
            value.tag = GROOVE_V_NULLABLE;
            value.u.nullable = JZ_NEW(groove_Value);
            *value.u.nullable = children.ptr[0];
            jz_vec_drop(&children, sizeof(groove_Value), NULL);
        }
    } else {
        result = str_err("policy binding directory node kind is unknown");
        goto done;
    }
    *name_out = name;
    name.some = false;
    *value_out = value;
done:
    jazz_model__OptStr__drop(&name);
    groove__records__values__ValueVec__drop(&payload);
    groove__records__values__ValueVec__drop(&values);
    return result;
}

jz_err PD(policy_binding_directory_claims_from_value)(groove_Value value, jz_btree *out) {
    jz_btree_init(out, &jazz_model__STR_GVALUE_DESC);
    if (value.tag != GROOVE_V_ARRAY) {
        GV(drop)(&value);
        return str_err("policy binding directory claims must be an array");
    }
    groove_ValueVec *nodes = &value.u.items;
    jz_err e = JZ_OK;
    if (nodes->len > POLICY_CLAIM_DIRECTORY_MAX_NODES) {
        e = str_err("policy binding directory claims exceed node limit");
    } else {
        u32 cursor = 0;
        while (cursor < nodes->len) {
            jazz_model_OptStr name;
            groove_Value v;
            e = jazz_model__model__policy_directory__decode_policy_claim_node(nodes, &cursor, true, &name, &v);
            if (e.code) break;
            if (!name.some) { GV(drop)(&v); e = str_err("policy binding directory root claim is unnamed"); break; }
            if (jz_btree_insert(out, &name.s, &v, NULL)) {
                e = str_err("policy binding directory contains duplicate claim names");
                break;
            }
        }
    }
    GV(drop)(&value);
    if (e.code) jz_btree_drop(out);
    return e;
}
