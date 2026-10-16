/* model/policy_claims.rs: JSON session claims -> policy binding values. */
#include "jazz_model/jazz_model.h"
#include <math.h>

#define PC(x) jazz_model__model__policy_claims__##x
#define GV(x) groove__records__values__Value__##x

const jz_btree_desc jazz_model__STR_GVALUE_DESC = {
    sizeof(jz_str), sizeof(groove_Value), jz_cmp_str, jazz_model__str_drop_v, groove__records__values__Value__drop_v,
};

static groove_Value gv_string(jz_str s) {
    groove_Value v;
    memset(&v, 0, sizeof v);
    v.tag = GROOVE_V_STRING;
    v.u.s = s;
    return v;
}

static const char *jazz_model__model__policy_claims__auth_mode_for_author_issuer(jz_slice issuer) {
    if (jazz_model__slice_eq_cstr(issuer, jazz_types__ids__AuthorSubject__LOCAL_FIRST_ISSUER)) return "local-first";
    if (jazz_model__slice_eq_cstr(issuer, jazz_types__ids__AuthorSubject__ANONYMOUS_ISSUER)) return "anonymous";
    return "external";
}

void PC(canonical_policy_binding_claims)(const jazz_types_AuthorSubject *author, jz_btree *provider_claims, jz_btree *out) {
    jz_str issuer = {0}, subject = {0};
    jazz_types__ids__AuthorSubject__principal_parts(author, &issuer, &subject);
    jz_btree_init(out, &jazz_model__STR_GVALUE_DESC);
    jz_str name;
    groove_Value value;
    while (jz_btree_pop_first(provider_claims, &name, &value)) {
        jz_str key = jazz_model__query__provider_claim_key(jz_str_slice(&name));
        jz_str_drop(&name);
        jz_btree_insert(out, &key, &value, NULL);
    }
    jz_btree_drop(provider_claims);
    jz_str k = jazz_model__query__provider_claim_key(JZ_LIT("iss"));
    groove_Value v = gv_string(jz_str_clone(&issuer));
    jz_btree_insert(out, &k, &v, NULL);
    k = jazz_model__query__provider_claim_key(JZ_LIT("sub"));
    v = gv_string(subject);
    jz_btree_insert(out, &k, &v, NULL);
    jz_btree author_claims;
    PC(author_policy_claims)(*author, &author_claims);
    jz_btree_append(out, &author_claims);
    jz_btree_drop(&author_claims);
    k = jz_str_from("authMode");
    v = gv_string(jz_str_from(jazz_model__model__policy_claims__auth_mode_for_author_issuer(jz_str_slice(&issuer))));
    jz_btree_insert(out, &k, &v, NULL);
    jz_str_drop(&issuer);
}

static groove_Value record_field(const groove_OwnedRecord *record, const char *name, const char *expect) {
    groove_Value v;
    jz_err e = groove__records__OwnedRecord__get(record, jz_slice_of(name, (u32)strlen(name)), &v);
    if (e.code) jz_panic(expect);
    return v;
}

void PC(author_policy_claims)(jazz_types_AuthorSubject author, jz_btree *out) {
    jazz_types_RowAuthor row_author;
    jz_err e = jazz_types__ids__RowAuthor__from_persisted_subject(author, &row_author);
    bool has_row_author = !e.code;
    jz_err_drop(&e);
    groove_Value value = has_row_author ? jazz_types__ids__RowAuthor__to_value(row_author) : jazz_types__ids__AuthorSubject__to_value(author);
    if (value.tag != GROOVE_V_RECORD) JZ_UNREACHABLE();
    groove_Value account = record_field(&value.u.record, "account", "author account field");
    if (account.tag == GROOVE_V_NULLABLE && account.u.nullable) {
        groove_Value *inner = account.u.nullable;
        account = *inner;
        jz_free(inner);
    }
    groove_Value identity = record_field(&value.u.record, "identity", "author identity field");
    if (identity.tag != GROOVE_V_RECORD) JZ_UNREACHABLE();
    groove_Value issuer = record_field(&identity.u.record, "issuer", "author issuer field");
    groove_Value subject = record_field(&identity.u.record, "subject", "author subject field");
    jz_btree_init(out, &jazz_model__STR_GVALUE_DESC);
    jz_str k = jz_str_from("user.identity");
    jz_btree_insert(out, &k, &identity, NULL);
    k = jz_str_from("user.identity.issuer");
    jz_btree_insert(out, &k, &issuer, NULL);
    k = jz_str_from("user.identity.subject");
    jz_btree_insert(out, &k, &subject, NULL);
    if (has_row_author) {
        k = jz_str_from("user");
        jz_btree_insert(out, &k, &value, NULL);
        k = jz_str_from("user.account");
        jz_btree_insert(out, &k, &account, NULL);
    } else {
        GV(drop)(&value);
        GV(drop)(&account);
    }
}

jz_err PC(json_number_to_policy_claim)(const jz_json *number, u8 origin, groove_Value *out) {
    memset(out, 0, sizeof *out);
    if (number->kind == JZ_JSON_U64) {
        u64 value = number->v.u;
        if (origin == JAZZ_NUMERIC_CLAIM_EXACT_JSON || value <= JAZZ_MAX_SAFE_JS_INTEGER) {
            out->tag = GROOVE_V_U64;
            out->u.u64_ = value;
        } else {
            out->tag = GROOVE_V_F64;
            out->u.f64_ = (f64)value;
        }
        return JZ_OK;
    }
    if (number->kind == JZ_JSON_I64) {
        i64 value = number->v.i;
        u64 mag = value < 0 ? (u64)0 - (u64)value : (u64)value;
        if (origin == JAZZ_NUMERIC_CLAIM_EXACT_JSON || mag <= JAZZ_MAX_SAFE_JS_INTEGER) {
            out->tag = GROOVE_V_I64;
            out->u.i64_ = value;
        } else {
            out->tag = GROOVE_V_F64;
            out->u.f64_ = (f64)value;
        }
        return JZ_OK;
    }
    if (number->kind != JZ_JSON_F64) return jz_err_new(JAZZ_MODEL_ERR_STRING, "unsupported numeric claim");
    f64 value = number->v.f;
    if (!isfinite(value)) return jz_err_new(JAZZ_MODEL_ERR_STRING, "unsupported numeric claim");
    if (value == trunc(value) && fabs(value) <= (f64)JAZZ_MAX_SAFE_JS_INTEGER) {
        if (value < 0.0) { out->tag = GROOVE_V_I64; out->u.i64_ = (i64)value; }
        else { out->tag = GROOVE_V_U64; out->u.u64_ = (u64)value; }
        return JZ_OK;
    }
    out->tag = GROOVE_V_F64;
    out->u.f64_ = value;
    return JZ_OK;
}

jz_err PC(json_value_to_policy_claim)(const jz_json *value, u8 origin, bool *some, groove_Value *out) {
    memset(out, 0, sizeof *out);
    *some = true;
    switch (value->kind) {
    case JZ_JSON_NULL: out->tag = GROOVE_V_NULLABLE; out->u.nullable = NULL; return JZ_OK;
    case JZ_JSON_BOOL: out->tag = GROOVE_V_BOOL; out->u.b = value->v.b; return JZ_OK;
    case JZ_JSON_U64: case JZ_JSON_I64: case JZ_JSON_F64: return PC(json_number_to_policy_claim)(value, origin, out);
    case JZ_JSON_STR: {
        jz_uuid u;
        jz_err e = jz_uuid_parse(jz_str_slice(&value->v.s), &u);
        if (!e.code) { out->tag = GROOVE_V_UUID; out->u.uuid = u; return JZ_OK; }
        jz_err_drop(&e);
        *out = gv_string(jz_str_clone(&value->v.s));
        return JZ_OK;
    }
    case JZ_JSON_ARR: {
        groove_ValueVec projected = {0};
        jz_vec_reserve(&projected, value->v.arr.len, sizeof(groove_Value));
        for (u32 i = 0; i < value->v.arr.len; i++) {
            bool inner_some;
            groove_Value inner;
            jz_err e = PC(json_value_to_policy_claim)(value->v.arr.ptr[i], origin, &inner_some, &inner);
            if (e.code || !inner_some) {
                groove__records__values__ValueVec__drop(&projected);
                *some = false;
                return e;
            }
            JZ_VEC_PUSH(projected, inner);
        }
        out->tag = GROOVE_V_ARRAY;
        out->u.items = projected;
        return JZ_OK;
    }
    case JZ_JSON_OBJ: {
        /* serde_json's map is already sorted (BTreeMap) */
        groove_ValueVec entries = {0};
        jz_str *k;
        jz_json **v;
        JZ_BTREE_FOR(&value->v.obj, k, v) {
            bool inner_some;
            groove_Value inner;
            jz_err e = PC(json_value_to_policy_claim)(*v, origin, &inner_some, &inner);
            if (e.code || !inner_some) {
                groove__records__values__ValueVec__drop(&entries);
                *some = false;
                return e;
            }
            groove_Value pair;
            memset(&pair, 0, sizeof pair);
            pair.tag = GROOVE_V_TUPLE;
            JZ_VEC_PUSH(pair.u.items, gv_string(jz_str_clone(k)));
            JZ_VEC_PUSH(pair.u.items, inner);
            JZ_VEC_PUSH(entries, pair);
        }
        out->tag = GROOVE_V_TUPLE;
        out->u.items = entries;
        return JZ_OK;
    }
    }
    return JZ_OK;
}

bool PC(policy_claim_at_path)(const jz_btree *claims, const jazz_model_StrVec *path, groove_Value *out) {
    jz_str name;
    if (jazz_model__query__author_claim_path_key(path->ptr, path->len, &name)) {
        groove_Value *hit = jz_btree_get(claims, &name);
        jz_str_drop(&name);
        if (!hit) return false;
        GV(Clone__clone)(out, hit);
        return true;
    }
    jz_str root;
    u32 tail_start;
    if (path->len >= 2 && jz_str_eq_cstr(&path->ptr[0], "claims")) {
        root = jazz_model__query__provider_claim_key(jz_str_slice(&path->ptr[1]));
        tail_start = 2;
    } else if (path->len == 1) {
        root = jz_str_clone(&path->ptr[0]);
        tail_start = 1;
    } else return false;
    groove_Value *hit = jz_btree_get(claims, &root);
    jz_str_drop(&root);
    if (!hit) return false;
    groove_Value value;
    GV(Clone__clone)(&value, hit);
    for (u32 s = tail_start; s < path->len; s++) {
        const jz_str *segment = &path->ptr[s];
        if (value.tag != GROOVE_V_TUPLE) { GV(drop)(&value); return false; }
        groove_Value found;
        bool have = false;
        for (u32 i = 0; i < value.u.items.len; i++) {
            groove_Value *entry = &value.u.items.ptr[i];
            if (entry->tag == GROOVE_V_TUPLE && entry->u.items.len == 2 && entry->u.items.ptr[0].tag == GROOVE_V_STRING &&
                jz_str_eq(&entry->u.items.ptr[0].u.s, segment)) {
                found = entry->u.items.ptr[1];
                entry->u.items.len = 1; /* pair.pop() */
                have = true;
                break;
            }
        }
        GV(drop)(&value);
        if (!have) return false;
        value = found;
    }
    *out = value;
    return true;
}
