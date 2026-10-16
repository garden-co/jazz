/* binding_args.c: port of crates/jazz/layers/db/src/binding_args.rs */
#include "db.h"

#define MAX_SAFE_JS_INTEGER 9007199254740991ull
typedef enum { NUMERIC_CLAIM_ORIGIN_JAVASCRIPT = 0, NUMERIC_CLAIM_ORIGIN_PARSED_JSON = 1 } jazz_model_NumericClaimOrigin;
/* Result<Option<Value>, String>: *has = Some */
jz_err jazz_model__model__policy_claims__json_value_to_policy_claim(jz_json *value, u32 origin, bool *has, groove_Value *out);
void jazz_model__model__policy_claims__canonical_policy_binding_claims(const jazz_types_AuthorSubject *author, jz_btree *provider_claims, jz_btree *out);

jz_err jazz_db__binding_args__row_uuid(jz_slice bytes, jazz_types_RowUuid *out) {
    if (bytes.len != 16) return jz_err_new(JAZZ_DB_STRING_ERROR, "row id must be 16 bytes");
    memcpy(out->b, bytes.ptr, 16);
    return JZ_OK;
}

bool jazz_db__binding_args__safe_u64(f64 value, u64 *out) {
    bool finite = value == value && value != __builtin_inf() && value != -__builtin_inf();
    if (finite && value >= 0.0 && value == __builtin_trunc(value) && value <= (f64)MAX_SAFE_JS_INTEGER) {
        *out = (u64)value;
        return true;
    }
    return false;
}

jz_err jazz_db__binding_args__checked_u64(f64 value, jz_slice name, u64 *out) {
    if (jazz_db__binding_args__safe_u64(value, out)) return JZ_OK;
    return jz_err_fmt(JAZZ_DB_STRING_ERROR, "%.*s must be a nonnegative safe integer", (int)name.len, name.ptr);
}

jz_err jazz_db__binding_args__author_subject(jz_slice bytes, jazz_types_AuthorSubject *out) {
    if (!jz_utf8_valid(bytes.ptr, bytes.len))
        return jz_err_new(JAZZ_DB_STRING_ERROR, "author subject must be canonical UTF-8 JSON");
    jz_err e = jazz_types__ids__AuthorSubject__from_untrusted_canonical(bytes, out);
    if (JZ_IS_ERR(e)) e.code = JAZZ_DB_STRING_ERROR; /* error.to_string() */
    return e;
}

static bool slice_eq(jz_slice s, const char *lit) { usize n = strlen(lit); return s.len == n && memcmp(s.ptr, lit, n) == 0; }

jz_err jazz_db__binding_args__durability_tier(jz_slice tier, jazz_model_DurabilityTier *out) {
    if (slice_eq(tier, "None") || slice_eq(tier, "none")) { *out = JAZZ_DUR_NONE; return JZ_OK; }
    if (slice_eq(tier, "Local") || slice_eq(tier, "local")) { *out = JAZZ_DUR_LOCAL; return JZ_OK; }
    if (slice_eq(tier, "Global") || slice_eq(tier, "global")) { *out = JAZZ_DUR_GLOBAL; return JZ_OK; }
    return jz_err_fmt(JAZZ_DB_STRING_ERROR, "unknown durability tier %.*s", (int)tier.len, tier.ptr);
}

jz_err jazz_db__binding_args__read_tier(jz_slice tier, jazz_db_ReadTier *out) {
    if (slice_eq(tier, "local-first") || slice_eq(tier, "LocalFirst")) { *out = JAZZ_DB_READ_LOCAL_FIRST; return JZ_OK; }
    if (slice_eq(tier, "remote") || slice_eq(tier, "Remote")) { *out = JAZZ_DB_READ_REMOTE; return JZ_OK; }
    if (slice_eq(tier, "local-only") || slice_eq(tier, "LocalOnly")) { *out = JAZZ_DB_READ_LOCAL_ONLY; return JZ_OK; }
    return jz_err_fmt(JAZZ_DB_STRING_ERROR, "unknown read tier %.*s", (int)tier.len, tier.ptr);
}

jz_err jazz_db__binding_args__policy_claims(const jazz_types_AuthorSubject *author, jz_json *claims, jz_btree *out) {
    jz_btree projected;
    jz_btree_init(&projected, &jazz_db_str_value_desc);
    switch (claims->kind) {
    case JZ_JSON_NULL:
        break;
    case JZ_JSON_OBJ: {
        jz_str *key;
        jz_json **value;
        JZ_BTREE_FOR(&claims->v.obj, key, value) {
            bool has = false;
            groove_Value claim;
            jz_json *taken = *value;
            *value = jz_json_null();
            jz_err e = jazz_model__model__policy_claims__json_value_to_policy_claim(taken, NUMERIC_CLAIM_ORIGIN_JAVASCRIPT, &has, &claim);
            if (JZ_IS_ERR(e)) { jz_btree_drop(&projected); jz_json_free(claims); return e; }
            if (has) {
                jz_str k = jz_str_clone(key);
                jz_btree_insert(&projected, &k, &claim, NULL);
            }
        }
        break;
    }
    default:
        jz_json_free(claims);
        return jz_err_new(JAZZ_DB_STRING_ERROR, "identity claims must be an object");
    }
    jz_json_free(claims);
    jazz_model__model__policy_claims__canonical_policy_binding_claims(author, &projected, out);
    return JZ_OK;
}
