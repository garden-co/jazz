/* Port of crates/jazz/layers/node/src/node/query_eval/prepared_bindings.rs
 * Preparation and coercion of query-binding values. */
#include "jazz_node/node/node_qe_b.h"

void jazz_node_qeb_str_drop(void *s) { jz_str_drop(s); }
void jazz_node_qeb_str_clone(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone(src); }
void jazz_node_qeb_value_clone(void *dst, const void *src) { groove_Value_clone(dst, src); }
void jazz_node_qeb_column_type_clone(void *dst, const void *src) { groove_ColumnType_clone(dst, src); }
const jz_btree_desc jazz_node_qeb_STR_COLUMN_TYPE_DESC = { sizeof(jz_str), sizeof(groove_ColumnType), jz_cmp_str, jazz_node_qeb_str_drop, groove_ColumnType_drop };
const jz_btree_desc jazz_node_qeb_STR_CLAIM_PARAM_DESC = { sizeof(jz_str), sizeof(jazz_engine_ProgramClaimParam), jz_cmp_str, jazz_node_qeb_str_drop, jazz_engine_ProgramClaimParam_drop };
const jz_btree_desc jazz_node_qeb_STR_VALUE_DESC = { sizeof(jz_str), sizeof(groove_Value), jz_cmp_str, jazz_node_qeb_str_drop, groove_Value_drop };
const jz_btree_desc jazz_node_qeb_STR_SET_DESC = { sizeof(jz_str), 0, jz_cmp_str, jazz_node_qeb_str_drop, NULL };
const jz_btree_desc jazz_node_qeb_STR_STR_DESC = { sizeof(jz_str), sizeof(jz_str), jz_cmp_str, jazz_node_qeb_str_drop, jazz_node_qeb_str_drop };

jz_str jazz_node__node__query_eval__prepared_bindings__query_binding_value_signature(const jazz_model_Binding *binding)
{
    jz_str out = {0};
    jz_str *k; void *v;
    bool first = true;
    JZ_BTREE_FOR(&binding->values, k, v) {
        if (!first) jz_str_push_char(&out, ',');
        jz_str_push(&out, k->ptr, k->len);
        first = false;
    }
    return out;
}

jz_str jazz_node__node__query_eval__prepared_bindings__policy_plan_cache_signature(
    const jazz_model_Binding *binding, jazz_types_AuthorSubject identity, const jz_str *claims_scope)
{
    /* Authorization lowering still embeds the permission subject in source plans. */
    jz_str sig = jazz_node__node__query_eval__prepared_bindings__query_binding_value_signature(binding);
    jz_str ident = {0};
    jazz_types_AuthorSubject_debug(&ident, &identity);
    jz_str out = jz_format("%.*s|subject=%.*s|claims=%.*s", (int)sig.len, sig.ptr, (int)ident.len, ident.ptr,
                           (int)claims_scope->len, claims_scope->ptr);
    jz_str_drop(&sig);
    jz_str_drop(&ident);
    return out;
}

jz_str jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_prepared_params(
    const jazz_node_PreparedQueryParam *params, u32 n)
{
    jz_btree user_params, claim_params;
    jz_btree_init(&user_params, &jazz_node_qeb_STR_COLUMN_TYPE_DESC);
    jz_btree_init(&claim_params, &jazz_node_qeb_STR_CLAIM_PARAM_DESC);
    for (u32 i = 0; i < n; i++) {
        const jazz_node_PreparedQueryParam *param = &params[i];
        jz_str name = jz_str_clone(&param->name);
        switch (param->source_tag) {
        case JZN_PQPS_USER: {
            groove_ColumnType ty;
            groove_ColumnType_clone(&ty, &param->ty);
            groove_ColumnType old;
            if (jz_btree_insert(&user_params, &name, &ty, &old)) { jz_str_drop(&name); groove_ColumnType_drop(&old); }
            break;
        }
        case JZN_PQPS_CLAIM: {
            jazz_engine_ProgramClaimParam cp;
            memset(&cp, 0, sizeof cp);
            for (u32 s = 0; s < param->claim.segs.len; s++) JZ_VEC_PUSH(cp.path.segs, jz_str_clone(&param->claim.segs.ptr[s]));
            groove_ColumnType_clone(&cp.ty, &param->ty);
            jazz_engine_ProgramClaimParam old;
            if (jz_btree_insert(&claim_params, &name, &cp, &old)) { jz_str_drop(&name); jazz_engine_ProgramClaimParam_drop(&old); }
            break;
        }
        }
    }
    jz_str out = jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_parts(&user_params, &claim_params);
    jz_btree_drop(&user_params);
    jz_btree_drop(&claim_params);
    return out;
}

static void jazz_node__node__query_eval__prepared_bindings__push_usize(jz_bytes *bytes, u64 value)
{
    u8 le[8];
    for (int i = 0; i < 8; i++) le[i] = (u8)(value >> (8 * i));
    jz_bytes_push(bytes, le, 8);
}
static void jazz_node__node__query_eval__prepared_bindings__push_str(jz_bytes *bytes, const char *p, u32 n)
{
    jazz_node__node__query_eval__prepared_bindings__push_usize(bytes, n);
    jz_bytes_push(bytes, (const u8 *)p, n);
}

jz_str jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_parts(
    const jz_btree *param_types, const jz_btree *claim_params)
{
    jz_bytes bytes = {0};
    static const char tag[] = "jazz-binding-source-v1";
    jz_bytes_push(&bytes, (const u8 *)tag, sizeof tag - 1);
    jazz_node__node__query_eval__prepared_bindings__push_usize(&bytes, param_types->len);
    jz_str *name; void *val;
    JZ_BTREE_FOR(param_types, name, val) {
        jazz_node__node__query_eval__prepared_bindings__push_str(&bytes, name->ptr, name->len);
        jz_str dbg = {0};
        groove_ColumnType_debug(&dbg, val);
        jazz_node__node__query_eval__prepared_bindings__push_str(&bytes, dbg.ptr, dbg.len);
        jz_str_drop(&dbg);
    }
    jazz_node__node__query_eval__prepared_bindings__push_usize(&bytes, claim_params->len);
    JZ_BTREE_FOR(claim_params, name, val) {
        const jazz_engine_ProgramClaimParam *claim = val;
        jazz_node__node__query_eval__prepared_bindings__push_str(&bytes, name->ptr, name->len);
        jazz_node__node__query_eval__prepared_bindings__push_usize(&bytes, claim->path.segs.len);
        for (u32 i = 0; i < claim->path.segs.len; i++)
            jazz_node__node__query_eval__prepared_bindings__push_str(&bytes, claim->path.segs.ptr[i].ptr, claim->path.segs.ptr[i].len);
        jz_str dbg = {0};
        groove_ColumnType_debug(&dbg, &claim->ty);
        jazz_node__node__query_eval__prepared_bindings__push_str(&bytes, dbg.ptr, dbg.len);
        jz_str_drop(&dbg);
    }
    u8 hash[32];
    jz_blake3(bytes.ptr, bytes.len, hash);
    jz_bytes_drop(&bytes);
    jz_str hex = {0};
    jz_hex_encode(hash, 32, &hex);
    jz_str out = jz_format("jazz-query-binding:%.*s", (int)hex.len, hex.ptr);
    jz_str_drop(&hex);
    return out;
}

bool jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_parts_if_needed(
    const jz_btree *param_types, const jz_btree *claim_params, jz_str *out)
{
    if (param_types->len == 0 && claim_params->len == 0) return false;
    *out = jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_parts(param_types, claim_params);
    return true;
}

bool jazz_node__node__query_eval__prepared_bindings__authorization_binding_source_shape(
    const jazz_model_ValidatedQuery *shape, const jz_btree *extra_user_params, const jz_btree *claim_params, jz_str *out)
{
    jz_btree param_types;
    jz_btree_clone(&param_types, &shape->params, jazz_node_qeb_str_clone, jazz_node_qeb_column_type_clone);
    jz_str *k; groove_ColumnType *v;
    JZ_BTREE_FOR(extra_user_params, k, v) {
        jz_str kk = jz_str_clone(k);
        groove_ColumnType vv, old;
        groove_ColumnType_clone(&vv, v);
        if (jz_btree_insert(&param_types, &kk, &vv, &old)) { jz_str_drop(&kk); groove_ColumnType_drop(&old); }
    }
    bool some = param_types.len != 0 || claim_params->len != 0;
    if (some) *out = jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_parts(&param_types, claim_params);
    jz_btree_drop(&param_types);
    return some;
}

static jz_str join_path(const jazz_engine_ClaimPath *path)
{
    jz_str out = {0};
    for (u32 i = 0; i < path->segs.len; i++) {
        if (i) jz_str_push_char(&out, '.');
        jz_str_push(&out, path->segs.ptr[i].ptr, path->segs.ptr[i].len);
    }
    return out;
}

jz_err jazz_node__node__query_eval__prepared_bindings__binding_values_for_plan(
    const jazz_model_Binding *binding, const jazz_node_PreparedQueryParam *params, u32 n,
    const jazz_engine_PolicyContext *policy, jazz_node_PreparedClaimBindingMode mode, groove_ValueVec *out)
{
    groove_ValueVec values = {0};
    jz_vec_reserve(&values, n, sizeof(groove_Value));
    for (u32 i = 0; i < n; i++) {
        const jazz_node_PreparedQueryParam *param = &params[i];
        groove_Value value;
        switch (param->source_tag) {
        case JZN_PQPS_USER: {
            const groove_Value *found = jz_btree_get(&binding->values, &param->name);
            if (!found) {
                jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop);
                return jz_err_fmt(JZN_E_Query, "missing parameter %.*s", (int)param->name.len, param->name.ptr);
            }
            groove_Value_clone(&value, found);
            break;
        }
        case JZN_PQPS_CLAIM: {
            bool has;
            jz_err e = jazz_node__node__query_eval__prepared_bindings__prepared_claim_value(&param->claim, policy, &has, &value);
            if (JZ_IS_ERR(e)) { jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop); return e; }
            if (!has) {
                jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop);
                if (mode == JZN_PCBM_FAIL_CLOSED_AUTHORIZATION_SUPPORT) {
                    jz_str joined = join_path(&param->claim);
                    jz_err err = jz_err_fmt(JZN_E_AuthorizationSupportMissingClaim,
                                            "authorization support policy claim is not bound: %.*s", (int)joined.len, joined.ptr);
                    jz_str_drop(&joined);
                    return err;
                }
                return JZN_INVALID_STORED("claim prepared param is not bound");
            }
            break;
        }
        default: JZ_UNREACHABLE();
        }
        JZ_VEC_PUSH(values, jazz_engine__query_engine__coerce_prepared_binding_value(value, &param->ty));
    }
    *out = values;
    return JZ_OK;
}

jz_err jazz_node__node__query_eval__prepared_bindings__prepared_claim_value(
    const jazz_engine_ClaimPath *path, const jazz_engine_PolicyContext *policy, bool *has, groove_Value *out)
{
    *has = false;
    jazz_types_AuthorSubject permission_subject;
    const jz_btree *claims;
    switch (policy->tag) {
    case JZ_PC_IDENTITY:
    case JZ_PC_AUTHORIZATION_SUBPLAN:
        permission_subject = policy->permission_subject;
        claims = &policy->claims;
        break;
    case JZ_PC_SYSTEM:
    default:
        return JZN_INVALID_STORED("claim prepared params require an identity policy context");
    }
    jz_str name;
    if (jazz_model__query__author_claim_path_key(path->segs.ptr, path->segs.len, &name)) {
        jz_btree author;
        jazz_model__model__policy_claims__author_policy_claims(permission_subject, &author);
        jz_str k;
        *has = jz_btree_remove(&author, &name, &k, out);
        if (*has) jz_str_drop(&k);
        jz_str_drop(&name);
        jz_btree_drop(&author);
        return JZ_OK;
    }
    if (jazz_model__model__policy_claims__policy_claim_at_path(claims, path->segs.ptr, path->segs.len, out)) {
        *has = true;
        return JZ_OK;
    }
    if (path->segs.len == 1) {
        jz_btree defaults;
        jazz_node__node__default_policy_claim_values(permission_subject, &defaults);
        jz_str k;
        *has = jz_btree_remove(&defaults, &path->segs.ptr[0], &k, out);
        if (*has) jz_str_drop(&k);
        jz_btree_drop(&defaults);
        return JZ_OK;
    }
    return JZ_OK;
}

void jazz_node__node__query_eval__prepared_bindings__coerce_binding_values_for_shape(
    const jazz_model_ValidatedQuery *shape, jz_btree *values)
{
    jz_str *name; groove_Value *value;
    JZ_BTREE_FOR(values, name, value) {
        const groove_ColumnType *ty = jz_btree_get(&shape->params, name);
        if (!ty) continue;
        groove_Value copy;
        groove_Value_clone(&copy, value);
        groove_Value_drop(value);
        *value = jazz_engine__query_engine__coerce_prepared_binding_value(copy, ty);
    }
}
