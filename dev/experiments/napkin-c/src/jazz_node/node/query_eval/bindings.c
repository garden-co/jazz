/* Port of crates/jazz/layers/node/src/node/query_eval/bindings.rs
 * Stable query, policy-claim, and prepared-plan binding mechanics. */
#include "jazz_node/node/node_qe_b.h"

typedef jazz_model_Predicate Pred;
typedef jazz_model_Operand Operand;
typedef jazz_model_PredicateVec PredVec;

static void pred_vec_drop(PredVec *v) { jz_vec_drop(v, sizeof(Pred), jazz_model_Predicate_drop); }
static void pred_vec_clone(PredVec *dst, const PredVec *src)
{
    memset(dst, 0, sizeof *dst);
    jz_vec_reserve(dst, src->len, sizeof(Pred));
    for (u32 i = 0; i < src->len; i++) jazz_model_Predicate_clone(&dst->ptr[i], &src->ptr[i]);
    dst->len = src->len;
}
#define VEC_CLONE(dst, src, T, clonefn) do { memset(&(dst), 0, sizeof(dst)); jz_vec_reserve(&(dst), (src).len, sizeof(T)); \
    for (u32 _i = 0; _i < (src).len; _i++) clonefn(&(dst).ptr[_i], &(src).ptr[_i]); (dst).len = (src).len; } while (0)

jazz_model_Query jazz_node__node__query_eval__bindings__authorization_query_from_read_policy(const jazz_model_TableSchema *table)
{
    const jazz_model_Query *policy = table->read_policy;
    jazz_model_Query query = jazz_model__query__builders__Query__from(table->name.ptr, table->name.len);
    if (!policy) {
        /* Missing SELECT is empty read authority, regardless of table names. */
        Pred any;
        memset(&any, 0, sizeof any);
        any.tag = JZ_PRED_ANY;
        jz_vec_push(&query.filters, &any, sizeof any);
        return query;
    }
    pred_vec_drop(&query.filters);
    pred_vec_clone(&query.filters, &policy->filters);
    jz_vec_drop(&query.joins, sizeof(jazz_model_JoinVia), jazz_model_JoinVia_drop);
    VEC_CLONE(query.joins, policy->joins, jazz_model_JoinVia, jazz_model_JoinVia_clone);
    jz_vec_drop(&query.reachable, sizeof(jazz_model_ReachableVia), jazz_model_ReachableVia_drop);
    VEC_CLONE(query.reachable, policy->reachable, jazz_model_ReachableVia, jazz_model_ReachableVia_clone);
    jz_vec_drop(&query.inherits, sizeof(jazz_model_InheritsVia), jazz_model_InheritsVia_drop);
    VEC_CLONE(query.inherits, policy->inherits, jazz_model_InheritsVia, jazz_model_InheritsVia_clone);
    memset(&query.includes, 0, sizeof query.includes);
    for (u32 i = 0; i < policy->includes.len; i++) {
        jazz_model_Include inc = policy->includes.ptr[i];
        inc.path = jz_str_clone(&inc.path);
        jz_vec_push(&query.includes, &inc, sizeof inc);
    }
    jz_vec_drop(&query.policy_branches, sizeof(jazz_model_PolicyBranch), jazz_model_PolicyBranch_drop);
    VEC_CLONE(query.policy_branches, policy->policy_branches, jazz_model_PolicyBranch, jazz_model_PolicyBranch_clone);
    return query;
}

/* ---------- claim helpers ---------- */
static const char *const BUILTIN_POLICY_CLAIMS[] = { "user", "isAdmin" };

static bool jazz_node__node__query_eval__bindings__is_builtin_policy_claim(const jz_str *name)
{
    for (u32 i = 0; i < 2; i++) if (jz_str_eq_cstr(name, BUILTIN_POLICY_CLAIMS[i])) return true;
    return jz_str_eq_cstr(name, "user.account") || jz_str_eq_cstr(name, "user.identity") ||
           jz_str_eq_cstr(name, "user.identity.issuer") || jz_str_eq_cstr(name, "user.identity.subject");
}

static bool claim_present(const jz_btree *claims, const jz_str *name)
{
    if (!claims) return false;
    jz_str_vec path = jazz_model__query__policy_and_request_contracts__operand_claim_path(name);
    groove_Value v;
    bool found = jazz_model__model__policy_claims__policy_claim_at_path(claims, path.ptr, path.len, &v);
    if (found) groove_Value_drop(&v);
    jz_str_vec_drop(&path);
    return found;
}

static bool jazz_node__node__query_eval__bindings__operand_contains_unbound_claim(const Operand *operand, const jz_btree *claims)
{
    return operand->tag == JZ_OPERAND_CLAIM && !jazz_node__node__query_eval__bindings__is_builtin_policy_claim(&operand->name) &&
           !claim_present(claims, &operand->name);
}
static bool any_operand_cb(const Operand *op, void *ctx) { return jazz_node__node__query_eval__bindings__operand_contains_unbound_claim(op, ctx); }
static bool jazz_node__node__query_eval__bindings__predicate_contains_unbound_claim(const Pred *p, const jz_btree *claims)
{
    return jazz_model__query__policy_and_request_contracts__Predicate__any_operand(p, any_operand_cb, (void *)claims);
}
static bool jazz_node__node__query_eval__bindings__operands_contain_unbound_claim(const Operand *const *ops, u32 n, const jz_btree *claims)
{
    for (u32 i = 0; i < n; i++) if (jazz_node__node__query_eval__bindings__operand_contains_unbound_claim(ops[i], claims)) return true;
    return false;
}

static Pred jazz_node__node__query_eval__bindings__false_predicate(void)
{
    Pred p;
    memset(&p, 0, sizeof p);
    p.tag = JZ_PRED_EQ;
    p.left.tag = JZ_OPERAND_LITERAL;
    p.left.literal = gv_bool(true);
    p.right.tag = JZ_OPERAND_LITERAL;
    p.right.literal = gv_bool(false);
    return p;
}

/* ---------- rewrite_claim_*_for_binding ---------- */
jazz_model_JoinVia jazz_node__node__query_eval__bindings__rewrite_claim_join_for_binding(jazz_model_JoinVia join, const jz_btree *claims)
{
    for (u32 i = 0; i < join.filters.len; i++)
        join.filters.ptr[i] = jazz_node__node__query_eval__bindings__rewrite_claim_predicate_for_binding(join.filters.ptr[i], claims);
    for (u32 i = 0; i < join.nested_joins.len; i++)
        join.nested_joins.ptr[i] = jazz_node__node__query_eval__bindings__rewrite_claim_join_for_binding(join.nested_joins.ptr[i], claims);
    return join;
}

static Pred replace_with_false(Pred *p)
{
    jazz_model_Predicate_drop(p);
    return jazz_node__node__query_eval__bindings__false_predicate();
}

Pred jazz_node__node__query_eval__bindings__rewrite_claim_predicate_for_binding(Pred predicate, const jz_btree *claims)
{
    switch (predicate.tag) {
    case JZ_PRED_ALL:
    case JZ_PRED_ANY:
        for (u32 i = 0; i < predicate.children.len; i++)
            predicate.children.ptr[i] = jazz_node__node__query_eval__bindings__rewrite_claim_predicate_for_binding(predicate.children.ptr[i], claims);
        return predicate;
    case JZ_PRED_NOT:
        if (jazz_node__node__query_eval__bindings__predicate_contains_unbound_claim(predicate.inner, claims)) return replace_with_false(&predicate);
        *predicate.inner = jazz_node__node__query_eval__bindings__rewrite_claim_predicate_for_binding(*predicate.inner, claims);
        return predicate;
    case JZ_PRED_EQ:
    case JZ_PRED_NE:
    case JZ_PRED_CONTAINS: {
        const Operand *ops[2] = { &predicate.left, &predicate.right };
        if (jazz_node__node__query_eval__bindings__operands_contain_unbound_claim(ops, 2, claims)) return replace_with_false(&predicate);
        return predicate;
    }
    case JZ_PRED_IN: {
        const Operand **ops = jz_alloc(sizeof(Operand *) * (predicate.values.len + 1));
        ops[0] = &predicate.left;
        for (u32 i = 0; i < predicate.values.len; i++) ops[i + 1] = &predicate.values.ptr[i];
        bool unbound = jazz_node__node__query_eval__bindings__operands_contain_unbound_claim(ops, predicate.values.len + 1, claims);
        jz_free(ops);
        if (unbound) return replace_with_false(&predicate);
        return predicate;
    }
    case JZ_PRED_GT: case JZ_PRED_GTE: case JZ_PRED_LT: case JZ_PRED_LTE:
        return replace_with_false(&predicate);
    case JZ_PRED_ENUM_MATCH:
        *predicate.inner = jazz_node__node__query_eval__bindings__rewrite_claim_predicate_for_binding(*predicate.inner, claims);
        return predicate;
    case JZ_PRED_IS_NULL:
        if (predicate.left.tag == JZ_OPERAND_CLAIM) {
            jz_str_vec path = jazz_model__query__policy_and_request_contracts__operand_claim_path(&predicate.left.name);
            groove_Value v;
            bool found = claims && jazz_model__model__policy_claims__policy_claim_at_path(claims, path.ptr, path.len, &v);
            jz_str_vec_drop(&path);
            bool null_claim = found && v.tag == GV_NULLABLE && v.v.nullable == NULL;
            if (found) groove_Value_drop(&v);
            jazz_model_Predicate_drop(&predicate);
            if (null_claim) {
                Pred all;
                memset(&all, 0, sizeof all);
                all.tag = JZ_PRED_ALL;
                return all;
            }
            /* Missing claims must not match IS NULL. */
            return jazz_node__node__query_eval__bindings__false_predicate();
        }
        return predicate;
    }
    JZ_UNREACHABLE();
}

void jazz_node__node__query_eval__bindings__default_permission_scope_claim_values(jazz_types_AuthorSubject writer, jz_btree *out)
{
    jazz_node__node__query_eval__bindings__default_policy_claim_values(writer, out);
}

/* Alpha-compat built-ins live at the node admission/query boundary. */
void jazz_node__node__query_eval__bindings__default_policy_claim_values(jazz_types_AuthorSubject writer, jz_btree *out)
{
    jazz_model__model__policy_claims__author_policy_claims(writer, out);
    jz_str k = jz_str_from("isAdmin");
    groove_Value v = gv_bool(false), old;
    if (jz_btree_insert(out, &k, &v, &old)) { jz_str_drop(&k); groove_Value_drop(&old); }
}

/* ---------- bind_scope_claim_operands ---------- */
typedef struct ScopeCtx { const jz_btree *claim_values; jz_btree *binding_values; } ScopeCtx;

static void jazz_node__node__query_eval__bindings__bind_scope_claim_operand(Operand *operand, const jz_btree *claim_values, jz_btree *binding_values)
{
    if (operand->tag != JZ_OPERAND_CLAIM) return;
    jz_str_vec path = jazz_model__query__policy_and_request_contracts__operand_claim_path(&operand->name);
    groove_Value value;
    bool found = jazz_model__model__policy_claims__policy_claim_at_path(claim_values, path.ptr, path.len, &value);
    jz_str_vec_drop(&path);
    if (!found) return;
    jazz_engine_ClaimPath cp;
    cp.segs = jazz_model__query__policy_and_request_contracts__operand_claim_path(&operand->name);
    jz_str param = jazz_engine__query_engine__fields__claim_param_field(&cp);
    jz_str_vec_drop((jz_str_vec *)&cp.segs);
    jz_str key = jz_str_clone(&param);
    groove_Value old;
    if (jz_btree_insert(binding_values, &key, &value, &old)) { jz_str_drop(&key); groove_Value_drop(&old); }
    jazz_model_Operand_drop(operand);
    memset(operand, 0, sizeof *operand);
    operand->tag = JZ_OPERAND_PARAM;
    operand->name = param;
}

static void jazz_node__node__query_eval__bindings__bind_scope_claim_predicate(Pred *predicate, const jz_btree *claim_values, jz_btree *binding_values)
{
    switch (predicate->tag) {
    case JZ_PRED_NOT:
        if (jazz_node__node__query_eval__bindings__predicate_contains_unbound_claim(predicate->inner, claim_values)) {
            jazz_model_Predicate_drop(predicate);
            *predicate = jazz_node__node__query_eval__bindings__false_predicate();
            return;
        }
        jazz_node__node__query_eval__bindings__bind_scope_claim_predicate(predicate->inner, claim_values, binding_values);
        return;
    case JZ_PRED_IS_NULL:
        if (predicate->left.tag == JZ_OPERAND_CLAIM) {
            /* Null checks need no typed parameter: resolve them before claim slot inference. */
            Pred copy;
            jazz_model_Predicate_clone(&copy, predicate);
            Pred r = jazz_node__node__query_eval__bindings__rewrite_claim_predicate_for_binding(copy, claim_values);
            jazz_model_Predicate_drop(predicate);
            *predicate = r;
            return;
        }
        jazz_node__node__query_eval__bindings__bind_scope_claim_operand(&predicate->left, claim_values, binding_values);
        return;
    case JZ_PRED_ALL:
    case JZ_PRED_ANY:
        for (u32 i = 0; i < predicate->children.len; i++)
            jazz_node__node__query_eval__bindings__bind_scope_claim_predicate(&predicate->children.ptr[i], claim_values, binding_values);
        return;
    case JZ_PRED_EQ: case JZ_PRED_NE: case JZ_PRED_GT: case JZ_PRED_GTE: case JZ_PRED_LT: case JZ_PRED_LTE: case JZ_PRED_CONTAINS:
        jazz_node__node__query_eval__bindings__bind_scope_claim_operand(&predicate->left, claim_values, binding_values);
        jazz_node__node__query_eval__bindings__bind_scope_claim_operand(&predicate->right, claim_values, binding_values);
        return;
    case JZ_PRED_IN:
        jazz_node__node__query_eval__bindings__bind_scope_claim_operand(&predicate->left, claim_values, binding_values);
        for (u32 i = 0; i < predicate->values.len; i++)
            jazz_node__node__query_eval__bindings__bind_scope_claim_operand(&predicate->values.ptr[i], claim_values, binding_values);
        return;
    case JZ_PRED_ENUM_MATCH:
        jazz_node__node__query_eval__bindings__bind_scope_claim_predicate(predicate->inner, claim_values, binding_values);
        return;
    }
}
static void scope_cb(Pred *p, void *ctx)
{
    ScopeCtx *c = ctx;
    jazz_node__node__query_eval__bindings__bind_scope_claim_predicate(p, c->claim_values, c->binding_values);
}
void jazz_node__node__query_eval__bindings__bind_scope_claim_operands(jazz_model_Query *query, const jz_btree *claim_values, jz_btree *binding_values)
{
    ScopeCtx c = { claim_values, binding_values };
    jazz_model__query__policy_and_request_contracts__Query__for_each_condition_mut(query, scope_cb, &c);
}

/* ---------- claim parameter disambiguation ---------- */
jz_err jazz_node__node__query_eval__bindings__disambiguate_policy_claim_params(jazz_model_Query *query, const jazz_model_JazzSchema *schema, jz_btree *binding_values, jz_btree *out)
{
    jz_btree empty;
    jz_btree_init(&empty, &jazz_node_qeb_STR_CLAIM_PARAM_DESC);
    jz_err e = jazz_node__node__query_eval__bindings__disambiguate_policy_claim_params_with_outer_slots(query, schema, binding_values, &empty, out);
    jz_btree_drop(&empty);
    return e;
}

jz_str jazz_node__node__query_eval__bindings__typed_claim_param_alias(const jz_str *name, const groove_ColumnType *ty)
{
    jz_str t = {0};
    groove_ColumnType_debug(&t, ty);
    jz_str out = jz_format("__jazz_claim_typed:%u:%.*s:%.*s", t.len, (int)t.len, t.ptr, (int)name->len, name->ptr);
    jz_str_drop(&t);
    return out;
}

static void rename_operand_param(Operand *operand, void *ctx)
{
    const jz_btree *aliases = ctx;
    if (operand->tag != JZ_OPERAND_PARAM) return;
    const jz_str *alias = jz_btree_get(aliases, &operand->name);
    if (alias) { jz_str_drop(&operand->name); operand->name = jz_str_clone(alias); }
}
static void rename_cb(Pred *p, void *ctx) { jazz_model__query__policy_and_request_contracts__Predicate__for_each_operand_mut(p, rename_operand_param, ctx); }

static void jazz_node__node__query_eval__bindings__rename_query_params(jazz_model_Query *query, const jz_btree *aliases)
{
    jazz_model__query__policy_and_request_contracts__Query__for_each_condition_mut(query, rename_cb, (void *)aliases);
    for (u32 i = 0; i < query->reachable.len; i++) rename_operand_param(&query->reachable.ptr[i].from, (void *)aliases);
    for (u32 b = 0; b < query->policy_branches.len; b++) {
        jazz_model_PolicyBranch *branch = &query->policy_branches.ptr[b];
        for (u32 i = 0; i < branch->reachable.len; i++) rename_operand_param(&branch->reachable.ptr[i].from, (void *)aliases);
    }
}

/* Give a policy-local claim parameter a stable binding slot. */
jz_err jazz_node__node__query_eval__bindings__disambiguate_policy_claim_params_with_outer_slots(
    jazz_model_Query *query, const jazz_model_JazzSchema *schema, jz_btree *binding_values, const jz_btree *outer_slots, jz_btree *out)
{
    jazz_model_ValidatedQuery shape;
    JZ_TRY(jazz_model__query__builders__Query__validate_runtime(query, schema, &shape));
    jz_btree aliases, claims;
    jz_btree_init(&aliases, &jazz_node_qeb_STR_STR_DESC);
    jz_btree_init(&claims, &jazz_node_qeb_STR_CLAIM_PARAM_DESC);
    jz_str *name; groove_ColumnType *ty;
    JZ_BTREE_FOR(&shape.params, name, ty) {
        jazz_engine_ClaimPath path;
        if (!jazz_engine__query_engine__fields__claim_path_from_param_field(name, &path)) continue;
        jz_str alias;
        bool found = false;
        jz_str *sname; jazz_engine_ProgramClaimParam *slot;
        JZ_BTREE_FOR(outer_slots, sname, slot) {
            if (jazz_engine_ClaimPath_eq(&slot->path, &path) && groove_ColumnType_eq(&slot->ty, ty)) { alias = jz_str_clone(sname); found = true; break; }
        }
        if (!found) alias = jazz_node__node__query_eval__bindings__typed_claim_param_alias(name, ty);
        jz_str k = jz_str_clone(name), v = jz_str_clone(&alias), old;
        if (jz_btree_insert(&aliases, &k, &v, &old)) { jz_str_drop(&k); jz_str_drop(&old); }
        jazz_engine_ProgramClaimParam cp;
        cp.path = path;
        groove_ColumnType_clone(&cp.ty, ty);
        jazz_engine_ProgramClaimParam oldp;
        if (jz_btree_insert(&claims, &alias, &cp, &oldp)) { jz_str_drop(&alias); jazz_engine_ProgramClaimParam_drop(&oldp); }
    }
    jazz_model_ValidatedQuery_drop(&shape);
    jazz_node__node__query_eval__bindings__rename_query_params(query, &aliases);
    jz_str k, alias;
    while (jz_btree_pop_first(&aliases, &k, &alias)) {
        jz_str rk;
        groove_Value value;
        if (jz_btree_remove(binding_values, &k, &rk, &value)) {
            jz_str_drop(&rk);
            groove_Value old;
            if (jz_btree_insert(binding_values, &alias, &value, &old)) { jz_str_drop(&alias); groove_Value_drop(&old); }
        } else {
            jz_str_drop(&alias);
        }
        jz_str_drop(&k);
    }
    jz_btree_drop(&aliases);
    *out = claims;
    return JZ_OK;
}

u32 jazz_node__node__query_eval__bindings__ParamBindingMode__cache_key(jazz_node_ParamBindingMode mode)
{
    switch (mode) {
    case JZN_PBM_INLINE_ALL_REACHABLE_SEEDS: return 0; /* ParamBindingModeCacheKey::InlineAllReachableSeeds */
    case JZN_PBM_RETAIN_ALL_PARAMS: return 1;          /* ParamBindingModeCacheKey::RetainAllParams */
    }
    JZ_UNREACHABLE();
}

jz_str jazz_node__node__query_eval__bindings__binding_user_params_cache_key(const jz_btree *params)
{
    jz_str out = {0};
    jz_str_push_char(&out, '{');
    jz_str *k; groove_ColumnType *v; bool first = true;
    JZ_BTREE_FOR(params, k, v) {
        if (!first) jz_str_push_cstr(&out, ", ");
        jazz_node_qeb_debug_str(&out, k);
        jz_str_push_cstr(&out, ": ");
        groove_ColumnType_debug(&out, v);
        first = false;
    }
    jz_str_push_char(&out, '}');
    return out;
}

jz_str jazz_node__node__query_eval__bindings__binding_claim_params_cache_key(const jz_btree *params)
{
    jz_str out = {0};
    jz_str_push_char(&out, '{');
    jz_str *k; jazz_engine_ProgramClaimParam *v; bool first = true;
    JZ_BTREE_FOR(params, k, v) {
        if (!first) jz_str_push_cstr(&out, ", ");
        jazz_node_qeb_debug_str(&out, k);
        jz_str_push_cstr(&out, ": ProgramClaimParam { path: ClaimPath(");
        jazz_node_qeb_debug_str_vec(&out, v->path.segs.ptr, v->path.segs.len);
        jz_str_push_cstr(&out, "), ty: ");
        groove_ColumnType_debug(&out, &v->ty);
        jz_str_push_cstr(&out, " }");
        first = false;
    }
    jz_str_push_char(&out, '}');
    return out;
}

/* ---------- binding parameters into literals ---------- */
static jazz_engine_SourceId jazz_node__node__query_eval__bindings__bind_source_for_table(const jz_str *table)
{
    jazz_engine_SourceId s;
    memset(&s, 0, sizeof s);
    s.table = jz_str_clone(table);
    return s;
}

static bool jazz_node__node__query_eval__bindings__should_inline_reachable_seed(const Operand *operand, jazz_node_ParamBindingMode mode)
{
    if (operand->tag == JZ_OPERAND_PARAM) {
        switch (mode) {
        case JZN_PBM_INLINE_ALL_REACHABLE_SEEDS: return true;
        case JZN_PBM_RETAIN_ALL_PARAMS: return false;
        }
    }
    return false;
}

static jz_err jazz_node__node__query_eval__bindings__bind_query_operand_with_target_type(
    Operand operand, const jazz_model_Binding *binding, const groove_ColumnType *target_type, jazz_node_ParamBindingMode mode, Operand *out)
{
    switch (operand.tag) {
    case JZ_OPERAND_PARAM: {
        if (mode == JZN_PBM_RETAIN_ALL_PARAMS) { *out = operand; return JZ_OK; }
        const groove_Value *found = jz_btree_get(&binding->values, &operand.name);
        if (!found) {
            jz_err e = jz_err_fmt(JZN_E_Query, "missing parameter %.*s", (int)operand.name.len, operand.name.ptr);
            jazz_model_Operand_drop(&operand);
            return e;
        }
        groove_Value value;
        groove_Value_clone(&value, found);
        jazz_model_Operand_drop(&operand);
        memset(out, 0, sizeof *out);
        out->tag = JZ_OPERAND_LITERAL;
        out->literal = target_type ? jazz_node__node__query_eval__normalization__coerce_literal_for_column_type(value, target_type) : value;
        return JZ_OK;
    }
    case JZ_OPERAND_LITERAL:
        if (target_type) operand.literal = jazz_node__node__query_eval__normalization__coerce_literal_for_column_type(operand.literal, target_type);
        *out = operand;
        return JZ_OK;
    case JZ_OPERAND_COLUMN:
    case JZ_OPERAND_CLAIM:
    default:
        *out = operand;
        return JZ_OK;
    }
}

static jz_err jazz_node__node__query_eval__bindings__bind_query_operand(Operand operand, const jazz_model_Binding *binding, jazz_node_ParamBindingMode mode, Operand *out)
{
    return jazz_node__node__query_eval__bindings__bind_query_operand_with_target_type(operand, binding, NULL, mode, out);
}

typedef struct OptType { bool has; groove_ColumnType ty; } OptType;
static void opt_type_drop(OptType *t) { if (t->has) groove_ColumnType_drop(&t->ty); }

static jz_err jazz_node__node__query_eval__bindings__bind_predicates(PredVec *predicates, const jazz_model_Binding *binding, const jazz_model_JazzSchema *schema, const jazz_engine_SourceId *source, jazz_node_ParamBindingMode mode);

static jz_err jazz_node__node__query_eval__bindings__bind_binary_predicate(Pred *p, const jazz_model_Binding *binding, const jazz_model_JazzSchema *schema, const jazz_engine_SourceId *source, jazz_node_ParamBindingMode mode)
{
    OptType lt, rt;
    JZ_TRY(jazz_node__node__query_eval__normalization__operand_column_type(schema, source, &p->left, &lt.has, &lt.ty));
    jz_err e = jazz_node__node__query_eval__normalization__operand_column_type(schema, source, &p->right, &rt.has, &rt.ty);
    if (JZ_IS_ERR(e)) { opt_type_drop(&lt); return e; }
    Operand l, r;
    e = jazz_node__node__query_eval__bindings__bind_query_operand_with_target_type(p->left, binding, rt.has ? &rt.ty : NULL, mode, &l);
    memset(&p->left, 0, sizeof p->left);
    if (!JZ_IS_ERR(e)) {
        e = jazz_node__node__query_eval__bindings__bind_query_operand_with_target_type(p->right, binding, lt.has ? &lt.ty : NULL, mode, &r);
        memset(&p->right, 0, sizeof p->right);
        if (!JZ_IS_ERR(e)) { p->left = l; p->right = r; }
        else jazz_model_Operand_drop(&l);
    }
    opt_type_drop(&lt);
    opt_type_drop(&rt);
    return e;
}

/* Binds `p` in place (Rust consumes and rebuilds it). */
static jz_err jazz_node__node__query_eval__bindings__bind_query_predicate(Pred *p, const jazz_model_Binding *binding, const jazz_model_JazzSchema *schema, const jazz_engine_SourceId *source, jazz_node_ParamBindingMode mode)
{
    switch (p->tag) {
    case JZ_PRED_ALL:
    case JZ_PRED_ANY:
        return jazz_node__node__query_eval__bindings__bind_predicates(&p->children, binding, schema, source, mode);
    case JZ_PRED_NOT:
        return jazz_node__node__query_eval__bindings__bind_query_predicate(p->inner, binding, schema, source, mode);
    case JZ_PRED_EQ: case JZ_PRED_NE: case JZ_PRED_GT: case JZ_PRED_GTE: case JZ_PRED_LT: case JZ_PRED_LTE:
        return jazz_node__node__query_eval__bindings__bind_binary_predicate(p, binding, schema, source, mode);
    case JZ_PRED_IN: {
        Operand left;
        JZ_TRY(jazz_node__node__query_eval__bindings__bind_query_operand(p->left, binding, mode, &left));
        p->left = left;
        OptType target;
        JZ_TRY(jazz_node__node__query_eval__normalization__operand_column_type(schema, source, &p->left, &target.has, &target.ty));
        for (u32 i = 0; i < p->values.len; i++) {
            Operand v;
            jz_err e = jazz_node__node__query_eval__bindings__bind_query_operand_with_target_type(p->values.ptr[i], binding, target.has ? &target.ty : NULL, mode, &v);
            if (JZ_IS_ERR(e)) {
                /* the consumed operand is gone; keep the vector droppable */
                memset(&p->values.ptr[i], 0, sizeof(Operand));
                opt_type_drop(&target);
                return e;
            }
            p->values.ptr[i] = v;
        }
        opt_type_drop(&target);
        return JZ_OK;
    }
    case JZ_PRED_CONTAINS: {
        Operand left;
        JZ_TRY(jazz_node__node__query_eval__bindings__bind_query_operand(p->left, binding, mode, &left));
        p->left = left;
        OptType needle;
        JZ_TRY(jazz_node__node__query_eval__normalization__contains_needle_type(schema, source, &p->left, &needle.has, &needle.ty));
        Operand right;
        jz_err e = jazz_node__node__query_eval__bindings__bind_query_operand_with_target_type(p->right, binding, needle.has ? &needle.ty : NULL, mode, &right);
        opt_type_drop(&needle);
        if (JZ_IS_ERR(e)) { memset(&p->right, 0, sizeof p->right); return e; }
        p->right = right;
        if (p->left.tag == JZ_OPERAND_LITERAL && p->left.literal.tag == GV_ARRAY) {
            OptType target;
            JZ_TRY(jazz_node__node__query_eval__normalization__operand_column_type(schema, source, &p->right, &target.has, &target.ty));
            groove_ValueVec values = p->left.literal.v.items;
            Pred in;
            memset(&in, 0, sizeof in);
            in.tag = JZ_PRED_IN;
            in.left = p->right;
            for (u32 i = 0; i < values.len; i++) {
                Operand lit;
                memset(&lit, 0, sizeof lit);
                lit.tag = JZ_OPERAND_LITERAL;
                if (target.has) {
                    groove_Value copy;
                    groove_Value_clone(&copy, &values.ptr[i]);
                    lit.literal = jazz_node__node__query_eval__normalization__coerce_literal_for_column_type(copy, &target.ty);
                    groove_Value_drop(&values.ptr[i]);
                } else {
                    lit.literal = values.ptr[i];
                }
                jz_vec_push(&in.values, &lit, sizeof lit);
            }
            jz_free(values.ptr);
            opt_type_drop(&target);
            jz_str_drop(&p->left.name);
            *p = in;
        }
        return JZ_OK;
    }
    case JZ_PRED_IS_NULL: {
        Operand op;
        JZ_TRY(jazz_node__node__query_eval__bindings__bind_query_operand(p->left, binding, mode, &op));
        p->left = op;
        return JZ_OK;
    }
    case JZ_PRED_ENUM_MATCH:
        return JZ_OK;
    }
    JZ_UNREACHABLE();
}

static jz_err jazz_node__node__query_eval__bindings__bind_predicates(PredVec *predicates, const jazz_model_Binding *binding, const jazz_model_JazzSchema *schema, const jazz_engine_SourceId *source, jazz_node_ParamBindingMode mode)
{
    for (u32 i = 0; i < predicates->len; i++)
        JZ_TRY(jazz_node__node__query_eval__bindings__bind_query_predicate(&predicates->ptr[i], binding, schema, source, mode));
    return JZ_OK;
}

static jz_err bind_predicates_for_table(PredVec *preds, const jz_str *table, const jazz_model_Binding *binding, const jazz_model_JazzSchema *schema, jazz_node_ParamBindingMode mode)
{
    jazz_engine_SourceId source = jazz_node__node__query_eval__bindings__bind_source_for_table(table);
    jz_err e = jazz_node__node__query_eval__bindings__bind_predicates(preds, binding, schema, &source, mode);
    jazz_engine_SourceId_drop(&source);
    return e;
}

static jz_err jazz_node__node__query_eval__bindings__bind_reachable_seed_filters(jazz_model_ReachableVia *reachable, const jazz_model_Binding *binding, const jazz_model_JazzSchema *schema, jazz_node_ParamBindingMode mode)
{
    if (reachable->seed) return bind_predicates_for_table(&reachable->seed->filters, &reachable->seed->table, binding, schema, mode);
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__bindings__bind_reachable(jazz_model_ReachableVia *reachable, const jazz_model_Binding *binding, const jazz_model_JazzSchema *schema, jazz_node_ParamBindingMode mode)
{
    if (jazz_node__node__query_eval__bindings__should_inline_reachable_seed(&reachable->from, mode)) {
        Operand from;
        jz_err e = jazz_node__node__query_eval__bindings__bind_query_operand(reachable->from, binding, mode, &from);
        if (JZ_IS_ERR(e)) { memset(&reachable->from, 0, sizeof reachable->from); return e; }
        reachable->from = from;
    }
    JZ_TRY(bind_predicates_for_table(&reachable->access_filters, &reachable->access_table, binding, schema, mode));
    JZ_TRY(bind_predicates_for_table(&reachable->edge_filters, &reachable->edge_table, binding, schema, mode));
    return jazz_node__node__query_eval__bindings__bind_reachable_seed_filters(reachable, binding, schema, mode);
}

static jz_err jazz_node__node__query_eval__bindings__bind_join_filter_literals(jazz_model_JoinVia *join, const jazz_model_Binding *binding, const jazz_model_JazzSchema *schema, jazz_node_ParamBindingMode mode)
{
    JZ_TRY(bind_predicates_for_table(&join->filters, &join->table, binding, schema, mode));
    for (u32 i = 0; i < join->nested_joins.len; i++)
        JZ_TRY(jazz_node__node__query_eval__bindings__bind_join_filter_literals(&join->nested_joins.ptr[i], binding, schema, mode));
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__bindings__bind_array_subquery_filter_literals(jazz_model_ArraySubquery *subquery, const jazz_model_Binding *binding, const jazz_model_JazzSchema *schema, jazz_node_ParamBindingMode mode)
{
    JZ_TRY(bind_predicates_for_table(&subquery->filters, &subquery->table, binding, schema, mode));
    for (u32 i = 0; i < subquery->nested_arrays.len; i++)
        JZ_TRY(jazz_node__node__query_eval__bindings__bind_array_subquery_filter_literals(&subquery->nested_arrays.ptr[i], binding, schema, mode));
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__bindings__bind_flat_join_predicate(
    Pred *predicate, const jazz_model_Binding *binding, const jazz_model_JazzSchema *schema, jazz_node_ParamBindingMode mode,
    const jz_str *root_scope, const jz_btree *source_tables)
{
    if (predicate->tag == JZ_PRED_ALL) {
        for (u32 i = 0; i < predicate->children.len; i++)
            JZ_TRY(jazz_node__node__query_eval__bindings__bind_flat_join_predicate(&predicate->children.ptr[i], binding, schema, mode, root_scope, source_tables));
        return JZ_OK;
    }
    jz_btree sources;
    JZ_TRY(jazz_model__query__validation__flat_join_predicate_sources(predicate, &sources));
    jz_str *first; void *unused;
    const jz_str *scope = jz_btree_first(&sources, (void **)&first, &unused) ? first : root_scope;
    jz_str scope_copy = jz_str_clone(scope);
    jz_btree_drop(&sources);
    jz_err e = JZ_OK;
    const jz_str *table = jz_btree_get(source_tables, &scope_copy);
    if (!table) {
        e = jazz_node__node__Error_query_lowering(jz_format("flat join filter references unknown source %.*s", (int)scope_copy.len, scope_copy.ptr));
        goto done;
    }
    Pred unq;
    e = jazz_model__query__validation__unqualify_flat_join_predicate(predicate, &scope_copy, &unq);
    if (JZ_IS_ERR(e)) goto done;
    jazz_engine_SourceId source = jazz_node__node__query_eval__bindings__bind_source_for_table(table);
    e = jazz_node__node__query_eval__bindings__bind_query_predicate(&unq, binding, schema, &source, mode);
    jazz_engine_SourceId_drop(&source);
    if (JZ_IS_ERR(e)) { jazz_model_Predicate_drop(&unq); goto done; }
    Pred q;
    e = jazz_model__query__validation__qualify_flat_join_source_predicate(unq, &scope_copy, &q);
    if (JZ_IS_ERR(e)) goto done;
    jazz_model_Predicate_drop(predicate);
    *predicate = q;
done:
    jz_str_drop(&scope_copy);
    return e;
}

jz_err jazz_node__node__query_eval__bindings__bind_query_params_with_mode(
    const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, const jazz_model_JazzSchema *schema,
    jazz_node_ParamBindingMode mode, jazz_model_ValidatedQuery *out)
{
    jazz_model_Query query;
    jazz_model_Query_clone(&query, &shape->query);
    jazz_engine_SourceId root_source = jazz_node__node__query_eval__normalization__root_source_id(query.table.ptr, query.table.len);
    jz_err e = JZ_OK;
    if (query.flat_join) {
        const jazz_model_FlatJoin *fj = query.flat_join;
        jz_str root_scope = fj->has_root_alias ? jz_str_clone(&fj->root_alias) : jz_str_clone(&query.table);
        jz_btree source_tables;
        jz_btree_init(&source_tables, &jazz_node_qeb_STR_STR_DESC);
        jz_str k = jz_str_clone(&root_scope), v = jz_str_clone(&query.table), old;
        jz_btree_insert(&source_tables, &k, &v, NULL);
        for (u32 i = 0; i < fj->sources.len; i++) {
            const jazz_model_FlatJoinSource *src = &fj->sources.ptr[i];
            k = src->has_alias ? jz_str_clone(&src->alias) : jz_str_clone(&src->table);
            v = jz_str_clone(&src->table);
            if (jz_btree_insert(&source_tables, &k, &v, &old)) { jz_str_drop(&k); jz_str_drop(&old); }
        }
        for (u32 i = 0; i < query.filters.len && !JZ_IS_ERR(e); i++)
            e = jazz_node__node__query_eval__bindings__bind_flat_join_predicate(&query.filters.ptr[i], binding, schema, mode, &root_scope, &source_tables);
        jz_btree_drop(&source_tables);
        jz_str_drop(&root_scope);
    } else {
        e = jazz_node__node__query_eval__bindings__bind_predicates(&query.filters, binding, schema, &root_source, mode);
    }
    for (u32 i = 0; i < query.joins.len && !JZ_IS_ERR(e); i++)
        e = jazz_node__node__query_eval__bindings__bind_join_filter_literals(&query.joins.ptr[i], binding, schema, mode);
    for (u32 i = 0; i < query.reachable.len && !JZ_IS_ERR(e); i++)
        e = jazz_node__node__query_eval__bindings__bind_reachable(&query.reachable.ptr[i], binding, schema, mode);
    for (u32 i = 0; i < query.array_subqueries.len && !JZ_IS_ERR(e); i++)
        e = jazz_node__node__query_eval__bindings__bind_array_subquery_filter_literals(&query.array_subqueries.ptr[i], binding, schema, mode);
    for (u32 b = 0; b < query.policy_branches.len && !JZ_IS_ERR(e); b++) {
        jazz_model_PolicyBranch *branch = &query.policy_branches.ptr[b];
        e = jazz_node__node__query_eval__bindings__bind_predicates(&branch->filters, binding, schema, &root_source, mode);
        for (u32 i = 0; i < branch->joins.len && !JZ_IS_ERR(e); i++)
            e = jazz_node__node__query_eval__bindings__bind_join_filter_literals(&branch->joins.ptr[i], binding, schema, mode);
        for (u32 i = 0; i < branch->reachable.len && !JZ_IS_ERR(e); i++)
            e = jazz_node__node__query_eval__bindings__bind_reachable(&branch->reachable.ptr[i], binding, schema, mode);
    }
    jazz_engine_SourceId_drop(&root_source);
    if (JZ_IS_ERR(e)) { jazz_model_Query_drop(&query); return e; }
    jazz_model_ValidatedQuery rebound;
    e = jazz_model__query__builders__Query__validate_with_schema_version(&query, schema, shape->schema_version, &rebound);
    jazz_model_Query_drop(&query);
    if (JZ_IS_ERR(e)) return e;
    if (!jz_uuid_eq(&rebound.schema_version, &shape->schema_version)) {
        jazz_model_ValidatedQuery_drop(&rebound);
        return JZN_INVALID_STORED("bound query schema changed");
    }
    *out = rebound;
    return JZ_OK;
}

jz_err jazz_node__node__query_eval__bindings__inline_snapshot_bind_filter_literals(
    const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, const jazz_model_JazzSchema *schema, jazz_model_ValidatedQuery *out)
{
    return jazz_node__node__query_eval__bindings__bind_query_params_with_mode(shape, binding, schema, JZN_PBM_INLINE_ALL_REACHABLE_SEEDS, out);
}

void jazz_node__node__query_eval__bindings__retarget_binding_value_sources(jazz_engine_NormalizedRowSetShape *shape, const jz_str *binding_source_shape)
{
    void *k; jazz_engine_RowSetExpr *node;
    JZ_BTREE_FOR(&shape->nodes, k, node) {
        if (node->tag == JZ_RSE_VALUE_SOURCE && node->vs_mode == JZ_VSM_BINDING) {
            jz_str_drop(&node->vs_shape);
            node->vs_shape = jz_str_clone(binding_source_shape);
        }
    }
}

/* ---------- claim params of a normalized shape ---------- */
static void insert_claim_param(jz_btree *params, jz_str name, jazz_engine_ClaimPath path, groove_ColumnType ty)
{
    jazz_engine_ProgramClaimParam cp = { path, ty }, old;
    if (jz_btree_insert(params, &name, &cp, &old)) { jz_str_drop(&name); jazz_engine_ProgramClaimParam_drop(&old); }
}
static jazz_engine_ClaimPath claim_path_clone(const jazz_engine_ClaimPath *p)
{
    jazz_engine_ClaimPath c;
    memset(&c, 0, sizeof c);
    for (u32 i = 0; i < p->segs.len; i++) JZ_VEC_PUSH(c.segs, jz_str_clone(&p->segs.ptr[i]));
    return c;
}

typedef struct FieldParamCtx { const jz_btree *param_types; jz_btree *params; } FieldParamCtx;

static void jazz_node__node__query_eval__bindings__collect_claim_field_param(const jazz_engine_NormalizedValueRef *value, const jz_btree *param_types, jz_btree *params)
{
    if (value->tag != JZ_NVR_PARAM) return;
    jazz_engine_ClaimPath path;
    if (!jazz_engine__query_engine__fields__claim_path_from_param_field(&value->field, &path)) return;
    const groove_ColumnType *ty = jz_btree_get(param_types, &value->field);
    if (!ty) { jazz_engine_ClaimPath_drop(&path); return; }
    if (jz_btree_contains(params, &value->field)) { jazz_engine_ClaimPath_drop(&path); return; } /* entry().or_insert */
    groove_ColumnType t;
    groove_ColumnType_clone(&t, ty);
    insert_claim_param(params, jz_str_clone(&value->field), path, t);
}

static void jazz_node__node__query_eval__bindings__collect_claim_field_param_authoritative(const jazz_engine_NormalizedValueRef *value, groove_ColumnType ty, jz_btree *params)
{
    if (value->tag != JZ_NVR_PARAM) { groove_ColumnType_drop(&ty); return; }
    jazz_engine_ClaimPath path;
    if (!jazz_engine__query_engine__fields__claim_path_from_param_field(&value->field, &path)) { groove_ColumnType_drop(&ty); return; }
    insert_claim_param(params, jz_str_clone(&value->field), path, ty);
}

static jz_err field_param_cb(const jazz_engine_NormalizedValueRef *v, void *ctx)
{
    FieldParamCtx *c = ctx;
    jazz_node__node__query_eval__bindings__collect_claim_field_param(v, c->param_types, c->params);
    return JZ_OK;
}
static void jazz_node__node__query_eval__bindings__collect_claim_field_params_from_predicate(const jazz_engine_PredicateExpr *p, const jz_btree *param_types, jz_btree *params)
{
    /* Payload fields belong to the enum value, but they can still contain claim parameters. */
    FieldParamCtx c = { param_types, params };
    jz_err e = jazz_engine__query_engine__input__PredicateExpr__try_for_each_value(p, field_param_cb, &c);
    jz_err_drop(&e);
}

static groove_ColumnType uuid_type(void) { groove_ColumnType t; memset(&t, 0, sizeof t); t.tag = GCT_UUID; return t; }

static void jazz_node__node__query_eval__bindings__collect_claim_field_params_from_node(const jazz_engine_RowSetExpr *node, const jz_btree *param_types, jz_btree *params)
{
    switch (node->tag) {
    case JZ_RSE_FILTER:
    case JZ_RSE_JOIN:
    case JZ_RSE_CORRELATED_PATH_PROJECTION:
        jazz_node__node__query_eval__bindings__collect_claim_field_params_from_predicate(node->predicate, param_types, params);
        break;
    case JZ_RSE_RECURSIVE:
        jazz_node__node__query_eval__bindings__collect_claim_field_param_authoritative(&node->frontier_key, uuid_type(), params);
        for (u32 i = 0; i < node->dedupe_keys.len; i++)
            jazz_node__node__query_eval__bindings__collect_claim_field_param_authoritative(&node->dedupe_keys.ptr[i], uuid_type(), params);
        break;
    case JZ_RSE_PROJECT:
        for (u32 i = 0; i < node->projections.len; i++) {
            groove_ColumnType t;
            groove_ColumnType_clone(&t, &node->projections.ptr[i].output.ty);
            jazz_node__node__query_eval__bindings__collect_claim_field_param_authoritative(&node->projections.ptr[i].value, t, params);
        }
        break;
    case JZ_RSE_DISTINCT:
        for (u32 i = 0; i < node->keys.len; i++)
            jazz_node__node__query_eval__bindings__collect_claim_field_param_authoritative(&node->keys.ptr[i], uuid_type(), params);
        break;
    case JZ_RSE_ORDER_BY:
        for (u32 i = 0; i < node->order_keys.len; i++)
            jazz_node__node__query_eval__bindings__collect_claim_field_param_authoritative(&node->order_keys.ptr[i].value, uuid_type(), params);
        break;
    case JZ_RSE_SLICE:
        for (u32 i = 0; i < node->partition_by.len; i++)
            jazz_node__node__query_eval__bindings__collect_claim_field_param_authoritative(&node->partition_by.ptr[i], uuid_type(), params);
        for (u32 i = 0; i < node->tie_breaker.len; i++)
            jazz_node__node__query_eval__bindings__collect_claim_field_param_authoritative(&node->tie_breaker.ptr[i], uuid_type(), params);
        break;
    case JZ_RSE_AGGREGATE:
        for (u32 i = 0; i < node->group_by.len; i++)
            jazz_node__node__query_eval__bindings__collect_claim_field_param_authoritative(&node->group_by.ptr[i], uuid_type(), params);
        for (u32 i = 0; i < node->outputs.len; i++) {
            const jazz_engine_AggregateExpr *o = &node->outputs.ptr[i];
            if (!o->has_input) continue;
            groove_ColumnType t;
            groove_ColumnType_clone(&t, &o->output.ty);
            jazz_node__node__query_eval__bindings__collect_claim_field_param_authoritative(&o->input, t, params);
        }
        break;
    case JZ_RSE_VALUE_SOURCE: case JZ_RSE_FRONTIER_SOURCE: case JZ_RSE_SOURCE: case JZ_RSE_UNION:
    default:
        break;
    }
}

jz_btree jazz_node__node__query_eval__bindings__binding_claim_params_for_shape(const jazz_engine_NormalizedRowSetShape *shape, const jz_btree *param_types)
{
    jz_btree params;
    jz_btree_init(&params, &jazz_node_qeb_STR_CLAIM_PARAM_DESC);
    void *k; jazz_engine_RowSetExpr *node;
    JZ_BTREE_FOR(&shape->nodes, k, node) {
        if (node->tag == JZ_RSE_VALUE_SOURCE && node->vs_mode == JZ_VSM_BINDING) {
            for (u32 i = 0; i < node->columns.len; i++) {
                const jazz_engine_ValueSourceColumn *c = &node->columns.ptr[i];
                if (c->value.tag != JZ_NVR_CLAIM) continue;
                groove_ColumnType t;
                groove_ColumnType_clone(&t, &c->ty);
                insert_claim_param(&params, jazz_engine__query_engine__fields__claim_param_field(&c->value.claim), claim_path_clone(&c->value.claim), t);
            }
        }
        jazz_node__node__query_eval__bindings__collect_claim_field_params_from_node(node, param_types, &params);
    }
    return params;
}

jz_btree jazz_node__node__query_eval__bindings__normalized_source_tables(const jazz_engine_NormalizedRowSetShape *shape)
{
    jz_btree out;
    jz_btree_init(&out, &jazz_node_qeb_STR_SET_DESC);
    void *k; jazz_engine_RowSetExpr *node;
    JZ_BTREE_FOR(&shape->nodes, k, node) {
        if (node->tag != JZ_RSE_SOURCE) continue;
        jz_str t = jz_str_clone(&node->source.table);
        if (jz_btree_contains(&out, &t)) jz_str_drop(&t); else jz_btree_insert(&out, &t, NULL, NULL);
    }
    jazz_engine_SourceId *src; void *unused;
    JZ_BTREE_FOR(&shape->auxiliary_sources, src, unused) {
        jz_str t = jz_str_clone(&src->table);
        if (jz_btree_contains(&out, &t)) jz_str_drop(&t); else jz_btree_insert(&out, &t, NULL, NULL);
    }
    return out;
}

static jz_err collect_seed(const jazz_model_JazzSchema *schema, const jazz_model_ReachableVia *reachable, jz_btree *params)
{
    const jazz_model_ReachableSeed *seed = reachable->seed;
    if (!seed || !seed->has_user_column || !seed->has_user_claim) return JZ_OK;
    groove_ColumnType ty;
    JZ_TRY(jazz_node__node__query_eval__normalization__schema_column_type(schema, &seed->table, &seed->user_column, &ty));
    jazz_engine_ClaimPath path;
    path.segs = jazz_model__query__policy_and_request_contracts__operand_claim_path(&seed->user_claim);
    insert_claim_param(params, jazz_engine__query_engine__fields__claim_param_field(&path), path, ty);
    return JZ_OK;
}

jz_err jazz_node__node__query_eval__bindings__collect_reachable_seed_claim_params(const jazz_model_JazzSchema *schema, const jazz_model_Query *query, jz_btree *params)
{
    for (u32 i = 0; i < query->reachable.len; i++) JZ_TRY(collect_seed(schema, &query->reachable.ptr[i], params));
    for (u32 b = 0; b < query->policy_branches.len; b++) {
        const jazz_model_PolicyBranch *branch = &query->policy_branches.ptr[b];
        for (u32 i = 0; i < branch->reachable.len; i++) JZ_TRY(collect_seed(schema, &branch->reachable.ptr[i], params));
    }
    return JZ_OK;
}

/* ---------- NodeState program bindings ---------- */
jazz_engine_ProgramBinding jazz_node__node__query_eval__bindings__NodeState__program_binding_for_shape(
    const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    const jz_str *source_shape, jz_btree extra_user_params, jz_btree claim_params)
{
    jazz_engine_ProgramBinding pb;
    memset(&pb, 0, sizeof pb);
    jz_btree_clone(&pb.param_types, &shape->params, jazz_node_qeb_str_clone, jazz_node_qeb_column_type_clone);
    jz_str *k; groove_ColumnType *v;
    JZ_BTREE_FOR(&extra_user_params, k, v) {
        jz_str kk = jz_str_clone(k);
        groove_ColumnType vv, old;
        groove_ColumnType_clone(&vv, v);
        if (jz_btree_insert(&pb.param_types, &kk, &vv, &old)) { jz_str_drop(&kk); groove_ColumnType_drop(&old); }
    }
    pb.id = jazz_model__query__validation__Binding__binding_id(binding);
    if (source_shape) { pb.has_source_shape = true; pb.source_shape = jz_str_clone(source_shape); }
    pb.extra_user_params = extra_user_params;
    pb.claim_params = claim_params;
    jz_btree_clone(&pb.values, &binding->values, jazz_node_qeb_str_clone, jazz_node_qeb_value_clone);
    return pb;
}

jz_err jazz_node__node__query_eval__bindings__NodeState__program_binding_for_shape_and_policy(
    const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    const jz_str *source_shape, jz_btree extra_user_params, jz_btree claim_params, const jazz_engine_PolicyContext *policy,
    jazz_engine_ProgramBinding *out)
{
    return jazz_node__node__query_eval__bindings__NodeState__program_binding_for_shape_and_policy_with_prepared_claim_mode(
        self, shape, binding, source_shape, extra_user_params, claim_params, policy, JZN_PCBM_STRICT, out);
}

static jz_str join_claim_path(const jazz_engine_ClaimPath *p)
{
    jz_str out = {0};
    for (u32 i = 0; i < p->segs.len; i++) { if (i) jz_str_push_char(&out, '.'); jz_str_push(&out, p->segs.ptr[i].ptr, p->segs.ptr[i].len); }
    return out;
}

jz_err jazz_node__node__query_eval__bindings__NodeState__program_binding_for_shape_and_policy_with_prepared_claim_mode(
    const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    const jz_str *source_shape, jz_btree extra_user_params, jz_btree claim_params, const jazz_engine_PolicyContext *policy,
    jazz_node_PreparedClaimBindingMode mode, jazz_engine_ProgramBinding *out)
{
    /* System authority bypasses row policy entirely; derive a replacement
     * binding-source key from its ordinary query/user parameters only. */
    jz_str replaced_shape;
    bool has_replaced = false;
    const jz_str *effective_shape = source_shape;
    if (policy->tag == JZ_PC_SYSTEM) {
        jz_btree param_types;
        jz_btree_clone(&param_types, &shape->params, jazz_node_qeb_str_clone, jazz_node_qeb_column_type_clone);
        jz_str *k; groove_ColumnType *v;
        JZ_BTREE_FOR(&extra_user_params, k, v) {
            jz_str kk = jz_str_clone(k);
            groove_ColumnType vv, old;
            groove_ColumnType_clone(&vv, v);
            if (jz_btree_insert(&param_types, &kk, &vv, &old)) { jz_str_drop(&kk); groove_ColumnType_drop(&old); }
        }
        effective_shape = NULL;
        if (source_shape) {
            jz_btree none;
            jz_btree_init(&none, &jazz_node_qeb_STR_CLAIM_PARAM_DESC);
            has_replaced = jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_parts_if_needed(&param_types, &none, &replaced_shape);
            jz_btree_drop(&none);
            if (has_replaced) effective_shape = &replaced_shape;
        }
        jz_btree_drop(&param_types);
        jz_btree_drop(&claim_params);
        jz_btree_init(&claim_params, &jazz_node_qeb_STR_CLAIM_PARAM_DESC);
    }
    jz_btree claims_copy;
    jz_btree_clone(&claims_copy, &claim_params, jazz_node_qeb_str_clone, NULL);
    {
        /* clone values of the claim params properly */
        jz_btree_drop(&claims_copy);
        jz_btree_init(&claims_copy, &jazz_node_qeb_STR_CLAIM_PARAM_DESC);
        jz_str *k; jazz_engine_ProgramClaimParam *v;
        JZ_BTREE_FOR(&claim_params, k, v) {
            jazz_engine_ProgramClaimParam c;
            jazz_engine_ProgramClaimParam_clone(&c, v);
            jz_str kk = jz_str_clone(k);
            jz_btree_insert(&claims_copy, &kk, &c, NULL);
        }
    }
    jazz_engine_ProgramBinding pb = jazz_node__node__query_eval__bindings__NodeState__program_binding_for_shape(
        self, shape, binding, effective_shape, extra_user_params, claims_copy);
    if (has_replaced) jz_str_drop(&replaced_shape);
    jz_err e = JZ_OK;
    if (claim_params.len) {
        jz_btree values;
        jz_btree_clone(&values, &binding->values, jazz_node_qeb_str_clone, jazz_node_qeb_value_clone);
        jz_str *name; jazz_engine_ProgramClaimParam *claim;
        JZ_BTREE_FOR(&claim_params, name, claim) {
            bool has;
            groove_Value value;
            e = jazz_node__node__query_eval__prepared_bindings__prepared_claim_value(&claim->path, policy, &has, &value);
            if (JZ_IS_ERR(e)) break;
            if (!has) {
                jz_str joined = join_claim_path(&claim->path);
                if (mode == JZN_PCBM_FAIL_CLOSED_AUTHORIZATION_SUPPORT)
                    e = jz_err_fmt(JZN_E_AuthorizationSupportMissingClaim, "authorization support policy claim is not bound: %.*s", (int)joined.len, joined.ptr);
                else if (policy->tag == JZ_PC_AUTHORIZATION_SUBPLAN)
                    /* An absent claim cannot establish a policy proof. */
                    e = jazz_node__node__Error_query_capability(jz_format("policy authorization requires unbound claim %.*s", (int)joined.len, joined.ptr));
                else
                    e = JZN_INVALID_STORED("claim prepared param is not bound");
                jz_str_drop(&joined);
                break;
            }
            jz_str kk = jz_str_clone(name);
            groove_Value coerced = jazz_engine__query_engine__coerce_prepared_binding_value(value, &claim->ty), old;
            if (jz_btree_insert(&values, &kk, &coerced, &old)) { jz_str_drop(&kk); groove_Value_drop(&old); }
        }
        if (!JZ_IS_ERR(e)) e = jazz_model__query__validation__binding_id_for_values(&values, &pb.id);
        if (JZ_IS_ERR(e)) {
            jz_btree_drop(&values);
        } else {
            jz_btree_drop(&pb.values);
            pb.values = values;
        }
    }
    jz_btree_drop(&claim_params);
    if (JZ_IS_ERR(e)) {
        if (pb.has_source_shape) jz_str_drop(&pb.source_shape);
        jz_btree_drop(&pb.extra_user_params); jz_btree_drop(&pb.param_types); jz_btree_drop(&pb.claim_params); jz_btree_drop(&pb.values);
        return e;
    }
    *out = pb;
    return JZ_OK;
}
