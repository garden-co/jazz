/* query/relation_resolution.rs: normalize the relation facade subset into ordinary Query shapes. */
#include "jazz_model/jazz_model.h"

#define Q(x) jazz_model__query__##x
#define VDROP(v, N) jz_vec_drop(&(v), sizeof(*(v).ptr), Q(N##__drop_v))
#define VCLONE(d, s, N) jz_vec_clone(&(d), &(s), sizeof(*(s).ptr), Q(N##__Clone__clone_v))

static jz_err unify_err(const char *message) {
    return Q(QueryError__new)(JAZZ_QE_UNSUPPORTED_RELATION_QUERY, "unsupported relation query: %s", message);
}

static jz_str scstr(const char *s) { return jz_str_from(s); }
static bool opt_str_is(const jazz_model_OptStr *o, const jz_str *s) { return o->some && jz_str_eq(&o->s, s); }

/* ---------------------------------------------------------------- plan */
static void predvec_drop_v(void *v) { VDROP(*(jazz_model_PredicateVec *)v, Predicate); }
static const jz_btree_desc STR_PREDVEC_DESC = {sizeof(jz_str), sizeof(jazz_model_PredicateVec), jz_cmp_str, jazz_model__str_drop_v, predvec_drop_v};
static void projvec_drop_v(void *v) { VDROP(*(jazz_model_RelationProjectColumnVec *)v, RelationProjectColumn); }
const jz_btree_desc jazz_model__query__STR_PROJECTION_DESC = {sizeof(jz_str), sizeof(jazz_model_RelationProjectColumnVec), jz_cmp_str,
                                                              jazz_model__str_drop_v, projvec_drop_v};

typedef struct RelationFacadeJoin { jz_str left_scope, left_column, right_scope, right_column; } RelationFacadeJoin;
typedef JZ_VEC(RelationFacadeJoin) RelationFacadeJoinVec;
static void facade_join_drop(RelationFacadeJoin *j) {
    jz_str_drop(&j->left_scope); jz_str_drop(&j->left_column); jz_str_drop(&j->right_scope); jz_str_drop(&j->right_column);
}
static void facade_join_drop_v(void *j) { facade_join_drop(j); }
static RelationFacadeJoin facade_join_clone(const RelationFacadeJoin *j) {
    return (RelationFacadeJoin){jz_str_clone(&j->left_scope), jz_str_clone(&j->left_column), jz_str_clone(&j->right_scope),
                                jz_str_clone(&j->right_column)};
}

typedef struct RelationFacadePlan {
    jz_btree tables_by_scope;  /* String -> String */
    jz_btree filters_by_scope; /* String -> Vec<Predicate> */
    RelationFacadeJoinVec joins;
    jazz_model_OptStr output_scope;
    bool has_output_projection; jazz_model_RelationProjectColumnVec output_projection;
    jazz_model_OrderByVec order_by;
    bool has_limit; u64 limit;
    u64 offset;
} RelationFacadePlan;

static void plan_init(RelationFacadePlan *p) {
    memset(p, 0, sizeof(*p));
    jz_btree_init(&p->tables_by_scope, &jazz_model__schema__STR_STR_DESC);
    jz_btree_init(&p->filters_by_scope, &STR_PREDVEC_DESC);
}
static void plan_drop(RelationFacadePlan *p) {
    jz_btree_drop(&p->tables_by_scope);
    jz_btree_drop(&p->filters_by_scope);
    jz_vec_drop(&p->joins, sizeof(RelationFacadeJoin), facade_join_drop_v);
    jazz_model__OptStr__drop(&p->output_scope);
    VDROP(p->output_projection, RelationProjectColumn);
    VDROP(p->order_by, OrderBy);
}

/* Compose one output window around an already-composed inner window. */
enum { STEP_OFFSET, STEP_LIMIT };
static void fold_relation_output_step(u64 *offset, bool *has_limit, u64 *limit, u8 step, u64 value) {
    if (step == STEP_OFFSET) {
        u64 o = *offset + value;
        *offset = o < *offset ? UINT64_MAX : o;
        if (*has_limit) *limit = *limit > value ? *limit - value : 0;
    } else {
        *limit = *has_limit ? (*limit < value ? *limit : value) : value;
        *has_limit = true;
    }
}

/* Query builder steps inlined (Query::order_by / limit / offset) */
static void query_order_by(jazz_model_Query *q, jz_str column, u8 direction) {
    jazz_model_OrderBy *o = JAZZ_VEC_SLOT(q->order_by);
    o->column = column; o->direction = direction;
}

/* ---------------------------------------------------------------- forward decls */
static jz_err collect_relation_facade(const jazz_model_RelationExpr *expr, RelationFacadePlan *plan);
static jz_err relation_gather_to_query(const jazz_model_RelationExpr *expr, bool *some, jazz_model_Query *out);
static jz_err relation_path_join_from_output(RelationFacadePlan *plan, const jz_str *output_scope, jazz_model_JoinVia *out);
static jz_err relation_predicate_to_query_predicate(const jazz_model_RelationPredicate *p, bool *some, jz_str *scope, jazz_model_Predicate *out);
static jz_err relation_projection_from_expr(const jazz_model_RelationExpr *expr, bool *some, jz_str *scope, jazz_model_RelationProjectColumnVec *cols);
static jz_err relation_value_to_operand(const jazz_model_RelationValueRef *v, jazz_model_Operand *out);
typedef struct PeeledSteps {
    const jazz_model_RelationExpr *base;
    JZ_VEC(const jazz_model_RelationPredicate *) filters;
    jazz_model_RelationOrderByVec order_by;
    u64 offset; bool has_limit; u64 limit;
} PeeledSteps;
static void peeled_drop(PeeledSteps *s) {
    jz_vec_drop(&s->filters, sizeof(void *), NULL);
    VDROP(s->order_by, RelationOrderBy);
}
static jz_err peel_relation_output_steps(const jazz_model_RelationExpr *expr, PeeledSteps *out);

/* ---------------------------------------------------------------- relation_query_to_query */
jz_err Q(relation_query_to_query)(const jazz_model_RelationQuery *query, jazz_model_Query *out) {
    jazz_model_RelationUnionParts parts;
    if (Q(relation_union_parts)(&query->rel, &parts)) {
        jz_err e = JZ_OK;
        if (parts.inputs.len == 0) { Q(RelationUnionParts__drop)(&parts); return unify_err("union requires at least one labeled input"); }
        jz_btree labels;
        jz_btree_init(&labels, &jazz_model__STR_SET_DESC);
        jazz_model_OptStr output = {0};
        JZ_VEC_FOR(const jazz_model_RelationUnionArm, arm, parts.inputs) {
            bool bad = arm->label.len == 0 || arm->label.len > 4096 || memchr(arm->label.ptr, 0, arm->label.len) != NULL;
            if (!bad) {
                jz_str k = jz_str_clone(&arm->label);
                if (jz_btree_insert(&labels, &k, NULL, NULL)) { bad = true; }
            }
            if (bad) { e = unify_err("union arm labels must be 1..=4096 bytes, NUL-free, and unique"); break; }
            jazz_model_RelationQuery arm_rel;
            Q(RelationExpr__Clone__clone)(&arm_rel.rel, &arm->input);
            jazz_model_Query arm_query;
            memset(&arm_query, 0, sizeof(arm_query));
            e = Q(relation_query_to_query)(&arm_rel, &arm_query);
            Q(RelationQuery__drop)(&arm_rel);
            if (JZ_IS_ERR(e)) break;
            if (output.some && !jz_str_eq(&output.s, &arm_query.table)) {
                Q(Query__drop)(&arm_query);
                e = unify_err("union inputs must output the same table");
                break;
            }
            if (!output.some) { output.some = true; output.s = jz_str_clone(&arm_query.table); }
            Q(Query__drop)(&arm_query);
        }
        jz_btree_drop(&labels);
        Q(RelationUnionParts__drop)(&parts);
        if (JZ_IS_ERR(e)) { jazz_model__OptStr__drop(&output); return e; }
        JZ_ASSERT(output.some, "non-empty union has an output table");
        *out = Q(Query__from)(output.s, (jazz_model_OptStr){0});
        out->relation = JZ_NEW(jazz_model_RelationQuery);
        Q(RelationQuery__Clone__clone)(out->relation, query);
        return JZ_OK;
    }
    bool gathered = false;
    JZ_TRY(relation_gather_to_query(&query->rel, &gathered, out));
    if (gathered) return JZ_OK;

    RelationFacadePlan plan;
    plan_init(&plan);
    jz_err e = collect_relation_facade(&query->rel, &plan);
    if (JZ_IS_ERR(e)) { plan_drop(&plan); return e; }
    if (!plan.output_scope.some) { plan_drop(&plan); return unify_err("relation query must end in a project over one output table"); }
    jz_str output_scope = jz_str_clone(&plan.output_scope.s);
    const jz_str *table = jz_btree_get(&plan.tables_by_scope, &output_scope);
    if (!table) { jz_str_drop(&output_scope); plan_drop(&plan); return unify_err("relation query output scope is unknown"); }
    jazz_model_Query q = Q(Query__from)(jz_str_clone(table), (jazz_model_OptStr){0});
    q.relation = JZ_NEW(jazz_model_RelationQuery);
    Q(RelationQuery__Clone__clone)(q.relation, query);

    jazz_model_PredicateVec filters = {0};
    jz_str removed_key;
    if (jz_btree_remove(&plan.filters_by_scope, &output_scope, &removed_key, &filters)) jz_str_drop(&removed_key);
    JZ_VEC_FOR(jazz_model_Predicate, f, filters) Q(Query__filter)(&q, *f);
    jz_vec_drop(&filters, sizeof(jazz_model_Predicate), NULL); /* moved out */

    if (plan.joins.len) {
        jazz_model_JoinVia join;
        e = relation_path_join_from_output(&plan, &output_scope, &join);
        if (JZ_IS_ERR(e)) goto fail;
        JZ_VEC_PUSH(q.joins, join);
    }
    if (plan.filters_by_scope.len) { e = unify_err("relation query filters on non-adjacent scopes are not unified yet"); goto fail; }
    JZ_VEC_FOR(jazz_model_OrderBy, o, plan.order_by) query_order_by(&q, jz_str_clone(&o->column), o->direction);
    if (plan.has_limit) { q.has_limit = true; q.limit = plan.limit; }
    if (plan.offset != 0) q.offset = plan.offset;
    jz_str_drop(&output_scope);
    plan_drop(&plan);
    *out = q;
    return JZ_OK;
fail:
    jz_str_drop(&output_scope);
    plan_drop(&plan);
    Q(Query__drop)(&q);
    return e;
}

/* ---------------------------------------------------------------- output projections */
jz_err Q(relation_output_projection)(const jazz_model_RelationQuery *relation, jz_str *scope, jazz_model_RelationProjectColumnVec *columns) {
    jazz_model_RelationUnionParts parts;
    if (Q(relation_union_parts)(&relation->rel, &parts)) {
        Q(RelationUnionParts__drop)(&parts);
        bool some = false;
        JZ_TRY(relation_projection_from_expr(&relation->rel, &some, scope, columns));
        if (!some) return unify_err("relation query must define an output projection");
        jz_err e = Q(validate_relation_projection_aliases)(columns);
        /* A UNION always retains its arm projections. */
        if (!JZ_IS_ERR(e)) e = Q(reject_reserved_relation_aliases)(columns);
        if (JZ_IS_ERR(e)) { jz_str_drop(scope); VDROP(*columns, RelationProjectColumn); }
        return e;
    }
    RelationFacadePlan plan;
    plan_init(&plan);
    jz_err e = collect_relation_facade(&relation->rel, &plan);
    if (!JZ_IS_ERR(e) && !plan.output_scope.some) e = unify_err("relation query must end in a project over one output table");
    if (!JZ_IS_ERR(e) && !plan.has_output_projection) e = unify_err("relation query must define an output projection");
    if (!JZ_IS_ERR(e)) {
        *scope = plan.output_scope.s;
        plan.output_scope = (jazz_model_OptStr){0};
        *columns = plan.output_projection;
        plan.output_projection = (jazz_model_RelationProjectColumnVec){0};
    }
    plan_drop(&plan);
    return e;
}

jz_err Q(relation_output_projection_if_present)(const jazz_model_RelationQuery *relation, bool *some, jazz_model_RelationProjectColumnVec *columns) {
    PeeledSteps steps;
    JZ_TRY(peel_relation_output_steps(&relation->rel, &steps));
    bool gather = steps.base->tag == JAZZ_REXPR_GATHER;
    peeled_drop(&steps);
    if (gather) { *some = false; return JZ_OK; }
    jz_str scope = {0};
    JZ_TRY(Q(relation_output_projection)(relation, &scope, columns));
    jz_str_drop(&scope);
    *some = true;
    return JZ_OK;
}

static bool relation_expr_contains_union(const jazz_model_RelationExpr *expr) {
    switch (expr->tag) {
    case JAZZ_REXPR_UNION: return true;
    case JAZZ_REXPR_PROJECT: case JAZZ_REXPR_FILTER: case JAZZ_REXPR_ORDER_BY: case JAZZ_REXPR_OFFSET: case JAZZ_REXPR_LIMIT:
        return relation_expr_contains_union(expr->input);
    case JAZZ_REXPR_JOIN: return relation_expr_contains_union(expr->input) || relation_expr_contains_union(expr->right);
    default: return false;
    }
}

static jz_err insert_leaf_projection(jz_btree *projections, const jz_str *label, jazz_model_RelationProjectColumnVec columns) {
    jz_str k = jz_str_clone(label);
    jazz_model_RelationProjectColumnVec old;
    if (jz_btree_insert(projections, &k, &columns, &old)) {
        VDROP(old, RelationProjectColumn);
        return unify_err("UNION leaf projection labels must be unique");
    }
    return JZ_OK;
}

static jz_err collect_relation_union_leaf_projections(const jazz_model_RelationExpr *expr, const jz_str *label,
                                                      const jazz_model_RelationProjectColumnVec *inherited, jz_btree *projections) {
    jazz_model_RelationProjectColumnVec cols = {0};
    switch (expr->tag) {
    case JAZZ_REXPR_UNION:
        JZ_VEC_FOR(const jazz_model_RelationUnionArm, arm, expr->inputs) {
            jz_str arm_label = label ? Q(compose_union_arm_path)(jz_str_slice(label), jz_str_slice(&arm->label)) : jz_str_clone(&arm->label);
            jz_err e = collect_relation_union_leaf_projections(&arm->input, &arm_label, inherited, projections);
            jz_str_drop(&arm_label);
            JZ_TRY(e);
        }
        return JZ_OK;
    case JAZZ_REXPR_PROJECT:
        if (relation_expr_contains_union(expr->input))
            return collect_relation_union_leaf_projections(expr->input, label, inherited ? inherited : &expr->columns, projections);
        if (!label) return unify_err("UNION leaf projection has no arm label");
        VCLONE(cols, *(inherited ? inherited : &expr->columns), RelationProjectColumn);
        return insert_leaf_projection(projections, label, cols);
    case JAZZ_REXPR_FILTER: case JAZZ_REXPR_ORDER_BY: case JAZZ_REXPR_OFFSET: case JAZZ_REXPR_LIMIT:
        return collect_relation_union_leaf_projections(expr->input, label, inherited, projections);
    default: { /* TableScan | Join | Gather | Distinct */
        if (!label) return unify_err("UNION leaf has no output projection");
        if (inherited) {
            VCLONE(cols, *inherited, RelationProjectColumn);
        } else {
            bool some = false;
            jz_str scope = {0};
            JZ_TRY(relation_projection_from_expr(expr, &some, &scope, &cols));
            if (!some) return unify_err("UNION leaf has no output projection");
            jz_str_drop(&scope);
        }
        return insert_leaf_projection(projections, label, cols);
    }
    }
}

jz_err Q(relation_union_leaf_projections)(const jazz_model_RelationQuery *relation, jz_btree *out) {
    jz_btree_init(out, &jazz_model__query__STR_PROJECTION_DESC);
    jz_err e = collect_relation_union_leaf_projections(&relation->rel, NULL, NULL, out);
    if (JZ_IS_ERR(e)) jz_btree_drop(out);
    return e;
}

/* Encode a nested semantic UNION arm path without relying on a user label separator. */
jz_str Q(compose_union_arm_path)(jz_slice prefix, jz_slice label) {
    return jz_format("%u:%.*s%u:%.*s", prefix.len, (int)prefix.len, prefix.ptr, label.len, (int)label.len, label.ptr);
}

static jz_err relation_projection_from_expr(const jazz_model_RelationExpr *expr, bool *some, jz_str *scope, jazz_model_RelationProjectColumnVec *cols) {
    switch (expr->tag) {
    case JAZZ_REXPR_PROJECT:
        JZ_VEC_FOR(const jazz_model_RelationProjectColumn, c, expr->columns) {
            if (c->expr.tag == JAZZ_KEYREF_COLUMN && c->expr.column.scope.some) {
                *scope = jz_str_clone(&c->expr.column.scope.s);
                VCLONE(*cols, expr->columns, RelationProjectColumn);
                *some = true;
                return JZ_OK;
            }
        }
        return relation_projection_from_expr(expr->input, some, scope, cols);
    case JAZZ_REXPR_FILTER: case JAZZ_REXPR_ORDER_BY: case JAZZ_REXPR_OFFSET: case JAZZ_REXPR_LIMIT:
        return relation_projection_from_expr(expr->input, some, scope, cols);
    case JAZZ_REXPR_UNION:
        if (expr->inputs.len == 0) { *some = false; return JZ_OK; }
        return relation_projection_from_expr(&expr->inputs.ptr[0].input, some, scope, cols);
    default: *some = false; return JZ_OK;
    }
}

/* ---------------------------------------------------------------- aliases */
static const char *const RESERVED_RELATION_ALIASES[] = {"row_uuid", "tx_time", "tx_node_id"};
static const char *const RESERVED_RELATION_ALIAS_PREFIXES[] = {"$", JAZZ_APP_COLUMN_PREFIX, "__", "left.", "right."};

jz_err Q(validate_relation_projection_aliases)(const jazz_model_RelationProjectColumnVec *columns) {
    jz_btree aliases;
    jz_btree_init(&aliases, &jazz_model__STR_SET_DESC);
    jz_err e = JZ_OK;
    JZ_VEC_FOR(const jazz_model_RelationProjectColumn, c, *columns) {
        bool bad = c->alias.len == 0 || memchr(c->alias.ptr, 0, c->alias.len) != NULL;
        if (!bad) { jz_str k = jz_str_clone(&c->alias); bad = jz_btree_insert(&aliases, &k, NULL, NULL); }
        if (bad) { e = unify_err("relation project aliases must be non-empty, NUL-free, and unique"); break; }
    }
    jz_btree_drop(&aliases);
    return e;
}

jz_err Q(reject_reserved_relation_aliases)(const jazz_model_RelationProjectColumnVec *columns) {
    JZ_VEC_FOR(const jazz_model_RelationProjectColumn, c, *columns) {
        bool reserved = false;
        for (u32 i = 0; i < 3 && !reserved; i++) reserved = jz_str_eq_cstr(&c->alias, RESERVED_RELATION_ALIASES[i]);
        for (u32 i = 0; i < 5 && !reserved; i++) {
            u32 n = (u32)strlen(RESERVED_RELATION_ALIAS_PREFIXES[i]);
            reserved = c->alias.len >= n && memcmp(c->alias.ptr, RESERVED_RELATION_ALIAS_PREFIXES[i], n) == 0;
        }
        if (reserved) {
            jz_str dbg = {0};
            jazz_model__debug_str(&dbg, c->alias.ptr, c->alias.len);
            jz_err e = Q(QueryError__new)(JAZZ_QE_UNSUPPORTED_RELATION_QUERY,
                "unsupported relation query: relation project alias %.*s is a reserved internal name; aliases must not "
                "be row_uuid, tx_time or tx_node_id, or start with \"$\", \"%s\", \"__\", \"left.\" or \"right.\"",
                (int)dbg.len, dbg.ptr, JAZZ_APP_COLUMN_PREFIX);
            jz_str_drop(&dbg);
            return e;
        }
    }
    return JZ_OK;
}

bool Q(relation_union_presentation_order)(const jazz_model_RelationQuery *relation, jazz_model_OrderByVec *out) {
    jazz_model_RelationUnionParts parts;
    if (!Q(relation_union_parts)(&relation->rel, &parts)) return false;
    bool some = parts.has_order_by;
    if (some) {
        memset(out, 0, sizeof(*out));
        JZ_VEC_FOR(const jazz_model_RelationOrderBy, t, parts.order_by) {
            jazz_model_OrderBy *o = JAZZ_VEC_SLOT(*out);
            o->column = jz_str_clone(&t->column.column);
            o->direction = t->direction;
        }
    }
    Q(RelationUnionParts__drop)(&parts);
    return some;
}

/* ---------------------------------------------------------------- union parts */
void Q(RelationUnionParts__drop)(jazz_model_RelationUnionParts *p) {
    VDROP(p->inputs, RelationUnionArm);
    VDROP(p->order_by, RelationOrderBy);
}
static bool union_parts_has_terminal_steps(const jazz_model_RelationUnionParts *p) {
    return p->has_order_by || p->has_offset || p->has_limit;
}

bool Q(relation_union_parts)(const jazz_model_RelationExpr *expr, jazz_model_RelationUnionParts *out) {
    switch (expr->tag) {
    case JAZZ_REXPR_UNION:
        memset(out, 0, sizeof(*out));
        VCLONE(out->inputs, expr->inputs, RelationUnionArm);
        return true;
    case JAZZ_REXPR_FILTER:
    case JAZZ_REXPR_PROJECT: {
        if (!Q(relation_union_parts)(expr->input, out)) return false;
        if (union_parts_has_terminal_steps(out)) { Q(RelationUnionParts__drop)(out); return false; }
        JZ_VEC_FOR(jazz_model_RelationUnionArm, arm, out->inputs) {
            jazz_model_RelationExpr wrapped;
            memset(&wrapped, 0, sizeof(wrapped));
            wrapped.tag = expr->tag;
            wrapped.input = Q(RelationExpr__box)(arm->input);
            if (expr->tag == JAZZ_REXPR_FILTER) Q(RelationPredicate__Clone__clone)(&wrapped.predicate, &expr->predicate);
            else VCLONE(wrapped.columns, expr->columns, RelationProjectColumn);
            arm->input = wrapped;
        }
        return true;
    }
    case JAZZ_REXPR_ORDER_BY:
        if (!Q(relation_union_parts)(expr->input, out)) return false;
        if (union_parts_has_terminal_steps(out)) { Q(RelationUnionParts__drop)(out); return false; }
        out->has_order_by = true;
        VCLONE(out->order_by, expr->terms, RelationOrderBy);
        return true;
    case JAZZ_REXPR_OFFSET:
        if (!Q(relation_union_parts)(expr->input, out)) return false;
        if (out->has_offset || out->has_limit) { Q(RelationUnionParts__drop)(out); return false; }
        out->has_offset = true; out->offset = expr->n;
        return true;
    case JAZZ_REXPR_LIMIT:
        if (!Q(relation_union_parts)(expr->input, out)) return false;
        if (out->has_limit) { Q(RelationUnionParts__drop)(out); return false; }
        out->has_limit = true; out->limit = expr->n;
        return true;
    default: return false;
    }
}

/* ---------------------------------------------------------------- gather */
static bool key_is_current_row(const jazz_model_RelationKeyRef *k) {
    return k->tag == JAZZ_KEYREF_ROW_ID && k->row_id == JAZZ_ROWID_CURRENT;
}

static jz_err relation_gather_seed(const jazz_model_RelationExpr *seed, jz_str *table_out, jazz_model_PredicateVec *filters);
static jz_err relation_gather_step(const jazz_model_RelationExpr *step, const jz_str *seed_table, jz_str *edge_table,
                                   jz_str *edge_member_column, jz_str *edge_parent_column, jazz_model_PredicateVec *edge_filters);

static jz_err relation_gather_to_query(const jazz_model_RelationExpr *expr, bool *some, jazz_model_Query *out) {
    PeeledSteps steps;
    JZ_TRY(peel_relation_output_steps(expr, &steps));
    const jazz_model_RelationExpr *g = steps.base;
    if (g->tag != JAZZ_REXPR_GATHER) { peeled_drop(&steps); *some = false; return JZ_OK; }
    jz_err e = JZ_OK;
    if (!key_is_current_row(&g->frontier_key) || g->keys.len != 1 || !key_is_current_row(&g->keys.ptr[0])) {
        peeled_drop(&steps);
        return unify_err("gather requires current-row frontier and dedupe keys");
    }
    jz_str seed_table = {0};
    jazz_model_PredicateVec seed_filters = {0};
    e = relation_gather_seed(g->input, &seed_table, &seed_filters);
    if (JZ_IS_ERR(e)) { peeled_drop(&steps); return e; }
    jz_str edge_table = {0}, edge_member = {0}, edge_parent = {0};
    jazz_model_PredicateVec edge_filters = {0};
    e = relation_gather_step(g->right, &seed_table, &edge_table, &edge_member, &edge_parent, &edge_filters);
    if (JZ_IS_ERR(e)) { jz_str_drop(&seed_table); VDROP(seed_filters, Predicate); peeled_drop(&steps); return e; }

    jazz_model_Query q = Q(Query__from)(jz_str_clone(&seed_table), (jazz_model_OptStr){0});
    for (u32 i = 0; i < steps.filters.len; i++) {
        bool has = false;
        jz_str scope = {0};
        jazz_model_Predicate filter;
        e = relation_predicate_to_query_predicate(steps.filters.ptr[i], &has, &scope, &filter);
        if (JZ_IS_ERR(e)) goto fail;
        if (!has) continue;
        if (scope.len != 0 && !jz_str_eq(&scope, &seed_table)) {
            jz_str_drop(&scope); Q(Predicate__drop)(&filter);
            e = unify_err("gather output filters must be scoped to the gathered table");
            goto fail;
        }
        jz_str_drop(&scope);
        Q(Query__filter)(&q, filter);
    }
    {
        /* Treat each candidate output row as its own access row. The reachability closure then acts
         * as a membership filter over that same table, yielding the seed rows and every reached row. */
        jazz_model_ReachableVia *rv = JAZZ_VEC_SLOT(q.reachable);
        rv->access_table = jz_str_clone(&seed_table);
        rv->access_row_column = scstr("id");
        rv->access_team_column = scstr("id");
        rv->access_team_target = JAZZ_JOIN_TARGET_ROW_ID;
        rv->from.tag = JAZZ_OPERAND_LITERAL;
        rv->from.literal.tag = GROOVE_V_UUID;
        memset(&rv->from.literal.u.uuid, 0, sizeof(jz_uuid)); /* Uuid::nil() */
        rv->edge_table = edge_table;
        rv->edge_member_column = edge_member;
        rv->edge_parent_column = edge_parent;
        rv->edge_filters = edge_filters;
        rv->bound = g->bound;
        rv->seed = JZ_NEW(jazz_model_ReachableSeed);
        rv->seed->table = jz_str_clone(&seed_table);
        rv->seed->team_column = scstr("id");
        rv->seed->filters = seed_filters;
        seed_filters = (jazz_model_PredicateVec){0};
        edge_table = edge_member = edge_parent = (jz_str){0};
        edge_filters = (jazz_model_PredicateVec){0};
    }
    JZ_VEC_FOR(const jazz_model_RelationOrderBy, o, steps.order_by) {
        if (o->column.scope.some && !jz_str_eq(&o->column.scope.s, &seed_table)) {
            e = unify_err("gather order_by must be scoped to the gathered table");
            goto fail;
        }
        query_order_by(&q, jz_str_clone(&o->column.column), o->direction);
    }
    if (steps.has_limit) { q.has_limit = true; q.limit = steps.limit; }
    if (steps.offset != 0) q.offset = steps.offset;
    jz_str_drop(&seed_table);
    peeled_drop(&steps);
    *out = q;
    *some = true;
    return JZ_OK;
fail:
    Q(Query__drop)(&q);
    jz_str_drop(&seed_table);
    VDROP(seed_filters, Predicate);
    jz_str_drop(&edge_table); jz_str_drop(&edge_member); jz_str_drop(&edge_parent);
    VDROP(edge_filters, Predicate);
    peeled_drop(&steps);
    return e;
}

static jz_err peel_relation_output_steps(const jazz_model_RelationExpr *expr, PeeledSteps *out) {
    memset(out, 0, sizeof(*out));
    struct { u8 kind; u64 value; } steps[JAZZ_MAX_RELATION_DEPTH * 2];
    u32 nsteps = 0;
    JZ_VEC(struct { u8 kind; u64 value; }) more = {0};
    const jazz_model_RelationExpr *current = expr;
    for (;;) {
        if (out->filters.len && (current->tag == JAZZ_REXPR_OFFSET || current->tag == JAZZ_REXPR_LIMIT)) {
            jz_vec_drop(&more, sizeof(*more.ptr), NULL);
            peeled_drop(out);
            return unify_err("gather output filters cannot wrap limit or offset");
        }
        if (current->tag == JAZZ_REXPR_FILTER) {
            const jazz_model_RelationPredicate *p = &current->predicate;
            JZ_VEC_PUSH(out->filters, p);
        } else if (current->tag == JAZZ_REXPR_ORDER_BY) {
            JZ_VEC_FOR(const jazz_model_RelationOrderBy, t, current->terms)
                Q(RelationOrderBy__Clone__clone)(JAZZ_VEC_SLOT(out->order_by), t);
        } else if (current->tag == JAZZ_REXPR_OFFSET || current->tag == JAZZ_REXPR_LIMIT) {
            u8 kind = current->tag == JAZZ_REXPR_OFFSET ? STEP_OFFSET : STEP_LIMIT;
            if (nsteps < sizeof(steps) / sizeof(steps[0])) { steps[nsteps].kind = kind; steps[nsteps].value = current->n; nsteps++; }
            else { __typeof__(*more.ptr) s = {kind, current->n}; JZ_VEC_PUSH(more, s); }
        } else {
            break;
        }
        current = current->input;
    }
    /* output_steps.into_iter().rev().fold((0, None), fold_relation_output_step) */
    for (u32 i = more.len; i-- > 0;) fold_relation_output_step(&out->offset, &out->has_limit, &out->limit, more.ptr[i].kind, more.ptr[i].value);
    for (u32 i = nsteps; i-- > 0;) fold_relation_output_step(&out->offset, &out->has_limit, &out->limit, steps[i].kind, steps[i].value);
    jz_vec_drop(&more, sizeof(*more.ptr), NULL);
    out->base = current;
    return JZ_OK;
}

static jz_err relation_gather_seed(const jazz_model_RelationExpr *seed, jz_str *table_out, jazz_model_PredicateVec *filters) {
    const jazz_model_RelationExpr *current = seed;
    jz_err e = JZ_OK;
    while (current->tag == JAZZ_REXPR_FILTER) {
        bool has = false;
        jz_str scope = {0};
        jazz_model_Predicate p;
        e = relation_predicate_to_query_predicate(&current->predicate, &has, &scope, &p);
        if (JZ_IS_ERR(e)) goto fail;
        const jazz_model_RelationExpr *input = current->input;
        if (!has) { current = input; continue; }
        if (input->tag != JAZZ_REXPR_TABLE_SCAN) {
            jz_str_drop(&scope); Q(Predicate__drop)(&p);
            e = unify_err("gather seed filters must be directly over one table scan");
            goto fail;
        }
        const jz_str *expected_scope = input->alias.some ? &input->alias.s : &input->table;
        if (!jz_str_eq(&scope, expected_scope)) {
            jz_str_drop(&scope); Q(Predicate__drop)(&p);
            e = unify_err("gather seed filters must be scoped to the seed table");
            goto fail;
        }
        jz_str_drop(&scope);
        JZ_VEC_PUSH(*filters, p);
        current = input;
    }
    if (current->tag != JAZZ_REXPR_TABLE_SCAN) { e = unify_err("gather seed must be a table scan with optional filters"); goto fail; }
    if (current->alias.some) { e = unify_err("gather seed aliases are not unified yet"); goto fail; }
    *table_out = jz_str_clone(&current->table);
    return JZ_OK;
fail:
    VDROP(*filters, Predicate);
    return e;
}

static jz_err relation_gather_step_scan(const jazz_model_RelationExpr *input, const jazz_model_RelationExpr **out) {
    const jazz_model_RelationExpr *current = input;
    while (current->tag == JAZZ_REXPR_FILTER) current = current->input;
    if (current->tag == JAZZ_REXPR_TABLE_SCAN) { *out = current; return JZ_OK; }
    return unify_err("gather step filters must be directly over one table scan");
}

static jz_err relation_gather_step_predicates(const jazz_model_RelationPredicate *p, jazz_model_RelationPredicateVec *filters) {
    switch (p->tag) {
    case JAZZ_RPE_AND:
        JZ_VEC_FOR(const jazz_model_RelationPredicate, c, p->items) JZ_TRY(relation_gather_step_predicates(c, filters));
        return JZ_OK;
    case JAZZ_RPE_TRUE: return JZ_OK;
    default:
        Q(RelationPredicate__Clone__clone)(JAZZ_VEC_SLOT(*filters), p);
        return JZ_OK;
    }
}

static jz_err relation_gather_step_filters(const jazz_model_RelationExpr *input, jazz_model_RelationPredicateVec *filters) {
    const jazz_model_RelationExpr *current = input;
    while (current->tag == JAZZ_REXPR_FILTER) {
        jz_err e = relation_gather_step_predicates(&current->predicate, filters);
        if (JZ_IS_ERR(e)) { VDROP(*filters, RelationPredicate); return e; }
        current = current->input;
    }
    return JZ_OK;
}

static bool is_frontier_cmp(const jazz_model_RelationPredicate *p, const jz_str *edge_scope) {
    return p->tag == JAZZ_RPE_CMP && p->op == JAZZ_PCMP_EQ && p->right.tag == JAZZ_RVAL_ROW_ID &&
           p->right.row_id == JAZZ_ROWID_FRONTIER && opt_str_is(&p->left.scope, edge_scope);
}

static jz_err relation_gather_step(const jazz_model_RelationExpr *step, const jz_str *seed_table, jz_str *edge_table_out,
                                   jz_str *edge_member_column, jz_str *edge_parent_column, jazz_model_PredicateVec *edge_filters) {
    if (step->tag != JAZZ_REXPR_PROJECT) return unify_err("gather step must project its forward-hop target");
    const jazz_model_RelationExpr *input = step->input;
    if (input->tag != JAZZ_REXPR_JOIN || input->join_kind != JAZZ_JOIN_INNER)
        return unify_err("gather step must be an inner forward-hop join");
    const jazz_model_RelationExpr *scan;
    JZ_TRY(relation_gather_step_scan(input->input, &scan));
    const jz_str *edge_table = &scan->table;
    const jz_str *edge_scope = scan->alias.some ? &scan->alias.s : edge_table;
    const jazz_model_RelationExpr *right = input->right;
    if (right->tag != JAZZ_REXPR_TABLE_SCAN || !right->alias.some)
        return unify_err("gather step target must use a scoped table scan");
    const jz_str *target_alias = &right->alias.s;
    if (!jz_str_eq(&right->table, seed_table)) return unify_err("gather step must return rows from the seed table");
    if (input->on.len != 1) return unify_err("gather step requires exactly one forward-hop join condition");
    const jazz_model_RelationJoinCondition *condition = &input->on.ptr[0];
    if (!opt_str_is(&condition->left.scope, edge_scope) || !opt_str_is(&condition->right.scope, target_alias) ||
        !jz_str_eq_cstr(&condition->right.column, "id"))
        return unify_err("gather step join must connect its table FK to the target row id");
    JZ_VEC_FOR(const jazz_model_RelationProjectColumn, c, step->columns) {
        if (c->expr.tag != JAZZ_KEYREF_COLUMN || !opt_str_is(&c->expr.column.scope, target_alias))
            return unify_err("gather step must project only its forward-hop target");
    }
    /* The next frontier is the hop target's row id. An `id` alias must still be that row's id. */
    JZ_VEC_FOR(const jazz_model_RelationProjectColumn, c, step->columns) {
        if (jz_str_eq_cstr(&c->alias, "id") && !(c->expr.tag == JAZZ_KEYREF_COLUMN && jz_str_eq_cstr(&c->expr.column.column, "id")))
            return unify_err("gather step must not project another target column as id");
    }

    jazz_model_RelationPredicateVec filters = {0};
    JZ_TRY(relation_gather_step_filters(input->input, &filters));
    const jz_str *member = NULL;
    JZ_VEC_FOR(const jazz_model_RelationPredicate, p, filters) {
        if (is_frontier_cmp(p, edge_scope)) { member = &p->left.column; break; }
    }
    jz_err e = JZ_OK;
    if (!member) { e = unify_err("gather step must compare one edge column to the frontier row id"); goto out; }
    JZ_VEC_FOR(const jazz_model_RelationPredicate, p, filters) {
        if (is_frontier_cmp(p, edge_scope)) continue;
        bool has = false;
        jz_str scope = {0};
        jazz_model_Predicate pred;
        e = relation_predicate_to_query_predicate(p, &has, &scope, &pred);
        if (JZ_IS_ERR(e)) break;
        if (!has) continue;
        bool ok = jz_str_eq(&scope, edge_scope);
        jz_str_drop(&scope);
        if (!ok) { Q(Predicate__drop)(&pred); e = unify_err("gather step filters must be scoped to the edge table"); break; }
        JZ_VEC_PUSH(*edge_filters, pred);
    }
    if (JZ_IS_ERR(e)) { VDROP(*edge_filters, Predicate); goto out; }
    *edge_table_out = jz_str_clone(edge_table);
    *edge_member_column = jz_str_clone(member);
    *edge_parent_column = jz_str_clone(&condition->left.column);
out:
    VDROP(filters, RelationPredicate);
    return e;
}

/* ---------------------------------------------------------------- facade joins */
static jz_err relation_join_columns(const RelationFacadeJoin *join, const jz_str *left_scope, const jz_str *right_scope,
                                    jz_str *left_column, jz_str *right_column) {
    if (jz_str_eq(&join->left_scope, left_scope) && jz_str_eq(&join->right_scope, right_scope)) {
        *left_column = jz_str_clone(&join->left_column); *right_column = jz_str_clone(&join->right_column);
        return JZ_OK;
    }
    if (jz_str_eq(&join->right_scope, left_scope) && jz_str_eq(&join->left_scope, right_scope)) {
        *left_column = jz_str_clone(&join->right_column); *right_column = jz_str_clone(&join->left_column);
        return JZ_OK;
    }
    return unify_err("relation query join path contains a non-adjacent edge");
}

typedef struct PathStep { jz_str scope; RelationFacadeJoin join; } PathStep;
typedef JZ_VEC(PathStep) PathStepVec;
static void path_step_drop_v(void *p) { PathStep *s = p; jz_str_drop(&s->scope); facade_join_drop(&s->join); }

static jz_err build_relation_path_join(RelationFacadePlan *plan, const jz_str *left_scope, const PathStepVec *path, u32 index,
                                       jazz_model_JoinVia *out) {
    if (index >= path->len) return unify_err("relation query join path ended unexpectedly");
    const PathStep *s = &path->ptr[index];
    jz_str left_column, right_column;
    JZ_TRY(relation_join_columns(&s->join, left_scope, &s->scope, &left_column, &right_column));
    const jz_str *join_table = jz_btree_get(&plan->tables_by_scope, &s->scope);
    if (!join_table) { jz_str_drop(&left_column); jz_str_drop(&right_column); return unify_err("relation query join scope is unknown"); }
    jazz_model_JoinVia join;
    memset(&join, 0, sizeof(join));
    join.table = jz_str_clone(join_table);
    join.on_column = right_column;
    join.target = JAZZ_JOIN_TARGET_COLUMN;
    if (!jz_str_eq_cstr(&left_column, "id")) { join.source_column.some = true; join.source_column.s = left_column; }
    else jz_str_drop(&left_column);
    jz_str removed_key;
    if (jz_btree_remove(&plan->filters_by_scope, &s->scope, &removed_key, &join.filters)) jz_str_drop(&removed_key);
    if (index + 1 < path->len) {
        jazz_model_JoinVia nested;
        jz_err e = build_relation_path_join(plan, &s->scope, path, index + 1, &nested);
        if (JZ_IS_ERR(e)) { Q(JoinVia__drop)(&join); return e; }
        JZ_VEC_PUSH(join.nested_joins, nested);
    }
    *out = join;
    return JZ_OK;
}

static jz_err relation_path_join_from_output(RelationFacadePlan *plan, const jz_str *output_scope, jazz_model_JoinVia *out) {
    PathStepVec path = {0};
    jz_str current = jz_str_clone(output_scope);
    jazz_model_OptStr previous = {0};
    jz_err e = JZ_OK;
    for (;;) {
        const RelationFacadeJoin *found = NULL;
        u32 incident = 0;
        JZ_VEC_FOR(const RelationFacadeJoin, j, plan->joins) {
            if (!jz_str_eq(&j->left_scope, &current) && !jz_str_eq(&j->right_scope, &current)) continue;
            if (previous.some && (jz_str_eq(&j->left_scope, &previous.s) || jz_str_eq(&j->right_scope, &previous.s))) continue;
            if (incident++ == 0) found = j;
        }
        if (incident == 0) break;
        if (incident > 1) { e = unify_err("relation query joins must form one output-rooted path in this slice"); goto done; }
        jz_str next = jz_str_eq(&found->left_scope, &current) ? jz_str_clone(&found->right_scope) : jz_str_clone(&found->left_scope);
        PathStep *s = JAZZ_VEC_SLOT(path);
        s->scope = jz_str_clone(&next);
        s->join = facade_join_clone(found);
        jazz_model__OptStr__drop(&previous);
        previous.some = true; previous.s = current;
        current = next;
    }
    if (path.len != plan->joins.len) { e = unify_err("relation query joins must connect directly to the output scope in this slice"); goto done; }
    if (path.len == 0) { e = unify_err("relation query join path is empty"); goto done; }
    e = build_relation_path_join(plan, output_scope, &path, 0, out);
done:
    jz_str_drop(&current);
    jazz_model__OptStr__drop(&previous);
    jz_vec_drop(&path, sizeof(PathStep), path_step_drop_v);
    return e;
}

/* ---------------------------------------------------------------- facade collection */
static jz_err collect_relation_facade(const jazz_model_RelationExpr *expr, RelationFacadePlan *plan) {
    switch (expr->tag) {
    case JAZZ_REXPR_TABLE_SCAN: {
        jz_str scope = expr->alias.some ? jz_str_clone(&expr->alias.s) : jz_str_clone(&expr->table);
        jz_str table = jz_str_clone(&expr->table);
        jz_str existing;
        if (jz_btree_insert(&plan->tables_by_scope, &scope, &table, &existing)) {
            bool same = jz_str_eq(&existing, &expr->table);
            jz_str_drop(&existing);
            if (!same) return unify_err("relation query reuses an alias for different tables");
        }
        return JZ_OK;
    }
    case JAZZ_REXPR_FILTER: {
        JZ_TRY(collect_relation_facade(expr->input, plan));
        bool has = false;
        jz_str scope = {0};
        jazz_model_Predicate p;
        JZ_TRY(relation_predicate_to_query_predicate(&expr->predicate, &has, &scope, &p));
        if (has) {
            bool inserted;
            jazz_model_PredicateVec *v = jz_btree_entry(&plan->filters_by_scope, &scope, &inserted);
            if (!inserted) jz_str_drop(&scope);
            JZ_VEC_PUSH(*v, p);
        }
        return JZ_OK;
    }
    case JAZZ_REXPR_JOIN:
        if (expr->join_kind != JAZZ_JOIN_INNER) return unify_err("left relation joins are not unified yet");
        JZ_TRY(collect_relation_facade(expr->input, plan));
        JZ_TRY(collect_relation_facade(expr->right, plan));
        JZ_VEC_FOR(const jazz_model_RelationJoinCondition, c, expr->on) {
            jz_str ls, rs;
            JZ_TRY(Q(relation_scope)(&c->left, &ls));
            jz_err e = Q(relation_scope)(&c->right, &rs);
            if (JZ_IS_ERR(e)) { jz_str_drop(&ls); return e; }
            RelationFacadeJoin j = {ls, jz_str_clone(&c->left.column), rs, jz_str_clone(&c->right.column)};
            JZ_VEC_PUSH(plan->joins, j);
        }
        return JZ_OK;
    case JAZZ_REXPR_PROJECT: {
        JZ_TRY(collect_relation_facade(expr->input, plan));
        jazz_model_OptStr output_scope = {0};
        JZ_TRY(Q(validate_relation_projection_aliases)(&expr->columns));
        JZ_VEC_FOR(const jazz_model_RelationProjectColumn, c, expr->columns) {
            jz_str scope;
            if (c->expr.tag == JAZZ_KEYREF_COLUMN) {
                jz_err e = Q(relation_scope)(&c->expr.column, &scope);
                if (JZ_IS_ERR(e)) { jazz_model__OptStr__drop(&output_scope); return e; }
            } else if (c->expr.row_id == JAZZ_ROWID_CURRENT) {
                continue;
            } else {
                jazz_model__OptStr__drop(&output_scope);
                return unify_err("outer/frontier row-id relation projections are not unified yet");
            }
            if (output_scope.some) {
                bool same = jz_str_eq(&output_scope.s, &scope);
                jz_str_drop(&scope);
                if (!same) { jazz_model__OptStr__drop(&output_scope); return unify_err("relation query project must select one output scope"); }
            } else {
                output_scope.some = true; output_scope.s = scope;
            }
        }
        jazz_model__OptStr__drop(&plan->output_scope);
        plan->output_scope = output_scope;
        VDROP(plan->output_projection, RelationProjectColumn);
        VCLONE(plan->output_projection, expr->columns, RelationProjectColumn);
        plan->has_output_projection = true;
        return JZ_OK;
    }
    case JAZZ_REXPR_ORDER_BY: {
        JZ_TRY(collect_relation_facade(expr->input, plan));
        if (!plan->output_scope.some) return unify_err("relation order_by requires projected output scope");
        JZ_VEC_FOR(const jazz_model_RelationOrderBy, t, expr->terms) {
            jz_str scope;
            JZ_TRY(Q(relation_scope)(&t->column, &scope));
            bool same = jz_str_eq(&scope, &plan->output_scope.s);
            jz_str_drop(&scope);
            if (!same) return unify_err("relation order_by on non-output scope is not unified yet");
            jazz_model_OrderBy *o = JAZZ_VEC_SLOT(plan->order_by);
            o->column = jz_str_clone(&t->column.column);
            o->direction = t->direction;
        }
        return JZ_OK;
    }
    case JAZZ_REXPR_OFFSET:
    case JAZZ_REXPR_LIMIT:
        JZ_TRY(collect_relation_facade(expr->input, plan));
        fold_relation_output_step(&plan->offset, &plan->has_limit, &plan->limit,
                                  expr->tag == JAZZ_REXPR_OFFSET ? STEP_OFFSET : STEP_LIMIT, expr->n);
        return JZ_OK;
    default:
        return unify_err("union/gather/distinct relation query lowering is not unified yet");
    }
}

jz_err Q(relation_scope)(const jazz_model_RelationColumnRef *column, jz_str *out) {
    if (!column->scope.some) return unify_err("relation column refs must be scoped for unified lowering");
    *out = jz_str_clone(&column->scope.s);
    return JZ_OK;
}

/* ---------------------------------------------------------------- predicates */
/* `column` closure of relation_leaf_predicate */
typedef jz_err (*ColumnOperandFn)(const jazz_model_RelationColumnRef *column, void *ctx, jazz_model_Operand *out);

static jz_err relation_leaf_predicate(const jazz_model_RelationPredicate *p, ColumnOperandFn column, void *ctx, bool *some,
                                      jazz_model_Predicate *out) {
    static const u8 CMP_TAGS[] = {JAZZ_PRED_EQ, JAZZ_PRED_NE, JAZZ_PRED_LT, JAZZ_PRED_LTE, JAZZ_PRED_GT, JAZZ_PRED_GTE};
    jazz_model_Operand left, right;
    *some = true;
    switch (p->tag) {
    case JAZZ_RPE_CMP:
    case JAZZ_RPE_CONTAINS: {
        JZ_TRY(column(&p->left, ctx, &left));
        jz_err e = relation_value_to_operand(&p->right, &right);
        if (JZ_IS_ERR(e)) { Q(Operand__drop)(&left); return e; }
        *out = Q(binary)(p->tag == JAZZ_RPE_CMP ? CMP_TAGS[p->op] : JAZZ_PRED_CONTAINS, left, right);
        return JZ_OK;
    }
    case JAZZ_RPE_IS_NULL:
        JZ_TRY(column(&p->left, ctx, &left));
        *out = Q(is_null)(left);
        return JZ_OK;
    case JAZZ_RPE_IS_NOT_NULL:
        JZ_TRY(column(&p->left, ctx, &left));
        *out = Q(not)(Q(is_null)(left));
        return JZ_OK;
    case JAZZ_RPE_IN: {
        JZ_TRY(column(&p->left, ctx, &left));
        jazz_model_OperandVec values = {0};
        JZ_VEC_FOR(const jazz_model_RelationValueRef, v, p->values) {
            jazz_model_Operand op;
            jz_err e = relation_value_to_operand(v, &op);
            if (JZ_IS_ERR(e)) { Q(Operand__drop)(&left); jz_vec_drop(&values, sizeof(jazz_model_Operand), Q(Operand__drop_v)); return e; }
            JZ_VEC_PUSH(values, op);
        }
        *out = Q(in_list)(left, values);
        return JZ_OK;
    }
    default: *some = false; return JZ_OK;
    }
}

static jz_err scoped_column_operand(const jazz_model_RelationColumnRef *column, void *ctx, jazz_model_Operand *out) {
    jz_str *scope = ctx;
    jz_str_drop(scope);
    JZ_TRY(Q(relation_scope)(column, scope));
    *out = Q(col)(jz_str_clone(&column->column));
    return JZ_OK;
}
static jz_err payload_column_operand(const jazz_model_RelationColumnRef *column, void *ctx, jazz_model_Operand *out) {
    (void)ctx;
    if (column->scope.some) return unify_err("payload enum predicate fields must be unscoped");
    *out = Q(col)(jz_str_clone(&column->column));
    return JZ_OK;
}

static jazz_model_Predicate always_false(void) { return Q(any_of)((jazz_model_PredicateVec){0}); }
static bool is_always_false(const jazz_model_Predicate *p) { return p->tag == JAZZ_PRED_ANY && p->children.len == 0; }

static jz_err relation_payload_predicate_to_predicate(const jazz_model_RelationPredicate *p, jazz_model_Predicate *out);
static jz_err relation_predicate_list(const jazz_model_RelationPredicateVec *ps, bool is_and, bool *some, jz_str *scope, jazz_model_Predicate *out);

static jz_err relation_predicate_to_query_predicate(const jazz_model_RelationPredicate *p, bool *some, jz_str *scope, jazz_model_Predicate *out) {
    jz_str leaf_scope = {0};
    bool leaf = false;
    jz_err e = relation_leaf_predicate(p, scoped_column_operand, &leaf_scope, &leaf, out);
    if (JZ_IS_ERR(e)) { jz_str_drop(&leaf_scope); return e; }
    if (leaf) { *some = true; *scope = leaf_scope; return JZ_OK; }
    switch (p->tag) {
    case JAZZ_RPE_ENUM_MATCH: {
        jz_str s;
        JZ_TRY(Q(relation_scope)(&p->left, &s));
        jazz_model_Predicate payload;
        e = relation_payload_predicate_to_predicate(p->inner, &payload);
        if (JZ_IS_ERR(e)) { jz_str_drop(&s); return e; }
        memset(out, 0, sizeof(*out));
        out->tag = JAZZ_PRED_ENUM_MATCH;
        out->column = jz_str_clone(&p->left.column);
        out->case_ = jz_str_clone(&p->case_);
        out->inner = Q(Predicate__box)(payload);
        *some = true; *scope = s;
        return JZ_OK;
    }
    case JAZZ_RPE_AND: return relation_predicate_list(&p->items, true, some, scope, out);
    case JAZZ_RPE_OR: return relation_predicate_list(&p->items, false, some, scope, out);
    case JAZZ_RPE_NOT: {
        bool has = false;
        jz_str s = {0};
        jazz_model_Predicate inner;
        JZ_TRY(relation_predicate_to_query_predicate(p->inner, &has, &s, &inner));
        if (!has) { *some = true; *scope = (jz_str){0}; *out = always_false(); return JZ_OK; }
        if (is_always_false(&inner)) { jz_str_drop(&s); Q(Predicate__drop)(&inner); *some = false; return JZ_OK; }
        *some = true; *scope = s; *out = Q(not)(inner);
        return JZ_OK;
    }
    case JAZZ_RPE_TRUE: *some = false; return JZ_OK;
    case JAZZ_RPE_FALSE: *some = true; *scope = (jz_str){0}; *out = always_false(); return JZ_OK;
    default: JZ_UNREACHABLE(); /* leaf predicates are converted above */
    }
}

static jz_err payload_children(const jazz_model_RelationPredicateVec *items, jazz_model_PredicateVec *out) {
    memset(out, 0, sizeof(*out));
    JZ_VEC_FOR(const jazz_model_RelationPredicate, c, *items) {
        jazz_model_Predicate p;
        jz_err e = relation_payload_predicate_to_predicate(c, &p);
        if (JZ_IS_ERR(e)) { VDROP(*out, Predicate); return e; }
        JZ_VEC_PUSH(*out, p);
    }
    return JZ_OK;
}

static jz_err relation_payload_predicate_to_predicate(const jazz_model_RelationPredicate *p, jazz_model_Predicate *out) {
    bool leaf = false;
    JZ_TRY(relation_leaf_predicate(p, payload_column_operand, NULL, &leaf, out));
    if (leaf) return JZ_OK;
    jazz_model_PredicateVec children;
    switch (p->tag) {
    case JAZZ_RPE_AND: JZ_TRY(payload_children(&p->items, &children)); *out = Q(all_of)(children); return JZ_OK;
    case JAZZ_RPE_OR: JZ_TRY(payload_children(&p->items, &children)); *out = Q(any_of)(children); return JZ_OK;
    case JAZZ_RPE_NOT: {
        jazz_model_Predicate inner;
        JZ_TRY(relation_payload_predicate_to_predicate(p->inner, &inner));
        *out = Q(not)(inner);
        return JZ_OK;
    }
    case JAZZ_RPE_TRUE: *out = Q(all_of)((jazz_model_PredicateVec){0}); return JZ_OK;
    case JAZZ_RPE_FALSE: *out = always_false(); return JZ_OK;
    case JAZZ_RPE_ENUM_MATCH: return unify_err("nested payload enum matches are not supported");
    default: JZ_UNREACHABLE();
    }
}

static jz_err relation_predicate_list(const jazz_model_RelationPredicateVec *ps, bool is_and, bool *some, jz_str *scope_out, jazz_model_Predicate *out) {
    jazz_model_OptStr scope = {0};
    jazz_model_PredicateVec items = {0};
    jz_err e = JZ_OK;
    JZ_VEC_FOR(const jazz_model_RelationPredicate, c, *ps) {
        bool has = false;
        jz_str ps_scope = {0};
        jazz_model_Predicate p;
        e = relation_predicate_to_query_predicate(c, &has, &ps_scope, &p);
        if (JZ_IS_ERR(e)) goto fail;
        if (!has) {
            if (is_and) continue;
            *some = false;
            goto cleanup;
        }
        if (is_always_false(&p)) {
            jz_str_drop(&ps_scope);
            Q(Predicate__drop)(&p);
            if (is_and) { *some = true; *scope_out = (jz_str){0}; *out = always_false(); goto cleanup; }
            continue;
        }
        if (ps_scope.len == 0) { jz_str_drop(&ps_scope); JZ_VEC_PUSH(items, p); continue; }
        if (scope.some) {
            bool same = jz_str_eq(&scope.s, &ps_scope);
            jz_str_drop(&ps_scope);
            if (!same) {
                Q(Predicate__drop)(&p);
                e = unify_err("relation predicate composition across scopes is not unified yet");
                goto fail;
            }
        } else {
            scope.some = true; scope.s = ps_scope;
        }
        JZ_VEC_PUSH(items, p);
    }
    if (!scope.some) {
        if (is_and) *some = false;
        else { *some = true; *scope_out = (jz_str){0}; *out = always_false(); }
        goto cleanup;
    }
    *some = true;
    *scope_out = scope.s;
    *out = is_and ? Q(all_of)(items) : Q(any_of)(items);
    return JZ_OK;
cleanup:
    jazz_model__OptStr__drop(&scope);
    VDROP(items, Predicate);
    return JZ_OK;
fail:
    jazz_model__OptStr__drop(&scope);
    VDROP(items, Predicate);
    return e;
}

/* ---------------------------------------------------------------- values */
static jz_err relation_value_to_operand(const jazz_model_RelationValueRef *v, jazz_model_Operand *out) {
    switch (v->tag) {
    case JAZZ_RVAL_LITERAL: {
        groove_Value value;
        JZ_TRY(Q(json_value_to_record_value)(v->literal, &value));
        *out = Q(lit)(value);
        return JZ_OK;
    }
    case JAZZ_RVAL_PARAM: *out = Q(param)(jz_str_clone(&v->param)); return JZ_OK;
    case JAZZ_RVAL_SESSION_REF: {
        jz_str name;
        if (Q(author_claim_path_key)(v->session_ref.ptr, v->session_ref.len, &name)) { *out = Q(claim)(name); return JZ_OK; }
        if (v->session_ref.len == 1) { *out = Q(claim)(jz_str_clone(&v->session_ref.ptr[0])); return JZ_OK; }
        if (v->session_ref.len == 2 && jz_str_eq_cstr(&v->session_ref.ptr[0], "claims")) {
            *out = Q(claim)(Q(provider_claim_operand_key)(jz_str_slice(&v->session_ref.ptr[1])));
            return JZ_OK;
        }
        return unify_err("session refs support session.user and session.claims[\"name\"]");
    }
    default: return unify_err("outer/frontier/row-id relation predicate operands are not unified yet");
    }
}

static groove_Value gv_tag(u8 tag) { groove_Value v; memset(&v, 0, sizeof(v)); v.tag = tag; return v; }

static jz_err runtime_value_object_to_record_value(const jz_json *map, groove_Value *out);

static jz_err json_array_to_values(const jz_json *arr, groove_Value *out) {
    *out = gv_tag(GROOVE_V_ARRAY);
    for (u32 i = 0; i < arr->v.arr.len; i++) {
        groove_Value item;
        jz_err e = Q(json_value_to_record_value)(arr->v.arr.ptr[i], &item);
        if (JZ_IS_ERR(e)) { groove__records__values__Value__drop(out); return e; }
        JZ_VEC_PUSH(out->u.items, item);
    }
    return JZ_OK;
}

jz_err Q(json_value_to_record_value)(const jz_json *value, groove_Value *out) {
    switch (value->kind) {
    case JZ_JSON_NULL: *out = gv_tag(GROOVE_V_NULLABLE); return JZ_OK; /* Nullable(None) */
    case JZ_JSON_BOOL: *out = gv_tag(GROOVE_V_BOOL); out->u.b = value->v.b; return JZ_OK;
    case JZ_JSON_U64: *out = gv_tag(GROOVE_V_U64); out->u.u64_ = value->v.u; return JZ_OK;
    case JZ_JSON_I64:
        if (value->v.i >= 0) { *out = gv_tag(GROOVE_V_U64); out->u.u64_ = (u64)value->v.i; return JZ_OK; }
        *out = gv_tag(GROOVE_V_I64); out->u.i64_ = value->v.i; return JZ_OK;
    case JZ_JSON_F64:
        if (value->v.f != value->v.f || value->v.f - value->v.f != 0) return unify_err("relation literal is not finite");
        *out = gv_tag(GROOVE_V_F64); out->u.f64_ = value->v.f; return JZ_OK;
    case JZ_JSON_STR: *out = gv_tag(GROOVE_V_STRING); out->u.s = jz_str_clone(&value->v.s); return JZ_OK;
    case JZ_JSON_ARR: return json_array_to_values(value, out);
    case JZ_JSON_OBJ: return runtime_value_object_to_record_value(value, out);
    }
    JZ_UNREACHABLE();
}

static jz_err runtime_bool_value(const jz_json *v, bool *out) {
    if (v && v->kind == JZ_JSON_BOOL) { *out = v->v.b; return JZ_OK; }
    return unify_err("Boolean relation literal requires a boolean value");
}
static jz_err runtime_string_value(const jz_json *v, const jz_str **out) {
    if (v && v->kind == JZ_JSON_STR) { *out = &v->v.s; return JZ_OK; }
    return unify_err("string relation literal requires a string value");
}
static bool json_is_number(const jz_json *v) { return v && (v->kind == JZ_JSON_U64 || v->kind == JZ_JSON_I64 || v->kind == JZ_JSON_F64); }
static jz_err runtime_bytea_value(const jz_json *v, jz_bytes *out) {
    if (!v || v->kind != JZ_JSON_ARR) return unify_err("Bytea relation literal requires an array value");
    memset(out, 0, sizeof(*out));
    for (u32 i = 0; i < v->v.arr.len; i++) {
        u64 b;
        if (!jz_json_as_u64(v->v.arr.ptr[i], &b) || b > 0xff) {
            jz_vec_drop(out, 1, NULL);
            return unify_err("Bytea relation literal values must be bytes");
        }
        u8 byte = (u8)b;
        JZ_VEC_PUSH(*out, byte);
    }
    return JZ_OK;
}
static jz_err runtime_u64_value(const jz_json *v, u64 *out) {
    if (!json_is_number(v)) return unify_err("integer relation literal requires a numeric value");
    if (!jz_json_as_u64(v, out)) return unify_err("integer relation literal must be non-negative");
    return JZ_OK;
}
static jz_err runtime_i32_value(const jz_json *v, i32 *out) {
    if (!json_is_number(v)) return unify_err("Integer relation literal requires an integer value");
    i64 x;
    if (!jz_json_as_i64(v, &x) || x < INT32_MIN || x > INT32_MAX)
        return unify_err("Integer relation literal requires a signed 32-bit integer");
    *out = (i32)x;
    return JZ_OK;
}
static jz_err parse_i64(const jz_str *s, i64 *out) {
    /* str::parse::<i64> */
    u32 i = 0;
    bool neg = false;
    if (s->len && (s->ptr[0] == '+' || s->ptr[0] == '-')) { neg = s->ptr[0] == '-'; i = 1; }
    if (i >= s->len) return (jz_err){1, NULL};
    u64 acc = 0;
    for (; i < s->len; i++) {
        char c = s->ptr[i];
        if (c < '0' || c > '9') return (jz_err){1, NULL};
        if (acc > (UINT64_MAX - (u64)(c - '0')) / 10) return (jz_err){1, NULL};
        acc = acc * 10 + (u64)(c - '0');
    }
    if (neg ? acc > (u64)INT64_MAX + 1 : acc > (u64)INT64_MAX) return (jz_err){1, NULL};
    *out = neg ? (i64)(0 - acc) : (i64)acc;
    return JZ_OK;
}
static jz_err runtime_i64_value(const jz_json *v, i64 *out) {
    if (json_is_number(v)) {
        if (!jz_json_as_i64(v, out)) return unify_err("BigInt relation literal requires a signed 64-bit integer");
        return JZ_OK;
    }
    if (v && v->kind == JZ_JSON_STR) {
        if (JZ_IS_ERR(parse_i64(&v->v.s, out))) return unify_err("BigInt relation literal requires a signed 64-bit integer");
        return JZ_OK;
    }
    return unify_err("BigInt relation literal requires an integer value");
}
static jz_err runtime_f64_value(const jz_json *v, f64 *out) {
    if (!json_is_number(v)) return unify_err("double relation literal requires a numeric value");
    if (!jz_json_as_f64(v, out)) return unify_err("double relation literal must be finite");
    return JZ_OK;
}

static jz_err runtime_value_object_to_record_value(const jz_json *map, groove_Value *out) {
    const jz_json *type = jz_json_obj_get(map, "type");
    if (!type || type->kind != JZ_JSON_STR) return unify_err("object relation literals must use the runtime value envelope");
    const jz_str *value_type = &type->v.s;
    const jz_json *value = jz_json_obj_get(map, "value");
    const jz_str *s;
    if (jz_str_eq_cstr(value_type, "Null")) { *out = gv_tag(GROOVE_V_NULLABLE); return JZ_OK; }
    if (jz_str_eq_cstr(value_type, "Boolean")) { *out = gv_tag(GROOVE_V_BOOL); return runtime_bool_value(value, &out->u.b); }
    if (jz_str_eq_cstr(value_type, "Text")) {
        JZ_TRY(runtime_string_value(value, &s));
        *out = gv_tag(GROOVE_V_STRING); out->u.s = jz_str_clone(s);
        return JZ_OK;
    }
    if (jz_str_eq_cstr(value_type, "Uuid")) {
        JZ_TRY(runtime_string_value(value, &s));
        *out = gv_tag(GROOVE_V_UUID);
        jz_err e = jz_uuid_parse(jz_str_slice(s), &out->u.uuid);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); return unify_err("invalid Uuid relation literal"); }
        return JZ_OK;
    }
    if (jz_str_eq_cstr(value_type, "Bytea")) { *out = gv_tag(GROOVE_V_BYTES); return runtime_bytea_value(value, &out->u.bytes); }
    if (jz_str_eq_cstr(value_type, "Integer")) { *out = gv_tag(GROOVE_V_I32); return runtime_i32_value(value, &out->u.i32_); }
    if (jz_str_eq_cstr(value_type, "Timestamp")) { *out = gv_tag(GROOVE_V_U64); return runtime_u64_value(value, &out->u.u64_); }
    if (jz_str_eq_cstr(value_type, "BigInt")) { *out = gv_tag(GROOVE_V_I64); return runtime_i64_value(value, &out->u.i64_); }
    if (jz_str_eq_cstr(value_type, "Double")) { *out = gv_tag(GROOVE_V_F64); return runtime_f64_value(value, &out->u.f64_); }
    if (jz_str_eq_cstr(value_type, "Array")) {
        if (!value || value->kind != JZ_JSON_ARR) return unify_err("Array relation literal requires an array value");
        return json_array_to_values(value, out);
    }
    return Q(QueryError__new)(JAZZ_QE_UNSUPPORTED_RELATION_QUERY, "unsupported relation query: unsupported runtime relation literal type %.*s",
                              (int)value_type->len, value_type->ptr);
}
