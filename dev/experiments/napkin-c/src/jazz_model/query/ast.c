/* query/ast.rs: Query AST, relation facade vocabulary, derived Clone/PartialEq/
 * drop glue, and the Query serde impls (Postcard + human-readable JSON).
 * query.rs include!s this file, so items live in jazz_model::query. */
#include <string.h>
#include "jazz_model/jazz_model.h"

#define Q(x) jazz_model__query__##x
#define V_WRAP(N, T) \
    void Q(N##__drop_v)(void *v) { Q(N##__drop)((T *)v); } \
    void Q(N##__Clone__clone_v)(void *d, const void *s) { Q(N##__Clone__clone)((T *)d, (const T *)s); }
#define VDROP(v, N) jz_vec_drop(&(v), sizeof(*(v).ptr), Q(N##__drop_v))
#define VCLONE(d, s, N) jz_vec_clone(&(d), &(s), sizeof(*(s).ptr), Q(N##__Clone__clone_v))
#define VEQ(a, b, N) JAZZ_VEC_EQ(&(a), &(b), Q(N##__PartialEq__eq))
#define OSTR_CLONE(s) jazz_model__OptStr__clone(&(s))
#define OSTR_EQ(a, b) jazz_model__OptStr__eq(&(a), &(b))
#define STR_EQ(a, b) jz_str_eq(&(a), &(b))

/* QUERY_NAMESPACE = uuid!("5d39e9ed-88f3-5b58-b8db-8786b02f5d2f") */
const jz_uuid jazz_model__query__QUERY_NAMESPACE = {{0x5d, 0x39, 0xe9, 0xed, 0x88, 0xf3, 0x5b, 0x58,
                                                     0xb8, 0xdb, 0x87, 0x86, 0xb0, 0x2f, 0x5d, 0x2f}};

/* ================================================================ RelationColumnRef */
void Q(RelationColumnRef__drop)(jazz_model_RelationColumnRef *v) {
    jazz_model__OptStr__drop(&v->scope);
    jz_str_drop(&v->column);
}
void Q(RelationColumnRef__Clone__clone)(jazz_model_RelationColumnRef *d, const jazz_model_RelationColumnRef *s) {
    d->scope = OSTR_CLONE(s->scope);
    d->column = jz_str_clone(&s->column);
}
bool Q(RelationColumnRef__PartialEq__eq)(const jazz_model_RelationColumnRef *a, const jazz_model_RelationColumnRef *b) {
    return OSTR_EQ(a->scope, b->scope) && STR_EQ(a->column, b->column);
}
/* derive(Ord): Option<String> (None < Some), then column */
int Q(RelationColumnRef__Ord__cmp)(const jazz_model_RelationColumnRef *a, const jazz_model_RelationColumnRef *b) {
    if (a->scope.some != b->scope.some) return a->scope.some ? 1 : -1;
    if (a->scope.some) {
        int c = jz_str_cmp(&a->scope.s, &b->scope.s);
        if (c) return c;
    }
    return jz_str_cmp(&a->column, &b->column);
}
V_WRAP(RelationColumnRef, jazz_model_RelationColumnRef)

/* ================================================================ RelationValueRef */
void Q(RelationValueRef__drop)(jazz_model_RelationValueRef *v) {
    switch (v->tag) {
    case JAZZ_RVAL_LITERAL: if (v->literal) jz_json_free(v->literal); v->literal = NULL; break;
    case JAZZ_RVAL_PARAM: jz_str_drop(&v->param); break;
    case JAZZ_RVAL_SESSION_REF: jazz_model__StrVec__drop(&v->session_ref); break;
    case JAZZ_RVAL_OUTER_COLUMN:
    case JAZZ_RVAL_FRONTIER_COLUMN: Q(RelationColumnRef__drop)(&v->column); break;
    default: break;
    }
}
void Q(RelationValueRef__Clone__clone)(jazz_model_RelationValueRef *d, const jazz_model_RelationValueRef *s) {
    memset(d, 0, sizeof(*d));
    d->tag = s->tag;
    switch (s->tag) {
    case JAZZ_RVAL_LITERAL: d->literal = jz_json_clone(s->literal); break;
    case JAZZ_RVAL_PARAM: d->param = jz_str_clone(&s->param); break;
    case JAZZ_RVAL_SESSION_REF: jazz_model__StrVec__clone(&d->session_ref, &s->session_ref); break;
    case JAZZ_RVAL_OUTER_COLUMN:
    case JAZZ_RVAL_FRONTIER_COLUMN: Q(RelationColumnRef__Clone__clone)(&d->column, &s->column); break;
    default: d->row_id = s->row_id; break;
    }
}
bool Q(RelationValueRef__PartialEq__eq)(const jazz_model_RelationValueRef *a, const jazz_model_RelationValueRef *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_RVAL_LITERAL: return jz_json_eq(a->literal, b->literal);
    case JAZZ_RVAL_PARAM: return STR_EQ(a->param, b->param);
    case JAZZ_RVAL_SESSION_REF: return jazz_model__StrVec__eq(&a->session_ref, &b->session_ref);
    case JAZZ_RVAL_OUTER_COLUMN:
    case JAZZ_RVAL_FRONTIER_COLUMN: return Q(RelationColumnRef__PartialEq__eq)(&a->column, &b->column);
    default: return a->row_id == b->row_id;
    }
}
V_WRAP(RelationValueRef, jazz_model_RelationValueRef)

/* ================================================================ RelationPredicate */
void Q(RelationPredicate__drop)(jazz_model_RelationPredicate *v) {
    switch (v->tag) {
    case JAZZ_RPE_CMP:
    case JAZZ_RPE_CONTAINS:
        Q(RelationColumnRef__drop)(&v->left);
        Q(RelationValueRef__drop)(&v->right);
        break;
    case JAZZ_RPE_IS_NULL:
    case JAZZ_RPE_IS_NOT_NULL: Q(RelationColumnRef__drop)(&v->left); break;
    case JAZZ_RPE_IN:
        Q(RelationColumnRef__drop)(&v->left);
        VDROP(v->values, RelationValueRef);
        break;
    case JAZZ_RPE_ENUM_MATCH:
        Q(RelationColumnRef__drop)(&v->left);
        jz_str_drop(&v->case_);
        /* fallthrough */
    case JAZZ_RPE_NOT:
        if (v->inner) { Q(RelationPredicate__drop)(v->inner); jz_free(v->inner); v->inner = NULL; }
        break;
    case JAZZ_RPE_AND:
    case JAZZ_RPE_OR: VDROP(v->items, RelationPredicate); break;
    default: break;
    }
}
static jazz_model_RelationPredicate *rel_pred_box_clone(const jazz_model_RelationPredicate *s) {
    jazz_model_RelationPredicate *b = JZ_NEW(jazz_model_RelationPredicate);
    Q(RelationPredicate__Clone__clone)(b, s);
    return b;
}
void Q(RelationPredicate__Clone__clone)(jazz_model_RelationPredicate *d, const jazz_model_RelationPredicate *s) {
    memset(d, 0, sizeof(*d));
    d->tag = s->tag;
    switch (s->tag) {
    case JAZZ_RPE_CMP:
    case JAZZ_RPE_CONTAINS:
        Q(RelationColumnRef__Clone__clone)(&d->left, &s->left);
        d->op = s->op;
        Q(RelationValueRef__Clone__clone)(&d->right, &s->right);
        break;
    case JAZZ_RPE_IS_NULL:
    case JAZZ_RPE_IS_NOT_NULL: Q(RelationColumnRef__Clone__clone)(&d->left, &s->left); break;
    case JAZZ_RPE_IN:
        Q(RelationColumnRef__Clone__clone)(&d->left, &s->left);
        VCLONE(d->values, s->values, RelationValueRef);
        break;
    case JAZZ_RPE_ENUM_MATCH:
        Q(RelationColumnRef__Clone__clone)(&d->left, &s->left);
        d->case_ = jz_str_clone(&s->case_);
        d->inner = rel_pred_box_clone(s->inner);
        break;
    case JAZZ_RPE_NOT: d->inner = rel_pred_box_clone(s->inner); break;
    case JAZZ_RPE_AND:
    case JAZZ_RPE_OR: VCLONE(d->items, s->items, RelationPredicate); break;
    default: break;
    }
}
bool Q(RelationPredicate__PartialEq__eq)(const jazz_model_RelationPredicate *a, const jazz_model_RelationPredicate *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_RPE_CMP:
        return Q(RelationColumnRef__PartialEq__eq)(&a->left, &b->left) && a->op == b->op &&
               Q(RelationValueRef__PartialEq__eq)(&a->right, &b->right);
    case JAZZ_RPE_CONTAINS:
        return Q(RelationColumnRef__PartialEq__eq)(&a->left, &b->left) &&
               Q(RelationValueRef__PartialEq__eq)(&a->right, &b->right);
    case JAZZ_RPE_IS_NULL:
    case JAZZ_RPE_IS_NOT_NULL: return Q(RelationColumnRef__PartialEq__eq)(&a->left, &b->left);
    case JAZZ_RPE_IN:
        return Q(RelationColumnRef__PartialEq__eq)(&a->left, &b->left) && VEQ(a->values, b->values, RelationValueRef);
    case JAZZ_RPE_ENUM_MATCH:
        return Q(RelationColumnRef__PartialEq__eq)(&a->left, &b->left) && STR_EQ(a->case_, b->case_) &&
               Q(RelationPredicate__PartialEq__eq)(a->inner, b->inner);
    case JAZZ_RPE_NOT: return Q(RelationPredicate__PartialEq__eq)(a->inner, b->inner);
    case JAZZ_RPE_AND:
    case JAZZ_RPE_OR: return VEQ(a->items, b->items, RelationPredicate);
    default: return true;
    }
}
V_WRAP(RelationPredicate, jazz_model_RelationPredicate)

/* ================================================================ small relation structs */
void Q(RelationKeyRef__drop)(jazz_model_RelationKeyRef *v) {
    if (v->tag == JAZZ_KEYREF_COLUMN) Q(RelationColumnRef__drop)(&v->column);
}
void Q(RelationKeyRef__Clone__clone)(jazz_model_RelationKeyRef *d, const jazz_model_RelationKeyRef *s) {
    memset(d, 0, sizeof(*d));
    d->tag = s->tag;
    if (s->tag == JAZZ_KEYREF_COLUMN) Q(RelationColumnRef__Clone__clone)(&d->column, &s->column);
    else d->row_id = s->row_id;
}
bool Q(RelationKeyRef__PartialEq__eq)(const jazz_model_RelationKeyRef *a, const jazz_model_RelationKeyRef *b) {
    if (a->tag != b->tag) return false;
    return a->tag == JAZZ_KEYREF_COLUMN ? Q(RelationColumnRef__PartialEq__eq)(&a->column, &b->column)
                                        : a->row_id == b->row_id;
}
V_WRAP(RelationKeyRef, jazz_model_RelationKeyRef)

void Q(RelationProjectColumn__drop)(jazz_model_RelationProjectColumn *v) {
    jz_str_drop(&v->alias);
    Q(RelationKeyRef__drop)(&v->expr);
}
void Q(RelationProjectColumn__Clone__clone)(jazz_model_RelationProjectColumn *d, const jazz_model_RelationProjectColumn *s) {
    d->alias = jz_str_clone(&s->alias);
    Q(RelationKeyRef__Clone__clone)(&d->expr, &s->expr);
}
bool Q(RelationProjectColumn__PartialEq__eq)(const jazz_model_RelationProjectColumn *a, const jazz_model_RelationProjectColumn *b) {
    return STR_EQ(a->alias, b->alias) && Q(RelationKeyRef__PartialEq__eq)(&a->expr, &b->expr);
}
V_WRAP(RelationProjectColumn, jazz_model_RelationProjectColumn)

void Q(RelationJoinCondition__drop)(jazz_model_RelationJoinCondition *v) {
    Q(RelationColumnRef__drop)(&v->left);
    Q(RelationColumnRef__drop)(&v->right);
}
void Q(RelationJoinCondition__Clone__clone)(jazz_model_RelationJoinCondition *d, const jazz_model_RelationJoinCondition *s) {
    Q(RelationColumnRef__Clone__clone)(&d->left, &s->left);
    Q(RelationColumnRef__Clone__clone)(&d->right, &s->right);
}
bool Q(RelationJoinCondition__PartialEq__eq)(const jazz_model_RelationJoinCondition *a, const jazz_model_RelationJoinCondition *b) {
    return Q(RelationColumnRef__PartialEq__eq)(&a->left, &b->left) && Q(RelationColumnRef__PartialEq__eq)(&a->right, &b->right);
}
V_WRAP(RelationJoinCondition, jazz_model_RelationJoinCondition)

void Q(RelationOrderBy__drop)(jazz_model_RelationOrderBy *v) { Q(RelationColumnRef__drop)(&v->column); }
void Q(RelationOrderBy__Clone__clone)(jazz_model_RelationOrderBy *d, const jazz_model_RelationOrderBy *s) {
    Q(RelationColumnRef__Clone__clone)(&d->column, &s->column);
    d->direction = s->direction;
}
bool Q(RelationOrderBy__PartialEq__eq)(const jazz_model_RelationOrderBy *a, const jazz_model_RelationOrderBy *b) {
    return Q(RelationColumnRef__PartialEq__eq)(&a->column, &b->column) && a->direction == b->direction;
}
V_WRAP(RelationOrderBy, jazz_model_RelationOrderBy)

/* ================================================================ RelationExpr */
jazz_model_RelationExpr *Q(RelationExpr__box)(jazz_model_RelationExpr e) {
    jazz_model_RelationExpr *b = JZ_NEW(jazz_model_RelationExpr);
    *b = e;
    return b;
}
static void rel_expr_box_drop(jazz_model_RelationExpr **e) {
    if (*e) { Q(RelationExpr__drop)(*e); jz_free(*e); *e = NULL; }
}
static jazz_model_RelationExpr *rel_expr_box_clone(const jazz_model_RelationExpr *s) {
    jazz_model_RelationExpr *b = JZ_NEW(jazz_model_RelationExpr);
    Q(RelationExpr__Clone__clone)(b, s);
    return b;
}
void Q(RelationExpr__drop)(jazz_model_RelationExpr *v) {
    switch (v->tag) {
    case JAZZ_REXPR_TABLE_SCAN:
        jz_str_drop(&v->table);
        jazz_model__OptStr__drop(&v->alias);
        break;
    case JAZZ_REXPR_FILTER:
        rel_expr_box_drop(&v->input);
        Q(RelationPredicate__drop)(&v->predicate);
        break;
    case JAZZ_REXPR_UNION: VDROP(v->inputs, RelationUnionArm); break;
    case JAZZ_REXPR_JOIN:
        rel_expr_box_drop(&v->input);
        rel_expr_box_drop(&v->right);
        VDROP(v->on, RelationJoinCondition);
        break;
    case JAZZ_REXPR_PROJECT:
        rel_expr_box_drop(&v->input);
        VDROP(v->columns, RelationProjectColumn);
        break;
    case JAZZ_REXPR_GATHER:
        rel_expr_box_drop(&v->input);
        rel_expr_box_drop(&v->right);
        Q(RelationKeyRef__drop)(&v->frontier_key);
        VDROP(v->keys, RelationKeyRef);
        break;
    case JAZZ_REXPR_DISTINCT:
        rel_expr_box_drop(&v->input);
        VDROP(v->keys, RelationKeyRef);
        break;
    case JAZZ_REXPR_ORDER_BY:
        rel_expr_box_drop(&v->input);
        VDROP(v->terms, RelationOrderBy);
        break;
    default: rel_expr_box_drop(&v->input); break;
    }
}
void Q(RelationExpr__Clone__clone)(jazz_model_RelationExpr *d, const jazz_model_RelationExpr *s) {
    memset(d, 0, sizeof(*d));
    d->tag = s->tag;
    switch (s->tag) {
    case JAZZ_REXPR_TABLE_SCAN:
        d->table = jz_str_clone(&s->table);
        d->alias = OSTR_CLONE(s->alias);
        break;
    case JAZZ_REXPR_FILTER:
        d->input = rel_expr_box_clone(s->input);
        Q(RelationPredicate__Clone__clone)(&d->predicate, &s->predicate);
        break;
    case JAZZ_REXPR_UNION: VCLONE(d->inputs, s->inputs, RelationUnionArm); break;
    case JAZZ_REXPR_JOIN:
        d->input = rel_expr_box_clone(s->input);
        d->right = rel_expr_box_clone(s->right);
        VCLONE(d->on, s->on, RelationJoinCondition);
        d->join_kind = s->join_kind;
        break;
    case JAZZ_REXPR_PROJECT:
        d->input = rel_expr_box_clone(s->input);
        VCLONE(d->columns, s->columns, RelationProjectColumn);
        break;
    case JAZZ_REXPR_GATHER:
        d->input = rel_expr_box_clone(s->input);
        d->right = rel_expr_box_clone(s->right);
        Q(RelationKeyRef__Clone__clone)(&d->frontier_key, &s->frontier_key);
        d->bound = s->bound;
        VCLONE(d->keys, s->keys, RelationKeyRef);
        break;
    case JAZZ_REXPR_DISTINCT:
        d->input = rel_expr_box_clone(s->input);
        VCLONE(d->keys, s->keys, RelationKeyRef);
        break;
    case JAZZ_REXPR_ORDER_BY:
        d->input = rel_expr_box_clone(s->input);
        VCLONE(d->terms, s->terms, RelationOrderBy);
        break;
    default:
        d->input = rel_expr_box_clone(s->input);
        d->n = s->n;
        break;
    }
}
static bool bound_eq(jazz_model_RecursionBound a, jazz_model_RecursionBound b) {
    return a.tag == b.tag && (a.tag == JAZZ_BOUND_FIXPOINT || a.max_depth == b.max_depth);
}
bool Q(RelationExpr__PartialEq__eq)(const jazz_model_RelationExpr *a, const jazz_model_RelationExpr *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_REXPR_TABLE_SCAN: return STR_EQ(a->table, b->table) && OSTR_EQ(a->alias, b->alias);
    case JAZZ_REXPR_FILTER:
        return Q(RelationExpr__PartialEq__eq)(a->input, b->input) &&
               Q(RelationPredicate__PartialEq__eq)(&a->predicate, &b->predicate);
    case JAZZ_REXPR_UNION: return VEQ(a->inputs, b->inputs, RelationUnionArm);
    case JAZZ_REXPR_JOIN:
        return Q(RelationExpr__PartialEq__eq)(a->input, b->input) && Q(RelationExpr__PartialEq__eq)(a->right, b->right) &&
               VEQ(a->on, b->on, RelationJoinCondition) && a->join_kind == b->join_kind;
    case JAZZ_REXPR_PROJECT:
        return Q(RelationExpr__PartialEq__eq)(a->input, b->input) && VEQ(a->columns, b->columns, RelationProjectColumn);
    case JAZZ_REXPR_GATHER:
        return Q(RelationExpr__PartialEq__eq)(a->input, b->input) && Q(RelationExpr__PartialEq__eq)(a->right, b->right) &&
               Q(RelationKeyRef__PartialEq__eq)(&a->frontier_key, &b->frontier_key) && bound_eq(a->bound, b->bound) &&
               VEQ(a->keys, b->keys, RelationKeyRef);
    case JAZZ_REXPR_DISTINCT:
        return Q(RelationExpr__PartialEq__eq)(a->input, b->input) && VEQ(a->keys, b->keys, RelationKeyRef);
    case JAZZ_REXPR_ORDER_BY:
        return Q(RelationExpr__PartialEq__eq)(a->input, b->input) && VEQ(a->terms, b->terms, RelationOrderBy);
    default: return Q(RelationExpr__PartialEq__eq)(a->input, b->input) && a->n == b->n;
    }
}
V_WRAP(RelationExpr, jazz_model_RelationExpr)

void Q(RelationUnionArm__drop)(jazz_model_RelationUnionArm *v) {
    jz_str_drop(&v->label);
    Q(RelationExpr__drop)(&v->input);
}
void Q(RelationUnionArm__Clone__clone)(jazz_model_RelationUnionArm *d, const jazz_model_RelationUnionArm *s) {
    d->label = jz_str_clone(&s->label);
    Q(RelationExpr__Clone__clone)(&d->input, &s->input);
}
bool Q(RelationUnionArm__PartialEq__eq)(const jazz_model_RelationUnionArm *a, const jazz_model_RelationUnionArm *b) {
    return STR_EQ(a->label, b->label) && Q(RelationExpr__PartialEq__eq)(&a->input, &b->input);
}
V_WRAP(RelationUnionArm, jazz_model_RelationUnionArm)

void Q(RelationQuery__drop)(jazz_model_RelationQuery *v) { Q(RelationExpr__drop)(&v->rel); }
void Q(RelationQuery__Clone__clone)(jazz_model_RelationQuery *d, const jazz_model_RelationQuery *s) {
    Q(RelationExpr__Clone__clone)(&d->rel, &s->rel);
}
bool Q(RelationQuery__PartialEq__eq)(const jazz_model_RelationQuery *a, const jazz_model_RelationQuery *b) {
    return Q(RelationExpr__PartialEq__eq)(&a->rel, &b->rel);
}
V_WRAP(RelationQuery, jazz_model_RelationQuery)

/* ================================================================ FlatJoin */
void Q(FlatJoinSource__drop)(jazz_model_FlatJoinSource *v) {
    jz_str_drop(&v->table);
    jazz_model__OptStr__drop(&v->alias);
    jz_str_drop(&v->on.left);
    jz_str_drop(&v->on.right);
}
void Q(FlatJoinSource__Clone__clone)(jazz_model_FlatJoinSource *d, const jazz_model_FlatJoinSource *s) {
    d->table = jz_str_clone(&s->table);
    d->alias = OSTR_CLONE(s->alias);
    d->on.left = jz_str_clone(&s->on.left);
    d->on.right = jz_str_clone(&s->on.right);
}
bool Q(FlatJoinSource__PartialEq__eq)(const jazz_model_FlatJoinSource *a, const jazz_model_FlatJoinSource *b) {
    return STR_EQ(a->table, b->table) && OSTR_EQ(a->alias, b->alias) && STR_EQ(a->on.left, b->on.left) &&
           STR_EQ(a->on.right, b->on.right);
}
V_WRAP(FlatJoinSource, jazz_model_FlatJoinSource)

void Q(FlatJoin__drop)(jazz_model_FlatJoin *v) {
    jazz_model__OptStr__drop(&v->root_alias);
    VDROP(v->sources, FlatJoinSource);
}
void Q(FlatJoin__Clone__clone)(jazz_model_FlatJoin *d, const jazz_model_FlatJoin *s) {
    d->root_alias = OSTR_CLONE(s->root_alias);
    VCLONE(d->sources, s->sources, FlatJoinSource);
}
bool Q(FlatJoin__PartialEq__eq)(const jazz_model_FlatJoin *a, const jazz_model_FlatJoin *b) {
    return OSTR_EQ(a->root_alias, b->root_alias) && VEQ(a->sources, b->sources, FlatJoinSource);
}
V_WRAP(FlatJoin, jazz_model_FlatJoin)

/* ================================================================ Query */
jazz_model_Query *Q(Query__box_new)(void) { return JZ_NEW(jazz_model_Query); }

void Q(Query__drop)(jazz_model_Query *q) {
    jz_str_drop(&q->table);
    VDROP(q->filters, Predicate);
    VDROP(q->joins, JoinVia);
    if (q->flat_join) { Q(FlatJoin__drop)(q->flat_join); jz_free(q->flat_join); q->flat_join = NULL; }
    VDROP(q->policy_branches, PolicyBranch);
    VDROP(q->reachable, ReachableVia);
    VDROP(q->inherits, InheritsVia);
    VDROP(q->includes, Include);
    VDROP(q->array_subqueries, ArraySubquery);
    if (q->has_select) jazz_model__StrVec__drop(&q->select);
    q->has_select = false;
    VDROP(q->order_by, OrderBy);
    if (q->aggregate) { Q(AggregateQuery__drop)(q->aggregate); jz_free(q->aggregate); q->aggregate = NULL; }
    if (q->relation) { Q(RelationQuery__drop)(q->relation); jz_free(q->relation); q->relation = NULL; }
}
void Q(Query__Clone__clone)(jazz_model_Query *d, const jazz_model_Query *s) {
    memset(d, 0, sizeof(*d));
    d->table = jz_str_clone(&s->table);
    VCLONE(d->filters, s->filters, Predicate);
    VCLONE(d->joins, s->joins, JoinVia);
    if (s->flat_join) { d->flat_join = JZ_NEW(jazz_model_FlatJoin); Q(FlatJoin__Clone__clone)(d->flat_join, s->flat_join); }
    VCLONE(d->policy_branches, s->policy_branches, PolicyBranch);
    VCLONE(d->reachable, s->reachable, ReachableVia);
    VCLONE(d->inherits, s->inherits, InheritsVia);
    VCLONE(d->includes, s->includes, Include);
    VCLONE(d->array_subqueries, s->array_subqueries, ArraySubquery);
    d->has_select = s->has_select;
    if (s->has_select) jazz_model__StrVec__clone(&d->select, &s->select);
    VCLONE(d->order_by, s->order_by, OrderBy);
    if (s->aggregate) { d->aggregate = JZ_NEW(jazz_model_AggregateQuery); Q(AggregateQuery__Clone__clone)(d->aggregate, s->aggregate); }
    d->has_limit = s->has_limit;
    d->limit = s->limit;
    d->offset = s->offset;
    if (s->relation) { d->relation = JZ_NEW(jazz_model_RelationQuery); Q(RelationQuery__Clone__clone)(d->relation, s->relation); }
}
bool Q(Query__PartialEq__eq)(const jazz_model_Query *a, const jazz_model_Query *b) {
    if (!STR_EQ(a->table, b->table) || !VEQ(a->filters, b->filters, Predicate) || !VEQ(a->joins, b->joins, JoinVia))
        return false;
    if (!a->flat_join != !b->flat_join || (a->flat_join && !Q(FlatJoin__PartialEq__eq)(a->flat_join, b->flat_join)))
        return false;
    if (!VEQ(a->policy_branches, b->policy_branches, PolicyBranch) || !VEQ(a->reachable, b->reachable, ReachableVia) ||
        !VEQ(a->inherits, b->inherits, InheritsVia) || !VEQ(a->includes, b->includes, Include) ||
        !VEQ(a->array_subqueries, b->array_subqueries, ArraySubquery))
        return false;
    if (a->has_select != b->has_select || (a->has_select && !jazz_model__StrVec__eq(&a->select, &b->select))) return false;
    if (!VEQ(a->order_by, b->order_by, OrderBy)) return false;
    if (!a->aggregate != !b->aggregate || (a->aggregate && !Q(AggregateQuery__PartialEq__eq)(a->aggregate, b->aggregate)))
        return false;
    if (a->has_limit != b->has_limit || (a->has_limit && a->limit != b->limit) || a->offset != b->offset) return false;
    if (!a->relation != !b->relation || (a->relation && !Q(RelationQuery__PartialEq__eq)(a->relation, b->relation)))
        return false;
    return true;
}
void Q(Query__drop_v)(void *v) { Q(Query__drop)((jazz_model_Query *)v); }
void Q(Query__Clone__clone_v)(void *d, const void *s) { Q(Query__Clone__clone)((jazz_model_Query *)d, (const jazz_model_Query *)s); }

/* ================================================================ FlatJoin serde */
void Q(FlatJoinOn__Serialize__serialize)(jz_pc_w *w, const jazz_model_FlatJoinOn *v) {
    jz_pc_str(w, &v->left);
    jz_pc_str(w, &v->right);
}
jz_err Q(FlatJoinOn__Deserialize__deserialize)(jz_pc_r *r, jazz_model_FlatJoinOn *out) {
    JZ_TRY(jz_pc_r_str(r, &out->left));
    return jz_pc_r_str(r, &out->right);
}
void Q(FlatJoinOn__Serialize__serialize_json)(jz_json_w *w, const jazz_model_FlatJoinOn *v) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "left"); jazz_model__json_w_str(w, &v->left);
    jz_jw_key(w, "right"); jazz_model__json_w_str(w, &v->right);
    jz_jw_end_obj(w);
}
void Q(FlatJoinSource__Serialize__serialize)(jz_pc_w *w, const jazz_model_FlatJoinSource *v) {
    jz_pc_str(w, &v->table);
    jazz_model__OptStr__serialize_pc(w, &v->alias);
    Q(FlatJoinOn__Serialize__serialize)(w, &v->on);
}
jz_err Q(FlatJoinSource__Deserialize__deserialize)(jz_pc_r *r, jazz_model_FlatJoinSource *out) {
    JZ_TRY(jz_pc_r_str(r, &out->table));
    JZ_TRY(jazz_model__OptStr__deserialize_pc(r, &out->alias));
    return Q(FlatJoinOn__Deserialize__deserialize)(r, &out->on);
}
void Q(FlatJoinSource__Serialize__serialize_json)(jz_json_w *w, const jazz_model_FlatJoinSource *v) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "table"); jazz_model__json_w_str(w, &v->table);
    jz_jw_key(w, "alias"); jazz_model__OptStr__serialize_json(w, &v->alias);
    jz_jw_key(w, "on"); Q(FlatJoinOn__Serialize__serialize_json)(w, &v->on);
    jz_jw_end_obj(w);
}
void Q(FlatJoin__Serialize__serialize)(jz_pc_w *w, const jazz_model_FlatJoin *v) {
    jazz_model__OptStr__serialize_pc(w, &v->root_alias);
    jz_pc_seq(w, v->sources.len);
    JZ_VEC_FOR(const jazz_model_FlatJoinSource, s, v->sources) Q(FlatJoinSource__Serialize__serialize)(w, s);
}
jz_err Q(FlatJoin__Deserialize__deserialize)(jz_pc_r *r, jazz_model_FlatJoin *out) {
    u32 n;
    JZ_TRY(jazz_model__OptStr__deserialize_pc(r, &out->root_alias));
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) JZ_TRY(Q(FlatJoinSource__Deserialize__deserialize)(r, JAZZ_VEC_SLOT(out->sources)));
    return JZ_OK;
}
void Q(FlatJoin__Serialize__serialize_json)(jz_json_w *w, const jazz_model_FlatJoin *v) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "root_alias"); jazz_model__OptStr__serialize_json(w, &v->root_alias);
    jz_jw_key(w, "sources");
    jz_jw_begin_arr(w);
    JZ_VEC_FOR(const jazz_model_FlatJoinSource, s, v->sources) Q(FlatJoinSource__Serialize__serialize_json)(w, s);
    jz_jw_end_arr(w);
    jz_jw_end_obj(w);
}

/* ================================================================ Query serde (Postcard) */
#define PC_VEC_W(w, vec, T, N) do { jz_pc_seq(w, (vec).len); JZ_VEC_FOR(const T, _it, vec) Q(N##__Serialize__serialize)(w, _it); } while (0)
#define PC_VEC_R(r, vec, N) do { u32 _n; JZ_TRY(jz_pc_r_seq(r, &_n)); \
    for (u32 _i = 0; _i < _n; _i++) JZ_TRY(Q(N##__Deserialize__deserialize)(r, JAZZ_VEC_SLOT(vec))); } while (0)

static void query_serialize_fields(jz_pc_w *w, const jazz_model_Query *q) {
    jz_pc_str(w, &q->table);
    PC_VEC_W(w, q->filters, jazz_model_Predicate, Predicate);
    PC_VEC_W(w, q->joins, jazz_model_JoinVia, JoinVia);
    jz_pc_option(w, q->flat_join != NULL);
    if (q->flat_join) Q(FlatJoin__Serialize__serialize)(w, q->flat_join);
    PC_VEC_W(w, q->policy_branches, jazz_model_PolicyBranch, PolicyBranch);
    PC_VEC_W(w, q->reachable, jazz_model_ReachableVia, ReachableVia);
    PC_VEC_W(w, q->inherits, jazz_model_InheritsVia, InheritsVia);
    PC_VEC_W(w, q->includes, jazz_model_Include, Include);
    PC_VEC_W(w, q->array_subqueries, jazz_model_ArraySubquery, ArraySubquery);
    jz_pc_option(w, q->has_select);
    if (q->has_select) jazz_model__StrVec__serialize_pc(w, &q->select);
    PC_VEC_W(w, q->order_by, jazz_model_OrderBy, OrderBy);
    jz_pc_option(w, q->aggregate != NULL);
    if (q->aggregate) Q(AggregateQuery__Serialize__serialize)(w, q->aggregate);
    jazz_model__OptU64__serialize_pc(w, q->has_limit, q->limit);
    jz_pc_varint(w, q->offset);
}

jz_err Q(Query__Serialize__serialize)(jz_pc_w *w, const jazz_model_Query *q) {
    query_serialize_fields(w, q);
    /* #[serde(with = "relation_query_wire")]: non-human serializer carries the typed wire tree */
    return Q(relation_query_wire__serialize)(w, q->relation);
}

static jz_err query_deserialize_fields(jz_pc_r *r, jazz_model_Query *q) {
    bool some;
    JZ_TRY(jz_pc_r_str(r, &q->table));
    PC_VEC_R(r, q->filters, Predicate);
    PC_VEC_R(r, q->joins, JoinVia);
    JZ_TRY(jz_pc_r_option(r, &some));
    if (some) {
        q->flat_join = JZ_NEW(jazz_model_FlatJoin);
        JZ_TRY(Q(FlatJoin__Deserialize__deserialize)(r, q->flat_join));
    }
    PC_VEC_R(r, q->policy_branches, PolicyBranch);
    PC_VEC_R(r, q->reachable, ReachableVia);
    PC_VEC_R(r, q->inherits, InheritsVia);
    PC_VEC_R(r, q->includes, Include);
    PC_VEC_R(r, q->array_subqueries, ArraySubquery);
    JZ_TRY(jz_pc_r_option(r, &q->has_select));
    if (q->has_select) JZ_TRY(jazz_model__StrVec__deserialize_pc(r, &q->select));
    PC_VEC_R(r, q->order_by, OrderBy);
    JZ_TRY(jz_pc_r_option(r, &some));
    if (some) {
        q->aggregate = JZ_NEW(jazz_model_AggregateQuery);
        JZ_TRY(Q(AggregateQuery__Deserialize__deserialize)(r, q->aggregate));
    }
    JZ_TRY(jazz_model__OptU64__deserialize_pc(r, &q->has_limit, &q->limit));
    JZ_TRY(jz_pc_r_varint(r, &q->offset));
    return Q(relation_query_wire__deserialize)(r, &q->relation);
}

jz_err Q(Query__Deserialize__deserialize)(jz_pc_r *r, jazz_model_Query *out) {
    memset(out, 0, sizeof(*out));
    JZ_TRY(jz_pc_r_enter(r));
    jz_err e = query_deserialize_fields(r, out);
    jz_pc_r_leave(r);
    if (e.code) Q(Query__drop)(out);
    return e;
}

/* ================================================================ RelationQuery serde (JSON, human-readable) */
static const char *const ROW_ID_NAMES[] = {"Current", "Outer", "Frontier"};
static const char *const CMP_OP_NAMES[] = {"Eq", "Ne", "Lt", "Le", "Gt", "Ge"};
static const char *const ORDER_DIRECTION_NAMES[] = {"Asc", "Desc"};

static void json_column_ref(jz_json_w *w, const jazz_model_RelationColumnRef *c) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "scope"); jazz_model__OptStr__serialize_json(w, &c->scope);
    jz_jw_key(w, "column"); jazz_model__json_w_str(w, &c->column);
    jz_jw_end_obj(w);
}
static void json_newtype(jz_json_w *w, const char *variant) { jz_jw_begin_obj(w); jz_jw_key(w, variant); }

static void json_value_ref(jz_json_w *w, const jazz_model_RelationValueRef *v) {
    static const char *const NAMES[] = {"Literal", "Param", "SessionRef", "OuterColumn", "FrontierColumn", "RowId"};
    json_newtype(w, NAMES[v->tag]);
    switch (v->tag) {
    case JAZZ_RVAL_LITERAL: jazz_model__json_w_value(w, v->literal); break;
    case JAZZ_RVAL_PARAM: jazz_model__json_w_str(w, &v->param); break;
    case JAZZ_RVAL_SESSION_REF: jazz_model__StrVec__serialize_json(w, &v->session_ref); break;
    case JAZZ_RVAL_OUTER_COLUMN:
    case JAZZ_RVAL_FRONTIER_COLUMN: json_column_ref(w, &v->column); break;
    default: jazz_model__json_w_cstr(w, ROW_ID_NAMES[v->row_id]); break;
    }
    jz_jw_end_obj(w);
}
static void json_key_ref(jz_json_w *w, const jazz_model_RelationKeyRef *k) {
    if (k->tag == JAZZ_KEYREF_COLUMN) { json_newtype(w, "Column"); json_column_ref(w, &k->column); }
    else { json_newtype(w, "RowId"); jazz_model__json_w_cstr(w, ROW_ID_NAMES[k->row_id]); }
    jz_jw_end_obj(w);
}
static void json_key_refs(jz_json_w *w, const jazz_model_RelationKeyRefVec *v) {
    jz_jw_begin_arr(w);
    JZ_VEC_FOR(const jazz_model_RelationKeyRef, k, *v) json_key_ref(w, k);
    jz_jw_end_arr(w);
}
static void json_bound(jz_json_w *w, jazz_model_RecursionBound b) {
    if (b.tag == JAZZ_BOUND_FIXPOINT) { jazz_model__json_w_cstr(w, "Fixpoint"); return; }
    json_newtype(w, "MaxDepth");
    jz_jw_u64(w, b.max_depth);
    jz_jw_end_obj(w);
}

static void json_relation_predicate(jz_json_w *w, const jazz_model_RelationPredicate *p) {
    static const char *const NAMES[] = {"Cmp", "IsNull", "IsNotNull", "In", "Contains", "EnumMatch", "And", "Or", "Not", "True", "False"};
    if (p->tag == JAZZ_RPE_TRUE || p->tag == JAZZ_RPE_FALSE) { jazz_model__json_w_cstr(w, NAMES[p->tag]); return; }
    json_newtype(w, NAMES[p->tag]);
    switch (p->tag) {
    case JAZZ_RPE_CMP:
        jz_jw_begin_obj(w);
        jz_jw_key(w, "left"); json_column_ref(w, &p->left);
        jz_jw_key(w, "op"); jazz_model__json_w_cstr(w, CMP_OP_NAMES[p->op]);
        jz_jw_key(w, "right"); json_value_ref(w, &p->right);
        jz_jw_end_obj(w);
        break;
    case JAZZ_RPE_IS_NULL:
    case JAZZ_RPE_IS_NOT_NULL:
        jz_jw_begin_obj(w);
        jz_jw_key(w, "column"); json_column_ref(w, &p->left);
        jz_jw_end_obj(w);
        break;
    case JAZZ_RPE_IN:
        jz_jw_begin_obj(w);
        jz_jw_key(w, "left"); json_column_ref(w, &p->left);
        jz_jw_key(w, "values");
        jz_jw_begin_arr(w);
        JZ_VEC_FOR(const jazz_model_RelationValueRef, v, p->values) json_value_ref(w, v);
        jz_jw_end_arr(w);
        jz_jw_end_obj(w);
        break;
    case JAZZ_RPE_CONTAINS:
        jz_jw_begin_obj(w);
        jz_jw_key(w, "left"); json_column_ref(w, &p->left);
        jz_jw_key(w, "right"); json_value_ref(w, &p->right);
        jz_jw_end_obj(w);
        break;
    case JAZZ_RPE_ENUM_MATCH:
        jz_jw_begin_obj(w);
        jz_jw_key(w, "column"); json_column_ref(w, &p->left);
        jz_jw_key(w, "case"); jazz_model__json_w_str(w, &p->case_);
        jz_jw_key(w, "payload"); json_relation_predicate(w, p->inner);
        jz_jw_end_obj(w);
        break;
    case JAZZ_RPE_AND:
    case JAZZ_RPE_OR:
        jz_jw_begin_arr(w);
        JZ_VEC_FOR(const jazz_model_RelationPredicate, c, p->items) json_relation_predicate(w, c);
        jz_jw_end_arr(w);
        break;
    default: json_relation_predicate(w, p->inner); break;
    }
    jz_jw_end_obj(w);
}

static void json_relation_expr(jz_json_w *w, const jazz_model_RelationExpr *e) {
    static const char *const NAMES[] = {"TableScan", "Filter", "Union", "Join", "Project", "Gather", "Distinct", "OrderBy", "Offset", "Limit"};
    json_newtype(w, NAMES[e->tag]);
    jz_jw_begin_obj(w);
    switch (e->tag) {
    case JAZZ_REXPR_TABLE_SCAN:
        jz_jw_key(w, "table"); jazz_model__json_w_str(w, &e->table);
        if (e->alias.some) { jz_jw_key(w, "alias"); jazz_model__json_w_str(w, &e->alias.s); } /* skip_serializing_if None */
        break;
    case JAZZ_REXPR_FILTER:
        jz_jw_key(w, "input"); json_relation_expr(w, e->input);
        jz_jw_key(w, "predicate"); json_relation_predicate(w, &e->predicate);
        break;
    case JAZZ_REXPR_UNION:
        jz_jw_key(w, "inputs");
        jz_jw_begin_arr(w);
        JZ_VEC_FOR(const jazz_model_RelationUnionArm, arm, e->inputs) {
            jz_jw_begin_obj(w);
            jz_jw_key(w, "label"); jazz_model__json_w_str(w, &arm->label);
            jz_jw_key(w, "input"); json_relation_expr(w, &arm->input);
            jz_jw_end_obj(w);
        }
        jz_jw_end_arr(w);
        break;
    case JAZZ_REXPR_JOIN:
        jz_jw_key(w, "left"); json_relation_expr(w, e->input);
        jz_jw_key(w, "right"); json_relation_expr(w, e->right);
        jz_jw_key(w, "on");
        jz_jw_begin_arr(w);
        JZ_VEC_FOR(const jazz_model_RelationJoinCondition, c, e->on) {
            jz_jw_begin_obj(w);
            jz_jw_key(w, "left"); json_column_ref(w, &c->left);
            jz_jw_key(w, "right"); json_column_ref(w, &c->right);
            jz_jw_end_obj(w);
        }
        jz_jw_end_arr(w);
        jz_jw_key(w, "join_kind"); jazz_model__json_w_cstr(w, e->join_kind == JAZZ_JOIN_INNER ? "Inner" : "Left");
        break;
    case JAZZ_REXPR_PROJECT:
        jz_jw_key(w, "input"); json_relation_expr(w, e->input);
        jz_jw_key(w, "columns");
        jz_jw_begin_arr(w);
        JZ_VEC_FOR(const jazz_model_RelationProjectColumn, c, e->columns) {
            jz_jw_begin_obj(w);
            jz_jw_key(w, "alias"); jazz_model__json_w_str(w, &c->alias);
            jz_jw_key(w, "expr"); json_key_ref(w, &c->expr);
            jz_jw_end_obj(w);
        }
        jz_jw_end_arr(w);
        break;
    case JAZZ_REXPR_GATHER:
        jz_jw_key(w, "seed"); json_relation_expr(w, e->input);
        jz_jw_key(w, "step"); json_relation_expr(w, e->right);
        jz_jw_key(w, "frontier_key"); json_key_ref(w, &e->frontier_key);
        jz_jw_key(w, "bound"); json_bound(w, e->bound);
        jz_jw_key(w, "dedupe_key"); json_key_refs(w, &e->keys);
        break;
    case JAZZ_REXPR_DISTINCT:
        jz_jw_key(w, "input"); json_relation_expr(w, e->input);
        jz_jw_key(w, "key"); json_key_refs(w, &e->keys);
        break;
    case JAZZ_REXPR_ORDER_BY:
        jz_jw_key(w, "input"); json_relation_expr(w, e->input);
        jz_jw_key(w, "terms");
        jz_jw_begin_arr(w);
        JZ_VEC_FOR(const jazz_model_RelationOrderBy, t, e->terms) {
            jz_jw_begin_obj(w);
            jz_jw_key(w, "column"); json_column_ref(w, &t->column);
            jz_jw_key(w, "direction"); jazz_model__json_w_cstr(w, ORDER_DIRECTION_NAMES[t->direction]);
            jz_jw_end_obj(w);
        }
        jz_jw_end_arr(w);
        break;
    default:
        jz_jw_key(w, "input"); json_relation_expr(w, e->input);
        jz_jw_key(w, e->tag == JAZZ_REXPR_OFFSET ? "offset" : "limit");
        jz_jw_u64(w, e->n);
        break;
    }
    jz_jw_end_obj(w);
    jz_jw_end_obj(w);
}

void Q(RelationQuery__Serialize__serialize_json)(jz_json_w *w, const jazz_model_RelationQuery *q) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "rel");
    json_relation_expr(w, &q->rel);
    jz_jw_end_obj(w);
}

#define JSON_VEC(w, key, vec, T, N) do { jz_jw_key(w, key); jz_jw_begin_arr(w); \
    JZ_VEC_FOR(const T, _it, vec) Q(N##__Serialize__serialize_json)(w, _it); jz_jw_end_arr(w); } while (0)

void Q(Query__Serialize__serialize_json)(jz_json_w *w, const jazz_model_Query *q) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "table"); jazz_model__json_w_str(w, &q->table);
    JSON_VEC(w, "filters", q->filters, jazz_model_Predicate, Predicate);
    JSON_VEC(w, "joins", q->joins, jazz_model_JoinVia, JoinVia);
    jz_jw_key(w, "flat_join");
    if (q->flat_join) Q(FlatJoin__Serialize__serialize_json)(w, q->flat_join); else jz_jw_null(w);
    JSON_VEC(w, "policy_branches", q->policy_branches, jazz_model_PolicyBranch, PolicyBranch);
    JSON_VEC(w, "reachable", q->reachable, jazz_model_ReachableVia, ReachableVia);
    JSON_VEC(w, "inherits", q->inherits, jazz_model_InheritsVia, InheritsVia);
    JSON_VEC(w, "includes", q->includes, jazz_model_Include, Include);
    JSON_VEC(w, "array_subqueries", q->array_subqueries, jazz_model_ArraySubquery, ArraySubquery);
    jz_jw_key(w, "select");
    if (q->has_select) jazz_model__StrVec__serialize_json(w, &q->select); else jz_jw_null(w);
    JSON_VEC(w, "order_by", q->order_by, jazz_model_OrderBy, OrderBy);
    jz_jw_key(w, "aggregate");
    if (q->aggregate) Q(AggregateQuery__Serialize__serialize_json)(w, q->aggregate); else jz_jw_null(w);
    jz_jw_key(w, "limit"); jazz_model__OptU64__serialize_json(w, q->has_limit, q->limit);
    jz_jw_key(w, "offset"); jz_jw_u64(w, q->offset);
    /* relation_query_wire::serialize: human-readable keeps the relation tree */
    jz_jw_key(w, "relation");
    if (q->relation) Q(RelationQuery__Serialize__serialize_json)(w, q->relation); else jz_jw_null(w);
    jz_jw_end_obj(w);
}
