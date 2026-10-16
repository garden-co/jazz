/* query/policy_and_request_contracts.rs: policy branches, request contract
 * types (joins, reachability, includes, array subqueries, aggregates),
 * predicates/operands with bounded decoding, and provider-claim keys.
 * Derived Clone/PartialEq/drop glue and serde impls are hand-written. */
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
#define PC_VEC_W(w, vec, T, N) do { jz_pc_seq(w, (vec).len); JZ_VEC_FOR(const T, _it, vec) Q(N##__Serialize__serialize)(w, _it); } while (0)
#define PC_VEC_R(r, vec, N) do { u32 _n; JZ_TRY(jz_pc_r_seq(r, &_n)); \
    for (u32 _i = 0; _i < _n; _i++) JZ_TRY(Q(N##__Deserialize__deserialize)(r, JAZZ_VEC_SLOT(vec))); } while (0)
#define JSON_VEC(w, vec, T, N) do { jz_jw_begin_arr(w); \
    JZ_VEC_FOR(const T, _it, vec) Q(N##__Serialize__serialize_json)(w, _it); jz_jw_end_arr(w); } while (0)
#define JKEY_STR(w, k, s) do { jz_jw_key(w, k); jazz_model__json_w_str(w, &(s)); } while (0)
#define JKEY_OSTR(w, k, s) do { jz_jw_key(w, k); jazz_model__OptStr__serialize_json(w, &(s)); } while (0)

static const char *const JOIN_MODE_NAMES[] = {"Inner", "Holes"};
static const char *const ASQ_REQUIREMENT_NAMES[] = {"Optional", "AtLeastOne", "MatchCorrelationCardinality"};
static const char *const ORDER_DIRECTION_NAMES[] = {"Asc", "Desc"};
static const char *const AGG_FUNCTION_NAMES[] = {"Count", "Sum", "Avg", "Min", "Max"};
static const char *const JOIN_TARGET_NAMES[] = {"Column", "RowId", "Uncorrelated"};
static const char *const INHERITS_OPERATION_NAMES[] = {"Select", "Insert", "Update", "Delete"};
static const char *const OPERAND_NAMES[] = {"Column", "Param", "Claim", "Literal"};
static const char *const PREDICATE_NAMES[] = {"All", "Any", "Not", "Eq", "Ne", "In", "Gt", "Gte", "Lt", "Lte", "Contains", "EnumMatch", "IsNull"};

static jz_err r_unit_variant(jz_pc_r *r, u32 n, u8 *out) {
    u32 idx;
    JZ_TRY(jz_pc_r_variant(r, &idx, n));
    *out = (u8)idx;
    return JZ_OK;
}

/* ================================================================ Operand */
void Q(Operand__drop)(jazz_model_Operand *v) {
    if (v->tag == JAZZ_OPERAND_LITERAL) groove__records__values__Value__drop(&v->literal);
    else jz_str_drop(&v->name);
}
void Q(Operand__Clone__clone)(jazz_model_Operand *d, const jazz_model_Operand *s) {
    memset(d, 0, sizeof(*d));
    d->tag = s->tag;
    if (s->tag == JAZZ_OPERAND_LITERAL) groove__records__values__Value__Clone__clone(&d->literal, &s->literal);
    else d->name = jz_str_clone(&s->name);
}
bool Q(Operand__PartialEq__eq)(const jazz_model_Operand *a, const jazz_model_Operand *b) {
    if (a->tag != b->tag) return false;
    if (a->tag == JAZZ_OPERAND_LITERAL) return groove__records__values__Value__PartialEq__eq(&a->literal, &b->literal);
    return STR_EQ(a->name, b->name);
}
V_WRAP(Operand, jazz_model_Operand)

void Q(Operand__Serialize__serialize)(jz_pc_w *w, const jazz_model_Operand *v) {
    jz_pc_variant(w, v->tag);
    if (v->tag == JAZZ_OPERAND_LITERAL) groove__records__values__Value__Serialize__serialize(w, &v->literal);
    else jz_pc_str(w, &v->name);
}
jz_err Q(Operand__Deserialize__deserialize)(jz_pc_r *r, jazz_model_Operand *out) {
    u32 idx;
    JZ_TRY(jz_pc_r_variant(r, &idx, 4));
    out->tag = (u8)idx;
    if (idx == JAZZ_OPERAND_LITERAL) return groove__records__values__Value__Deserialize__deserialize(r, &out->literal);
    return jz_pc_r_str(r, &out->name);
}
void Q(Operand__Serialize__serialize_json)(jz_json_w *w, const jazz_model_Operand *v) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, OPERAND_NAMES[v->tag]);
    if (v->tag == JAZZ_OPERAND_LITERAL) groove__records__values__Value__Serialize__serialize_json(w, &v->literal);
    else jazz_model__json_w_str(w, &v->name);
    jz_jw_end_obj(w);
}

/* ================================================================ Predicate */
jazz_model_Predicate *Q(Predicate__box)(jazz_model_Predicate p) {
    jazz_model_Predicate *b = JZ_NEW(jazz_model_Predicate);
    *b = p;
    return b;
}
void Q(Predicate__drop)(jazz_model_Predicate *v) {
    switch (v->tag) {
    case JAZZ_PRED_ALL:
    case JAZZ_PRED_ANY: VDROP(v->children, Predicate); break;
    case JAZZ_PRED_ENUM_MATCH:
        jz_str_drop(&v->column);
        jz_str_drop(&v->case_);
        /* fallthrough */
    case JAZZ_PRED_NOT:
        if (v->inner) { Q(Predicate__drop)(v->inner); jz_free(v->inner); v->inner = NULL; }
        break;
    case JAZZ_PRED_IN:
        Q(Operand__drop)(&v->left);
        VDROP(v->values, Operand);
        break;
    case JAZZ_PRED_IS_NULL: Q(Operand__drop)(&v->left); break;
    default:
        Q(Operand__drop)(&v->left);
        Q(Operand__drop)(&v->right);
        break;
    }
}
void Q(Predicate__Clone__clone)(jazz_model_Predicate *d, const jazz_model_Predicate *s) {
    memset(d, 0, sizeof(*d));
    d->tag = s->tag;
    switch (s->tag) {
    case JAZZ_PRED_ALL:
    case JAZZ_PRED_ANY: VCLONE(d->children, s->children, Predicate); break;
    case JAZZ_PRED_ENUM_MATCH:
        d->column = jz_str_clone(&s->column);
        d->case_ = jz_str_clone(&s->case_);
        /* fallthrough */
    case JAZZ_PRED_NOT:
        d->inner = JZ_NEW(jazz_model_Predicate);
        Q(Predicate__Clone__clone)(d->inner, s->inner);
        break;
    case JAZZ_PRED_IN:
        Q(Operand__Clone__clone)(&d->left, &s->left);
        VCLONE(d->values, s->values, Operand);
        break;
    case JAZZ_PRED_IS_NULL: Q(Operand__Clone__clone)(&d->left, &s->left); break;
    default:
        Q(Operand__Clone__clone)(&d->left, &s->left);
        Q(Operand__Clone__clone)(&d->right, &s->right);
        break;
    }
}
bool Q(Predicate__PartialEq__eq)(const jazz_model_Predicate *a, const jazz_model_Predicate *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_PRED_ALL:
    case JAZZ_PRED_ANY: return VEQ(a->children, b->children, Predicate);
    case JAZZ_PRED_NOT: return Q(Predicate__PartialEq__eq)(a->inner, b->inner);
    case JAZZ_PRED_ENUM_MATCH:
        return STR_EQ(a->column, b->column) && STR_EQ(a->case_, b->case_) && Q(Predicate__PartialEq__eq)(a->inner, b->inner);
    case JAZZ_PRED_IN: return Q(Operand__PartialEq__eq)(&a->left, &b->left) && VEQ(a->values, b->values, Operand);
    case JAZZ_PRED_IS_NULL: return Q(Operand__PartialEq__eq)(&a->left, &b->left);
    default: return Q(Operand__PartialEq__eq)(&a->left, &b->left) && Q(Operand__PartialEq__eq)(&a->right, &b->right);
    }
}
V_WRAP(Predicate, jazz_model_Predicate)

/* Calls `f` on every operand, descending through All/Any/Not/EnumMatch payloads. */
void Q(Predicate__for_each_operand_mut)(jazz_model_Predicate *p, jazz_model_OperandMutFn f, void *ctx) {
    switch (p->tag) {
    case JAZZ_PRED_ALL:
    case JAZZ_PRED_ANY:
        JZ_VEC_FOR(jazz_model_Predicate, c, p->children) Q(Predicate__for_each_operand_mut)(c, f, ctx);
        break;
    case JAZZ_PRED_NOT:
    case JAZZ_PRED_ENUM_MATCH: Q(Predicate__for_each_operand_mut)(p->inner, f, ctx); break;
    case JAZZ_PRED_IN:
        f(&p->left, ctx);
        JZ_VEC_FOR(jazz_model_Operand, o, p->values) f(o, ctx);
        break;
    case JAZZ_PRED_IS_NULL: f(&p->left, ctx); break;
    default:
        f(&p->left, ctx);
        f(&p->right, ctx);
        break;
    }
}
bool Q(Predicate__any_operand)(const jazz_model_Predicate *p, jazz_model_OperandPredFn f, void *ctx) {
    switch (p->tag) {
    case JAZZ_PRED_ALL:
    case JAZZ_PRED_ANY:
        JZ_VEC_FOR(const jazz_model_Predicate, c, p->children) if (Q(Predicate__any_operand)(c, f, ctx)) return true;
        return false;
    case JAZZ_PRED_NOT:
    case JAZZ_PRED_ENUM_MATCH: return Q(Predicate__any_operand)(p->inner, f, ctx);
    case JAZZ_PRED_IN:
        if (f(&p->left, ctx)) return true;
        JZ_VEC_FOR(const jazz_model_Operand, o, p->values) if (f(o, ctx)) return true;
        return false;
    case JAZZ_PRED_IS_NULL: return f(&p->left, ctx);
    default: return f(&p->left, ctx) || f(&p->right, ctx);
    }
}

void Q(Predicate__Serialize__serialize)(jz_pc_w *w, const jazz_model_Predicate *p) {
    jz_pc_variant(w, p->tag);
    switch (p->tag) {
    case JAZZ_PRED_ALL:
    case JAZZ_PRED_ANY: PC_VEC_W(w, p->children, jazz_model_Predicate, Predicate); break;
    case JAZZ_PRED_NOT: Q(Predicate__Serialize__serialize)(w, p->inner); break;
    case JAZZ_PRED_IN:
        Q(Operand__Serialize__serialize)(w, &p->left);
        PC_VEC_W(w, p->values, jazz_model_Operand, Operand);
        break;
    case JAZZ_PRED_ENUM_MATCH:
        jz_pc_str(w, &p->column);
        jz_pc_str(w, &p->case_);
        Q(Predicate__Serialize__serialize)(w, p->inner);
        break;
    case JAZZ_PRED_IS_NULL: Q(Operand__Serialize__serialize)(w, &p->left); break;
    default:
        Q(Operand__Serialize__serialize)(w, &p->left);
        Q(Operand__Serialize__serialize)(w, &p->right);
        break;
    }
}

/* thread_local PREDICATE_DECODE: (depth, PolicyExpressionBudget); depth 0 = idle. */
static u64 PREDICATE_DECODE_DEPTH;
static jazz_model_PolicyExpressionBudget PREDICATE_DECODE_BUDGET;

/* deserialize_predicate_children: a length header the node budget cannot hold
 * is refused before any child is read or allocated. */
static jz_err deserialize_predicate_children(jz_pc_r *r, jazz_model_PredicateVec *out) {
    u32 capacity;
    jazz_model_PolicyExpressionLimitError err;
    JZ_TRY(jz_pc_r_seq(r, &capacity));
    if (!jazz_model__query__policy_limits__PolicyExpressionBudget__reserve(&PREDICATE_DECODE_BUDGET, capacity, &err))
        return jazz_model__query__policy_limits__PolicyExpressionLimitError__into_err(&err);
    jz_vec_reserve(out, capacity, sizeof(jazz_model_Predicate));
    for (u32 i = 0; i < capacity; i++) JZ_TRY(Q(Predicate__Deserialize__deserialize)(r, JAZZ_VEC_SLOT(*out)));
    return JZ_OK;
}

/* #[serde(remote = "Self", deny_unknown_fields, expecting = "a bounded policy predicate")] */
static jz_err predicate_deserialize_remote(jz_pc_r *r, jazz_model_Predicate *out) {
    u32 idx;
    JZ_TRY(jz_pc_r_variant(r, &idx, JAZZ_PRED_COUNT));
    out->tag = (u8)idx;
    switch (idx) {
    case JAZZ_PRED_ALL:
    case JAZZ_PRED_ANY: return deserialize_predicate_children(r, &out->children);
    case JAZZ_PRED_NOT:
        out->inner = JZ_NEW(jazz_model_Predicate);
        return Q(Predicate__Deserialize__deserialize)(r, out->inner);
    case JAZZ_PRED_IN:
        JZ_TRY(Q(Operand__Deserialize__deserialize)(r, &out->left));
        PC_VEC_R(r, out->values, Operand);
        return JZ_OK;
    case JAZZ_PRED_ENUM_MATCH:
        JZ_TRY(jz_pc_r_str(r, &out->column));
        JZ_TRY(jz_pc_r_str(r, &out->case_));
        out->inner = JZ_NEW(jazz_model_Predicate);
        return Q(Predicate__Deserialize__deserialize)(r, out->inner);
    case JAZZ_PRED_IS_NULL: return Q(Operand__Deserialize__deserialize)(r, &out->left);
    default:
        JZ_TRY(Q(Operand__Deserialize__deserialize)(r, &out->left));
        return Q(Operand__Deserialize__deserialize)(r, &out->right);
    }
}

/* Counts every node against the budget of the outermost predicate at its
 * nesting depth before its contents are read. */
jz_err Q(Predicate__Deserialize__deserialize)(jz_pc_r *r, jazz_model_Predicate *out) {
    jazz_model_PolicyExpressionLimitError err;
    jz_err e = JZ_OK;
    PREDICATE_DECODE_DEPTH += 1;
    if (!jazz_model__query__policy_limits__PolicyExpressionBudget__enter(&PREDICATE_DECODE_BUDGET, PREDICATE_DECODE_DEPTH, &err))
        e = jazz_model__query__policy_limits__PolicyExpressionLimitError__into_err(&err);
    else
        e = predicate_deserialize_remote(r, out);
    /* struct Leave: Drop */
    PREDICATE_DECODE_DEPTH -= 1;
    if (PREDICATE_DECODE_DEPTH == 0) PREDICATE_DECODE_BUDGET.nodes = 0;
    return e;
}

void Q(Predicate__Serialize__serialize_json)(jz_json_w *w, const jazz_model_Predicate *p) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, PREDICATE_NAMES[p->tag]);
    switch (p->tag) {
    case JAZZ_PRED_ALL:
    case JAZZ_PRED_ANY: JSON_VEC(w, p->children, jazz_model_Predicate, Predicate); break;
    case JAZZ_PRED_NOT: Q(Predicate__Serialize__serialize_json)(w, p->inner); break;
    case JAZZ_PRED_IN:
        jz_jw_begin_arr(w);
        Q(Operand__Serialize__serialize_json)(w, &p->left);
        JSON_VEC(w, p->values, jazz_model_Operand, Operand);
        jz_jw_end_arr(w);
        break;
    case JAZZ_PRED_ENUM_MATCH:
        jz_jw_begin_obj(w);
        JKEY_STR(w, "column", p->column);
        JKEY_STR(w, "case", p->case_);
        jz_jw_key(w, "payload");
        Q(Predicate__Serialize__serialize_json)(w, p->inner);
        jz_jw_end_obj(w);
        break;
    case JAZZ_PRED_IS_NULL: Q(Operand__Serialize__serialize_json)(w, &p->left); break;
    default:
        jz_jw_begin_arr(w);
        Q(Operand__Serialize__serialize_json)(w, &p->left);
        Q(Operand__Serialize__serialize_json)(w, &p->right);
        jz_jw_end_arr(w);
        break;
    }
    jz_jw_end_obj(w);
}

/* ================================================================ Include / OrderBy */
void Q(Include__drop)(jazz_model_Include *v) { jz_str_drop(&v->path); }
void Q(Include__Clone__clone)(jazz_model_Include *d, const jazz_model_Include *s) {
    *d = *s;
    d->path = jz_str_clone(&s->path);
}
bool Q(Include__PartialEq__eq)(const jazz_model_Include *a, const jazz_model_Include *b) {
    return STR_EQ(a->path, b->path) && a->join_mode == b->join_mode && a->require == b->require;
}
V_WRAP(Include, jazz_model_Include)
void Q(Include__Serialize__serialize)(jz_pc_w *w, const jazz_model_Include *v) {
    jz_pc_str(w, &v->path);
    jz_pc_variant(w, v->join_mode);
    jz_pc_bool(w, v->require);
}
jz_err Q(Include__Deserialize__deserialize)(jz_pc_r *r, jazz_model_Include *out) {
    JZ_TRY(jz_pc_r_str(r, &out->path));
    JZ_TRY(r_unit_variant(r, 2, &out->join_mode));
    return jz_pc_r_bool(r, &out->require);
}
void Q(Include__Serialize__serialize_json)(jz_json_w *w, const jazz_model_Include *v) {
    jz_jw_begin_obj(w);
    JKEY_STR(w, "path", v->path);
    jz_jw_key(w, "join_mode"); jazz_model__json_w_cstr(w, JOIN_MODE_NAMES[v->join_mode]);
    jz_jw_key(w, "require"); jz_jw_bool(w, v->require);
    jz_jw_end_obj(w);
}

void Q(OrderBy__drop)(jazz_model_OrderBy *v) { jz_str_drop(&v->column); }
void Q(OrderBy__Clone__clone)(jazz_model_OrderBy *d, const jazz_model_OrderBy *s) {
    d->column = jz_str_clone(&s->column);
    d->direction = s->direction;
}
bool Q(OrderBy__PartialEq__eq)(const jazz_model_OrderBy *a, const jazz_model_OrderBy *b) {
    return STR_EQ(a->column, b->column) && a->direction == b->direction;
}
V_WRAP(OrderBy, jazz_model_OrderBy)
void Q(OrderBy__Serialize__serialize)(jz_pc_w *w, const jazz_model_OrderBy *v) {
    jz_pc_str(w, &v->column);
    jz_pc_variant(w, v->direction);
}
jz_err Q(OrderBy__Deserialize__deserialize)(jz_pc_r *r, jazz_model_OrderBy *out) {
    JZ_TRY(jz_pc_r_str(r, &out->column));
    return r_unit_variant(r, 2, &out->direction);
}
void Q(OrderBy__Serialize__serialize_json)(jz_json_w *w, const jazz_model_OrderBy *v) {
    jz_jw_begin_obj(w);
    JKEY_STR(w, "column", v->column);
    jz_jw_key(w, "direction"); jazz_model__json_w_cstr(w, ORDER_DIRECTION_NAMES[v->direction]);
    jz_jw_end_obj(w);
}

/* ================================================================ ArraySubquery */
void Q(ArraySubquery__drop)(jazz_model_ArraySubquery *v) {
    jz_str_drop(&v->column_name);
    jz_str_drop(&v->table);
    jz_str_drop(&v->inner_column);
    jz_str_drop(&v->outer_column);
    VDROP(v->filters, Predicate);
    if (v->has_select) jazz_model__StrVec__drop(&v->select);
    v->has_select = false;
    VDROP(v->order_by, OrderBy);
    VDROP(v->nested_arrays, ArraySubquery);
}
void Q(ArraySubquery__Clone__clone)(jazz_model_ArraySubquery *d, const jazz_model_ArraySubquery *s) {
    memset(d, 0, sizeof(*d));
    d->column_name = jz_str_clone(&s->column_name);
    d->table = jz_str_clone(&s->table);
    d->inner_column = jz_str_clone(&s->inner_column);
    d->outer_column = jz_str_clone(&s->outer_column);
    VCLONE(d->filters, s->filters, Predicate);
    d->has_select = s->has_select;
    if (s->has_select) jazz_model__StrVec__clone(&d->select, &s->select);
    VCLONE(d->order_by, s->order_by, OrderBy);
    d->has_limit = s->has_limit;
    d->limit = s->limit;
    d->offset = s->offset;
    d->requirement = s->requirement;
    VCLONE(d->nested_arrays, s->nested_arrays, ArraySubquery);
}
bool Q(ArraySubquery__PartialEq__eq)(const jazz_model_ArraySubquery *a, const jazz_model_ArraySubquery *b) {
    return STR_EQ(a->column_name, b->column_name) && STR_EQ(a->table, b->table) &&
           STR_EQ(a->inner_column, b->inner_column) && STR_EQ(a->outer_column, b->outer_column) &&
           VEQ(a->filters, b->filters, Predicate) && a->has_select == b->has_select &&
           (!a->has_select || jazz_model__StrVec__eq(&a->select, &b->select)) && VEQ(a->order_by, b->order_by, OrderBy) &&
           a->has_limit == b->has_limit && (!a->has_limit || a->limit == b->limit) && a->offset == b->offset &&
           a->requirement == b->requirement && VEQ(a->nested_arrays, b->nested_arrays, ArraySubquery);
}
V_WRAP(ArraySubquery, jazz_model_ArraySubquery)

/* fn local_column: keep the part after the last '.' */
static jz_str array_subquery_local_column(jz_str column) {
    for (u32 i = column.len; i > 0; i--) {
        if (column.ptr[i - 1] == '.') {
            jz_str local = jz_str_from_slice(jz_slice_of(column.ptr + i, column.len - i));
            jz_str_drop(&column);
            return local;
        }
    }
    return column;
}
jazz_model_ArraySubquery Q(ArraySubquery__new)(jz_str column_name, jz_str table, jz_str inner_column, jz_str outer_column) {
    jazz_model_ArraySubquery s;
    memset(&s, 0, sizeof(s));
    s.column_name = column_name;
    s.table = table;
    s.inner_column = array_subquery_local_column(inner_column);
    s.outer_column = array_subquery_local_column(outer_column);
    s.requirement = JAZZ_ASQ_OPTIONAL;
    return s;
}

void Q(ArraySubquery__Serialize__serialize)(jz_pc_w *w, const jazz_model_ArraySubquery *v) {
    jz_pc_str(w, &v->column_name);
    jz_pc_str(w, &v->table);
    jz_pc_str(w, &v->inner_column);
    jz_pc_str(w, &v->outer_column);
    PC_VEC_W(w, v->filters, jazz_model_Predicate, Predicate);
    jz_pc_option(w, v->has_select);
    if (v->has_select) jazz_model__StrVec__serialize_pc(w, &v->select);
    PC_VEC_W(w, v->order_by, jazz_model_OrderBy, OrderBy);
    jazz_model__OptU64__serialize_pc(w, v->has_limit, v->limit);
    jz_pc_varint(w, v->offset);
    jz_pc_variant(w, v->requirement);
    PC_VEC_W(w, v->nested_arrays, jazz_model_ArraySubquery, ArraySubquery);
}
jz_err Q(ArraySubquery__Deserialize__deserialize)(jz_pc_r *r, jazz_model_ArraySubquery *out) {
    JZ_TRY(jz_pc_r_enter(r));
    JZ_TRY(jz_pc_r_str(r, &out->column_name));
    JZ_TRY(jz_pc_r_str(r, &out->table));
    JZ_TRY(jz_pc_r_str(r, &out->inner_column));
    JZ_TRY(jz_pc_r_str(r, &out->outer_column));
    PC_VEC_R(r, out->filters, Predicate);
    JZ_TRY(jz_pc_r_option(r, &out->has_select));
    if (out->has_select) JZ_TRY(jazz_model__StrVec__deserialize_pc(r, &out->select));
    PC_VEC_R(r, out->order_by, OrderBy);
    JZ_TRY(jazz_model__OptU64__deserialize_pc(r, &out->has_limit, &out->limit));
    JZ_TRY(jz_pc_r_varint(r, &out->offset));
    JZ_TRY(r_unit_variant(r, 3, &out->requirement));
    PC_VEC_R(r, out->nested_arrays, ArraySubquery);
    jz_pc_r_leave(r);
    return JZ_OK;
}
void Q(ArraySubquery__Serialize__serialize_json)(jz_json_w *w, const jazz_model_ArraySubquery *v) {
    jz_jw_begin_obj(w);
    JKEY_STR(w, "column_name", v->column_name);
    JKEY_STR(w, "table", v->table);
    JKEY_STR(w, "inner_column", v->inner_column);
    JKEY_STR(w, "outer_column", v->outer_column);
    jz_jw_key(w, "filters"); JSON_VEC(w, v->filters, jazz_model_Predicate, Predicate);
    jz_jw_key(w, "select");
    if (v->has_select) jazz_model__StrVec__serialize_json(w, &v->select); else jz_jw_null(w);
    jz_jw_key(w, "order_by"); JSON_VEC(w, v->order_by, jazz_model_OrderBy, OrderBy);
    jz_jw_key(w, "limit"); jazz_model__OptU64__serialize_json(w, v->has_limit, v->limit);
    jz_jw_key(w, "offset"); jz_jw_u64(w, v->offset);
    jz_jw_key(w, "requirement"); jazz_model__json_w_cstr(w, ASQ_REQUIREMENT_NAMES[v->requirement]);
    jz_jw_key(w, "nested_arrays"); JSON_VEC(w, v->nested_arrays, jazz_model_ArraySubquery, ArraySubquery);
    jz_jw_end_obj(w);
}

/* ================================================================ Aggregate / AggregateQuery */
void Q(Aggregate__drop)(jazz_model_Aggregate *v) {
    jazz_model__OptStr__drop(&v->column);
    jz_str_drop(&v->alias);
}
void Q(Aggregate__Clone__clone)(jazz_model_Aggregate *d, const jazz_model_Aggregate *s) {
    d->function = s->function;
    d->column = OSTR_CLONE(s->column);
    d->alias = jz_str_clone(&s->alias);
}
bool Q(Aggregate__PartialEq__eq)(const jazz_model_Aggregate *a, const jazz_model_Aggregate *b) {
    return a->function == b->function && OSTR_EQ(a->column, b->column) && STR_EQ(a->alias, b->alias);
}
V_WRAP(Aggregate, jazz_model_Aggregate)
void Q(Aggregate__Serialize__serialize)(jz_pc_w *w, const jazz_model_Aggregate *v) {
    jz_pc_variant(w, v->function);
    jazz_model__OptStr__serialize_pc(w, &v->column);
    jz_pc_str(w, &v->alias);
}
jz_err Q(Aggregate__Deserialize__deserialize)(jz_pc_r *r, jazz_model_Aggregate *out) {
    JZ_TRY(r_unit_variant(r, 5, &out->function));
    JZ_TRY(jazz_model__OptStr__deserialize_pc(r, &out->column));
    return jz_pc_r_str(r, &out->alias);
}
void Q(Aggregate__Serialize__serialize_json)(jz_json_w *w, const jazz_model_Aggregate *v) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "function"); jazz_model__json_w_cstr(w, AGG_FUNCTION_NAMES[v->function]);
    JKEY_OSTR(w, "column", v->column);
    JKEY_STR(w, "alias", v->alias);
    jz_jw_end_obj(w);
}

void Q(AggregateQuery__drop)(jazz_model_AggregateQuery *v) {
    VDROP(v->aggregates, Aggregate);
    jazz_model__OptStr__drop(&v->group_by);
}
void Q(AggregateQuery__Clone__clone)(jazz_model_AggregateQuery *d, const jazz_model_AggregateQuery *s) {
    VCLONE(d->aggregates, s->aggregates, Aggregate);
    d->group_by = OSTR_CLONE(s->group_by);
}
bool Q(AggregateQuery__PartialEq__eq)(const jazz_model_AggregateQuery *a, const jazz_model_AggregateQuery *b) {
    return VEQ(a->aggregates, b->aggregates, Aggregate) && OSTR_EQ(a->group_by, b->group_by);
}
V_WRAP(AggregateQuery, jazz_model_AggregateQuery)
void Q(AggregateQuery__Serialize__serialize)(jz_pc_w *w, const jazz_model_AggregateQuery *v) {
    PC_VEC_W(w, v->aggregates, jazz_model_Aggregate, Aggregate);
    jazz_model__OptStr__serialize_pc(w, &v->group_by);
}
jz_err Q(AggregateQuery__Deserialize__deserialize)(jz_pc_r *r, jazz_model_AggregateQuery *out) {
    PC_VEC_R(r, out->aggregates, Aggregate);
    return jazz_model__OptStr__deserialize_pc(r, &out->group_by);
}
void Q(AggregateQuery__Serialize__serialize_json)(jz_json_w *w, const jazz_model_AggregateQuery *v) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "aggregates"); JSON_VEC(w, v->aggregates, jazz_model_Aggregate, Aggregate);
    JKEY_OSTR(w, "group_by", v->group_by);
    jz_jw_end_obj(w);
}

/* ================================================================ JoinCorrelation / JoinSourceLookup */
void Q(JoinCorrelation__drop)(jazz_model_JoinCorrelation *v) {
    jz_str_drop(&v->join_column);
    jz_str_drop(&v->source_column);
}
void Q(JoinCorrelation__Clone__clone)(jazz_model_JoinCorrelation *d, const jazz_model_JoinCorrelation *s) {
    d->join_column = jz_str_clone(&s->join_column);
    d->source_column = jz_str_clone(&s->source_column);
}
bool Q(JoinCorrelation__PartialEq__eq)(const jazz_model_JoinCorrelation *a, const jazz_model_JoinCorrelation *b) {
    return STR_EQ(a->join_column, b->join_column) && STR_EQ(a->source_column, b->source_column);
}
V_WRAP(JoinCorrelation, jazz_model_JoinCorrelation)
void Q(JoinCorrelation__Serialize__serialize)(jz_pc_w *w, const jazz_model_JoinCorrelation *v) {
    jz_pc_str(w, &v->join_column);
    jz_pc_str(w, &v->source_column);
}
jz_err Q(JoinCorrelation__Deserialize__deserialize)(jz_pc_r *r, jazz_model_JoinCorrelation *out) {
    JZ_TRY(jz_pc_r_str(r, &out->join_column));
    return jz_pc_r_str(r, &out->source_column);
}
void Q(JoinCorrelation__Serialize__serialize_json)(jz_json_w *w, const jazz_model_JoinCorrelation *v) {
    jz_jw_begin_obj(w);
    JKEY_STR(w, "join_column", v->join_column);
    JKEY_STR(w, "source_column", v->source_column);
    jz_jw_end_obj(w);
}

void Q(JoinSourceLookup__drop)(jazz_model_JoinSourceLookup *v) {
    jz_str_drop(&v->table);
    jz_str_drop(&v->row_id_source_column);
    jz_str_drop(&v->value_column);
}
void Q(JoinSourceLookup__Clone__clone)(jazz_model_JoinSourceLookup *d, const jazz_model_JoinSourceLookup *s) {
    d->table = jz_str_clone(&s->table);
    d->row_id_source_column = jz_str_clone(&s->row_id_source_column);
    d->value_column = jz_str_clone(&s->value_column);
}
bool Q(JoinSourceLookup__PartialEq__eq)(const jazz_model_JoinSourceLookup *a, const jazz_model_JoinSourceLookup *b) {
    return STR_EQ(a->table, b->table) && STR_EQ(a->row_id_source_column, b->row_id_source_column) &&
           STR_EQ(a->value_column, b->value_column);
}
V_WRAP(JoinSourceLookup, jazz_model_JoinSourceLookup)
void Q(JoinSourceLookup__Serialize__serialize)(jz_pc_w *w, const jazz_model_JoinSourceLookup *v) {
    jz_pc_str(w, &v->table);
    jz_pc_str(w, &v->row_id_source_column);
    jz_pc_str(w, &v->value_column);
}
jz_err Q(JoinSourceLookup__Deserialize__deserialize)(jz_pc_r *r, jazz_model_JoinSourceLookup *out) {
    JZ_TRY(jz_pc_r_str(r, &out->table));
    JZ_TRY(jz_pc_r_str(r, &out->row_id_source_column));
    return jz_pc_r_str(r, &out->value_column);
}
void Q(JoinSourceLookup__Serialize__serialize_json)(jz_json_w *w, const jazz_model_JoinSourceLookup *v) {
    jz_jw_begin_obj(w);
    JKEY_STR(w, "table", v->table);
    JKEY_STR(w, "row_id_source_column", v->row_id_source_column);
    JKEY_STR(w, "value_column", v->value_column);
    jz_jw_end_obj(w);
}

/* ================================================================ JoinVia */
void Q(JoinVia__drop)(jazz_model_JoinVia *v) {
    jz_str_drop(&v->table);
    jz_str_drop(&v->on_column);
    jazz_model__OptStr__drop(&v->source_column);
    if (v->source_lookup) { Q(JoinSourceLookup__drop)(v->source_lookup); jz_free(v->source_lookup); v->source_lookup = NULL; }
    VDROP(v->correlated_filters, JoinCorrelation);
    VDROP(v->filters, Predicate);
    VDROP(v->nested_joins, JoinVia);
}
void Q(JoinVia__Clone__clone)(jazz_model_JoinVia *d, const jazz_model_JoinVia *s) {
    memset(d, 0, sizeof(*d));
    d->table = jz_str_clone(&s->table);
    d->on_column = jz_str_clone(&s->on_column);
    d->target = s->target;
    d->source_column = OSTR_CLONE(s->source_column);
    if (s->source_lookup) {
        d->source_lookup = JZ_NEW(jazz_model_JoinSourceLookup);
        Q(JoinSourceLookup__Clone__clone)(d->source_lookup, s->source_lookup);
    }
    VCLONE(d->correlated_filters, s->correlated_filters, JoinCorrelation);
    VCLONE(d->filters, s->filters, Predicate);
    VCLONE(d->nested_joins, s->nested_joins, JoinVia);
}
bool Q(JoinVia__PartialEq__eq)(const jazz_model_JoinVia *a, const jazz_model_JoinVia *b) {
    if (!STR_EQ(a->table, b->table) || !STR_EQ(a->on_column, b->on_column) || a->target != b->target ||
        !OSTR_EQ(a->source_column, b->source_column))
        return false;
    if (!a->source_lookup != !b->source_lookup ||
        (a->source_lookup && !Q(JoinSourceLookup__PartialEq__eq)(a->source_lookup, b->source_lookup)))
        return false;
    return VEQ(a->correlated_filters, b->correlated_filters, JoinCorrelation) && VEQ(a->filters, b->filters, Predicate) &&
           VEQ(a->nested_joins, b->nested_joins, JoinVia);
}
V_WRAP(JoinVia, jazz_model_JoinVia)

void Q(JoinVia__Serialize__serialize)(jz_pc_w *w, const jazz_model_JoinVia *v) {
    jz_pc_str(w, &v->table);
    jz_pc_str(w, &v->on_column);
    jz_pc_variant(w, v->target);
    jazz_model__OptStr__serialize_pc(w, &v->source_column);
    jz_pc_option(w, v->source_lookup != NULL);
    if (v->source_lookup) Q(JoinSourceLookup__Serialize__serialize)(w, v->source_lookup);
    PC_VEC_W(w, v->correlated_filters, jazz_model_JoinCorrelation, JoinCorrelation);
    PC_VEC_W(w, v->filters, jazz_model_Predicate, Predicate);
    PC_VEC_W(w, v->nested_joins, jazz_model_JoinVia, JoinVia);
}
jz_err Q(JoinVia__Deserialize__deserialize)(jz_pc_r *r, jazz_model_JoinVia *out) {
    bool some;
    JZ_TRY(jz_pc_r_enter(r));
    JZ_TRY(jz_pc_r_str(r, &out->table));
    JZ_TRY(jz_pc_r_str(r, &out->on_column));
    JZ_TRY(r_unit_variant(r, 3, &out->target));
    JZ_TRY(jazz_model__OptStr__deserialize_pc(r, &out->source_column));
    JZ_TRY(jz_pc_r_option(r, &some));
    if (some) {
        out->source_lookup = JZ_NEW(jazz_model_JoinSourceLookup);
        JZ_TRY(Q(JoinSourceLookup__Deserialize__deserialize)(r, out->source_lookup));
    }
    PC_VEC_R(r, out->correlated_filters, JoinCorrelation);
    PC_VEC_R(r, out->filters, Predicate);
    PC_VEC_R(r, out->nested_joins, JoinVia);
    jz_pc_r_leave(r);
    return JZ_OK;
}
void Q(JoinVia__Serialize__serialize_json)(jz_json_w *w, const jazz_model_JoinVia *v) {
    jz_jw_begin_obj(w);
    JKEY_STR(w, "table", v->table);
    JKEY_STR(w, "on_column", v->on_column);
    jz_jw_key(w, "target"); jazz_model__json_w_cstr(w, JOIN_TARGET_NAMES[v->target]);
    JKEY_OSTR(w, "source_column", v->source_column);
    jz_jw_key(w, "source_lookup");
    if (v->source_lookup) Q(JoinSourceLookup__Serialize__serialize_json)(w, v->source_lookup); else jz_jw_null(w);
    jz_jw_key(w, "correlated_filters"); JSON_VEC(w, v->correlated_filters, jazz_model_JoinCorrelation, JoinCorrelation);
    jz_jw_key(w, "filters"); JSON_VEC(w, v->filters, jazz_model_Predicate, Predicate);
    jz_jw_key(w, "nested_joins"); JSON_VEC(w, v->nested_joins, jazz_model_JoinVia, JoinVia);
    jz_jw_end_obj(w);
}

/* Calls `f` on each top-level filter of this join and its nested joins. */
void Q(JoinVia__for_each_condition_mut)(jazz_model_JoinVia *j, jazz_model_PredicateMutFn f, void *ctx) {
    JZ_VEC_FOR(jazz_model_Predicate, p, j->filters) f(p, ctx);
    JZ_VEC_FOR(jazz_model_JoinVia, n, j->nested_joins) Q(JoinVia__for_each_condition_mut)(n, f, ctx);
}

/* ================================================================ RecursionBound */
jazz_model_RecursionBound Q(RecursionBound__default_max_depth)(void) {
    return (jazz_model_RecursionBound){JAZZ_BOUND_MAX_DEPTH, 8};
}
u64 Q(RecursionBound__depth_steps)(jazz_model_RecursionBound b) {
    return b.tag == JAZZ_BOUND_FIXPOINT ? 128 : b.max_depth;
}
void Q(RecursionBound__Serialize__serialize)(jz_pc_w *w, const jazz_model_RecursionBound *v) {
    jz_pc_variant(w, v->tag);
    if (v->tag == JAZZ_BOUND_MAX_DEPTH) jz_pc_varint(w, v->max_depth);
}
jz_err Q(RecursionBound__Deserialize__deserialize)(jz_pc_r *r, jazz_model_RecursionBound *out) {
    JZ_TRY(r_unit_variant(r, 2, &out->tag));
    if (out->tag == JAZZ_BOUND_MAX_DEPTH) return jz_pc_r_varint(r, &out->max_depth);
    return JZ_OK;
}
void Q(RecursionBound__Serialize__serialize_json)(jz_json_w *w, const jazz_model_RecursionBound *v) {
    if (v->tag == JAZZ_BOUND_FIXPOINT) { jazz_model__json_w_cstr(w, "Fixpoint"); return; }
    jz_jw_begin_obj(w);
    jz_jw_key(w, "MaxDepth");
    jz_jw_u64(w, v->max_depth);
    jz_jw_end_obj(w);
}

/* ================================================================ ReachableSeed / ReachableVia */
void Q(ReachableSeed__drop)(jazz_model_ReachableSeed *v) {
    jz_str_drop(&v->table);
    jazz_model__OptStr__drop(&v->user_column);
    jazz_model__OptStr__drop(&v->user_claim);
    jz_str_drop(&v->team_column);
    VDROP(v->filters, Predicate);
}
void Q(ReachableSeed__Clone__clone)(jazz_model_ReachableSeed *d, const jazz_model_ReachableSeed *s) {
    memset(d, 0, sizeof(*d));
    d->table = jz_str_clone(&s->table);
    d->user_column = OSTR_CLONE(s->user_column);
    d->user_claim = OSTR_CLONE(s->user_claim);
    d->team_column = jz_str_clone(&s->team_column);
    VCLONE(d->filters, s->filters, Predicate);
}
bool Q(ReachableSeed__PartialEq__eq)(const jazz_model_ReachableSeed *a, const jazz_model_ReachableSeed *b) {
    return STR_EQ(a->table, b->table) && OSTR_EQ(a->user_column, b->user_column) && OSTR_EQ(a->user_claim, b->user_claim) &&
           STR_EQ(a->team_column, b->team_column) && VEQ(a->filters, b->filters, Predicate);
}
V_WRAP(ReachableSeed, jazz_model_ReachableSeed)
void Q(ReachableSeed__Serialize__serialize)(jz_pc_w *w, const jazz_model_ReachableSeed *v) {
    jz_pc_str(w, &v->table);
    jazz_model__OptStr__serialize_pc(w, &v->user_column);
    jazz_model__OptStr__serialize_pc(w, &v->user_claim);
    jz_pc_str(w, &v->team_column);
    PC_VEC_W(w, v->filters, jazz_model_Predicate, Predicate);
}
jz_err Q(ReachableSeed__Deserialize__deserialize)(jz_pc_r *r, jazz_model_ReachableSeed *out) {
    JZ_TRY(jz_pc_r_str(r, &out->table));
    JZ_TRY(jazz_model__OptStr__deserialize_pc(r, &out->user_column));
    JZ_TRY(jazz_model__OptStr__deserialize_pc(r, &out->user_claim));
    JZ_TRY(jz_pc_r_str(r, &out->team_column));
    PC_VEC_R(r, out->filters, Predicate);
    return JZ_OK;
}
void Q(ReachableSeed__Serialize__serialize_json)(jz_json_w *w, const jazz_model_ReachableSeed *v) {
    jz_jw_begin_obj(w);
    JKEY_STR(w, "table", v->table);
    JKEY_OSTR(w, "user_column", v->user_column);
    JKEY_OSTR(w, "user_claim", v->user_claim);
    JKEY_STR(w, "team_column", v->team_column);
    jz_jw_key(w, "filters"); JSON_VEC(w, v->filters, jazz_model_Predicate, Predicate);
    jz_jw_end_obj(w);
}

void Q(ReachableVia__drop)(jazz_model_ReachableVia *v) {
    jz_str_drop(&v->access_table);
    jz_str_drop(&v->access_row_column);
    jz_str_drop(&v->access_team_column);
    Q(Operand__drop)(&v->from);
    VDROP(v->access_filters, Predicate);
    jz_str_drop(&v->edge_table);
    jz_str_drop(&v->edge_member_column);
    jz_str_drop(&v->edge_parent_column);
    VDROP(v->edge_filters, Predicate);
    if (v->seed) { Q(ReachableSeed__drop)(v->seed); jz_free(v->seed); v->seed = NULL; }
}
void Q(ReachableVia__Clone__clone)(jazz_model_ReachableVia *d, const jazz_model_ReachableVia *s) {
    memset(d, 0, sizeof(*d));
    d->access_table = jz_str_clone(&s->access_table);
    d->access_row_column = jz_str_clone(&s->access_row_column);
    d->access_team_column = jz_str_clone(&s->access_team_column);
    d->access_team_target = s->access_team_target;
    Q(Operand__Clone__clone)(&d->from, &s->from);
    VCLONE(d->access_filters, s->access_filters, Predicate);
    d->edge_table = jz_str_clone(&s->edge_table);
    d->edge_member_column = jz_str_clone(&s->edge_member_column);
    d->edge_parent_column = jz_str_clone(&s->edge_parent_column);
    VCLONE(d->edge_filters, s->edge_filters, Predicate);
    d->bound = s->bound;
    if (s->seed) { d->seed = JZ_NEW(jazz_model_ReachableSeed); Q(ReachableSeed__Clone__clone)(d->seed, s->seed); }
}
bool Q(ReachableVia__PartialEq__eq)(const jazz_model_ReachableVia *a, const jazz_model_ReachableVia *b) {
    if (!STR_EQ(a->access_table, b->access_table) || !STR_EQ(a->access_row_column, b->access_row_column) ||
        !STR_EQ(a->access_team_column, b->access_team_column) || a->access_team_target != b->access_team_target ||
        !Q(Operand__PartialEq__eq)(&a->from, &b->from) || !VEQ(a->access_filters, b->access_filters, Predicate) ||
        !STR_EQ(a->edge_table, b->edge_table) || !STR_EQ(a->edge_member_column, b->edge_member_column) ||
        !STR_EQ(a->edge_parent_column, b->edge_parent_column) || !VEQ(a->edge_filters, b->edge_filters, Predicate))
        return false;
    if (a->bound.tag != b->bound.tag || (a->bound.tag == JAZZ_BOUND_MAX_DEPTH && a->bound.max_depth != b->bound.max_depth))
        return false;
    if (!a->seed != !b->seed) return false;
    return !a->seed || Q(ReachableSeed__PartialEq__eq)(a->seed, b->seed);
}
V_WRAP(ReachableVia, jazz_model_ReachableVia)
void Q(ReachableVia__Serialize__serialize)(jz_pc_w *w, const jazz_model_ReachableVia *v) {
    jz_pc_str(w, &v->access_table);
    jz_pc_str(w, &v->access_row_column);
    jz_pc_str(w, &v->access_team_column);
    jz_pc_variant(w, v->access_team_target);
    Q(Operand__Serialize__serialize)(w, &v->from);
    PC_VEC_W(w, v->access_filters, jazz_model_Predicate, Predicate);
    jz_pc_str(w, &v->edge_table);
    jz_pc_str(w, &v->edge_member_column);
    jz_pc_str(w, &v->edge_parent_column);
    PC_VEC_W(w, v->edge_filters, jazz_model_Predicate, Predicate);
    Q(RecursionBound__Serialize__serialize)(w, &v->bound);
    jz_pc_option(w, v->seed != NULL);
    if (v->seed) Q(ReachableSeed__Serialize__serialize)(w, v->seed);
}
jz_err Q(ReachableVia__Deserialize__deserialize)(jz_pc_r *r, jazz_model_ReachableVia *out) {
    bool some;
    JZ_TRY(jz_pc_r_str(r, &out->access_table));
    JZ_TRY(jz_pc_r_str(r, &out->access_row_column));
    JZ_TRY(jz_pc_r_str(r, &out->access_team_column));
    JZ_TRY(r_unit_variant(r, 3, &out->access_team_target));
    JZ_TRY(Q(Operand__Deserialize__deserialize)(r, &out->from));
    PC_VEC_R(r, out->access_filters, Predicate);
    JZ_TRY(jz_pc_r_str(r, &out->edge_table));
    JZ_TRY(jz_pc_r_str(r, &out->edge_member_column));
    JZ_TRY(jz_pc_r_str(r, &out->edge_parent_column));
    PC_VEC_R(r, out->edge_filters, Predicate);
    JZ_TRY(Q(RecursionBound__Deserialize__deserialize)(r, &out->bound));
    JZ_TRY(jz_pc_r_option(r, &some));
    if (some) {
        out->seed = JZ_NEW(jazz_model_ReachableSeed);
        JZ_TRY(Q(ReachableSeed__Deserialize__deserialize)(r, out->seed));
    }
    return JZ_OK;
}
void Q(ReachableVia__Serialize__serialize_json)(jz_json_w *w, const jazz_model_ReachableVia *v) {
    jz_jw_begin_obj(w);
    JKEY_STR(w, "access_table", v->access_table);
    JKEY_STR(w, "access_row_column", v->access_row_column);
    JKEY_STR(w, "access_team_column", v->access_team_column);
    jz_jw_key(w, "access_team_target"); jazz_model__json_w_cstr(w, JOIN_TARGET_NAMES[v->access_team_target]);
    jz_jw_key(w, "from"); Q(Operand__Serialize__serialize_json)(w, &v->from);
    jz_jw_key(w, "access_filters"); JSON_VEC(w, v->access_filters, jazz_model_Predicate, Predicate);
    JKEY_STR(w, "edge_table", v->edge_table);
    JKEY_STR(w, "edge_member_column", v->edge_member_column);
    JKEY_STR(w, "edge_parent_column", v->edge_parent_column);
    jz_jw_key(w, "edge_filters"); JSON_VEC(w, v->edge_filters, jazz_model_Predicate, Predicate);
    jz_jw_key(w, "bound"); Q(RecursionBound__Serialize__serialize_json)(w, &v->bound);
    jz_jw_key(w, "seed");
    if (v->seed) Q(ReachableSeed__Serialize__serialize_json)(w, v->seed); else jz_jw_null(w);
    jz_jw_end_obj(w);
}

/* ================================================================ InheritsVia */
void Q(InheritsVia__drop)(jazz_model_InheritsVia *v) { jz_str_drop(&v->parent_column); }
void Q(InheritsVia__Clone__clone)(jazz_model_InheritsVia *d, const jazz_model_InheritsVia *s) {
    *d = *s;
    d->parent_column = jz_str_clone(&s->parent_column);
}
bool Q(InheritsVia__PartialEq__eq)(const jazz_model_InheritsVia *a, const jazz_model_InheritsVia *b) {
    return STR_EQ(a->parent_column, b->parent_column) && a->operation == b->operation &&
           a->has_max_depth == b->has_max_depth && (!a->has_max_depth || a->max_depth == b->max_depth);
}
V_WRAP(InheritsVia, jazz_model_InheritsVia)
void Q(InheritsVia__Serialize__serialize)(jz_pc_w *w, const jazz_model_InheritsVia *v) {
    jz_pc_str(w, &v->parent_column);
    jz_pc_variant(w, v->operation);
    jazz_model__OptU64__serialize_pc(w, v->has_max_depth, v->max_depth);
}
jz_err Q(InheritsVia__Deserialize__deserialize)(jz_pc_r *r, jazz_model_InheritsVia *out) {
    JZ_TRY(jz_pc_r_str(r, &out->parent_column));
    JZ_TRY(r_unit_variant(r, 4, &out->operation));
    return jazz_model__OptU64__deserialize_pc(r, &out->has_max_depth, &out->max_depth);
}
void Q(InheritsVia__Serialize__serialize_json)(jz_json_w *w, const jazz_model_InheritsVia *v) {
    jz_jw_begin_obj(w);
    JKEY_STR(w, "parent_column", v->parent_column);
    jz_jw_key(w, "operation"); jazz_model__json_w_cstr(w, INHERITS_OPERATION_NAMES[v->operation]);
    jz_jw_key(w, "max_depth"); jazz_model__OptU64__serialize_json(w, v->has_max_depth, v->max_depth);
    jz_jw_end_obj(w);
}

/* ================================================================ PolicyBranch */
void Q(PolicyBranch__drop)(jazz_model_PolicyBranch *v) {
    VDROP(v->filters, Predicate);
    VDROP(v->joins, JoinVia);
    VDROP(v->reachable, ReachableVia);
    VDROP(v->inherits, InheritsVia);
}
void Q(PolicyBranch__Clone__clone)(jazz_model_PolicyBranch *d, const jazz_model_PolicyBranch *s) {
    VCLONE(d->filters, s->filters, Predicate);
    VCLONE(d->joins, s->joins, JoinVia);
    VCLONE(d->reachable, s->reachable, ReachableVia);
    VCLONE(d->inherits, s->inherits, InheritsVia);
}
bool Q(PolicyBranch__PartialEq__eq)(const jazz_model_PolicyBranch *a, const jazz_model_PolicyBranch *b) {
    return VEQ(a->filters, b->filters, Predicate) && VEQ(a->joins, b->joins, JoinVia) &&
           VEQ(a->reachable, b->reachable, ReachableVia) && VEQ(a->inherits, b->inherits, InheritsVia);
}
V_WRAP(PolicyBranch, jazz_model_PolicyBranch)
void Q(PolicyBranch__Serialize__serialize)(jz_pc_w *w, const jazz_model_PolicyBranch *v) {
    PC_VEC_W(w, v->filters, jazz_model_Predicate, Predicate);
    PC_VEC_W(w, v->joins, jazz_model_JoinVia, JoinVia);
    PC_VEC_W(w, v->reachable, jazz_model_ReachableVia, ReachableVia);
    PC_VEC_W(w, v->inherits, jazz_model_InheritsVia, InheritsVia);
}
jz_err Q(PolicyBranch__Deserialize__deserialize)(jz_pc_r *r, jazz_model_PolicyBranch *out) {
    PC_VEC_R(r, out->filters, Predicate);
    PC_VEC_R(r, out->joins, JoinVia);
    PC_VEC_R(r, out->reachable, ReachableVia);
    PC_VEC_R(r, out->inherits, InheritsVia);
    return JZ_OK;
}
void Q(PolicyBranch__Serialize__serialize_json)(jz_json_w *w, const jazz_model_PolicyBranch *v) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "filters"); JSON_VEC(w, v->filters, jazz_model_Predicate, Predicate);
    jz_jw_key(w, "joins"); JSON_VEC(w, v->joins, jazz_model_JoinVia, JoinVia);
    jz_jw_key(w, "reachable"); JSON_VEC(w, v->reachable, jazz_model_ReachableVia, ReachableVia);
    jz_jw_key(w, "inherits"); JSON_VEC(w, v->inherits, jazz_model_InheritsVia, InheritsVia);
    jz_jw_end_obj(w);
}

/* Convert a policy query into all policy-only alternatives it represents.
 * `Predicate::Any(vec![])` alone is the converter's explicit constant-false base. */
void Q(PolicyBranch__alternatives_from_query)(jazz_model_Query query, jazz_model_PolicyBranchVec *out) {
    bool base_is_converter_false = query.filters.len == 1 && query.filters.ptr[0].tag == JAZZ_PRED_ANY &&
                                   query.filters.ptr[0].children.len == 0 && query.joins.len == 0 &&
                                   query.reachable.len == 0 && query.inherits.len == 0;
    memset(out, 0, sizeof(*out));
    if (!base_is_converter_false) {
        jazz_model_PolicyBranch *b = JAZZ_VEC_SLOT(*out);
        b->filters = query.filters;
        b->joins = query.joins;
        b->reachable = query.reachable;
        b->inherits = query.inherits;
        memset(&query.filters, 0, sizeof(query.filters));
        memset(&query.joins, 0, sizeof(query.joins));
        memset(&query.reachable, 0, sizeof(query.reachable));
        memset(&query.inherits, 0, sizeof(query.inherits));
    }
    jz_vec_extend(out, query.policy_branches.ptr, query.policy_branches.len, sizeof(jazz_model_PolicyBranch));
    jz_free(query.policy_branches.ptr);
    memset(&query.policy_branches, 0, sizeof(query.policy_branches));
    Q(Query__drop)(&query); /* `..` drops the remaining fields */
}

/* ================================================================ for_each_condition */
static void for_each_condition_in(jazz_model_PredicateVec *filters, jazz_model_JoinViaVec *joins,
                                  jazz_model_ReachableViaVec *reachable, jazz_model_PredicateMutFn f, void *ctx) {
    JZ_VEC_FOR(jazz_model_Predicate, p, *filters) f(p, ctx);
    JZ_VEC_FOR(jazz_model_JoinVia, j, *joins) Q(JoinVia__for_each_condition_mut)(j, f, ctx);
    JZ_VEC_FOR(jazz_model_ReachableVia, rv, *reachable) {
        JZ_VEC_FOR(jazz_model_Predicate, p, rv->access_filters) f(p, ctx);
        JZ_VEC_FOR(jazz_model_Predicate, p, rv->edge_filters) f(p, ctx);
        if (rv->seed) JZ_VEC_FOR(jazz_model_Predicate, p, rv->seed->filters) f(p, ctx);
    }
}
void Q(Query__for_each_condition_mut)(jazz_model_Query *q, jazz_model_PredicateMutFn f, void *ctx) {
    for_each_condition_in(&q->filters, &q->joins, &q->reachable, f, ctx);
    JZ_VEC_FOR(jazz_model_PolicyBranch, b, q->policy_branches) for_each_condition_in(&b->filters, &b->joins, &b->reachable, f, ctx);
}

/* ================================================================ ShapeId / BindingId Debug */
static void debug_uuid_newtype(jz_str *out, const char *name, const jz_uuid *id) {
    char buf[37];
    jz_uuid_to_string(id, buf);
    jz_str_appendf(out, "%s(%s)", name, buf);
}
void Q(ShapeId__Debug__fmt)(jz_str *out, const jazz_model_ShapeId *id) { debug_uuid_newtype(out, "ShapeId", &id->id); }
void Q(BindingId__Debug__fmt)(jz_str *out, const jazz_model_BindingId *id) { debug_uuid_newtype(out, "BindingId", &id->id); }

/* ================================================================ provider-claim keys */
#define PROVIDER_CLAIM_PATH_V1 "\0claim-path-v1:"
#define PROVIDER_CLAIM_PATH_V1_LEN 15u

jz_str Q(provider_claim_operand_key)(jz_slice name) {
    jz_str key = {0};
    jz_str_push(&key, JAZZ_PROVIDER_CLAIM_PREFIX, JAZZ_PROVIDER_CLAIM_PREFIX_LEN);
    jz_str_push(&key, (const char *)name.ptr, name.len);
    return key;
}
jz_str Q(provider_claim_key)(jz_slice name) { return Q(provider_claim_operand_key)(name); }

jz_str Q(provider_claim_path_operand_key)(const jz_str *segments, u32 n) {
    if (n == 1) return Q(provider_claim_operand_key)(jz_str_slice(&segments[0]));
    jz_str key = {0};
    jz_str_push(&key, PROVIDER_CLAIM_PATH_V1, PROVIDER_CLAIM_PATH_V1_LEN);
    for (u32 i = 0; i < n; i++) jz_str_appendf(&key, "%u:%.*s", segments[i].len, (int)segments[i].len, segments[i].ptr);
    return key;
}

static void split_into(jz_slice name, char sep, jazz_model_StrVec *out) {
    u32 start = 0;
    for (u32 i = 0; i <= name.len; i++) {
        if (i == name.len || name.ptr[i] == (u8)sep) {
            JZ_VEC_PUSH(*out, jz_str_from_slice(jz_slice_of(name.ptr + start, i - start)));
            start = i + 1;
        }
    }
}

void Q(operand_claim_path)(jz_slice name, jazz_model_StrVec *out) {
    memset(out, 0, sizeof(*out));
    if (name.len >= PROVIDER_CLAIM_PATH_V1_LEN && memcmp(name.ptr, PROVIDER_CLAIM_PATH_V1, PROVIDER_CLAIM_PATH_V1_LEN) == 0) {
        jz_slice rest = jz_slice_of(name.ptr + PROVIDER_CLAIM_PATH_V1_LEN, name.len - PROVIDER_CLAIM_PATH_V1_LEN);
        jazz_model_StrVec segments = {0};
        JZ_VEC_PUSH(segments, jz_str_from("claims"));
        while (rest.len) {
            u32 colon = 0;
            while (colon < rest.len && rest.ptr[colon] != ':') colon++;
            if (colon == rest.len || colon == 0) break;
            u64 length = 0;
            bool ok = true;
            for (u32 i = 0; i < colon; i++) {
                if (rest.ptr[i] < '0' || rest.ptr[i] > '9' || length > (UINT32_MAX / 10)) { ok = false; break; }
                length = length * 10 + (u64)(rest.ptr[i] - '0');
            }
            if (!ok) break;
            jz_slice tail = jz_slice_of(rest.ptr + colon + 1, rest.len - colon - 1);
            if (length > tail.len) break;
            /* tail.get(..length) also rejects a non-char-boundary split */
            if (length < tail.len && (tail.ptr[length] & 0xC0) == 0x80) break;
            JZ_VEC_PUSH(segments, jz_str_from_slice(jz_slice_of(tail.ptr, (u32)length)));
            rest = jz_slice_of(tail.ptr + length, tail.len - (u32)length);
        }
        if (rest.len == 0 && segments.len > 2) {
            jz_str roundtrip = Q(provider_claim_path_operand_key)(segments.ptr + 1, segments.len - 1);
            bool same = jazz_model__slice_eq(jz_str_slice(&roundtrip), name);
            jz_str_drop(&roundtrip);
            if (same) { *out = segments; return; }
        }
        jazz_model__StrVec__drop(&segments);
    }
    if (jazz_model__slice_eq_cstr(name, "user.account") || jazz_model__slice_eq_cstr(name, "user.identity") ||
        jazz_model__slice_eq_cstr(name, "user.identity.issuer") || jazz_model__slice_eq_cstr(name, "user.identity.subject")) {
        split_into(name, '.', out);
        return;
    }
    if (name.len >= JAZZ_PROVIDER_CLAIM_PREFIX_LEN && memcmp(name.ptr, JAZZ_PROVIDER_CLAIM_PREFIX, JAZZ_PROVIDER_CLAIM_PREFIX_LEN) == 0) {
        JZ_VEC_PUSH(*out, jz_str_from("claims"));
        JZ_VEC_PUSH(*out, jz_str_from_slice(jz_slice_of(name.ptr + JAZZ_PROVIDER_CLAIM_PREFIX_LEN, name.len - JAZZ_PROVIDER_CLAIM_PREFIX_LEN)));
        return;
    }
    JZ_VEC_PUSH(*out, jz_str_from_slice(name));
}

/* Recognize the finite structured author vocabulary without interpreting
 * provider claim names as paths. */
bool Q(author_claim_path_key)(const jz_str *path, u32 n, jz_str *out) {
    jz_str name = {0};
    for (u32 i = 0; i < n; i++) {
        if (i) jz_str_push_char(&name, '.');
        jz_str_push(&name, path[i].ptr, path[i].len);
    }
    bool valid = jz_str_eq_cstr(&name, "user") || jz_str_eq_cstr(&name, "user.account") ||
                 jz_str_eq_cstr(&name, "user.identity") || jz_str_eq_cstr(&name, "user.identity.issuer") ||
                 jz_str_eq_cstr(&name, "user.identity.subject");
    for (u32 i = 0; valid && i < n; i++)
        if (memchr(path[i].ptr, '.', path[i].len)) valid = false;
    if (!valid) { jz_str_drop(&name); return false; }
    *out = name;
    return true;
}
