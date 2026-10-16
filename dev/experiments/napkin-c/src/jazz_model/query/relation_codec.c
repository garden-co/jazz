/* query/relation_codec.rs: typed Postcard representation for relation queries.
 *
 * Napkin mapping: Rust converts RelationExpr <-> WireRelationExpr (an
 * isomorphic tree whose literals are WireJson and whose dimensions are u32)
 * and then (de)serializes the wire tree. The C port fuses the conversion into
 * the Postcard writer/reader, so the wire grammar, budgets, validation passes
 * and error strings are kept but the intermediate copy is not materialized. */
#include <string.h>
#include "jazz_model/jazz_model.h"

#define Q(x) jazz_model__query__##x

#define MAX_RELATION_BYTES (1u << 20)
#define MAX_RELATION_ITEMS 4096u
#define MAX_RELATION_STRING_BYTES (1u << 16)

/* ---------------------------------------------------------------- RelationWireError */
static jz_err wire_too_large(void) { return jz_err_new(JAZZ_MODEL_ERR_RELATION_WIRE_TOO_LARGE, "relation query is too large"); }
static jz_err wire_non_finite(void) {
    return jz_err_new(JAZZ_MODEL_ERR_RELATION_WIRE_NON_FINITE, "relation query contains a non-finite JSON number");
}
static jz_err wire_invalid(const char *what) { return jz_err_fmt(JAZZ_MODEL_ERR_RELATION_WIRE_INVALID, "relation query %s", what); }
static jz_err wire_postcard(jz_err e) {
    jz_err out = jz_err_fmt(JAZZ_MODEL_ERR_RELATION_WIRE_POSTCARD, "malformed Postcard relation query: %.*s",
                            e.msg ? (int)e.msg->len : 0, e.msg ? e.msg->ptr : "");
    jz_err_drop(&e);
    return out;
}
/* serde::{ser,de}::Error::custom(RelationWireError) */
static jz_err wire_custom(jz_err e) {
    if (!e.code) return e;
    jz_err out = jazz_model__serde_err("%.*s", e.msg ? (int)e.msg->len : 0, e.msg ? e.msg->ptr : "");
    jz_err_drop(&e);
    return out;
}
static jz_err de_custom(const char *msg) { return jazz_model__serde_err("%s", msg); }

/* ================================================================ validate_wire */
typedef struct Validator { u64 nodes; u64 strings; } Validator;

static jz_err v_node(Validator *v) {
    v->nodes += 1;
    if (v->nodes > MAX_RELATION_ITEMS) return wire_invalid("node count exceeds limit");
    return JZ_OK;
}
static jz_err v_collection(u32 len) {
    return len > MAX_RELATION_ITEMS ? wire_invalid("collection exceeds limit") : JZ_OK;
}
static jz_err v_text(Validator *v, const jz_str *s) {
    if (s->len > MAX_RELATION_STRING_BYTES) return wire_invalid("string exceeds limit");
    v->strings += s->len;
    return v->strings > MAX_RELATION_BYTES ? wire_too_large() : JZ_OK;
}
static jz_err v_column(Validator *v, const jazz_model_RelationColumnRef *c) {
    if (c->scope.some) JZ_TRY(v_text(v, &c->scope.s));
    return v_text(v, &c->column);
}
static jz_err v_key(Validator *v, const jazz_model_RelationKeyRef *k) {
    JZ_TRY(v_node(v));
    return k->tag == JAZZ_KEYREF_COLUMN ? v_column(v, &k->column) : JZ_OK;
}
static jz_err v_json(Validator *v, const jz_json *j, u32 depth) {
    if (depth > JAZZ_MAX_RELATION_DEPTH) return wire_invalid("depth exceeds limit");
    JZ_TRY(v_node(v));
    switch (j->kind) {
    case JZ_JSON_F64:
        if (!__builtin_isfinite(j->v.f)) return wire_non_finite();
        return JZ_OK;
    case JZ_JSON_STR: return v_text(v, &j->v.s);
    case JZ_JSON_ARR:
        JZ_TRY(v_collection(j->v.arr.len));
        for (u32 i = 0; i < j->v.arr.len; i++) JZ_TRY(v_json(v, j->v.arr.ptr[i], depth + 1));
        return JZ_OK;
    case JZ_JSON_OBJ: {
        /* serde_json::Map iterates in key order, so keys are canonical by construction */
        JZ_TRY(v_collection(j->v.obj.len));
        jz_err e = JZ_OK;
        jz_str *key; jz_json **val;
        JZ_BTREE_FOR(&j->v.obj, key, val) {
            e = v_text(v, key);
            if (!e.code) e = v_json(v, *val, depth + 1);
            if (e.code) break;
        }
        return e;
    }
    default: return JZ_OK;
    }
}
static jz_err v_value(Validator *v, const jazz_model_RelationValueRef *val, u32 depth) {
    JZ_TRY(v_node(v));
    switch (val->tag) {
    case JAZZ_RVAL_LITERAL: return v_json(v, val->literal, depth + 1);
    case JAZZ_RVAL_PARAM: return v_text(v, &val->param);
    case JAZZ_RVAL_SESSION_REF:
        JZ_TRY(v_collection(val->session_ref.len));
        JZ_VEC_FOR(const jz_str, s, val->session_ref) JZ_TRY(v_text(v, s));
        return JZ_OK;
    case JAZZ_RVAL_OUTER_COLUMN:
    case JAZZ_RVAL_FRONTIER_COLUMN: return v_column(v, &val->column);
    default: return JZ_OK;
    }
}
static jz_err v_predicate(Validator *v, const jazz_model_RelationPredicate *p, u32 depth) {
    if (depth > JAZZ_MAX_RELATION_DEPTH) return wire_invalid("depth exceeds limit");
    JZ_TRY(v_node(v));
    switch (p->tag) {
    case JAZZ_RPE_CMP:
    case JAZZ_RPE_CONTAINS:
        JZ_TRY(v_column(v, &p->left));
        return v_value(v, &p->right, depth + 1);
    case JAZZ_RPE_IS_NULL:
    case JAZZ_RPE_IS_NOT_NULL: return v_column(v, &p->left);
    case JAZZ_RPE_IN:
        JZ_TRY(v_column(v, &p->left));
        JZ_TRY(v_collection(p->values.len));
        JZ_VEC_FOR(const jazz_model_RelationValueRef, x, p->values) JZ_TRY(v_value(v, x, depth + 1));
        return JZ_OK;
    case JAZZ_RPE_ENUM_MATCH:
        JZ_TRY(v_column(v, &p->left));
        JZ_TRY(v_text(v, &p->case_));
        return v_predicate(v, p->inner, depth + 1);
    case JAZZ_RPE_AND:
    case JAZZ_RPE_OR:
        JZ_TRY(v_collection(p->items.len));
        JZ_VEC_FOR(const jazz_model_RelationPredicate, c, p->items) JZ_TRY(v_predicate(v, c, depth + 1));
        return JZ_OK;
    case JAZZ_RPE_NOT: return v_predicate(v, p->inner, depth + 1);
    default: return JZ_OK;
    }
}
static jz_err v_expr(Validator *v, const jazz_model_RelationExpr *e, u32 depth) {
    if (depth > JAZZ_MAX_RELATION_DEPTH) return wire_invalid("depth exceeds limit");
    JZ_TRY(v_node(v));
    switch (e->tag) {
    case JAZZ_REXPR_TABLE_SCAN:
        JZ_TRY(v_text(v, &e->table));
        if (e->alias.some) JZ_TRY(v_text(v, &e->alias.s));
        return JZ_OK;
    case JAZZ_REXPR_FILTER:
        JZ_TRY(v_expr(v, e->input, depth + 1));
        return v_predicate(v, &e->predicate, depth + 1);
    case JAZZ_REXPR_UNION: {
        JZ_TRY(v_collection(e->inputs.len));
        jz_btree labels;
        jz_btree_init(&labels, &jazz_model__STRPTR_SET_DESC);
        jz_err err = JZ_OK;
        JZ_VEC_FOR(const jazz_model_RelationUnionArm, arm, e->inputs) {
            const jz_str *label = &arm->label;
            err = v_text(v, label);
            if (err.code) break;
            if (label->len == 0 || label->len > 4096 || memchr(label->ptr, '\0', label->len) ||
                jz_btree_insert(&labels, &label, NULL, NULL)) {
                err = wire_invalid("invalid union label");
                break;
            }
            err = v_expr(v, &arm->input, depth + 1);
            if (err.code) break;
        }
        jz_btree_drop(&labels);
        return err;
    }
    case JAZZ_REXPR_JOIN:
        JZ_TRY(v_expr(v, e->input, depth + 1));
        JZ_TRY(v_expr(v, e->right, depth + 1));
        JZ_TRY(v_collection(e->on.len));
        JZ_VEC_FOR(const jazz_model_RelationJoinCondition, c, e->on) {
            JZ_TRY(v_node(v));
            JZ_TRY(v_column(v, &c->left));
            JZ_TRY(v_column(v, &c->right));
        }
        return JZ_OK;
    case JAZZ_REXPR_PROJECT:
        JZ_TRY(v_expr(v, e->input, depth + 1));
        JZ_TRY(v_collection(e->columns.len));
        JZ_VEC_FOR(const jazz_model_RelationProjectColumn, c, e->columns) {
            JZ_TRY(v_node(v));
            JZ_TRY(v_text(v, &c->alias));
            if (c->expr.tag == JAZZ_KEYREF_COLUMN) JZ_TRY(v_column(v, &c->expr.column));
        }
        return JZ_OK;
    case JAZZ_REXPR_GATHER:
        JZ_TRY(v_expr(v, e->input, depth + 1));
        JZ_TRY(v_expr(v, e->right, depth + 1));
        JZ_TRY(v_key(v, &e->frontier_key));
        JZ_TRY(v_collection(e->keys.len));
        JZ_VEC_FOR(const jazz_model_RelationKeyRef, k, e->keys) JZ_TRY(v_key(v, k));
        return JZ_OK;
    case JAZZ_REXPR_DISTINCT:
        JZ_TRY(v_expr(v, e->input, depth + 1));
        JZ_TRY(v_collection(e->keys.len));
        JZ_VEC_FOR(const jazz_model_RelationKeyRef, k, e->keys) JZ_TRY(v_key(v, k));
        return JZ_OK;
    case JAZZ_REXPR_ORDER_BY:
        JZ_TRY(v_expr(v, e->input, depth + 1));
        JZ_TRY(v_collection(e->terms.len));
        JZ_VEC_FOR(const jazz_model_RelationOrderBy, t, e->terms) {
            JZ_TRY(v_node(v));
            JZ_TRY(v_column(v, &t->column));
        }
        return JZ_OK;
    default: return v_expr(v, e->input, depth + 1);
    }
}
static jz_err validate_wire(const jazz_model_RelationQuery *q) {
    Validator v = {0, 0};
    return v_expr(&v, &q->rel, 0);
}

/* ================================================================ wire_* + Serialize (fused) */
static void w_column(jz_pc_w *w, const jazz_model_RelationColumnRef *c) {
    jazz_model__OptStr__serialize_pc(w, &c->scope);
    jz_pc_str(w, &c->column);
}
static void w_key(jz_pc_w *w, const jazz_model_RelationKeyRef *k) {
    jz_pc_variant(w, k->tag);
    if (k->tag == JAZZ_KEYREF_COLUMN) w_column(w, &k->column);
    else jz_pc_variant(w, k->row_id);
}
/* fn wire_json + WireJson::serialize */
static jz_err w_json(jz_pc_w *w, const jz_json *j) {
    enum { WJ_NULL, WJ_BOOL, WJ_I64, WJ_U64, WJ_F64, WJ_STRING, WJ_ARRAY, WJ_OBJECT };
    switch (j->kind) {
    case JZ_JSON_NULL: jz_pc_variant(w, WJ_NULL); return JZ_OK;
    case JZ_JSON_BOOL: jz_pc_variant(w, WJ_BOOL); jz_pc_bool(w, j->v.b); return JZ_OK;
    case JZ_JSON_I64: jz_pc_variant(w, WJ_I64); jz_pc_zigzag(w, j->v.i); return JZ_OK;
    case JZ_JSON_U64:
        if (j->v.u <= (u64)INT64_MAX) { jz_pc_variant(w, WJ_I64); jz_pc_zigzag(w, (i64)j->v.u); }
        else { jz_pc_variant(w, WJ_U64); jz_pc_varint(w, j->v.u); }
        return JZ_OK;
    case JZ_JSON_F64: {
        if (!__builtin_isfinite(j->v.f)) return wire_non_finite();
        u64 bits;
        memcpy(&bits, &j->v.f, 8);
        jz_pc_variant(w, WJ_F64);
        jz_pc_varint(w, bits);
        return JZ_OK;
    }
    case JZ_JSON_STR: jz_pc_variant(w, WJ_STRING); jz_pc_str(w, &j->v.s); return JZ_OK;
    case JZ_JSON_ARR:
        jz_pc_variant(w, WJ_ARRAY);
        jz_pc_seq(w, j->v.arr.len);
        for (u32 i = 0; i < j->v.arr.len; i++) JZ_TRY(w_json(w, j->v.arr.ptr[i]));
        return JZ_OK;
    default: {
        jz_err e = JZ_OK;
        jz_pc_variant(w, WJ_OBJECT);
        jz_pc_seq(w, j->v.obj.len);
        jz_str *key; jz_json **val;
        JZ_BTREE_FOR(&j->v.obj, key, val) {
            jz_pc_str(w, key);
            e = w_json(w, *val);
            if (e.code) break;
        }
        return e;
    }
    }
}
static jz_err w_value(jz_pc_w *w, const jazz_model_RelationValueRef *v) {
    jz_pc_variant(w, v->tag);
    switch (v->tag) {
    case JAZZ_RVAL_LITERAL: return w_json(w, v->literal);
    case JAZZ_RVAL_PARAM: jz_pc_str(w, &v->param); return JZ_OK;
    case JAZZ_RVAL_SESSION_REF: jazz_model__StrVec__serialize_pc(w, &v->session_ref); return JZ_OK;
    case JAZZ_RVAL_OUTER_COLUMN:
    case JAZZ_RVAL_FRONTIER_COLUMN: w_column(w, &v->column); return JZ_OK;
    default: jz_pc_variant(w, v->row_id); return JZ_OK;
    }
}
static jz_err w_predicate(jz_pc_w *w, const jazz_model_RelationPredicate *p) {
    jz_pc_variant(w, p->tag);
    switch (p->tag) {
    case JAZZ_RPE_CMP:
        w_column(w, &p->left);
        jz_pc_variant(w, p->op);
        return w_value(w, &p->right);
    case JAZZ_RPE_IS_NULL:
    case JAZZ_RPE_IS_NOT_NULL: w_column(w, &p->left); return JZ_OK;
    case JAZZ_RPE_IN:
        w_column(w, &p->left);
        jz_pc_seq(w, p->values.len);
        JZ_VEC_FOR(const jazz_model_RelationValueRef, x, p->values) JZ_TRY(w_value(w, x));
        return JZ_OK;
    case JAZZ_RPE_CONTAINS:
        w_column(w, &p->left);
        return w_value(w, &p->right);
    case JAZZ_RPE_ENUM_MATCH:
        w_column(w, &p->left);
        jz_pc_str(w, &p->case_);
        return w_predicate(w, p->inner);
    case JAZZ_RPE_AND:
    case JAZZ_RPE_OR:
        jz_pc_seq(w, p->items.len);
        JZ_VEC_FOR(const jazz_model_RelationPredicate, c, p->items) JZ_TRY(w_predicate(w, c));
        return JZ_OK;
    case JAZZ_RPE_NOT: return w_predicate(w, p->inner);
    default: return JZ_OK;
    }
}
/* fn u32_dimension */
static jz_err u32_dimension(u64 value) {
    return value > UINT32_MAX ? wire_invalid("dimension exceeds u32") : JZ_OK;
}
static jz_err w_expr(jz_pc_w *w, const jazz_model_RelationExpr *e) {
    jz_pc_variant(w, e->tag);
    switch (e->tag) {
    case JAZZ_REXPR_TABLE_SCAN:
        jz_pc_str(w, &e->table);
        jazz_model__OptStr__serialize_pc(w, &e->alias);
        return JZ_OK;
    case JAZZ_REXPR_FILTER:
        JZ_TRY(w_expr(w, e->input));
        return w_predicate(w, &e->predicate);
    case JAZZ_REXPR_UNION:
        jz_pc_seq(w, e->inputs.len);
        JZ_VEC_FOR(const jazz_model_RelationUnionArm, arm, e->inputs) {
            jz_pc_str(w, &arm->label);
            JZ_TRY(w_expr(w, &arm->input));
        }
        return JZ_OK;
    case JAZZ_REXPR_JOIN:
        JZ_TRY(w_expr(w, e->input));
        JZ_TRY(w_expr(w, e->right));
        jz_pc_seq(w, e->on.len);
        JZ_VEC_FOR(const jazz_model_RelationJoinCondition, c, e->on) {
            w_column(w, &c->left);
            w_column(w, &c->right);
        }
        jz_pc_variant(w, e->join_kind);
        return JZ_OK;
    case JAZZ_REXPR_PROJECT:
        JZ_TRY(w_expr(w, e->input));
        jz_pc_seq(w, e->columns.len);
        JZ_VEC_FOR(const jazz_model_RelationProjectColumn, c, e->columns) {
            jz_pc_str(w, &c->alias);
            w_key(w, &c->expr);
        }
        return JZ_OK;
    case JAZZ_REXPR_GATHER:
        JZ_TRY(w_expr(w, e->input));
        JZ_TRY(w_expr(w, e->right));
        w_key(w, &e->frontier_key);
        jz_pc_variant(w, e->bound.tag);
        if (e->bound.tag == JAZZ_BOUND_MAX_DEPTH) {
            JZ_TRY(u32_dimension(e->bound.max_depth));
            jz_pc_varint(w, e->bound.max_depth);
        }
        jz_pc_seq(w, e->keys.len);
        JZ_VEC_FOR(const jazz_model_RelationKeyRef, k, e->keys) w_key(w, k);
        return JZ_OK;
    case JAZZ_REXPR_DISTINCT:
        JZ_TRY(w_expr(w, e->input));
        jz_pc_seq(w, e->keys.len);
        JZ_VEC_FOR(const jazz_model_RelationKeyRef, k, e->keys) w_key(w, k);
        return JZ_OK;
    case JAZZ_REXPR_ORDER_BY:
        JZ_TRY(w_expr(w, e->input));
        jz_pc_seq(w, e->terms.len);
        JZ_VEC_FOR(const jazz_model_RelationOrderBy, t, e->terms) {
            w_column(w, &t->column);
            jz_pc_variant(w, t->direction);
        }
        return JZ_OK;
    default:
        JZ_TRY(w_expr(w, e->input));
        JZ_TRY(u32_dimension(e->n));
        jz_pc_varint(w, e->n);
        return JZ_OK;
    }
}

/* fn ensure_wire_size: postcard::experimental::serialized_size */
static jz_err ensure_wire_size(const jazz_model_RelationQuery *q) {
    jz_bytes tmp = {0};
    jz_pc_w w = {&tmp};
    jz_err e = w_expr(&w, &q->rel);
    u32 size = tmp.len;
    jz_bytes_drop(&tmp);
    JZ_TRY(e);
    return size > MAX_RELATION_BYTES ? wire_too_large() : JZ_OK;
}

/* relation_query_to_wire: convert, ensure_wire_size, validate_wire */
static jz_err relation_query_to_wire(const jazz_model_RelationQuery *q) {
    JZ_TRY(ensure_wire_size(q));
    return validate_wire(q);
}

/* ================================================================ Deserialize (budgeted, fused relation_*) */
/* thread_local DESERIALIZE_BUDGET: Option<DeserializeBudget> */
static struct { bool active; u64 depth, nodes, strings; } DESERIALIZE_BUDGET;

static jz_err budget_scope_enter(void) {
    if (DESERIALIZE_BUDGET.active) return de_custom("nested relation-query decoder");
    DESERIALIZE_BUDGET.active = true;
    DESERIALIZE_BUDGET.depth = DESERIALIZE_BUDGET.nodes = DESERIALIZE_BUDGET.strings = 0;
    return JZ_OK;
}
/* impl Drop for DeserializeBudgetScope */
static void budget_scope_drop(void) { DESERIALIZE_BUDGET.active = false; }

static jz_err budget_enter(void) {
    if (!DESERIALIZE_BUDGET.active) return de_custom("relation-query budget missing");
    DESERIALIZE_BUDGET.depth += 1;
    DESERIALIZE_BUDGET.nodes += 1;
    if (DESERIALIZE_BUDGET.depth > JAZZ_MAX_RELATION_DEPTH + 1 || DESERIALIZE_BUDGET.nodes > MAX_RELATION_ITEMS)
        return de_custom("relation-query tree limit");
    return JZ_OK;
}
static jz_err budget_node(void) {
    if (!DESERIALIZE_BUDGET.active) return de_custom("relation-query budget missing");
    DESERIALIZE_BUDGET.nodes += 1;
    return DESERIALIZE_BUDGET.nodes > MAX_RELATION_ITEMS ? de_custom("relation-query node limit") : JZ_OK;
}
static void budget_leave(void) {
    if (DESERIALIZE_BUDGET.active && DESERIALIZE_BUDGET.depth) DESERIALIZE_BUDGET.depth -= 1;
}
static jz_err budget_string(u32 len) {
    if (!DESERIALIZE_BUDGET.active) return de_custom("relation-query budget missing");
    DESERIALIZE_BUDGET.strings += len;
    if (len > MAX_RELATION_STRING_BYTES || DESERIALIZE_BUDGET.strings > MAX_RELATION_BYTES)
        return de_custom("relation-query string limit");
    return JZ_OK;
}
/* deserialize_bounded_string */
static jz_err r_bounded_string(jz_pc_r *r, jz_str *out) {
    JZ_TRY(jz_pc_r_str(r, out));
    return budget_string(out->len);
}
static jz_err r_bounded_option_string(jz_pc_r *r, jazz_model_OptStr *out) {
    JZ_TRY(jz_pc_r_option(r, &out->some));
    return out->some ? r_bounded_string(r, &out->s) : JZ_OK;
}
/* deserialize_bounded_vec: length header, then at most MAX_RELATION_ITEMS elements */
static jz_err r_bounded_len(jz_pc_r *r, u32 *n) {
    JZ_TRY(jz_pc_r_seq(r, n));
    return JZ_OK;
}
static jz_err bounded_push_check(u32 len) {
    return len == MAX_RELATION_ITEMS ? de_custom("relation-query collection limit") : JZ_OK;
}
static jz_err r_unit(jz_pc_r *r, u32 n, u8 *out) {
    u32 idx;
    JZ_TRY(jz_pc_r_variant(r, &idx, n));
    *out = (u8)idx;
    return JZ_OK;
}

static jz_err r_column(jz_pc_r *r, jazz_model_RelationColumnRef *c) {
    JZ_TRY(r_bounded_option_string(r, &c->scope));
    return r_bounded_string(r, &c->column);
}
static jz_err r_key(jz_pc_r *r, jazz_model_RelationKeyRef *k) {
    JZ_TRY(r_unit(r, 2, &k->tag));
    if (k->tag == JAZZ_KEYREF_COLUMN) return r_column(r, &k->column);
    return r_unit(r, 3, &k->row_id);
}
/* WireJson::deserialize + fn relation_json; also the canonical object-key check of validate_wire */
static jz_err r_json_inner(jz_pc_r *r, jz_json **out);
static jz_err r_json_nested(jz_pc_r *r, jz_json **out) {
    JZ_TRY(budget_enter());
    jz_err e = r_json_inner(r, out);
    budget_leave();
    return e;
}
static jz_err r_json_inner(jz_pc_r *r, jz_json **out) {
    u32 idx, n;
    JZ_TRY(jz_pc_r_variant(r, &idx, 8));
    switch (idx) {
    case 0: *out = jz_json_null(); return JZ_OK;
    case 1: { bool b; JZ_TRY(jz_pc_r_bool(r, &b)); *out = jz_json_bool(b); return JZ_OK; }
    case 2: { i64 i; JZ_TRY(jz_pc_r_zigzag(r, &i)); *out = jz_json_i64(i); return JZ_OK; }
    case 3: { u64 u; JZ_TRY(jz_pc_r_varint(r, &u)); *out = jz_json_u64(u); return JZ_OK; }
    case 4: {
        u64 bits;
        f64 f;
        JZ_TRY(jz_pc_r_varint(r, &bits));
        memcpy(&f, &bits, 8);
        if (!__builtin_isfinite(f)) return wire_non_finite();
        *out = jz_json_f64(f);
        return JZ_OK;
    }
    case 5: {
        jz_str s = {0};
        jz_err e = r_bounded_string(r, &s);
        if (e.code) { jz_str_drop(&s); return e; }
        *out = jz_json_str_owned(s);
        return JZ_OK;
    }
    case 6: {
        JZ_TRY(r_bounded_len(r, &n));
        jz_json *arr = jz_json_arr();
        *out = arr;
        for (u32 i = 0; i < n; i++) {
            jz_json *item = NULL;
            JZ_TRY(bounded_push_check(i));
            JZ_TRY(r_json_nested(r, &item));
            jz_json_arr_push(arr, item);
        }
        return JZ_OK;
    }
    default: {
        JZ_TRY(r_bounded_len(r, &n));
        jz_json *obj = jz_json_obj();
        jz_str previous = {0};
        bool has_previous = false;
        jz_err e = JZ_OK;
        *out = obj;
        for (u32 i = 0; i < n && !e.code; i++) {
            jz_str key = {0};
            jz_json *val = NULL;
            e = bounded_push_check(i);
            if (!e.code) e = r_bounded_string(r, &key);          /* "object key" */
            if (!e.code) e = r_json_nested(r, &val);             /* "object value" */
            if (!e.code && has_previous && jz_str_cmp(&previous, &key) >= 0) e = wire_invalid("object keys are not canonical");
            if (e.code) { jz_str_drop(&key); if (val) jz_json_free(val); break; }
            jz_str_drop(&previous);
            previous = jz_str_clone(&key);
            has_previous = true;
            jz_btree_insert(&obj->v.obj, &key, &val, NULL);
        }
        jz_str_drop(&previous);
        return e;
    }
    }
}
static jz_err r_value(jz_pc_r *r, jazz_model_RelationValueRef *v) {
    JZ_TRY(r_unit(r, 6, &v->tag));
    switch (v->tag) {
    case JAZZ_RVAL_LITERAL: return r_json_nested(r, &v->literal);
    case JAZZ_RVAL_PARAM: return r_bounded_string(r, &v->param);
    case JAZZ_RVAL_SESSION_REF: {
        u32 n;
        JZ_TRY(r_bounded_len(r, &n));
        for (u32 i = 0; i < n; i++) {
            JZ_TRY(bounded_push_check(v->session_ref.len));
            JZ_TRY(r_bounded_string(r, JAZZ_VEC_SLOT(v->session_ref)));
        }
        return JZ_OK;
    }
    case JAZZ_RVAL_OUTER_COLUMN:
    case JAZZ_RVAL_FRONTIER_COLUMN: return r_column(r, &v->column);
    default: return r_unit(r, 3, &v->row_id);
    }
}
static jz_err r_value_node(jz_pc_r *r, jazz_model_RelationValueRef *v) {
    JZ_TRY(budget_node());
    return r_value(r, v);
}
static jz_err r_predicate(jz_pc_r *r, jazz_model_RelationPredicate *p);
static jz_err r_predicate_nested(jz_pc_r *r, jazz_model_RelationPredicate *p) {
    JZ_TRY(budget_enter());
    jz_err e = r_predicate(r, p);
    budget_leave();
    return e;
}
static jz_err r_predicate_box(jz_pc_r *r, jazz_model_RelationPredicate **out) {
    *out = JZ_NEW(jazz_model_RelationPredicate);
    return r_predicate_nested(r, *out);
}
static jz_err r_predicate(jz_pc_r *r, jazz_model_RelationPredicate *p) {
    u32 n;
    JZ_TRY(r_unit(r, 11, &p->tag));
    switch (p->tag) {
    case JAZZ_RPE_CMP:
        JZ_TRY(r_column(r, &p->left));
        JZ_TRY(r_unit(r, 6, &p->op));
        return r_value_node(r, &p->right);
    case JAZZ_RPE_IS_NULL:
    case JAZZ_RPE_IS_NOT_NULL: return r_column(r, &p->left);
    case JAZZ_RPE_IN:
        JZ_TRY(r_column(r, &p->left));
        JZ_TRY(r_bounded_len(r, &n));
        for (u32 i = 0; i < n; i++) {
            JZ_TRY(bounded_push_check(p->values.len));
            JZ_TRY(r_value_node(r, JAZZ_VEC_SLOT(p->values)));
        }
        return JZ_OK;
    case JAZZ_RPE_CONTAINS:
        JZ_TRY(r_column(r, &p->left));
        return r_value_node(r, &p->right);
    case JAZZ_RPE_ENUM_MATCH:
        JZ_TRY(r_column(r, &p->left));
        JZ_TRY(r_bounded_string(r, &p->case_));
        return r_predicate_box(r, &p->inner);
    case JAZZ_RPE_AND:
    case JAZZ_RPE_OR:
        JZ_TRY(r_bounded_len(r, &n));
        for (u32 i = 0; i < n; i++) {
            JZ_TRY(bounded_push_check(p->items.len));
            JZ_TRY(r_predicate_nested(r, JAZZ_VEC_SLOT(p->items)));
        }
        return JZ_OK;
    case JAZZ_RPE_NOT: return r_predicate_box(r, &p->inner);
    default: return JZ_OK;
    }
}
static jz_err r_key_node(jz_pc_r *r, jazz_model_RelationKeyRef *k) {
    JZ_TRY(budget_node());
    return r_key(r, k);
}
static jz_err r_key_vec(jz_pc_r *r, jazz_model_RelationKeyRefVec *keys) {
    u32 n;
    JZ_TRY(r_bounded_len(r, &n));
    for (u32 i = 0; i < n; i++) {
        JZ_TRY(bounded_push_check(keys->len));
        JZ_TRY(r_key_node(r, JAZZ_VEC_SLOT(*keys)));
    }
    return JZ_OK;
}
static jz_err r_expr(jz_pc_r *r, jazz_model_RelationExpr *e);
static jz_err r_expr_nested(jz_pc_r *r, jazz_model_RelationExpr *e) {
    JZ_TRY(budget_enter());
    jz_err err = r_expr(r, e);
    budget_leave();
    return err;
}
static jz_err r_expr_box(jz_pc_r *r, jazz_model_RelationExpr **out) {
    *out = JZ_NEW(jazz_model_RelationExpr);
    return r_expr_nested(r, *out);
}
static jz_err r_expr(jz_pc_r *r, jazz_model_RelationExpr *e) {
    u32 n;
    JZ_TRY(r_unit(r, 10, &e->tag));
    switch (e->tag) {
    case JAZZ_REXPR_TABLE_SCAN:
        JZ_TRY(r_bounded_string(r, &e->table));
        return r_bounded_option_string(r, &e->alias);
    case JAZZ_REXPR_FILTER:
        JZ_TRY(r_expr_box(r, &e->input));
        return r_predicate_nested(r, &e->predicate);
    case JAZZ_REXPR_UNION:
        JZ_TRY(r_bounded_len(r, &n));
        for (u32 i = 0; i < n; i++) {
            JZ_TRY(bounded_push_check(e->inputs.len));
            jazz_model_RelationUnionArm *arm = JAZZ_VEC_SLOT(e->inputs);
            JZ_TRY(r_bounded_string(r, &arm->label));
            JZ_TRY(r_expr_nested(r, &arm->input));
        }
        return JZ_OK;
    case JAZZ_REXPR_JOIN:
        JZ_TRY(r_expr_box(r, &e->input));
        JZ_TRY(r_expr_box(r, &e->right));
        JZ_TRY(r_bounded_len(r, &n));
        for (u32 i = 0; i < n; i++) {
            JZ_TRY(bounded_push_check(e->on.len));
            JZ_TRY(budget_node());
            jazz_model_RelationJoinCondition *c = JAZZ_VEC_SLOT(e->on);
            JZ_TRY(r_column(r, &c->left));
            JZ_TRY(r_column(r, &c->right));
        }
        return r_unit(r, 2, &e->join_kind);
    case JAZZ_REXPR_PROJECT:
        JZ_TRY(r_expr_box(r, &e->input));
        JZ_TRY(r_bounded_len(r, &n));
        for (u32 i = 0; i < n; i++) {
            JZ_TRY(bounded_push_check(e->columns.len));
            JZ_TRY(budget_node());
            jazz_model_RelationProjectColumn *c = JAZZ_VEC_SLOT(e->columns);
            JZ_TRY(r_bounded_string(r, &c->alias));
            JZ_TRY(r_key(r, &c->expr));
        }
        return JZ_OK;
    case JAZZ_REXPR_GATHER: {
        u32 depth;
        JZ_TRY(r_expr_box(r, &e->input));
        JZ_TRY(r_expr_box(r, &e->right));
        JZ_TRY(r_key_node(r, &e->frontier_key));
        JZ_TRY(r_unit(r, 2, &e->bound.tag));
        if (e->bound.tag == JAZZ_BOUND_MAX_DEPTH) {
            JZ_TRY(jz_pc_r_u32(r, &depth));
            e->bound.max_depth = depth;
        }
        return r_key_vec(r, &e->keys);
    }
    case JAZZ_REXPR_DISTINCT:
        JZ_TRY(r_expr_box(r, &e->input));
        return r_key_vec(r, &e->keys);
    case JAZZ_REXPR_ORDER_BY:
        JZ_TRY(r_expr_box(r, &e->input));
        JZ_TRY(r_bounded_len(r, &n));
        for (u32 i = 0; i < n; i++) {
            JZ_TRY(bounded_push_check(e->terms.len));
            JZ_TRY(budget_node());
            jazz_model_RelationOrderBy *t = JAZZ_VEC_SLOT(e->terms);
            JZ_TRY(r_column(r, &t->column));
            JZ_TRY(r_unit(r, 2, &t->direction));
        }
        return JZ_OK;
    default: {
        u32 dim;
        JZ_TRY(r_expr_box(r, &e->input));
        JZ_TRY(jz_pc_r_u32(r, &dim));
        e->n = dim;
        return JZ_OK;
    }
    }
}

/* WireRelationQuery::deserialize: scope guard, then `rel` via deserialize_nested */
static jz_err wire_relation_query_deserialize(jz_pc_r *r, jazz_model_RelationQuery *out) {
    JZ_TRY(budget_scope_enter());
    jz_err e = r_expr_nested(r, &out->rel);
    budget_scope_drop();
    return e;
}

/* relation_query_from_wire: ensure_wire_size, validate_wire, convert */
static jz_err relation_query_from_wire(const jazz_model_RelationQuery *q) {
    JZ_TRY(ensure_wire_size(q));
    return validate_wire(q);
}

/* ================================================================ relation_query_wire (serde with) */
jz_err Q(relation_query_wire__serialize)(jz_pc_w *w, const jazz_model_RelationQuery *value) {
    /* non-human: value.as_ref().map(relation_query_to_wire).transpose().map_err(custom)?.serialize() */
    if (value) JZ_TRY(wire_custom(relation_query_to_wire(value)));
    jz_pc_option(w, value != NULL);
    if (value) return wire_custom(w_expr(w, &value->rel));
    return JZ_OK;
}
jz_err Q(relation_query_wire__deserialize)(jz_pc_r *r, jazz_model_RelationQuery **out) {
    bool some;
    *out = NULL;
    JZ_TRY(jz_pc_r_option(r, &some));
    if (!some) return JZ_OK;
    jazz_model_RelationQuery *q = JZ_NEW(jazz_model_RelationQuery);
    jz_err e = wire_relation_query_deserialize(r, q);
    if (!e.code) e = wire_custom(relation_query_from_wire(q));
    if (e.code) {
        Q(RelationQuery__drop)(q);
        jz_free(q);
        return e;
    }
    *out = q;
    return JZ_OK;
}
jz_err Q(relation_query_wire__serialize_required)(jz_pc_w *w, const jazz_model_RelationQuery *value) {
    JZ_TRY(wire_custom(relation_query_to_wire(value)));
    return wire_custom(w_expr(w, &value->rel));
}
jz_err Q(relation_query_wire__deserialize_required)(jz_pc_r *r, jazz_model_RelationQuery *out) {
    memset(out, 0, sizeof(*out));
    jz_err e = wire_relation_query_deserialize(r, out);
    if (!e.code) e = wire_custom(relation_query_from_wire(out));
    if (e.code) Q(RelationQuery__drop)(out);
    return e;
}

/* ================================================================ direct native read payloads */
jz_err Q(encode_relation_query_postcard)(const jazz_model_RelationQuery *value, jz_bytes *out) {
    JZ_TRY(relation_query_to_wire(value));
    jz_bytes bytes = {0};
    jz_pc_w w = {&bytes};
    jz_err e = w_expr(&w, &value->rel);
    if (e.code) { jz_bytes_drop(&bytes); return e; }
    if (bytes.len > MAX_RELATION_BYTES) { jz_bytes_drop(&bytes); return wire_too_large(); }
    *out = bytes;
    return JZ_OK;
}
jz_err Q(decode_relation_query_postcard)(jz_slice bytes, jazz_model_RelationQuery *out) {
    if (bytes.len > MAX_RELATION_BYTES) return wire_too_large();
    /* crate::postcard_exact::decode_postcard_exact::<WireRelationQuery> */
    jz_pc_r r = {bytes.ptr, bytes.ptr + bytes.len, 0};
    memset(out, 0, sizeof(*out));
    jz_err e = wire_relation_query_deserialize(&r, out);
    if (!e.code) e = jz_pc_r_finish(&r);
    if (e.code) {
        Q(RelationQuery__drop)(out);
        return wire_postcard(e);
    }
    e = relation_query_from_wire(out);
    if (e.code) Q(RelationQuery__drop)(out);
    return e;
}
