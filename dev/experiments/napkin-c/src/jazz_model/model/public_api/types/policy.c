/* model/public_api/types/policy.rs: OperationPolicy / TablePolicies.
 * The policy_expr DSL module and TablePolicyBuilder are Rust-builder-only API
 * (absent from the shipped WASM) and are not ported. */
#include "jazz_model/jazz_model.h"

#define P(x) jazz_model__model__public_api__types__policy__##x
#define PE(x) jazz_model__model__public_api__policy__PolicyExpr__##x

static void opt_expr_drop(jazz_model_PolicyExpr **e) {
    if (*e) { PE(drop)(*e); jz_free(*e); *e = NULL; }
}

static jazz_model_PolicyExpr *opt_expr_clone(const jazz_model_PolicyExpr *e) {
    if (!e) return NULL;
    jazz_model_PolicyExpr *c = JZ_NEW(jazz_model_PolicyExpr);
    PE(Clone__clone)(c, e);
    return c;
}

static bool opt_expr_eq(const jazz_model_PolicyExpr *a, const jazz_model_PolicyExpr *b) {
    if (!a || !b) return a == b;
    return PE(PartialEq__eq)(a, b);
}

void P(OperationPolicy__drop)(jazz_model_OperationPolicy *p) {
    opt_expr_drop(&p->using_);
    opt_expr_drop(&p->with_check);
}

static void operation_policy_clone(jazz_model_OperationPolicy *dst, const jazz_model_OperationPolicy *src) {
    dst->using_ = opt_expr_clone(src->using_);
    dst->with_check = opt_expr_clone(src->with_check);
}

static bool operation_policy_eq(const jazz_model_OperationPolicy *a, const jazz_model_OperationPolicy *b) {
    return opt_expr_eq(a->using_, b->using_) && opt_expr_eq(a->with_check, b->with_check);
}

void P(TablePolicies__drop)(jazz_model_TablePolicies *p) {
    P(OperationPolicy__drop)(&p->select);
    P(OperationPolicy__drop)(&p->insert);
    P(OperationPolicy__drop)(&p->update);
    P(OperationPolicy__drop)(&p->delete_);
}

void P(TablePolicies__Clone__clone)(jazz_model_TablePolicies *dst, const jazz_model_TablePolicies *src) {
    operation_policy_clone(&dst->select, &src->select);
    operation_policy_clone(&dst->insert, &src->insert);
    operation_policy_clone(&dst->update, &src->update);
    operation_policy_clone(&dst->delete_, &src->delete_);
}

bool P(TablePolicies__PartialEq__eq)(const jazz_model_TablePolicies *a, const jazz_model_TablePolicies *b) {
    return operation_policy_eq(&a->select, &b->select) && operation_policy_eq(&a->insert, &b->insert) &&
           operation_policy_eq(&a->update, &b->update) && operation_policy_eq(&a->delete_, &b->delete_);
}

bool P(TablePolicies__has_any_explicit_policy)(const jazz_model_TablePolicies *p) {
    return p->select.using_ || p->insert.with_check || p->update.using_ || p->update.with_check || p->delete_.using_;
}

bool P(TablePolicies__has_explicit_update_policy)(const jazz_model_TablePolicies *p) {
    return p->update.using_ || p->update.with_check;
}

static void opt_expr_serialize(jz_json_w *w, const char *key, const jazz_model_PolicyExpr *e) {
    jz_jw_key(w, key);
    if (e) (void)PE(Serialize__serialize)(w, e);
    else jz_jw_null(w);
}

static void operation_policy_serialize(jz_json_w *w, const jazz_model_OperationPolicy *p) {
    jz_jw_begin_obj(w);
    opt_expr_serialize(w, "using", p->using_);
    opt_expr_serialize(w, "with_check", p->with_check);
    jz_jw_end_obj(w);
}

void P(TablePolicies__Serialize__serialize)(jz_json_w *w, const jazz_model_TablePolicies *p) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "select");
    operation_policy_serialize(w, &p->select);
    jz_jw_key(w, "insert");
    operation_policy_serialize(w, &p->insert);
    jz_jw_key(w, "update");
    operation_policy_serialize(w, &p->update);
    jz_jw_key(w, "delete");
    operation_policy_serialize(w, &p->delete_);
    jz_jw_end_obj(w);
}

static jz_err opt_expr_deserialize(const jz_json *obj, const char *key, jazz_model_PolicyExpr **out) {
    const jz_json *f = jz_json_obj_get(obj, key);
    *out = NULL;
    if (!f || f->kind == JZ_JSON_NULL) return JZ_OK;
    jazz_model_PolicyExpr *e = JZ_NEW(jazz_model_PolicyExpr);
    jz_err err = PE(Deserialize__deserialize)(f, e);
    if (err.code) { jz_free(e); return err; }
    *out = e;
    return JZ_OK;
}

/* #[serde(default)] struct OperationPolicy */
static jz_err operation_policy_deserialize(const jz_json *obj, const char *key, jazz_model_OperationPolicy *out) {
    memset(out, 0, sizeof *out);
    const jz_json *f = jz_json_obj_get(obj, key);
    if (!f) return JZ_OK;
    JZ_TRY(jz_json_expect_obj(f, "struct OperationPolicy"));
    JZ_TRY(opt_expr_deserialize(f, "using", &out->using_));
    jz_err e = opt_expr_deserialize(f, "with_check", &out->with_check);
    if (e.code) P(OperationPolicy__drop)(out);
    return e;
}

/* #[serde(default)] struct TablePolicies */
jz_err P(TablePolicies__Deserialize__deserialize)(const jz_json *j, jazz_model_TablePolicies *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_json_expect_obj(j, "struct TablePolicies"));
    jz_err e = operation_policy_deserialize(j, "select", &out->select);
    if (!e.code) e = operation_policy_deserialize(j, "insert", &out->insert);
    if (!e.code) e = operation_policy_deserialize(j, "update", &out->update);
    if (!e.code) e = operation_policy_deserialize(j, "delete", &out->delete_);
    if (e.code) P(TablePolicies__drop)(out);
    return e;
}
