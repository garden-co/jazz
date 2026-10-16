/* lowering/graph_lowering.rs: translation from analyzed plans to executable
 * Groove operators (joins, projections, predicates, windows, aggregates,
 * recursive relations and parameter value sources). Public output-terminal
 * construction remains in terminals.c.
 *
 * Mapping notes:
 *  - LoweringContext is passed as a mutable pointer (lazy parameter domain).
 *  - `last_join_right` keeps a borrowed pointer to the right RelationInputPlan
 *    instead of a deep clone (it is only read).
 *  - lower_relation_input_cached borrows the resolved source rather than
 *    cloning it (`resolved_sources.get(id).cloned()` is only read). */
#include "../lowering.h"

#define F(x) jazz_engine__query_engine__fields__##x
#define PF(x) groove__ivm__graph__ProjectField__##x
#define GB(x) groove__ivm__graph__GraphBuilder__##x
#define GP(x) groove__ivm__op_types__PredicateExpr__##x
#define LIT(x) groove__ivm__op_types__LiteralValue__##x
#define VT_EQ groove__records__values__ValueType__PartialEq__eq
#define VT_DROP groove__records__values__ValueType__drop

typedef groove_PredicateExpr GPred;
typedef jazz_engine_LoweredRelationInput LRI;

/* ---------------------------------------------------------------- small helpers */
static void pfvec_drop(groove_ProjectFieldVec *v) { jz_vec_drop(v, sizeof(groove_ProjectField), groove__ivm__graph__ProjectField__drop_v); }
static void pfvec_append(groove_ProjectFieldVec *dst, groove_ProjectFieldVec *src) {
    jz_vec_extend(dst, src->ptr, src->len, sizeof(groove_ProjectField));
    jz_vec_drop(src, sizeof(groove_ProjectField), NULL);
}
static void gpvec_drop(groove_PredicateVec *v) { jz_vec_drop(v, sizeof(GPred), groove__ivm__op_types__PredicateExpr__drop_v); }
static jz_str s_lit(const char *s) { return jz_str_from(s); }
static jz_str s_clone(const jz_str *s) { return jz_str_clone(s); }
static void strset_init(jz_btree *t) { jz_btree_init(t, &jazz_engine__STRSET_DESC); }
static void depths_init(jz_btree *t) { jz_btree_init(t, &jazz_engine__STR_U32_MAP_DESC); }
static void depth_put(jz_btree *depths, const jz_str *field, u32 depth) {
    jz_str k = s_clone(field);
    jazz_engine__btree_put(depths, &k, &depth);
}
static u32 depth_get_or(const jz_btree *depths, const jz_str *field, u32 dflt) {
    const u32 *d = jz_btree_get(depths, field);
    return d ? *d : dflt;
}
/* entry(field).and_modify(max).or_insert(depth) */
static void depth_max(jz_btree *depths, const jz_str *field, u32 depth) {
    u32 *d = jz_btree_get(depths, field);
    if (d) { if (depth > *d) *d = depth; return; }
    depth_put(depths, field, depth);
}
static jazz_engine_StrVec strvec_two(jz_str a, jz_str b) { jazz_engine_StrVec v = {0}; JZ_VEC_PUSH(v, a); JZ_VEC_PUSH(v, b); return v; }
static jazz_engine_StrVec strvec_one_lit(const char *s) { return jazz_engine__strvec_one(s_lit(s)); }

static groove_GraphBuilder *gb_node(groove_GraphBuilder node) {
    groove_GraphBuilder *g = jz_rc_new(sizeof(groove_GraphBuilder), groove__ivm__graph__GraphBuilder__drop_in_place);
    *g = node;
    return g;
}
static groove_LiteralValue lit_string(jz_str s) { groove_LiteralValue v; memset(&v, 0, sizeof v); v.tag = GROOVE_LIT_STRING; v.u.s = s; return v; }
static groove_LiteralValue lit_from_value(groove_Value value) {
    groove_LiteralValue lit;
    LIT(From_Value__from)(&lit, &value);
    return lit;
}
static jazz_engine_UrErr decode_value(const jz_bytes *bytes, const char *what, groove_Value *out) {
    jz_pc_r r = {bytes->ptr, bytes->ptr + bytes->len, 0};
    jz_err e = groove__records__values__Value__Deserialize__postcard(&r, out);
    if (!JZ_IS_ERR(e)) return NULL;
    jz_str shown = jazz_engine__err_display(&e);
    jz_err_drop(&e);
    jazz_engine_UrErr err = jazz_engine__ur_op(jz_format("%s could not be decoded: %.*s", what, JE_STR_ARG(shown)));
    jz_str_drop(&shown);
    return err;
}
static jazz_engine_UrErr ur_op_dbg_sid(const char *fmt, const jazz_engine_SourceId *sid) {
    JE_DBG(d, sid_debug, sid);
    jazz_engine_UrErr e = jazz_engine__ur_op(jz_format(fmt, JE_STR_ARG(d)));
    jz_str_drop(&d);
    return e;
}
static jazz_engine_UrErr ur_rt_dbg_sid(const char *fmt, const jazz_engine_SourceId *sid) {
    JE_DBG(d, sid_debug, sid);
    jazz_engine_UrErr e = jazz_engine__ur_rt(jz_format(fmt, JE_STR_ARG(d)));
    jz_str_drop(&d);
    return e;
}
static bool uses_policy_value_comparison(const jazz_engine_LoweringContext *request) {
    return request->request->policy.tag == JAZZ_ENGINE_PCTX_AUTHORIZATION_SUBPLAN;
}
/* Policy-predicate programs reuse relation lowering but do not publish rows. */
static bool omits_public_occurrence_carriers(const jazz_engine_LoweringContext *request) {
    return uses_policy_value_comparison(request) ||
           (request->request->output.has_app_rows && !request->request->output.app_rows.public_terminal);
}

/* ---------------------------------------------------------------- source descriptor lookups */
bool JE_GL(resolved_source_descriptor_index)(const jazz_engine_ResolvedSource *source, jz_slice field, u32 *out) {
    if (groove__records__RecordDescriptor__field_index(source->row_shape.descriptor, (const char *)field.ptr, field.len, out)) return true;
    u32 n;
    const groove_DescriptorField *d = groove__records__RecordDescriptor__fields(source->row_shape.descriptor, &n);
    for (u32 i = 0; i < n; i++)
        if (d[i].has_name && jazz_engine__str_eq_slice(&d[i].name, field)) { *out = i; return true; }
    return false;
}

const groove_ValueType *JE_GL(source_field_type)(const jazz_engine_ResolvedSource *source, jz_slice field) {
    u32 idx, n;
    if (!JE_GL(resolved_source_descriptor_index)(source, field, &idx)) return NULL;
    const groove_DescriptorField *d = groove__records__RecordDescriptor__fields(source->row_shape.descriptor, &n);
    return idx < n ? &d[idx].value_type : NULL;
}

static u32 value_type_nullable_depth(const groove_ValueType *t) {
    u32 depth = 0;
    while (t->tag == GROOVE_VT_NULLABLE) { depth++; t = t->u.inner; }
    return depth;
}
static u32 source_field_nullable_depth(const jazz_engine_ResolvedSource *source, jz_slice field) {
    const groove_ValueType *t = JE_GL(source_field_type)(source, field);
    return t ? value_type_nullable_depth(t) : 0;
}
static bool source_field_is_nullable(const jazz_engine_ResolvedSource *source, jz_slice field) { return source_field_nullable_depth(source, field) > 0; }

jazz_engine_UrErr JE_GL(require_source_field)(const jazz_engine_ResolvedSource *source, jz_slice field, jz_str *out) {
    u32 idx, n;
    if (!JE_GL(resolved_source_descriptor_index)(source, field, &idx)) {
        JE_DBG(d, sid_debug, &source->row_shape.source);
        jazz_engine_UrErr e = jazz_engine__ur_op(jz_format("resolved source %.*s does not provide field '%.*s'", JE_STR_ARG(d), (int)field.len, field.ptr));
        jz_str_drop(&d);
        return e;
    }
    const groove_DescriptorField *d = groove__records__RecordDescriptor__fields(source->row_shape.descriptor, &n);
    if (idx >= n || !d[idx].has_name) {
        JE_DBG(ds, sid_debug, &source->row_shape.source);
        jazz_engine_UrErr e = jazz_engine__ur_op(jz_format("resolved source %.*s field '%.*s' has no carrier name", JE_STR_ARG(ds), (int)field.len, field.ptr));
        jz_str_drop(&ds);
        return e;
    }
    *out = s_clone(&d[idx].name);
    return NULL;
}

/* source_fields(): descriptor names, then routing fields */
void JE_GL(source_fields)(const jazz_engine_ResolvedSource *source, jazz_engine_StrVec *out) {
    jazz_engine_StrVec v = {0};
    u32 n;
    const groove_DescriptorField *d = groove__records__RecordDescriptor__fields(source->row_shape.descriptor, &n);
    for (u32 i = 0; i < n; i++) if (d[i].has_name) jazz_engine__strvec_push_clone(&v, &d[i].name);
    const jz_str *r; void *unit;
    JZ_BTREE_FOR(&source->routing_fields, r, unit) jazz_engine__strvec_push_clone(&v, r);
    *out = v;
}
static void source_fields_into_set(const jazz_engine_ResolvedSource *source, jz_btree *set) {
    jazz_engine_StrVec v;
    JE_GL(source_fields)(source, &v);
    JZ_VEC_FOR(jz_str, s, v) jazz_engine__btree_set_add(set, s);
    jz_vec_drop(&v, sizeof(jz_str), NULL);
}
static void source_nullable_field_depths(const jazz_engine_ResolvedSource *source, jz_btree *out) {
    depths_init(out);
    jazz_engine_StrVec names;
    JE_GL(source_fields)(source, &names);
    JZ_VEC_FOR(jz_str, name, names) {
        u32 depth = source_field_nullable_depth(source, JZ_S(name));
        if (depth == 0) continue;
        if (jazz_engine__str_starts_with(name, JAZZ_ENGINE_USER_COLUMN_PREFIX)) {
            usize pl = strlen(JAZZ_ENGINE_USER_COLUMN_PREFIX);
            jz_str logical = jz_str_from_slice(jz_slice_of(name->ptr + pl, name->len - (u32)pl));
            jazz_engine__btree_put(out, &logical, &depth);
        }
        depth_put(out, name, depth);
    }
    jazz_engine__strvec_drop(&names);
}
static void source_nullable_fields(const jazz_engine_ResolvedSource *source, jz_btree *out) {
    jz_btree depths;
    source_nullable_field_depths(source, &depths);
    strset_init(out);
    const jz_str *k; const u32 *v;
    JZ_BTREE_FOR(&depths, k, v) jazz_engine__strset_insert_clone(out, k);
    jz_btree_drop(&depths);
}

/* ---------------------------------------------------------------- LoweredRelationInput */
void JE_GL(LoweredRelationInput__drop)(LRI *l) {
    groove_gb_drop(l->graph);
    if (l->has_root_source) JE_RES(ResolvedSource__drop)(&l->root_source);
    jz_btree_drop(&l->fields);
    jz_btree_drop(&l->nullable_fields);
    jz_btree_drop(&l->nullable_field_depths);
    if (l->has_union_occurrence_carrier) { jz_str_drop(&l->union_label_field); jz_str_drop(&l->union_row_field); }
}
void JE_GL(LoweredRelationInput__Clone__clone)(LRI *dst, const LRI *src) {
    memset(dst, 0, sizeof *dst);
    dst->graph = src->graph ? groove_gb_clone(src->graph) : NULL;
    dst->has_root_source = src->has_root_source;
    if (src->has_root_source) JE_RES(ResolvedSource__Clone__clone)(&dst->root_source, &src->root_source);
    jazz_engine__strset_clone(&dst->fields, &src->fields);
    jazz_engine__strset_clone(&dst->nullable_fields, &src->nullable_fields);
    jz_btree_clone(&dst->nullable_field_depths, &src->nullable_field_depths, jazz_engine__str_clone_v, NULL);
    dst->has_union_occurrence_carrier = src->has_union_occurrence_carrier;
    if (src->has_union_occurrence_carrier) {
        dst->union_label_field = s_clone(&src->union_label_field);
        dst->union_row_field = s_clone(&src->union_row_field);
    }
}
static void lri_drop_v(void *p) { JE_GL(LoweredRelationInput__drop)(p); }
static int usize_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    uintptr_t x = *(const uintptr_t *)a, y = *(const uintptr_t *)b;
    return x < y ? -1 : x > y;
}
const jz_btree_desc jazz_engine__U32_LRI_MAP_DESC = {sizeof(uintptr_t), sizeof(LRI), usize_cmp, NULL, lri_drop_v};
static void lri_clone_v(void *d, const void *s) { JE_GL(LoweredRelationInput__Clone__clone)(d, s); }
static void usize_clone_v(void *d, const void *s) { *(uintptr_t *)d = *(const uintptr_t *)s; }

static void lri_empty(LRI *l, groove_GraphBuilder *graph) {
    memset(l, 0, sizeof *l);
    l->graph = graph;
    strset_init(&l->fields);
    strset_init(&l->nullable_fields);
    depths_init(&l->nullable_field_depths);
}
/* LoweredRelationInput { graph, root_source: Some(source), fields: source_fields, nullable_*: source_* } */
static void lri_of_source(LRI *l, groove_GraphBuilder *graph, const jazz_engine_ResolvedSource *source) {
    memset(l, 0, sizeof *l);
    l->graph = graph;
    l->has_root_source = true;
    JE_RES(ResolvedSource__Clone__clone)(&l->root_source, source);
    strset_init(&l->fields);
    source_fields_into_set(source, &l->fields);
    source_nullable_fields(source, &l->nullable_fields);
    source_nullable_field_depths(source, &l->nullable_field_depths);
}
static groove_GraphBuilder *lri_take_graph(LRI *l) {
    groove_GraphBuilder *g = l->graph;
    l->graph = NULL;
    return g;
}

/* ---------------------------------------------------------------- literal coercion */
groove_LiteralValue JE_GL(coerce_literal_for_value_type)(groove_LiteralValue value, const groove_ValueType *value_type) {
    if (value.tag == GROOVE_LIT_STRING && value_type->tag == GROOVE_VT_UUID) {
        jz_uuid u;
        jz_err e = jz_uuid_parse(JZ_S(&value.u.s), &u);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); return value; }
        jz_str_drop(&value.u.s);
        groove_LiteralValue r;
        memset(&r, 0, sizeof r);
        r.tag = GROOVE_LIT_UUID;
        r.u.uuid = u;
        return r;
    }
    if (value.tag == GROOVE_LIT_UUID && value_type->tag == GROOVE_VT_STRING) {
        char buf[37];
        jz_uuid_to_string(&value.u.uuid, buf);
        return lit_string(jz_str_from(buf));
    }
    if (value.tag == GROOVE_LIT_STRING && value_type->tag == GROOVE_VT_ENUM_TAG) {
        u8 d;
        jz_err e = groove__records__values__ScalarEnumSchema__discriminant(value_type->u.enum_tag, &value.u.s, &d);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); return value; }
        jz_str_drop(&value.u.s);
        groove_LiteralValue r;
        memset(&r, 0, sizeof r);
        r.tag = GROOVE_LIT_ENUM_TAG;
        r.u.u8_ = d;
        return r;
    }
    if (value.tag == GROOVE_LIT_NULLABLE && value.u.nullable) {
        *value.u.nullable = JE_GL(coerce_literal_for_value_type)(*value.u.nullable, value_type);
        return value;
    }
    if (value_type->tag == GROOVE_VT_NULLABLE) {
        groove_LiteralValue inner = JE_GL(coerce_literal_for_value_type)(value, value_type->u.inner);
        groove_LiteralValue r;
        memset(&r, 0, sizeof r);
        r.tag = GROOVE_LIT_NULLABLE;
        r.u.nullable = jz_memdup(&inner, sizeof inner);
        return r;
    }
    if (value.tag == GROOVE_LIT_ARRAY && value_type->tag == GROOVE_VT_ARRAY) {
        JZ_VEC_FOR(groove_LiteralValue, item, value.u.items) *item = JE_GL(coerce_literal_for_value_type)(*item, value_type->u.inner);
        return value;
    }
    if (value.tag == GROOVE_LIT_TUPLE && value_type->tag == GROOVE_VT_TUPLE && value.u.items.len == value_type->u.tuple.len) {
        for (u32 i = 0; i < value.u.items.len; i++)
            value.u.items.ptr[i] = JE_GL(coerce_literal_for_value_type)(value.u.items.ptr[i], &value_type->u.tuple.ptr[i]);
        return value;
    }
    return value;
}
static groove_LiteralValue coerce_literal_for_source_field(groove_LiteralValue value, const jazz_engine_ResolvedSource *source, const jz_str *field) {
    const groove_ValueType *t = JE_GL(source_field_type)(source, JZ_S(field));
    return t ? JE_GL(coerce_literal_for_value_type)(value, t) : value;
}
static groove_LiteralValue coerce_literal_for_source_array_element(groove_LiteralValue value, const jazz_engine_ResolvedSource *source, const jz_str *field) {
    const groove_ValueType *t = JE_GL(source_field_type)(source, JZ_S(field));
    if (!t) return value;
    const groove_ValueType *nn = groove__records__values__ValueType__non_nullable(t);
    return nn->tag == GROOVE_VT_ARRAY ? JE_GL(coerce_literal_for_value_type)(value, nn->u.inner) : value;
}

/* ---------------------------------------------------------------- claims */
static jazz_engine_UrErr unbound_claim(const jazz_engine_ClaimPath *path) {
    jazz_engine_UnsupportedReason r = jazz_engine__ur_new(JAZZ_ENGINE_UR_UNBOUND_CLAIM);
    jazz_engine__strvec_clone(&r.claim.segments, &path->segments);
    return jazz_engine__ur_box(r);
}
jazz_engine_UrErr JE_GL(claim_value)(const jazz_engine_ClaimPath *path, const jazz_engine_PolicyContext *policy, groove_Value *out) {
    if (policy->tag == JAZZ_ENGINE_PCTX_SYSTEM) return JE_UR_OP("claim values require an identity policy context");
    const jazz_types_AuthorSubject *permission_subject = &policy->permission_subject;
    jz_str name;
    if (jazz_model__query__author_claim_path_key(path->segments.ptr, path->segments.len, &name)) {
        jz_btree claims;
        jazz_model__model__policy_claims__author_policy_claims(*permission_subject, &claims);
        jz_str key;
        bool found = jz_btree_remove(&claims, &name, &key, out);
        if (found) jz_str_drop(&key);
        jz_btree_drop(&claims);
        jz_str_drop(&name);
        return found ? NULL : unbound_claim(path);
    }
    if (jazz_model__model__policy_claims__policy_claim_at_path(&policy->claims, path->segments.ptr, path->segments.len, out)) return NULL;
    if (path->segments.len == 1 && jz_str_eq_cstr(&path->segments.ptr[0], "user")) {
        jazz_types__ids__AuthorSubject__to_value(permission_subject, out);
        return NULL;
    }
    return unbound_claim(path);
}
static bool is_unbound_claim_reason(const jazz_engine_UnsupportedReason *r) { return r->tag == JAZZ_ENGINE_UR_UNBOUND_CLAIM; }

/* ---------------------------------------------------------------- value references */
typedef struct LoweredValueRef { bool is_field; jz_str field; groove_LiteralValue literal; } LoweredValueRef;
static void lvr_drop(LoweredValueRef *v) {
    if (v->is_field) jz_str_drop(&v->field);
    else LIT(drop)(&v->literal);
}
static LoweredValueRef lvr_field(jz_str f) { LoweredValueRef r; memset(&r, 0, sizeof r); r.is_field = true; r.field = f; return r; }
static LoweredValueRef lvr_lit(groove_LiteralValue v) { LoweredValueRef r; memset(&r, 0, sizeof r); r.literal = v; return r; }

static const char *provenance_source_field(u8 field) {
    switch (field) {
    case JAZZ_ENGINE_PROV_CREATED_AT: return "$createdAt";
    case JAZZ_ENGINE_PROV_CREATED_BY: return "$createdBy";
    case JAZZ_ENGINE_PROV_UPDATED_AT: return "$updatedAt";
    default: return "$updatedBy";
    }
}

static jazz_engine_UrErr lower_value_ref(const jazz_engine_NormalizedValueRef *value, const jazz_engine_SourceId *source_id,
                                         const jazz_engine_ResolvedSource *source, jazz_engine_LoweringContext *request, LoweredValueRef *out) {
    switch (value->tag) {
    case JAZZ_ENGINE_NVR_TEMPLATE_LITERAL: {
        groove_Value v;
        JE_UR_TRY(JE_ARG(LoweringContext__literal)(request, value->u.template_literal, &v));
        *out = lvr_lit(lit_from_value(v));
        return NULL;
    }
    case JAZZ_ENGINE_NVR_SOURCE_FIELD: {
        const jz_str *field = &value->u.source_field.field;
        if (!sid_eq(&value->u.source_field.source, source_id))
            return ur_op_dbg_sid("predicate references unsupported source %.*s", &value->u.source_field.source);
        if (jz_str_eq_cstr(field, "__root_union_arm")) { *out = lvr_field(s_clone(field)); return NULL; }
        jz_str f;
        jazz_engine_UrErr e = JE_GL(require_source_field)(source, JZ_S(field), &f);
        if (e) {
            jazz_engine__ur_free(e);
            jz_str user = F(user_column_field)(JZ_S(field));
            e = JE_GL(require_source_field)(source, JZ_S(&user), &f);
            jz_str_drop(&user);
            if (e) return e;
        }
        *out = lvr_field(f);
        return NULL;
    }
    case JAZZ_ENGINE_NVR_PARAM: {
        const groove_Value *v = jz_btree_get(&request->request->input.binding.values, &value->u.param);
        if (!v) return jazz_engine__ur_op(jz_format("binding parameter '%.*s' is not bound", JE_STR_ARG(value->u.param)));
        groove_Value c;
        groove__records__values__Value__Clone__clone(&c, v);
        *out = lvr_lit(lit_from_value(c));
        return NULL;
    }
    case JAZZ_ENGINE_NVR_CLAIM: {
        groove_Value v;
        JE_UR_TRY(JE_GL(claim_value)(&value->u.claim, &request->request->policy, &v));
        *out = lvr_lit(lit_from_value(v));
        return NULL;
    }
    case JAZZ_ENGINE_NVR_FRONTIER_COLUMN: return JE_UR_OP("frontier values are not valid in root source predicates");
    case JAZZ_ENGINE_NVR_ROW_ID: {
        if (value->sub == JAZZ_ENGINE_ROWID_FRONTIER) return JE_UR_OP("frontier row ids are not valid in root source predicates");
        if (!sid_eq(&value->u.source, source_id)) return ur_op_dbg_sid("predicate references unsupported row id source %.*s", &value->u.source);
        jz_str f;
        JE_UR_TRY(JE_GL(require_source_field)(source, JZ_S(&source->row_shape.row_uuid_field), &f));
        *out = lvr_field(f);
        return NULL;
    }
    case JAZZ_ENGINE_NVR_PROVENANCE: {
        if (!sid_eq(&value->u.source, source_id)) return ur_op_dbg_sid("predicate references unsupported provenance source %.*s", &value->u.source);
        jz_str f;
        const char *p = provenance_source_field(value->sub);
        JE_UR_TRY(JE_GL(require_source_field)(source, JZ_C(p), &f));
        *out = lvr_field(f);
        return NULL;
    }
    default: { /* Literal */
        groove_Value v;
        JE_UR_TRY(decode_value(&value->u.literal, "literal value", &v));
        *out = lvr_lit(lit_from_value(v));
        return NULL;
    }
    }
}

static jazz_engine_UrErr lower_field_ref(const jazz_engine_NormalizedValueRef *value, const jazz_engine_LinearCurrentRoot *plan,
                                         const jazz_engine_ResolvedSource *source, jazz_engine_LoweringContext *request, const char *context, jz_str *out) {
    const jazz_engine_SourceId *source_id = JE_PLAN(LinearRoot__source)(&plan->root);
    if (!source_id) return jazz_engine__ur_op(jz_format("%s must be a root source field", context));
    LoweredValueRef v;
    JE_UR_TRY(lower_value_ref(value, source_id, source, request, &v));
    if (!v.is_field) { lvr_drop(&v); return jazz_engine__ur_op(jz_format("%s must be a root source field", context)); }
    *out = v.field;
    return NULL;
}

static jazz_engine_UrErr lower_join_key_ref(const jazz_engine_NormalizedValueRef *value, const jazz_engine_SourceId *source_id,
                                            const jazz_engine_ResolvedSource *source, jazz_engine_LoweringContext *request, jz_str *out) {
    LoweredValueRef v;
    JE_UR_TRY(lower_value_ref(value, source_id, source, request, &v));
    if (!v.is_field) { lvr_drop(&v); return JE_UR_OP("join_via join keys must be source fields"); }
    *out = v.field;
    return NULL;
}

/* ---------------------------------------------------------------- predicates */
static GPred gp_list(u8 tag, groove_PredicateVec children) { GPred p; memset(&p, 0, sizeof p); p.tag = tag; p.u.children = children; return p; }
static GPred constant_predicate(bool value) { groove_PredicateVec none = {0}; return gp_list(value ? GROOVE_PRED_AND : GROOVE_PRED_OR, none); }
static GPred gp_null(u8 tag, jz_str field) { GPred p; memset(&p, 0, sizeof p); p.tag = tag; p.u.null.field = field; return p; }
static GPred gp_fields(u8 tag, jz_str field, jz_str other) { GPred p; memset(&p, 0, sizeof p); p.tag = tag; p.u.fields.field = field; p.u.fields.other = other; return p; }
static GPred gp_lit(u8 tag, jz_str field, groove_LiteralValue value) { GPred p; memset(&p, 0, sizeof p); p.tag = tag; p.u.lit.field = field; p.u.lit.value = value; return p; }
static GPred gp_canonical(GPred p) { GPred out; GP(canonicalize)(&out, p); return out; }

static u8 invert_comparison(u8 op) {
    switch (op) {
    case JAZZ_ENGINE_CMP_EQ: return JAZZ_ENGINE_CMP_NE;
    case JAZZ_ENGINE_CMP_NE: return JAZZ_ENGINE_CMP_EQ;
    case JAZZ_ENGINE_CMP_LT: return JAZZ_ENGINE_CMP_GTE;
    case JAZZ_ENGINE_CMP_LTE: return JAZZ_ENGINE_CMP_GT;
    case JAZZ_ENGINE_CMP_GT: return JAZZ_ENGINE_CMP_LTE;
    default: return JAZZ_ENGINE_CMP_LT;
    }
}
static u8 predicate_kind(u8 op) {
    switch (op) {
    case JAZZ_ENGINE_CMP_EQ: return GROOVE_PK_EQ;
    case JAZZ_ENGINE_CMP_NE: return GROOVE_PK_NEQ;
    case JAZZ_ENGINE_CMP_LT: return GROOVE_PK_LT;
    case JAZZ_ENGINE_CMP_LTE: return GROOVE_PK_LT_EQ;
    case JAZZ_ENGINE_CMP_GT: return GROOVE_PK_GT;
    default: return GROOVE_PK_GT_EQ;
    }
}
static const char *comparison_op_name(u8 op) {
    static const char *const names[] = {"Eq", "Ne", "Lt", "Lte", "Gt", "Gte"};
    return op < 6 ? names[op] : "?";
}

static bool literal_float(const groove_LiteralValue *value, f64 *out) {
    while (value->tag == GROOVE_LIT_NULLABLE && value->u.nullable) value = value->u.nullable;
    if (value->tag != GROOVE_LIT_F64) return false;
    *out = value->u.f64_;
    return true;
}
static bool compare_literals(const groove_LiteralValue *left, u8 op, const groove_LiteralValue *right) {
    f64 l, r;
    if (literal_float(left, &l) && literal_float(right, &r)) {
        /* Predicate semantics are numeric; unordered comparisons are false for every operator. */
        if (l != l || r != r) return false;
        switch (op) {
        case JAZZ_ENGINE_CMP_EQ: return l == r;
        case JAZZ_ENGINE_CMP_NE: return l != r;
        case JAZZ_ENGINE_CMP_LT: return l < r;
        case JAZZ_ENGINE_CMP_LTE: return l <= r;
        case JAZZ_ENGINE_CMP_GT: return l > r;
        default: return l >= r;
        }
    }
    if (op == JAZZ_ENGINE_CMP_EQ) return LIT(PartialEq__eq)(left, right);
    if (op == JAZZ_ENGINE_CMP_NE) return !LIT(PartialEq__eq)(left, right);
    int c = LIT(PartialOrd__partial_cmp)(left, right);
    switch (op) {
    case JAZZ_ENGINE_CMP_LT: return c == -1;
    case JAZZ_ENGINE_CMP_LTE: return c == -1 || c == 0;
    case JAZZ_ENGINE_CMP_GT: return c == 1;
    default: return c == 1 || c == 0;
    }
}

static jazz_engine_UrErr lower_compare(const jazz_engine_NormalizedValueRef *left_ref, u8 op, const jazz_engine_NormalizedValueRef *right_ref, bool negated,
                                       const jazz_engine_SourceId *source_id, const jazz_engine_ResolvedSource *source,
                                       jazz_engine_LoweringContext *request, GPred *out) {
    LoweredValueRef left, right;
    JE_UR_TRY(lower_value_ref(left_ref, source_id, source, request, &left));
    jazz_engine_UrErr e = lower_value_ref(right_ref, source_id, source, request, &right);
    if (e) { lvr_drop(&left); return e; }
    u8 effective_op = negated ? invert_comparison(op) : op;
    u8 kind = predicate_kind(effective_op);
    if (left.is_field && !right.is_field) {
        groove_LiteralValue v = coerce_literal_for_source_field(right.literal, source, &left.field);
        *out = GP(from_field_literal)(kind, left.field, v);
        return NULL;
    }
    if (!left.is_field && right.is_field) {
        groove_LiteralValue v = coerce_literal_for_source_field(left.literal, source, &right.field);
        *out = GP(from_field_literal)(groove__ivm__op_types__PredicateKind__reversed(kind), right.field, v);
        return NULL;
    }
    if (left.is_field && right.is_field) {
        if (effective_op == JAZZ_ENGINE_CMP_EQ) { *out = gp_fields(GROOVE_PRED_EQ_FIELD, left.field, right.field); return NULL; }
        if (effective_op == JAZZ_ENGINE_CMP_NE) { *out = gp_fields(GROOVE_PRED_NEQ_FIELD, left.field, right.field); return NULL; }
        lvr_drop(&left);
        lvr_drop(&right);
        return jazz_engine__ur_op(jz_format("field-to-field comparison %s is not lowered yet", comparison_op_name(effective_op)));
    }
    /* Inverting an operator is not boolean negation for unordered floats. */
    bool matches = compare_literals(&left.literal, op, &right.literal);
    lvr_drop(&left);
    lvr_drop(&right);
    *out = constant_predicate(negated ? !matches : matches);
    return NULL;
}

static jazz_engine_UrErr lower_contains(const jazz_engine_NormalizedValueRef *value_ref, const jazz_engine_NormalizedValueRef *needle_ref,
                                        const jazz_engine_SourceId *source_id, const jazz_engine_ResolvedSource *source,
                                        jazz_engine_LoweringContext *request, GPred *out) {
    LoweredValueRef value, needle;
    JE_UR_TRY(lower_value_ref(value_ref, source_id, source, request, &value));
    jazz_engine_UrErr e = lower_value_ref(needle_ref, source_id, source, request, &needle);
    if (e) { lvr_drop(&value); return e; }
    if (!value.is_field && value.literal.tag == GROOVE_LIT_ARRAY && !needle.is_field) {
        bool found = false;
        JZ_VEC_FOR(const groove_LiteralValue, item, value.literal.u.items) if (LIT(PartialEq__eq)(item, &needle.literal)) { found = true; break; }
        lvr_drop(&value);
        lvr_drop(&needle);
        *out = constant_predicate(found);
        return NULL;
    }
    if (value.is_field && !needle.is_field) {
        groove_LiteralValue v = coerce_literal_for_source_array_element(needle.literal, source, &value.field);
        *out = gp_lit(GROOVE_PRED_CONTAINS, value.field, v);
        return NULL;
    }
    if (value.is_field && needle.is_field) { *out = gp_fields(GROOVE_PRED_CONTAINS_FIELD, value.field, needle.field); return NULL; }
    if (!value.is_field && value.literal.tag == GROOVE_LIT_ARRAY && needle.is_field) {
        if (value.literal.u.items.len == 0) {
            lvr_drop(&value);
            lvr_drop(&needle);
            *out = constant_predicate(false);
            return NULL;
        }
        groove_PredicateVec ors = {0};
        JZ_VEC_FOR(groove_LiteralValue, item, value.literal.u.items)
            JZ_VEC_PUSH(ors, gp_lit(GROOVE_PRED_EQ, s_clone(&needle.field), coerce_literal_for_source_field(*item, source, &needle.field)));
        jz_vec_drop(&value.literal.u.items, sizeof(groove_LiteralValue), NULL);
        jz_str_drop(&needle.field);
        *out = gp_list(GROOVE_PRED_OR, ors);
        return NULL;
    }
    lvr_drop(&value);
    lvr_drop(&needle);
    return JE_UR_OP("array contains requires a source field haystack");
}

static jazz_engine_UrErr lower_null_test(const jazz_engine_NormalizedValueRef *value_ref, bool is_null, const jazz_engine_SourceId *source_id,
                                         const jazz_engine_ResolvedSource *source, jazz_engine_LoweringContext *request, GPred *out) {
    LoweredValueRef v;
    JE_UR_TRY(lower_value_ref(value_ref, source_id, source, request, &v));
    if (v.is_field) { *out = gp_null(is_null ? GROOVE_PRED_IS_NULL : GROOVE_PRED_IS_NOT_NULL, v.field); return NULL; }
    bool none = v.literal.tag == GROOVE_LIT_NULLABLE && !v.literal.u.nullable;
    lvr_drop(&v);
    *out = constant_predicate(none ? is_null : !is_null);
    return NULL;
}

static jazz_engine_UrErr lower_enum_payload_predicate(const jazz_engine_PredicateExpr *predicate, GPred *out) {
    switch (predicate->tag) {
    case JAZZ_ENGINE_PRED_TRUE: *out = constant_predicate(true); return NULL;
    case JAZZ_ENGINE_PRED_FALSE: *out = constant_predicate(false); return NULL;
    case JAZZ_ENGINE_PRED_COMPARE: {
        const jazz_engine_NormalizedValueRef *l = &predicate->u.pair.left, *r = &predicate->u.pair.right;
        const jz_str *field;
        const jz_bytes *bytes;
        u8 op;
        if (l->tag == JAZZ_ENGINE_NVR_SOURCE_FIELD && r->tag == JAZZ_ENGINE_NVR_LITERAL) {
            field = &l->u.source_field.field; bytes = &r->u.literal; op = predicate->op;
        } else if (l->tag == JAZZ_ENGINE_NVR_LITERAL && r->tag == JAZZ_ENGINE_NVR_SOURCE_FIELD) {
            field = &r->u.source_field.field; bytes = &l->u.literal; op = invert_comparison(predicate->op);
        } else {
            return JE_UR_OP("payload enum predicates require field/literal comparisons");
        }
        groove_Value v;
        JE_UR_TRY(decode_value(bytes, "payload enum literal", &v));
        *out = GP(from_field_literal)(predicate_kind(op), s_clone(field), lit_from_value(v));
        return NULL;
    }
    case JAZZ_ENGINE_PRED_IS_NULL:
    case JAZZ_ENGINE_PRED_IS_NOT_NULL:
        if (predicate->u.value.tag != JAZZ_ENGINE_NVR_SOURCE_FIELD) return JE_UR_OP("unsupported payload enum predicate");
        *out = gp_null(predicate->tag == JAZZ_ENGINE_PRED_IS_NULL ? GROOVE_PRED_IS_NULL : GROOVE_PRED_IS_NOT_NULL, s_clone(&predicate->u.value.u.source_field.field));
        return NULL;
    case JAZZ_ENGINE_PRED_AND:
    case JAZZ_ENGINE_PRED_OR: {
        groove_PredicateVec children = {0};
        JZ_VEC_FOR(const jazz_engine_PredicateExpr, child, predicate->u.children) {
            GPred c;
            jazz_engine_UrErr e = lower_enum_payload_predicate(child, &c);
            if (e) { gpvec_drop(&children); return e; }
            JZ_VEC_PUSH(children, c);
        }
        *out = gp_list(predicate->tag == JAZZ_ENGINE_PRED_AND ? GROOVE_PRED_AND : GROOVE_PRED_OR, children);
        return NULL;
    }
    case JAZZ_ENGINE_PRED_NOT: return JE_UR_OP("negated payload enum predicates are not lowered yet");
    default: return JE_UR_OP("unsupported payload enum predicate");
    }
}

static jazz_engine_UrErr lower_not_predicate(const jazz_engine_PredicateExpr *predicate, const jazz_engine_SourceId *source_id,
                                             const jazz_engine_ResolvedSource *source, jazz_engine_LoweringContext *request, GPred *out);

typedef jazz_engine_UrErr (*PredLowerFn)(const jazz_engine_PredicateExpr *p, const jazz_engine_SourceId *sid, const jazz_engine_ResolvedSource *s,
                                         jazz_engine_LoweringContext *rq, GPred *out);
static jazz_engine_UrErr lower_children(const jazz_engine_PredicateVec *children, PredLowerFn f, const jazz_engine_SourceId *sid,
                                        const jazz_engine_ResolvedSource *s, jazz_engine_LoweringContext *rq, u8 tag, GPred *out) {
    groove_PredicateVec v = {0};
    JZ_VEC_FOR(const jazz_engine_PredicateExpr, child, *children) {
        GPred c;
        jazz_engine_UrErr e = f(child, sid, s, rq, &c);
        if (e) { gpvec_drop(&v); return e; }
        JZ_VEC_PUSH(v, c);
    }
    *out = gp_list(tag, v);
    return NULL;
}

static jazz_engine_UrErr lower_predicate_inner(const jazz_engine_PredicateExpr *predicate, const jazz_engine_SourceId *source_id,
                                               const jazz_engine_ResolvedSource *source, jazz_engine_LoweringContext *request, GPred *out) {
    switch (predicate->tag) {
    case JAZZ_ENGINE_PRED_TRUE: *out = constant_predicate(true); return NULL;
    case JAZZ_ENGINE_PRED_FALSE: *out = constant_predicate(false); return NULL;
    case JAZZ_ENGINE_PRED_COMPARE:
        return lower_compare(&predicate->u.pair.left, predicate->op, &predicate->u.pair.right, false, source_id, source, request, out);
    case JAZZ_ENGINE_PRED_IN: {
        groove_PredicateVec v = {0};
        JZ_VEC_FOR(const jazz_engine_NormalizedValueRef, option, predicate->u.in.options) {
            GPred c;
            jazz_engine_UrErr e = lower_compare(&predicate->u.in.value, JAZZ_ENGINE_CMP_EQ, option, false, source_id, source, request, &c);
            if (e) { gpvec_drop(&v); return e; }
            JZ_VEC_PUSH(v, c);
        }
        *out = gp_list(GROOVE_PRED_OR, v);
        return NULL;
    }
    case JAZZ_ENGINE_PRED_ARRAY_CONTAINS: return lower_contains(&predicate->u.pair.left, &predicate->u.pair.right, source_id, source, request, out);
    case JAZZ_ENGINE_PRED_TEXT_CONTAINS: return JE_UR_OP("text containment predicates are not lowered yet");
    case JAZZ_ENGINE_PRED_IS_NULL: return lower_null_test(&predicate->u.value, true, source_id, source, request, out);
    case JAZZ_ENGINE_PRED_IS_NOT_NULL: return lower_null_test(&predicate->u.value, false, source_id, source, request, out);
    case JAZZ_ENGINE_PRED_ENUM_MATCH: {
        LoweredValueRef v;
        JE_UR_TRY(lower_value_ref(&predicate->u.enum_match.value, source_id, source, request, &v));
        if (!v.is_field) { lvr_drop(&v); return JE_UR_OP("enum match requires a source field"); }
        GPred payload;
        jazz_engine_UrErr e = lower_enum_payload_predicate(predicate->u.enum_match.payload, &payload);
        if (e) { lvr_drop(&v); return e; }
        GPred p;
        memset(&p, 0, sizeof p);
        p.tag = GROOVE_PRED_ENUM_MATCH;
        p.u.enum_match.field = v.field;
        p.u.enum_match.case_tag = predicate->u.enum_match.case_tag;
        p.u.enum_match.payload = jz_memdup(&payload, sizeof payload);
        *out = p;
        return NULL;
    }
    case JAZZ_ENGINE_PRED_AND: return lower_children(&predicate->u.children, JE_GL(lower_predicate), source_id, source, request, GROOVE_PRED_AND, out);
    case JAZZ_ENGINE_PRED_OR: return lower_children(&predicate->u.children, JE_GL(lower_predicate), source_id, source, request, GROOVE_PRED_OR, out);
    default: return lower_not_predicate(predicate->u.not_, source_id, source, request, out);
    }
}

jazz_engine_UrErr JE_GL(lower_predicate)(const jazz_engine_PredicateExpr *predicate, const jazz_engine_SourceId *source_id,
                                         const jazz_engine_ResolvedSource *source, jazz_engine_LoweringContext *request, GPred *out) {
    if (JE_ARG(LoweringContext__predicate_argument)(request, predicate, source_id, source, out)) return NULL;
    GPred lowered;
    jazz_engine_UrErr e = lower_predicate_inner(predicate, source_id, source, request, &lowered);
    if (e) {
        if (!is_unbound_claim_reason(e)) return e;
        jazz_engine__ur_free(e);
        lowered = constant_predicate(false);
    }
    *out = gp_canonical(lowered);
    return NULL;
}

/* Groove comparisons use SQL-null semantics; Jazz membership negation is two-valued. */
static jazz_engine_UrErr lower_two_valued_ne(const jazz_engine_NormalizedValueRef *left, const jazz_engine_NormalizedValueRef *right,
                                             const jazz_engine_SourceId *source_id, const jazz_engine_ResolvedSource *source,
                                             jazz_engine_LoweringContext *request, GPred *out) {
    GPred p[5];
    u32 n = 0;
    jazz_engine_UrErr e;
    if ((e = lower_null_test(left, true, source_id, source, request, &p[n]))) goto fail;
    n++;
    if ((e = lower_null_test(right, false, source_id, source, request, &p[n]))) goto fail;
    n++;
    if ((e = lower_null_test(left, false, source_id, source, request, &p[n]))) goto fail;
    n++;
    if ((e = lower_null_test(right, true, source_id, source, request, &p[n]))) goto fail;
    n++;
    if ((e = lower_compare(left, JAZZ_ENGINE_CMP_EQ, right, true, source_id, source, request, &p[n]))) goto fail;
    n++;
    groove_PredicateVec a = {0}, b = {0}, ors = {0};
    JZ_VEC_PUSH(a, p[0]);
    JZ_VEC_PUSH(a, p[1]);
    JZ_VEC_PUSH(b, p[2]);
    JZ_VEC_PUSH(b, p[3]);
    JZ_VEC_PUSH(ors, gp_list(GROOVE_PRED_AND, a));
    JZ_VEC_PUSH(ors, gp_list(GROOVE_PRED_AND, b));
    JZ_VEC_PUSH(ors, p[4]);
    *out = gp_canonical(gp_list(GROOVE_PRED_OR, ors));
    return NULL;
fail:
    for (u32 i = 0; i < n; i++) GP(drop)(&p[i]);
    return e;
}

static jazz_engine_UrErr lower_not_predicate_inner(const jazz_engine_PredicateExpr *predicate, const jazz_engine_SourceId *source_id,
                                                   const jazz_engine_ResolvedSource *source, jazz_engine_LoweringContext *request, GPred *out) {
    switch (predicate->tag) {
    case JAZZ_ENGINE_PRED_TRUE: *out = constant_predicate(false); return NULL;
    case JAZZ_ENGINE_PRED_FALSE: *out = constant_predicate(true); return NULL;
    case JAZZ_ENGINE_PRED_COMPARE:
        return lower_compare(&predicate->u.pair.left, predicate->op, &predicate->u.pair.right, true, source_id, source, request, out);
    case JAZZ_ENGINE_PRED_IN: {
        groove_PredicateVec v = {0};
        JZ_VEC_FOR(const jazz_engine_NormalizedValueRef, option, predicate->u.in.options) {
            GPred c;
            jazz_engine_UrErr e = lower_two_valued_ne(&predicate->u.in.value, option, source_id, source, request, &c);
            if (e) { gpvec_drop(&v); return e; }
            JZ_VEC_PUSH(v, c);
        }
        *out = gp_list(GROOVE_PRED_AND, v);
        return NULL;
    }
    case JAZZ_ENGINE_PRED_ARRAY_CONTAINS:
    case JAZZ_ENGINE_PRED_TEXT_CONTAINS: return JE_UR_OP("negated containment predicates are not lowered yet");
    case JAZZ_ENGINE_PRED_IS_NULL: return lower_null_test(&predicate->u.value, false, source_id, source, request, out);
    case JAZZ_ENGINE_PRED_IS_NOT_NULL: return lower_null_test(&predicate->u.value, true, source_id, source, request, out);
    case JAZZ_ENGINE_PRED_AND: return lower_children(&predicate->u.children, lower_not_predicate, source_id, source, request, GROOVE_PRED_OR, out);
    case JAZZ_ENGINE_PRED_OR: return lower_children(&predicate->u.children, lower_not_predicate, source_id, source, request, GROOVE_PRED_AND, out);
    case JAZZ_ENGINE_PRED_NOT: return JE_GL(lower_predicate)(predicate->u.not_, source_id, source, request, out);
    default: return JE_UR_OP("negated enum match predicates are not lowered yet");
    }
}

static jazz_engine_UrErr lower_not_predicate(const jazz_engine_PredicateExpr *predicate, const jazz_engine_SourceId *source_id,
                                             const jazz_engine_ResolvedSource *source, jazz_engine_LoweringContext *request, GPred *out) {
    GPred lowered;
    jazz_engine_UrErr e = lower_not_predicate_inner(predicate, source_id, source, request, &lowered);
    if (e) {
        if (!is_unbound_claim_reason(e)) return e;
        jazz_engine__ur_free(e);
        lowered = constant_predicate(false);
    }
    *out = gp_canonical(lowered);
    return NULL;
}

bool JE_GL(has_explicit_closure_path)(const jazz_engine_NormalizedRowSetShape *shape) {
    JZ_VEC_FOR(const jazz_engine_ClosurePath, p, shape->closure_paths) if (p->tag == JAZZ_ENGINE_CLOSURE_EXPLICIT_INCLUDE) return true;
    return false;
}

/* ---------------------------------------------------------------- join helpers */
groove_GraphBuilder *JE_GL(policy_join_if_needed)(groove_GraphBuilder *left, groove_GraphBuilder *right, jazz_engine_StrVec left_on,
                                                 jazz_engine_StrVec right_on, jazz_engine_LoweringContext *request) {
    if (uses_policy_value_comparison(request)) return GB(policy_join)(left, right, left_on, right_on);
    return GB(join)(left, right, left_on, right_on);
}

static groove_FieldRefVec field_refs_named(jazz_engine_StrVec names) {
    groove_FieldRefVec v = {0};
    JZ_VEC_FOR(jz_str, n, names) JZ_VEC_PUSH(v, groove__ivm__graph__FieldRef__name(*n));
    jz_vec_drop(&names, sizeof(jz_str), NULL);
    return v;
}
static groove_FieldRefVec field_refs_stored(jazz_engine_StrVec names) {
    groove_FieldRefVec v = {0};
    JZ_VEC_FOR(jz_str, n, names) JZ_VEC_PUSH(v, groove__ivm__graph__FieldRef__stored_name(*n));
    jz_vec_drop(&names, sizeof(jz_str), NULL);
    return v;
}

static groove_GraphBuilder *semi_join_if_needed(groove_GraphBuilder *left, groove_GraphBuilder *right, jazz_engine_StrVec left_on,
                                                jazz_engine_StrVec right_on, jazz_engine_LoweringContext *request) {
    groove_GraphBuilder node;
    memset(&node, 0, sizeof node);
    node.tag = GROOVE_GB_SEMI_JOIN;
    node.u.join.left = left;
    node.u.join.right = right;
    node.u.join.left_on = field_refs_named(left_on);
    node.u.join.right_on = field_refs_named(right_on);
    node.u.join.comparison = uses_policy_value_comparison(request) ? GROOVE_CMP_POLICY : GROOVE_CMP_EXACT;
    return gb_node(node);
}

/* ---------------------------------------------------------------- value sources */
typedef jazz_engine_TypedOutputFieldVec ParamVec; /* Vec<(String, ColumnType)> */
static void paramvec_drop(ParamVec *v) { jz_vec_drop(v, sizeof(jazz_engine_TypedOutputField), jazz_engine__tof_drop_v); }

static groove_RecordDescriptor descriptor_from_named(const jz_str *names, usize name_stride, const groove_ValueType *types, usize type_stride, u32 n) {
    jz_str *ns = jz_alloc(sizeof(jz_str) * (n ? n : 1));
    groove_ValueType *ts = jz_alloc(sizeof(groove_ValueType) * (n ? n : 1));
    for (u32 i = 0; i < n; i++) {
        ns[i] = s_clone((const jz_str *)((const u8 *)names + i * name_stride));
        ts[i] = groove_vt_clone((const groove_ValueType *)((const u8 *)types + i * type_stride));
    }
    groove_RecordDescriptor d = groove__records__RecordDescriptor__new(ns, ts, n);
    jz_free(ns);
    jz_free(ts);
    return d;
}
static groove_RecordDescriptor value_source_descriptor(const jazz_engine_ValueSourceColumnVec *columns) {
    if (!columns->len) return descriptor_from_named(NULL, 0, NULL, 0, 0);
    return descriptor_from_named(&columns->ptr[0].name, sizeof(jazz_engine_ValueSourceColumn), &columns->ptr[0].ty,
                                 sizeof(jazz_engine_ValueSourceColumn), columns->len);
}
static groove_RecordDescriptor params_descriptor(const ParamVec *params) {
    if (!params->len) return descriptor_from_named(NULL, 0, NULL, 0, 0);
    return descriptor_from_named(&params->ptr[0].name, sizeof(jazz_engine_TypedOutputField), &params->ptr[0].ty,
                                 sizeof(jazz_engine_TypedOutputField), params->len);
}

/* additional: NULL or one (name, ty) pair, consumed */
static jazz_engine_UrErr binding_descriptor_params_with_user_params(jazz_engine_LoweringContext *request, jazz_engine_TypedOutputField *additional,
                                                                    ParamVec *out) {
    const jazz_engine_ParameterDomain *domain;
    jazz_engine_UrErr e = JE_ARG(LoweringContext__parameter_domain)(request, &domain);
    if (e) { if (additional) tof_drop(additional); return e; }
    jz_btree user_params;
    jazz_engine__strvtmap_clone(&user_params, &request->request->input.binding.extra_user_params);
    const jz_str *k; const groove_ValueType *t;
    JZ_BTREE_FOR(&domain->user_params, k, t) {
        jz_str kc = s_clone(k);
        groove_ValueType tc = groove_vt_clone(t);
        jazz_engine__btree_put(&user_params, &kc, &tc);
    }
    if (additional) {
        /* A source's physical type is only a fallback for synthetic parameters;
         * it must not replace a validated binding type (including nullability
         * and enum registry identity). */
        jazz_engine__btree_or_insert(&user_params, &additional->name, &additional->ty);
    }
    ParamVec v = {0};
    jz_str pk; groove_ValueType pt;
    while (jz_btree_pop_first(&user_params, &pk, &pt)) {
        jazz_engine_TypedOutputField f = {pk, pt};
        JZ_VEC_PUSH(v, f);
    }
    jz_btree_drop(&user_params);
    const jazz_engine_ClaimParameter *param;
    JZ_BTREE_FOR(&domain->claim_params, k, param) {
        jazz_engine_TypedOutputField f = {s_clone(k), groove_vt_clone(&param->ty)};
        JZ_VEC_PUSH(v, f);
    }
    *out = v;
    return NULL;
}
static jazz_engine_UrErr binding_descriptor_params(jazz_engine_LoweringContext *request, ParamVec *out) {
    return binding_descriptor_params_with_user_params(request, NULL, out);
}
static jazz_engine_UrErr binding_source_descriptor_with_user_params(jazz_engine_LoweringContext *request, jazz_engine_TypedOutputField *additional,
                                                                    groove_RecordDescriptor *out) {
    ParamVec params;
    JE_UR_TRY(binding_descriptor_params_with_user_params(request, additional, &params));
    *out = params_descriptor(&params);
    paramvec_drop(&params);
    return NULL;
}

static jazz_engine_UrErr lower_value_source_column(const jazz_engine_ValueSourceColumn *column, jazz_engine_LoweringContext *request, groove_Value *out) {
    switch (column->value.tag) {
    case JAZZ_ENGINE_NVR_PARAM: {
        const groove_Value *v = jz_btree_get(&request->request->input.binding.values, &column->value.u.param);
        if (!v) return jazz_engine__ur_op(jz_format("binding parameter '%.*s' is not bound", JE_STR_ARG(column->value.u.param)));
        groove__records__values__Value__Clone__clone(out, v);
        return NULL;
    }
    case JAZZ_ENGINE_NVR_LITERAL: return decode_value(&column->value.u.literal, "literal value", out);
    case JAZZ_ENGINE_NVR_CLAIM: return JE_GL(claim_value)(&column->value.u.claim, &request->request->policy, out);
    default: return JE_UR_OP("value source columns must be binding params, literals, or claims");
    }
}

static jazz_engine_UrErr value_source_row(const jazz_engine_ValueSourceColumnVec *columns, jazz_engine_LoweringContext *request, groove_ValueVec *out) {
    groove_ValueVec row = {0};
    JZ_VEC_FOR(const jazz_engine_ValueSourceColumn, column, *columns) {
        groove_Value v;
        jazz_engine_UrErr e = lower_value_source_column(column, request, &v);
        if (e) { jz_vec_drop(&row, sizeof(groove_Value), groove__records__values__Value__drop_v); return e; }
        JZ_VEC_PUSH(row, v);
    }
    *out = row;
    return NULL;
}

static jazz_engine_UrErr values_graph(groove_RecordDescriptor descriptor, const jazz_engine_ValueSourceColumnVec *columns,
                                      jazz_engine_LoweringContext *request, const char *what, groove_GraphBuilder **out) {
    groove_ValueVec row;
    JE_UR_TRY(value_source_row(columns, request, &row));
    jz_err err = GB(values)(descriptor, &row, 1, out);
    jz_vec_drop(&row, sizeof(groove_Value), groove__records__values__Value__drop_v);
    if (!JZ_IS_ERR(err)) return NULL;
    jz_str shown = jazz_engine__err_display(&err);
    jz_err_drop(&err);
    jazz_engine_UrErr e = jazz_engine__ur_op(jz_format("%s: %.*s", what, JE_STR_ARG(shown)));
    jz_str_drop(&shown);
    return e;
}

static const jazz_engine_TypedOutputField *param_find(const ParamVec *params, const jz_str *name) {
    JZ_VEC_FOR(const jazz_engine_TypedOutputField, p, *params) if (jz_str_eq(&p->name, name)) return p;
    return NULL;
}

static jazz_engine_UrErr lower_value_source(const jz_str *shape, const jazz_engine_ValueSourceColumnVec *columns, u8 mode,
                                            jazz_engine_LoweringContext *request, groove_GraphBuilder **out) {
    groove_RecordDescriptor descriptor = value_source_descriptor(columns);
    if (mode == JAZZ_ENGINE_VSM_INLINE) return values_graph(descriptor, columns, request, "inline value source could not encode", out);

    const jazz_engine_ParameterDomain *domain;
    JE_UR_TRY(JE_ARG(LoweringContext__parameter_domain)(request, &domain));
    ParamVec params;
    JE_UR_TRY(binding_descriptor_params(request, &params));
    jazz_engine_UrErr e = NULL;
    JZ_VEC_FOR(const jazz_engine_ValueSourceColumn, column, *columns) {
        switch (column->value.tag) {
        case JAZZ_ENGINE_NVR_PARAM: {
            const jz_str *param = &column->value.u.param;
            const jazz_engine_TypedOutputField *existing = param_find(&params, param);
            if (!existing) {
                e = jazz_engine__ur_op(jz_format("binding parameter '%.*s' is not part of the program parameter domain", JE_STR_ARG(*param)));
                goto done;
            }
            if (!VT_EQ(&existing->ty, &column->ty)) {
                e = jazz_engine__ur_op(jz_format("binding parameter '%.*s' has conflicting value-source types", JE_STR_ARG(*param)));
                goto done;
            }
            break;
        }
        case JAZZ_ENGINE_NVR_CLAIM: {
            jz_str param = F(claim_param_field)(&column->value.u.claim);
            const jazz_engine_ClaimParameter *existing = jz_btree_get(&domain->claim_params, &param);
            if (!existing) e = jazz_engine__ur_op(jz_format("claim parameter '%.*s' is not part of the program parameter domain", JE_STR_ARG(param)));
            else if (!VT_EQ(&existing->ty, &column->ty))
                e = jazz_engine__ur_op(jz_format("claim parameter '%.*s' has conflicting value-source types", JE_STR_ARG(param)));
            jz_str_drop(&param);
            if (e) goto done;
            break;
        }
        case JAZZ_ENGINE_NVR_LITERAL: break;
        default:
            e = JE_UR_OP("binding value source columns must reference binding params, claims, or literals");
            goto done;
        }
    }
    if (params.len == 0) {
        paramvec_drop(&params);
        return values_graph(descriptor, columns, request, "binding value source could not encode constant row", out);
    }
    {
        groove_RecordDescriptor input_descriptor = params_descriptor(&params);
        jz_btree projected, source_user_params;
        strset_init(&projected);
        strset_init(&source_user_params);
        JZ_VEC_FOR(const jazz_engine_ValueSourceColumn, column, *columns) jazz_engine__strset_insert_clone(&projected, &column->name);
        JZ_VEC_FOR(const jazz_engine_ValueSourceColumn, column, *columns) {
            if (column->value.tag == JAZZ_ENGINE_NVR_PARAM && jz_btree_contains(&domain->user_params, &column->value.u.param)) {
                jazz_engine__strset_insert_clone(&source_user_params, &column->value.u.param);
            } else if (column->value.tag == JAZZ_ENGINE_NVR_CLAIM) {
                jz_str param = F(claim_param_field)(&column->value.u.claim);
                if (jz_btree_contains(&domain->claim_params, &param)) jazz_engine__btree_set_add(&source_user_params, &param);
                else jz_str_drop(&param);
            }
        }
        groove_ProjectFieldVec retained_routes = {0};
        const jz_str *param; const void *unit;
        JZ_BTREE_FOR(&domain->user_params, param, unit) {
            if (!jz_btree_contains(&source_user_params, param)) continue;
            jz_str route_field = F(route_param_field)(JZ_S(param));
            if (!jz_btree_contains(&projected, &route_field)) JZ_VEC_PUSH(retained_routes, PF(renamed)(s_clone(param), route_field));
            else jz_str_drop(&route_field);
        }
        /* A nested policy graph can consume an enclosing claim only in a
         * sibling/ancestor branch. The shared binding descriptor nevertheless
         * needs that slot to survive this value-source projection so
         * downstream authorization joins can route it. */
        JZ_BTREE_FOR(&domain->claim_params, param, unit)
            if (!jz_btree_contains(&projected, param)) JZ_VEC_PUSH(retained_routes, PF(named)(s_clone(param)));
        jz_btree_drop(&projected);
        jz_btree_drop(&source_user_params);

        groove_ProjectFieldVec fields = {0};
        JZ_VEC_FOR(const jazz_engine_ValueSourceColumn, column, *columns) {
            switch (column->value.tag) {
            case JAZZ_ENGINE_NVR_PARAM: JZ_VEC_PUSH(fields, PF(renamed)(s_clone(&column->value.u.param), s_clone(&column->name))); break;
            case JAZZ_ENGINE_NVR_CLAIM: JZ_VEC_PUSH(fields, PF(renamed)(F(claim_param_field)(&column->value.u.claim), s_clone(&column->name))); break;
            case JAZZ_ENGINE_NVR_LITERAL: {
                groove_Value value;
                e = decode_value(&column->value.u.literal, "literal value", &value);
                if (e) { pfvec_drop(&fields); pfvec_drop(&retained_routes); goto done; }
                JZ_VEC_PUSH(fields, PF(literal)(s_clone(&column->name), lit_from_value(value)));
                break;
            }
            default: jz_panic("checked above");
            }
        }
        pfvec_append(&fields, &retained_routes);
        *out = GB(project_fields)(GB(binding_source)(s_clone(shape), input_descriptor), fields);
    }
done:
    paramvec_drop(&params);
    return e;
}

/* ---------------------------------------------------------------- key pairs */
typedef jazz_engine_UrErr (*KeyResolveFn)(void *ctx, const jazz_engine_NormalizedValueRef *value, jz_str *out);
typedef struct KeyResolver { KeyResolveFn f; void *ctx; } KeyResolver;
typedef struct BiKeyCtx { const char *non_equality, *mismatch; KeyResolver left, right; } BiKeyCtx;

typedef struct JoinKeyCtx { const jazz_engine_SourceId *source_id; const jazz_engine_ResolvedSource *source; jazz_engine_LoweringContext *request; } JoinKeyCtx;
static jazz_engine_UrErr resolve_join_key(void *ctx, const jazz_engine_NormalizedValueRef *value, jz_str *out) {
    JoinKeyCtx *c = ctx;
    return lower_join_key_ref(value, c->source_id, c->source, c->request, out);
}
typedef struct RelKeyCtx { const jazz_engine_RelationInputPlan *plan; const LRI *output; jazz_engine_LoweringContext *request; } RelKeyCtx;
static jazz_engine_UrErr resolve_relation_key(void *ctx, const jazz_engine_NormalizedValueRef *value, jz_str *out) {
    RelKeyCtx *c = ctx;
    return JE_GL(lower_relation_key_ref)(value, c->plan, c->output, c->request, out);
}

static jz_str key_pair_error(jazz_engine_UrErr err, jz_str *ok) {
    jz_str s = {0};
    if (!err) {
        jz_str_push_cstr(&s, "accepted ");
        jazz_engine__fmt__str(&s, ok);
        jz_str_drop(ok);
    } else {
        JE_RES(UnsupportedReason__Debug__fmt)(&s, err);
        jazz_engine__ur_free(err);
    }
    return s;
}

static jazz_engine_UrErr lower_bidirectional_key_pair(const jazz_engine_PredicateExpr *predicate, const BiKeyCtx *c, jz_str *left_key, jz_str *right_key) {
    if (predicate->tag != JAZZ_ENGINE_PRED_COMPARE || predicate->op != JAZZ_ENGINE_CMP_EQ) return jazz_engine__ur_op(jz_str_from(c->non_equality));
    const jazz_engine_NormalizedValueRef *left = &predicate->u.pair.left, *right = &predicate->u.pair.right;
    jz_str dl = {0}, dr = {0}, sl = {0}, sr = {0};
    jazz_engine_UrErr edl = c->left.f(c->left.ctx, left, &dl);
    jazz_engine_UrErr edr = c->right.f(c->right.ctx, right, &dr);
    if (!edl && !edr) { *left_key = dl; *right_key = dr; return NULL; }
    jazz_engine_UrErr esl = c->left.f(c->left.ctx, right, &sl);
    jazz_engine_UrErr esr = c->right.f(c->right.ctx, left, &sr);
    if (!esl && !esr) {
        if (!edl) jz_str_drop(&dl); else jazz_engine__ur_free(edl);
        if (!edr) jz_str_drop(&dr); else jazz_engine__ur_free(edr);
        *left_key = sl;
        *right_key = sr;
        return NULL;
    }
    jz_str a = key_pair_error(edl, &dl), b = key_pair_error(edr, &dr), x = key_pair_error(esl, &sl), y = key_pair_error(esr, &sr);
    jazz_engine_UrErr e = jazz_engine__ur_op(jz_format("%s; direct errors: %.*s, %.*s; swapped errors: %.*s, %.*s", c->mismatch,
                                                       JE_STR_ARG(a), JE_STR_ARG(b), JE_STR_ARG(x), JE_STR_ARG(y)));
    jz_str_drop(&a);
    jz_str_drop(&b);
    jz_str_drop(&x);
    jz_str_drop(&y);
    return e;
}

/* Lowers a conjunction of equality predicates (or a single one) into the
 * parallel left and right key lists, one pair per conjunct. */
static jazz_engine_UrErr lower_key_pairs(const jazz_engine_PredicateExpr *predicate, const char *empty_message, const BiKeyCtx *c,
                                         jazz_engine_StrVec *left_keys, jazz_engine_StrVec *right_keys) {
    jazz_engine_StrVec l = {0}, r = {0};
    /* A true join condition is an uncorrelated existence gate. Routing keys
     * are added by the caller, so independent prepared sessions stay isolated. */
    if (predicate->tag == JAZZ_ENGINE_PRED_TRUE) { *left_keys = l; *right_keys = r; return NULL; }
    const jazz_engine_PredicateExpr *preds = predicate;
    u32 n = 1;
    if (predicate->tag == JAZZ_ENGINE_PRED_AND) { preds = predicate->u.children.ptr; n = predicate->u.children.len; }
    for (u32 i = 0; i < n; i++) {
        jz_str lk, rk;
        jazz_engine_UrErr e = lower_bidirectional_key_pair(&preds[i], c, &lk, &rk);
        if (e) { jazz_engine__strvec_drop(&l); jazz_engine__strvec_drop(&r); return e; }
        JZ_VEC_PUSH(l, lk);
        JZ_VEC_PUSH(r, rk);
    }
    if (l.len == 0) return jazz_engine__ur_op(jz_str_from(empty_message));
    *left_keys = l;
    *right_keys = r;
    return NULL;
}

jazz_engine_UrErr JE_GL(lower_path_key_pair)(const jazz_engine_PredicateExpr *predicate, const jazz_engine_SourceId *parent_source_id,
                                             const jazz_engine_ResolvedSource *parent_source, const jazz_engine_SourceId *child_source_id,
                                             const jazz_engine_ResolvedSource *child_source, jazz_engine_LoweringContext *request,
                                             jz_str *parent_key, jz_str *child_key) {
    JoinKeyCtx l = {parent_source_id, parent_source, request}, r = {child_source_id, child_source, request};
    BiKeyCtx c = {"correlated path projection only lowers equality correlations",
                  "correlated path projection correlation must compare parent and child fields",
                  {resolve_join_key, &l}, {resolve_join_key, &r}};
    return lower_bidirectional_key_pair(predicate, &c, parent_key, child_key);
}

/* lower_join_key_pairs (and its lower_join_key_pair) have no caller in the Rust source: omitted. */

/* accumulated join fields: BTreeMap<(SourceId, String), (String, usize)> */
typedef struct AccKey { jazz_engine_SourceId source; jz_str field; } AccKey;
typedef struct AccVal { jz_str field; u32 depth; } AccVal;
static int acc_key_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const AccKey *x = a, *y = b;
    int c = sid_cmp(&x->source, &y->source);
    return c ? c : jz_str_cmp(&x->field, &y->field);
}
static void acc_key_drop(void *k) { AccKey *x = k; sid_drop(&x->source); jz_str_drop(&x->field); }
static void acc_val_drop(void *v) { jz_str_drop(&((AccVal *)v)->field); }
static const jz_btree_desc ACC_JOIN_FIELDS_DESC = {sizeof(AccKey), sizeof(AccVal), acc_key_cmp, acc_key_drop, acc_val_drop};

/* accumulated_join_field: NULL = None (borrowed) */
static const AccVal *accumulated_join_field(const jazz_engine_NormalizedValueRef *value, const jz_btree *fields) {
    AccKey key;
    if (value->tag == JAZZ_ENGINE_NVR_SOURCE_FIELD) {
        key.source = value->u.source_field.source;
        key.field = F(user_column_field)(JZ_S(&value->u.source_field.field));
    } else if (value->tag == JAZZ_ENGINE_NVR_ROW_ID && value->sub == JAZZ_ENGINE_ROWID_SOURCE) {
        key.source = value->u.source;
        key.field = s_lit("row_uuid");
    } else {
        return NULL;
    }
    const AccVal *v = jz_btree_get(fields, &key);
    jz_str_drop(&key.field);
    return v;
}

static jazz_engine_UrErr lower_linear_root_key_ref(const jazz_engine_NormalizedValueRef *value, const jazz_engine_LinearRoot *root,
                                                   const jazz_engine_ResolvedSource *source, jazz_engine_LoweringContext *request, jz_str *out) {
    switch (root->tag) {
    case JAZZ_ENGINE_LROOT_SOURCE: return lower_join_key_ref(value, &root->source, source, request, out);
    case JAZZ_ENGINE_LROOT_FRONTIER:
        if (value->tag == JAZZ_ENGINE_NVR_FRONTIER_COLUMN && jz_str_eq(&value->u.frontier_column.frontier, &root->frontier)) {
            JZ_VEC_FOR(const jazz_engine_ValueSourceColumn, column, root->columns)
                if (jz_str_eq(&column->name, &value->u.frontier_column.field)) { *out = s_clone(&value->u.frontier_column.field); return NULL; }
        }
        if (value->tag == JAZZ_ENGINE_NVR_ROW_ID && value->sub == JAZZ_ENGINE_ROWID_FRONTIER && jz_str_eq(&value->u.frontier, &root->frontier)) {
            JZ_VEC_FOR(const jazz_engine_ValueSourceColumn, column, root->columns)
                if (jz_str_eq_cstr(&column->name, "row_uuid")) { *out = s_lit("row_uuid"); return NULL; }
        }
        return JE_UR_OP("join left key must be a frontier column");
    default: {
        const jz_str *name = value->tag == JAZZ_ENGINE_NVR_PARAM ? &value->u.param
                           : value->tag == JAZZ_ENGINE_NVR_FRONTIER_COLUMN ? &value->u.frontier_column.field : NULL;
        if (name) {
            JZ_VEC_FOR(const jazz_engine_ValueSourceColumn, column, root->columns)
                if (jz_str_eq(&column->name, name)) { *out = s_clone(name); return NULL; }
        }
        return JE_UR_OP("join left key must be a value-source column");
    }
    }
}

typedef struct LinearLeftCtx { const jazz_engine_LinearRoot *root; const jazz_engine_ResolvedSource *source; const jz_btree *accumulated; jazz_engine_LoweringContext *request; } LinearLeftCtx;
static jazz_engine_UrErr resolve_linear_left_key(void *ctx, const jazz_engine_NormalizedValueRef *value, jz_str *out) {
    LinearLeftCtx *c = ctx;
    jazz_engine_UrErr e = lower_linear_root_key_ref(value, c->root, c->source, c->request, out);
    if (!e) return NULL;
    jazz_engine__ur_free(e);
    const AccVal *acc = accumulated_join_field(value, c->accumulated);
    if (acc) { *out = s_clone(&acc->field); return NULL; }
    return JE_UR_OP("join left key must reference the root or an accumulated join source");
}

static jazz_engine_UrErr lower_linear_join_key_pairs(const jazz_engine_PredicateExpr *predicate, const jazz_engine_LinearRoot *left_root,
                                                     const jazz_engine_ResolvedSource *left_source, const jazz_engine_RelationInputPlan *right_plan,
                                                     const LRI *right_output, const jz_btree *accumulated_join_fields,
                                                     jazz_engine_LoweringContext *request, jazz_engine_StrVec *left_keys, jazz_engine_StrVec *right_keys) {
    LinearLeftCtx l = {left_root, left_source, accumulated_join_fields, request};
    RelKeyCtx r = {right_plan, right_output, request};
    BiKeyCtx c = {"join_via only lowers equality join predicates",
                  "join_via join predicate must compare left root and right relation fields",
                  {resolve_linear_left_key, &l}, {resolve_relation_key, &r}};
    return lower_key_pairs(predicate, "join_via requires at least one equality join predicate", &c, left_keys, right_keys);
}

jazz_engine_UrErr JE_GL(lower_root_to_relation_key_pairs)(const jazz_engine_PredicateExpr *predicate, const jazz_engine_ResolvedSource *root_source,
                                                          const jazz_engine_RelationInputPlan *right_plan, const LRI *right_output,
                                                          jazz_engine_LoweringContext *request, jazz_engine_StrVec *left_keys, jazz_engine_StrVec *right_keys) {
    JoinKeyCtx l = {&root_source->row_shape.source, root_source, request};
    RelKeyCtx r = {right_plan, right_output, request};
    BiKeyCtx c = {"join contribution membership only lowers equality predicates",
                  "join contribution membership must compare root fields to relation output fields",
                  {resolve_join_key, &l}, {resolve_relation_key, &r}};
    return lower_key_pairs(predicate, "join contribution membership requires at least one equality predicate", &c, left_keys, right_keys);
}

static bool linear_ends_in_projection(const jazz_engine_LinearCurrentRoot *linear) {
    return linear->steps.len && linear->steps.ptr[linear->steps.len - 1].tag == JAZZ_ENGINE_STEP_PROJECT;
}

static jazz_engine_UrErr lower_named_relation_field(const jazz_engine_NormalizedValueRef *value, const jz_btree *fields, jz_str *out) {
    jz_str field;
    switch (value->tag) {
    case JAZZ_ENGINE_NVR_FRONTIER_COLUMN: field = value->u.frontier_column.field; break;
    case JAZZ_ENGINE_NVR_PARAM: field = value->u.param; break;
    case JAZZ_ENGINE_NVR_SOURCE_FIELD: field = value->u.source_field.field; break;
    case JAZZ_ENGINE_NVR_ROW_ID:
        if (value->sub == JAZZ_ENGINE_ROWID_FRONTIER) { field = jazz_engine__str_view(JZ_LIT("row_uuid")); break; }
        /* fallthrough */
    default: return JE_UR_OP("join relation key must be an output field");
    }
    if (jz_btree_contains(fields, &field)) { *out = s_clone(&field); return NULL; }
    return jazz_engine__ur_op(jz_format("join relation does not output field '%.*s'", JE_STR_ARG(field)));
}

jazz_engine_UrErr JE_GL(lower_relation_key_ref)(const jazz_engine_NormalizedValueRef *value, const jazz_engine_RelationInputPlan *plan,
                                                const LRI *output, jazz_engine_LoweringContext *request, jz_str *out) {
    if (plan->tag != JAZZ_ENGINE_RIP_LINEAR) return lower_named_relation_field(value, &output->fields, out);
    const jazz_engine_LinearCurrentRoot *linear = &plan->u.linear;
    if (linear_ends_in_projection(linear)) {
        jazz_engine_UrErr e = lower_named_relation_field(value, &output->fields, out);
        if (!e) return NULL;
        jazz_engine__ur_free(e);
    }
    const jazz_engine_SourceId *root_source = JE_PLAN(LinearRoot__source)(&linear->root);
    if (linear->steps.len && linear->steps.ptr[linear->steps.len - 1].tag == JAZZ_ENGINE_STEP_PROJECT &&
        value->tag == JAZZ_ENGINE_NVR_ROW_ID && value->sub == JAZZ_ENGINE_ROWID_SOURCE && root_source && sid_eq(root_source, &value->u.source)) {
        const jazz_engine_RowProjectionVec *columns = &linear->steps.ptr[linear->steps.len - 1].u.project;
        JZ_VEC_FOR(const jazz_engine_RowProjection, column, *columns) {
            if (column->value.tag == JAZZ_ENGINE_NVR_ROW_ID && column->value.sub == JAZZ_ENGINE_ROWID_SOURCE && sid_eq(&column->value.u.source, &value->u.source)) {
                /* Relation-backed policy expressions commonly project their
                 * source row ID as public `id`. Subsequent outer joins must
                 * consume that projected private proof field, rather than
                 * asking the graph for the pre-projection `row_uuid` name. */
                *out = s_clone(&column->output.name);
                return NULL;
            }
        }
    }
    if (output->has_root_source && root_source) {
        jazz_engine_UrErr e = lower_join_key_ref(value, root_source, &output->root_source, request, out);
        if (!e) return NULL;
        jazz_engine__ur_free(e);
    }
    return lower_named_relation_field(value, &output->fields, out);
}

/* ---------------------------------------------------------------- projections */
typedef struct LastJoinRight {
    const jazz_engine_RelationInputPlan *plan; /* borrowed (Rust clones it; only read) */
    jz_btree nullable_fields, nullable_field_depths, fields;
} LastJoinRight;
static void last_join_right_drop(LastJoinRight *r) {
    jz_btree_drop(&r->nullable_fields);
    jz_btree_drop(&r->nullable_field_depths);
    jz_btree_drop(&r->fields);
}

enum { PS_ARGUMENT, PS_FIELD, PS_LITERAL };
typedef struct ProjectionSource { u8 tag; groove_ProjectExpr argument; jz_str field; u32 nullable_depth; groove_LiteralValue literal; } ProjectionSource;

typedef struct ProjectionFieldPlan {
    groove_ProjectField project;
    jz_btree unwrap_before_project; /* String -> usize */
    bool has_nullable_after_project; jz_str nullable_after_name; u32 nullable_after_depth;
    bool has_source_nullability; jz_str source_nullability_field; u32 source_nullability_depth;
} ProjectionFieldPlan;
static void projection_field_plan_drop(ProjectionFieldPlan *p) {
    PF(drop)(&p->project);
    jz_btree_drop(&p->unwrap_before_project);
    if (p->has_nullable_after_project) jz_str_drop(&p->nullable_after_name);
    if (p->has_source_nullability) jz_str_drop(&p->source_nullability_field);
}

static jazz_engine_UrErr lower_relation_projection_ref(const jazz_engine_NormalizedValueRef *value, const jazz_engine_RelationInputPlan *plan,
                                                       bool *has, jz_str *out) {
    *has = false;
    switch (plan->tag) {
    case JAZZ_ENGINE_RIP_LINEAR: {
        const jazz_engine_LinearCurrentRoot *linear = &plan->u.linear;
        if (linear->root.tag == JAZZ_ENGINE_LROOT_SOURCE) {
            const jazz_engine_SourceId *source_id = &linear->root.source;
            if (value->tag == JAZZ_ENGINE_NVR_SOURCE_FIELD && sid_eq(&value->u.source_field.source, source_id)) {
                *has = true;
                *out = F(user_column_field)(JZ_S(&value->u.source_field.field));
                return NULL;
            }
            if (value->tag == JAZZ_ENGINE_NVR_ROW_ID && value->sub == JAZZ_ENGINE_ROWID_SOURCE && sid_eq(&value->u.source, source_id)) {
                *has = true;
                *out = s_lit("row_uuid");
                return NULL;
            }
        }
        if (value->tag == JAZZ_ENGINE_NVR_PARAM) { *has = true; *out = s_clone(&value->u.param); }
        else if (value->tag == JAZZ_ENGINE_NVR_FRONTIER_COLUMN) { *has = true; *out = s_clone(&value->u.frontier_column.field); }
        return NULL;
    }
    case JAZZ_ENGINE_RIP_RECURSIVE:
        if (value->tag == JAZZ_ENGINE_NVR_FRONTIER_COLUMN && jz_str_eq(&value->u.frontier_column.frontier, &plan->u.recursive.frontier)) {
            *has = true;
            *out = s_clone(&value->u.frontier_column.field);
        } else if (value->tag == JAZZ_ENGINE_NVR_PARAM) {
            *has = true;
            *out = s_clone(&value->u.param);
        }
        return NULL;
    default:
        switch (value->tag) {
        case JAZZ_ENGINE_NVR_PARAM: *has = true; *out = s_clone(&value->u.param); break;
        case JAZZ_ENGINE_NVR_FRONTIER_COLUMN: *has = true; *out = s_clone(&value->u.frontier_column.field); break;
        case JAZZ_ENGINE_NVR_SOURCE_FIELD: *has = true; *out = s_clone(&value->u.source_field.field); break;
        case JAZZ_ENGINE_NVR_ROW_ID: *has = true; *out = s_lit("row_uuid"); break;
        default: break;
        }
        return NULL;
    }
}

/* false = Ok(None) */
static jazz_engine_UrErr lower_literal_projection_value(const jazz_engine_NormalizedValueRef *value, jazz_engine_LoweringContext *request,
                                                        bool *has, groove_LiteralValue *out) {
    *has = false;
    groove_Value v;
    switch (value->tag) {
    case JAZZ_ENGINE_NVR_TEMPLATE_LITERAL: JE_UR_TRY(JE_ARG(LoweringContext__literal)(request, value->u.template_literal, &v)); break;
    case JAZZ_ENGINE_NVR_LITERAL: JE_UR_TRY(decode_value(&value->u.literal, "literal value", &v)); break;
    case JAZZ_ENGINE_NVR_PARAM: {
        const groove_Value *b = jz_btree_get(&request->request->input.binding.values, &value->u.param);
        if (!b) return jazz_engine__ur_op(jz_format("binding parameter '%.*s' is not bound", JE_STR_ARG(value->u.param)));
        groove__records__values__Value__Clone__clone(&v, b);
        break;
    }
    default: return NULL;
    }
    *has = true;
    *out = lit_from_value(v);
    return NULL;
}

static jz_str field_after_join(jz_str field, const LastJoinRight *last_join_right) {
    if (!last_join_right) return field;
    jz_str f = F(left_field)(JZ_S(&field));
    jz_str_drop(&field);
    return f;
}

static jazz_engine_UrErr lower_projection_source(const jazz_engine_NormalizedValueRef *value, const jazz_engine_LinearCurrentRoot *plan,
                                                 const jazz_engine_ResolvedSource *source, const jz_btree *fields, const jz_btree *field_nullable_depths,
                                                 const LastJoinRight *last_join_right, const jz_btree *accumulated_join_fields,
                                                 jazz_engine_LoweringContext *request, ProjectionSource *out) {
    memset(out, 0, sizeof *out);
    jz_str field;
    jazz_engine_UrErr e = lower_linear_root_key_ref(value, &plan->root, source, request, &field);
    if (!e) {
        out->tag = PS_FIELD;
        out->nullable_depth = depth_get_or(field_nullable_depths, &field, 0);
        out->field = field_after_join(field, last_join_right);
        return NULL;
    }
    jazz_engine__ur_free(e);
    if (value->tag == JAZZ_ENGINE_NVR_PARAM && jz_btree_contains(fields, &value->u.param)) {
        out->tag = PS_FIELD;
        out->field = field_after_join(s_clone(&value->u.param), last_join_right);
        return NULL;
    }
    const AccVal *acc = accumulated_join_field(value, accumulated_join_fields);
    if (acc) {
        out->tag = PS_FIELD;
        out->nullable_depth = depth_get_or(field_nullable_depths, &acc->field, 0);
        out->field = field_after_join(s_clone(&acc->field), last_join_right);
        return NULL;
    }
    if (last_join_right) {
        bool has;
        jz_str rf;
        JE_UR_TRY(lower_relation_projection_ref(value, last_join_right->plan, &has, &rf));
        if (has) {
            out->tag = PS_FIELD;
            out->nullable_depth = jz_btree_contains(&last_join_right->nullable_fields, &rf) ? depth_get_or(&last_join_right->nullable_field_depths, &rf, 1) : 0;
            out->field = F(right_field)(JZ_S(&rf));
            jz_str_drop(&rf);
            return NULL;
        }
    }
    if (value->tag == JAZZ_ENGINE_NVR_PARAM) {
        const groove_ValueType *ty = jz_btree_get(&request->request->input.binding.param_types, &value->u.param);
        if (ty && JE_ARG(LoweringContext__scalar_argument)(request, jazz_engine__nvr_cloned(value), groove_vt_clone(ty), false, &out->argument)) {
            out->tag = PS_ARGUMENT;
            return NULL;
        }
    }
    if (value->tag == JAZZ_ENGINE_NVR_TEMPLATE_LITERAL) {
        groove_ValueType ty;
        if (JE_ARG(LoweringContext__literal_type)(request, value->u.template_literal, &ty) &&
            JE_ARG(LoweringContext__scalar_argument)(request, jazz_engine__nvr_cloned(value), ty, false, &out->argument)) {
            out->tag = PS_ARGUMENT;
            return NULL;
        }
    }
    bool has;
    JE_UR_TRY(lower_literal_projection_value(value, request, &has, &out->literal));
    if (!has) return JE_UR_OP("project value must reference the current root, last join input, or a literal");
    out->tag = PS_LITERAL;
    return NULL;
}

static jazz_engine_UrErr lower_projection_field(const jazz_engine_RowProjection *column, const jazz_engine_LinearCurrentRoot *plan,
                                                const jazz_engine_ResolvedSource *source, const jz_btree *fields, const jz_btree *field_nullable_depths,
                                                const LastJoinRight *last_join_right, const jz_btree *accumulated_join_fields,
                                                jazz_engine_LoweringContext *request, ProjectionFieldPlan *out) {
    ProjectionSource ps;
    JE_UR_TRY(lower_projection_source(&column->value, plan, source, fields, field_nullable_depths, last_join_right, accumulated_join_fields, request, &ps));
    memset(out, 0, sizeof *out);
    depths_init(&out->unwrap_before_project);
    switch (ps.tag) {
    case PS_FIELD: {
        u32 output_depth = value_type_nullable_depth(&column->output.ty);
        u32 unwrap_depth = ps.nullable_depth > output_depth ? ps.nullable_depth - output_depth : 0;
        if (unwrap_depth > 0) depth_put(&out->unwrap_before_project, &ps.field, unwrap_depth);
        if (output_depth > 0) {
            out->has_nullable_after_project = true;
            out->nullable_after_name = s_clone(&column->output.name);
            out->nullable_after_depth = output_depth;
        }
        out->has_source_nullability = true;
        out->source_nullability_field = s_clone(&ps.field);
        out->source_nullability_depth = ps.nullable_depth;
        out->project = PF(renamed)(ps.field, s_clone(&column->output.name));
        break;
    }
    case PS_LITERAL: out->project = PF(literal)(s_clone(&column->output.name), ps.literal); break;
    default: {
        groove_ProjectField pf;
        memset(&pf, 0, sizeof pf);
        pf.expression = ps.argument;
        pf.output_name = s_clone(&column->output.name);
        pf.output_identity.tag = GROOVE_FIELD_ID_NAME;
        pf.output_identity.name = s_clone(&column->output.name);
        out->project = pf;
        break;
    }
    }
    return NULL;
}

/* ---------------------------------------------------------------- equality-parameter filter joins */
typedef struct EqualityParamJoin { jz_str field, param; groove_ValueType value_type; bool nullable; } EqualityParamJoin;
static void equality_param_join_drop(EqualityParamJoin *j) { jz_str_drop(&j->field); jz_str_drop(&j->param); VT_DROP(&j->value_type); }

/* false = Ok(None) */
static jazz_engine_UrErr source_join_field(const jazz_engine_NormalizedValueRef *value, const jazz_engine_SourceId *source_id,
                                           const jazz_engine_ResolvedSource *source, bool *has, EqualityParamJoin *out) {
    *has = false;
    jz_str field;
    if (value->tag == JAZZ_ENGINE_NVR_SOURCE_FIELD && sid_eq(&value->u.source_field.source, source_id)) {
        jz_str user = F(user_column_field)(JZ_S(&value->u.source_field.field));
        jazz_engine_UrErr e = JE_GL(require_source_field)(source, JZ_S(&user), &field);
        jz_str_drop(&user);
        if (e) {
            jazz_engine__ur_free(e);
            JE_UR_TRY(JE_GL(require_source_field)(source, JZ_S(&value->u.source_field.field), &field));
        }
    } else if (value->tag == JAZZ_ENGINE_NVR_ROW_ID && value->sub == JAZZ_ENGINE_ROWID_SOURCE && sid_eq(&value->u.source, source_id)) {
        JE_UR_TRY(JE_GL(require_source_field)(source, JZ_S(&source->row_shape.row_uuid_field), &field));
    } else {
        return NULL;
    }
    const groove_ValueType *vt = JE_GL(source_field_type)(source, JZ_S(&field));
    if (!vt) {
        jz_str d = {0};
        jazz_engine__fmt__str(&d, &field);
        jazz_engine_UrErr e = jazz_engine__ur_rt(jz_format("source field %.*s is missing from resolved descriptor", JE_STR_ARG(d)));
        jz_str_drop(&d);
        jz_str_drop(&field);
        return e;
    }
    memset(out, 0, sizeof *out);
    out->field = field;
    if (vt->tag == GROOVE_VT_NULLABLE) { out->value_type = groove_vt_clone(vt->u.inner); out->nullable = true; }
    else out->value_type = groove_vt_clone(vt);
    *has = true;
    return NULL;
}

/* The binding slot an equality operand reads, when it has one.
 *
 * A claim operand is a binding slot too when the program carries that claim
 * as a prepared routing parameter (see `claim_binding_slot`). Joining it
 * against the binding source keeps the claim value out of the graph, so
 * every session with the same query shares one graph instead of compiling
 * its own literal copy (#4017). Any other claim stays a literal predicate. */
static jazz_engine_UrErr equality_binding_param(const jazz_engine_NormalizedValueRef *value, jazz_engine_LoweringContext *request, bool *has, jz_str *out) {
    *has = false;
    if (value->tag == JAZZ_ENGINE_NVR_PARAM) { *has = true; *out = s_clone(&value->u.param); return NULL; }
    if (value->tag == JAZZ_ENGINE_NVR_CLAIM && request->request->input.binding.has_source_shape) {
        const jazz_engine_ParameterDomain *domain;
        JE_UR_TRY(JE_ARG(LoweringContext__parameter_domain)(request, &domain));
        jz_str slot;
        if (JE_LOW(claim_binding_slot)(domain, &value->u.claim, &slot)) {
            if (jz_btree_contains(&domain->routing_params, &slot)) { *has = true; *out = slot; }
            else jz_str_drop(&slot);
        }
    }
    return NULL;
}

static jazz_engine_UrErr equality_param_join(const jazz_engine_PredicateExpr *predicate, const jazz_engine_SourceId *source_id,
                                             const jazz_engine_ResolvedSource *source, jazz_engine_LoweringContext *request,
                                             bool *has, EqualityParamJoin *out) {
    *has = false;
    if (predicate->tag != JAZZ_ENGINE_PRED_COMPARE || predicate->op != JAZZ_ENGINE_CMP_EQ) return NULL;
    bool hl, hr;
    EqualityParamJoin fl, fr;
    JE_UR_TRY(source_join_field(&predicate->u.pair.left, source_id, source, &hl, &fl));
    jazz_engine_UrErr e = source_join_field(&predicate->u.pair.right, source_id, source, &hr, &fr);
    if (e) { if (hl) equality_param_join_drop(&fl); return e; }
    EqualityParamJoin field;
    const jazz_engine_NormalizedValueRef *value;
    if (hl && !hr) { field = fl; value = &predicate->u.pair.right; }
    else if (!hl && hr) { field = fr; value = &predicate->u.pair.left; }
    else {
        if (hl) equality_param_join_drop(&fl);
        if (hr) equality_param_join_drop(&fr);
        return NULL;
    }
    bool hp;
    e = equality_binding_param(value, request, &hp, &field.param);
    if (e || !hp) { jz_str_drop(&field.field); VT_DROP(&field.value_type); return e; }
    *out = field;
    *has = true;
    return NULL;
}

static jazz_engine_UrErr lower_equality_param_filter_joins(groove_GraphBuilder *graph, const jazz_engine_PredicateExpr *predicate,
                                                           const jazz_engine_SourceId *source_id, const jazz_engine_ResolvedSource *source,
                                                           const jz_btree *available_route_fields, jazz_engine_LoweringContext *request,
                                                           groove_GraphBuilder **out_graph, jazz_engine_PredicateExpr *out_residual,
                                                           jz_btree *out_routes) {
    const jazz_engine_PredicateExpr *predicates = predicate;
    u32 n = 1;
    if (predicate->tag == JAZZ_ENGINE_PRED_AND) { predicates = predicate->u.children.ptr; n = predicate->u.children.len; }
    jazz_engine_PredicateVec residual = {0};
    jz_btree retained_route_fields;
    jazz_engine__strset_clone(&retained_route_fields, available_route_fields);
    jazz_engine_UrErr e = NULL;
    for (u32 i = 0; i < n; i++) {
        bool has;
        EqualityParamJoin join;
        if ((e = equality_param_join(&predicates[i], source_id, source, request, &has, &join))) goto fail;
        if (!has || !request->request->input.binding.has_source_shape) {
            if (has) equality_param_join_drop(&join);
            JZ_VEC_PUSH(residual, jazz_engine__pred_cloned(&predicates[i]));
            continue;
        }
        const jz_str *binding_source_shape = &request->request->input.binding.source_shape;
        const jazz_engine_ParameterDomain *domain;
        if ((e = JE_ARG(LoweringContext__parameter_domain)(request, &domain))) { equality_param_join_drop(&join); goto fail; }
        bool is_claim_param = jz_btree_contains(&domain->claim_params, &join.param);
        groove_RecordDescriptor binding_descriptor;
        if (is_claim_param) {
            e = binding_source_descriptor_with_user_params(request, NULL, &binding_descriptor);
        } else {
            jazz_engine_TypedOutputField extra = {s_clone(&join.param), groove_vt_clone(&join.value_type)};
            e = binding_source_descriptor_with_user_params(request, &extra, &binding_descriptor);
        }
        if (e) { equality_param_join_drop(&join); goto fail; }
        jazz_engine_StrVec binding_fields = {0};
        u32 nf;
        const groove_DescriptorField *bf = groove__records__RecordDescriptor__fields(binding_descriptor, &nf);
        for (u32 j = 0; j < nf; j++) {
            if (!bf[j].has_name) {
                jazz_engine__strvec_drop(&binding_fields);
                equality_param_join_drop(&join);
                e = JE_UR_OP("binding fields must be named");
                goto fail;
            }
            jazz_engine__strvec_push_clone(&binding_fields, &bf[j].name);
        }
        groove_GraphBuilder *binding = GB(binding_source)(s_clone(binding_source_shape), binding_descriptor);
        jz_str route_field = is_claim_param ? s_clone(&join.param) : F(route_param_field)(JZ_S(&join.param));
        bool route_is_nullable = false;
        const jazz_engine_ClaimParameter *claim = jz_btree_get(&domain->claim_params, &join.param);
        if (claim) route_is_nullable = claim->ty.tag == GROOVE_VT_NULLABLE;
        else {
            const groove_ValueType *ty = jz_btree_get(&domain->user_params, &join.param);
            route_is_nullable = ty && ty->tag == GROOVE_VT_NULLABLE;
        }
        /* Equality unwrapping must not become the route carrier's value
         * conversion. Keep a second, untouched copy of a nullable binding for
         * the policy-arm union and for the downstream result-membership
         * window, then unwrap only the copy used as the equality key. */
        bool has_carrier = join.nullable && route_is_nullable;
        jz_str route_carrier = {0};
        if (has_carrier) {
            route_carrier = jz_format("__jazz_route_carrier:%.*s", JE_STR_ARG(join.param));
            groove_ProjectFieldVec fields = {0};
            JZ_VEC_FOR(jz_str, f, binding_fields) JZ_VEC_PUSH(fields, PF(named)(s_clone(f)));
            JZ_VEC_PUSH(fields, PF(renamed)(s_clone(&join.param), s_clone(&route_carrier)));
            binding = GB(project_fields)(binding, fields);
        }
        jazz_engine__strvec_drop(&binding_fields);
        groove_ProjectFieldVec projection = JE_TERM(project_source_fields_from_prefix_rewrapping_nullable)(source, JAZZ_ENGINE_LEFT_JOIN_PREFIX,
                                                                                                            join.nullable ? &join.field : NULL);
        const jz_str *rf; const void *unit;
        JZ_BTREE_FOR(&retained_route_fields, rf, unit) JZ_VEC_PUSH(projection, PF(renamed)(F(left_field)(JZ_S(rf)), s_clone(rf)));
        JZ_VEC_PUSH(projection, PF(renamed)(F(right_field)(JZ_S(has_carrier ? &route_carrier : &join.param)), s_clone(&route_field)));
        if (has_carrier) jz_str_drop(&route_carrier);
        if (join.nullable) {
            graph = GB(unwrap_nullable)(graph, s_clone(&join.field));
            binding = GB(unwrap_nullable)(binding, s_clone(&join.param));
        }
        graph = GB(project_fields)(JE_GL(policy_join_if_needed)(graph, binding, jazz_engine__strvec_one(join.field), jazz_engine__strvec_one(join.param), request),
                                   projection);
        VT_DROP(&join.value_type);
        jazz_engine__btree_set_add(&retained_route_fields, &route_field);
    }
    memset(out_residual, 0, sizeof *out_residual);
    if (residual.len == 0) {
        out_residual->tag = JAZZ_ENGINE_PRED_TRUE;
        jz_vec_drop(&residual, sizeof(jazz_engine_PredicateExpr), NULL);
    } else if (residual.len == 1) {
        *out_residual = residual.ptr[0];
        jz_vec_drop(&residual, sizeof(jazz_engine_PredicateExpr), NULL);
    } else {
        out_residual->tag = JAZZ_ENGINE_PRED_AND;
        out_residual->u.children = residual;
    }
    *out_graph = graph;
    *out_routes = retained_route_fields;
    return NULL;
fail:
    groove_gb_drop(graph);
    jz_vec_drop(&residual, sizeof(jazz_engine_PredicateExpr), jazz_engine__pred_drop_v);
    jz_btree_drop(&retained_route_fields);
    return e;
}

static groove_ProjectFieldVec project_left_source_fields_with_join_routes(const jazz_engine_ResolvedSource *source, const jz_btree *existing_route_fields,
                                                                          const jz_btree *introduced_route_fields) {
    groove_ProjectFieldVec fields = JE_TERM(project_source_fields_from_prefix)(source, JAZZ_ENGINE_LEFT_JOIN_PREFIX);
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(existing_route_fields, f, unit) JZ_VEC_PUSH(fields, PF(renamed)(F(left_field)(JZ_S(f)), s_clone(f)));
    JZ_BTREE_FOR(introduced_route_fields, f, unit) JZ_VEC_PUSH(fields, PF(renamed)(F(right_field)(JZ_S(f)), s_clone(f)));
    return fields;
}

/* ---------------------------------------------------------------- windows, aggregates, order keys */
static jazz_engine_UrErr lower_order_key(const jazz_engine_OrderKey *key, const jazz_engine_LinearCurrentRoot *plan, const jazz_engine_ResolvedSource *source,
                                         jazz_engine_LoweringContext *request, groove_TopByOrder *out) {
    jz_str lowered;
    JE_UR_TRY(lower_field_ref(&key->value, plan, source, request, "order key", &lowered));
    bool relation_output = false;
    const jazz_engine_RowSetOutputRequest *output = &request->request->output;
    if (output->has_app_rows && output->app_rows.projection.tag == JAZZ_ENGINE_PP_RELATION) {
        JZ_VEC_FOR(const jazz_engine_LinearStep, step, plan->steps) if (step->tag == JAZZ_ENGINE_STEP_PROJECT) { relation_output = true; break; }
    }
    const groove_DescriptorField *window = JE_TERM(collect_window_source_field)(source, &key->value);
    groove_FieldRef field;
    if (relation_output && window) {
        u32 idx;
        if (!JE_GL(resolved_source_descriptor_index)(source, JZ_S(&lowered), &idx)) {
            jz_str d = {0};
            jazz_engine__fmt__str(&d, &lowered);
            jazz_engine_UrErr e = jazz_engine__ur_op(jz_format("resolved order key field %.*s is missing from the source descriptor", JE_STR_ARG(d)));
            jz_str_drop(&d);
            jz_str_drop(&lowered);
            return e;
        }
        field = groove__ivm__graph__FieldRef__resolved(idx);
        jz_str_drop(&lowered);
    } else if (window) {
        if (!window->has_name) jz_panic("window fields are named");
        field = groove__ivm__graph__FieldRef__stored_name(s_clone(&window->name));
        jz_str_drop(&lowered);
    } else {
        field = groove__ivm__graph__FieldRef__name(lowered);
    }
    out->field = field;
    out->direction = key->direction == JAZZ_ENGINE_SORT_ASC ? GROOVE_DIR_ASC : GROOVE_DIR_DESC;
    return NULL;
}

static jazz_engine_UrErr lower_field_refs(const jazz_engine_NvrVec *values, const jazz_engine_LinearCurrentRoot *plan, const jazz_engine_ResolvedSource *source,
                                          jazz_engine_LoweringContext *request, const char *context, jazz_engine_StrVec *out) {
    jazz_engine_StrVec v = {0};
    JZ_VEC_FOR(const jazz_engine_NormalizedValueRef, value, *values) {
        jz_str f;
        jazz_engine_UrErr e = lower_field_ref(value, plan, source, request, context, &f);
        if (e) { jazz_engine__strvec_drop(&v); return e; }
        JZ_VEC_PUSH(v, f);
    }
    *out = v;
    return NULL;
}

static jazz_engine_UrErr lower_window(groove_GraphBuilder *graph, const jazz_engine_OrderKeyVec *order, const jazz_engine_NvrVec *partition_by,
                                      const jz_btree *route_fields, bool has_limit, u32 limit, u32 offset, const jazz_engine_NvrVec *tie_breaker,
                                      const jazz_engine_LinearCurrentRoot *plan, const jazz_engine_ResolvedSource *source,
                                      jazz_engine_LoweringContext *request, groove_GraphBuilder **out) {
    jazz_engine_StrVec group_cols, tie_candidates;
    jazz_engine_UrErr e = lower_field_refs(partition_by, plan, source, request, "slice partition key", &group_cols);
    if (e) { groove_gb_drop(graph); return e; }
    /* One maintained graph serves every active binding. A window must therefore
     * be independent for each route tuple before the runtime filters its sinks. */
    const jz_str *route_field; const void *unit;
    JZ_BTREE_FOR(route_fields, route_field, unit)
        if (!jazz_engine__strvec_contains(&group_cols, route_field)) jazz_engine__strvec_push_clone(&group_cols, route_field);
    groove_TopByOrderVec order_cols = {0};
    JZ_VEC_FOR(const jazz_engine_OrderKey, key, *order) {
        groove_TopByOrder o;
        if ((e = lower_order_key(key, plan, source, request, &o))) goto fail;
        JZ_VEC_PUSH(order_cols, o);
    }
    if (tie_breaker->len == 0) {
        tie_candidates = jazz_engine__strvec_one(s_clone(&source->row_shape.row_uuid_field));
    } else if ((e = lower_field_refs(tie_breaker, plan, source, request, "slice tie-breaker", &tie_candidates))) {
        goto fail;
    }
    jazz_engine_StrVec tie_cols = {0};
    JZ_VEC_FOR(jz_str, tie, tie_candidates) {
        groove_FieldRef probe = groove__ivm__graph__FieldRef__name(s_clone(tie));
        bool ordered = false;
        JZ_VEC_FOR(const groove_TopByOrder, o, order_cols) if (groove__ivm__graph__FieldRef__PartialEq__eq(&o->field, &probe)) { ordered = true; break; }
        groove__ivm__graph__FieldRef__drop(&probe);
        if (ordered) jz_str_drop(tie);
        else JZ_VEC_PUSH(tie_cols, *tie);
    }
    jz_vec_drop(&tie_candidates, sizeof(jz_str), NULL);
    groove_TopByLimit top_by_limit = {has_limit ? GROOVE_TOPBY_FINITE : GROOVE_TOPBY_UNBOUNDED, has_limit ? (u64)limit : 0};
    if (order->len == 0 && offset == 0 && has_limit && limit == 1) {
        jz_vec_drop(&order_cols, sizeof(groove_TopByOrder), groove__ivm__graph__TopByOrder__drop_v);
        *out = GB(arg_min_by)(graph, group_cols, tie_cols);
        return NULL;
    }
    *out = GB(top_by)(graph, group_cols, order_cols, tie_cols, (u64)offset, top_by_limit);
    return NULL;
fail:
    groove_gb_drop(graph);
    jazz_engine__strvec_drop(&group_cols);
    jz_vec_drop(&order_cols, sizeof(groove_TopByOrder), groove__ivm__graph__TopByOrder__drop_v);
    return e;
}

static jazz_engine_UrErr lower_aggregate_expr(const jazz_engine_AggregateExpr *aggregate, const jazz_engine_LinearCurrentRoot *plan,
                                              const jazz_engine_ResolvedSource *source, jazz_engine_LoweringContext *request, groove_AggregateExpr *out) {
    memset(out, 0, sizeof *out);
    if (aggregate->has_input) {
        jz_str carrier;
        JE_UR_TRY(lower_field_ref(&aggregate->input, plan, source, request, "aggregate input", &carrier));
        u32 n, index = 0;
        const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(source->row_shape.descriptor, &n);
        bool found = false;
        for (u32 i = 0; i < n; i++) if (fields[i].has_name && jz_str_eq(&fields[i].name, &carrier)) { index = i; found = true; break; }
        if (!found) {
            jz_str d = {0};
            jazz_engine__fmt__str(&d, &carrier);
            jazz_engine_UrErr e = jazz_engine__ur_op(jz_format("aggregate input carrier %.*s is missing from resolved source", JE_STR_ARG(d)));
            jz_str_drop(&d);
            jz_str_drop(&carrier);
            return e;
        }
        /* PlanExpr carries a name. Preserve the selected source slot by
         * choosing a spelling that its logical-then-carrier resolver will
         * bind back to that exact slot, including user-prefix collisions. */
        const groove_DescriptorField *field = &fields[index];
        const jz_str *candidates[2] = {field->has_name ? &field->name : NULL, groove__records__DescriptorField__logical_name(field)};
        const jz_str *name = NULL;
        for (u32 i = 0; i < 2 && !name; i++) {
            u32 idx;
            if (candidates[i] && JE_GL(resolved_source_descriptor_index)(source, JZ_S(candidates[i]), &idx) && idx == index) name = candidates[i];
        }
        if (!name) {
            jz_str d = {0};
            jazz_engine__fmt__str(&d, &carrier);
            jazz_engine_UrErr e = jazz_engine__ur_op(jz_format("aggregate input carrier %.*s has no unambiguous expression name", JE_STR_ARG(d)));
            jz_str_drop(&d);
            jz_str_drop(&carrier);
            return e;
        }
        jz_str_drop(&carrier);
        out->has_expression = true;
        out->expression = groove__ivm__op_types__PlanExpr__field(s_clone(name));
    }
    switch (aggregate->function) {
    case JAZZ_ENGINE_AGG_COUNT: out->function = GROOVE_AGG_COUNT; break;
    case JAZZ_ENGINE_AGG_SUM: out->function = GROOVE_AGG_SUM; break;
    case JAZZ_ENGINE_AGG_AVG: out->function = GROOVE_AGG_AVG; break;
    case JAZZ_ENGINE_AGG_MIN: out->function = GROOVE_AGG_MIN; break;
    default: out->function = GROOVE_AGG_MAX; break;
    }
    out->distinct = false;
    out->has_output_name = true;
    out->output_name = F(aggregate_output_field)(JZ_S(&aggregate->output.name));
    out->has_output_identity = true;
    out->output_identity.tag = GROOVE_FIELD_ID_NAME;
    out->output_identity.name = F(aggregate_output_field)(JZ_S(&aggregate->output.name));
    return NULL;
}

/* consumes graph */
static jazz_engine_UrErr lower_aggregate(groove_GraphBuilder *graph, const jazz_engine_NvrVec *group_by, const jazz_engine_AggregateExprVec *outputs,
                                         const jazz_engine_LinearCurrentRoot *plan, const jazz_engine_ResolvedSource *source,
                                         jazz_engine_LoweringContext *request, LRI *out) {
    jazz_engine_StrVec group_cols;
    groove_AggregateExprVec aggregates = {0};
    jz_btree unwrap_fields;
    strset_init(&unwrap_fields);
    jazz_engine_UrErr e = lower_field_refs(group_by, plan, source, request, "aggregate group key", &group_cols);
    if (e) { groove_gb_drop(graph); jz_btree_drop(&unwrap_fields); return e; }
    JZ_VEC_FOR(const jazz_engine_AggregateExpr, aggregate, *outputs) {
        groove_AggregateExpr a;
        if ((e = lower_aggregate_expr(aggregate, plan, source, request, &a))) goto fail;
        JZ_VEC_PUSH(aggregates, a);
    }
    JZ_VEC_FOR(const jz_str, field, group_cols) if (source_field_is_nullable(source, JZ_S(field))) jazz_engine__strset_insert_clone(&unwrap_fields, field);
    JZ_VEC_FOR(const jazz_engine_AggregateExpr, aggregate, *outputs) {
        if (!aggregate->has_input) continue;
        jz_str field;
        if ((e = lower_field_ref(&aggregate->input, plan, source, request, "aggregate input", &field))) goto fail;
        if (source_field_is_nullable(source, JZ_S(&field))) jazz_engine__btree_set_add(&unwrap_fields, &field);
        else jz_str_drop(&field);
    }
    jz_str uf;
    while (jz_btree_pop_first(&unwrap_fields, &uf, NULL)) {
        groove_GraphBuilder node;
        memset(&node, 0, sizeof node);
        node.tag = GROOVE_GB_UNWRAP_NULLABLE;
        node.u.unwrap_nullable.input = graph;
        node.u.unwrap_nullable.field = groove__ivm__graph__FieldRef__stored_name(uf);
        graph = gb_node(node);
    }
    jz_btree_drop(&unwrap_fields);
    lri_empty(out, NULL);
    JZ_VEC_FOR(const jz_str, field, group_cols) jazz_engine__strset_insert_clone(&out->fields, field);
    JZ_VEC_FOR(const jazz_engine_AggregateExpr, aggregate, *outputs) {
        jz_str f = F(aggregate_output_field)(JZ_S(&aggregate->output.name));
        jazz_engine__btree_set_add(&out->fields, &f);
    }
    groove_GraphBuilder node;
    memset(&node, 0, sizeof node);
    node.tag = GROOVE_GB_AGGREGATE;
    node.u.aggregate.input = graph;
    node.u.aggregate.group_cols = field_refs_stored(group_cols);
    node.u.aggregate.aggregates = aggregates;
    out->graph = gb_node(node);
    out->has_root_source = true;
    JE_RES(ResolvedSource__Clone__clone)(&out->root_source, source);
    return NULL;
fail:
    groove_gb_drop(graph);
    jazz_engine__strvec_drop(&group_cols);
    jz_vec_drop(&aggregates, sizeof(groove_AggregateExpr), groove__ivm__op_types__AggregateExpr__drop_v);
    jz_btree_drop(&unwrap_fields);
    return e;
}

/* ---------------------------------------------------------------- linear plan lowering */
static uintptr_t relation_input_key(const jazz_engine_RelationInputPlan *plan) { return (uintptr_t)plan; }

static void strset_extend_clone(jz_btree *dst, const jz_btree *src) {
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(src, f, unit) jazz_engine__strset_insert_clone(dst, f);
}
/* {x in src | x in filter} */
static void strset_intersect(const jz_btree *src, const jz_btree *filter, jz_btree *out) {
    strset_init(out);
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(src, f, unit) if (jz_btree_contains(filter, f)) jazz_engine__strset_insert_clone(out, f);
}
static void strset_difference(const jz_btree *src, const jz_btree *minus, jz_btree *out) {
    strset_init(out);
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(src, f, unit) if (!jz_btree_contains(minus, f)) jazz_engine__strset_insert_clone(out, f);
}
static void str_u32_clone(jz_btree *dst, const jz_btree *src) { jz_btree_clone(dst, src, jazz_engine__str_clone_v, NULL); }
static bool is_binding_route(const jz_str *field) {
    jz_slice route;
    if (F(route_param_from_field)(JZ_S(field), &route)) return true;
    jazz_engine_ClaimPath claim;
    if (F(claim_path_from_param_field)(JZ_S(field), &claim)) { JE_IN(ClaimPath__drop)(&claim); return true; }
    return false;
}
static jz_slice route_param_or_field(const jz_str *field) {
    jz_slice route;
    return F(route_param_from_field)(JZ_S(field), &route) ? route : JZ_S(field);
}

typedef struct LinearState {
    groove_GraphBuilder *graph;
    jz_btree fields, nullable_fields, nullable_field_depths;
    const jazz_engine_OrderKeyVec *pending_order; /* NULL = None (borrowed; Rust clones the keys) */
    bool has_last_join_right; LastJoinRight last_join_right;
    jz_btree accumulated_join_fields;             /* (SourceId, String) -> (String, usize) */
    jz_btree accumulated_union_occurrence_fields; /* BTreeSet<String> */
    jz_btree available_route_fields;              /* BTreeSet<String> */
} LinearState;
static void clear_last_join_right(LinearState *st) {
    if (st->has_last_join_right) last_join_right_drop(&st->last_join_right);
    st->has_last_join_right = false;
}
static void linear_state_drop(LinearState *st) {
    groove_gb_drop(st->graph);
    jz_btree_drop(&st->fields);
    jz_btree_drop(&st->nullable_fields);
    jz_btree_drop(&st->nullable_field_depths);
    clear_last_join_right(st);
    jz_btree_drop(&st->accumulated_join_fields);
    jz_btree_drop(&st->accumulated_union_occurrence_fields);
    jz_btree_drop(&st->available_route_fields);
}

typedef struct LinearEnv {
    const jazz_engine_LinearCurrentRoot *plan;
    const jazz_engine_ResolvedSource *root_source;
    const jz_btree *resolved_sources;
    jazz_engine_LoweringContext *request;
    const jz_btree *lowered;                     /* NULL = None */
    const jazz_engine_LinearCurrentRoot *cache_plan; /* NULL = None */
    bool retain_final_project_input_fields;
    const jz_btree *route_fields;
} LinearEnv;

/* graph = lower_window(graph, order, [], routes, None, 0, [RowId(Source(root))]) */
static jazz_engine_UrErr lower_order_fallback(LinearState *st, const LinearEnv *env, const jazz_engine_OrderKeyVec *order) {
    const jazz_engine_SourceId *root = JE_PLAN(LinearRoot__source)(&env->plan->root);
    if (!root) return JE_UR_OP("order fallback must be a source");
    jazz_engine_NvrVec partition = {0}, tie = {0};
    jazz_engine_NormalizedValueRef row;
    memset(&row, 0, sizeof row);
    row.tag = JAZZ_ENGINE_NVR_ROW_ID;
    row.sub = JAZZ_ENGINE_ROWID_SOURCE;
    sid_clone(&row.u.source, root);
    JZ_VEC_PUSH(tie, row);
    groove_GraphBuilder *graph = st->graph;
    st->graph = NULL;
    jazz_engine_UrErr e = lower_window(graph, order, &partition, &st->available_route_fields, false, 0, 0, &tie, env->plan, env->root_source, env->request, &st->graph);
    jazz_engine__nvrvec_drop(&tie);
    return e;
}

static jazz_engine_UrErr linear_filter_step(LinearState *st, const LinearEnv *env, const jazz_engine_PredicateExpr *predicate) {
    clear_last_join_right(st);
    const jazz_engine_SourceId *source = JE_PLAN(LinearRoot__source)(&env->plan->root);
    if (!source) return JE_UR_OP("filters on value/frontier sources are not lowered yet");
    groove_GraphBuilder *graph = st->graph;
    st->graph = NULL;
    jazz_engine_PredicateExpr residual;
    jz_btree introduced;
    JE_UR_TRY(lower_equality_param_filter_joins(graph, predicate, source, env->root_source, &st->available_route_fields, env->request,
                                                &st->graph, &residual, &introduced));
    strset_extend_clone(&st->fields, &introduced);
    jz_str f;
    while (jz_btree_pop_first(&introduced, &f, NULL)) jazz_engine__btree_set_add(&st->available_route_fields, &f);
    jz_btree_drop(&introduced);
    jazz_engine_UrErr e = NULL;
    if (residual.tag != JAZZ_ENGINE_PRED_TRUE) {
        GPred lowered;
        e = JE_GL(lower_predicate)(&residual, source, env->root_source, env->request, &lowered);
        if (!e) st->graph = uses_policy_value_comparison(env->request) ? GB(policy_filter)(st->graph, lowered) : GB(filter)(st->graph, lowered);
    }
    pred_drop(&residual);
    return e;
}

static void unwrap_nullable_keys(groove_GraphBuilder **graph, const jazz_engine_StrVec *keys, const jz_btree *nullable_fields,
                                 const jz_btree *depths, jz_btree *unwrapped) {
    strset_init(unwrapped);
    JZ_VEC_FOR(const jz_str, key, *keys) {
        if (!jz_btree_contains(nullable_fields, key)) continue;
        jz_str k = s_clone(key);
        if (!jazz_engine__btree_set_add(unwrapped, &k)) continue;
        *graph = JE_GL(unwrap_nullable_join_key)(*graph, s_clone(key), depth_get_or(depths, key, 1));
    }
}
static void remove_keys(const jz_btree *keys, jz_btree *nullable_fields, jz_btree *depths) {
    const jz_str *k; const void *unit;
    JZ_BTREE_FOR(keys, k, unit) {
        jz_str ok; u32 od;
        if (jz_btree_remove(nullable_fields, k, &ok, NULL)) jz_str_drop(&ok);
        if (jz_btree_remove(depths, k, &ok, &od)) jz_str_drop(&ok);
    }
}

static jazz_engine_UrErr linear_join_step(LinearState *st, const LinearEnv *env, u32 step_index) {
    const jazz_engine_LinearCurrentRoot *plan = env->plan;
    const jazz_engine_LinearStep *step = &plan->steps.ptr[step_index];
    const jazz_engine_RelationInputPlan *right = step->u.join.right;
    u8 mode = step->u.join.mode;
    jazz_engine_LoweringContext *request = env->request;
    if (mode != JAZZ_ENGINE_JOIN_INNER && mode != JAZZ_ENGINE_JOIN_SEMI) return JE_UR_OP("join_via only lowers inner/semi joins");
    LRI lowered_right;
    if (env->lowered) {
        const jazz_engine_RelationInputPlan *key_plan = right;
        if (env->cache_plan && step_index < env->cache_plan->steps.len && env->cache_plan->steps.ptr[step_index].tag == JAZZ_ENGINE_STEP_JOIN)
            key_plan = env->cache_plan->steps.ptr[step_index].u.join.right;
        uintptr_t key = relation_input_key(key_plan);
        const LRI *found = jz_btree_get(env->lowered, &key);
        if (!found) {
            JE_DBG(d, JE_PLAN(RelationInputPlan__Debug__fmt), right);
            jazz_engine_UrErr e = jazz_engine__ur_rt(jz_format("join relation input was not lowered at step %u: right=%.*s", step_index, JE_STR_ARG(d)));
            jz_str_drop(&d);
            return e;
        }
        JE_GL(LoweredRelationInput__Clone__clone)(&lowered_right, found);
    } else {
        JE_UR_TRY(JE_GL(lower_relation_input)(right, env->resolved_sources, request, &lowered_right));
    }
    jazz_engine_StrVec left_keys = {0}, right_keys = {0};
    jz_btree unwrapped_left = {0}, unwrapped_right = {0}, right_nullable = {0}, right_depths = {0};
    groove_GraphBuilder *right_graph = NULL;
    jazz_engine_UrErr e = lower_linear_join_key_pairs(&step->u.join.on, &plan->root, env->root_source, right, &lowered_right,
                                                      &st->accumulated_join_fields, request, &left_keys, &right_keys);
    if (e) goto out;
    unwrap_nullable_keys(&st->graph, &left_keys, &st->nullable_fields, &st->nullable_field_depths, &unwrapped_left);
    remove_keys(&unwrapped_left, &st->nullable_fields, &st->nullable_field_depths);
    jazz_engine__strset_clone(&right_nullable, &lowered_right.nullable_fields);
    str_u32_clone(&right_depths, &lowered_right.nullable_field_depths);
    right_graph = lri_take_graph(&lowered_right);
    unwrap_nullable_keys(&right_graph, &right_keys, &lowered_right.nullable_fields, &lowered_right.nullable_field_depths, &unwrapped_right);
    remove_keys(&unwrapped_right, &right_nullable, &right_depths);

    if (mode == JAZZ_ENGINE_JOIN_SEMI) {
        /* Route each left occurrence before applying the existence gate. A Groove
         * semi-join preserves left multiplicity and collapses every matching right
         * derivation, while carrying the route on the left keeps prepared
         * identities distinct. */
        jz_btree right_route_fields, missing_route_fields;
        strset_intersect(env->route_fields, &lowered_right.fields, &right_route_fields);
        strset_difference(&right_route_fields, &st->available_route_fields, &missing_route_fields);
        if (missing_route_fields.len) {
            if (request->request->input.binding.has_source_shape) {
                groove_RecordDescriptor descriptor;
                if ((e = binding_source_descriptor_with_user_params(request, NULL, &descriptor))) {
                    jz_btree_drop(&right_route_fields);
                    jz_btree_drop(&missing_route_fields);
                    goto out;
                }
                groove_GraphBuilder *binding = GB(binding_source)(s_clone(&request->request->input.binding.source_shape), descriptor);
                jazz_engine_StrVec existing_route_fields = {0}, binding_route_fields = {0};
                const jz_str *f; const void *unit;
                JZ_BTREE_FOR(&st->available_route_fields, f, unit) {
                    if (!jz_btree_contains(&right_route_fields, f)) continue;
                    jazz_engine__strvec_push_clone(&existing_route_fields, f);
                    JZ_VEC_PUSH(binding_route_fields, jz_str_from_slice(route_param_or_field(f)));
                }
                groove_ProjectFieldVec projection = {0};
                JZ_BTREE_FOR(&st->fields, f, unit) JZ_VEC_PUSH(projection, PF(renamed)(F(left_field)(JZ_S(f)), s_clone(f)));
                JZ_BTREE_FOR(&missing_route_fields, f, unit) JZ_VEC_PUSH(projection, PF(renamed)(F(right_field)(route_param_or_field(f)), s_clone(f)));
                st->graph = GB(project_fields)(JE_GL(policy_join_if_needed)(st->graph, binding, existing_route_fields, binding_route_fields, request), projection);
            } else {
                groove_ProjectFieldVec projection = {0};
                const jz_str *f; const void *unit;
                JZ_BTREE_FOR(&st->fields, f, unit) JZ_VEC_PUSH(projection, PF(named)(s_clone(f)));
                JZ_BTREE_FOR(&missing_route_fields, f, unit) {
                    groove_ProjectField pf;
                    if ((e = JE_TERM(route_literal_project_field)(JZ_S(f), request, &pf))) {
                        pfvec_drop(&projection);
                        jz_btree_drop(&right_route_fields);
                        jz_btree_drop(&missing_route_fields);
                        goto out;
                    }
                    JZ_VEC_PUSH(projection, pf);
                }
                st->graph = GB(project_fields)(st->graph, projection);
            }
            strset_extend_clone(&st->fields, &missing_route_fields);
            strset_extend_clone(&st->available_route_fields, &missing_route_fields);
        }
        jz_btree_drop(&missing_route_fields);
        jazz_engine_StrVec right_fields;
        jazz_engine__strvec_clone(&right_fields, &right_keys);
        jz_str f;
        while (jz_btree_pop_first(&right_route_fields, &f, NULL)) {
            if (jazz_engine__strvec_contains(&right_keys, &f)) { jz_str_drop(&f); continue; }
            jazz_engine__strvec_push_clone(&right_fields, &f);
            jazz_engine__strvec_push_clone(&left_keys, &f);
            JZ_VEC_PUSH(right_keys, f);
        }
        jz_btree_drop(&right_route_fields);
        groove_ProjectFieldVec rproj = {0};
        JZ_VEC_FOR(jz_str, rf, right_fields) JZ_VEC_PUSH(rproj, PF(named)(*rf));
        jz_vec_drop(&right_fields, sizeof(jz_str), NULL);
        groove_GraphBuilder *rg = GB(project_fields)(right_graph, rproj);
        right_graph = NULL;
        st->graph = semi_join_if_needed(st->graph, rg, left_keys, right_keys, request);
        memset(&left_keys, 0, sizeof left_keys);
        memset(&right_keys, 0, sizeof right_keys);
        clear_last_join_right(st);
    } else {
        /* One prepared graph serves every binding of its source. When both sides
         * carry the same binding route, a left row must only meet right rows
         * derived for its own binding; joining on the relation keys alone would
         * pair one binding's row with another binding's proof (#4017). */
        const jz_str *f; const void *unit;
        JZ_BTREE_FOR(&st->available_route_fields, f, unit) {
            if (is_binding_route(f) && jz_btree_contains(&lowered_right.fields, f) && !jazz_engine__strvec_contains(&right_keys, f)) {
                jazz_engine__strvec_push_clone(&left_keys, f);
                jazz_engine__strvec_push_clone(&right_keys, f);
            }
        }
        st->graph = JE_GL(policy_join_if_needed)(st->graph, right_graph, left_keys, right_keys, request);
        right_graph = NULL;
        memset(&left_keys, 0, sizeof left_keys);
        memset(&right_keys, 0, sizeof right_keys);
        clear_last_join_right(st);
        st->has_last_join_right = true;
        st->last_join_right.plan = right;
        st->last_join_right.nullable_fields = right_nullable;
        st->last_join_right.nullable_field_depths = right_depths;
        jazz_engine__strset_clone(&st->last_join_right.fields, &lowered_right.fields);
        memset(&right_nullable, 0, sizeof right_nullable);
        memset(&right_depths, 0, sizeof right_depths);
    }
    bool next_is_project = step_index + 1 < plan->steps.len && plan->steps.ptr[step_index + 1].tag == JAZZ_ENGINE_STEP_PROJECT;
    bool next_is_join = step_index + 1 < plan->steps.len && plan->steps.ptr[step_index + 1].tag == JAZZ_ENGINE_STEP_JOIN;
    if (mode == JAZZ_ENGINE_JOIN_INNER && next_is_join) {
        /* Consecutive joins must retain the right input for the next predicate.
         * Public queries also use those values as occurrence carriers;
         * authorization subplans keep the same single lowering path, but mark
         * them internal so a decision terminal never asks for public row identity. */
        bool policy_subplan = omits_public_occurrence_carriers(request);
        if (!st->has_last_join_right) jz_panic("inner join records its right input");
        LastJoinRight taken = st->last_join_right;
        st->has_last_join_right = false;
        const jazz_engine_SourceId *right_source = JE_PLAN(RelationInputPlan__root_source)(right);
        if (!right_source) {
            last_join_right_drop(&taken);
            e = JE_UR_OP("multi-source flat join requires a source-rooted right input");
            goto out;
        }
        jz_btree union_occurrence_outputs;
        jz_btree_init(&union_occurrence_outputs, &jazz_engine__STR_STR_MAP_DESC);
        if (!policy_subplan && right->tag == JAZZ_ENGINE_RIP_UNION && lowered_right.has_union_occurrence_carrier) {
            jz_str k = s_clone(&lowered_right.union_label_field), v = jz_format("__root_join_arm_%u", step_index);
            jazz_engine__btree_put(&union_occurrence_outputs, &k, &v);
            k = s_clone(&lowered_right.union_row_field);
            v = jz_format("__root_join_row_%u", step_index);
            jazz_engine__btree_put(&union_occurrence_outputs, &k, &v);
        }
        groove_ProjectFieldVec projection = {0};
        const jz_str *f; const void *unit;
        JZ_BTREE_FOR(&st->fields, f, unit) JZ_VEC_PUSH(projection, PF(renamed)(F(left_field)(JZ_S(f)), s_clone(f)));
        jz_btree next_fields, next_nullable, next_depths;
        jazz_engine__strset_clone(&next_fields, &st->fields);
        jazz_engine__strset_clone(&next_nullable, &st->nullable_fields);
        str_u32_clone(&next_depths, &st->nullable_field_depths);
        /* A binding route the right input introduces must stay addressable under
         * its own name: the next join keys on it, so a later input pairs only
         * with rows derived for the same binding. Renaming it away would let a
         * chain whose left lacked the route pair one binding's proof with
         * another's (#4017). */
        JZ_BTREE_FOR(&taken.fields, f, unit) {
            if (is_binding_route(f) && !jz_btree_contains(&st->fields, f)) {
                JZ_VEC_PUSH(projection, PF(renamed)(F(right_field)(JZ_S(f)), s_clone(f)));
                jazz_engine__strset_insert_clone(&next_fields, f);
                jazz_engine__strset_insert_clone(&st->available_route_fields, f);
            }
        }
        JZ_BTREE_FOR(&taken.fields, f, unit) {
            const jz_str *mapped = jz_btree_get(&union_occurrence_outputs, f);
            jz_str output = mapped ? s_clone(mapped)
                          : policy_subplan ? jz_format("__policy_join_source_%u_%.*s", step_index, JE_STR_ARG(*f))
                                           : jz_format("__flat_join_source_%u_%.*s", step_index, JE_STR_ARG(*f));
            JZ_VEC_PUSH(projection, PF(renamed)(F(right_field)(JZ_S(f)), s_clone(&output)));
            if (mapped) jazz_engine__strset_insert_clone(&st->accumulated_union_occurrence_fields, &output);
            u32 nullable_depth = depth_get_or(&taken.nullable_field_depths, f, 0);
            AccKey ak = {jazz_engine__sid_cloned(right_source), s_clone(f)};
            AccVal av = {s_clone(&output), nullable_depth};
            jazz_engine__btree_put(&st->accumulated_join_fields, &ak, &av);
            jazz_engine__strset_insert_clone(&next_fields, &output);
            if (jz_btree_contains(&taken.nullable_fields, f)) {
                jazz_engine__strset_insert_clone(&next_nullable, &output);
                u32 d = nullable_depth > 1 ? nullable_depth : 1;
                jazz_engine__btree_put(&next_depths, &output, &d);
            } else {
                jz_str_drop(&output);
            }
        }
        jz_btree_drop(&union_occurrence_outputs);
        last_join_right_drop(&taken);
        st->graph = GB(project_fields)(st->graph, projection);
        jz_btree_drop(&st->fields);
        jz_btree_drop(&st->nullable_fields);
        jz_btree_drop(&st->nullable_field_depths);
        st->fields = next_fields;
        st->nullable_fields = next_nullable;
        st->nullable_field_depths = next_depths;
    }
    if (plan->root.tag == JAZZ_ENGINE_LROOT_SOURCE && !next_is_project && !next_is_join) {
        jz_btree introduced, retained;
        strset_intersect(env->route_fields, &lowered_right.fields, &introduced);
        jazz_engine__strset_clone(&retained, &st->available_route_fields);
        strset_extend_clone(&retained, &introduced);
        bool semi = mode == JAZZ_ENGINE_JOIN_SEMI;
        groove_ProjectFieldVec projection = semi ? JE_TERM(project_source_fields_with_routes)(env->root_source, &retained)
                                                 : project_left_source_fields_with_join_routes(env->root_source, &st->available_route_fields, &introduced);
        jz_btree_drop(&retained);
        jz_btree occurrence_fields;
        strset_init(&occurrence_fields);
        if (!omits_public_occurrence_carriers(request)) {
            /* Earlier consecutive UNION joins already flattened their complete
             * occurrence pair into the left input. Retain both fields under their
             * terminal names; row identity without its union arm is ambiguous. */
            const jz_str *output; const void *unit;
            JZ_BTREE_FOR(&st->accumulated_union_occurrence_fields, output, unit) {
                JZ_VEC_PUSH(projection, PF(renamed)(semi ? s_clone(output) : F(left_field)(JZ_S(output)), s_clone(output)));
                jazz_engine__strset_insert_clone(&occurrence_fields, output);
            }
            /* A trailing semi-join filters the complete public tuple but contributes
             * no occurrence of its own. Preserve the earlier inner-join row IDs from
             * the left input: terminals still use them to distinguish public root
             * occurrences. */
            const AccKey *ak; const AccVal *av;
            JZ_BTREE_FOR(&st->accumulated_join_fields, ak, av) {
                const jazz_engine_ResolvedSource *s = jz_btree_get(env->resolved_sources, &ak->source);
                if (s && jz_str_eq(&s->row_shape.row_uuid_field, &ak->field)) {
                    JZ_VEC_PUSH(projection, PF(renamed)(semi ? s_clone(&av->field) : F(left_field)(JZ_S(&av->field)), s_clone(&av->field)));
                    jazz_engine__strset_insert_clone(&occurrence_fields, &av->field);
                }
            }
        }
        if (mode == JAZZ_ENGINE_JOIN_INNER && !omits_public_occurrence_carriers(request)) {
            /* A union's root source is one of its arms, but its public occurrence
             * is the `(arm, row)` pair. Check the relation shape before inspecting
             * that source so an aliased common arm cannot make us drop the union
             * carrier that result-membership terminals require. */
            if (right->tag == JAZZ_ENGINE_RIP_UNION) {
                if (lowered_right.has_union_occurrence_carrier) {
                    jz_str arm_output = jz_format("__root_join_arm_%u", step_index), row_output = jz_format("__root_join_row_%u", step_index);
                    JZ_VEC_PUSH(projection, PF(renamed)(F(right_field)(JZ_S(&lowered_right.union_label_field)), s_clone(&arm_output)));
                    JZ_VEC_PUSH(projection, PF(renamed)(F(right_field)(JZ_S(&lowered_right.union_row_field)), s_clone(&row_output)));
                    jazz_engine__btree_set_add(&occurrence_fields, &arm_output);
                    jazz_engine__btree_set_add(&occurrence_fields, &row_output);
                }
            } else {
                const jazz_engine_SourceId *right_source_id = JE_PLAN(RelationInputPlan__root_source)(right);
                if (right_source_id) {
                    const jazz_engine_ResolvedSource *right_source = jz_btree_get(env->resolved_sources, right_source_id);
                    if (!right_source) {
                        e = ur_op_dbg_sid("inner join occurrence source %.*s was not resolved", right_source_id);
                        pfvec_drop(&projection);
                        jz_btree_drop(&occurrence_fields);
                        jz_btree_drop(&introduced);
                        goto out;
                    }
                    bool app_join_source = right_source_id->path.len && right_source_id->path.ptr[right_source_id->path.len - 1].tag == JAZZ_ENGINE_ROLE_ALIAS;
                    const jz_str *right_row = &right_source->row_shape.row_uuid_field;
                    if (app_join_source && st->has_last_join_right && jz_btree_contains(&st->last_join_right.fields, right_row)) {
                        jz_str output = jz_format("__root_join_row_%u", step_index);
                        JZ_VEC_PUSH(projection, PF(renamed)(F(right_field)(JZ_S(right_row)), s_clone(&output)));
                        jazz_engine__btree_set_add(&occurrence_fields, &output);
                    }
                }
            }
        }
        /* The chain's flattened records carried every joined field so this
         * projection could name any of them; drop the ones it did not. */
        st->graph = groove__ivm__graph__join_chains__GraphBuilder__narrow_projected_join_chain(GB(project_fields)(st->graph, projection));
        jz_btree_drop(&st->fields);
        strset_init(&st->fields);
        source_fields_into_set(env->root_source, &st->fields);
        strset_extend_clone(&st->fields, &st->available_route_fields);
        strset_extend_clone(&st->fields, &introduced);
        strset_extend_clone(&st->fields, &occurrence_fields);
        jz_btree_drop(&occurrence_fields);
        jz_btree_drop(&st->nullable_fields);
        jz_btree_drop(&st->nullable_field_depths);
        source_nullable_fields(env->root_source, &st->nullable_fields);
        source_nullable_field_depths(env->root_source, &st->nullable_field_depths);
        jz_str f;
        while (jz_btree_pop_first(&introduced, &f, NULL)) jazz_engine__btree_set_add(&st->available_route_fields, &f);
        jz_btree_drop(&introduced);
        clear_last_join_right(st);
    }
out:
    jazz_engine__strvec_drop(&left_keys);
    jazz_engine__strvec_drop(&right_keys);
    jz_btree_drop(&unwrapped_left);
    jz_btree_drop(&unwrapped_right);
    jz_btree_drop(&right_nullable);
    jz_btree_drop(&right_depths);
    groove_gb_drop(right_graph);
    JE_GL(LoweredRelationInput__drop)(&lowered_right);
    return e;
}

static void field_plans_drop(JZ_VEC(ProjectionFieldPlan) *v) {
    JZ_VEC_FOR(ProjectionFieldPlan, p, *v) projection_field_plan_drop(p);
    jz_vec_drop(v, sizeof(ProjectionFieldPlan), NULL);
}

static jazz_engine_UrErr linear_project_step(LinearState *st, const LinearEnv *env, u32 step_index, const jazz_engine_RowProjectionVec *columns) {
    const jazz_engine_LinearCurrentRoot *plan = env->plan;
    /* A contributor freezes the same rendered relation that membership predicates
     * name, but it must also retain the pre-projection physical row/version
     * fields needed to encode the admitted source occurrence. Capture the exact
     * fields available at this boundary before the facade aliases them. Bind
     * source order while the graph still has the source descriptor. A relation
     * output projection may narrow or rename those fields, and unbounded order
     * still determines the observable row sequence when no Slice follows. */
    if (st->pending_order) {
        const jazz_engine_OrderKeyVec *order = st->pending_order;
        st->pending_order = NULL;
        JE_UR_TRY(lower_order_fallback(st, env, order));
    }
    bool has_retained = env->retain_final_project_input_fields && step_index + 1 == plan->steps.len;
    jz_btree retained_fields = {0}, retained_depths = {0};
    if (has_retained) {
        jazz_engine__strset_clone(&retained_fields, &st->fields);
        str_u32_clone(&retained_depths, &st->nullable_field_depths);
    }
    jz_btree unwrap_fields, projected_nullable_field_depths, wrap_after_project;
    depths_init(&unwrap_fields);
    depths_init(&projected_nullable_field_depths);
    depths_init(&wrap_after_project);
    JZ_VEC(ProjectionFieldPlan) field_plans = {0};
    groove_ProjectFieldVec project_fields = {0};
    jazz_engine_UrErr e = NULL;
    const LastJoinRight *ljr = st->has_last_join_right ? &st->last_join_right : NULL;
    JZ_VEC_FOR(const jazz_engine_RowProjection, column, *columns) {
        ProjectionFieldPlan p;
        if ((e = lower_projection_field(column, plan, env->root_source, &st->fields, &st->nullable_field_depths, ljr,
                                        &st->accumulated_join_fields, env->request, &p))) goto fail;
        JZ_VEC_PUSH(field_plans, p);
    }
    const char *retained_prefix = ljr ? JAZZ_ENGINE_LEFT_JOIN_PREFIX : "";
    if (has_retained) {
        /* Retained source fields keep their pre-projection meaning for later
         * global order/window and contributor encoding. An output may reuse such
         * a name only as an identity copy; anything else would silently replace
         * the source value (e.g. a UNION ordered by a source column sorting by an
         * alias of another column). Relation validation reserves the carrier
         * namespaces, so this is defense in depth. */
        JZ_VEC_FOR(const ProjectionFieldPlan, field, field_plans) {
            const jz_str *output = &field->project.output_name;
            bool is_identity = false;
            if (field->has_source_nullability) {
                jz_str prefixed = jazz_engine__str_concat(retained_prefix, output);
                is_identity = jz_str_eq(&field->source_nullability_field, output) || jz_str_eq(&field->source_nullability_field, &prefixed);
                jz_str_drop(&prefixed);
            }
            if (jz_btree_contains(&retained_fields, output) && !is_identity) {
                jz_str d = {0};
                jazz_engine__fmt__str(&d, output);
                e = jazz_engine__ur_op(jz_format("projection output %.*s would replace the retained source field of the same name", JE_STR_ARG(d)));
                jz_str_drop(&d);
                goto fail;
            }
        }
    }
    JZ_VEC_FOR(const ProjectionFieldPlan, field, field_plans) {
        const jz_str *source; const u32 *depth;
        JZ_BTREE_FOR(&field->unwrap_before_project, source, depth) depth_max(&unwrap_fields, source, *depth);
    }
    JZ_VEC_FOR(ProjectionFieldPlan, field, field_plans) {
        u32 output_depth = field->has_nullable_after_project ? field->nullable_after_depth : 0;
        if (field->has_source_nullability) {
            /* Every alias sees the same transformed input. Plan its wrappers only
             * after all aliases agree on source unwraps. */
            u32 unwrapped = depth_get_or(&unwrap_fields, &field->source_nullability_field, 0);
            u32 remaining = field->source_nullability_depth > unwrapped ? field->source_nullability_depth - unwrapped : 0;
            u32 wrappers = output_depth > remaining ? output_depth - remaining : 0;
            if (wrappers > 0) {
                groove_ProjectField np = PF(nullable)(s_clone(&field->source_nullability_field), s_clone(&field->project.output_name));
                PF(drop)(&field->project);
                field->project = np;
                if (wrappers > 1) depth_put(&wrap_after_project, &field->project.output_name, wrappers - 1);
            }
        }
        if (field->has_nullable_after_project) {
            jazz_engine__btree_put(&projected_nullable_field_depths, &field->nullable_after_name, &field->nullable_after_depth);
            field->has_nullable_after_project = false;
        }
        JZ_VEC_PUSH(project_fields, field->project);
        memset(&field->project, 0, sizeof field->project);
        jz_btree_drop(&field->unwrap_before_project);
        if (field->has_source_nullability) jz_str_drop(&field->source_nullability_field);
        field->has_source_nullability = false;
    }
    jz_vec_drop(&field_plans, sizeof(ProjectionFieldPlan), NULL);
    {
        const jz_str *field; const u32 *depth;
        JZ_BTREE_FOR(&unwrap_fields, field, depth)
            for (u32 i = 0; i < *depth; i++) st->graph = GB(unwrap_nullable)(st->graph, s_clone(field));
    }
    jz_btree retained_route_fields, projected_outputs;
    strset_init(&retained_route_fields);
    strset_init(&projected_outputs);
    JZ_VEC_FOR(const groove_ProjectField, pf, project_fields) jazz_engine__strset_insert_clone(&projected_outputs, &pf->output_name);
    {
        const jz_str *field; const void *unit;
        if (ljr) {
            JZ_BTREE_FOR(&st->available_route_fields, field, unit) {
                if (!jz_btree_contains(&projected_outputs, field)) JZ_VEC_PUSH(project_fields, PF(renamed)(F(left_field)(JZ_S(field)), s_clone(field)));
                jazz_engine__strset_insert_clone(&retained_route_fields, field);
            }
            JZ_BTREE_FOR(env->route_fields, field, unit) {
                if (!jz_btree_contains(&ljr->fields, field)) continue;
                if (!jz_btree_contains(&projected_outputs, field)) JZ_VEC_PUSH(project_fields, PF(renamed)(F(right_field)(JZ_S(field)), s_clone(field)));
                jazz_engine__strset_insert_clone(&retained_route_fields, field);
            }
        } else {
            JZ_BTREE_FOR(&st->available_route_fields, field, unit) {
                if (!jz_btree_contains(&projected_outputs, field)) JZ_VEC_PUSH(project_fields, PF(named)(s_clone(field)));
                jazz_engine__strset_insert_clone(&retained_route_fields, field);
            }
        }
    }
    if (has_retained) {
        /* A final relation projection follows a joined graph, so every retained
         * root descriptor field (including magic provenance and version metadata)
         * still lives on the left side. Keeping user fields qualified but
         * appending metadata unqualified made nested covered-input joins fail
         * while preparing their receiver-local terminal. */
        const jz_str *field; const void *unit;
        JZ_BTREE_FOR(&retained_fields, field, unit) {
            jz_str fc = s_clone(field);
            if (!jazz_engine__btree_set_add(&projected_outputs, &fc)) continue;
            jz_str source_field = jazz_engine__str_concat(retained_prefix, field);
            u32 declared_depth = depth_get_or(&retained_depths, field, 0);
            u32 removed_layers = depth_get_or(&unwrap_fields, &source_field, 0);
            if (removed_layers > declared_depth) removed_layers = declared_depth;
            /* Contributor carriers promise the pre-projection source layout,
             * independently of public aliases. Restore wrappers removed by shared
             * source unwraps. */
            if (removed_layers > 0) {
                JZ_VEC_PUSH(project_fields, PF(nullable)(source_field, s_clone(field)));
                if (removed_layers > 1) depth_put(&wrap_after_project, field, removed_layers - 1);
            } else {
                JZ_VEC_PUSH(project_fields, PF(renamed)(source_field, s_clone(field)));
            }
        }
    }
    jz_btree_drop(&projected_outputs);
    st->graph = groove__ivm__graph__join_chains__GraphBuilder__narrow_projected_join_chain(GB(project_fields)(st->graph, project_fields));
    jz_btree_drop(&st->fields);
    strset_init(&st->fields);
    JZ_VEC_FOR(const jazz_engine_RowProjection, column, *columns) jazz_engine__strset_insert_clone(&st->fields, &column->output.name);
    strset_extend_clone(&st->fields, &retained_route_fields);
    if (has_retained) {
        jz_str f;
        while (jz_btree_pop_first(&retained_fields, &f, NULL)) jazz_engine__btree_set_add(&st->fields, &f);
    }
    {
        u32 max_layer = 0;
        const jz_str *k; const u32 *v;
        JZ_BTREE_FOR(&wrap_after_project, k, v) if (*v > max_layer) max_layer = *v;
        for (u32 layer = 0; layer < max_layer; layer++) {
            groove_ProjectFieldVec wrap = {0};
            const jz_str *name; const void *unit;
            JZ_BTREE_FOR(&st->fields, name, unit) {
                if (depth_get_or(&wrap_after_project, name, 0) > layer) JZ_VEC_PUSH(wrap, PF(nullable)(s_clone(name), s_clone(name)));
                else JZ_VEC_PUSH(wrap, PF(named)(s_clone(name)));
            }
            st->graph = GB(project_fields)(st->graph, wrap);
        }
    }
    jz_btree_drop(&st->nullable_fields);
    jz_btree_drop(&st->nullable_field_depths);
    strset_init(&st->nullable_fields);
    {
        const jz_str *k; const u32 *v;
        JZ_BTREE_FOR(&projected_nullable_field_depths, k, v) jazz_engine__strset_insert_clone(&st->nullable_fields, k);
    }
    st->nullable_field_depths = projected_nullable_field_depths;
    if (has_retained) {
        const jz_str *field; const u32 *depth;
        JZ_BTREE_FOR(&retained_depths, field, depth) {
            if (!jz_btree_contains(&st->fields, field)) continue;
            jazz_engine__strset_insert_clone(&st->nullable_fields, field);
            depth_max(&st->nullable_field_depths, field, *depth);
        }
    }
    jz_btree_drop(&st->available_route_fields);
    st->available_route_fields = retained_route_fields;
    clear_last_join_right(st);
    jz_btree_drop(&retained_fields);
    jz_btree_drop(&retained_depths);
    jz_btree_drop(&unwrap_fields);
    jz_btree_drop(&wrap_after_project);
    return NULL;
fail:
    field_plans_drop((void *)&field_plans);
    pfvec_drop(&project_fields);
    jz_btree_drop(&retained_fields);
    jz_btree_drop(&retained_depths);
    jz_btree_drop(&unwrap_fields);
    jz_btree_drop(&projected_nullable_field_depths);
    jz_btree_drop(&wrap_after_project);
    return e;
}

/* consumes graph */
static jazz_engine_UrErr lower_linear_plan_steps_cached(groove_GraphBuilder *graph, const jazz_engine_LinearCurrentRoot *plan,
                                                        const jazz_engine_ResolvedSource *root_source, const jz_btree *resolved_sources,
                                                        jazz_engine_LoweringContext *request, const jz_btree *lowered,
                                                        const jazz_engine_LinearCurrentRoot *cache_plan, bool retain_final_project_input_fields, LRI *out) {
    LinearState st;
    memset(&st, 0, sizeof st);
    bool is_source = plan->root.tag == JAZZ_ENGINE_LROOT_SOURCE;
    if (is_source) {
        st.graph = graph;
    } else if (plan->root.tag == JAZZ_ENGINE_LROOT_VALUE) {
        groove_gb_drop(graph);
        JE_UR_TRY(lower_value_source(&plan->root.shape, &plan->root.columns, plan->root.mode, request, &st.graph));
    } else {
        groove_gb_drop(graph);
        st.graph = GB(frontier_source)(s_clone(&plan->root.frontier), value_source_descriptor(&plan->root.columns));
    }
    strset_init(&st.fields);
    if (is_source) source_fields_into_set(root_source, &st.fields);
    else JZ_VEC_FOR(const jazz_engine_ValueSourceColumn, column, plan->root.columns) jazz_engine__strset_insert_clone(&st.fields, &column->name);
    if (is_source) {
        source_nullable_fields(root_source, &st.nullable_fields);
        source_nullable_field_depths(root_source, &st.nullable_field_depths);
        jazz_engine__strset_clone(&st.available_route_fields, &root_source->routing_fields);
    } else {
        strset_init(&st.nullable_fields);
        depths_init(&st.nullable_field_depths);
        strset_init(&st.available_route_fields);
    }
    /* Earlier inner-join inputs are flattened into the left record before a
     * subsequent join. Keep their source-qualified field addresses so the final
     * flat-join projection can still name every contributing source. */
    jz_btree_init(&st.accumulated_join_fields, &ACC_JOIN_FIELDS_DESC);
    /* UNION inputs have an occurrence identity which is not source-qualified:
     * it is the complete `(arm, row)` pair. Keep its public terminal names while
     * flattening a consecutive inner join so the final projection can retain the
     * pair for result-membership. */
    strset_init(&st.accumulated_union_occurrence_fields);
    const jazz_engine_ParameterDomain *domain;
    jazz_engine_UrErr e = JE_ARG(LoweringContext__parameter_domain)(request, &domain);
    if (e) goto fail;
    LinearEnv env = {plan, root_source, resolved_sources, request, lowered, cache_plan, retain_final_project_input_fields, &domain->routing_params};

    for (u32 step_index = 0; step_index < plan->steps.len; step_index++) {
        const jazz_engine_LinearStep *step = &plan->steps.ptr[step_index];
        switch (step->tag) {
        case JAZZ_ENGINE_STEP_FILTER: e = linear_filter_step(&st, &env, &step->u.filter); break;
        case JAZZ_ENGINE_STEP_JOIN: e = linear_join_step(&st, &env, step_index); break;
        case JAZZ_ENGINE_STEP_PROJECT: e = linear_project_step(&st, &env, step_index, &step->u.project); break;
        case JAZZ_ENGINE_STEP_ORDER_BY:
            clear_last_join_right(&st);
            st.pending_order = &step->u.order_by;
            break;
        case JAZZ_ENGINE_STEP_SLICE: {
            clear_last_join_right(&st);
            jazz_engine_OrderKeyVec none = {0};
            const jazz_engine_OrderKeyVec *order = st.pending_order ? st.pending_order : &none;
            st.pending_order = NULL;
            groove_GraphBuilder *g = st.graph;
            st.graph = NULL;
            e = lower_window(g, order, &step->u.slice.partition_by, &st.available_route_fields, step->u.slice.has_limit, step->u.slice.limit,
                             step->u.slice.offset, &step->u.slice.tie_breaker, plan, root_source, request, &st.graph);
            break;
        }
        default: {
            clear_last_join_right(&st);
            if (st.pending_order) { e = JE_UR_OP("order-by before aggregate is not lowered yet"); break; }
            LRI agg;
            groove_GraphBuilder *g = st.graph;
            st.graph = NULL;
            e = lower_aggregate(g, &step->u.aggregate.group_by, &step->u.aggregate.outputs, plan, root_source, request, &agg);
            if (e) break;
            st.graph = lri_take_graph(&agg);
            jz_btree_drop(&st.fields);
            st.fields = agg.fields;
            memset(&agg.fields, 0, sizeof agg.fields);
            JE_GL(LoweredRelationInput__drop)(&agg);
            jz_btree_clear(&st.nullable_fields);
            jz_btree_clear(&st.nullable_field_depths);
            jz_btree_clear(&st.available_route_fields);
            break;
        }
        }
        if (e) goto fail;
    }
    if (st.pending_order) {
        const jazz_engine_OrderKeyVec *order = st.pending_order;
        st.pending_order = NULL;
        if ((e = lower_order_fallback(&st, &env, order))) goto fail;
    }
    memset(out, 0, sizeof *out);
    out->graph = st.graph;
    out->has_root_source = true;
    JE_RES(ResolvedSource__Clone__clone)(&out->root_source, root_source);
    out->fields = st.fields;
    out->nullable_fields = st.nullable_fields;
    out->nullable_field_depths = st.nullable_field_depths;
    clear_last_join_right(&st);
    jz_btree_drop(&st.accumulated_join_fields);
    jz_btree_drop(&st.accumulated_union_occurrence_fields);
    jz_btree_drop(&st.available_route_fields);
    return NULL;
fail:
    linear_state_drop(&st);
    return e;
}

jazz_engine_UrErr JE_GL(lower_linear_plan_steps)(groove_GraphBuilder *graph, const jazz_engine_LinearCurrentRoot *plan, const jazz_engine_ResolvedSource *root_source,
                                                 const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, LRI *out) {
    return lower_linear_plan_steps_cached(graph, plan, root_source, resolved_sources, request, NULL, NULL, false, out);
}

/* Lower a recursive seed for a covered receiver input. The seed's rendered
 * relation establishes the initial frontier, while the retained source fields
 * encode the exact admitted source occurrence for the receiver closure. */
jazz_engine_UrErr JE_GL(lower_recursive_seed_membership)(const jazz_engine_RecursiveRelationPlan *relation, const jazz_engine_ResolvedSource *seed_source,
                                                         const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, LRI *out) {
    return lower_linear_plan_steps_cached(groove_gb_clone(seed_source->graph), &relation->seed, seed_source, resolved_sources, request, NULL, NULL, true, out);
}

/* ---------------------------------------------------------------- nullable join keys */
static groove_GraphBuilder *unwrap_join_key_if_nullable(groove_GraphBuilder *graph, jz_str field, u32 nullable_depth) {
    for (u32 i = 0; i < nullable_depth; i++) graph = GB(unwrap_nullable)(graph, s_clone(&field));
    jz_str_drop(&field);
    return graph;
}
groove_GraphBuilder *JE_GL(unwrap_nullable_join_key)(groove_GraphBuilder *graph, jz_str field, u32 nullable_depth) {
    return unwrap_join_key_if_nullable(graph, field, nullable_depth);
}

/* Step lists that only drop steps are borrowed views (shallow element copies);
 * Rust clones them, but lowering only reads them. */
static void step_view_free(jazz_engine_LinearStepVec *v) { jz_vec_drop(v, sizeof(jazz_engine_LinearStep), NULL); }
static jazz_engine_LinearStepVec steps_without_window_or_project(const jazz_engine_LinearStepVec *steps) {
    jazz_engine_LinearStepVec v = {0};
    JZ_VEC_FOR(const jazz_engine_LinearStep, step, *steps) {
        if (step->tag == JAZZ_ENGINE_STEP_ORDER_BY || step->tag == JAZZ_ENGINE_STEP_SLICE || step->tag == JAZZ_ENGINE_STEP_PROJECT) continue;
        JZ_VEC_PUSH(v, *step);
    }
    return v;
}
static jazz_engine_LinearStepVec child_steps_for_relation_edges(const jazz_engine_LinearStepVec *steps) {
    bool previous_was_order_by = false;
    jazz_engine_LinearStepVec filtered = {0};
    jz_vec_reserve(&filtered, steps->len, sizeof(jazz_engine_LinearStep));
    JZ_VEC_FOR(const jazz_engine_LinearStep, step, *steps) {
        /* Relation-edge terminals describe the physical parent/child witnesses,
         * not the child query's rendered row. A final public projection can
         * rename `row_uuid` to `id` and discard the version fields the edge
         * needs; retain the source layout here and let the public collector
         * perform that projection on its own path. */
        if (step->tag == JAZZ_ENGINE_STEP_PROJECT) { previous_was_order_by = false; continue; }
        if (step->tag == JAZZ_ENGINE_STEP_SLICE && !previous_was_order_by) { previous_was_order_by = false; continue; }
        previous_was_order_by = step->tag == JAZZ_ENGINE_STEP_ORDER_BY;
        JZ_VEC_PUSH(filtered, *step);
    }
    return filtered;
}

/* ---------------------------------------------------------------- correlated paths */
static jazz_engine_UrErr lower_correlated_path_plan(groove_GraphBuilder *graph, const jazz_engine_CorrelatedPathPlan *path,
                                                    const jazz_engine_ResolvedSource *root_source, const jz_btree *resolved_sources,
                                                    jazz_engine_LoweringContext *request, LRI *out);

/* Apply requirements declared by nested relation builders to the rows of their
 * immediate parent. Optional nested relations never gate that parent; their own
 * descendants are handled when the optional relation is collected. Consumes parent. */
static jazz_engine_UrErr lower_required_nested_parent_graph(groove_GraphBuilder *parent, const jazz_engine_CorrelatedPathPlanVec *nested,
                                                            const jazz_engine_ResolvedSource *parent_source, const jz_btree *resolved_sources,
                                                            jazz_engine_LoweringContext *request, groove_GraphBuilder **out) {
    JZ_VEC_FOR(const jazz_engine_CorrelatedPathPlan, path, *nested) {
        if (path->requirement == JAZZ_ENGINE_CORR_OPTIONAL) continue;
        LRI lowered;
        JE_UR_TRY(lower_correlated_path_plan(parent, path, parent_source, resolved_sources, request, &lowered));
        parent = lri_take_graph(&lowered);
        JE_GL(LoweredRelationInput__drop)(&lowered);
    }
    *out = parent;
    return NULL;
}

/* Required-match joins unwrap nullable keys to reject null correlations. Their
 * output still represents the original parent row, including its field identity,
 * routing carriers, and every nullable wrapper expected by collector union arms. */
static groove_GraphBuilder *restore_correlation_parent_shape(groove_GraphBuilder *graph, const jazz_engine_ResolvedSource *source, const jz_str *key,
                                                             u32 nullable_depth) {
    if (nullable_depth == 0) return GB(project_fields)(graph, JE_TERM(project_source_fields_with_routes)(source, &source->routing_fields));
    for (u32 i = 0; i < nullable_depth; i++) {
        groove_ProjectFieldVec fields = JE_TERM(project_source_fields_with_routes)(source, &source->routing_fields);
        JZ_VEC_FOR(groove_ProjectField, field, fields) {
            if (!jz_str_eq(&field->output_name, key)) continue;
            groove__ivm__graph__ProjectExpr__drop(&field->expression);
            memset(&field->expression, 0, sizeof field->expression);
            field->expression.tag = GROOVE_PEXPR_NULLABLE;
            field->expression.u.field = groove__ivm__graph__FieldRef__stored_name(s_clone(key));
        }
        graph = GB(project_fields)(graph, fields);
    }
    return graph;
}

static void correlation_keys_with_routes(jz_str parent_key, jz_str child_key, const jz_btree *shared_route_fields,
                                         jazz_engine_StrVec *parent_keys, jazz_engine_StrVec *child_keys) {
    jazz_engine_StrVec p = jazz_engine__strvec_one(parent_key), c = jazz_engine__strvec_one(child_key);
    const jz_str *field; const void *unit;
    JZ_BTREE_FOR(shared_route_fields, field, unit) {
        bool present = false;
        for (u32 i = 0; i < p.len && i < c.len; i++)
            if (jz_str_eq(&p.ptr[i], field) && jz_str_eq(&c.ptr[i], field)) { present = true; break; }
        if (present) continue;
        jazz_engine__strvec_push_clone(&p, field);
        jazz_engine__strvec_push_clone(&c, field);
    }
    *parent_keys = p;
    *child_keys = c;
}

/* consumes parent, child, parent_key, child_key */
static jazz_engine_UrErr lower_cardinality_complete_parent_graph(groove_GraphBuilder *parent, groove_GraphBuilder *child,
                                                                 const jazz_engine_ResolvedSource *root_source, jz_str parent_key, jz_str child_key,
                                                                 const jz_btree *shared_route_fields, groove_GraphBuilder **out) {
    const groove_ValueType *parent_key_type = JE_GL(source_field_type)(root_source, JZ_S(&parent_key));
    if (!parent_key_type) {
        jz_str d = {0};
        jazz_engine__fmt__str(&d, &parent_key);
        jazz_engine_UrErr e = jazz_engine__ur_op(jz_format("match-correlation-cardinality parent key %.*s is not projected", JE_STR_ARG(d)));
        jz_str_drop(&d);
        jz_str_drop(&parent_key);
        jz_str_drop(&child_key);
        groove_gb_drop(parent);
        groove_gb_drop(child);
        return e;
    }
    bool is_array_key = parent_key_type->tag == GROOVE_VT_ARRAY ||
                        (parent_key_type->tag == GROOVE_VT_NULLABLE && parent_key_type->u.inner->tag == GROOVE_VT_ARRAY);
    if (!is_array_key) {
        jazz_engine_StrVec parent_keys, child_keys;
        correlation_keys_with_routes(parent_key, child_key, shared_route_fields, &parent_keys, &child_keys);
        *out = GB(project_fields)(GB(semi_join)(parent, child, parent_keys, child_keys),
                                  JE_TERM(project_source_fields_with_routes)(root_source, &root_source->routing_fields));
        return NULL;
    }
    const char *required_element_field = "__jazz_required_correlation_element";
    const jz_str *row = &root_source->row_shape.row_uuid_field;
    groove_GraphBuilder *required = GB(unnest)(groove_gb_clone(parent), parent_key, s_lit(required_element_field));
    groove_ProjectFieldVec covered_fields = JE_TERM(project_source_fields_from_prefix)(root_source, JAZZ_ENGINE_LEFT_JOIN_PREFIX);
    JZ_VEC_PUSH(covered_fields, PF(renamed)(F(left_field)(JZ_C(required_element_field)), s_lit(required_element_field)));
    groove_GraphBuilder *covered = GB(project_fields)(GB(join)(groove_gb_clone(required), child, strvec_one_lit(required_element_field),
                                                               jazz_engine__strvec_one(child_key)), covered_fields);
    groove_GraphBuilder *missing = GB(project_fields)(GB(anti_join)(required, covered, strvec_two(s_clone(row), s_lit(required_element_field)),
                                                                    strvec_two(s_clone(row), s_lit(required_element_field))),
                                                      JE_TERM(project_source_fields_from_prefix)(root_source, ""));
    *out = GB(anti_join)(parent, missing, jazz_engine__strvec_one(s_clone(row)), jazz_engine__strvec_one(s_clone(row)));
    return NULL;
}

static jazz_engine_UrErr path_child_source(const jazz_engine_CorrelatedPathPlan *path, const jz_btree *resolved_sources,
                                           const jazz_engine_SourceId **child_root, const jazz_engine_ResolvedSource **child_source) {
    *child_root = JE_PLAN(LinearRoot__source)(&path->child.root);
    if (!*child_root) return JE_UR_OP("path child must be a source");
    *child_source = jz_btree_get(resolved_sources, *child_root);
    if (!*child_source) return ur_rt_dbg_sid("path child source %.*s was not resolved", *child_root);
    return NULL;
}

static jazz_engine_UrErr lower_correlated_path_plan(groove_GraphBuilder *graph, const jazz_engine_CorrelatedPathPlan *path,
                                                    const jazz_engine_ResolvedSource *root_source, const jz_btree *resolved_sources,
                                                    jazz_engine_LoweringContext *request, LRI *out) {
    LRI parent, child;
    JE_UR_TRY(JE_GL(lower_linear_plan_steps)(graph, &path->parent, root_source, resolved_sources, request, &parent));
    const jazz_engine_SourceId *child_root;
    const jazz_engine_ResolvedSource *child_source;
    groove_GraphBuilder *child_graph = NULL;
    jz_btree shared_route_fields = {0};
    jz_str parent_key = {0}, child_key = {0};
    jazz_engine_UrErr e = path_child_source(path, resolved_sources, &child_root, &child_source);
    if (e) goto fail;
    /* The path graph carries physical parent/child witnesses. Public child
     * projections are applied by the collector after the path, so they must not
     * rename or discard the row/version fields used here. */
    jazz_engine_LinearCurrentRoot child_relation_plan = {path->child.root, steps_without_window_or_project(&path->child.steps)};
    e = JE_GL(lower_linear_plan_steps)(groove_gb_clone(child_source->graph), &child_relation_plan, child_source, resolved_sources, request, &child);
    step_view_free(&child_relation_plan.steps);
    if (e) goto fail;
    /* A correlated child can be maintained once for several provenance routes.
     * Route is therefore part of existence identity whenever both sides carry
     * it; otherwise a qualifying child on one route can keep a different
     * route's parent spuriously present. */
    strset_intersect(&root_source->routing_fields, &child.fields, &shared_route_fields);
    e = lower_required_nested_parent_graph(lri_take_graph(&child), &path->nested, child_source, resolved_sources, request, &child_graph);
    JE_GL(LoweredRelationInput__drop)(&child);
    if (e) goto fail;
    const jazz_engine_SourceId *parent_root = JE_PLAN(LinearRoot__source)(&path->parent.root);
    if (!parent_root) { e = JE_UR_OP("path parent must be a source"); goto fail; }
    if ((e = JE_GL(lower_path_key_pair)(&path->correlation, parent_root, root_source, child_root, child_source, request, &parent_key, &child_key))) goto fail;
    u32 parent_key_nullable_depth = source_field_nullable_depth(root_source, JZ_S(&parent_key));
    u32 child_key_nullable_depth = source_field_nullable_depth(child_source, JZ_S(&child_key));
    child_graph = unwrap_join_key_if_nullable(child_graph, s_clone(&child_key), child_key_nullable_depth);
    LRI lowered;
    switch (path->requirement) {
    case JAZZ_ENGINE_CORR_OPTIONAL:
        lri_of_source(&lowered, lri_take_graph(&parent), root_source);
        groove_gb_drop(child_graph);
        jz_str_drop(&child_key);
        break;
    case JAZZ_ENGINE_CORR_AT_LEAST_ONE: {
        groove_GraphBuilder *p = unwrap_join_key_if_nullable(lri_take_graph(&parent), s_clone(&parent_key), parent_key_nullable_depth);
        jazz_engine_StrVec parent_keys, child_keys;
        correlation_keys_with_routes(s_clone(&parent_key), child_key, &shared_route_fields, &parent_keys, &child_keys);
        groove_GraphBuilder *g = restore_correlation_parent_shape(GB(semi_join)(p, child_graph, parent_keys, child_keys), root_source, &parent_key,
                                                                  parent_key_nullable_depth);
        lri_of_source(&lowered, g, root_source);
        break;
    }
    default: {
        groove_GraphBuilder *p = unwrap_join_key_if_nullable(lri_take_graph(&parent), s_clone(&parent_key), parent_key_nullable_depth);
        groove_GraphBuilder *g;
        e = lower_cardinality_complete_parent_graph(p, child_graph, root_source, s_clone(&parent_key), child_key, &shared_route_fields, &g);
        child_graph = NULL;
        memset(&child_key, 0, sizeof child_key);
        if (e) goto fail;
        lri_of_source(&lowered, restore_correlation_parent_shape(g, root_source, &parent_key, parent_key_nullable_depth), root_source);
        break;
    }
    }
    jz_str_drop(&parent_key);
    jz_btree_drop(&shared_route_fields);
    JE_GL(LoweredRelationInput__drop)(&parent);
    if (path->output_steps.len == 0) { *out = lowered; return NULL; }
    jazz_engine_LinearCurrentRoot tail = {path->parent.root, path->output_steps};
    e = JE_GL(lower_linear_plan_steps)(lri_take_graph(&lowered), &tail, root_source, resolved_sources, request, out);
    JE_GL(LoweredRelationInput__drop)(&lowered);
    return e;
fail:
    JE_GL(LoweredRelationInput__drop)(&parent);
    groove_gb_drop(child_graph);
    jz_btree_drop(&shared_route_fields);
    jz_str_drop(&parent_key);
    jz_str_drop(&child_key);
    return e;
}

jazz_engine_UrErr JE_GL(lower_correlated_path_relation_graph)(const jazz_engine_CorrelatedPathPlan *path, const jazz_engine_ResolvedSource *root_source,
                                                              const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, LRI *out) {
    LRI parent;
    JE_UR_TRY(JE_GL(lower_linear_plan_steps)(groove_gb_clone(root_source->graph), &path->parent, root_source, resolved_sources, request, &parent));
    jazz_engine_UrErr e = JE_GL(lower_correlated_path_relation_graph_from_parent)(path, lri_take_graph(&parent), root_source, resolved_sources, request, true, out);
    JE_GL(LoweredRelationInput__drop)(&parent);
    return e;
}

jazz_engine_UrErr JE_GL(lower_correlated_path_relation_graph_from_parent)(const jazz_engine_CorrelatedPathPlan *path, groove_GraphBuilder *parent,
    const jazz_engine_ResolvedSource *parent_source, const jz_btree *resolved_sources, jazz_engine_LoweringContext *request,
    bool retain_child_window, LRI *out) {
    const jazz_engine_SourceId *child_root;
    const jazz_engine_ResolvedSource *child_source;
    groove_GraphBuilder *child_graph = NULL;
    jazz_engine_UrErr e = path_child_source(path, resolved_sources, &child_root, &child_source);
    if (e) goto fail;
    /* Correlated paths are a source-level relationship boundary. The collector
     * projects the public child record after that boundary, while relationship
     * edges and nested paths still need the child's physical row/version
     * fields. In particular, a final public projection can rename or omit
     * `row_uuid`, which must not shape the path graph. */
    jazz_engine_LinearCurrentRoot child_plan = {path->child.root, retain_child_window ? child_steps_for_relation_edges(&path->child.steps)
                                                                                      : steps_without_window_or_project(&path->child.steps)};
    LRI child;
    e = JE_GL(lower_linear_plan_steps)(groove_gb_clone(child_source->graph), &child_plan, child_source, resolved_sources, request, &child);
    step_view_free(&child_plan.steps);
    if (e) goto fail;
    e = lower_required_nested_parent_graph(lri_take_graph(&child), &path->nested, child_source, resolved_sources, request, &child_graph);
    JE_GL(LoweredRelationInput__drop)(&child);
    if (e) goto fail;
    const jazz_engine_SourceId *parent_root = JE_PLAN(LinearRoot__source)(&path->parent.root);
    if (!parent_root) { e = JE_UR_OP("path parent must be a source"); goto fail; }
    jz_str parent_key, child_key;
    if ((e = JE_GL(lower_path_key_pair)(&path->correlation, parent_root, parent_source, child_root, child_source, request, &parent_key, &child_key))) goto fail;
    u32 parent_key_nullable_depth = source_field_nullable_depth(parent_source, JZ_S(&parent_key));
    u32 child_key_nullable_depth = source_field_nullable_depth(child_source, JZ_S(&child_key));
    parent = unwrap_join_key_if_nullable(parent, s_clone(&parent_key), parent_key_nullable_depth);
    child_graph = unwrap_join_key_if_nullable(child_graph, s_clone(&child_key), child_key_nullable_depth);
    lri_empty(out, GB(join)(parent, child_graph, jazz_engine__strvec_one(parent_key), jazz_engine__strvec_one(child_key)));
    return NULL;
fail:
    groove_gb_drop(parent);
    groove_gb_drop(child_graph);
    return e;
}

/* ---------------------------------------------------------------- relation inputs */
static jazz_engine_UrErr lower_relation_input_cached(const jazz_engine_RelationInputPlan *plan, const jz_btree *resolved_sources,
                                                     jazz_engine_LoweringContext *request, const jz_btree *lowered,
                                                     bool retain_final_project_input_fields, LRI *out);

typedef JZ_VEC(const jazz_engine_RelationInputPlan *) PlanRefVec;
/* Relation inputs are nested by policy joins. Lower them in postorder so a
 * recursive policy graph is compiled iteratively rather than consuming the
 * server shell stack through nested `lower_relation_input` calls. */
static void push_join_rights_rev(void *pending, const jazz_engine_LinearStepVec *steps) {
    typedef struct { const jazz_engine_RelationInputPlan *plan; bool visited; } Pending;
    for (u32 i = steps->len; i-- > 0;) {
        if (steps->ptr[i].tag != JAZZ_ENGINE_STEP_JOIN) continue;
        Pending p = {steps->ptr[i].u.join.right, false};
        jz_vec_push(pending, &p, sizeof p);
    }
}
static PlanRefVec relation_input_postorder(const jazz_engine_RelationInputPlan *root) {
    typedef struct { const jazz_engine_RelationInputPlan *plan; bool visited; } Pending;
    JZ_VEC(Pending) pending = {0};
    PlanRefVec ordered = {0};
    Pending first = {root, false};
    JZ_VEC_PUSH(pending, first);
    while (pending.len) {
        Pending top = pending.ptr[--pending.len];
        if (top.visited) { JZ_VEC_PUSH(ordered, top.plan); continue; }
        Pending again = {top.plan, true};
        JZ_VEC_PUSH(pending, again);
        switch (top.plan->tag) {
        case JAZZ_ENGINE_RIP_LINEAR: push_join_rights_rev(&pending, &top.plan->u.linear.steps); break;
        case JAZZ_ENGINE_RIP_UNION: {
            const jazz_engine_UnionBranchPlanVec *branches = &top.plan->u.union_.branches;
            for (u32 i = branches->len; i-- > 0;) {
                Pending p = {&branches->ptr[i].plan, false};
                JZ_VEC_PUSH(pending, p);
            }
            break;
        }
        default:
            push_join_rights_rev(&pending, &top.plan->u.recursive.step.steps);
            push_join_rights_rev(&pending, &top.plan->u.recursive.seed.steps);
            break;
        }
    }
    jz_vec_drop(&pending, sizeof(Pending), NULL);
    return ordered;
}

static jazz_engine_UrErr lower_relation_input_with_retained_final_fields(const jazz_engine_RelationInputPlan *plan, const jz_btree *resolved_sources,
                                                                         jazz_engine_LoweringContext *request, bool retain_final_project_input_fields,
                                                                         LRI *out) {
    jz_btree lowered;
    jz_btree_init(&lowered, &jazz_engine__U32_LRI_MAP_DESC);
    uintptr_t final_plan_key = relation_input_key(plan);
    PlanRefVec order = relation_input_postorder(plan);
    jazz_engine_UrErr e = NULL;
    for (u32 i = 0; i < order.len && !e; i++) {
        LRI output;
        uintptr_t key = relation_input_key(order.ptr[i]);
        e = lower_relation_input_cached(order.ptr[i], resolved_sources, request, &lowered, retain_final_project_input_fields && key == final_plan_key, &output);
        if (!e) jazz_engine__btree_put(&lowered, &key, &output);
    }
    jz_vec_drop(&order, sizeof(const jazz_engine_RelationInputPlan *), NULL);
    if (!e) {
        uintptr_t k;
        if (!jz_btree_remove(&lowered, &final_plan_key, &k, out)) e = JE_UR_RT("relation input was not lowered");
    }
    jz_btree_drop(&lowered);
    return e;
}

jazz_engine_UrErr JE_GL(lower_relation_input)(const jazz_engine_RelationInputPlan *plan, const jz_btree *resolved_sources,
                                              jazz_engine_LoweringContext *request, LRI *out) {
    return lower_relation_input_with_retained_final_fields(plan, resolved_sources, request, false, out);
}

/* Lower a relation as an input to a source-closure contributor terminal.
 *
 * A relation facade's final `Project` renders its public output. Contributor
 * terminals instead freeze the source occurrence that supplied that output,
 * including its row/version witness fields. Retain those physical fields
 * alongside, rather than instead of, the facade projection: membership may
 * deliberately name a public bridge alias such as `bridge_root`. */
jazz_engine_UrErr JE_GL(lower_relation_input_for_contributor)(const jazz_engine_RelationInputPlan *plan, const jz_btree *resolved_sources,
                                                              jazz_engine_LoweringContext *request, LRI *out) {
    return lower_relation_input_with_retained_final_fields(plan, resolved_sources, request, true, out);
}

/* ---------------------------------------------------------------- unions */
typedef struct UnionLeaf { const jazz_engine_RelationInputPlan *plan; jz_str label; } UnionLeaf;
typedef JZ_VEC(UnionLeaf) UnionLeafVec;
static void union_leaves_drop(UnionLeafVec *v) {
    JZ_VEC_FOR(UnionLeaf, l, *v) jz_str_drop(&l->label);
    jz_vec_drop(v, sizeof(UnionLeaf), NULL);
}

/* Flatten nested UNION ALL inputs without using the call stack. The complete
 * path label remains the occurrence carrier for each leaf arm. */
static UnionLeafVec union_branch_plans_with_labels(const jazz_engine_UnionPlan *union_, const jz_str *prefix) {
    UnionLeafVec leaves = {0}, pending = {0};
    for (u32 i = union_->branches.len; i-- > 0;) {
        const jazz_engine_UnionBranchPlan *branch = &union_->branches.ptr[i];
        UnionLeaf p = {&branch->plan, prefix ? jazz_model__query__compose_union_arm_path(prefix, &branch->label) : s_clone(&branch->label)};
        JZ_VEC_PUSH(pending, p);
    }
    while (pending.len) {
        UnionLeaf top = pending.ptr[--pending.len];
        if (top.plan->tag == JAZZ_ENGINE_RIP_UNION) {
            const jazz_engine_UnionBranchPlanVec *branches = &top.plan->u.union_.branches;
            for (u32 i = branches->len; i-- > 0;) {
                UnionLeaf p = {&branches->ptr[i].plan, jazz_model__query__compose_union_arm_path(&top.label, &branches->ptr[i].label)};
                JZ_VEC_PUSH(pending, p);
            }
            jz_str_drop(&top.label);
        } else {
            JZ_VEC_PUSH(leaves, top);
        }
    }
    jz_vec_drop(&pending, sizeof(UnionLeaf), NULL);
    return leaves;
}

typedef JZ_VEC(LRI) LriVec;
static void lri_vec_drop(LriVec *v) { jz_vec_drop(v, sizeof(LRI), lri_drop_v); }

static bool carrier_eq(const LRI *a, const LRI *b) {
    if (a->has_union_occurrence_carrier != b->has_union_occurrence_carrier) return false;
    return !a->has_union_occurrence_carrier ||
           (jz_str_eq(&a->union_label_field, &b->union_label_field) && jz_str_eq(&a->union_row_field, &b->union_row_field));
}
static void set_carrier(LRI *l, const char *label, const char *row) {
    if (l->has_union_occurrence_carrier) { jz_str_drop(&l->union_label_field); jz_str_drop(&l->union_row_field); }
    l->has_union_occurrence_carrier = true;
    l->union_label_field = s_lit(label);
    l->union_row_field = s_lit(row);
}
static groove_ProjectFieldVec named_fields(const jz_btree *fields) {
    groove_ProjectFieldVec v = {0};
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(fields, f, unit) JZ_VEC_PUSH(v, PF(named)(s_clone(f)));
    return v;
}

static jazz_engine_UrErr align_union_route_fields(LRI *branch, const jz_btree *fields, jazz_engine_LoweringContext *request) {
    const jazz_engine_ParameterDomain *domain;
    JE_UR_TRY(JE_ARG(LoweringContext__parameter_domain)(request, &domain));
    const jz_btree *route_fields = &domain->routing_params;
    jz_btree missing;
    strset_difference(fields, &branch->fields, &missing);
    bool foreign = false;
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(&missing, f, unit) if (!jz_btree_contains(route_fields, f)) { foreign = true; break; }
    jz_btree_drop(&missing);
    if (foreign) return JE_UR_OP("union branches must output the same fields");
    if (request->request->input.binding.has_source_shape) {
        groove_RecordDescriptor descriptor;
        JE_UR_TRY(binding_source_descriptor_with_user_params(request, NULL, &descriptor));
        groove_GraphBuilder *binding = GB(binding_source)(s_clone(&request->request->input.binding.source_shape), descriptor);
        groove_ProjectFieldVec project_fields = {0};
        JZ_BTREE_FOR(fields, f, unit) {
            if (jz_btree_contains(&branch->fields, f)) JZ_VEC_PUSH(project_fields, PF(renamed)(F(left_field)(JZ_S(f)), s_clone(f)));
            else JZ_VEC_PUSH(project_fields, PF(renamed)(F(right_field)(route_param_or_field(f)), s_clone(f)));
        }
        jazz_engine_StrVec existing_route_fields = {0}, binding_route_fields = {0};
        JZ_BTREE_FOR(&branch->fields, f, unit) {
            if (!jz_btree_contains(route_fields, f)) continue;
            jazz_engine__strvec_push_clone(&existing_route_fields, f);
            JZ_VEC_PUSH(binding_route_fields, jz_str_from_slice(route_param_or_field(f)));
        }
        branch->graph = GB(project_fields)(JE_GL(policy_join_if_needed)(branch->graph, binding, existing_route_fields, binding_route_fields, request),
                                           project_fields);
    } else {
        groove_ProjectFieldVec project_fields = {0};
        JZ_BTREE_FOR(fields, f, unit) {
            if (jz_btree_contains(&branch->fields, f)) { JZ_VEC_PUSH(project_fields, PF(named)(s_clone(f))); continue; }
            groove_ProjectField pf;
            jazz_engine_UrErr e = JE_TERM(route_literal_project_field)(JZ_S(f), request, &pf);
            if (e) { pfvec_drop(&project_fields); return e; }
            JZ_VEC_PUSH(project_fields, pf);
        }
        branch->graph = GB(project_fields)(branch->graph, project_fields);
    }
    jz_btree_drop(&branch->fields);
    jazz_engine__strset_clone(&branch->fields, fields);
    return NULL;
}

/* consumes lowered */
static jazz_engine_UrErr lower_union_inputs(LriVec lowered, jazz_engine_LoweringContext *request, LRI *out) {
    jz_btree union_fields;
    strset_init(&union_fields);
    JZ_VEC_FOR(const LRI, branch, lowered) strset_extend_clone(&union_fields, &branch->fields);
    bool needs_alignment = false;
    JZ_VEC_FOR(const LRI, branch, lowered) if (!jz_btree_eq(&branch->fields, &union_fields, NULL)) { needs_alignment = true; break; }
    jazz_engine_UrErr e = NULL;
    if (needs_alignment)
        JZ_VEC_FOR(LRI, branch, lowered) if ((e = align_union_route_fields(branch, &union_fields, request))) break;
    jz_btree_drop(&union_fields);
    if (e) { lri_vec_drop(&lowered); return e; }
    if (lowered.len == 0) { lri_vec_drop(&lowered); return JE_UR_OP("union row-set nodes require at least one input"); }
    LRI result = lowered.ptr[0];
    groove_GraphBuilderVec graphs = {0};
    JZ_VEC_PUSH(graphs, lri_take_graph(&result));
    u32 i = 1;
    for (; i < lowered.len; i++) {
        LRI *branch = &lowered.ptr[i];
        if (!jz_btree_eq(&branch->fields, &result.fields, NULL)) { e = JE_UR_OP("union branches must output the same fields"); break; }
        if (!carrier_eq(branch, &result)) { e = JE_UR_OP("union branches must use the same typed occurrence carrier"); break; }
        strset_extend_clone(&result.nullable_fields, &branch->nullable_fields);
        const jz_str *field; const u32 *depth;
        JZ_BTREE_FOR(&branch->nullable_field_depths, field, depth) depth_max(&result.nullable_field_depths, field, *depth);
        bool same_root = result.has_root_source == branch->has_root_source &&
                         (!result.has_root_source || sid_eq(&result.root_source.row_shape.source, &branch->root_source.row_shape.source));
        if (!same_root && result.has_root_source) {
            JE_RES(ResolvedSource__drop)(&result.root_source);
            result.has_root_source = false;
        }
        JZ_VEC_PUSH(graphs, lri_take_graph(branch));
        JE_GL(LoweredRelationInput__drop)(branch);
    }
    if (e) {
        for (; i < lowered.len; i++) JE_GL(LoweredRelationInput__drop)(&lowered.ptr[i]);
        jz_vec_drop(&lowered, sizeof(LRI), NULL);
        jz_vec_drop(&graphs, sizeof(groove_GraphBuilder *), groove__ivm__graph__GraphBuilder__drop_v);
        JE_GL(LoweredRelationInput__drop)(&result);
        return e;
    }
    jz_vec_drop(&lowered, sizeof(LRI), NULL);
    result.graph = GB(union)(graphs);
    *out = result;
    return NULL;
}

static jazz_engine_UrErr lower_union_relation_input_with_prefix_cached(const jazz_engine_UnionPlan *union_, const jz_btree *resolved_sources,
                                                                       jazz_engine_LoweringContext *request, const jz_str *prefix,
                                                                       const jz_btree *lowered, LRI *out) {
    LriVec outputs = {0};
    UnionLeafVec leaves = union_branch_plans_with_labels(union_, prefix);
    jazz_engine_UrErr e = NULL;
    JZ_VEC_FOR(const UnionLeaf, leaf, leaves) {
        if (leaf->plan->tag == JAZZ_ENGINE_RIP_UNION) jz_panic("nested unions are flattened above");
        if (leaf->plan->tag == JAZZ_ENGINE_RIP_RECURSIVE) { e = JE_UR_OP("recursive UNION ALL join arms lack a stable finite occurrence carrier"); break; }
        const jazz_engine_LinearCurrentRoot *linear = &leaf->plan->u.linear;
        const jazz_engine_SourceId *source_id = JE_PLAN(LinearRoot__source)(&linear->root);
        if (!source_id) { e = JE_UR_OP("UNION ALL join occurrence identity requires a stable source row"); break; }
        const jazz_engine_ResolvedSource *source = jz_btree_get(resolved_sources, source_id);
        if (!source) { e = ur_rt_dbg_sid("union source %.*s was not resolved", source_id); break; }
        jazz_engine_LinearCurrentRoot retained;
        JE_PLAN(LinearCurrentRoot__Clone__clone)(&retained, linear);
        if (retained.steps.len && retained.steps.ptr[retained.steps.len - 1].tag == JAZZ_ENGINE_STEP_PROJECT) {
            jazz_engine_RowProjection column;
            memset(&column, 0, sizeof column);
            column.output.name = s_lit("__union_occurrence_row");
            column.output.ty = groove_vt_scalar(GROOVE_VT_UUID);
            column.value.tag = JAZZ_ENGINE_NVR_ROW_ID;
            column.value.sub = JAZZ_ENGINE_ROWID_SOURCE;
            sid_clone(&column.value.u.source, source_id);
            JZ_VEC_PUSH(retained.steps.ptr[retained.steps.len - 1].u.project, column);
        }
        LRI output;
        e = lower_linear_plan_steps_cached(groove_gb_clone(source->graph), &retained, source, resolved_sources, request, lowered, linear, false, &output);
        JE_PLAN(LinearCurrentRoot__drop)(&retained);
        if (e) break;
        if (!jazz_engine__strset_contains_cstr(&output.fields, "__union_occurrence_row")) {
            const jz_str *row_field = &source->row_shape.row_uuid_field;
            if (!jz_btree_contains(&output.fields, row_field)) {
                JE_GL(LoweredRelationInput__drop)(&output);
                e = JE_UR_OP("UNION ALL arm projection discarded its stable source row identity");
                break;
            }
            groove_ProjectFieldVec pf = named_fields(&output.fields);
            JZ_VEC_PUSH(pf, PF(renamed)(s_clone(row_field), s_lit("__union_occurrence_row")));
            output.graph = GB(project_fields)(output.graph, pf);
            jazz_engine__strset_insert_cstr(&output.fields, "__union_occurrence_row");
        }
        groove_ProjectFieldVec pf = named_fields(&output.fields);
        JZ_VEC_PUSH(pf, PF(literal)(s_lit("__union_occurrence_arm"), lit_string(s_clone(&leaf->label))));
        output.graph = GB(project_fields)(output.graph, pf);
        jazz_engine__strset_insert_cstr(&output.fields, "__union_occurrence_arm");
        set_carrier(&output, "__union_occurrence_arm", "__union_occurrence_row");
        JZ_VEC_PUSH(outputs, output);
    }
    union_leaves_drop(&leaves);
    if (e) { lri_vec_drop(&outputs); return e; }
    return lower_union_inputs(outputs, request, out);
}

/* consumes input */
static jazz_engine_UrErr lower_union_terminal_steps(LRI input, const jazz_engine_LinearStepVec *steps, const jazz_engine_ResolvedSource *root_source,
                                                    jazz_engine_LoweringContext *request, LRI *out) {
    if (steps->len == 0) { *out = input; return NULL; }
    const jazz_engine_ResolvedSource *root = input.has_root_source ? &input.root_source : root_source;
    /* LinearCurrentRoot { root: Source { source: root's source, Visible }, steps: [] } as a borrowed view */
    jazz_engine_LinearCurrentRoot plan;
    memset(&plan, 0, sizeof plan);
    plan.root.tag = JAZZ_ENGINE_LROOT_SOURCE;
    plan.root.source = root->row_shape.source;
    plan.root.visibility = JAZZ_ENGINE_VIS_VISIBLE;
    const jazz_engine_OrderKeyVec *pending_order = NULL;
    jazz_engine_OrderKeyVec none = {0};
    jazz_engine_UrErr e = NULL;
    JZ_VEC_FOR(const jazz_engine_LinearStep, step, *steps) {
        if (step->tag == JAZZ_ENGINE_STEP_ORDER_BY) { pending_order = &step->u.order_by; continue; }
        if (step->tag != JAZZ_ENGINE_STEP_SLICE) { e = JE_UR_OP("only global order/window operators may wrap a public UNION ALL"); break; }
        const jazz_engine_OrderKeyVec *order = pending_order ? pending_order : &none;
        pending_order = NULL;
        if ((e = lower_window(lri_take_graph(&input), order, &step->u.slice.partition_by, &root->routing_fields, step->u.slice.has_limit,
                              step->u.slice.limit, step->u.slice.offset, &step->u.slice.tie_breaker, &plan, root, request, &input.graph))) break;
    }
    if (!e && pending_order) {
        jazz_engine_NvrVec partition = {0}, tie = {0};
        jazz_engine_NormalizedValueRef row;
        memset(&row, 0, sizeof row);
        row.tag = JAZZ_ENGINE_NVR_ROW_ID;
        row.sub = JAZZ_ENGINE_ROWID_SOURCE;
        sid_clone(&row.u.source, &root->row_shape.source);
        JZ_VEC_PUSH(tie, row);
        e = lower_window(lri_take_graph(&input), pending_order, &partition, &root->routing_fields, false, 0, 0, &tie, &plan, root, request, &input.graph);
        jazz_engine__nvrvec_drop(&tie);
    }
    if (e) { JE_GL(LoweredRelationInput__drop)(&input); return e; }
    *out = input;
    return NULL;
}

/* root_graph: NULL = None (consumed) */
static jazz_engine_UrErr lower_union_plan(const jazz_engine_UnionPlan *union_, groove_GraphBuilder *root_graph, const jazz_engine_ResolvedSource *root_source,
                                          const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, LRI *out) {
    LriVec lowered = {0};
    UnionLeafVec leaves = union_branch_plans_with_labels(union_, NULL);
    jazz_engine_UrErr e = NULL;
    JZ_VEC_FOR(const UnionLeaf, leaf, leaves) {
        const jazz_engine_RelationInputPlan *branch_plan = leaf->plan;
        LRI input;
        if (branch_plan->tag == JAZZ_ENGINE_RIP_LINEAR) {
            const jazz_engine_LinearCurrentRoot *linear = &branch_plan->u.linear;
            const jazz_engine_SourceId *source_id = JE_PLAN(LinearRoot__source)(&linear->root);
            if (!source_id) { e = JE_UR_OP("union branch must have a source"); break; }
            if (!sid_eq(source_id, &root_source->row_shape.source)) { e = JE_UR_OP("root union branches must share the query result source"); break; }
            groove_GraphBuilder *graph;
            if (root_graph) {
                graph = groove_gb_clone(root_graph);
            } else {
                const jazz_engine_ResolvedSource *s = jz_btree_get(resolved_sources, source_id);
                if (!s) { e = ur_rt_dbg_sid("union branch source %.*s was not resolved", source_id); break; }
                graph = groove_gb_clone(s->graph);
            }
            if ((e = lower_linear_plan_steps_cached(graph, linear, root_source, resolved_sources, request, NULL, NULL, true, &input))) break;
        } else if ((e = JE_GL(lower_relation_input_for_contributor)(branch_plan, resolved_sources, request, &input))) {
            break;
        }
        /* A public root UNION ALL has no join-side occurrence slot. Retain an
         * explicit typed `(arm, source-row)` carrier beside the real root so the
         * terminal can address two derivations of one physical row. */
        const jazz_engine_SourceId *source_id = JE_PLAN(RelationInputPlan__root_source)(branch_plan);
        const jazz_engine_ResolvedSource *source = source_id ? jz_btree_get(resolved_sources, source_id) : NULL;
        if (!source_id) e = JE_UR_OP("UNION ALL root arm lacks a stable source row");
        else if (!source) e = ur_rt_dbg_sid("union source %.*s was not resolved", source_id);
        else if (!jz_btree_contains(&input.fields, &source->row_shape.row_uuid_field))
            e = JE_UR_OP("UNION ALL root arm projection discarded its stable source row identity");
        if (e) { JE_GL(LoweredRelationInput__drop)(&input); break; }
        /* Keep the source descriptor's order ahead of public aliases. Global
         * UNION ordering addresses source fields by descriptor index. */
        jazz_engine_StrVec source_field_names;
        JE_GL(source_fields)(source, &source_field_names);
        jz_btree source_field_set;
        strset_init(&source_field_set);
        groove_ProjectFieldVec retained = {0};
        JZ_VEC_FOR(const jz_str, name, source_field_names) {
            jazz_engine__strset_insert_clone(&source_field_set, name);
            JZ_VEC_PUSH(retained, PF(named)(s_clone(name)));
        }
        jazz_engine__strvec_drop(&source_field_names);
        const jz_str *f; const void *unit;
        JZ_BTREE_FOR(&input.fields, f, unit) if (!jz_btree_contains(&source_field_set, f)) JZ_VEC_PUSH(retained, PF(named)(s_clone(f)));
        jz_btree_drop(&source_field_set);
        JZ_VEC_PUSH(retained, PF(renamed)(s_clone(&source->row_shape.row_uuid_field), s_lit("__root_union_row")));
        JZ_VEC_PUSH(retained, PF(literal)(s_lit("__root_union_arm"), lit_string(s_clone(&leaf->label))));
        input.graph = GB(project_fields)(input.graph, retained);
        jazz_engine__strset_insert_cstr(&input.fields, "__root_union_row");
        jazz_engine__strset_insert_cstr(&input.fields, "__root_union_arm");
        set_carrier(&input, "__root_union_arm", "__root_union_row");
        JZ_VEC_PUSH(lowered, input);
    }
    union_leaves_drop(&leaves);
    groove_gb_drop(root_graph);
    if (e) { lri_vec_drop(&lowered); return e; }
    LRI unioned;
    JE_UR_TRY(lower_union_inputs(lowered, request, &unioned));
    return lower_union_terminal_steps(unioned, &union_->terminal_steps, root_source, request, out);
}

static jazz_engine_UrErr lower_relation_input_cached(const jazz_engine_RelationInputPlan *plan, const jz_btree *resolved_sources,
                                                     jazz_engine_LoweringContext *request, const jz_btree *lowered,
                                                     bool retain_final_project_input_fields, LRI *out) {
    switch (plan->tag) {
    case JAZZ_ENGINE_RIP_LINEAR: {
        const jazz_engine_LinearCurrentRoot *linear = &plan->u.linear;
        const jazz_engine_SourceId *source_id = JE_PLAN(LinearRoot__source)(&linear->root);
        if (!source_id) return JE_UR_OP("linear join input must have a source");
        const jazz_engine_ResolvedSource *source = jz_btree_get(resolved_sources, source_id);
        if (!source) return ur_rt_dbg_sid("join source %.*s was not resolved", source_id);
        return lower_linear_plan_steps_cached(groove_gb_clone(source->graph), linear, source, resolved_sources, request, lowered, linear,
                                              retain_final_project_input_fields, out);
    }
    case JAZZ_ENGINE_RIP_UNION: return lower_union_relation_input_with_prefix_cached(&plan->u.union_, resolved_sources, request, NULL, lowered, out);
    default: {
        const jazz_engine_RecursiveRelationPlan *relation = &plan->u.recursive;
        const jazz_engine_SourceId *source_id = JE_PLAN(RecursiveRelationPlan__root_source)(relation);
        if (!source_id) return JE_UR_OP("recursive join input must include a table source");
        const jazz_engine_ResolvedSource *source = jz_btree_get(resolved_sources, source_id);
        if (!source) return ur_rt_dbg_sid("recursive join source %.*s was not resolved", source_id);
        return JE_GL(lower_recursive_relation_cached)(NULL, relation, source, resolved_sources, request, lowered, out);
    }
    }
}

/* ---------------------------------------------------------------- recursive relations */
jazz_engine_UrErr JE_GL(lower_recursive_relation_cached)(groove_GraphBuilder *root_graph, const jazz_engine_RecursiveRelationPlan *relation,
                                                         const jazz_engine_ResolvedSource *root_source, const jz_btree *resolved_sources,
                                                         jazz_engine_LoweringContext *request, const jz_btree *lowered, LRI *out) {
    /* A recursive relation is ordinarily reached through relation-input postorder
     * and receives its lowered join children. The recursion-owned witness is also
     * lowered directly by closure terminals; build the same immediate child cache
     * there instead of taking an alternate lowering path. */
    jz_btree owned_children, witness_children;
    jz_btree_init(&witness_children, &jazz_engine__U32_LRI_MAP_DESC);
    if (lowered) jz_btree_clone(&owned_children, lowered, usize_clone_v, lri_clone_v);
    else jz_btree_init(&owned_children, &jazz_engine__U32_LRI_MAP_DESC);
    LRI seed = {0}, step = {0}, witness = {0};
    jazz_engine_UrErr e = NULL;
    const jazz_engine_LinearCurrentRoot *linears[2] = {&relation->seed, &relation->step};
    for (u32 l = 0; l < 2 && !e; l++) {
        JZ_VEC_FOR(const jazz_engine_LinearStep, s, linears[l]->steps) {
            if (s->tag != JAZZ_ENGINE_STEP_JOIN) continue;
            LRI child;
            if ((e = JE_GL(lower_relation_input)(s->u.join.right, resolved_sources, request, &child))) break;
            uintptr_t key = relation_input_key(s->u.join.right);
            jazz_engine__btree_or_insert(&owned_children, &key, &child);
        }
    }
    if (e) goto fail;
    const jazz_engine_SourceId *seed_sid = JE_PLAN(RecursiveRelationPlan__seed_source)(relation);
    const jazz_engine_ResolvedSource *seed_root_source = seed_sid ? jz_btree_get(resolved_sources, seed_sid) : NULL;
    groove_GraphBuilder *seed_graph;
    if (seed_root_source) { seed_graph = groove_gb_clone(seed_root_source->graph); groove_gb_drop(root_graph); }
    else seed_graph = root_graph ? root_graph : groove_gb_clone(root_source->graph);
    root_graph = NULL;
    if ((e = lower_linear_plan_steps_cached(seed_graph, &relation->seed, seed_root_source ? seed_root_source : root_source, resolved_sources, request,
                                            &owned_children, NULL, false, &seed))) goto fail;
    const jazz_engine_SourceId *step_source_id = JE_PLAN(RecursiveRelationPlan__step_source)(relation);
    if (!step_source_id) { e = JE_UR_OP("recursive step must include a table source"); goto fail; }
    const jazz_engine_ResolvedSource *step_source = jz_btree_get(resolved_sources, step_source_id);
    if (!step_source) { e = ur_rt_dbg_sid("recursive step source %.*s was not resolved", step_source_id); goto fail; }
    if ((e = lower_linear_plan_steps_cached(groove_gb_clone(step_source->graph), &relation->step, step_source, resolved_sources, request,
                                            &owned_children, NULL, false, &step))) goto fail;
    /* Preserve a generic recursion-owned view of the physical step rows before
     * the normalized recursive projection discards them. The runtime executes
     * this side graph with the exact same frontier/depth as `step`; it is not a
     * second reachability traversal. */
    const jazz_engine_LinearStepVec *rs = &relation->step.steps;
    if (!rs->len || rs->ptr[rs->len - 1].tag != JAZZ_ENGINE_STEP_PROJECT) { e = JE_UR_OP("recursive step witness requires a final projection"); goto fail; }
    /* witness_step_plan = step minus its final projection (a borrowed view).
     * The side output is still authority-local until its terminal is routed to
     * an admitted session. Preserve the same route carriers as the main step;
     * the wire encoder, not this graph, removes private claim fields. */
    jazz_engine_LinearCurrentRoot witness_step_plan = relation->step;
    witness_step_plan.steps.len--;
    witness_step_plan.steps.cap = 0;
    JZ_VEC_FOR(const jazz_engine_LinearStep, s, witness_step_plan.steps) {
        if (s->tag != JAZZ_ENGINE_STEP_JOIN) continue;
        LRI child;
        if ((e = JE_GL(lower_relation_input)(s->u.join.right, resolved_sources, request, &child))) goto fail;
        uintptr_t key = relation_input_key(s->u.join.right);
        jazz_engine__btree_put(&witness_children, &key, &child);
    }
    if ((e = lower_linear_plan_steps_cached(groove_gb_clone(step_source->graph), &witness_step_plan, step_source, resolved_sources, request,
                                            &witness_children, &witness_step_plan, false, &witness))) goto fail;
    bool truncate_at_max_iters = relation->bound.tag == JAZZ_MODEL_RECURSION_MAX_DEPTH;
    u32 max_iters = relation->bound.tag == JAZZ_MODEL_RECURSION_FIXPOINT ? JAZZ_ENGINE_FIXPOINT_MAX_ITERS : relation->bound.max_depth;
    if (!jz_btree_eq(&seed.fields, &step.fields, NULL)) { e = JE_UR_OP("recursive seed and step outputs must have the same fields"); goto fail; }
    lri_empty(out, GB(recursive_with_step_witness)(lri_take_graph(&seed), lri_take_graph(&step), lri_take_graph(&witness),
                                                   s_clone(&relation->frontier), max_iters, truncate_at_max_iters));
    out->has_root_source = true;
    JE_RES(ResolvedSource__Clone__clone)(&out->root_source, root_source);
    jz_btree_drop(&out->fields);
    out->fields = seed.fields;
    memset(&seed.fields, 0, sizeof seed.fields);
fail:
    groove_gb_drop(root_graph);
    JE_GL(LoweredRelationInput__drop)(&seed);
    JE_GL(LoweredRelationInput__drop)(&step);
    JE_GL(LoweredRelationInput__drop)(&witness);
    jz_btree_drop(&owned_children);
    jz_btree_drop(&witness_children);
    return e;
}

/* ---------------------------------------------------------------- plan entry */
jazz_engine_UrErr JE_GL(lower_plan_steps)(groove_GraphBuilder *graph, const jazz_engine_AnalyzedQueryPlan *plan, const jazz_engine_ResolvedSource *root_source,
                                          const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, LRI *out) {
    switch (plan->tag) {
    case JAZZ_ENGINE_AQP_LINEAR: {
        const jazz_engine_RowSetOutputRequest *output = &request->request->output;
        bool retain_final_project_input_fields = output->has_app_rows && output->app_rows.projection.tag == JAZZ_ENGINE_PP_RELATION;
        return lower_linear_plan_steps_cached(graph, &plan->u.linear, root_source, resolved_sources, request, NULL, NULL,
                                              retain_final_project_input_fields, out);
    }
    case JAZZ_ENGINE_AQP_UNION: return lower_union_plan(&plan->u.union_, graph, root_source, resolved_sources, request, out);
    case JAZZ_ENGINE_AQP_CORRELATED_PATH: return lower_correlated_path_plan(graph, &plan->u.correlated, root_source, resolved_sources, request, out);
    default: return JE_GL(lower_recursive_relation_cached)(graph, &plan->u.recursive, root_source, resolved_sources, request, NULL, out);
    }
}
