/* input.rs: normalized row-set program input and its derived impls. */
#include "mod.h"

/* ---------------------------------------------------------------- SourceRole / SourceId */
static const char *const ROLE_NAMES[] = {"Root", "Alias", "RecursiveSeed", "RecursiveStep", "CorrelatedChild", "Policy"};

void JE_IN(SourceRole__Clone__clone)(jazz_engine_SourceRole *dst, const jazz_engine_SourceRole *src) {
    dst->tag = src->tag;
    dst->name = src->tag == JAZZ_ENGINE_ROLE_ROOT ? (jz_str){0} : jz_str_clone(&src->name);
}
static void role_clone_v(void *d, const void *s) { JE_IN(SourceRole__Clone__clone)((jazz_engine_SourceRole *)d, (const jazz_engine_SourceRole *)s); }
static void role_drop_v(void *r) { jz_str_drop(&((jazz_engine_SourceRole *)r)->name); }
static int role_cmp(const jazz_engine_SourceRole *a, const jazz_engine_SourceRole *b) {
    if (a->tag != b->tag) return a->tag < b->tag ? -1 : 1;
    if (a->tag == JAZZ_ENGINE_ROLE_ROOT) return 0;
    return jz_str_cmp(&a->name, &b->name);
}

void JE_IN(SourceId__Clone__clone)(jazz_engine_SourceId *dst, const jazz_engine_SourceId *src) {
    dst->table = jz_str_clone(&src->table);
    jz_vec_clone(&dst->path, &src->path, sizeof(jazz_engine_SourceRole), role_clone_v);
}
void JE_IN(SourceId__drop)(jazz_engine_SourceId *s) {
    jz_str_drop(&s->table);
    jz_vec_drop(&s->path, sizeof(jazz_engine_SourceRole), role_drop_v);
}
bool JE_IN(SourceId__PartialEq__eq)(const jazz_engine_SourceId *a, const jazz_engine_SourceId *b) {
    if (!jz_str_eq(&a->table, &b->table) || a->path.len != b->path.len) return false;
    for (u32 i = 0; i < a->path.len; i++)
        if (role_cmp(&a->path.ptr[i], &b->path.ptr[i]) != 0) return false;
    return true;
}
int JE_IN(SourceId__Ord__cmp)(const jazz_engine_SourceId *a, const jazz_engine_SourceId *b) {
    int c = jz_str_cmp(&a->table, &b->table);
    if (c) return c;
    u32 n = a->path.len < b->path.len ? a->path.len : b->path.len;
    for (u32 i = 0; i < n; i++) {
        c = role_cmp(&a->path.ptr[i], &b->path.ptr[i]);
        if (c) return c;
    }
    return a->path.len < b->path.len ? -1 : a->path.len > b->path.len;
}
u64 JE_IN(SourceId__Hash__hash)(const jazz_engine_SourceId *s) {
    u64 h = jz_str_hash(&s->table);
    h = jz_hash_combine(h, s->path.len);
    JZ_VEC_FOR(const jazz_engine_SourceRole, r, s->path) {
        h = jz_hash_combine(h, r->tag);
        if (r->tag != JAZZ_ENGINE_ROLE_ROOT) h = jz_hash_combine(h, jz_str_hash(&r->name));
    }
    return h;
}
static void role_debug_v(jz_str *out, const void *v) {
    const jazz_engine_SourceRole *r = (const jazz_engine_SourceRole *)v;
    if (r->tag == JAZZ_ENGINE_ROLE_ROOT) {
        jz_str_push_cstr(out, "Root");
        return;
    }
    jazz_engine__fmt__tuple_begin(out, ROLE_NAMES[r->tag]);
    jazz_engine__fmt__str(out, &r->name);
    jazz_engine__fmt__tuple_end(out);
}
void JE_IN(SourceId__Debug__fmt)(jz_str *out, const jazz_engine_SourceId *s) {
    jazz_engine__fmt__struct_begin(out, "SourceId");
    jazz_engine__fmt__field(out, "table", true);
    jazz_engine__fmt__str(out, &s->table);
    jazz_engine__fmt__field(out, "path", false);
    jazz_engine__fmt__struct_begin(out, "SourcePath");
    jazz_engine__fmt__field(out, "components", true);
    jazz_engine__fmt__list(out, s->path.ptr, s->path.len, sizeof(jazz_engine_SourceRole), role_debug_v);
    jazz_engine__fmt__struct_end(out);
    jazz_engine__fmt__struct_end(out);
}

/* ProgramSourceRole has the same variant order as SourceRole; the protocol mirror stores
 * a tag vector plus names interned alongside. */
void JE_IN(SourceId__program_source_id)(const jazz_engine_SourceId *s, jazz_protocol_ProgramSourceId *out) {
    out->table = jazz_protocol__intern_str(&s->table);
    memset(&out->path, 0, sizeof out->path);
    JZ_VEC_FOR(const jazz_engine_SourceRole, role, s->path) {
        switch (role->tag) {
        case JAZZ_ENGINE_ROLE_ROOT: JZ_VEC_PUSH(out->path, (u8)0); break;
        case JAZZ_ENGINE_ROLE_ALIAS: JZ_VEC_PUSH(out->path, (u8)1); jazz_protocol__intern_str(&role->name); break;
        case JAZZ_ENGINE_ROLE_RECURSIVE_SEED: JZ_VEC_PUSH(out->path, (u8)2); jazz_protocol__intern_str(&role->name); break;
        case JAZZ_ENGINE_ROLE_RECURSIVE_STEP: JZ_VEC_PUSH(out->path, (u8)3); jazz_protocol__intern_str(&role->name); break;
        case JAZZ_ENGINE_ROLE_CORRELATED_CHILD: JZ_VEC_PUSH(out->path, (u8)4); jazz_protocol__intern_str(&role->name); break;
        case JAZZ_ENGINE_ROLE_POLICY: JZ_VEC_PUSH(out->path, (u8)5); jazz_protocol__intern_str(&role->name); break;
        }
    }
}

void jazz_engine__sid_drop_v(void *s) { sid_drop((jazz_engine_SourceId *)s); }
void jazz_engine__sid_clone_v(void *d, const void *s) { sid_clone((jazz_engine_SourceId *)d, (const jazz_engine_SourceId *)s); }
int jazz_engine__sid_cmp_v(const void *a, const void *b, void *ctx) { return sid_cmp((const jazz_engine_SourceId *)a, (const jazz_engine_SourceId *)b); }
void jazz_engine__sid_debug_v(jz_str *out, const void *s) { sid_debug(out, (const jazz_engine_SourceId *)s); }
jazz_engine_SourceId jazz_engine__sid_cloned(const jazz_engine_SourceId *s) { jazz_engine_SourceId d; sid_clone(&d, s); return d; }
static u64 sid_hash_v(const void *s) { return JE_IN(SourceId__Hash__hash)((const jazz_engine_SourceId *)s); }
static bool sid_eq_v(const void *a, const void *b) { return sid_eq((const jazz_engine_SourceId *)a, (const jazz_engine_SourceId *)b); }
void jazz_engine__sidset_clone(jz_btree *dst, const jz_btree *src) { jz_btree_clone(dst, src, jazz_engine__sid_clone_v, NULL); }

/* ---------------------------------------------------------------- ProgramPathId */
void JE_IN(ProgramPathId__Clone__clone)(jazz_engine_ProgramPathId *dst, const jazz_engine_ProgramPathId *src) {
    sid_clone(&dst->owner, &src->owner);
    sid_clone(&dst->child, &src->child);
}
void JE_IN(ProgramPathId__drop)(jazz_engine_ProgramPathId *p) { sid_drop(&p->owner); sid_drop(&p->child); }
bool JE_IN(ProgramPathId__PartialEq__eq)(const jazz_engine_ProgramPathId *a, const jazz_engine_ProgramPathId *b) {
    return sid_eq(&a->owner, &b->owner) && sid_eq(&a->child, &b->child);
}
int JE_IN(ProgramPathId__Ord__cmp)(const jazz_engine_ProgramPathId *a, const jazz_engine_ProgramPathId *b) {
    int c = sid_cmp(&a->owner, &b->owner);
    return c ? c : sid_cmp(&a->child, &b->child);
}
void JE_IN(ProgramPathId__Debug__fmt)(jz_str *out, const jazz_engine_ProgramPathId *p) {
    jazz_engine__fmt__struct_begin(out, "ProgramPathId");
    jazz_engine__fmt__field(out, "owner", true);
    sid_debug(out, &p->owner);
    jazz_engine__fmt__field(out, "child", false);
    sid_debug(out, &p->child);
    jazz_engine__fmt__struct_end(out);
}

/* ---------------------------------------------------------------- ClaimPath */
void JE_IN(ClaimPath__Clone__clone)(jazz_engine_ClaimPath *dst, const jazz_engine_ClaimPath *src) { jazz_engine__strvec_clone(&dst->segments, &src->segments); }
void JE_IN(ClaimPath__drop)(jazz_engine_ClaimPath *p) { jazz_engine__strvec_drop(&p->segments); }
void JE_IN(ClaimPath__Debug__fmt)(jz_str *out, const jazz_engine_ClaimPath *p) {
    jazz_engine__fmt__tuple_begin(out, "ClaimPath");
    jazz_engine__fmt__strvec(out, &p->segments);
    jazz_engine__fmt__tuple_end(out);
}

/* ---------------------------------------------------------------- NormalizedValueRef */
void JE_IN(NormalizedValueRef__Clone__clone)(jazz_engine_NormalizedValueRef *dst, const jazz_engine_NormalizedValueRef *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    dst->sub = src->sub;
    switch (src->tag) {
    case JAZZ_ENGINE_NVR_SOURCE_FIELD:
        sid_clone(&dst->u.source_field.source, &src->u.source_field.source);
        dst->u.source_field.field = jz_str_clone(&src->u.source_field.field);
        break;
    case JAZZ_ENGINE_NVR_PARAM: dst->u.param = jz_str_clone(&src->u.param); break;
    case JAZZ_ENGINE_NVR_CLAIM: JE_IN(ClaimPath__Clone__clone)(&dst->u.claim, &src->u.claim); break;
    case JAZZ_ENGINE_NVR_FRONTIER_COLUMN:
        dst->u.frontier_column.frontier = jz_str_clone(&src->u.frontier_column.frontier);
        dst->u.frontier_column.field = jz_str_clone(&src->u.frontier_column.field);
        break;
    case JAZZ_ENGINE_NVR_ROW_ID:
        if (src->sub == JAZZ_ENGINE_ROWID_SOURCE) sid_clone(&dst->u.source, &src->u.source);
        else dst->u.frontier = jz_str_clone(&src->u.frontier);
        break;
    case JAZZ_ENGINE_NVR_PROVENANCE: sid_clone(&dst->u.source, &src->u.source); break;
    case JAZZ_ENGINE_NVR_LITERAL: dst->u.literal = jz_bytes_clone(&src->u.literal); break;
    case JAZZ_ENGINE_NVR_TEMPLATE_LITERAL: dst->u.template_literal = src->u.template_literal; break;
    }
}
void JE_IN(NormalizedValueRef__drop)(jazz_engine_NormalizedValueRef *v) {
    switch (v->tag) {
    case JAZZ_ENGINE_NVR_SOURCE_FIELD: sid_drop(&v->u.source_field.source); jz_str_drop(&v->u.source_field.field); break;
    case JAZZ_ENGINE_NVR_PARAM: jz_str_drop(&v->u.param); break;
    case JAZZ_ENGINE_NVR_CLAIM: JE_IN(ClaimPath__drop)(&v->u.claim); break;
    case JAZZ_ENGINE_NVR_FRONTIER_COLUMN: jz_str_drop(&v->u.frontier_column.frontier); jz_str_drop(&v->u.frontier_column.field); break;
    case JAZZ_ENGINE_NVR_ROW_ID:
        if (v->sub == JAZZ_ENGINE_ROWID_SOURCE) sid_drop(&v->u.source);
        else jz_str_drop(&v->u.frontier);
        break;
    case JAZZ_ENGINE_NVR_PROVENANCE: sid_drop(&v->u.source); break;
    case JAZZ_ENGINE_NVR_LITERAL: jz_bytes_drop(&v->u.literal); break;
    default: break;
    }
}
int JE_IN(NormalizedValueRef__Ord__cmp)(const jazz_engine_NormalizedValueRef *a, const jazz_engine_NormalizedValueRef *b) {
    if (a->tag != b->tag) return a->tag < b->tag ? -1 : 1;
    int c;
    switch (a->tag) {
    case JAZZ_ENGINE_NVR_SOURCE_FIELD:
        c = sid_cmp(&a->u.source_field.source, &b->u.source_field.source);
        return c ? c : jz_str_cmp(&a->u.source_field.field, &b->u.source_field.field);
    case JAZZ_ENGINE_NVR_PARAM: return jz_str_cmp(&a->u.param, &b->u.param);
    case JAZZ_ENGINE_NVR_CLAIM: return jazz_engine__strvec_cmp(&a->u.claim.segments, &b->u.claim.segments);
    case JAZZ_ENGINE_NVR_FRONTIER_COLUMN:
        c = jz_str_cmp(&a->u.frontier_column.frontier, &b->u.frontier_column.frontier);
        return c ? c : jz_str_cmp(&a->u.frontier_column.field, &b->u.frontier_column.field);
    case JAZZ_ENGINE_NVR_ROW_ID:
        if (a->sub != b->sub) return a->sub < b->sub ? -1 : 1;
        return a->sub == JAZZ_ENGINE_ROWID_SOURCE ? sid_cmp(&a->u.source, &b->u.source) : jz_str_cmp(&a->u.frontier, &b->u.frontier);
    case JAZZ_ENGINE_NVR_PROVENANCE:
        c = sid_cmp(&a->u.source, &b->u.source);
        return c ? c : (a->sub < b->sub ? -1 : a->sub > b->sub);
    case JAZZ_ENGINE_NVR_LITERAL: return jz_bytes_cmp(&a->u.literal, &b->u.literal);
    case JAZZ_ENGINE_NVR_TEMPLATE_LITERAL:
        return a->u.template_literal < b->u.template_literal ? -1 : a->u.template_literal > b->u.template_literal;
    }
    return 0;
}
bool JE_IN(NormalizedValueRef__PartialEq__eq)(const jazz_engine_NormalizedValueRef *a, const jazz_engine_NormalizedValueRef *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_ENGINE_NVR_SOURCE_FIELD:
        return sid_eq(&a->u.source_field.source, &b->u.source_field.source) && jz_str_eq(&a->u.source_field.field, &b->u.source_field.field);
    case JAZZ_ENGINE_NVR_PARAM: return jz_str_eq(&a->u.param, &b->u.param);
    case JAZZ_ENGINE_NVR_CLAIM: return jazz_engine__strvec_eq(&a->u.claim.segments, &b->u.claim.segments);
    case JAZZ_ENGINE_NVR_FRONTIER_COLUMN:
        return jz_str_eq(&a->u.frontier_column.frontier, &b->u.frontier_column.frontier) && jz_str_eq(&a->u.frontier_column.field, &b->u.frontier_column.field);
    case JAZZ_ENGINE_NVR_ROW_ID:
        if (a->sub != b->sub) return false;
        return a->sub == JAZZ_ENGINE_ROWID_SOURCE ? sid_eq(&a->u.source, &b->u.source) : jz_str_eq(&a->u.frontier, &b->u.frontier);
    case JAZZ_ENGINE_NVR_PROVENANCE: return a->sub == b->sub && sid_eq(&a->u.source, &b->u.source);
    case JAZZ_ENGINE_NVR_LITERAL: return jz_bytes_eq(&a->u.literal, &b->u.literal);
    case JAZZ_ENGINE_NVR_TEMPLATE_LITERAL: return a->u.template_literal == b->u.template_literal;
    }
    return false;
}
static const char *const PROVENANCE_NAMES[] = {"CreatedAt", "CreatedBy", "UpdatedAt", "UpdatedBy"};
static void frontier_debug(jz_str *out, const jz_str *f) {
    jazz_engine__fmt__tuple_begin(out, "FrontierId");
    jazz_engine__fmt__str(out, f);
    jazz_engine__fmt__tuple_end(out);
}
void JE_IN(NormalizedValueRef__Debug__fmt)(jz_str *out, const jazz_engine_NormalizedValueRef *v) {
    switch (v->tag) {
    case JAZZ_ENGINE_NVR_SOURCE_FIELD:
        jazz_engine__fmt__struct_begin(out, "SourceField");
        jazz_engine__fmt__field(out, "source", true);
        sid_debug(out, &v->u.source_field.source);
        jazz_engine__fmt__field(out, "field", false);
        jazz_engine__fmt__str(out, &v->u.source_field.field);
        jazz_engine__fmt__struct_end(out);
        break;
    case JAZZ_ENGINE_NVR_PARAM:
        jazz_engine__fmt__tuple_begin(out, "Param");
        jazz_engine__fmt__str(out, &v->u.param);
        jazz_engine__fmt__tuple_end(out);
        break;
    case JAZZ_ENGINE_NVR_CLAIM:
        jazz_engine__fmt__tuple_begin(out, "Claim");
        JE_IN(ClaimPath__Debug__fmt)(out, &v->u.claim);
        jazz_engine__fmt__tuple_end(out);
        break;
    case JAZZ_ENGINE_NVR_FRONTIER_COLUMN:
        jazz_engine__fmt__struct_begin(out, "FrontierColumn");
        jazz_engine__fmt__field(out, "frontier", true);
        frontier_debug(out, &v->u.frontier_column.frontier);
        jazz_engine__fmt__field(out, "field", false);
        jazz_engine__fmt__str(out, &v->u.frontier_column.field);
        jazz_engine__fmt__struct_end(out);
        break;
    case JAZZ_ENGINE_NVR_ROW_ID:
        jazz_engine__fmt__tuple_begin(out, "RowId");
        if (v->sub == JAZZ_ENGINE_ROWID_SOURCE) {
            jazz_engine__fmt__tuple_begin(out, "Source");
            sid_debug(out, &v->u.source);
        } else {
            jazz_engine__fmt__tuple_begin(out, "Frontier");
            frontier_debug(out, &v->u.frontier);
        }
        jazz_engine__fmt__tuple_end(out);
        jazz_engine__fmt__tuple_end(out);
        break;
    case JAZZ_ENGINE_NVR_PROVENANCE:
        jazz_engine__fmt__struct_begin(out, "Provenance");
        jazz_engine__fmt__field(out, "source", true);
        sid_debug(out, &v->u.source);
        jazz_engine__fmt__field(out, "field", false);
        jz_str_push_cstr(out, PROVENANCE_NAMES[v->sub]);
        jazz_engine__fmt__struct_end(out);
        break;
    case JAZZ_ENGINE_NVR_LITERAL:
        jazz_engine__fmt__tuple_begin(out, "Literal");
        jz_str_push_char(out, '[');
        for (u32 i = 0; i < v->u.literal.len; i++) {
            jazz_engine__fmt__sep(out, i == 0);
            jz_str_appendf(out, "%u", v->u.literal.ptr[i]);
        }
        jz_str_push_char(out, ']');
        jazz_engine__fmt__tuple_end(out);
        break;
    case JAZZ_ENGINE_NVR_TEMPLATE_LITERAL:
        jz_str_appendf(out, "TemplateLiteral(%u)", v->u.template_literal);
        break;
    }
}
void jazz_engine__nvr_drop_v(void *v) { nvr_drop((jazz_engine_NormalizedValueRef *)v); }
void jazz_engine__nvr_clone_v(void *d, const void *s) { nvr_clone((jazz_engine_NormalizedValueRef *)d, (const jazz_engine_NormalizedValueRef *)s); }
void jazz_engine__nvr_debug_v(jz_str *out, const void *v) { nvr_debug(out, (const jazz_engine_NormalizedValueRef *)v); }
int jazz_engine__nvr_cmp_v(const void *a, const void *b, void *ctx) { return nvr_cmp((const jazz_engine_NormalizedValueRef *)a, (const jazz_engine_NormalizedValueRef *)b); }
void jazz_engine__nvrvec_clone(jazz_engine_NvrVec *dst, const jazz_engine_NvrVec *src) { jz_vec_clone(dst, src, sizeof(jazz_engine_NormalizedValueRef), jazz_engine__nvr_clone_v); }
void jazz_engine__nvrvec_drop(jazz_engine_NvrVec *v) { jz_vec_drop(v, sizeof(jazz_engine_NormalizedValueRef), jazz_engine__nvr_drop_v); }
jazz_engine_NormalizedValueRef jazz_engine__nvr_cloned(const jazz_engine_NormalizedValueRef *v) { jazz_engine_NormalizedValueRef d; nvr_clone(&d, v); return d; }
static bool nvrvec_eq(const jazz_engine_NvrVec *a, const jazz_engine_NvrVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!nvr_eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
static int nvrvec_cmp(const jazz_engine_NvrVec *a, const jazz_engine_NvrVec *b) {
    u32 n = a->len < b->len ? a->len : b->len;
    for (u32 i = 0; i < n; i++) {
        int c = nvr_cmp(&a->ptr[i], &b->ptr[i]);
        if (c) return c;
    }
    return a->len < b->len ? -1 : a->len > b->len;
}

/* ---------------------------------------------------------------- PredicateExpr */
void JE_IN(PredicateExpr__Clone__clone)(jazz_engine_PredicateExpr *dst, const jazz_engine_PredicateExpr *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    dst->op = src->op;
    switch (src->tag) {
    case JAZZ_ENGINE_PRED_TRUE:
    case JAZZ_ENGINE_PRED_FALSE: break;
    case JAZZ_ENGINE_PRED_COMPARE:
    case JAZZ_ENGINE_PRED_ARRAY_CONTAINS:
    case JAZZ_ENGINE_PRED_TEXT_CONTAINS:
        nvr_clone(&dst->u.pair.left, &src->u.pair.left);
        nvr_clone(&dst->u.pair.right, &src->u.pair.right);
        break;
    case JAZZ_ENGINE_PRED_IN:
        nvr_clone(&dst->u.in.value, &src->u.in.value);
        jazz_engine__nvrvec_clone(&dst->u.in.options, &src->u.in.options);
        break;
    case JAZZ_ENGINE_PRED_IS_NULL:
    case JAZZ_ENGINE_PRED_IS_NOT_NULL: nvr_clone(&dst->u.value, &src->u.value); break;
    case JAZZ_ENGINE_PRED_ENUM_MATCH:
        nvr_clone(&dst->u.enum_match.value, &src->u.enum_match.value);
        dst->u.enum_match.case_tag = src->u.enum_match.case_tag;
        dst->u.enum_match.payload = JZ_NEW(jazz_engine_PredicateExpr);
        pred_clone(dst->u.enum_match.payload, src->u.enum_match.payload);
        break;
    case JAZZ_ENGINE_PRED_AND:
    case JAZZ_ENGINE_PRED_OR: jz_vec_clone(&dst->u.children, &src->u.children, sizeof(jazz_engine_PredicateExpr), jazz_engine__pred_clone_v); break;
    case JAZZ_ENGINE_PRED_NOT:
        dst->u.not_ = JZ_NEW(jazz_engine_PredicateExpr);
        pred_clone(dst->u.not_, src->u.not_);
        break;
    }
}
void JE_IN(PredicateExpr__drop)(jazz_engine_PredicateExpr *p) {
    switch (p->tag) {
    case JAZZ_ENGINE_PRED_COMPARE:
    case JAZZ_ENGINE_PRED_ARRAY_CONTAINS:
    case JAZZ_ENGINE_PRED_TEXT_CONTAINS: nvr_drop(&p->u.pair.left); nvr_drop(&p->u.pair.right); break;
    case JAZZ_ENGINE_PRED_IN: nvr_drop(&p->u.in.value); jazz_engine__nvrvec_drop(&p->u.in.options); break;
    case JAZZ_ENGINE_PRED_IS_NULL:
    case JAZZ_ENGINE_PRED_IS_NOT_NULL: nvr_drop(&p->u.value); break;
    case JAZZ_ENGINE_PRED_ENUM_MATCH:
        nvr_drop(&p->u.enum_match.value);
        pred_drop(p->u.enum_match.payload);
        jz_free(p->u.enum_match.payload);
        break;
    case JAZZ_ENGINE_PRED_AND:
    case JAZZ_ENGINE_PRED_OR: jz_vec_drop(&p->u.children, sizeof(jazz_engine_PredicateExpr), jazz_engine__pred_drop_v); break;
    case JAZZ_ENGINE_PRED_NOT: pred_drop(p->u.not_); jz_free(p->u.not_); break;
    default: break;
    }
}
bool JE_IN(PredicateExpr__PartialEq__eq)(const jazz_engine_PredicateExpr *a, const jazz_engine_PredicateExpr *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_ENGINE_PRED_TRUE:
    case JAZZ_ENGINE_PRED_FALSE: return true;
    case JAZZ_ENGINE_PRED_COMPARE:
        if (a->op != b->op) return false;
        /* fallthrough */
    case JAZZ_ENGINE_PRED_ARRAY_CONTAINS:
    case JAZZ_ENGINE_PRED_TEXT_CONTAINS: return nvr_eq(&a->u.pair.left, &b->u.pair.left) && nvr_eq(&a->u.pair.right, &b->u.pair.right);
    case JAZZ_ENGINE_PRED_IN: return nvr_eq(&a->u.in.value, &b->u.in.value) && nvrvec_eq(&a->u.in.options, &b->u.in.options);
    case JAZZ_ENGINE_PRED_IS_NULL:
    case JAZZ_ENGINE_PRED_IS_NOT_NULL: return nvr_eq(&a->u.value, &b->u.value);
    case JAZZ_ENGINE_PRED_ENUM_MATCH:
        return nvr_eq(&a->u.enum_match.value, &b->u.enum_match.value) && a->u.enum_match.case_tag == b->u.enum_match.case_tag &&
               JE_IN(PredicateExpr__PartialEq__eq)(a->u.enum_match.payload, b->u.enum_match.payload);
    case JAZZ_ENGINE_PRED_AND:
    case JAZZ_ENGINE_PRED_OR:
        if (a->u.children.len != b->u.children.len) return false;
        for (u32 i = 0; i < a->u.children.len; i++)
            if (!JE_IN(PredicateExpr__PartialEq__eq)(&a->u.children.ptr[i], &b->u.children.ptr[i])) return false;
        return true;
    case JAZZ_ENGINE_PRED_NOT: return JE_IN(PredicateExpr__PartialEq__eq)(a->u.not_, b->u.not_);
    }
    return false;
}
static const char *const CMP_NAMES[] = {"Eq", "Ne", "Lt", "Lte", "Gt", "Gte"};
static void pred_debug_v(jz_str *out, const void *p) { pred_debug(out, (const jazz_engine_PredicateExpr *)p); }
static void pair_debug(jz_str *out, const char *name, const char *l, const char *r, const jazz_engine_PredicateExpr *p) {
    jazz_engine__fmt__struct_begin(out, name);
    jazz_engine__fmt__field(out, l, true);
    nvr_debug(out, &p->u.pair.left);
    jazz_engine__fmt__field(out, r, false);
    nvr_debug(out, &p->u.pair.right);
    jazz_engine__fmt__struct_end(out);
}
void JE_IN(PredicateExpr__Debug__fmt)(jz_str *out, const jazz_engine_PredicateExpr *p) {
    switch (p->tag) {
    case JAZZ_ENGINE_PRED_TRUE: jz_str_push_cstr(out, "True"); break;
    case JAZZ_ENGINE_PRED_FALSE: jz_str_push_cstr(out, "False"); break;
    case JAZZ_ENGINE_PRED_COMPARE:
        jazz_engine__fmt__struct_begin(out, "Compare");
        jazz_engine__fmt__field(out, "left", true);
        nvr_debug(out, &p->u.pair.left);
        jazz_engine__fmt__field(out, "op", false);
        jz_str_push_cstr(out, CMP_NAMES[p->op]);
        jazz_engine__fmt__field(out, "right", false);
        nvr_debug(out, &p->u.pair.right);
        jazz_engine__fmt__struct_end(out);
        break;
    case JAZZ_ENGINE_PRED_IN:
        jazz_engine__fmt__struct_begin(out, "In");
        jazz_engine__fmt__field(out, "value", true);
        nvr_debug(out, &p->u.in.value);
        jazz_engine__fmt__field(out, "options", false);
        jazz_engine__fmt__list(out, p->u.in.options.ptr, p->u.in.options.len, sizeof(jazz_engine_NormalizedValueRef), jazz_engine__nvr_debug_v);
        jazz_engine__fmt__struct_end(out);
        break;
    case JAZZ_ENGINE_PRED_ARRAY_CONTAINS: pair_debug(out, "ArrayContains", "value", "needle", p); break;
    case JAZZ_ENGINE_PRED_TEXT_CONTAINS: pair_debug(out, "TextContains", "value", "needle", p); break;
    case JAZZ_ENGINE_PRED_IS_NULL:
    case JAZZ_ENGINE_PRED_IS_NOT_NULL:
        jazz_engine__fmt__tuple_begin(out, p->tag == JAZZ_ENGINE_PRED_IS_NULL ? "IsNull" : "IsNotNull");
        nvr_debug(out, &p->u.value);
        jazz_engine__fmt__tuple_end(out);
        break;
    case JAZZ_ENGINE_PRED_ENUM_MATCH:
        jazz_engine__fmt__struct_begin(out, "EnumMatch");
        jazz_engine__fmt__field(out, "value", true);
        nvr_debug(out, &p->u.enum_match.value);
        jazz_engine__fmt__field(out, "case_tag", false);
        jz_str_appendf(out, "%u", p->u.enum_match.case_tag);
        jazz_engine__fmt__field(out, "payload", false);
        pred_debug(out, p->u.enum_match.payload);
        jazz_engine__fmt__struct_end(out);
        break;
    case JAZZ_ENGINE_PRED_AND:
    case JAZZ_ENGINE_PRED_OR:
        jazz_engine__fmt__tuple_begin(out, p->tag == JAZZ_ENGINE_PRED_AND ? "And" : "Or");
        jazz_engine__fmt__list(out, p->u.children.ptr, p->u.children.len, sizeof(jazz_engine_PredicateExpr), pred_debug_v);
        jazz_engine__fmt__tuple_end(out);
        break;
    case JAZZ_ENGINE_PRED_NOT:
        jazz_engine__fmt__tuple_begin(out, "Not");
        pred_debug(out, p->u.not_);
        jazz_engine__fmt__tuple_end(out);
        break;
    }
}
void jazz_engine__pred_drop_v(void *p) { pred_drop((jazz_engine_PredicateExpr *)p); }
void jazz_engine__pred_clone_v(void *d, const void *s) { pred_clone((jazz_engine_PredicateExpr *)d, (const jazz_engine_PredicateExpr *)s); }
jazz_engine_PredicateExpr jazz_engine__pred_cloned(const jazz_engine_PredicateExpr *p) { jazz_engine_PredicateExpr d; pred_clone(&d, p); return d; }

/* predicate_value_walker!(try_for_each_value) */
jz_err JE_IN(PredicateExpr__try_for_each_value)(const jazz_engine_PredicateExpr *p, jazz_engine_NvrVisit f, void *ctx) {
    switch (p->tag) {
    case JAZZ_ENGINE_PRED_TRUE:
    case JAZZ_ENGINE_PRED_FALSE: break;
    case JAZZ_ENGINE_PRED_COMPARE:
    case JAZZ_ENGINE_PRED_ARRAY_CONTAINS:
    case JAZZ_ENGINE_PRED_TEXT_CONTAINS:
        JZ_TRY(f(&p->u.pair.left, ctx));
        JZ_TRY(f(&p->u.pair.right, ctx));
        break;
    case JAZZ_ENGINE_PRED_IN:
        JZ_TRY(f(&p->u.in.value, ctx));
        JZ_VEC_FOR(const jazz_engine_NormalizedValueRef, option, p->u.in.options) JZ_TRY(f(option, ctx));
        break;
    case JAZZ_ENGINE_PRED_IS_NULL:
    case JAZZ_ENGINE_PRED_IS_NOT_NULL: JZ_TRY(f(&p->u.value, ctx)); break;
    case JAZZ_ENGINE_PRED_ENUM_MATCH:
        JZ_TRY(f(&p->u.enum_match.value, ctx));
        JZ_TRY(JE_IN(PredicateExpr__try_for_each_value)(p->u.enum_match.payload, f, ctx));
        break;
    case JAZZ_ENGINE_PRED_AND:
    case JAZZ_ENGINE_PRED_OR:
        JZ_VEC_FOR(const jazz_engine_PredicateExpr, child, p->u.children) JZ_TRY(JE_IN(PredicateExpr__try_for_each_value)(child, f, ctx));
        break;
    case JAZZ_ENGINE_PRED_NOT: JZ_TRY(JE_IN(PredicateExpr__try_for_each_value)(p->u.not_, f, ctx)); break;
    }
    return JZ_OK;
}

/* predicate_value_walker!(try_for_each_value_mut, mut) */
jz_err JE_IN(PredicateExpr__try_for_each_value_mut)(jazz_engine_PredicateExpr *p, jazz_engine_NvrVisitMut f, void *ctx) {
    switch (p->tag) {
    case JAZZ_ENGINE_PRED_TRUE:
    case JAZZ_ENGINE_PRED_FALSE: break;
    case JAZZ_ENGINE_PRED_COMPARE:
    case JAZZ_ENGINE_PRED_ARRAY_CONTAINS:
    case JAZZ_ENGINE_PRED_TEXT_CONTAINS:
        JZ_TRY(f(&p->u.pair.left, ctx));
        JZ_TRY(f(&p->u.pair.right, ctx));
        break;
    case JAZZ_ENGINE_PRED_IN:
        JZ_TRY(f(&p->u.in.value, ctx));
        JZ_VEC_FOR(jazz_engine_NormalizedValueRef, option, p->u.in.options) JZ_TRY(f(option, ctx));
        break;
    case JAZZ_ENGINE_PRED_IS_NULL:
    case JAZZ_ENGINE_PRED_IS_NOT_NULL: JZ_TRY(f(&p->u.value, ctx)); break;
    case JAZZ_ENGINE_PRED_ENUM_MATCH:
        JZ_TRY(f(&p->u.enum_match.value, ctx));
        JZ_TRY(JE_IN(PredicateExpr__try_for_each_value_mut)(p->u.enum_match.payload, f, ctx));
        break;
    case JAZZ_ENGINE_PRED_AND:
    case JAZZ_ENGINE_PRED_OR:
        JZ_VEC_FOR(jazz_engine_PredicateExpr, child, p->u.children) JZ_TRY(JE_IN(PredicateExpr__try_for_each_value_mut)(child, f, ctx));
        break;
    case JAZZ_ENGINE_PRED_NOT: JZ_TRY(JE_IN(PredicateExpr__try_for_each_value_mut)(p->u.not_, f, ctx)); break;
    }
    return JZ_OK;
}

/* ---------------------------------------------------------------- ResultRowRef / ResultId */
const jz_btree_desc jazz_engine__SOURCE_ID_SET_DESC = {sizeof(jazz_engine_SourceId), 0, jazz_engine__sid_cmp_v, jazz_engine__sid_drop_v, NULL};
const jz_btree_desc jazz_engine__NVR_SET_DESC = {sizeof(jazz_engine_NormalizedValueRef), 0, jazz_engine__nvr_cmp_v, jazz_engine__nvr_drop_v, NULL};
const jz_btree_desc jazz_engine__SOURCE_ID_STR_MAP_DESC = {sizeof(jazz_engine_SourceId), sizeof(jz_str), jazz_engine__sid_cmp_v, jazz_engine__sid_drop_v, jazz_engine__str_drop_v};
const jz_hmap_desc jazz_engine__SOURCE_ID_HSET_DESC = {sizeof(jazz_engine_SourceId), 0, sid_hash_v, sid_eq_v, jazz_engine__sid_drop_v, NULL};

void JE_IN(ResultRowRef__Clone__clone)(jazz_engine_ResultRowRef *dst, const jazz_engine_ResultRowRef *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    switch (src->tag) {
    case JAZZ_ENGINE_RRR_SOURCE: sid_clone(&dst->source, &src->source); break;
    case JAZZ_ENGINE_RRR_SOURCE_ALTERNATIVES: jazz_engine__sidset_clone(&dst->alternatives, &src->alternatives); break;
    case JAZZ_ENGINE_RRR_PROJECTED:
        nvr_clone(&dst->row, &src->row);
        if (src->source_discriminator) {
            dst->source_discriminator = JZ_NEW(jazz_engine_NormalizedValueRef);
            nvr_clone(dst->source_discriminator, src->source_discriminator);
        }
        break;
    }
}
void JE_IN(ResultRowRef__drop)(jazz_engine_ResultRowRef *r) {
    switch (r->tag) {
    case JAZZ_ENGINE_RRR_SOURCE: sid_drop(&r->source); break;
    case JAZZ_ENGINE_RRR_SOURCE_ALTERNATIVES: jz_btree_drop(&r->alternatives); break;
    case JAZZ_ENGINE_RRR_PROJECTED:
        nvr_drop(&r->row);
        if (r->source_discriminator) {
            nvr_drop(r->source_discriminator);
            jz_free(r->source_discriminator);
        }
        break;
    }
}
static int rrr_cmp(const jazz_engine_ResultRowRef *a, const jazz_engine_ResultRowRef *b) {
    if (a->tag != b->tag) return a->tag < b->tag ? -1 : 1;
    switch (a->tag) {
    case JAZZ_ENGINE_RRR_SOURCE: return sid_cmp(&a->source, &b->source);
    case JAZZ_ENGINE_RRR_SOURCE_ALTERNATIVES: {
        jz_btree_iter ia, ib;
        jz_btree_iter_begin(&ia, &a->alternatives);
        jz_btree_iter_begin(&ib, &b->alternatives);
        for (;;) {
            void *ka, *kb, *v;
            bool ha = jz_btree_next(&ia, &ka, &v), hb = jz_btree_next(&ib, &kb, &v);
            if (!ha || !hb) return ha == hb ? 0 : (ha ? 1 : -1);
            int c = sid_cmp((const jazz_engine_SourceId *)ka, (const jazz_engine_SourceId *)kb);
            if (c) return c;
        }
    }
    case JAZZ_ENGINE_RRR_PROJECTED: {
        int c = nvr_cmp(&a->row, &b->row);
        if (c) return c;
        if (!a->source_discriminator || !b->source_discriminator)
            return (a->source_discriminator != NULL) - (b->source_discriminator != NULL);
        return nvr_cmp(a->source_discriminator, b->source_discriminator);
    }
    }
    return 0;
}
static void synthetic_clone(jazz_engine_SyntheticIdentitySpec *dst, const jazz_engine_SyntheticIdentitySpec *src) {
    dst->table = jz_str_clone(&src->table);
    jazz_engine__strvec_clone(&dst->key_columns, &src->key_columns);
    jazz_engine__strvec_clone(&dst->revision_columns, &src->revision_columns);
}
void JE_IN(ResultId__Clone__clone)(jazz_engine_ResultId *dst, const jazz_engine_ResultId *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    switch (src->tag) {
    case JAZZ_ENGINE_RESULT_REAL_ROW:
        dst->u.real_row.table = jz_str_clone(&src->u.real_row.table);
        JE_IN(ResultRowRef__Clone__clone)(&dst->u.real_row.row, &src->u.real_row.row);
        break;
    case JAZZ_ENGINE_RESULT_SYNTHETIC_TUPLE: synthetic_clone(&dst->u.synthetic, &src->u.synthetic); break;
    case JAZZ_ENGINE_RESULT_PATH_TUPLE:
        JE_IN(ProgramPathId__Clone__clone)(&dst->u.path_tuple.path, &src->u.path_tuple.path);
        jazz_engine__nvrvec_clone(&dst->u.path_tuple.revision, &src->u.path_tuple.revision);
        break;
    }
}
void JE_IN(ResultId__drop)(jazz_engine_ResultId *r) {
    switch (r->tag) {
    case JAZZ_ENGINE_RESULT_REAL_ROW: jz_str_drop(&r->u.real_row.table); JE_IN(ResultRowRef__drop)(&r->u.real_row.row); break;
    case JAZZ_ENGINE_RESULT_SYNTHETIC_TUPLE:
        jz_str_drop(&r->u.synthetic.table);
        jazz_engine__strvec_drop(&r->u.synthetic.key_columns);
        jazz_engine__strvec_drop(&r->u.synthetic.revision_columns);
        break;
    case JAZZ_ENGINE_RESULT_PATH_TUPLE:
        JE_IN(ProgramPathId__drop)(&r->u.path_tuple.path);
        jazz_engine__nvrvec_drop(&r->u.path_tuple.revision);
        break;
    }
}
int JE_IN(ResultId__Ord__cmp)(const jazz_engine_ResultId *a, const jazz_engine_ResultId *b) {
    if (a->tag != b->tag) return a->tag < b->tag ? -1 : 1;
    int c;
    switch (a->tag) {
    case JAZZ_ENGINE_RESULT_REAL_ROW:
        c = jz_str_cmp(&a->u.real_row.table, &b->u.real_row.table);
        return c ? c : rrr_cmp(&a->u.real_row.row, &b->u.real_row.row);
    case JAZZ_ENGINE_RESULT_SYNTHETIC_TUPLE:
        c = jz_str_cmp(&a->u.synthetic.table, &b->u.synthetic.table);
        if (c) return c;
        c = jazz_engine__strvec_cmp(&a->u.synthetic.key_columns, &b->u.synthetic.key_columns);
        return c ? c : jazz_engine__strvec_cmp(&a->u.synthetic.revision_columns, &b->u.synthetic.revision_columns);
    case JAZZ_ENGINE_RESULT_PATH_TUPLE:
        c = JE_IN(ProgramPathId__Ord__cmp)(&a->u.path_tuple.path, &b->u.path_tuple.path);
        return c ? c : nvrvec_cmp(&a->u.path_tuple.revision, &b->u.path_tuple.revision);
    }
    return 0;
}
bool JE_IN(ResultId__PartialEq__eq)(const jazz_engine_ResultId *a, const jazz_engine_ResultId *b) { return JE_IN(ResultId__Ord__cmp)(a, b) == 0; }
static void rrr_debug(jz_str *out, const jazz_engine_ResultRowRef *r) {
    switch (r->tag) {
    case JAZZ_ENGINE_RRR_SOURCE:
        jazz_engine__fmt__tuple_begin(out, "Source");
        sid_debug(out, &r->source);
        jazz_engine__fmt__tuple_end(out);
        break;
    case JAZZ_ENGINE_RRR_SOURCE_ALTERNATIVES:
        jazz_engine__fmt__tuple_begin(out, "SourceAlternatives");
        jazz_engine__fmt__set(out, &r->alternatives, jazz_engine__sid_debug_v);
        jazz_engine__fmt__tuple_end(out);
        break;
    case JAZZ_ENGINE_RRR_PROJECTED:
        jazz_engine__fmt__struct_begin(out, "Projected");
        jazz_engine__fmt__field(out, "row", true);
        nvr_debug(out, &r->row);
        jazz_engine__fmt__field(out, "source_discriminator", false);
        if (r->source_discriminator) {
            jazz_engine__fmt__tuple_begin(out, "Some");
            nvr_debug(out, r->source_discriminator);
            jazz_engine__fmt__tuple_end(out);
        } else {
            jazz_engine__fmt__option_none(out);
        }
        jazz_engine__fmt__struct_end(out);
        break;
    }
}
void JE_IN(ResultId__Debug__fmt)(jz_str *out, const jazz_engine_ResultId *r) {
    switch (r->tag) {
    case JAZZ_ENGINE_RESULT_REAL_ROW:
        jazz_engine__fmt__struct_begin(out, "RealRow");
        jazz_engine__fmt__field(out, "table", true);
        jazz_engine__fmt__str(out, &r->u.real_row.table);
        jazz_engine__fmt__field(out, "row", false);
        rrr_debug(out, &r->u.real_row.row);
        break;
    case JAZZ_ENGINE_RESULT_SYNTHETIC_TUPLE:
        jazz_engine__fmt__struct_begin(out, "SyntheticTuple");
        jazz_engine__fmt__field(out, "identity", true);
        jazz_engine__fmt__struct_begin(out, "SyntheticIdentitySpec");
        jazz_engine__fmt__field(out, "table", true);
        jazz_engine__fmt__str(out, &r->u.synthetic.table);
        jazz_engine__fmt__field(out, "key_columns", false);
        jazz_engine__fmt__strvec(out, &r->u.synthetic.key_columns);
        jazz_engine__fmt__field(out, "revision_columns", false);
        jazz_engine__fmt__strvec(out, &r->u.synthetic.revision_columns);
        jazz_engine__fmt__struct_end(out);
        break;
    case JAZZ_ENGINE_RESULT_PATH_TUPLE:
        jazz_engine__fmt__struct_begin(out, "PathTuple");
        jazz_engine__fmt__field(out, "path", true);
        JE_IN(ProgramPathId__Debug__fmt)(out, &r->u.path_tuple.path);
        jazz_engine__fmt__field(out, "revision", false);
        jazz_engine__fmt__list(out, r->u.path_tuple.revision.ptr, r->u.path_tuple.revision.len, sizeof(jazz_engine_NormalizedValueRef), jazz_engine__nvr_debug_v);
        break;
    }
    jazz_engine__fmt__struct_end(out);
}

/* ---------------------------------------------------------------- TypedOutputField */
void JE_IN(TypedOutputField__Clone__clone)(jazz_engine_TypedOutputField *dst, const jazz_engine_TypedOutputField *src) {
    dst->name = jz_str_clone(&src->name);
    groove__records__values__ValueType__Clone__clone(&dst->ty, &src->ty);
}
void JE_IN(TypedOutputField__drop)(jazz_engine_TypedOutputField *f) { jz_str_drop(&f->name); groove__records__values__ValueType__drop(&f->ty); }
void JE_IN(TypedOutputField__Debug__fmt)(jz_str *out, const jazz_engine_TypedOutputField *f) {
    jazz_engine__fmt__struct_begin(out, "TypedOutputField");
    jazz_engine__fmt__field(out, "name", true);
    jazz_engine__fmt__str(out, &f->name);
    jazz_engine__fmt__field(out, "ty", false);
    groove__records__values__ValueType__Debug__fmt(out, &f->ty);
    jazz_engine__fmt__struct_end(out);
}
void jazz_engine__tof_drop_v(void *f) { tof_drop((jazz_engine_TypedOutputField *)f); }
void jazz_engine__tof_clone_v(void *d, const void *s) { tof_clone((jazz_engine_TypedOutputField *)d, (const jazz_engine_TypedOutputField *)s); }
static void tof_debug_v(jz_str *out, const void *f) { JE_IN(TypedOutputField__Debug__fmt)(out, (const jazz_engine_TypedOutputField *)f); }

/* ---------------------------------------------------------------- RowSetExpr */
static void vsc_clone_v(void *d, const void *s) {
    jazz_engine_ValueSourceColumn *dst = d;
    const jazz_engine_ValueSourceColumn *src = s;
    dst->name = jz_str_clone(&src->name);
    nvr_clone(&dst->value, &src->value);
    groove__records__values__ValueType__Clone__clone(&dst->ty, &src->ty);
}
static void vsc_drop_v(void *p) {
    jazz_engine_ValueSourceColumn *c = p;
    jz_str_drop(&c->name);
    nvr_drop(&c->value);
    groove__records__values__ValueType__drop(&c->ty);
}
static void union_input_clone_v(void *d, const void *s) {
    jazz_engine_UnionInput *dst = d;
    const jazz_engine_UnionInput *src = s;
    dst->node = jz_str_clone(&src->node);
    dst->label = jz_str_clone(&src->label);
}
static void union_input_drop_v(void *p) {
    jazz_engine_UnionInput *u = p;
    jz_str_drop(&u->node);
    jz_str_drop(&u->label);
}
static void projection_clone_v(void *d, const void *s) {
    jazz_engine_RowProjection *dst = d;
    const jazz_engine_RowProjection *src = s;
    tof_clone(&dst->output, &src->output);
    nvr_clone(&dst->value, &src->value);
}
static void projection_drop_v(void *p) {
    jazz_engine_RowProjection *r = p;
    tof_drop(&r->output);
    nvr_drop(&r->value);
}
static void order_key_clone_v(void *d, const void *s) {
    jazz_engine_OrderKey *dst = d;
    const jazz_engine_OrderKey *src = s;
    nvr_clone(&dst->value, &src->value);
    dst->direction = src->direction;
}
static void order_key_drop_v(void *p) { nvr_drop(&((jazz_engine_OrderKey *)p)->value); }
static void aggregate_clone_v(void *d, const void *s) {
    jazz_engine_AggregateExpr *dst = d;
    const jazz_engine_AggregateExpr *src = s;
    tof_clone(&dst->output, &src->output);
    dst->function = src->function;
    dst->has_input = src->has_input;
    if (src->has_input) nvr_clone(&dst->input, &src->input);
}
static void aggregate_drop_v(void *p) {
    jazz_engine_AggregateExpr *a = p;
    tof_drop(&a->output);
    if (a->has_input) nvr_drop(&a->input);
}

void JE_IN(RowSetExpr__Clone__clone)(jazz_engine_RowSetExpr *dst, const jazz_engine_RowSetExpr *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    switch (src->tag) {
    case JAZZ_ENGINE_EXPR_SOURCE:
        sid_clone(&dst->u.source.source, &src->u.source.source);
        dst->u.source.visibility = src->u.source.visibility;
        break;
    case JAZZ_ENGINE_EXPR_VALUE_SOURCE:
        dst->u.value_source.shape = jz_str_clone(&src->u.value_source.shape);
        jz_vec_clone(&dst->u.value_source.columns, &src->u.value_source.columns, sizeof(jazz_engine_ValueSourceColumn), vsc_clone_v);
        dst->u.value_source.mode = src->u.value_source.mode;
        break;
    case JAZZ_ENGINE_EXPR_FRONTIER_SOURCE:
        dst->u.frontier_source.frontier = jz_str_clone(&src->u.frontier_source.frontier);
        jz_vec_clone(&dst->u.frontier_source.columns, &src->u.frontier_source.columns, sizeof(jazz_engine_ValueSourceColumn), vsc_clone_v);
        break;
    case JAZZ_ENGINE_EXPR_FILTER:
        dst->u.filter.input = jz_str_clone(&src->u.filter.input);
        pred_clone(&dst->u.filter.predicate, &src->u.filter.predicate);
        break;
    case JAZZ_ENGINE_EXPR_JOIN:
        dst->u.join.left = jz_str_clone(&src->u.join.left);
        dst->u.join.right = jz_str_clone(&src->u.join.right);
        dst->u.join.mode = src->u.join.mode;
        pred_clone(&dst->u.join.on, &src->u.join.on);
        break;
    case JAZZ_ENGINE_EXPR_RECURSIVE_RELATION:
        dst->u.recursive.seed = jz_str_clone(&src->u.recursive.seed);
        dst->u.recursive.step = jz_str_clone(&src->u.recursive.step);
        dst->u.recursive.frontier = jz_str_clone(&src->u.recursive.frontier);
        nvr_clone(&dst->u.recursive.frontier_key, &src->u.recursive.frontier_key);
        jazz_engine__nvrvec_clone(&dst->u.recursive.dedupe_keys, &src->u.recursive.dedupe_keys);
        dst->u.recursive.bound = src->u.recursive.bound;
        break;
    case JAZZ_ENGINE_EXPR_UNION:
        jz_vec_clone(&dst->u.union_.inputs, &src->u.union_.inputs, sizeof(jazz_engine_UnionInput), union_input_clone_v);
        break;
    case JAZZ_ENGINE_EXPR_DISTINCT:
        dst->u.distinct.input = jz_str_clone(&src->u.distinct.input);
        jazz_engine__nvrvec_clone(&dst->u.distinct.keys, &src->u.distinct.keys);
        break;
    case JAZZ_ENGINE_EXPR_PROJECT:
        dst->u.project.input = jz_str_clone(&src->u.project.input);
        jz_vec_clone(&dst->u.project.columns, &src->u.project.columns, sizeof(jazz_engine_RowProjection), projection_clone_v);
        break;
    case JAZZ_ENGINE_EXPR_CORRELATED_PATH_PROJECTION:
        dst->u.correlated.input = jz_str_clone(&src->u.correlated.input);
        dst->u.correlated.child_input = jz_str_clone(&src->u.correlated.child_input);
        JE_IN(ProgramPathId__Clone__clone)(&dst->u.correlated.path, &src->u.correlated.path);
        pred_clone(&dst->u.correlated.correlation, &src->u.correlated.correlation);
        dst->u.correlated.requirement = src->u.correlated.requirement;
        break;
    case JAZZ_ENGINE_EXPR_ORDER_BY:
        dst->u.order_by.input = jz_str_clone(&src->u.order_by.input);
        jz_vec_clone(&dst->u.order_by.keys, &src->u.order_by.keys, sizeof(jazz_engine_OrderKey), order_key_clone_v);
        break;
    case JAZZ_ENGINE_EXPR_SLICE:
        dst->u.slice.input = jz_str_clone(&src->u.slice.input);
        jazz_engine__nvrvec_clone(&dst->u.slice.partition_by, &src->u.slice.partition_by);
        dst->u.slice.has_limit = src->u.slice.has_limit;
        dst->u.slice.limit = src->u.slice.limit;
        dst->u.slice.offset = src->u.slice.offset;
        jazz_engine__nvrvec_clone(&dst->u.slice.tie_breaker, &src->u.slice.tie_breaker);
        dst->u.slice.has_rank_output = src->u.slice.has_rank_output;
        if (src->u.slice.has_rank_output) tof_clone(&dst->u.slice.rank_output, &src->u.slice.rank_output);
        break;
    case JAZZ_ENGINE_EXPR_AGGREGATE:
        dst->u.aggregate.input = jz_str_clone(&src->u.aggregate.input);
        jazz_engine__nvrvec_clone(&dst->u.aggregate.group_by, &src->u.aggregate.group_by);
        jz_vec_clone(&dst->u.aggregate.outputs, &src->u.aggregate.outputs, sizeof(jazz_engine_AggregateExpr), aggregate_clone_v);
        break;
    }
}
void JE_IN(RowSetExpr__drop)(jazz_engine_RowSetExpr *e) {
    switch (e->tag) {
    case JAZZ_ENGINE_EXPR_SOURCE: sid_drop(&e->u.source.source); break;
    case JAZZ_ENGINE_EXPR_VALUE_SOURCE:
        jz_str_drop(&e->u.value_source.shape);
        jz_vec_drop(&e->u.value_source.columns, sizeof(jazz_engine_ValueSourceColumn), vsc_drop_v);
        break;
    case JAZZ_ENGINE_EXPR_FRONTIER_SOURCE:
        jz_str_drop(&e->u.frontier_source.frontier);
        jz_vec_drop(&e->u.frontier_source.columns, sizeof(jazz_engine_ValueSourceColumn), vsc_drop_v);
        break;
    case JAZZ_ENGINE_EXPR_FILTER: jz_str_drop(&e->u.filter.input); pred_drop(&e->u.filter.predicate); break;
    case JAZZ_ENGINE_EXPR_JOIN: jz_str_drop(&e->u.join.left); jz_str_drop(&e->u.join.right); pred_drop(&e->u.join.on); break;
    case JAZZ_ENGINE_EXPR_RECURSIVE_RELATION:
        jz_str_drop(&e->u.recursive.seed);
        jz_str_drop(&e->u.recursive.step);
        jz_str_drop(&e->u.recursive.frontier);
        nvr_drop(&e->u.recursive.frontier_key);
        jazz_engine__nvrvec_drop(&e->u.recursive.dedupe_keys);
        break;
    case JAZZ_ENGINE_EXPR_UNION: jz_vec_drop(&e->u.union_.inputs, sizeof(jazz_engine_UnionInput), union_input_drop_v); break;
    case JAZZ_ENGINE_EXPR_DISTINCT: jz_str_drop(&e->u.distinct.input); jazz_engine__nvrvec_drop(&e->u.distinct.keys); break;
    case JAZZ_ENGINE_EXPR_PROJECT:
        jz_str_drop(&e->u.project.input);
        jz_vec_drop(&e->u.project.columns, sizeof(jazz_engine_RowProjection), projection_drop_v);
        break;
    case JAZZ_ENGINE_EXPR_CORRELATED_PATH_PROJECTION:
        jz_str_drop(&e->u.correlated.input);
        jz_str_drop(&e->u.correlated.child_input);
        JE_IN(ProgramPathId__drop)(&e->u.correlated.path);
        pred_drop(&e->u.correlated.correlation);
        break;
    case JAZZ_ENGINE_EXPR_ORDER_BY:
        jz_str_drop(&e->u.order_by.input);
        jz_vec_drop(&e->u.order_by.keys, sizeof(jazz_engine_OrderKey), order_key_drop_v);
        break;
    case JAZZ_ENGINE_EXPR_SLICE:
        jz_str_drop(&e->u.slice.input);
        jazz_engine__nvrvec_drop(&e->u.slice.partition_by);
        jazz_engine__nvrvec_drop(&e->u.slice.tie_breaker);
        if (e->u.slice.has_rank_output) tof_drop(&e->u.slice.rank_output);
        break;
    case JAZZ_ENGINE_EXPR_AGGREGATE:
        jz_str_drop(&e->u.aggregate.input);
        jazz_engine__nvrvec_drop(&e->u.aggregate.group_by);
        jz_vec_drop(&e->u.aggregate.outputs, sizeof(jazz_engine_AggregateExpr), aggregate_drop_v);
        break;
    }
}
void jazz_engine__expr_drop_v(void *e) { JE_IN(RowSetExpr__drop)((jazz_engine_RowSetExpr *)e); }
static void expr_clone_v(void *d, const void *s) { JE_IN(RowSetExpr__Clone__clone)((jazz_engine_RowSetExpr *)d, (const jazz_engine_RowSetExpr *)s); }

static void node_id_debug(jz_str *out, const jz_str *n) {
    jazz_engine__fmt__tuple_begin(out, "RowSetNodeId");
    jazz_engine__fmt__str(out, n);
    jazz_engine__fmt__tuple_end(out);
}
static void node_id_debug_v(jz_str *out, const void *n) { node_id_debug(out, (const jz_str *)n); }
static void vsc_debug_v(jz_str *out, const void *p) {
    const jazz_engine_ValueSourceColumn *c = p;
    jazz_engine__fmt__struct_begin(out, "ValueSourceColumn");
    jazz_engine__fmt__field(out, "name", true);
    jazz_engine__fmt__str(out, &c->name);
    jazz_engine__fmt__field(out, "value", false);
    nvr_debug(out, &c->value);
    jazz_engine__fmt__field(out, "ty", false);
    groove__records__values__ValueType__Debug__fmt(out, &c->ty);
    jazz_engine__fmt__struct_end(out);
}
static const char *const JOIN_MODE_NAMES[] = {"Inner", "Semi", "NullExtend"};
static const char *const CORRELATION_NAMES[] = {"Optional", "AtLeastOne", "MatchCorrelationCardinality"};
static const char *const AGG_NAMES[] = {"Count", "Sum", "Avg", "Min", "Max"};
static void nvrs_debug(jz_str *out, const jazz_engine_NvrVec *v) {
    jazz_engine__fmt__list(out, v->ptr, v->len, sizeof(jazz_engine_NormalizedValueRef), jazz_engine__nvr_debug_v);
}
static void named_input(jz_str *out, const char *name, const jz_str *input) {
    jazz_engine__fmt__struct_begin(out, name);
    jazz_engine__fmt__field(out, "input", true);
    node_id_debug(out, input);
}
void JE_IN(RowSetExpr__Debug__fmt)(jz_str *out, const jazz_engine_RowSetExpr *e) {
    switch (e->tag) {
    case JAZZ_ENGINE_EXPR_SOURCE:
        jazz_engine__fmt__struct_begin(out, "Source");
        jazz_engine__fmt__field(out, "source", true);
        sid_debug(out, &e->u.source.source);
        jazz_engine__fmt__field(out, "visibility", false);
        jz_str_push_cstr(out, e->u.source.visibility == JAZZ_ENGINE_VIS_VISIBLE ? "Visible" : "IncludeDeleted");
        break;
    case JAZZ_ENGINE_EXPR_VALUE_SOURCE:
        jazz_engine__fmt__struct_begin(out, "ValueSource");
        jazz_engine__fmt__field(out, "shape", true);
        jazz_engine__fmt__str(out, &e->u.value_source.shape);
        jazz_engine__fmt__field(out, "columns", false);
        jazz_engine__fmt__list(out, e->u.value_source.columns.ptr, e->u.value_source.columns.len, sizeof(jazz_engine_ValueSourceColumn), vsc_debug_v);
        jazz_engine__fmt__field(out, "mode", false);
        jz_str_push_cstr(out, e->u.value_source.mode == JAZZ_ENGINE_VSM_BINDING ? "Binding" : "Inline");
        break;
    case JAZZ_ENGINE_EXPR_FRONTIER_SOURCE:
        jazz_engine__fmt__struct_begin(out, "FrontierSource");
        jazz_engine__fmt__field(out, "frontier", true);
        frontier_debug(out, &e->u.frontier_source.frontier);
        jazz_engine__fmt__field(out, "columns", false);
        jazz_engine__fmt__list(out, e->u.frontier_source.columns.ptr, e->u.frontier_source.columns.len, sizeof(jazz_engine_ValueSourceColumn), vsc_debug_v);
        break;
    case JAZZ_ENGINE_EXPR_FILTER:
        named_input(out, "Filter", &e->u.filter.input);
        jazz_engine__fmt__field(out, "predicate", false);
        pred_debug(out, &e->u.filter.predicate);
        break;
    case JAZZ_ENGINE_EXPR_JOIN:
        jazz_engine__fmt__struct_begin(out, "Join");
        jazz_engine__fmt__field(out, "left", true);
        node_id_debug(out, &e->u.join.left);
        jazz_engine__fmt__field(out, "right", false);
        node_id_debug(out, &e->u.join.right);
        jazz_engine__fmt__field(out, "mode", false);
        jz_str_push_cstr(out, JOIN_MODE_NAMES[e->u.join.mode]);
        jazz_engine__fmt__field(out, "on", false);
        pred_debug(out, &e->u.join.on);
        break;
    case JAZZ_ENGINE_EXPR_RECURSIVE_RELATION:
        jazz_engine__fmt__struct_begin(out, "RecursiveRelation");
        jazz_engine__fmt__field(out, "seed", true);
        node_id_debug(out, &e->u.recursive.seed);
        jazz_engine__fmt__field(out, "step", false);
        node_id_debug(out, &e->u.recursive.step);
        jazz_engine__fmt__field(out, "frontier", false);
        frontier_debug(out, &e->u.recursive.frontier);
        jazz_engine__fmt__field(out, "frontier_key", false);
        nvr_debug(out, &e->u.recursive.frontier_key);
        jazz_engine__fmt__field(out, "dedupe_keys", false);
        nvrs_debug(out, &e->u.recursive.dedupe_keys);
        jazz_engine__fmt__field(out, "bound", false);
        if (e->u.recursive.bound.tag == JAZZ_MODEL_RECURSION_FIXPOINT) jz_str_push_cstr(out, "Fixpoint");
        else jz_str_appendf(out, "MaxDepth(%u)", e->u.recursive.bound.max_depth);
        break;
    case JAZZ_ENGINE_EXPR_UNION:
        jazz_engine__fmt__struct_begin(out, "Union");
        jazz_engine__fmt__field(out, "inputs", true);
        jz_str_push_char(out, '[');
        for (u32 i = 0; i < e->u.union_.inputs.len; i++) {
            jazz_engine__fmt__sep(out, i == 0);
            jazz_engine__fmt__struct_begin(out, "UnionInput");
            jazz_engine__fmt__field(out, "node", true);
            node_id_debug(out, &e->u.union_.inputs.ptr[i].node);
            jazz_engine__fmt__field(out, "label", false);
            jazz_engine__fmt__str(out, &e->u.union_.inputs.ptr[i].label);
            jazz_engine__fmt__struct_end(out);
        }
        jz_str_push_char(out, ']');
        break;
    case JAZZ_ENGINE_EXPR_DISTINCT:
        named_input(out, "Distinct", &e->u.distinct.input);
        jazz_engine__fmt__field(out, "keys", false);
        nvrs_debug(out, &e->u.distinct.keys);
        break;
    case JAZZ_ENGINE_EXPR_PROJECT:
        named_input(out, "Project", &e->u.project.input);
        jazz_engine__fmt__field(out, "columns", false);
        jz_str_push_char(out, '[');
        for (u32 i = 0; i < e->u.project.columns.len; i++) {
            jazz_engine__fmt__sep(out, i == 0);
            jazz_engine__fmt__struct_begin(out, "RowProjection");
            jazz_engine__fmt__field(out, "output", true);
            JE_IN(TypedOutputField__Debug__fmt)(out, &e->u.project.columns.ptr[i].output);
            jazz_engine__fmt__field(out, "value", false);
            nvr_debug(out, &e->u.project.columns.ptr[i].value);
            jazz_engine__fmt__struct_end(out);
        }
        jz_str_push_char(out, ']');
        break;
    case JAZZ_ENGINE_EXPR_CORRELATED_PATH_PROJECTION:
        named_input(out, "CorrelatedPathProjection", &e->u.correlated.input);
        jazz_engine__fmt__field(out, "child_input", false);
        node_id_debug(out, &e->u.correlated.child_input);
        jazz_engine__fmt__field(out, "path", false);
        JE_IN(ProgramPathId__Debug__fmt)(out, &e->u.correlated.path);
        jazz_engine__fmt__field(out, "correlation", false);
        pred_debug(out, &e->u.correlated.correlation);
        jazz_engine__fmt__field(out, "requirement", false);
        jz_str_push_cstr(out, CORRELATION_NAMES[e->u.correlated.requirement]);
        break;
    case JAZZ_ENGINE_EXPR_ORDER_BY:
        named_input(out, "OrderBy", &e->u.order_by.input);
        jazz_engine__fmt__field(out, "keys", false);
        jz_str_push_char(out, '[');
        for (u32 i = 0; i < e->u.order_by.keys.len; i++) {
            jazz_engine__fmt__sep(out, i == 0);
            jazz_engine__fmt__struct_begin(out, "OrderKey");
            jazz_engine__fmt__field(out, "value", true);
            nvr_debug(out, &e->u.order_by.keys.ptr[i].value);
            jazz_engine__fmt__field(out, "direction", false);
            jz_str_push_cstr(out, e->u.order_by.keys.ptr[i].direction == JAZZ_ENGINE_SORT_ASC ? "Asc" : "Desc");
            jazz_engine__fmt__struct_end(out);
        }
        jz_str_push_char(out, ']');
        break;
    case JAZZ_ENGINE_EXPR_SLICE:
        named_input(out, "Slice", &e->u.slice.input);
        jazz_engine__fmt__field(out, "partition_by", false);
        nvrs_debug(out, &e->u.slice.partition_by);
        jazz_engine__fmt__field(out, "limit", false);
        if (e->u.slice.has_limit) jz_str_appendf(out, "Some(%u)", e->u.slice.limit);
        else jazz_engine__fmt__option_none(out);
        jazz_engine__fmt__field(out, "offset", false);
        jz_str_appendf(out, "%u", e->u.slice.offset);
        jazz_engine__fmt__field(out, "tie_breaker", false);
        nvrs_debug(out, &e->u.slice.tie_breaker);
        jazz_engine__fmt__field(out, "rank_output", false);
        if (e->u.slice.has_rank_output) {
            jazz_engine__fmt__tuple_begin(out, "Some");
            JE_IN(TypedOutputField__Debug__fmt)(out, &e->u.slice.rank_output);
            jazz_engine__fmt__tuple_end(out);
        } else {
            jazz_engine__fmt__option_none(out);
        }
        break;
    case JAZZ_ENGINE_EXPR_AGGREGATE:
        named_input(out, "Aggregate", &e->u.aggregate.input);
        jazz_engine__fmt__field(out, "group_by", false);
        nvrs_debug(out, &e->u.aggregate.group_by);
        jazz_engine__fmt__field(out, "outputs", false);
        jz_str_push_char(out, '[');
        for (u32 i = 0; i < e->u.aggregate.outputs.len; i++) {
            const jazz_engine_AggregateExpr *a = &e->u.aggregate.outputs.ptr[i];
            jazz_engine__fmt__sep(out, i == 0);
            jazz_engine__fmt__struct_begin(out, "AggregateExpr");
            jazz_engine__fmt__field(out, "output", true);
            JE_IN(TypedOutputField__Debug__fmt)(out, &a->output);
            jazz_engine__fmt__field(out, "function", false);
            jz_str_push_cstr(out, AGG_NAMES[a->function]);
            jazz_engine__fmt__field(out, "input", false);
            if (a->has_input) {
                jazz_engine__fmt__tuple_begin(out, "Some");
                nvr_debug(out, &a->input);
                jazz_engine__fmt__tuple_end(out);
            } else {
                jazz_engine__fmt__option_none(out);
            }
            jazz_engine__fmt__struct_end(out);
        }
        jz_str_push_char(out, ']');
        break;
    }
    jazz_engine__fmt__struct_end(out);
}
static void expr_debug_v(jz_str *out, const void *e) { JE_IN(RowSetExpr__Debug__fmt)(out, (const jazz_engine_RowSetExpr *)e); }

/* ---------------------------------------------------------------- closure / contributions */
void JE_IN(ClosurePathSegment__Clone__clone)(jazz_engine_ClosurePathSegment *dst, const jazz_engine_ClosurePathSegment *src) {
    sid_clone(&dst->parent, &src->parent);
    sid_clone(&dst->target, &src->target);
    dst->source_field = jz_str_clone(&src->source_field);
}
bool JE_IN(ClosurePathSegment__PartialEq__eq)(const jazz_engine_ClosurePathSegment *a, const jazz_engine_ClosurePathSegment *b) {
    return sid_eq(&a->parent, &b->parent) && sid_eq(&a->target, &b->target) && jz_str_eq(&a->source_field, &b->source_field);
}
void JE_IN(ClosurePathSegment__drop)(jazz_engine_ClosurePathSegment *s) { sid_drop(&s->parent); sid_drop(&s->target); jz_str_drop(&s->source_field); }
static void segment_clone_v(void *d, const void *s) { JE_IN(ClosurePathSegment__Clone__clone)(d, s); }
static void segment_drop_v(void *s) { JE_IN(ClosurePathSegment__drop)(s); }
static void closure_path_clone_v(void *d, const void *s) {
    jazz_engine_ClosurePath *dst = d;
    const jazz_engine_ClosurePath *src = s;
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    dst->id = jz_str_clone(&src->id);
    if (src->tag == JAZZ_ENGINE_CLOSURE_IMPLICIT_ROOT_REFERENCE) {
        JE_IN(ClosurePathSegment__Clone__clone)(&dst->segment, &src->segment);
    } else {
        jz_vec_clone(&dst->segments, &src->segments, sizeof(jazz_engine_ClosurePathSegment), segment_clone_v);
        dst->has_root_gate = src->has_root_gate;
        dst->root_gate = src->root_gate;
    }
}
static void closure_path_drop_v(void *p) {
    jazz_engine_ClosurePath *c = p;
    jz_str_drop(&c->id);
    if (c->tag == JAZZ_ENGINE_CLOSURE_IMPLICIT_ROOT_REFERENCE) JE_IN(ClosurePathSegment__drop)(&c->segment);
    else jz_vec_drop(&c->segments, sizeof(jazz_engine_ClosurePathSegment), segment_drop_v);
}
static void join_contribution_clone_v(void *d, const void *s) {
    jazz_engine_JoinContribution *dst = d;
    const jazz_engine_JoinContribution *src = s;
    dst->parent = NULL;
    if (src->parent) {
        dst->parent = JZ_NEW(jazz_engine_SourceId);
        sid_clone(dst->parent, src->parent);
    }
    dst->id = jz_str_clone(&src->id);
    sid_clone(&dst->source, &src->source);
    dst->input = jz_str_clone(&src->input);
    pred_clone(&dst->membership, &src->membership);
}
static void join_contribution_drop_v(void *p) {
    jazz_engine_JoinContribution *c = p;
    if (c->parent) {
        sid_drop(c->parent);
        jz_free(c->parent);
    }
    jz_str_drop(&c->id);
    sid_drop(&c->source);
    jz_str_drop(&c->input);
    pred_drop(&c->membership);
}
static void inherited_clone_v(void *d, const void *s) {
    jazz_engine_InheritedContribution *dst = d;
    const jazz_engine_InheritedContribution *src = s;
    dst->id = jz_str_clone(&src->id);
    sid_clone(&dst->source, &src->source);
    dst->input = jz_str_clone(&src->input);
    pred_clone(&dst->membership, &src->membership);
}
static void inherited_drop_v(void *p) {
    jazz_engine_InheritedContribution *c = p;
    jz_str_drop(&c->id);
    sid_drop(&c->source);
    jz_str_drop(&c->input);
    pred_drop(&c->membership);
}
static void reachable_clone_v(void *d, const void *s) {
    jazz_engine_ReachableContribution *dst = d;
    const jazz_engine_ReachableContribution *src = s;
    dst->id = jz_str_clone(&src->id);
    sid_clone(&dst->access_source, &src->access_source);
    sid_clone(&dst->edge_source, &src->edge_source);
    dst->access_input = jz_str_clone(&src->access_input);
    dst->root_ref_field = jz_str_clone(&src->root_ref_field);
}
static void reachable_drop_v(void *p) {
    jazz_engine_ReachableContribution *c = p;
    jz_str_drop(&c->id);
    sid_drop(&c->access_source);
    sid_drop(&c->edge_source);
    jz_str_drop(&c->access_input);
    jz_str_drop(&c->root_ref_field);
}

static int node_id_cmp_v(const void *a, const void *b, void *ctx) { return jz_str_cmp((const jz_str *)a, (const jz_str *)b); }
const jz_btree_desc jazz_engine__NODE_EXPR_MAP_DESC = {sizeof(jz_str), sizeof(jazz_engine_RowSetExpr), node_id_cmp_v, jazz_engine__str_drop_v, jazz_engine__expr_drop_v};

void JE_IN(NormalizedRowSetShape__Clone__clone)(jazz_engine_NormalizedRowSetShape *dst, const jazz_engine_NormalizedRowSetShape *src) {
    dst->identity.shape_id = src->identity.shape_id;
    dst->identity.canonical = jz_bytes_clone(&src->identity.canonical);
    dst->root = jz_str_clone(&src->root);
    JE_IN(ResultId__Clone__clone)(&dst->result, &src->result);
    jazz_engine__sidset_clone(&dst->auxiliary_sources, &src->auxiliary_sources);
    jz_vec_clone(&dst->closure_paths, &src->closure_paths, sizeof(jazz_engine_ClosurePath), closure_path_clone_v);
    jz_vec_clone(&dst->join_contributions, &src->join_contributions, sizeof(jazz_engine_JoinContribution), join_contribution_clone_v);
    jz_vec_clone(&dst->inherited_contributions, &src->inherited_contributions, sizeof(jazz_engine_InheritedContribution), inherited_clone_v);
    jz_vec_clone(&dst->reachable_contributions, &src->reachable_contributions, sizeof(jazz_engine_ReachableContribution), reachable_clone_v);
    jz_btree_clone(&dst->nodes, &src->nodes, jazz_engine__str_clone_v, expr_clone_v);
}
void JE_IN(NormalizedRowSetShape__drop)(jazz_engine_NormalizedRowSetShape *s) {
    jz_bytes_drop(&s->identity.canonical);
    jz_str_drop(&s->root);
    JE_IN(ResultId__drop)(&s->result);
    jz_btree_drop(&s->auxiliary_sources);
    jz_vec_drop(&s->closure_paths, sizeof(jazz_engine_ClosurePath), closure_path_drop_v);
    jz_vec_drop(&s->join_contributions, sizeof(jazz_engine_JoinContribution), join_contribution_drop_v);
    jz_vec_drop(&s->inherited_contributions, sizeof(jazz_engine_InheritedContribution), inherited_drop_v);
    jz_vec_drop(&s->reachable_contributions, sizeof(jazz_engine_ReachableContribution), reachable_drop_v);
    jz_btree_drop(&s->nodes);
}
static void segment_debug(jz_str *out, const jazz_engine_ClosurePathSegment *s) {
    jazz_engine__fmt__struct_begin(out, "ClosurePathSegment");
    jazz_engine__fmt__field(out, "parent", true);
    sid_debug(out, &s->parent);
    jazz_engine__fmt__field(out, "target", false);
    sid_debug(out, &s->target);
    jazz_engine__fmt__field(out, "source_field", false);
    jazz_engine__fmt__str(out, &s->source_field);
    jazz_engine__fmt__struct_end(out);
}
static void segment_debug_v(jz_str *out, const void *s) { segment_debug(out, s); }
static void closure_path_debug_v(jz_str *out, const void *p) {
    const jazz_engine_ClosurePath *c = p;
    if (c->tag == JAZZ_ENGINE_CLOSURE_IMPLICIT_ROOT_REFERENCE) {
        jazz_engine__fmt__struct_begin(out, "ImplicitRootReference");
        jazz_engine__fmt__field(out, "id", true);
        jazz_engine__fmt__str(out, &c->id);
        jazz_engine__fmt__field(out, "segment", false);
        segment_debug(out, &c->segment);
    } else {
        jazz_engine__fmt__struct_begin(out, "ExplicitInclude");
        jazz_engine__fmt__field(out, "id", true);
        jazz_engine__fmt__str(out, &c->id);
        jazz_engine__fmt__field(out, "segments", false);
        jazz_engine__fmt__list(out, c->segments.ptr, c->segments.len, sizeof(jazz_engine_ClosurePathSegment), segment_debug_v);
        jazz_engine__fmt__field(out, "root_gate", false);
        if (c->has_root_gate) jz_str_push_cstr(out, c->root_gate == JAZZ_ENGINE_GATE_REQUIRED ? "Some(Required)" : "Some(Inner)");
        else jazz_engine__fmt__option_none(out);
    }
    jazz_engine__fmt__struct_end(out);
}
static void join_contribution_debug_v(jz_str *out, const void *p) {
    const jazz_engine_JoinContribution *c = p;
    jazz_engine__fmt__struct_begin(out, "JoinContribution");
    jazz_engine__fmt__field(out, "parent", true);
    if (c->parent) {
        jazz_engine__fmt__tuple_begin(out, "Some");
        sid_debug(out, c->parent);
        jazz_engine__fmt__tuple_end(out);
    } else {
        jazz_engine__fmt__option_none(out);
    }
    jazz_engine__fmt__field(out, "id", false);
    jazz_engine__fmt__str(out, &c->id);
    jazz_engine__fmt__field(out, "source", false);
    sid_debug(out, &c->source);
    jazz_engine__fmt__field(out, "input", false);
    node_id_debug(out, &c->input);
    jazz_engine__fmt__field(out, "membership", false);
    pred_debug(out, &c->membership);
    jazz_engine__fmt__struct_end(out);
}
static void inherited_debug_v(jz_str *out, const void *p) {
    const jazz_engine_InheritedContribution *c = p;
    jazz_engine__fmt__struct_begin(out, "InheritedContribution");
    jazz_engine__fmt__field(out, "id", true);
    jazz_engine__fmt__str(out, &c->id);
    jazz_engine__fmt__field(out, "source", false);
    sid_debug(out, &c->source);
    jazz_engine__fmt__field(out, "input", false);
    node_id_debug(out, &c->input);
    jazz_engine__fmt__field(out, "membership", false);
    pred_debug(out, &c->membership);
    jazz_engine__fmt__struct_end(out);
}
static void reachable_debug_v(jz_str *out, const void *p) {
    const jazz_engine_ReachableContribution *c = p;
    jazz_engine__fmt__struct_begin(out, "ReachableContribution");
    jazz_engine__fmt__field(out, "id", true);
    jazz_engine__fmt__str(out, &c->id);
    jazz_engine__fmt__field(out, "access_source", false);
    sid_debug(out, &c->access_source);
    jazz_engine__fmt__field(out, "edge_source", false);
    sid_debug(out, &c->edge_source);
    jazz_engine__fmt__field(out, "access_input", false);
    node_id_debug(out, &c->access_input);
    jazz_engine__fmt__field(out, "root_ref_field", false);
    jazz_engine__fmt__str(out, &c->root_ref_field);
    jazz_engine__fmt__struct_end(out);
}
void JE_IN(NormalizedRowSetShape__Debug__fmt)(jz_str *out, const jazz_engine_NormalizedRowSetShape *s) {
    jazz_engine__fmt__struct_begin(out, "NormalizedRowSetShape");
    jazz_engine__fmt__field(out, "identity", true);
    jazz_engine__fmt__struct_begin(out, "NormalizedShapeIdentity");
    jazz_engine__fmt__field(out, "shape_id", true);
    jazz_types__ids__uuid_newtype__Debug__fmt(out, "ShapeId", &s->identity.shape_id.u);
    jazz_engine__fmt__field(out, "canonical", false);
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < s->identity.canonical.len; i++) {
        jazz_engine__fmt__sep(out, i == 0);
        jz_str_appendf(out, "%u", s->identity.canonical.ptr[i]);
    }
    jz_str_push_char(out, ']');
    jazz_engine__fmt__struct_end(out);
    jazz_engine__fmt__field(out, "root", false);
    node_id_debug(out, &s->root);
    jazz_engine__fmt__field(out, "result", false);
    JE_IN(ResultId__Debug__fmt)(out, &s->result);
    jazz_engine__fmt__field(out, "auxiliary_sources", false);
    jazz_engine__fmt__set(out, &s->auxiliary_sources, jazz_engine__sid_debug_v);
    jazz_engine__fmt__field(out, "closure_paths", false);
    jazz_engine__fmt__list(out, s->closure_paths.ptr, s->closure_paths.len, sizeof(jazz_engine_ClosurePath), closure_path_debug_v);
    jazz_engine__fmt__field(out, "join_contributions", false);
    jazz_engine__fmt__list(out, s->join_contributions.ptr, s->join_contributions.len, sizeof(jazz_engine_JoinContribution), join_contribution_debug_v);
    jazz_engine__fmt__field(out, "inherited_contributions", false);
    jazz_engine__fmt__list(out, s->inherited_contributions.ptr, s->inherited_contributions.len, sizeof(jazz_engine_InheritedContribution), inherited_debug_v);
    jazz_engine__fmt__field(out, "reachable_contributions", false);
    jazz_engine__fmt__list(out, s->reachable_contributions.ptr, s->reachable_contributions.len, sizeof(jazz_engine_ReachableContribution), reachable_debug_v);
    jazz_engine__fmt__field(out, "nodes", false);
    jazz_engine__fmt__map(out, &s->nodes, node_id_debug_v, expr_debug_v);
    jazz_engine__fmt__struct_end(out);
}

/* ---------------------------------------------------------------- bindings */
void JE_IN(ProgramClaimParam__Clone__clone)(jazz_engine_ProgramClaimParam *dst, const jazz_engine_ProgramClaimParam *src) {
    JE_IN(ClaimPath__Clone__clone)(&dst->path, &src->path);
    groove__records__values__ValueType__Clone__clone(&dst->ty, &src->ty);
}
void JE_IN(ProgramClaimParam__drop)(jazz_engine_ProgramClaimParam *p) { JE_IN(ClaimPath__drop)(&p->path); groove__records__values__ValueType__drop(&p->ty); }
void jazz_engine__claim_param_drop_v(void *p) { JE_IN(ProgramClaimParam__drop)(p); }
void jazz_engine__claim_param_clone_v(void *d, const void *s) { JE_IN(ProgramClaimParam__Clone__clone)(d, s); }
const jz_btree_desc jazz_engine__STR_CLAIM_PARAM_MAP_DESC = {sizeof(jz_str), sizeof(jazz_engine_ProgramClaimParam), jz_cmp_str, jazz_engine__str_drop_v, jazz_engine__claim_param_drop_v};

void JE_IN(ProgramBinding__Clone__clone)(jazz_engine_ProgramBinding *dst, const jazz_engine_ProgramBinding *src) {
    dst->id = src->id;
    dst->has_source_shape = src->has_source_shape;
    dst->source_shape = src->has_source_shape ? jz_str_clone(&src->source_shape) : (jz_str){0};
    jazz_engine__strvtmap_clone(&dst->extra_user_params, &src->extra_user_params);
    jazz_engine__strvtmap_clone(&dst->param_types, &src->param_types);
    jz_btree_clone(&dst->claim_params, &src->claim_params, jazz_engine__str_clone_v, jazz_engine__claim_param_clone_v);
    jz_btree_clone(&dst->values, &src->values, jazz_engine__str_clone_v, groove__records__values__Value__clone_v);
}
void JE_IN(ProgramBinding__drop)(jazz_engine_ProgramBinding *b) {
    jz_str_drop(&b->source_shape);
    jz_btree_drop(&b->extra_user_params);
    jz_btree_drop(&b->param_types);
    jz_btree_drop(&b->claim_params);
    jz_btree_drop(&b->values);
}
void JE_IN(RowSetProgramInput__Clone__clone)(jazz_engine_RowSetProgramInput *dst, const jazz_engine_RowSetProgramInput *src) {
    JE_IN(NormalizedRowSetShape__Clone__clone)(&dst->shape, &src->shape);
    JE_IN(ProgramBinding__Clone__clone)(&dst->binding, &src->binding);
}
void JE_IN(RowSetProgramInput__drop)(jazz_engine_RowSetProgramInput *i) {
    JE_IN(NormalizedRowSetShape__drop)(&i->shape);
    JE_IN(ProgramBinding__drop)(&i->binding);
}
static void claim_param_debug_v(jz_str *out, const void *p) {
    const jazz_engine_ProgramClaimParam *c = p;
    jazz_engine__fmt__struct_begin(out, "ProgramClaimParam");
    jazz_engine__fmt__field(out, "path", true);
    JE_IN(ClaimPath__Debug__fmt)(out, &c->path);
    jazz_engine__fmt__field(out, "ty", false);
    groove__records__values__ValueType__Debug__fmt(out, &c->ty);
    jazz_engine__fmt__struct_end(out);
}
void JE_IN(RowSetProgramInput__Debug__fmt)(jz_str *out, const jazz_engine_RowSetProgramInput *i) {
    const jazz_engine_ProgramBinding *b = &i->binding;
    jazz_engine__fmt__struct_begin(out, "RowSetProgramInput");
    jazz_engine__fmt__field(out, "shape", true);
    JE_IN(NormalizedRowSetShape__Debug__fmt)(out, &i->shape);
    jazz_engine__fmt__field(out, "binding", false);
    jazz_engine__fmt__struct_begin(out, "ProgramBinding");
    jazz_engine__fmt__field(out, "id", true);
    jazz_types__ids__uuid_newtype__Debug__fmt(out, "BindingId", &b->id.u);
    jazz_engine__fmt__field(out, "source_shape", false);
    if (b->has_source_shape) {
        jazz_engine__fmt__tuple_begin(out, "Some");
        jazz_engine__fmt__str(out, &b->source_shape);
        jazz_engine__fmt__tuple_end(out);
    } else {
        jazz_engine__fmt__option_none(out);
    }
    jazz_engine__fmt__field(out, "extra_user_params", false);
    jazz_engine__fmt__map(out, &b->extra_user_params, jazz_engine__fmt__str_v, jazz_engine__fmt__vt_v);
    jazz_engine__fmt__field(out, "param_types", false);
    jazz_engine__fmt__map(out, &b->param_types, jazz_engine__fmt__str_v, jazz_engine__fmt__vt_v);
    jazz_engine__fmt__field(out, "claim_params", false);
    jazz_engine__fmt__map(out, &b->claim_params, jazz_engine__fmt__str_v, claim_param_debug_v);
    jazz_engine__fmt__field(out, "values", false);
    jazz_engine__fmt__map(out, &b->values, jazz_engine__fmt__str_v, jazz_engine__fmt__value_v);
    jazz_engine__fmt__struct_end(out);
    jazz_engine__fmt__struct_end(out);
}
void JE_IN(QueryAuthorizationMode__Debug__fmt)(jz_str *out, u8 m) {
    jz_str_push_cstr(out, m == JAZZ_ENGINE_AUTH_TRUSTED_SERVING ? "TrustedServing" : "ClientLocal");
}

/* ---------------------------------------------------------------- QueryProgramRequest */
void JE_IN(QueryProgramRequest__Clone__clone)(jazz_engine_QueryProgramRequest *dst, const jazz_engine_QueryProgramRequest *src) {
    dst->authorization_mode = src->authorization_mode;
    JE_READ(QueryReadSet__Clone__clone)(&dst->reads, &src->reads);
    JE_POL(PolicyContext__Clone__clone)(&dst->policy, &src->policy);
    JE_IN(RowSetProgramInput__Clone__clone)(&dst->input, &src->input);
    JE_OUT(RowSetOutputRequest__Clone__clone)(&dst->output, &src->output);
}
void JE_IN(QueryProgramRequest__drop)(jazz_engine_QueryProgramRequest *r) {
    JE_READ(QueryReadSet__drop)(&r->reads);
    JE_POL(PolicyContext__drop)(&r->policy);
    JE_IN(RowSetProgramInput__drop)(&r->input);
    JE_OUT(RowSetOutputRequest__drop)(&r->output);
}

/* ---------------------------------------------------------------- element helpers shared with lowering */
void jazz_engine__vsc_clone_v(void *d, const void *s) { vsc_clone_v(d, s); }
void jazz_engine__vsc_drop_v(void *p) { vsc_drop_v(p); }
void jazz_engine__vsc_debug_v(jz_str *out, const void *p) { vsc_debug_v(out, p); }
void jazz_engine__projection_clone_v(void *d, const void *s) { projection_clone_v(d, s); }
void jazz_engine__projection_drop_v(void *p) { projection_drop_v(p); }
void jazz_engine__order_key_clone_v(void *d, const void *s) { order_key_clone_v(d, s); }
void jazz_engine__order_key_drop_v(void *p) { order_key_drop_v(p); }
void jazz_engine__aggregate_clone_v(void *d, const void *s) { aggregate_clone_v(d, s); }
void jazz_engine__aggregate_drop_v(void *p) { aggregate_drop_v(p); }
void jazz_engine__node_id_debug(jz_str *out, const jz_str *n) { node_id_debug(out, n); }
void jazz_engine__frontier_id_debug(jz_str *out, const jz_str *f) { frontier_debug(out, f); }
void jazz_engine__nvrs_debug(jz_str *out, const jazz_engine_NvrVec *v) { nvrs_debug(out, v); }
const char *jazz_engine__join_mode_name(u8 m) { return JOIN_MODE_NAMES[m]; }
const char *jazz_engine__correlation_name(u8 r) { return CORRELATION_NAMES[r]; }
const char *jazz_engine__aggregate_function_name(u8 f) { return AGG_NAMES[f]; }

/* ---------------------------------------------------------------- derived PartialEq for the program request
 * (used by the template cache's family match: `entry.request != template_request`). */
#define VTEQ groove__records__values__ValueType__PartialEq__eq
static bool tof_eq(const jazz_engine_TypedOutputField *a, const jazz_engine_TypedOutputField *b) { return jz_str_eq(&a->name, &b->name) && VTEQ(&a->ty, &b->ty); }
static bool vsc_eq(const jazz_engine_ValueSourceColumn *a, const jazz_engine_ValueSourceColumn *b) {
    return jz_str_eq(&a->name, &b->name) && nvr_eq(&a->value, &b->value) && VTEQ(&a->ty, &b->ty);
}
static bool vscs_eq(const jazz_engine_ValueSourceColumnVec *a, const jazz_engine_ValueSourceColumnVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!vsc_eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
static bool bound_eq(const jazz_model_RecursionBound *a, const jazz_model_RecursionBound *b) {
    return a->tag == b->tag && (a->tag == JAZZ_MODEL_RECURSION_FIXPOINT || a->max_depth == b->max_depth);
}
static bool expr_eq(const jazz_engine_RowSetExpr *a, const jazz_engine_RowSetExpr *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_ENGINE_EXPR_SOURCE: return sid_eq(&a->u.source.source, &b->u.source.source) && a->u.source.visibility == b->u.source.visibility;
    case JAZZ_ENGINE_EXPR_VALUE_SOURCE:
        return jz_str_eq(&a->u.value_source.shape, &b->u.value_source.shape) && vscs_eq(&a->u.value_source.columns, &b->u.value_source.columns) &&
               a->u.value_source.mode == b->u.value_source.mode;
    case JAZZ_ENGINE_EXPR_FRONTIER_SOURCE:
        return jz_str_eq(&a->u.frontier_source.frontier, &b->u.frontier_source.frontier) && vscs_eq(&a->u.frontier_source.columns, &b->u.frontier_source.columns);
    case JAZZ_ENGINE_EXPR_FILTER:
        return jz_str_eq(&a->u.filter.input, &b->u.filter.input) && JE_IN(PredicateExpr__PartialEq__eq)(&a->u.filter.predicate, &b->u.filter.predicate);
    case JAZZ_ENGINE_EXPR_JOIN:
        return jz_str_eq(&a->u.join.left, &b->u.join.left) && jz_str_eq(&a->u.join.right, &b->u.join.right) && a->u.join.mode == b->u.join.mode &&
               JE_IN(PredicateExpr__PartialEq__eq)(&a->u.join.on, &b->u.join.on);
    case JAZZ_ENGINE_EXPR_RECURSIVE_RELATION:
        return jz_str_eq(&a->u.recursive.seed, &b->u.recursive.seed) && jz_str_eq(&a->u.recursive.step, &b->u.recursive.step) &&
               jz_str_eq(&a->u.recursive.frontier, &b->u.recursive.frontier) && nvr_eq(&a->u.recursive.frontier_key, &b->u.recursive.frontier_key) &&
               nvrvec_eq(&a->u.recursive.dedupe_keys, &b->u.recursive.dedupe_keys) && bound_eq(&a->u.recursive.bound, &b->u.recursive.bound);
    case JAZZ_ENGINE_EXPR_UNION:
        if (a->u.union_.inputs.len != b->u.union_.inputs.len) return false;
        for (u32 i = 0; i < a->u.union_.inputs.len; i++)
            if (!jz_str_eq(&a->u.union_.inputs.ptr[i].node, &b->u.union_.inputs.ptr[i].node) ||
                !jz_str_eq(&a->u.union_.inputs.ptr[i].label, &b->u.union_.inputs.ptr[i].label))
                return false;
        return true;
    case JAZZ_ENGINE_EXPR_DISTINCT: return jz_str_eq(&a->u.distinct.input, &b->u.distinct.input) && nvrvec_eq(&a->u.distinct.keys, &b->u.distinct.keys);
    case JAZZ_ENGINE_EXPR_PROJECT:
        if (!jz_str_eq(&a->u.project.input, &b->u.project.input) || a->u.project.columns.len != b->u.project.columns.len) return false;
        for (u32 i = 0; i < a->u.project.columns.len; i++)
            if (!tof_eq(&a->u.project.columns.ptr[i].output, &b->u.project.columns.ptr[i].output) ||
                !nvr_eq(&a->u.project.columns.ptr[i].value, &b->u.project.columns.ptr[i].value))
                return false;
        return true;
    case JAZZ_ENGINE_EXPR_CORRELATED_PATH_PROJECTION:
        return jz_str_eq(&a->u.correlated.input, &b->u.correlated.input) && jz_str_eq(&a->u.correlated.child_input, &b->u.correlated.child_input) &&
               JE_IN(ProgramPathId__PartialEq__eq)(&a->u.correlated.path, &b->u.correlated.path) &&
               JE_IN(PredicateExpr__PartialEq__eq)(&a->u.correlated.correlation, &b->u.correlated.correlation) &&
               a->u.correlated.requirement == b->u.correlated.requirement;
    case JAZZ_ENGINE_EXPR_ORDER_BY:
        if (!jz_str_eq(&a->u.order_by.input, &b->u.order_by.input) || a->u.order_by.keys.len != b->u.order_by.keys.len) return false;
        for (u32 i = 0; i < a->u.order_by.keys.len; i++)
            if (!nvr_eq(&a->u.order_by.keys.ptr[i].value, &b->u.order_by.keys.ptr[i].value) ||
                a->u.order_by.keys.ptr[i].direction != b->u.order_by.keys.ptr[i].direction)
                return false;
        return true;
    case JAZZ_ENGINE_EXPR_SLICE:
        return jz_str_eq(&a->u.slice.input, &b->u.slice.input) && nvrvec_eq(&a->u.slice.partition_by, &b->u.slice.partition_by) &&
               a->u.slice.has_limit == b->u.slice.has_limit && (!a->u.slice.has_limit || a->u.slice.limit == b->u.slice.limit) &&
               a->u.slice.offset == b->u.slice.offset && nvrvec_eq(&a->u.slice.tie_breaker, &b->u.slice.tie_breaker) &&
               a->u.slice.has_rank_output == b->u.slice.has_rank_output &&
               (!a->u.slice.has_rank_output || tof_eq(&a->u.slice.rank_output, &b->u.slice.rank_output));
    case JAZZ_ENGINE_EXPR_AGGREGATE:
        if (!jz_str_eq(&a->u.aggregate.input, &b->u.aggregate.input) || !nvrvec_eq(&a->u.aggregate.group_by, &b->u.aggregate.group_by) ||
            a->u.aggregate.outputs.len != b->u.aggregate.outputs.len)
            return false;
        for (u32 i = 0; i < a->u.aggregate.outputs.len; i++) {
            const jazz_engine_AggregateExpr *x = &a->u.aggregate.outputs.ptr[i], *y = &b->u.aggregate.outputs.ptr[i];
            if (!tof_eq(&x->output, &y->output) || x->function != y->function || x->has_input != y->has_input ||
                (x->has_input && !nvr_eq(&x->input, &y->input)))
                return false;
        }
        return true;
    }
    return false;
}
static bool expr_eq_v(const void *a, const void *b) { return expr_eq(a, b); }
static bool closure_path_eq(const jazz_engine_ClosurePath *a, const jazz_engine_ClosurePath *b) {
    if (a->tag != b->tag || !jz_str_eq(&a->id, &b->id)) return false;
    if (a->tag == JAZZ_ENGINE_CLOSURE_IMPLICIT_ROOT_REFERENCE) return JE_IN(ClosurePathSegment__PartialEq__eq)(&a->segment, &b->segment);
    if (a->segments.len != b->segments.len || a->has_root_gate != b->has_root_gate || (a->has_root_gate && a->root_gate != b->root_gate)) return false;
    for (u32 i = 0; i < a->segments.len; i++)
        if (!JE_IN(ClosurePathSegment__PartialEq__eq)(&a->segments.ptr[i], &b->segments.ptr[i])) return false;
    return true;
}
static bool shape_eq(const jazz_engine_NormalizedRowSetShape *a, const jazz_engine_NormalizedRowSetShape *b) {
    if (!jz_uuid_eq(&a->identity.shape_id.u, &b->identity.shape_id.u) || !jz_bytes_eq(&a->identity.canonical, &b->identity.canonical) ||
        !jz_str_eq(&a->root, &b->root) || !JE_IN(ResultId__PartialEq__eq)(&a->result, &b->result) ||
        !jz_btree_eq(&a->auxiliary_sources, &b->auxiliary_sources, NULL))
        return false;
    if (a->closure_paths.len != b->closure_paths.len || a->join_contributions.len != b->join_contributions.len ||
        a->inherited_contributions.len != b->inherited_contributions.len || a->reachable_contributions.len != b->reachable_contributions.len)
        return false;
    for (u32 i = 0; i < a->closure_paths.len; i++)
        if (!closure_path_eq(&a->closure_paths.ptr[i], &b->closure_paths.ptr[i])) return false;
    for (u32 i = 0; i < a->join_contributions.len; i++) {
        const jazz_engine_JoinContribution *x = &a->join_contributions.ptr[i], *y = &b->join_contributions.ptr[i];
        if ((x->parent == NULL) != (y->parent == NULL) || (x->parent && !sid_eq(x->parent, y->parent)) || !jz_str_eq(&x->id, &y->id) ||
            !sid_eq(&x->source, &y->source) || !jz_str_eq(&x->input, &y->input) || !JE_IN(PredicateExpr__PartialEq__eq)(&x->membership, &y->membership))
            return false;
    }
    for (u32 i = 0; i < a->inherited_contributions.len; i++) {
        const jazz_engine_InheritedContribution *x = &a->inherited_contributions.ptr[i], *y = &b->inherited_contributions.ptr[i];
        if (!jz_str_eq(&x->id, &y->id) || !sid_eq(&x->source, &y->source) || !jz_str_eq(&x->input, &y->input) ||
            !JE_IN(PredicateExpr__PartialEq__eq)(&x->membership, &y->membership))
            return false;
    }
    for (u32 i = 0; i < a->reachable_contributions.len; i++) {
        const jazz_engine_ReachableContribution *x = &a->reachable_contributions.ptr[i], *y = &b->reachable_contributions.ptr[i];
        if (!jz_str_eq(&x->id, &y->id) || !sid_eq(&x->access_source, &y->access_source) || !sid_eq(&x->edge_source, &y->edge_source) ||
            !jz_str_eq(&x->access_input, &y->access_input) || !jz_str_eq(&x->root_ref_field, &y->root_ref_field))
            return false;
    }
    return jz_btree_eq(&a->nodes, &b->nodes, expr_eq_v);
}
static bool claim_param_eq_v(const void *a, const void *b) {
    const jazz_engine_ProgramClaimParam *x = a, *y = b;
    return jazz_engine__strvec_eq(&x->path.segments, &y->path.segments) && VTEQ(&x->ty, &y->ty);
}
static bool binding_eq(const jazz_engine_ProgramBinding *a, const jazz_engine_ProgramBinding *b) {
    return jz_uuid_eq(&a->id.u, &b->id.u) && a->has_source_shape == b->has_source_shape &&
           (!a->has_source_shape || jz_str_eq(&a->source_shape, &b->source_shape)) &&
           jz_btree_eq(&a->extra_user_params, &b->extra_user_params, jazz_engine__vt_eq_v) &&
           jz_btree_eq(&a->param_types, &b->param_types, jazz_engine__vt_eq_v) &&
           jz_btree_eq(&a->claim_params, &b->claim_params, claim_param_eq_v) &&
           jz_btree_eq(&a->values, &b->values, groove__records__values__Value__eq_v);
}
bool JE_IN(QueryProgramRequest__PartialEq__eq)(const jazz_engine_QueryProgramRequest *a, const jazz_engine_QueryProgramRequest *b) {
    return a->authorization_mode == b->authorization_mode && JE_READ(QueryReadSet__PartialEq__eq)(&a->reads, &b->reads) &&
           JE_POL(PolicyContext__PartialEq__eq)(&a->policy, &b->policy) && shape_eq(&a->input.shape, &b->input.shape) &&
           binding_eq(&a->input.binding, &b->input.binding) && JE_OUT(RowSetOutputRequest__PartialEq__eq)(&a->output, &b->output);
}
