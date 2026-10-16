/* output.rs: row-set output request vocabulary and its derived impls. */
#include "mod.h"

/* ---------------------------------------------------------------- FieldProjection / AppPathProjection */
void JE_OUT(FieldProjection__Clone__clone)(jazz_engine_FieldProjection *dst, const jazz_engine_FieldProjection *src) {
    dst->tag = src->tag;
    if (src->tag == JAZZ_ENGINE_FP_FIELDS) jazz_engine__strset_clone(&dst->fields, &src->fields);
    else jz_btree_init(&dst->fields, &jazz_engine__STRSET_DESC);
}
void JE_OUT(FieldProjection__drop)(jazz_engine_FieldProjection *f) { if (f->tag == JAZZ_ENGINE_FP_FIELDS) jz_btree_drop(&f->fields); }
void JE_OUT(FieldProjection__Debug__fmt)(jz_str *out, const jazz_engine_FieldProjection *f) {
    if (f->tag == JAZZ_ENGINE_FP_ALL) {
        jz_str_push_cstr(out, "All");
        return;
    }
    jazz_engine__fmt__tuple_begin(out, "Fields");
    jazz_engine__fmt__strset(out, &f->fields);
    jazz_engine__fmt__tuple_end(out);
}
static void app_path_clone_v(void *d, const void *s) { JE_OUT(AppPathProjection__Clone__clone)(d, s); }
static void app_path_drop_v(void *p) { JE_OUT(AppPathProjection__drop)(p); }
void JE_OUT(AppPathProjection__Clone__clone)(jazz_engine_AppPathProjection *dst, const jazz_engine_AppPathProjection *src) {
    JE_IN(ProgramPathId__Clone__clone)(&dst->path, &src->path);
    dst->field = jz_str_clone(&src->field);
    dst->cardinality = src->cardinality;
    JE_OUT(FieldProjection__Clone__clone)(&dst->fields, &src->fields);
    jz_vec_clone(&dst->children, &src->children, sizeof(jazz_engine_AppPathProjection), app_path_clone_v);
    dst->hole_policy = src->hole_policy;
}
void JE_OUT(AppPathProjection__drop)(jazz_engine_AppPathProjection *p) {
    JE_IN(ProgramPathId__drop)(&p->path);
    jz_str_drop(&p->field);
    JE_OUT(FieldProjection__drop)(&p->fields);
    jz_vec_drop(&p->children, sizeof(jazz_engine_AppPathProjection), app_path_drop_v);
}
static void rpc_clone_v(void *d, const void *s) { jazz_model__query__RelationProjectColumn__Clone__clone(d, s); }
static void rpc_drop_v(void *c) { jazz_model__query__RelationProjectColumn__drop(c); }
void JE_OUT(PayloadProjection__Clone__clone)(jazz_engine_PayloadProjection *dst, const jazz_engine_PayloadProjection *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    switch (src->tag) {
    case JAZZ_ENGINE_PP_SHAPE_DEFAULT: break;
    case JAZZ_ENGINE_PP_RELATION:
        jz_vec_clone(&dst->relation, &src->relation, sizeof(jazz_model_RelationProjectColumn), rpc_clone_v);
        break;
    case JAZZ_ENGINE_PP_TREE:
        JE_OUT(FieldProjection__Clone__clone)(&dst->tree.fields, &src->tree.fields);
        jz_vec_clone(&dst->tree.paths, &src->tree.paths, sizeof(jazz_engine_AppPathProjection), app_path_clone_v);
        break;
    }
}
void JE_OUT(PayloadProjection__drop)(jazz_engine_PayloadProjection *p) {
    switch (p->tag) {
    case JAZZ_ENGINE_PP_RELATION: jz_vec_drop(&p->relation, sizeof(jazz_model_RelationProjectColumn), rpc_drop_v); break;
    case JAZZ_ENGINE_PP_TREE:
        JE_OUT(FieldProjection__drop)(&p->tree.fields);
        jz_vec_drop(&p->tree.paths, sizeof(jazz_engine_AppPathProjection), app_path_drop_v);
        break;
    default: break;
    }
}
static void rpc_debug_v(jz_str *out, const void *c) { jazz_model__query__RelationProjectColumn__Debug__fmt(out, c); }
static const char *const CARD_NAMES[] = {"One", "Many"};
static const char *const HOLE_NAMES[] = {"KeepParentWithHoles", "DropIncompleteParent"};
static void app_path_debug_v(jz_str *out, const void *v) {
    const jazz_engine_AppPathProjection *p = v;
    jazz_engine__fmt__struct_begin(out, "AppPathProjection");
    jazz_engine__fmt__field(out, "path", true);
    JE_IN(ProgramPathId__Debug__fmt)(out, &p->path);
    jazz_engine__fmt__field(out, "field", false);
    jazz_engine__fmt__str(out, &p->field);
    jazz_engine__fmt__field(out, "cardinality", false);
    jz_str_push_cstr(out, CARD_NAMES[p->cardinality]);
    jazz_engine__fmt__field(out, "fields", false);
    JE_OUT(FieldProjection__Debug__fmt)(out, &p->fields);
    jazz_engine__fmt__field(out, "children", false);
    jazz_engine__fmt__list(out, p->children.ptr, p->children.len, sizeof(jazz_engine_AppPathProjection), app_path_debug_v);
    jazz_engine__fmt__field(out, "hole_policy", false);
    jz_str_push_cstr(out, HOLE_NAMES[p->hole_policy]);
    jazz_engine__fmt__struct_end(out);
}
static void payload_projection_debug(jz_str *out, const jazz_engine_PayloadProjection *p) {
    switch (p->tag) {
    case JAZZ_ENGINE_PP_SHAPE_DEFAULT: jz_str_push_cstr(out, "ShapeDefault"); break;
    case JAZZ_ENGINE_PP_RELATION:
        jazz_engine__fmt__tuple_begin(out, "Relation");
        jazz_engine__fmt__list(out, p->relation.ptr, p->relation.len, sizeof(jazz_model_RelationProjectColumn), rpc_debug_v);
        jazz_engine__fmt__tuple_end(out);
        break;
    case JAZZ_ENGINE_PP_TREE:
        jazz_engine__fmt__tuple_begin(out, "Tree");
        jazz_engine__fmt__struct_begin(out, "AppProjectionTree");
        jazz_engine__fmt__field(out, "fields", true);
        JE_OUT(FieldProjection__Debug__fmt)(out, &p->tree.fields);
        jazz_engine__fmt__field(out, "paths", false);
        jazz_engine__fmt__list(out, p->tree.paths.ptr, p->tree.paths.len, sizeof(jazz_engine_AppPathProjection), app_path_debug_v);
        jazz_engine__fmt__struct_end(out);
        jazz_engine__fmt__tuple_end(out);
        break;
    }
}

/* ---------------------------------------------------------------- read.rs coverage identities */
void JE_READ(CoverageScope__Clone__clone)(jazz_engine_CoverageScope *dst, const jazz_engine_CoverageScope *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    if (src->tag == JAZZ_ENGINE_SCOPE_SOURCE) sid_clone(&dst->source, &src->source);
    else if (src->tag == JAZZ_ENGINE_SCOPE_PATH) JE_IN(ProgramPathId__Clone__clone)(&dst->path, &src->path);
}
void JE_READ(CoverageScope__drop)(jazz_engine_CoverageScope *s) {
    if (s->tag == JAZZ_ENGINE_SCOPE_SOURCE) sid_drop(&s->source);
    else if (s->tag == JAZZ_ENGINE_SCOPE_PATH) JE_IN(ProgramPathId__drop)(&s->path);
}
int JE_READ(CoverageScope__Ord__cmp)(const jazz_engine_CoverageScope *a, const jazz_engine_CoverageScope *b) {
    if (a->tag != b->tag) return a->tag < b->tag ? -1 : 1;
    if (a->tag == JAZZ_ENGINE_SCOPE_SOURCE) return sid_cmp(&a->source, &b->source);
    if (a->tag == JAZZ_ENGINE_SCOPE_PATH) return JE_IN(ProgramPathId__Ord__cmp)(&a->path, &b->path);
    return 0;
}
bool JE_READ(CoverageScope__PartialEq__eq)(const jazz_engine_CoverageScope *a, const jazz_engine_CoverageScope *b) {
    if (a->tag != b->tag) return false;
    if (a->tag == JAZZ_ENGINE_SCOPE_SOURCE) return sid_eq(&a->source, &b->source);
    if (a->tag == JAZZ_ENGINE_SCOPE_PATH) return JE_IN(ProgramPathId__PartialEq__eq)(&a->path, &b->path);
    return true;
}
void JE_READ(CoverageScope__Debug__fmt)(jz_str *out, const jazz_engine_CoverageScope *s) {
    switch (s->tag) {
    case JAZZ_ENGINE_SCOPE_PROGRAM: jz_str_push_cstr(out, "Program"); break;
    case JAZZ_ENGINE_SCOPE_SOURCE:
        jazz_engine__fmt__tuple_begin(out, "Source");
        sid_debug(out, &s->source);
        jazz_engine__fmt__tuple_end(out);
        break;
    case JAZZ_ENGINE_SCOPE_PATH:
        jazz_engine__fmt__tuple_begin(out, "Path");
        JE_IN(ProgramPathId__Debug__fmt)(out, &s->path);
        jazz_engine__fmt__tuple_end(out);
        break;
    }
}
static void position_clone(jazz_engine_FrontierPosition *dst, const jazz_engine_FrontierPosition *src) {
    *dst = *src;
    if (src->tag == JAZZ_ENGINE_FPOS_SNAPSHOT || src->tag == JAZZ_ENGINE_FPOS_STREAM_POSITION) dst->bytes = jz_bytes_clone(&src->bytes);
}
static int position_cmp(const jazz_engine_FrontierPosition *a, const jazz_engine_FrontierPosition *b) {
    if (a->tag != b->tag) return a->tag < b->tag ? -1 : 1;
    switch (a->tag) {
    case JAZZ_ENGINE_FPOS_GLOBAL_TIME: return a->time.t < b->time.t ? -1 : a->time.t > b->time.t;
    case JAZZ_ENGINE_FPOS_TRANSACTION:
        if (a->tx.time != b->tx.time) return a->tx.time < b->tx.time ? -1 : 1;
        return jz_uuid_cmp(&a->tx.node, &b->tx.node);
    default: return jz_bytes_cmp(&a->bytes, &b->bytes);
    }
}
void JE_READ(CoverageFrontier__Clone__clone)(jazz_engine_CoverageFrontier *dst, const jazz_engine_CoverageFrontier *src) {
    JE_READ(CoverageScope__Clone__clone)(&dst->scope, &src->scope);
    dst->frontier.tag = src->frontier.tag;
    memset(&dst->frontier.through, 0, sizeof dst->frontier.through);
    if (src->frontier.tag == JAZZ_ENGINE_FREQ_THROUGH) {
        const jazz_engine_ResolvedFrontier *r = &src->frontier.through;
        dst->frontier.through.tier = r->tier;
        dst->frontier.through.has_stream = r->has_stream;
        if (r->has_stream) dst->frontier.through.stream = jz_str_clone(&r->stream);
        position_clone(&dst->frontier.through.through, &r->through);
    }
}
void JE_READ(CoverageFrontier__drop)(jazz_engine_CoverageFrontier *f) {
    JE_READ(CoverageScope__drop)(&f->scope);
    if (f->frontier.tag == JAZZ_ENGINE_FREQ_THROUGH) {
        jz_str_drop(&f->frontier.through.stream);
        jz_bytes_drop(&f->frontier.through.through.bytes);
    }
}
int JE_READ(CoverageFrontier__Ord__cmp)(const jazz_engine_CoverageFrontier *a, const jazz_engine_CoverageFrontier *b) {
    int c = JE_READ(CoverageScope__Ord__cmp)(&a->scope, &b->scope);
    if (c) return c;
    if (a->frontier.tag != b->frontier.tag) return a->frontier.tag < b->frontier.tag ? -1 : 1;
    if (a->frontier.tag == JAZZ_ENGINE_FREQ_NONE) return 0;
    const jazz_engine_ResolvedFrontier *x = &a->frontier.through, *y = &b->frontier.through;
    if (x->tier != y->tier) return x->tier < y->tier ? -1 : 1;
    if (x->has_stream != y->has_stream) return x->has_stream ? 1 : -1;
    if (x->has_stream && (c = jz_str_cmp(&x->stream, &y->stream))) return c;
    return position_cmp(&x->through, &y->through);
}
void jazz_engine__tier_debug(jz_str *out, u8 tier) {
    static const char *const names[] = {"None", "Local", "Global"};
    jz_str_push_cstr(out, names[tier]);
}
static void bytes_debug(jz_str *out, const jz_bytes *b) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < b->len; i++) {
        jazz_engine__fmt__sep(out, i == 0);
        jz_str_appendf(out, "%u", b->ptr[i]);
    }
    jz_str_push_char(out, ']');
}
void JE_READ(CoverageFrontier__Debug__fmt)(jz_str *out, const jazz_engine_CoverageFrontier *f) {
    jazz_engine__fmt__struct_begin(out, "CoverageFrontier");
    jazz_engine__fmt__field(out, "scope", true);
    JE_READ(CoverageScope__Debug__fmt)(out, &f->scope);
    jazz_engine__fmt__field(out, "frontier", false);
    if (f->frontier.tag == JAZZ_ENGINE_FREQ_NONE) {
        jz_str_push_cstr(out, "None");
    } else {
        const jazz_engine_ResolvedFrontier *r = &f->frontier.through;
        jazz_engine__fmt__tuple_begin(out, "Through");
        jazz_engine__fmt__struct_begin(out, "ResolvedFrontier");
        jazz_engine__fmt__field(out, "tier", true);
        jazz_engine__tier_debug(out, r->tier);
        jazz_engine__fmt__field(out, "stream", false);
        if (r->has_stream) {
            jazz_engine__fmt__tuple_begin(out, "Some");
            jazz_engine__fmt__str(out, &r->stream);
            jazz_engine__fmt__tuple_end(out);
        } else {
            jazz_engine__fmt__option_none(out);
        }
        jazz_engine__fmt__field(out, "through", false);
        switch (r->through.tag) {
        case JAZZ_ENGINE_FPOS_GLOBAL_TIME: jz_str_appendf(out, "GlobalTime(GlobalTime(%llu))", (unsigned long long)r->through.time.t); break;
        case JAZZ_ENGINE_FPOS_SNAPSHOT: jazz_engine__fmt__tuple_begin(out, "Snapshot"); bytes_debug(out, &r->through.bytes); jazz_engine__fmt__tuple_end(out); break;
        case JAZZ_ENGINE_FPOS_TRANSACTION: jazz_engine__fmt__tuple_begin(out, "Transaction"); jazz_model__tx__TxId__Debug__fmt(out, &r->through.tx); jazz_engine__fmt__tuple_end(out); break;
        case JAZZ_ENGINE_FPOS_STREAM_POSITION: jazz_engine__fmt__tuple_begin(out, "StreamPosition"); bytes_debug(out, &r->through.bytes); jazz_engine__fmt__tuple_end(out); break;
        }
        jazz_engine__fmt__struct_end(out);
        jazz_engine__fmt__tuple_end(out);
    }
    jazz_engine__fmt__struct_end(out);
}
void JE_READ(BatchId__Debug__fmt)(jz_str *out, const jazz_engine_BatchId *b) {
    u8 hash[32];
    jz_blake3(b->ptr, b->len, hash);
    jz_str hex = {0};
    jz_hex_encode(hash, 32, &hex);
    jz_str_appendf(out, "BatchId { len: %u, fingerprint: %.*s }", b->len, 12, hex.ptr);
    jz_str_drop(&hex);
}

/* ---------------------------------------------------------------- ProgramFactKey */
jazz_engine_ProgramFactKey jazz_engine__fact_key_simple(u8 tag) {
    jazz_engine_ProgramFactKey k;
    memset(&k, 0, sizeof k);
    k.tag = tag;
    return k;
}
void JE_OUT(ProgramFactKey__Clone__clone)(jazz_engine_ProgramFactKey *dst, const jazz_engine_ProgramFactKey *src) {
    *dst = jazz_engine__fact_key_simple(src->tag);
    switch (src->tag) {
    case JAZZ_ENGINE_FACT_PROGRAM_SOURCE_COVERAGE: JE_READ(CoverageScope__Clone__clone)(&dst->scope, &src->scope); break;
    case JAZZ_ENGINE_FACT_READ_FRONTIER_SETTLED: JE_READ(CoverageFrontier__Clone__clone)(&dst->frontier, &src->frontier); break;
    case JAZZ_ENGINE_FACT_COMPLETE_TX_PAYLOAD_COVERAGE:
        dst->batch = jz_bytes_clone(&src->batch);
        dst->tier = src->tier;
        break;
    case JAZZ_ENGINE_FACT_VIEW_COMPLETE_EXCLUSIVE_COVERAGE:
        JE_READ(CoverageScope__Clone__clone)(&dst->scope, &src->scope);
        if (src->result) {
            dst->result = JZ_NEW(jazz_engine_ResultId);
            JE_IN(ResultId__Clone__clone)(dst->result, src->result);
        }
        dst->tier = src->tier;
        break;
    case JAZZ_ENGINE_FACT_POLICY_DECISION:
        dst->decision.role = src->decision.role;
        dst->decision.fingerprint = jz_bytes_clone(&src->decision.fingerprint);
        break;
    case JAZZ_ENGINE_FACT_PREDICATE_OUTPUT_SET: dst->role = src->role; break;
    case JAZZ_ENGINE_FACT_POINT_READS: dst->present = src->present; break;
    default: break;
    }
}
void JE_OUT(ProgramFactKey__drop)(jazz_engine_ProgramFactKey *k) {
    switch (k->tag) {
    case JAZZ_ENGINE_FACT_PROGRAM_SOURCE_COVERAGE: JE_READ(CoverageScope__drop)(&k->scope); break;
    case JAZZ_ENGINE_FACT_READ_FRONTIER_SETTLED: JE_READ(CoverageFrontier__drop)(&k->frontier); break;
    case JAZZ_ENGINE_FACT_COMPLETE_TX_PAYLOAD_COVERAGE: jz_bytes_drop(&k->batch); break;
    case JAZZ_ENGINE_FACT_VIEW_COMPLETE_EXCLUSIVE_COVERAGE:
        JE_READ(CoverageScope__drop)(&k->scope);
        if (k->result) {
            JE_IN(ResultId__drop)(k->result);
            jz_free(k->result);
        }
        break;
    case JAZZ_ENGINE_FACT_POLICY_DECISION: jz_bytes_drop(&k->decision.fingerprint); break;
    default: break;
    }
}
int JE_OUT(ProgramFactKey__Ord__cmp)(const jazz_engine_ProgramFactKey *a, const jazz_engine_ProgramFactKey *b) {
    if (a->tag != b->tag) return a->tag < b->tag ? -1 : 1;
    int c;
    switch (a->tag) {
    case JAZZ_ENGINE_FACT_PROGRAM_SOURCE_COVERAGE: return JE_READ(CoverageScope__Ord__cmp)(&a->scope, &b->scope);
    case JAZZ_ENGINE_FACT_READ_FRONTIER_SETTLED: return JE_READ(CoverageFrontier__Ord__cmp)(&a->frontier, &b->frontier);
    case JAZZ_ENGINE_FACT_COMPLETE_TX_PAYLOAD_COVERAGE:
        c = jz_bytes_cmp(&a->batch, &b->batch);
        return c ? c : (a->tier < b->tier ? -1 : a->tier > b->tier);
    case JAZZ_ENGINE_FACT_VIEW_COMPLETE_EXCLUSIVE_COVERAGE:
        c = JE_READ(CoverageScope__Ord__cmp)(&a->scope, &b->scope);
        if (c) return c;
        if (!a->result || !b->result) {
            c = (a->result != NULL) - (b->result != NULL);
        } else {
            c = JE_IN(ResultId__Ord__cmp)(a->result, b->result);
        }
        return c ? c : (a->tier < b->tier ? -1 : a->tier > b->tier);
    case JAZZ_ENGINE_FACT_POLICY_DECISION:
        if (a->decision.role != b->decision.role) return a->decision.role < b->decision.role ? -1 : 1;
        return jz_bytes_cmp(&a->decision.fingerprint, &b->decision.fingerprint);
    case JAZZ_ENGINE_FACT_PREDICATE_OUTPUT_SET: return a->role < b->role ? -1 : a->role > b->role;
    case JAZZ_ENGINE_FACT_POINT_READS: return (int)a->present - (int)b->present;
    default: return 0;
    }
}
void JE_OUT(PolicyDecisionRole__Debug__fmt)(jz_str *out, u8 role) { jz_str_push_cstr(out, role == JAZZ_ENGINE_PDR_READ ? "Read" : "Write"); }
void JE_OUT(ProgramFactKey__Debug__fmt)(jz_str *out, const jazz_engine_ProgramFactKey *k) {
    static const char *const unit_names[] = {
        "AuthorizedRows", "ResultMembership", "RelationEdges", "PathCorrelationCoverage", NULL, NULL, NULL, NULL,
        "VersionWitnesses", "ReplacementWitnesses", NULL, "PolicyWitnesses", "ContributingMembers", "PredicateReads", NULL, NULL,
    };
    switch (k->tag) {
    case JAZZ_ENGINE_FACT_PROGRAM_SOURCE_COVERAGE:
        jazz_engine__fmt__tuple_begin(out, "ProgramSourceCoverage");
        JE_READ(CoverageScope__Debug__fmt)(out, &k->scope);
        jazz_engine__fmt__tuple_end(out);
        break;
    case JAZZ_ENGINE_FACT_READ_FRONTIER_SETTLED:
        jazz_engine__fmt__tuple_begin(out, "ReadFrontierSettled");
        JE_READ(CoverageFrontier__Debug__fmt)(out, &k->frontier);
        jazz_engine__fmt__tuple_end(out);
        break;
    case JAZZ_ENGINE_FACT_COMPLETE_TX_PAYLOAD_COVERAGE:
        jazz_engine__fmt__struct_begin(out, "CompleteTxPayloadCoverage");
        jazz_engine__fmt__field(out, "batch", true);
        JE_READ(BatchId__Debug__fmt)(out, &k->batch);
        jazz_engine__fmt__field(out, "tier", false);
        jazz_engine__tier_debug(out, k->tier);
        jazz_engine__fmt__struct_end(out);
        break;
    case JAZZ_ENGINE_FACT_VIEW_COMPLETE_EXCLUSIVE_COVERAGE:
        jazz_engine__fmt__struct_begin(out, "ViewCompleteExclusiveCoverage");
        jazz_engine__fmt__field(out, "view", true);
        JE_READ(CoverageScope__Debug__fmt)(out, &k->scope);
        jazz_engine__fmt__field(out, "result", false);
        if (k->result) {
            jazz_engine__fmt__tuple_begin(out, "Some");
            JE_IN(ResultId__Debug__fmt)(out, k->result);
            jazz_engine__fmt__tuple_end(out);
        } else {
            jazz_engine__fmt__option_none(out);
        }
        jazz_engine__fmt__field(out, "tier", false);
        jazz_engine__tier_debug(out, k->tier);
        jazz_engine__fmt__struct_end(out);
        break;
    case JAZZ_ENGINE_FACT_POLICY_DECISION:
        jazz_engine__fmt__struct_begin(out, "PolicyDecision");
        jazz_engine__fmt__field(out, "decision", true);
        jazz_engine__fmt__struct_begin(out, "PolicyDecisionFactKey");
        jazz_engine__fmt__field(out, "role", true);
        JE_OUT(PolicyDecisionRole__Debug__fmt)(out, k->decision.role);
        jazz_engine__fmt__field(out, "fingerprint", false);
        bytes_debug(out, &k->decision.fingerprint);
        jazz_engine__fmt__struct_end(out);
        jazz_engine__fmt__struct_end(out);
        break;
    case JAZZ_ENGINE_FACT_PREDICATE_OUTPUT_SET:
        jazz_engine__fmt__struct_begin(out, "PredicateOutputSet");
        jazz_engine__fmt__field(out, "role", true);
        jz_str_push_cstr(out, k->role == JAZZ_ENGINE_POSR_BASE ? "Base" : "Now");
        jazz_engine__fmt__struct_end(out);
        break;
    case JAZZ_ENGINE_FACT_POINT_READS:
        jazz_engine__fmt__struct_begin(out, "PointReads");
        jazz_engine__fmt__field(out, "present", true);
        jazz_engine__fmt__bool(out, k->present);
        jazz_engine__fmt__struct_end(out);
        break;
    default: jz_str_push_cstr(out, unit_names[k->tag]); break;
    }
}
void jazz_engine__fact_key_drop_v(void *k) { JE_OUT(ProgramFactKey__drop)(k); }
void jazz_engine__fact_key_clone_v(void *d, const void *s) { JE_OUT(ProgramFactKey__Clone__clone)(d, s); }
void jazz_engine__fact_key_debug_v(jz_str *out, const void *k) { JE_OUT(ProgramFactKey__Debug__fmt)(out, k); }
static int fact_key_cmp_v(const void *a, const void *b, void *ctx) { return JE_OUT(ProgramFactKey__Ord__cmp)(a, b); }
const jz_btree_desc jazz_engine__FACT_KEY_SET_DESC = {sizeof(jazz_engine_ProgramFactKey), 0, fact_key_cmp_v, jazz_engine__fact_key_drop_v, NULL};

/* ---------------------------------------------------------------- RowSetOutputRequest */
void JE_OUT(RowSetOutputRequest__Clone__clone)(jazz_engine_RowSetOutputRequest *dst, const jazz_engine_RowSetOutputRequest *src) {
    dst->has_app_rows = src->has_app_rows;
    memset(&dst->app_rows, 0, sizeof dst->app_rows);
    if (src->has_app_rows) {
        dst->app_rows.public_terminal = src->app_rows.public_terminal;
        JE_OUT(PayloadProjection__Clone__clone)(&dst->app_rows.projection, &src->app_rows.projection);
    }
    jz_btree_clone(&dst->facts, &src->facts, jazz_engine__fact_key_clone_v, NULL);
}
void JE_OUT(RowSetOutputRequest__drop)(jazz_engine_RowSetOutputRequest *r) {
    if (r->has_app_rows) JE_OUT(PayloadProjection__drop)(&r->app_rows.projection);
    jz_btree_drop(&r->facts);
}
void JE_OUT(RowSetOutputRequest__Debug__fmt)(jz_str *out, const jazz_engine_RowSetOutputRequest *r) {
    jazz_engine__fmt__struct_begin(out, "RowSetOutputRequest");
    jazz_engine__fmt__field(out, "app_rows", true);
    if (r->has_app_rows) {
        jazz_engine__fmt__tuple_begin(out, "Some");
        jazz_engine__fmt__struct_begin(out, "AppRowOutputRequest");
        jazz_engine__fmt__field(out, "public_terminal", true);
        jazz_engine__fmt__bool(out, r->app_rows.public_terminal);
        jazz_engine__fmt__field(out, "projection", false);
        payload_projection_debug(out, &r->app_rows.projection);
        jazz_engine__fmt__struct_end(out);
        jazz_engine__fmt__tuple_end(out);
    } else {
        jazz_engine__fmt__option_none(out);
    }
    jazz_engine__fmt__field(out, "facts", false);
    jazz_engine__fmt__set(out, &r->facts, jazz_engine__fact_key_debug_v);
    jazz_engine__fmt__struct_end(out);
}

/* derived PartialEq for the output request (used by the template cache's request equality) */
static bool field_projection_eq(const jazz_engine_FieldProjection *a, const jazz_engine_FieldProjection *b) {
    return a->tag == b->tag && (a->tag == JAZZ_ENGINE_FP_ALL || jz_btree_eq(&a->fields, &b->fields, NULL));
}
static bool path_projections_eq(const jazz_engine_AppPathProjectionVec *a, const jazz_engine_AppPathProjectionVec *b);
static bool path_projection_eq(const jazz_engine_AppPathProjection *a, const jazz_engine_AppPathProjection *b) {
    return JE_IN(ProgramPathId__PartialEq__eq)(&a->path, &b->path) && jz_str_eq(&a->field, &b->field) && a->cardinality == b->cardinality &&
           field_projection_eq(&a->fields, &b->fields) && path_projections_eq(&a->children, &b->children) && a->hole_policy == b->hole_policy;
}
static bool path_projections_eq(const jazz_engine_AppPathProjectionVec *a, const jazz_engine_AppPathProjectionVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!path_projection_eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
static bool payload_projection_eq(const jazz_engine_PayloadProjection *a, const jazz_engine_PayloadProjection *b) {
    if (a->tag != b->tag) return false;
    if (a->tag == JAZZ_ENGINE_PP_RELATION) {
        if (a->relation.len != b->relation.len) return false;
        for (u32 i = 0; i < a->relation.len; i++)
            if (!jazz_model__query__RelationProjectColumn__PartialEq__eq(&a->relation.ptr[i], &b->relation.ptr[i])) return false;
    }
    if (a->tag == JAZZ_ENGINE_PP_TREE)
        return field_projection_eq(&a->tree.fields, &b->tree.fields) && path_projections_eq(&a->tree.paths, &b->tree.paths);
    return true;
}
bool JE_OUT(RowSetOutputRequest__PartialEq__eq)(const jazz_engine_RowSetOutputRequest *a, const jazz_engine_RowSetOutputRequest *b) {
    if (a->has_app_rows != b->has_app_rows) return false;
    if (a->has_app_rows && (a->app_rows.public_terminal != b->app_rows.public_terminal ||
                            !payload_projection_eq(&a->app_rows.projection, &b->app_rows.projection)))
        return false;
    return jz_btree_eq(&a->facts, &b->facts, NULL);
}
