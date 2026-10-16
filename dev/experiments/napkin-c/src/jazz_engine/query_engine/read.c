/* read.rs: requested read views. The Rust SourceResolution generic is only ever
 * instantiated at RequestedSourceStage in the shipped code; C has that one copy. */
#include "mod.h"

static void svid_clone_v(void *d, const void *s) { *(jazz_types_SchemaVersionId *)d = *(const jazz_types_SchemaVersionId *)s; }
static int svid_cmp_v(const void *a, const void *b, void *ctx) { return jz_uuid_cmp(&((const jazz_types_SchemaVersionId *)a)->u, &((const jazz_types_SchemaVersionId *)b)->u); }
static const jz_btree_desc SVID_SET_DESC = {sizeof(jazz_types_SchemaVersionId), 0, svid_cmp_v, NULL, NULL};

static void projection_clone(jazz_engine_SchemaProjection *dst, const jazz_engine_SchemaProjection *src) {
    *dst = *src;
    if (src->storage_tag == JAZZ_ENGINE_SSS_EXPLICIT) jz_btree_clone(&dst->storage_explicit, &src->storage_explicit, svid_clone_v, NULL);
    else jz_btree_init(&dst->storage_explicit, &SVID_SET_DESC);
    dst->lens_fingerprint = src->lens_tag == JAZZ_ENGINE_LENS_FINGERPRINT ? jz_bytes_clone(&src->lens_fingerprint) : (jz_bytes){0};
}
static void projection_drop(jazz_engine_SchemaProjection *p) {
    if (p->storage_tag == JAZZ_ENGINE_SSS_EXPLICIT) jz_btree_drop(&p->storage_explicit);
    jz_bytes_drop(&p->lens_fingerprint);
}
static bool svid_set_eq(const jz_btree *a, const jz_btree *b) { return jz_btree_eq(a, b, NULL); }
static bool projection_eq(const jazz_engine_SchemaProjection *a, const jazz_engine_SchemaProjection *b) {
    if (a->schema_family_tag != b->schema_family_tag) return false;
    if (a->schema_family_tag == JAZZ_ENGINE_SFS_EXPLICIT && !jz_uuid_eq(&a->schema_family.u, &b->schema_family.u)) return false;
    if (a->storage_tag != b->storage_tag) return false;
    if (a->storage_tag == JAZZ_ENGINE_SSS_SINGLE && !jz_uuid_eq(&a->storage_single.u, &b->storage_single.u)) return false;
    if (a->storage_tag == JAZZ_ENGINE_SSS_EXPLICIT && !svid_set_eq(&a->storage_explicit, &b->storage_explicit)) return false;
    if (a->lens_tag != b->lens_tag) return false;
    return a->lens_tag == JAZZ_ENGINE_LENS_CANONICAL || jz_bytes_eq(&a->lens_fingerprint, &b->lens_fingerprint);
}
static void svid_debug_v(jz_str *out, const void *v) { jazz_types__ids__uuid_newtype__Debug__fmt(out, "SchemaVersionId", &((const jazz_types_SchemaVersionId *)v)->u); }
static void projection_debug(jz_str *out, const jazz_engine_SchemaProjection *p) {
    jazz_engine__fmt__struct_begin(out, "SchemaProjection");
    jazz_engine__fmt__field(out, "schema_family", true);
    if (p->schema_family_tag == JAZZ_ENGINE_SFS_CURRENT) {
        jz_str_push_cstr(out, "Current");
    } else {
        jazz_engine__fmt__tuple_begin(out, "ExplicitSchemaFamily");
        jazz_types__ids__uuid_newtype__Debug__fmt(out, "SchemaFamilyId", &p->schema_family.u);
        jazz_engine__fmt__tuple_end(out);
    }
    jazz_engine__fmt__field(out, "storage", false);
    switch (p->storage_tag) {
    case JAZZ_ENGINE_SSS_COMPATIBLE_PARTITIONS: jz_str_push_cstr(out, "CompatiblePartitions"); break;
    case JAZZ_ENGINE_SSS_SINGLE:
        jazz_engine__fmt__tuple_begin(out, "Single");
        svid_debug_v(out, &p->storage_single);
        jazz_engine__fmt__tuple_end(out);
        break;
    case JAZZ_ENGINE_SSS_EXPLICIT:
        jazz_engine__fmt__tuple_begin(out, "Explicit");
        jazz_engine__fmt__set(out, &p->storage_explicit, svid_debug_v);
        jazz_engine__fmt__tuple_end(out);
        break;
    }
    jazz_engine__fmt__field(out, "lens", false);
    if (p->lens_tag == JAZZ_ENGINE_LENS_CANONICAL) {
        jz_str_push_cstr(out, "Canonical");
    } else {
        jazz_engine__fmt__tuple_begin(out, "Fingerprint");
        jz_str_push_char(out, '[');
        for (u32 i = 0; i < p->lens_fingerprint.len; i++) {
            jazz_engine__fmt__sep(out, i == 0);
            jz_str_appendf(out, "%u", p->lens_fingerprint.ptr[i]);
        }
        jz_str_push_char(out, ']');
        jazz_engine__fmt__tuple_end(out);
    }
    jazz_engine__fmt__struct_end(out);
}

static void data_clone(jazz_engine_DataSource *dst, const jazz_engine_DataSource *src) {
    dst->tag = src->tag;
    dst->branch = NULL;
    if (src->tag == JAZZ_ENGINE_DATA_BRANCH) jazz_model__model__branch__BranchKey__Clone__clone(&dst->branch, src->branch);
}
static void data_drop(jazz_engine_DataSource *d) { if (d->tag == JAZZ_ENGINE_DATA_BRANCH) jazz_model__model__branch__BranchKey__drop(d->branch); }
static bool data_eq(const jazz_engine_DataSource *a, const jazz_engine_DataSource *b) {
    return a->tag == b->tag && (a->tag == JAZZ_ENGINE_DATA_CURRENT || jazz_model__model__branch__BranchKey__PartialEq__eq(a->branch, b->branch));
}
static void data_debug(jz_str *out, const jazz_engine_DataSource *d) {
    if (d->tag == JAZZ_ENGINE_DATA_CURRENT) {
        jz_str_push_cstr(out, "Current");
        return;
    }
    jazz_engine__fmt__tuple_begin(out, "Branch");
    jazz_model__model__branch__BranchKey__Debug__fmt(out, d->branch);
    jazz_engine__fmt__tuple_end(out);
}

static void overlay_clone_v(void *d, const void *s) {
    jazz_engine_OverlayRef *dst = d;
    const jazz_engine_OverlayRef *src = s;
    *dst = *src;
    dst->batch = src->tag == JAZZ_ENGINE_OVERLAY_DIRECT_BATCH ? jz_bytes_clone(&src->batch) : (jz_bytes){0};
}
static void overlay_drop_v(void *p) { jz_bytes_drop(&((jazz_engine_OverlayRef *)p)->batch); }
static bool overlay_eq(const jazz_engine_OverlayRef *a, const jazz_engine_OverlayRef *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_ENGINE_OVERLAY_DIRECT_BATCH: return jz_bytes_eq(&a->batch, &b->batch);
    case JAZZ_ENGINE_OVERLAY_ACCEPTED_TRANSACTION: return a->tx.time == b->tx.time && jz_uuid_eq(&a->tx.node, &b->tx.node);
    case JAZZ_ENGINE_OVERLAY_OPEN_TRANSACTION: return jz_uuid_eq(&a->open.u, &b->open.u);
    default: return true;
    }
}
static void overlay_debug_v(jz_str *out, const void *p) {
    const jazz_engine_OverlayRef *o = p;
    switch (o->tag) {
    case JAZZ_ENGINE_OVERLAY_PENDING_LOCAL: jz_str_push_cstr(out, "PendingLocal"); break;
    case JAZZ_ENGINE_OVERLAY_DIRECT_BATCH:
        jazz_engine__fmt__tuple_begin(out, "DirectBatch");
        JE_READ(BatchId__Debug__fmt)(out, &o->batch);
        jazz_engine__fmt__tuple_end(out);
        break;
    case JAZZ_ENGINE_OVERLAY_ACCEPTED_TRANSACTION:
        jazz_engine__fmt__tuple_begin(out, "AcceptedTransaction");
        jazz_model__tx__TxId__Debug__fmt(out, &o->tx);
        jazz_engine__fmt__tuple_end(out);
        break;
    case JAZZ_ENGINE_OVERLAY_OPEN_TRANSACTION:
        jazz_engine__fmt__tuple_begin(out, "OpenTransaction");
        jazz_model__model__transaction__OpenTransactionId__Debug__fmt(out, &o->open);
        jazz_engine__fmt__tuple_end(out);
        break;
    }
}

static bool has_projection(u8 tag) { return tag != JAZZ_ENGINE_SRC_WITH_OVERLAYS && tag != JAZZ_ENGINE_SRC_MERGE; }
static bool has_data(u8 tag) { return tag == JAZZ_ENGINE_SRC_VISIBLE_CURRENT || tag == JAZZ_ENGINE_SRC_HISTORY_CUT || tag == JAZZ_ENGINE_SRC_SNAPSHOT_REF; }
static void source_expr_clone_v(void *d, const void *s) { JE_READ(SourceExpr__Clone__clone)(d, s); }
static void source_expr_drop_v(void *e) { JE_READ(SourceExpr__drop)(e); }

void JE_READ(SourceExpr__Clone__clone)(jazz_engine_SourceExpr *dst, const jazz_engine_SourceExpr *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    if (has_projection(src->tag)) projection_clone(&dst->projection, &src->projection);
    if (has_data(src->tag)) data_clone(&dst->data, &src->data);
    switch (src->tag) {
    case JAZZ_ENGINE_SRC_VISIBLE_CURRENT: dst->tier = src->tier; break;
    case JAZZ_ENGINE_SRC_BRANCH_VIEW:
        jazz_model__model__branch__BranchKey__Clone__clone(&dst->head, src->head);
        dst->has_base = src->has_base;
        if (src->has_base) {
            dst->base.tag = src->base.tag;
            jazz_model__model__branch__BranchKey__Clone__clone(&dst->base.key, src->base.key);
            if (src->base.tag == JAZZ_ENGINE_BVSB_SNAPSHOT) jazz_model__model__branch__SnapshotRef__Clone__clone(&dst->base.snapshot, src->base.snapshot);
        }
        dst->tier = src->tier;
        break;
    case JAZZ_ENGINE_SRC_HISTORY_CUT: dst->position = src->position; break;
    case JAZZ_ENGINE_SRC_SNAPSHOT_REF: jazz_model__tx__Snapshot__Clone__clone(&dst->snapshot, src->snapshot); break;
    case JAZZ_ENGINE_SRC_SETTLED_BINDING_VIEW:
        dst->current_default = src->current_default;
        dst->binding_view = src->binding_view;
        dst->has_authority_result_key = src->has_authority_result_key;
        dst->authority_result_key = src->authority_result_key;
        break;
    case JAZZ_ENGINE_SRC_WITH_OVERLAYS:
    case JAZZ_ENGINE_SRC_LENS_PROJECT:
        dst->input = JZ_NEW(jazz_engine_SourceExpr);
        JE_READ(SourceExpr__Clone__clone)(dst->input, src->input);
        if (src->tag == JAZZ_ENGINE_SRC_WITH_OVERLAYS)
            jz_vec_clone(&dst->overlays, &src->overlays, sizeof(jazz_engine_OverlayRef), overlay_clone_v);
        break;
    case JAZZ_ENGINE_SRC_MERGE:
        jz_vec_clone(&dst->inputs, &src->inputs, sizeof(jazz_engine_SourceExpr), source_expr_clone_v);
        dst->mode = src->mode;
        break;
    }
}
void JE_READ(SourceExpr__drop)(jazz_engine_SourceExpr *e) {
    if (has_projection(e->tag)) projection_drop(&e->projection);
    if (has_data(e->tag)) data_drop(&e->data);
    switch (e->tag) {
    case JAZZ_ENGINE_SRC_BRANCH_VIEW:
        jazz_model__model__branch__BranchKey__drop(e->head);
        if (e->has_base) {
            jazz_model__model__branch__BranchKey__drop(e->base.key);
            if (e->base.tag == JAZZ_ENGINE_BVSB_SNAPSHOT) jazz_model__model__branch__SnapshotRef__drop(e->base.snapshot);
        }
        break;
    case JAZZ_ENGINE_SRC_SNAPSHOT_REF: jazz_model__tx__Snapshot__drop(e->snapshot); break;
    case JAZZ_ENGINE_SRC_WITH_OVERLAYS:
    case JAZZ_ENGINE_SRC_LENS_PROJECT:
        JE_READ(SourceExpr__drop)(e->input);
        jz_free(e->input);
        jz_vec_drop(&e->overlays, sizeof(jazz_engine_OverlayRef), overlay_drop_v);
        break;
    case JAZZ_ENGINE_SRC_MERGE: jz_vec_drop(&e->inputs, sizeof(jazz_engine_SourceExpr), source_expr_drop_v); break;
    default: break;
    }
}
bool JE_READ(SourceExpr__PartialEq__eq)(const jazz_engine_SourceExpr *a, const jazz_engine_SourceExpr *b) {
    if (a->tag != b->tag) return false;
    if (has_projection(a->tag) && !projection_eq(&a->projection, &b->projection)) return false;
    if (has_data(a->tag) && !data_eq(&a->data, &b->data)) return false;
    switch (a->tag) {
    case JAZZ_ENGINE_SRC_VISIBLE_CURRENT: return a->tier == b->tier;
    case JAZZ_ENGINE_SRC_BRANCH_VIEW:
        if (!jazz_model__model__branch__BranchKey__PartialEq__eq(a->head, b->head) || a->has_base != b->has_base || a->tier != b->tier) return false;
        if (!a->has_base) return true;
        if (a->base.tag != b->base.tag || !jazz_model__model__branch__BranchKey__PartialEq__eq(a->base.key, b->base.key)) return false;
        return a->base.tag == JAZZ_ENGINE_BVSB_CURRENT || jazz_model__model__branch__SnapshotRef__PartialEq__eq(a->base.snapshot, b->base.snapshot);
    case JAZZ_ENGINE_SRC_HISTORY_CUT: return a->position.t == b->position.t;
    case JAZZ_ENGINE_SRC_SNAPSHOT_REF: return jazz_model__tx__Snapshot__PartialEq__eq(a->snapshot, b->snapshot);
    case JAZZ_ENGINE_SRC_SETTLED_BINDING_VIEW:
        if (a->current_default != b->current_default || memcmp(&a->binding_view, &b->binding_view, sizeof a->binding_view) != 0) return false;
        if (a->has_authority_result_key != b->has_authority_result_key) return false;
        return !a->has_authority_result_key || memcmp(&a->authority_result_key, &b->authority_result_key, sizeof a->authority_result_key) == 0;
    case JAZZ_ENGINE_SRC_WITH_OVERLAYS:
        if (!JE_READ(SourceExpr__PartialEq__eq)(a->input, b->input) || a->overlays.len != b->overlays.len) return false;
        for (u32 i = 0; i < a->overlays.len; i++)
            if (!overlay_eq(&a->overlays.ptr[i], &b->overlays.ptr[i])) return false;
        return true;
    case JAZZ_ENGINE_SRC_LENS_PROJECT: return JE_READ(SourceExpr__PartialEq__eq)(a->input, b->input);
    case JAZZ_ENGINE_SRC_MERGE:
        if (a->mode != b->mode || a->inputs.len != b->inputs.len) return false;
        for (u32 i = 0; i < a->inputs.len; i++)
            if (!JE_READ(SourceExpr__PartialEq__eq)(&a->inputs.ptr[i], &b->inputs.ptr[i])) return false;
        return true;
    }
    return false;
}
static void source_expr_debug_v(jz_str *out, const void *e) { JE_READ(SourceExpr__Debug__fmt)(out, e); }
void JE_READ(SourceExpr__Debug__fmt)(jz_str *out, const jazz_engine_SourceExpr *e) {
    static const char *const names[] = {"VisibleCurrent", "BranchView", "HistoryCut", "SnapshotRef", "SettledBindingView", "WithOverlays", "LensProject", "Merge"};
    jazz_engine__fmt__struct_begin(out, names[e->tag]);
    bool first = true;
    if (e->tag == JAZZ_ENGINE_SRC_SETTLED_BINDING_VIEW) {
        jazz_engine__fmt__field(out, "current_default", true);
        jazz_engine__fmt__bool(out, e->current_default);
        first = false;
    }
    if (has_projection(e->tag) && e->tag != JAZZ_ENGINE_SRC_LENS_PROJECT) {
        jazz_engine__fmt__field(out, "projection", first);
        projection_debug(out, &e->projection);
        first = false;
    }
    if (has_data(e->tag)) {
        jazz_engine__fmt__field(out, "data", false);
        data_debug(out, &e->data);
    }
    switch (e->tag) {
    case JAZZ_ENGINE_SRC_VISIBLE_CURRENT:
        jazz_engine__fmt__field(out, "tier", false);
        jazz_engine__tier_debug(out, e->tier);
        break;
    case JAZZ_ENGINE_SRC_BRANCH_VIEW:
        jazz_engine__fmt__field(out, "head", false);
        jazz_model__model__branch__BranchKey__Debug__fmt(out, e->head);
        jazz_engine__fmt__field(out, "base", false);
        if (!e->has_base) {
            jazz_engine__fmt__option_none(out);
        } else {
            jazz_engine__fmt__tuple_begin(out, "Some");
            jazz_engine__fmt__tuple_begin(out, e->base.tag == JAZZ_ENGINE_BVSB_CURRENT ? "Current" : "Snapshot");
            jazz_model__model__branch__BranchKey__Debug__fmt(out, e->base.key);
            if (e->base.tag == JAZZ_ENGINE_BVSB_SNAPSHOT) {
                jz_str_push_cstr(out, ", ");
                jazz_model__model__branch__SnapshotRef__Debug__fmt(out, e->base.snapshot);
            }
            jazz_engine__fmt__tuple_end(out);
            jazz_engine__fmt__tuple_end(out);
        }
        jazz_engine__fmt__field(out, "tier", false);
        jazz_engine__tier_debug(out, e->tier);
        break;
    case JAZZ_ENGINE_SRC_HISTORY_CUT:
        jazz_engine__fmt__field(out, "position", false);
        jz_str_appendf(out, "GlobalTime(%llu)", (unsigned long long)e->position.t);
        break;
    case JAZZ_ENGINE_SRC_SNAPSHOT_REF:
        jazz_engine__fmt__field(out, "snapshot", false);
        jazz_model__tx__Snapshot__Debug__fmt(out, e->snapshot);
        break;
    case JAZZ_ENGINE_SRC_SETTLED_BINDING_VIEW:
        jazz_engine__fmt__field(out, "binding_view", false);
        jazz_protocol__protocol__BindingViewKey__Debug__fmt(out, &e->binding_view);
        jazz_engine__fmt__field(out, "authority_result_key", false);
        if (e->has_authority_result_key) {
            jazz_engine__fmt__tuple_begin(out, "Some");
            jazz_protocol__protocol__AuthorityResultKey__Debug__fmt(out, &e->authority_result_key);
            jazz_engine__fmt__tuple_end(out);
        } else {
            jazz_engine__fmt__option_none(out);
        }
        break;
    case JAZZ_ENGINE_SRC_WITH_OVERLAYS:
        jazz_engine__fmt__field(out, "input", true);
        JE_READ(SourceExpr__Debug__fmt)(out, e->input);
        jazz_engine__fmt__field(out, "overlays", false);
        jazz_engine__fmt__struct_begin(out, "OverlayStack");
        jazz_engine__fmt__field(out, "entries", true);
        jazz_engine__fmt__list(out, e->overlays.ptr, e->overlays.len, sizeof(jazz_engine_OverlayRef), overlay_debug_v);
        jazz_engine__fmt__struct_end(out);
        break;
    case JAZZ_ENGINE_SRC_LENS_PROJECT:
        jazz_engine__fmt__field(out, "input", true);
        JE_READ(SourceExpr__Debug__fmt)(out, e->input);
        jazz_engine__fmt__field(out, "projection", false);
        projection_debug(out, &e->projection);
        break;
    case JAZZ_ENGINE_SRC_MERGE:
        jazz_engine__fmt__field(out, "inputs", true);
        jazz_engine__fmt__list(out, e->inputs.ptr, e->inputs.len, sizeof(jazz_engine_SourceExpr), source_expr_debug_v);
        jazz_engine__fmt__field(out, "mode", false);
        jz_str_push_cstr(out, e->mode == JAZZ_ENGINE_MERGE_LAST_WRITE_WINS ? "LastWriteWins" : "Union");
        jazz_engine__fmt__field(out, "resolution", false);
        jz_str_push_cstr(out, "()");
        break;
    }
    jazz_engine__fmt__struct_end(out);
}

bool JE_READ(SourceExpr__current_tier)(const jazz_engine_SourceExpr *e, u8 *tier) {
    switch (e->tag) {
    case JAZZ_ENGINE_SRC_VISIBLE_CURRENT:
    case JAZZ_ENGINE_SRC_BRANCH_VIEW: *tier = e->tier; return true;
    case JAZZ_ENGINE_SRC_WITH_OVERLAYS:
    case JAZZ_ENGINE_SRC_LENS_PROJECT: return JE_READ(SourceExpr__current_tier)(e->input, tier);
    case JAZZ_ENGINE_SRC_MERGE: {
        bool have_first = false;
        u8 first = 0;
        JZ_VEC_FOR(const jazz_engine_SourceExpr, input, e->inputs) {
            u8 t;
            if (!JE_READ(SourceExpr__current_tier)(input, &t)) continue;
            if (!have_first) {
                first = t;
                have_first = true;
            } else if (t != first) {
                return false;
            }
        }
        if (!have_first) return false;
        *tier = first;
        return true;
    }
    default: return false;
    }
}

const jz_btree_desc jazz_engine__SOURCE_EXPR_MAP_DESC = {sizeof(jazz_engine_SourceId), sizeof(jazz_engine_SourceExpr), jazz_engine__sid_cmp_v, jazz_engine__sid_drop_v, source_expr_drop_v};

bool JE_READ(ReadView__source_current_tier)(const jazz_engine_ReadView *v, const jazz_engine_SourceId *source, u8 *tier) {
    const jazz_engine_SourceExpr *e = jz_btree_get(&v->sources, source);
    if (!e) return false;
    return JE_READ(SourceExpr__current_tier)(e, tier);
}
jazz_types_SchemaVersionId JE_READ(ReadView__read_schema)(const jazz_engine_ReadView *v) { return v->read_schema; }
void JE_READ(ReadView__Clone__clone)(jazz_engine_ReadView *dst, const jazz_engine_ReadView *src) {
    dst->read_schema = src->read_schema;
    dst->policy_schema = src->policy_schema;
    jz_btree_clone(&dst->sources, &src->sources, jazz_engine__sid_clone_v, source_expr_clone_v);
}
void JE_READ(ReadView__drop)(jazz_engine_ReadView *v) { jz_btree_drop(&v->sources); }
void JE_READ(ReadView__Debug__fmt)(jz_str *out, const jazz_engine_ReadView *v) {
    jazz_engine__fmt__struct_begin(out, "ReadView");
    jazz_engine__fmt__field(out, "read_schema", true);
    svid_debug_v(out, &v->read_schema);
    jazz_engine__fmt__field(out, "policy_schema", false);
    svid_debug_v(out, &v->policy_schema);
    jazz_engine__fmt__field(out, "sources", false);
    jazz_engine__fmt__map(out, &v->sources, jazz_engine__sid_debug_v, source_expr_debug_v);
    jazz_engine__fmt__struct_end(out);
}

static int u8_cmp_v(const void *a, const void *b, void *ctx) { return (int)*(const u8 *)a - (int)*(const u8 *)b; }
static void read_view_drop_v(void *v) { JE_READ(ReadView__drop)(v); }
static void read_view_clone_v(void *d, const void *s) { JE_READ(ReadView__Clone__clone)(d, s); }
static void u8_clone_v(void *d, const void *s) { *(u8 *)d = *(const u8 *)s; }
const jz_btree_desc jazz_engine__FACT_READ_MAP_DESC = {sizeof(u8), sizeof(jazz_engine_ReadView), u8_cmp_v, NULL, read_view_drop_v};

void JE_READ(QueryReadSet__primary)(jazz_engine_ReadView primary, jazz_engine_QueryReadSet *out) {
    out->primary = primary;
    jz_btree_init(&out->fact_reads, &jazz_engine__FACT_READ_MAP_DESC);
}
void JE_READ(QueryReadSet__Clone__clone)(jazz_engine_QueryReadSet *dst, const jazz_engine_QueryReadSet *src) {
    JE_READ(ReadView__Clone__clone)(&dst->primary, &src->primary);
    jz_btree_clone(&dst->fact_reads, &src->fact_reads, u8_clone_v, read_view_clone_v);
}
void JE_READ(QueryReadSet__drop)(jazz_engine_QueryReadSet *r) {
    JE_READ(ReadView__drop)(&r->primary);
    jz_btree_drop(&r->fact_reads);
}
static void role_debug_v(jz_str *out, const void *r) {
    jz_str_push_cstr(out, *(const u8 *)r == JAZZ_ENGINE_FRR_PREDICATE_OUTPUT_BASE ? "PredicateOutputBase" : "PredicateOutputNow");
}
static void read_view_debug_v(jz_str *out, const void *v) { JE_READ(ReadView__Debug__fmt)(out, v); }
void JE_READ(QueryReadSet__Debug__fmt)(jz_str *out, const jazz_engine_QueryReadSet *r) {
    jazz_engine__fmt__struct_begin(out, "QueryReadSet");
    jazz_engine__fmt__field(out, "primary", true);
    JE_READ(ReadView__Debug__fmt)(out, &r->primary);
    jazz_engine__fmt__field(out, "fact_reads", false);
    jazz_engine__fmt__map(out, &r->fact_reads, role_debug_v, read_view_debug_v);
    jazz_engine__fmt__struct_end(out);
}

/* derived PartialEq for ReadView / QueryReadSet (used by the template cache's request equality) */
static bool source_expr_eq_v(const void *a, const void *b) { return JE_READ(SourceExpr__PartialEq__eq)(a, b); }
bool JE_READ(ReadView__PartialEq__eq)(const jazz_engine_ReadView *a, const jazz_engine_ReadView *b) {
    return jz_uuid_eq(&a->read_schema.u, &b->read_schema.u) && jz_uuid_eq(&a->policy_schema.u, &b->policy_schema.u) &&
           jz_btree_eq(&a->sources, &b->sources, source_expr_eq_v);
}
static bool read_view_eq_v(const void *a, const void *b) { return JE_READ(ReadView__PartialEq__eq)(a, b); }
bool JE_READ(QueryReadSet__PartialEq__eq)(const jazz_engine_QueryReadSet *a, const jazz_engine_QueryReadSet *b) {
    return JE_READ(ReadView__PartialEq__eq)(&a->primary, &b->primary) && jz_btree_eq(&a->fact_reads, &b->fact_reads, read_view_eq_v);
}
