/* jazz_node::node::query_eval::read_sources
 *
 * Turn logical query read requirements into concrete Groove source graphs.
 * This stage chooses physical, inline, historical, branch, or settled-view
 * inputs and applies durability, schema projection, and access-path decisions.
 *
 * Napkin C port (unit node-qe-a). The Rust preparer is generic over S:
 * OrderedKvStorage (shipped only as BoxedStorage); here the node carries the
 * storage vtable, so there is one copy. Async fns are synchronous. */
#include "jz_rt.h"
#include "jazz_node/_ext_node-qe-a.h"
#include "jazz_node/node/query_eval/read_sources.h"
#include "jazz_node/node/query_eval/lowering.h"
#include "jazz_node/node/query_eval/normalization.h"

#define SL(s) jz_str_slice(&(s))
#define CS(lit) JZ_LIT(lit)
typedef jazz_node_JazzSourceGraphPreparer Prep;
typedef jazz_engine_SourceRequest SReq;
typedef u32 SchemaVersionAlias;

/* ---------- shared btree descriptors ---------- */
static void str_drop_v(void *p) { jz_str_drop((jz_str *)p); }
static void str_clone_v(void *d, const void *s) { *(jz_str *)d = jz_str_clone((const jz_str *)s); }
static void desc_drop_v(void *p) { (void)p; }
const jz_btree_desc jazz_node_AccessPaths_desc = { sizeof(jazz_engine_SourceId), sizeof(jazz_node_CurrentAccessPath), jazz_engine_SourceId_cmp, jazz_engine_SourceId_drop, jazz_node_CurrentAccessPath_drop };
const jz_btree_desc jazz_node_SourceGraphs_desc = { sizeof(jazz_engine_SourceId), sizeof(groove_GraphBuilder *), jazz_engine_SourceId_cmp, jazz_engine_SourceId_drop, groove_GraphBuilder_free_v };
const jz_btree_desc jazz_node_SourceDescriptors_desc = { sizeof(jazz_engine_SourceId), sizeof(groove_RecordDescriptor), jazz_engine_SourceId_cmp, jazz_engine_SourceId_drop, desc_drop_v };
const jz_btree_desc jazz_node_SourceStrings_desc = { sizeof(jazz_engine_SourceId), sizeof(jz_str), jazz_engine_SourceId_cmp, jazz_engine_SourceId_drop, str_drop_v };
const jz_btree_desc jazz_node_StrSet_desc = { sizeof(jz_str), 0, jz_cmp_str, str_drop_v, NULL };
const jz_btree_desc jazz_node_StrStr_desc = { sizeof(jz_str), sizeof(jz_str), jz_cmp_str, str_drop_v, str_drop_v };
const jz_btree_desc jazz_node_StrValue_desc = { sizeof(jz_str), sizeof(groove_Value), jz_cmp_str, str_drop_v, groove_Value_drop_v };
static const jz_btree_desc column_ids_desc = { sizeof(jz_str), sizeof(jazz_types_PhysicalColumnId), jz_cmp_str, str_drop_v, NULL };

static void strset_insert(jz_btree *set, jz_str s) { if (jz_btree_insert(set, &s, NULL, NULL)) { /* replaced: key already moved in */ } }
static void strvec_push_cstr(jz_vec_str *v, const char *s) { jz_str x = jz_str_from(s); jz_vec_push(v, &x, sizeof(jz_str)); }
static void strvec_drop(jz_vec_str *v) { jz_vec_drop(v, sizeof(jz_str), str_drop_v); }
static void valuevec_clone(groove_ValueVec *dst, const groove_ValueVec *src) { jz_vec_clone(dst, src, sizeof(groove_Value), groove_Value_clone_v); }
static void valuevec_drop(groove_ValueVec *v) { jz_vec_drop(v, sizeof(groove_Value), groove_Value_drop_v); }
static bool valuevec_eq(const groove_ValueVec *a, const groove_ValueVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!groove_Value_eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
static bool opt_str_eq(bool ha, const jz_str *a, bool hb, const jz_str *b) { return ha == hb && (!ha || jz_str_eq(a, b)); }

/* ---------- current_row_column_field ---------- */
static groove_DescriptorField jazz_node__node__query_eval__read_sources__current_row_column_field(const jazz_model_ColumnSchema *column, groove_ValueType value_type) {
    groove_DescriptorField f = groove__records__DescriptorField__new(jazz_engine__query_engine__fields__user_column_field(SL(column->name)), value_type);
    return groove__records__DescriptorField__with_identity_name(f, jz_str_clone(&column->name));
}

/* ---------- CurrentIndexCandidateFilter / CurrentAccessPath derives ---------- */
void jazz_node_CurrentIndexCandidateFilter_drop(jazz_node_CurrentIndexCandidateFilter *f) {
    jz_str_drop(&f->table); jz_str_drop(&f->column); if (f->has_order_column) jz_str_drop(&f->order_column);
    valuevec_drop(&f->prefix); jz_str_drop(&f->source_column);
}
void jazz_node_CurrentIndexCandidateFilter_clone(jazz_node_CurrentIndexCandidateFilter *d, const jazz_node_CurrentIndexCandidateFilter *s) {
    d->table = jz_str_clone(&s->table); d->column = jz_str_clone(&s->column);
    d->has_order_column = s->has_order_column; if (s->has_order_column) d->order_column = jz_str_clone(&s->order_column);
    valuevec_clone(&d->prefix, &s->prefix); d->source_column = jz_str_clone(&s->source_column);
}
bool jazz_node_CurrentIndexCandidateFilter_eq(const jazz_node_CurrentIndexCandidateFilter *a, const jazz_node_CurrentIndexCandidateFilter *b) {
    return jz_str_eq(&a->table, &b->table) && jz_str_eq(&a->column, &b->column)
        && opt_str_eq(a->has_order_column, &a->order_column, b->has_order_column, &b->order_column)
        && valuevec_eq(&a->prefix, &b->prefix) && jz_str_eq(&a->source_column, &b->source_column);
}
void jazz_node_IndexIntersection_drop(void *p) { jazz_node_IndexIntersection *x = p; jz_str_drop(&x->column); valuevec_drop(&x->values); }
void jazz_node_IndexIntersection_clone(void *dst, const void *src) {
    jazz_node_IndexIntersection *d = dst; const jazz_node_IndexIntersection *s = src;
    d->column = jz_str_clone(&s->column); valuevec_clone(&d->values, &s->values);
}
void jazz_node_CurrentAccessPath_drop(void *pp) {
    jazz_node_CurrentAccessPath *p = pp;
    switch (p->tag) {
    case JAZZ_CAP_PRIMARY_KEY: valuevec_drop(&p->prefix); break;
    case JAZZ_CAP_PRIMARY_KEY_PAGE: break;
    case JAZZ_CAP_INDEX:
        jz_str_drop(&p->column); if (p->has_order_column) jz_str_drop(&p->order_column);
        valuevec_drop(&p->prefix);
        jz_vec_drop(&p->intersections, sizeof(jazz_node_IndexIntersection), jazz_node_IndexIntersection_drop);
        if (p->candidate_filter) { jazz_node_CurrentIndexCandidateFilter_drop(p->candidate_filter); jz_free(p->candidate_filter); }
        break;
    }
}
void jazz_node_CurrentAccessPath_clone(void *dst, const void *src) {
    jazz_node_CurrentAccessPath *d = dst; const jazz_node_CurrentAccessPath *s = src;
    memset(d, 0, sizeof *d);
    d->tag = s->tag;
    switch (s->tag) {
    case JAZZ_CAP_PRIMARY_KEY: valuevec_clone(&d->prefix, &s->prefix); break;
    case JAZZ_CAP_PRIMARY_KEY_PAGE: d->cap = s->cap; break;
    case JAZZ_CAP_INDEX:
        d->column = jz_str_clone(&s->column);
        d->has_order_column = s->has_order_column; if (s->has_order_column) d->order_column = jz_str_clone(&s->order_column);
        d->reverse = s->reverse;
        valuevec_clone(&d->prefix, &s->prefix);
        jz_vec_clone(&d->intersections, &s->intersections, sizeof(jazz_node_IndexIntersection), jazz_node_IndexIntersection_clone);
        if (s->candidate_filter) { d->candidate_filter = JZ_NEW(jazz_node_CurrentIndexCandidateFilter); jazz_node_CurrentIndexCandidateFilter_clone(d->candidate_filter, s->candidate_filter); }
        d->maintained = s->maintained; d->has_source_limit = s->has_source_limit; d->source_limit = s->source_limit;
        break;
    }
}
bool jazz_node_CurrentAccessPath_eq(const jazz_node_CurrentAccessPath *a, const jazz_node_CurrentAccessPath *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_CAP_PRIMARY_KEY: return valuevec_eq(&a->prefix, &b->prefix);
    case JAZZ_CAP_PRIMARY_KEY_PAGE: return a->cap == b->cap;
    case JAZZ_CAP_INDEX: {
        if (!jz_str_eq(&a->column, &b->column) || !opt_str_eq(a->has_order_column, &a->order_column, b->has_order_column, &b->order_column)
            || a->reverse != b->reverse || !valuevec_eq(&a->prefix, &b->prefix) || a->intersections.len != b->intersections.len)
            return false;
        for (u32 i = 0; i < a->intersections.len; i++)
            if (!jz_str_eq(&a->intersections.ptr[i].column, &b->intersections.ptr[i].column) || !valuevec_eq(&a->intersections.ptr[i].values, &b->intersections.ptr[i].values))
                return false;
        if ((a->candidate_filter == NULL) != (b->candidate_filter == NULL)) return false;
        if (a->candidate_filter && !jazz_node_CurrentIndexCandidateFilter_eq(a->candidate_filter, b->candidate_filter)) return false;
        return a->maintained == b->maintained && a->has_source_limit == b->has_source_limit && (!a->has_source_limit || a->source_limit == b->source_limit);
    }
    }
    return false;
}
static void values_debug(jz_str *out, const groove_ValueVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) { if (i) jz_str_push_cstr(out, ", "); groove_Value_debug(out, &v->ptr[i]); }
    jz_str_push_char(out, ']');
}
static void opt_str_debug(jz_str *out, bool has, const jz_str *s) {
    if (!has) { jz_str_push_cstr(out, "None"); return; }
    jz_str_appendf(out, "Some(\"%.*s\")", (int)s->len, s->ptr);
}
void jazz_node_CurrentAccessPath_debug(jz_str *out, const jazz_node_CurrentAccessPath *p) {
    switch (p->tag) {
    case JAZZ_CAP_PRIMARY_KEY: jz_str_push_cstr(out, "PrimaryKey("); values_debug(out, &p->prefix); jz_str_push_char(out, ')'); break;
    case JAZZ_CAP_PRIMARY_KEY_PAGE: jz_str_appendf(out, "PrimaryKeyPage { cap: %u }", p->cap); break;
    case JAZZ_CAP_INDEX:
        jz_str_appendf(out, "Index { column: \"%.*s\", order_column: ", (int)p->column.len, p->column.ptr);
        opt_str_debug(out, p->has_order_column, &p->order_column);
        jz_str_appendf(out, ", reverse: %s, prefix: ", p->reverse ? "true" : "false");
        values_debug(out, &p->prefix);
        jz_str_push_cstr(out, ", intersections: [");
        for (u32 i = 0; i < p->intersections.len; i++) {
            if (i) jz_str_push_cstr(out, ", ");
            jz_str_appendf(out, "(\"%.*s\", ", (int)p->intersections.ptr[i].column.len, p->intersections.ptr[i].column.ptr);
            values_debug(out, &p->intersections.ptr[i].values);
            jz_str_push_char(out, ')');
        }
        jz_str_push_cstr(out, "], candidate_filter: ");
        if (!p->candidate_filter) jz_str_push_cstr(out, "None");
        else {
            const jazz_node_CurrentIndexCandidateFilter *f = p->candidate_filter;
            jz_str_appendf(out, "Some(CurrentIndexCandidateFilter { table: \"%.*s\", column: \"%.*s\", order_column: ", (int)f->table.len, f->table.ptr, (int)f->column.len, f->column.ptr);
            opt_str_debug(out, f->has_order_column, &f->order_column);
            jz_str_push_cstr(out, ", prefix: "); values_debug(out, &f->prefix);
            jz_str_appendf(out, ", source_column: \"%.*s\" })", (int)f->source_column.len, f->source_column.ptr);
        }
        jz_str_appendf(out, ", maintained: %s, source_limit: ", p->maintained ? "true" : "false");
        if (p->has_source_limit) jz_str_appendf(out, "Some(%u)", p->source_limit); else jz_str_push_cstr(out, "None");
        jz_str_push_cstr(out, " }");
        break;
    }
}
void jazz_node_AccessPaths_debug(jz_str *out, const jz_btree *paths) {
    jazz_engine_SourceId *k; jazz_node_CurrentAccessPath *v; bool first = true;
    jz_str_push_char(out, '{');
    JZ_BTREE_FOR(paths, k, v) {
        if (!first) jz_str_push_cstr(out, ", ");
        first = false;
        jazz_engine_SourceId_debug(out, k); jz_str_push_cstr(out, ": "); jazz_node_CurrentAccessPath_debug(out, v);
    }
    jz_str_push_char(out, '}');
}
void jazz_node_AccessPaths_clone(jz_btree *dst, const jz_btree *src) { jz_btree_clone(dst, src, jazz_engine_SourceId_clone, jazz_node_CurrentAccessPath_clone); }

/* ---------- TransactionWriteOverlay ---------- */
static void overlay_table_key_drop(void *p) { jz_str_drop(&((jazz_node_OverlayTableKey *)p)->table); }
static int overlay_table_key_cmp(const void *a, const void *b, void *ctx) {
    const jazz_node_OverlayTableKey *x = a, *y = b;
    int c = jz_uuid_cmp(&x->schema.u, &y->schema.u);
    return c ? c : jz_str_cmp(&x->table, &y->table);
}
static void overlay_table_rc_release(void *p) { jz_rc_release(*(void **)p); }
static const jz_btree_desc overlay_tables_desc = { sizeof(jazz_node_OverlayTableKey), sizeof(jazz_node_TransactionOverlayTable *), overlay_table_key_cmp, overlay_table_key_drop, overlay_table_rc_release };
static const jz_btree_desc overlay_reads_desc = { sizeof(jazz_node_OverlayTableKey), 0, overlay_table_key_cmp, overlay_table_key_drop, NULL };
static void overlay_tables_drop(void *p) { jz_btree_drop((jz_btree *)p); }
static void current_row_box_drop(void *p) { jazz_node_CurrentRow **r = p; if (*r) { jazz_node_CurrentRow_drop(*r); jz_free(*r); } }
static int row_uuid_cmp(const void *a, const void *b, void *ctx) { return jz_uuid_cmp(&((const jazz_types_RowUuid *)a)->u, &((const jazz_types_RowUuid *)b)->u); }
static const jz_btree_desc overlay_rows_desc = { sizeof(jazz_types_RowUuid), sizeof(jazz_node_CurrentRow *), row_uuid_cmp, NULL, current_row_box_drop };
static void overlay_record_vec_drop(void *p) {
    jazz_node_OverlayRecordVec *v = p;
    for (u32 i = 0; i < v->len; i++) jz_bytes_drop(&v->ptr[i].record);
    jz_vec_drop(v, sizeof(jazz_node_OverlayRecord), NULL);
}
static void overlay_cache_drop(jazz_node_OverlayCache *c) {
    for (u32 i = 0; i < c->entries.len; i++) jz_rc_release(c->entries.ptr[i].records);
    jz_vec_drop(&c->entries, sizeof(jazz_node_OverlayCacheEntry), NULL);
}
static void overlay_table_drop(void *p) {
    jazz_node_TransactionOverlayTable *t = p;
    jz_btree_drop(&t->rows); overlay_cache_drop(&t->live_records); overlay_cache_drop(&t->written_records);
}
static void overlay_reads_drop(void *p) { jz_btree_drop(&((jazz_node_OverlayReads *)p)->set); }

static jz_btree *empty_overlay_tables(void) {
    jz_btree *t = jz_rc_new(sizeof(jz_btree), overlay_tables_drop);
    jz_btree_init(t, &overlay_tables_desc);
    return t;
}
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__Default__default(void) {
    jazz_node_TransactionWriteOverlay o;
    memset(&o, 0, sizeof o);
    o.tables = empty_overlay_tables();
    o.has_excluded = false;
    o.reads = NULL;
    o.evidence_tier = JAZZ_TIER_LOCAL;
    return o;
}
void jazz_node_TransactionWriteOverlay_drop(jazz_node_TransactionWriteOverlay *o) {
    jz_rc_release(o->tables);
    if (o->has_excluded) jz_str_drop(&o->excluded_table);
    if (o->reads) jz_rc_release(o->reads);
}
void jazz_node_TransactionWriteOverlay_clone(jazz_node_TransactionWriteOverlay *d, const jazz_node_TransactionWriteOverlay *s) {
    *d = *s;
    d->tables = jz_rc_clone(s->tables);
    if (s->has_excluded) d->excluded_table = jz_str_clone(&s->excluded_table);
    if (s->reads) d->reads = jz_rc_clone(s->reads);
}
jazz_node_TransactionOverlayTable *jazz_node__node__query_eval__read_sources__TransactionOverlayTable__new(jz_btree rows) {
    jazz_node_TransactionOverlayTable *t = jz_rc_new(sizeof *t, overlay_table_drop);
    memset(t, 0, sizeof *t);
    t->rows = rows;
    return t;
}
u32 jazz_node__node__query_eval__read_sources__TransactionOverlayTable__len(const jazz_node_TransactionOverlayTable *t) { return t->rows.len; }
bool jazz_node__node__query_eval__read_sources__TransactionOverlayTable__is_empty(const jazz_node_TransactionOverlayTable *t) { return t->rows.len == 0; }

typedef jz_err (*overlay_encode_fn)(void *ctx, jazz_node_OverlayRecordVec *out);
static jz_err jazz_node__node__query_eval__read_sources__TransactionOverlayTable__cached(
    jazz_node_OverlayCache *cache, groove_RecordDescriptor descriptor, overlay_encode_fn encode, void *ctx, jazz_node_OverlayRecordVec **out) {
    if (cache->poisoned) return JZN_INVALID_STORED("transaction overlay cache poisoned");
    for (u32 i = 0; i < cache->entries.len; i++) {
        if (cache->entries.ptr[i].descriptor == descriptor) { *out = jz_rc_clone(cache->entries.ptr[i].records); return JZ_OK; }
    }
    jazz_node_OverlayRecordVec *records = jz_rc_new(sizeof *records, overlay_record_vec_drop);
    memset(records, 0, sizeof *records);
    jz_err e = encode(ctx, records);
    if (JZ_IS_ERR(e)) { jz_rc_release(records); return e; }
    if (cache->poisoned) { jz_rc_release(records); return JZN_INVALID_STORED("transaction overlay cache poisoned"); }
    jazz_node_OverlayCacheEntry entry = { descriptor, jz_rc_clone(records) };
    jz_vec_push(&cache->entries, &entry, sizeof entry);
    *out = records;
    return JZ_OK;
}
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__from_tables(jz_btree *tables) {
    jazz_node_TransactionWriteOverlay o;
    memset(&o, 0, sizeof o);
    o.tables = tables;
    o.has_excluded = false;
    o.reads = NULL;
    o.evidence_tier = JAZZ_TIER_GLOBAL;
    return o;
}
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__accepted_state(void) {
    return jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__from_tables(empty_overlay_tables());
}
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__committed_view(const jazz_node_TransactionWriteOverlay *s) {
    jazz_node_TransactionWriteOverlay o = jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__Default__default();
    o.evidence_tier = s->evidence_tier;
    return o;
}
jazz_model_DurabilityTier jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__evidence_tier(const jazz_node_TransactionWriteOverlay *o) { return o->evidence_tier; }
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__excluding(const jazz_node_TransactionWriteOverlay *s, jazz_types_SchemaVersionId schema, jz_slice table, jazz_types_RowUuid row) {
    jazz_node_TransactionWriteOverlay o;
    o.tables = jz_rc_clone(s->tables);
    o.has_excluded = true; o.excluded_schema = schema; o.excluded_table = jz_str_from_slice(table); o.excluded_row = row;
    o.reads = s->reads ? jz_rc_clone(s->reads) : NULL;
    o.evidence_tier = s->evidence_tier;
    return o;
}
jazz_node_TransactionWriteOverlay jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__recording(const jazz_node_TransactionWriteOverlay *s, jazz_node_OverlayReads *reads) {
    jazz_node_TransactionWriteOverlay o;
    o.tables = jz_rc_clone(s->tables);
    o.has_excluded = s->has_excluded;
    if (s->has_excluded) { o.excluded_schema = s->excluded_schema; o.excluded_table = jz_str_clone(&s->excluded_table); o.excluded_row = s->excluded_row; }
    o.reads = reads;
    o.evidence_tier = s->evidence_tier;
    return o;
}
bool jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__is_active(const jazz_node_TransactionWriteOverlay *o) {
    if (o->reads) return true;
    jazz_node_OverlayTableKey *k; jazz_node_TransactionOverlayTable **t;
    JZ_BTREE_FOR(o->tables, k, t) { if ((*t)->rows.len != 0) return true; }
    return false;
}
u32 jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__rows_in(const jazz_node_TransactionWriteOverlay *o, const jz_btree *tables) {
    u32 sum = 0;
    jazz_node_OverlayTableKey *k; void *unused;
    JZ_BTREE_FOR(tables, k, unused) {
        jazz_node_TransactionOverlayTable **t = jz_btree_get(o->tables, k);
        if (t) sum += (*t)->rows.len;
    }
    return sum;
}
static void jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__record_read(const jazz_node_TransactionWriteOverlay *o, jazz_types_SchemaVersionId schema, jz_slice table) {
    if (o->reads && !o->reads->poisoned) {
        jazz_node_OverlayTableKey key = { schema, jz_str_from_slice(table) };
        if (jz_btree_insert(&o->reads->set, &key, NULL, NULL)) { /* already present */ }
    }
}
static jazz_node_TransactionOverlayTable *jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__table(const jazz_node_TransactionWriteOverlay *o, jazz_types_SchemaVersionId schema, jz_slice table) {
    jazz_node_OverlayTableKey key = { schema, jz_str_from_slice(table) };
    jazz_node_TransactionOverlayTable **t = jz_btree_get(o->tables, &key);
    jz_str_drop(&key.table);
    if (!t || (*t)->rows.len == 0) return NULL;
    return *t;
}
static bool jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__excludes(const jazz_node_TransactionWriteOverlay *o, jazz_types_SchemaVersionId schema, jz_slice table, jazz_types_RowUuid row) {
    if (!o->has_excluded) return false;
    jz_str t = jz_str_from_slice(table);
    bool r = jz_uuid_eq(&o->excluded_schema.u, &schema.u) && jz_str_eq(&o->excluded_table, &t) && jz_uuid_eq(&o->excluded_row.u, &row.u);
    jz_str_drop(&t);
    return r;
}

/* ---------- small helpers ---------- */
static groove_StaticScanSpec jazz_node__node__query_eval__read_sources__primary_key_page_scan(u32 cap) {
    /* The physical source helper prepends the shared branch coordinate. The
     * remaining primary-key column is row_uuid, matching default result order. */
    groove_StaticScanSpec s; memset(&s, 0, sizeof s);
    s.tag = GROOVE_SCAN_PREFIX_LIMIT; s.max_items = cap;
    return s;
}

static jz_sre jazz_node__node__query_eval__read_sources__source_resolution_error(const SReq *request, jazz_engine_SourceGap gap) {
    jazz_engine_SourceResolutionError *e = JZ_NEW(jazz_engine_SourceResolutionError);
    e->request = JZ_NEW(jazz_engine_SourceRequest);
    jazz_engine_SourceRequest_clone(e->request, request);
    e->gap = gap;
    return e;
}
static jz_sre sre(const SReq *request, u32 gap_tag) {
    jazz_engine_SourceGap gap; memset(&gap, 0, sizeof gap); gap.tag = gap_tag;
    return jazz_node__node__query_eval__read_sources__source_resolution_error(request, gap);
}
/* map_err(|_| source_resolution_error(request, gap))? */
#define SRE_MAP(expr, req, gap) do { jz_err _e = (expr); if (JZ_IS_ERR(_e)) { jz_err_drop(&_e); return sre((req), (gap)); } } while (0)
#define SRE_TRY(expr) do { jz_sre _s = (expr); if (_s) return _s; } while (0)

static jz_sre jazz_node__node__query_eval__read_sources__source_resolution_error_from_policy_proof(const SReq *request, jz_err error);
static jazz_engine_ReadView jazz_node__node__query_eval__read_sources__policy_read_view_projected_through(const jazz_engine_ReadView *policy_view, const jazz_engine_ReadView *enclosing_view);
static jz_err jazz_node__node__query_eval__read_sources__with_requested_author_paths(groove_GraphBuilder *graph, groove_RecordDescriptor descriptor, jz_btree metadata, const jazz_engine_SourceRequirements *requirements,
    groove_GraphBuilder **out_graph, groove_RecordDescriptor *out_desc, jz_btree *out_meta);
static jz_err jazz_node__node__query_eval__read_sources__resolved_current_source_graph(jazz_node_NodeState *node, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier,
    const jazz_engine_SourceRequirements *requirements, const jazz_engine_SourceAuthorizationRequest *authorization, jazz_types_SchemaVersionId policy_schema,
    const jazz_engine_ReadView *policy_read_view, const char *branch_witness_field, groove_GraphBuilder *selected_base,
    groove_GraphBuilder **out_graph, groove_RecordDescriptor *out_desc, jz_btree *out_meta, jz_btree *out_routing);
static groove_ProjectFieldVec jazz_node__node__query_eval__read_sources__storage_to_canonical_current_source_fields(const jazz_model_TableSchema *table, bool include_version, bool include_settle_position);
static jz_err jazz_node__node__query_eval__read_sources__branch_view_storage_source_fields(const jazz_model_TableSchema *table, const jazz_model_BranchKey *head, groove_ProjectFieldVec *out);
static groove_RecordDescriptor jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields_for_branch(const jazz_model_TableSchema *table, const jz_btree *metadata, bool branch_columns_nonnullable);
static groove_RecordDescriptor jazz_node__node__query_eval__read_sources__current_row_descriptor(const jazz_model_TableSchema *table);
static jz_err jazz_node__node__query_eval__read_sources__inline_current_graph(const jazz_model_TableSchema *table, jazz_node_CurrentRowVec rows, groove_GraphBuilder **out);
static jz_err jazz_node__node__query_eval__read_sources__inline_current_graph_with_source_metadata(const jazz_model_TableSchema *table, jazz_node_CurrentRowVec rows, SchemaVersionAlias alias, const char *coverage,
    const jazz_engine_SourceRequirements *requirements, groove_GraphBuilder **out_graph, groove_RecordDescriptor *out_desc, jz_btree *out_meta);
static jz_err jazz_node__node__query_eval__read_sources__inline_current_graph_with_source_metadata_and_branch_witness(const jazz_model_TableSchema *table, jazz_node_CurrentRowVec rows, SchemaVersionAlias alias, const char *coverage,
    const jazz_engine_SourceRequirements *requirements, const char *witness_field, const jazz_model_BranchKey *witness_key,
    groove_GraphBuilder **out_graph, groove_RecordDescriptor *out_desc, jz_btree *out_meta);
static jz_err jazz_node__node__query_eval__read_sources__inline_current_record_for_output(const jazz_model_TableSchema *table, groove_RecordDescriptor output, const jazz_node_CurrentRow *row, SchemaVersionAlias alias,
    const char *coverage, const jazz_engine_SourceRequirements *requirements, jz_bytes *out);
typedef struct { jazz_node_CurrentRow row; bool deleted; } RowDeleted;
typedef JZ_VEC(RowDeleted) RowDeletedVec;
static jz_err jazz_node__node__query_eval__read_sources__inline_snapshot_include_deleted_current_graph_with_source_metadata(const jazz_model_TableSchema *table, RowDeletedVec rows, SchemaVersionAlias alias,
    const char *coverage, const jazz_engine_SourceRequirements *requirements, groove_GraphBuilder **out_graph, groove_RecordDescriptor *out_desc, jz_btree *out_meta);
static groove_RecordDescriptor jazz_node__node__query_eval__read_sources__include_deleted_current_row_descriptor(const jazz_model_TableSchema *table);
static jz_err jazz_node__node__query_eval__read_sources__include_deleted_branch_graph(const jazz_model_TableSchema *table, const jazz_model_BranchKey *head, groove_GraphBuilder *content, groove_GraphBuilder *deletions, groove_GraphBuilder **out);
static groove_GraphBuilder *jazz_node__node__query_eval__read_sources__include_deleted_current_graph(const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier);
static groove_GraphBuilder *jazz_node__node__query_eval__read_sources__include_deleted_from_images(const jazz_model_TableSchema *table, groove_GraphBuilder *current);
static groove_GraphBuilder *jazz_node__node__query_eval__read_sources__deletion_register_current_source_graph(jz_slice table, jazz_model_DurabilityTier tier);
static groove_GraphBuilder *jazz_node__node__query_eval__read_sources__content_version_current_source_graph(const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, bool include_global_time);
static groove_GraphBuilder *jazz_node__node__query_eval__read_sources__selected_visible_current_primary_key_graph(const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, groove_ValueVec prefix);
static jz_err jazz_node__node__query_eval__read_sources__lookup_by_row_id(groove_GraphBuilder *rows, groove_GraphBuilder *row_ids, groove_GraphBuilder **out);
static bool jazz_node__node__query_eval__read_sources__source_in_recursive_step(const SReq *request);
static groove_StaticScanSpec jazz_node__node__query_eval__read_sources__row_prefix_scan(const groove_ValueVec *prefix);
jz_err jazz_node__node__query_eval__read_sources__NodeState__physical_global_current_source_for_index_scan(const jazz_node_NodeState *self, const jazz_model_TableSchema *table, jazz_types_SchemaVersionId schema_version,
    jz_slice column, const jz_str *order_column, bool reverse, const groove_ValueVec *prefix, const jazz_node_IndexIntersectionVec *intersections, bool maintained,
    bool has_source_limit, u32 source_limit, const jazz_node_CurrentIndexCandidateFilter *candidate_filter, jz_slice projection_target, groove_GraphBuilder **out);
jz_err jazz_node__node__query_eval__read_sources__NodeState__physical_global_current_source_for_index_scan_with_output(const jazz_node_NodeState *self, const jazz_model_TableSchema *table, jazz_types_SchemaVersionId schema_version,
    jz_slice column, const jz_str *order_column, bool reverse, const groove_ValueVec *prefix, const jazz_node_IndexIntersectionVec *intersections, bool maintained,
    bool has_source_limit, u32 source_limit, const jazz_node_CurrentIndexCandidateFilter *candidate_filter, jz_slice projection_target, groove_RecordDescriptor output, groove_GraphBuilder **out);
jz_err jazz_node__node__query_eval__read_sources__NodeState__physical_current_index_prefix_rows_up_to(const jazz_node_NodeState *self, jazz_node_PhysicalCurrentClass cls, const jazz_model_TableSchema *table, jazz_types_SchemaVersionId schema_version,
    const jazz_model_BranchKey *branch_key, jz_slice column, const jz_str *order_column, const groove_ValueVec *prefix, u32 max_items, u32 *out);
jz_err jazz_node__node__query_eval__read_sources__NodeState__physical_current_source_for_index_scan_in(const jazz_node_NodeState *self, jazz_node_PhysicalCurrentClass cls, const jazz_model_TableSchema *table, jazz_types_SchemaVersionId schema_version,
    const jazz_model_BranchKey *branch_key, jz_slice column, const jz_str *order_column, bool reverse, const groove_ValueVec *prefix, const jazz_node_IndexIntersectionVec *intersections, bool maintained,
    bool has_source_limit, u32 source_limit, const jazz_node_CurrentIndexCandidateFilter *candidate_filter, jz_slice projection_target, groove_GraphBuilder **out);

/* preparer methods */
static bool jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__local_exclusion_scope(const Prep *self, const SReq *request, jazz_protocol_PolicyBindingKey *out);
static bool jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__excludes_below_pending(const Prep *self, const SReq *request);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__exclude_settled_arm(Prep *self, const SReq *request, groove_GraphBuilder *graph, bool pending_ahead, groove_GraphBuilder **out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__stored_column_ids_for_read_table(const Prep *self, const SReq *request, const jazz_model_TableSchema *table, jz_btree *out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__prepare_source_graph_dispatch(Prep *self, const SReq *request, jazz_engine_ResolvedSource *out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__prepare_unprojected_visible_current_source_graph(Prep *self, const SReq *request, jazz_model_DurabilityTier tier, jazz_engine_ResolvedSource *out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__prepare_projected_visible_current_source_graph(Prep *self, const SReq *request, jazz_model_DurabilityTier tier, jazz_engine_ResolvedSource *out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__prepare_inline_visible_current_source_graph(Prep *self, const SReq *request, jazz_engine_ResolvedSource *out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__overlay_transaction_writes(Prep *self, const SReq *request, jazz_engine_ResolvedSource *resolved);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__prepare_source_graph_without_local_exclusions(Prep *self, const SReq *request, jazz_engine_ResolvedSource *out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__selected_global_current_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, groove_GraphBuilder **out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__pending_local_current_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *table, groove_GraphBuilder **out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__visible_current_physical_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_node_PhysicalCurrentClass cls, jazz_model_DurabilityTier deletion_tier, groove_GraphBuilder **out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__cached_policy_authorization_access_path(Prep *self, const SReq *request, bool *some, jazz_node_CurrentAccessPath *out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__selected_global_current_source_graph_for_access_path(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, jazz_node_CurrentAccessPath access_path, groove_GraphBuilder **out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__pending_deletion_winner_graph(Prep *self, const SReq *request, groove_GraphBuilder **out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__deletion_register_source_for_request(Prep *self, const SReq *request, const jazz_model_TableSchema *table, bool has_tier, jazz_model_DurabilityTier tier,
    bool has_history, jazz_types_GlobalTime history_position, bool has_open_tx, jazz_model_OpenTransactionId open_tx, bool *some, jazz_engine_DeletionRegisterSource *out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__content_version_source_for_request(Prep *self, const SReq *request, const jazz_model_TableSchema *table, bool has_tier, jazz_model_DurabilityTier tier,
    bool has_history, jazz_types_GlobalTime history_position, bool has_open_tx, jazz_model_OpenTransactionId open_tx, bool *some, jazz_engine_ContentVersionSource *out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_historical_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_types_GlobalTime position, jazz_node_CurrentSourceGraph *out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_maintained_visible_current_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, jazz_node_CurrentSourceGraph *out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__live_branch_view_registers(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier,
    const jazz_model_BranchKey *head, const jazz_model_BranchKey *base, groove_GraphBuilder **content, groove_GraphBuilder **deletions);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_branch_content_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier,
    const jz_btree *stored_keys, const jazz_node_BranchLayerAccess *access, groove_GraphBuilder **out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_branch_deletion_source_graph(Prep *self, const SReq *request, jazz_model_DurabilityTier tier,
    const jz_btree *stored_keys, const jazz_node_BranchLayerAccess *access, const groove_GraphBuilder *row_ids, groove_GraphBuilder **out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_content_current_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *read_table, jazz_model_DurabilityTier tier,
    bool include_global_time, bool exclude_deleted, groove_GraphBuilder **out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_deletion_register_current_source_graph(Prep *self, const SReq *request, jazz_model_DurabilityTier tier, groove_GraphBuilder **out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_visible_current_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, jazz_node_CurrentSourceGraph *out);
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_include_deleted_current_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, groove_GraphBuilder **out);

/* ---------- local exclusions ---------- */
static bool jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__local_exclusion_scope(const Prep *self, const SReq *request, jazz_protocol_PolicyBindingKey *out) {
    if (request->authorization.tag == JAZZ_SAR_POLICY_PROOF) return false;
    const jazz_engine_SourceExpr *e = jz_btree_get(&self->read_view->sources, &request->source);
    if (!e || !jazz_node__node__query_eval__unavailable_inputs__is_current_app_source(e)) return false;
    if (!self->has_local_unavailable_scope) return false;
    if (out) jazz_protocol_PolicyBindingKey_clone(out, &self->local_unavailable_scope);
    return true;
}

static bool jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__excludes_below_pending(const Prep *self, const SReq *request) {
    if (!jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__local_exclusion_scope(self, request, NULL)) return false;
    if (jz_btree_contains(&self->inline_sources, &request->source)) return false;
    const jazz_engine_SourceExpr *e = jz_btree_get(&self->read_view->sources, &request->source);
    if (!e) return false;
    switch (e->tag) {
    case JAZZ_SE_VISIBLE_CURRENT: return e->tier == JAZZ_TIER_LOCAL;
    case JAZZ_SE_WITH_OVERLAYS: return e->overlays.entries.len == 1 && e->overlays.entries.ptr[0].tag == JAZZ_OVL_PENDING_LOCAL;
    default: return false;
    }
}

static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__exclude_settled_arm(Prep *self, const SReq *request, groove_GraphBuilder *graph, bool pending_ahead, groove_GraphBuilder **out) {
    if (!jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__excludes_below_pending(self, request)) { *out = graph; return NULL; }
    jazz_protocol_PolicyBindingKey scope;
    if (!jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__local_exclusion_scope(self, request, &scope)) jz_panic("checked scope");
    jz_err e = jazz_node__node__query_eval__unavailable_inputs__NodeState__exclude_local_unavailable_graph(self->node, &scope, self->read_view->read_schema, request, graph, pending_ahead, out);
    jazz_protocol_PolicyBindingKey_drop(&scope);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return sre(request, JAZZ_GAP_LOCAL_AVAILABILITY_INPUT); }
    return NULL;
}

static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__stored_column_ids_for_read_table(const Prep *self, const SReq *request, const jazz_model_TableSchema *table, jz_btree *out) {
    const jazz_node_PhysicalMapping *mapping = jz_btree_get(&self->node->catalogue.physical_mappings, &self->read_view->read_schema);
    if (!mapping) return sre(request, JAZZ_GAP_SCHEMA_PROJECTION);
    const jazz_node_PhysicalTableMapping *table_mapping = jz_btree_get(&mapping->tables, &table->name);
    if (!table_mapping) return sre(request, JAZZ_GAP_SCHEMA_PROJECTION);
    jz_btree_init(out, &column_ids_desc);
    for (u32 i = 0; i < table->columns.len; i++) {
        const jazz_model_ColumnSchema *column = &table->columns.ptr[i];
        const jazz_types_PhysicalColumnId *id = jz_btree_get(&table_mapping->columns, &column->name);
        if (!id) { jz_btree_drop(out); return sre(request, JAZZ_GAP_SCHEMA_PROJECTION); }
        jz_str name = jz_str_clone(&column->name);
        jz_btree_insert(out, &name, id, NULL);
    }
    return NULL;
}

/* ---------- shared bits of the source families ---------- */
static bool projection_is_canonical(const jazz_engine_SchemaProjection *p) {
    return p->schema_family == JAZZ_SFS_CURRENT
        && (p->storage == JAZZ_SSS_SINGLE || p->storage == JAZZ_SSS_COMPATIBLE)
        && p->lens == JAZZ_LENS_CANONICAL;
}
/* Policy-proof dependencies are raw evidence for the outer policy: re-applying
 * their own read policy recursively both changes the outer predicate's meaning
 * and can manufacture a proof cycle. */
static void effective_authorization(const SReq *request, jazz_engine_SourceAuthorizationRequest *out) {
    if (request->authorization.tag == JAZZ_SAR_POLICY_PROOF) { memset(out, 0, sizeof *out); out->tag = JAZZ_SAR_SYSTEM; }
    else jazz_engine_SourceAuthorizationRequest_clone(out, &request->authorization);
}
static bool plan_matches_read_row(const jazz_engine_PolicyAuthorizationPlan *plan, const jazz_model_TableSchema *table) {
    return jz_str_eq(&plan->protected_source.table, &table->name) && plan->role == JAZZ_PDR_READ && jz_str_eq_cstr(&plan->protected_row_field, "row_uuid");
}
static jazz_node_ParamBindingMode plan_param_binding_mode(const jazz_engine_PolicyAuthorizationPlan *plan) {
    return plan->has_binding_source_shape ? JAZZ_PBM_RETAIN_ALL_PARAMS : JAZZ_PBM_INLINE_ALL_REACHABLE_SEEDS;
}
static jz_str opt_shape_clone(const jazz_engine_PolicyAuthorizationPlan *plan) {
    jz_str s = {0}; if (plan->has_binding_source_shape) s = jz_str_clone(&plan->binding_source_shape); return s;
}
static jz_btree btree_clone_plain(const jz_btree *src) { jz_btree d; jz_btree_clone(&d, src, NULL, NULL); return d; }
/* policy_request.map(|mut request| { request.reads.primary = projected_through(..); request }) */
static void project_policy_request(jz_err e, jazz_engine_QueryProgramRequest *request, const jazz_engine_ReadView *read_view) {
    if (JZ_IS_ERR(e)) return;
    jazz_engine_ReadView projected = jazz_node__node__query_eval__read_sources__policy_read_view_projected_through(&request->reads.primary, read_view);
    jazz_engine_ReadView_drop(&request->reads.primary);
    request->reads.primary = projected;
}
static jz_err table_read_policy_request(Prep *self, const jazz_model_TableSchema *table, const jazz_engine_SourceAuthorizationRequest *auth, jazz_model_DurabilityTier tier, jazz_engine_QueryProgramRequest *out) {
    const jazz_engine_PolicyAuthorizationPlan *plan = &auth->plan;
    return jazz_node__node__query_eval__authorization__NodeState__table_read_policy_authorization_request(
        self->node, self->read_view->policy_schema, SL(table->name), auth->permission_subject, plan_param_binding_mode(plan), tier,
        plan->has_binding_source_shape, opt_shape_clone(plan), btree_clone_plain(&plan->binding_user_params), btree_clone_plain(&plan->binding_claim_params), out);
}
static jz_err table_read_policy_request_for_include_deleted(Prep *self, const jazz_model_TableSchema *table, const jazz_engine_SourceAuthorizationRequest *auth, jazz_model_DurabilityTier tier, jazz_engine_QueryProgramRequest *out) {
    const jazz_engine_PolicyAuthorizationPlan *plan = &auth->plan;
    return jazz_node__node__query_eval__authorization__NodeState__table_read_policy_authorization_request_for_include_deleted(
        self->node, self->read_view->policy_schema, SL(table->name), auth->permission_subject, tier,
        plan->has_binding_source_shape, opt_shape_clone(plan), btree_clone_plain(&plan->binding_user_params), btree_clone_plain(&plan->binding_claim_params), out);
}
/* compose_policy_filtered_current_source_graph(..).map_err(from_policy_proof)? */
static jz_sre compose_filtered(Prep *self, const SReq *request, jz_err policy_err, jazz_engine_QueryProgramRequest *policy_request, groove_GraphBuilder *base,
    const jz_vec_str *output_fields, groove_GraphBuilder **graph, jz_btree *route_fields) {
    jazz_node_PolicyAuthorizationGraph filtered;
    jz_err e = jazz_node__node__query_eval__authorization__NodeState__compose_policy_filtered_current_source_graph(
        self->node, policy_err, policy_request, base, output_fields->ptr, output_fields->len, &filtered);
    if (JZ_IS_ERR(e)) return jazz_node__node__query_eval__read_sources__source_resolution_error_from_policy_proof(request, e);
    *graph = filtered.graph;
    if (route_fields) *route_fields = filtered.route_fields; else jz_btree_drop(&filtered.route_fields);
    jz_btree_drop(&filtered.access_paths);
    return NULL;
}
static jazz_model_TableSchema *table_arc(jazz_model_TableSchema table) {
    jazz_model_TableSchema *t = jz_rc_new(sizeof *t, jazz_model_TableSchema_drop);
    *t = table;
    return t;
}
static jazz_model_TableSchema *table_arc_clone(const jazz_model_TableSchema *table) {
    jazz_model_TableSchema *t = jz_rc_new(sizeof *t, jazz_model_TableSchema_drop);
    jazz_model_TableSchema_clone(t, table);
    return t;
}
/* ResolvedSource { stored_column_ids: self.stored_column_ids_for_read_table(request, &table)?, ... } */
static jz_sre make_resolved(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_model_TableSchema *arc, groove_GraphBuilder *graph,
    groove_RecordDescriptor descriptor, jz_btree metadata, jz_btree routing_fields, bool requires_result_payload,
    bool has_cv, jazz_engine_ContentVersionSource cv, bool has_dr, jazz_engine_DeletionRegisterSource dr, jazz_engine_ResolvedSource *out) {
    memset(out, 0, sizeof *out);
    jz_sre s = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__stored_column_ids_for_read_table(self, request, table, &out->stored_column_ids);
    if (s) {
        jz_rc_release(arc); groove_GraphBuilder_free(graph); jz_btree_drop(&metadata); jz_btree_drop(&routing_fields);
        if (has_cv) { groove_GraphBuilder_free(cv.graph); jz_str_drop(&cv.row_uuid_field); }
        if (has_dr) { groove_GraphBuilder_free(dr.graph); jz_str_drop(&dr.row_uuid_field); }
        return s;
    }
    out->table_schema = arc;
    out->graph = graph;
    jazz_engine_SourceId_clone(&out->row_shape.source, &request->source);
    out->row_shape.descriptor = descriptor;
    out->row_shape.row_uuid_field = jz_str_from("row_uuid");
    out->row_shape.metadata = metadata;
    out->routing_fields = routing_fields;
    out->requires_result_payload = requires_result_payload;
    out->has_content_version = has_cv; if (has_cv) out->content_version = cv;
    out->has_deletion_register = has_dr; if (has_dr) out->deletion_register = dr;
    out->has_authorized_deletion_preimage = false;
    return NULL;
}
static jz_btree empty_metadata(void) { jz_btree m; jz_btree_init(&m, &jazz_engine_MetadataFields_desc); return m; }
static jz_btree empty_strset(void) { jz_btree m; jz_btree_init(&m, &jazz_node_StrSet_desc); return m; }
static const jazz_engine_ContentVersionSource NO_CV;
static const jazz_engine_DeletionRegisterSource NO_DR;
static bool requirements_need_content_version(const jazz_engine_SourceRequirements *r) {
    jazz_engine_SourceMetadataRequirement *k; void *v;
    JZ_BTREE_FOR(&r->metadata, k, v) {
        if (k->tag == JAZZ_SMR_VERSION_PAYLOADS || k->tag == JAZZ_SMR_VERSION_WITNESSES || k->tag == JAZZ_SMR_PROVENANCE) return true;
    }
    return false;
}
static bool requirements_contains(const jazz_engine_SourceRequirements *r, u32 tag) {
    jazz_engine_SourceMetadataRequirement key; memset(&key, 0, sizeof key); key.tag = tag;
    return jz_btree_contains(&r->metadata, &key);
}
/* maintained_view_history_storage_field_names(table).map(named).chain(once(renamed("branch_key","supplying_branch_key"))) */
static groove_ProjectFieldVec history_fields_with_supplying_branch(const jazz_model_TableSchema *table) {
    jz_vec_str names = jazz_node__node__query_eval__read_sources__maintained_view_history_storage_field_names(table);
    groove_ProjectFieldVec fields = {0};
    for (u32 i = 0; i < names.len; i++) { groove_ProjectField *f = groove__ivm__graph__ProjectField__named(SL(names.ptr[i])); JZ_VEC_PUSH(fields, f); }
    groove_ProjectField *b = groove__ivm__graph__ProjectField__renamed(CS("branch_key"), CS("supplying_branch_key"));
    JZ_VEC_PUSH(fields, b);
    strvec_drop(&names);
    return fields;
}
static groove_ProjectFieldVec register_fields_with_supplying_branch(void) {
    jz_vec_str names = jazz_node__node__query_eval__read_sources__register_storage_field_names();
    groove_ProjectFieldVec fields = {0};
    for (u32 i = 0; i < names.len; i++) { groove_ProjectField *f = groove__ivm__graph__ProjectField__named(SL(names.ptr[i])); JZ_VEC_PUSH(fields, f); }
    groove_ProjectField *b = groove__ivm__graph__ProjectField__renamed(CS("branch_key"), CS("supplying_branch_key"));
    JZ_VEC_PUSH(fields, b);
    strvec_drop(&names);
    return fields;
}
static const jz_str ROW_UUID_ARR[1] = { { (char *)"row_uuid", 8, 0 } };
static const jz_str TX_ORDER_ARR[2] = { { (char *)"tx_time", 7, 0 }, { (char *)"tx_node_id", 10, 0 } };
static groove_GraphBuilder *project_row_uuid(groove_GraphBuilder *g) { return groove__ivm__graph__GraphBuilder__project(g, ROW_UUID_ARR, 1); }
static groove_GraphBuilder *anti_join_row_uuid(groove_GraphBuilder *l, groove_GraphBuilder *r) { return groove__ivm__graph__GraphBuilder__anti_join(l, r, ROW_UUID_ARR, 1, ROW_UUID_ARR, 1); }
static groove_GraphBuilder *semi_join_row_uuid(groove_GraphBuilder *l, groove_GraphBuilder *r) { return groove__ivm__graph__GraphBuilder__semi_join(l, r, ROW_UUID_ARR, 1, ROW_UUID_ARR, 1); }
static groove_GraphBuilder *arg_max_by_tx(groove_GraphBuilder *g) { return groove__ivm__graph__GraphBuilder__arg_max_by(g, ROW_UUID_ARR, 1, TX_ORDER_ARR, 2); }
static groove_GraphBuilder *union2(groove_GraphBuilder *a, groove_GraphBuilder *b) { groove_GraphBuilder *in[2] = { a, b }; return groove__ivm__graph__GraphBuilder__union(in, 2); }
static groove_PredicateExpr *deletion_is(u8 tag) {
    groove_Value v; v.tag = GROOVE_V_ENUM_TAG; v.v.u = tag;
    return groove__ivm__op_types__PredicateExpr__eq(CS("_deletion"), groove__ivm__op_types__LiteralValue__from_value(v));
}
static void covered_trace_lookup(const Prep *self, const SReq *request, bool matched) {
    jz_str line = jz_format("JAZZ_COVERED_INPUT_TRACE stage=covered_input_lookup request=");
    jazz_engine_SourceId_debug(&line, &request->source);
    jz_str_appendf(&line, " matched=%s candidates=[", matched ? "true" : "false");
    jazz_engine_SourceId *k; groove_GraphBuilder **g; bool first = true;
    JZ_BTREE_FOR(&self->covered_input_sources, k, g) {
        if (!first) jz_str_push_cstr(&line, ", ");
        first = false;
        jazz_engine_SourceId_debug(&line, k);
    }
    jz_str_push_char(&line, ']');
    jazz_types__debug_env__eprint(line);
}

/* ======================================================================
 * The complete source-family dispatcher. Keep uncommon historical and
 * branch paths out of the ordinary inline policy-evaluation frame.
 * ====================================================================== */
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__prepare_source_graph_dispatch(Prep *self, const SReq *request, jazz_engine_ResolvedSource *out) {
    const jazz_engine_SourceExpr *source = jz_btree_get(&self->read_view->sources, &request->source);
    if (!source) return sre(request, JAZZ_GAP_COVERAGE);
    /* The overlay is explicit in the source expression; Local-first is
     * ordinary local storage, never an implicit authority-scoped overlay. */
    jazz_engine_SourceExpr pending_current; bool has_pending_current = false;
    if (source->tag == JAZZ_SE_WITH_OVERLAYS && source->overlays.entries.len == 1 && source->overlays.entries.ptr[0].tag == JAZZ_OVL_PENDING_LOCAL) {
        if (source->input->tag != JAZZ_SE_SETTLED_BINDING_VIEW) return sre(request, JAZZ_GAP_COVERAGE);
        memset(&pending_current, 0, sizeof pending_current);
        pending_current.tag = JAZZ_SE_VISIBLE_CURRENT;
        jazz_engine_SchemaProjection_clone(&pending_current.projection, &source->input->projection);
        pending_current.data.tag = JAZZ_DS_CURRENT;
        pending_current.tier = JAZZ_TIER_LOCAL;
        has_pending_current = true;
    }
    bool pending_overlay = has_pending_current;
    if (has_pending_current) source = &pending_current;
    groove_GraphBuilder *covered_input_source = NULL;
    bool has_covered_descriptor = false; groove_RecordDescriptor covered_input_descriptor = NULL;
    if (request->visibility == JAZZ_VIS_VISIBLE) {
        groove_GraphBuilder **g = jz_btree_get(&self->covered_input_sources, &request->source);
        if (g) covered_input_source = groove_GraphBuilder_clone(*g);
        groove_RecordDescriptor *d = jz_btree_get(&self->covered_input_descriptors, &request->source);
        if (d) { has_covered_descriptor = true; covered_input_descriptor = *d; }
    }
    jz_sre err = NULL;
    if ((covered_input_source != NULL) != has_covered_descriptor) { err = sre(request, JAZZ_GAP_COVERAGE); goto fail; }

    /* A receiver-local Local source has exactly two possible inputs: the
     * authority-covered frontier and this node's still-pending Ahead overlay. */
    bool receiver_local_overlay = covered_input_source != NULL && pending_overlay;
    if (jazz_types__debug_env__covered_input_trace() && self->covered_input_sources.len != 0)
        covered_trace_lookup(self, request, covered_input_source != NULL);
    if (covered_input_source
        && (source->tag == JAZZ_SE_SETTLED_BINDING_VIEW
            || (source->tag == JAZZ_SE_VISIBLE_CURRENT && source->tier >= JAZZ_TIER_GLOBAL))) {
        /* The compiler created this source map only for an exact
         * authority-covered occurrence. */
        if (request->visibility != JAZZ_VIS_VISIBLE) { err = sre(request, JAZZ_GAP_COVERAGE); goto fail; }
        jazz_model_TableSchema table;
        jz_err e = jazz_node__node__NodeState__table_in_schema(self->node, SL(request->source.table), self->read_view->read_schema, &table);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_SCHEMA_PROJECTION); goto fail; }
        jz_btree metadata = jazz_node__node__query_eval__read_sources__covered_input_source_metadata(&request->requirements, &table);
        if (!has_covered_descriptor) jz_panic("checked alongside compiler-owned covered input source");
        groove_RecordDescriptor descriptor = covered_input_descriptor;
        jazz_model_TableSchema *arc = table_arc_clone(&table);
        err = make_resolved(self, request, &table, arc, covered_input_source, descriptor, metadata, empty_strset(), false, false, NO_CV, false, NO_DR, out);
        jazz_model_TableSchema_drop(&table);
        if (has_pending_current) jazz_engine_SourceExpr_drop(&pending_current);
        return err;
    }
    const jazz_engine_SchemaProjection *projection;
    bool has_graph_tier = false; jazz_model_DurabilityTier graph_tier = JAZZ_TIER_NONE;
    bool has_history = false; jazz_types_GlobalTime history_position = {0};
    bool has_snapshot = false; jazz_model_Snapshot snapshot = {0};
    bool has_open_tx = false; jazz_model_OpenTransactionId open_tx_overlay = {0};
    bool has_branch_view = false; const jazz_model_BranchKey *bv_head = NULL; const jazz_engine_BranchViewSourceBase *bv_base = NULL;
    switch (source->tag) {
    case JAZZ_SE_VISIBLE_CURRENT:
        if (source->data.tag != JAZZ_DS_CURRENT) { err = sre(request, JAZZ_GAP_HISTORICAL_STORAGE_CUT); goto fail; }
        projection = &source->projection; has_graph_tier = true; graph_tier = source->tier;
        break;
    case JAZZ_SE_BRANCH_VIEW:
        projection = &source->projection; has_graph_tier = true; graph_tier = source->tier;
        has_branch_view = true; bv_head = &source->head; bv_base = source->base;
        break;
    case JAZZ_SE_HISTORY_CUT:
        if (source->data.tag != JAZZ_DS_CURRENT) { err = sre(request, JAZZ_GAP_HISTORICAL_STORAGE_CUT); goto fail; }
        projection = &source->projection; has_history = true; history_position = source->position;
        break;
    case JAZZ_SE_SNAPSHOT_REF:
        if (source->data.tag != JAZZ_DS_CURRENT) { err = sre(request, JAZZ_GAP_HISTORICAL_STORAGE_CUT); goto fail; }
        projection = &source->projection; has_snapshot = true; snapshot.bytes = jz_bytes_clone(&source->snapshot.bytes);
        break;
    case JAZZ_SE_SETTLED_BINDING_VIEW:
        /* A settled authority binding is not a source of application rows. */
        err = sre(request, JAZZ_GAP_COVERAGE); goto fail;
    case JAZZ_SE_WITH_OVERLAYS: {
        const jazz_engine_SourceExpr *input = source->input;
        if (input->tag == JAZZ_SE_VISIBLE_CURRENT && input->data.tag == JAZZ_DS_CURRENT) {
            projection = &input->projection; has_graph_tier = true; graph_tier = input->tier;
        } else if (input->tag == JAZZ_SE_SNAPSHOT_REF && input->data.tag == JAZZ_DS_CURRENT) {
            projection = &input->projection;
        } else { err = sre(request, JAZZ_GAP_TRANSACTION_READ_OVERLAY); goto fail; }
        if (source->overlays.entries.len != 1 || source->overlays.entries.ptr[0].tag != JAZZ_OVL_OPEN_TX) { err = sre(request, JAZZ_GAP_TRANSACTION_READ_OVERLAY); goto fail; }
        has_open_tx = true; open_tx_overlay = source->overlays.entries.ptr[0].open_tx;
        break;
    }
    default:
        err = sre(request, JAZZ_GAP_HISTORICAL_STORAGE_CUT); goto fail;
    }
    if (!projection_is_canonical(projection)) { err = sre(request, JAZZ_GAP_SCHEMA_PROJECTION); goto fail; }
    jazz_model_TableSchema table;
    {
        jz_err e = jazz_node__node__NodeState__table_in_schema(self->node, SL(request->source.table), self->read_view->read_schema, &table);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_SCHEMA_PROJECTION); goto fail; }
    }
    jazz_engine_SourceAuthorizationRequest authorization;
    effective_authorization(request, &authorization);
    jazz_node_CurrentRowVec *inline_rows = jz_btree_get(&self->inline_sources, &request->source);
    if (inline_rows) {
        if (request->visibility != JAZZ_VIS_VISIBLE || authorization.tag != JAZZ_SAR_SYSTEM) { err = sre(request, JAZZ_GAP_COVERAGE); goto fail_table; }
        SchemaVersionAlias alias;
        jz_err e = jazz_node__node__NodeState__ensure_schema_version_alias(self->node, self->read_view->read_schema, &alias);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_COVERAGE); goto fail_table; }
        jazz_node_CurrentRowVec rows; jazz_node_CurrentRowVec_clone(&rows, inline_rows);
        groove_GraphBuilder *graph; groove_RecordDescriptor descriptor; jz_btree metadata;
        e = jazz_node__node__query_eval__read_sources__inline_current_graph_with_source_metadata(&table, rows, alias, "inline-current", &request->requirements, &graph, &descriptor, &metadata);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_COVERAGE); goto fail_table; }
        err = make_resolved(self, request, &table, table_arc_clone(&table), graph, descriptor, metadata, empty_strset(), false, false, NO_CV, false, NO_DR, out);
        goto done_table;
    }
    bool snapshot_source = has_snapshot;
    bool has_snapshot_cv = false; jazz_engine_ContentVersionSource snapshot_content_version = {0};
    groove_GraphBuilder *graph = NULL; groove_RecordDescriptor descriptor = NULL; jz_btree metadata; jz_btree routing_fields;
    if (has_branch_view) {
        const jazz_model_BranchKey *head = bv_head; const jazz_engine_BranchViewSourceBase *base = bv_base;
        bool base_is_none_or_current = base == NULL || base->tag == JAZZ_BVB_CURRENT;
        if (request->visibility == JAZZ_VIS_INCLUDE_DELETED && base_is_none_or_current) {
            if (!has_graph_tier) jz_panic("branch view has a current tier");
            jazz_model_DurabilityTier tier = graph_tier;
            /* A head-over-current BranchView selects its winner from the head
             * when present and otherwise from the base. */
            const jazz_model_BranchKey *live_base = NULL;
            if (base && base->tag == JAZZ_BVB_CURRENT && !jazz_model_BranchKey_eq(&base->branch, head)) live_base = &base->branch;
            groove_GraphBuilder *content, *deletions;
            err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__live_branch_view_registers(self, request, &table, tier, head, live_base, &content, &deletions);
            if (err) goto fail_auth;
            groove_GraphBuilder *bgraph;
            jz_err e = jazz_node__node__query_eval__read_sources__include_deleted_branch_graph(&table, head, content, deletions, &bgraph);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_COVERAGE); goto fail_auth; }
            switch (authorization.tag) {
            case JAZZ_SAR_SYSTEM: graph = bgraph; break;
            case JAZZ_SAR_POLICY_FILTERED: case JAZZ_SAR_POLICY_PROOF: {
                jazz_engine_QueryProgramRequest policy_request;
                jz_err pe = table_read_policy_request_for_include_deleted(self, &table, &authorization, tier, &policy_request);
                project_policy_request(pe, &policy_request, self->read_view);
                jz_vec_str output_fields = jazz_node__node__query_eval__read_sources__current_row_fields(&table);
                strvec_push_cstr(&output_fields, "__jazz_deleted");
                err = compose_filtered(self, request, pe, &policy_request, bgraph, &output_fields, &graph, NULL);
                strvec_drop(&output_fields);
                if (err) goto fail_auth;
                break;
            }
            }
            groove_GraphBuilder *g2; groove_RecordDescriptor d2; jz_btree m2;
            e = jazz_node__node__query_eval__read_sources__with_requested_author_paths(graph, jazz_node__node__query_eval__read_sources__include_deleted_current_row_descriptor(&table), empty_metadata(), &request->requirements, &g2, &d2, &m2);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_COVERAGE); goto fail_auth; }
            err = make_resolved(self, request, &table, table_arc_clone(&table), g2, d2, m2, empty_strset(), false, false, NO_CV, false, NO_DR, out);
            goto done_auth;
        }
        if (request->visibility != JAZZ_VIS_VISIBLE) { err = sre(request, JAZZ_GAP_COVERAGE); goto fail_auth; }
        if (base_is_none_or_current) {
            const char *branch_witness_field = table.branch_by.len != 0 ? "supplying_branch_key" : NULL;
            const jazz_model_BranchKey *live_base = NULL;
            if (base && base->tag == JAZZ_BVB_CURRENT && !jazz_model_BranchKey_eq(&base->branch, head)) live_base = &base->branch;
            if (!has_graph_tier) jz_panic("branch view has a current tier");
            groove_GraphBuilder *content, *deletions;
            err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__live_branch_view_registers(self, request, &table, graph_tier, head, live_base, &content, &deletions);
            if (err) goto fail_auth;
            bool has_cv = false; jazz_engine_ContentVersionSource content_version = {0};
            if (requirements_need_content_version(&request->requirements)) {
                has_cv = true;
                content_version.graph = groove__ivm__graph__GraphBuilder__project_fields(groove_GraphBuilder_clone(content), history_fields_with_supplying_branch(&table));
                content_version.row_uuid_field = jz_str_from("row_uuid");
            }
            bool has_dr = false; jazz_engine_DeletionRegisterSource deletion_register = {0};
            if (requirements_contains(&request->requirements, JAZZ_SMR_DELETION_MARKERS)) {
                has_dr = true;
                deletion_register.graph = groove__ivm__graph__GraphBuilder__project_fields(groove_GraphBuilder_clone(deletions), register_fields_with_supplying_branch());
                deletion_register.row_uuid_field = jz_str_from("row_uuid");
            }
            groove_GraphBuilder *deleted = project_row_uuid(groove__ivm__graph__GraphBuilder__filter(deletions, deletion_is(0)));
            groove_ProjectFieldVec bfields;
            jz_err e = jazz_node__node__query_eval__read_sources__branch_view_storage_source_fields(&table, head, &bfields);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); groove_GraphBuilder_free(content); groove_GraphBuilder_free(deleted); err = sre(request, JAZZ_GAP_SCHEMA_PROJECTION); goto fail_auth; }
            groove_GraphBuilder *selected_base = groove__ivm__graph__GraphBuilder__project_fields(anti_join_row_uuid(content, deleted), bfields);
            groove_GraphBuilder *g2; groove_RecordDescriptor d2; jz_btree m2, r2;
            e = jazz_node__node__query_eval__read_sources__resolved_current_source_graph(self->node, &table, graph_tier, &request->requirements, &authorization,
                self->read_view->policy_schema, self->read_view, branch_witness_field, selected_base, &g2, &d2, &m2, &r2);
            if (JZ_IS_ERR(e)) { err = jazz_node__node__query_eval__read_sources__source_resolution_error_from_policy_proof(request, e); goto fail_auth; }
            err = make_resolved(self, request, &table, table_arc_clone(&table), g2, d2, m2, r2, true, has_cv, content_version, has_dr, deletion_register, out);
            goto done_auth;
        }
        if (base && base->tag == JAZZ_BVB_SNAPSHOT) {
            const char *branch_witness_field = table.branch_by.len != 0 ? "supplying_branch_key" : NULL;
            if (!has_graph_tier) jz_panic("branch view has a current tier");
            jazz_model_DurabilityTier tier = graph_tier;
            const jazz_model_BranchKey *frozen_base_key = &base->branch; const jazz_model_SnapshotRef *frozen_snapshot = &base->snapshot;
            jz_btree head_keys;
            jz_err e = jazz_node__node__source_resolution__NodeState__equivalent_stored_branch_keys(self->node, SL(request->source.table), self->read_view->read_schema, head, &head_keys);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_COVERAGE); goto fail_auth; }
            /* The frozen view stacks the live head over the snapshot base,
             * content and deletion registers separately (INV-BVIEW-9). */
            jazz_node_BranchLayerAccess full; memset(&full, 0, sizeof full); full.tag = JAZZ_BLA_FULL;
            groove_GraphBuilder *head_content, *head_deletions;
            err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_branch_content_source_graph(self, request, &table, tier, &head_keys, &full, &head_content);
            if (err) { jz_btree_drop(&head_keys); goto fail_auth; }
            err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_branch_deletion_source_graph(self, request, tier, &head_keys, &full, NULL, &head_deletions);
            jz_btree_drop(&head_keys);
            if (err) { groove_GraphBuilder_free(head_content); goto fail_auth; }
            jazz_node_Layer *head_content_layer = jazz_node__node__layer_stack__Layer__new(groove_GraphBuilder_clone(head_content));
            jazz_node_Layer *head_deletion_layer = jazz_node__node__layer_stack__Layer__new(groove_GraphBuilder_clone(head_deletions));
            groove_GraphBuilder *deleted = project_row_uuid(groove__ivm__graph__GraphBuilder__filter(groove_GraphBuilder_clone(head_deletions), deletion_is(0)));
            groove_GraphBuilder *restored = project_row_uuid(groove__ivm__graph__GraphBuilder__filter(head_deletions, deletion_is(1)));
            groove_ProjectFieldVec bfields;
            e = jazz_node__node__query_eval__read_sources__branch_view_storage_source_fields(&table, head, &bfields);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_SCHEMA_PROJECTION); goto frozen_fail; }
            groove_GraphBuilder *selected_head = groove__ivm__graph__GraphBuilder__project_fields(anti_join_row_uuid(groove_GraphBuilder_clone(head_content), groove_GraphBuilder_clone(deleted)), bfields);
            jazz_engine_SourceAuthorizationRequest system_authorization; memset(&system_authorization, 0, sizeof system_authorization); system_authorization.tag = JAZZ_SAR_SYSTEM;
            groove_GraphBuilder *live_head; groove_RecordDescriptor unused_desc; jz_btree fmeta, frouting;
            e = jazz_node__node__query_eval__read_sources__resolved_current_source_graph(self->node, &table, tier, &request->requirements, &system_authorization,
                self->read_view->policy_schema, self->read_view, branch_witness_field, selected_head, &live_head, &unused_desc, &fmeta, &frouting);
            if (JZ_IS_ERR(e)) { err = jazz_node__node__query_eval__read_sources__source_resolution_error_from_policy_proof(request, e); goto frozen_fail; }
            groove_RecordDescriptor fdesc = jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields_for_branch(&table, &fmeta, branch_witness_field != NULL);
            /* Capture only the base snapshot once. */
            jazz_node_CurrentRowVec frozen_base_rows, frozen_base_deleted_rows;
            e = jazz_node__node__source_resolution__NodeState__branch_snapshot_rows_for_schema(self->node, SL(request->source.table), self->read_view->read_schema, head, frozen_base_key, frozen_snapshot, &frozen_base_rows, &frozen_base_deleted_rows);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); groove_GraphBuilder_free(live_head); err = sre(request, JAZZ_GAP_COVERAGE); goto frozen_fail; }
            SchemaVersionAlias alias;
            e = jazz_node__node__NodeState__ensure_schema_version_alias(self->node, self->read_view->read_schema, &alias);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); groove_GraphBuilder_free(live_head); jazz_node_CurrentRowVec_drop(&frozen_base_rows); jazz_node_CurrentRowVec_drop(&frozen_base_deleted_rows); err = sre(request, JAZZ_GAP_COVERAGE); goto frozen_fail; }
            groove_GraphBuilder *opening; groove_RecordDescriptor opening_descriptor; jz_btree opening_metadata;
            e = jazz_node__node__query_eval__read_sources__inline_current_graph_with_source_metadata_and_branch_witness(&table, frozen_base_rows, alias, "frozen-branch-base", &request->requirements,
                branch_witness_field, frozen_base_key, &opening, &opening_descriptor, &opening_metadata);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); groove_GraphBuilder_free(live_head); jazz_node_CurrentRowVec_drop(&frozen_base_deleted_rows); err = sre(request, JAZZ_GAP_COVERAGE); goto frozen_fail; }
            if (fdesc != opening_descriptor || !jz_btree_eq(&fmeta, &opening_metadata, NULL)) {
                jz_btree_drop(&opening_metadata); groove_GraphBuilder_free(opening); groove_GraphBuilder_free(live_head); jazz_node_CurrentRowVec_drop(&frozen_base_deleted_rows);
                err = sre(request, JAZZ_GAP_SCHEMA_PROJECTION); goto frozen_fail;
            }
            jz_btree_drop(&opening_metadata);
            jazz_node_Layer *above_content[1] = { head_content_layer };
            groove_GraphBuilder *inherited = jazz_node__node__layer_stack__exposed(opening, above_content, 1);
            jazz_node_Layer *above_deletion[1] = { head_deletion_layer };
            /* A deletion-register winner is explicit for both states. */
            if (frozen_base_deleted_rows.len == 0) {
                jazz_node_CurrentRowVec_drop(&frozen_base_deleted_rows);
                inherited = union2(jazz_node__node__layer_stack__exposed(groove_GraphBuilder_clone(inherited), above_deletion, 1),
                                   semi_join_row_uuid(inherited, restored));
            } else {
                groove_GraphBuilder *frozen_base_deletions; groove_RecordDescriptor deletion_descriptor; jz_btree deletion_metadata;
                e = jazz_node__node__query_eval__read_sources__inline_current_graph_with_source_metadata_and_branch_witness(&table, frozen_base_deleted_rows, alias, "frozen-branch-base-deletions",
                    &request->requirements, branch_witness_field, frozen_base_key, &frozen_base_deletions, &deletion_descriptor, &deletion_metadata);
                if (JZ_IS_ERR(e)) { jz_err_drop(&e); groove_GraphBuilder_free(inherited); groove_GraphBuilder_free(live_head); restored = NULL; err = sre(request, JAZZ_GAP_COVERAGE); goto frozen_fail; }
                if (fdesc != deletion_descriptor || !jz_btree_eq(&fmeta, &deletion_metadata, NULL)) {
                    jz_btree_drop(&deletion_metadata); groove_GraphBuilder_free(frozen_base_deletions); groove_GraphBuilder_free(inherited); groove_GraphBuilder_free(live_head);
                    err = sre(request, JAZZ_GAP_SCHEMA_PROJECTION); goto frozen_fail;
                }
                jz_btree_drop(&deletion_metadata);
                groove_GraphBuilder *inherited_without_frozen_deletions = anti_join_row_uuid(groove_GraphBuilder_clone(inherited), project_row_uuid(frozen_base_deletions));
                /* A head Restored winner deliberately also reveals a base row
                 * whose frozen deletion winner was Deleted. */
                inherited = union2(jazz_node__node__layer_stack__exposed(inherited_without_frozen_deletions, above_deletion, 1),
                                   semi_join_row_uuid(inherited, restored));
            }
            restored = NULL;
            groove_GraphBuilder *unfiltered = union2(live_head, inherited);
            groove_GraphBuilder *fgraph = NULL;
            switch (authorization.tag) {
            case JAZZ_SAR_SYSTEM: fgraph = unfiltered; break;
            case JAZZ_SAR_POLICY_FILTERED: case JAZZ_SAR_POLICY_PROOF: {
                jazz_engine_QueryProgramRequest policy_request;
                jz_err pe = table_read_policy_request(self, &table, &authorization, tier, &policy_request);
                project_policy_request(pe, &policy_request, self->read_view);
                jz_vec_str output_fields = jazz_node__node__query_eval__read_sources__current_row_fields(&table);
                if (branch_witness_field) strvec_push_cstr(&output_fields, branch_witness_field);
                err = compose_filtered(self, request, pe, &policy_request, unfiltered, &output_fields, &fgraph, NULL);
                strvec_drop(&output_fields);
                if (err) { jz_btree_drop(&fmeta); jz_btree_drop(&frouting); goto frozen_fail; }
                break;
            }
            }
            bool has_cv = false; jazz_engine_ContentVersionSource cv = {0};
            if (requirements_need_content_version(&request->requirements)) {
                has_cv = true;
                cv.graph = groove__ivm__graph__GraphBuilder__project_fields(head_content, history_fields_with_supplying_branch(&table));
                cv.row_uuid_field = jz_str_from("row_uuid");
                head_content = NULL;
            }
            if (head_content) groove_GraphBuilder_free(head_content);
            groove_GraphBuilder_free(deleted);
            jazz_node_Layer_free(head_content_layer); jazz_node_Layer_free(head_deletion_layer);
            err = make_resolved(self, request, &table, table_arc_clone(&table), fgraph, fdesc, fmeta, frouting, true, has_cv, cv, false, NO_DR, out);
            goto done_auth;
        frozen_fail:
            if (head_content) groove_GraphBuilder_free(head_content);
            if (restored) groove_GraphBuilder_free(restored);
            groove_GraphBuilder_free(deleted);
            jazz_node_Layer_free(head_content_layer); jazz_node_Layer_free(head_deletion_layer);
            goto fail_auth;
        }
        if (!has_graph_tier) jz_panic("branch view has a current tier");
        jazz_node_CurrentRowVec rows;
        jz_err e = jazz_node__node__source_resolution__NodeState__branch_view_rows_for_schema(self->node, SL(request->source.table), self->read_view->read_schema, graph_tier, head, base, &rows);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_COVERAGE); goto fail_auth; }
        groove_GraphBuilder *base_graph;
        if (request->requirements.metadata.len == 0) {
            e = jazz_node__node__query_eval__read_sources__inline_current_graph(&table, rows, &base_graph);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_COVERAGE); goto fail_auth; }
            descriptor = jazz_node__node__query_eval__read_sources__current_row_descriptor(&table);
            metadata = empty_metadata();
        } else {
            SchemaVersionAlias alias;
            e = jazz_node__node__NodeState__ensure_schema_version_alias(self->node, self->read_view->read_schema, &alias);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); jazz_node_CurrentRowVec_drop(&rows); err = sre(request, JAZZ_GAP_COVERAGE); goto fail_auth; }
            e = jazz_node__node__query_eval__read_sources__inline_current_graph_with_source_metadata(&table, rows, alias, "branch-view", &request->requirements, &base_graph, &descriptor, &metadata);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_COVERAGE); goto fail_auth; }
        }
        switch (authorization.tag) {
        case JAZZ_SAR_SYSTEM: graph = base_graph; break;
        case JAZZ_SAR_POLICY_FILTERED: case JAZZ_SAR_POLICY_PROOF: {
            if (!plan_matches_read_row(&authorization.plan, &table)) { groove_GraphBuilder_free(base_graph); jz_btree_drop(&metadata); err = sre(request, JAZZ_GAP_COVERAGE); goto fail_auth; }
            jazz_engine_QueryProgramRequest policy_request;
            jz_err pe = table_read_policy_request(self, &table, &authorization, graph_tier, &policy_request);
            project_policy_request(pe, &policy_request, self->read_view);
            jz_vec_str output_fields = jazz_node__node__query_eval__read_sources__current_row_fields(&table);
            err = compose_filtered(self, request, pe, &policy_request, base_graph, &output_fields, &graph, NULL);
            strvec_drop(&output_fields);
            if (err) { jz_btree_drop(&metadata); goto fail_auth; }
            break;
        }
        }
        routing_fields = empty_strset();
    } else if (has_history) {
        if (request->visibility != JAZZ_VIS_VISIBLE) { err = sre(request, JAZZ_GAP_HISTORICAL_STORAGE_CUT); goto fail_auth; }
        jazz_node_CurrentSourceGraph hist;
        err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_historical_source_graph(self, request, &table, history_position, &hist);
        if (err) goto fail_auth;
        descriptor = hist.descriptor; metadata = hist.metadata;
        switch (authorization.tag) {
        case JAZZ_SAR_SYSTEM: graph = hist.graph; break;
        case JAZZ_SAR_POLICY_FILTERED: case JAZZ_SAR_POLICY_PROOF: {
            const jazz_engine_PolicyAuthorizationPlan *plan = &authorization.plan;
            if (!plan_matches_read_row(plan, &table)) { groove_GraphBuilder_free(hist.graph); jz_btree_drop(&metadata); err = sre(request, JAZZ_GAP_HISTORICAL_STORAGE_CUT); goto fail_auth; }
            jazz_engine_QueryProgramRequest policy_request;
            jz_err pe = jazz_node__node__query_eval__authorization__NodeState__table_read_policy_authorization_request_at(
                self->node, self->read_view->policy_schema, SL(table.name), authorization.permission_subject, JAZZ_PBM_INLINE_ALL_REACHABLE_SEEDS, history_position,
                plan->has_binding_source_shape, opt_shape_clone(plan), btree_clone_plain(&plan->binding_user_params), btree_clone_plain(&plan->binding_claim_params), &policy_request);
            jz_vec_str output_fields;
            jz_err fe = jazz_node__node__query_eval__lowering__descriptor_field_names(descriptor, &output_fields);
            if (JZ_IS_ERR(fe)) {
                jz_err_drop(&fe); if (!JZ_IS_ERR(pe)) jazz_engine_QueryProgramRequest_drop(&policy_request); else jz_err_drop(&pe);
                groove_GraphBuilder_free(hist.graph); jz_btree_drop(&metadata); err = sre(request, JAZZ_GAP_HISTORICAL_STORAGE_CUT); goto fail_auth;
            }
            err = compose_filtered(self, request, pe, &policy_request, hist.graph, &output_fields, &graph, NULL);
            strvec_drop(&output_fields);
            if (err) { jz_btree_drop(&metadata); goto fail_auth; }
            break;
        }
        }
        routing_fields = empty_strset();
    } else if (has_snapshot) {
        if (request->visibility != JAZZ_VIS_VISIBLE) { err = sre(request, JAZZ_GAP_HISTORICAL_STORAGE_CUT); goto fail_auth; }
        jazz_node_CurrentRowVec rows;
        jz_err e = jazz_node__node__source_resolution__NodeState__projected_snapshot_current_rows(self->node, SL(request->source.table), self->read_view->read_schema, &snapshot, &rows);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_HISTORICAL_STORAGE_CUT); goto fail_auth; }
        SchemaVersionAlias alias;
        e = jazz_node__node__NodeState__ensure_schema_version_alias(self->node, self->read_view->read_schema, &alias);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); jazz_node_CurrentRowVec_drop(&rows); err = sre(request, JAZZ_GAP_HISTORICAL_STORAGE_CUT); goto fail_auth; }
        groove_GraphBuilder *base;
        e = jazz_node__node__query_eval__read_sources__inline_current_graph_with_source_metadata(&table, rows, alias, "snapshot", &request->requirements, &base, &descriptor, &metadata);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_HISTORICAL_STORAGE_CUT); goto fail_auth; }
        if (requirements_contains(&request->requirements, JAZZ_SMR_VERSION_PAYLOADS)) {
            jz_vec_str hf = jazz_node__node__query_eval__read_sources__maintained_view_history_storage_field_names(&table);
            has_snapshot_cv = true;
            snapshot_content_version.graph = groove__ivm__graph__GraphBuilder__project(groove_GraphBuilder_clone(base), hf.ptr, hf.len);
            snapshot_content_version.row_uuid_field = jz_str_from("row_uuid");
            strvec_drop(&hf);
        }
        /* A prepared binding routes the policy proof by its claim and
         * parameter fields, which later joins project. */
        switch (authorization.tag) {
        case JAZZ_SAR_SYSTEM: graph = base; routing_fields = empty_strset(); break;
        case JAZZ_SAR_POLICY_FILTERED: case JAZZ_SAR_POLICY_PROOF: {
            if (!plan_matches_read_row(&authorization.plan, &table)) { groove_GraphBuilder_free(base); jz_btree_drop(&metadata); err = sre(request, JAZZ_GAP_HISTORICAL_STORAGE_CUT); goto fail_snapshot_cv; }
            jazz_engine_QueryProgramRequest policy_request;
            jz_err pe = table_read_policy_request(self, &table, &authorization, JAZZ_TIER_GLOBAL, &policy_request);
            project_policy_request(pe, &policy_request, self->read_view);
            jz_vec_str output_fields;
            jz_err fe = jazz_node__node__query_eval__lowering__descriptor_field_names(descriptor, &output_fields);
            if (JZ_IS_ERR(fe)) {
                jz_err_drop(&fe); if (!JZ_IS_ERR(pe)) jazz_engine_QueryProgramRequest_drop(&policy_request); else jz_err_drop(&pe);
                groove_GraphBuilder_free(base); jz_btree_drop(&metadata); err = sre(request, JAZZ_GAP_HISTORICAL_STORAGE_CUT); goto fail_snapshot_cv;
            }
            err = compose_filtered(self, request, pe, &policy_request, base, &output_fields, &graph, &routing_fields);
            strvec_drop(&output_fields);
            if (err) { jz_btree_drop(&metadata); goto fail_snapshot_cv; }
            break;
        }
        }
    } else if (has_open_tx) {
        bool include_deleted = request->visibility == JAZZ_VIS_INCLUDE_DELETED;
        /* A query's root source needs no read of its own; policy-subplan
         * sources need none either. Any other source claims the narrowed
         * read its query offers for it (garden-co/jazz#3694). */
        bool is_root = request->source.path.components.len == 1 && request->source.path.components.ptr[0].tag == JAZZ_SR_ROOT;
        if (!self->policy_subplan && !is_root) {
            jz_err e = jazz_node__node__open_tx__NodeState__claim_tx_source_read(self->node, open_tx_overlay, &request->source);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_TRANSACTION_READ_OVERLAY); goto fail_auth; }
        }
        jazz_node_CurrentRowVec rows;
        jz_err e = jazz_node__node__open_tx__NodeState__tx_current_rows_in_schema_with_options(self->node, open_tx_overlay, self->read_view->read_schema, SL(request->source.table), include_deleted, &rows);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_TRANSACTION_READ_OVERLAY); goto fail_auth; }
        groove_GraphBuilder *base;
        if (include_deleted) {
            RowDeletedVec flagged = {0};
            for (u32 i = 0; i < rows.len; i++) {
                RowDeleted rd; rd.deleted = jazz_node__node__CurrentRow__is_deleted(&rows.ptr[i]); rd.row = rows.ptr[i];
                JZ_VEC_PUSH(flagged, rd);
            }
            jz_vec_drop(&rows, sizeof(jazz_node_CurrentRow), NULL);
            SchemaVersionAlias alias;
            e = jazz_node__node__NodeState__ensure_schema_version_alias(self->node, self->read_view->read_schema, &alias);
            if (JZ_IS_ERR(e)) {
                jz_err_drop(&e);
                for (u32 i = 0; i < flagged.len; i++) jazz_node_CurrentRow_drop(&flagged.ptr[i].row);
                jz_vec_drop(&flagged, sizeof(RowDeleted), NULL);
                err = sre(request, JAZZ_GAP_TRANSACTION_READ_OVERLAY); goto fail_auth;
            }
            e = jazz_node__node__query_eval__read_sources__inline_snapshot_include_deleted_current_graph_with_source_metadata(&table, flagged, alias, "open-transaction", &request->requirements, &base, &descriptor, &metadata);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_TRANSACTION_READ_OVERLAY); goto fail_auth; }
        } else {
            SchemaVersionAlias alias;
            e = jazz_node__node__NodeState__ensure_schema_version_alias(self->node, self->read_view->read_schema, &alias);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); jazz_node_CurrentRowVec_drop(&rows); err = sre(request, JAZZ_GAP_TRANSACTION_READ_OVERLAY); goto fail_auth; }
            e = jazz_node__node__query_eval__read_sources__inline_current_graph_with_source_metadata(&table, rows, alias, "open-transaction", &request->requirements, &base, &descriptor, &metadata);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_TRANSACTION_READ_OVERLAY); goto fail_auth; }
        }
        /* An open transaction is a snapshot plus its staged overlay, not an
         * authorization result: filter it through the identity-bound policy. */
        switch (authorization.tag) {
        case JAZZ_SAR_SYSTEM: graph = base; break;
        case JAZZ_SAR_POLICY_FILTERED: case JAZZ_SAR_POLICY_PROOF: {
            if (!plan_matches_read_row(&authorization.plan, &table)) { groove_GraphBuilder_free(base); jz_btree_drop(&metadata); err = sre(request, JAZZ_GAP_COVERAGE); goto fail_auth; }
            jazz_engine_QueryProgramRequest policy_request; jz_err pe;
            if (include_deleted) pe = table_read_policy_request_for_include_deleted(self, &table, &authorization, JAZZ_TIER_GLOBAL, &policy_request);
            else pe = table_read_policy_request(self, &table, &authorization, JAZZ_TIER_GLOBAL, &policy_request);
            project_policy_request(pe, &policy_request, self->read_view);
            jz_vec_str output_fields;
            jz_err fe = jazz_node__node__query_eval__lowering__descriptor_field_names(descriptor, &output_fields);
            if (JZ_IS_ERR(fe)) {
                jz_err_drop(&fe); if (!JZ_IS_ERR(pe)) jazz_engine_QueryProgramRequest_drop(&policy_request); else jz_err_drop(&pe);
                groove_GraphBuilder_free(base); jz_btree_drop(&metadata); err = sre(request, JAZZ_GAP_TRANSACTION_READ_OVERLAY); goto fail_auth;
            }
            if (include_deleted) strvec_push_cstr(&output_fields, "__jazz_deleted");
            err = compose_filtered(self, request, pe, &policy_request, base, &output_fields, &graph, NULL);
            strvec_drop(&output_fields);
            if (err) { jz_btree_drop(&metadata); goto fail_auth; }
            break;
        }
        }
        routing_fields = empty_strset();
    } else if (request->visibility == JAZZ_VIS_VISIBLE
               && (jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__needs_projected_current_source(self, SL(request->source.table))
                   || jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__excludes_below_pending(self, request))
               && !receiver_local_overlay) {
        if (!has_graph_tier) jz_panic("visible current source has a tier");
        if (request->requirements.metadata.len != 0) {
            jazz_node_CurrentSourceGraph src;
            err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_maintained_visible_current_source_graph(self, request, &table, graph_tier, &src);
            if (err) goto fail_auth;
            jz_btree_drop(&src.metadata);
            jz_err e = jazz_node__node__query_eval__read_sources__resolved_current_source_graph(self->node, &table, graph_tier, &request->requirements, &authorization,
                self->read_view->policy_schema, self->read_view, NULL, src.graph, &graph, &descriptor, &metadata, &routing_fields);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_COVERAGE); goto fail_auth; }
        } else {
            jazz_model_DurabilityTier tier = graph_tier;
            groove_GraphBuilder *selected_policy_base = NULL;
            if (jz_btree_contains(&self->access_paths, &request->source) || jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__excludes_below_pending(self, request)) {
                selected_policy_base = NULL;
            } else {
                bool some; jazz_node_CurrentAccessPath access_path;
                err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__cached_policy_authorization_access_path(self, request, &some, &access_path);
                if (err) goto fail_auth;
                if (some) {
                    err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__selected_global_current_source_graph_for_access_path(self, request, &table, tier, access_path, &selected_policy_base);
                    if (err) goto fail_auth;
                }
            }
            jazz_node_CurrentSourceGraph src;
            if (selected_policy_base) {
                src.graph = groove__ivm__graph__GraphBuilder__project_fields(selected_policy_base, jazz_node__node__query_eval__read_sources__storage_to_canonical_current_source_fields(&table, true, false));
                src.descriptor = jazz_node__node__query_eval__read_sources__current_row_descriptor(&table);
                src.metadata = empty_metadata();
            } else {
                err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_visible_current_source_graph(self, request, &table, tier, &src);
                if (err) goto fail_auth;
            }
            switch (authorization.tag) {
            case JAZZ_SAR_SYSTEM: graph = src.graph; routing_fields = empty_strset(); break;
            case JAZZ_SAR_POLICY_FILTERED: case JAZZ_SAR_POLICY_PROOF: {
                if (!plan_matches_read_row(&authorization.plan, &table)) { groove_GraphBuilder_free(src.graph); jz_btree_drop(&src.metadata); err = sre(request, JAZZ_GAP_COVERAGE); goto fail_auth; }
                jazz_engine_QueryProgramRequest policy_request;
                jz_err pe = table_read_policy_request(self, &table, &authorization, graph_tier, &policy_request);
                project_policy_request(pe, &policy_request, self->read_view);
                jz_vec_str output_fields = jazz_node__node__query_eval__read_sources__current_row_fields(&table);
                err = compose_filtered(self, request, pe, &policy_request, src.graph, &output_fields, &graph, &routing_fields);
                strvec_drop(&output_fields);
                if (err) { jz_btree_drop(&src.metadata); goto fail_auth; }
                break;
            }
            }
            descriptor = src.descriptor; metadata = src.metadata;
        }
    } else if (request->visibility == JAZZ_VIS_INCLUDE_DELETED && pending_overlay) {
        /* A receiver's own pending deletion needs no read-policy proof. */
        groove_GraphBuilder *winners;
        err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__pending_deletion_winner_graph(self, request, &winners);
        if (err) goto fail_auth;
        groove_ProjectFieldVec pf = {0};
        groove_ProjectField *f;
        f = groove__ivm__graph__ProjectField__named(CS("row_uuid")); JZ_VEC_PUSH(pf, f);
        f = groove__ivm__graph__ProjectField__named(CS("tx_time")); JZ_VEC_PUSH(pf, f);
        f = groove__ivm__graph__ProjectField__named(CS("tx_node_id")); JZ_VEC_PUSH(pf, f);
        f = groove__ivm__graph__ProjectField__literal(CS("__jazz_deleted"), groove__ivm__op_types__LiteralValue__from_value(groove_Value_bool(true))); JZ_VEC_PUSH(pf, f);
        graph = groove__ivm__graph__GraphBuilder__project_fields(groove__ivm__graph__GraphBuilder__filter(winners, deletion_is(0)), pf);
        descriptor = jazz_node__node__query_eval__read_sources__include_deleted_current_row_descriptor(&table);
        metadata = empty_metadata(); routing_fields = empty_strset();
    } else if (request->visibility == JAZZ_VIS_INCLUDE_DELETED
               && (jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__needs_projected_current_source(self, SL(request->source.table))
                   || jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__excludes_below_pending(self, request))) {
        if (!has_graph_tier) jz_panic("visible current source has a tier");
        jazz_model_DurabilityTier tier = graph_tier;
        groove_GraphBuilder *base;
        err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_include_deleted_current_source_graph(self, request, &table, tier, &base);
        if (err) goto fail_auth;
        /* The policy proof carries the binding routes it depends on; they
         * must reach the deletion witness. */
        switch (authorization.tag) {
        case JAZZ_SAR_SYSTEM: graph = groove_GraphBuilder_clone(base); routing_fields = empty_strset(); break;
        case JAZZ_SAR_POLICY_FILTERED: case JAZZ_SAR_POLICY_PROOF: {
            if (!plan_matches_read_row(&authorization.plan, &table)) { groove_GraphBuilder_free(base); err = sre(request, JAZZ_GAP_COVERAGE); goto fail_auth; }
            jazz_engine_QueryProgramRequest policy_request;
            jz_err pe = table_read_policy_request_for_include_deleted(self, &table, &authorization, tier, &policy_request);
            /* Use the same projected read view as dependency preparation. */
            project_policy_request(pe, &policy_request, self->read_view);
            jz_vec_str output_fields = jazz_node__node__query_eval__read_sources__current_row_fields(&table);
            strvec_push_cstr(&output_fields, "__jazz_deleted");
            err = compose_filtered(self, request, pe, &policy_request, groove_GraphBuilder_clone(base), &output_fields, &graph, &routing_fields);
            strvec_drop(&output_fields);
            if (err) { groove_GraphBuilder_free(base); goto fail_auth; }
            break;
        }
        }
        groove_GraphBuilder_free(base);
        descriptor = jazz_node__node__query_eval__read_sources__include_deleted_current_row_descriptor(&table);
        metadata = empty_metadata();
    } else if (request->visibility == JAZZ_VIS_INCLUDE_DELETED) {
        if (!has_graph_tier) jz_panic("visible current source has a tier");
        jazz_model_DurabilityTier tier = graph_tier;
        groove_GraphBuilder *base = jazz_node__node__query_eval__read_sources__include_deleted_current_graph(&table, tier);
        switch (authorization.tag) {
        case JAZZ_SAR_SYSTEM: graph = base; routing_fields = empty_strset(); break;
        case JAZZ_SAR_POLICY_FILTERED: case JAZZ_SAR_POLICY_PROOF: {
            if (!plan_matches_read_row(&authorization.plan, &table)) { groove_GraphBuilder_free(base); err = sre(request, JAZZ_GAP_COVERAGE); goto fail_auth; }
            jazz_engine_QueryProgramRequest policy_request;
            jz_err pe = table_read_policy_request_for_include_deleted(self, &table, &authorization, tier, &policy_request);
            project_policy_request(pe, &policy_request, self->read_view);
            jz_vec_str output_fields = jazz_node__node__query_eval__read_sources__current_row_fields(&table);
            strvec_push_cstr(&output_fields, "__jazz_deleted");
            err = compose_filtered(self, request, pe, &policy_request, base, &output_fields, &graph, &routing_fields);
            strvec_drop(&output_fields);
            if (err) goto fail_auth;
            break;
        }
        }
        descriptor = jazz_node__node__query_eval__read_sources__include_deleted_current_row_descriptor(&table);
        metadata = empty_metadata();
    } else {
        if (!has_graph_tier) jz_panic("visible current source has a tier");
        jazz_model_DurabilityTier tier = graph_tier;
        groove_GraphBuilder *selected_base = NULL;
        if (receiver_local_overlay) {
            /* This is deliberately an Ahead-only source: the companion
             * CoveredInput source is the complete, policy-scoped remote frontier. */
            self->node->query_engine_read_metrics.source_full_scans += 1;
            err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__pending_local_current_source_graph(self, request, &table, &selected_base);
            if (err) goto fail_auth;
        } else {
            err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__selected_global_current_source_graph(self, request, &table, tier, &selected_base);
            if (err) goto fail_auth;
            if (!selected_base) {
                bool some; jazz_node_CurrentAccessPath access_path;
                err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__cached_policy_authorization_access_path(self, request, &some, &access_path);
                if (err) goto fail_auth;
                if (some) {
                    err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__selected_global_current_source_graph_for_access_path(self, request, &table, tier, access_path, &selected_base);
                    if (err) goto fail_auth;
                }
            }
        }
        if (!selected_base) self->node->query_engine_read_metrics.source_full_scans += 1;
        jz_err e = jazz_node__node__query_eval__read_sources__resolved_current_source_graph(self->node, &table, tier, &request->requirements, &authorization,
            self->read_view->policy_schema, self->read_view, NULL, selected_base, &graph, &descriptor, &metadata, &routing_fields);
        if (JZ_IS_ERR(e)) { err = jazz_node__node__query_eval__read_sources__source_resolution_error_from_policy_proof(request, e); goto fail_auth; }
    }
    /* Own pending deletions carry only winner coordinates and need no
     * read-policy proof. Never manufacture author data for that overlay. */
    if (request->visibility == JAZZ_VIS_INCLUDE_DELETED && !pending_overlay) {
        groove_GraphBuilder *g2; groove_RecordDescriptor d2; jz_btree m2;
        jz_err e = jazz_node__node__query_eval__read_sources__with_requested_author_paths(graph, descriptor, metadata, &request->requirements, &g2, &d2, &m2);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); jz_btree_drop(&routing_fields); err = sre(request, JAZZ_GAP_COVERAGE); goto fail_snapshot_cv; }
        graph = g2; descriptor = d2; metadata = m2;
    }
    bool has_dr = false; jazz_engine_DeletionRegisterSource deletion_register = {0};
    if (!snapshot_source) {
        /* The snapshot is immutable and already excludes rows whose deletion
         * winner is visible at its cut. */
        err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__deletion_register_source_for_request(self, request, &table, has_graph_tier, graph_tier,
            has_history, history_position, has_open_tx, open_tx_overlay, &has_dr, &deletion_register);
        if (err) goto fail_graph;
    }
    bool has_cv = false; jazz_engine_ContentVersionSource content_version = {0};
    if (snapshot_source) { has_cv = has_snapshot_cv; content_version = snapshot_content_version; has_snapshot_cv = false; }
    else {
        err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__content_version_source_for_request(self, request, &table, has_graph_tier, graph_tier,
            has_history, history_position, has_open_tx, open_tx_overlay, &has_cv, &content_version);
        if (err) { if (has_dr) { groove_GraphBuilder_free(deletion_register.graph); jz_str_drop(&deletion_register.row_uuid_field); } goto fail_graph; }
    }
    /* The descriptor attached to the receiver input is the compiler's
     * canonical current-storage descriptor. */
    if (has_covered_descriptor) descriptor = covered_input_descriptor;
    if (covered_input_source) {
        /* An online remote read with immediate local updates composes the
         * authority closure with the eligible local-current overlay. */
        if (jazz_types__debug_env__covered_input_trace()) {
            jz_str line = jz_format("JAZZ_COVERED_INPUT_TRACE stage=union_covered_input request=");
            jazz_engine_SourceId_debug(&line, &request->source);
            jz_str_appendf(&line, " descriptor=%p", (const void *)descriptor);
            jazz_types__debug_env__eprint(line);
        }
        groove_GraphBuilder *covered = covered_input_source; covered_input_source = NULL;
        if (receiver_local_overlay) {
            /* Existing cached rows outside this exact source occurrence cannot
             * enter merely because they have a pending edit. */
            jz_str projection_target;
            err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__current_projection_target(self, request, &table, &projection_target);
            if (err) { groove_GraphBuilder_free(covered); goto fail_cv_dr; }
            groove_GraphBuilder *accepted;
            jz_err e = jazz_node__node__physical__projections__NodeState__physical_current_source_graph_with_projection_target(self->node, self->read_view->read_schema, SL(request->source.table), JAZZ_PCC_GLOBAL, projection_target, &accepted);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); groove_GraphBuilder_free(covered); err = sre(request, JAZZ_GAP_SCHEMA_PROJECTION); goto fail_cv_dr; }
            groove_GraphBuilder *out_of_scope = anti_join_row_uuid(project_row_uuid(accepted), project_row_uuid(groove_GraphBuilder_clone(covered)));
            graph = anti_join_row_uuid(graph, out_of_scope);
        }
        err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__exclude_settled_arm(self, request, covered, false, &covered);
        if (err) goto fail_cv_dr;
        groove_GraphBuilder *winner;
        if (receiver_local_overlay) {
            /* [local, origin] with the covered authority closure as origin. */
            jazz_node_Layer *layers[2] = { jazz_node__node__layer_stack__Layer__new(graph), jazz_node__node__layer_stack__Layer__new(covered) };
            winner = jazz_node__node__layer_stack__compose(layers, 2);
        } else {
            winner = arg_max_by_tx(union2(covered, graph));
        }
        if (receiver_local_overlay) {
            groove_GraphBuilder *winners;
            err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__pending_deletion_winner_graph(self, request, &winners);
            if (err) { groove_GraphBuilder_free(winner); graph = NULL; goto fail_cv_dr; }
            groove_GraphBuilder *deleted = project_row_uuid(groove__ivm__graph__GraphBuilder__filter(winners, deletion_is(0)));
            graph = anti_join_row_uuid(winner, deleted);
        } else {
            graph = winner;
        }
    }
    err = make_resolved(self, request, &table, table_arc(table), graph, descriptor, metadata, routing_fields, false, has_cv, content_version, has_dr, deletion_register, out);
    jazz_engine_SourceAuthorizationRequest_drop(&authorization);
    if (has_snapshot) jz_bytes_drop(&snapshot.bytes);
    if (has_pending_current) jazz_engine_SourceExpr_drop(&pending_current);
    return err;

fail_cv_dr:
    if (has_cv) { groove_GraphBuilder_free(content_version.graph); jz_str_drop(&content_version.row_uuid_field); }
    if (has_dr) { groove_GraphBuilder_free(deletion_register.graph); jz_str_drop(&deletion_register.row_uuid_field); }
fail_graph:
    if (graph) groove_GraphBuilder_free(graph);
    jz_btree_drop(&metadata); jz_btree_drop(&routing_fields);
fail_snapshot_cv:
    if (has_snapshot_cv) { groove_GraphBuilder_free(snapshot_content_version.graph); jz_str_drop(&snapshot_content_version.row_uuid_field); }
fail_auth:
done_auth:
    jazz_engine_SourceAuthorizationRequest_drop(&authorization);
fail_table:
done_table:
    jazz_model_TableSchema_drop(&table);
fail:
    if (covered_input_source) groove_GraphBuilder_free(covered_input_source);
    if (has_snapshot) jz_bytes_drop(&snapshot.bytes);
    if (has_pending_current) jazz_engine_SourceExpr_drop(&pending_current);
    return err;
}

/* Prepare the ordinary current-row source without constructing the
 * historical/branch source-state machine. This is the hot path for regular
 * reads and policy admission. */
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__prepare_unprojected_visible_current_source_graph(Prep *self, const SReq *request, jazz_model_DurabilityTier tier, jazz_engine_ResolvedSource *out) {
    const jazz_engine_SourceExpr *e0 = jz_btree_get(&self->read_view->sources, &request->source);
    if (!e0 || e0->tag != JAZZ_SE_VISIBLE_CURRENT || e0->data.tag != JAZZ_DS_CURRENT) return sre(request, JAZZ_GAP_COVERAGE);
    jazz_engine_SchemaProjection projection; jazz_engine_SchemaProjection_clone(&projection, &e0->projection);
    bool canonical = projection_is_canonical(&projection);
    jazz_engine_SchemaProjection_drop(&projection);
    if (!canonical) return sre(request, JAZZ_GAP_SCHEMA_PROJECTION);
    jazz_model_TableSchema table;
    SRE_MAP(jazz_node__node__NodeState__table_in_schema(self->node, SL(request->source.table), self->read_view->read_schema, &table), request, JAZZ_GAP_SCHEMA_PROJECTION);
    jazz_engine_SourceAuthorizationRequest authorization;
    effective_authorization(request, &authorization);
    jz_sre err = NULL;
    groove_GraphBuilder *selected_base = NULL;
    err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__selected_global_current_source_graph(self, request, &table, tier, &selected_base);
    if (err) goto done;
    if (!selected_base) {
        bool some; jazz_node_CurrentAccessPath access_path;
        err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__cached_policy_authorization_access_path(self, request, &some, &access_path);
        if (err) goto done;
        if (some) {
            err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__selected_global_current_source_graph_for_access_path(self, request, &table, tier, access_path, &selected_base);
            if (err) goto done;
        }
    }
    if (!selected_base) self->node->query_engine_read_metrics.source_full_scans += 1;
    groove_GraphBuilder *graph; groove_RecordDescriptor descriptor; jz_btree metadata, routing_fields;
    jz_err e = jazz_node__node__query_eval__read_sources__resolved_current_source_graph(self->node, &table, tier, &request->requirements, &authorization,
        self->read_view->policy_schema, self->read_view, NULL, selected_base, &graph, &descriptor, &metadata, &routing_fields);
    if (JZ_IS_ERR(e)) { err = jazz_node__node__query_eval__read_sources__source_resolution_error_from_policy_proof(request, e); goto done; }
    bool has_dr; jazz_engine_DeletionRegisterSource dr;
    jazz_types_GlobalTime no_pos = {0}; jazz_model_OpenTransactionId no_tx = {0};
    err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__deletion_register_source_for_request(self, request, &table, true, tier, false, no_pos, false, no_tx, &has_dr, &dr);
    if (err) { groove_GraphBuilder_free(graph); jz_btree_drop(&metadata); jz_btree_drop(&routing_fields); goto done; }
    bool has_cv; jazz_engine_ContentVersionSource cv;
    err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__content_version_source_for_request(self, request, &table, true, tier, false, no_pos, false, no_tx, &has_cv, &cv);
    if (err) {
        groove_GraphBuilder_free(graph); jz_btree_drop(&metadata); jz_btree_drop(&routing_fields);
        if (has_dr) { groove_GraphBuilder_free(dr.graph); jz_str_drop(&dr.row_uuid_field); }
        goto done;
    }
    err = make_resolved(self, request, &table, table_arc_clone(&table), graph, descriptor, metadata, routing_fields, false, has_cv, cv, has_dr, dr, out);
done:
    jazz_engine_SourceAuthorizationRequest_drop(&authorization);
    jazz_model_TableSchema_drop(&table);
    return err;
}

/* Prepare a current-row source whose storage layout needs the projected
 * source path. Kept separate from historical and branch preparation so peer
 * admission does not inherit their async frame. */
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__prepare_projected_visible_current_source_graph(Prep *self, const SReq *request, jazz_model_DurabilityTier tier, jazz_engine_ResolvedSource *out) {
    const jazz_engine_SourceExpr *e0 = jz_btree_get(&self->read_view->sources, &request->source);
    if (!e0 || e0->tag != JAZZ_SE_VISIBLE_CURRENT || e0->data.tag != JAZZ_DS_CURRENT) return sre(request, JAZZ_GAP_COVERAGE);
    jazz_engine_SchemaProjection projection; jazz_engine_SchemaProjection_clone(&projection, &e0->projection);
    bool canonical = projection_is_canonical(&projection);
    jazz_engine_SchemaProjection_drop(&projection);
    if (!canonical) return sre(request, JAZZ_GAP_SCHEMA_PROJECTION);
    jazz_model_TableSchema table;
    SRE_MAP(jazz_node__node__NodeState__table_in_schema(self->node, SL(request->source.table), self->read_view->read_schema, &table), request, JAZZ_GAP_SCHEMA_PROJECTION);
    jazz_engine_SourceAuthorizationRequest authorization;
    effective_authorization(request, &authorization);
    jz_sre err = NULL;
    groove_GraphBuilder *graph; groove_RecordDescriptor descriptor; jz_btree metadata, routing_fields;
    if (request->requirements.metadata.len != 0) {
        jazz_node_CurrentSourceGraph src;
        err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_maintained_visible_current_source_graph(self, request, &table, tier, &src);
        if (err) goto done;
        jz_btree_drop(&src.metadata);
        jz_err e = jazz_node__node__query_eval__read_sources__resolved_current_source_graph(self->node, &table, tier, &request->requirements, &authorization,
            self->read_view->policy_schema, self->read_view, NULL, src.graph, &graph, &descriptor, &metadata, &routing_fields);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_COVERAGE); goto done; }
    } else {
        groove_GraphBuilder *selected_policy_base = NULL;
        if (jz_btree_contains(&self->access_paths, &request->source) || jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__excludes_below_pending(self, request)) {
            selected_policy_base = NULL;
        } else {
            bool some; jazz_node_CurrentAccessPath access_path;
            err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__cached_policy_authorization_access_path(self, request, &some, &access_path);
            if (err) goto done;
            if (some) {
                err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__selected_global_current_source_graph_for_access_path(self, request, &table, tier, access_path, &selected_policy_base);
                if (err) goto done;
            }
        }
        jazz_node_CurrentSourceGraph src;
        if (selected_policy_base) {
            src.graph = groove__ivm__graph__GraphBuilder__project_fields(selected_policy_base, jazz_node__node__query_eval__read_sources__storage_to_canonical_current_source_fields(&table, true, false));
            src.descriptor = jazz_node__node__query_eval__read_sources__current_row_descriptor(&table);
            src.metadata = empty_metadata();
        } else {
            err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_visible_current_source_graph(self, request, &table, tier, &src);
            if (err) goto done;
        }
        switch (authorization.tag) {
        case JAZZ_SAR_SYSTEM: graph = src.graph; routing_fields = empty_strset(); break;
        case JAZZ_SAR_POLICY_FILTERED: case JAZZ_SAR_POLICY_PROOF: {
            if (!plan_matches_read_row(&authorization.plan, &table)) { groove_GraphBuilder_free(src.graph); jz_btree_drop(&src.metadata); err = sre(request, JAZZ_GAP_COVERAGE); goto done; }
            jazz_engine_QueryProgramRequest policy_request;
            jz_err pe = table_read_policy_request(self, &table, &authorization, tier, &policy_request);
            project_policy_request(pe, &policy_request, self->read_view);
            jz_vec_str output_fields = jazz_node__node__query_eval__read_sources__current_row_fields(&table);
            err = compose_filtered(self, request, pe, &policy_request, src.graph, &output_fields, &graph, &routing_fields);
            strvec_drop(&output_fields);
            if (err) { jz_btree_drop(&src.metadata); goto done; }
            break;
        }
        }
        descriptor = src.descriptor; metadata = src.metadata;
    }
    {
        bool has_dr; jazz_engine_DeletionRegisterSource dr;
        jazz_types_GlobalTime no_pos = {0}; jazz_model_OpenTransactionId no_tx = {0};
        err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__deletion_register_source_for_request(self, request, &table, true, tier, false, no_pos, false, no_tx, &has_dr, &dr);
        if (err) { groove_GraphBuilder_free(graph); jz_btree_drop(&metadata); jz_btree_drop(&routing_fields); goto done; }
        bool has_cv; jazz_engine_ContentVersionSource cv;
        err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__content_version_source_for_request(self, request, &table, true, tier, false, no_pos, false, no_tx, &has_cv, &cv);
        if (err) {
            groove_GraphBuilder_free(graph); jz_btree_drop(&metadata); jz_btree_drop(&routing_fields);
            if (has_dr) { groove_GraphBuilder_free(dr.graph); jz_str_drop(&dr.row_uuid_field); }
            goto done;
        }
        err = make_resolved(self, request, &table, table_arc_clone(&table), graph, descriptor, metadata, routing_fields, false, has_cv, cv, has_dr, dr, out);
    }
done:
    jazz_engine_SourceAuthorizationRequest_drop(&authorization);
    jazz_model_TableSchema_drop(&table);
    return err;
}

static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__prepare_inline_visible_current_source_graph(Prep *self, const SReq *request, jazz_engine_ResolvedSource *out) {
    const jazz_engine_SourceExpr *e0 = jz_btree_get(&self->read_view->sources, &request->source);
    if (!e0 || e0->tag != JAZZ_SE_VISIBLE_CURRENT || e0->data.tag != JAZZ_DS_CURRENT) return sre(request, JAZZ_GAP_COVERAGE);
    const jazz_node_CurrentRowVec *rows = jz_btree_get(&self->inline_sources, &request->source);
    if (!rows) return sre(request, JAZZ_GAP_COVERAGE);
    if (!projection_is_canonical(&e0->projection)) return sre(request, JAZZ_GAP_SCHEMA_PROJECTION);
    jazz_engine_SourceAuthorizationRequest authorization;
    effective_authorization(request, &authorization);
    bool system = authorization.tag == JAZZ_SAR_SYSTEM;
    jazz_engine_SourceAuthorizationRequest_drop(&authorization);
    if (request->visibility != JAZZ_VIS_VISIBLE || !system) return sre(request, JAZZ_GAP_COVERAGE);
    jazz_model_TableSchema table;
    SRE_MAP(jazz_node__node__NodeState__table_in_schema(self->node, SL(request->source.table), self->read_view->read_schema, &table), request, JAZZ_GAP_SCHEMA_PROJECTION);
    SchemaVersionAlias alias;
    jz_err e = jazz_node__node__NodeState__ensure_schema_version_alias(self->node, self->read_view->read_schema, &alias);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); jazz_model_TableSchema_drop(&table); return sre(request, JAZZ_GAP_COVERAGE); }
    jazz_node_CurrentRowVec cloned; jazz_node_CurrentRowVec_clone(&cloned, rows);
    groove_GraphBuilder *graph; groove_RecordDescriptor descriptor; jz_btree metadata;
    e = jazz_node__node__query_eval__read_sources__inline_current_graph_with_source_metadata(&table, cloned, alias, "inline-current", &request->requirements, &graph, &descriptor, &metadata);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); jazz_model_TableSchema_drop(&table); return sre(request, JAZZ_GAP_COVERAGE); }
    jz_sre err = make_resolved(self, request, &table, table_arc_clone(&table), graph, descriptor, metadata, empty_strset(), false, false, NO_CV, false, NO_DR, out);
    jazz_model_TableSchema_drop(&table);
    return err;
}

/* impl SourceGraphPreparer for JazzSourceGraphPreparer */
jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__SourceGraphPreparer__prepare_source_graph(Prep *self, const SReq *request, jazz_engine_ResolvedSource *out) {
    jazz_protocol_PolicyBindingKey scope;
    bool has_scope = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__local_exclusion_scope(self, request, &scope);
    /* A physical limit is valid only when no later source filter can remove
     * candidates. Unavailability is a mutable anti-join, so retain the
     * logical query limit after it instead. */
    if (has_scope) {
        jazz_node_CurrentAccessPath *p = jz_btree_get(&self->access_paths, &request->source);
        if (p && p->tag == JAZZ_CAP_INDEX) p->has_source_limit = false;
    }
    jz_sre err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__prepare_source_graph_without_local_exclusions(self, request, out);
    if (err) { if (has_scope) jazz_protocol_PolicyBindingKey_drop(&scope); return err; }
    /* Only a write-policy check of a multi-row unit carries an overlay. */
    if (jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__is_active(&self->transaction_overlay)) {
        err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__overlay_transaction_writes(self, request, out);
        if (err) { jazz_engine_ResolvedSource_drop(out); if (has_scope) jazz_protocol_PolicyBindingKey_drop(&scope); return err; }
    }
    if (has_scope && !jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__excludes_below_pending(self, request)) {
        groove_GraphBuilder *g;
        jz_err e = jazz_node__node__query_eval__unavailable_inputs__NodeState__exclude_local_unavailable_graph(self->node, &scope, self->read_view->read_schema, request, out->graph, false, &g);
        out->graph = NULL;
        jazz_protocol_PolicyBindingKey_drop(&scope);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); jazz_engine_ResolvedSource_drop(out); return sre(request, JAZZ_GAP_LOCAL_AVAILABILITY_INPUT); }
        out->graph = g;
        return NULL;
    }
    if (has_scope) jazz_protocol_PolicyBindingKey_drop(&scope);
    return NULL;
}
static jz_err prepare_source_graph_vt(void *self, const jazz_engine_SourceRequest *req, jazz_engine_ResolvedSource *out, jazz_engine_SourceResolutionError *err) {
    jz_sre s = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__SourceGraphPreparer__prepare_source_graph(self, req, out);
    if (!s) return JZ_OK;
    *err = *s; jz_free(s);
    return jz_err_new(JZ_SRE_CODE, "source resolution error");
}
const jazz_engine_SourceGraphPreparerVtbl jazz_node_JazzSourceGraphPreparer_SourceGraphPreparer_vtbl = { prepare_source_graph_vt };

/* Layer the candidate transaction's own writes over one committed write-policy
 * evidence source (INV-RLS-9). */
typedef struct {
    const jazz_node_TransactionOverlayTable *rows; const jazz_engine_ResolvedSource *resolved; groove_RecordDescriptor descriptor;
    SchemaVersionAlias alias; const SReq *request;
} live_encode_ctx;
static jz_err encode_live_records(void *vctx, jazz_node_OverlayRecordVec *out) {
    live_encode_ctx *c = vctx;
    jazz_types_RowUuid *row_uuid; jazz_node_CurrentRow **row;
    JZ_BTREE_FOR(&c->rows->rows, row_uuid, row) {
        if (!*row) continue;
        jazz_node_OverlayRecord rec; rec.row = *row_uuid;
        JZ_TRY(jazz_node__node__query_eval__read_sources__inline_current_record_for_output(c->resolved->table_schema, c->descriptor, *row, c->alias, "transaction-overlay", &c->request->requirements, &rec.record));
        jz_vec_push(out, &rec, sizeof rec);
    }
    return JZ_OK;
}
typedef struct { const jazz_node_TransactionOverlayTable *rows; groove_RecordDescriptor descriptor; } written_encode_ctx;
static jz_err encode_written_records(void *vctx, jazz_node_OverlayRecordVec *out) {
    written_encode_ctx *c = vctx;
    jazz_types_RowUuid *row_uuid; jazz_node_CurrentRow **row;
    JZ_BTREE_FOR(&c->rows->rows, row_uuid, row) {
        jazz_node_OverlayRecord rec; rec.row = *row_uuid;
        groove_Value v = groove_Value_uuid(row_uuid->u);
        JZ_TRY(groove__records__RecordDescriptor__create(c->descriptor, &v, 1, &rec.record));
        jz_vec_push(out, &rec, sizeof rec);
    }
    return JZ_OK;
}
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__overlay_transaction_writes(Prep *self, const SReq *request, jazz_engine_ResolvedSource *resolved) {
    const jazz_engine_SourceExpr *e0 = jz_btree_get(&self->read_view->sources, &request->source);
    if (!self->policy_subplan || request->visibility != JAZZ_VIS_VISIBLE
        || jz_btree_contains(&self->inline_sources, &request->source)
        || jz_btree_contains(&self->covered_input_sources, &request->source)
        || !(e0 && e0->tag == JAZZ_SE_VISIBLE_CURRENT && e0->data.tag == JAZZ_DS_CURRENT))
        return NULL;
    /* Every overlay-eligible table the policy reads is recorded. */
    jazz_types_SchemaVersionId read_schema = self->read_view->read_schema;
    jz_str table_name = jz_str_clone(&request->source.table);
    jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__record_read(&self->transaction_overlay, read_schema, SL(table_name));
    jazz_node_TransactionOverlayTable *rows = jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__table(&self->transaction_overlay, read_schema, SL(table_name));
    if (!rows) { jz_str_drop(&table_name); return NULL; }
    rows = jz_rc_clone(rows);
    jz_sre err = NULL;
    /* Policy subplan sources are read as raw evidence (INV-RLS-21). */
    if (resolved->routing_fields.len != 0) { err = sre(request, JAZZ_GAP_TRANSACTION_READ_OVERLAY); goto done; }
    SchemaVersionAlias alias;
    jz_err e = jazz_node__node__NodeState__ensure_schema_version_alias(self->node, read_schema, &alias);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_TRANSACTION_READ_OVERLAY); goto done; }
    /* Encode the live rows as records of the committed arm's exact inferred output. */
    groove_RecordDescriptor descriptor;
    e = groove__db__Database__graph_output_descriptor(&self->node->database, resolved->graph, &descriptor);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_TRANSACTION_READ_OVERLAY); goto done; }
    live_encode_ctx lctx = { rows, resolved, descriptor, alias, request };
    jazz_node_OverlayRecordVec *live_records;
    e = jazz_node__node__query_eval__read_sources__TransactionOverlayTable__cached(&rows->live_records, descriptor, encode_live_records, &lctx, &live_records);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_TRANSACTION_READ_OVERLAY); goto done; }
    jz_str row_field = jz_str_clone(&resolved->row_shape.row_uuid_field);
    groove_DescriptorFieldVec wf = {0};
    groove_DescriptorField f = groove__records__DescriptorField__new(jz_str_clone(&row_field), groove_ValueType_simple(GROOVE_VT_UUID));
    JZ_VEC_PUSH(wf, f);
    groove_RecordDescriptor written_descriptor = groove__records__RecordDescriptor__new(wf);
    written_encode_ctx wctx = { rows, written_descriptor };
    jazz_node_OverlayRecordVec *written_records;
    e = jazz_node__node__query_eval__read_sources__TransactionOverlayTable__cached(&rows->written_records, written_descriptor, encode_written_records, &wctx, &written_records);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); jz_rc_release(live_records); jz_str_drop(&row_field); err = sre(request, JAZZ_GAP_TRANSACTION_READ_OVERLAY); goto done; }
    /* The row under check is the inline candidate; its committed state stays untouched. */
    jz_vec_bytes live_vec = {0};
    for (u32 i = 0; i < live_records->len; i++) {
        if (jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__excludes(&self->transaction_overlay, read_schema, SL(table_name), live_records->ptr[i].row)) continue;
        jz_bytes b = jz_bytes_clone(&live_records->ptr[i].record); JZ_VEC_PUSH(live_vec, b);
    }
    groove_GraphBuilder *live = groove__ivm__graph__GraphBuilder__inline_records(descriptor, live_vec);
    jz_vec_bytes written_vec = {0};
    for (u32 i = 0; i < written_records->len; i++) {
        if (jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__excludes(&self->transaction_overlay, read_schema, SL(table_name), written_records->ptr[i].row)) continue;
        jz_bytes b = jz_bytes_clone(&written_records->ptr[i].record); JZ_VEC_PUSH(written_vec, b);
    }
    groove_GraphBuilder *committed = groove__ivm__graph__GraphBuilder__anti_join(
        groove_GraphBuilder_clone(resolved->graph), groove__ivm__graph__GraphBuilder__inline_records(written_descriptor, written_vec),
        &row_field, 1, &row_field, 1);
    groove_GraphBuilder_free(resolved->graph);
    resolved->graph = union2(committed, live);
    jz_str_drop(&row_field);
    jz_rc_release(live_records); jz_rc_release(written_records);
done:
    jz_rc_release(rows);
    jz_str_drop(&table_name);
    return err;
}

static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__prepare_source_graph_without_local_exclusions(Prep *self, const SReq *request, jazz_engine_ResolvedSource *out) {
    /* A receiver-owned covered source must take the single resolver path that
     * either uses the strict authority frontier directly or unions it with the
     * eligible local frontier. */
    if (jz_btree_contains(&self->covered_input_sources, &request->source))
        return jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__prepare_source_graph_dispatch(self, request, out);
    const jazz_engine_SourceExpr *e0 = jz_btree_get(&self->read_view->sources, &request->source);
    if (e0 && e0->tag == JAZZ_SE_VISIBLE_CURRENT && e0->data.tag == JAZZ_DS_CURRENT) {
        jazz_engine_SchemaProjection projection; jazz_engine_SchemaProjection_clone(&projection, &e0->projection);
        jazz_model_DurabilityTier tier = e0->tier;
        jazz_engine_SchemaProjection_drop(&projection);
        if (jz_btree_contains(&self->inline_sources, &request->source))
            return jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__prepare_inline_visible_current_source_graph(self, request, out);
        if (request->visibility == JAZZ_VIS_VISIBLE) {
            if (jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__needs_projected_current_source(self, SL(request->source.table)))
                return jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__prepare_projected_visible_current_source_graph(self, request, tier, out);
            return jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__prepare_unprojected_visible_current_source_graph(self, request, tier, out);
        }
    }
    return jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__prepare_source_graph_dispatch(self, request, out);
}

/* ======================================================================
 * impl JazzSourceGraphPreparer: policy dependencies and current sources
 * ====================================================================== */
static bool str_contains(const jz_str *s, const char *needle) {
    u32 n = (u32)strlen(needle);
    if (n > s->len) return false;
    for (u32 i = 0; i + n <= s->len; i++) if (memcmp(s->ptr + i, needle, n) == 0) return true;
    return false;
}
jz_err jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__policy_dependency_request(Prep *self, const SReq *request, bool *some, jazz_engine_QueryProgramRequest *out) {
    *some = false;
    if (request->authorization.tag != JAZZ_SAR_POLICY_FILTERED) return JZ_OK;
    jazz_types_AuthorSubject permission_subject = request->authorization.permission_subject;
    const jazz_engine_PolicyAuthorizationPlan *plan = &request->authorization.plan;
    if (!jz_str_eq(&plan->protected_source.table, &request->source.table) || plan->role != JAZZ_PDR_READ || !jz_str_eq_cstr(&plan->protected_row_field, "row_uuid"))
        return JZN_QUERY_CAPABILITY("%s", "policy authorization plan does not match source dependency");
    const jazz_engine_SourceExpr *source = jz_btree_get(&self->read_view->sources, &request->source);
    if (!source) return JZN_INVALID_STORED("query source dependency missing");
    bool has_shape = plan->has_binding_source_shape;
    jz_str binding_source_shape = opt_shape_clone(plan);
    jz_btree binding_user_params = btree_clone_plain(&plan->binding_user_params);
    jz_btree binding_claim_params = btree_clone_plain(&plan->binding_claim_params);
    jz_err dependency;
    switch (source->tag) {
    case JAZZ_SE_HISTORY_CUT:
        if (source->data.tag != JAZZ_DS_CURRENT) goto none;
        dependency = jazz_node__node__query_eval__authorization__NodeState__table_read_policy_authorization_request_at(
            self->node, self->read_view->policy_schema, SL(request->source.table), permission_subject, JAZZ_PBM_INLINE_ALL_REACHABLE_SEEDS, source->position,
            has_shape, binding_source_shape, binding_user_params, binding_claim_params, out);
        break;
    case JAZZ_SE_SNAPSHOT_REF: {
        if (source->data.tag != JAZZ_DS_CURRENT) goto none;
        jazz_node_ParamBindingMode mode = has_shape ? JAZZ_PBM_RETAIN_ALL_PARAMS : JAZZ_PBM_INLINE_ALL_REACHABLE_SEEDS;
        dependency = jazz_node__node__query_eval__authorization__NodeState__table_read_policy_authorization_request(
            self->node, self->read_view->policy_schema, SL(request->source.table), permission_subject, mode, JAZZ_TIER_GLOBAL,
            has_shape, binding_source_shape, binding_user_params, binding_claim_params, out);
        break;
    }
    case JAZZ_SE_VISIBLE_CURRENT: case JAZZ_SE_BRANCH_VIEW: case JAZZ_SE_SETTLED_BINDING_VIEW: case JAZZ_SE_WITH_OVERLAYS: {
        jazz_model_DurabilityTier tier;
        if (!jazz_engine__query_engine__read__SourceExpr__current_tier(source, &tier)) tier = JAZZ_TIER_GLOBAL;
        if (request->visibility == JAZZ_VIS_INCLUDE_DELETED) {
            dependency = jazz_node__node__query_eval__authorization__NodeState__table_read_policy_authorization_request_for_include_deleted(
                self->node, self->read_view->policy_schema, SL(request->source.table), permission_subject, tier,
                has_shape, binding_source_shape, binding_user_params, binding_claim_params, out);
        } else {
            jazz_node_ParamBindingMode mode = has_shape ? JAZZ_PBM_RETAIN_ALL_PARAMS : JAZZ_PBM_INLINE_ALL_REACHABLE_SEEDS;
            dependency = jazz_node__node__query_eval__authorization__NodeState__table_read_policy_authorization_request(
                self->node, self->read_view->policy_schema, SL(request->source.table), permission_subject, mode, tier,
                has_shape, binding_source_shape, binding_user_params, binding_claim_params, out);
        }
        break;
    }
    default:
    none:
        if (has_shape) jz_str_drop(&binding_source_shape);
        jz_btree_drop(&binding_user_params); jz_btree_drop(&binding_claim_params);
        return JZ_OK;
    }
    if (!JZ_IS_ERR(dependency)) {
        jazz_engine_ReadView projected = jazz_node__node__query_eval__read_sources__policy_read_view_projected_through(&out->reads.primary, self->read_view);
        jazz_engine_ReadView_drop(&out->reads.primary);
        out->reads.primary = projected;
        *some = true;
        return JZ_OK;
    }
    if (dependency.code == JZN_E_QueryCapability) {
        if (dependency.msg && str_contains(dependency.msg, "PolicyProofCycle")) return dependency;
        jz_err_drop(&dependency);
        return JZ_OK;
    }
    return dependency;
}

jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__current_projection_target(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jz_str *out) {
    const jz_str *cached = jz_btree_get(&self->current_projection_targets, &request->source);
    if (cached) { *out = jz_str_clone(cached); return NULL; }
    jz_btree required_fields = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__current_projection_required_fields(self, request, table);
    jz_str target;
    jz_err e = jazz_node__node__physical__NodeState__ensure_physical_current_projection_for_enum_columns(self->node, self->read_view->read_schema, SL(table->name), &required_fields, &target);
    jz_btree_drop(&required_fields);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return sre(request, JAZZ_GAP_SCHEMA_PROJECTION); }
    jazz_engine_SourceId key; jazz_engine_SourceId_clone(&key, &request->source);
    jz_str stored = jz_str_clone(&target);
    jz_str old;
    if (jz_btree_insert(&self->current_projection_targets, &key, &stored, &old)) jz_str_drop(&old);
    *out = target;
    return NULL;
}

jz_btree jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__current_projection_required_fields(const Prep *self, const SReq *request, const jazz_model_TableSchema *table) {
    /* A maintained version witness identifies the immutable stored version; it
     * must not widen this current-query source to every user column. */
    jz_btree fields = empty_strset();
    switch (request->requirements.app_fields.tag) {
    case JAZZ_FR_ALL:
        for (u32 i = 0; i < table->columns.len; i++) strset_insert(&fields, jz_str_clone(&table->columns.ptr[i].name));
        break;
    case JAZZ_FR_NONE: break;
    case JAZZ_FR_FIELDS: jz_btree_drop(&fields); jz_btree_clone(&fields, &request->requirements.app_fields.fields, str_clone_v, NULL); break;
    }
    return fields;
}

static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__selected_global_current_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, groove_GraphBuilder **out) {
    *out = NULL;
    const jazz_node_CurrentAccessPath *p = jz_btree_get(&self->access_paths, &request->source);
    if (!p) return NULL;
    jazz_node_CurrentAccessPath access_path; jazz_node_CurrentAccessPath_clone(&access_path, p);
    return jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__selected_global_current_source_graph_for_access_path(self, request, table, tier, access_path, out);
}

/* The only storage-backed half of a Local-first covered receiver input. */
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__pending_local_current_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *table, groove_GraphBuilder **out) {
    groove_GraphBuilder *graph;
    SRE_TRY(jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__visible_current_physical_source_graph(self, request, table, JAZZ_PCC_AHEAD, JAZZ_TIER_LOCAL, &graph));
    return jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__exclude_settled_arm(self, request, graph, true, out);
}

/* Read a physical current arm with the ordinary deletion fence. */
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__visible_current_physical_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_node_PhysicalCurrentClass cls, jazz_model_DurabilityTier deletion_tier, groove_GraphBuilder **out) {
    jz_str projection_target;
    SRE_TRY(jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__current_projection_target(self, request, table, &projection_target));
    groove_GraphBuilder *content;
    SRE_MAP(jazz_node__node__physical__projections__NodeState__physical_current_source_graph_with_projection_target(self->node, self->read_view->read_schema, SL(request->source.table), cls, projection_target, &content), request, JAZZ_GAP_SCHEMA_PROJECTION);
    groove_GraphBuilder *reg;
    jz_sre s = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_deletion_register_current_source_graph(self, request, deletion_tier, &reg);
    if (s) { groove_GraphBuilder_free(content); return s; }
    groove_GraphBuilder *deleted_winners = project_row_uuid(groove__ivm__graph__GraphBuilder__filter(reg, deletion_is(0)));
    *out = anti_join_row_uuid(content, deleted_winners);
    return NULL;
}

/* Reuse the access paths chosen while compiling the already-prepared policy
 * dependency: a generic planner receipt, not a policy-specific choice. */
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__cached_policy_authorization_access_path(Prep *self, const SReq *request, bool *some, jazz_node_CurrentAccessPath *out) {
    *some = false;
    bool has_req; jazz_engine_QueryProgramRequest policy_request;
    jz_err e = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__policy_dependency_request(self, request, &has_req, &policy_request);
    if (JZ_IS_ERR(e)) return jazz_node__node__query_eval__read_sources__source_resolution_error_from_policy_proof(request, e);
    if (!has_req) return NULL;
    jz_str cache_key = jazz_node__node__query_eval__lowering__policy_authorization_graph_cache_key(&policy_request);
    jazz_engine_QueryProgramRequest_drop(&policy_request);
    jazz_node_PolicyAuthorizationGraph authorization;
    bool found = jazz_node__node__query_eval__lowering__NodeState__policy_authorization_graph_cache_get(self->node, jz_str_slice(&cache_key), &authorization);
    jz_str_drop(&cache_key);
    if (!found) return sre(request, JAZZ_GAP_COVERAGE);
    const jazz_node_CurrentAccessPath *p = jz_btree_get(&authorization.access_paths, &request->source);
    if (p) { jazz_node_CurrentAccessPath_clone(out, p); *some = true; }
    jazz_node_PolicyAuthorizationGraph_drop(&authorization);
    return NULL;
}

static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__selected_global_current_source_graph_for_access_path(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, jazz_node_CurrentAccessPath access_path, groove_GraphBuilder **out) {
    *out = NULL;
    jz_sre err = NULL;
    switch (access_path.tag) {
    case JAZZ_CAP_PRIMARY_KEY:
        if (self->count_access_path_metrics) self->node->query_engine_read_metrics.source_primary_key_scans += 1;
        *out = jazz_node__node__query_eval__read_sources__selected_visible_current_primary_key_graph(table, tier, access_path.prefix);
        access_path.prefix.ptr = NULL; access_path.prefix.len = access_path.prefix.cap = 0;
        break;
    case JAZZ_CAP_PRIMARY_KEY_PAGE: {
        if (tier != JAZZ_TIER_GLOBAL) break;
        jz_str projection_target;
        err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__current_projection_target(self, request, table, &projection_target);
        if (err) break;
        jz_err e = jazz_node__node__physical__projections__NodeState__physical_current_source_scan_graph_with_projection_target(self->node, self->read_view->read_schema, SL(request->source.table),
            JAZZ_PCC_GLOBAL, projection_target, jazz_node__node__query_eval__read_sources__primary_key_page_scan(access_path.cap), out);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_SCHEMA_PROJECTION); }
        break;
    }
    case JAZZ_CAP_INDEX: {
        if (tier != JAZZ_TIER_GLOBAL) break;
        bool has_limit = (access_path.has_order_column || request->visibility == JAZZ_VIS_INCLUDE_DELETED) && access_path.has_source_limit;
        jz_str projection_target;
        err = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__current_projection_target(self, request, table, &projection_target);
        if (err) break;
        jz_err e = jazz_node__node__query_eval__read_sources__NodeState__physical_global_current_source_for_index_scan(self->node, table, self->read_view->read_schema,
            SL(access_path.column), access_path.has_order_column ? &access_path.order_column : NULL, access_path.reverse, &access_path.prefix, &access_path.intersections,
            false, has_limit, access_path.source_limit, NULL, SL(projection_target), out);
        jz_str_drop(&projection_target);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = sre(request, JAZZ_GAP_COVERAGE); break; }
        self->node->query_engine_read_metrics.source_index_probes += 1 + (u64)access_path.intersections.len;
        break;
    }
    }
    jazz_node_CurrentAccessPath_drop(&access_path);
    return err;
}

static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__pending_deletion_winner_graph(Prep *self, const SReq *request, groove_GraphBuilder **out) {
    groove_GraphBuilder *ahead;
    if (jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__needs_projected_current_source(self, SL(request->source.table))) {
        jazz_types_PhysicalTableId table_id;
        SRE_MAP(jazz_node__node__physical__NodeState__physical_table_id_for_schema(self->node, self->read_view->read_schema, SL(request->source.table), &table_id), request, JAZZ_GAP_SCHEMA_PROJECTION);
        (void)table_id;
        jazz_model_BranchKey def = jazz_model__model__branch__BranchKey__default();
        jz_err e = jazz_node__node__physical__projections__NodeState__physical_current_marker_source_graph(self->node, self->read_view->read_schema, SL(request->source.table), JAZZ_PCC_AHEAD, &def, NULL, &ahead);
        jazz_model_BranchKey_drop(&def);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); return sre(request, JAZZ_GAP_SCHEMA_PROJECTION); }
    } else {
        jz_str name = jazz_node__node__codec__ahead_current_table_name(SL(request->source.table));
        ahead = groove__ivm__graph__GraphBuilder__table(SL(name));
        jz_str_drop(&name);
    }
    /* Ahead can contain several unconfirmed images of the same row. The
     * newest one decides, so a pending restore supersedes a pending delete. */
    groove_ProjectFieldVec fields = jazz_node__node__query_eval__read_sources__register_storage_fields_for_query_engine(CS(""));
    groove_ProjectFieldVec fields2 = jazz_node__node__query_eval__read_sources__register_storage_fields_for_query_engine(CS(""));
    *out = groove__ivm__graph__GraphBuilder__filter(
        groove__ivm__graph__GraphBuilder__project_fields(arg_max_by_tx(groove__ivm__graph__GraphBuilder__project_fields(ahead, fields)), fields2),
        groove__ivm__op_types__PredicateExpr__is_not_null(CS("_deletion")));
    return NULL;
}

static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__deletion_register_source_for_request(Prep *self, const SReq *request, const jazz_model_TableSchema *table, bool has_tier, jazz_model_DurabilityTier tier,
    bool has_history, jazz_types_GlobalTime history_position, bool has_open_tx, jazz_model_OpenTransactionId open_tx, bool *some, jazz_engine_DeletionRegisterSource *out) {
    *some = false;
    if (!requirements_contains(&request->requirements, JAZZ_SMR_DELETION_MARKERS)) return NULL;
    if (!has_tier) return sre(request, JAZZ_GAP_COVERAGE);
    if (request->visibility != JAZZ_VIS_VISIBLE || has_history || has_open_tx) return sre(request, JAZZ_GAP_COVERAGE);
    const jazz_engine_SourceExpr *e0 = jz_btree_get(&self->read_view->sources, &request->source);
    if (e0 && e0->tag == JAZZ_SE_WITH_OVERLAYS && e0->overlays.entries.len == 1 && e0->overlays.entries.ptr[0].tag == JAZZ_OVL_PENDING_LOCAL) {
        SRE_TRY(jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__pending_deletion_winner_graph(self, request, &out->graph));
        out->row_uuid_field = jz_str_from("row_uuid"); *some = true;
        return NULL;
    }
    if (jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__needs_projected_current_source(self, SL(request->source.table))) {
        SRE_TRY(jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_deletion_register_current_source_graph(self, request, tier, &out->graph));
        out->row_uuid_field = jz_str_from("row_uuid"); *some = true;
        return NULL;
    }
    out->graph = jazz_node__node__query_eval__read_sources__deletion_register_current_source_graph(SL(table->name), tier);
    out->row_uuid_field = jz_str_from("row_uuid"); *some = true;
    return NULL;
}

static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__content_version_source_for_request(Prep *self, const SReq *request, const jazz_model_TableSchema *table, bool has_tier, jazz_model_DurabilityTier tier,
    bool has_history, jazz_types_GlobalTime history_position, bool has_open_tx, jazz_model_OpenTransactionId open_tx, bool *some, jazz_engine_ContentVersionSource *out) {
    *some = false;
    if (!requirements_contains(&request->requirements, JAZZ_SMR_VERSION_PAYLOADS)) return NULL;
    if (!has_tier) return sre(request, JAZZ_GAP_COVERAGE);
    if (request->visibility != JAZZ_VIS_VISIBLE || has_history || has_open_tx) return sre(request, JAZZ_GAP_COVERAGE);
    if (jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__needs_projected_current_source(self, SL(request->source.table))) {
        SRE_TRY(jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_content_current_source_graph(self, request, table, tier, false, false, &out->graph));
        out->row_uuid_field = jz_str_from("row_uuid"); *some = true;
        return NULL;
    }
    out->graph = jazz_node__node__query_eval__read_sources__content_version_current_source_graph(table, tier, false);
    out->row_uuid_field = jz_str_from("row_uuid"); *some = true;
    return NULL;
}

static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_historical_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_types_GlobalTime position, jazz_node_CurrentSourceGraph *out) {
    jazz_node_CurrentRowVec rows;
    if (jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__can_use_bounded_historical_source(self, SL(request->source.table))) {
        self->node->query_engine_read_metrics.source_global_time_range_scans += 1;
        SRE_MAP(jazz_node__node__query_eval__NodeState__bounded_historical_current_rows(self->node, SL(request->source.table), position, &rows), request, JAZZ_GAP_HISTORICAL_STORAGE_CUT);
    } else {
        self->node->query_engine_read_metrics.source_full_scans += 1;
        SRE_MAP(jazz_node__node__source_resolution__NodeState__projected_historical_current_rows(self->node, SL(request->source.table), self->read_view->read_schema, position, &rows), request, JAZZ_GAP_HISTORICAL_STORAGE_CUT);
    }
    SchemaVersionAlias alias;
    jz_err e = jazz_node__node__NodeState__ensure_schema_version_alias(self->node, self->read_view->read_schema, &alias);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); jazz_node_CurrentRowVec_drop(&rows); return sre(request, JAZZ_GAP_HISTORICAL_STORAGE_CUT); }
    /* Historical rows must satisfy the same metadata contract as current and
     * snapshot sources. */
    SRE_MAP(jazz_node__node__query_eval__read_sources__inline_current_graph_with_source_metadata(table, rows, alias, "historical", &request->requirements, &out->graph, &out->descriptor, &out->metadata), request, JAZZ_GAP_HISTORICAL_STORAGE_CUT);
    return NULL;
}

static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_maintained_visible_current_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, jazz_node_CurrentSourceGraph *out) {
    /* The schema-aware projection already retains the complete current version
     * witness tuple. */
    groove_GraphBuilder *projected;
    SRE_TRY(jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_content_current_source_graph(self, request, table, tier, true, true, &projected));
    out->graph = projected;
    out->descriptor = jazz_node__node__query_eval__read_sources__current_row_descriptor(table);
    out->metadata = empty_metadata();
    return NULL;
}

/* How this branch-view source reads its branch layers: the source's access
 * path, when one bounds a branch layer (SPEC 11 11.2). */
static jazz_node_BranchLayerAccess jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__branch_layer_access(const Prep *self, const SReq *request) {
    jazz_node_BranchLayerAccess a; memset(&a, 0, sizeof a);
    const jazz_node_CurrentAccessPath *p = jz_btree_get(&self->access_paths, &request->source);
    if (!p) { a.tag = JAZZ_BLA_FULL; return a; }
    switch (p->tag) {
    case JAZZ_CAP_PRIMARY_KEY: a.tag = JAZZ_BLA_PRIMARY_KEY; valuevec_clone(&a.prefix, &p->prefix); break;
    case JAZZ_CAP_INDEX:
        /* A branch layer is masked before its predicates, so neither a result
         * cap nor another table's candidate set may narrow it. */
        a.tag = JAZZ_BLA_INDEX;
        a.column = jz_str_clone(&p->column);
        a.has_order_column = p->has_order_column; if (p->has_order_column) a.order_column = jz_str_clone(&p->order_column);
        valuevec_clone(&a.prefix, &p->prefix);
        jz_vec_clone(&a.intersections, &p->intersections, sizeof(jazz_node_IndexIntersection), jazz_node_IndexIntersection_clone);
        a.maintained = p->maintained;
        break;
    case JAZZ_CAP_PRIMARY_KEY_PAGE: a.tag = JAZZ_BLA_FULL; break;
    }
    return a;
}
void jazz_node_BranchLayerAccess_drop(jazz_node_BranchLayerAccess *a) {
    switch (a->tag) {
    case JAZZ_BLA_FULL: break;
    case JAZZ_BLA_PRIMARY_KEY: valuevec_drop(&a->prefix); break;
    case JAZZ_BLA_INDEX:
        jz_str_drop(&a->column); if (a->has_order_column) jz_str_drop(&a->order_column);
        valuevec_drop(&a->prefix); jz_vec_drop(&a->intersections, sizeof(jazz_node_IndexIntersection), jazz_node_IndexIntersection_drop);
        break;
    }
}

/* ---------- branch layers, local masks and projected current sources ---------- */

/* PresenceRead: Lookup(row ids) | Whole */
typedef struct { bool lookup; groove_GraphBuilder *row_ids; } PresenceRead;
static PresenceRead presence_read_clone(const PresenceRead *p) { PresenceRead r = { p->lookup, p->lookup ? groove_GraphBuilder_clone(p->row_ids) : NULL }; return r; }
static void presence_read_drop(PresenceRead *p) { if (p->lookup && p->row_ids) groove_GraphBuilder_free(p->row_ids); p->row_ids = NULL; }

static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__presence(const Prep *self, const SReq *request, const jazz_node_Ref *refs, u32 n_refs, const jz_btree *stored_keys, PresenceRead among, groove_GraphBuilder **out);
static PresenceRead jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__local_mask_read(Prep *self, const SReq *request, const jazz_model_TableSchema *table, const jz_btree *stored_keys,
    jz_slice column, const jz_str *order_column, const groove_ValueVec *prefix, const groove_GraphBuilder *origin_candidates);

static jz_sre stored_keys_for(Prep *self, const SReq *request, const jazz_model_BranchKey *key, jz_btree *out) {
    SRE_MAP(jazz_node__node__source_resolution__NodeState__equivalent_stored_branch_keys(self->node, SL(request->source.table), self->read_view->read_schema, key, out), request, JAZZ_GAP_COVERAGE);
    return NULL;
}

static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__live_branch_view_registers(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier,
    const jazz_model_BranchKey *head, const jazz_model_BranchKey *base, groove_GraphBuilder **content_out, groove_GraphBuilder **deletions_out) {
    jazz_node_BranchLayerAccess access = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__branch_layer_access(self, request);
    bool indexed = access.tag == JAZZ_BLA_INDEX;
    /* Narrow the deletion registers to the candidates' row ids only where a
     * lookup can run. */
    bool narrow = indexed && !jazz_node__node__query_eval__read_sources__source_in_recursive_step(request);
    jz_btree head_keys, base_keys; jz_sre s;
    jz_btree_init(&base_keys, &jazz_node_BranchKeySet_desc);
    groove_GraphBuilder *head_content = NULL, *base_content = NULL, *head_deletions = NULL, *base_deletions = NULL, *candidates = NULL;
    if ((s = stored_keys_for(self, request, head, &head_keys))) { jazz_node_BranchLayerAccess_drop(&access); return s; }
    if ((s = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_branch_content_source_graph(self, request, table, tier, &head_keys, &access, &head_content))) goto fail;
    if (!base) {
        if (narrow) candidates = project_row_uuid(groove_GraphBuilder_clone(head_content));
        if ((s = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_branch_deletion_source_graph(self, request, tier, &head_keys, &access, candidates, &head_deletions))) goto fail;
        if (candidates) groove_GraphBuilder_free(candidates);
        *content_out = head_content; *deletions_out = head_deletions;
        jz_btree_drop(&head_keys); jz_btree_drop(&base_keys); jazz_node_BranchLayerAccess_drop(&access);
        return NULL;
    }
    if ((s = stored_keys_for(self, request, base, &base_keys))) goto fail;
    if ((s = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_branch_content_source_graph(self, request, table, tier, &base_keys, &access, &base_content))) goto fail;
    if (narrow) candidates = union2(project_row_uuid(groove_GraphBuilder_clone(head_content)), project_row_uuid(groove_GraphBuilder_clone(base_content)));
    if ((s = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_branch_deletion_source_graph(self, request, tier, &head_keys, &access, candidates, &head_deletions))) goto fail;
    if ((s = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_branch_deletion_source_graph(self, request, tier, &base_keys, &access, candidates, &base_deletions))) goto fail;
    jazz_node_Layer *head_layer;
    if (indexed) {
        if (jazz_node__node__query_eval__read_sources__source_in_recursive_step(request)) {
            /* No lookup: the head's presence reads its whole branch key. */
            if (self->count_access_path_metrics) self->node->query_engine_read_metrics.source_branch_register_full_scans += 1;
        }
        u32 n_refs; const jazz_node_Ref *refs = jazz_node__node__refs__Ref__stack(tier, &n_refs);
        PresenceRead among = { true, project_row_uuid(groove_GraphBuilder_clone(base_content)) };
        groove_GraphBuilder *presence;
        if ((s = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__presence(self, request, refs, n_refs, &head_keys, among, &presence))) goto fail;
        head_layer = jazz_node__node__layer_stack__Layer__masked_by_presence(head_content, presence);
    } else {
        head_layer = jazz_node__node__layer_stack__Layer__new(head_content);
    }
    jazz_node_Layer *content_layers[2] = { head_layer, jazz_node__node__layer_stack__Layer__new(base_content) };
    jazz_node_Layer *deletion_layers[2] = { jazz_node__node__layer_stack__Layer__new(head_deletions), jazz_node__node__layer_stack__Layer__new(base_deletions) };
    *content_out = jazz_node__node__layer_stack__compose(content_layers, 2);
    *deletions_out = jazz_node__node__layer_stack__compose(deletion_layers, 2);
    if (candidates) groove_GraphBuilder_free(candidates);
    jz_btree_drop(&head_keys); jz_btree_drop(&base_keys); jazz_node_BranchLayerAccess_drop(&access);
    return NULL;
fail:
    if (head_content) groove_GraphBuilder_free(head_content);
    if (base_content) groove_GraphBuilder_free(base_content);
    if (head_deletions) groove_GraphBuilder_free(head_deletions);
    if (base_deletions) groove_GraphBuilder_free(base_deletions);
    if (candidates) groove_GraphBuilder_free(candidates);
    jz_btree_drop(&head_keys); jz_btree_drop(&base_keys); jazz_node_BranchLayerAccess_drop(&access);
    return s;
}

/* How the local delta masks origin under an index prefix: by its whole
 * presence or by a lookup of origin's candidates, whichever reads fewer rows.
 * `P` (pending) is summed over the read's stored keys; `C` is counted from
 * origin's index keys under the prefix, only up to `P`. Ties and failures read
 * the delta whole; a failure counts local_mask_count_fallbacks. */
static PresenceRead jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__local_mask_read(Prep *self, const SReq *request, const jazz_model_TableSchema *table, const jz_btree *stored_keys,
    jz_slice column, const jz_str *order_column, const groove_ValueVec *prefix, const groove_GraphBuilder *origin_candidates) {
    PresenceRead whole = { false, NULL };
    if (jazz_node__node__query_eval__read_sources__source_in_recursive_step(request)) return whole;
    jazz_types_SchemaVersionId read_schema = self->read_view->read_schema;
    jazz_types_PhysicalTableId table_id;
    jz_err e = jazz_node__node__physical__NodeState__physical_table_id_for_schema(self->node, read_schema, SL(table->name), &table_id);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); self->node->query_engine_read_metrics.local_mask_count_fallbacks += 1; return whole; }
    /* `Whole` reads the delta under the read's own branch keys only. */
    usize pending = 0;
    jazz_model_BranchKey *branch_key; void *_v; JZ_BTREE_FOR(stored_keys, branch_key, _v) {
        pending += jazz_node__node__AheadCurrentKeys__branch_rows(self->node->ahead_current_keys, table_id, branch_key);
    }
    usize settled = 0;
    jazz_model_BranchKey *branch_key2; void *_v2; JZ_BTREE_FOR(stored_keys, branch_key2, _v2) {
        if (settled >= pending) return whole;
        u32 rows;
        e = jazz_node__node__query_eval__read_sources__NodeState__physical_current_index_prefix_rows_up_to(self->node, JAZZ_PCC_GLOBAL, table, read_schema, branch_key2, column, order_column, prefix, (u32)(pending - settled), &rows);
        if (JZ_IS_ERR(e)) {
            /* Exact either way, but a mapping or index-name mismatch here
             * would silently cost every read the whole delta. */
            jz_err_drop(&e);
            self->node->query_engine_read_metrics.local_mask_count_fallbacks += 1;
            return whole;
        }
        settled += rows;
    }
    if (settled < pending) {
        PresenceRead lookup = { true, project_row_uuid(groove_GraphBuilder_clone(origin_candidates)) };
        return lookup;
    }
    return whole;
}

/* The row_uuids a branch key holds in `refs`, whatever their values: a
 * layer_stack presence mask. Lookup reads it only for the given row ids by a
 * keyed lookup; Whole reads the refs' rows whole. A source inside a recursive
 * step cannot look rows up, so there every presence is read whole. */
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__presence(const Prep *self, const SReq *request, const jazz_node_Ref *refs, u32 n_refs, const jz_btree *stored_keys, PresenceRead among, groove_GraphBuilder **out) {
    groove_GraphBuilder *row_ids = NULL;
    if (among.lookup && !jazz_node__node__query_eval__read_sources__source_in_recursive_step(request)) row_ids = among.row_ids;
    else presence_read_drop(&among);
    JZ_VEC(groove_GraphBuilder *) rows = {0};
    for (u32 i = 0; i < n_refs; i++) {
        jazz_model_BranchKey *branch_key; void *_v; JZ_BTREE_FOR(stored_keys, branch_key, _v) {
            groove_GraphBuilder *marker;
            jz_err e = jazz_node__node__physical__projections__NodeState__physical_current_marker_source_graph(self->node, self->read_view->read_schema, SL(request->source.table),
                jazz_node__node__refs__Ref__physical_class(refs[i]), branch_key, NULL, &marker);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); goto fail; }
            groove_GraphBuilder *present = marker;
            if (row_ids) {
                e = jazz_node__node__query_eval__read_sources__lookup_by_row_id(marker, groove_GraphBuilder_clone(row_ids), &present);
                if (JZ_IS_ERR(e)) { jz_err_drop(&e); goto fail; }
            }
            JZ_VEC_PUSH(rows, project_row_uuid(present));
        }
    }
    if (row_ids) groove_GraphBuilder_free(row_ids);
    *out = groove__ivm__graph__GraphBuilder__union(rows.ptr, rows.len);
    jz_free(rows.ptr);
    return NULL;
fail:
    if (row_ids) groove_GraphBuilder_free(row_ids);
    for (u32 i = 0; i < rows.len; i++) groove_GraphBuilder_free(rows.ptr[i]);
    jz_free(rows.ptr);
    return sre(request, JAZZ_GAP_SCHEMA_PROJECTION);
}

/* class_rows: one class's rows under every stored spelling of the key. */
static jz_err branch_class_rows(Prep *self, const SReq *request, const jazz_model_TableSchema *table, const jz_btree *stored_keys, const jazz_node_BranchLayerAccess *access,
    jazz_node_PhysicalCurrentClass cls, const jz_str *projection_target, groove_GraphBuilder **out) {
    JZ_VEC(groove_GraphBuilder *) parts = {0};
    jazz_model_BranchKey *branch_key; void *_v; JZ_BTREE_FOR(stored_keys, branch_key, _v) {
        groove_GraphBuilder *g; jz_err e;
        switch (access->tag) {
        case JAZZ_BLA_FULL:
            e = jazz_node__node__physical__projections__NodeState__physical_current_branch_source_graph_with_projection_target(self->node, self->read_view->read_schema, SL(request->source.table), cls, jz_str_clone(projection_target), branch_key, NULL, &g);
            break;
        case JAZZ_BLA_PRIMARY_KEY: {
            groove_StaticScanSpec scan = jazz_node__node__query_eval__read_sources__row_prefix_scan(&access->prefix);
            e = jazz_node__node__physical__projections__NodeState__physical_current_branch_source_graph_with_projection_target(self->node, self->read_view->read_schema, SL(request->source.table), cls, jz_str_clone(projection_target), branch_key, &scan, &g);
            break;
        }
        default:
            e = jazz_node__node__query_eval__read_sources__NodeState__physical_current_source_for_index_scan_in(self->node, cls, table, self->read_view->read_schema, branch_key,
                SL(access->column), access->has_order_column ? &access->order_column : NULL, false, &access->prefix, &access->intersections, access->maintained, false, 0, NULL, SL(*projection_target), &g);
            break;
        }
        if (JZ_IS_ERR(e)) { for (u32 i = 0; i < parts.len; i++) groove_GraphBuilder_free(parts.ptr[i]); jz_free(parts.ptr); return e; }
        JZ_VEC_PUSH(parts, g);
    }
    *out = groove__ivm__graph__GraphBuilder__union(parts.ptr, parts.len);
    jz_free(parts.ptr);
    return JZ_OK;
}

typedef struct {
    Prep *self; const SReq *request; const jazz_model_TableSchema *table; const jz_btree *stored_keys; const jazz_node_BranchLayerAccess *access;
    const jz_str *projection_target; const jz_vec_str *physical_fields; groove_GraphBuilder *origin; const PresenceRead *local_mask; jz_sre err;
} content_layer_ctx;
static void *content_layer_fn(void *vctx, jazz_node_Ref r, bool *failed) {
    content_layer_ctx *c = vctx;
    jazz_node_Ref origin = { JAZZ_REF_REMOTE, JAZZ_REMOTE_ORIGIN };
    if (jazz_node_Ref_eq(r, origin)) return jazz_node__node__layer_stack__Layer__new(groove_GraphBuilder_clone(c->origin));
    groove_GraphBuilder *rows;
    jz_err e = branch_class_rows(c->self, c->request, c->table, c->stored_keys, c->access, jazz_node__node__refs__Ref__physical_class(r), c->projection_target, &rows);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); c->err = sre(c->request, JAZZ_GAP_SCHEMA_PROJECTION); *failed = true; return NULL; }
    rows = groove__ivm__graph__GraphBuilder__project(rows, c->physical_fields->ptr, c->physical_fields->len);
    /* Under an index prefix the local delta masks origin by its presence,
     * never by its rows under the prefix: a pending edit that moved a row out
     * of the prefix still hides its origin image. */
    if (c->access->tag == JAZZ_BLA_INDEX && c->local_mask) {
        groove_GraphBuilder *presence;
        jz_sre s = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__presence(c->self, c->request, &r, 1, c->stored_keys, presence_read_clone(c->local_mask), &presence);
        if (s) { groove_GraphBuilder_free(rows); c->err = s; *failed = true; return NULL; }
        return jazz_node__node__layer_stack__Layer__masked_by_presence(rows, presence);
    }
    return jazz_node__node__layer_stack__Layer__new(rows);
}

/* Build the maintained content-winner relation for one logical branch key:
 * its [local, origin] ref stack, each ref read under `access`. `stored_keys`
 * includes historical short spellings; they compete as one branch-local row. */
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_branch_content_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier,
    const jz_btree *stored_keys, const jazz_node_BranchLayerAccess *access, groove_GraphBuilder **out) {
    jz_btree required_fields = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__current_projection_required_fields(self, request, table);
    jz_str projection_target; jz_vec_str physical_fields = {0};
    jz_err e = jazz_node__node__physical__projections__NodeState__ensure_physical_current_winner_projection(self->node, self->read_view->read_schema, SL(request->source.table), &projection_target, &physical_fields);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); jz_btree_drop(&required_fields); return sre(request, JAZZ_GAP_SCHEMA_PROJECTION); }
    groove_ProjectFieldVec post_winner_fields;
    e = jazz_node__node__physical__projections__NodeState__physical_current_post_winner_projection_fields(self->node, self->read_view->read_schema, SL(request->source.table), &required_fields, &post_winner_fields);
    jz_btree_drop(&required_fields);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); jz_str_drop(&projection_target); strvec_drop(&physical_fields); return sre(request, JAZZ_GAP_SCHEMA_PROJECTION); }
    u32 n_refs; (void)jazz_node__node__refs__Ref__stack(tier, &n_refs);
    u64 refs = n_refs, keys = stored_keys->len;
    if (self->count_access_path_metrics) {
        switch (access->tag) {
        case JAZZ_BLA_FULL: self->node->query_engine_read_metrics.source_full_scans += 1; break;
        case JAZZ_BLA_PRIMARY_KEY: self->node->query_engine_read_metrics.source_primary_key_scans += 1; break;
        default:
            /* Each ref under the prefix; the local delta's mask over origin is
             * its presence, not an index probe. */
            self->node->query_engine_read_metrics.source_index_probes += refs * keys * (1 + (u64)access->intersections.len);
            break;
        }
    }
    jazz_node_Ref origin_ref = { JAZZ_REF_REMOTE, JAZZ_REMOTE_ORIGIN };
    groove_GraphBuilder *origin;
    e = branch_class_rows(self, request, table, stored_keys, access, jazz_node__node__refs__Ref__physical_class(origin_ref), &projection_target, &origin);
    jz_sre s = NULL;
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); s = sre(request, JAZZ_GAP_SCHEMA_PROJECTION); goto done; }
    origin = groove__ivm__graph__GraphBuilder__project(origin, physical_fields.ptr, physical_fields.len);
    /* Under an index prefix the local delta masks origin by its presence, read
     * whichever way costs less here (see local_mask_read). */
    PresenceRead local_mask; bool has_local_mask = false;
    if (access->tag == JAZZ_BLA_INDEX && tier != JAZZ_TIER_GLOBAL) {
        local_mask = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__local_mask_read(self, request, table, stored_keys, SL(access->column),
            access->has_order_column ? &access->order_column : NULL, &access->prefix, origin);
        has_local_mask = true;
    }
    content_layer_ctx ctx = { self, request, table, stored_keys, access, &projection_target, &physical_fields, origin, has_local_mask ? &local_mask : NULL, NULL };
    bool failed = false;
    jazz_node_LayerVec layers = jazz_node__node__layer_stack__try_ref_layers(tier, content_layer_fn, &ctx, &failed);
    groove_GraphBuilder_free(origin);
    if (has_local_mask) presence_read_drop(&local_mask);
    if (failed) { s = ctx.err; goto done; }
    groove_GraphBuilder *content = jazz_node__node__layer_stack__compose(layers.ptr, layers.len);
    jz_free(layers.ptr);
    if (tier != JAZZ_TIER_GLOBAL) content = groove__ivm__graph__GraphBuilder__project(content, physical_fields.ptr, physical_fields.len);
    *out = groove__ivm__graph__GraphBuilder__project_fields(content, post_winner_fields);
    post_winner_fields.ptr = NULL; post_winner_fields.len = 0;
done:
    groove_ProjectFieldVec_drop(&post_winner_fields);
    jz_str_drop(&projection_target); strvec_drop(&physical_fields);
    return s;
}

typedef struct {
    Prep *self; const SReq *request; const jz_btree *stored_keys; const groove_StaticScanSpec *scan; const groove_GraphBuilder *row_ids; const jz_vec_str *fields; jz_sre err;
} deletion_layer_ctx;
static void *deletion_layer_fn(void *vctx, jazz_node_Ref r, bool *failed) {
    deletion_layer_ctx *c = vctx;
    JZ_VEC(groove_GraphBuilder *) parts = {0};
    jazz_model_BranchKey *branch_key; void *_v; JZ_BTREE_FOR(c->stored_keys, branch_key, _v) {
        groove_GraphBuilder *rows;
        jz_err e = jazz_node__node__physical__projections__NodeState__physical_current_marker_source_graph(c->self->node, c->self->read_view->read_schema, SL(c->request->source.table),
            jazz_node__node__refs__Ref__physical_class(r), branch_key, c->scan, &rows);
        if (!JZ_IS_ERR(e) && c->row_ids) e = jazz_node__node__query_eval__read_sources__lookup_by_row_id(rows, groove_GraphBuilder_clone(c->row_ids), &rows);
        if (JZ_IS_ERR(e)) {
            jz_err_drop(&e);
            for (u32 i = 0; i < parts.len; i++) groove_GraphBuilder_free(parts.ptr[i]);
            jz_free(parts.ptr);
            c->err = sre(c->request, JAZZ_GAP_SCHEMA_PROJECTION); *failed = true; return NULL;
        }
        JZ_VEC_PUSH(parts, rows);
    }
    groove_GraphBuilder *u = groove__ivm__graph__GraphBuilder__union(parts.ptr, parts.len);
    jz_free(parts.ptr);
    return jazz_node__node__layer_stack__Layer__new(groove__ivm__graph__GraphBuilder__project(u, c->fields->ptr, c->fields->len));
}

/* Build the maintained deletion-register winner relation for one logical
 * branch key. A primary-key `access` narrows every layer by its prefix;
 * `row_ids` narrows every layer to those rows by a keyed lookup. */
static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_branch_deletion_source_graph(Prep *self, const SReq *request, jazz_model_DurabilityTier tier,
    const jz_btree *stored_keys, const jazz_node_BranchLayerAccess *access, const groove_GraphBuilder *row_ids, groove_GraphBuilder **out) {
    jz_vec_str fields = {0};
    strvec_push_cstr(&fields, "branch_key");
    jz_vec_str reg = jazz_node__node__query_eval__read_sources__register_storage_field_names();
    for (u32 i = 0; i < reg.len; i++) jz_vec_push(&fields, &reg.ptr[i], sizeof(jz_str));
    jz_free(reg.ptr);
    groove_StaticScanSpec scan_v; const groove_StaticScanSpec *scan = NULL;
    if (access->tag == JAZZ_BLA_PRIMARY_KEY) { scan_v = jazz_node__node__query_eval__read_sources__row_prefix_scan(&access->prefix); scan = &scan_v; }
    else if (!row_ids) {
        /* Neither a prefix nor a lookup: the register reads the whole branch key. */
        if (self->count_access_path_metrics) self->node->query_engine_read_metrics.source_branch_register_full_scans += 1;
    }
    /* Physical current tables are schema-variant; read them through the
     * current projection. Deletion is a cell of the winning row image. */
    deletion_layer_ctx ctx = { self, request, stored_keys, scan, row_ids, &fields, NULL };
    bool failed = false;
    jazz_node_LayerVec layers = jazz_node__node__layer_stack__try_ref_layers(tier, deletion_layer_fn, &ctx, &failed);
    if (failed) { strvec_drop(&fields); return ctx.err; }
    groove_GraphBuilder *reg_g = jazz_node__node__layer_stack__compose(layers.ptr, layers.len);
    jz_free(layers.ptr);
    if (tier != JAZZ_TIER_GLOBAL) reg_g = groove__ivm__graph__GraphBuilder__project(reg_g, fields.ptr, fields.len);
    strvec_drop(&fields);
    *out = groove__ivm__graph__GraphBuilder__filter(reg_g, groove__ivm__op_types__PredicateExpr__is_not_null(CS("_deletion")));
    return NULL;
}

static groove_GraphBuilder *finish_content(groove_GraphBuilder *content, bool exclude_deleted, jz_vec_str *fields) {
    if (!exclude_deleted) return groove__ivm__graph__GraphBuilder__project(content, fields->ptr, fields->len);
    return groove__ivm__graph__GraphBuilder__filter(content, jazz_node__node__codec__not_deleted_predicate());
}

static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_content_current_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *read_table, jazz_model_DurabilityTier tier,
    bool include_global_time, bool exclude_deleted, groove_GraphBuilder **out) {
    jz_vec_str fields = jazz_node__node__query_eval__read_sources__global_current_storage_fields(read_table, true, include_global_time);
    /* Unfiltered callers receive the image's deletion cell. */
    if (!exclude_deleted) strvec_push_cstr(&fields, "_deletion");
    jz_sre s = NULL; jz_err e;
    jazz_types_SchemaVersionId schema = self->read_view->read_schema;
    jz_slice tname = SL(request->source.table);
    /* Global current storage has already selected the physical winner. Apply
     * the ordinary lens-aware projection directly so added-column defaults
     * survive. Local reads still need to choose between Global and Ahead
     * candidates before their compatibility boundary. */
    if (tier == JAZZ_TIER_GLOBAL) {
        jz_str projection_target;
        if ((s = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__current_projection_target(self, request, read_table, &projection_target))) { strvec_drop(&fields); return s; }
        const jazz_node_CurrentAccessPath *ap = jz_btree_get(&self->access_paths, &request->source);
        groove_GraphBuilder *content;
        u32 gap = JAZZ_GAP_SCHEMA_PROJECTION;
        if (ap && ap->tag == JAZZ_CAP_PRIMARY_KEY) {
            if (self->count_access_path_metrics) self->node->query_engine_read_metrics.source_primary_key_scans += 1;
            groove_ValueVec prefix; valuevec_clone(&prefix, &ap->prefix);
            e = jazz_node__node__physical__projections__NodeState__physical_current_source_scan_graph_with_projection_target(self->node, schema, tname, JAZZ_PCC_GLOBAL, projection_target,
                jazz_node__node__query_eval__normalization__static_scan_for_prefix(prefix, 1), &content);
        } else if (ap && ap->tag == JAZZ_CAP_PRIMARY_KEY_PAGE) {
            e = jazz_node__node__physical__projections__NodeState__physical_current_source_scan_graph_with_projection_target(self->node, schema, tname, JAZZ_PCC_GLOBAL, projection_target,
                jazz_node__node__query_eval__read_sources__primary_key_page_scan(ap->cap), &content);
        } else if (ap && ap->tag == JAZZ_CAP_INDEX) {
            /* An ordered-page probe re-proves its page after the deletion
             * anti-join, so its cap survives it. */
            bool has_limit = (ap->has_order_column || !exclude_deleted) && ap->has_source_limit;
            self->node->query_engine_read_metrics.source_index_probes += 1 + (u64)ap->intersections.len + (ap->candidate_filter ? 1 : 0);
            e = jazz_node__node__query_eval__read_sources__NodeState__physical_global_current_source_for_index_scan(self->node, read_table, schema, SL(ap->column),
                ap->has_order_column ? &ap->order_column : NULL, ap->reverse, &ap->prefix, &ap->intersections, ap->maintained, has_limit, ap->source_limit, ap->candidate_filter, SL(projection_target), &content);
            jz_str_drop(&projection_target);
            gap = JAZZ_GAP_COVERAGE;
        } else {
            self->node->query_engine_read_metrics.source_full_scans += 1;
            e = jazz_node__node__physical__projections__NodeState__physical_current_source_graph_with_projection_target(self->node, schema, tname, JAZZ_PCC_GLOBAL, projection_target, &content);
        }
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); strvec_drop(&fields); return sre(request, gap); }
        /* The settled image is the winner; its _deletion cell decides. */
        *out = finish_content(content, exclude_deleted, &fields);
        strvec_drop(&fields);
        return NULL;
    }
    jz_btree required_fields = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__current_projection_required_fields(self, request, read_table);
    jz_str projection_target; jz_vec_str physical_fields = {0};
    e = jazz_node__node__physical__projections__NodeState__ensure_physical_current_winner_projection(self->node, schema, tname, &projection_target, &physical_fields);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); jz_btree_drop(&required_fields); strvec_drop(&fields); return sre(request, JAZZ_GAP_SCHEMA_PROJECTION); }
    groove_ProjectFieldVec post_winner_fields = {0};
    e = jazz_node__node__physical__projections__NodeState__physical_current_post_winner_projection_fields(self->node, schema, tname, &required_fields, &post_winner_fields);
    jz_btree_drop(&required_fields);
    groove_GraphBuilder *global = NULL, *ahead = NULL;
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); s = sre(request, JAZZ_GAP_SCHEMA_PROJECTION); goto done; }
    groove_RecordDescriptor raw_global_output;
    {
        jazz_types_PhysicalTableId table_id;
        e = jazz_node__node__physical__NodeState__physical_table_id_for_schema(self->node, schema, tname, &table_id);
        if (!JZ_IS_ERR(e)) {
            jz_str name = jazz_node__node__physical__physical_global_current_table_name(table_id);
            const groove_TableSchema *ts;
            e = self->node->database.vt->table_schema(self->node->database.self, SL(name), &ts);
            jz_str_drop(&name);
            if (!JZ_IS_ERR(e)) raw_global_output = groove__schema__TableSchema__record_schema(ts);
        }
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); s = sre(request, JAZZ_GAP_SCHEMA_PROJECTION); goto done; }
    }
    const jazz_node_CurrentAccessPath *access_path = jz_btree_get(&self->access_paths, &request->source);
    u32 gap = JAZZ_GAP_SCHEMA_PROJECTION;
    if (access_path && access_path->tag == JAZZ_CAP_PRIMARY_KEY) {
        if (self->count_access_path_metrics) self->node->query_engine_read_metrics.source_primary_key_scans += 1;
        groove_ValueVec prefix; valuevec_clone(&prefix, &access_path->prefix);
        e = jazz_node__node__physical__projections__NodeState__physical_current_source_scan_graph_with_projection_target(self->node, schema, tname, JAZZ_PCC_GLOBAL, jz_str_clone(&projection_target),
            jazz_node__node__query_eval__normalization__static_scan_for_prefix(prefix, 1), &global);
    } else if (access_path && access_path->tag == JAZZ_CAP_PRIMARY_KEY_PAGE) {
        e = jazz_node__node__physical__projections__NodeState__physical_current_source_scan_graph_with_projection_target(self->node, schema, tname, JAZZ_PCC_GLOBAL, jz_str_clone(&projection_target),
            jazz_node__node__query_eval__read_sources__primary_key_page_scan(access_path->cap), &global);
    } else if (access_path && access_path->tag == JAZZ_CAP_INDEX) {
        /* A Local winner combines these settled candidates with the ahead
         * overlay, so a settled link outside the candidate prefix may still be
         * needed (#3340). Never filter the settled side by another table's
         * index. */
        bool has_limit = !exclude_deleted && access_path->has_source_limit;
        self->node->query_engine_read_metrics.source_index_probes += 1 + (u64)access_path->intersections.len;
        e = jazz_node__node__query_eval__read_sources__NodeState__physical_global_current_source_for_index_scan_with_output(self->node, read_table, schema, SL(access_path->column),
            access_path->has_order_column ? &access_path->order_column : NULL, access_path->reverse, &access_path->prefix, &access_path->intersections, access_path->maintained,
            has_limit, access_path->source_limit, NULL, SL(projection_target), raw_global_output, &global);
        gap = JAZZ_GAP_COVERAGE;
    } else {
        self->node->query_engine_read_metrics.source_full_scans += 1;
        e = jazz_node__node__physical__projections__NodeState__physical_current_source_graph_with_projection_target(self->node, schema, tname, JAZZ_PCC_GLOBAL, jz_str_clone(&projection_target), &global);
    }
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); s = sre(request, gap); goto done; }
    groove_GraphBuilder *excluded;
    if ((s = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__exclude_settled_arm(self, request, global, false, &excluded))) { global = NULL; goto done; }
    global = excluded;
    groove_GraphBuilder *content;
    if (tier == JAZZ_TIER_GLOBAL) {
        content = global; global = NULL;
    } else {
        global = groove__ivm__graph__GraphBuilder__project(global, physical_fields.ptr, physical_fields.len);
        /* An unlimited index read stays index-bounded on the local delta's
         * rows: delta rows under the prefix join the result, and the delta's
         * presence names the settled candidates they replace. */
        if (access_path && access_path->tag == JAZZ_CAP_INDEX && !(!exclude_deleted && access_path->has_source_limit)) {
            jazz_model_BranchKey default_key; memset(&default_key, 0, sizeof default_key);
            self->node->query_engine_read_metrics.source_index_probes += 1 + (u64)access_path->intersections.len;
            e = jazz_node__node__query_eval__read_sources__NodeState__physical_current_source_for_index_scan_in(self->node, JAZZ_PCC_AHEAD, read_table, schema, &default_key,
                SL(access_path->column), access_path->has_order_column ? &access_path->order_column : NULL, access_path->reverse, &access_path->prefix, &access_path->intersections,
                access_path->maintained, false, 0, NULL, SL(projection_target), &ahead);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); s = sre(request, JAZZ_GAP_SCHEMA_PROJECTION); goto done; }
            jz_btree stored_keys; jz_btree_init(&stored_keys, &jazz_node_BranchKeySet_desc);
            jazz_model_BranchKey k; memset(&k, 0, sizeof k);
            jz_btree_insert(&stored_keys, &k, NULL, NULL);
            PresenceRead among = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__local_mask_read(self, request, read_table, &stored_keys, SL(access_path->column),
                access_path->has_order_column ? &access_path->order_column : NULL, &access_path->prefix, global);
            jazz_node_Ref local = { JAZZ_REF_LOCAL, 0 };
            groove_GraphBuilder *presence;
            s = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__presence(self, request, &local, 1, &stored_keys, among, &presence);
            jz_btree_drop(&stored_keys);
            if (s) goto done;
            if ((s = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__exclude_settled_arm(self, request, ahead, true, &excluded))) { ahead = NULL; groove_GraphBuilder_free(presence); goto done; }
            ahead = groove__ivm__graph__GraphBuilder__project(excluded, physical_fields.ptr, physical_fields.len);
            jazz_node_Layer *layers[2] = { jazz_node__node__layer_stack__Layer__masked_by_presence(ahead, presence), jazz_node__node__layer_stack__Layer__new(global) };
            ahead = NULL; global = NULL;
            content = groove__ivm__graph__GraphBuilder__project(jazz_node__node__layer_stack__compose(layers, 2), physical_fields.ptr, physical_fields.len);
            content = groove__ivm__graph__GraphBuilder__project_fields(content, post_winner_fields);
            post_winner_fields.ptr = NULL; post_winner_fields.len = 0;
            *out = finish_content(content, exclude_deleted, &fields);
            goto done;
        }
        if (access_path && access_path->tag == JAZZ_CAP_PRIMARY_KEY) {
            groove_ValueVec prefix; valuevec_clone(&prefix, &access_path->prefix);
            e = jazz_node__node__physical__projections__NodeState__physical_current_source_scan_graph_with_projection_target(self->node, schema, tname, JAZZ_PCC_AHEAD, jz_str_clone(&projection_target),
                jazz_node__node__query_eval__normalization__static_scan_for_prefix(prefix, 3), &ahead);
        } else {
            if (access_path && access_path->tag == JAZZ_CAP_INDEX) {
                /* A Local-ahead winner can change the indexed column and
                 * therefore no longer be present under the settled candidate's
                 * prefix. Scan the ahead current table in full before arg-max. */
                self->node->query_engine_read_metrics.source_full_scans += 1;
            }
            e = jazz_node__node__physical__projections__NodeState__physical_current_source_graph_with_projection_target(self->node, schema, tname, JAZZ_PCC_AHEAD, jz_str_clone(&projection_target), &ahead);
        }
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); ahead = NULL; s = sre(request, JAZZ_GAP_SCHEMA_PROJECTION); goto done; }
        if ((s = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__exclude_settled_arm(self, request, ahead, true, &excluded))) { ahead = NULL; goto done; }
        ahead = groove__ivm__graph__GraphBuilder__project(excluded, physical_fields.ptr, physical_fields.len);
        jazz_node_Layer *layers[2] = { jazz_node__node__layer_stack__Layer__new(ahead), jazz_node__node__layer_stack__Layer__new(global) };
        ahead = NULL; global = NULL;
        content = groove__ivm__graph__GraphBuilder__project(jazz_node__node__layer_stack__compose(layers, 2), physical_fields.ptr, physical_fields.len);
    }
    content = groove__ivm__graph__GraphBuilder__project_fields(content, post_winner_fields);
    post_winner_fields.ptr = NULL; post_winner_fields.len = 0;
    /* Deletion is a cell of the winning image selected above. */
    *out = finish_content(content, exclude_deleted, &fields);
done:
    if (global) groove_GraphBuilder_free(global);
    if (ahead) groove_GraphBuilder_free(ahead);
    groove_ProjectFieldVec_drop(&post_winner_fields);
    jz_str_drop(&projection_target); strvec_drop(&physical_fields); strvec_drop(&fields);
    return s;
}

typedef struct { Prep *self; const SReq *request; const groove_ProjectFieldVec *fields; jz_sre err; } marker_layer_ctx;
static void *marker_layer_fn(void *vctx, jazz_node_Ref r, bool *failed) {
    marker_layer_ctx *c = vctx;
    jazz_model_BranchKey default_key; memset(&default_key, 0, sizeof default_key);
    groove_GraphBuilder *rows;
    jz_err e = jazz_node__node__physical__projections__NodeState__physical_current_marker_source_graph(c->self->node, c->self->read_view->read_schema, SL(c->request->source.table),
        jazz_node__node__refs__Ref__physical_class(r), &default_key, NULL, &rows);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); c->err = sre(c->request, JAZZ_GAP_SCHEMA_PROJECTION); *failed = true; return NULL; }
    return jazz_node__node__layer_stack__Layer__new(groove__ivm__graph__GraphBuilder__project_fields(rows, groove_ProjectFieldVec_clone(c->fields)));
}

static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_deletion_register_current_source_graph(Prep *self, const SReq *request, jazz_model_DurabilityTier tier, groove_GraphBuilder **out) {
    if (tier == JAZZ_TIER_GLOBAL && self->has_bounded_deletion_register && jazz_engine_SourceId_eq(&self->bounded_source, &request->source)) {
        *out = groove_GraphBuilder_clone(self->bounded_register);
        return NULL;
    }
    jazz_types_PhysicalTableId table_id;
    SRE_MAP(jazz_node__node__physical__NodeState__physical_table_id_for_schema(self->node, self->read_view->read_schema, SL(request->source.table), &table_id), request, JAZZ_GAP_SCHEMA_PROJECTION);
    (void)table_id;
    groove_ProjectFieldVec fields = jazz_node__node__query_eval__read_sources__register_storage_fields_for_query_engine(CS(""));
    marker_layer_ctx ctx = { self, request, &fields, NULL };
    bool failed = false;
    jazz_node_LayerVec layers = jazz_node__node__layer_stack__try_ref_layers(tier, marker_layer_fn, &ctx, &failed);
    if (failed) { groove_ProjectFieldVec_drop(&fields); return ctx.err; }
    groove_GraphBuilder *reg = jazz_node__node__layer_stack__compose(layers.ptr, layers.len);
    jz_free(layers.ptr);
    if (tier != JAZZ_TIER_GLOBAL) reg = groove__ivm__graph__GraphBuilder__project_fields(reg, fields);
    else groove_ProjectFieldVec_drop(&fields);
    *out = groove__ivm__graph__GraphBuilder__filter(reg, groove__ivm__op_types__PredicateExpr__is_not_null(CS("_deletion")));
    return NULL;
}

static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_visible_current_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, jazz_node_CurrentSourceGraph *out) {
    groove_GraphBuilder *g;
    SRE_TRY(jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_content_current_source_graph(self, request, table, tier, false, true, &g));
    /* Project heterogeneous physical rows at the source boundary so the rest
     * of the query graph retains the requested logical descriptor. */
    out->graph = groove__ivm__graph__GraphBuilder__project_fields(g, jazz_node__node__query_eval__read_sources__storage_to_canonical_current_source_fields(table, true, false));
    out->descriptor = jazz_node__node__query_eval__read_sources__current_row_descriptor(table);
    out->metadata = empty_metadata();
    return NULL;
}

static jz_sre jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_include_deleted_current_source_graph(Prep *self, const SReq *request, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, groove_GraphBuilder **out) {
    groove_ProjectFieldVec fields = jazz_node__node__query_eval__read_sources__storage_to_canonical_current_source_fields(table, true, false);
    JZ_VEC_PUSH(fields, groove__ivm__graph__ProjectField__named(CS("_deletion")));
    groove_GraphBuilder *content;
    jz_sre s = jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__projected_content_current_source_graph(self, request, table, tier, false, false, &content);
    if (s) { groove_ProjectFieldVec_drop(&fields); return s; }
    content = groove__ivm__graph__GraphBuilder__project_fields(content, fields);
    *out = jazz_node__node__query_eval__read_sources__include_deleted_from_images(table, content);
    return NULL;
}

bool jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__can_use_bounded_historical_source(const Prep *self, jz_slice table) {
    if (jz_uuid_cmp(&self->read_view->read_schema.u, &self->node->catalogue.local_schema_version_id.u) != 0) return false;
    jazz_types_PhysicalTableId id;
    jz_err e = jazz_node__node__physical__NodeState__physical_table_id_for_schema(self->node, self->read_view->read_schema, table, &id);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    return true;
}

bool jazz_node__node__query_eval__read_sources__JazzSourceGraphPreparer__needs_projected_current_source(Prep *self, jz_slice table) {
    jazz_types_PhysicalTableId id;
    jz_err e = jazz_node__node__physical__NodeState__physical_table_id_for_schema(self->node, self->read_view->read_schema, table, &id);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    return true;
}

/* ---------- free source-graph helpers ---------- */

static groove_GraphBuilder *jazz_node__node__query_eval__read_sources__deletion_register_current_source_graph(jz_slice table, jazz_model_DurabilityTier tier) {
    groove_ProjectFieldVec fields = jazz_node__node__query_eval__read_sources__register_storage_fields_for_query_engine(CS(""));
    jz_str gname = jazz_node__node__codec__global_current_table_name(table);
    groove_GraphBuilder *global = groove__ivm__graph__GraphBuilder__project_fields(groove__ivm__graph__GraphBuilder__table(SL(gname)), groove_ProjectFieldVec_clone(&fields));
    jz_str_drop(&gname);
    groove_GraphBuilder *current;
    if (tier == JAZZ_TIER_GLOBAL) {
        current = global;
        groove_ProjectFieldVec_drop(&fields);
    } else {
        jz_str aname = jazz_node__node__codec__ahead_current_table_name(table);
        groove_GraphBuilder *ahead = groove__ivm__graph__GraphBuilder__project_fields(groove__ivm__graph__GraphBuilder__table(SL(aname)), groove_ProjectFieldVec_clone(&fields));
        jz_str_drop(&aname);
        current = groove__ivm__graph__GraphBuilder__project_fields(arg_max_by_tx(union2(global, ahead)), fields);
    }
    return groove__ivm__graph__GraphBuilder__filter(current, groove__ivm__op_types__PredicateExpr__is_not_null(CS("_deletion")));
}

static groove_GraphBuilder *jazz_node__node__query_eval__read_sources__content_version_current_source_graph(const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, bool include_global_time) {
    jz_vec_str fields = jazz_node__node__query_eval__read_sources__maintained_view_history_storage_field_names(table);
    if (include_global_time) strvec_push_cstr(&fields, "global_time");
    jz_str gname = jazz_node__node__codec__global_current_table_name(SL(table->name));
    groove_GraphBuilder *out;
    if (tier == JAZZ_TIER_GLOBAL) {
        out = groove__ivm__graph__GraphBuilder__project(groove__ivm__graph__GraphBuilder__table(SL(gname)), fields.ptr, fields.len);
    } else {
        jz_str aname = jazz_node__node__codec__ahead_current_table_name(SL(table->name));
        groove_GraphBuilder *ahead = groove__ivm__graph__GraphBuilder__project(groove__ivm__graph__GraphBuilder__table(SL(aname)), fields.ptr, fields.len);
        groove_GraphBuilder *global = groove__ivm__graph__GraphBuilder__project(groove__ivm__graph__GraphBuilder__table(SL(gname)), fields.ptr, fields.len);
        jz_str_drop(&aname);
        out = groove__ivm__graph__GraphBuilder__project(arg_max_by_tx(union2(global, ahead)), fields.ptr, fields.len);
    }
    jz_str_drop(&gname); strvec_drop(&fields);
    return out;
}

static groove_GraphBuilder *jazz_node__node__query_eval__read_sources__selected_visible_current_primary_key_graph(const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, groove_ValueVec prefix) {
    jz_vec_str content_fields = {0};
    strvec_push_cstr(&content_fields, "row_uuid");
    strvec_push_cstr(&content_fields, "schema_version");
    strvec_push_cstr(&content_fields, "authored_columns");
    for (u32 i = 0; i < table->columns.len; i++) {
        jz_str f = jazz_engine__query_engine__fields__user_column_field(SL(table->columns.ptr[i].name));
        jz_vec_push(&content_fields, &f, sizeof f);
    }
    static const char *const tail[] = { "created_by", "created_at", "updated_by", "updated_at", "tx_time", "tx_node_id" };
    for (u32 i = 0; i < 6; i++) strvec_push_cstr(&content_fields, tail[i]);
    groove_StaticScanSpec content_scan = jazz_node__node__query_eval__normalization__static_scan_for_prefix(prefix, 1);
    jz_vec_str image_fields; jz_vec_clone(&image_fields, &content_fields, sizeof(jz_str), str_clone_v);
    strvec_push_cstr(&image_fields, "_deletion");
    jz_str gname = jazz_node__node__codec__global_current_table_name(SL(table->name));
    groove_GraphBuilder *current;
    if (tier == JAZZ_TIER_GLOBAL) {
        current = groove__ivm__graph__GraphBuilder__project(groove__ivm__graph__GraphBuilder__table_scan(SL(gname), content_scan), image_fields.ptr, image_fields.len);
    } else {
        groove_StaticScanSpec scan2; groove_StaticScanSpec_clone(&scan2, &content_scan);
        jz_str aname = jazz_node__node__codec__ahead_current_table_name(SL(table->name));
        groove_GraphBuilder *g = groove__ivm__graph__GraphBuilder__project(groove__ivm__graph__GraphBuilder__table_scan(SL(gname), scan2), image_fields.ptr, image_fields.len);
        groove_GraphBuilder *a = groove__ivm__graph__GraphBuilder__project(groove__ivm__graph__GraphBuilder__table_scan(SL(aname), content_scan), image_fields.ptr, image_fields.len);
        jz_str_drop(&aname);
        current = groove__ivm__graph__GraphBuilder__project(arg_max_by_tx(union2(g, a)), image_fields.ptr, image_fields.len);
    }
    jz_str_drop(&gname);
    groove_GraphBuilder *out = groove__ivm__graph__GraphBuilder__project(groove__ivm__graph__GraphBuilder__filter(current, jazz_node__node__codec__not_deleted_predicate()), content_fields.ptr, content_fields.len);
    strvec_drop(&image_fields); strvec_drop(&content_fields);
    return out;
}

/* The rows of a physical current table source whose row_uuid is one of
 * row_ids' (a keyed lookup: one storage probe per distinct id). */
static jz_err jazz_node__node__query_eval__read_sources__lookup_by_row_id(groove_GraphBuilder *rows, groove_GraphBuilder *row_ids, groove_GraphBuilder **out) {
    groove_GraphBuilder *g = groove__ivm__graph__GraphBuilder__lookup(rows, row_ids, ROW_UUID_ARR, 1);
    if (!g) return JZN_INVALID_STORED("a keyed lookup needs a physical current table source");
    *out = g;
    return JZ_OK;
}

/* Whether request's source sits inside a recursive step: Groove evaluates a
 * step without storage reads, so a keyed lookup there is unsupported. */
static bool jazz_node__node__query_eval__read_sources__source_in_recursive_step(const SReq *request) {
    for (u32 i = 0; i < request->source.path.components.len; i++)
        if (request->source.path.components.ptr[i].tag == JAZZ_SR_RECURSIVE_STEP) return true;
    return false;
}

/* A scan of every key under a row-id prefix, whatever the key length below it. */
static groove_StaticScanSpec jazz_node__node__query_eval__read_sources__row_prefix_scan(const groove_ValueVec *prefix) {
    groove_StaticScanSpec s; memset(&s, 0, sizeof s);
    s.tag = GROOVE_SCAN_PREFIX;
    for (u32 i = 0; i < prefix->len; i++) {
        groove_Value v; groove_Value_clone_v(&v, &prefix->ptr[i]);
        groove_LiteralValue *l = groove__ivm__op_types__LiteralValue__from_value(v);
        JZ_VEC_PUSH(s.prefix, l);
    }
    return s;
}

groove_ProjectFieldVec jazz_node__node__query_eval__read_sources__register_storage_fields_for_query_engine(jz_slice prefix) {
    jz_vec_str names = jazz_node__node__query_eval__read_sources__register_storage_field_names();
    groove_ProjectFieldVec out = {0};
    for (u32 i = 0; i < names.len; i++) {
        jz_str src = jz_format("%.*s%.*s", (int)prefix.len, (const char *)prefix.ptr, (int)names.ptr[i].len, names.ptr[i].ptr);
        groove_ProjectField *f = groove__ivm__graph__ProjectField__renamed(SL(src), SL(names.ptr[i]));
        jz_str_drop(&src);
        JZ_VEC_PUSH(out, f);
    }
    strvec_drop(&names);
    return out;
}

jz_vec_str jazz_node__node__query_eval__read_sources__register_storage_field_names(void) {
    static const char *const names[] = { "row_uuid", "tx_time", "tx_node_id", "schema_version", "created_by", "created_at", "updated_by", "updated_at", "_deletion" };
    jz_vec_str out = {0};
    for (u32 i = 0; i < 9; i++) strvec_push_cstr(&out, names[i]);
    return out;
}

/* policy_proof_cycle_from_capability: parse `PolicyProofCycle { table: "<t>", depth: <n>` */
static bool jazz_node__node__query_eval__read_sources__policy_proof_cycle_from_capability(const jz_str *message, jz_str *table, u32 *depth) {
    static const char needle[] = "PolicyProofCycle { table: \"";
    const u32 nl = sizeof(needle) - 1;
    if (message->len < nl) return false;
    i64 at = -1;
    for (i64 i = (i64)message->len - nl; i >= 0; i--) if (memcmp(message->ptr + i, needle, nl) == 0) { at = i; break; }
    if (at < 0) return false;
    const char *p = message->ptr + at + nl, *end = message->ptr + message->len;
    const char *q = memchr(p, '"', (usize)(end - p));
    if (!q) return false;
    jz_slice t = jz_slice_of(p, (u32)(q - p));
    static const char dn[] = ", depth: ";
    const char *r = q + 1, *found = NULL;
    for (const char *c = r; c + (sizeof(dn) - 1) <= end; c++) if (memcmp(c, dn, sizeof(dn) - 1) == 0) { found = c; break; }
    if (!found) return false;
    r = found + sizeof(dn) - 1;
    u64 d = 0; u32 digits = 0;
    while (r < end && *r >= '0' && *r <= '9') { u64 nd = d * 10 + (u64)(*r - '0'); if (nd < d || nd > 0xffffffffu) return false; d = nd; r++; digits++; }
    if (digits == 0) return false;
    *table = jz_str_from_slice(t); *depth = (u32)d;
    return true;
}

static jz_sre jazz_node__node__query_eval__read_sources__source_resolution_error_from_policy_proof(const SReq *request, jz_err error) {
    jz_str table; u32 depth;
    jz_sre s;
    if (error.code == JZN_E_PolicyProofCycle && jazz_node__Error__as_policy_proof_cycle(&error, &table, &depth)) {
        s = sre(request, JAZZ_GAP_POLICY_PROOF_CYCLE); s->gap.table = table; s->gap.depth = depth;
    } else if (error.code == JZN_E_QueryCapability && error.msg && jazz_node__node__query_eval__read_sources__policy_proof_cycle_from_capability(error.msg, &table, &depth)) {
        s = sre(request, JAZZ_GAP_POLICY_PROOF_CYCLE); s->gap.table = table; s->gap.depth = depth;
    } else {
        s = sre(request, JAZZ_GAP_COVERAGE);
    }
    jz_err_drop(&error);
    return s;
}

static jazz_engine_ReadView jazz_node__node__query_eval__read_sources__policy_read_view_projected_through(const jazz_engine_ReadView *policy_view, const jazz_engine_ReadView *enclosing_view) {
    jazz_engine_ReadView projected; jazz_engine_ReadView_clone(&projected, policy_view);
    /* enclosing_frozen_view: the first snapshot (with its overlays) of the
     * enclosing view. */
    const jazz_engine_SourceExpr *frozen_snapshot = NULL; const jazz_engine_OverlayStack *frozen_overlays = NULL;
    {
        jazz_engine_SourceId *k; jazz_engine_SourceExpr *src;
        JZ_BTREE_FOR(&enclosing_view->sources, k, src) {
            if (src->tag == JAZZ_SE_SNAPSHOT_REF) { frozen_snapshot = src; break; }
            if (src->tag == JAZZ_SE_WITH_OVERLAYS && src->input->tag == JAZZ_SE_SNAPSHOT_REF) { frozen_snapshot = src->input; frozen_overlays = &src->overlays; break; }
        }
    }
    /* The outer normalized query and the independently normalized policy
     * proof may use different aliases for one logical table. Preserve every
     * policy source; a snapshot is a program-wide cut, so policy-only sources
     * must use it too. */
    jazz_engine_SourceId *policy_source; jazz_engine_SourceExpr *projected_source;
    JZ_BTREE_FOR(&projected.sources, policy_source, projected_source) {
        const jazz_engine_SourceExpr *match = NULL;
        jazz_engine_SourceId *k; jazz_engine_SourceExpr *src;
        JZ_BTREE_FOR(&enclosing_view->sources, k, src) { if (jz_str_eq(&k->table, &policy_source->table)) { match = src; break; } }
        if (match) {
            jazz_engine_SourceExpr c; jazz_engine_SourceExpr_clone(&c, match);
            jazz_engine_SourceExpr_drop(projected_source); *projected_source = c;
        } else if (frozen_snapshot && projected_source->tag == JAZZ_SE_VISIBLE_CURRENT && projected_source->data.tag == JAZZ_DS_CURRENT) {
            jazz_engine_SourceExpr snapshot_source; memset(&snapshot_source, 0, sizeof snapshot_source);
            snapshot_source.tag = JAZZ_SE_SNAPSHOT_REF;
            jazz_engine_SchemaProjection_clone(&snapshot_source.projection, &projected_source->projection);
            snapshot_source.data.tag = JAZZ_DS_CURRENT;
            jazz_model_Snapshot_clone(&snapshot_source.snapshot, &frozen_snapshot->snapshot);
            jazz_engine_SourceExpr_drop(projected_source);
            if (frozen_overlays) {
                memset(projected_source, 0, sizeof *projected_source);
                projected_source->tag = JAZZ_SE_WITH_OVERLAYS;
                projected_source->input = jz_alloc(sizeof(jazz_engine_SourceExpr));
                *projected_source->input = snapshot_source;
                jazz_engine_OverlayStack_clone(&projected_source->overlays, frozen_overlays);
            } else {
                *projected_source = snapshot_source;
            }
        }
    }
    return projected;
}

/* std::env::var_os is always None on wasm32-unknown-unknown. */
bool jazz_node__node__query_eval__read_sources__capability_trace_enabled(void) {
    return false;
}

void jazz_node__node__query_eval__read_sources__trace_capability_compile(jazz_types_NodeUuid node_uuid, bool has_alias, u64 node_alias,
    const jazz_engine_QueryProgramRequest *request, const void *program, const void *report) {
    (void)node_uuid; (void)has_alias; (void)node_alias; (void)request; (void)program; (void)report;
    /* JAZZ_CAPABILITY_TRACE_FILE unset (no process environment): trace skipped. */
}

static void metadata_insert(jz_btree *metadata, jazz_engine_SourceMetadataRequirement key, jazz_engine_SourceMetadataFields val) {
    jz_btree_insert(metadata, &key, &val, NULL);
}
static jazz_engine_SourceMetadataRequirement smr(u32 tag) { jazz_engine_SourceMetadataRequirement r; memset(&r, 0, sizeof r); r.tag = tag; return r; }
static jazz_engine_SourceMetadataFields smf1(u32 tag, const char *f0) { jazz_engine_SourceMetadataFields f; memset(&f, 0, sizeof f); f.tag = tag; f.f0 = jz_str_from(f0); return f; }
static jazz_engine_SourceMetadataFields smf_provenance(jz_str field) { jazz_engine_SourceMetadataFields f; memset(&f, 0, sizeof f); f.tag = JAZZ_SMF_PROVENANCE; f.f0 = field; return f; }
static void push_pf(groove_ProjectFieldVec *v, groove_ProjectField *f) { JZ_VEC_PUSH(*v, f); }
static groove_ProjectField *author_path_field(const jz_str *name) {
    jz_slice root; const char *const *path; u32 n;
    if (!jazz_types__ids__AuthorSubject__metadata_path(SL(*name), &root, &path, &n)) jz_panic("validated author path");
    return groove__ivm__graph__ProjectField__record_field(root, path, n, jz_str_clone(name));
}

/* Expose requested nested-author fields on policy preimages that retain
 * complete author records. */
static jz_err jazz_node__node__query_eval__read_sources__with_requested_author_paths(groove_GraphBuilder *graph, groove_RecordDescriptor descriptor, jz_btree metadata, const jazz_engine_SourceRequirements *requirements,
    groove_GraphBuilder **out_graph, groove_RecordDescriptor *out_descriptor, jz_btree *out_metadata) {
    jz_vec_str missing = {0};
    jazz_engine_SourceMetadataRequirement *req; void *v;
    JZ_BTREE_FOR(&requirements->metadata, req, v) {
        if (req->tag == JAZZ_SMR_AUTHOR_PATH && !jz_btree_contains(&metadata, req)) { jz_str c = jz_str_clone(&req->author_path); jz_vec_push(&missing, &c, sizeof c); }
    }
    if (missing.len == 0) { *out_graph = graph; *out_descriptor = descriptor; *out_metadata = metadata; return JZ_OK; }
    groove_DescriptorFieldVec fields; jz_vec_clone(&fields, groove__records__RecordDescriptor__fields(descriptor), sizeof(groove_DescriptorField), groove_DescriptorField_clone);
    jz_vec_str names;
    jz_err e = jazz_node__node__query_eval__lowering__descriptor_field_names(descriptor, &names);
    if (JZ_IS_ERR(e)) { strvec_drop(&missing); jz_vec_drop(&fields, sizeof(groove_DescriptorField), groove_DescriptorField_drop); groove_GraphBuilder_free(graph); jz_btree_drop(&metadata); return e; }
    groove_ProjectFieldVec projection = {0};
    for (u32 i = 0; i < names.len; i++) push_pf(&projection, groove__ivm__graph__ProjectField__named(SL(names.ptr[i])));
    strvec_drop(&names);
    for (u32 i = 0; i < missing.len; i++) {
        jz_str *name = &missing.ptr[i];
        push_pf(&projection, author_path_field(name));
        const groove_ValueType *t = jazz_types__ids__AuthorSubject__metadata_type(SL(*name));
        if (!t) jz_panic("validated author path");
        groove_ValueType tc; groove_ValueType_clone(&tc, t);
        groove_DescriptorField f = groove__records__DescriptorField__new(jz_str_clone(name), tc);
        jz_vec_push(&fields, &f, sizeof f);
        jazz_engine_SourceMetadataRequirement k = smr(JAZZ_SMR_AUTHOR_PATH); k.author_path = jz_str_clone(name);
        metadata_insert(&metadata, k, smf_provenance(jz_str_clone(name)));
    }
    strvec_drop(&missing);
    *out_graph = groove__ivm__graph__GraphBuilder__project_fields(graph, projection);
    *out_descriptor = groove__records__RecordDescriptor__new(fields);
    *out_metadata = metadata;
    return JZ_OK;
}

static const char *jazz_node__node__query_eval__read_sources__source_provenance_field(jazz_engine_ProvenanceField field) {
    switch (field) {
    case JAZZ_PROV_CREATED_AT: return "$createdAt";
    case JAZZ_PROV_CREATED_BY: return "$createdBy";
    case JAZZ_PROV_UPDATED_AT: return "$updatedAt";
    case JAZZ_PROV_UPDATED_BY: return "$updatedBy";
    }
    return "";
}

static void push_user_columns_with_identity(groove_ProjectFieldVec *fields, const jazz_model_TableSchema *table) {
    for (u32 i = 0; i < table->columns.len; i++) {
        jz_str f = jazz_engine__query_engine__fields__user_column_field(SL(table->columns.ptr[i].name));
        push_pf(fields, groove__ivm__graph__ProjectField__named_with_identity(SL(f), jz_str_clone(&table->columns.ptr[i].name)));
        jz_str_drop(&f);
    }
}

static groove_ProjectFieldVec jazz_node__node__query_eval__read_sources__canonical_current_source_fields(const jazz_model_TableSchema *table, bool include_version) {
    groove_ProjectFieldVec fields = {0};
    push_pf(&fields, groove__ivm__graph__ProjectField__named(CS("row_uuid")));
    push_user_columns_with_identity(&fields, table);
    static const char *const tail[] = { "$createdBy", "$createdAt", "$updatedBy", "$updatedAt", "tx_time", "tx_node_id" };
    for (u32 i = 0; i < 6; i++) push_pf(&fields, groove__ivm__graph__ProjectField__named(jz_slice_of(tail[i], (u32)strlen(tail[i]))));
    if (include_version) {
        push_pf(&fields, groove__ivm__graph__ProjectField__named(CS("schema_version")));
        push_pf(&fields, groove__ivm__graph__ProjectField__named(CS("authored_columns")));
    }
    return fields;
}

static groove_ProjectFieldVec jazz_node__node__query_eval__read_sources__storage_to_canonical_current_source_fields(const jazz_model_TableSchema *table, bool include_version, bool include_settle_position) {
    groove_ProjectFieldVec fields = {0};
    push_pf(&fields, groove__ivm__graph__ProjectField__named(CS("row_uuid")));
    push_user_columns_with_identity(&fields, table);
    push_pf(&fields, groove__ivm__graph__ProjectField__renamed(CS("created_by"), CS("$createdBy")));
    push_pf(&fields, groove__ivm__graph__ProjectField__renamed(CS("created_at"), CS("$createdAt")));
    push_pf(&fields, groove__ivm__graph__ProjectField__renamed(CS("updated_by"), CS("$updatedBy")));
    push_pf(&fields, groove__ivm__graph__ProjectField__renamed(CS("updated_at"), CS("$updatedAt")));
    push_pf(&fields, groove__ivm__graph__ProjectField__named(CS("tx_time")));
    push_pf(&fields, groove__ivm__graph__ProjectField__named(CS("tx_node_id")));
    if (include_version) {
        push_pf(&fields, groove__ivm__graph__ProjectField__named(CS("schema_version")));
        push_pf(&fields, groove__ivm__graph__ProjectField__named(CS("authored_columns")));
    }
    if (include_settle_position) push_pf(&fields, groove__ivm__graph__ProjectField__renamed(CS("global_time"), CS("settle_position")));
    return fields;
}

static bool strvec_contains(const jz_vec_str *v, const jz_str *s) { for (u32 i = 0; i < v->len; i++) if (jz_str_eq(&v->ptr[i], s)) return true; return false; }

static jz_err jazz_node__node__query_eval__read_sources__branch_view_storage_source_fields(const jazz_model_TableSchema *table, const jazz_model_BranchKey *head, groove_ProjectFieldVec *out) {
    u32 nv; const jazz_model_BranchKeyValue *head_values = jazz_model__branch__BranchKey__values(head, &nv);
    groove_ProjectFieldVec fields = {0};
    push_pf(&fields, groove__ivm__graph__ProjectField__renamed(CS("branch_key"), CS("supplying_branch_key")));
    push_pf(&fields, groove__ivm__graph__ProjectField__named(CS("row_uuid")));
    push_pf(&fields, groove__ivm__graph__ProjectField__named(CS("schema_version")));
    push_pf(&fields, groove__ivm__graph__ProjectField__named(CS("authored_columns")));
    for (u32 i = 0; i < table->columns.len; i++) {
        const jazz_model_ColumnSchema *column = &table->columns.ptr[i];
        jz_str uf = jazz_engine__query_engine__fields__user_column_field(SL(column->name));
        if (strvec_contains(&table->branch_by, &column->name)) {
            /* head_values: BTreeMap from the key's values; last spelling wins */
            const jazz_model_BranchKeyValue *hv = NULL;
            for (u32 j = 0; j < nv; j++) if (jz_str_eq(&head_values[j].column, &column->name)) hv = &head_values[j];
            if (!hv) { jz_str_drop(&uf); groove_ProjectFieldVec_drop(&fields); return jz_err_fmt(JZN_E_InvalidBranchKey, "invalid branch key: %s", "head branch key missing projected table column"); }
            groove_Value value;
            jz_err e = jazz_model__branch__EncodedBranchValue__decode(&hv->value, &value);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); jz_str_drop(&uf); groove_ProjectFieldVec_drop(&fields); return jz_err_fmt(JZN_E_InvalidBranchKey, "invalid branch key: %s", "invalid head branch value encoding"); }
            push_pf(&fields, groove__ivm__graph__ProjectField__literal_with_identity(SL(uf), groove__ivm__op_types__LiteralValue__from_value(value), jz_str_clone(&column->name)));
        } else {
            push_pf(&fields, groove__ivm__graph__ProjectField__named_with_identity(SL(uf), jz_str_clone(&column->name)));
        }
        jz_str_drop(&uf);
    }
    static const char *const tail[] = { "created_by", "created_at", "updated_by", "updated_at", "tx_time", "tx_node_id", "global_time" };
    for (u32 i = 0; i < 7; i++) push_pf(&fields, groove__ivm__graph__ProjectField__named(jz_slice_of(tail[i], (u32)strlen(tail[i]))));
    *out = fields;
    return JZ_OK;
}

static groove_RecordDescriptor jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields_for_branch_and_deletion(const jazz_model_TableSchema *table, const jz_btree *metadata,
    bool branch_columns_nonnullable, bool include_deletion_marker);

groove_RecordDescriptor jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields(const jazz_model_TableSchema *table, const jz_btree *metadata) {
    return jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields_for_branch(table, metadata, false);
}

/* The current-source compatibility boundary is the schema's canonical
 * current-row representation: physical catalogue registration assigns stable
 * enum identities there, which a receiver-owned input must retain exactly. */
typedef struct { jz_str name; u32 identity_kind; jz_str identity_name; u32 identity_slot; const groove_ValueType *value_type; } current_type_entry;
groove_RecordDescriptor jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields_for_current_storage(const jazz_model_TableSchema *table, const jz_btree *metadata) {
    groove_RecordDescriptor logical = jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields(table, metadata);
    groove_RecordDescriptor current = groove__schema__TableSchema__record_schema(jazz_model__schema__TableSchema__global_current_storage_table(table));
    const groove_DescriptorFieldVec *current_fields = groove__records__RecordDescriptor__fields(current);
    JZ_VEC(current_type_entry) current_types = {0};
    for (u32 i = 0; i < table->columns.len; i++) {
        jz_str storage_name = jazz_model__schema__app_storage_column_name(SL(table->columns.ptr[i].name));
        u32 index;
        if (groove__records__RecordDescriptor__field_index(current, SL(storage_name), &index) && index < current_fields->len) {
            const groove_DescriptorField *f = &current_fields->ptr[index];
            current_type_entry en = { jazz_engine__query_engine__fields__user_column_field(SL(table->columns.ptr[i].name)), f->identity_kind, f->identity_name, f->identity_slot, &f->value_type };
            /* BTreeMap::collect: later duplicates replace earlier */
            bool replaced = false;
            for (u32 j = 0; j < current_types.len; j++) if (jz_str_eq(&current_types.ptr[j].name, &en.name)) { jz_str_drop(&current_types.ptr[j].name); current_types.ptr[j] = en; replaced = true; }
            if (!replaced) JZ_VEC_PUSH(current_types, en);
        }
        jz_str_drop(&storage_name);
    }
    const groove_DescriptorFieldVec *lf = groove__records__RecordDescriptor__fields(logical);
    groove_DescriptorFieldVec out = {0};
    for (u32 i = 0; i < lf->len; i++) {
        const groove_DescriptorField *field = &lf->ptr[i];
        if (!field->has_name) jz_panic("compiler-owned current source descriptor fields are named");
        groove_DescriptorField nf; memset(&nf, 0, sizeof nf);
        nf.name = jz_str_clone(&field->name); nf.has_name = true;
        const current_type_entry *ct = NULL;
        for (u32 j = 0; j < current_types.len; j++) if (jz_str_eq(&current_types.ptr[j].name, &field->name)) { ct = &current_types.ptr[j]; break; }
        if (ct) {
            if (field->identity_kind == GROOVE_FID_NAME && ct->identity_kind == GROOVE_FID_SLOT) {
                nf.identity_kind = GROOVE_FID_NAMED_SLOT; nf.identity_name = jz_str_clone(&field->identity_name); nf.identity_slot = ct->identity_slot;
            } else if (field->identity_kind == GROOVE_FID_NAME && ct->identity_kind == GROOVE_FID_NAMED_SLOT) {
                nf.identity_kind = GROOVE_FID_NAMED_SLOT; nf.identity_name = jz_str_clone(&field->identity_name); nf.identity_slot = ct->identity_slot;
            } else if (field->identity_kind == GROOVE_FID_NAME && ct->identity_kind == GROOVE_FID_NAME && jz_str_eq(&ct->identity_name, &field->name) && !jz_str_eq(&field->identity_name, &ct->identity_name)) {
                nf.identity_kind = GROOVE_FID_NAME; nf.identity_name = jz_str_clone(&field->identity_name);
            } else if (ct->identity_kind != GROOVE_FID_NONE) {
                nf.identity_kind = ct->identity_kind; nf.identity_name = jz_str_clone(&ct->identity_name); nf.identity_slot = ct->identity_slot;
            } else {
                nf.identity_kind = field->identity_kind; nf.identity_name = jz_str_clone(&field->identity_name); nf.identity_slot = field->identity_slot;
            }
            groove_ValueType_clone(&nf.value_type, ct->value_type);
        } else {
            nf.identity_kind = field->identity_kind; nf.identity_name = jz_str_clone(&field->identity_name); nf.identity_slot = field->identity_slot;
            groove_ValueType_clone(&nf.value_type, &field->value_type);
        }
        jz_vec_push(&out, &nf, sizeof nf);
    }
    for (u32 j = 0; j < current_types.len; j++) jz_str_drop(&current_types.ptr[j].name);
    jz_free(current_types.ptr);
    return groove__records__RecordDescriptor__new(out);
}

static groove_RecordDescriptor jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields_for_branch(const jazz_model_TableSchema *table, const jz_btree *metadata, bool branch_columns_nonnullable) {
    return jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields_for_branch_and_deletion(table, metadata, branch_columns_nonnullable, false);
}

static void push_df(groove_DescriptorFieldVec *v, const char *name, groove_ValueType t) { groove_DescriptorField f = groove__records__DescriptorField__new(jz_str_from(name), t); jz_vec_push(v, &f, sizeof f); }
static bool metadata_has(const jz_btree *m, u32 tag) { jazz_engine_SourceMetadataRequirement k = smr(tag); return jz_btree_contains(m, &k); }

static groove_RecordDescriptor jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields_for_branch_and_deletion(const jazz_model_TableSchema *table, const jz_btree *metadata,
    bool branch_columns_nonnullable, bool include_deletion_marker) {
    /* Inline policy candidates may still contain indirect scalars. Preserve
     * their semantic kind through this second encoding into query sources. */
    groove_DescriptorFieldVec fields = {0};
    push_df(&fields, "row_uuid", groove_ValueType_simple(GROOVE_VT_UUID));
    for (u32 i = 0; i < table->columns.len; i++) {
        const jazz_model_ColumnSchema *column = &table->columns.ptr[i];
        groove_ValueType vt = jazz_node__node__codec__current_row_column_type(column);
        if (!(branch_columns_nonnullable && strvec_contains(&table->branch_by, &column->name))) vt = groove_ValueType_nullable(vt);
        groove_DescriptorField f = jazz_node__node__query_eval__read_sources__current_row_column_field(column, vt);
        jz_vec_push(&fields, &f, sizeof f);
    }
    push_df(&fields, "$createdBy", jazz_types__ids__RowAuthor__value_type());
    push_df(&fields, "$createdAt", groove_ValueType_simple(GROOVE_VT_U64));
    push_df(&fields, "$updatedBy", jazz_types__ids__RowAuthor__value_type());
    push_df(&fields, "$updatedAt", groove_ValueType_simple(GROOVE_VT_U64));
    push_df(&fields, "tx_time", groove_ValueType_simple(GROOVE_VT_U64));
    push_df(&fields, "tx_node_id", groove_ValueType_simple(GROOVE_VT_U64));
    jazz_engine_SourceMetadataRequirement *k; jazz_engine_SourceMetadataFields *mv;
    JZ_BTREE_FOR(metadata, k, mv) {
        if (k->tag == JAZZ_SMR_AUTHOR_PATH) {
            const groove_ValueType *t = jazz_types__ids__AuthorSubject__metadata_type(SL(k->author_path));
            if (!t) jz_panic("author path");
            groove_ValueType tc; groove_ValueType_clone(&tc, t);
            groove_DescriptorField f = groove__records__DescriptorField__new(jz_str_clone(&k->author_path), tc);
            jz_vec_push(&fields, &f, sizeof f);
        }
    }
    if (metadata_has(metadata, JAZZ_SMR_VERSION_WITNESSES)) {
        push_df(&fields, "table", groove_ValueType_simple(GROOVE_VT_STRING));
        push_df(&fields, "layer", groove_ValueType_simple(GROOVE_VT_STRING));
        push_df(&fields, "schema_version", groove_ValueType_simple(GROOVE_VT_U64));
        push_df(&fields, "authored_columns", groove_ValueType_nullable(groove_ValueType_array(groove_ValueType_simple(GROOVE_VT_U64))));
        push_df(&fields, "created_by", jazz_types__ids__RowAuthor__value_type());
        push_df(&fields, "created_at", groove_ValueType_simple(GROOVE_VT_U64));
        push_df(&fields, "updated_by", jazz_types__ids__RowAuthor__value_type());
        push_df(&fields, "updated_at", groove_ValueType_simple(GROOVE_VT_U64));
        jazz_engine_SourceMetadataRequirement vk = smr(JAZZ_SMR_VERSION_WITNESSES);
        const jazz_engine_SourceMetadataFields *vw = jz_btree_get(metadata, &vk);
        if (vw && vw->tag == JAZZ_SMF_VERSION_WITNESSES && vw->has_opt0) {
            groove_DescriptorField f = groove__records__DescriptorField__new(jz_str_clone(&vw->opt0), groove_ValueType_simple(GROOVE_VT_BYTES));
            jz_vec_push(&fields, &f, sizeof f);
        }
    }
    if (branch_columns_nonnullable && !metadata_has(metadata, JAZZ_SMR_VERSION_WITNESSES)) push_df(&fields, "supplying_branch_key", groove_ValueType_simple(GROOVE_VT_BYTES));
    if (metadata_has(metadata, JAZZ_SMR_SETTLE_POSITION)) push_df(&fields, "settle_position", groove_ValueType_nullable(groove_ValueType_simple(GROOVE_VT_U64)));
    if (metadata_has(metadata, JAZZ_SMR_COVERAGE)) push_df(&fields, "coverage", groove_ValueType_simple(GROOVE_VT_STRING));
    if (include_deletion_marker) push_df(&fields, "__jazz_deleted", groove_ValueType_simple(GROOVE_VT_BOOL));
    return groove__records__RecordDescriptor__new(fields);
}

static jz_err jazz_node__node__query_eval__read_sources__resolved_current_source_graph(jazz_node_NodeState *node, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier,
    const jazz_engine_SourceRequirements *requirements, const jazz_engine_SourceAuthorizationRequest *authorization, jazz_types_SchemaVersionId policy_schema,
    const jazz_engine_ReadView *policy_read_view, const char *branch_witness_field, groove_GraphBuilder *selected_base,
    groove_GraphBuilder **out_graph, groove_RecordDescriptor *out_desc, jz_btree *out_meta, jz_btree *out_routing) {
    groove_ProjectFieldVec fields = jazz_node__node__query_eval__read_sources__canonical_current_source_fields(table, false);
    jazz_engine_SourceMetadataRequirement *req; void *rv;
    JZ_BTREE_FOR(&requirements->metadata, req, rv) {
        if (req->tag == JAZZ_SMR_AUTHOR_PATH) push_pf(&fields, author_path_field(&req->author_path));
    }
    jz_btree metadata = empty_metadata();
    bool needs_version_witnesses = requirements_contains(requirements, JAZZ_SMR_VERSION_WITNESSES);
    if (!needs_version_witnesses) {
        JZ_BTREE_FOR(&requirements->metadata, req, rv) { if (req->tag == JAZZ_SMR_PROVENANCE) { needs_version_witnesses = true; break; } }
    }
    bool needs_settle_position = requirements_contains(requirements, JAZZ_SMR_SETTLE_POSITION);
    jz_slice bwf = branch_witness_field ? jz_slice_of(branch_witness_field, (u32)strlen(branch_witness_field)) : CS("");
    if (needs_version_witnesses) {
        push_pf(&fields, groove__ivm__graph__ProjectField__literal(CS("table"), groove__ivm__op_types__LiteralValue__from_value(groove_Value_string(jz_str_clone(&table->name)))));
        push_pf(&fields, groove__ivm__graph__ProjectField__literal(CS("layer"), groove__ivm__op_types__LiteralValue__from_value(groove_Value_string(jz_str_from("content")))));
        push_pf(&fields, groove__ivm__graph__ProjectField__named(CS("schema_version")));
        push_pf(&fields, groove__ivm__graph__ProjectField__named(CS("authored_columns")));
        push_pf(&fields, groove__ivm__graph__ProjectField__renamed(CS("$createdBy"), CS("created_by")));
        push_pf(&fields, groove__ivm__graph__ProjectField__renamed(CS("$createdAt"), CS("created_at")));
        push_pf(&fields, groove__ivm__graph__ProjectField__renamed(CS("$updatedBy"), CS("updated_by")));
        push_pf(&fields, groove__ivm__graph__ProjectField__renamed(CS("$updatedAt"), CS("updated_at")));
        if (branch_witness_field) push_pf(&fields, groove__ivm__graph__ProjectField__named(bwf));
        jazz_engine_SourceMetadataFields vw; memset(&vw, 0, sizeof vw);
        vw.tag = JAZZ_SMF_VERSION_WITNESSES;
        vw.f0 = jz_str_from("schema_version"); vw.f1 = jz_str_from("tx_time"); vw.f2 = jz_str_from("tx_node_id");
        if (branch_witness_field) { vw.has_opt0 = true; vw.opt0 = jz_str_from(branch_witness_field); }
        metadata_insert(&metadata, smr(JAZZ_SMR_VERSION_WITNESSES), vw);
    }
    if (needs_settle_position) {
        push_pf(&fields, groove__ivm__graph__ProjectField__named(CS("settle_position")));
        metadata_insert(&metadata, smr(JAZZ_SMR_SETTLE_POSITION), smf1(JAZZ_SMF_SETTLE_POSITION, "settle_position"));
    }
    if (requirements_contains(requirements, JAZZ_SMR_COVERAGE)) {
        push_pf(&fields, groove__ivm__graph__ProjectField__literal(CS("coverage"), groove__ivm__op_types__LiteralValue__from_value(groove_Value_string(jz_str_from("visible-current")))));
        metadata_insert(&metadata, smr(JAZZ_SMR_COVERAGE), smf1(JAZZ_SMF_COVERAGE, "coverage"));
    }
    JZ_BTREE_FOR(&requirements->metadata, req, rv) {
        if (req->tag == JAZZ_SMR_AUTHOR_PATH) {
            jazz_engine_SourceMetadataRequirement k; jazz_engine_SourceMetadataRequirement_clone(&k, req);
            metadata_insert(&metadata, k, smf_provenance(jz_str_clone(&req->author_path)));
        }
        if (req->tag == JAZZ_SMR_PROVENANCE) {
            /* Every resolver branch normalizes storage names before it exposes
             * this source: metadata names the canonical field. */
            jazz_engine_SourceMetadataRequirement k = smr(JAZZ_SMR_PROVENANCE); k.provenance = req->provenance;
            metadata_insert(&metadata, k, smf_provenance(jz_str_from(jazz_node__node__query_eval__read_sources__source_provenance_field(req->provenance))));
        }
    }
    /* The current graph below carries storage cells, not materialized public
     * values; bind the prepared layout to that same source contract. */
    groove_RecordDescriptor descriptor = !branch_witness_field
        ? jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields_for_current_storage(table, &metadata)
        : jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields_for_branch(table, &metadata, true);
    groove_GraphBuilder *base; jz_btree routing_fields; jz_err e;
    if (authorization->tag == JAZZ_SAR_SYSTEM) {
        if (selected_base) {
            groove_ProjectFieldVec selected_fields = jazz_node__node__query_eval__read_sources__storage_to_canonical_current_source_fields(table, needs_version_witnesses, needs_settle_position);
            if (branch_witness_field) push_pf(&selected_fields, groove__ivm__graph__ProjectField__named(bwf));
            base = groove__ivm__graph__GraphBuilder__project_fields(selected_base, selected_fields);
        } else if (needs_version_witnesses) {
            groove_GraphBuilder *g;
            e = jazz_node__node__query_eval__authorization__NodeState__maintained_view_content_current_with_version(node, table, tier, &g);
            if (JZ_IS_ERR(e)) goto fail;
            base = groove__ivm__graph__GraphBuilder__project_fields(g, jazz_node__node__query_eval__read_sources__storage_to_canonical_current_source_fields(table, true, needs_settle_position));
        } else {
            base = groove__ivm__graph__GraphBuilder__project_fields(jazz_node__node__codec__visible_current_graph(table, tier), jazz_node__node__query_eval__read_sources__canonical_current_source_fields(table, false));
        }
        routing_fields = empty_strset();
    } else {
        const jazz_engine_PolicyAuthorizationPlan *plan = &authorization->plan;
        if (!plan_matches_read_row(plan, table)) {
            e = JZN_QUERY_CAPABILITY("%s", "policy authorization plan does not match resolved source");
            goto fail;
        }
        jazz_engine_QueryProgramRequest policy_request; memset(&policy_request, 0, sizeof policy_request);
        jz_err policy_err = jazz_node__node__query_eval__authorization__NodeState__table_read_policy_authorization_request(
            node, policy_schema, SL(table->name), authorization->permission_subject, plan_param_binding_mode(plan), tier,
            plan->has_binding_source_shape, opt_shape_clone(plan), btree_clone_plain(&plan->binding_user_params), btree_clone_plain(&plan->binding_claim_params), &policy_request);
        if (policy_read_view) project_policy_request(policy_err, &policy_request, policy_read_view);
        jz_vec_str output_fields = jazz_node__node__query_eval__read_sources__global_current_storage_fields(table, needs_version_witnesses, needs_settle_position);
        if (branch_witness_field) strvec_push_cstr(&output_fields, branch_witness_field);
        groove_GraphBuilder *b;
        if (selected_base) b = selected_base;
        else {
            e = jazz_node__node__query_eval__authorization__NodeState__maintained_view_content_current_with_version(node, table, tier, &b);
            if (JZ_IS_ERR(e)) { if (!JZ_IS_ERR(policy_err)) jazz_engine_QueryProgramRequest_drop(&policy_request); jz_err_drop(&policy_err); strvec_drop(&output_fields); goto fail; }
        }
        jazz_node_PolicyAuthorizationGraph storage_graph;
        e = jazz_node__node__query_eval__authorization__NodeState__compose_policy_filtered_current_source_graph(node, policy_err, &policy_request, groove_GraphBuilder_clone(b), output_fields.ptr, output_fields.len, &storage_graph);
        groove_GraphBuilder_free(b);
        strvec_drop(&output_fields);
        if (JZ_IS_ERR(e)) goto fail;
        groove_ProjectFieldVec canonical_fields = jazz_node__node__query_eval__read_sources__storage_to_canonical_current_source_fields(table, needs_version_witnesses, needs_settle_position);
        if (branch_witness_field) push_pf(&canonical_fields, groove__ivm__graph__ProjectField__named(bwf));
        jz_str *rf; void *rfv;
        JZ_BTREE_FOR(&storage_graph.route_fields, rf, rfv) push_pf(&canonical_fields, groove__ivm__graph__ProjectField__named(SL(*rf)));
        base = groove__ivm__graph__GraphBuilder__project_fields(storage_graph.graph, canonical_fields);
        routing_fields = storage_graph.route_fields;
        jz_btree_drop(&storage_graph.access_paths);
    }
    {
        jz_str *rf; void *rfv;
        JZ_BTREE_FOR(&routing_fields, rf, rfv) push_pf(&fields, groove__ivm__graph__ProjectField__named(SL(*rf)));
    }
    if (metadata.len == 0) { *out_graph = base; groove_ProjectFieldVec_drop(&fields); }
    else *out_graph = groove__ivm__graph__GraphBuilder__project_fields(base, fields);
    *out_desc = descriptor; *out_meta = metadata; *out_routing = routing_fields;
    return JZ_OK;
fail:
    if (selected_base && authorization->tag == JAZZ_SAR_SYSTEM) { /* consumed above */ }
    groove_ProjectFieldVec_drop(&fields); jz_btree_drop(&metadata);
    return e;
}

/* ---------- NodeState: physical current-source access paths ---------- */

static void source_equalities_val_drop(void *p) { jz_btree_drop((jz_btree *)p); }
static const jz_btree_desc source_equalities_desc = { sizeof(jazz_engine_SourceId), sizeof(jz_btree), jazz_engine_SourceId_cmp, jazz_engine_SourceId_drop, source_equalities_val_drop };
static jz_btree empty_equalities(void) { jz_btree m; jz_btree_init(&m, &jazz_node_StrValue_desc); return m; }
static jz_btree empty_access_paths(void) { jz_btree m; jz_btree_init(&m, &jazz_node_AccessPaths_desc); return m; }
static void access_paths_insert(jz_btree *paths, const jazz_engine_SourceId *source, jazz_node_CurrentAccessPath path) {
    jazz_engine_SourceId k; jazz_engine_SourceId_clone(&k, source);
    jz_btree_insert(paths, &k, &path, NULL);
}

static jz_err jazz_node__node__query_eval__read_sources__normalized_program_equalities(const jazz_engine_NormalizedRowSetShape *shape, const jazz_engine_RowSetNodeId *node_id,
    const jazz_engine_ProgramBinding *binding, const jazz_engine_PolicyContext *policy, bool *some, jz_btree *out);
static jz_err jazz_node__node__query_eval__read_sources__collect_normalized_program_equalities(const jazz_engine_PredicateExpr *predicate, const jazz_engine_ProgramBinding *binding,
    const jazz_engine_PolicyContext *policy, jz_btree *equalities);
static bool jazz_node__node__query_eval__read_sources__select_composite_equality_access_path(const jazz_model_TableSchema *table, const jz_btree *equalities, jazz_node_CurrentAccessPath *out);
static void jazz_node__node__query_eval__read_sources__attach_covered_join_key_filter(const jazz_model_TableSchema *join_table, jz_slice on_column, jz_slice root_table,
    const jazz_node_CurrentAccessPath *root_path, jazz_node_CurrentAccessPath *join_path);
static const jazz_engine_SourceId *jazz_node__node__query_eval__read_sources__single_root_join_source(const jazz_engine_QueryProgramRequest *request, jz_slice join_table);

static jz_err jazz_node__node__query_eval__read_sources__NodeState__normalized_program_access_paths(const jazz_node_NodeState *self, const jazz_engine_RowSetProgramInput *input,
    const jazz_engine_ReadView *read_view, const jazz_engine_PolicyContext *policy, bool allow_local, bool allow_secondary_indexes, jz_btree *out);
static jz_err jazz_node__node__query_eval__read_sources__NodeState__guarded_current_access_path(const jazz_node_NodeState *self, const jazz_engine_ReadView *read_view, const jazz_engine_SourceId *source,
    const jz_btree *equalities, bool allow_local, bool allow_secondary_indexes, jazz_node_AccessPathSelector selector, const char *covered_column, bool *some, jazz_node_CurrentAccessPath *out);

/* Select physical current-source access paths from the normalized program
 * itself: a deliberately trivial rule-based planner over supported indexed
 * equality constraints. It has no policy branch; every selected path merely
 * narrows candidate rows and the lowered predicate graph still decides. */
jz_err jazz_node__node__query_eval__read_sources__NodeState__query_program_access_paths(const jazz_node_NodeState *self, const jazz_engine_QueryProgramRequest *request, bool allow_secondary_indexes, jz_btree *out) {
    /* A policy proof may contain both an owner arm and a correlated
     * membership arm for the same protected source: any alternative or
     * relational proof retains full source scans. */
    jazz_engine_RowSetNodeId *k; jazz_engine_RowSetExpr *node;
    JZ_BTREE_FOR(&request->input.shape.nodes, k, node) {
        if (node->tag == JAZZ_RSE_UNION || node->tag == JAZZ_RSE_JOIN || node->tag == JAZZ_RSE_RECURSIVE) { *out = empty_access_paths(); return JZ_OK; }
    }
    return jazz_node__node__query_eval__read_sources__NodeState__normalized_program_access_paths(self, &request->input, &request->reads.primary, &request->policy, false, allow_secondary_indexes, out);
}

static jz_err jazz_node__node__query_eval__read_sources__NodeState__normalized_program_access_paths(const jazz_node_NodeState *self, const jazz_engine_RowSetProgramInput *input,
    const jazz_engine_ReadView *read_view, const jazz_engine_PolicyContext *policy, bool allow_local, bool allow_secondary_indexes, jz_btree *out) {
    jz_btree equalities_by_source; jz_btree_init(&equalities_by_source, &source_equalities_desc);
    /* Only a source's own Filter -> Source pipeline is recognized; inspecting
     * every normalized node lets independent recursive seed/step pipelines
     * participate while refusing to infer through relational operators. */
    jazz_engine_RowSetNodeId *node_id; jazz_engine_RowSetExpr *nv;
    JZ_BTREE_FOR(&input->shape.nodes, node_id, nv) {
        bool some; jz_btree equalities;
        jz_err e = jazz_node__node__query_eval__read_sources__normalized_program_equalities(&input->shape, node_id, &input->binding, policy, &some, &equalities);
        if (JZ_IS_ERR(e)) { jz_btree_drop(&equalities_by_source); return e; }
        if (!some) continue;
        jazz_engine_SourceId *source; jz_btree *eqs;
        JZ_BTREE_FOR(&equalities, source, eqs) {
            jz_btree *entry = jz_btree_get(&equalities_by_source, source);
            if (!entry) {
                jazz_engine_SourceId sk; jazz_engine_SourceId_clone(&sk, source);
                jz_btree fresh = empty_equalities();
                jz_btree_insert(&equalities_by_source, &sk, &fresh, NULL);
                entry = jz_btree_get(&equalities_by_source, source);
            }
            /* BTreeMap::extend: later entries replace earlier ones */
            jz_str *f; groove_Value *v;
            JZ_BTREE_FOR(eqs, f, v) {
                jz_str fk = jz_str_clone(f); groove_Value vc; groove_Value_clone_v(&vc, v);
                jz_btree_insert(entry, &fk, &vc, NULL);
            }
        }
        jz_btree_drop(&equalities);
    }
    jz_btree paths = empty_access_paths();
    jazz_engine_SourceId *source; jz_btree *equalities;
    JZ_BTREE_FOR(&equalities_by_source, source, equalities) {
        bool some; jazz_node_CurrentAccessPath path;
        jz_err e = jazz_node__node__query_eval__read_sources__NodeState__guarded_current_access_path(self, read_view, source, equalities, allow_local, allow_secondary_indexes, JAZZ_APS_ORDINARY, NULL, &some, &path);
        if (JZ_IS_ERR(e)) { jz_btree_drop(&paths); jz_btree_drop(&equalities_by_source); return e; }
        if (some) access_paths_insert(&paths, source, path);
    }
    jz_btree_drop(&equalities_by_source);
    *out = paths;
    return JZ_OK;
}

/* The single admission guard for a physical current-source access path.
 * `covered_column` lets a caller that needs a covered key accept a declared
 * [equality, covered_column] composite index when no ordinary path exists. */
static jz_err jazz_node__node__query_eval__read_sources__NodeState__guarded_current_access_path(const jazz_node_NodeState *self, const jazz_engine_ReadView *read_view, const jazz_engine_SourceId *source,
    const jz_btree *equalities, bool allow_local, bool allow_secondary_indexes, jazz_node_AccessPathSelector selector, const char *covered_column, bool *some, jazz_node_CurrentAccessPath *out) {
    *some = false;
    jazz_model_DurabilityTier tier;
    if (!jazz_engine__query_engine__read__ReadView__source_current_tier(read_view, source, &tier)) return JZ_OK;
    /* Local sources still combine the selected settled candidates with the
     * complete ahead overlay before choosing a winner. Any other tier keeps
     * its full source. */
    if (!(tier == JAZZ_TIER_GLOBAL || tier == JAZZ_TIER_LOCAL)) return JZ_OK;
    const jazz_model_TableSchema *table;
    JZ_TRY(jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(self, SL(source->table), read_view->read_schema, &table));
    jazz_node_CurrentAccessPath path; bool selected;
    if (selector == JAZZ_APS_ORDINARY) {
        selected = jazz_node__node__query_eval__normalization__select_current_access_path(table, equalities, &path);
        if (!selected && covered_column)
            selected = jazz_node__node__query_eval__normalization__select_composite_leading_equality_access_path(table, equalities, jz_slice_of(covered_column, (u32)strlen(covered_column)), &path);
    } else {
        selected = jazz_node__node__query_eval__read_sources__select_composite_equality_access_path(table, equalities, &path);
    }
    if (!selected) return JZ_OK;
    /* Authorization dependencies are cached by policy shape and claim schema,
     * not by a resolved claim value: keep those reusable graphs
     * identity-neutral. */
    if (!allow_secondary_indexes && path.tag == JAZZ_CAP_INDEX) { jazz_node_CurrentAccessPath_drop(&path); return JZ_OK; }
    if (!allow_local && path.tag == JAZZ_CAP_INDEX) path.maintained = true;
    *some = true; *out = path;
    return JZ_OK;
}

jz_err jazz_node__node__query_eval__read_sources__NodeState__one_shot_access_paths(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_model_DurabilityTier tier, jz_btree *out) {
    if (!(tier == JAZZ_TIER_LOCAL || tier == JAZZ_TIER_GLOBAL)) { *out = empty_access_paths(); return JZ_OK; }
    jazz_engine_NormalizedRowSetShape normalized;
    JZ_TRY(jazz_node__node__query_eval__normalization__NodeState__normalized_row_set_shape(self, shape, binding, &normalized));
    jz_btree extra, claims; jz_btree_init(&extra, &jazz_engine_ColumnTypes_desc); jz_btree_init(&claims, &jazz_engine_ProgramClaimParams_desc);
    jazz_engine_RowSetProgramInput input;
    input.binding = jazz_node__node__query_eval__bindings__NodeState__program_binding_for_shape(self, shape, binding, NULL, extra, claims);
    input.shape = normalized;
    jazz_engine_RequestedReadSet reads = jazz_node__node__query_eval__query_read_sets__current_query_read_set(&input.shape, shape->schema_version, shape->schema_version, tier, NULL, NULL, false);
    jazz_engine_PolicyContext system; memset(&system, 0, sizeof system); system.tag = JAZZ_PC_SYSTEM;
    jz_btree paths;
    jz_err e = jazz_node__node__query_eval__read_sources__NodeState__normalized_program_access_paths(self, &input, &reads.primary, &system, true, true, &paths);
    jazz_engine_RequestedReadSet_drop(&reads);
    jazz_engine_NormalizedRowSetShape_drop(&input.shape); jazz_engine_ProgramBinding_drop(&input.binding);
    JZ_TRY(e);
    /* A source cap is stronger than an index access path: it is only safe
     * when the source is itself the final result prefix. */
    const jazz_model_Query *query = &shape->query;
    if (!query->has_limit) { *out = paths; return JZ_OK; }
    u32 limit = query->limit;
    if (query->offset != 0 || query->order_by.len != 0 || query->joins.len != 0 || query->flat_join || query->policy_branches.len != 0 || query->reachable.len != 0
        || query->inherits.len != 0 || query->includes.len != 0 || query->array_subqueries.len != 0 || query->aggregate) {
        *out = paths; return JZ_OK;
    }
    jazz_engine_SourceId root = jazz_node__node__query_eval__normalization__root_source_id(query->table.ptr, query->table.len);
    const jazz_model_TableSchema *table;
    e = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(self, SL(query->table), shape->schema_version, &table);
    if (JZ_IS_ERR(e)) { jazz_engine_SourceId_drop(&root); jz_btree_drop(&paths); return e; }
    if (jazz_model__schema__TableSchema__has_any_policy(table)) { jazz_engine_SourceId_drop(&root); *out = paths; return JZ_OK; }
    /* A Local ahead winner can remove an otherwise matching settled row; only
     * the Global physical winner is stable enough for this source bound. */
    if (tier == JAZZ_TIER_GLOBAL) {
        jazz_node_CurrentAccessPath *p = jz_btree_get(&paths, &root);
        if (p && p->tag == JAZZ_CAP_INDEX) { p->has_source_limit = true; p->source_limit = limit; }
    }
    jazz_engine_SourceId_drop(&root);
    *out = paths;
    return JZ_OK;
}

static bool query_is_plain_root(const jazz_model_Query *q) {
    return !q->flat_join && q->policy_branches.len == 0 && q->reachable.len == 0 && q->inherits.len == 0 && q->array_subqueries.len == 0 && !q->relation;
}

/* Binding-specific access paths for an executing current query. Neither a
 * one-result consumer nor a retained subscription may embed these prefixes
 * into a generic prepared-plan cache. */
jz_err jazz_node__node__query_eval__read_sources__NodeState__current_query_hydration_access_paths(const jazz_node_NodeState *self, const jazz_engine_QueryProgramRequest *request,
    const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_node_HydrationLifetime lifetime, jz_btree *out) {
    jz_btree paths; jz_err e;
    const jazz_model_Query *query = &shape->query;
    jazz_engine_SourceId root = jazz_node__node__query_eval__normalization__root_source_id(query->table.ptr, query->table.len);
    if (lifetime == JAZZ_HL_RETAINED) {
        e = jazz_node__node__query_eval__read_sources__NodeState__current_query_primary_key_access_paths(self, shape, binding, &paths);
        if (JZ_IS_ERR(e)) goto fail_root;
    } else {
        /* Preserve bounded current ID reads; the generic cache must not
         * retain binding-specific prefixes. */
        jazz_model_DurabilityTier tier;
        if (!jazz_engine__query_engine__read__ReadView__source_current_tier(&request->reads.primary, &root, &tier)) tier = JAZZ_TIER_NONE;
        jz_btree one_shot;
        e = jazz_node__node__query_eval__read_sources__NodeState__one_shot_access_paths(self, shape, binding, tier, &one_shot);
        if (JZ_IS_ERR(e)) goto fail_root;
        paths = empty_access_paths();
        jazz_engine_SourceId *s; jazz_node_CurrentAccessPath *p;
        JZ_BTREE_FOR(&one_shot, s, p) {
            if (p->tag == JAZZ_CAP_PRIMARY_KEY) { jazz_node_CurrentAccessPath c; jazz_node_CurrentAccessPath_clone(&c, p); access_paths_insert(&paths, s, c); }
        }
        jz_btree_drop(&one_shot);
    }
    {
        jz_btree program;
        e = jazz_node__node__query_eval__read_sources__NodeState__query_program_access_paths(self, request, true, &program);
        if (JZ_IS_ERR(e)) goto fail;
        jazz_engine_SourceId *s; jazz_node_CurrentAccessPath *p;
        JZ_BTREE_FOR(&program, s, p) {
            if (p->tag == JAZZ_CAP_INDEX) { jazz_node_CurrentAccessPath c; jazz_node_CurrentAccessPath_clone(&c, p); access_paths_insert(&paths, s, c); }
        }
        jz_btree_drop(&program);
    }
    if (lifetime == JAZZ_HL_FIRST_RESULT) {
        /* A conjunctive root equality applies to every result even when
         * includes or an existential join add relational nodes; select only
         * this root occurrence here. */
        if (query_is_plain_root(query) && !query->aggregate) {
            if (!jz_btree_contains(&paths, &root)) {
                jz_btree equalities;
                e = jazz_node__node__query_eval__normalization__root_literal_equalities(query, binding, &equalities);
                if (JZ_IS_ERR(e)) goto fail;
                bool some; jazz_node_CurrentAccessPath path;
                e = jazz_node__node__query_eval__read_sources__NodeState__guarded_current_access_path(self, &request->reads.primary, &root, &equalities, true, true, JAZZ_APS_ORDINARY, NULL, &some, &path);
                jz_btree_drop(&equalities);
                if (JZ_IS_ERR(e)) goto fail;
                if (some) {
                    if (path.tag == JAZZ_CAP_INDEX) access_paths_insert(&paths, &root, path);
                    else jazz_node_CurrentAccessPath_drop(&path);
                }
            }
            /* An unordered, unbounded first result may address a declared
             * (a, b) composite index with both equalities as one prefix; it
             * replaces only a plain equality probe. */
            const jazz_node_CurrentAccessPath *existing = jz_btree_get(&paths, &root);
            bool replaceable = !existing || (existing->tag == JAZZ_CAP_INDEX && !existing->has_order_column && !existing->has_source_limit);
            if (replaceable && !query->has_limit && query->order_by.len == 0) {
                jz_btree equalities;
                e = jazz_node__node__query_eval__normalization__root_literal_equalities(query, binding, &equalities);
                if (JZ_IS_ERR(e)) goto fail;
                bool some; jazz_node_CurrentAccessPath path;
                e = jazz_node__node__query_eval__read_sources__NodeState__guarded_current_access_path(self, &request->reads.primary, &root, &equalities, true, true, JAZZ_APS_COMPOSITE_EQUALITY, NULL, &some, &path);
                jz_btree_drop(&equalities);
                if (JZ_IS_ERR(e)) goto fail;
                if (some && path.tag == JAZZ_CAP_INDEX) {
                    const jazz_node_CurrentAccessPath *kept = jz_btree_get(&paths, &root);
                    if (kept && kept->tag == JAZZ_CAP_INDEX) path.maintained = kept->maintained || path.maintained;
                    access_paths_insert(&paths, &root, path);
                } else if (some) jazz_node_CurrentAccessPath_drop(&path);
            }
        }
        jazz_model_DurabilityTier root_tier;
        if (jazz_engine__query_engine__read__ReadView__source_current_tier(&request->reads.primary, &root, &root_tier) && (root_tier == JAZZ_TIER_GLOBAL || root_tier == JAZZ_TIER_LOCAL)) {
            jazz_node_CurrentAccessPath *p = jz_btree_get(&paths, &root);
            /* A first-result owner retires this graph after hydration:
             * intersect durable index keys before fetching complete rows. */
            if (p && p->tag == JAZZ_CAP_INDEX && p->intersections.len != 0) p->maintained = false;
        }
        if (query->joins.len == 1 && query_is_plain_root(query)) {
            const jazz_model_JoinVia *join = &query->joins.ptr[0];
            if (join->target == JAZZ_JOIN_TARGET_COLUMN && !join->has_source_column && !join->has_source_lookup && join->correlated_filters.len == 0 && join->nested_joins.len == 0) {
                /* An exact root id fixes the junction's foreign key; without
                 * one, a conjunctive junction filter can still narrow this
                 * occurrence before the ordinary join. */
                jz_btree root_eq;
                e = jazz_node__node__query_eval__normalization__root_literal_equalities(query, binding, &root_eq);
                if (JZ_IS_ERR(e)) goto fail;
                jz_str id_key = jz_str_from("id");
                const groove_Value *idv = jz_btree_get(&root_eq, &id_key);
                jz_str_drop(&id_key);
                bool has_root_id = idv && idv->tag == GROOVE_V_UUID; jz_uuid row_id = has_root_id ? idv->v.uuid : (jz_uuid){0};
                jz_btree_drop(&root_eq);
                jz_btree equalities;
                if (has_root_id) {
                    equalities = empty_equalities();
                    jz_str k2 = jz_str_clone(&join->on_column); groove_Value v2 = groove_Value_uuid(row_id);
                    jz_btree_insert(&equalities, &k2, &v2, NULL);
                } else {
                    e = jazz_node__node__query_eval__normalization__literal_equalities_for_filters(&join->filters, binding, &equalities);
                    if (JZ_IS_ERR(e)) goto fail;
                }
                /* Key the path by the occurrence normalization actually
                 * emitted; admission uses the same guard as the ordinary
                 * selector. */
                const jazz_engine_SourceId *join_source = jazz_node__node__query_eval__read_sources__single_root_join_source(request, SL(join->table));
                if (join_source) {
                    jazz_model_DurabilityTier t1, t2;
                    bool both_global = jazz_engine__query_engine__read__ReadView__source_current_tier(&request->reads.primary, &root, &t1) && t1 == JAZZ_TIER_GLOBAL
                        && jazz_engine__query_engine__read__ReadView__source_current_tier(&request->reads.primary, join_source, &t2) && t2 == JAZZ_TIER_GLOBAL;
                    bool covered = !has_root_id && both_global && !query->has_limit && query->order_by.len == 0 && !query->aggregate;
                    char *covered_cstr = NULL;
                    if (covered) { covered_cstr = jz_alloc(join->on_column.len + 1); memcpy(covered_cstr, join->on_column.ptr, join->on_column.len); covered_cstr[join->on_column.len] = 0; }
                    bool some; jazz_node_CurrentAccessPath path;
                    e = jazz_node__node__query_eval__read_sources__NodeState__guarded_current_access_path(self, &request->reads.primary, join_source, &equalities, true, true, JAZZ_APS_ORDINARY, covered_cstr, &some, &path);
                    if (JZ_IS_ERR(e)) { jz_free(covered_cstr); jz_btree_drop(&equalities); goto fail; }
                    if (some && path.tag == JAZZ_CAP_INDEX) {
                        if (covered) {
                            jazz_model_TableSchema join_table;
                            e = jazz_node__node__NodeState__table_in_schema((jazz_node_NodeState *)self, SL(join->table), request->reads.primary.read_schema, &join_table);
                            if (JZ_IS_ERR(e)) { jz_free(covered_cstr); jz_btree_drop(&equalities); jazz_node_CurrentAccessPath_drop(&path); goto fail; }
                            jazz_node__node__query_eval__read_sources__attach_covered_join_key_filter(&join_table, SL(join->on_column), SL(query->table), jz_btree_get(&paths, &root), &path);
                            jazz_model_TableSchema_drop(&join_table);
                        }
                        access_paths_insert(&paths, join_source, path);
                    } else if (some) jazz_node_CurrentAccessPath_drop(&path);
                    jz_free(covered_cstr);
                }
                jz_btree_drop(&equalities);
            }
        }
    }
    jazz_engine_SourceId_drop(&root);
    *out = paths;
    return JZ_OK;
fail:
    jz_btree_drop(&paths);
fail_root:
    jazz_engine_SourceId_drop(&root);
    return e;
}

jz_err jazz_node__node__query_eval__read_sources__NodeState__current_query_primary_key_access_paths(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jz_btree *out);
static jz_err jazz_node__node__query_eval__read_sources__NodeState__add_reachable_access_paths(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_types_SchemaVersionId schema_version,
    const jazz_model_Binding *binding, jz_btree *access_paths);
static jz_err jazz_node__node__query_eval__read_sources__NodeState__add_primary_key_access_path_for_filters(const jazz_node_NodeState *self, const jazz_engine_SourceId *source, jz_slice table_name,
    jazz_types_SchemaVersionId schema_version, const jazz_model_PredicateVec *filters, const jazz_model_Binding *binding, jz_btree *access_paths);

jz_err jazz_node__node__query_eval__read_sources__NodeState__current_query_primary_key_access_paths(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jz_btree *out) {
    const jazz_model_Query *query = &shape->query;
    jz_btree access_paths = empty_access_paths();
    jz_btree equalities;
    JZ_TRY(jazz_node__node__query_eval__normalization__root_literal_equalities(query, binding, &equalities));
    const jazz_model_TableSchema *t;
    jz_err e = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(self, SL(query->table), shape->schema_version, &t);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&equalities); return e; }
    /* Policy-bearing roots take the point path too (#3511): the point narrows
     * only the content source; deletion and grant changes still retract. */
    jz_str id_key = jz_str_from("id");
    const groove_Value *value = jz_btree_get(&equalities, &id_key);
    jz_str_drop(&id_key);
    if (value) {
        jazz_node_CurrentAccessPath p; memset(&p, 0, sizeof p); p.tag = JAZZ_CAP_PRIMARY_KEY;
        groove_Value vc; groove_Value_clone_v(&vc, value); JZ_VEC_PUSH(p.prefix, vc);
        jazz_engine_SourceId root = jazz_node__node__query_eval__normalization__root_source_id(query->table.ptr, query->table.len);
        access_paths_insert(&access_paths, &root, p);
        jazz_engine_SourceId_drop(&root);
    }
    jz_btree_drop(&equalities);
    e = jazz_node__node__query_eval__read_sources__NodeState__add_reachable_access_paths(self, query, shape->schema_version, binding, &access_paths);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&access_paths); return e; }
    *out = access_paths;
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__read_sources__NodeState__add_reachable_access_paths(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_types_SchemaVersionId schema_version,
    const jazz_model_Binding *binding, jz_btree *access_paths) {
    for (u32 index = 0; index < query->reachable.len; index++) {
        const jazz_model_ReachableVia *reachable = &query->reachable.ptr[index];
        jz_str reachable_id = jz_format("reachable:%u", index);
        jz_err e = JZ_OK;
        if (reachable->has_seed) {
            jazz_engine_SourceId source = jazz_node__node__query_eval__normalization__reachable_seed_source_id(&reachable->seed, SL(reachable_id));
            e = jazz_node__node__query_eval__read_sources__NodeState__add_primary_key_access_path_for_filters(self, &source, SL(reachable->seed.table), schema_version, &reachable->seed.filters, binding, access_paths);
            jazz_engine_SourceId_drop(&source);
        }
        if (!JZ_IS_ERR(e)) {
            jazz_engine_SourceId edge_source = jazz_node__node__query_eval__normalization__reachable_edge_source_id(reachable, SL(reachable_id));
            e = jazz_node__node__query_eval__read_sources__NodeState__add_primary_key_access_path_for_filters(self, &edge_source, SL(reachable->edge_table), schema_version, &reachable->edge_filters, binding, access_paths);
            jazz_engine_SourceId_drop(&edge_source);
        }
        if (!JZ_IS_ERR(e)) {
            jazz_engine_SourceId access_source = jazz_node__node__query_eval__normalization__reachable_access_source_id(reachable, SL(reachable_id));
            e = jazz_node__node__query_eval__read_sources__NodeState__add_primary_key_access_path_for_filters(self, &access_source, SL(reachable->access_table), schema_version, &reachable->access_filters, binding, access_paths);
            jazz_engine_SourceId_drop(&access_source);
        }
        jz_str_drop(&reachable_id);
        JZ_TRY(e);
    }
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__read_sources__NodeState__add_primary_key_access_path_for_filters(const jazz_node_NodeState *self, const jazz_engine_SourceId *source, jz_slice table_name,
    jazz_types_SchemaVersionId schema_version, const jazz_model_PredicateVec *filters, const jazz_model_Binding *binding, jz_btree *access_paths) {
    const jazz_model_TableSchema *table;
    JZ_TRY(jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(self, table_name, schema_version, &table));
    jz_btree equalities;
    JZ_TRY(jazz_node__node__query_eval__normalization__literal_equalities_for_filters(filters, binding, &equalities));
    jazz_node_CurrentAccessPath path;
    if (jazz_node__node__query_eval__normalization__select_current_access_path(table, &equalities, &path)) {
        if (path.tag == JAZZ_CAP_PRIMARY_KEY) access_paths_insert(access_paths, source, path);
        else jazz_node_CurrentAccessPath_drop(&path);
    }
    jz_btree_drop(&equalities);
    return JZ_OK;
}

jz_err jazz_node__node__query_eval__read_sources__NodeState__physical_global_current_source_for_index_scan(const jazz_node_NodeState *self, const jazz_model_TableSchema *table, jazz_types_SchemaVersionId schema_version,
    jz_slice column, const jz_str *order_column, bool reverse, const groove_ValueVec *prefix, const jazz_node_IndexIntersectionVec *intersections, bool maintained,
    bool has_source_limit, u32 source_limit, const jazz_node_CurrentIndexCandidateFilter *candidate_filter, jz_slice projection_target, groove_GraphBuilder **out) {
    return jazz_node__node__query_eval__read_sources__NodeState__physical_global_current_source_for_index_scan_with_output(self, table, schema_version, column, order_column, reverse, prefix, intersections, maintained,
        has_source_limit, source_limit, candidate_filter, projection_target, groove__schema__TableSchema__record_schema(jazz_model__schema__TableSchema__global_current_storage_table(table)), out);
}

jz_err jazz_node__node__query_eval__read_sources__NodeState__physical_global_current_source_for_index_scan_with_output(const jazz_node_NodeState *self, const jazz_model_TableSchema *table, jazz_types_SchemaVersionId schema_version,
    jz_slice column, const jz_str *order_column, bool reverse, const groove_ValueVec *prefix, const jazz_node_IndexIntersectionVec *intersections, bool maintained,
    bool has_source_limit, u32 source_limit, const jazz_node_CurrentIndexCandidateFilter *candidate_filter, jz_slice projection_target, groove_RecordDescriptor output, groove_GraphBuilder **out) {
    (void)output;
    jazz_model_BranchKey default_key; memset(&default_key, 0, sizeof default_key);
    return jazz_node__node__query_eval__read_sources__NodeState__physical_current_source_for_index_scan_in(self, JAZZ_PCC_GLOBAL, table, schema_version, &default_key, column, order_column, reverse, prefix, intersections,
        maintained, has_source_limit, source_limit, candidate_filter, projection_target, out);
}

static jz_err table_mapping_for(const jazz_node_NodeState *self, jazz_types_SchemaVersionId schema_version, const jz_str *table, const char *missing, const jazz_node_PhysicalTableMapping **out) {
    const jazz_node_PhysicalMapping *mapping = jz_btree_get(&self->catalogue.physical_mappings, &schema_version);
    const jazz_node_PhysicalTableMapping *tm = mapping ? jz_btree_get(&mapping->tables, table) : NULL;
    if (!tm) return JZN_INVALID_STORED(missing);
    *out = tm;
    return JZ_OK;
}
static jz_err column_id_for(const jazz_node_PhysicalTableMapping *m, jz_slice column, const char *missing, jazz_types_PhysicalColumnId *out) {
    jz_str k = jz_str_from_slice(column);
    const jazz_types_PhysicalColumnId *id = jz_btree_get(&m->columns, &k);
    jz_str_drop(&k);
    if (!id) return JZN_INVALID_STORED(missing);
    *out = *id;
    return JZ_OK;
}
static groove_ValueVec branch_index_prefix(const jazz_model_BranchKey *branch_key, const groove_ValueVec *prefix) {
    groove_ValueVec v = {0};
    JZ_VEC_PUSH(v, groove_Value_bytes(jz_bytes_clone(&branch_key->canonical)));
    for (u32 i = 0; i < prefix->len; i++) { groove_Value c; groove_Value_clone_v(&c, &prefix->ptr[i]); JZ_VEC_PUSH(v, c); }
    return v;
}
static groove_LiteralVec literals_from_values(const groove_ValueVec *values) {
    groove_LiteralVec out = {0};
    for (u32 i = 0; i < values->len; i++) { groove_Value c; groove_Value_clone_v(&c, &values->ptr[i]); groove_LiteralValue *l = groove__ivm__op_types__LiteralValue__from_value(c); JZ_VEC_PUSH(out, l); }
    return out;
}

/* How many rows a physical_current_source_for_index_scan_in read of `class`
 * under `prefix` of one branch key starts from, counting no further than
 * max_items. Reads index keys only. */
jz_err jazz_node__node__query_eval__read_sources__NodeState__physical_current_index_prefix_rows_up_to(const jazz_node_NodeState *self, jazz_node_PhysicalCurrentClass cls, const jazz_model_TableSchema *table, jazz_types_SchemaVersionId schema_version,
    const jazz_model_BranchKey *branch_key, jz_slice column, const jz_str *order_column, const groove_ValueVec *prefix, u32 max_items, u32 *out) {
    const jazz_node_PhysicalTableMapping *mapping;
    JZ_TRY(table_mapping_for(self, schema_version, &table->name, "physical current index table mapping missing", &mapping));
    jz_str index;
    jazz_types_PhysicalColumnId ids[2];
    JZ_TRY(column_id_for(mapping, column, "physical current index column mapping missing", &ids[0]));
    if (order_column) {
        JZ_TRY(column_id_for(mapping, SL(*order_column), "physical current index column mapping missing", &ids[1]));
        index = jazz_node__node__physical__physical_current_composite_index_name_ids(ids, 2);
    } else {
        index = jazz_node__node__physical__physical_current_index_name_id(ids[0]);
    }
    jz_str storage_table;
    jz_err e = jazz_node__node__physical__NodeState__physical_current_table_for_schema(self, schema_version, SL(table->name), cls, &storage_table);
    if (JZ_IS_ERR(e)) { jz_str_drop(&index); return e; }
    groove_ValueVec scan_prefix = branch_index_prefix(branch_key, prefix);
    e = self->database.vt->index_prefix_entries_up_to(self->database.self, SL(storage_table), SL(index), scan_prefix.ptr, scan_prefix.len, max_items, out);
    valuevec_drop(&scan_prefix); jz_str_drop(&storage_table); jz_str_drop(&index);
    return jzn_groove(e);
}

/* Read one physical current class under an index prefix of one branch key.
 * Physical current indexes lead with the branch key, so a branch layer's
 * index read never touches another branch's entries. */
jz_err jazz_node__node__query_eval__read_sources__NodeState__physical_current_source_for_index_scan_in(const jazz_node_NodeState *self, jazz_node_PhysicalCurrentClass cls, const jazz_model_TableSchema *table, jazz_types_SchemaVersionId schema_version,
    const jazz_model_BranchKey *branch_key, jz_slice column, const jz_str *order_column, bool reverse, const groove_ValueVec *prefix, const jazz_node_IndexIntersectionVec *intersections, bool maintained,
    bool has_source_limit, u32 source_limit, const jazz_node_CurrentIndexCandidateFilter *candidate_filter, jz_slice projection_target, groove_GraphBuilder **out) {
    const jazz_node_PhysicalTableMapping *mapping;
    JZ_TRY(table_mapping_for(self, schema_version, &table->name, "physical current index table mapping missing", &mapping));
    jazz_types_PhysicalColumnId column_id;
    JZ_TRY(column_id_for(mapping, column, "physical current index column mapping missing", &column_id));
    jz_str storage_table;
    JZ_TRY(jazz_node__node__physical__NodeState__physical_current_table_for_schema(self, schema_version, SL(table->name), cls, &storage_table));
    /* Root reads address the shared (empty) branch coordinate; physical
     * current indexes include that coordinate first. */
    groove_ValueVec scan_prefix = branch_index_prefix(branch_key, prefix);
    groove_StaticScanSpec scan; memset(&scan, 0, sizeof scan);
    scan.prefix = literals_from_values(&scan_prefix);
    if (has_source_limit && reverse) { scan.tag = GROOVE_SCAN_REVERSE_PREFIX_LIMIT; scan.max_items = source_limit; }
    else if (has_source_limit) { scan.tag = GROOVE_SCAN_PREFIX_LIMIT; scan.max_items = source_limit; }
    else scan.tag = GROOVE_SCAN_PREFIX;
    valuevec_drop(&scan_prefix);
    groove_IndexScanVec isect = {0};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < intersections->len; i++) {
        jazz_types_PhysicalColumnId cid;
        e = column_id_for(mapping, SL(intersections->ptr[i].column), "physical current intersected index column mapping missing", &cid);
        if (JZ_IS_ERR(e)) goto fail;
        groove_IndexScan is; memset(&is, 0, sizeof is);
        is.index = jazz_node__node__physical__physical_current_index_name_id(cid);
        groove_ValueVec ip = branch_index_prefix(branch_key, &intersections->ptr[i].values);
        is.scan.tag = GROOVE_SCAN_PREFIX; is.scan.prefix = literals_from_values(&ip);
        valuevec_drop(&ip);
        JZ_VEC_PUSH(isect, is);
    }
    jz_str primary_index;
    if (order_column) {
        jazz_types_PhysicalColumnId ids[2] = { column_id, 0 };
        e = column_id_for(mapping, SL(*order_column), "physical current ordered index column mapping missing", &ids[1]);
        if (JZ_IS_ERR(e)) goto fail;
        primary_index = jazz_node__node__physical__physical_current_composite_index_name_ids(ids, 2);
    } else {
        primary_index = jazz_node__node__physical__physical_current_index_name_id(column_id);
    }
    if (maintained) {
        /* IndexedRowsIntersection is a hydration request, not a live source:
         * model each equality as an index source and express the intersection
         * in IVM so table deltas drive ordinary semi-join updates. */
        groove_GraphBuilder *graph = groove__ivm__graph__GraphBuilder__variant_index_scan(SL(storage_table), SL(primary_index), scan, projection_target);
        for (u32 i = 0; i < isect.len; i++) {
            groove_GraphBuilder *right = groove__ivm__graph__GraphBuilder__variant_index_scan(SL(storage_table), SL(isect.ptr[i].index), isect.ptr[i].scan, projection_target);
            jz_str_drop(&isect.ptr[i].index);
            graph = semi_join_row_uuid(graph, right);
        }
        jz_free(isect.ptr);
        *out = graph;
    } else if (candidate_filter && isect.len == 0 && !has_source_limit) {
        /* The candidate scan and this source are separate storage reads, so
         * this shape is only for a first-result snapshot. */
        const jazz_node_PhysicalTableMapping *candidate_mapping;
        e = table_mapping_for(self, schema_version, &candidate_filter->table, "candidate index table mapping missing", &candidate_mapping);
        if (JZ_IS_ERR(e)) { jz_str_drop(&primary_index); groove_StaticScanSpec_drop(&scan); goto fail; }
        jazz_types_PhysicalColumnId cids[2];
        e = column_id_for(candidate_mapping, SL(candidate_filter->column), "candidate index column mapping missing", &cids[0]);
        if (!JZ_IS_ERR(e) && candidate_filter->has_order_column) e = column_id_for(candidate_mapping, SL(candidate_filter->order_column), "candidate index column mapping missing", &cids[1]);
        if (JZ_IS_ERR(e)) { jz_str_drop(&primary_index); groove_StaticScanSpec_drop(&scan); goto fail; }
        jz_str candidate_index = candidate_filter->has_order_column ? jazz_node__node__physical__physical_current_composite_index_name_ids(cids, 2) : jazz_node__node__physical__physical_current_index_name_id(cids[0]);
        jazz_types_PhysicalColumnId source_column_id;
        e = column_id_for(mapping, SL(candidate_filter->source_column), "candidate-filtered source column mapping missing", &source_column_id);
        if (JZ_IS_ERR(e)) { jz_str_drop(&candidate_index); jz_str_drop(&primary_index); groove_StaticScanSpec_drop(&scan); goto fail; }
        groove_IndexCandidateFilter f; memset(&f, 0, sizeof f);
        f.table = jazz_node__node__physical__physical_global_current_table_name(candidate_mapping->table_id);
        f.index = candidate_index;
        groove_ValueVec cp = branch_index_prefix(branch_key, &candidate_filter->prefix);
        f.scan.tag = GROOVE_SCAN_PREFIX; f.scan.prefix = literals_from_values(&cp);
        valuevec_drop(&cp);
        f.source_column = jazz_node__node__physical__physical_user_column_field(source_column_id);
        f.candidate_column = jz_str_from("row_uuid");
        groove_IndexCandidateFilter *boxed = jz_alloc(sizeof f); *boxed = f;
        *out = groove__ivm__graph__GraphBuilder__variant_index_candidate_scan(SL(storage_table), SL(primary_index), scan, boxed, projection_target);
        jz_free(isect.ptr);
    } else {
        *out = groove__ivm__graph__GraphBuilder__variant_index_intersection_scan(SL(storage_table), SL(primary_index), scan, isect.ptr, isect.len, projection_target);
    }
    jz_str_drop(&primary_index); jz_str_drop(&storage_table);
    return JZ_OK;
fail:
    for (u32 i = 0; i < isect.len; i++) { jz_str_drop(&isect.ptr[i].index); groove_StaticScanSpec_drop(&isect.ptr[i].scan); }
    jz_free(isect.ptr);
    jz_str_drop(&storage_table);
    return e;
}

/* Return only restrictions that are plainly safe to push into a physical
 * source: never through relational operators or alternative branches. */
static jz_err jazz_node__node__query_eval__read_sources__normalized_program_equalities(const jazz_engine_NormalizedRowSetShape *shape, const jazz_engine_RowSetNodeId *node_id,
    const jazz_engine_ProgramBinding *binding, const jazz_engine_PolicyContext *policy, bool *some, jz_btree *out) {
    const jazz_engine_RowSetExpr *node = jz_btree_get(&shape->nodes, node_id);
    if (!node) return JZN_INVALID_STORED("normalized access-path node missing");
    switch (node->tag) {
    case JAZZ_RSE_FILTER: {
        JZ_TRY(jazz_node__node__query_eval__read_sources__normalized_program_equalities(shape, &node->input, binding, policy, some, out));
        if (!*some) return JZ_OK;
        jz_err e = jazz_node__node__query_eval__read_sources__collect_normalized_program_equalities(&node->predicate, binding, policy, out);
        if (JZ_IS_ERR(e)) { jz_btree_drop(out); return e; }
        return JZ_OK;
    }
    case JAZZ_RSE_DISTINCT: case JAZZ_RSE_ORDER_BY: case JAZZ_RSE_PROJECT: case JAZZ_RSE_SLICE:
        return jazz_node__node__query_eval__read_sources__normalized_program_equalities(shape, &node->input, binding, policy, some, out);
    case JAZZ_RSE_SOURCE: {
        jz_btree_init(out, &source_equalities_desc);
        jazz_engine_SourceId k; jazz_engine_SourceId_clone(&k, &node->source);
        jz_btree v = empty_equalities();
        jz_btree_insert(out, &k, &v, NULL);
        *some = true;
        return JZ_OK;
    }
    default:
        *some = false;
        return JZ_OK;
    }
}

static bool jazz_node__node__query_eval__read_sources__normalized_program_source_field(const jazz_engine_NormalizedValueRef *value, jazz_engine_SourceId *source, jz_str *field) {
    if (value->tag == JAZZ_NVR_SOURCE_FIELD) { jazz_engine_SourceId_clone(source, &value->source); *field = jz_str_clone(&value->field); return true; }
    if (value->tag == JAZZ_NVR_ROW_ID && value->row_id_kind == JAZZ_ROWID_SOURCE) { jazz_engine_SourceId_clone(source, &value->source); *field = jz_str_from("id"); return true; }
    return false;
}

static jz_err jazz_node__node__query_eval__read_sources__normalized_bound_value(const jazz_engine_NormalizedValueRef *value, const jazz_engine_ProgramBinding *binding,
    const jazz_engine_PolicyContext *policy, bool *some, groove_Value *out) {
    *some = false;
    switch (value->tag) {
    case JAZZ_NVR_PARAM: {
        const groove_Value *v = jz_btree_get(&binding->values, &value->field);
        if (v) { groove_Value_clone_v(out, v); *some = true; }
        return JZ_OK;
    }
    case JAZZ_NVR_LITERAL: {
        jz_err e = groove__records__Value__from_postcard(jz_bytes_slice(&value->literal), out);
        if (JZ_IS_ERR(e)) {
            jz_err le = JZN_QUERY_LOWERING("literal decoding failed: %.*s", e.msg ? (int)e.msg->len : 0, e.msg ? e.msg->ptr : "");
            jz_err_drop(&e);
            return le;
        }
        *some = true;
        return JZ_OK;
    }
    case JAZZ_NVR_CLAIM:
        /* Access-path selection is optional: system reads have no identity
         * from which to bind a claim, so they retain the ordinary scan. */
        if (policy->tag == JAZZ_PC_SYSTEM) return JZ_OK;
        return jazz_node__node__query_eval__prepared_bindings__prepared_claim_value(&value->claim, policy, some, out);
    default:
        return JZ_OK;
    }
}

static jz_err jazz_node__node__query_eval__read_sources__normalized_program_equality(const jazz_engine_NormalizedValueRef *field, const jazz_engine_NormalizedValueRef *value,
    const jazz_engine_ProgramBinding *binding, const jazz_engine_PolicyContext *policy, bool *some, jazz_engine_SourceId *source_out, jz_str *field_out, groove_Value *value_out) {
    *some = false;
    jazz_engine_SourceId source; jz_str f;
    if (!jazz_node__node__query_eval__read_sources__normalized_program_source_field(field, &source, &f)) return JZ_OK;
    bool has; groove_Value v;
    jz_err e = jazz_node__node__query_eval__read_sources__normalized_bound_value(value, binding, policy, &has, &v);
    if (JZ_IS_ERR(e) || !has) { jazz_engine_SourceId_drop(&source); jz_str_drop(&f); return e; }
    if (v.tag == GROOVE_V_NULLABLE) {
        /* The physical current index stores its own nullable envelope: do not
         * manufacture a nested nullable prefix. */
        groove_Value_drop_v(&v); jazz_engine_SourceId_drop(&source); jz_str_drop(&f);
        return JZ_OK;
    }
    *some = true; *source_out = source; *field_out = f; *value_out = v;
    return JZ_OK;
}

static void equalities_entry_or_insert(jz_btree *equalities, jazz_engine_SourceId source, jz_str field, groove_Value value) {
    jz_btree *entry = jz_btree_get(equalities, &source);
    if (!entry) {
        jz_btree fresh = empty_equalities();
        jazz_engine_SourceId k; jazz_engine_SourceId_clone(&k, &source);
        jz_btree_insert(equalities, &k, &fresh, NULL);
        entry = jz_btree_get(equalities, &source);
    }
    jazz_engine_SourceId_drop(&source);
    if (jz_btree_contains(entry, &field)) { jz_str_drop(&field); groove_Value_drop_v(&value); return; }
    jz_btree_insert(entry, &field, &value, NULL);
}

static jz_err jazz_node__node__query_eval__read_sources__collect_normalized_program_equalities(const jazz_engine_PredicateExpr *predicate, const jazz_engine_ProgramBinding *binding,
    const jazz_engine_PolicyContext *policy, jz_btree *equalities) {
    switch (predicate->tag) {
    case JAZZ_NP_AND:
        for (u32 i = 0; i < predicate->children.len; i++)
            JZ_TRY(jazz_node__node__query_eval__read_sources__collect_normalized_program_equalities(&predicate->children.ptr[i], binding, policy, equalities));
        break;
    case JAZZ_NP_COMPARE: {
        if (predicate->op != JAZZ_CMP_EQ) break;
        bool some; jazz_engine_SourceId source; jz_str field; groove_Value value;
        JZ_TRY(jazz_node__node__query_eval__read_sources__normalized_program_equality(&predicate->left, &predicate->right, binding, policy, &some, &source, &field, &value));
        if (some) { equalities_entry_or_insert(equalities, source, field, value); break; }
        JZ_TRY(jazz_node__node__query_eval__read_sources__normalized_program_equality(&predicate->right, &predicate->left, binding, policy, &some, &source, &field, &value));
        if (some) equalities_entry_or_insert(equalities, source, field, value);
        break;
    }
    /* Do not descend into a disjunction: an equality within only one branch
     * is not a restriction on the other branch. */
    default: break;
    }
    return JZ_OK;
}

jz_vec_str jazz_node__node__query_eval__read_sources__current_row_fields(const jazz_model_TableSchema *table) {
    return jazz_engine__query_engine__fields__current_row_field_names(table);
}

jz_vec_str jazz_node__node__query_eval__read_sources__global_current_storage_fields(const jazz_model_TableSchema *table, bool include_version, bool include_settle_position) {
    jz_vec_str fields = {0};
    strvec_push_cstr(&fields, "row_uuid");
    if (include_version) { strvec_push_cstr(&fields, "schema_version"); strvec_push_cstr(&fields, "authored_columns"); }
    for (u32 i = 0; i < table->columns.len; i++) { jz_str f = jazz_engine__query_engine__fields__user_column_field(SL(table->columns.ptr[i].name)); jz_vec_push(&fields, &f, sizeof f); }
    static const char *const tail[] = { "created_by", "created_at", "updated_by", "updated_at", "tx_time", "tx_node_id" };
    for (u32 i = 0; i < 6; i++) strvec_push_cstr(&fields, tail[i]);
    if (include_settle_position) strvec_push_cstr(&fields, "global_time");
    return fields;
}

static groove_RecordDescriptor jazz_node__node__query_eval__read_sources__current_row_descriptor(const jazz_model_TableSchema *table) {
    /* Source metadata and the executable canonical-row projection must carry
     * the same logical identities. */
    jz_btree empty = empty_metadata();
    groove_RecordDescriptor d = jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields(table, &empty);
    jz_btree_drop(&empty);
    return d;
}

groove_GraphBuilder *jazz_node__node__query_eval__read_sources__empty_authorized_row_id_graph(void) {
    groove_DescriptorFieldVec fields = {0};
    push_df(&fields, "row_uuid", groove_ValueType_simple(GROOVE_VT_UUID));
    jz_vec_bytes none = {0};
    return groove__ivm__graph__GraphBuilder__inline_records(groove__records__RecordDescriptor__new(fields), none);
}

/* ---------- inline current records ---------- */

static jz_err jazz_node__node__query_eval__read_sources__row_author_value(jazz_types_AuthorSubject author, groove_Value *out) {
    jz_err e = jazz_types__ids__RowAuthor__from_subject_value(author, out);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return jz_err_fmt(JZN_E_UnadmittedWriteAuthor, "%s", "write author is not admitted"); }
    return JZ_OK;
}

typedef JZ_VEC(groove_Value) ValueBuf;
static void vpush(ValueBuf *v, groove_Value x) { jz_vec_push(v, &x, sizeof x); }
static groove_Value boxed_nullable(bool some, groove_Value inner) {
    groove_Value v = groove_Value_null();
    if (some) { v.v.nullable = jz_alloc(sizeof(groove_Value)); *v.v.nullable = inner; }
    return v;
}

static jz_err jazz_node__node__query_eval__read_sources__append_author_projection_values(ValueBuf *values, groove_RecordDescriptor descriptor, jazz_types_AuthorSubject created_by, jazz_types_AuthorSubject updated_by) {
    const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(descriptor);
    for (u32 i = 0; i < fields->len; i++) {
        const groove_DescriptorField *field = &fields->ptr[i];
        if (!field->has_name) continue;
        jz_slice root; const char *const *path; u32 n;
        if (!jazz_types__ids__AuthorSubject__metadata_path(SL(field->name), &root, &path, &n)) continue;
        jazz_types_AuthorSubject author = jz_slice_cmp(root, CS("$createdBy")) == 0 ? created_by : updated_by;
        groove_Value value;
        JZ_TRY(jazz_node__node__query_eval__read_sources__row_author_value(author, &value));
        for (u32 j = 0; j < n; j++) {
            if (value.tag != GROOVE_V_RECORD) jz_panic("validated author record");
            groove_Value member;
            if (!groove__records__values__Value__record_get(&value, jz_slice_of(path[j], (u32)strlen(path[j])), &member)) jz_panic("validated author field");
            groove_Value_drop(&value);
            value = member;
        }
        vpush(values, value);
    }
    return JZ_OK;
}

static jz_err row_provenance(const jazz_node_CurrentRow *row, jazz_node_RowProvenance *p) {
    bool some;
    JZ_TRY(jazz_node__node__CurrentRow__provenance(row, &some, p));
    if (!some) return JZN_INVALID_STORED("physical current source row is missing provenance");
    return JZ_OK;
}
static void push_tx_alias(ValueBuf *values, const jazz_node_CurrentRow *row) {
    u64 tx_time = 0, alias = 0;
    if (!jazz_node__node__CurrentRow__projected_tx_alias(row, &tx_time, &alias)) { tx_time = 0; alias = 0; }
    vpush(values, groove_Value_u64(tx_time));
    vpush(values, groove_Value_u64(alias));
}
static jz_err push_provenance_values(ValueBuf *values, const jazz_node_RowProvenance *p) {
    groove_Value a;
    JZ_TRY(jazz_node__node__query_eval__read_sources__row_author_value(p->created_by, &a)); vpush(values, a);
    vpush(values, groove_Value_u64(p->created_at));
    JZ_TRY(jazz_node__node__query_eval__read_sources__row_author_value(p->updated_by, &a)); vpush(values, a);
    vpush(values, groove_Value_u64(p->updated_at));
    return JZ_OK;
}
static void valuebuf_drop(ValueBuf *v) { jz_vec_drop(v, sizeof(groove_Value), groove_Value_drop_v); }

static jz_err jazz_node__node__query_eval__read_sources__inline_current_record(const jazz_model_TableSchema *table, groove_RecordDescriptor descriptor, const jazz_node_CurrentRow *row, jz_bytes *out) {
    ValueBuf values = {0};
    vpush(&values, groove_Value_uuid(row->row_uuid.u));
    for (u32 i = 0; i < table->columns.len; i++) {
        groove_Value cell; bool some = jazz_node__node__CurrentRow__cell(row, table, SL(table->columns.ptr[i].name), &cell);
        vpush(&values, boxed_nullable(some, cell));
    }
    jazz_node_RowProvenance p; jz_err e;
    if (JZ_IS_ERR(e = row_provenance(row, &p)) || JZ_IS_ERR(e = push_provenance_values(&values, &p))) { valuebuf_drop(&values); return e; }
    push_tx_alias(&values, row);
    e = jazz_node__node__query_eval__read_sources__append_author_projection_values(&values, descriptor, p.created_by, p.updated_by);
    if (!JZ_IS_ERR(e)) e = jzn_groove(groove__records__RecordDescriptor__create(descriptor, values.ptr, values.len, out));
    valuebuf_drop(&values);
    return e;
}

static jz_err jazz_node__node__query_eval__read_sources__inline_current_graph(const jazz_model_TableSchema *table, jazz_node_CurrentRowVec rows, groove_GraphBuilder **out) {
    groove_RecordDescriptor descriptor = jazz_node__node__query_eval__read_sources__current_row_descriptor(table);
    jz_vec_bytes records = {0};
    for (u32 i = 0; i < rows.len; i++) {
        jz_bytes r;
        jz_err e = jazz_node__node__query_eval__read_sources__inline_current_record(table, descriptor, &rows.ptr[i], &r);
        if (JZ_IS_ERR(e)) { for (u32 j = 0; j < records.len; j++) jz_bytes_drop(&records.ptr[j]); jz_free(records.ptr); jazz_node_CurrentRowVec_drop(&rows); return e; }
        jz_vec_push(&records, &r, sizeof r);
    }
    jazz_node_CurrentRowVec_drop(&rows);
    *out = groove__ivm__graph__GraphBuilder__inline_records(descriptor, records);
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__read_sources__inline_current_graph_with_source_metadata(const jazz_model_TableSchema *table, jazz_node_CurrentRowVec rows, SchemaVersionAlias alias, const char *coverage,
    const jazz_engine_SourceRequirements *requirements, groove_GraphBuilder **out_graph, groove_RecordDescriptor *out_desc, jz_btree *out_meta) {
    return jazz_node__node__query_eval__read_sources__inline_current_graph_with_source_metadata_and_branch_witness(table, rows, alias, coverage, requirements, NULL, NULL, out_graph, out_desc, out_meta);
}

static jz_err jazz_node__node__query_eval__read_sources__inline_current_values_with_source_metadata_and_deletion(const jazz_model_TableSchema *table, groove_RecordDescriptor descriptor, const jazz_node_CurrentRow *row,
    SchemaVersionAlias alias, const char *coverage, const jazz_model_BranchKey *branch_witness_key, bool has_deletion_marker, bool deletion_marker, ValueBuf *out);

static jz_err jazz_node__node__query_eval__read_sources__inline_current_record_with_source_metadata_and_deletion(const jazz_model_TableSchema *table, groove_RecordDescriptor descriptor, const jazz_node_CurrentRow *row,
    SchemaVersionAlias alias, const char *coverage, const jazz_model_BranchKey *branch_witness_key, bool has_deletion_marker, bool deletion_marker, jz_bytes *out) {
    ValueBuf values;
    JZ_TRY(jazz_node__node__query_eval__read_sources__inline_current_values_with_source_metadata_and_deletion(table, descriptor, row, alias, coverage, branch_witness_key, has_deletion_marker, deletion_marker, &values));
    jz_err e = jzn_groove(groove__records__RecordDescriptor__create(descriptor, values.ptr, values.len, out));
    valuebuf_drop(&values);
    return e;
}

static jz_err jazz_node__node__query_eval__read_sources__inline_current_graph_with_source_metadata_and_branch_witness(const jazz_model_TableSchema *table, jazz_node_CurrentRowVec rows, SchemaVersionAlias alias, const char *coverage,
    const jazz_engine_SourceRequirements *requirements, const char *witness_field, const jazz_model_BranchKey *witness_key,
    groove_GraphBuilder **out_graph, groove_RecordDescriptor *out_desc, jz_btree *out_meta) {
    jz_btree metadata = jazz_node__node__query_eval__read_sources__inline_source_metadata(requirements, witness_field);
    groove_RecordDescriptor descriptor = jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields_for_branch(table, &metadata, witness_field != NULL);
    jz_vec_bytes records = {0};
    for (u32 i = 0; i < rows.len; i++) {
        jz_bytes r;
        jz_err e = jazz_node__node__query_eval__read_sources__inline_current_record_with_source_metadata_and_deletion(table, descriptor, &rows.ptr[i], alias, coverage, witness_field ? witness_key : NULL, false, false, &r);
        if (JZ_IS_ERR(e)) { for (u32 j = 0; j < records.len; j++) jz_bytes_drop(&records.ptr[j]); jz_free(records.ptr); jazz_node_CurrentRowVec_drop(&rows); jz_btree_drop(&metadata); return e; }
        jz_vec_push(&records, &r, sizeof r);
    }
    jazz_node_CurrentRowVec_drop(&rows);
    *out_graph = groove__ivm__graph__GraphBuilder__inline_records(descriptor, records);
    *out_desc = descriptor; *out_meta = metadata;
    return JZ_OK;
}

jz_btree jazz_node__node__query_eval__read_sources__inline_source_metadata(const jazz_engine_SourceRequirements *requirements, const char *branch_witness_field) {
    jz_btree metadata = empty_metadata();
    /* Provenance is carried by the same content-version witness as ordinary
     * table sources: a provenance-only requirement still needs the hidden
     * version fields. */
    bool needs_version_witnesses = requirements_contains(requirements, JAZZ_SMR_VERSION_WITNESSES);
    jazz_engine_SourceMetadataRequirement *req; void *rv;
    if (!needs_version_witnesses) { JZ_BTREE_FOR(&requirements->metadata, req, rv) { if (req->tag == JAZZ_SMR_PROVENANCE) { needs_version_witnesses = true; break; } } }
    if (needs_version_witnesses) {
        jazz_engine_SourceMetadataFields vw; memset(&vw, 0, sizeof vw);
        vw.tag = JAZZ_SMF_VERSION_WITNESSES;
        vw.f0 = jz_str_from("schema_version"); vw.f1 = jz_str_from("tx_time"); vw.f2 = jz_str_from("tx_node_id");
        if (branch_witness_field) { vw.has_opt0 = true; vw.opt0 = jz_str_from(branch_witness_field); }
        metadata_insert(&metadata, smr(JAZZ_SMR_VERSION_WITNESSES), vw);
    }
    if (requirements_contains(requirements, JAZZ_SMR_COVERAGE)) metadata_insert(&metadata, smr(JAZZ_SMR_COVERAGE), smf1(JAZZ_SMF_COVERAGE, "coverage"));
    if (requirements_contains(requirements, JAZZ_SMR_SETTLE_POSITION)) metadata_insert(&metadata, smr(JAZZ_SMR_SETTLE_POSITION), smf1(JAZZ_SMF_SETTLE_POSITION, "settle_position"));
    JZ_BTREE_FOR(&requirements->metadata, req, rv) {
        if (req->tag == JAZZ_SMR_AUTHOR_PATH) {
            jazz_engine_SourceMetadataRequirement k; jazz_engine_SourceMetadataRequirement_clone(&k, req);
            metadata_insert(&metadata, k, smf_provenance(jz_str_clone(&req->author_path)));
        }
        if (req->tag == JAZZ_SMR_PROVENANCE) {
            jazz_engine_SourceMetadataRequirement k = smr(JAZZ_SMR_PROVENANCE); k.provenance = req->provenance;
            metadata_insert(&metadata, k, smf_provenance(jz_str_from(jazz_node__node__query_eval__read_sources__source_provenance_field(req->provenance))));
        }
    }
    return metadata;
}

/* Preserve the supplying physical branch in covered version witnesses. */
jz_btree jazz_node__node__query_eval__read_sources__covered_input_source_metadata(const jazz_engine_SourceRequirements *req, const jazz_model_TableSchema *table) {
    return jazz_node__node__query_eval__read_sources__inline_source_metadata(req, table->branch_by.len != 0 ? "supplying_branch_key" : NULL);
}

/* Encode one already-authorized current row for a receiver-owned covered
 * input. */
jz_err jazz_node__node__query_eval__read_sources__covered_input_record(const jazz_model_TableSchema *table, groove_RecordDescriptor descriptor, const jazz_node_CurrentRow *row, u32 schema_version_alias, const jazz_model_BranchKey *source_branch, jz_bytes *out) {
    u32 idx;
    bool has = groove__records__RecordDescriptor__field_index(descriptor, CS("supplying_branch_key"), &idx);
    return jazz_node__node__query_eval__read_sources__inline_current_record_with_source_metadata_and_deletion(table, descriptor, row, schema_version_alias, "authority-covered-input", has ? source_branch : NULL, false, false, out);
}

static jz_err jazz_node__node__query_eval__read_sources__inline_current_values_with_source_metadata_and_deletion(const jazz_model_TableSchema *table, groove_RecordDescriptor descriptor, const jazz_node_CurrentRow *row,
    SchemaVersionAlias alias, const char *coverage, const jazz_model_BranchKey *branch_witness_key, bool has_deletion_marker, bool deletion_marker, ValueBuf *out) {
    ValueBuf values = {0};
    const groove_DescriptorFieldVec *dfields = groove__records__RecordDescriptor__fields(descriptor);
    vpush(&values, groove_Value_uuid(row->row_uuid.u));
    jz_err e = JZ_OK;
    for (u32 ci = 0; ci < table->columns.len; ci++) {
        const jazz_model_ColumnSchema *column = &table->columns.ptr[ci];
        groove_Value cell; bool some = jazz_node__node__CurrentRow__cell(row, table, SL(column->name), &cell);
        if (branch_witness_key && strvec_contains(&table->branch_by, &column->name) && dfields->ptr[ci + 1].value_type.tag != GROOVE_VT_NULLABLE) {
            if (!some) { e = JZN_INVALID_STORED("frozen branch row is missing a branch column value"); goto fail; }
            vpush(&values, cell);
        } else {
            vpush(&values, boxed_nullable(some, cell));
        }
    }
    jazz_node_RowProvenance p;
    if (JZ_IS_ERR(e = row_provenance(row, &p)) || JZ_IS_ERR(e = push_provenance_values(&values, &p))) goto fail;
    push_tx_alias(&values, row);
    if (JZ_IS_ERR(e = jazz_node__node__query_eval__read_sources__append_author_projection_values(&values, descriptor, p.created_by, p.updated_by))) goto fail;
    u32 idx;
    if (groove__records__RecordDescriptor__field_index(descriptor, CS("table"), &idx)) {
        vpush(&values, groove_Value_string(jz_str_clone(&table->name)));
        vpush(&values, groove_Value_string(jz_str_from("content")));
        vpush(&values, groove_Value_u64(alias));
        vpush(&values, groove_Value_null());
        if (JZ_IS_ERR(e = push_provenance_values(&values, &p))) goto fail;
    }
    if (branch_witness_key) vpush(&values, groove_Value_bytes(jz_bytes_clone(&branch_witness_key->canonical)));
    if (groove__records__RecordDescriptor__field_index(descriptor, CS("settle_position"), &idx)) vpush(&values, groove_Value_null());
    if (groove__records__RecordDescriptor__field_index(descriptor, CS("coverage"), &idx)) vpush(&values, groove_Value_string(jz_str_from(coverage)));
    if (has_deletion_marker) vpush(&values, groove_Value_bool(deletion_marker));
    *out = values;
    return JZ_OK;
fail:
    valuebuf_drop(&values);
    return e;
}

/* Encode one current row as a record of `output`, the exact inferred output
 * of the committed source arm it will be unioned with. Fields are filled by
 * name, adjusting only nullable wrapping. */
static jz_err jazz_node__node__query_eval__read_sources__inline_current_record_for_output(const jazz_model_TableSchema *table, groove_RecordDescriptor output, const jazz_node_CurrentRow *row, SchemaVersionAlias alias,
    const char *coverage, const jazz_engine_SourceRequirements *requirements, jz_bytes *out) {
    /* Supply every field of the canonical current-source vocabulary, not only
     * the declared requirements. */
    jazz_engine_SourceRequirements complete; jazz_engine_SourceRequirements_clone(&complete, requirements);
    static const u32 extra[] = { JAZZ_SMR_VERSION_WITNESSES, JAZZ_SMR_SETTLE_POSITION, JAZZ_SMR_COVERAGE };
    for (u32 i = 0; i < 3; i++) { jazz_engine_SourceMetadataRequirement k = smr(extra[i]); jz_btree_insert(&complete.metadata, &k, NULL, NULL); }
    jz_btree metadata = jazz_node__node__query_eval__read_sources__inline_source_metadata(&complete, NULL);
    jz_btree_drop(&complete.metadata); jz_btree_drop(&complete.app_fields.fields);
    groove_RecordDescriptor inline_d = jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields_for_branch(table, &metadata, false);
    jz_btree_drop(&metadata);
    ValueBuf values;
    JZ_TRY(jazz_node__node__query_eval__read_sources__inline_current_values_with_source_metadata_and_deletion(table, inline_d, row, alias, coverage, NULL, false, false, &values));
    const groove_DescriptorFieldVec *ofields = groove__records__RecordDescriptor__fields(output);
    const groove_DescriptorFieldVec *ifields = groove__records__RecordDescriptor__fields(inline_d);
    ValueBuf fitted = {0};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < ofields->len; i++) {
        const groove_DescriptorField *field = &ofields->ptr[i];
        i64 index = -1;
        if (field->has_name) for (u32 j = 0; j < ifields->len; j++) if (ifields->ptr[j].has_name && jz_str_eq(&ifields->ptr[j].name, &field->name)) { index = j; break; }
        if (index < 0 || (u32)index >= values.len) { e = JZN_INVALID_STORED("overlay row cannot supply a source field"); break; }
        groove_Value value; groove_Value_clone(&value, &values.ptr[index]);
        if (field->value_type.tag == GROOVE_VT_NULLABLE) {
            if (value.tag != GROOVE_V_NULLABLE) value = boxed_nullable(true, value);
        } else if (value.tag == GROOVE_V_NULLABLE && value.v.nullable) {
            groove_Value inner = *value.v.nullable; jz_free(value.v.nullable); value = inner;
        }
        vpush(&fitted, value);
    }
    if (!JZ_IS_ERR(e)) e = jzn_groove(groove__records__RecordDescriptor__create(output, fitted.ptr, fitted.len, out));
    valuebuf_drop(&fitted); valuebuf_drop(&values);
    return e;
}

static jz_err jazz_node__node__query_eval__read_sources__inline_snapshot_include_deleted_current_graph_with_source_metadata(const jazz_model_TableSchema *table, RowDeletedVec rows, SchemaVersionAlias alias,
    const char *coverage, const jazz_engine_SourceRequirements *requirements, groove_GraphBuilder **out_graph, groove_RecordDescriptor *out_desc, jz_btree *out_meta) {
    jz_btree metadata = jazz_node__node__query_eval__read_sources__inline_source_metadata(requirements, NULL);
    groove_RecordDescriptor descriptor = jazz_node__node__query_eval__read_sources__current_row_descriptor_with_hidden_source_fields_for_branch_and_deletion(table, &metadata, false, true);
    jz_vec_bytes records = {0};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < rows.len && !JZ_IS_ERR(e); i++) {
        jz_bytes r;
        e = jazz_node__node__query_eval__read_sources__inline_current_record_with_source_metadata_and_deletion(table, descriptor, &rows.ptr[i].row, alias, coverage, NULL, true, rows.ptr[i].deleted, &r);
        if (!JZ_IS_ERR(e)) jz_vec_push(&records, &r, sizeof r);
    }
    for (u32 i = 0; i < rows.len; i++) jazz_node_CurrentRow_drop(&rows.ptr[i].row);
    jz_free(rows.ptr);
    if (JZ_IS_ERR(e)) { for (u32 j = 0; j < records.len; j++) jz_bytes_drop(&records.ptr[j]); jz_free(records.ptr); jz_btree_drop(&metadata); return e; }
    *out_graph = groove__ivm__graph__GraphBuilder__inline_records(descriptor, records);
    *out_desc = descriptor; *out_meta = metadata;
    return JZ_OK;
}

static groove_RecordDescriptor jazz_node__node__query_eval__read_sources__include_deleted_current_row_descriptor(const jazz_model_TableSchema *table) {
    groove_DescriptorFieldVec fields = {0};
    push_df(&fields, "row_uuid", groove_ValueType_simple(GROOVE_VT_UUID));
    for (u32 i = 0; i < table->columns.len; i++) {
        groove_ValueType t; groove_ValueType_clone(&t, &table->columns.ptr[i].column_type);
        groove_DescriptorField f = groove__records__DescriptorField__new(jazz_engine__query_engine__fields__user_column_field(SL(table->columns.ptr[i].name)), groove_ValueType_nullable(t));
        jz_vec_push(&fields, &f, sizeof f);
    }
    push_df(&fields, "$createdBy", jazz_types__ids__RowAuthor__value_type());
    push_df(&fields, "$createdAt", groove_ValueType_simple(GROOVE_VT_U64));
    push_df(&fields, "$updatedBy", jazz_types__ids__RowAuthor__value_type());
    push_df(&fields, "$updatedAt", groove_ValueType_simple(GROOVE_VT_U64));
    push_df(&fields, "tx_time", groove_ValueType_simple(GROOVE_VT_U64));
    push_df(&fields, "tx_node_id", groove_ValueType_simple(GROOVE_VT_U64));
    push_df(&fields, "__jazz_deleted", groove_ValueType_simple(GROOVE_VT_BOOL));
    return groove__records__RecordDescriptor__new(fields);
}

static groove_PredicateExpr *deletion_eq_tag0(void) {
    groove_Value v; v.tag = GROOVE_V_ENUM_TAG; v.v.u = 0;
    return groove__ivm__op_types__PredicateExpr__eq(CS("_deletion"), groove__ivm__op_types__LiteralValue__from_value(v));
}
static groove_ProjectField *deleted_flag_field(bool deleted) {
    return groove__ivm__graph__ProjectField__literal(CS("__jazz_deleted"), groove__ivm__op_types__LiteralValue__from_value(groove_Value_bool(deleted)));
}
static groove_ProjectFieldVec current_row_named_fields_with_flag(const jazz_model_TableSchema *table, bool deleted) {
    jz_vec_str names = jazz_node__node__query_eval__read_sources__current_row_fields(table);
    groove_ProjectFieldVec out = {0};
    for (u32 i = 0; i < names.len; i++) push_pf(&out, groove__ivm__graph__ProjectField__named(SL(names.ptr[i])));
    strvec_drop(&names);
    push_pf(&out, deleted_flag_field(deleted));
    return out;
}

static jz_err jazz_node__node__query_eval__read_sources__include_deleted_branch_graph(const jazz_model_TableSchema *table, const jazz_model_BranchKey *head, groove_GraphBuilder *content, groove_GraphBuilder *deletions, groove_GraphBuilder **out) {
    groove_ProjectFieldVec storage;
    jz_err e = jazz_node__node__query_eval__read_sources__branch_view_storage_source_fields(table, head, &storage);
    if (JZ_IS_ERR(e)) { groove_GraphBuilder_free(content); groove_GraphBuilder_free(deletions); return e; }
    content = groove__ivm__graph__GraphBuilder__project_fields(groove__ivm__graph__GraphBuilder__project_fields(content, storage),
        jazz_node__node__query_eval__read_sources__storage_to_canonical_current_source_fields(table, false, false));
    groove_ProjectFieldVec dw = {0};
    push_pf(&dw, groove__ivm__graph__ProjectField__named(CS("row_uuid")));
    push_pf(&dw, groove__ivm__graph__ProjectField__named(CS("tx_time")));
    push_pf(&dw, groove__ivm__graph__ProjectField__named(CS("tx_node_id")));
    push_pf(&dw, groove__ivm__graph__ProjectField__renamed(CS("updated_by"), CS("$updatedBy")));
    push_pf(&dw, groove__ivm__graph__ProjectField__renamed(CS("updated_at"), CS("$updatedAt")));
    groove_GraphBuilder *deleted_winners = groove__ivm__graph__GraphBuilder__project_fields(groove__ivm__graph__GraphBuilder__filter(deletions, deletion_eq_tag0()), dw);
    groove_GraphBuilder *undeleted = groove__ivm__graph__GraphBuilder__project_fields(
        anti_join_row_uuid(groove_GraphBuilder_clone(content), groove_GraphBuilder_clone(deleted_winners)), current_row_named_fields_with_flag(table, false));
    jz_vec_str names = jazz_node__node__query_eval__read_sources__current_row_fields(table);
    groove_ProjectFieldVec df = {0};
    for (u32 i = 0; i < names.len; i++) {
        const jz_str *field = &names.ptr[i];
        bool right = jz_str_eq_cstr(field, "$updatedBy") || jz_str_eq_cstr(field, "$updatedAt") || jz_str_eq_cstr(field, "tx_time") || jz_str_eq_cstr(field, "tx_node_id");
        jz_str source = right ? jazz_engine__query_engine__fields__right_field(SL(*field)) : jazz_engine__query_engine__fields__left_field(SL(*field));
        push_pf(&df, groove__ivm__graph__ProjectField__renamed(SL(source), SL(*field)));
        jz_str_drop(&source);
    }
    strvec_drop(&names);
    push_pf(&df, deleted_flag_field(true));
    groove_GraphBuilder *deleted = groove__ivm__graph__GraphBuilder__project_fields(
        groove__ivm__graph__GraphBuilder__join(content, deleted_winners, ROW_UUID_ARR, 1, ROW_UUID_ARR, 1), df);
    *out = union2(undeleted, deleted);
    return JZ_OK;
}

static groove_GraphBuilder *normalize_include_deleted_content_fields(groove_GraphBuilder *graph, const jz_vec_str *user_fields) {
    groove_ProjectFieldVec f = {0};
    push_pf(&f, groove__ivm__graph__ProjectField__named(CS("row_uuid")));
    for (u32 i = 0; i < user_fields->len; i++) push_pf(&f, groove__ivm__graph__ProjectField__named(SL(user_fields->ptr[i])));
    push_pf(&f, groove__ivm__graph__ProjectField__renamed(CS("created_by"), CS("$createdBy")));
    push_pf(&f, groove__ivm__graph__ProjectField__renamed(CS("created_at"), CS("$createdAt")));
    push_pf(&f, groove__ivm__graph__ProjectField__renamed(CS("updated_by"), CS("$updatedBy")));
    push_pf(&f, groove__ivm__graph__ProjectField__renamed(CS("updated_at"), CS("$updatedAt")));
    push_pf(&f, groove__ivm__graph__ProjectField__named(CS("tx_time")));
    push_pf(&f, groove__ivm__graph__ProjectField__named(CS("tx_node_id")));
    push_pf(&f, groove__ivm__graph__ProjectField__named(CS("_deletion")));
    return groove__ivm__graph__GraphBuilder__project_fields(graph, f);
}

static groove_GraphBuilder *jazz_node__node__query_eval__read_sources__include_deleted_current_graph(const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier) {
    jz_vec_str user_fields = {0};
    for (u32 i = 0; i < table->columns.len; i++) { jz_str f = jazz_engine__query_engine__fields__user_column_field(SL(table->columns.ptr[i].name)); jz_vec_push(&user_fields, &f, sizeof f); }
    jz_vec_str content_storage_fields = {0};
    strvec_push_cstr(&content_storage_fields, "row_uuid");
    strvec_push_cstr(&content_storage_fields, "schema_version");
    strvec_push_cstr(&content_storage_fields, "authored_columns");
    for (u32 i = 0; i < user_fields.len; i++) { jz_str c = jz_str_clone(&user_fields.ptr[i]); jz_vec_push(&content_storage_fields, &c, sizeof c); }
    static const char *const tail[] = { "created_by", "created_at", "updated_by", "updated_at", "tx_time", "tx_node_id", "_deletion" };
    for (u32 i = 0; i < 7; i++) strvec_push_cstr(&content_storage_fields, tail[i]);
    jz_str gname = jazz_node__node__codec__global_current_table_name(SL(table->name));
    groove_GraphBuilder *global = normalize_include_deleted_content_fields(
        groove__ivm__graph__GraphBuilder__project(groove__ivm__graph__GraphBuilder__table(SL(gname)), content_storage_fields.ptr, content_storage_fields.len), &user_fields);
    jz_str_drop(&gname);
    /* The winning image carries its own deletion state and provenance: a
     * deleted row reports the deleting write as its last update. */
    groove_GraphBuilder *current;
    if (tier == JAZZ_TIER_GLOBAL) current = global;
    else {
        jz_str aname = jazz_node__node__codec__ahead_current_table_name(SL(table->name));
        groove_GraphBuilder *ahead = normalize_include_deleted_content_fields(
            groove__ivm__graph__GraphBuilder__project(groove__ivm__graph__GraphBuilder__table(SL(aname)), content_storage_fields.ptr, content_storage_fields.len), &user_fields);
        jz_str_drop(&aname);
        current = arg_max_by_tx(union2(global, ahead));
    }
    strvec_drop(&content_storage_fields); strvec_drop(&user_fields);
    return jazz_node__node__query_eval__read_sources__include_deleted_from_images(table, current);
}

/* Split winning row images into the IncludeDeleted carrier: every current
 * row field plus __jazz_deleted derived from the image's _deletion cell. */
static groove_GraphBuilder *jazz_node__node__query_eval__read_sources__include_deleted_from_images(const jazz_model_TableSchema *table, groove_GraphBuilder *current) {
    groove_GraphBuilder *live = groove__ivm__graph__GraphBuilder__project_fields(
        groove__ivm__graph__GraphBuilder__filter(groove_GraphBuilder_clone(current), jazz_node__node__codec__not_deleted_predicate()), current_row_named_fields_with_flag(table, false));
    groove_GraphBuilder *deleted = groove__ivm__graph__GraphBuilder__project_fields(
        groove__ivm__graph__GraphBuilder__filter(current, deletion_eq_tag0()), current_row_named_fields_with_flag(table, true));
    return union2(live, deleted);
}

jz_vec_str jazz_node__node__query_eval__read_sources__maintained_view_history_storage_field_names(const jazz_model_TableSchema *table) {
    static const char *const head[] = { "row_uuid", "tx_time", "tx_node_id", "schema_version", "created_by", "created_at", "updated_by", "updated_at" };
    jz_vec_str fields = {0};
    for (u32 i = 0; i < 8; i++) strvec_push_cstr(&fields, head[i]);
    for (u32 i = 0; i < table->columns.len; i++) { jz_str f = jazz_engine__query_engine__fields__user_column_field(SL(table->columns.ptr[i].name)); jz_vec_push(&fields, &f, sizeof f); }
    strvec_push_cstr(&fields, "authored_columns");
    return fields;
}

/* A first-result equality conjunction can use both columns of an explicitly
 * declared two-column composite index as one prefix; other indexed
 * equalities stay as intersections. An `id` equality keeps the primary key. */
static bool jazz_node__node__query_eval__read_sources__select_composite_equality_access_path(const jazz_model_TableSchema *table, const jz_btree *equalities, jazz_node_CurrentAccessPath *out) {
    jz_str id_key = jz_str_from("id");
    bool has_id = jz_btree_contains(equalities, &id_key);
    jz_str_drop(&id_key);
    if (has_id) return false;
    jz_vec_str *columns; void *cv;
    JZ_BTREE_FOR(&table->composite_indexes, columns, cv) {
        if (columns->len != 2) continue;
        const jz_str *first = &columns->ptr[0], *second = &columns->ptr[1];
        const groove_Value *fv = jz_btree_get(equalities, first);
        if (!fv) continue;
        const groove_Value *sv = jz_btree_get(equalities, second);
        if (!sv) continue;
        jazz_node_CurrentAccessPath p; memset(&p, 0, sizeof p);
        p.tag = JAZZ_CAP_INDEX;
        jz_vec_str indexed = jazz_model__schema__TableSchema__global_current_indexed_columns(table);
        for (u32 i = 0; i < indexed.len; i++) {
            const jz_str *column = &indexed.ptr[i];
            if (jz_str_eq(column, first) || jz_str_eq(column, second)) continue;
            const groove_Value *v = jz_btree_get(equalities, column);
            if (!v) continue;
            jazz_node_IndexIntersection is; memset(&is, 0, sizeof is);
            is.column = jz_str_clone(column);
            groove_Value vc; groove_Value_clone_v(&vc, v);
            JZ_VEC_PUSH(is.values, jazz_node__node__query_eval__normalization__physical_current_index_value(table, SL(*column), vc));
            jz_vec_push(&p.intersections, &is, sizeof is);
        }
        strvec_drop(&indexed);
        p.column = jz_str_clone(first);
        p.has_order_column = true; p.order_column = jz_str_clone(second);
        p.reverse = false;
        groove_Value a, b; groove_Value_clone_v(&a, fv); groove_Value_clone_v(&b, sv);
        JZ_VEC_PUSH(p.prefix, jazz_node__node__query_eval__normalization__physical_current_index_value(table, SL(*first), a));
        JZ_VEC_PUSH(p.prefix, jazz_node__node__query_eval__normalization__physical_current_index_value(table, SL(*second), b));
        p.maintained = false; p.candidate_filter = NULL; p.has_source_limit = false;
        *out = p;
        return true;
    }
    return false;
}

/* Attach a covered-key candidate filter to an admitted junction index path:
 * only when the junction's index entries carry on_column and the root path
 * names a plain, uncapped snapshot prefix. */
static void jazz_node__node__query_eval__read_sources__attach_covered_join_key_filter(const jazz_model_TableSchema *join_table, jz_slice on_column, jz_slice root_table,
    const jazz_node_CurrentAccessPath *root_path, jazz_node_CurrentAccessPath *join_path) {
    if (!root_path || root_path->tag != JAZZ_CAP_INDEX || root_path->maintained || root_path->has_source_limit) return;
    if (join_path->tag != JAZZ_CAP_INDEX || join_path->reverse || join_path->maintained || join_path->has_source_limit) return;
    if (join_path->prefix.len != 1 || join_path->intersections.len != 0) return;
    if (join_path->has_order_column && jz_slice_cmp(SL(join_path->order_column), on_column) != 0) return;
    bool declared = false;
    jz_vec_str *columns; void *cv;
    JZ_BTREE_FOR(&join_table->composite_indexes, columns, cv) {
        if (columns->len == 2 && jz_str_eq(&columns->ptr[0], &join_path->column) && jz_slice_cmp(SL(columns->ptr[1]), on_column) == 0) { declared = true; break; }
    }
    if (!declared) return;
    if (join_path->has_order_column) jz_str_drop(&join_path->order_column);
    join_path->has_order_column = true; join_path->order_column = jz_str_from_slice(on_column);
    if (join_path->candidate_filter) { jazz_node_CurrentIndexCandidateFilter_drop(join_path->candidate_filter); jz_free(join_path->candidate_filter); }
    jazz_node_CurrentIndexCandidateFilter *f = jz_alloc(sizeof *f); memset(f, 0, sizeof *f);
    f->table = jz_str_from_slice(root_table);
    f->column = jz_str_clone(&root_path->column);
    f->has_order_column = root_path->has_order_column; if (root_path->has_order_column) f->order_column = jz_str_clone(&root_path->order_column);
    valuevec_clone(&f->prefix, &root_path->prefix);
    f->source_column = jz_str_from_slice(on_column);
    join_path->candidate_filter = f;
}

static const jazz_engine_SourceId *jazz_node__node__query_eval__read_sources__single_root_join_source(const jazz_engine_QueryProgramRequest *request, jz_slice join_table) {
    const jazz_engine_JoinContribution *found = NULL;
    for (u32 i = 0; i < request->input.shape.join_contributions.len; i++) {
        const jazz_engine_JoinContribution *c = &request->input.shape.join_contributions.ptr[i];
        if (c->has_parent) continue;
        if (found) return NULL;
        found = c;
    }
    if (!found || jz_slice_cmp(SL(found->source.table), join_table) != 0) return NULL;
    return &found->source;
}

void jazz_node_JazzSourceGraphPreparer_drop(jazz_node_JazzSourceGraphPreparer *p) {
    if (p->has_local_unavailable_scope) jazz_protocol_PolicyBindingKey_drop(&p->local_unavailable_scope);
    jz_btree_drop(&p->inline_sources);
    jz_btree_drop(&p->covered_input_sources);
    jz_btree_drop(&p->covered_input_descriptors);
    jz_btree_drop(&p->access_paths);
    if (p->has_bounded_deletion_register) { jazz_engine_SourceId_drop(&p->bounded_source); groove_GraphBuilder_free(p->bounded_register); }
    jz_btree_drop(&p->current_projection_targets);
    jazz_node_TransactionWriteOverlay_drop(&p->transaction_overlay);
}
