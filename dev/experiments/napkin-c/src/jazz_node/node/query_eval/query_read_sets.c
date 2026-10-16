/* Port of crates/jazz/layers/node/src/node/query_eval/query_read_sets.rs
 * Read-view-specific source selection for query evaluation. */
#include "jazz_node/node/node_qe_b.h"

static jazz_engine_SchemaProjection jazz_node__node__query_eval__query_read_sets__canonical_projection(jazz_types_SchemaVersionId v)
{
    jazz_engine_SchemaProjection p;
    memset(&p, 0, sizeof p);
    p.family_tag = JZ_SFS_CURRENT;
    p.storage_tag = JZ_SSS_SINGLE;
    p.single = v;
    p.lens_tag = JZ_LENS_CANONICAL;
    return p;
}

/* Closure `expr` of the Rust source: builds one SourceExpr per source. */
enum { EXPR_CURRENT, EXPR_HISTORY, EXPR_SNAPSHOT, EXPR_TX };
typedef struct ExprCtx {
    u32 kind;
    const jazz_engine_SchemaProjection *projection;
    const jazz_protocol_BindingViewKey *settled_binding_view;
    const jazz_protocol_AuthorityResultKey *settled_authority_result_key;
    bool settled_current_default;
    jazz_model_DurabilityTier tier;
    jazz_types_GlobalTime position;
    const jazz_model_Snapshot *snapshot;
    jazz_model_OpenTransactionId tx_id;
} ExprCtx;

static jazz_engine_SourceExpr make_expr(const ExprCtx *c)
{
    jazz_engine_SourceExpr e;
    memset(&e, 0, sizeof e);
    switch (c->kind) {
    case EXPR_CURRENT:
        if (c->settled_binding_view) {
            e.tag = JZ_SE_SETTLED_BINDING_VIEW;
            e.current_default = c->settled_current_default;
            jazz_engine_SchemaProjection_clone(&e.projection, c->projection);
            e.binding_view = *c->settled_binding_view;
            if (c->settled_authority_result_key) {
                e.has_authority_result_key = true;
                jazz_protocol_AuthorityResultKey_clone(&e.authority_result_key, c->settled_authority_result_key);
            }
        } else {
            e.tag = JZ_SE_VISIBLE_CURRENT;
            jazz_engine_SchemaProjection_clone(&e.projection, c->projection);
            e.data.tag = JZ_DS_CURRENT;
            e.tier = c->tier;
        }
        break;
    case EXPR_HISTORY:
        e.tag = JZ_SE_HISTORY_CUT;
        jazz_engine_SchemaProjection_clone(&e.projection, c->projection);
        e.data.tag = JZ_DS_CURRENT;
        e.position = c->position;
        break;
    case EXPR_SNAPSHOT:
        e.tag = JZ_SE_SNAPSHOT_REF;
        jazz_engine_SchemaProjection_clone(&e.projection, c->projection);
        e.data.tag = JZ_DS_CURRENT;
        jazz_model_Snapshot_clone(&e.snapshot, c->snapshot);
        break;
    case EXPR_TX: {
        jazz_engine_SourceExpr *input = JZ_NEW(jazz_engine_SourceExpr);
        input->tag = JZ_SE_SNAPSHOT_REF;
        jazz_engine_SchemaProjection_clone(&input->projection, c->projection);
        input->data.tag = JZ_DS_CURRENT;
        jazz_model_Snapshot_clone(&input->snapshot, c->snapshot);
        e.tag = JZ_SE_WITH_OVERLAYS;
        e.input = input;
        jazz_engine_OverlayRef ov;
        memset(&ov, 0, sizeof ov);
        ov.tag = JZ_OVL_OPEN_TX;
        ov.open = c->tx_id;
        jz_vec_push(&e.overlays.entries, &ov, sizeof ov);
        break;
    }
    }
    return e;
}

static void insert_source(jz_btree *sources, const jazz_engine_SourceId *src, const ExprCtx *c)
{
    jazz_engine_SourceId id;
    jazz_engine_SourceId_clone(&id, src);
    jazz_engine_SourceExpr e = make_expr(c);
    jazz_engine_SourceExpr old;
    if (jz_btree_insert(sources, &id, &e, &old)) { jazz_engine_SourceId_drop(&id); jazz_engine_SourceExpr_drop(&old); }
}

/* uniform_read_set(read_schema, policy_schema, sources, expr): node sources
 * (RowSetExpr::Source) optionally followed by the auxiliary sources. */
static jazz_engine_RequestedReadSet jazz_node__node__query_eval__query_read_sets__uniform_read_set(
    jazz_types_SchemaVersionId read_schema, jazz_types_SchemaVersionId policy_schema,
    const jazz_engine_NormalizedRowSetShape *shape, bool include_auxiliary, const ExprCtx *c)
{
    jazz_engine_ReadView view;
    view.read_schema = read_schema;
    view.policy_schema = policy_schema;
    jz_btree_init(&view.sources, &jazz_engine_SOURCE_GRAPH_DESC);
    void *k; jazz_engine_RowSetExpr *node;
    JZ_BTREE_FOR(&shape->nodes, k, node) {
        if (node->tag == JZ_RSE_SOURCE) insert_source(&view.sources, &node->source, c);
    }
    if (include_auxiliary) {
        jazz_engine_SourceId *src; void *unused;
        JZ_BTREE_FOR(&shape->auxiliary_sources, src, unused) insert_source(&view.sources, src, c);
    }
    return jazz_engine__query_engine__read__QueryReadSet__primary(view);
}

jazz_engine_RequestedReadSet jazz_node__node__query_eval__query_read_sets__current_query_read_set(
    const jazz_engine_NormalizedRowSetShape *shape, jazz_types_SchemaVersionId read_schema,
    jazz_types_SchemaVersionId policy_schema, jazz_model_DurabilityTier tier,
    const jazz_protocol_BindingViewKey *settled_binding_view,
    const jazz_protocol_AuthorityResultKey *settled_authority_result_key, bool settled_current_default)
{
    jazz_engine_SchemaProjection projection = jazz_node__node__query_eval__query_read_sets__canonical_projection(read_schema);
    ExprCtx c = { EXPR_CURRENT, &projection, settled_binding_view, settled_authority_result_key, settled_current_default, tier };
    jazz_engine_RequestedReadSet r = jazz_node__node__query_eval__query_read_sets__uniform_read_set(read_schema, policy_schema, shape, true, &c);
    jazz_engine_SchemaProjection_drop(&projection);
    return r;
}

jazz_engine_RequestedReadSet jazz_node__node__query_eval__query_read_sets__historical_query_read_set(
    const jazz_engine_NormalizedRowSetShape *shape, jazz_types_SchemaVersionId schema_version, jazz_types_GlobalTime position)
{
    jazz_engine_SchemaProjection projection = jazz_node__node__query_eval__query_read_sets__canonical_projection(schema_version);
    ExprCtx c = { EXPR_HISTORY, &projection };
    c.position = position;
    /* Historical reads cover only the shape's node sources. */
    jazz_engine_RequestedReadSet r = jazz_node__node__query_eval__query_read_sets__uniform_read_set(schema_version, schema_version, shape, false, &c);
    jazz_engine_SchemaProjection_drop(&projection);
    return r;
}

jazz_engine_RequestedReadSet jazz_node__node__query_eval__query_read_sets__snapshot_query_read_set(
    const jazz_engine_NormalizedRowSetShape *shape, jazz_types_SchemaVersionId schema_version, const jazz_model_Snapshot *snapshot)
{
    jazz_engine_SchemaProjection projection = jazz_node__node__query_eval__query_read_sets__canonical_projection(schema_version);
    ExprCtx c = { EXPR_SNAPSHOT, &projection };
    c.snapshot = snapshot;
    jazz_engine_RequestedReadSet r = jazz_node__node__query_eval__query_read_sets__uniform_read_set(schema_version, schema_version, shape, true, &c);
    jazz_engine_SchemaProjection_drop(&projection);
    return r;
}

jazz_engine_RequestedReadSet jazz_node__node__query_eval__query_read_sets__tx_query_read_set(
    const jazz_engine_NormalizedRowSetShape *shape, jazz_types_SchemaVersionId schema_version,
    jazz_model_OpenTransactionId tx_id, const jazz_model_Snapshot *snapshot)
{
    jazz_engine_SchemaProjection projection = jazz_node__node__query_eval__query_read_sets__canonical_projection(schema_version);
    ExprCtx c = { EXPR_TX, &projection };
    c.snapshot = snapshot;
    c.tx_id = tx_id;
    jazz_engine_RequestedReadSet r = jazz_node__node__query_eval__query_read_sets__uniform_read_set(schema_version, schema_version, shape, true, &c);
    jazz_engine_SchemaProjection_drop(&projection);
    return r;
}

jz_err jazz_node__node__query_eval__query_read_sets__query_read_set_for_read_view(
    const jazz_engine_NormalizedRowSetShape *shape, jazz_types_SchemaVersionId read_schema,
    jazz_types_SchemaVersionId policy_schema, jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view,
    const jazz_protocol_BindingViewKey *settled_binding_view, const jazz_protocol_AuthorityResultKey *settled_authority_result_key,
    const jazz_model_JazzSchema *schema, jazz_engine_RequestedReadSet *out)
{
    /* Settled rows are already the authority's effective result for the exact
     * BindingViewKey; reapplying branch selection locally would be wrong. */
    if (settled_binding_view) {
        *out = jazz_node__node__query_eval__query_read_sets__current_query_read_set(
            shape, read_schema, policy_schema, tier, settled_binding_view, settled_authority_result_key,
            read_view->source.tag == JZ_RVS_CURRENT);
        return JZ_OK;
    }
    switch (read_view->source.tag) {
    case JZ_RVS_CURRENT:
        *out = jazz_node__node__query_eval__query_read_sets__current_query_read_set(shape, read_schema, policy_schema, tier, NULL, NULL, false);
        return JZ_OK;
    case JZ_RVS_SNAPSHOT: {
        jazz_model_Snapshot snap;
        jazz_model_Snapshot_clone(&snap, &read_view->source.snapshot);
        *out = jazz_node__node__query_eval__query_read_sets__snapshot_query_read_set(shape, read_schema, &snap);
        jazz_model_Snapshot_drop(&snap);
        return JZ_OK;
    }
    case JZ_RVS_BRANCH_VIEW: {
        jazz_engine_SchemaProjection projection = jazz_node__node__query_eval__query_read_sets__canonical_projection(read_schema);
        jz_btree sources;
        jz_btree_init(&sources, &jazz_engine_SOURCE_GRAPH_DESC);
        /* all_sources(): node sources, then auxiliary sources */
        JZ_VEC(const jazz_engine_SourceId *) all = {0};
        void *k; jazz_engine_RowSetExpr *node;
        JZ_BTREE_FOR(&shape->nodes, k, node) if (node->tag == JZ_RSE_SOURCE) { const jazz_engine_SourceId *p = &node->source; jz_vec_push(&all, &p, sizeof p); }
        jazz_engine_SourceId *aux; void *unused;
        JZ_BTREE_FOR(&shape->auxiliary_sources, aux, unused) { const jazz_engine_SourceId *p = aux; jz_vec_push(&all, &p, sizeof p); }
        jz_err err = JZ_OK;
        for (u32 i = 0; i < all.len; i++) {
            const jazz_engine_SourceId *source = all.ptr[i];
            const jazz_model_TableSchema *table = NULL;
            for (u32 t = 0; t < schema->tables.len; t++) {
                if (jz_str_eq(&schema->tables.ptr[t].name, &source->table)) { table = &schema->tables.ptr[t]; break; }
            }
            if (!table) { err = JZN_INVALID_STORED("branch view source table missing"); break; }
            jazz_model_BranchKey head_key;
            void *base;
            jz_err be = jazz_node__node__layer_stack__branch_view_stack(schema, table, read_view->source.head, read_view->source.base, &head_key, &base);
            if (JZ_IS_ERR(be)) {
                /* map_err(Error::InvalidBranchKey) */
                const jz_str *m = be.msg;
                err = jz_err_fmt(JZN_E_InvalidBranchKey, "invalid branch key: %.*s", m ? (int)m->len : 0, m ? m->ptr : "");
                jz_err_drop(&be);
                break;
            }
            jazz_engine_SourceId id;
            jazz_engine_SourceId_clone(&id, source);
            jazz_engine_SourceExpr e;
            memset(&e, 0, sizeof e);
            e.tag = JZ_SE_BRANCH_VIEW;
            jazz_engine_SchemaProjection_clone(&e.projection, &projection);
            e.head = head_key;
            e.base = base;
            e.tier = tier;
            jazz_engine_SourceExpr old;
            if (jz_btree_insert(&sources, &id, &e, &old)) { jazz_engine_SourceId_drop(&id); jazz_engine_SourceExpr_drop(&old); }
        }
        jz_free(all.ptr);
        jazz_engine_SchemaProjection_drop(&projection);
        if (JZ_IS_ERR(err)) { jz_btree_drop(&sources); return err; }
        jazz_engine_ReadView view = { read_schema, policy_schema, sources };
        *out = jazz_engine__query_engine__read__QueryReadSet__primary(view);
        return JZ_OK;
    }
    default: JZ_UNREACHABLE();
    }
}
