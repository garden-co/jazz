/* jazz_node::node::query_eval::normalization
 *
 * Normalize validated Jazz queries and policy clauses into the query engine's
 * logical row-set representation.
 *
 * This stage assigns stable source identities, coerces predicates and
 * parameters, expands joins, arrays, reachability, and inherited policies, and
 * describes requested outputs. It does not choose physical storage sources or
 * build executable Groove graphs.
 *
 * Napkin C port (unit node-qe-a). NodeState<S> has one copy. */
#include "jz_rt.h"
#include "jazz_node/_ext_node-qe-a.h"
#include "jazz_node/node/query_eval/read_sources.h"
#include "jazz_node/node/query_eval/normalization.h"

#define NM(x) jazz_node__node__query_eval__normalization__##x
#define SL(s) jz_str_slice(&(s))
typedef jazz_engine_SourceId SourceId;
typedef jazz_engine_NormalizedValueRef VRef;
typedef jazz_engine_PredicateExpr PExpr;
typedef jazz_engine_RowSetExpr RSE;
typedef jazz_engine_RowSetNodeId NodeId;
typedef jazz_model_RuntimeSchema RuntimeSchema;

jz_btree jazz_node__node__query_eval__predicate_params(const jazz_model_Predicate *predicates, u32 n); /* query_eval.rs */
extern const char JAZZ_PENDING_BINDING_SOURCE_SHAPE[];                                                 /* query_eval.rs */

static void str_drop_v(void *p) { jz_str_drop((jz_str *)p); }
static void str_clone_v(void *d, const void *s) { *(jz_str *)d = jz_str_clone((const jz_str *)s); }
static bool str_is(const jz_str *s, const char *lit) { return jz_str_eq_cstr(s, lit); }
static jz_str S(const char *lit) { return jz_str_from(lit); }

/* ---------- constructors ---------- */
static jazz_engine_SourceRole role(u32 tag, jz_str name) { jazz_engine_SourceRole r; r.tag = tag; r.name = name; return r; }
static SourceId sid1(jz_str table, jazz_engine_SourceRole a) {
    SourceId s; s.table = table; s.path.components.ptr = NULL; s.path.components.len = s.path.components.cap = 0;
    jz_vec_push(&s.path.components, &a, sizeof a);
    return s;
}
static SourceId sid2(jz_str table, jazz_engine_SourceRole a, jazz_engine_SourceRole b) {
    SourceId s = sid1(table, a);
    jz_vec_push(&s.path.components, &b, sizeof b);
    return s;
}
static SourceId sid_clone(const SourceId *s) { SourceId d; jazz_engine_SourceId_clone(&d, s); return d; }
static jz_str empty_str(void) { jz_str s = {0}; return s; }

static VRef vr(u32 tag) { VRef v; __builtin_memset(&v, 0, sizeof v); v.tag = tag; return v; }
static VRef vr_row_id(const SourceId *source) { VRef v = vr(JAZZ_NVR_ROW_ID); v.row_id_kind = JAZZ_ROWID_SOURCE; v.source = sid_clone(source); return v; }
static VRef vr_field(const SourceId *source, jz_str field) { VRef v = vr(JAZZ_NVR_SOURCE_FIELD); v.source = sid_clone(source); v.field = field; return v; }
static VRef vr_param(jz_str name) { VRef v = vr(JAZZ_NVR_PARAM); v.field = name; return v; }
static VRef vr_claim(jazz_engine_ClaimPath path) { VRef v = vr(JAZZ_NVR_CLAIM); v.claim = path; return v; }
static VRef vr_frontier(const jazz_engine_FrontierId *frontier, const char *field) {
    VRef v = vr(JAZZ_NVR_FRONTIER_COLUMN); v.frontier.s = jz_str_clone(&frontier->s); v.field = S(field); return v;
}
static VRef vr_clone(const VRef *s) { VRef d; jazz_engine_NormalizedValueRef_clone(&d, s); return d; }
static jazz_engine_ClaimPath claim_path_of(jz_slice claim) {
    __typeof__(jazz_model__query__operand_claim_path(claim)) segs = jazz_model__query__operand_claim_path(claim);
    jazz_engine_ClaimPath p; p.segments.ptr = segs.ptr; p.segments.len = segs.len; p.segments.cap = segs.cap;
    return p;
}

static PExpr pe(u32 tag) { PExpr p; __builtin_memset(&p, 0, sizeof p); p.tag = tag; return p; }
static PExpr pe_compare(VRef left, jazz_engine_ComparisonOp op, VRef right) { PExpr p = pe(JAZZ_NP_COMPARE); p.left = left; p.op = op; p.right = right; return p; }
static PExpr pe_clone(const PExpr *s) { PExpr d; jazz_engine_PredicateExpr_clone(&d, s); return d; }
static void pevec_drop(jazz_engine_PredicateVec *v) { jz_vec_drop(v, sizeof(PExpr), jazz_engine_PredicateExpr_drop); }
static void vrvec_drop(jazz_engine_ValueRefVec *v) { jz_vec_drop(v, sizeof(VRef), jazz_engine_NormalizedValueRef_drop); }

static RSE rse(u32 tag) { RSE e; __builtin_memset(&e, 0, sizeof e); e.tag = tag; return e; }
static RSE rse_source(const SourceId *source, jazz_engine_RowVisibility visibility) { RSE e = rse(JAZZ_RSE_SOURCE); e.source = sid_clone(source); e.visibility = visibility; return e; }
static RSE rse_filter(NodeId input, PExpr predicate) { RSE e = rse(JAZZ_RSE_FILTER); e.input = input; e.predicate = predicate; return e; }
static RSE rse_join(NodeId left, NodeId right, jazz_engine_JoinMode mode, PExpr on) { RSE e = rse(JAZZ_RSE_JOIN); e.input = left; e.right = right; e.join_mode = mode; e.predicate = on; return e; }

static NodeId nid(jz_str s) { NodeId n; n.s = s; return n; }
static NodeId nid_clone(const NodeId *n) { return nid(jz_str_clone(&n->s)); }
static void node_id_drop(NodeId *n) { jz_str_drop(&n->s); }
/* nodes.insert(id, expression): both moved in */
static void nodes_insert(jz_btree *nodes, NodeId id, RSE e) { jz_btree_insert(nodes, &id, &e, NULL); }

static jazz_engine_TypedOutputField typed_output_field(jz_str name, groove_ColumnType ty) { jazz_engine_TypedOutputField f; f.name = name; f.ty = ty; return f; }
static groove_ColumnType ty_simple(u32 tag) { return groove_ValueType_simple(tag); }
static groove_ColumnType ty_clone(const groove_ColumnType *t) { groove_ColumnType d; groove_ValueType_clone(&d, t); return d; }

typedef JZ_VEC(jazz_engine_RowProjection) ProjVec;
typedef JZ_VEC(jazz_engine_ValueSourceColumn) ColumnVec;
static jazz_engine_RowProjection projection(jz_str name, groove_ColumnType ty, VRef value) {
    jazz_engine_RowProjection p; p.output = typed_output_field(name, ty); p.value = value; return p;
}
static void projvec_push(ProjVec *v, jazz_engine_RowProjection p) { jz_vec_push(v, &p, sizeof p); }
static void value_source_column_drop(void *p) {
    jazz_engine_ValueSourceColumn *c = p; jz_str_drop(&c->name); jazz_engine_NormalizedValueRef_drop(&c->value); groove_ValueType_drop(&c->ty);
}
static void value_source_column_clone(void *d, const void *s) {
    jazz_engine_ValueSourceColumn *dc = d; const jazz_engine_ValueSourceColumn *sc = s;
    dc->name = jz_str_clone(&sc->name); jazz_engine_NormalizedValueRef_clone(&dc->value, &sc->value); groove_ValueType_clone(&dc->ty, &sc->ty);
}
static void columns_drop(ColumnVec *v) { jz_vec_drop(v, sizeof(jazz_engine_ValueSourceColumn), value_source_column_drop); }
static void columns_clone(ColumnVec *d, const ColumnVec *s) { jz_vec_clone(d, s, sizeof(jazz_engine_ValueSourceColumn), value_source_column_clone); }
static void columns_push(ColumnVec *v, jz_str name, VRef value, groove_ColumnType ty) {
    jazz_engine_ValueSourceColumn c; c.name = name; c.value = value; c.ty = ty; jz_vec_push(v, &c, sizeof c);
}

static jz_err query_lowering_msg(const char *msg) { return JZN_QUERY_LOWERING("%s", msg); }
static jz_err query_capability_msg(const char *msg) { return JZN_QUERY_CAPABILITY("%s", msg); }
/* normalization_gap(err.to_string()) */
static jz_err normalization_gap_from(jz_err e) {
    int n = e.msg ? (int)e.msg->len : 0; const char *p = e.msg ? e.msg->ptr : "";
    jz_err out = JZN_QUERY_LOWERING("%.*s", n, p);
    jz_err_drop(&e);
    return out;
}
/* Error::QueryCapability(error.to_string()) */
static jz_err capability_from(jz_err e) {
    int n = e.msg ? (int)e.msg->len : 0; const char *p = e.msg ? e.msg->ptr : "";
    jz_err out = JZN_QUERY_CAPABILITY("%.*s", n, p);
    jz_err_drop(&e);
    return out;
}

/* ---------- source identities ---------- */
SourceId NM(root_source_id)(const char *table, u32 len) {
    return sid1(jz_str_from_slice(jz_slice_of(table, len)), role(JAZZ_SR_ROOT, empty_str()));
}
static SourceId root_source_of(const jz_str *table) { return NM(root_source_id)(table->ptr, table->len); }

static void rewrite_union_inputs(RSE *e, const jz_btree *ids);
static NodeId map_id(const jz_btree *ids, const NodeId *id) {
    const NodeId *mapped = jz_btree_get(ids, id);
    if (!mapped) jz_panic("normal relation arm references a node in its own DAG");
    return nid_clone(mapped);
}
static void remap(const jz_btree *ids, NodeId *slot) { NodeId m = map_id(ids, slot); node_id_drop(slot); *slot = m; }
static void node_id_drop_v(void *p) { node_id_drop(p); }
static int node_id_cmp(const void *a, const void *b, void *ctx) { return jz_cmp_str(a, b, ctx); }
static const jz_btree_desc node_id_map_desc = { sizeof(NodeId), sizeof(NodeId), node_id_cmp, node_id_drop_v, node_id_drop_v };
static void rewrite_union_inputs(RSE *e, const jz_btree *ids) {
    for (u32 i = 0; i < e->inputs.len; i++) remap(ids, &e->inputs.ptr[i].node);
}

/* Move one independently-normalized relation arm into the union program.
 * Source identities intentionally stay unchanged: public unions require the
 * same real-row domain, while node ids must remain disjoint so each arm keeps
 * its own filters, recursive frontier, and policy proof. */
static jz_err jazz_node__node__query_eval__normalization__prefix_normalized_relation_arm(
    jazz_engine_NormalizedRowSetShape *normalized, const jz_str *prefix, jz_btree *destination, jz_btree *auxiliary_sources,
    void *closure_paths, void *join_contributions, void *inherited_contributions, void *reachable_contributions) {
    jz_btree ids; jz_btree_init(&ids, &node_id_map_desc);
    NodeId *k; RSE *v;
    JZ_BTREE_FOR(&normalized->nodes, k, v) {
        NodeId from = nid_clone(k);
        NodeId to = nid(jz_format("%.*s:%.*s", (int)prefix->len, prefix->ptr, (int)k->s.len, k->s.ptr));
        jz_btree_insert(&ids, &from, &to, NULL);
    }
    jz_btree rewritten; jz_btree_init(&rewritten, normalized->nodes.d);
    NodeId id; RSE expression;
    while (jz_btree_pop_first(&normalized->nodes, &id, &expression)) {
        switch (expression.tag) {
        case JAZZ_RSE_FILTER: case JAZZ_RSE_DISTINCT: case JAZZ_RSE_PROJECT: case JAZZ_RSE_ORDER_BY:
        case JAZZ_RSE_SLICE: case JAZZ_RSE_AGGREGATE:
            remap(&ids, &expression.input); break;
        case JAZZ_RSE_JOIN: remap(&ids, &expression.input); remap(&ids, &expression.right); break;
        case JAZZ_RSE_RECURSIVE: remap(&ids, &expression.input); remap(&ids, &expression.step); break;
        case JAZZ_RSE_UNION: rewrite_union_inputs(&expression, &ids); break;
        case JAZZ_RSE_CORRELATED_PATH: remap(&ids, &expression.input); remap(&ids, &expression.right); break;
        default: break; /* Source / ValueSource / FrontierSource */
        }
        NodeId mapped = map_id(&ids, &id);
        node_id_drop(&id);
        jz_btree_insert(&rewritten, &mapped, &expression, NULL);
    }
    NodeId *dk; RSE *dv;
    JZ_BTREE_FOR(destination, dk, dv) {
        if (jz_btree_contains(&rewritten, dk)) {
            jz_btree_drop(&rewritten); jz_btree_drop(&ids);
            return query_capability_msg("relation UNION arm node ids collide after prefixing");
        }
    }
    jz_btree_append(destination, &rewritten);
    jz_btree_drop(&rewritten);
    remap(&ids, &normalized->root);
    jz_btree_append(auxiliary_sources, &normalized->auxiliary_sources);
    for (u32 i = 0; i < normalized->closure_paths.len; i++) {
        jz_str *pid = &normalized->closure_paths.ptr[i].id;
        jz_str next = jz_format("%.*s:%.*s", (int)prefix->len, prefix->ptr, (int)pid->len, pid->ptr);
        jz_str_drop(pid); *pid = next;
    }
    jz_vec_extend(closure_paths, normalized->closure_paths.ptr, normalized->closure_paths.len, sizeof(jazz_engine_ClosurePath));
    normalized->closure_paths.len = 0;
    for (u32 i = 0; i < normalized->join_contributions.len; i++) {
        jazz_engine_JoinContribution *c = &normalized->join_contributions.ptr[i];
        jz_str next = jz_format("%.*s:%.*s", (int)prefix->len, prefix->ptr, (int)c->id.len, c->id.ptr);
        jz_str_drop(&c->id); c->id = next;
        remap(&ids, &c->input);
    }
    jz_vec_extend(join_contributions, normalized->join_contributions.ptr, normalized->join_contributions.len, sizeof(jazz_engine_JoinContribution));
    normalized->join_contributions.len = 0;
    for (u32 i = 0; i < normalized->inherited_contributions.len; i++) {
        jazz_engine_InheritedContribution *c = &normalized->inherited_contributions.ptr[i];
        jz_str next = jz_format("%.*s:%.*s", (int)prefix->len, prefix->ptr, (int)c->id.len, c->id.ptr);
        jz_str_drop(&c->id); c->id = next;
        remap(&ids, &c->input);
    }
    jz_vec_extend(inherited_contributions, normalized->inherited_contributions.ptr, normalized->inherited_contributions.len, sizeof(jazz_engine_InheritedContribution));
    normalized->inherited_contributions.len = 0;
    for (u32 i = 0; i < normalized->reachable_contributions.len; i++) {
        jazz_engine_ReachableContribution *c = &normalized->reachable_contributions.ptr[i];
        jz_str next = jz_format("%.*s:%.*s", (int)prefix->len, prefix->ptr, (int)c->id.len, c->id.ptr);
        jz_str_drop(&c->id); c->id = next;
        remap(&ids, &c->access_input);
    }
    jz_vec_extend(reachable_contributions, normalized->reachable_contributions.ptr, normalized->reachable_contributions.len, sizeof(jazz_engine_ReachableContribution));
    normalized->reachable_contributions.len = 0;
    jz_btree_drop(&ids);
    return JZ_OK;
}

SourceId NM(nested_join_source_id)(const jazz_model_JoinVia *join, const jz_str *path) {
    return sid1(jz_str_clone(&join->table), role(JAZZ_SR_ALIAS, jz_str_clone(path)));
}

static SourceId jazz_node__node__query_eval__normalization__join_lookup_source_id(const jazz_model_JoinSourceLookup *lookup, const jz_str *path) {
    return sid1(jz_str_clone(&lookup->table), role(JAZZ_SR_ALIAS, jz_format("%.*s:source_lookup", (int)path->len, path->ptr)));
}

/* ---------- output requests ---------- */


static jz_err jazz_node__node__query_eval__normalization__app_row_payload_projection(const jazz_model_Query *query, bool collect_relations, bool retain_order_keys, jazz_engine_PayloadProjection *out);

static void facts_insert(jz_btree *facts, u32 tag) {
    jazz_engine_ProgramFactKey k; __builtin_memset(&k, 0, sizeof k); k.tag = tag; /* CoverageScope::Program carries no payload */
    jz_btree_insert(facts, &k, NULL, NULL);
}

jz_err NM(current_query_output_request)(u32 output, const jazz_model_Query *query, jazz_engine_RowSetOutputRequest *out) {
    __builtin_memset(out, 0, sizeof *out);
    jz_btree_init(&out->facts, &jazz_engine_ProgramFactKey_set_desc);
    switch (output) {
    case JZN_CQPO_APP_ROWS: case JZN_CQPO_POLICY_PREDICATE: break;
    case JZN_CQPO_AUTHORIZED_ROWS: facts_insert(&out->facts, JAZZ_PFK_AUTHORIZED_ROWS); break;
    case JZN_CQPO_RELATION_SNAPSHOT:
        if (query->array_subqueries.len != 0) {
            facts_insert(&out->facts, JAZZ_PFK_RELATION_EDGES);
            facts_insert(&out->facts, JAZZ_PFK_PATH_CORRELATION_COVERAGE);
        }
        break;
    case JZN_CQPO_MAINTAINED_VIEW:
        /* A maintained aggregate receiver derives its synthetic groups from
         * the exact source closure. Aggregate ResultPayload facts are derived
         * output, not authority input. */
        if (query->aggregate) {
            facts_insert(&out->facts, JAZZ_PFK_VERSION_WITNESSES);
            facts_insert(&out->facts, JAZZ_PFK_REPLACEMENT_WITNESSES);
            facts_insert(&out->facts, JAZZ_PFK_PROGRAM_SOURCE_COVERAGE);
        } else if (query->array_subqueries.len != 0) {
            facts_insert(&out->facts, JAZZ_PFK_RESULT_MEMBERSHIP);
            facts_insert(&out->facts, JAZZ_PFK_VERSION_WITNESSES);
            facts_insert(&out->facts, JAZZ_PFK_REPLACEMENT_WITNESSES);
            facts_insert(&out->facts, JAZZ_PFK_RELATION_EDGES);
            facts_insert(&out->facts, JAZZ_PFK_PROGRAM_SOURCE_COVERAGE);
        } else {
            facts_insert(&out->facts, JAZZ_PFK_RESULT_MEMBERSHIP);
            facts_insert(&out->facts, JAZZ_PFK_VERSION_WITNESSES);
            facts_insert(&out->facts, JAZZ_PFK_REPLACEMENT_WITNESSES);
            facts_insert(&out->facts, JAZZ_PFK_PROGRAM_SOURCE_COVERAGE);
        }
        break;
    }
    if (output == JZN_CQPO_APP_ROWS || output == JZN_CQPO_POLICY_PREDICATE || output == JZN_CQPO_RELATION_SNAPSHOT || output == JZN_CQPO_MAINTAINED_VIEW) {
        out->has_app_rows = true;
        out->app_rows.public_terminal = output != JZN_CQPO_POLICY_PREDICATE;
        /* Only where `materialize_and_finalize_query_rows` strips the public
         * projection again after sorting; flat-join and include rows skip that
         * step and would leak the key. */
        jz_err e = jazz_node__node__query_eval__normalization__app_row_payload_projection(query,
            output == JZN_CQPO_MAINTAINED_VIEW || query->array_subqueries.len != 0,
            output == JZN_CQPO_APP_ROWS && !query->flat_join && query->array_subqueries.len == 0,
            &out->app_rows.projection);
        if (JZ_IS_ERR(e)) { jz_btree_drop(&out->facts); return e; }
    }
    return JZ_OK;
}

/* Whether a maintained current-read can retain only its delivered result
 * members and re-load their immutable content bodies from storage. This is a
 * proof for the small common case, not a heuristic. */
bool NM(storage_backed_maintained_view_eligible)(const jazz_model_Query *query, jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view,
    const jazz_engine_NormalizedRowSetShape *normalized) {
    jazz_protocol_ReadViewSpec def = jazz_protocol_ReadViewSpec_default();
    bool is_default = jazz_protocol_ReadViewSpec_eq(read_view, &def);
    jazz_protocol_ReadViewSpec_drop(&def);
    return tier != JAZZ_TIER_NONE
        && is_default
        && query->joins.len == 0
        && !query->flat_join
        && query->reachable.len == 0
        && query->inherits.len == 0
        && query->includes.len == 0
        && query->array_subqueries.len == 0
        && !query->aggregate
        && !query->relation
        && query->policy_branches.len == 0
        /* Normalization also injects the default root-reference closure for
         * reference-bearing tables; the storage-backed subset is a proof over
         * the normalized program, not a surface-query shortcut. */
        && normalized->closure_paths.len == 0
        && normalized->auxiliary_sources.len == 0
        && normalized->join_contributions.len == 0
        && normalized->inherited_contributions.len == 0
        && normalized->reachable_contributions.len == 0;
}

typedef JZ_VEC(jazz_engine_AppPathProjection) PathProjVec;
static PathProjVec jazz_node__node__query_eval__normalization__app_row_path_projections(const SourceId *owner, const jazz_model_ArraySubquery *subqueries, u32 n, const u32 *path, u32 npath);

static jz_err jazz_node__node__query_eval__normalization__app_row_payload_projection(const jazz_model_Query *query, bool collect_relations, bool retain_order_keys, jazz_engine_PayloadProjection *out) {
    __builtin_memset(out, 0, sizeof *out);
    /* A retained relation projection is the whole public row shape. Validation
     * rejects include/select presentation over it; never discard either here. */
    bool has_relation_projection = false;
    jazz_model_RelationProjectColumnVec columns = {0};
    if (query->relation) {
        jazz_model_RelationUnionParts parts;
        if (jazz_model__query__relation_union_parts(query->relation->rel, &parts)) {
            jazz_model_RelationUnionParts_drop(&parts);
            jz_str scope;
            JZ_TRY(jazz_model__query__relation_output_projection(query->relation, &scope, &columns));
            jz_str_drop(&scope);
            has_relation_projection = true;
        } else {
            JZ_TRY(jazz_model__query__relation_output_projection_if_present(query->relation, &has_relation_projection, &columns));
        }
    }
    if (has_relation_projection) {
        if (query->array_subqueries.len != 0 || query->has_select) {
            jazz_model_RelationProjectColumnVec_drop(&columns);
            return query_capability_msg("a relation output projection cannot carry include or select presentation");
        }
        out->tag = JAZZ_PP_RELATION;
        out->relation_columns = jz_memdup(&columns, sizeof columns);
        return JZ_OK;
    }
    PathProjVec paths = {0};
    if (collect_relations) {
        SourceId root = root_source_of(&query->table);
        paths = jazz_node__node__query_eval__normalization__app_row_path_projections(&root, query->array_subqueries.ptr, query->array_subqueries.len, NULL, 0);
        jazz_engine_SourceId_drop(&root);
    }
    if (!query->has_select && paths.len == 0) { out->tag = JAZZ_PP_SHAPE_DEFAULT; return JZ_OK; }
    jazz_engine_FieldProjection fields; __builtin_memset(&fields, 0, sizeof fields);
    if (query->has_select) {
        fields.tag = JAZZ_FP_FIELDS;
        jz_btree_init(&fields.fields, &jazz_node_StrSet_desc);
        for (u32 i = 0; i < query->select.len; i++) {
            if (str_is(&query->select.ptr[i], "id")) continue;
            jz_str c = jz_str_clone(&query->select.ptr[i]); jz_btree_insert(&fields.fields, &c, NULL, NULL);
        }
        for (u32 i = 0; i < query->includes.len; i++) {
            const jz_str *p = &query->includes.ptr[i].path;
            u32 cut = 0; while (cut < p->len && p->ptr[cut] != '.') cut++;
            jz_str root_field = jz_str_from_slice(jz_slice_of(p->ptr, cut));
            jz_btree_insert(&fields.fields, &root_field, NULL, NULL);
        }
        /* One-shot reads re-sort the emitted rows by `order_by` after
         * materialization, so an order key must reach that sort even when
         * `select` projects it away. The public projection drops it again
         * afterwards (#3503). */
        if (retain_order_keys)
            for (u32 i = 0; i < query->order_by.len; i++)
                if (!str_is(&query->order_by.ptr[i].column, "id")) {
                    jz_str c = jz_str_clone(&query->order_by.ptr[i].column); jz_btree_insert(&fields.fields, &c, NULL, NULL);
                }
    } else {
        fields.tag = JAZZ_FP_ALL;
    }
    out->tag = JAZZ_PP_TREE;
    out->tree.fields = fields;
    out->tree.paths.ptr = paths.ptr; out->tree.paths.len = paths.len; out->tree.paths.cap = paths.cap;
    return JZ_OK;
}

static PathProjVec jazz_node__node__query_eval__normalization__app_row_path_projections(const SourceId *owner, const jazz_model_ArraySubquery *subqueries, u32 n, const u32 *path, u32 npath) {
    PathProjVec out = {0};
    for (u32 index = 0; index < n; index++) {
        const jazz_model_ArraySubquery *subquery = &subqueries[index];
        u32 *child_path = jz_alloc(sizeof(u32) * (npath + 1));
        for (u32 i = 0; i < npath; i++) child_path[i] = path[i];
        child_path[npath] = index;
        SourceId child = NM(correlated_child_source_id)(owner, subquery, child_path, npath + 1);
        jazz_engine_FieldProjection fields; __builtin_memset(&fields, 0, sizeof fields);
        if (subquery->has_select) {
            fields.tag = JAZZ_FP_FIELDS;
            jz_btree_init(&fields.fields, &jazz_node_StrSet_desc);
            for (u32 i = 0; i < subquery->select.len; i++) {
                if (str_is(&subquery->select.ptr[i], "id")) continue;
                jz_str c = jz_str_clone(&subquery->select.ptr[i]); jz_btree_insert(&fields.fields, &c, NULL, NULL);
            }
        } else {
            fields.tag = JAZZ_FP_ALL;
        }
        jazz_engine_AppPathProjection p; __builtin_memset(&p, 0, sizeof p);
        p.path.owner = sid_clone(owner);
        p.path.child = sid_clone(&child);
        p.field = jz_str_clone(&subquery->column_name);
        p.cardinality = JAZZ_PCARD_MANY;
        p.fields = fields;
        PathProjVec children = jazz_node__node__query_eval__normalization__app_row_path_projections(&child, subquery->nested_arrays.ptr, subquery->nested_arrays.len, child_path, npath + 1);
        p.children.ptr = children.ptr; p.children.len = children.len; p.children.cap = children.cap;
        p.hole_policy = JAZZ_PHP_KEEP;
        jz_vec_push(&out, &p, sizeof p);
        jazz_engine_SourceId_drop(&child);
        jz_free(child_path);
    }
    return out;
}

jz_err NM(required_field_idx)(groove_RecordDescriptor descriptor, jz_slice field, u32 *out) {
    if (groove__records__RecordDescriptor__field_index(descriptor, field, out)) return JZ_OK;
    return JZN_QUERY_LOWERING("query-engine relation snapshot sink did not emit field '%.*s'", (int)field.len, (const char *)field.ptr);
}

/* ---------- predicates ---------- */
static jz_err jazz_node__node__query_eval__normalization__normalize_predicate(const RuntimeSchema *schema, const SourceId *source, const jazz_model_Predicate *predicate, bool flat_join_physical_alias, PExpr *out);

static jz_err jazz_node__node__query_eval__normalization__normalize_predicates(const RuntimeSchema *schema, const SourceId *source, const jazz_model_Predicate *predicates, u32 n, bool flat_join_physical_alias, PExpr *out) {
    if (n == 0) { *out = pe(JAZZ_NP_TRUE); return JZ_OK; }
    if (n == 1) return jazz_node__node__query_eval__normalization__normalize_predicate(schema, source, &predicates[0], flat_join_physical_alias, out);
    PExpr and = pe(JAZZ_NP_AND);
    for (u32 i = 0; i < n; i++) {
        PExpr p;
        jz_err e = jazz_node__node__query_eval__normalization__normalize_predicate(schema, source, &predicates[i], flat_join_physical_alias, &p);
        if (JZ_IS_ERR(e)) { pevec_drop(&and.children); return e; }
        jz_vec_push(&and.children, &p, sizeof p);
    }
    *out = and;
    return JZ_OK;
}

typedef JZ_VEC(jazz_model_Predicate) PredVec;
static void predvec_drop(void *p) { jz_vec_drop(p, sizeof(jazz_model_Predicate), jazz_model_Predicate_drop); }
static const jz_btree_desc routed_filters_desc = { sizeof(jz_str), sizeof(PredVec), jz_cmp_str, str_drop_v, predvec_drop };

static jz_err jazz_node__node__query_eval__normalization__route_flat_join_filters(const jazz_model_Predicate *predicate, jz_slice root_scope, jz_btree *routed) {
    if (predicate->tag == JAZZ_PRED_ALL) {
        for (u32 i = 0; i < predicate->children.len; i++)
            JZ_TRY(jazz_node__node__query_eval__normalization__route_flat_join_filters(&predicate->children.ptr[i], root_scope, routed));
        return JZ_OK;
    }
    jz_btree sources;
    JZ_TRY(jazz_model__query__flat_join_predicate_sources(predicate, &sources));
    void *first_key = NULL, *unused;
    jz_slice scope = root_scope;
    if (jz_btree_first(&sources, &first_key, &unused)) scope = jz_str_slice((const jz_str *)first_key);
    jazz_model_Predicate unqualified;
    jz_err e = jazz_model__query__unqualify_flat_join_predicate(predicate, scope, &unqualified);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&sources); return e; }
    jz_str key = jz_str_from_slice(scope);
    jz_btree_drop(&sources);
    PredVec *list = jz_btree_entry(routed, &key, NULL);
    jz_vec_push(list, &unqualified, sizeof unqualified);
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__normalization__flat_join_filters_by_source(const jazz_model_PredicateVec *filters, jz_slice root_scope, jz_btree *out) {
    jz_btree_init(out, &routed_filters_desc);
    for (u32 i = 0; i < filters->len; i++) {
        jz_err e = jazz_node__node__query_eval__normalization__route_flat_join_filters(&filters->ptr[i], root_scope, out);
        if (JZ_IS_ERR(e)) { jz_btree_drop(out); return e; }
    }
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__normalization__root_equality_literal(const jazz_model_Operand *field, const jazz_model_Operand *value, const jazz_model_Binding *binding,
    bool *some, jz_str *column, groove_Value *out) {
    *some = false;
    if (field->tag != JAZZ_OPERAND_COLUMN) return JZ_OK;
    switch (value->tag) {
    case JAZZ_OPERAND_LITERAL: groove_Value_clone(out, &value->literal); break;
    case JAZZ_OPERAND_PARAM: {
        const groove_Value *v = jz_btree_get(&binding->values, &value->name);
        if (!v) return jz_err_fmt(JZN_E_Query, "missing parameter %.*s", (int)value->name.len, value->name.ptr);
        groove_Value_clone(out, v);
        break;
    }
    default: return JZ_OK;
    }
    *column = jz_str_clone(&field->name);
    *some = true;
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__normalization__collect_root_literal_equalities(const jazz_model_Predicate *predicate, const jazz_model_Binding *binding, jz_btree *equalities) {
    switch (predicate->tag) {
    case JAZZ_PRED_ALL:
        for (u32 i = 0; i < predicate->children.len; i++)
            JZ_TRY(jazz_node__node__query_eval__normalization__collect_root_literal_equalities(&predicate->children.ptr[i], binding, equalities));
        break;
    case JAZZ_PRED_EQ: {
        bool some; jz_str field; groove_Value value;
        JZ_TRY(jazz_node__node__query_eval__normalization__root_equality_literal(&predicate->left, &predicate->right, binding, &some, &field, &value));
        if (!some) JZ_TRY(jazz_node__node__query_eval__normalization__root_equality_literal(&predicate->right, &predicate->left, binding, &some, &field, &value));
        if (some) {
            if (jz_btree_contains(equalities, &field)) { jz_str_drop(&field); groove_Value_drop(&value); }
            else jz_btree_insert(equalities, &field, &value, NULL); /* entry(field).or_insert(value) */
        }
        break;
    }
    default: break;
    }
    return JZ_OK;
}

jz_err NM(literal_equalities_for_filters)(const jazz_model_PredicateVec *filters, const jazz_model_Binding *binding, jz_btree *out) {
    jz_btree_init(out, &jazz_node_StrValue_desc);
    for (u32 i = 0; i < filters->len; i++) {
        jz_err e = jazz_node__node__query_eval__normalization__collect_root_literal_equalities(&filters->ptr[i], binding, out);
        if (JZ_IS_ERR(e)) { jz_btree_drop(out); return e; }
    }
    return JZ_OK;
}

jz_err NM(root_literal_equalities)(const jazz_model_Query *query, const jazz_model_Binding *binding, jz_btree *out) {
    return NM(literal_equalities_for_filters)(&query->filters, binding, out);
}

static groove_Value jazz_node__node__query_eval__normalization__coerce_literal_preserving_nullable(groove_Value value, const groove_ColumnType *column_type);

bool NM(select_current_access_path)(const jazz_model_TableSchema *table, const jz_btree *equalities, jazz_node_CurrentAccessPath *out) {
    __builtin_memset(out, 0, sizeof *out);
    jz_str id = S("id");
    const groove_Value *idv = jz_btree_get(equalities, &id);
    jz_str_drop(&id);
    if (idv) {
        out->tag = JAZZ_CAP_PRIMARY_KEY;
        groove_Value v; groove_Value_clone(&v, idv);
        jz_vec_push(&out->prefix, &v, sizeof v);
        return true;
    }
    jazz_node_IndexIntersectionVec probes = {0};
    jz_vec_str columns = jazz_model__schema__TableSchema__global_current_indexed_columns(table);
    for (u32 i = 0; i < columns.len; i++) {
        const groove_Value *value = jz_btree_get(equalities, &columns.ptr[i]);
        if (!value) continue;
        groove_Value c; groove_Value_clone(&c, value);
        jazz_node_IndexIntersection probe; __builtin_memset(&probe, 0, sizeof probe);
        probe.column = jz_str_clone(&columns.ptr[i]);
        groove_Value pv = NM(physical_current_index_value)(table, SL(columns.ptr[i]), c);
        jz_vec_push(&probe.values, &pv, sizeof pv);
        jz_vec_push(&probes, &probe, sizeof probe);
    }
    jz_vec_drop(&columns, sizeof(jz_str), str_drop_v);
    if (probes.len == 0) { jz_free(probes.ptr); return false; }
    out->tag = JAZZ_CAP_INDEX;
    jazz_node_IndexIntersection first = probes.ptr[0];
    out->column = first.column;
    out->prefix = first.values;
    out->has_order_column = false;
    out->reverse = false;
    jz_vec_remove(&probes, 0, NULL, sizeof(jazz_node_IndexIntersection));
    out->intersections = probes;
    out->maintained = false;
    out->candidate_filter = NULL;
    out->has_source_limit = false;
    return true;
}

/* Select a declared two-column composite index `[first, covered]` by an
 * equality on `first` alone, so each index entry also carries `covered`. */
bool NM(select_composite_leading_equality_access_path)(const jazz_model_TableSchema *table, const jz_btree *equalities, jz_slice covered, jazz_node_CurrentAccessPath *out) {
    __builtin_memset(out, 0, sizeof *out);
    jz_vec_str *columns; void *u;
    JZ_BTREE_FOR(&table->composite_indexes, columns, u) {
        if (columns->len != 2) continue;
        const jz_str *first = &columns->ptr[0], *second = &columns->ptr[1];
        if (jz_slice_cmp(jz_str_slice(second), covered) != 0 || str_is(first, "id")) continue;
        const groove_Value *value = jz_btree_get(equalities, first);
        if (!value) continue;
        groove_Value c; groove_Value_clone(&c, value);
        out->tag = JAZZ_CAP_INDEX;
        out->column = jz_str_clone(first);
        out->has_order_column = true; out->order_column = jz_str_clone(second);
        out->reverse = false;
        groove_Value pv = NM(physical_current_index_value)(table, jz_str_slice(first), c);
        jz_vec_push(&out->prefix, &pv, sizeof pv);
        out->maintained = false; out->candidate_filter = NULL; out->has_source_limit = false;
        return true;
    }
    return false;
}

static groove_Value value_some(groove_Value inner) { return groove_Value_some(inner); }

/* Current storage uses one nullable envelope to represent an un-authored cell.
 * Predicates use logical values, but secondary-index keys are physical
 * current-row values, so preserve that declared nullable shape before adding
 * the storage envelope. */
groove_Value NM(physical_current_index_value)(const jazz_model_TableSchema *table, jz_slice column, groove_Value value) {
    groove_Value logical_value = value;
    for (u32 i = 0; i < table->columns.len; i++) {
        if (jz_slice_cmp(jz_str_slice(&table->columns.ptr[i].name), column) == 0) {
            logical_value = jazz_node__node__query_eval__normalization__coerce_literal_preserving_nullable(value, &table->columns.ptr[i].column_type);
            break;
        }
    }
    return value_some(logical_value);
}

static groove_Value jazz_node__node__query_eval__normalization__coerce_literal_preserving_nullable(groove_Value value, const groove_ColumnType *column_type) {
    if (column_type->tag == GROOVE_VT_NULLABLE) {
        const groove_ColumnType *inner = groove_ValueType_inner(column_type);
        if (value.tag == GROOVE_V_NULLABLE) {
            if (!value.v.nullable) return value;
            groove_Value in = *value.v.nullable; jz_free(value.v.nullable);
            return value_some(jazz_node__node__query_eval__normalization__coerce_literal_preserving_nullable(in, inner));
        }
        return value_some(jazz_node__node__query_eval__normalization__coerce_literal_preserving_nullable(value, inner));
    }
    return NM(coerce_literal_for_column_type)(value, column_type);
}

groove_StaticScanSpec NM(static_scan_for_prefix)(groove_ValueVec prefix, u32 full_key_len) {
    groove_StaticScanSpec spec; __builtin_memset(&spec, 0, sizeof spec);
    for (u32 i = 0; i < prefix.len; i++) {
        groove_LiteralValue *l = groove__ivm__op_types__LiteralValue__from_value(prefix.ptr[i]);
        jz_vec_push(&spec.prefix, &l, sizeof l);
    }
    jz_free(prefix.ptr);
    spec.tag = spec.prefix.len == full_key_len ? GROOVE_SCAN_POINT : GROOVE_SCAN_PREFIX;
    return spec;
}

/* ---------- operands ---------- */
static bool jazz_node__node__query_eval__normalization__provenance_field(const jz_str *column, jazz_engine_ProvenanceField *out) {
    if (str_is(column, "$createdAt")) { *out = JAZZ_PROV_CREATED_AT; return true; }
    if (str_is(column, "$createdBy")) { *out = JAZZ_PROV_CREATED_BY; return true; }
    if (str_is(column, "$updatedAt")) { *out = JAZZ_PROV_UPDATED_AT; return true; }
    if (str_is(column, "$updatedBy")) { *out = JAZZ_PROV_UPDATED_BY; return true; }
    return false;
}

static jz_err literal_bytes(const groove_Value *value, jz_bytes *out) {
    jz_err e = groove__records__Value__to_postcard(value, out);
    if (JZ_IS_ERR(e)) {
        int n = e.msg ? (int)e.msg->len : 0; const char *p = e.msg ? e.msg->ptr : "";
        jz_err w = JZN_QUERY_LOWERING("literal encoding failed: %.*s", n, p);
        jz_err_drop(&e);
        return w;
    }
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__normalization__normalize_operand_with_target_type_and_flat_join_alias(const SourceId *source, const jazz_model_Operand *operand,
    const groove_ColumnType *target_type, bool flat_join_physical_alias, VRef *out) {
    switch (operand->tag) {
    case JAZZ_OPERAND_COLUMN: {
        if (flat_join_physical_alias && str_is(&operand->name, "_id")) { *out = vr_row_id(source); return JZ_OK; }
        if (str_is(&operand->name, "id")) { *out = vr_row_id(source); return JZ_OK; }
        jazz_engine_ProvenanceField field;
        if (jazz_node__node__query_eval__normalization__provenance_field(&operand->name, &field)) {
            VRef v = vr(JAZZ_NVR_PROVENANCE); v.source = sid_clone(source); v.provenance = field; *out = v;
        } else {
            *out = vr_field(source, jz_str_clone(&operand->name));
        }
        return JZ_OK;
    }
    case JAZZ_OPERAND_PARAM: *out = vr_param(jz_str_clone(&operand->name)); return JZ_OK;
    case JAZZ_OPERAND_CLAIM: *out = vr_claim(claim_path_of(SL(operand->name))); return JZ_OK;
    default: {
        groove_Value value; groove_Value_clone(&value, &operand->literal);
        if (target_type) value = NM(coerce_literal_for_column_type)(value, target_type);
        VRef v = vr(JAZZ_NVR_LITERAL);
        jz_err e = literal_bytes(&value, &v.literal);
        groove_Value_drop(&value);
        if (JZ_IS_ERR(e)) return e;
        *out = v;
        return JZ_OK;
    }
    }
}

static jz_err jazz_node__node__query_eval__normalization__normalize_operand_with_target_type(const SourceId *source, const jazz_model_Operand *operand, const groove_ColumnType *target_type, VRef *out) {
    return jazz_node__node__query_eval__normalization__normalize_operand_with_target_type_and_flat_join_alias(source, operand, target_type, false, out);
}

static jz_err jazz_node__node__query_eval__normalization__normalize_operand(const SourceId *source, const jazz_model_Operand *operand, VRef *out) {
    return jazz_node__node__query_eval__normalization__normalize_operand_with_target_type(source, operand, NULL, out);
}

static jz_err normalize_column(const SourceId *source, const jz_str *column, VRef *out) {
    jazz_model_Operand op; __builtin_memset(&op, 0, sizeof op);
    op.tag = JAZZ_OPERAND_COLUMN; op.name = *column; /* borrowed */
    return jazz_node__node__query_eval__normalization__normalize_operand(source, &op, out);
}

jz_err NM(table_schema)(const RuntimeSchema *schema, jz_slice table, const jazz_model_TableSchema **out) {
    for (u32 i = 0; i < schema->tables.len; i++)
        if (jz_slice_cmp(jz_str_slice(&schema->tables.ptr[i].name), table) == 0) { *out = &schema->tables.ptr[i]; return JZ_OK; }
    return JZN_QUERY_LOWERING("unknown query table %.*s", (int)table.len, (const char *)table.ptr);
}

jz_err NM(operand_column_type)(const RuntimeSchema *schema, const SourceId *source, const jazz_model_Operand *operand, bool *has, groove_ColumnType *out) {
    *has = false;
    if (operand->tag != JAZZ_OPERAND_COLUMN) return JZ_OK;
    jazz_engine_ProvenanceField field;
    if (jazz_node__node__query_eval__normalization__provenance_field(&operand->name, &field)) {
        *has = true;
        *out = (field == JAZZ_PROV_CREATED_AT || field == JAZZ_PROV_UPDATED_AT) ? ty_simple(GROOVE_VT_U64) : jazz_types__ids__RowAuthor__value_type();
        return JZ_OK;
    }
    if (str_is(&operand->name, "id")) { *has = true; *out = ty_simple(GROOVE_VT_UUID); return JZ_OK; }
    const jazz_model_TableSchema *table;
    JZ_TRY(NM(table_schema)(schema, SL(source->table), &table));
    for (u32 i = 0; i < table->columns.len; i++)
        if (jz_str_eq(&table->columns.ptr[i].name, &operand->name)) { *has = true; *out = ty_clone(&table->columns.ptr[i].column_type); return JZ_OK; }
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__normalization__predicate_operand_column_type(const RuntimeSchema *schema, const SourceId *source, const jazz_model_Operand *operand,
    bool flat_join_physical_alias, bool *has, groove_ColumnType *out) {
    if (flat_join_physical_alias && operand->tag == JAZZ_OPERAND_COLUMN && str_is(&operand->name, "_id")) {
        *has = true; *out = ty_simple(GROOVE_VT_UUID); return JZ_OK;
    }
    return NM(operand_column_type)(schema, source, operand, has, out);
}

static bool needle_of(const groove_ColumnType *t, groove_ColumnType *out) {
    if (t->tag == GROOVE_VT_ARRAY) { *out = ty_clone(groove_ValueType_inner(t)); return true; }
    if (t->tag == GROOVE_VT_NULLABLE) {
        const groove_ColumnType *inner = groove_ValueType_inner(t);
        if (inner->tag == GROOVE_VT_ARRAY) { *out = ty_clone(groove_ValueType_inner(inner)); return true; }
        if (inner->tag == GROOVE_VT_STRING) { *out = ty_simple(GROOVE_VT_STRING); return true; }
        return false;
    }
    if (t->tag == GROOVE_VT_STRING) { *out = ty_simple(GROOVE_VT_STRING); return true; }
    return false;
}

jz_err NM(contains_needle_type)(const RuntimeSchema *schema, const SourceId *source, const jazz_model_Operand *value, bool *has, groove_ColumnType *out) {
    bool h; groove_ColumnType t;
    JZ_TRY(NM(operand_column_type)(schema, source, value, &h, &t));
    *has = h && needle_of(&t, out);
    if (h) groove_ValueType_drop(&t);
    return JZ_OK;
}

groove_Value NM(coerce_literal_for_column_type)(groove_Value value, const groove_ColumnType *column_type) {
    if (value.tag == GROOVE_V_UUID && column_type->tag == GROOVE_VT_STRING) {
        char buf[37]; jz_uuid_to_string(&value.v.uuid, buf);
        return groove_Value_string(jz_str_from_slice(jz_slice_of(buf, 36)));
    }
    if (value.tag == GROOVE_V_STRING && column_type->tag == GROOVE_VT_UUID) {
        jz_uuid u;
        jz_err e = jz_uuid_parse(jz_str_slice(&value.v.s), &u);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); return value; }
        jz_str_drop(&value.v.s);
        return groove_Value_uuid(u);
    }
    if (value.tag == GROOVE_V_NULLABLE && value.v.nullable && column_type->tag == GROOVE_VT_NULLABLE) {
        groove_Value in = *value.v.nullable; jz_free(value.v.nullable);
        return value_some(NM(coerce_literal_for_column_type)(in, groove_ValueType_inner(column_type)));
    }
    if (value.tag == GROOVE_V_ARRAY && column_type->tag == GROOVE_VT_ARRAY) {
        const groove_ColumnType *inner = groove_ValueType_inner(column_type);
        for (u32 i = 0; i < value.v.vec.len; i++) value.v.vec.ptr[i] = NM(coerce_literal_for_column_type)(value.v.vec.ptr[i], inner);
        return value;
    }
    if (value.tag == GROOVE_V_TUPLE && column_type->tag == GROOVE_VT_TUPLE) {
        const JZ_VEC(groove_ColumnType) *types = column_type->payload;
        if (types && types->len == value.v.vec.len) {
            for (u32 i = 0; i < value.v.vec.len; i++) value.v.vec.ptr[i] = NM(coerce_literal_for_column_type)(value.v.vec.ptr[i], &types->ptr[i]);
            return value;
        }
    }
    if (value.tag == GROOVE_V_NULLABLE && value.v.nullable) {
        groove_Value in = *value.v.nullable; jz_free(value.v.nullable);
        return value_some(NM(coerce_literal_for_column_type)(in, column_type));
    }
    if (column_type->tag == GROOVE_VT_NULLABLE) return NM(coerce_literal_for_column_type)(value, groove_ValueType_inner(column_type));
    return value;
}

static jz_err jazz_node__node__query_eval__normalization__normalize_compare(const RuntimeSchema *schema, const SourceId *source, const jazz_model_Operand *left,
    jazz_engine_ComparisonOp op, const jazz_model_Operand *right, bool flat_join_physical_alias, PExpr *out) {
    bool lh, rh; groove_ColumnType lt, rt;
    JZ_TRY(jazz_node__node__query_eval__normalization__predicate_operand_column_type(schema, source, left, flat_join_physical_alias, &lh, &lt));
    jz_err e = jazz_node__node__query_eval__normalization__predicate_operand_column_type(schema, source, right, flat_join_physical_alias, &rh, &rt);
    if (JZ_IS_ERR(e)) { if (lh) groove_ValueType_drop(&lt); return e; }
    VRef l, r;
    e = jazz_node__node__query_eval__normalization__normalize_operand_with_target_type_and_flat_join_alias(source, left, rh ? &rt : NULL, flat_join_physical_alias, &l);
    if (!JZ_IS_ERR(e)) {
        e = jazz_node__node__query_eval__normalization__normalize_operand_with_target_type_and_flat_join_alias(source, right, lh ? &lt : NULL, flat_join_physical_alias, &r);
        if (JZ_IS_ERR(e)) jazz_engine_NormalizedValueRef_drop(&l);
    }
    if (lh) groove_ValueType_drop(&lt);
    if (rh) groove_ValueType_drop(&rt);
    if (JZ_IS_ERR(e)) return e;
    *out = pe_compare(l, op, r);
    return JZ_OK;
}

jz_err jazz_node__node__query_eval__normalization__normalize_enum_payload_predicate(groove_RecordDescriptor descriptor, const SourceId *source, const jazz_model_Predicate *predicate, PExpr *out);

static jazz_engine_ComparisonOp compare_op_of(u32 tag) {
    switch (tag) {
    case JAZZ_PRED_EQ: return JAZZ_CMP_EQ;
    case JAZZ_PRED_NE: return JAZZ_CMP_NE;
    case JAZZ_PRED_GT: return JAZZ_CMP_GT;
    case JAZZ_PRED_GTE: return JAZZ_CMP_GTE;
    case JAZZ_PRED_LT: return JAZZ_CMP_LT;
    default: return JAZZ_CMP_LTE;
    }
}

static jz_err jazz_node__node__query_eval__normalization__normalize_predicate(const RuntimeSchema *schema, const SourceId *source, const jazz_model_Predicate *predicate, bool flat_join_physical_alias, PExpr *out) {
    switch (predicate->tag) {
    case JAZZ_PRED_ALL: case JAZZ_PRED_ANY: {
        PExpr p = pe(predicate->tag == JAZZ_PRED_ALL ? JAZZ_NP_AND : JAZZ_NP_OR);
        for (u32 i = 0; i < predicate->children.len; i++) {
            PExpr c;
            jz_err e = jazz_node__node__query_eval__normalization__normalize_predicate(schema, source, &predicate->children.ptr[i], flat_join_physical_alias, &c);
            if (JZ_IS_ERR(e)) { pevec_drop(&p.children); return e; }
            jz_vec_push(&p.children, &c, sizeof c);
        }
        *out = p;
        return JZ_OK;
    }
    case JAZZ_PRED_NOT: {
        PExpr inner;
        JZ_TRY(jazz_node__node__query_eval__normalization__normalize_predicate(schema, source, predicate->inner, flat_join_physical_alias, &inner));
        PExpr p = pe(JAZZ_NP_NOT); p.inner = jz_memdup(&inner, sizeof inner); *out = p;
        return JZ_OK;
    }
    case JAZZ_PRED_EQ: case JAZZ_PRED_NE: case JAZZ_PRED_GT: case JAZZ_PRED_GTE: case JAZZ_PRED_LT: case JAZZ_PRED_LTE:
        return jazz_node__node__query_eval__normalization__normalize_compare(schema, source, &predicate->left, compare_op_of(predicate->tag), &predicate->right, flat_join_physical_alias, out);
    case JAZZ_PRED_IN: {
        PExpr p = pe(JAZZ_NP_IN);
        JZ_TRY(jazz_node__node__query_eval__normalization__normalize_operand_with_target_type_and_flat_join_alias(source, &predicate->left, NULL, flat_join_physical_alias, &p.left));
        for (u32 i = 0; i < predicate->options.len; i++) {
            bool has; groove_ColumnType ty;
            jz_err e = jazz_node__node__query_eval__normalization__predicate_operand_column_type(schema, source, &predicate->left, flat_join_physical_alias, &has, &ty);
            VRef option;
            if (!JZ_IS_ERR(e)) {
                e = jazz_node__node__query_eval__normalization__normalize_operand_with_target_type_and_flat_join_alias(source, &predicate->options.ptr[i], has ? &ty : NULL, flat_join_physical_alias, &option);
                if (has) groove_ValueType_drop(&ty);
            }
            if (JZ_IS_ERR(e)) { jazz_engine_PredicateExpr_drop(&p); return e; }
            jz_vec_push(&p.options, &option, sizeof option);
        }
        *out = p;
        return JZ_OK;
    }
    case JAZZ_PRED_CONTAINS: {
        PExpr p = pe(JAZZ_NP_ARRAY_CONTAINS);
        JZ_TRY(jazz_node__node__query_eval__normalization__normalize_operand_with_target_type_and_flat_join_alias(source, &predicate->left, NULL, flat_join_physical_alias, &p.left));
        bool has; groove_ColumnType ty;
        jz_err e = NM(contains_needle_type)(schema, source, &predicate->left, &has, &ty);
        if (!JZ_IS_ERR(e)) {
            e = jazz_node__node__query_eval__normalization__normalize_operand_with_target_type_and_flat_join_alias(source, &predicate->right, has ? &ty : NULL, flat_join_physical_alias, &p.right);
            if (has) groove_ValueType_drop(&ty);
        }
        if (JZ_IS_ERR(e)) { jazz_engine_NormalizedValueRef_drop(&p.left); return e; }
        *out = p;
        return JZ_OK;
    }
    case JAZZ_PRED_IS_NULL: {
        PExpr p = pe(JAZZ_NP_IS_NULL);
        JZ_TRY(jazz_node__node__query_eval__normalization__normalize_operand_with_target_type_and_flat_join_alias(source, &predicate->left, NULL, flat_join_physical_alias, &p.left));
        *out = p;
        return JZ_OK;
    }
    default: { /* EnumMatch */
        jazz_model_Operand column; __builtin_memset(&column, 0, sizeof column);
        column.tag = JAZZ_OPERAND_COLUMN; column.name = predicate->column;
        bool has; groove_ColumnType column_type;
        JZ_TRY(NM(operand_column_type)(schema, source, &column, &has, &column_type));
        if (!has) return query_lowering_msg("enum match column has no type");
        const groove_ColumnType *ct = column_type.tag == GROOVE_VT_NULLABLE ? groove_ValueType_inner(&column_type) : &column_type;
        const groove_EnumSchema *enum_schema = ct->tag == GROOVE_VT_ENUM ? groove_ValueType_as_enum(ct) : NULL;
        if (!enum_schema) { groove_ValueType_drop(&column_type); return query_lowering_msg("enum match requires a payload enum column"); }
        u32 case_tag; const groove_RecordDescriptor_s *payload_descriptor;
        if (!groove__records__values__EnumSchema__case_by_name(enum_schema, SL(predicate->enum_case), &case_tag, &payload_descriptor)) {
            groove_ValueType_drop(&column_type);
            return JZN_QUERY_LOWERING("unknown payload enum case %.*s", (int)predicate->enum_case.len, predicate->enum_case.ptr);
        }
        PExpr p = pe(JAZZ_NP_ENUM_MATCH);
        jz_err e = jazz_node__node__query_eval__normalization__normalize_operand(source, &column, &p.left);
        if (JZ_IS_ERR(e)) { groove_ValueType_drop(&column_type); return e; }
        p.case_tag = case_tag;
        PExpr payload;
        e = jazz_node__node__query_eval__normalization__normalize_enum_payload_predicate(payload_descriptor, source, predicate->inner, &payload);
        groove_ValueType_drop(&column_type);
        if (JZ_IS_ERR(e)) { jazz_engine_NormalizedValueRef_drop(&p.left); return e; }
        p.inner = jz_memdup(&payload, sizeof payload);
        *out = p;
        return JZ_OK;
    }
    }
}

/* ---------- enum payload predicates ---------- */
static bool jazz_node__node__query_eval__normalization__enum_payload_field_type(groove_RecordDescriptor descriptor, const jz_str *field, groove_ColumnType *out) {
    const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(descriptor);
    for (u32 i = 0; i < fields->len; i++)
        if (fields->ptr[i].has_name && jz_str_eq(&fields->ptr[i].name, field)) { *out = ty_clone(&fields->ptr[i].value_type); return true; }
    return false;
}

static jz_err jazz_node__node__query_eval__normalization__enum_payload_operand_type(groove_RecordDescriptor descriptor, const jazz_model_Operand *operand, bool *has, groove_ColumnType *out) {
    *has = false;
    if (operand->tag != JAZZ_OPERAND_COLUMN) return JZ_OK;
    if (!jazz_node__node__query_eval__normalization__enum_payload_field_type(descriptor, &operand->name, out))
        return JZN_QUERY_LOWERING("unknown payload enum field %.*s", (int)operand->name.len, operand->name.ptr);
    *has = true;
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__normalization__enum_payload_contains_needle_type(groove_RecordDescriptor descriptor, const jazz_model_Operand *value, bool *has, groove_ColumnType *out) {
    bool h; groove_ColumnType t;
    JZ_TRY(jazz_node__node__query_eval__normalization__enum_payload_operand_type(descriptor, value, &h, &t));
    *has = h && needle_of(&t, out);
    if (h) groove_ValueType_drop(&t);
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__normalization__normalize_enum_payload_operand(groove_RecordDescriptor descriptor, const SourceId *source, const jazz_model_Operand *operand,
    const groove_ColumnType *target_type, VRef *out) {
    switch (operand->tag) {
    case JAZZ_OPERAND_COLUMN: {
        groove_ColumnType t;
        if (!jazz_node__node__query_eval__normalization__enum_payload_field_type(descriptor, &operand->name, &t))
            return JZN_QUERY_LOWERING("unknown payload enum field %.*s", (int)operand->name.len, operand->name.ptr);
        groove_ValueType_drop(&t);
        *out = vr_field(source, jz_str_clone(&operand->name));
        return JZ_OK;
    }
    case JAZZ_OPERAND_PARAM: *out = vr_param(jz_str_clone(&operand->name)); return JZ_OK;
    case JAZZ_OPERAND_CLAIM: *out = vr_claim(claim_path_of(SL(operand->name))); return JZ_OK;
    default: {
        groove_Value value; groove_Value_clone(&value, &operand->literal);
        if (target_type) value = NM(coerce_literal_for_column_type)(value, target_type);
        VRef v = vr(JAZZ_NVR_LITERAL);
        jz_err e = literal_bytes(&value, &v.literal);
        groove_Value_drop(&value);
        if (JZ_IS_ERR(e)) return e;
        *out = v;
        return JZ_OK;
    }
    }
}

static jz_err jazz_node__node__query_eval__normalization__normalize_enum_payload_compare(groove_RecordDescriptor descriptor, const SourceId *source, const jazz_model_Operand *left,
    jazz_engine_ComparisonOp op, const jazz_model_Operand *right, PExpr *out) {
    bool lh, rh; groove_ColumnType lt, rt;
    JZ_TRY(jazz_node__node__query_eval__normalization__enum_payload_operand_type(descriptor, left, &lh, &lt));
    jz_err e = jazz_node__node__query_eval__normalization__enum_payload_operand_type(descriptor, right, &rh, &rt);
    if (JZ_IS_ERR(e)) { if (lh) groove_ValueType_drop(&lt); return e; }
    VRef l, r;
    e = jazz_node__node__query_eval__normalization__normalize_enum_payload_operand(descriptor, source, left, rh ? &rt : NULL, &l);
    if (!JZ_IS_ERR(e)) {
        e = jazz_node__node__query_eval__normalization__normalize_enum_payload_operand(descriptor, source, right, lh ? &lt : NULL, &r);
        if (JZ_IS_ERR(e)) jazz_engine_NormalizedValueRef_drop(&l);
    }
    if (lh) groove_ValueType_drop(&lt);
    if (rh) groove_ValueType_drop(&rt);
    if (JZ_IS_ERR(e)) return e;
    *out = pe_compare(l, op, r);
    return JZ_OK;
}

/* Normalize a predicate evaluated inside one selected payload-enum case.
 * Payload fields are case-local. They must never be resolved against the
 * outer table, even when that table happens to have the same field name. */
jz_err jazz_node__node__query_eval__normalization__normalize_enum_payload_predicate(groove_RecordDescriptor descriptor, const SourceId *source, const jazz_model_Predicate *predicate, PExpr *out) {
    switch (predicate->tag) {
    case JAZZ_PRED_ALL: case JAZZ_PRED_ANY: {
        PExpr p = pe(predicate->tag == JAZZ_PRED_ALL ? JAZZ_NP_AND : JAZZ_NP_OR);
        for (u32 i = 0; i < predicate->children.len; i++) {
            PExpr c;
            jz_err e = jazz_node__node__query_eval__normalization__normalize_enum_payload_predicate(descriptor, source, &predicate->children.ptr[i], &c);
            if (JZ_IS_ERR(e)) { pevec_drop(&p.children); return e; }
            jz_vec_push(&p.children, &c, sizeof c);
        }
        *out = p;
        return JZ_OK;
    }
    case JAZZ_PRED_NOT: {
        PExpr inner;
        JZ_TRY(jazz_node__node__query_eval__normalization__normalize_enum_payload_predicate(descriptor, source, predicate->inner, &inner));
        PExpr p = pe(JAZZ_NP_NOT); p.inner = jz_memdup(&inner, sizeof inner); *out = p;
        return JZ_OK;
    }
    case JAZZ_PRED_EQ: case JAZZ_PRED_NE: case JAZZ_PRED_GT: case JAZZ_PRED_GTE: case JAZZ_PRED_LT: case JAZZ_PRED_LTE:
        return jazz_node__node__query_eval__normalization__normalize_enum_payload_compare(descriptor, source, &predicate->left, compare_op_of(predicate->tag), &predicate->right, out);
    case JAZZ_PRED_IN: {
        bool has; groove_ColumnType target_type;
        JZ_TRY(jazz_node__node__query_eval__normalization__enum_payload_operand_type(descriptor, &predicate->left, &has, &target_type));
        PExpr p = pe(JAZZ_NP_IN);
        jz_err e = jazz_node__node__query_eval__normalization__normalize_enum_payload_operand(descriptor, source, &predicate->left, NULL, &p.left);
        for (u32 i = 0; !JZ_IS_ERR(e) && i < predicate->options.len; i++) {
            VRef option;
            e = jazz_node__node__query_eval__normalization__normalize_enum_payload_operand(descriptor, source, &predicate->options.ptr[i], has ? &target_type : NULL, &option);
            if (!JZ_IS_ERR(e)) jz_vec_push(&p.options, &option, sizeof option);
        }
        if (has) groove_ValueType_drop(&target_type);
        if (JZ_IS_ERR(e)) { jazz_engine_PredicateExpr_drop(&p); return e; }
        *out = p;
        return JZ_OK;
    }
    case JAZZ_PRED_CONTAINS: {
        bool has; groove_ColumnType needle_type;
        JZ_TRY(jazz_node__node__query_eval__normalization__enum_payload_contains_needle_type(descriptor, &predicate->left, &has, &needle_type));
        PExpr p = pe(JAZZ_NP_ARRAY_CONTAINS);
        jz_err e = jazz_node__node__query_eval__normalization__normalize_enum_payload_operand(descriptor, source, &predicate->left, NULL, &p.left);
        if (!JZ_IS_ERR(e)) {
            e = jazz_node__node__query_eval__normalization__normalize_enum_payload_operand(descriptor, source, &predicate->right, has ? &needle_type : NULL, &p.right);
            if (JZ_IS_ERR(e)) jazz_engine_NormalizedValueRef_drop(&p.left);
        }
        if (has) groove_ValueType_drop(&needle_type);
        if (JZ_IS_ERR(e)) return e;
        *out = p;
        return JZ_OK;
    }
    case JAZZ_PRED_IS_NULL: {
        PExpr p = pe(JAZZ_NP_IS_NULL);
        JZ_TRY(jazz_node__node__query_eval__normalization__normalize_enum_payload_operand(descriptor, source, &predicate->left, NULL, &p.left));
        *out = p;
        return JZ_OK;
    }
    default:
        return query_lowering_msg("nested payload enum matches are not supported");
    }
}

/* ---------- order / aggregate ---------- */
static jz_err jazz_node__node__query_eval__normalization__normalize_order_key(const SourceId *source, const jazz_model_OrderBy *order, jazz_engine_OrderKey *out) {
    JZ_TRY(normalize_column(source, &order->column, &out->value));
    out->direction = order->direction == JAZZ_ORDER_ASC ? JAZZ_SORT_ASC : JAZZ_SORT_DESC;
    return JZ_OK;
}

typedef JZ_VEC(jazz_engine_OrderKey) OrderKeyVec;
static void order_key_drop(void *p) { jazz_engine_NormalizedValueRef_drop(&((jazz_engine_OrderKey *)p)->value); }

static jz_err order_keys_for(const SourceId *source, const jazz_model_OrderBy *orders, u32 n, OrderKeyVec *out) {
    out->ptr = NULL; out->len = out->cap = 0;
    for (u32 i = 0; i < n; i++) {
        jazz_engine_OrderKey k;
        jz_err e = jazz_node__node__query_eval__normalization__normalize_order_key(source, &orders[i], &k);
        if (JZ_IS_ERR(e)) { jz_vec_drop(out, sizeof(jazz_engine_OrderKey), order_key_drop); return e; }
        jz_vec_push(out, &k, sizeof k);
    }
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__normalization__normalized_aggregate_group_by(const SourceId *source, const jazz_model_AggregateQuery *aggregate, jazz_engine_ValueRefVec *out) {
    out->ptr = NULL; out->len = out->cap = 0;
    if (aggregate->has_group_by) {
        VRef v;
        JZ_TRY(normalize_column(source, &aggregate->group_by, &v));
        jz_vec_push(out, &v, sizeof v);
    }
    return JZ_OK;
}

static jazz_engine_AggregateFunction jazz_node__node__query_eval__normalization__normalized_aggregate_function(jazz_model_AggregateFunction function) {
    switch (function) {
    case JAZZ_AGG_COUNT: return JAZZ_NAGG_COUNT;
    case JAZZ_AGG_SUM: return JAZZ_NAGG_SUM;
    case JAZZ_AGG_AVG: return JAZZ_NAGG_AVG;
    case JAZZ_AGG_MIN: return JAZZ_NAGG_MIN;
    default: return JAZZ_NAGG_MAX;
    }
}

static groove_ColumnType jazz_node__node__query_eval__normalization__normalized_aggregate_output_type(const jazz_model_Aggregate *aggregate) {
    switch (aggregate->function) {
    case JAZZ_AGG_COUNT: return ty_simple(GROOVE_VT_U64);
    case JAZZ_AGG_AVG: return groove_ValueType_nullable(ty_simple(GROOVE_VT_F64));
    /* Aggregate lowering is currently reported as an unsupported query-engine
     * capability before Groove needs the exact result type. */
    default: return groove_ValueType_nullable(ty_simple(GROOVE_VT_BYTES));
    }
}

static void aggregate_expr_drop(void *p) {
    jazz_engine_AggregateExpr *a = p;
    jz_str_drop(&a->output.name); groove_ValueType_drop(&a->output.ty);
    if (a->has_input) jazz_engine_NormalizedValueRef_drop(&a->input);
}

static jz_err jazz_node__node__query_eval__normalization__normalized_aggregate_outputs(const SourceId *source, const jazz_model_AggregateQuery *aggregate, void *out_vec) {
    JZ_VEC(jazz_engine_AggregateExpr) *out = out_vec;
    out->ptr = NULL; out->len = out->cap = 0;
    for (u32 i = 0; i < aggregate->aggregates.len; i++) {
        const jazz_model_Aggregate *a = &aggregate->aggregates.ptr[i];
        jazz_engine_AggregateExpr x; __builtin_memset(&x, 0, sizeof x);
        if (a->has_column) {
            jz_err e = normalize_column(source, &a->column, &x.input);
            if (JZ_IS_ERR(e)) { jz_vec_drop(out, sizeof(jazz_engine_AggregateExpr), aggregate_expr_drop); return e; }
            x.has_input = true;
        }
        x.output = typed_output_field(jz_str_clone(&a->alias), jazz_node__node__query_eval__normalization__normalized_aggregate_output_type(a));
        x.function = jazz_node__node__query_eval__normalization__normalized_aggregate_function(a->function);
        jz_vec_push(out, &x, sizeof x);
    }
    return JZ_OK;
}

static jazz_engine_CorrelationRequirement jazz_node__node__query_eval__normalization__array_requirement(jazz_model_ArraySubqueryRequirement requirement) {
    switch (requirement) {
    case JAZZ_ASQ_OPTIONAL: return JAZZ_CR_OPTIONAL;
    case JAZZ_ASQ_AT_LEAST_ONE: return JAZZ_CR_AT_LEAST_ONE;
    default: return JAZZ_CR_MATCH;
    }
}

static jz_str path_id_of(const u32 *path, u32 n) {
    jz_str s = {0};
    for (u32 i = 0; i < n; i++) {
        if (i) jz_str_push_char(&s, '.');
        jz_str_appendf(&s, "%u", path[i]);
    }
    return s;
}

SourceId NM(correlated_child_source_id)(const SourceId *owner, const jazz_model_ArraySubquery *subquery, const u32 *path, u32 n) {
    SourceId s; s.table = jz_str_clone(&subquery->table);
    jz_vec_clone(&s.path.components, &owner->path.components, sizeof(jazz_engine_SourceRole), NULL);
    for (u32 i = 0; i < s.path.components.len; i++) s.path.components.ptr[i].name = jz_str_clone(&owner->path.components.ptr[i].name);
    jz_str path_id = path_id_of(path, n);
    jazz_engine_SourceRole r = role(JAZZ_SR_CORRELATED_CHILD,
        jz_format("%.*s:%.*s", (int)path_id.len, path_id.ptr, (int)subquery->column_name.len, subquery->column_name.ptr));
    jz_str_drop(&path_id);
    jz_vec_push(&s.path.components, &r, sizeof r);
    return s;
}

/* The source an implicit root reference `column` reads from `table`. Source
 * identities cross native/WASM peers, so implicit references get their own
 * stable, column-named namespace. */
SourceId NM(implicit_reference_source_id)(jz_slice table, jz_slice column) {
    return sid2(jz_str_from_slice(table), role(JAZZ_SR_ROOT, empty_str()),
        role(JAZZ_SR_ALIAS, jz_format("reference:%.*s", (int)column.len, (const char *)column.ptr)));
}

SourceId NM(include_auxiliary_source_id)(jz_str table, u32 include_index, u32 segment_index) {
    return sid2(table, role(JAZZ_SR_ROOT, empty_str()), role(JAZZ_SR_ALIAS, jz_format("include:%u:%u", include_index, segment_index)));
}

/* ---------- normalization context (the &mut accumulators threaded through) ---------- */
typedef __typeof__(((jazz_engine_NormalizedRowSetShape *)0)->closure_paths) ClosurePathVec;
typedef __typeof__(((jazz_engine_NormalizedRowSetShape *)0)->join_contributions) JoinContribVec;
typedef __typeof__(((jazz_engine_NormalizedRowSetShape *)0)->inherited_contributions) InheritedContribVec;
typedef __typeof__(((jazz_engine_NormalizedRowSetShape *)0)->reachable_contributions) ReachableContribVec;
typedef struct NormCtx {
    jz_btree *nodes;               /* RowSetNodeId -> RowSetExpr */
    jz_btree *auxiliary_sources;   /* BTreeSet<SourceId> */
    JoinContribVec *join_contributions;
    InheritedContribVec *inherited_contributions;
    ReachableContribVec *reachable_contributions;
} NormCtx;

static void sourceset_insert(jz_btree *set, const SourceId *s) { SourceId c = sid_clone(s); jz_btree_insert(set, &c, NULL, NULL); }

static void closure_path_segment_drop(void *p) {
    jazz_engine_ClosurePathSegment *s = p;
    jazz_engine_SourceId_drop(&s->parent); jazz_engine_SourceId_drop(&s->target); jz_str_drop(&s->source_field);
}
static void closure_path_drop(void *p) {
    jazz_engine_ClosurePath *c = p;
    jz_str_drop(&c->id);
    jz_vec_drop(&c->segments, sizeof(jazz_engine_ClosurePathSegment), closure_path_segment_drop);
}

static jz_err jazz_node__node__query_eval__normalization__collect_closure_paths(const jazz_node_NodeState *node, const jz_str *root_table, jazz_types_SchemaVersionId schema_version,
    const jazz_model_Include *includes, u32 nincludes, jz_btree *out_sources, ClosurePathVec *out_paths) {
    jz_btree_init(out_sources, &jazz_engine_SourceId_set_desc);
    out_paths->ptr = NULL; out_paths->len = out_paths->cap = 0;
    SourceId root_source = root_source_of(root_table);
    const jazz_model_TableSchema *root_schema;
    jz_err e = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(node, SL(*root_table), schema_version, &root_schema);
    if (JZ_IS_ERR(e)) goto fail;
    jz_btree explicit_root_segments; jz_btree_init(&explicit_root_segments, &jazz_node_StrSet_desc);
    for (u32 i = 0; i < nincludes; i++) {
        const jz_str *p = &includes[i].path;
        u32 cut = 0; while (cut < p->len && p->ptr[cut] != '.') cut++;
        jz_str seg = jz_str_from_slice(jz_slice_of(p->ptr, cut));
        jz_btree_insert(&explicit_root_segments, &seg, NULL, NULL);
    }
    jz_str *column; jz_str *target_table;
    JZ_BTREE_FOR(&root_schema->references, column, target_table) {
        if (jz_btree_contains(&explicit_root_segments, column)) continue;
        SourceId target = NM(implicit_reference_source_id)(SL(*target_table), SL(*column));
        sourceset_insert(out_sources, &target);
        jazz_engine_ClosurePath path; __builtin_memset(&path, 0, sizeof path);
        path.tag = 0; /* ImplicitRootReference */
        path.id = jz_format("reference:%.*s", (int)column->len, column->ptr);
        jazz_engine_ClosurePathSegment segment = { sid_clone(&root_source), target, jz_str_clone(column) };
        jz_vec_push(&path.segments, &segment, sizeof segment);
        path.has_root_gate = false;
        jz_vec_push(out_paths, &path, sizeof path);
    }
    jz_btree_drop(&explicit_root_segments);
    for (u32 include_index = 0; include_index < nincludes; include_index++) {
        const jazz_model_Include *include = &includes[include_index];
        jz_str current_table_name = jz_str_clone(root_table);
        SourceId parent = sid_clone(&root_source);
        jazz_engine_ClosurePath path; __builtin_memset(&path, 0, sizeof path);
        path.tag = 1; /* ExplicitInclude */
        u32 start = 0, segment_index = 0;
        const jz_str *p = &include->path;
        while (start <= p->len) {
            u32 end = start; while (end < p->len && p->ptr[end] != '.') end++;
            jz_slice segment = jz_slice_of(p->ptr + start, end - start);
            const jazz_model_TableSchema *current_table;
            e = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(node, SL(current_table_name), schema_version, &current_table);
            if (JZ_IS_ERR(e)) { jz_str_drop(&current_table_name); jazz_engine_SourceId_drop(&parent); closure_path_drop(&path); goto fail; }
            jz_str seg_key = jz_str_from_slice(segment);
            const jz_str *target = jz_btree_get(&current_table->references, &seg_key);
            jz_str_drop(&seg_key);
            if (!target) {
                jz_str_drop(&current_table_name); jazz_engine_SourceId_drop(&parent); closure_path_drop(&path);
                e = JZN_INVALID_STORED("include path was not validated");
                goto fail;
            }
            jz_str target_table_name = jz_str_clone(target);
            SourceId target_source = NM(include_auxiliary_source_id)(jz_str_clone(&target_table_name), include_index, segment_index);
            sourceset_insert(out_sources, &target_source);
            jazz_engine_ClosurePathSegment s = { sid_clone(&parent), sid_clone(&target_source), jz_str_from_slice(segment) };
            jz_vec_push(&path.segments, &s, sizeof s);
            jazz_engine_SourceId_drop(&parent);
            parent = target_source;
            jz_str_drop(&current_table_name);
            current_table_name = target_table_name;
            segment_index++;
            start = end + 1;
        }
        jz_str_drop(&current_table_name);
        jazz_engine_SourceId_drop(&parent);
        path.id = jz_format("include:%u:%.*s", include_index, (int)p->len, p->ptr);
        if (include->require) { path.has_root_gate = true; path.root_gate = JAZZ_CRG_REQUIRED; }
        else if (include->join_mode == JAZZ_INCLUDE_INNER) { path.has_root_gate = true; path.root_gate = JAZZ_CRG_INNER; }
        else path.has_root_gate = false;
        jz_vec_push(out_paths, &path, sizeof path);
    }
    jazz_engine_SourceId_drop(&root_source);
    return JZ_OK;
fail:
    jazz_engine_SourceId_drop(&root_source);
    jz_btree_drop(out_sources);
    jz_vec_drop(out_paths, sizeof(jazz_engine_ClosurePath), closure_path_drop);
    return e;
}

VRef NM(source_column_value)(const SourceId *source, jz_slice column, jazz_model_JoinTarget target) {
    if (target == JAZZ_JOIN_TARGET_ROW_ID || jz_slice_cmp(column, JZ_LIT("id")) == 0) return vr_row_id(source);
    return vr_field(source, jz_str_from_slice(column));
}

static VRef jazz_node__node__query_eval__normalization__reachable_access_key(const SourceId *access_source, jz_slice column, jazz_model_JoinTarget target) {
    return NM(source_column_value)(access_source, column, target);
}

static jz_err jazz_node__node__query_eval__normalization__normalize_array_subquery(jz_btree *nodes, NodeId current, const RuntimeSchema *schema, const SourceId *owner_source,
    const jazz_model_ArraySubquery *subquery, const u32 *path, u32 npath, NodeId *out) {
    jz_str path_id = path_id_of(path, npath);
    int pl = (int)path_id.len; const char *pp = path_id.ptr;
    SourceId child_source = NM(correlated_child_source_id)(owner_source, subquery, path, npath);
    NodeId child_node = nid(jz_format("array_subquery:%.*s:source", pl, pp));
    nodes_insert(nodes, nid_clone(&child_node), rse_source(&child_source, JAZZ_VIS_VISIBLE));
    NodeId child_current = child_node;
    jz_err e = JZ_OK;

    if (subquery->filters.len != 0) {
        NodeId filter_node = nid(jz_format("array_subquery:%.*s:filter", pl, pp));
        PExpr predicate;
        e = jazz_node__node__query_eval__normalization__normalize_predicates(schema, &child_source, subquery->filters.ptr, subquery->filters.len, false, &predicate);
        if (JZ_IS_ERR(e)) { e = normalization_gap_from(e); node_id_drop(&filter_node); goto done; }
        nodes_insert(nodes, nid_clone(&filter_node), rse_filter(child_current, predicate));
        child_current = filter_node;
    }
    if (subquery->order_by.len != 0) {
        NodeId order_node = nid(jz_format("array_subquery:%.*s:order", pl, pp));
        OrderKeyVec keys;
        e = order_keys_for(&child_source, subquery->order_by.ptr, subquery->order_by.len, &keys);
        if (JZ_IS_ERR(e)) { e = normalization_gap_from(e); node_id_drop(&order_node); goto done; }
        RSE x = rse(JAZZ_RSE_ORDER_BY); x.input = child_current;
        x.order_keys.ptr = keys.ptr; x.order_keys.len = keys.len; x.order_keys.cap = keys.cap;
        nodes_insert(nodes, nid_clone(&order_node), x);
        child_current = order_node;
    }
    if (subquery->has_limit || subquery->offset != 0) {
        NodeId slice_node = nid(jz_format("array_subquery:%.*s:slice", pl, pp));
        RSE x = rse(JAZZ_RSE_SLICE); x.input = child_current;
        VRef part = NM(source_column_value)(&child_source, SL(subquery->inner_column), JAZZ_JOIN_TARGET_COLUMN);
        jz_vec_push(&x.keys, &part, sizeof part);
        x.has_limit = subquery->has_limit; x.limit = subquery->limit;
        x.offset = subquery->offset;
        VRef tie = vr_row_id(&child_source);
        jz_vec_push(&x.tie_breaker, &tie, sizeof tie);
        x.has_rank_output = false;
        nodes_insert(nodes, nid_clone(&slice_node), x);
        child_current = slice_node;
    }
    NodeId nested_parent_input = nid_clone(&child_current);
    NodeId path_node = nid(jz_format("array_subquery:%.*s:path", pl, pp));
    VRef right;
    e = normalize_column(owner_source, &subquery->outer_column, &right);
    if (JZ_IS_ERR(e)) { e = normalization_gap_from(e); node_id_drop(&path_node); node_id_drop(&nested_parent_input); goto done; }
    RSE x = rse(JAZZ_RSE_CORRELATED_PATH);
    x.input = current; current.s.ptr = NULL; current.s.len = current.s.cap = 0;
    x.right = child_current; child_current.s.ptr = NULL; child_current.s.len = child_current.s.cap = 0;
    x.path.owner = sid_clone(owner_source); x.path.child = sid_clone(&child_source);
    x.predicate = pe_compare(NM(source_column_value)(&child_source, SL(subquery->inner_column), JAZZ_JOIN_TARGET_COLUMN), JAZZ_CMP_EQ, right);
    x.requirement = jazz_node__node__query_eval__normalization__array_requirement(subquery->requirement);
    nodes_insert(nodes, nid_clone(&path_node), x);
    for (u32 nested_index = 0; nested_index < subquery->nested_arrays.len; nested_index++) {
        u32 *nested_path = jz_alloc(sizeof(u32) * (npath + 1));
        for (u32 i = 0; i < npath; i++) nested_path[i] = path[i];
        nested_path[npath] = nested_index;
        NodeId ignored;
        e = jazz_node__node__query_eval__normalization__normalize_array_subquery(nodes, nid_clone(&nested_parent_input), schema, &child_source,
            &subquery->nested_arrays.ptr[nested_index], nested_path, npath + 1, &ignored);
        jz_free(nested_path);
        if (JZ_IS_ERR(e)) break;
        node_id_drop(&ignored);
    }
    node_id_drop(&nested_parent_input);
    if (JZ_IS_ERR(e)) { node_id_drop(&path_node); goto done; }
    *out = path_node;
done:
    if (current.s.ptr) node_id_drop(&current);
    if (child_current.s.ptr && JZ_IS_ERR(e)) node_id_drop(&child_current);
    jazz_engine_SourceId_drop(&child_source);
    jz_str_drop(&path_id);
    return e;
}

static jz_err jazz_node__node__query_eval__normalization__normalize_reachable_seed(jz_btree *nodes, const RuntimeSchema *schema, const jazz_model_ReachableVia *reachable,
    const jz_str *reachable_id, const jz_str *binding_source_shape, const jz_btree *param_types, NodeId *out_node, ColumnVec *out_columns);
static jazz_engine_ValueRefVec jazz_node__node__query_eval__normalization__reachable_dedupe_keys(const jazz_engine_FrontierId *frontier, const ColumnVec *columns);

static jz_err jazz_node__node__query_eval__normalization__normalize_reachable(jz_btree *nodes, NodeId current, const RuntimeSchema *schema, const SourceId *root_source,
    const jazz_model_ReachableVia *reachable, u32 index, const char *prefix, const jz_str *binding_source_shape, const jz_btree *param_types,
    NodeId *out_node, jazz_engine_ReachableContribution *out_contribution) {
    jz_str reachable_id = prefix[0] == 0 ? jz_format("reachable:%u", index) : jz_format("%s:reachable:%u", prefix, index);
    int rl = (int)reachable_id.len; const char *rp = reachable_id.ptr;
    jazz_engine_FrontierId frontier = { jz_format("%.*s:frontier", rl, rp) };
    NodeId seed_node; ColumnVec columns;
    jz_err e = jazz_node__node__query_eval__normalization__normalize_reachable_seed(nodes, schema, reachable, &reachable_id, binding_source_shape, param_types, &seed_node, &columns);
    if (JZ_IS_ERR(e)) { node_id_drop(&current); jz_str_drop(&frontier.s); jz_str_drop(&reachable_id); return e; }
    NodeId frontier_node = nid(jz_format("%.*s:frontier", rl, rp));
    {
        RSE x = rse(JAZZ_RSE_FRONTIER_SOURCE); x.frontier.s = jz_str_clone(&frontier.s);
        ColumnVec c; columns_clone(&c, &columns);
        x.columns.ptr = c.ptr; x.columns.len = c.len; x.columns.cap = c.cap;
        nodes_insert(nodes, nid_clone(&frontier_node), x);
    }
    SourceId edge_source = NM(reachable_edge_source_id)(reachable, SL(reachable_id));
    NodeId edge_current = nid(jz_format("%.*s:edge_source", rl, rp));
    nodes_insert(nodes, nid_clone(&edge_current), rse_source(&edge_source, JAZZ_VIS_VISIBLE));
    if (reachable->edge_filters.len != 0) {
        NodeId edge_filter_node = nid(jz_format("%.*s:edge_filter", rl, rp));
        PExpr predicate;
        e = jazz_node__node__query_eval__normalization__normalize_predicates(schema, &edge_source, reachable->edge_filters.ptr, reachable->edge_filters.len, false, &predicate);
        if (JZ_IS_ERR(e)) goto fail_edge;
        nodes_insert(nodes, nid_clone(&edge_filter_node), rse_filter(edge_current, predicate));
        edge_current = edge_filter_node;
        if (0) { fail_edge: node_id_drop(&edge_filter_node); node_id_drop(&edge_current); goto fail; }
    }
    NodeId step_join_node = nid(jz_format("%.*s:step_join", rl, rp));
    nodes_insert(nodes, nid_clone(&step_join_node), rse_join(frontier_node, edge_current, JAZZ_JM_INNER,
        pe_compare(vr_frontier(&frontier, "reachable_team"), JAZZ_CMP_EQ,
                   NM(source_column_value)(&edge_source, SL(reachable->edge_member_column), JAZZ_JOIN_TARGET_COLUMN))));
    NodeId step_project_node = nid(jz_format("%.*s:step_project", rl, rp));
    {
        ProjVec step_columns = {0};
        projvec_push(&step_columns, projection(S("team"), ty_simple(GROOVE_VT_UUID), vr_frontier(&frontier, "team")));
        projvec_push(&step_columns, projection(S("reachable_team"), ty_simple(GROOVE_VT_UUID),
            NM(source_column_value)(&edge_source, SL(reachable->edge_parent_column), JAZZ_JOIN_TARGET_COLUMN)));
        for (u32 i = 0; i < columns.len; i++) {
            const jazz_engine_ValueSourceColumn *column = &columns.ptr[i];
            if (str_is(&column->name, "team") || str_is(&column->name, "reachable_team")) continue;
            VRef v = vr(JAZZ_NVR_FRONTIER_COLUMN); v.frontier.s = jz_str_clone(&frontier.s); v.field = jz_str_clone(&column->name);
            projvec_push(&step_columns, projection(jz_str_clone(&column->name), ty_clone(&column->ty), v));
        }
        RSE x = rse(JAZZ_RSE_PROJECT); x.input = step_join_node;
        x.projections.ptr = step_columns.ptr; x.projections.len = step_columns.len; x.projections.cap = step_columns.cap;
        nodes_insert(nodes, nid_clone(&step_project_node), x);
    }
    NodeId closure_node = nid(jz_format("%.*s:closure", rl, rp));
    {
        RSE x = rse(JAZZ_RSE_RECURSIVE);
        x.input = seed_node; x.step = step_project_node;
        x.frontier.s = jz_str_clone(&frontier.s);
        x.frontier_key = vr_frontier(&frontier, "reachable_team");
        x.keys = jazz_node__node__query_eval__normalization__reachable_dedupe_keys(&frontier, &columns);
        x.bound.tag = reachable->bound.tag; x.bound.max_depth = reachable->bound.max_depth;
        nodes_insert(nodes, nid_clone(&closure_node), x);
    }
    SourceId access_source = NM(reachable_access_source_id)(reachable, SL(reachable_id));
    NodeId access_current = nid(jz_format("%.*s:access_source", rl, rp));
    nodes_insert(nodes, nid_clone(&access_current), rse_source(&access_source, JAZZ_VIS_VISIBLE));
    if (reachable->access_filters.len != 0) {
        NodeId access_filter_node = nid(jz_format("%.*s:access_filter", rl, rp));
        PExpr predicate;
        e = jazz_node__node__query_eval__normalization__normalize_predicates(schema, &access_source, reachable->access_filters.ptr, reachable->access_filters.len, false, &predicate);
        if (JZ_IS_ERR(e)) {
            node_id_drop(&access_filter_node); node_id_drop(&access_current); node_id_drop(&closure_node);
            jazz_engine_SourceId_drop(&access_source); goto fail;
        }
        nodes_insert(nodes, nid_clone(&access_filter_node), rse_filter(access_current, predicate));
        access_current = access_filter_node;
    }
    NodeId access_join_node = nid(jz_format("%.*s:access_join", rl, rp));
    nodes_insert(nodes, nid_clone(&access_join_node), rse_join(access_current, closure_node, JAZZ_JM_INNER,
        pe_compare(jazz_node__node__query_eval__normalization__reachable_access_key(&access_source, SL(reachable->access_team_column), reachable->access_team_target),
                   JAZZ_CMP_EQ, vr_frontier(&frontier, "reachable_team"))));
    NodeId root_join_node = nid(jz_format("%.*s:root_join", rl, rp));
    nodes_insert(nodes, nid_clone(&root_join_node), rse_join(current, nid_clone(&access_join_node), JAZZ_JM_INNER,
        pe_compare(NM(source_column_value)(root_source, JZ_LIT("id"), JAZZ_JOIN_TARGET_COLUMN), JAZZ_CMP_EQ,
                   jazz_node__node__query_eval__normalization__reachable_access_key(&access_source, SL(reachable->access_row_column), JAZZ_JOIN_TARGET_COLUMN))));
    *out_node = root_join_node;
    out_contribution->id = reachable_id;
    out_contribution->access_source = access_source;
    out_contribution->edge_source = edge_source;
    out_contribution->access_input = access_join_node;
    out_contribution->root_ref_field = jz_str_clone(&reachable->access_row_column);
    columns_drop(&columns);
    jz_str_drop(&frontier.s);
    return JZ_OK;
fail:
    node_id_drop(&current);
    jazz_engine_SourceId_drop(&edge_source);
    columns_drop(&columns);
    jz_str_drop(&frontier.s);
    jz_str_drop(&reachable_id);
    return e;
}

static jz_err jazz_node__node__query_eval__normalization__normalize_join_via_right(NormCtx *cx, JoinContribVec *nested_contributions, const RuntimeSchema *schema,
    const jazz_model_JoinVia *join, const jz_str *path, NodeId *out_node, SourceId *out_source);
static VRef jazz_node__node__query_eval__normalization__join_via_target_key(const SourceId *join_source, const jazz_model_JoinVia *join);
static PExpr jazz_node__node__query_eval__normalization__join_via_predicate(const SourceId *left_source, const SourceId *right_source, const jazz_model_JoinVia *join);
static ProjVec jazz_node__node__query_eval__normalization__source_public_field_projections(const jazz_model_TableSchema *table, const SourceId *source);

static void join_contribution_drop(void *p) {
    jazz_engine_JoinContribution *c = p;
    if (c->has_parent) jazz_engine_SourceId_drop(&c->parent);
    jz_str_drop(&c->id); jazz_engine_SourceId_drop(&c->source); node_id_drop(&c->input); jazz_engine_PredicateExpr_drop(&c->membership);
}

static jz_err jazz_node__node__query_eval__normalization__normalize_join_via_right(NormCtx *cx, JoinContribVec *nested_contributions, const RuntimeSchema *schema,
    const jazz_model_JoinVia *join, const jz_str *path, NodeId *out_node, SourceId *out_source) {
    int pl = (int)path->len; const char *pp = path->ptr;
    SourceId join_source = NM(nested_join_source_id)(join, path);
    sourceset_insert(cx->auxiliary_sources, &join_source);
    const jazz_model_TableSchema *table;
    jz_err e = NM(table_schema)(schema, SL(join->table), &table);
    if (JZ_IS_ERR(e)) { jazz_engine_SourceId_drop(&join_source); return e; }
    NodeId current = nid(jz_format("%.*s:source", pl, pp));
    nodes_insert(cx->nodes, nid_clone(&current), rse_source(&join_source, JAZZ_VIS_VISIBLE));
    if (join->filters.len != 0) {
        NodeId filter_node = nid(jz_format("%.*s:filter", pl, pp));
        PExpr predicate;
        e = jazz_node__node__query_eval__normalization__normalize_predicates(schema, &join_source, join->filters.ptr, join->filters.len, false, &predicate);
        if (JZ_IS_ERR(e)) { node_id_drop(&filter_node); node_id_drop(&current); jazz_engine_SourceId_drop(&join_source); return e; }
        nodes_insert(cx->nodes, nid_clone(&filter_node), rse_filter(current, predicate));
        current = filter_node;
    }
    if (join->has_source_lookup) {
        const jazz_model_JoinSourceLookup *lookup = &join->source_lookup;
        SourceId lookup_source = jazz_node__node__query_eval__normalization__join_lookup_source_id(lookup, path);
        sourceset_insert(cx->auxiliary_sources, &lookup_source);
        NodeId lookup_source_node = nid(jz_format("%.*s:lookup_source", pl, pp));
        nodes_insert(cx->nodes, nid_clone(&lookup_source_node), rse_source(&lookup_source, JAZZ_VIS_VISIBLE));
        NodeId lookup_join_node = nid(jz_format("%.*s:lookup_join", pl, pp));
        nodes_insert(cx->nodes, nid_clone(&lookup_join_node), rse_join(current, lookup_source_node, JAZZ_JM_INNER,
            pe_compare(jazz_node__node__query_eval__normalization__join_via_target_key(&join_source, join), JAZZ_CMP_EQ,
                       NM(source_column_value)(&lookup_source, SL(lookup->value_column), JAZZ_JOIN_TARGET_COLUMN))));
        NodeId lookup_project_node = nid(jz_format("%.*s:lookup_project", pl, pp));
        ProjVec columns = jazz_node__node__query_eval__normalization__source_public_field_projections(table, &join_source);
        projvec_push(&columns, projection(jz_str_clone(&lookup->row_id_source_column), ty_simple(GROOVE_VT_UUID), vr_row_id(&lookup_source)));
        jazz_engine_SourceId_drop(&lookup_source);
        RSE x = rse(JAZZ_RSE_PROJECT); x.input = lookup_join_node;
        x.projections.ptr = columns.ptr; x.projections.len = columns.len; x.projections.cap = columns.cap;
        nodes_insert(cx->nodes, nid_clone(&lookup_project_node), x);
        current = lookup_project_node;
    }
    for (u32 nested_index = 0; nested_index < join->nested_joins.len; nested_index++) {
        const jazz_model_JoinVia *nested = &join->nested_joins.ptr[nested_index];
        jz_str nested_path = jz_format("%.*s:nested:%u", pl, pp, nested_index);
        JoinContribVec descendants = {0};
        NodeId nested_right; SourceId nested_source;
        e = jazz_node__node__query_eval__normalization__normalize_join_via_right(cx, &descendants, schema, nested, &nested_path, &nested_right, &nested_source);
        if (JZ_IS_ERR(e)) {
            jz_str_drop(&nested_path); jz_vec_drop(&descendants, sizeof(jazz_engine_JoinContribution), join_contribution_drop);
            node_id_drop(&current); jazz_engine_SourceId_drop(&join_source); return e;
        }
        /* Publish parents before descendants so each nested source can be
         * constrained to parent rows that actually contribute to visible roots. */
        jazz_engine_JoinContribution c; __builtin_memset(&c, 0, sizeof c);
        c.has_parent = true; c.parent = sid_clone(&join_source);
        c.id = jz_str_clone(&nested_path);
        c.source = sid_clone(&nested_source);
        c.input = nid_clone(&nested_right);
        c.membership = jazz_node__node__query_eval__normalization__join_via_predicate(&join_source, &nested_source, nested);
        jz_vec_push(nested_contributions, &c, sizeof c);
        jz_vec_extend(nested_contributions, descendants.ptr, descendants.len, sizeof(jazz_engine_JoinContribution));
        jz_free(descendants.ptr);
        NodeId nested_join_node = nid(jz_format("%.*s:join", (int)nested_path.len, nested_path.ptr));
        nodes_insert(cx->nodes, nid_clone(&nested_join_node), rse_join(current, nested_right, JAZZ_JM_INNER,
            jazz_node__node__query_eval__normalization__join_via_predicate(&join_source, &nested_source, nested)));
        NodeId project_node = nid(jz_format("%.*s:parent_project", (int)nested_path.len, nested_path.ptr));
        ProjVec cols = jazz_node__node__query_eval__normalization__source_public_field_projections(table, &join_source);
        RSE x = rse(JAZZ_RSE_PROJECT); x.input = nested_join_node;
        x.projections.ptr = cols.ptr; x.projections.len = cols.len; x.projections.cap = cols.cap;
        nodes_insert(cx->nodes, nid_clone(&project_node), x);
        current = project_node;
        jazz_engine_SourceId_drop(&nested_source);
        jz_str_drop(&nested_path);
    }
    *out_node = current;
    *out_source = join_source;
    return JZ_OK;
}

static jazz_engine_ValueRefVec jazz_node__node__query_eval__normalization__reachable_dedupe_keys(const jazz_engine_FrontierId *frontier, const ColumnVec *columns) {
    jazz_engine_ValueRefVec out = {0};
    VRef first = vr_frontier(frontier, "reachable_team");
    jz_vec_push(&out, &first, sizeof first);
    for (u32 i = 0; i < columns->len; i++) {
        const jz_str *name = &columns->ptr[i].name;
        if (str_is(name, "team") || str_is(name, "reachable_team")) continue;
        VRef v = vr(JAZZ_NVR_FRONTIER_COLUMN); v.frontier.s = jz_str_clone(&frontier->s); v.field = jz_str_clone(name);
        jz_vec_push(&out, &v, sizeof v);
    }
    return out;
}

static jz_err jazz_node__node__query_eval__normalization__reachable_edge_route_columns(const jazz_model_ReachableVia *reachable, const jz_btree *param_types, ColumnVec *out) {
    out->ptr = NULL; out->len = out->cap = 0;
    jz_btree params = jazz_node__node__query_eval__predicate_params(reachable->edge_filters.ptr, reachable->edge_filters.len);
    jz_str *param; void *u;
    JZ_BTREE_FOR(&params, param, u) {
        const groove_ColumnType *ty = jz_btree_get(param_types, param);
        if (!ty) {
            jz_err e = JZN_QUERY_LOWERING("unknown reachable edge parameter %.*s", (int)param->len, param->ptr);
            jz_btree_drop(&params); columns_drop(out);
            return e;
        }
        columns_push(out, jazz_engine__query_engine__fields__route_param_field(SL(*param)), vr_param(jz_str_clone(param)), ty_clone(ty));
    }
    jz_btree_drop(&params);
    return JZ_OK;
}

static jz_err seed_team_must_be_uuid(const jazz_model_ReachableSeed *seed, const groove_ColumnType *team_column_ty) {
    if (team_column_ty->tag == GROOVE_VT_UUID) return JZ_OK;
    jz_str dbg = {0}; groove_ValueType_debug(&dbg, team_column_ty);
    jz_err e = JZN_QUERY_LOWERING("reachable_via seed %.*s.%.*s must be uuid, found %.*s",
        (int)seed->table.len, seed->table.ptr, (int)seed->team_column.len, seed->team_column.ptr, (int)dbg.len, dbg.ptr);
    jz_str_drop(&dbg);
    return e;
}

static jz_err jazz_node__node__query_eval__normalization__reachable_seed_frontier_columns(const RuntimeSchema *schema, const SourceId *source, const jazz_model_ReachableSeed *seed, ColumnVec *out) {
    out->ptr = NULL; out->len = out->cap = 0;
    groove_ColumnType team_column_ty;
    JZ_TRY(NM(schema_column_type)(schema, &seed->table, &seed->team_column, &team_column_ty));
    jz_err e = seed_team_must_be_uuid(seed, &team_column_ty);
    groove_ValueType_drop(&team_column_ty);
    if (JZ_IS_ERR(e)) return e;
    VRef value = NM(source_column_value)(source, SL(seed->team_column), JAZZ_JOIN_TARGET_COLUMN);
    columns_push(out, S("team"), vr_clone(&value), ty_simple(GROOVE_VT_UUID));
    columns_push(out, S("reachable_team"), value, ty_simple(GROOVE_VT_UUID));
    if (seed->has_user_claim) {
        if (!seed->has_user_column) { columns_drop(out); return query_lowering_msg("reachable_via relation seed user_claim requires user_column"); }
        groove_ColumnType user_column_ty;
        e = NM(schema_column_type)(schema, &seed->table, &seed->user_column, &user_column_ty);
        if (JZ_IS_ERR(e)) { columns_drop(out); return e; }
        jazz_engine_ClaimPath path = claim_path_of(SL(seed->user_claim));
        columns_push(out, jazz_engine__query_engine__fields__claim_param_field(&path), vr_claim(path), user_column_ty);
    }
    return JZ_OK;
}

static jz_err reachable_seed_unsupported(void) { return query_lowering_msg("reachable_via currently supports uuid parameter/claim/literal seeds only"); }

static jz_err jazz_node__node__query_eval__normalization__literal_value_ref(const groove_Value *value, VRef *out) {
    VRef v = vr(JAZZ_NVR_LITERAL);
    JZ_TRY(literal_bytes(value, &v.literal));
    *out = v;
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__normalization__reachable_seed_value_ref(const jazz_model_Operand *seed, VRef *out) {
    switch (seed->tag) {
    case JAZZ_OPERAND_PARAM: *out = vr_param(jz_str_clone(&seed->name)); return JZ_OK;
    case JAZZ_OPERAND_CLAIM: *out = vr_claim(claim_path_of(SL(seed->name))); return JZ_OK;
    case JAZZ_OPERAND_LITERAL:
        if (seed->literal.tag == GROOVE_V_UUID) return jazz_node__node__query_eval__normalization__literal_value_ref(&seed->literal, out);
        return reachable_seed_unsupported();
    default: return reachable_seed_unsupported();
    }
}

static jz_err jazz_node__node__query_eval__normalization__reachable_seed_value_source_mode(const jazz_model_Operand *seed, jazz_engine_ValueSourceMode *out) {
    switch (seed->tag) {
    case JAZZ_OPERAND_PARAM: case JAZZ_OPERAND_CLAIM: *out = JAZZ_VSM_BINDING; return JZ_OK;
    case JAZZ_OPERAND_LITERAL:
        if (seed->literal.tag == GROOVE_V_UUID) { *out = JAZZ_VSM_INLINE; return JZ_OK; }
        return reachable_seed_unsupported();
    default: return reachable_seed_unsupported();
    }
}

static groove_ColumnType param_type_or_uuid(const jz_btree *param_types, const jz_str *param) {
    const groove_ColumnType *t = jz_btree_get(param_types, param);
    return t ? ty_clone(t) : ty_simple(GROOVE_VT_UUID);
}

static jz_err jazz_node__node__query_eval__normalization__reachable_frontier_columns(const jazz_model_Operand *seed, const jz_btree *param_types, ColumnVec *out) {
    out->ptr = NULL; out->len = out->cap = 0;
    VRef value;
    JZ_TRY(jazz_node__node__query_eval__normalization__reachable_seed_value_ref(seed, &value));
    groove_ColumnType ty;
    switch (seed->tag) {
    case JAZZ_OPERAND_PARAM: ty = param_type_or_uuid(param_types, &seed->name); break;
    case JAZZ_OPERAND_LITERAL:
        if (seed->literal.tag == GROOVE_V_UUID) { ty = ty_simple(GROOVE_VT_UUID); break; }
        jazz_engine_NormalizedValueRef_drop(&value);
        return reachable_seed_unsupported();
    case JAZZ_OPERAND_CLAIM: ty = ty_simple(GROOVE_VT_UUID); break;
    default: jazz_engine_NormalizedValueRef_drop(&value); return reachable_seed_unsupported();
    }
    columns_push(out, S("team"), vr_clone(&value), ty_clone(&ty));
    columns_push(out, S("reachable_team"), value, ty);
    if (seed->tag == JAZZ_OPERAND_PARAM)
        columns_push(out, jazz_engine__query_engine__fields__route_param_field(SL(seed->name)), vr_param(jz_str_clone(&seed->name)), param_type_or_uuid(param_types, &seed->name));
    if (seed->tag == JAZZ_OPERAND_CLAIM) {
        jazz_engine_ClaimPath path = claim_path_of(SL(seed->name));
        columns_push(out, jazz_engine__query_engine__fields__claim_param_field(&path), vr_claim(path), ty_simple(GROOVE_VT_UUID));
    }
    if (seed->tag == JAZZ_OPERAND_PARAM && !str_is(&seed->name, "team") && !str_is(&seed->name, "reachable_team"))
        columns_push(out, jz_str_clone(&seed->name), vr_param(jz_str_clone(&seed->name)), param_type_or_uuid(param_types, &seed->name));
    return JZ_OK;
}

static bool column_named(const ColumnVec *columns, const jz_str *name) {
    for (u32 i = 0; i < columns->len; i++) if (jz_str_eq(&columns->ptr[i].name, name)) return true;
    return false;
}

static jz_err jazz_node__node__query_eval__normalization__normalize_reachable_seed(jz_btree *nodes, const RuntimeSchema *schema, const jazz_model_ReachableVia *reachable,
    const jz_str *reachable_id, const jz_str *binding_source_shape, const jz_btree *param_types, NodeId *out_node, ColumnVec *out_columns) {
    int rl = (int)reachable_id->len; const char *rp = reachable_id->ptr;
    jz_err e;
    if (reachable->has_seed) {
        const jazz_model_ReachableSeed *seed = &reachable->seed;
        SourceId seed_source = NM(reachable_seed_source_id)(seed, jz_str_slice(reachable_id));
        ColumnVec columns, edge_route_columns;
        e = jazz_node__node__query_eval__normalization__reachable_seed_frontier_columns(schema, &seed_source, seed, &columns);
        if (JZ_IS_ERR(e)) { jazz_engine_SourceId_drop(&seed_source); return e; }
        e = jazz_node__node__query_eval__normalization__reachable_edge_route_columns(reachable, param_types, &edge_route_columns);
        if (JZ_IS_ERR(e)) { columns_drop(&columns); jazz_engine_SourceId_drop(&seed_source); return e; }
        for (u32 i = 0; i < edge_route_columns.len; i++)
            if (!column_named(&columns, &edge_route_columns.ptr[i].name)) {
                jazz_engine_ValueSourceColumn c; value_source_column_clone(&c, &edge_route_columns.ptr[i]);
                jz_vec_push(&columns, &c, sizeof c);
            }
        bool has_user_column_ty = false; groove_ColumnType user_column_ty;
        if (seed->has_user_column) {
            e = NM(schema_column_type)(schema, &seed->table, &seed->user_column, &user_column_ty);
            if (JZ_IS_ERR(e)) goto seed_fail;
            has_user_column_ty = true;
        }
        groove_ColumnType team_column_ty;
        e = NM(schema_column_type)(schema, &seed->table, &seed->team_column, &team_column_ty);
        if (!JZ_IS_ERR(e)) { e = seed_team_must_be_uuid(seed, &team_column_ty); groove_ValueType_drop(&team_column_ty); }
        if (JZ_IS_ERR(e)) { if (has_user_column_ty) groove_ValueType_drop(&user_column_ty); goto seed_fail; }
        NodeId seed_current = nid(jz_format("%.*s:seed_source", rl, rp));
        nodes_insert(nodes, nid_clone(&seed_current), rse_source(&seed_source, JAZZ_VIS_VISIBLE));
        bool has_claim_route = false; jz_str claim_field = {0};
        if (seed->has_user_claim) {
            jazz_engine_ClaimPath claim_path = claim_path_of(SL(seed->user_claim));
            claim_field = jazz_engine__query_engine__fields__claim_param_field(&claim_path);
            jazz_engine_ClaimPath_drop(&claim_path);
            has_claim_route = true;
        }
        if (seed->has_user_column && has_claim_route) {
            NodeId seed_claim_filter_node = nid(jz_format("%.*s:seed_claim_filter", rl, rp));
            nodes_insert(nodes, nid_clone(&seed_claim_filter_node), rse_filter(seed_current,
                pe_compare(NM(source_column_value)(&seed_source, SL(seed->user_column), JAZZ_JOIN_TARGET_COLUMN), JAZZ_CMP_EQ, vr_param(jz_str_clone(&claim_field)))));
            seed_current = seed_claim_filter_node;
        }
        if (seed->filters.len != 0) {
            NodeId seed_filter_node = nid(jz_format("%.*s:seed_filter", rl, rp));
            PExpr predicate;
            e = jazz_node__node__query_eval__normalization__normalize_predicates(schema, &seed_source, seed->filters.ptr, seed->filters.len, false, &predicate);
            if (JZ_IS_ERR(e)) {
                node_id_drop(&seed_filter_node); node_id_drop(&seed_current);
                if (has_claim_route) jz_str_drop(&claim_field);
                if (has_user_column_ty) groove_ValueType_drop(&user_column_ty);
                goto seed_fail;
            }
            nodes_insert(nodes, nid_clone(&seed_filter_node), rse_filter(seed_current, predicate));
            seed_current = seed_filter_node;
        }
        NodeId seed_project_node = nid(jz_format("%.*s:seed_project", rl, rp));
        VRef seed_team_value = NM(source_column_value)(&seed_source, SL(seed->team_column), JAZZ_JOIN_TARGET_COLUMN);
        ProjVec seed_columns = {0};
        projvec_push(&seed_columns, projection(S("team"), ty_simple(GROOVE_VT_UUID), vr_clone(&seed_team_value)));
        projvec_push(&seed_columns, projection(S("reachable_team"), ty_simple(GROOVE_VT_UUID), seed_team_value));
        if (has_claim_route) {
            groove_ColumnType t = has_user_column_ty ? ty_clone(&user_column_ty) : ty_simple(GROOVE_VT_UUID);
            projvec_push(&seed_columns, projection(jz_str_clone(&claim_field), t, vr_param(jz_str_clone(&claim_field))));
            jz_str_drop(&claim_field);
        }
        for (u32 i = 0; i < edge_route_columns.len; i++) {
            jazz_engine_ValueSourceColumn *c = &edge_route_columns.ptr[i];
            projvec_push(&seed_columns, projection(jz_str_clone(&c->name), c->ty, c->value));
            jz_str_drop(&c->name);
        }
        jz_free(edge_route_columns.ptr);
        if (has_user_column_ty) groove_ValueType_drop(&user_column_ty);
        RSE x = rse(JAZZ_RSE_PROJECT); x.input = seed_current;
        x.projections.ptr = seed_columns.ptr; x.projections.len = seed_columns.len; x.projections.cap = seed_columns.cap;
        nodes_insert(nodes, nid_clone(&seed_project_node), x);
        jazz_engine_SourceId_drop(&seed_source);
        *out_node = seed_project_node;
        *out_columns = columns;
        return JZ_OK;
    seed_fail:
        columns_drop(&columns); columns_drop(&edge_route_columns); jazz_engine_SourceId_drop(&seed_source);
        return e;
    }
    ColumnVec columns, edge;
    JZ_TRY(jazz_node__node__query_eval__normalization__reachable_frontier_columns(&reachable->from, param_types, &columns));
    e = jazz_node__node__query_eval__normalization__reachable_edge_route_columns(reachable, param_types, &edge);
    if (JZ_IS_ERR(e)) { columns_drop(&columns); return e; }
    for (u32 i = 0; i < edge.len; i++) {
        if (!column_named(&columns, &edge.ptr[i].name)) { jz_vec_push(&columns, &edge.ptr[i], sizeof edge.ptr[i]); }
        else value_source_column_drop(&edge.ptr[i]);
    }
    jz_free(edge.ptr);
    jazz_engine_ValueSourceMode mode;
    e = jazz_node__node__query_eval__normalization__reachable_seed_value_source_mode(&reachable->from, &mode);
    if (JZ_IS_ERR(e)) { columns_drop(&columns); return e; }
    NodeId seed_node = nid(jz_format("%.*s:seed", rl, rp));
    RSE x = rse(JAZZ_RSE_VALUE_SOURCE);
    x.shape = jz_str_clone(binding_source_shape);
    ColumnVec c; columns_clone(&c, &columns);
    x.columns.ptr = c.ptr; x.columns.len = c.len; x.columns.cap = c.cap;
    x.mode = mode;
    nodes_insert(nodes, nid_clone(&seed_node), x);
    *out_node = seed_node;
    *out_columns = columns;
    return JZ_OK;
}

jz_err NM(schema_column_type)(const RuntimeSchema *schema, const jz_str *table, const jz_str *column, groove_ColumnType *out) {
    const jazz_model_TableSchema *schema_table;
    JZ_TRY(NM(table_schema)(schema, SL(*table), &schema_table));
    if (str_is(column, "id")) { *out = ty_simple(GROOVE_VT_UUID); return JZ_OK; }
    for (u32 i = 0; i < schema_table->columns.len; i++)
        if (jz_str_eq(&schema_table->columns.ptr[i].name, column)) { *out = ty_clone(&schema_table->columns.ptr[i].column_type); return JZ_OK; }
    return JZN_QUERY_LOWERING("unknown query column %.*s.%.*s", (int)table->len, table->ptr, (int)column->len, column->ptr);
}

static jazz_engine_TypedOutputField jazz_node__node__query_eval__normalization__row_id_output_field(void) { return typed_output_field(S("id"), ty_simple(GROOVE_VT_UUID)); }

static ProjVec jazz_node__node__query_eval__normalization__source_public_field_projections(const jazz_model_TableSchema *table, const SourceId *source) {
    ProjVec out = {0};
    jazz_engine_RowProjection id; id.output = jazz_node__node__query_eval__normalization__row_id_output_field(); id.value = vr_row_id(source);
    projvec_push(&out, id);
    for (u32 i = 0; i < table->columns.len; i++) {
        const jazz_model_ColumnSchema *column = &table->columns.ptr[i];
        projvec_push(&out, projection(jz_str_clone(&column->name), ty_clone(&column->column_type), vr_field(source, jz_str_clone(&column->name))));
    }
    return out;
}

static VRef jazz_node__node__query_eval__normalization__join_via_root_key(const SourceId *root_source, const jazz_model_JoinVia *join) {
    if (join->has_source_column) {
        if (str_is(&join->source_column, "id")) return vr_row_id(root_source);
        return vr_field(root_source, jz_str_clone(&join->source_column));
    }
    return vr_row_id(root_source);
}

static void projvec_drop(ProjVec *v) {
    for (u32 i = 0; i < v->len; i++) {
        jz_str_drop(&v->ptr[i].output.name); groove_ValueType_drop(&v->ptr[i].output.ty); jazz_engine_NormalizedValueRef_drop(&v->ptr[i].value);
    }
    jz_free(v->ptr); v->ptr = NULL; v->len = v->cap = 0;
}

static jz_err jazz_node__node__query_eval__normalization__relation_row_projection(const RuntimeSchema *schema, const jazz_model_Query *query, const SourceId *root_source, ProjVec *out) {
    out->ptr = NULL; out->len = out->cap = 0;
    if (!query->relation) return query_lowering_msg("relation projection is missing its relation tree");
    jz_str output_scope; jazz_model_RelationProjectColumnVec columns;
    jz_err e = jazz_model__query__relation_output_projection(query->relation, &output_scope, &columns);
    if (JZ_IS_ERR(e)) return capability_from(e);
    projvec_push(out, projection(S("row_uuid"), ty_simple(GROOVE_VT_UUID), vr_row_id(root_source)));
    for (u32 i = 0; i < columns.len; i++) {
        const jazz_model_RelationProjectColumn *column = &columns.ptr[i];
        VRef value; groove_ColumnType ty;
        if (column->expr_tag == JAZZ_RPE_ROW_ID && column->row_id == JAZZ_RRID_CURRENT) {
            value = vr_row_id(root_source); ty = ty_simple(GROOVE_VT_UUID);
        } else if (column->expr_tag == JAZZ_RPE_COLUMN) {
            jz_str scope;
            e = jazz_model__query__relation_scope(&column->column, &scope);
            if (JZ_IS_ERR(e)) { e = capability_from(e); goto fail; }
            bool same = jz_str_eq(&scope, &output_scope);
            jz_str_drop(&scope);
            if (!same) { e = query_capability_msg("relation projection must select from its output scope"); goto fail; }
            e = NM(schema_column_type)(schema, &query->table, &column->column.column, &ty);
            if (JZ_IS_ERR(e)) goto fail;
            value = NM(source_column_value)(root_source, SL(column->column.column), JAZZ_JOIN_TARGET_COLUMN);
        } else {
            e = query_capability_msg("outer/frontier row-id relation projections are not unified yet");
            goto fail;
        }
        projvec_push(out, projection(jz_str_clone(&column->alias), ty, value));
    }
    jz_str_drop(&output_scope);
    jazz_model_RelationProjectColumnVec_drop(&columns);
    return JZ_OK;
fail:
    jz_str_drop(&output_scope);
    jazz_model_RelationProjectColumnVec_drop(&columns);
    projvec_drop(out);
    return e;
}

static VRef jazz_node__node__query_eval__normalization__join_via_target_key(const SourceId *join_source, const jazz_model_JoinVia *join) {
    return NM(source_column_value)(join_source, SL(join->on_column), join->target);
}

static PExpr jazz_node__node__query_eval__normalization__join_via_predicate(const SourceId *left_source, const SourceId *right_source, const jazz_model_JoinVia *join) {
    if (join->target == JAZZ_JOIN_TARGET_UNCORRELATED) return pe(JAZZ_NP_TRUE);
    jazz_engine_PredicateVec key_pairs = {0};
    PExpr first;
    if (join->has_source_lookup) {
        first = pe_compare(vr_field(left_source, jz_str_clone(&join->source_lookup.row_id_source_column)), JAZZ_CMP_EQ,
                           vr_field(right_source, jz_str_clone(&join->source_lookup.row_id_source_column)));
    } else {
        first = pe_compare(jazz_node__node__query_eval__normalization__join_via_root_key(left_source, join), JAZZ_CMP_EQ,
                           jazz_node__node__query_eval__normalization__join_via_target_key(right_source, join));
    }
    jz_vec_push(&key_pairs, &first, sizeof first);
    for (u32 i = 0; i < join->correlated_filters.len; i++) {
        const jazz_model_JoinCorrelation *correlation = &join->correlated_filters.ptr[i];
        PExpr p = pe_compare(NM(source_column_value)(left_source, SL(correlation->source_column), JAZZ_JOIN_TARGET_COLUMN), JAZZ_CMP_EQ,
                             NM(source_column_value)(right_source, SL(correlation->join_column), JAZZ_JOIN_TARGET_COLUMN));
        jz_vec_push(&key_pairs, &p, sizeof p);
    }
    if (key_pairs.len == 1) { PExpr only = key_pairs.ptr[0]; jz_free(key_pairs.ptr); return only; }
    PExpr and = pe(JAZZ_NP_AND); and.children = key_pairs;
    return and;
}

SourceId NM(reachable_edge_source_id)(const jazz_model_ReachableVia *reachable, jz_slice reachable_id) {
    return sid2(jz_str_clone(&reachable->edge_table), role(JAZZ_SR_ROOT, empty_str()),
        role(JAZZ_SR_RECURSIVE_STEP, jz_format("%.*s:%.*s", (int)reachable_id.len, (const char *)reachable_id.ptr, (int)reachable->edge_table.len, reachable->edge_table.ptr)));
}

SourceId NM(reachable_access_source_id)(const jazz_model_ReachableVia *reachable, jz_slice reachable_id) {
    return sid1(jz_str_clone(&reachable->access_table),
        role(JAZZ_SR_ALIAS, jz_format("%.*s:%.*s", (int)reachable_id.len, (const char *)reachable_id.ptr, (int)reachable->access_table.len, reachable->access_table.ptr)));
}

SourceId NM(reachable_seed_source_id)(const jazz_model_ReachableSeed *seed, jz_slice reachable_id) {
    return sid2(jz_str_clone(&seed->table), role(JAZZ_SR_ROOT, empty_str()),
        role(JAZZ_SR_RECURSIVE_SEED, jz_format("%.*s:%.*s", (int)reachable_id.len, (const char *)reachable_id.ptr, (int)seed->table.len, seed->table.ptr)));
}

static SourceId jazz_node__node__query_eval__normalization__inherited_parent_source_id(const jz_str *table, const jz_str *prefix) {
    return sid1(jz_str_clone(table), role(JAZZ_SR_ALIAS, jz_str_clone(prefix)));
}

/* ---------- policy chains ---------- */
typedef struct FilterJoinChain { const jazz_model_Predicate *filters; u32 nfilters; const jazz_model_JoinVia *joins; u32 njoins; } FilterJoinChain;
typedef struct PolicyAtomChain {
    const jazz_model_Predicate *filters; u32 nfilters; const jazz_model_JoinVia *joins; u32 njoins;
    const jazz_model_InheritsVia *inherits; u32 ninherits; const jazz_model_ReachableVia *reachable; u32 nreachable;
} PolicyAtomChain;
static PolicyAtomChain chain_of(const jazz_model_PredicateVec *f, const jazz_model_JoinViaVec *j, const jazz_model_InheritsVia *i, u32 ni, const jazz_model_ReachableVia *r, u32 nr) {
    PolicyAtomChain c = { f->ptr, f->len, j->ptr, j->len, i, ni, r, nr }; return c;
}

/* The inheritance atoms expanded on the current policy-composition path. A
 * policy can refer back to its own table through an `InheritsVia`; the
 * normalized graph is finite only when that expansion is bounded. */
typedef struct InheritanceExpansionKey { jz_str child_table; jz_str parent_column; jazz_model_InheritsOperation operation; } InheritanceExpansionKey;
static int inheritance_key_cmp(const void *a, const void *b, void *ctx) {
    const InheritanceExpansionKey *x = a, *y = b;
    int c = jz_str_cmp(&x->child_table, &y->child_table); if (c) return c;
    c = jz_str_cmp(&x->parent_column, &y->parent_column); if (c) return c;
    return (int)x->operation - (int)y->operation;
}
static void inheritance_key_drop(void *p) { InheritanceExpansionKey *k = p; jz_str_drop(&k->child_table); jz_str_drop(&k->parent_column); }
static void inheritance_key_clone(void *d, const void *s) {
    InheritanceExpansionKey *dk = d; const InheritanceExpansionKey *sk = s;
    dk->child_table = jz_str_clone(&sk->child_table); dk->parent_column = jz_str_clone(&sk->parent_column); dk->operation = sk->operation;
}
static void u32_clone(void *d, const void *s) { *(u32 *)d = *(const u32 *)s; }
static const jz_btree_desc inheritance_uses_desc = { sizeof(InheritanceExpansionKey), sizeof(u32), inheritance_key_cmp, inheritance_key_drop, NULL };
typedef struct InheritanceExpansionPath { jz_btree uses; } InheritanceExpansionPath;
static InheritanceExpansionPath inheritance_path_default(void) { InheritanceExpansionPath p; jz_btree_init(&p.uses, &inheritance_uses_desc); return p; }

#define JAZZ_RECURSION_DEFAULT_MAX_DEPTH 8u /* RecursionBound::default_max_depth().depth_steps() */
static bool jazz_node__node__query_eval__normalization__InheritanceExpansionPath__descend(const InheritanceExpansionPath *self, const jz_str *child_table,
    const jazz_model_InheritsVia *inherits, InheritanceExpansionPath *out) {
    InheritanceExpansionKey key = { jz_str_clone(child_table), jz_str_clone(&inherits->parent_column), inherits->operation };
    const u32 *u = jz_btree_get(&self->uses, &key);
    u32 used = u ? *u : 0;
    u32 limit = inherits->has_max_depth ? inherits->max_depth : JAZZ_RECURSION_DEFAULT_MAX_DEPTH;
    if (used >= limit) { inheritance_key_drop(&key); return false; }
    jz_btree_clone(&out->uses, &self->uses, inheritance_key_clone, u32_clone);
    u32 next = used + 1;
    jz_btree_insert(&out->uses, &key, &next, NULL);
    return true;
}

static NodeId jazz_node__node__query_eval__normalization__normalize_false_policy_branch(jz_btree *nodes, NodeId input, const jz_str *prefix) {
    NodeId node = nid(jz_format("%.*s:max_depth", (int)prefix->len, prefix->ptr));
    nodes_insert(nodes, nid_clone(&node), rse_filter(input, pe(JAZZ_NP_OR)));
    return node;
}

static jz_err jazz_node__node__query_eval__normalization__normalize_filter_join_chain(NormCtx *cx, const RuntimeSchema *schema, const SourceId *root_source, NodeId start,
    const jz_str *prefix, FilterJoinChain chain, bool record_join_contributions, NodeId *out) {
    NodeId current = start;
    int pl = (int)prefix->len; const char *pp = prefix->ptr;
    if (chain.nfilters != 0) {
        NodeId filter_node = nid(jz_format("%.*s:filter", pl, pp));
        PExpr predicate;
        jz_err e = jazz_node__node__query_eval__normalization__normalize_predicates(schema, root_source, chain.filters, chain.nfilters, false, &predicate);
        if (JZ_IS_ERR(e)) { node_id_drop(&filter_node); node_id_drop(&current); return e; }
        nodes_insert(cx->nodes, nid_clone(&filter_node), rse_filter(current, predicate));
        current = filter_node;
    }
    for (u32 index = 0; index < chain.njoins; index++) {
        const jazz_model_JoinVia *join = &chain.joins[index];
        jz_str path = str_is(prefix, "query") ? jz_format("join_via:%u", index) : jz_format("%.*s:join_via:%u", pl, pp, index);
        JoinContribVec nested_contributions = {0};
        NodeId right; SourceId join_source;
        jz_err e = jazz_node__node__query_eval__normalization__normalize_join_via_right(cx, &nested_contributions, schema, join, &path, &right, &join_source);
        if (JZ_IS_ERR(e)) { jz_str_drop(&path); node_id_drop(&current); return e; }
        PExpr join_predicate = jazz_node__node__query_eval__normalization__join_via_predicate(root_source, &join_source, join);
        if (record_join_contributions) {
            jazz_engine_JoinContribution c; __builtin_memset(&c, 0, sizeof c);
            c.has_parent = false; c.id = jz_str_clone(&path); c.source = sid_clone(&join_source); c.input = nid_clone(&right); c.membership = pe_clone(&join_predicate);
            jz_vec_push(cx->join_contributions, &c, sizeof c);
            jz_vec_extend(cx->join_contributions, nested_contributions.ptr, nested_contributions.len, sizeof(jazz_engine_JoinContribution));
            jz_free(nested_contributions.ptr);
        } else {
            jz_vec_drop(&nested_contributions, sizeof(jazz_engine_JoinContribution), join_contribution_drop);
        }
        NodeId join_node = nid(jz_format("%.*s:join", (int)path.len, path.ptr));
        nodes_insert(cx->nodes, nid_clone(&join_node), rse_join(current, right,
            join->target == JAZZ_JOIN_TARGET_UNCORRELATED ? JAZZ_JM_SEMI : JAZZ_JM_INNER, join_predicate));
        current = join_node;
        jazz_engine_SourceId_drop(&join_source);
        jz_str_drop(&path);
    }
    *out = current;
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__normalization__normalize_inherited_parent_policy(NormCtx *cx, const RuntimeSchema *schema, const SourceId *child_source, NodeId child_current,
    const jazz_model_InheritsVia *inherits, const jz_str *prefix, const jz_str *binding_source_shape, const jz_btree *param_types, bool record_inherited_contributions,
    const InheritanceExpansionPath *inheritance_path, NodeId *out);

static jz_err jazz_node__node__query_eval__normalization__normalize_policy_atom_chain(NormCtx *cx, const RuntimeSchema *schema, const SourceId *root_source, NodeId start,
    const jz_str *prefix, PolicyAtomChain chain, const jz_str *binding_source_shape, const jz_btree *param_types, bool record_join_contributions,
    bool record_inherited_contributions, const InheritanceExpansionPath *inheritance_path, NodeId *out) {
    NodeId current;
    FilterJoinChain fj = { chain.filters, chain.nfilters, chain.joins, chain.njoins };
    JZ_TRY(jazz_node__node__query_eval__normalization__normalize_filter_join_chain(cx, schema, root_source, start, prefix, fj, record_join_contributions, &current));
    for (u32 index = 0; index < chain.ninherits; index++) {
        jz_str p = jz_format("%.*s:inherits:%u", (int)prefix->len, prefix->ptr, index);
        NodeId next;
        jz_err e = jazz_node__node__query_eval__normalization__normalize_inherited_parent_policy(cx, schema, root_source, current, &chain.inherits[index], &p,
            binding_source_shape, param_types, record_inherited_contributions, inheritance_path, &next);
        jz_str_drop(&p);
        if (JZ_IS_ERR(e)) return e;
        current = next;
    }
    for (u32 index = 0; index < chain.nreachable; index++) {
        jz_str reachable_prefix = str_is(prefix, "query") ? empty_str() : jz_str_clone(prefix);
        jz_str_push_char(&reachable_prefix, 0); /* NUL-terminate for the "%s" formatting below */
        NodeId next; jazz_engine_ReachableContribution contribution;
        jz_err e = jazz_node__node__query_eval__normalization__normalize_reachable(cx->nodes, current, schema, root_source, &chain.reachable[index], index,
            reachable_prefix.ptr, binding_source_shape, param_types, &next, &contribution);
        jz_str_drop(&reachable_prefix);
        if (JZ_IS_ERR(e)) return e;
        current = next;
        jz_vec_push(cx->reachable_contributions, &contribution, sizeof contribution);
    }
    *out = current;
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__normalization__normalize_policy_branch_authorization(NormCtx *cx, const RuntimeSchema *schema, const SourceId *root_source,
    NodeId current, const jz_str *prefix, const jazz_model_Query *policy, const jz_str *binding_source_shape, const jz_btree *param_types,
    const InheritanceExpansionPath *inheritance_path, NodeId *out);

static jz_err jazz_node__node__query_eval__normalization__normalize_inherited_parent_policy(NormCtx *cx, const RuntimeSchema *schema, const SourceId *child_source, NodeId child_current,
    const jazz_model_InheritsVia *inherits, const jz_str *prefix, const jz_str *binding_source_shape, const jz_btree *param_types, bool record_inherited_contributions,
    const InheritanceExpansionPath *inheritance_path, NodeId *out) {
    const jazz_model_TableSchema *child_table;
    jz_err e = NM(table_schema)(schema, SL(child_source->table), &child_table);
    if (JZ_IS_ERR(e)) { node_id_drop(&child_current); return e; }
    const jz_str *parent_name = jz_btree_get(&child_table->references, &inherits->parent_column);
    if (!parent_name) {
        node_id_drop(&child_current);
        return JZN_QUERY_LOWERING("%.*s.%.*s is not a parent reference", (int)child_source->table.len, child_source->table.ptr,
            (int)inherits->parent_column.len, inherits->parent_column.ptr);
    }
    jz_str parent_table_name = jz_str_clone(parent_name);
    const jazz_model_TableSchema *parent_table;
    e = NM(table_schema)(schema, SL(parent_table_name), &parent_table);
    if (JZ_IS_ERR(e)) { jz_str_drop(&parent_table_name); node_id_drop(&child_current); return e; }
    InheritanceExpansionPath parent_inheritance_path;
    if (!jazz_node__node__query_eval__normalization__InheritanceExpansionPath__descend(inheritance_path, &child_source->table, inherits, &parent_inheritance_path)) {
        jz_str_drop(&parent_table_name);
        *out = jazz_node__node__query_eval__normalization__normalize_false_policy_branch(cx->nodes, child_current, prefix);
        return JZ_OK;
    }
    SourceId parent_source = jazz_node__node__query_eval__normalization__inherited_parent_source_id(&parent_table_name, prefix);
    jz_str_drop(&parent_table_name);
    sourceset_insert(cx->auxiliary_sources, &parent_source);
    NodeId parent_current = nid(jz_format("%.*s:source", (int)prefix->len, prefix->ptr));
    nodes_insert(cx->nodes, nid_clone(&parent_current), rse_source(&parent_source, JAZZ_VIS_VISIBLE));
    const jazz_model_Query *parent_policy;
    switch (inherits->operation) {
    case JAZZ_INHERITS_SELECT: parent_policy = parent_table->read_policy; break;
    case JAZZ_INHERITS_INSERT: parent_policy = parent_table->write_policies.insert_check; break;
    case JAZZ_INHERITS_UPDATE: parent_policy = parent_table->write_policies.update_using; break;
    default: parent_policy = parent_table->write_policies.delete_using; break;
    }
    if (parent_policy) {
        jz_str p = jz_format("%.*s:parent_policy", (int)prefix->len, prefix->ptr);
        NodeId next;
        if (parent_policy->policy_branches.len != 0) {
            e = jazz_node__node__query_eval__normalization__normalize_policy_branch_authorization(cx, schema, &parent_source, parent_current, &p, parent_policy,
                binding_source_shape, param_types, &parent_inheritance_path, &next);
        } else {
            e = jazz_node__node__query_eval__normalization__normalize_policy_atom_chain(cx, schema, &parent_source, parent_current, &p,
                chain_of(&parent_policy->filters, &parent_policy->joins, parent_policy->inherits.ptr, parent_policy->inherits.len,
                         parent_policy->reachable.ptr, parent_policy->reachable.len),
                binding_source_shape, param_types, false, false, &parent_inheritance_path, &next);
        }
        jz_str_drop(&p);
        if (JZ_IS_ERR(e)) { jz_btree_drop(&parent_inheritance_path.uses); jazz_engine_SourceId_drop(&parent_source); node_id_drop(&child_current); return e; }
        parent_current = next;
    } else if (inherits->operation != JAZZ_INHERITS_UPDATE || !parent_table->write_policies.update_check) {
        /* Inheritance needs an explicit parent operation. A CHECK-only UPDATE
         * still declares UPDATE and has no old-row predicate to prove. */
        node_id_drop(&parent_current);
        jz_btree_drop(&parent_inheritance_path.uses);
        jazz_engine_SourceId_drop(&parent_source);
        *out = jazz_node__node__query_eval__normalization__normalize_false_policy_branch(cx->nodes, child_current, prefix);
        return JZ_OK;
    }
    jz_btree_drop(&parent_inheritance_path.uses);
    NodeId join_node = nid(jz_format("%.*s:join", (int)prefix->len, prefix->ptr));
    PExpr membership = pe_compare(vr_field(child_source, jz_str_clone(&inherits->parent_column)), JAZZ_CMP_EQ, vr_row_id(&parent_source));
    if (record_inherited_contributions) {
        jazz_engine_InheritedContribution c;
        c.id = jz_str_clone(prefix); c.source = sid_clone(&parent_source); c.input = nid_clone(&parent_current); c.membership = pe_clone(&membership);
        jz_vec_push(cx->inherited_contributions, &c, sizeof c);
    }
    nodes_insert(cx->nodes, nid_clone(&join_node), rse_join(child_current, parent_current, JAZZ_JM_SEMI, membership));
    jazz_engine_SourceId_drop(&parent_source);
    *out = join_node;
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__normalization__policy_branch_semantic_label(const jazz_model_PredicateVec *filters, const jazz_model_JoinViaVec *joins,
    const jazz_model_ReachableVia *reachable, u32 nreachable, const jazz_model_InheritsVia *inherits, u32 ninherits, jz_str *out) {
    jz_bytes bytes = {0};
    jz_pc_w w = { &bytes };
    jz_pc_seq(&w, filters->len);
    for (u32 i = 0; i < filters->len; i++) jazz_model_Predicate_serialize(&w, &filters->ptr[i]);
    jz_pc_seq(&w, joins->len);
    for (u32 i = 0; i < joins->len; i++) jazz_model_JoinVia_serialize(&w, &joins->ptr[i]);
    jz_pc_seq(&w, nreachable);
    for (u32 i = 0; i < nreachable; i++) jazz_model_ReachableVia_serialize(&w, &reachable[i]);
    jz_pc_seq(&w, ninherits);
    for (u32 i = 0; i < ninherits; i++) jazz_model_InheritsVia_serialize(&w, &inherits[i]);
    u8 hash[32];
    jz_blake3(bytes.ptr, bytes.len, hash);
    jz_bytes_drop(&bytes);
    jz_str hex = {0};
    jz_hex_encode(hash, 32, &hex);
    *out = jz_format("policy:%.*s", (int)hex.len, hex.ptr);
    jz_str_drop(&hex);
    return JZ_OK;
}

static NodeId jazz_node__node__query_eval__normalization__normalize_row_id_projection(jz_btree *nodes, NodeId input, const SourceId *root_source, NodeId node_id) {
    RSE x = rse(JAZZ_RSE_PROJECT); x.input = input;
    jazz_engine_RowProjection p = projection(S("row_uuid"), ty_simple(GROOVE_VT_UUID), vr_row_id(root_source));
    jz_vec_push(&x.projections, &p, sizeof p);
    nodes_insert(nodes, nid_clone(&node_id), x);
    return node_id;
}

typedef JZ_VEC(jazz_engine_UnionInput) UnionInputVec;

/* Keep the rows of `current` whose root row id appears in any authorized
 * branch of `union_inputs`, registering the `{prefix}:authorized_rows` union
 * and the `{prefix}:authorize` inner join. */
static NodeId jazz_node__node__query_eval__normalization__authorize_by_union(jz_btree *nodes, NodeId current, UnionInputVec union_inputs, const SourceId *root_source, const jz_str *prefix) {
    NodeId union_node = nid(jz_format("%.*s:authorized_rows", (int)prefix->len, prefix->ptr));
    RSE u = rse(JAZZ_RSE_UNION);
    u.inputs.ptr = union_inputs.ptr; u.inputs.len = union_inputs.len; u.inputs.cap = union_inputs.cap;
    nodes_insert(nodes, nid_clone(&union_node), u);
    NodeId join_node = nid(jz_format("%.*s:authorize", (int)prefix->len, prefix->ptr));
    nodes_insert(nodes, nid_clone(&join_node), rse_join(current, union_node, JAZZ_JM_INNER,
        pe_compare(vr_row_id(root_source), JAZZ_CMP_EQ, vr_field(root_source, S("row_uuid")))));
    return join_node;
}

static void union_input_drop(void *p) { jazz_engine_UnionInput *u = p; node_id_drop(&u->node); jz_str_drop(&u->label); }

/* Lower one policy alternative rooted at `{prefix}:root` and add its row-id
 * projection to `union_inputs`. */
static jz_err policy_alternative(NormCtx *cx, const RuntimeSchema *schema, const SourceId *root_source, const jz_str *prefix, PolicyAtomChain chain,
    const jazz_model_PredicateVec *label_filters, const jazz_model_JoinViaVec *label_joins, const jz_str *binding_source_shape, const jz_btree *param_types,
    bool record_inherited, const InheritanceExpansionPath *inheritance_path, UnionInputVec *union_inputs) {
    int pl = (int)prefix->len; const char *pp = prefix->ptr;
    NodeId source_node = nid(jz_format("%.*s:root", pl, pp));
    nodes_insert(cx->nodes, nid_clone(&source_node), rse_source(root_source, JAZZ_VIS_VISIBLE));
    NodeId current;
    JZ_TRY(jazz_node__node__query_eval__normalization__normalize_policy_atom_chain(cx, schema, root_source, source_node, prefix, chain,
        binding_source_shape, param_types, false, record_inherited, inheritance_path, &current));
    jazz_engine_UnionInput input;
    jz_err e = jazz_node__node__query_eval__normalization__policy_branch_semantic_label(label_filters, label_joins, chain.reachable, chain.nreachable, chain.inherits, chain.ninherits, &input.label);
    if (JZ_IS_ERR(e)) { node_id_drop(&current); return e; }
    input.node = jazz_node__node__query_eval__normalization__normalize_row_id_projection(cx->nodes, current, root_source, nid(jz_format("%.*s:row_id", pl, pp)));
    jz_vec_push(union_inputs, &input, sizeof input);
    return JZ_OK;
}

static bool jazz_node__node__query_eval__normalization__policy_branch_base_is_converter_false(const jazz_model_Query *query) {
    return query->filters.len == 1 && query->filters.ptr[0].tag == JAZZ_PRED_ANY && query->filters.ptr[0].children.len == 0
        && query->joins.len == 0 && query->reachable.len == 0 && query->inherits.len == 0;
}

static jz_err jazz_node__node__query_eval__normalization__normalize_policy_branch_authorization(NormCtx *cx, const RuntimeSchema *schema, const SourceId *root_source,
    NodeId current, const jz_str *prefix, const jazz_model_Query *policy, const jz_str *binding_source_shape, const jz_btree *param_types,
    const InheritanceExpansionPath *inheritance_path, NodeId *out) {
    UnionInputVec union_inputs = {0};
    jz_err e = JZ_OK;
    if (!jazz_node__node__query_eval__normalization__policy_branch_base_is_converter_false(policy)) {
        jz_str p = jz_format("%.*s:base", (int)prefix->len, prefix->ptr);
        e = policy_alternative(cx, schema, root_source, &p,
            chain_of(&policy->filters, &policy->joins, policy->inherits.ptr, policy->inherits.len, policy->reachable.ptr, policy->reachable.len),
            &policy->filters, &policy->joins, binding_source_shape, param_types, false, inheritance_path, &union_inputs);
        jz_str_drop(&p);
    }
    for (u32 index = 0; !JZ_IS_ERR(e) && index < policy->policy_branches.len; index++) {
        const jazz_model_PolicyBranch *branch = &policy->policy_branches.ptr[index];
        jz_str p = jz_format("%.*s:%u", (int)prefix->len, prefix->ptr, index);
        e = policy_alternative(cx, schema, root_source, &p,
            chain_of(&branch->filters, &branch->joins, branch->inherits.ptr, branch->inherits.len, branch->reachable.ptr, branch->reachable.len),
            &branch->filters, &branch->joins, binding_source_shape, param_types, false, inheritance_path, &union_inputs);
        jz_str_drop(&p);
    }
    if (JZ_IS_ERR(e)) { jz_vec_drop(&union_inputs, sizeof(jazz_engine_UnionInput), union_input_drop); node_id_drop(&current); return e; }
    *out = jazz_node__node__query_eval__normalization__authorize_by_union(cx->nodes, current, union_inputs, root_source, prefix);
    return JZ_OK;
}

static bool jazz_node__node__query_eval__normalization__unsupported_policy_branch_reason(const jazz_model_Query *query, jz_str *out) {
    (void)query; (void)out;
    return false;
}

/* ---------- NodeState ---------- */
jz_err NM(NodeState__collect_policy_dependency_claim_params)(const jazz_node_NodeState *self, const RuntimeSchema *schema, const jazz_engine_PolicyContext *policy,
    const jazz_engine_NormalizedRowSetShape *input, jz_btree *params) {
    (void)self;
    if (policy->tag == JAZZ_PC_SYSTEM) return JZ_OK;
    const jz_btree *claims = &policy->claims;
    jz_btree tables = jazz_node__node__query_eval__bindings__normalized_source_tables(input);
    jz_str *table_name; void *u;
    jz_err e = JZ_OK;
    JZ_BTREE_FOR(&tables, table_name, u) {
        const jazz_model_TableSchema *table = NULL;
        for (u32 i = 0; i < schema->tables.len; i++) if (jz_str_eq(&schema->tables.ptr[i].name, table_name)) { table = &schema->tables.ptr[i]; break; }
        if (!table) { e = jz_err_fmt(JZN_E_TableNotFound, "table not found: %.*s", (int)table_name->len, table_name->ptr); break; }
        jazz_model_Query query = jazz_node__node__query_eval__bindings__authorization_query_from_read_policy(table);
        jz_btree values; jz_btree_init(&values, &jazz_node_StrValue_desc);
        jazz_node__node__query_eval__bindings__bind_scope_claim_operands(&query, claims, &values);
        jz_btree found;
        e = jazz_node__node__query_eval__bindings__disambiguate_policy_claim_params(&query, schema, &values, &found);
        jazz_model_Query_drop(&query);
        jz_btree_drop(&values);
        if (JZ_IS_ERR(e)) break;
        jz_str name; jazz_engine_ProgramClaimParam claim;
        while (jz_btree_pop_first(&found, &name, &claim)) {
            /* The root policy may rediscover the same claim slot while walking
             * its source tables. Keep the already-lowered slot in that case; a
             * typed alias is only needed when the same claim path is required
             * at a genuinely different schema type. */
            bool known = false;
            void *pk; jazz_engine_ProgramClaimParam *existing;
            JZ_BTREE_FOR(params, pk, existing) {
                if (existing->path.segments.len != claim.path.segments.len || !groove_ValueType_eq(&existing->ty, &claim.ty)) continue;
                bool same = true;
                for (u32 i = 0; i < claim.path.segments.len; i++)
                    if (!jz_str_eq(&existing->path.segments.ptr[i], &claim.path.segments.ptr[i])) { same = false; break; }
                if (same) { known = true; break; }
            }
            if (known) { jz_str_drop(&name); jazz_engine_ClaimPath_drop(&claim.path); groove_ValueType_drop(&claim.ty); continue; }
            jz_btree_insert(params, &name, &claim, NULL);
        }
        jz_btree_drop(&found);
    }
    jz_btree_drop(&tables);
    return e;
}

static jz_err jazz_node__node__query_eval__normalization__NodeState__normalized_relation_union_row_set_shape(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape,
    const jazz_model_RelationQuery *relation, const jazz_model_Binding *binding, const RuntimeSchema *schema, jazz_engine_NormalizedRowSetShape *out);

static void shape_identity_and_result(jazz_engine_NormalizedRowSetShape *n, const jazz_model_ValidatedQuery *shape, const jz_str *table, SourceId root_source) {
    n->identity.shape_id = shape->shape_id;
    n->identity.canonical = jz_bytes_clone(&shape->canonical);
    __builtin_memset(&n->result, 0, sizeof n->result);
    n->result.tag = JAZZ_RID_REAL_ROW;
    n->result.table = jz_str_clone(table);
    n->result.row.tag = JAZZ_RRR_SOURCE;
    n->result.row.source = root_source;
}

jz_err NM(NodeState__normalized_row_set_shape)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_engine_NormalizedRowSetShape *out) {
    const RuntimeSchema *schema = jazz_node__node__state__catalogue__SchemaCatalogue__schema_for_version(&self->catalogue, shape->schema_version);
    if (!schema) return JZN_INVALID_STORED("query schema version is unknown");
    const jazz_model_Query *query = &shape->query;
    if (query->relation) {
        jazz_model_RelationUnionParts parts;
        if (jazz_model__query__relation_union_parts(query->relation->rel, &parts)) {
            jazz_model_RelationUnionParts_drop(&parts);
            return jazz_node__node__query_eval__normalization__NodeState__normalized_relation_union_row_set_shape(self, shape, query->relation, binding, schema, out);
        }
    }
    SourceId root_source = root_source_of(&query->table);
    jz_btree auxiliary_sources; ClosurePathVec closure_paths;
    jz_err e = jazz_node__node__query_eval__normalization__collect_closure_paths(self, &query->table, shape->schema_version, query->includes.ptr, query->includes.len,
        &auxiliary_sources, &closure_paths);
    if (JZ_IS_ERR(e)) { jazz_engine_SourceId_drop(&root_source); return e; }
    jz_btree nodes; jz_btree_init(&nodes, &jazz_engine_RowSetNodes_desc);
    NodeId current = nid(S("root"));
    nodes_insert(&nodes, nid_clone(&current), rse_source(&root_source, JAZZ_VIS_VISIBLE));
    JoinContribVec join_contributions = {0};
    InheritedContribVec inherited_contributions = {0};
    ReachableContribVec reachable_contributions = {0};
    NormCtx cx = { &nodes, &auxiliary_sources, &join_contributions, &inherited_contributions, &reachable_contributions };
    InheritanceExpansionPath inheritance_path = inheritance_path_default();
    jz_str binding_source_shape = jz_str_from(JAZZ_PENDING_BINDING_SOURCE_SHAPE);
    jazz_model_PredicateVec no_filters = {0};
    const jazz_model_PredicateVec *query_chain_filters = query->flat_join ? &no_filters : &query->filters;
    jz_str unsupported_marker = {0};
    bool has_unsupported = jazz_node__node__query_eval__normalization__unsupported_policy_branch_reason(query, &unsupported_marker);
    jazz_model_PolicyBranchVecVec factors = {0};
    bool has_factors = !has_unsupported && jazz_node__node__query_eval__normalization__policy_branch_base_is_converter_false(query)
        && jazz_node__node__query_eval__policy_factoring__factor_policy_branches(query->policy_branches.ptr, query->policy_branches.len, &factors);
    if (has_factors) {
        /* The branches are a product of independent factors: authorize a row
         * when each factor has a satisfied alternative, instead of lowering
         * every combination. */
        for (u32 factor_index = 0; !JZ_IS_ERR(e) && factor_index < factors.len; factor_index++) {
            const jazz_model_PolicyBranchVec *alternatives = &factors.ptr[factor_index];
            UnionInputVec union_inputs = {0};
            for (u32 index = 0; !JZ_IS_ERR(e) && index < alternatives->len; index++) {
                const jazz_model_PolicyBranch *alternative = &alternatives->ptr[index];
                jz_str prefix = jz_format("policy_factor:%u:%u", factor_index, index);
                e = policy_alternative(&cx, schema, &root_source, &prefix,
                    chain_of(&alternative->filters, &alternative->joins, alternative->inherits.ptr, alternative->inherits.len, alternative->reachable.ptr, alternative->reachable.len),
                    &alternative->filters, &alternative->joins, &binding_source_shape, &shape->params, false, &inheritance_path, &union_inputs);
                jz_str_drop(&prefix);
            }
            if (JZ_IS_ERR(e)) { jz_vec_drop(&union_inputs, sizeof(jazz_engine_UnionInput), union_input_drop); break; }
            jz_str prefix = jz_format("policy_factor:%u", factor_index);
            current = jazz_node__node__query_eval__normalization__authorize_by_union(&nodes, current, union_inputs, &root_source, &prefix);
            jz_str_drop(&prefix);
        }
        jazz_model_PolicyBranchVecVec_drop(&factors);
    } else if (!has_unsupported && query->policy_branches.len != 0) {
        UnionInputVec union_inputs = {0};
        if (!jazz_node__node__query_eval__normalization__policy_branch_base_is_converter_false(query)) {
            jz_str prefix = S("policy_branch:base");
            e = policy_alternative(&cx, schema, &root_source, &prefix,
                chain_of(query_chain_filters, &query->joins, query->inherits.ptr, query->inherits.len, query->reachable.ptr, query->reachable.len),
                &query->filters, &query->joins, &binding_source_shape, &shape->params, true, &inheritance_path, &union_inputs);
            jz_str_drop(&prefix);
        }
        for (u32 index = 0; !JZ_IS_ERR(e) && index < query->policy_branches.len; index++) {
            const jazz_model_PolicyBranch *branch = &query->policy_branches.ptr[index];
            jz_str prefix = jz_format("policy_branch:%u", index);
            e = policy_alternative(&cx, schema, &root_source, &prefix,
                chain_of(&branch->filters, &branch->joins, branch->inherits.ptr, branch->inherits.len, branch->reachable.ptr, branch->reachable.len),
                &branch->filters, &branch->joins, &binding_source_shape, &shape->params, false, &inheritance_path, &union_inputs);
            jz_str_drop(&prefix);
        }
        if (JZ_IS_ERR(e)) jz_vec_drop(&union_inputs, sizeof(jazz_engine_UnionInput), union_input_drop);
        else {
            jz_str prefix = S("policy_branch");
            current = jazz_node__node__query_eval__normalization__authorize_by_union(&nodes, current, union_inputs, &root_source, &prefix);
            jz_str_drop(&prefix);
        }
    } else {
        jz_str prefix = S("query");
        NodeId next;
        e = jazz_node__node__query_eval__normalization__normalize_policy_atom_chain(&cx, schema, &root_source, current, &prefix,
            chain_of(query_chain_filters, &query->joins, query->inherits.ptr, query->inherits.len, query->reachable.ptr, query->reachable.len),
            &binding_source_shape, &shape->params, true, true, &inheritance_path, &next);
        jz_str_drop(&prefix);
        if (!JZ_IS_ERR(e)) current = next; else current.s.ptr = NULL;
    }
    if (JZ_IS_ERR(e)) goto fail;

    /* Flat joins are an output form separate from `JoinVia`. Every input stays
     * a normal source, so read-policy filtering and read-view/lens projection
     * happen before Groove's inner JoinOp combines records. */
    if (query->flat_join) {
        const jazz_model_FlatJoin *flat_join = query->flat_join;
        jz_str root_name = flat_join->has_root_alias ? jz_str_clone(&flat_join->root_alias) : jz_str_clone(&query->table);
        jz_btree routed_filters;
        e = jazz_node__node__query_eval__normalization__flat_join_filters_by_source(&query->filters, SL(root_name), &routed_filters);
        if (JZ_IS_ERR(e)) { jz_str_drop(&root_name); goto fail; }
        PredVec filters;
        jz_str removed_key;
        if (jz_btree_remove(&routed_filters, &root_name, &removed_key, &filters)) {
            jz_str_drop(&removed_key);
            NodeId filter_node = nid(S("flat_join:root_filter"));
            PExpr predicate;
            e = jazz_node__node__query_eval__normalization__normalize_predicates(schema, &root_source, filters.ptr, filters.len, true, &predicate);
            predvec_drop(&filters);
            if (JZ_IS_ERR(e)) { node_id_drop(&filter_node); jz_btree_drop(&routed_filters); jz_str_drop(&root_name); goto fail; }
            nodes_insert(&nodes, nid_clone(&filter_node), rse_filter(current, predicate));
            current = filter_node;
        }
        jz_btree sources; jz_btree_init(&sources, &jazz_node_SourceStrings_desc); /* name -> SourceId (keyed by string) */
        typedef struct { jz_str name; SourceId source; } NamedSource;
        JZ_VEC(SourceId) tuple_sources = {0};
        JZ_VEC(NamedSource) output_sources = {0};
        typedef JZ_VEC(NamedSource) NamedVec;
        NamedVec scope_sources = {0}; /* BTreeMap<String, SourceId> as a small association list */
        NamedSource rs = { jz_str_clone(&root_name), sid_clone(&root_source) };
        jz_vec_push(&scope_sources, &rs, sizeof rs);
        SourceId rt = sid_clone(&root_source); jz_vec_push(&tuple_sources, &rt, sizeof rt);
        NamedSource ro = { flat_join->has_root_alias ? jz_str_clone(&flat_join->root_alias) : jz_str_clone(&query->table), sid_clone(&root_source) };
        jz_vec_push(&output_sources, &ro, sizeof ro);
        jz_btree_drop(&sources);
        for (u32 index = 0; !JZ_IS_ERR(e) && index < flat_join->sources.len; index++) {
            const jazz_model_FlatJoinSource *join = &flat_join->sources.ptr[index];
            jz_str name = join->has_alias ? jz_str_clone(&join->alias) : jz_str_clone(&join->table);
            SourceId source = sid1(jz_str_clone(&join->table), role(JAZZ_SR_ALIAS, jz_format("flat_join:%u:%.*s", index, (int)name.len, name.ptr)));
            NodeId source_node = nid(jz_format("flat_join:%u:source", index));
            nodes_insert(&nodes, nid_clone(&source_node), rse_source(&source, JAZZ_VIS_VISIBLE));
            NodeId right_input;
            if (jz_btree_remove(&routed_filters, &name, &removed_key, &filters)) {
                jz_str_drop(&removed_key);
                NodeId filter_node = nid(jz_format("flat_join:%u:filter", index));
                PExpr predicate;
                e = jazz_node__node__query_eval__normalization__normalize_predicates(schema, &source, filters.ptr, filters.len, true, &predicate);
                predvec_drop(&filters);
                if (JZ_IS_ERR(e)) { node_id_drop(&filter_node); node_id_drop(&source_node); jz_str_drop(&name); jazz_engine_SourceId_drop(&source); break; }
                nodes_insert(&nodes, nid_clone(&filter_node), rse_filter(source_node, predicate));
                right_input = filter_node;
            } else {
                right_input = source_node;
            }
            sourceset_insert(&auxiliary_sources, &source);
            /* value_ref(field): resolve a qualified `scope.column` field */
            const jz_str *left_field = &join->on_left;
            u32 dot = left_field->len; while (dot > 0 && left_field->ptr[dot - 1] != '.') dot--;
            VRef left = {0};
            if (dot == 0) {
                e = JZN_QUERY_CAPABILITY("flat join field must be qualified: %.*s", (int)left_field->len, left_field->ptr);
            } else {
                jz_slice scope = jz_slice_of(left_field->ptr, dot - 1);
                jz_slice column = jz_slice_of(left_field->ptr + dot, left_field->len - dot);
                const SourceId *scoped = NULL;
                for (u32 i = 0; i < scope_sources.len; i++) if (jz_slice_cmp(jz_str_slice(&scope_sources.ptr[i].name), scope) == 0) scoped = &scope_sources.ptr[i].source;
                if (!scoped) e = JZN_QUERY_CAPABILITY("unknown flat join source %.*s", (int)scope.len, (const char *)scope.ptr);
                else left = jz_slice_cmp(column, JZ_LIT("_id")) == 0 ? vr_row_id(scoped) : NM(source_column_value)(scoped, column, JAZZ_JOIN_TARGET_COLUMN);
            }
            const jz_str *right_field = &join->on_right;
            u32 rdot = right_field->len; while (rdot > 0 && right_field->ptr[rdot - 1] != '.') rdot--;
            if (!JZ_IS_ERR(e) && rdot == 0) {
                jazz_engine_NormalizedValueRef_drop(&left);
                e = JZN_QUERY_CAPABILITY("flat join field must be qualified: %.*s", (int)right_field->len, right_field->ptr);
            }
            if (JZ_IS_ERR(e)) { node_id_drop(&right_input); jz_str_drop(&name); jazz_engine_SourceId_drop(&source); break; }
            jz_slice right_column = jz_slice_of(right_field->ptr + rdot, right_field->len - rdot);
            NodeId join_node = nid(jz_format("flat_join:%u:join", index));
            PExpr membership = pe_compare(left, JAZZ_CMP_EQ,
                jz_slice_cmp(right_column, JZ_LIT("_id")) == 0 ? vr_row_id(&source) : NM(source_column_value)(&source, right_column, JAZZ_JOIN_TARGET_COLUMN));
            nodes_insert(&nodes, nid_clone(&join_node), rse_join(current, nid_clone(&right_input), JAZZ_JM_INNER, pe_clone(&membership)));
            /* Flat tuple sources must cross as exact receiver inputs, not as
             * fields of an authority-rendered tuple. */
            jazz_engine_JoinContribution c; __builtin_memset(&c, 0, sizeof c);
            c.has_parent = false; c.id = jz_format("flat_join:%u", index); c.source = sid_clone(&source); c.input = right_input; c.membership = membership;
            jz_vec_push(&join_contributions, &c, sizeof c);
            current = join_node;
            NamedSource ns = { jz_str_clone(&name), sid_clone(&source) };
            jz_vec_push(&scope_sources, &ns, sizeof ns);
            NamedSource os = { name, sid_clone(&source) };
            jz_vec_push(&output_sources, &os, sizeof os);
            jz_vec_push(&tuple_sources, &source, sizeof source);
        }
        if (!JZ_IS_ERR(e)) {
            void *scope_key, *unused;
            if (jz_btree_first(&routed_filters, &scope_key, &unused)) {
                const jz_str *scope = scope_key;
                e = JZN_QUERY_LOWERING("flat join filter references unknown source %.*s", (int)scope->len, scope->ptr);
            }
        }
        ProjVec columns = {0};
        if (!JZ_IS_ERR(e)) {
            for (u32 position = 0; position < tuple_sources.len; position++)
                projvec_push(&columns, projection(position == 0 ? S("row_uuid") : jz_format("__flat_join_row_%u", position), ty_simple(GROOVE_VT_UUID),
                    vr_row_id(&tuple_sources.ptr[position])));
            /* Retain the representative root version used by the existing
             * real-row membership envelope. */
            projvec_push(&columns, projection(S("tx_time"), ty_simple(GROOVE_VT_U64), vr_field(&root_source, S("tx_time"))));
            projvec_push(&columns, projection(S("tx_node_id"), ty_simple(GROOVE_VT_U64), vr_field(&root_source, S("tx_node_id"))));
            for (u32 i = 0; !JZ_IS_ERR(e) && i < output_sources.len; i++) {
                const NamedSource *ns = &output_sources.ptr[i];
                const jazz_model_TableSchema *source_schema = NULL;
                for (u32 t = 0; t < schema->tables.len; t++) if (jz_str_eq(&schema->tables.ptr[t].name, &ns->source.table)) { source_schema = &schema->tables.ptr[t]; break; }
                if (!source_schema) { e = JZN_QUERY_CAPABILITY("unknown flat join table %.*s", (int)ns->source.table.len, ns->source.table.ptr); break; }
                for (u32 c = 0; c < source_schema->columns.len; c++) {
                    const jazz_model_ColumnSchema *column = &source_schema->columns.ptr[c];
                    projvec_push(&columns, projection(jz_format("%.*s.%.*s", (int)ns->name.len, ns->name.ptr, (int)column->name.len, column->name.ptr),
                        ty_clone(&column->column_type), vr_field(&ns->source, jz_str_clone(&column->name))));
                }
            }
        }
        for (u32 i = 0; i < scope_sources.len; i++) { jz_str_drop(&scope_sources.ptr[i].name); jazz_engine_SourceId_drop(&scope_sources.ptr[i].source); }
        jz_free(scope_sources.ptr);
        for (u32 i = 0; i < output_sources.len; i++) { jz_str_drop(&output_sources.ptr[i].name); jazz_engine_SourceId_drop(&output_sources.ptr[i].source); }
        jz_free(output_sources.ptr);
        jz_vec_drop(&tuple_sources, sizeof(SourceId), jazz_engine_SourceId_drop);
        jz_btree_drop(&routed_filters);
        jz_str_drop(&root_name);
        if (JZ_IS_ERR(e)) { projvec_drop(&columns); goto fail; }
        NodeId projection_node = nid(S("flat_join:output"));
        RSE x = rse(JAZZ_RSE_PROJECT); x.input = current;
        x.projections.ptr = columns.ptr; x.projections.len = columns.len; x.projections.cap = columns.cap;
        nodes_insert(&nodes, nid_clone(&projection_node), x);
        current = projection_node;
    }

    for (u32 index = 0; index < query->array_subqueries.len; index++) {
        NodeId next;
        e = jazz_node__node__query_eval__normalization__normalize_array_subquery(&nodes, current, schema, &root_source, &query->array_subqueries.ptr[index], &index, 1, &next);
        if (JZ_IS_ERR(e)) { current.s.ptr = NULL; goto fail; }
        current = next;
    }
    if (!query->aggregate && query->order_by.len != 0) {
        OrderKeyVec keys;
        e = order_keys_for(&root_source, query->order_by.ptr, query->order_by.len, &keys);
        if (JZ_IS_ERR(e)) goto fail;
        NodeId order_node = nid(S("order"));
        RSE x = rse(JAZZ_RSE_ORDER_BY); x.input = current;
        x.order_keys.ptr = keys.ptr; x.order_keys.len = keys.len; x.order_keys.cap = keys.cap;
        nodes_insert(&nodes, nid_clone(&order_node), x);
        current = order_node;
    }
    if (!query->aggregate && (query->has_limit || query->offset != 0)) {
        NodeId slice_node = nid(S("slice"));
        RSE x = rse(JAZZ_RSE_SLICE); x.input = current;
        x.has_limit = query->has_limit; x.limit = query->limit; x.offset = query->offset;
        VRef tie = vr_row_id(&root_source); jz_vec_push(&x.tie_breaker, &tie, sizeof tie);
        x.has_rank_output = false;
        nodes_insert(&nodes, nid_clone(&slice_node), x);
        current = slice_node;
    }
    /* Relation output aliases are a terminal presentation concern. Keep
     * source-bound ordering and pagination above this projection so their keys
     * still resolve against the source descriptor. */
    bool has_relation_output_projection = false;
    if (query->relation) {
        jazz_model_RelationProjectColumnVec cols = {0};
        e = jazz_model__query__relation_output_projection_if_present(query->relation, &has_relation_output_projection, &cols);
        if (JZ_IS_ERR(e)) goto fail;
        if (has_relation_output_projection) jazz_model_RelationProjectColumnVec_drop(&cols);
    }
    if (has_relation_output_projection) {
        ProjVec columns;
        e = jazz_node__node__query_eval__normalization__relation_row_projection(schema, query, &root_source, &columns);
        if (JZ_IS_ERR(e)) goto fail;
        NodeId project_node = nid(S("relation:output"));
        RSE x = rse(JAZZ_RSE_PROJECT); x.input = current;
        x.projections.ptr = columns.ptr; x.projections.len = columns.len; x.projections.cap = columns.cap;
        nodes_insert(&nodes, nid_clone(&project_node), x);
        current = project_node;
    }
    if (has_unsupported) {
        NodeId node = nid(S("unsupported:policy_branches"));
        RSE x = rse(JAZZ_RSE_DISTINCT); x.input = current;
        VRef marker = vr(JAZZ_NVR_LITERAL); marker.literal = jz_bytes_from((const u8 *)unsupported_marker.ptr, unsupported_marker.len);
        jz_str_drop(&unsupported_marker);
        jz_vec_push(&x.keys, &marker, sizeof marker);
        nodes_insert(&nodes, nid_clone(&node), x);
        current = node;
    }
    if (query->aggregate) {
        RSE x = rse(JAZZ_RSE_AGGREGATE);
        e = jazz_node__node__query_eval__normalization__normalized_aggregate_group_by(&root_source, query->aggregate, &x.keys);
        if (JZ_IS_ERR(e)) goto fail;
        e = jazz_node__node__query_eval__normalization__normalized_aggregate_outputs(&root_source, query->aggregate, &x.outputs);
        if (JZ_IS_ERR(e)) { vrvec_drop(&x.keys); goto fail; }
        NodeId aggregate_node = nid(S("aggregate"));
        x.input = current;
        nodes_insert(&nodes, nid_clone(&aggregate_node), x);
        current = aggregate_node;
    }
    jz_btree_drop(&inheritance_path.uses);
    jz_str_drop(&binding_source_shape);
    __builtin_memset(out, 0, sizeof *out);
    shape_identity_and_result(out, shape, &query->table, root_source);
    out->root = current;
    out->auxiliary_sources = auxiliary_sources;
    out->closure_paths.ptr = closure_paths.ptr; out->closure_paths.len = closure_paths.len; out->closure_paths.cap = closure_paths.cap;
    out->join_contributions.ptr = join_contributions.ptr; out->join_contributions.len = join_contributions.len; out->join_contributions.cap = join_contributions.cap;
    out->inherited_contributions.ptr = inherited_contributions.ptr; out->inherited_contributions.len = inherited_contributions.len; out->inherited_contributions.cap = inherited_contributions.cap;
    out->reachable_contributions.ptr = reachable_contributions.ptr; out->reachable_contributions.len = reachable_contributions.len; out->reachable_contributions.cap = reachable_contributions.cap;
    out->nodes = nodes;
    jz_btree claim_params = jazz_node__node__query_eval__bindings__binding_claim_params_for_shape(out, &shape->params);
    jz_str source_shape = jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_parts(&shape->params, &claim_params);
    jz_btree_drop(&claim_params);
    jazz_node__node__query_eval__bindings__retarget_binding_value_sources(out, &source_shape);
    jz_str_drop(&source_shape);
    return JZ_OK;
fail:
    if (current.s.ptr) node_id_drop(&current);
    jz_btree_drop(&inheritance_path.uses);
    jz_str_drop(&binding_source_shape);
    if (has_unsupported) jz_str_drop(&unsupported_marker);
    jazz_engine_SourceId_drop(&root_source);
    jz_btree_drop(&auxiliary_sources);
    jz_vec_drop(&closure_paths, sizeof(jazz_engine_ClosurePath), closure_path_drop);
    jz_vec_drop(&join_contributions, sizeof(jazz_engine_JoinContribution), join_contribution_drop);
    jz_free(inherited_contributions.ptr); jz_free(reachable_contributions.ptr);
    jz_btree_drop(&nodes);
    return e;
}

/* Normalize a retained public `UNION ALL` by reusing the established
 * single-relation facade for each arm. The arm query is normalized before the
 * union, so joins and recursive gathers keep their existing Groove
 * source/policy lowering; only the public union boundary is new. */
static jz_err jazz_node__node__query_eval__normalization__NodeState__normalized_relation_union_row_set_shape(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape,
    const jazz_model_RelationQuery *relation, const jazz_model_Binding *binding, const RuntimeSchema *schema, jazz_engine_NormalizedRowSetShape *out) {
    jazz_model_RelationUnionParts parts;
    if (!jazz_model__query__relation_union_parts(relation->rel, &parts))
        return query_capability_msg("retained relation body must be a UNION ALL with global terminal operators in builder order");
    jz_btree nodes; jz_btree_init(&nodes, &jazz_engine_RowSetNodes_desc);
    UnionInputVec union_inputs = {0};
    jz_btree auxiliary_sources; jz_btree_init(&auxiliary_sources, &jazz_engine_SourceId_set_desc);
    ClosurePathVec closure_paths = {0};
    JoinContribVec join_contributions = {0};
    InheritedContribVec inherited_contributions = {0};
    ReachableContribVec reachable_contributions = {0};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < parts.inputs.len; i++) {
        const jazz_model_RelationUnionArm *arm = &parts.inputs.ptr[i];
        jazz_model_RelationQuery arm_relation; jazz_model_RelationQuery_clone(&arm_relation, arm->input);
        jazz_model_Query arm_query;
        e = jazz_model__query__relation_query_to_query(&arm_relation, &arm_query);
        if (JZ_IS_ERR(e)) break;
        if (!jz_str_eq(&arm_query.table, &shape->query.table)) {
            jazz_model_Query_drop(&arm_query);
            e = query_capability_msg("UNION ALL arms must emit the same output table");
            break;
        }
        arm_query.relation = jz_memdup(&arm_relation, sizeof arm_relation);
        jazz_model_ValidatedQuery arm_shape;
        e = jazz_model__query__validate_union_arm_with_schema_version(&arm_query, schema, shape->schema_version, &arm_shape);
        jazz_model_Query_drop(&arm_query);
        if (JZ_IS_ERR(e)) break;
        jazz_engine_NormalizedRowSetShape normalized;
        e = NM(NodeState__normalized_row_set_shape)(self, &arm_shape, binding, &normalized);
        jazz_model_ValidatedQuery_drop(&arm_shape);
        if (JZ_IS_ERR(e)) break;
        jz_str prefix = jz_format("relation_union:%.*s", (int)arm->label.len, arm->label.ptr);
        e = jazz_node__node__query_eval__normalization__prefix_normalized_relation_arm(&normalized, &prefix, &nodes, &auxiliary_sources,
            &closure_paths, &join_contributions, &inherited_contributions, &reachable_contributions);
        jz_str_drop(&prefix);
        if (JZ_IS_ERR(e)) { jazz_engine_NormalizedRowSetShape_drop(&normalized); break; }
        jazz_engine_UnionInput input = { nid_clone(&normalized.root), jz_str_clone(&arm->label) };
        jz_vec_push(&union_inputs, &input, sizeof input);
        jazz_engine_NormalizedRowSetShape_drop(&normalized);
    }
    if (JZ_IS_ERR(e)) goto fail;
    NodeId root = nid(S("relation_union:root"));
    {
        RSE u = rse(JAZZ_RSE_UNION);
        u.inputs.ptr = union_inputs.ptr; u.inputs.len = union_inputs.len; u.inputs.cap = union_inputs.cap;
        union_inputs.ptr = NULL; union_inputs.len = union_inputs.cap = 0;
        nodes_insert(&nodes, nid_clone(&root), u);
    }
    SourceId root_source = root_source_of(&shape->query.table);
    NodeId terminal = root;
    if (parts.has_order_by) {
        OrderKeyVec keys = {0};
        for (u32 i = 0; i < parts.order_by.len; i++) {
            jazz_engine_OrderKey k;
            e = normalize_column(&root_source, &parts.order_by.ptr[i].column.column, &k.value);
            if (JZ_IS_ERR(e)) break;
            k.direction = parts.order_by.ptr[i].direction == JAZZ_ORDER_ASC ? JAZZ_SORT_ASC : JAZZ_SORT_DESC;
            jz_vec_push(&keys, &k, sizeof k);
        }
        if (JZ_IS_ERR(e)) { jz_vec_drop(&keys, sizeof(jazz_engine_OrderKey), order_key_drop); node_id_drop(&terminal); jazz_engine_SourceId_drop(&root_source); goto fail; }
        NodeId order = nid(S("relation_union:order"));
        RSE x = rse(JAZZ_RSE_ORDER_BY); x.input = terminal;
        x.order_keys.ptr = keys.ptr; x.order_keys.len = keys.len; x.order_keys.cap = keys.cap;
        nodes_insert(&nodes, nid_clone(&order), x);
        terminal = order;
    }
    if (parts.has_limit || parts.has_offset) {
        NodeId slice = nid(S("relation_union:slice"));
        RSE x = rse(JAZZ_RSE_SLICE); x.input = terminal;
        x.has_limit = parts.has_limit; x.limit = parts.limit;
        x.offset = parts.has_offset ? parts.offset : 0;
        VRef a = vr_row_id(&root_source); jz_vec_push(&x.tie_breaker, &a, sizeof a);
        /* A physical row can occur through more than one UNION ALL arm. Its
         * semantic arm carrier completes the global page order. */
        VRef b = vr_field(&root_source, S("__root_union_arm")); jz_vec_push(&x.tie_breaker, &b, sizeof b);
        x.has_rank_output = false;
        nodes_insert(&nodes, nid_clone(&slice), x);
        terminal = slice;
    }
    jazz_engine_SourceId_drop(&root_source);
    jazz_model_RelationUnionParts_drop(&parts);
    __builtin_memset(out, 0, sizeof *out);
    shape_identity_and_result(out, shape, &shape->query.table, root_source_of(&shape->query.table));
    out->root = terminal;
    out->auxiliary_sources = auxiliary_sources;
    out->closure_paths.ptr = closure_paths.ptr; out->closure_paths.len = closure_paths.len; out->closure_paths.cap = closure_paths.cap;
    out->join_contributions.ptr = join_contributions.ptr; out->join_contributions.len = join_contributions.len; out->join_contributions.cap = join_contributions.cap;
    out->inherited_contributions.ptr = inherited_contributions.ptr; out->inherited_contributions.len = inherited_contributions.len; out->inherited_contributions.cap = inherited_contributions.cap;
    out->reachable_contributions.ptr = reachable_contributions.ptr; out->reachable_contributions.len = reachable_contributions.len; out->reachable_contributions.cap = reachable_contributions.cap;
    out->nodes = nodes;
    return JZ_OK;
fail:
    jazz_model_RelationUnionParts_drop(&parts);
    jz_vec_drop(&union_inputs, sizeof(jazz_engine_UnionInput), union_input_drop);
    jz_btree_drop(&nodes);
    jz_btree_drop(&auxiliary_sources);
    jz_vec_drop(&closure_paths, sizeof(jazz_engine_ClosurePath), closure_path_drop);
    jz_vec_drop(&join_contributions, sizeof(jazz_engine_JoinContribution), join_contribution_drop);
    jz_free(inherited_contributions.ptr); jz_free(reachable_contributions.ptr);
    return e;
}

jz_err NM(NodeState__normalized_include_deleted_row_set_shape)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_engine_NormalizedRowSetShape *out) {
    JZ_TRY(NM(NodeState__normalized_row_set_shape)(self, shape, binding, out));
    SourceId root_source = root_source_of(&shape->query.table);
    NodeId *k; RSE *node;
    JZ_BTREE_FOR(&out->nodes, k, node)
        if (node->tag == JAZZ_RSE_SOURCE && jazz_engine_SourceId_eq(&node->source, &root_source)) node->visibility = JAZZ_VIS_INCLUDE_DELETED;
    jazz_engine_SourceId_drop(&root_source);
    return JZ_OK;
}
