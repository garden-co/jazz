/* Port of crates/jazz/layers/node/src/node/query_eval/authorization.rs
 * Query authorization semantics and policy-proof graph construction.
 *
 * This module selects operation-specific policies and policy-owning schemas,
 * constructs read/write authorization requests and support scopes, and caches
 * executable proof graphs. Parameter-slot and claim rewriting mechanics live
 * in the bindings module.
 *
 * The #[cfg(test)] proof-compilation pause (PolicyProofCompilationPause and
 * its thread-local) is test-only and not ported. */
#include "jazz_node/node/node_qe_b.h"

#define BIND(name) jazz_node__node__query_eval__bindings__##name
#define LOWER(name) jazz_node__node__query_eval__lowering__##name

static void str_vec_free_c(char **v, u32 n) { for (u32 i = 0; i < n; i++) jz_free(v[i]); jz_free(v); }
static char **cstrs(const jz_str *v, u32 n)
{
    char **out = jz_zalloc(sizeof(char *) * (n ? n : 1));
    for (u32 i = 0; i < n; i++) {
        out[i] = jz_alloc(v[i].len + 1);
        memcpy(out[i], v[i].ptr, v[i].len);
        out[i][v[i].len] = 0;
    }
    return out;
}
static bool str_contains(const jz_str *s, const char *needle)
{
    usize n = strlen(needle);
    if (n > s->len) return false;
    for (usize i = 0; i + n <= s->len; i++)
        if (memcmp(s->ptr + i, needle, n) == 0) return true;
    return false;
}

static void shape_binding_drop(void *p) { jazz_node_ShapeBinding *s = p; jazz_model_ValidatedQuery_drop(&s->shape); jazz_model_Binding_drop(&s->binding); }
void jazz_node_AuthorizationSupportScope_drop(void *p)
{
    jazz_node_AuthorizationSupportScope *s = p;
    jz_str_drop(&s->operation.table);
    jazz_protocol_RegisterShapeOptions_drop(&s->options);
    jz_vec_drop(&s->subscriptions, sizeof(jazz_node_ShapeBinding), shape_binding_drop);
}

static jz_str lit_str(const char *s) { return jz_str_from(s); }

static jazz_node_PolicyAuthorizationGraph jazz_node__node__query_eval__authorization__empty_policy_filtered_current_source_graph(groove_GraphBuilder *base, groove_ProjectField *output_fields, u32 n)
{
    static const char *const keys[] = {"row_uuid"};
    jazz_node_PolicyAuthorizationGraph g;
    memset(&g, 0, sizeof g);
    groove_GraphBuilder *joined = groove__ivm__graph__GraphBuilder__join(base, LOWER(empty_authorized_row_id_graph)(), keys, 1, keys, 1);
    JZ_VEC(groove_ProjectField) fields = {0};
    for (u32 i = 0; i < n; i++) { groove_ProjectField f; groove_ProjectField_clone(&f, &output_fields[i]); JZ_VEC_PUSH(fields, f); }
    g.graph = groove__ivm__graph__GraphBuilder__project_fields(joined, fields.ptr, fields.len);
    jz_vec_drop(&fields, sizeof(groove_ProjectField), NULL);
    jz_btree_init(&g.route_fields, &jazz_node_qeb_STR_SET_DESC);
    jz_btree_init(&g.access_paths, &jazz_node_ACCESS_PATHS_DESC);
    return g;
}

static void jazz_node__node__query_eval__authorization__prune_session_unsatisfiable_branches(jazz_model_Query *query, const jz_btree *claim_values);

static void rewrite_preds(jazz_model_PredicateVec *v, const jz_btree *claims)
{
    for (u32 i = 0; i < v->len; i++)
        v->ptr[i] = BIND(rewrite_claim_predicate_for_binding)(v->ptr[i], claims);
}

__attribute__((unused))
static jz_err jazz_node__node__query_eval__authorization__compile_permission_scope_policy(jazz_model_Query query, const jz_btree *claims, const jz_btree *claim_values, const jazz_model_JazzSchema *schema, jazz_node_ShapeBinding *out)
{
    rewrite_preds(&query.filters, claims);
    for (u32 i = 0; i < query.joins.len; i++)
        query.joins.ptr[i] = BIND(rewrite_claim_join_for_binding)(query.joins.ptr[i], claims);
    JZ_VEC_FOR(jazz_model_ReachableVia, reachable, query.reachable) {
        rewrite_preds(&reachable->access_filters, claims);
        rewrite_preds(&reachable->edge_filters, claims);
        if (reachable->seed) rewrite_preds(&reachable->seed->filters, claims);
    }
    jazz_node__node__query_eval__authorization__prune_session_unsatisfiable_branches(&query, claim_values);
    jz_btree values;
    jz_btree_init(&values, &jazz_node_qeb_STR_VALUE_DESC);
    BIND(bind_scope_claim_operands)(&query, claim_values, &values);
    jazz_model_ValidatedQuery shape;
    jz_err e = jazz_model__query__builders__Query__validate_runtime(&query, schema, &shape);
    jazz_model_Query_drop(&query);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&values); return e; }
    jazz_node__node__query_eval__prepared_bindings__coerce_binding_values_for_shape(&shape, &values);
    jazz_model_Binding binding;
    e = jazz_model__query__validation__ValidatedQuery__bind(&shape, values, &binding);
    if (JZ_IS_ERR(e)) { jazz_model_ValidatedQuery_drop(&shape); return e; }
    out->shape = shape;
    out->binding = binding;
    return JZ_OK;
}

/* Option<bool> */
enum { OB_NONE = -1, OB_FALSE = 0, OB_TRUE = 1 };
static int jazz_node__node__query_eval__authorization__claim_only_predicate_value(const jazz_model_Predicate *predicate, const jz_btree *claim_values);

/* Drops the policy alternatives this session's claims can never satisfy.
 *
 * Support is compiled for one immutable claim snapshot, so an alternative
 * whose filters include a claim-only comparison that is false for these
 * claims contributes no row, whatever the data. Keeping it would make every
 * support view hydrate the joins of, for example, another role's branches.
 * The final write-policy evaluation is unaffected: it still evaluates the
 * complete policy. When every alternative would be dropped, the query is left
 * unchanged, so an empty branch list is never mistaken for an unrestricted
 * one. */
static bool keep_branch(void *item, void *ctx)
{
    (void)item;
    const bool **cursor = ctx;
    bool keep = **cursor;
    (*cursor)++;
    return keep;
}

static void jazz_node__node__query_eval__authorization__prune_session_unsatisfiable_branches(jazz_model_Query *query, const jz_btree *claim_values)
{
    u32 n = query->policy_branches.len;
    if (n == 0) return;
    bool *satisfiable = jz_alloc(sizeof(bool) * n);
    bool all = true, any = false;
    for (u32 i = 0; i < n; i++) {
        const jazz_model_PolicyBranch *branch = &query->policy_branches.ptr[i];
        bool keep = true;
        JZ_VEC_FOR(const jazz_model_Predicate, predicate, branch->filters)
            if (jazz_node__node__query_eval__authorization__claim_only_predicate_value(predicate, claim_values) == OB_FALSE) { keep = false; break; }
        satisfiable[i] = keep;
        all = all && keep;
        any = any || keep;
    }
    if (all || !any) { jz_free(satisfiable); return; }
    const bool *cursor = satisfiable;
    jz_vec_retain(&query->policy_branches, sizeof(jazz_model_PolicyBranch), keep_branch, &cursor, jazz_model_PolicyBranch_drop);
    jz_free(satisfiable);
}

static int jazz_node__node__query_eval__authorization__claim_only_operands_equal(const jazz_model_Operand *left, const jazz_model_Operand *right, const jz_btree *claim_values);

/* The exact value of a predicate that compares only claims and literals, or
 * `None` when it reads row data or cannot be decided without the runtime's
 * type coercion (differently typed operands, null or missing claims). */
static int jazz_node__node__query_eval__authorization__claim_only_predicate_value(const jazz_model_Predicate *predicate, const jz_btree *claim_values)
{
    switch (predicate->tag) {
    case JZ_PRED_ALL: {
        int all = OB_TRUE;
        JZ_VEC_FOR(const jazz_model_Predicate, p, predicate->children) {
            int v = jazz_node__node__query_eval__authorization__claim_only_predicate_value(p, claim_values);
            if (v == OB_FALSE) return OB_FALSE;
            if (v == OB_NONE) all = OB_NONE;
        }
        return all;
    }
    case JZ_PRED_ANY: {
        int any = OB_FALSE;
        JZ_VEC_FOR(const jazz_model_Predicate, p, predicate->children) {
            int v = jazz_node__node__query_eval__authorization__claim_only_predicate_value(p, claim_values);
            if (v == OB_TRUE) return OB_TRUE;
            if (v == OB_NONE) any = OB_NONE;
        }
        return any;
    }
    case JZ_PRED_NOT: {
        int v = jazz_node__node__query_eval__authorization__claim_only_predicate_value(predicate->inner, claim_values);
        return v == OB_NONE ? OB_NONE : !v;
    }
    case JZ_PRED_EQ:
        return jazz_node__node__query_eval__authorization__claim_only_operands_equal(&predicate->left, &predicate->right, claim_values);
    case JZ_PRED_NE: {
        int v = jazz_node__node__query_eval__authorization__claim_only_operands_equal(&predicate->left, &predicate->right, claim_values);
        return v == OB_NONE ? OB_NONE : !v;
    }
    case JZ_PRED_IN: {
        int any = OB_FALSE;
        JZ_VEC_FOR(const jazz_model_Operand, value, predicate->values) {
            int v = jazz_node__node__query_eval__authorization__claim_only_operands_equal(&predicate->left, value, claim_values);
            if (v == OB_TRUE) return OB_TRUE;
            if (v == OB_NONE) any = OB_NONE;
        }
        return any;
    }
    default: return OB_NONE;
    }
}

static bool jazz_node__node__query_eval__authorization__claim_only_operand_value(const jazz_model_Operand *operand, const jz_btree *claim_values, groove_Value *out);

static int jazz_node__node__query_eval__authorization__claim_only_operands_equal(const jazz_model_Operand *left, const jazz_model_Operand *right, const jz_btree *claim_values)
{
    groove_Value l, r;
    if (!jazz_node__node__query_eval__authorization__claim_only_operand_value(left, claim_values, &l)) return OB_NONE;
    if (!jazz_node__node__query_eval__authorization__claim_only_operand_value(right, claim_values, &r)) { groove_Value_drop(&l); return OB_NONE; }
    int res = OB_NONE;
    /* std::mem::discriminant(&left) == std::mem::discriminant(&right) */
    if (l.tag == r.tag) res = groove_Value_eq(&l, &r) ? OB_TRUE : OB_FALSE;
    groove_Value_drop(&l);
    groove_Value_drop(&r);
    return res;
}

static bool jazz_node__node__query_eval__authorization__claim_only_operand_value(const jazz_model_Operand *operand, const jz_btree *claim_values, groove_Value *out)
{
    groove_Value value;
    switch (operand->tag) {
    case JZ_OPERAND_LITERAL:
        groove_Value_clone(&value, &operand->literal);
        break;
    case JZ_OPERAND_CLAIM: {
        jz_str_vec path = jazz_model__query__policy_and_request_contracts__operand_claim_path(&operand->name);
        bool found = jazz_model__model__policy_claims__policy_claim_at_path(claim_values, path.ptr, path.len, &value);
        jz_str_vec_drop(&path);
        if (!found) return false;
        break;
    }
    default: return false;
    }
    if (value.tag == GV_NULLABLE) {
        if (!value.v.nullable) { groove_Value_drop(&value); return false; }
        groove_Value_clone(out, value.v.nullable);
        groove_Value_drop(&value);
        return true;
    }
    *out = value;
    return true;
}

static void jazz_node__node__query_eval__authorization__authorization_scope_action(const jazz_protocol_PermissionAdviceAction *action, jazz_protocol_AuthorizationScopeOperation *op, const jz_str **table)
{
    switch (action->tag) {
    case JZ_PAA_READ: *op = JZ_ASO_READ; break;
    case JZ_PAA_INSERT: *op = JZ_ASO_INSERT; break;
    case JZ_PAA_UPDATE: *op = JZ_ASO_UPDATE; break;
    case JZ_PAA_DELETE: *op = JZ_ASO_DELETE; break;
    default: JZ_UNREACHABLE();
    }
    *table = &action->table;
}

typedef JZ_VEC(jazz_model_Query) QueryVec;
static void push_opt_query(QueryVec *v, const jazz_model_Query *q)
{
    if (!q) return;
    jazz_model_Query c;
    jazz_model_Query_clone(&c, q);
    JZ_VEC_PUSH(*v, c);
}

static QueryVec jazz_node__node__query_eval__authorization__authorization_policy_queries(const jazz_model_TableSchema *table, jazz_protocol_AuthorizationScopeOperation operation)
{
    QueryVec out = {0};
    const jazz_model_WritePolicies *wp = jazz_model__schema__TableSchema__write_policies(table);
    switch (operation) {
    case JZ_ASO_READ:
        /* No dependency data can prove an absent read grant. Admission still
         * evaluates the constant-false read policy; no support scope is needed. */
        if (!table->read_policy) break;
        JZ_VEC_PUSH(out, BIND(authorization_query_from_read_policy)(table));
        break;
    case JZ_ASO_INSERT: push_opt_query(&out, wp->insert_check); break;
    case JZ_ASO_UPDATE: push_opt_query(&out, wp->update_using); push_opt_query(&out, wp->update_check); break;
    case JZ_ASO_DELETE: push_opt_query(&out, wp->delete_using); break;
    }
    return out;
}

static jazz_model_Query seed_policy(jazz_model_Query policy, jazz_types_RowUuid row)
{
    groove_Value v;
    memset(&v, 0, sizeof v);
    v.tag = GV_UUID;
    v.v.uuid = row;
    return jazz_model__query__builders__Query__filter(policy,
        jazz_model__query__builders__eq(jazz_model__query__builders__col("id"), jazz_model__query__builders__lit(v)));
}

/* Policy clauses for one advice action, seeded with the target row where the
 * clause is evaluated on the stored row. Such a clause only needs that row and
 * the dependencies reachable from it, not every row the policy matches
 * (#3468). `update_check` stays unseeded: it runs on the patched row, whose
 * dependencies (for example a new parent) the stored row cannot reach. */
static QueryVec jazz_node__node__query_eval__authorization__authorization_row_policy_queries(const jazz_model_TableSchema *table, const jazz_protocol_PermissionAdviceAction *action)
{
    QueryVec out;
    switch (action->tag) {
    case JZ_PAA_READ:
        out = jazz_node__node__query_eval__authorization__authorization_policy_queries(table, JZ_ASO_READ);
        for (u32 i = 0; i < out.len; i++) out.ptr[i] = seed_policy(out.ptr[i], action->row);
        return out;
    case JZ_PAA_INSERT:
        return jazz_node__node__query_eval__authorization__authorization_policy_queries(table, JZ_ASO_INSERT);
    case JZ_PAA_UPDATE: {
        const jazz_model_WritePolicies *wp = jazz_model__schema__TableSchema__write_policies(table);
        memset(&out, 0, sizeof out);
        if (wp->update_using) {
            jazz_model_Query c;
            jazz_model_Query_clone(&c, wp->update_using);
            JZ_VEC_PUSH(out, seed_policy(c, action->row));
        }
        push_opt_query(&out, wp->update_check);
        return out;
    }
    case JZ_PAA_DELETE:
        out = jazz_node__node__query_eval__authorization__authorization_policy_queries(table, JZ_ASO_DELETE);
        for (u32 i = 0; i < out.len; i++) out.ptr[i] = seed_policy(out.ptr[i], action->row);
        return out;
    default: JZ_UNREACHABLE();
    }
}

static jazz_protocol_AuthorizationOperationKey jazz_node__node__query_eval__authorization__authorization_operation_key(jazz_protocol_AuthorizationScopeOperation operation, const jz_str *table, const jazz_protocol_PermissionAdviceAction *action, jz_bytes action_bytes)
{
    jazz_protocol_AuthorizationOperationKey k;
    memset(&k, 0, sizeof k);
    k.operation = operation;
    k.table = jz_str_clone(table);
    switch (action->tag) {
    case JZ_PAA_READ: case JZ_PAA_UPDATE: case JZ_PAA_DELETE: k.has_row = true; k.row = action->row; break;
    case JZ_PAA_INSERT: k.has_row = false; break;
    }
    jz_blake3(action_bytes.ptr, action_bytes.len, k.candidate_digest);
    jz_bytes_drop(&action_bytes);
    return k;
}

/* ---------------- NodeState impl ---------------- */

static void AUTHZ(NodeState__authorization_subplan_policy)(const jazz_node_NodeState *self, jazz_types_AuthorSubject identity, const jz_str *table, jazz_engine_PolicyDecisionRole role, jazz_engine_PolicyContext *out);
static jz_err AUTHZ(NodeState__inline_policy_subplan_input)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *policy_shape, const jazz_model_JazzSchema *schema, jazz_model_ValidatedQuery *shape_out, jazz_model_Binding *binding_out, jazz_engine_NormalizedRowSetShape *input_shape, jazz_engine_ProgramBinding *input_binding);
static jz_err AUTHZ(NodeState__write_policy_query_program_allows)(jazz_node_NodeState *self, const jazz_engine_QueryProgram *program, const jazz_model_ValidatedQuery *policy_shape, const jazz_model_Binding *binding, bool *out);
static jz_err AUTHZ(NodeState__policy_authorization_row_id_graph_inner)(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, const jz_btree *forced_access_paths, bool cache, bool has_bounded, jazz_engine_SourceId bounded_source, groove_GraphBuilder *bounded_graph, jazz_node_PolicyAuthorizationGraph *out);

static jz_btree primary_key_access_paths(const jz_str *table, jazz_types_RowUuid row_uuid)
{
    jz_btree access_paths;
    jz_btree_init(&access_paths, &jazz_node_ACCESS_PATHS_DESC);
    jazz_engine_SourceId src = jazz_node__node__query_eval__normalization__root_source_id(table->ptr, table->len);
    jazz_node_CurrentAccessPath p;
    memset(&p, 0, sizeof p);
    p.tag = JZN_CAP_PRIMARY_KEY;
    groove_Value v;
    memset(&v, 0, sizeof v);
    v.tag = GV_UUID;
    v.v.uuid = row_uuid;
    JZ_VEC_PUSH(p.values, v);
    jz_btree_insert(&access_paths, &src, &p, NULL);
    return access_paths;
}

jz_err AUTHZ(NodeState__read_policy_query_allows_open_tx_row)(jazz_node_NodeState *self, jazz_model_OpenTransactionId tx_id, const jazz_model_Query *policy, jazz_types_SchemaVersionId policy_schema_version, jazz_types_RowUuid row_uuid, jazz_types_AuthorSubject identity, bool *out)
{
    const jazz_model_JazzSchema *ps = jazz_node__node__SchemaCatalogue__schema_for_version(&self->catalogue, policy_schema_version);
    if (!ps) return JZN_INVALID_STORED("policy schema payload missing");
    jazz_model_JazzSchema policy_schema;
    jazz_model_JazzSchema_clone(&policy_schema, ps);
    /* Access-path pushdown is an optimization, not part of the decision
     * boundary. Keep the target coordinate in the policy AST as well so
     * inline transaction overlays cannot authorize one row from another
     * visible row returned by the same source graph. */
    jazz_model_Query pq;
    jazz_model_Query_clone(&pq, policy);
    pq = seed_policy(pq, row_uuid);
    jazz_model_ValidatedQuery validated;
    jz_err e = jazz_model__query__builders__Query__validate_with_schema_version(&pq, &policy_schema, policy_schema_version, &validated);
    jazz_model_Query_drop(&pq);
    if (JZ_IS_ERR(e)) goto out_schema;
    jazz_model_ValidatedQuery policy_shape;
    jazz_model_Binding binding;
    jazz_engine_QueryProgramRequest request;
    memset(&request, 0, sizeof request);
    e = AUTHZ(NodeState__inline_policy_subplan_input)(self, &validated, &policy_schema, &policy_shape, &binding, &request.shape, &request.binding);
    jazz_model_ValidatedQuery_drop(&validated);
    if (JZ_IS_ERR(e)) goto out_schema;
    AUTHZ(NodeState__authorization_subplan_policy)(self, identity, &policy_shape.query.table, JZ_PDR_READ, &request.policy);
    const jazz_node_OpenTransaction *otx;
    e = jazz_node__node__NodeState__open_tx(self, tx_id, &otx);
    if (JZ_IS_ERR(e)) goto out_req;
    request.authorization_mode = JZ_QAM_TRUSTED_SERVING;
    request.reads = jazz_node__node__query_eval__query_read_sets__tx_query_read_set(&request.shape, policy_shape.schema_version, tx_id, &otx->base_snapshot);
    e = LOWER(current_query_output_request)(JZ_CQPO_POLICY_PREDICATE, &policy_shape.query, &request.output);
    if (JZ_IS_ERR(e)) goto out_req;
    jz_btree access_paths = primary_key_access_paths(&policy_shape.query.table, row_uuid);
    jazz_engine_QueryProgram *program;
    e = LOWER(NodeState__compile_query_program_request_with_access_paths)(self, request, access_paths, &program);
    if (JZ_IS_ERR(e)) goto out_shape;
    e = AUTHZ(NodeState__write_policy_query_program_allows)(self, program, &policy_shape, &binding, out);
    jz_rc_release(program);
    goto out_shape;
out_req:
    jazz_engine_QueryProgramRequest_drop(&request);
out_shape:
    jazz_model_ValidatedQuery_drop(&policy_shape);
    jazz_model_Binding_drop(&binding);
out_schema:
    jazz_model_JazzSchema_drop(&policy_schema);
    return e;
}

jz_err AUTHZ(NodeState__policy_authorization_row_id_graph)(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, jazz_node_PolicyAuthorizationGraph *out)
{
    /* query_engine_read_metrics.policy_authorization_graphs += 1 */
    ((u64 *)self->query_engine_read_metrics)[0] += 1;
    jazz_node_PolicyAuthorizationGraphCacheKey cache_key = LOWER(policy_authorization_graph_cache_key)(&request);
    bool hit = LOWER(NodeState__policy_authorization_graph_cache_get)(self, &cache_key, out);
    jz_bytes_drop(&cache_key.key);
    if (hit) { jazz_engine_QueryProgramRequest_drop(&request); return JZ_OK; }
    jazz_engine_SourceId none;
    memset(&none, 0, sizeof none);
    return AUTHZ(NodeState__policy_authorization_row_id_graph_inner)(self, request, NULL, true, false, none, NULL, out);
}

jz_err AUTHZ(NodeState__point_policy_authorization_row_id_graph)(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, jz_btree access_paths, bool has_bounded, jazz_engine_SourceId bounded_source, groove_GraphBuilder *bounded_graph, jazz_node_PolicyAuthorizationGraph *out)
{
    ((u64 *)self->query_engine_read_metrics)[0] += 1;
    jz_err e = AUTHZ(NodeState__policy_authorization_row_id_graph_inner)(self, request, &access_paths, false, has_bounded, bounded_source, bounded_graph, out);
    jz_btree_drop(&access_paths);
    return e;
}

static void lease_drop(void *p) { (void)p; }

static jz_err AUTHZ(NodeState__policy_authorization_row_id_graph_inner)(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, const jz_btree *forced_access_paths, bool cache, bool has_bounded, jazz_engine_SourceId bounded_source, groove_GraphBuilder *bounded_graph, jazz_node_PolicyAuthorizationGraph *out)
{
    jazz_node_PolicyAuthorizationGraphCacheKey cache_key = LOWER(policy_authorization_graph_cache_key)(&request);
    LOWER(NodeState__restore_expired_policy_compilation_state)(self);
    bool has_proof_table = request.policy.tag == JZ_PC_AUTHORIZATION_SUBPLAN;
    jz_str proof_table = has_proof_table ? jz_str_clone(&request.policy.protected_source.table) : (jz_str){0};
    void *proof_lease = has_proof_table ? jz_rc_new(1, lease_drop) : NULL; /* Rc<()> */
    jz_err e = JZ_OK;
    if (has_proof_table) {
        u32 depth = self->query.policy_proof_stack.len;
        JZ_VEC_FOR(jazz_node_PolicyProofStackEntry, active, self->query.policy_proof_stack) {
            if (jz_str_eq(&active->table, &proof_table)) {
                e = jz_err_fmt(JZN_E_PolicyProofCycle, "PolicyProofCycle: table '%.*s' re-entered at depth %u",
                               (int)proof_table.len, proof_table.ptr, depth);
                jz_str_drop(&proof_table);
                jz_rc_release(proof_lease);
                jz_bytes_drop(&cache_key.key);
                jazz_engine_QueryProgramRequest_drop(&request);
                return e;
            }
        }
        if (!proof_lease) jz_panic("proof table owns a cancellation lease");
        jazz_node_PolicyProofStackEntry entry = {jz_str_clone(&proof_table), proof_lease /* Rc::downgrade */};
        JZ_VEC_PUSH(self->query.policy_proof_stack, entry);
    }

    /* let result = async { ... }.await; */
    jz_btree access_paths;
    if (forced_access_paths) {
        jz_btree_clone(&access_paths, forced_access_paths, NULL, NULL);
    } else {
        /* Cached authorization dependencies deliberately do not carry secondary
         * index prefixes resolved from this identity's claims. Their cache key
         * describes claim shape, not values. */
        e = LOWER(NodeState__query_program_access_paths)(self, &request, false, &access_paths);
        if (JZ_IS_ERR(e)) { jazz_engine_QueryProgramRequest_drop(&request); goto pop; }
    }
    jazz_engine_QueryProgram *program;
    jz_btree ap_clone;
    jz_btree_clone(&ap_clone, &access_paths, NULL, NULL);
    if (cache)
        e = LOWER(NodeState__compile_query_program_request_with_access_paths)(self, request, ap_clone, &program);
    else
        e = LOWER(NodeState__compile_query_program_request_with_shared_access_paths)(self, request, ap_clone, has_bounded, bounded_source, bounded_graph, &program);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&access_paths); goto pop; }
    groove_GraphBuilder *graph;
    e = LOWER(lowered_terminal_graph)(program, "policy.authorized_rows", &graph);
    if (JZ_IS_ERR(e)) { jz_rc_release(program); jz_btree_drop(&access_paths); goto pop; }
    jz_btree route_fields;
    bool found = false;
    JZ_VEC_FOR(const jazz_engine_LoweredTerminal, terminal, program->lowered.terminals) {
        if (!jz_str_eq_cstr(&terminal->sink, "policy.authorized_rows")) continue;
        if (terminal->output.tag == JZ_OTS_FACT) route_fields = LOWER(output_routing_fields_for_query_eval)(terminal->output.payload);
        else jz_btree_init(&route_fields, &jazz_node_qeb_STR_SET_DESC);
        found = true;
        break;
    }
    if (!found) jz_btree_init(&route_fields, &jazz_node_qeb_STR_SET_DESC);
    jz_rc_release(program);
    out->graph = graph;
    out->route_fields = route_fields;
    out->access_paths = access_paths;
    if (cache) {
        jazz_node_PolicyAuthorizationGraph c;
        jazz_node_PolicyAuthorizationGraph_clone(&c, out);
        jazz_node_PolicyAuthorizationGraph old;
        jazz_node_PolicyAuthorizationGraphCacheKey k = {jz_bytes_clone(&cache_key.key)};
        if (jz_btree_insert(&self->query.policy_authorization_graph_cache, &k, &c, &old)) {
            jazz_node_PolicyAuthorizationGraph_drop(&old);
            jz_bytes_drop(&k.key);
        }
    }
pop:
    if (proof_lease) {
        jazz_node_PolicyProofStackEntry entry;
        if (self->query.policy_proof_stack.len == 0) jz_panic("policy proof stack entry is balanced");
        entry = self->query.policy_proof_stack.ptr[--self->query.policy_proof_stack.len];
        JZ_ASSERT(entry.lease == proof_lease, "Weak::ptr_eq(entry.lease, lease)");
        jz_str_drop(&entry.table);
        jz_rc_release(proof_lease);
    }
    if (has_proof_table) jz_str_drop(&proof_table);
    jz_bytes_drop(&cache_key.key);
    return e;
}

/* The policy context of an authorization subplan protecting `table` for
 * `identity`. System authority keeps its unrestricted context. */
static void AUTHZ(NodeState__authorization_subplan_policy)(const jazz_node_NodeState *self, jazz_types_AuthorSubject identity, const jz_str *table, jazz_engine_PolicyDecisionRole role, jazz_engine_PolicyContext *out)
{
    AUTHZ(NodeState__query_program_policy_context)(self, identity, out);
    if (out->tag != JZ_PC_IDENTITY) return;
    out->tag = JZ_PC_AUTHORIZATION_SUBPLAN;
    out->protected_source = jazz_node__node__query_eval__normalization__root_source_id(table->ptr, table->len);
    out->role = role;
}

/* Inline a policy query's reachable seeds and build its row-set input,
 * returning the bound shape and binding with it. */
static jz_err AUTHZ(NodeState__inline_policy_subplan_input)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *policy_shape_in, const jazz_model_JazzSchema *schema, jazz_model_ValidatedQuery *shape_out, jazz_model_Binding *binding_out, jazz_engine_NormalizedRowSetShape *input_shape, jazz_engine_ProgramBinding *input_binding)
{
    jz_btree empty;
    jz_btree_init(&empty, &jazz_node_qeb_STR_VALUE_DESC);
    jazz_model_Binding policy_binding;
    JZ_TRY(jazz_model__query__validation__ValidatedQuery__bind(policy_shape_in, empty, &policy_binding));
    jazz_model_ValidatedQuery policy_shape;
    jz_err e = BIND(bind_query_params_with_mode)(policy_shape_in, &policy_binding, schema, JZN_PBM_INLINE_ALL_REACHABLE_SEEDS, &policy_shape);
    jazz_model_Binding_drop(&policy_binding);
    JZ_TRY(e);
    jz_btree_init(&empty, &jazz_node_qeb_STR_VALUE_DESC);
    jazz_model_Binding binding;
    e = jazz_model__query__validation__ValidatedQuery__bind(&policy_shape, empty, &binding);
    if (JZ_IS_ERR(e)) { jazz_model_ValidatedQuery_drop(&policy_shape); return e; }
    e = jazz_node__node__query_eval__normalization__NodeState__normalized_row_set_shape(self, &policy_shape, &binding, input_shape);
    if (JZ_IS_ERR(e)) { jazz_model_ValidatedQuery_drop(&policy_shape); jazz_model_Binding_drop(&binding); return e; }
    jz_btree claim_params = BIND(binding_claim_params_for_shape)(input_shape, &policy_shape.params);
    jz_str source_shape;
    bool has_source = jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_parts_if_needed(&policy_shape.params, &claim_params, &source_shape);
    jz_btree extra;
    jz_btree_init(&extra, &jazz_node_qeb_STR_COLUMN_TYPE_DESC);
    *input_binding = BIND(NodeState__program_binding_for_shape)(self, &policy_shape, &binding, has_source ? &source_shape : NULL, extra, claim_params);
    if (has_source) jz_str_drop(&source_shape);
    *shape_out = policy_shape;
    *binding_out = binding;
    return JZ_OK;
}

void AUTHZ(NodeState__query_program_policy_context)(const jazz_node_NodeState *self, jazz_types_AuthorSubject identity, jazz_engine_PolicyContext *out)
{
    memset(out, 0, sizeof *out);
    if (jazz_types_AuthorSubject_is_system(identity)) {
        out->tag = JZ_PC_SYSTEM;
        return;
    }
    jz_btree claims;
    jazz_node__node__default_policy_claim_values(identity, &claims);
    const jz_btree *session = NULL;
    if (self->has_active_session_claims && jazz_types_AuthorSubject_eq(self->active_session_identity, identity))
        session = &self->active_session_claims;
    else
        session = jz_btree_get(&self->session_claims, &identity);
    if (session) {
        jz_btree c;
        jz_btree_clone(&c, session, jazz_node_qeb_str_clone, jazz_node_qeb_value_clone);
        jz_btree_append(&claims, &c);
        jz_btree_drop(&c);
    }
    jz_btree author;
    jazz_model__model__policy_claims__author_policy_claims(identity, &author);
    jz_btree_append(&claims, &author);
    jz_btree_drop(&author);
    out->tag = JZ_PC_IDENTITY;
    out->mode = JZ_PEM_ENFORCING;
    out->permission_subject = identity;
    out->claims = claims;
    out->has_attribution = false;
}

static jz_err write_policy_program_for_request(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, jz_btree access_paths, const jazz_model_ValidatedQuery *policy_shape, const jazz_model_Binding *binding, bool *out)
{
    jazz_engine_QueryProgram *program;
    JZ_TRY(LOWER(NodeState__compile_query_program_request_with_access_paths)(self, request, access_paths, &program));
    jz_err e = AUTHZ(NodeState__write_policy_query_program_allows)(self, program, policy_shape, binding, out);
    jz_rc_release(program);
    return e;
}

jz_err AUTHZ(NodeState__write_policy_query_allows_current_row)(jazz_node_NodeState *self, const jazz_model_Query *policy, jazz_types_RowUuid row_uuid, jazz_types_AuthorSubject identity, bool *out)
{
    jazz_model_ValidatedQuery validated;
    JZ_TRY(jazz_model__query__builders__Query__validate(policy, &self->catalogue.schema, &validated));
    jazz_model_ValidatedQuery policy_shape;
    jazz_model_Binding binding;
    jazz_engine_QueryProgramRequest request;
    memset(&request, 0, sizeof request);
    jz_err e = AUTHZ(NodeState__inline_policy_subplan_input)(self, &validated, &self->catalogue.schema, &policy_shape, &binding, &request.shape, &request.binding);
    jazz_model_ValidatedQuery_drop(&validated);
    JZ_TRY(e);
    AUTHZ(NodeState__authorization_subplan_policy)(self, identity, &policy_shape.query.table, JZ_PDR_WRITE, &request.policy);
    request.authorization_mode = JZ_QAM_TRUSTED_SERVING;
    request.reads = jazz_node__node__query_eval__query_read_sets__current_query_read_set(
        &request.shape, policy_shape.schema_version, policy_shape.schema_version, JZ_DURABILITY_LOCAL, NULL, NULL, false);
    e = LOWER(current_query_output_request)(JZ_CQPO_POLICY_PREDICATE, &policy_shape.query, &request.output);
    if (JZ_IS_ERR(e)) { jazz_engine_QueryProgramRequest_drop(&request); goto out; }
    /* A primary-key access path addresses the physical row UUID without
     * overloading public `id`, which may be a declared user column. */
    e = write_policy_program_for_request(self, request, primary_key_access_paths(&policy_shape.query.table, row_uuid), &policy_shape, &binding, out);
out:
    jazz_model_ValidatedQuery_drop(&policy_shape);
    jazz_model_Binding_drop(&binding);
    return e;
}

static bool schema_has_table(const jazz_model_JazzSchema *s, const jazz_model_TableSchema *table)
{
    JZ_VEC_FOR(const jazz_model_TableSchema, known, s->tables)
        if (jazz_model_TableSchema_eq(known, table)) return true;
    return false;
}

/* Authorize an inline old/candidate row through the query program.
 *
 * Insert candidates reinterpret plain `inherits(parent)` as parent
 * update-using authorization. Existing/update-check rows retain ordinary read
 * inheritance unless the policy names an explicit write operation. */
jz_err AUTHZ(NodeState__write_policy_query_allows_candidate)(jazz_node_NodeState *self, const jazz_model_TableSchema *table, const jazz_model_Query *policy, jazz_types_RowUuid row_uuid, const jz_btree *cells, jazz_types_AuthorSubject identity, bool insert_candidate, bool *out)
{
    jazz_types_SchemaVersionId policy_schema_version;
    if (schema_has_table(&self->catalogue.active_schema.compiled, table)) {
        policy_schema_version = self->catalogue.active_schema.schema;
    } else if (schema_has_table(&self->catalogue.schema, table)) {
        policy_schema_version = self->catalogue.local_schema_version_id;
    } else {
        policy_schema_version = self->catalogue.local_schema_version_id;
        const jazz_types_SchemaVersionId *sv;
        const jazz_protocol_SchemaVersion *payload;
        JZ_BTREE_FOR(&self->catalogue.catalogue_schemas, sv, payload) {
            if (schema_has_table(&payload->schema, table)) { policy_schema_version = *sv; break; }
        }
    }
    return AUTHZ(NodeState__write_policy_query_allows_candidate_for_schema)(self, policy_schema_version, table, policy, row_uuid, cells, identity, insert_candidate, out);
}

jz_err AUTHZ(NodeState__write_policy_query_allows_candidate_for_schema)(jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jazz_model_TableSchema *table, const jazz_model_Query *policy, jazz_types_RowUuid row_uuid, const jz_btree *cells, jazz_types_AuthorSubject identity, bool insert_candidate, bool *out)
{
    jazz_model_RowProvenance provenance = {identity, 0, identity, 0};
    return AUTHZ(NodeState__write_policy_query_allows_candidate_with_provenance_for_schema)(self, v, table, policy, row_uuid, cells, identity, insert_candidate, provenance, out);
}

static jz_err AUTHZ(NodeState__policy_query_allows_candidate_with_provenance_for_schema)(jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jazz_model_TableSchema *table, const jazz_model_Query *policy, jazz_types_RowUuid row_uuid, const jz_btree *cells, jazz_types_AuthorSubject identity, bool insert_candidate, jazz_model_RowProvenance provenance, jazz_engine_PolicyDecisionRole role, const jazz_node_TransactionWriteOverlay *transaction_overlay, bool *out);

/* Authorize an inline candidate with the provenance the pending version will
 * expose if accepted. Public provenance policies must evaluate this metadata
 * exactly like an ordinary current-row source; synthesizing it from the
 * permission subject would let an updater appear to be the original creator. */
jz_err AUTHZ(NodeState__write_policy_query_allows_candidate_with_provenance_for_schema)(jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jazz_model_TableSchema *table, const jazz_model_Query *policy, jazz_types_RowUuid row_uuid, const jz_btree *cells, jazz_types_AuthorSubject identity, bool insert_candidate, jazz_model_RowProvenance provenance, bool *out)
{
    jazz_node_TransactionWriteOverlay overlay = jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__default();
    jz_err e = AUTHZ(NodeState__policy_query_allows_candidate_with_provenance_for_schema)(self, v, table, policy, row_uuid, cells, identity, insert_candidate, provenance, JZ_PDR_WRITE, &overlay, out);
    jazz_node_TransactionWriteOverlay_drop(&overlay);
    return e;
}

/* Authorize an inline candidate whose policy evidence is committed state
 * overlaid with its own transaction's other writes (`INV-RLS-9`). With an
 * empty overlay this is exactly
 * `write_policy_query_allows_candidate_with_provenance_for_schema`. */
jz_err AUTHZ(NodeState__write_policy_query_allows_candidate_over_transaction)(jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jazz_model_TableSchema *table, const jazz_model_Query *policy, jazz_types_RowUuid row_uuid, const jz_btree *cells, jazz_types_AuthorSubject identity, bool insert_candidate, jazz_model_RowProvenance provenance, const jazz_node_TransactionWriteOverlay *overlay, bool *out)
{
    return AUTHZ(NodeState__policy_query_allows_candidate_with_provenance_for_schema)(self, v, table, policy, row_uuid, cells, identity, insert_candidate, provenance, JZ_PDR_WRITE, overlay, out);
}

/* Authorize read-for-write visibility of a row a commit unit acts on. Only a
 * commit unit's decision asks this, so its evidence is authority-accepted
 * state (`INV-RLS-9`). */
jz_err AUTHZ(NodeState__read_policy_query_allows_candidate_with_provenance_for_schema)(jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jazz_model_TableSchema *table, const jazz_model_Query *policy, jazz_types_RowUuid row_uuid, const jz_btree *cells, jazz_types_AuthorSubject identity, jazz_model_RowProvenance provenance, bool *out)
{
    jazz_node_TransactionWriteOverlay overlay = jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__accepted_state();
    jz_err e = AUTHZ(NodeState__policy_query_allows_candidate_with_provenance_for_schema)(self, v, table, policy, row_uuid, cells, identity, false, provenance, JZ_PDR_READ, &overlay, out);
    jazz_node_TransactionWriteOverlay_drop(&overlay);
    return e;
}

static jz_err AUTHZ(NodeState__policy_query_allows_candidate_with_provenance_for_schema)(jazz_node_NodeState *self, jazz_types_SchemaVersionId policy_schema_version, const jazz_model_TableSchema *table, const jazz_model_Query *policy_in, jazz_types_RowUuid row_uuid, const jz_btree *cells, jazz_types_AuthorSubject identity, bool insert_candidate, jazz_model_RowProvenance provenance, jazz_engine_PolicyDecisionRole role, const jazz_node_TransactionWriteOverlay *transaction_overlay, bool *out)
{
    jazz_model_Query policy;
    jazz_model_Query_clone(&policy, policy_in);
    if (insert_candidate) {
        JZ_VEC_FOR(jazz_model_InheritsVia, inherits, policy.inherits)
            if (inherits->operation == JZ_INH_SELECT) inherits->operation = JZ_INH_UPDATE;
        JZ_VEC_FOR(jazz_model_PolicyBranch, branch, policy.policy_branches)
            JZ_VEC_FOR(jazz_model_InheritsVia, inherits, branch->inherits)
                if (inherits->operation == JZ_INH_SELECT) inherits->operation = JZ_INH_UPDATE;
    }
    jz_err e;
    const jazz_model_JazzSchema *policy_schema = jazz_node__node__SchemaCatalogue__schema_for_version(&self->catalogue, policy_schema_version);
    if (!policy_schema) { jazz_model_Query_drop(&policy); return JZN_INVALID_STORED("policy schema payload missing"); }
    jazz_model_ValidatedQuery validated;
    e = jazz_model__query__builders__Query__validate_with_schema_version(&policy, policy_schema, policy_schema_version, &validated);
    jazz_model_Query_drop(&policy);
    JZ_TRY(e);
    jazz_model_ValidatedQuery policy_shape;
    jazz_model_Binding binding;
    jazz_engine_QueryProgramRequest request;
    memset(&request, 0, sizeof request);
    e = AUTHZ(NodeState__inline_policy_subplan_input)(self, &validated, policy_schema, &policy_shape, &binding, &request.shape, &request.binding);
    jazz_model_ValidatedQuery_drop(&validated);
    JZ_TRY(e);
    jazz_engine_SourceId root_source = jazz_node__node__query_eval__normalization__root_source_id(policy_shape.query.table.ptr, policy_shape.query.table.len);
    AUTHZ(NodeState__authorization_subplan_policy)(self, identity, &policy_shape.query.table, role, &request.policy);
    request.authorization_mode = JZ_QAM_TRUSTED_SERVING;
    request.reads = jazz_node__node__query_eval__query_read_sets__current_query_read_set(
        &request.shape, policy_shape.schema_version, policy_shape.schema_version,
        jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__evidence_tier(transaction_overlay), NULL, NULL, false);
    e = LOWER(current_query_output_request)(JZ_CQPO_POLICY_PREDICATE, &policy_shape.query, &request.output);
    if (JZ_IS_ERR(e)) goto fail_req;
    jazz_node_CurrentRow candidate;
    e = jazz_node__node__current_row_from_cells_with_explicit_provenance(table, row_uuid, cells, provenance, NULL, &candidate);
    if (JZ_IS_ERR(e)) goto fail_req;
    jz_btree inline_sources;
    jz_btree_init(&inline_sources, &jazz_node_INLINE_SOURCES_DESC);
    jazz_node_CurrentRowVec rows = {0};
    JZ_VEC_PUSH(rows, candidate);
    jz_btree_insert(&inline_sources, &root_source, &rows, NULL);
    memset(&root_source, 0, sizeof root_source);
    jz_btree access_paths;
    e = LOWER(NodeState__current_query_primary_key_access_paths)(self, &policy_shape, &binding, &access_paths);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&inline_sources); goto fail_req; }
    jazz_engine_QueryProgram *program;
    if (!jazz_node__node__query_eval__read_sources__TransactionWriteOverlay__is_active(transaction_overlay)) {
        e = LOWER(NodeState__compile_query_program_request_with_inline_sources_and_access_paths)(self, request, inline_sources, access_paths, &program);
    } else {
        jazz_node_TransactionWriteOverlay overlay;
        jazz_node_TransactionWriteOverlay_clone(&overlay, transaction_overlay);
        e = LOWER(NodeState__compile_query_program_request_with_inline_sources_and_transaction_overlay)(self, request, inline_sources, access_paths, overlay, &program);
    }
    if (!JZ_IS_ERR(e)) {
        e = AUTHZ(NodeState__write_policy_query_program_allows)(self, program, &policy_shape, &binding, out);
        jz_rc_release(program);
    }
    goto out;
fail_req:
    jazz_engine_QueryProgramRequest_drop(&request);
    jazz_engine_SourceId_drop(&root_source);
out:
    jazz_model_ValidatedQuery_drop(&policy_shape);
    jazz_model_Binding_drop(&binding);
    return e;
}

static jz_err AUTHZ(NodeState__write_policy_query_program_allows)(jazz_node_NodeState *self, const jazz_engine_QueryProgram *program, const jazz_model_ValidatedQuery *policy_shape, const jazz_model_Binding *binding, bool *out)
{
    jazz_node_PreparedQueryPlan plan;
    JZ_TRY(LOWER(NodeState__prepared_query_plan_from_program)(self, program, policy_shape, binding, &plan));
    groove_RecordDeltas *deltas;
    jz_err e = JZ_OK;
    switch (plan.tag) {
    case JZN_PQP_GRAPH:
        e = groove__db__query__Database__query_graph(self->database, plan.graph, &deltas);
        plan.graph = NULL;
        if (JZ_IS_ERR(e)) e = jazz_node__node__Error_groove(e);
        break;
    case JZN_PQP_PREPARED: {
        groove_ValueVec values;
        e = jazz_node__node__query_eval__prepared_bindings__binding_values_for_plan(binding, plan.params.ptr, plan.params.len, &program->request.policy, JZN_PCBM_STRICT, &values);
        if (JZ_IS_ERR(e)) break;
        void *snapshot;
        e = jazz_node__node__prepared__NodeState__bind_shape_snapshot(self, plan.shape, &values, &snapshot);
        jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop);
        if (JZ_IS_ERR(e)) break;
        e = jazz_node__node__query_eval__take_required_sink_deltas(snapshot, jazz_engine__query_engine__JAZZ_APP_ROWS_SINK, &deltas);
        break;
    }
    case JZN_PQP_PEER_MAINTAINED_MARKER:
        e = JZN_INVALID_STORED("peer maintained marker cannot execute write policy plan");
        break;
    }
    jazz_node_PreparedQueryPlan_drop(&plan);
    JZ_TRY(e);
    *out = false;
    groove_RecordDeltasIter it = {deltas, 0};
    groove_BorrowedRecord record;
    i64 weight;
    while (groove__ivm__RecordDeltas__next(&it, &record, &weight))
        if (weight > 0) { *out = true; break; }
    groove_RecordDeltas_drop(deltas);
    return JZ_OK;
}

/* Evaluate a validated query shape against this node's local knowledge.
 *
 * Phase B step 2 returns output-relation rows only. Provenance-closure
 * shipping and settled result set reads are introduced by the wire step. */
jz_err AUTHZ(NodeState__query_rows)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_model_DurabilityTier tier, jazz_node_CurrentRowVec *out)
{
    JZ_TRY(jazz_node__node__NodeState__require_catalogue_ready(self));
    return jazz_node__node__query_eval__NodeState__query_rows_with_prepared_plan(self, shape, binding, tier, NULL, out);
}

typedef JZ_VEC(groove_ProjectField) ProjectFieldVec;
static void pf_vec_drop(ProjectFieldVec *v) { jz_vec_drop(v, sizeof(groove_ProjectField), groove_ProjectField_drop); }
static void push_renamed_right(ProjectFieldVec *v, const jz_str *field)
{
    char *name = cstrs(field, 1)[0];
    JZ_VEC_PUSH(*v, groove__ivm__graph__ProjectField__renamed(jazz_engine__query_engine__fields__right_field(field), name));
    jz_free(name);
}
static void push_renamed_left(ProjectFieldVec *v, const jz_str *field)
{
    char **n = cstrs(field, 1);
    JZ_VEC_PUSH(*v, groove__ivm__graph__ProjectField__renamed(jazz_engine__query_engine__fields__left_field(n[0]), n[0]));
    str_vec_free_c(n, 1);
}

typedef struct {
    groove_GraphBuilder *binding;
    const ProjectFieldVec *output_fields;
    const jz_btree *binding_route_fields;
} WithBindingCtx;

static bool str_slice_contains(const jz_str *v, u32 n, const jz_str *s)
{
    for (u32 i = 0; i < n; i++) if (jz_str_eq(&v[i], s)) return true;
    return false;
}

static groove_GraphBuilder *with_binding(const WithBindingCtx *c, groove_GraphBuilder *graph, const jz_str *keys, u32 nkeys, const jz_str *carried, u32 ncarried)
{
    char **k = cstrs(keys, nkeys);
    groove_GraphBuilder *joined = groove__ivm__graph__GraphBuilder__join(graph, groove_GraphBuilder_clone(c->binding), (const char *const *)k, nkeys, (const char *const *)k, nkeys);
    str_vec_free_c(k, nkeys);
    ProjectFieldVec fields = {0};
    JZ_VEC_FOR(const groove_ProjectField, field, *c->output_fields) {
        groove_ProjectField f;
        groove_ProjectField_clone(&f, field);
        char **n = cstrs(&field->output_name, 1);
        jz_str_drop(&f.expression.field.name);
        f.expression.tag = GPE_FIELD;
        f.expression.field = groove__ivm__graph__FieldRef__stored_name(jazz_engine__query_engine__fields__left_field(n[0]));
        str_vec_free_c(n, 1);
        JZ_VEC_PUSH(fields, f);
    }
    for (u32 i = 0; i < ncarried; i++) push_renamed_left(&fields, &carried[i]);
    const jz_str *field;
    const void *v;
    JZ_BTREE_FOR(c->binding_route_fields, field, v) {
        (void)v;
        if (str_slice_contains(carried, ncarried, field)) continue;
        push_renamed_right(&fields, field);
    }
    groove_GraphBuilder *out = groove__ivm__graph__GraphBuilder__project_fields(joined, fields.ptr, fields.len);
    jz_vec_drop(&fields, sizeof(groove_ProjectField), NULL);
    return out;
}

jz_err AUTHZ(NodeState__compose_policy_filtered_current_source_graph)(jazz_node_NodeState *self, jz_err policy_request_err, jazz_engine_QueryProgramRequest policy_request, groove_GraphBuilder *base, const jz_str *output_field_names, u32 n, jazz_node_PolicyAuthorizationGraph *out)
{
    /* query_engine_read_metrics.policy_authorized_source_joins += 1 */
    ((u64 *)self->query_engine_read_metrics)[1] += 1;
    /* Policy proof joins restore the protected source's exact descriptor. The
     * join's prefixed carrier names are execution plumbing, not new
     * application identities. */
    jz_err e;
    groove_RecordDescriptor *descriptor;
    e = groove__db__query__Database__graph_output_descriptor(self->database, base, &descriptor);
    if (JZ_IS_ERR(e)) goto fail_input;
    ProjectFieldVec output_fields = {0};
    u32 nf;
    const groove_DescriptorField *dfields = groove__records__descriptor__RecordDescriptor__fields(descriptor, &nf);
    for (u32 i = 0; i < n; i++) {
        const groove_DescriptorField *field = NULL;
        for (u32 j = 0; j < nf; j++)
            if (dfields[j].has_name && jz_str_eq(&dfields[j].name, &output_field_names[i])) { field = &dfields[j]; break; }
        if (!field) { e = JZN_INVALID_STORED("policy source output carrier is missing"); break; }
        if (!field->has_identity) { e = JZN_INVALID_STORED("policy source output identity is missing"); break; }
        groove_ProjectField f;
        memset(&f, 0, sizeof f);
        char **nm = cstrs(&output_field_names[i], 1);
        f.expression.tag = GPE_FIELD;
        f.expression.field = groove__ivm__graph__FieldRef__stored_name(jazz_engine__query_engine__fields__left_field(nm[0]));
        str_vec_free_c(nm, 1);
        f.output_name = jz_str_clone(&output_field_names[i]);
        f.output_identity = jz_memdup(&field->identity, sizeof field->identity);
        JZ_VEC_PUSH(output_fields, f);
    }
    groove_RecordDescriptor_drop(descriptor);
    if (JZ_IS_ERR(e)) { pf_vec_drop(&output_fields); goto fail_input; }
    if (JZ_IS_ERR(policy_request_err)) {
        jz_err err = policy_request_err;
        if (err.code == JZN_E_QueryCapability && err.msg && str_contains(err.msg, "PolicyProofCycle")) {
            pf_vec_drop(&output_fields);
            groove_GraphBuilder_drop(base);
            return err;
        }
        if (err.code == JZN_E_QueryCapability) {
            jz_err_drop(&err);
            *out = jazz_node__node__query_eval__authorization__empty_policy_filtered_current_source_graph(base, output_fields.ptr, output_fields.len);
            pf_vec_drop(&output_fields);
            return JZ_OK;
        }
        pf_vec_drop(&output_fields);
        groove_GraphBuilder_drop(base);
        return err;
    }
    /* The protected storage source has no binding fields of its own, but the
     * authorization proof is routed by the enclosing prepared binding. Carry
     * that descriptor alongside the source before joining the proof so a
     * later storage delta has every route field the proof advertises. */
    const jazz_engine_ProgramBinding *pb = &policy_request.binding;
    bool has_binding_routes = pb->has_source_shape;
    groove_GraphBuilder *binding_graph = NULL;
    jz_btree binding_route_fields;
    jz_btree_init(&binding_route_fields, &jazz_node_qeb_STR_SET_DESC);
    if (has_binding_routes) {
        JZ_VEC(groove_FieldSpecNamed) specs = {0};
        const jz_str *name;
        const groove_ColumnType *ty;
        JZ_BTREE_FOR(&pb->param_types, name, ty) {
            groove_FieldSpecNamed s = {jz_str_clone(name), *ty};
            JZ_VEC_PUSH(specs, s);
            jz_str c = jz_str_clone(name);
            if (!jz_btree_insert(&binding_route_fields, &c, NULL, NULL)) {} else jz_str_drop(&c);
        }
        const jazz_engine_ProgramClaimParam *claim;
        JZ_BTREE_FOR(&pb->claim_params, name, claim) {
            groove_FieldSpecNamed s = {jz_str_clone(name), claim->ty};
            JZ_VEC_PUSH(specs, s);
            jz_str c = jz_str_clone(name);
            if (!jz_btree_insert(&binding_route_fields, &c, NULL, NULL)) {} else jz_str_drop(&c);
        }
        groove_RecordDescriptor *d = groove__records__descriptor__RecordDescriptor__new_named(specs.ptr, specs.len);
        JZ_VEC_FOR(groove_FieldSpecNamed, s, specs) jz_str_drop(&s->name);
        jz_vec_drop(&specs, sizeof(groove_FieldSpecNamed), NULL);
        binding_graph = groove__ivm__graph__GraphBuilder__binding_source(&pb->source_shape, d);
    }
    jazz_node_PolicyAuthorizationGraphCacheKey cache_key = LOWER(policy_authorization_graph_cache_key)(&policy_request);
    jazz_node_PolicyAuthorizationGraph authorized;
    bool hit = LOWER(NodeState__policy_authorization_graph_cache_get)(self, &cache_key, &authorized);
    jz_bytes_drop(&cache_key.key);
    jazz_engine_QueryProgramRequest_drop(&policy_request);
    if (!hit) {
        e = JZN_INVALID_STORED("policy dependency was not prepared before source composition");
        if (binding_graph) groove_GraphBuilder_drop(binding_graph);
        jz_btree_drop(&binding_route_fields);
        pf_vec_drop(&output_fields);
        groove_GraphBuilder_drop(base);
        return e;
    }
    /* Authorization is existential per protected row and binding route:
     * multiple policy branches or multiple qualifying grant rows are
     * alternative proofs, not additional copies of the application row.
     * Collapse those proofs before the protected source reaches ordinary
     * relational operators (especially finite TopBy windows). Route fields are
     * part of the key because one prepared authorization graph can serve
     * several independently routed bindings. */
    jz_str_vec authorization_keys = {0};
    JZ_VEC_PUSH(authorization_keys, lit_str("row_uuid"));
    {
        const jz_str *f;
        const void *v;
        JZ_BTREE_FOR(&authorized.route_fields, f, v) { (void)v; JZ_VEC_PUSH(authorization_keys, jz_str_clone(f)); }
    }
    char **ak = cstrs(authorization_keys.ptr, authorization_keys.len);
    groove_GraphBuilder *authorized_graph = groove__ivm__graph__GraphBuilder__arg_max_by(authorized.graph, (const char *const *)ak, authorization_keys.len, (const char *const *)ak, authorization_keys.len);
    authorized.graph = NULL;
    /* join_keys == authorization_keys */
    u32 nj = authorization_keys.len;
    memset(out, 0, sizeof *out);
    jz_btree_init(&out->access_paths, &jazz_node_ACCESS_PATHS_DESC);
    if (!has_binding_routes) {
        ProjectFieldVec fields;
        jz_vec_clone(&fields, &output_fields, sizeof(groove_ProjectField), (void (*)(void *, const void *))groove_ProjectField_clone);
        const jz_str *f;
        const void *v;
        JZ_BTREE_FOR(&authorized.route_fields, f, v) { (void)v; push_renamed_right(&fields, f); }
        groove_GraphBuilder *joined = groove__ivm__graph__GraphBuilder__join(base, authorized_graph, (const char *const *)ak, nj, (const char *const *)ak, nj);
        out->graph = groove__ivm__graph__GraphBuilder__project_fields(joined, fields.ptr, fields.len);
        jz_vec_drop(&fields, sizeof(groove_ProjectField), NULL);
        jz_btree_init(&out->route_fields, &jazz_node_qeb_STR_SET_DESC);
        goto done;
    }
    WithBindingCtx wctx = {binding_graph, &output_fields, &binding_route_fields};
    if (authorized.route_fields.len == 0) {
        /* The proof admits a row for every binding alike; pair it with each
         * binding so the source carries the enclosing slots. */
        ProjectFieldVec fields;
        jz_vec_clone(&fields, &output_fields, sizeof(groove_ProjectField), (void (*)(void *, const void *))groove_ProjectField_clone);
        groove_GraphBuilder *joined = groove__ivm__graph__GraphBuilder__join(base, authorized_graph, (const char *const *)ak, nj, (const char *const *)ak, nj);
        groove_GraphBuilder *authorized_rows = groove__ivm__graph__GraphBuilder__project_fields(joined, fields.ptr, fields.len);
        jz_vec_drop(&fields, sizeof(groove_ProjectField), NULL);
        out->graph = with_binding(&wctx, authorized_rows, NULL, 0, NULL, 0);
        out->route_fields = binding_route_fields;
        jz_btree_init(&binding_route_fields, &jazz_node_qeb_STR_SET_DESC);
        goto done;
    }
    /* Take the policy's own routes from its proof first, then attach the
     * enclosing binding's other slots only to bindings with those route
     * values. Pairing every source row with every live binding before the
     * proof would hold a copy of the whole source per binding in a graph that
     * many bindings share. */
    {
        jz_str_vec proof_routes = {0};
        const jz_str *f;
        const void *v;
        JZ_BTREE_FOR(&authorized.route_fields, f, v) { (void)v; JZ_VEC_PUSH(proof_routes, jz_str_clone(f)); }
        ProjectFieldVec fields;
        jz_vec_clone(&fields, &output_fields, sizeof(groove_ProjectField), (void (*)(void *, const void *))groove_ProjectField_clone);
        JZ_VEC_FOR(jz_str, r, proof_routes) push_renamed_right(&fields, r);
        static const char *const row_uuid[] = {"row_uuid"};
        groove_GraphBuilder *joined = groove__ivm__graph__GraphBuilder__join(base, authorized_graph, row_uuid, 1, row_uuid, 1);
        groove_GraphBuilder *authorized_rows = groove__ivm__graph__GraphBuilder__project_fields(joined, fields.ptr, fields.len);
        jz_vec_drop(&fields, sizeof(groove_ProjectField), NULL);
        jz_str_vec binding_keys = {0};
        JZ_VEC_FOR(jz_str, r, proof_routes)
            if (jz_btree_contains(&binding_route_fields, r)) JZ_VEC_PUSH(binding_keys, jz_str_clone(r));
        jz_btree route_fields;
        jz_btree_clone(&route_fields, &binding_route_fields, jazz_node_qeb_str_clone, NULL);
        JZ_VEC_FOR(jz_str, r, proof_routes) {
            jz_str c = jz_str_clone(r);
            if (jz_btree_contains(&route_fields, &c)) jz_str_drop(&c);
            else jz_btree_insert(&route_fields, &c, NULL, NULL);
        }
        bool is_subset = true;
        JZ_BTREE_FOR(&binding_route_fields, f, v) { (void)v; if (!jz_btree_contains(&authorized.route_fields, f)) { is_subset = false; break; } }
        if (is_subset) out->graph = authorized_rows;
        else out->graph = with_binding(&wctx, authorized_rows, binding_keys.ptr, binding_keys.len, proof_routes.ptr, proof_routes.len);
        out->route_fields = route_fields;
        jz_str_vec_drop(&binding_keys);
        jz_str_vec_drop(&proof_routes);
    }
done:
    str_vec_free_c(ak, authorization_keys.len);
    jz_str_vec_drop(&authorization_keys);
    jazz_node_PolicyAuthorizationGraph_drop(&authorized);
    if (binding_graph) groove_GraphBuilder_drop(binding_graph);
    jz_btree_drop(&binding_route_fields);
    pf_vec_drop(&output_fields);
    return JZ_OK;
fail_input:
    if (JZ_IS_ERR(policy_request_err)) jz_err_drop(&policy_request_err);
    else jazz_engine_QueryProgramRequest_drop(&policy_request);
    groove_GraphBuilder_drop(base);
    return e;
}

static jz_err AUTHZ(NodeState__table_read_policy_authorization_request_with_root_visibility)(jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jz_str *table_name, jazz_types_AuthorSubject identity, jazz_node_ParamBindingMode mode, jazz_model_DurabilityTier tier, const jz_str *binding_source_shape, jz_btree binding_user_params, jz_btree binding_claim_params, bool include_deleted_root, jazz_engine_QueryProgramRequest *out);

jz_err AUTHZ(NodeState__table_read_policy_authorization_request)(jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jz_str *table_name, jazz_types_AuthorSubject identity, jazz_node_ParamBindingMode mode, jazz_model_DurabilityTier tier, const jz_str *binding_source_shape, jz_btree binding_user_params, jz_btree binding_claim_params, jazz_engine_QueryProgramRequest *out)
{
    return AUTHZ(NodeState__table_read_policy_authorization_request_with_root_visibility)(self, v, table_name, identity, mode, tier, binding_source_shape, binding_user_params, binding_claim_params, false, out);
}

static jz_err find_policy_table(const jazz_model_JazzSchema *policy_schema, const jz_str *table_name, const jazz_model_TableSchema **out)
{
    JZ_VEC_FOR(const jazz_model_TableSchema, candidate, policy_schema->tables)
        if (jz_str_eq(&candidate->name, table_name)) { *out = candidate; return JZ_OK; }
    return jz_err_fmt(JZN_E_TableNotFound, "table not found: %.*s", (int)table_name->len, table_name->ptr);
}

static void merge_claim_params(jz_btree *into, jz_btree from)
{
    jz_str k;
    jazz_engine_ProgramClaimParam v;
    while (jz_btree_pop_first(&from, &k, &v)) {
        jz_str ok;
        jazz_engine_ProgramClaimParam ov;
        if (jz_btree_remove(into, &k, &ok, &ov)) { jz_str_drop(&ok); jazz_engine_ProgramClaimParam_drop(&ov); }
        jz_btree_insert(into, &k, &v, NULL);
    }
    jz_btree_drop(&from);
}

jz_err AUTHZ(NodeState__table_read_policy_authorization_request_at)(const jazz_node_NodeState *self, jazz_types_SchemaVersionId policy_schema_version, const jz_str *table_name, jazz_types_AuthorSubject identity, jazz_node_ParamBindingMode mode, jazz_types_GlobalTime position, const jz_str *binding_source_shape, jz_btree binding_user_params, jz_btree binding_claim_params, jazz_engine_QueryProgramRequest *out)
{
    jz_err e;
    const jazz_model_JazzSchema *policy_schema = jazz_node__node__SchemaCatalogue__schema_for_version(&self->catalogue, policy_schema_version);
    if (!policy_schema) { e = JZN_INVALID_STORED("policy schema version is unknown"); goto fail_params; }
    const jazz_model_TableSchema *table;
    if (JZ_IS_ERR(e = find_policy_table(policy_schema, table_name, &table))) goto fail_params;
    /* System authority bypasses the table policy.  Use the unfiltered table
     * query rather than merely dropping prepared claim descriptors: retaining
     * policy claim operands in the shape would still require an identity
     * context when the historical graph is lowered. */
    jazz_model_Query query = jazz_types_AuthorSubject_is_system(identity)
        ? jazz_model__query__builders__Query__from(table->name.ptr, table->name.len)
        : BIND(authorization_query_from_read_policy)(table);
    if (query.includes.len != 0) {
        jazz_model_Query_drop(&query);
        e = JZN_INVALID_STORED("historical policy source filters do not support include policies");
        goto fail_params;
    }
    jazz_model_ValidatedQuery validated;
    e = jazz_model__query__builders__Query__validate(&query, policy_schema, &validated);
    jazz_model_Query_drop(&query);
    if (JZ_IS_ERR(e)) goto fail_params;
    jz_btree empty;
    jz_btree_init(&empty, &jazz_node_qeb_STR_VALUE_DESC);
    jazz_model_Binding policy_binding;
    e = jazz_model__query__validation__ValidatedQuery__bind(&validated, empty, &policy_binding);
    if (JZ_IS_ERR(e)) { jazz_model_ValidatedQuery_drop(&validated); goto fail_params; }
    jazz_model_ValidatedQuery policy_shape;
    e = BIND(bind_query_params_with_mode)(&validated, &policy_binding, policy_schema, mode, &policy_shape);
    jazz_model_ValidatedQuery_drop(&validated);
    jazz_model_Binding_drop(&policy_binding);
    if (JZ_IS_ERR(e)) goto fail_params;
    if (policy_shape.params.len != 0) {
        jazz_model_ValidatedQuery_drop(&policy_shape);
        e = jazz_node__node__Error_query_capability(jz_str_from("historical policy source filters with runtime parameters must lower through query-engine binding sources"));
        goto fail_params;
    }
    jz_btree_init(&empty, &jazz_node_qeb_STR_VALUE_DESC);
    jazz_model_Binding binding;
    e = jazz_model__query__validation__ValidatedQuery__bind(&policy_shape, empty, &binding);
    if (JZ_IS_ERR(e)) { jazz_model_ValidatedQuery_drop(&policy_shape); goto fail_params; }
    memset(out, 0, sizeof *out);
    e = jazz_node__node__query_eval__normalization__NodeState__normalized_row_set_shape(self, &policy_shape, &binding, &out->shape);
    if (JZ_IS_ERR(e)) goto fail_shape;
    jz_btree claim_params = binding_claim_params;
    jz_btree_init(&binding_claim_params, &jazz_node_qeb_STR_CLAIM_PARAM_DESC);
    merge_claim_params(&claim_params, BIND(binding_claim_params_for_shape)(&out->shape, &policy_shape.params));
    e = BIND(collect_reachable_seed_claim_params)(policy_schema, &policy_shape.query, &claim_params);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&claim_params); jazz_engine_NormalizedRowSetShape_drop(&out->shape); goto fail_shape; }
    jz_str source_shape;
    bool has_source = binding_source_shape != NULL;
    if (has_source) source_shape = jz_str_clone(binding_source_shape);
    else has_source = jazz_node__node__query_eval__prepared_bindings__authorization_binding_source_shape(&policy_shape, &binding_user_params, &claim_params, &source_shape);
    if (has_source) BIND(retarget_binding_value_sources)(&out->shape, &source_shape);
    AUTHZ(NodeState__authorization_subplan_policy)(self, identity, &policy_shape.query.table, JZ_PDR_READ, &out->policy);
    e = BIND(NodeState__program_binding_for_shape_and_policy)(self, &policy_shape, &binding, has_source ? &source_shape : NULL, binding_user_params, claim_params, &out->policy, &out->binding);
    jz_btree_init(&binding_user_params, &jazz_node_qeb_STR_COLUMN_TYPE_DESC);
    if (has_source) jz_str_drop(&source_shape);
    if (JZ_IS_ERR(e)) { jazz_engine_PolicyContext_drop(&out->policy); jazz_engine_NormalizedRowSetShape_drop(&out->shape); goto fail_shape; }
    out->authorization_mode = JZ_QAM_TRUSTED_SERVING;
    out->reads = jazz_node__node__query_eval__query_read_sets__historical_query_read_set(&out->shape, policy_schema_version, position);
    e = LOWER(current_query_output_request)(JZ_CQPO_AUTHORIZED_ROWS, &policy_shape.query, &out->output);
    if (JZ_IS_ERR(e)) jazz_engine_QueryProgramRequest_drop(out);
fail_shape:
    jazz_model_ValidatedQuery_drop(&policy_shape);
    jazz_model_Binding_drop(&binding);
fail_params:
    jz_btree_drop(&binding_user_params);
    jz_btree_drop(&binding_claim_params);
    return e;
}

jz_err AUTHZ(NodeState__table_read_policy_authorization_request_for_include_deleted)(jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jz_str *table_name, jazz_types_AuthorSubject identity, jazz_model_DurabilityTier tier, const jz_str *binding_source_shape, jz_btree binding_user_params, jz_btree binding_claim_params, jazz_engine_QueryProgramRequest *out)
{
    return AUTHZ(NodeState__table_read_policy_authorization_request_with_root_visibility)(self, v, table_name, identity, JZN_PBM_INLINE_ALL_REACHABLE_SEEDS, tier, binding_source_shape, binding_user_params, binding_claim_params, true, out);
}

static jz_err AUTHZ(NodeState__table_read_policy_authorization_request_with_root_visibility)(jazz_node_NodeState *self, jazz_types_SchemaVersionId policy_schema_version, const jz_str *table_name, jazz_types_AuthorSubject identity, jazz_node_ParamBindingMode param_binding_mode, jazz_model_DurabilityTier tier, const jz_str *binding_source_shape, jz_btree binding_user_params, jz_btree binding_claim_params, bool include_deleted_root, jazz_engine_QueryProgramRequest *out)
{
    jz_err e = JZ_OK;
    jazz_node_ReadPolicyAuthorizationRequestCacheKey cache_key;
    memset(&cache_key, 0, sizeof cache_key);
    cache_key.policy_schema_version = policy_schema_version;
    cache_key.table_name = jz_str_clone(table_name);
    cache_key.identity = identity;
    cache_key.param_binding_mode = BIND(ParamBindingMode__cache_key)(param_binding_mode);
    cache_key.tier = tier;
    cache_key.has_binding_source_shape = binding_source_shape != NULL;
    if (binding_source_shape) cache_key.binding_source_shape = jz_str_clone(binding_source_shape);
    cache_key.binding_user_params = BIND(binding_user_params_cache_key)(&binding_user_params);
    cache_key.binding_claim_params = BIND(binding_claim_params_cache_key)(&binding_claim_params);
    if (!jazz_node__node__NodeState__active_session_claim_scope_key(self, identity, &cache_key.active_session_claims))
        cache_key.active_session_claims = (jz_str){0}; /* unwrap_or_default */
    cache_key.include_deleted_root = include_deleted_root;
    const jazz_engine_QueryProgramRequest *cached = jz_btree_get(&self->query.read_policy_authorization_request_cache, &cache_key);
    if (cached) {
        jazz_engine_QueryProgramRequest_clone(out, cached);
        jazz_node_ReadPolicyAuthorizationRequestCacheKey_drop(&cache_key);
        goto fail_params;
    }
    const jazz_model_JazzSchema *policy_schema = jazz_node__node__SchemaCatalogue__schema_for_version(&self->catalogue, policy_schema_version);
    if (!policy_schema) { e = JZN_INVALID_STORED("policy schema version is unknown"); goto fail_key; }
    const jazz_model_TableSchema *table;
    if (JZ_IS_ERR(e = find_policy_table(policy_schema, table_name, &table))) goto fail_key;
    jazz_engine_PolicyContext policy;
    AUTHZ(NodeState__authorization_subplan_policy)(self, identity, table_name, JZ_PDR_READ, &policy);
    /* System authority bypasses the table policy.  Its authorization subplan
     * must therefore describe all rows, not the policy's claim predicates:
     * those operands are invalid without an identity context even if the
     * prepared binding descriptor itself has no claim slots. */
    jazz_model_Query query = jazz_types_AuthorSubject_is_system(identity)
        ? jazz_model__query__builders__Query__from(table->name.ptr, table->name.len)
        : BIND(authorization_query_from_read_policy)(table);
    jz_btree policy_binding_values;
    jz_btree_init(&policy_binding_values, &jazz_node_qeb_STR_VALUE_DESC);
    if (param_binding_mode == JZN_PBM_RETAIN_ALL_PARAMS && policy.tag == JZ_PC_AUTHORIZATION_SUBPLAN)
        BIND(bind_scope_claim_operands)(&query, &policy.claims, &policy_binding_values);
    jazz_model_ValidatedQuery policy_shape;
    jazz_model_Binding binding;
    jz_btree declared_claim_params;
    if (query.includes.len != 0) {
        jazz_model_Query_drop(&query);
        e = JZN_INVALID_STORED("maintained subscription view policy slice does not support include policies");
        goto fail_values;
    }
    e = BIND(disambiguate_policy_claim_params_with_outer_slots)(&query, policy_schema, &policy_binding_values, &binding_claim_params, &declared_claim_params);
    if (JZ_IS_ERR(e)) { jazz_model_Query_drop(&query); goto fail_values; }
    jazz_model_ValidatedQuery validated;
    e = jazz_model__query__builders__Query__validate(&query, policy_schema, &validated);
    jazz_model_Query_drop(&query);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&declared_claim_params); goto fail_values; }
    jazz_node__node__query_eval__prepared_bindings__coerce_binding_values_for_shape(&validated, &policy_binding_values);
    jz_btree vclone;
    jz_btree_clone(&vclone, &policy_binding_values, jazz_node_qeb_str_clone, jazz_node_qeb_value_clone);
    jazz_model_Binding policy_binding;
    e = jazz_model__query__validation__ValidatedQuery__bind(&validated, vclone, &policy_binding);
    if (JZ_IS_ERR(e)) { jazz_model_ValidatedQuery_drop(&validated); jz_btree_drop(&declared_claim_params); goto fail_values; }
    e = BIND(bind_query_params_with_mode)(&validated, &policy_binding, policy_schema, param_binding_mode, &policy_shape);
    jazz_model_ValidatedQuery_drop(&validated);
    jazz_model_Binding_drop(&policy_binding);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&declared_claim_params); goto fail_values; }
    {
        const jz_str *name;
        const void *ty;
        JZ_BTREE_FOR(&policy_shape.params, name, ty) {
            (void)ty;
            if (!jz_btree_contains(&policy_binding_values, name)) {
                e = jazz_node__node__Error_query_capability(jz_str_from("maintained policy source filters with runtime parameters must lower through query-engine binding sources"));
                jazz_model_ValidatedQuery_drop(&policy_shape);
                jz_btree_drop(&declared_claim_params);
                goto fail_values;
            }
        }
    }
    e = jazz_model__query__validation__ValidatedQuery__bind(&policy_shape, policy_binding_values, &binding);
    jz_btree_init(&policy_binding_values, &jazz_node_qeb_STR_VALUE_DESC);
    if (JZ_IS_ERR(e)) { jazz_model_ValidatedQuery_drop(&policy_shape); jz_btree_drop(&declared_claim_params); goto fail_values; }
    jazz_engine_QueryProgramRequest request;
    memset(&request, 0, sizeof request);
    if (include_deleted_root)
        e = jazz_node__node__query_eval__normalization__NodeState__normalized_include_deleted_row_set_shape(self, &policy_shape, &binding, &request.shape);
    else
        e = jazz_node__node__query_eval__normalization__NodeState__normalized_row_set_shape(self, &policy_shape, &binding, &request.shape);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&declared_claim_params); goto fail_shape; }
    jz_btree claim_params = binding_claim_params;
    jz_btree_init(&binding_claim_params, &jazz_node_qeb_STR_CLAIM_PARAM_DESC);
    merge_claim_params(&claim_params, BIND(binding_claim_params_for_shape)(&request.shape, &policy_shape.params));
    merge_claim_params(&claim_params, declared_claim_params);
    e = BIND(collect_reachable_seed_claim_params)(policy_schema, &policy_shape.query, &claim_params);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&claim_params); jazz_engine_NormalizedRowSetShape_drop(&request.shape); goto fail_shape; }
    {
        const jz_str *name;
        jazz_engine_ProgramClaimParam *claim;
        JZ_BTREE_FOR(&claim_params, name, claim) {
            const groove_ColumnType *ty = jz_btree_get(&policy_shape.params, name);
            if (ty) { /* claim.ty = ty.clone() */ jazz_node_qeb_column_type_clone(&claim->ty, ty); }
        }
    }
    jz_str source_shape;
    bool has_source = binding_source_shape != NULL;
    if (has_source) source_shape = jz_str_clone(binding_source_shape);
    else has_source = jazz_node__node__query_eval__prepared_bindings__authorization_binding_source_shape(&policy_shape, &binding_user_params, &claim_params, &source_shape);
    if (has_source) BIND(retarget_binding_value_sources)(&request.shape, &source_shape);
    e = BIND(NodeState__program_binding_for_shape_and_policy)(self, &policy_shape, &binding, has_source ? &source_shape : NULL, binding_user_params, claim_params, &policy, &request.binding);
    jz_btree_init(&binding_user_params, &jazz_node_qeb_STR_COLUMN_TYPE_DESC);
    if (has_source) jz_str_drop(&source_shape);
    if (JZ_IS_ERR(e)) { jazz_engine_NormalizedRowSetShape_drop(&request.shape); goto fail_shape; }
    request.authorization_mode = JZ_QAM_TRUSTED_SERVING;
    request.reads = jazz_node__node__query_eval__query_read_sets__current_query_read_set(&request.shape, policy_schema_version, policy_schema_version, tier, NULL, NULL, false);
    request.policy = policy;
    memset(&policy, 0, sizeof policy);
    e = LOWER(current_query_output_request)(JZ_CQPO_AUTHORIZED_ROWS, &policy_shape.query, &request.output);
    if (JZ_IS_ERR(e)) { jazz_engine_QueryProgramRequest_drop(&request); goto fail_shape; }
    jazz_engine_QueryProgramRequest_clone(out, &request);
    {
        jazz_engine_QueryProgramRequest old;
        jazz_node_ReadPolicyAuthorizationRequestCacheKey oldk;
        if (jz_btree_remove(&self->query.read_policy_authorization_request_cache, &cache_key, &oldk, &old)) {
            jazz_node_ReadPolicyAuthorizationRequestCacheKey_drop(&oldk);
            jazz_engine_QueryProgramRequest_drop(&old);
        }
        jz_btree_insert(&self->query.read_policy_authorization_request_cache, &cache_key, &request, NULL);
        memset(&cache_key, 0, sizeof cache_key);
    }
fail_shape:
    jazz_model_ValidatedQuery_drop(&policy_shape);
    jazz_model_Binding_drop(&binding);
fail_values:
    jz_btree_drop(&policy_binding_values);
    jazz_engine_PolicyContext_drop(&policy);
fail_key:
    jazz_node_ReadPolicyAuthorizationRequestCacheKey_drop(&cache_key);
fail_params:
    jz_btree_drop(&binding_user_params);
    jz_btree_drop(&binding_claim_params);
    return e;
}

static jz_err AUTHZ(NodeState__maintained_view_content_current_with_version_in_schema)(const jazz_node_NodeState *self, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, jazz_types_SchemaVersionId schema_version, groove_GraphBuilder **out);

jz_err AUTHZ(NodeState__maintained_view_content_current_with_version)(const jazz_node_NodeState *self, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, groove_GraphBuilder **out)
{
    jazz_types_SchemaVersionId schema_version = self->catalogue.local_schema_version_id;
    return AUTHZ(NodeState__maintained_view_content_current_with_version_in_schema)(self, table, tier, schema_version, out);
}

typedef struct { const jazz_node_NodeState *self; jazz_types_SchemaVersionId schema_version; const jazz_model_TableSchema *table; char **image; u32 nimage; } LayerCtx;
static jz_err layer_fn(const jazz_node_LayerRef *r, void *ctx, jazz_node_Layer *out)
{
    LayerCtx *c = ctx;
    groove_GraphBuilder *rows;
    JZ_TRY(jazz_node__node__physical__NodeState__physical_current_source_graph(c->self, c->schema_version, &c->table->name, jazz_node__node__layer_stack__LayerRef__physical_class(r), &rows));
    *out = jazz_node__node__layer_stack__Layer__new(groove__ivm__graph__GraphBuilder__project(rows, (const char *const *)c->image, c->nimage));
    return JZ_OK;
}

static jz_err AUTHZ(NodeState__maintained_view_content_current_with_version_in_schema)(const jazz_node_NodeState *self, const jazz_model_TableSchema *table, jazz_model_DurabilityTier tier, jazz_types_SchemaVersionId schema_version, groove_GraphBuilder **out)
{
    jz_str_vec content_fields = jazz_node__node__global_current_storage_fields(table, true, true);
    jz_str_vec image_fields;
    jz_vec_clone(&image_fields, &content_fields, sizeof(jz_str), jazz_node_qeb_str_clone);
    JZ_VEC_PUSH(image_fields, jz_str_from("_deletion"));
    char **image = cstrs(image_fields.ptr, image_fields.len);
    char **content = cstrs(content_fields.ptr, content_fields.len);
    /* Select the winning row image, then drop it when it is deleted. */
    LayerCtx ctx = {self, schema_version, table, image, image_fields.len};
    jazz_node_LayerVec layers;
    jz_err e = jazz_node__node__layer_stack__try_ref_layers(tier, layer_fn, &ctx, &layers);
    if (!JZ_IS_ERR(e)) {
        groove_GraphBuilder *current = jazz_node__node__layer_stack__compose(layers);
        if (tier != JZ_DURABILITY_GLOBAL)
            current = groove__ivm__graph__GraphBuilder__project(current, (const char *const *)image, image_fields.len);
        current = groove__ivm__graph__GraphBuilder__filter(current, jazz_node__node__not_deleted_predicate());
        *out = groove__ivm__graph__GraphBuilder__project(current, (const char *const *)content, content_fields.len);
    }
    str_vec_free_c(image, image_fields.len);
    str_vec_free_c(content, content_fields.len);
    jz_str_vec_drop(&image_fields);
    jz_str_vec_drop(&content_fields);
    return e;
}

/* Pick the policy-owning schema for one authorization-support operation.
 *
 * A newer read-policy revision must not shadow an older write-policy revision
 * (or vice versa): the terminal proof must hydrate precisely the policy clause
 * that admission will evaluate. */
static jazz_types_SchemaVersionId AUTHZ(NodeState__authorization_scope_policy_schema_for_action)(const jazz_node_NodeState *self, const jz_str *table, jazz_protocol_AuthorizationScopeOperation operation)
{
    (void)operation;
    jazz_types_SchemaVersionId write_schema = self->catalogue.active_schema.schema;
    const jazz_model_TableSchema *t;
    jz_err e = jazz_node__node__NodeState__table_in_schema_ref(self, table, write_schema, &t);
    bool has_policy_table = !JZ_IS_ERR(e);
    jz_err_drop(&e);
    return has_policy_table ? write_schema : self->catalogue.local_schema_version_id;
}

/* Compile the exact policy clauses needed for one non-mutating operation.
 * This is intentionally separate from the legacy table-wide write scope:
 * callers are not switched until the receipt transport is negotiated. */
jz_err AUTHZ(NodeState__authorization_support_scope)(const jazz_node_NodeState *self, jazz_types_AuthorSubject writer, const jazz_protocol_PermissionAdviceAction *action, jazz_node_AuthorizationSupportScope *out)
{
    return AUTHZ(NodeState__authorization_support_scope_for_session)(self, writer, jz_btree_get(&self->session_claims, &writer), action, out);
}

/* Compile authorization support from the claims admitted by this exact
 * transport session, never the process-wide last session for its author. */
jz_err AUTHZ(NodeState__authorization_support_scope_for_session)(const jazz_node_NodeState *self, jazz_types_AuthorSubject writer, const jz_btree *claims, const jazz_protocol_PermissionAdviceAction *action, jazz_node_AuthorizationSupportScope *out)
{
    jazz_protocol_AuthorizationScopeOperation operation;
    const jz_str *table_name;
    jazz_node__node__query_eval__authorization__authorization_scope_action(action, &operation, &table_name);
    /* `PermissionAdviceAction` is reconstructed after policy projection, so
     * its table belongs to the policy-owning schema, not necessarily this
     * node's base API schema. In particular, a rename lens can turn an
     * authored `users` version into a `people` policy action while an
     * old-schema subscriber remains live. Resolve both the policy and its
     * support-query schema from that policy view. */
    jazz_types_SchemaVersionId policy_schema_version = AUTHZ(NodeState__authorization_scope_policy_schema_for_action)(self, table_name, operation);
    const jazz_model_JazzSchema *policy_schema;
    if (jz_uuid_eq(&policy_schema_version, &self->catalogue.active_schema.schema)) {
        policy_schema = &self->catalogue.active_schema.compiled;
    } else {
        const jazz_protocol_SchemaVersion *sv = jz_btree_get(&self->catalogue.catalogue_schemas, &policy_schema_version);
        if (!sv) return JZN_INVALID_STORED("authorization policy schema is unknown");
        policy_schema = &sv->schema;
    }
    jazz_model_TableSchema table;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema(self, table_name, policy_schema_version, &table));
    QueryVec policies = jazz_node__node__query_eval__authorization__authorization_row_policy_queries(&table, action);
    jazz_model_TableSchema_drop(&table);
    jz_btree claim_values;
    AUTHZ(permission_scope_claim_values)(writer, claims, &claim_values);
    /* Authorization support is authority-current: historic/branch views and
     * weaker durability tiers cannot vouch for the authoritative edge. */
    jazz_protocol_RegisterShapeOptions options = jazz_protocol_RegisterShapeOptions_default();
    jazz_node_ShapeBindingVec subscriptions = {0};
    jz_err e = JZ_OK;
    JZ_VEC_FOR(jazz_model_Query, policy, policies) {
        jazz_model_Query c;
        jazz_model_Query_clone(&c, policy);
        jazz_node_ShapeBinding sb;
        e = jazz_node__node__query_eval__authorization__compile_permission_scope_policy(c, claims, &claim_values, policy_schema, &sb);
        if (JZ_IS_ERR(e)) break;
        JZ_VEC_PUSH(subscriptions, sb);
    }
    if (JZ_IS_ERR(e)) {
        jz_vec_drop(&subscriptions, sizeof(jazz_node_ShapeBinding), shape_binding_drop);
        goto out;
    }
    /* postcard::to_allocvec(..) cannot fail for these in-memory values; the
     * Rust error strings ("authorization policy serialization failed", ...)
     * are therefore unreachable here. */
    jz_bytes policy_bytes = {0}, claim_bytes = {0}, support_bytes = {0}, action_bytes = {0};
    {
        jz_pc_w w = {&policy_bytes};
        jz_pc_variant(&w, operation);
        jz_pc_seq(&w, policies.len);
        JZ_VEC_FOR(jazz_model_Query, policy, policies) jazz_model_Query_pc_encode(&w, policy);
    }
    { jz_pc_w w = {&claim_bytes}; jazz_protocol_ClaimMap_pc_encode(&w, &claim_values); }
    {
        jz_pc_w w = {&support_bytes};
        jz_pc_variant(&w, operation);
        jazz_protocol_RegisterShapeOptions_pc_encode(&w, &options);
        jz_pc_seq(&w, subscriptions.len);
        JZ_VEC_FOR(jazz_node_ShapeBinding, sb, subscriptions) {
            jazz_model_ShapeId sid = jazz_model__query__validation__ValidatedQuery__shape_id(&sb->shape);
            jazz_model_BindingId bid = jazz_model__query__validation__Binding__binding_id(&sb->binding);
            jz_pc_uuid(&w, &sid);
            jz_pc_uuid(&w, &bid);
        }
    }
    { jz_pc_w w = {&action_bytes}; jazz_protocol_PermissionAdviceAction_pc_encode(&w, action); }
    memset(out, 0, sizeof *out);
    out->operation = jazz_node__node__query_eval__authorization__authorization_operation_key(operation, table_name, action, action_bytes);
    jz_blake3(support_bytes.ptr, support_bytes.len, out->key.support_shape_digest);
    out->key.subject = writer;
    jz_blake3(claim_bytes.ptr, claim_bytes.len, out->key.claims_digest);
    jz_blake3(policy_bytes.ptr, policy_bytes.len, out->key.policy_digest);
    out->options = options;
    memset(&options, 0, sizeof options);
    out->subscriptions = subscriptions;
    jz_bytes_drop(&policy_bytes);
    jz_bytes_drop(&claim_bytes);
    jz_bytes_drop(&support_bytes);
out:
    jazz_protocol_RegisterShapeOptions_drop(&options);
    jz_btree_drop(&claim_values);
    jz_vec_drop(&policies, sizeof(jazz_model_Query), jazz_model_Query_drop);
    return e;
}

void AUTHZ(permission_scope_claim_values)(jazz_types_AuthorSubject writer, const jz_btree *claims, jz_btree *out)
{
    if (claims) jz_btree_clone(out, claims, jazz_node_qeb_str_clone, jazz_node_qeb_value_clone);
    else jz_btree_init(out, &jazz_node_qeb_STR_VALUE_DESC);
    /* `user` is Jazz's reserved, issuer-scoped logical identity. Provider
     * claims such as `sub` and `user_id` retain their admitted values, while
     * other Jazz defaults remain fallbacks. */
    jz_btree defaults;
    BIND(default_permission_scope_claim_values)(writer, &defaults);
    jz_str name;
    groove_Value value;
    while (jz_btree_pop_first(&defaults, &name, &value)) {
        bool is_user = jz_str_eq_cstr(&name, "user") || (name.len >= 5 && memcmp(name.ptr, "user.", 5) == 0);
        if (is_user) {
            jz_str ok;
            groove_Value ov;
            if (jz_btree_remove(out, &name, &ok, &ov)) { jz_str_drop(&ok); groove_Value_drop(&ov); }
            jz_btree_insert(out, &name, &value, NULL);
        } else if (jz_btree_contains(out, &name)) {
            jz_str_drop(&name);
            groove_Value_drop(&value);
        } else {
            jz_btree_insert(out, &name, &value, NULL);
        }
    }
    jz_btree_drop(&defaults);
}
