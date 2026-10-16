/* jazz_node::node::query_eval
 *
 * Query execution, shape registration, binding routing, and read-set
 * evaluation for `jazz/SPEC/6_queries.md`. This module owns lowering validated
 * Jazz queries to groove plans, evaluating one-shot reads, recording predicate
 * reads, and applying binding deltas; the pure AST lives in crate::query,
 * policy checks in super::policy, and sync view transport in super::views.
 * It is the node layer's query bridge to groove IVM.
 *
 * Napkin C port (unit node-qe-a). Async fns are synchronous; NodeState<S> has
 * one copy (storage behind groove_Database's vtable). The testing-only
 * thread-local counters and #[cfg(test)] entry points are not ported. */
#include "jz_rt.h"
#include "jazz_node/_ext_node-qe-a.h"
#include "jazz_node/node/query_eval/read_sources.h"
#include "jazz_node/node/query_eval/lowering.h"
#include "jazz_node/node/query_eval/normalization.h"
#include "jazz_node/node/query_eval.h"

#define SL(s) jz_str_slice(&(s))
#define QE(x) jazz_node__node__query_eval__##x
#define QES(x) jazz_node__node__query_eval__NodeState__##x
#define RS(x) jazz_node__node__query_eval__read_sources__##x
#define RSN(x) jazz_node__node__query_eval__read_sources__NodeState__##x
#define LW(x) jazz_node__node__query_eval__lowering__##x
#define LWN(x) jazz_node__node__query_eval__lowering__NodeState__##x
#define NM(x) jazz_node__node__query_eval__normalization__##x
#define NMN(x) jazz_node__node__query_eval__normalization__NodeState__##x
#define BND(x) jazz_node__node__query_eval__bindings__##x
#define QRS(x) jazz_node__node__query_eval__query_read_sets__##x

const char JAZZ_APP_ROWS_SINK[] = "app_rows";
const char JAZZ_PENDING_BINDING_SOURCE_SHAPE[] = "__jazz_pending_binding_source";
/* Bounded attempts of one ordered page probe before the complete source. */
#define ORDERED_PAGE_PROBE_ATTEMPTS 3u
/* Largest prefix a retried ordered page probe reads, unless the requested
 * page alone is larger. */
#define ORDERED_PAGE_PROBE_MAX_CAP 4096u
#define UUID_PAGE_PROBE_ATTEMPTS 3u
#define UUID_PAGE_PROBE_MAX_CAP 4096u

static void str_drop_v(void *p) { jz_str_drop((jz_str *)p); }
static void str_clone_v(void *d, const void *s) { *(jz_str *)d = jz_str_clone((const jz_str *)s); }
static jz_err invalid_stored(const char *msg) { return JZN_INVALID_STORED(msg); }
static u32 sat_add_u32(u32 a, u32 b) { u32 r = a + b; return r < a ? 0xffffffffu : r; }
static u32 sat_mul_u32(u32 a, u32 b) { u64 r = (u64)a * b; return r > 0xffffffffu ? 0xffffffffu : (u32)r; }
static jz_btree empty_values(void) { jz_btree t; jz_btree_init(&t, &jazz_node_StrValue_desc); return t; }
static jz_btree empty_str_set(void) { jz_btree t; jz_btree_init(&t, &jazz_node_StrSet_desc); return t; }
static jz_btree clone_values(const jz_btree *v) { jz_btree t; jz_btree_clone(&t, v, str_clone_v, groove_Value_clone_v); return t; }
static jz_err capability_error_from_report(jazz_engine_CapabilityReport *report) {
    jz_str s = {0}; jazz_engine_CapabilityReport_debug(&s, report);
    jz_err e = JZN_QUERY_CAPABILITY("%.*s", (int)s.len, s.ptr);
    jz_str_drop(&s); jazz_engine_CapabilityReport_free(report);
    return e;
}
static jazz_engine_SourceId root_source(const jz_str *table) { return NM(root_source_id)(table->ptr, table->len); }
typedef JZ_VEC(jz_uuid) UuidVec;

static const jazz_model_RuntimeSchema *catalogue_schema_for(const jazz_node_NodeState *self, jazz_types_SchemaVersionId v) {
    const jazz_node_CatalogueSchema *c = jz_btree_get(&self->catalogue.catalogue_schemas, &v);
    return c ? c->schema : NULL;
}

/* ---------- result-member helpers ---------- */

/* Aggregate terminal membership is structurally identified by the aggregate
 * query plan and its synthetic group-key member. Its table label is not an
 * identity and must not participate in public delivery decisions. */
bool QE(is_public_aggregate_result_member)(const jazz_protocol_ResultMemberEntry *member, const jz_str *result_table, bool aggregate_query) {
    (void)result_table;
    return aggregate_query && member->tag == JAZZ_RME_SYNTHETIC;
}

typedef struct { const jazz_protocol_ResultMemberEntry *candidate; } Replaced;
typedef JZ_VEC(jazz_protocol_ResultMemberEntry) MemberVec;
static void member_drop_v(void *p) { jazz_protocol_ResultMemberEntry_drop(p); }

jz_err QE(replace_stale_authoritative_occurrence_member)(jz_btree *result_set, jz_btree *result_payloads, const jz_btree *authoritative_member_adds,
    const jazz_protocol_ResultMemberEntry *member, const jz_str *result_table, bool aggregate_query) {
    if (!jz_btree_contains(authoritative_member_adds, member)) return JZ_OK;
    bool has; jazz_types_OutputOccurrenceId occurrence_id;
    JZ_TRY(QE(public_result_member_occurrence_id)(member, result_table, aggregate_query, &has, &occurrence_id));
    if (!has) return JZ_OK;
    MemberVec replaced = {0};
    jazz_protocol_ResultMemberEntry *candidate; void *unit;
    JZ_BTREE_FOR(result_set, candidate, unit) {
        if (jazz_protocol_ResultMemberEntry_eq(candidate, member)) continue;
        bool chas; jazz_types_OutputOccurrenceId candidate_id;
        jz_err e = QE(public_result_member_occurrence_id)(candidate, result_table, aggregate_query, &chas, &candidate_id);
        if (JZ_IS_ERR(e)) {
            jz_vec_drop(&replaced, sizeof(jazz_protocol_ResultMemberEntry), member_drop_v);
            jazz_types_OutputOccurrenceId_drop(&occurrence_id);
            return e;
        }
        if (!chas) continue;
        if (jazz_types_OutputOccurrenceId_eq(&candidate_id, &occurrence_id)) {
            jazz_protocol_ResultMemberEntry c; jazz_protocol_ResultMemberEntry_clone(&c, candidate);
            JZ_VEC_PUSH(replaced, c);
        }
        jazz_types_OutputOccurrenceId_drop(&candidate_id);
    }
    for (u32 i = 0; i < replaced.len; i++) {
        jz_btree_remove(result_set, &replaced.ptr[i], NULL, NULL);
        jz_btree_remove(result_payloads, &replaced.ptr[i], NULL, NULL);
    }
    jz_vec_drop(&replaced, sizeof(jazz_protocol_ResultMemberEntry), member_drop_v);
    jazz_types_OutputOccurrenceId_drop(&occurrence_id);
    return JZ_OK;
}

bool QE(is_public_result_member)(const jazz_protocol_ResultMemberEntry *member, const jz_str *result_table, bool aggregate_query) {
    const jz_str *name = jazz_protocol__protocol__ResultMemberEntry__table_name(member);
    return (name && jz_str_eq(name, result_table)) || QE(is_public_aggregate_result_member)(member, result_table, aggregate_query);
}

static const jz_uuid NAMESPACE_OID = {{0x6b, 0xa7, 0xb8, 0x12, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}};
static void push_be_u64(jz_bytes *b, u64 v) { u8 x[8]; for (int i = 0; i < 8; i++) x[i] = (u8)(v >> (56 - 8 * i)); jz_bytes_push(b, x, 8); }

jz_err QE(aggregate_result_member_row_uuid)(const jazz_protocol_ResultMemberEntry *member, jazz_types_RowUuid *out) {
    if (member->tag != JAZZ_RME_SYNTHETIC) return invalid_stored("aggregate result member must be synthetic");
    static const char prefix[] = "jazz:aggregate-result:v1";
    jz_bytes identity = jz_bytes_from((const u8 *)prefix, (u32)sizeof(prefix) - 1);
    push_be_u64(&identity, member->table.len);
    jz_bytes_push(&identity, (const u8 *)member->table.ptr, member->table.len);
    push_be_u64(&identity, member->row.len);
    jz_bytes_push(&identity, member->row.ptr, member->row.len);
    out->u = jz_uuid_v5(&NAMESPACE_OID, identity.ptr, identity.len);
    jz_bytes_drop(&identity);
    return JZ_OK;
}

jz_err QE(public_result_member_occurrence_id)(const jazz_protocol_ResultMemberEntry *member, const jz_str *result_table, bool aggregate_query,
    bool *has, jazz_types_OutputOccurrenceId *out) {
    if (jazz_protocol__protocol__ResultMemberEntry__output_occurrence_id(member, out)) { *has = true; return JZ_OK; }
    *has = false;
    if (!QE(is_public_aggregate_result_member)(member, result_table, aggregate_query)) return JZ_OK;
    jazz_types_RowUuid row;
    JZ_TRY(QE(aggregate_result_member_row_uuid)(member, &row));
    jazz_types_ObjectId id = { row.u };
    *out = jazz_types__object__OutputOccurrenceId__single_source(id);
    *has = true;
    return JZ_OK;
}

jz_err QE(take_required_sink_deltas)(groove_MultisinkDeltas *deltas, jz_slice sink, groove_RecordDeltas **out) {
    jz_str key = jz_str_from_slice(sink);
    groove_RecordDeltas taken;
    bool found = jz_btree_remove(&deltas->sinks, &key, NULL, &taken);
    jz_str_drop(&key);
    groove_MultisinkDeltas_drop(deltas);
    if (!found) return invalid_stored("multisink subscription did not deliver required sink");
    *out = jz_memdup(&taken, sizeof taken);
    return JZ_OK;
}

/* ---------- NodeState: registration and compile front doors ---------- */

jz_err QES(resolve_time_travel_position)(jazz_node_NodeState *self, jazz_types_TxTime time, jazz_types_GlobalTime *out) {
    groove_RawRecordVec raws = {0};
    groove_Database *db = &self->database;
    if (time.t == UINT64_MAX) {
        JZ_TRY(jzn_groove(db->vt->primary_key_scan_raw(db->self, JZ_LIT("jazz_transactions"), NULL, 0, &raws)));
    } else {
        groove_Value lo[2] = { groove_Value_u64(0), groove_Value_u64(0) };
        groove_Value hi[2] = { groove_Value_u64(time.t + 1), groove_Value_u64(0) };
        JZ_TRY(jzn_groove(db->vt->primary_key_scan_range_raw(db->self, JZ_LIT("jazz_transactions"), lo, 2, hi, 2, &raws)));
    }
    jazz_types_GlobalTime position = {0};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < raws.len; i++) {
        groove_BorrowedRecord record = groove__db__RawRecord__record(&raws.ptr[i]);
        bool some; u64 global_time;
        e = groove__records__BorrowedRecord__get_nullable_u64(&record, jazz_node__node__TransactionRowRecord__FIELD_GLOBAL_TIME_IDX, &some, &global_time);
        if (JZ_IS_ERR(e)) { e.code = JZN_E_Record; break; }
        if (!some) continue;
        if (global_time > position.t) position.t = global_time;
    }
    jz_vec_drop(&raws, sizeof(groove_RawRecord), groove_RawRecord_drop);
    JZ_TRY(e);
    *out = position;
    return JZ_OK;
}

/* Resolve a registered shape id back to its validated query, if known.
 * Used by the Db sync surface to reconstruct (shape, binding) from the
 * RegisterShape / Subscribe a subscriber sent over a connection. */
bool QES(registered_shape)(const jazz_node_NodeState *self, jazz_model_ShapeId shape_id, jazz_model_ValidatedQuery *out) {
    const jazz_model_ValidatedQuery *q = jz_btree_get(&self->query.registered_shapes, &shape_id);
    if (!q) return false;
    jazz_model_ValidatedQuery_clone(out, q);
    return true;
}

/* zip(shape.params().keys(), values) */
static jz_btree zip_param_values(const jazz_model_ValidatedQuery *shape, const groove_ValueVec *values) {
    jz_btree out = empty_values();
    jz_str *name; void *ty; u32 i = 0;
    JZ_BTREE_FOR(&shape->params, name, ty) {
        if (i >= values->len) break;
        jz_str k = jz_str_clone(name); groove_Value v; groove_Value_clone(&v, &values->ptr[i++]);
        jz_btree_insert(&out, &k, &v, NULL);
    }
    return out;
}

/* Compiler-owned source identities accepted in one strict peer closure.
 * This is intentionally capability discovery only: it allocates no Groove
 * inputs and never derives authority output from local rows. */
jz_err QES(compiled_covered_input_sources_for_subscription)(const jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, jz_btree *out) {
    const jazz_node_RegisteredBinding *registered =
        jazz_node__node__query_eval__subscriptions__NodeState__unique_registered_binding_for_subscription(self, &subscription);
    if (!registered) return invalid_stored("subscription referenced unregistered binding");
    const jazz_model_ValidatedQuery *shape = jz_btree_get(&self->query.registered_shapes, &subscription.shape_id);
    if (!shape) return invalid_stored("subscription referenced unregistered shape");
    jazz_model_Binding binding;
    JZ_TRY(jazz_model__query__validation__ValidatedQuery__bind(shape, zip_param_values(shape, &registered->values), &binding));
    jazz_engine_QueryProgramRequest request;
    jz_err e = QES(current_query_program_request)(self, shape, &binding, registered->options.tier, registered->compiler_identity,
        JZN_CQPO_MAINTAINED_VIEW, &registered->options.read_view, &registered->binding_view_key, JAZZ_QAM_CLIENT_LOCAL, &request);
    jazz_model_Binding_drop(&binding);
    JZ_TRY(e);
    JZ_VEC(jazz_engine_SourceRequest) source_requests = {0};
    jazz_engine_CapabilityReport *report = NULL;
    e = jazz_engine__query_engine__lowering__query_program_source_requests(&request, (void *)&source_requests, &report);
    if (JZ_IS_ERR(e)) { jazz_engine_QueryProgramRequest_drop(&request); return capability_error_from_report(report); }
    jz_btree tables = empty_str_set();
    for (u32 i = 0; i < source_requests.len; i++) {
        const jazz_engine_SourceRequest *sr = &source_requests.ptr[i];
        if (sr->visibility != JAZZ_VIS_VISIBLE) continue;
        const jazz_engine_SourceExpr *expr = jz_btree_get(&request.reads.primary.sources, &sr->source);
        if (!expr || expr->tag != JAZZ_SE_SETTLED_BINDING_VIEW) continue;
        jz_str t = jz_str_clone(&sr->source.table);
        jz_btree_insert(&tables, &t, NULL, NULL);
    }
    jz_vec_drop(&source_requests, sizeof(jazz_engine_SourceRequest), jazz_engine_SourceRequest_drop);
    jz_btree_init(out, &jazz_node_CompiledScopeTables_desc);
    jz_str *table; void *unit;
    JZ_BTREE_FOR(&tables, table, unit) {
        jazz_types_PhysicalTableId id;
        e = jazz_node__node__physical__NodeState__scope_physical_table((jazz_node_NodeState *)self, request.reads.primary.read_schema, SL(*table), &id);
        if (JZ_IS_ERR(e)) break;
        jz_str name = jz_str_clone(table);
        jz_btree_insert(out, &id, &name, NULL);
    }
    jz_btree_drop(&tables);
    jazz_engine_QueryProgramRequest_drop(&request);
    if (JZ_IS_ERR(e)) { jz_btree_drop(out); return e; }
    return JZ_OK;
}

jz_err QES(compile_current_query_program)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, u32 output, jazz_engine_QueryProgram *out) {
    return QES(compile_current_query_program_in_authorization_mode)(self, shape, binding, tier, identity, output, JAZZ_QAM_TRUSTED_SERVING, out);
}

jz_err QES(compile_current_query_program_in_authorization_mode)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, u32 output, jazz_engine_QueryAuthorizationMode authorization_mode, jazz_engine_QueryProgram *out) {
    jazz_protocol_ReadViewSpec read_view = jazz_protocol_ReadViewSpec_default();
    jz_err e = QES(compile_current_query_program_with_settled_view)(self, shape, binding, tier, identity, output, &read_view, NULL, authorization_mode, out);
    jazz_protocol_ReadViewSpec_drop(&read_view);
    return e;
}

jz_err QES(compile_current_query_program_for_read_view_in_authorization_mode)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, u32 output,
    const jazz_protocol_ReadViewSpec *read_view, jazz_engine_QueryAuthorizationMode authorization_mode, jazz_engine_QueryProgram *out) {
    return QES(compile_current_query_program_with_settled_view)(self, shape, binding, tier, identity, output, read_view, NULL, authorization_mode, out);
}

jz_err QES(compile_current_query_program_with_settled_view)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, u32 output, const jazz_protocol_ReadViewSpec *read_view,
    const jazz_protocol_BindingViewKey *settled_binding_view, jazz_engine_QueryAuthorizationMode authorization_mode, jazz_engine_QueryProgram *out) {
    return QES(compile_current_query_program_with_settled_view_and_prepared_claim_mode)(self, shape, binding, tier, identity, output, read_view,
        settled_binding_view, authorization_mode, JZN_PCBM_STRICT, out);
}

jz_err QES(compile_current_query_program_with_settled_view_and_prepared_claim_mode)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, u32 output, const jazz_protocol_ReadViewSpec *read_view,
    const jazz_protocol_BindingViewKey *settled_binding_view, jazz_engine_QueryAuthorizationMode authorization_mode,
    jazz_node_PreparedClaimBindingMode prepared_claim_binding_mode, jazz_engine_QueryProgram *out) {
    jazz_engine_QueryProgramRequest request; jz_btree access_paths;
    JZ_TRY(QES(current_query_program_request_and_access_paths)(self, shape, binding, tier, identity, output, read_view, settled_binding_view,
        authorization_mode, prepared_claim_binding_mode, &request, &access_paths));
    return LWN(compile_query_program_request_with_access_paths)(self, request, access_paths, out);
}

jz_err QES(current_query_program_request_and_access_paths)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, u32 output, const jazz_protocol_ReadViewSpec *read_view,
    const jazz_protocol_BindingViewKey *settled_binding_view, jazz_engine_QueryAuthorizationMode authorization_mode,
    jazz_node_PreparedClaimBindingMode prepared_claim_binding_mode, jazz_engine_QueryProgramRequest *request_out, jz_btree *access_paths_out) {
    /* A branch view's layers are masked before their predicates (SPEC 4
     * §4.7), so an index bounds each of its content layers for a one-shot
     * read as much as for a maintained one. */
    bool allow_secondary_indexes = output == JZN_CQPO_MAINTAINED_VIEW || read_view->source.tag == JAZZ_RVS_BRANCH_VIEW;
    jazz_engine_QueryProgramRequest request;
    JZ_TRY(QES(current_query_program_request_with_prepared_claim_mode)(self, shape, binding, tier, identity, output, read_view, settled_binding_view,
        authorization_mode, prepared_claim_binding_mode, false, &request));
    /* Retain the established primary-key paths. The new selector contributes
     * only secondary equality indexes for concrete maintained roots; it must
     * not inject another primary-key cap. */
    jz_btree access_paths;
    jz_err e = RSN(current_query_primary_key_access_paths)(self, shape, binding, &access_paths);
    if (JZ_IS_ERR(e)) { jazz_engine_QueryProgramRequest_drop(&request); return e; }
    if (allow_secondary_indexes) {
        jz_btree extra;
        e = RSN(query_program_access_paths)(self, &request, true, &extra);
        if (JZ_IS_ERR(e)) { jz_btree_drop(&access_paths); jazz_engine_QueryProgramRequest_drop(&request); return e; }
        jazz_engine_SourceId k; jazz_node_CurrentAccessPath v;
        while (jz_btree_pop_first(&extra, &k, &v)) {
            if (v.tag == JAZZ_CAP_INDEX) jz_btree_insert(&access_paths, &k, &v, NULL);
            else { jazz_engine_SourceId_drop(&k); jazz_node_CurrentAccessPath_drop(&v); }
        }
        jz_btree_drop(&extra);
    }
    *request_out = request;
    *access_paths_out = access_paths;
    return JZ_OK;
}

jz_err QES(compile_current_query_program_for_one_shot_read)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, const jazz_protocol_BindingViewKey *settled_binding_view,
    jazz_engine_QueryAuthorizationMode authorization_mode, jazz_engine_QueryProgram *out) {
    jazz_protocol_ReadViewSpec read_view = jazz_protocol_ReadViewSpec_default();
    jazz_engine_QueryProgramRequest request;
    jz_err e = QES(current_query_program_request)(self, shape, binding, tier, identity, JZN_CQPO_APP_ROWS, &read_view, settled_binding_view, authorization_mode, &request);
    jazz_protocol_ReadViewSpec_drop(&read_view);
    JZ_TRY(e);
    /* The first result is hydration of the same live source graph used by a
     * retained consumer, not a snapshot-only physical scan. */
    jz_btree access_paths;
    e = RSN(current_query_hydration_access_paths)(self, &request, shape, binding, JAZZ_HL_FIRST_RESULT, &access_paths);
    if (JZ_IS_ERR(e)) { jazz_engine_QueryProgramRequest_drop(&request); return e; }
    return LWN(compile_query_program_request_with_access_paths)(self, request, access_paths, out);
}

static bool root_current_tier_is_global(const jazz_engine_QueryProgramRequest *request, const jazz_engine_SourceId *root) {
    jazz_model_DurabilityTier t;
    return jazz_engine__query_engine__read__ReadView__source_current_tier(&request->reads.primary, root, &t) && t == JAZZ_TIER_GLOBAL;
}

/* One-shot Global listings have a deterministic UUID order. Cap the physical
 * current source before policy evaluation, then prove the page against the
 * same permission and deletion graph as the complete read. */
jz_err QES(compile_uuid_page_probe_program)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_AuthorSubject identity, u32 cap, bool *has, jazz_engine_QueryProgram *program_out, bool *exhausted_out) {
    *has = false;
    jazz_protocol_ReadViewSpec read_view = jazz_protocol_ReadViewSpec_default();
    jazz_engine_QueryProgramRequest request;
    jz_err e = QES(current_query_program_request)(self, shape, binding, JAZZ_TIER_GLOBAL, identity, JZN_CQPO_APP_ROWS, &read_view, NULL, JAZZ_QAM_TRUSTED_SERVING, &request);
    jazz_protocol_ReadViewSpec_drop(&read_view);
    JZ_TRY(e);
    jazz_engine_SourceId root = root_source(&shape->query.table);
    if (!root_current_tier_is_global(&request, &root)) {
        jazz_engine_SourceId_drop(&root); jazz_engine_QueryProgramRequest_drop(&request); return JZ_OK;
    }
    jz_btree access_paths;
    e = RSN(current_query_hydration_access_paths)(self, &request, shape, binding, JAZZ_HL_FIRST_RESULT, &access_paths);
    if (JZ_IS_ERR(e)) { jazz_engine_SourceId_drop(&root); jazz_engine_QueryProgramRequest_drop(&request); return e; }
    if (jz_btree_contains(&access_paths, &root)) {
        jz_btree_drop(&access_paths); jazz_engine_SourceId_drop(&root); jazz_engine_QueryProgramRequest_drop(&request); return JZ_OK;
    }
    jazz_engine_SourceId key; jazz_engine_SourceId_clone(&key, &root);
    jazz_node_CurrentAccessPath page; __builtin_memset(&page, 0, sizeof page);
    page.tag = JAZZ_CAP_PRIMARY_KEY_PAGE; page.cap = cap;
    jz_btree_insert(&access_paths, &key, &page, NULL);
    groove_GraphBuilder *reg; bool exhausted;
    e = QES(bounded_deletion_register_for_uuid_page)(self, shape, cap, &reg, &exhausted);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&access_paths); jazz_engine_SourceId_drop(&root); jazz_engine_QueryProgramRequest_drop(&request); return e; }
    JZ_TRY(LWN(compile_query_program_request_with_bounded_deletion_register)(self, request, access_paths, root, reg, program_out));
    *exhausted_out = exhausted;
    *has = true;
    return JZ_OK;
}

static jz_err query_row_uuids(groove_Database *db, groove_GraphBuilder *graph, UuidVec *out) {
    groove_GraphRows rows = {0};
    JZ_TRY(jzn_groove(db->vt->query_graph(db->self, graph, &rows)));
    jz_err e = JZ_OK;
    for (u32 i = 0; i < rows.len; i++) {
        groove_BorrowedRecord r = groove__records__OwnedRecord__as_borrowed(&rows.ptr[i].row);
        jz_uuid u;
        e = groove__records__BorrowedRecord__get_uuid(&r, 0, &u);
        if (JZ_IS_ERR(e)) { e.code = JZN_E_Record; break; }
        JZ_VEC_PUSH(*out, u);
    }
    groove_GraphRows_drop(&rows);
    return e;
}

static const jazz_node_PhysicalTableMapping *physical_table_mapping(const jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const jz_str *table) {
    const jazz_node_PhysicalMapping *m = jz_btree_get(&self->catalogue.physical_mappings, &v);
    return m ? jz_btree_get(&m->tables, table) : NULL;
}
static groove_Value default_branch_value(void) {
    jazz_model_BranchKey k = jazz_model__model__branch__BranchKey__default();
    groove_Value v = groove_Value_bytes(jazz_model__model__branch__BranchKey__canonical_bytes(&k));
    jazz_model_BranchKey_drop(&k);
    return v;
}
static groove_GraphBuilder *project_row_uuid(groove_GraphBuilder *g) {
    static const char *const f[] = { "row_uuid" };
    return groove__ivm__graph__GraphBuilder__project_cstrs(g, f, 1);
}
static groove_GraphBuilder *deletion_register_from_markers(groove_GraphBuilder *markers) {
    groove_GraphBuilder *g = groove__ivm__graph__GraphBuilder__project_fields(markers, RS(register_storage_fields_for_query_engine)(JZ_LIT("")));
    return groove__ivm__graph__GraphBuilder__filter(g, groove__ivm__op_types__PredicateExpr__is_not_null(JZ_LIT("_deletion")));
}

/* Match the content scan's raw UUID prefix. Reading deletion winners from
 * exactly the same capped entries preserves the anti-join after the cap. */
jz_err QES(bounded_deletion_register_for_uuid_page)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, u32 cap,
    groove_GraphBuilder **register_out, bool *exhausted_out) {
    const jazz_node_PhysicalTableMapping *mapping = physical_table_mapping(self, shape->schema_version, &shape->query.table);
    if (!mapping) return invalid_stored("UUID page probe has no physical table mapping");
    jz_str content_table = jazz_node__node__physical__physical_global_current_table_name(mapping->table_id);
    groove_Value branch = default_branch_value();
    groove_StaticScanSpec scan; __builtin_memset(&scan, 0, sizeof scan);
    scan.tag = GROOVE_SCAN_PREFIX_LIMIT; scan.max_items = cap;
    { groove_Value b; groove_Value_clone(&b, &branch); groove_LiteralValue *l = groove__ivm__op_types__LiteralValue__from_value(b); JZ_VEC_PUSH(scan.prefix, l); }
    jz_btree none = empty_str_set();
    jz_str projection;
    jz_err e = jazz_node__node__physical__NodeState__ensure_physical_current_projection_for_enum_columns(self, shape->schema_version, SL(shape->query.table), &none, &projection);
    jz_btree_drop(&none);
    if (JZ_IS_ERR(e)) goto fail;
    groove_StaticScanSpec scan2; groove_StaticScanSpec_clone(&scan2, &scan);
    UuidVec row_uuids = {0};
    e = query_row_uuids(&self->database, project_row_uuid(groove__ivm__graph__GraphBuilder__variant_source_scan(SL(content_table), SL(projection), scan2)), &row_uuids);
    jz_str_drop(&projection);
    if (JZ_IS_ERR(e)) { jz_free(row_uuids.ptr); goto fail; }
    /* A short projected prefix does not by itself prove physical exhaustion:
     * an incompatible schema variant may be omitted. A reverse point seek
     * proves it only when the last raw UUID was among the projected
     * candidates. Otherwise the probe may widen or fall back to the complete
     * source. */
    bool exhausted = false;
    if (row_uuids.len < cap) {
        groove_Database *db = &self->database;
        bool some = false; groove_RawRecord last_raw;
        e = jzn_groove(db->vt->primary_key_last_raw(db->self, SL(content_table), &branch, 1, &some, &last_raw));
        if (JZ_IS_ERR(e)) { jz_free(row_uuids.ptr); goto fail; }
        const groove_TableSchema *ts;
        e = jzn_groove(db->vt->table_schema(db->self, SL(content_table), &ts));
        u32 row_uuid_index = 0;
        if (!JZ_IS_ERR(e) && !groove__records__RecordDescriptor__field_index(groove__schema__TableSchema__record_schema(ts), JZ_LIT("row_uuid"), &row_uuid_index))
            e = invalid_stored("UUID page probe physical row ID is missing");
        bool has_last = false; jz_uuid last_raw_uuid;
        if (!JZ_IS_ERR(e) && some) {
            groove_BorrowedRecord r = groove__db__RawRecord__record(&last_raw);
            e = groove__records__BorrowedRecord__get_uuid(&r, row_uuid_index, &last_raw_uuid);
            if (JZ_IS_ERR(e)) e.code = JZN_E_Record; else has_last = true;
        }
        if (some) groove_RawRecord_drop(&last_raw);
        if (JZ_IS_ERR(e)) { jz_free(row_uuids.ptr); goto fail; }
        bool has_max = row_uuids.len > 0; jz_uuid max = {{0}};
        for (u32 i = 0; i < row_uuids.len; i++) if (i == 0 || jz_uuid_cmp(&row_uuids.ptr[i], &max) > 0) max = row_uuids.ptr[i];
        exhausted = has_last == has_max && (!has_last || jz_uuid_eq(&last_raw_uuid, &max));
    }
    jz_free(row_uuids.ptr);
    /* Deletion markers live on the content rows themselves, so the register
     * reads the same capped primary-key entries as the content source through
     * the winner projection, which needs only system fields. */
    groove_GraphBuilder *markers;
    e = jazz_node__node__physical__NodeState__physical_global_marker_page_graph(self, shape->schema_version, SL(shape->query.table), scan, &markers);
    scan.prefix.ptr = NULL; scan.prefix.len = 0;
    if (JZ_IS_ERR(e)) goto fail;
    *register_out = deletion_register_from_markers(markers);
    *exhausted_out = exhausted;
    groove_Value_drop(&branch); jz_str_drop(&content_table);
    return JZ_OK;
fail:
    groove_StaticScanSpec_drop(&scan); groove_Value_drop(&branch); jz_str_drop(&content_table);
    return e;
}

static bool query_has_no_page_shape_features(const jazz_model_Query *q, bool allow_filters, bool allow_order) {
    return (allow_filters || q->filters.len == 0) && (allow_order || q->order_by.len == 0) && !q->has_select && q->joins.len == 0
        && !q->flat_join && q->policy_branches.len == 0 && q->reachable.len == 0 && q->inherits.len == 0 && q->includes.len == 0
        && q->array_subqueries.len == 0 && !q->aggregate && !q->relation;
}

/* One probe iteration shared by try_uuid_page_probe and try_ordered_page_probe:
 * validate the capped query, compile its probe, hydrate it once and
 * materialize its rows. */
typedef jz_err (*probe_compile_fn)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_AuthorSubject identity, const jz_str *column, u32 cap, bool *has, jazz_engine_QueryProgram *program, bool *exhausted);

static jz_err run_page_probe(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_AuthorSubject identity, const jazz_model_RuntimeSchema *schema, const jazz_model_TableSchema *table, const jz_str *column, u32 cap,
    probe_compile_fn compile, bool *has, jazz_model_Query *probe_query, jazz_node_CurrentRowVec *rows, bool *exhausted) {
    *has = false;
    jazz_model_Query_clone(probe_query, &shape->query);
    probe_query->has_limit = true; probe_query->limit = cap;
    jazz_model_ValidatedQuery probe_shape;
    jz_err e = jazz_model__query__ast__Query__validate_with_schema_version(probe_query, schema, shape->schema_version, &probe_shape);
    if (JZ_IS_ERR(e)) { e.code = JZN_E_Query; jazz_model_Query_drop(probe_query); return e; }
    jazz_model_Binding probe_binding;
    e = jazz_model__query__validation__ValidatedQuery__bind(&probe_shape, clone_values(&binding->values), &probe_binding);
    if (JZ_IS_ERR(e)) { e.code = JZN_E_Query; jazz_model_ValidatedQuery_drop(&probe_shape); jazz_model_Query_drop(probe_query); return e; }
    jazz_engine_QueryProgram program; bool found = false;
    e = compile(self, &probe_shape, &probe_binding, identity, column, cap, &found, &program, exhausted);
    if (JZ_IS_ERR(e) || !found) goto done;
    jazz_engine_AppRowSchema app_output;
    e = LW(materialization_app_row_schema)(NULL, &program, &app_output);
    if (JZ_IS_ERR(e)) { jazz_engine_QueryProgram_drop(&program); goto done; }
    groove_RootIndirectValues root_indirect_values;
    e = QES(projection_dropped_root_values)(self, probe_query, shape->schema_version, &root_indirect_values);
    if (JZ_IS_ERR(e)) { jazz_engine_AppRowSchema_drop(&app_output); jazz_engine_QueryProgram_drop(&program); goto done; }
    groove_RecordDeltas *deltas = NULL;
    e = LWN(hydrate_lowered_program_once)(self, program, &probe_binding, root_indirect_values, &deltas);
    if (!JZ_IS_ERR(e)) {
        e = QES(materialize_and_finalize_query_rows)(self, probe_query, shape->schema_version, table, &app_output, deltas, NULL, rows);
        groove_RecordDeltas_drop(deltas); jz_free(deltas);
        if (!JZ_IS_ERR(e)) *has = true;
    }
    jazz_engine_AppRowSchema_drop(&app_output);
done:
    jazz_model_Binding_drop(&probe_binding);
    jazz_model_ValidatedQuery_drop(&probe_shape);
    if (!*has) jazz_model_Query_drop(probe_query);
    return e;
}

static jz_err uuid_probe_compile(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_AuthorSubject identity, const jz_str *column, u32 cap, bool *has, jazz_engine_QueryProgram *program, bool *exhausted) {
    (void)column;
    return QES(compile_uuid_page_probe_program)(self, shape, binding, identity, cap, has, program, exhausted);
}

/* A UUID beyond the requested page, or an exhausted physical prefix, proves
 * that later source rows cannot enter this page. Sparse policy visibility
 * widens twice before using the complete source. */
jz_err QES(try_uuid_page_probe)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_AuthorSubject identity, bool *has, jazz_node_CurrentRowVec *out) {
    *has = false;
    const jazz_model_Query *query = &shape->query;
    if (!query->has_limit || query->limit == 0 || query->limit > 1000) return JZ_OK;
    u32 limit = query->limit;
    if (query->offset != 0 || !query_has_no_page_shape_features(query, false, false)) return JZ_OK;
    const jazz_model_RuntimeSchema *schema = catalogue_schema_for(self, shape->schema_version);
    if (!schema) return invalid_stored("query schema version is unknown");
    jazz_model_TableSchema table;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema(self, SL(query->table), shape->schema_version, &table));
    u32 cap = sat_add_u32(limit, 1);
    u32 max_cap = cap > UUID_PAGE_PROBE_MAX_CAP ? cap : UUID_PAGE_PROBE_MAX_CAP;
    jz_err e = JZ_OK;
    for (u32 attempt = 0; attempt < UUID_PAGE_PROBE_ATTEMPTS; attempt++) {
        jazz_model_Query probe_query; jazz_node_CurrentRowVec rows = {0}; bool ran = false, exhausted = false;
        e = run_page_probe(self, shape, binding, identity, schema, &table, NULL, cap, uuid_probe_compile, &ran, &probe_query, &rows, &exhausted);
        if (JZ_IS_ERR(e) || !ran) break;
        jazz_model_Query_drop(&probe_query);
        if (rows.len > limit || exhausted) {
            jz_vec_truncate(&rows, limit, sizeof(jazz_node_CurrentRow), jazz_node_CurrentRow_drop);
            *out = rows; *has = true;
            break;
        }
        jazz_node_CurrentRowVec_drop(&rows);
        if (attempt + 1 == UUID_PAGE_PROBE_ATTEMPTS || cap >= max_cap) break;
        cap = sat_mul_u32(cap, 4); if (cap > max_cap) cap = max_cap;
    }
    jazz_model_TableSchema_drop(&table);
    return e;
}

/* Compile one bounded ordered-page probe at the Global tier.
 *
 * The probe never selects an index itself. It narrows the root path that
 * first-result hydration already admitted through
 * guarded_current_access_path, and only when that path is exactly the
 * single-column equality probe whose column leads the declared
 * (equality_column, order_column) composite index. Re-addressing that prefix
 * through the composite index keeps the same candidate domain and only orders
 * it, so the cap can be re-proved after the graph applies every filter,
 * deletion check, and policy. Any other admitted shape, including no
 * admitted path at all, declines the probe. */
jz_err QES(compile_ordered_page_probe_program)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_AuthorSubject identity, const jz_str *equality_column, u32 cap, bool *has, jazz_engine_QueryProgram *program_out, bool *exhausted_out) {
    *has = false;
    jazz_protocol_ReadViewSpec read_view = jazz_protocol_ReadViewSpec_default();
    jazz_engine_QueryProgramRequest request;
    jz_err e = QES(current_query_program_request)(self, shape, binding, JAZZ_TIER_GLOBAL, identity, JZN_CQPO_APP_ROWS, &read_view, NULL, JAZZ_QAM_TRUSTED_SERVING, &request);
    jazz_protocol_ReadViewSpec_drop(&read_view);
    JZ_TRY(e);
    jazz_engine_SourceId root = root_source(&shape->query.table);
    jz_btree access_paths; bool have_paths = false;
    if (!root_current_tier_is_global(&request, &root)) goto decline;
    e = RSN(current_query_hydration_access_paths)(self, &request, shape, binding, JAZZ_HL_FIRST_RESULT, &access_paths);
    if (JZ_IS_ERR(e)) goto decline;
    have_paths = true;
    jazz_node_CurrentAccessPath *path = jz_btree_get(&access_paths, &root);
    /* A covered-key filter is only attached to join paths; a root path
     * carrying one is not a plain ordered-page candidate. */
    if (!path || path->tag != JAZZ_CAP_INDEX || path->candidate_filter) goto decline;
    if (!jz_str_eq(&path->column, equality_column) || path->has_order_column || path->prefix.len != 1
        || path->intersections.len != 0 || path->has_source_limit)
        goto decline;
    const jazz_model_OrderBy *order = &shape->query.order_by.ptr[0];
    path->has_order_column = true; path->order_column = jz_str_clone(&order->column);
    path->reverse = order->direction == JAZZ_ORDER_DESC;
    path->has_source_limit = true; path->source_limit = cap;
    /* A first-result owner retires this graph after hydration. */
    path->maintained = false;
    jazz_node_CurrentAccessPath path_copy; jazz_node_CurrentAccessPath_clone(&path_copy, path);
    /* The graph's content source reads exactly these capped composite index
     * entries, so only their deletion winners can affect the page. Policy
     * subplans that specialise this occurrence inherit the same capped path
     * and therefore the same register. */
    groove_GraphBuilder *reg; bool exhausted;
    e = QES(bounded_deletion_register_for_ordered_page)(self, shape, &path_copy, cap, &reg, &exhausted);
    jazz_node_CurrentAccessPath_drop(&path_copy);
    if (JZ_IS_ERR(e)) goto decline;
    JZ_TRY(LWN(compile_query_program_request_with_bounded_deletion_register)(self, request, access_paths, root, reg, program_out));
    *exhausted_out = exhausted;
    *has = true;
    return JZ_OK;
decline:
    if (have_paths) jz_btree_drop(&access_paths);
    jazz_engine_SourceId_drop(&root);
    jazz_engine_QueryProgramRequest_drop(&request);
    return e;
}

/* Materialize only the deletion winners whose content rows can enter a
 * bounded ordered page probe. The caller holds the node's read lock over both
 * this snapshot and execution of the lowered query program.
 *
 * Also reports whether the physical index prefix is exhausted: it holds fewer
 * than cap raw entries, so the capped content source saw every candidate the
 * prefix can ever produce. */
jz_err QES(bounded_deletion_register_for_ordered_page)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_node_CurrentAccessPath *path,
    u32 cap, groove_GraphBuilder **register_out, bool *exhausted_out) {
    if (path->tag != JAZZ_CAP_INDEX || !path->has_order_column) return invalid_stored("ordered page probe requires a composite index");
    if (path->intersections.len != 0) return invalid_stored("ordered page probe cannot intersect indexes");
    const jazz_node_PhysicalTableMapping *mapping = physical_table_mapping(self, shape->schema_version, &shape->query.table);
    if (!mapping) return invalid_stored("ordered page probe has no physical table mapping");
    const jazz_types_PhysicalColumnId *column_id = jz_btree_get(&mapping->columns, &path->column);
    if (!column_id) return invalid_stored("ordered page probe has no equality column mapping");
    const jazz_types_PhysicalColumnId *order_column_id = jz_btree_get(&mapping->columns, &path->order_column);
    if (!order_column_id) return invalid_stored("ordered page probe has no order column mapping");
    jz_str content_table = jazz_node__node__physical__physical_global_current_table_name(mapping->table_id);
    jazz_types_PhysicalColumnId ids[2] = { *column_id, *order_column_id };
    jz_str index = jazz_node__node__physical__physical_current_composite_index_name_ids(ids, 2);
    groove_StaticScanSpec scan; __builtin_memset(&scan, 0, sizeof scan);
    scan.tag = path->reverse ? GROOVE_SCAN_REVERSE_PREFIX_LIMIT : GROOVE_SCAN_PREFIX_LIMIT;
    scan.max_items = cap;
    { groove_LiteralValue *l = groove__ivm__op_types__LiteralValue__from_value(default_branch_value()); JZ_VEC_PUSH(scan.prefix, l); }
    for (u32 i = 0; i < path->prefix.len; i++) {
        groove_Value v; groove_Value_clone(&v, &path->prefix.ptr[i]);
        groove_LiteralValue *l = groove__ivm__op_types__LiteralValue__from_value(v); JZ_VEC_PUSH(scan.prefix, l);
    }
    /* Both this read and the query graph's content source cap the same raw
     * composite index entries before projection. With no required fields this
     * projection omits no more rows than the graph's own projection target,
     * so every row the graph can admit keeps its deletion register. */
    jz_btree none = empty_str_set();
    jz_str projection;
    jz_err e = jazz_node__node__physical__NodeState__ensure_physical_current_projection_for_enum_columns(self, shape->schema_version, SL(shape->query.table), &none, &projection);
    jz_btree_drop(&none);
    if (JZ_IS_ERR(e)) goto fail;
    groove_StaticScanSpec s2; groove_StaticScanSpec_clone(&s2, &scan);
    UuidVec row_uuids = {0};
    e = query_row_uuids(&self->database,
        project_row_uuid(groove__ivm__graph__GraphBuilder__variant_index_scan(SL(content_table), SL(index), s2, SL(projection))), &row_uuids);
    jz_str_drop(&projection);
    u32 projected = row_uuids.len; jz_free(row_uuids.ptr);
    if (JZ_IS_ERR(e)) goto fail;
    /* The projection may omit a schema-incompatible entry, so a short
     * projected list does not prove the prefix is short. Only the raw entry
     * count, which the content source caps identically, does. The recount
     * runs only when the projected list is already short, and reads at most
     * cap index entries. */
    bool exhausted = false;
    if (projected < cap) {
        groove_StaticScanSpec s3; groove_StaticScanSpec_clone(&s3, &scan);
        groove_GraphRows raw = {0};
        e = jzn_groove(self->database.vt->query_graph(self->database.self, groove__ivm__graph__GraphBuilder__index_scan(SL(content_table), SL(index), s3), &raw));
        if (JZ_IS_ERR(e)) goto fail;
        exhausted = raw.len < cap;
        groove_GraphRows_drop(&raw);
    }
    /* Deletion markers live on the content rows themselves, so the register
     * reads the same capped composite index entries through the winner
     * projection, which needs only system fields. */
    groove_GraphBuilder *markers;
    e = jazz_node__node__physical__projections__NodeState__physical_global_marker_index_graph(self, shape->schema_version, SL(shape->query.table), index, scan, &markers);
    index.ptr = NULL; index.len = index.cap = 0; scan.prefix.ptr = NULL; scan.prefix.len = 0;
    if (JZ_IS_ERR(e)) goto fail;
    *register_out = deletion_register_from_markers(markers);
    *exhausted_out = exhausted;
    jz_str_drop(&content_table);
    return JZ_OK;
fail:
    groove_StaticScanSpec_drop(&scan); jz_str_drop(&index); jz_str_drop(&content_table);
    return e;
}

static bool order_column_type_admitted(const groove_ColumnType *t) {
    switch (t->tag) {
    case GROOVE_VT_U8: case GROOVE_VT_U16: case GROOVE_VT_U32: case GROOVE_VT_U64: case GROOVE_VT_I32: case GROOVE_VT_I64:
    case GROOVE_VT_BOOL: case GROOVE_VT_STRING: case GROOVE_VT_BYTES: case GROOVE_VT_UUID: return true;
    default: return false;
    }
}

static jz_err ordered_probe_compile(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_AuthorSubject identity, const jz_str *column, u32 cap, bool *has, jazz_engine_QueryProgram *program, bool *exhausted) {
    return QES(compile_ordered_page_probe_program)(self, shape, binding, identity, column, cap, has, program, exhausted);
}

/* Probe an ordered current index a page at a time. The query graph still
 * applies all filters, deletion checks, and policy. An extra visible row with
 * a sort key strictly worse than the page's last row proves that the
 * requested page is final, and so does an exhausted physical prefix. Ties and
 * sparse visibility retry with up to two larger bounded prefixes (4x each, at
 * most max(4096, limit + 1) entries) before falling back to the ordinary
 * complete source. */
jz_err QES(try_ordered_page_probe)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_AuthorSubject identity, bool *has, jazz_node_CurrentRowVec *out) {
    *has = false;
    const jazz_model_Query *query = &shape->query;
    if (!query->has_limit || query->limit == 0) return JZ_OK;
    u32 limit = query->limit;
    if (query->offset != 0 || query->order_by.len != 1 || !query_has_no_page_shape_features(query, true, true)) return JZ_OK;
    const jz_str *order_column = &query->order_by.ptr[0].column;
    jazz_model_TableSchema table;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema(self, SL(query->table), shape->schema_version, &table));
    jz_err e = JZ_OK;
    jz_btree paths; bool have_paths = false;
    /* The probe is exact only when the physical index key order equals the
     * query comparator for the order column. Admit only scalar types whose
     * order-preserving key encoding and comparator agree; nullable,
     * floating-point, composite, and physical-only encodings decline. */
    bool admitted = false;
    for (u32 i = 0; i < table.columns.len; i++)
        if (jz_str_eq(&table.columns.ptr[i].name, order_column) && order_column_type_admitted(&table.columns.ptr[i].column_type)) admitted = true;
    if (!admitted) goto done;
    e = RSN(one_shot_access_paths)(self, shape, binding, JAZZ_TIER_GLOBAL, &paths);
    if (JZ_IS_ERR(e)) goto done;
    have_paths = true;
    jazz_engine_SourceId root = root_source(&query->table);
    const jazz_node_CurrentAccessPath *path = jz_btree_get(&paths, &root);
    jazz_engine_SourceId_drop(&root);
    if (!path || path->tag != JAZZ_CAP_INDEX) goto done;
    const jz_str *column = &path->column;
    if (path->intersections.len != 0) goto done;
    {
        jz_vec_str pair = {0};
        JZ_VEC_PUSH(pair, jz_str_clone(column)); JZ_VEC_PUSH(pair, jz_str_clone(order_column));
        bool declared = jz_btree_contains(&table.composite_indexes, &pair);
        jz_vec_drop(&pair, sizeof(jz_str), str_drop_v);
        if (!declared) goto done;
    }
    /* A conjunctive claim equality on another column makes this prefix sparse
     * by construction. For example, scanning an organization page in
     * timestamp order cannot efficiently find one user's owner rows.
     * Alternative policy branches may still admit the ordered prefix. */
    if (table.read_policy && table.read_policy->policy_branches.len == 0) {
        const jazz_model_PredicateVec *filters = &table.read_policy->filters;
        for (u32 i = 0; i < filters->len; i++) {
            const jazz_model_Predicate *f = &filters->ptr[i];
            if (f->tag != JAZZ_PRED_EQ) continue;
            const jz_str *claim_column = NULL;
            if (f->left.tag == JAZZ_OPERAND_COLUMN && f->right.tag == JAZZ_OPERAND_CLAIM) claim_column = &f->left.name;
            else if (f->left.tag == JAZZ_OPERAND_CLAIM && f->right.tag == JAZZ_OPERAND_COLUMN) claim_column = &f->right.name;
            if (claim_column && !jz_str_eq(claim_column, column)) goto done;
        }
    }
    const jazz_model_RuntimeSchema *schema = catalogue_schema_for(self, shape->schema_version);
    if (!schema) { e = invalid_stored("query schema version is unknown"); goto done; }
    u32 cap = sat_add_u32(limit, 1);
    u32 max_cap = cap > ORDERED_PAGE_PROBE_MAX_CAP ? cap : ORDERED_PAGE_PROBE_MAX_CAP;
    for (u32 attempt = 0; attempt < ORDERED_PAGE_PROBE_ATTEMPTS; attempt++) {
        jazz_model_Query probe_query; jazz_node_CurrentRowVec rows = {0}; bool ran = false, exhausted = false;
        e = run_page_probe(self, shape, binding, identity, schema, &table, column, cap, ordered_probe_compile, &ran, &probe_query, &rows, &exhausted);
        if (JZ_IS_ERR(e) || !ran) break;
        jazz_model_Query_drop(&probe_query);
        /* The index yields every row tied with the page's last row before any
         * strictly worse one, so one strictly worse visible row anywhere in
         * the probe proves the whole tie group was read and the query's own
         * comparator ordered it. The rows are sorted, so the last one is the
         * worst. */
        bool strictly_worse_row = false;
        if (rows.len > limit) {
            groove_Value a, b;
            bool ha = QE(query_order_value)(&rows.ptr[limit - 1], &table, order_column, &a);
            bool hb = QE(query_order_value)(&rows.ptr[rows.len - 1], &table, order_column, &b);
            strictly_worse_row = ha != hb || (ha && !groove_Value_eq(&a, &b));
            if (ha) groove_Value_drop(&a);
            if (hb) groove_Value_drop(&b);
        }
        if (strictly_worse_row || exhausted) {
            jz_vec_truncate(&rows, limit, sizeof(jazz_node_CurrentRow), jazz_node_CurrentRow_drop);
            *out = rows; *has = true;
            break;
        }
        jazz_node_CurrentRowVec_drop(&rows);
        if (attempt + 1 == ORDERED_PAGE_PROBE_ATTEMPTS || cap >= max_cap) break;
        cap = sat_mul_u32(cap, 4); if (cap > max_cap) cap = max_cap;
    }
done:
    if (have_paths) jz_btree_drop(&paths);
    jazz_model_TableSchema_drop(&table);
    return e;
}

jz_err QES(compile_current_query_program_with_access_paths)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, u32 output, jz_btree access_paths,
    jazz_engine_QueryAuthorizationMode authorization_mode, jazz_engine_QueryProgram *out) {
    jazz_protocol_ReadViewSpec read_view = jazz_protocol_ReadViewSpec_default();
    jazz_engine_QueryProgramRequest request;
    jz_err e = QES(current_query_program_request_with_inline_binding_source)(self, shape, binding, tier, identity, output, &read_view, authorization_mode, &request);
    jazz_protocol_ReadViewSpec_drop(&read_view);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&access_paths); return e; }
    return LWN(compile_query_program_request_with_access_paths)(self, request, access_paths, out);
}

/* RowSetProgramInput for the historical / snapshot / include-deleted / open-tx programs. */
static jz_err program_input_for_shape(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_engine_NormalizedRowSetShape input_shape, jazz_engine_RowSetProgramInput *out) {
    jz_btree claim_params = BND(binding_claim_params_for_shape)(&input_shape, &shape->params);
    jz_str source_shape; bool has_source_shape =
        jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_parts_if_needed(&shape->params, &claim_params, &source_shape);
    jz_btree claim_params2 = BND(binding_claim_params_for_shape)(&input_shape, &shape->params);
    jz_btree extra; jz_btree_init(&extra, &jazz_engine_ColumnTypes_desc);
    out->binding = jazz_node__node__query_eval__bindings__NodeState__program_binding_for_shape(self, shape, binding,
        has_source_shape ? &source_shape : NULL, extra, claim_params2);
    if (has_source_shape) jz_str_drop(&source_shape);
    jz_btree_drop(&claim_params);
    out->shape = input_shape;
    return JZ_OK;
}

static jz_err require_schema_version(const jazz_node_NodeState *self, jazz_types_SchemaVersionId v) {
    if (!catalogue_schema_for(self, v)) return invalid_stored("query schema version is unknown");
    return JZ_OK;
}

static jz_err finish_request(const jazz_node_NodeState *self, jazz_engine_QueryAuthorizationMode mode, jazz_engine_RequestedReadSet reads,
    jazz_types_AuthorSubject identity, jazz_engine_RowSetProgramInput input, u32 output, const jazz_model_Query *query, jazz_engine_QueryProgramRequest *out) {
    out->authorization_mode = mode;
    out->reads = reads;
    out->policy = jazz_node__node__query_eval__authorization__NodeState__query_program_policy_context(self, identity);
    out->input = input;
    jz_err e = NM(current_query_output_request)(output, query, &out->output);
    if (JZ_IS_ERR(e)) {
        jazz_engine_RequestedReadSet_drop(&out->reads); jazz_engine_PolicyContext_drop(&out->policy);
        jazz_engine_NormalizedRowSetShape_drop(&out->input.shape); jazz_engine_ProgramBinding_drop(&out->input.binding);
    }
    return e;
}

jz_err QES(compile_historical_query_program)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_GlobalTime position, jazz_types_AuthorSubject identity, u32 output, jazz_engine_QueryProgram *out) {
    JZ_TRY(require_schema_version(self, shape->schema_version));
    jazz_engine_NormalizedRowSetShape input_shape;
    JZ_TRY(NMN(normalized_row_set_shape)(self, shape, binding, &input_shape));
    jazz_engine_RowSetProgramInput input;
    JZ_TRY(program_input_for_shape(self, shape, binding, input_shape, &input));
    jazz_engine_QueryProgramRequest request;
    JZ_TRY(finish_request(self, JAZZ_QAM_TRUSTED_SERVING, QRS(historical_query_read_set)(&input.shape, shape->schema_version, position),
        identity, input, output, &shape->query, &request));
    return LWN(compile_query_program_request)(self, request, out);
}

jz_err QES(compile_snapshot_query_program)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    const jazz_model_Snapshot *snapshot, jazz_types_AuthorSubject identity, u32 output, jazz_engine_QueryProgram *out) {
    JZ_TRY(require_schema_version(self, shape->schema_version));
    jazz_engine_NormalizedRowSetShape input_shape;
    JZ_TRY(NMN(normalized_row_set_shape)(self, shape, binding, &input_shape));
    jazz_engine_RowSetProgramInput input;
    JZ_TRY(program_input_for_shape(self, shape, binding, input_shape, &input));
    jazz_engine_QueryProgramRequest request;
    JZ_TRY(finish_request(self, JAZZ_QAM_TRUSTED_SERVING, QRS(snapshot_query_read_set)(&input.shape, shape->schema_version, snapshot),
        identity, input, output, &shape->query, &request));
    return LWN(compile_query_program_request)(self, request, out);
}

jz_err QES(compile_include_deleted_query_program_in_authorization_mode)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_engine_QueryAuthorizationMode authorization_mode,
    const jazz_protocol_ReadViewSpec *read_view, const jazz_types_RowUuid *physical_row, jazz_engine_QueryProgram *out) {
    JZ_TRY(require_schema_version(self, shape->schema_version));
    jazz_engine_NormalizedRowSetShape input_shape;
    JZ_TRY(NMN(normalized_include_deleted_row_set_shape)(self, shape, binding, &input_shape));
    jazz_engine_RowSetProgramInput input;
    JZ_TRY(program_input_for_shape(self, shape, binding, input_shape, &input));
    jazz_engine_RequestedReadSet reads;
    jz_err e = QRS(query_read_set_for_read_view)(&input.shape, shape->schema_version,
        jazz_node__node__policy__NodeState__read_policy_schema_for_table_name(self, SL(shape->query.table), shape->schema_version, &input.shape),
        tier, read_view, NULL, NULL, jazz_node__node__state__catalogue__SchemaCatalogue__schema(&self->catalogue), &reads);
    if (JZ_IS_ERR(e)) { jazz_engine_NormalizedRowSetShape_drop(&input.shape); jazz_engine_ProgramBinding_drop(&input.binding); return e; }
    jazz_engine_QueryProgramRequest request;
    JZ_TRY(finish_request(self, authorization_mode, reads, identity, input, JZN_CQPO_APP_ROWS, &shape->query, &request));
    /* This one-shot include-deleted source has no deletion anti-join after
     * it. The proof remains deliberately narrower than ordinary visible
     * reads, which discard the physical cap before their anti-join. */
    jz_btree access_paths;
    if (physical_row) {
        jz_btree_init(&access_paths, &jazz_node_AccessPaths_desc);
        jazz_engine_SourceId root = root_source(&shape->query.table);
        jazz_node_CurrentAccessPath p; __builtin_memset(&p, 0, sizeof p);
        p.tag = JAZZ_CAP_PRIMARY_KEY;
        JZ_VEC_PUSH(p.prefix, groove_Value_uuid(physical_row->u));
        jz_btree_insert(&access_paths, &root, &p, NULL);
    } else {
        e = RSN(one_shot_access_paths)(self, shape, binding, tier, &access_paths);
        if (JZ_IS_ERR(e)) { jazz_engine_QueryProgramRequest_drop(&request); return e; }
    }
    return LWN(compile_query_program_request_with_access_paths)(self, request, access_paths, out);
}

jz_err QES(compile_open_tx_query_program)(jazz_node_NodeState *self, jazz_model_OpenTransactionId tx_id, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, jazz_types_AuthorSubject identity, u32 output, bool include_deleted,
    jazz_engine_QueryAuthorizationMode authorization_mode, jazz_engine_QueryProgram *out) {
    const jazz_node_OpenTransaction *tx;
    JZ_TRY(jazz_node__node__open_tx__NodeState__open_tx(self, tx_id, &tx));
    jazz_model_Snapshot snapshot; jazz_model_Snapshot_clone(&snapshot, &tx->base_snapshot);
    const jazz_model_RuntimeSchema *read_schema = catalogue_schema_for(self, shape->schema_version);
    jz_err e = JZ_OK;
    if (!read_schema) { e = invalid_stored("query schema version is unknown"); goto out_snapshot; }
    jazz_model_ValidatedQuery lowered_shape;
    e = BND(inline_snapshot_bind_filter_literals)(shape, binding, read_schema, &lowered_shape);
    if (JZ_IS_ERR(e)) goto out_snapshot;
    jazz_model_Binding lowered_binding;
    e = jazz_model__query__validation__ValidatedQuery__bind(&lowered_shape, empty_values(), &lowered_binding);
    if (JZ_IS_ERR(e)) { e.code = JZN_E_Query; goto out_shape; }
    e = require_schema_version(self, lowered_shape.schema_version);
    jazz_engine_NormalizedRowSetShape input_shape;
    if (!JZ_IS_ERR(e))
        e = include_deleted ? NMN(normalized_include_deleted_row_set_shape)(self, &lowered_shape, &lowered_binding, &input_shape)
                            : NMN(normalized_row_set_shape)(self, &lowered_shape, &lowered_binding, &input_shape);
    if (JZ_IS_ERR(e)) goto out_binding;
    jazz_engine_RowSetProgramInput input;
    program_input_for_shape(self, &lowered_shape, &lowered_binding, input_shape, &input);
    jazz_engine_QueryProgramRequest request;
    e = finish_request(self, authorization_mode, QRS(tx_query_read_set)(&input.shape, lowered_shape.schema_version, tx_id, &snapshot),
        identity, input, output, &lowered_shape.query, &request);
    if (!JZ_IS_ERR(e)) e = LWN(compile_query_program_request)(self, request, out);
out_binding:
    jazz_model_Binding_drop(&lowered_binding);
out_shape:
    jazz_model_ValidatedQuery_drop(&lowered_shape);
out_snapshot:
    jazz_model_Snapshot_drop(&snapshot);
    return e;
}

jz_err QES(current_query_program_request)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, u32 output, const jazz_protocol_ReadViewSpec *read_view,
    const jazz_protocol_BindingViewKey *settled_binding_view, jazz_engine_QueryAuthorizationMode authorization_mode, jazz_engine_QueryProgramRequest *out) {
    return QES(current_query_program_request_with_prepared_claim_mode)(self, shape, binding, tier, identity, output, read_view, settled_binding_view,
        authorization_mode, JZN_PCBM_STRICT, false, out);
}

jz_err QES(current_query_program_request_with_inline_binding_source)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, u32 output,
    const jazz_protocol_ReadViewSpec *read_view, jazz_engine_QueryAuthorizationMode authorization_mode, jazz_engine_QueryProgramRequest *out) {
    return QES(current_query_program_request_with_prepared_claim_mode)(self, shape, binding, tier, identity, output, read_view, NULL,
        authorization_mode, JZN_PCBM_STRICT, true, out);
}

/* The storage-backed maintained subset retrieves omitted witnesses by the
 * result table name at the wire boundary. A table-rename lens breaks that
 * identity: its result member is named with the projected table but its
 * persisted history is named with the authored table. Keep such shapes on the
 * self-contained witness path until the fallback resolves canonical physical
 * identities end-to-end. */
bool QES(storage_backed_maintained_root_has_identity_table_mapping)(const jazz_node_NodeState *self, jazz_types_SchemaVersionId schema_version, const jz_str *table) {
    if (!jazz_types_SchemaVersionId_eq(schema_version, self->catalogue.local_schema_version_id)) return false;
    const jazz_node_PhysicalTableMapping *m = physical_table_mapping(self, schema_version, table);
    if (!m) return false;
    jazz_types_PhysicalTableId table_id = m->table_id;
    void *vk; jazz_node_PhysicalMapping *mapping;
    JZ_BTREE_FOR(&self->catalogue.physical_mappings, vk, mapping) {
        jz_str *candidate_name; jazz_node_PhysicalTableMapping *candidate;
        JZ_BTREE_FOR(&mapping->tables, candidate_name, candidate) {
            if (jz_uuid_eq(&candidate->table_id.u, &table_id.u) && !jz_str_eq(candidate_name, table)) return false;
        }
    }
    return true;
}

static const jazz_model_RuntimeSchema *schema_for_policy_version(const jazz_node_NodeState *self, jazz_types_SchemaVersionId v, const char *unknown) {
    (void)unknown;
    if (jazz_types_SchemaVersionId_eq(v, jazz_node__node__state__catalogue__SchemaCatalogue__active_schema_version(&self->catalogue)))
        return jazz_node__node__state__catalogue__SchemaCatalogue__active_compiled(&self->catalogue);
    return NULL;
}

jz_err QES(current_query_program_request_with_prepared_claim_mode)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape_in,
    const jazz_model_Binding *binding_in, jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, u32 output,
    const jazz_protocol_ReadViewSpec *read_view, const jazz_protocol_BindingViewKey *settled_binding_view,
    jazz_engine_QueryAuthorizationMode authorization_mode, jazz_node_PreparedClaimBindingMode prepared_claim_binding_mode,
    bool force_inline_binding_source, jazz_engine_QueryProgramRequest *out) {
    jazz_engine_PolicyContext policy = jazz_node__node__query_eval__authorization__NodeState__query_program_policy_context(self, identity);
    jazz_model_ShapeId requested_shape_id = shape_in->shape_id;
    const jazz_model_ValidatedQuery *shape = shape_in; const jazz_model_Binding *binding = binding_in;
    jz_err e = JZ_OK;
    /* Linked shapes carry read-policy alternatives so a trusted serving
     * authority can establish the authorized residual frontier. Neither
     * System authority nor a client-local receiver may evaluate those
     * alternatives: System bypasses them, while a receiver consumes the exact
     * already-authorized CoveredInput closure. Retaining branches locally
     * would both re-evaluate policy and turn policy-proof sources into
     * receiver inputs. */
    jazz_model_ValidatedQuery residual_shape; jazz_model_Binding residual_binding; bool has_residual = false;
    jazz_model_ValidatedQuery lowered_shape; jazz_model_Binding lowered_binding; bool has_lowered = false;
    jazz_engine_NormalizedRowSetShape input_shape; bool has_input = false;
    jz_btree binding_claim_params; bool has_claims = false;
    jz_str source_shape = {0}; bool has_source_shape = false;
    bool strips_policy_branches = policy.tag == JAZZ_PC_SYSTEM || authorization_mode == JAZZ_QAM_CLIENT_LOCAL;
    if (strips_policy_branches) {
        e = QES(policy_stripped_shape)(self, shape, binding, &has_residual, &residual_shape, &residual_binding);
        if (JZ_IS_ERR(e)) goto fail;
        if (has_residual) { shape = &residual_shape; binding = &residual_binding; }
    }
    /* Global-tier client-local receivers must lower concrete bindings into
     * their upstream-scoped settled views rather than evaluate a
     * server-maintained binding graph. A Local-tier maintained client
     * subscription has no settled view and reads unfiltered local data, so its
     * bindings can share one prepared shape, like serving bindings do. */
    bool client_local = authorization_mode == JAZZ_QAM_CLIENT_LOCAL;
    bool client_local_prepared = client_local && tier == JAZZ_TIER_LOCAL && jazz_protocol__protocol__ReadViewSpec__is_default(read_view)
        && output == JZN_CQPO_MAINTAINED_VIEW;
    bool use_prepared_binding_source = (!client_local || client_local_prepared) && !force_inline_binding_source
        && QES(can_use_prepared_current_query_plan)(self, shape) && !settled_binding_view && output != JZN_CQPO_RELATION_SNAPSHOT;
    if (!use_prepared_binding_source) {
        const jazz_model_RuntimeSchema *read_schema = catalogue_schema_for(self, shape->schema_version);
        if (!read_schema) { e = invalid_stored("query schema version is unknown"); goto fail; }
        e = BND(inline_snapshot_bind_filter_literals)(shape, binding, read_schema, &lowered_shape);
        if (JZ_IS_ERR(e)) goto fail;
        e = jazz_model__query__validation__ValidatedQuery__bind(&lowered_shape, empty_values(), &lowered_binding);
        if (JZ_IS_ERR(e)) { e.code = JZN_E_Query; jazz_model_ValidatedQuery_drop(&lowered_shape); goto fail; }
        has_lowered = true;
        shape = &lowered_shape; binding = &lowered_binding;
    }
    e = NMN(normalized_row_set_shape)(self, shape, binding, &input_shape);
    if (JZ_IS_ERR(e)) goto fail;
    has_input = true;
    /* A cold receiver is compiled before RegisterShape is processed. Its
     * exact requested shape already identifies the input window; registry
     * timing must not decide whether to apply its offset twice. */
    bool settled_view_matches_query_window = settled_binding_view && jazz_model_ShapeId_eq(settled_binding_view->shape_id, requested_shape_id);
    if (settled_view_matches_query_window && !shape->query.aggregate) {
        jazz_engine_RowSetExpr *root = jz_btree_get(&input_shape.nodes, &input_shape.root);
        /* Covered inputs for this exact root occurrence are already the output
         * of the authority window. Evaluate order and limit with the ordinary
         * Groove operator, relative to that page. Keeping the limit also
         * bounds newly inserted pending overlay rows. Local-first has no
         * settled source and retains its absolute offset. */
        if (root && root->tag == JAZZ_RSE_SLICE) root->offset = 0;
    }
    jazz_types_SchemaVersionId policy_schema_version =
        jazz_node__node__policy__NodeState__read_policy_schema_for_table_name(self, SL(shape->query.table), shape->schema_version, &input_shape);
    binding_claim_params = BND(binding_claim_params_for_shape)(&input_shape, &shape->params);
    has_claims = true;
    if (use_prepared_binding_source && !client_local) {
        /* Resolve policies exactly as the nested authorization subplans do.
         * Deployed permissions live only in the active compiled view; the
         * catalogue entry for the same version carries structural schema with
         * placeholder policies. Collecting from that entry would omit claim
         * slots which a nested read policy later requires from this shared
         * binding descriptor. */
        const jazz_model_RuntimeSchema *policy_schema = schema_for_policy_version(self, policy_schema_version, NULL);
        if (!policy_schema) {
            if (jazz_types_SchemaVersionId_eq(policy_schema_version, self->catalogue.local_schema_version_id))
                policy_schema = jazz_node__node__state__catalogue__SchemaCatalogue__schema(&self->catalogue);
            else {
                policy_schema = catalogue_schema_for(self, policy_schema_version);
                if (!policy_schema) { e = invalid_stored("policy schema version is unknown"); goto fail; }
            }
        }
        e = NMN(collect_policy_dependency_claim_params)(self, policy_schema, &policy, &input_shape, &binding_claim_params);
        if (JZ_IS_ERR(e)) goto fail;
    }
    /* System reads bypass policy evaluation and have no session from which a
     * prepared claim can be bound. A policy-derived claim slot must therefore
     * never survive into their shared descriptor. Client-local reads evaluate
     * no read policy, so they collect no policy-dependency claims above, but a
     * claim the query itself reads stays a binding slot and is bound from the
     * reader's session. */
    if (policy.tag == JAZZ_PC_SYSTEM) jz_btree_clear(&binding_claim_params);
    if (use_prepared_binding_source)
        has_source_shape = jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_parts_if_needed(&shape->params, &binding_claim_params, &source_shape);
    /* Prepared binding-source names are runtime identities. On a client,
     * equal author identities may hold distinct authenticated sessions: give
     * their claim scopes separate source identities so a later session cannot
     * replace an already-maintained sibling binding.
     *
     * Trusted serving shares one source across sessions. Claim values bound as
     * slots are part of a session's binding key, so sessions with different
     * claims hold different bindings of one shared graph; a claim still
     * inlined as a literal makes the graph itself distinct. A per-session name
     * would give every connected user a private copy of the whole compiled
     * program and its IVM graph (#4017). */
    if (has_source_shape && client_local) {
        jz_str scope;
        if (jazz_node__node__state__lifecycle__NodeState__active_session_claim_scope_key(self, identity, &scope)) {
            jz_str named = jz_format("%.*s:session:%.*s", (int)source_shape.len, source_shape.ptr, (int)scope.len, scope.ptr);
            jz_str_drop(&source_shape); jz_str_drop(&scope);
            source_shape = named;
        }
    }
    if (jazz_types__debug_env__covered_input_trace()) {
        jz_str id = {0}, pol = {0};
        jazz_types_AuthorSubject_debug(&id, identity);
        jazz_engine_PolicyContext_debug(&pol, &policy);
        jz_str line = jz_format("JAZZ_COVERED_INPUT_TRACE stage=program_scope identity=%.*s mode=%s prepared=%s source_shape=%s%.*s%s strips_policy_branches=%s query_policy_branches=%u query_includes=%u policy=%.*s",
            (int)id.len, id.ptr, authorization_mode == JAZZ_QAM_CLIENT_LOCAL ? "ClientLocal" : "TrustedServing",
            use_prepared_binding_source ? "true" : "false", has_source_shape ? "Some(\"" : "None",
            has_source_shape ? (int)source_shape.len : 0, has_source_shape ? source_shape.ptr : "", has_source_shape ? "\")" : "",
            strips_policy_branches ? "true" : "false", shape->query.policy_branches.len, shape->query.includes.len, (int)pol.len, pol.ptr);
        jazz_types__debug_env__eprint(line);
        jz_str_drop(&id); jz_str_drop(&pol);
    }
    const jazz_model_RuntimeSchema *query_schema = schema_for_policy_version(self, shape->schema_version, NULL);
    if (!query_schema) {
        query_schema = catalogue_schema_for(self, shape->schema_version);
        if (!query_schema) { e = invalid_stored("query schema version is unknown"); goto fail; }
    }
    const jazz_model_TableSchema *root_table;
    e = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(self, SL(shape->query.table), shape->schema_version, &root_table);
    if (JZ_IS_ERR(e)) goto fail;
    bool root_has_read_policy = root_table->read_policy != NULL;
    /* A strict receiver obtains its result only by replacing its
     * descriptor-bound CoveredInput sources from this exact settled authority
     * view. Dropping version witnesses here would leave it with no source
     * registry and tempt the runtime to reopen local storage for an authority
     * reset. Local-first keeps ordinary local storage as its source for its
     * entire lifetime, including when propagation is enabled. It may load
     * exact result payloads from that storage rather than retaining every
     * source payload in every query. Cold remote receivers still retain
     * witnesses even before their first authority receipt; they may not fall
     * back to storage. */
    bool storage_backed_result_materialization = output == JZN_CQPO_MAINTAINED_VIEW
        && (authorization_mode != JAZZ_QAM_CLIENT_LOCAL || tier == JAZZ_TIER_LOCAL)
        && !settled_binding_view && !root_has_read_policy
        && QES(storage_backed_maintained_root_has_identity_table_mapping)(self, shape->schema_version, &shape->query.table)
        && NM(storage_backed_maintained_view_eligible)(&shape->query, tier, read_view, &input_shape);
    jz_btree extra; jz_btree_init(&extra, &jazz_engine_ColumnTypes_desc);
    jazz_engine_ProgramBinding program_binding;
    e = jazz_node__node__query_eval__bindings__NodeState__program_binding_for_shape_and_policy_with_prepared_claim_mode(self, shape, binding,
        has_source_shape ? &source_shape : NULL, extra, binding_claim_params, &policy, prepared_claim_binding_mode, &program_binding);
    has_claims = false;
    if (JZ_IS_ERR(e)) goto fail;
    /* Client-local and trusted-serving plans never share a binding source:
     * their graphs differ in policy and source authority. Namespace the final
     * name, since System authority re-derives it above. */
    if (client_local && program_binding.has_source_shape) jz_str_push_cstr(&program_binding.source_shape, ":client-local");
    jazz_engine_RowSetProgramInput input = { input_shape, program_binding };
    has_input = false;
    jazz_engine_RowSetOutputRequest output_request;
    e = NM(current_query_output_request)(output, &shape->query, &output_request);
    if (JZ_IS_ERR(e)) { jazz_engine_NormalizedRowSetShape_drop(&input.shape); jazz_engine_ProgramBinding_drop(&input.binding); goto fail; }
    if (storage_backed_result_materialization) {
        /* A simple current root query carries the exact visible content
         * transaction in its result-member terminal. Keeping every source
         * version/replacement body in every binding merely so the facade can
         * re-read that immutable version multiplies retained state by source
         * rows x bindings. The member's exact identity is enough to load the
         * immutable body from the node store on entry; deletion/restore
         * removals need only retire the old occurrence, never materialize a
         * newer winner. */
        jazz_engine_ProgramFactKey k; __builtin_memset(&k, 0, sizeof k); k.tag = JAZZ_PFK_VERSION_WITNESSES;
        jz_btree_remove(&output_request.facts, &k, NULL, NULL);
    }
    jazz_engine_RequestedReadSet reads;
    e = QRS(query_read_set_for_read_view)(&input.shape, shape->schema_version, policy_schema_version, tier, read_view, settled_binding_view, NULL, query_schema, &reads);
    if (JZ_IS_ERR(e)) {
        jazz_engine_NormalizedRowSetShape_drop(&input.shape); jazz_engine_ProgramBinding_drop(&input.binding);
        jz_btree_drop(&output_request.facts); goto fail;
    }
    out->authorization_mode = authorization_mode;
    out->reads = reads;
    out->policy = policy;
    out->input = input;
    out->output = output_request;
    if (has_source_shape) jz_str_drop(&source_shape);
    if (has_lowered) { jazz_model_Binding_drop(&lowered_binding); jazz_model_ValidatedQuery_drop(&lowered_shape); }
    if (has_residual) { jazz_model_Binding_drop(&residual_binding); jazz_model_ValidatedQuery_drop(&residual_shape); }
    return JZ_OK;
fail:
    if (has_source_shape) jz_str_drop(&source_shape);
    if (has_claims) jz_btree_drop(&binding_claim_params);
    if (has_input) jazz_engine_NormalizedRowSetShape_drop(&input_shape);
    if (has_lowered) { jazz_model_Binding_drop(&lowered_binding); jazz_model_ValidatedQuery_drop(&lowered_shape); }
    if (has_residual) { jazz_model_Binding_drop(&residual_binding); jazz_model_ValidatedQuery_drop(&residual_shape); }
    jazz_engine_PolicyContext_drop(&policy);
    return e;
}

/* ---------- client receiver inputs and one-shot reads ---------- */

typedef struct { jazz_engine_SourceId source; groove_RecordDescriptor descriptor; } Occurrence;
typedef JZ_VEC(Occurrence) OccurrenceVec;
typedef JZ_VEC(jazz_engine_SourceRequest) SourceRequestVec;
static void occurrence_drop(void *p) { jazz_engine_SourceId_drop(&((Occurrence *)p)->source); }
static void metadata_set_drop(void *p) { jz_btree_drop((jz_btree *)p); }
static const jz_btree_desc table_metadata_desc = { sizeof(jz_str), sizeof(jz_btree), jz_cmp_str, str_drop_v, metadata_set_drop };

/* Allocate exact receiver input capabilities from compiler-owned source
 * requirements, before lowering the receiver graph. This deliberately does
 * not compile a placeholder SettledBindingView from result members merely to
 * discover its descriptor. */
jz_err QES(allocate_client_receiver_input_sources)(jazz_node_NodeState *self, const jazz_engine_QueryProgramRequest *request,
    jz_btree *runtime_sources_out, jz_btree *runtime_source_descriptors_out, jz_btree *sources_out) {
    jz_btree runtime_sources; jz_btree_init(&runtime_sources, &jazz_node_SourceGraphs_desc);
    jz_btree runtime_source_descriptors; jz_btree_init(&runtime_source_descriptors, &jazz_node_SourceDescriptors_desc);
    jz_btree sources; jz_btree_init(&sources, &jazz_node_CoveredInputSources_desc);
    /* Share only within this exact receiver authority scope/read schema.
     * Occurrences retain their own row shapes and downstream operators; the
     * input carries the union of their metadata, encoded once per row. */
    jz_btree table_metadata; jz_btree_init(&table_metadata, &table_metadata_desc);
    OccurrenceVec occurrences = {0};
    SourceRequestVec source_requests = {0};
    jazz_engine_CapabilityReport *report = NULL;
    jz_err e = jazz_engine__query_engine__lowering__query_program_source_requests(request, (void *)&source_requests, &report);
    if (JZ_IS_ERR(e)) { e = capability_error_from_report(report); goto fail; }
    jazz_types_SchemaVersionId read_schema = request->reads.primary.read_schema;
    for (u32 i = 0; i < source_requests.len; i++) {
        jazz_engine_SourceRequest *sr = &source_requests.ptr[i];
        if (sr->visibility != JAZZ_VIS_VISIBLE) continue;
        const jazz_engine_SourceExpr *expression = jz_btree_get(&request->reads.primary.sources, &sr->source);
        if (!expression) continue;
        if (expression->tag == JAZZ_SE_WITH_OVERLAYS && expression->overlays.entries.len == 1
            && expression->overlays.entries.ptr[0].tag == 0 /* OverlayRef::PendingLocal */)
            expression = expression->input;
        bool settled = expression->tag == JAZZ_SE_SETTLED_BINDING_VIEW;
        if (!settled) continue;
        const jazz_model_TableSchema *table;
        e = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(self, SL(sr->source.table), read_schema, &table);
        if (JZ_IS_ERR(e)) goto fail;
        jz_btree metadata = RS(covered_input_source_metadata)(&sr->requirements, table);
        groove_RecordDescriptor descriptor = RS(current_row_descriptor_with_hidden_source_fields_for_current_storage)(table, &metadata);
        jz_str tkey = jz_str_clone(&sr->source.table);
        bool inserted;
        jz_btree *slot = jz_btree_entry(&table_metadata, &tkey, &inserted);
        if (inserted) jz_btree_init(slot, metadata.d); else jz_str_drop(&tkey);
        jz_btree_append(slot, &metadata);
        jz_btree_drop(&metadata);
        Occurrence o; jazz_engine_SourceId_clone(&o.source, &sr->source); o.descriptor = descriptor;
        JZ_VEC_PUSH(occurrences, o);
    }
    jz_str *table_name; jz_btree *metadata;
    JZ_BTREE_FOR(&table_metadata, table_name, metadata) {
        const jazz_model_TableSchema *table;
        e = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(self, SL(*table_name), read_schema, &table);
        if (JZ_IS_ERR(e)) goto fail;
        groove_RecordDescriptor descriptor = RS(current_row_descriptor_with_hidden_source_fields_for_current_storage)(table, metadata);
        groove_InputSourceId id = groove__db__Database__allocate_input_source(&self->database, descriptor);
        jazz_types_PhysicalTableId pid;
        e = jazz_node__node__physical__NodeState__scope_physical_table(self, read_schema, SL(*table_name), &pid);
        if (JZ_IS_ERR(e)) goto fail;
        jazz_node_CoveredInputSource src = { id, descriptor, jz_str_clone(table_name) };
        jz_btree_insert(&sources, &pid, &src, NULL);
    }
    for (u32 i = 0; i < occurrences.len; i++) {
        Occurrence *o = &occurrences.ptr[i];
        jazz_types_PhysicalTableId pid;
        e = jazz_node__node__physical__NodeState__scope_physical_table(self, read_schema, SL(o->source.table), &pid);
        if (JZ_IS_ERR(e)) goto fail;
        const jazz_node_CoveredInputSource *shared = jz_btree_get(&sources, &pid);
        if (!shared) jz_panic("covered input source for occurrence");
        groove_GraphBuilder *graph = groove__ivm__graph__GraphBuilder__input_source(shared->id, shared->descriptor);
        if (shared->descriptor != o->descriptor) {
            const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(o->descriptor);
            jz_vec_str names = {0};
            for (u32 f = 0; f < fields->len; f++) {
                if (!fields->ptr[f].has_name) jz_panic("covered input fields are named");
                JZ_VEC_PUSH(names, jz_str_clone(&fields->ptr[f].name));
            }
            graph = groove__ivm__graph__GraphBuilder__project(graph, names.ptr, names.len);
            jz_vec_drop(&names, sizeof(jz_str), str_drop_v);
        }
        jazz_engine_SourceId k1, k2; jazz_engine_SourceId_clone(&k1, &o->source); jazz_engine_SourceId_clone(&k2, &o->source);
        jz_btree_insert(&runtime_sources, &k1, &graph, NULL);
        jz_btree_insert(&runtime_source_descriptors, &k2, &o->descriptor, NULL);
    }
    jz_vec_drop(&occurrences, sizeof(Occurrence), occurrence_drop);
    jz_vec_drop(&source_requests, sizeof(jazz_engine_SourceRequest), jazz_engine_SourceRequest_drop);
    jz_btree_drop(&table_metadata);
    *runtime_sources_out = runtime_sources; *runtime_source_descriptors_out = runtime_source_descriptors; *sources_out = sources;
    return JZ_OK;
fail:
    jz_vec_drop(&occurrences, sizeof(Occurrence), occurrence_drop);
    jz_vec_drop(&source_requests, sizeof(jazz_engine_SourceRequest), jazz_engine_SourceRequest_drop);
    jz_btree_drop(&table_metadata);
    jz_btree_drop(&runtime_sources); jz_btree_drop(&runtime_source_descriptors); jz_btree_drop(&sources);
    return e;
}

/* Compilation/closure validation failures happen before these runtime inputs
 * are owned by a subscription, so retire the allocations here. */
jz_err QES(retire_covered_input_sources)(jazz_node_NodeState *self, const jz_btree *sources) {
    JZ_VEC(groove_InputSourceId) ids = {0};
    void *k; jazz_node_CoveredInputSource *src;
    JZ_BTREE_FOR(sources, k, src) JZ_VEC_PUSH(ids, src->id);
    jz_err e = jzn_groove(groove__db__Database__retire_input_sources(&self->database, ids.ptr, ids.len));
    jz_free(ids.ptr);
    return e;
}

/* Recompile one client-side settled read against ephemeral, exact
 * CoveredInput sources. A one-shot is not allowed to materialize an authority
 * result set: it uses the same residual source descriptors as a maintained
 * receiver, installs the exact closure, evaluates once, and retires its
 * runtime-local sources immediately afterwards.
 *
 * None means the named authority result has not yet claimed a complete
 * closure. Callers preserve normal remote pending behavior in that case; they
 * must not fall back to retained result members or storage. */
jz_err QES(compile_client_one_shot_with_covered_inputs)(jazz_node_NodeState *self, jazz_engine_QueryProgramRequest request, jz_btree access_paths,
    jazz_types_SchemaVersionId result_schema_version, const jazz_protocol_ReadViewSpec *read_view, const jazz_protocol_AuthorityResultKey *authority_result_key,
    bool *has, jazz_engine_QueryProgram *program_out, jazz_node_CoveredInputReceiver **receiver_out) {
    *has = false;
    jz_btree runtime_sources, runtime_source_descriptors, sources;
    jz_err e = QES(allocate_client_receiver_input_sources)(self, &request, &runtime_sources, &runtime_source_descriptors, &sources);
    if (JZ_IS_ERR(e)) { jazz_engine_QueryProgramRequest_drop(&request); jz_btree_drop(&access_paths); return e; }
    if (sources.len == 0) {
        jz_btree_drop(&runtime_sources); jz_btree_drop(&runtime_source_descriptors); jz_btree_drop(&sources);
        jazz_engine_QueryProgramRequest_drop(&request); jz_btree_drop(&access_paths);
        return invalid_stored("client settled read has no covered-input source occurrences");
    }
    jz_btree inline_sources; jz_btree_init(&inline_sources, &jazz_node_CurrentRowsBySource_desc);
    jazz_engine_QueryProgram program;
    e = LWN(compile_query_program_request_with_inline_sources_access_paths_and_covered_inputs)(self, request, inline_sources, access_paths,
        runtime_sources, runtime_source_descriptors, &program);
    if (JZ_IS_ERR(e)) {
        JZ_TRY(QES(retire_covered_input_sources)(self, &sources));
        jz_btree_drop(&sources);
        return e;
    }
    jazz_protocol_ReadViewSpec rv; jazz_protocol_ReadViewSpec_clone(&rv, read_view);
    jazz_node_CoveredInputReceiver *receiver = jazz_node__node__query_eval__maintained_views__CoveredInputReceiver__new(sources, rv);
    bool installed = false;
    e = jazz_node__node__query_eval__maintained_views__NodeState__replace_covered_input_receiver(self, receiver, result_schema_version, authority_result_key, &installed);
    if (JZ_IS_ERR(e) || !installed) {
        JZ_TRY(QES(retire_covered_input_sources)(self, jazz_node__node__query_eval__maintained_views__CoveredInputReceiver__sources(receiver)));
        jazz_node_CoveredInputReceiver_free(receiver);
        jazz_engine_QueryProgram_drop(&program);
        return e;
    }
    *program_out = program; *receiver_out = receiver; *has = true;
    return JZ_OK;
}

jz_err QES(query_rows_with_prepared_plan)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, const void *prepared_plan, jazz_node_CurrentRowVec *out) {
    jazz_types_AuthorSubject system = { JAZZ_AUTHOR_SYSTEM, {{0}} };
    return QES(query_rows_with_prepared_plan_for_identity)(self, shape, binding, tier, prepared_plan, system, out);
}

jz_err QES(query_rows_with_prepared_plan_for_identity)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, const void *prepared_plan, jazz_types_AuthorSubject identity, jazz_node_CurrentRowVec *out) {
    return QES(query_rows_with_options_for_identity)(self, shape, binding, tier, prepared_plan, identity, false, JAZZ_QAM_TRUSTED_SERVING, NULL, out);
}

/* Execute an ordinary local client read. The upstream serving host is the
 * confidentiality boundary; this path must not re-evaluate row policy. */
jz_err QES(query_rows_for_client)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_node_CurrentRowVec *out) {
    return QES(query_rows_with_options_for_identity)(self, shape, binding, tier, NULL, identity, false, JAZZ_QAM_CLIENT_LOCAL, NULL, out);
}

jz_err QES(query_rows_local_preview)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    const void *prepared_plan, jazz_node_CurrentRowVec *out) {
    return QES(query_rows_with_prepared_plan)(self, shape, binding, JAZZ_TIER_LOCAL, prepared_plan, out);
}

jz_err QES(query_rows_local_preview_profiled)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    const void *prepared_plan, jazz_node_CurrentRowVec *out, jazz_node_QueryReadProfile *profile) {
    jazz_types_AuthorSubject system = { JAZZ_AUTHOR_SYSTEM, {{0}} };
    return QES(query_rows_with_options_for_identity_profiled)(self, shape, binding, JAZZ_TIER_LOCAL, prepared_plan, system, out, profile);
}

jz_err QES(query_rows_including_deleted_in_authorization_mode)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, const void *prepared_plan, jazz_types_AuthorSubject identity, jazz_engine_QueryAuthorizationMode authorization_mode,
    jazz_node_CurrentRowVec *out) {
    return QES(query_rows_with_options_for_identity)(self, shape, binding, tier, prepared_plan, identity, true, authorization_mode, NULL, out);
}

#define MAT(x) jazz_node__node__query_eval__materialization__NodeState__##x

jz_err QES(query_rows_with_options_for_identity)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape_in, const jazz_model_Binding *binding_in,
    jazz_model_DurabilityTier tier, const void *prepared_plan, jazz_types_AuthorSubject identity, bool include_deleted,
    jazz_engine_QueryAuthorizationMode authorization_mode, jazz_node_QueryReadProfile *profile, jazz_node_CurrentRowVec *out) {
    (void)prepared_plan;
    u64 phase_started = profile ? jz_now_ms() : 0;
    if (include_deleted) {
        jazz_protocol_ReadViewSpec rv = jazz_protocol_ReadViewSpec_default();
        jazz_node_CurrentRowVec rows = {0};
        jz_err e = QES(query_rows_including_deleted_with_query_engine)(self, shape_in, binding_in, tier, identity, authorization_mode, &rv, &rows);
        jazz_protocol_ReadViewSpec_drop(&rv);
        JZ_TRY(e);
        e = MAT(finish_engine_query_rows_in_schema)(self, &shape_in->query, shape_in->schema_version, &rows);
        if (!JZ_IS_ERR(e)) e = MAT(apply_projection_in_schema)(self, &shape_in->query, shape_in->schema_version, rows.ptr, rows.len);
        if (JZ_IS_ERR(e)) { jazz_node_CurrentRowVec_drop(&rows); return e; }
        *out = rows;
        return JZ_OK;
    }
    bool has;
    if (authorization_mode == JAZZ_QAM_TRUSTED_SERVING && tier == JAZZ_TIER_GLOBAL) {
        JZ_TRY(QES(try_uuid_page_probe)(self, shape_in, binding_in, identity, &has, out));
        if (has) return JZ_OK;
        JZ_TRY(QES(try_ordered_page_probe)(self, shape_in, binding_in, identity, &has, out));
        if (has) return JZ_OK;
    }
    jazz_node_ClientSettledBindingView client_view; bool has_client_view = false;
    if (authorization_mode == JAZZ_QAM_CLIENT_LOCAL) {
        jazz_protocol_ReadViewSpec rv = jazz_protocol_ReadViewSpec_default();
        has_client_view = QES(client_settled_binding_view_for_query)(self, shape_in, binding_in, tier, &rv, &client_view);
        jazz_protocol_ReadViewSpec_drop(&rv);
    }
    /* A serving node evaluates its complete authority program. A
     * SettledBindingView is a receiver-local CoveredInput source, not a
     * server-side cache or an alternate trusted read path. */
    const jazz_protocol_BindingViewKey *settled_binding_view =
        authorization_mode == JAZZ_QAM_CLIENT_LOCAL && has_client_view ? &client_view.key : NULL;
    jz_err e = JZ_OK;
    jazz_protocol_AuthorityResultKey settled_authority_result_key; bool has_authority_key = false;
    jazz_model_ValidatedQuery rebased_shape; jazz_model_Binding rebased_binding; bool has_rebased = false;
    jazz_node_CoveredInputReceiver *ephemeral_covered_inputs = NULL;
    *out = (jazz_node_CurrentRowVec){0};
    /* Ordinary Global reads are allowed to consume only a source binding view
     * registered by upstream coverage. A client-local plan without that
     * host-owned route must not fall back to its raw overlay. */
    if (authorization_mode == JAZZ_QAM_CLIENT_LOCAL && tier >= JAZZ_TIER_GLOBAL && !settled_binding_view) goto done;
    /* A binding-view name is a routing key, not authorization evidence.
     * Client-side source installation must select the one exact,
     * policy-scoped receipt that owns that view before it can evaluate a
     * one-shot graph. */
    if (authorization_mode == JAZZ_QAM_CLIENT_LOCAL && settled_binding_view)
        has_authority_key = jazz_node__node__query_eval__subscriptions__NodeState__unique_authority_result_key_for_binding_view(self, settled_binding_view, &settled_authority_result_key);
    if (authorization_mode == JAZZ_QAM_CLIENT_LOCAL && settled_binding_view && !has_authority_key) goto done;
    const jazz_model_ValidatedQuery *shape = shape_in; const jazz_model_Binding *binding = binding_in;
    if (has_client_view && client_view.retained_window) {
        const jazz_model_RuntimeSchema *schema = catalogue_schema_for(self, shape_in->schema_version);
        if (!schema) { e = invalid_stored("query schema version is unknown"); goto done; }
        jazz_model_Query rebased_query; jazz_model_Query_clone(&rebased_query, &shape_in->query);
        u32 relative_offset, relative_limit; bool has_relative_limit;
        jazz_node__node__RetainedRootWindowSource__relative_window_for(client_view.retained_window, shape_in, &relative_offset, &has_relative_limit, &relative_limit);
        rebased_query.offset = relative_offset;
        rebased_query.has_limit = has_relative_limit; rebased_query.limit = relative_limit;
        e = jazz_model__query__ast__Query__validate_with_schema_version(&rebased_query, schema, shape_in->schema_version, &rebased_shape);
        jazz_model_Query_drop(&rebased_query);
        if (JZ_IS_ERR(e)) { e.code = JZN_E_Query; goto done; }
        e = jazz_model__query__validation__ValidatedQuery__bind(&rebased_shape, clone_values(&binding_in->values), &rebased_binding);
        if (JZ_IS_ERR(e)) { e.code = JZN_E_Query; jazz_model_ValidatedQuery_drop(&rebased_shape); goto done; }
        has_rebased = true;
        shape = &rebased_shape; binding = &rebased_binding;
    }
    if (profile) profile->resolve_view = jz_now_ms() - phase_started;
    phase_started = profile ? jz_now_ms() : 0;
    jazz_engine_QueryProgram program;
    if (has_authority_key) {
        jazz_protocol_ReadViewSpec rv = jazz_protocol_ReadViewSpec_default();
        jazz_engine_QueryProgramRequest request;
        e = QES(current_query_program_request)(self, shape, binding, tier, identity, JZN_CQPO_APP_ROWS, &rv, settled_binding_view, authorization_mode, &request);
        if (JZ_IS_ERR(e)) { jazz_protocol_ReadViewSpec_drop(&rv); goto done; }
        jz_btree access_paths;
        e = RSN(current_query_hydration_access_paths)(self, &request, shape, binding, JAZZ_HL_FIRST_RESULT, &access_paths);
        if (JZ_IS_ERR(e)) { jazz_engine_QueryProgramRequest_drop(&request); jazz_protocol_ReadViewSpec_drop(&rv); goto done; }
        bool found;
        e = QES(compile_client_one_shot_with_covered_inputs)(self, request, access_paths, shape->schema_version, &rv, &settled_authority_result_key,
            &found, &program, &ephemeral_covered_inputs);
        jazz_protocol_ReadViewSpec_drop(&rv);
        if (JZ_IS_ERR(e) || !found) goto done;
    } else {
        e = QES(compile_current_query_program_for_one_shot_read)(self, shape, binding, tier, identity, settled_binding_view, authorization_mode, &program);
        if (JZ_IS_ERR(e)) goto done;
    }
    jazz_engine_AppRowSchema app_output;
    e = LW(materialization_app_row_schema)(NULL, &program, &app_output);
    if (JZ_IS_ERR(e)) { jazz_engine_QueryProgram_drop(&program); goto retire; }
    if (profile) profile->compile_program = jz_now_ms() - phase_started;
    phase_started = profile ? jz_now_ms() : 0;
    groove_RootIndirectValues root_indirect_values;
    e = QES(projection_dropped_root_values)(self, &shape->query, shape->schema_version, &root_indirect_values);
    if (JZ_IS_ERR(e)) { jazz_engine_QueryProgram_drop(&program); jazz_engine_AppRowSchema_drop(&app_output); goto retire; }
    groove_RecordDeltas *deltas = NULL;
    jz_err deltas_result = LWN(hydrate_lowered_program_once)(self, program, binding, root_indirect_values, &deltas);
    /* Retire transient receiver inputs even if the one-shot graph itself
     * fails. These identities are runtime-local capabilities and must never
     * be re-used by a later receipt. */
    jz_err retire_result = JZ_OK;
    if (ephemeral_covered_inputs) {
        retire_result = QES(retire_covered_input_sources)(self, jazz_node__node__query_eval__maintained_views__CoveredInputReceiver__sources(ephemeral_covered_inputs));
        jazz_node_CoveredInputReceiver_free(ephemeral_covered_inputs);
        ephemeral_covered_inputs = NULL;
    }
    if (JZ_IS_ERR(deltas_result)) { jz_err_drop(&retire_result); jazz_engine_AppRowSchema_drop(&app_output); e = deltas_result; goto done; }
    if (JZ_IS_ERR(retire_result)) { groove_RecordDeltas_drop(deltas); jz_free(deltas); jazz_engine_AppRowSchema_drop(&app_output); e = retire_result; goto done; }
    if (profile) profile->execute_plan = jz_now_ms() - phase_started;
    jazz_model_TableSchema table_schema;
    e = MAT(query_output_table)(self, &shape->query, shape->schema_version, &table_schema);
    if (!JZ_IS_ERR(e)) {
        e = QES(materialize_and_finalize_query_rows)(self, &shape->query, shape->schema_version, &table_schema, &app_output, deltas, profile, out);
        jazz_model_TableSchema_drop(&table_schema);
    }
    groove_RecordDeltas_drop(deltas); jz_free(deltas);
    jazz_engine_AppRowSchema_drop(&app_output);
    goto done;
retire:
    if (ephemeral_covered_inputs) {
        jz_err r = QES(retire_covered_input_sources)(self, jazz_node__node__query_eval__maintained_views__CoveredInputReceiver__sources(ephemeral_covered_inputs));
        jz_err_drop(&r);
        jazz_node_CoveredInputReceiver_free(ephemeral_covered_inputs);
    }
done:
    if (has_rebased) { jazz_model_Binding_drop(&rebased_binding); jazz_model_ValidatedQuery_drop(&rebased_shape); }
    if (has_authority_key) jazz_protocol_AuthorityResultKey_drop(&settled_authority_result_key);
    if (has_client_view) jazz_node_ClientSettledBindingView_drop(&client_view);
    return e;
}

static void str_set_drop_rc(void *p) { jz_btree_drop((jz_btree *)p); }

/* Root fields a one-shot read can leave as physical large-value descriptors:
 * stored columns that materialize_and_finalize_query_rows projects away
 * without reading them first. Rebuilding such a value only to drop it made a
 * projected listing scale with the size of the columns it excluded (#3471).
 *
 * Every other field, including ordering keys that the in-memory sort re-reads,
 * stays materialized. Structured, aggregate and joined results keep the
 * complete materialization because their public fields are not the root
 * table's columns. */
jz_err QES(projection_dropped_root_values)(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_types_SchemaVersionId schema_version,
    groove_RootIndirectValues *out) {
    out->tag = GROOVE_RIV_MATERIALIZE; out->physical_fields = NULL;
    if (!query->has_select) return JZ_OK;
    if (query->aggregate || query->relation || query->flat_join || query->array_subqueries.len != 0) return JZ_OK;
    jazz_model_TableSchema table;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema((jazz_node_NodeState *)self, SL(query->table), schema_version, &table));
    jz_btree dropped = empty_str_set();
    for (u32 i = 0; i < table.columns.len; i++) {
        const jz_str *name = &table.columns.ptr[i].name;
        bool selected = false, ordered = false;
        for (u32 s = 0; s < query->select.len; s++) if (jz_str_eq(&query->select.ptr[s], name)) selected = true;
        for (u32 o = 0; o < query->order_by.len; o++) if (jz_str_eq(&query->order_by.ptr[o].column, name)) ordered = true;
        if (selected || ordered) continue;
        jz_str f = jazz_engine__query_engine__fields__user_column_field(SL(*name));
        jz_btree_insert(&dropped, &f, NULL, NULL);
    }
    jazz_model_TableSchema_drop(&table);
    if (dropped.len == 0) { jz_btree_drop(&dropped); return JZ_OK; }
    jz_btree *arc = jz_rc_new(sizeof(jz_btree), str_set_drop_rc);
    *arc = dropped;
    out->tag = GROOVE_RIV_PHYSICAL_FIELDS; out->physical_fields = arc;
    return JZ_OK;
}

/* Root fields a retained maintained view can leave as physical large-value
 * descriptors, for its whole lifetime.
 *
 * Beside the app-row cells named in projection_dropped_root_values, a
 * maintained view's version and replacement witnesses carry every stored
 * column of the root table under its table-qualified field. The receiver
 * decodes those witnesses into history rows, whose canonical form keeps large
 * values physical, and projects the dropped columns away before any row is
 * published. Rebuilding them there made a listing fetch every chunk of each
 * large value it excludes (#3830). */
jz_err QES(maintained_projection_dropped_root_values)(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_types_SchemaVersionId schema_version,
    groove_RootIndirectValues *out) {
    groove_RootIndirectValues base;
    JZ_TRY(QES(projection_dropped_root_values)(self, query, schema_version, &base));
    out->tag = GROOVE_RIV_MATERIALIZE; out->physical_fields = NULL;
    if (base.tag != GROOVE_RIV_PHYSICAL_FIELDS) return JZ_OK;
    jazz_model_TableSchema table;
    jz_err e = jazz_node__node__NodeState__table_in_schema((jazz_node_NodeState *)self, SL(query->table), schema_version, &table);
    if (JZ_IS_ERR(e)) { jz_rc_release(base.physical_fields); return e; }
    jz_vec_str witnessed = {0};
    for (u32 i = 0; i < table.columns.len; i++) {
        jz_str f = jazz_engine__query_engine__fields__user_column_field(SL(table.columns.ptr[i].name));
        if (jz_btree_contains(base.physical_fields, &f))
            JZ_VEC_PUSH(witnessed, jazz_engine__query_engine__fields__table_user_column_field(SL(table.name), SL(table.columns.ptr[i].name)));
        jz_str_drop(&f);
    }
    jz_btree dropped; jz_btree_clone(&dropped, base.physical_fields, str_clone_v, NULL);
    jz_rc_release(base.physical_fields);
    for (u32 i = 0; i < witnessed.len; i++) jz_btree_insert(&dropped, &witnessed.ptr[i], NULL, NULL);
    jz_free(witnessed.ptr);
    jazz_model_TableSchema_drop(&table);
    jz_btree *arc = jz_rc_new(sizeof(jz_btree), str_set_drop_rc);
    *arc = dropped;
    out->tag = GROOVE_RIV_PHYSICAL_FIELDS; out->physical_fields = arc;
    return JZ_OK;
}

/* Materialize one-shot current rows and expose the canonical public result.
 * Passing no profile keeps the ordinary read path clock-free. */
jz_err QES(materialize_and_finalize_query_rows)(jazz_node_NodeState *self, const jazz_model_Query *query, jazz_types_SchemaVersionId schema_version,
    const jazz_model_TableSchema *table_schema, const jazz_engine_AppRowSchema *app_output, const groove_RecordDeltas *deltas,
    jazz_node_QueryReadProfile *profile, jazz_node_CurrentRowVec *out) {
    u64 phase_started = profile ? jz_now_ms() : 0;
    jazz_node_CurrentRowVec rows = {0};
    jz_err e = JZ_OK;
    if (query->aggregate) {
        JZ_TRY(MAT(materialize_aggregate_query_rows)(self, query, app_output, deltas, &rows));
    } else if (query->flat_join) {
        groove_RecordDeltasIter it = { deltas, 0 }; groove_BorrowedRecord record; i64 weight;
        while (groove__ivm__RecordDeltas__next(&it, &record, &weight)) {
            if (weight <= 0) continue;
            groove_OwnedRecord owned = { jz_bytes_from(record.raw, record.len), record.descriptor };
            JZ_VEC_PUSH(rows, jazz_node__node__CurrentRow__new(&query->table, owned));
        }
    } else {
        groove_RecordDeltasIter it = { deltas, 0 }; groove_BorrowedRecord record; i64 weight;
        while (groove__ivm__RecordDeltas__next(&it, &record, &weight)) {
            if (weight <= 0) continue;
            jazz_node_CurrentRow row, materialized;
            e = jazz_node__node__codec__decode_current_row(table_schema, &record, &row);
            if (JZ_IS_ERR(e)) goto fail;
            e = jazz_node__node__state__commit__NodeState__materialize_current_row(self, table_schema, row, &materialized);
            if (JZ_IS_ERR(e)) goto fail;
            JZ_VEC_PUSH(rows, materialized);
        }
    }
    if (!query->aggregate)
        for (u32 i = 0; i < rows.len; i++) {
            e = MAT(bind_app_row_schema_fields)(&rows.ptr[i], app_output);
            if (JZ_IS_ERR(e)) goto fail;
        }
    /* The graph used for synchronous materialization intentionally retains
     * physical provenance fields so policy witnesses can be resolved above.
     * Do not let that internal descriptor cross the public CurrentRow
     * boundary: subscriptions use the public terminal shape, and native/WASM
     * consumers must see the same layout from both read paths.
     * Relation terminals and tree collectors own their public fields in the
     * app-row descriptor. Those fields are not necessarily columns of the
     * root table, so normalizing such output against that table would
     * silently discard aliases or recursive payload before the client can
     * read it. Flat rows still need this boundary to remove materializer-only
     * physical fields. */
    if (!query->relation && !query->flat_join && query->array_subqueries.len == 0) {
        e = QE(normalize_public_current_rows)(query, table_schema, &rows);
        if (JZ_IS_ERR(e)) goto fail;
    }
    if (profile) profile->decode_materialize = jz_now_ms() - phase_started;
    phase_started = profile ? jz_now_ms() : 0;
    e = MAT(finish_engine_query_rows_in_schema)(self, query, schema_version, &rows);
    if (JZ_IS_ERR(e)) goto fail;
    if (profile) profile->finish_rows = jz_now_ms() - phase_started;
    phase_started = profile ? jz_now_ms() : 0;
    if (!query->flat_join && query->array_subqueries.len == 0) {
        e = MAT(apply_projection_in_schema)(self, query, schema_version, rows.ptr, rows.len);
        if (JZ_IS_ERR(e)) goto fail;
    }
    if (profile) profile->apply_projection = jz_now_ms() - phase_started;
    *out = rows;
    return JZ_OK;
fail:
    jazz_node_CurrentRowVec_drop(&rows);
    return e;
}

jz_err QES(query_rows_with_options_for_identity_profiled)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, const void *prepared_plan, jazz_types_AuthorSubject identity, jazz_node_CurrentRowVec *out, jazz_node_QueryReadProfile *profile_out) {
    u64 started = jz_now_ms();
    jazz_node_QueryReadProfile profile; __builtin_memset(&profile, 0, sizeof profile);
    JZ_TRY(QES(query_rows_with_options_for_identity)(self, shape, binding, tier, prepared_plan, identity, false, JAZZ_QAM_TRUSTED_SERVING, &profile, out));
    profile.total = jz_now_ms() - started;
    *profile_out = profile;
    return JZ_OK;
}

/* ---------- settled binding views ---------- */

void jazz_node_ClientSettledBindingView_drop(jazz_node_ClientSettledBindingView *v) {
    jazz_protocol_BindingViewKey_drop(&v->key);
    if (v->retained_window) jazz_node_RetainedRootWindowSource_free(v->retained_window);
}

/* Select the server-owned result boundary for an ordinary client read.
 *
 * Local and process-only reads intentionally scan the complete local overlay.
 * Global reads consume only the identity-scoped result members emitted by the
 * serving host. This is host-owned routing, not request-controlled
 * authorization. */
bool QES(client_settled_binding_view_key_for_query)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view, jazz_protocol_BindingViewKey *out) {
    jazz_node_ClientSettledBindingView view;
    if (!QES(client_settled_binding_view_for_query)(self, shape, binding, tier, read_view, &view)) return false;
    *out = view.key;
    if (view.retained_window) jazz_node_RetainedRootWindowSource_free(view.retained_window);
    return true;
}

bool QES(client_settled_binding_view_for_query)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view, jazz_node_ClientSettledBindingView *out) {
    (void)self;
    /* A no-tier read is an ordinary process-local overlay read. It has no
     * authority-receipt contract, so constructing a synthetic binding view
     * here would make the caller wait for a receipt that cannot exist and
     * incorrectly hide its local rows. */
    if (tier == JAZZ_TIER_NONE) return false;
    /* Local-first never substitutes a remote scope for local knowledge.
     * Propagation remains enabled independently of this source choice. */
    if (tier == JAZZ_TIER_LOCAL) return false;
    jazz_model_DurabilityTier settled_tier = tier;
    jazz_protocol_RegisterShapeOptions options = jazz_protocol_RegisterShapeOptions_default();
    /* Storage durability does not select a different authority subscription.
     * Consume the exact tier the caller requested. */
    options.tier = settled_tier;
    jazz_protocol_ReadViewSpec_drop(&options.read_view);
    jazz_protocol_ReadViewSpec_clone(&options.read_view, read_view);
    jz_bytes read_view_key = jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&options);
    jazz_protocol_RegisterShapeOptions_drop(&options);
    out->key = jazz_protocol__protocol__BindingViewKey__new(shape->shape_id, jazz_model__query__validation__Binding__binding_id(binding), read_view_key);
    jazz_node_RetainedRootWindowSource *window = jazz_node__node__RetainedRootWindowSource__for_shape(shape);
    if (jazz_node__node__RetainedRootWindowSource__is_bounded(window)) out->retained_window = window;
    else { jazz_node_RetainedRootWindowSource_free(window); out->retained_window = NULL; }
    return true;
}

/* The query with its read-policy alternatives removed, for authorities that
 * do not evaluate them (System and client-local reads), or None when it has
 * none. */
jz_err QES(policy_stripped_shape)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    bool *has, jazz_model_ValidatedQuery *shape_out, jazz_model_Binding *binding_out) {
    *has = false;
    if (shape->query.policy_branches.len == 0) return JZ_OK;
    const jazz_model_RuntimeSchema *schema;
    if (jazz_types_SchemaVersionId_eq(shape->schema_version, self->catalogue.local_schema_version_id))
        schema = jazz_node__node__state__catalogue__SchemaCatalogue__schema(&self->catalogue);
    else {
        schema = catalogue_schema_for(self, shape->schema_version);
        if (!schema) return invalid_stored("query schema version is unknown");
    }
    jazz_model_Query query; jazz_model_Query_clone(&query, &shape->query);
    jazz_model_PolicyBranchVec *branches = (jazz_model_PolicyBranchVec *)&query.policy_branches;
    jazz_model_PolicyBranchVec none = {0};
    jazz_model_PolicyBranchVecVec tmp = {0}; JZ_VEC_PUSH(tmp, *branches); jazz_model_PolicyBranchVecVec_drop(&tmp);
    *branches = none;
    jz_err e = jazz_model__query__ast__Query__validate_with_schema_version(&query, schema, shape->schema_version, shape_out);
    jazz_model_Query_drop(&query);
    if (JZ_IS_ERR(e)) { e.code = JZN_E_Query; return e; }
    jz_btree values = empty_values();
    jz_str *name; groove_Value *value;
    JZ_BTREE_FOR(&binding->values, name, value) {
        if (!jz_btree_contains(&shape_out->params, name)) continue;
        jz_str k = jz_str_clone(name); groove_Value v; groove_Value_clone(&v, value);
        jz_btree_insert(&values, &k, &v, NULL);
    }
    e = jazz_model__query__validation__ValidatedQuery__bind(shape_out, values, binding_out);
    if (JZ_IS_ERR(e)) { e.code = JZN_E_Query; jazz_model_ValidatedQuery_drop(shape_out); return e; }
    *has = true;
    return JZ_OK;
}

/* The binding-source name a prepared Local-tier client-local plan of this
 * query would share, or None when the query has no binding slot. Mirrors the
 * derivation in current_query_program_request_with_prepared_claim_mode. */
jz_err QES(client_local_prepared_source_name)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape_in, const jazz_model_Binding *binding_in,
    jazz_types_AuthorSubject identity, bool *has, jz_str *out) {
    *has = false;
    bool has_residual; jazz_model_ValidatedQuery rshape; jazz_model_Binding rbinding;
    JZ_TRY(QES(policy_stripped_shape)(self, shape_in, binding_in, &has_residual, &rshape, &rbinding));
    const jazz_model_ValidatedQuery *shape = has_residual ? &rshape : shape_in;
    const jazz_model_Binding *binding = has_residual ? &rbinding : binding_in;
    jz_err e = JZ_OK;
    jz_str source_shape; bool has_source = false;
    jazz_engine_PolicyContext policy = jazz_node__node__query_eval__authorization__NodeState__query_program_policy_context(self, identity);
    if (policy.tag == JAZZ_PC_SYSTEM) {
        jz_btree no_claims; jz_btree_init(&no_claims, &jazz_engine_ProgramClaimParams_desc);
        has_source = jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_parts_if_needed(&shape->params, &no_claims, &source_shape);
        jz_btree_drop(&no_claims);
    } else {
        jazz_engine_NormalizedRowSetShape input_shape;
        e = NMN(normalized_row_set_shape)(self, shape, binding, &input_shape);
        if (!JZ_IS_ERR(e)) {
            jz_btree claim_params = BND(binding_claim_params_for_shape)(&input_shape, &shape->params);
            has_source = jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_parts_if_needed(&shape->params, &claim_params, &source_shape);
            jz_btree_drop(&claim_params);
            jazz_engine_NormalizedRowSetShape_drop(&input_shape);
            jz_str scope;
            if (has_source && jazz_node__node__state__lifecycle__NodeState__active_session_claim_scope_key(self, identity, &scope)) {
                jz_str named = jz_format("%.*s:session:%.*s", (int)source_shape.len, source_shape.ptr, (int)scope.len, scope.ptr);
                jz_str_drop(&source_shape); jz_str_drop(&scope);
                source_shape = named;
            }
        }
    }
    jazz_engine_PolicyContext_drop(&policy);
    if (!JZ_IS_ERR(e) && has_source) {
        *out = jz_format("%.*s:client-local", (int)source_shape.len, source_shape.ptr);
        jz_str_drop(&source_shape);
        *has = true;
    }
    if (has_residual) { jazz_model_Binding_drop(&rbinding); jazz_model_ValidatedQuery_drop(&rshape); }
    return e;
}

bool QES(can_use_prepared_current_query_plan)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape) {
    return jazz_types_SchemaVersionId_eq(shape->schema_version, self->catalogue.local_schema_version_id)
        && !QES(required_include_membership_is_identity_sensitive)(self, shape);
}

bool QES(required_include_membership_is_identity_sensitive)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape) {
    (void)self;
    /* Even a target without SELECT is identity-sensitive: ordinary users see
     * no rows while SYSTEM bypasses policy. Never share that required
     * membership through the policy-independent prepared shortcut. */
    for (u32 i = 0; i < shape->query.includes.len; i++) {
        const jazz_model_Include *inc = &shape->query.includes.ptr[i];
        if (inc->require || inc->join_mode == JAZZ_INCLUDE_INNER) return true;
    }
    return false;
}

/* ---------- historical, snapshot and include-deleted reads ---------- */

/* Evaluate a validated query against the globally settled state at position.
 *
 * This is a settled-history read: it considers only transactions with
 * global_time <= position, chooses the ordinary per-row winners from that
 * subset, and evaluates the query against that historical state. */
jz_err QES(query_rows_at)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_GlobalTime position, jazz_node_CurrentRowVec *out) {
    JZ_TRY(jazz_node__node__NodeState__require_catalogue_ready(self));
    jazz_types_AuthorSubject system = { JAZZ_AUTHOR_SYSTEM, {{0}} };
    return QES(query_rows_at_for_identity)(self, shape, binding, position, system, out);
}

jz_err QES(query_rows_at_for_identity)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_GlobalTime position, jazz_types_AuthorSubject identity, jazz_node_CurrentRowVec *out) {
    jazz_node_CurrentRowVec rows;
    JZ_TRY(QES(query_rows_at_with_query_engine)(self, shape, binding, position, identity, &rows));
    const jazz_model_Query *query = &shape->query;
    jz_err e = MAT(finish_engine_query_rows_in_schema)(self, query, shape->schema_version, &rows);
    /* The historical program keeps unselected order keys for the sort above
     * (app_row_payload_projection); drop them from public rows. */
    if (!JZ_IS_ERR(e) && !query->flat_join && query->array_subqueries.len == 0)
        e = MAT(apply_projection_in_schema)(self, query, shape->schema_version, rows.ptr, rows.len);
    if (JZ_IS_ERR(e)) { jazz_node_CurrentRowVec_drop(&rows); return e; }
    *out = rows;
    return JZ_OK;
}

/* Shared tail of the historical / snapshot reads: run the lowered app-row
 * graph once and materialize its rows. */
static jz_err run_app_rows_program(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *lowered_shape, jazz_engine_QueryProgram *program,
    bool materialization_graph, bool include_deleted, const jazz_model_TableSchema *table_in, jazz_node_CurrentRowVec *out) {
    groove_GraphBuilder *graph;
    jz_err e = materialization_graph ? LW(lowered_materialization_app_rows_graph)(program, &graph) : LW(lowered_app_rows_graph)(program, &graph);
    if (JZ_IS_ERR(e)) return e;
    groove_RecordDeltas deltas;
    JZ_TRY(jzn_groove(groove__db__Database__query_graph_deltas(&self->database, graph, &deltas)));
    const jazz_model_Query *query = &lowered_shape->query;
    if (query->aggregate) {
        jazz_engine_AppRowSchema schema;
        e = LW(materialization_app_row_schema)(NULL, program, &schema);
        if (!JZ_IS_ERR(e)) { e = MAT(materialize_aggregate_query_rows)(self, query, &schema, &deltas, out); jazz_engine_AppRowSchema_drop(&schema); }
    } else {
        jazz_model_TableSchema table;
        if (table_in) jazz_model_TableSchema_clone(&table, table_in);
        else {
            const jazz_model_TableSchema *t;
            e = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(self, SL(query->table), lowered_shape->schema_version, &t);
            if (!JZ_IS_ERR(e)) jazz_model_TableSchema_clone(&table, t);
        }
        if (!JZ_IS_ERR(e))
            e = include_deleted ? MAT(materialize_include_deleted_query_rows)(self, table, &deltas, out)
                                : MAT(materialize_historical_query_rows)(self, table, &deltas, out);
    }
    groove_RecordDeltas_drop(&deltas);
    return e;
}

static jz_err lower_shape_with_literals(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_ValidatedQuery *lowered_shape, jazz_model_Binding *lowered_binding) {
    const jazz_model_RuntimeSchema *read_schema = catalogue_schema_for(self, shape->schema_version);
    if (!read_schema) return invalid_stored("query schema version is unknown");
    JZ_TRY(BND(inline_snapshot_bind_filter_literals)(shape, binding, read_schema, lowered_shape));
    jz_err e = jazz_model__query__validation__ValidatedQuery__bind(lowered_shape, empty_values(), lowered_binding);
    if (JZ_IS_ERR(e)) { e.code = JZN_E_Query; jazz_model_ValidatedQuery_drop(lowered_shape); }
    return e;
}

jz_err QES(query_rows_at_with_query_engine)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_GlobalTime position, jazz_types_AuthorSubject identity, jazz_node_CurrentRowVec *out) {
    jazz_model_ValidatedQuery lowered_shape; jazz_model_Binding lowered_binding;
    JZ_TRY(lower_shape_with_literals(self, shape, binding, &lowered_shape, &lowered_binding));
    jazz_engine_QueryProgram program;
    jz_err e = QES(compile_historical_query_program)(self, &lowered_shape, &lowered_binding, position, identity, JZN_CQPO_APP_ROWS, &program);
    if (!JZ_IS_ERR(e)) {
        e = run_app_rows_program(self, &lowered_shape, &program, false, false, NULL, out);
        jazz_engine_QueryProgram_drop(&program);
    }
    jazz_model_Binding_drop(&lowered_binding); jazz_model_ValidatedQuery_drop(&lowered_shape);
    return e;
}

jz_err QES(query_rows_at_snapshot)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    const jazz_model_Snapshot *snapshot, jazz_node_CurrentRowVec *out) {
    jazz_model_ValidatedQuery lowered_shape; jazz_model_Binding lowered_binding;
    JZ_TRY(lower_shape_with_literals(self, shape, binding, &lowered_shape, &lowered_binding));
    jazz_types_AuthorSubject system = { JAZZ_AUTHOR_SYSTEM, {{0}} };
    jazz_engine_QueryProgram program;
    jz_err e = QES(compile_snapshot_query_program)(self, &lowered_shape, &lowered_binding, snapshot, system, JZN_CQPO_APP_ROWS, &program);
    if (!JZ_IS_ERR(e)) {
        jazz_node_CurrentRowVec rows = {0};
        e = run_app_rows_program(self, &lowered_shape, &program, false, false, NULL, &rows);
        jazz_engine_QueryProgram_drop(&program);
        if (!JZ_IS_ERR(e)) e = MAT(finish_engine_query_rows_in_schema)(self, &lowered_shape.query, lowered_shape.schema_version, &rows);
        if (JZ_IS_ERR(e)) jazz_node_CurrentRowVec_drop(&rows); else *out = rows;
    }
    jazz_model_Binding_drop(&lowered_binding); jazz_model_ValidatedQuery_drop(&lowered_shape);
    return e;
}

jz_err QES(query_rows_including_deleted_with_query_engine)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_engine_QueryAuthorizationMode authorization_mode,
    const jazz_protocol_ReadViewSpec *read_view, jazz_node_CurrentRowVec *out) {
    const jazz_model_RuntimeSchema *read_schema = catalogue_schema_for(self, shape->schema_version);
    if (!read_schema) return invalid_stored("query schema version is unknown");
    jazz_model_ValidatedQuery lowered_shape;
    JZ_TRY(BND(inline_snapshot_bind_filter_literals)(shape, binding, read_schema, &lowered_shape));
    const jazz_model_Query *query = &lowered_shape.query;
    jazz_model_TableSchema table;
    jz_err e;
    if (query->aggregate) e = MAT(query_output_table)(self, query, lowered_shape.schema_version, &table);
    else {
        const jazz_model_TableSchema *t;
        e = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(self, SL(query->table), lowered_shape.schema_version, &t);
        if (!JZ_IS_ERR(e)) jazz_model_TableSchema_clone(&table, t);
    }
    if (JZ_IS_ERR(e)) { jazz_model_ValidatedQuery_drop(&lowered_shape); return e; }
    jazz_model_Binding lowered_binding;
    e = jazz_model__query__validation__ValidatedQuery__bind(&lowered_shape, empty_values(), &lowered_binding);
    if (JZ_IS_ERR(e)) { e.code = JZN_E_Query; goto out_table; }
    jazz_engine_QueryProgram program;
    e = QES(compile_include_deleted_query_program_in_authorization_mode)(self, &lowered_shape, &lowered_binding, tier, identity, authorization_mode,
        read_view, NULL, &program);
    if (!JZ_IS_ERR(e)) {
        e = run_app_rows_program(self, &lowered_shape, &program, true, true, &table, out);
        jazz_engine_QueryProgram_drop(&program);
    }
    jazz_model_Binding_drop(&lowered_binding);
out_table:
    jazz_model_TableSchema_drop(&table);
    jazz_model_ValidatedQuery_drop(&lowered_shape);
    return e;
}

jz_err QES(current_rows_at)(jazz_node_NodeState *self, jz_slice table, jazz_types_GlobalTime position, jazz_node_CurrentRowVec *out) {
    self->query_engine_read_metrics.source_global_time_range_scans += 1;
    return QES(bounded_historical_current_rows)(self, table, position, out);
}

/* Global rows of table as of seq position: a row whose latest accepted seq is
 * at or below the cut reads current; a newer row seeks its history for the
 * newest accepted image at or below the cut. */
jz_err QES(bounded_historical_current_rows)(jazz_node_NodeState *self, jz_slice table, jazz_types_GlobalTime position, jazz_node_CurrentRowVec *out) {
    const jazz_model_TableSchema *ts;
    JZ_TRY(jazz_node__node__NodeState__table(self, table, &ts));
    jazz_model_TableSchema table_schema; jazz_model_TableSchema_clone(&table_schema, ts);
    jazz_types_PhysicalTableId table_id;
    jz_err e = jazz_node__node__physical__NodeState__physical_table_id_for_schema(self, self->catalogue.local_schema_version_id, table, &table_id);
    if (JZ_IS_ERR(e)) { jazz_model_TableSchema_drop(&table_schema); return e; }
    jz_str current_table = jazz_node__node__physical__physical_global_current_table_name(table_id);
    jazz_model_BranchKey branch = jazz_model__model__branch__BranchKey__default();
    groove_Value prefix = groove_Value_bytes(jazz_model__model__branch__BranchKey__canonical_bytes(&branch));
    groove_RawRecordVec records = {0};
    e = jzn_groove(self->database.vt->primary_key_scan_raw(self->database.self, SL(current_table), &prefix, 1, &records));
    groove_Value_drop(&prefix);
    jazz_node_CurrentRowVec rows = {0};
    for (u32 i = 0; !JZ_IS_ERR(e) && i < records.len; i++) {
        groove_BorrowedRecord record = groove__db__RawRecord__record(&records.ptr[i]);
        jazz_types_RowUuid row_uuid; bool has_seq; u64 seq = 0;
        e = groove__records__BorrowedRecord__get_uuid(&record, jazz_node__node__GlobalCurrentRowRecord__FIELD_ROW_UUID_IDX, &row_uuid.u);
        if (!JZ_IS_ERR(e)) e = groove__records__BorrowedRecord__get_nullable_u64(&record, jazz_node__node__GlobalCurrentRowRecord__FIELD_GLOBAL_TIME_IDX, &has_seq, &seq);
        if (JZ_IS_ERR(e)) { e.code = JZN_E_Record; break; }
        if (!has_seq) seq = 0;
        jazz_node_VersionRow *version = NULL;
        if (seq <= position.t) {
            u64 tx_time, tx_node;
            e = groove__records__BorrowedRecord__get_u64(&record, jazz_node__node__GlobalCurrentRowRecord__FIELD_TX_TIME_IDX, &tx_time);
            if (!JZ_IS_ERR(e)) e = groove__records__BorrowedRecord__get_u64(&record, jazz_node__node__GlobalCurrentRowRecord__FIELD_TX_NODE_ID_IDX, &tx_node);
            if (JZ_IS_ERR(e)) { e.code = JZN_E_Record; break; }
            jazz_types_TxTime t = { tx_time };
            e = jazz_node__node__views__NodeState__query_version_by_alias(self, table, row_uuid, t, tx_node, &version);
            if (JZ_IS_ERR(e)) break;
            if (!version) { e = invalid_stored("historical row winner is missing"); break; }
        } else {
            jazz_node_VersionRowVec versions = {0};
            e = jazz_node__node__currency__NodeState__query_row_versions_in_branch(self, table, &branch, row_uuid, &versions);
            if (JZ_IS_ERR(e)) break;
            bool has_best = false; jazz_types_GlobalTime best_seq = {0}; u32 best = 0;
            for (u32 v = 0; v < versions.len; v++) {
                jazz_types_TxId tx_id;
                e = jazz_node__node__NodeState__version_tx_id(self, versions.ptr[v], &tx_id);
                if (JZ_IS_ERR(e)) break;
                bool found, has_gt; u32 fate; jazz_types_GlobalTime gt; jazz_model_DurabilityTier dt;
                e = jazz_node__node__currency__NodeState__query_transaction_state(self, tx_id, &found, &fate, &has_gt, &gt, &dt);
                if (JZ_IS_ERR(e)) break;
                if (!found || !has_gt) continue;
                if (gt.t <= position.t && (!has_best || best_seq.t < gt.t)) { has_best = true; best_seq = gt; best = v; }
            }
            if (!JZ_IS_ERR(e) && has_best) { version = versions.ptr[best]; versions.ptr[best] = NULL; }
            for (u32 v = 0; v < versions.len; v++) if (versions.ptr[v]) jazz_node_VersionRow_free(versions.ptr[v]);
            jz_free(versions.ptr);
            if (JZ_IS_ERR(e)) break;
            if (!version) continue;
        }
        if (!jazz_node__node__VersionRow__is_deleted(version)) {
            jazz_node_CurrentRow row;
            e = jazz_node__node__state__read_payload__NodeState__current_row_from_materialized_version(self, &table_schema, version, &row);
            if (!JZ_IS_ERR(e)) JZ_VEC_PUSH(rows, row);
        }
        jazz_node_VersionRow_free(version);
    }
    jz_vec_drop(&records, sizeof(groove_RawRecord), groove_RawRecord_drop);
    jazz_model_BranchKey_drop(&branch); jz_str_drop(&current_table); jazz_model_TableSchema_drop(&table_schema);
    if (JZ_IS_ERR(e)) { jazz_node_CurrentRowVec_drop(&rows); return e; }
    jazz_node__node__sort_current_rows(rows.ptr, rows.len);
    *out = rows;
    return JZ_OK;
}

static void empty_relation_snapshot(jazz_node_RelationSnapshot *out) { __builtin_memset(out, 0, sizeof *out); }

jz_err QES(query_relation_snapshot_in_authorization_mode)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, const jazz_protocol_ReadViewSpec *read_view,
    jazz_engine_QueryAuthorizationMode authorization_mode, jazz_node_RelationSnapshot *out) {
    /* Structured reads are not an exception to receiver-local authority
     * evaluation. Use the exact same selected receipt and ephemeral
     * descriptor-bound inputs as scalar one-shots; otherwise a tree read could
     * silently reopen local storage while a scalar read correctly consumes
     * CoveredInput. */
    jazz_node_ClientSettledBindingView client_view; bool has_view = false;
    if (authorization_mode == JAZZ_QAM_CLIENT_LOCAL)
        has_view = QES(client_settled_binding_view_for_query)(self, shape, binding, tier, read_view, &client_view);
    const jazz_protocol_BindingViewKey *settled_binding_view = has_view ? &client_view.key : NULL;
    jz_err e = JZ_OK;
    jazz_protocol_AuthorityResultKey authority_result_key; bool has_key = false;
    empty_relation_snapshot(out);
    if (authorization_mode == JAZZ_QAM_CLIENT_LOCAL && tier >= JAZZ_TIER_GLOBAL && !settled_binding_view) goto done;
    if (settled_binding_view)
        has_key = jazz_node__node__query_eval__subscriptions__NodeState__unique_authority_result_key_for_binding_view(self, settled_binding_view, &authority_result_key);
    if (authorization_mode == JAZZ_QAM_CLIENT_LOCAL && settled_binding_view && !has_key) goto done;
    jazz_engine_QueryProgram program; jazz_node_CoveredInputReceiver *receiver = NULL;
    if (has_key) {
        jazz_engine_QueryProgramRequest request;
        e = QES(current_query_program_request)(self, shape, binding, tier, identity, JZN_CQPO_RELATION_SNAPSHOT, read_view, settled_binding_view, authorization_mode, &request);
        if (JZ_IS_ERR(e)) goto done;
        jz_btree access_paths;
        e = RSN(one_shot_access_paths)(self, shape, binding, tier, &access_paths);
        if (JZ_IS_ERR(e)) { jazz_engine_QueryProgramRequest_drop(&request); goto done; }
        bool found;
        e = QES(compile_client_one_shot_with_covered_inputs)(self, request, access_paths, shape->schema_version, read_view, &authority_result_key, &found, &program, &receiver);
        if (JZ_IS_ERR(e) || !found) goto done;
    } else {
        e = QES(compile_current_query_program_for_read_view_in_authorization_mode)(self, shape, binding, tier, identity, JZN_CQPO_RELATION_SNAPSHOT,
            read_view, authorization_mode, &program);
        if (JZ_IS_ERR(e)) goto done;
    }
    u32 nsinks; groove_Sink *sinks = LW(lowered_program_sinks)(&program, &nsinks);
    groove_MultisinkDeltas snapshots;
    jz_err snapshots_result = jzn_groove(groove__db__Database__query_graphs(&self->database, sinks, nsinks, &snapshots));
    jz_err retire_result = JZ_OK;
    if (receiver) {
        retire_result = QES(retire_covered_input_sources)(self, jazz_node__node__query_eval__maintained_views__CoveredInputReceiver__sources(receiver));
        jazz_node_CoveredInputReceiver_free(receiver);
    }
    if (JZ_IS_ERR(snapshots_result)) { jz_err_drop(&retire_result); e = snapshots_result; jazz_engine_QueryProgram_drop(&program); goto done; }
    if (JZ_IS_ERR(retire_result)) { groove_MultisinkDeltas_drop(&snapshots); e = retire_result; jazz_engine_QueryProgram_drop(&program); goto done; }
    e = MAT(materialize_relation_snapshot_from_query_engine)(self, shape, read_view, &snapshots, &program, out);
    groove_MultisinkDeltas_drop(&snapshots);
    jazz_engine_QueryProgram_drop(&program);
done:
    if (has_key) jazz_protocol_AuthorityResultKey_drop(&authority_result_key);
    if (has_view) jazz_node_ClientSettledBindingView_drop(&client_view);
    return e;
}

jz_err QES(maintained_witness_for_result_member)(const jazz_node_NodeState *self, jazz_node_VersionRow *const *versions, u32 n,
    jazz_types_SchemaVersionId result_schema, const jz_str *result_table, jazz_types_RowUuid row_uuid, const jazz_node_VersionRow **out) {
    jazz_types_PhysicalTableId table_id;
    JZ_TRY(jazz_node__node__physical__NodeState__physical_table_id_for_schema(self, result_schema, SL(*result_table), &table_id));
    *out = NULL;
    for (u32 i = n; i-- > 0;) {
        const jazz_node_VersionRow *version = versions[i];
        jazz_types_RowUuid r = jazz_node__node__VersionRow__row_uuid(version);
        if (!jz_uuid_eq(&r.u, &row_uuid.u) || jazz_node__node__VersionRow__is_deleted(version)) continue;
        jazz_types_PhysicalTableId vid;
        JZ_TRY(jazz_node__node__physical__NodeState__physical_table_id_for_version(self, version, &vid));
        if (jz_uuid_eq(&vid.u, &table_id.u)) { *out = version; return JZ_OK; }
    }
    return JZ_OK;
}

/* ---------- link bindings and physical-row probes ---------- */

jz_err QES(prepare_query_binding_for_link)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape_in, const jazz_model_Binding *binding_in,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_model_ValidatedQuery *shape_out, jazz_model_Binding *binding_out,
    jazz_node_PreparedQueryPlan **plan_out) {
    JZ_TRY(QES(query_binding_for_link)(self, shape_in, binding_in, shape_out, binding_out));
    jz_err e = QES(prepared_query_plan)(self, shape_out, binding_out, tier, identity, plan_out);
    if (JZ_IS_ERR(e)) { jazz_model_Binding_drop(binding_out); jazz_model_ValidatedQuery_drop(shape_out); }
    return e;
}

jz_err QES(query_binding_for_link)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_ValidatedQuery *shape_out, jazz_model_Binding *binding_out) {
    const jazz_model_RuntimeSchema *schema = catalogue_schema_for(self, shape->schema_version);
    if (!schema) return invalid_stored("query schema version is unknown");
    JZ_TRY(BND(bind_query_params_with_mode)(shape, binding, schema, JAZZ_PBM_RETAIN_ALL_PARAMS, shape_out));
    jz_err e = jazz_model__query__validation__ValidatedQuery__bind(shape_out, clone_values(&binding->values), binding_out);
    if (JZ_IS_ERR(e)) { e.code = JZN_E_Query; jazz_model_ValidatedQuery_drop(shape_out); }
    return e;
}

jz_err QES(query_rows_for_link)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_node_CurrentRowVec *out) {
    return QES(query_rows_with_prepared_plan_for_identity)(self, shape, binding, tier, NULL, identity, out);
}

static groove_RootIndirectValues riv(u32 tag) { groove_RootIndirectValues v = { tag, NULL }; return v; }

/* Execute a serving query with its root constrained to a physical row UUID
 * for internal authorization probes. */
jz_err QES(query_rows_for_link_physical_row)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_types_RowUuid row_uuid, jazz_node_CurrentRowVec *out) {
    return QES(query_rows_for_physical_row_in_authorization_mode)(self, shape, binding, tier, identity, row_uuid, JAZZ_QAM_TRUSTED_SERVING,
        riv(GROOVE_RIV_MATERIALIZE), out);
}

/* Like query_rows_for_link_physical_row, for callers that inspect only row
 * identity and provenance: large values stay physical descriptors. A policy
 * predicate that reads a large column still materializes that field inside
 * the graph, so visibility is unchanged; the probe no longer rebuilds whole
 * values it never reads (#3471). */
jz_err QES(query_row_visibility_for_link_physical_row)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_types_RowUuid row_uuid, jazz_node_CurrentRowVec *out) {
    return QES(query_rows_for_physical_row_in_authorization_mode)(self, shape, binding, tier, identity, row_uuid, JAZZ_QAM_TRUSTED_SERVING,
        riv(GROOVE_RIV_PHYSICAL), out);
}

jz_err QES(query_rows_for_client_physical_row)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_types_RowUuid row_uuid, jazz_node_CurrentRowVec *out) {
    return QES(query_rows_for_physical_row_in_authorization_mode)(self, shape, binding, tier, identity, row_uuid, JAZZ_QAM_CLIENT_LOCAL,
        riv(GROOVE_RIV_MATERIALIZE), out);
}

/* Execute a point-read program through its prepared plan. A policy can
 * introduce claim parameters even though this physical row lookup has no
 * public query parameters. Those programs must go through Groove's
 * prepare/bind boundary just like ordinary serving reads; executing the
 * lowered graph directly leaves its binding source unprepared and fails
 * instead of representing a denied read. */
static jz_err point_read_deltas(jazz_node_NodeState *self, jazz_engine_QueryProgram *program, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, jazz_types_AuthorSubject identity, groove_RootIndirectValues root_indirect_values, bool graph_with_root_values,
    groove_RecordDeltas *out) {
    jazz_node_PreparedQueryPlan plan;
    jz_err e = LWN(prepared_query_plan_from_program)(self, program, shape, binding, &plan);
    jazz_engine_QueryProgram_drop(program);
    if (JZ_IS_ERR(e)) return e;
    jazz_engine_PolicyContext policy = jazz_node__node__query_eval__authorization__NodeState__query_program_policy_context(self, identity);
    switch (plan.tag) {
    case JZN_PQP_PREPARED: {
        groove_ValueVec values = {0};
        e = jazz_node__node__query_eval__prepared_bindings__binding_values_for_plan(binding, plan.params.ptr, plan.params.len, &policy, JZN_PCBM_STRICT, &values);
        if (!JZ_IS_ERR(e)) {
            groove_PreparedShapeId id = { plan.shape };
            e = QES(bind_disposable_shape_snapshot)(self, id, values.ptr, values.len, root_indirect_values, out);
        }
        jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop_v);
        break;
    }
    case JZN_PQP_GRAPH: {
        groove_GraphBuilder *g = plan.graph; plan.graph = NULL;
        e = graph_with_root_values ? jzn_groove(groove__db__Database__query_graph_with_root_values(&self->database, g, root_indirect_values, out))
                                   : jzn_groove(groove__db__Database__query_graph_deltas(&self->database, g, out));
        break;
    }
    default: jz_panic("point reads never use peer-maintained plans");
    }
    jazz_engine_PolicyContext_drop(&policy);
    jazz_node_PreparedQueryPlan_drop(&plan);
    return e;
}

jz_err QES(query_rows_for_physical_row_in_authorization_mode)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_types_RowUuid row_uuid, jazz_engine_QueryAuthorizationMode authorization_mode,
    groove_RootIndirectValues root_indirect_values, jazz_node_CurrentRowVec *out) {
    const jazz_model_TableSchema *t;
    JZ_TRY(jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(self, SL(shape->query.table), shape->schema_version, &t));
    jazz_model_TableSchema table; jazz_model_TableSchema_clone(&table, t);
    jz_btree access_paths; jz_btree_init(&access_paths, &jazz_node_AccessPaths_desc);
    jazz_engine_SourceId root = root_source(&shape->query.table);
    jazz_node_CurrentAccessPath p; __builtin_memset(&p, 0, sizeof p);
    p.tag = JAZZ_CAP_PRIMARY_KEY; JZ_VEC_PUSH(p.prefix, groove_Value_uuid(row_uuid.u));
    jz_btree_insert(&access_paths, &root, &p, NULL);
    jazz_engine_QueryProgram program;
    jz_err e = QES(compile_current_query_program_with_access_paths)(self, shape, binding, tier, identity, JZN_CQPO_APP_ROWS, access_paths, authorization_mode, &program);
    if (JZ_IS_ERR(e)) goto out;
    groove_RecordDeltas deltas;
    e = point_read_deltas(self, &program, shape, binding, identity, root_indirect_values, true, &deltas);
    if (JZ_IS_ERR(e)) goto out;
    jazz_node_CurrentRowVec rows = {0};
    e = MAT(materialize_inline_current_query_rows)(self, &table, &deltas, &rows);
    groove_RecordDeltas_drop(&deltas);
    if (!JZ_IS_ERR(e)) e = MAT(finish_engine_query_rows_in_schema)(self, &shape->query, shape->schema_version, &rows);
    if (JZ_IS_ERR(e)) jazz_node_CurrentRowVec_drop(&rows); else *out = rows;
out:
    jazz_model_TableSchema_drop(&table);
    return e;
}

jz_err QES(query_readable_current_row_including_deleted)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_types_RowUuid row_uuid, jazz_node_CurrentRowVec *out) {
    const jazz_model_TableSchema *t;
    JZ_TRY(jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(self, SL(shape->query.table), shape->schema_version, &t));
    jazz_model_TableSchema table; jazz_model_TableSchema_clone(&table, t);
    jazz_protocol_ReadViewSpec rv = jazz_protocol_ReadViewSpec_default();
    jazz_engine_QueryProgram program;
    jz_err e = QES(compile_include_deleted_query_program_in_authorization_mode)(self, shape, binding, tier, identity, JAZZ_QAM_TRUSTED_SERVING, &rv, &row_uuid, &program);
    jazz_protocol_ReadViewSpec_drop(&rv);
    if (JZ_IS_ERR(e)) { jazz_model_TableSchema_drop(&table); return e; }
    groove_RecordDeltas deltas;
    e = point_read_deltas(self, &program, shape, binding, identity, riv(GROOVE_RIV_MATERIALIZE), false, &deltas);
    if (JZ_IS_ERR(e)) { jazz_model_TableSchema_drop(&table); return e; }
    e = MAT(materialize_include_deleted_query_rows)(self, table, &deltas, out);
    groove_RecordDeltas_drop(&deltas);
    return e;
}

/* ---------- relation snapshots, ordering and projection ---------- */

jz_err QES(query_relation_snapshot_for_serving_in_read_view)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, const jazz_protocol_ReadViewSpec *read_view, jazz_node_RelationSnapshot *out) {
    return QES(query_relation_snapshot_in_authorization_mode)(self, shape, binding, tier, identity, read_view, JAZZ_QAM_TRUSTED_SERVING, out);
}

jz_err QES(query_relation_snapshot_for_client)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, const jazz_protocol_ReadViewSpec *read_view, jazz_node_RelationSnapshot *out) {
    return QES(query_relation_snapshot_in_authorization_mode)(self, shape, binding, tier, identity, read_view, JAZZ_QAM_CLIENT_LOCAL, out);
}

jz_err QES(subscription_snapshot_in_authorization_mode)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, const jazz_protocol_ReadViewSpec *read_view,
    jazz_engine_QueryAuthorizationMode authorization_mode, jazz_node_RelationSnapshot *out) {
    jazz_protocol_ReadViewSpec def = jazz_protocol_ReadViewSpec_default();
    bool is_default = jazz_protocol_ReadViewSpec_eq(read_view, &def);
    jazz_protocol_ReadViewSpec_drop(&def);
    if (shape->query.array_subqueries.len == 0 && is_default) {
        jazz_node_CurrentRowVec rows;
        if (authorization_mode == JAZZ_QAM_CLIENT_LOCAL) JZ_TRY(QES(query_rows_for_client)(self, shape, binding, tier, identity, &rows));
        else JZ_TRY(QES(query_rows_with_prepared_plan_for_identity)(self, shape, binding, tier, NULL, identity, &rows));
        empty_relation_snapshot(out);
        out->root_count = rows.len;
        out->rows = rows;
        return JZ_OK;
    }
    if (authorization_mode == JAZZ_QAM_CLIENT_LOCAL)
        return QES(query_relation_snapshot_for_client)(self, shape, binding, tier, identity, read_view, out);
    return QES(query_relation_snapshot_for_serving_in_read_view)(self, shape, binding, tier, identity, read_view, out);
}

jz_err QES(query_rows_at_for_link)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_GlobalTime position, jazz_types_AuthorSubject identity, jazz_node_CurrentRowVec *out) {
    return QES(query_rows_at_for_identity)(self, shape, binding, position, identity, out);
}

bool QES(uses_schema_projected_read)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape) {
    return !jazz_types_SchemaVersionId_eq(shape->schema_version, self->catalogue.local_schema_version_id);
}

jz_err QES(apply_query_order_with_occurrences)(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_node_CurrentRowVec *rows,
    jazz_types_OutputOccurrenceIdVec *occurrence_ids) {
    jazz_model_Query presentation_query; jazz_model_Query_clone(&presentation_query, query);
    if (presentation_query.order_by.len == 0 && presentation_query.relation) {
        jazz_model_OrderByVec order_by = {0};
        if (jazz_model__query__relation_union_presentation_order(presentation_query.relation, &order_by)) {
            jazz_model_OrderByVec *dst = (jazz_model_OrderByVec *)&presentation_query.order_by;
            jazz_model_OrderByVec_drop(dst);
            *dst = order_by;
        }
    }
    const jazz_model_TableSchema *table = NULL;
    jz_err e = JZ_OK;
    if (presentation_query.order_by.len != 0 && !presentation_query.aggregate)
        e = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(self, SL(presentation_query.table),
            jazz_node__node__state__catalogue__SchemaCatalogue__active_schema_version(&self->catalogue), &table);
    if (!JZ_IS_ERR(e)) e = MAT(sort_query_rows_with_occurrences)(&presentation_query, table, rows, occurrence_ids);
    jazz_model_Query_drop(&presentation_query);
    return e;
}

jz_err QES(apply_projection)(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_node_CurrentRowVec *rows) {
    return MAT(apply_projection_in_schema)(self, query, jazz_node__node__state__catalogue__SchemaCatalogue__active_schema_version(&self->catalogue), rows->ptr, rows->len);
}

/* ---------- open-transaction reads ---------- */

static jazz_types_AuthorSubject system_subject(void) { jazz_types_AuthorSubject s = { JAZZ_AUTHOR_SYSTEM, {{0}} }; return s; }

/* Evaluate a validated query inside an open exclusive transaction. */
jz_err QES(tx_query)(jazz_node_NodeState *self, jazz_model_OpenTransactionId tx_id, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_node_CurrentRowVec *out) {
    return QES(tx_query_with_options)(self, tx_id, shape, binding, false, out);
}

/* Evaluate a validated query inside an open transaction using the local
 * client read boundary with explicit root-row deletion visibility. */
jz_err QES(tx_query_with_options)(jazz_node_NodeState *self, jazz_model_OpenTransactionId tx_id, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, bool include_deleted, jazz_node_CurrentRowVec *out) {
    return QES(tx_query_in_authorization_mode)(self, tx_id, shape, binding, system_subject(), include_deleted, JAZZ_QAM_CLIENT_LOCAL, out);
}

/* Evaluate a validated query inside an open exclusive transaction as identity. */
jz_err QES(tx_query_for_identity)(jazz_node_NodeState *self, jazz_model_OpenTransactionId tx_id, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, jazz_types_AuthorSubject identity, jazz_node_CurrentRowVec *out) {
    return QES(tx_query_for_identity_with_options)(self, tx_id, shape, binding, identity, false, out);
}

/* Evaluate a validated query inside an open transaction with explicit
 * root-row deletion visibility. */
jz_err QES(tx_query_for_identity_with_options)(jazz_node_NodeState *self, jazz_model_OpenTransactionId tx_id, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, jazz_types_AuthorSubject identity, bool include_deleted, jazz_node_CurrentRowVec *out) {
    return QES(tx_query_in_authorization_mode)(self, tx_id, shape, binding, identity, include_deleted, JAZZ_QAM_TRUSTED_SERVING, out);
}

/* Evaluate a query and its relation payload inside an open transaction.
 *
 * This uses the same transaction snapshot and staged overlay as
 * tx_query_with_options. In particular, it must not fall back to the ordinary
 * maintained relation view: doing so would silently omit writes staged by the
 * transaction. */
jz_err QES(tx_relation_snapshot_with_options)(jazz_node_NodeState *self, jazz_model_OpenTransactionId tx_id, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, bool include_deleted, jazz_node_RelationSnapshot *out) {
    return QES(tx_relation_snapshot_in_authorization_mode)(self, tx_id, shape, binding, system_subject(), include_deleted, JAZZ_QAM_CLIENT_LOCAL, out);
}

/* Evaluate a relation payload inside an open transaction as its bound serving
 * identity. */
jz_err QES(tx_relation_snapshot_for_identity_with_options)(jazz_node_NodeState *self, jazz_model_OpenTransactionId tx_id, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, jazz_types_AuthorSubject identity, bool include_deleted, jazz_node_RelationSnapshot *out) {
    return QES(tx_relation_snapshot_in_authorization_mode)(self, tx_id, shape, binding, identity, include_deleted, JAZZ_QAM_TRUSTED_SERVING, out);
}

static jz_err push_predicate_read(jazz_node_NodeState *self, jazz_model_OpenTransactionId tx_id, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding) {
    jazz_model_PredicateRead predicate_read;
    predicate_read.table = jz_str_clone(&shape->query.table);
    predicate_read.shape_id = shape->shape_id;
    jazz_model_Query_clone(&predicate_read.shape, &shape->query);
    predicate_read.binding_id = jazz_model__query__validation__Binding__binding_id(binding);
    predicate_read.binding_values = clone_values(&binding->values);
    jazz_node_OpenTransaction *open_tx;
    jz_err e = jazz_node__node__open_tx__NodeState__open_tx_mut(self, tx_id, &open_tx);
    if (JZ_IS_ERR(e)) {
        jz_str_drop(&predicate_read.table); jazz_model_Query_drop(&predicate_read.shape); jz_btree_drop(&predicate_read.binding_values);
        return e;
    }
    jazz_node__node__OpenTransaction__push_predicate_read(open_tx, predicate_read);
    return JZ_OK;
}

typedef JZ_VEC(jazz_types_RowUuid) RowUuidVec;
typedef struct { jz_str table; RowUuidVec rows; } TableRows;
static void row_vec_drop_v(void *p) { RowUuidVec *v = p; jz_free(v->ptr); }
static const jz_btree_desc returned_rows_desc = { sizeof(jz_str), sizeof(RowUuidVec), jz_cmp_str, str_drop_v, row_vec_drop_v };

jz_err QES(tx_relation_snapshot_in_authorization_mode)(jazz_node_NodeState *self, jazz_model_OpenTransactionId tx_id, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, jazz_types_AuthorSubject identity_in, bool include_deleted, jazz_engine_QueryAuthorizationMode authorization_mode,
    jazz_node_RelationSnapshot *out) {
    jazz_types_AuthorSubject identity;
    JZ_TRY(QES(transaction_query_identity)(self, tx_id, identity_in, authorization_mode, &identity));
    jazz_node_SourceNarrowing *narrowing;
    JZ_TRY(QES(offer_tx_query_narrowed_reads)(self, tx_id, shape, binding, include_deleted, &narrowing));
    jazz_engine_QueryProgram program;
    jz_err program_result = QES(compile_open_tx_query_program)(self, tx_id, shape, binding, identity, JZN_CQPO_RELATION_SNAPSHOT, include_deleted, authorization_mode, &program);
    jazz_node_NarrowedSourceReadVec narrowed_reads = {0};
    jz_err e = jazz_node__node__open_tx__NodeState__withdraw_tx_narrowed_source_reads(self, tx_id, narrowing, &narrowed_reads);
    if (JZ_IS_ERR(e)) { if (!JZ_IS_ERR(program_result)) jazz_engine_QueryProgram_drop(&program); jz_err_drop(&program_result); return e; }
    if (JZ_IS_ERR(program_result)) { jz_vec_drop(&narrowed_reads, sizeof(jazz_node_NarrowedSourceRead), jazz_node_NarrowedSourceRead_drop); return program_result; }
    u32 nsinks; groove_Sink *sinks = LW(lowered_program_sinks)(&program, &nsinks);
    groove_MultisinkDeltas snapshots;
    e = jzn_groove(groove__db__Database__query_graphs(&self->database, sinks, nsinks, &snapshots));
    jazz_node_RelationSnapshot snapshot;
    if (!JZ_IS_ERR(e)) {
        jazz_protocol_ReadViewSpec rv = jazz_protocol_ReadViewSpec_default();
        e = MAT(materialize_relation_snapshot_from_query_engine)(self, shape, &rv, &snapshots, &program, &snapshot);
        jazz_protocol_ReadViewSpec_drop(&rv);
        groove_MultisinkDeltas_drop(&snapshots);
    }
    jazz_engine_QueryProgram_drop(&program);
    if (JZ_IS_ERR(e)) goto fail_reads;
    e = push_predicate_read(self, tx_id, shape, binding);
    if (JZ_IS_ERR(e)) goto fail_snapshot;
    if (!shape->query.aggregate) {
        /* Prove every row the relation returns, root and included alike. */
        jz_btree returned; jz_btree_init(&returned, &returned_rows_desc);
        for (u32 i = 0; i < snapshot.rows.len; i++) {
            jz_str t = jz_str_clone(jazz_node__node__CurrentRow__table(&snapshot.rows.ptr[i]));
            bool inserted; RowUuidVec *slot = jz_btree_entry(&returned, &t, &inserted);
            if (!inserted) jz_str_drop(&t);
            JZ_VEC_PUSH(*slot, jazz_node__node__CurrentRow__row_uuid(&snapshot.rows.ptr[i]));
        }
        jz_str *table; RowUuidVec *rows;
        JZ_BTREE_FOR(&returned, table, rows) {
            e = jazz_node__node__open_tx__NodeState__record_tx_query_row_reads(self, tx_id, shape->schema_version, SL(*table), rows->ptr, rows->len);
            if (JZ_IS_ERR(e)) break;
        }
        jz_btree_drop(&returned);
    } else {
        e = QES(record_tx_aggregate_input_reads)(self, tx_id, shape, binding, identity, authorization_mode);
    }
    if (JZ_IS_ERR(e)) goto fail_snapshot;
    e = QES(record_tx_narrowed_source_reads)(self, tx_id, narrowed_reads, identity, authorization_mode);
    if (JZ_IS_ERR(e)) { jazz_node_CurrentRowVec_drop(&snapshot.rows); return e; }
    *out = snapshot;
    return JZ_OK;
fail_snapshot:
    jazz_node_CurrentRowVec_drop(&snapshot.rows);
fail_reads:
    jz_vec_drop(&narrowed_reads, sizeof(jazz_node_NarrowedSourceRead), jazz_node_NarrowedSourceRead_drop);
    return e;
}

jz_err QES(tx_query_in_authorization_mode)(jazz_node_NodeState *self, jazz_model_OpenTransactionId tx_id, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, jazz_types_AuthorSubject identity_in, bool include_deleted, jazz_engine_QueryAuthorizationMode authorization_mode,
    jazz_node_CurrentRowVec *out) {
    jazz_types_AuthorSubject identity;
    JZ_TRY(QES(transaction_query_identity)(self, tx_id, identity_in, authorization_mode, &identity));
    const jazz_model_Query *query = &shape->query;
    jazz_model_TableSchema table;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema(self, SL(query->table), shape->schema_version, &table));
    jazz_node_SourceNarrowing *narrowing;
    jz_err e = QES(offer_tx_query_narrowed_reads)(self, tx_id, shape, binding, include_deleted, &narrowing);
    if (JZ_IS_ERR(e)) { jazz_model_TableSchema_drop(&table); return e; }
    jazz_engine_QueryProgram program;
    jz_err program_result = QES(compile_open_tx_query_program)(self, tx_id, shape, binding, identity, JZN_CQPO_APP_ROWS, include_deleted, authorization_mode, &program);
    jazz_node_NarrowedSourceReadVec narrowed_reads = {0};
    e = jazz_node__node__open_tx__NodeState__withdraw_tx_narrowed_source_reads(self, tx_id, narrowing, &narrowed_reads);
    if (JZ_IS_ERR(e) || JZ_IS_ERR(program_result)) {
        if (!JZ_IS_ERR(program_result)) jazz_engine_QueryProgram_drop(&program);
        if (JZ_IS_ERR(e)) jz_err_drop(&program_result); else e = program_result;
        jz_vec_drop(&narrowed_reads, sizeof(jazz_node_NarrowedSourceRead), jazz_node_NarrowedSourceRead_drop);
        jazz_model_TableSchema_drop(&table);
        return e;
    }
    jazz_node_CurrentRowVec rows = {0};
    groove_GraphBuilder *graph;
    e = LW(lowered_materialization_app_rows_graph)(&program, &graph);
    if (!JZ_IS_ERR(e)) {
        groove_RecordDeltas deltas;
        e = jzn_groove(groove__db__Database__query_graph_deltas(&self->database, graph, &deltas));
        if (!JZ_IS_ERR(e)) {
            if (query->aggregate) {
                jazz_engine_AppRowSchema schema;
                e = LW(materialization_app_row_schema)(NULL, &program, &schema);
                if (!JZ_IS_ERR(e)) { e = MAT(materialize_aggregate_query_rows)(self, query, &schema, &deltas, &rows); jazz_engine_AppRowSchema_drop(&schema); }
            } else {
                e = MAT(materialize_inline_current_query_rows)(self, &table, &deltas, &rows);
            }
            groove_RecordDeltas_drop(&deltas);
        }
    }
    jazz_engine_QueryProgram_drop(&program);
    if (!JZ_IS_ERR(e)) e = push_predicate_read(self, tx_id, shape, binding);
    if (!JZ_IS_ERR(e)) {
        if (!query->aggregate) {
            RowUuidVec root_rows = {0};
            for (u32 i = 0; i < rows.len; i++)
                if (jz_str_eq(jazz_node__node__CurrentRow__table(&rows.ptr[i]), &query->table))
                    JZ_VEC_PUSH(root_rows, jazz_node__node__CurrentRow__row_uuid(&rows.ptr[i]));
            e = jazz_node__node__open_tx__NodeState__record_tx_query_row_reads(self, tx_id, shape->schema_version, SL(query->table), root_rows.ptr, root_rows.len);
            jz_free(root_rows.ptr);
        } else {
            e = QES(record_tx_aggregate_input_reads)(self, tx_id, shape, binding, identity, authorization_mode);
        }
    }
    if (JZ_IS_ERR(e)) {
        jz_vec_drop(&narrowed_reads, sizeof(jazz_node_NarrowedSourceRead), jazz_node_NarrowedSourceRead_drop);
        goto fail;
    }
    e = QES(record_tx_narrowed_source_reads)(self, tx_id, narrowed_reads, identity, authorization_mode);
    if (!JZ_IS_ERR(e)) e = MAT(finish_engine_query_rows_in_schema)(self, query, shape->schema_version, &rows);
    if (!JZ_IS_ERR(e) && query->array_subqueries.len == 0) e = MAT(apply_projection_in_schema)(self, query, shape->schema_version, rows.ptr, rows.len);
    if (JZ_IS_ERR(e)) goto fail;
    jazz_model_TableSchema_drop(&table);
    *out = rows;
    return JZ_OK;
fail:
    jazz_node_CurrentRowVec_drop(&rows);
    jazz_model_TableSchema_drop(&table);
    return e;
}

/* Prove the rows an exclusive aggregate read consumed. An aggregate returns no
 * rows to prove, so the transaction also reads the aggregate's input rows; the
 * authority validates the aggregate against them (garden-co/jazz#3694). */
jz_err QES(record_tx_aggregate_input_reads)(jazz_node_NodeState *self, jazz_model_OpenTransactionId tx_id, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, jazz_types_AuthorSubject identity, jazz_engine_QueryAuthorizationMode authorization_mode) {
    jazz_model_Query input;
    if (!jazz_model__query__builders__Query__aggregate_input(&shape->query, &input)) return JZ_OK;
    bool exclusive;
    jz_err e = jazz_node__node__open_tx__NodeState__transaction_is_exclusive(self, tx_id, &exclusive);
    if (JZ_IS_ERR(e) || !exclusive) { jazz_model_Query_drop(&input); return e; }
    const jazz_model_RuntimeSchema *schema = catalogue_schema_for(self, shape->schema_version);
    if (!schema) { jazz_model_Query_drop(&input); return invalid_stored("transaction schema is unknown"); }
    jazz_model_ValidatedQuery validated;
    e = jazz_model__query__ast__Query__validate(&input, schema, &validated);
    jazz_model_Query_drop(&input);
    if (JZ_IS_ERR(e)) { e.code = JZN_E_Query; return e; }
    jazz_model_Binding input_binding;
    e = jazz_model__query__validation__ValidatedQuery__bind(&validated, clone_values(&binding->values), &input_binding);
    if (JZ_IS_ERR(e)) { e.code = JZN_E_Query; jazz_model_ValidatedQuery_drop(&validated); return e; }
    jazz_node_CurrentRowVec rows = {0};
    e = QES(tx_query_in_authorization_mode)(self, tx_id, &validated, &input_binding, identity, false, authorization_mode, &rows);
    if (!JZ_IS_ERR(e)) jazz_node_CurrentRowVec_drop(&rows);
    jazz_model_Binding_drop(&input_binding); jazz_model_ValidatedQuery_drop(&validated);
    return e;
}

/* The queries a partial node hydrates before an exclusive read of shape: for
 * each source it reads beyond its root (joined, included, correlated and
 * relation sources), the narrowed read the transaction records for that
 * source (garden-co/jazz#3694). Rows the replica never received there then do
 * not make the read conflict. Fails with Error::UnsupportedExclusiveRead
 * before anything is hydrated when a source has no narrowed read. */
jz_err QES(exclusive_source_hydration_queries)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    bool include_deleted, jazz_model_QueryVec *out) {
    jazz_node_ExclusiveSourceReads reads;
    JZ_TRY(jazz_node__node__query_eval__narrowed_reads__NodeState__exclusive_source_reads(self, shape, binding, include_deleted, &reads));
    out->ptr = NULL; out->len = out->cap = 0;
    void *k; jazz_node_NarrowedSourceRead *read;
    JZ_BTREE_FOR(&reads.reads, k, read) {
        const jazz_model_Query *query = &read->shape.query;
        bool seen = false;
        for (u32 i = 0; i < out->len && !seen; i++) seen = jazz_model_Query_eq(&out->ptr[i], query);
        if (!seen) { jazz_model_Query q; jazz_model_Query_clone(&q, query); JZ_VEC_PUSH(*out, q); }
    }
    jazz_node_ExclusiveSourceReads_drop(&reads);
    return JZ_OK;
}

/* Offer the narrowed reads of shape's non-root sources to the sources of an
 * exclusive transaction query about to be compiled. Returns the narrowing to
 * restore once it is compiled. Fails with Error::UnsupportedExclusiveRead when
 * a source has none. */
jz_err QES(offer_tx_query_narrowed_reads)(jazz_node_NodeState *self, jazz_model_OpenTransactionId tx_id, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, bool include_deleted, jazz_node_SourceNarrowing **out) {
    bool exclusive;
    JZ_TRY(jazz_node__node__open_tx__NodeState__transaction_is_exclusive(self, tx_id, &exclusive));
    jazz_node_ExclusiveSourceReads reads;
    bool recording = false;
    if (exclusive) {
        const jazz_node_OpenTransaction *tx;
        JZ_TRY(jazz_node__node__open_tx__NodeState__open_tx(self, tx_id, &tx));
        recording = jazz_node__node__OpenTransaction__source_narrowing_recording(tx);
    }
    if (exclusive && !recording) JZ_TRY(jazz_node__node__query_eval__narrowed_reads__NodeState__exclusive_source_reads(self, shape, binding, include_deleted, &reads));
    else reads = jazz_node_ExclusiveSourceReads_default();
    return jazz_node__node__open_tx__NodeState__offer_tx_narrowed_source_reads(self, tx_id, reads, out);
}

/* Record the narrowed reads a transaction query's sources claimed. Each runs
 * as its own query in the transaction, which records its predicate read and
 * proves the rows it returns; the sources beyond its root only correlate it
 * with the outer query's root and record nothing (garden-co/jazz#3694). */
jz_err QES(record_tx_narrowed_source_reads)(jazz_node_NodeState *self, jazz_model_OpenTransactionId tx_id, jazz_node_NarrowedSourceReadVec reads,
    jazz_types_AuthorSubject identity, jazz_engine_QueryAuthorizationMode authorization_mode) {
    jz_err e = JZ_OK;
    bool exclusive = false;
    if (reads.len != 0) e = jazz_node__node__open_tx__NodeState__transaction_is_exclusive(self, tx_id, &exclusive);
    for (u32 i = 0; !JZ_IS_ERR(e) && exclusive && i < reads.len; i++) {
        jazz_node_NarrowedSourceRead *read = &reads.ptr[i];
        jazz_model_ShapeId shape_id = read->shape.shape_id;
        jazz_model_BindingId binding_id = jazz_model__query__validation__Binding__binding_id(&read->binding);
        jazz_node_OpenTransaction *tx;
        e = jazz_node__node__open_tx__NodeState__open_tx_mut(self, tx_id, &tx);
        if (JZ_IS_ERR(e)) break;
        if (jazz_node__node__OpenTransaction__narrowed_predicate_reads_contains(tx, shape_id, binding_id)) continue;
        jazz_node__node__OpenTransaction__set_source_narrowing_recording(tx, true);
        jazz_node_CurrentRowVec rows = {0};
        jz_err recorded = QES(tx_query_in_authorization_mode)(self, tx_id, &read->shape, &read->binding, identity, false, authorization_mode, &rows);
        if (!JZ_IS_ERR(recorded)) jazz_node_CurrentRowVec_drop(&rows);
        e = jazz_node__node__open_tx__NodeState__open_tx_mut(self, tx_id, &tx);
        if (JZ_IS_ERR(e)) { jz_err_drop(&recorded); break; }
        jazz_node__node__OpenTransaction__set_source_narrowing_recording(tx, false);
        if (JZ_IS_ERR(recorded)) { e = recorded; break; }
        e = jazz_node__node__open_tx__NodeState__open_tx_mut(self, tx_id, &tx);
        if (JZ_IS_ERR(e)) break;
        jazz_node__node__OpenTransaction__narrowed_predicate_reads_insert(tx, shape_id, binding_id);
    }
    jz_vec_drop(&reads, sizeof(jazz_node_NarrowedSourceRead), jazz_node_NarrowedSourceRead_drop);
    return e;
}

jz_err QES(transaction_query_identity)(const jazz_node_NodeState *self, jazz_model_OpenTransactionId tx_id, jazz_types_AuthorSubject identity,
    jazz_engine_QueryAuthorizationMode authorization_mode, jazz_types_AuthorSubject *out) {
    const jazz_node_OpenTransaction *tx;
    JZ_TRY(jazz_node__node__open_tx__NodeState__open_tx(self, tx_id, &tx));
    bool has_bound; jazz_types_AuthorSubject bound_identity;
    u32 kind = jazz_node__node__OpenTransaction__kind(tx, &has_bound, &bound_identity);
    if (kind == JAZZ_OTK_EXCLUSIVE || has_bound) {
        /* Explicitly bound exclusive transactions are identity capabilities:
         * ordinary and serving reads use the identity fixed at begin. A
         * serving-side mergeable batch is also an identity capability. The raw
         * foreign-function argument selects no authority beyond the subject
         * fixed at begin. */
        if (authorization_mode != JAZZ_QAM_CLIENT_LOCAL && !jazz_types_AuthorSubject_eq(&identity, &bound_identity))
            return jz_err_new(JZN_E_OpenTransactionIdentityMismatch, "open transaction identity mismatch");
        *out = bound_identity;
        return JZ_OK;
    }
    *out = identity;
    return JZ_OK;
}

jz_str QES(prepared_plan_claim_scope)(const jazz_node_NodeState *self, jazz_types_AuthorSubject identity) {
    jz_str scope;
    if (jazz_node__node__state__lifecycle__NodeState__active_session_claim_scope_key(self, identity, &scope)) {
        jz_str out = jz_format("request:%.*s", (int)scope.len, scope.ptr);
        jz_str_drop(&scope);
        return out;
    }
    return jz_format("session:%llu", (unsigned long long)jazz_node__node__state__lifecycle__NodeState__session_claim_revision(self, identity));
}

jz_err QES(prepared_query_plan)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_node_PreparedQueryPlan **out) {
    jz_str claim_scope = QES(prepared_plan_claim_scope)(self, identity);
    jazz_node_QueryShapeCacheKey key = { shape->shape_id, tier,
        jazz_node__node__query_eval__prepared_bindings__policy_plan_cache_signature(binding, identity, &claim_scope) };
    jz_str_drop(&claim_scope);
    jazz_node_PreparedQueryPlan **cached = jz_btree_get(&self->query.query_shape_cache, &key);
    if (cached && (*cached)->tag != JZN_PQP_PEER_MAINTAINED_MARKER) {
        *out = jz_rc_clone(*cached);
        jz_str_drop(&key.signature);
        return JZ_OK;
    }
    jazz_engine_QueryProgram program;
    jz_err e = QES(compile_current_query_program)(self, shape, binding, tier, identity, JZN_CQPO_APP_ROWS, &program);
    if (JZ_IS_ERR(e)) { jz_str_drop(&key.signature); return e; }
    jazz_node_PreparedQueryPlan built;
    e = LWN(prepared_query_plan_from_program)(self, &program, shape, binding, &built);
    jazz_engine_QueryProgram_drop(&program);
    if (JZ_IS_ERR(e)) { jz_str_drop(&key.signature); return e; }
    jazz_node_PreparedQueryPlan *plan = jz_rc_new(sizeof(jazz_node_PreparedQueryPlan), jazz_node_PreparedQueryPlan_drop_v);
    *plan = built;
    jazz_node_PreparedQueryPlan *stored = jz_rc_clone(plan);
    jz_btree_insert(&self->query.query_shape_cache, &key, &stored, NULL);
    *out = plan;
    return JZ_OK;
}

jz_err QES(ensure_peer_maintained_subscription_view_supported)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, const jazz_protocol_ReadViewSpec *read_view, jazz_engine_QueryAuthorizationMode authorization_mode) {
    /* JoinVia is an existential constraint on this query's root-row result,
     * not flat joined output: maintained membership and delivery remain
     * addressed by the selected root row. Flat public join output carries its
     * source tuple through the maintained terminal, so it can safely address
     * several occurrences for one root as well. */
    jazz_engine_QueryProgramRequest request; jz_btree access_paths;
    JZ_TRY(QES(current_query_program_request_and_access_paths)(self, shape, binding, tier, identity, JZN_CQPO_MAINTAINED_VIEW, read_view, NULL,
        authorization_mode, JZN_PCBM_STRICT, &request, &access_paths));
    return LWN(ensure_query_program_request_supported)(self, request, access_paths);
}

jazz_node_PreparedQueryPlan *QES(mark_peer_maintained_query_shape_cache)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, jazz_model_DurabilityTier tier) {
    jazz_node_QueryShapeCacheKey key = { shape->shape_id, tier, jazz_node__node__query_eval__prepared_bindings__query_binding_value_signature(binding) };
    bool inserted;
    jazz_node_PreparedQueryPlan **slot = jz_btree_entry(&self->query.query_shape_cache, &key, &inserted);
    if (inserted) {
        jazz_node_PreparedQueryPlan *marker = jz_rc_new(sizeof(jazz_node_PreparedQueryPlan), jazz_node_PreparedQueryPlan_drop_v);
        __builtin_memset(marker, 0, sizeof *marker);
        marker->tag = JZN_PQP_PEER_MAINTAINED_MARKER;
        *slot = marker;
    } else {
        jz_str_drop(&key.signature);
    }
    return jz_rc_clone(*slot);
}

/* ---------- seeded maintained subscription views ---------- */

static jz_bytes read_view_key_for(jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view) {
    jazz_protocol_RegisterShapeOptions options = jazz_protocol_RegisterShapeOptions_default();
    options.tier = tier;
    jazz_protocol_ReadViewSpec_drop(&options.read_view);
    jazz_protocol_ReadViewSpec_clone(&options.read_view, read_view);
    jz_bytes key = jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&options);
    jazz_protocol_RegisterShapeOptions_drop(&options);
    return key;
}

static void drop_covered_input_sources_of(jazz_node_SeededMaintainedView *v) { jz_btree_drop(&v->covered_input_sources); }

jz_err QES(open_seeded_maintained_subscription_view)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_AuthorSubject identity, jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view, jazz_node_SeededMaintainedView *out) {
    return QES(open_seeded_maintained_subscription_view_with_waker)(self, shape, binding, identity, tier, read_view, read_view_key_for(tier, read_view), NULL, out);
}

/* Owner-loop variant retaining a durable wake route during cold initial
 * hydration. */
jz_err QES(open_seeded_maintained_subscription_view_with_waker)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_AuthorSubject identity, jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view, jz_bytes read_view_key, void *progress_waker,
    jazz_node_SeededMaintainedView *out) {
    /* A scope-isolated client relay serves already admitted local data. It is
     * not an authority and may not re-evaluate policies using its deliberately
     * incomplete support-row cache. Strict remote children use the same
     * ClientLocal mode with an exact covered-input source. */
    jazz_engine_QueryAuthorizationMode authorization_mode = QES(peer_query_authorization_mode)(self);
    JZ_TRY(QES(open_seeded_maintained_subscription_view_in_authorization_mode)(self, shape, binding, identity, tier, read_view, read_view_key,
        authorization_mode, NULL, NULL, JZN_PCBM_STRICT, false, progress_waker, out));
    drop_covered_input_sources_of(out);
    return JZ_OK;
}

jazz_engine_QueryAuthorizationMode QES(peer_query_authorization_mode)(const jazz_node_NodeState *self) {
    return jazz_node__node__NodeState__client_relay_scope(self) ? JAZZ_QAM_CLIENT_LOCAL : JAZZ_QAM_TRUSTED_SERVING;
}

/* Re-publish a Core-confirmed window from a durable relay to its non-durable
 * browser peer. The relay's Global receipt already names the
 * authority-selected members, so this must consume that membership as its
 * source instead of applying the query window a second time. */
jz_err QES(open_seeded_relay_subscription_view)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_AuthorSubject identity, const jazz_protocol_ReadViewSpec *read_view, jazz_protocol_AuthorityResultKey authority_result_key,
    jazz_node_SeededMaintainedView *out) {
    return QES(open_seeded_relay_subscription_view_with_waker)(self, shape, binding, identity, read_view, read_view_key_for(JAZZ_TIER_GLOBAL, read_view),
        authority_result_key, NULL, out);
}

static void vec_append(void *dst, void *src, usize elem) {
    struct { u8 *ptr; u32 len, cap; } *s = src;
    jz_vec_extend(dst, s->ptr, s->len, elem);
    jz_free(s->ptr); s->ptr = NULL; s->len = s->cap = 0;
}
/* transitions.<field>.extend(extra.<field>) for every field, as the Rust
 * spells out at each call site. */
static void transitions_extend(jazz_node_ResultTransitions *t, jazz_node_ResultTransitions *extra, bool with_supporting) {
    vec_append(&t->adds, &extra->adds, sizeof(jazz_protocol_ResultMemberEntry));
    vec_append(&t->removes, &extra->removes, sizeof(jazz_protocol_ResultMemberEntry));
    vec_append(&t->result_payload_adds, &extra->result_payload_adds, sizeof(void *));
    vec_append(&t->result_payload_removes, &extra->result_payload_removes, sizeof(jazz_protocol_ResultMemberEntry));
    if (with_supporting) t->supporting_changed |= extra->supporting_changed;
    vec_append(&t->program_fact_adds, &extra->program_fact_adds, sizeof(void *));
    vec_append(&t->program_fact_removes, &extra->program_fact_removes, sizeof(void *));
    vec_append(&t->terminal_operations, &extra->terminal_operations, sizeof(void *));
}

jz_err QES(open_seeded_relay_subscription_view_with_waker)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_AuthorSubject identity, const jazz_protocol_ReadViewSpec *read_view, jz_bytes read_view_key, jazz_protocol_AuthorityResultKey authority_result_key,
    void *progress_waker, jazz_node_SeededMaintainedView *out) {
    jazz_protocol_BindingViewKey settled_binding_view = jazz_protocol__protocol__AuthorityResultKey__binding_view(&authority_result_key);
    jazz_protocol_AuthorityResultKey key_copy; jazz_protocol_AuthorityResultKey_clone(&key_copy, &authority_result_key);
    jazz_node_SeededMaintainedView opened;
    jz_err e = QES(open_seeded_maintained_subscription_view_in_authorization_mode)(self, shape, binding, identity, JAZZ_TIER_GLOBAL, read_view, read_view_key,
        JAZZ_QAM_CLIENT_LOCAL, &settled_binding_view, &key_copy, JZN_PCBM_STRICT, false, progress_waker, &opened);
    jazz_protocol_BindingViewKey_drop(&settled_binding_view);
    jazz_protocol_AuthorityResultKey_drop(&key_copy);
    if (JZ_IS_ERR(e)) { jazz_protocol_AuthorityResultKey_drop(&authority_result_key); return e; }
    /* Seeded relay children are ordinary receiver-local maintained views.
     * Construct their retained local state before installing the exact
     * closure so their first reset and later terminal transitions share the
     * same install -> drive -> drain reducer as a late client opener. */
    jazz_node_LocalMaintainedViewSubscription local; __builtin_memset(&local, 0, sizeof local);
    local.subscription = opened.subscription;
    local.retained_prepared_plan = NULL;
    local.maintained = opened.maintained;
    local.terminal_schemas = opened.terminal_schemas;
    local.tables = opened.tables;
    jazz_model_Query_clone(&local.result_query, &shape->query);
    local.result_table = jz_str_clone(&shape->query.table);
    local.result_schema_version = shape->schema_version;
    local.has_result_select = shape->query.has_select;
    if (shape->query.has_select) jz_vec_clone(&local.result_select, &shape->query.select, sizeof(jz_str), str_clone_v);
    if (shape->query.relation) {
        e = jazz_model__query__relation_output_projection_if_present(shape->query.relation, &local.has_result_relation_projection, &local.result_relation_projection);
        if (!JZ_IS_ERR(e)) {
            jazz_model_RelationUnionParts parts;
            if (jazz_model__query__relation_union_parts(shape->query.relation->rel, &parts)) {
                jazz_model_RelationUnionParts_drop(&parts);
                e = jazz_model__query__relation_union_leaf_projections(shape->query.relation, &local.result_relation_projections);
                local.has_result_relation_projections = !JZ_IS_ERR(e);
            }
        }
        if (JZ_IS_ERR(e)) { e.code = JZN_E_Query; goto fail; }
    }
    jz_btree_init(&local.result_set, &jazz_protocol_ResultMemberSet_desc);
    jz_btree_init(&local.result_payloads, &jazz_protocol_ResultPayloads_desc);
    jz_btree_init(&local.program_facts, &jazz_protocol_ProgramFactSet_desc);
    local.initial_received = opened.initial_received;
    jazz_protocol_ReadViewSpec rv; jazz_protocol_ReadViewSpec_clone(&rv, read_view);
    local.covered_input_receiver = jazz_node__node__query_eval__maintained_views__CoveredInputReceiver__new(opened.covered_input_sources, rv);
    jazz_node_ResultTransitions transitions = opened.transitions;
    jazz_node_ResultTransitions copy; jazz_node_ResultTransitions_clone(&copy, &transitions);
    e = jazz_node__node__query_eval__maintained_views__NodeState__apply_local_maintained_view_transitions_inner(self, &local, copy, false, NULL);
    if (JZ_IS_ERR(e)) { jazz_node_ResultTransitions_drop(&transitions); goto fail; }
    if (!jazz_node__node__query_eval__maintained_views__CoveredInputReceiver__is_empty(local.covered_input_receiver)) {
        bool has_extra; jazz_node_ResultTransitions extra;
        e = jazz_node__node__query_eval__maintained_views__NodeState__install_opened_local_covered_receiver(self, &local, &authority_result_key, progress_waker, &has_extra, &extra);
        if (JZ_IS_ERR(e)) { jazz_node_ResultTransitions_drop(&transitions); goto fail; }
        if (has_extra) { transitions_extend(&transitions, &extra, false); jazz_node_ResultTransitions_drop(&extra); }
        else local.initial_received = false;
    }
    if (local.initial_received) {
        void *witnesses;
        e = jazz_node__node__query_eval__maintained_views__NodeState__selected_deletion_witnesses(self, &authority_result_key, shape->schema_version, &witnesses);
        if (JZ_IS_ERR(e)) { jazz_node_ResultTransitions_drop(&transitions); goto fail; }
        transitions.supporting_changed |=
            jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__replace_selected_deletion_witnesses(local.maintained, witnesses);
    }
    jazz_protocol_AuthorityResultKey_drop(&authority_result_key);
    out->subscription = local.subscription; out->maintained = local.maintained; out->terminal_schemas = local.terminal_schemas;
    out->transitions = transitions; out->tables = local.tables; out->initial_received = local.initial_received;
    jz_btree_init(&out->covered_input_sources, &jazz_node_CoveredInputSources_desc);
    out->covered_input_receiver = local.covered_input_receiver;
    jazz_model_Query_drop(&local.result_query); jz_str_drop(&local.result_table);
    jz_vec_drop(&local.result_select, sizeof(jz_str), str_drop_v);
    if (local.has_result_relation_projection) jazz_model_RelationProjectColumnVec_drop(&local.result_relation_projection);
    jz_btree_drop(&local.result_set); jz_btree_drop(&local.result_payloads); jz_btree_drop(&local.program_facts);
    return JZ_OK;
fail:
    jazz_protocol_AuthorityResultKey_drop(&authority_result_key);
    groove__db__Database__unsubscribe(&self->database, groove__ivm__MultisinkSubscription__id(local.subscription));
    groove_MultisinkSubscription_free(local.subscription);
    jazz_node_MaintainedSubscriptionView_free(local.maintained);
    jazz_node_MaintainedTerminalSchemas_free(local.terminal_schemas);
    jz_btree_drop(&local.tables);
    jazz_model_Query_drop(&local.result_query); jz_str_drop(&local.result_table);
    if (local.covered_input_receiver) jazz_node_CoveredInputReceiver_free(local.covered_input_receiver);
    return e;
}

/* Hydrate a terminal CommitUnit authorization-support clause. Unlike an
 * ordinary prepared query, a missing policy claim is a denied proof and is
 * surfaced to the peer as an empty, settled authorization view. */
jz_err QES(open_seeded_authorization_support_subscription_view)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_types_AuthorSubject identity, jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view, jazz_node_SeededMaintainedView *out) {
    return QES(open_seeded_authorization_support_subscription_view_with_waker)(self, shape, binding, identity, tier, read_view, read_view_key_for(tier, read_view), NULL, out);
}

jz_err QES(open_seeded_authorization_support_subscription_view_with_waker)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, jazz_types_AuthorSubject identity, jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view,
    jz_bytes read_view_key, void *progress_waker, jazz_node_SeededMaintainedView *out) {
    JZ_TRY(QES(open_seeded_maintained_subscription_view_in_authorization_mode)(self, shape, binding, identity, tier, read_view, read_view_key,
        JAZZ_QAM_TRUSTED_SERVING, NULL, NULL, JZN_PCBM_FAIL_CLOSED_AUTHORIZATION_SUPPORT, false, progress_waker, out));
    drop_covered_input_sources_of(out);
    return JZ_OK;
}

typedef struct { jz_str name; u32 alias; } WitnessKey;
static void witness_key_drop(void *p) { jz_str_drop(&((WitnessKey *)p)->name); }
static int witness_key_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const WitnessKey *x = a, *y = b;
    int c = jz_str_cmp(&x->name, &y->name);
    return c ? c : (x->alias < y->alias ? -1 : x->alias > y->alias);
}
static const jz_btree_desc witness_table_names_desc = { sizeof(WitnessKey), sizeof(jz_str), witness_key_cmp, witness_key_drop, str_drop_v };

typedef struct { const jazz_model_Binding *binding; } LiteralRetainCtx;
static bool literal_token_alive(const void *k, void *v, void *ctx) { (void)k; (void)ctx; return jz_rc_strong(((jazz_node_LiteralShapeEntry *)v)->token) > 0; }

static void trace_line(const char *s) { jazz_types__debug_env__eprint(jz_str_from(s)); }

/* The opened maintained view the Rust returns as a 7-tuple; on any failure
 * after subscribing, the subscription is unsubscribed and every allocated
 * covered input is retired. */
jz_err QES(open_seeded_maintained_subscription_view_in_authorization_mode)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape_in,
    const jazz_model_Binding *binding_in, jazz_types_AuthorSubject identity, jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view,
    jz_bytes read_view_key, jazz_engine_QueryAuthorizationMode authorization_mode, const jazz_protocol_BindingViewKey *settled_binding_view,
    const jazz_protocol_AuthorityResultKey *settled_authority_result_key, jazz_node_PreparedClaimBindingMode prepared_claim_binding_mode,
    bool pending_overlay, void *progress_waker, jazz_node_SeededMaintainedView *out) {
    const jazz_model_RuntimeSchema *schema = catalogue_schema_for(self, shape_in->schema_version);
    if (!schema) { jz_bytes_drop(&read_view_key); return invalid_stored("query schema version is unknown"); }
    jazz_model_ValidatedQuery shape; jazz_model_Binding binding;
    jz_err e = BND(bind_query_params_with_mode)(shape_in, binding_in, schema, JAZZ_PBM_RETAIN_ALL_PARAMS, &shape);
    if (JZ_IS_ERR(e)) { jz_bytes_drop(&read_view_key); return e; }
    e = jazz_model__query__validation__ValidatedQuery__bind(&shape, clone_values(&binding_in->values), &binding);
    if (JZ_IS_ERR(e)) { e.code = JZN_E_Query; jazz_model_ValidatedQuery_drop(&shape); jz_bytes_drop(&read_view_key); return e; }
    jz_btree covered_input_sources; jz_btree_init(&covered_input_sources, &jazz_node_CoveredInputSources_desc);
    groove_MultisinkSubscription *subscription = NULL;
    jazz_node_MaintainedSubscriptionView *maintained = NULL;
    jazz_node_MaintainedTerminalSchemas *terminal_schemas = NULL;
    jz_btree tables; bool have_tables = false;
    jazz_node_ResultTransitions transitions; __builtin_memset(&transitions, 0, sizeof transitions);
    jz_btree inline_content_branch_keys; jz_btree_init(&inline_content_branch_keys, &jazz_node_BranchKeySet_desc);
    /* A lone Local-tier subscription gains nothing from routing through a
     * shared binding source, and its literal graph hydrates faster. Share only
     * once a sibling of the same shape with a different binding is open: the
     * first subscriber keeps its literal graph for its lifetime and holds a
     * token so later siblings know to prepare the shared shape. Reopening the
     * same binding (a remount, or a resubscribe whose predecessor's teardown
     * is still queued) gains nothing from sharing either, so it stays literal
     * too. */
    jz_str lone_client_local_source = {0}; bool has_lone = false;
    if (authorization_mode == JAZZ_QAM_CLIENT_LOCAL && tier == JAZZ_TIER_LOCAL && jazz_protocol__protocol__ReadViewSpec__is_default(read_view) && !settled_binding_view) {
        jz_str source_shape; bool has_source;
        e = QES(client_local_prepared_source_name)(self, &shape, &binding, identity, &has_source, &source_shape);
        if (JZ_IS_ERR(e)) goto fail_early;
        if (has_source) {
            const jazz_node_LiteralShapeEntry *entry = jz_hmap_get(&self->client_local_literal_shapes, &source_shape);
            bool sibling_open = entry && jz_rc_strong(entry->token) > 0 && !jazz_model_Binding_eq(&entry->binding, &binding);
            if (!sibling_open && !groove__db__Database__prepared_binding_source_is_bound(&self->database, &source_shape)) {
                lone_client_local_source = source_shape; has_lone = true;
            } else jz_str_drop(&source_shape);
        }
    }
    jazz_engine_QueryProgramRequest request;
    e = QES(current_query_program_request_with_prepared_claim_mode)(self, &shape, &binding, tier, identity, JZN_CQPO_MAINTAINED_VIEW, read_view,
        settled_binding_view, authorization_mode, prepared_claim_binding_mode, has_lone, &request);
    if (JZ_IS_ERR(e)) goto fail_early;
    if (settled_authority_result_key) {
        void *k; jazz_engine_SourceExpr *source;
        JZ_BTREE_FOR(&request.reads.primary.sources, k, source) {
            if (source->tag != JAZZ_SE_SETTLED_BINDING_VIEW) continue;
            if (source->authority_result_key) { jazz_protocol_AuthorityResultKey_drop(source->authority_result_key); jz_free(source->authority_result_key); }
            source->authority_result_key = jz_alloc(sizeof(jazz_protocol_AuthorityResultKey));
            jazz_protocol_AuthorityResultKey_clone(source->authority_result_key, settled_authority_result_key);
        }
    }
    if (pending_overlay) {
        if (authorization_mode != JAZZ_QAM_CLIENT_LOCAL) {
            jazz_engine_QueryProgramRequest_drop(&request);
            e = invalid_stored("pending receiver overlay requires client-local execution");
            goto fail_early;
        }
        void *k; jazz_engine_SourceExpr *source;
        JZ_BTREE_FOR(&request.reads.primary.sources, k, source) {
            if (source->tag != JAZZ_SE_SETTLED_BINDING_VIEW) continue;
            jazz_engine_SourceExpr *input = jz_alloc(sizeof *input);
            *input = *source;
            __builtin_memset(source, 0, sizeof *source);
            source->tag = JAZZ_SE_WITH_OVERLAYS;
            source->input = input;
            jazz_engine_OverlayRef pending; __builtin_memset(&pending, 0, sizeof pending); /* OverlayRef::PendingLocal */
            JZ_VEC_PUSH(source->overlays.entries, pending);
        }
    }
    jz_btree access_paths;
    e = RSN(current_query_hydration_access_paths)(self, &request, &shape, &binding, JAZZ_HL_RETAINED, &access_paths);
    if (JZ_IS_ERR(e)) { jazz_engine_QueryProgramRequest_drop(&request); goto fail_early; }
    /* Receiver inputs are allocated from the compiler's source requirements
     * before any source is resolved. This keeps the initial lowering from
     * consulting an authority result just to discover a descriptor. */
    jz_btree runtime_sources, runtime_source_descriptors;
    if (authorization_mode == JAZZ_QAM_CLIENT_LOCAL) {
        /* Local-first uses ordinary local current state for its entire
         * lifetime. Receiving an authority scope does not revoke cached
         * knowledge. Only settled remote source expressions require an exact
         * receiver input; do not convert Local into a provisional
         * authority-scoped read. */
        jz_btree_drop(&covered_input_sources);
        e = QES(allocate_client_receiver_input_sources)(self, &request, &runtime_sources, &runtime_source_descriptors, &covered_input_sources);
        if (JZ_IS_ERR(e)) {
            jz_btree_init(&covered_input_sources, &jazz_node_CoveredInputSources_desc);
            jazz_engine_QueryProgramRequest_drop(&request); jz_btree_drop(&access_paths); goto fail_early;
        }
    } else {
        jz_btree_init(&runtime_sources, &jazz_node_SourceGraphs_desc);
        jz_btree_init(&runtime_source_descriptors, &jazz_node_SourceDescriptors_desc);
    }
    jazz_engine_QueryProgram program;
    if (runtime_sources.len == 0) {
        jz_btree_drop(&runtime_sources); jz_btree_drop(&runtime_source_descriptors);
        e = LWN(compile_query_program_request_with_access_paths)(self, request, access_paths, &program);
    } else {
        jz_btree inline_sources; jz_btree_init(&inline_sources, &jazz_node_CurrentRowsBySource_desc);
        e = LWN(compile_query_program_request_with_inline_sources_access_paths_and_covered_inputs)(self, request, inline_sources, access_paths,
            runtime_sources, runtime_source_descriptors, &program);
    }
    if (JZ_IS_ERR(e)) goto fail_retire;
    if (jazz_types__debug_env__covered_input_trace()) {
        jz_str line = jz_format("JAZZ_COVERED_INPUT_TRACE stage=opened_program table=%.*s node=", (int)shape.query.table.len, shape.query.table.ptr);
        char node[37]; jz_uuid_to_string(&self->node_uuid.u, node);
        jz_str_appendf(&line, "%.36s mode=%s identity=", node, authorization_mode == JAZZ_QAM_CLIENT_LOCAL ? "ClientLocal" : "TrustedServing");
        jazz_types_AuthorSubject_debug(&line, identity);
        jz_str_appendf(&line, " tier=%u settled_view=", (unsigned)tier);
        if (settled_binding_view) { jz_str_push_cstr(&line, "Some("); jazz_protocol_BindingViewKey_debug(&line, settled_binding_view); jz_str_push_cstr(&line, ")"); }
        else jz_str_push_cstr(&line, "None");
        jz_str_push_cstr(&line, " authority_key=");
        if (settled_authority_result_key) { jz_str_push_cstr(&line, "Some("); jazz_protocol_AuthorityResultKey_debug(&line, settled_authority_result_key); jz_str_push_cstr(&line, ")"); }
        else jz_str_push_cstr(&line, "None");
        jz_str_push_cstr(&line, " sources=[");
        jazz_engine_SourceId *sid; void *sv; bool first = true;
        JZ_BTREE_FOR(&program.request.reads.primary.sources, sid, sv) { if (!first) jz_str_push_cstr(&line, ", "); first = false; jazz_engine_SourceId_debug(&line, sid); }
        jz_str_push_cstr(&line, "] descriptors=[");
        first = true;
        JZ_BTREE_FOR(jazz_engine__query_engine__QueryProgram__source_descriptors(&program), sid, sv) { if (!first) jz_str_push_cstr(&line, ", "); first = false; jazz_engine_SourceId_debug(&line, sid); }
        jz_str_push_cstr(&line, "]");
        jazz_types__debug_env__eprint(line);
    }
    jz_btree_clone(&tables, jazz_engine__query_engine__LoweredProgram__maintained_terminal_tables(&program.lowered), str_clone_v, jazz_model_TableSchema_clone);
    have_tables = true;
    jz_btree targeted_refresh_tables; jz_btree_clone(&targeted_refresh_tables, &program.lowered.targeted_refresh_tables, str_clone_v, NULL);
    bool targeted_refresh_uncertain = program.lowered.targeted_refresh_uncertain;
    terminal_schemas = jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__terminal_schemas_for_program(&program);
    jz_str binding_source_shape;
    if (program.request.input.binding.has_source_shape) binding_source_shape = jz_str_clone(&program.request.input.binding.source_shape);
    else {
        jazz_node_PreparedQueryParamVec params = LW(prepared_params_from_domain)(&program.lowered.parameters);
        binding_source_shape = jazz_node__node__query_eval__prepared_bindings__query_binding_source_shape_for_prepared_params(params.ptr, params.len);
        jz_vec_drop(&params, sizeof(jazz_node_PreparedQueryParam), jazz_node_PreparedQueryParam_drop);
    }
    jazz_engine_ProgramFactKey vw; __builtin_memset(&vw, 0, sizeof vw); vw.tag = JAZZ_PFK_VERSION_WITNESSES;
    bool storage_backed_result_materialization = !jz_btree_contains(&program.request.output.facts, &vw);
    {
        void *k; jazz_engine_SourceExpr *source;
        JZ_BTREE_FOR(&program.request.reads.primary.sources, k, source) {
            if (source->tag == JAZZ_SE_BRANCH_VIEW && source->base && source->base->tag == JAZZ_BVB_SNAPSHOT) {
                jazz_model_BranchKey bk; jazz_model_BranchKey_clone(&bk, &source->base->branch);
                jz_btree_insert(&inline_content_branch_keys, &bk, NULL, NULL);
            }
        }
    }
    /* A retained view never reads the stored columns its projection drops, so
     * it keeps them physical for its whole lifetime. Rebuilding them made a
     * listing fetch every chunk of a large value it excludes, and hold back
     * its other rows until the last chunk arrived (#3830). */
    groove_RootIndirectValues root_indirect_values;
    e = QES(maintained_projection_dropped_root_values)(self, &shape.query, shape.schema_version, &root_indirect_values);
    if (JZ_IS_ERR(e)) { jazz_engine_QueryProgram_drop(&program); jz_str_drop(&binding_source_shape); jz_btree_drop(&targeted_refresh_tables); goto fail_retire; }
    e = LWN(subscribe_lowered_program_with_root_values)(self, program, &binding, binding_source_shape, prepared_claim_binding_mode, progress_waker,
        root_indirect_values, &subscription);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&targeted_refresh_tables); goto fail_retire; }
    if (jazz_types__debug_env__covered_input_trace()) trace_line("JAZZ_COVERED_INPUT_TRACE stage=receiver_subscription_opened");
    maintained = jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__default();
    const jazz_node_PhysicalMapping *mapping = jz_btree_get(&self->catalogue.physical_mappings, &shape.schema_version);
    if (!mapping) { jz_btree_drop(&targeted_refresh_tables); e = invalid_stored("maintained scope schema has no physical mapping"); goto fail_unsubscribe; }
    jz_btree_drop(&maintained->physical_tables);
    jz_btree_init(&maintained->physical_tables, &jazz_node_PhysicalTablesByName_desc);
    {
        jz_str *name; jazz_node_PhysicalTableIdentity *table;
        JZ_BTREE_FOR(jazz_node__node__PhysicalMapping__identity_tables(mapping), name, table) {
            jz_str n = jz_str_clone(name);
            jz_btree_insert(&maintained->physical_tables, &n, &table->id, NULL);
        }
    }
    jz_btree_drop(&maintained->targeted_refresh_tables);
    maintained->targeted_refresh_tables = targeted_refresh_tables;
    maintained->targeted_refresh_uncertain = targeted_refresh_uncertain;
    jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__set_read_view(maintained, read_view_key);
    read_view_key.ptr = NULL; read_view_key.len = read_view_key.cap = 0;
    /* Resolve names from permanent physical catalogue identities, never from
     * equal row UUIDs or a search for the first matching table label. Keep
     * logical source identity unchanged: only the immutable payload coordinate
     * follows the row's authored schema across a table rename. */
    jz_btree witness_table_names; jz_btree_init(&witness_table_names, &witness_table_names_desc);
    {
        jz_str *logical_name; void *tv;
        JZ_BTREE_FOR(&tables, logical_name, tv) {
            jazz_types_PhysicalTableId table_id;
            e = jazz_node__node__physical__NodeState__physical_table_id_for_schema(self, shape.schema_version, SL(*logical_name), &table_id);
            if (JZ_IS_ERR(e)) break;
            jazz_types_SchemaVersionId *schema_id; jazz_node_PhysicalMapping *m;
            JZ_BTREE_FOR(&self->catalogue.physical_mappings, schema_id, m) {
                const u32 *alias = jz_btree_get(jazz_node__node__state__catalogue__SchemaCatalogue__schema_version_aliases(&self->catalogue), schema_id);
                if (!alias) continue;
                jz_str *authored_name; jazz_node_PhysicalTableMapping *table_mapping;
                JZ_BTREE_FOR(&m->tables, authored_name, table_mapping) {
                    if (jz_uuid_eq(&table_mapping->table_id.u, &table_id.u) && !jz_str_eq(authored_name, logical_name)) {
                        WitnessKey wk = { jz_str_clone(logical_name), *alias };
                        jz_str an = jz_str_clone(authored_name);
                        jz_btree_insert(&witness_table_names, &wk, &an, NULL);
                    }
                }
            }
        }
    }
    if (JZ_IS_ERR(e)) { jz_btree_drop(&witness_table_names); goto fail_unsubscribe; }
    jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__set_witness_table_names(maintained, witness_table_names);
    if (storage_backed_result_materialization)
        jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__enable_storage_backed_result_materialization(maintained);
    {
        jazz_model_BranchKey *bk; void *u;
        JZ_BTREE_FOR(&inline_content_branch_keys, bk, u)
            jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__enable_inline_content_branch_key(maintained, bk);
    }
    /* A cold opening may depend on a peer that can only be advanced after this
     * call returns. Keep Stream A unpublished until the first complete Stream
     * B snapshot arrives; publication drains this same subscription and gates
     * ViewUpdate on initial_received. */
    bool initial_received = false;
    for (bool first = true;; first = false) {
        groove_MultisinkDeltas deltas; jz_err event_error = JZ_OK;
        u32 poll = groove__ivm__MultisinkSubscription__poll_next_event(subscription, progress_waker, &deltas, &event_error);
        if (poll == GROOVE_POLL_PENDING) break;
        if (poll == GROOVE_POLL_ERROR) { e = jzn_groove(event_error); goto fail_unsubscribe; }
        if (first && jazz_types__debug_env__covered_input_trace()) trace_line("JAZZ_COVERED_INPUT_TRACE stage=receiver_initial_snapshot");
        jazz_node_ResultTransitions delta_transitions;
        e = jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__apply_multisink_deltas(maintained, &deltas, terminal_schemas, &tables,
            jazz_node__node__NodeState__node_aliases(self), &delta_transitions);
        if (JZ_IS_ERR(e)) goto fail_unsubscribe;
        /* A root collector's opening is its first ordinary terminal transition.
         * Retain the same root and descendant edits that seeded maintained, so
         * the first published/reset snapshot is folded from the receiver-local
         * terminal tree rather than a relational root record with empty nested
         * collections. */
        transitions_extend(&transitions, &delta_transitions, true);
        jazz_node_ResultTransitions_drop(&delta_transitions);
        initial_received = true;
    }
    if (jazz_types__debug_env__covered_input_trace()) trace_line("JAZZ_COVERED_INPUT_TRACE stage=receiver_initial_applied");
    if (has_lone) {
        void *token = jz_rc_new(1, NULL);
        jz_hmap_retain(&self->client_local_literal_shapes, literal_token_alive, NULL);
        jazz_node_LiteralShapeEntry entry; entry.token = token; /* Arc::downgrade: the Weak shares the allocation */
        jazz_model_Binding_clone(&entry.binding, &binding);
        jz_hmap_insert(&self->client_local_literal_shapes, &lone_client_local_source, &entry, NULL);
        has_lone = false;
        jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__hold_client_local_literal_token(maintained, token);
    }
    out->subscription = subscription; out->maintained = maintained; out->terminal_schemas = terminal_schemas;
    out->transitions = transitions; out->tables = tables; out->initial_received = initial_received;
    out->covered_input_sources = covered_input_sources; out->covered_input_receiver = NULL;
    jz_btree_drop(&inline_content_branch_keys);
    jazz_model_Binding_drop(&binding); jazz_model_ValidatedQuery_drop(&shape);
    return JZ_OK;
fail_unsubscribe:
    groove__db__Database__unsubscribe(&self->database, groove__ivm__MultisinkSubscription__id(subscription));
    groove_MultisinkSubscription_free(subscription);
fail_retire: {
        jz_err r = QES(retire_covered_input_sources)(self, &covered_input_sources);
        if (JZ_IS_ERR(r)) { jz_err_drop(&e); e = r; }
    }
fail_early:
    if (maintained) jazz_node_MaintainedSubscriptionView_free(maintained);
    if (terminal_schemas) jazz_node_MaintainedTerminalSchemas_free(terminal_schemas);
    if (have_tables) jz_btree_drop(&tables);
    jazz_node_ResultTransitions_drop(&transitions);
    if (has_lone) jz_str_drop(&lone_client_local_source);
    jz_btree_drop(&inline_content_branch_keys);
    jz_btree_drop(&covered_input_sources);
    jz_bytes_drop(&read_view_key);
    jazz_model_Binding_drop(&binding); jazz_model_ValidatedQuery_drop(&shape);
    return e;
}

/* ---------- prepared-shape snapshots ---------- */

jz_err QES(bind_shape_snapshot)(jazz_node_NodeState *self, groove_PreparedShapeId shape, const groove_Value *values, u32 n, groove_MultisinkDeltas *out) {
    groove_MultisinkSubscription *subscription;
    JZ_TRY(jzn_groove(groove__db__Database__bind_shape(&self->database, shape.id, values, n, &subscription)));
    u64 subscription_id = groove__ivm__MultisinkSubscription__id(subscription);
    u32 tag; groove_MultisinkDeltas *snapshot = NULL; jz_err event_error = JZ_OK;
    jz_err e = groove__db__Database__poll_initial_event(&self->database, subscription, &tag, &snapshot, &event_error);
    if (!JZ_IS_ERR(e) && tag == GROOVE_SE_ERROR) e = jzn_groove(event_error);
    groove__db__Database__unsubscribe(&self->database, subscription_id);
    groove_MultisinkSubscription_free(subscription);
    if (JZ_IS_ERR(e)) return e;
    *out = *snapshot; jz_free(snapshot);
    return JZ_OK;
}

/* Bind and immediately retire a one-shot prepared graph. Point reads compile
 * a physical-row access path, so their graph cannot be safely shared with
 * later rows; retaining it would leak one shape per advice or repair request. */
jz_err QES(bind_disposable_shape_snapshot)(jazz_node_NodeState *self, groove_PreparedShapeId shape, const groove_Value *values, u32 n,
    groove_RootIndirectValues root_indirect_values, groove_RecordDeltas *out) {
    groove_MultisinkSubscription *subscription;
    jz_err e = jzn_groove(groove__db__Database__bind_shape_with_root_values(&self->database, shape.id, values, n, root_indirect_values, &subscription));
    if (JZ_IS_ERR(e)) {
        JZ_TRY(jzn_groove(groove__db__Database__retire_prepared_shape(&self->database, shape.id)));
        return e;
    }
    u64 subscription_id = groove__ivm__MultisinkSubscription__id(subscription);
    u32 tag; groove_MultisinkDeltas *snapshot = NULL; jz_err event_error = JZ_OK;
    jz_err poll = groove__db__Database__poll_initial_event(&self->database, subscription, &tag, &snapshot, &event_error);
    if (!JZ_IS_ERR(poll) && tag == GROOVE_SE_ERROR) poll = jzn_groove(event_error);
    groove__db__Database__unsubscribe(&self->database, subscription_id);
    groove_MultisinkSubscription_free(subscription);
    jz_err retired = jzn_groove(groove__db__Database__retire_prepared_shape(&self->database, shape.id));
    if (JZ_IS_ERR(retired)) { if (!JZ_IS_ERR(poll)) { groove_MultisinkDeltas_drop(snapshot); jz_free(snapshot); } jz_err_drop(&poll); return retired; }
    if (JZ_IS_ERR(poll)) return poll;
    groove_RecordDeltas *deltas;
    e = QE(take_required_sink_deltas)(snapshot, jz_slice_of(JAZZ_APP_ROWS_SINK, (u32)sizeof(JAZZ_APP_ROWS_SINK) - 1), &deltas);
    jz_free(snapshot);
    JZ_TRY(e);
    *out = *deltas; jz_free(deltas);
    return JZ_OK;
}

/* ---- aggregate rows, historical reads and free helpers ---- */

static jz_err aggregate_append_field(const jazz_engine_AppRowSchema *output, const groove_BorrowedRecord *record,
    const groove_DescriptorField *field, const jz_str *public_name, jz_str carrier,
    groove_DescriptorFieldVec *fields, JZ_VEC(groove_Value) *values, jazz_node_CurrentRowPublicationFieldVec *publication_fields) {
    u32 index;
    jz_err e = jazz_node__node__query_eval__query_result_rows__aggregate_record_field_index(record, field, &index);
    if (JZ_IS_ERR(e)) { jz_str_drop(&carrier); return e; }
    const jazz_node_CurrentRowPublicationField *binding =
        field->has_name ? jazz_engine__query_engine__AppRowSchema__publication_field(output, &field->name) : NULL;
    if (!binding) { jz_str_drop(&carrier); return invalid_stored("lowered aggregate field has no publication binding"); }
    const jz_str *app = jazz_node__node__CurrentRowPublicationField__application_name(binding);
    if (!app || !jz_str_eq(app, public_name)) {
        jz_str_drop(&carrier);
        return invalid_stored("aggregate publication name differs from query output");
    }
    groove_ValueType vt;
    groove_ValueType_clone(&vt, &groove__records__RecordDescriptor__fields(record->descriptor)->ptr[index].value_type);
    groove_DescriptorField f = groove__records__DescriptorField__with_identity_name_owned(
        groove__records__DescriptorField__new(carrier, vt), jz_str_clone(public_name));
    groove_Value value;
    e = groove__records__BorrowedRecord__get_idx(record, index, &value);
    if (JZ_IS_ERR(e)) { groove_DescriptorField_drop(&f); return jzn_groove(e); }
    jz_vec_push(fields, &f, sizeof f);
    jz_vec_push(values, &value, sizeof value);
    jazz_node_CurrentRowPublicationField pf; jazz_node_CurrentRowPublicationField_clone(&pf, binding);
    jz_vec_push(publication_fields, &pf, sizeof pf);
    return JZ_OK;
}

jz_err QE(aggregate_current_row_from_record)(const jazz_model_Query *query, const jazz_engine_AppRowSchema *output,
    jazz_types_RowUuid row_uuid, const groove_BorrowedRecord *record, jazz_node_CurrentRow *out) {
    groove_DescriptorFieldVec fields = {0};
    JZ_VEC(groove_Value) values = {0};
    jazz_node_CurrentRowPublicationFieldVec publication_fields = {0};
    groove_ValueType uuid_ty = { GROOVE_VT_UUID, NULL };
    groove_DescriptorField row_field = groove__records__DescriptorField__new(jz_str_from("row_uuid"), uuid_ty);
    jz_vec_push(&fields, &row_field, sizeof row_field);
    groove_Value row_value = groove_Value_uuid(row_uuid.u);
    jz_vec_push(&values, &row_value, sizeof row_value);
    jazz_node_CurrentRowPublicationField hidden = { JAZZ_CRPF_RESULT_FIELD, jz_str_from("row_uuid"), JAZZ_CRRV_HIDDEN_METADATA };
    jz_vec_push(&publication_fields, &hidden, sizeof hidden);
    jz_err e = JZ_OK;
    const jazz_model_AggregateQuery *aggregate = query->aggregate;
    if (!aggregate) { e = invalid_stored("aggregate record has no aggregate query shape"); goto fail; }
    const jazz_engine_AggregateResultSchema *schema;
    e = jazz_node__node__query_eval__query_result_rows__aggregate_output_schema(output, &schema);
    if (JZ_IS_ERR(e)) goto fail;
    if (schema->group_key_fields.len != (u32)(aggregate->has_group_by ? 1 : 0)
        || schema->value_fields.len != aggregate->aggregates.len) {
        e = invalid_stored("aggregate query differs from its lowered output shape");
        goto fail;
    }
    if (aggregate->has_group_by) {
        e = aggregate_append_field(output, record, &schema->group_key_fields.ptr[0], &aggregate->group_by,
            jazz_engine__query_engine__fields__user_column_field(jz_str_slice(&aggregate->group_by)),
            &fields, (void *)&values, &publication_fields);
        if (JZ_IS_ERR(e)) goto fail;
    }
    for (u32 i = 0; i < schema->value_fields.len && i < aggregate->aggregates.len; i++) {
        const jazz_model_Aggregate *expression = &aggregate->aggregates.ptr[i];
        jz_str agg_field = jazz_engine__query_engine__fields__aggregate_output_field(jz_str_slice(&expression->alias));
        jz_str carrier = jazz_engine__query_engine__fields__user_column_field(jz_str_slice(&agg_field));
        jz_str_drop(&agg_field);
        e = aggregate_append_field(output, record, &schema->value_fields.ptr[i], &expression->alias, carrier,
            &fields, (void *)&values, &publication_fields);
        if (JZ_IS_ERR(e)) goto fail;
    }
    groove_RecordDescriptor descriptor = groove__records__RecordDescriptor__new_with_fields(fields);
    jz_bytes raw;
    e = groove__records__RecordDescriptor__create(descriptor, values.ptr, values.len, &raw);
    jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop_v);
    if (JZ_IS_ERR(e)) { jz_vec_drop(&publication_fields, sizeof(jazz_node_CurrentRowPublicationField), jazz_node_CurrentRowPublicationField_drop); return jzn_groove(e); }
    groove_OwnedRecord owned = { raw, descriptor };
    *out = jazz_node__node__CurrentRow__new_with_publication_fields(&query->table, owned, publication_fields);
    return JZ_OK;
fail:
    jz_vec_drop(&fields, sizeof(groove_DescriptorField), groove_DescriptorField_drop);
    jz_vec_drop(&values, sizeof(groove_Value), groove_Value_drop_v);
    jz_vec_drop(&publication_fields, sizeof(jazz_node_CurrentRowPublicationField), jazz_node_CurrentRowPublicationField_drop);
    return e;
}

/* Read a validated query at this handle's historical settle position.
 *
 * Partial nodes return HistoricalReadRequiresServer rather than answering
 * from incomplete local history. A later protocol slice wires that error to a
 * server-evaluated one-shot. */
jz_err QE(HistoricalRead__read)(jazz_node_HistoricalRead *self, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, jazz_node_CurrentRowVec *out) {
    if (!jazz_node__node__NodeState__is_history_complete_for(self->node, shape, self->position))
        return jz_err_new(JZN_E_HistoricalReadRequiresServer, "historical read requires server");
    return QES(query_rows_at)(self->node, shape, binding, self->position, out);
}

const jazz_node_VersionRow *QE(local_maintained_view_content_witness)(const jazz_node_VersionRowVec *versions,
    const jz_str *table, jazz_types_RowUuid row_uuid) {
    for (u32 i = versions->len; i-- > 0;) {
        const jazz_node_VersionRow *version = versions->ptr[i];
        jazz_types_RowUuid r = jazz_node__node__VersionRow__row_uuid(version);
        if (jz_str_eq(jazz_node__node__VersionRow__table(version), table)
            && memcmp(r.u.b, row_uuid.u.b, 16) == 0 && !jazz_node__node__VersionRow__is_deleted(version))
            return version;
    }
    return NULL;
}

bool QE(current_row_has_required_subscription_cells)(const jazz_node_CurrentRow *row, const jazz_model_TableSchema *table,
    const jz_vec_str *projection) {
    for (u32 i = 0; i < table->columns.len; i++) {
        const jazz_model_ColumnSchema *column = &table->columns.ptr[i];
        if (projection) {
            bool contained = false;
            for (u32 j = 0; j < projection->len; j++) if (jz_str_eq(&projection->ptr[j], &column->name)) { contained = true; break; }
            if (!contained) continue;
        }
        if (column->column_type.tag == GROOVE_VT_NULLABLE) continue;
        groove_Value cell;
        if (!jazz_node__node__CurrentRow__cell(row, table, jz_str_slice(&column->name), &cell)) return false;
        groove_Value_drop(&cell);
    }
    return true;
}

static int default_query_row_order(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const jazz_node_CurrentRow *left = a, *right = b;
    jazz_types_RowUuid lu = jazz_node__node__CurrentRow__row_uuid(left), ru = jazz_node__node__CurrentRow__row_uuid(right);
    int c = memcmp(lu.u.b, ru.u.b, 16);
    if (c) return c;
    u64 lt = 0, ln = 0, rt = 0, rn = 0;
    bool hl = jazz_node__node__CurrentRow__projected_tx_alias(left, &lt, &ln);
    bool hr = jazz_node__node__CurrentRow__projected_tx_alias(right, &rt, &rn);
    if (hl != hr) return hl ? 1 : -1;
    if (hl) {
        if (lt != rt) return lt < rt ? -1 : 1;
        if (ln != rn) return ln < rn ? -1 : 1;
    }
    return jz_slice_cmp(jazz_node__node__CurrentRow__raw(left), jazz_node__node__CurrentRow__raw(right));
}

void QE(sort_query_default_rows)(jazz_node_CurrentRowVec *rows) {
    jz_sort(rows->ptr, rows->len, sizeof(jazz_node_CurrentRow), default_query_row_order, NULL);
}

static const jz_btree_desc str_index_desc = { sizeof(jz_str), sizeof(u32), jz_cmp_str, str_drop_v, NULL };
static bool binding_lookup(void *ctx, const jz_str *name, u32 *index) {
    const u32 *found = jz_btree_get((const jz_btree *)ctx, name);
    if (!found) return false;
    *index = *found;
    return true;
}

/* Convert materializer-only rows back to the canonical application row
 * descriptor before exposing them through a one-shot query. The materializer
 * may retain physical schema/provenance fields while resolving a row, whereas
 * subscriptions are emitted from the public app-row terminal directly. */
jz_err QE(normalize_public_current_rows)(const jazz_model_Query *query, const jazz_model_TableSchema *table,
    jazz_node_CurrentRowVec *rows) {
    jz_vec_str columns = {0};
    for (u32 i = 0; i < table->columns.len; i++) { jz_str c = jz_str_clone(&table->columns.ptr[i].name); jz_vec_push(&columns, &c, sizeof c); }
    jz_err e = JZ_OK;
    for (u32 r = 0; r < rows->len; r++) {
        jazz_node_CurrentRow *row = &rows->ptr[r];
        jazz_node_CurrentRow projected;
        if (query->aggregate) {
            const jazz_model_AggregateQuery *aggregate = query->aggregate;
            /* A grouped source and an aggregate result may share a public name.
             * Select each synthetic table column through its declared role
             * before projecting; a public-name lookup alone loses that
             * distinction. */
            jz_btree bindings; jz_btree_init(&bindings, &str_index_desc);
            const jazz_node_CurrentRowPublicationFieldVec *pubs = jazz_node__node__CurrentRow__publication_fields(row);
            if (aggregate->has_group_by) {
                u32 index = 0; bool found = false;
                for (u32 i = 0; i < pubs->len; i++) {
                    const jz_str *app = jazz_node__node__CurrentRowPublicationField__application_name(&pubs->ptr[i]);
                    if (pubs->ptr[i].tag == JAZZ_CRPF_STORED_COLUMN && app && jz_str_eq(app, &aggregate->group_by)) { index = i; found = true; break; }
                }
                if (!found) { jz_btree_drop(&bindings); e = invalid_stored("aggregate group has no source publication binding"); goto done; }
                jz_str key = jz_str_clone(&aggregate->group_by);
                jz_btree_insert(&bindings, &key, &index, NULL);
            }
            for (u32 a = 0; a < aggregate->aggregates.len; a++) {
                const jazz_model_Aggregate *output = &aggregate->aggregates.ptr[a];
                u32 index = 0; bool found = false;
                for (u32 i = 0; i < pubs->len; i++) {
                    const jz_str *app = jazz_node__node__CurrentRowPublicationField__application_name(&pubs->ptr[i]);
                    if (pubs->ptr[i].tag == JAZZ_CRPF_RESULT_FIELD && pubs->ptr[i].visibility == JAZZ_CRRV_APPLICATION_CELL
                        && app && jz_str_eq(app, &output->alias)) { index = i; found = true; break; }
                }
                if (!found) { jz_btree_drop(&bindings); e = invalid_stored("aggregate output has no result publication binding"); goto done; }
                jz_str key = QE(aggregate_output_column)(&output->alias);
                jz_btree_insert(&bindings, &key, &index, NULL);
            }
            e = jazz_node__node__CurrentRow__project_with_column_binding(row, table, &columns, binding_lookup, &bindings, &projected);
            jz_btree_drop(&bindings);
        } else {
            e = jazz_node__node__CurrentRow__project(row, table, &columns, &projected);
        }
        if (JZ_IS_ERR(e)) goto done;
        jazz_node_CurrentRow_drop(row);
        *row = projected;
    }
done:
    jz_vec_drop(&columns, sizeof(jz_str), str_drop_v);
    return e;
}

void QE(apply_query_window)(const jazz_model_Query *query, jazz_node_CurrentRowVec *rows) {
    u32 offset = query->offset < rows->len ? query->offset : rows->len;
    u32 limit = query->has_limit ? query->limit : rows->len - offset;
    u32 end = sat_add_u32(offset, limit);
    if (end > rows->len) end = rows->len;
    if (offset > 0 || end < rows->len) {
        jazz_node_CurrentRowVec windowed = {0};
        for (u32 i = offset; i < end; i++) {
            jazz_node_CurrentRow c; jazz_node_CurrentRow_clone(&c, &rows->ptr[i]);
            jz_vec_push(&windowed, &c, sizeof c);
        }
        jazz_node_CurrentRowVec_drop(rows);
        *rows = windowed;
    }
}

static const groove_ColumnType MAGIC_U64_TYPE = { GROOVE_VT_U64, NULL };
const groove_ColumnType *QE(magic_current_column_type)(jz_slice column) {
    const groove_ValueType *ty = jazz_types__ids__AuthorSubject__metadata_type(column);
    if (ty) return ty;
    if (jz_slice_cmp(column, JZ_LIT("$createdAt")) == 0 || jz_slice_cmp(column, JZ_LIT("$updatedAt")) == 0) return &MAGIC_U64_TYPE;
    return NULL;
}

bool QE(is_magic_current_column)(jz_slice column) {
    return QE(magic_current_column_type)(column) != NULL;
}

static void collect_operand_param(const jazz_model_Operand *operand, jz_btree *params) {
    if (operand->tag == JAZZ_OPERAND_PARAM) {
        jz_str p = jz_str_clone(&operand->name);
        if (jz_btree_contains(params, &p)) jz_str_drop(&p);
        else jz_btree_insert(params, &p, NULL, NULL);
    }
}

static void extend_str_set(jz_btree *dst, jz_btree src) { jz_btree_append(dst, &src); jz_btree_drop(&src); }

jz_btree QE(predicate_params)(const jazz_model_Predicate *predicates, u32 n) {
    jz_btree params = empty_str_set();
    for (u32 i = 0; i < n; i++) {
        const jazz_model_Predicate *predicate = &predicates[i];
        switch (predicate->tag) {
        case JAZZ_PRED_ALL: case JAZZ_PRED_ANY:
            extend_str_set(&params, QE(predicate_params)(predicate->children.ptr, predicate->children.len));
            break;
        case JAZZ_PRED_NOT: case JAZZ_PRED_ENUM_MATCH:
            extend_str_set(&params, QE(predicate_params)(predicate->inner, 1));
            break;
        case JAZZ_PRED_EQ: case JAZZ_PRED_NE: case JAZZ_PRED_GT: case JAZZ_PRED_GTE:
        case JAZZ_PRED_LT: case JAZZ_PRED_LTE: case JAZZ_PRED_CONTAINS:
            collect_operand_param(&predicate->left, &params);
            collect_operand_param(&predicate->right, &params);
            break;
        case JAZZ_PRED_IN:
            collect_operand_param(&predicate->left, &params);
            for (u32 c = 0; c < predicate->options.len; c++) collect_operand_param(&predicate->options.ptr[c], &params);
            break;
        case JAZZ_PRED_IS_NULL:
            collect_operand_param(&predicate->left, &params);
            break;
        }
    }
    return params;
}

bool QE(query_order_value)(const jazz_node_CurrentRow *row, const jazz_model_TableSchema *table, const jz_str *column, groove_Value *out) {
    if (jz_str_eq_cstr(column, "id")) { *out = groove_Value_uuid(jazz_node__node__CurrentRow__row_uuid(row).u); return true; }
    if (QE(is_magic_current_column)(jz_str_slice(column))) return jazz_node__node__CurrentRow__raw_field(row, jz_str_slice(column), out);
    return jazz_node__node__CurrentRow__cell(row, table, jz_str_slice(column), out);
}

jz_str QE(aggregate_output_column)(const jz_str *output) {
    return jazz_engine__query_engine__fields__aggregate_output_column(jz_str_slice(output));
}
