/* Port of crates/jazz/layers/node/src/node/query_eval/maintained_views.rs
 * Local maintained-view state, reconciliation, resets, and terminal deltas.
 *
 * Subscription control-plane routing lives in the subscriptions module; this
 * module owns the live Groove subscription, authoritative membership state,
 * cached materialization inputs, and conversion of terminal changes into
 * public deltas. */
#include "jazz_node/node/node_qe_b.h"

typedef jazz_node_LocalMaintainedViewSubscription Local;
typedef jazz_node_CoveredInputReceiver Receiver;

/* CoveredInstall: how a covered install runs (see `CoveredInstall::DetachCold`, #3349). */
typedef enum { COVERED_INSTALL_DETACH_COLD = 0, COVERED_INSTALL_COMPLETE = 1 } CoveredInstall;

static void trace_line(jz_str *line) { jz_js_log(1, line->ptr, line->len); jz_str_drop(line); } /* eprintln! */

/* ---------------- CoveredInputReceiver ---------------- */

jazz_node_CoveredInputReceiver MV(CoveredInputReceiver__new)(jz_btree sources, jazz_protocol_ReadViewSpec read_view)
{
    jazz_node_CoveredInputReceiver r;
    memset(&r, 0, sizeof r);
    r.sources = sources;
    r.read_view = read_view;
    jz_btree_init(&r.installed_records, &jazz_node_GPT_INSTALLED_RECORDS_DESC);
    return r;
}

bool MV(CoveredInputReceiver__is_empty)(const jazz_node_CoveredInputReceiver *self)
{
    return self->sources.len == 0;
}

/* ---------------- LocalMaintainedViewSubscription ---------------- */

/* Seed a facade's retained decoder at its opening boundary. Copy the
 * compiler-addressed collections already decoded by the local terminal; do not
 * infer collection semantics from arbitrary array-valued columns. */
jz_err MV(LocalMaintainedViewSubscription__decoded_terminal_records)(const Local *self, jz_btree *out)
{
    jz_btree_init(out, &jazz_node_OCCURRENCE_TERMINAL_RECORD_DESC);
    const jz_btree *records = jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__decoded_terminal_records(self->maintained);
    const jazz_node_TerminalRootLayout *layout = MV(LocalMaintainedViewSubscription__terminal_root_layout)(self);
    bool root_union_arm = layout && layout->root_union_arm;
    const jz_bytes *key;
    void *const *record;
    JZ_BTREE_FOR(records, key, record) {
        jazz_types_OutputOccurrenceId occ;
        jz_err e = jazz_node__node__terminal_root__terminal_root_occurrence_id_with_root_union(key, root_union_arm, &occ);
        if (JZ_IS_ERR(e)) { jz_btree_drop(out); return jazz_node__node__api_error__from_node_error(e); }
        void *clone = jazz_node_TerminalRecordState_clone_box(*record);
        jz_btree_insert(out, &occ, &clone, NULL);
    }
    return JZ_OK;
}

const jazz_node_TerminalRootLayout *MV(LocalMaintainedViewSubscription__terminal_root_layout)(const Local *self)
{
    return jazz_node__node__MaintainedTerminalSchemas__terminal_root_layout(self->terminal_schemas);
}

bool MV(LocalMaintainedViewSubscription__has_root_collector)(const Local *self)
{
    return jazz_node__node__MaintainedTerminalSchemas__has_root_collector(self->terminal_schemas);
}

/* Whether this graph has consumed its first local terminal batch. Before this
 * point an empty materialization is pending, not an authorization result. */
bool MV(LocalMaintainedViewSubscription__initial_snapshot_received)(const Local *self)
{
    return self->initial_received;
}

jz_err MV(LocalMaintainedViewSubscription__relation_projection_for_member)(const Local *self, const jazz_node_ResultMemberEntry *member, const jazz_model_RelationProjectColumnVec **out)
{
    bool has;
    jazz_types_OutputOccurrenceId occurrence;
    JZ_TRY(jazz_node__node__query_eval__public_result_member_occurrence_id(member, &self->result_table, self->result_query.aggregate != NULL, &has, &occurrence));
    if (!has) return JZN_INVALID_STORED("maintained union member has no occurrence identity");
    jz_err e = MV(LocalMaintainedViewSubscription__relation_projection_for_occurrence)(self, &occurrence, out);
    jazz_types_OutputOccurrenceId_drop(&occurrence);
    return e;
}

jz_err MV(LocalMaintainedViewSubscription__relation_projection_for_occurrence)(const Local *self, const jazz_types_OutputOccurrenceId *occurrence, const jazz_model_RelationProjectColumnVec **out)
{
    if (!self->has_result_relation_projections) {
        *out = self->has_result_relation_projection ? &self->result_relation_projection : NULL;
        return JZ_OK;
    }
    u32 n;
    const jazz_types_UnionArm *arms = jazz_types__object__OutputOccurrenceId__union_arms(occurrence, &n);
    const jz_str *label = NULL;
    for (u32 i = 0; i < n; i++)
        if (arms[i].position == 0) { label = &arms[i].label; break; }
    if (!label) return JZN_INVALID_STORED("maintained union member has no position-0 arm label");
    const jazz_model_RelationProjectColumnVec *p = jz_btree_get(&self->result_relation_projections, label);
    if (!p) return JZN_INVALID_STORED("maintained union member has an unknown arm label");
    *out = p;
    return JZ_OK;
}

u64 MV(LocalMaintainedViewSubscription__subscription_id)(const Local *self)
{
    return groove__ivm__MultisinkSubscription__id(self->subscription);
}

/* Whether a storage publication can affect this maintained view's inputs. */
bool MV(LocalMaintainedViewSubscription__uses_logical_tables)(const Local *self, const jz_hmap *changed_logical_tables)
{
    if (self->maintained->targeted_refresh_uncertain) return true;
    const jz_str *table;
    const void *v;
    JZ_BTREE_FOR(&self->maintained->targeted_refresh_tables, table, v) {
        (void)v;
        if (jz_hmap_get(changed_logical_tables, table)) return true;
    }
    return false;
}

const jazz_types_OutputOccurrenceIdVec *MV(LocalMaintainedViewSubscription__root_occurrence_ids)(const Local *self)
{
    return &self->root_occurrence_ids;
}

bool MV(LocalMaintainedViewSubscription__has_covered_input_sources)(const Local *self)
{
    return !MV(CoveredInputReceiver__is_empty)(&self->covered_input_receiver);
}

bool MV(LocalMaintainedViewSubscription__has_installed_covered_closure)(const Local *self, const jazz_protocol_AuthorityResultKey *key, u64 generation)
{
    const Receiver *r = &self->covered_input_receiver;
    return r->has_installed_closure && jazz_protocol_AuthorityResultKey_eq(&r->installed_closure_key, key) &&
           r->installed_closure_generation == generation;
}

static bool MV(LocalMaintainedViewSubscription__needs_ordered_relation_snapshot)(const Local *self)
{
    bool query_has_window = self->result_query.order_by.len != 0 || self->result_query.has_limit || self->result_query.offset != 0;
    bool relation_has_union_window = false;
    if (self->result_query.relation) {
        const jazz_model_RelationQuery *relation = self->result_query.relation;
        const jazz_model_RelationUnionParts *parts = jazz_model__query__relation_resolution__relation_union_parts(relation->rel);
        relation_has_union_window = jazz_model__query__relation_union_presentation_order(relation) ||
                                    (parts && (parts->has_limit || parts->has_offset));
    }
    return (self->has_result_relation_projection || self->has_result_relation_projections) &&
           (query_has_window || relation_has_union_window);
}

/* ---------------- NodeState impl ---------------- */

static jz_err MV(NodeState__drain_local_maintained_view_subscription_transitions)(jazz_node_NodeState *self, Local *local, const jazz_protocol_AuthorityResultKey *authoritative_result_key, const void *progress_waker, bool *has, jazz_node_ResultTransitions *out);
static jz_err MV(NodeState__apply_local_maintained_view_transitions)(jazz_node_NodeState *self, Local *local, jazz_node_ResultTransitions transitions, jazz_node_LocalMaintainedViewSubscriptionUpdate *out);
static jz_err MV(NodeState__replace_covered_input_receiver_in)(jazz_node_NodeState *self, Receiver *receiver, jazz_types_SchemaVersionId schema, const jazz_protocol_AuthorityResultKey *key, CoveredInstall install, bool *out);
static jz_err MV(NodeState__apply_covered_input_receiver_incremental)(jazz_node_NodeState *self, Receiver *receiver, jazz_types_SchemaVersionId schema, const jazz_protocol_AuthorityResultKey *key, jazz_node_AuthoritySourceIncremental incremental, bool *out);
static CoveredInstall MV(NodeState__local_covered_install)(const jazz_node_NodeState *self);
static jz_err MV(NodeState__covered_input_runtime_record)(jazz_node_NodeState *self, const jazz_protocol_SupportingRow *input, const jazz_node_CoveredInputSource *runtime_source, jazz_types_SchemaVersionId schema, const jazz_protocol_ReadViewSpec *read_view, bool *has, jz_bytes *out);
static jz_err MV(NodeState__project_covered_input_row_in_read_view)(jazz_node_NodeState *self, const jazz_model_TableSchema *table, jazz_types_SchemaVersionId schema, const jazz_protocol_ReadViewSpec *read_view, const jazz_node_VersionRow *version, jazz_node_CurrentRow row, jazz_node_CurrentRow *out);
static bool jazz_node__node__query_eval__maintained_views__drained_transition_is_empty(bool states_empty, bool payload_states_empty, bool fact_states_empty, const groove_TerminalOperationVec *terminal_operations);

jz_err MV(NodeState__open_maintained_view_subscription_in_authorization_mode)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_types_AuthorSubject identity, jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view, const jazz_node_SubscriptionPreparedPlan *retained, jazz_engine_QueryAuthorizationMode mode, Local *local_out, jazz_node_RelationSnapshot *snapshot_out)
{
    return MV(NodeState__open_maintained_view_subscription_in_authorization_mode_with_waker)(
        self, shape, binding, identity, tier, read_view, retained, mode, false, NULL, local_out, snapshot_out);
}

/* Owner-loop variant that preserves a durable wake route while opening cold
 * maintained subscription hydration. */
jz_err MV(NodeState__open_maintained_view_subscription_in_authorization_mode_with_waker)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_types_AuthorSubject identity, jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *read_view, const jazz_node_SubscriptionPreparedPlan *retained, jazz_engine_QueryAuthorizationMode mode, bool pending_overlay, const void *progress_waker, Local *local_out, jazz_node_RelationSnapshot *snapshot_out)
{
    if (retained) {
        if (retained->authorization_mode != mode)
            return JZN_INVALID_STORED("maintained subscription retained a plan from another authorization mode");
        JZ_ASSERT(jz_rc_strong(retained->plan) > 0, "retained plan is alive");
    }
    bool has_settled_binding_view = false;
    jazz_protocol_BindingViewKey settled_binding_view;
    if (mode == JZ_QAM_CLIENT_LOCAL)
        has_settled_binding_view = jazz_node__node__query_eval__NodeState__client_settled_binding_view_key_for_query(
            self, shape, binding, tier, read_view, &settled_binding_view);
    /* A newly attached local receiver must immediately consume an
     * already-settled *exact* authority closure.  The binding-view key is
     * only routing; resolve its unique scoped receipt here rather than
     * opening an empty receiver and letting facade refresh infer output from
     * a sibling/authority result. */
    bool has_settled_key = false;
    jazz_protocol_AuthorityResultKey settled_key;
    if (has_settled_binding_view)
        has_settled_key = jazz_node__node__query_eval__subscriptions__NodeState__unique_authority_result_key_for_binding_view(
            self, settled_binding_view, &settled_key);

    jazz_protocol_RegisterShapeOptions opts = jazz_protocol_RegisterShapeOptions_default();
    opts.tier = tier;
    jazz_protocol_ReadViewSpec_drop(&opts.read_view);
    jazz_protocol_ReadViewSpec_clone(&opts.read_view, read_view);
    jazz_protocol_ReadViewKey read_view_key = jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&opts);
    jazz_protocol_RegisterShapeOptions_drop(&opts);

    jazz_node_SeededMaintainedOpen opened;
    jz_err e = jazz_node__node__NodeState__open_seeded_maintained_subscription_view_in_authorization_mode(
        self, shape, binding, identity, tier, read_view, read_view_key, mode,
        has_settled_binding_view ? &settled_binding_view : NULL,
        has_settled_key ? &settled_key : NULL,
        JZN_PCBM_STRICT, pending_overlay, progress_waker, &opened);
    if (JZ_IS_ERR(e)) goto fail_key;

    Local local;
    memset(&local, 0, sizeof local);
    local.subscription = opened.subscription;
    if (retained) { local.has_retained_prepared_plan = true; local.retained_prepared_plan = *retained; }
    local.maintained = opened.maintained;
    local.terminal_schemas = opened.terminal_schemas;
    local.tables = opened.tables;
    jazz_model_Query_clone(&local.result_query, &shape->query);
    local.result_table = jz_str_clone(&shape->query.table);
    local.result_schema_version = shape->schema_version;
    local.has_result_select = shape->query.has_select;
    if (shape->query.has_select) jz_vec_clone(&local.result_select, &shape->query.select, sizeof(jz_str), (void (*)(void *, const void *))jazz_node_qeb_str_clone);
    if (shape->query.relation) {
        e = jazz_model__query__relation_output_projection_if_present(shape->query.relation, &local.has_result_relation_projection, &local.result_relation_projection);
        if (JZ_IS_ERR(e)) goto fail_local;
        if (jazz_model__query__relation_resolution__relation_union_parts(shape->query.relation->rel)) {
            e = jazz_model__query__relation_union_leaf_projections(shape->query.relation, &local.result_relation_projections);
            if (JZ_IS_ERR(e)) goto fail_local;
            local.has_result_relation_projections = true;
        }
    }
    jz_btree_init(&local.result_set, &jazz_protocol_RESULT_MEMBER_SET_DESC);
    jz_btree_init(&local.result_payloads, &jazz_protocol_RESULT_MEMBER_PAYLOAD_DESC);
    jz_btree_init(&local.program_facts, &jazz_protocol_PROGRAM_FACT_SET_DESC);
    local.initial_received = opened.initial_received;
    {
        jazz_protocol_ReadViewSpec rv;
        jazz_protocol_ReadViewSpec_clone(&rv, read_view);
        local.covered_input_receiver = MV(CoveredInputReceiver__new)(opened.covered_input_sources, rv);
    }
    if (jazz_types__debug_env__covered_input_trace()) {
        jz_str line = jz_format("JAZZ_COVERED_INPUT_TRACE stage=open root_terminal=%s sources=%u initial_received=%s",
            MV(LocalMaintainedViewSubscription__has_root_collector)(&local) ? "true" : "false",
            local.covered_input_receiver.sources.len, local.initial_received ? "true" : "false");
        trace_line(&line);
    }
    if (jazz_types__debug_env__covered_input_trace()) {
        jz_str line = jz_format("JAZZ_COVERED_INPUT_TRACE stage=initial_transitions adds=%u removes=%u facts_adds=%u facts_removes=%u terminal_ops=%u",
            opened.transitions.adds.len, opened.transitions.removes.len, opened.transitions.program_fact_adds.len,
            opened.transitions.program_fact_removes.len, opened.transitions.terminal_operations.len);
        trace_line(&line);
    }
    {
        jazz_node_LocalMaintainedViewSubscriptionUpdate initial_delta;
        e = MV(NodeState__apply_local_maintained_view_transitions)(self, &local, opened.transitions, &initial_delta);
        if (JZ_IS_ERR(e)) goto fail_local_moved;
        jazz_node_LocalMaintainedViewSubscriptionUpdate_drop(&initial_delta);
    }
    if (MV(LocalMaintainedViewSubscription__has_covered_input_sources)(&local) && has_settled_key) {
        /* Local terminal initialization and authority closure readiness are
         * independent. A missing closure must not erase a consumed initial
         * batch: an eventual empty closure need not produce another batch.
         * Publication still checks the exact authority receipt separately. */
        bool installed;
        jazz_node_ResultTransitions t;
        e = MV(NodeState__install_opened_local_covered_receiver)(self, &local, &settled_key, progress_waker, &installed, &t);
        if (JZ_IS_ERR(e)) goto fail_local_moved;
        if (installed) jazz_node_ResultTransitions_drop(&t);
    }
    jazz_node_LocalMaintainedRelationSnapshot initial;
    e = MAT(NodeState__materialize_local_maintained_relation_snapshot_with_occurrences)(self, &local, &initial);
    if (JZ_IS_ERR(e)) goto fail_local_moved;
    jz_vec_drop(&local.root_occurrence_ids, sizeof(jazz_types_OutputOccurrenceId), jazz_types_OutputOccurrenceId_drop);
    local.root_occurrence_ids = initial.root_occurrence_ids;
    *local_out = local;
    *snapshot_out = initial.snapshot;
    if (has_settled_key) jazz_protocol_AuthorityResultKey_drop(&settled_key);
    return JZ_OK;
fail_local:
    jazz_node_ResultTransitions_drop(&opened.transitions);
fail_local_moved:
    local.has_retained_prepared_plan = false; /* still owned by the caller on failure */
    jazz_node_LocalMaintainedViewSubscription_drop(&local);
fail_key:
    if (has_settled_key) jazz_protocol_AuthorityResultKey_drop(&settled_key);
    return e;
}

jz_err MV(NodeState__drain_local_maintained_view_subscription)(jazz_node_NodeState *self, Local *local, const jazz_protocol_AuthorityResultKey *key, bool *has, jazz_node_LocalMaintainedViewSubscriptionUpdate *out)
{
    return MV(NodeState__drain_local_maintained_view_subscription_with_waker)(self, local, key, NULL, has, out);
}

jz_err MV(NodeState__drain_local_maintained_view_subscription_with_waker)(jazz_node_NodeState *self, Local *local, const jazz_protocol_AuthorityResultKey *key, const void *waker, bool *has, jazz_node_LocalMaintainedViewSubscriptionUpdate *out)
{
    jz_btree empty;
    jz_btree_init(&empty, &jazz_node_TABLE_ROW_SET_DESC);
    bool suppressed;
    jz_err e = MV(NodeState__drain_local_maintained_view_subscription_preserving_rows_with_waker)(self, local, key, &empty, waker, has, out, &suppressed);
    jz_btree_drop(&empty);
    return e;
}

jz_err MV(NodeState__drain_local_maintained_view_subscription_preserving_rows_with_waker)(jazz_node_NodeState *self, Local *local, const jazz_protocol_AuthorityResultKey *key, const jz_btree *preserved_row_keys, const void *waker, bool *has, jazz_node_LocalMaintainedViewSubscriptionUpdate *out, bool *suppressed)
{
    (void)preserved_row_keys;
    /* The drain no longer preserves rows or suppresses authoritative changes;
     * the parameter and the constant `false` keep the caller API. */
    *suppressed = false;
    *has = false;
    bool got;
    jazz_node_ResultTransitions transitions;
    JZ_TRY(MV(NodeState__drain_local_maintained_view_subscription_transitions)(self, local, key, waker, &got, &transitions));
    if (!got) return JZ_OK;
    JZ_TRY(MV(NodeState__apply_local_maintained_view_transitions)(self, local, transitions, out));
    *has = true;
    return JZ_OK;
}

jz_err MV(NodeState__drain_local_maintained_view_subscription_state_with_waker)(jazz_node_NodeState *self, Local *local, const jazz_protocol_AuthorityResultKey *key, const void *waker, bool *out)
{
    *out = false;
    bool got;
    jazz_node_ResultTransitions transitions;
    JZ_TRY(MV(NodeState__drain_local_maintained_view_subscription_transitions)(self, local, key, waker, &got, &transitions));
    if (!got) return JZ_OK;
    jazz_node_LocalMaintainedViewSubscriptionUpdate u;
    JZ_TRY(MV(NodeState__apply_local_maintained_view_transitions_inner)(self, local, transitions, false, &u));
    jazz_node_LocalMaintainedViewSubscriptionUpdate_drop(&u);
    *out = true;
    return JZ_OK;
}

/* Install an already-claimed exact authority closure into a newly opened
 * receiver, drive the one shared Groove graph to quiescence, and fold the
 * resulting local terminal into its retained state.  Both ordinary late
 * client opening and seeded relay opening use this sequence: neither may read
 * an authority result/output cache to synthesize its reset.
 *
 * `None` means the receipt has not claimed a complete closure yet.  A present
 * empty transition is a valid empty exact closure and remains distinct from
 * that pending state. */
jz_err MV(NodeState__install_opened_local_covered_receiver)(jazz_node_NodeState *self, Local *local, const jazz_protocol_AuthorityResultKey *key, const void *waker, bool *has, jazz_node_ResultTransitions *out)
{
    *has = false;
    bool replaced;
    JZ_TRY(MV(NodeState__replace_local_maintained_covered_inputs)(self, local, key, &replaced));
    if (!replaced) return JZ_OK;
    JZ_TRY(jazz_node__node__NodeState__drive_ready_query_runtime_with_waker(self, waker));
    bool got;
    jazz_node_ResultTransitions transitions;
    JZ_TRY(MV(NodeState__drain_local_maintained_view_subscription_transitions)(self, local, key, waker, &got, &transitions));
    if (!got) jazz_node_ResultTransitions_default(&transitions);
    /* Keep the retained receiver snapshot in lockstep with the exact same
     * terminal batch the caller publishes.  `false` avoids a second facade
     * materialization; state still folds terminal ordering/moves. */
    jazz_node_ResultTransitions copy;
    jazz_node_ResultTransitions_clone(&copy, &transitions);
    jazz_node_LocalMaintainedViewSubscriptionUpdate u;
    jz_err e = MV(NodeState__apply_local_maintained_view_transitions_inner)(self, local, copy, false, &u);
    if (JZ_IS_ERR(e)) { jazz_node_ResultTransitions_drop(&transitions); return e; }
    jazz_node_LocalMaintainedViewSubscriptionUpdate_drop(&u);
    *out = transitions;
    *has = true;
    return JZ_OK;
}

bool MV(NodeState__local_maintained_authority_reconciliation_due)(const jazz_node_NodeState *self, const Local *local, const jazz_protocol_AuthorityResultKey *key)
{
    return MV(NodeState__covered_input_receiver_reconciliation_due)(self, &local->covered_input_receiver, key);
}

bool MV(NodeState__covered_input_receiver_reconciliation_due)(const jazz_node_NodeState *self, const Receiver *receiver, const jazz_protocol_AuthorityResultKey *key)
{
    /* A local-knowledge graph has no authority input slots to replace. Remote
     * receipts can advance sync without gating or retiring its locally stored
     * rows and pending writes. */
    if (MV(CoveredInputReceiver__is_empty)(receiver)) return false;
    const jazz_node_AuthorityResultState *authority_result = jz_btree_get(&self->query.authority_results, key);
    if (!authority_result) return false;
    if (authority_result->source_closure.tag != JZN_ASC_CLAIMED) return false;
    return jazz_node__node__query_eval__local_authority_reconciliation__LocalAuthorityReconciliation__is_due(
        &receiver->local_authority, key, authority_result->source_closure.generation);
}

/* Whether a covered receiver still has admitted evaluation work, such as an
 * evaluation detached while it waits for large-value chunks. A failed
 * receiver has none: its error must be drained and reported, not waited on
 * (a failed chunk fetch would otherwise stall it silently). */
bool MV(NodeState__covered_receiver_evaluation_pending)(const jazz_node_NodeState *self, const Local *local)
{
    return MV(LocalMaintainedViewSubscription__has_covered_input_sources)(local) &&
           jazz_node__node__NodeState__subscription_has_pending_query_evaluation(self, MV(LocalMaintainedViewSubscription__subscription_id)(local));
}

/* Replace the exact authority-covered source frontier of a receiver's local
 * maintained graph. The authority selects and ships the input closure; this
 * function neither re-runs policy nor reads an arbitrary current winner as a
 * fallback.
 *
 * Every compiled source occurrence is keyed by full `ProgramSourceId`. A fact
 * for an unknown source is a protocol/compile mismatch, not an opportunity to
 * guess by table or collector name. Replacements are submitted as one
 * database batch so the graph observes only the old or new closure, never a
 * cross-source mixture. */
jz_err MV(NodeState__replace_local_maintained_covered_inputs)(jazz_node_NodeState *self, Local *local, const jazz_protocol_AuthorityResultKey *key, bool *out)
{
    *out = false;
    const jazz_node_AuthorityResultState *authority_result = jz_btree_get(&self->query.authority_results, key);
    if (!authority_result)
        return JZN_INVALID_STORED("covered input reconciliation has no exact authority receipt");
    u64 closure_generation;
    switch (authority_result->source_closure.tag) {
    case JZN_ASC_PENDING: return JZ_OK;
    case JZN_ASC_CLAIMED: closure_generation = authority_result->source_closure.generation; break;
    default: JZ_UNREACHABLE();
    }
    Receiver *r = &local->covered_input_receiver;
    bool installed_for_authority = r->has_installed_closure && jazz_protocol_AuthorityResultKey_eq(&r->installed_closure_key, key);
    if (!installed_for_authority)
        return MV(NodeState__replace_covered_input_receiver_in)(self, r, local->result_schema_version, key, MV(NodeState__local_covered_install)(self), out);
    /* Generic ViewUpdate frames may change facts unrelated to the receiver
     * input frontier.  Only the source-closure generation names the state an
     * incremental source delta may be applied to. */
    if (r->has_installed_generation && r->installed_generation == closure_generation) return JZ_OK;
    /* A claimed closure with no retained deltas is a reset successor. It
     * intentionally replaces all sources; only a non-reset publication carries
     * the predecessor-preserving incremental record below. */
    if (authority_result->source_incrementals.len == 0)
        return MV(NodeState__replace_covered_input_receiver_in)(self, r, local->result_schema_version, key, MV(NodeState__local_covered_install)(self), out);
    if (!r->has_installed_generation) jz_panic("installed receiver has a source receipt");
    jazz_node_AuthoritySourceIncremental incremental;
    JZ_TRY(jazz_node__node__coalesce_authority_source_incrementals(&authority_result->source_incrementals, r->installed_generation, closure_generation, &incremental));
    return MV(NodeState__apply_covered_input_receiver_incremental)(self, r, local->result_schema_version, key, incremental, out);
}

typedef struct { bool has; jz_bytes record; } StagedRecord;
static void staged_record_drop(void *p) { StagedRecord *s = p; if (s->has) jz_bytes_drop(&s->record); }
static const jz_btree_desc STAGED_RECORDS_DESC = {sizeof(jazz_protocol_SupportingRow), sizeof(StagedRecord), jazz_protocol_SupportingRow_cmp, jazz_protocol_SupportingRow_drop, staged_record_drop};
typedef JZ_VEC(jz_bytes) BytesVec;
typedef struct { BytesVec adds, removes; } SourceChange;
static void bytes_drop_v(void *p) { jz_bytes_drop(p); }
static void source_change_drop(void *p) { SourceChange *c = p; jz_vec_drop(&c->adds, sizeof(jz_bytes), bytes_drop_v); jz_vec_drop(&c->removes, sizeof(jz_bytes), bytes_drop_v); }
static const jz_btree_desc CHANGES_DESC = {sizeof(jazz_types_GlobalPhysicalTableId), sizeof(SourceChange), jz_cmp_uuid, NULL, source_change_drop};

static SourceChange *change_entry(jz_btree *changes, const jazz_types_GlobalPhysicalTableId *t)
{
    bool inserted;
    SourceChange *c = jz_btree_entry(changes, t, &inserted);
    if (inserted) memset(c, 0, sizeof *c);
    return c;
}

/* Apply one authority frame's exact source delta to the already-installed
 * receiver frontier. This is intentionally distinct from reset: it never
 * scans/replaces an unchanged source set. */
static jz_err MV(NodeState__apply_covered_input_receiver_incremental)(jazz_node_NodeState *self, Receiver *receiver, jazz_types_SchemaVersionId schema, const jazz_protocol_AuthorityResultKey *key, jazz_node_AuthoritySourceIncremental incremental, bool *out)
{
    jz_err e = JZ_OK;
    *out = false;
    if (!receiver->has_installed_generation || receiver->installed_generation != incremental.predecessor_generation) {
        jazz_node_AuthoritySourceIncremental_drop(&incremental);
        return JZN_INVALID_STORED("covered input incremental delta does not name receiver predecessor");
    }
    /* Stage the local successor before changing Groove or receipt state. The
     * runtime validates its complete batch before mutation; keeping this
     * mirror staged gives the same all-or-nothing failure boundary. */
    jz_btree staged_records, changes;
    jz_btree_init(&staged_records, &STAGED_RECORDS_DESC);
    jz_btree_init(&changes, &CHANGES_DESC);
    JZ_VEC_FOR(jazz_protocol_SupportingRow, input, incremental.removes) {
        const jz_btree *records = jz_btree_get(&receiver->installed_records, &input->physical_table);
        if (!records) { e = JZN_INVALID_STORED("incremental covered input removes an unknown source occurrence"); goto done; }
        const jz_bytes *record = jz_btree_get(records, input);
        if (record) {
            jazz_protocol_SupportingRow k;
            jazz_protocol_SupportingRow_clone(&k, input);
            StagedRecord none = {false, {0}};
            StagedRecord old;
            if (jz_btree_insert(&staged_records, &k, &none, &old)) { jazz_protocol_SupportingRow_drop(&k); staged_record_drop(&old); }
            JZ_VEC_PUSH(change_entry(&changes, &input->physical_table)->removes, jz_bytes_clone(record));
        } else if (input->version.layer == JZ_LAYER_CONTENT) {
            e = JZN_INVALID_STORED("incremental covered input removes a content witness absent from receiver predecessor");
            goto done;
        }
    }
    JZ_VEC_FOR(jazz_protocol_SupportingRow, input, incremental.adds) {
        const jazz_node_CoveredInputSource *runtime_source = jz_btree_get(&receiver->sources, &input->physical_table);
        if (!runtime_source) { e = JZN_INVALID_STORED("incremental covered input names no compiled source occurrence"); goto done; }
        bool has_record;
        jz_bytes record;
        e = MV(NodeState__covered_input_runtime_record)(self, input, runtime_source, schema, &receiver->read_view, &has_record, &record);
        if (JZ_IS_ERR(e)) goto done;
        if (!has_record) continue;
        const jz_btree *records = jz_btree_get(&receiver->installed_records, &input->physical_table);
        if (!records) jz_panic("receiver records are initialized with every compiled source");
        const StagedRecord *staged = jz_btree_get(&staged_records, input);
        bool present = staged ? staged->has : jz_btree_contains(records, input);
        if (present) {
            jz_bytes_drop(&record);
            e = JZN_INVALID_STORED("incremental covered input duplicates a receiver content witness");
            goto done;
        }
        jazz_protocol_SupportingRow k;
        jazz_protocol_SupportingRow_clone(&k, input);
        StagedRecord some = {true, jz_bytes_clone(&record)};
        StagedRecord old;
        if (jz_btree_insert(&staged_records, &k, &some, &old)) { jazz_protocol_SupportingRow_drop(&k); staged_record_drop(&old); }
        JZ_VEC_PUSH(change_entry(&changes, &input->physical_table)->adds, record);
    }
    {
        JZ_VEC(groove_InputSourceDelta) deltas = {0};
        jazz_types_GlobalPhysicalTableId source;
        SourceChange change;
        while (jz_btree_pop_first(&changes, &source, &change)) {
            const jazz_node_CoveredInputSource *runtime_source = jz_btree_get(&receiver->sources, &source);
            if (!runtime_source) jz_panic("changed source was validated above");
            groove_InputSourceDelta d;
            memset(&d, 0, sizeof d);
            d.id = runtime_source->id;
            d.descriptor = groove_RecordDescriptor_clone(runtime_source->descriptor);
            memcpy(&d.adds, &change.adds, sizeof d.adds);
            memcpy(&d.removes, &change.removes, sizeof d.removes);
            JZ_VEC_PUSH(deltas, d);
        }
        groove_InputSourceMetrics metrics;
        switch (MV(NodeState__local_covered_install)(self)) {
        /* See `CoveredInstall::DetachCold` (#3349). */
        case COVERED_INSTALL_DETACH_COLD:
            e = groove__db__query__Database__apply_input_source_deltas_detaching_cold(self->database, deltas.ptr, deltas.len, &metrics);
            break;
        case COVERED_INSTALL_COMPLETE:
            e = groove__db__query__Database__apply_input_source_deltas_metrics(self->database, deltas.ptr, deltas.len, &metrics);
            break;
        }
        jz_vec_drop(&deltas, sizeof(groove_InputSourceDelta), groove_InputSourceDelta_drop);
        if (JZ_IS_ERR(e)) { e = jazz_node__node__Error_groove(e); goto done; }
        if (jazz_types__debug_env__covered_input_trace()) {
            jz_str line = jz_format("JAZZ_COVERED_INPUT_TRACE stage=incremental_receiver_delta predecessor=%llu generation=%llu tick=%llu processed=%llu",
                (unsigned long long)incremental.predecessor_generation, (unsigned long long)incremental.generation,
                (unsigned long long)metrics.tick, (unsigned long long)metrics.records_processed);
            trace_line(&line);
        }
    }
    {
        jazz_protocol_AuthorityResultKey k;
        jazz_protocol_AuthorityResultKey_clone(&k, key);
        jazz_node__node__query_eval__local_authority_reconciliation__LocalAuthorityReconciliation__replace_source(&receiver->local_authority, k, incremental.generation);
    }
    if (receiver->has_installed_closure) jazz_protocol_AuthorityResultKey_drop(&receiver->installed_closure_key);
    jazz_protocol_AuthorityResultKey_clone(&receiver->installed_closure_key, key);
    receiver->has_installed_closure = true;
    receiver->installed_closure_generation = incremental.generation;
    receiver->has_installed_generation = true;
    receiver->installed_generation = incremental.generation;
    /* Every fallible preparation and the atomic runtime batch succeeded.
     * Commit only touched mirror entries; untouched records never clone. */
    {
        jazz_protocol_SupportingRow input;
        StagedRecord record;
        while (jz_btree_pop_first(&staged_records, &input, &record)) {
            jz_btree *records = jz_btree_get(&receiver->installed_records, &input.physical_table);
            if (!records) jz_panic("staged source was validated before runtime mutation");
            if (record.has) {
                jz_bytes old;
                jazz_protocol_SupportingRow oldk;
                if (jz_btree_remove(records, &input, &oldk, &old)) { jazz_protocol_SupportingRow_drop(&oldk); jz_bytes_drop(&old); }
                jz_btree_insert(records, &input, &record.record, NULL);
            } else {
                jazz_protocol_SupportingRow oldk;
                jz_bytes old;
                if (jz_btree_remove(records, &input, &oldk, &old)) { jazz_protocol_SupportingRow_drop(&oldk); jz_bytes_drop(&old); }
                jazz_protocol_SupportingRow_drop(&input);
            }
        }
    }
    *out = true;
done:
    jz_btree_drop(&staged_records);
    jz_btree_drop(&changes);
    jazz_node_AuthoritySourceIncremental_drop(&incremental);
    return e;
}

/* Install one exact authority closure into a receiver-owned source map. Both
 * the facade and relay publication use this same source-only path. This form
 * completes the install's evaluation before returning, for callers that read
 * the result immediately (relay publication, one-shot reads). */
jz_err MV(NodeState__replace_covered_input_receiver)(jazz_node_NodeState *self, Receiver *receiver, jazz_types_SchemaVersionId schema, const jazz_protocol_AuthorityResultKey *key, bool *out)
{
    return MV(NodeState__replace_covered_input_receiver_in)(self, receiver, schema, key, COVERED_INSTALL_COMPLETE, out);
}

/* How a local subscriber's covered install runs: detached only for a host
 * that drops pending ticks (see `CoveredInstall::DetachCold`). */
static CoveredInstall MV(NodeState__local_covered_install)(const jazz_node_NodeState *self)
{
    return jazz_node__node__NodeState__detaches_covered_chunk_waits(self) ? COVERED_INSTALL_DETACH_COLD : COVERED_INSTALL_COMPLETE;
}

typedef struct { jazz_types_GlobalPhysicalTableId old, new_; } Remap;

static jz_err MV(NodeState__replace_covered_input_receiver_in)(jazz_node_NodeState *self, Receiver *receiver, jazz_types_SchemaVersionId schema, const jazz_protocol_AuthorityResultKey *key, CoveredInstall install, bool *out)
{
    *out = false;
    if (receiver->sources.len == 0) return JZ_OK;
    /* A local graph may be prepared before the first trusted catalogue
     * replaces its bootstrap identities. Bind its existing table inputs to
     * that catalogue at the snapshot boundary, not to a stale bootstrap UUID.
     * This is once per table, never an occurrence expansion or row lookup. */
    JZ_VEC(Remap) remapping = {0};
    {
        const jazz_types_GlobalPhysicalTableId *old;
        const jazz_node_CoveredInputSource *source;
        JZ_BTREE_FOR(&receiver->sources, old, source) {
            Remap m;
            m.old = *old;
            jz_err e = jazz_node__node__views__NodeState__scope_physical_table(self, schema, source->table, &m.new_);
            if (JZ_IS_ERR(e)) { jz_vec_drop(&remapping, sizeof(Remap), NULL); return e; }
            JZ_VEC_PUSH(remapping, m);
        }
    }
    JZ_VEC_FOR(Remap, m, remapping) {
        if (jz_uuid_eq(&m->old, &m->new_)) continue;
        jazz_node_CoveredInputSource source;
        if (!jz_btree_remove(&receiver->sources, &m->old, NULL, &source)) jz_panic("compiled table exists");
        jazz_node_CoveredInputSource prev;
        if (jz_btree_insert(&receiver->sources, &m->new_, &source, &prev)) jazz_node_CoveredInputSource_drop(&prev);
    }
    jz_vec_drop(&remapping, sizeof(Remap), NULL);
    /* A newer authority generation invalidates the old source closure before
     * any validation.  In particular, a reset cannot reuse a detached receipt
     * just because its rows happen to look identical. */
    if (receiver->has_installed_closure) jazz_protocol_AuthorityResultKey_drop(&receiver->installed_closure_key);
    receiver->has_installed_closure = false;
    const jazz_node_AuthorityResultState *authority_result = jz_btree_get(&self->query.authority_results, key);
    if (!authority_result)
        return JZN_INVALID_STORED("covered input replacement has no exact authority receipt");
    u64 closure_generation;
    switch (authority_result->source_closure.tag) {
    case JZN_ASC_PENDING:
        /* Opening a usage site is not a claim that every source is empty. Keep
         * strict receivers pending until an exact reset manifest arrives; this
         * is deliberately not an error. */
        if (jazz_types__debug_env__covered_input_trace()) {
            jz_str line = jz_format("JAZZ_COVERED_INPUT_TRACE stage=covered_closure_pending sources=%u", receiver->sources.len);
            trace_line(&line);
        }
        return JZ_OK;
    case JZN_ASC_CLAIMED: closure_generation = authority_result->source_closure.generation; break;
    default: JZ_UNREACHABLE();
    }
    JZ_VEC(jazz_protocol_SupportingRow) inputs = {0};
    {
        const void *ck;
        const jazz_protocol_SupportingRow *input;
        JZ_BTREE_FOR(&authority_result->covered_input_versions, ck, input) {
            (void)ck;
            jazz_protocol_SupportingRow c;
            jazz_protocol_SupportingRow_clone(&c, input);
            JZ_VEC_PUSH(inputs, c);
        }
    }
    jz_btree records;
    jz_btree_init(&records, &jazz_node_GPT_INSTALLED_RECORDS_DESC);
    {
        const jazz_types_GlobalPhysicalTableId *source;
        const void *v;
        JZ_BTREE_FOR(&receiver->sources, source, v) {
            (void)v;
            jz_btree rows;
            jz_btree_init(&rows, &jazz_node_SUPPORTING_ROW_BYTES_DESC);
            jz_btree_insert(&records, source, &rows, NULL);
        }
    }
    jz_err e = JZ_OK;
    u32 i = 0;
    for (; i < inputs.len; i++) {
        jazz_protocol_SupportingRow *input = JZ_VEC_AT(inputs, i);
        const jazz_node_CoveredInputSource *runtime_source = jz_btree_get(&receiver->sources, &input->physical_table);
        if (!runtime_source) { e = JZN_INVALID_STORED("authority covered input names no compiled source occurrence"); break; }
        bool has_record;
        jz_bytes record;
        e = MV(NodeState__covered_input_runtime_record)(self, input, runtime_source, schema, &receiver->read_view, &has_record, &record);
        if (JZ_IS_ERR(e)) break;
        if (!has_record) { jazz_protocol_SupportingRow_drop(input); continue; }
        jz_btree *rows = jz_btree_get(&records, &input->physical_table);
        if (!rows) jz_panic("source presence was checked above");
        jazz_protocol_SupportingRow oldk;
        jz_bytes old;
        if (jz_btree_remove(rows, input, &oldk, &old)) { jazz_protocol_SupportingRow_drop(&oldk); jz_bytes_drop(&old); }
        jz_btree_insert(rows, input, &record, NULL);
    }
    if (JZ_IS_ERR(e)) {
        for (; i < inputs.len; i++) jazz_protocol_SupportingRow_drop(JZ_VEC_AT(inputs, i));
        jz_vec_drop(&inputs, sizeof(jazz_protocol_SupportingRow), NULL);
        jz_btree_drop(&records);
        return e;
    }
    jz_vec_drop(&inputs, sizeof(jazz_protocol_SupportingRow), NULL);

    jz_str counts = jz_str_from("[");
    JZ_VEC(groove_InputSourceReplacement) replacements = {0};
    {
        const jazz_types_GlobalPhysicalTableId *source;
        const jazz_node_CoveredInputSource *runtime_source;
        bool first = true;
        JZ_BTREE_FOR(&receiver->sources, source, runtime_source) {
            groove_InputSourceReplacement r;
            memset(&r, 0, sizeof r);
            r.id = runtime_source->id;
            r.descriptor = groove_RecordDescriptor_clone(runtime_source->descriptor);
            const jz_btree *rows = jz_btree_get(&records, source);
            if (rows) {
                const void *rk;
                const jz_bytes *rv;
                JZ_BTREE_FOR(rows, rk, rv) { (void)rk; JZ_VEC_PUSH(r.records, jz_bytes_clone(rv)); }
            }
            char id[37];
            jz_uuid_to_string(source, id);
            jz_str_appendf(&counts, "%s(%s, %u)", first ? "" : ", ", id, rows ? rows->len : 0);
            first = false;
            JZ_VEC_PUSH(replacements, r);
        }
    }
    jz_str_push_char(&counts, ']');
    groove_InputSourceMetrics m;
    switch (install) {
    /* See `CoveredInstall::DetachCold` (#3349). */
    case COVERED_INSTALL_DETACH_COLD:
        e = groove__db__query__Database__replace_input_sources_detaching_cold(self->database, replacements.ptr, replacements.len, &m);
        break;
    case COVERED_INSTALL_COMPLETE:
        e = groove__db__query__Database__replace_input_sources_metrics(self->database, replacements.ptr, replacements.len, &m);
        break;
    }
    jz_vec_drop(&replacements, sizeof(groove_InputSourceReplacement), groove_InputSourceReplacement_drop);
    if (JZ_IS_ERR(e)) { jz_str_drop(&counts); jz_btree_drop(&records); return jazz_node__node__Error_groove(e); }
    if (jazz_types__debug_env__covered_input_trace()) {
        jz_str line = jz_format("JAZZ_COVERED_INPUT_TRACE stage=replaced sources=%.*s tick=%llu processed=%llu notifications=%llu notification_records=%llu",
            (int)counts.len, counts.ptr, (unsigned long long)m.tick, (unsigned long long)m.records_processed,
            (unsigned long long)m.notifications_sent, (unsigned long long)m.notification_records);
        trace_line(&line);
    }
    jz_str_drop(&counts);
    {
        jazz_protocol_AuthorityResultKey k;
        jazz_protocol_AuthorityResultKey_clone(&k, key);
        jazz_node__node__query_eval__local_authority_reconciliation__LocalAuthorityReconciliation__replace_source(&receiver->local_authority, k, closure_generation);
    }
    jazz_protocol_AuthorityResultKey_clone(&receiver->installed_closure_key, key);
    receiver->has_installed_closure = true;
    receiver->installed_closure_generation = closure_generation;
    receiver->has_installed_generation = true;
    receiver->installed_generation = closure_generation;
    jz_btree_drop(&receiver->installed_records);
    receiver->installed_records = records;
    *out = true;
    return JZ_OK;
}

/* Decode one exact covered source fact into its descriptor-bound runtime
 * tuple. Deletion witnesses are validated but intentionally contribute no
 * tuple: their absence retracts a previously installed content carrier. */
static jz_err MV(NodeState__covered_input_runtime_record)(jazz_node_NodeState *self, const jazz_protocol_SupportingRow *input, const jazz_node_CoveredInputSource *runtime_source, jazz_types_SchemaVersionId schema, const jazz_protocol_ReadViewSpec *read_view, bool *has, jz_bytes *out)
{
    *has = false;
    jazz_model_TableSchema source_table;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema(self, runtime_source->table, schema, &source_table));
    jz_err e;
    bool has_version;
    jazz_node_VersionRow version;
    e = jazz_node__node__views__NodeState__covered_input_version(self, input, schema, &has_version, &version);
    if (JZ_IS_ERR(e)) goto out_table;
    if (!has_version) { e = jazz_node__node__Error_missing_transaction(input->version.tx); goto out_table; }
    if (jazz_node__node__codec__VersionRow__is_deleted(&version)) goto out_version;
    bool has_row;
    jazz_node_CurrentRow row;
    e = MAT(NodeState__projected_current_row_from_materialized_version_in_read_schema)(self, schema, &version, &has_row, &row);
    if (JZ_IS_ERR(e)) goto out_version;
    if (!has_row) { e = JZN_INVALID_STORED("authority covered content version cannot materialize a current row"); goto out_version; }
    /* Initial resets and incremental edits share the same logical branch
     * projection, while the encoded witness below retains the physical
     * supplying branch. A base row shown through a head is still a base
     * version; neither projection nor a relay hop can relabel its identity. */
    jazz_node_CurrentRow projected;
    e = MV(NodeState__project_covered_input_row_in_read_view)(self, &source_table, schema, read_view, &version, row, &projected);
    if (JZ_IS_ERR(e)) goto out_version;
    if (!jz_str_eq(jazz_node__node__CurrentRow__table(&projected), &source_table.name)) {
        jazz_node_CurrentRow_drop(&projected);
        e = JZN_INVALID_STORED("authority covered input does not project into its compiled source schema");
        goto out_version;
    }
    /* Cells are projected for the requested read schema, but the hidden
     * witness identifies immutable history. Keep its authored alias so
     * publication reloads the original record rather than treating the
     * logical projection as a synthetic version in the read schema. */
    jazz_types_SchemaVersionAlias schema_alias = jazz_node__node__codec__VersionRow__schema_version_alias(&version);
    jazz_model_BranchKey branch = jazz_node__node__codec__VersionRow__branch_key(&version);
    e = jazz_node__node__query_eval__read_sources__covered_input_record(&source_table, runtime_source->descriptor, &projected, schema_alias, &branch, out);
    jazz_model_BranchKey_drop(&branch);
    jazz_node_CurrentRow_drop(&projected);
    if (!JZ_IS_ERR(e)) *has = true;
out_version:
    jazz_node_VersionRow_drop(&version);
out_table:
    jazz_model_TableSchema_drop(&source_table);
    return e;
}

static jz_err MV(NodeState__project_covered_input_row_in_read_view)(jazz_node_NodeState *self, const jazz_model_TableSchema *table, jazz_types_SchemaVersionId schema, const jazz_protocol_ReadViewSpec *read_view, const jazz_node_VersionRow *version, jazz_node_CurrentRow row, jazz_node_CurrentRow *out)
{
    if (read_view->source.tag != JZ_RVS_BRANCH_VIEW) { *out = row; return JZ_OK; }
    jazz_node_CurrentRow_drop(&row);
    const jazz_protocol_SchemaVersion *sv = jz_btree_get(&self->catalogue.catalogue_schemas, &schema);
    if (!sv) return JZN_INVALID_STORED("covered input read schema is unknown");
    jazz_model_BranchKey head;
    jz_err e = jazz_model__schema__JazzSchema__project_branch_view_selector(&sv->schema, table, read_view->source.head, &head);
    if (JZ_IS_ERR(e)) {
        /* .map_err(Error::InvalidBranchKey) */
        jz_err r = jz_err_fmt(JZN_E_InvalidBranchKey, "invalid branch key: %.*s",
                              e.msg ? (int)e.msg->len : 0, e.msg ? e.msg->ptr : "");
        jz_err_drop(&e);
        return r;
    }
    jz_btree a, b, versions, d;
    jz_btree_init(&a, &jazz_node_ROW_UUID_VERSION_ROW_DESC);
    jz_btree_init(&b, &jazz_node_ROW_UUID_VERSION_ROW_DESC);
    jz_btree_init(&versions, &jazz_node_ROW_UUID_VERSION_ROW_DESC);
    jz_btree_init(&d, &jazz_node_ROW_UUID_VERSION_ROW_DESC);
    jazz_types_RowUuid rid = jazz_node__node__codec__VersionRow__row_uuid(version);
    jazz_node_VersionRow vc;
    jazz_node_VersionRow_clone(&vc, version);
    jz_btree_insert(&versions, &rid, &vc, NULL);
    jazz_node_CurrentRowVec rows;
    e = jazz_node__node__views__NodeState__materialize_branch_view_winners(self, &table->name, schema, &head, a, b, versions, d, &rows);
    jazz_model_BranchKey_drop(&head);
    JZ_TRY(e);
    if (rows.len == 0) {
        jz_vec_drop(&rows, sizeof(jazz_node_CurrentRow), jazz_node_CurrentRow_drop);
        return JZN_INVALID_STORED("covered branch input did not materialize");
    }
    *out = rows.ptr[0];
    for (u32 i = 1; i < rows.len; i++) jazz_node_CurrentRow_drop(&rows.ptr[i]);
    jz_vec_drop(&rows, sizeof(jazz_node_CurrentRow), NULL);
    return JZ_OK;
}

/* drain state maps */
typedef struct { bool before, after; } BeforeAfter;
typedef struct { bool has_before; jazz_protocol_ResultMemberPayloadEntry before; bool has_after; jazz_protocol_ResultMemberPayloadEntry after; } PayloadBeforeAfter;
static void payload_ba_drop(void *p)
{
    PayloadBeforeAfter *s = p;
    if (s->has_before) jazz_protocol_ResultMemberPayloadEntry_drop(&s->before);
    if (s->has_after) jazz_protocol_ResultMemberPayloadEntry_drop(&s->after);
}
static const jz_btree_desc MEMBER_STATES_DESC = {sizeof(jazz_node_ResultMemberEntry), sizeof(BeforeAfter), jazz_protocol_ResultMemberEntry_cmp, jazz_protocol_ResultMemberEntry_drop, NULL};
static const jz_btree_desc PAYLOAD_STATES_DESC = {sizeof(jazz_node_ResultMemberEntry), sizeof(PayloadBeforeAfter), jazz_protocol_ResultMemberEntry_cmp, jazz_protocol_ResultMemberEntry_drop, payload_ba_drop};
static const jz_btree_desc FACT_STATES_DESC = {sizeof(jazz_protocol_ProgramFactEntry), sizeof(BeforeAfter), jazz_protocol_ProgramFactEntry_cmp, jazz_protocol_ProgramFactEntry_drop, NULL};

static void state_set(jz_btree *states, jazz_node_ResultMemberEntry entry, bool before, bool after, void (*kdrop)(void *))
{
    bool inserted;
    BeforeAfter *s = jz_btree_entry(states, &entry, &inserted);
    if (inserted) { s->before = before; s->after = after; }
    else { s->after = after; kdrop(&entry); }
}

static jz_err MV(NodeState__drain_local_maintained_view_subscription_transitions)(jazz_node_NodeState *self, Local *local, const jazz_protocol_AuthorityResultKey *authoritative_result_key, const void *progress_waker, bool *has, jazz_node_ResultTransitions *out)
{
    *has = false;
    if (authoritative_result_key && MV(NodeState__local_maintained_authority_reconciliation_due)(self, local, authoritative_result_key)) {
        /* Drive the receiver graph from the exact source closure before
         * draining its terminal. This replaces the former authority
         * membership/result shortcut. */
        bool replaced;
        JZ_TRY(MV(NodeState__replace_local_maintained_covered_inputs)(self, local, authoritative_result_key, &replaced));
        if (!replaced) {
            /* The exact usage site has not received a closure manifest yet. Do
             * not drain/publish the graph's opening state as a strict remote
             * result; the runtime will retain the pending authority reset and
             * try again once the manifest arrives. */
            return JZ_OK;
        }
    }
    JZ_TRY(jazz_node__node__NodeState__drive_ready_query_runtime_with_waker(self, progress_waker));
    /* A covered receiver's evaluation may be waiting on large-value chunks.
     * Its terminal is incomplete until that work finishes, so do not drain
     * (and let the caller publish) a partial authority state. */
    if (authoritative_result_key && MV(NodeState__covered_receiver_evaluation_pending)(self, local)) return JZ_OK;

    jz_btree states, payload_states, fact_states;
    jz_btree_init(&states, &MEMBER_STATES_DESC);
    jz_btree_init(&payload_states, &PAYLOAD_STATES_DESC);
    jz_btree_init(&fact_states, &FACT_STATES_DESC);
    groove_TerminalOperationVec terminal_operations = {0};
    jz_err e = JZ_OK;
    for (;;) {
        groove_MultisinkBatch deltas;
        u32 r = groove__ivm__MultisinkSubscription__try_recv(local->subscription, &deltas);
        if (r == GROOVE_TRY_RECV_EMPTY) break;
        if (r == GROOVE_TRY_RECV_DISCONNECTED) { e = jz_err_fmt(JZN_E_SubscriptionClosed, "prepared point-read subscription closed"); goto fail; }
        local->initial_received = true;
        if (jazz_types__debug_env__covered_input_trace()) {
            jz_str line = jz_format("JAZZ_COVERED_INPUT_TRACE stage=drain sinks=%u terminals=%u", deltas.sinks_len, deltas.terminal_sinks_len);
            trace_line(&line);
        }
        jazz_node_ResultTransitions transitions;
        e = jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__apply_multisink_deltas(
            local->maintained, deltas, local->terminal_schemas, &local->tables, self->node_aliases, &transitions);
        if (JZ_IS_ERR(e)) goto fail;
        JZ_VEC_FOR(groove_TerminalOperation, op, transitions.terminal_operations) JZ_VEC_PUSH(terminal_operations, *op);
        transitions.terminal_operations.len = 0;
        if (jazz_types__debug_env__covered_input_trace()) {
            jz_str line = jz_format("JAZZ_COVERED_INPUT_TRACE stage=transitions terminal_ops=%u adds=%u removes=%u",
                terminal_operations.len, transitions.adds.len, transitions.removes.len);
            trace_line(&line);
        }
        JZ_VEC_FOR(jazz_node_ResultMemberEntry, entry, transitions.adds) {
            bool before = jz_btree_contains(&local->result_set, entry);
            state_set(&states, *entry, before, true, jazz_protocol_ResultMemberEntry_drop);
        }
        transitions.adds.len = 0;
        JZ_VEC_FOR(jazz_node_ResultMemberEntry, entry, transitions.removes) {
            bool before = jz_btree_contains(&local->result_set, entry);
            state_set(&states, *entry, before, false, jazz_protocol_ResultMemberEntry_drop);
        }
        transitions.removes.len = 0;
        JZ_VEC_FOR(jazz_node_ResultMemberEntry, member, transitions.result_payload_removes) {
            bool inserted;
            PayloadBeforeAfter *s = jz_btree_entry(&payload_states, member, &inserted);
            if (inserted) {
                const jazz_protocol_ResultMemberPayloadEntry *b = jz_btree_get(&local->result_payloads, member);
                s->has_before = b != NULL;
                if (b) jazz_protocol_ResultMemberPayloadEntry_clone(&s->before, b);
                s->has_after = false;
            } else {
                if (s->has_after) jazz_protocol_ResultMemberPayloadEntry_drop(&s->after);
                s->has_after = false;
                jazz_protocol_ResultMemberEntry_drop(member);
            }
        }
        transitions.result_payload_removes.len = 0;
        JZ_VEC_FOR(jazz_protocol_MemberPayloadPair, pair, transitions.result_payload_adds) {
            bool inserted;
            PayloadBeforeAfter *s = jz_btree_entry(&payload_states, &pair->member, &inserted);
            if (inserted) {
                const jazz_protocol_ResultMemberPayloadEntry *b = jz_btree_get(&local->result_payloads, &pair->member);
                s->has_before = b != NULL;
                if (b) jazz_protocol_ResultMemberPayloadEntry_clone(&s->before, b);
            } else {
                if (s->has_after) jazz_protocol_ResultMemberPayloadEntry_drop(&s->after);
                jazz_protocol_ResultMemberEntry_drop(&pair->member);
            }
            s->has_after = true;
            s->after = pair->payload;
        }
        transitions.result_payload_adds.len = 0;
        JZ_VEC_FOR(jazz_protocol_ProgramFactEntry, fact, transitions.program_fact_adds) {
            bool before = jz_btree_contains(&local->program_facts, fact);
            bool inserted;
            BeforeAfter *s = jz_btree_entry(&fact_states, fact, &inserted);
            if (inserted) { s->before = before; s->after = true; }
            else { s->after = true; jazz_protocol_ProgramFactEntry_drop(fact); }
        }
        transitions.program_fact_adds.len = 0;
        JZ_VEC_FOR(jazz_protocol_ProgramFactEntry, fact, transitions.program_fact_removes) {
            bool before = jz_btree_contains(&local->program_facts, fact);
            bool inserted;
            BeforeAfter *s = jz_btree_entry(&fact_states, fact, &inserted);
            if (inserted) { s->before = before; s->after = false; }
            else { s->after = false; jazz_protocol_ProgramFactEntry_drop(fact); }
        }
        transitions.program_fact_removes.len = 0;
        jazz_node_ResultTransitions_drop(&transitions);
    }
    if (jazz_node__node__query_eval__maintained_views__drained_transition_is_empty(
            states.len == 0, payload_states.len == 0, fact_states.len == 0, &terminal_operations)) {
        jz_vec_drop(&terminal_operations, sizeof(groove_TerminalOperation), groove_TerminalOperation_drop);
        jz_btree_drop(&states);
        jz_btree_drop(&payload_states);
        jz_btree_drop(&fact_states);
        return JZ_OK;
    }
    jazz_node_ResultTransitions transitions;
    jazz_node_ResultTransitions_default(&transitions);
    transitions.authoritative_membership_changed = false;
    transitions.terminal_operations = terminal_operations;
    {
        jazz_node_ResultMemberEntry entry;
        BeforeAfter s;
        while (jz_btree_pop_first(&states, &entry, &s)) {
            if (!s.before && s.after) JZ_VEC_PUSH(transitions.adds, entry);
            else if (s.before && !s.after) JZ_VEC_PUSH(transitions.removes, entry);
            else jazz_protocol_ResultMemberEntry_drop(&entry);
        }
    }
    {
        jazz_node_ResultMemberEntry member;
        PayloadBeforeAfter s;
        while (jz_btree_pop_first(&payload_states, &member, &s)) {
            if (!s.has_before && s.has_after) {
                jazz_protocol_MemberPayloadPair p = {member, s.after};
                JZ_VEC_PUSH(transitions.result_payload_adds, p);
            } else if (s.has_before && !s.has_after) {
                JZ_VEC_PUSH(transitions.result_payload_removes, member);
                jazz_protocol_ResultMemberPayloadEntry_drop(&s.before);
            } else if (s.has_before && s.has_after && !jazz_protocol_ResultMemberPayloadEntry_eq(&s.before, &s.after)) {
                jazz_node_ResultMemberEntry c;
                jazz_protocol_ResultMemberEntry_clone(&c, &member);
                JZ_VEC_PUSH(transitions.result_payload_removes, c);
                jazz_protocol_MemberPayloadPair p = {member, s.after};
                JZ_VEC_PUSH(transitions.result_payload_adds, p);
                jazz_protocol_ResultMemberPayloadEntry_drop(&s.before);
            } else {
                jazz_protocol_ResultMemberEntry_drop(&member);
                payload_ba_drop(&s);
            }
        }
    }
    {
        jazz_protocol_ProgramFactEntry fact;
        BeforeAfter s;
        while (jz_btree_pop_first(&fact_states, &fact, &s)) {
            if (!s.before && s.after) JZ_VEC_PUSH(transitions.program_fact_adds, fact);
            else if (s.before && !s.after) JZ_VEC_PUSH(transitions.program_fact_removes, fact);
            else jazz_protocol_ProgramFactEntry_drop(&fact);
        }
    }
    /* Aggregate facts are the payload vocabulary for synthetic result members.
     * Preserve their current values alongside membership while coalescing a
     * multisink batch; otherwise a present NULL or a revised aggregate can be
     * mistaken for an absent payload at materialization. */
    if (local->result_query.aggregate) {
        jz_vec_drop(&transitions.result_payload_adds, sizeof(jazz_protocol_MemberPayloadPair), NULL);
        jz_vec_drop(&transitions.result_payload_removes, sizeof(jazz_node_ResultMemberEntry), jazz_protocol_ResultMemberEntry_drop);
        memset(&transitions.result_payload_adds, 0, sizeof transitions.result_payload_adds);
        memset(&transitions.result_payload_removes, 0, sizeof transitions.result_payload_removes);
        JZ_VEC_FOR(jazz_protocol_ProgramFactEntry, fact, transitions.program_fact_adds) {
            if (fact->tag != JZ_PFE_RESULT_PAYLOAD) continue;
            jazz_protocol_MemberPayloadPair p;
            jazz_protocol_ResultMemberEntry_clone(&p.member, &fact->result_payload.member);
            jazz_protocol_ResultMemberPayloadEntry_clone(&p.payload, &fact->result_payload);
            JZ_VEC_PUSH(transitions.result_payload_adds, p);
        }
        JZ_VEC_FOR(jazz_protocol_ProgramFactEntry, fact, transitions.program_fact_removes) {
            if (fact->tag != JZ_PFE_RESULT_PAYLOAD) continue;
            jazz_node_ResultMemberEntry m;
            jazz_protocol_ResultMemberEntry_clone(&m, &fact->result_payload.member);
            JZ_VEC_PUSH(transitions.result_payload_removes, m);
        }
    }
    jz_btree_drop(&states);
    jz_btree_drop(&payload_states);
    jz_btree_drop(&fact_states);
    *out = transitions;
    *has = true;
    return JZ_OK;
fail:
    jz_vec_drop(&terminal_operations, sizeof(groove_TerminalOperation), groove_TerminalOperation_drop);
    jz_btree_drop(&states);
    jz_btree_drop(&payload_states);
    jz_btree_drop(&fact_states);
    return e;
}

static jz_err MV(NodeState__apply_local_maintained_view_transitions)(jazz_node_NodeState *self, Local *local, jazz_node_ResultTransitions transitions, jazz_node_LocalMaintainedViewSubscriptionUpdate *out)
{
    return MV(NodeState__apply_local_maintained_view_transitions_inner)(self, local, transitions, true, out);
}

static const jz_btree_desc ROW_UUID_SET = {sizeof(jazz_types_RowUuid), 0, jz_cmp_uuid, NULL, NULL};

jz_err MV(NodeState__apply_local_maintained_view_transitions_inner)(jazz_node_NodeState *self, Local *local, jazz_node_ResultTransitions transitions, bool materialize_update, jazz_node_LocalMaintainedViewSubscriptionUpdate *out)
{
    /* The compiler, not the query's surface syntax, selects the output owner.
     * Every public root collector (flat or nested) publishes via its own
     * terminal state and positional operations. */
    bool structured_output = jazz_node__node__MaintainedTerminalSchemas__has_root_collector(local->terminal_schemas);
    const jz_str *rt = &local->result_table;
    bool agg = local->result_query.aggregate != NULL;
    jz_err e = JZ_OK;
    groove_TerminalOperationVec terminal_operations;
    jz_vec_clone(&terminal_operations, &transitions.terminal_operations, sizeof(groove_TerminalOperation), (void (*)(void *, const void *))groove_TerminalOperation_clone);
    jz_btree aggregate_replacements;
    jz_btree_init(&aggregate_replacements, &ROW_UUID_SET);
    JZ_VEC_FOR(jazz_node_ResultMemberEntry, member, transitions.adds) {
        if (!jazz_node__node__query_eval__is_public_aggregate_result_member(member, rt, agg)) continue;
        jazz_types_RowUuid u;
        e = jazz_node__node__query_eval__aggregate_result_member_row_uuid(member, &u);
        if (JZ_IS_ERR(e)) goto fail;
        jz_btree_insert(&aggregate_replacements, &u, NULL, NULL);
    }
    JZ_VEC(jazz_node_OccurrenceRow) added = {0};
    jazz_types_OutputOccurrenceIdVec removed = {0};
    JZ_VEC_FOR(jazz_node_ResultMemberEntry, member, transitions.result_payload_removes) {
        jazz_node_ResultMemberEntry k;
        jazz_protocol_ResultMemberPayloadEntry v;
        if (jz_btree_remove(&local->result_payloads, member, &k, &v)) { jazz_protocol_ResultMemberEntry_drop(&k); jazz_protocol_ResultMemberPayloadEntry_drop(&v); }
    }
    JZ_VEC_FOR(jazz_protocol_MemberPayloadPair, pair, transitions.result_payload_adds) {
        if (!jazz_node__node__query_eval__is_public_result_member(&pair->member, rt, agg)) continue;
        jazz_node_ResultMemberEntry k;
        jazz_protocol_ResultMemberPayloadEntry v;
        if (jz_btree_remove(&local->result_payloads, &pair->member, &k, &v)) { jazz_protocol_ResultMemberEntry_drop(&k); jazz_protocol_ResultMemberPayloadEntry_drop(&v); }
        jz_btree_insert(&local->result_payloads, &pair->member, &pair->payload, NULL);
        memset(pair, 0, sizeof *pair);
        pair->member.tag = UINT32_MAX; /* moved */
    }
    JZ_VEC_FOR(jazz_node_ResultMemberEntry, member, transitions.adds) {
        if (!jazz_node__node__query_eval__is_public_result_member(member, rt, agg)) continue;
        /* Authority-scope re-entry can surface a new internal result member
         * for an occurrence the public facade already tracks. In that case
         * replace the stale member so the current scope wins. Ordinary
         * content updates keep their member identity: replacing them here
         * turns an update into a duplicate add and leaves the public
         * subscription with the old payload. */
        e = jazz_node__node__query_eval__replace_stale_authoritative_occurrence_member(
            &local->result_set, &local->result_payloads, &transitions.authoritative_member_adds, member, rt, agg);
        if (JZ_IS_ERR(e)) goto fail_lists;
        jazz_node_ResultMemberEntry c;
        jazz_protocol_ResultMemberEntry_clone(&c, member);
        bool inserted = !jz_btree_contains(&local->result_set, &c);
        if (inserted) jz_btree_insert(&local->result_set, &c, NULL, NULL);
        else jazz_protocol_ResultMemberEntry_drop(&c);
        if (inserted && materialize_update && !structured_output) {
            bool has_row;
            jazz_node_CurrentRow row;
            e = MAT(NodeState__materialize_local_maintained_view_result_member)(self, local, member, &has_row, &row);
            if (JZ_IS_ERR(e)) goto fail_lists;
            if (!has_row) continue;
            bool has_occ;
            jazz_types_OutputOccurrenceId occurrence_id;
            e = jazz_node__node__query_eval__public_result_member_occurrence_id(member, rt, agg, &has_occ, &occurrence_id);
            if (JZ_IS_ERR(e)) { jazz_node_CurrentRow_drop(&row); goto fail_lists; }
            if (!has_occ) { jazz_node_CurrentRow_drop(&row); continue; }
            if (jazz_types__debug_env__covered_input_trace()) {
                const jz_str *t = jazz_node__node__CurrentRow__table(&row);
                jz_str line = jz_format("JAZZ_COVERED_INPUT_TRACE stage=local_maintained_added table=%.*s occurrence=", (int)t->len, t->ptr);
                jazz_types_OutputOccurrenceId_debug(&line, &occurrence_id);
                trace_line(&line);
            }
            jazz_node_OccurrenceRow pr = {occurrence_id, row};
            JZ_VEC_PUSH(added, pr);
        }
    }
    JZ_VEC_FOR(jazz_node_ResultMemberEntry, member, transitions.removes) {
        if (!jazz_node__node__query_eval__is_public_result_member(member, rt, agg)) continue;
        jazz_node_ResultMemberEntry k;
        if (!jz_btree_remove(&local->result_set, member, &k, NULL)) continue;
        jazz_protocol_ResultMemberEntry_drop(&k);
        if (!(materialize_update && !structured_output)) continue;
        jazz_types_OutputOccurrenceId occurrence_id;
        if (jazz_protocol__protocol__ResultMemberEntry__output_occurrence_id(member, &occurrence_id)) {
            JZ_VEC_PUSH(removed, occurrence_id);
        } else if (jazz_node__node__query_eval__is_public_aggregate_result_member(member, rt, agg)) {
            jazz_types_RowUuid row_uuid;
            e = jazz_node__node__query_eval__aggregate_result_member_row_uuid(member, &row_uuid);
            if (JZ_IS_ERR(e)) goto fail_lists;
            bool replacement_is_current = false;
            const jazz_node_ResultMemberEntry *candidate;
            const void *cv;
            JZ_BTREE_FOR(&local->result_set, candidate, cv) {
                (void)cv;
                if (!jazz_node__node__query_eval__is_public_aggregate_result_member(candidate, rt, agg)) continue;
                jazz_types_RowUuid cu;
                jz_err ce = jazz_node__node__query_eval__aggregate_result_member_row_uuid(candidate, &cu);
                if (JZ_IS_ERR(ce)) { jz_err_drop(&ce); continue; }
                if (jz_uuid_eq(&cu, &row_uuid)) { replacement_is_current = true; break; }
            }
            if (!jz_btree_contains(&aggregate_replacements, &row_uuid) && !replacement_is_current)
                JZ_VEC_PUSH(removed, jazz_types__object__OutputOccurrenceId__single_source(row_uuid));
        }
    }
    JZ_VEC_FOR(jazz_protocol_ProgramFactEntry, fact, transitions.program_fact_removes) {
        jazz_protocol_ProgramFactEntry k;
        if (jz_btree_remove(&local->program_facts, fact, &k, NULL)) jazz_protocol_ProgramFactEntry_drop(&k);
    }
    JZ_VEC_FOR(jazz_protocol_ProgramFactEntry, fact, transitions.program_fact_adds) {
        if (jz_btree_contains(&local->program_facts, fact)) continue;
        jazz_protocol_ProgramFactEntry c;
        jazz_protocol_ProgramFactEntry_clone(&c, fact);
        jz_btree_insert(&local->program_facts, &c, NULL, NULL);
    }
    memset(out, 0, sizeof *out);
    if (structured_output) {
        out->tag = JZN_LMVSU_STRUCTURED;
        out->terminal_operations = terminal_operations;
        memset(&terminal_operations, 0, sizeof terminal_operations);
    } else if (materialize_update && (agg || MV(LocalMaintainedViewSubscription__needs_ordered_relation_snapshot)(local))) {
        jazz_node_LocalMaintainedRelationSnapshot materialized;
        e = MAT(NodeState__materialize_local_maintained_relation_snapshot_with_occurrences)(self, local, &materialized);
        if (JZ_IS_ERR(e)) goto fail_lists;
        jz_vec_drop(&local->root_occurrence_ids, sizeof(jazz_types_OutputOccurrenceId), jazz_types_OutputOccurrenceId_drop);
        jz_vec_clone(&local->root_occurrence_ids, &materialized.root_occurrence_ids, sizeof(jazz_types_OutputOccurrenceId), (void (*)(void *, const void *))jazz_types_OutputOccurrenceId_clone);
        out->tag = JZN_LMVSU_ORDERED_WINDOW;
        out->snapshot = materialized.snapshot;
        out->occurrence_ids = materialized.root_occurrence_ids;
    } else {
        if (terminal_operations.len != 0) {
            e = JZN_INVALID_STORED("non-terminal app-row path emitted collector terminal operations");
            goto fail_lists;
        }
        out->tag = JZN_LMVSU_FLAT;
        memcpy(&out->added, &added, sizeof added);
        memcpy(&out->removed, &removed, sizeof removed);
        memset(&added, 0, sizeof added);
        memset(&removed, 0, sizeof removed);
    }
fail_lists:
    for (u32 i = 0; i < added.len; i++) {
        jazz_types_OutputOccurrenceId_drop(&added.ptr[i].occurrence);
        jazz_node_CurrentRow_drop(&added.ptr[i].row);
    }
    jz_vec_drop(&added, sizeof(jazz_node_OccurrenceRow), NULL);
    jz_vec_drop(&removed, sizeof(jazz_types_OutputOccurrenceId), jazz_types_OutputOccurrenceId_drop);
fail:
    jz_vec_drop(&terminal_operations, sizeof(groove_TerminalOperation), groove_TerminalOperation_drop);
    jz_btree_drop(&aggregate_replacements);
    /* moved payload pairs were zeroed; the owner drop skips them */
    JZ_VEC_FOR(jazz_protocol_MemberPayloadPair, pair, transitions.result_payload_adds) {
        if (pair->member.tag == UINT32_MAX) continue;
        jazz_protocol_ResultMemberEntry_drop(&pair->member);
        jazz_protocol_ResultMemberPayloadEntry_drop(&pair->payload);
    }
    transitions.result_payload_adds.len = 0;
    jazz_node_ResultTransitions_drop(&transitions);
    return e;
}

/* A root collector can change only positions or a projected payload.  Those
 * edits deliberately have no membership, payload-witness, or program-fact
 * delta, but they are still a public receiver transition. */
static bool jazz_node__node__query_eval__maintained_views__drained_transition_is_empty(bool states_empty, bool payload_states_empty, bool fact_states_empty, const groove_TerminalOperationVec *terminal_operations)
{
    return states_empty && payload_states_empty && fact_states_empty && terminal_operations->len == 0;
}
