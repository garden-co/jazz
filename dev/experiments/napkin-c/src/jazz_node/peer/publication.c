/* jazz_node::peer — maintained-view publication for one downstream peer:
 * rehydrate, incremental drain, cloned usage-site resets and the Q-at-W
 * watermark catch-up. Rust: crates/jazz/layers/node/src/peer/publication.rs
 * (include!d into peer.rs). Async fns are synchronous here; Option<T> results
 * are out-pointers with NULL = None; Poll::Pending is `*ready = false`.
 * #[cfg(any(test, feature = "testing"))] helpers are not ported. */
#include "peer.h"

typedef jazz_protocol_ResultMemberEntry RME;
typedef jazz_protocol_ResultMemberEntryVec RMEVec;
typedef jazz_node_PeerSubscriptionState PSS;
typedef jazz_node_MaintainedSubscriptionViewSubscription MSVS;
typedef jazz_protocol_KnownStateDeclaration KSD;
typedef jazz_protocol_SyncMessage SyncMsg;
typedef jazz_protocol_RegisterShapeOptions RSO;

/* What one maintained publication sends to `subscription`. Rehydrate,
 * incremental drain and cloned reset each plan these inputs; one bundler
 * fills in the link-wide rest. */
typedef struct PublicationPlan {
    SubKey subscription;
    jazz_node_PolicyBinding policy;
    jazz_model_DurabilityTier tier;
    bool default_read_view;
    jazz_protocol_SupportingRowsUpdate *supporting_update; /* Option, owned */
    jazz_types_GlobalTime settled_through;
    bool has_known_state; KSD known_state;
    bool has_leave_scan_after; jazz_types_GlobalTime leave_scan_after;
    RMEVec adds, removes;
} PublicationPlan;

/* What a usage-site reset knows about its downstream receiver before the
 * canonical delta is applied. */
typedef struct ClonedResetBasis {
    bool has_known_state; KSD known_state;
    bool has_known_membership_position; jazz_types_GlobalTime known_membership_position;
    bool authorization_matches;
    bool removed_members_are_ordinary;
    bool source_had_program_fact_transitions;
    bool has_flat_row_removes; RMEVec flat_row_removes;
} ClonedResetBasis;

typedef struct MaintainedCanonicalUpdate { bool changed; SyncMsg *update; } MaintainedCanonicalUpdate;

static void rme_vec_drop(RMEVec *v) { jz_vec_drop(v, sizeof(RME), jazz_protocol__protocol__ResultMemberEntry__drop); memset(v, 0, sizeof *v); }
static void rme_push_clone(RMEVec *v, const RME *m) { RME c; jazz_protocol__protocol__ResultMemberEntry__clone(&c, m); JZ_VEC_PUSH(*v, c); }
static void rme_set_insert_clone(jz_btree *set, const RME *m)
{
    RME c;
    jazz_protocol__protocol__ResultMemberEntry__clone(&c, m);
    if (jz_btree_insert(set, &c, NULL, NULL)) jazz_protocol__protocol__ResultMemberEntry__drop(&c);
}
static void plan_drop(PublicationPlan *plan)
{
    JZP(PolicyBinding__drop)(&plan->policy);
    if (plan->supporting_update) { jazz_protocol__protocol__SupportingRowsUpdate__drop(plan->supporting_update); jz_free(plan->supporting_update); }
    rme_vec_drop(&plan->adds);
    rme_vec_drop(&plan->removes);
}
static void basis_drop(ClonedResetBasis *b) { if (b->has_flat_row_removes) rme_vec_drop(&b->flat_row_removes); }

static jazz_protocol_ReadViewKey default_read_view_key(void)
{
    RSO opts;
    jazz_protocol__protocol__RegisterShapeOptions__default(&opts);
    jazz_protocol_ReadViewKey key = jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&opts);
    jazz_protocol__protocol__RegisterShapeOptions__drop(&opts);
    return key;
}

static bool read_view_key_eq(jazz_protocol_ReadViewKey a, jazz_protocol_ReadViewKey b) { return jz_uuid_eq(&a.id, &b.id); }

static jz_str subkey_debug(SubKey k) { jz_str s = {0}; jazz_protocol__protocol__SubscriptionKey__debug(&s, &k); return s; }

static void trace_line(jz_str line) { jazz_node__debug_env__eprintln(&line); jz_str_drop(&line); }

/* ViewUpdate { subscription, settled_through, [], default inventory, rows } */
static SyncMsg *view_update_without_payload(SubKey sub, jazz_types_GlobalTime settled_through, jazz_protocol_SupportingRowsUpdate rows)
{
    jazz_protocol_ViewUpdatePayload payload;
    memset(&payload, 0, sizeof payload);
    payload.subscription = sub;
    payload.settled_through = settled_through;
    payload.supporting_rows = rows;
    return jazz_protocol__protocol__SyncMessage__ViewUpdate(payload);
}

static jazz_protocol_SupportingRowsUpdate empty_snapshot(void)
{
    jazz_protocol_SupportingRowsUpdate u;
    jazz_protocol_SupportingRowVec rows = {0};
    jazz_protocol__protocol__SupportingRowsUpdate__snapshot(&u, rows);
    return u;
}

static void supporting_row_refs_cloned(const jzn_SupportingRowRefVec *refs, jazz_protocol_SupportingRowVec *out)
{
    memset(out, 0, sizeof *out);
    for (u32 i = 0; i < refs->len; i++) {
        jazz_protocol_SupportingRow c;
        jazz_protocol__protocol__SupportingRow__clone(&c, refs->ptr[i]);
        JZ_VEC_PUSH(*out, c);
    }
}

/* ---------------- free helpers ---------------- */

static bool ordinary_current_content_member(const RME *member)
{
    if (member->tag != JAZZ_RME_ROW) return false;
    const jazz_protocol_RealRowMemberEntry *row = jazz_protocol__protocol__ResultMemberEntry__as_real_row(member);
    return row && jazz_protocol__protocol__RealRowMemberEntry__layer(row) == JAZZ_LAYER_CONTENT
        && !jazz_protocol__protocol__RealRowMemberEntry__has_deletion_tx(row)
        && jazz_protocol__protocol__RealRowMemberEntry__source(row) == JAZZ_RRS_CURRENT
        && !jazz_protocol__protocol__RealRowMemberEntry__has_branch_or_prefix(row)
        && !jazz_protocol__protocol__RealRowMemberEntry__has_batch(row);
}

static bool ordinary_flat_row_duplicate_view(const jazz_model_ValidatedQuery *shape, const jz_btree *current_members, bool removed_members_are_ordinary,
                                             jazz_protocol_ReadViewKey canonical_read_view, bool canonical_program_facts_empty,
                                             bool source_had_program_fact_transitions)
{
    const jazz_model_Query *query = jazz_model__query__ValidatedQuery__query(shape);
    if (!(query->flat_join == NULL && query->includes.len == 0 && query->array_subqueries.len == 0 && query->aggregate == NULL
          && read_view_key_eq(canonical_read_view, default_read_view_key()) && canonical_program_facts_empty
          && !source_had_program_fact_transitions && removed_members_are_ordinary))
        return false;
    RME *m; void *unused;
    JZ_BTREE_FOR(current_members, m, unused) {
        if (!ordinary_current_content_member(m)) return false;
    }
    return true;
}

/* Whether a maintained publication exposes `member`: rows of its output
 * tables (narrowed to `result_table`), and synthetic aggregate members unless
 * the aggregate is policy scoped. */
static bool published_member(const RME *member, const jz_str *result_table, const jz_btree *output_tables, bool aggregate_is_policy_scoped)
{
    const jz_str *table_name = jazz_protocol__protocol__ResultMemberEntry__table_name(member);
    if (!table_name) return false;
    bool synthetic = member->tag == JAZZ_RME_SYNTHETIC;
    return (synthetic || !result_table || jz_str_eq(table_name, result_table))
        && (jz_btree_contains(output_tables, table_name) || (synthetic && !aggregate_is_policy_scoped));
}

static jz_err aggregate_is_policy_scoped(const NS *node, const jazz_model_ValidatedQuery *shape, bool *out)
{
    const jazz_model_Query *query = jazz_model__query__ValidatedQuery__query(shape);
    if (!query->aggregate) { *out = false; return JZ_OK; }
    const jazz_model_TableSchema *table;
    JZ_TRY(jazz_node__node__NodeState__table(node, &query->table, &table));
    *out = table->read_policy != NULL;
    return JZ_OK;
}

static usize traced_bundle_count(SyncMsg *update)
{
    jazz_protocol_ViewUpdatePayload *view = jazz_protocol__protocol__SyncMessage__as_view_update(update);
    if (!view) return 0;
    jazz_protocol_VersionBundleVec bundles;
    JZP(delivery__view_update_singleton_bundles)(view->version_carriers.ptr, view->version_carriers.len, &bundles);
    usize n = bundles.len;
    jz_vec_drop(&bundles, sizeof(bundles.ptr[0]), jazz_protocol__protocol__VersionBundle__drop);
    return n;
}

/* Reconcile a retained downstream result set against a cold maintained-view
 * snapshot that contains a static deletion witness. The active membership is
 * authoritative for removals. Publishability only gates members the
 * downstream has not already received, because Stream A can arrive before
 * Stream B's content witness during cold hydration. */
static void reconcile_one(jz_btree *states, const RME *member, const jz_btree *previous, const jz_btree *active, const jz_btree *published)
{
    bool was_published = jz_btree_contains(previous, member);
    bool is_published = jz_btree_contains(active, member) && (was_published || jz_btree_contains(published, member));
    bool val[2] = { was_published, is_published };
    RME c;
    jazz_protocol__protocol__ResultMemberEntry__clone(&c, member);
    bool old[2];
    if (jz_btree_insert(states, &c, val, old)) jazz_protocol__protocol__ResultMemberEntry__drop(&c);
}

static void reconcile_retained_members_after_initial_deletion_witness(jz_btree *states, const jz_btree *previous_members,
                                                                      const jz_btree *active_members, const jz_btree *published_members)
{
    /* previous_members.union(active_members) */
    RME *m; void *unused;
    JZ_BTREE_FOR(previous_members, m, unused) reconcile_one(states, m, previous_members, active_members, published_members);
    JZ_BTREE_FOR(active_members, m, unused) {
        if (jz_btree_contains(previous_members, m)) continue;
        reconcile_one(states, m, previous_members, active_members, published_members);
    }
}

/* ---------------- PeerState: lookups ---------------- */

const MSVS *JZP(PeerState__maintained_view)(const PS *p, SubKey sub)
{
    PSS *state = JZP(PeerState__state)(p, sub);
    return state ? state->maintained_subscription_view : NULL;
}

/* The tier and read view the subscription was prepared with. */
static jz_err prepared_context(const PS *p, SubKey sub, const char *missing, jazz_model_DurabilityTier *tier, jazz_protocol_ReadViewSpec **read_view)
{
    PSS *state = JZP(PeerState__state)(p, sub);
    if (!state || !state->prepared_query) return JZN_INVALID_STORED(missing);
    JZP(CachedPeerQueryPlan__context)(state->prepared_query, tier, read_view);
    return JZ_OK;
}

/* The membership last shipped to the subscription (empty if none). */
static void member_result_set(const PS *p, SubKey sub, jz_btree *out)
{
    PSS *state = JZP(PeerState__state)(p, sub);
    if (state) JZP(PeerSubscriptionState__member_result_set)(state, out);
    else jz_btree_init(out, &jzp_desc_member_set);
}

bool JZP(PeerState__has_maintained_subscription)(const PS *p, SubKey sub) { return JZP(PeerState__maintained_view)(p, sub) != NULL; }

bool JZP(PeerState__has_pending_initial_maintained_subscription)(const PS *p, SubKey sub)
{
    const MSVS *m = JZP(PeerState__maintained_view)(p, sub);
    return m && !m->initial_received;
}

static bool has_current_pending_initial_maintained_subscription(const PS *p, const NS *node, SubKey sub)
{
    PSS *state = JZP(PeerState__state)(p, sub);
    return state && state->has_groove_runtime_token && state->groove_runtime_token == jazz_node__node__NodeState__groove_runtime_token(node)
        && state->has_physical_identity_generation && state->physical_identity_generation == jazz_node__node__NodeState__physical_identity_generation(node)
        && state->maintained_subscription_view && !state->maintained_subscription_view->initial_received;
}

static bool fast_cursor_authorization_matches(const PS *p, SubKey sub, const KSD *known_state)
{
    switch (p->role.tag) {
    case JZN_PEER_ROLE_RELAY: return true;
    case JZN_PEER_ROLE_CLIENT_LINK: {
        PSS *state = JZP(PeerState__state)(p, sub);
        u64 progress;
        return state && state->has_served_authorization_progress
            && JZP(subscription_state__fast_authorization_progress)(known_state, &progress) && progress == state->authorization_progress;
    }
    default: JZ_UNREACHABLE();
    }
}

/* Read the settlement watermark from the exact receipt retained by the served
 * subscription. Never route this through a BindingViewKey: two policy scopes
 * may share that key while having different watermarks. A missing exact
 * source is only valid for a genuinely direct, unscoped publication. */
static jazz_types_GlobalTime settlement_time_for_publication(const PS *p, const NS *node, SubKey sub, jazz_protocol_BindingViewKey binding_view)
{
    PSS *state = JZP(PeerState__state)(p, sub);
    jazz_types_GlobalTime t;
    if (state && state->has_authority_result_source) {
        if (jazz_node__node__query_eval__subscriptions__NodeState__settled_through_for_authority_result(node, &state->authority_result_source, &t)) return t;
        return 0;
    }
    jazz_protocol_AuthorityResultKey unscoped;
    jazz_protocol__protocol__AuthorityResultKey__unscoped(binding_view, &unscoped);
    bool some = jazz_node__node__query_eval__subscriptions__NodeState__settled_through_for_authority_result(node, &unscoped, &t);
    jazz_protocol__protocol__AuthorityResultKey__drop(&unscoped);
    return some ? t : jazz_node__node__NodeState__committed_global_time(node);
}

bool JZP(PeerState__subscription_awaits_selected_authority_source)(const PS *p, SubKey sub)
{
    PSS *state = JZP(PeerState__state)(p, sub);
    return state && state->awaiting_selected_authority_source;
}

jazz_types_GlobalTime JZP(PeerState__canonical_subscription_settlement_time)(const PS *p, const NS *node, SubKey sub)
{
    return settlement_time_for_publication(p, node, sub, jazz_protocol__protocol__BindingViewKey__from_canonical_subscription_key(sub));
}

static jazz_types_GlobalTime maintained_publication_cut(const PS *p, const NS *node, SubKey sub)
{
    /* An authority computes a closure from its own maintained state. A strict
     * client relay instead consumes one selected authority receipt; its local
     * committed clock cannot certify that receipt's inputs. */
    if (JZP(PeerState__subscription_awaits_selected_authority_source)(p, sub))
        return JZP(PeerState__canonical_subscription_settlement_time)(p, node, sub);
    return jazz_node__node__NodeState__committed_global_time(node);
}

/* Bundle `plan` under its served policy binding from the facts of the
 * canonical maintained view `source`. Consumes the plan. */
static jz_err bundle_publication(const PS *p, NS *node, const jazz_model_ValidatedQuery *shape, SubKey source,
                                 const jazz_node_MaintainedSubscriptionView *maintained_facts, PublicationPlan *plan, SyncMsg **out)
{
    jazz_node_MaintainedViewBundleInputs inputs;
    memset(&inputs, 0, sizeof inputs);
    inputs.supporting_update = plan->supporting_update;
    plan->supporting_update = NULL;
    inputs.shape = shape;
    inputs.has_default_read_view = plan->default_read_view;
    inputs.allow_authoritative_scalar_exit_refresh = !JZP(PeerState__subscription_awaits_selected_authority_source)(p, source);
    inputs.subscription = plan->subscription;
    inputs.settled_through = plan->settled_through;
    JZP(PeerState__acknowledged_complete_tx_payloads)(p, &inputs.peer_complete_tx_payloads);
    inputs.known_state = plan->has_known_state ? &plan->known_state : NULL;
    inputs.has_leave_scan_after = plan->has_leave_scan_after;
    inputs.leave_scan_after = plan->leave_scan_after;
    inputs.complete_exclusive_payloads = p->ship_complete_exclusive_payloads && p->role.tag == JZN_PEER_ROLE_RELAY;
    jz_btree_init(&inputs.previous_result_set, &jzn_desc_txid_set);
    inputs.result_member_adds = plan->adds;
    inputs.result_member_removes = plan->removes;
    memset(&plan->adds, 0, sizeof plan->adds);
    memset(&plan->removes, 0, sizeof plan->removes);
    inputs.identity = plan->policy.identity;
    inputs.tier = plan->tier;
    inputs.maintained_facts = maintained_facts;
    inputs.allow_storage_witness_fallback = false;
    NS *scoped = jazz_node__node__NodeState__scoped_active_session_claims(node, plan->policy.identity, plan->policy.claims);
    jz_btree_init(&plan->policy.claims, plan->policy.claims.d); /* moved into the scope */
    jz_err e = jazz_node__node__views__NodeState__view_update_for_maintained_result_members(scoped, inputs, out);
    jazz_node__node__NodeState__end_scoped_active_session_claims(scoped);
    plan_drop(plan);
    return e;
}

/* ---------------- catalogue snapshot announcement ---------------- */
void JZP(PeerState__reset_catalogue_snapshot_announcement)(PS *p) { p->has_announced_catalogue_fingerprint = false; }
bool JZP(PeerState__has_announced_catalogue_snapshot)(const PS *p) { return p->has_announced_catalogue_fingerprint; }
bool JZP(PeerState__needs_catalogue_snapshot)(const PS *p, const u8 fingerprint[32])
{
    return !(p->has_announced_catalogue_fingerprint && memcmp(p->announced_catalogue_fingerprint, fingerprint, 32) == 0);
}
void JZP(PeerState__mark_catalogue_snapshot_announced)(PS *p, const u8 fingerprint[32])
{
    p->has_announced_catalogue_fingerprint = true;
    memcpy(p->announced_catalogue_fingerprint, fingerprint, 32);
}

/* ---------------- constructors, roles and identities ---------------- */

/* Construct a standalone SYSTEM-scoped peer. Network relays must use
 * PeerState::relay so a missing admitted policy binding fails closed. */
void JZP(PeerState__new)(PS *out) { JZP(PeerState__default)(out); }

/* Construct a permanent relay peer. */
void JZP(PeerState__relay)(PS *out)
{
    JZP(PeerState__default)(out);
    out->role.tag = JZN_PEER_ROLE_RELAY;
    out->transport_capability.tag = JZN_RTC_MULTIPLEXED_RELAY;
}

/* Construct a subjectless scope-isolated relay with its one
 * handshake-admitted foreground session. Topology-private. */
void JZP(PeerState__scope_isolated_relay)(PS *out, jazz_types_AuthorSubject identity, jz_btree claims, u64 admission_epoch)
{
    JZP(PeerState__default)(out);
    out->role.tag = JZN_PEER_ROLE_RELAY;
    out->transport_capability.tag = JZN_RTC_SCOPE_ISOLATED_CLIENT_RELAY;
    out->transport_capability.binding.identity = identity;
    out->transport_capability.binding.claims = claims;
    out->transport_capability.admission_epoch = admission_epoch;
}

bool JZP(PeerState__admits_relay_binding)(const PS *p, const jazz_node_PolicyBinding *binding)
{
    if (p->transport_capability.tag != JZN_RTC_SCOPE_ISOLATED_CLIENT_RELAY) return false;
    const jazz_protocol_DelegatedSessionBinding *admitted = &p->transport_capability.binding;
    return jazz_types_AuthorSubject_eq(&admitted->identity, &binding->identity)
        && jz_btree_eq(&admitted->claims, &binding->claims, groove__records__values__Value__eq_v);
}

/* The one immutable user binding selected by server-side scope-relay
 * admission. Raw SessionClaims frames must not replace it mid-connection. */
const jazz_protocol_DelegatedSessionBinding *JZP(PeerState__admitted_scope_relay_binding)(const PS *p)
{
    switch (p->transport_capability.tag) {
    case JZN_RTC_SCOPE_ISOLATED_CLIENT_RELAY: return &p->transport_capability.binding;
    case JZN_RTC_ORDINARY_SESSION:
    case JZN_RTC_MULTIPLEXED_RELAY: return NULL;
    default: JZ_UNREACHABLE();
    }
}

/* #[cfg(feature = "runtime")]: replace the per-attachment capability epoch
 * after the server has detached and resumed this scope-isolated relay. */
bool JZP(PeerState__refresh_scope_relay_admission_epoch)(PS *p)
{
    if (p->transport_capability.tag != JZN_RTC_SCOPE_ISOLATED_CLIENT_RELAY) return false;
    if (p->transport_capability.admission_epoch == UINT64_MAX) jz_panic("scope-isolated relay admission epoch exhausted");
    p->transport_capability.admission_epoch += 1;
    return true;
}

bool JZP(PeerState__rejects_raw_session_claims)(const PS *p) { return p->transport_capability.tag == JZN_RTC_SCOPE_ISOLATED_CLIENT_RELAY; }

/* Construct a peer link that terminates one client author identity. */
void JZP(PeerState__client_link)(PS *out, jazz_types_AuthorSubject identity)
{
    JZP(PeerState__default)(out);
    out->role.tag = JZN_PEER_ROLE_CLIENT_LINK;
    out->role.identity = identity;
}

/* Construct a client link whose wire identity and read-policy identity differ. */
void JZP(PeerState__client_link_with_permission_identity)(PS *out, jazz_types_AuthorSubject identity, jazz_types_AuthorSubject permission_identity)
{
    JZP(PeerState__client_link)(out, identity);
    out->has_permission_identity = true;
    out->permission_identity = permission_identity;
}

jazz_node_PeerRole JZP(PeerState__role)(const PS *p) { return p->role; }

/* The principal terminated by this peer link, if it terminates one. */
bool JZP(PeerState__link_identity)(const PS *p, jazz_types_AuthorSubject *out)
{
    return JZP(subscription_state__PeerRole__permission_subject)(p->role, out);
}

/* Compatibility accessor for direct client links. */
jazz_types_AuthorSubject JZP(PeerState__identity)(const PS *p)
{
    jazz_types_AuthorSubject a;
    if (!JZP(PeerState__link_identity)(p, &a)) jz_panic("relay transport has no identity; use an admitted policy binding");
    return a;
}

/* The principal that may be used for policy composition. */
bool JZP(PeerState__permission_subject)(const PS *p, jazz_types_AuthorSubject *out)
{
    if (p->has_permission_identity) { *out = p->permission_identity; return true; }
    return JZP(subscription_state__PeerRole__permission_subject)(p->role, out);
}

/* ---------------- served policy binding / authority source ---------------- */

/* Bind the authorization snapshot selected at subscriber admission to one
 * usage site; a trusted relay may multiplex distinct sessions on one
 * transport. Consumes `claims`. */
void JZP(PeerState__set_subscription_policy_binding)(PS *p, SubKey sub, jazz_types_AuthorSubject identity, jz_btree claims)
{
    if (jazz_node__debug_env__covered_input_trace()) {
        jz_str k = subkey_debug(sub);
        trace_line(jz_format("JAZZ_COVERED_INPUT_TRACE stage=served_policy_binding peer=%p owner=%llu role=%u subscription=%.*s identity=%u claims=%u",
                             (void *)p, (unsigned long long)p->publication_owner, p->role.tag, (int)k.len, k.ptr, identity.tag, claims.len));
        jz_str_drop(&k);
    }
    PSS *state = JZP(PeerState__state_entry)(p, sub);
    if (state->has_policy_binding) JZP(PolicyBinding__drop)(&state->policy_binding);
    state->has_policy_binding = true;
    state->policy_binding.identity = identity;
    state->policy_binding.claims = claims;
}

/* Associate a relay-owned maintained receiver with the precise upstream
 * authority receipt that supplies its membership. */
void JZP(PeerState__set_subscription_authority_result_source)(PS *p, SubKey sub, jazz_protocol_AuthorityResultKey authority_result_key)
{
    PSS *state = JZP(PeerState__state_entry)(p, sub);
    jazz_protocol_AuthorityResultKey for_reconciliation;
    jazz_protocol__protocol__AuthorityResultKey__clone(&for_reconciliation, &authority_result_key);
    jazz_node__node__query_eval__local_authority_reconciliation__LocalAuthorityReconciliation__replace_source(state->local_authority, for_reconciliation, 0);
    if (state->has_authority_result_source) jazz_protocol__protocol__AuthorityResultKey__drop(&state->authority_result_source);
    state->has_authority_result_source = true;
    state->authority_result_source = authority_result_key;
}

/* Mark a scope-relay served usage as pending the exact U source installed above. */
void JZP(PeerState__set_subscription_awaiting_selected_authority_source)(PS *p, SubKey sub, bool awaiting)
{
    JZP(PeerState__state_entry)(p, sub)->awaiting_selected_authority_source = awaiting;
}

/* The exact authority receipt selected for this concrete downstream usage. */
const jazz_protocol_AuthorityResultKey *JZP(PeerState__subscription_authority_result_source)(const PS *p, SubKey sub)
{
    PSS *state = JZP(PeerState__state)(p, sub);
    return state && state->has_authority_result_source ? &state->authority_result_source : NULL;
}

bool JZP(PeerState__subscription_policy_binding)(const PS *p, SubKey sub, jazz_node_PolicyBinding *out)
{
    PSS *state = JZP(PeerState__state)(p, sub);
    if (!state || !state->has_policy_binding) return false;
    JZP(PolicyBinding__clone)(out, &state->policy_binding);
    return true;
}

/* Return the immutable policy snapshot for a served subscription. A relay
 * must never substitute its SYSTEM transport identity when a multiplexed
 * subscriber's binding was lost. */
static jz_err served_subscription_policy_binding(const PS *p, SubKey sub, jazz_node_PolicyBinding *out)
{
    if (JZP(PeerState__subscription_policy_binding)(p, sub, out)) return JZ_OK;
    if (jazz_node__debug_env__covered_input_trace()) {
        jz_str k = subkey_debug(sub);
        trace_line(jz_format("JAZZ_COVERED_INPUT_TRACE stage=missing_served_policy_binding peer=%p owner=%llu role=%u subscription=%.*s states=%u",
                             (void *)p, (unsigned long long)p->publication_owner, p->role.tag, (int)k.len, k.ptr, p->publication_states.len));
        jz_str_drop(&k);
    }
    return JZN_INVALID_STORED("served subscription is missing its immutable policy binding");
}

/* Standalone no-waker helpers serve one peer identity directly; this explicit
 * identity fallback is sound only there. Owner-loop paths fail closed. */
static jz_err ensure_direct_internal_subscription_policy_binding(PS *p, NS *node, SubKey sub)
{
    PSS *state = JZP(PeerState__state)(p, sub);
    if (state && state->has_policy_binding) return JZ_OK;
    if (p->role.tag == JZN_PEER_ROLE_RELAY)
        return JZN_INVALID_STORED("relay subscription requires an explicit immutable policy binding");
    jazz_types_AuthorSubject identity;
    if (!JZP(PeerState__permission_subject)(p, &identity))
        return JZN_INVALID_STORED("direct subscription is missing a terminated permission subject");
    jz_btree claims;
    jazz_node__node__NodeState__session_claims_for(node, identity, &claims);
    JZP(PeerState__set_subscription_policy_binding)(p, sub, identity, claims);
    return JZ_OK;
}

static void clear_stale_groove_runtime_handles(PS *p, NS *node, SubKey sub)
{
    u64 current_token = jazz_node__node__NodeState__groove_runtime_token(node);
    PSS *state = JZP(PeerState__state)(p, sub);
    if (!state || !JZP(PeerSubscriptionState__runtime_is_stale)(state, node)) return;
    /* UUID adoption can invalidate metadata while the underlying runtime
     * remains live. Release that graph now; an old-runtime subscription ID
     * must never address a replacement runtime. */
    if (state->has_groove_runtime_token && state->groove_runtime_token == current_token && state->maintained_subscription_view) {
        MSVS *stale = state->maintained_subscription_view;
        state->maintained_subscription_view = NULL;
        jazz_node__node__NodeState__unsubscribe_groove_subscription(node, groove__ivm__MultisinkSubscription__id(stale->subscription));
        JZP(MaintainedSubscriptionViewSubscription__drop)(stale);
    }
    JZP(PeerSubscriptionState__clear_groove_runtime_handles)(state);
}

static void replace_maintained_subscription_view(PS *p, NS *node, SubKey sub, MSVS *replacement)
{
    u64 runtime_token = jazz_node__node__NodeState__groove_runtime_token(node);
    PSS *state = JZP(PeerState__state_entry)(p, sub);
    bool same_runtime = state->has_groove_runtime_token && state->groove_runtime_token == runtime_token;
    MSVS *stale = state->maintained_subscription_view;
    state->maintained_subscription_view = replacement;
    state->has_groove_runtime_token = true;
    state->groove_runtime_token = runtime_token;
    state->has_physical_identity_generation = true;
    state->physical_identity_generation = jazz_node__node__NodeState__physical_identity_generation(node);
    if (stale && same_runtime)
        jazz_node__node__NodeState__unsubscribe_groove_subscription(node, groove__ivm__MultisinkSubscription__id(stale->subscription));
    JZP(MaintainedSubscriptionViewSubscription__drop)(stale);
}

static bool requires_selected_authority_source(const PS *p, SubKey sub, jazz_node_RehydratePurpose purpose)
{
    return purpose == JZN_REHYDRATE_QUERY && JZP(PeerState__subscription_awaits_selected_authority_source)(p, sub);
}

static bool selected_authority_source(const PS *p, SubKey sub, jazz_protocol_AuthorityResultKey *out)
{
    PSS *state = JZP(PeerState__state)(p, sub);
    if (!state || !state->has_authority_result_source) return false;
    jazz_protocol__protocol__AuthorityResultKey__clone(out, &state->authority_result_source);
    return true;
}

static jz_err ensure_query_subscription_registered(const PS *p, NS *node, SubKey sub, const jazz_model_ValidatedQuery *shape,
                                                   const jazz_model_Binding *binding, const RSO *opts, const jazz_node_PolicyBinding *policy_binding)
{
    jazz_protocol_Subscribe subscribe;
    memset(&subscribe, 0, sizeof subscribe);
    subscribe.shape_id = jazz_model__query__ValidatedQuery__shape_id(shape);
    subscribe.subscription = sub;
    JZP(delivery__binding_values_in_param_order)(shape, binding, &subscribe.values);
    RSO opts_owned;
    jazz_protocol__protocol__RegisterShapeOptions__clone(&opts_owned, opts);
    jz_btree claims;
    JZP(claims_clone)(&claims, &policy_binding->claims);
    return jazz_node__node__query_eval__subscriptions__NodeState__register_query_subscription_for_peer(
        node, p->publication_owner, jazz_model__query__ValidatedQuery__shape_id(shape), jazz_protocol__protocol__ShapeAst__from_validated(shape),
        opts_owned, subscribe, jazz_protocol__protocol__PolicyBindingKey__from_canonical_parts(policy_binding->identity, claims));
}

static void install_prepared_plan(PSS *state, const NS *node, jazz_node_CachedPeerQueryPlan *cached)
{
    JZP(CachedPeerQueryPlan__drop)(state->prepared_query);
    state->prepared_query = cached;
    state->has_groove_runtime_token = true;
    state->groove_runtime_token = jazz_node__node__NodeState__groove_runtime_token(node);
    state->has_physical_identity_generation = true;
    state->physical_identity_generation = jazz_node__node__NodeState__physical_identity_generation(node);
}

/* ---------------- forward declarations ---------------- */
static jz_err rehydrate_query_maintained_subscription_view(PS *p, NS *node, jazz_node_MaintainedRehydrateRequest request, const jz_waker *waker, SyncMsg **out);
static jz_err query_update_maintained_subscription_view_with_metadata(PS *p, NS *node, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    SubKey sub, const jz_str *result_table_filter, const jz_waker *waker, bool *some, MaintainedCanonicalUpdate *out);
static jz_err query_update_inner_for_subscription_with_metadata(PS *p, NS *node, SubKey sub, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, const RSO *opts, const jz_waker *waker, bool *some, MaintainedCanonicalUpdate *out);
static jz_err rehydrate_query_for_subscription_with_purpose(PS *p, NS *node, SubKey sub, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, const RSO *opts, jazz_node_RehydratePurpose purpose, const jz_waker *waker, SyncMsg **out);
static jz_err watermark_catch_up(NS *node, const KSD *known_state, bool default_read_view, bool has_single_table, const jz_str *table,
    jazz_types_GlobalPhysicalTableId physical_table, const jazz_model_Query *query, const jzn_SupportingRowRefVec *rows, const u8 *revision,
    bool authorization_matches, jazz_protocol_SupportingRowsUpdate **out);
static void cloned_reset_basis(const PS *p, SubKey maintained_subscription, SubKey target_subscription, const RME *source_removes, u32 nremoves,
    bool source_had_program_fact_transitions, ClonedResetBasis *out);
static jz_err cloned_subscription_reset(PS *p, NS *node, SubKey maintained_subscription, SubKey target_subscription,
    const jazz_model_ValidatedQuery *shape, ClonedResetBasis basis, SyncMsg **out);

static jz_err query_update_maintained_subscription_view(PS *p, NS *node, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
                                                        SubKey sub, const jz_str *result_table_filter, const jz_waker *waker, SyncMsg **out)
{
    bool some = false;
    MaintainedCanonicalUpdate update = {0};
    *out = NULL;
    JZ_TRY(query_update_maintained_subscription_view_with_metadata(p, node, shape, binding, sub, result_table_filter, waker, &some, &update));
    if (some) *out = update.update;
    return JZ_OK;
}

static SubKey subscription_for(const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_protocol_ReadViewKey read_view)
{
    SubKey k;
    memset(&k, 0, sizeof k);
    k.shape_id = jazz_model__query__ValidatedQuery__shape_id(shape);
    k.binding_id = jazz_model__query__Binding__binding_id(binding);
    k.read_view = read_view;
    return k;
}

/* Builds a full current-row view update, using tx-level refs for complete
 * transaction payloads in this peer's inventory and bundles for new or
 * partial view payload. */
jz_err JZP(PeerState__current_rows_update)(PS *p, NS *node, const jz_str *table, SyncMsg **out)
{
    jazz_model_ValidatedQuery *shape; jazz_model_Binding *binding;
    JZ_TRY(jazz_node__node__views__NodeState__whole_table_shape_binding(node, table, &shape, &binding));
    RSO opts;
    jazz_protocol__protocol__RegisterShapeOptions__default(&opts);
    SubKey sub = subscription_for(shape, binding, jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&opts));
    jazz_protocol_ReadViewSpec *read_view = NULL;
    jz_btree previous; bool has_previous = false;
    jazz_node_PolicyBinding policy_binding; bool has_policy = false;
    jz_err e;
    /* The direct-peer counterpart to rehydrate_query_with_opts: it opens a
     * served maintained view, so it needs the same explicit direct-peer
     * binding before that view can evaluate policy. */
    if (JZ_IS_ERR(e = ensure_direct_internal_subscription_policy_binding(p, node, sub))) goto done;
    clear_stale_groove_runtime_handles(p, node, sub);
    if (JZ_IS_ERR(e = served_subscription_policy_binding(p, sub, &policy_binding))) goto done;
    has_policy = true;
    if (JZ_IS_ERR(e = ensure_query_subscription_registered(p, node, sub, shape, binding, &opts, &policy_binding))) goto done;
    {
        PSS *state = JZP(PeerState__state)(p, sub);
        bool needs_prepare = !state || !state->prepared_query || !JZP(CachedPeerQueryPlan__has_runtime_plan)(state->prepared_query);
        if (needs_prepare) {
            jazz_node_PreparedQueryPlan *plan = jazz_node__node__query_eval__NodeState__mark_peer_maintained_query_shape_cache(node, shape, binding, opts.tier);
            install_prepared_plan(JZP(PeerState__state_entry)(p, sub), node, JZP(CachedPeerQueryPlan__with_plan)(&opts, plan));
        } else {
            JZP(PeerState__state_entry)(p, sub);
        }
    }
    jazz_model_DurabilityTier tier;
    if (JZ_IS_ERR(e = prepared_context(p, sub, "maintained subscription view is missing prepared state", &tier, &read_view))) goto done;
    member_result_set(p, sub, &previous);
    has_previous = true;
    if (!JZP(PeerState__maintained_view)(p, sub)) {
        jazz_node_MaintainedRehydrateRequest request = {
            .shape = shape, .binding = binding, .subscription = sub, .previous_member_result_set = &previous,
            /* The first receiver closure is a complete successor, not a
             * result-member delta. Its reset bit claims the exact
             * ProgramSourceCoverage manifest so a client can atomically
             * install and settle its local source graph. */
            .reset_input_set = true, .result_table_filter = table, .tier = tier, .read_view = read_view, .purpose = JZN_REHYDRATE_QUERY,
        };
        if (JZ_IS_ERR(e = rehydrate_query_maintained_subscription_view(p, node, request, NULL, out))) goto done;
        if (*out) goto done;
        if (JZ_IS_ERR(e = jazz_node__node__NodeState__drive_query_runtime(node))) goto done;
    }
    if (JZP(PeerState__maintained_view)(p, sub)) {
        if (JZ_IS_ERR(e = query_update_maintained_subscription_view(p, node, shape, binding, sub, table, NULL, out))) goto done;
        if (*out) goto done;
        if (JZ_IS_ERR(e = jazz_node__node__NodeState__drive_query_runtime(node))) goto done;
        if (JZ_IS_ERR(e = query_update_maintained_subscription_view(p, node, shape, binding, sub, table, NULL, out))) goto done;
        if (!*out) e = JZN_INVALID_STORED("maintained hydration ended without an initial publication");
        goto done;
    }
    jz_panic("maintained subscription view state is either absent or present");
done:
    if (has_previous) jz_btree_drop(&previous);
    if (has_policy) JZP(PolicyBinding__drop)(&policy_binding);
    if (read_view) jazz_protocol__protocol__ReadViewSpec__drop(read_view);
    jazz_protocol__protocol__RegisterShapeOptions__drop(&opts);
    jazz_model__query__ValidatedQuery__drop(shape);
    jazz_model__query__Binding__drop(binding);
    return e;
}

static jz_err await_query_update_for_subscription_with_opts(PS *p, NS *node, SubKey sub, const jazz_model_ValidatedQuery *shape,
                                                            const jazz_model_Binding *binding, const RSO *opts, SyncMsg **out)
{
    bool some = false;
    MaintainedCanonicalUpdate update = {0};
    JZ_TRY(query_update_inner_for_subscription_with_metadata(p, node, sub, shape, binding, opts, NULL, &some, &update));
    if (some) { *out = update.update; return JZ_OK; }
    JZ_TRY(jazz_node__node__NodeState__drive_query_runtime(node));
    JZ_TRY(query_update_inner_for_subscription_with_metadata(p, node, sub, shape, binding, opts, NULL, &some, &update));
    if (!some) return JZN_INVALID_STORED("maintained hydration ended without a query publication");
    *out = update.update;
    return JZ_OK;
}

/* Build an incremental view update addressed to a usage-site subscription. */
jz_err JZP(PeerState__query_update_for_subscription)(PS *p, NS *node, SubKey sub, const jazz_model_ValidatedQuery *shape,
                                                     const jazz_model_Binding *binding, SyncMsg **out)
{
    RSO opts;
    jazz_protocol__protocol__RegisterShapeOptions__default(&opts);
    jz_err e = await_query_update_for_subscription_with_opts(p, node, sub, shape, binding, &opts, out);
    jazz_protocol__protocol__RegisterShapeOptions__drop(&opts);
    return e;
}

/* Builds a query-binding view update. */
jz_err JZP(PeerState__query_update)(PS *p, NS *node, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, SyncMsg **out)
{
    jazz_protocol_ReadViewKey default_key;
    memset(&default_key, 0, sizeof default_key);
    return JZP(PeerState__query_update_for_subscription)(p, node, subscription_for(shape, binding, default_key), shape, binding, out);
}

jz_err JZP(PeerState__query_update_for_subscription_with_opts_and_waker)(PS *p, NS *node, SubKey sub, const jazz_model_ValidatedQuery *shape,
                                                                         const jazz_model_Binding *binding, const RSO *opts, const jz_waker *waker,
                                                                         bool *ready, SyncMsg **out)
{
    bool some = false;
    MaintainedCanonicalUpdate update = {0};
    *out = NULL;
    JZ_TRY(query_update_inner_for_subscription_with_metadata(p, node, sub, shape, binding, opts, waker, &some, &update));
    *ready = some;
    if (some) {
        if (update.changed) *out = update.update;
        else jazz_protocol__protocol__SyncMessage__drop(update.update);
    }
    return JZ_OK;
}

/* Preserves the read view and tier used when the shape was registered. */
jz_err JZP(PeerState__query_update_for_subscription_with_opts)(PS *p, NS *node, SubKey sub, const jazz_model_ValidatedQuery *shape,
                                                               const jazz_model_Binding *binding, const RSO *opts, SyncMsg **out)
{
    bool ready;
    return JZP(PeerState__query_update_for_subscription_with_opts_and_waker)(p, node, sub, shape, binding, opts, NULL, &ready, out);
}

/* Normal delivery and late-usage cloning must share runtime recovery. A clone
 * can arrive just after catalogue activation invalidates the canonical
 * handle; draining that missing handle alone cannot reopen it. */
static jz_err query_update_inner_for_subscription_with_metadata(PS *p, NS *node, SubKey sub, const jazz_model_ValidatedQuery *shape,
                                                                const jazz_model_Binding *binding, const RSO *opts, const jz_waker *waker,
                                                                bool *some, MaintainedCanonicalUpdate *out)
{
    *some = false;
    /* Replacing the runtime or adopting new physical identities retires the
     * maintained source frontier. Publish a complete successor closure:
     * incremental adds cannot retract facts under the old physical IDs. */
    PSS *state = JZP(PeerState__state)(p, sub);
    bool metadata_was_stale = state && JZP(PeerSubscriptionState__runtime_is_stale)(state, node);
    const MSVS *maintained = JZP(PeerState__maintained_view)(p, sub);
    if (metadata_was_stale && maintained && maintained->initial_received)
        p->metrics.maintained_subscription_view->full_diff_fallbacks.runtime_resets += 1;
    clear_stale_groove_runtime_handles(p, node, sub);
    jazz_node_PolicyBinding policy_binding;
    JZ_TRY(served_subscription_policy_binding(p, sub, &policy_binding));
    jz_err e = ensure_query_subscription_registered(p, node, sub, shape, binding, opts, &policy_binding);
    JZP(PolicyBinding__drop)(&policy_binding);
    if (JZ_IS_ERR(e)) return e;
    state = JZP(PeerState__state)(p, sub);
    if (!state) {
        *some = true;
        out->changed = true;
        out->update = view_update_without_payload(sub, maintained_publication_cut(p, node, sub), empty_snapshot());
        return JZ_OK;
    }
    if (state->maintained_subscription_view)
        return query_update_maintained_subscription_view_with_metadata(p, node, shape, binding, sub, NULL, waker, some, out);
    jz_btree previous;
    member_result_set(p, sub, &previous);
    jazz_model_DurabilityTier tier;
    jazz_protocol_ReadViewSpec *read_view;
    bool has_runtime_plan;
    if (state->prepared_query) {
        JZP(CachedPeerQueryPlan__context)(state->prepared_query, &tier, &read_view);
        has_runtime_plan = JZP(CachedPeerQueryPlan__has_runtime_plan)(state->prepared_query);
    } else {
        tier = opts->tier;
        read_view = jazz_protocol__protocol__ReadViewSpec__clone(opts->read_view);
        has_runtime_plan = false;
    }
    if (!has_runtime_plan) {
        jazz_node_PreparedQueryPlan *plan = jazz_node__node__query_eval__NodeState__mark_peer_maintained_query_shape_cache(node, shape, binding, tier);
        PSS *entry = JZP(PeerState__state_entry)(p, sub);
        if (entry->prepared_query) {
            JZP(CachedPeerQueryPlan__replace_runtime_plan)(entry->prepared_query, plan);
            entry->has_groove_runtime_token = true;
            entry->groove_runtime_token = jazz_node__node__NodeState__groove_runtime_token(node);
            entry->has_physical_identity_generation = true;
            entry->physical_identity_generation = jazz_node__node__NodeState__physical_identity_generation(node);
        } else {
            install_prepared_plan(entry, node, JZP(CachedPeerQueryPlan__with_context)(tier, jazz_protocol__protocol__ReadViewSpec__clone(read_view), plan));
        }
    }
    jazz_node_MaintainedRehydrateRequest request = {
        .shape = shape, .binding = binding, .subscription = sub, .previous_member_result_set = &previous,
        .reset_input_set = metadata_was_stale, .result_table_filter = NULL, .tier = tier, .read_view = read_view, .purpose = JZN_REHYDRATE_QUERY,
    };
    SyncMsg *update = NULL;
    e = rehydrate_query_maintained_subscription_view(p, node, request, waker, &update);
    jz_btree_drop(&previous);
    jazz_protocol__protocol__ReadViewSpec__drop(read_view);
    if (JZ_IS_ERR(e)) return e;
    if (update) {
        *some = true;
        out->changed = true;
        out->update = update;
    }
    return JZ_OK;
}

static void take_program_facts(jazz_protocol_ProgramFactEntryPtrVec *dst, jazz_protocol_ProgramFactEntryPtrVec *src)
{
    /* Groove terminals belong to the local host binding ABI. Peer sync
     * carries the maintained program's covered inputs, never this
     * authority-owned output. */
    for (u32 i = 0; i < src->len; i++) {
        if (jazz_protocol__protocol__ProgramFactEntry__is_peer_source_closure_fact(src->ptr[i])) JZ_VEC_PUSH(*dst, src->ptr[i]);
        else jazz_protocol__protocol__ProgramFactEntry__drop(src->ptr[i]);
    }
    jz_vec_drop(src, sizeof(src->ptr[0]), NULL);
    memset(src, 0, sizeof *src);
}

static void mark_state(jz_btree *states, const jz_btree *previous, RME *member /* moved */, bool after)
{
    bool *existing = jz_btree_get(states, member);
    if (existing) {
        existing[1] = after;
        jazz_protocol__protocol__ResultMemberEntry__drop(member);
        return;
    }
    bool val[2] = { jz_btree_contains(previous, member), after };
    jz_btree_insert(states, member, val, NULL);
}

jz_err JZP(PeerState__drain_maintained_subscription_view_changes)(PS *p, NS *node, const jazz_model_ValidatedQuery *shape, SubKey sub,
                                                                   const jz_str *result_table_filter, const jz_waker *waker, jazz_node_ResultTransitions *out)
{
    jazz_node_ResultTransitions_default(out);
    /* Strict Global relay children own a receiver-local graph. Before
     * draining its terminal, atomically replace every compiled input source
     * from the exact selected authority closure. */
    bool deletion_changes = false;
    PSS *state = JZP(PeerState__state)(p, sub);
    MSVS *view = state ? state->maintained_subscription_view : NULL;
    if (view && view->has_source_authority_result && view->covered_input_receiver) {
        jazz_protocol_AuthorityResultKey source;
        jazz_protocol__protocol__AuthorityResultKey__clone(&source, &view->source_authority_result);
        jazz_node_CoveredInputReceiver *receiver = view->covered_input_receiver;
        view->covered_input_receiver = NULL;
        jazz_types_SchemaVersionId schema_version = view->result_schema_version;
        bool due = jazz_node__node__query_eval__maintained_views__NodeState__covered_input_receiver_reconciliation_due(node, receiver, &source);
        bool replaced = true;
        jz_err e = JZ_OK;
        if (due) e = jazz_node__node__query_eval__maintained_views__NodeState__replace_covered_input_receiver(node, receiver, schema_version, &source, &replaced);
        state = JZP(PeerState__state)(p, sub);
        JZ_ASSERT(state && state->maintained_subscription_view, "maintained receiver state survives replacement");
        state->maintained_subscription_view->covered_input_receiver = receiver;
        if (JZ_IS_ERR(e)) { jazz_protocol__protocol__AuthorityResultKey__drop(&source); return e; }
        if (!replaced) {
            /* Pending is not an empty strict result. Leave the receiver
             * attached and wait for the exact claimed source closure. */
            jazz_protocol__protocol__AuthorityResultKey__drop(&source);
            return JZ_OK;
        }
        if (due) {
            jz_btree witnesses;
            e = jazz_node__node__query_eval__subscriptions__NodeState__selected_deletion_witnesses(node, &source, schema_version, &witnesses);
            if (JZ_IS_ERR(e)) { jazz_protocol__protocol__AuthorityResultKey__drop(&source); return e; }
            state = JZP(PeerState__state)(p, sub);
            if (state && state->maintained_subscription_view)
                deletion_changes = JZN_MSV(MaintainedSubscriptionView__replace_selected_deletion_witnesses)(state->maintained_subscription_view->maintained, witnesses);
            else
                jz_btree_drop(&witnesses);
        }
        jazz_protocol__protocol__AuthorityResultKey__drop(&source);
    }
    JZ_TRY(jazz_node__node__NodeState__drive_ready_query_runtime_with_waker(node, waker));
    const MSVS *maintained_view = JZP(PeerState__maintained_view)(p, sub);
    jz_btree empty_tables; jz_btree_init(&empty_tables, &jzn_desc_str_ptr);
    jz_btree *output_tables = maintained_view ? jz_rc_clone(maintained_view->tables) : NULL;
    const jz_btree *tables_ref = output_tables ? output_tables : &empty_tables;
    /* A Local+Full relay read opens immediately against worker-local
     * knowledge; once the independently propagated exact source is live,
     * reconcile against it (only reported by the trace below). */
    bool has_trace_source = false; jazz_protocol_AuthorityResultKey trace_source;
    if (maintained_view && maintained_view->has_source_authority_result) {
        jazz_protocol__protocol__AuthorityResultKey__clone(&trace_source, &maintained_view->source_authority_result);
        has_trace_source = true;
    } else if (selected_authority_source(p, sub, &trace_source)) {
        has_trace_source = jazz_node__node__query_eval__subscriptions__NodeState__has_settled_authority_result(node, &trace_source);
        if (!has_trace_source) jazz_protocol__protocol__AuthorityResultKey__drop(&trace_source);
    }
    bool policy_scoped;
    jz_err e = aggregate_is_policy_scoped(node, shape, &policy_scoped);
    jz_btree states; jz_btree_init(&states, &jzp_desc_member_states);
    bool supporting_changed = deletion_changes;
    usize observed_result_delta_batches = 0;
    bool requires_authoritative_membership_reconcile = false;
    bool initial_deletion_witness = false;
    if (JZ_IS_ERR(e)) goto fail;
    {
        /* Classify transitions against the membership this link last shipped.
         * Borrow it: it holds every member. */
        state = JZP(PeerState__state)(p, sub);
        if (!state || !state->maintained_subscription_view) goto finish;
        const jz_btree *previous = &state->result_member_set;
        MSVS *m = state->maintained_subscription_view;
        for (;;) {
            groove_MultisinkBatch *deltas = NULL;
            u32 rc = groove__ivm__MultisinkSubscription__try_recv(m->subscription, &deltas);
            if (rc != GROOVE_TRY_RECV_OK) break; /* Empty | Disconnected */
            /* A completed cold hydration delivers its complete snapshot
             * through this same receiver. A static deletion witness in it is
             * not an incremental membership transition and must not
             * recursively reopen this subscription. */
            bool initial_snapshot = !m->initial_received;
            m->initial_received = true;
            p->metrics.maintained_subscription_view->delta_batches_in += 1;
            jazz_node_ResultTransitions t;
            e = JZN_MSV(MaintainedSubscriptionView__apply_multisink_deltas)(m->maintained, deltas, m->terminal_schemas, m->tables, &node->node_aliases, &t);
            if (JZ_IS_ERR(e)) goto fail;
            if (initial_snapshot) {
                initial_deletion_witness |= t.requires_authoritative_membership_reconcile;
            } else {
                supporting_changed |= t.supporting_changed;
                observed_result_delta_batches += t.observed_result_delta_batches;
                requires_authoritative_membership_reconcile |= t.requires_authoritative_membership_reconcile;
            }
            if (jazz_node__debug_env__covered_input_trace() && (t.adds.len || t.program_fact_adds.len)) {
                jz_str k = subkey_debug(sub);
                const jz_str *qt = &jazz_model__query__ValidatedQuery__query(shape)->table;
                trace_line(jz_format("JAZZ_COVERED_INPUT_TRACE stage=publication_filter subscription=%.*s table=%.*s result_filter=%.*s authority=%s members=%u raw_facts=%u",
                                     (int)k.len, k.ptr, (int)qt->len, qt->ptr, result_table_filter ? (int)result_table_filter->len : 4,
                                     result_table_filter ? result_table_filter->ptr : "None", has_trace_source ? "Some" : "None", t.adds.len, t.program_fact_adds.len));
                jz_str_drop(&k);
            }
            take_program_facts(&out->program_fact_adds, &t.program_fact_adds);
            take_program_facts(&out->program_fact_removes, &t.program_fact_removes);
            for (u32 i = 0; i < t.adds.len; i++) mark_state(&states, previous, &t.adds.ptr[i], true);
            for (u32 i = 0; i < t.removes.len; i++) mark_state(&states, previous, &t.removes.ptr[i], false);
            t.adds.len = 0;
            t.removes.len = 0;
            jazz_node_ResultTransitions_drop(&t);
        }
    }
    if (initial_deletion_witness) {
        jz_btree previous;
        member_result_set(p, sub, &previous);
        const MSVS *hydrated = JZP(PeerState__maintained_view)(p, sub);
        if (!hydrated) {
            jz_btree_drop(&previous);
            e = JZN_INVALID_STORED("initial maintained subscription snapshot missing after receive");
            goto fail;
        }
        jz_btree active;
        JZN_MSV(MaintainedSubscriptionView__active_result_members)(hydrated->maintained, &active);
        const jz_btree *published = JZN_MSV(MaintainedSubscriptionView__published_result_members)(hydrated->maintained);
        reconcile_retained_members_after_initial_deletion_witness(&states, &previous, &active, published);
        jz_btree_drop(&active);
        jz_btree_drop(&previous);
    }
    {
        RME *member; bool *ba;
        JZ_BTREE_FOR(&states, member, ba) {
            if (!published_member(member, result_table_filter, tables_ref, policy_scoped)) continue;
            if (!ba[0] && ba[1]) rme_push_clone(&out->adds, member);
            else if (ba[0] && !ba[1]) rme_push_clone(&out->removes, member);
        }
    }
finish:
    out->authoritative_membership_changed = false;
    out->observed_result_delta_batches = observed_result_delta_batches;
    out->requires_authoritative_membership_reconcile = requires_authoritative_membership_reconcile;
    out->supporting_changed = supporting_changed;
    e = JZ_OK;
    goto cleanup;
fail:
    jazz_node_ResultTransitions_drop(out);
    jazz_node_ResultTransitions_default(out);
cleanup:
    jz_btree_drop(&states);
    jz_btree_drop(&empty_tables);
    if (output_tables) jz_rc_release(output_tables);
    if (has_trace_source) jazz_protocol__protocol__AuthorityResultKey__drop(&trace_source);
    return e;
}

static jz_err query_update_maintained_subscription_view_with_metadata(PS *p, NS *node, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
                                                                      SubKey sub, const jz_str *result_table_filter, const jz_waker *waker,
                                                                      bool *some, MaintainedCanonicalUpdate *out)
{
    *some = false;
    bool trace_rehydrate = jazz_node__debug_env__rehydrate_trace();
    u64 trace_start = jz_now_ms();
    if (trace_rehydrate) jazz_node__node__NodeState__reset_storage_read_metrics(node);
    /* The first terminal drain after a cold open is the authority's complete
     * successor closure, even when opening itself had to return before
     * Groove produced that terminal batch. */
    const MSVS *before = JZP(PeerState__maintained_view)(p, sub);
    bool initial_received_before_drain = before && before->initial_received;
    jazz_node_ResultTransitions t;
    JZ_TRY(JZP(PeerState__drain_maintained_subscription_view_changes)(p, node, shape, sub, result_table_filter, waker, &t));
    const MSVS *initial_state = JZP(PeerState__maintained_view)(p, sub);
    if (!initial_state || !initial_state->initial_received) { jazz_node_ResultTransitions_drop(&t); return JZ_OK; }
    bool initial_snapshot_completed = !initial_received_before_drain;
    u64 drain_elapsed = jz_now_ms() - trace_start;
    groove_StorageReadMetrics drain_reads = {0};
    if (trace_rehydrate) drain_reads = jazz_node__node__NodeState__take_storage_read_metrics(node);
    RMEVec adds = t.adds, removes = t.removes;
    memset(&t.adds, 0, sizeof t.adds);
    memset(&t.removes, 0, sizeof t.removes);
    u32 result_add_count = adds.len, result_remove_count = removes.len;
    bool public_result_is_silent = adds.len == 0 && removes.len == 0;
    bool facts_empty = t.program_fact_adds.len == 0 && t.program_fact_removes.len == 0;
    bool reconcile = t.requires_authoritative_membership_reconcile || t.observed_result_delta_batches > 0;
    bool supporting_changed = t.supporting_changed;
    jazz_node_ResultTransitions_drop(&t);
    jz_err e = JZ_OK;
    jazz_protocol_ReadViewSpec *read_view = NULL;
    jazz_model_DurabilityTier tier;
    jazz_protocol_SupportingRowsUpdate supporting_rows; bool has_supporting_rows = false;
    /* A deletion witness can require a one-shot membership reconciliation
     * only when it produced no exact source-closure transition. */
    if (public_result_is_silent && !supporting_changed && facts_empty && reconcile) {
        if (JZ_IS_ERR(e = prepared_context(p, sub, "maintained subscription view is missing prepared state", &tier, &read_view))) goto done;
        p->metrics.maintained_subscription_view->full_diff_fallbacks.membership_reconciliations += 1;
        jz_btree previous;
        member_result_set(p, sub, &previous);
        jazz_node_MaintainedRehydrateRequest request = {
            .shape = shape, .binding = binding, .subscription = sub, .previous_member_result_set = &previous, .reset_input_set = false,
            .result_table_filter = result_table_filter, .tier = tier, .read_view = read_view, .purpose = JZN_REHYDRATE_QUERY,
        };
        SyncMsg *update = NULL;
        e = rehydrate_query_maintained_subscription_view(p, node, request, waker, &update);
        jz_btree_drop(&previous);
        if (!JZ_IS_ERR(e) && update) { *some = true; out->changed = true; out->update = update; }
        goto done;
    }
    {
        PSS *state = JZP(PeerState__state)(p, sub);
        if (state) {
            RMEVec replaced;
            JZP(delivery__replacement_removals)(state, adds.ptr, adds.len, &replaced);
            jz_vec_extend(&removes, replaced.ptr, replaced.len, sizeof(RME));
            jz_vec_drop(&replaced, sizeof(RME), NULL);
        }
        /* collect::<BTreeSet<_>>(): sorted, deduplicated net removals */
        jz_btree set; jz_btree_init(&set, &jzp_desc_member_set);
        for (u32 i = 0; i < removes.len; i++)
            if (jz_btree_insert(&set, &removes.ptr[i], NULL, NULL)) jazz_protocol__protocol__ResultMemberEntry__drop(&removes.ptr[i]);
        removes.len = 0;
        RME m;
        while (jz_btree_pop_first(&set, &m, NULL)) JZ_VEC_PUSH(removes, m);
        jz_btree_drop(&set);
    }
    /* Probe only identities touched since the last successful publication
     * against the predecessor and final maintained closure. */
    {
        PSS *state = JZP(PeerState__state)(p, sub);
        if (!state) { e = JZN_INVALID_STORED("maintained subscription publication state is missing"); goto done; }
        if (!state->maintained_subscription_view) { e = JZN_INVALID_STORED("maintained subscription view is missing source closure state"); goto done; }
        const jazz_node_MaintainedSubscriptionView *maintained = state->maintained_subscription_view->maintained;
        jazz_protocol_SupportingRowVec delta_adds = {0}, delta_removes = {0};
        if (!initial_snapshot_completed && state->has_supporting_revision
            && JZN_MSV(MaintainedSubscriptionView__unpublished_supporting_delta)(maintained, &delta_adds, &delta_removes)) {
            memset(&supporting_rows, 0, sizeof supporting_rows);
            supporting_rows.tag = JAZZ_SRU_DELTA;
            memcpy(supporting_rows.predecessor, state->supporting_revision, 16);
            if (delta_adds.len == 0 && delta_removes.len == 0) {
                memcpy(supporting_rows.revision, state->supporting_revision, 16);
            } else {
                jz_uuid fresh = jz_uuid_v4();
                memcpy(supporting_rows.revision, fresh.b, 16);
            }
            supporting_rows.added = delta_adds;
            supporting_rows.removed = delta_removes;
        } else {
            jzn_SupportingRowRefVec refs = {0};
            JZN_MSV(MaintainedSubscriptionView__supporting_rows)(maintained, &refs);
            jazz_protocol_SupportingRowVec rows;
            supporting_row_refs_cloned(&refs, &rows);
            jz_vec_drop(&refs, sizeof(refs.ptr[0]), NULL);
            jazz_protocol__protocol__SupportingRowsUpdate__snapshot(&supporting_rows, rows);
        }
        has_supporting_rows = true;
    }
    u32 fact_add_count, fact_remove_count;
    jazz_protocol__protocol__SupportingRowsUpdate__added_rows(&supporting_rows, &fact_add_count);
    jazz_protocol__protocol__SupportingRowsUpdate__removed_rows(&supporting_rows, &fact_remove_count);
    u8 revision[16];
    jazz_protocol__protocol__SupportingRowsUpdate__revision(&supporting_rows, revision);
    if (adds.len == 0 && removes.len == 0 && fact_add_count == 0 && fact_remove_count == 0) {
        bool changed = initial_snapshot_completed || jazz_protocol__protocol__SupportingRowsUpdate__is_snapshot(&supporting_rows);
        PSS *state = JZP(PeerState__state)(p, sub);
        if (state) JZP(PeerSubscriptionState__acknowledge_published)(state, revision);
        *some = true;
        out->changed = changed;
        out->update = view_update_without_payload(sub, maintained_publication_cut(p, node, sub), supporting_rows);
        has_supporting_rows = false;
        goto done;
    }
    if (JZ_IS_ERR(e = prepared_context(p, sub, "maintained subscription view is missing prepared state", &tier, &read_view))) goto done;
    u64 bundle_start = jz_now_ms();
    if (trace_rehydrate) jazz_node__node__NodeState__reset_storage_read_metrics(node);
    PublicationPlan plan;
    memset(&plan, 0, sizeof plan);
    plan.subscription = sub;
    if (JZ_IS_ERR(e = served_subscription_policy_binding(p, sub, &plan.policy))) goto done;
    plan.tier = tier;
    plan.default_read_view = jazz_protocol__protocol__ReadViewSpec__is_default(read_view);
    plan.supporting_update = JZ_NEW(jazz_protocol_SupportingRowsUpdate);
    jazz_protocol__protocol__SupportingRowsUpdate__clone(plan.supporting_update, &supporting_rows);
    plan.settled_through = maintained_publication_cut(p, node, sub);
    const KSD *known = JZP(PeerState__known_state)(p, sub);
    if (known) { plan.has_known_state = true; plan.known_state = *known; }
    for (u32 i = 0; i < adds.len; i++) rme_push_clone(&plan.adds, &adds.ptr[i]);
    for (u32 i = 0; i < removes.len; i++) rme_push_clone(&plan.removes, &removes.ptr[i]);
    const MSVS *maintained = JZP(PeerState__maintained_view)(p, sub);
    if (!maintained) { plan_drop(&plan); e = JZN_INVALID_STORED("maintained subscription view subscription missing"); goto done; }
    SyncMsg *update = NULL;
    if (JZ_IS_ERR(e = bundle_publication(p, node, shape, sub, maintained->maintained, &plan, &update))) goto done;
    if (trace_rehydrate) {
        u64 bundle_elapsed = jz_now_ms() - bundle_start;
        groove_StorageReadMetrics bundle_reads = jazz_node__node__NodeState__take_storage_read_metrics(node);
        jz_str k = subkey_debug(sub);
        const jz_str *qt = &jazz_model__query__ValidatedQuery__query(shape)->table;
        trace_line(jz_format("JAZZ_REHYDRATE_TRACE stage=update table=%.*s subscription=%.*s drain_ms=%llu bundle_ms=%llu adds=%u removes=%u fact_adds=%u fact_removes=%u bundles=%u fallback=false drain_reads=%llu drain_ranges=%llu bundle_reads=%llu bundle_ranges=%llu",
                             (int)qt->len, qt->ptr, (int)k.len, k.ptr, (unsigned long long)drain_elapsed, (unsigned long long)bundle_elapsed,
                             result_add_count, result_remove_count, fact_add_count, fact_remove_count, (u32)traced_bundle_count(update),
                             (unsigned long long)drain_reads.total.reads, (unsigned long long)drain_reads.total.ranges,
                             (unsigned long long)bundle_reads.total.reads, (unsigned long long)bundle_reads.total.ranges));
        jz_str_drop(&k);
    }
    p->metrics.maintained_subscription_view->hits_out += 1;
    /* Advance the predecessor and retire the journal only after the fallible
     * bundle construction succeeds. */
    JZP(PeerState__record_outgoing_view_update_metadata)(p, update);
    /* Advance the receiver's exact-membership baseline by the delta shipped
     * here. */
    JZP(PeerState__apply_outgoing_view_delta)(p, sub, false, adds.ptr, adds.len, removes.ptr, removes.len);
    jazz_protocol_ViewUpdatePayload *view = jazz_protocol__protocol__SyncMessage__as_view_update(update);
    if (view) JZP(PeerSubscriptionState__acknowledge_published)(JZP(PeerState__state_entry)(p, view->subscription), revision);
    *some = true;
    out->changed = true;
    out->update = update;
done:
    if (has_supporting_rows) jazz_protocol__protocol__SupportingRowsUpdate__drop(&supporting_rows);
    if (read_view) jazz_protocol__protocol__ReadViewSpec__drop(read_view);
    rme_vec_drop(&adds);
    rme_vec_drop(&removes);
    return e;
}

static void seeded_drop(jazz_node_SeededMaintainedView *opened)
{
    groove__ivm__MultisinkSubscription__drop(opened->subscription);
    JZN_MSV(MaintainedSubscriptionView__drop)(opened->maintained);
    jazz_node_MaintainedTerminalSchemas_drop(opened->terminal_schemas);
    jazz_node_ResultTransitions_drop(&opened->transitions);
    jz_btree_drop(&opened->tables);
    jz_btree_drop(&opened->covered_input_sources);
    if (opened->covered_input_receiver) jazz_node__node__query_eval__maintained_views__CoveredInputReceiver__drop(opened->covered_input_receiver);
}

static void tables_rc_drop(void *t) { jz_btree_drop(t); }

/* Move the opened view parts into a retained subscription record. */
static MSVS *maintained_subscription_from(jazz_node_SeededMaintainedView *opened, const jazz_model_ValidatedQuery *shape,
                                          bool has_source, const jazz_protocol_AuthorityResultKey *source, bool initial_received)
{
    MSVS *m = JZ_NEW(MSVS);
    m->subscription = opened->subscription;
    m->maintained = opened->maintained;
    m->terminal_schemas = opened->terminal_schemas;
    m->tables = jz_rc_new(sizeof(jz_btree), tables_rc_drop);
    *m->tables = opened->tables;
    m->covered_input_receiver = opened->covered_input_receiver;
    m->result_schema_version = jazz_model__query__ValidatedQuery__schema_version(shape);
    m->has_source_authority_result = has_source;
    if (has_source) jazz_protocol__protocol__AuthorityResultKey__clone(&m->source_authority_result, source);
    m->initial_received = initial_received;
    opened->subscription = NULL; opened->maintained = NULL; opened->terminal_schemas = NULL; opened->covered_input_receiver = NULL;
    jz_btree_init(&opened->tables, opened->tables.d);
    return m;
}

static jz_err rehydrate_query_maintained_subscription_view(PS *p, NS *node, jazz_node_MaintainedRehydrateRequest request, const jz_waker *waker, SyncMsg **out)
{
    const jazz_model_ValidatedQuery *shape = request.shape;
    SubKey sub = request.subscription;
    const jz_btree *previous = request.previous_member_result_set;
    *out = NULL;
    bool trace_rehydrate = jazz_node__debug_env__rehydrate_trace();
    u64 open_start = jz_now_ms();
    if (trace_rehydrate) jazz_node__node__NodeState__reset_storage_read_metrics(node);
    bool relay_requires_authority_source = requires_selected_authority_source(p, sub, request.purpose);
    /* The downstream usage registration chose this policy scope. Carry that
     * exact receipt into source resolution; the shared binding-view key alone
     * is not an authority identity in a multiplexed relay. */
    bool has_source = false; jazz_protocol_AuthorityResultKey source;
    if (relay_requires_authority_source) {
        /* Suspend this opening until the connection records its exact
         * upstream usage source; guessing from the group key would leak or
         * erase a sibling policy's membership. */
        if (!selected_authority_source(p, sub, &source)) return JZ_OK;
        /* Do not open a cold maintained receiver against an empty
         * pre-settlement source. */
        if (!jazz_node__node__query_eval__subscriptions__NodeState__has_settled_authority_result(node, &source)) {
            jazz_protocol__protocol__AuthorityResultKey__drop(&source);
            return JZ_OK;
        }
        has_source = true;
    }
    jazz_node_PolicyBinding policy;
    jz_err e = served_subscription_policy_binding(p, sub, &policy);
    if (JZ_IS_ERR(e)) { if (has_source) jazz_protocol__protocol__AuthorityResultKey__drop(&source); return e; }
    jazz_types_AuthorSubject policy_identity = policy.identity;
    if (jazz_node__debug_env__covered_input_trace()) {
        jz_str k = subkey_debug(sub);
        trace_line(jz_format("JAZZ_COVERED_INPUT_TRACE stage=rehydrate peer=%p owner=%llu subscription=%.*s identity=%u source=%s purpose=%s",
                             (void *)p, (unsigned long long)p->publication_owner, (int)k.len, k.ptr, policy_identity.tag, has_source ? "Some" : "None",
                             request.purpose == JZN_REHYDRATE_QUERY ? "Query" : "AuthorizationSupport"));
        jz_str_drop(&k);
    }
    jazz_node_SeededMaintainedView opened;
    memset(&opened, 0, sizeof opened);
    NS *scoped = jazz_node__node__NodeState__scoped_active_session_claims(node, policy_identity, policy.claims);
    if (request.purpose == JZN_REHYDRATE_QUERY && relay_requires_authority_source) {
        /* A relay's selected Global child is the browser half of a durable
         * worker authority receipt. */
        jazz_protocol_AuthorityResultKey k;
        jazz_protocol__protocol__AuthorityResultKey__clone(&k, &source);
        e = jazz_node__node__query_eval__NodeState__open_seeded_relay_subscription_view_with_waker(scoped, shape, request.binding, policy_identity,
                                                                                                  request.read_view, sub.read_view, k, waker, &opened);
    } else if (request.purpose == JZN_REHYDRATE_QUERY) {
        e = jazz_node__node__query_eval__NodeState__open_seeded_maintained_subscription_view_with_waker(scoped, shape, request.binding, policy_identity,
                                                                                                       request.tier, request.read_view, sub.read_view, waker, &opened);
        if (!JZ_IS_ERR(e) && opened.covered_input_receiver) { /* ordinary trusted-serving view: no receiver */
            jazz_node__node__query_eval__maintained_views__CoveredInputReceiver__drop(opened.covered_input_receiver);
            opened.covered_input_receiver = NULL;
        }
    } else {
        e = jazz_node__node__query_eval__NodeState__open_seeded_authorization_support_subscription_view_with_waker(scoped, shape, request.binding, policy_identity,
                                                                                                                  request.tier, request.read_view, sub.read_view, waker, &opened);
        if (!JZ_IS_ERR(e) && opened.covered_input_receiver) {
            jazz_node__node__query_eval__maintained_views__CoveredInputReceiver__drop(opened.covered_input_receiver);
            opened.covered_input_receiver = NULL;
        }
    }
    jazz_node__node__NodeState__end_scoped_active_session_claims(scoped);
    if (JZ_IS_ERR(e)) {
        if (has_source) jazz_protocol__protocol__AuthorityResultKey__drop(&source);
        if (e.code == JZN_E_AUTHORIZATION_SUPPORT_MISSING_CLAIM && request.purpose == JZN_REHYDRATE_AUTHORIZATION_SUPPORT) {
            /* A missing claim cannot manufacture a valid successor closure;
             * preserve that failure for an established result set. */
            if (previous->len != 0) return e;
            jz_err_drop(&e);
            SyncMsg *update = view_update_without_payload(sub, maintained_publication_cut(p, node, sub), empty_snapshot());
            JZP(PeerState__record_outgoing_view_update)(p, update);
            JZP(PeerState__state_entry)(p, sub)->has_served_authorization_progress = true;
            *out = update;
            return JZ_OK;
        }
        return e;
    }
    /* open_seeded_relay_subscription_view_with_waker has already installed
     * the exact source closure, driven the receiver graph, and folded the
     * same terminal batch it returns here. */
    jazz_node_ResultTransitions *transitions = &opened.transitions;
    if (jazz_node__debug_env__covered_input_trace()) {
        jz_str k = subkey_debug(sub);
        trace_line(jz_format("JAZZ_COVERED_INPUT_TRACE stage=rehydrate_opened owner=%llu subscription=%.*s identity=%u initial=%s adds=%u facts=%u",
                             (unsigned long long)p->publication_owner, (int)k.len, k.ptr, policy_identity.tag, opened.initial_received ? "true" : "false",
                             transitions->adds.len, transitions->program_fact_adds.len));
        jz_str_drop(&k);
    }
    if (!jazz_node__node__MaintainedTerminalSchemas__has_root_collector(opened.terminal_schemas))
        JZN_MSV(MaintainedSubscriptionView__discard_structured_app_rows)(opened.maintained);
    if (!opened.initial_received) {
        MSVS *pending = maintained_subscription_from(&opened, shape, has_source, &source, false);
        replace_maintained_subscription_view(p, node, sub, pending);
        seeded_drop(&opened);
        if (has_source) jazz_protocol__protocol__AuthorityResultKey__drop(&source);
        return JZ_OK;
    }
    u64 open_elapsed = jz_now_ms() - open_start;
    groove_StorageReadMetrics open_reads = {0};
    if (trace_rehydrate) open_reads = jazz_node__node__NodeState__take_storage_read_metrics(node);
    u32 raw_add_count = transitions->adds.len, raw_remove_count = transitions->removes.len, raw_fact_add_count = transitions->program_fact_adds.len;
    u64 filter_start = jz_now_ms();
    bool policy_scoped;
    jz_btree current; jz_btree_init(&current, &jzp_desc_member_set);
    RMEVec adds = {0}, removes = {0};
    jazz_protocol_SupportingRowsUpdate *catch_up = NULL;
    if (JZ_IS_ERR(e = aggregate_is_policy_scoped(node, shape, &policy_scoped))) goto fail;
    const KSD *known_ptr = JZP(PeerState__known_state)(p, sub);
    bool has_known = known_ptr != NULL; KSD known_state;
    if (has_known) known_state = *known_ptr;
    jazz_types_GlobalTime position = 0;
    bool has_position = JZP(subscription_state__fast_current_membership_position)(has_known ? &known_state : NULL, &position);
    bool authorization_matches = fast_cursor_authorization_matches(p, sub, has_known ? &known_state : NULL);
    jazz_types_GlobalTime watermark = maintained_publication_cut(p, node, sub);
    bool simple_membership_delta = transitions->program_fact_adds.len == 0 && transitions->program_fact_removes.len == 0;
    for (u32 i = 0; i < transitions->adds.len; i++) {
        RME *member = &transitions->adds.ptr[i];
        if (published_member(member, request.result_table_filter, &opened.tables, policy_scoped)) JZ_VEC_PUSH(adds, *member);
        else jazz_protocol__protocol__ResultMemberEntry__drop(member);
    }
    transitions->adds.len = 0;
    for (u32 i = 0; i < adds.len; i++) rme_set_insert_clone(&current, &adds.ptr[i]);
    {
        RME *m; void *unused;
        JZ_BTREE_FOR(previous, m, unused) if (!jz_btree_contains(&current, m)) rme_push_clone(&removes, m);
    }
    /* A live reconciliation is still a delta to the receiver's existing
     * result set. Rehydration used for an explicit reset is different: the
     * receiver discards that set and needs the complete replacement. */
    if (!request.reset_input_set) {
        u32 w = 0;
        for (u32 i = 0; i < adds.len; i++) {
            if (jz_btree_contains(previous, &adds.ptr[i])) jazz_protocol__protocol__ResultMemberEntry__drop(&adds.ptr[i]);
            else adds.ptr[w++] = adds.ptr[i];
        }
        adds.len = w;
    }
    /* The downstream cursor tracks data progress, not authorization. A client
     * may only reuse a pre-cursor membership diff when this peer retained the
     * view and the receiver echoes its exact server-stamped authorization
     * generation. Fresh, legacy, and tokenless client declarations keep the
     * #1266 authoritative reset. */
    bool cursor_membership_mismatch = !authorization_matches && has_position
        && JZP(subscription_state__fast_cursor_requires_authoritative_reset)(position, previous, &current);
    bool any_positioned = false;
    for (u32 i = 0; i < adds.len; i++) { jazz_types_GlobalTime s; if (JZP(subscription_state__member_settle_position)(&adds.ptr[i], &s)) { any_positioned = true; break; } }
    bool reset_input_set;
    if (request.reset_input_set && simple_membership_delta && !cursor_membership_mismatch && has_position && watermark > 0 && position >= watermark) {
        rme_vec_drop(&adds);
        rme_vec_drop(&removes);
        reset_input_set = false;
    } else if (request.reset_input_set && !cursor_membership_mismatch && simple_membership_delta && has_position && any_positioned) {
        u32 w = 0;
        for (u32 i = 0; i < adds.len; i++) {
            jazz_types_GlobalTime settled;
            if (!JZP(subscription_state__member_settle_position)(&adds.ptr[i], &settled) || settled > position) adds.ptr[w++] = adds.ptr[i];
            else jazz_protocol__protocol__ResultMemberEntry__drop(&adds.ptr[i]);
        }
        adds.len = w;
        rme_vec_drop(&removes);
        reset_input_set = false;
    } else {
        reset_input_set = request.reset_input_set;
    }
    /* "Q at W" against a single table: membership depends only on each row's
     * own image, so the rows whose seq moved past W are the whole difference
     * between the receiver's set and the current one. */
    if (watermark > 0) {
        const jz_str *single_table = NULL; jazz_types_GlobalPhysicalTableId physical_table;
        bool has_single = JZN_MSV(MaintainedSubscriptionView__single_physical_table)(opened.maintained, &single_table, &physical_table);
        jzn_SupportingRowRefVec rows = {0};
        JZN_MSV(MaintainedSubscriptionView__supporting_rows)(opened.maintained, &rows);
        e = watermark_catch_up(node, has_known ? &known_state : NULL, jazz_protocol__protocol__ReadViewSpec__is_default(request.read_view), has_single,
                               single_table, physical_table, jazz_model__query__ValidatedQuery__query(shape), &rows, NULL, authorization_matches, &catch_up);
        jz_vec_drop(&rows, sizeof(rows.ptr[0]), NULL);
        if (JZ_IS_ERR(e)) goto fail;
    }
    if (catch_up) {
        rme_vec_drop(&removes);
        JZP(PeerState__settle_watermark_declaration)(p, sub);
    }
    bool bundle_has_known = !(cursor_membership_mismatch && !catch_up) && has_known;
    u64 filter_elapsed = jz_now_ms() - filter_start;
    u32 result_add_count = adds.len, result_remove_count = removes.len;
    u32 positioned_members = 0;
    if (trace_rehydrate)
        for (u32 i = 0; i < adds.len; i++) { jazz_types_GlobalTime s; if (JZP(subscription_state__member_settle_position)(&adds.ptr[i], &s)) positioned_members++; }
    u64 bundle_start = jz_now_ms();
    if (trace_rehydrate) jazz_node__node__NodeState__reset_storage_read_metrics(node);
    PublicationPlan plan;
    memset(&plan, 0, sizeof plan);
    plan.subscription = sub;
    if (JZ_IS_ERR(e = served_subscription_policy_binding(p, sub, &plan.policy))) goto fail;
    plan.tier = request.tier;
    plan.default_read_view = jazz_protocol__protocol__ReadViewSpec__is_default(request.read_view);
    plan.supporting_update = catch_up;
    catch_up = NULL;
    plan.settled_through = watermark;
    plan.has_known_state = bundle_has_known;
    if (bundle_has_known) plan.known_state = known_state;
    plan.has_leave_scan_after = has_position;
    plan.leave_scan_after = position;
    plan.adds = adds;
    plan.removes = removes;
    memset(&adds, 0, sizeof adds);
    memset(&removes, 0, sizeof removes);
    SyncMsg *update = NULL;
    e = bundle_publication(p, node, shape, sub, opened.maintained, &plan, &update);
    if (JZ_IS_ERR(e)) {
        jazz_node__node__NodeState__unsubscribe_groove_subscription(node, groove__ivm__MultisinkSubscription__id(opened.subscription));
        goto fail;
    }
    if (trace_rehydrate) {
        u64 bundle_elapsed = jz_now_ms() - bundle_start;
        groove_StorageReadMetrics bundle_reads = jazz_node__node__NodeState__take_storage_read_metrics(node);
        jz_str k = subkey_debug(sub);
        jz_str buckets = JZP(delivery__storage_read_metrics_buckets)(&open_reads);
        const jz_str *qt = &jazz_model__query__ValidatedQuery__query(shape)->table;
        trace_line(jz_format("JAZZ_REHYDRATE_TRACE stage=rehydrate table=%.*s subscription=%.*s reset=%s known_state=%s positioned_members=%u open_ms=%llu filter_ms=%llu bundle_ms=%llu raw_adds=%u raw_removes=%u raw_fact_adds=%u adds=%u removes=%u bundles=%u open_reads=%llu open_ranges=%llu open_read_buckets=%.*s bundle_reads=%llu bundle_ranges=%llu",
                             (int)qt->len, qt->ptr, (int)k.len, k.ptr, reset_input_set ? "true" : "false", has_known ? "Some" : "None", positioned_members,
                             (unsigned long long)open_elapsed, (unsigned long long)filter_elapsed, (unsigned long long)bundle_elapsed,
                             raw_add_count, raw_remove_count, raw_fact_add_count, result_add_count, result_remove_count, (u32)traced_bundle_count(update),
                             (unsigned long long)open_reads.total.reads, (unsigned long long)open_reads.total.ranges, (int)buckets.len, buckets.ptr,
                             (unsigned long long)bundle_reads.total.reads, (unsigned long long)bundle_reads.total.ranges));
        jz_str_drop(&buckets);
        jz_str_drop(&k);
    }
    MSVS *retained = maintained_subscription_from(&opened, shape, has_source, &source, true);
    /* Whichever frame shape was chosen (full reset, membership delta, or
     * Q-at-W catch-up), the receiver now holds exactly the current
     * membership. Record it as the baseline. */
    RMEVec current_members = {0};
    RME m;
    while (jz_btree_pop_first(&current, &m, NULL)) JZ_VEC_PUSH(current_members, m);
    JZP(PeerState__apply_outgoing_view_delta)(p, sub, true, current_members.ptr, current_members.len, NULL, 0);
    rme_vec_drop(&current_members);
    replace_maintained_subscription_view(p, node, sub, retained);
    JZP(PeerState__record_outgoing_view_update)(p, update);
    JZP(PeerState__state_entry)(p, sub)->has_served_authorization_progress = true;
    p->metrics.maintained_subscription_view->hits_out += 1;
    *out = update;
    e = JZ_OK;
fail:
    if (catch_up) { jazz_protocol__protocol__SupportingRowsUpdate__drop(catch_up); jz_free(catch_up); }
    rme_vec_drop(&adds);
    rme_vec_drop(&removes);
    jz_btree_drop(&current);
    seeded_drop(&opened);
    if (has_source) jazz_protocol__protocol__AuthorityResultKey__drop(&source);
    return e;
}

/* Owner-loop variant retaining the tick owner's cold-query wake route. */
jz_err JZP(PeerState__rehydrate_query_for_subscription_with_opts_and_waker)(PS *p, NS *node, SubKey sub, const jazz_model_ValidatedQuery *shape,
                                                                            const jazz_model_Binding *binding, const RSO *opts, const jz_waker *waker, SyncMsg **out)
{
    return rehydrate_query_for_subscription_with_purpose(p, node, sub, shape, binding, opts, JZN_REHYDRATE_QUERY, waker, out);
}

/* Build a reset-result-set query view update for a usage-site subscription. */
jz_err JZP(PeerState__rehydrate_query_for_subscription_with_opts)(PS *p, NS *node, SubKey sub, const jazz_model_ValidatedQuery *shape,
                                                                  const jazz_model_Binding *binding, const RSO *opts, SyncMsg **out)
{
    JZ_TRY(ensure_direct_internal_subscription_policy_binding(p, node, sub));
    return JZP(PeerState__rehydrate_query_for_subscription_with_opts_and_waker)(p, node, sub, shape, binding, opts, NULL, out);
}

jz_err JZP(PeerState__rehydrate_query_for_subscription_from_maintained_subscription)(PS *p, NS *node, SubKey maintained_subscription,
                                                                                     SubKey target_subscription, const jazz_model_ValidatedQuery *shape, SyncMsg **out);

/* Build a reset-result-set query-binding view update with registration options. */
jz_err JZP(PeerState__rehydrate_query_with_opts)(PS *p, NS *node, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
                                                 const RSO *opts, SyncMsg **out)
{
    SubKey sub = subscription_for(shape, binding, jazz_protocol__protocol__RegisterShapeOptions__read_view_key(opts));
    JZ_TRY(JZP(PeerState__rehydrate_query_for_subscription_with_opts)(p, node, sub, shape, binding, opts, out));
    if (*out) return JZ_OK;
    JZ_TRY(jazz_node__node__NodeState__drive_query_runtime(node));
    JZ_TRY(JZP(PeerState__rehydrate_query_for_subscription_from_maintained_subscription)(p, node, sub, sub, shape, out));
    if (!*out) return JZN_INVALID_STORED("query hydration ended without an initial publication");
    return JZ_OK;
}

/* Build a reset-result-set query-binding view update. */
jz_err JZP(PeerState__rehydrate_query)(PS *p, NS *node, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, SyncMsg **out)
{
    RSO opts;
    jazz_protocol__protocol__RegisterShapeOptions__default(&opts);
    jz_err e = JZP(PeerState__rehydrate_query_with_opts)(p, node, shape, binding, &opts, out);
    jazz_protocol__protocol__RegisterShapeOptions__drop(&opts);
    return e;
}

/* Build a reset-result_set current-row view update. */
jz_err JZP(PeerState__rehydrate_current_rows)(PS *p, NS *node, const jz_str *table, SyncMsg **out)
{
    jazz_model_ValidatedQuery *shape; jazz_model_Binding *binding;
    JZ_TRY(jazz_node__node__views__NodeState__whole_table_shape_binding(node, table, &shape, &binding));
    jz_err e = JZP(PeerState__rehydrate_query)(p, node, shape, binding, out);
    jazz_model__query__ValidatedQuery__drop(shape);
    jazz_model__query__Binding__drop(binding);
    return e;
}

/* Count a full rehydrate-and-diff that retires `subscription`'s view.
 * A first open or a retry of a still-cold view is ordinary hydration. */
static void note_full_diff_reopen(PS *p, SubKey sub, jazz_node_RehydratePurpose purpose)
{
    const MSVS *m = JZP(PeerState__maintained_view)(p, sub);
    if (!m || !m->initial_received) return;
    jazz_node_FullDiffFallbackMetrics *fallbacks = &p->metrics.maintained_subscription_view->full_diff_fallbacks;
    switch (purpose) {
    case JZN_REHYDRATE_QUERY: fallbacks->query_reopens += 1; break;
    case JZN_REHYDRATE_AUTHORIZATION_SUPPORT: fallbacks->authorization_support_reopens += 1; break;
    }
}

/* Count a claim refresh that retires a published direct query view; its
 * replacement coverage key is opened cold. */
void JZP(PeerState__note_claim_refresh_full_diff)(PS *p, SubKey sub) { note_full_diff_reopen(p, sub, JZN_REHYDRATE_QUERY); }

static jz_err rehydrate_query_for_subscription_with_purpose(PS *p, NS *node, SubKey sub, const jazz_model_ValidatedQuery *shape,
                                                            const jazz_model_Binding *binding, const RSO *opts, jazz_node_RehydratePurpose purpose,
                                                            const jz_waker *waker, SyncMsg **out)
{
    *out = NULL;
    if (requires_selected_authority_source(p, sub, purpose)) {
        jazz_protocol_AuthorityResultKey source;
        if (!selected_authority_source(p, sub, &source)) return JZ_OK;
        bool settled = jazz_node__node__query_eval__subscriptions__NodeState__has_settled_authority_result(node, &source);
        jazz_protocol__protocol__AuthorityResultKey__drop(&source);
        /* Readiness is checked before any teardown or registration. An
         * unrelated dirty wake must not destroy a pending strict receiver. */
        if (!settled) return JZ_OK;
    }
    if (purpose == JZN_REHYDRATE_QUERY && has_current_pending_initial_maintained_subscription(p, node, sub)) {
        /* The maintained receiver already owns the registration and its
         * storage waker. Drain it in place. */
        return query_update_maintained_subscription_view(p, node, shape, binding, sub, NULL, waker, out);
    }
    note_full_diff_reopen(p, sub, purpose);
    clear_stale_groove_runtime_handles(p, node, sub);
    PSS *old = JZP(PeerState__state)(p, sub);
    jz_btree previous;
    member_result_set(p, sub, &previous);
    jz_btree previous_member_index;
    if (old) jz_btree_clone(&previous_member_index, &old->member_index, JZP(MemberIndexKey__clone), JZP(MemberSlot__clone));
    else jz_btree_init(&previous_member_index, &jzp_desc_member_index);
    jazz_node_LocalAuthorityReconciliation *previous_local_authority = old
        ? jazz_node__node__query_eval__local_authority_reconciliation__LocalAuthorityReconciliation__clone(old->local_authority)
        : jazz_node__node__query_eval__local_authority_reconciliation__LocalAuthorityReconciliation__default();
    const KSD *known_ptr = JZP(PeerState__known_state)(p, sub);
    bool has_known = known_ptr != NULL; KSD known_state;
    if (has_known) known_state = *known_ptr;
    bool has_retained_authorization = old && old->has_served_authorization_progress;
    u64 retained_authorization = has_retained_authorization ? old->authorization_progress : 0;
    /* The relay's exact upstream receipt is immutable lifecycle metadata for
     * this usage site. Carry it through the replacement. */
    bool has_source = false; jazz_protocol_AuthorityResultKey authority_result_source;
    if (old && old->has_authority_result_source) {
        jazz_protocol__protocol__AuthorityResultKey__clone(&authority_result_source, &old->authority_result_source);
        has_source = true;
    }
    bool awaiting = old && old->awaiting_selected_authority_source;
    jazz_node_PolicyBinding policy_binding;
    jz_err e = served_subscription_policy_binding(p, sub, &policy_binding);
    if (JZ_IS_ERR(e)) goto fail_early;
    /* Retire the old publication before retaining its replacement. */
    JZP(PeerState__forget_subscription_with_node)(p, node, sub);
    if (JZ_IS_ERR(e = ensure_query_subscription_registered(p, node, sub, shape, binding, opts, &policy_binding))) {
        JZP(PolicyBinding__drop)(&policy_binding);
        goto fail_early;
    }
    if (has_known) jz_btree_insert(&p->downstream_known_states, &sub, &known_state, NULL);
    jazz_node_PreparedQueryPlan *plan = jazz_node__node__query_eval__NodeState__mark_peer_maintained_query_shape_cache(node, shape, binding, opts->tier);
    jazz_node_CachedPeerQueryPlan *cached = JZP(CachedPeerQueryPlan__with_plan)(opts, plan);
    jazz_model_DurabilityTier tier; jazz_protocol_ReadViewSpec *read_view;
    JZP(CachedPeerQueryPlan__context)(cached, &tier, &read_view);
    PSS *state = JZP(PeerState__state_entry)(p, sub);
    install_prepared_plan(state, node, cached);
    jz_btree_drop(&state->result_member_set);
    JZP(PeerSubscriptionState__member_result_set)(&(PSS){ .result_member_set = previous }, &state->result_member_set);
    jz_btree_drop(&state->member_index);
    state->member_index = previous_member_index;
    jazz_node__node__query_eval__local_authority_reconciliation__LocalAuthorityReconciliation__drop(state->local_authority);
    state->local_authority = previous_local_authority;
    if (state->has_policy_binding) JZP(PolicyBinding__drop)(&state->policy_binding);
    state->has_policy_binding = true;
    state->policy_binding = policy_binding;
    if (state->has_authority_result_source) jazz_protocol__protocol__AuthorityResultKey__drop(&state->authority_result_source);
    state->has_authority_result_source = has_source;
    if (has_source) state->authority_result_source = authority_result_source;
    state->awaiting_selected_authority_source = awaiting;
    if (has_retained_authorization) {
        state->authorization_progress = retained_authorization;
        state->has_served_authorization_progress = true;
    }
    jazz_node_MaintainedRehydrateRequest request = {
        .shape = shape, .binding = binding, .subscription = sub, .previous_member_result_set = &previous, .reset_input_set = true,
        .result_table_filter = NULL, .tier = tier, .read_view = read_view, .purpose = purpose,
    };
    e = rehydrate_query_maintained_subscription_view(p, node, request, waker, out);
    jazz_protocol__protocol__ReadViewSpec__drop(read_view);
    jz_btree_drop(&previous);
    return e;
fail_early:
    jz_btree_drop(&previous);
    jz_btree_drop(&previous_member_index);
    jazz_node__node__query_eval__local_authority_reconciliation__LocalAuthorityReconciliation__drop(previous_local_authority);
    if (has_source) jazz_protocol__protocol__AuthorityResultKey__drop(&authority_result_source);
    return e;
}

/* Hydrate an authority-owned authorization proof using its admitted
 * permission subject. A trusted backend link normally serves ordinary reads
 * as SYSTEM, but that bypass must not leak into a proof of a particular
 * session's policy clauses. The AuthorizationSupportPeerScope guard (Drop
 * restores the role and forgets an incomplete subscription) is the `done`
 * block below. */
jz_err JZP(PeerState__rehydrate_authorization_support_query_for_identity)(PS *p, NS *node, jazz_types_AuthorSubject identity, jz_btree claims,
                                                                          SubKey sub, const jazz_model_ValidatedQuery *shape,
                                                                          const jazz_model_Binding *binding, const RSO *opts, SyncMsg **out)
{
    *out = NULL;
    JZP(PeerState__set_subscription_policy_binding)(p, sub, identity, claims);
    jazz_node_PeerRole previous_role = p->role;
    bool previous_has_permission = p->has_permission_identity;
    jazz_types_AuthorSubject previous_permission_identity = p->permission_identity;
    bool completed = false;
    p->role.tag = JZN_PEER_ROLE_CLIENT_LINK;
    p->role.identity = identity;
    p->has_permission_identity = true;
    p->permission_identity = identity;
    SyncMsg *opening = NULL;
    jz_err e = rehydrate_query_for_subscription_with_purpose(p, node, sub, shape, binding, opts, JZN_REHYDRATE_AUTHORIZATION_SUPPORT, NULL, &opening);
    if (JZ_IS_ERR(e)) goto done;
    if (opening) {
        completed = true;
        *out = opening;
        goto done;
    }
    /* The proof owns this retained receiver. Await its ordinary storage
     * progress instead of treating a cold opening as unavailable. */
    if (JZ_IS_ERR(e = jazz_node__node__NodeState__drive_query_runtime(node))) goto done;
    if (JZ_IS_ERR(e = query_update_maintained_subscription_view(p, node, shape, binding, sub, NULL, NULL, out))) goto done;
    if (!*out) e = JZN_INVALID_STORED("authorization hydration missing initial snapshot after storage progress");
    completed = !JZ_IS_ERR(e);
done:
    /* impl Drop for AuthorizationSupportPeerScope */
    if (!completed) JZP(PeerState__forget_subscription_with_node)(p, node, sub);
    p->role = previous_role;
    p->has_permission_identity = previous_has_permission;
    p->permission_identity = previous_permission_identity;
    return e;
}

jz_err JZP(PeerState__rehydrate_query_for_subscription_from_maintained_subscription_and_waker)(PS *p, NS *node, SubKey maintained_subscription,
                                                                                              SubKey target_subscription, const jazz_model_ValidatedQuery *shape,
                                                                                              const jz_waker *waker, SyncMsg **out)
{
    *out = NULL;
    clear_stale_groove_runtime_handles(p, node, maintained_subscription);
    jazz_node_ResultTransitions t;
    JZ_TRY(JZP(PeerState__drain_maintained_subscription_view_changes)(p, node, shape, maintained_subscription, NULL, waker, &t));
    const MSVS *m = JZP(PeerState__maintained_view)(p, maintained_subscription);
    if (!m || !m->initial_received) { jazz_node_ResultTransitions_drop(&t); return JZ_OK; }
    bool source_had_program_fact_transitions = t.supporting_changed || t.program_fact_adds.len || t.program_fact_removes.len;
    ClonedResetBasis basis;
    cloned_reset_basis(p, maintained_subscription, target_subscription, t.removes.ptr, t.removes.len, source_had_program_fact_transitions, &basis);
    if (t.supporting_changed || t.adds.len || t.removes.len || t.program_fact_adds.len || t.program_fact_removes.len)
        JZP(PeerState__apply_outgoing_view_delta)(p, maintained_subscription, false, t.adds.ptr, t.adds.len, t.removes.ptr, t.removes.len);
    jazz_node_ResultTransitions_drop(&t);
    return cloned_subscription_reset(p, node, maintained_subscription, target_subscription, shape, basis, out);
}

/* Build a usage-site update from an already-maintained canonical subscription. */
jz_err JZP(PeerState__rehydrate_query_for_subscription_from_maintained_subscription)(PS *p, NS *node, SubKey maintained_subscription,
                                                                                     SubKey target_subscription, const jazz_model_ValidatedQuery *shape, SyncMsg **out)
{
    JZ_TRY(ensure_direct_internal_subscription_policy_binding(p, node, target_subscription));
    return JZP(PeerState__rehydrate_query_for_subscription_from_maintained_subscription_and_waker)(p, node, maintained_subscription, target_subscription,
                                                                                                  shape, NULL, out);
}

/* A late usage gets the same physical state as the canonical publisher. Its
 * snapshot must establish that publisher's cursor, because future deltas are
 * shared/fanned out from that canonical stream. */
static void bind_cloned_supporting_revision(const PS *p, SubKey source, jazz_protocol_ViewUpdatePayload *view)
{
    PSS *state = JZP(PeerState__state)(p, source);
    if (state && state->has_supporting_revision && view->supporting_rows.tag == JAZZ_SRU_SNAPSHOT)
        memcpy(view->supporting_rows.revision, state->supporting_revision, 16);
}

/* Consume canonical maintained-view work and return its publishable
 * transition without starting the fallible usage-site reset. */
jz_err JZP(PeerState__reconcile_maintained_subscription_for_clone)(PS *p, NS *node, SubKey maintained_subscription, const jazz_model_ValidatedQuery *shape,
                                                                   const jazz_model_Binding *binding, const RSO *opts, const jz_waker *waker,
                                                                   bool *some, jazz_node_ReconciledMaintainedSubscriptionClone *out)
{
    MaintainedCanonicalUpdate canonical = {0};
    JZ_TRY(query_update_inner_for_subscription_with_metadata(p, node, maintained_subscription, shape, binding, opts, waker, some, &canonical));
    if (!*some) return JZ_OK;
    memset(out, 0, sizeof *out);
    out->source_had_program_fact_transitions = canonical.changed;
    if (canonical.changed) out->canonical_update = canonical.update;
    else jazz_protocol__protocol__SyncMessage__drop(canonical.update);
    return JZ_OK;
}

/* Assemble a usage-site reset after the caller has published the canonical
 * transition to every established sibling. */
jz_err JZP(PeerState__rehydrate_query_for_subscription_from_reconciled_maintained_subscription)(PS *p, NS *node, SubKey maintained_subscription,
                                                                                               SubKey target_subscription, const jazz_model_ValidatedQuery *shape,
                                                                                               jazz_node_ReconciledMaintainedSubscriptionClone reconciled, SyncMsg **out)
{
    ClonedResetBasis basis;
    cloned_reset_basis(p, maintained_subscription, target_subscription, reconciled.source_removes.ptr, reconciled.source_removes.len,
                       reconciled.source_had_program_fact_transitions, &basis);
    if (reconciled.canonical_update) jazz_protocol__protocol__SyncMessage__drop(reconciled.canonical_update);
    rme_vec_drop(&reconciled.source_removes);
    return cloned_subscription_reset(p, node, maintained_subscription, target_subscription, shape, basis, out);
}

/* Read what a usage-site reset needs to know about its downstream receiver.
 * Callers take this before applying the canonical delta. */
static void cloned_reset_basis(const PS *p, SubKey maintained_subscription, SubKey target_subscription, const RME *source_removes, u32 nremoves,
                               bool source_had_program_fact_transitions, ClonedResetBasis *out)
{
    memset(out, 0, sizeof *out);
    const KSD *known = JZP(PeerState__known_state)(p, target_subscription);
    out->has_known_state = known != NULL;
    if (known) out->known_state = *known;
    const KSD *ks = out->has_known_state ? &out->known_state : NULL;
    out->has_known_membership_position = JZP(subscription_state__fast_current_membership_position)(ks, &out->known_membership_position);
    out->authorization_matches = fast_cursor_authorization_matches(p, maintained_subscription, ks);
    bool ordinary = true;
    for (u32 i = 0; i < nremoves; i++) if (!ordinary_current_content_member(&source_removes[i])) { ordinary = false; break; }
    out->removed_members_are_ordinary = ordinary;
    out->source_had_program_fact_transitions = source_had_program_fact_transitions;
    bool client_link = p->role.tag != JZN_PEER_ROLE_RELAY;
    if (client_link && out->authorization_matches && ordinary && read_view_key_eq(maintained_subscription.read_view, default_read_view_key())
        && !source_had_program_fact_transitions) {
        PSS *canonical = JZP(PeerState__state)(p, maintained_subscription);
        JZ_ASSERT(canonical, "publication_states[&maintained_subscription]");
        jzn_SupportingRowRefVec rows;
        JZP(PeerSubscriptionState__supporting_rows)(canonical, &rows);
        bool no_rows = rows.len == 0;
        jz_vec_drop(&rows, sizeof(rows.ptr[0]), NULL);
        if (no_rows) {
            out->has_flat_row_removes = true;
            for (u32 i = 0; i < nremoves; i++) rme_push_clone(&out->flat_row_removes, &source_removes[i]);
        }
    }
}

/* Assemble a usage-site reset from the canonical publisher's current state,
 * shared by direct rehydration and the reconciled clone path. */
static jz_err cloned_subscription_reset(PS *p, NS *node, SubKey maintained_subscription, SubKey target_subscription,
                                        const jazz_model_ValidatedQuery *shape, ClonedResetBasis basis, SyncMsg **out)
{
    *out = NULL;
    bool client_link = p->role.tag != JZN_PEER_ROLE_RELAY;
    jazz_protocol_ReadViewSpec *read_view = NULL;
    jazz_protocol_SupportingRowsUpdate *catch_up = NULL;
    RMEVec adds = {0}, removes = {0};
    jz_err e = JZ_OK;
    PSS *canonical_state = JZP(PeerState__state)(p, maintained_subscription);
    if (!canonical_state) { e = JZN_INVALID_STORED("coverage group subscription is missing peer state"); goto done; }
    const jz_btree *current = &canonical_state->result_member_set;
    jzn_SupportingRowRefVec fact_rows;
    JZP(PeerSubscriptionState__supporting_rows)(canonical_state, &fact_rows);
    bool facts_empty = fact_rows.len == 0;
    jz_vec_drop(&fact_rows, sizeof(fact_rows.ptr[0]), NULL);
    bool can_forward_flat_removals = client_link
        && ordinary_flat_row_duplicate_view(shape, current, basis.removed_members_are_ordinary, maintained_subscription.read_view, facts_empty,
                                            basis.source_had_program_fact_transitions);
    bool authorization_mismatch = client_link && basis.has_known_membership_position && !basis.authorization_matches;
    if (can_forward_flat_removals && basis.authorization_matches && basis.has_flat_row_removes) {
        removes = basis.flat_row_removes;
        memset(&basis.flat_row_removes, 0, sizeof basis.flat_row_removes);
    }
    jazz_model_DurabilityTier tier;
    if (JZ_IS_ERR(e = prepared_context(p, maintained_subscription, "coverage group subscription is missing prepared state", &tier, &read_view))) goto done;
    jazz_types_GlobalTime position = basis.known_membership_position;
    jazz_types_GlobalTime canonical_settled = JZP(PeerState__canonical_subscription_settlement_time)(p, node, maintained_subscription);
    bool any_positioned = false;
    {
        RME *m; void *unused;
        JZ_BTREE_FOR(current, m, unused) { jazz_types_GlobalTime s; if (JZP(subscription_state__member_settle_position)(m, &s)) { any_positioned = true; break; } }
    }
    if (facts_empty && !authorization_mismatch && basis.has_known_membership_position && canonical_settled > 0 && position >= canonical_settled) {
        /* nothing to add */
    } else if (facts_empty && !authorization_mismatch && basis.has_known_membership_position && any_positioned) {
        RME *m; void *unused;
        JZ_BTREE_FOR(current, m, unused) {
            jazz_types_GlobalTime settled;
            if (!JZP(subscription_state__member_settle_position)(m, &settled) || settled > position) rme_push_clone(&adds, m);
        }
    } else {
        RME *m; void *unused;
        JZ_BTREE_FOR(current, m, unused) rme_push_clone(&adds, m);
    }
    PublicationPlan plan;
    memset(&plan, 0, sizeof plan);
    if (JZ_IS_ERR(e = served_subscription_policy_binding(p, target_subscription, &plan.policy))) goto done;
    jazz_types_GlobalTime settled_through = maintained_publication_cut(p, node, maintained_subscription);
    const KSD *ks = basis.has_known_state ? &basis.known_state : NULL;
    bool catch_up_authorization_matches = fast_cursor_authorization_matches(p, target_subscription, ks);
    canonical_state = JZP(PeerState__state)(p, maintained_subscription);
    if (!canonical_state) { JZP(PolicyBinding__drop)(&plan.policy); e = JZN_INVALID_STORED("coverage group subscription is missing peer state"); goto done; }
    if (canonical_state->maintained_subscription_view && settled_through > 0) {
        const jazz_node_MaintainedSubscriptionView *mv = canonical_state->maintained_subscription_view->maintained;
        const jz_str *single_table = NULL; jazz_types_GlobalPhysicalTableId physical_table;
        bool has_single = JZN_MSV(MaintainedSubscriptionView__single_physical_table)(mv, &single_table, &physical_table);
        jzn_SupportingRowRefVec rows = {0};
        JZN_MSV(MaintainedSubscriptionView__supporting_rows)(mv, &rows);
        e = watermark_catch_up(node, ks, jazz_protocol__protocol__ReadViewSpec__is_default(read_view), has_single, single_table, physical_table,
                               jazz_model__query__ValidatedQuery__query(shape), &rows,
                               canonical_state->has_supporting_revision ? canonical_state->supporting_revision : NULL, catch_up_authorization_matches, &catch_up);
        jz_vec_drop(&rows, sizeof(rows.ptr[0]), NULL);
        if (JZ_IS_ERR(e)) { JZP(PolicyBinding__drop)(&plan.policy); goto done; }
    }
    if (catch_up) JZP(PeerState__settle_watermark_declaration)(p, target_subscription);
    jazz_types_GlobalTime leave_scan_after;
    bool has_leave = JZP(subscription_state__fast_current_membership_position)(ks, &leave_scan_after);
    bool plan_has_known;
    if (catch_up) {
        plan_has_known = basis.has_known_state;
        rme_vec_drop(&removes);
    } else {
        plan_has_known = !authorization_mismatch && basis.has_known_state;
    }
    /* A newly attached usage site starts with no subscription-scoped facts
     * even when it shares the canonical evaluator. Rehydrate the evaluator's
     * complete current fact closure. */
    plan.subscription = target_subscription;
    plan.tier = tier;
    plan.default_read_view = jazz_protocol__protocol__ReadViewSpec__is_default(read_view);
    plan.supporting_update = catch_up;
    catch_up = NULL;
    plan.settled_through = settled_through;
    plan.has_known_state = plan_has_known;
    if (plan_has_known) plan.known_state = basis.known_state;
    plan.has_leave_scan_after = has_leave;
    plan.leave_scan_after = leave_scan_after;
    plan.adds = adds;
    plan.removes = removes;
    memset(&adds, 0, sizeof adds);
    memset(&removes, 0, sizeof removes);
    const MSVS *maintained = JZP(PeerState__maintained_view)(p, maintained_subscription);
    if (!maintained) { plan_drop(&plan); e = JZN_INVALID_STORED("coverage group subscription is missing maintained state"); goto done; }
    SyncMsg *target_reset = NULL;
    if (JZ_IS_ERR(e = bundle_publication(p, node, shape, maintained_subscription, maintained->maintained, &plan, &target_reset))) goto done;
    jazz_protocol_ViewUpdatePayload *view = jazz_protocol__protocol__SyncMessage__as_view_update(target_reset);
    if (view) bind_cloned_supporting_revision(p, maintained_subscription, view);
    JZP(PeerState__record_outgoing_view_update_metadata)(p, target_reset);
    p->metrics.maintained_subscription_view->hits_out += 1;
    *out = target_reset;
done:
    if (catch_up) { jazz_protocol__protocol__SupportingRowsUpdate__drop(catch_up); jz_free(catch_up); }
    if (read_view) jazz_protocol__protocol__ReadViewSpec__drop(read_view);
    rme_vec_drop(&adds);
    rme_vec_drop(&removes);
    basis_drop(&basis);
    return e;
}

/* ---------------- Q-at-W watermark catch-up ---------------- */

/* Answer a "Q at W" declaration from the by_seq index when the view reads a
 * single table, so its membership depends only on each row's own image.
 * `revision` pins the successor to a canonical publisher's revision. */
static jz_err watermark_catch_up(NS *node, const KSD *known_state, bool default_read_view, bool has_single_table, const jz_str *table,
                                 jazz_types_GlobalPhysicalTableId physical_table, const jazz_model_Query *query, const jzn_SupportingRowRefVec *rows,
                                 const u8 *revision, bool authorization_matches, jazz_protocol_SupportingRowsUpdate **out)
{
    *out = NULL;
    if (!known_state || known_state->tag != JAZZ_KSD_WATERMARK) return JZ_OK;
    if (!default_read_view || !has_single_table) return JZ_OK;
    /* Unmoved rows keep their membership only when it depends on nothing but
     * the row itself: no window, join or aggregate over other rows. */
    if (!JZP(row_local_membership)(query)) return JZ_OK;
    const jazz_model_TableSchema *schema;
    JZ_TRY(jazz_node__node__NodeState__table(node, table, &schema));
    const jazz_model_Query *read_policy = schema->read_policy;
    if (read_policy && !JZP(row_local_membership)(read_policy)) return JZ_OK;
    /* Claims can change without any row moving, so a claims-dependent view
     * may skip unmoved rows only for a receiver that echoes this link's
     * current authorization progress. */
    if (!authorization_matches && (JZP(query_uses_claims)(query) || (read_policy && JZP(query_uses_claims)(read_policy)))) return JZ_OK;
    jazz_protocol_SupportingRowsUpdate *update = NULL;
    JZ_TRY(jazz_node__node__global_state__NodeState__supporting_catch_up_after(node, table, physical_table, rows->ptr, rows->len, known_state->position,
                                                                             known_state->supporting_revision, &update));
    if (revision && update->tag == JAZZ_SRU_CATCH_UP) memcpy(update->revision, revision, 16);
    *out = update;
    return JZ_OK;
}

/* A read policy with no conditions: readability never changes, so a catch-up
 * needs no authorization receipt. */
bool JZP(read_policy_admits_every_row)(const jazz_model_Query *policy)
{
    if (!(policy->joins.len == 0 && policy->flat_join == NULL && policy->policy_branches.len == 0 && policy->reachable.len == 0
          && policy->inherits.len == 0 && policy->relation == NULL))
        return false;
    for (u32 i = 0; i < policy->filters.len; i++) {
        const jazz_model_Predicate *f = &policy->filters.ptr[i];
        if (!(f->tag == JAZZ_PRED_ALL && f->children.len == 0)) return false;
    }
    return true;
}

/* Membership of a row in this query depends only on the row's own image. */
bool JZP(row_local_membership)(const jazz_model_Query *query)
{
    bool scalar_filters = query->filters.len == 0;
    if (!scalar_filters) {
        /* simple_scalar_exit_query(&Query { select: None, ..query.clone() }) */
        jazz_model_Query *unselected = jazz_model__query__Query__clone(query);
        if (unselected->has_select) {
            jz_vec_drop(&unselected->select, sizeof(jz_str), jzn_str_drop_v);
            memset(&unselected->select, 0, sizeof unselected->select);
            unselected->has_select = false;
        }
        scalar_filters = jazz_node__node__views__simple_scalar_exit_query(unselected);
        jazz_model__query__Query__drop(unselected);
    }
    return scalar_filters && query->joins.len == 0 && query->flat_join == NULL && query->policy_branches.len == 0 && query->reachable.len == 0
        && query->inherits.len == 0 && query->includes.len == 0 && query->array_subqueries.len == 0 && query->aggregate == NULL
        && !query->has_limit && query->offset == 0 && query->relation == NULL;
}

static bool operand_is_claim(const jazz_model_Operand *operand) { return operand->tag == JAZZ_OPERAND_CLAIM; }

static bool predicate_uses_claims(const jazz_model_Predicate *filter)
{
    switch (filter->tag) {
    case JAZZ_PRED_ALL:
    case JAZZ_PRED_ANY:
        for (u32 i = 0; i < filter->children.len; i++) if (predicate_uses_claims(&filter->children.ptr[i])) return true;
        return false;
    case JAZZ_PRED_NOT: return predicate_uses_claims(filter->inner);
    case JAZZ_PRED_EQ: case JAZZ_PRED_NE: case JAZZ_PRED_GT: case JAZZ_PRED_GTE: case JAZZ_PRED_LT: case JAZZ_PRED_LTE: case JAZZ_PRED_CONTAINS:
        return operand_is_claim(&filter->left) || operand_is_claim(&filter->right);
    case JAZZ_PRED_IN:
        if (operand_is_claim(&filter->left)) return true;
        for (u32 i = 0; i < filter->values.len; i++) if (operand_is_claim(&filter->values.ptr[i])) return true;
        return false;
    case JAZZ_PRED_ENUM_MATCH: return predicate_uses_claims(filter->inner);
    case JAZZ_PRED_IS_NULL: return operand_is_claim(&filter->left);
    default: JZ_UNREACHABLE();
    }
}

bool JZP(query_uses_claims)(const jazz_model_Query *query)
{
    for (u32 i = 0; i < query->filters.len; i++) if (predicate_uses_claims(&query->filters.ptr[i])) return true;
    return false;
}
