/* jazz_node::peer::subscription_state — peer-local subscription and
 * relay-authority support state.
 * Rust: crates/jazz/layers/node/src/peer/subscription_state.rs */
#include "peer.h"

bool JZP(subscription_state__fast_current_membership_position)(const jazz_protocol_KnownStateDeclaration *known_state, jazz_types_GlobalTime *out)
{
    if (!known_state) return false;
    switch (known_state->tag) {
    case JAZZ_KSD_FAST:
        if (known_state->completeness != JAZZ_KSC_FAST_CURRENT_MEMBERSHIP) return false;
        *out = known_state->position;
        return true;
    case JAZZ_KSD_FAST_WITH_AUTH:
        if (known_state->completeness != JAZZ_KSC_FAST_CURRENT_MEMBERSHIP) return false;
        *out = known_state->position;
        return true;
    case JAZZ_KSD_WATERMARK:
        *out = known_state->position;
        return true;
    case JAZZ_KSD_RESERVED2: /* retired: uninhabited */
    default:
        JZ_UNREACHABLE();
    }
}

bool JZP(subscription_state__fast_authorization_progress)(const jazz_protocol_KnownStateDeclaration *known_state, u64 *out)
{
    if (!known_state) return false;
    switch (known_state->tag) {
    case JAZZ_KSD_FAST_WITH_AUTH:
        if (known_state->completeness != JAZZ_KSC_FAST_CURRENT_MEMBERSHIP) return false;
        *out = known_state->authorization_progress;
        return true;
    case JAZZ_KSD_WATERMARK:
        if (!known_state->has_authorization_progress) return false;
        *out = known_state->authorization_progress;
        return true;
    case JAZZ_KSD_FAST:
        return false;
    case JAZZ_KSD_RESERVED2:
    default:
        JZ_UNREACHABLE();
    }
}

bool JZP(subscription_state__member_settle_position)(const jazz_protocol_ResultMemberEntry *member, jazz_types_GlobalTime *out)
{
    /* Row(row) | TypedRow { row, .. } => row.settle_position; Synthetic / PathTuple => None */
    if (member->tag != JAZZ_RME_ROW && member->tag != JAZZ_RME_TYPED_ROW) return false;
    const jazz_protocol_RealRowMemberEntry *row = jazz_protocol__protocol__ResultMemberEntry__as_real_row(member);
    return row && jazz_protocol__protocol__RealRowMemberEntry__settle_position(row, out);
}

bool JZP(subscription_state__fast_cursor_requires_authoritative_reset)(jazz_types_GlobalTime position, const jz_btree *previous, const jz_btree *current)
{
    return JZP(subscription_state__fast_cursor_membership_mismatch)(position, previous, current);
}

bool JZP(subscription_state__fast_cursor_membership_mismatch)(jazz_types_GlobalTime position, const jz_btree *previous, const jz_btree *current)
{
    jazz_protocol_ResultMemberEntry *m; void *unused;
    /* previous.difference(current).next().is_some() */
    JZ_BTREE_FOR(previous, m, unused) {
        if (!jz_btree_contains(current, m)) return true;
    }
    /* current.difference(previous).any(|m| settle_position(m).is_none_or(|s| s <= position)) */
    JZ_BTREE_FOR(current, m, unused) {
        if (jz_btree_contains(previous, m)) continue;
        jazz_types_GlobalTime settled;
        if (!JZP(subscription_state__member_settle_position)(m, &settled) || settled <= position) return true;
    }
    return false;
}

/* PeerRole::permission_subject: a relay is transport only and has no
 * principal; treating it as SYSTEM would turn topology into a policy bypass. */
bool JZP(subscription_state__PeerRole__permission_subject)(jazz_node_PeerRole role, jazz_types_AuthorSubject *out)
{
    switch (role.tag) {
    case JZN_PEER_ROLE_RELAY: return false;
    case JZN_PEER_ROLE_CLIENT_LINK: *out = role.identity; return true;
    default: JZ_UNREACHABLE();
    }
}

bool JZP(PeerRole__eq)(jazz_node_PeerRole a, jazz_node_PeerRole b)
{
    if (a.tag != b.tag) return false;
    return a.tag == JZN_PEER_ROLE_RELAY || jazz_types_AuthorSubject_eq(&a.identity, &b.identity);
}

/* ---------------- MemberIndexKey / MemberSlot ---------------- */
static int cmp_row_key(const jazz_node_RowKey *a, const jazz_node_RowKey *b)
{
    int c = jazz_types__object__OutputOccurrenceId__cmp(a->occurrence, b->occurrence);
    if (c) return c;
    c = jz_str_cmp(a->table, b->table);
    if (c) return c;
    return jz_uuid_cmp(&a->row, &b->row);
}

int JZP(MemberIndexKey__cmp)(const void *pa, const void *pb, void *ctx)
{
    const jazz_node_MemberIndexKey *a = pa, *b = pb;
    if (a->tag != b->tag) return a->tag < b->tag ? -1 : 1;
    switch (a->tag) {
    case JZN_MIK_ROW: return cmp_row_key(&a->row, &b->row);
    case JZN_MIK_SYNTHETIC: {
        int c = jz_str_cmp(&a->synthetic_table, &b->synthetic_table);
        return c ? c : jz_bytes_cmp(&a->synthetic_row, &b->synthetic_row);
    }
    case JZN_MIK_MEMBER: return jazz_protocol__protocol__ResultMemberEntry__cmp(&a->member, &b->member, ctx);
    default: JZ_UNREACHABLE();
    }
}

void JZP(MemberIndexKey__clone)(void *pdst, const void *psrc)
{
    jazz_node_MemberIndexKey *dst = pdst;
    const jazz_node_MemberIndexKey *src = psrc;
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    switch (src->tag) {
    case JZN_MIK_ROW:
        dst->row.occurrence = jazz_types__object__OutputOccurrenceId__clone(src->row.occurrence);
        dst->row.table = src->row.table;
        dst->row.row = src->row.row;
        break;
    case JZN_MIK_SYNTHETIC:
        dst->synthetic_table = jz_str_clone(&src->synthetic_table);
        dst->synthetic_row = jz_bytes_clone(&src->synthetic_row);
        break;
    case JZN_MIK_MEMBER:
        jazz_protocol__protocol__ResultMemberEntry__clone(&dst->member, &src->member);
        break;
    }
}

void JZP(MemberIndexKey__drop)(void *pk)
{
    jazz_node_MemberIndexKey *k = pk;
    switch (k->tag) {
    case JZN_MIK_ROW: jazz_types__object__OutputOccurrenceId__drop(k->row.occurrence); break;
    case JZN_MIK_SYNTHETIC: jz_str_drop(&k->synthetic_table); jz_bytes_drop(&k->synthetic_row); break;
    case JZN_MIK_MEMBER: jazz_protocol__protocol__ResultMemberEntry__drop(&k->member); break;
    }
}

void JZP(MemberSlot__clone)(void *pdst, const void *psrc)
{
    jazz_node_MemberSlot *dst = pdst;
    const jazz_node_MemberSlot *src = psrc;
    jazz_protocol__protocol__ResultMemberEntry__clone(&dst->member, &src->member);
    dst->refcount = src->refcount;
}

void JZP(MemberSlot__drop)(void *ps)
{
    jazz_protocol__protocol__ResultMemberEntry__drop(&((jazz_node_MemberSlot *)ps)->member);
}

const jz_btree_desc jzp_desc_member_index = { sizeof(jazz_node_MemberIndexKey), sizeof(jazz_node_MemberSlot), JZP(MemberIndexKey__cmp),
    JZP(MemberIndexKey__drop), JZP(MemberSlot__drop) };
const jz_btree_desc jzp_desc_member_set = { sizeof(jazz_protocol_ResultMemberEntry), 0, jazz_protocol__protocol__ResultMemberEntry__cmp,
    jazz_protocol__protocol__ResultMemberEntry__drop, NULL };
const jz_btree_desc jzp_desc_member_states = { sizeof(jazz_protocol_ResultMemberEntry), 2 * sizeof(bool), jazz_protocol__protocol__ResultMemberEntry__cmp,
    jazz_protocol__protocol__ResultMemberEntry__drop, NULL };

/* ---------------- CachedPeerQueryPlan ---------------- */
jazz_node_CachedPeerQueryPlan *JZP(CachedPeerQueryPlan__with_plan)(const jazz_protocol_RegisterShapeOptions *opts, jazz_node_PreparedQueryPlan *plan)
{
    return JZP(CachedPeerQueryPlan__with_context)(opts->tier, jazz_protocol__protocol__ReadViewSpec__clone(opts->read_view), plan);
}

jazz_node_CachedPeerQueryPlan *JZP(CachedPeerQueryPlan__with_context)(jazz_model_DurabilityTier tier, jazz_protocol_ReadViewSpec *read_view, jazz_node_PreparedQueryPlan *plan)
{
    jazz_node_CachedPeerQueryPlan *p = JZ_NEW(jazz_node_CachedPeerQueryPlan);
    p->tier = tier;
    p->read_view = read_view;
    p->plan = plan;
    return p;
}

bool JZP(CachedPeerQueryPlan__has_runtime_plan)(const jazz_node_CachedPeerQueryPlan *p) { return p->plan != NULL; }

void JZP(CachedPeerQueryPlan__replace_runtime_plan)(jazz_node_CachedPeerQueryPlan *p, jazz_node_PreparedQueryPlan *plan)
{
    if (p->plan) jz_rc_release(p->plan);
    p->plan = plan;
}

void JZP(CachedPeerQueryPlan__clear_runtime_plan)(jazz_node_CachedPeerQueryPlan *p)
{
    if (p->plan) jz_rc_release(p->plan);
    p->plan = NULL;
}

/* (tier, Arc::clone(&read_view)); the Arc is modelled as an owned clone. */
void JZP(CachedPeerQueryPlan__context)(const jazz_node_CachedPeerQueryPlan *p, jazz_model_DurabilityTier *tier, jazz_protocol_ReadViewSpec **read_view)
{
    *tier = p->tier;
    *read_view = jazz_protocol__protocol__ReadViewSpec__clone(p->read_view);
}

void JZP(CachedPeerQueryPlan__drop)(jazz_node_CachedPeerQueryPlan *p)
{
    if (!p) return;
    jazz_protocol__protocol__ReadViewSpec__drop(p->read_view);
    if (p->plan) jz_rc_release(p->plan);
    jz_free(p);
}

/* ---------------- MaintainedSubscriptionViewSubscription ---------------- */
void JZP(MaintainedSubscriptionViewSubscription__drop)(jazz_node_MaintainedSubscriptionViewSubscription *m)
{
    if (!m) return;
    groove__ivm__MultisinkSubscription__drop(m->subscription);
    JZN_MSV(MaintainedSubscriptionView__drop)(m->maintained);
    jazz_node_MaintainedTerminalSchemas_drop(m->terminal_schemas);
    if (m->tables) jz_rc_release(m->tables);
    if (m->covered_input_receiver) jazz_node__node__query_eval__maintained_views__CoveredInputReceiver__drop(m->covered_input_receiver);
    if (m->has_source_authority_result) jazz_protocol__protocol__AuthorityResultKey__drop(&m->source_authority_result);
    jz_free(m);
}

/* ---------------- PeerSubscriptionState ---------------- */
jazz_node_PeerSubscriptionState *JZP(PeerSubscriptionState__default)(void)
{
    jazz_node_PeerSubscriptionState *s = JZ_NEW(jazz_node_PeerSubscriptionState);
    jz_btree_init(&s->result_member_set, &jzp_desc_member_set);
    jz_btree_init(&s->member_index, &jzp_desc_member_index);
    s->local_authority = jazz_node__node__query_eval__local_authority_reconciliation__LocalAuthorityReconciliation__default();
    return s;
}

void JZP(PeerSubscriptionState__drop)(jazz_node_PeerSubscriptionState *s)
{
    if (!s) return;
    if (s->has_policy_binding) JZP(PolicyBinding__drop)(&s->policy_binding);
    if (s->has_authority_result_source) jazz_protocol__protocol__AuthorityResultKey__drop(&s->authority_result_source);
    jz_btree_drop(&s->result_member_set);
    jazz_node__node__query_eval__local_authority_reconciliation__LocalAuthorityReconciliation__drop(s->local_authority);
    jz_btree_drop(&s->member_index);
    JZP(MaintainedSubscriptionViewSubscription__drop)(s->maintained_subscription_view);
    JZP(CachedPeerQueryPlan__drop)(s->prepared_query);
    jz_free(s);
}

void JZP(PeerSubscriptionState__clear_groove_runtime_handles)(jazz_node_PeerSubscriptionState *s)
{
    s->has_supporting_revision = false;
    JZP(MaintainedSubscriptionViewSubscription__drop)(s->maintained_subscription_view);
    s->maintained_subscription_view = NULL;
    if (s->prepared_query) {
        /* The compiled plan belongs to one runtime, but its semantic context
         * remains the admitted context for this subscription. */
        JZP(CachedPeerQueryPlan__clear_runtime_plan)(s->prepared_query);
    }
    s->has_groove_runtime_token = false;
    s->has_physical_identity_generation = false;
}

/* Whether this state's runtime handles belong to a replaced Groove runtime
 * or a superseded physical identity generation. */
bool JZP(PeerSubscriptionState__runtime_is_stale)(const jazz_node_PeerSubscriptionState *s, const NS *node)
{
    return (s->has_groove_runtime_token && s->groove_runtime_token != jazz_node__node__NodeState__groove_runtime_token(node))
        || (s->has_physical_identity_generation
            && s->physical_identity_generation != jazz_node__node__NodeState__physical_identity_generation(node));
}

/* Record that the subscription's supporting rows at `revision` were shipped,
 * acknowledging the maintained view's source closure. */
void JZP(PeerSubscriptionState__acknowledge_published)(jazz_node_PeerSubscriptionState *s, const u8 revision[16])
{
    s->has_supporting_revision = true;
    memcpy(s->supporting_revision, revision, 16);
    if (s->maintained_subscription_view)
        JZN_MSV(MaintainedSubscriptionView__acknowledge_peer_source_closure)(s->maintained_subscription_view->maintained);
}

void JZP(PeerSubscriptionState__member_result_set)(const jazz_node_PeerSubscriptionState *s, jz_btree *out)
{
    jz_btree_clone(out, &s->result_member_set, jazz_protocol__protocol__ResultMemberEntry__clone, NULL);
}

void JZP(PeerSubscriptionState__supporting_rows)(const jazz_node_PeerSubscriptionState *s, jzn_SupportingRowRefVec *out)
{
    memset(out, 0, sizeof *out);
    if (s->maintained_subscription_view)
        JZN_MSV(MaintainedSubscriptionView__supporting_rows)(s->maintained_subscription_view->maintained, out);
}

void JZP(PeerSubscriptionState__previous_tx_ids)(const jazz_node_PeerSubscriptionState *s, jz_btree *out)
{
    jz_btree_init(out, &jzn_desc_txid_set);
    if (!s->has_supporting_revision || !s->maintained_subscription_view) return;
    jzn_SupportingRowRefVec rows = {0};
    JZN_MSV(MaintainedSubscriptionView__acknowledged_supporting_rows)(s->maintained_subscription_view->maintained, &rows);
    for (u32 i = 0; i < rows.len; i++) jz_btree_insert(out, &rows.ptr[i]->version.tx, NULL, NULL);
    jz_vec_drop(&rows, sizeof(rows.ptr[0]), NULL);
}
