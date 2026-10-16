/* jazz_node::peer — known-state / subscription lifecycle methods of PeerState.
 * Rust: crates/jazz/layers/node/src/peer/known_state.rs (include!d into peer.rs). */
#include "peer.h"

/* Drops only the per-subscription result_set cache. Version payload dedup is
 * per-peer and survives subscription rehydration. */
void JZP(PeerState__forget_subscription)(PS *p, SubKey sub)
{
    jz_btree_remove(&p->publication_states, &sub, NULL, NULL);
    jz_btree_remove(&p->downstream_known_states, &sub, NULL, NULL);
}

/* Record a downstream known-state declaration for a usage-site subscription. */
void JZP(PeerState__declare_known_state)(PS *p, SubKey sub, const jazz_protocol_KnownStateDeclaration *declaration /* NULL = None */)
{
    if (declaration) jz_btree_insert(&p->downstream_known_states, &sub, declaration, NULL);
    else jz_btree_remove(&p->downstream_known_states, &sub, NULL, NULL);
}

/* A watermark is answered once: afterwards the receiver holds a new revision,
 * so later rehydrates only reuse the position for body dedupe. */
void JZP(PeerState__settle_watermark_declaration)(PS *p, SubKey sub)
{
    jazz_protocol_KnownStateDeclaration *current = jz_btree_get(&p->downstream_known_states, &sub);
    if (!current || current->tag != JAZZ_KSD_WATERMARK) return;
    jazz_protocol_KnownStateDeclaration settled = {0};
    settled.completeness = JAZZ_KSC_FAST_CURRENT_MEMBERSHIP;
    settled.position = current->position;
    if (current->has_authorization_progress) {
        settled.tag = JAZZ_KSD_FAST_WITH_AUTH;
        settled.has_authorization_progress = true;
        settled.authorization_progress = current->authorization_progress;
    } else {
        settled.tag = JAZZ_KSD_FAST;
    }
    jz_btree_insert(&p->downstream_known_states, &sub, &settled, NULL);
}

/* Advance retained per-binding authorization generations after this reader's
 * authority is rebuilt. */
void JZP(PeerState__advance_authorization_progress)(PS *p)
{
    SubKey *k; jazz_node_PeerSubscriptionState **state;
    JZ_BTREE_FOR(&p->publication_states, k, state) {
        if ((*state)->authorization_progress == UINT64_MAX) jz_panic("authorization progress overflow must stop reset suppression");
        (*state)->authorization_progress += 1;
    }
}

u64 JZP(PeerState__authorization_progress_for_subscription)(const PS *p, SubKey sub)
{
    jazz_node_PeerSubscriptionState *state = JZP(PeerState__state)(p, sub);
    return state ? state->authorization_progress : 0;
}

/* Carry an already-served authorization generation to a replacement
 * maintained usage. Claim rebinding changes the coverage-group key, but does
 * not reset the concrete downstream usages' generation. */
void JZP(PeerState__retain_authorization_progress_for_subscription)(PS *p, SubKey sub, u64 authorization_progress)
{
    jazz_node_PeerSubscriptionState *state = JZP(PeerState__state_entry)(p, sub);
    state->authorization_progress = authorization_progress;
    state->has_served_authorization_progress = true;
}

/* Drop one subscription and eagerly unregister any maintained Groove
 * subscription from the runtime before dropping the receiver. */
bool JZP(PeerState__forget_subscription_with_node)(PS *p, NS *node, SubKey sub)
{
    jazz_node_PeerSubscriptionState *state = NULL;
    SubKey removed_key;
    if (!jz_btree_remove(&p->publication_states, &sub, &removed_key, &state)) {
        jz_btree_remove(&p->downstream_known_states, &sub, NULL, NULL);
        /* ensure_query_subscription_registered may have installed the
         * node-side shape before this peer had enough state to create a
         * maintained receiver. Retire that owner too; the node-side removal is
         * idempotent for an already-forgotten publication. */
        jazz_node__node__query_eval__subscriptions__NodeState__release_query_subscription_for_peer(node, p->publication_owner, sub);
        return false;
    }
    jazz_protocol_PolicyBindingKey *admitted_policy_binding = NULL;
    if (state->has_policy_binding) {
        jz_btree claims;
        JZP(claims_clone)(&claims, &state->policy_binding.claims);
        admitted_policy_binding = jazz_protocol__protocol__PolicyBindingKey__from_canonical_parts(state->policy_binding.identity, claims);
    }
    jz_btree_remove(&p->downstream_known_states, &sub, NULL, NULL);
    bool unsubscribed = false;
    if (state->has_groove_runtime_token && state->groove_runtime_token == jazz_node__node__NodeState__groove_runtime_token(node)) {
        jazz_node_MaintainedSubscriptionViewSubscription *maintained = state->maintained_subscription_view;
        state->maintained_subscription_view = NULL;
        if (maintained) {
            unsubscribed = jazz_node__node__NodeState__unsubscribe_groove_subscription(node, groove__ivm__MultisinkSubscription__id(maintained->subscription));
            JZP(MaintainedSubscriptionViewSubscription__drop)(maintained);
        }
    }
    JZP(PeerSubscriptionState__drop)(state);
    if (admitted_policy_binding) {
        jazz_node__node__query_eval__subscriptions__NodeState__release_query_subscription_for_peer_with_admitted_policy_binding(node, p->publication_owner, sub,
                                                                                                                             admitted_policy_binding);
    } else {
        jazz_node__node__query_eval__subscriptions__NodeState__release_query_subscription_for_peer(node, p->publication_owner, sub);
    }
    return unsubscribed;
}

static SubKey default_view_subscription(const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding)
{
    SubKey k;
    memset(&k, 0, sizeof k);
    k.shape_id = jazz_model__query__ValidatedQuery__shape_id(shape);
    k.binding_id = jazz_model__query__Binding__binding_id(binding);
    /* read_view: Default::default() */
    return k;
}

/* Drop one query-binding result set on this peer. */
void JZP(PeerState__forget_query_binding)(PS *p, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding)
{
    JZP(PeerState__forget_subscription)(p, default_view_subscription(shape, binding));
}

bool JZP(PeerState__forget_query_binding_with_node)(PS *p, NS *node, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding)
{
    return JZP(PeerState__forget_subscription_with_node)(p, node, default_view_subscription(shape, binding));
}

/* Return transaction refs whose complete payload bundles have shipped on this peer. */
const jz_btree *JZP(PeerState__shipped_complete_tx_payloads)(const PS *p) { return &p->shipped_complete_tx_payloads; }

/* Complete-payload inventory refs are only safe once the receiver has
 * explicitly acknowledged durable application. Until the protocol grows that
 * ack, every served update must carry the required bundles again. */
void JZP(PeerState__acknowledged_complete_tx_payloads)(const PS *p, jz_btree *out)
{
    jz_btree_init(out, &jzn_desc_txid_set);
}

void JZP(PeerState__set_ship_complete_exclusive_payloads)(PS *p, bool enabled) { p->ship_complete_exclusive_payloads = enabled; }

/* Forget complete-tx payload dedup markers for transactions whose local
 * payloads were evicted, so a standard rehydrate may resend them. */
usize JZP(PeerState__forget_evicted_versions)(PS *p, const jazz_model_TxId *tx_ids, u32 n)
{
    usize count = 0;
    for (u32 i = 0; i < n; i++)
        if (jz_btree_remove(&p->shipped_complete_tx_payloads, &tx_ids[i], NULL, NULL)) count++;
    return count;
}

/* Return current result_set for one subscription (false = None). */
bool JZP(PeerState__subscription_result_sets)(const PS *p, SubKey sub, jz_btree *out)
{
    jazz_node_PeerSubscriptionState *state = JZP(PeerState__state)(p, sub);
    if (!state) return false;
    JZP(PeerSubscriptionState__previous_tx_ids)(state, out);
    return true;
}

jazz_node_MaintainedSubscriptionViewMetrics JZP(PeerState__maintained_subscription_view_metrics)(const PS *p)
{
    return *p->metrics.maintained_subscription_view;
}

/* Inspect the current retained index footprint of one maintained
 * subscription. O(subscription size); never called by publication. Returns
 * false (None) when the subscription has no maintained view. */
bool JZP(PeerState__inspect_maintained_subscription_view_footprint)(const PS *p, SubKey sub, jazz_node_MaintainedSubscriptionViewFootprint *out)
{
    const jazz_node_MaintainedSubscriptionViewSubscription *maintained = JZP(PeerState__maintained_view)(p, sub);
    if (!maintained) return false;
    JZN_MSV(MaintainedSubscriptionView__footprint)(maintained->maintained, out);
    return true;
}
