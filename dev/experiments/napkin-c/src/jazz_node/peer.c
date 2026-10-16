/* jazz_node::peer — per-peer server-side sync state outside the Jazz data
 * model. Owns shipped-complete-transaction-payload deduplication and
 * per-subscription incremental result maintenance for one downstream peer.
 * Rust: crates/jazz/layers/node/src/peer.rs. The `include!`d
 * peer/publication.rs, peer/known_state.rs and peer/repair.rs are the
 * translation units peer/publication.c, peer/known_state.c, peer/repair.c. */
#include "jazz_node/peer/peer.h"

/* static NEXT_PUBLICATION_OWNER: AtomicU64 = AtomicU64::new(1); (single-threaded wasm) */
static u64 NEXT_PUBLICATION_OWNER = 1;

/* ---------------- (AuthorSubject, BTreeMap<String, Value>) helpers ---------------- */
static void str_clone_v(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone(src); }

void JZP(claims_clone)(jz_btree *dst, const jz_btree *src)
{
    jz_btree_clone(dst, src, str_clone_v, groove__records__values__Value__clone);
}

void JZP(PolicyBinding__clone)(jazz_node_PolicyBinding *dst, const jazz_node_PolicyBinding *src)
{
    dst->identity = src->identity;
    JZP(claims_clone)(&dst->claims, &src->claims);
}

void JZP(PolicyBinding__drop)(jazz_node_PolicyBinding *b) { jz_btree_drop(&b->claims); }

bool JZP(PolicyBinding__eq)(const jazz_node_PolicyBinding *a, const jazz_types_AuthorSubject *identity, const jz_btree *claims)
{
    return jazz_types_AuthorSubject_eq(&a->identity, identity) && jz_btree_eq(&a->claims, claims, groove__records__values__Value__eq_v);
}

/* ---------------- PeerState ---------------- */
static int cmp_subkey_v(const void *a, const void *b, void *ctx) { return jazz_protocol_SubscriptionKey_cmp(a, b); }
static void state_ptr_drop_v(void *p) { JZP(PeerSubscriptionState__drop)(*(jazz_node_PeerSubscriptionState **)p); }
static const jz_btree_desc desc_publication_states = { sizeof(SubKey), sizeof(jazz_node_PeerSubscriptionState *), cmp_subkey_v, NULL, state_ptr_drop_v };
static const jz_btree_desc desc_known_states = { sizeof(SubKey), sizeof(jazz_protocol_KnownStateDeclaration), cmp_subkey_v, NULL, NULL };

void JZP(PeerState__default)(PS *out)
{
    memset(out, 0, sizeof *out);
    out->publication_owner = NEXT_PUBLICATION_OWNER++;
    /* The default is a standalone, SYSTEM-scoped peer helper. A real relay
     * can multiplex admitted sessions and must opt into the explicit
     * PeerState::relay() role, where every served subscription is required to
     * carry its immutable policy binding. */
    out->role.tag = JZN_PEER_ROLE_CLIENT_LINK;
    out->role.identity.tag = JAZZ_AUTHOR_SYSTEM;
    out->has_permission_identity = false;
    out->transport_capability.tag = JZN_RTC_ORDINARY_SESSION;
    out->authority_query_delegate = false;
    jz_btree_init(&out->shipped_complete_tx_payloads, &jzn_desc_txid_set);
    out->ship_complete_exclusive_payloads = false;
    jz_btree_init(&out->publication_states, &desc_publication_states);
    jz_btree_init(&out->downstream_known_states, &desc_known_states);
    out->has_announced_catalogue_fingerprint = false;
    out->metrics.maintained_subscription_view = JZ_NEW(jazz_node_MaintainedSubscriptionViewMetrics);
}

void JZP(PeerState__drop)(PS *p)
{
    if (p->transport_capability.tag == JZN_RTC_SCOPE_ISOLATED_CLIENT_RELAY) jz_btree_drop(&p->transport_capability.binding.claims);
    jz_btree_drop(&p->shipped_complete_tx_payloads);
    jz_btree_drop(&p->publication_states);
    jz_btree_drop(&p->downstream_known_states);
    jz_free(p->metrics.maintained_subscription_view);
}

jazz_node_PeerSubscriptionState *JZP(PeerState__state)(const PS *p, SubKey sub)
{
    jazz_node_PeerSubscriptionState **s = jz_btree_get(&p->publication_states, &sub);
    return s ? *s : NULL;
}

jazz_node_PeerSubscriptionState *JZP(PeerState__state_entry)(PS *p, SubKey sub)
{
    bool inserted;
    jazz_node_PeerSubscriptionState **s = jz_btree_entry(&p->publication_states, &sub, &inserted);
    if (inserted) *s = JZP(PeerSubscriptionState__default)();
    return *s;
}

const jazz_protocol_KnownStateDeclaration *JZP(PeerState__known_state)(const PS *p, SubKey sub)
{
    return jz_btree_get(&p->downstream_known_states, &sub);
}

/* ---------------- FullDiffFallbackMetrics ---------------- */
/* All full-diff fallbacks regardless of purpose. */
u64 JZP(FullDiffFallbackMetrics__total)(const jazz_node_FullDiffFallbackMetrics *m)
{
    return m->membership_reconciliations + m->query_reopens + m->authorization_support_reopens + m->runtime_resets;
}

/* Accumulate another peer's counters. */
void JZP(FullDiffFallbackMetrics__add)(jazz_node_FullDiffFallbackMetrics *m, jazz_node_FullDiffFallbackMetrics other)
{
    m->membership_reconciliations += other.membership_reconciliations;
    m->query_reopens += other.query_reopens;
    m->authorization_support_reopens += other.authorization_support_reopens;
    m->runtime_resets += other.runtime_resets;
}
