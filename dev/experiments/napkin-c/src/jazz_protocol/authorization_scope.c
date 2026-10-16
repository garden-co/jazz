/* authorization_scope.c: jazz_protocol::authorization_scope (crates/jazz/layers/protocol/src/authorization_scope.rs)
 * Rc<RefCell<State>> -> rc'd AuthScopeState (single-threaded; borrow checks elided). */
#include "jz_rt.h"
#include "jazz_protocol/protocol.h"
#include "jazz_protocol/authorization_scope.h"

/* ---------------- key orderings (derive(Ord)) ---------------- */
static int subscription_key_cmp(const void *pa, const void *pb, void *ctx) {
    (void)ctx;
    const jazz_protocol_SubscriptionKey *a = pa, *b = pb;
    int c = jz_uuid_cmp(&a->shape_id.u, &b->shape_id.u);
    if (c) return c;
    c = jz_uuid_cmp(&a->binding_id.u, &b->binding_id.u);
    if (c) return c;
    return jz_uuid_cmp(&a->read_view.id, &b->read_view.id);
}
static int clause_cmp(const void *pa, const void *pb, void *ctx) {
    (void)ctx;
    const jazz_protocol_SupportClause *a = pa, *b = pb;
    int c = jz_uuid_cmp(&a->shape.u, &b->shape.u);
    return c ? c : jz_uuid_cmp(&a->binding.u, &b->binding.u);
}
static bool clause_eq(const jazz_protocol_SupportClause *a, const jazz_protocol_SupportClause *b) { return clause_cmp(a, b, NULL) == 0; }
static int scope_key_cmp(const void *pa, const void *pb, void *ctx) {
    (void)ctx;
    const jazz_protocol_AuthorizationSupportScopeKey *a = pa, *b = pb;
    int c = memcmp(a->support_shape_digest, b->support_shape_digest, 32);
    if (c) return c;
    c = jazz_types__ids__AuthorSubject__Ord__cmp(&a->subject, &b->subject);
    if (c) return c;
    c = memcmp(a->claims_digest, b->claims_digest, 32);
    if (c) return c;
    return memcmp(a->policy_digest, b->policy_digest, 32);
}
static bool scope_key_eq(const jazz_protocol_AuthorizationSupportScopeKey *a, const jazz_protocol_AuthorizationSupportScopeKey *b) {
    return scope_key_cmp(a, b, NULL) == 0;
}

/* ---------------- AuthorityScopeAggregate ---------------- */
const jz_btree_desc jazz_protocol__authorization_scope__DESC_CLAUSE_SET = { sizeof(jazz_protocol_SupportClause), 0, clause_cmp, NULL, NULL };
static const jz_btree_desc DESC_SUB_CLAUSE = { sizeof(jazz_protocol_SubscriptionKey), sizeof(jazz_protocol_SupportClause), subscription_key_cmp, NULL, NULL };
static const jz_btree_desc DESC_SUB_APPLIED = { sizeof(jazz_protocol_SubscriptionKey), sizeof(jazz_protocol_AppliedBound), subscription_key_cmp, NULL, NULL };

void jazz_protocol__authorization_scope__AuthorityScopeAggregate__new(jazz_protocol_AuthorityScopeAggregate *out, jz_btree expected_support_moved) {
    out->expected_support = expected_support_moved;
    jz_btree_init(&out->members, &DESC_SUB_CLAUSE);
    jz_btree_init(&out->applied, &DESC_SUB_APPLIED);
}
void jazz_protocol__authorization_scope__AuthorityScopeAggregate__Clone__clone(jazz_protocol_AuthorityScopeAggregate *dst, const jazz_protocol_AuthorityScopeAggregate *src) {
    jz_btree_clone(&dst->expected_support, &src->expected_support, NULL, NULL);
    jz_btree_clone(&dst->members, &src->members, NULL, NULL);
    jz_btree_clone(&dst->applied, &src->applied, NULL, NULL);
}
void jazz_protocol__authorization_scope__AuthorityScopeAggregate__drop(jazz_protocol_AuthorityScopeAggregate *a) {
    jz_btree_drop(&a->expected_support);
    jz_btree_drop(&a->members);
    jz_btree_drop(&a->applied);
}
static bool members_have_clause(const jazz_protocol_AuthorityScopeAggregate *a, const jazz_protocol_SupportClause *clause) {
    jazz_protocol_SubscriptionKey *k; jazz_protocol_SupportClause *v;
    JZ_BTREE_FOR(&a->members, k, v) { (void)k; if (clause_eq(v, clause)) return true; }
    return false;
}
bool jazz_protocol__authorization_scope__AuthorityScopeAggregate__register(jazz_protocol_AuthorityScopeAggregate *a, jazz_protocol_SubscriptionKey sub, jazz_protocol_SupportClause clause) {
    if (!jz_btree_contains(&a->expected_support, &clause) || jz_btree_contains(&a->members, &sub) || members_have_clause(a, &clause))
        return false;
    jz_btree_insert(&a->members, &sub, &clause, NULL);
    return true;
}
void jazz_protocol__authorization_scope__AuthorityScopeAggregate__forget(jazz_protocol_AuthorityScopeAggregate *a, jazz_protocol_SubscriptionKey sub) {
    jz_btree_remove(&a->members, &sub, NULL, NULL);
    jz_btree_remove(&a->applied, &sub, NULL, NULL);
}
bool jazz_protocol__authorization_scope__AuthorityScopeAggregate__has_no_members(const jazz_protocol_AuthorityScopeAggregate *a) { return a->members.len == 0; }
bool jazz_protocol__authorization_scope__AuthorityScopeAggregate__apply(jazz_protocol_AuthorityScopeAggregate *a, jazz_protocol_SubscriptionKey sub, jazz_types_GlobalTime settled, u64 progress, jazz_protocol_AppliedBound *out) {
    if (!jz_btree_contains(&a->members, &sub)) return false;
    jazz_protocol_AppliedBound v = { settled, progress };
    jz_btree_insert(&a->applied, &sub, &v, NULL);
    return jazz_protocol__authorization_scope__AuthorityScopeAggregate__bounds(a, out);
}
bool jazz_protocol__authorization_scope__AuthorityScopeAggregate__bounds(const jazz_protocol_AuthorityScopeAggregate *a, jazz_protocol_AppliedBound *out) {
    if (a->members.len != a->expected_support.len) return false;
    jazz_protocol_SupportClause *expected; void *unit;
    JZ_BTREE_FOR(&a->expected_support, expected, unit) { (void)unit; if (!members_have_clause(a, expected)) return false; }
    jazz_protocol_SubscriptionKey *member; jazz_protocol_SupportClause *clause;
    JZ_BTREE_FOR(&a->members, member, clause) { (void)clause; if (!jz_btree_contains(&a->applied, member)) return false; }
    bool any = false;
    jazz_protocol_AppliedBound best = {0};
    jazz_protocol_SubscriptionKey *k; jazz_protocol_AppliedBound *v;
    JZ_BTREE_FOR(&a->applied, k, v) {
        (void)k;
        if (!any || v->settled < best.settled) best.settled = v->settled; /* min_by_key(settled.0): first minimum */
        if (!any || v->progress < best.progress) best.progress = v->progress;
        any = true;
    }
    if (!any) return false;
    *out = best;
    return true;
}

/* ---------------- AuthorityContext ---------------- */
bool jazz_protocol__authorization_scope__AuthorityContext__same_admitted_link(jazz_protocol_AuthorityContext a, jazz_protocol_AuthorityContext b) {
    return memcmp(a.authority, b.authority, 16) == 0 && jazz_types_AuthorSubject_eq(&a.link, &b.link)
        && a.connection_id == b.connection_id && a.connection_epoch == b.connection_epoch;
}
bool jazz_protocol__authorization_scope__AuthorityContext__PartialEq__eq(const jazz_protocol_AuthorityContext *a, const jazz_protocol_AuthorityContext *b) {
    return jazz_protocol__authorization_scope__AuthorityContext__same_admitted_link(*a, *b)
        && a->claims_revision == b->claims_revision && a->policy_epoch == b->policy_epoch
        && a->authorization_progress == b->authorization_progress && a->settled_through == b->settled_through;
}

/* ---------------- registry ---------------- */
enum { PHASE_IDLE = 0, PHASE_HYDRATING, PHASE_PROVEN };
typedef struct Entry {
    usize refs;
    u32 phase; u64 generation, owner;
    bool has_receipt; jazz_protocol_AuthorizationScopeReceipt receipt;
    jz_deque waiters; /* u64 */
    u64 next_generation;
} Entry;
static void entry_drop(void *p) { Entry *e = p; jz_deque_drop(&e->waiters, NULL); }
static const jz_btree_desc DESC_SCOPE_ENTRY = { sizeof(jazz_protocol_AuthorizationSupportScopeKey), sizeof(Entry), scope_key_cmp, NULL, entry_drop };
static void state_drop(void *p) { jazz_protocol_AuthScopeState *s = p; jz_btree_drop(&s->scopes); }

jazz_protocol_AuthorizationScopeRegistry jazz_protocol__authorization_scope__AuthorizationScopeRegistry__default(void) {
    jazz_protocol_AuthScopeState *s = jz_rc_new(sizeof *s, state_drop);
    jz_btree_init(&s->scopes, &DESC_SCOPE_ENTRY);
    s->next_lease = 0;
    return s;
}
jazz_protocol_AuthorizationScopeRegistry jazz_protocol__authorization_scope__AuthorizationScopeRegistry__clone(jazz_protocol_AuthorizationScopeRegistry r) { return jz_rc_clone(r); }
void jazz_protocol__authorization_scope__AuthorizationScopeRegistry__drop(jazz_protocol_AuthorizationScopeRegistry r) { jz_rc_release(r); }

static u64 start(Entry *e, u64 owner) {
    u64 generation = e->next_generation;
    e->next_generation += 1;
    e->phase = PHASE_HYDRATING;
    e->generation = generation;
    e->owner = owner;
    return generation;
}
static bool waiters_front_is(const Entry *e, u64 id) {
    const u64 *front = e->waiters.len ? jz_deque_at(&e->waiters, 0) : NULL;
    return front && *front == id;
}

bool jazz_protocol__authorization_scope__AuthorizationScopeRegistry__acquire(jazz_protocol_AuthorizationScopeRegistry r, jazz_protocol_AuthorizationSupportScopeKey key, jazz_protocol_AuthorizationScopeLease *lease, jazz_protocol_AuthorizationScopeAcquisition *acq) {
    jazz_protocol_AuthScopeState *s = r;
    if (!jz_btree_contains(&s->scopes, &key) && s->scopes.len >= JAZZ_MAX_AUTHORIZATION_SCOPES) return false;
    u64 id = s->next_lease;
    s->next_lease += 1;
    bool inserted;
    Entry *e = jz_btree_entry(&s->scopes, &key, &inserted);
    if (inserted) {
        /* Entry::default() */
        e->refs = 0;
        e->phase = PHASE_IDLE;
        e->has_receipt = false;
        jz_deque_init(&e->waiters, sizeof(u64));
        e->next_generation = 1;
    }
    e->refs += 1;
    bool has_owner = false;
    u64 owner_generation = 0;
    switch (e->phase) {
    case PHASE_IDLE: {
        u64 token = start(e, id);
        has_owner = true;
        owner_generation = token;
        acq->tag = JAZZ_ASA_OWNER;
        acq->token = token;
        break;
    }
    case PHASE_HYDRATING:
        jz_deque_push_back(&e->waiters, &id);
        acq->tag = JAZZ_ASA_WAITING;
        break;
    default:
        acq->tag = JAZZ_ASA_PROVEN;
        break;
    }
    lease->state = jz_rc_clone(s);
    lease->key = key;
    lease->id = id;
    lease->has_owner_generation = has_owner;
    lease->owner_generation = owner_generation;
    return true;
}
bool jazz_protocol__authorization_scope__AuthorizationScopeRegistry__take_promotion(jazz_protocol_AuthorizationScopeRegistry r, jazz_protocol_AuthorizationScopeLease *lease, jazz_protocol_AuthorizationScopeOwnerToken *out) {
    if (r != lease->state) return false;
    Entry *e = jz_btree_get(&r->scopes, &lease->key);
    if (!e) return false;
    if (e->phase != PHASE_IDLE || !waiters_front_is(e, lease->id)) return false;
    jz_deque_pop_front(&e->waiters, NULL);
    u64 token = start(e, lease->id);
    lease->has_owner_generation = true;
    lease->owner_generation = token;
    *out = token;
    return true;
}
static bool receipt_matches(const jazz_protocol_AuthorizationScopeReceipt *receipt, const jazz_protocol_AuthorityContext *ctx, u64 required_progress, u64 required_cut) {
    u64 progress = ctx->authorization_progress > required_progress ? ctx->authorization_progress : required_progress;
    u64 cut = ctx->settled_through > required_cut ? ctx->settled_through : required_cut;
    return memcmp(receipt->authority, ctx->authority, 16) == 0
        && jazz_types_AuthorSubject_eq(&receipt->link, &ctx->link)
        && receipt->authority_epoch == ctx->connection_epoch
        && receipt->claims_revision == ctx->claims_revision
        && receipt->policy_epoch == ctx->policy_epoch
        && receipt->authorization_progress >= progress
        && receipt->settled_through >= cut;
}
static jazz_protocol_AuthorizationScopeInstall retry_or_idle(Entry *e, jazz_protocol_AuthorizationScopeLease *lease, u64 token) {
    (void)token; /* the deterministic front waiter receives its token through take_promotion */
    e->has_receipt = false;
    e->phase = PHASE_IDLE;
    jazz_protocol_AuthorizationScopeInstall out = { JAZZ_ASI_WAITING, 0 };
    if (e->waiters.len == 0) {
        u64 retry = start(e, lease->id);
        lease->has_owner_generation = true;
        lease->owner_generation = retry;
        out.tag = JAZZ_ASI_RETRY;
        out.token = retry;
    } else {
        lease->has_owner_generation = false;
    }
    return out;
}
jazz_protocol_AuthorizationScopeInstall jazz_protocol__authorization_scope__AuthorizationScopeRegistry__install(jazz_protocol_AuthorizationScopeRegistry r, jazz_protocol_AuthorizationScopeLease *lease, jazz_protocol_AuthorizationScopeOwnerToken token, jazz_protocol_AuthorityContext ctx, const jazz_protocol_AuthorizationScopeReceipt *receipt) {
    jazz_protocol_AuthorizationScopeInstall rejected = { JAZZ_ASI_REJECTED, 0 };
    if (r != lease->state || !scope_key_eq(&receipt->key, &lease->key) || !lease->has_owner_generation || lease->owner_generation != token)
        return rejected;
    Entry *e = jz_btree_get(&r->scopes, &receipt->key);
    if (!e) return rejected;
    if (!(e->phase == PHASE_HYDRATING && e->generation == token && e->owner == lease->id)) return rejected;
    if (receipt_matches(receipt, &ctx, ctx.authorization_progress, ctx.settled_through)) {
        e->phase = PHASE_PROVEN;
        e->has_receipt = true;
        e->receipt = *receipt;
        lease->has_owner_generation = false;
        jazz_protocol_AuthorizationScopeInstall ok = { JAZZ_ASI_INSTALLED, 0 };
        return ok;
    }
    return retry_or_idle(e, lease, token);
}
jazz_protocol_AuthorizationScopeReadiness jazz_protocol__authorization_scope__AuthorizationScopeRegistry__receipt(jazz_protocol_AuthorizationScopeRegistry r, jazz_protocol_AuthorizationScopeLease *lease, jazz_protocol_AuthorityContext ctx, u64 required_progress, u64 required_cut) {
    jazz_protocol_AuthorizationScopeReadiness out;
    memset(&out, 0, sizeof out);
    out.tag = JAZZ_ASR_WAITING;
    if (r != lease->state) return out;
    Entry *e = jz_btree_get(&r->scopes, &lease->key);
    if (!e) return out;
    if (e->has_receipt) {
        if (receipt_matches(&e->receipt, &ctx, required_progress, required_cut)) {
            out.tag = JAZZ_ASR_PROVEN;
            out.receipt = e->receipt;
            return out;
        }
        e->has_receipt = false;
        e->phase = PHASE_IDLE;
    }
    if (e->phase == PHASE_IDLE && e->waiters.len == 0) {
        u64 token = start(e, lease->id);
        lease->has_owner_generation = true;
        lease->owner_generation = token;
        out.tag = JAZZ_ASR_OWNER;
        out.token = token;
    } else if (e->phase == PHASE_HYDRATING && e->owner == lease->id) {
        out.tag = JAZZ_ASR_OWNER;
        out.token = e->generation;
    }
    return out;
}
usize jazz_protocol__authorization_scope__AuthorizationScopeRegistry__len(jazz_protocol_AuthorizationScopeRegistry r) { return r->scopes.len; }
bool jazz_protocol__authorization_scope__AuthorizationScopeRegistry__is_empty(jazz_protocol_AuthorizationScopeRegistry r) { return r->scopes.len == 0; }

static bool waiter_keep(void *item, void *ctx) { return *(const u64 *)item != *(const u64 *)ctx; }
static void deque_retain(jz_deque *q, bool (*keep)(void *, void *), void *ctx) {
    u32 n = q->len;
    for (u32 i = 0; i < n; i++) {
        u64 v;
        jz_deque_pop_front(q, &v);
        if (keep(&v, ctx)) jz_deque_push_back(q, &v);
    }
}
void jazz_protocol__authorization_scope__AuthorizationScopeLease__Drop__drop(jazz_protocol_AuthorizationScopeLease *lease) {
    jazz_protocol_AuthScopeState *s = lease->state;
    Entry *e = jz_btree_get(&s->scopes, &lease->key);
    if (e) {
        if (!lease->has_owner_generation) {
            deque_retain(&e->waiters, waiter_keep, &lease->id);
        } else if (e->phase == PHASE_HYDRATING && lease->owner_generation == e->generation && e->owner == lease->id) {
            e->phase = PHASE_IDLE;
        }
        e->refs -= 1;
        if (e->refs == 0) jz_btree_remove(&s->scopes, &lease->key, NULL, NULL);
    }
    jz_rc_release(s);
}
