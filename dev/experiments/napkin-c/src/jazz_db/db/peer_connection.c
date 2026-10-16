/* peer_connection.c: port of crates/jazz/layers/db/src/db/peer_connection.rs
 *
 * Per-peer synchronization, repair, and resume machinery.
 *
 * A peer connection binds transport state to either an upstream authority or
 * a served subscriber. It applies and emits sync messages, tracks coverage,
 * performs bounded repair, and preserves authenticated reconnect state.
 *
 * Port notes: `PeerConnection::tick` is plain synchronous C. Every
 * `node.lock().await` is `jazz_db_node_lock`/`jazz_db_node_unlock` around
 * the call; `node.borrow()` is `jazz_db__db__LocalMutexBorrow__borrow`. The
 * boxed futures of the Rust tick (subscriber dispatch, authority view
 * application) are direct calls. */
#include "peer_connection.h"

#define PC_FN(name) jazz_db__db__peer_connection__PeerConnection__##name
#define NS_FN(name) jazz_node__node__NodeState__##name
#define PEER_FN(name) jazz_node__peer__PeerState__##name
typedef jazz_db_PeerConnection PC;

static void str_key_clone(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone((const jz_str *)src); }
static jz_err err_clone(const jz_err *e) {
    if (!e->code) return JZ_OK;
    if (!e->msg) return (jz_err){e->code, NULL};
    return jz_err_fmt(e->code, "%.*s", (int)e->msg->len, e->msg->ptr);
}
static void claims_clone(jz_btree *dst, const jz_btree *src) { jz_btree_clone(dst, src, str_key_clone, groove__records__Value__clone); }
static void policy_binding_from(jazz_db_PolicyBinding *out, jazz_types_AuthorSubject identity, const jz_btree *claims) {
    out->identity = identity;
    claims_clone(&out->claims, claims);
}
static jz_err node_err(jz_err e) { return jazz_node__node__api_error__Error__From__from(e); }
static jz_err protocol_err(const char *msg) { return jz_err_new(JAZZ_EC_PROTOCOL, msg); }
static void sync_message_vec_drop(void *p) { jazz_protocol__protocol__SyncMessage__drop(p); }
static void published_ptr_drop(void *slot) { jazz_node__node__PublishedTransaction__drop(*(void **)slot); }

/* ---------------------------------------------------------------- descriptors */

static int cmp_subscription_key(const void *a, const void *b, void *ctx) { return jazz_protocol__protocol__SubscriptionKey__cmp(a, b, ctx); }
static int cmp_txid(const void *a, const void *b, void *ctx) { return jazz_model__tx__TxId__cmp(a, b, ctx); }
static void pending_upstream_subscription_drop(void *p) { jazz_db__db__PendingUpstreamSubscription__drop(p); }
static void upload_deque_drop(void *p) { jz_deque_drop((jz_deque *)p, jazz_db__db__peer_connection__PendingLargeValueUpload__drop); }

const jz_btree_desc jazz_db_sent_subscriptions_desc = { sizeof(jazz_protocol_SubscriptionKey), sizeof(jazz_db_PendingUpstreamSubscription), cmp_subscription_key, NULL, pending_upstream_subscription_drop };
const jz_btree_desc jazz_db_subscription_global_time_desc = { sizeof(jazz_protocol_SubscriptionKey), sizeof(jazz_types_GlobalTime), cmp_subscription_key, NULL, NULL };
const jz_btree_desc jazz_db_author_u64_desc = { sizeof(jazz_types_AuthorSubject), sizeof(u64), jazz_types__ids__AuthorSubject__cmp, NULL, NULL };
const jz_btree_desc jazz_db_txid_large_value_ref_desc = { sizeof(jazz_model_TxId), sizeof(groove_LargeValueRef), cmp_txid, NULL, NULL };
const jz_btree_desc jazz_db_pending_catalogue_subscriptions_desc = { sizeof(jazz_protocol_SubscriptionKey), sizeof(jazz_db_PendingCatalogueSubscription), cmp_subscription_key, NULL, jazz_db__db__peer_connection__PendingCatalogueSubscription__drop };
const jz_btree_desc jazz_db_large_value_upload_queues_desc = { sizeof(jazz_model_TxId), sizeof(jz_deque), cmp_txid, NULL, upload_deque_drop };

/* ---------------------------------------------------------------- relay rejections */

/// Both wire rejections and receiver validation failures terminate the same
/// downstream usages. A relay has no public SubscriptionList of its own.
static u32 queue_relay_subscription_rejection(jz_btree *owners, jz_btree *pending, jazz_db_PendingUpstreamCommands *upstream,
                                              jazz_protocol_SubscriptionKey subscription, const jazz_protocol_SubscribeRejectReason *reason) {
    jazz_db_KeyedRelayOwnerVec removed;
    jazz_db__db__node_runtime__take_relay_upstream_subscription_owner(owners, subscription, &removed);
    u32 count = removed.len;
    jz_btree retired;
    jz_btree_init(&retired, &jazz_db_subscription_key_set_desc);
    for (u32 i = 0; i < removed.len; ++i) {
        jazz_db_KeyedRelayOwner *item = &removed.ptr[i];
        jz_btree_insert(&retired, &item->subscription, NULL, NULL);
        bool inserted = false;
        jz_deque *queue = (jz_deque *)jz_btree_entry(pending, &item->owner.downstream_connection_epoch, &inserted);
        if (inserted) jz_deque_init(queue, sizeof(jazz_db_RelaySubscriptionRejection));
        jazz_db_RelaySubscriptionRejection rejection;
        rejection.coverage = item->owner.coverage;
        rejection.policy_binding = item->owner.policy_binding;
        rejection.downstream_subscriptions = item->owner.downstream_subscriptions;
        jazz_protocol__protocol__SubscribeRejectReason__clone(&rejection.reason, reason);
        jz_deque_push_back(queue, &rejection);
        /* The remaining owner fields were not moved into the rejection. */
        jazz_db__db__PendingUpstreamSubscription__drop(&item->owner.request);
        jazz_db__db__ScalarReconciliation__drop(&item->owner.scalar_reconciliation);
    }
    jz_vec_drop(&removed, sizeof(jazz_db_KeyedRelayOwner), NULL);
    jazz_protocol_SubscriptionKey *key;
    void *unit;
    JZ_BTREE_FOR(&retired, key, unit) {
        (void)unit;
        jazz_db_PendingUpstreamCommand *command = (jazz_db_PendingUpstreamCommand *)jz_vec_push_slot(upstream, sizeof(jazz_db_PendingUpstreamCommand));
        memset(command, 0, sizeof(*command));
        command->tag = JAZZ_PUC_UNSUBSCRIBE;
        command->unsubscribe = *key;
    }
    jz_btree_drop(&retired);
    return count;
}

u32 jazz_db__db__peer_connection__route_subscription_refresh_failure(jazz_db_SubscriptionList *subscriptions, const jz_err *error) {
    jz_str shown = {0};
    jazz_node__node__api_error__Error__display(&shown, error);
    jz_trace_event(0, "stderr", "jazz subscription refresh failed: %.*s", (int)shown.len, shown.ptr);
    jz_str_drop(&shown);
    u32 delivered = 0;
    for (u32 i = 0; i < subscriptions->len; ++i) {
        jazz_db_SubscriptionState *state = jazz_db_weak_upgrade(subscriptions->ptr[i]);
        if (!state) continue;
        if (!*state->closed) {
            jazz_protocol_SubscribeRejectReason reason;
            jazz_protocol__protocol__SubscribeRejectReason__server_failure(&reason, JAZZ_SERVER_FAILURE_INTERNAL);
            jz_err e = jazz_db__first_load__SubscriptionState__send_rejection(state, reason);
            if (e.code == 0) delivered += 1; else jz_err_drop(&e);
        }
        jz_rc_release(state);
    }
    return delivered;
}

/// Deliver a receiver-side authority-closure rejection only to the public
/// usage site named by the malformed frame. The local node has already
/// rejected the complete batch before this point, so this does not turn a
/// protocol violation into a partial result or receipt.
static u32 route_invalid_authority_source_closure(jazz_db_SubscriptionList *subscriptions, const jz_err *error) {
    jazz_protocol_SubscriptionKey subscription;
    const void *transition = NULL;
    if (!jazz_node__node__Error__as_invalid_authority_source_closure(error, &subscription, &transition)) return 0;
    jazz_protocol_SubscribeRejectReason reason;
    jazz_protocol__protocol__SubscribeRejectReason__invalid_authority_source_closure(&reason, transition);
    u32 delivered = 0;
    for (u32 i = 0; i < subscriptions->len; ++i) {
        jazz_db_SubscriptionState *state = jazz_db_weak_upgrade(subscriptions->ptr[i]);
        if (!state) continue;
        bool names = false;
        for (u32 h = 0; h < state->upstream_subscription_handles.len; ++h)
            if (jazz_protocol_SubscriptionKey_eq(&state->upstream_subscription_handles.ptr[h].subscription, &subscription)) { names = true; break; }
        if (!*state->closed && names) {
            jazz_protocol_SubscribeRejectReason r;
            jazz_protocol__protocol__SubscribeRejectReason__clone(&r, &reason);
            jz_err e = jazz_db__first_load__SubscriptionState__send_rejection(state, r);
            if (e.code == 0) delivered += 1; else jz_err_drop(&e);
        }
        jz_rc_release(state);
    }
    jazz_protocol__protocol__SubscribeRejectReason__drop(&reason);
    return delivered;
}

/* ---------------------------------------------------------------- subscription keys */

/// Namespace for relay-owned usage-site subscription handles.
///
/// A new coverage lifetime mints a fresh handle. Identical live scope-relay
/// requests reuse that handle and retain independent connection pins; after
/// final release, a later lifetime cannot inherit the retired stream's replies.
static const jz_uuid RELAY_UPSTREAM_SUBSCRIPTION_NAMESPACE = {{0xae, 0x3e, 0xb9, 0xf7, 0x65, 0xcc, 0x52, 0x8d, 0x8f, 0x3e, 0xa7, 0x72, 0xfb, 0x6f, 0x68, 0xfe}};

/// Namespace for the maintained receiver owned by one policy-partitioned
/// coverage group. Relayed policy snapshots need a distinct runtime receiver
/// even when their query bindings are identical.
static const jz_uuid COVERAGE_GROUP_SUBSCRIPTION_NAMESPACE = {{0x19, 0xfd, 0xc8, 0x30, 0x2d, 0xd8, 0x58, 0x76, 0xae, 0x31, 0xa8, 0xf5, 0x26, 0x51, 0x2a, 0xc5}};

static void serialize_claims(jz_pc_w *w, const jz_btree *claims) {
    jz_pc_seq(w, claims->len);
    jz_str *k;
    groove_Value *v;
    JZ_BTREE_FOR(claims, k, v) {
        jz_pc_str(w, k);
        groove__records__values__Value__Serialize__serialize(w, v);
    }
}

static jazz_protocol_SubscriptionKey relay_upstream_subscription_key(u64 connection_epoch, jazz_protocol_SubscriptionKey downstream,
                                                                     jazz_protocol_ReadViewKey upstream_read_view, const jazz_db_PolicyBinding *policy_binding) {
    /* An upstream relay usage site is admitted under this exact snapshot. A
     * direct downstream claim refresh must therefore get a fresh opaque
     * handle rather than reusing a wire subscription whose authority has
     * retained the old delegated context. */
    jz_bytes identity = {0};
    jz_pc_w w = { &identity };
    jz_pc_varint(&w, connection_epoch);
    jazz_protocol__protocol__SubscriptionKey__Serialize__serialize(&w, &downstream);
    jazz_protocol__protocol__ReadViewKey__Serialize__serialize(&w, &upstream_read_view);
    jazz_types__ids__AuthorSubject__Serialize__serialize(&w, &policy_binding->identity);
    serialize_claims(&w, &policy_binding->claims);
    jazz_protocol_SubscriptionKey key;
    key.shape_id = downstream.shape_id;
    key.binding_id.u = jz_uuid_v5(&RELAY_UPSTREAM_SUBSCRIPTION_NAMESPACE, identity.ptr, identity.len);
    key.read_view = upstream_read_view;
    jz_bytes_drop(&identity);
    return key;
}

static void relay_authority_coverage_key(const jazz_db_CoverageKey *coverage, jazz_db_CoverageKey *out) {
    jazz_db__db__CoverageKey__clone(out, coverage);
    jazz_protocol__protocol__RegisterShapeOptions__drop(&out->opts);
    jazz_db__db__upstream_register_shape_options(&out->opts, coverage->opts.tier, jazz_protocol__protocol__ReadViewSpec__clone(coverage->opts.read_view), JAZZ_DUR_GLOBAL);
    out->opts.binding_source = JAZZ_BINDING_SOURCE_RELAY_AUTHORITY_SESSION;
}

jazz_protocol_SubscriptionKey jazz_db__db__peer_connection__coverage_group_subscription_key(const jazz_db_CoverageKey *coverage) {
    jazz_model_BindingId binding_id = coverage->binding_id;
    if (coverage->has_policy_binding) {
        jz_bytes identity = {0};
        jz_pc_w w = { &identity };
        jz_pc_uuid(&w, &coverage->binding_id.u);
        jazz_protocol__protocol__PolicyBindingKey__Serialize__serialize(&w, &coverage->policy_binding);
        binding_id.u = jz_uuid_v5(&COVERAGE_GROUP_SUBSCRIPTION_NAMESPACE, identity.ptr, identity.len);
        jz_bytes_drop(&identity);
    }
    jazz_protocol_SubscriptionKey key;
    key.shape_id = coverage->shape_id;
    key.binding_id = binding_id;
    key.read_view = jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&coverage->opts);
    return key;
}

/* ---------------------------------------------------------------- publication outcomes */

static jz_err finish_peer_publication_outcome_with_refresh(jazz_db_NodeMutex *node, jazz_db_SubscriptionList *subscriptions, jazz_db_ActiveAuthorityViewReceipts *receipts,
                                                          const jz_waker *progress_waker, jazz_node_PublicationOutcome outcome, bool refresh,
                                                          u32 *changed_out, bool *published_any_out) {
    u32 changed = 0;
    bool published_any = false;
    for (;;) {
        if (outcome.publications.len != 0) {
            published_any = true;
            u32 n = outcome.publications.len;
            jz_err *persisted = (jz_err *)jz_zalloc(sizeof(jz_err) * (n ? n : 1));
            for (u32 i = 0; i < n; ++i) persisted[i] = jazz_node__node__PublishedTransaction__persist(outcome.publications.ptr[i]);
            jazz_node_NodeState *state = jazz_db_node_lock(node);
            jz_err failed = JZ_OK;
            for (u32 i = 0; i < n; ++i) {
                if (failed.code) { jz_err_drop(&persisted[i]); continue; }
                jazz_model_TxId tx_id = jazz_node__node__PublishedTransaction__tx_id(outcome.publications.ptr[i]);
                jz_err e = NS_FN(settle_published_transaction)(state, tx_id, persisted[i]);
                if (e.code) failed = node_err(e);
            }
            jazz_db_node_unlock(node);
            jz_free(persisted);
            jz_vec_drop(&outcome.publications, sizeof(jazz_node_PublishedTransaction *), published_ptr_drop);
            memset(&outcome.publications, 0, sizeof(outcome.publications));
            if (failed.code) { jazz_node__node__PublicationOutcome__drop(&outcome); return failed; }
            if (refresh) {
                u32 refreshed = 0;
                jz_err e = jazz_db__db__node_runtime__refresh_subscriptions_in(node, subscriptions, receipts, progress_waker, &refreshed);
                if (e.code) { refreshed = jazz_db__db__peer_connection__route_subscription_refresh_failure(subscriptions, &e); jz_err_drop(&e); }
                changed += refreshed;
            }
        }
        jazz_protocol_SyncMessage message;
        if (!jz_deque_pop_front(&outcome.post_settlement_work, &message)) break;
        jazz_node_PublicationOutcome next;
        jazz_node_CommitUnitIngestContext ctx = { jazz_types__ids__AuthorSubject__SYSTEM, JAZZ_TRUST_TRUSTED_BACKEND, false, false };
        jazz_node_NodeState *state = jazz_db_node_lock(node);
        jz_err e = NS_FN(apply_sync_message_with_ingest_context)(state, message, &ctx, &next);
        jazz_db_node_unlock(node);
        if (e.code) { jazz_node__node__PublicationOutcome__drop(&outcome); return node_err(e); }
        outcome.publications = next.publications;
        memset(&next.publications, 0, sizeof(next.publications));
        jazz_protocol_SyncMessage more;
        while (jz_deque_pop_front(&next.post_settlement_work, &more)) jz_deque_push_back(&outcome.post_settlement_work, &more);
        jazz_node__node__PublicationOutcome__drop(&next);
    }
    jazz_node__node__PublicationOutcome__drop(&outcome);
    if (changed_out) *changed_out = changed;
    if (published_any_out) *published_any_out = published_any;
    return JZ_OK;
}

static jz_err finish_peer_publication_outcome(jazz_db_NodeMutex *node, jazz_db_SubscriptionList *subscriptions, jazz_db_ActiveAuthorityViewReceipts *receipts,
                                             const jz_waker *progress_waker, jazz_node_PublicationOutcome outcome, u32 *changed) {
    return finish_peer_publication_outcome_with_refresh(node, subscriptions, receipts, progress_waker, outcome, true, changed, NULL);
}

/* ---------------------------------------------------------------- subscriber dispatch */

/// An internal system capability may forward a node-specific durable origin;
/// every externally admitted identity must match its provenance exactly.
static bool admitted_provenance_matches(jazz_types_AuthorSubject admitted, jazz_types_AuthorSubject made_by) {
    if (admitted.tag == JAZZ_AUTHOR_SYSTEM) return made_by.tag == JAZZ_AUTHOR_SYSTEM_AT;
    return jazz_types_AuthorSubject_eq(made_by, admitted);
}

/// Dispatch one admitted subscriber message into node ingest.
///
/// In Rust this is one boxed suspension boundary at the peer/control-plane
/// seam; in the synchronous port it is a plain call.
static jz_err dispatch_admitted_subscriber_message(jazz_db_NodeMutex *node, jazz_node_PeerState *peer, bool local_receiver,
                                                  jazz_node_CommitUnitIngestContext ingest_context, jazz_db_PolicyBinding session_claim_binding,
                                                  jz_btree *local_fate_routes, jazz_db_SyncMessageVec *downstream_fates,
                                                  const jz_waker *progress_waker, jazz_protocol_SyncMessage message, jazz_node_PublicationOutcome *out) {
    (void)progress_waker;
    jz_err err = JZ_OK;
    jazz_node_PeerRole role = PEER_FN(role)(peer);
    if (message.tag == JAZZ_SM_COMMIT_UNIT && role.tag == JAZZ_PEER_ROLE_CLIENT_LINK) {
        jazz_node_NodeState *n = jazz_db_node_lock(node);
        jz_err e = NS_FN(require_staged_large_values_for_versions)(n, &message.v.commit_unit.versions);
        jazz_db_node_unlock(node);
        if (e.code) { err = node_err(e); goto fail; }
    }
    if (message.tag == JAZZ_SM_COMMIT_UNIT && local_receiver) {
        const jazz_protocol_TransactionHdr *hdr = jazz_protocol__protocol__Transaction__hdr(message.v.commit_unit.tx);
        jazz_db__db__register_local_fate_route(local_fate_routes, hdr->tx_id, downstream_fates);
        jazz_node_NodeState *state = jazz_db_node_lock(node);
        jz_err e = NS_FN(ingest_relay_commit_unit_with_encoder_trust)(state, message.v.commit_unit.tx, message.v.commit_unit.versions,
            jazz_node__node__CommitUnitTrust__is_trusted((jazz_node_CommitUnitTrust)ingest_context.trust) || ingest_context.version_receipts_validated);
        jazz_db_node_unlock(node);
        jazz_db__db__PolicyBinding__drop(&session_claim_binding);
        if (e.code) return node_err(e);
        jazz_node__node__PublicationOutcome__settled(out, NULL, 0);
        return JZ_OK;
    }
    if (message.tag == JAZZ_SM_COMMIT_UNIT && (role.tag == JAZZ_PEER_ROLE_CLIENT_LINK || role.tag == JAZZ_PEER_ROLE_RELAY)) {
        const jazz_protocol_TransactionHdr *tx = jazz_protocol__protocol__Transaction__hdr(message.v.commit_unit.tx);
        /* Terminal authorization belongs to the immutable session selected
         * at request admission, never to the relay's subjectless transport
         * identity. Both transaction kinds need this authorization. */
        jazz_types_AuthorSubject permission_subject;
        switch (ingest_context.trust) {
        case JAZZ_TRUST_SESSION: permission_subject = ingest_context.identity; break;
        case JAZZ_TRUST_RELAY: permission_subject = session_claim_binding.identity; break;
        case JAZZ_TRUST_TRUSTED_BACKEND: case JAZZ_TRUST_TRUSTED_AUTHORITY:
            permission_subject = tx->has_permission_subject ? tx->permission_subject : tx->made_by; break;
        default: permission_subject = ingest_context.identity; break;
        }
        /* A direct Session keeps the established authorization-fate path
         * for an ordinary user-attribution mismatch. It must, however,
         * reject a forged durable SYSTEM origin before persistence. */
        bool must_bind_provenance;
        switch (ingest_context.trust) {
        case JAZZ_TRUST_SESSION: must_bind_provenance = tx->made_by.tag == JAZZ_AUTHOR_SYSTEM_AT; break;
        case JAZZ_TRUST_RELAY: must_bind_provenance = PEER_FN(admits_relay_binding)(peer, &session_claim_binding.identity, &session_claim_binding.claims); break;
        default: must_bind_provenance = false; break;
        }
        if (must_bind_provenance && !admitted_provenance_matches(permission_subject, tx->made_by)) {
            /* A relay may replay a unit the authority already stored under a
             * different durable origin. It cannot introduce that origin:
             * accept only an exact known payload. */
            bool known_replay = false;
            jazz_node_NodeState *n = jazz_db_node_lock(node);
            jz_err e = NS_FN(known_untrusted_commit_unit_matches)(n, message.v.commit_unit.tx, &message.v.commit_unit.versions, &known_replay);
            jazz_db_node_unlock(node);
            if (e.code) { err = node_err(e); goto fail; }
            if (!known_replay) { err = protocol_err("commit provenance does not match the admitted session"); goto fail; }
        }
        /* Only a relay's terminal ingest consumes this receipt. */
        bool admitted_write_authorization = false;
        if (ingest_context.trust == JAZZ_TRUST_RELAY) {
            jazz_node_NodeState *n = jazz_db_node_lock(node);
            jz_err e = PEER_FN(prove_terminal_commit_authorization)(peer, n, permission_subject, session_claim_binding.claims,
                                                                   &message.v.commit_unit.versions, tx->tx_id, &admitted_write_authorization);
            jazz_db_node_unlock(node);
            memset(&session_claim_binding.claims, 0, sizeof(session_claim_binding.claims));
            if (e.code) { err = node_err(e); goto fail; }
        }
        jazz_node_CommitUnitIngestContext ctx = ingest_context;
        ctx.admitted_write_authorization = admitted_write_authorization;
        jazz_node_NodeState *n = jazz_db_node_lock(node);
        jz_err e = NS_FN(apply_sync_message_with_ingest_context)(n, message, &ctx, out);
        jazz_db_node_unlock(node);
        jazz_db__db__PolicyBinding__drop(&session_claim_binding);
        return e.code ? node_err(e) : JZ_OK;
    }
    {
        jazz_node_NodeState *n = jazz_db_node_lock(node);
        jz_err e = NS_FN(apply_sync_message_with_ingest_context)(n, message, &ingest_context, out);
        jazz_db_node_unlock(node);
        jazz_db__db__PolicyBinding__drop(&session_claim_binding);
        return e.code ? node_err(e) : JZ_OK;
    }
fail:
    jazz_protocol__protocol__SyncMessage__drop(&message);
    jazz_db__db__PolicyBinding__drop(&session_claim_binding);
    return err;
}

/* ---------------------------------------------------------------- control responses */

/// Lazily emits one authorization-scope proof sequence.
static bool PendingAuthorizationScopeSequence_next_message(const jazz_db_PendingAuthorizationScopeSequence *self, jazz_protocol_SyncMessage *out) {
    u64 clause_count = self->hydration.clauses.len;
    u64 proof_steps = clause_count * 3; /* checked_mul cannot overflow for u32 * 3 in u64 */
    if (self->next_step < proof_steps) {
        const jazz_db_ServedAuthorizationScopeClause *clause = &self->hydration.clauses.ptr[self->next_step / 3];
        switch (self->next_step % 3) {
        case 0: jazz_protocol__protocol__SyncMessage__clone(out, &clause->register_); return true;
        case 1: jazz_protocol__protocol__SyncMessage__clone(out, &clause->subscribe); return true;
        case 2: {
            jazz_protocol_SyncMessage view;
            jazz_protocol__protocol__SyncMessage__clone(&view, &clause->view);
            jazz_protocol_ViewUpdatePayload *payload = NULL;
            jz_err e = jazz_protocol__protocol__ViewUpdatePayload__from_view_update(&view, &payload);
            jazz_protocol__protocol__SyncMessage__drop(&view);
            if (e.code) jz_panic("authority scope clauses are view updates");
            memset(out, 0, sizeof(*out));
            out->tag = JAZZ_SM_AUTHORIZATION_SCOPE_VIEW;
            out->v.scope_view.request_id = self->request_id;
            out->v.scope_view.key = jazz_protocol__protocol__AuthorizationSupportScopeKey__clone(self->key);
            out->v.scope_view.clause_index = (u16)(self->next_step / 3);
            out->v.scope_view.clause_count = (u16)clause_count;
            out->v.scope_view.view = payload;
            return true;
        }
        default: jz_panic("modulo three has only three cases");
        }
    }
    if (self->next_step == proof_steps) {
        memset(out, 0, sizeof(*out));
        out->tag = JAZZ_SM_AUTHORIZATION_SCOPE_AGGREGATE_RECEIPT;
        out->v.scope_receipt.request_id = self->request_id;
        out->v.scope_receipt.receipt = jazz_protocol__protocol__AuthorizationScopeReceipt__clone(self->hydration.receipt);
        return true;
    }
    if (self->next_step < proof_steps + 1) return false;
    u64 unsubscribe_index = self->next_step - (proof_steps + 1);
    if (unsubscribe_index >= self->hydration.clauses.len) return false;
    memset(out, 0, sizeof(*out));
    out->tag = JAZZ_SM_UNSUBSCRIBE;
    out->v.unsubscribe.subscription = self->hydration.clauses.ptr[unsubscribe_index].subscription;
    return true;
}

static void PendingAuthorizationScopeSequence_advance(jazz_db_PendingAuthorizationScopeSequence *self) {
    if (self->next_step != UINT32_MAX) self->next_step += 1;
}

void jazz_db__db__peer_connection__PendingSubscriberControlResponse__drop(void *p) {
    jazz_db_PendingSubscriberControlResponse *r = (jazz_db_PendingSubscriberControlResponse *)p;
    if (r->tag == JAZZ_PSCR_AUTHORIZATION_SCOPE_SEQUENCE) {
        jazz_protocol__protocol__AuthorizationSupportScopeKey__drop(r->sequence.key);
        jazz_db__db__ServedAuthorizationScopeHydration__drop(&r->sequence.hydration);
    } else {
        jazz_protocol__protocol__SyncMessage__drop(&r->message);
    }
}

void jazz_db__db__peer_connection__PendingSubscriberControlResponse__clone(void *dst, const void *src) {
    const jazz_db_PendingSubscriberControlResponse *s = (const jazz_db_PendingSubscriberControlResponse *)src;
    jazz_db_PendingSubscriberControlResponse *d = (jazz_db_PendingSubscriberControlResponse *)dst;
    memset(d, 0, sizeof(*d));
    d->tag = s->tag;
    if (s->tag == JAZZ_PSCR_AUTHORIZATION_SCOPE_SEQUENCE) {
        d->sequence.request_id = s->sequence.request_id;
        d->sequence.key = jazz_protocol__protocol__AuthorizationSupportScopeKey__clone(s->sequence.key);
        jazz_db__db__ServedAuthorizationScopeHydration__clone(&d->sequence.hydration, &s->sequence.hydration);
        d->sequence.next_step = s->sequence.next_step;
    } else {
        jazz_protocol__protocol__SyncMessage__clone(&d->message, &s->message);
    }
}

static void queue_direct_control(jz_deque *pending, jazz_protocol_SyncMessage message) {
    jazz_db_PendingSubscriberControlResponse r;
    memset(&r, 0, sizeof(r));
    r.tag = JAZZ_PSCR_DIRECT;
    r.message = message;
    jz_deque_push_back(pending, &r);
}

void jazz_db__db__peer_connection__queue_sync_context_control(jz_deque *pending, jazz_protocol_SyncMessage message) {
    jazz_db_PendingSubscriberControlResponse r;
    memset(&r, 0, sizeof(r));
    r.tag = JAZZ_PSCR_WITH_SYNC_CONTEXT;
    r.message = message;
    jz_deque_push_back(pending, &r);
}

static void queue_authorization_scope_sequence(jz_deque *pending, jazz_protocol_PermissionAdviceRequestId request_id,
                                               jazz_protocol_AuthorizationSupportScopeKey *key, jazz_db_ServedAuthorizationScopeHydration hydration) {
    jazz_db_PendingSubscriberControlResponse r;
    memset(&r, 0, sizeof(r));
    r.tag = JAZZ_PSCR_AUTHORIZATION_SCOPE_SEQUENCE;
    r.sequence.request_id = request_id;
    r.sequence.key = key;
    r.sequence.hydration = hydration;
    r.sequence.next_step = 0;
    jz_deque_push_back(pending, &r);
}

/* ---------------------------------------------------------------- large value uploads */

void jazz_db__db__peer_connection__PendingLargeValueUpload__drop(void *p) {
    jazz_db_PendingLargeValueUpload *u = (jazz_db_PendingLargeValueUpload *)p;
    jz_deque_drop(&u->requested, NULL);
    jz_deque_drop(&u->in_flight, NULL);
}

/// Transfer one detached upstream's resumable upload state. A reply that was
/// in flight is no longer tied to a live transport, so restore its exact batch
/// to the requested frontier; a start awaiting its first frontier restarts
/// only after the shared admission deadline.
void jazz_db__db__peer_connection__take_reconnectable_large_value_uploads(jz_btree *uploads_in, jz_btree *awaiting_in, jz_btree *out) {
    jz_btree awaiting = *awaiting_in;
    jz_btree_init(awaiting_in, awaiting.d);
    jz_btree uploads = *uploads_in;
    jz_btree_init(uploads_in, uploads.d);
    jazz_model_TxId *tx_id;
    groove_LargeValueRef *value_ref;
    JZ_BTREE_FOR(&awaiting, tx_id, value_ref) {
        jz_deque *queue = (jz_deque *)jz_btree_get(&uploads, tx_id);
        if (!queue) continue;
        jazz_db_PendingLargeValueUpload *upload = NULL;
        for (u32 i = 0; i < queue->len; ++i) {
            jazz_db_PendingLargeValueUpload *candidate = (jazz_db_PendingLargeValueUpload *)jz_deque_at(queue, i);
            if (groove_LargeValueRef_eq(&candidate->value_ref, value_ref)) { upload = candidate; break; }
        }
        if (!upload) continue;
        if (upload->in_flight.len == 0) {
            upload->started = false;
        } else {
            groove_NodeRef node;
            while (jz_deque_pop_back(&upload->in_flight, &node)) jz_deque_push_front(&upload->requested, &node);
        }
    }
    jz_btree_drop(&awaiting);
    *out = uploads;
}

/// Merge independently detached links without duplicating a logical
/// transaction/value upload.
void jazz_db__db__peer_connection__merge_reconnectable_large_value_uploads(jz_btree *destination_uploads, jz_btree uploads) {
    jazz_model_TxId tx_id;
    jz_deque queue;
    while (jz_btree_pop_first(&uploads, &tx_id, &queue)) {
        bool inserted = false;
        jz_deque *retained = (jz_deque *)jz_btree_entry(destination_uploads, &tx_id, &inserted);
        if (inserted) jz_deque_init(retained, sizeof(jazz_db_PendingLargeValueUpload));
        jazz_db_PendingLargeValueUpload upload;
        while (jz_deque_pop_front(&queue, &upload)) {
            bool exists = false;
            for (u32 i = 0; i < retained->len; ++i)
                if (groove_LargeValueRef_eq(&((jazz_db_PendingLargeValueUpload *)jz_deque_at(retained, i))->value_ref, &upload.value_ref)) { exists = true; break; }
            if (exists) { jazz_db__db__peer_connection__PendingLargeValueUpload__drop(&upload); continue; }
            jz_deque_push_back(retained, &upload);
        }
        jz_deque_drop(&queue, NULL);
    }
    jz_btree_drop(&uploads);
}

enum { RATE_LIMITED_UPLOAD_RETRY_DELAY_MS = 1000 };

typedef JZ_VEC(groove_LargeValueRef) LargeValueRefVec;

static void collect_large_value_refs(const groove_Value *value, LargeValueRefVec *refs) {
    switch (value->tag) {
    case GROOVE_V_LARGE: {
        for (u32 i = 0; i < refs->len; ++i)
            if (groove_LargeValueRef_eq(&refs->ptr[i], value->v.large)) return;
        groove_LargeValueRef copy = *value->v.large;
        jz_vec_push(refs, &copy, sizeof(copy));
        return;
    }
    case GROOVE_V_TUPLE: case GROOVE_V_ARRAY:
        for (u32 i = 0; i < value->v.values.len; ++i) collect_large_value_refs(&value->v.values.ptr[i], refs);
        return;
    case GROOVE_V_NULLABLE:
        if (value->v.nullable) collect_large_value_refs(value->v.nullable, refs);
        return;
    case GROOVE_V_RECORD: {
        groove_ValueVec values;
        jz_err e = groove__records__OwnedRecord__to_values(&value->v.record, &values);
        if (e.code) { jz_err_drop(&e); return; }
        for (u32 i = 0; i < values.len; ++i) collect_large_value_refs(&values.ptr[i], refs);
        groove__records__ValueVec__drop(&values);
        return;
    }
    case GROOVE_V_ENUM: {
        groove_ValueVec values;
        jz_err e = groove__records__OwnedRecord__to_values(&value->v.enum_value.record, &values);
        if (e.code) { jz_err_drop(&e); return; }
        for (u32 i = 0; i < values.len; ++i) collect_large_value_refs(&values.ptr[i], refs);
        groove__records__ValueVec__drop(&values);
        return;
    }
    default: return;
    }
}

static void commit_unit_large_value_refs(const jazz_protocol_SyncMessage *unit, LargeValueRefVec *refs) {
    memset(refs, 0, sizeof(*refs));
    jazz_protocol_VersionVec versions;
    jazz_protocol__protocol__SyncMessage__uploaded_versions(unit, &versions);
    for (u32 v = 0; v < versions.len; ++v) {
        u32 count = jazz_protocol__protocol__VersionRecord__application_cell_count(versions.ptr[v]);
        for (u32 position = 0; position < count; ++position) {
            groove_Value value;
            if (jazz_protocol__protocol__VersionRecord__cell_at(versions.ptr[v], position, &value)) {
                collect_large_value_refs(&value, refs);
                groove__records__Value__drop(&value);
            }
        }
    }
    jz_vec_drop(&versions, sizeof(jazz_protocol_VersionRecord *), NULL);
}

void jazz_db__db__peer_connection__PendingCatalogueSubscription__drop(void *p) {
    jazz_db_PendingCatalogueSubscription *s = (jazz_db_PendingCatalogueSubscription *)p;
    jazz_protocol__protocol__Subscribe__drop(&s->subscribe);
    jazz_db__db__PolicyBinding__drop(&s->policy_binding);
}

/* ---------------------------------------------------------------- PeerConnection: session claims */

void PC_FN(take_released_outbox_tx_ids)(PC *self, jazz_db_TxIdVec *out) {
    memcpy(out, &self->released_outbox_tx_ids, sizeof(*out));
    memset(&self->released_outbox_tx_ids, 0, sizeof(self->released_outbox_tx_ids));
}

void PC_FN(forget_released_outbox_tx_ids)(PC *self, const jz_hmap *released) {
    if (self->link.tag != JAZZ_LINK_UPSTREAM) return;
    jz_btree *uploaded = &self->link.v.upstream.uploaded;
    jazz_model_TxId *tx_id;
    void *unit;
    JZ_HMAP_FOR(released, tx_id, unit) { (void)unit; jz_btree_remove(uploaded, tx_id, NULL, NULL); }
}

/// Clone the binding-driven auxiliary I/O endpoint for this peer link.
jazz_db_PeerIoPump PC_FN(io_pump)(const PC *self) {
    /* A plain byte-adapter connection drives its own credits/auxiliary
     * frames. Transfer output only when a binding actually takes the pump. */
    jazz_db__db__PeerIoPump__claim_binding_output(&self->auxiliary_pump);
    return jazz_db__db__PeerIoPump__clone(&self->auxiliary_pump);
}

/// Replace the claims authenticated by the host for this subscriber link.
/// Wire peers cannot invoke this path; bindings use it only after their
/// trusted authentication layer has accepted a refreshed session.
void PC_FN(update_authenticated_session_claims)(PC *self, jz_btree claims) {
    if (self->link.tag != JAZZ_LINK_SUBSCRIBER) { jz_btree_drop(&claims); return; }
    jazz_db_SubscriberConnectionState *s = &self->link.v.subscriber;
    if (PEER_FN(admitted_scope_relay_binding)(s->peer)) {
        /* A scope relay receives a fresh immutable capability only through a
         * new server-authenticated connection. */
        jz_btree_drop(&claims);
        return;
    }
    if (jz_btree_eq(&s->session_claims, &claims, groove__records__Value__eq)) { jz_btree_drop(&claims); return; }
    jz_btree_drop(&s->session_claims);
    s->session_claims = claims;
    if (s->session_claim_revision != UINT64_MAX) s->session_claim_revision += 1;
}

/// Return the claims admitted for this subscriber session. They are scoped
/// while the node lock is held instead of being installed under the shared
/// author identity.
static bool subscriber_session_claim_binding(const PC *self, jazz_db_PolicyBinding *out) {
    if (self->link.tag != JAZZ_LINK_SUBSCRIBER) return false;
    const jazz_db_SubscriberConnectionState *s = &self->link.v.subscriber;
    const jazz_node_ScopeRelayBinding *binding = PEER_FN(admitted_scope_relay_binding)(s->peer);
    if (binding) { policy_binding_from(out, binding->identity, &binding->claims); return true; }
    policy_binding_from(out, s->ingest_context.identity, &s->session_claims);
    return true;
}

/// Keep the legacy admission map available to write-policy evaluation and
/// upstream propagation. Read compilation uses the scoped context above.
static void bind_subscriber_session_claims(PC *self) {
    if (self->link.tag == JAZZ_LINK_SUBSCRIBER && PEER_FN(admitted_scope_relay_binding)(self->link.v.subscriber.peer)) {
        /* All scope-relay policy work takes the immutable binding through
         * `scoped_active_session_claims`. */
        return;
    }
    jazz_db_PolicyBinding binding;
    if (!subscriber_session_claim_binding(self, &binding)) return;
    jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
    NS_FN(set_session_claims)(n, binding.identity, binding.claims);
    jazz_db_node_unlock(self->node);
}

static u64 subscriber_session_claim_revision(const PC *self) {
    if (self->link.tag != JAZZ_LINK_SUBSCRIBER) return 0;
    return self->link.v.subscriber.session_claim_revision;
}

/* ---------------------------------------------------------------- claim refresh */

typedef struct CoverageReplacement { jazz_db_CoverageKey old; jazz_db_CoverageKey refreshed; } CoverageReplacement;
typedef struct UpstreamReplacement {
    jazz_db_CoverageKey coverage;
    jazz_db_CoverageKey old_coverage;
    jazz_protocol_SubscriptionKey old_upstream_subscription;
    jazz_protocol_SubscriptionKey fresh_upstream_subscription;
    jazz_model_ValidatedQuery *shape;
    jazz_model_Binding *binding;
    jazz_protocol_RegisterShapeOptions opts;
    jz_btree downstream_subscriptions;
} UpstreamReplacement;
typedef struct GroupSnapshot {
    jazz_db_CoverageKey coverage;
    jazz_model_ValidatedQuery *shape;
    jazz_model_Binding *binding;
    jazz_db_PolicyBinding policy_binding;
    u8 policy_binding_origin;
    JZ_VEC(jazz_protocol_SubscriptionKey) subscribers;
} GroupSnapshot;

static void subscription_key_set_clone(jz_btree *dst, const jz_btree *src) { jz_btree_clone(dst, src, NULL, NULL); }

static void coverage_group_reset_pending_initial(jazz_db_CoverageGroup *group) {
    group->initialized = false;
    jz_btree_drop(&group->pending_initial_subscribers);
    subscription_key_set_clone(&group->pending_initial_subscribers, &group->subscribers);
    if (group->has_pending_initial_update) {
        jazz_protocol__protocol__SyncMessage__drop(&group->pending_initial_update.message);
        group->has_pending_initial_update = false;
    }
    jz_deque_drop(&group->pending_incremental_updates, jazz_db__db__KeyedSyncMessage__drop);
    jz_deque_init(&group->pending_incremental_updates, sizeof(jazz_db_KeyedSyncMessage));
}

static bool pending_subscribe_names(const jazz_db_PendingUpstreamCommand *command, const jazz_protocol_SubscriptionKey *subscription) {
    return command->tag == JAZZ_PUC_SUBSCRIBE && jazz_protocol_SubscriptionKey_eq(&command->subscribe.subscription, subscription);
}
static bool keep_not_pending_subscribe(void *item, void *ctx) { return !pending_subscribe_names((const jazz_db_PendingUpstreamCommand *)item, (const jazz_protocol_SubscriptionKey *)ctx); }

static jazz_protocol_SyncMessage take_last_or_clone(bool *has_slot, jazz_protocol_SyncMessage *slot, bool more_follow);
static jazz_protocol_SyncMessage retarget_view_update(jazz_protocol_SyncMessage message, jazz_protocol_SubscriptionKey target);
static void stamp_view_update_authorization_progress_from(jazz_node_PeerState *peer, jazz_protocol_SubscriptionKey source, jazz_protocol_SyncMessage *message);
static jz_err send_subscriber_with_sync_context_full(jazz_db_NodeMutex *node, jazz_node_PeerState *peer, jazz_db_Transport *transport, jz_btree *local_fate_routes,
                                                    jazz_db_SyncMessageVec *downstream_fates, jazz_protocol_SyncMessage message);

/// Rebuild this subscriber's maintained views if its process-local claims
/// changed. Policy claim values are bound when a maintained view opens, so
/// retaining the old view after a claim change would retain its authority.
static jz_err rebind_subscriber_views_after_claim_change(PC *self, const jz_waker *progress_waker, bool *out) {
    u64 connection_epoch = self->connection_epoch;
    *out = false;
    if (self->link.tag != JAZZ_LINK_SUBSCRIBER) return JZ_OK;
    jazz_db_PolicyBinding session_claim_binding;
    bool has_binding = subscriber_session_claim_binding(self, &session_claim_binding);
    u64 current_revision = subscriber_session_claim_revision(self);
    if (self->observed_session_claim_revision == current_revision) {
        if (has_binding) jazz_db__db__PolicyBinding__drop(&session_claim_binding);
        return JZ_OK;
    }
    jazz_db_SubscriberConnectionState *link = &self->link.v.subscriber;
    jazz_node_PeerState *peer = link->peer;
    jz_btree *served = &link->served;
    jz_btree *coverage_groups = &link->coverage_groups;
    jazz_db_PendingUpstreamCommands *upstream_subscriptions = link->upstream_subscriptions;
    PEER_FN(advance_authorization_progress)(peer);
    if (!has_binding) jz_panic("subscriber claims");
    jazz_db_PolicyBinding refreshed_direct_binding;
    jazz_db__db__PolicyBinding__clone(&refreshed_direct_binding, &session_claim_binding);
    jz_err err = JZ_OK;
    /* `CoverageKey` is also the maintained receiver's stable identity. A
     * relay key includes its admitted policy snapshot, so a direct claim
     * refresh must replace that key rather than merely changing mutable
     * state under the old key. */
    JZ_VEC(CoverageReplacement) coverage_replacements = {0};
    {
        jazz_db_CoverageKey *coverage;
        jazz_db_CoverageGroup *group;
        JZ_BTREE_FOR(coverage_groups, coverage, group) {
            if (group->policy_binding_origin != JAZZ_CPBO_DIRECT_ADMITTED) continue;
            CoverageReplacement r;
            jazz_db__db__CoverageKey__clone(&r.old, coverage);
            jazz_db__db__CoverageKey__clone(&r.refreshed, coverage);
            if (r.refreshed.has_policy_binding) {
                r.refreshed.policy_binding.identity = refreshed_direct_binding.identity;
                jz_btree_drop(&r.refreshed.policy_binding.canonical_claims);
                jz_btree claims;
                claims_clone(&claims, &refreshed_direct_binding.claims);
                jazz_protocol__protocol__CanonicalPolicyClaims__new(&r.refreshed.policy_binding.canonical_claims, claims);
            }
            if (jazz_db__db__CoverageKey__eq(&r.old, &r.refreshed)) {
                jazz_db__db__CoverageKey__drop(&r.old);
                jazz_db__db__CoverageKey__drop(&r.refreshed);
                continue;
            }
            jz_vec_push(&coverage_replacements, &r, sizeof(r));
        }
    }
    /* replaced_coverage_by_new: refreshed -> old (lookup by linear scan) */
    jz_btree stale_maintained_subscriptions;
    jz_btree_init(&stale_maintained_subscriptions, &jazz_db_subscription_key_set_desc);
    for (u32 i = 0; i < coverage_replacements.len; ++i) {
        jazz_protocol_SubscriptionKey k = jazz_db__db__peer_connection__coverage_group_subscription_key(&coverage_replacements.ptr[i].old);
        jz_btree_insert(&stale_maintained_subscriptions, &k, NULL, NULL);
    }
    for (u32 i = 0; i < coverage_replacements.len; ++i) {
        CoverageReplacement *r = &coverage_replacements.ptr[i];
        if (jz_btree_contains(coverage_groups, &r->refreshed)) {
            err = protocol_err("claim refresh would merge direct and existing relay coverage");
            goto out_replacements;
        }
        jazz_db_CoverageKey removed_key;
        jazz_db_CoverageGroup group;
        if (!jz_btree_remove(coverage_groups, &r->old, &removed_key, &group)) jz_panic("coverage key came from coverage_groups");
        jazz_db__db__CoverageKey__drop(&removed_key);
        /* A serving authority's propagated usage is only a lifecycle handle:
         * its owner must nevertheless follow the replacement coverage key.
         * Scope relays instead retire and replace the opaque upstream usage
         * below. */
        if (!jazz_protocol_SubscriptionKey_eq(&group.authority_result_subscription, &group.upstream_subscription)) {
            jazz_db_RelayOwnerKey key;
            memset(&key, 0, sizeof(key));
            key.subscription = group.upstream_subscription;
            key.downstream_connection_epoch = connection_epoch;
            key.read_view = jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&r->old.opts);
            jazz_db_RelayUpstreamSubscriptionOwner *owner = (jazz_db_RelayUpstreamSubscriptionOwner *)jz_btree_get(self->relay_upstream_subscription_owners, &key);
            if (owner) {
                JZ_ASSERT(owner->downstream_connection_epoch == connection_epoch, "direct claim refresh found an upstream handle owned by another connection");
                JZ_ASSERT(jazz_db__db__CoverageKey__eq(&owner->coverage, &r->old), "direct claim refresh found an upstream handle for another coverage group");
                jazz_db__db__CoverageKey__drop(&owner->coverage);
                jazz_db__db__CoverageKey__clone(&owner->coverage, &r->refreshed);
                jazz_db__db__PolicyBinding__drop(&owner->policy_binding);
                jazz_db__db__PolicyBinding__clone(&owner->policy_binding, &refreshed_direct_binding);
            }
        }
        jazz_db_CoverageKey new_key;
        jazz_db__db__CoverageKey__clone(&new_key, &r->refreshed);
        jz_btree_insert(coverage_groups, &new_key, &group, NULL);
        jazz_protocol_SubscriptionKey *sk;
        jazz_db_CoverageKey *coverage;
        JZ_BTREE_FOR(served, sk, coverage) {
            (void)sk;
            if (jazz_db__db__CoverageKey__eq(coverage, &r->old)) {
                jazz_db__db__CoverageKey__drop(coverage);
                jazz_db__db__CoverageKey__clone(coverage, &r->refreshed);
            }
        }
    }
    /* Coverage-key replacement changes the maintained receiver key. Retire
     * the old key explicitly before scheduling a fresh group. */
    {
        jazz_protocol_SubscriptionKey *stale;
        void *unit;
        JZ_BTREE_FOR(&stale_maintained_subscriptions, stale, unit) {
            (void)unit;
            PEER_FN(note_claim_refresh_full_diff)(peer, *stale);
            jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
            NS_FN(apply_unsubscribe)(n, *stale);
            PEER_FN(forget_subscription_with_node)(peer, n, *stale);
            jazz_db_node_unlock(self->node);
        }
    }
    /* A direct group can have already propagated its old snapshot to an
     * upstream authority. Retire that one usage site and allocate a fresh
     * opaque handle whose identity includes the new immutable delegated
     * snapshot. */
    JZ_VEC(UpstreamReplacement) upstream_replacements = {0};
    jz_btree refreshed_authority_sources; /* CoverageKey -> AuthorityResultKey */
    jz_btree_init(&refreshed_authority_sources, &jazz_db_coverage_authority_result_desc);
    jz_btree refreshed_upstream_handles;  /* CoverageKey -> SubscriptionKey */
    jz_btree_init(&refreshed_upstream_handles, &jazz_db_coverage_subscription_desc);
    JZ_VEC(GroupSnapshot) groups = {0};
    {
        jazz_db_CoverageKey *coverage;
        jazz_db_CoverageGroup *group;
        JZ_BTREE_FOR(coverage_groups, coverage, group) {
            /* The connection's authenticated snapshot owns only direct usage
             * sites; identity equality is not provenance. */
            if (group->policy_binding_origin == JAZZ_CPBO_DIRECT_ADMITTED) {
                jazz_db__db__PolicyBinding__drop(&group->policy_binding);
                jazz_db__db__PolicyBinding__clone(&group->policy_binding, &refreshed_direct_binding);
                jazz_protocol_SubscriptionKey *first = NULL;
                void *unit;
                if (jazz_protocol_SubscriptionKey_eq(&group->authority_result_subscription, &group->upstream_subscription)
                    && jz_btree_first(&group->subscribers, (void **)&first, &unit)) {
                    jazz_protocol_SubscriptionKey downstream_subscription = *first;
                    jazz_protocol_SubscriptionKey old_upstream_subscription = group->upstream_subscription;
                    jazz_db_CoverageKey authority_coverage;
                    relay_authority_coverage_key(coverage, &authority_coverage);
                    jazz_protocol_SubscriptionKey fresh_upstream_subscription;
                    jazz_protocol_SubscriptionKey *cached = (jazz_protocol_SubscriptionKey *)jz_btree_get(&refreshed_upstream_handles, &authority_coverage);
                    bool found = false;
                    if (cached) { fresh_upstream_subscription = *cached; found = true; }
                    else {
                        jazz_db_RelayOwnerKey *ok;
                        jazz_db_RelayUpstreamSubscriptionOwner *owner;
                        JZ_BTREE_FOR(self->relay_upstream_subscription_owners, ok, owner) {
                            jazz_db_CoverageKey owner_authority;
                            relay_authority_coverage_key(&owner->coverage, &owner_authority);
                            bool matches = jazz_db__db__CoverageKey__eq(&owner_authority, &authority_coverage) && jazz_db__db__PolicyBinding__eq(&owner->policy_binding, &refreshed_direct_binding);
                            jazz_db__db__CoverageKey__drop(&owner_authority);
                            if (matches) { fresh_upstream_subscription = ok->subscription; found = true; break; }
                        }
                    }
                    if (!found)
                        fresh_upstream_subscription = relay_upstream_subscription_key(connection_epoch, downstream_subscription,
                            jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&group->upstream_opts), &refreshed_direct_binding);
                    {
                        jazz_db_CoverageKey k;
                        jazz_db__db__CoverageKey__clone(&k, &authority_coverage);
                        jazz_protocol_SubscriptionKey old;
                        if (jz_btree_insert(&refreshed_upstream_handles, &k, &fresh_upstream_subscription, &old)) jazz_db__db__CoverageKey__drop(&k);
                    }
                    jazz_db__db__CoverageKey__drop(&authority_coverage);
                    if (!jazz_protocol_SubscriptionKey_eq(&old_upstream_subscription, &fresh_upstream_subscription)) {
                        group->upstream_subscription = fresh_upstream_subscription;
                        JZ_ASSERT(jazz_protocol_SubscriptionKey_eq(&group->authority_result_subscription, &old_upstream_subscription),
                                  "only a non-authoritative relay refreshes an upstream authority source");
                        group->authority_result_subscription = fresh_upstream_subscription;
                        jazz_protocol_BindingViewKey view;
                        memset(&view, 0, sizeof(view));
                        view.shape_id = jazz_model__query__ValidatedQuery__shape_id(group->shape);
                        view.binding_id = jazz_model__query__Binding__binding_id(group->binding);
                        view.read_view = jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&group->upstream_opts);
                        jz_btree claims;
                        claims_clone(&claims, &refreshed_direct_binding.claims);
                        jazz_protocol_PolicyBindingKey policy;
                        jazz_protocol__protocol__PolicyBindingKey__from_canonical_parts(&policy, refreshed_direct_binding.identity, claims);
                        jazz_protocol_AuthorityResultKey source;
                        jazz_protocol__protocol__AuthorityResultKey__policy_scoped(&source, view, policy);
                        jazz_db_CoverageKey ck;
                        jazz_db__db__CoverageKey__clone(&ck, coverage);
                        jazz_protocol_AuthorityResultKey old_source;
                        if (jz_btree_insert(&refreshed_authority_sources, &ck, &source, &old_source)) {
                            jazz_db__db__CoverageKey__drop(&ck);
                            jazz_protocol__protocol__AuthorityResultKey__drop(&old_source);
                        }
                        group->awaiting_upstream_settlement = true;
                        /* Do not let the next owner-loop pass rehydrate a
                         * subscriber from the old result set. */
                        coverage_group_reset_pending_initial(group);
                        UpstreamReplacement u;
                        jazz_db__db__CoverageKey__clone(&u.coverage, coverage);
                        bool replaced = false;
                        for (u32 i = 0; i < coverage_replacements.len; ++i)
                            if (jazz_db__db__CoverageKey__eq(&coverage_replacements.ptr[i].refreshed, coverage)) {
                                jazz_db__db__CoverageKey__clone(&u.old_coverage, &coverage_replacements.ptr[i].old);
                                replaced = true;
                                break;
                            }
                        if (!replaced) jazz_db__db__CoverageKey__clone(&u.old_coverage, coverage);
                        u.old_upstream_subscription = old_upstream_subscription;
                        u.fresh_upstream_subscription = fresh_upstream_subscription;
                        u.shape = jazz_model__query__ValidatedQuery__clone(group->shape);
                        u.binding = jazz_model__query__Binding__clone(group->binding);
                        jazz_protocol__protocol__RegisterShapeOptions__clone(&u.opts, &group->upstream_opts);
                        subscription_key_set_clone(&u.downstream_subscriptions, &group->subscribers);
                        jz_vec_push(&upstream_replacements, &u, sizeof(u));
                    }
                }
            }
            GroupSnapshot g;
            memset(&g, 0, sizeof(g));
            jazz_db__db__CoverageKey__clone(&g.coverage, coverage);
            g.shape = jazz_model__query__ValidatedQuery__clone(group->shape);
            g.binding = jazz_model__query__Binding__clone(group->binding);
            jazz_db__db__PolicyBinding__clone(&g.policy_binding, &group->policy_binding);
            g.policy_binding_origin = group->policy_binding_origin;
            jazz_protocol_SubscriptionKey *sk;
            void *unit;
            JZ_BTREE_FOR(&group->subscribers, sk, unit) { (void)unit; jz_vec_push(&g.subscribers, sk, sizeof(*sk)); }
            jz_vec_push(&groups, &g, sizeof(g));
        }
    }
    bool refreshed_upstream_usage = upstream_replacements.len != 0;
    jz_btree deferred_rehydrates; /* BTreeSet<CoverageKey> */
    jz_btree_init(&deferred_rehydrates, &jazz_db_coverage_set_desc);
    for (u32 i = 0; i < upstream_replacements.len; ++i) {
        jazz_db_CoverageKey k;
        jazz_db__db__CoverageKey__clone(&k, &upstream_replacements.ptr[i].coverage);
        if (!jz_btree_insert(&deferred_rehydrates, &k, NULL, NULL)) {} else jazz_db__db__CoverageKey__drop(&k);
    }
    for (u32 i = 0; i < upstream_replacements.len; ++i) {
        UpstreamReplacement *u = &upstream_replacements.ptr[i];
        /* Retire the group-owned receiver explicitly; it will reopen only
         * after the fresh exact U settles. */
        jazz_protocol_SubscriptionKey maintained_subscription = jazz_db__db__peer_connection__coverage_group_subscription_key(&u->coverage);
        {
            jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
            NS_FN(apply_unsubscribe)(n, maintained_subscription);
            PEER_FN(forget_subscription_with_node)(peer, n, maintained_subscription);
            jazz_db_node_unlock(self->node);
        }
        jazz_db_RelayUpstreamSubscriptionOwner old_owner;
        bool has_old_owner = jazz_db__db__node_runtime__retire_relay_upstream_subscription(self->relay_upstream_subscription_owners,
            u->old_upstream_subscription, connection_epoch, &u->old_coverage, &old_owner);
        /* Withdraw the old local source before this subscriber's owner loop
         * can observe its already-settled BindingViewKey again. */
        jazz_db_PendingUpstreamCommands *pending = upstream_subscriptions;
        if (has_old_owner) {
            {
                jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
                NS_FN(apply_unsubscribe)(n, u->old_upstream_subscription);
                jazz_db_node_unlock(self->node);
            }
            bool old_open_was_pending = false;
            for (u32 c = 0; c < pending->len; ++c)
                if (pending_subscribe_names(&pending->ptr[c], &u->old_upstream_subscription)) { old_open_was_pending = true; break; }
            jz_vec_retain(pending, sizeof(jazz_db_PendingUpstreamCommand), keep_not_pending_subscribe, &u->old_upstream_subscription, jazz_db__db__PendingUpstreamCommand__drop);
            if (!old_open_was_pending) {
                jazz_db_PendingUpstreamCommand *command = (jazz_db_PendingUpstreamCommand *)jz_vec_push_slot(pending, sizeof(jazz_db_PendingUpstreamCommand));
                memset(command, 0, sizeof(*command));
                command->tag = JAZZ_PUC_UNSUBSCRIBE;
                command->unsubscribe = u->old_upstream_subscription;
            }
            jazz_db__db__RelayUpstreamSubscriptionOwner__drop(&old_owner);
        }
        jazz_db_RelayOwnerKey key;
        memset(&key, 0, sizeof(key));
        key.subscription = u->fresh_upstream_subscription;
        key.downstream_connection_epoch = connection_epoch;
        key.read_view = jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&u->coverage.opts);
        jazz_db_RelayUpstreamSubscriptionOwner owner;
        memset(&owner, 0, sizeof(owner));
        owner.request.subscription = u->fresh_upstream_subscription;
        owner.request.shape = jazz_model__query__ValidatedQuery__clone(u->shape);
        owner.request.binding = jazz_model__query__Binding__clone(u->binding);
        jazz_protocol__protocol__RegisterShapeOptions__clone(&owner.request.opts, &u->opts);
        owner.request.identity = refreshed_direct_binding.identity;
        owner.request.has_policy_binding = true;
        jazz_db__db__PolicyBinding__clone(&owner.request.policy_binding, &refreshed_direct_binding);
        owner.scalar_authority_revision = 0;
        jazz_db__db__ScalarReconciliation__default(&owner.scalar_reconciliation);
        owner.downstream_connection_epoch = connection_epoch;
        owner.coverage = u->coverage;
        memset(&u->coverage, 0, sizeof(u->coverage));
        jazz_db__db__PolicyBinding__clone(&owner.policy_binding, &refreshed_direct_binding);
        owner.downstream_subscriptions = u->downstream_subscriptions;
        memset(&u->downstream_subscriptions, 0, sizeof(u->downstream_subscriptions));
        jazz_db_RelayUpstreamSubscriptionOwner replaced_owner;
        if (jz_btree_insert(self->relay_upstream_subscription_owners, &key, &owner, &replaced_owner)) jazz_db__db__RelayUpstreamSubscriptionOwner__drop(&replaced_owner);
        jazz_db_PendingUpstreamCommand *command = (jazz_db_PendingUpstreamCommand *)jz_vec_push_slot(pending, sizeof(jazz_db_PendingUpstreamCommand));
        memset(command, 0, sizeof(*command));
        command->tag = JAZZ_PUC_SUBSCRIBE;
        command->subscribe.subscription = u->fresh_upstream_subscription;
        command->subscribe.shape = u->shape; u->shape = NULL;
        command->subscribe.binding = u->binding; u->binding = NULL;
        command->subscribe.opts = u->opts; memset(&u->opts, 0, sizeof(u->opts));
        command->subscribe.identity = refreshed_direct_binding.identity;
        command->subscribe.has_policy_binding = true;
        jazz_db__db__PolicyBinding__clone(&command->subscribe.policy_binding, &refreshed_direct_binding);
    }
    if (refreshed_upstream_usage) {
        link->serve_dirty = true;
        jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_IMMEDIATE);
    }
    bool rebind_pending = false;
    for (u32 gi = 0; gi < groups.len; ++gi) {
        GroupSnapshot *g = &groups.ptr[gi];
        jazz_protocol_SubscriptionKey maintained_subscription = jazz_db__db__peer_connection__coverage_group_subscription_key(&g->coverage);
        u64 retained_authorization_progress = 0;
        for (u32 s = 0; s < g->subscribers.len; ++s) {
            u64 p = PEER_FN(authorization_progress_for_subscription)(peer, g->subscribers.ptr[s]);
            if (p > retained_authorization_progress) retained_authorization_progress = p;
        }
        PEER_FN(set_subscription_policy_binding)(peer, maintained_subscription, g->policy_binding.identity, g->policy_binding.claims);
        memset(&g->policy_binding.claims, 0, sizeof(g->policy_binding.claims));
        PEER_FN(retain_authorization_progress_for_subscription)(peer, maintained_subscription, retained_authorization_progress);
        if (g->policy_binding_origin == JAZZ_CPBO_DIRECT_ADMITTED) {
            /* Update every concrete usage site before reopening the shared
             * evaluator. */
            for (u32 s = 0; s < g->subscribers.len; ++s) {
                jz_btree claims;
                claims_clone(&claims, &refreshed_direct_binding.claims);
                PEER_FN(set_subscription_policy_binding)(peer, g->subscribers.ptr[s], refreshed_direct_binding.identity, claims);
            }
        }
        if (jz_btree_contains(&deferred_rehydrates, &g->coverage)) {
            jazz_protocol_AuthorityResultKey *source = (jazz_protocol_AuthorityResultKey *)jz_btree_get(&refreshed_authority_sources, &g->coverage);
            if (!source) jz_panic("every deferred relay refresh selects a new exact U source");
            jazz_protocol_AuthorityResultKey s1;
            jazz_protocol__protocol__AuthorityResultKey__clone(&s1, source);
            PEER_FN(set_subscription_authority_result_source)(peer, maintained_subscription, s1);
            PEER_FN(set_subscription_awaiting_selected_authority_source)(peer, maintained_subscription, true);
            for (u32 s = 0; s < g->subscribers.len; ++s) {
                jazz_protocol_AuthorityResultKey sn;
                jazz_protocol__protocol__AuthorityResultKey__clone(&sn, source);
                PEER_FN(set_subscription_authority_result_source)(peer, g->subscribers.ptr[s], sn);
            }
            /* The old maintained group may still contain rows now forbidden
             * by the refreshed session; the owner loop opens a fresh reset
             * only after the new usage site has settled upstream. */
            continue;
        }
        JZ_VEC(jazz_protocol_SubscriptionKey) refresh_subscribers = {0};
        {
            jazz_db_CoverageGroup *group = (jazz_db_CoverageGroup *)jz_btree_get(coverage_groups, &g->coverage);
            if (!group) jz_panic("claim-refresh coverage group remains registered");
            if (!group->has_pending_claim_refresh_revision || group->pending_claim_refresh_revision != current_revision) {
                /* A new admitted claim revision is a new opening transition
                 * for every live usage. */
                coverage_group_reset_pending_initial(group);
                group->has_pending_claim_refresh_revision = true;
                group->pending_claim_refresh_revision = current_revision;
            }
            jazz_protocol_SubscriptionKey *sk;
            void *unit;
            JZ_BTREE_FOR(&group->pending_initial_subscribers, sk, unit) { (void)unit; jz_vec_push(&refresh_subscribers, sk, sizeof(*sk)); }
        }
        bool has_update = false;
        jazz_protocol_SyncMessage update;
        {
            jazz_node_NodeState *owner = jazz_db_node_lock(self->node);
            jz_btree claims;
            claims_clone(&claims, &session_claim_binding.claims);
            jazz_node_NodeState *scoped = NS_FN(scoped_active_session_claims)(owner, session_claim_binding.identity, claims);
            jazz_protocol_RegisterShapeOptions opts;
            jazz_protocol__protocol__RegisterShapeOptions__clone(&opts, &g->coverage.opts);
            jz_err e = PEER_FN(rehydrate_query_for_subscription_with_opts_and_waker)(peer, scoped, maintained_subscription, g->shape, g->binding, opts, progress_waker, &has_update, &update);
            NS_FN(end_scoped_claims)(owner);
            jazz_db_node_unlock(self->node);
            if (e.code) { err = node_err(e); jz_vec_drop(&refresh_subscribers, sizeof(jazz_protocol_SubscriptionKey), NULL); goto out_groups; }
        }
        if (!has_update) {
            /* A cold maintained view may yield while storage/runtime work is
             * pending. Do not acknowledge the claim revision until we have
             * actually replaced every direct usage site's result set. */
            rebind_pending = true;
            jz_vec_drop(&refresh_subscribers, sizeof(jazz_protocol_SubscriptionKey), NULL);
            continue;
        }
        bool has_shared = true;
        for (u32 s = 0; s < refresh_subscribers.len; ++s) {
            jazz_protocol_SubscriptionKey subscription = refresh_subscribers.ptr[s];
            jazz_protocol_SyncMessage next = take_last_or_clone(&has_shared, &update, s + 1 < refresh_subscribers.len);
            jazz_protocol_SyncMessage retargeted = retarget_view_update(next, subscription);
            stamp_view_update_authorization_progress_from(peer, maintained_subscription, &retargeted);
            jz_err e = send_subscriber_with_sync_context_full(self->node, peer, &self->transport, self->local_fate_routes, self->downstream_fates, retargeted);
            if (e.code) {
                err = e;
                if (has_shared) jazz_protocol__protocol__SyncMessage__drop(&update);
                jz_vec_drop(&refresh_subscribers, sizeof(jazz_protocol_SubscriptionKey), NULL);
                goto out_groups;
            }
            jazz_db_CoverageGroup *group = (jazz_db_CoverageGroup *)jz_btree_get(coverage_groups, &g->coverage);
            if (!group) jz_panic("claim-refresh coverage group remains registered");
            jz_btree_remove(&group->pending_initial_subscribers, &subscription, NULL, NULL);
        }
        if (has_shared) jazz_protocol__protocol__SyncMessage__drop(&update);
        jz_vec_drop(&refresh_subscribers, sizeof(jazz_protocol_SubscriptionKey), NULL);
        /* This rehydrate is the replacement opening snapshot for every
         * successfully sent usage in the group. */
        jazz_db_CoverageGroup *group = (jazz_db_CoverageGroup *)jz_btree_get(coverage_groups, &g->coverage);
        if (!group) jz_panic("rehydrated coverage group remains registered");
        group->initialized = group->pending_initial_subscribers.len == 0;
    }
    if (rebind_pending) {
        jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_IMMEDIATE);
        *out = true;
    } else {
        self->observed_session_claim_revision = current_revision;
        *out = true;
    }
out_groups:
    for (u32 gi = 0; gi < groups.len; ++gi) {
        GroupSnapshot *g = &groups.ptr[gi];
        jazz_db__db__CoverageKey__drop(&g->coverage);
        jazz_model__query__ValidatedQuery__drop(g->shape);
        jazz_model__query__Binding__drop(g->binding);
        jazz_db__db__PolicyBinding__drop(&g->policy_binding);
        jz_vec_drop(&g->subscribers, sizeof(jazz_protocol_SubscriptionKey), NULL);
    }
    jz_vec_drop(&groups, sizeof(GroupSnapshot), NULL);
    for (u32 i = 0; i < upstream_replacements.len; ++i) {
        UpstreamReplacement *u = &upstream_replacements.ptr[i];
        jazz_db__db__CoverageKey__drop(&u->coverage);
        jazz_db__db__CoverageKey__drop(&u->old_coverage);
        if (u->shape) jazz_model__query__ValidatedQuery__drop(u->shape);
        if (u->binding) jazz_model__query__Binding__drop(u->binding);
        jazz_protocol__protocol__RegisterShapeOptions__drop(&u->opts);
        jz_btree_drop(&u->downstream_subscriptions);
    }
    jz_vec_drop(&upstream_replacements, sizeof(UpstreamReplacement), NULL);
    jz_btree_drop(&deferred_rehydrates);
    jz_btree_drop(&refreshed_authority_sources);
    jz_btree_drop(&refreshed_upstream_handles);
out_replacements:
    for (u32 i = 0; i < coverage_replacements.len; ++i) {
        jazz_db__db__CoverageKey__drop(&coverage_replacements.ptr[i].old);
        jazz_db__db__CoverageKey__drop(&coverage_replacements.ptr[i].refreshed);
    }
    jz_vec_drop(&coverage_replacements, sizeof(CoverageReplacement), NULL);
    jz_btree_drop(&stale_maintained_subscriptions);
    jazz_db__db__PolicyBinding__drop(&refreshed_direct_binding);
    jazz_db__db__PolicyBinding__drop(&session_claim_binding);
    return err;
}

/* ---------------------------------------------------------------- accessors */

/// The structured error the remote peer sent before ending this link, if any.
jazz_db_WireError *PC_FN(remote_wire_error)(const PC *self) {
    return jazz_db__db__node_runtime__Transport__remote_wire_error(&self->transport);
}

/// Return the serialized byte size of the latest resume/catch-up response
/// sent by this connection.
bool PC_FN(last_resume_bytes)(const PC *self, u32 *out) {
    if (self->has_last_resume_bytes) *out = self->last_resume_bytes;
    return self->has_last_resume_bytes;
}

/// Full-diff fallbacks of this connection's served maintained views.
/// Upstream links serve no subscriptions and report zero.
jazz_node_FullDiffFallbackMetrics PC_FN(full_diff_fallbacks)(const PC *self) {
    if (self->link.tag == JAZZ_LINK_SUBSCRIBER) return PEER_FN(full_diff_fallbacks)(self->link.v.subscriber.peer);
    jazz_node_FullDiffFallbackMetrics zero = {0, 0};
    return zero;
}

/// Extract this subscriber connection's resume cursor for a reconnect.
bool PC_FN(take_resume_cursor)(PC *self, jazz_db_ResumeCursor *out) {
    if (self->link.tag != JAZZ_LINK_SUBSCRIBER) return false;
    jazz_db_SubscriberConnectionState *s = &self->link.v.subscriber;
    jazz_node_PeerRole role = PEER_FN(role)(s->peer);
    jazz_node_PeerState *replacement = role.tag == JAZZ_PEER_ROLE_RELAY ? PEER_FN(relay)() : PEER_FN(client_link)(role.identity);
    out->peer = s->peer;
    s->peer = replacement;
    out->ingest_context = s->ingest_context;
    out->session_claims = s->session_claims;
    jz_btree_init(&s->session_claims, out->session_claims.d);
    out->session_claim_revision = s->session_claim_revision;
    return true;
}

/* ---------------------------------------------------------------- rehydrate / staging */

/// Rehydrate every view served on this subscriber link. This is used when an
/// authority installs its first permissions head or replaces it with a
/// tighter one: the client keeps the same subscription, but its visible
/// membership must be recalculated immediately.
jz_err PC_FN(rehydrate_subscriber_views)(PC *self) {
    jz_waker waker;
    bool has_waker = jazz_db__db__node_runtime__make_query_runtime_waker(self->scheduler, self->query_runtime_wake_pending, self->query_runtime_waker, &waker);
    const jz_waker *progress_waker = has_waker ? &waker : NULL;
    jazz_db_PolicyBinding session_claim_binding;
    bool has_binding = subscriber_session_claim_binding(self, &session_claim_binding);
    if (self->link.tag != JAZZ_LINK_SUBSCRIBER) return JZ_OK;
    jazz_db_SubscriberConnectionState *link = &self->link.v.subscriber;
    jazz_node_PeerState *peer = link->peer;
    PEER_FN(advance_authorization_progress)(peer);
    typedef struct {
        jazz_db_CoverageKey coverage;
        jazz_model_ValidatedQuery *shape;
        jazz_model_Binding *binding;
        jazz_db_PolicyBinding policy_binding;
        jazz_protocol_SubscriptionKey authority_result_subscription;
        bool awaiting_upstream_settlement;
        JZ_VEC(jazz_protocol_SubscriptionKey) subscribers;
    } Group;
    JZ_VEC(Group) groups = {0};
    {
        jazz_db_CoverageKey *coverage;
        jazz_db_CoverageGroup *group;
        JZ_BTREE_FOR(&link->coverage_groups, coverage, group) {
            Group g;
            memset(&g, 0, sizeof(g));
            jazz_db__db__CoverageKey__clone(&g.coverage, coverage);
            g.shape = jazz_model__query__ValidatedQuery__clone(group->shape);
            g.binding = jazz_model__query__Binding__clone(group->binding);
            jazz_db__db__PolicyBinding__clone(&g.policy_binding, &group->policy_binding);
            g.authority_result_subscription = group->authority_result_subscription;
            g.awaiting_upstream_settlement = group->awaiting_upstream_settlement;
            jazz_protocol_SubscriptionKey *sk;
            void *unit;
            JZ_BTREE_FOR(&group->subscribers, sk, unit) { (void)unit; jz_vec_push(&g.subscribers, sk, sizeof(*sk)); }
            jz_vec_push(&groups, &g, sizeof(g));
        }
    }
    jz_err err = JZ_OK;
    u32 gi = 0;
    for (; gi < groups.len; ++gi) {
        Group *g = &groups.ptr[gi];
        jazz_protocol_SubscriptionKey group_subscription = jazz_db__db__peer_connection__coverage_group_subscription_key(&g->coverage);
        bool scope_relay;
        {
            jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
            scope_relay = NS_FN(client_relay_scope)(n) != NULL;
            jazz_db_node_unlock(self->node);
        }
        PEER_FN(set_subscription_policy_binding)(peer, group_subscription, g->policy_binding.identity, g->policy_binding.claims);
        memset(&g->policy_binding.claims, 0, sizeof(g->policy_binding.claims));
        if (g->awaiting_upstream_settlement) {
            jazz_protocol_AuthorityResultKey authority_result_source;
            jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
            jz_err e = NS_FN(authority_result_key_for_subscription)(n, g->authority_result_subscription, &authority_result_source);
            jazz_db_node_unlock(self->node);
            if (e.code) {
                /* A strict scope relay may not rehydrate from its overlay or
                 * guess a sibling's receipt. */
                jz_err_drop(&e);
                link->serve_dirty = true;
                continue;
            }
            if (scope_relay) {
                n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
                bool settled = NS_FN(has_settled_authority_result)(n, &authority_result_source);
                jazz_db_node_unlock(self->node);
                if (!settled) {
                    /* Recovery cannot turn an unsettled strict relay read into
                     * a definitive empty result. */
                    jazz_protocol__protocol__AuthorityResultKey__drop(&authority_result_source);
                    link->serve_dirty = true;
                    continue;
                }
            }
            PEER_FN(set_subscription_authority_result_source)(peer, group_subscription, authority_result_source);
            PEER_FN(set_subscription_awaiting_selected_authority_source)(peer, group_subscription, scope_relay);
        } else {
            PEER_FN(set_subscription_awaiting_selected_authority_source)(peer, group_subscription, false);
        }
        bool has_update = false;
        jazz_protocol_SyncMessage update;
        {
            if (!has_binding) jz_panic("subscriber claims");
            jazz_node_NodeState *owner = jazz_db_node_lock(self->node);
            jz_btree claims;
            claims_clone(&claims, &session_claim_binding.claims);
            jazz_node_NodeState *scoped = NS_FN(scoped_active_session_claims)(owner, session_claim_binding.identity, claims);
            jazz_protocol_RegisterShapeOptions opts;
            jazz_protocol__protocol__RegisterShapeOptions__clone(&opts, &g->coverage.opts);
            jz_err e = PEER_FN(rehydrate_query_for_subscription_with_opts_and_waker)(peer, scoped, group_subscription, g->shape, g->binding, opts, progress_waker, &has_update, &update);
            NS_FN(end_scoped_claims)(owner);
            jazz_db_node_unlock(self->node);
            if (e.code) { err = node_err(e); break; }
        }
        if (!has_update) { link->serve_dirty = true; continue; }
        bool has_shared = true;
        for (u32 s = 0; s < g->subscribers.len; ++s) {
            jazz_protocol_SyncMessage next = take_last_or_clone(&has_shared, &update, s + 1 < g->subscribers.len);
            jazz_protocol_SyncMessage retargeted = retarget_view_update(next, g->subscribers.ptr[s]);
            stamp_view_update_authorization_progress_from(peer, group_subscription, &retargeted);
            self->has_last_resume_bytes = true;
            self->last_resume_bytes = jazz_protocol__protocol__SyncMessage__postcard_len(&retargeted);
            err = send_subscriber_with_sync_context_full(self->node, peer, &self->transport, self->local_fate_routes, self->downstream_fates, retargeted);
            if (err.code) break;
        }
        if (has_shared) jazz_protocol__protocol__SyncMessage__drop(&update);
        if (err.code) break;
    }
    if (!err.code) link->serve_dirty = true;
    for (u32 i = 0; i < groups.len; ++i) {
        Group *g = &groups.ptr[i];
        jazz_db__db__CoverageKey__drop(&g->coverage);
        jazz_model__query__ValidatedQuery__drop(g->shape);
        jazz_model__query__Binding__drop(g->binding);
        jazz_db__db__PolicyBinding__drop(&g->policy_binding);
        jz_vec_drop(&g->subscribers, sizeof(jazz_protocol_SubscriptionKey), NULL);
    }
    jz_vec_drop(&groups, sizeof(Group), NULL);
    if (has_binding) jazz_db__db__PolicyBinding__drop(&session_claim_binding);
    return err;
}

static bool handle_transport_backpressure(jazz_db_NodeMutex *node, jazz_db_SharedTickScheduler *scheduler, const jazz_db_TransportError *error);

/// Preserve every frame currently staged on this link across an authority
/// handoff, but make its ViewUpdates ineligible as the new authority's
/// receipt. The next transport arrival is therefore the first receipt
/// candidate after selection.
void PC_FN(stage_inbound_without_authority_receipt)(PC *self) {
    for (u32 i = 0; i < self->staged_inbound.len; ++i)
        ((jazz_db_StagedInboundMessage *)jz_deque_at(&self->staged_inbound, i))->authority_receipt_eligible = false;
    if (self->link.tag == JAZZ_LINK_UPSTREAM) {
        jazz_db_UpstreamConnectionState *up = &self->link.v.upstream;
        /* A surviving link's old delta chain is not a newly selected
         * authority receipt. Reissue its admitted opens and ignore deltas
         * until each independent snapshot arrives. Never reconstruct claims. */
        jazz_protocol_SubscriptionKey *key;
        jazz_db_PendingUpstreamSubscription *request;
        JZ_BTREE_FOR(&up->sent_subscriptions, key, request) {
            bool unsubscribing = false, reopening = false;
            for (u32 c = 0; c < up->pending.len; ++c) {
                const jazz_db_PendingUpstreamCommand *command = &up->pending.ptr[c];
                if (command->tag == JAZZ_PUC_UNSUBSCRIBE && jazz_protocol_SubscriptionKey_eq(&command->unsubscribe, key)) unsubscribing = true;
                if (pending_subscribe_names(command, key)) reopening = true;
            }
            if (unsubscribing) continue;
            bool inserted = false;
            jazz_types_GlobalTime *t = (jazz_types_GlobalTime *)jz_btree_entry(&up->awaiting_support_snapshots, key, &inserted);
            if (inserted) *t = 0;
            if (!reopening) {
                jazz_db_PendingUpstreamCommand *command = (jazz_db_PendingUpstreamCommand *)jz_vec_push_slot(&up->pending, sizeof(jazz_db_PendingUpstreamCommand));
                memset(command, 0, sizeof(*command));
                command->tag = JAZZ_PUC_SUBSCRIBE;
                jazz_db__db__PendingUpstreamSubscription__clone(&command->subscribe, request);
            }
        }
    }
    for (;;) {
        bool has = false;
        jazz_db_ReceivedSyncMessage message;
        jazz_db_TransportError error;
        jz_err e = jazz_db__db__node_runtime__Transport__try_recv_owned_result(&self->transport, &has, &message, &error);
        if (e.code == 0) {
            if (!has) { self->inbound_authority_receipt_quarantine = false; break; }
            jazz_db_StagedInboundMessage staged = { message.message, message.lease, false, message.receipts_validated };
            jz_deque_push_back(&self->staged_inbound, &staged);
            continue;
        }
        jz_err_drop(&e);
        if (handle_transport_backpressure(self->node, self->scheduler, &error)) {
            jazz_protocol__wire__TransportError__drop(&error);
            self->inbound_authority_receipt_quarantine = true;
            break;
        }
        if (self->has_startup_error) jz_err_drop(&self->startup_error);
        self->has_startup_error = true;
        self->startup_error = jazz_db__db__peer_connection__transport_error(error);
        break;
    }
}

static jz_err tick_inner(PC *self, jazz_db_DbTickStats *out);

/// Service this connection once: drain inbound, apply, wake subscriptions,
/// and flush pending outbound. Non-blocking; the binding calls it in its loop.
jz_err PC_FN(tick)(PC *self, jazz_db_DbTickStats *out) {
    jz_err result = tick_inner(self, out);
    if (result.code == 0) {
        jazz_db_WireFlushStatus status;
        jazz_db_TransportError error;
        jz_err e = jazz_db__db__node_runtime__Transport__poll_flush(&self->transport, &status, &error);
        if (e.code == 0) {
            if (status == JAZZ_FLUSH_MORE_READY) jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_AFTER_CURRENT_TURN);
        } else {
            jz_err_drop(&e);
            result = jazz_db__db__peer_connection__transport_error(error);
        }
    }
    if (result.code == 0) {
        u64 delay_ms;
        if (jazz_db__db__node_runtime__Transport__incomplete_receive_timeout_ms(&self->transport, &delay_ms) && self->scheduler->has)
            self->scheduler->s.vt->schedule_tick_after(self->scheduler->s.self, delay_ms);
    }
    if (result.code && self->link.tag == JAZZ_LINK_UPSTREAM)
        jazz_db__db__finish_open_schema_connection(self->open_schema_admission, self->connection_epoch, err_clone(&result));
    return result;
}

static jz_err retry_local_replay_after_progress(PC *self) {
    u64 epoch = *self->subscriber_dirty_epoch;
    if (self->local_replay_epoch == epoch) return JZ_OK;
    self->local_replay_epoch = epoch;
    if (self->link.tag != JAZZ_LINK_SUBSCRIBER || !self->link.v.subscriber.local_receiver) return JZ_OK;
    jazz_types_AuthorSubject author = self->link.v.subscriber.ingest_context.identity;
    jazz_db_UploadOutbox *outbox = (jazz_db_UploadOutbox *)jz_rc_clone(self->link.v.subscriber.outbox);
    bool blocked = false;
    jazz_model_TxId *tx;
    jazz_db_LocalFateRouteVec *routes;
    JZ_BTREE_FOR(self->local_fate_routes, tx, routes) {
        (void)tx;
        for (u32 i = 0; i < routes->len; ++i) {
            const jazz_db_LocalFateRoute *route = &routes->ptr[i];
            if (!route->replay_ready && route->has_replay_author && jazz_types_AuthorSubject_eq(route->replay_author, author)) { blocked = true; break; }
        }
        if (blocked) break;
    }
    jz_err e = JZ_OK;
    if (blocked) e = jazz_db__db__restore_local_subscriber_replay(self->node, outbox, self->local_fate_routes, author, self->downstream_fates);
    jz_rc_release(outbox);
    return e;
}

/* ---------------------------------------------------------------- dirty epochs / scheduling */

bool PC_FN(mark_subscriber_dirty)(PC *self) {
    if (self->link.tag != JAZZ_LINK_SUBSCRIBER) return false;
    self->link.v.subscriber.serve_dirty = true;
    self->observed_subscriber_dirty_epoch = *self->subscriber_dirty_epoch;
    return true;
}

static void observe_shared_subscriber_dirty_epoch(PC *self) {
    u64 epoch = *self->subscriber_dirty_epoch;
    if (self->observed_subscriber_dirty_epoch == epoch) return;
    self->observed_subscriber_dirty_epoch = epoch;
    if (self->link.tag == JAZZ_LINK_SUBSCRIBER) self->link.v.subscriber.serve_dirty = true;
}

void jazz_db__db__peer_connection__schedule_tick_in(jazz_db_SharedTickScheduler *scheduler, jazz_db_TickUrgency urgency) {
    if (scheduler->has) scheduler->s.vt->schedule_tick(scheduler->s.self, urgency);
}

static u32 serialized_sync_message_len(const jazz_protocol_SyncMessage *message) {
    u32 len = 0;
    if (!jazz_db__wire__encoded_sync_message_len(message, &len)) return 0;
    return len;
}

static void view_update_parts_from_message(jazz_protocol_SyncMessage message, jazz_node_ViewUpdateParts *out) {
    if (message.tag != JAZZ_SM_VIEW_UPDATE) jz_panic("expected view update message");
    jazz_protocol_ViewUpdatePayload *payload = message.v.view_update;
    memset(out, 0, sizeof(*out));
    out->wire_rows = payload->supporting_rows;
    out->subscription = payload->subscription;
    out->settled_through = payload->settled_through;
    out->defer_settlement = false;
    out->reset_input_set = true;
    out->version_carriers = payload->version_carriers;
    memcpy(&out->peer_complete_tx_payload_refs, &payload->peer_payload_inventory.complete_tx_payloads, sizeof(out->peer_complete_tx_payload_refs));
    out->has_authorization_progress = payload->peer_payload_inventory.has_authorization_progress;
    out->authorization_progress = payload->peer_payload_inventory.authorization_progress;
    out->opening_pending = payload->peer_payload_inventory.opening_pending;
    jz_free(payload); /* fields moved out */
}

static void push_view_update_message_for_receiver(jazz_db_PendingAuthorityViewUpdateVec *ready, jazz_protocol_SyncMessage message, bool authority_receipt_eligible) {
    jazz_db_PendingAuthorityViewUpdate update;
    view_update_parts_from_message(message, &update.parts);
    update.authority_receipt_eligible = authority_receipt_eligible;
    jz_vec_push(ready, &update, sizeof(update));
}

static void stage_initial_coverage_clear_for_update(const jazz_protocol_SyncMessage *update, const jz_btree *latest, jz_btree *clears) {
    if (update->tag != JAZZ_SM_VIEW_UPDATE) return;
    const jazz_protocol_ViewUpdatePayload *payload = update->v.view_update;
    if (payload->peer_payload_inventory.opening_pending) return;
    jazz_db_CoverageKey *coverage;
    jazz_protocol_SubscriptionKey *current;
    JZ_BTREE_FOR(latest, coverage, current) {
        if (jazz_protocol_SubscriptionKey_eq(current, &payload->subscription)) {
            jazz_db_CoverageKey k;
            jazz_db__db__CoverageKey__clone(&k, coverage);
            if (jz_btree_insert(clears, &k, NULL, NULL)) jazz_db__db__CoverageKey__drop(&k);
            return;
        }
    }
}

/* ---------------------------------------------------------------- authority view application */

static bool frame_is_selected(bool authority_link_selected, const jazz_db_PendingAuthorityViewUpdate *update) {
    return authority_link_selected && update->authority_receipt_eligible;
}

typedef struct { jazz_protocol_SubscriptionKey subscription; jazz_types_GlobalTime settled_through; } ConfirmedSubscription;
typedef struct { jazz_protocol_SubscriptionKey subscription; jazz_protocol_BindingViewKey binding_view; jazz_types_GlobalTime settled_through; } ConfirmedBindingView;

static jz_err apply_pending_authority_view_updates(jazz_db_NodeMutex *node, jazz_db_SubscriptionList *subscriptions, jazz_db_PendingAuthorityViewUpdateVec *pending,
                                                  jz_btree *relay_owners, jz_btree *relay_rejections, jazz_db_PendingUpstreamCommands *upstream,
                                                  jz_btree *awaiting, jz_btree *clears, jz_btree *query_coverage_registrations,
                                                  jazz_db_ActiveAuthorityViewReceipts *active_authority_view_receipts, jz_btree *coverage_refresh_generations,
                                                  u64 *subscriber_dirty_epoch, jazz_db_SharedTickScheduler *scheduler, const jz_waker *progress_waker, u64 connection_epoch) {
    (void)progress_waker;
    /* A replacement authority selection is connection-scoped. Frames which
     * were already queued on an older transport retain their
     * transport-local `authority_receipt_eligible` bit, so combine it with
     * the currently selected connection here. */
    bool authority_link_selected = !active_authority_view_receipts->has || active_authority_view_receipts->connection_epoch == connection_epoch;
    JZ_VEC(ConfirmedSubscription) confirmed_subscriptions = {0};
    jz_btree source_confirmations;
    jz_btree_init(&source_confirmations, &jazz_db_subscription_key_set_desc);
    jazz_types_GlobalTime batch_cut = 0;
    bool has_ineligible_cut = false;
    jazz_types_GlobalTime ineligible_cut = 0;
    jz_btree publishing_subscriptions;
    jz_btree_init(&publishing_subscriptions, &jazz_db_subscription_key_set_desc);
    for (u32 i = 0; i < pending->len; ++i) {
        const jazz_db_PendingAuthorityViewUpdate *update = &pending->ptr[i];
        bool selected = frame_is_selected(authority_link_selected, update);
        if (selected && !update->parts.opening_pending) {
            ConfirmedSubscription c = { update->parts.subscription, update->parts.settled_through };
            jz_vec_push(&confirmed_subscriptions, &c, sizeof(c));
            if (!update->parts.defer_settlement
                && (update->parts.reset_input_set || jazz_node__node__ViewUpdateParts__supporting_adds_len(&update->parts) != 0
                    || jazz_node__node__ViewUpdateParts__supporting_removes_len(&update->parts) != 0))
                jz_btree_insert(&source_confirmations, &update->parts.subscription, NULL, NULL);
        }
        if (update->parts.settled_through > batch_cut) batch_cut = update->parts.settled_through;
        if (!selected) {
            if (!has_ineligible_cut || update->parts.settled_through > ineligible_cut) ineligible_cut = update->parts.settled_through;
            has_ineligible_cut = true;
        } else {
            jz_btree_insert(&publishing_subscriptions, &update->parts.subscription, NULL, NULL);
        }
    }
    JZ_VEC(ConfirmedBindingView) confirmed_binding_views = {0};
    bool relay_authority_session_owner;
    {
        jazz_node_NodeState *node_ref = jazz_db__db__LocalMutexBorrow__borrow(node);
        relay_authority_session_owner = NS_FN(client_relay_scope)(node_ref) != NULL;
        for (u32 i = 0; i < confirmed_subscriptions.len; ++i) {
            jazz_protocol_BindingViewKey binding_view;
            jz_err e = NS_FN(binding_view_key_for_subscription)(node_ref, confirmed_subscriptions.ptr[i].subscription, &binding_view);
            if (e.code) { jz_err_drop(&e); continue; }
            ConfirmedBindingView c = { confirmed_subscriptions.ptr[i].subscription, binding_view, confirmed_subscriptions.ptr[i].settled_through };
            jz_vec_push(&confirmed_binding_views, &c, sizeof(c));
        }
        jazz_db_node_unlock(node);
    }
    if (active_authority_view_receipts->has) {
        jazz_db_ActiveAuthorityViewReceipts *receipts = active_authority_view_receipts;
        bool has_invalidation = false;
        jazz_types_GlobalTime invalidation_cut = 0;
        if (receipts->connection_epoch != connection_epoch) { has_invalidation = true; invalidation_cut = batch_cut; }
        else if (has_ineligible_cut) { has_invalidation = true; invalidation_cut = ineligible_cut; }
        if (has_invalidation) {
            /* A nonselected upstream update may recompute binding views
             * beyond the wire subscription it names. Until that dependency
             * closure is proven exact, no receipt remains safe. */
            jz_btree_clear(&receipts->binding_views);
            jz_btree_clear(&receipts->subscriptions);
            if (invalidation_cut > receipts->confirmation_floor) receipts->confirmation_floor = invalidation_cut;
            /* Make the receipt demotion visible before this stale authority
             * frame can publish any ordinary update. */
            jazz_db__db__demote_authority_receipt_subscriptions(subscriptions, &publishing_subscriptions);
        }
    }
    /* A frame from a nonselected upstream is only evidence that the selected
     * receipt is no longer current. It is not an input to that receipt's
     * receiver-local graph. */
    jazz_protocol_VersionCarrierVec unselected_carriers = {0};
    jazz_node_ViewUpdatePartsVec updates = {0};
    for (u32 i = 0; i < pending->len; ++i) {
        jazz_db_PendingAuthorityViewUpdate *update = &pending->ptr[i];
        if (!frame_is_selected(authority_link_selected, update)) {
            for (u32 c = 0; c < update->parts.version_carriers.len; ++c) {
                jazz_protocol_VersionCarrier *carrier = jazz_protocol__protocol__VersionCarrier__clone(update->parts.version_carriers.ptr[c]);
                jz_vec_push(&unselected_carriers, &carrier, sizeof(carrier));
            }
            jazz_node__node__ViewUpdateParts__drop(&update->parts);
        } else {
            jz_vec_push(&updates, &update->parts, sizeof(update->parts));
        }
    }
    pending->len = 0;
    jz_err err = JZ_OK;
    if (unselected_carriers.len != 0) {
        jazz_node_NodeState *n = jazz_db_node_lock(node);
        jz_err e = NS_FN(ingest_unselected_authority_view_bundles)(n, &unselected_carriers);
        jazz_db_node_unlock(node);
        if (e.code) { err = node_err(e); jz_vec_drop(&updates, sizeof(jazz_node_ViewUpdateParts), jazz_node__node__ViewUpdateParts__drop); goto out; }
    }
    if (updates.len != 0) {
        jazz_node_NodeState *node_ref = jazz_db_node_lock(node);
        jz_err result = NS_FN(apply_view_updates_in_batch)(node_ref, updates);
        if (result.code == 0) {
            jazz_types_GlobalTime authoritative_cut = 0;
            for (u32 i = 0; i < confirmed_subscriptions.len; ++i)
                if (confirmed_subscriptions.ptr[i].settled_through > authoritative_cut) authoritative_cut = confirmed_subscriptions.ptr[i].settled_through;
            NS_FN(record_authoritative_settled_through)(node_ref, authoritative_cut);
            jazz_db_node_unlock(node);
        } else if (result.code == JAZZ_NODE_ERR_INVALID_AUTHORITY_SOURCE_CLOSURE) {
            jazz_db_node_unlock(node);
            u32 public_rejections = route_invalid_authority_source_closure(subscriptions, &result);
            jazz_protocol_SubscriptionKey subscription;
            const void *transition = NULL;
            if (!jazz_node__node__Error__as_invalid_authority_source_closure(&result, &subscription, &transition)) jz_panic("unreachable");
            bool one_shot_owner = jz_btree_contains(query_coverage_registrations, &subscription);
            jazz_protocol_SubscribeRejectReason reason;
            jazz_protocol__protocol__SubscribeRejectReason__invalid_authority_source_closure(&reason, transition);
            u32 relay_rejections_queued = queue_relay_subscription_rejection(relay_owners, relay_rejections, upstream, subscription, &reason);
            jazz_protocol__protocol__SubscribeRejectReason__drop(&reason);
            if (relay_rejections_queued > 0) {
                *subscriber_dirty_epoch += 1;
                jazz_db__db__peer_connection__schedule_tick_in(scheduler, JAZZ_TICK_IMMEDIATE);
            }
            /* One-shot attachments have no public stream sender. If no
             * downstream owner receives the scoped rejection either, surface
             * the original protocol error through the owner tick. */
            if (one_shot_owner || (public_rejections == 0 && relay_rejections_queued == 0)) { err = node_err(result); goto out; }
            jz_err_drop(&result);
            goto out;
        } else {
            jazz_db_node_unlock(node);
            err = node_err(result);
            goto out;
        }
    }
    /* Per-owner receipt revisions survive replacement of a shared binding
     * slot. Only successfully applied selected source receipts advance
     * reconciliation. */
    for (u32 i = 0; i < subscriptions->len; ++i) {
        jazz_db_SubscriptionState *state = jazz_db_weak_upgrade(subscriptions->ptr[i]);
        if (!state) continue;
        for (u32 h = 0; h < state->upstream_subscription_handles.len; ++h)
            if (jz_btree_contains(&source_confirmations, &state->upstream_subscription_handles.ptr[h].subscription)) {
                state->scalar_authority_revision += 1;
                break;
            }
        jz_rc_release(state);
    }
    {
        jazz_db_RelayOwnerKey *k;
        jazz_db_RelayUpstreamSubscriptionOwner *owner;
        JZ_BTREE_FOR(relay_owners, k, owner) {
            (void)k;
            if (jz_btree_contains(&source_confirmations, &owner->request.subscription)) owner->scalar_authority_revision += 1;
        }
    }
    if (relay_authority_session_owner) {
        /* A relay authority view is input to every locally served browser
         * Global child. Advance the shared generation only after the
         * validated batch commits. */
        *subscriber_dirty_epoch += 1;
        jazz_db__db__peer_connection__schedule_tick_in(scheduler, JAZZ_TICK_IMMEDIATE);
    }
    if (active_authority_view_receipts->has && active_authority_view_receipts->connection_epoch == connection_epoch) {
        for (u32 i = 0; i < confirmed_binding_views.len; ++i) {
            ConfirmedBindingView *c = &confirmed_binding_views.ptr[i];
            if (c->settled_through < active_authority_view_receipts->confirmation_floor) continue;
            /* Public streams, relay scopes, and auxiliary queries all require
             * their own accepted usage receipt. */
            jz_btree_insert(&active_authority_view_receipts->binding_views, &c->binding_view, NULL, NULL);
            jz_btree_insert(&active_authority_view_receipts->subscriptions, &c->subscription, NULL, NULL);
        }
    }
    if (clears->len != 0) {
        jazz_db_CoverageKey coverage;
        while (jz_btree_pop_first(clears, &coverage, NULL)) {
            jz_btree_remove(awaiting, &coverage, NULL, NULL);
            /* Coalescing is valid only while this relay authority reply
             * remains outstanding. */
            if (relay_authority_session_owner) jz_btree_remove(coverage_refresh_generations, &coverage, NULL, NULL);
            jazz_db__db__CoverageKey__drop(&coverage);
        }
    }
out:
    jz_vec_drop(&unselected_carriers, sizeof(jazz_protocol_VersionCarrier *), NULL);
    jz_vec_drop(&confirmed_subscriptions, sizeof(ConfirmedSubscription), NULL);
    jz_vec_drop(&confirmed_binding_views, sizeof(ConfirmedBindingView), NULL);
    jz_btree_drop(&source_confirmations);
    jz_btree_drop(&publishing_subscriptions);
    return err;
}

jz_err jazz_db__db__peer_connection__transport_error(jazz_db_TransportError error) {
    if (error.tag == JAZZ_TRANSPORT_BACKPRESSURE) {
        jazz_protocol__wire__TransportError__drop(&error);
        return jz_err_new(JAZZ_EC_BACKPRESSURE, "transport backpressure");
    }
    jz_err e = jz_err_fmt(JAZZ_EC_PROTOCOL, "%.*s", (int)error.message.len, error.message.ptr);
    jazz_protocol__wire__TransportError__drop(&error);
    return e;
}

/* ---------------------------------------------------------------- authorization scope serving */

static bool query_root_filters_reference_id(const jazz_model_Query *query);

static jazz_protocol_PermissionAdvice evaluate_authoritative_permission_advice(jazz_node_NodeState *node, jazz_types_AuthorSubject identity, jazz_protocol_PermissionAdviceAction *action) {
    jz_err result = JZ_OK;
    bool allowed = false;
    switch (action->tag) {
    case JAZZ_PAA_INSERT: {
        const jazz_node_TableSchema *schema = NULL;
        jz_err e = NS_FN(table)(node, jz_str_slice(&action->table), &schema);
        if (e.code) { jz_err_drop(&e); jazz_protocol__protocol__PermissionAdviceAction__drop(action); return JAZZ_ADVICE_UNKNOWN; }
        const jazz_model_Query *insert_check = jazz_node__node__TableSchema__insert_check(schema);
        if (insert_check && query_root_filters_reference_id(insert_check)) { jazz_protocol__protocol__PermissionAdviceAction__drop(action); return JAZZ_ADVICE_UNKNOWN; }
        jazz_types_RowUuid zero;
        memset(&zero, 0, sizeof(zero));
        jazz_node_MergeableCommit *commit = jazz_node__node__MergeableCommit__new(jz_str_slice(&action->table), zero, 0);
        jazz_node__node__MergeableCommit__made_by(commit, identity);
        jazz_node__node__MergeableCommit__permission_subject(commit, identity);
        jazz_node__node__MergeableCommit__cells(commit, action->cells);
        jz_btree_init(&action->cells, action->cells.d);
        result = NS_FN(dry_run_insert_allows)(node, commit, &allowed);
        break;
    }
    case JAZZ_PAA_READ:
        result = NS_FN(dry_run_read_current_allows)(node, jz_str_slice(&action->table), action->row, identity, &allowed);
        break;
    case JAZZ_PAA_UPDATE: {
        bool exists = false;
        result = NS_FN(local_current_row_exists)(node, jz_str_slice(&action->table), action->row, &exists);
        if (result.code) break;
        if (!exists) { allowed = false; break; }
        jazz_node_MergeableCommit *commit = jazz_node__node__MergeableCommit__new(jz_str_slice(&action->table), action->row, 0);
        jazz_node__node__MergeableCommit__made_by(commit, identity);
        jazz_node__node__MergeableCommit__permission_subject(commit, identity);
        jazz_node__node__MergeableCommit__cells(commit, action->patch);
        jz_btree_init(&action->patch, action->patch.d);
        result = NS_FN(dry_run_insert_allows)(node, commit, &allowed);
        break;
    }
    default: /* Delete */
        result = NS_FN(dry_run_delete_current_allows)(node, jz_str_slice(&action->table), action->row, identity, &allowed);
        break;
    }
    jazz_protocol__protocol__PermissionAdviceAction__drop(action);
    if (result.code) { jz_err_drop(&result); return JAZZ_ADVICE_UNKNOWN; }
    return allowed ? JAZZ_ADVICE_ALLOWED : JAZZ_ADVICE_DENIED;
}

/// Temporary proof receivers never outlive the request. Keep the node owner
/// through cleanup so cancellation cannot require a synchronous re-lock.
typedef struct AuthorizationScopeReceivers {
    jazz_node_PeerState *peer;
    jazz_node_NodeState *node;
    JZ_VEC(jazz_protocol_SubscriptionKey) subscriptions;
} AuthorizationScopeReceivers;

static void AuthorizationScopeReceivers_drop(AuthorizationScopeReceivers *self) {
    for (u32 i = 0; i < self->subscriptions.len; ++i) PEER_FN(forget_subscription_with_node)(self->peer, self->node, self->subscriptions.ptr[i]);
    jz_vec_drop(&self->subscriptions, sizeof(jazz_protocol_SubscriptionKey), NULL);
}

static void queue_scope_unavailable(jz_deque *pending, jazz_protocol_PermissionAdviceRequestId request_id) {
    jazz_protocol_SyncMessage m;
    memset(&m, 0, sizeof(m));
    m.tag = JAZZ_SM_AUTHORIZATION_SCOPE_UNAVAILABLE;
    m.v.scope_unavailable.request_id = request_id;
    queue_direct_control(pending, m);
}

static groove_ValueVec binding_values_in_param_order(const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding);

typedef struct { jazz_model_ShapeId shape_id; jazz_model_BindingId binding_id; } ShapeBindingId;
static int cmp_shape_binding(const void *a, const void *b, void *ctx) { (void)ctx; return memcmp(a, b, sizeof(ShapeBindingId)); }
static const jz_btree_desc shape_binding_set_desc = { sizeof(ShapeBindingId), 0, cmp_shape_binding, NULL, NULL };

enum { MAX_AUTHORIZATION_SCOPES = JAZZ_DB_MAX_AUTHORIZATION_SCOPES };

/// Compile and serve an authorization scope entirely at the serving authority.
///
/// This intentionally does not reuse the subscriber's shape registry or any
/// caller-provided subscription. The authority allocates opaque usage-site
/// keys, registers canonical shapes in the receiver, and only then sends the
/// ordinary view updates in authority-scope envelopes.
static jz_err serve_authorization_scope_intent(jazz_db_NodeMutex *node, jazz_node_PeerState *peer, jz_deque *pending_control_responses,
                                              jazz_types_AuthorSubject identity, jz_btree session_claims, u64 connection_epoch,
                                              jazz_protocol_PermissionAdviceRequestId request_id, jazz_protocol_PermissionAdviceAction *action, u8 trust,
                                              jz_btree *hydrations, u64 *hydration_count) {
    jz_err err = JZ_OK;
    {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
        bool ready = NS_FN(is_history_complete)(n) && jazz_db__db__subscriber_permissions_ready(NS_FN(permissions_ready)(n), trust);
        jazz_db_node_unlock(node);
        if (!ready) {
            queue_scope_unavailable(pending_control_responses, request_id);
            goto out_action;
        }
    }
    jazz_node_AuthorizationSupportScope scope;
    {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
        jz_err e = NS_FN(authorization_support_scope_for_session)(n, identity, &session_claims, action, &scope);
        jazz_db_node_unlock(node);
        if (e.code) {
            jz_err_drop(&e);
            queue_scope_unavailable(pending_control_responses, request_id);
            goto out_action;
        }
    }
    jz_btree seen_support;
    jz_btree_init(&seen_support, &shape_binding_set_desc);
    JZ_VEC(u32) support_clauses = {0}; /* indices into scope.subscriptions */
    for (u32 i = 0; i < scope.subscriptions.len; ++i) {
        ShapeBindingId id = { jazz_model__query__ValidatedQuery__shape_id(scope.subscriptions.ptr[i].shape), jazz_model__query__Binding__binding_id(scope.subscriptions.ptr[i].binding) };
        if (!jz_btree_insert(&seen_support, &id, NULL, NULL)) jz_vec_push(&support_clauses, &i, sizeof(i));
    }
    jz_btree_drop(&seen_support);
    if (support_clauses.len > 0xffff) { err = protocol_err("authorization scope has too many clauses"); goto out_scope; }
    u16 clause_count = (u16)support_clauses.len;
    if (clause_count == 0) {
        jazz_protocol_PermissionAdvice advice;
        {
            jazz_node_NodeState *owner = jazz_db_node_lock(node);
            jz_btree claims;
            claims_clone(&claims, &session_claims);
            jazz_node_NodeState *scoped = NS_FN(scoped_active_session_claims)(owner, identity, claims);
            advice = evaluate_authoritative_permission_advice(scoped, identity, action);
            action = NULL;
            NS_FN(end_scoped_claims)(owner);
            jazz_db_node_unlock(node);
        }
        jazz_protocol_SyncMessage m;
        memset(&m, 0, sizeof(m));
        m.tag = JAZZ_SM_AUTHORIZATION_SCOPE_DECISION;
        m.v.scope_decision.request_id = request_id;
        m.v.scope_decision.advice = (u8)advice;
        queue_direct_control(pending_control_responses, m);
        goto out_scope;
    }
    u64 current_claims_revision, current_policy_epoch;
    jazz_types_GlobalTime current_cut;
    {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
        current_claims_revision = NS_FN(session_claim_revision)(n, identity);
        current_policy_epoch = NS_FN(active_catalogue_seq)(n);
        current_cut = NS_FN(committed_global_time)(n);
        jazz_db_node_unlock(node);
    }
    /* A cache entry is evidence, not a generic response cache. Prune every
     * revision/cut mismatch before looking up this compiled support key. */
    jazz_db__db__retain_current_scope_hydrations(hydrations, current_claims_revision, current_policy_epoch, current_cut);
    {
        const jazz_db_ServedAuthorizationScopeHydration *hydration = (const jazz_db_ServedAuthorizationScopeHydration *)jz_btree_get(hydrations, &scope.key);
        if (hydration) {
            jazz_db_ServedAuthorizationScopeHydration copy;
            jazz_db__db__ServedAuthorizationScopeHydration__clone(&copy, hydration);
            queue_authorization_scope_sequence(pending_control_responses, request_id, jazz_protocol__protocol__AuthorizationSupportScopeKey__clone(scope.key), copy);
            goto out_scope;
        }
    }
    if (*hydration_count != UINT64_MAX) *hydration_count += 1;
    jazz_protocol_AuthorityScopeAggregate *aggregate;
    {
        JZ_VEC(ShapeBindingId) ids = {0};
        for (u32 c = 0; c < support_clauses.len; ++c) {
            const jazz_node_ScopeSubscription *s = &scope.subscriptions.ptr[support_clauses.ptr[c]];
            ShapeBindingId id = { jazz_model__query__ValidatedQuery__shape_id(s->shape), jazz_model__query__Binding__binding_id(s->binding) };
            jz_vec_push(&ids, &id, sizeof(id));
        }
        aggregate = jazz_protocol__authorization_scope__AuthorityScopeAggregate__new(ids.ptr, ids.len);
        jz_vec_drop(&ids, sizeof(ShapeBindingId), NULL);
    }
    jazz_node_NodeState *node_owner = jazz_db_node_lock(node);
    AuthorizationScopeReceivers receivers = { peer, node_owner, {0} };
    JZ_VEC(jazz_db_ServedAuthorizationScopeClause) served_clauses = {0};
    bool unavailable = false;
    for (u32 index = 0; index < support_clauses.len; ++index) {
        const jazz_node_ScopeSubscription *clause = &scope.subscriptions.ptr[support_clauses.ptr[index]];
        const jazz_model_ValidatedQuery *shape = clause->shape;
        const jazz_model_Binding *binding = clause->binding;
        jazz_protocol_SubscriptionKey subscription;
        subscription.shape_id = jazz_model__query__ValidatedQuery__shape_id(shape);
        /* This usage-site key belongs to the authority, and cannot be
         * inferred from the canonical binding identity. */
        subscription.binding_id.u = jz_uuid_v4();
        subscription.read_view = jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&scope.options);
        ShapeBindingId id = { subscription.shape_id, jazz_model__query__Binding__binding_id(binding) };
        if (!jazz_protocol__authorization_scope__AuthorityScopeAggregate__register(aggregate, subscription, &id)) { unavailable = true; break; }
        jz_err supported;
        {
            jz_btree claims;
            claims_clone(&claims, &session_claims);
            jazz_node_NodeState *scoped = NS_FN(scoped_active_session_claims)(receivers.node, identity, claims);
            supported = NS_FN(ensure_peer_maintained_subscription_view_supported)(scoped, shape, binding, scope.options.tier, identity, scope.options.read_view, JAZZ_QAM_TRUSTED_SERVING);
            NS_FN(end_scoped_claims)(receivers.node);
        }
        if (supported.code) { jz_err_drop(&supported); unavailable = true; break; }
        groove_ValueVec values = binding_values_in_param_order(shape, binding);
        jazz_protocol_SyncMessage register_;
        memset(&register_, 0, sizeof(register_));
        register_.tag = JAZZ_SM_REGISTER_SHAPE;
        register_.v.register_shape.shape_id = subscription.shape_id;
        register_.v.register_shape.ast = jazz_protocol__protocol__ShapeAst__from_validated(shape);
        jazz_protocol__protocol__RegisterShapeOptions__clone(&register_.v.register_shape.opts, &scope.options);
        jazz_protocol_SyncMessage subscribe;
        memset(&subscribe, 0, sizeof(subscribe));
        subscribe.tag = JAZZ_SM_SUBSCRIBE;
        subscribe.v.subscribe.shape_id = subscription.shape_id;
        subscribe.v.subscribe.subscription = subscription;
        memcpy(&subscribe.v.subscribe.values, &values, sizeof(values));
        subscribe.v.subscribe.known_state = NULL;
        subscribe.v.subscribe.has_delegated_session = false;
        jz_vec_push(&receivers.subscriptions, &subscription, sizeof(subscription));
        PEER_FN(declare_known_state)(receivers.peer, subscription, NULL);
        /* Authority scope support has no wire Subscribe admission: bind
         * that exact admission snapshot before the owner-loop rehydrate opens
         * a maintained view. */
        {
            jz_btree claims;
            claims_clone(&claims, &session_claims);
            PEER_FN(set_subscription_policy_binding)(receivers.peer, subscription, identity, claims);
        }
        jazz_protocol_SyncMessage update;
        {
            jz_btree claims;
            claims_clone(&claims, &session_claims);
            jazz_node_NodeState *scoped = NS_FN(scoped_active_session_claims)(receivers.node, identity, claims);
            jz_btree claims2;
            claims_clone(&claims2, &session_claims);
            jazz_protocol_RegisterShapeOptions opts;
            jazz_protocol__protocol__RegisterShapeOptions__clone(&opts, &scope.options);
            jz_err e = PEER_FN(rehydrate_authorization_support_query_for_identity)(receivers.peer, scoped, identity, claims2, subscription, shape, binding, opts, &update);
            NS_FN(end_scoped_claims)(receivers.node);
            if (e.code) {
                err = node_err(e);
                jazz_protocol__protocol__SyncMessage__drop(&register_);
                jazz_protocol__protocol__SyncMessage__drop(&subscribe);
                break;
            }
        }
        if (update.tag != JAZZ_SM_VIEW_UPDATE) {
            err = protocol_err("scope hydration was not a view update");
            jazz_protocol__protocol__SyncMessage__drop(&update);
            jazz_protocol__protocol__SyncMessage__drop(&register_);
            jazz_protocol__protocol__SyncMessage__drop(&subscribe);
            break;
        }
        jazz_types_GlobalTime cut = update.v.view_update->settled_through;
        u64 progress = PEER_FN(authorization_progress_for_subscription)(receivers.peer, subscription);
        if (!jazz_protocol__authorization_scope__AuthorityScopeAggregate__apply(aggregate, subscription, cut, progress) && index + 1 == support_clauses.len) {
            jazz_protocol__protocol__SyncMessage__drop(&update);
            jazz_protocol__protocol__SyncMessage__drop(&register_);
            jazz_protocol__protocol__SyncMessage__drop(&subscribe);
            unavailable = true;
            break;
        }
        jazz_db_ServedAuthorizationScopeClause served = { subscription, register_, subscribe, update };
        jz_vec_push(&served_clauses, &served, sizeof(served));
    }
    if (!err.code && !unavailable) {
        jazz_types_GlobalTime settled_through;
        u64 authorization_progress;
        if (!jazz_protocol__authorization_scope__AuthorityScopeAggregate__bounds(aggregate, &settled_through, &authorization_progress)) {
            unavailable = true;
        } else {
            jazz_protocol_AuthorizationScopeReceiptFields fields;
            fields.key = jazz_protocol__protocol__AuthorizationSupportScopeKey__clone(scope.key);
            jazz_types_NodeUuid node_uuid = NS_FN(node_uuid)(receivers.node);
            memcpy(fields.authority, &node_uuid, 16);
            fields.link = identity;
            fields.authority_epoch = connection_epoch;
            fields.claims_revision = current_claims_revision;
            fields.policy_epoch = current_policy_epoch;
            fields.settled_through = settled_through;
            fields.authorization_progress = authorization_progress;
            jazz_db_ServedAuthorizationScopeHydration hydration;
            memcpy(&hydration.clauses, &served_clauses, sizeof(hydration.clauses));
            memset(&served_clauses, 0, sizeof(served_clauses));
            hydration.receipt = jazz_protocol__protocol__AuthorizationScopeReceipt__new(&fields);
            if (hydrations->len < MAX_AUTHORIZATION_SCOPES) {
                jazz_db_ServedAuthorizationScopeHydration cached;
                jazz_db__db__ServedAuthorizationScopeHydration__clone(&cached, &hydration);
                jazz_protocol_AuthorizationSupportScopeKey *k = jazz_protocol__protocol__AuthorizationSupportScopeKey__clone(scope.key);
                jazz_db_ServedAuthorizationScopeHydration old;
                if (jz_btree_insert(hydrations, &k, &cached, &old)) { jazz_db__db__ServedAuthorizationScopeHydration__drop(&old); jazz_protocol__protocol__AuthorizationSupportScopeKey__drop(k); }
            }
            /* Scope views are proof material, not application subscriptions.
             * Their lifetime ends after the receipt. */
            AuthorizationScopeReceivers_drop(&receivers);
            jazz_db_node_unlock(node);
            queue_authorization_scope_sequence(pending_control_responses, request_id, scope.key, hydration);
            scope.key = NULL;
            goto out_aggregate;
        }
    }
    AuthorizationScopeReceivers_drop(&receivers);
    jazz_db_node_unlock(node);
    if (!err.code && unavailable) queue_scope_unavailable(pending_control_responses, request_id);
out_aggregate:
    for (u32 i = 0; i < served_clauses.len; ++i) {
        jazz_protocol__protocol__SyncMessage__drop(&served_clauses.ptr[i].register_);
        jazz_protocol__protocol__SyncMessage__drop(&served_clauses.ptr[i].subscribe);
        jazz_protocol__protocol__SyncMessage__drop(&served_clauses.ptr[i].view);
    }
    jz_vec_drop(&served_clauses, sizeof(jazz_db_ServedAuthorizationScopeClause), NULL);
    jazz_protocol__authorization_scope__AuthorityScopeAggregate__drop(aggregate);
out_scope:
    jz_vec_drop(&support_clauses, sizeof(u32), NULL);
    jazz_node__node__AuthorizationSupportScope__drop(&scope);
out_action:
    if (action) jazz_protocol__protocol__PermissionAdviceAction__drop(action);
    jz_btree_drop(&session_claims);
    return err;
}

/// Check a transported receipt against the authority identity authenticated
/// by this connection and the support view already applied locally.
bool jazz_db__db__peer_connection__authorization_scope_receipt_matches_transport_context(const jazz_protocol_AuthorizationScopeReceipt *receipt_p, jazz_protocol_AuthorityContext expected,
                                                                                         jazz_types_AuthorSubject expected_subject, bool has_applied_cut, jazz_types_GlobalTime applied_cut) {
    jazz_protocol_AuthorizationScopeReceiptFields receipt;
    jazz_protocol__protocol__AuthorizationScopeReceipt__fields(receipt_p, &receipt);
    return jazz_types_AuthorSubject_eq(receipt.link, expected_subject)
        && jazz_types_AuthorSubject_eq(jazz_protocol__protocol__AuthorizationSupportScopeKey__subject(receipt.key), expected_subject)
        && memcmp(receipt.authority, expected.authority, 16) == 0
        && receipt.authority_epoch == expected.connection_epoch
        && receipt.claims_revision == expected.claims_revision
        && receipt.policy_epoch == expected.policy_epoch
        && receipt.authorization_progress >= expected.authorization_progress
        && receipt.settled_through >= expected.settled_through
        && has_applied_cut && applied_cut >= receipt.settled_through;
}

static bool keep_not_recipient(void *item, void *ctx) {
    return !jazz_protocol_SubscriptionKey_eq(&((jazz_db_KeyedSyncMessage *)item)->subscription, (const jazz_protocol_SubscriptionKey *)ctx);
}

/// Undo a served usage-site admission that failed before its opening reset
/// was accepted. A coverage group owns shared canonical state, so preserve
/// it for siblings while removing every per-usage registration. If this was
/// the last usage, retire the group and cancel (or withdraw) its upstream
/// ownership too.
static void rollback_rejected_subscriber_admission(jazz_db_NodeMutex *node, jazz_node_PeerState *peer, jz_btree *served, jz_btree *coverage_groups,
                                                   jz_btree *relay_upstream_subscription_owners, jazz_db_PendingUpstreamCommands *upstream_subscriptions,
                                                   u64 connection_epoch, jazz_protocol_SubscriptionKey subscription) {
    jazz_db_CoverageKey coverage;
    if (!jz_btree_remove(served, &subscription, NULL, &coverage)) return;
    jazz_db_CoverageGroup *group = (jazz_db_CoverageGroup *)jz_btree_get(coverage_groups, &coverage);
    if (!group) {
        /* Admission always installs the group before `served`; avoid
         * retaining the usage-site state if an earlier invariant violation
         * broke that ordering. */
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
        NS_FN(apply_unsubscribe)(n, subscription);
        PEER_FN(forget_subscription)(peer, subscription);
        jazz_db_node_unlock(node);
        jazz_db__db__CoverageKey__drop(&coverage);
        return;
    }
    jz_btree_remove(&group->subscribers, &subscription, NULL, NULL);
    jz_btree_remove(&group->pending_initial_subscribers, &subscription, NULL, NULL);
    {
        /* pending_incremental_updates.retain(|(recipient, _)| *recipient != subscription) */
        jz_deque kept;
        jz_deque_init(&kept, sizeof(jazz_db_KeyedSyncMessage));
        jazz_db_KeyedSyncMessage item;
        while (jz_deque_pop_front(&group->pending_incremental_updates, &item)) {
            if (keep_not_recipient(&item, &subscription)) jz_deque_push_back(&kept, &item);
            else jazz_db__db__KeyedSyncMessage__drop(&item);
        }
        jz_deque_drop(&group->pending_incremental_updates, NULL);
        group->pending_incremental_updates = kept;
    }
    if (group->has_pending_initial_update && jazz_protocol_SubscriptionKey_eq(&group->pending_initial_update.subscription, &subscription)) {
        jazz_protocol__protocol__SyncMessage__drop(&group->pending_initial_update.message);
        group->has_pending_initial_update = false;
    }
    {
        jazz_db_RelayOwnerKey key;
        memset(&key, 0, sizeof(key));
        key.subscription = group->upstream_subscription;
        key.downstream_connection_epoch = connection_epoch;
        key.read_view = jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&coverage.opts);
        jazz_db_RelayUpstreamSubscriptionOwner *owner = (jazz_db_RelayUpstreamSubscriptionOwner *)jz_btree_get(relay_upstream_subscription_owners, &key);
        if (owner && owner->downstream_connection_epoch == connection_epoch && jazz_db__db__CoverageKey__eq(&owner->coverage, &coverage))
            jz_btree_remove(&owner->downstream_subscriptions, &subscription, NULL, NULL);
    }
    bool retire_group = group->subscribers.len == 0;
    jazz_protocol_SubscriptionKey upstream_subscription = group->upstream_subscription;
    jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
    NS_FN(apply_unsubscribe)(n, subscription);
    PEER_FN(forget_subscription)(peer, subscription);
    if (!retire_group) { jazz_db_node_unlock(node); jazz_db__db__CoverageKey__drop(&coverage); return; }
    PEER_FN(forget_subscription_with_node)(peer, n, jazz_db__db__peer_connection__coverage_group_subscription_key(&coverage));
    jazz_db_node_unlock(node);
    jz_btree_remove(coverage_groups, &coverage, NULL, NULL);
    jazz_db_RelayUpstreamSubscriptionOwner retired;
    if (!jazz_db__db__node_runtime__retire_relay_upstream_subscription(relay_upstream_subscription_owners, upstream_subscription, connection_epoch, &coverage, &retired)) {
        jazz_db__db__CoverageKey__drop(&coverage);
        return;
    }
    jazz_db__db__RelayUpstreamSubscriptionOwner__drop(&retired);
    jazz_db_PendingUpstreamCommands *pending = upstream_subscriptions;
    bool open_was_pending = false;
    for (u32 c = 0; c < pending->len; ++c)
        if (pending_subscribe_names(&pending->ptr[c], &upstream_subscription)) { open_was_pending = true; break; }
    jz_vec_retain(pending, sizeof(jazz_db_PendingUpstreamCommand), keep_not_pending_subscribe, &upstream_subscription, jazz_db__db__PendingUpstreamCommand__drop);
    if (!open_was_pending) {
        jazz_db_PendingUpstreamCommand *command = (jazz_db_PendingUpstreamCommand *)jz_vec_push_slot(pending, sizeof(jazz_db_PendingUpstreamCommand));
        memset(command, 0, sizeof(*command));
        command->tag = JAZZ_PUC_UNSUBSCRIBE;
        command->unsubscribe = upstream_subscription;
    }
    jazz_db__db__CoverageKey__drop(&coverage);
}

static bool predicate_references_id(const jazz_model_Predicate *predicate);

static bool operand_is_id(const jazz_model_Operand *operand) {
    return operand->tag == JAZZ_OPERAND_COLUMN && jz_str_eq_cstr(&operand->column, "id");
}

static bool root_join_references_id(const jazz_model_JoinVia *join) {
    if (!join->has_source_column || jz_str_eq_cstr(&join->source_column, "id")) return true;
    if (join->has_source_lookup && jz_str_eq_cstr(&join->source_lookup_row_id_source_column, "id")) return true;
    for (u32 i = 0; i < join->correlated_filters.len; ++i)
        if (jz_str_eq_cstr(&join->correlated_filters.ptr[i].source_column, "id")) return true;
    return false;
}

static bool predicate_references_id(const jazz_model_Predicate *predicate) {
    switch (predicate->tag) {
    case JAZZ_PRED_ALL: case JAZZ_PRED_ANY:
        for (u32 i = 0; i < predicate->items.len; ++i) if (predicate_references_id(&predicate->items.ptr[i])) return true;
        return false;
    case JAZZ_PRED_NOT: return predicate_references_id(predicate->item);
    case JAZZ_PRED_EQ: case JAZZ_PRED_NE: case JAZZ_PRED_GT: case JAZZ_PRED_GTE:
    case JAZZ_PRED_LT: case JAZZ_PRED_LTE: case JAZZ_PRED_CONTAINS:
        return operand_is_id(&predicate->left) || operand_is_id(&predicate->right);
    case JAZZ_PRED_IN:
        if (operand_is_id(&predicate->left)) return true;
        for (u32 i = 0; i < predicate->values.len; ++i) if (operand_is_id(&predicate->values.ptr[i])) return true;
        return false;
    case JAZZ_PRED_IS_NULL: return operand_is_id(&predicate->left);
    default: /* EnumMatch */
        return jz_str_eq_cstr(&predicate->column, "id") || predicate_references_id(predicate->item);
    }
}

static bool query_root_filters_reference_id(const jazz_model_Query *query_p) {
    const jazz_model_QueryPolicyParts *query = jazz_model__query__Query__policy_parts(query_p);
    for (u32 i = 0; i < query->filters.len; ++i) if (predicate_references_id(&query->filters.ptr[i])) return true;
    if (query->reachable_len != 0) return true;
    for (u32 i = 0; i < query->joins.len; ++i) if (root_join_references_id(&query->joins.ptr[i])) return true;
    for (u32 b = 0; b < query->policy_branches.len; ++b) {
        const jazz_model_QueryPolicyBranch *branch = &query->policy_branches.ptr[b];
        for (u32 i = 0; i < branch->filters.len; ++i) if (predicate_references_id(&branch->filters.ptr[i])) return true;
        if (branch->reachable_len != 0) return true;
        for (u32 i = 0; i < branch->joins.len; ++i) if (root_join_references_id(&branch->joins.ptr[i])) return true;
    }
    return false;
}

/* ---------------------------------------------------------------- send helpers */

static void drop_peer_request(jazz_db_NodeMutex *node) {
    jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
    NS_FN(record_dropped_peer_request)(n);
    jazz_db_node_unlock(node);
}

static bool handle_transport_backpressure(jazz_db_NodeMutex *node, jazz_db_SharedTickScheduler *scheduler, const jazz_db_TransportError *error) {
    if (error->tag != JAZZ_TRANSPORT_BACKPRESSURE) return false;
    jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
    NS_FN(record_transport_backpressure_retry)(n);
    jazz_db_node_unlock(node);
    jazz_db__db__peer_connection__schedule_tick_in(scheduler, JAZZ_TICK_DEFERRED);
    return true;
}

static bool handle_db_backpressure(jazz_db_NodeMutex *node, jazz_db_SharedTickScheduler *scheduler, const jz_err *error) {
    if (error->code != JAZZ_EC_BACKPRESSURE) return false;
    jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
    NS_FN(record_transport_backpressure_retry)(n);
    jazz_db_node_unlock(node);
    jazz_db__db__peer_connection__schedule_tick_in(scheduler, JAZZ_TICK_DEFERRED);
    return true;
}

static jz_err transport_send(jazz_db_Transport *transport, jazz_protocol_SyncMessage message) {
    jazz_db_TransportError error;
    jz_err e = jazz_db__db__node_runtime__Transport__send(transport, message, &error);
    if (e.code == 0) return JZ_OK;
    jz_err_drop(&e);
    return jazz_db__db__peer_connection__transport_error(error);
}

static jz_err send_catalogue_snapshot_if_needed(jazz_db_NodeMutex *node, jazz_node_PeerState *peer, jazz_db_Transport *transport);
static jz_err prepare_for_send(jazz_db_NodeMutex *node, const jazz_node_PeerState *peer, jazz_protocol_SyncMessage *message);

static jz_err send_sync_message_chunked(jazz_db_Transport *transport, jazz_protocol_SyncMessage message) {
    return transport_send(transport, message);
}

static jz_err send_with_sync_context(jazz_db_NodeMutex *node, jazz_node_PeerState *peer, jazz_db_Transport *transport, jazz_protocol_SyncMessage message) {
    jz_err e = send_catalogue_snapshot_if_needed(node, peer, transport);
    if (e.code) { jazz_protocol__protocol__SyncMessage__drop(&message); return e; }
    e = prepare_for_send(node, peer, &message);
    if (e.code) { jazz_protocol__protocol__SyncMessage__drop(&message); return e; }
    return send_sync_message_chunked(transport, message);
}

/// [`send_with_sync_context`] that hands a backpressured message back to its
/// caller instead of dropping it.
static jz_err try_send_with_sync_context(jazz_db_NodeMutex *node, jazz_node_PeerState *peer, jazz_db_Transport *transport, jazz_protocol_SyncMessage message,
                                         bool *returned, jazz_protocol_SyncMessage *back) {
    *returned = false;
    jz_err e = send_catalogue_snapshot_if_needed(node, peer, transport);
    if (e.code == JAZZ_EC_BACKPRESSURE) { jz_err_drop(&e); *returned = true; *back = message; return JZ_OK; }
    if (e.code) { jazz_protocol__protocol__SyncMessage__drop(&message); return e; }
    e = prepare_for_send(node, peer, &message);
    if (e.code) { jazz_protocol__protocol__SyncMessage__drop(&message); return e; }
    jazz_db_TransportError error;
    e = jazz_db__db__node_runtime__Transport__try_send(transport, message, returned, back, &error);
    if (e.code) { jz_err_drop(&e); return jazz_db__db__peer_connection__transport_error(error); }
    return JZ_OK;
}

/// Stamp and bound an outbound message. Idempotent, so a message handed back
/// by backpressure is prepared again unchanged on retry.
static jz_err prepare_for_send(jazz_db_NodeMutex *node, const jazz_node_PeerState *peer, jazz_protocol_SyncMessage *message) {
    if (message->tag == JAZZ_SM_VIEW_UPDATE) {
        jazz_protocol_ViewUpdatePayload *payload = message->v.view_update;
        if (!payload->peer_payload_inventory.has_authorization_progress) {
            payload->peer_payload_inventory.has_authorization_progress = true;
            payload->peer_payload_inventory.authorization_progress = PEER_FN(authorization_progress_for_subscription)(peer, payload->subscription);
        }
    }
    if (jazz_types__debug_env__covered_input_trace() && message->tag == JAZZ_SM_VIEW_UPDATE) {
        const jazz_protocol_ViewUpdatePayload *payload = message->v.view_update;
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
        bool relay = NS_FN(client_relay_scope)(n) != NULL;
        jazz_db_node_unlock(node);
        jz_str sub = {0};
        jazz_protocol__protocol__SubscriptionKey__debug(&sub, &payload->subscription);
        jz_trace_event(0, "JAZZ_COVERED_INPUT_TRACE", "stage=transport_send relay=%d subscription=%.*s pending=%d rows=%u carriers=%u",
                       relay, (int)sub.len, sub.ptr, payload->peer_payload_inventory.opening_pending,
                       jazz_protocol__protocol__SupportingRowsUpdate__added_rows_len(payload->supporting_rows), payload->version_carriers.len);
        jz_str_drop(&sub);
    }
    return JZ_OK;
}

static void stamp_subscriber_opening_state(jazz_db_NodeMutex *node, const jazz_node_PeerState *peer, jazz_protocol_SyncMessage *message);
static jz_err send_prepared_subscriber_with_sync_context(jazz_db_NodeMutex *node, jazz_node_PeerState *peer, jazz_db_Transport *transport, jz_btree *local_fate_routes,
                                                        jazz_db_SyncMessageVec *downstream_fates, jazz_protocol_SyncMessage message);

static jz_err send_subscriber_with_sync_context_full(jazz_db_NodeMutex *node, jazz_node_PeerState *peer, jazz_db_Transport *transport, jz_btree *local_fate_routes,
                                                    jazz_db_SyncMessageVec *downstream_fates, jazz_protocol_SyncMessage message) {
    stamp_subscriber_opening_state(node, peer, &message);
    return send_prepared_subscriber_with_sync_context(node, peer, transport, local_fate_routes, downstream_fates, message);
}

jz_err jazz_db__db__peer_connection__send_subscriber_with_sync_context(jazz_db_NodeMutex *node, jazz_node_PeerState *peer, jazz_db_Transport *transport, jz_btree *local_fate_routes,
                                                                       jazz_db_SyncMessageVec *downstream_fates, jazz_protocol_SyncMessage message) {
    return send_subscriber_with_sync_context_full(node, peer, transport, local_fate_routes, downstream_fates, message);
}

static void stamp_subscriber_opening_state(jazz_db_NodeMutex *node, const jazz_node_PeerState *peer, jazz_protocol_SyncMessage *message) {
    /* A selected upstream source does not itself make this a strict read.
     * Local-first still publishes cached inputs while that source is pending. */
    if (message->tag != JAZZ_SM_VIEW_UPDATE) return;
    jazz_protocol_ViewUpdatePayload *payload = message->v.view_update;
    if (!PEER_FN(subscription_awaits_selected_authority_source)(peer, payload->subscription)) return;
    const jazz_protocol_AuthorityResultKey *source = PEER_FN(subscription_authority_result_source)(peer, payload->subscription);
    bool source_settled = false;
    if (source) {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
        source_settled = NS_FN(has_settled_authority_result)(n, source);
        jazz_db_node_unlock(node);
    }
    if (source && !source_settled) {
        /* This D belongs to a non-authoritative scope relay whose selected U
         * source has not delivered a live authority reset yet. */
        payload->peer_payload_inventory.opening_pending = true;
    }
}

static jz_err pending_view_update_tx_ids(const jazz_protocol_SyncMessage *message, jz_btree *out);

/// A retained publication already owns its opening/authorization envelope.
/// Retry must not restamp it from a newer authority state before acceptance.
static jz_err send_prepared_subscriber_with_sync_context(jazz_db_NodeMutex *node, jazz_node_PeerState *peer, jazz_db_Transport *transport, jz_btree *local_fate_routes,
                                                        jazz_db_SyncMessageVec *downstream_fates, jazz_protocol_SyncMessage message) {
    jz_btree pending_tx_ids;
    jz_err e = pending_view_update_tx_ids(&message, &pending_tx_ids);
    if (e.code) { jazz_protocol__protocol__SyncMessage__drop(&message); return e; }
    e = send_with_sync_context(node, peer, transport, message);
    if (e.code == 0) {
        jazz_model_TxId *tx;
        void *unit;
        JZ_BTREE_FOR(&pending_tx_ids, tx, unit) { (void)unit; jazz_db__db__register_local_fate_observer(local_fate_routes, *tx, downstream_fates); }
    }
    jz_btree_drop(&pending_tx_ids);
    return e;
}

static jz_err try_send_prepared_subscriber_with_sync_context(jazz_db_NodeMutex *node, jazz_node_PeerState *peer, jazz_db_Transport *transport, jz_btree *local_fate_routes,
                                                            jazz_db_SyncMessageVec *downstream_fates, jazz_protocol_SyncMessage message, bool *rejected, jazz_protocol_SyncMessage *back) {
    jz_btree pending_tx_ids;
    jz_err e = pending_view_update_tx_ids(&message, &pending_tx_ids);
    if (e.code) { jazz_protocol__protocol__SyncMessage__drop(&message); return e; }
    e = try_send_with_sync_context(node, peer, transport, message, rejected, back);
    if (e.code == 0 && !*rejected) {
        jazz_model_TxId *tx;
        void *unit;
        JZ_BTREE_FOR(&pending_tx_ids, tx, unit) { (void)unit; jazz_db__db__register_local_fate_observer(local_fate_routes, *tx, downstream_fates); }
    }
    jz_btree_drop(&pending_tx_ids);
    return e;
}

/// Pending transactions a view update ships, whose fates its receiver waits on.
static jz_err pending_view_update_tx_ids(const jazz_protocol_SyncMessage *message, jz_btree *out) {
    jz_btree_init(out, &jazz_db_txid_set_desc);
    if (message->tag != JAZZ_SM_VIEW_UPDATE) return JZ_OK;
    const jazz_protocol_ViewUpdatePayload *payload = message->v.view_update;
    for (u32 c = 0; c < payload->version_carriers.len; ++c) {
        jazz_protocol_BundleRefVec bundles;
        jz_err e = jazz_protocol__protocol__VersionCarrier__bundle_refs(payload->version_carriers.ptr[c], &bundles);
        if (e.code) { jz_err_drop(&e); jz_btree_drop(out); return protocol_err("malformed version-bundle run"); }
        for (u32 b = 0; b < bundles.len; ++b)
            if (bundles.ptr[b].fate_tag == JAZZ_FATE_PENDING) jz_btree_insert(out, &bundles.ptr[b].tx_id, NULL, NULL);
        jz_vec_drop(&bundles, sizeof(jazz_protocol_BundleRef), NULL);
    }
    return JZ_OK;
}

/// Whether an authority answer about this link's uploads (a fate) may change
/// its outbox: it arrived as an authority receipt and, when the link expects
/// a particular authority, from the one admitted on it now.
static bool upload_answer_receipt_eligible(bool authority_receipt_eligible, bool has_expected, jazz_protocol_AuthorityContext expected,
                                           bool has_admitted, jazz_protocol_AuthorityContext admitted) {
    return authority_receipt_eligible
        && (!has_expected || (has_admitted && jazz_protocol__protocol__AuthorityContext__same_admitted_link(&admitted, &expected)));
}

/// Whether `message` is a terminal fate for `tx_id` that no upstream can
/// revise: a rejection, or a Global-durable acceptance carrying its global
/// time. Only such a fate retires an upload-outbox entry.
static bool fate_update_settles_upload(const jazz_protocol_SyncMessage *message, jazz_model_TxId tx_id) {
    if (message->tag != JAZZ_SM_FATE_UPDATE) return false;
    if (!jazz_model_TxId_eq(message->v.fate_update.tx_id, tx_id)) return false;
    if (message->v.fate_update.fate.tag == JAZZ_FATE_REJECTED) return true;
    return message->v.fate_update.fate.tag == JAZZ_FATE_ACCEPTED && message->v.fate_update.has_global_time
        && message->v.fate_update.has_durability && message->v.fate_update.durability >= JAZZ_DUR_GLOBAL;
}

/// Deliver terminal/local fate updates in FIFO order without letting a
/// bounded byte transport turn an already-produced settlement into a dropped
/// message. We remove only after `send` accepts the logical message, so a
/// retry neither duplicates a sent fate nor loses a later fate behind it.
static jz_err flush_downstream_fates(jazz_db_NodeMutex *node, jazz_node_PeerState *peer, jazz_db_Transport *transport, jazz_db_SyncMessageVec *fates,
                                     jazz_db_ReadBarrierReceiver *read_barrier, jazz_db_SharedTickScheduler *scheduler, bool *out) {
    /* Send from a detached batch so draining K queued fates is O(K). */
    jazz_db_SyncMessageVec pending = *fates;
    memset(fates, 0, sizeof(*fates));
    u32 sent = 0;
    jz_err result = JZ_OK;
    bool ok = true;
    for (;;) {
        if (sent >= pending.len) { ok = true; break; }
        jazz_protocol_SyncMessage fate;
        jazz_protocol__protocol__SyncMessage__clone(&fate, &pending.ptr[sent]);
        jz_err e = send_with_sync_context(node, peer, transport, fate);
        if (e.code == 0) {
            /* Requests a now-answered write released are served on the next
             * turn of this link. */
            if (jazz_db__db__read_barrier__ReadBarrierReceiver__answer_sent(read_barrier, &pending.ptr[sent]))
                jazz_db__db__peer_connection__schedule_tick_in(scheduler, JAZZ_TICK_IMMEDIATE);
            sent += 1;
        } else if (e.code == JAZZ_EC_BACKPRESSURE) {
            jz_err_drop(&e);
            jazz_db__db__peer_connection__schedule_tick_in(scheduler, JAZZ_TICK_DEFERRED);
            ok = false;
            break;
        } else {
            result = e;
            break;
        }
    }
    /* pending.drain(..sent); pending.append(queue); *queue = pending */
    for (u32 i = 0; i < sent; ++i) jazz_protocol__protocol__SyncMessage__drop(&pending.ptr[i]);
    jazz_db_SyncMessageVec rest = {0};
    if (pending.len > sent) jz_vec_extend(&rest, pending.ptr + sent, pending.len - sent, sizeof(jazz_protocol_SyncMessage));
    jz_vec_drop(&pending, sizeof(jazz_protocol_SyncMessage), NULL);
    if (fates->len) jz_vec_extend(&rest, fates->ptr, fates->len, sizeof(jazz_protocol_SyncMessage));
    jz_vec_drop(fates, sizeof(jazz_protocol_SyncMessage), NULL);
    *fates = rest;
    if (result.code) return result;
    *out = ok;
    return JZ_OK;
}

/// Retry the one ordinary-wire chunk response whose byte admission was
/// refused.
static jz_err flush_pending_chunk_response(jazz_db_Transport *transport, jazz_db_ChunkResponseBatch **pending, jazz_db_SharedTickScheduler *scheduler, bool *out) {
    jazz_db_ChunkResponseBatch *response = *pending;
    *pending = NULL;
    if (!response) { *out = true; return JZ_OK; }
    jazz_protocol_SyncMessage message;
    jazz_protocol__protocol__ChunkResponseBatch__into_message(jazz_protocol__protocol__ChunkResponseBatch__clone(response), &message);
    jazz_db_TransportError error;
    jz_err e = jazz_db__db__node_runtime__Transport__send(transport, message, &error);
    if (e.code == 0) { jazz_protocol__protocol__ChunkResponseBatch__drop(response); *out = true; return JZ_OK; }
    jz_err_drop(&e);
    *pending = response;
    if (error.tag == JAZZ_TRANSPORT_BACKPRESSURE) {
        jazz_protocol__wire__TransportError__drop(&error);
        jazz_db__db__peer_connection__schedule_tick_in(scheduler, JAZZ_TICK_DEFERRED);
        *out = false;
        return JZ_OK;
    }
    return jazz_db__db__peer_connection__transport_error(error);
}

/// Retry a subscriber-control reply such as `SubscribeRejected`. One
/// retained message bounds a stalled link; the subscriber tick stops as soon
/// as it creates one.
static jz_err flush_pending_control_responses(jazz_db_NodeMutex *node, jazz_node_PeerState *peer, jazz_db_Transport *transport, jz_deque *pending,
                                              jazz_db_SharedTickScheduler *scheduler, bool *out) {
    for (;;) {
        if (pending->len == 0) { *out = true; return JZ_OK; }
        jazz_db_PendingSubscriberControlResponse *response = (jazz_db_PendingSubscriberControlResponse *)jz_deque_at(pending, 0);
        jz_err send_result;
        switch (response->tag) {
        case JAZZ_PSCR_DIRECT: {
            jazz_protocol_SyncMessage m;
            jazz_protocol__protocol__SyncMessage__clone(&m, &response->message);
            send_result = transport_send(transport, m);
            break;
        }
        case JAZZ_PSCR_WITH_SYNC_CONTEXT: {
            jazz_protocol_SyncMessage m;
            jazz_protocol__protocol__SyncMessage__clone(&m, &response->message);
            send_result = send_with_sync_context(node, peer, transport, m);
            break;
        }
        default: {
            jazz_protocol_SyncMessage m;
            if (!PendingAuthorizationScopeSequence_next_message(&response->sequence, &m)) {
                jazz_db_PendingSubscriberControlResponse done;
                jz_deque_pop_front(pending, &done);
                jazz_db__db__peer_connection__PendingSubscriberControlResponse__drop(&done);
                continue;
            }
            /* Scope hydration carries native physical row identities just
             * like ordinary subscriptions. Install the authority catalogue
             * before delivering those rows to a newly connected client. */
            send_result = send_with_sync_context(node, peer, transport, m);
            break;
        }
        }
        if (send_result.code == 0) {
            response = (jazz_db_PendingSubscriberControlResponse *)jz_deque_at(pending, 0);
            if (!response) jz_panic("accepted control operation remains queued");
            bool finished;
            if (response->tag == JAZZ_PSCR_AUTHORIZATION_SCOPE_SEQUENCE) {
                PendingAuthorizationScopeSequence_advance(&response->sequence);
                jazz_protocol_SyncMessage probe;
                finished = !PendingAuthorizationScopeSequence_next_message(&response->sequence, &probe);
                if (!finished) jazz_protocol__protocol__SyncMessage__drop(&probe);
            } else {
                finished = true;
            }
            if (finished) {
                jazz_db_PendingSubscriberControlResponse done;
                jz_deque_pop_front(pending, &done);
                jazz_db__db__peer_connection__PendingSubscriberControlResponse__drop(&done);
            }
        } else if (send_result.code == JAZZ_EC_BACKPRESSURE) {
            jz_err_drop(&send_result);
            jazz_db__db__peer_connection__schedule_tick_in(scheduler, JAZZ_TICK_DEFERRED);
            *out = false;
            return JZ_OK;
        } else {
            return send_result;
        }
    }
}

/// Send an authority catalogue snapshot exactly once per peer fingerprint.
/// Trusted relay links have no application subscription during bootstrap,
/// so catalogue propagation must not depend on a later ViewUpdate or fate.
static jz_err send_catalogue_snapshot_if_needed(jazz_db_NodeMutex *node, jazz_node_PeerState *peer, jazz_db_Transport *transport) {
    jazz_protocol_CatalogueSnapshot *snapshot = NULL;
    {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
        jz_err e = NS_FN(catalogue_snapshot)(n, &snapshot);
        jazz_db_node_unlock(node);
        if (e.code) return node_err(e);
    }
    jz_str json = {0};
    jazz_protocol__protocol__CatalogueSnapshot__to_json_vec(snapshot, &json);
    u8 catalogue_fingerprint[32];
    jz_blake3((const u8 *)json.ptr, json.len, catalogue_fingerprint);
    jz_str_drop(&json);
    if (PEER_FN(needs_catalogue_snapshot)(peer, catalogue_fingerprint)) {
        jazz_protocol_SyncMessage m;
        memset(&m, 0, sizeof(m));
        m.tag = JAZZ_SM_CATALOGUE_SNAPSHOT;
        m.v.catalogue_snapshot = snapshot;
        JZ_TRY(transport_send(transport, m));
        PEER_FN(mark_catalogue_snapshot_announced)(peer, catalogue_fingerprint);
        return JZ_OK;
    }
    jazz_protocol__protocol__CatalogueSnapshot__drop(snapshot);
    return JZ_OK;
}

static jz_err send_with_local_sync_context(jazz_db_NodeMutex *node, jazz_db_Transport *transport, jazz_protocol_SyncMessage message) {
    (void)node;
    return transport_send(transport, message);
}

static void stamp_view_update_authorization_progress_from(jazz_node_PeerState *peer, jazz_protocol_SubscriptionKey source_subscription, jazz_protocol_SyncMessage *message) {
    if (message->tag != JAZZ_SM_VIEW_UPDATE) return;
    jazz_protocol_ViewUpdatePayload *payload = message->v.view_update;
    u64 source_progress = PEER_FN(authorization_progress_for_subscription)(peer, source_subscription);
    if (!jazz_protocol_SubscriptionKey_eq(&source_subscription, &payload->subscription)
        && source_progress == PEER_FN(authorization_progress_for_subscription)(peer, payload->subscription))
        return;
    payload->peer_payload_inventory.has_authorization_progress = true;
    payload->peer_payload_inventory.authorization_progress = source_progress;
}

/// Hand one generated update to each sibling subscriber: every sibling but
/// the last gets a clone, and the last takes the original.
static jazz_protocol_SyncMessage take_last_or_clone(bool *has_slot, jazz_protocol_SyncMessage *slot, bool more_follow) {
    jazz_protocol_SyncMessage out;
    if (more_follow) {
        if (!*has_slot) jz_panic("update remains for later siblings");
        jazz_protocol__protocol__SyncMessage__clone(&out, slot);
        return out;
    }
    if (!*has_slot) jz_panic("update remains for the last sibling");
    *has_slot = false;
    out = *slot;
    return out;
}

static jazz_protocol_SyncMessage retarget_view_update(jazz_protocol_SyncMessage message, jazz_protocol_SubscriptionKey target) {
    if (message.tag == JAZZ_SM_VIEW_UPDATE) message.v.view_update->subscription = target;
    return message;
}

static bool write_state_update_tx_id(const jazz_protocol_SyncMessage *message, jazz_model_TxId *out) {
    if (message->tag != JAZZ_SM_FATE_UPDATE) return false;
    *out = message->v.fate_update.tx_id;
    return true;
}

static void handle_write_state_update(jazz_db_NodeMutex *node, jz_btree *waiters, jazz_db_MutationErrorState *mutation_errors,
                                      jz_btree *browser_relay_recovered_tx_ids, jazz_db_SharedTickScheduler *scheduler, jazz_model_TxId tx_id) {
    jazz_db__db__node_runtime__notify_write_state_waiters(waiters, tx_id);
    /* Extract the owned rejection before deciding how to report it. */
    const void *rejected;
    {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
        rejected = NS_FN(rejected_transaction)(n, tx_id);
        jazz_db_node_unlock(node);
    }
    if (rejected) {
        jazz_db__mutation_errors__queue_mutation_error(mutation_errors, scheduler, tx_id, jazz_db__mutation_errors__mutation_error_event(rejected));
        return;
    }
    /* A restarted browser relay re-uploads durable foreground commits whose
     * TxId node belongs to the former, non-durable foreground runtime. An
     * attached foreground runtime still needs one live notification when
     * that exact replayed transaction is rejected. */
    if (!jz_btree_contains(browser_relay_recovered_tx_ids, &tx_id)) return;
    jazz_node_TransactionRecord record;
    bool has_record;
    {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
        has_record = NS_FN(transaction_record)(n, tx_id, &record);
        jazz_db_node_unlock(node);
    }
    if (!has_record) return;
    bool terminal = record.fate.tag == JAZZ_FATE_REJECTED
        || (record.fate.tag == JAZZ_FATE_ACCEPTED && record.has_global_time && record.durability >= JAZZ_DUR_GLOBAL);
    if (!terminal || !jz_btree_remove(browser_relay_recovered_tx_ids, &tx_id, NULL, NULL)) return;
    if (record.fate.tag != JAZZ_FATE_REJECTED) return;
    jazz_db__mutation_errors__queue_mutation_error(mutation_errors, scheduler, tx_id,
        jazz_db__mutation_errors__mutation_error_event_for_kind(tx_id, (jazz_model_TxKind)record.kind, &record.fate.rejected));
}

/// Bindings carry values positionally; the shape orders them by param name.
static groove_ValueVec binding_values_in_param_order(const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding) {
    groove_ValueVec out = {0};
    const jz_btree *params = jazz_model__query__ValidatedQuery__params(shape);
    const jz_btree *values = jazz_model__query__Binding__values(binding);
    jz_str *name;
    void *param;
    JZ_BTREE_FOR(params, name, param) {
        (void)param;
        const groove_Value *value = (const groove_Value *)jz_btree_get(values, name);
        if (!value) jz_panic("binding is missing a shape parameter value");
        groove_Value *slot = (groove_Value *)jz_vec_push_slot(&out, sizeof(groove_Value));
        groove__records__Value__clone(slot, value);
    }
    return out;
}

static jz_str chunk_locator_fingerprint(const u8 locator[32]) {
    u8 hash[32];
    jz_blake3(locator, 32, hash);
    jz_str hex = {0};
    jz_hex_encode(hash, 32, &hex);
    jz_str out = jz_str_from_slice(jz_slice_of(hex.ptr, 16));
    jz_str_drop(&hex);
    return out;
}

/// Describe an unavailable locally-owned upload chunk without disclosing its
/// retrieval capability. A locator grants exact chunk retrieval, so the
/// diagnostic carries a stable fingerprint rather than raw locator bytes.
static jz_str large_value_upload_chunk_context(jazz_model_TxId tx_id, const groove_LargeValueRef *value_ref, const groove_NodeRef *node_ref,
                                               const char *replica_role, jazz_types_NodeUuid source_node) {
    jz_str root_hash = {0}, chunk_hash = {0}, tx = {0}, src = {0};
    jz_hex_encode(value_ref->root.object_hash, 32, &root_hash);
    jz_hex_encode(node_ref->object_hash, 32, &chunk_hash);
    jz_str root_locator = chunk_locator_fingerprint(value_ref->root.locator);
    jz_str chunk_locator = chunk_locator_fingerprint(node_ref->locator);
    jazz_model__tx__TxId__debug(&tx, &tx_id);
    jazz_types__ids__NodeUuid__debug(&src, &source_node);
    jz_str out = jz_format("role=%s source_node=%.*s transaction=%.*s root_hash=%.*s root_locator=%.*s chunk_hash=%.*s chunk_locator=%.*s",
                           replica_role, (int)src.len, src.ptr, (int)tx.len, tx.ptr, (int)root_hash.len, root_hash.ptr,
                           (int)root_locator.len, root_locator.ptr, (int)chunk_hash.len, chunk_hash.ptr, (int)chunk_locator.len, chunk_locator.ptr);
    jz_str_drop(&root_hash); jz_str_drop(&chunk_hash); jz_str_drop(&root_locator); jz_str_drop(&chunk_locator); jz_str_drop(&tx); jz_str_drop(&src);
    return out;
}

/* ================================================================ tick_inner
 *
 * PeerConnection::tick_inner. The Rust body is one async block per link
 * role with nested `Box::pin(async { .. })` stop scopes; each scope becomes
 * a static function returning jz_err with a `bool *stop` out flag
 * (`Ok(true)` = stop this tick). */

typedef struct TickCtx {
    PC *self;
    jazz_db_DbTickStats stats;
    bool has_progress_waker;
    jz_waker progress_waker;
    u64 connection_epoch;
    bool permits_delegated_sessions;
    bool has_session_claim_binding;
    jazz_db_PolicyBinding session_claim_binding;
    JZ_VEC(jazz_db_BufferLease *) received_leases;
} TickCtx;

static const jz_waker *tick_waker(const TickCtx *t) { return t->has_progress_waker ? &t->progress_waker : NULL; }

/// `self.transport.send(m)` + `handle_transport_backpressure`: a
/// backpressured send sets `*backpressured` and returns Ok.
static jz_err tick_send(TickCtx *t, jazz_protocol_SyncMessage message, bool *backpressured) {
    PC *self = t->self;
    *backpressured = false;
    jazz_db_TransportError error;
    jz_err e = jazz_db__db__node_runtime__Transport__send(&self->transport, message, &error);
    if (e.code == 0) return JZ_OK;
    jz_err_drop(&e);
    if (handle_transport_backpressure(self->node, self->scheduler, &error)) {
        jazz_protocol__wire__TransportError__drop(&error);
        *backpressured = true;
        return JZ_OK;
    }
    return jazz_db__db__peer_connection__transport_error(error);
}

/* `if let Err(error) = self.transport.send(m) { if backpressure { return Ok(true) } return Err(..) }` */
#define TICK_SEND_OR_STOP(t, msg) do { \
        bool _bp; jz_err _e = tick_send((t), (msg), &_bp); \
        if (_e.code) return _e; \
        if (_bp) { *stop = true; return JZ_OK; } \
    } while (0)

static int cmp_node_uuid(const void *a, const void *b, void *ctx) { (void)ctx; return memcmp(a, b, 16); }
static const jz_btree_desc node_uuid_set_desc = { 16, 0, cmp_node_uuid, NULL, NULL };

typedef struct ToUpload { jazz_model_TxId tx_id; bool has_unit; jazz_protocol_SyncMessage unit; } ToUpload;
static void to_upload_drop(void *p) { ToUpload *u = p; if (u->has_unit) jazz_protocol__protocol__SyncMessage__drop(&u->unit); }

static void permission_waiter_remove(jz_btree *waiters, const jazz_protocol_PermissionAdviceRequestId *id, bool send_unknown) {
    jazz_db_Oneshot *waiter = NULL;
    if (jz_btree_remove(waiters, id, NULL, &waiter) && waiter) {
        if (send_unknown) { u8 advice = JAZZ_ADVICE_UNKNOWN; (void)jazz_db__oneshot__send(waiter, &advice); }
        jazz_db__oneshot__drop_sender(waiter);
    }
}

static bool session_claims_for_identity(jazz_db_NodeMutex *node, jazz_types_AuthorSubject identity, jz_btree *out) {
    jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(node);
    jazz_node_SessionClaimsRevVec claims;
    NS_FN(session_claims_with_revisions)(n, &claims);
    jazz_db_node_unlock(node);
    bool found = false;
    for (u32 i = 0; i < claims.len; ++i) {
        if (jazz_types_AuthorSubject_eq(claims.ptr[i].identity, identity)) {
            claims_clone(out, &claims.ptr[i].claims);
            found = true;
            break;
        }
    }
    jazz_node__node__SessionClaimsRevVec__drop(&claims);
    return found;
}

/// Upload one locally-authored commit (and its large values) on this
/// link. `*blocked` = this writer's later writes must wait; `*break_uploads`
/// = a full large-value channel holds back every later commit.
static jz_err upstream_upload_one(TickCtx *t, jazz_db_UpstreamConnectionState *up, ToUpload *item, jz_btree *blocked_writers,
                                  bool *break_uploads, bool *stop) {
    PC *self = t->self;
    jazz_model_TxId tx_id = item->tx_id;
    if (jz_btree_contains(blocked_writers, &tx_id.node)) return JZ_OK;
    if (jz_btree_contains(&up->failed_large_value_uploads, &tx_id)
        || jz_btree_contains(&up->awaiting_large_value_uploads, &tx_id)
        || up->awaiting_large_value_uploads.len != 0) {
        jz_btree_insert(blocked_writers, &tx_id.node, NULL, NULL);
        return JZ_OK;
    }
    u64 now_ms = self->upload_retry_clock->now_ms(self->upload_retry_clock->self);
    const u64 *deadline = (const u64 *)jz_btree_get(self->large_value_upload_retry_deadlines, &tx_id);
    if (deadline && now_ms < *deadline) {
        /* A rate-limited large value holds back its writer's later writes
         * until it goes up. */
        jz_btree_insert(blocked_writers, &tx_id.node, NULL, NULL);
        return JZ_OK;
    }
    jz_btree_remove(self->large_value_upload_retry_deadlines, &tx_id, NULL, NULL);
    jazz_protocol_SyncMessage unit;
    if (item->has_unit) {
        unit = item->unit;
        item->has_unit = false;
    } else {
        jazz_node_NodeState *n = jazz_db_node_lock(self->node);
        jz_err e = NS_FN(commit_unit_for)(n, tx_id, &unit);
        jazz_db_node_unlock(self->node);
        if (e.code) return node_err(e);
    }
    bool inserted = false;
    jz_deque *uploads = (jz_deque *)jz_btree_entry(&up->large_value_uploads, &tx_id, &inserted);
    if (inserted) {
        LargeValueRefVec refs;
        commit_unit_large_value_refs(&unit, &refs);
        jz_deque_init(uploads, sizeof(jazz_db_PendingLargeValueUpload));
        for (u32 i = 0; i < refs.len; ++i) {
            jazz_db_PendingLargeValueUpload u;
            memset(&u, 0, sizeof(u));
            u.value_ref = refs.ptr[i];
            jz_deque_init(&u.requested, sizeof(groove_NodeRef));
            jz_deque_init(&u.in_flight, sizeof(groove_NodeRef));
            u.has_retry_not_before_ms = false;
            u.started = false;
            jz_deque_push_back(uploads, &u);
        }
        jz_vec_drop(&refs, sizeof(groove_LargeValueRef), NULL);
    }
    if (uploads->len != 0) {
        jazz_db_PendingLargeValueUpload *upload = (jazz_db_PendingLargeValueUpload *)jz_deque_at(uploads, 0);
        now_ms = self->upload_retry_clock->now_ms(self->upload_retry_clock->self);
        if (upload->has_retry_not_before_ms && now_ms < upload->retry_not_before_ms) {
            jz_btree_insert(blocked_writers, &tx_id.node, NULL, NULL);
            jazz_protocol__protocol__SyncMessage__drop(&unit);
            return JZ_OK;
        }
        upload->has_retry_not_before_ms = false;
        bool supplying_nodes = upload->started;
        u32 supplied_count = 0;
        jazz_protocol_SyncMessage message;
        memset(&message, 0, sizeof(message));
        if (!upload->started) {
            message.tag = JAZZ_SM_CHUNK_UPLOAD_START;
            message.v.chunk_upload_start.value_ref = upload->value_ref;
        } else {
            u32 take = upload->requested.len < 64 ? upload->requested.len : 64;
            if (take == 0) {
                jz_btree_insert(blocked_writers, &tx_id.node, NULL, NULL);
                jazz_protocol__protocol__SyncMessage__drop(&unit);
                return JZ_OK;
            }
            message.tag = JAZZ_SM_CHUNK_UPLOAD_NODES;
            message.v.chunk_upload_nodes.value_ref = upload->value_ref;
            jz_vec_reserve(&message.v.chunk_upload_nodes.chunks, take, sizeof(groove_StagedChunk));
            for (u32 i = 0; i < take; ++i) {
                groove_NodeRef node_ref = *(groove_NodeRef *)jz_deque_at(&upload->requested, i);
                const char *replica_role;
                jazz_types_NodeUuid source_node;
                jz_bytes encoded = {0};
                jz_err result;
                {
                    jazz_node_NodeState *n = jazz_db_node_lock(self->node);
                    replica_role = NS_FN(authored_commit_durability)(n) == JAZZ_DUR_NONE ? "non-durable-client" : "durable-relay";
                    source_node = NS_FN(node_uuid)(n);
                    result = NS_FN(local_chunk)(n, node_ref.locator, node_ref.object_hash, &encoded);
                    jazz_db_node_unlock(self->node);
                }
                if (result.code) {
                    jz_str context = large_value_upload_chunk_context(tx_id, &upload->value_ref, &node_ref, replica_role, source_node);
                    jz_err out = jz_err_fmt(JAZZ_NODE_ERR_LARGE_VALUE_UPLOAD_CHUNK_UNAVAILABLE, "large value upload chunk unavailable (%.*s): %.*s",
                                            (int)context.len, context.ptr, result.msg ? (int)result.msg->len : 0, result.msg ? result.msg->ptr : "");
                    jz_str_drop(&context);
                    jz_err_drop(&result);
                    jazz_protocol__protocol__SyncMessage__drop(&message);
                    jazz_protocol__protocol__SyncMessage__drop(&unit);
                    return node_err(out);
                }
                groove_StagedChunk chunk = { node_ref, encoded };
                jz_vec_push(&message.v.chunk_upload_nodes.chunks, &chunk, sizeof(chunk));
            }
            supplied_count = message.v.chunk_upload_nodes.chunks.len;
        }
        bool bp;
        jz_err e = tick_send(t, message, &bp);
        if (e.code) { jazz_protocol__protocol__SyncMessage__drop(&unit); return e; }
        if (bp) {
            /* A full large-value channel holds back this commit and every
             * later one, but not requests: none of those commits is counted
             * yet. */
            *break_uploads = true;
            jazz_protocol__protocol__SyncMessage__drop(&unit);
            return JZ_OK;
        }
        if (supplying_nodes) {
            for (u32 i = 0; i < supplied_count; ++i) {
                groove_NodeRef sent;
                if (!jz_deque_pop_front(&upload->requested, &sent)) jz_panic("sent nodes came from requested frontier");
                jz_deque_push_back(&upload->in_flight, &sent);
            }
        }
        upload->started = true;
        jazz_db__db__UploadOutbox__note_upload_progress(up->outbox);
        groove_LargeValueRef awaiting = upload->value_ref;
        jz_btree_insert(&up->awaiting_large_value_uploads, &tx_id, &awaiting, NULL);
    }
    if (jz_btree_contains(&up->awaiting_large_value_uploads, &tx_id) || uploads->len != 0) {
        jz_btree_insert(blocked_writers, &tx_id.node, NULL, NULL);
        jazz_protocol__protocol__SyncMessage__drop(&unit);
        return JZ_OK;
    }
    jz_err e = send_with_local_sync_context(self->node, &self->transport, unit);
    if (e.code) {
        if (handle_db_backpressure(self->node, self->scheduler, &e)) { jz_err_drop(&e); *stop = true; return JZ_OK; }
        return e;
    }
    jz_btree_remove(&up->large_value_uploads, &tx_id, NULL, NULL);
    jz_btree_insert(&up->uploaded, &tx_id, NULL, NULL);
    jazz_db__db__UploadOutbox__mark_on_wire(up->outbox, tx_id);
    jazz_db__read_barrier__ReadBarrierSender__commit_unit_sent(&up->read_barrier);
    return JZ_OK;
}

/// One `PendingUpstreamCommand::Subscribe` at the head of `pending`.
static jz_err upstream_send_subscribe(TickCtx *t, jazz_db_UpstreamConnectionState *up, jazz_db_PendingUpstreamSubscription *pending_subscription, bool *stop) {
    PC *self = t->self;
    const jazz_model_ValidatedQuery *shape = pending_subscription->shape;
    const jazz_model_Binding *binding = pending_subscription->binding;
    jazz_db_ShapeRegistrationKey registration_key = { jazz_model__query__ValidatedQuery__shape_id(shape),
                                                      jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&pending_subscription->opts) };
    if (!jz_btree_insert(&up->announced_shapes, &registration_key, NULL, NULL)) {
        jazz_protocol_SyncMessage reg;
        memset(&reg, 0, sizeof(reg));
        reg.tag = JAZZ_SM_REGISTER_SHAPE;
        reg.v.register_shape.shape_id = registration_key.shape_id;
        reg.v.register_shape.ast = jazz_protocol__protocol__ShapeAst__from_validated(shape);
        /* The local producer-side registration must use the same
         * read-view/compiler identity as the Subscribe we are about to apply
         * and transmit. Registering a default alias here leaves the exact
         * usage site without compiler options. */
        jazz_protocol__protocol__RegisterShapeOptions__clone(&reg.v.register_shape.opts, &pending_subscription->opts);
        jazz_node_PublicationOutcome outcome;
        jazz_node_NodeState *n = jazz_db_node_lock(self->node);
        jz_err e = NS_FN(apply_sync_message)(n, reg, &outcome);
        jazz_db_node_unlock(self->node);
        if (e.code) return node_err(e);
        u32 changed = 0;
        JZ_TRY(finish_peer_publication_outcome(self->node, self->subscriptions, self->active_authority_view_receipts, tick_waker(t), outcome, &changed));
        t->stats.subscription_events += changed;
        jazz_protocol_SyncMessage wire;
        memset(&wire, 0, sizeof(wire));
        wire.tag = JAZZ_SM_REGISTER_SHAPE;
        wire.v.register_shape.shape_id = registration_key.shape_id;
        wire.v.register_shape.ast = jazz_protocol__protocol__ShapeAst__from_validated(shape);
        jazz_protocol__protocol__RegisterShapeOptions__clone(&wire.v.register_shape.opts, &pending_subscription->opts);
        bool bp;
        e = tick_send(t, wire, &bp);
        if (e.code || bp) {
            jz_btree_remove(&up->announced_shapes, &registration_key, NULL, NULL);
            if (bp) *stop = true;
            return e;
        }
    }
    groove_ValueVec values = binding_values_in_param_order(shape, binding);
    void *known_state = NULL;
    {
        jazz_node_NodeState *n = jazz_db_node_lock(self->node);
        jz_err e = NS_FN(known_state_declaration_for_subscription)(n, shape, binding, pending_subscription->subscription, (const jazz_model_ValueVec *)&values,
                                                                   pending_subscription->identity,
                                                                   t->permits_delegated_sessions && pending_subscription->has_policy_binding ? &pending_subscription->policy_binding : NULL,
                                                                   &known_state);
        jazz_db_node_unlock(self->node);
        if (e.code) { groove__records__ValueVec__drop(&values); return node_err(e); }
    }
    jazz_protocol_SyncMessage subscribe;
    memset(&subscribe, 0, sizeof(subscribe));
    subscribe.tag = JAZZ_SM_SUBSCRIBE;
    subscribe.v.subscribe.shape_id = registration_key.shape_id;
    subscribe.v.subscribe.subscription = pending_subscription->subscription;
    memcpy(&subscribe.v.subscribe.values, &values, sizeof(values));
    subscribe.v.subscribe.known_state = known_state;
    if (t->permits_delegated_sessions && pending_subscription->has_policy_binding) {
        subscribe.v.subscribe.has_delegated_session = true;
        subscribe.v.subscribe.delegated_session.identity = pending_subscription->policy_binding.identity;
        claims_clone(&subscribe.v.subscribe.delegated_session.claims, &pending_subscription->policy_binding.claims);
    }
    jazz_protocol_SyncMessage applied;
    jazz_protocol__protocol__SyncMessage__clone(&applied, &subscribe);
    jazz_node_PublicationOutcome outcome;
    {
        jazz_node_NodeState *n = jazz_db_node_lock(self->node);
        jz_err e = NS_FN(apply_sync_message)(n, applied, &outcome);
        jazz_db_node_unlock(self->node);
        if (e.code) { jazz_protocol__protocol__SyncMessage__drop(&subscribe); return node_err(e); }
    }
    u32 changed = 0;
    jz_err e = finish_peer_publication_outcome(self->node, self->subscriptions, self->active_authority_view_receipts, tick_waker(t), outcome, &changed);
    if (e.code) { jazz_protocol__protocol__SyncMessage__drop(&subscribe); return e; }
    t->stats.subscription_events += changed;
    TICK_SEND_OR_STOP(t, subscribe);
    jazz_db_PendingUpstreamSubscription copy;
    jazz_db__db__PendingUpstreamSubscription__clone(&copy, pending_subscription);
    jazz_db_PendingUpstreamSubscription old;
    if (jz_btree_insert(&up->sent_subscriptions, &pending_subscription->subscription, &copy, &old)) jazz_db__db__PendingUpstreamSubscription__drop(&old);
    return JZ_OK;
}

static bool lease_request_matches(const jazz_db_AuthorizationScopeLeaseRequest *request, const jazz_protocol_PermissionAdviceAction *action,
                                  const jazz_db_PolicyBinding *binding, bool has_delegated, const jazz_protocol_DelegatedSessionBinding *delegated) {
    if (!jazz_protocol__protocol__PermissionAdviceAction__eq(request->action, action)) return false;
    if (!jazz_db__db__PolicyBinding__eq(&request->session_claim_binding, binding)) return false;
    if (request->has_delegated_session != has_delegated) return false;
    return !has_delegated || jazz_protocol__protocol__DelegatedSessionBinding__eq(&request->delegated_session, delegated);
}

static jazz_protocol_SyncMessage scope_intent_message(jazz_protocol_PermissionAdviceRequestId request_id, const jazz_protocol_PermissionAdviceAction *action,
                                                      bool has_delegated, const jazz_protocol_DelegatedSessionBinding *delegated) {
    jazz_protocol_SyncMessage m;
    memset(&m, 0, sizeof(m));
    m.tag = JAZZ_SM_AUTHORIZATION_SCOPE_INTENT;
    m.v.scope_intent.request_id = request_id;
    m.v.scope_intent.action = jazz_protocol__protocol__PermissionAdviceAction__clone(action);
    m.v.scope_intent.has_delegated_session = has_delegated;
    if (has_delegated) jazz_protocol__protocol__DelegatedSessionBinding__clone(&m.v.scope_intent.delegated_session, delegated);
    return m;
}

/// One `PendingUpstreamCommand::AuthorizationScopeIntent` at the head of
/// `pending`. `*removed` = the command was already removed (`continue`).
static jz_err upstream_send_scope_intent(TickCtx *t, jazz_db_UpstreamConnectionState *up, jazz_db_PendingUpstreamCommand *command, bool *removed, bool *stop) {
    PC *self = t->self;
    jz_btree *requests = &up->scope_lease_manager.requests;
    jazz_protocol_PermissionAdviceRequestId request_id = command->intent.request_id;
    /* An old or unauthenticated upstream must never receive a downgraded
     * preflight. Resolve conservatively instead. */
    if (!up->has_expected_scope_authority) {
        jazz_db_AuthorizationScopeLeaseRequest request;
        if (jz_btree_remove(requests, &request_id, NULL, &request)) {
            jazz_protocol_PermissionAdviceRequestId *waiter_id;
            void *unit;
            JZ_BTREE_FOR(&request.waiters, waiter_id, unit) { (void)unit; permission_waiter_remove(self->permission_advice_waiters, waiter_id, false); }
            jazz_db__db__AuthorizationScopeLeaseRequest__drop(&request);
        } else {
            permission_waiter_remove(self->permission_advice_waiters, &request_id, false);
        }
        return JZ_OK;
    }
    bool has_live_waiter = false;
    const jazz_db_AuthorizationScopeLeaseRequest *existing_request = (const jazz_db_AuthorizationScopeLeaseRequest *)jz_btree_get(requests, &request_id);
    if (existing_request) {
        jazz_protocol_PermissionAdviceRequestId *waiter_id;
        void *unit;
        JZ_BTREE_FOR(&existing_request->waiters, waiter_id, unit) {
            (void)unit;
            if (jz_btree_contains(self->permission_advice_waiters, waiter_id)) { has_live_waiter = true; break; }
        }
    } else {
        has_live_waiter = jz_btree_contains(self->permission_advice_waiters, &request_id);
    }
    if (!has_live_waiter) {
        jazz_db_AuthorizationScopeLeaseRequest request;
        if (jz_btree_remove(requests, &request_id, NULL, &request)) jazz_db__db__AuthorizationScopeLeaseRequest__drop(&request);
        jazz_db_PendingUpstreamCommand gone;
        jz_vec_remove(&up->pending, 0, &gone, sizeof(gone));
        jazz_db__db__PendingUpstreamCommand__drop(&gone);
        *removed = true;
        return JZ_OK;
    }
    jazz_protocol_AuthorityContext expected = up->expected_scope_authority;
    jazz_db_PolicyBinding session_claim_binding;
    if (command->intent.has_session_claim_binding) {
        jazz_db__db__PolicyBinding__clone(&session_claim_binding, &command->intent.session_claim_binding);
    } else if (existing_request) {
        jazz_db__db__PolicyBinding__clone(&session_claim_binding, &existing_request->session_claim_binding);
    } else {
        jazz_types_AuthorSubject identity = expected.link;
        session_claim_binding.identity = identity;
        if (!session_claims_for_identity(self->node, identity, &session_claim_binding.claims))
            jz_btree_init(&session_claim_binding.claims, &jazz_db_str_value_desc);
    }
    /* Allocation establishes the immutable request binding before the first
     * wire send. A backpressured command remains in this queue, so it must
     * carry the same binding on a later turn. */
    if (command->intent.has_session_claim_binding) jazz_db__db__PolicyBinding__drop(&command->intent.session_claim_binding);
    jazz_db__db__PolicyBinding__clone(&command->intent.session_claim_binding, &session_claim_binding);
    command->intent.has_session_claim_binding = true;
    bool claims_still_bound;
    {
        jz_btree current;
        if (!session_claims_for_identity(self->node, session_claim_binding.identity, &current)) jz_btree_init(&current, &jazz_db_str_value_desc);
        claims_still_bound = jz_btree_eq(&current, &session_claim_binding.claims, groove__records__Value__eq);
        jz_btree_drop(&current);
    }
    if (!command->intent.has_delegated_session && !claims_still_bound) {
        jazz_db_AuthorizationScopeLeaseRequest request;
        if (jz_btree_remove(requests, &request_id, NULL, &request)) {
            jazz_protocol_PermissionAdviceRequestId *waiter_id;
            void *unit;
            JZ_BTREE_FOR(&request.waiters, waiter_id, unit) { (void)unit; permission_waiter_remove(self->permission_advice_waiters, waiter_id, true); }
            jazz_db__db__AuthorizationScopeLeaseRequest__drop(&request);
        } else {
            permission_waiter_remove(self->permission_advice_waiters, &request_id, true);
        }
        jazz_db__db__PolicyBinding__drop(&session_claim_binding);
        jazz_db_PendingUpstreamCommand gone;
        jz_vec_remove(&up->pending, 0, &gone, sizeof(gone));
        jazz_db__db__PendingUpstreamCommand__drop(&gone);
        *removed = true;
        return JZ_OK;
    }
    jazz_protocol_PermissionAdviceRequestId *wire_request_id = NULL;
    jazz_db_AuthorizationScopeLeaseRequest *matching = NULL;
    {
        jazz_protocol_PermissionAdviceRequestId *k;
        jazz_db_AuthorizationScopeLeaseRequest *v;
        JZ_BTREE_FOR(requests, k, v) {
            if (lease_request_matches(v, command->intent.action, &session_claim_binding, command->intent.has_delegated_session, &command->intent.delegated_session)) {
                wire_request_id = k;
                matching = v;
                break;
            }
        }
    }
    if (matching) {
        jz_btree_insert(&matching->waiters, &request_id, NULL, NULL);
        jazz_db__db__PolicyBinding__drop(&session_claim_binding);
        if (!matching->intent_sent) {
            TICK_SEND_OR_STOP(t, scope_intent_message(*wire_request_id, matching->action, command->intent.has_delegated_session, &command->intent.delegated_session));
            matching->intent_sent = true;
        }
    } else {
        jazz_db_AuthorizationScopeLeaseRequest request;
        memset(&request, 0, sizeof(request));
        request.action = jazz_protocol__protocol__PermissionAdviceAction__clone(command->intent.action);
        request.session_claim_binding = session_claim_binding;
        request.has_delegated_session = command->intent.has_delegated_session;
        if (request.has_delegated_session) jazz_protocol__protocol__DelegatedSessionBinding__clone(&request.delegated_session, &command->intent.delegated_session);
        jz_btree_init(&request.waiters, &jazz_db_permission_request_id_set_desc);
        jz_btree_insert(&request.waiters, &request_id, NULL, NULL);
        request.intent_sent = false;
        request.key = NULL;
        request.lease = NULL;
        request.owner = NULL;
        request.has_clause_count = false;
        jz_btree_init(&request.applied_clauses, &jazz_db_scope_applied_clauses_desc);
        jazz_db_AuthorizationScopeLeaseRequest old;
        if (jz_btree_insert(requests, &request_id, &request, &old)) jazz_db__db__AuthorizationScopeLeaseRequest__drop(&old);
        TICK_SEND_OR_STOP(t, scope_intent_message(request_id, command->intent.action, command->intent.has_delegated_session, &command->intent.delegated_session));
        jazz_db_AuthorizationScopeLeaseRequest *inserted = (jazz_db_AuthorizationScopeLeaseRequest *)jz_btree_get(requests, &request_id);
        if (!inserted) jz_panic("inserted authorization scope request");
        inserted->intent_sent = true;
    }
    return JZ_OK;
}

/// The upstream `outbound_stop` scope: auxiliary pump output, session
/// claims, the upload outbox, the read barrier and pending commands.
static jz_err upstream_outbound(TickCtx *t, jazz_db_UpstreamConnectionState *up, bool *stop) {
    PC *self = t->self;
    *stop = false;
    {
        jazz_protocol_SyncMessage message;
        if (jazz_db__db__PeerIoPump__take_outbound(&self->auxiliary_pump, 64, &message)) {
            jazz_protocol_SyncMessage copy;
            jazz_protocol__protocol__SyncMessage__clone(&copy, &message);
            jazz_db_TransportError error;
            jz_err e = jazz_db__db__node_runtime__Transport__send(&self->transport, copy, &error);
            if (e.code) {
                jz_err_drop(&e);
                jazz_db__db__PeerIoPump__restore_outbound(&self->auxiliary_pump, message);
                if (handle_transport_backpressure(self->node, self->scheduler, &error)) {
                    jazz_protocol__wire__TransportError__drop(&error);
                    *stop = true;
                    return JZ_OK;
                }
                return jazz_db__db__peer_connection__transport_error(error);
            }
            jazz_db__db__PeerIoPump__acknowledge_outbound(&self->auxiliary_pump, &message);
            jazz_protocol__protocol__SyncMessage__drop(&message);
        }
    }
    if (up->upstream_subscriptions->len) {
        jz_vec_extend(&up->pending, up->upstream_subscriptions->ptr, up->upstream_subscriptions->len, sizeof(jazz_db_PendingUpstreamCommand));
        up->upstream_subscriptions->len = 0;
    }
    if (t->permits_delegated_sessions) {
        jazz_node_SessionClaimsRevVec claims;
        {
            jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
            NS_FN(session_claims_with_revisions)(n, &claims);
            jazz_db_node_unlock(self->node);
        }
        for (u32 i = 0; i < claims.len; ++i) {
            jazz_types_AuthorSubject identity = claims.ptr[i].identity;
            u64 revision = claims.ptr[i].revision;
            const u64 *sent = (const u64 *)jz_btree_get(&up->sent_session_claim_revisions, &identity);
            if (sent && *sent >= revision) continue;
            jazz_protocol_SyncMessage m;
            memset(&m, 0, sizeof(m));
            m.tag = JAZZ_SM_SESSION_CLAIMS;
            m.v.session_claims.identity = identity;
            m.v.session_claims.claims = claims.ptr[i].claims;
            jz_btree_init(&claims.ptr[i].claims, &jazz_db_str_value_desc);
            bool bp;
            jz_err e = tick_send(t, m, &bp);
            if (e.code || bp) { jazz_node__node__SessionClaimsRevVec__drop(&claims); if (bp) *stop = true; return e; }
            jz_btree_insert(&up->sent_session_claim_revisions, &identity, &revision, NULL);
        }
        jazz_node__node__SessionClaimsRevVec__drop(&claims);
    }
    /* Upload locally-authored commits not yet shipped on this link before
     * sending any request, so a request names every write ahead of it in
     * its read barrier. On a new link this resends the outbox before
     * reissuing opens (SPEC 8 §8.2.1). */
    JZ_VEC(ToUpload) to_upload = {0};
    {
        u32 outbox_len = jazz_db__db__UploadOutbox__len(up->outbox);
        bool has_expected_missing = outbox_len >= up->uploaded.len;
        u32 expected_missing = outbox_len - (has_expected_missing ? up->uploaded.len : 0);
        u32 suffix_len = 0;
        for (u32 i = outbox_len; i > 0; --i) {
            if (jz_btree_contains(&up->uploaded, &jazz_db__db__UploadOutbox__at(up->outbox, i - 1)->tx_id)) break;
            suffix_len++;
        }
        bool use_suffix = has_expected_missing && expected_missing == suffix_len;
        for (u32 i = 0; i < outbox_len; ++i) {
            const jazz_db_PendingUpload *pending = jazz_db__db__UploadOutbox__at(up->outbox, i);
            if (use_suffix ? i < outbox_len - suffix_len : jz_btree_contains(&up->uploaded, &pending->tx_id)) continue;
            /* Fate cleanup and authority handoff can leave holes in this
             * link's uploaded set. Preserve the general set-difference
             * behavior there. */
            ToUpload *slot = (ToUpload *)jz_vec_push_slot(&to_upload, sizeof(ToUpload));
            slot->tx_id = pending->tx_id;
            slot->has_unit = pending->has_unit;
            if (pending->has_unit) jazz_protocol__protocol__SyncMessage__clone(&slot->unit, &pending->unit);
        }
    }
    /* A writer's writes go up in outbox order (SPEC 14): once one of them is
     * held back on this pass, none of that writer's later writes overtakes
     * it. Other writers' uploads go on. */
    jz_btree blocked_writers;
    jz_btree_init(&blocked_writers, &node_uuid_set_desc);
    jz_err err = JZ_OK;
    for (u32 i = 0; i < to_upload.len; ++i) {
        bool break_uploads = false;
        err = upstream_upload_one(t, up, &to_upload.ptr[i], &blocked_writers, &break_uploads, stop);
        if (err.code || *stop || break_uploads) break;
    }
    jz_btree_drop(&blocked_writers);
    jz_vec_drop(&to_upload, sizeof(ToUpload), to_upload_drop);
    if (err.code || *stop) return err;
    if (up->pending.len != 0) {
        jazz_protocol_SyncMessage barrier;
        if (jazz_db__read_barrier__ReadBarrierSender__pending_barrier(&up->read_barrier, &barrier)) {
            TICK_SEND_OR_STOP(t, barrier);
            jazz_db__read_barrier__ReadBarrierSender__barrier_accepted(&up->read_barrier);
        }
    }
    const u32 pending_index = 0;
    while (pending_index < up->pending.len) {
        jazz_db_PendingUpstreamCommand *command = &up->pending.ptr[pending_index];
        switch (command->tag) {
        case JAZZ_PUC_SUBSCRIBE:
            JZ_TRY(upstream_send_subscribe(t, up, &command->subscribe, stop));
            if (*stop) return JZ_OK;
            break;
        case JAZZ_PUC_UNSUBSCRIBE: {
            jazz_protocol_SubscriptionKey subscription = command->unsubscribe;
            jazz_db_PendingUpstreamSubscription old;
            if (jz_btree_remove(&up->sent_subscriptions, &subscription, NULL, &old)) jazz_db__db__PendingUpstreamSubscription__drop(&old);
            jz_btree_remove(&up->awaiting_support_snapshots, &subscription, NULL, NULL);
            if (self->active_authority_view_receipts->has) jz_btree_remove(&self->active_authority_view_receipts->subscriptions, &subscription, NULL, NULL);
            jazz_db_ShapeRegistrationKey key = { subscription.shape_id, subscription.read_view };
            jz_btree_remove(&up->announced_shapes, &key, NULL, NULL);
            /* Local finalization may already have applied this retirement.
             * Reapplying is idempotent, and the command remains in `pending`
             * until the send succeeds. */
            {
                jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
                NS_FN(apply_unsubscribe)(n, subscription);
                jazz_db_node_unlock(self->node);
            }
            jazz_protocol_SyncMessage m;
            memset(&m, 0, sizeof(m));
            m.tag = JAZZ_SM_UNSUBSCRIBE;
            m.v.unsubscribe.subscription = subscription;
            TICK_SEND_OR_STOP(t, m);
            break;
        }
        default: {
            bool removed = false;
            JZ_TRY(upstream_send_scope_intent(t, up, command, &removed, stop));
            if (*stop) return JZ_OK;
            if (removed) continue;
            break;
        }
        }
        jazz_db_PendingUpstreamCommand done;
        jz_vec_remove(&up->pending, pending_index, &done, sizeof(done));
        jazz_db__db__PendingUpstreamCommand__drop(&done);
    }
    return JZ_OK;
}

/* ---------------------------------------------------------------- upstream inbound */

typedef struct UpstreamInbound {
    bool applied;
    JZ_VEC(jazz_node_PublishedTransaction *) publications;
    jazz_db_PendingAuthorityViewUpdateVec pending_view_updates;
    jz_btree pending_initial_coverage_clears; /* BTreeSet<CoverageKey> */
    bool deferred_stop;
} UpstreamInbound;

static jz_err inbound_flush_views(TickCtx *t, jazz_db_UpstreamConnectionState *up, UpstreamInbound *in) {
    PC *self = t->self;
    if (in->pending_view_updates.len == 0) return JZ_OK;
    return apply_pending_authority_view_updates(self->node, self->subscriptions, &in->pending_view_updates, self->relay_upstream_subscription_owners,
                                                self->pending_relay_subscription_rejections, up->upstream_subscriptions, self->awaiting_initial_authority_coverage,
                                                &in->pending_initial_coverage_clears, self->query_coverage_registrations, self->active_authority_view_receipts,
                                                self->coverage_refresh_generations, self->subscriber_dirty_epoch, self->scheduler, tick_waker(t), self->connection_epoch);
}

static void bump_subscriber_dirty_epoch(PC *self) { *self->subscriber_dirty_epoch += 1; }

static void outcome_take_publications(UpstreamInbound *in, jazz_node_PublicationOutcome *outcome) {
    if (outcome->publications.len)
        jz_vec_extend(&in->publications, outcome->publications.ptr, outcome->publications.len, sizeof(jazz_node_PublishedTransaction *));
    outcome->publications.len = 0;
    jazz_node__node__PublicationOutcome__drop(outcome);
}

static void lease_request_resolve(PC *self, jz_btree *requests, const jazz_protocol_PermissionAdviceRequestId *request_id, bool send, u8 advice) {
    jazz_db_AuthorizationScopeLeaseRequest request;
    if (!jz_btree_remove(requests, request_id, NULL, &request)) return;
    jazz_protocol_PermissionAdviceRequestId *waiter_id;
    void *unit;
    JZ_BTREE_FOR(&request.waiters, waiter_id, unit) {
        (void)unit;
        jazz_db_Oneshot *waiter = NULL;
        if (jz_btree_remove(self->permission_advice_waiters, waiter_id, NULL, &waiter) && waiter) {
            if (send) (void)jazz_db__oneshot__send(waiter, &advice);
            jazz_db__oneshot__drop_sender(waiter);
        }
    }
    jazz_db__db__AuthorizationScopeLeaseRequest__drop(&request);
}

static bool binding_claims_still_bound(PC *self, const jazz_db_PolicyBinding *binding) {
    jz_btree current;
    if (!session_claims_for_identity(self->node, binding->identity, &current)) jz_btree_init(&current, &jazz_db_str_value_desc);
    bool same = jz_btree_eq(&current, &binding->claims, groove__records__Value__eq);
    jz_btree_drop(&current);
    return same;
}

static bool keep_not_tx(const jazz_db_PendingUpload *pending, void *ctx) { return !jazz_model_TxId_eq(pending->tx_id, *(const jazz_model_TxId *)ctx); }

static void inbound_chunk_upload_result(TickCtx *t, jazz_db_UpstreamConnectionState *up, jazz_protocol_SyncMessage *message) {
    PC *self = t->self;
    groove_LargeValueRef *value_ref = &message->v.chunk_upload_result.value_ref;
    jazz_protocol_ChunkUploadStatus *status = &message->v.chunk_upload_result.status;
    bool rate_limited = status->tag == JAZZ_CHUNK_UPLOAD_RATE_LIMITED;
    bool has_pending_tx = false;
    jazz_model_TxId pending_tx;
    {
        jazz_model_TxId *tx;
        groove_LargeValueRef *awaiting;
        JZ_BTREE_FOR(&up->awaiting_large_value_uploads, tx, awaiting) {
            if (groove_LargeValueRef_eq(awaiting, value_ref)) { pending_tx = *tx; has_pending_tx = true; break; }
        }
    }
    switch (status->tag) {
    case JAZZ_CHUNK_UPLOAD_NEED:
        if (has_pending_tx) {
            jazz_db__db__UploadOutbox__note_upload_progress(up->outbox);
            jz_btree_remove(&up->awaiting_large_value_uploads, &pending_tx, NULL, NULL);
            jz_deque *uploads = (jz_deque *)jz_btree_get(&up->large_value_uploads, &pending_tx);
            if (uploads && uploads->len) {
                jazz_db_PendingLargeValueUpload *upload = (jazz_db_PendingLargeValueUpload *)jz_deque_at(uploads, 0);
                groove_NodeRef drop_ref;
                while (jz_deque_pop_front(&upload->in_flight, &drop_ref)) {}
                for (u32 i = 0; i < status->need.len; ++i) jz_deque_push_back(&upload->requested, &status->need.ptr[i]);
            }
        }
        break;
    case JAZZ_CHUNK_UPLOAD_STAGED:
        if (has_pending_tx) {
            jazz_db__db__UploadOutbox__note_upload_progress(up->outbox);
            jz_btree_remove(&up->awaiting_large_value_uploads, &pending_tx, NULL, NULL);
            jz_deque *uploads = (jz_deque *)jz_btree_get(&up->large_value_uploads, &pending_tx);
            if (uploads) {
                if (uploads->len) {
                    jazz_db_PendingLargeValueUpload *upload = (jazz_db_PendingLargeValueUpload *)jz_deque_at(uploads, 0);
                    groove_NodeRef drop_ref;
                    while (jz_deque_pop_front(&upload->in_flight, &drop_ref)) {}
                }
                jazz_db_PendingLargeValueUpload front;
                if (jz_deque_pop_front(uploads, &front)) jazz_db__db__peer_connection__PendingLargeValueUpload__drop(&front);
            }
        }
        break;
    case JAZZ_CHUNK_UPLOAD_RATE_LIMITED:
        if (has_pending_tx) {
            jz_btree_remove(&up->awaiting_large_value_uploads, &pending_tx, NULL, NULL);
            jz_deque *uploads = (jz_deque *)jz_btree_get(&up->large_value_uploads, &pending_tx);
            if (uploads && uploads->len) {
                jazz_db_PendingLargeValueUpload *upload = (jazz_db_PendingLargeValueUpload *)jz_deque_at(uploads, 0);
                groove_NodeRef node_ref;
                while (jz_deque_pop_back(&upload->in_flight, &node_ref)) jz_deque_push_front(&upload->requested, &node_ref);
                u64 now_ms = self->upload_retry_clock->now_ms(self->upload_retry_clock->self);
                u64 deadline = now_ms > UINT64_MAX - RATE_LIMITED_UPLOAD_RETRY_DELAY_MS ? UINT64_MAX : now_ms + RATE_LIMITED_UPLOAD_RETRY_DELAY_MS;
                upload->has_retry_not_before_ms = true;
                upload->retry_not_before_ms = deadline;
                jz_btree_insert(self->large_value_upload_retry_deadlines, &pending_tx, &deadline, NULL);
                if (self->scheduler->has) self->scheduler->s.vt->schedule_tick_after(self->scheduler->s.self, RATE_LIMITED_UPLOAD_RETRY_DELAY_MS);
            }
        }
        break;
    default: /* Rejected */
        if (has_pending_tx) {
            jz_btree_remove(&up->awaiting_large_value_uploads, &pending_tx, NULL, NULL);
            jz_btree_insert(&up->failed_large_value_uploads, &pending_tx, NULL, NULL);
            jz_btree_remove(&up->large_value_uploads, &pending_tx, NULL, NULL);
            jz_btree_remove(self->large_value_upload_retry_deadlines, &pending_tx, NULL, NULL);
            jazz_db__db__UploadOutbox__retain(up->outbox, keep_not_tx, &pending_tx);
            jazz_db__db__UploadOutbox__mark_upload_failed(up->outbox, pending_tx, "its large value was not staged by the server");
            jazz_db_StagedInboundMessage staged;
            memset(&staged, 0, sizeof(staged));
            staged.message.tag = JAZZ_SM_FATE_UPDATE;
            staged.message.v.fate_update.tx_id = pending_tx;
            staged.message.v.fate_update.fate.tag = JAZZ_FATE_REJECTED;
            staged.message.v.fate_update.fate.rejected.tag = JAZZ_REJECTION_MALFORMED_COMMIT;
            staged.message.v.fate_update.fate.rejected.detail = jz_str_from("large-value upload was not staged; upload again");
            staged.message.v.fate_update.has_global_time = false;
            staged.message.v.fate_update.has_durability = false;
            staged.authority_receipt_eligible = false;
            staged.lease = NULL;
            staged.receipts_validated = false;
            jz_deque_push_front(&self->staged_inbound, &staged);
        }
        break;
    }
    if (!rate_limited) jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_IMMEDIATE);
}

/// SyncMessage::ViewUpdate on an upstream link. `*cont` = `continue`.
static jz_err inbound_view_update(TickCtx *t, jazz_db_UpstreamConnectionState *up, UpstreamInbound *in, jazz_protocol_SyncMessage message,
                                  bool authority_receipt_eligible, bool *cont) {
    PC *self = t->self;
    jazz_protocol_ViewUpdatePayload *view = message.v.view_update;
    jazz_protocol_SubscriptionKey subscription = view->subscription;
    jazz_types_GlobalTime settled_through = view->settled_through;
    *cont = false;
    jazz_types_GlobalTime *minimum_cut = (jazz_types_GlobalTime *)jz_btree_get(&up->awaiting_support_snapshots, &subscription);
    if (minimum_cut) {
        if (!authority_receipt_eligible && settled_through > *minimum_cut) *minimum_cut = settled_through;
        bool snapshot = jazz_protocol__protocol__SupportingRowsUpdate__is_snapshot(view->supporting_rows) && !view->peer_payload_inventory.opening_pending;
        if (!authority_receipt_eligible || !snapshot || settled_through < *minimum_cut) {
            jazz_node_NodeState *n = jazz_db_node_lock(self->node);
            jz_err e = NS_FN(remember_discarded_pending_view_transactions)(n, &view->version_carriers);
            jazz_db_node_unlock(self->node);
            jazz_protocol__protocol__SyncMessage__drop(&message);
            *cont = true;
            return e.code ? node_err(e) : JZ_OK;
        }
        jz_btree_remove(&up->awaiting_support_snapshots, &subscription, NULL, NULL);
    }
    u32 missing = 0;
    {
        jazz_node_NodeState *n = jazz_db_node_lock(self->node);
        jz_err e = NS_FN(missing_known_state_row_version_refs)(n, &message, &missing);
        jazz_db_node_unlock(self->node);
        if (e.code) { jazz_protocol__protocol__SyncMessage__drop(&message); return node_err(e); }
    }
    /* A catch-up is a delta against the revision declared with its
     * watermark. If this receiver has moved on since (an older frame landed
     * in between), ask again without a watermark for a complete set. */
    u8 predecessor[16];
    if (jazz_protocol__protocol__SupportingRowsUpdate__catch_up_predecessor(view->supporting_rows, predecessor)) {
        bool restart = false;
        for (u32 i = 0; i < in->pending_view_updates.len; ++i)
            if (jazz_protocol_SubscriptionKey_eq(&in->pending_view_updates.ptr[i].parts.subscription, &subscription)) { restart = true; break; }
        if (!restart) {
            jazz_node_NodeState *n = jazz_db_node_lock(self->node);
            restart = !NS_FN(holds_supporting_revision)(n, subscription, predecessor);
            jazz_db_node_unlock(self->node);
        }
        if (restart) {
            /* A late catch-up for a view this link no longer serves
             * (unsubscribed or replaced after a reopen) has nothing to
             * re-request. */
            const jazz_db_PendingUpstreamSubscription *request = (const jazz_db_PendingUpstreamSubscription *)jz_btree_get(&up->sent_subscriptions, &subscription);
            jazz_node_NodeState *n = jazz_db_node_lock(self->node);
            if (request) {
                NS_FN(forget_supporting_revision)(n, subscription);
                jz_btree_insert(&up->awaiting_support_snapshots, &subscription, &settled_through, NULL);
                jazz_db_PendingUpstreamCommand *command = (jazz_db_PendingUpstreamCommand *)jz_vec_push_slot(&up->pending, sizeof(jazz_db_PendingUpstreamCommand));
                memset(command, 0, sizeof(*command));
                command->tag = JAZZ_PUC_SUBSCRIBE;
                jazz_db__db__PendingUpstreamSubscription__clone(&command->subscribe, request);
            }
            jz_err e = NS_FN(remember_discarded_pending_view_transactions)(n, &view->version_carriers);
            jazz_db_node_unlock(self->node);
            jazz_protocol__protocol__SyncMessage__drop(&message);
            *cont = true;
            if (e.code) return node_err(e);
            if (request) jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_IMMEDIATE);
            return JZ_OK;
        }
    }
    if (missing == 0) {
        jz_btree_remove(&up->missing_body_resends, &subscription, NULL, NULL);
        stage_initial_coverage_clear_for_update(&message, self->latest_coverage_subscriptions, &in->pending_initial_coverage_clears);
        push_view_update_message_for_receiver(&in->pending_view_updates, message, authority_receipt_eligible);
        return JZ_OK;
    }
    /* Core ships the current image of every row this receiver does not
     * already hold. An update that names a held row whose body is gone
     * (evicted since the view was declared) reopens the view without known
     * state, so Core resends every row. There is no per-version fetch. */
    {
        jazz_node_NodeState *n = jazz_db_node_lock(self->node);
        jz_err e = NS_FN(remember_discarded_pending_view_transactions)(n, &view->version_carriers);
        jazz_db_node_unlock(self->node);
        jazz_protocol__protocol__SyncMessage__drop(&message);
        if (e.code) return node_err(e);
    }
    *cont = true;
    /* A late update for a view this link no longer serves has nothing to
     * reopen. */
    const jazz_db_PendingUpstreamSubscription *request = (const jazz_db_PendingUpstreamSubscription *)jz_btree_get(&up->sent_subscriptions, &subscription);
    if (!request) return JZ_OK;
    if (jz_btree_insert(&up->missing_body_resends, &subscription, NULL, NULL))
        return protocol_err("a full resend omitted rows this receiver does not hold");
    {
        jazz_node_NodeState *n = jazz_db_node_lock(self->node);
        NS_FN(forget_declared_known_state)(n, subscription);
        jazz_db_node_unlock(self->node);
    }
    jz_btree_insert(&up->awaiting_support_snapshots, &subscription, &settled_through, NULL);
    jazz_db_PendingUpstreamCommand *command = (jazz_db_PendingUpstreamCommand *)jz_vec_push_slot(&up->pending, sizeof(jazz_db_PendingUpstreamCommand));
    memset(command, 0, sizeof(*command));
    command->tag = JAZZ_PUC_SUBSCRIBE;
    jazz_db__db__PendingUpstreamSubscription__clone(&command->subscribe, request);
    jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_IMMEDIATE);
    return JZ_OK;
}

/// SyncMessage::AuthorizationScopeView on an upstream link.
static jz_err inbound_scope_view(TickCtx *t, jazz_db_UpstreamConnectionState *up, UpstreamInbound *in, jazz_protocol_SyncMessage message,
                                 bool authority_receipt_eligible, bool *cont) {
    PC *self = t->self;
    jz_btree *requests = &up->scope_lease_manager.requests;
    jazz_protocol_PermissionAdviceRequestId request_id = message.v.scope_view.request_id;
    jazz_protocol_AuthorizationSupportScopeKey *key = message.v.scope_view.key;
    u16 clause_index = message.v.scope_view.clause_index;
    u16 clause_count = message.v.scope_view.clause_count;
    jazz_protocol_ViewUpdatePayload *view = message.v.scope_view.view;
    *cont = true;
    if (!up->has_expected_scope_authority) { drop_peer_request(self->node); jazz_protocol__protocol__SyncMessage__drop(&message); return JZ_OK; }
    jazz_protocol_SubscriptionKey subscription = view->subscription;
    jazz_types_GlobalTime settled_through = view->settled_through;
    u64 authorization_progress = view->peer_payload_inventory.has_authorization_progress ? view->peer_payload_inventory.authorization_progress : 0;
    if (clause_count == 0 || clause_index >= clause_count) { drop_peer_request(self->node); jazz_protocol__protocol__SyncMessage__drop(&message); return JZ_OK; }
    jazz_db_AuthorizationScopeLeaseRequest *prior = (jazz_db_AuthorizationScopeLeaseRequest *)jz_btree_get(requests, &request_id);
    /* A cancelled intent cannot be revived by a late/replayed authority
     * view. */
    if (!prior) { jazz_protocol__protocol__SyncMessage__drop(&message); return JZ_OK; }
    jazz_db_PolicyBinding session_claim_binding;
    jazz_db__db__PolicyBinding__clone(&session_claim_binding, &prior->session_claim_binding);
    bool delegated_session = prior->has_delegated_session;
    bool key_mismatch = (prior->key && !jazz_protocol__protocol__AuthorizationSupportScopeKey__eq(prior->key, key))
                        || (prior->has_clause_count && prior->clause_count != clause_count);
    bool needs_acquire = prior->lease == NULL;
    jz_err err = JZ_OK;
    /* A trusted backend's link identity is SYSTEM, while its request
     * carries the user binding that owns this support proof. The
     * authority-selected key must name that exact immutable binding. */
    if (!jazz_types_AuthorSubject_eq(jazz_protocol__protocol__AuthorizationSupportScopeKey__subject(key), session_claim_binding.identity)) {
        drop_peer_request(self->node);
        goto drop_message;
    }
    if (!delegated_session && !binding_claims_still_bound(self, &session_claim_binding)) {
        lease_request_resolve(self, requests, &request_id, true, JAZZ_ADVICE_UNKNOWN);
        goto drop_message;
    }
    if (key_mismatch) { drop_peer_request(self->node); goto drop_message; }
    /* The first authenticated view reveals the server-selected scope key.
     * Acquire here, before the aggregate receipt, so concurrent actions that
     * compile to this same support scope share one registry lifecycle rather
     * than racing after hydration has already completed. */
    bool acquired = false;
    void *lease = NULL, *owner = NULL;
    u32 acquisition = JAZZ_SCOPE_ACQ_WAITING;
    if (needs_acquire)
        acquired = jazz_protocol__authorization_scope__AuthorizationScopeRegistry__acquire(up->scope_lease_manager.registry,
                                                                                           jazz_protocol__protocol__AuthorizationSupportScopeKey__clone(key), &lease, &acquisition, &owner);
    jazz_db_AuthorizationScopeLeaseRequest *request = (jazz_db_AuthorizationScopeLeaseRequest *)jz_btree_get(requests, &request_id);
    if (!request) goto drop_message;
    if (request->key) jazz_protocol__protocol__AuthorizationSupportScopeKey__drop(request->key);
    request->key = key;
    message.v.scope_view.key = NULL;
    request->has_clause_count = true;
    request->clause_count = clause_count;
    if (!request->lease) {
        if (!acquired) {
            permission_waiter_remove(self->permission_advice_waiters, &request_id, false);
            jazz_db_AuthorizationScopeLeaseRequest gone;
            if (jz_btree_remove(requests, &request_id, NULL, &gone)) jazz_db__db__AuthorizationScopeLeaseRequest__drop(&gone);
            goto drop_message;
        }
        request->owner = acquisition == JAZZ_SCOPE_ACQ_OWNER ? owner : NULL;
        request->lease = lease;
    }
    const jazz_db_AppliedScopeClause *prior_clause = (const jazz_db_AppliedScopeClause *)jz_btree_get(&request->applied_clauses, &clause_index);
    if (prior_clause && !jazz_protocol_SubscriptionKey_eq(&prior_clause->subscription, &subscription)) {
        drop_peer_request(self->node);
        goto drop_message;
    }
    /* The envelope remains a normal ViewUpdate; retain existing
     * batching/settlement semantics before a receipt can be accepted. */
    {
        jazz_protocol_SyncMessage update;
        memset(&update, 0, sizeof(update));
        update.tag = JAZZ_SM_VIEW_UPDATE;
        update.v.view_update = view;
        message.v.scope_view.view = NULL;
        push_view_update_message_for_receiver(&in->pending_view_updates, update, authority_receipt_eligible);
    }
    {
        jazz_db_AppliedScopeClause clause = { subscription, settled_through, authorization_progress };
        jz_btree_insert(&request->applied_clauses, &clause_index, &clause, NULL);
    }
    *cont = false;
drop_message:
    jazz_db__db__PolicyBinding__drop(&session_claim_binding);
    jazz_protocol__protocol__SyncMessage__drop(&message);
    return err;
}

/// SyncMessage::AuthorizationScopeAggregateReceipt on an upstream link.
static jz_err inbound_scope_receipt(TickCtx *t, jazz_db_UpstreamConnectionState *up, UpstreamInbound *in, jazz_protocol_SyncMessage message, bool *cont) {
    PC *self = t->self;
    jz_btree *requests = &up->scope_lease_manager.requests;
    jazz_protocol_PermissionAdviceRequestId request_id = message.v.scope_receipt.request_id;
    jazz_protocol_AuthorizationScopeReceipt *receipt_p = message.v.scope_receipt.receipt;
    *cont = true;
    /* Reject a receipt before applying any queued scope views when this
     * request's admitted claims have changed. A B-scoped proof must never
     * materialize support for an A request. */
    {
        const jazz_db_AuthorizationScopeLeaseRequest *request = (const jazz_db_AuthorizationScopeLeaseRequest *)jz_btree_get(requests, &request_id);
        bool claims_still_bound = !request || request->has_delegated_session || binding_claims_still_bound(self, &request->session_claim_binding);
        bool not_delegated = !request || !request->has_delegated_session;
        if (not_delegated && !claims_still_bound) {
            lease_request_resolve(self, requests, &request_id, true, JAZZ_ADVICE_UNKNOWN);
            jazz_protocol__protocol__SyncMessage__drop(&message);
            return JZ_OK;
        }
    }
    /* The authority's FIFO ordering says this receipt follows the views,
     * but apply the queued views now so receipt admission is never merely
     * queued. */
    jz_err e = inbound_flush_views(t, up, in);
    if (e.code) { jazz_protocol__protocol__SyncMessage__drop(&message); return e; }
    if (!up->has_expected_scope_authority) { drop_peer_request(self->node); jazz_protocol__protocol__SyncMessage__drop(&message); return JZ_OK; }
    jazz_protocol_AuthorityContext *expected = &up->expected_scope_authority;
    jazz_db_AuthorizationScopeLeaseRequest *request = (jazz_db_AuthorizationScopeLeaseRequest *)jz_btree_get(requests, &request_id);
    if (!request) { jazz_protocol__protocol__SyncMessage__drop(&message); return JZ_OK; }
    jazz_protocol_AuthorizationScopeReceiptFields receipt;
    jazz_protocol__protocol__AuthorizationScopeReceipt__fields(receipt_p, &receipt);
    /* An empty compiled support set is a valid public policy proof. It has
     * no views by construction; the authority receipt itself supplies the
     * server-chosen key and current context. */
    u16 clause_count = request->has_clause_count ? request->clause_count : 0;
    bool all_current = request->applied_clauses.len == clause_count;
    bool has_min_cut = false;
    jazz_types_GlobalTime min_cut = 0;
    bool has_min_progress = false;
    u64 min_progress = 0;
    {
        u16 *index;
        jazz_db_AppliedScopeClause *clause;
        JZ_BTREE_FOR(&request->applied_clauses, index, clause) {
            if (!(*index < clause_count && clause->cut >= receipt.settled_through)) all_current = false;
            if (!has_min_cut || clause->cut < min_cut) { min_cut = clause->cut; has_min_cut = true; }
            if (!has_min_progress || clause->progress < min_progress) { min_progress = clause->progress; has_min_progress = true; }
        }
    }
    bool has_applied_cut;
    jazz_types_GlobalTime applied_cut;
    if (clause_count == 0) { has_applied_cut = true; applied_cut = receipt.settled_through; }
    else { has_applied_cut = has_min_cut; applied_cut = min_cut; }
    u64 observed_claims, observed_policy;
    {
        jazz_node_NodeState *observed = jazz_db__db__LocalMutexBorrow__borrow(self->node);
        /* Delegated subjects have independent local and authority claim
         * counters. Only the admitted authority receipt can advance the
         * remote revision for this immutable snapshot. */
        observed_claims = request->has_delegated_session ? 0 : NS_FN(session_claim_revision)(observed, expected->link);
        observed_policy = NS_FN(active_catalogue_seq)(observed);
        jazz_db_node_unlock(self->node);
    }
    /* Context components are monotonic per admitted connection. A receipt
     * may advance an otherwise opaque authority revision, but it can never
     * decrease a component already admitted. */
    u64 applied_progress = has_min_progress ? min_progress : 0;
    bool receipt_decreased = receipt.claims_revision < expected->claims_revision
                             || receipt.policy_epoch < expected->policy_epoch
                             || receipt.authorization_progress < expected->authorization_progress
                             || receipt.settled_through < expected->settled_through;
    if (observed_claims > expected->claims_revision) expected->claims_revision = observed_claims;
    else if (observed_claims == 0) expected->claims_revision = receipt.claims_revision;
    if (observed_policy > expected->policy_epoch) expected->policy_epoch = observed_policy;
    else if (observed_policy == 0) expected->policy_epoch = receipt.policy_epoch;
    if (applied_progress > expected->authorization_progress) expected->authorization_progress = applied_progress;
    if (receipt.settled_through > expected->settled_through) expected->settled_through = receipt.settled_through;
    bool receipt_current = request->key && jazz_protocol__protocol__AuthorizationSupportScopeKey__eq(request->key, receipt.key)
                           && all_current && !receipt_decreased
                           && jazz_db__db__peer_connection__authorization_scope_receipt_matches_transport_context(receipt_p, *expected, request->session_claim_binding.identity,
                                                                                                                  has_applied_cut, applied_cut);
    if (!receipt_current) {
        if (!request->has_delegated_session && !binding_claims_still_bound(self, &request->session_claim_binding)) {
            lease_request_resolve(self, requests, &request_id, true, JAZZ_ADVICE_UNKNOWN);
            jazz_protocol__protocol__SyncMessage__drop(&message);
            return JZ_OK;
        }
        /* A claim/catalogue/progress transition can race a just-completed
         * hydration. Retire its lease and allocate a new opaque wire id so
         * old views/receipts cannot revive the operation; retain the caller
         * waiters and reacquire under the observed context. */
        jazz_protocol_PermissionAdviceRequestId retry_id;
        jz_uuid fresh = jz_uuid_v4();
        memcpy(retry_id.b, &fresh, 16);
        jazz_db_AuthorizationScopeLeaseRequest retry;
        memset(&retry, 0, sizeof(retry));
        retry.action = jazz_protocol__protocol__PermissionAdviceAction__clone(request->action);
        jazz_db__db__PolicyBinding__clone(&retry.session_claim_binding, &request->session_claim_binding);
        retry.has_delegated_session = request->has_delegated_session;
        if (retry.has_delegated_session) jazz_protocol__protocol__DelegatedSessionBinding__clone(&retry.delegated_session, &request->delegated_session);
        jz_btree_clone(&retry.waiters, &request->waiters, NULL, NULL);
        retry.intent_sent = false;
        jz_btree_init(&retry.applied_clauses, &jazz_db_scope_applied_clauses_desc);
        jazz_db_PendingUpstreamCommand *command = (jazz_db_PendingUpstreamCommand *)jz_vec_push_slot(&up->pending, sizeof(jazz_db_PendingUpstreamCommand));
        memset(command, 0, sizeof(*command));
        command->tag = JAZZ_PUC_AUTHORIZATION_SCOPE_INTENT;
        command->intent.request_id = retry_id;
        command->intent.action = jazz_protocol__protocol__PermissionAdviceAction__clone(request->action);
        command->intent.has_session_claim_binding = true;
        jazz_db__db__PolicyBinding__clone(&command->intent.session_claim_binding, &request->session_claim_binding);
        command->intent.has_delegated_session = request->has_delegated_session;
        if (request->has_delegated_session) jazz_protocol__protocol__DelegatedSessionBinding__clone(&command->intent.delegated_session, &request->delegated_session);
        jazz_db_AuthorizationScopeLeaseRequest gone;
        if (jz_btree_remove(requests, &request_id, NULL, &gone)) jazz_db__db__AuthorizationScopeLeaseRequest__drop(&gone);
        jz_btree_insert(requests, &retry_id, &retry, NULL);
        drop_peer_request(self->node);
        jazz_protocol__protocol__SyncMessage__drop(&message);
        return JZ_OK;
    }
    /* Transport admission retains its authority link, while this support
     * lease uses the current context under the immutable delegated subject
     * named by the request and receipt. */
    jazz_protocol_AuthorityContext scope_authority = *expected;
    scope_authority.link = request->session_claim_binding.identity;
    void *owner = request->owner;
    request->owner = NULL;
    bool admitted;
    if (request->lease && owner)
        admitted = jazz_protocol__authorization_scope__AuthorizationScopeRegistry__install(up->scope_lease_manager.registry, request->lease, owner, scope_authority,
                                                                                         jazz_protocol__protocol__AuthorizationScopeReceipt__clone(receipt_p));
    else if (request->lease)
        admitted = jazz_protocol__authorization_scope__AuthorizationScopeRegistry__receipt(up->scope_lease_manager.registry, request->lease, scope_authority,
                                                                                         receipt.authorization_progress, receipt.settled_through);
    else
        admitted = false;
    if (!admitted) { jazz_protocol__protocol__SyncMessage__drop(&message); return JZ_OK; }
    jazz_protocol_PermissionAdviceAction *action = jazz_protocol__protocol__PermissionAdviceAction__clone(request->action);
    jazz_db_PolicyBinding session_claim_binding;
    jazz_db__db__PolicyBinding__clone(&session_claim_binding, &request->session_claim_binding);
    jz_btree waiter_ids;
    jz_btree_clone(&waiter_ids, &request->waiters, NULL, NULL);
    {
        jazz_db_AuthorizationScopeLeaseRequest gone;
        if (jz_btree_remove(requests, &request_id, NULL, &gone)) jazz_db__db__AuthorizationScopeLeaseRequest__drop(&gone);
    }
    jazz_protocol_PermissionAdvice advice;
    {
        jazz_node_NodeState *owner_state = jazz_db_node_lock(self->node);
        jazz_node_NodeState *scoped = NS_FN(scoped_active_session_claims)(owner_state, session_claim_binding.identity, session_claim_binding.claims);
        advice = evaluate_authoritative_permission_advice(scoped, jazz_protocol__protocol__AuthorizationSupportScopeKey__subject(receipt.key), action);
        NS_FN(end_scoped_claims)(owner_state);
        jazz_db_node_unlock(self->node);
    }
    {
        jazz_protocol_PermissionAdviceRequestId *waiter_id;
        void *unit;
        JZ_BTREE_FOR(&waiter_ids, waiter_id, unit) {
            (void)unit;
            jazz_db_Oneshot *waiter = NULL;
            if (jz_btree_remove(self->permission_advice_waiters, waiter_id, NULL, &waiter) && waiter) {
                u8 a = (u8)advice;
                (void)jazz_db__oneshot__send(waiter, &a);
                jazz_db__oneshot__drop_sender(waiter);
            }
        }
    }
    jz_btree_drop(&waiter_ids);
    jazz_protocol__protocol__SyncMessage__drop(&message);
    *cont = false;
    return JZ_OK;
}

/// The fallthrough `message =>` arm on an upstream link. `*brk` = the
/// message was deferred for catalogue persistence (`break`).
static jz_err inbound_apply_other(TickCtx *t, jazz_db_UpstreamConnectionState *up, UpstreamInbound *in, jazz_protocol_SyncMessage message,
                                  jazz_db_BufferLease *lease, bool authority_receipt_eligible, bool receipts_validated, bool *brk) {
    PC *self = t->self;
    *brk = false;
    jazz_db_HeldAuthority admitted = *self->admitted_upstream_authority;
    bool outbox_release_receipt_eligible = upload_answer_receipt_eligible(authority_receipt_eligible, up->has_expected_scope_authority, up->expected_scope_authority,
                                                                          admitted.has, admitted.ctx);
    bool has_routed_fate = false;
    jazz_model_TxId routed_tx;
    jazz_protocol_SyncMessage routed_fate;
    if (message.tag == JAZZ_SM_FATE_UPDATE) {
        has_routed_fate = true;
        routed_tx = message.v.fate_update.tx_id;
        jazz_protocol__protocol__SyncMessage__clone(&routed_fate, &message);
    }
    bool has_released = message.tag == JAZZ_SM_FATE_UPDATE && outbox_release_receipt_eligible && fate_update_settles_upload(&message, message.v.fate_update.tx_id);
    jazz_model_TxId released_tx = has_released ? message.v.fate_update.tx_id : (jazz_model_TxId){0};
    jz_err e = inbound_flush_views(t, up, in);
    if (e.code) goto fail;
    {
        jazz_node_NodeState *ingress_owner = jazz_db_node_lock(self->node);
        if (jazz_node__node__is_catalogue_mutation(&message)) {
            bool deferred = false;
            e = NS_FN(defer_catalogue_for_persistence)(ingress_owner, tick_waker(t), &deferred);
            if (e.code) { jazz_db_node_unlock(self->node); e = node_err(e); goto fail; }
            if (deferred) {
                jazz_db_node_unlock(self->node);
                jazz_db_StagedInboundMessage staged = { message, lease ? jazz_db__wire__BufferLease__clone(lease) : NULL, authority_receipt_eligible, receipts_validated };
                jz_deque_push_front(&self->staged_inbound, &staged);
                if (has_routed_fate) jazz_protocol__protocol__SyncMessage__drop(&routed_fate);
                *brk = true;
                return JZ_OK;
            }
        }
        if (up->local_receiver && message.tag == JAZZ_SM_COMMIT_UNIT) {
            e = NS_FN(ingest_relay_commit_unit_with_encoder_trust)(ingress_owner, message.v.commit_unit.tx, message.v.commit_unit.versions, true);
        } else {
            jazz_node_PublicationOutcome outcome;
            e = NS_FN(apply_sync_message_with_ingest_context)(ingress_owner, message, NULL, &outcome);
            if (e.code == 0) outcome_take_publications(in, &outcome);
        }
        jazz_db_node_unlock(self->node);
        if (e.code) { e = node_err(e); if (has_routed_fate) jazz_protocol__protocol__SyncMessage__drop(&routed_fate); return e; }
    }
    if (has_routed_fate) {
        jazz_db__db__route_local_fate(self->local_fate_routes, routed_tx, &routed_fate);
        jazz_protocol__protocol__SyncMessage__drop(&routed_fate);
    }
    if (has_released) jz_vec_push(&self->released_outbox_tx_ids, &released_tx, sizeof(released_tx));
    return JZ_OK;
fail:
    jazz_protocol__protocol__SyncMessage__drop(&message);
    if (has_routed_fate) jazz_protocol__protocol__SyncMessage__drop(&routed_fate);
    return e;
}

/// The upstream inbound loop and its batch boundary.
static jz_err upstream_inbound(TickCtx *t, jazz_db_UpstreamConnectionState *up, bool *stop) {
    PC *self = t->self;
    UpstreamInbound in;
    memset(&in, 0, sizeof(in));
    jz_btree_init(&in.pending_initial_coverage_clears, &jazz_db_coverage_set_desc);
    jz_err err = JZ_OK;
    for (;;) {
        jazz_db_StagedInboundMessage next;
        if (!jz_deque_pop_front(&self->staged_inbound, &next)) {
            bool has = false;
            jazz_db_ReceivedSyncMessage received;
            jazz_db_TransportError error;
            jz_err e = jazz_db__db__node_runtime__Transport__try_recv_owned_result(&self->transport, &has, &received, &error);
            if (e.code) {
                jz_err_drop(&e);
                if (handle_transport_backpressure(self->node, self->scheduler, &error)) {
                    jazz_protocol__wire__TransportError__drop(&error);
                    in.deferred_stop = true;
                    break;
                }
                err = jazz_db__db__peer_connection__transport_error(error);
                goto out;
            }
            if (!has) {
                self->inbound_authority_receipt_quarantine = false;
                break;
            }
            next.message = received.message;
            next.lease = received.lease;
            next.authority_receipt_eligible = !self->inbound_authority_receipt_quarantine;
            next.receipts_validated = received.receipts_validated;
        }
        jazz_protocol_SyncMessage message = next.message;
        jazz_db_BufferLease *lease = next.lease;
        bool authority_receipt_eligible = next.authority_receipt_eligible;
        bool receipts_validated = next.receipts_validated;
        if (lease) {
            jazz_db_BufferLease *held = jazz_db__wire__BufferLease__clone(lease);
            jz_vec_push(&t->received_leases, &held, sizeof(held));
        }
        jazz_model_TxId write_state_tx_id;
        bool has_write_state_tx_id = write_state_update_tx_id(&message, &write_state_tx_id);
        bool cont = false, brk = false;
        switch (message.tag) {
        case JAZZ_SM_CHUNK_RESPONSE_BATCH:
            for (u32 i = 0; i < message.v.chunk_response_batch.responses.len; ++i)
                jazz_db__db__PeerChunkResolver__complete(self->auxiliary_pump.resolver, message.v.chunk_response_batch.responses.ptr[i]);
            message.v.chunk_response_batch.responses.len = 0;
            jazz_protocol__protocol__SyncMessage__drop(&message);
            cont = true;
            break;
        case JAZZ_SM_CHUNK_REQUEST_BATCH:
            drop_peer_request(self->node);
            jazz_protocol__protocol__SyncMessage__drop(&message);
            cont = true;
            break;
        case JAZZ_SM_CHUNK_UPLOAD_RESULT:
            inbound_chunk_upload_result(t, up, &message);
            jazz_protocol__protocol__SyncMessage__drop(&message);
            cont = true;
            break;
        case JAZZ_SM_CATALOGUE_SNAPSHOT: {
            err = inbound_flush_views(t, up, &in);
            if (err.code) { jazz_protocol__protocol__SyncMessage__drop(&message); goto out_lease; }
            jazz_node_NodeState *catalogue_owner = jazz_db_node_lock(self->node);
            bool deferred = false;
            jz_err e = NS_FN(defer_catalogue_for_persistence)(catalogue_owner, tick_waker(t), &deferred);
            if (e.code) { jazz_db_node_unlock(self->node); jazz_protocol__protocol__SyncMessage__drop(&message); err = node_err(e); goto out_lease; }
            if (deferred) {
                jazz_db_node_unlock(self->node);
                jazz_db_StagedInboundMessage staged = { message, lease ? jazz_db__wire__BufferLease__clone(lease) : NULL, authority_receipt_eligible, receipts_validated };
                jz_deque_push_front(&self->staged_inbound, &staged);
                brk = true;
                break;
            }
            jazz_node_PublicationOutcome outcome;
            e = NS_FN(apply_trusted_catalogue_snapshot)(catalogue_owner, (jazz_protocol_CatalogueSnapshot *)message.v.catalogue_snapshot, &outcome);
            jazz_db_node_unlock(self->node);
            if (e.code) { err = node_err(e); goto out_lease; }
            if (self->open_schema_admission->has) {
                jazz_types_SchemaVersionId requested = self->open_schema_admission->pending.schema;
                bool admitted;
                {
                    jazz_node_NodeState *n = jazz_db_node_lock(self->node);
                    admitted = jz_btree_contains(NS_FN(catalogue_schemas)(n), &requested);
                    jazz_db_node_unlock(self->node);
                }
                if (self->open_schema_admission->has) {
                    jazz_db_PendingOpenSchema *pending = &self->open_schema_admission->pending;
                    if (pending->has_connection_epoch && pending->connection_epoch == self->connection_epoch) pending->authoritative_catalogue_received = true;
                }
                jazz_db__db__finish_open_schema_connection(self->open_schema_admission, self->connection_epoch, admitted ? JZ_OK : jazz_db__db__pending_open_schema_error());
            }
            outcome_take_publications(&in, &outcome);
            break;
        }
        case JAZZ_SM_CURRENT_ROWS_RECEIPT: {
            err = inbound_flush_views(t, up, &in);
            if (err.code) { jazz_protocol__protocol__SyncMessage__drop(&message); goto out_lease; }
            jazz_db_HeldAuthority selected = *self->admitted_upstream_authority;
            err = jazz_db__db__row_availability__receive_current_rows(self->node, self->current_rows, selected.has ? &selected.ctx : NULL,
                                                                      up->has_expected_scope_authority ? &up->expected_scope_authority : NULL,
                                                                      authority_receipt_eligible, message.v.current_rows_receipt);
            if (err.code) goto out_lease;
            jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_IMMEDIATE);
            in.applied = true;
            cont = true;
            break;
        }
        case JAZZ_SM_VIEW_UPDATE:
            err = inbound_view_update(t, up, &in, message, authority_receipt_eligible, &cont);
            if (err.code) goto out_lease;
            break;
        case JAZZ_SM_SUBSCRIBE_REJECTED: {
            jazz_protocol_SubscriptionKey subscription = message.v.subscribe_rejected.subscription;
            if (!authority_receipt_eligible || (self->active_authority_view_receipts->has && self->active_authority_view_receipts->connection_epoch != self->connection_epoch)) {
                jazz_protocol__protocol__SyncMessage__drop(&message);
                cont = true;
                break;
            }
            jazz_db_PendingUpstreamSubscription old;
            if (jz_btree_remove(&up->sent_subscriptions, &subscription, NULL, &old)) jazz_db__db__PendingUpstreamSubscription__drop(&old);
            jz_btree_remove(&up->awaiting_support_snapshots, &subscription, NULL, NULL);
            u32 delivered = queue_relay_subscription_rejection(self->relay_upstream_subscription_owners, self->pending_relay_subscription_rejections,
                                                               up->upstream_subscriptions, subscription, &message.v.subscribe_rejected.reason);
            if (delivered > 0) {
                bump_subscriber_dirty_epoch(self);
                jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_IMMEDIATE);
            } else {
                t->stats.subscription_events += jazz_db__db__node_runtime__route_upstream_subscription_rejection(self->subscriptions, self->upstream_subscription_owners,
                                                                                                                 subscription, &message.v.subscribe_rejected.reason);
            }
            jazz_protocol__protocol__SyncMessage__drop(&message);
            break;
        }
        case JAZZ_SM_AUTHORIZATION_SCOPE_VIEW:
            err = inbound_scope_view(t, up, &in, message, authority_receipt_eligible, &cont);
            if (err.code) goto out_lease;
            break;
        case JAZZ_SM_AUTHORIZATION_SCOPE_AGGREGATE_RECEIPT:
            err = inbound_scope_receipt(t, up, &in, message, &cont);
            if (err.code) goto out_lease;
            break;
        case JAZZ_SM_AUTHORIZATION_SCOPE_UNAVAILABLE:
            lease_request_resolve(self, &up->scope_lease_manager.requests, &message.v.scope_unavailable.request_id, true, JAZZ_ADVICE_UNKNOWN);
            jazz_protocol__protocol__SyncMessage__drop(&message);
            break;
        case JAZZ_SM_AUTHORIZATION_SCOPE_DECISION:
            if (!up->has_expected_scope_authority) {
                drop_peer_request(self->node);
                jazz_protocol__protocol__SyncMessage__drop(&message);
                cont = true;
                break;
            }
            lease_request_resolve(self, &up->scope_lease_manager.requests, &message.v.scope_decision.request_id, true, message.v.scope_decision.advice);
            jazz_protocol__protocol__SyncMessage__drop(&message);
            break;
        default:
            err = inbound_apply_other(t, up, &in, message, lease, authority_receipt_eligible, receipts_validated, &brk);
            if (err.code) goto out_lease;
            break;
        }
        if (lease) jazz_db__wire__BufferLease__drop(lease);
        if (brk) break;
        if (cont) continue;
        if (has_write_state_tx_id)
            handle_write_state_update(self->node, self->write_state_waiters, self->mutation_errors, self->browser_relay_recovered_tx_ids, self->scheduler, write_state_tx_id);
        in.applied = true;
        continue;
    out_lease:
        if (lease) jazz_db__wire__BufferLease__drop(lease);
        goto out;
    }
    err = inbound_flush_views(t, up, &in);
    if (err.code) goto out;
    if (in.applied) {
        u32 n = in.publications.len;
        jz_err *persisted = (jz_err *)jz_zalloc(sizeof(jz_err) * (n ? n : 1));
        for (u32 i = 0; i < n; ++i) persisted[i] = jazz_node__node__PublishedTransaction__persist(in.publications.ptr[i]);
        jazz_node_NodeState *node = jazz_db_node_lock(self->node);
        for (u32 i = 0; i < n; ++i) {
            if (err.code) { jz_err_drop(&persisted[i]); continue; }
            jz_err e = NS_FN(settle_published_transaction)(node, jazz_node__node__PublishedTransaction__tx_id(in.publications.ptr[i]), persisted[i]);
            if (e.code) err = node_err(e);
        }
        jazz_db_node_unlock(self->node);
        jz_free(persisted);
        if (err.code) goto out;
        /* Durable application is complete at this boundary. A refresh
         * failure belongs to the resident subscriptions; returning it would
         * discard this tick's progress receipt and make the already-consumed
         * batch eligible for replay. */
        u32 changed = 0;
        jz_err refresh = jazz_db__db__node_runtime__refresh_subscriptions_in(self->node, self->subscriptions, self->active_authority_view_receipts, tick_waker(t), &changed);
        if (refresh.code) { changed = jazz_db__db__peer_connection__route_subscription_refresh_failure(self->subscriptions, &refresh); jz_err_drop(&refresh); }
        t->stats.subscription_events += changed;
        t->stats.remote_sync_applied += 1;
        bump_subscriber_dirty_epoch(self);
    }
    *stop = in.deferred_stop;
out:
    jz_vec_drop(&in.publications, sizeof(jazz_node_PublishedTransaction *), published_ptr_drop);
    for (u32 i = 0; i < in.pending_view_updates.len; ++i) jazz_node__node__ViewUpdateParts__drop(&in.pending_view_updates.ptr[i].parts);
    jz_vec_drop(&in.pending_view_updates, sizeof(jazz_db_PendingAuthorityViewUpdate), NULL);
    jz_btree_drop(&in.pending_initial_coverage_clears);
    return err;
}

/* ---------------------------------------------------------------- subscriber tick */

typedef struct SubscriberTick {
    bool applied_inbound;
    bool scheduled_follow_up;
    bool sent_view_update;
    bool needs_subscription_refresh;
} SubscriberTick;

/* Flow out of one subscriber inbound arm. */
enum { SUB_CONTINUE = 0, SUB_STOP = 1, SUB_BREAK = 2 };

/* flush_subscriber_controls_or_stop! */
static jz_err sub_flush_controls(PC *self, jazz_db_SubscriberConnectionState *sub, bool *ok) {
    return flush_pending_control_responses(self->node, sub->peer, &self->transport, &self->pending_control_responses, self->scheduler, ok);
}

/// Queue a control rejection, schedule its flush and flush now. Used where
/// the Rust arm runs inside its own boxed admission future, so
/// `flush_subscriber_controls_or_stop!` and the following `return Ok(true)`
/// both only `continue` the outer receive loop.
static jz_err sub_reject_now(PC *self, jazz_db_SubscriberConnectionState *sub, jazz_protocol_SyncMessage rejection) {
    queue_direct_control(&self->pending_control_responses, rejection);
    jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_IMMEDIATE);
    bool ok;
    return sub_flush_controls(self, sub, &ok);
}

static jazz_protocol_SyncMessage server_failure_message(jazz_protocol_SubscriptionKey subscription, const jz_err *error) {
    jazz_protocol_SyncMessage m;
    jazz_db__db__server_subscription_failure_rejection_message(&m, subscription, error);
    return m;
}

static jazz_protocol_SyncMessage unsupported_capability_message(jazz_protocol_SubscriptionKey subscription, jz_str detail) {
    jazz_protocol_SyncMessage m;
    jazz_db__db__unsupported_shape_capability_rejection_message(&m, subscription, detail);
    return m;
}

static bool sub_has_binding(const TickCtx *t) { return t->has_session_claim_binding; }

/// The subscriber link's catalogue/relay-rejection/flush prelude.
static jz_err subscriber_prelude(TickCtx *t, jazz_db_SubscriberConnectionState *sub, bool *stop) {
    PC *self = t->self;
    jazz_node_PeerState *peer = sub->peer;
    /* These links have a host-admitted app identity (or an existing trusted
     * catalogue capability). Send the same app-wide metadata that precedes a
     * ViewUpdate before query compilation: a replica reopening an unknown
     * published schema cannot register that query until the catalogue
     * arrives. A generic relay has no admitted application scope and
     * retains its request-driven delivery. */
    bool catalogue_ready;
    {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
        catalogue_ready = NS_FN(catalogue_bootstrap_state)(n) == JAZZ_CATALOGUE_BOOTSTRAP_READY;
        jazz_db_node_unlock(self->node);
    }
    bool owner_admitted = !self->open_schema_admission->has || self->open_schema_admission->pending.authoritative_catalogue_received;
    u8 trust = sub->ingest_context.trust;
    bool catalogue_entitled = jazz_node__node__CommitUnitTrust__is_trusted((jazz_node_CommitUnitTrust)trust)
        || (!PEER_FN(has_announced_catalogue_snapshot)(peer)
            && (trust == JAZZ_TRUST_SESSION || (trust == JAZZ_TRUST_RELAY && PEER_FN(admitted_scope_relay_binding)(peer) != NULL)));
    if (catalogue_ready && owner_admitted && catalogue_entitled) {
        jz_err e = send_catalogue_snapshot_if_needed(self->node, peer, &self->transport);
        if (e.code == JAZZ_EC_BACKPRESSURE) {
            jz_err_drop(&e);
            jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_DEFERRED);
            *stop = true;
            return JZ_OK;
        }
        if (e.code) return e;
    }
    jz_deque relay_rejections;
    if (!jz_btree_remove(self->pending_relay_subscription_rejections, &t->connection_epoch, NULL, &relay_rejections))
        jz_deque_init(&relay_rejections, sizeof(jazz_db_RelaySubscriptionRejection));
    jazz_db_RelaySubscriptionRejection rejection;
    while (jz_deque_pop_front(&relay_rejections, &rejection)) {
        /* Forward the authority's reason to every active usage site before
         * retiring the group. One coverage evaluator may have many
         * downstream wire subscriptions. The relay rejection already owns
         * this bounded recipient set, so move its responses to the semantic
         * control queue rather than asking the byte transport to retain
         * another logical message after its one admitted backlog. */
        const jazz_db_CoverageGroup *active = (const jazz_db_CoverageGroup *)jz_btree_get(&sub->coverage_groups, &rejection.coverage);
        if (active) {
            jazz_protocol_SubscriptionKey *subscription;
            void *unit;
            JZ_BTREE_FOR(&active->subscribers, subscription, unit) {
                (void)unit;
                if (!jz_btree_contains(&rejection.downstream_subscriptions, subscription)) continue;
                jazz_protocol_SyncMessage m;
                memset(&m, 0, sizeof(m));
                m.tag = JAZZ_SM_SUBSCRIBE_REJECTED;
                m.v.subscribe_rejected.subscription = *subscription;
                jazz_protocol__protocol__SubscribeRejectReason__clone(&m.v.subscribe_rejected.reason, &rejection.reason);
                queue_direct_control(&self->pending_control_responses, m);
            }
        }
        jazz_db_CoverageGroup group;
        if (jz_btree_remove(&sub->coverage_groups, &rejection.coverage, NULL, &group)) {
            jazz_protocol_SubscriptionKey group_subscription = jazz_db__db__peer_connection__coverage_group_subscription_key(&rejection.coverage);
            jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(self->node);
            /* `group_subscription` owns the one shared maintained evaluator.
             * The individual subscribers are still separately registered
             * wire usage sites, including the case where a peer deliberately
             * used noncanonical binding handles. Retire both layers: dropping
             * only the group leaves the individual registrations and
             * known-state declarations resident after rejection. The peer
             * facade may not have created publication state yet for an
             * opening group, but registration is already policy-scoped in the
             * node. Retire that exact group usage first; a bare wire
             * unsubscribe is intentionally ambiguous across reader scopes. */
            jz_btree claims;
            claims_clone(&claims, &rejection.policy_binding.claims);
            jazz_protocol_PolicyBindingKey key;
            jazz_protocol__protocol__PolicyBindingKey__from_canonical_parts(&key, rejection.policy_binding.identity, claims);
            NS_FN(apply_unsubscribe_with_admitted_policy_binding)(node, group_subscription, key);
            PEER_FN(forget_subscription_with_node)(peer, node, group_subscription);
            jazz_protocol_SubscriptionKey *subscription;
            void *unit;
            JZ_BTREE_FOR(&group.subscribers, subscription, unit) {
                (void)unit;
                claims_clone(&claims, &rejection.policy_binding.claims);
                jazz_protocol__protocol__PolicyBindingKey__from_canonical_parts(&key, rejection.policy_binding.identity, claims);
                NS_FN(apply_unsubscribe_with_admitted_policy_binding)(node, *subscription, key);
                if (!jazz_protocol_SubscriptionKey_eq(subscription, &group_subscription)) PEER_FN(forget_subscription_with_node)(peer, node, *subscription);
                jazz_db_CoverageKey served_coverage;
                if (jz_btree_remove(&sub->served, subscription, NULL, &served_coverage)) jazz_db__db__CoverageKey__drop(&served_coverage);
            }
            jazz_db_node_unlock(self->node);
            jazz_db__db__CoverageGroup__drop(&group);
        }
        jazz_db__db__RelaySubscriptionRejection__drop(&rejection);
    }
    jz_deque_drop(&relay_rejections, jazz_db__db__RelaySubscriptionRejection__drop);
    bool ok;
    JZ_TRY(flush_downstream_fates(self->node, peer, &self->transport, self->downstream_fates, &sub->read_barrier, self->scheduler, &ok));
    if (!ok) { *stop = true; return JZ_OK; }
    JZ_TRY(flush_pending_chunk_response(&self->transport, &self->pending_chunk_response, self->scheduler, &ok));
    if (!ok) { *stop = true; return JZ_OK; }
    JZ_TRY(sub_flush_controls(self, sub, &ok));
    if (!ok) { *stop = true; return JZ_OK; }
    jazz_protocol_SyncMessage message;
    if (jazz_db__db__PeerIoPump__take_outbound(&self->auxiliary_pump, 64, &message)) {
        jazz_protocol_SyncMessage copy;
        jazz_protocol__protocol__SyncMessage__clone(&copy, &message);
        jazz_db_TransportError error;
        jz_err e = jazz_db__db__node_runtime__Transport__send(&self->transport, copy, &error);
        if (e.code) {
            jz_err_drop(&e);
            jazz_db__db__PeerIoPump__restore_outbound(&self->auxiliary_pump, message);
            if (handle_transport_backpressure(self->node, self->scheduler, &error)) {
                jazz_protocol__wire__TransportError__drop(&error);
                *stop = true;
                return JZ_OK;
            }
            return jazz_db__db__peer_connection__transport_error(error);
        }
        jazz_db__db__PeerIoPump__acknowledge_outbound(&self->auxiliary_pump, &message);
        jazz_protocol__protocol__SyncMessage__drop(&message);
    }
    return JZ_OK;
}

static jz_err sub_chunk_request_batch(TickCtx *t, jazz_protocol_SyncMessage message, int *flow) {
    PC *self = t->self;
    jazz_protocol_ChunkResponseEntryVec responses = {0};
    *flow = SUB_CONTINUE;
    for (u32 i = 0; i < message.v.chunk_request_batch.requests.len; ++i) {
        jazz_protocol_ChunkRequest *request = (jazz_protocol_ChunkRequest *)message.v.chunk_request_batch.requests.ptr[i];
        if (jazz_db__db__PeerIoPump__is_disconnected(&self->auxiliary_pump)) break;
        jz_bytes bytes = {0};
        jz_err local;
        {
            jazz_node_NodeState *n = jazz_db_node_lock(self->node);
            local = NS_FN(local_chunk)(n, request->locator, request->expected_hash, &bytes);
            jazz_db_node_unlock(self->node);
        }
        if (jazz_db__db__PeerIoPump__is_disconnected(&self->auxiliary_pump)) { jz_err_drop(&local); jz_bytes_drop(&bytes); break; }
        if (local.code == 0) {
            jazz_protocol_ChunkResponseEntry entry = { request->request_id, JAZZ_CHUNK_RESPONSE_FOUND, bytes };
            jz_vec_push(&responses, &entry, sizeof(entry));
        } else if (local.code == GROOVE_CHUNK_STORAGE_UNAVAILABLE) {
            jz_err_drop(&local);
            jazz_db__db__PeerChunkResolver__enqueue_relay(self->auxiliary_pump.resolver, self->connection_epoch, request);
            message.v.chunk_request_batch.requests.ptr[i] = NULL;
        } else {
            jz_err_drop(&local);
            jazz_protocol_ChunkResponseEntry entry = { request->request_id, JAZZ_CHUNK_RESPONSE_UNAVAILABLE, {0} };
            jz_vec_push(&responses, &entry, sizeof(entry));
        }
    }
    jazz_protocol__protocol__SyncMessage__drop(&message);
    if (responses.len != 0 && !jazz_db__db__PeerIoPump__is_disconnected(&self->auxiliary_pump)) {
        JZ_ASSERT(self->pending_chunk_response == NULL, "subscriber drains a retained chunk response before reading another request");
        self->pending_chunk_response = jazz_protocol__protocol__ChunkResponseBatch__new(responses);
        jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_IMMEDIATE);
        *flow = SUB_STOP;
        return JZ_OK;
    }
    jz_vec_drop(&responses, sizeof(jazz_protocol_ChunkResponseEntry), NULL);
    return JZ_OK;
}

static jz_err sub_current_rows_request(TickCtx *t, jazz_db_SubscriberConnectionState *sub, jazz_protocol_SyncMessage message, int *flow) {
    PC *self = t->self;
    jazz_protocol_CurrentRowsRequest *request = (jazz_protocol_CurrentRowsRequest *)message.v.current_rows_request;
    jz_err err = JZ_OK;
    *flow = SUB_CONTINUE;
    jazz_db_ConnectionSessionContext session;
    if (!jazz_db__db__node_runtime__Transport__connection_session_context(&self->transport, &session)) { drop_peer_request(self->node); goto out; }
    if (!jazz_db__row_availability__valid_request(request)) { drop_peer_request(self->node); goto out; }
    jazz_db_PolicyBinding binding;
    if (!jazz_db__db__admitted_request_policy_binding(sub->ingest_context, sub->peer, sub_has_binding(t) ? &t->session_claim_binding : NULL,
                                                      request->has_delegated_session ? &request->delegated_session : NULL, &binding)) {
        drop_peer_request(self->node);
        goto out;
    }
    jz_btree context_claims;
    claims_clone(&context_claims, &binding.claims);
    jazz_protocol_PolicyBindingKey context;
    jazz_protocol__protocol__PolicyBindingKey__from_canonical_parts(&context, binding.identity, context_claims);
    bool can_mint;
    {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
        can_mint = NS_FN(can_mint_current_row_receipts)(n);
        jazz_db_node_unlock(self->node);
    }
    if (can_mint) {
        u64 *progress = jazz_db__row_availability__CurrentRowsRouter__progress_entry(self->current_rows, t->connection_epoch);
        if (*progress == UINT64_MAX) jz_panic("current row progress exhausted");
        *progress += 1;
        u64 current = *progress;
        u64 epoch = session.local.epoch;
        jazz_protocol_CurrentRowsReceipt *receipt = NULL;
        jazz_node_NodeState *owner = jazz_db_node_lock(self->node);
        jazz_node_NodeState *scoped = NS_FN(scoped_active_session_claims)(owner, binding.identity, binding.claims);
        jz_err e = NS_FN(evaluate_current_rows)(scoped, request, context, epoch, current, &receipt);
        NS_FN(end_scoped_claims)(owner);
        jazz_db_node_unlock(self->node);
        if (e.code) { err = node_err(e); goto out; }
        jazz_protocol_SyncMessage m;
        memset(&m, 0, sizeof(m));
        m.tag = JAZZ_SM_CURRENT_ROWS_RECEIPT;
        m.v.current_rows_receipt = receipt;
        jazz_db__db__peer_connection__queue_sync_context_control(&self->pending_control_responses, m);
    } else {
        jazz_protocol_PermissionAdviceRequestId upstream_id;
        jz_uuid fresh = jz_uuid_v4();
        memcpy(upstream_id.b, &fresh, 16);
        jazz_db_CurrentRowsRoute route;
        memset(&route, 0, sizeof(route));
        jazz_protocol__protocol__CurrentRowsRequest__clone(&route.request, request);
        route.request.request_id = upstream_id;
        if (route.request.has_delegated_session) {
            jazz_protocol__protocol__DelegatedSessionBinding__drop(&route.request.delegated_session);
            route.request.has_delegated_session = false;
        }
        jazz_protocol__protocol__PolicyBindingKey__clone(&route.context, &context);
        route.has_upstream = false;
        route.has_downstream = true;
        route.downstream_epoch = t->connection_epoch;
        route.downstream_request_id = request->request_id;
        route.sender = NULL;
        if (!self->admitted_upstream_authority->has || !jazz_db__row_availability__CurrentRowsRouter__admit(self->current_rows, route)) {
            jazz_protocol_SyncMessage m;
            memset(&m, 0, sizeof(m));
            m.tag = JAZZ_SM_CURRENT_ROWS_RECEIPT;
            m.v.current_rows_receipt = jazz_db__row_availability__unknown_receipt(request, context);
            queue_direct_control(&self->pending_control_responses, m);
        } else {
            jazz_protocol__protocol__PolicyBindingKey__drop(&context);
        }
        jazz_db__db__PolicyBinding__drop(&binding);
    }
    jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_IMMEDIATE);
    {
        bool ok;
        err = sub_flush_controls(self, sub, &ok);
        if (!err.code && !ok) *flow = SUB_STOP;
    }
out:
    jazz_protocol__protocol__SyncMessage__drop(&message);
    return err;
}

static jz_err sub_scope_intent(TickCtx *t, jazz_db_SubscriberConnectionState *sub, jazz_protocol_SyncMessage message, int *flow) {
    PC *self = t->self;
    *flow = SUB_CONTINUE;
    jazz_db_ConnectionSessionContext context;
    bool admitted = jazz_db__db__node_runtime__Transport__connection_session_context(&self->transport, &context)
                    && (context.negotiated_features & JAZZ_FEATURE_AUTHORIZATION_SCOPE_VIEWS) != 0;
    if (!admitted) { drop_peer_request(self->node); jazz_protocol__protocol__SyncMessage__drop(&message); return JZ_OK; }
    jazz_db_PolicyBinding scope;
    if (!jazz_db__db__admitted_request_policy_binding(sub->ingest_context, sub->peer, sub_has_binding(t) ? &t->session_claim_binding : NULL,
                                                      message.v.scope_intent.has_delegated_session ? &message.v.scope_intent.delegated_session : NULL, &scope)) {
        drop_peer_request(self->node);
        jazz_protocol__protocol__SyncMessage__drop(&message);
        return JZ_OK;
    }
    jazz_protocol_PermissionAdviceAction *action = message.v.scope_intent.action;
    message.v.scope_intent.action = NULL;
    jazz_protocol_PermissionAdviceRequestId request_id = message.v.scope_intent.request_id;
    jazz_protocol__protocol__SyncMessage__drop(&message);
    JZ_TRY(serve_authorization_scope_intent(self->node, sub->peer, &self->pending_control_responses, scope.identity, scope.claims, t->connection_epoch,
                                            request_id, action, sub->ingest_context.trust, &sub->authority_scope_hydrations, &sub->authority_scope_hydration_count));
    if (self->pending_control_responses.len != 0) {
        jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_IMMEDIATE);
        bool ok;
        JZ_TRY(sub_flush_controls(self, sub, &ok));
        if (!ok) *flow = SUB_STOP;
    }
    return JZ_OK;
}

static void shape_registration_insert(jz_btree *registrations, jazz_db_ShapeRegistrationKey key, jazz_db_SubscriberShapeRegistration registration) {
    jazz_db_SubscriberShapeRegistration old;
    if (jz_btree_insert(registrations, &key, &registration, &old)) {
        if (old.tag == JAZZ_SSR_REJECTED_UNSUPPORTED_CAPABILITY) jz_str_drop(&old.detail);
        else jazz_protocol__protocol__RegisterShapeOptions__drop(&old.opts);
    }
}

/// SyncMessage::RegisterShape from a subscriber. Every early return of the
/// boxed Rust admission future continues the receive loop.
static jz_err sub_register_shape(TickCtx *t, jazz_db_SubscriberConnectionState *sub, jazz_protocol_SyncMessage message) {
    PC *self = t->self;
    jazz_model_ShapeId shape_id = message.v.register_shape.shape_id;
    jazz_protocol_ShapeAst *ast = message.v.register_shape.ast;
    jazz_protocol_RegisterShapeOptions opts = message.v.register_shape.opts;
    jz_err err = JZ_OK;
    jazz_model_ValidatedQuery *shape = NULL;
    const char *size_error = NULL;
    if (!jazz_db__db__validate_shape_registration_size(ast, &opts, &size_error)) {
        /* No stable subscription key exists before the read-view key is
         * derived. Fail the peer link rather than inventing unnegotiated wire
         * semantics or hashing attacker-sized options. */
        err = protocol_err(size_error);
        goto out;
    }
    jazz_protocol_ReadViewKey read_view_key = jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&opts);
    jazz_db_ShapeRegistrationKey registration_key = { shape_id, read_view_key };
    if (!jz_btree_contains(&sub->shape_registrations, &registration_key) && sub->shape_registrations.len >= JAZZ_MAX_SHAPE_REGISTRATIONS_PER_PEER) {
        jz_err error = jz_err_new(JAZZ_NODE_ERR_UNSUPPORTED_SYNC_MESSAGE, "peer shape registration limit exceeded");
        jazz_protocol_SyncMessage rejection = server_failure_message(jazz_db__db__register_shape_rejection_subscription(shape_id, read_view_key), &error);
        jz_err_drop(&error);
        err = sub_reject_now(self, sub, rejection);
        goto out;
    }
    jazz_node_PeerRole role = PEER_FN(role)(sub->peer);
    {
        jz_err error = jazz_db__db__ensure_supported_register_shape_options(&opts, sub->local_receiver, role,
                                                                            jazz_db__db__delegated_session_capability(sub->ingest_context, role));
        if (error.code) {
            jz_str detail = error.msg ? jz_str_clone(error.msg) : jz_str_from("");
            jazz_db_SubscriberShapeRegistration rejected;
            memset(&rejected, 0, sizeof(rejected));
            rejected.tag = JAZZ_SSR_REJECTED_UNSUPPORTED_CAPABILITY;
            rejected.detail = jz_str_clone(&detail);
            shape_registration_insert(&sub->shape_registrations, registration_key, rejected);
            jz_err_drop(&error);
            err = sub_reject_now(self, sub, unsupported_capability_message(jazz_db__db__register_shape_rejection_subscription(shape_id, read_view_key), detail));
            goto out;
        }
    }
    {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
        jz_err e = jazz_db__db__validate_shape_ast_for_registration(n, shape_id, ast, &shape);
        jazz_db_node_unlock(self->node);
        if (e.code) {
            if (jazz_db__db__is_server_shape_validation_failure(&e)) {
                jazz_protocol_SyncMessage rejection = server_failure_message(jazz_db__db__register_shape_rejection_subscription(shape_id, read_view_key), &e);
                jz_err_drop(&e);
                err = sub_reject_now(self, sub, rejection);
            } else {
                jz_err_drop(&e);
                drop_peer_request(self->node);
            }
            goto out;
        }
    }
    jazz_types_AuthorSubject permission_subject;
    if (shape && jazz_model__query__ValidatedQuery__params_is_empty(shape) && jazz_db__db__subscriber_permission_subject(sub->ingest_context, &permission_subject)) {
        /* Branch compilation may install an empty process-local sparse
         * source. Defer that side-effecting preflight until Subscribe, where
         * the authenticated branch gate is available. */
        jz_btree empty;
        jz_btree_init(&empty, &jazz_db_str_value_desc);
        jazz_model_Binding *binding = NULL;
        jz_err e = jazz_model__query__ValidatedQuery__bind(shape, empty, &binding);
        if (e.code) { jz_err_drop(&e); drop_peer_request(self->node); goto out; }
        if (!sub_has_binding(t)) jz_panic("subscriber claims");
        jz_btree claims;
        claims_clone(&claims, &t->session_claim_binding.claims);
        jazz_node_NodeState *owner = jazz_db_node_lock(self->node);
        jazz_node_NodeState *scoped = NS_FN(scoped_active_session_claims)(owner, t->session_claim_binding.identity, claims);
        u8 mode = NS_FN(peer_query_authorization_mode)(scoped);
        jz_err supported = NS_FN(ensure_peer_maintained_subscription_view_supported)(scoped, shape, binding, opts.tier, permission_subject, opts.read_view, mode);
        NS_FN(end_scoped_claims)(owner);
        jazz_db_node_unlock(self->node);
        jazz_protocol_SubscriptionKey subscription = { shape_id, jazz_model__query__Binding__binding_id(binding), read_view_key };
        jazz_model__query__Binding__drop(binding);
        if (supported.code == JAZZ_NODE_ERR_QUERY_CAPABILITY) {
            jz_str detail = supported.msg ? jz_str_clone(supported.msg) : jz_str_from("");
            jazz_db_SubscriberShapeRegistration rejected;
            memset(&rejected, 0, sizeof(rejected));
            rejected.tag = JAZZ_SSR_REJECTED_UNSUPPORTED_CAPABILITY;
            rejected.detail = jz_str_clone(&detail);
            shape_registration_insert(&sub->shape_registrations, registration_key, rejected);
            jz_err_drop(&supported);
            err = sub_reject_now(self, sub, unsupported_capability_message(subscription, detail));
            goto out;
        } else if (supported.code) {
            jazz_protocol_SyncMessage rejection = server_failure_message(subscription, &supported);
            jz_err_drop(&supported);
            err = sub_reject_now(self, sub, rejection);
            goto out;
        }
    }
    bool awaiting_catalogue_admission = shape == NULL;
    const jazz_db_SubscriberShapeRegistration *existing = (const jazz_db_SubscriberShapeRegistration *)jz_btree_get(&sub->shape_registrations, &registration_key);
    if (existing) {
        if (existing->tag != JAZZ_SSR_REJECTED_UNSUPPORTED_CAPABILITY && !jazz_protocol__protocol__RegisterShapeOptions__eq(&existing->opts, &opts)) {
            drop_peer_request(self->node);
            goto out;
        }
        if (existing->tag == JAZZ_SSR_REJECTED_UNSUPPORTED_CAPABILITY) {
            err = sub_reject_now(self, sub, unsupported_capability_message(jazz_db__db__register_shape_rejection_subscription(shape_id, read_view_key), jz_str_clone(&existing->detail)));
            goto out;
        }
    }
    jazz_protocol_SubscriptionKey rejection_subscription = jazz_db__db__register_shape_rejection_subscription(shape_id, registration_key.read_view);
    {
        jazz_protocol_RegisterShapeOptions registered_opts;
        jazz_protocol__protocol__RegisterShapeOptions__clone(&registered_opts, &opts);
        jazz_node_NodeState *n = jazz_db_node_lock(self->node);
        jz_err e = NS_FN(register_shape_for_peer_with_options)(n, t->connection_epoch, shape_id, ast, registered_opts);
        jazz_db_node_unlock(self->node);
        ast = NULL;
        if (e.code) {
            jazz_protocol_SyncMessage rejection = server_failure_message(rejection_subscription, &e);
            jz_err_drop(&e);
            err = sub_reject_now(self, sub, rejection);
            goto out;
        }
    }
    {
        jazz_db_SubscriberShapeRegistration registration;
        memset(&registration, 0, sizeof(registration));
        registration.tag = awaiting_catalogue_admission ? JAZZ_SSR_PENDING_CATALOGUE_ADMISSION : JAZZ_SSR_REGISTERED;
        registration.opts = opts;
        memset(&opts, 0, sizeof(opts));
        shape_registration_insert(&sub->shape_registrations, registration_key, registration);
    }
out:
    if (shape) jazz_model__query__ValidatedQuery__drop(shape);
    if (ast) jazz_protocol__protocol__ShapeAst__drop(ast);
    jazz_protocol__protocol__RegisterShapeOptions__drop(&opts);
    return err;
}

static jazz_protocol_PolicyBindingKey policy_binding_key_of(const jazz_db_PolicyBinding *binding) {
    jz_btree claims;
    claims_clone(&claims, &binding->claims);
    jazz_protocol_PolicyBindingKey key;
    jazz_protocol__protocol__PolicyBindingKey__from_canonical_parts(&key, binding->identity, claims);
    return key;
}

/// SyncMessage::Subscribe from a subscriber (the boxed admission future).
/// `parked` is the policy binding of a request parked for its catalogue.
static jz_err sub_subscribe(TickCtx *t, jazz_db_SubscriberConnectionState *sub, SubscriberTick *st, jazz_protocol_SyncMessage message,
                            bool has_parked, jazz_db_PolicyBinding *parked) {
    PC *self = t->self;
    jazz_node_PeerState *peer = sub->peer;
    jazz_protocol_Subscribe subscribe = message.v.subscribe;
    jz_err err = JZ_OK;
    bool subscription_has_delegated_session = subscribe.has_delegated_session;
    bool has_binding;
    jazz_db_PolicyBinding session_claim_binding;
    jazz_model_ValidatedQuery *shape = NULL;
    jazz_model_Binding *binding = NULL;
    jazz_db_CoverageKey coverage;
    bool has_coverage = false;
    jazz_protocol_RegisterShapeOptions opts;
    bool has_opts = false;
    if (has_parked) {
        session_claim_binding = *parked;
        has_binding = true;
    } else {
        has_binding = jazz_db__db__admitted_request_policy_binding(sub->ingest_context, peer, sub_has_binding(t) ? &t->session_claim_binding : NULL,
                                                                   subscribe.has_delegated_session ? &subscribe.delegated_session : NULL, &session_claim_binding);
    }
    if (!has_binding) {
        /* A relay's transport is not a user. It may only carry the
         * topology-assigned immutable session snapshot for this request. */
        drop_peer_request(self->node);
        jazz_protocol__protocol__SyncMessage__drop(&message);
        return JZ_OK;
    }
    jazz_model_ShapeId shape_id = subscribe.shape_id;
    jazz_protocol_SubscriptionKey subscription = subscribe.subscription;
    if (memcmp(&shape_id, &subscription.shape_id, sizeof(shape_id)) != 0) { drop_peer_request(self->node); goto out; }
    /* An incoming repeat can race catalogue activation. Validate it against
     * the parked immutable request even if the shape is now ready, then
     * consume that one pending owner before normal admission. */
    {
        const jazz_db_PendingCatalogueSubscription *existing = (const jazz_db_PendingCatalogueSubscription *)jz_btree_get(&sub->pending_catalogue_subscriptions, &subscription);
        if (existing) {
            if (!jazz_protocol__protocol__Subscribe__eq(&existing->subscribe, &subscribe) || !jazz_db__db__PolicyBinding__eq(&existing->policy_binding, &session_claim_binding)) {
                err = protocol_err("conflicting pending catalogue subscription");
                goto out;
            }
            jz_btree_remove(&sub->pending_catalogue_subscriptions, &subscription, NULL, NULL);
        }
    }
    jazz_db_ShapeRegistrationKey registration_key = { shape_id, subscription.read_view };
    const jazz_db_SubscriberShapeRegistration *registration = (const jazz_db_SubscriberShapeRegistration *)jz_btree_get(&sub->shape_registrations, &registration_key);
    if (!registration) { drop_peer_request(self->node); goto out; }
    bool pending_catalogue_admission = registration->tag == JAZZ_SSR_PENDING_CATALOGUE_ADMISSION;
    if (registration->tag == JAZZ_SSR_REJECTED_UNSUPPORTED_CAPABILITY) {
        /* Keep the original permanent rejection, but let views already served
         * by this connection flush first. A rejected shape must not starve
         * unrelated subscriptions. */
        queue_direct_control(&sub->deferred_subscribe_rejections, unsupported_capability_message(subscription, jz_str_clone(&registration->detail)));
        goto out;
    }
    jazz_protocol__protocol__RegisterShapeOptions__clone(&opts, &registration->opts);
    has_opts = true;
    {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
        shape = NS_FN(registered_shape)(n, shape_id);
        jazz_db_node_unlock(self->node);
    }
    if (!shape) {
        if (pending_catalogue_admission) {
            u32 encoded_bytes = 0;
            JZ_TRY(jazz_db__db__pending_catalogue_subscription_size(&subscribe, &session_claim_binding, &encoded_bytes));
            u64 retained_bytes = 0;
            jazz_protocol_SubscriptionKey *k;
            jazz_db_PendingCatalogueSubscription *v;
            JZ_BTREE_FOR(&sub->pending_catalogue_subscriptions, k, v) { (void)k; retained_bytes += v->encoded_bytes; }
            if (sub->pending_catalogue_subscriptions.len >= JAZZ_MAX_SHAPE_REGISTRATIONS_PER_PEER
                || retained_bytes + encoded_bytes > (u64)JAZZ_MAX_SHAPE_REGISTRATION_BYTES * JAZZ_MAX_SHAPE_REGISTRATIONS_PER_PEER) {
                err = protocol_err("pending catalogue subscription limit exceeded");
                goto out;
            }
            jazz_db_PendingCatalogueSubscription pending = { subscribe, session_claim_binding, encoded_bytes };
            jazz_db_PendingCatalogueSubscription old;
            if (jz_btree_insert(&sub->pending_catalogue_subscriptions, &subscription, &pending, &old)) jazz_db__db__peer_connection__PendingCatalogueSubscription__drop(&old);
            jazz_protocol__protocol__RegisterShapeOptions__drop(&opts);
            return JZ_OK;
        }
        drop_peer_request(self->node);
        goto out;
    }
    jazz_model_ValueVec values = subscribe.values;
    if (values.len != jazz_model__query__ValidatedQuery__params_len(shape)) { drop_peer_request(self->node); goto out; }
    {
        jz_btree value_map;
        jz_btree_init(&value_map, &jazz_db_str_value_desc);
        u32 i = 0;
        jz_str *name;
        void *param;
        JZ_BTREE_FOR(jazz_model__query__ValidatedQuery__params(shape), name, param) {
            (void)param;
            jz_str key = jz_str_clone(name);
            groove_Value value;
            groove__records__Value__clone(&value, &((groove_Value *)values.ptr)[i++]);
            jz_btree_insert(&value_map, &key, &value, NULL);
        }
        jz_err e = jazz_model__query__ValidatedQuery__bind(shape, value_map, &binding);
        if (e.code) { jz_err_drop(&e); drop_peer_request(self->node); goto out; }
    }
    jazz_node_PeerRole role = PEER_FN(role)(peer);
    {
        jz_err e = jazz_db__db__ensure_supported_register_shape_options(&opts, sub->local_receiver, role, jazz_db__db__delegated_session_capability(sub->ingest_context, role));
        if (e.code) { jz_err_drop(&e); drop_peer_request(self->node); goto out; }
    }
    jazz_db_PolicyBinding subscription_policy_binding;
    jazz_db__db__PolicyBinding__clone(&subscription_policy_binding, &session_claim_binding);
    jazz_protocol_RegisterShapeOptions coverage_opts;
    jazz_protocol__protocol__RegisterShapeOptions__clone(&coverage_opts, &opts);
    jazz_db__db__coverage_key(&coverage, shape, binding, coverage_opts);
    has_coverage = true;
    /* A coverage group is an authority evaluation namespace, not merely a
     * transport optimization. Every admitted reader gets its own immutable
     * policy scope, including a direct subscriber on a single authenticated
     * connection. Otherwise two direct readers with the same public query
     * can reuse one maintained authority result. */
    if (coverage.has_policy_binding) jazz_protocol__protocol__PolicyBindingKey__drop(&coverage.policy_binding);
    coverage.has_policy_binding = true;
    coverage.policy_binding = policy_binding_key_of(&subscription_policy_binding);
    {
        const jazz_db_CoverageKey *existing_coverage = (const jazz_db_CoverageKey *)jz_btree_get(&sub->served, &subscription);
        if (existing_coverage && !jazz_db__db__CoverageKey__eq(existing_coverage, &coverage)) {
            jazz_db__db__PolicyBinding__drop(&subscription_policy_binding);
            drop_peer_request(self->node);
            goto out;
        }
    }
    if (pending_catalogue_admission) {
        jazz_db_SubscriberShapeRegistration registered;
        memset(&registered, 0, sizeof(registered));
        registered.tag = JAZZ_SSR_REGISTERED;
        jazz_protocol__protocol__RegisterShapeOptions__clone(&registered.opts, &opts);
        shape_registration_insert(&sub->shape_registrations, registration_key, registered);
    }
    {
        jz_btree claims;
        claims_clone(&claims, &session_claim_binding.claims);
        jazz_node_NodeState *owner = jazz_db_node_lock(self->node);
        jazz_node_NodeState *scoped = NS_FN(scoped_active_session_claims)(owner, session_claim_binding.identity, claims);
        u8 mode = NS_FN(peer_query_authorization_mode)(scoped);
        jz_err supported = NS_FN(ensure_peer_maintained_subscription_view_supported)(scoped, shape, binding, opts.tier, subscription_policy_binding.identity, opts.read_view, mode);
        NS_FN(end_scoped_claims)(owner);
        jazz_db_node_unlock(self->node);
        if (supported.code) {
            jazz_protocol_SyncMessage rejection;
            if (supported.code == JAZZ_NODE_ERR_QUERY_CAPABILITY)
                rejection = unsupported_capability_message(subscription, supported.msg ? jz_str_clone(supported.msg) : jz_str_from(""));
            else
                rejection = server_failure_message(subscription, &supported);
            jz_err_drop(&supported);
            jazz_db__db__PolicyBinding__drop(&subscription_policy_binding);
            err = sub_reject_now(self, sub, rejection);
            goto out;
        }
    }
    jazz_protocol_SubscriptionKey group_subscription = jazz_db__db__peer_connection__coverage_group_subscription_key(&coverage);
    bool local_subscriber = sub->local_receiver;
    bool scope_relay;
    {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
        scope_relay = NS_FN(client_relay_scope)(n) != NULL;
        jazz_db_node_unlock(self->node);
    }
    jazz_protocol_RegisterShapeOptions upstream_opts;
    if (local_subscriber || scope_relay) {
        jazz_protocol_ReadViewSpec *view = NULL;
        jazz_protocol__protocol__ReadViewSpec__clone_into(&view, opts.read_view);
        jazz_db__db__upstream_register_shape_options(&upstream_opts, opts.tier, view, JAZZ_DUR_GLOBAL);
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
        if (NS_FN(client_relay_scope)(n) != NULL) upstream_opts.binding_source = JAZZ_BINDING_SOURCE_RELAY_AUTHORITY_SESSION;
        jazz_db_node_unlock(self->node);
    } else {
        /* The incoming relay capability belongs to this hop. An authority
         * forwards the admitted policy snapshot on its own trusted link,
         * without claiming a client relay capability on that separate
         * connection. */
        jazz_protocol__protocol__RegisterShapeOptions__clone(&upstream_opts, &opts);
        upstream_opts.binding_source = JAZZ_BINDING_SOURCE_DIRECT;
    }
    /* A scope relay has one authority stream per exact admitted coverage.
     * Foreground connections pin it independently; their disconnects do not
     * each own a duplicate predecessor sequence. */
    bool has_shared_upstream = false;
    jazz_protocol_SubscriptionKey upstream_subscription;
    if (scope_relay) {
        jazz_db_CoverageKey wanted;
        relay_authority_coverage_key(&coverage, &wanted);
        jazz_db_RelayOwnerKey *key;
        jazz_db_RelayUpstreamSubscriptionOwner *owner;
        JZ_BTREE_FOR(self->relay_upstream_subscription_owners, key, owner) {
            jazz_db_CoverageKey candidate;
            relay_authority_coverage_key(&owner->coverage, &candidate);
            bool same = jazz_db__db__CoverageKey__eq(&candidate, &wanted) && jazz_db__db__PolicyBinding__eq(&owner->policy_binding, &subscription_policy_binding);
            jazz_db__db__CoverageKey__drop(&candidate);
            if (same) { upstream_subscription = key->subscription; has_shared_upstream = true; break; }
        }
        jazz_db__db__CoverageKey__drop(&wanted);
    }
    if (!has_shared_upstream)
        upstream_subscription = relay_upstream_subscription_key(t->connection_epoch, subscription, jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&upstream_opts),
                                                                &subscription_policy_binding);
    /* This is a topology-role distinction, not a transport-direction
     * distinction. The browser worker receives its foreground subscriber on
     * a `local_receiver = false` link, yet it is still a non-authoritative
     * scope relay: only its separately registered upstream usage can receive
     * a live authority result. A serving authority, by contrast, evaluates
     * the incoming downstream usage itself and therefore owns D. */
    bool propagates_to_selected_authority = local_subscriber || scope_relay;
    bool waits_for_selected_authority = propagates_to_selected_authority && opts.tier > JAZZ_DUR_LOCAL;
    /* The concrete wire subscription is only a publication handle. The
     * shared server-side evaluator belongs to the coverage group, whose
     * opaque handle incorporates the admitted policy scope. Registering the
     * public handle here would let two direct readers with an identical
     * query overwrite each other's NodeState usage entry. */
    jazz_protocol_SubscriptionKey authority_result_subscription = propagates_to_selected_authority ? upstream_subscription : group_subscription;
    bool first_subscriber;
    {
        const jazz_db_CoverageGroup *group = (const jazz_db_CoverageGroup *)jz_btree_get(&sub->coverage_groups, &coverage);
        first_subscriber = !group || group->subscribers.len == 0;
    }
    bool permissions_ready;
    jazz_types_GlobalTime committed;
    {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
        permissions_ready = jazz_db__db__subscriber_permissions_ready(NS_FN(permissions_ready)(n), sub->ingest_context.trust);
        committed = NS_FN(committed_global_time)(n);
        jazz_db_node_unlock(self->node);
    }
    bool has_opening_pending = !permissions_ready;
    jazz_protocol_SyncMessage opening_pending;
    if (has_opening_pending) {
        memset(&opening_pending, 0, sizeof(opening_pending));
        opening_pending.tag = JAZZ_SM_VIEW_UPDATE;
        jazz_protocol_PeerPayloadInventory inventory;
        memset(&inventory, 0, sizeof(inventory));
        inventory.opening_pending = true;
        opening_pending.v.view_update = jazz_protocol__protocol__ViewUpdatePayload__new(subscription, committed, inventory,
                                                                                        jazz_protocol__protocol__SupportingRowsUpdate__snapshot_empty());
    }
    /* Known state describes this concrete receiver, not the canonical
     * coverage evaluator shared with its sibling usage subscriptions. Local
     * nodes may still withhold delivery pending upstream settlement, but the
     * cursor retains the same usage-site ownership. */
    void *known_state = subscribe.known_state;
    PEER_FN(declare_known_state)(peer, subscription, known_state ? jazz_protocol__protocol__KnownStateDeclaration__clone(known_state) : NULL);
    /* The first usage's rehydrate is the group's; let it answer that
     * usage's "Q at W" as a catch-up. */
    if (first_subscriber && known_state && jazz_protocol__protocol__KnownStateDeclaration__tag(known_state) == JAZZ_KNOWN_STATE_WATERMARK)
        PEER_FN(declare_known_state)(peer, group_subscription, jazz_protocol__protocol__KnownStateDeclaration__clone(known_state));
    {
        jz_btree claims;
        claims_clone(&claims, &subscription_policy_binding.claims);
        PEER_FN(set_subscription_policy_binding)(peer, subscription, subscription_policy_binding.identity, claims);
        /* The group key owns the maintained evaluator; install the same
         * admitted snapshot before any owner-loop rehydrate or delta can
         * touch it. */
        claims_clone(&claims, &subscription_policy_binding.claims);
        PEER_FN(set_subscription_policy_binding)(peer, group_subscription, subscription_policy_binding.identity, claims);
    }
    jazz_node_PublicationOutcome outcome;
    {
        jazz_protocol_Subscribe group_subscribe = subscribe;
        group_subscribe.subscription = group_subscription;
        memset(&message.v.subscribe, 0, sizeof(message.v.subscribe));
        message.tag = JAZZ_SM_RESERVED14; /* moved out */
        jazz_node_NodeState *n = jazz_db_node_lock(self->node);
        jz_err e = NS_FN(apply_subscribe_with_admitted_policy_binding)(n, group_subscribe, policy_binding_key_of(&subscription_policy_binding));
        jazz_db_node_unlock(self->node);
        if (e.code) {
            err = node_err(e);
            jazz_db__db__PolicyBinding__drop(&subscription_policy_binding);
            jazz_protocol__protocol__RegisterShapeOptions__drop(&upstream_opts);
            if (has_opening_pending) jazz_protocol__protocol__SyncMessage__drop(&opening_pending);
            goto out;
        }
        jazz_node__node__PublicationOutcome__settled(&outcome, NULL, 0);
    }
    jazz_protocol_BindingViewKey upstream_binding_view = { jazz_model__query__ValidatedQuery__shape_id(shape), jazz_model__query__Binding__binding_id(binding),
                                                           jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&upstream_opts) };
    bool has_selected_key = scope_relay && propagates_to_selected_authority;
    jazz_protocol_AuthorityResultKey selected_authority_result_key;
    if (has_selected_key)
        jazz_protocol__protocol__AuthorityResultKey__policy_scoped(&selected_authority_result_key, upstream_binding_view, policy_binding_key_of(&subscription_policy_binding));
    /* Record the exact source on the concrete D usage before any
     * recovery/publication path can emit a frame. The U wire registration is
     * queued later in this owner-loop turn, but its canonical authority key
     * is already determined by the immutable admitted binding. This lets the
     * send boundary preserve opening provenance instead of accidentally
     * publishing D's empty local overlay as a final strict result. */
    if (has_selected_key) {
        /* The coverage-group subscription owns the maintained receiver.
         * Install U on that owner before any publication path can open it;
         * D only receives the group's retargeted ViewUpdates and must not be
         * mistaken for the maintained receiver's source. */
        jazz_protocol_AuthorityResultKey copy;
        jazz_protocol__protocol__AuthorityResultKey__clone(&copy, &selected_authority_result_key);
        PEER_FN(set_subscription_authority_result_source)(peer, group_subscription, copy);
        PEER_FN(set_subscription_awaiting_selected_authority_source)(peer, group_subscription, waits_for_selected_authority);
        jazz_protocol__protocol__AuthorityResultKey__clone(&copy, &selected_authority_result_key);
        PEER_FN(set_subscription_authority_result_source)(peer, subscription, copy);
        PEER_FN(set_subscription_awaiting_selected_authority_source)(peer, subscription, waits_for_selected_authority);
    }
    if (local_subscriber && upstream_opts.binding_source == JAZZ_BINDING_SOURCE_RELAY_AUTHORITY_SESSION) {
        /* Settled authority membership survives a RocksDB reopen, while its
         * wire registration does not. Do not let that ownerless result settle
         * this fresh Global usage site before the new relay authority
         * registration receives a current reset from upstream. */
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
        NS_FN(invalidate_ownerless_settled_result_view)(n, upstream_binding_view);
        jazz_db_node_unlock(self->node);
    }
    {
        u32 changed = 0;
        bool published = false;
        jz_err e = finish_peer_publication_outcome_with_refresh(self->node, self->subscriptions, self->active_authority_view_receipts, tick_waker(t), outcome, false,
                                                                &changed, &published);
        if (e.code) {
            err = e;
            jazz_db__db__PolicyBinding__drop(&subscription_policy_binding);
            jazz_protocol__protocol__RegisterShapeOptions__drop(&upstream_opts);
            if (has_opening_pending) jazz_protocol__protocol__SyncMessage__drop(&opening_pending);
            if (has_selected_key) jazz_protocol__protocol__AuthorityResultKey__drop(&selected_authority_result_key);
            goto out;
        }
        t->stats.subscription_events += changed;
        st->needs_subscription_refresh |= published;
    }
    /* A strict child of a non-authoritative relay cannot open its maintained
     * receiver against the local overlay: it must wait until the exact
     * upstream authority receipt selected above has settled. `local_receiver`
     * describes the link, not the topology role, so a browser scope relay
     * needs this too even though its foreground link is a normal subscriber
     * connection. A serving authority selects D and evaluates it locally, so
     * it deliberately does not take this handoff path. */
    bool waiting_for_selected_authority_settlement = false;
    if (waits_for_selected_authority) {
        /* This usage site owns a distinct receipt. A same-shaped sibling must
         * neither settle nor block it. */
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
        bool settled = false;
        if (has_selected_key) {
            settled = NS_FN(has_settled_authority_result)(n, &selected_authority_result_key);
        } else {
            jazz_protocol_AuthorityResultKey key;
            jz_err e = NS_FN(authority_result_key_for_subscription)(n, authority_result_subscription, &key);
            if (e.code == 0) { settled = NS_FN(has_settled_authority_result)(n, &key); jazz_protocol__protocol__AuthorityResultKey__drop(&key); }
            else jz_err_drop(&e);
        }
        jazz_db_node_unlock(self->node);
        waiting_for_selected_authority_settlement = !settled;
    }
    jazz_db_CoverageGroup *group = (jazz_db_CoverageGroup *)jz_btree_get(&sub->coverage_groups, &coverage);
    if (!group) {
        jazz_db_CoverageGroup fresh_group;
        group = &fresh_group;
        memset(group, 0, sizeof(*group));
        group->shape = jazz_model__query__ValidatedQuery__clone(shape);
        group->binding = jazz_model__query__Binding__clone(binding);
        jazz_db__db__PolicyBinding__clone(&group->policy_binding, &subscription_policy_binding);
        group->policy_binding_origin = subscription_has_delegated_session ? JAZZ_CPBO_DELEGATED : JAZZ_CPBO_DIRECT_ADMITTED;
        jz_btree_init(&group->subscribers, &jazz_db_subscription_key_set_desc);
        jz_btree_init(&group->pending_initial_subscribers, &jazz_db_subscription_key_set_desc);
        group->has_pending_initial_update = false;
        jz_deque_init(&group->pending_incremental_updates, sizeof(jazz_db_KeyedSyncMessage));
        group->has_publication_runtime_token = false;
        group->has_pending_claim_refresh_revision = false;
        group->initialized = false;
        group->authority_result_subscription = authority_result_subscription;
        group->upstream_subscription = upstream_subscription;
        jazz_protocol__protocol__RegisterShapeOptions__clone(&group->upstream_opts, &upstream_opts);
        group->awaiting_upstream_settlement = waiting_for_selected_authority_settlement;
        jazz_db_CoverageKey group_key;
        jazz_db__db__CoverageKey__clone(&group_key, &coverage);
        jz_btree_insert(&sub->coverage_groups, &group_key, &fresh_group, NULL);
        group = (jazz_db_CoverageGroup *)jz_btree_get(&sub->coverage_groups, &coverage);
    }
    jz_btree_insert(&group->subscribers, &subscription, NULL, NULL);
    jz_btree_insert(&group->pending_initial_subscribers, &subscription, NULL, NULL);
    if (has_selected_key) {
        /* Keep the policy-scoped U source selected at admission. A later
         * owner-loop lookup must not collapse it to an unscoped local cache. */
        PEER_FN(set_subscription_authority_result_source)(peer, group_subscription, selected_authority_result_key);
    }
    {
        jazz_db_RelayOwnerKey owner_key = { group->upstream_subscription, t->connection_epoch, jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&coverage.opts) };
        bool fresh = false;
        jazz_db_RelayUpstreamSubscriptionOwner *existing = (jazz_db_RelayUpstreamSubscriptionOwner *)jz_btree_entry(self->relay_upstream_subscription_owners, &owner_key, &fresh);
        if (!fresh) {
            JZ_ASSERT(existing->downstream_connection_epoch == t->connection_epoch, "relay upstream handle changed downstream owner");
            JZ_ASSERT(jazz_db__db__CoverageKey__eq(&existing->coverage, &coverage), "relay upstream handle changed coverage group");
            jz_btree_insert(&existing->downstream_subscriptions, &subscription, NULL, NULL);
        } else {
            memset(existing, 0, sizeof(*existing));
            existing->request.subscription = group->upstream_subscription;
            existing->request.shape = jazz_model__query__ValidatedQuery__clone(shape);
            existing->request.binding = jazz_model__query__Binding__clone(binding);
            jazz_protocol__protocol__RegisterShapeOptions__clone(&existing->request.opts, &upstream_opts);
            existing->request.identity = group->policy_binding.identity;
            existing->request.has_policy_binding = true;
            jazz_db__db__PolicyBinding__clone(&existing->request.policy_binding, &group->policy_binding);
            existing->scalar_authority_revision = 0;
            jazz_db__db__ScalarReconciliation__default(&existing->scalar_reconciliation);
            existing->downstream_connection_epoch = t->connection_epoch;
            jazz_db__db__CoverageKey__clone(&existing->coverage, &coverage);
            jazz_db__db__PolicyBinding__clone(&existing->policy_binding, &group->policy_binding);
            jz_btree_init(&existing->downstream_subscriptions, &jazz_db_subscription_key_set_desc);
            jz_btree_insert(&existing->downstream_subscriptions, &subscription, NULL, NULL);
        }
    }
    {
        jazz_db_CoverageKey served_coverage;
        jazz_db__db__CoverageKey__clone(&served_coverage, &coverage);
        jazz_db_CoverageKey old;
        if (jz_btree_insert(&sub->served, &subscription, &served_coverage, &old)) jazz_db__db__CoverageKey__drop(&old);
    }
    if (has_opening_pending) {
        stamp_view_update_authorization_progress_from(peer, group_subscription, &opening_pending);
        self->has_last_resume_bytes = true;
        self->last_resume_bytes = serialized_sync_message_len(&opening_pending);
        jz_err e = jazz_db__db__peer_connection__send_subscriber_with_sync_context(self->node, peer, &self->transport, self->local_fate_routes, self->downstream_fates,
                                                                                   opening_pending);
        if (e.code) { err = e; jazz_db__db__PolicyBinding__drop(&subscription_policy_binding); jazz_protocol__protocol__RegisterShapeOptions__drop(&upstream_opts); goto out; }
        st->sent_view_update = true;
    }
    if (first_subscriber) {
        jazz_db_PendingUpstreamCommand *command = (jazz_db_PendingUpstreamCommand *)jz_vec_push_slot(sub->upstream_subscriptions, sizeof(jazz_db_PendingUpstreamCommand));
        memset(command, 0, sizeof(*command));
        command->tag = JAZZ_PUC_SUBSCRIBE;
        command->subscribe.subscription = group->upstream_subscription;
        command->subscribe.shape = jazz_model__query__ValidatedQuery__clone(shape);
        command->subscribe.binding = binding;
        binding = NULL;
        jazz_protocol__protocol__RegisterShapeOptions__clone(&command->subscribe.opts, &group->upstream_opts);
        /* The relay's transport identity is SYSTEM/backend, but each
         * multiplexed subscription carries the caller's admitted policy
         * context. */
        command->subscribe.identity = group->policy_binding.identity;
        command->subscribe.has_policy_binding = true;
        jazz_db__db__PolicyBinding__clone(&command->subscribe.policy_binding, &group->policy_binding);
    }
    /* Opening a subscription creates work that may require cold storage. Let
     * the current transport owner return first, so other frames (notably
     * local write receipts) can be ingressed before initial hydration is
     * attempted. */
    jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_AFTER_CURRENT_TURN);
    st->scheduled_follow_up = true;
    jazz_db__db__PolicyBinding__drop(&subscription_policy_binding);
    jazz_protocol__protocol__RegisterShapeOptions__drop(&upstream_opts);
out:
    if (has_coverage) jazz_db__db__CoverageKey__drop(&coverage);
    if (has_opts) jazz_protocol__protocol__RegisterShapeOptions__drop(&opts);
    if (binding) jazz_model__query__Binding__drop(binding);
    if (shape) jazz_model__query__ValidatedQuery__drop(shape);
    jazz_db__db__PolicyBinding__drop(&session_claim_binding);
    jazz_protocol__protocol__SyncMessage__drop(&message);
    return err;
}

static void keyed_deque_retain_not(jz_deque *q, const jazz_protocol_SubscriptionKey *subscription) {
    jz_deque kept;
    jz_deque_init(&kept, sizeof(jazz_db_KeyedSyncMessage));
    jazz_db_KeyedSyncMessage item;
    while (jz_deque_pop_front(q, &item)) {
        if (keep_not_recipient(&item, (void *)subscription)) jz_deque_push_back(&kept, &item);
        else jazz_db__db__KeyedSyncMessage__drop(&item);
    }
    jz_deque_drop(q, NULL);
    *q = kept;
}

static void sub_unsubscribe(TickCtx *t, jazz_db_SubscriberConnectionState *sub, jazz_protocol_SubscriptionKey subscription) {
    PC *self = t->self;
    jazz_node_PeerState *peer = sub->peer;
    u64 connection_epoch = t->connection_epoch;
    jz_btree_remove(&sub->pending_catalogue_subscriptions, &subscription, NULL, NULL);
    {
        jazz_types_AuthorSubject identity;
        jz_btree claims;
        bool has_policy = PEER_FN(subscription_policy_binding)(peer, subscription, &identity, &claims);
        jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(self->node);
        if (has_policy) {
            jazz_protocol_PolicyBindingKey key;
            jazz_protocol__protocol__PolicyBindingKey__from_canonical_parts(&key, identity, claims);
            NS_FN(apply_unsubscribe_with_admitted_policy_binding)(node, subscription, key);
        } else {
            NS_FN(apply_unsubscribe)(node, subscription);
        }
        jazz_db_node_unlock(self->node);
    }
    jazz_db_CoverageKey coverage;
    if (jz_btree_remove(&sub->served, &subscription, NULL, &coverage)) {
        jazz_db_CoverageGroup *group = (jazz_db_CoverageGroup *)jz_btree_get(&sub->coverage_groups, &coverage);
        if (group) {
            jz_btree_remove(&group->subscribers, &subscription, NULL, NULL);
            jz_btree_remove(&group->pending_initial_subscribers, &subscription, NULL, NULL);
            keyed_deque_retain_not(&group->pending_incremental_updates, &subscription);
            if (group->has_pending_initial_update && jazz_protocol_SubscriptionKey_eq(&group->pending_initial_update.subscription, &subscription)) {
                jazz_protocol__protocol__SyncMessage__drop(&group->pending_initial_update.message);
                group->has_pending_initial_update = false;
            }
            jazz_db_RelayOwnerKey owner_key = { group->upstream_subscription, connection_epoch, jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&coverage.opts) };
            jazz_db_RelayUpstreamSubscriptionOwner *owner = (jazz_db_RelayUpstreamSubscriptionOwner *)jz_btree_get(self->relay_upstream_subscription_owners, &owner_key);
            if (owner && owner->downstream_connection_epoch == connection_epoch && jazz_db__db__CoverageKey__eq(&owner->coverage, &coverage))
                jz_btree_remove(&owner->downstream_subscriptions, &subscription, NULL, NULL);
            if (group->subscribers.len == 0) {
                jazz_protocol_SubscriptionKey upstream_subscription = group->upstream_subscription;
                jazz_protocol_SubscriptionKey group_subscription = jazz_db__db__peer_connection__coverage_group_subscription_key(&coverage);
                /* A coverage group owns a maintained Groove receiver.
                 * Forgetting only the peer-side cursor leaves that receiver
                 * dormant in the shared runtime; a later re-open of the same
                 * logical coverage then races a stale source subscription
                 * instead of observing the current authority membership. Tear
                 * down both pieces together when the final usage site goes
                 * away. */
                {
                    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(self->node);
                    PEER_FN(forget_subscription_with_node)(peer, node, group_subscription);
                    jazz_db_node_unlock(self->node);
                }
                jz_btree_remove(&sub->coverage_groups, &coverage, NULL, NULL);
                jazz_db_RelayUpstreamSubscriptionOwner retired;
                if (jazz_db__db__node_runtime__retire_relay_upstream_subscription(self->relay_upstream_subscription_owners, upstream_subscription, connection_epoch,
                                                                                  &coverage, &retired)) {
                    jazz_db__db__RelayUpstreamSubscriptionOwner__drop(&retired);
                    /* The relay owns both the local exact authority receipt
                     * and its wire usage site. Retiring only the remote wire
                     * handle leaks a settled receipt until a later lifecycle
                     * sweep. */
                    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(self->node);
                    NS_FN(apply_unsubscribe)(node, upstream_subscription);
                    jazz_db_node_unlock(self->node);
                    jazz_db_PendingUpstreamCommand *command = (jazz_db_PendingUpstreamCommand *)jz_vec_push_slot(sub->upstream_subscriptions, sizeof(jazz_db_PendingUpstreamCommand));
                    memset(command, 0, sizeof(*command));
                    command->tag = JAZZ_PUC_UNSUBSCRIBE;
                    command->unsubscribe = upstream_subscription;
                }
            }
        }
        jazz_db__db__CoverageKey__drop(&coverage);
    }
    jazz_db_ShapeRegistrationKey registration_key = { subscription.shape_id, subscription.read_view };
    bool registration_still_served = false;
    {
        jazz_protocol_SubscriptionKey *active;
        jazz_db_CoverageKey *v;
        JZ_BTREE_FOR(&sub->served, active, v) {
            (void)v;
            if (memcmp(&active->shape_id, &subscription.shape_id, sizeof(subscription.shape_id)) == 0
                && memcmp(&active->read_view, &subscription.read_view, sizeof(subscription.read_view)) == 0) { registration_still_served = true; break; }
        }
    }
    if (registration_still_served) return;
    jazz_db_SubscriberShapeRegistration removed;
    if (!jz_btree_remove(&sub->shape_registrations, &registration_key, NULL, &removed)) return;
    bool owned = jazz_db__db__SubscriberShapeRegistration__owns_node_shape(&removed);
    if (removed.tag == JAZZ_SSR_REJECTED_UNSUPPORTED_CAPABILITY) jz_str_drop(&removed.detail);
    else jazz_protocol__protocol__RegisterShapeOptions__drop(&removed.opts);
    if (!owned) return;
    {
        jazz_db_ShapeRegistrationKey *k;
        jazz_db_SubscriberShapeRegistration *r;
        JZ_BTREE_FOR(&sub->shape_registrations, k, r)
            if (memcmp(&k->shape_id, &subscription.shape_id, sizeof(subscription.shape_id)) == 0 && jazz_db__db__SubscriberShapeRegistration__owns_node_shape(r)) return;
    }
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(self->node);
    NS_FN(release_shape_for_peer)(node, connection_epoch, subscription.shape_id);
    jazz_db_node_unlock(self->node);
}

/// The fallthrough `other =>` arm (the boxed upload-admission future).
/// `*catalogue_deferred` = the message was staged for catalogue persistence.
static jz_err sub_other(TickCtx *t, jazz_db_SubscriberConnectionState *sub, SubscriberTick *st, jazz_protocol_SyncMessage other,
                        jazz_db_BufferLease *lease, bool receipts_validated, bool *catalogue_deferred) {
    PC *self = t->self;
    jazz_node_PeerState *peer = sub->peer;
    *catalogue_deferred = false;
    if (other.tag == JAZZ_SM_COMMIT_UNIT)
        jazz_db__db__read_barrier__ReadBarrierReceiver__commit_unit_received(&sub->read_barrier, jazz_protocol__protocol__Transaction__hdr(other.v.commit_unit.tx)->tx_id);
    if (other.tag == JAZZ_SM_SESSION_CLAIMS && (sub->ingest_context.trust == JAZZ_TRUST_SESSION || sub->ingest_context.trust == JAZZ_TRUST_RELAY)) {
        /* Claims are fixed at host admission. A session cannot broaden
         * itself, and a subjectless relay cannot mutate the node-wide claim
         * cache; delegated bindings are request-local and topology-admitted
         * instead. */
        drop_peer_request(self->node);
        jazz_protocol__protocol__SyncMessage__drop(&other);
        return JZ_OK;
    }
    bool has_local_upload = other.tag == JAZZ_SM_COMMIT_UNIT;
    jazz_model_TxId upload_tx;
    jazz_protocol_SyncMessage upload_unit;
    if (has_local_upload) {
        upload_tx = jazz_protocol__protocol__Transaction__hdr(other.v.commit_unit.tx)->tx_id;
        jazz_protocol__protocol__SyncMessage__clone(&upload_unit, &other);
    }
    if (has_local_upload && !sub->local_receiver) {
        bool ready;
        {
            jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
            ready = jazz_db__db__subscriber_permissions_ready(NS_FN(permissions_ready)(n), sub->ingest_context.trust);
            jazz_db_node_unlock(self->node);
        }
        if (!ready) {
            jazz_protocol_SyncMessage response;
            memset(&response, 0, sizeof(response));
            response.tag = JAZZ_SM_FATE_UPDATE;
            response.v.fate_update.tx_id = upload_tx;
            response.v.fate_update.fate.tag = JAZZ_FATE_REJECTED;
            response.v.fate_update.fate.rejected.tag = JAZZ_REJECTION_MALFORMED_COMMIT;
            response.v.fate_update.fate.rejected.detail = jz_str_from("permissions_head_missing: no published permissions head");
            jz_vec_push(self->downstream_fates, &response, sizeof(response));
            jazz_protocol__protocol__SyncMessage__drop(&upload_unit);
            jazz_protocol__protocol__SyncMessage__drop(&other);
            return JZ_OK;
        }
    }
    jazz_model_TxId write_state_tx_id;
    bool has_write_state_tx_id = write_state_update_tx_id(&other, &write_state_tx_id);
    /* RegisterShape (registers the shape ahead of its binding), plus the
     * write-upload path: any responses (e.g. fate updates) flow back to the
     * subscriber. */
    jazz_node_PublicationOutcome outcome;
    jz_err e;
    if (jazz_node__node__is_catalogue_mutation(&other)) {
        jazz_node_NodeState *owner = jazz_db_node_lock(self->node);
        bool deferred = false;
        e = NS_FN(defer_catalogue_for_persistence)(owner, tick_waker(t), &deferred);
        if (e.code) { jazz_db_node_unlock(self->node); goto fail_node; }
        if (deferred) {
            jazz_db_node_unlock(self->node);
            jazz_db_StagedInboundMessage staged = { other, lease ? jazz_db__wire__BufferLease__clone(lease) : NULL, false, receipts_validated };
            jz_deque_push_front(&self->staged_inbound, &staged);
            *catalogue_deferred = true;
            if (has_local_upload) jazz_protocol__protocol__SyncMessage__drop(&upload_unit);
            return JZ_OK;
        }
        e = NS_FN(apply_sync_message_with_ingest_context)(owner, other, &sub->ingest_context, &outcome);
        jazz_db_node_unlock(self->node);
        if (e.code) goto fail_node_moved;
    } else {
        if (!sub_has_binding(t)) jz_panic("subscriber dispatch has an admitted immutable session binding");
        jazz_node_CommitUnitIngestContext ctx = sub->ingest_context;
        ctx.version_receipts_validated = receipts_validated;
        jazz_db_PolicyBinding binding;
        jazz_db__db__PolicyBinding__clone(&binding, &t->session_claim_binding);
        e = dispatch_admitted_subscriber_message(self->node, peer, sub->local_receiver, ctx, binding, self->local_fate_routes, self->downstream_fates,
                                                 tick_waker(t), other, &outcome);
        if (e.code) { if (has_local_upload) jazz_protocol__protocol__SyncMessage__drop(&upload_unit); return e; }
    }
    jazz_db_SyncMessageVec responses = {0};
    e = jazz_db__db__PublicationOutcome__responses(&outcome, &responses);
    if (e.code) { jazz_node__node__PublicationOutcome__drop(&outcome); if (has_local_upload) jazz_protocol__protocol__SyncMessage__drop(&upload_unit); return e; }
    {
        u32 changed = 0;
        bool published = false;
        e = finish_peer_publication_outcome_with_refresh(self->node, self->subscriptions, self->active_authority_view_receipts, tick_waker(t), outcome, false, &changed, &published);
        if (e.code) {
            jz_vec_drop(&responses, sizeof(jazz_protocol_SyncMessage), sync_message_vec_drop);
            if (has_local_upload) jazz_protocol__protocol__SyncMessage__drop(&upload_unit);
            return e;
        }
        t->stats.subscription_events += changed;
        st->needs_subscription_refresh |= published;
    }
    if (has_write_state_tx_id)
        handle_write_state_update(self->node, self->write_state_waiters, self->mutation_errors, self->browser_relay_recovered_tx_ids, self->scheduler, write_state_tx_id);
    /* A declared root with no upstream that settled this upload terminally
     * (the same predicate an upstream fate must meet to release an outbox
     * entry) has nobody left to ask. Queueing it would retain the unit
     * forever. Any node with an upstream must still relay, even though its
     * own ingest says Global. */
    bool settled_here = false;
    if (jazz_db__db__UploadOutbox__settles_uploads_locally(sub->outbox) && has_local_upload)
        for (u32 i = 0; i < responses.len; ++i)
            if (fate_update_settles_upload(&responses.ptr[i], upload_tx)) { settled_here = true; break; }
    jz_err err = JZ_OK;
    for (u32 i = 0; i < responses.len; ++i) {
        if (err.code) { jazz_protocol__protocol__SyncMessage__drop(&responses.ptr[i]); continue; }
        if (responses.ptr[i].tag == JAZZ_SM_FATE_UPDATE) jz_vec_push(self->downstream_fates, &responses.ptr[i], sizeof(jazz_protocol_SyncMessage));
        else err = send_with_sync_context(self->node, peer, &self->transport, responses.ptr[i]);
    }
    jz_vec_drop(&responses, sizeof(jazz_protocol_SyncMessage), NULL);
    if (err.code) { if (has_local_upload) jazz_protocol__protocol__SyncMessage__drop(&upload_unit); return err; }
    if (has_local_upload) {
        if (settled_here) {
            /* A reconnect may have reconstructed this upload before the unit
             * arrived here. */
            if (jazz_db__db__UploadOutbox__contains(sub->outbox, upload_tx)) jz_vec_push(&self->released_outbox_tx_ids, &upload_tx, sizeof(upload_tx));
            jazz_protocol__protocol__SyncMessage__drop(&upload_unit);
        } else if (jazz_db__db__queue_pending_upload_in(sub->outbox, upload_tx, true, upload_unit)) {
            jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_DEFERRED);
        }
    }
    return JZ_OK;
fail_node:
    jazz_protocol__protocol__SyncMessage__drop(&other);
fail_node_moved:
    if (has_local_upload) jazz_protocol__protocol__SyncMessage__drop(&upload_unit);
    return node_err(e);
}

/// The subscriber receive loop.
static jz_err subscriber_inbound(TickCtx *t, jazz_db_SubscriberConnectionState *sub, SubscriberTick *st, bool *stop) {
    PC *self = t->self;
    jazz_node_PeerState *peer = sub->peer;
    for (;;) {
        /* Drain new controls first, so cancellation retires parked requests
         * before catalogue activation can replay them. */
        jazz_db_HeldRequest released;
        bool barrier_passed = jazz_db__db__read_barrier__ReadBarrierReceiver__pop_released(&sub->read_barrier, &released);
        jazz_protocol_SyncMessage message;
        bool has_parked = false;
        jazz_db_PolicyBinding parked_policy_binding;
        jazz_db_BufferLease *lease = NULL;
        bool receipts_validated = false;
        if (barrier_passed) {
            message = released.message;
            lease = released.lease;
            receipts_validated = released.receipts_validated;
        } else {
            jazz_db_StagedInboundMessage staged;
            if (jz_deque_pop_front(&self->staged_inbound, &staged)) {
                message = staged.message;
                lease = staged.lease;
                receipts_validated = staged.receipts_validated;
            } else {
                bool has = false;
                jazz_db_ReceivedSyncMessage received;
                jazz_db_TransportError error;
                jz_err e = jazz_db__db__node_runtime__Transport__try_recv_owned_result(&self->transport, &has, &received, &error);
                if (e.code) {
                    jz_err_drop(&e);
                    if (handle_transport_backpressure(self->node, self->scheduler, &error)) {
                        jazz_protocol__wire__TransportError__drop(&error);
                        *stop = true;
                        return JZ_OK;
                    }
                    return jazz_db__db__peer_connection__transport_error(error);
                }
                if (has) {
                    message = received.message;
                    lease = received.lease;
                    receipts_validated = received.receipts_validated;
                } else {
                    /* Input ran out with requests held: answer the writes
                     * ingested so far now, so the requests behind them are
                     * served in this same turn (SPEC 8 §8.2.1). */
                    if (jazz_db__db__read_barrier__ReadBarrierReceiver__is_holding(&sub->read_barrier)) {
                        jazz_db__db__queue_local_acknowledgements(self->local_fate_routes, self->node);
                        bool ok;
                        JZ_TRY(flush_downstream_fates(self->node, peer, &self->transport, self->downstream_fates, &sub->read_barrier, self->scheduler, &ok));
                        if (!ok) { *stop = true; return JZ_OK; }
                        if (jazz_db__db__read_barrier__ReadBarrierReceiver__has_released(&sub->read_barrier)) continue;
                    }
                    bool ready = false;
                    jazz_protocol_SubscriptionKey key;
                    {
                        jazz_protocol_SubscriptionKey *k;
                        jazz_db_PendingCatalogueSubscription *pending;
                        JZ_BTREE_FOR(&sub->pending_catalogue_subscriptions, k, pending) {
                            jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
                            jazz_model_ValidatedQuery *registered = NS_FN(registered_shape)(n, pending->subscribe.shape_id);
                            jazz_db_node_unlock(self->node);
                            if (registered) { jazz_model__query__ValidatedQuery__drop(registered); key = *k; ready = true; break; }
                        }
                    }
                    if (!ready) break;
                    jazz_db_PendingCatalogueSubscription pending;
                    if (!jz_btree_remove(&sub->pending_catalogue_subscriptions, &key, NULL, &pending)) jz_panic("selected pending request");
                    memset(&message, 0, sizeof(message));
                    message.tag = JAZZ_SM_SUBSCRIBE;
                    message.v.subscribe = pending.subscribe;
                    has_parked = true;
                    parked_policy_binding = pending.policy_binding;
                    lease = NULL;
                    receipts_validated = false;
                }
            }
        }
        /* Hold requests behind an unanswered read barrier (SPEC 8 §8.2.1). A
         * request already released, or one parked for its catalogue, passed
         * its barrier before. */
        if (!barrier_passed && !has_parked) {
            jazz_db_HeldRequest held = { message, lease, receipts_validated };
            jazz_db_HeldRequest admitted;
            if (!jazz_db__db__read_barrier__ReadBarrierReceiver__admit(&sub->read_barrier, held, &admitted)) continue;
            message = admitted.message;
            lease = admitted.lease;
            receipts_validated = admitted.receipts_validated;
        }
        if (lease) {
            jazz_db_BufferLease *held = jazz_db__wire__BufferLease__clone(lease);
            jz_vec_push(&t->received_leases, &held, sizeof(held));
        }
        if (jazz_db__db__subscriber_inbound_message_is_authority_only(&message, sub->ingest_context, peer)) {
            drop_peer_request(self->node);
            jazz_protocol__protocol__SyncMessage__drop(&message);
            if (has_parked) jazz_db__db__PolicyBinding__drop(&parked_policy_binding);
            if (lease) jazz_db__wire__BufferLease__drop(lease);
            continue;
        }
        bool previously_applied_inbound = st->applied_inbound;
        st->applied_inbound = true;
        int flow = SUB_CONTINUE;
        jz_err err = JZ_OK;
        switch (message.tag) {
        case JAZZ_SM_CHUNK_REQUEST_BATCH:
            err = sub_chunk_request_batch(t, message, &flow);
            break;
        case JAZZ_SM_CHUNK_RESPONSE_BATCH:
            drop_peer_request(self->node);
            jazz_protocol__protocol__SyncMessage__drop(&message);
            break;
        case JAZZ_SM_CURRENT_ROWS_CANCEL:
            jazz_db__row_availability__CurrentRowsRouter__cancel_downstream(self->current_rows, t->connection_epoch, message.v.current_rows_cancel.request_id);
            jazz_protocol__protocol__SyncMessage__drop(&message);
            break;
        case JAZZ_SM_CURRENT_ROWS_REQUEST:
            err = sub_current_rows_request(t, sub, message, &flow);
            break;
        case JAZZ_SM_AUTHORIZATION_SCOPE_INTENT:
            err = sub_scope_intent(t, sub, message, &flow);
            break;
        /* Authority-only messages from subscribers are rejected. */
        case JAZZ_SM_AUTHORIZATION_SCOPE_VIEW: case JAZZ_SM_AUTHORIZATION_SCOPE_AGGREGATE_RECEIPT:
        case JAZZ_SM_AUTHORIZATION_SCOPE_UNAVAILABLE: case JAZZ_SM_AUTHORIZATION_SCOPE_DECISION:
            drop_peer_request(self->node);
            jazz_protocol__protocol__SyncMessage__drop(&message);
            break;
        case JAZZ_SM_REGISTER_SHAPE:
            /* Shape admission carries the parsed AST and may compile policy
             * dependencies. Keep that inactive state out of ordinary
             * commit-serving turns. */
            err = sub_register_shape(t, sub, message);
            break;
        case JAZZ_SM_SUBSCRIBE:
            /* Subscription admission has a substantially larger state machine
             * than ordinary peer messages. */
            err = sub_subscribe(t, sub, st, message, has_parked, &parked_policy_binding);
            has_parked = false;
            break;
        case JAZZ_SM_UNSUBSCRIBE:
            sub_unsubscribe(t, sub, message.v.unsubscribe.subscription);
            jazz_protocol__protocol__SyncMessage__drop(&message);
            break;
        default: {
            /* Upload admission has its own policy/persistence work. */
            bool catalogue_deferred = false;
            err = sub_other(t, sub, st, message, lease, receipts_validated, &catalogue_deferred);
            if (!err.code && catalogue_deferred) {
                st->applied_inbound = previously_applied_inbound;
                flow = SUB_BREAK;
            }
            break;
        }
        }
        if (has_parked) jazz_db__db__PolicyBinding__drop(&parked_policy_binding);
        if (lease) jazz_db__wire__BufferLease__drop(lease);
        if (err.code) return err;
        if (flow == SUB_STOP) { *stop = true; return JZ_OK; }
        if (flow == SUB_BREAK) break;
    }
    return JZ_OK;
}

/* ---------------------------------------------------------------- subscriber view serving */

static jazz_node_NodeState *tick_scoped_claims(TickCtx *t) {
    if (!sub_has_binding(t)) jz_panic("subscriber claims");
    jz_btree claims;
    claims_clone(&claims, &t->session_claim_binding.claims);
    jazz_node_NodeState *owner = jazz_db_node_lock(t->self->node);
    return NS_FN(scoped_active_session_claims)(owner, t->session_claim_binding.identity, claims);
}

static void tick_end_scoped_claims(TickCtx *t) {
    NS_FN(end_scoped_claims)(t->self->node->state);
    jazz_db_node_unlock(t->self->node);
}

/// Roll back a rejected opening and queue its rejection (always stops).
static void serve_reject_opening(TickCtx *t, jazz_db_SubscriberConnectionState *sub, jazz_protocol_SubscriptionKey subscription, jz_err error) {
    PC *self = t->self;
    rollback_rejected_subscriber_admission(self->node, sub->peer, &sub->served, &sub->coverage_groups, self->relay_upstream_subscription_owners,
                                           sub->upstream_subscriptions, t->connection_epoch, subscription);
    jazz_protocol_SyncMessage rejection;
    if (error.code == JAZZ_NODE_ERR_QUERY_CAPABILITY)
        rejection = unsupported_capability_message(subscription, error.msg ? jz_str_clone(error.msg) : jz_str_from(""));
    else
        rejection = server_failure_message(subscription, &error);
    jz_err_drop(&error);
    queue_direct_control(&self->pending_control_responses, rejection);
    jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_IMMEDIATE);
}

/// Send queued sibling updates in order. `*stop` on backpressure.
static jz_err serve_drain_incremental(TickCtx *t, jazz_db_SubscriberConnectionState *sub, SubscriberTick *st, jazz_db_CoverageGroup *group, bool *stop) {
    PC *self = t->self;
    while (group->pending_incremental_updates.len != 0) {
        jazz_db_KeyedSyncMessage *front = (jazz_db_KeyedSyncMessage *)jz_deque_at(&group->pending_incremental_updates, 0);
        jazz_protocol_SyncMessage update;
        jazz_protocol__protocol__SyncMessage__clone(&update, &front->message);
        jz_err e = send_prepared_subscriber_with_sync_context(self->node, sub->peer, &self->transport, self->local_fate_routes, self->downstream_fates, update);
        if (e.code) {
            if (e.code == JAZZ_EC_BACKPRESSURE) {
                jz_err_drop(&e);
                jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_DEFERRED);
                *stop = true;
                return JZ_OK;
            }
            return e;
        }
        jazz_db_KeyedSyncMessage done;
        jz_deque_pop_front(&group->pending_incremental_updates, &done);
        jazz_db__db__KeyedSyncMessage__drop(&done);
        st->sent_view_update = true;
    }
    return JZ_OK;
}

static void queue_group_update(TickCtx *t, jazz_db_SubscriberConnectionState *sub, jazz_db_CoverageGroup *group, jazz_protocol_SubscriptionKey group_subscription,
                               jazz_protocol_SubscriptionKey recipient, jazz_protocol_SyncMessage update) {
    update = retarget_view_update(update, recipient);
    stamp_view_update_authorization_progress_from(sub->peer, group_subscription, &update);
    stamp_subscriber_opening_state(t->self->node, sub->peer, &update);
    jazz_db_KeyedSyncMessage keyed = { recipient, update };
    jz_deque_push_back(&group->pending_incremental_updates, &keyed);
}

static bool serve_should_retry(jazz_node_PeerState *peer, jazz_protocol_SubscriptionKey group_subscription) {
    return !PEER_FN(subscription_awaits_selected_authority_source)(peer, group_subscription)
        && !PEER_FN(has_pending_initial_maintained_subscription)(peer, group_subscription);
}

/// Open each pending initial usage of one coverage group.
static jz_err serve_group_initial(TickCtx *t, jazz_db_SubscriberConnectionState *sub, SubscriberTick *st, const jazz_db_CoverageKey *coverage,
                                  jazz_db_CoverageGroup *group, jazz_protocol_SubscriptionKey group_subscription, jz_btree *pending_initial,
                                  bool *serve_again, bool *stop) {
    PC *self = t->self;
    jazz_node_PeerState *peer = sub->peer;
    jz_btree established_subscribers;
    jz_btree_init(&established_subscribers, &jazz_db_subscription_key_set_desc);
    {
        jazz_protocol_SubscriptionKey *k;
        void *unit;
        JZ_BTREE_FOR(&group->subscribers, k, unit) { (void)unit; if (!jz_btree_contains(pending_initial, k)) jz_btree_insert(&established_subscribers, k, NULL, NULL); }
    }
    bool has_retained = group->has_pending_initial_update;
    jazz_protocol_SubscriptionKey retained_subscription;
    if (has_retained) retained_subscription = group->pending_initial_update.subscription;
    /* A new sibling may sort before the stalled recipient. Its opening cannot
     * overtake an already-generated publication. */
    JZ_VEC(jazz_protocol_SubscriptionKey) initial_order = {0};
    if (has_retained && jz_btree_contains(pending_initial, &retained_subscription)) jz_vec_push(&initial_order, &retained_subscription, sizeof(retained_subscription));
    {
        jazz_protocol_SubscriptionKey *k;
        void *unit;
        JZ_BTREE_FOR(pending_initial, k, unit) {
            (void)unit;
            if (has_retained && jazz_protocol_SubscriptionKey_eq(k, &retained_subscription)) continue;
            jz_vec_push(&initial_order, k, sizeof(*k));
        }
    }
    jz_err err = JZ_OK;
    for (u32 i = 0; i < initial_order.len; ++i) {
        jazz_protocol_SubscriptionKey subscription = initial_order.ptr[i];
        jazz_protocol_SyncMessage update;
        if (group->has_pending_initial_update) {
            JZ_ASSERT(jazz_protocol_SubscriptionKey_eq(&group->pending_initial_update.subscription, &subscription), "retained opening belongs to this recipient");
            update = group->pending_initial_update.message;
            group->has_pending_initial_update = false;
            /* Inbound writes may have arrived while this exact opening waited
             * for capacity. Publish their delta on a later turn. */
            *serve_again = true;
        } else {
            bool cloning_existing = group->initialized || PEER_FN(has_maintained_subscription)(peer, group_subscription);
            jazz_node_ReconciledMaintainedSubscription *reconciled = NULL;
            if (cloning_existing) {
                jazz_node_NodeState *scoped = tick_scoped_claims(t);
                jz_err e = PEER_FN(reconcile_maintained_subscription_for_clone)(peer, scoped, group_subscription, group->shape, group->binding, &coverage->opts,
                                                                              tick_waker(t), &reconciled);
                tick_end_scoped_claims(t);
                if (e.code) { serve_reject_opening(t, sub, subscription, e); *stop = true; goto out; }
                if (!reconciled) {
                    jz_btree_insert(&group->pending_initial_subscribers, &subscription, NULL, NULL);
                    if (serve_should_retry(peer, group_subscription)) *serve_again = true;
                    continue;
                }
            }
            const jazz_protocol_SyncMessage *canonical_update = reconciled ? jazz_node__peer__ReconciledMaintainedSubscription__canonical_update(reconciled) : NULL;
            if (canonical_update) {
                /* Reconciliation has already advanced the canonical
                 * maintained state. Publish that durable transition to every
                 * established usage before any fallible reset assembly for the
                 * new usage can fail. */
                jazz_protocol_SubscriptionKey *sibling;
                void *unit;
                JZ_BTREE_FOR(&established_subscribers, sibling, unit) {
                    (void)unit;
                    jazz_protocol_SyncMessage sibling_update;
                    jazz_protocol__protocol__SyncMessage__clone(&sibling_update, canonical_update);
                    queue_group_update(t, sub, group, group_subscription, *sibling, sibling_update);
                }
                err = serve_drain_incremental(t, sub, st, group, stop);
                if (err.code || *stop) goto out;
            }
            jz_err e;
            bool has_update = false;
            if (reconciled) {
                jazz_node_NodeState *scoped = tick_scoped_claims(t);
                e = PEER_FN(rehydrate_query_for_subscription_from_reconciled_maintained_subscription)(peer, scoped, group_subscription, subscription, group->shape,
                                                                                                    reconciled, &update);
                tick_end_scoped_claims(t);
                has_update = e.code == 0;
            } else {
                jazz_protocol_RegisterShapeOptions opts;
                jazz_protocol__protocol__RegisterShapeOptions__clone(&opts, &coverage->opts);
                jazz_node_NodeState *scoped = tick_scoped_claims(t);
                e = PEER_FN(rehydrate_query_for_subscription_with_opts_and_waker)(peer, scoped, group_subscription, group->shape, group->binding, opts, tick_waker(t),
                                                                                 &has_update, &update);
                tick_end_scoped_claims(t);
                if (e.code == 0 && has_update) update = retarget_view_update(update, subscription);
            }
            if (e.code) { serve_reject_opening(t, sub, subscription, e); *stop = true; goto out; }
            if (!has_update) {
                jz_btree_insert(&group->pending_initial_subscribers, &subscription, NULL, NULL);
                if (serve_should_retry(peer, group_subscription)) *serve_again = true;
                continue;
            }
            group->initialized = true;
            stamp_view_update_authorization_progress_from(peer, group_subscription, &update);
            stamp_subscriber_opening_state(self->node, peer, &update);
        }
        self->has_last_resume_bytes = true;
        self->last_resume_bytes = serialized_sync_message_len(&update);
        /* Keep the exact generated opening until the semantic transport
         * accepts it. Rehydrating on retry would advance its receipt.
         * Backpressure hands the opening back, so no copy is held while the
         * transport owns it. */
        bool rejected = false;
        jazz_protocol_SyncMessage back;
        err = try_send_prepared_subscriber_with_sync_context(self->node, peer, &self->transport, self->local_fate_routes, self->downstream_fates, update, &rejected, &back);
        if (err.code) goto out;
        if (rejected) {
            group->has_pending_initial_update = true;
            group->pending_initial_update.subscription = subscription;
            group->pending_initial_update.message = back;
            jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_DEFERRED);
            *stop = true;
            goto out;
        }
        group->has_pending_initial_update = false;
        jz_btree_remove(&group->pending_initial_subscribers, &subscription, NULL, NULL);
        st->sent_view_update = true;
        jz_btree_insert(&established_subscribers, &subscription, NULL, NULL);
    }
out:
    jz_vec_drop(&initial_order, sizeof(jazz_protocol_SubscriptionKey), NULL);
    jz_btree_drop(&established_subscribers);
    return err;
}

/// Serve one coverage group. `*stop` = end this tick.
static jz_err serve_group(TickCtx *t, jazz_db_SubscriberConnectionState *sub, SubscriberTick *st, const jazz_db_CoverageKey *coverage, jazz_db_CoverageGroup *group,
                          bool *serve_again, bool *stop) {
    PC *self = t->self;
    jazz_node_PeerState *peer = sub->peer;
    u64 runtime_token;
    {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
        runtime_token = NS_FN(groove_runtime_token)(n);
        jazz_db_node_unlock(self->node);
    }
    if (group->has_publication_runtime_token && group->publication_runtime_token != runtime_token
        && (group->has_pending_initial_update || group->pending_incremental_updates.len != 0)) {
        /* INV-SYNC-13/14: these envelopes never crossed transport acceptance.
         * A policy revision must reauthorize them, not treat their cached row
         * bytes as previously delivered copies. */
        if (group->has_pending_initial_update) { jazz_protocol__protocol__SyncMessage__drop(&group->pending_initial_update.message); group->has_pending_initial_update = false; }
        jz_deque_drop(&group->pending_incremental_updates, jazz_db__db__KeyedSyncMessage__drop);
        jz_deque_init(&group->pending_incremental_updates, sizeof(jazz_db_KeyedSyncMessage));
        jz_btree_drop(&group->pending_initial_subscribers);
        subscription_key_set_clone(&group->pending_initial_subscribers, &group->subscribers);
        group->initialized = false;
    }
    group->has_publication_runtime_token = true;
    group->publication_runtime_token = runtime_token;
    jazz_protocol_SubscriptionKey group_subscription = jazz_db__db__peer_connection__coverage_group_subscription_key(coverage);
    {
        jz_btree claims;
        claims_clone(&claims, &group->policy_binding.claims);
        PEER_FN(set_subscription_policy_binding)(peer, group_subscription, group->policy_binding.identity, claims);
    }
    /* The maintained receiver is addressed by the policy-partitioned
     * coverage-group key. Its membership source is the locally admitted
     * downstream usage on an authority, or the separate upstream usage only
     * on a non-authoritative relay. Keep that exact association even for
     * groups that did not have to wait on this turn; strict relay
     * materialization always needs it. Admission selected the canonical
     * authority result for this coverage group. In particular, a scoped
     * client relay's U carries the immutable delegated policy binding. Do not
     * re-derive it from the wire subscription here: that can collapse a
     * scoped U into a sibling/unscoped cache entry between registration and
     * first publication. */
    bool has_key = false;
    jazz_protocol_AuthorityResultKey upstream_authority_result_key;
    {
        const jazz_protocol_AuthorityResultKey *source = PEER_FN(subscription_authority_result_source)(peer, group_subscription);
        if (source) {
            jazz_protocol__protocol__AuthorityResultKey__clone(&upstream_authority_result_key, source);
            has_key = true;
        } else {
            jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
            jz_err e = NS_FN(authority_result_key_for_subscription)(n, group->authority_result_subscription, &upstream_authority_result_key);
            jazz_db_node_unlock(self->node);
            if (e.code == 0) has_key = true; else jz_err_drop(&e);
        }
    }
    bool upstream_authority_is_settled = false;
    if (has_key) {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
        upstream_authority_is_settled = NS_FN(has_settled_authority_result)(n, &upstream_authority_result_key);
        jazz_db_node_unlock(self->node);
    }
    bool settled_handoff = group->awaiting_upstream_settlement && upstream_authority_is_settled;
    if (group->awaiting_upstream_settlement && !settled_handoff) {
        if (has_key) jazz_protocol__protocol__AuthorityResultKey__drop(&upstream_authority_result_key);
        return JZ_OK;
    }
    if (has_key) PEER_FN(set_subscription_authority_result_source)(peer, group_subscription, upstream_authority_result_key);
    jz_btree pending_initial;
    if (group->pending_incremental_updates.len == 0) {
        subscription_key_set_clone(&pending_initial, &group->pending_initial_subscribers);
    } else {
        /* Finish an earlier publication before opening a new sibling. */
        *serve_again = true;
        jz_btree_init(&pending_initial, &jazz_db_subscription_key_set_desc);
    }
    bool serving_initial = pending_initial.len != 0;
    if (serving_initial) {
        jz_err e = serve_group_initial(t, sub, st, coverage, group, group_subscription, &pending_initial, serve_again, stop);
        jz_btree_drop(&pending_initial);
        return e;
    }
    jz_btree_drop(&pending_initial);
    if (group->pending_incremental_updates.len == 0) {
        bool ready = false, has_update = false;
        jazz_protocol_SyncMessage update;
        jz_err e;
        jazz_protocol_RegisterShapeOptions opts;
        jazz_protocol__protocol__RegisterShapeOptions__clone(&opts, &coverage->opts);
        jazz_node_NodeState *scoped = tick_scoped_claims(t);
        if (settled_handoff && PEER_FN(has_maintained_subscription)(peer, group_subscription)) {
            /* A cold handoff opens the maintained view on its first turn.
             * Retrying that full rehydrate would discard the just-opened
             * receiver each time, so resume the existing view's initial
             * delta. That delta is not an authority successor closure: the
             * exact upstream reset has already been installed/forwarded. In
             * particular, never relabel an empty local terminal tick as
             * `reset_input_set`, because a receiver would correctly reject
             * its absent ProgramSourceCoverage manifest. */
            e = PEER_FN(query_update_for_subscription_with_opts_and_waker)(peer, scoped, group_subscription, group->shape, group->binding, opts, tick_waker(t),
                                                                          &ready, &has_update, &update);
        } else if (settled_handoff) {
            e = PEER_FN(rehydrate_query_for_subscription_with_opts_and_waker)(peer, scoped, group_subscription, group->shape, group->binding, opts, tick_waker(t),
                                                                             &has_update, &update);
            ready = has_update;
        } else {
            e = PEER_FN(query_update_for_subscription_with_opts_and_waker)(peer, scoped, group_subscription, group->shape, group->binding, opts, tick_waker(t),
                                                                          &ready, &has_update, &update);
        }
        tick_end_scoped_claims(t);
        if (e.code) {
            jazz_protocol_SubscriptionKey *subscription;
            void *unit;
            JZ_BTREE_FOR(&group->subscribers, subscription, unit) {
                (void)unit;
                queue_direct_control(&self->pending_control_responses, server_failure_message(*subscription, &e));
            }
            jz_err_drop(&e);
            jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_IMMEDIATE);
            *stop = true;
            return JZ_OK;
        }
        if (!ready) { *serve_again = true; return JZ_OK; }
        if (settled_handoff) {
            group->awaiting_upstream_settlement = false;
            PEER_FN(set_subscription_awaiting_selected_authority_source)(peer, group_subscription, false);
        }
        /* A settled unchanged view has no more work. Only an unfinished
         * hydration needs another owner turn. */
        if (!has_update) return JZ_OK;
        /* Producer-local change tracking suppresses no-ops; an empty snapshot
         * is meaningful. */
        bool has_shared = true;
        u32 remaining = group->subscribers.len;
        jazz_protocol_SubscriptionKey *subscription;
        void *unit;
        JZ_BTREE_FOR(&group->subscribers, subscription, unit) {
            (void)unit;
            remaining--;
            jazz_protocol_SyncMessage recipient_update = take_last_or_clone(&has_shared, &update, remaining != 0);
            queue_group_update(t, sub, group, group_subscription, *subscription, recipient_update);
        }
        if (has_shared) jazz_protocol__protocol__SyncMessage__drop(&update);
    }
    return serve_drain_incremental(t, sub, st, group, stop);
}

/// The post-ingress publication/refresh and view-serving phases.
static jz_err subscriber_serve(TickCtx *t, jazz_db_SubscriberConnectionState *sub, SubscriberTick *st, bool *stop) {
    PC *self = t->self;
    jazz_node_PeerState *peer = sub->peer;
    jazz_db__db__queue_local_acknowledgements(self->local_fate_routes, self->node);
    bool ok;
    JZ_TRY(flush_downstream_fates(self->node, peer, &self->transport, self->downstream_fates, &sub->read_barrier, self->scheduler, &ok));
    if (!ok) { *stop = true; return JZ_OK; }
    if (st->needs_subscription_refresh) {
        u32 changed = 0;
        jz_err e = jazz_db__db__node_runtime__refresh_subscriptions_in(self->node, self->subscriptions, self->active_authority_view_receipts, tick_waker(t), &changed);
        if (e.code) { changed = jazz_db__db__peer_connection__route_subscription_refresh_failure(self->subscriptions, &e); jz_err_drop(&e); }
        t->stats.subscription_events += changed;
    }
    if (st->applied_inbound && !st->scheduled_follow_up) jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_AFTER_CURRENT_TURN);
    if (st->applied_inbound) {
        u64 next = *self->subscriber_dirty_epoch + 1;
        *self->subscriber_dirty_epoch = next;
        self->observed_subscriber_dirty_epoch = next;
        sub->serve_dirty = true;
        /* Inbound admission, resident publication, and persistence form one
         * owner-loop turn. Cold downstream view assembly is a separately
         * scheduled turn so it cannot withhold the writer's durability
         * receipt or later cancellation/flush traffic on this connection. */
        jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_AFTER_CURRENT_TURN);
        *stop = true;
        return JZ_OK;
    }
    bool permissions_ready;
    {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
        permissions_ready = jazz_db__db__subscriber_permissions_ready(NS_FN(permissions_ready)(n), sub->ingest_context.trust);
        jazz_db_node_unlock(self->node);
    }
    if (sub->serve_dirty && permissions_ready) {
        bool serve_again = false;
        jazz_db_CoverageKey *coverage;
        jazz_db_CoverageGroup *group;
        JZ_BTREE_FOR(&sub->coverage_groups, coverage, group) {
            JZ_TRY(serve_group(t, sub, st, coverage, group, &serve_again, stop));
            if (*stop) return JZ_OK;
        }
        sub->serve_dirty = serve_again;
        if (serve_again) jazz_db__db__peer_connection__schedule_tick_in(self->scheduler, JAZZ_TICK_DEFERRED);
    }
    if (st->sent_view_update) {
        jazz_db_PendingSubscriberControlResponse response;
        while (jz_deque_pop_front(&sub->deferred_subscribe_rejections, &response)) jz_deque_push_back(&self->pending_control_responses, &response);
        JZ_TRY(sub_flush_controls(self, sub, &ok));
        if (!ok) { *stop = true; return JZ_OK; }
    }
    return JZ_OK;
}

/* ---------------------------------------------------------------- tick_inner */

static jz_err tick_inner(PC *self, jazz_db_DbTickStats *out) {
    /* Retain decoded message reservations through batched application, not
     * merely until dequeue. Deferrals clone the same lease into their front. */
    TickCtx t;
    memset(&t, 0, sizeof(t));
    t.self = self;
    memset(out, 0, sizeof(*out));
    if (self->has_startup_error) { self->has_startup_error = false; return self->startup_error; }
    jz_err err = JZ_OK;
    /* Finish draining the old authority's transport backlog before sending
     * reissued opens. Otherwise a fast confirming snapshot can join that
     * backlog and be discarded as an ineligible pre-handoff receipt. */
    if (self->inbound_authority_receipt_quarantine) {
        PC_FN(stage_inbound_without_authority_receipt)(self);
        if (self->has_startup_error) { self->has_startup_error = false; return self->startup_error; }
        if (self->inbound_authority_receipt_quarantine) return JZ_OK;
    }
    t.has_progress_waker = jazz_db__db__node_runtime__make_query_runtime_waker(self->scheduler, self->query_runtime_wake_pending, self->query_runtime_waker, &t.progress_waker);
    t.connection_epoch = self->connection_epoch;
    /* The host-admitted scope-isolated worker owns one immutable foreground
     * session and may forward that exact binding upstream. A generic
     * multiplexed relay has no per-binding admission capability and must
     * forward rather than select a user binding. Raw wire input cannot enable
     * either path. */
    t.permits_delegated_sessions = jazz_db__db__node_runtime__Transport__permits_delegated_sessions(&self->transport);
    if (!t.permits_delegated_sessions) {
        jazz_node_NodeState *n = jazz_db__db__LocalMutexBorrow__borrow(self->node);
        t.permits_delegated_sessions = NS_FN(client_relay_scope)(n) != NULL;
        jazz_db_node_unlock(self->node);
    }
    observe_shared_subscriber_dirty_epoch(self);
    JZ_TRY(retry_local_replay_after_progress(self));
    t.has_session_claim_binding = subscriber_session_claim_binding(self, &t.session_claim_binding);
    bind_subscriber_session_claims(self);
    bool stop = false;
    {
        bool rebound;
        err = rebind_subscriber_views_after_claim_change(self, tick_waker(&t), &rebound);
        if (err.code) goto out;
    }
    err = jazz_db__db__row_availability__PeerConnection__pump_current_rows(self);
    if (err.code) goto out;
    if (self->link.tag == JAZZ_LINK_UPSTREAM) {
        jazz_db_UpstreamConnectionState *up = &self->link.v.upstream;
        err = upstream_outbound(&t, up, &stop);
        if (err.code || stop) goto out;
        err = upstream_inbound(&t, up, &stop);
        if (err.code || stop) goto out;
    } else {
        jazz_db_SubscriberConnectionState *sub = &self->link.v.subscriber;
        SubscriberTick st;
        memset(&st, 0, sizeof(st));
        err = subscriber_prelude(&t, sub, &stop);
        if (err.code || stop) goto out;
        err = subscriber_inbound(&t, sub, &st, &stop);
        if (err.code || stop) goto out;
        /* Keep post-ingress publication/refresh work in its own phase. It
         * contains the maintained-view serving graph and does not need to
         * inflate the inbound message dispatcher. */
        err = subscriber_serve(&t, sub, &st, &stop);
        if (err.code || stop) goto out;
    }
out:
    *out = t.stats;
    if (t.has_session_claim_binding) jazz_db__db__PolicyBinding__drop(&t.session_claim_binding);
    for (u32 i = 0; i < t.received_leases.len; ++i) jazz_db__wire__BufferLease__drop(t.received_leases.ptr[i]);
    jz_vec_drop(&t.received_leases, sizeof(jazz_db_BufferLease *), NULL);
    return err;
}
