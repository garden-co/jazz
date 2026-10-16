/* subscriptions.c: port of crates/jazz/layers/db/src/db/subscriptions.rs
 * Subscription opening, query attachment, coverage, and cleanup.
 * (#[cfg(any(test, feature = "testing"))] diagnostics are not ported.) */
#include "jz_rt.h"
#include "../db.h"
#include "first_load.h"

#define SUB_FN(n) jazz_db__db__subscriptions__Db__##n
#define NS(n) jazz_node__node__NodeState__##n
#define LMVS(n) jazz_node__node__LocalMaintainedViewSubscription__##n
#define NODE(n) jazz_db__db__node_runtime__Node__##n
#define MUT(n) jazz_db__db__mutations__Db__##n

static jz_err ne(jz_err e) { return JZ_IS_ERR(e) ? jazz_node__node__api_error__Error__from_node_error(e) : e; }
static void unlock_node(const jazz_db_Db *db) { jazz_db__db__SharedNodeState__unlock(db->node->node); }

/* reads.rs entry points used here */
jz_err jazz_db__db__reads__Db__prepare_serialized_query_async(const jazz_db_Db *db, jz_slice query, const jazz_db_PolicyBinding *request_scope, jazz_db_PreparedQuery *out);
jz_err jazz_db__db__reads__Db__prepare_query(const jazz_db_Db *db, const jazz_model_Query *query, jazz_db_PreparedQuery *out);
jz_err jazz_db__db__transactions__Db__lock_for_transaction_operation(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id, jazz_node_NodeState **out);

static jz_err SUB_FN(open_subscription)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_types_AuthorSubject author, u32 authorization_mode, bool allow_pending_overlay, jazz_db_SubscriptionStream *out);
static jz_err SUB_FN(open_relation_subscription)(const jazz_db_Db *db, const jazz_model_RelationQuery *query, jazz_db_ReadOpts opts, jazz_types_AuthorSubject author, u32 authorization_mode, jazz_db_SubscriptionStream *out);
static jz_err SUB_FN(open_gated_subscription)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_types_AuthorSubject author, u32 authorization_mode, bool allow_pending_overlay, bool has_coverage_tier, u32 coverage_tier, jazz_db_SubscriptionStream *out);
jz_err SUB_FN(subscribe_for_identity)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_types_AuthorSubject author, jazz_db_SubscriptionStream *out);
jz_err SUB_FN(subscribe_client_for_identity)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_types_AuthorSubject author, jazz_db_SubscriptionStream *out);
jz_err SUB_FN(subscribe)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_db_SubscriptionStream *out);

static void opts_drop(jazz_db_ReadOpts *o) { jazz_protocol__protocol__ReadViewSpec__drop(&o->read_view); }
static jazz_db_ReadOpts opts_clone(const jazz_db_ReadOpts *o) {
    jazz_db_ReadOpts c = *o;
    jazz_protocol__protocol__ReadViewSpec__clone(&c.read_view, &o->read_view);
    return c;
}
static jazz_protocol_ReadViewSpec read_view_clone(const jazz_protocol_ReadViewSpec *v) {
    jazz_protocol_ReadViewSpec c;
    jazz_protocol__protocol__ReadViewSpec__clone(&c, v);
    return c;
}
static void policy_binding_drop(bool has, jazz_db_PolicyBinding *b) { if (has) jz_btree_drop(&b->claims); }

/* Decode, prepare, scope, and open a serialized host subscription. */
jz_err SUB_FN(subscribe_serialized_query)(const jazz_db_Db *db, jz_slice query, jazz_db_ReadOpts opts, const jazz_db_PolicyBinding *request_scope, jazz_db_SerializedSubscriptionAuthorization authorization, jazz_db_SubscriptionStream *out) {
    jz_err e = MUT(await_open_schema_for_coverage)(db, jazz_db__db__read_coverage_tier(&opts));
    if (JZ_IS_ERR(e)) { opts_drop(&opts); return e; }
    jazz_db_PreparedQuery prepared;
    e = jazz_db__db__reads__Db__prepare_serialized_query_async(db, query, request_scope, &prepared);
    if (JZ_IS_ERR(e)) { opts_drop(&opts); return e; }
    switch (authorization.tag) {
    case JAZZ_SSA_CLIENT_LOCAL: e = SUB_FN(subscribe)(db, &prepared, opts, out); break;
    case JAZZ_SSA_TRUSTED_SERVING: e = SUB_FN(subscribe_for_identity)(db, &prepared, opts, authorization.author, out); break;
    default: {
        jazz_types_AuthorSubject request_identity;
        bool has_identity = jazz_db__db__PreparedQuery__request_identity(&prepared, &request_identity);
        bool is_system = jazz_types__ids__AuthorSubject__cmp(&authorization.author, &jazz_types__ids__AuthorSubject__SYSTEM) == 0;
        if (!is_system && (!has_identity || jazz_types__ids__AuthorSubject__cmp(&request_identity, &authorization.author) != 0)) {
            opts_drop(&opts);
            e = jz_err_new(JAZZ_EC_PROTOCOL, "trusted client subscription requires immutable request claims");
            break;
        }
        e = SUB_FN(subscribe_client_for_identity)(db, &prepared, opts, authorization.author, out);
        break;
    }
    }
    jazz_db__db__PreparedQuery__drop(&prepared);
    return e;
}

/* Subscribe to a query and return a stream of materialized subscription events. */
jz_err SUB_FN(subscribe)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_db_SubscriptionStream *out) {
    return SUB_FN(open_subscription)(db, prepared, opts, db->identity.author, JAZZ_QAM_CLIENT_LOCAL, true, out);
}

/* Subscribe to a query evaluated as `author`. */
jz_err SUB_FN(subscribe_for_identity)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_types_AuthorSubject author, jazz_db_SubscriptionStream *out) {
    return SUB_FN(open_subscription)(db, prepared, opts, author, JAZZ_QAM_TRUSTED_SERVING, false, out);
}

/* Subscribe from a trusted backend client on behalf of an admitted author. */
jz_err SUB_FN(subscribe_client_for_identity)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_types_AuthorSubject author, jazz_db_SubscriptionStream *out) {
    u32 mode = jazz_db__db__read_coverage_tier(&opts) >= JAZZ_DUR_GLOBAL ? JAZZ_QAM_CLIENT_LOCAL : JAZZ_QAM_TRUSTED_SERVING;
    return SUB_FN(open_subscription)(db, prepared, opts, author, mode, false, out);
}

/* Subscribe to an output-changing relation query. */
jz_err SUB_FN(subscribe_relation_query)(const jazz_db_Db *db, const jazz_model_RelationQuery *query, jazz_db_ReadOpts opts, jazz_db_SubscriptionStream *out) {
    return SUB_FN(open_relation_subscription)(db, query, opts, db->identity.author, JAZZ_QAM_CLIENT_LOCAL, out);
}

/* LocalOnly owns no remote coverage, even for a memory-only foreground. */
static void SUB_FN(local_query_attachment)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, const jazz_db_ReadOpts *opts, jazz_db_QueryAttachment *out) {
    memset(out, 0, sizeof *out);
    jazz_protocol_RegisterShapeOptions rso;
    jazz_protocol__protocol__RegisterShapeOptions__default(&rso);
    jazz_protocol__protocol__ReadViewSpec__drop(&rso.read_view);
    rso.read_view = read_view_clone(&opts->read_view);
    jazz_protocol_ReadViewKey key = jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&rso);
    jazz_protocol__protocol__RegisterShapeOptions__drop(&rso);
    JZ_VEC_PUSH(out->subscriptions, NODE(next_subscription_key)(db->node, prepared->shape, key));
    out->requires_delivery_receipt = false;
    out->requires_current_authority_receipt = false;
}

static void SUB_FN(register_query_coverage)(const jazz_db_Db *db, jazz_protocol_CoverageKey coverage, jazz_db_PendingUpstreamSubscription subscription) {
    jz_btree *registrations = db->node->query_coverage_registrations;
    jazz_db_QueryCoverageRegistration *registration = jz_btree_get(registrations, &subscription.subscription);
    if (registration) {
        registration->ref_count += 1;
        jazz_protocol__protocol__CoverageKey__drop(&coverage);
        jazz_db__db__PendingUpstreamSubscription__drop(&subscription);
        return;
    }
    jazz_db_QueryCoverageRegistration fresh = { coverage, subscription, 1 };
    jz_btree_insert(registrations, &subscription.subscription, &fresh, NULL);
}

static void push_upstream_command(const jazz_db_Db *db, jazz_db_PendingUpstreamCommand cmd) {
    JZ_VEC_PUSH(db->node->upstream_subscriptions->v, cmd);
}

static jz_err SUB_FN(attach_query_shape_binding_with_opts)(const jazz_db_Db *db, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_protocol_RegisterShapeOptions opts, jazz_types_AuthorSubject identity, bool has_policy_binding, const jazz_db_PolicyBinding *policy_binding, jazz_protocol_SubscriptionKey *out) {
    jazz_protocol_SubscriptionKey subscription = NODE(next_subscription_key)(db->node, shape, jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&opts));
    jazz_db_PendingUpstreamCommand cmd;
    memset(&cmd, 0, sizeof cmd);
    cmd.tag = JAZZ_PUC_SUBSCRIBE;
    cmd.subscribe.subscription = subscription;
    cmd.subscribe.shape = jazz_model__query__ValidatedQuery__clone(shape);
    cmd.subscribe.binding = jazz_model__query__Binding__clone(binding);
    jazz_protocol__protocol__RegisterShapeOptions__clone(&cmd.subscribe.opts, &opts);
    cmd.subscribe.identity = identity;
    cmd.subscribe.has_policy_binding = has_policy_binding;
    if (has_policy_binding) cmd.subscribe.policy_binding = jazz_db__db__PolicyBinding__clone(policy_binding);
    push_upstream_command(db, cmd);
    jazz_protocol_CoverageKey key;
    jazz_db__db__request_coverage_key(shape, binding, opts, has_policy_binding ? policy_binding : NULL, &key);
    if (jz_btree_insert(db->node->latest_coverage_subscriptions, &key, &subscription, NULL)) { /* replaced */ }
    NODE(schedule_tick)(db->node, JAZZ_TICK_IMMEDIATE);
    *out = subscription;
    return JZ_OK;
}

static jz_err SUB_FN(attach_or_refresh_query_coverage_with_node)(const jazz_db_Db *db, const jazz_node_NodeState *node, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_protocol_RegisterShapeOptions upstream_opts, jazz_types_AuthorSubject identity, bool has_policy_binding, jazz_db_PolicyBinding policy_binding, bool requires_current_authority_receipt, jazz_db_QueryAttachment *out) {
    bool requires_delivery_receipt = requires_current_authority_receipt || db->node->upstream_durability_floor == JAZZ_DUR_LOCAL;
    jazz_protocol_BindingViewKey binding_view = {
        jazz_model__query__ValidatedQuery__shape_id(shape), jazz_model__query__Binding__binding_id(binding),
        jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&upstream_opts),
    };
    jazz_protocol_RegisterShapeOptions opts_for_key;
    jazz_protocol__protocol__RegisterShapeOptions__clone(&opts_for_key, &upstream_opts);
    jazz_protocol_CoverageKey coverage;
    jazz_db__db__request_coverage_key(shape, binding, opts_for_key, has_policy_binding ? &policy_binding : NULL, &coverage);
    jazz_protocol_AuthorityResultKey authority_key;
    if (coverage.has_policy_binding) {
        jazz_protocol_PolicyBindingKey pb;
        jazz_protocol__protocol__PolicyBindingKey__clone(&pb, &coverage.policy_binding);
        jazz_protocol__protocol__AuthorityResultKey__policy_scoped(&authority_key, binding_view, pb);
    } else {
        jazz_protocol__protocol__AuthorityResultKey__unscoped(&authority_key, binding_view);
    }
    u64 required_after = NS(applied_authority_result_generation)(node, &authority_key);
    jazz_protocol__protocol__AuthorityResultKey__drop(&authority_key);
    memset(out, 0, sizeof *out);
    /* All live usages pin one stream. One-shot freshness is a newer receipt
     * on that stream, not another stream with the same inputs. */
    jazz_protocol_SubscriptionKey *latest = NULL;
    if (jz_btree_contains(db->node->upstream_coverage_refcounts, &coverage))
        latest = jz_btree_get(db->node->latest_coverage_subscriptions, &coverage);
    if (latest) {
        jazz_protocol_SubscriptionKey subscription = *latest;
        bool inserted;
        jazz_protocol_CoverageKey ck;
        jazz_protocol__protocol__CoverageKey__clone(&ck, &coverage);
        usize *count = jz_btree_entry(db->node->upstream_coverage_refcounts, &ck, &inserted);
        if (!inserted) jazz_protocol__protocol__CoverageKey__drop(&ck);
        *count += 1;
        jazz_db_PendingUpstreamSubscription pending = {0};
        pending.subscription = subscription;
        pending.shape = jazz_model__query__ValidatedQuery__clone(shape);
        pending.binding = jazz_model__query__Binding__clone(binding);
        jazz_protocol__protocol__RegisterShapeOptions__clone(&pending.opts, &upstream_opts);
        pending.identity = identity;
        pending.has_policy_binding = has_policy_binding;
        if (has_policy_binding) pending.policy_binding = jazz_db__db__PolicyBinding__clone(&policy_binding);
        jazz_db_PendingUpstreamSubscription pending_clone;
        jazz_db__db__PendingUpstreamSubscription__clone(&pending_clone, &pending);
        jazz_protocol_CoverageKey reg_key;
        jazz_protocol__protocol__CoverageKey__clone(&reg_key, &coverage);
        SUB_FN(register_query_coverage)(db, reg_key, pending_clone);
        jz_btree *refreshes = db->node->coverage_refresh_generations;
        u64 *current = jz_btree_get(refreshes, &coverage);
        if (!current || *current != required_after) {
            jazz_protocol_CoverageKey rk;
            jazz_protocol__protocol__CoverageKey__clone(&rk, &coverage);
            jz_btree_insert(refreshes, &rk, &required_after, NULL);
            jazz_db_PendingUpstreamCommand cmd;
            memset(&cmd, 0, sizeof cmd);
            cmd.tag = JAZZ_PUC_SUBSCRIBE;
            cmd.subscribe = pending;
            push_upstream_command(db, cmd);
            NODE(schedule_tick)(db->node, JAZZ_TICK_IMMEDIATE);
        } else {
            jazz_db__db__PendingUpstreamSubscription__drop(&pending);
        }
        JZ_VEC_PUSH(out->subscriptions, subscription);
        jazz_db_BindingViewRequirement req = { binding_view, required_after };
        JZ_VEC_PUSH(out->required_after, req);
        out->requires_delivery_receipt = requires_delivery_receipt;
        out->requires_current_authority_receipt = requires_current_authority_receipt;
        JZ_VEC_PUSH(out->registrations, subscription);
        jazz_db_CoverageRefresh refresh = { coverage, required_after };
        JZ_VEC_PUSH(out->refreshes, refresh);
        jazz_protocol__protocol__RegisterShapeOptions__drop(&upstream_opts);
        policy_binding_drop(has_policy_binding, &policy_binding);
        return JZ_OK;
    }
    jazz_protocol_SubscriptionKey subscription;
    jazz_protocol_RegisterShapeOptions attach_opts;
    jazz_protocol__protocol__RegisterShapeOptions__clone(&attach_opts, &upstream_opts);
    jz_err e = SUB_FN(attach_query_shape_binding_with_opts)(db, shape, binding, attach_opts, identity, has_policy_binding, &policy_binding, &subscription);
    if (JZ_IS_ERR(e)) {
        jazz_protocol__protocol__CoverageKey__drop(&coverage);
        jazz_protocol__protocol__RegisterShapeOptions__drop(&upstream_opts);
        policy_binding_drop(has_policy_binding, &policy_binding);
        return e;
    }
    bool inserted;
    jazz_protocol_CoverageKey ck;
    jazz_protocol__protocol__CoverageKey__clone(&ck, &coverage);
    usize *count = jz_btree_entry(db->node->upstream_coverage_refcounts, &ck, &inserted);
    if (!inserted) jazz_protocol__protocol__CoverageKey__drop(&ck);
    *count += 1;
    jazz_db_PendingUpstreamSubscription pending = {0};
    pending.subscription = subscription;
    pending.shape = jazz_model__query__ValidatedQuery__clone(shape);
    pending.binding = jazz_model__query__Binding__clone(binding);
    pending.opts = upstream_opts;
    pending.identity = identity;
    pending.has_policy_binding = has_policy_binding;
    if (has_policy_binding) pending.policy_binding = policy_binding;
    SUB_FN(register_query_coverage)(db, coverage, pending);
    JZ_VEC_PUSH(out->subscriptions, subscription);
    jazz_db_BindingViewRequirement req = { binding_view, required_after };
    JZ_VEC_PUSH(out->required_after, req);
    out->requires_delivery_receipt = requires_delivery_receipt;
    out->requires_current_authority_receipt = requires_current_authority_receipt;
    JZ_VEC_PUSH(out->registrations, subscription);
    return JZ_OK;
}

static jz_err SUB_FN(attach_or_refresh_query_coverage)(const jazz_db_Db *db, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_protocol_RegisterShapeOptions upstream_opts, jazz_types_AuthorSubject identity, bool has_policy_binding, jazz_db_PolicyBinding policy_binding, bool requires_current_authority_receipt, jazz_db_QueryAttachment *out) {
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(db->node->node, "attach_or_refresh_query_coverage");
    jz_err e = SUB_FN(attach_or_refresh_query_coverage_with_node)(db, node, shape, binding, upstream_opts, identity, has_policy_binding, policy_binding,
                                                                  requires_current_authority_receipt, out);
    unlock_node(db);
    return e;
}

/* Attach a one-shot usage-site query coverage request. */
jz_err SUB_FN(attach_query_with_opts)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_db_QueryAttachment *out) {
    jz_err e = MUT(ensure_open_schema_admitted)(db);
    if (!JZ_IS_ERR(e)) e = jazz_db__db__ensure_supported_read_view(&opts);
    if (JZ_IS_ERR(e)) { opts_drop(&opts); return e; }
    if (opts.tier == JAZZ_DB_READ_LOCAL_ONLY) {
        SUB_FN(local_query_attachment)(db, prepared, &opts, out);
        opts_drop(&opts);
        return JZ_OK;
    }
    u32 coverage_tier = jazz_db__db__read_coverage_tier(&opts);
    jazz_protocol_RegisterShapeOptions upstream_opts;
    jazz_protocol_ReadViewSpec rv = read_view_clone(&opts.read_view);
    NODE(upstream_register_shape_options)(db->node, coverage_tier, &rv, &upstream_opts);
    opts_drop(&opts);
    bool has_pb = false;
    jazz_db_PolicyBinding pb;
    e = jazz_db__db__PreparedQuery__request_policy_binding(prepared, db->identity.author, &has_pb, &pb);
    if (JZ_IS_ERR(e)) { jazz_protocol__protocol__RegisterShapeOptions__drop(&upstream_opts); return e; }
    return SUB_FN(attach_or_refresh_query_coverage)(db, prepared->shape, prepared->binding, upstream_opts, db->identity.author, has_pb, pb,
                                                    coverage_tier >= JAZZ_DUR_GLOBAL, out);
}

/* Attach a one-shot usage-site query coverage request evaluated as `author`. */
jz_err SUB_FN(attach_query_with_opts_for_identity)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_types_AuthorSubject author, jazz_db_QueryAttachment *out) {
    jz_err e = MUT(ensure_open_schema_admitted)(db);
    if (!JZ_IS_ERR(e)) e = jazz_db__db__ensure_supported_read_view(&opts);
    if (JZ_IS_ERR(e)) { opts_drop(&opts); return e; }
    if (opts.tier == JAZZ_DB_READ_LOCAL_ONLY) {
        SUB_FN(local_query_attachment)(db, prepared, &opts, out);
        opts_drop(&opts);
        return JZ_OK;
    }
    u32 coverage_tier = jazz_db__db__read_coverage_tier(&opts);
    jazz_protocol_RegisterShapeOptions upstream_opts;
    jazz_protocol_ReadViewSpec rv = read_view_clone(&opts.read_view);
    NODE(upstream_register_shape_options)(db->node, coverage_tier, &rv, &upstream_opts);
    opts_drop(&opts);
    jazz_node_NodeState *owner = jazz_db__db__LocalMutexBorrow__borrow(db->node->node, "attach_query_with_opts_for_identity");
    jazz_node_ActiveSessionClaimsScope *scope;
    e = jazz_db__db__PreparedQuery__scoped_node(prepared, owner, author, &scope);
    if (JZ_IS_ERR(e)) { unlock_node(db); jazz_protocol__protocol__RegisterShapeOptions__drop(&upstream_opts); return e; }
    jazz_model_ValidatedQuery *shape;
    jazz_model_Binding *binding;
    e = ne(NS(prepare_query_binding_for_link)(jazz_node__node__ActiveSessionClaimsScope__node(scope), prepared->shape, prepared->binding,
                                             upstream_opts.tier, author, &shape, &binding)); /* super::block_on */
    jazz_node__node__ActiveSessionClaimsScope__drop(scope);
    unlock_node(db);
    if (JZ_IS_ERR(e)) { jazz_protocol__protocol__RegisterShapeOptions__drop(&upstream_opts); return e; }
    bool has_pb = false;
    jazz_db_PolicyBinding pb;
    e = jazz_db__db__PreparedQuery__request_policy_binding(prepared, author, &has_pb, &pb);
    if (!JZ_IS_ERR(e))
        e = SUB_FN(attach_or_refresh_query_coverage)(db, shape, binding, upstream_opts, author, has_pb, pb, coverage_tier >= JAZZ_DUR_GLOBAL, out);
    else
        jazz_protocol__protocol__RegisterShapeOptions__drop(&upstream_opts);
    jazz_model__query__ValidatedQuery__drop(shape);
    jazz_model__query__Binding__drop(binding);
    return e;
}

jz_err SUB_FN(attach_query_with_coverage_async)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, u32 coverage_tier, bool has_open_tx, jazz_model_OpenTransactionId open_tx_id, bool has_author, jazz_types_AuthorSubject author, jazz_db_QueryAttachment *out) {
    jz_err e = MUT(await_open_schema_for_coverage)(db, coverage_tier);
    if (!JZ_IS_ERR(e)) e = jazz_db__db__ensure_supported_read_view(&opts);
    if (JZ_IS_ERR(e)) { opts_drop(&opts); return e; }
    jazz_node_NodeState *locked;
    if (has_open_tx) {
        e = jazz_db__db__transactions__Db__lock_for_transaction_operation(db, open_tx_id, &locked);
        if (JZ_IS_ERR(e)) { opts_drop(&opts); return e; }
    } else {
        locked = jazz_db__db__SharedNodeState__lock(db->node->node);
    }
    jazz_types_AuthorSubject identity = has_author ? author : db->identity.author;
    jazz_node_ActiveSessionClaimsScope *scope;
    e = jazz_db__db__PreparedQuery__scoped_node(prepared, locked, identity, &scope);
    if (JZ_IS_ERR(e)) { unlock_node(db); opts_drop(&opts); return e; }
    jazz_node_NodeState *node = jazz_node__node__ActiveSessionClaimsScope__node(scope);
    if (has_open_tx) {
        void *snapshot;
        e = ne(NS(open_transaction_snapshot)(node, open_tx_id, &snapshot));
        if (JZ_IS_ERR(e)) goto fail;
        jazz_protocol__protocol__ReadViewSpec__drop(&opts.read_view);
        memset(&opts.read_view, 0, sizeof opts.read_view);
        opts.read_view.source_tag = JAZZ_RVS_SNAPSHOT;
        opts.read_view.snapshot = snapshot; /* .into() */
    }
    if (opts.tier == JAZZ_DB_READ_LOCAL_ONLY) {
        SUB_FN(local_query_attachment)(db, prepared, &opts, out);
        jazz_node__node__ActiveSessionClaimsScope__drop(scope);
        unlock_node(db);
        opts_drop(&opts);
        return JZ_OK;
    }
    jazz_protocol_RegisterShapeOptions upstream_opts;
    jazz_protocol_ReadViewSpec rv = read_view_clone(&opts.read_view);
    NODE(upstream_register_shape_options)(db->node, coverage_tier, &rv, &upstream_opts);
    jazz_model_ValidatedQuery *shape;
    jazz_model_Binding *binding;
    if (has_author) {
        e = ne(NS(prepare_query_binding_for_link)(node, prepared->shape, prepared->binding, upstream_opts.tier, author, &shape, &binding));
        if (JZ_IS_ERR(e)) { jazz_protocol__protocol__RegisterShapeOptions__drop(&upstream_opts); goto fail; }
    } else {
        shape = jazz_model__query__ValidatedQuery__clone(prepared->shape);
        binding = jazz_model__query__Binding__clone(prepared->binding);
    }
    bool has_pb = false;
    jazz_db_PolicyBinding pb;
    e = jazz_db__db__PreparedQuery__request_policy_binding(prepared, identity, &has_pb, &pb);
    if (!JZ_IS_ERR(e))
        e = SUB_FN(attach_or_refresh_query_coverage_with_node)(db, node, shape, binding, upstream_opts, identity, has_pb, pb, coverage_tier >= JAZZ_DUR_GLOBAL, out);
    else
        jazz_protocol__protocol__RegisterShapeOptions__drop(&upstream_opts);
    jazz_model__query__ValidatedQuery__drop(shape);
    jazz_model__query__Binding__drop(binding);
fail:
    jazz_node__node__ActiveSessionClaimsScope__drop(scope);
    unlock_node(db);
    opts_drop(&opts);
    return e;
}

/* Wait for the node owner before opening ordinary or identity-aware coverage. */
jz_err SUB_FN(attach_query_with_opts_async)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, bool has_open_tx, jazz_model_OpenTransactionId open_tx_id, bool has_author, jazz_types_AuthorSubject author, jazz_db_QueryAttachment *out) {
    u32 coverage_tier = jazz_db__db__read_coverage_tier(&opts);
    return SUB_FN(attach_query_with_coverage_async)(db, prepared, opts, coverage_tier, has_open_tx, open_tx_id, has_author, author, out);
}

/* Attach a one-shot usage-site query coverage request at the default tier. */
jz_err SUB_FN(attach_query)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_QueryAttachment *out) {
    jazz_db_ReadOpts opts;
    jazz_db__db__ReadOpts__default(&opts);
    return SUB_FN(attach_query_with_opts)(db, prepared, opts, out);
}

/* LocalOnly attachments are immediately ready against this node's data.
 * With Full propagation, memory-only foregrounds first receive their
 * owner's local query answer; remote reads also require an authority receipt. */
bool SUB_FN(query_attachment_is_covered)(const jazz_db_Db *db, const jazz_db_QueryAttachment *attachment) {
    /* Local propagation does not gate a durable node's local knowledge. */
    if (!attachment->requires_delivery_receipt) return true;
    jazz_db_SharedNodeState *shared = db->node->node;
    if (shared->locked) return false; /* try_lock: node owner unavailable */
    const jazz_node_NodeState *node = jazz_db__db__SharedNodeState__lock(shared);
    const jazz_db_AuthorityViewReceiptsCell *receipts = db->node->active_authority_view_receipts;
    bool has_current_authority_receipt = false;
    if (receipts->has) {
        has_current_authority_receipt = true;
        JZ_VEC_FOR(const jazz_db_BindingViewRequirement, r, attachment->required_after)
            if (!jz_btree_contains(&receipts->binding_views, &r->view)) { has_current_authority_receipt = false; break; }
        if (has_current_authority_receipt)
            JZ_VEC_FOR(const jazz_protocol_SubscriptionKey, s, attachment->subscriptions)
                if (!jz_btree_contains(&receipts->subscriptions, s)) { has_current_authority_receipt = false; break; }
    }
    bool covered = true;
    JZ_VEC_FOR(const jazz_db_BindingViewRequirement, r, attachment->required_after) {
        bool have_receipt = false, consistent = true;
        jazz_protocol_AuthorityResultKey receipt;
        JZ_VEC_FOR(const jazz_protocol_SubscriptionKey, s, attachment->subscriptions) {
            jazz_protocol_AuthorityResultKey key;
            jz_err e = NS(authority_result_key_for_subscription)(node, *s, &key);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); continue; } /* .ok() */
            if (!jazz_protocol__protocol__SubscriptionKey__eq(&key.binding_view, &r->view)) { jazz_protocol__protocol__AuthorityResultKey__drop(&key); continue; }
            if (!have_receipt) { receipt = key; have_receipt = true; continue; }
            if (!jazz_protocol__protocol__AuthorityResultKey__eq(&key, &receipt)) consistent = false;
            jazz_protocol__protocol__AuthorityResultKey__drop(&key);
        }
        if (!have_receipt) { covered = false; break; }
        /* Rebuild/eviction preserves sequencing but discards the delivered
         * answer. A counter alone cannot cover a read. */
        bool ok = consistent
            && NS(applied_authority_result_generation)(node, &receipt) > r->generation
            && NS(has_settled_authority_result)(node, &receipt)
            && !NS(opening_pending_for_authority_result)(node, &receipt);
        jazz_protocol__protocol__AuthorityResultKey__drop(&receipt);
        if (!ok) { covered = false; break; }
    }
    covered = covered && (!attachment->requires_current_authority_receipt || has_current_authority_receipt);
    unlock_node(db);
    if (covered) {
        jz_btree *refreshes = db->node->coverage_refresh_generations;
        JZ_VEC_FOR(const jazz_db_CoverageRefresh, r, attachment->refreshes) {
            u64 *generation = jz_btree_get(refreshes, &r->coverage);
            if (generation && *generation == r->generation) jz_btree_remove(refreshes, &r->coverage, NULL, NULL);
        }
    }
    return covered;
}

typedef void (*jazz_db_UnsubscribeFn)(void *ctx, jazz_protocol_SubscriptionKey subscription);

static void SUB_FN(detach_query_using)(const jazz_db_Db *db, jazz_db_QueryAttachment attachment, jazz_db_UnsubscribeFn unsubscribe, void *ctx) {
    typedef struct { jazz_protocol_SubscriptionKey subscription; jazz_protocol_CoverageKey coverage; } Removed;
    JZ_VEC(Removed) removed_subscriptions = {0};
    jz_btree *registrations = db->node->query_coverage_registrations;
    JZ_VEC_FOR(jazz_protocol_SubscriptionKey, subscription, attachment.registrations) {
        jazz_db_QueryCoverageRegistration *registration = jz_btree_get(registrations, subscription);
        if (!registration) continue;
        jazz_protocol_CoverageKey coverage;
        jazz_protocol__protocol__CoverageKey__clone(&coverage, &registration->coverage);
        registration->ref_count = registration->ref_count ? registration->ref_count - 1 : 0;
        if (registration->ref_count == 0) jz_btree_remove(registrations, subscription, NULL, NULL);
        jz_btree *coverage_refcounts = db->node->upstream_coverage_refcounts;
        usize *count = jz_btree_get(coverage_refcounts, &coverage);
        if (!count) { jazz_protocol__protocol__CoverageKey__drop(&coverage); continue; }
        *count = *count ? *count - 1 : 0;
        bool last_coverage_pin = *count == 0;
        if (last_coverage_pin) {
            jz_btree_remove(coverage_refcounts, &coverage, NULL, NULL);
            jz_btree_remove(db->node->awaiting_initial_authority_coverage, &coverage, NULL, NULL);
            Removed r = { *subscription, coverage };
            JZ_VEC_PUSH(removed_subscriptions, r);
        } else {
            jazz_protocol__protocol__CoverageKey__drop(&coverage);
        }
    }
    JZ_VEC_FOR(Removed, r, removed_subscriptions) {
        jazz_db_AuthorityViewReceiptsCell *receipts = db->node->active_authority_view_receipts;
        if (receipts->has) jz_btree_remove(&receipts->subscriptions, &r->subscription, NULL, NULL);
        jz_btree_remove(db->node->coverage_refresh_generations, &r->coverage, NULL, NULL);
        unsubscribe(ctx, r->subscription);
        jazz_protocol_SubscriptionKey *latest = jz_btree_get(db->node->latest_coverage_subscriptions, &r->coverage);
        if (latest && jazz_protocol__protocol__SubscriptionKey__eq(latest, &r->subscription))
            jz_btree_remove(db->node->latest_coverage_subscriptions, &r->coverage, NULL, NULL);
        jazz_db_PendingUpstreamCommand cmd;
        memset(&cmd, 0, sizeof cmd);
        cmd.tag = JAZZ_PUC_UNSUBSCRIBE;
        cmd.unsubscribe = r->subscription;
        push_upstream_command(db, cmd);
        jazz_protocol__protocol__CoverageKey__drop(&r->coverage);
    }
    jz_free(removed_subscriptions.ptr);
    jazz_db__db__QueryAttachment__drop(&attachment);
    NODE(schedule_tick)(db->node, JAZZ_TICK_IMMEDIATE);
}

/* Queued `node.lock().await.apply_unsubscribe(subscription)` */
typedef struct UnsubscribeOp { jazz_db_SharedNodeState *node; jazz_protocol_SubscriptionKey subscription; } UnsubscribeOp;
static bool unsubscribe_op_poll(void *self, const jz_waker *cx, void *out) {
    UnsubscribeOp *op = self;
    jazz_node_NodeState *node = jazz_db__db__SharedNodeState__lock(op->node);
    NS(apply_unsubscribe)(node, op->subscription);
    jazz_db__db__SharedNodeState__unlock(op->node);
    *(jz_err *)out = JZ_OK;
    return true;
}
static void unsubscribe_op_drop(void *self) { UnsubscribeOp *op = self; jz_rc_release(op->node); jz_free(op); }

static void detach_unsubscribe(void *ctx, jazz_protocol_SubscriptionKey subscription) {
    const jazz_db_Db *db = ctx;
    jazz_db_SharedNodeState *shared = db->node->node;
    if (!shared->locked) {
        jazz_node_NodeState *owner = jazz_db__db__SharedNodeState__lock(shared);
        NS(apply_unsubscribe)(owner, subscription);
        jazz_db__db__SharedNodeState__unlock(shared);
    } else {
        UnsubscribeOp *op = JZ_NEW(UnsubscribeOp);
        op->node = jz_rc_clone(shared);
        op->subscription = subscription;
        NODE(enqueue_transaction_cleanup)(db->node, (jazz_db_LocalFuture){ op, unsubscribe_op_poll, unsubscribe_op_drop });
    }
}

/* Detach a one-shot query coverage request. If storage owns the node,
 * physical cleanup runs on a subsequent tick after that operation yields it. */
void SUB_FN(detach_query)(const jazz_db_Db *db, jazz_db_QueryAttachment attachment) {
    SUB_FN(detach_query_using)(db, attachment, detach_unsubscribe, (void *)db);
}

static void detach_async_unsubscribe(void *ctx, jazz_protocol_SubscriptionKey subscription) {
    NS(apply_unsubscribe)((jazz_node_NodeState *)ctx, subscription);
}

/* Release binding-owned coverage after asynchronously acquiring its owner. */
void SUB_FN(detach_query_async)(const jazz_db_Db *db, jazz_db_QueryAttachment attachment) {
    jazz_node_NodeState *owner = jazz_db__db__SharedNodeState__lock(db->node->node);
    SUB_FN(detach_query_using)(db, attachment, detach_async_unsubscribe, owner);
    unlock_node(db);
}

static jz_err SUB_FN(open_subscription)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_types_AuthorSubject author, u32 authorization_mode, bool allow_pending_overlay, jazz_db_SubscriptionStream *out) {
    jazz_db_ReadOpts local_first_opts = opts_clone(&opts);
    local_first_opts.first_load.tag = JAZZ_FL_DELIVER;
    local_first_opts.first_load.timeout_ms = 0;
    /* Boxed so this wrapper adds no inline opener frame to its callers. */
    jz_err e = SUB_FN(open_gated_subscription)(db, prepared, opts, author, authorization_mode, allow_pending_overlay, false, 0, out);
    if (JZ_IS_ERR(e)) { opts_drop(&local_first_opts); return e; }
    /* A remote window that stops being able to answer before it opens is
     * served by the plain local-first read of the same window. */
    if (jazz_db__db__SubscriptionStream__is_remote_window(out)) {
        jazz_db_SubscriptionStream *fallback = JZ_NEW(jazz_db_SubscriptionStream);
        e = SUB_FN(open_gated_subscription)(db, prepared, local_first_opts, author, authorization_mode, allow_pending_overlay, false, 0, fallback);
        if (JZ_IS_ERR(e)) { jz_free(fallback); jazz_db__db__SubscriptionStream__Drop__drop(out); return e; }
        out->window_fallback = fallback;
    } else {
        opts_drop(&local_first_opts);
    }
    return JZ_OK;
}

/* Internal one-shot remote-answer path. */
jz_err SUB_FN(subscribe_remote_answer)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_db_SubscriptionStream *out) {
    opts.first_load.tag = JAZZ_FL_DELIVER;
    opts.first_load.timeout_ms = 0;
    u32 coverage_tier = opts.tier == JAZZ_DB_READ_LOCAL_ONLY ? JAZZ_DUR_LOCAL : JAZZ_DUR_GLOBAL;
    jazz_types_AuthorSubject none = {0};
    return SUB_FN(subscribe_one_shot_with_coverage)(db, prepared, opts, false, none, coverage_tier, out);
}

/* A one-shot server answer may have local-first semantics. */
jz_err SUB_FN(subscribe_one_shot_with_coverage)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, bool has_author, jazz_types_AuthorSubject author, u32 coverage_tier, jazz_db_SubscriptionStream *out) {
    u32 authorization_mode = has_author && coverage_tier < JAZZ_DUR_GLOBAL ? JAZZ_QAM_TRUSTED_SERVING : JAZZ_QAM_CLIENT_LOCAL;
    return SUB_FN(open_gated_subscription)(db, prepared, opts, has_author ? author : db->identity.author, authorization_mode, !has_author, true, coverage_tier, out);
}

static jz_err SUB_FN(validate_prepared_shape_for_registration)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared) {
    jazz_model_ShapeAst *ast = jazz_model__shape__ShapeAst__from_validated(prepared->shape);
    jazz_node_NodeState *node = jazz_db__db__SharedNodeState__lock(db->node->node);
    bool has = false;
    jazz_model_ValidatedQuery *validated = NULL;
    jz_err e = jazz_db__db__validate_shape_ast_for_registration(node, jazz_model__query__ValidatedQuery__shape_id(prepared->shape), ast, &has, (jazz_model_ValidatedQuery *)&validated);
    unlock_node(db);
    jazz_model__shape__ShapeAst__drop(ast);
    return ne(e);
}

typedef struct OpenedUpstreamCoverage { jazz_db_UpstreamCoverageHandleVec handles; bool awaits_initial_authority_response; } OpenedUpstreamCoverage;

static jz_err SUB_FN(open_subscription_upstream_coverage)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_protocol_RegisterShapeOptions opts, jazz_types_AuthorSubject identity, u32 authorization_mode, OpenedUpstreamCoverage *out) {
    jazz_node_NodeState *owner = jazz_db__db__SharedNodeState__lock(db->node->node);
    jazz_node_ActiveSessionClaimsScope *scope;
    jz_err e = jazz_db__db__PreparedQuery__scoped_node(prepared, owner, identity, &scope);
    if (!JZ_IS_ERR(e)) {
        e = ne(NS(ensure_peer_maintained_subscription_view_supported)(jazz_node__node__ActiveSessionClaimsScope__node(scope), shape, binding, opts.tier,
                                                                      identity, &opts.read_view, authorization_mode));
        jazz_node__node__ActiveSessionClaimsScope__drop(scope);
    }
    unlock_node(db);
    bool has_pb = false;
    jazz_db_PolicyBinding pb;
    if (!JZ_IS_ERR(e)) e = jazz_db__db__PreparedQuery__request_policy_binding(prepared, identity, &has_pb, &pb);
    if (JZ_IS_ERR(e)) { jazz_protocol__protocol__RegisterShapeOptions__drop(&opts); return e; }
    jazz_protocol_RegisterShapeOptions key_opts;
    jazz_protocol__protocol__RegisterShapeOptions__clone(&key_opts, &opts);
    jazz_protocol_CoverageKey coverage;
    jazz_db__db__request_coverage_key(shape, binding, key_opts, has_pb ? &pb : NULL, &coverage);
    memset(out, 0, sizeof *out);
    if (jz_btree_contains(db->node->upstream_coverage_refcounts, &coverage)) {
        jazz_protocol_SubscriptionKey *latest = jz_btree_get(db->node->latest_coverage_subscriptions, &coverage);
        if (latest) {
            jazz_protocol_SubscriptionKey subscription = *latest;
            bool inserted;
            jazz_protocol_CoverageKey ck;
            jazz_protocol__protocol__CoverageKey__clone(&ck, &coverage);
            usize *count = jz_btree_entry(db->node->upstream_coverage_refcounts, &ck, &inserted);
            if (!inserted) jazz_protocol__protocol__CoverageKey__drop(&ck);
            *count += 1;
            out->awaits_initial_authority_response = jz_btree_contains(db->node->awaiting_initial_authority_coverage, &coverage);
            jazz_db_UpstreamCoverageHandle handle = { coverage, subscription };
            JZ_VEC_PUSH(out->handles, handle);
            jazz_protocol__protocol__RegisterShapeOptions__drop(&opts);
            policy_binding_drop(has_pb, &pb);
            return JZ_OK;
        }
    }
    jazz_protocol_SubscriptionKey subscription;
    e = SUB_FN(attach_query_shape_binding_with_opts)(db, shape, binding, opts, identity, has_pb, &pb, &subscription);
    policy_binding_drop(has_pb, &pb);
    if (JZ_IS_ERR(e)) { jazz_protocol__protocol__CoverageKey__drop(&coverage); return e; }
    bool inserted;
    jazz_protocol_CoverageKey ck;
    jazz_protocol__protocol__CoverageKey__clone(&ck, &coverage);
    usize *count = jz_btree_entry(db->node->upstream_coverage_refcounts, &ck, &inserted);
    if (!inserted) jazz_protocol__protocol__CoverageKey__drop(&ck);
    *count += 1;
    bool has_live_upstream = false;
    jazz_db_PeerConnection **connections = (jazz_db_PeerConnection **)db->node->connections.ptr;
    for (u32 i = 0; i < db->node->connections.len; i++)
        if (connections[i]->link.tag == JAZZ_LINK_UPSTREAM) { has_live_upstream = true; break; }
    if (has_live_upstream) {
        jazz_protocol_CoverageKey ak;
        jazz_protocol__protocol__CoverageKey__clone(&ak, &coverage);
        if (jz_btree_insert(db->node->awaiting_initial_authority_coverage, &ak, NULL, NULL)) { /* already awaiting */ }
    }
    jazz_db_UpstreamCoverageHandle handle = { coverage, subscription };
    JZ_VEC_PUSH(out->handles, handle);
    out->awaits_initial_authority_response = has_live_upstream;
    return JZ_OK;
}

/* Failed-opening cleanup: the one intentionally ID-based cleanup path. */
typedef struct OpeningCleanup {
    jazz_db_Node *node;
    jazz_db_UpstreamCoverageHandleVec *opening_upstream; /* Rc<RefCell<Vec<..>>> */
    jazz_db_LocalCleanupCell *local_cleanup;            /* Rc<Cell<Option<(u64, SubscriptionId)>>> */
} OpeningCleanup;
static void opening_cleanup_call(void *ctx) {
    OpeningCleanup *c = ctx;
    jazz_db_PendingSubscriptionFinalization cmd;
    memset(&cmd, 0, sizeof cmd);
    cmd.state = NULL;
    cmd.opening_upstream = *c->opening_upstream; /* mem::take */
    memset(c->opening_upstream, 0, sizeof *c->opening_upstream);
    if (c->local_cleanup->has) {                  /* .take() */
        cmd.has_opening_local = true;
        cmd.opening_local_token = c->local_cleanup->token;
        cmd.opening_local_id = c->local_cleanup->id;
        c->local_cleanup->has = false;
    }
    cmd.acknowledgement = NULL;
    NODE(enqueue_subscription_finalization)(c->node, cmd);
}

/* Success cleanup: FnOnce(Option<oneshot::Sender<()>>) -> Option<SubscriptionFinalizationFuture> */
typedef struct StreamCleanup { jazz_db_Node *node; jazz_db_SubscriptionState *state; jazz_db_RcBoolCell *closed; } StreamCleanup;
typedef struct DrainFinalizations { jazz_db_Node *node; } DrainFinalizations;
static bool drain_finalizations_poll(void *self, const jz_waker *cx, void *out) {
    DrainFinalizations *d = self;
    u32 n;
    *(jz_err *)out = NODE(drain_subscription_finalizations)(d->node, &n);
    return true;
}
static void drain_finalizations_drop(void *self) { DrainFinalizations *d = self; jz_rc_release(d->node); jz_free(d); }
static bool stream_cleanup_call(void *ctx, jz_chan *acknowledgement, jazz_db_LocalFuture *out) {
    StreamCleanup *c = ctx;
    c->closed->v = true;
    bool has_ack = acknowledgement != NULL;
    jazz_db_PendingSubscriptionFinalization cmd;
    memset(&cmd, 0, sizeof cmd);
    cmd.state = c->state; c->state = NULL;
    cmd.acknowledgement = acknowledgement;
    NODE(enqueue_subscription_finalization)(c->node, cmd);
    if (!has_ack) return false;
    DrainFinalizations *d = JZ_NEW(DrainFinalizations);
    d->node = jz_rc_clone(c->node);
    *out = (jazz_db_LocalFuture){ d, drain_finalizations_poll, drain_finalizations_drop };
    return true;
}
static void stream_cleanup_drop(void *ctx) {
    StreamCleanup *c = ctx;
    if (c->state) jz_rc_release(c->state);
    jz_rc_release(c->closed);
    jz_rc_release(c->node);
    jz_free(c);
}

static jz_err SUB_FN(open_gated_subscription)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_types_AuthorSubject author, u32 authorization_mode, bool allow_pending_overlay, bool has_coverage_tier, u32 coverage_tier_in, jazz_db_SubscriptionStream *out) {
    u32 coverage_tier = has_coverage_tier ? coverage_tier_in : jazz_db__db__read_coverage_tier(&opts);
    jz_err e = MUT(await_open_schema_for_coverage)(db, coverage_tier);
    if (!JZ_IS_ERR(e)) e = jazz_db__db__ensure_supported_subscription_read_opts(&opts);
    if (!JZ_IS_ERR(e)) e = SUB_FN(validate_prepared_shape_for_registration)(db, prepared);
    if (JZ_IS_ERR(e)) { opts_drop(&opts); return e; }
    bool has_gate = false;
    jazz_db_OpeningGate opening_gate;
    coverage_tier = jazz_db__db__first_load__Db__resolve_first_load(db, prepared, &opts, authorization_mode, coverage_tier, &has_gate, &opening_gate);
    u32 requested_read_tier = coverage_tier;
    /* A non-durable foreground registers Local coverage; while the gate is
     * armed it also holds Global witness coverage for the same read. */
    bool authority_witnessed = opts.tier != JAZZ_DB_READ_LOCAL_ONLY && db->node->upstream_durability_floor == JAZZ_DUR_LOCAL
        && has_gate && opening_gate.route == JAZZ_OR_LOCAL_FIRST;
    if (authority_witnessed && has_gate) opening_gate.witnessed = true;
    u32 read_tier = requested_read_tier;
    bool pending_overlay = allow_pending_overlay && authorization_mode == JAZZ_QAM_CLIENT_LOCAL && opts.tier == JAZZ_DB_READ_LOCAL_FIRST
        && coverage_tier >= JAZZ_DUR_GLOBAL;
    jazz_node_NodeState *owner = jazz_db__db__SharedNodeState__lock(db->node->node);
    jazz_node_ActiveSessionClaimsScope *scope;
    e = jazz_db__db__PreparedQuery__scoped_node(prepared, owner, author, &scope);
    if (JZ_IS_ERR(e)) { unlock_node(db); opts_drop(&opts); return e; }
    jazz_node_NodeState *node = jazz_node__node__ActiveSessionClaimsScope__node(scope);
    jazz_model_ValidatedQuery *local_shape;
    jazz_model_Binding *local_binding;
    e = ne(NS(query_binding_for_link)(node, prepared->shape, prepared->binding, &local_shape, &local_binding));
    if (JZ_IS_ERR(e)) { jazz_node__node__ActiveSessionClaimsScope__drop(scope); unlock_node(db); opts_drop(&opts); return e; }
    /* The subscription opener performs one bounded IVM poll; keep the host
     * scheduler as the cold-storage continuation owner. */
    jz_waker progress_waker;
    bool has_waker = NODE(query_runtime_waker)(db->node, &progress_waker);
    jazz_node_LocalMaintainedViewSubscription *subscription = NULL;
    jazz_node_RelationSnapshot snapshot;
    e = ne(NS(open_maintained_view_subscription_in_authorization_mode_with_waker)(node, local_shape, local_binding, author, read_tier, &opts.read_view, NULL,
                                                                                 authorization_mode, pending_overlay, has_waker ? &progress_waker : NULL,
                                                                                 &subscription, &snapshot));
    u64 local_runtime_token = NS(groove_runtime_token)(node);
    jazz_node__node__ActiveSessionClaimsScope__drop(scope);
    unlock_node(db);
    if (JZ_IS_ERR(e)) {
        jazz_model__query__ValidatedQuery__drop(local_shape);
        jazz_model__query__Binding__drop(local_binding);
        opts_drop(&opts);
        return e;
    }
    groove_SubscriptionId local_subscription_id = LMVS(subscription_id)(subscription);
    jazz_db_LocalCleanupCell *local_subscription_cleanup = jz_rc_new(sizeof(jazz_db_LocalCleanupCell), NULL);
    local_subscription_cleanup->has = true;
    local_subscription_cleanup->token = local_runtime_token;
    local_subscription_cleanup->id = local_subscription_id;
    jazz_db_UpstreamCoverageHandleVec *opening_upstream = jz_rc_new(sizeof(jazz_db_UpstreamCoverageHandleVec), NULL);
    memset(opening_upstream, 0, sizeof *opening_upstream);
    OpeningCleanup *opening_cleanup = JZ_NEW(OpeningCleanup);
    opening_cleanup->node = db->node;
    opening_cleanup->opening_upstream = opening_upstream;
    opening_cleanup->local_cleanup = local_subscription_cleanup;
    jazz_db_CleanupGuard local_cleanup;
    jazz_db__db__CleanupGuard__new(&local_cleanup, opening_cleanup_call, opening_cleanup);

    jazz_model_ValidatedQuery *state_shape = local_shape;
    jazz_model_Binding *state_binding = local_binding;
    bool has_remote_read_tier = false; u32 remote_read_tier = 0;
    bool requires_authority_receipt = false;
    jazz_db_UpstreamCoverageHandleVec upstream_subscription_handles = {0};
    jazz_db_UpstreamCoverageHandleVec authority_witness = {0};
    bool suppress_provisional_opening = false;
    bool has_authority_key = false;
    jazz_protocol_AuthorityResultKey settled_authority_result;
    jazz_db_RelationSnapshotIndex snapshot_index;
    memset(&snapshot_index, 0, sizeof snapshot_index);
    bool snapshot_index_live = false;
    /* This opener may have been waiting for the node mutex while a close
     * attempt closed finalization admission; reject it and let the guard
     * transfer cleanup to the still-live node owner. */
    e = NODE(ensure_subscription_finalization_open)(db->node);
    if (JZ_IS_ERR(e)) goto fail;
    /* Compiler-owned root collectors own the terminal snapshot and positional edits. */
    bool terminal_rows = LMVS(has_root_collector)(subscription);
    /* LocalOnly never sends a query to another node. */
    bool propagates_upstream = opts.tier != JAZZ_DB_READ_LOCAL_ONLY;
    if (propagates_upstream) {
        jazz_protocol_RegisterShapeOptions upstream_opts;
        jazz_protocol_ReadViewSpec rv = read_view_clone(&opts.read_view);
        NODE(upstream_register_shape_options)(db->node, requested_read_tier, &rv, &upstream_opts);
        jazz_model_ValidatedQuery *shape;
        jazz_model_Binding *binding;
        if (upstream_opts.tier == read_tier) {
            shape = jazz_model__query__ValidatedQuery__clone(state_shape);
            binding = jazz_model__query__Binding__clone(state_binding);
        } else {
            jazz_node_NodeState *o = jazz_db__db__SharedNodeState__lock(db->node->node);
            jazz_node_ActiveSessionClaimsScope *sc;
            e = jazz_db__db__PreparedQuery__scoped_node(prepared, o, author, &sc);
            if (!JZ_IS_ERR(e)) {
                /* Binding normalization is tier-independent. */
                e = ne(NS(query_binding_for_link)(jazz_node__node__ActiveSessionClaimsScope__node(sc), prepared->shape, prepared->binding, &shape, &binding));
                jazz_node__node__ActiveSessionClaimsScope__drop(sc);
            }
            unlock_node(db);
            if (JZ_IS_ERR(e)) { jazz_protocol__protocol__RegisterShapeOptions__drop(&upstream_opts); goto fail; }
        }
        jazz_model__query__ValidatedQuery__drop(state_shape);
        jazz_model__query__Binding__drop(state_binding);
        state_shape = jazz_model__query__ValidatedQuery__clone(shape);
        state_binding = jazz_model__query__Binding__clone(binding);
        has_remote_read_tier = true;
        remote_read_tier = upstream_opts.tier;
        /* Global cache possession is never a settlement receipt. */
        requires_authority_receipt = upstream_opts.tier >= JAZZ_DUR_GLOBAL;
        OpenedUpstreamCoverage opened;
        e = SUB_FN(open_subscription_upstream_coverage)(db, prepared, shape, binding, upstream_opts, author, authorization_mode, &opened);
        if (JZ_IS_ERR(e)) { jazz_model__query__ValidatedQuery__drop(shape); jazz_model__query__Binding__drop(binding); goto fail; }
        upstream_subscription_handles = opened.handles;
        JZ_VEC_FOR(jazz_db_UpstreamCoverageHandle, h, upstream_subscription_handles) {
            jazz_db_UpstreamCoverageHandle c;
            jazz_db__db__UpstreamCoverageHandle__clone(&c, h);
            JZ_VEC_PUSH(*opening_upstream, c);
        }
        if (authority_witnessed) {
            jazz_protocol_RegisterShapeOptions witness_opts;
            jazz_protocol_ReadViewSpec wrv = read_view_clone(&opts.read_view);
            NODE(upstream_register_shape_options)(db->node, JAZZ_DUR_GLOBAL, &wrv, &witness_opts);
            OpenedUpstreamCoverage witness;
            e = SUB_FN(open_subscription_upstream_coverage)(db, prepared, shape, binding, witness_opts, author, authorization_mode, &witness);
            if (JZ_IS_ERR(e)) { jazz_model__query__ValidatedQuery__drop(shape); jazz_model__query__Binding__drop(binding); goto fail; }
            JZ_VEC_FOR(jazz_db_UpstreamCoverageHandle, h, witness.handles) {
                jazz_db_UpstreamCoverageHandle c;
                jazz_db__db__UpstreamCoverageHandle__clone(&c, h);
                JZ_VEC_PUSH(*opening_upstream, c);
            }
            authority_witness = witness.handles;
        }
        jazz_model__query__ValidatedQuery__drop(shape);
        jazz_model__query__Binding__drop(binding);
        suppress_provisional_opening = authorization_mode == JAZZ_QAM_CLIENT_LOCAL && requested_read_tier >= JAZZ_DUR_GLOBAL
            && opened.awaits_initial_authority_response && snapshot.root_count == 0 && snapshot.edges.len == 0;
    }
    u32 settled_tier = has_remote_read_tier ? remote_read_tier : read_tier;
    if (has_remote_read_tier) {
        jazz_protocol_RegisterShapeOptions rso;
        jazz_protocol__protocol__RegisterShapeOptions__default(&rso);
        jazz_protocol__protocol__ReadViewSpec__drop(&rso.read_view);
        rso.tier = settled_tier;
        rso.read_view = read_view_clone(&opts.read_view);
        jazz_protocol_BindingViewKey binding_view_key = {
            jazz_model__query__ValidatedQuery__shape_id(state_shape), jazz_model__query__Binding__binding_id(state_binding),
            jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&rso),
        };
        jazz_protocol__protocol__RegisterShapeOptions__drop(&rso);
        const jazz_node_NodeState *n = jazz_db__db__SharedNodeState__lock(db->node->node);
        bool consistent = true;
        JZ_VEC_FOR(jazz_db_UpstreamCoverageHandle, h, upstream_subscription_handles) {
            jazz_protocol_AuthorityResultKey key;
            jz_err ke = NS(authority_result_key_for_subscription)(n, h->subscription, &key);
            if (JZ_IS_ERR(ke)) { jz_err_drop(&ke); continue; }
            if (!jazz_protocol__protocol__SubscriptionKey__eq(&key.binding_view, &binding_view_key)) { jazz_protocol__protocol__AuthorityResultKey__drop(&key); continue; }
            if (!has_authority_key) { settled_authority_result = key; has_authority_key = true; continue; }
            if (!jazz_protocol__protocol__AuthorityResultKey__eq(&key, &settled_authority_result)) consistent = false;
            jazz_protocol__protocol__AuthorityResultKey__drop(&key);
        }
        unlock_node(db);
        if (has_authority_key && !consistent) { jazz_protocol__protocol__AuthorityResultKey__drop(&settled_authority_result); has_authority_key = false; }
    }
    /* `open_maintained...` creates a receiver-local graph but does not install
     * a previously received authority closure: fold it into the public
     * stream owner's snapshot with the same terminal reducer. */
    {
        u32 n_occ = 0;
        const jazz_types_OutputOccurrenceId *occ = LMVS(root_occurrence_ids)(subscription, &n_occ);
        jazz_db__db__RelationSnapshotIndex__with_root_occurrences(&snapshot_index, (jazz_types_OutputOccurrenceId *)occ, n_occ);
        snapshot_index_live = true;
    }
    if (authorization_mode == JAZZ_QAM_CLIENT_LOCAL && has_authority_key) {
        jazz_node_NodeState *n = jazz_db__db__SharedNodeState__lock(db->node->node);
        jz_btree preserve;
        jz_btree_init(&preserve, &jazz_db_occurrence_set_desc);
        bool has_update = false;
        jazz_node_LocalMaintainedViewSubscriptionUpdate update;
        e = ne(NS(drain_local_maintained_view_subscription_preserving_rows_with_waker)(n, subscription, &settled_authority_result, &preserve,
                                                                                     has_waker ? &progress_waker : NULL, &has_update, &update));
        unlock_node(db);
        jz_btree_drop(&preserve);
        if (JZ_IS_ERR(e)) goto fail;
        if (has_update) {
            const jazz_node_TerminalRootLayout *terminal_layout = LMVS(terminal_root_layout)(subscription);
            jazz_db_SubscriptionEvent ignored;
            e = jazz_db__db__apply_maintained_update_to_snapshot(&snapshot, &snapshot_index, update,
                                                                 jazz_model__query__Query__table(jazz_model__query__ValidatedQuery__query(prepared->shape)),
                                                                 read_tier, false, terminal_layout, &ignored);
            if (JZ_IS_ERR(e)) goto fail;
            jazz_db__db__SubscriptionEvent__drop(&ignored);
        }
    }
    bool covered_closure_installed = true;
    {
        const jazz_node_NodeState *n = jazz_db__db__SharedNodeState__lock(db->node->node);
        if (has_authority_key)
            covered_closure_installed = LMVS(has_installed_covered_closure)(subscription, &settled_authority_result,
                                                                            NS(applied_authority_result_generation)(n, &settled_authority_result));
        unlock_node(db);
    }
    bool settled;
    {
        const jazz_node_NodeState *n = jazz_db__db__SharedNodeState__lock(db->node->node);
        settled = jazz_db__db__subscription_is_settled(n, db->node->active_authority_view_receipts, state_shape, state_binding, settled_tier,
                                                       read_view_clone(&opts.read_view), requires_authority_receipt,
                                                       has_authority_key ? &settled_authority_result : NULL)
            && (!LMVS(has_covered_input_sources)(subscription)
                /* An installed closure whose evaluation still waits (e.g. on
                 * large-value chunks) has not produced its rows yet (#3349). */
                || (covered_closure_installed && !NS(covered_receiver_evaluation_pending)(n, subscription)));
        unlock_node(db);
    }
    /* An empty local opening carries no observable result information at a
     * Global request; do not publish it as the first delivery. */
    suppress_provisional_opening |= authorization_mode == JAZZ_QAM_CLIENT_LOCAL && requested_read_tier >= JAZZ_DUR_GLOBAL && has_remote_read_tier
        && !settled && snapshot.root_count == 0 && snapshot.edges.len == 0;
    jz_chan *receiver = jz_chan_new(sizeof(jazz_db_SubscriptionEvent)); /* unbounded() */
    jazz_db_SubscriptionSender sender;
    sender.sender = receiver;
    jz_rc_clone(receiver); /* sender + receiver share the channel */
    sender.publication = jz_rc_new(sizeof(jazz_db_SubscriptionPublication), NULL);
    memset(sender.publication, 0, sizeof *sender.publication);
    jz_btree_init(&sender.publication->unresolved, &jazz_db_occurrence_set_desc);
    sender.publication->has_opening_gate = has_gate;
    if (has_gate) sender.publication->opening_gate = opening_gate;
    sender.publication->remote_window = has_gate && opening_gate.route == JAZZ_OR_REMOTE_WINDOW;
    sender.requested_tier = read_tier;
    /* root_occurrence_ids: snapshot_index.roots.iter().flatten().cloned() */
    jazz_db_OccurrenceVec root_occurrence_ids = {0};
    e = jazz_db__db__snapshot_root_occurrences(&snapshot, &snapshot_index, &root_occurrence_ids);
    jazz_db_OutputRowVec initial_outputs = {0};
    if (!JZ_IS_ERR(e)) e = jazz_db__db__materialize_subscription_terminal_records(&snapshot, &snapshot_index);
    if (!JZ_IS_ERR(e)) e = jazz_db__db__subscription_outputs_with_occurrence_sidecar(&snapshot, root_occurrence_ids.ptr, root_occurrence_ids.len, &initial_outputs);
    if (JZ_IS_ERR(e)) { jazz_db__db__SubscriptionSender__drop(&sender); jz_chan_release(receiver); goto fail; }
    jazz_node_RelationSnapshot state_snapshot;
    jazz_db__db__relation_snapshot_with_delta_slack(&snapshot, &state_snapshot);
    jazz_db__db__RelationSnapshotIndex__drop(&snapshot_index);
    jazz_db__db__RelationSnapshotIndex__with_root_occurrences(&snapshot_index, root_occurrence_ids.ptr, root_occurrence_ids.len);
    jz_btree_drop(&snapshot_index.terminal_records);
    e = LMVS(decoded_terminal_records)(subscription, &snapshot_index.terminal_records);
    if (JZ_IS_ERR(e)) {
        jz_btree_init(&snapshot_index.terminal_records, &jazz_db_occurrence_set_desc);
        jazz_node__node__RelationSnapshot__drop(&state_snapshot);
        jazz_db__db__SubscriptionSender__drop(&sender); jz_chan_release(receiver);
        goto fail;
    }
    bool pending_initial_owner_result = authorization_mode == JAZZ_QAM_CLIENT_LOCAL && read_tier == JAZZ_DUR_LOCAL && propagates_upstream
        && db->node->upstream_durability_floor == JAZZ_DUR_LOCAL;
    /* Even a warm, empty foreground graph is provisional until its owner has answered. */
    bool pending_initial_local_snapshot = pending_initial_owner_result || !LMVS(initial_snapshot_received)(subscription);
    settled = settled && !pending_initial_owner_result;
    jazz_db_RcBoolCell *closed = jz_rc_new(sizeof(jazz_db_RcBoolCell), NULL);
    closed->v = false;
    bool scalar_reconciliation_enabled = read_tier < JAZZ_DUR_GLOBAL && has_remote_read_tier && propagates_upstream
        && jazz_protocol__protocol__ReadViewSpec__is_default(&opts.read_view)
        && jazz_node__node__simple_scalar_exit_query(jazz_model__query__ValidatedQuery__query(state_shape));
    /* A stream that already opens settled rides a coverage that was live
     * before it; only a fresh strict opening can miss a deletion. */
    jazz_db_DeletionReconciliation deletion_reconciliation;
    memset(&deletion_reconciliation, 0, sizeof deletion_reconciliation);
    deletion_reconciliation.tag = (read_tier >= JAZZ_DUR_GLOBAL && !settled && authorization_mode == JAZZ_QAM_CLIENT_LOCAL && has_remote_read_tier
                                   && propagates_upstream && jazz_protocol__protocol__ReadViewSpec__is_default(&opts.read_view)
                                   && jazz_node__node__single_table_scalar_query(jazz_model__query__ValidatedQuery__query(state_shape)))
        ? JAZZ_DR_UNCHECKED : JAZZ_DR_DISABLED;
    jazz_db_SubscriptionState *state = jz_rc_new(sizeof(jazz_db_SubscriptionState), (void (*)(void *))jazz_db__db__SubscriptionState__drop);
    memset(state, 0, sizeof *state);
    state->closed = jz_rc_clone(closed);
    state->terminal_rows = terminal_rows;
    state->deletion_reconciliation = deletion_reconciliation;
    state->scalar_reconciliation_enabled = scalar_reconciliation_enabled;
    state->scalar_authority_revision = 0;
    jazz_db__db__ScalarReconciliation__default(&state->scalar_reconciliation);
    state->kind.shape = state_shape;
    state->kind.binding = state_binding;
    state->kind.maintained_subscription = subscription;
    state->groove_runtime_token = local_runtime_token;
    state->local_subscription_cleanup = jz_rc_clone(local_subscription_cleanup);
    state->upstream_subscription_handles = upstream_subscription_handles;
    state->propagates_upstream = propagates_upstream;
    state->has_request_identity_claims = prepared->has_request_identity_claims;
    if (prepared->has_request_identity_claims) state->request_identity_claims = jazz_db__db__PolicyBinding__clone(&prepared->request_identity_claims);
    state->author = author;
    state->authorization_mode = authorization_mode;
    state->read_tier = read_tier;
    state->pending_overlay = pending_overlay;
    state->has_remote_read_tier = has_remote_read_tier;
    state->remote_read_tier = remote_read_tier;
    state->requires_authority_receipt = requires_authority_receipt;
    state->read_view = read_view_clone(&opts.read_view);
    state->snapshot = state_snapshot;
    state->snapshot_index = snapshot_index;
    state->snapshot_source = JAZZ_SSS_LOCAL_MAINTAINED;
    state->settled = settled;
    state->pending_initial_local_snapshot = pending_initial_local_snapshot;
    state->pending_initial_owner_result = pending_initial_owner_result;
    state->authority_witness = authority_witness;
    state->sender = sender;
    {
        jazz_node_NodeState *n = jazz_db__db__SharedNodeState__lock(db->node->node);
        jazz_db_SubscriptionEvent event = jazz_db__db__SubscriptionEvent__delta(true, read_tier, settled);
        event.publishable = !suppress_provisional_opening && !pending_initial_local_snapshot;
        event.added = initial_outputs;
        bool materialized = false;
        e = jazz_db__db__SubscriptionSender__materialized(&state->sender, n, prepared->shape, &event, &materialized);
        jazz_db_SubscriptionPublicationSnapshot none;
        memset(&none, 0, sizeof none);
        bool published;
        if (!JZ_IS_ERR(e)) e = jazz_db__db__SubscriptionSender__publish(&state->sender, event, false, none, &state->snapshot, &state->snapshot_index, materialized, &published);
        else jazz_db__db__SubscriptionEvent__drop(&event);
        unlock_node(db);
        if (JZ_IS_ERR(e)) {
            /* the guard still owns cleanup; the state's own local handle is released with it */
            state->kind.maintained_subscription = NULL;
            state->upstream_subscription_handles = (jazz_db_UpstreamCoverageHandleVec){0};
            state->authority_witness = (jazz_db_UpstreamCoverageHandleVec){0};
            jz_rc_release(state);
            jz_rc_release(closed);
            jz_chan_release(receiver);
            jz_free(root_occurrence_ids.ptr);
            jazz_node__node__LocalMaintainedViewSubscription__drop(subscription);
            subscription = NULL;
            jz_vec_drop(&upstream_subscription_handles, sizeof(jazz_db_UpstreamCoverageHandle), jazz_db__db__UpstreamCoverageHandle__drop);
            jz_vec_drop(&authority_witness, sizeof(jazz_db_UpstreamCoverageHandle), jazz_db__db__UpstreamCoverageHandle__drop);
            state_shape = NULL; state_binding = NULL;
            snapshot_index_live = false;
            goto fail_after_state;
        }
    }
    jz_free(root_occurrence_ids.ptr);
    JZ_VEC_PUSH(db->node->subscriptions->v, state); /* Rc::downgrade */
    if (has_gate) jazz_db__db__first_load__Db__register_opening_gate(db, state);
    /* On success, replace the guard with one command carrying local and
     * upstream cleanup so Drop never touches the async node mutex. */
    {
        void (*call)(void *); void *ctx;
        jazz_db__db__CleanupGuard__take(&local_cleanup, &call, &ctx);
        jz_free(ctx); /* drop(local_cleanup.take()) */
    }
    jazz_db__db__node_runtime__register_upstream_subscription_owner(db->node->upstream_subscription_owners, state->upstream_subscription_handles.ptr,
                                                                    state->upstream_subscription_handles.len, state);
    StreamCleanup *cleanup = JZ_NEW(StreamCleanup);
    cleanup->node = jz_rc_clone(db->node);
    cleanup->state = jz_rc_clone(state);
    cleanup->closed = closed;
    memset(out, 0, sizeof *out);
    out->receiver = receiver;
    out->state = state;
    out->has_cleanup = true;
    out->cleanup = (jazz_db_SubscriptionCleanup){ stream_cleanup_call, cleanup, stream_cleanup_drop };
    out->finalization_tag = JAZZ_SF_NONE;
    out->terminated = false;
    out->window_fallback = NULL;
    out->serving_fallback = false;
    jz_rc_release(opening_upstream);
    jz_rc_release(local_subscription_cleanup);
    jz_snapshot_drop:;
    jazz_node__node__RelationSnapshot__drop(&snapshot);
    opts_drop(&opts);
    return JZ_OK;

fail:
    if (has_authority_key) jazz_protocol__protocol__AuthorityResultKey__drop(&settled_authority_result);
    if (subscription) jazz_node__node__LocalMaintainedViewSubscription__drop(subscription);
    jz_vec_drop(&upstream_subscription_handles, sizeof(jazz_db_UpstreamCoverageHandle), jazz_db__db__UpstreamCoverageHandle__drop);
    jz_vec_drop(&authority_witness, sizeof(jazz_db_UpstreamCoverageHandle), jazz_db__db__UpstreamCoverageHandle__drop);
    jazz_node__node__RelationSnapshot__drop(&snapshot);
fail_after_state:
    if (snapshot_index_live) jazz_db__db__RelationSnapshotIndex__drop(&snapshot_index);
    if (state_shape) jazz_model__query__ValidatedQuery__drop(state_shape);
    if (state_binding) jazz_model__query__Binding__drop(state_binding);
    jazz_db__db__CleanupGuard__Drop__drop(&local_cleanup); /* enqueues ID-based cleanup */
    jz_free(opening_cleanup);
    jz_rc_release(opening_upstream);
    jz_rc_release(local_subscription_cleanup);
    opts_drop(&opts);
    return e;
}

static jz_err SUB_FN(open_relation_subscription)(const jazz_db_Db *db, const jazz_model_RelationQuery *query, jazz_db_ReadOpts opts, jazz_types_AuthorSubject author, u32 authorization_mode, jazz_db_SubscriptionStream *out) {
    jz_err e = MUT(await_open_schema_for_coverage)(db, jazz_db__db__read_coverage_tier(&opts));
    if (!JZ_IS_ERR(e)) e = jazz_db__db__ensure_supported_subscription_read_opts(&opts);
    if (JZ_IS_ERR(e)) { opts_drop(&opts); return e; }
    jazz_model_Query *q;
    e = jazz_model__query__relation_resolution__relation_query_to_query(query, &q);
    if (JZ_IS_ERR(e)) { opts_drop(&opts); return jazz_node__node__api_error__Error__from_query_error(e); }
    jazz_db_PreparedQuery prepared;
    e = jazz_db__db__reads__Db__prepare_query(db, q, &prepared);
    jazz_model__query__Query__drop(q);
    if (JZ_IS_ERR(e)) { opts_drop(&opts); return e; }
    e = SUB_FN(open_subscription)(db, &prepared, opts, author, authorization_mode, true, out);
    jazz_db__db__PreparedQuery__drop(&prepared);
    return e;
}
