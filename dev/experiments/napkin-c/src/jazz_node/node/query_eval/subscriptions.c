/* Port of crates/jazz/layers/node/src/node/query_eval/subscriptions.rs
 * Subscription registration, routing, known-state, and reset lifecycle.
 *
 * This module owns the control-plane state that maps shapes and bindings to
 * durable view identities. Query compilation, maintained terminal reduction,
 * and row materialization remain separate stages. */
#include "jazz_node/node/node_qe_b.h"

#define SUBS(name) jazz_node__node__query_eval__subscriptions__NodeState__##name

static void SUBS(record_shape_options)(jazz_node_NodeState *self, jazz_model_ShapeId shape_id, jazz_protocol_RegisterShapeOptions opts);
static jz_err SUBS(retain_validated_shape_registration)(jazz_node_NodeState *self, jazz_model_ShapeId shape_id, jazz_protocol_ShapeAst ast, bool has_shape, jazz_model_ValidatedQuery shape);
static void SUBS(reclaim_shape_if_unowned)(jazz_node_NodeState *self, jazz_model_ShapeId shape_id);
static jz_err SUBS(drain_parked_binding_deltas_for_shape)(jazz_node_NodeState *self, jazz_model_ShapeId shape_id);
static jz_err SUBS(apply_known_shape_subscribe)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, jazz_protocol_Subscribe subscribe, bool has_policy_binding, jazz_protocol_PolicyBindingKey policy_binding);
static void SUBS(remove_parked_binding_usage)(jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, const jazz_protocol_PolicyBindingKey *policy_binding);
static void SUBS(apply_unsubscribe_registered_binding)(jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, jazz_node_RegisteredBindingUsageKey registered_key);
static bool SUBS(registered_binding_resolves_to_binding_view_key)(const jazz_node_NodeState *self, jazz_protocol_BindingViewKey binding_view_key);
static bool SUBS(registered_binding_resolves_to_authority_result_key)(const jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key);
static bool SUBS(unique_registered_binding_usage_key)(const jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, jazz_node_RegisteredBindingUsageKey *out);
static bool SUBS(unique_registered_binding_entry)(const jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, const jazz_node_RegisteredBindingUsageKey **key, const jazz_node_RegisteredBinding **binding);
static jz_err SUBS(canonical_whole_table_binding_view_key)(const jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, bool *has, jazz_protocol_BindingViewKey *out);
static jz_err jazz_node__node__query_eval__subscriptions__subscribe_binding_view_key(const jazz_model_ValidatedQuery *shape, const jazz_protocol_Subscribe *subscribe, jazz_protocol_BindingViewKey *out);
static jazz_node_RegisteredBindingUsageKey jazz_node__node__query_eval__subscriptions__registered_binding_usage_key(jazz_protocol_SubscriptionKey subscription, bool has_policy_binding, jazz_protocol_PolicyBindingKey policy_binding);

static bool opt_policy_eq(bool ha, const jazz_protocol_PolicyBindingKey *a, bool hb, const jazz_protocol_PolicyBindingKey *b)
{
    if (ha != hb) return false;
    return !ha || jazz_protocol_PolicyBindingKey_eq(a, b);
}

jz_err SUBS(register_shape_with_options)(jazz_node_NodeState *self, jazz_model_ShapeId shape_id, jazz_protocol_ShapeAst ast, jazz_protocol_RegisterShapeOptions opts)
{
    bool has_shape;
    jazz_model_ValidatedQuery shape;
    jz_err e = SUBS(validate_shape_ast_for_registration)(self, shape_id, &ast, &has_shape, &shape);
    if (JZ_IS_ERR(e)) {
        jazz_protocol_ShapeAst_drop(&ast);
        jazz_protocol_RegisterShapeOptions_drop(&opts);
        return e;
    }
    SUBS(record_shape_options)(self, shape_id, opts);
    JZ_TRY(SUBS(retain_validated_shape_registration)(self, shape_id, ast, has_shape, shape));
    jz_btree_insert(&self->query.locally_registered_shapes, &shape_id, NULL, NULL);
    return JZ_OK;
}

typedef struct { u64 peer; u32 count; } CountPeerCtx;

/* Retain a peer shape together with the exact read-view/compiler options
 * carried by its registration.  A `SubscriptionKey` names this option
 * identity, so shape ownership alone cannot reconstruct it later. */
jz_err SUBS(register_shape_for_peer_with_options)(jazz_node_NodeState *self, u64 peer, jazz_model_ShapeId shape_id, jazz_protocol_ShapeAst ast, jazz_protocol_RegisterShapeOptions opts)
{
    jz_err e = JZ_OK;
    const jazz_protocol_ShapeAst *existing = jz_btree_get(&self->parking.parked_shape_registrations, &shape_id);
    if (existing && !jazz_protocol_ShapeAst_eq(existing, &ast)) {
        e = JZN_INVALID_STORED("conflicting parked shape registration");
        goto fail;
    }
    bool has_shape;
    jazz_model_ValidatedQuery shape;
    e = SUBS(validate_shape_ast_for_registration)(self, shape_id, &ast, &has_shape, &shape);
    if (JZ_IS_ERR(e)) goto fail;
    const jz_btree *owners = jz_btree_get(&self->query.peer_shape_owners, &shape_id);
    bool already_owned = owners && jz_btree_contains(owners, &peer);
    if (!already_owned) {
        usize peer_shape_count = 0;
        const jazz_model_ShapeId *k;
        const jz_btree *v;
        JZ_BTREE_FOR(&self->query.peer_shape_owners, k, v) {
            (void)k;
            if (jz_btree_contains(v, &peer)) peer_shape_count++;
        }
        if (peer_shape_count >= JZ_MAX_SHAPE_REGISTRATIONS_PER_PEER) {
            e = JZN_UNSUPPORTED_SYNC("peer shape registration limit exceeded");
            goto fail_shape;
        }
        if (!jz_btree_contains(&self->query.peer_shape_owners, &shape_id) &&
            self->query.peer_shape_owners.len >= JZ_MAX_RETAINED_PEER_SHAPES) {
            e = JZN_UNSUPPORTED_SYNC("global shape registration limit exceeded");
            goto fail_shape;
        }
    }

    SUBS(record_shape_options)(self, shape_id, opts);
    JZ_TRY(SUBS(retain_validated_shape_registration)(self, shape_id, ast, has_shape, shape));
    bool inserted;
    jz_btree *set = jz_btree_entry(&self->query.peer_shape_owners, &shape_id, &inserted);
    if (inserted) jz_btree_init(set, &jazz_node_U64_SET_DESC);
    jz_btree_insert(set, &peer, NULL, NULL);
    return JZ_OK;
fail_shape:
    if (has_shape) jazz_model_ValidatedQuery_drop(&shape);
fail:
    jazz_protocol_ShapeAst_drop(&ast);
    jazz_protocol_RegisterShapeOptions_drop(&opts);
    return e;
}

static void SUBS(record_shape_options)(jazz_node_NodeState *self, jazz_model_ShapeId shape_id, jazz_protocol_RegisterShapeOptions opts)
{
    /* Legacy/default subscriptions carry `ReadViewKey::default()` rather
     * than recomputing the options key. Preserve the received options at
     * that wire identity as well; this is an alias, not reconstruction. */
    jazz_protocol_RegisterShapeOptions def = jazz_protocol_RegisterShapeOptions_default();
    bool is_default = jazz_protocol_RegisterShapeOptions_eq(&opts, &def);
    jazz_protocol_RegisterShapeOptions_drop(&def);
    if (is_default) {
        jazz_node_ShapeReadViewKey k = {shape_id, jazz_protocol_ReadViewKey_default()};
        jazz_protocol_RegisterShapeOptions c;
        jazz_protocol_RegisterShapeOptions_clone(&c, &opts);
        jazz_protocol_RegisterShapeOptions old;
        if (jz_btree_insert(&self->query.registered_shape_options, &k, &c, &old))
            jazz_protocol_RegisterShapeOptions_drop(&old);
    }
    jazz_node_ShapeReadViewKey k = {shape_id, jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&opts)};
    jazz_protocol_RegisterShapeOptions old;
    if (jz_btree_insert(&self->query.registered_shape_options, &k, &opts, &old))
        jazz_protocol_RegisterShapeOptions_drop(&old);
}

static jz_err SUBS(retain_validated_shape_registration)(jazz_node_NodeState *self, jazz_model_ShapeId shape_id, jazz_protocol_ShapeAst ast, bool has_shape, jazz_model_ValidatedQuery shape)
{
    if (!has_shape) {
        jazz_protocol_ShapeAst old;
        if (!jz_btree_insert(&self->parking.parked_shape_registrations, &shape_id, &ast, &old))
            self->sync_metrics.parked_catalogue_shapes += 1;
        else
            jazz_protocol_ShapeAst_drop(&old);
        return JZ_OK;
    }
    jazz_protocol_ShapeAst_drop(&ast);
    jazz_model_ValidatedQuery old;
    if (jz_btree_insert(&self->query.registered_shapes, &shape_id, &shape, &old))
        jazz_model_ValidatedQuery_drop(&old);
    return SUBS(drain_parked_binding_deltas_for_shape)(self, shape_id);
}

jz_err SUBS(validate_shape_ast_for_registration)(const jazz_node_NodeState *self, jazz_model_ShapeId shape_id, const jazz_protocol_ShapeAst *ast, bool *has, jazz_model_ValidatedQuery *out)
{
    *has = false;
    if (ast->version != JZ_SHAPE_AST_VERSION)
        return JZN_INVALID_STORED("unsupported query AST version");
    const jazz_model_JazzSchema *schema;
    if (jz_uuid_eq(&ast->schema_version, &self->catalogue.local_schema_version_id)) {
        schema = &self->catalogue.schema;
    } else {
        const jazz_protocol_SchemaVersion *sv = jz_btree_get(&self->catalogue.catalogue_schemas, &ast->schema_version);
        if (!sv) return JZ_OK;
        schema = &sv->schema;
    }
    jazz_model_ValidatedQuery shape;
    switch (ast->body_tag) {
    case JZ_SHAPE_BODY_QUERY:
        JZ_TRY(jazz_model__query__builders__Query__validate_with_schema_version(&ast->query, schema, ast->schema_version, &shape));
        break;
    case JZ_SHAPE_BODY_RELATION: {
        jazz_model_Query q;
        JZ_TRY(jazz_model__query__relation_resolution__relation_query_to_query(&ast->relation, &q));
        jz_err e = jazz_model__query__builders__Query__validate_with_schema_version(&q, schema, ast->schema_version, &shape);
        jazz_model_Query_drop(&q);
        JZ_TRY(e);
        break;
    }
    default: JZ_UNREACHABLE();
    }
    jazz_model_ShapeId sid = jazz_model__query__validation__ValidatedQuery__shape_id(&shape);
    if (!jz_uuid_eq(&sid, &shape_id)) {
        jazz_model_ValidatedQuery_drop(&shape);
        return JZN_INVALID_STORED("shape id does not match AST");
    }
    *out = shape;
    *has = true;
    return JZ_OK;
}

typedef struct { jazz_model_ShapeId shape_id; jazz_protocol_ShapeAst ast; } ReadyShape;
static void ready_shape_drop(void *p) { jazz_protocol_ShapeAst_drop(&((ReadyShape *)p)->ast); }

jz_err SUBS(drain_parked_shape_registrations)(jazz_node_NodeState *self)
{
    JZ_VEC(ReadyShape) ready = {0};
    const jazz_model_ShapeId *sid;
    const jazz_protocol_ShapeAst *ast;
    JZ_BTREE_FOR(&self->parking.parked_shape_registrations, sid, ast) {
        if (jz_btree_contains(&self->catalogue.catalogue_schemas, &ast->schema_version)) {
            ReadyShape r;
            r.shape_id = *sid;
            jazz_protocol_ShapeAst_clone(&r.ast, ast);
            JZ_VEC_PUSH(ready, r);
        }
    }
    jz_err e = JZ_OK;
    u32 i = 0;
    for (; i < ready.len; i++) {
        ReadyShape *r = JZ_VEC_AT(ready, i);
        bool has;
        jazz_model_ValidatedQuery shape;
        e = SUBS(validate_shape_ast_for_registration)(self, r->shape_id, &r->ast, &has, &shape);
        if (JZ_IS_ERR(e)) break;
        if (!has) { e = JZN_INVALID_STORED("catalogued shape registration remained unresolved"); break; }
        jazz_protocol_ShapeAst old;
        if (jz_btree_remove(&self->parking.parked_shape_registrations, &r->shape_id, NULL, &old))
            jazz_protocol_ShapeAst_drop(&old);
        self->sync_metrics.parked_catalogue_shapes_resolved += 1;
        jazz_protocol_ShapeAst moved = r->ast;
        memset(&r->ast, 0, sizeof r->ast);
        e = SUBS(retain_validated_shape_registration)(self, r->shape_id, moved, true, shape);
        if (JZ_IS_ERR(e)) { i++; break; }
    }
    for (; i < ready.len; i++) ready_shape_drop(JZ_VEC_AT(ready, i));
    jz_vec_drop(&ready, sizeof(ReadyShape), NULL);
    return e;
}

void SUBS(release_shape_for_peer)(jazz_node_NodeState *self, u64 peer, jazz_model_ShapeId shape_id)
{
    bool became_unowned;
    {
        jz_btree *owners = jz_btree_get(&self->query.peer_shape_owners, &shape_id);
        if (!owners) return;
        if (!jz_btree_remove(owners, &peer, NULL, NULL)) return;
        became_unowned = owners->len == 0;
    }
    if (!became_unowned) return;
    jz_btree_remove(&self->query.peer_shape_owners, &shape_id, NULL, NULL);
    SUBS(reclaim_shape_if_unowned)(self, shape_id);
}

typedef struct { u64 peer; jz_btree *newly_unowned; } ReleasePeerCtx;
static bool release_peer_keep(const void *k, void *v, void *ctx)
{
    ReleasePeerCtx *c = ctx;
    jz_btree *owners = v;
    jz_btree_remove(owners, &c->peer, NULL, NULL);
    if (owners->len == 0) {
        jz_btree_insert(c->newly_unowned, k, NULL, NULL);
        return false;
    }
    return true;
}

void SUBS(release_shapes_for_peer)(jazz_node_NodeState *self, u64 peer)
{
    static const jz_btree_desc SHAPE_SET = {sizeof(jazz_model_ShapeId), 0, jz_cmp_uuid, NULL, NULL};
    jz_btree newly_unowned;
    jz_btree_init(&newly_unowned, &SHAPE_SET);
    ReleasePeerCtx c = {peer, &newly_unowned};
    jz_btree_retain(&self->query.peer_shape_owners, release_peer_keep, &c);
    jazz_model_ShapeId sid;
    while (jz_btree_pop_first(&newly_unowned, &sid, NULL))
        SUBS(reclaim_shape_if_unowned)(self, sid);
    jz_btree_drop(&newly_unowned);
}

/* Retain a shape while this node is actively serving it to one concrete
 * downstream publication.  Unlike `register_shape_for_peer`, this owner
 * is local process state and must be retired with the `PeerState` that
 * created it. */
jz_err SUBS(register_query_subscription_for_peer)(jazz_node_NodeState *self, u64 publication_owner, jazz_model_ShapeId shape_id, jazz_protocol_ShapeAst ast, jazz_protocol_RegisterShapeOptions opts, jazz_protocol_Subscribe subscribe, jazz_protocol_PolicyBindingKey policy_binding)
{
    bool has_shape;
    jazz_model_ValidatedQuery shape;
    jz_err e = SUBS(validate_shape_ast_for_registration)(self, shape_id, &ast, &has_shape, &shape);
    if (JZ_IS_ERR(e)) goto fail;
    if (!jazz_protocol_ReadViewKey_eq(jazz_protocol__protocol__RegisterShapeOptions__read_view_key(&opts), subscribe.subscription.read_view)) {
        if (has_shape) jazz_model_ValidatedQuery_drop(&shape);
        e = JZN_INVALID_STORED("served subscription read-view does not match its registration options");
        goto fail;
    }
    SUBS(record_shape_options)(self, shape_id, opts);
    e = SUBS(retain_validated_shape_registration)(self, shape_id, ast, has_shape, shape);
    if (JZ_IS_ERR(e)) {
        jazz_protocol_Subscribe_drop(&subscribe);
        jazz_protocol_PolicyBindingKey_drop(&policy_binding);
        return e;
    }
    jazz_protocol_SubscriptionKey subscription = subscribe.subscription;
    jazz_protocol_PolicyBindingKey pb_clone;
    jazz_protocol_PolicyBindingKey_clone(&pb_clone, &policy_binding);
    e = SUBS(apply_subscribe_with_admitted_policy_binding)(self, subscribe, pb_clone);
    if (JZ_IS_ERR(e)) { jazz_protocol_PolicyBindingKey_drop(&policy_binding); return e; }

    bool inserted;
    jz_btree *shape_owners = jz_btree_entry(&self->query.outbound_shape_owners, &shape_id, &inserted);
    if (inserted) jz_btree_init(shape_owners, &jazz_node_OWNER_SUBSCRIPTION_SET_DESC);
    jazz_node_OwnerSubscription os = {publication_owner, subscription};
    jz_btree_insert(shape_owners, &os, NULL, NULL);

    jazz_node_SubscriptionPolicyKey spk = {subscription, policy_binding};
    jz_btree *binding_owners = jz_btree_entry(&self->query.outbound_binding_owners, &spk, &inserted);
    if (inserted) jz_btree_init(binding_owners, &jazz_node_U64_SET_DESC);
    else jazz_protocol_PolicyBindingKey_drop(&spk.policy_binding);
    jz_btree_insert(binding_owners, &publication_owner, NULL, NULL);
    return JZ_OK;
fail:
    jazz_protocol_ShapeAst_drop(&ast);
    jazz_protocol_RegisterShapeOptions_drop(&opts);
    jazz_protocol_Subscribe_drop(&subscribe);
    jazz_protocol_PolicyBindingKey_drop(&policy_binding);
    return e;
}

/* Release one served publication's ownership.  A shape is reclaimed only
 * after the final local registration, inbound peer registration, and
 * outbound publication has gone away. */
void SUBS(release_query_subscription_for_peer)(jazz_node_NodeState *self, u64 publication_owner, jazz_protocol_SubscriptionKey subscription)
{
    const jazz_node_RegisteredBinding *binding = SUBS(unique_registered_binding_for_subscription)(self, subscription);
    if (!binding || !binding->authority_result_key.has_policy_binding) return;
    jazz_protocol_PolicyBindingKey policy_binding;
    jazz_protocol_PolicyBindingKey_clone(&policy_binding, &binding->authority_result_key.policy_binding);
    SUBS(release_query_subscription_for_peer_with_admitted_policy_binding)(self, publication_owner, subscription, policy_binding);
}

/* Release one served usage under the immutable policy scope that admitted
 * it. A peer may share its wire handle with another scope, so this is the
 * only valid teardown path for multiplexed serving. */
void SUBS(release_query_subscription_for_peer_with_admitted_policy_binding)(jazz_node_NodeState *self, u64 publication_owner, jazz_protocol_SubscriptionKey subscription, jazz_protocol_PolicyBindingKey policy_binding)
{
    jazz_model_ShapeId shape_id = subscription.shape_id;
    jz_btree *shape_owners = jz_btree_get(&self->query.outbound_shape_owners, &shape_id);
    jazz_node_OwnerSubscription os = {publication_owner, subscription};
    if (!shape_owners || !jz_btree_remove(shape_owners, &os, NULL, NULL)) {
        jazz_protocol_PolicyBindingKey_drop(&policy_binding);
        return;
    }
    bool became_unowned = shape_owners->len == 0;
    if (became_unowned)
        jz_btree_remove(&self->query.outbound_shape_owners, &shape_id, NULL, NULL);
    jazz_node_SubscriptionPolicyKey spk = {subscription, policy_binding};
    jz_btree *owners = jz_btree_get(&self->query.outbound_binding_owners, &spk);
    bool binding_became_unowned = false;
    if (owners) {
        jz_btree_remove(owners, &publication_owner, NULL, NULL);
        binding_became_unowned = owners->len == 0;
    }
    if (binding_became_unowned) {
        jz_btree_remove(&self->query.outbound_binding_owners, &spk, NULL, NULL);
        SUBS(apply_unsubscribe_with_admitted_policy_binding)(self, subscription, policy_binding);
    } else {
        jazz_protocol_PolicyBindingKey_drop(&policy_binding);
    }
    SUBS(reclaim_shape_if_unowned)(self, shape_id);
}

static bool keep_not_shape_rvk(const void *k, void *v, void *ctx)
{
    (void)v;
    return !jz_uuid_eq(&((const jazz_node_ShapeReadViewKey *)k)->shape_id, ctx);
}
/* query_shape_cache key is (ShapeId, BindingId, ..): shape id leads the tuple */
static bool keep_not_shape_prefix(const void *k, void *v, void *ctx)
{
    (void)v;
    return !jz_uuid_eq((const jz_uuid *)k, ctx);
}
static bool keep_not_bv_shape(const void *k, void *v, void *ctx)
{
    (void)v;
    return !jz_uuid_eq(&((const jazz_protocol_BindingViewKey *)k)->shape_id, ctx);
}
static bool keep_not_arkey_shape(const void *k, void *v, void *ctx)
{
    (void)v;
    return !jz_uuid_eq(&((const jazz_protocol_AuthorityResultKey *)k)->binding_view.shape_id, ctx);
}
static bool keep_not_sub_shape(const void *k, void *v, void *ctx)
{
    (void)v;
    return !jz_uuid_eq(&((const jazz_node_SubscriptionPolicyKey *)k)->subscription.shape_id, ctx);
}

static void SUBS(reclaim_shape_if_unowned)(jazz_node_NodeState *self, jazz_model_ShapeId shape_id)
{
    if (jz_btree_contains(&self->query.locally_registered_shapes, &shape_id) ||
        jz_btree_contains(&self->query.peer_shape_owners, &shape_id) ||
        jz_btree_contains(&self->query.outbound_shape_owners, &shape_id))
        return;
    jazz_protocol_ShapeAst ast;
    if (jz_btree_remove(&self->parking.parked_shape_registrations, &shape_id, NULL, &ast)) jazz_protocol_ShapeAst_drop(&ast);
    jazz_node_ParkedSubscriptionVec parked;
    if (jz_btree_remove(&self->parking.parked_binding_deltas, &shape_id, NULL, &parked))
        jz_vec_drop(&parked, sizeof(jazz_node_ParkedSubscription), jazz_node_ParkedSubscription_drop);
    jazz_model_ValidatedQuery shape;
    if (jz_btree_remove(&self->query.registered_shapes, &shape_id, NULL, &shape)) jazz_model_ValidatedQuery_drop(&shape);
    jz_btree_retain(&self->query.registered_shape_options, keep_not_shape_rvk, &shape_id);
    jz_btree bindings;
    if (jz_btree_remove(&self->query.registered_bindings, &shape_id, NULL, &bindings)) jz_btree_drop(&bindings);
    jz_btree_retain(&self->query.query_shape_cache, keep_not_shape_prefix, &shape_id);
    jz_btree_clear(&self->query.compiled_query_program_cache);
    jazz_engine__query_engine__QueryProgramTemplateCache__clear(self->query.query_program_templates);
    { u32 el = self->query.supported_query_program_requests.elem;
      jz_deque_drop(&self->query.supported_query_program_requests, jazz_node_SupportedQueryProgram_drop);
      jz_deque_init(&self->query.supported_query_program_requests, el); }
    jz_btree_retain(&self->query.applied_view_update_generations, keep_not_bv_shape, &shape_id);
    jz_btree_retain(&self->query.authority_results, keep_not_arkey_shape, &shape_id);

    jz_btree_retain(&self->query.outbound_binding_owners, keep_not_sub_shape, &shape_id);
}

jz_err SUBS(apply_subscribe)(jazz_node_NodeState *self, jazz_protocol_Subscribe subscribe)
{
    bool has_policy_binding = subscribe.has_delegated_session;
    jazz_protocol_PolicyBindingKey policy_binding;
    memset(&policy_binding, 0, sizeof policy_binding);
    if (has_policy_binding)
        policy_binding = jazz_protocol__protocol__PolicyBindingKey__from_delegated_session(&subscribe.delegated_session);
    const jazz_model_ValidatedQuery *registered = jz_btree_get(&self->query.registered_shapes, &subscribe.shape_id);
    if (!registered) {
        bool inserted;
        jazz_node_ParkedSubscriptionVec *bucket = jz_btree_entry(&self->parking.parked_binding_deltas, &subscribe.shape_id, &inserted);
        if (inserted) memset(bucket, 0, sizeof *bucket);
        jazz_node_ParkedSubscription p = {subscribe, has_policy_binding, policy_binding};
        JZ_VEC_PUSH(*bucket, p);
        return JZ_OK;
    }
    jazz_model_ValidatedQuery shape;
    jazz_model_ValidatedQuery_clone(&shape, registered);
    jz_err e = SUBS(apply_known_shape_subscribe)(self, &shape, subscribe, has_policy_binding, policy_binding);
    jazz_model_ValidatedQuery_drop(&shape);
    return e;
}

/* Register a server-admitted subscriber usage under the exact immutable
 * policy snapshot selected by its connection. This is deliberately a
 * privileged server-side entry point: direct clients do not put their
 * claims on the wire, and a relay's transport identity must never be used
 * to recreate the subscriber's policy scope later. */
jz_err SUBS(apply_subscribe_with_admitted_policy_binding)(jazz_node_NodeState *self, jazz_protocol_Subscribe subscribe, jazz_protocol_PolicyBindingKey policy_binding)
{
    jz_err e;
    if (subscribe.has_delegated_session) {
        jazz_protocol_PolicyBindingKey d = jazz_protocol__protocol__PolicyBindingKey__from_delegated_session(&subscribe.delegated_session);
        bool eq = jazz_protocol_PolicyBindingKey_eq(&d, &policy_binding);
        jazz_protocol_PolicyBindingKey_drop(&d);
        if (!eq) {
            e = JZN_INVALID_STORED("delegated subscription policy binding disagrees with admission");
            goto fail;
        }
    }
    const jazz_model_ValidatedQuery *registered = jz_btree_get(&self->query.registered_shapes, &subscribe.shape_id);
    if (!registered) {
        /* Server admission always registers the shape before this call.
         * Parking only the wire Subscribe would lose the immutable policy
         * binding and could later reopen it under an unrelated reader. */
        e = JZN_INVALID_STORED("admitted subscription referenced unregistered shape");
        goto fail;
    }
    jazz_model_ValidatedQuery shape;
    jazz_model_ValidatedQuery_clone(&shape, registered);
    e = SUBS(apply_known_shape_subscribe)(self, &shape, subscribe, true, policy_binding);
    jazz_model_ValidatedQuery_drop(&shape);
    return e;
fail:
    jazz_protocol_Subscribe_drop(&subscribe);
    jazz_protocol_PolicyBindingKey_drop(&policy_binding);
    return e;
}

/* Whether this receiver's installed supporting set for `subscription` is
 * exactly `revision`. */
bool SUBS(holds_supporting_revision)(const jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, const u8 revision[16])
{
    jazz_protocol_AuthorityResultKey key;
    jz_err e = SUBS(authority_result_key_for_subscription)(self, subscription, &key);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    const jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, &key);
    bool r = state && state->has_supporting_revision && memcmp(state->supporting_revision, revision, 16) == 0;
    jazz_protocol_AuthorityResultKey_drop(&key);
    return r;
}

/* Stop declaring a watermark for `subscription` until a new complete set is
 * installed; the next subscribe asks for one. */
void SUBS(forget_supporting_revision)(jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription)
{
    jazz_protocol_AuthorityResultKey key;
    jz_err e = SUBS(authority_result_key_for_subscription)(self, subscription, &key);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return; }
    jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, &key);
    if (state) state->has_supporting_revision = false;
    jazz_protocol_AuthorityResultKey_drop(&key);
}

/* Declare nothing for this view on its next open, so the serving peer resends
 * every row with its body. Used when an update named a held row whose body
 * this receiver no longer has. */
void SUBS(forget_declared_known_state)(jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription)
{
    jazz_protocol_AuthorityResultKey key;
    jz_err e = SUBS(authority_result_key_for_subscription)(self, subscription, &key);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return; }
    jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, &key);
    if (state) {
        state->has_supporting_revision = false;
        state->has_settled_through = false;
    }
    jazz_protocol_AuthorityResultKey_drop(&key);
}

/* Return the exact policy-scoped durable identity fixed at subscription
 * admission. Wire updates contain only the usage handle and cannot choose or
 * reconstruct this identity themselves. */
jz_err SUBS(authority_result_key_for_subscription)(const jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, jazz_protocol_AuthorityResultKey *out)
{
    const jazz_node_RegisteredBinding *binding = SUBS(unique_registered_binding_for_subscription)(self, subscription);
    if (binding) {
        jazz_protocol_AuthorityResultKey_clone(out, &binding->authority_result_key);
        return JZ_OK;
    }
    bool has;
    jazz_protocol_BindingViewKey bv;
    jz_err e = SUBS(canonical_whole_table_binding_view_key)(self, subscription, &has, &bv);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); has = false; }
    if (has) {
        *out = jazz_protocol__protocol__AuthorityResultKey__unscoped(bv);
        return JZ_OK;
    }
    return JZN_INVALID_STORED("subscription referenced unregistered binding");
}

/* Return an authority receipt only when the canonical binding view names
 * exactly one policy scope.  This is a compatibility lookup for callers that
 * do not yet carry a usage subscription; it intentionally refuses to guess in
 * a multiplexed relay. New relay paths must carry the exact
 * `AuthorityResultKey` from registration instead. */
bool SUBS(unique_authority_result_key_for_binding_view)(const jazz_node_NodeState *self, jazz_protocol_BindingViewKey binding_view, jazz_protocol_AuthorityResultKey *out)
{
    const jazz_protocol_AuthorityResultKey *first = NULL;
    const jazz_protocol_AuthorityResultKey *k;
    const void *v;
    JZ_BTREE_FOR(&self->query.authority_results, k, v) {
        (void)v;
        if (!jazz_protocol_BindingViewKey_eq(&k->binding_view, &binding_view)) continue;
        if (first) return false;
        first = k;
    }
    if (!first) return false;
    jazz_protocol_AuthorityResultKey_clone(out, first);
    return true;
}

static jz_err SUBS(drain_parked_binding_deltas_for_shape)(jazz_node_NodeState *self, jazz_model_ShapeId shape_id)
{
    jazz_node_ParkedSubscriptionVec deltas;
    if (!jz_btree_remove(&self->parking.parked_binding_deltas, &shape_id, NULL, &deltas))
        return JZ_OK;
    const jazz_model_ValidatedQuery *registered = jz_btree_get(&self->query.registered_shapes, &shape_id);
    if (!registered) {
        jz_btree_insert(&self->parking.parked_binding_deltas, &shape_id, &deltas, NULL);
        return JZ_OK;
    }
    jazz_model_ValidatedQuery shape;
    jazz_model_ValidatedQuery_clone(&shape, registered);
    jz_err e = JZ_OK;
    u32 i = 0;
    for (; i < deltas.len; i++) {
        jazz_node_ParkedSubscription *p = JZ_VEC_AT(deltas, i);
        e = SUBS(apply_known_shape_subscribe)(self, &shape, p->subscribe, p->has_policy_binding, p->policy_binding);
        if (JZ_IS_ERR(e)) { i++; break; }
    }
    for (; i < deltas.len; i++) jazz_node_ParkedSubscription_drop(JZ_VEC_AT(deltas, i));
    jz_vec_drop(&deltas, sizeof(jazz_node_ParkedSubscription), NULL);
    jazz_model_ValidatedQuery_drop(&shape);
    return e;
}

static jz_err SUBS(apply_known_shape_subscribe)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, jazz_protocol_Subscribe subscribe, bool has_policy_binding, jazz_protocol_PolicyBindingKey policy_binding)
{
    jazz_protocol_BindingViewKey binding_view_key;
    jz_err e = jazz_node__node__query_eval__subscriptions__subscribe_binding_view_key(shape, &subscribe, &binding_view_key);
    if (JZ_IS_ERR(e)) {
        jazz_protocol_Subscribe_drop(&subscribe);
        if (has_policy_binding) jazz_protocol_PolicyBindingKey_drop(&policy_binding);
        return e;
    }
    jazz_protocol_AuthorityResultKey authority_result_key = has_policy_binding
        ? jazz_protocol__protocol__AuthorityResultKey__policy_scoped(binding_view_key, policy_binding)
        : jazz_protocol__protocol__AuthorityResultKey__unscoped(binding_view_key);

    jazz_protocol_RegisterShapeOptions options;
    jazz_node_ShapeReadViewKey okey = {subscribe.shape_id, subscribe.subscription.read_view};
    const jazz_protocol_RegisterShapeOptions *found = jz_btree_get(&self->query.registered_shape_options, &okey);
    if (!found && jazz_protocol_ReadViewKey_eq(subscribe.subscription.read_view, jazz_protocol_ReadViewKey_default())) {
        jazz_protocol_RegisterShapeOptions def = jazz_protocol_RegisterShapeOptions_default();
        const jazz_node_ShapeReadViewKey *k;
        const jazz_protocol_RegisterShapeOptions *v;
        JZ_BTREE_FOR(&self->query.registered_shape_options, k, v) {
            if (jz_uuid_eq(&k->shape_id, &subscribe.shape_id) && jazz_protocol_RegisterShapeOptions_eq(v, &def)) {
                found = v;
                break;
            }
        }
        jazz_protocol_RegisterShapeOptions_drop(&def);
    }
    if (!found) {
        jazz_protocol_Subscribe_drop(&subscribe);
        jazz_protocol_AuthorityResultKey_drop(&authority_result_key);
        return JZN_INVALID_STORED("subscription referenced unregistered read-view options");
    }
    jazz_protocol_RegisterShapeOptions_clone(&options, found);

    if (jazz_types__debug_env__covered_input_trace()) {
        jz_str line = jz_str_from("JAZZ_COVERED_INPUT_TRACE stage=subscribe_receipt binding=");
        jazz_protocol_BindingViewKey_debug(&line, &binding_view_key);
        jz_str_push_cstr(&line, " delegated=");
        jazz_protocol_DelegatedSessionBinding_debug(&line, &subscribe.delegated_session, subscribe.has_delegated_session);
        jz_str_push_cstr(&line, " authority=");
        jazz_protocol_AuthorityResultKey_debug(&line, &authority_result_key);
        jz_js_log(1, line.ptr, line.len); /* eprintln! */
        jz_str_drop(&line);
    }
    /* A new wire subscription needs a fresh authority receipt. It may retire
     * only its own policy-scoped retained local page; another scope with the
     * same public binding remains isolated. */
    jazz_node_RetainedRootWindowSourceSlot retained;
    jazz_protocol_AuthorityResultKey removed_key;
    if (jz_btree_remove(&self->query.retained_root_window_sources, &authority_result_key, &removed_key, &retained)) {
        jazz_protocol_AuthorityResultKey_drop(&removed_key);
        jazz_node_RetainedRootWindowSource_drop(&retained);
        jazz_protocol_AuthorityResultKey c;
        jazz_protocol_AuthorityResultKey_clone(&c, &authority_result_key);
        jazz_node__node__NodeState__clear_settled_result_view(self, c);
    }
    {
        jazz_protocol_AuthorityResultKey c;
        jazz_protocol_AuthorityResultKey_clone(&c, &authority_result_key);
        bool inserted;
        jazz_node_AuthorityResultState *state = jz_btree_entry(&self->query.authority_results, &c, &inserted);
        if (inserted) jazz_node_AuthorityResultState_init(state);
        else jazz_protocol_AuthorityResultKey_drop(&c);
        state->known_state_declared = subscribe.has_known_state;
    }
    jazz_protocol_PolicyBindingKey pb;
    if (authority_result_key.has_policy_binding)
        jazz_protocol_PolicyBindingKey_clone(&pb, &authority_result_key.policy_binding);
    else
        memset(&pb, 0, sizeof pb);
    jazz_node_RegisteredBindingUsageKey registered_key = jazz_node__node__query_eval__subscriptions__registered_binding_usage_key(
        subscribe.subscription, authority_result_key.has_policy_binding, pb);
    bool inserted;
    jz_btree *bindings = jz_btree_entry(&self->query.registered_bindings, &subscribe.shape_id, &inserted);
    if (inserted) jz_btree_init(bindings, &jazz_node_REGISTERED_BINDING_MAP_DESC);
    jazz_node_RegisteredBinding rb;
    rb.values = subscribe.values;
    memset(&subscribe.values, 0, sizeof subscribe.values);
    rb.read_view = subscribe.subscription.read_view;
    rb.binding_view_key = binding_view_key;
    rb.authority_result_key = authority_result_key;
    rb.options = options;
    if (subscribe.has_delegated_session) rb.compiler_identity = subscribe.delegated_session.identity;
    else { rb.compiler_identity.tag = JZ_AUTHOR_SYSTEM; rb.compiler_identity.author = NULL; }
    jazz_node_RegisteredBinding old_rb;
    jazz_node_RegisteredBindingUsageKey old_key;
    (void)old_key;
    if (jz_btree_insert(bindings, &registered_key, &rb, &old_rb)) {
        jazz_node_RegisteredBinding_drop(&old_rb);
        jazz_node_RegisteredBindingUsageKey_drop(&registered_key);
    }
    jazz_protocol_Subscribe_drop(&subscribe);
    return JZ_OK;
}

void SUBS(apply_unsubscribe)(jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription)
{
    bool has_parked = false;
    const jazz_protocol_PolicyBindingKey *parked_policy = NULL;
    bool parked_policy_has = false;
    const jazz_node_ParkedSubscriptionVec *parked = jz_btree_get(&self->parking.parked_binding_deltas, &subscription.shape_id);
    if (parked) {
        JZ_VEC_FOR(const jazz_node_ParkedSubscription, p, *parked) {
            if (!jazz_protocol_SubscriptionKey_eq(&p->subscribe.subscription, &subscription)) continue;
            if (has_parked) {
                if (!opt_policy_eq(parked_policy_has, parked_policy, p->has_policy_binding, &p->policy_binding)) {
                    /* A bare wire handle must not choose between policy scopes
                     * that share the same usage-site key. */
                    return;
                }
            } else {
                has_parked = true;
                parked_policy_has = p->has_policy_binding;
                parked_policy = &p->policy_binding;
            }
        }
    }
    if (has_parked) {
        jazz_protocol_PolicyBindingKey c;
        if (parked_policy_has) jazz_protocol_PolicyBindingKey_clone(&c, parked_policy);
        SUBS(remove_parked_binding_usage)(self, subscription, parked_policy_has ? &c : NULL);
        if (parked_policy_has) jazz_protocol_PolicyBindingKey_drop(&c);
    }

    jazz_node_RegisteredBindingUsageKey registered_key;
    if (!SUBS(unique_registered_binding_usage_key)(self, subscription, &registered_key)) {
        /* A bare wire handle is deliberately insufficient to retire an
         * ambiguous multiplexed policy scope. Authenticated serving paths call
         * `apply_unsubscribe_with_admitted_policy_binding` instead. */
        return;
    }
    SUBS(apply_unsubscribe_registered_binding)(self, subscription, registered_key);
}

/* Retire exactly the policy scope selected by authenticated connection
 * admission. This is intentionally internal: a client-controlled Subscribe
 * payload must never select the scope later used for teardown. */
void SUBS(apply_unsubscribe_with_admitted_policy_binding)(jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, jazz_protocol_PolicyBindingKey policy_binding)
{
    SUBS(remove_parked_binding_usage)(self, subscription, &policy_binding);
    SUBS(apply_unsubscribe_registered_binding)(self, subscription,
        jazz_node__node__query_eval__subscriptions__registered_binding_usage_key(subscription, true, policy_binding));
}

typedef struct { jazz_protocol_SubscriptionKey subscription; const jazz_protocol_PolicyBindingKey *policy; } ParkedRemoveCtx;
static bool parked_keep(void *item, void *ctx)
{
    const jazz_node_ParkedSubscription *p = item;
    const ParkedRemoveCtx *c = ctx;
    return !jazz_protocol_SubscriptionKey_eq(&p->subscribe.subscription, &c->subscription) ||
           !opt_policy_eq(p->has_policy_binding, &p->policy_binding, c->policy != NULL, c->policy);
}

static void SUBS(remove_parked_binding_usage)(jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, const jazz_protocol_PolicyBindingKey *policy_binding)
{
    bool remove_bucket = false;
    jazz_node_ParkedSubscriptionVec *parked = jz_btree_get(&self->parking.parked_binding_deltas, &subscription.shape_id);
    if (parked) {
        ParkedRemoveCtx c = {subscription, policy_binding};
        jz_vec_retain(parked, sizeof(jazz_node_ParkedSubscription), parked_keep, &c, jazz_node_ParkedSubscription_drop);
        remove_bucket = parked->len == 0;
    }
    if (remove_bucket) {
        jazz_node_ParkedSubscriptionVec v;
        if (jz_btree_remove(&self->parking.parked_binding_deltas, &subscription.shape_id, NULL, &v))
            jz_vec_drop(&v, sizeof(jazz_node_ParkedSubscription), jazz_node_ParkedSubscription_drop);
    }
}

static void SUBS(apply_unsubscribe_registered_binding)(jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, jazz_node_RegisteredBindingUsageKey registered_key)
{
    const jz_btree *bindings = jz_btree_get(&self->query.registered_bindings, &subscription.shape_id);
    const jazz_node_RegisteredBinding *found = bindings ? jz_btree_get(bindings, &registered_key) : NULL;
    if (!found) { jazz_node_RegisteredBindingUsageKey_drop(&registered_key); return; }
    jazz_protocol_AuthorityResultKey authority_result_key;
    jazz_protocol_AuthorityResultKey_clone(&authority_result_key, &found->authority_result_key);

    bool has_window = false;
    jazz_node_RetainedRootWindowSourceSlot window;
    const jazz_model_ValidatedQuery *shape = jz_btree_get(&self->query.registered_shapes, &subscription.shape_id);
    if (shape && self->authored_commit_durability == JZ_DURABILITY_NONE) {
        jazz_node__node__RetainedRootWindowSource__for_shape(shape, &window);
        if (jazz_node__node__RetainedRootWindowSource__is_bounded(&window)) {
            const jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, &authority_result_key);
            has_window = state && state->source_closure.tag == JZN_ASC_CLAIMED;
        }
        if (!has_window) jazz_node_RetainedRootWindowSource_drop(&window);
    }
    jz_btree *mbindings = jz_btree_get(&self->query.registered_bindings, &subscription.shape_id);
    if (mbindings) {
        jazz_node_RegisteredBindingUsageKey ok;
        jazz_node_RegisteredBinding ov;
        if (jz_btree_remove(mbindings, &registered_key, &ok, &ov)) {
            jazz_node_RegisteredBindingUsageKey_drop(&ok);
            jazz_node_RegisteredBinding_drop(&ov);
        }
    }
    jazz_node_RegisteredBindingUsageKey_drop(&registered_key);
    if (!SUBS(registered_binding_resolves_to_authority_result_key)(self, &authority_result_key)) {
        /* Registered bindings are the receipt ownership record. Once the last
         * downstream usage site releases this exact binding view, revoke its
         * authority-selected membership rather than retaining a browser cache
         * after scope teardown. */
        if (has_window) {
            jazz_node_RetainedRootWindowSourceSlot old;
            if (jz_btree_insert(&self->query.retained_root_window_sources, &authority_result_key, &window, &old))
                jazz_node_RetainedRootWindowSource_drop(&old);
        } else {
            jazz_node__node__NodeState__retire_authority_result_view(self, authority_result_key);
        }
    } else {
        if (has_window) jazz_node_RetainedRootWindowSource_drop(&window);
        jazz_protocol_AuthorityResultKey_drop(&authority_result_key);
    }
}

static bool SUBS(registered_binding_resolves_to_binding_view_key)(const jazz_node_NodeState *self, jazz_protocol_BindingViewKey binding_view_key)
{
    const jz_btree *bindings = jz_btree_get(&self->query.registered_bindings, &binding_view_key.shape_id);
    if (!bindings) return false;
    const void *k;
    const jazz_node_RegisteredBinding *registered;
    JZ_BTREE_FOR(bindings, k, registered) {
        (void)k;
        if (!jazz_protocol_ReadViewKey_eq(registered->read_view, binding_view_key.read_view)) continue;
        if (jazz_protocol_BindingViewKey_eq(&registered->binding_view_key, &binding_view_key)) return true;
    }
    return false;
}

static bool SUBS(registered_binding_resolves_to_authority_result_key)(const jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key)
{
    const jz_btree *bindings = jz_btree_get(&self->query.registered_bindings, &key->binding_view.shape_id);
    if (!bindings) return false;
    const void *k;
    const jazz_node_RegisteredBinding *registered;
    JZ_BTREE_FOR(bindings, k, registered) {
        (void)k;
        if (jazz_protocol_AuthorityResultKey_eq(&registered->authority_result_key, key)) return true;
    }
    return false;
}

typedef JZ_VEC(jazz_protocol_AuthorityResultKey) ArKeyVec;

/* Forget an in-memory authority result that has no live wire owner.
 *
 * Retained membership must not outlive its relay registration owner. An
 * ownerless `RelayAuthoritySession` view cannot satisfy a new downstream
 * usage site: it must first receive a current authoritative reset. Neither
 * membership nor registration ownership survives a process restart. */
bool SUBS(invalidate_ownerless_settled_result_view)(jazz_node_NodeState *self, jazz_protocol_BindingViewKey binding_view_key)
{
    if (SUBS(registered_binding_resolves_to_binding_view_key)(self, binding_view_key)) return false;
    ArKeyVec keys = {0};
    const jazz_protocol_AuthorityResultKey *k;
    const void *v;
    JZ_BTREE_FOR(&self->query.authority_results, k, v) {
        (void)v;
        if (jazz_protocol_BindingViewKey_eq(&k->binding_view, &binding_view_key)) {
            jazz_protocol_AuthorityResultKey c;
            jazz_protocol_AuthorityResultKey_clone(&c, k);
            JZ_VEC_PUSH(keys, c);
        }
    }
    if (keys.len == 0) { jz_vec_drop(&keys, sizeof(jazz_protocol_AuthorityResultKey), NULL); return false; }
    JZ_VEC_FOR(jazz_protocol_AuthorityResultKey, key, keys)
        jazz_node__node__NodeState__retire_authority_result_view(self, *key);
    jz_vec_drop(&keys, sizeof(jazz_protocol_AuthorityResultKey), NULL);
    return true;
}

/* Retiring an authority connection keeps its input cache but removes the live
 * settlement claim. A relay must await a new receipt before serving a fresh
 * strict downstream usage from that cached source. */
void SUBS(invalidate_authority_result_settlement)(jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key)
{
    jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, key);
    if (state) state->live_settled = false;
}

/* Exact receipt variant for a usage subscription that carries delegated
 * policy context. Unlike the binding-only compatibility facade, this never
 * searches across sessions. */
bool SUBS(has_settled_authority_result)(const jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key)
{
    const jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, key);
    return state && state->live_settled && !jz_btree_contains(&self->query.retained_root_window_sources, key);
}

typedef JZ_VEC(jazz_protocol_SupportingRow) SupportingRowVec;

/* Only exact deletion witnesses already admitted on this selected usage. */
jz_err SUBS(selected_deletion_witnesses)(jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key, jazz_types_SchemaVersionId schema, jz_btree *out)
{
    SupportingRowVec inputs = {0};
    const jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, key);
    if (state) {
        const void *ck;
        const jazz_protocol_SupportingRow *input;
        JZ_BTREE_FOR(&state->covered_input_versions, ck, input) {
            (void)ck;
            if (input->version.layer != JZ_LAYER_DELETION) continue;
            jazz_protocol_SupportingRow c;
            jazz_protocol_SupportingRow_clone(&c, input);
            JZ_VEC_PUSH(inputs, c);
        }
    }
    jz_btree_init(out, &jazz_node_SUPPORTING_ROW_VERSION_ROW_DESC);
    jz_err e = JZ_OK;
    u32 i = 0;
    for (; i < inputs.len; i++) {
        jazz_protocol_SupportingRow *input = JZ_VEC_AT(inputs, i);
        bool has;
        jazz_node_VersionRow version;
        e = jazz_node__node__views__NodeState__covered_input_version(self, input, schema, &has, &version);
        if (JZ_IS_ERR(e)) break;
        if (!has) { e = jazz_node__node__Error_missing_transaction(input->version.tx); break; }
        jz_btree_insert(out, input, &version, NULL);
    }
    if (JZ_IS_ERR(e)) {
        for (; i < inputs.len; i++) jazz_protocol_SupportingRow_drop(JZ_VEC_AT(inputs, i));
        jz_btree_drop(out);
    }
    jz_vec_drop(&inputs, sizeof(jazz_protocol_SupportingRow), NULL);
    return e;
}

/* Root identities suffice only for the unprojected scalar pilot. Policy proof
 * sources must never become reconciliation candidates. */
jz_btree SUBS(scalar_authority_input_rows)(const jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key, const jz_str *table)
{
    jz_btree rows;
    jz_btree_init(&rows, &jazz_node_ROW_UUID_SET_DESC);
    const jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, key);
    if (!state) return rows;
    const void *ck;
    const jazz_protocol_SupportingRow *input;
    JZ_BTREE_FOR(&state->covered_input_versions, ck, input) {
        (void)ck;
        const jazz_model_ValidatedQuery *shape = jz_btree_get(&self->query.registered_shapes, &key->binding_view.shape_id);
        if (!shape) continue;
        jazz_types_GlobalPhysicalTableId pt;
        jz_err e = jazz_node__node__views__NodeState__scope_physical_table((jazz_node_NodeState *)self, shape->schema_version, table, &pt);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); continue; }
        if (jz_uuid_eq(&pt, &input->physical_table))
            jz_btree_insert(&rows, &input->row, NULL, NULL);
    }
    return rows;
}

u64 SUBS(applied_authority_result_generation)(const jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key)
{
    const jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, key);
    return state ? state->applied_view_update_generation : 0;
}

jz_err SUBS(allocate_authoritative_reset_generation)(jazz_node_NodeState *self, u64 *out)
{
    u64 generation = self->next_authoritative_reset_generation;
    if (generation == 0)
        return JZN_INVALID_STORED("authoritative reset generation exhausted");
    if (generation == UINT64_MAX)
        return JZN_INVALID_STORED("authoritative reset generation exhausted");
    self->next_authoritative_reset_generation = generation + 1;
    *out = generation;
    return JZ_OK;
}

/* Generation of the exact authority-selected source frontier, if this usage
 * site has claimed one. This is intentionally not the raw ViewUpdate counter:
 * later incremental receipt frames can advance that counter without replacing
 * the covered source closure. */
bool SUBS(authority_source_closure_generation)(const jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key, u64 *out)
{
    const jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, key);
    if (!state) return false;
    switch (state->source_closure.tag) {
    case JZN_ASC_PENDING: return false;
    case JZN_ASC_CLAIMED: *out = state->source_closure.generation; return true;
    default: JZ_UNREACHABLE();
    }
}

/* Snapshot exact authority receipts whose next publication must be a reset.
 * Snapshotting is intentionally non-destructive: an error or cancellation
 * leaves the same generation pending for the next refresh. */
jz_btree SUBS(snapshot_pending_authoritative_resets)(const jazz_node_NodeState *self)
{
    jz_btree out;
    jz_btree_init(&out, &jazz_node_AUTHORITY_RESULT_GENERATION_DESC);
    const jazz_protocol_AuthorityResultKey *k;
    const jazz_node_AuthorityResultState *state;
    JZ_BTREE_FOR(&self->query.authority_results, k, state) {
        if (!state->has_pending_authoritative_reset) continue;
        jazz_protocol_AuthorityResultKey c;
        jazz_protocol_AuthorityResultKey_clone(&c, k);
        jz_btree_insert(&out, &c, &state->pending_authoritative_reset, NULL);
    }
    return out;
}

/* Acknowledge one exact reset only if no newer reset superseded it.
 *
 * Never recreate a retired result: acknowledgement is a receipt for an
 * existing exact authority key, not an admission path. */
bool SUBS(acknowledge_authoritative_reset)(jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key, u64 generation)
{
    jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, key);
    if (!state) return false;
    if (state->has_pending_authoritative_reset && state->pending_authoritative_reset == generation) {
        state->has_pending_authoritative_reset = false;
        return true;
    }
    return false;
}

bool SUBS(publication_deferred_for_authority_result)(const jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key)
{
    const jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, key);
    return state && state->deferred_publication;
}

bool SUBS(opening_pending_for_authority_result)(const jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key)
{
    const jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, key);
    return state && state->pending_opening;
}

/* Return the settlement watermark for one exact authority result.
 *
 * A binding view can be shared by several delegated policy snapshots on a
 * relay. Publication code that already has a usage-site receipt must use this
 * exact lookup rather than the binding-view compatibility facade, which
 * deliberately refuses to select a sibling scope. */
bool SUBS(settled_through_for_authority_result)(const jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key, jazz_types_GlobalTime *out)
{
    const jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, key);
    if (!state || !state->has_settled_through) return false;
    *out = state->settled_through;
    return true;
}

jz_err SUBS(known_state_declaration_for_subscription)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_protocol_SubscriptionKey subscription, const groove_Value *values, u32 nvalues, jazz_types_AuthorSubject identity, const jazz_node_PolicyBindingParts *policy_binding, bool *has, jazz_protocol_KnownStateDeclaration *out)
{
    (void)values; (void)nvalues;
    *has = false;
    jazz_protocol_BindingViewKey binding_view_key = {
        jazz_model__query__validation__ValidatedQuery__shape_id(shape),
        jazz_model__query__validation__Binding__binding_id(binding),
        subscription.read_view,
    };
    /* This declaration is assembled before the outbound Subscribe is
     * registered locally, so resolve its exact future receipt from the same
     * immutable delegated policy snapshot that will be sent with it. Do not
     * inspect a sibling receipt just because it shares the canonical binding
     * view. */
    jazz_protocol_AuthorityResultKey authority_result_key;
    if (policy_binding) {
        jz_btree claims;
        jz_btree_clone(&claims, policy_binding->claims, NULL, NULL);
        authority_result_key = jazz_protocol__protocol__AuthorityResultKey__policy_scoped(
            binding_view_key, jazz_protocol__protocol__PolicyBindingKey__from_canonical_parts(policy_binding->identity, claims));
    } else {
        authority_result_key = jazz_protocol__protocol__AuthorityResultKey__unscoped(binding_view_key);
    }
    jz_err e = JZ_OK;
    /* A retained local window is not proof of the complete upstream input. */
    if (jz_btree_contains(&self->query.retained_root_window_sources, &authority_result_key))
        goto done;
    /* A reopened receiver restores a row-local view's watermark from its
     * durable record and rebuilds the held set from synced rows. */
    const jazz_node_AuthorityResultState *state = jz_btree_get(&self->query.authority_results, &authority_result_key);
    if (!state || !state->has_settled_through) {
        bool restored;
        e = jazz_node__node__query_eval__subscription_watermarks__NodeState__restore_subscription_watermark(
            self, shape, binding, &authority_result_key, identity, &restored);
        if (JZ_IS_ERR(e)) goto done;
    }
    /* This process's unevicted receipt may deduplicate bodies on reconnect;
     * restart restores neither this cursor nor scope. The serving peer must
     * still send a fresh complete supporting set, and ordinary receipt
     * admission/repair must finish before the query becomes confirmed. */
    state = jz_btree_get(&self->query.authority_results, &authority_result_key);
    if (state && state->has_settled_through) {
        jazz_types_GlobalTime position = state->settled_through;
        bool has_ap = state->has_authorization_progress;
        u64 ap = state->authorization_progress;
        memset(out, 0, sizeof *out);
        /* "Q at W": with an installed, claimed supporting set the serving peer
         * can answer with only the rows that moved past W. */
        if (!state->has_pending_authoritative_reset && state->source_closure.tag == JZN_ASC_CLAIMED &&
            state->has_supporting_revision) {
            out->tag = JZ_KSD_WATERMARK;
            out->position = position;
            out->has_authorization_progress = has_ap;
            out->authorization_progress = ap;
            memcpy(out->supporting_revision, state->supporting_revision, 16);
            *has = true;
            goto done;
        }
        if (has_ap) {
            out->tag = JZ_KSD_FAST_WITH_AUTHORIZATION_PROGRESS;
            out->completeness = JZ_KSC_FAST_CURRENT_MEMBERSHIP;
            out->position = position;
            out->has_authorization_progress = true;
            out->authorization_progress = ap;
        } else {
            out->tag = JZ_KSD_FAST;
            out->completeness = JZ_KSC_FAST_CURRENT_MEMBERSHIP;
            out->position = position;
        }
        *has = true;
        goto done;
    }
    /* Without a watermark there is no known state to declare: rows are
     * identified by (row, row_seq), not by exact version sets, so the serving
     * peer answers with a complete set. */
done:
    jazz_protocol_AuthorityResultKey_drop(&authority_result_key);
    return e;
}

jz_err SUBS(binding_view_key_for_subscription)(const jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, jazz_protocol_BindingViewKey *out)
{
    const jazz_node_RegisteredBinding *registered = SUBS(unique_registered_binding_for_subscription)(self, subscription);
    if (registered) { *out = registered->binding_view_key; return JZ_OK; }
    bool has;
    JZ_TRY(SUBS(canonical_whole_table_binding_view_key)(self, subscription, &has, out));
    if (has) return JZ_OK;
    return JZN_INVALID_STORED("subscription referenced unregistered binding");
}

/* Resolve a wire handle only when one admitted scope owns it. Callers at
 * authenticated ingress use their exact admission binding instead; this
 * compatibility lookup must never choose a sibling scope by arrival order. */
const jazz_node_RegisteredBinding *SUBS(unique_registered_binding_for_subscription)(const jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription)
{
    const jazz_node_RegisteredBindingUsageKey *k;
    const jazz_node_RegisteredBinding *b;
    if (!SUBS(unique_registered_binding_entry)(self, subscription, &k, &b)) return NULL;
    return b;
}

static bool SUBS(unique_registered_binding_usage_key)(const jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, jazz_node_RegisteredBindingUsageKey *out)
{
    const jazz_node_RegisteredBindingUsageKey *k;
    const jazz_node_RegisteredBinding *b;
    if (!SUBS(unique_registered_binding_entry)(self, subscription, &k, &b)) return false;
    jazz_node_RegisteredBindingUsageKey_clone(out, k);
    return true;
}

static bool SUBS(unique_registered_binding_entry)(const jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, const jazz_node_RegisteredBindingUsageKey **key, const jazz_node_RegisteredBinding **binding)
{
    const jz_btree *bindings = jz_btree_get(&self->query.registered_bindings, &subscription.shape_id);
    if (!bindings) return false;
    const jazz_node_RegisteredBindingUsageKey *k;
    const jazz_node_RegisteredBinding *b;
    bool found = false;
    JZ_BTREE_FOR(bindings, k, b) {
        if (!jz_uuid_eq(&k->binding_id, &subscription.binding_id) || !jazz_protocol_ReadViewKey_eq(k->read_view, subscription.read_view))
            continue;
        if (found) return false;
        found = true;
        *key = k;
        *binding = b;
    }
    return found;
}

static jz_err SUBS(canonical_whole_table_binding_view_key)(const jazz_node_NodeState *self, jazz_protocol_SubscriptionKey subscription, bool *has, jazz_protocol_BindingViewKey *out)
{
    *has = false;
    JZ_VEC_FOR(const jazz_model_TableSchema, table, self->catalogue.schema.tables) {
        jazz_protocol_SubscriptionKey k;
        JZ_TRY(jazz_node__node__views__NodeState__whole_table_subscription_key(self, &table->name, &k));
        if (jazz_protocol_SubscriptionKey_eq(&k, &subscription)) {
            *out = jazz_protocol__protocol__BindingViewKey__from_canonical_subscription_key(subscription);
            *has = true;
            return JZ_OK;
        }
    }
    return JZ_OK;
}

/* The binding view a subscribe names: its shape bound to its parameter values,
 * read through its read view. */
static jz_err jazz_node__node__query_eval__subscriptions__subscribe_binding_view_key(const jazz_model_ValidatedQuery *shape, const jazz_protocol_Subscribe *subscribe, jazz_protocol_BindingViewKey *out)
{
    if (subscribe->values.len != shape->params.len)
        return JZN_INVALID_STORED("binding arity mismatch");
    jz_btree value_map;
    jz_btree_init(&value_map, &jazz_node_qeb_STR_VALUE_DESC);
    u32 i = 0;
    const jz_str *name;
    const void *ty;
    JZ_BTREE_FOR(&shape->params, name, ty) {
        (void)ty;
        jz_str n = jz_str_clone(name);
        groove_Value v;
        groove_Value_clone(&v, JZ_VEC_AT(subscribe->values, i));
        jz_btree_insert(&value_map, &n, &v, NULL);
        i++;
    }
    jazz_model_Binding binding;
    JZ_TRY(jazz_model__query__validation__ValidatedQuery__bind(shape, value_map, &binding));
    out->shape_id = subscribe->shape_id;
    out->binding_id = jazz_model__query__validation__Binding__binding_id(&binding);
    out->read_view = subscribe->subscription.read_view;
    jazz_model_Binding_drop(&binding);
    return JZ_OK;
}

static jazz_node_RegisteredBindingUsageKey jazz_node__node__query_eval__subscriptions__registered_binding_usage_key(jazz_protocol_SubscriptionKey subscription, bool has_policy_binding, jazz_protocol_PolicyBindingKey policy_binding)
{
    jazz_node_RegisteredBindingUsageKey k;
    k.binding_id = subscription.binding_id;
    k.read_view = subscription.read_view;
    k.has_policy_binding = has_policy_binding;
    if (has_policy_binding) k.policy_binding = policy_binding;
    else memset(&k.policy_binding, 0, sizeof k.policy_binding);
    return k;
}
