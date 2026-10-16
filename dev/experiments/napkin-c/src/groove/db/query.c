/* db/query.c: port of crates/groove/src/db/query.rs.
 * Subscriptions, prepared shapes, input sources and resident progress over the
 * IVM runtime. async fns are synchronous; Poll -> bool (true = Ready). */
#include "mod.h"

static void noop_wake(void *ctx) { (void)ctx; }
static const jz_waker NOOP_WAKER = { noop_wake, NULL };
static void set_flag_wake(void *ctx) { *(bool *)ctx = true; }

/* Rc<MeteredStorage<'static, StagedWriteOverlay<'static, LayoutStorage>>> as OwnedStorage */
static groove_OwnedStorage owned_read_storage(const groove_Database *db) {
    groove_StagedWriteOverlay *overlay = groove__storage__StagedWriteOverlay__new_owned(jz_rc_clone(db->storage), groove__storage__LayoutStorage__as_dyn(db->storage),
                                                                                         jz_rc_clone(db->resident_writes));
    groove_MeteredStorage *metered = groove__db__MeteredStorage__new_owned(overlay, groove__storage__StagedWriteOverlay__as_dyn(overlay), jz_rc_clone(db->storage_read_metrics));
    return groove__storage__OwnedStorage__new(metered, groove__db__MeteredStorage__as_dyn(metered));
}
/* StagedWriteOverlay::new(&self.storage, &self.resident_writes) + MeteredStorage::new */
typedef struct BorrowedRead { groove_StagedWriteOverlay *overlay; groove_MeteredStorage metered; } BorrowedRead;
static void borrowed_read_open(BorrowedRead *r, const groove_Database *db) {
    r->overlay = groove__storage__StagedWriteOverlay__new(groove__storage__LayoutStorage__as_dyn(db->storage), db->resident_writes);
    r->metered = groove__db__MeteredStorage__new(groove__storage__StagedWriteOverlay__as_dyn(r->overlay), db->storage_read_metrics);
}
static void borrowed_read_close(BorrowedRead *r) {
    groove__db__MeteredStorage__drop(&r->metered);
    groove__storage__StagedWriteOverlay__release(r->overlay);
}

jz_err groove__db__query__Database__describe_template_input(const groove_Database *db, groove_GraphBuilder graph, groove_TemplateGraphInput *out) {
    groove_RecordDescriptor output;
    jz_err e = groove__db__query__Database__graph_output_descriptor(db, &graph, &output);
    if (e.code) { groove__ivm__graph__GraphBuilder__drop(&graph); return e; }
    *out = groove__ivm__template__TemplateGraphInput__new(graph, output);
    return JZ_OK;
}
jz_err groove__db__query__Database__graph_output_descriptor(const groove_Database *db, const groove_GraphBuilder *graph, groove_RecordDescriptor *out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    return groove__ivm__runtime__IvmRuntime__infer_builder_output(db->ivm_runtime, graph, out);
}
groove_InputSourceId groove__db__query__Database__allocate_input_source(groove_Database *db, groove_RecordDescriptor descriptor) {
    return groove__ivm__runtime__IvmRuntime__allocate_input_source(db->ivm_runtime, descriptor);
}

static bool is_input_source_preflight_error(const jz_err *error) {
    return groove__ivm__runtime__IvmRuntimeError__is_record_encoding(error) || error->code == GROOVE_EXT_IVM_FOREIGN_INPUT_SOURCE ||
           error->code == GROOVE_EXT_IVM_INPUT_SOURCE_RETIRED || error->code == GROOVE_EXT_IVM_BINDING_SOURCE_DESCRIPTOR_MISMATCH;
}
static bool is_retire_preflight_error(const jz_err *error) {
    return error->code == GROOVE_EXT_IVM_FOREIGN_INPUT_SOURCE || error->code == GROOVE_EXT_IVM_INPUT_SOURCE_RETIRED;
}
static jz_err finish_input_source_tick(groove_Database *db, jz_err tick_error, groove_TickMetrics *metrics, bool (*is_preflight)(const jz_err *), groove_TickMetrics **out) {
    if (tick_error.code) {
        if (!is_preflight(&tick_error)) db->poisoned = true;
        return tick_error;
    }
    if (db->last_tick_metrics) groove__ivm__runtime__TickMetrics__drop(db->last_tick_metrics);
    db->last_tick_metrics = groove__ivm__runtime__TickMetrics__clone(metrics);
    jz_err e = groove__db__query__Database__drive_resident_progress_now(db);
    if (e.code) {
        db->poisoned = true;
        groove__ivm__runtime__TickMetrics__drop(metrics);
        return e;
    }
    *out = metrics;
    return JZ_OK;
}
static jz_err replace_input_sources_in(groove_Database *db, groove_InputSourceReplacement *items, u32 n, bool detach_cold, groove_TickMetrics **out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    groove_OwnedStorage storage = owned_read_storage(db);
    groove_TickMetrics *tick = NULL;
    jz_err e = groove__ivm__runtime__IvmRuntime__replace_input_sources(db->ivm_runtime, items, n, detach_cold, &storage, &tick);
    groove__storage__OwnedStorage__drop(&storage);
    return finish_input_source_tick(db, e, tick, is_input_source_preflight_error, out);
}
jz_err groove__db__query__Database__replace_input_sources(groove_Database *db, groove_InputSourceReplacement *items, u32 n, groove_TickMetrics **out) {
    return replace_input_sources_in(db, items, n, false, out);
}
jz_err groove__db__query__Database__replace_input_sources_detaching_cold(groove_Database *db, groove_InputSourceReplacement *items, u32 n, groove_TickMetrics **out) {
    return replace_input_sources_in(db, items, n, true, out);
}
static jz_err apply_input_source_deltas_in(groove_Database *db, groove_InputSourceDelta *items, u32 n, bool detach_cold, groove_TickMetrics **out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    groove_OwnedStorage storage = owned_read_storage(db);
    groove_TickMetrics *tick = NULL;
    jz_err e = groove__ivm__runtime__IvmRuntime__apply_input_source_deltas(db->ivm_runtime, items, n, detach_cold, &storage, &tick);
    groove__storage__OwnedStorage__drop(&storage);
    return finish_input_source_tick(db, e, tick, is_input_source_preflight_error, out);
}
jz_err groove__db__query__Database__apply_input_source_deltas(groove_Database *db, groove_InputSourceDelta *items, u32 n, groove_TickMetrics **out) {
    return apply_input_source_deltas_in(db, items, n, false, out);
}
jz_err groove__db__query__Database__apply_input_source_deltas_detaching_cold(groove_Database *db, groove_InputSourceDelta *items, u32 n, groove_TickMetrics **out) {
    return apply_input_source_deltas_in(db, items, n, true, out);
}
jz_err groove__db__query__Database__retire_input_sources(groove_Database *db, const groove_InputSourceId *ids, u32 n, groove_TickMetrics **out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    groove_OwnedStorage storage = owned_read_storage(db);
    groove_TickMetrics *tick = NULL;
    jz_err e = groove__ivm__runtime__IvmRuntime__retire_input_sources(db->ivm_runtime, ids, n, &storage, &tick);
    groove__storage__OwnedStorage__drop(&storage);
    return finish_input_source_tick(db, e, tick, is_retire_preflight_error, out);
}

/* ---- resident progress ---- */
static bool finish_progress(groove_Database *db, bool ready, jz_err progress, jz_err *out) {
    groove__db__commit__Database__refresh_resident_writes(db);
    if (!ready) return false;
    if (progress.code) db->poisoned = true;
    *out = progress;
    return true;
}
static bool poll_resident_progress(groove_Database *db, const jz_waker *cx, jz_err *out) {
    jz_err e = groove__db__commit__Database__ensure_not_poisoned(db);
    if (e.code) { *out = e; return true; }
    jz_err progress = JZ_OK;
    bool ready = groove__ivm__runtime__IvmRuntime__poll_resident_incremental(db->ivm_runtime, cx, &progress);
    return finish_progress(db, ready, progress, out);
}
jz_err groove__db__query__Database__drive_resident_progress_now(groove_Database *db) {
    for (;;) {
        /* Never use a new direct call as an excuse to poll a cold evaluation
         * again. */
        if (!groove__ivm__runtime__IvmRuntime__has_resident_continuation(db->ivm_runtime)) return JZ_OK;
        jz_err result;
        if (poll_resident_progress(db, &NOOP_WAKER, &result)) return result;
        /* A bounded CPU slice that yielded records an explicit resident
         * continuation; storage may self-wake before it is ready. */
        if (!groove__ivm__runtime__IvmRuntime__has_resident_continuation(db->ivm_runtime)) return JZ_OK;
    }
}
jz_err groove__db__query__Database__drain_self_scheduled_resident_progress(groove_Database *db) { return groove__db__query__Database__drive_resident_progress_now(db); }
bool groove__db__query__Database__has_pending_progress(const groove_Database *db) { return groove__ivm__runtime__IvmRuntime__has_pending_incremental(db->ivm_runtime); }
bool groove__db__query__Database__subscription_has_pending_progress(const groove_Database *db, groove_SubscriptionId subscription) {
    return groove__ivm__runtime__IvmRuntime__subscription_has_pending_progress(db->ivm_runtime, subscription);
}
bool groove__db__query__Database__subscription_has_pending_evaluation(const groove_Database *db, groove_SubscriptionId subscription) {
    return groove__ivm__runtime__IvmRuntime__subscription_has_pending_evaluation(db->ivm_runtime, subscription);
}
bool groove__db__query__Database__poll_progress(groove_Database *db, const jz_waker *cx, jz_err *out) {
    jz_err e = groove__db__commit__Database__ensure_not_poisoned(db);
    if (e.code) { *out = e; return true; }
    jz_err progress = JZ_OK;
    bool ready = groove__ivm__runtime__IvmRuntime__poll_pending_incremental(db->ivm_runtime, cx, &progress);
    return finish_progress(db, ready, progress, out);
}
/* std::future::poll_fn(|cx| self.poll_progress(cx)).await: the synchronous
 * port drains ready tasks between polls. */
jz_err groove__db__query__Database__drive_progress(groove_Database *db) {
    for (;;) {
        bool woken = false;
        jz_waker cx = { set_flag_wake, &woken };
        jz_err result;
        if (groove__db__query__Database__poll_progress(db, &cx, &result)) return result;
        jz_task_run_ready();
        if (!woken) jz_panic("database progress cannot make progress");
    }
}
jz_err groove__db__query__Database__drive_ready_progress_with_waker(groove_Database *db, const jz_waker *progress_waker) {
    jz_err result = JZ_OK;
    bool ready;
    if (progress_waker) {
        groove__ivm__runtime__IvmRuntime__retain_owner_progress_waker(db->ivm_runtime, progress_waker);
        ready = groove__db__query__Database__poll_progress(db, progress_waker, &result);
    } else {
        ready = groove__db__query__Database__poll_progress(db, &NOOP_WAKER, &result);
    }
    return ready ? result : JZ_OK;
}
jz_err groove__db__query__Database__drive_ready_progress(groove_Database *db) { return groove__db__query__Database__drive_ready_progress_with_waker(db, NULL); }

/* ---- subscription polling ---- */
static jz_err subscription_event_result(groove_SubscriptionEvent event, groove_PublicationUpdate *out) {
    if (event.tag == GROOVE_SUB_EVENT_UPDATE) { *out = event.update; return JZ_OK; }
    if (event.error_tag == GROOVE_SUB_ERROR_ENDED) {
        jz_err_drop(&event.error);
        return groove__db__Error__unit(GROOVE_DB_SubscriptionEnded);
    }
    return groove__ivm__runtime__SubscriptionError__into_error(event.error_tag, event.error); /* SubscriptionFailed: transparent */
}
typedef bool (*PollNextEvent)(const void *subscription, const jz_waker *cx, groove_SubscriptionEvent *out);
static bool poll_one(const void *s, const jz_waker *cx, groove_SubscriptionEvent *out) { return groove__ivm__runtime__Subscription__poll_next_event(s, cx, out); }
static bool poll_multi(const void *s, const jz_waker *cx, groove_SubscriptionEvent *out) { return groove__ivm__runtime__MultisinkSubscription__poll_next_event(s, cx, out); }
static bool poll_subscription_event(groove_Database *db, const jz_waker *cx, PollNextEvent poll_next_event, const void *subscription, jz_err *err, groove_PublicationUpdate *out) {
    groove_SubscriptionEvent event;
    if (poll_next_event(subscription, cx, &event)) { *err = subscription_event_result(event, out); return true; }
    jz_err progress;
    if (groove__db__query__Database__poll_progress(db, cx, &progress)) {
        if (progress.code) { *err = progress; return true; }
    }
    if (poll_next_event(subscription, cx, &event)) { *err = subscription_event_result(event, out); return true; }
    return false;
}
bool groove__db__query__Database__poll_subscription_with_publication(groove_Database *db, const groove_Subscription *subscription, const jz_waker *cx, jz_err *err,
                                                                     groove_PublicationUpdate *out) {
    return poll_subscription_event(db, cx, poll_one, subscription, err, out);
}
bool groove__db__query__Database__poll_subscription(groove_Database *db, const groove_Subscription *subscription, const jz_waker *cx, jz_err *err, groove_RecordDeltas **out) {
    groove_PublicationUpdate update;
    if (!poll_subscription_event(db, cx, poll_one, subscription, err, &update)) return false;
    if (!err->code) *out = update.deltas;
    return true;
}
bool groove__db__query__Database__poll_multisink_subscription_with_publication(groove_Database *db, const groove_MultisinkSubscription *subscription, const jz_waker *cx,
                                                                               jz_err *err, groove_PublicationUpdate *out) {
    return poll_subscription_event(db, cx, poll_multi, subscription, err, out);
}
bool groove__db__query__Database__poll_multisink_subscription(groove_Database *db, const groove_MultisinkSubscription *subscription, const jz_waker *cx, jz_err *err,
                                                              groove_MultisinkDeltas **out) {
    groove_PublicationUpdate update;
    if (!poll_subscription_event(db, cx, poll_multi, subscription, err, &update)) return false;
    if (!err->code) *out = update.deltas;
    return true;
}
/* poll_fn(...).await for the next_* family */
static jz_err await_subscription(groove_Database *db, PollNextEvent poll_next_event, const void *subscription, groove_PublicationUpdate *out) {
    for (;;) {
        bool woken = false;
        jz_waker cx = { set_flag_wake, &woken };
        jz_err err = JZ_OK;
        if (poll_subscription_event(db, &cx, poll_next_event, subscription, &err, out)) return err;
        jz_task_run_ready();
        if (!woken) jz_panic("subscription cannot make progress");
    }
}
jz_err groove__db__query__Database__next_subscription(groove_Database *db, const groove_Subscription *subscription, groove_RecordDeltas **out) {
    groove_PublicationUpdate update;
    JZ_TRY(await_subscription(db, poll_one, subscription, &update));
    *out = update.deltas;
    return JZ_OK;
}
jz_err groove__db__query__Database__next_subscription_with_publication(groove_Database *db, const groove_Subscription *subscription, groove_PublicationUpdate *out) {
    return await_subscription(db, poll_one, subscription, out);
}
jz_err groove__db__query__Database__next_multisink_subscription(groove_Database *db, const groove_MultisinkSubscription *subscription, groove_MultisinkDeltas **out) {
    groove_PublicationUpdate update;
    JZ_TRY(await_subscription(db, poll_multi, subscription, &update));
    *out = update.deltas;
    return JZ_OK;
}
jz_err groove__db__query__Database__next_multisink_subscription_with_publication(groove_Database *db, const groove_MultisinkSubscription *subscription,
                                                                                 groove_PublicationUpdate *out) {
    return await_subscription(db, poll_multi, subscription, out);
}

/* ---- subscribe ---- */
jz_err groove__db__query__Database__subscribe_one_sink(groove_Database *db, groove_GraphBuilder graph, groove_Subscription **out) {
    jz_err e = groove__db__commit__Database__ensure_not_poisoned(db);
    if (e.code) { groove__ivm__graph__GraphBuilder__drop(&graph); return e; }
    groove_OwnedStorage storage = owned_read_storage(db);
    e = groove__ivm__runtime__IvmRuntime__subscribe_one_sink_with_waker(db->ivm_runtime, graph, &storage, NULL, out);
    groove__storage__OwnedStorage__drop(&storage);
    JZ_TRY(e);
    /* A direct async opening owns the immediately-resident continuation chain,
     * but it must not await a cold read with no durable owner to resume it. */
    return groove__db__query__Database__drive_resident_progress_now(db);
}
jz_err groove__db__query__Database__subscribe_with_lifetime_and_root_values(groove_Database *db, const jz_str *names, groove_GraphBuilder *graphs, u32 n, u8 lifetime,
                                                                            groove_RootIndirectValues root_indirect_values, const jz_waker *progress_waker,
                                                                            groove_MultisinkSubscription **out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    groove_OwnedStorage storage = owned_read_storage(db);
    jz_err e = groove__ivm__runtime__IvmRuntime__subscribe_with_lifetime(db->ivm_runtime, names, graphs, n, &storage, lifetime, root_indirect_values, progress_waker, out);
    groove__storage__OwnedStorage__drop(&storage);
    return e;
}
jz_err groove__db__query__Database__subscribe_with_lifetime(groove_Database *db, const jz_str *names, groove_GraphBuilder *graphs, u32 n, u8 lifetime,
                                                            const jz_waker *progress_waker, groove_MultisinkSubscription **out) {
    groove_RootIndirectValues materialize = { GROOVE_ROOT_MATERIALIZE, NULL };
    return groove__db__query__Database__subscribe_with_lifetime_and_root_values(db, names, graphs, n, lifetime, materialize, progress_waker, out);
}
jz_err groove__db__query__Database__subscribe_with_waker(groove_Database *db, const jz_str *names, groove_GraphBuilder *graphs, u32 n, const jz_waker *progress_waker,
                                                         groove_MultisinkSubscription **out) {
    return groove__db__query__Database__subscribe_with_lifetime(db, names, graphs, n, GROOVE_LIFETIME_RETAINED, progress_waker, out);
}
jz_err groove__db__query__Database__subscribe(groove_Database *db, const jz_str *names, groove_GraphBuilder *graphs, u32 n, groove_MultisinkSubscription **out) {
    JZ_TRY(groove__db__query__Database__subscribe_with_waker(db, names, graphs, n, NULL, out));
    return groove__db__query__Database__drive_resident_progress_now(db);
}
static groove_GraphBuilder take_planned_graph(groove_PlannedQuery *planned) {
    groove_GraphBuilder graph = *planned->graph;
    jz_free(planned->graph);
    planned->graph = NULL;
    return graph;
}
jz_err groove__db__query__Database__subscribe_query(groove_Database *db, const groove_Query *query, groove_Subscription **out) {
    groove_PlannedQuery planned;
    JZ_TRY(groove__ivm__planner__plan_query(query, groove__ivm__runtime__IvmRuntime__schema(db->ivm_runtime), &planned));
    return groove__db__query__Database__subscribe_one_sink(db, take_planned_graph(&planned), out);
}

/* ---- prepared shapes ---- */
static void str_drop_v(void *p) { jz_str_drop(p); }
jz_err groove__db__query__Database__prepare_query(groove_Database *db, const groove_Query *query, groove_PreparedShape *out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    groove_PlannedPreparedShape planned;
    JZ_TRY(groove__ivm__planner__plan_prepared_shape(query, groove__ivm__runtime__IvmRuntime__schema(db->ivm_runtime), &planned));
    u32 n = planned.public_output.len;
    jz_str *names = jz_alloc((n ? n : 1) * sizeof(jz_str));
    groove_ValueType *types = jz_alloc((n ? n : 1) * sizeof(groove_ValueType));
    for (u32 i = 0; i < n; i++) {
        names[i] = jz_str_clone(&planned.public_output.ptr[i].name);
        groove__records__values__ValueType__Clone__clone(&types[i], &planned.public_output.ptr[i].value_type);
    }
    groove_RecordDescriptor output = groove__records__RecordDescriptor__new(names, types, n);
    for (u32 i = 0; i < n; i++) jz_str_drop(&names[i]);
    jz_free(names);
    jz_free(types);
    for (u32 i = 0; i < n; i++) {
        jz_str_drop(&planned.public_output.ptr[i].name);
        groove__records__values__ValueType__drop(&planned.public_output.ptr[i].value_type);
    }
    jz_vec_drop(&planned.public_output, sizeof(groove_PublicOutputField), NULL);
    groove_IvmPreparedShape *shape;
    jz_err e = groove__db__query__Database__prepare_one_sink(db, take_planned_graph(&planned.planned), planned.shape, planned.binding_descriptor,
                                                            planned.output_key_fields.ptr, planned.output_key_fields.len, &shape);
    jz_vec_drop(&planned.output_key_fields, sizeof(jz_str), str_drop_v);
    if (e.code) {
        groove_PreparedShape tmp = { 0, planned.parameters, output };
        groove__db__PreparedShape__drop(&tmp);
        return e;
    }
    out->id = groove__ivm__runtime__PreparedShape__id(shape);
    out->parameters = planned.parameters;
    out->output = output;
    groove__ivm__runtime__PreparedShape__drop(shape);
    return JZ_OK;
}

/* bindings: &[(&str, Value)] */
jz_err groove__db__query__Database__bind(groove_Database *db, const groove_PreparedShape *prepared, const groove_ShapeBinding *bindings, u32 nb, groove_Subscription **out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    groove_ValueVec values = {0};
    jz_vec_reserve(&values, prepared->parameters.len, sizeof(groove_Value));
    for (u32 p = 0; p < prepared->parameters.len; p++) {
        const groove_QueryParameter *parameter = &prepared->parameters.ptr[p];
        u32 matches = 0, last = 0;
        for (u32 b = 0; b < nb; b++)
            if (bindings[b].name.len == parameter->name.len && memcmp(bindings[b].name.ptr, parameter->name.ptr, parameter->name.len) == 0) { matches++; last = b; }
        if (matches == 1) {
            groove_Value v;
            groove__records__values__Value__Clone__clone(&v, &bindings[last].value);
            JZ_VEC_PUSH(values, v);
        } else {
            groove__records__values__ValueVec__drop(&values);
            return groove__db__Error__str(matches == 0 ? GROOVE_DB_MissingParameter : GROOVE_DB_DuplicateParameter, (const char *)parameter->name.ptr, parameter->name.len);
        }
    }
    for (u32 b = 0; b < nb; b++) {
        bool known = false;
        for (u32 p = 0; p < prepared->parameters.len && !known; p++)
            known = bindings[b].name.len == prepared->parameters.ptr[p].name.len &&
                    memcmp(bindings[b].name.ptr, prepared->parameters.ptr[p].name.ptr, bindings[b].name.len) == 0;
        if (!known) {
            groove__records__values__ValueVec__drop(&values);
            return groove__db__Error__str(GROOVE_DB_UnknownParameter, (const char *)bindings[b].name.ptr, bindings[b].name.len);
        }
    }
    groove_OwnedStorage storage = owned_read_storage(db);
    groove_LiveAttach *live = NULL;
    jz_err e = groove__ivm__runtime__IvmRuntime__prepare_live_attach(db->ivm_runtime, prepared->id, values.ptr, values.len, &storage, &live);
    if (!e.code)
        e = groove__ivm__runtime__IvmRuntime__bind_shape_one_sink_with_output_and_waker(db->ivm_runtime, prepared->id, values.ptr, values.len, prepared->output, &storage, NULL,
                                                                                       live, out);
    groove__storage__OwnedStorage__drop(&storage);
    groove__records__values__ValueVec__drop(&values);
    JZ_TRY(e);
    return groove__db__query__Database__drive_resident_progress_now(db);
}

jz_err groove__db__query__Database__prepare_one_sink(groove_Database *db, groove_GraphBuilder graph, jz_str binding_source_shape, groove_RecordDescriptor binding_descriptor,
                                                     const jz_str *output_key_fields, u32 n, groove_IvmPreparedShape **out) {
    jz_err e = groove__db__commit__Database__ensure_not_poisoned(db);
    if (e.code) { groove__ivm__graph__GraphBuilder__drop(&graph); jz_str_drop(&binding_source_shape); return e; }
    jz_str *fields = jz_alloc((n ? n : 1) * sizeof(jz_str));
    for (u32 i = 0; i < n; i++) fields[i] = jz_str_clone(&output_key_fields[i]);
    BorrowedRead r;
    borrowed_read_open(&r, db);
    e = groove__ivm__runtime__IvmRuntime__prepare_one_sink(db->ivm_runtime, graph, binding_source_shape, binding_descriptor, fields, n,
                                                          groove__db__MeteredStorage__as_dyn(&r.metered), out);
    borrowed_read_close(&r);
    jz_free(fields);
    return e;
}
jz_err groove__db__query__Database__prepare_one_sink_with_routing(groove_Database *db, groove_GraphBuilder output_graph, groove_GraphBuilder routing_graph,
                                                                  jz_str binding_source_shape, groove_RecordDescriptor binding_descriptor, const jz_str *routing_key_fields,
                                                                  u32 n, groove_IvmPreparedShape **out) {
    jz_err e = groove__db__commit__Database__ensure_not_poisoned(db);
    if (e.code) {
        groove__ivm__graph__GraphBuilder__drop(&output_graph);
        groove__ivm__graph__GraphBuilder__drop(&routing_graph);
        jz_str_drop(&binding_source_shape);
        return e;
    }
    jz_str *fields = jz_alloc((n ? n : 1) * sizeof(jz_str));
    for (u32 i = 0; i < n; i++) fields[i] = jz_str_clone(&routing_key_fields[i]);
    BorrowedRead r;
    borrowed_read_open(&r, db);
    e = groove__ivm__runtime__IvmRuntime__prepare_one_sink_with_routing(db->ivm_runtime, output_graph, routing_graph, binding_source_shape, binding_descriptor, fields, n,
                                                                       groove__db__MeteredStorage__as_dyn(&r.metered), out);
    borrowed_read_close(&r);
    jz_free(fields);
    return e;
}
jz_err groove__db__query__Database__prepare(groove_Database *db, groove_RoutedMultisinkTerminal **terminals, u32 n, jz_str binding_source_shape,
                                            groove_RecordDescriptor binding_descriptor, groove_IvmPreparedShape **out) {
    jz_err e = groove__db__commit__Database__ensure_not_poisoned(db);
    if (e.code) { jz_str_drop(&binding_source_shape); return e; }
    BorrowedRead r;
    borrowed_read_open(&r, db);
    e = groove__ivm__runtime__IvmRuntime__prepare(db->ivm_runtime, terminals, n, binding_source_shape, binding_descriptor, groove__db__MeteredStorage__as_dyn(&r.metered), out);
    borrowed_read_close(&r);
    return e;
}
jz_err groove__db__query__Database__prepare_shared(groove_Database *db, groove_RoutedMultisinkTerminal **terminals, u32 n, jz_str binding_source_shape,
                                                   groove_RecordDescriptor binding_descriptor, groove_IvmPreparedShape **out) {
    jz_err e = groove__db__commit__Database__ensure_not_poisoned(db);
    if (e.code) { jz_str_drop(&binding_source_shape); return e; }
    BorrowedRead r;
    borrowed_read_open(&r, db);
    e = groove__ivm__runtime__IvmRuntime__prepare_shared(db->ivm_runtime, terminals, n, binding_source_shape, binding_descriptor, groove__db__MeteredStorage__as_dyn(&r.metered),
                                                        out);
    borrowed_read_close(&r);
    return e;
}
void groove__db__query__Database__release_shared_prepared_shape(groove_Database *db, groove_PreparedShapeId shape) {
    groove__ivm__runtime__IvmRuntime__release_shared_prepared_shape(db->ivm_runtime, shape);
}

jz_err groove__db__query__Database__bind_shape_one_sink(groove_Database *db, groove_PreparedShapeId shape, const groove_Value *values, u32 n, groove_Subscription **out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    groove_OwnedStorage storage = owned_read_storage(db);
    groove_LiveAttach *live = NULL;
    jz_err e = groove__ivm__runtime__IvmRuntime__prepare_live_attach(db->ivm_runtime, shape, values, n, &storage, &live);
    if (!e.code) e = groove__ivm__runtime__IvmRuntime__bind_shape_one_sink_with_waker(db->ivm_runtime, shape, values, n, &storage, NULL, live, out);
    groove__storage__OwnedStorage__drop(&storage);
    JZ_TRY(e);
    return groove__db__query__Database__drive_resident_progress_now(db);
}
jz_err groove__db__query__Database__bind_shape_one_sink_with_output(groove_Database *db, groove_PreparedShapeId shape, const groove_Value *values, u32 n,
                                                                    groove_RecordDescriptor public_output, groove_Subscription **out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    groove_OwnedStorage storage = owned_read_storage(db);
    groove_LiveAttach *live = NULL;
    jz_err e = groove__ivm__runtime__IvmRuntime__prepare_live_attach(db->ivm_runtime, shape, values, n, &storage, &live);
    if (!e.code)
        e = groove__ivm__runtime__IvmRuntime__bind_shape_one_sink_with_output_and_waker(db->ivm_runtime, shape, values, n, public_output, &storage, NULL, live, out);
    groove__storage__OwnedStorage__drop(&storage);
    JZ_TRY(e);
    return groove__db__query__Database__drive_resident_progress_now(db);
}
jz_err groove__db__query__Database__bind_shape_with_lifetime_and_root_values(groove_Database *db, groove_PreparedShapeId shape, const groove_Value *values, u32 n, u8 lifetime,
                                                                             groove_RootIndirectValues root_indirect_values, const jz_waker *progress_waker,
                                                                             groove_MultisinkSubscription **out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    groove_OwnedStorage storage = owned_read_storage(db);
    groove_LiveAttach *live = NULL;
    jz_err e = JZ_OK;
    if (lifetime == GROOVE_LIFETIME_RETAINED) e = groove__ivm__runtime__IvmRuntime__prepare_live_attach(db->ivm_runtime, shape, values, n, &storage, &live);
    if (!e.code)
        e = groove__ivm__runtime__IvmRuntime__bind_shape_with_lifetime(db->ivm_runtime, shape, values, n, &storage, lifetime, root_indirect_values, progress_waker, live, out);
    groove__storage__OwnedStorage__drop(&storage);
    return e;
}
jz_err groove__db__query__Database__bind_shape_with_lifetime(groove_Database *db, groove_PreparedShapeId shape, const groove_Value *values, u32 n, u8 lifetime,
                                                             const jz_waker *progress_waker, groove_MultisinkSubscription **out) {
    groove_RootIndirectValues materialize = { GROOVE_ROOT_MATERIALIZE, NULL };
    return groove__db__query__Database__bind_shape_with_lifetime_and_root_values(db, shape, values, n, lifetime, materialize, progress_waker, out);
}
jz_err groove__db__query__Database__bind_shape_with_root_values(groove_Database *db, groove_PreparedShapeId shape, const groove_Value *values, u32 n,
                                                                groove_RootIndirectValues root_indirect_values, groove_MultisinkSubscription **out) {
    /* Physical root values are only valid for a first result: later retained
     * updates arrive materialized and could not retract them. */
    u8 lifetime = root_indirect_values.tag == GROOVE_ROOT_MATERIALIZE ? GROOVE_LIFETIME_RETAINED : GROOVE_LIFETIME_FIRST_RESULT;
    JZ_TRY(groove__db__query__Database__bind_shape_with_lifetime_and_root_values(db, shape, values, n, lifetime, root_indirect_values, NULL, out));
    return groove__db__query__Database__drive_resident_progress_now(db);
}
jz_err groove__db__query__Database__bind_shape(groove_Database *db, groove_PreparedShapeId shape, const groove_Value *values, u32 n, groove_MultisinkSubscription **out) {
    groove_RootIndirectValues materialize = { GROOVE_ROOT_MATERIALIZE, NULL };
    return groove__db__query__Database__bind_shape_with_root_values(db, shape, values, n, materialize, out);
}
jz_err groove__db__query__Database__bind_shape_owned_with_root_values(groove_Database *db, groove_PreparedShapeId shape, const groove_Value *values, u32 n,
                                                                      groove_RootIndirectValues root_indirect_values, const jz_waker *progress_waker,
                                                                      groove_MultisinkSubscription **out) {
    JZ_TRY(groove__db__query__Database__bind_shape_with_lifetime_and_root_values(db, shape, values, n, GROOVE_LIFETIME_RETAINED, root_indirect_values, progress_waker, out));
    groove__ivm__runtime__IvmRuntime__transfer_prepared_shape_to_bindings(db->ivm_runtime, shape);
    return JZ_OK;
}

/* ---- snapshots ---- */
jz_err groove__db__query__Database__query_graph_with_root_values(groove_Database *db, groove_GraphBuilder graph, groove_RootIndirectValues root_indirect_values,
                                                                 groove_RecordDeltas **out) {
    jz_err e = groove__db__commit__Database__ensure_not_poisoned(db);
    if (e.code) { groove__ivm__graph__GraphBuilder__drop(&graph); return e; }
    BorrowedRead r;
    borrowed_read_open(&r, db);
    e = groove__ivm__runtime__IvmRuntime__query_snapshot_with_root_values(db->ivm_runtime, graph, groove__db__MeteredStorage__as_dyn(&r.metered), root_indirect_values, out);
    borrowed_read_close(&r);
    return e;
}
jz_err groove__db__query__Database__query_graph(groove_Database *db, groove_GraphBuilder graph, groove_RecordDeltas **out) {
    groove_RootIndirectValues materialize = { GROOVE_ROOT_MATERIALIZE, NULL };
    return groove__db__query__Database__query_graph_with_root_values(db, graph, materialize, out);
}
jz_err groove__db__query__Database__query(groove_Database *db, const groove_Query *query, groove_RecordDeltas **out) {
    groove_PlannedQuery planned;
    JZ_TRY(groove__ivm__planner__plan_query(query, groove__ivm__runtime__IvmRuntime__schema(db->ivm_runtime), &planned));
    return groove__db__query__Database__query_graph(db, take_planned_graph(&planned), out);
}
jz_err groove__db__query__Database__query_graphs(groove_Database *db, const jz_str *names, groove_GraphBuilder *graphs, u32 n, groove_MultisinkDeltas **out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    BorrowedRead r;
    borrowed_read_open(&r, db);
    jz_err e = groove__ivm__runtime__IvmRuntime__query_snapshots(db->ivm_runtime, names, graphs, n, groove__db__MeteredStorage__as_dyn(&r.metered), out);
    borrowed_read_close(&r);
    return e;
}
bool groove__db__query__Database__unsubscribe(groove_Database *db, groove_SubscriptionId subscription_id) {
    return groove__ivm__runtime__IvmRuntime__unsubscribe(db->ivm_runtime, subscription_id);
}
jz_err groove__db__query__Database__retire_prepared_shape(groove_Database *db, groove_PreparedShapeId shape) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    return groove__ivm__runtime__IvmRuntime__retire_prepared_shape(db->ivm_runtime, shape);
}
jz_err groove__db__query__Database__prune_dropped_subscriptions(groove_Database *db, u32 *out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    return groove__ivm__runtime__IvmRuntime__prune_dropped_subscriptions_with_storage(db->ivm_runtime, groove__storage__LayoutStorage__as_dyn(db->storage), out);
}
