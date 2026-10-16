/* groove::ivm::runtime (mod.rs): IvmRuntime construction and accessors,
 * variant projection cases, IvmRuntimeError Display strings. */
#include "mod.h"

/* ---------- IvmRuntimeError ---------- */
/* thiserror Display text per variant, in declaration order. */
static const char *const IVM_ERR_FMT[] = {
    "runtime persistence outcome is indeterminate; reopen before continuing",
    "aggregate result exceeds its declared numeric width",
    "graph field not found: %.*s",
    "exact graph field reference cannot be represented unambiguously by name: %.*s",
    "graph field index out of bounds: %llu",
    "graph node has unexpected input arity: NodeId(%llu)",
    "graph node is missing input: NodeId(%llu)",
    "graph node not found: NodeId(%llu)",
    "graph contains a dependency cycle at node: NodeId(%llu)",
    "graph output descriptors do not match",
    "enum tag %llu is absent from this projection target",
    "enum tag projection requires an enum value",
    "payload enum tag %llu is absent from this projection target",
    "payload enum projection requires an enum value",
    "index not found: %.*s",
    "invalid persisted index entry: %.*s",
    "intersected index sources currently require prefix scans",
    "candidate-filtered index sources require snapshot row projection and prefix scans",
    "unsupported keyed lookup: %.*s",
    "join key arity mismatch: left=%llu, right=%llu",
    "shape key field not found: %.*s",
    "table has no primary key: %.*s",
    "runtime node state missing: NodeId(%llu)",
    "runtime node state operator mismatch: NodeId(%llu)",
    "runtime state advanced out of order: current=%.*s, next=%.*s",
    "persist node expected key/value bytes",
    "binding sources can only be evaluated through prepared shapes",
    "multisink subscription must have at least one sink",
    "multisink sink already exists: %.*s",
    "multisink sink requires prepare because it contains a binding source: %.*s",
    "routed multisink sink %.*s has %llu route fields, expected %llu",
    "binding source not found: %.*s",
    "binding source descriptor mismatch: %.*s",
    "input source belongs to a different IVM runtime",
    "input source has been retired",
    "duplicate schema version %llu for table %.*s",
    "%.*s", /* RecordEncoding: transparent */
    "recursive node NodeId(%llu) exceeded iteration limit %llu",
    "%.*s", /* Storage: transparent */
    "%.*s", /* Chunk: transparent */
    "%.*s", /* LargeValue: transparent */
    "storage unavailable for durable node",
    "evaluation blocked on a non-resident storage input",
    "streaming checksum window and work budgets must be non-zero",
    "streaming checksum requires a String or Bytes field",
    "subscription shape not found: PreparedShapeId(%llu)",
    "cannot retire prepared shape PreparedShapeId(%llu) while it has active bindings",
    "runtime state is stale: expected=%.*s, actual=%.*s",
    "table not found: %.*s",
    "table already exists: %.*s",
    "field already exists in the live catalogue: %.*s.%.*s",
    "unknown schema version %llu for table %.*s",
    "variant projection not found: %.*s.%.*s",
    "schema-variant table requires a fixed-output projection: %.*s",
    "variant projection output descriptor mismatch: %.*s.%.*s",
    "variant projection case already registered with different semantics: %.*s.%.*s version %llu",
    "variant projection case not found: %.*s.%.*s version %llu",
    "variant projection source descriptor mismatch: %.*s.%.*s version %llu",
    "variant projection enum field not found: %.*s.%.*s.%.*s",
    "variant projection enum output must contain exactly one field: %.*s.%.*s",
    "variant projection field is not an enum: %.*s.%.*s.%.*s",
    "variant projection enum schema does not match fixed output: %.*s.%.*s.%.*s",
    "variant projection payload does not match enum case: %.*s.%.*s.%.*s",
    "recursive enum projection descriptor mismatch at %.*s",
    "enum match field is not an enum: %.*s",
    "enum match payload descriptor does not match its declared case: %.*s",
    "unique index violation: %.*s",
    "unsupported join key",
    "non-monotone recursive delta reached positive-only incremental recursion",
    "nested recursive graphs are not supported in v0",
    "unsupported arg_by graph: %.*s",
    "collect_by is terminal-only and cannot feed another graph node",
    "invalid collect_by descriptor: %.*s",
    "invalid top_by descriptor: %.*s",
    "collect_by expand encountered duplicate output occurrence source ids",
    "unsupported operator",
};

static const char *fmt_of(u32 code) {
    u32 i = code - GROOVE_IVM_ERR_BASE - 1;
    if (i >= sizeof(IVM_ERR_FMT) / sizeof(IVM_ERR_FMT[0])) jz_panic("unknown IvmRuntimeError");
    return IVM_ERR_FMT[i];
}

jz_err groove__ivm__runtime__IvmRuntimeError__simple(u32 code) { return jz_err_new(code, fmt_of(code)); }

jz_err groove__ivm__runtime__IvmRuntimeError__with_str(u32 code, const char *p, u32 n) { return jz_err_fmt(code, fmt_of(code), (int)n, p); }

jz_err groove__ivm__runtime__IvmRuntimeError__with_node(u32 code, groove_NodeId node) { return jz_err_fmt(code, fmt_of(code), (unsigned long long)node); }

jz_err groove__ivm__runtime__IvmRuntimeError__with_usize(u32 code, u64 v) { return jz_err_fmt(code, fmt_of(code), (unsigned long long)v); }

jz_err groove__ivm__runtime__IvmRuntimeError__table_target(u32 code, const jz_str *table, const char *target, u32 tlen) {
    return jz_err_fmt(code, fmt_of(code), (int)table->len, table->ptr, (int)tlen, target);
}

jz_err groove__ivm__runtime__IvmRuntimeError__table_target_version(u32 code, const jz_str *table, const char *target, u32 tlen, u64 version) {
    if (code == GROOVE_IVM_UnknownTableVariant || code == GROOVE_IVM_DuplicateTableVariant)
        return jz_err_fmt(code, fmt_of(code), (unsigned long long)version, (int)table->len, table->ptr);
    return jz_err_fmt(code, fmt_of(code), (int)table->len, table->ptr, (int)tlen, target, (unsigned long long)version);
}

jz_err groove__ivm__runtime__IvmRuntimeError__table_target_field(u32 code, const jz_str *table, const char *target, u32 tlen, const char *field, u32 flen) {
    if (code == GROOVE_IVM_TableFieldAlreadyExists)
        return jz_err_fmt(code, fmt_of(code), (int)table->len, table->ptr, (int)flen, field);
    return jz_err_fmt(code, fmt_of(code), (int)table->len, table->ptr, (int)tlen, target, (int)flen, field);
}

jz_err groove__ivm__runtime__IvmRuntimeError__join_arity(u64 left, u64 right) {
    return jz_err_fmt(GROOVE_IVM_JoinKeyArityMismatch, fmt_of(GROOVE_IVM_JoinKeyArityMismatch), (unsigned long long)left, (unsigned long long)right);
}

jz_err groove__ivm__runtime__IvmRuntimeError__recursive_limit(groove_NodeId node, u64 max_iters) {
    return jz_err_fmt(GROOVE_IVM_RecursiveIterationLimit, fmt_of(GROOVE_IVM_RecursiveIterationLimit), (unsigned long long)node, (unsigned long long)max_iters);
}

jz_err groove__ivm__runtime__IvmRuntimeError__stale(jz_str expected, bool has_actual, jz_str actual) {
    /* actual={actual:?}: Some("..") or None */
    jz_str shown = has_actual ? jz_format("Some(\"%.*s\")", (int)actual.len, actual.ptr) : jz_str_from("None");
    jz_err e = jz_err_fmt(GROOVE_IVM_StaleRuntimeState, fmt_of(GROOVE_IVM_StaleRuntimeState), (int)expected.len, expected.ptr, (int)shown.len, shown.ptr);
    jz_str_drop(&shown);
    jz_str_drop(&expected);
    if (has_actual) jz_str_drop(&actual);
    return e;
}

jz_err groove__ivm__runtime__IvmRuntimeError__out_of_order(jz_str current, jz_str next) {
    jz_err e = jz_err_fmt(GROOVE_IVM_OutOfOrderRuntimeState, fmt_of(GROOVE_IVM_OutOfOrderRuntimeState), (int)current.len, current.ptr, (int)next.len, next.ptr);
    jz_str_drop(&current);
    jz_str_drop(&next);
    return e;
}

jz_err groove__ivm__runtime__IvmRuntimeError__route_arity(const jz_str *sink, u64 expected, u64 actual) {
    return jz_err_fmt(GROOVE_IVM_RoutedMultisinkRouteArityMismatch, fmt_of(GROOVE_IVM_RoutedMultisinkRouteArityMismatch),
                      (int)sink->len, sink->ptr, (unsigned long long)actual, (unsigned long long)expected);
}

/* ---------- variant projections ---------- */
static u64 vp_key_hash(const void *k) {
    const groove_VariantProjectionKey *key = k;
    u64 h = jz_str_hash(&key->table);
    h = jz_hash_combine(h, key->target.tag);
    return jz_hash_combine(h, jz_str_hash(&key->target.name));
}
static bool vp_key_eq(const void *a, const void *b) {
    const groove_VariantProjectionKey *x = a, *y = b;
    return jz_str_eq(&x->table, &y->table) && groove_VariantProjectionTarget_eq(&x->target, &y->target);
}
static void vp_key_drop(void *k) {
    groove_VariantProjectionKey *key = k;
    jz_str_drop(&key->table);
    jz_str_drop(&key->target.name);
}
static void vp_drop_v(void *p) { groove_VariantProjection_drop(p); }
static void vpc_drop_v(void *p) { groove_VariantProjectionCase_drop(p); }
static u64 u32_hash(const void *k) { return jz_hash_u64(*(const u32 *)k); }
static bool u32_eq(const void *a, const void *b) { return *(const u32 *)a == *(const u32 *)b; }
const jz_hmap_desc groove_ivm_rt_VARIANT_PROJECTIONS_MAP = { sizeof(groove_VariantProjectionKey), sizeof(groove_VariantProjection), vp_key_hash, vp_key_eq, vp_key_drop, vp_drop_v };
const jz_hmap_desc groove_ivm_rt_VARIANT_CASES_MAP = { sizeof(u32), sizeof(groove_VariantProjectionCase), u32_hash, u32_eq, NULL, vpc_drop_v };

groove_RecordDescriptor groove__ivm__runtime__VariantProjectionCase__source(const groove_VariantProjectionCase *c) {
    return c->source;
}

/* A raw source case may refresh after live registry evolution only when its
 * projection program is unchanged and its input descriptor advances
 * append-only. */
bool groove__ivm__runtime__VariantProjectionCase__can_refresh_registries_to(const groove_VariantProjectionCase *c, const groove_VariantProjectionCase *next) {
    if (c->tag == groove_VPC_Project && next->tag == groove_VPC_Project)
        return c->omit_unrepresentable_enum_rows == next->omit_unrepresentable_enum_rows &&
               groove_MapProjectOp_eq(&c->project, &next->project) &&
               groove__records__RecordDescriptor__can_evolve_registry_to(c->source, next->source);
    if (c->tag == groove_VPC_Ignore && next->tag == groove_VPC_Ignore)
        return groove__records__RecordDescriptor__can_evolve_registry_to(c->source, next->source);
    return false;
}

void groove_VariantProjectionCase_clone(groove_VariantProjectionCase *d, const groove_VariantProjectionCase *s) {
    *d = *s;
    if (s->tag != groove_VPC_Ignore) groove_MapProjectOp_clone(&d->project, &s->project);
    if (s->raw_projection) d->raw_projection = jz_rc_clone(s->raw_projection);
}

void groove_VariantProjectionCase_drop(groove_VariantProjectionCase *c) {
    if (c->tag != groove_VPC_Ignore) groove_MapProjectOp_drop(&c->project);
    if (c->raw_projection) jz_rc_release(c->raw_projection);
}

/* derive(PartialEq): Arc<PreparedProjection> compares by value, approximated by pointer here */
bool groove_VariantProjectionCase_eq(const groove_VariantProjectionCase *a, const groove_VariantProjectionCase *b) {
    if (a->tag != b->tag || a->source != b->source) return false;
    switch (a->tag) {
    case groove_VPC_Project:
        return groove_MapProjectOp_eq(&a->project, &b->project) && a->raw_projection == b->raw_projection &&
               a->omit_unrepresentable_enum_rows == b->omit_unrepresentable_enum_rows;
    case groove_VPC_Ignore:
        return true;
    default:
        return a->enum_tag == b->enum_tag && a->payload == b->payload && groove_MapProjectOp_eq(&a->project, &b->project) &&
               a->raw_projection == b->raw_projection;
    }
}

void groove_VariantProjection_clone(groove_VariantProjection *d, const groove_VariantProjection *s) {
    d->output = s->output;
    jz_hmap_init(&d->cases, &groove_ivm_rt_VARIANT_CASES_MAP);
    u32 *k; groove_VariantProjectionCase *v;
    JZ_HMAP_FOR(&s->cases, k, v) {
        groove_VariantProjectionCase c; groove_VariantProjectionCase_clone(&c, v);
        jz_hmap_insert(&d->cases, k, &c, NULL);
    }
}

void groove_VariantProjection_drop(groove_VariantProjection *p) { jz_hmap_drop(&p->cases); }

/* ---------- runtime maps ---------- */
static u64 u64_hash(const void *k) { return jz_hash_u64(*(const u64 *)k); }
static bool u64_eq(const void *a, const void *b) { return *(const u64 *)a == *(const u64 *)b; }
static void hmap_drop_v(void *m) { jz_hmap_drop(m); }
static void desc_map_drop_v(void *m) { jz_hmap_drop(m); }
static void arrangement_as_of_drop(void *p) {
    groove_AsOfArrangement *a = p;
    if (a->value) groove__ivm__runtime__join__ArrangementState__drop(a->value);
}
static void deferred_drop_v(void *v) { jz_vec_drop(v, sizeof(void *) * 2, NULL); }

const jz_hmap_desc groove_ivm_rt_U64_SET = { sizeof(u64), 0, u64_hash, u64_eq, NULL, NULL };
const jz_hmap_desc groove_ivm_rt_U32_DESC_MAP = { sizeof(u32), sizeof(groove_RecordDescriptor), u32_hash, u32_eq, NULL, NULL };
const jz_hmap_desc groove_ivm_rt_STR_VARIANT_DESCS_MAP = { sizeof(jz_str), sizeof(jz_hmap), groove_ivm_rt_str_hash_v, groove_ivm_rt_str_eq_v, groove_ivm_rt_str_drop_v, desc_map_drop_v };
static const jz_hmap_desc MULTISINK_MAP = { sizeof(u64), sizeof(void *), u64_hash, u64_eq, NULL, groove_MultisinkSubscriptionState_drop_v };
static const jz_hmap_desc NODE_SUBS_MAP = { sizeof(groove_NodeId), sizeof(jz_hmap), groove_NodeId_hash, groove_NodeId_eq, NULL, hmap_drop_v };
static const jz_hmap_desc SHAPES_MAP = { sizeof(u64), sizeof(void *), u64_hash, u64_eq, NULL, groove_RoutedMultisinkShapeState_drop_v };
static const jz_hmap_desc U64_U64_MAP = { sizeof(u64), sizeof(u64), u64_hash, u64_eq, NULL, NULL };
static const jz_hmap_desc DEFERRED_MAP = { sizeof(u64), sizeof(jz_vec), u64_hash, u64_eq, NULL, deferred_drop_v };
const jz_hmap_desc groove_ivm_rt_OPERATOR_STATES_MAP = { sizeof(groove_OperatorStateKey), sizeof(groove_OperatorState), groove_ScopedKey_hash, groove_ScopedKey_eq, NULL, groove_OperatorState_drop_v };
const jz_hmap_desc groove_ivm_rt_ARRANGEMENT_STATES_MAP = { sizeof(groove_ArrangementKey), sizeof(groove_AsOfArrangement), groove_ScopedKey_hash, groove_ScopedKey_eq, NULL, arrangement_as_of_drop };
const jz_hmap_desc groove_ivm_rt_ARRANGEMENT_KEY_SET = { sizeof(groove_ArrangementKey), 0, groove_ScopedKey_hash, groove_ScopedKey_eq, NULL, NULL };
const jz_hmap_desc groove_ivm_rt_NODE_KEYSET_MAP = { sizeof(groove_NodeId), sizeof(jz_hmap), groove_NodeId_hash, groove_NodeId_eq, NULL, hmap_drop_v };
const jz_hmap_desc groove_ivm_rt_EVAL_MEMO_KEY_SET = { sizeof(groove_EvalMemoKey), 0, groove_EvalMemoKey_hash, groove_EvalMemoKey_eq, NULL, NULL };

/* ---------- IvmRuntime ---------- */
/* Insert one retained hydration memo while keeping its node index and byte
 * budget synchronized. */
void groove__ivm__runtime__IvmRuntime__insert_retained_eval_memo(groove_IvmRuntime *self, groove_EvalMemoKey key, groove_EvalMemoEntry entry) {
    u32 payload_bytes = entry.payload_bytes;
    bool inserted;
    jz_hmap *keys = jz_hmap_entry(&self->eval_memo_keys_by_node, &key.node, &inserted);
    if (inserted) jz_hmap_init(keys, &groove_ivm_rt_EVAL_MEMO_KEY_SET);
    jz_hmap_insert(keys, &key, NULL, NULL);
    groove_EvalMemoEntry previous;
    if (groove__ivm__runtime__evaluation_memo__EvaluationMemo__insert(&self->eval_memo, key, entry, &previous)) {
        self->eval_memo_bytes = self->eval_memo_bytes > previous.payload_bytes ? self->eval_memo_bytes - previous.payload_bytes : 0;
        groove_EvalMemoEntry_drop(&previous);
    }
    u32 sum = self->eval_memo_bytes + payload_bytes;
    self->eval_memo_bytes = sum < self->eval_memo_bytes ? UINT32_MAX : sum;
}

void groove__ivm__runtime__IvmRuntime__remove_retained_eval_memo(groove_IvmRuntime *self, const groove_EvalMemoKey *key) {
    groove_EvalMemoEntry entry;
    if (groove__ivm__runtime__evaluation_memo__EvaluationMemo__remove(&self->eval_memo, key, &entry)) {
        self->eval_memo_bytes = self->eval_memo_bytes > entry.payload_bytes ? self->eval_memo_bytes - entry.payload_bytes : 0;
        groove_EvalMemoEntry_drop(&entry);
    }
    jz_hmap *keys = jz_hmap_get(&self->eval_memo_keys_by_node, &key->node);
    if (keys) {
        jz_hmap_remove(keys, key, NULL, NULL);
        if (keys->len == 0) jz_hmap_remove(&self->eval_memo_keys_by_node, &key->node, NULL, NULL);
    }
}

void groove__ivm__runtime__IvmRuntime__remove_retained_eval_memos_for_nodes(groove_IvmRuntime *self, const jz_hmap *nodes) {
    groove_NodeId *node; void *unused;
    JZ_HMAP_FOR(nodes, node, unused) {
        jz_hmap keys;
        if (!jz_hmap_remove(&self->eval_memo_keys_by_node, node, NULL, &keys)) continue;
        groove_EvalMemoKey *key; void *u2;
        JZ_HMAP_FOR(&keys, key, u2) {
            groove_EvalMemoEntry entry;
            if (groove__ivm__runtime__evaluation_memo__EvaluationMemo__remove(&self->eval_memo, key, &entry)) {
                self->eval_memo_bytes = self->eval_memo_bytes > entry.payload_bytes ? self->eval_memo_bytes - entry.payload_bytes : 0;
                groove_EvalMemoEntry_drop(&entry);
            }
        }
        jz_hmap_drop(&keys);
    }
}

static u64 NEXT_INPUT_SOURCE_RUNTIME_NAMESPACE = 1;

jz_err groove__ivm__runtime__IvmRuntime__new(groove_DatabaseSchema schema, groove_IvmRuntime **out) {
    return groove__ivm__runtime__IvmRuntime__new_with_index_ids(schema, groove__ivm__runtime__index_ids__SharedIndexIds__default(), out);
}

/* Build a runtime whose schema indexes take their durable numeric ids from index_ids. */
jz_err groove__ivm__runtime__IvmRuntime__new_with_index_ids(groove_DatabaseSchema schema, groove_SharedIndexIds index_ids, groove_IvmRuntime **out) {
    groove_IvmRuntime *rt = JZ_NEW(groove_IvmRuntime);
    jz_hmap_init(&rt->table_storage_descriptors, &groove_ivm_rt_STR_DESC_MAP);
    jz_hmap_init(&rt->table_descriptors, &groove_ivm_rt_STR_DESC_MAP);
    jz_hmap_init(&rt->variant_descriptors, &groove_ivm_rt_STR_VARIANT_DESCS_MAP);
    JZ_VEC_FOR(const groove_TableSchema, table, schema.tables) {
        jz_str name = jz_str_clone(&table->name);
        groove_RecordDescriptor d = groove__schema__TableSchema__record_schema(table);
        if (jz_hmap_insert(&rt->table_storage_descriptors, &name, &d, NULL)) jz_str_drop(&name);
    }
    JZ_VEC_FOR(const groove_TableSchema, table, schema.tables) {
        if (groove__schema__TableSchema__has_variants(table)) continue;
        jz_str name = jz_str_clone(&table->name);
        groove_RecordDescriptor d = groove__schema__TableSchema__record_schema(table);
        if (jz_hmap_insert(&rt->table_descriptors, &name, &d, NULL)) jz_str_drop(&name);
    }
    JZ_VEC_FOR(const groove_TableSchema, table, schema.tables) {
        if (!groove__schema__TableSchema__has_variants(table)) continue;
        jz_hmap descriptors; jz_hmap_init(&descriptors, &groove_ivm_rt_U32_DESC_MAP);
        JZ_VEC_FOR(const groove_TableVariant, variant, table->variants) {
            groove_RecordDescriptor d;
            if (groove__schema__TableSchema__record_schema_for_variant(table, variant->tag, &d))
                jz_hmap_insert(&descriptors, &variant->tag, &d, NULL);
        }
        jz_str name = jz_str_clone(&table->name);
        jz_hmap old;
        if (jz_hmap_insert(&rt->variant_descriptors, &name, &descriptors, &old)) jz_hmap_drop(&old);
    }
    rt->schema = schema;
    rt->chunk_provider = groove__chunks__OwnedChunkProvider__default();
    jz_hmap_init(&rt->variant_projections, &groove_ivm_rt_VARIANT_PROJECTIONS_MAP);
    rt->graph = groove__ivm__graph__IvmGraph__new();
    groove__ivm__runtime__typed_projection__ProjectionPlans__default(&rt->projection_plans);
    groove__ivm__runtime__compilation_cache__CompilationCache__default(&rt->compilation_cache);
    jz_hmap_init(&rt->multisink_subscriptions, &MULTISINK_MAP);
    jz_hmap_init(&rt->subscriptions_by_output_node, &NODE_SUBS_MAP);
    rt->pending_incremental = groove__ivm__runtime__runtime_tick__PendingIncrementalEvaluation__default();
    rt->pending_incremental_polling = false;
    rt->has_owner_progress_waker = false;
    rt->has_owner_fanout_waker = false;
    rt->ephemeral_graph_gc_pending = false;
    jz_hmap_init(&rt->gc_candidates, &groove_ivm_rt_NODE_SET);
    jz_hmap_init(&rt->operator_states, &groove_ivm_rt_OPERATOR_STATES_MAP);
    jz_hmap_init(&rt->arrangement_states, &groove_ivm_rt_ARRANGEMENT_STATES_MAP);
    jz_hmap_init(&rt->arrangement_keys_by_input, &groove_ivm_rt_NODE_KEYSET_MAP);
    groove__ivm__runtime__evaluation_memo__EvaluationMemo__default(&rt->eval_memo);
    jz_hmap_init(&rt->eval_memo_keys_by_node, &groove_ivm_rt_NODE_KEYSET_MAP);
    jz_hmap_init(&rt->table_frontiers, &groove_ivm_rt_STR_U64_MAP);
    jz_hmap_init(&rt->binding_frontiers, &groove_ivm_rt_BINDING_U64_MAP);
    rt->memo_use_clock = 0;
    rt->eval_memo_bytes = 0;
    rt->hydration_memo_hits = 0;
    rt->hydration_memo_computes = 0;
    jz_hmap_init(&rt->hydration_memo_computed_nodes, &groove_ivm_rt_NODE_SET);
    jz_hmap_init(&rt->node_meta, &groove_ivm_rt_NODE_META_MAP);
    rt->current_tick = 0;
    rt->next_subscription_id = 1;
    rt->next_shape_id = 1;
    rt->logical_nodes_requested = 0;
    rt->auto_direct_family_enabled = true;
    rt->plain_output_root_positions = true;
    rt->collect_tick_runtime_stats = false;
    jz_hmap_init(&rt->prepared_shapes, &SHAPES_MAP);
    jz_hmap_init(&rt->auto_direct_families, &U64_U64_MAP);
    jz_hmap_init(&rt->shared_prepared_shapes, &U64_U64_MAP);
    groove_BindingSources_init(&rt->binding_sources);
    rt->input_source_runtime_namespace = NEXT_INPUT_SOURCE_RUNTIME_NAMESPACE++;
    rt->next_input_source_id = 1;
    rt->live_attaches = 0;
    jz_hmap_init(&rt->deferred_notifications, &DEFERRED_MAP);
    jz_hmap_init(&rt->durable_notification_publications, &groove_ivm_rt_U64_SET);
    jz_hmap_init(&rt->completed_deferred_publications, &groove_ivm_rt_U64_SET);
    rt->persistence_indeterminate = jz_rc_new(sizeof(bool), NULL);
    *rt->persistence_indeterminate = false;
    rt->index_ids = index_ids;
    jz_err e = groove__ivm__runtime__schema__IvmRuntime__define_schema_index_variant_projections(rt);
    if (!JZ_IS_ERR(e)) e = groove__ivm__runtime__subscriptions__IvmRuntime__add_dedup_schema_indices(rt);
    if (JZ_IS_ERR(e)) { groove_IvmRuntime_drop(rt); return e; }
    *out = rt;
    return JZ_OK;
}

void groove_IvmRuntime_drop(groove_IvmRuntime *rt) {
    groove_DatabaseSchema_drop(&rt->schema);
    groove_OwnedChunkProvider_drop(&rt->chunk_provider);
    jz_hmap_drop(&rt->table_storage_descriptors);
    jz_hmap_drop(&rt->table_descriptors);
    jz_hmap_drop(&rt->variant_descriptors);
    jz_hmap_drop(&rt->variant_projections);
    groove_IvmGraph_drop(rt->graph);
    groove_ProjectionPlans_drop(&rt->projection_plans);
    groove_CompilationCache_drop(&rt->compilation_cache);
    jz_hmap_drop(&rt->multisink_subscriptions);
    jz_hmap_drop(&rt->subscriptions_by_output_node);
    groove_PendingIncrementalEvaluation_drop(&rt->pending_incremental);
    jz_hmap_drop(&rt->gc_candidates);
    jz_hmap_drop(&rt->prepared_shapes);
    jz_hmap_drop(&rt->auto_direct_families);
    jz_hmap_drop(&rt->shared_prepared_shapes);
    groove_BindingSources_drop(&rt->binding_sources);
    jz_vec_drop(&rt->pending_binding_retractions, sizeof(groove_BindingDelta), NULL);
    jz_hmap_drop(&rt->deferred_notifications);
    jz_hmap_drop(&rt->durable_notification_publications);
    jz_hmap_drop(&rt->completed_deferred_publications);
    jz_rc_release(rt->persistence_indeterminate);
    jz_rc_release(rt->index_ids);
    jz_hmap_drop(&rt->operator_states);
    jz_hmap_drop(&rt->arrangement_states);
    jz_hmap_drop(&rt->arrangement_keys_by_input);
    jz_hmap_drop(&rt->eval_memo_keys_by_node);
    groove_EvaluationMemo_drop(&rt->eval_memo);
    jz_hmap_drop(&rt->table_frontiers);
    jz_hmap_drop(&rt->binding_frontiers);
    jz_hmap_drop(&rt->hydration_memo_computed_nodes);
    jz_hmap_drop(&rt->node_meta);
    jz_free(rt);
}

/* Allocate one opaque, runtime-local mutable input identity with its
 * permanent record descriptor. */
groove_InputSourceId groove__ivm__runtime__IvmRuntime__allocate_input_source(groove_IvmRuntime *self, groove_RecordDescriptor descriptor) {
    groove_InputSourceId id = { self->input_source_runtime_namespace, self->next_input_source_id };
    if (self->next_input_source_id == UINT64_MAX) jz_panic("input source IDs must not wrap within one runtime");
    self->next_input_source_id += 1;
    groove_BindingSourceKey key = groove__ivm__graph__InputSourceId__binding_key(id);
    bool vacant;
    groove_BindingSourceState *state = groove__ivm__runtime__subscriptions__BindingSources__entry_vacant(&self->binding_sources, key, &vacant);
    if (!vacant) jz_panic("internal error: entered unreachable code: fresh input source identity already has state");
    state->descriptor = descriptor;
    jz_hmap_init(&state->refcounts, &groove_ivm_rt_U64_SET);
    state->initialized = false;
    return id;
}

void groove__ivm__runtime__IvmRuntime__set_chunk_provider(groove_IvmRuntime *self, const groove_ChunkProviderVt *vt, void *provider) {
    groove_OwnedChunkProvider_drop(&self->chunk_provider);
    self->chunk_provider = groove__chunks__OwnedChunkProvider__new(vt, provider);
}

void groove__ivm__runtime__IvmRuntime__set_owned_chunk_provider(groove_IvmRuntime *self, groove_OwnedChunkProvider provider) {
    groove_OwnedChunkProvider_drop(&self->chunk_provider);
    self->chunk_provider = provider;
}

groove_OwnedChunkProvider groove__ivm__runtime__IvmRuntime__chunk_provider(const groove_IvmRuntime *self) {
    return groove__chunks__OwnedChunkProvider__clone(&self->chunk_provider);
}

void groove__ivm__runtime__IvmRuntime__set_tick_runtime_stats_enabled(groove_IvmRuntime *self, bool enabled) { self->collect_tick_runtime_stats = enabled; }
const groove_IvmGraph *groove__ivm__runtime__IvmRuntime__graph(const groove_IvmRuntime *self) { return self->graph; }
void groove__ivm__runtime__IvmRuntime__set_auto_direct_family_enabled(groove_IvmRuntime *self, bool enabled) { self->auto_direct_family_enabled = enabled; }
void groove__ivm__runtime__IvmRuntime__set_plain_output_root_positions_enabled(groove_IvmRuntime *self, bool enabled) { self->plain_output_root_positions = enabled; }
bool groove__ivm__runtime__IvmRuntime__plain_output_root_positions_enabled(const groove_IvmRuntime *self) { return self->plain_output_root_positions; }
const groove_DatabaseSchema *groove__ivm__runtime__IvmRuntime__schema(const groove_IvmRuntime *self) { return &self->schema; }

const groove_TableSchema *groove__ivm__runtime__IvmRuntime__table(const groove_IvmRuntime *self, const char *table, u32 len) {
    return groove__schema__DatabaseSchema__table(&self->schema, table, len);
}

static const groove_RecordDescriptor *str_desc_get(const jz_hmap *m, const char *table, u32 len) {
    jz_str key = { (char *)table, len, len };
    return jz_hmap_get(m, &key);
}

const groove_RecordDescriptor *groove__ivm__runtime__IvmRuntime__table_descriptor(const groove_IvmRuntime *self, const char *table, u32 len) {
    return str_desc_get(&self->table_descriptors, table, len);
}

groove_SharedIndexIds groove__ivm__runtime__IvmRuntime__index_ids(const groove_IvmRuntime *self) { return self->index_ids; }

/* The durable id of a declared schema index (allocated in memory when first seen). */
u32 groove__ivm__runtime__IvmRuntime__index_storage_id(const groove_IvmRuntime *self, const char *table, u32 len, const groove_IndexSchema *index) {
    return groove__ivm__runtime__index_ids__IndexIdRegistry__resolve(self->index_ids, table, len, index);
}

const groove_RecordDescriptor *groove__ivm__runtime__IvmRuntime__table_storage_descriptor(const groove_IvmRuntime *self, const char *table, u32 len) {
    return str_desc_get(&self->table_storage_descriptors, table, len);
}

const groove_RecordDescriptor *groove__ivm__runtime__IvmRuntime__record_descriptor(const groove_IvmRuntime *self, const char *table, u32 len, u32 variant_tag) {
    const groove_RecordDescriptor *d = str_desc_get(&self->table_descriptors, table, len);
    if (d && variant_tag == 0) return d;
    jz_str key = { (char *)table, len, len };
    const jz_hmap *descriptors = jz_hmap_get(&self->variant_descriptors, &key);
    if (!descriptors) return NULL;
    return jz_hmap_get(descriptors, &variant_tag);
}

/* Freezes a batch output buffer whose rows are handed out as slices. A buffer
 * left mostly unused by an overestimate or doubling growth is copied down. */
jz_bytes groove__ivm__runtime__freeze_batch_buffer(jz_bytes buffer) {
    if (buffer.cap - buffer.len > buffer.len / 2) {
        jz_bytes copy = jz_bytes_from(buffer.ptr, buffer.len);
        jz_bytes_drop(&buffer);
        return copy;
    }
    return buffer;
}

/* ---------- #[derive(Clone)] for IvmRuntime (used by register_table_index staging) ---------- */
/* Value clones owned by sibling units (rt-a): declared here, provided there. */
void groove_MultisinkSubscriptionState_clone_v(void *dst, const void *src);
void groove_RoutedMultisinkShapeState_clone_v(void *dst, const void *src);
void groove_BindingSources_clone(groove_BindingSources *dst, const groove_BindingSources *src);
void groove_BindingDelta_clone(groove_BindingDelta *dst, const groove_BindingDelta *src);
void groove_OperatorState_clone_v(void *dst, const void *src);

typedef void (*clone_fn)(void *dst, const void *src);
/* HashMap::clone: one generic copy; NULL clone = bitwise (Copy) element. */
static void hmap_clone(jz_hmap *dst, const jz_hmap *src, clone_fn kclone, clone_fn vclone) {
    jz_hmap_init(dst, src->d);
    _Alignas(8) u8 kbuf[256]; _Alignas(8) u8 vbuf[256]; /* key/value sizes are bounded by the runtime maps above */
    void *k, *v;
    JZ_HMAP_FOR(src, k, v) {
        if (kclone) kclone(kbuf, k); else memcpy(kbuf, k, src->d->key_size);
        if (src->d->val_size) { if (vclone) vclone(vbuf, v); else memcpy(vbuf, v, src->d->val_size); }
        jz_hmap_insert(dst, kbuf, vbuf, NULL);
    }
}
static void str_clone_v(void *d, const void *s) { *(jz_str *)d = jz_str_clone(s); }
static void u32_desc_map_clone_v(void *d, const void *s) { hmap_clone(d, s, NULL, NULL); }
static void set_clone_v(void *d, const void *s) { hmap_clone(d, s, NULL, NULL); }
static void vp_key_clone_v(void *d, const void *s) {
    groove_VariantProjectionKey *dk = d; const groove_VariantProjectionKey *sk = s;
    dk->table = jz_str_clone(&sk->table);
    groove_VariantProjectionTarget_clone(&dk->target, &sk->target);
}
static void vp_clone_v(void *d, const void *s) { groove_VariantProjection_clone(d, s); }
static void binding_key_clone_v(void *d, const void *s) { groove_BindingSourceKey_clone(d, s); }
static void deferred_clone_v(void *d, const void *s) { jz_vec_clone(d, s, sizeof(void *) * 2, NULL); }
static void arrangement_as_of_clone_v(void *d, const void *s) {
    groove_AsOfArrangement *da = d; const groove_AsOfArrangement *sa = s;
    *da = *sa;
    if (sa->value) da->value = groove__ivm__runtime__join__ArrangementState__clone(sa->value);
}
static void binding_delta_clone_v(void *d, const void *s) { groove_BindingDelta_clone(d, s); }

void groove_IvmRuntime_clone(groove_IvmRuntime *dst, const groove_IvmRuntime *src) {
    *dst = *src; /* Copy fields (counters, flags, wakers, namespace) */
    groove_DatabaseSchema_clone(&dst->schema, &src->schema);
    dst->chunk_provider = groove__chunks__OwnedChunkProvider__clone(&src->chunk_provider);
    hmap_clone(&dst->table_storage_descriptors, &src->table_storage_descriptors, str_clone_v, NULL);
    hmap_clone(&dst->table_descriptors, &src->table_descriptors, str_clone_v, NULL);
    hmap_clone(&dst->variant_descriptors, &src->variant_descriptors, str_clone_v, u32_desc_map_clone_v);
    hmap_clone(&dst->variant_projections, &src->variant_projections, vp_key_clone_v, vp_clone_v);
    dst->graph = groove__ivm__graph__IvmGraph__clone(src->graph);
    groove_ProjectionPlans_clone(&dst->projection_plans, &src->projection_plans);
    /* CompilationCache::clone is Self::default(): forks drop installation hints. */
    groove__ivm__runtime__compilation_cache__CompilationCache__default(&dst->compilation_cache);
    hmap_clone(&dst->multisink_subscriptions, &src->multisink_subscriptions, NULL, groove_MultisinkSubscriptionState_clone_v);
    hmap_clone(&dst->subscriptions_by_output_node, &src->subscriptions_by_output_node, NULL, set_clone_v);
    dst->pending_incremental.rc = src->pending_incremental.rc ? jz_rc_clone(src->pending_incremental.rc) : NULL;
    hmap_clone(&dst->gc_candidates, &src->gc_candidates, NULL, NULL);
    hmap_clone(&dst->prepared_shapes, &src->prepared_shapes, NULL, groove_RoutedMultisinkShapeState_clone_v);
    hmap_clone(&dst->auto_direct_families, &src->auto_direct_families, NULL, NULL);
    hmap_clone(&dst->shared_prepared_shapes, &src->shared_prepared_shapes, NULL, NULL);
    groove_BindingSources_clone(&dst->binding_sources, &src->binding_sources);
    jz_vec_clone(&dst->pending_binding_retractions, &src->pending_binding_retractions, sizeof(groove_BindingDelta), binding_delta_clone_v);
    hmap_clone(&dst->deferred_notifications, &src->deferred_notifications, NULL, deferred_clone_v);
    hmap_clone(&dst->durable_notification_publications, &src->durable_notification_publications, NULL, NULL);
    hmap_clone(&dst->completed_deferred_publications, &src->completed_deferred_publications, NULL, NULL);
    dst->persistence_indeterminate = jz_rc_clone(src->persistence_indeterminate);
    dst->index_ids = jz_rc_clone(src->index_ids);
    hmap_clone(&dst->operator_states, &src->operator_states, NULL, groove_OperatorState_clone_v);
    hmap_clone(&dst->arrangement_states, &src->arrangement_states, NULL, arrangement_as_of_clone_v);
    hmap_clone(&dst->arrangement_keys_by_input, &src->arrangement_keys_by_input, NULL, set_clone_v);
    hmap_clone(&dst->eval_memo_keys_by_node, &src->eval_memo_keys_by_node, NULL, set_clone_v);
    groove_EvaluationMemo_clone(&dst->eval_memo, &src->eval_memo);
    hmap_clone(&dst->table_frontiers, &src->table_frontiers, str_clone_v, NULL);
    hmap_clone(&dst->binding_frontiers, &src->binding_frontiers, binding_key_clone_v, NULL);
    hmap_clone(&dst->hydration_memo_computed_nodes, &src->hydration_memo_computed_nodes, NULL, NULL);
    hmap_clone(&dst->node_meta, &src->node_meta, NULL, groove_NodeRuntimeMeta_clone);
}
