/* sources.c: port of crates/groove/src/ivm/template/sources.rs.
 * Separate instance-owned leaves from immutable source operator context. */
#include "jz_rt.h"
#include "../../_ext_groove-ivm-db.h"
#include "../template.h"

static u64 ptr_hash_v(const void *k) { return jz_hash_u64((u64)(uintptr_t)*(const void *const *)k); }
static bool ptr_eq_v(const void *a, const void *b) { return *(const void *const *)a == *(const void *const *)b; }
static void arc_release_v(void *p) { groove__ivm__graph__arc_release(*(groove_GraphArc *)p); }
static const jz_hmap_desc REWRITTEN_DESC = { sizeof(void *), sizeof(groove_GraphArc), ptr_hash_v, ptr_eq_v, NULL, arc_release_v };
static const jz_hmap_desc LEAF_SLOTS_DESC = { sizeof(void *), sizeof(u32), ptr_hash_v, ptr_eq_v, NULL, NULL };

typedef struct SourceBindings {
    groove_TemplateGraphInputVec inputs;
    jz_hmap rewritten; /* HashMap<usize, Arc<GraphBuilder>> */
} SourceBindings;

static void SourceBindings__new(SourceBindings *self) {
    memset(&self->inputs, 0, sizeof self->inputs);
    jz_hmap_init(&self->rewritten, &REWRITTEN_DESC);
}

static bool is_external_leaf(const groove_GraphBuilder *graph) {
    switch (graph->tag) {
    case GROOVE_GB_TABLE: case GROOVE_GB_INDEX: case GROOVE_GB_INPUT_SOURCE: case GROOVE_GB_INLINE_RECORDS:
    case GROOVE_GB_TYPED_TEMPLATE:
        return true;
    case GROOVE_GB_TEMPLATE_INPUT:
        return graph->u.template_input.input != NULL;
    /* A lookup reads its table by the keys of its input: the whole subtree is one source. */
    case GROOVE_GB_LOOKUP:
        return true;
    /* The ordinary compiler proves a direct-table winner's primary key and
     * chooses its comparison semantics; keep that proof attached to the leaf. */
    case GROOVE_GB_ARG_MAX_BY: case GROOVE_GB_ARG_MIN_BY:
        return graph->u.arg_by.input->tag == GROOVE_GB_TABLE;
    default:
        return false;
    }
}

static groove_GraphArc rewritten_get(jz_hmap *m, const groove_GraphBuilder *key) {
    groove_GraphArc *found = jz_hmap_get(m, &key);
    if (!found) jz_panic("key not found");
    return groove__ivm__graph__arc_clone(*found);
}
static groove_GraphArc map_child(groove_GraphArc const *child, void *ctx) { return rewritten_get(ctx, *child); }
typedef struct { const groove_GraphBuilder *graph; bool expanded; } frame;
typedef JZ_VEC(frame) frame_stack;
static void push_child(groove_GraphArc const *child, void *ctx) {
    frame f = { *child, false };
    JZ_VEC_PUSH(*(frame_stack *)ctx, f);
}

static jz_err SourceBindings__blueprint(SourceBindings *self, const groove_GraphBuilder *root, groove_TemplateDescribeFn describe, void *ctx,
                                        groove_GraphBuilder *out) {
    frame_stack pending = {0};
    frame first = { root, false };
    JZ_VEC_PUSH(pending, first);
    while (pending.len) {
        frame top = pending.ptr[--pending.len];
        const groove_GraphBuilder *graph = top.graph;
        if (jz_hmap_get(&self->rewritten, &graph)) continue;
        if (is_external_leaf(graph)) {
            groove_GraphBuilder copy;
            groove__ivm__graph__GraphBuilder__Clone__clone(&copy, graph);
            groove_TemplateGraphInput input;
            jz_err e = describe(ctx, copy, &input);
            if (e.code) { jz_vec_drop(&pending, sizeof(frame), NULL); return e; }
            groove_GraphBuilder slot; memset(&slot, 0, sizeof slot);
            slot.tag = GROOVE_GB_TEMPLATE_INPUT;
            slot.u.template_input.slot = self->inputs.len;
            slot.u.template_input.output = groove__ivm__template__TemplateGraphInput__descriptor(&input);
            slot.u.template_input.input = NULL;
            JZ_VEC_PUSH(self->inputs, input);
            groove_GraphArc arc = groove__ivm__graph__arc_new(slot);
            jz_hmap_insert(&self->rewritten, &graph, &arc, NULL);
        } else if (!top.expanded) {
            frame again = { graph, true };
            JZ_VEC_PUSH(pending, again);
            groove__ivm__graph__GraphBuilder__visit_inputs(graph, push_child, &pending);
        } else {
            groove_GraphBuilder mapped;
            groove__ivm__graph__GraphBuilder__map_inputs(graph, map_child, &self->rewritten, &mapped);
            groove_GraphArc arc = groove__ivm__graph__arc_new(mapped);
            jz_hmap_insert(&self->rewritten, &graph, &arc, NULL);
        }
    }
    jz_vec_drop(&pending, sizeof(frame), NULL);
    groove_GraphArc result = rewritten_get(&self->rewritten, root);
    groove__ivm__graph__GraphBuilder__Clone__clone(out, result);
    groove__ivm__graph__arc_release(result);
    return JZ_OK;
}

/* ---------------- match_template_sources ---------------- */
typedef struct { bool some; groove_TemplateGraphInput input; } OptInput;
typedef struct MatchCtx {
    JZ_VEC(OptInput) inputs;
    jz_hmap leaf_slots; /* HashMap<*const GraphBuilder, u32> */
    bool has_error; jz_err error;
    groove_TemplateDescribeFn describe; void *describe_ctx;
} MatchCtx;

static i8 match_hook(void *ctxp, const groove_GraphBuilder *fresh, const groove_GraphBuilder *slot) {
    MatchCtx *ctx = ctxp;
    if (!is_external_leaf(fresh)) {
        /* A blueprint slot can only stand for an external leaf. */
        return (slot->tag == GROOVE_GB_TEMPLATE_INPUT && slot->u.template_input.input == NULL) ? 0 : -1;
    }
    if (slot->tag != GROOVE_GB_TEMPLATE_INPUT || slot->u.template_input.input != NULL) return 0;
    u32 slot_index = slot->u.template_input.slot;
    const u32 *existing = jz_hmap_get(&ctx->leaf_slots, &fresh);
    if (existing) return *existing == slot_index;
    u32 index = slot_index;
    while (ctx->inputs.len <= index) { OptInput none = { false }; JZ_VEC_PUSH(ctx->inputs, none); }
    /* Two distinct fresh leaves cannot share one blueprint slot. */
    if (ctx->inputs.ptr[index].some) return 0;
    groove_GraphBuilder copy;
    groove__ivm__graph__GraphBuilder__Clone__clone(&copy, fresh);
    groove_TemplateGraphInput input;
    jz_err e = ctx->describe(ctx->describe_ctx, copy, &input);
    if (e.code) {
        if (ctx->has_error) jz_err_drop(&ctx->error);
        ctx->has_error = true;
        ctx->error = e;
        return 0;
    }
    if (groove__ivm__template__TemplateGraphInput__descriptor(&input) == slot->u.template_input.output) {
        jz_hmap_insert(&ctx->leaf_slots, &fresh, &slot_index, NULL);
        ctx->inputs.ptr[index].some = true;
        ctx->inputs.ptr[index].input = input;
        return 1;
    }
    groove__ivm__template__TemplateGraphInput__drop(&input);
    return 0;
}
static void opt_input_drop_v(void *p) { OptInput *o = p; if (o->some) groove__ivm__template__TemplateGraphInput__drop(&o->input); }

jz_err groove__ivm__template__sources__match_template_sources(const groove_GraphBuilder *const *graphs, u32 n,
                                                              const groove_GraphBuilder *const *blueprints, u32 nb,
                                                              groove_TemplateDescribeFn describe, void *describe_ctx,
                                                              bool *matched_out, groove_TemplateGraphInputVec *out) {
    *matched_out = false;
    if (n != nb) return JZ_OK;
    MatchCtx ctx;
    memset(&ctx, 0, sizeof ctx);
    jz_hmap_init(&ctx.leaf_slots, &LEAF_SLOTS_DESC);
    ctx.describe = describe;
    ctx.describe_ctx = describe_ctx;
    jz_err result = JZ_OK;
    bool all = true;
    for (u32 i = 0; i < n; i++) {
        bool matched = groove__ivm__runtime__subscriptions__graph_builders_equal_with(graphs[i], blueprints[i], match_hook, &ctx);
        if (ctx.has_error) { result = ctx.error; ctx.has_error = false; all = false; break; }
        if (!matched) { all = false; break; }
    }
    if (all) {
        /* inputs.into_iter().collect::<Option<Vec<_>>>() */
        bool complete = true;
        for (u32 i = 0; i < ctx.inputs.len; i++) if (!ctx.inputs.ptr[i].some) { complete = false; break; }
        if (complete) {
            memset(out, 0, sizeof *out);
            for (u32 i = 0; i < ctx.inputs.len; i++) JZ_VEC_PUSH(*out, ctx.inputs.ptr[i].input);
            ctx.inputs.len = 0;
            *matched_out = true;
        }
    }
    jz_vec_drop(&ctx.inputs, sizeof(OptInput), opt_input_drop_v);
    jz_hmap_drop(&ctx.leaf_slots);
    return result;
}

jz_err groove__ivm__template__sources__split_template_sources(const groove_GraphBuilder *const *graphs, u32 n,
                                                              groove_TemplateDescribeFn describe, void *ctx,
                                                              groove_GraphBuilderVec *out_graphs, groove_TemplateGraphInputVec *out_inputs) {
    SourceBindings bindings;
    SourceBindings__new(&bindings);
    groove_GraphBuilderVec result = {0};
    for (u32 i = 0; i < n; i++) {
        groove_GraphBuilder blueprint;
        jz_err e = SourceBindings__blueprint(&bindings, graphs[i], describe, ctx, &blueprint);
        if (e.code) {
            jz_vec_drop(&result, sizeof(groove_GraphBuilder), groove__ivm__graph__GraphBuilder__drop_v);
            groove__ivm__template__TemplateGraphInputVec__drop(&bindings.inputs);
            jz_hmap_drop(&bindings.rewritten);
            return e;
        }
        JZ_VEC_PUSH(result, blueprint);
    }
    jz_hmap_drop(&bindings.rewritten);
    *out_graphs = result;
    *out_inputs = bindings.inputs;
    return JZ_OK;
}
