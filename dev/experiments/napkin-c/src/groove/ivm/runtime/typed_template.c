/* groove::ivm::runtime::typed_template: compile a whole typed family; bind
 * sources and predicates without re-lowering. */
#include "mod.h"

#define TT(name) groove__ivm__runtime__typed_template__##name

static u64 NEXT_TEMPLATE = 1;

/* HashMap<usize(ptr), Arc<GraphBuilder>> */
static void rc_release_v(void *p) { jz_rc_release(*(void **)p); }
static const jz_hmap_desc PTR_ARC_MAP = { sizeof(u64), sizeof(groove_GraphBuilder *), groove_NodeId_hash, groove_NodeId_eq, NULL, rc_release_v };
/* HashMap<NodeId, TemplateNodeRef> */
static const jz_hmap_desc NODE_REF_MAP = { sizeof(groove_NodeId), sizeof(groove_TemplateNodeRefB), groove_NodeId_hash, groove_NodeId_eq, NULL, NULL };
/* HashMap<usize, usize> and HashMap<NodeId, usize> use groove_ivm_rt_NODE_U64_MAP (u64 keys/values). */

typedef JZ_VEC(groove_GraphBuilder *) ArcVec;
typedef JZ_VEC(groove_CompiledForestEntryB) EntryVec;
#define VMOVE(dst, src) memcpy(&(dst), &(src), sizeof(dst))

static groove_GraphBuilder *arc_new(groove_GraphBuilder value) {
    groove_GraphBuilder *p = jz_rc_new(sizeof(groove_GraphBuilder), groove__ivm__graph__GraphBuilder__drop_v);
    *p = value;
    return p;
}

static void pred_drop_v(void *p) { groove_PredicateExpr_drop(p); }
static void builder_drop_v(void *p) { groove__ivm__graph__GraphBuilder__drop(p); }

/* All terminal families of one lowered program. Terminals share fragments;
 * compiling them together preserves that sharing, as ordinary installation
 * does, instead of recompiling each common fragment once per terminal. */
typedef struct Forest {
    groove_GraphBuilderValVec roots;
    ArcVec inputs;
    groove_RecordDescriptorVecB contracts;
    groove_PredicateExprVecB predicates;
    groove_PredicateExprVecB markers;
} Forest;

static void forest_drop(Forest *f) {
    jz_vec_drop(&f->roots, sizeof(groove_GraphBuilder), builder_drop_v);
    jz_vec_drop(&f->inputs, sizeof(groove_GraphBuilder *), rc_release_v);
    jz_vec_drop(&f->contracts, sizeof(groove_RecordDescriptor), NULL);
    jz_vec_drop(&f->predicates, sizeof(groove_PredicateExpr), pred_drop_v);
    jz_vec_drop(&f->markers, sizeof(groove_PredicateExpr), pred_drop_v);
}

/* One terminal's view of a forest: its local fallback graph and the forest
 * slots it owns, in local slot order. Nothing here is value-dependent. */
typedef struct Slice {
    groove_GraphBuilder fallback;
    groove_UsizeVecT inputs;
    groove_UsizeVecT predicates;
} Slice;

/* Preserve dependency information while distinguishing equal-valued arguments.
 * These are compiler expressions, never valid executable constant predicates. */
static groove_PredicateExpr predicate_marker(usize slot, const groove_PredicateExpr *predicate) {
    jz_btree fields; jz_btree_init(&fields, &groove_ivm_rt_STR_SET);
    groove__ivm__runtime__subscriptions__PredicateExpr__referenced_fields(predicate, &fields);
    groove_PredicateExpr marker; memset(&marker, 0, sizeof marker);
    marker.tag = groove_Pred_TemplateArgument;
    marker.slot = (u32)slot;
    jz_str field;
    while (jz_btree_pop_first(&fields, &field, NULL)) JZ_VEC_PUSH(marker.fields, field);
    jz_btree_drop(&fields);
    return marker;
}

static bool marker_slot(const groove_PredicateExpr *predicate, usize *out) {
    if (predicate->tag == groove_Pred_TemplateArgument) { *out = predicate->slot; return true; }
    return false;
}

static groove_PredicateExpr predicate_marker_with_slot(usize slot, const groove_PredicateExpr *marker) {
    groove_PredicateExpr out;
    if (marker->tag == groove_Pred_TemplateArgument) {
        memset(&out, 0, sizeof out);
        out.tag = groove_Pred_TemplateArgument;
        out.slot = (u32)slot;
        JZ_VEC_FOR(const jz_str, f, marker->fields) JZ_VEC_PUSH(out.fields, jz_str_clone(f));
        return out;
    }
    groove_PredicateExpr_clone(&out, marker);
    return out;
}

/* ---- shared traversal plumbing ---- */
typedef struct Pending { const groove_GraphBuilder *node; bool expanded; } Pending;
typedef JZ_VEC(Pending) PendingVec;

static void push_child(groove_GraphBuilder *const *child, void *ctx) {
    Pending p = { *child, false };
    JZ_VEC_PUSH(*(PendingVec *)ctx, p);
}

static groove_GraphBuilder *map_rewritten(groove_GraphBuilder *const *child, void *ctx) {
    u64 key = (u64)(usize)*child;
    groove_GraphBuilder **found = jz_hmap_get((jz_hmap *)ctx, &key);
    return jz_rc_clone(*found); /* rewritten[&ptr].clone(); presence is an invariant */
}

static bool is_slot_marker(const groove_PredicateExpr *predicate, const groove_PredicateExpr *markers, u32 nmarkers, usize *slot) {
    return marker_slot(predicate, slot) && *slot < nmarkers && groove_PredicateExpr_eq(&markers[*slot], predicate);
}

static jz_err forest(const groove_GraphBuilder *const *roots, u32 nroots, groove_DescribeFn describe, void *dctx, Forest *out) {
    Forest forest; memset(&forest, 0, sizeof forest);
    jz_vec_reserve(&forest.roots, nroots, sizeof(groove_GraphBuilder));
    /* One pointer memo across roots: a fragment shared by several terminals
     * keeps one slot numbering and one rewritten node. */
    jz_hmap rewritten; jz_hmap_init(&rewritten, &PTR_ARC_MAP);
    PendingVec pending = {0};
    jz_err e = JZ_OK;
    for (u32 r = 0; r < nroots; r++) {
        pending.len = 0;
        Pending first = { roots[r], false };
        JZ_VEC_PUSH(pending, first);
        while (pending.len) {
            Pending cur = pending.ptr[--pending.len];
            const groove_GraphBuilder *node = cur.node;
            u64 key = (u64)(usize)node;
            if (jz_hmap_get(&rewritten, &key)) continue;
            bool has_output = false;
            groove_RecordDescriptor output = 0;
            switch (node->tag) {
            case groove_GB_TemplateInput:
                if (node->input == NULL) { has_output = true; output = node->output; }
                break;
            case groove_GB_BindingSource: case groove_GB_InlineRecords: case groove_GB_InputSource: case groove_GB_FrontierSource:
                has_output = true; output = node->output; break;
            case groove_GB_ArgMaxBy: case groove_GB_ArgMinBy:
                if (node->input->tag == groove_GB_Table) {
                    e = describe(dctx, node, &output);
                    if (JZ_IS_ERR(e)) goto fail;
                    has_output = true;
                }
                break;
            case groove_GB_Table: case groove_GB_Index: case groove_GB_Lookup: case groove_GB_Recursive:
            case groove_GB_RecursiveStepWitness: case groove_GB_VariantProject:
                e = describe(dctx, node, &output);
                if (JZ_IS_ERR(e)) goto fail;
                has_output = true;
                break;
            case groove_GB_TypedTemplate: case groove_GB_CollectBy:
                e = GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
                goto fail;
            default: break;
            }
            if (has_output) {
                u32 slot = forest.inputs.len;
                groove_GraphBuilder copy; groove__ivm__graph__GraphBuilder__clone(&copy, node);
                JZ_VEC_PUSH(forest.inputs, arc_new(copy));
                JZ_VEC_PUSH(forest.contracts, output);
                groove_GraphBuilder leaf; memset(&leaf, 0, sizeof leaf);
                leaf.tag = groove_GB_TemplateInput; leaf.slot = slot; leaf.output = output; leaf.input = NULL;
                groove_GraphBuilder *arc = arc_new(leaf);
                jz_hmap_insert(&rewritten, &key, &arc, NULL);
                continue;
            }
            if (!cur.expanded) {
                Pending again = { node, true };
                JZ_VEC_PUSH(pending, again);
                groove__ivm__graph__GraphBuilder__visit_inputs(node, push_child, &pending);
                continue;
            }
            groove_GraphBuilder bound;
            groove__ivm__graph__GraphBuilder__map_inputs(node, map_rewritten, &rewritten, &bound);
            if (bound.tag == groove_GB_Filter) {
                groove_PredicateExpr marker = predicate_marker(forest.predicates.len, &bound.predicate);
                JZ_VEC_PUSH(forest.predicates, bound.predicate); /* mem::replace */
                groove_PredicateExpr_clone(&bound.predicate, &marker);
                JZ_VEC_PUSH(forest.markers, marker);
            }
            groove_GraphBuilder *arc = arc_new(bound);
            jz_hmap_insert(&rewritten, &key, &arc, NULL);
        }
        u64 rk = (u64)(usize)roots[r];
        groove_GraphBuilder **root = jz_hmap_get(&rewritten, &rk);
        groove__ivm__graph__GraphBuilder__clone(jz_vec_push_slot(&forest.roots, sizeof(groove_GraphBuilder)), *root);
    }
    jz_vec_drop(&pending, sizeof(Pending), NULL);
    jz_hmap_drop(&rewritten);
    *out = forest;
    return JZ_OK;
fail:
    jz_vec_drop(&pending, sizeof(Pending), NULL);
    jz_hmap_drop(&rewritten);
    forest_drop(&forest);
    return e;
}

static void slice_drop(Slice *s) {
    groove__ivm__graph__GraphBuilder__drop(&s->fallback);
    jz_vec_drop(&s->inputs, sizeof(usize), NULL);
    jz_vec_drop(&s->predicates, sizeof(usize), NULL);
}

/* Renumber one terminal's forest slots densely. Every forest filter carries
 * a unique marker, and every unbound leaf is a forest input slot. */
static jz_err slice(const groove_GraphBuilder *root, const groove_PredicateExpr *markers, u32 nmarkers, Slice *out) {
    jz_hmap inputs; jz_hmap_init(&inputs, &groove_ivm_rt_NODE_U64_MAP);
    jz_hmap predicates; jz_hmap_init(&predicates, &groove_ivm_rt_NODE_U64_MAP);
    groove_UsizeVecT input_order = {0}, predicate_order = {0};
    jz_hmap rewritten; jz_hmap_init(&rewritten, &PTR_ARC_MAP);
    PendingVec pending = {0};
    jz_err e = JZ_OK;
    Pending first = { root, false };
    JZ_VEC_PUSH(pending, first);
    while (pending.len) {
        Pending cur = pending.ptr[--pending.len];
        const groove_GraphBuilder *node = cur.node;
        u64 key = (u64)(usize)node;
        if (jz_hmap_get(&rewritten, &key)) continue;
        if (node->tag == groove_GB_TemplateInput && node->input == NULL) {
            u64 global = node->slot;
            bool inserted;
            u64 *local = jz_hmap_entry(&inputs, &global, &inserted);
            if (inserted) {
                JZ_VEC_PUSH(input_order, (usize)global);
                *local = input_order.len - 1;
            }
            groove_GraphBuilder leaf; memset(&leaf, 0, sizeof leaf);
            leaf.tag = groove_GB_TemplateInput; leaf.slot = (u32)*local; leaf.output = node->output; leaf.input = NULL;
            groove_GraphBuilder *arc = arc_new(leaf);
            jz_hmap_insert(&rewritten, &key, &arc, NULL);
            continue;
        }
        if (!cur.expanded) {
            Pending again = { node, true };
            JZ_VEC_PUSH(pending, again);
            groove__ivm__graph__GraphBuilder__visit_inputs(node, push_child, &pending);
            continue;
        }
        groove_GraphBuilder bound;
        groove__ivm__graph__GraphBuilder__map_inputs(node, map_rewritten, &rewritten, &bound);
        if (bound.tag == groove_GB_Filter) {
            usize global;
            if (!is_slot_marker(&bound.predicate, markers, nmarkers, &global)) {
                groove__ivm__graph__GraphBuilder__drop(&bound);
                e = GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
                goto done;
            }
            u64 g = global;
            bool inserted;
            u64 *local = jz_hmap_entry(&predicates, &g, &inserted);
            if (inserted) {
                JZ_VEC_PUSH(predicate_order, global);
                *local = predicate_order.len - 1;
            }
            groove_PredicateExpr next = predicate_marker_with_slot((usize)*local, &bound.predicate);
            groove_PredicateExpr_drop(&bound.predicate);
            bound.predicate = next;
        }
        groove_GraphBuilder *arc = arc_new(bound);
        jz_hmap_insert(&rewritten, &key, &arc, NULL);
    }
    {
        u64 rk = (u64)(usize)root;
        groove_GraphBuilder **r = jz_hmap_get(&rewritten, &rk);
        groove__ivm__graph__GraphBuilder__clone(&out->fallback, *r);
        out->inputs = input_order;
        out->predicates = predicate_order;
        input_order.ptr = NULL; predicate_order.ptr = NULL;
        input_order.len = input_order.cap = predicate_order.len = predicate_order.cap = 0;
    }
done:
    jz_vec_drop(&input_order, sizeof(usize), NULL);
    jz_vec_drop(&predicate_order, sizeof(usize), NULL);
    jz_vec_drop(&pending, sizeof(Pending), NULL);
    jz_hmap_drop(&rewritten);
    jz_hmap_drop(&inputs);
    jz_hmap_drop(&predicates);
    return e;
}

static u64 forest_fingerprint(const groove_GraphBuilder *roots, u32 n) {
    /* DefaultHasher over (len, fingerprint per root) */
    u64 h = jz_hash_u64((u64)n);
    for (u32 i = 0; i < n; i++)
        h = jz_hash_combine(h, groove__ivm__runtime__subscriptions__graph_builder_fingerprint(&roots[i]));
    return h;
}

static jz_err compile_typed_forest(groove_IvmRuntime *self, const Forest *forest, EntryVec *out);

static jz_err describe_unsupported(void *ctx, const groove_GraphBuilder *g, groove_RecordDescriptor *out) {
    (void)ctx; (void)g; (void)out;
    return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
}

/* Compiler reuse has one exact whole-program lookup, not a recipe lookup per
 * operator. Each returned graph owns fresh source/predicate arguments. */
jz_err TT(TypedGraphTemplateCache__compile)(groove_TypedGraphTemplateCacheB *self, const groove_GraphBuilder *graphs, u32 n, groove_GraphBuilderValVec *out) {
    return TT(TypedGraphTemplateCache__compile_with_sources)(self, graphs, n, describe_unsupported, NULL, out);
}

static void cache_entry_drop(groove_TemplateCacheEntryB *entry) { groove__ivm__template__TemplateCacheEntry__drop_v(entry); }

static groove_GraphBuilder *map_to_typed(groove_GraphBuilder *const *child, void *ctx) {
    (void)child;
    return jz_rc_clone(ctx);
}

/* Extend families through source projections while leaving catalogue-
 * sensitive leaves on their ordinary, validated installation path.
 * Descriptors supplied here are declarations, checked again at install. */
jz_err TT(TypedGraphTemplateCache__compile_with_sources)(groove_TypedGraphTemplateCacheB *self, const groove_GraphBuilder *graphs, u32 n,
                                                         groove_DescribeFn describe, void *dctx, groove_GraphBuilderValVec *out) {
    /* Binding-route filters must remain below an explicit terminal. */
    JZ_VEC(const groove_GraphBuilder *) roots = {0};
    jz_vec_reserve(&roots, n, sizeof(void *));
    jz_err e;
    for (u32 i = 0; i < n; i++) {
        const groove_GraphBuilder *graph = &graphs[i];
        e = groove__ivm__runtime__record_projection__validate_collect_by_terminality(graph);
        if (JZ_IS_ERR(e)) goto fail_roots;
        if (graph->tag == groove_GB_CollectBy) {
            e = groove__ivm__runtime__record_projection__validate_collect_by_terminality(graph->input);
            if (JZ_IS_ERR(e)) goto fail_roots;
            JZ_VEC_PUSH(roots, (const groove_GraphBuilder *)graph->input);
        } else {
            JZ_VEC_PUSH(roots, graph);
        }
    }
    Forest f;
    e = forest(roots.ptr, roots.len, describe, dctx, &f);
    jz_vec_drop(&roots, sizeof(void *), NULL);
    if (JZ_IS_ERR(e)) return e;
    u64 fingerprint = forest_fingerprint(f.roots.ptr, f.roots.len);
    groove_CompiledForestB *compiled = NULL;
    for (u32 i = 0; i < self->entries.len; i++) {
        groove_TemplateCacheEntryB *entry = jz_deque_at(&self->entries, i);
        if (entry->key != fingerprint || entry->graphs.len != f.roots.len) continue;
        bool all = true;
        for (u32 k = 0; k < f.roots.len && all; k++)
            all = groove__ivm__runtime__subscriptions__graph_builders_equal(&entry->graphs.ptr[k], &f.roots.ptr[k]);
        if (all) { compiled = entry->forest; break; }
    }
    if (compiled) {
        self->reuses += 1;
        compiled = jz_rc_clone(compiled);
        jz_vec_drop(&f.roots, sizeof(groove_GraphBuilder), builder_drop_v);
    } else {
        groove_IvmRuntime *compiler;
        e = groove__ivm__runtime__IvmRuntime__new(groove__schema__DatabaseSchema__new_empty(), &compiler);
        if (JZ_IS_ERR(e)) { forest_drop(&f); return e; }
        EntryVec programs = {0};
        e = compile_typed_forest(compiler, &f, &programs);
        groove_IvmRuntime_drop(compiler);
        if (JZ_IS_ERR(e)) { forest_drop(&f); return e; }
        groove__ivm__template__CompiledForest__from_vec(programs.ptr, programs.len, &compiled);
        jz_vec_drop(&programs, sizeof(groove_CompiledForestEntryB), NULL); /* elements moved */
        self->compilations += 1;
        if (self->entries.len == 128) {
            groove_TemplateCacheEntryB old;
            jz_deque_pop_front(&self->entries, &old);
            cache_entry_drop(&old);
        }
        groove_TemplateCacheEntryB entry = { fingerprint, {0}, jz_rc_clone(compiled) };
        VMOVE(entry.graphs, f.roots); /* forest.roots moved into the cache */
        f.roots.ptr = NULL; f.roots.len = f.roots.cap = 0;
        jz_deque_push_back(&self->entries, &entry);
    }
    groove_GraphBuilderValVec result = {0};
    jz_vec_reserve(&result, n, sizeof(groove_GraphBuilder));
    for (u32 i = 0; i < n && i < compiled->len; i++) {
        const groove_CompiledForestEntryB *item = &compiled->items[i];
        groove_GraphBuilder typed; memset(&typed, 0, sizeof typed);
        typed.tag = groove_GB_TypedTemplate;
        typed.program = jz_rc_clone(item->template_);
        JZ_VEC_FOR(const usize, slot, item->slice.inputs) JZ_VEC_PUSH(typed.inputs, (groove_GraphBuilder *)jz_rc_clone(f.inputs.ptr[*slot]));
        JZ_VEC_FOR(const usize, slot, item->slice.predicates)
            groove_PredicateExpr_clone(jz_vec_push_slot(&typed.predicates, sizeof(groove_PredicateExpr)), &f.predicates.ptr[*slot]);
        typed.scalars = NULL; typed.scalars_len = 0; /* Arc::from([]) */
        groove_GraphBuilder *slot_out = jz_vec_push_slot(&result, sizeof(groove_GraphBuilder));
        if (graphs[i].tag == groove_GB_CollectBy) {
            groove_GraphBuilder *arc = arc_new(typed);
            groove__ivm__graph__GraphBuilder__map_inputs(&graphs[i], map_to_typed, arc, slot_out);
            jz_rc_release(arc);
        } else {
            *slot_out = typed;
        }
    }
    jz_rc_release(compiled);
    forest_drop(&f);
    *out = result;
    return JZ_OK;
fail_roots:
    jz_vec_drop(&roots, sizeof(void *), NULL);
    return e;
}

static void template_drop_v(void *p) { groove__ivm__template__TypedGraphTemplate__drop(p); }

static jz_err extract_typed_template(const groove_IvmRuntime *self, const groove_CompiledNode *compiled, const jz_hmap *sources,
                                     const Slice *slice, const Forest *forest, groove_TypedGraphTemplateB *out);

static jz_err compile_typed_forest(groove_IvmRuntime *self, const Forest *forest, EntryVec *out) {
    jz_hmap sources; jz_hmap_init(&sources, &groove_ivm_rt_NODE_U64_MAP); /* NodeId -> usize */
    groove_TemplateGraphInputVecB replacements = {0};
    groove_GraphBuilderValVec bound = {0};
    jz_err e = JZ_OK;
    for (u32 slot = 0; slot < forest->contracts.len; slot++) {
        groove_RecordDescriptor output = forest->contracts.ptr[slot];
        groove_InputSourceId id = groove__ivm__runtime__IvmRuntime__allocate_input_source(self, output);
        groove_GraphBuilder replacement = groove__ivm__graph__GraphBuilder__input_source(id, output);
        groove_CompiledNode c;
        e = groove__ivm__runtime__compilation__IvmRuntime__add_dedup_graph(self, &replacement, &c);
        if (JZ_IS_ERR(e)) { groove__ivm__graph__GraphBuilder__drop(&replacement); goto done; }
        u64 s = slot;
        jz_hmap_insert(&sources, &c.node, &s, NULL);
        JZ_VEC_PUSH(replacements, groove__ivm__template__TemplateGraphInput__with_output_contract(replacement, output));
    }
    /* One forest bind and one scratch graph: shared fragments keep one
     * builder identity, so the compilation cache compiles them once. */
    e = groove__ivm__template__bind_template_graphs_inner(forest->roots.ptr, forest->roots.len, replacements.ptr, replacements.len, true, NULL, &bound);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); e = GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch); goto done; }
    jz_vec_reserve(out, bound.len, sizeof(groove_CompiledForestEntryB));
    for (u32 i = 0; i < forest->roots.len && i < bound.len; i++) {
        groove_CompiledNode compiled;
        e = groove__ivm__runtime__compilation__IvmRuntime__add_dedup_template_graph(self, &bound.ptr[i], &compiled);
        if (JZ_IS_ERR(e)) goto done;
        Slice sl;
        e = slice(&forest->roots.ptr[i], forest->markers.ptr, forest->markers.len, &sl);
        if (JZ_IS_ERR(e)) goto done;
        groove_TypedGraphTemplateB *program = jz_rc_new(sizeof(groove_TypedGraphTemplateB), template_drop_v);
        e = extract_typed_template(self, &compiled, &sources, &sl, forest, program);
        if (JZ_IS_ERR(e)) {
            memset(program, 0, sizeof *program);
            jz_rc_release(program);
            slice_drop(&sl);
            goto done;
        }
        groove_CompiledForestEntryB entry;
        entry.template_ = program;
        entry.slice.inputs.ptr = sl.inputs.ptr; entry.slice.inputs.len = sl.inputs.len; entry.slice.inputs.cap = sl.inputs.cap;
        entry.slice.predicates.ptr = sl.predicates.ptr; entry.slice.predicates.len = sl.predicates.len; entry.slice.predicates.cap = sl.predicates.cap;
        groove__ivm__graph__GraphBuilder__drop(&sl.fallback);
        JZ_VEC_PUSH(*out, entry);
    }
done:
    jz_vec_drop(&bound, sizeof(groove_GraphBuilder), builder_drop_v);
    groove__ivm__template__TemplateGraphInputVec__drop(&replacements);
    jz_hmap_drop(&sources);
    return e;
}

typedef struct NodePending { groove_NodeId id; bool expanded; } NodePending;

static void template_node_drop_v(void *p) {
    groove_TemplateNodeB *n = p;
    groove_NodeDescriptor_drop(&n->descriptor);
    jz_vec_drop(&n->inputs, sizeof(groove_TemplateNodeRefB), NULL);
}

static jz_err extract_typed_template(const groove_IvmRuntime *self, const groove_CompiledNode *compiled, const jz_hmap *sources,
                                     const Slice *slice, const Forest *forest, groove_TypedGraphTemplateB *out) {
    jz_hmap local_inputs; jz_hmap_init(&local_inputs, &groove_ivm_rt_NODE_U64_MAP);
    for (u32 local = 0; local < slice->inputs.len; local++) {
        u64 g = slice->inputs.ptr[local], l = local;
        jz_hmap_insert(&local_inputs, &g, &l, NULL);
    }
    jz_hmap local_predicates; jz_hmap_init(&local_predicates, &groove_ivm_rt_NODE_U64_MAP);
    for (u32 local = 0; local < slice->predicates.len; local++) {
        u64 g = slice->predicates.ptr[local], l = local;
        jz_hmap_insert(&local_predicates, &g, &l, NULL);
    }
    jz_hmap references; jz_hmap_init(&references, &NODE_REF_MAP);
    groove_TemplateNodeVecB nodes = {0};
    JZ_VEC(NodePending) pending = {0};
    jz_err e = JZ_OK;
    NodePending root = { compiled->node, false };
    JZ_VEC_PUSH(pending, root);
    if (compiled->has_root_ordering_node) {
        NodePending ord = { compiled->root_ordering_node, false };
        JZ_VEC_PUSH(pending, ord);
    }
    while (pending.len) {
        NodePending cur = pending.ptr[--pending.len];
        groove_NodeId id = cur.id;
        if (jz_hmap_get(&references, &id)) continue;
        const u64 *global = jz_hmap_get(sources, &id);
        if (global) {
            /* A source outside this terminal's slice cannot be reachable:
             * input source ids are unique per forest slot. */
            const u64 *local = jz_hmap_get(&local_inputs, global);
            if (!local) { e = GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator); goto fail; }
            groove_TemplateNodeRefB ref = { GROOVE_TNREF_INPUT_B, (u32)*local };
            jz_hmap_insert(&references, &id, &ref, NULL);
            continue;
        }
        const groove_GraphNode *node = groove__ivm__graph__IvmGraph__node(self->graph, id);
        if (!node) { e = GROOVE_IVM_ERR_N(GROOVE_IVM_GraphNodeNotFound, id); goto fail; }
        const groove_NodeDescriptor *descriptor = &node->descriptor;
        if (!cur.expanded) {
            NodePending again = { id, true };
            JZ_VEC_PUSH(pending, again);
            JZ_VEC_FOR(const groove_NodeId, in, descriptor->inputs) { NodePending p = { *in, false }; JZ_VEC_PUSH(pending, p); }
            continue;
        }
        groove_TemplateNodeB instruction; memset(&instruction, 0, sizeof instruction);
        JZ_VEC_FOR(const groove_NodeId, in, descriptor->inputs)
            JZ_VEC_PUSH(instruction.inputs, *(groove_TemplateNodeRefB *)jz_hmap_get(&references, in));
        groove_OpType_clone(&instruction.descriptor.operator_, &descriptor->operator_);
        instruction.descriptor.output = descriptor->output; /* inputs cleared */
        if (instruction.descriptor.operator_.tag == groove_Op_Filter) {
            groove_FilterOp *filter = &instruction.descriptor.operator_.v.filter;
            usize slot;
            const u64 *local = NULL;
            if (is_slot_marker(&filter->predicate, forest->markers.ptr, forest->markers.len, &slot)) {
                u64 s = slot;
                local = jz_hmap_get(&local_predicates, &s);
            }
            if (!local) { template_node_drop_v(&instruction); e = GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator); goto fail; }
            groove_PredicateExpr_drop(&filter->predicate);
            memset(&filter->predicate, 0, sizeof filter->predicate);
            filter->predicate.tag = groove_Pred_And; /* And(Vec::new()) */
            instruction.has_predicate = true;
            instruction.predicate = (u32)*local;
        }
        groove_TemplateNodeRefB ref = { GROOVE_TNREF_NODE_B, nodes.len };
        jz_hmap_insert(&references, &id, &ref, NULL);
        JZ_VEC_PUSH(nodes, instruction);
    }
    memset(out, 0, sizeof *out);
    out->identity = NEXT_TEMPLATE++;
    VMOVE(out->nodes, nodes);
    JZ_VEC_FOR(const usize, slot, slice->inputs) JZ_VEC_PUSH(out->inputs, forest->contracts.ptr[*slot]);
    out->output = compiled->output;
    out->root = *(groove_TemplateNodeRefB *)jz_hmap_get(&references, &compiled->node);
    out->has_ordering = compiled->has_root_ordering_node;
    if (compiled->has_root_ordering_node)
        out->ordering = *(groove_TemplateNodeRefB *)jz_hmap_get(&references, &compiled->root_ordering_node);
    out->terminal = false;
    groove__ivm__graph__GraphBuilder__clone(&out->fallback, &slice->fallback);
    for (u32 local = 0; local < slice->predicates.len; local++)
        JZ_VEC_PUSH(out->predicate_markers, predicate_marker_with_slot(local, &forest->markers.ptr[slice->predicates.ptr[local]]));
    jz_vec_drop(&pending, sizeof(NodePending), NULL);
    jz_hmap_drop(&references);
    jz_hmap_drop(&local_inputs);
    jz_hmap_drop(&local_predicates);
    return JZ_OK;
fail:
    jz_vec_drop(&nodes, sizeof(groove_TemplateNodeB), template_node_drop_v);
    jz_vec_drop(&pending, sizeof(NodePending), NULL);
    jz_hmap_drop(&references);
    jz_hmap_drop(&local_inputs);
    jz_hmap_drop(&local_predicates);
    return e;
}

static groove_NodeId resolve(groove_TemplateNodeRefB reference, const groove_CompiledNode *inputs, const groove_NodeIdVec *installed) {
    return reference.tag == GROOVE_TNREF_INPUT_B ? inputs[reference.index].node : installed->ptr[reference.index];
}

jz_err TT(IvmRuntime__install_typed_template)(groove_IvmRuntime *self, const groove_TypedGraphTemplate *program_,
    const groove_CompiledNode *inputs, u32 n, groove_GraphBuilder *const *graphs, u32 ngraphs,
    const groove_PredicateExpr *predicates, u32 np, const groove_TemplateScalarArgument *scalars, u32 ns, groove_CompiledNode *out) {
    const groove_TypedGraphTemplateB *program = (const groove_TypedGraphTemplateB *)program_;
    bool mismatch = n != program->inputs.len || np != program->predicate_markers.len;
    for (u32 i = 0; !mismatch && i < np && i < program->predicate_markers.len; i++)
        if (!groove__ivm__op_types__PredicateExpr__accepts_template_argument(&program->predicate_markers.ptr[i], &predicates[i])) mismatch = true;
    for (u32 i = 0; !mismatch && i < n; i++)
        if (inputs[i].output != program->inputs.ptr[i]) mismatch = true;
    if (mismatch) return GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch);
    /* Until inherited root ordering has an explicit relocation, preserve the
     * full compiler rather than silently dropping source ordering. */
    for (u32 i = 0; i < n; i++) {
        if (!inputs[i].has_root_ordering_node) continue;
        groove_GraphBuilder bound;
        jz_err e = groove__ivm__template__TypedGraphTemplate__bind_declarative_with_arguments(program_, graphs, ngraphs, predicates, np, scalars, ns, &bound);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); return GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch); }
        e = groove__ivm__runtime__compilation__IvmRuntime__add_dedup_graph(self, &bound, out);
        groove__ivm__graph__GraphBuilder__drop(&bound);
        return e;
    }
    groove_NodeIdVec installed = {0};
    jz_vec_reserve(&installed, program->nodes.len, sizeof(groove_NodeId));
    JZ_VEC_FOR(const groove_TemplateNodeB, instruction, program->nodes) {
        groove_NodeIdVec node_inputs = {0};
        JZ_VEC_FOR(const groove_TemplateNodeRefB, r, instruction->inputs) JZ_VEC_PUSH(node_inputs, resolve(*r, inputs, &installed));
        /* Only argument-consuming operators need an instance copy; every
         * other node is hashed and compared from the program itself. */
        const groove_OpType *declared = &instruction->descriptor.operator_;
        bool binds_scalars = false;
        if (declared->tag == groove_Op_MapProject)
            JZ_VEC_FOR(const groove_ProjectionExpr, x, declared->v.map_project.expressions)
                if (x->expression.tag == groove_ProjectExpr_TemplateArgument) { binds_scalars = true; break; }
        groove_OpType bound;
        const groove_OpType *operator_ = declared;
        bool owns = false;
        if (instruction->has_predicate || binds_scalars) {
            groove_OpType_clone(&bound, declared);
            owns = true;
            if (instruction->has_predicate && bound.tag == groove_Op_Filter) {
                groove_PredicateExpr_drop(&bound.v.filter.predicate);
                groove_PredicateExpr_clone(&bound.v.filter.predicate, &predicates[instruction->predicate]);
            }
            if (bound.tag == groove_Op_MapProject) {
                JZ_VEC_FOR(groove_ProjectionExpr, x, bound.v.map_project.expressions) {
                    jz_err e = groove__ivm__template__arguments__bind_project_expression(&x->expression, scalars, ns);
                    if (JZ_IS_ERR(e)) {
                        jz_err_drop(&e);
                        groove_OpType_drop(&bound);
                        jz_vec_drop(&node_inputs, sizeof(groove_NodeId), NULL);
                        jz_vec_drop(&installed, sizeof(groove_NodeId), NULL);
                        return GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch);
                    }
                }
            }
            operator_ = &bound;
        }
        self->logical_nodes_requested += 1;
        groove_NodeDurability durability; memset(&durability, 0, sizeof durability);
        durability.tag = groove_NodeDurability_Ephemeral;
        groove_NodeId id = groove__ivm__graph__IvmGraph__dedup_node_parts(self->graph, operator_, node_inputs, instruction->descriptor.output, durability);
        if (owns) groove_OpType_drop(&bound);
        groove__ivm__runtime__graph_lifecycle__IvmRuntime__initialize_node_runtime(self, id);
        JZ_VEC_PUSH(installed, id);
    }
    out->node = resolve(program->root, inputs, &installed);
    out->output = program->output;
    out->has_root_ordering_node = program->has_ordering;
    if (program->has_ordering) out->root_ordering_node = resolve(program->ordering, inputs, &installed);
    jz_vec_drop(&installed, sizeof(groove_NodeId), NULL);
    return JZ_OK;
}
