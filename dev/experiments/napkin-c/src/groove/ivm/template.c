/* template.c: port of crates/groove/src/ivm/template.rs.
 * Immutable operator topology with explicit, typed, instance-owned inputs.
 * A compiler artifact, never a storage or wire encoding. */
#include "jz_rt.h"
#include "../_ext_groove-ivm-db.h"
#include "template.h"

/* ---------------- TemplateBindingError (Display) ---------------- */
jz_err groove__ivm__template__TemplateBindingError__scalar_argument(u32 slot) {
    return jz_err_fmt(GROOVE_TEMPLATE_ERR_SCALAR_ARGUMENT, "template scalar argument %u is missing or has a different type", (unsigned)slot);
}
jz_err groove__ivm__template__TemplateBindingError__predicate_argument(u32 slot) {
    return jz_err_fmt(GROOVE_TEMPLATE_ERR_PREDICATE_ARGUMENT, "template predicate argument %u is missing or remains unbound", (unsigned)slot);
}
jz_err groove__ivm__template__TemplateBindingError__predicate_arity(void) {
    return jz_err_new(GROOVE_TEMPLATE_ERR_PREDICATE_ARITY, "template predicate argument count does not match");
}
jz_err groove__ivm__template__TemplateBindingError__missing_input(u32 slot) {
    return jz_err_fmt(GROOVE_TEMPLATE_ERR_MISSING_INPUT, "template input %u is missing", (unsigned)slot);
}
jz_err groove__ivm__template__TemplateBindingError__descriptor_mismatch(u32 slot) {
    return jz_err_fmt(GROOVE_TEMPLATE_ERR_DESCRIPTOR_MISMATCH, "template input %u has a different record descriptor", (unsigned)slot);
}

/* ---------------- TypedGraphTemplate ---------------- */
static void template_node_drop_v(void *p) {
    groove_TemplateNode *n = p;
    groove__ivm__graph__NodeDescriptor__drop(&n->descriptor);
    jz_vec_drop(&n->inputs, sizeof(groove_TemplateNodeRef), NULL);
}
void groove__ivm__template__TypedGraphTemplate__drop(groove_TypedGraphTemplate *v) {
    jz_vec_drop(&v->nodes, sizeof(groove_TemplateNode), template_node_drop_v);
    jz_vec_drop(&v->inputs, sizeof(groove_RecordDescriptor), NULL);
    groove__ivm__graph__GraphBuilder__drop(&v->fallback);
    jz_vec_drop(&v->predicate_markers, sizeof(groove_PredicateExpr), groove__ivm__op_types__PredicateExpr__drop_v);
}
/* impl PartialEq for TypedGraphTemplate: identity only */
bool groove__ivm__template__TypedGraphTemplate__PartialEq__eq(const groove_TypedGraphTemplate *a, const groove_TypedGraphTemplate *b) {
    return a->identity == b->identity;
}
/* impl Hash for TypedGraphTemplate: identity only */
void groove__ivm__template__TypedGraphTemplate__Hash__hash(const groove_TypedGraphTemplate *v, groove_Hasher *h) {
    groove_hash_u64(h, v->identity);
}
groove_RecordDescriptor groove__ivm__template__TypedGraphTemplate__output_descriptor(const groove_TypedGraphTemplate *self) {
    return self->output;
}
void groove__ivm__template__TemplateNodeRef__Debug__fmt(jz_str *out, const groove_TemplateNodeRef *v) {
    groove__fmt__debug_tuple_begin(out, v->tag == GROOVE_TNREF_INPUT ? "Input" : "Node");
    jz_str_appendf(out, "%u", (unsigned)v->index);
    groove__fmt__debug_tuple_end(out);
}
void groove__ivm__template__TypedGraphTemplate__Debug__fmt(jz_str *out, const groove_TypedGraphTemplate *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "TypedGraphTemplate");
    groove__fmt__DebugStruct__field(&d, "identity"); jz_str_appendf(out, "%llu", (unsigned long long)v->identity);
    groove__fmt__DebugStruct__field(&d, "nodes");
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->nodes.len; i++) {
        const groove_TemplateNode *n = &v->nodes.ptr[i];
        if (i) jz_str_push_cstr(out, ", ");
        groove_DebugStruct nd = groove__fmt__debug_struct(out, "TemplateNode");
        groove__fmt__DebugStruct__field(&nd, "descriptor"); groove__ivm__graph__NodeDescriptor__Debug__fmt(out, &n->descriptor);
        groove__fmt__DebugStruct__field(&nd, "inputs");
        jz_str_push_char(out, '[');
        for (u32 j = 0; j < n->inputs.len; j++) { if (j) jz_str_push_cstr(out, ", "); groove__ivm__template__TemplateNodeRef__Debug__fmt(out, &n->inputs.ptr[j]); }
        jz_str_push_char(out, ']');
        groove__fmt__DebugStruct__field(&nd, "predicate");
        if (n->has_predicate) jz_str_appendf(out, "Some(%u)", (unsigned)n->predicate); else jz_str_push_cstr(out, "None");
        groove__fmt__DebugStruct__finish(&nd);
    }
    jz_str_push_char(out, ']');
    groove__fmt__DebugStruct__field(&d, "inputs");
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->inputs.len; i++) { if (i) jz_str_push_cstr(out, ", "); groove__records__RecordDescriptor__Debug__fmt(out, v->inputs.ptr[i]); }
    jz_str_push_char(out, ']');
    groove__fmt__DebugStruct__field(&d, "output"); groove__records__RecordDescriptor__Debug__fmt(out, v->output);
    groove__fmt__DebugStruct__field(&d, "root"); groove__ivm__template__TemplateNodeRef__Debug__fmt(out, &v->root);
    groove__fmt__DebugStruct__field(&d, "ordering");
    if (v->has_ordering) {
        jz_str_push_cstr(out, "Some(");
        groove__ivm__template__TemplateNodeRef__Debug__fmt(out, &v->ordering);
        jz_str_push_char(out, ')');
    } else jz_str_push_cstr(out, "None");
    groove__fmt__DebugStruct__field(&d, "terminal"); jz_str_push_cstr(out, v->terminal ? "true" : "false");
    groove__fmt__DebugStruct__field(&d, "fallback"); groove__ivm__graph__GraphBuilder__Debug__fmt(out, &v->fallback);
    groove__fmt__DebugStruct__field(&d, "predicate_markers");
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->predicate_markers.len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        groove__ivm__op_types__PredicateExpr__Debug__fmt(out, &v->predicate_markers.ptr[i]);
    }
    jz_str_push_char(out, ']');
    groove__fmt__DebugStruct__finish(&d);
}

/* ---------------- TypedGraphTemplateCache ---------------- */
static void forest_drop(void *p) {
    groove_CompiledForest *f = p;
    for (u32 i = 0; i < f->len; i++) {
        jz_rc_release(f->items[i].template_);
        jz_vec_drop(&f->items[i].slice.inputs, sizeof(u32), NULL);
        jz_vec_drop(&f->items[i].slice.predicates, sizeof(u32), NULL);
    }
}
static void cache_entry_drop_v(void *p) {
    groove_TemplateCacheEntry *e = p;
    jz_vec_drop(&e->graphs, sizeof(groove_GraphBuilder), groove__ivm__graph__GraphBuilder__drop_v);
    jz_rc_release(e->forest);
    (void)forest_drop;
}
void groove__ivm__template__TypedGraphTemplateCache__Default__default(groove_TypedGraphTemplateCache *self) {
    jz_deque_init(&self->entries, sizeof(groove_TemplateCacheEntry));
    self->compilations = 0;
    self->reuses = 0;
}
void groove__ivm__template__TypedGraphTemplateCache__drop(groove_TypedGraphTemplateCache *self) {
    jz_deque_drop(&self->entries, cache_entry_drop_v);
}
void groove__ivm__template__TypedGraphTemplateCache__counters(const groove_TypedGraphTemplateCache *self, u64 *compilations, u64 *reuses) {
    *compilations = self->compilations;
    *reuses = self->reuses;
}
void groove__ivm__template__TypedGraphTemplateCache__clear(groove_TypedGraphTemplateCache *self) {
    groove_TemplateCacheEntry e;
    while (jz_deque_pop_front(&self->entries, &e)) cache_entry_drop_v(&e);
}

/* ---------------- TemplateGraphInput ---------------- */
groove_TemplateGraphInput groove__ivm__template__TemplateGraphInput__new(groove_GraphBuilder graph, groove_RecordDescriptor output) {
    groove_TemplateGraphInput in = { groove__ivm__graph__arc_new(graph), output };
    return in;
}
groove_RecordDescriptor groove__ivm__template__TemplateGraphInput__descriptor(const groove_TemplateGraphInput *self) { return self->output; }
groove_TemplateGraphInput groove__ivm__template__TemplateGraphInput__with_output_contract(groove_GraphBuilder graph, groove_RecordDescriptor output) {
    return groove__ivm__template__TemplateGraphInput__new(graph, output);
}
void groove__ivm__template__TemplateGraphInput__drop(groove_TemplateGraphInput *self) { groove__ivm__graph__arc_release(self->graph); }
static void tgi_drop_v(void *p) { groove__ivm__template__TemplateGraphInput__drop(p); }
void groove__ivm__template__TemplateGraphInputVec__drop(groove_TemplateGraphInputVec *v) { jz_vec_drop(v, sizeof(groove_TemplateGraphInput), tgi_drop_v); }

/* ---------------- rewritten maps: HashMap<*const GraphBuilder, Arc<GraphBuilder>> ---------------- */
static u64 ptr_hash_v(const void *k) { return jz_hash_u64((u64)(uintptr_t)*(const void *const *)k); }
static bool ptr_eq_v(const void *a, const void *b) { return *(const void *const *)a == *(const void *const *)b; }
static void arc_release_v(void *p) { groove__ivm__graph__arc_release(*(groove_GraphArc *)p); }
static const jz_hmap_desc REWRITTEN_DESC = { sizeof(void *), sizeof(groove_GraphArc), ptr_hash_v, ptr_eq_v, NULL, arc_release_v };
static groove_GraphArc rewritten_lookup(void *ctx_map, const groove_GraphBuilder *key) {
    groove_GraphArc *found = jz_hmap_get((jz_hmap *)ctx_map, &key);
    if (!found) jz_panic("key not found");
    return groove__ivm__graph__arc_clone(*found);
}
static groove_GraphArc map_from_rewritten(groove_GraphArc const *child, void *ctx) { return rewritten_lookup(ctx, *child); }
static void rewritten_insert(jz_hmap *m, const groove_GraphBuilder *key, groove_GraphArc value) {
    groove_GraphArc old;
    if (jz_hmap_insert(m, &key, &value, &old)) groove__ivm__graph__arc_release(old);
}

/* bind_declarative / bind_declarative_with_arguments */
jz_err groove__ivm__template__TypedGraphTemplate__bind_declarative(const groove_TypedGraphTemplate *self, const groove_GraphArc *inputs, u32 ni,
                                                                   const groove_PredicateExpr *predicates, u32 np, groove_GraphBuilder *out) {
    return groove__ivm__template__TypedGraphTemplate__bind_declarative_with_arguments(self, inputs, ni, predicates, np, NULL, 0, out);
}
jz_err groove__ivm__template__TypedGraphTemplate__bind_declarative_with_arguments(const groove_TypedGraphTemplate *self, const groove_GraphArc *inputs, u32 ni,
                                                                                  const groove_PredicateExpr *predicates, u32 np,
                                                                                  const groove_TemplateScalarArgument *scalars, u32 ns, groove_GraphBuilder *out) {
    if (np != self->predicate_markers.len) return groove__ivm__template__TemplateBindingError__predicate_arity();
    for (u32 slot = 0; slot < np; slot++)
        if (!groove__ivm__op_types__PredicateExpr__accepts_template_argument(&self->predicate_markers.ptr[slot], &predicates[slot]))
            return groove__ivm__template__TemplateBindingError__predicate_argument(slot);
    jz_hmap rewritten;
    jz_hmap_init(&rewritten, &REWRITTEN_DESC);
    groove_GraphRefVec order;
    groove__ivm__graph__GraphBuilder__postorder(&self->fallback, &order);
    for (u32 i = 0; i < order.len; i++) {
        const groove_GraphBuilder *node = order.ptr[i];
        groove_GraphBuilder bound;
        groove__ivm__graph__GraphBuilder__map_inputs(node, map_from_rewritten, &rewritten, &bound);
        if (bound.tag == GROOVE_GB_FILTER) {
            for (u32 slot = 0; slot < self->predicate_markers.len; slot++) {
                if (groove__ivm__op_types__PredicateExpr__PartialEq__eq(&self->predicate_markers.ptr[slot], &bound.u.filter.predicate)) {
                    groove__ivm__op_types__PredicateExpr__drop(&bound.u.filter.predicate);
                    groove__ivm__op_types__PredicateExpr__Clone__clone(&bound.u.filter.predicate, &predicates[slot]);
                    break;
                }
            }
        }
        if (bound.tag == GROOVE_GB_PROJECT) {
            for (u32 f = 0; f < bound.u.project.fields.len; f++) {
                jz_err e = groove__ivm__template__arguments__bind_project_expression(&bound.u.project.fields.ptr[f].expression, scalars, ns);
                if (e.code) {
                    groove__ivm__graph__GraphBuilder__drop(&bound);
                    jz_vec_drop(&order, sizeof(void *), NULL);
                    jz_hmap_drop(&rewritten);
                    return e;
                }
            }
        }
        rewritten_insert(&rewritten, node, groove__ivm__graph__arc_new(bound));
    }
    jz_vec_drop(&order, sizeof(void *), NULL);
    groove_GraphArc fallback = rewritten_lookup(&rewritten, &self->fallback);
    groove_TemplateGraphInputVec bound_inputs = {0};
    for (u32 i = 0; i < ni && i < self->inputs.len; i++) {
        groove_GraphBuilder copy;
        groove__ivm__graph__GraphBuilder__Clone__clone(&copy, inputs[i]);
        JZ_VEC_PUSH(bound_inputs, groove__ivm__template__TemplateGraphInput__with_output_contract(copy, self->inputs.ptr[i]));
    }
    groove_GraphBuilderVec graphs;
    jz_err e = groove__ivm__template__bind_template_graphs_inner(fallback, 1, bound_inputs.ptr, bound_inputs.len, true, NULL, &graphs);
    groove__ivm__template__TemplateGraphInputVec__drop(&bound_inputs);
    groove__ivm__graph__arc_release(fallback);
    jz_hmap_drop(&rewritten);
    JZ_TRY(e);
    /* .remove(0) */
    jz_vec_remove(&graphs, 0, out, sizeof(groove_GraphBuilder));
    jz_vec_drop(&graphs, sizeof(groove_GraphBuilder), groove__ivm__graph__GraphBuilder__drop_v);
    return JZ_OK;
}

jz_err groove__ivm__template__compile_template_graphs(const groove_GraphBuilder *graphs, u32 n, groove_GraphBuilderVec *out) {
    groove_TypedGraphTemplateCache cache;
    groove__ivm__template__TypedGraphTemplateCache__Default__default(&cache);
    jz_err e = groove__ivm__runtime__typed_template__TypedGraphTemplateCache__compile(&cache, graphs, n, out);
    groove__ivm__template__TypedGraphTemplateCache__drop(&cache);
    return e;
}

jz_err groove__ivm__template__bind_template_graphs(const groove_GraphBuilder *graphs, u32 n, const groove_TemplateGraphInput *inputs, u32 ni, groove_GraphBuilderVec *out) {
    return groove__ivm__template__bind_template_graphs_inner(graphs, n, inputs, ni, false, NULL, out);
}
jz_err groove__ivm__template__bind_template_program(const groove_GraphBuilder *graphs, u32 n, const groove_TemplateGraphInput *inputs, u32 ni,
                                                    const groove_PredicateExpr *predicates, u32 np, groove_ScalarArgSlice *scalars, groove_GraphBuilderVec *out) {
    groove_TemplateArguments arguments = { predicates, np, &scalars };
    jz_err e = groove__ivm__template__bind_template_graphs_inner(graphs, n, inputs, ni, false, &arguments, out);
    jz_rc_release(scalars); /* the Arc argument is dropped at the end of the call */
    return e;
}

/* Compiler-owned normalized families may contain bound contract wrappers
 * above local slots. Never recurse into a supplied replacement, whose slots
 * belong to its original owner. */
typedef struct { const groove_GraphBuilder *node; bool expanded; } bind_frame;
typedef JZ_VEC(bind_frame) bind_stack;
static void push_child(groove_GraphArc const *child, void *ctx) {
    bind_frame f = { *child, false };
    JZ_VEC_PUSH(*(bind_stack *)ctx, f);
}
jz_err groove__ivm__template__bind_template_graphs_inner(const groove_GraphBuilder *graphs, u32 n, const groove_TemplateGraphInput *inputs, u32 ni,
                                                         bool descend_bound, const groove_TemplateArguments *arguments, groove_GraphBuilderVec *out) {
    jz_hmap rewritten;
    jz_hmap_init(&rewritten, &REWRITTEN_DESC);
    bind_stack pending = {0};
    jz_err err = JZ_OK;
    for (u32 r = 0; r < n && !err.code; r++) {
        pending.len = 0;
        bind_frame root = { &graphs[r], false };
        JZ_VEC_PUSH(pending, root);
        while (pending.len) {
            bind_frame top = pending.ptr[--pending.len];
            const groove_GraphBuilder *node = top.node;
            if (jz_hmap_get(&rewritten, &node)) continue;
            if (node->tag == GROOVE_GB_TEMPLATE_INPUT) {
                u32 slot = node->u.template_input.slot;
                groove_RecordDescriptor output = node->u.template_input.output;
                bool bound = node->u.template_input.input != NULL;
                if (bound && !descend_bound) {
                    groove_GraphBuilder copy;
                    groove__ivm__graph__GraphBuilder__Clone__clone(&copy, node);
                    rewritten_insert(&rewritten, node, groove__ivm__graph__arc_new(copy));
                    continue;
                }
                if (!bound) {
                    if (slot >= ni) { err = groove__ivm__template__TemplateBindingError__missing_input(slot); break; }
                    const groove_TemplateGraphInput *input = &inputs[slot];
                    if (input->output != output) { err = groove__ivm__template__TemplateBindingError__descriptor_mismatch(slot); break; }
                    groove_GraphBuilder slot_node; memset(&slot_node, 0, sizeof slot_node);
                    slot_node.tag = GROOVE_GB_TEMPLATE_INPUT;
                    slot_node.u.template_input.slot = slot;
                    slot_node.u.template_input.output = output;
                    slot_node.u.template_input.input = groove__ivm__graph__arc_clone(input->graph);
                    rewritten_insert(&rewritten, node, groove__ivm__graph__arc_new(slot_node));
                    continue;
                }
            }
            if (!top.expanded) {
                bind_frame again = { node, true };
                JZ_VEC_PUSH(pending, again);
                groove__ivm__graph__GraphBuilder__visit_inputs(node, push_child, &pending);
                continue;
            }
            groove_GraphBuilder bound;
            groove__ivm__graph__GraphBuilder__map_inputs(node, map_from_rewritten, &rewritten, &bound);
            if (arguments) {
                jz_err e = groove__ivm__template__arguments__TemplateArguments__bind_node(arguments, &bound);
                if (e.code) { groove__ivm__graph__GraphBuilder__drop(&bound); err = e; break; }
            }
            rewritten_insert(&rewritten, node, groove__ivm__graph__arc_new(bound));
        }
    }
    jz_vec_drop(&pending, sizeof(bind_frame), NULL);
    if (err.code) { jz_hmap_drop(&rewritten); return err; }
    memset(out, 0, sizeof *out);
    jz_vec_reserve(out, n, sizeof(groove_GraphBuilder));
    for (u32 r = 0; r < n; r++) {
        groove_GraphArc arc = rewritten_lookup(&rewritten, &graphs[r]);
        groove_GraphBuilder copy;
        groove__ivm__graph__GraphBuilder__Clone__clone(&copy, arc);
        groove__ivm__graph__arc_release(arc);
        JZ_VEC_PUSH(*out, copy);
    }
    jz_hmap_drop(&rewritten);
    return JZ_OK;
}
