/* Port of crates/jazz/layers/node/src/node/query_eval/route_discipline.rs
 * Route discipline for prepared graphs shared across bindings (#4017). */
#include "jazz_node/node/node_qe_b.h"

#define RV(...) jz_err_fmt(JZN_ROUTE_VIOLATION, __VA_ARGS__)
typedef const groove_GraphBuilder *GB;

/* ---------- Debug helpers for string collections ---------- */
void jazz_node_qeb_debug_str(jz_str *out, const jz_str *s)
{
    jz_str_push_char(out, '"');
    for (u32 i = 0; i < s->len; i++) {
        char c = s->ptr[i];
        if (c == '"' || c == '\\') jz_str_push_char(out, '\\');
        jz_str_push_char(out, c);
    }
    jz_str_push_char(out, '"');
}
void jazz_node_qeb_debug_str_set(jz_str *out, const jz_btree *set)
{
    jz_str_push_char(out, '{');
    jz_str *k; void *v; bool first = true;
    JZ_BTREE_FOR(set, k, v) {
        if (!first) jz_str_push_cstr(out, ", ");
        jazz_node_qeb_debug_str(out, k);
        first = false;
    }
    jz_str_push_char(out, '}');
}
void jazz_node_qeb_debug_str_vec(jz_str *out, const jz_str *v, u32 n)
{
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < n; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        jazz_node_qeb_debug_str(out, &v[i]);
    }
    jz_str_push_char(out, ']');
}
static void debug_str_map(jz_str *out, const jz_btree *m)
{
    jz_str_push_char(out, '{');
    jz_str *k; jz_str *v; bool first = true;
    JZ_BTREE_FOR(m, k, v) {
        if (!first) jz_str_push_cstr(out, ", ");
        jazz_node_qeb_debug_str(out, k);
        jz_str_push_cstr(out, ": ");
        jazz_node_qeb_debug_str(out, v);
        first = false;
    }
    jz_str_push_char(out, '}');
}

/* ---------- pointer-keyed memo maps ---------- */
static u64 ptr_hash(const void *k) { return jz_hash_u64((u64)(uintptr_t)*(void *const *)k); }
static bool ptr_eq(const void *a, const void *b) { return *(void *const *)a == *(void *const *)b; }
static void btree_val_drop(void *p) { jz_btree_drop(p); }
static const jz_hmap_desc PTR_SET_MAP_DESC = { sizeof(void *), sizeof(jz_btree), ptr_hash, ptr_eq, NULL, btree_val_drop };
static int ptr_cmp(const void *a, const void *b, void *ctx)
{
    uintptr_t l = (uintptr_t)*(void *const *)a, r = (uintptr_t)*(void *const *)b;
    return l < r ? -1 : l > r;
}
static const jz_btree_desc PTR_SET_DESC = { sizeof(void *), 0, ptr_cmp, NULL, NULL };
typedef struct RewrittenPair { groove_GraphBuilder *original; groove_GraphBuilder *done; } RewrittenPair;
static void rewritten_pair_drop(void *p)
{
    RewrittenPair *r = p;
    jz_rc_release(r->original);
    jz_rc_release(r->done);
}
const jz_hmap_desc jazz_node_qeb_REWRITTEN_DESC = { sizeof(void *), sizeof(RewrittenPair), ptr_hash, ptr_eq, NULL, rewritten_pair_drop };

static jz_btree str_set_new(void) { jz_btree t; jz_btree_init(&t, &jazz_node_qeb_STR_SET_DESC); return t; }
static void str_set_insert(jz_btree *t, jz_str s) { if (jz_btree_contains(t, &s)) jz_str_drop(&s); else jz_btree_insert(t, &s, NULL, NULL); }
static jz_btree str_set_clone(const jz_btree *t) { jz_btree c; jz_btree_clone(&c, t, jazz_node_qeb_str_clone, NULL); return c; }
static bool str_set_is_subset(const jz_btree *a, const jz_btree *b)
{
    jz_str *k; void *v;
    JZ_BTREE_FOR(a, k, v) if (!jz_btree_contains(b, k)) return false;
    return true;
}
/* first element of a \ b */
static const jz_str *str_set_difference_first(const jz_btree *a, const jz_btree *b)
{
    jz_str *k; void *v;
    JZ_BTREE_FOR(a, k, v) if (!jz_btree_contains(b, k)) return k;
    return NULL;
}

/* ---------- graph structure ---------- */
/* child_arcs(): pointers to every child slot of `node`, in Rust's order. */
typedef groove_GraphBuilder **Slot;
static u32 jazz_node__node__query_eval__route_discipline__child_arcs(groove_GraphBuilder *node, Slot **slots)
{
    switch (node->tag) {
    case GGB_TYPED_TEMPLATE:
    case GGB_UNION: {
        *slots = jz_alloc(sizeof(Slot) * (node->inputs.len ? node->inputs.len : 1));
        for (u32 i = 0; i < node->inputs.len; i++) (*slots)[i] = &node->inputs.ptr[i];
        return node->inputs.len;
    }
    case GGB_TEMPLATE_INPUT:
        if (!node->input) return 0;
        *slots = jz_alloc(sizeof(void *));
        (*slots)[0] = &node->input;
        return 1;
    case GGB_RECURSIVE: {
        *slots = jz_alloc(sizeof(void *) * 3);
        u32 n = 0;
        (*slots)[n++] = &node->seed;
        (*slots)[n++] = &node->step;
        if (node->step_witness) (*slots)[n++] = &node->step_witness;
        return n;
    }
    case GGB_RECURSIVE_STEP_WITNESS:
        *slots = jz_alloc(sizeof(void *));
        (*slots)[0] = &node->recursive;
        return 1;
    case GGB_FILTER: case GGB_UNWRAP_NULLABLE: case GGB_UNNEST: case GGB_VARIANT_PROJECT: case GGB_PROJECT:
    case GGB_STREAMING_CHECKSUM: case GGB_ARG_MAX_BY: case GGB_ARG_MIN_BY: case GGB_TOP_BY: case GGB_COLLECT_BY:
    case GGB_AGGREGATE: case GGB_LOOKUP:
        *slots = jz_alloc(sizeof(void *));
        (*slots)[0] = &node->input;
        return 1;
    case GGB_JOIN: case GGB_SEMI_JOIN: case GGB_ANTI_JOIN:
        *slots = jz_alloc(sizeof(void *) * 2);
        (*slots)[0] = &node->left;
        (*slots)[1] = &node->right;
        return 2;
    case GGB_TABLE: case GGB_INLINE_RECORDS: case GGB_INPUT_SOURCE: case GGB_INDEX: case GGB_FRONTIER_SOURCE:
    case GGB_BINDING_SOURCE:
    default:
        return 0;
    }
}

/* children(): child nodes of `node`. */
typedef JZ_VEC(GB) GBVec;
static GBVec jazz_node__node__query_eval__route_discipline__children(GB node)
{
    Slot *childs = NULL;
    u32 n = jazz_node__node__query_eval__route_discipline__child_arcs((groove_GraphBuilder *)node, &childs);
    GBVec out = {0};
    for (u32 i = 0; i < n; i++) { GB c = *childs[i]; jz_vec_push(&out, &c, sizeof c); }
    if (n) jz_free(childs);
    return out;
}

/* ---------- binding slots ---------- */
jz_btree jazz_node__node__query_eval__route_discipline__binding_slot_routes(const jazz_engine_ParameterDomain *domain)
{
    jz_btree out;
    jz_btree_init(&out, &jazz_node_qeb_STR_STR_DESC);
    jz_str *k; void *v;
    JZ_BTREE_FOR(&domain->user_params, k, v) {
        jz_str key = jz_str_clone(k), route = jazz_engine__query_engine__fields__route_param_field(k), old;
        if (jz_btree_insert(&out, &key, &route, &old)) { jz_str_drop(&key); jz_str_drop(&old); }
    }
    JZ_BTREE_FOR(&domain->claim_params, k, v) {
        jz_str key = jz_str_clone(k), route = jz_str_clone(k), old;
        if (jz_btree_insert(&out, &key, &route, &old)) { jz_str_drop(&key); jz_str_drop(&old); }
    }
    return out;
}

/* Slot names a graph may use for its route. */
static jz_btree jazz_node__node__query_eval__route_discipline__slot_aliases(
    const jazz_node_RouteTerminal *terminals, u32 n, const jz_btree *slot_routes, jazz_node_RouteInferFn infer, void *ctx)
{
    jz_btree stored = str_set_new();
    jz_btree seen;
    jz_btree_init(&seen, &PTR_SET_DESC);
    GBVec pending = {0};
    for (u32 i = 0; i < n; i++) jz_vec_push(&pending, &terminals[i].graph, sizeof(GB));
    jz_btree out;
    jz_btree_init(&out, &jazz_node_qeb_STR_STR_DESC);
    bool unknown = false;
    while (pending.len) {
        GB node = pending.ptr[--pending.len];
        if (jz_btree_contains(&seen, &node)) continue;
        jz_btree_insert(&seen, &node, NULL, NULL);
        GBVec kids = jazz_node__node__query_eval__route_discipline__children(node);
        if (node->tag != GGB_BINDING_SOURCE && (kids.len == 0 || node->tag == GGB_LOOKUP)) {
            groove_RecordDescriptor *output;
            jz_err e = infer(node, &output, ctx);
            if (JZ_IS_ERR(e)) {
                /* Unknown columns could be named like any slot. */
                jz_err_drop(&e);
                jz_free(kids.ptr);
                unknown = true;
                break;
            }
            u32 nf;
            const groove_DescriptorField *fields = groove__records__descriptor__RecordDescriptor__fields(output, &nf);
            for (u32 f = 0; f < nf; f++) if (fields[f].has_name) str_set_insert(&stored, jz_str_clone(&fields[f].name));
            groove_RecordDescriptor_drop(output);
        }
        for (u32 i = 0; i < kids.len; i++) jz_vec_push(&pending, &kids.ptr[i], sizeof(GB));
        jz_free(kids.ptr);
    }
    if (!unknown) {
        jz_str *slot; jz_str *route;
        JZ_BTREE_FOR(slot_routes, slot, route) {
            if (!jz_str_eq(slot, route) && !jz_btree_contains(&stored, slot)) {
                jz_str a = jz_str_clone(slot), b = jz_str_clone(route);
                jz_btree_insert(&out, &a, &b, NULL);
            }
        }
    }
    jz_free(pending.ptr);
    jz_btree_drop(&seen);
    jz_btree_drop(&stored);
    return out;
}

/* ---------- RouteChecker ---------- */
typedef struct RouteChecker {
    const jz_btree *slot_routes;
    jz_btree aliases;
    jz_btree routes;
    jazz_node_RouteInferFn infer; void *infer_ctx;
    jz_hmap deps, carried, masks, equivalents;
    jz_btree checked;
} RouteChecker;

/* Binding routes whose values can decide which rows `node` outputs. */
static jz_btree jazz_node__node__query_eval__route_discipline__RouteChecker__depends(RouteChecker *self, GB node)
{
    const jz_btree *memo = jz_hmap_get(&self->deps, &node);
    if (memo) return str_set_clone(memo);
    jz_btree depends = str_set_new();
    if (node->tag == GGB_BINDING_SOURCE) {
        u32 nf;
        const groove_DescriptorField *fields = groove__records__descriptor__RecordDescriptor__fields(node->output, &nf);
        for (u32 f = 0; f < nf; f++) {
            if (!fields[f].has_name) continue;
            const jz_str *route = jz_btree_get(self->slot_routes, &fields[f].name);
            str_set_insert(&depends, jz_str_clone(route ? route : &fields[f].name));
        }
    } else {
        GBVec kids = jazz_node__node__query_eval__route_discipline__children(node);
        for (u32 i = 0; i < kids.len; i++) {
            jz_btree d = jazz_node__node__query_eval__route_discipline__RouteChecker__depends(self, kids.ptr[i]);
            jz_str k;
            while (jz_btree_pop_first(&d, &k, NULL)) str_set_insert(&depends, k);
            jz_btree_drop(&d);
        }
        jz_free(kids.ptr);
    }
    jz_btree copy = str_set_clone(&depends);
    jz_hmap_insert(&self->deps, &node, &copy, NULL);
    return depends;
}

static jz_err jazz_node__node__query_eval__route_discipline__RouteChecker__output_names(const RouteChecker *self, GB node, jz_str_vec *out)
{
    groove_RecordDescriptor *output;
    jz_err e = self->infer(node, &output, self->infer_ctx);
    if (JZ_IS_ERR(e)) {
        jz_str dbg = {0};
        groove_Error_debug(&dbg, &e);
        jz_err_drop(&e);
        jz_err r = RV("cannot describe a binding-dependent input: %.*s", (int)dbg.len, dbg.ptr);
        jz_str_drop(&dbg);
        return r;
    }
    u32 nf;
    const groove_DescriptorField *fields = groove__records__descriptor__RecordDescriptor__fields(output, &nf);
    memset(out, 0, sizeof *out);
    for (u32 f = 0; f < nf; f++) {
        jz_str name = fields[f].has_name ? jz_str_clone(&fields[f].name) : (jz_str){0};
        jz_vec_push(out, &name, sizeof name);
    }
    groove_RecordDescriptor_drop(output);
    return JZ_OK;
}

/* `name`, or the route it aliases. */
static jz_str jazz_node__node__query_eval__route_discipline__RouteChecker__route_name(const RouteChecker *self, jz_str name)
{
    const jz_str *alias = jz_btree_get(&self->aliases, &name);
    if (alias) { jz_str_drop(&name); return jz_str_clone(alias); }
    return name;
}

/* Binding route fields present in `node`'s output records. */
static jz_err jazz_node__node__query_eval__route_discipline__RouteChecker__carried(RouteChecker *self, GB node, jz_btree *out)
{
    const jz_btree *memo = jz_hmap_get(&self->carried, &node);
    if (memo) { *out = str_set_clone(memo); return JZ_OK; }
    jz_str_vec names;
    JZ_TRY(jazz_node__node__query_eval__route_discipline__RouteChecker__output_names(self, node, &names));
    jz_btree carried = str_set_new();
    for (u32 i = 0; i < names.len; i++) {
        jz_str field = jazz_node__node__query_eval__route_discipline__RouteChecker__route_name(self, names.ptr[i]);
        if (jz_btree_contains(&self->routes, &field)) str_set_insert(&carried, field);
        else jz_str_drop(&field);
    }
    jz_free(names.ptr);
    jz_btree copy = str_set_clone(&carried);
    jz_hmap_insert(&self->carried, &node, &copy, NULL);
    *out = carried;
    return JZ_OK;
}

/* Binding routes `node` depends on through a binding lookup. */
static jz_err jazz_node__node__query_eval__route_discipline__RouteChecker__masked(RouteChecker *self, GB node, jz_btree *out)
{
    const jz_btree *memo = jz_hmap_get(&self->masks, &node);
    if (memo) { *out = str_set_clone(memo); return JZ_OK; }
    jz_btree masked;
    if (node->tag == GGB_LOOKUP) {
        masked = jazz_node__node__query_eval__route_discipline__RouteChecker__depends(self, node->input);
    } else {
        masked = str_set_new();
        GBVec kids = jazz_node__node__query_eval__route_discipline__children(node);
        for (u32 i = 0; i < kids.len; i++) {
            jz_btree m;
            jz_err e = jazz_node__node__query_eval__route_discipline__RouteChecker__masked(self, kids.ptr[i], &m);
            if (JZ_IS_ERR(e)) { jz_free(kids.ptr); jz_btree_drop(&masked); return e; }
            jz_str k;
            while (jz_btree_pop_first(&m, &k, NULL)) str_set_insert(&masked, k);
            jz_btree_drop(&m);
        }
        jz_free(kids.ptr);
    }
    jz_btree copy = str_set_clone(&masked);
    jz_hmap_insert(&self->masks, &node, &copy, NULL);
    *out = masked;
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__route_discipline__RouteChecker__raw_field_name(const RouteChecker *self, GB input, const groove_FieldRef *field, jz_str *out)
{
    switch (field->tag) {
    case GFR_NAME:
    case GFR_STORED_NAME:
        *out = jz_str_clone(&field->name);
        return JZ_OK;
    case GFR_RESOLVED: {
        jz_str_vec names;
        JZ_TRY(jazz_node__node__query_eval__route_discipline__RouteChecker__output_names(self, input, &names));
        *out = field->index < names.len ? jz_str_clone(&names.ptr[field->index]) : (jz_str){0};
        jz_str_vec_drop(&names);
        return JZ_OK;
    }
    }
    JZ_UNREACHABLE();
}

/* The name of `field` in `input`, as the route it aliases if any. */
static jz_err jazz_node__node__query_eval__route_discipline__RouteChecker__field_name(const RouteChecker *self, GB input, const groove_FieldRef *field, jz_str *out)
{
    jz_str name;
    JZ_TRY(jazz_node__node__query_eval__route_discipline__RouteChecker__raw_field_name(self, input, field, &name));
    *out = jazz_node__node__query_eval__route_discipline__RouteChecker__route_name(self, name);
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__route_discipline__RouteChecker__equivalents(RouteChecker *self, GB node, jz_btree *out);

/* The route a join key stands for. */
static jz_err jazz_node__node__query_eval__route_discipline__RouteChecker__key_route(RouteChecker *self, GB input, const groove_FieldRef *field, jz_str *out)
{
    jz_str name;
    JZ_TRY(jazz_node__node__query_eval__route_discipline__RouteChecker__raw_field_name(self, input, field, &name));
    jz_btree equivalents;
    jz_err e = jazz_node__node__query_eval__route_discipline__RouteChecker__equivalents(self, input, &equivalents);
    if (JZ_IS_ERR(e)) { jz_str_drop(&name); return e; }
    const jz_str *eq = jz_btree_get(&equivalents, &name);
    if (eq) { *out = jz_str_clone(eq); jz_str_drop(&name); }
    else *out = jazz_node__node__query_eval__route_discipline__RouteChecker__route_name(self, name);
    jz_btree_drop(&equivalents);
    return JZ_OK;
}

static void str_map_insert(jz_btree *m, jz_str k, jz_str v)
{
    jz_str old;
    if (jz_btree_insert(m, &k, &v, &old)) { jz_str_drop(&k); jz_str_drop(&old); }
}

/* Fields of `node`'s output that equal a binding route in every row. */
static jz_err jazz_node__node__query_eval__route_discipline__RouteChecker__equivalents(RouteChecker *self, GB node, jz_btree *out)
{
    const jz_btree *memo = jz_hmap_get(&self->equivalents, &node);
    if (memo) { jz_btree_clone(out, memo, jazz_node_qeb_str_clone, jazz_node_qeb_str_clone); return JZ_OK; }
    jz_btree equivalents;
    jz_btree_init(&equivalents, &jazz_node_qeb_STR_STR_DESC);
    jz_err e = JZ_OK;
    switch (node->tag) {
    case GGB_JOIN: {
        static const char *const prefixes[2] = { "left.", "right." };
        GB sides[2] = { node->left, node->right };
        for (int s = 0; s < 2; s++) {
            jz_btree side;
            e = jazz_node__node__query_eval__route_discipline__RouteChecker__equivalents(self, sides[s], &side);
            if (JZ_IS_ERR(e)) goto fail;
            jz_str *f; jz_str *r;
            JZ_BTREE_FOR(&side, f, r) str_map_insert(&equivalents, jz_format("%s%.*s", prefixes[s], (int)f->len, f->ptr), jz_str_clone(r));
            jz_btree_drop(&side);
        }
        jz_btree left_carried, right_carried;
        e = jazz_node__node__query_eval__route_discipline__RouteChecker__carried(self, node->left, &left_carried);
        if (JZ_IS_ERR(e)) goto fail;
        e = jazz_node__node__query_eval__route_discipline__RouteChecker__carried(self, node->right, &right_carried);
        if (JZ_IS_ERR(e)) { jz_btree_drop(&left_carried); goto fail; }
        u32 nk = node->left_on.len < node->right_on.len ? node->left_on.len : node->right_on.len;
        for (u32 i = 0; i < nk && !JZ_IS_ERR(e); i++) {
            jz_str left_name = {0}, right_name = {0}, left_route = {0}, right_route = {0};
            e = jazz_node__node__query_eval__route_discipline__RouteChecker__raw_field_name(self, node->left, &node->left_on.ptr[i], &left_name);
            if (!JZ_IS_ERR(e)) e = jazz_node__node__query_eval__route_discipline__RouteChecker__raw_field_name(self, node->right, &node->right_on.ptr[i], &right_name);
            if (!JZ_IS_ERR(e)) e = jazz_node__node__query_eval__route_discipline__RouteChecker__key_route(self, node->left, &node->left_on.ptr[i], &left_route);
            if (!JZ_IS_ERR(e)) e = jazz_node__node__query_eval__route_discipline__RouteChecker__key_route(self, node->right, &node->right_on.ptr[i], &right_route);
            if (!JZ_IS_ERR(e)) {
                if (jz_btree_contains(&right_carried, &right_route) ||
                    (jz_btree_contains(&self->routes, &right_route) && !jz_str_eq(&right_route, &right_name)))
                    str_map_insert(&equivalents, jz_format("left.%.*s", (int)left_name.len, left_name.ptr), jz_str_clone(&right_route));
                if (jz_btree_contains(&left_carried, &left_route) ||
                    (jz_btree_contains(&self->routes, &left_route) && !jz_str_eq(&left_route, &left_name)))
                    str_map_insert(&equivalents, jz_format("right.%.*s", (int)right_name.len, right_name.ptr), jz_str_clone(&left_route));
            }
            jz_str_drop(&left_name); jz_str_drop(&right_name); jz_str_drop(&left_route); jz_str_drop(&right_route);
        }
        jz_btree_drop(&left_carried);
        jz_btree_drop(&right_carried);
        if (JZ_IS_ERR(e)) goto fail;
        break;
    }
    case GGB_PROJECT: {
        jz_btree input_equivalents;
        e = jazz_node__node__query_eval__route_discipline__RouteChecker__equivalents(self, node->input, &input_equivalents);
        if (JZ_IS_ERR(e)) goto fail;
        for (u32 i = 0; i < node->fields.len; i++) {
            const groove_ProjectField *field = &node->fields.ptr[i];
            switch (field->expression.tag) {
            case GPE_FIELD: case GPE_NULLABLE: case GPE_NULLABLE_FLAT: break;
            default: continue;
            }
            jz_str source;
            e = jazz_node__node__query_eval__route_discipline__RouteChecker__raw_field_name(self, node->input, &field->expression.field, &source);
            if (JZ_IS_ERR(e)) break;
            const jz_str *route = jz_btree_get(&input_equivalents, &source);
            if (route) str_map_insert(&equivalents, jz_str_clone(&field->output_name), jz_str_clone(route));
            jz_str_drop(&source);
        }
        jz_btree_drop(&input_equivalents);
        if (JZ_IS_ERR(e)) goto fail;
        break;
    }
    case GGB_FILTER: case GGB_UNWRAP_NULLABLE: case GGB_ARG_MAX_BY: case GGB_ARG_MIN_BY: case GGB_TOP_BY:
        jz_btree_drop(&equivalents);
        e = jazz_node__node__query_eval__route_discipline__RouteChecker__equivalents(self, node->input, &equivalents);
        if (JZ_IS_ERR(e)) return e;
        break;
    case GGB_SEMI_JOIN: case GGB_ANTI_JOIN:
        jz_btree_drop(&equivalents);
        e = jazz_node__node__query_eval__route_discipline__RouteChecker__equivalents(self, node->left, &equivalents);
        if (JZ_IS_ERR(e)) return e;
        break;
    default:
        break;
    }
    {
        jz_btree copy;
        jz_btree_clone(&copy, &equivalents, jazz_node_qeb_str_clone, jazz_node_qeb_str_clone);
        jz_hmap_insert(&self->equivalents, &node, &copy, NULL);
    }
    *out = equivalents;
    return JZ_OK;
fail:
    jz_btree_drop(&equivalents);
    return e;
}

static jz_str jazz_node__node__query_eval__route_discipline__sketch(GB node, u32 depth)
{
    jz_str dbg = {0};
    groove_GraphBuilder_debug(&dbg, node);
    u32 end = 0;
    while (end < dbg.len && dbg.ptr[end] != ' ' && dbg.ptr[end] != '{' && dbg.ptr[end] != '(') end++;
    jz_str name = jz_str_from_slice(jz_slice_of(dbg.ptr, end));
    jz_str_drop(&dbg);
    if (depth == 0) return name;
    GBVec kids = jazz_node__node__query_eval__route_discipline__children(node);
    if (kids.len == 0) { jz_free(kids.ptr); return name; }
    jz_str out = name;
    jz_str_push_char(&out, '[');
    for (u32 i = 0; i < kids.len; i++) {
        if (i) jz_str_push_cstr(&out, ", ");
        jz_str k = jazz_node__node__query_eval__route_discipline__sketch(kids.ptr[i], depth - 1);
        jz_str_push(&out, k.ptr, k.len);
        jz_str_drop(&k);
    }
    jz_str_push_char(&out, ']');
    jz_free(kids.ptr);
    return out;
}

static const char *jazz_node__node__query_eval__route_discipline__operator_name(GB node)
{
    switch (node->tag) {
    case GGB_RECURSIVE: return "Recursive";
    case GGB_RECURSIVE_STEP_WITNESS: return "RecursiveStepWitness";
    case GGB_TYPED_TEMPLATE: return "TypedTemplate";
    case GGB_TEMPLATE_INPUT: return "TemplateInput";
    case GGB_STREAMING_CHECKSUM: return "StreamingChecksum";
    case GGB_JOIN: return "join";
    case GGB_SEMI_JOIN: return "semi-join";
    case GGB_ANTI_JOIN: return "anti-join";
    default: return "operator";
    }
}

static jz_err group_names(RouteChecker *self, GB input, const groove_FieldRefVec *cols, jz_btree *out)
{
    *out = str_set_new();
    for (u32 i = 0; i < cols->len; i++) {
        jz_str n;
        jz_err e = jazz_node__node__query_eval__route_discipline__RouteChecker__field_name(self, input, &cols->ptr[i], &n);
        if (JZ_IS_ERR(e)) { jz_btree_drop(out); return e; }
        str_set_insert(out, n);
    }
    return JZ_OK;
}

static jz_err trace_names_suffix(RouteChecker *self, GB node, jz_str *reason)
{
    jz_str_vec ln, rn;
    JZ_TRY(jazz_node__node__query_eval__route_discipline__RouteChecker__output_names(self, node->left, &ln));
    jz_err e = jazz_node__node__query_eval__route_discipline__RouteChecker__output_names(self, node->right, &rn);
    if (JZ_IS_ERR(e)) { jz_str_vec_drop(&ln); return e; }
    jz_str sk = jazz_node__node__query_eval__route_discipline__sketch(node, 12);
    jz_str_appendf(reason, "sketch %.*s; left names ", (int)sk.len, sk.ptr);
    jazz_node_qeb_debug_str_vec(reason, ln.ptr, ln.len);
    jz_str_push_cstr(reason, ", right names ");
    jazz_node_qeb_debug_str_vec(reason, rn.ptr, rn.len);
    jz_str_push_char(reason, ')');
    jz_str_drop(&sk);
    jz_str_vec_drop(&ln);
    jz_str_vec_drop(&rn);
    return JZ_OK;
}

static jz_err jazz_node__node__query_eval__route_discipline__RouteChecker__check_node(RouteChecker *self, GB node)
{
    switch (node->tag) {
    case GGB_JOIN: case GGB_SEMI_JOIN: case GGB_ANTI_JOIN: {
        jz_btree left_depends = jazz_node__node__query_eval__route_discipline__RouteChecker__depends(self, node->left);
        jz_btree right_depends = jazz_node__node__query_eval__route_discipline__RouteChecker__depends(self, node->right);
        jz_err e = JZ_OK;
        jz_btree left_carried = str_set_new(), right_carried = str_set_new(), keyed = str_set_new(), required = str_set_new();
        if (left_depends.len == 0 && right_depends.len == 0) goto join_done;
        /* A binding-dependent input must carry the routes it depends on. */
        jz_btree_drop(&left_carried); jz_btree_drop(&right_carried);
        e = jazz_node__node__query_eval__route_discipline__RouteChecker__carried(self, node->left, &left_carried);
        if (JZ_IS_ERR(e)) { right_carried = str_set_new(); goto join_done; }
        e = jazz_node__node__query_eval__route_discipline__RouteChecker__carried(self, node->right, &right_carried);
        if (JZ_IS_ERR(e)) { right_carried = str_set_new(); goto join_done; }
        {
            const char *side_names[2] = { "left", "right" };
            const jz_btree *deps[2] = { &left_depends, &right_depends };
            const jz_btree *car[2] = { &left_carried, &right_carried };
            for (int s = 0; s < 2; s++) {
                if (str_set_is_subset(deps[s], car[s])) continue;
                jz_str reason = jz_format("%s %s input depends on binding routes ", jazz_node__node__query_eval__route_discipline__operator_name(node), side_names[s]);
                jazz_node_qeb_debug_str_set(&reason, deps[s]);
                jz_str_push_cstr(&reason, " but carries ");
                jazz_node_qeb_debug_str_set(&reason, car[s]);
                if (jazz_types__debug_env__covered_input_trace()) {
                    jz_str_push_cstr(&reason, " (");
                    e = trace_names_suffix(self, node, &reason);
                    if (JZ_IS_ERR(e)) { jz_str_drop(&reason); goto join_done; }
                }
                e = RV("%.*s", (int)reason.len, reason.ptr);
                jz_str_drop(&reason);
                goto join_done;
            }
        }
        {
            u32 nk = node->left_on.len < node->right_on.len ? node->left_on.len : node->right_on.len;
            for (u32 i = 0; i < nk; i++) {
                jz_str lk, rk;
                e = jazz_node__node__query_eval__route_discipline__RouteChecker__key_route(self, node->left, &node->left_on.ptr[i], &lk);
                if (JZ_IS_ERR(e)) goto join_done;
                e = jazz_node__node__query_eval__route_discipline__RouteChecker__key_route(self, node->right, &node->right_on.ptr[i], &rk);
                if (JZ_IS_ERR(e)) { jz_str_drop(&lk); goto join_done; }
                if (jz_str_eq(&lk, &rk)) str_set_insert(&keyed, lk); else jz_str_drop(&lk);
                jz_str_drop(&rk);
            }
        }
        /* An inner join must pair each route both sides carry. A semi or anti
         * join emits left rows only, so every route its right input depends on
         * must also key the left row. */
        if (node->tag == GGB_JOIN) {
            jz_str *k; void *v;
            JZ_BTREE_FOR(&left_carried, k, v) if (jz_btree_contains(&right_carried, k)) str_set_insert(&required, jz_str_clone(k));
        } else {
            jz_btree_drop(&required);
            required = str_set_clone(&right_depends);
        }
        {
            const jz_str *route = str_set_difference_first(&required, &keyed);
            if (route) {
                jz_str reason = jz_format("join does not key on binding route %.*s", (int)route->len, route->ptr);
                if (jazz_types__debug_env__covered_input_trace()) {
                    jz_str_push_cstr(&reason, " (keys [");
                    u32 nk = node->left_on.len < node->right_on.len ? node->left_on.len : node->right_on.len;
                    for (u32 i = 0; i < nk; i++) {
                        jz_str l, r;
                        e = jazz_node__node__query_eval__route_discipline__RouteChecker__field_name(self, node->left, &node->left_on.ptr[i], &l);
                        if (JZ_IS_ERR(e)) { jz_str_drop(&reason); goto join_done; }
                        e = jazz_node__node__query_eval__route_discipline__RouteChecker__field_name(self, node->right, &node->right_on.ptr[i], &r);
                        if (JZ_IS_ERR(e)) { jz_str_drop(&l); jz_str_drop(&reason); goto join_done; }
                        if (i) jz_str_push_cstr(&reason, ", ");
                        jz_str_push_char(&reason, '(');
                        jazz_node_qeb_debug_str(&reason, &l);
                        jz_str_push_cstr(&reason, ", ");
                        jazz_node_qeb_debug_str(&reason, &r);
                        jz_str_push_char(&reason, ')');
                        jz_str_drop(&l); jz_str_drop(&r);
                    }
                    jz_btree left_equivalents;
                    e = jazz_node__node__query_eval__route_discipline__RouteChecker__equivalents(self, node->left, &left_equivalents);
                    if (JZ_IS_ERR(e)) { jz_str_drop(&reason); goto join_done; }
                    jz_str_push_cstr(&reason, "], left carries ");
                    jazz_node_qeb_debug_str_set(&reason, &left_carried);
                    jz_str_push_cstr(&reason, ", right carries ");
                    jazz_node_qeb_debug_str_set(&reason, &right_carried);
                    jz_str_push_cstr(&reason, ", left equivalents ");
                    debug_str_map(&reason, &left_equivalents);
                    jz_str_push_cstr(&reason, ", ");
                    jz_btree_drop(&left_equivalents);
                    e = trace_names_suffix(self, node, &reason);
                    if (JZ_IS_ERR(e)) { jz_str_drop(&reason); goto join_done; }
                }
                e = RV("%.*s", (int)reason.len, reason.ptr);
                jz_str_drop(&reason);
            }
        }
    join_done:
        jz_btree_drop(&left_depends); jz_btree_drop(&right_depends);
        jz_btree_drop(&left_carried); jz_btree_drop(&right_carried);
        jz_btree_drop(&keyed); jz_btree_drop(&required);
        return e;
    }
    case GGB_AGGREGATE: case GGB_ARG_MAX_BY: case GGB_ARG_MIN_BY: case GGB_TOP_BY: {
        if (node->tag == GGB_TOP_BY && node->offset == 0 && node->limit_tag == GTBL_UNBOUNDED)
            return JZ_OK; /* Orders without excluding any row: nothing to separate. */
        /* Grouping across bindings would aggregate, rank or page one
         * subscriber's rows together with another's. */
        jz_btree depends = jazz_node__node__query_eval__route_discipline__RouteChecker__depends(self, node->input);
        if (depends.len == 0) { jz_btree_drop(&depends); return JZ_OK; }
        jz_btree groups;
        jz_err e = group_names(self, node->input, &node->group_cols, &groups);
        if (JZ_IS_ERR(e)) { jz_btree_drop(&depends); return e; }
        const jz_str *route = str_set_difference_first(&depends, &groups);
        if (route) e = RV("grouping does not group by binding route %.*s", (int)route->len, route->ptr);
        jz_btree_drop(&groups);
        jz_btree_drop(&depends);
        return e;
    }
    case GGB_COLLECT_BY: {
        /* Each collection must belong to one binding's parent record. */
        jz_btree depends = jazz_node__node__query_eval__route_discipline__RouteChecker__depends(self, node->input);
        if (depends.len == 0) { jz_btree_drop(&depends); return JZ_OK; }
        JZ_VEC(const groove_FieldRefVec *) groupings = {0};
        JZ_VEC(const groove_CollectByBuilder *) slots = {0};
        const groove_FieldRefVec *g0 = &node->collect->group_cols;
        jz_vec_push(&groupings, &g0, sizeof g0);
        for (u32 i = 0; i < node->collect->slots.len; i++) { const groove_CollectByBuilder *s = &node->collect->slots.ptr[i]; jz_vec_push(&slots, &s, sizeof s); }
        while (slots.len) {
            const groove_CollectByBuilder *slot = slots.ptr[--slots.len];
            const groove_FieldRefVec *g = &slot->group_cols;
            jz_vec_push(&groupings, &g, sizeof g);
            for (u32 i = 0; i < slot->slots.len; i++) { const groove_CollectByBuilder *s = &slot->slots.ptr[i]; jz_vec_push(&slots, &s, sizeof s); }
        }
        jz_err e = JZ_OK;
        for (u32 i = 0; i < groupings.len && !JZ_IS_ERR(e); i++) {
            jz_btree groups;
            e = group_names(self, node->input, groupings.ptr[i], &groups);
            if (JZ_IS_ERR(e)) break;
            const jz_str *route = str_set_difference_first(&depends, &groups);
            if (route) e = RV("collection does not group by binding route %.*s", (int)route->len, route->ptr);
            jz_btree_drop(&groups);
        }
        jz_free(groupings.ptr);
        jz_free(slots.ptr);
        jz_btree_drop(&depends);
        return e;
    }
    case GGB_RECURSIVE: case GGB_RECURSIVE_STEP_WITNESS: case GGB_STREAMING_CHECKSUM: case GGB_TYPED_TEMPLATE: case GGB_TEMPLATE_INPUT: {
        /* Not route-aware yet: keep them private to one binding. */
        jz_btree depends = jazz_node__node__query_eval__route_discipline__RouteChecker__depends(self, node);
        jz_err e = JZ_OK;
        if (depends.len) {
            jz_str d = {0};
            jazz_node_qeb_debug_str_set(&d, &depends);
            e = RV("%s is not route-aware but depends on binding routes %.*s", jazz_node__node__query_eval__route_discipline__operator_name(node), (int)d.len, d.ptr);
            jz_str_drop(&d);
        }
        jz_btree_drop(&depends);
        return e;
    }
    default:
        return JZ_OK;
    }
}

static jz_err jazz_node__node__query_eval__route_discipline__RouteChecker__check(RouteChecker *self, GB root)
{
    GBVec pending = {0};
    jz_vec_push(&pending, &root, sizeof root);
    jz_err e = JZ_OK;
    while (pending.len) {
        GB node = pending.ptr[--pending.len];
        if (jz_btree_contains(&self->checked, &node)) continue;
        jz_btree_insert(&self->checked, &node, NULL, NULL);
        GBVec kids = jazz_node__node__query_eval__route_discipline__children(node);
        for (u32 i = 0; i < kids.len; i++) jz_vec_push(&pending, &kids.ptr[i], sizeof(GB));
        jz_free(kids.ptr);
        e = jazz_node__node__query_eval__route_discipline__RouteChecker__check_node(self, node);
        if (JZ_IS_ERR(e)) break;
    }
    jz_free(pending.ptr);
    return e;
}

/* Checks that every terminal keeps each binding's rows apart. */
jz_err jazz_node__node__query_eval__route_discipline__check_route_discipline(
    jazz_node_RouteTerminal *terminals, u32 n, const jz_btree *slot_routes, jazz_node_RouteInferFn infer, void *infer_ctx)
{
    RouteChecker checker;
    checker.slot_routes = slot_routes;
    checker.aliases = jazz_node__node__query_eval__route_discipline__slot_aliases(terminals, n, slot_routes, infer, infer_ctx);
    checker.routes = str_set_new();
    jz_str *k; jz_str *v;
    JZ_BTREE_FOR(slot_routes, k, v) str_set_insert(&checker.routes, jz_str_clone(v));
    checker.infer = infer;
    checker.infer_ctx = infer_ctx;
    jz_hmap_init(&checker.deps, &PTR_SET_MAP_DESC);
    jz_hmap_init(&checker.carried, &PTR_SET_MAP_DESC);
    jz_hmap_init(&checker.masks, &PTR_SET_MAP_DESC);
    jz_hmap_init(&checker.equivalents, &PTR_SET_MAP_DESC);
    jz_btree_init(&checker.checked, &PTR_SET_DESC);
    jz_err e = JZ_OK;
    for (u32 i = 0; i < n; i++) {
        jazz_node_RouteTerminal *t = &terminals[i];
        jz_btree depends = jazz_node__node__query_eval__route_discipline__RouteChecker__depends(&checker, t->graph);
        if (!str_set_is_subset(&depends, &t->delivered)) {
            jz_str d = {0}, dl = {0};
            jazz_node_qeb_debug_str_set(&d, &depends);
            jazz_node_qeb_debug_str_set(&dl, &t->delivered);
            e = RV("terminal %.*s depends on binding routes %.*s but delivers by %.*s", (int)t->sink_len, t->sink, (int)d.len, d.ptr, (int)dl.len, dl.ptr);
            jz_str_drop(&d); jz_str_drop(&dl);
            jz_btree_drop(&depends);
            break;
        }
        jz_btree_drop(&depends);
        e = jazz_node__node__query_eval__route_discipline__RouteChecker__check(&checker, t->graph);
        if (JZ_IS_ERR(e)) break;
        jz_btree masked;
        e = jazz_node__node__query_eval__route_discipline__RouteChecker__masked(&checker, t->graph, &masked);
        if (JZ_IS_ERR(e)) break;
        if (masked.len) {
            jz_str m = {0};
            jazz_node_qeb_debug_str_set(&m, &masked);
            e = RV("terminal %.*s reads binding-keyed lookup rows for routes %.*s, which shared programs do not support yet", (int)t->sink_len, t->sink, (int)m.len, m.ptr);
            jz_str_drop(&m);
        }
        jz_btree_drop(&masked);
        if (JZ_IS_ERR(e)) break;
    }
    jz_btree_drop(&checker.aliases);
    jz_btree_drop(&checker.routes);
    jz_hmap_drop(&checker.deps);
    jz_hmap_drop(&checker.carried);
    jz_hmap_drop(&checker.masks);
    jz_hmap_drop(&checker.equivalents);
    jz_btree_drop(&checker.checked);
    return e;
}

/* ---------- program binding sources ---------- */
static u64 jazz_node__node__query_eval__route_discipline__program_binding_suffix_hash(const jazz_engine_QueryProgram *program)
{
    u64 h = 0;
    for (u32 i = 0; i < program->lowered.terminals.len; i++) {
        const jazz_engine_LoweredTerminal *t = &program->lowered.terminals.ptr[i];
        h = jz_hash_combine(h, jz_str_hash(&t->sink));
        h = groove_GraphBuilder_hash_into(t->graph, h);
    }
    h = jz_hash_combine(h, program->lowered.internal_app_rows_graph != NULL);
    if (program->lowered.internal_app_rows_graph) h = groove_GraphBuilder_hash_into(program->lowered.internal_app_rows_graph, h);
    return h;
}
/* A binding-source suffix naming `program` by its lowered graphs. */
static jz_str jazz_node__node__query_eval__route_discipline__program_binding_suffix(const jazz_engine_QueryProgram *program)
{
    return jz_format(":program:%016llx", (unsigned long long)jazz_node__node__query_eval__route_discipline__program_binding_suffix_hash(program));
}

/* The names of the binding sources `program`'s graphs read. */
static jz_btree jazz_node__node__query_eval__route_discipline__program_binding_source_names(const jazz_engine_QueryProgram *program)
{
    jz_btree names = str_set_new();
    jz_btree seen;
    jz_btree_init(&seen, &PTR_SET_DESC);
    GBVec pending = {0};
    for (u32 i = 0; i < program->lowered.terminals.len; i++) jz_vec_push(&pending, &program->lowered.terminals.ptr[i].graph, sizeof(GB));
    if (program->lowered.internal_app_rows_graph) jz_vec_push(&pending, &program->lowered.internal_app_rows_graph, sizeof(GB));
    while (pending.len) {
        GB node = pending.ptr[--pending.len];
        if (jz_btree_contains(&seen, &node)) continue;
        jz_btree_insert(&seen, &node, NULL, NULL);
        if (node->tag == GGB_BINDING_SOURCE) str_set_insert(&names, jz_str_clone(&node->shape));
        GBVec kids = jazz_node__node__query_eval__route_discipline__children(node);
        for (u32 i = 0; i < kids.len; i++) jz_vec_push(&pending, &kids.ptr[i], sizeof(GB));
        jz_free(kids.ptr);
    }
    jz_free(pending.ptr);
    jz_btree_drop(&seen);
    return names;
}

typedef groove_GraphBuilder *(*MapFn)(GB node, void *ctx);

/* rewrite_inputs: replace every child slot with arc(child). */
static void jazz_node__node__query_eval__route_discipline__rewrite_inputs(groove_GraphBuilder *node, jz_hmap *rewritten)
{
    Slot *slots = NULL;
    u32 n = jazz_node__node__query_eval__route_discipline__child_arcs(node, &slots);
    for (u32 i = 0; i < n; i++) {
        groove_GraphBuilder *child = *slots[i];
        const RewrittenPair *p = jz_hmap_get(rewritten, &child);
        if (!p) jz_panic("key not found in HashMap");
        groove_GraphBuilder *replacement = jz_rc_clone(p->done);
        jz_rc_release(child);
        *slots[i] = replacement;
    }
    if (n) jz_free(slots);
}

/* Returns the node (borrowed, *owned=false) or a new owned node. */
static groove_GraphBuilder *jazz_node__node__query_eval__route_discipline__rewrite_node(GB node, MapFn map, void *ctx, jz_hmap *rewritten, bool *owned)
{
    groove_GraphBuilder *mapped = NULL;
    bool changed_inputs = false;
    Slot *slots = NULL;
    u32 n = jazz_node__node__query_eval__route_discipline__child_arcs((groove_GraphBuilder *)node, &slots);
    for (u32 i = 0; i < n; i++) {
        groove_GraphBuilder *child = *slots[i];
        if (!jz_hmap_get(rewritten, &child)) {
            bool child_owned;
            groove_GraphBuilder *r = jazz_node__node__query_eval__route_discipline__rewrite_node(child, map, ctx, rewritten, &child_owned);
            groove_GraphBuilder *done;
            if (!child_owned) done = jz_rc_clone(child);
            else {
                /* Arc::new(done) */
                done = r;
            }
            /* Keep the original allocation alive so its address stays valid. */
            RewrittenPair pair = { jz_rc_clone(child), done };
            jz_hmap_insert(rewritten, &child, &pair, NULL);
        }
        const RewrittenPair *p = jz_hmap_get(rewritten, &child);
        changed_inputs |= p->done != child;
    }
    if (n) jz_free(slots);
    if (changed_inputs) {
        mapped = groove_GraphBuilder_clone(node);
        jazz_node__node__query_eval__route_discipline__rewrite_inputs(mapped, rewritten);
    }
    groove_GraphBuilder *replacement = map(mapped ? mapped : node, ctx);
    if (replacement) {
        if (mapped) groove_GraphBuilder_drop(mapped);
        mapped = replacement;
    }
    *owned = mapped != NULL;
    return mapped ? mapped : (groove_GraphBuilder *)node;
}

/* Applies `map` bottom-up, copying only changed nodes and their ancestors. */
static groove_GraphBuilder *jazz_node__node__query_eval__route_discipline__rewrite_graph(GB graph, MapFn map, void *ctx, jz_hmap *rewritten)
{
    bool owned;
    groove_GraphBuilder *r = jazz_node__node__query_eval__route_discipline__rewrite_node(graph, map, ctx, rewritten, &owned);
    return owned ? r : groove_GraphBuilder_clone(r);
}

static groove_GraphBuilder *isolate_map(GB node, void *ctx)
{
    const jz_str *suffix = ctx;
    if (node->tag != GGB_BINDING_SOURCE) return NULL;
    groove_GraphBuilder *g = jz_rc_new(sizeof(groove_GraphBuilder), (void (*)(void *))groove_GraphBuilder_drop);
    memset(g, 0, sizeof *g);
    g->tag = GGB_BINDING_SOURCE;
    g->shape = jz_format("%.*s%.*s", (int)node->shape.len, node->shape.ptr, (int)suffix->len, suffix->ptr);
    g->output = groove_RecordDescriptor_clone(node->output);
    return g;
}

/* Renames every binding source `graph` reads by appending `suffix`. */
groove_GraphBuilder *jazz_node__node__query_eval__route_discipline__isolate_binding_sources(const groove_GraphBuilder *graph, const jz_str *suffix, jz_hmap *rewritten)
{
    return jazz_node__node__query_eval__route_discipline__rewrite_graph(graph, isolate_map, (void *)suffix, rewritten);
}

static groove_GraphBuilder *jazz_node__node__query_eval__route_discipline__expand_templates(GB graph);
static groove_GraphBuilder *expand_map(GB node, void *ctx)
{
    switch (node->tag) {
    case GGB_TYPED_TEMPLATE: {
        groove_GraphBuilder *expanded;
        jz_err e = groove__ivm__template__TypedGraphTemplate__bind_declarative_with_arguments(node->program, &node->inputs, &node->predicates, node->scalars, &expanded);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); return NULL; }
        groove_GraphBuilder *r = jazz_node__node__query_eval__route_discipline__expand_templates(expanded);
        groove_GraphBuilder_drop(expanded);
        return r;
    }
    case GGB_TEMPLATE_INPUT:
        if (node->input) return groove_GraphBuilder_clone(node->input);
        return NULL;
    default:
        return NULL;
    }
}
static groove_GraphBuilder *jazz_node__node__query_eval__route_discipline__expand_templates_with_memo(GB graph, jz_hmap *rewritten)
{
    return jazz_node__node__query_eval__route_discipline__rewrite_graph(graph, expand_map, NULL, rewritten);
}
/* `graph` with every typed template replaced by the declarative operators it was compiled from. */
static groove_GraphBuilder *jazz_node__node__query_eval__route_discipline__expand_templates(GB graph)
{
    jz_hmap memo;
    jz_hmap_init(&memo, &jazz_node_qeb_REWRITTEN_DESC);
    groove_GraphBuilder *r = jazz_node__node__query_eval__route_discipline__expand_templates_with_memo(graph, &memo);
    jz_hmap_drop(&memo);
    return r;
}

static jz_str jazz_node__node__query_eval__route_discipline__NodeState__isolated_binding_suffix(const jazz_node_NodeState *self, const jazz_engine_QueryProgramRequest *request)
{
    jz_str session;
    if (request->policy.tag == JZ_PC_IDENTITY) {
        if (!jazz_node__node__NodeState__active_session_claim_scope_key(self, request->policy.permission_subject, &session)) {
            session = (jz_str){0};
            jazz_types_AuthorSubject_debug(&session, &request->policy.permission_subject);
        }
    } else {
        session = jz_str_from("system");
    }
    jz_blake3_hasher *h = jz_blake3_new();
    static const char tag[] = "jazz/isolated-binding-source/v1";
    jz_blake3_update(h, (const u8 *)tag, sizeof tag); /* includes the trailing \0 */
    jz_blake3_update(h, (const u8 *)session.ptr, session.len);
    jz_str id = {0};
    jazz_model_BindingId_debug(&id, &request->binding.id);
    jz_blake3_update(h, (const u8 *)id.ptr, id.len);
    u8 out[32];
    jz_blake3_finalize(h, out);
    jz_blake3_free(h);
    jz_str hex = {0};
    jz_hex_encode(out, 32, &hex);
    jz_str r = jz_format(":isolated:%.*s", (int)hex.len, hex.ptr);
    jz_str_drop(&hex);
    jz_str_drop(&id);
    jz_str_drop(&session);
    return r;
}

/* Names the binding source `program` reads after the program itself. */
void jazz_node__node__query_eval__route_discipline__NodeState__scope_program_binding_source(const jazz_node_NodeState *self, jazz_engine_QueryProgram *program)
{
    jz_btree read_sources = jazz_node__node__query_eval__route_discipline__program_binding_source_names(program);
    jz_str source_shape;
    if (program->request.binding.has_source_shape) {
        source_shape = jz_str_clone(&program->request.binding.source_shape);
    } else {
        jz_str *first; void *v;
        if (!jz_btree_first(&read_sources, (void **)&first, &v)) { jz_btree_drop(&read_sources); return; }
        source_shape = jz_str_clone(first);
    }
    if (program->request.policy.tag == JZ_PC_AUTHORIZATION_SUBPLAN) {
        /* A subplan is embedded in, and checked with, its outer program. */
        jz_str_drop(&source_shape);
        jz_btree_drop(&read_sources);
        return;
    }
    jz_err discipline;
    if (read_sources.len > 1) {
        jz_str d = {0};
        jazz_node_qeb_debug_str_set(&d, &read_sources);
        discipline = RV("program reads several binding sources %.*s", (int)d.len, d.ptr);
        jz_str_drop(&d);
    } else {
        discipline = jazz_node__node__query_eval__route_discipline__NodeState__check_program_route_discipline(self, program);
    }
    jz_btree_drop(&read_sources);
    jz_str suffix;
    if (!JZ_IS_ERR(discipline)) {
        if (program->request.authorization_mode == JZ_QAM_CLIENT_LOCAL) { jz_str_drop(&source_shape); return; }
        suffix = jazz_node__node__query_eval__route_discipline__program_binding_suffix(program);
    } else {
        if (jazz_types__debug_env__covered_input_trace()) {
            const jz_str *m = discipline.msg;
            jz_str line = jz_format("JAZZ_COVERED_INPUT_TRACE stage=isolate_binding_source source_shape=%.*s reason=RouteViolation(\"%.*s\")",
                                    (int)source_shape.len, source_shape.ptr, m ? (int)m->len : 0, m ? m->ptr : "");
            jz_js_log(1, line.ptr, line.len); /* eprintln! */
            jz_str_drop(&line);
        }
        jz_err_drop(&discipline);
        suffix = jazz_node__node__query_eval__route_discipline__NodeState__isolated_binding_suffix(self, &program->request);
    }
    /* One suffix applies to the whole program. */
    jz_hmap rewritten;
    jz_hmap_init(&rewritten, &jazz_node_qeb_REWRITTEN_DESC);
    for (u32 i = 0; i < program->lowered.terminals.len; i++) {
        jazz_engine_LoweredTerminal *t = &program->lowered.terminals.ptr[i];
        groove_GraphBuilder *g = jazz_node__node__query_eval__route_discipline__isolate_binding_sources(t->graph, &suffix, &rewritten);
        groove_GraphBuilder_drop(t->graph);
        t->graph = g;
    }
    if (program->lowered.internal_app_rows_graph) {
        groove_GraphBuilder *g = jazz_node__node__query_eval__route_discipline__isolate_binding_sources(program->lowered.internal_app_rows_graph, &suffix, &rewritten);
        groove_GraphBuilder_drop(program->lowered.internal_app_rows_graph);
        program->lowered.internal_app_rows_graph = g;
    }
    jz_hmap_drop(&rewritten);
    jz_str scoped = jz_format("%.*s%.*s", (int)source_shape.len, source_shape.ptr, (int)suffix.len, suffix.ptr);
    jz_str_drop(&source_shape);
    jz_str_drop(&suffix);
    if (program->request.binding.has_source_shape) jz_str_drop(&program->request.binding.source_shape);
    program->request.binding.has_source_shape = true;
    program->request.binding.source_shape = scoped;
}

static jz_err infer_via_database(const groove_GraphBuilder *g, groove_RecordDescriptor **out, void *ctx)
{
    const jazz_node_NodeState *self = ctx;
    return groove__db__query__Database__graph_output_descriptor(self->database, g, out);
}

jz_err jazz_node__node__query_eval__route_discipline__NodeState__check_program_route_discipline(const jazz_node_NodeState *self, const jazz_engine_QueryProgram *program)
{
    const jazz_engine_ParameterDomain *parameters = &program->lowered.parameters;
    jz_str_vec route_params = jazz_node__node__query_eval__lowering__prepared_route_param_names(parameters);
    /* Expansion depends only on each immutable input, not its terminal. */
    jz_hmap expanded_inputs;
    jz_hmap_init(&expanded_inputs, &jazz_node_qeb_REWRITTEN_DESC);
    u32 nt = program->lowered.terminals.len;
    groove_GraphBuilder **expanded = jz_alloc(sizeof(void *) * (nt + 1));
    for (u32 i = 0; i < nt; i++)
        expanded[i] = jazz_node__node__query_eval__route_discipline__expand_templates_with_memo(program->lowered.terminals.ptr[i].graph, &expanded_inputs);
    groove_GraphBuilder *expanded_app_rows = program->lowered.internal_app_rows_graph
        ? jazz_node__node__query_eval__route_discipline__expand_templates_with_memo(program->lowered.internal_app_rows_graph, &expanded_inputs)
        : NULL;
    JZ_VEC(jazz_node_RouteTerminal) terminals = {0};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < nt; i++) {
        const jazz_engine_LoweredTerminal *t = &program->lowered.terminals.ptr[i];
        jz_str_vec eligible;
        e = jazz_node__node__query_eval__lowering__terminal_route_eligible_fields(&t->output, &eligible);
        if (JZ_IS_ERR(e)) {
            jz_str d = {0};
            jazz_node_Error_debug(&d, &e);
            jz_err_drop(&e);
            e = RV("%.*s", (int)d.len, d.ptr);
            jz_str_drop(&d);
            goto done;
        }
        jz_str_vec fields = jazz_node__node__query_eval__lowering__terminal_route_fields(route_params.ptr, route_params.len, eligible.ptr, eligible.len);
        jazz_node_RouteTerminal rt = { t->sink.ptr, t->sink.len, expanded[i], str_set_new() };
        for (u32 f = 0; f < fields.len; f++) str_set_insert(&rt.delivered, fields.ptr[f]);
        jz_free(fields.ptr);
        jz_str_vec_drop(&eligible);
        jz_vec_push(&terminals, &rt, sizeof rt);
    }
    if (expanded_app_rows) {
        jz_str_vec eligible;
        e = jazz_node__node__query_eval__lowering__app_row_terminal_route_eligible_fields(&program->lowered.output, &eligible);
        if (JZ_IS_ERR(e)) {
            jz_str d = {0};
            jazz_node_Error_debug(&d, &e);
            jz_err_drop(&e);
            e = RV("%.*s", (int)d.len, d.ptr);
            jz_str_drop(&d);
            goto done;
        }
        jazz_node_RouteTerminal rt = { "internal-app-rows", 17, expanded_app_rows, str_set_new() };
        for (u32 p = 0; p < route_params.len; p++) {
            bool contained = false;
            for (u32 f = 0; f < eligible.len; f++) if (jz_str_eq(&eligible.ptr[f], &route_params.ptr[p])) { contained = true; break; }
            if (contained) str_set_insert(&rt.delivered, jz_str_clone(&route_params.ptr[p]));
        }
        jz_str_vec_drop(&eligible);
        jz_vec_push(&terminals, &rt, sizeof rt);
    }
    {
        jz_btree slot_routes = jazz_node__node__query_eval__route_discipline__binding_slot_routes(parameters);
        e = jazz_node__node__query_eval__route_discipline__check_route_discipline(terminals.ptr, terminals.len, &slot_routes, infer_via_database, (void *)self);
        jz_btree_drop(&slot_routes);
    }
done:
    JZ_VEC_FOR(jazz_node_RouteTerminal, t, terminals) jz_btree_drop(&t->delivered);
    jz_free(terminals.ptr);
    for (u32 i = 0; i < nt; i++) groove_GraphBuilder_drop(expanded[i]);
    jz_free(expanded);
    if (expanded_app_rows) groove_GraphBuilder_drop(expanded_app_rows);
    jz_hmap_drop(&expanded_inputs);
    jz_str_vec_drop(&route_params);
    return e;
}
