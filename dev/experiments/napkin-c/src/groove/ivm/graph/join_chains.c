/* join_chains.c: port of crates/groove/src/ivm/graph/join_chains.rs.
 * Demand-driven narrowing of flattened inner-join chains. */
#include "jz_rt.h"
#include "../../_ext_groove-ivm-db.h"
#include "../op_types.h"
#include "../graph.h"

static const char LEFT[] = "left.";
static const char RIGHT[] = "right.";

/* type Narrowed = HashMap<(*const GraphBuilder, BTreeSet<String>), Option<Arc<GraphBuilder>>> */
typedef struct NarrowKey { const groove_GraphBuilder *node; jz_btree demand; } NarrowKey;
static u64 narrow_key_hash(const void *k) {
    const NarrowKey *key = k;
    u64 h = jz_hash_u64((u64)(uintptr_t)key->node);
    h = jz_hash_combine(h, key->demand.len);
    jz_str *name; void *unused;
    JZ_BTREE_FOR(&key->demand, name, unused) h = jz_hash_combine(h, jz_str_hash(name));
    return h;
}
static bool narrow_key_eq(const void *a, const void *b) {
    const NarrowKey *x = a, *y = b;
    return x->node == y->node && jz_btree_eq(&x->demand, &y->demand, NULL);
}
static void narrow_key_drop(void *k) { jz_btree_drop(&((NarrowKey *)k)->demand); }
static void narrow_val_drop(void *v) { groove__ivm__graph__arc_release(*(groove_GraphArc *)v); }
static const jz_hmap_desc NARROWED_DESC = { sizeof(NarrowKey), sizeof(groove_GraphArc), narrow_key_hash, narrow_key_eq, narrow_key_drop, narrow_val_drop };

static void strset_insert(jz_btree *set, jz_str name) {
    if (jz_btree_contains(set, &name)) { jz_str_drop(&name); return; }
    jz_btree_insert(set, &name, NULL, NULL);
}
static void strset_clone(jz_btree *dst, const jz_btree *src) { jz_btree_clone(dst, src, groove__ivm__jz_str_clone_v, NULL); }

static bool field_ref_name(const groove_FieldRef *field, jz_str *out) {
    switch (field->tag) {
    case GROOVE_FIELDREF_NAME: case GROOVE_FIELDREF_STORED_NAME: *out = jz_str_clone(&field->name); return true;
    default: return false; /* Resolved(_) */
    }
}

static const groove_FieldRef *project_expr_source(const groove_ProjectExpr *expression) {
    switch (expression->tag) {
    case GROOVE_PEXPR_FIELD: case GROOVE_PEXPR_NULLABLE: case GROOVE_PEXPR_NULLABLE_FLAT: return &expression->u.field;
    case GROOVE_PEXPR_RECORD_FIELD: return &expression->u.record_field.source;
    case GROOVE_PEXPR_ENUM_TAG_REMAP: return &expression->u.enum_tag_remap.source;
    case GROOVE_PEXPR_ENUM_REMAP: return &expression->u.enum_remap.source;
    case GROOVE_PEXPR_RECURSIVE_ENUM_REMAP: return &expression->u.recursive.source;
    default: return NULL; /* Literal / TypedLiteral / Null / TemplateArgument */
    }
}

/* The input names a projection reads, or None if one is positional. */
static bool demanded_names(const groove_ProjectField *fields, u32 n, jz_btree *names) {
    jz_btree_init(names, &groove__ivm__STRSET_DESC);
    for (u32 i = 0; i < n; i++) {
        const groove_FieldRef *source = project_expr_source(&fields[i].expression);
        if (source) {
            jz_str name;
            if (!field_ref_name(source, &name)) { jz_btree_drop(names); return false; }
            strset_insert(names, name);
        }
    }
    return true;
}

/* Whether a consumer naming `name` can resolve to this output field. */
static bool provides(const groove_ProjectField *field, const jz_str *name) {
    const jz_str *logical;
    switch (field->output_identity.tag) {
    case GROOVE_FIELD_ID_NAME: case GROOVE_FIELD_ID_NAMED_SLOT: logical = &field->output_identity.name; break;
    default: logical = &field->output_name; break; /* Slot(_) */
    }
    return jz_str_eq(&field->output_name, name) || jz_str_eq(logical, name);
}

/* The input fields a join side must provide: the demanded outputs of that
 * side, without their side prefix, plus its keys. */
static bool join_side_demand(const jz_btree *demand, const char *prefix, const groove_FieldRefVec *keys, jz_btree *side) {
    u32 plen = (u32)strlen(prefix);
    jz_btree_init(side, &groove__ivm__STRSET_DESC);
    jz_str *name; void *unused;
    JZ_BTREE_FOR(demand, name, unused) {
        if (name->len >= plen && memcmp(name->ptr, prefix, plen) == 0) {
            jz_slice rest = { (const u8 *)name->ptr + plen, name->len - plen };
            strset_insert(side, jz_str_from_slice(rest));
        }
    }
    for (u32 i = 0; i < keys->len; i++) {
        jz_str key;
        if (!field_ref_name(&keys->ptr[i], &key)) { jz_btree_drop(side); return false; }
        strset_insert(side, key);
    }
    return true;
}

static groove_GraphArc narrow_chain(const groove_GraphBuilder *node, const jz_btree *demand, jz_hmap *narrowed);

static groove_GraphArc narrow_input_uncached(groove_GraphArc node, const jz_btree *demand, jz_hmap *narrowed) {
    if (node->tag != GROOVE_GB_PROJECT) return narrow_chain(node, demand, narrowed);
    groove_GraphArc input = node->u.project.input;
    const groove_ProjectFieldVec *fields = &node->u.project.fields;
    if (input->tag != GROOVE_GB_JOIN && input->tag != GROOVE_GB_SEMI_JOIN) return NULL;
    groove_ProjectFieldVec kept = {0};
    for (u32 i = 0; i < fields->len; i++) {
        bool any = false;
        jz_str *name; void *unused;
        JZ_BTREE_FOR(demand, name, unused) if (provides(&fields->ptr[i], name)) { any = true; break; }
        if (any) {
            groove_ProjectField copy;
            groove__ivm__graph__ProjectField__Clone__clone(&copy, &fields->ptr[i]);
            JZ_VEC_PUSH(kept, copy);
        }
    }
    /* The consumer resolved every demanded name against this projection. If
     * one no longer resolves, keep the projection whole rather than guess. */
    bool all = true;
    {
        jz_str *name; void *unused;
        JZ_BTREE_FOR(demand, name, unused) {
            bool found = false;
            for (u32 i = 0; i < kept.len && !found; i++) found = provides(&kept.ptr[i], name);
            if (!found) { all = false; break; }
        }
    }
    if (!all) {
        for (u32 i = 0; i < kept.len; i++) groove__ivm__graph__ProjectField__drop(&kept.ptr[i]);
        jz_vec_drop(&kept, sizeof(groove_ProjectField), NULL);
        return NULL;
    }
    groove_GraphArc narrowed_input = NULL;
    jz_btree kept_demand;
    if (demanded_names(kept.ptr, kept.len, &kept_demand)) {
        narrowed_input = narrow_chain(input, &kept_demand, narrowed);
        jz_btree_drop(&kept_demand);
    }
    if (kept.len == fields->len && !narrowed_input) {
        for (u32 i = 0; i < kept.len; i++) groove__ivm__graph__ProjectField__drop(&kept.ptr[i]);
        jz_vec_drop(&kept, sizeof(groove_ProjectField), NULL);
        return NULL;
    }
    groove_GraphBuilder project; memset(&project, 0, sizeof project);
    project.tag = GROOVE_GB_PROJECT;
    project.u.project.input = narrowed_input ? narrowed_input : groove__ivm__graph__arc_clone(input);
    project.u.project.fields = kept;
    return groove__ivm__graph__arc_new(project);
}

/* Narrows one input of a chain node (memoised per (node, demand)). */
static groove_GraphArc narrow_join_input(groove_GraphArc node, const jz_btree *demand, jz_hmap *narrowed) {
    NarrowKey key;
    key.node = node;
    strset_clone(&key.demand, demand);
    groove_GraphArc *cached = jz_hmap_get(narrowed, &key);
    if (cached) {
        jz_btree_drop(&key.demand);
        return *cached ? groove__ivm__graph__arc_clone(*cached) : NULL;
    }
    groove_GraphArc rebuilt = narrow_input_uncached(node, demand, narrowed);
    groove_GraphArc stored = rebuilt ? groove__ivm__graph__arc_clone(rebuilt) : NULL;
    jz_hmap_insert(narrowed, &key, &stored, NULL);
    return rebuilt;
}

/* Rebuilds a chain node so that it still provides every demanded name.
 * Returns NULL (None) when nothing below it changed. */
static groove_GraphArc narrow_chain(const groove_GraphBuilder *node, const jz_btree *demand, jz_hmap *narrowed) {
    switch (node->tag) {
    case GROOVE_GB_JOIN: {
        jz_btree left_demand, right_demand;
        if (!join_side_demand(demand, LEFT, &node->u.join.left_on, &left_demand)) return NULL;
        if (!join_side_demand(demand, RIGHT, &node->u.join.right_on, &right_demand)) { jz_btree_drop(&left_demand); return NULL; }
        groove_GraphArc narrowed_left = narrow_join_input(node->u.join.left, &left_demand, narrowed);
        groove_GraphArc narrowed_right = narrow_join_input(node->u.join.right, &right_demand, narrowed);
        jz_btree_drop(&left_demand);
        jz_btree_drop(&right_demand);
        if (!narrowed_left && !narrowed_right) return NULL;
        groove_GraphBuilder join; memset(&join, 0, sizeof join);
        join.tag = GROOVE_GB_JOIN;
        join.u.join.left = narrowed_left ? narrowed_left : groove__ivm__graph__arc_clone(node->u.join.left);
        join.u.join.right = narrowed_right ? narrowed_right : groove__ivm__graph__arc_clone(node->u.join.right);
        groove__ivm__graph__fieldrefvec_clone(&join.u.join.left_on, &node->u.join.left_on);
        groove__ivm__graph__fieldrefvec_clone(&join.u.join.right_on, &node->u.join.right_on);
        join.u.join.comparison = node->u.join.comparison;
        return groove__ivm__graph__arc_new(join);
    }
    /* A semi-join outputs its left record unchanged and reads only the left
     * keys; its right input is an existence gate. */
    case GROOVE_GB_SEMI_JOIN: {
        jz_btree left_demand;
        strset_clone(&left_demand, demand);
        for (u32 i = 0; i < node->u.join.left_on.len; i++) {
            jz_str key;
            if (!field_ref_name(&node->u.join.left_on.ptr[i], &key)) { jz_btree_drop(&left_demand); return NULL; }
            strset_insert(&left_demand, key);
        }
        groove_GraphArc narrowed_left = narrow_join_input(node->u.join.left, &left_demand, narrowed);
        jz_btree_drop(&left_demand);
        if (!narrowed_left) return NULL;
        groove_GraphBuilder semi; memset(&semi, 0, sizeof semi);
        semi.tag = GROOVE_GB_SEMI_JOIN;
        semi.u.join.left = narrowed_left;
        semi.u.join.right = groove__ivm__graph__arc_clone(node->u.join.right);
        groove__ivm__graph__fieldrefvec_clone(&semi.u.join.left_on, &node->u.join.left_on);
        groove__ivm__graph__fieldrefvec_clone(&semi.u.join.right_on, &node->u.join.right_on);
        semi.u.join.comparison = node->u.join.comparison;
        return groove__ivm__graph__arc_new(semi);
    }
    case GROOVE_GB_UNWRAP_NULLABLE: {
        jz_btree input_demand;
        strset_clone(&input_demand, demand);
        jz_str name;
        if (!field_ref_name(&node->u.unwrap_nullable.field, &name)) { jz_btree_drop(&input_demand); return NULL; }
        strset_insert(&input_demand, name);
        groove_GraphArc input = narrow_join_input(node->u.unwrap_nullable.input, &input_demand, narrowed);
        jz_btree_drop(&input_demand);
        if (!input) return NULL;
        groove_GraphBuilder unwrap; memset(&unwrap, 0, sizeof unwrap);
        unwrap.tag = GROOVE_GB_UNWRAP_NULLABLE;
        unwrap.u.unwrap_nullable.input = input;
        groove__ivm__graph__FieldRef__Clone__clone(&unwrap.u.unwrap_nullable.field, &node->u.unwrap_nullable.field);
        return groove__ivm__graph__arc_new(unwrap);
    }
    case GROOVE_GB_FILTER: {
        jz_btree input_demand;
        strset_clone(&input_demand, demand);
        groove__ivm__runtime__subscriptions__PredicateExpr__referenced_fields(&node->u.filter.predicate, &input_demand);
        groove_GraphArc input = narrow_join_input(node->u.filter.input, &input_demand, narrowed);
        jz_btree_drop(&input_demand);
        if (!input) return NULL;
        groove_GraphBuilder filter; memset(&filter, 0, sizeof filter);
        filter.tag = GROOVE_GB_FILTER;
        filter.u.filter.input = input;
        groove__ivm__op_types__PredicateExpr__Clone__clone(&filter.u.filter.predicate, &node->u.filter.predicate);
        filter.u.filter.comparison = node->u.filter.comparison;
        return groove__ivm__graph__arc_new(filter);
    }
    default:
        return NULL;
    }
}

groove_GraphBuilder groove__ivm__graph__join_chains__GraphBuilder__narrow_projected_join_chain(groove_GraphBuilder self) {
    if (self.tag != GROOVE_GB_PROJECT) return self;
    groove_GraphArc input = self.u.project.input;
    jz_hmap narrowed_nodes;
    jz_hmap_init(&narrowed_nodes, &NARROWED_DESC);
    groove_GraphArc narrowed = NULL;
    jz_btree demand;
    if (demanded_names(self.u.project.fields.ptr, self.u.project.fields.len, &demand)) {
        narrowed = narrow_chain(input, &demand, &narrowed_nodes);
        jz_btree_drop(&demand);
    }
    jz_hmap_drop(&narrowed_nodes);
    if (narrowed) {
        groove__ivm__graph__arc_release(input);
        self.u.project.input = narrowed;
    }
    return self;
}
