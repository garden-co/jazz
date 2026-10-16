/* arguments.c: port of crates/groove/src/ivm/template/arguments.rs.
 * Typed bind-time arguments. Values never enter the reusable program. */
#include "jz_rt.h"
#include "../../_ext_groove-ivm-db.h"
#include "../template.h"

/* ---------------- TemplateScalarArgument (derives) ---------------- */
jz_err groove__ivm__template__arguments__TemplateScalarArgument__new(groove_Value value, groove_ValueType value_type, groove_TemplateScalarArgument *out) {
    jz_bytes encoded;
    jz_err e = groove__records__encode_single_field_value(&value, &value_type, &encoded);
    if (e.code) {
        groove__records__values__Value__drop(&value);
        groove__records__values__ValueType__drop(&value_type);
        return e;
    }
    jz_bytes_drop(&encoded);
    groove__ivm__op_types__LiteralValue__From_Value__from(&out->value, &value);
    out->value_type = value_type;
    return JZ_OK;
}
void groove__ivm__template__arguments__TemplateScalarArgument__Clone__clone(groove_TemplateScalarArgument *dst, const groove_TemplateScalarArgument *src) {
    groove__ivm__op_types__LiteralValue__Clone__clone(&dst->value, &src->value);
    groove__records__values__ValueType__Clone__clone(&dst->value_type, &src->value_type);
}
void groove__ivm__template__arguments__TemplateScalarArgument__drop(groove_TemplateScalarArgument *v) {
    groove__ivm__op_types__LiteralValue__drop(&v->value);
    groove__records__values__ValueType__drop(&v->value_type);
}
bool groove__ivm__template__arguments__TemplateScalarArgument__PartialEq__eq(const groove_TemplateScalarArgument *a, const groove_TemplateScalarArgument *b) {
    return groove__ivm__op_types__LiteralValue__PartialEq__eq(&a->value, &b->value) &&
           groove__records__values__ValueType__PartialEq__eq(&a->value_type, &b->value_type);
}
void groove__ivm__template__arguments__TemplateScalarArgument__Hash__hash(const groove_TemplateScalarArgument *v, groove_Hasher *h) {
    groove__ivm__op_types__LiteralValue__Hash__hash(&v->value, h);
    groove__records__values__ValueType__Hash__hash(&v->value_type, h);
}
void groove__ivm__template__arguments__TemplateScalarArgument__Debug__fmt(jz_str *out, const groove_TemplateScalarArgument *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "TemplateScalarArgument");
    groove__fmt__DebugStruct__field(&d, "value"); groove__ivm__op_types__LiteralValue__Debug__fmt(out, &v->value);
    groove__fmt__DebugStruct__field(&d, "value_type"); groove__records__values__ValueType__Debug__fmt(out, &v->value_type);
    groove__fmt__DebugStruct__finish(&d);
}
static void scalar_slice_drop(void *p) {
    groove_ScalarArgSlice *s = p;
    for (u32 i = 0; i < s->len; i++) groove__ivm__template__arguments__TemplateScalarArgument__drop(&s->items[i]);
}
/* Arc<[TemplateScalarArgument]>::from(vec) */
groove_ScalarArgSlice *groove__ivm__template__arguments__scalar_slice_new(const groove_TemplateScalarArgument *items, u32 n) {
    groove_ScalarArgSlice *s = jz_rc_new(sizeof(groove_ScalarArgSlice) + n * sizeof(groove_TemplateScalarArgument), scalar_slice_drop);
    s->len = n;
    if (n) memcpy(s->items, items, n * sizeof(groove_TemplateScalarArgument));
    return s;
}

/* ---------------- binding ---------------- */
jz_err groove__ivm__template__arguments__bind_project_expression(groove_ProjectExpr *expression, const groove_TemplateScalarArgument *arguments, u32 n) {
    if (expression->tag == GROOVE_PEXPR_TEMPLATE_ARGUMENT) {
        u32 slot = expression->u.template_argument.slot;
        const groove_ValueType *value_type = &expression->u.template_argument.value_type;
        const groove_TemplateScalarArgument *argument = slot < n ? &arguments[slot] : NULL;
        if (argument && !groove__records__values__ValueType__PartialEq__eq(&argument->value_type, value_type)) argument = NULL;
        if (!argument) return groove__ivm__template__TemplateBindingError__scalar_argument(slot);
        groove_ProjectExpr typed; memset(&typed, 0, sizeof typed);
        typed.tag = GROOVE_PEXPR_TYPED_LITERAL;
        groove__ivm__op_types__LiteralValue__Clone__clone(&typed.u.typed_literal.value, &argument->value);
        groove__records__values__ValueType__Clone__clone(&typed.u.typed_literal.value_type, value_type);
        groove__ivm__graph__ProjectExpr__drop(expression);
        *expression = typed;
    }
    return JZ_OK;
}

static jz_err bind_predicate(groove_PredicateExpr *predicate, const groove_PredicateExpr *arguments, u32 n) {
    if (predicate->tag == GROOVE_PRED_TEMPLATE_ARGUMENT) {
        u32 slot = predicate->u.template_argument.slot;
        const groove_PredicateExpr *argument = slot < n ? &arguments[slot] : NULL;
        if (argument && !groove__ivm__op_types__PredicateExpr__accepts_template_argument(predicate, argument)) argument = NULL;
        if (!argument) return groove__ivm__template__TemplateBindingError__predicate_argument(slot);
        groove_PredicateExpr bound;
        groove__ivm__op_types__PredicateExpr__Clone__clone(&bound, argument);
        groove__ivm__op_types__PredicateExpr__drop(predicate);
        *predicate = bound;
        return JZ_OK;
    }
    switch (predicate->tag) {
    case GROOVE_PRED_AND: case GROOVE_PRED_OR:
        for (u32 i = 0; i < predicate->u.children.len; i++) JZ_TRY(bind_predicate(&predicate->u.children.ptr[i], arguments, n));
        break;
    case GROOVE_PRED_ENUM_MATCH:
        JZ_TRY(bind_predicate(predicate->u.enum_match.payload, arguments, n));
        break;
    default:
        break;
    }
    return JZ_OK;
}

jz_err groove__ivm__template__arguments__TemplateArguments__bind_node(const groove_TemplateArguments *self, groove_GraphBuilder *node) {
    switch (node->tag) {
    case GROOVE_GB_FILTER:
        JZ_TRY(bind_predicate(&node->u.filter.predicate, self->predicates, self->n_predicates));
        break;
    case GROOVE_GB_PROJECT: {
        const groove_ScalarArgSlice *scalars = *self->scalars;
        for (u32 i = 0; i < node->u.project.fields.len; i++)
            JZ_TRY(groove__ivm__template__arguments__bind_project_expression(&node->u.project.fields.ptr[i].expression, scalars->items, scalars->len));
        break;
    }
    case GROOVE_GB_TYPED_TEMPLATE:
        for (u32 i = 0; i < node->u.typed_template.predicates.len; i++)
            JZ_TRY(bind_predicate(&node->u.typed_template.predicates.ptr[i], self->predicates, self->n_predicates));
        jz_rc_release(node->u.typed_template.scalars);
        node->u.typed_template.scalars = jz_rc_clone(*self->scalars);
        break;
    default:
        break;
    }
    return JZ_OK;
}

/* HashMap<usize, Arc<GraphBuilder>> keyed by node address */
static u64 ptr_hash_v(const void *k) { return jz_hash_u64((u64)(uintptr_t)*(const void *const *)k); }
static bool ptr_eq_v(const void *a, const void *b) { return *(const void *const *)a == *(const void *const *)b; }
static void arc_release_v(void *p) { groove__ivm__graph__arc_release(*(groove_GraphArc *)p); }
static const jz_hmap_desc REWRITTEN_DESC = { sizeof(void *), sizeof(groove_GraphArc), ptr_hash_v, ptr_eq_v, NULL, arc_release_v };
static groove_GraphArc lookup(jz_hmap *m, const groove_GraphBuilder *key) {
    groove_GraphArc *found = jz_hmap_get(m, &key);
    if (!found) jz_panic("key not found");
    return groove__ivm__graph__arc_clone(*found);
}
static groove_GraphArc map_child(groove_GraphArc const *child, void *ctx) { return lookup(ctx, *child); }
typedef struct { const groove_GraphBuilder *node; bool expanded; } frame;
typedef JZ_VEC(frame) frame_stack;
static void push_child(groove_GraphArc const *child, void *ctx) {
    frame f = { *child, false };
    JZ_VEC_PUSH(*(frame_stack *)ctx, f);
}

jz_err groove__ivm__template__arguments__bind_template_arguments(const groove_GraphBuilder *graphs, u32 n, const groove_PredicateExpr *predicates, u32 np,
                                                                 groove_ScalarArgSlice *scalars, groove_GraphBuilderVec *out) {
    jz_hmap rewritten;
    jz_hmap_init(&rewritten, &REWRITTEN_DESC);
    frame_stack pending = {0};
    groove_TemplateArguments arguments = { predicates, np, &scalars };
    jz_err err = JZ_OK;
    for (u32 r = 0; r < n && !err.code; r++) {
        pending.len = 0;
        frame root = { &graphs[r], false };
        JZ_VEC_PUSH(pending, root);
        while (pending.len) {
            frame top = pending.ptr[--pending.len];
            const groove_GraphBuilder *node = top.node;
            if (jz_hmap_get(&rewritten, &node)) continue;
            if (node->tag == GROOVE_GB_TEMPLATE_INPUT && node->u.template_input.input) {
                groove_GraphBuilder copy;
                groove__ivm__graph__GraphBuilder__Clone__clone(&copy, node);
                groove_GraphArc arc = groove__ivm__graph__arc_new(copy);
                jz_hmap_insert(&rewritten, &node, &arc, NULL);
                continue;
            }
            if (!top.expanded) {
                frame again = { node, true };
                JZ_VEC_PUSH(pending, again);
                groove__ivm__graph__GraphBuilder__visit_inputs(node, push_child, &pending);
                continue;
            }
            groove_GraphBuilder bound;
            groove__ivm__graph__GraphBuilder__map_inputs(node, map_child, &rewritten, &bound);
            jz_err e = groove__ivm__template__arguments__TemplateArguments__bind_node(&arguments, &bound);
            if (e.code) { groove__ivm__graph__GraphBuilder__drop(&bound); err = e; break; }
            groove_GraphArc arc = groove__ivm__graph__arc_new(bound);
            jz_hmap_insert(&rewritten, &node, &arc, NULL);
        }
    }
    jz_vec_drop(&pending, sizeof(frame), NULL);
    if (!err.code) {
        memset(out, 0, sizeof *out);
        for (u32 r = 0; r < n; r++) {
            groove_GraphArc arc = lookup(&rewritten, &graphs[r]);
            groove_GraphBuilder copy;
            groove__ivm__graph__GraphBuilder__Clone__clone(&copy, arc);
            groove__ivm__graph__arc_release(arc);
            JZ_VEC_PUSH(*out, copy);
        }
    }
    jz_hmap_drop(&rewritten);
    jz_rc_release(scalars);
    return err;
}
