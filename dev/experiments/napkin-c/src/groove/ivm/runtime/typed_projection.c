/* groove::ivm::runtime::typed_projection: reusable, source-independent
 * projection compilation. */
#include "mod.h"

#define PLAN_SLOTS 512u
#define MAX_KEY_BYTES (8u * 1024u)

static void plan_drop(void *p) {
    groove_ProjectionPlan *plan = p;
    for (u32 i = 0; i < plan->fields.len; i++) groove_ProjectField_drop(&plan->fields.ptr[i]);
    jz_vec_drop(&plan->fields, sizeof(groove_ProjectField), NULL);
    if (plan->expressions_set) {
        for (u32 i = 0; i < plan->expressions.len; i++) groove_ProjectionExpr_drop(&plan->expressions.ptr[i]);
        jz_vec_drop(&plan->expressions, sizeof(groove_ProjectionExpr), NULL);
    }
}

jz_err groove__ivm__runtime__typed_projection__ProjectionPlan__expressions(groove_ProjectionPlan *plan, const groove_ProjectionExprVec **out) {
    if (plan->expressions_set) { *out = &plan->expressions; return JZ_OK; }
    /* Keep output inference and expression validation at their existing
     * boundaries: inferring an output alone must not validate execution. */
    groove_ProjectionExprVec expressions = {0};
    for (u32 i = 0; i < plan->fields.len; i++) {
        const groove_ProjectField *field = &plan->fields.ptr[i];
        groove_ProjectionExpr e;
        memset(&e, 0, sizeof(e));
        jz_err err = groove__ivm__runtime__record_projection__project_field_expr(plan->input, field, &e.expression);
        if (JZ_IS_ERR(err)) {
            for (u32 j = 0; j < expressions.len; j++) groove_ProjectionExpr_drop(&expressions.ptr[j]);
            jz_vec_drop(&expressions, sizeof(groove_ProjectionExpr), NULL);
            return err;
        }
        e.has_output_name = true;
        e.output_name = jz_str_clone(&field->output_name);
        e.has_identity = field->has_identity;
        if (field->has_identity) groove_FieldIdentity_clone(&e.output_identity, &field->output_identity);
        JZ_VEC_PUSH(expressions, e);
    }
    plan->expressions = expressions;
    plan->expressions_set = true;
    if (!plan->expressions_set) jz_panic("projection expressions initialized");
    *out = &plan->expressions;
    return JZ_OK;
}

void groove__ivm__runtime__typed_projection__ProjectionPlans__default(groove_ProjectionPlans *p) {
    memset(p, 0, sizeof(*p));
    jz_vec_reserve(&p->slots, PLAN_SLOTS, sizeof(groove_ProjectionPlan *));
    memset(p->slots.ptr, 0, PLAN_SLOTS * sizeof(groove_ProjectionPlan *));
    p->slots.len = PLAN_SLOTS;
}

void groove_ProjectionPlans_drop(groove_ProjectionPlans *p) {
    for (u32 i = 0; i < p->slots.len; i++) if (p->slots.ptr[i]) jz_rc_release(p->slots.ptr[i]);
    jz_vec_drop(&p->slots, sizeof(groove_ProjectionPlan *), NULL);
}

void groove_ProjectionPlans_clone(groove_ProjectionPlans *dst, const groove_ProjectionPlans *src) {
    memset(dst, 0, sizeof(*dst));
    jz_vec_clone(&dst->slots, &src->slots, sizeof(groove_ProjectionPlan *), NULL);
    for (u32 i = 0; i < dst->slots.len; i++) if (dst->slots.ptr[i]) jz_rc_clone(dst->slots.ptr[i]);
}

/* KeyHasher: DefaultHasher plus a running count of hashed bytes. */
typedef struct KeyHasher { u64 inner; u32 bytes; } KeyHasher;
static void key_hash_str(KeyHasher *h, const jz_str *s) {
    h->bytes += s->len + 8;
    h->inner = jz_hash_combine(h->inner, jz_str_hash(s));
}
static void key_hash_literal(KeyHasher *h, const groove_LiteralValue *l) {
    switch (l->tag) {
    case groove_Lit_String: key_hash_str(h, &l->v.s); break;
    case groove_Lit_Bytes: h->bytes += l->v.bytes.len + 8; h->inner = jz_hash_combine(h->inner, jz_hash_bytes(l->v.bytes.ptr, l->v.bytes.len)); break;
    case groove_Lit_Tuple: case groove_Lit_Array:
        h->bytes += 8;
        for (u32 i = 0; i < l->v.vals.len; i++) key_hash_literal(h, &l->v.vals.ptr[i]);
        break;
    case groove_Lit_Nullable: h->bytes += 8; if (l->v.nullable) key_hash_literal(h, l->v.nullable); break;
    case groove_Lit_NativeRecord: h->bytes += l->v.native.descriptor.len + l->v.native.raw.len + 16; break;
    default: h->bytes += 9; h->inner = jz_hash_combine(h->inner, l->v.u); break;
    }
}

jz_err groove__ivm__runtime__typed_projection__ProjectionPlans__get(groove_ProjectionPlans *p, groove_RecordDescriptor input, const groove_ProjectField *fields, u32 n, groove_ProjectionPlan **out) {
    KeyHasher hash = { 0, 0 };
    hash.inner = groove__records__RecordDescriptor__hash(input, hash.inner);
    hash.bytes += 8;
    hash.inner = jz_hash_combine(hash.inner, n);
    for (u32 i = 0; i < n; i++) {
        hash.inner = groove_ProjectField_hash(&fields[i], hash.inner);
        key_hash_str(&hash, &fields[i].output_name);
        if (fields[i].expression.tag == groove_ProjectExpr_Literal) key_hash_literal(&hash, &fields[i].expression.literal);
        if (fields[i].expression.tag == groove_ProjectExpr_Field && fields[i].expression.field.tag != groove_FieldRef_Resolved)
            key_hash_str(&hash, &fields[i].expression.field.name);
    }
    u32 slot = (u32)(hash.inner % PLAN_SLOTS);
    groove_ProjectionPlan *plan = p->slots.ptr[slot];
    if (plan && plan->input == input && plan->fields.len == n) {
        bool same = true;
        for (u32 i = 0; i < n && same; i++) same = groove_ProjectField_eq(&plan->fields.ptr[i], &fields[i]);
        if (same) { *out = jz_rc_clone(plan); return JZ_OK; }
    }
    groove_ProjectFieldVec owned = {0};
    jz_vec_reserve(&owned, n, sizeof(groove_ProjectField));
    for (u32 i = 0; i < n; i++) groove_ProjectField_clone(&owned.ptr[i], &fields[i]);
    owned.len = n;
    groove_RecordDescriptor output;
    jz_err e = groove__ivm__runtime__record_projection__project_descriptor(input, owned.ptr, owned.len, &output);
    if (JZ_IS_ERR(e)) {
        for (u32 i = 0; i < n; i++) groove_ProjectField_drop(&owned.ptr[i]);
        jz_vec_drop(&owned, sizeof(groove_ProjectField), NULL);
        return e;
    }
    groove_ProjectionPlan *np = jz_rc_new(sizeof(groove_ProjectionPlan), plan_drop);
    np->input = input;
    np->fields = owned;
    np->output = output;
    np->expressions_set = false;
    memset(&np->expressions, 0, sizeof(np->expressions));
    if (hash.bytes <= MAX_KEY_BYTES) {
        if (p->slots.ptr[slot]) jz_rc_release(p->slots.ptr[slot]);
        p->slots.ptr[slot] = jz_rc_clone(np);
    }
    *out = np;
    return JZ_OK;
}

/* impl IvmRuntime { fn projection_plan } */
jz_err groove__ivm__runtime__typed_projection__IvmRuntime__projection_plan(groove_IvmRuntime *self, groove_RecordDescriptor input, const groove_ProjectField *fields, u32 n, groove_ProjectionPlan **out) {
    return groove__ivm__runtime__typed_projection__ProjectionPlans__get(&self->projection_plans, input, fields, n, out);
}
