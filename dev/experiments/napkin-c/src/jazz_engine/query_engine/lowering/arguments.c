/* lowering/arguments.rs: pure bind-time recipes, separate from logical/wire identity
 * and authority. */
#include "../lowering.h"

void jazz_engine__opt_vt_drop_v(void *p) {
    jazz_engine_OptValueType *o = p;
    if (o->some) groove__records__values__ValueType__drop(&o->ty);
}
void jazz_engine__opt_vt_clone_v(void *d, const void *s) {
    jazz_engine_OptValueType *dst = d;
    const jazz_engine_OptValueType *src = s;
    dst->some = src->some;
    if (src->some) groove__records__values__ValueType__Clone__clone(&dst->ty, &src->ty);
    else memset(&dst->ty, 0, sizeof dst->ty);
}

/* ---------------------------------------------------------------- LoweringContext */
void JE_ARG(LoweringContext__concrete)(jazz_engine_LoweringContext *ctx, const jazz_engine_QueryProgramRequest *request) {
    memset(ctx, 0, sizeof *ctx);
    ctx->request = request;
}
void JE_ARG(LoweringContext__compiling)(jazz_engine_LoweringContext *ctx, const jazz_engine_QueryProgramRequest *request,
                                       jazz_engine_ProgramArgumentRecipes *arguments) {
    memset(ctx, 0, sizeof *ctx);
    ctx->request = request;
    ctx->arguments = arguments;
}
void JE_ARG(LoweringContext__drop)(jazz_engine_LoweringContext *ctx) {
    if (ctx->parameter_domain_state == 1) JE_LOW(ParameterDomain__drop)(&ctx->parameter_domain);
    else if (ctx->parameter_domain_state == 2) JE_RES(UnsupportedReason__drop)(&ctx->parameter_domain_err);
    ctx->parameter_domain_state = 0;
}

/* parameter_domain_for_request for this context's request, derived once per lowering. */
jazz_engine_UrErr JE_ARG(LoweringContext__parameter_domain)(jazz_engine_LoweringContext *ctx, const jazz_engine_ParameterDomain **out) {
    if (ctx->parameter_domain_state == 0) {
        jazz_engine_UrErr err = JE_LOW(parameter_domain_for_request)(ctx->request, &ctx->parameter_domain);
        if (err) {
            ctx->parameter_domain_err = jazz_engine__ur_take(err);
            ctx->parameter_domain_state = 2;
        } else {
            ctx->parameter_domain_state = 1;
        }
    }
    if (ctx->parameter_domain_state == 2) {
        jazz_engine_UnsupportedReason clone;
        JE_RES(UnsupportedReason__Clone__clone)(&clone, &ctx->parameter_domain_err);
        return jazz_engine__ur_box(clone);
    }
    *out = &ctx->parameter_domain;
    return NULL;
}

static jz_err stop_on_argument(const jazz_engine_NormalizedValueRef *value, void *ctx) {
    (void)ctx;
    if (value->tag == JAZZ_ENGINE_NVR_PARAM || value->tag == JAZZ_ENGINE_NVR_TEMPLATE_LITERAL) return jz_err_new(1, NULL);
    return JZ_OK;
}
static bool JE_ARG(predicate_contains_argument)(const jazz_engine_PredicateExpr *predicate) {
    jz_err e = JE_IN(PredicateExpr__try_for_each_value)(predicate, stop_on_argument, NULL);
    bool stopped = JZ_IS_ERR(e);
    jz_err_drop(&e);
    return stopped;
}

bool JE_ARG(LoweringContext__predicate_argument)(jazz_engine_LoweringContext *ctx, const jazz_engine_PredicateExpr *predicate,
                                                const jazz_engine_SourceId *source_id, const jazz_engine_ResolvedSource *source,
                                                groove_PredicateExpr *out) {
    if (!JE_ARG(predicate_contains_argument)(predicate)) return false;
    jazz_engine_ProgramArgumentRecipes *arguments = ctx->arguments;
    if (!arguments) return false;
    u32 slot = arguments->predicates.len;
    jazz_engine_PredicateArgument recipe;
    pred_clone(&recipe.predicate, predicate);
    sid_clone(&recipe.source_id, source_id);
    JE_RES(ResolvedSource__Clone__clone)(&recipe.source, source);
    JZ_VEC_PUSH(arguments->predicates, recipe);
    memset(out, 0, sizeof *out);
    out->tag = GROOVE_PRED_TEMPLATE_ARGUMENT;
    out->u.template_argument.slot = slot;
    JE_GL(source_fields)(source, &out->u.template_argument.fields);
    return true;
}

jazz_engine_UrErr JE_ARG(LoweringContext__literal)(const jazz_engine_LoweringContext *ctx, u32 slot, groove_Value *out) {
    if (slot >= ctx->nliterals) return jazz_engine__ur_rt(jz_format("compiler literal %u is not bound", slot));
    groove__records__values__Value__Clone__clone(out, &ctx->literals[slot]);
    return NULL;
}

bool JE_ARG(LoweringContext__literal_type)(const jazz_engine_LoweringContext *ctx, u32 slot, groove_ValueType *out) {
    if (!ctx->arguments) return false;
    const jazz_engine_OptValueTypeVec *types = &ctx->arguments->literal_types;
    if (slot >= types->len || !types->ptr[slot].some) return false;
    groove__records__values__ValueType__Clone__clone(out, &types->ptr[slot].ty);
    return true;
}

static bool scalar_argument_eq(const jazz_engine_ScalarArgument *a, const jazz_engine_ScalarArgument *b) {
    return nvr_eq(&a->value, &b->value) && groove__records__values__ValueType__PartialEq__eq(&a->ty, &b->ty) && a->coerce == b->coerce;
}

bool JE_ARG(LoweringContext__scalar_argument)(jazz_engine_LoweringContext *ctx, jazz_engine_NormalizedValueRef value, groove_ValueType ty,
                                             bool coerce, groove_ProjectExpr *out) {
    jazz_engine_ProgramArgumentRecipes *arguments = ctx->arguments;
    if (!arguments) {
        nvr_drop(&value);
        groove__records__values__ValueType__drop(&ty);
        return false;
    }
    /* Multiple witnesses often project the identical route: one recipe per argument. */
    jazz_engine_ScalarArgument recipe = {value, groove_vt_clone(&ty), coerce};
    u32 slot = arguments->scalars.len;
    for (u32 i = 0; i < arguments->scalars.len; i++)
        if (scalar_argument_eq(&arguments->scalars.ptr[i], &recipe)) { slot = i; break; }
    if (slot == arguments->scalars.len) {
        JZ_VEC_PUSH(arguments->scalars, recipe);
    } else {
        nvr_drop(&recipe.value);
        groove__records__values__ValueType__drop(&recipe.ty);
    }
    memset(out, 0, sizeof *out);
    out->tag = GROOVE_PEXPR_TEMPLATE_ARGUMENT;
    out->u.template_argument.slot = slot;
    out->u.template_argument.value_type = ty;
    return true;
}

/* ---------------------------------------------------------------- ProgramArgumentRecipes */
static void predicate_argument_clone_v(void *d, const void *s) {
    jazz_engine_PredicateArgument *dst = d;
    const jazz_engine_PredicateArgument *src = s;
    pred_clone(&dst->predicate, &src->predicate);
    sid_clone(&dst->source_id, &src->source_id);
    JE_RES(ResolvedSource__Clone__clone)(&dst->source, &src->source);
}
static void predicate_argument_drop_v(void *p) {
    jazz_engine_PredicateArgument *a = p;
    pred_drop(&a->predicate);
    sid_drop(&a->source_id);
    JE_RES(ResolvedSource__drop)(&a->source);
}
static void scalar_argument_clone_v(void *d, const void *s) {
    jazz_engine_ScalarArgument *dst = d;
    const jazz_engine_ScalarArgument *src = s;
    nvr_clone(&dst->value, &src->value);
    groove__records__values__ValueType__Clone__clone(&dst->ty, &src->ty);
    dst->coerce = src->coerce;
}
static void scalar_argument_drop_v(void *p) {
    jazz_engine_ScalarArgument *a = p;
    nvr_drop(&a->value);
    groove__records__values__ValueType__drop(&a->ty);
}
void JE_ARG(ProgramArgumentRecipes__with_literal_types)(jazz_engine_ProgramArgumentRecipes *out, jazz_engine_OptValueTypeVec literal_types) {
    memset(out, 0, sizeof *out);
    out->literal_types = literal_types;
}
void JE_ARG(ProgramArgumentRecipes__Clone__clone)(jazz_engine_ProgramArgumentRecipes *dst, const jazz_engine_ProgramArgumentRecipes *src) {
    jz_vec_clone(&dst->predicates, &src->predicates, sizeof(jazz_engine_PredicateArgument), predicate_argument_clone_v);
    jz_vec_clone(&dst->scalars, &src->scalars, sizeof(jazz_engine_ScalarArgument), scalar_argument_clone_v);
    jz_vec_clone(&dst->literal_types, &src->literal_types, sizeof(jazz_engine_OptValueType), jazz_engine__opt_vt_clone_v);
}
void JE_ARG(ProgramArgumentRecipes__drop)(jazz_engine_ProgramArgumentRecipes *r) {
    jz_vec_drop(&r->predicates, sizeof(jazz_engine_PredicateArgument), predicate_argument_drop_v);
    jz_vec_drop(&r->scalars, sizeof(jazz_engine_ScalarArgument), scalar_argument_drop_v);
    jz_vec_drop(&r->literal_types, sizeof(jazz_engine_OptValueType), jazz_engine__opt_vt_drop_v);
}

static jazz_engine_UrErr decode_literal(const jz_bytes *bytes, groove_Value *out) {
    jz_pc_r r = {bytes->ptr, bytes->ptr + bytes->len, 0};
    jz_err e = groove__records__values__Value__Deserialize__postcard(&r, out);
    if (!JZ_IS_ERR(e)) return NULL;
    jz_str shown = jazz_engine__err_display(&e);
    jz_err_drop(&e);
    jazz_engine_UrErr err = jazz_engine__ur_op(jz_format("literal decoding failed: %.*s", JE_STR_ARG(shown)));
    jz_str_drop(&shown);
    return err;
}

static jazz_engine_UrErr bind_scalar(const jazz_engine_ScalarArgument *recipe, const jazz_engine_LoweringContext *context,
                                     const jazz_engine_QueryProgramRequest *request, groove_TemplateScalarArgumentVec *scalars) {
    groove_Value value;
    switch (recipe->value.tag) {
    case JAZZ_ENGINE_NVR_TEMPLATE_LITERAL:
        JE_UR_TRY(JE_ARG(LoweringContext__literal)(context, recipe->value.u.template_literal, &value));
        break;
    case JAZZ_ENGINE_NVR_PARAM: {
        const groove_Value *bound = jz_btree_get(&request->input.binding.values, &recipe->value.u.param);
        if (!bound)
            return jazz_engine__ur_op(jz_format("binding parameter '%.*s' is not bound", JE_STR_ARG(recipe->value.u.param)));
        groove__records__values__Value__Clone__clone(&value, bound);
        break;
    }
    case JAZZ_ENGINE_NVR_CLAIM:
        JE_UR_TRY(JE_GL(claim_value)(&recipe->value.u.claim, &request->policy, &value));
        break;
    case JAZZ_ENGINE_NVR_LITERAL:
        JE_UR_TRY(decode_literal(&recipe->value.u.literal, &value));
        break;
    default:
        return JE_UR_OP("template scalar must be a parameter, claim or literal");
    }
    if (recipe->coerce) value = jazz_engine__query_engine__binding_values__coerce_prepared_binding_value(value, &recipe->ty);
    jz_err e = groove__ivm__template__arguments__TemplateScalarArgument__new(value, groove_vt_clone(&recipe->ty), scalars);
    if (!JZ_IS_ERR(e)) return NULL;
    jz_str shown = {0};
    groove__ivm__runtime__IvmRuntimeError__Display__fmt(&shown, &e);
    jz_err_drop(&e);
    jazz_engine_UrErr err = jazz_engine__ur_op(jz_format("template scalar binding failed: %.*s", JE_STR_ARG(shown)));
    jz_str_drop(&shown);
    return err;
}

jazz_engine_UrErr JE_ARG(ProgramArgumentRecipes__bind)(const jazz_engine_ProgramArgumentRecipes *self, groove_GraphBuilder *const *graphs, u32 ngraphs,
                                                      const groove_TemplateGraphInput *inputs, u32 ninputs,
                                                      const jazz_engine_QueryProgramRequest *request, const groove_Value *literals, u32 nliterals,
                                                      groove_GraphBuilderVec *out) {
    jazz_engine_LoweringContext context;
    JE_ARG(LoweringContext__concrete)(&context, request);
    context.literals = literals;
    context.nliterals = nliterals;
    groove_PredicateVec predicates = {0};
    groove_TemplateScalarArgumentVec scalars = {0};
    jazz_engine_UrErr err = NULL;
    JZ_VEC_FOR(const jazz_engine_PredicateArgument, recipe, self->predicates) {
        groove_PredicateExpr lowered;
        err = JE_GL(lower_predicate)(&recipe->predicate, &recipe->source_id, &recipe->source, &context, &lowered);
        if (err) goto done;
        JZ_VEC_PUSH(predicates, lowered);
    }
    JZ_VEC_FOR(const jazz_engine_ScalarArgument, recipe, self->scalars) {
        err = bind_scalar(recipe, &context, request, &scalars);
        if (err) goto done;
    }
    {
        jz_err e = groove__ivm__template__bind_template_program(graphs, ngraphs, inputs, ninputs, predicates.ptr, predicates.len, scalars, out);
        memset(&scalars, 0, sizeof scalars); /* moved (Arc::from) */
        if (JZ_IS_ERR(e)) {
            jz_str shown = {0};
            groove__ivm__runtime__IvmRuntimeError__Display__fmt(&shown, &e);
            jz_err_drop(&e);
            err = jazz_engine__ur_rt(jz_format("template argument binding failed: %.*s", JE_STR_ARG(shown)));
            jz_str_drop(&shown);
        }
    }
done:
    jz_vec_drop(&predicates, sizeof(groove_PredicateExpr), groove__ivm__op_types__PredicateExpr__drop_v);
    groove__ivm__template__TemplateScalarArgumentVec__drop(&scalars);
    JE_ARG(LoweringContext__drop)(&context);
    return err;
}

/* ---------------------------------------------------------------- extract_template_literals
 * Lift ordinary literal operands into a compiler-local argument namespace. */
typedef struct { groove_ValueVec *literals; jazz_engine_UrErr err; } lift_ctx;

static jazz_engine_UrErr lift_value(jazz_engine_NormalizedValueRef *value, groove_ValueVec *literals) {
    if (value->tag != JAZZ_ENGINE_NVR_LITERAL) return NULL;
    groove_Value decoded;
    JE_UR_TRY(decode_literal(&value->u.literal, &decoded));
    nvr_drop(value);
    memset(value, 0, sizeof *value);
    value->tag = JAZZ_ENGINE_NVR_TEMPLATE_LITERAL;
    value->u.template_literal = literals->len;
    JZ_VEC_PUSH(*literals, decoded);
    return NULL;
}
static jz_err lift_visit(jazz_engine_NormalizedValueRef *value, void *ctx) {
    lift_ctx *c = ctx;
    c->err = lift_value(value, c->literals);
    return c->err ? jz_err_new(1, NULL) : JZ_OK;
}

jazz_engine_UrErr JE_ARG(extract_template_literals)(jazz_engine_QueryProgramRequest *request, groove_ValueVec *out) {
    groove_ValueVec literals = {0};
    jazz_engine_UrErr err = NULL;
    jz_str *id; jazz_engine_RowSetExpr *node;
    JZ_BTREE_FOR(&request->input.shape.nodes, id, node) {
        if (node->tag == JAZZ_ENGINE_EXPR_FILTER) {
            lift_ctx c = {&literals, NULL};
            jz_err e = JE_IN(PredicateExpr__try_for_each_value_mut)(&node->u.filter.predicate, lift_visit, &c);
            jz_err_drop(&e);
            if (c.err) { err = c.err; break; }
        } else if (node->tag == JAZZ_ENGINE_EXPR_PROJECT) {
            JZ_VEC_FOR(jazz_engine_RowProjection, column, node->u.project.columns) {
                err = lift_value(&column->value, &literals);
                if (err) break;
            }
            if (err) break;
        }
        /* Values in semantic planner markers, keys, and closure metadata stay exact. */
    }
    if (err) {
        jz_vec_drop(&literals, sizeof(groove_Value), groove__records__values__Value__drop_v);
        return err;
    }
    if (literals.len) {
        /* Only this private compiler copy loses its public facade identity. */
        memset(&request->input.shape.identity.shape_id, 0, sizeof request->input.shape.identity.shape_id);
        request->input.shape.identity.canonical.len = 0;
    }
    *out = literals;
    return NULL;
}
