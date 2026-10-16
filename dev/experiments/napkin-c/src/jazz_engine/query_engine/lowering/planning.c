/* lowering/planning.rs: query-shape analysis and executable-plan construction.
 *
 * Turns normalized row-set shapes into explicit linear, correlated, union and
 * recursive plans. It does not construct Groove operators or choose fields. */
#include "../lowering.h"

/* ================================================================ derives: clone / drop */
void JE_PLAN(LinearRoot__Clone__clone)(jazz_engine_LinearRoot *dst, const jazz_engine_LinearRoot *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    switch (src->tag) {
    case JAZZ_ENGINE_LROOT_SOURCE:
        sid_clone(&dst->source, &src->source);
        dst->visibility = src->visibility;
        break;
    case JAZZ_ENGINE_LROOT_VALUE:
        dst->shape = jz_str_clone(&src->shape);
        jz_vec_clone(&dst->columns, &src->columns, sizeof(jazz_engine_ValueSourceColumn), jazz_engine__vsc_clone_v);
        dst->mode = src->mode;
        break;
    case JAZZ_ENGINE_LROOT_FRONTIER:
        dst->frontier = jz_str_clone(&src->frontier);
        jz_vec_clone(&dst->columns, &src->columns, sizeof(jazz_engine_ValueSourceColumn), jazz_engine__vsc_clone_v);
        break;
    }
}
void JE_PLAN(LinearRoot__drop)(jazz_engine_LinearRoot *r) {
    switch (r->tag) {
    case JAZZ_ENGINE_LROOT_SOURCE: sid_drop(&r->source); break;
    case JAZZ_ENGINE_LROOT_VALUE:
        jz_str_drop(&r->shape);
        jz_vec_drop(&r->columns, sizeof(jazz_engine_ValueSourceColumn), jazz_engine__vsc_drop_v);
        break;
    case JAZZ_ENGINE_LROOT_FRONTIER:
        jz_str_drop(&r->frontier);
        jz_vec_drop(&r->columns, sizeof(jazz_engine_ValueSourceColumn), jazz_engine__vsc_drop_v);
        break;
    }
}
void JE_PLAN(LinearCurrentRoot__Clone__clone)(jazz_engine_LinearCurrentRoot *dst, const jazz_engine_LinearCurrentRoot *src) {
    JE_PLAN(LinearRoot__Clone__clone)(&dst->root, &src->root);
    jazz_engine__steps_clone(&dst->steps, &src->steps);
}
void JE_PLAN(LinearCurrentRoot__drop)(jazz_engine_LinearCurrentRoot *r) {
    JE_PLAN(LinearRoot__drop)(&r->root);
    jazz_engine__steps_drop(&r->steps);
}

void JE_PLAN(LinearStep__Clone__clone)(jazz_engine_LinearStep *dst, const jazz_engine_LinearStep *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    switch (src->tag) {
    case JAZZ_ENGINE_STEP_FILTER: pred_clone(&dst->u.filter, &src->u.filter); break;
    case JAZZ_ENGINE_STEP_JOIN:
        dst->u.join.right = JZ_NEW(jazz_engine_RelationInputPlan);
        JE_PLAN(RelationInputPlan__Clone__clone)(dst->u.join.right, src->u.join.right);
        dst->u.join.mode = src->u.join.mode;
        pred_clone(&dst->u.join.on, &src->u.join.on);
        break;
    case JAZZ_ENGINE_STEP_PROJECT:
        jz_vec_clone(&dst->u.project, &src->u.project, sizeof(jazz_engine_RowProjection), jazz_engine__projection_clone_v);
        break;
    case JAZZ_ENGINE_STEP_ORDER_BY:
        jz_vec_clone(&dst->u.order_by, &src->u.order_by, sizeof(jazz_engine_OrderKey), jazz_engine__order_key_clone_v);
        break;
    case JAZZ_ENGINE_STEP_SLICE:
        jazz_engine__nvrvec_clone(&dst->u.slice.partition_by, &src->u.slice.partition_by);
        dst->u.slice.has_limit = src->u.slice.has_limit;
        dst->u.slice.limit = src->u.slice.limit;
        dst->u.slice.offset = src->u.slice.offset;
        jazz_engine__nvrvec_clone(&dst->u.slice.tie_breaker, &src->u.slice.tie_breaker);
        dst->u.slice.has_rank_output = src->u.slice.has_rank_output;
        if (src->u.slice.has_rank_output) tof_clone(&dst->u.slice.rank_output, &src->u.slice.rank_output);
        break;
    case JAZZ_ENGINE_STEP_AGGREGATE:
        jazz_engine__nvrvec_clone(&dst->u.aggregate.group_by, &src->u.aggregate.group_by);
        jz_vec_clone(&dst->u.aggregate.outputs, &src->u.aggregate.outputs, sizeof(jazz_engine_AggregateExpr), jazz_engine__aggregate_clone_v);
        break;
    }
}
void JE_PLAN(LinearStep__drop)(jazz_engine_LinearStep *s) {
    switch (s->tag) {
    case JAZZ_ENGINE_STEP_FILTER: pred_drop(&s->u.filter); break;
    case JAZZ_ENGINE_STEP_JOIN:
        JE_PLAN(RelationInputPlan__drop)(s->u.join.right);
        jz_free(s->u.join.right);
        pred_drop(&s->u.join.on);
        break;
    case JAZZ_ENGINE_STEP_PROJECT: jz_vec_drop(&s->u.project, sizeof(jazz_engine_RowProjection), jazz_engine__projection_drop_v); break;
    case JAZZ_ENGINE_STEP_ORDER_BY: jz_vec_drop(&s->u.order_by, sizeof(jazz_engine_OrderKey), jazz_engine__order_key_drop_v); break;
    case JAZZ_ENGINE_STEP_SLICE:
        jazz_engine__nvrvec_drop(&s->u.slice.partition_by);
        jazz_engine__nvrvec_drop(&s->u.slice.tie_breaker);
        if (s->u.slice.has_rank_output) tof_drop(&s->u.slice.rank_output);
        break;
    case JAZZ_ENGINE_STEP_AGGREGATE:
        jazz_engine__nvrvec_drop(&s->u.aggregate.group_by);
        jz_vec_drop(&s->u.aggregate.outputs, sizeof(jazz_engine_AggregateExpr), jazz_engine__aggregate_drop_v);
        break;
    }
}
void jazz_engine__step_clone_v(void *d, const void *s) { JE_PLAN(LinearStep__Clone__clone)(d, s); }
void jazz_engine__step_drop_v(void *s) { JE_PLAN(LinearStep__drop)(s); }
void jazz_engine__steps_clone(jazz_engine_LinearStepVec *dst, const jazz_engine_LinearStepVec *src) {
    jz_vec_clone(dst, src, sizeof(jazz_engine_LinearStep), jazz_engine__step_clone_v);
}
void jazz_engine__steps_drop(jazz_engine_LinearStepVec *v) { jz_vec_drop(v, sizeof(jazz_engine_LinearStep), jazz_engine__step_drop_v); }

static void branch_clone_v(void *d, const void *s) {
    jazz_engine_UnionBranchPlan *dst = d;
    const jazz_engine_UnionBranchPlan *src = s;
    dst->label = jz_str_clone(&src->label);
    JE_PLAN(RelationInputPlan__Clone__clone)(&dst->plan, &src->plan);
}
static void branch_drop_v(void *p) {
    jazz_engine_UnionBranchPlan *b = p;
    jz_str_drop(&b->label);
    JE_PLAN(RelationInputPlan__drop)(&b->plan);
}
void JE_PLAN(UnionPlan__Clone__clone)(jazz_engine_UnionPlan *dst, const jazz_engine_UnionPlan *src) {
    jz_vec_clone(&dst->branches, &src->branches, sizeof(jazz_engine_UnionBranchPlan), branch_clone_v);
    jazz_engine__steps_clone(&dst->terminal_steps, &src->terminal_steps);
}
void JE_PLAN(UnionPlan__drop)(jazz_engine_UnionPlan *p) {
    jz_vec_drop(&p->branches, sizeof(jazz_engine_UnionBranchPlan), branch_drop_v);
    jazz_engine__steps_drop(&p->terminal_steps);
}
void JE_PLAN(RecursiveRelationPlan__Clone__clone)(jazz_engine_RecursiveRelationPlan *dst, const jazz_engine_RecursiveRelationPlan *src) {
    JE_PLAN(LinearCurrentRoot__Clone__clone)(&dst->seed, &src->seed);
    JE_PLAN(LinearCurrentRoot__Clone__clone)(&dst->step, &src->step);
    dst->frontier = jz_str_clone(&src->frontier);
    nvr_clone(&dst->frontier_key, &src->frontier_key);
    jazz_engine__nvrvec_clone(&dst->dedupe_keys, &src->dedupe_keys);
    dst->bound = src->bound;
}
void JE_PLAN(RecursiveRelationPlan__drop)(jazz_engine_RecursiveRelationPlan *p) {
    JE_PLAN(LinearCurrentRoot__drop)(&p->seed);
    JE_PLAN(LinearCurrentRoot__drop)(&p->step);
    jz_str_drop(&p->frontier);
    nvr_drop(&p->frontier_key);
    jazz_engine__nvrvec_drop(&p->dedupe_keys);
}
void JE_PLAN(RelationInputPlan__Clone__clone)(jazz_engine_RelationInputPlan *dst, const jazz_engine_RelationInputPlan *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    switch (src->tag) {
    case JAZZ_ENGINE_RIP_LINEAR: JE_PLAN(LinearCurrentRoot__Clone__clone)(&dst->u.linear, &src->u.linear); break;
    case JAZZ_ENGINE_RIP_UNION: JE_PLAN(UnionPlan__Clone__clone)(&dst->u.union_, &src->u.union_); break;
    case JAZZ_ENGINE_RIP_RECURSIVE: JE_PLAN(RecursiveRelationPlan__Clone__clone)(&dst->u.recursive, &src->u.recursive); break;
    }
}
void JE_PLAN(RelationInputPlan__drop)(jazz_engine_RelationInputPlan *p) {
    switch (p->tag) {
    case JAZZ_ENGINE_RIP_LINEAR: JE_PLAN(LinearCurrentRoot__drop)(&p->u.linear); break;
    case JAZZ_ENGINE_RIP_UNION: JE_PLAN(UnionPlan__drop)(&p->u.union_); break;
    case JAZZ_ENGINE_RIP_RECURSIVE: JE_PLAN(RecursiveRelationPlan__drop)(&p->u.recursive); break;
    }
}
static void cpp_clone_v(void *d, const void *s) { JE_PLAN(CorrelatedPathPlan__Clone__clone)(d, s); }
static void cpp_drop_v(void *p) { JE_PLAN(CorrelatedPathPlan__drop)(p); }
void JE_PLAN(CorrelatedPathPlan__Clone__clone)(jazz_engine_CorrelatedPathPlan *dst, const jazz_engine_CorrelatedPathPlan *src) {
    JE_PLAN(LinearCurrentRoot__Clone__clone)(&dst->parent, &src->parent);
    JE_PLAN(LinearCurrentRoot__Clone__clone)(&dst->child, &src->child);
    JE_IN(ProgramPathId__Clone__clone)(&dst->path, &src->path);
    pred_clone(&dst->correlation, &src->correlation);
    dst->requirement = src->requirement;
    jazz_engine__steps_clone(&dst->output_steps, &src->output_steps);
    jz_vec_clone(&dst->siblings, &src->siblings, sizeof(jazz_engine_CorrelatedPathPlan), cpp_clone_v);
    jz_vec_clone(&dst->nested, &src->nested, sizeof(jazz_engine_CorrelatedPathPlan), cpp_clone_v);
}
void JE_PLAN(CorrelatedPathPlan__drop)(jazz_engine_CorrelatedPathPlan *p) {
    JE_PLAN(LinearCurrentRoot__drop)(&p->parent);
    JE_PLAN(LinearCurrentRoot__drop)(&p->child);
    JE_IN(ProgramPathId__drop)(&p->path);
    pred_drop(&p->correlation);
    jazz_engine__steps_drop(&p->output_steps);
    jz_vec_drop(&p->siblings, sizeof(jazz_engine_CorrelatedPathPlan), cpp_drop_v);
    jz_vec_drop(&p->nested, sizeof(jazz_engine_CorrelatedPathPlan), cpp_drop_v);
}
void JE_PLAN(AnalyzedQueryPlan__Clone__clone)(jazz_engine_AnalyzedQueryPlan *dst, const jazz_engine_AnalyzedQueryPlan *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    switch (src->tag) {
    case JAZZ_ENGINE_AQP_LINEAR: JE_PLAN(LinearCurrentRoot__Clone__clone)(&dst->u.linear, &src->u.linear); break;
    case JAZZ_ENGINE_AQP_UNION: JE_PLAN(UnionPlan__Clone__clone)(&dst->u.union_, &src->u.union_); break;
    case JAZZ_ENGINE_AQP_CORRELATED_PATH: JE_PLAN(CorrelatedPathPlan__Clone__clone)(&dst->u.correlated, &src->u.correlated); break;
    case JAZZ_ENGINE_AQP_RECURSIVE_RELATION: JE_PLAN(RecursiveRelationPlan__Clone__clone)(&dst->u.recursive, &src->u.recursive); break;
    }
}
void JE_PLAN(AnalyzedQueryPlan__drop)(jazz_engine_AnalyzedQueryPlan *p) {
    switch (p->tag) {
    case JAZZ_ENGINE_AQP_LINEAR: JE_PLAN(LinearCurrentRoot__drop)(&p->u.linear); break;
    case JAZZ_ENGINE_AQP_UNION: JE_PLAN(UnionPlan__drop)(&p->u.union_); break;
    case JAZZ_ENGINE_AQP_CORRELATED_PATH: JE_PLAN(CorrelatedPathPlan__drop)(&p->u.correlated); break;
    case JAZZ_ENGINE_AQP_RECURSIVE_RELATION: JE_PLAN(RecursiveRelationPlan__drop)(&p->u.recursive); break;
    }
}

/* ================================================================ derives: Debug */
static void tof_debug(jz_str *out, const jazz_engine_TypedOutputField *f) { JE_IN(TypedOutputField__Debug__fmt)(out, f); }
static void option_tof_debug(jz_str *out, bool some, const jazz_engine_TypedOutputField *f) {
    if (!some) { jazz_engine__fmt__option_none(out); return; }
    jazz_engine__fmt__tuple_begin(out, "Some");
    tof_debug(out, f);
    jazz_engine__fmt__tuple_end(out);
}
static void projection_debug_v(jz_str *out, const void *p) {
    const jazz_engine_RowProjection *r = p;
    jazz_engine__fmt__struct_begin(out, "RowProjection");
    jazz_engine__fmt__field(out, "output", true);
    tof_debug(out, &r->output);
    jazz_engine__fmt__field(out, "value", false);
    nvr_debug(out, &r->value);
    jazz_engine__fmt__struct_end(out);
}
static void order_key_debug_v(jz_str *out, const void *p) {
    const jazz_engine_OrderKey *k = p;
    jazz_engine__fmt__struct_begin(out, "OrderKey");
    jazz_engine__fmt__field(out, "value", true);
    nvr_debug(out, &k->value);
    jazz_engine__fmt__field(out, "direction", false);
    jz_str_push_cstr(out, k->direction == JAZZ_ENGINE_SORT_ASC ? "Asc" : "Desc");
    jazz_engine__fmt__struct_end(out);
}
static void aggregate_debug_v(jz_str *out, const void *p) {
    const jazz_engine_AggregateExpr *a = p;
    jazz_engine__fmt__struct_begin(out, "AggregateExpr");
    jazz_engine__fmt__field(out, "output", true);
    tof_debug(out, &a->output);
    jazz_engine__fmt__field(out, "function", false);
    jz_str_push_cstr(out, jazz_engine__aggregate_function_name(a->function));
    jazz_engine__fmt__field(out, "input", false);
    if (a->has_input) {
        jazz_engine__fmt__tuple_begin(out, "Some");
        nvr_debug(out, &a->input);
        jazz_engine__fmt__tuple_end(out);
    } else {
        jazz_engine__fmt__option_none(out);
    }
    jazz_engine__fmt__struct_end(out);
}
static void step_debug_v(jz_str *out, const void *s) { JE_PLAN(LinearStep__Debug__fmt)(out, s); }
static void steps_debug(jz_str *out, const jazz_engine_LinearStepVec *v) {
    jazz_engine__fmt__list(out, v->ptr, v->len, sizeof(jazz_engine_LinearStep), step_debug_v);
}

void JE_PLAN(LinearRoot__Debug__fmt)(jz_str *out, const jazz_engine_LinearRoot *r) {
    switch (r->tag) {
    case JAZZ_ENGINE_LROOT_SOURCE:
        jazz_engine__fmt__struct_begin(out, "Source");
        jazz_engine__fmt__field(out, "source", true);
        sid_debug(out, &r->source);
        jazz_engine__fmt__field(out, "visibility", false);
        jz_str_push_cstr(out, r->visibility == JAZZ_ENGINE_VIS_VISIBLE ? "Visible" : "IncludeDeleted");
        break;
    case JAZZ_ENGINE_LROOT_VALUE:
        jazz_engine__fmt__struct_begin(out, "Value");
        jazz_engine__fmt__field(out, "shape", true);
        jazz_engine__fmt__str(out, &r->shape);
        jazz_engine__fmt__field(out, "columns", false);
        jazz_engine__fmt__list(out, r->columns.ptr, r->columns.len, sizeof(jazz_engine_ValueSourceColumn), jazz_engine__vsc_debug_v);
        jazz_engine__fmt__field(out, "mode", false);
        jz_str_push_cstr(out, r->mode == JAZZ_ENGINE_VSM_BINDING ? "Binding" : "Inline");
        break;
    case JAZZ_ENGINE_LROOT_FRONTIER:
        jazz_engine__fmt__struct_begin(out, "Frontier");
        jazz_engine__fmt__field(out, "frontier", true);
        jazz_engine__frontier_id_debug(out, &r->frontier);
        jazz_engine__fmt__field(out, "columns", false);
        jazz_engine__fmt__list(out, r->columns.ptr, r->columns.len, sizeof(jazz_engine_ValueSourceColumn), jazz_engine__vsc_debug_v);
        break;
    }
    jazz_engine__fmt__struct_end(out);
}
void JE_PLAN(LinearCurrentRoot__Debug__fmt)(jz_str *out, const jazz_engine_LinearCurrentRoot *r) {
    jazz_engine__fmt__struct_begin(out, "LinearCurrentRoot");
    jazz_engine__fmt__field(out, "root", true);
    JE_PLAN(LinearRoot__Debug__fmt)(out, &r->root);
    jazz_engine__fmt__field(out, "steps", false);
    steps_debug(out, &r->steps);
    jazz_engine__fmt__struct_end(out);
}
static void recursive_debug(jz_str *out, const jazz_engine_RecursiveRelationPlan *p) {
    jazz_engine__fmt__struct_begin(out, "RecursiveRelationPlan");
    jazz_engine__fmt__field(out, "seed", true);
    JE_PLAN(LinearCurrentRoot__Debug__fmt)(out, &p->seed);
    jazz_engine__fmt__field(out, "step", false);
    JE_PLAN(LinearCurrentRoot__Debug__fmt)(out, &p->step);
    jazz_engine__fmt__field(out, "frontier", false);
    jazz_engine__frontier_id_debug(out, &p->frontier);
    jazz_engine__fmt__field(out, "frontier_key", false);
    nvr_debug(out, &p->frontier_key);
    jazz_engine__fmt__field(out, "dedupe_keys", false);
    jazz_engine__nvrs_debug(out, &p->dedupe_keys);
    jazz_engine__fmt__field(out, "bound", false);
    if (p->bound.tag == JAZZ_MODEL_RECURSION_FIXPOINT) jz_str_push_cstr(out, "Fixpoint");
    else jz_str_appendf(out, "MaxDepth(%u)", p->bound.max_depth);
    jazz_engine__fmt__struct_end(out);
}
static void branch_debug_v(jz_str *out, const void *p) {
    const jazz_engine_UnionBranchPlan *b = p;
    jazz_engine__fmt__struct_begin(out, "UnionBranchPlan");
    jazz_engine__fmt__field(out, "label", true);
    jazz_engine__fmt__str(out, &b->label);
    jazz_engine__fmt__field(out, "plan", false);
    JE_PLAN(RelationInputPlan__Debug__fmt)(out, &b->plan);
    jazz_engine__fmt__struct_end(out);
}
static void union_debug(jz_str *out, const jazz_engine_UnionPlan *p) {
    jazz_engine__fmt__struct_begin(out, "UnionPlan");
    jazz_engine__fmt__field(out, "branches", true);
    jazz_engine__fmt__list(out, p->branches.ptr, p->branches.len, sizeof(jazz_engine_UnionBranchPlan), branch_debug_v);
    jazz_engine__fmt__field(out, "terminal_steps", false);
    steps_debug(out, &p->terminal_steps);
    jazz_engine__fmt__struct_end(out);
}
void JE_PLAN(RelationInputPlan__Debug__fmt)(jz_str *out, const jazz_engine_RelationInputPlan *p) {
    switch (p->tag) {
    case JAZZ_ENGINE_RIP_LINEAR:
        jazz_engine__fmt__tuple_begin(out, "Linear");
        JE_PLAN(LinearCurrentRoot__Debug__fmt)(out, &p->u.linear);
        break;
    case JAZZ_ENGINE_RIP_UNION:
        jazz_engine__fmt__tuple_begin(out, "Union");
        union_debug(out, &p->u.union_);
        break;
    case JAZZ_ENGINE_RIP_RECURSIVE:
        jazz_engine__fmt__tuple_begin(out, "Recursive");
        recursive_debug(out, &p->u.recursive);
        break;
    }
    jazz_engine__fmt__tuple_end(out);
}
void JE_PLAN(LinearStep__Debug__fmt)(jz_str *out, const jazz_engine_LinearStep *s) {
    switch (s->tag) {
    case JAZZ_ENGINE_STEP_FILTER:
        jazz_engine__fmt__tuple_begin(out, "Filter");
        pred_debug(out, &s->u.filter);
        jazz_engine__fmt__tuple_end(out);
        return;
    case JAZZ_ENGINE_STEP_JOIN:
        jazz_engine__fmt__struct_begin(out, "Join");
        jazz_engine__fmt__field(out, "right", true);
        JE_PLAN(RelationInputPlan__Debug__fmt)(out, s->u.join.right);
        jazz_engine__fmt__field(out, "mode", false);
        jz_str_push_cstr(out, jazz_engine__join_mode_name(s->u.join.mode));
        jazz_engine__fmt__field(out, "on", false);
        pred_debug(out, &s->u.join.on);
        break;
    case JAZZ_ENGINE_STEP_PROJECT:
        jazz_engine__fmt__tuple_begin(out, "Project");
        jazz_engine__fmt__list(out, s->u.project.ptr, s->u.project.len, sizeof(jazz_engine_RowProjection), projection_debug_v);
        jazz_engine__fmt__tuple_end(out);
        return;
    case JAZZ_ENGINE_STEP_ORDER_BY:
        jazz_engine__fmt__tuple_begin(out, "OrderBy");
        jazz_engine__fmt__list(out, s->u.order_by.ptr, s->u.order_by.len, sizeof(jazz_engine_OrderKey), order_key_debug_v);
        jazz_engine__fmt__tuple_end(out);
        return;
    case JAZZ_ENGINE_STEP_SLICE:
        jazz_engine__fmt__struct_begin(out, "Slice");
        jazz_engine__fmt__field(out, "partition_by", true);
        jazz_engine__nvrs_debug(out, &s->u.slice.partition_by);
        jazz_engine__fmt__field(out, "limit", false);
        if (s->u.slice.has_limit) jz_str_appendf(out, "Some(%u)", s->u.slice.limit);
        else jazz_engine__fmt__option_none(out);
        jazz_engine__fmt__field(out, "offset", false);
        jz_str_appendf(out, "%u", s->u.slice.offset);
        jazz_engine__fmt__field(out, "tie_breaker", false);
        jazz_engine__nvrs_debug(out, &s->u.slice.tie_breaker);
        jazz_engine__fmt__field(out, "rank_output", false);
        option_tof_debug(out, s->u.slice.has_rank_output, &s->u.slice.rank_output);
        break;
    case JAZZ_ENGINE_STEP_AGGREGATE:
        jazz_engine__fmt__struct_begin(out, "Aggregate");
        jazz_engine__fmt__field(out, "group_by", true);
        jazz_engine__nvrs_debug(out, &s->u.aggregate.group_by);
        jazz_engine__fmt__field(out, "outputs", false);
        jazz_engine__fmt__list(out, s->u.aggregate.outputs.ptr, s->u.aggregate.outputs.len, sizeof(jazz_engine_AggregateExpr), aggregate_debug_v);
        break;
    }
    jazz_engine__fmt__struct_end(out);
}

/* ================================================================ plan accessors */
const jazz_engine_SourceId *JE_PLAN(LinearRoot__source)(const jazz_engine_LinearRoot *r) {
    return r->tag == JAZZ_ENGINE_LROOT_SOURCE ? &r->source : NULL;
}

const jazz_engine_SourceId *JE_PLAN(AnalyzedQueryPlan__root_source)(const jazz_engine_AnalyzedQueryPlan *p) {
    const jazz_engine_SourceId *s = NULL;
    switch (p->tag) {
    case JAZZ_ENGINE_AQP_LINEAR:
        s = JE_PLAN(LinearRoot__source)(&p->u.linear.root);
        if (!s) jz_panic("linear root source");
        return s;
    case JAZZ_ENGINE_AQP_UNION:
        s = JE_PLAN(UnionPlan__root_source)(&p->u.union_);
        if (!s) jz_panic("union root source");
        return s;
    case JAZZ_ENGINE_AQP_CORRELATED_PATH:
        s = JE_PLAN(LinearRoot__source)(&p->u.correlated.parent.root);
        if (!s) jz_panic("path parent source");
        return s;
    case JAZZ_ENGINE_AQP_RECURSIVE_RELATION: {
        const jazz_engine_RecursiveRelationPlan *r = &p->u.recursive;
        s = JE_PLAN(LinearRoot__source)(&r->seed.root);
        if (!s) s = JE_PLAN(first_step_source)(r->seed.steps.ptr, r->seed.steps.len);
        if (!s) s = JE_PLAN(LinearRoot__source)(&r->step.root);
        if (!s) s = JE_PLAN(first_step_source)(r->step.steps.ptr, r->step.steps.len);
        if (!s) jz_panic("recursive source");
        return s;
    }
    }
    JZ_UNREACHABLE();
}

const char *JE_PLAN(AnalyzedQueryPlan__capability_label)(const jazz_engine_AnalyzedQueryPlan *p) {
    switch (p->tag) {
    case JAZZ_ENGINE_AQP_LINEAR: return "table-rooted current lowering";
    case JAZZ_ENGINE_AQP_UNION: return "union current lowering";
    case JAZZ_ENGINE_AQP_CORRELATED_PATH: return "correlated path projection analysis";
    default: return "recursive relation analysis";
    }
}

const jazz_engine_SourceId *JE_PLAN(first_step_source)(const jazz_engine_LinearStep *steps, u32 n) {
    for (u32 i = 0; i < n; i++) {
        if (steps[i].tag != JAZZ_ENGINE_STEP_JOIN) continue;
        const jazz_engine_SourceId *s = JE_PLAN(RelationInputPlan__root_source)(steps[i].u.join.right);
        if (s) return s;
    }
    return NULL;
}

const jazz_engine_SourceId *JE_PLAN(UnionPlan__root_source)(const jazz_engine_UnionPlan *p) {
    const jazz_engine_SourceId *first = NULL;
    for (u32 i = 0; i < p->branches.len; i++) {
        const jazz_engine_SourceId *s = JE_PLAN(RelationInputPlan__root_source)(&p->branches.ptr[i].plan);
        if (!s) continue;
        if (!first) first = s;
        else if (!sid_eq(s, first)) return NULL;
    }
    return first;
}

const jazz_engine_SourceId *JE_PLAN(RecursiveRelationPlan__root_source)(const jazz_engine_RecursiveRelationPlan *p) {
    const jazz_engine_SourceId *s = JE_PLAN(LinearRoot__source)(&p->seed.root);
    if (!s) s = JE_PLAN(first_step_source)(p->seed.steps.ptr, p->seed.steps.len);
    if (!s) s = JE_PLAN(LinearRoot__source)(&p->step.root);
    if (!s) s = JE_PLAN(first_step_source)(p->step.steps.ptr, p->step.steps.len);
    return s;
}
const jazz_engine_SourceId *JE_PLAN(RecursiveRelationPlan__seed_source)(const jazz_engine_RecursiveRelationPlan *p) {
    const jazz_engine_SourceId *s = JE_PLAN(LinearRoot__source)(&p->seed.root);
    return s ? s : JE_PLAN(first_step_source)(p->seed.steps.ptr, p->seed.steps.len);
}
const jazz_engine_SourceId *JE_PLAN(RecursiveRelationPlan__step_source)(const jazz_engine_RecursiveRelationPlan *p) {
    const jazz_engine_SourceId *s = JE_PLAN(LinearRoot__source)(&p->step.root);
    return s ? s : JE_PLAN(first_step_source)(p->step.steps.ptr, p->step.steps.len);
}
const jazz_engine_SourceId *JE_PLAN(RelationInputPlan__root_source)(const jazz_engine_RelationInputPlan *p) {
    switch (p->tag) {
    case JAZZ_ENGINE_RIP_LINEAR: return JE_PLAN(LinearRoot__source)(&p->u.linear.root);
    case JAZZ_ENGINE_RIP_UNION: return JE_PLAN(UnionPlan__root_source)(&p->u.union_);
    default: return JE_PLAN(RecursiveRelationPlan__root_source)(&p->u.recursive);
    }
}

/* ================================================================ analysis */
static jazz_engine_UrErr JE_PLAN(validate_row_set_nesting)(const jz_str *root, const jz_btree *nodes);
static jazz_engine_UrErr JE_PLAN(analyze_root_node)(const jazz_engine_QueryProgramRequest *request, jazz_engine_AnalyzedQueryPlan *out);
static void JE_PLAN(plan_with_default_result_order)(jazz_engine_AnalyzedQueryPlan *plan, const jazz_engine_QueryProgramRequest *request);
static void JE_PLAN(validate_recursive_arg_by_capabilities)(const jazz_engine_AnalyzedQueryPlan *plan, jazz_engine_UnsupportedReasonVec *gaps);
static void JE_PLAN(analyzed_plan_sources)(const jazz_engine_AnalyzedQueryPlan *plan, jz_btree *out);
static const jazz_engine_SchemaProjection *JE_PLAN(supported_current_storage_projection)(const jazz_engine_SourceExpr *source);
static jazz_engine_UrErr JE_PLAN(analyze_current_node)(const jz_str *node_id, const jz_btree *nodes, jz_btree *visited,
                                                        jazz_engine_LinearRoot *root, jazz_engine_LinearStepVec *steps);
static jazz_engine_UrErr JE_PLAN(analyze_union)(const jazz_engine_UnionInput *inputs, u32 n, const jz_btree *nodes, jz_btree *visited,
                                                 jazz_engine_UnionPlan *out);
static jazz_engine_UrErr JE_PLAN(analyze_linear_root)(const jz_str *node_id, const jazz_engine_QueryProgramRequest *request, jz_btree *visited,
                                                       jazz_engine_LinearCurrentRoot *out);
static jazz_engine_UrErr JE_PLAN(analyze_linear_subplan)(const jz_str *node_id, const jz_btree *nodes, jz_btree *visited,
                                                          jazz_engine_LinearCurrentRoot *out);
static jazz_engine_UrErr JE_PLAN(analyze_correlated_child_subplan)(const jz_str *child_input, const jazz_engine_ProgramPathId *path,
                                                                    const jz_btree *nodes, jz_btree *visited, jazz_engine_LinearCurrentRoot *out);
static jazz_engine_UrErr JE_PLAN(analyze_correlated_path_root)(const jz_str *node_id, const jazz_engine_QueryProgramRequest *request,
                                                                jz_btree *visited, jazz_engine_CorrelatedPathPlan *out);
static jazz_engine_UrErr JE_PLAN(collect_correlated_paths)(const jazz_engine_SourceId *owner, const jazz_engine_SourceId *excluded_child,
                                                            const jz_btree *nodes, jz_btree *visited, jazz_engine_CorrelatedPathPlanVec *out);
static jazz_engine_UrErr JE_PLAN(validate_result_source)(const jazz_engine_QueryProgramRequest *request, const jazz_engine_SourceId *source);
static jazz_engine_UrErr JE_PLAN(validate_join_relation)(const jazz_engine_LinearCurrentRoot *plan);
static void JE_PLAN(validate_step_order)(const jazz_engine_LinearStepVec *steps, jazz_engine_UnsupportedReasonVec *gaps);

static void push_gap(jazz_engine_UnsupportedReasonVec *gaps, jazz_engine_UnsupportedReason gap) { JZ_VEC_PUSH(*gaps, gap); }
static void push_source_gap(jazz_engine_UnsupportedReasonVec *gaps, u8 gap_tag) {
    jazz_engine_UnsupportedReason r = jazz_engine__ur_new(JAZZ_ENGINE_UR_SOURCE);
    r.source.tag = gap_tag;
    push_gap(gaps, r);
}
static void push_operator_gap(jazz_engine_UnsupportedReasonVec *gaps, const char *message) {
    jazz_engine_UnsupportedReason r = jazz_engine__ur_new(JAZZ_ENGINE_UR_OPERATOR);
    r.message = jz_str_from(message);
    push_gap(gaps, r);
}
static void visited_insert(jz_btree *visited, const jz_str *id) { jazz_engine__strset_insert_clone(visited, id); }

bool JE_PLAN(analyze_query_plan)(const jazz_engine_QueryProgramRequest *request, jazz_engine_AnalyzedQueryPlan *out,
                                 jazz_engine_UnsupportedReasonVec *gaps) {
    memset(gaps, 0, sizeof *gaps);
    jazz_engine_UrErr gap = JE_PLAN(validate_row_set_nesting)(&request->input.shape.root, &request->input.shape.nodes);
    if (gap) {
        push_gap(gaps, jazz_engine__ur_take(gap));
        return false;
    }

    if (request->reads.fact_reads.len != 0) push_source_gap(gaps, JAZZ_ENGINE_GAP_TRANSACTION_READ_OVERLAY);
    jazz_engine_AnalyzedQueryPlan plan;
    gap = JE_PLAN(analyze_root_node)(request, &plan);
    if (gap) {
        push_gap(gaps, jazz_engine__ur_take(gap));
        return false;
    }
    JE_PLAN(plan_with_default_result_order)(&plan, request);
    JE_PLAN(validate_output_capabilities)(request, &plan, gaps);
    JE_PLAN(validate_recursive_arg_by_capabilities)(&plan, gaps);

    jz_btree sources;
    JE_PLAN(analyzed_plan_sources)(&plan, &sources);
    jazz_engine_SourceId *plan_source; void *unused;
    JZ_BTREE_FOR(&sources, plan_source, unused) {
        const jazz_engine_SourceExpr *read_source = jz_btree_get(&request->reads.primary.sources, plan_source);
        const jazz_engine_SchemaProjection *projection = JE_PLAN(supported_current_storage_projection)(read_source);
        if (!projection) {
            push_source_gap(gaps, JAZZ_ENGINE_GAP_HISTORICAL_STORAGE_CUT);
            continue;
        }
        if (projection->schema_family_tag != JAZZ_ENGINE_SFS_CURRENT
            || !(projection->storage_tag == JAZZ_ENGINE_SSS_SINGLE || projection->storage_tag == JAZZ_ENGINE_SSS_COMPATIBLE_PARTITIONS)
            || projection->lens_tag != JAZZ_ENGINE_LENS_CANONICAL)
            push_source_gap(gaps, JAZZ_ENGINE_GAP_SCHEMA_PROJECTION);
    }
    jz_btree_drop(&sources);

    if (gaps->len == 0) {
        *out = plan;
        return true;
    }
    JE_PLAN(AnalyzedQueryPlan__drop)(&plan);
    return false;
}

/* children of a row-set node, borrowed from the node map */
typedef JZ_VEC(const jz_str *) node_ref_vec;
static void node_children(const jazz_engine_RowSetExpr *node, node_ref_vec *children) {
    switch (node->tag) {
    case JAZZ_ENGINE_EXPR_SOURCE:
    case JAZZ_ENGINE_EXPR_VALUE_SOURCE:
    case JAZZ_ENGINE_EXPR_FRONTIER_SOURCE: break;
    case JAZZ_ENGINE_EXPR_FILTER: JZ_VEC_PUSH(*children, (const jz_str *)&node->u.filter.input); break;
    case JAZZ_ENGINE_EXPR_DISTINCT: JZ_VEC_PUSH(*children, (const jz_str *)&node->u.distinct.input); break;
    case JAZZ_ENGINE_EXPR_PROJECT: JZ_VEC_PUSH(*children, (const jz_str *)&node->u.project.input); break;
    case JAZZ_ENGINE_EXPR_ORDER_BY: JZ_VEC_PUSH(*children, (const jz_str *)&node->u.order_by.input); break;
    case JAZZ_ENGINE_EXPR_SLICE: JZ_VEC_PUSH(*children, (const jz_str *)&node->u.slice.input); break;
    case JAZZ_ENGINE_EXPR_AGGREGATE: JZ_VEC_PUSH(*children, (const jz_str *)&node->u.aggregate.input); break;
    case JAZZ_ENGINE_EXPR_JOIN:
        JZ_VEC_PUSH(*children, (const jz_str *)&node->u.join.left);
        JZ_VEC_PUSH(*children, (const jz_str *)&node->u.join.right);
        break;
    case JAZZ_ENGINE_EXPR_RECURSIVE_RELATION:
        JZ_VEC_PUSH(*children, (const jz_str *)&node->u.recursive.seed);
        JZ_VEC_PUSH(*children, (const jz_str *)&node->u.recursive.step);
        break;
    case JAZZ_ENGINE_EXPR_UNION:
        JZ_VEC_FOR(const jazz_engine_UnionInput, in, node->u.union_.inputs) JZ_VEC_PUSH(*children, (const jz_str *)&in->node);
        break;
    case JAZZ_ENGINE_EXPR_CORRELATED_PATH_PROJECTION:
        JZ_VEC_PUSH(*children, (const jz_str *)&node->u.correlated.input);
        JZ_VEC_PUSH(*children, (const jz_str *)&node->u.correlated.child_input);
        break;
    }
}

typedef struct { const jz_str *id; bool expanded; } nesting_pending;

static jazz_engine_UrErr nesting_cycle(const jz_str *id) {
    JE_DBG(d, jazz_engine__node_id_debug, id);
    jazz_engine_UrErr e = jazz_engine__ur_op(jz_format("row-set nesting contains a cycle at node %.*s", JE_STR_ARG(d)));
    jz_str_drop(&d);
    return e;
}

static jazz_engine_UrErr JE_PLAN(validate_row_set_nesting)(const jz_str *root, const jz_btree *nodes) {
    JZ_VEC(nesting_pending) pending = {0};
    jz_btree active, heights;
    jz_btree_init(&active, &jazz_engine__STRSET_DESC);
    jz_btree_init(&heights, &jazz_engine__STR_U32_MAP_DESC);
    node_ref_vec children = {0};
    jazz_engine_UrErr err = NULL;
    nesting_pending first = {root, false};
    JZ_VEC_PUSH(pending, first);
    while (pending.len) {
        nesting_pending top = pending.ptr[--pending.len];
        const jz_str *node_id = top.id;
        if (jz_btree_contains(&heights, node_id)) continue;
        const jazz_engine_RowSetExpr *node = jz_btree_get(nodes, node_id);
        if (!node) {
            JE_DBG(d, jazz_engine__node_id_debug, node_id);
            err = jazz_engine__ur_op(jz_format("row-set node %.*s is missing", JE_STR_ARG(d)));
            jz_str_drop(&d);
            goto done;
        }
        children.len = 0;
        node_children(node, &children);
        if (top.expanded) {
            jz_btree_remove(&active, node_id, NULL, NULL);
            u32 max = 0;
            for (u32 i = 0; i < children.len; i++) {
                const u32 *h = jz_btree_get(&heights, children.ptr[i]);
                if (h && *h > max) max = *h;
            }
            u32 height = 1 + max;
            if (height > JAZZ_ENGINE_MAX_ROW_SET_NESTING_DEPTH) {
                err = jazz_engine__ur_op(jz_format("row-set nesting depth exceeds MAX_ROW_SET_NESTING_DEPTH (%u)", JAZZ_ENGINE_MAX_ROW_SET_NESTING_DEPTH));
                goto done;
            }
            jz_str k = jz_str_clone(node_id);
            jz_btree_insert(&heights, &k, &height, NULL);
            continue;
        }
        if (jz_btree_contains(&active, node_id)) {
            err = nesting_cycle(node_id);
            goto done;
        }
        visited_insert(&active, node_id);
        nesting_pending again = {node_id, true};
        JZ_VEC_PUSH(pending, again);
        for (u32 i = children.len; i-- > 0;) {
            const jz_str *child = children.ptr[i];
            if (jz_btree_contains(&active, child)) {
                err = nesting_cycle(child);
                goto done;
            }
            if (!jz_btree_contains(&heights, child)) {
                nesting_pending p = {child, false};
                JZ_VEC_PUSH(pending, p);
            }
        }
    }
done:
    jz_vec_drop(&pending, sizeof(nesting_pending), NULL);
    jz_vec_drop(&children, sizeof(const jz_str *), NULL);
    jz_btree_drop(&active);
    jz_btree_drop(&heights);
    return err;
}

static bool facts_contain(const jazz_engine_QueryProgramRequest *request, u8 tag) {
    jazz_engine_ProgramFactKey k = jazz_engine__fact_key_simple(tag);
    bool r = jz_btree_contains(&request->output.facts, &k);
    JE_OUT(ProgramFactKey__drop)(&k);
    return r;
}

static bool JE_PLAN(plan_contains_aggregate)(const jazz_engine_AnalyzedQueryPlan *plan);
static bool JE_PLAN(maintained_result_membership_window_supported)(const jazz_engine_AnalyzedQueryPlan *plan);

void JE_PLAN(validate_output_capabilities)(const jazz_engine_QueryProgramRequest *request, const jazz_engine_AnalyzedQueryPlan *plan,
                                           jazz_engine_UnsupportedReasonVec *gaps) {
    if (!facts_contain(request, JAZZ_ENGINE_FACT_RESULT_MEMBERSHIP) && !facts_contain(request, JAZZ_ENGINE_FACT_AUTHORIZED_ROWS)) return;
    if (!facts_contain(request, JAZZ_ENGINE_FACT_RESULT_MEMBERSHIP)) return;
    if (JE_PLAN(plan_contains_aggregate)(plan)) return;
    if (JE_PLAN(maintained_result_membership_window_supported)(plan)) return;
    push_operator_gap(gaps, "maintained subscription view window shape is not lowered yet");
}

static bool JE_PLAN(logical_recursive_plan_contains_arg_by)(const jazz_engine_RecursiveRelationPlan *relation) {
    JZ_VEC(const jazz_engine_LinearCurrentRoot *) linear_plans = {0};
    JZ_VEC(const jazz_engine_RelationInputPlan *) relation_inputs = {0};
    JZ_VEC_PUSH(linear_plans, (const jazz_engine_LinearCurrentRoot *)&relation->seed);
    JZ_VEC_PUSH(linear_plans, (const jazz_engine_LinearCurrentRoot *)&relation->step);
    bool found = false;
    for (;;) {
        if (linear_plans.len) {
            const jazz_engine_LinearCurrentRoot *plan = linear_plans.ptr[--linear_plans.len];
            bool pending_order_is_empty = true;
            JZ_VEC_FOR(const jazz_engine_LinearStep, step, plan->steps) {
                switch (step->tag) {
                case JAZZ_ENGINE_STEP_ORDER_BY: pending_order_is_empty = step->u.order_by.len == 0; break;
                case JAZZ_ENGINE_STEP_SLICE:
                    if (pending_order_is_empty && step->u.slice.offset == 0 && step->u.slice.has_limit && step->u.slice.limit == 1) {
                        found = true;
                        goto done;
                    }
                    pending_order_is_empty = true;
                    break;
                case JAZZ_ENGINE_STEP_AGGREGATE: pending_order_is_empty = true; break;
                case JAZZ_ENGINE_STEP_JOIN: JZ_VEC_PUSH(relation_inputs, (const jazz_engine_RelationInputPlan *)step->u.join.right); break;
                default: break;
                }
            }
            continue;
        }
        if (!relation_inputs.len) break;
        const jazz_engine_RelationInputPlan *input = relation_inputs.ptr[--relation_inputs.len];
        switch (input->tag) {
        case JAZZ_ENGINE_RIP_LINEAR: JZ_VEC_PUSH(linear_plans, &input->u.linear); break;
        case JAZZ_ENGINE_RIP_UNION:
            JZ_VEC_FOR(const jazz_engine_UnionBranchPlan, b, input->u.union_.branches) JZ_VEC_PUSH(relation_inputs, &b->plan);
            break;
        case JAZZ_ENGINE_RIP_RECURSIVE:
            JZ_VEC_PUSH(linear_plans, &input->u.recursive.seed);
            JZ_VEC_PUSH(linear_plans, &input->u.recursive.step);
            break;
        }
    }
done:
    jz_vec_drop(&linear_plans, sizeof(void *), NULL);
    jz_vec_drop(&relation_inputs, sizeof(void *), NULL);
    return found;
}

static void JE_PLAN(validate_recursive_arg_by_capabilities)(const jazz_engine_AnalyzedQueryPlan *plan, jazz_engine_UnsupportedReasonVec *gaps) {
    jazz_engine_PlanFragments f;
    JE_PLAN(collect_plan_fragments)(plan, &f);
    bool any = false;
    for (u32 i = 0; i < f.recursives.len && !any; i++) any = JE_PLAN(logical_recursive_plan_contains_arg_by)(f.recursives.ptr[i]);
    jazz_engine__plan_fragments_drop(&f);
    if (any) push_operator_gap(gaps, "arg_max_by and arg_min_by are not supported inside recursive seed or step graphs");
}

static jazz_engine_LinearStep JE_PLAN(default_root_order)(jazz_engine_SourceId source) {
    jazz_engine_LinearStep step;
    memset(&step, 0, sizeof step);
    step.tag = JAZZ_ENGINE_STEP_ORDER_BY;
    jazz_engine_OrderKey key;
    memset(&key, 0, sizeof key);
    key.value.tag = JAZZ_ENGINE_NVR_ROW_ID;
    key.value.sub = JAZZ_ENGINE_ROWID_SOURCE;
    key.value.u.source = source;
    key.direction = JAZZ_ENGINE_SORT_ASC;
    JZ_VEC_PUSH(step.u.order_by, key);
    return step;
}

static void JE_PLAN(inject_default_root_order)(const jazz_engine_LinearRoot *root, jazz_engine_LinearStepVec *steps) {
    const jazz_engine_SourceId *source = JE_PLAN(LinearRoot__source)(root);
    if (!source) return;
    u32 first_terminal = UINT32_MAX;
    for (u32 i = 0; i < steps->len; i++) {
        u8 t = steps->ptr[i].tag;
        if (t == JAZZ_ENGINE_STEP_ORDER_BY || t == JAZZ_ENGINE_STEP_SLICE || t == JAZZ_ENGINE_STEP_AGGREGATE) { first_terminal = i; break; }
    }
    if (first_terminal == UINT32_MAX) return;
    /* Explicit order and aggregate output own their order semantics. */
    u8 t = steps->ptr[first_terminal].tag;
    if (t == JAZZ_ENGINE_STEP_ORDER_BY || t == JAZZ_ENGINE_STEP_AGGREGATE) return;
    jazz_engine_LinearStep order = JE_PLAN(default_root_order)(jazz_engine__sid_cloned(source));
    jz_vec_insert(steps, first_terminal, &order, sizeof order);
}

/* Row-valued root windows have a public default order (ascending source row id). */
static void JE_PLAN(plan_with_default_result_order)(jazz_engine_AnalyzedQueryPlan *plan, const jazz_engine_QueryProgramRequest *request) {
    bool produces_root_rows = request->output.has_app_rows || facts_contain(request, JAZZ_ENGINE_FACT_RESULT_MEMBERSHIP);
    if (!produces_root_rows) return;
    switch (plan->tag) {
    case JAZZ_ENGINE_AQP_LINEAR: JE_PLAN(inject_default_root_order)(&plan->u.linear.root, &plan->u.linear.steps); break;
    case JAZZ_ENGINE_AQP_CORRELATED_PATH:
        /* the root order/window of a correlated root applies in its output tail */
        JE_PLAN(inject_default_root_order)(&plan->u.correlated.parent.root, &plan->u.correlated.output_steps);
        break;
    default: break;
    }
}

static bool JE_PLAN(is_slice_step)(const jazz_engine_LinearStep *step) { return step->tag == JAZZ_ENGINE_STEP_SLICE; }
static bool JE_PLAN(linear_window_supported)(const jazz_engine_LinearStep *steps, u32 n) {
    /* Relation-local slices use their declared row-id tie-breaker; root slices have
     * an explicit row-id OrderBy injected above. */
    (void)steps; (void)n;
    return true;
}

static bool JE_PLAN(maintained_result_membership_window_supported)(const jazz_engine_AnalyzedQueryPlan *plan) {
    jazz_engine_PlanFragments f;
    JE_PLAN(collect_plan_fragments)(plan, &f);
    bool recursive_slice = false;
    for (u32 i = 0; i < f.recursives.len && !recursive_slice; i++) {
        const jazz_engine_RecursiveRelationPlan *r = f.recursives.ptr[i];
        JZ_VEC_FOR(const jazz_engine_LinearStep, s, r->seed.steps) if (JE_PLAN(is_slice_step)(s)) recursive_slice = true;
        JZ_VEC_FOR(const jazz_engine_LinearStep, s, r->step.steps) if (JE_PLAN(is_slice_step)(s)) recursive_slice = true;
    }
    bool ok = !recursive_slice;
    if (ok)
        JZ_VEC_FOR(const jazz_engine_LinearFragment, frag, f.linears)
            if (!JE_PLAN(linear_window_supported)(frag->steps, frag->nsteps)) { ok = false; break; }
    jazz_engine__plan_fragments_drop(&f);
    return ok;
}

static bool JE_PLAN(plan_contains_aggregate)(const jazz_engine_AnalyzedQueryPlan *plan) {
    jazz_engine_PlanFragments f;
    JE_PLAN(collect_plan_fragments)(plan, &f);
    bool any = false;
    JZ_VEC_FOR(const jazz_engine_LinearFragment, frag, f.linears) {
        for (u32 i = 0; i < frag->nsteps; i++)
            if (frag->steps[i].tag == JAZZ_ENGINE_STEP_AGGREGATE) { any = true; break; }
        if (any) break;
    }
    jazz_engine__plan_fragments_drop(&f);
    return any;
}

bool JE_PLAN(root_aggregate_step)(const jazz_engine_AnalyzedQueryPlan *plan, const jazz_engine_NvrVec **group_by,
                                  const jazz_engine_AggregateExprVec **outputs) {
    if (plan->tag != JAZZ_ENGINE_AQP_LINEAR) return false;
    const jazz_engine_LinearStepVec *steps = &plan->u.linear.steps;
    if (!steps->len) return false;
    const jazz_engine_LinearStep *last = &steps->ptr[steps->len - 1];
    if (last->tag != JAZZ_ENGINE_STEP_AGGREGATE) return false;
    *group_by = &last->u.aggregate.group_by;
    *outputs = &last->u.aggregate.outputs;
    return true;
}

static jazz_engine_LinearStep slice_step_from(const jazz_engine_RowSetExpr *e) {
    jazz_engine_LinearStep s;
    memset(&s, 0, sizeof s);
    s.tag = JAZZ_ENGINE_STEP_SLICE;
    jazz_engine__nvrvec_clone(&s.u.slice.partition_by, &e->u.slice.partition_by);
    s.u.slice.has_limit = e->u.slice.has_limit;
    s.u.slice.limit = e->u.slice.limit;
    s.u.slice.offset = e->u.slice.offset;
    jazz_engine__nvrvec_clone(&s.u.slice.tie_breaker, &e->u.slice.tie_breaker);
    s.u.slice.has_rank_output = e->u.slice.has_rank_output;
    if (e->u.slice.has_rank_output) tof_clone(&s.u.slice.rank_output, &e->u.slice.rank_output);
    return s;
}
static jazz_engine_LinearStep order_step_from(const jazz_engine_OrderKeyVec *keys) {
    jazz_engine_LinearStep s;
    memset(&s, 0, sizeof s);
    s.tag = JAZZ_ENGINE_STEP_ORDER_BY;
    jz_vec_clone(&s.u.order_by, keys, sizeof(jazz_engine_OrderKey), jazz_engine__order_key_clone_v);
    return s;
}

/* -> the union inputs (NULL = None) plus its order/slice tail, innermost first */
static const jazz_engine_UnionInputVec *JE_PLAN(root_union_terminal_steps)(const jz_str *root, const jz_btree *nodes,
                                                                          jazz_engine_LinearStepVec *steps) {
    const jz_str *current = root;
    jazz_engine_LinearStepVec outer_to_inner = {0};
    for (;;) {
        const jazz_engine_RowSetExpr *node = jz_btree_get(nodes, current);
        if (!node) break;
        if (node->tag == JAZZ_ENGINE_EXPR_ORDER_BY) {
            JZ_VEC_PUSH(outer_to_inner, order_step_from(&node->u.order_by.keys));
            current = &node->u.order_by.input;
        } else if (node->tag == JAZZ_ENGINE_EXPR_SLICE) {
            JZ_VEC_PUSH(outer_to_inner, slice_step_from(node));
            current = &node->u.slice.input;
        } else if (node->tag == JAZZ_ENGINE_EXPR_UNION) {
            for (u32 i = 0, j = outer_to_inner.len; i + 1 < j; i++, j--) {
                jazz_engine_LinearStep t = outer_to_inner.ptr[i];
                outer_to_inner.ptr[i] = outer_to_inner.ptr[j - 1];
                outer_to_inner.ptr[j - 1] = t;
            }
            *steps = outer_to_inner;
            return &node->u.union_.inputs;
        } else {
            break;
        }
    }
    jazz_engine__steps_drop(&outer_to_inner);
    return NULL;
}

static jazz_engine_UrErr path_parent_source(const jazz_engine_LinearCurrentRoot *parent, const jazz_engine_SourceId **out) {
    *out = JE_PLAN(LinearRoot__source)(&parent->root);
    return *out ? NULL : JE_UR_OP("path parent must be a source");
}

static jazz_engine_UrErr JE_PLAN(analyze_root_node)(const jazz_engine_QueryProgramRequest *request, jazz_engine_AnalyzedQueryPlan *out) {
    const jazz_engine_NormalizedRowSetShape *shape = &request->input.shape;
    jz_btree visited;
    jz_btree_init(&visited, &jazz_engine__STRSET_DESC);
    jazz_engine_UrErr err = NULL;
    memset(out, 0, sizeof *out);
    const jazz_engine_RowSetExpr *root_node = jz_btree_get(&shape->nodes, &shape->root);
    if (!root_node) {
        JE_DBG(d, jazz_engine__node_id_debug, &shape->root);
        err = jazz_engine__ur_op(jz_format("row-set root node %.*s is missing", JE_STR_ARG(d)));
        jz_str_drop(&d);
        goto fail;
    }

    jazz_engine_LinearStepVec terminal_steps;
    const jazz_engine_UnionInputVec *root_union = JE_PLAN(root_union_terminal_steps)(&shape->root, &shape->nodes, &terminal_steps);
    if (root_union) {
        visited_insert(&visited, &shape->root);
        jazz_engine_UnionPlan u;
        err = JE_PLAN(analyze_union)(root_union->ptr, root_union->len, &shape->nodes, &visited, &u);
        if (err) { jazz_engine__steps_drop(&terminal_steps); goto fail; }
        jazz_engine__steps_drop(&u.terminal_steps);
        u.terminal_steps = terminal_steps;
        const jazz_engine_SourceId *rs = JE_PLAN(UnionPlan__root_source)(&u);
        err = rs ? JE_PLAN(validate_result_source)(request, rs) : JE_UR_OP("union result branches must share one root source");
        if (err) { JE_PLAN(UnionPlan__drop)(&u); goto fail; }
        out->tag = JAZZ_ENGINE_AQP_UNION;
        out->u.union_ = u;
        jz_btree_drop(&visited);
        return NULL;
    }

    switch (root_node->tag) {
    case JAZZ_ENGINE_EXPR_CORRELATED_PATH_PROJECTION: {
        const jazz_engine_RowSetExpr *n = root_node;
        visited_insert(&visited, &shape->root);
        jazz_engine_CorrelatedPathPlan p;
        memset(&p, 0, sizeof p);
        err = JE_PLAN(analyze_linear_root)(&n->u.correlated.input, request, &visited, &p.parent);
        if (err) goto fail;
        err = JE_PLAN(analyze_correlated_child_subplan)(&n->u.correlated.child_input, &n->u.correlated.path, &shape->nodes, &visited, &p.child);
        if (err) { JE_PLAN(LinearCurrentRoot__drop)(&p.parent); goto fail; }
        const jazz_engine_SourceId *parent_source;
        err = path_parent_source(&p.parent, &parent_source);
        if (!err) err = JE_PLAN(validate_result_source)(request, parent_source);
        if (err) { JE_PLAN(LinearCurrentRoot__drop)(&p.parent); JE_PLAN(LinearCurrentRoot__drop)(&p.child); goto fail; }
        JE_IN(ProgramPathId__Clone__clone)(&p.path, &n->u.correlated.path);
        pred_clone(&p.correlation, &n->u.correlated.correlation);
        p.requirement = n->u.correlated.requirement;
        err = path_parent_source(&p.parent, &parent_source);
        if (!err) err = JE_PLAN(collect_correlated_paths)(parent_source, &n->u.correlated.path.child, &shape->nodes, &visited, &p.siblings);
        if (!err) err = JE_PLAN(collect_correlated_paths)(&n->u.correlated.path.child, NULL, &shape->nodes, &visited, &p.nested);
        if (err) { JE_PLAN(CorrelatedPathPlan__drop)(&p); goto fail; }
        out->tag = JAZZ_ENGINE_AQP_CORRELATED_PATH;
        out->u.correlated = p;
        break;
    }
    case JAZZ_ENGINE_EXPR_RECURSIVE_RELATION: {
        const jazz_engine_RowSetExpr *n = root_node;
        visited_insert(&visited, &shape->root);
        jazz_engine_RecursiveRelationPlan r;
        memset(&r, 0, sizeof r);
        err = JE_PLAN(analyze_linear_root)(&n->u.recursive.seed, request, &visited, &r.seed);
        if (err) goto fail;
        err = JE_PLAN(analyze_linear_subplan)(&n->u.recursive.step, &shape->nodes, &visited, &r.step);
        if (err) { JE_PLAN(LinearCurrentRoot__drop)(&r.seed); goto fail; }
        bool ok = false;
        if (shape->result.tag == JAZZ_ENGINE_RESULT_REAL_ROW && shape->result.u.real_row.row.tag == JAZZ_ENGINE_RRR_SOURCE) {
            const jazz_engine_SourceId *result_source = &shape->result.u.real_row.row.source;
            const jazz_engine_SourceId *s1 = JE_PLAN(LinearRoot__source)(&r.seed.root);
            const jazz_engine_SourceId *s2 = JE_PLAN(LinearRoot__source)(&r.step.root);
            ok = (s1 && sid_eq(s1, result_source)) || (s2 && sid_eq(s2, result_source));
        } else if (shape->result.tag == JAZZ_ENGINE_RESULT_PATH_TUPLE) {
            ok = true;
        }
        if (!ok) {
            JE_PLAN(LinearCurrentRoot__drop)(&r.seed);
            JE_PLAN(LinearCurrentRoot__drop)(&r.step);
            err = JE_UR_OP("recursive relation result must be a seed/step real row or path tuple");
            goto fail;
        }
        r.frontier = jz_str_clone(&n->u.recursive.frontier);
        nvr_clone(&r.frontier_key, &n->u.recursive.frontier_key);
        jazz_engine__nvrvec_clone(&r.dedupe_keys, &n->u.recursive.dedupe_keys);
        r.bound = n->u.recursive.bound;
        out->tag = JAZZ_ENGINE_AQP_RECURSIVE_RELATION;
        out->u.recursive = r;
        break;
    }
    case JAZZ_ENGINE_EXPR_UNION: {
        visited_insert(&visited, &shape->root);
        jazz_engine_UnionPlan u;
        err = JE_PLAN(analyze_union)(root_node->u.union_.inputs.ptr, root_node->u.union_.inputs.len, &shape->nodes, &visited, &u);
        if (err) goto fail;
        const jazz_engine_SourceId *rs = JE_PLAN(UnionPlan__root_source)(&u);
        err = rs ? JE_PLAN(validate_result_source)(request, rs) : JE_UR_OP("union result branches must share one root source");
        if (err) { JE_PLAN(UnionPlan__drop)(&u); goto fail; }
        out->tag = JAZZ_ENGINE_AQP_UNION;
        out->u.union_ = u;
        break;
    }
    default: {
        jz_btree path_visited;
        jazz_engine__strset_clone(&path_visited, &visited);
        jazz_engine_CorrelatedPathPlan p;
        jazz_engine_UrErr path_err = JE_PLAN(analyze_correlated_path_root)(&shape->root, request, &path_visited, &p);
        if (!path_err) {
            jz_vec_drop(&p.nested, sizeof(jazz_engine_CorrelatedPathPlan), cpp_drop_v);
            const jazz_engine_SourceId *parent_source;
            err = JE_PLAN(collect_correlated_paths)(&p.path.child, NULL, &shape->nodes, &path_visited, &p.nested);
            if (!err) {
                jz_vec_drop(&p.siblings, sizeof(jazz_engine_CorrelatedPathPlan), cpp_drop_v);
                err = path_parent_source(&p.parent, &parent_source);
                if (!err) err = JE_PLAN(collect_correlated_paths)(parent_source, &p.path.child, &shape->nodes, &path_visited, &p.siblings);
            }
            if (!err) err = path_parent_source(&p.parent, &parent_source);
            if (!err) err = JE_PLAN(validate_result_source)(request, parent_source);
            if (err) { JE_PLAN(CorrelatedPathPlan__drop)(&p); jz_btree_drop(&path_visited); goto fail; }
            jz_btree_drop(&visited);
            visited = path_visited;
            out->tag = JAZZ_ENGINE_AQP_CORRELATED_PATH;
            out->u.correlated = p;
        } else {
            jazz_engine__ur_free(path_err);
            jz_btree_drop(&path_visited);
            jazz_engine_LinearCurrentRoot linear;
            err = JE_PLAN(analyze_linear_root)(&shape->root, request, &visited, &linear);
            if (err) goto fail;
            const jazz_engine_SourceId *ls = JE_PLAN(LinearRoot__source)(&linear.root);
            err = ls ? JE_PLAN(validate_result_source)(request, ls) : JE_UR_OP("result must be the root source row");
            if (err) { JE_PLAN(LinearCurrentRoot__drop)(&linear); goto fail; }
            out->tag = JAZZ_ENGINE_AQP_LINEAR;
            out->u.linear = linear;
        }
        break;
    }
    }

    if (visited.len != shape->nodes.len) {
        JE_PLAN(AnalyzedQueryPlan__drop)(out);
        err = JE_UR_OP("only connected current source/filter/join/order/slice/path/relation plans are lowered yet");
        goto fail;
    }
    jz_btree_drop(&visited);
    return NULL;
fail:
    jz_btree_drop(&visited);
    return err;
}

static jazz_engine_UrErr JE_PLAN(analyze_correlated_path_root)(const jz_str *node_id, const jazz_engine_QueryProgramRequest *request,
                                                                jz_btree *visited, jazz_engine_CorrelatedPathPlan *out) {
    const jz_btree *nodes = &request->input.shape.nodes;
    const jazz_engine_RowSetExpr *node = jz_btree_get(nodes, node_id);
    if (!node) {
        JE_DBG(d, jazz_engine__node_id_debug, node_id);
        jazz_engine_UrErr e = jazz_engine__ur_op(jz_format("row-set node %.*s is missing", JE_STR_ARG(d)));
        jz_str_drop(&d);
        return e;
    }
    visited_insert(visited, node_id);
    jazz_engine_UrErr err;
    switch (node->tag) {
    case JAZZ_ENGINE_EXPR_CORRELATED_PATH_PROJECTION: {
        memset(out, 0, sizeof *out);
        JE_UR_TRY(JE_PLAN(analyze_linear_root)(&node->u.correlated.input, request, visited, &out->parent));
        err = JE_PLAN(analyze_correlated_child_subplan)(&node->u.correlated.child_input, &node->u.correlated.path, nodes, visited, &out->child);
        if (err) { JE_PLAN(LinearCurrentRoot__drop)(&out->parent); return err; }
        JE_IN(ProgramPathId__Clone__clone)(&out->path, &node->u.correlated.path);
        pred_clone(&out->correlation, &node->u.correlated.correlation);
        out->requirement = node->u.correlated.requirement;
        err = JE_PLAN(collect_correlated_paths)(&node->u.correlated.path.child, NULL, nodes, visited, &out->nested);
        if (err) { JE_PLAN(CorrelatedPathPlan__drop)(out); return err; }
        return NULL;
    }
    case JAZZ_ENGINE_EXPR_ORDER_BY:
        JE_UR_TRY(JE_PLAN(analyze_correlated_path_root)(&node->u.order_by.input, request, visited, out));
        JZ_VEC_PUSH(out->output_steps, order_step_from(&node->u.order_by.keys));
        return NULL;
    case JAZZ_ENGINE_EXPR_SLICE:
        JE_UR_TRY(JE_PLAN(analyze_correlated_path_root)(&node->u.slice.input, request, visited, out));
        JZ_VEC_PUSH(out->output_steps, slice_step_from(node));
        return NULL;
    case JAZZ_ENGINE_EXPR_PROJECT: {
        JE_UR_TRY(JE_PLAN(analyze_correlated_path_root)(&node->u.project.input, request, visited, out));
        jazz_engine_LinearStep s;
        memset(&s, 0, sizeof s);
        s.tag = JAZZ_ENGINE_STEP_PROJECT;
        jz_vec_clone(&s.u.project, &node->u.project.columns, sizeof(jazz_engine_RowProjection), jazz_engine__projection_clone_v);
        JZ_VEC_PUSH(out->output_steps, s);
        return NULL;
    }
    default:
        return JE_UR_OP("root is not a correlated path plan");
    }
}

static void strset_extend_move(jz_btree *dst, jz_btree *src) {
    jz_str k;
    while (jz_btree_pop_first(src, &k, NULL)) {
        if (jz_btree_contains(dst, &k)) jz_str_drop(&k);
        else jz_btree_insert(dst, &k, NULL, NULL);
    }
    jz_btree_drop(src);
}

/* Collects the correlated paths owned by `owner`, except the one into `excluded_child`. */
static jazz_engine_UrErr JE_PLAN(collect_correlated_paths)(const jazz_engine_SourceId *owner, const jazz_engine_SourceId *excluded_child,
                                                            const jz_btree *nodes, jz_btree *visited, jazz_engine_CorrelatedPathPlanVec *out) {
    memset(out, 0, sizeof *out);
    jz_str *node_id; jazz_engine_RowSetExpr *node;
    JZ_BTREE_FOR(nodes, node_id, node) {
        if (node->tag != JAZZ_ENGINE_EXPR_CORRELATED_PATH_PROJECTION) continue;
        const jazz_engine_ProgramPathId *path = &node->u.correlated.path;
        if (!sid_eq(&path->owner, owner) || (excluded_child && sid_eq(&path->child, excluded_child))) continue;
        visited_insert(visited, node_id);
        jazz_engine_CorrelatedPathPlan p;
        memset(&p, 0, sizeof p);
        jz_btree parent_visited, child_visited;
        jz_btree_init(&parent_visited, &jazz_engine__STRSET_DESC);
        jazz_engine_UrErr err = JE_PLAN(analyze_linear_subplan)(&node->u.correlated.input, nodes, &parent_visited, &p.parent);
        if (err) { jz_btree_drop(&parent_visited); goto fail_with; }
        strset_extend_move(visited, &parent_visited);
        jz_btree_init(&child_visited, &jazz_engine__STRSET_DESC);
        err = JE_PLAN(analyze_correlated_child_subplan)(&node->u.correlated.child_input, path, nodes, &child_visited, &p.child);
        if (err) { jz_btree_drop(&child_visited); JE_PLAN(LinearCurrentRoot__drop)(&p.parent); goto fail_with; }
        strset_extend_move(visited, &child_visited);
        JE_IN(ProgramPathId__Clone__clone)(&p.path, path);
        pred_clone(&p.correlation, &node->u.correlated.correlation);
        p.requirement = node->u.correlated.requirement;
        err = JE_PLAN(collect_correlated_paths)(&path->child, NULL, nodes, visited, &p.nested);
        if (err) { JE_PLAN(CorrelatedPathPlan__drop)(&p); goto fail_with; }
        JZ_VEC_PUSH(*out, p);
        continue;
    fail_with:
        jz_vec_drop(out, sizeof(jazz_engine_CorrelatedPathPlan), cpp_drop_v);
        return err;
    }
    return NULL;
}

static jazz_engine_UrErr JE_PLAN(analyze_correlated_child_subplan)(const jz_str *child_input, const jazz_engine_ProgramPathId *path,
                                                                    const jz_btree *nodes, jz_btree *visited, jazz_engine_LinearCurrentRoot *out) {
    const jazz_engine_RowSetExpr *n = jz_btree_get(nodes, child_input);
    if (n && n->tag == JAZZ_ENGINE_EXPR_CORRELATED_PATH_PROJECTION && sid_eq(&n->u.correlated.path.owner, &path->child))
        return JE_PLAN(analyze_linear_subplan)(&n->u.correlated.input, nodes, visited, out);
    return JE_PLAN(analyze_linear_subplan)(child_input, nodes, visited, out);
}

static jazz_engine_UrErr first_order_gap(const jazz_engine_LinearStepVec *steps) {
    jazz_engine_UnsupportedReasonVec gaps = {0};
    JE_PLAN(validate_step_order)(steps, &gaps);
    jazz_engine_UrErr err = NULL;
    if (gaps.len) {
        err = jazz_engine__ur_box(gaps.ptr[0]);
        for (u32 i = 1; i < gaps.len; i++) JE_RES(UnsupportedReason__drop)(&gaps.ptr[i]);
    }
    jz_vec_drop(&gaps, sizeof(jazz_engine_UnsupportedReason), NULL);
    return err;
}

static jazz_engine_UrErr JE_PLAN(analyze_linear_root)(const jz_str *node_id, const jazz_engine_QueryProgramRequest *request, jz_btree *visited,
                                                       jazz_engine_LinearCurrentRoot *out) {
    return JE_PLAN(analyze_linear_subplan)(node_id, &request->input.shape.nodes, visited, out);
}

static jazz_engine_UrErr JE_PLAN(analyze_linear_subplan)(const jz_str *node_id, const jz_btree *nodes, jz_btree *visited,
                                                          jazz_engine_LinearCurrentRoot *out) {
    jazz_engine_LinearStepVec steps = {0};
    JE_UR_TRY(JE_PLAN(analyze_current_node)(node_id, nodes, visited, &out->root, &steps));
    jazz_engine_UrErr gap = first_order_gap(&steps);
    if (gap) {
        JE_PLAN(LinearRoot__drop)(&out->root);
        jazz_engine__steps_drop(&steps);
        return gap;
    }
    out->steps = steps;
    return NULL;
}

static jazz_engine_UrErr JE_PLAN(validate_result_source)(const jazz_engine_QueryProgramRequest *request, const jazz_engine_SourceId *source) {
    const jazz_engine_ResultId *r = &request->input.shape.result;
    if (r->tag == JAZZ_ENGINE_RESULT_REAL_ROW && r->u.real_row.row.tag == JAZZ_ENGINE_RRR_SOURCE && sid_eq(&r->u.real_row.row.source, source))
        return NULL;
    return JE_UR_OP("result must be the root source row");
}

/* ---------------------------------------------------------------- fragments */
void jazz_engine__plan_fragments_drop(jazz_engine_PlanFragments *f) {
    jz_vec_drop(&f->linears, sizeof(jazz_engine_LinearFragment), NULL);
    jz_vec_drop(&f->correlations, sizeof(void *), NULL);
    jz_vec_drop(&f->recursives, sizeof(void *), NULL);
}
static void JE_PLAN(collect_linear_fragments)(const jazz_engine_LinearCurrentRoot *linear, jazz_engine_PlanFragments *f);
static void JE_PLAN(collect_union_fragments)(const jazz_engine_UnionPlan *u, jazz_engine_PlanFragments *f);
static void JE_PLAN(collect_recursive_fragments)(const jazz_engine_RecursiveRelationPlan *r, jazz_engine_PlanFragments *f);
static void JE_PLAN(collect_step_relation_fragments)(const jazz_engine_LinearStepVec *steps, jazz_engine_PlanFragments *f);

static void JE_PLAN(collect_correlated_path_fragments)(const jazz_engine_CorrelatedPathPlan *path, jazz_engine_PlanFragments *f) {
    JE_PLAN(collect_linear_fragments)(&path->parent, f);
    JE_PLAN(collect_linear_fragments)(&path->child, f);
    JZ_VEC_PUSH(f->correlations, (const jazz_engine_PredicateExpr *)&path->correlation);
    if (path->output_steps.len) {
        jazz_engine_LinearFragment frag = {NULL, path->output_steps.ptr, path->output_steps.len};
        JZ_VEC_PUSH(f->linears, frag);
        JE_PLAN(collect_step_relation_fragments)(&path->output_steps, f);
    }
    JZ_VEC_FOR(const jazz_engine_CorrelatedPathPlan, s, path->siblings) JE_PLAN(collect_correlated_path_fragments)(s, f);
    JZ_VEC_FOR(const jazz_engine_CorrelatedPathPlan, s, path->nested) JE_PLAN(collect_correlated_path_fragments)(s, f);
}
static void JE_PLAN(collect_relation_fragments)(const jazz_engine_RelationInputPlan *plan, jazz_engine_PlanFragments *f) {
    switch (plan->tag) {
    case JAZZ_ENGINE_RIP_LINEAR: JE_PLAN(collect_linear_fragments)(&plan->u.linear, f); break;
    case JAZZ_ENGINE_RIP_UNION: JE_PLAN(collect_union_fragments)(&plan->u.union_, f); break;
    case JAZZ_ENGINE_RIP_RECURSIVE: JE_PLAN(collect_recursive_fragments)(&plan->u.recursive, f); break;
    }
}
static void JE_PLAN(collect_union_fragments)(const jazz_engine_UnionPlan *u, jazz_engine_PlanFragments *f) {
    JZ_VEC_FOR(const jazz_engine_UnionBranchPlan, b, u->branches) JE_PLAN(collect_relation_fragments)(&b->plan, f);
}
static void JE_PLAN(collect_recursive_fragments)(const jazz_engine_RecursiveRelationPlan *r, jazz_engine_PlanFragments *f) {
    JZ_VEC_PUSH(f->recursives, r);
    JE_PLAN(collect_linear_fragments)(&r->seed, f);
    JE_PLAN(collect_linear_fragments)(&r->step, f);
}
static void JE_PLAN(collect_linear_fragments)(const jazz_engine_LinearCurrentRoot *linear, jazz_engine_PlanFragments *f) {
    jazz_engine_LinearFragment frag = {&linear->root, linear->steps.ptr, linear->steps.len};
    JZ_VEC_PUSH(f->linears, frag);
    JE_PLAN(collect_step_relation_fragments)(&linear->steps, f);
}
static void JE_PLAN(collect_step_relation_fragments)(const jazz_engine_LinearStepVec *steps, jazz_engine_PlanFragments *f) {
    JZ_VEC_FOR(const jazz_engine_LinearStep, s, *steps)
        if (s->tag == JAZZ_ENGINE_STEP_JOIN) JE_PLAN(collect_relation_fragments)(s->u.join.right, f);
}
static void JE_PLAN(collect_analyzed_fragments)(const jazz_engine_AnalyzedQueryPlan *plan, jazz_engine_PlanFragments *f) {
    switch (plan->tag) {
    case JAZZ_ENGINE_AQP_LINEAR: JE_PLAN(collect_linear_fragments)(&plan->u.linear, f); break;
    case JAZZ_ENGINE_AQP_UNION: JE_PLAN(collect_union_fragments)(&plan->u.union_, f); break;
    case JAZZ_ENGINE_AQP_CORRELATED_PATH: JE_PLAN(collect_correlated_path_fragments)(&plan->u.correlated, f); break;
    case JAZZ_ENGINE_AQP_RECURSIVE_RELATION: JE_PLAN(collect_recursive_fragments)(&plan->u.recursive, f); break;
    }
}
void JE_PLAN(collect_plan_fragments)(const jazz_engine_AnalyzedQueryPlan *plan, jazz_engine_PlanFragments *out) {
    memset(out, 0, sizeof *out);
    JE_PLAN(collect_analyzed_fragments)(plan, out);
}

/* ---------------------------------------------------------------- sources */
static void sidset_insert_clone(jz_btree *set, const jazz_engine_SourceId *s) {
    if (jz_btree_contains(set, s)) return;
    jazz_engine_SourceId k = jazz_engine__sid_cloned(s);
    jz_btree_insert(set, &k, NULL, NULL);
}
static void JE_PLAN(analyzed_plan_sources)(const jazz_engine_AnalyzedQueryPlan *plan, jz_btree *out) {
    jz_btree_init(out, &jazz_engine__SOURCE_ID_SET_DESC);
    jazz_engine_PlanFragments f;
    JE_PLAN(collect_plan_fragments)(plan, &f);
    JZ_VEC_FOR(const jazz_engine_LinearFragment, frag, f.linears) {
        if (!frag->root) continue;
        const jazz_engine_SourceId *s = JE_PLAN(LinearRoot__source)(frag->root);
        if (s) sidset_insert_clone(out, s);
    }
    jazz_engine__plan_fragments_drop(&f);
}

void JE_PLAN(program_sources)(const jazz_engine_QueryProgramRequest *request, const jazz_engine_AnalyzedQueryPlan *plan, jz_btree *out) {
    JE_PLAN(analyzed_plan_sources)(plan, out);
    jazz_engine_SourceId *s; void *unused;
    JZ_BTREE_FOR(&request->input.shape.auxiliary_sources, s, unused) sidset_insert_clone(out, s);
}

const jz_btree_desc jazz_engine__SOURCE_ID_U8_MAP_DESC = {
    sizeof(jazz_engine_SourceId), sizeof(u8), jazz_engine__sid_cmp_v, jazz_engine__sid_drop_v, NULL
};

void JE_PLAN(source_visibilities)(const jazz_engine_AnalyzedQueryPlan *plan, jz_btree *out) {
    jz_btree_init(out, &jazz_engine__SOURCE_ID_U8_MAP_DESC);
    jazz_engine_PlanFragments f;
    JE_PLAN(collect_plan_fragments)(plan, &f);
    JZ_VEC_FOR(const jazz_engine_LinearFragment, frag, f.linears) {
        if (!frag->root || frag->root->tag != JAZZ_ENGINE_LROOT_SOURCE) continue;
        u8 *entry = jz_btree_get(out, &frag->root->source);
        if (!entry) {
            jazz_engine_SourceId k = jazz_engine__sid_cloned(&frag->root->source);
            u8 v = JAZZ_ENGINE_VIS_VISIBLE;
            jz_btree_insert(out, &k, &v, NULL);
            entry = jz_btree_get(out, &frag->root->source);
        }
        if (frag->root->visibility > *entry) *entry = frag->root->visibility;
    }
    jazz_engine__plan_fragments_drop(&f);
}

bool JE_PLAN(source_current_tier)(const jazz_engine_QueryProgramRequest *request, const jazz_engine_SourceId *source, u8 *tier) {
    const jazz_engine_SourceExpr *e = jz_btree_get(&request->reads.primary.sources, source);
    if (!e) return false;
    return JE_READ(SourceExpr__current_tier)(e, tier);
}

static const jazz_engine_SchemaProjection *JE_PLAN(supported_current_storage_projection)(const jazz_engine_SourceExpr *source) {
    if (!source) return NULL;
    switch (source->tag) {
    case JAZZ_ENGINE_SRC_VISIBLE_CURRENT:
    case JAZZ_ENGINE_SRC_BRANCH_VIEW:
    case JAZZ_ENGINE_SRC_SETTLED_BINDING_VIEW:
        return &source->projection;
    case JAZZ_ENGINE_SRC_HISTORY_CUT:
    case JAZZ_ENGINE_SRC_SNAPSHOT_REF:
        return source->data.tag == JAZZ_ENGINE_DATA_CURRENT ? &source->projection : NULL;
    case JAZZ_ENGINE_SRC_WITH_OVERLAYS: {
        const jazz_engine_OverlayRefVec *ov = &source->overlays;
        bool pending_local_over_settled = ov->len == 1 && ov->ptr[0].tag == JAZZ_ENGINE_OVERLAY_PENDING_LOCAL
                                          && source->input->tag == JAZZ_ENGINE_SRC_SETTLED_BINDING_VIEW;
        bool all_open = true;
        JZ_VEC_FOR(const jazz_engine_OverlayRef, o, *ov) if (o->tag != JAZZ_ENGINE_OVERLAY_OPEN_TRANSACTION) { all_open = false; break; }
        if (pending_local_over_settled || all_open) return JE_PLAN(supported_current_storage_projection)(source->input);
        return NULL;
    }
    default:
        return NULL;
    }
}

/* ---------------------------------------------------------------- node analysis */
static jazz_engine_UrErr node_missing(const jz_str *node_id) {
    JE_DBG(d, jazz_engine__node_id_debug, node_id);
    jazz_engine_UrErr e = jazz_engine__ur_op(jz_format("row-set node %.*s is missing", JE_STR_ARG(d)));
    jz_str_drop(&d);
    return e;
}

/* unsupported_marker_message: Some(text) when keys == [Literal(utf8 bytes)] */
static bool JE_PLAN(unsupported_marker_message)(const jazz_engine_NvrVec *keys, jz_str *out) {
    if (keys->len != 1 || keys->ptr[0].tag != JAZZ_ENGINE_NVR_LITERAL) return false;
    const jz_bytes *b = &keys->ptr[0].u.literal;
    if (!jz_utf8_valid(b->ptr, b->len)) return false;
    *out = jz_str_from_slice(jz_slice_of(b->ptr, b->len));
    return true;
}

static jazz_engine_UrErr JE_PLAN(analyze_current_node)(const jz_str *node_id, const jz_btree *nodes, jz_btree *visited,
                                                        jazz_engine_LinearRoot *root, jazz_engine_LinearStepVec *steps) {
    visited_insert(visited, node_id);
    const jazz_engine_RowSetExpr *node = jz_btree_get(nodes, node_id);
    if (!node) return node_missing(node_id);
    memset(root, 0, sizeof *root);
    jazz_engine_LinearStep step;
    memset(&step, 0, sizeof step);
    switch (node->tag) {
    case JAZZ_ENGINE_EXPR_SOURCE:
        root->tag = JAZZ_ENGINE_LROOT_SOURCE;
        sid_clone(&root->source, &node->u.source.source);
        root->visibility = node->u.source.visibility;
        memset(steps, 0, sizeof *steps);
        return NULL;
    case JAZZ_ENGINE_EXPR_VALUE_SOURCE:
        root->tag = JAZZ_ENGINE_LROOT_VALUE;
        root->shape = jz_str_clone(&node->u.value_source.shape);
        jz_vec_clone(&root->columns, &node->u.value_source.columns, sizeof(jazz_engine_ValueSourceColumn), jazz_engine__vsc_clone_v);
        root->mode = node->u.value_source.mode;
        memset(steps, 0, sizeof *steps);
        return NULL;
    case JAZZ_ENGINE_EXPR_FRONTIER_SOURCE:
        root->tag = JAZZ_ENGINE_LROOT_FRONTIER;
        root->frontier = jz_str_clone(&node->u.frontier_source.frontier);
        jz_vec_clone(&root->columns, &node->u.frontier_source.columns, sizeof(jazz_engine_ValueSourceColumn), jazz_engine__vsc_clone_v);
        memset(steps, 0, sizeof *steps);
        return NULL;
    case JAZZ_ENGINE_EXPR_FILTER:
        JE_UR_TRY(JE_PLAN(analyze_current_node)(&node->u.filter.input, nodes, visited, root, steps));
        step.tag = JAZZ_ENGINE_STEP_FILTER;
        pred_clone(&step.u.filter, &node->u.filter.predicate);
        JZ_VEC_PUSH(*steps, step);
        return NULL;
    case JAZZ_ENGINE_EXPR_ORDER_BY:
        JE_UR_TRY(JE_PLAN(analyze_current_node)(&node->u.order_by.input, nodes, visited, root, steps));
        JZ_VEC_PUSH(*steps, order_step_from(&node->u.order_by.keys));
        return NULL;
    case JAZZ_ENGINE_EXPR_SLICE:
        JE_UR_TRY(JE_PLAN(analyze_current_node)(&node->u.slice.input, nodes, visited, root, steps));
        JZ_VEC_PUSH(*steps, slice_step_from(node));
        return NULL;
    case JAZZ_ENGINE_EXPR_JOIN: {
        JE_UR_TRY(JE_PLAN(analyze_current_node)(&node->u.join.left, nodes, visited, root, steps));
        jazz_engine_RelationInputPlan *right = JZ_NEW(jazz_engine_RelationInputPlan);
        jazz_engine_UrErr err = JE_PLAN(analyze_relation_input_node)(&node->u.join.right, nodes, visited, right);
        if (err) {
            jz_free(right);
            JE_PLAN(LinearRoot__drop)(root);
            jazz_engine__steps_drop(steps);
            return err;
        }
        step.tag = JAZZ_ENGINE_STEP_JOIN;
        step.u.join.right = right;
        step.u.join.mode = node->u.join.mode;
        pred_clone(&step.u.join.on, &node->u.join.on);
        JZ_VEC_PUSH(*steps, step);
        return NULL;
    }
    case JAZZ_ENGINE_EXPR_PROJECT:
        JE_UR_TRY(JE_PLAN(analyze_current_node)(&node->u.project.input, nodes, visited, root, steps));
        step.tag = JAZZ_ENGINE_STEP_PROJECT;
        jz_vec_clone(&step.u.project, &node->u.project.columns, sizeof(jazz_engine_RowProjection), jazz_engine__projection_clone_v);
        JZ_VEC_PUSH(*steps, step);
        return NULL;
    case JAZZ_ENGINE_EXPR_RECURSIVE_RELATION:
        return JE_UR_OP("recursive relation row-set nodes are not lowered yet");
    case JAZZ_ENGINE_EXPR_UNION:
        return JE_UR_OP("union row-set nodes are not lowered yet");
    case JAZZ_ENGINE_EXPR_DISTINCT: {
        jz_str message;
        if (!JE_PLAN(unsupported_marker_message)(&node->u.distinct.keys, &message)) message = jz_str_from("distinct row-set nodes are not lowered yet");
        return jazz_engine__ur_op(message);
    }
    case JAZZ_ENGINE_EXPR_CORRELATED_PATH_PROJECTION:
        return JE_PLAN(analyze_current_node)(&node->u.correlated.input, nodes, visited, root, steps);
    case JAZZ_ENGINE_EXPR_AGGREGATE:
        JE_UR_TRY(JE_PLAN(analyze_current_node)(&node->u.aggregate.input, nodes, visited, root, steps));
        step.tag = JAZZ_ENGINE_STEP_AGGREGATE;
        jazz_engine__nvrvec_clone(&step.u.aggregate.group_by, &node->u.aggregate.group_by);
        jz_vec_clone(&step.u.aggregate.outputs, &node->u.aggregate.outputs, sizeof(jazz_engine_AggregateExpr), jazz_engine__aggregate_clone_v);
        JZ_VEC_PUSH(*steps, step);
        return NULL;
    }
    JZ_UNREACHABLE();
}

jazz_engine_UrErr JE_PLAN(analyze_relation_input_node)(const jz_str *node_id, const jz_btree *nodes, jz_btree *visited,
                                                       jazz_engine_RelationInputPlan *out) {
    JE_UR_TRY(JE_PLAN(validate_row_set_nesting)(node_id, nodes));
    const jazz_engine_RowSetExpr *node = jz_btree_get(nodes, node_id);
    if (!node) return node_missing(node_id);
    memset(out, 0, sizeof *out);
    switch (node->tag) {
    case JAZZ_ENGINE_EXPR_UNION:
        visited_insert(visited, node_id);
        JE_UR_TRY(JE_PLAN(analyze_union)(node->u.union_.inputs.ptr, node->u.union_.inputs.len, nodes, visited, &out->u.union_));
        out->tag = JAZZ_ENGINE_RIP_UNION;
        return NULL;
    case JAZZ_ENGINE_EXPR_RECURSIVE_RELATION: {
        visited_insert(visited, node_id);
        jazz_engine_RecursiveRelationPlan *r = &out->u.recursive;
        JE_UR_TRY(JE_PLAN(analyze_linear_subplan)(&node->u.recursive.seed, nodes, visited, &r->seed));
        jazz_engine_UrErr err = JE_PLAN(analyze_linear_subplan)(&node->u.recursive.step, nodes, visited, &r->step);
        if (err) { JE_PLAN(LinearCurrentRoot__drop)(&r->seed); return err; }
        r->frontier = jz_str_clone(&node->u.recursive.frontier);
        nvr_clone(&r->frontier_key, &node->u.recursive.frontier_key);
        jazz_engine__nvrvec_clone(&r->dedupe_keys, &node->u.recursive.dedupe_keys);
        r->bound = node->u.recursive.bound;
        out->tag = JAZZ_ENGINE_RIP_RECURSIVE;
        return NULL;
    }
    default: {
        jazz_engine_LinearCurrentRoot linear;
        JE_UR_TRY(JE_PLAN(analyze_linear_subplan)(node_id, nodes, visited, &linear));
        jazz_engine_UrErr err = JE_PLAN(validate_join_relation)(&linear);
        if (err) { JE_PLAN(LinearCurrentRoot__drop)(&linear); return err; }
        out->tag = JAZZ_ENGINE_RIP_LINEAR;
        out->u.linear = linear;
        return NULL;
    }
    }
}

static jazz_engine_UrErr JE_PLAN(analyze_union)(const jazz_engine_UnionInput *inputs, u32 n, const jz_btree *nodes, jz_btree *visited,
                                                 jazz_engine_UnionPlan *out) {
    memset(out, 0, sizeof *out);
    if (n == 0) return JE_UR_OP("union row-set nodes require at least one input");
    jz_btree labels;
    jz_btree_init(&labels, &jazz_engine__STRSET_DESC);
    jazz_engine_UrErr err = NULL;
    for (u32 i = 0; i < n; i++) {
        const jazz_engine_UnionInput *input = &inputs[i];
        if (input->label.len == 0 || memchr(input->label.ptr, '\0', input->label.len)) {
            err = JE_UR_OP("union arm labels must be non-empty, NUL-free stable semantic identities");
            break;
        }
        if (jz_btree_contains(&labels, &input->label)) {
            JE_DBG(d, jazz_engine__fmt__str, &input->label);
            err = jazz_engine__ur_op(jz_format(
                "union arm label %.*s is duplicated; occurrence identity requires unique stable semantic arm labels", JE_STR_ARG(d)));
            jz_str_drop(&d);
            break;
        }
        jazz_engine__strset_insert_clone(&labels, &input->label);
        jazz_engine_UnionBranchPlan branch;
        err = JE_PLAN(analyze_relation_input_node)(&input->node, nodes, visited, &branch.plan);
        if (err) break;
        branch.label = jz_str_clone(&input->label);
        JZ_VEC_PUSH(out->branches, branch);
    }
    jz_btree_drop(&labels);
    if (err) JE_PLAN(UnionPlan__drop)(out);
    return err;
}

static jazz_engine_UrErr JE_PLAN(validate_join_relation)(const jazz_engine_LinearCurrentRoot *plan) {
    JZ_VEC_FOR(const jazz_engine_LinearStep, step, plan->steps) {
        switch (step->tag) {
        case JAZZ_ENGINE_STEP_FILTER:
        case JAZZ_ENGINE_STEP_JOIN:
        case JAZZ_ENGINE_STEP_PROJECT: break;
        default: return JE_UR_OP("join inputs do not support order/slice/aggregate operators yet");
        }
    }
    return NULL;
}

static void JE_PLAN(validate_step_order)(const jazz_engine_LinearStepVec *steps, jazz_engine_UnsupportedReasonVec *gaps) {
    bool seen_order = false, seen_slice = false, seen_aggregate = false;
    JZ_VEC_FOR(const jazz_engine_LinearStep, step, *steps) {
        switch (step->tag) {
        case JAZZ_ENGINE_STEP_FILTER:
        case JAZZ_ENGINE_STEP_JOIN:
            if (seen_order || seen_slice || seen_aggregate)
                push_operator_gap(gaps, "filters/joins after order/slice/aggregate are not lowered yet");
            break;
        case JAZZ_ENGINE_STEP_PROJECT:
            if (seen_aggregate) push_operator_gap(gaps, "projects after aggregate are not lowered yet");
            break;
        case JAZZ_ENGINE_STEP_ORDER_BY:
            if (seen_aggregate) push_operator_gap(gaps, "order/slice after aggregate is not lowered yet");
            else if (seen_slice) push_operator_gap(gaps, "order-by after slice is not lowered yet");
            else if (seen_order) push_operator_gap(gaps, "multiple order-by nodes are not lowered yet");
            else seen_order = true;
            break;
        case JAZZ_ENGINE_STEP_SLICE:
            if (seen_aggregate) {
                push_operator_gap(gaps, "order/slice after aggregate is not lowered yet");
                break;
            }
            if (seen_slice) push_operator_gap(gaps, "multiple slice nodes are not lowered yet");
            if (step->u.slice.has_rank_output) push_operator_gap(gaps, "slice rank outputs are not lowered yet");
            seen_slice = true;
            break;
        case JAZZ_ENGINE_STEP_AGGREGATE:
            if (seen_order || seen_slice) push_operator_gap(gaps, "aggregate over ordered/windowed input is not lowered yet");
            seen_aggregate = true;
            break;
        }
    }
}
