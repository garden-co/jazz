/* lowering/requirements.rs: source-field and metadata requirements derived from
 * analyzed plans (the contract between planning and source resolution). */
#include "../lowering.h"

static void smr_insert(jz_btree *set, jazz_engine_SourceMetadataRequirement r) {
    if (jz_btree_contains(set, &r)) { JE_RES(SourceMetadataRequirement__drop)(&r); return; }
    jz_btree_insert(set, &r, NULL, NULL);
}
static void smr_insert_simple(jz_btree *set, u8 tag) { smr_insert(set, jazz_engine__smr_simple(tag)); }
static void smr_insert_provenance(jz_btree *set, u8 field) {
    jazz_engine_SourceMetadataRequirement r = jazz_engine__smr_simple(JAZZ_ENGINE_SMR_PROVENANCE);
    r.provenance = field;
    smr_insert(set, r);
}

static void JE_REQ(add_required_app_field)(jazz_engine_SourceRequirements *requirements, jz_str field) {
    jazz_types_AuthorMetadataPath mp;
    if (jazz_types__ids__AuthorSubject__metadata_path(JZ_S(&field), &mp)) {
        jazz_engine_SourceMetadataRequirement r = jazz_engine__smr_simple(JAZZ_ENGINE_SMR_AUTHOR_PATH);
        r.author_path = field; /* moved; the Rust clone + drop collapse into a move */
        smr_insert(&requirements->metadata, r);
        smr_insert_provenance(&requirements->metadata, strcmp(mp.root, "$createdBy") == 0 ? JAZZ_ENGINE_PROV_CREATED_BY : JAZZ_ENGINE_PROV_UPDATED_BY);
        return;
    }
    switch (requirements->app_fields.tag) {
    case JAZZ_ENGINE_FREQ_NONE_:
        requirements->app_fields.tag = JAZZ_ENGINE_FREQ_FIELDS;
        jz_btree_init(&requirements->app_fields.fields, &jazz_engine__STRSET_DESC);
        jz_btree_insert(&requirements->app_fields.fields, &field, NULL, NULL);
        break;
    case JAZZ_ENGINE_FREQ_FIELDS:
        if (jz_btree_contains(&requirements->app_fields.fields, &field)) jz_str_drop(&field);
        else jz_btree_insert(&requirements->app_fields.fields, &field, NULL, NULL);
        break;
    default:
        jz_str_drop(&field);
        break;
    }
}

/* From<FieldProjection> for FieldRequirement (by clone) */
static jazz_engine_FieldRequirement field_requirement_from(const jazz_engine_FieldProjection *p) {
    jazz_engine_FieldRequirement r;
    memset(&r, 0, sizeof r);
    if (p->tag == JAZZ_ENGINE_FP_ALL) {
        r.tag = JAZZ_ENGINE_FREQ_ALL;
    } else {
        r.tag = JAZZ_ENGINE_FREQ_FIELDS;
        jazz_engine__strset_clone(&r.fields, &p->fields);
    }
    return r;
}

static void JE_REQ(merge_field_requirement)(jazz_engine_FieldRequirement *existing, jazz_engine_FieldRequirement incoming) {
    switch (incoming.tag) {
    case JAZZ_ENGINE_FREQ_NONE_: return;
    case JAZZ_ENGINE_FREQ_ALL:
        JE_RES(FieldRequirement__drop)(existing);
        existing->tag = JAZZ_ENGINE_FREQ_ALL;
        return;
    default:
        switch (existing->tag) {
        case JAZZ_ENGINE_FREQ_NONE_: *existing = incoming; return;
        case JAZZ_ENGINE_FREQ_ALL: JE_RES(FieldRequirement__drop)(&incoming); return;
        default: {
            jz_str k;
            while (jz_btree_pop_first(&incoming.fields, &k, NULL)) {
                if (jz_btree_contains(&existing->fields, &k)) jz_str_drop(&k);
                else jz_btree_insert(&existing->fields, &k, NULL, NULL);
            }
            jz_btree_drop(&incoming.fields);
            return;
        }
        }
    }
}

static jazz_engine_CapErr JE_REQ(collect_app_path_projection_requirements)(const jazz_engine_AppPathProjection *paths, u32 n, jz_btree *requirements) {
    for (u32 i = 0; i < n; i++) {
        const jazz_engine_AppPathProjection *path = &paths[i];
        jazz_engine_SourceRequirements *sr = jz_btree_get(requirements, &path->path.child);
        if (!sr) {
            JE_DBG(d, sid_debug, &path->path.child);
            jazz_engine_CapErr e = JE_RES(CapabilityReport__runtime)(jz_format("app projection path child source %.*s was not initialized", JE_STR_ARG(d)));
            jz_str_drop(&d);
            return e;
        }
        JE_REQ(merge_field_requirement)(&sr->app_fields, field_requirement_from(&path->fields));
        JE_CAP_TRY(JE_REQ(collect_app_path_projection_requirements)(path->children.ptr, path->children.len, requirements));
    }
    return NULL;
}

static jazz_engine_UrErr JE_REQ(collect_value_requirements)(const jazz_engine_NormalizedValueRef *value, const jazz_engine_SourceId *source,
                                                            jazz_engine_SourceRequirements *requirements) {
    switch (value->tag) {
    case JAZZ_ENGINE_NVR_SOURCE_FIELD:
        if (!sid_eq(&value->u.source_field.source, source)) return NULL;
        JE_REQ(add_required_app_field)(requirements, jz_str_clone(&value->u.source_field.field));
        break;
    case JAZZ_ENGINE_NVR_PROVENANCE:
        if (!sid_eq(&value->u.source, source)) return NULL;
        smr_insert_provenance(&requirements->metadata, value->sub);
        break;
    default: /* RowId / Param / Claim / Literal / TemplateLiteral / FrontierColumn: nothing to require */
        break;
    }
    return NULL;
}

static jazz_engine_UrErr JE_REQ(collect_predicate_requirements)(const jazz_engine_PredicateExpr *predicate, const jazz_engine_SourceId *source,
                                                                jazz_engine_SourceRequirements *requirements) {
    switch (predicate->tag) {
    case JAZZ_ENGINE_PRED_TRUE:
    case JAZZ_ENGINE_PRED_FALSE: return NULL;
    case JAZZ_ENGINE_PRED_COMPARE:
    case JAZZ_ENGINE_PRED_ARRAY_CONTAINS:
    case JAZZ_ENGINE_PRED_TEXT_CONTAINS:
        JE_UR_TRY(JE_REQ(collect_value_requirements)(&predicate->u.pair.left, source, requirements));
        return JE_REQ(collect_value_requirements)(&predicate->u.pair.right, source, requirements);
    case JAZZ_ENGINE_PRED_IN:
        JE_UR_TRY(JE_REQ(collect_value_requirements)(&predicate->u.in.value, source, requirements));
        JZ_VEC_FOR(const jazz_engine_NormalizedValueRef, option, predicate->u.in.options)
            JE_UR_TRY(JE_REQ(collect_value_requirements)(option, source, requirements));
        return NULL;
    case JAZZ_ENGINE_PRED_IS_NULL:
    case JAZZ_ENGINE_PRED_IS_NOT_NULL:
        return JE_REQ(collect_value_requirements)(&predicate->u.value, source, requirements);
    case JAZZ_ENGINE_PRED_AND:
    case JAZZ_ENGINE_PRED_OR:
        JZ_VEC_FOR(const jazz_engine_PredicateExpr, p, predicate->u.children)
            JE_UR_TRY(JE_REQ(collect_predicate_requirements)(p, source, requirements));
        return NULL;
    case JAZZ_ENGINE_PRED_NOT:
        return JE_REQ(collect_predicate_requirements)(predicate->u.not_, source, requirements);
    case JAZZ_ENGINE_PRED_ENUM_MATCH:
        return JE_REQ(collect_value_requirements)(&predicate->u.enum_match.value, source, requirements);
    }
    return NULL;
}

static jazz_engine_UrErr step_requirements(const jazz_engine_LinearStep *step, const jazz_engine_SourceId *source,
                                           jazz_engine_SourceRequirements *requirements) {
    switch (step->tag) {
    case JAZZ_ENGINE_STEP_FILTER: return JE_REQ(collect_predicate_requirements)(&step->u.filter, source, requirements);
    case JAZZ_ENGINE_STEP_JOIN: return JE_REQ(collect_predicate_requirements)(&step->u.join.on, source, requirements);
    case JAZZ_ENGINE_STEP_PROJECT:
        JZ_VEC_FOR(const jazz_engine_RowProjection, c, step->u.project)
            JE_UR_TRY(JE_REQ(collect_value_requirements)(&c->value, source, requirements));
        return NULL;
    case JAZZ_ENGINE_STEP_ORDER_BY:
        JZ_VEC_FOR(const jazz_engine_OrderKey, k, step->u.order_by)
            JE_UR_TRY(JE_REQ(collect_value_requirements)(&k->value, source, requirements));
        return NULL;
    case JAZZ_ENGINE_STEP_SLICE:
        JZ_VEC_FOR(const jazz_engine_NormalizedValueRef, v, step->u.slice.partition_by)
            JE_UR_TRY(JE_REQ(collect_value_requirements)(v, source, requirements));
        JZ_VEC_FOR(const jazz_engine_NormalizedValueRef, v, step->u.slice.tie_breaker)
            JE_UR_TRY(JE_REQ(collect_value_requirements)(v, source, requirements));
        return NULL;
    case JAZZ_ENGINE_STEP_AGGREGATE:
        JZ_VEC_FOR(const jazz_engine_NormalizedValueRef, v, step->u.aggregate.group_by)
            JE_UR_TRY(JE_REQ(collect_value_requirements)(v, source, requirements));
        JZ_VEC_FOR(const jazz_engine_AggregateExpr, a, step->u.aggregate.outputs)
            if (a->has_input) JE_UR_TRY(JE_REQ(collect_value_requirements)(&a->input, source, requirements));
        return NULL;
    }
    return NULL;
}

static jazz_engine_CapErr explained_take(jazz_engine_UrErr gap, const char *capability) {
    return JE_RES(CapabilityReport__explained)(jazz_engine__ur_take(gap), jz_str_from(capability));
}

static jazz_engine_CapErr JE_REQ(collect_step_requirements)(const jazz_engine_LinearStep *step, const jazz_engine_SourceId *source,
                                                            jazz_engine_SourceRequirements *requirements) {
    jazz_engine_UrErr gap = step_requirements(step, source, requirements);
    return gap ? explained_take(gap, "operator source requirements are not lowered") : NULL;
}

static jazz_engine_CapErr JE_REQ(collect_predicate_requirements_for_all_sources)(const jazz_engine_PredicateExpr *predicate, jz_btree *requirements) {
    jazz_engine_SourceId *source; jazz_engine_SourceRequirements *sr;
    JZ_BTREE_FOR(requirements, source, sr) {
        jazz_engine_UrErr gap = JE_REQ(collect_predicate_requirements)(predicate, source, sr);
        if (gap) return explained_take(gap, "path correlation requirements are not lowered");
    }
    return NULL;
}

static jazz_engine_CapErr JE_REQ(collect_value_requirements_for_all_sources)(const jazz_engine_NormalizedValueRef *value, jz_btree *requirements) {
    jazz_engine_SourceId *source; jazz_engine_SourceRequirements *sr;
    JZ_BTREE_FOR(requirements, source, sr) {
        jazz_engine_UrErr gap = JE_REQ(collect_value_requirements)(value, source, sr);
        if (gap) return explained_take(gap, "relation key requirements are not lowered");
    }
    return NULL;
}

static bool frontier_local_key(const jazz_engine_NormalizedValueRef *v) {
    return v->tag == JAZZ_ENGINE_NVR_FRONTIER_COLUMN || (v->tag == JAZZ_ENGINE_NVR_ROW_ID && v->sub == JAZZ_ENGINE_ROWID_FRONTIER)
           || v->tag == JAZZ_ENGINE_NVR_PARAM || v->tag == JAZZ_ENGINE_NVR_LITERAL;
}

static jazz_engine_CapErr JE_REQ(collect_plan_requirements)(const jazz_engine_AnalyzedQueryPlan *plan, jz_btree *requirements) {
    jazz_engine_PlanFragments f;
    JE_PLAN(collect_plan_fragments)(plan, &f);
    jazz_engine_CapErr err = NULL;
    JZ_VEC_FOR(const jazz_engine_LinearFragment, frag, f.linears) {
        for (u32 i = 0; i < frag->nsteps; i++) {
            jazz_engine_SourceId *source; jazz_engine_SourceRequirements *sr;
            JZ_BTREE_FOR(requirements, source, sr) {
                err = JE_REQ(collect_step_requirements)(&frag->steps[i], source, sr);
                if (err) goto done;
            }
        }
    }
    for (u32 i = 0; i < f.correlations.len; i++) {
        err = JE_REQ(collect_predicate_requirements_for_all_sources)(f.correlations.ptr[i], requirements);
        if (err) goto done;
    }
    for (u32 i = 0; i < f.recursives.len; i++) {
        const jazz_engine_RecursiveRelationPlan *relation = f.recursives.ptr[i];
        if (!frontier_local_key(&relation->frontier_key)) {
            err = JE_REQ(collect_value_requirements_for_all_sources)(&relation->frontier_key, requirements);
            if (err) goto done;
        }
        JZ_VEC_FOR(const jazz_engine_NormalizedValueRef, key, relation->dedupe_keys) {
            if (frontier_local_key(key)) continue;
            err = JE_REQ(collect_value_requirements_for_all_sources)(key, requirements);
            if (err) goto done;
        }
    }
done:
    jazz_engine__plan_fragments_drop(&f);
    return err;
}

static jazz_engine_SourceRequirements *root_requirements(jz_btree *requirements, const jazz_engine_AnalyzedQueryPlan *plan) {
    jazz_engine_SourceRequirements *r = jz_btree_get(requirements, JE_PLAN(AnalyzedQueryPlan__root_source)(plan));
    if (!r) jz_panic("root source requirements were initialized");
    return r;
}

static bool provenance_of(const jz_str *field, u8 *out) {
    if (jz_str_eq_cstr(field, "$createdAt")) *out = JAZZ_ENGINE_PROV_CREATED_AT;
    else if (jz_str_eq_cstr(field, "$createdBy")) *out = JAZZ_ENGINE_PROV_CREATED_BY;
    else if (jz_str_eq_cstr(field, "$updatedAt")) *out = JAZZ_ENGINE_PROV_UPDATED_AT;
    else if (jz_str_eq_cstr(field, "$updatedBy")) *out = JAZZ_ENGINE_PROV_UPDATED_BY;
    else return false;
    return true;
}

static jazz_engine_CapErr requirements_body(const jazz_engine_QueryProgramRequest *request, const jazz_engine_AnalyzedQueryPlan *plan,
                                            jz_btree *requirements) {
    const jazz_engine_RowSetOutputRequest *output = &request->output;

    /* Every closure hop consumes its parent reference as an executable join key. */
    JZ_VEC_FOR(const jazz_engine_ClosurePath, path, request->input.shape.closure_paths) {
        jazz_engine_SegmentRefVec segments;
        JE_CLO(closure_path_segments)(path, &segments);
        for (u32 i = 0; i < segments.len; i++) {
            const jazz_engine_ClosurePathSegment *segment = segments.ptr[i];
            jazz_engine_SourceRequirements *sr = jz_btree_get(requirements, &segment->parent);
            if (!sr) {
                jz_vec_drop(&segments, sizeof(void *), NULL);
                JE_DBG(d, sid_debug, &segment->parent);
                jazz_engine_CapErr e = JE_RES(CapabilityReport__runtime)(jz_format("closure parent source %.*s was not initialized", JE_STR_ARG(d)));
                jz_str_drop(&d);
                return e;
            }
            JE_REQ(add_required_app_field)(sr, jz_str_clone(&segment->source_field));
        }
        jz_vec_drop(&segments, sizeof(void *), NULL);
    }

    /* A flat output carries an occurrence-addressed tuple: keep version metadata. */
    jazz_engine_TypedOutputFieldVec flat;
    JE_TERM(flat_join_payload_fields)(plan, &flat);
    bool has_flat = flat.len != 0;
    jz_vec_drop(&flat, sizeof(jazz_engine_TypedOutputField), jazz_engine__tof_drop_v);
    if (has_flat) {
        jazz_engine_SourceId *k; jazz_engine_SourceRequirements *sr;
        JZ_BTREE_FOR(requirements, k, sr) smr_insert_simple(&sr->metadata, JAZZ_ENGINE_SMR_VERSION_WITNESSES);
    }

    if (output->has_app_rows) {
        const jazz_engine_AppRowOutputRequest *app_rows = &output->app_rows;
        if (!(plan->tag == JAZZ_ENGINE_AQP_LINEAR || plan->tag == JAZZ_ENGINE_AQP_UNION || plan->tag == JAZZ_ENGINE_AQP_CORRELATED_PATH)) {
            jazz_engine_UnsupportedReason r = jazz_engine__ur_new(JAZZ_ENGINE_UR_OPERATOR);
            r.message = jz_str_from("app row materialization for recursive relation projections is not lowered yet");
            return JE_RES(CapabilityReport__explained)(r, jz_str_from("recursive relation app rows are not lowered"));
        }
        jazz_engine_SourceRequirements *rr = root_requirements(requirements, plan);
        const jazz_engine_PayloadProjection *proj = &app_rows->projection;
        bool projects_provenance = false;
        if (proj->tag == JAZZ_ENGINE_PP_TREE && proj->tree.fields.tag == JAZZ_ENGINE_FP_FIELDS) {
            jz_str *field; void *unused;
            JZ_BTREE_FOR(&proj->tree.fields.fields, field, unused) {
                u8 p;
                if (provenance_of(field, &p)) { projects_provenance = true; break; }
            }
        }
        if (app_rows->public_terminal && projects_provenance) {
            smr_insert_provenance(&rr->metadata, JAZZ_ENGINE_PROV_CREATED_AT);
            smr_insert_provenance(&rr->metadata, JAZZ_ENGINE_PROV_CREATED_BY);
            smr_insert_provenance(&rr->metadata, JAZZ_ENGINE_PROV_UPDATED_AT);
            smr_insert_provenance(&rr->metadata, JAZZ_ENGINE_PROV_UPDATED_BY);
        }
        jazz_engine_FieldRequirement incoming;
        memset(&incoming, 0, sizeof incoming);
        if (proj->tag == JAZZ_ENGINE_PP_TREE) incoming = field_requirement_from(&proj->tree.fields);
        else incoming.tag = JAZZ_ENGINE_FREQ_ALL; /* ShapeDefault / Relation */
        JE_REQ(merge_field_requirement)(&rr->app_fields, incoming);
        if (proj->tag == JAZZ_ENGINE_PP_TREE) {
            if (proj->tree.fields.tag == JAZZ_ENGINE_FP_FIELDS) {
                jz_str *field; void *unused;
                JZ_BTREE_FOR(&proj->tree.fields.fields, field, unused) {
                    u8 p;
                    if (provenance_of(field, &p)) smr_insert_provenance(&rr->metadata, p);
                }
            }
            JE_CAP_TRY(JE_REQ(collect_app_path_projection_requirements)(proj->tree.paths.ptr, proj->tree.paths.len, requirements));
        }
    }

    jazz_engine_ProgramFactKey *fact; void *unused;
    JZ_BTREE_FOR(&output->facts, fact, unused) {
        switch (fact->tag) {
        case JAZZ_ENGINE_FACT_AUTHORIZED_ROWS: break;
        case JAZZ_ENGINE_FACT_RESULT_MEMBERSHIP: {
            jazz_engine_SourceRequirements *rr = root_requirements(requirements, plan);
            smr_insert_simple(&rr->metadata, JAZZ_ENGINE_SMR_VERSION_WITNESSES);
            smr_insert_simple(&rr->metadata, JAZZ_ENGINE_SMR_SETTLE_POSITION);
            JZ_VEC_FOR(const jazz_engine_JoinContribution, c, request->input.shape.join_contributions) {
                jazz_engine_SourceRequirements *sr = jz_btree_get(requirements, &c->source);
                if (!sr) continue;
                smr_insert_simple(&sr->metadata, JAZZ_ENGINE_SMR_VERSION_WITNESSES);
                smr_insert_simple(&sr->metadata, JAZZ_ENGINE_SMR_SETTLE_POSITION);
            }
            break;
        }
        case JAZZ_ENGINE_FACT_VERSION_WITNESSES:
        case JAZZ_ENGINE_FACT_REPLACEMENT_WITNESSES: {
            /* A deletion is an ordinary row delta: no separate deleted-rows source. */
            jazz_engine_SourceId *k; jazz_engine_SourceRequirements *sr;
            JZ_BTREE_FOR(requirements, k, sr) {
                smr_insert_simple(&sr->metadata, JAZZ_ENGINE_SMR_VERSION_WITNESSES);
                smr_insert_simple(&sr->metadata, JAZZ_ENGINE_SMR_VERSION_PAYLOADS);
            }
            break;
        }
        case JAZZ_ENGINE_FACT_PROGRAM_SOURCE_COVERAGE:
            switch (fact->scope.tag) {
            case JAZZ_ENGINE_SCOPE_PROGRAM: {
                jazz_engine_SourceId *k; jazz_engine_SourceRequirements *sr;
                JZ_BTREE_FOR(requirements, k, sr) smr_insert_simple(&sr->metadata, JAZZ_ENGINE_SMR_COVERAGE);
                break;
            }
            case JAZZ_ENGINE_SCOPE_SOURCE: {
                jazz_engine_SourceRequirements *sr = jz_btree_get(requirements, &fact->scope.source);
                if (!sr) {
                    jazz_engine_UnsupportedReason r = jazz_engine__ur_new(JAZZ_ENGINE_UR_SOURCE);
                    r.source.tag = JAZZ_ENGINE_GAP_COVERAGE;
                    return jazz_engine__cap_single(r);
                }
                smr_insert_simple(&sr->metadata, JAZZ_ENGINE_SMR_COVERAGE);
                break;
            }
            default:
                smr_insert_simple(&root_requirements(requirements, plan)->metadata, JAZZ_ENGINE_SMR_COVERAGE);
                break;
            }
            break;
        case JAZZ_ENGINE_FACT_RELATION_EDGES:
        case JAZZ_ENGINE_FACT_PATH_CORRELATION_COVERAGE: {
            jazz_engine_SourceId *k; jazz_engine_SourceRequirements *sr;
            JZ_BTREE_FOR(requirements, k, sr) smr_insert_simple(&sr->metadata, JAZZ_ENGINE_SMR_VERSION_WITNESSES);
            break;
        }
        default: {
            jazz_engine_UnsupportedReason r = jazz_engine__ur_new(JAZZ_ENGINE_UR_OUTPUT);
            r.output = JZ_NEW(jazz_engine_ProgramFactKey);
            JE_OUT(ProgramFactKey__Clone__clone)(r.output, fact);
            return JE_RES(CapabilityReport__explained)(r, jz_str_from("requested fact is not lowered yet"));
        }
        }
    }

    return JE_REQ(collect_plan_requirements)(plan, requirements);
}

jazz_engine_CapErr JE_REQ(source_requirements)(const jazz_engine_QueryProgramRequest *request, const jazz_engine_AnalyzedQueryPlan *plan, jz_btree *out) {
    jz_btree_init(out, &jazz_engine__SOURCE_ID_REQ_MAP_DESC);
    jz_btree sources;
    JE_PLAN(program_sources)(request, plan, &sources);
    jazz_engine_SourceId k;
    while (jz_btree_pop_first(&sources, &k, NULL)) {
        jazz_engine_SourceRequirements r;
        JE_RES(SourceRequirements__Default__default)(&r);
        jz_btree_insert(out, &k, &r, NULL);
    }
    jz_btree_drop(&sources);
    jazz_engine_CapErr err = requirements_body(request, plan, out);
    if (err) jz_btree_drop(out);
    return err;
}
