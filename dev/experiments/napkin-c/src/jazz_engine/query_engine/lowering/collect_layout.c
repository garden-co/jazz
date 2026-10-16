/* lowering/collect_layout.rs: flat collector layouts for app-row association
 * trees, and the projections/builders that feed Groove CollectBy. */
#include "../lowering.h"

#define F(x) jazz_engine__query_engine__fields__##x
#define PF(x) groove__ivm__graph__ProjectField__##x
#define VT_DROP groove__records__values__ValueType__drop
#define VT_EQ groove__records__values__ValueType__PartialEq__eq

typedef JZ_VEC(const jazz_engine_CollectSlotLayout *) SlotRefVec;

/* ---------------------------------------------------------------- drops (terminals.rs types) */
void JE_TERM(CollectFlatField__drop)(jazz_engine_CollectFlatField *f) {
    jz_str_drop(&f->input);
    jz_str_drop(&f->output);
    VT_DROP(&f->value_type);
    VT_DROP(&f->output_value_type);
    jazz_engine__optstr_drop(&f->source_field);
    jazz_engine__optstr_drop(&f->source_public_name);
}
static void flat_field_drop_v(void *f) { JE_TERM(CollectFlatField__drop)(f); }
static void slot_drop_v(void *s) { JE_TERM(CollectSlotLayout__drop)(s); }
void JE_TERM(CollectSlotLayout__drop)(jazz_engine_CollectSlotLayout *s) {
    JE_IN(ProgramPathId__drop)(&s->path);
    jz_str_drop(&s->collection_field);
    jz_vec_drop(&s->fields, sizeof(jazz_engine_CollectFlatField), flat_field_drop_v);
    jz_str_drop(&s->row_id_input);
    jz_str_drop(&s->presence_input);
    jazz_engine__optstr_drop(&s->reference_array_input);
    jz_vec_drop(&s->order_cols, sizeof(groove_TopByOrder), groove__ivm__graph__TopByOrder__drop_v);
    jazz_engine__strvec_drop(&s->tie_cols);
    jz_vec_drop(&s->children, sizeof(jazz_engine_CollectSlotLayout), slot_drop_v);
}
void JE_TERM(CollectLayout__drop)(jazz_engine_CollectLayout *l) {
    jz_vec_drop(&l->root_fields, sizeof(jazz_engine_CollectFlatField), flat_field_drop_v);
    jazz_engine__strvec_drop(&l->root_occurrence_inputs);
    jz_vec_drop(&l->root_order_cols, sizeof(groove_TopByOrder), groove__ivm__graph__TopByOrder__drop_v);
    jazz_engine__strvec_drop(&l->root_tie_cols);
    jz_vec_drop(&l->slots, sizeof(jazz_engine_CollectSlotLayout), slot_drop_v);
}

/* lowering.rs public_root_field_name */
jz_str JE_LOW(public_root_field_name)(const jazz_engine_CollectFlatField *field) {
    /* Layout construction binds the public identity before collector fields are renamed. */
    return field->source_public_name.some ? jz_str_clone(&field->source_public_name.s) : jz_str_clone(&field->output);
}

/* ---------------------------------------------------------------- source descriptor lookups */
static bool resolved_source_field_index_by_name(const jazz_engine_ResolvedSource *source, jz_slice field, u32 *out) {
    u32 n;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(source->row_shape.descriptor, &n);
    for (u32 i = 0; i < n; i++) {
        if (fields[i].has_name && jazz_engine__str_eq_slice(&fields[i].name, field)) { *out = i; return true; }
    }
    return false;
}

static jazz_engine_OptStr resolved_source_field_name(const jazz_engine_ResolvedSource *source, jz_slice field) {
    jazz_engine_OptStr r = {0};
    u32 idx, n;
    if (!groove__records__RecordDescriptor__field_index(source->row_shape.descriptor, (const char *)field.ptr, field.len, &idx)) return r;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(source->row_shape.descriptor, &n);
    if (idx >= n || !fields[idx].has_name) return r;
    r.some = true;
    r.s = jz_str_clone(&fields[idx].name);
    return r;
}

static jazz_engine_OptStr resolved_source_public_name(const jazz_engine_ResolvedSource *source, jz_slice field) {
    jazz_engine_OptStr r = {0};
    u32 n;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(source->row_shape.descriptor, &n);
    for (u32 i = 0; i < n; i++) {
        if (fields[i].has_name && jazz_engine__str_eq_slice(&fields[i].name, field)) {
            jz_slice pub;
            if (F(descriptor_public_name)(&fields[i], &pub)) { r.some = true; r.s = jz_str_from_slice(pub); }
            return r;
        }
    }
    return r;
}

static bool is_magic_provenance(jz_slice f) {
    static const char *const names[] = {"$createdAt", "$createdBy", "$updatedAt", "$updatedBy"};
    for (u32 i = 0; i < 4; i++)
        if (f.len == strlen(names[i]) && memcmp(f.ptr, names[i], f.len) == 0) return true;
    return false;
}
/* "created_at" -> "$createdAt", ... ; NULL when not a provenance storage name */
static const char *provenance_public_name(jz_slice f) {
    static const char *const from[] = {"created_at", "created_by", "updated_at", "updated_by"};
    static const char *const to[] = {"$createdAt", "$createdBy", "$updatedAt", "$updatedBy"};
    for (u32 i = 0; i < 4; i++)
        if (f.len == strlen(from[i]) && memcmp(f.ptr, from[i], f.len) == 0) return to[i];
    return NULL;
}

static jz_str collect_projection_source_field(const jazz_engine_ResolvedSource *source, jz_slice field) {
    if (is_magic_provenance(field)) return jz_str_from_slice(field);
    jazz_engine_OptStr name = resolved_source_field_name(source, field);
    return name.some ? name.s : F(user_column_field)(field);
}

static jz_str collect_projection_output_field(jz_slice field) {
    if (is_magic_provenance(field)) return jz_str_from_slice(field);
    const char *p = provenance_public_name(field);
    if (p) return jz_str_from(p);
    /* The terminal's encoded record remains in the canonical current-row codec namespace. */
    return jz_str_from_slice(field);
}

static jz_str collect_nested_projection_output_field(const jazz_engine_ResolvedSource *source, jz_slice field) {
    if (is_magic_provenance(field)) return jz_str_from_slice(field);
    const char *p = provenance_public_name(field);
    if (p) return jz_str_from(p);
    /* Nested records are public tree payloads: user columns keep logical schema names. */
    jazz_engine_OptStr pub = resolved_source_public_name(source, field);
    return pub.some ? pub.s : jz_str_from_slice(field);
}

static void slots_drop(jazz_engine_CollectSlotLayoutVec *v) { jz_vec_drop(v, sizeof(jazz_engine_CollectSlotLayout), slot_drop_v); }
static void flat_fields_drop(jazz_engine_CollectFlatFieldVec *v) { jz_vec_drop(v, sizeof(jazz_engine_CollectFlatField), flat_field_drop_v); }

/* ---------------------------------------------------------------- output types */
groove_ValueType JE_CL(collect_unwrapped_output_type)(const jazz_engine_ResolvedSource *source, jz_slice source_field, const groove_ValueType *fallback) {
    if (jazz_engine__str_eq_slice(&source->row_shape.row_uuid_field, source_field)) return groove_vt_clone(fallback);
    jazz_engine_OptStr pub = resolved_source_public_name(source, source_field);
    jz_str logical = pub.some ? pub.s : jz_str_from_slice(source_field);
    const jazz_model_ColumnSchema *column = NULL;
    JZ_VEC_FOR(const jazz_model_ColumnSchema, c, source->table_schema->columns) {
        if (jz_str_eq(&c->name, &logical)) { column = c; break; }
    }
    jz_str_drop(&logical);
    if (!column) return groove_vt_clone(fallback);
    /* The collector unwraps the current-row presence cell, not the column's storage representation. */
    groove_ValueType unwrapped = fallback->tag == GROOVE_VT_NULLABLE ? groove_vt_clone(fallback->u.inner) : groove_vt_clone(fallback);
    /* Indirect JSON storage has only the presence wrapper; a nullable column's output must still admit NULL. */
    if (column->column_type.tag == GROOVE_VT_NULLABLE && unwrapped.tag != GROOVE_VT_NULLABLE)
        return groove_vt_boxed(GROOVE_VT_NULLABLE, unwrapped);
    return unwrapped;
}

/* A collector child input carries exactly one anchor wrapper around its logical output. */
groove_ValueType JE_CL(collect_slot_input_type)(const groove_ValueType *output_value_type) {
    return groove_vt_boxed(GROOVE_VT_NULLABLE, groove_vt_clone(output_value_type));
}

/* ---------------------------------------------------------------- layout construction */
static jazz_engine_CapErr collect_slot_layouts(const jazz_engine_AppPathProjection *paths, u32 npaths, const jz_btree *resolved_sources,
                                               u32 depth, u32 *next_slot, jazz_engine_CollectSlotLayoutVec *out);

jazz_engine_CapErr JE_CL(collect_layout)(const jazz_engine_AppProjectionTree *projection, const jazz_engine_AnalyzedQueryPlan *plan,
    const jazz_engine_ResolvedSource *root_source, const jz_btree *resolved_sources, jazz_engine_LoweringContext *request,
    const jz_btree *routing_param_fields, const jazz_engine_ParameterDomain *parameter_domain, const jz_btree *available_fields,
    jazz_engine_CollectLayout *out) {
    bool explicit_root_projection = projection->fields.tag == JAZZ_ENGINE_FP_FIELDS;
    jz_btree selected_root;
    jz_btree_init(&selected_root, &jazz_engine__STRSET_DESC);
    jazz_engine__strset_insert_clone(&selected_root, &root_source->row_shape.row_uuid_field);
    if (!explicit_root_projection) {
        /* Shape-default app rows are CurrentRows: keep magic provenance and current version fields. */
        jazz_engine_StrVec names = F(current_row_field_names)(root_source->table_schema);
        JZ_VEC_FOR(jz_str, n, names) jazz_engine__btree_set_add(&selected_root, n);
        jz_vec_drop(&names, sizeof(jz_str), NULL);
    } else {
        const jz_str *f; void *unit;
        JZ_BTREE_FOR(&projection->fields.fields, f, unit) {
            if (jz_str_eq_cstr(f, "id")) continue;
            jz_str s = collect_projection_source_field(root_source, JZ_S(f));
            jazz_engine__btree_set_add(&selected_root, &s);
        }
    }
    jazz_engine_CollectFlatFieldVec root_fields = {0};
    u32 nfields;
    const groove_DescriptorField *dfields = groove__records__RecordDescriptor__fields(root_source->row_shape.descriptor, &nfields);
    for (u32 i = 0; i < nfields; i++) {
        const groove_DescriptorField *field = &dfields[i];
        if (!field->has_name) continue;
        const jz_str *name = &field->name;
        bool is_row_id = jz_str_eq(name, &root_source->row_shape.row_uuid_field);
        jazz_engine_CollectFlatField cf;
        memset(&cf, 0, sizeof cf);
        cf.input = jz_format("__collect_root_%.*s", JE_STR_ARG(*name));
        cf.output = is_row_id ? jz_str_clone(name) : collect_projection_output_field(JZ_S(name));
        cf.value_type = groove_vt_clone(&field->value_type);
        cf.output_value_type = explicit_root_projection ? JE_CL(collect_unwrapped_output_type)(root_source, JZ_S(name), &field->value_type)
                                                        : groove_vt_clone(&field->value_type);
        cf.source_field = jazz_engine__optstr_some(jz_str_clone(name));
        jz_slice pub;
        if (F(descriptor_public_name)(field, &pub)) cf.source_public_name = jazz_engine__optstr_some(jz_str_from_slice(pub));
        cf.origin = JAZZ_ENGINE_CFO_SOURCE_ROW;
        cf.is_row_id = is_row_id;
        cf.is_presence = false;
        cf.is_output = jz_btree_contains(&selected_root, name);
        JZ_VEC_PUSH(root_fields, cf);
    }
    jz_btree_drop(&selected_root);

    jazz_engine_TypedOutputFieldVec occurrence = {0};
    jazz_engine_CapErr err = JE_TERM(root_join_occurrence_fields)(plan, resolved_sources, request, &occurrence);
    if (err) { flat_fields_drop(&root_fields); return err; }
    jazz_engine_StrVec root_occurrence_inputs = {0};
    for (u32 i = 0; i < occurrence.len; i++) {
        jazz_engine_TypedOutputField *o = &occurrence.ptr[i];
        if (!jz_btree_contains(available_fields, &o->name)) { tof_drop(o); continue; }
        jz_str input = jz_format("__collect_root_%.*s", JE_STR_ARG(o->name));
        jazz_engine_CollectFlatField cf;
        memset(&cf, 0, sizeof cf);
        cf.input = jz_str_clone(&input);
        cf.output = jz_str_clone(&o->name);
        cf.value_type = groove_vt_clone(&o->ty);
        cf.output_value_type = o->ty;
        cf.source_field = jazz_engine__optstr_some(o->name);
        cf.origin = JAZZ_ENGINE_CFO_DERIVED;
        JZ_VEC_PUSH(root_fields, cf);
        JZ_VEC_PUSH(root_occurrence_inputs, input);
    }
    jz_vec_drop(&occurrence, sizeof(jazz_engine_TypedOutputField), NULL);

    const jz_str *route_field; void *unit;
    JZ_BTREE_FOR(routing_param_fields, route_field, unit) {
        const groove_ValueType *value_type = NULL;
        jz_slice param;
        if (F(route_param_from_field)(JZ_S(route_field), &param)) {
            jz_str key = jazz_engine__str_view(param);
            value_type = jz_btree_get(&parameter_domain->user_params, &key);
            if (!value_type) value_type = jz_btree_get(&request->request->input.binding.param_types, &key);
        } else {
            const jazz_engine_ClaimParameter *claim = jz_btree_get(&parameter_domain->claim_params, route_field);
            if (claim) value_type = &claim->ty;
        }
        if (!value_type) {
            JE_DBG(d, jazz_engine__fmt__str, route_field);
            jazz_engine_CapErr e = jazz_engine__cap_runtime_fmt("collector route field %.*s has no parameter type", JE_STR_ARG(d));
            jz_str_drop(&d);
            flat_fields_drop(&root_fields);
            jazz_engine__strvec_drop(&root_occurrence_inputs);
            return e;
        }
        jazz_engine_CollectFlatField cf;
        memset(&cf, 0, sizeof cf);
        cf.input = jz_str_clone(route_field);
        cf.output = jz_str_clone(route_field);
        cf.value_type = groove_vt_clone(value_type);
        cf.output_value_type = groove_vt_clone(value_type);
        cf.source_field = jazz_engine__optstr_some(jz_str_clone(route_field));
        cf.source_public_name = jazz_engine__optstr_some(jz_str_clone(route_field));
        cf.origin = JAZZ_ENGINE_CFO_DERIVED;
        cf.is_output = true;
        JZ_VEC_PUSH(root_fields, cf);
    }
    u32 next_slot = 0;
    jazz_engine_CollectSlotLayoutVec slots = {0};
    err = collect_slot_layouts(projection->paths.ptr, projection->paths.len, resolved_sources, 1, &next_slot, &slots);
    if (err) {
        flat_fields_drop(&root_fields);
        jazz_engine__strvec_drop(&root_occurrence_inputs);
        return err;
    }
    memset(out, 0, sizeof *out);
    out->root_fields = root_fields;
    out->root_occurrence_inputs = root_occurrence_inputs;
    out->root_offset = 0;
    out->root_limit.tag = GROOVE_TOPBY_UNBOUNDED;
    out->slots = slots;
    return NULL;
}

static jazz_engine_CapErr collect_slot_layouts(const jazz_engine_AppPathProjection *paths, u32 npaths, const jz_btree *resolved_sources,
                                               u32 depth, u32 *next_slot, jazz_engine_CollectSlotLayoutVec *out) {
    if (depth > GROOVE_MAX_COLLECT_BY_TREE_DEPTH)
        return jazz_engine__cap_operator_fmt("association projection depth %u exceeds Groove MAX_COLLECT_BY_TREE_DEPTH (%u)", depth,
                                             (u32)GROOVE_MAX_COLLECT_BY_TREE_DEPTH);
    jazz_engine_CollectSlotLayoutVec result = {0};
    jazz_engine_CapErr err = NULL;
    for (u32 p = 0; p < npaths; p++) {
        const jazz_engine_AppPathProjection *path = &paths[p];
        const jazz_engine_ResolvedSource *source;
        err = JE_RES(ResolvedSource__lookup)(resolved_sources, &path->path.child, "app projection path child", &source);
        if (err) goto fail;
        u32 slot_id = (*next_slot)++;
        jz_str prefix = jz_format("__collect_path_%u", slot_id);
        jz_btree selected;
        jz_btree_init(&selected, &jazz_engine__STRSET_DESC);
        jazz_engine__strset_insert_clone(&selected, &source->row_shape.row_uuid_field);
        if (path->fields.tag == JAZZ_ENGINE_FP_ALL) {
            JZ_VEC_FOR(const jazz_model_ColumnSchema, c, source->table_schema->columns) {
                jz_str s = F(user_column_field)(JZ_S(&c->name));
                jazz_engine__btree_set_add(&selected, &s);
            }
        } else {
            const jz_str *f; void *unit;
            JZ_BTREE_FOR(&path->fields.fields, f, unit) {
                if (jz_str_eq_cstr(f, "id")) continue;
                jz_str s = collect_projection_source_field(source, JZ_S(f));
                jazz_engine__btree_set_add(&selected, &s);
            }
        }
        jazz_engine_CollectFlatFieldVec fields = {0};
        u32 nd;
        const groove_DescriptorField *dfields = groove__records__RecordDescriptor__fields(source->row_shape.descriptor, &nd);
        for (u32 i = 0; i < nd; i++) {
            if (!dfields[i].has_name || !jz_btree_contains(&selected, &dfields[i].name)) continue;
            const jz_str *source_field = &dfields[i].name;
            const groove_ValueType *svt = JE_GL(source_field_type)(source, JZ_S(source_field));
            if (!svt) {
                JE_DBG(ds, sid_debug, &source->row_shape.source);
                JE_DBG(df, jazz_engine__fmt__str, source_field);
                err = jazz_engine__cap_operator_fmt("association projection source %.*s does not provide field %.*s", JE_STR_ARG(ds), JE_STR_ARG(df));
                jz_str_drop(&ds);
                jz_str_drop(&df);
                flat_fields_drop(&fields);
                jz_btree_drop(&selected);
                jz_str_drop(&prefix);
                goto fail;
            }
            bool is_row_id = jz_str_eq(source_field, &source->row_shape.row_uuid_field);
            jazz_engine_CollectFlatField cf;
            memset(&cf, 0, sizeof cf);
            cf.output_value_type = JE_CL(collect_unwrapped_output_type)(source, JZ_S(source_field), svt);
            cf.value_type = is_row_id ? groove_vt_clone(svt) : JE_CL(collect_slot_input_type)(&cf.output_value_type);
            cf.input = jz_format("%.*s_%.*s", JE_STR_ARG(prefix), JE_STR_ARG(*source_field));
            cf.output = is_row_id ? jz_str_clone(source_field) : collect_nested_projection_output_field(source, JZ_S(source_field));
            if (!is_row_id) cf.source_public_name = resolved_source_public_name(source, JZ_S(source_field));
            cf.origin = JAZZ_ENGINE_CFO_SOURCE_ROW;
            cf.source_field = jazz_engine__optstr_some(jz_str_clone(source_field));
            cf.is_row_id = is_row_id;
            cf.is_output = true;
            JZ_VEC_PUSH(fields, cf);
        }
        jz_btree_drop(&selected);
        const jazz_engine_CollectFlatField *row_id = NULL;
        JZ_VEC_FOR(const jazz_engine_CollectFlatField, f, fields) if (f->is_row_id) { row_id = f; break; }
        if (!row_id) jz_panic("collector child projection always includes its row id");
        jazz_engine_CollectSlotLayout slot;
        memset(&slot, 0, sizeof slot);
        slot.row_id_input = jz_str_clone(&row_id->input);
        slot.presence_input = jz_format("%.*s_present", JE_STR_ARG(prefix));
        jz_str_drop(&prefix);
        err = collect_slot_layouts(path->children.ptr, path->children.len, resolved_sources, depth + 1, next_slot, &slot.children);
        if (err) {
            flat_fields_drop(&fields);
            jz_str_drop(&slot.row_id_input);
            jz_str_drop(&slot.presence_input);
            goto fail;
        }
        JE_IN(ProgramPathId__Clone__clone)(&slot.path, &path->path);
        slot.collection_field = jz_str_clone(&path->field);
        slot.fields = fields;
        slot.reference_order = false;
        slot.offset = 0;
        slot.limit.tag = GROOVE_TOPBY_UNBOUNDED;
        JZ_VEC_PUSH(result, slot);
    }
    *out = result;
    return NULL;
fail:
    slots_drop(&result);
    return err;
}

/* ---------------------------------------------------------------- projections */
static groove_ProjectField collect_root_field_projection(const jazz_engine_ResolvedSource *source, const jazz_engine_CollectFlatField *field,
                                                         jz_slice output, bool source_positions_are_stable) {
    if (!field->source_field.some) jz_panic("root collector fields retain their source field");
    const jz_str *source_field = &field->source_field.s;
    switch (field->origin) {
    case JAZZ_ENGINE_CFO_SOURCE_ROW: {
        if (!source_positions_are_stable) return PF(renamed)(jz_str_clone(source_field), jz_str_from_slice(output));
        u32 idx;
        if (!resolved_source_field_index_by_name(source, JZ_S(source_field), &idx))
            jz_panic("source-row collector fields are present in the source descriptor");
        return PF(renamed_resolved)(idx, jz_str_from_slice(output));
    }
    default:
        /* Route parameters and occurrence keys are appended by graph lowering. */
        return PF(renamed)(jz_str_clone(source_field), jz_str_from_slice(output));
    }
}

jazz_engine_CapErr JE_CL(root_collect_context_graph)(groove_GraphBuilder *graph, const jazz_engine_ResolvedSource *source,
    const jazz_engine_CollectLayout *layout, bool source_positions_are_stable, groove_GraphBuilder **out) {
    groove_ProjectFieldVec fields = {0};
    JZ_VEC_FOR(const jazz_engine_CollectFlatField, field, layout->root_fields) {
        if (!field->source_field.some) jz_panic("root collector fields retain their source field");
        JZ_VEC_PUSH(fields, collect_root_field_projection(source, field, JZ_S(&field->source_field.s), source_positions_are_stable));
        JZ_VEC_PUSH(fields, collect_root_field_projection(source, field, JZ_S(&field->input), source_positions_are_stable));
    }
    *out = groove__ivm__graph__GraphBuilder__project_fields(graph, fields);
    return NULL;
}

static void collect_all_slots(const jazz_engine_CollectSlotLayout *slots, u32 n, SlotRefVec *all) {
    for (u32 i = 0; i < n; i++) {
        JZ_VEC_PUSH(*all, &slots[i]);
        collect_all_slots(slots[i].children.ptr, slots[i].children.len, all);
    }
}

static groove_LiteralValue lit_bool(bool b) { groove_LiteralValue v; memset(&v, 0, sizeof v); v.tag = GROOVE_LIT_BOOL; v.u.b = b; return v; }

static groove_ProjectField collect_flat_default(const jazz_engine_CollectFlatField *field) {
    if (field->is_row_id) {
        groove_LiteralValue nil;
        memset(&nil, 0, sizeof nil);
        nil.tag = GROOVE_LIT_UUID; /* Uuid::nil() */
        return PF(literal)(jz_str_clone(&field->input), nil);
    }
    if (field->is_presence) return PF(literal)(jz_str_clone(&field->input), lit_bool(false));
    return PF(null_typed)(jz_str_clone(&field->input), groove_vt_clone(&field->value_type));
}

/* current_slot / slot_source: both NULL for None */
static jazz_engine_CapErr collect_flat_projection(const jazz_engine_CollectLayout *layout, const jazz_engine_CollectSlotLayout *current_slot,
    const jazz_engine_ResolvedSource *slot_source, const jz_btree *inherited_flat_fields, const jazz_engine_ResolvedSource *root_source,
    bool source_positions_are_stable, groove_ProjectFieldVec *out) {
    groove_ProjectFieldVec fields = {0};
    JZ_VEC_FOR(const jazz_engine_CollectFlatField, field, layout->root_fields) {
        if (current_slot) {
            JZ_VEC_PUSH(fields, PF(renamed)(F(left_field)(JZ_S(&field->input)), jz_str_clone(&field->input)));
        } else {
            if (!root_source) jz_panic("root projection carries its source descriptor");
            JZ_VEC_PUSH(fields, collect_root_field_projection(root_source, field, JZ_S(&field->input), source_positions_are_stable));
        }
    }
    SlotRefVec all = {0};
    collect_all_slots(layout->slots.ptr, layout->slots.len, &all);
    for (u32 s = 0; s < all.len; s++) {
        const jazz_engine_CollectSlotLayout *slot = all.ptr[s];
        const jazz_engine_ResolvedSource *current_source =
            (current_slot && JE_IN(ProgramPathId__PartialEq__eq)(&current_slot->path, &slot->path)) ? slot_source : NULL;
        bool is_current = current_source != NULL;
        JZ_VEC_FOR(const jazz_engine_CollectFlatField, field, slot->fields) {
            groove_ProjectField pf;
            if (is_current && slot->reference_array_input.some && jz_str_eq(&slot->reference_array_input.s, &field->input)) {
                if (!field->source_field.some) jz_panic("reference source");
                pf = PF(nullable_flat)(F(left_field)(JZ_S(&field->source_field.s)), jz_str_clone(&field->input));
            } else if (current_source) {
                if (!field->source_field.some) jz_panic("collector child fields retain their source field");
                const jz_str *source_field = &field->source_field.s;
                jz_str src = F(right_field)(JZ_S(source_field));
                if (field->is_row_id) {
                    pf = PF(renamed)(src, jz_str_clone(&field->input));
                } else {
                    /* Indirect nullable JSON cells arrive as exactly their logical output and need a distinct wrapper. */
                    const groove_ValueType *t = JE_GL(source_field_type)(current_source, JZ_S(source_field));
                    if (t && VT_EQ(t, &field->output_value_type)) pf = PF(nullable)(src, jz_str_clone(&field->input));
                    else pf = PF(nullable_flat)(src, jz_str_clone(&field->input));
                }
            } else if (jz_btree_contains(inherited_flat_fields, &field->input)) {
                pf = PF(renamed)(F(left_field)(JZ_S(&field->input)), jz_str_clone(&field->input));
            } else {
                pf = collect_flat_default(field);
            }
            JZ_VEC_PUSH(fields, pf);
        }
        if (is_current) JZ_VEC_PUSH(fields, PF(literal)(jz_str_clone(&slot->presence_input), lit_bool(true)));
        else if (jz_btree_contains(inherited_flat_fields, &slot->presence_input))
            JZ_VEC_PUSH(fields, PF(renamed)(F(left_field)(JZ_S(&slot->presence_input)), jz_str_clone(&slot->presence_input)));
        else JZ_VEC_PUSH(fields, PF(literal)(jz_str_clone(&slot->presence_input), lit_bool(false)));
    }
    jz_vec_drop(&all, sizeof(void *), NULL);
    *out = fields;
    return NULL;
}

jazz_engine_CapErr JE_CL(collect_anchor_graph)(groove_GraphBuilder *graph, const jazz_engine_ResolvedSource *root_source,
    const jazz_engine_CollectLayout *layout, bool source_positions_are_stable, groove_GraphBuilder **out) {
    jz_btree empty;
    jz_btree_init(&empty, &jazz_engine__STRSET_DESC);
    groove_ProjectFieldVec fields;
    jazz_engine_CapErr err = collect_flat_projection(layout, NULL, NULL, &empty, root_source, source_positions_are_stable, &fields);
    if (err) { groove_gb_drop(graph); return err; }
    *out = groove__ivm__graph__GraphBuilder__project_fields(graph, fields);
    return NULL;
}

static jazz_engine_CapErr collect_child_context_projection(const jazz_engine_CollectLayout *layout, const jazz_engine_CollectSlotLayout *current_slot,
    const jazz_engine_ResolvedSource *child_source, const jz_btree *inherited_flat_fields, groove_ProjectFieldVec *out) {
    groove_ProjectFieldVec fields = {0};
    u32 n;
    const groove_DescriptorField *d = groove__records__RecordDescriptor__fields(child_source->row_shape.descriptor, &n);
    for (u32 i = 0; i < n; i++) {
        if (!d[i].has_name) continue;
        JZ_VEC_PUSH(fields, PF(renamed)(F(right_field)(JZ_S(&d[i].name)), jz_str_clone(&d[i].name)));
    }
    groove_ProjectFieldVec flat;
    jazz_engine_CapErr err = collect_flat_projection(layout, current_slot, child_source, inherited_flat_fields, NULL, true, &flat);
    if (err) { jz_vec_drop(&fields, sizeof(groove_ProjectField), groove__ivm__graph__ProjectField__drop_v); return err; }
    jz_vec_extend(&fields, flat.ptr, flat.len, sizeof(groove_ProjectField));
    jz_vec_drop(&flat, sizeof(groove_ProjectField), NULL);
    *out = fields;
    return NULL;
}

static void collect_slot_flat_field_names(const jazz_engine_CollectSlotLayout *slot, jz_btree *into) {
    JZ_VEC_FOR(const jazz_engine_CollectFlatField, f, slot->fields) jazz_engine__strset_insert_clone(into, &f->input);
    jazz_engine__strset_insert_clone(into, &slot->presence_input);
}

jazz_engine_CapErr JE_CL(lower_collect_slot_graphs)(const jazz_engine_CollectSlotLayout *slot, const jazz_engine_CorrelatedPathPlan *path,
    groove_GraphBuilder *parent_graph, const jazz_engine_ResolvedSource *parent_source, const jazz_engine_ResolvedSource *root_source,
    const jazz_engine_CollectLayout *layout, const jz_btree *inherited_flat_fields, const jz_btree *resolved_sources,
    jazz_engine_LoweringContext *request, groove_GraphBuilderVec *out) {
    jazz_engine_LoweredRelationInput lowered;
    jazz_engine_UrErr ue = JE_GL(lower_correlated_path_relation_graph_from_parent)(path, parent_graph, parent_source, resolved_sources, request, false, &lowered);
    if (ue) return jazz_engine__ur_into_cap(ue);
    groove_GraphBuilder *joined = lowered.graph;
    lowered.graph = NULL;
    JE_GL(LoweredRelationInput__drop)(&lowered);
    const jazz_engine_ResolvedSource *child_source;
    jazz_engine_CapErr err = JE_RES(ResolvedSource__lookup)(resolved_sources, &slot->path.child, "collector child", &child_source);
    if (err) { groove_gb_drop(joined); return err; }
    groove_ProjectFieldVec afields, cfields;
    err = collect_flat_projection(layout, slot, child_source, inherited_flat_fields, NULL, true, &afields);
    if (err) { groove_gb_drop(joined); return err; }
    groove_GraphBuilder *association = groove__ivm__graph__GraphBuilder__project_fields(groove_gb_clone(joined), afields);
    err = collect_child_context_projection(layout, slot, child_source, inherited_flat_fields, &cfields);
    if (err) { groove_gb_drop(joined); groove_gb_drop(association); return err; }
    groove_GraphBuilder *context = groove__ivm__graph__GraphBuilder__project_fields(joined, cfields);
    groove_GraphBuilderVec graphs = {0};
    JZ_VEC_PUSH(graphs, association);
    jz_btree child_inherited;
    jazz_engine__strset_clone(&child_inherited, inherited_flat_fields);
    collect_slot_flat_field_names(slot, &child_inherited);
    JZ_VEC_FOR(const jazz_engine_CollectSlotLayout, nested_slot, slot->children) {
        const jazz_engine_CorrelatedPathPlan *nested_path = JE_TERM(find_nested_correlated_path)(path, &nested_slot->path);
        if (!nested_path) {
            JE_DBG(a, JE_IN(ProgramPathId__Debug__fmt), &nested_slot->path);
            JE_DBG(b, JE_IN(ProgramPathId__Debug__fmt), &slot->path);
            err = jazz_engine__cap_operator_fmt("app projection path %.*s is not nested below %.*s", JE_STR_ARG(a), JE_STR_ARG(b));
            jz_str_drop(&a);
            jz_str_drop(&b);
            goto fail;
        }
        err = JE_CL(lower_collect_slot_graphs)(nested_slot, nested_path, groove_gb_clone(context), child_source, root_source, layout,
                                               &child_inherited, resolved_sources, request, &graphs);
        if (err) goto fail;
    }
    jz_btree_drop(&child_inherited);
    groove_gb_drop(context);
    jz_vec_extend(out, graphs.ptr, graphs.len, sizeof(groove_GraphBuilder *));
    jz_vec_drop(&graphs, sizeof(groove_GraphBuilder *), NULL);
    return NULL;
fail:
    jz_btree_drop(&child_inherited);
    groove_gb_drop(context);
    jz_vec_drop(&graphs, sizeof(groove_GraphBuilder *), groove__ivm__graph__GraphBuilder__drop_v);
    return err;
}

/* ---------------------------------------------------------------- Groove builders and descriptors */
groove_CollectBySlotBuilder JE_CL(collect_slot_builder)(const jazz_engine_CollectSlotLayout *slot, jz_slice parent_row_id, const jz_btree *route_fields) {
    jazz_engine_StrVec group_cols = {0};
    JZ_VEC_PUSH(group_cols, jz_str_from_slice(parent_row_id));
    const jz_str *rf; void *unit;
    JZ_BTREE_FOR(route_fields, rf, unit) jazz_engine__strvec_push_clone(&group_cols, rf);
    groove_CollectByFieldVec child_fields = {0};
    JZ_VEC_FOR(const jazz_engine_CollectFlatField, field, slot->fields) {
        if (!field->is_output) continue;
        if (field->is_row_id || VT_EQ(&field->value_type, &field->output_value_type))
            JZ_VEC_PUSH(child_fields, groove__ivm__graph__CollectByField__renamed(jz_str_clone(&field->input), jz_str_clone(&field->output)));
        else
            JZ_VEC_PUSH(child_fields, groove__ivm__graph__CollectByField__renamed_unwrap_nullable(jz_str_clone(&field->input), jz_str_clone(&field->output)));
    }
    groove_CollectBySlotBuilderVec children = {0};
    JZ_VEC_FOR(const jazz_engine_CollectSlotLayout, child, slot->children)
        JZ_VEC_PUSH(children, JE_CL(collect_slot_builder)(child, JZ_S(&slot->row_id_input), route_fields));
    groove_TopByOrderVec order_cols;
    jz_vec_clone(&order_cols, &slot->order_cols, sizeof(groove_TopByOrder), groove__ivm__graph__TopByOrder__clone_v);
    jazz_engine_StrVec tie_cols;
    jazz_engine__strvec_clone(&tie_cols, &slot->tie_cols);
    groove_CollectBySlotBuilder builder = groove__ivm__graph__CollectBySlotBuilder__new(group_cols, child_fields, jz_str_clone(&slot->collection_field),
                                                                                        children, order_cols, tie_cols, slot->offset, slot->limit);
    /* Route fields identify a maintained binding; carry them only as execution owner keys. */
    groove__ivm__graph__CollectBySlotBuilder__with_owner_key_cols(&builder, jazz_engine__strvec_from_set(route_fields));
    groove__ivm__graph__CollectBySlotBuilder__with_presence_col(&builder, jz_str_clone(&slot->presence_input));
    builder.has_reference_array_col = slot->reference_array_input.some;
    if (slot->reference_array_input.some) builder.reference_array_col = groove__ivm__graph__FieldRef__name(jz_str_clone(&slot->reference_array_input.s));
    builder.reference_order = slot->reference_order;
    return builder;
}

typedef struct { JZ_VEC(jz_str) names; groove_ValueTypeVec types; } NamedTypes;

static groove_RecordDescriptor named_types_descriptor(NamedTypes *nt) {
    groove_RecordDescriptor d = groove__records__RecordDescriptor__new(nt->names.ptr, nt->types.ptr, nt->names.len);
    jz_vec_drop(&nt->names, sizeof(jz_str), NULL);
    jz_vec_drop(&nt->types, sizeof(groove_ValueType), NULL);
    return d;
}

static void collect_slot_output_field(const jazz_engine_CollectSlotLayout *slot, NamedTypes *into);

static groove_RecordDescriptor collect_slot_output_descriptor(const jazz_engine_CollectSlotLayout *slot) {
    NamedTypes nt = {{0}, {0}};
    JZ_VEC_FOR(const jazz_engine_CollectFlatField, f, slot->fields) {
        if (!f->is_output) continue;
        JZ_VEC_PUSH(nt.names, jz_str_clone(&f->output));
        JZ_VEC_PUSH(nt.types, groove_vt_clone(&f->output_value_type));
    }
    JZ_VEC_FOR(const jazz_engine_CollectSlotLayout, child, slot->children) collect_slot_output_field(child, &nt);
    return named_types_descriptor(&nt);
}

static void collect_slot_output_field(const jazz_engine_CollectSlotLayout *slot, NamedTypes *into) {
    groove_RecordDescriptor d = collect_slot_output_descriptor(slot);
    groove_ValueType record;
    memset(&record, 0, sizeof record);
    record.tag = GROOVE_VT_RECORD;
    record.u.record = (groove_RecordDescriptor *)jz_memdup(&d, sizeof d);
    JZ_VEC_PUSH(into->names, jz_str_clone(&slot->collection_field));
    JZ_VEC_PUSH(into->types, groove_vt_boxed(GROOVE_VT_ARRAY, record));
}

jazz_engine_CapErr JE_CL(collect_output_descriptor)(const jazz_engine_CollectLayout *layout, groove_RecordDescriptor *out) {
    NamedTypes nt = {{0}, {0}};
    JZ_VEC_FOR(const jazz_engine_CollectFlatField, f, layout->root_fields) {
        if (!f->is_output) continue;
        JZ_VEC_PUSH(nt.names, jz_str_clone(&f->output));
        JZ_VEC_PUSH(nt.types, groove_vt_clone(&f->output_value_type));
    }
    JZ_VEC_FOR(const jazz_engine_CollectSlotLayout, slot, layout->slots) collect_slot_output_field(slot, &nt);
    *out = named_types_descriptor(&nt);
    return NULL;
}
