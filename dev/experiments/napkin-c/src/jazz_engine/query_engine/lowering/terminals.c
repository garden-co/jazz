/* lowering/terminals.rs: construction of typed public and maintained-query
 * terminals. Terminal lowering defines result membership, structured
 * application rows, relation edges, version witnesses, aggregates, routing
 * fields, and the schemas used to decode those outputs.
 *
 * Mapping notes:
 *  - CapabilityResult<T> is a boxed report (NULL = Ok) plus out-params; a
 *    `.map_err(single_gap_report)` is JE_UR_CAP_TRY / jazz_engine__ur_into_cap.
 *  - Graph builders are jz_rc payload pointers; constructors consume them. */
#include "../lowering.h"

#define F(x) jazz_engine__query_engine__fields__##x
#define PF(x) groove__ivm__graph__ProjectField__##x
#define GB(x) groove__ivm__graph__GraphBuilder__##x
#define VT_EQ groove__records__values__ValueType__PartialEq__eq
#define VT_DROP groove__records__values__ValueType__drop
#define CRPF(x) jazz_engine__query_engine__publication__CurrentRowPublicationField__##x

typedef jazz_engine_CapErr CapErr;
typedef jazz_engine_ResolvedSource RSource;
typedef jazz_engine_LoweredTerminal LTerm;
typedef jazz_engine_CurrentRowPublicationField Crpf;

static jz_str s_lit(const char *s) { return jz_str_from(s); }
static jz_str s_clone(const jz_str *s) { return jz_str_clone(s); }
static void strset_init(jz_btree *t) { jz_btree_init(t, &jazz_engine__STRSET_DESC); }
static void strset_extend_clone(jz_btree *dst, const jz_btree *src) {
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(src, f, unit) jazz_engine__strset_insert_clone(dst, f);
}
static void strset_intersect(const jz_btree *src, const jz_btree *filter, jz_btree *out) {
    strset_init(out);
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(src, f, unit) if (jz_btree_contains(filter, f)) jazz_engine__strset_insert_clone(out, f);
}
static void pfvec_drop(groove_ProjectFieldVec *v) { jz_vec_drop(v, sizeof(groove_ProjectField), groove__ivm__graph__ProjectField__drop_v); }
static void graphs_drop(groove_GraphBuilderVec *v) { jz_vec_drop(v, sizeof(groove_GraphBuilder *), groove__ivm__graph__GraphBuilder__drop_v); }
static void order_cols_drop(groove_TopByOrderVec *v) { jz_vec_drop(v, sizeof(groove_TopByOrder), groove__ivm__graph__TopByOrder__drop_v); }
static void order_cols_clone(groove_TopByOrderVec *dst, const groove_TopByOrderVec *src) {
    jz_vec_clone(dst, src, sizeof(groove_TopByOrder), groove__ivm__graph__TopByOrder__clone_v);
}
static groove_LiteralValue lit_string(jz_str s) { groove_LiteralValue v; memset(&v, 0, sizeof v); v.tag = GROOVE_LIT_STRING; v.u.s = s; return v; }
static groove_TopByLimit limit_of(bool has, u32 n) { groove_TopByLimit l = {has ? GROOVE_TOPBY_FINITE : GROOVE_TOPBY_UNBOUNDED, has ? (u64)n : 0}; return l; }
static jz_str dbg_str(const jz_str *s) { jz_str d = {0}; jazz_engine__fmt__str(&d, s); return d; }
static CapErr cap_rt(const char *msg) { return JE_RES(CapabilityReport__runtime)(jz_str_from(msg)); }
static CapErr cap_op(const char *msg) { return JE_RES(CapabilityReport__operator)(jz_str_from(msg)); }
static void crpf_put(jz_btree *map, jz_str key, Crpf value) { jazz_engine__btree_put(map, &key, &value); }
static Crpf crpf_result(jz_str name, u8 visibility) { Crpf f; memset(&f, 0, sizeof f); f.tag = JAZZ_ENGINE_CRPF_RESULT_FIELD; f.name = name; f.visibility = visibility; return f; }
static void str_str_put(jz_btree *map, jz_str k, jz_str v) { jazz_engine__btree_put(map, &k, &v); }
static void str_u8_put(jz_btree *map, jz_str k, u8 v) { jazz_engine__btree_put(map, &k, &v); }

static void hidden_source_fields(const jazz_engine_SourceRowShape *row_shape, jz_btree *out);
static const jazz_engine_LinearStepVec *root_linear_steps(const jazz_engine_AnalyzedQueryPlan *plan);

/* ---------------------------------------------------------------- witness flavours */
enum { WF_VERSION, WF_REPLACEMENT };
enum { WL_CONTENT, WL_DELETION };
/* WitnessFlavour::of: false = None */
static bool witness_flavour_of(const jazz_engine_ProgramFactKey *key, u8 *out) {
    if (key->tag == JAZZ_ENGINE_FACT_VERSION_WITNESSES) { *out = WF_VERSION; return true; }
    if (key->tag == JAZZ_ENGINE_FACT_REPLACEMENT_WITNESSES) { *out = WF_REPLACEMENT; return true; }
    return false;
}
static u8 witness_terminal(u8 flavour, u8 layer) {
    if (flavour == WF_VERSION) return layer == WL_CONTENT ? JAZZ_ENGINE_PFT_VERSION_WITNESS_CONTENT : JAZZ_ENGINE_PFT_VERSION_WITNESS_DELETION;
    return layer == WL_CONTENT ? JAZZ_ENGINE_PFT_REPLACEMENT_WITNESS_CONTENT : JAZZ_ENGINE_PFT_REPLACEMENT_WITNESS_DELETION;
}
/* The `event_kind` literal; `maintained.{tag}` is also the sink prefix. */
static const char *witness_tag(u8 flavour, u8 layer) {
    if (flavour == WF_VERSION) return layer == WL_CONTENT ? "version_content" : "version_deletion";
    return layer == WL_CONTENT ? "replacement_content" : "replacement_deletion";
}

static bool version_witness_schemas_eq(const jazz_engine_VersionWitnessSchemas *a, const jazz_engine_VersionWitnessSchemas *b) {
    if (!jz_str_eq(&a->role_field, &b->role_field) || a->has_content != b->has_content || a->has_deletion != b->has_deletion) return false;
    if (a->has_content && !JE_SCH(VersionWitnessSchema__PartialEq__eq)(&a->content, &b->content)) return false;
    if (a->has_deletion && !JE_SCH(VersionWitnessSchema__PartialEq__eq)(&a->deletion, &b->deletion)) return false;
    return jz_btree_eq(&a->routing_param_fields, &b->routing_param_fields, NULL);
}
static bool expression_is_event_kind(const groove_ProjectExpr *expression, const char *tag) {
    groove_ProjectField literal = PF(literal)(s_lit("event_kind"), lit_string(s_lit(tag)));
    bool eq = groove__ivm__graph__ProjectExpr__PartialEq__eq(expression, &literal.expression);
    PF(drop)(&literal);
    return eq;
}

/* Share execution only after proving the complete typed source/routing schema
 * and graph equal, except for the explicitly known terminal role literal.
 * Logical sinks remain separate contracts; no per-row identity cache is needed. */
void JE_TERM(shared_witness_sinks)(const LTerm *terminals, u32 n, jz_btree *out) {
    jz_btree_init(out, &jazz_engine__STR_STR_MAP_DESC);
    for (u32 r = 0; r < n; r++) {
        const LTerm *replacement = &terminals[r];
        const jazz_engine_OutputTerminalSchema *ro = &replacement->output;
        if (ro->tag != JAZZ_ENGINE_OTS_FACT || ro->u.fact.key.tag != JAZZ_ENGINE_FACT_REPLACEMENT_WITNESSES ||
            ro->u.fact.schema.tag != JAZZ_ENGINE_PFS_REPLACEMENT_WITNESSES) continue;
        u8 layer;
        if (ro->u.fact.terminal == JAZZ_ENGINE_PFT_REPLACEMENT_WITNESS_CONTENT) layer = WL_CONTENT;
        else if (ro->u.fact.terminal == JAZZ_ENGINE_PFT_REPLACEMENT_WITNESS_DELETION) layer = WL_DELETION;
        else continue;
        const jazz_engine_VersionWitnessSchemas *replacement_schema = &ro->u.fact.schema.u.witnesses;
        u8 version_role = witness_terminal(WF_VERSION, layer);
        const char *version_tag = witness_tag(WF_VERSION, layer), *replacement_tag = witness_tag(WF_REPLACEMENT, layer);
        for (u32 v = 0; v < n; v++) {
            const LTerm *version = &terminals[v];
            bool used = false;
            const jz_str *k, *sink;
            JZ_BTREE_FOR(out, k, sink) if (jz_str_eq(sink, &version->sink)) { used = true; break; }
            if (used) continue;
            const jazz_engine_OutputTerminalSchema *vo = &version->output;
            if (vo->tag != JAZZ_ENGINE_OTS_FACT || vo->u.fact.key.tag != JAZZ_ENGINE_FACT_VERSION_WITNESSES ||
                vo->u.fact.schema.tag != JAZZ_ENGINE_PFS_VERSION_WITNESSES) continue;
            const jazz_engine_VersionWitnessSchemas *version_schema = &vo->u.fact.schema.u.witnesses;
            if (vo->u.fact.terminal != version_role || !version_witness_schemas_eq(version_schema, replacement_schema)) continue;
            if (version->graph->tag != GROOVE_GB_PROJECT || replacement->graph->tag != GROOVE_GB_PROJECT) continue;
            const groove_ProjectFieldVec *vf = &version->graph->u.project.fields, *rf = &replacement->graph->u.project.fields;
            if (!GB(PartialEq__eq)(version->graph->u.project.input, replacement->graph->u.project.input) || vf->len != rf->len) continue;
            u32 role_fields = 0;
            bool equal = true;
            for (u32 i = 0; i < vf->len && equal; i++) {
                const groove_ProjectField *left = &vf->ptr[i], *right = &rf->ptr[i];
                if (jz_str_eq(&left->output_name, &version_schema->role_field)) {
                    role_fields++;
                    equal = jz_str_eq(&left->output_name, &right->output_name) &&
                            groove__records__FieldIdentity__PartialEq__eq(&left->output_identity, &right->output_identity) &&
                            expression_is_event_kind(&left->expression, version_tag) && expression_is_event_kind(&right->expression, replacement_tag);
                } else {
                    equal = PF(PartialEq__eq)(left, right);
                }
            }
            if (equal && role_fields == 1) {
                str_str_put(out, s_clone(&replacement->sink), s_clone(&version->sink));
                break;
            }
        }
    }
}

/* ---------------------------------------------------------------- publication bindings */
/* resolved_source_public_name: false = None */
static bool resolved_source_public_name(const RSource *source, const jz_str *field, jz_str *out) {
    u32 n;
    const groove_DescriptorField *d = groove__records__RecordDescriptor__fields(source->row_shape.descriptor, &n);
    for (u32 i = 0; i < n; i++) {
        if (!d[i].has_name || !jz_str_eq(&d[i].name, field)) continue;
        jz_slice name;
        if (!F(descriptor_public_name)(&d[i], &name)) return false;
        *out = jz_str_from_slice(name);
        return true;
    }
    return false;
}

/* consumes name */
static Crpf source_publication_field(const RSource *source, jz_str name) {
    const jazz_types_PhysicalColumnId *id = jz_btree_get(&source->stored_column_ids, &name);
    if (id) {
        Crpf f;
        memset(&f, 0, sizeof f);
        f.tag = JAZZ_ENGINE_CRPF_STORED_COLUMN;
        f.id = *id;
        f.name = name;
        return f;
    }
    return crpf_result(name, JAZZ_ENGINE_CRRV_APPLICATION_CELL);
}

static void collect_publication_fields(const RSource *source, const jazz_engine_CollectLayout *layout, jz_btree *out) {
    jz_btree_init(out, &jazz_engine__STR_CRPF_MAP_DESC);
    JZ_VEC_FOR(const jazz_engine_CollectFlatField, field, layout->root_fields) {
        if (!field->is_output) continue;
        Crpf binding;
        if (field->origin == JAZZ_ENGINE_CFO_SOURCE_ROW && field->source_public_name.some && !field->is_row_id) {
            binding = source_publication_field(source, s_clone(&field->source_public_name.s));
        } else if (field->origin == JAZZ_ENGINE_CFO_SOURCE_ROW) {
            const jz_str *meta = field->source_field.some ? &field->source_field.s : &field->output;
            binding = crpf_result(s_clone(&field->output), jazz_engine__query_engine__publication__CurrentRowResultVisibility__current_row_metadata(JZ_S(meta)));
        } else {
            binding = crpf_result(s_clone(&field->output), field->is_row_id ? JAZZ_ENGINE_CRRV_HIDDEN_METADATA : JAZZ_ENGINE_CRRV_APPLICATION_CELL);
        }
        crpf_put(out, s_clone(&field->output), binding);
    }
    JZ_VEC_FOR(const jazz_engine_CollectSlotLayout, slot, layout->slots)
        crpf_put(out, s_clone(&slot->collection_field), crpf_result(s_clone(&slot->collection_field), JAZZ_ENGINE_CRRV_APPLICATION_CELL));
}

CapErr JE_TERM(validate_app_row_publication_schema)(const jazz_engine_AppRowSchema *rows) {
    u32 n;
    const groove_DescriptorField *d = groove__records__RecordDescriptor__fields(rows->descriptor, &n);
    for (u32 i = 0; i < n; i++) {
        if (!d[i].has_name) return cap_rt("application row descriptor has an unnamed field");
        const jz_str *name = &d[i].name;
        if (!jz_str_eq_cstr(name, "row_uuid") && !jz_btree_contains(&rows->hidden_fields, name) && !jz_btree_contains(&rows->publication_fields, name)) {
            jz_str q = dbg_str(name);
            CapErr e = jazz_engine__cap_runtime_fmt("application row field %.*s has no publication binding", JE_STR_ARG(q));
            jz_str_drop(&q);
            return e;
        }
    }
    return NULL;
}

/* ---------------------------------------------------------------- collector layout helpers */
/* CollectLayout::public_root_fields: root fields published as application columns; the row id stays internal. */
static bool is_public_root_field(const jazz_engine_CollectFlatField *field) { return field->is_output && !field->is_row_id; }

/* Public root columns keep their CurrentRow cell wrapper unless the projection
 * declared an unwrapped logical type. */
static void collect_layout_root_field_carriers(const jazz_engine_CollectLayout *layout, jz_btree *out) {
    jz_btree_init(out, &jazz_engine__STR_U8_MAP_DESC);
    JZ_VEC_FOR(const jazz_engine_CollectFlatField, field, layout->root_fields) {
        if (!is_public_root_field(field)) continue;
        str_u8_put(out, s_clone(&field->output), VT_EQ(&field->value_type, &field->output_value_type) ? JAZZ_ENGINE_CARRIER_CURRENT_ROW : JAZZ_ENGINE_CARRIER_LOGICAL);
    }
}
static void collect_layout_root_public_field_names(const jazz_engine_CollectLayout *layout, jz_btree *out) {
    jz_btree_init(out, &jazz_engine__STR_STR_MAP_DESC);
    JZ_VEC_FOR(const jazz_engine_CollectFlatField, field, layout->root_fields)
        if (is_public_root_field(field)) str_str_put(out, s_clone(&field->output), JE_LOW(public_root_field_name)(field));
}

/* Collector output columns for the root row, unwrapping nullable cells whose
 * declared output type is logical. */
static groove_CollectByFieldVec collect_root_output_fields(const jazz_engine_CollectFlatFieldVec *root_fields) {
    groove_CollectByFieldVec v = {0};
    JZ_VEC_FOR(const jazz_engine_CollectFlatField, field, *root_fields) {
        if (!field->is_output) continue;
        if (field->is_row_id || VT_EQ(&field->value_type, &field->output_value_type))
            JZ_VEC_PUSH(v, groove__ivm__graph__CollectByField__renamed(s_clone(&field->input), s_clone(&field->output)));
        else
            JZ_VEC_PUSH(v, groove__ivm__graph__CollectByField__renamed_unwrap_nullable(s_clone(&field->input), s_clone(&field->output)));
    }
    return v;
}

typedef struct LoweredAppRows {
    jz_btree publication_fields; /* String -> CurrentRowPublicationField */
    groove_GraphBuilder *graph;
    groove_RecordDescriptor descriptor;
    jz_btree hidden_fields;
    u8 carrier;
    jz_btree field_carriers;     /* String -> AppRowCarrier */
    jz_btree public_field_names; /* String -> String */
    jazz_engine_AppRowTerminal terminal;
} LoweredAppRows;

/* Collectors require their physical root UUID as the first grouping field.
 * For root UNION ALL the following discriminator is source position zero; the
 * prepared terminal layout carries that fact to the decoder. Route fields follow. */
static jazz_engine_StrVec root_collect_group_fields(jz_str root, const jazz_engine_StrVec *occurrence, const jz_btree *route_fields) {
    jazz_engine_StrVec v = jazz_engine__strvec_one(root);
    JZ_VEC_FOR(const jz_str, o, *occurrence) jazz_engine__strvec_push_clone(&v, o);
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(route_fields, f, unit) jazz_engine__strvec_push_clone(&v, f);
    return v;
}

static const jazz_engine_CorrelatedPathPlan *find_correlated_path_in_tree(const jazz_engine_CorrelatedPathPlan *path, const jazz_engine_ProgramPathId *target) {
    if (JE_IN(ProgramPathId__PartialEq__eq)(&path->path, target)) return path;
    JZ_VEC_FOR(const jazz_engine_CorrelatedPathPlan, c, path->siblings) {
        const jazz_engine_CorrelatedPathPlan *found = find_correlated_path_in_tree(c, target);
        if (found) return found;
    }
    JZ_VEC_FOR(const jazz_engine_CorrelatedPathPlan, c, path->nested) {
        const jazz_engine_CorrelatedPathPlan *found = find_correlated_path_in_tree(c, target);
        if (found) return found;
    }
    return NULL;
}
static const jazz_engine_CorrelatedPathPlan *find_correlated_path(const jazz_engine_AnalyzedQueryPlan *plan, const jazz_engine_ProgramPathId *path) {
    if (plan->tag != JAZZ_ENGINE_AQP_CORRELATED_PATH) return NULL;
    return find_correlated_path_in_tree(&plan->u.correlated, path);
}
const jazz_engine_CorrelatedPathPlan *JE_TERM(find_nested_correlated_path)(const jazz_engine_CorrelatedPathPlan *parent, const jazz_engine_ProgramPathId *path) {
    JZ_VEC_FOR(const jazz_engine_CorrelatedPathPlan, c, parent->nested) {
        const jazz_engine_CorrelatedPathPlan *found = find_correlated_path_in_tree(c, path);
        if (found) return found;
    }
    return NULL;
}
static CapErr path_not_correlated(const jazz_engine_ProgramPathId *path) {
    JE_DBG(d, JE_IN(ProgramPathId__Debug__fmt), path);
    CapErr e = jazz_engine__cap_operator_fmt("app projection path %.*s is not a correlated path in the query shape", JE_STR_ARG(d));
    jz_str_drop(&d);
    return e;
}

/* Resolve a window reference at the application-to-source boundary. Declared
 * columns use their canonical CurrentRow carrier, so a logical `user_title`
 * cannot resolve to the carrier for `title`. Derived logical fields retain
 * their explicit descriptor identity; provenance uses its metadata carrier. */
const groove_DescriptorField *JE_TERM(collect_window_source_field)(const RSource *source, const jazz_engine_NormalizedValueRef *value) {
    u32 n;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(source->row_shape.descriptor, &n);
    jz_str carrier;
    if (value->tag == JAZZ_ENGINE_NVR_SOURCE_FIELD) {
        const jz_str *field = &value->u.source_field.field;
        bool declared = false;
        JZ_VEC_FOR(const jazz_model_ColumnSchema, column, source->table_schema->columns) if (jz_str_eq(&column->name, field)) { declared = true; break; }
        if (!declared) {
            u32 idx;
            if (!groove__records__RecordDescriptor__field_index(source->row_shape.descriptor, field->ptr, field->len, &idx)) return NULL;
            return idx < n ? &fields[idx] : NULL;
        }
        carrier = F(user_column_field)(JZ_S(field));
    } else if (value->tag == JAZZ_ENGINE_NVR_PROVENANCE) {
        static const char *const names[] = {"$createdAt", "$createdBy", "$updatedAt", "$updatedBy"};
        carrier = s_lit(names[value->sub & 3]);
    } else {
        return NULL;
    }
    const groove_DescriptorField *found = NULL;
    for (u32 i = 0; i < n && !found; i++) if (fields[i].has_name && jz_str_eq(&fields[i].name, &carrier)) found = &fields[i];
    jz_str_drop(&carrier);
    return found;
}

static const jazz_engine_CollectFlatField *flat_field_by_source(const jazz_engine_CollectFlatFieldVec *fields, const jz_str *source_field) {
    JZ_VEC_FOR(const jazz_engine_CollectFlatField, f, *fields) if (f->source_field.some && jz_str_eq(&f->source_field.s, source_field)) return f;
    return NULL;
}
static const jazz_engine_CollectFlatField *flat_row_id_field(const jazz_engine_CollectFlatFieldVec *fields) {
    JZ_VEC_FOR(const jazz_engine_CollectFlatField, f, *fields) if (f->is_row_id) return f;
    return NULL;
}
static jazz_engine_CollectFlatField flat_field_new(jz_str input, jz_str output, groove_ValueType value_type, groove_ValueType output_value_type,
                                                   jazz_engine_OptStr source_field, jazz_engine_OptStr source_public_name, u8 origin) {
    jazz_engine_CollectFlatField f;
    memset(&f, 0, sizeof f);
    f.input = input;
    f.output = output;
    f.value_type = value_type;
    f.output_value_type = output_value_type;
    f.source_field = source_field;
    f.source_public_name = source_public_name;
    f.origin = origin;
    return f;
}
static jazz_engine_OptStr public_name_opt(const RSource *source, const jz_str *field) {
    jazz_engine_OptStr o = {0};
    o.some = resolved_source_public_name(source, field, &o.s);
    return o;
}

/* Keep values used to order or slice a nested collector slot in its internal
 * input row. They are deliberately not public payload fields: a path can order
 * by provenance even when its projection selects only ordinary columns. */
static CapErr retain_collect_root_value(jazz_engine_CollectLayout *layout, const jazz_engine_NormalizedValueRef *value, const RSource *source) {
    const groove_DescriptorField *descriptor_field = JE_TERM(collect_window_source_field)(source, value);
    if (!descriptor_field) return NULL;
    if (!descriptor_field->has_name) jz_panic("source window fields are named");
    const jz_str *source_field = &descriptor_field->name;
    if (flat_field_by_source(&layout->root_fields, source_field)) return NULL;
    JZ_VEC_PUSH(layout->root_fields, flat_field_new(jz_format("__collect_root_%.*s", JE_STR_ARG(*source_field)), s_clone(source_field),
                                                    groove_vt_clone(&descriptor_field->value_type), groove_vt_clone(&descriptor_field->value_type),
                                                    jazz_engine__optstr_some(s_clone(source_field)), public_name_opt(source, source_field),
                                                    JAZZ_ENGINE_CFO_SOURCE_ROW));
    return NULL;
}

static CapErr retain_collect_slot_value(jazz_engine_CollectSlotLayout *slot, const jazz_engine_NormalizedValueRef *value, const RSource *source) {
    const groove_DescriptorField *descriptor_field = JE_TERM(collect_window_source_field)(source, value);
    if (!descriptor_field) return NULL;
    if (!descriptor_field->has_name) jz_panic("source window fields are named");
    const jz_str *source_field = &descriptor_field->name;
    if (flat_field_by_source(&slot->fields, source_field)) return NULL;
    const jz_str *row = &source->row_shape.row_uuid_field;
    const jz_str *input = &slot->row_id_input;
    bool suffixed = input->len > row->len && input->ptr[input->len - row->len - 1] == '_' &&
                    memcmp(input->ptr + input->len - row->len, row->ptr, row->len) == 0;
    if (!suffixed) {
        jz_str a = dbg_str(input), b = dbg_str(row);
        CapErr e = jazz_engine__cap_runtime_fmt("collector child row-id input %.*s does not match source row-id field %.*s", JE_STR_ARG(a), JE_STR_ARG(b));
        jz_str_drop(&a);
        jz_str_drop(&b);
        return e;
    }
    u32 prefix_len = input->len - row->len - 1;
    /* Derive the unwrapped and input types exactly as selected slot fields do,
     * so the anchor and association arms agree on the outer wrapper. */
    groove_ValueType output_value_type = JE_CL(collect_unwrapped_output_type)(source, JZ_S(source_field), &descriptor_field->value_type);
    groove_ValueType value_type = JE_CL(collect_slot_input_type)(&output_value_type);
    JZ_VEC_PUSH(slot->fields, flat_field_new(jz_format("%.*s_%.*s", (int)prefix_len, input->ptr, JE_STR_ARG(*source_field)), s_clone(source_field),
                                             value_type, output_value_type, jazz_engine__optstr_some(s_clone(source_field)),
                                             public_name_opt(source, source_field), JAZZ_ENGINE_CFO_SOURCE_ROW));
    return NULL;
}

static CapErr window_key_missing(const char *fmt, const jazz_engine_NormalizedValueRef *value) {
    JE_DBG(d, nvr_debug, value);
    CapErr e = jazz_engine__cap_operator_fmt(fmt, JE_STR_ARG(d));
    jz_str_drop(&d);
    return e;
}

static CapErr collect_root_input_for_value(const jazz_engine_CollectLayout *layout, const jazz_engine_NormalizedValueRef *value, const RSource *source, jz_str *out) {
    if (value->tag == JAZZ_ENGINE_NVR_SOURCE_FIELD && jz_str_eq_cstr(&value->u.source_field.field, "__root_union_arm")) {
        const jazz_engine_CollectFlatField *c = flat_field_by_source(&layout->root_fields, &value->u.source_field.field);
        if (!c) return cap_op("collector root union-arm tie-breaker is missing");
        *out = s_clone(&c->input);
        return NULL;
    }
    const groove_DescriptorField *df = JE_TERM(collect_window_source_field)(source, value);
    const jazz_engine_CollectFlatField *c = NULL;
    if (df && df->has_name) c = flat_field_by_source(&layout->root_fields, &df->name);
    else if (value->tag == JAZZ_ENGINE_NVR_ROW_ID && value->sub == JAZZ_ENGINE_ROWID_SOURCE) c = flat_row_id_field(&layout->root_fields);
    if (!c) return window_key_missing("collector root window key %.*s is not present in the root projection", value);
    *out = s_clone(&c->input);
    return NULL;
}

static CapErr collect_slot_input_for_value(const jazz_engine_CollectSlotLayout *slot, const jazz_engine_NormalizedValueRef *value, const RSource *source, jz_str *out) {
    const groove_DescriptorField *df = JE_TERM(collect_window_source_field)(source, value);
    if (df && df->has_name) {
        const jazz_engine_CollectFlatField *c = flat_field_by_source(&slot->fields, &df->name);
        if (c) { *out = s_clone(&c->input); return NULL; }
    } else if (value->tag == JAZZ_ENGINE_NVR_ROW_ID && value->sub == JAZZ_ENGINE_ROWID_SOURCE) {
        *out = s_clone(&slot->row_id_input);
        return NULL;
    }
    return window_key_missing("collector window key %.*s is not present in the child projection", value);
}

typedef CapErr (*WindowInputFn)(void *ctx, const jazz_engine_NormalizedValueRef *value, const RSource *source, jz_str *out);
static CapErr root_input_v(void *ctx, const jazz_engine_NormalizedValueRef *value, const RSource *source, jz_str *out) { return collect_root_input_for_value(ctx, value, source, out); }
static CapErr slot_input_v(void *ctx, const jazz_engine_NormalizedValueRef *value, const RSource *source, jz_str *out) { return collect_slot_input_for_value(ctx, value, source, out); }
static CapErr window_order_cols(const jazz_engine_OrderKeyVec *keys, WindowInputFn f, void *ctx, const RSource *source, groove_TopByOrderVec *out) {
    groove_TopByOrderVec v = {0};
    JZ_VEC_FOR(const jazz_engine_OrderKey, key, *keys) {
        jz_str field;
        CapErr e = f(ctx, &key->value, source, &field);
        if (e) { order_cols_drop(&v); return e; }
        JZ_VEC_PUSH(v, key->direction == JAZZ_ENGINE_SORT_ASC ? groove__ivm__graph__TopByOrder__asc(field) : groove__ivm__graph__TopByOrder__desc(field));
    }
    *out = v;
    return NULL;
}
static CapErr window_tie_cols(const jazz_engine_NvrVec *values, WindowInputFn f, void *ctx, const RSource *source, jazz_engine_StrVec *out) {
    jazz_engine_StrVec v = {0};
    JZ_VEC_FOR(const jazz_engine_NormalizedValueRef, value, *values) {
        jz_str field;
        CapErr e = f(ctx, value, source, &field);
        if (e) { jazz_engine__strvec_drop(&v); return e; }
        JZ_VEC_PUSH(v, field);
    }
    *out = v;
    return NULL;
}

static CapErr align_collect_join_key_types(jazz_engine_CollectSlotLayoutVec *slots, const jazz_engine_AnalyzedQueryPlan *plan, const jz_btree *resolved_sources,
                                           jazz_engine_LoweringContext *request) {
    JZ_VEC_FOR(jazz_engine_CollectSlotLayout, slot, *slots) {
        const jazz_engine_CorrelatedPathPlan *path = find_correlated_path(plan, &slot->path);
        if (!path) return path_not_correlated(&slot->path);
        const jazz_engine_SourceId *parent_id = JE_PLAN(LinearRoot__source)(&path->parent.root);
        if (!parent_id) return cap_op("collector path parent must be a source");
        const jazz_engine_SourceId *child_id = JE_PLAN(LinearRoot__source)(&path->child.root);
        if (!child_id) return cap_op("collector path child must be a source");
        const RSource *parent, *child;
        JE_CAP_TRY(JE_RES(ResolvedSource__lookup)(resolved_sources, parent_id, "collector parent", &parent));
        JE_CAP_TRY(JE_RES(ResolvedSource__lookup)(resolved_sources, child_id, "collector child", &child));
        jz_str parent_key, child_key;
        JE_UR_CAP_TRY(JE_GL(lower_path_key_pair)(&path->correlation, parent_id, parent, child_id, child, request, &parent_key, &child_key));
        JZ_VEC_FOR(jazz_engine_CollectFlatField, field, slot->fields) {
            if (!field->source_field.some || !jz_str_eq(&field->source_field.s, &child_key)) continue;
            groove_ValueType payload = groove_vt_clone(groove__records__values__ValueType__non_nullable(&field->value_type));
            VT_DROP(&field->value_type);
            field->value_type = groove_vt_boxed(GROOVE_VT_NULLABLE, payload);
            break;
        }
        const groove_ValueType *parent_type = JE_GL(source_field_type)(parent, JZ_S(&parent_key));
        if (parent_type && groove__records__values__ValueType__is_array_fk(groove__records__values__ValueType__non_nullable(parent_type)) &&
            jz_str_eq(&child_key, &child->row_shape.row_uuid_field)) {
            jz_str input = jz_format("%.*s_references", JE_STR_ARG(slot->row_id_input));
            groove_ValueType value_type = groove_vt_boxed(GROOVE_VT_NULLABLE, groove_vt_boxed(GROOVE_VT_ARRAY, groove_vt_scalar(GROOVE_VT_UUID)));
            jazz_engine_OptStr none = {0};
            JZ_VEC_PUSH(slot->fields, flat_field_new(s_clone(&input), s_clone(&input), groove_vt_clone(&value_type), value_type,
                                                     jazz_engine__optstr_some(parent_key), none, JAZZ_ENGINE_CFO_DERIVED));
            jazz_engine__optstr_drop(&slot->reference_array_input);
            slot->reference_array_input = jazz_engine__optstr_some(input);
            bool ordered = false;
            JZ_VEC_FOR(const jazz_engine_LinearStep, step, path->child.steps) if (step->tag == JAZZ_ENGINE_STEP_ORDER_BY) { ordered = true; break; }
            slot->reference_order = !ordered;
        } else {
            jz_str_drop(&parent_key);
        }
        jz_str_drop(&child_key);
        JZ_VEC_FOR(const jazz_engine_LinearStep, step, path->child.steps) {
            if (step->tag == JAZZ_ENGINE_STEP_ORDER_BY) {
                JZ_VEC_FOR(const jazz_engine_OrderKey, key, step->u.order_by) JE_CAP_TRY(retain_collect_slot_value(slot, &key->value, child));
                groove_TopByOrderVec cols;
                JE_CAP_TRY(window_order_cols(&step->u.order_by, slot_input_v, slot, child, &cols));
                order_cols_drop(&slot->order_cols);
                slot->order_cols = cols;
            } else if (step->tag == JAZZ_ENGINE_STEP_SLICE) {
                JZ_VEC_FOR(const jazz_engine_NormalizedValueRef, value, step->u.slice.tie_breaker) JE_CAP_TRY(retain_collect_slot_value(slot, value, child));
                slot->offset = step->u.slice.offset;
                slot->limit = limit_of(step->u.slice.has_limit, step->u.slice.limit);
                jazz_engine_StrVec ties;
                JE_CAP_TRY(window_tie_cols(&step->u.slice.tie_breaker, slot_input_v, slot, child, &ties));
                jazz_engine__strvec_drop(&slot->tie_cols);
                slot->tie_cols = ties;
            }
        }
        if (slot->order_cols.len == 0) JZ_VEC_PUSH(slot->order_cols, groove__ivm__graph__TopByOrder__asc(s_clone(&slot->row_id_input)));
        if (slot->tie_cols.len == 0) jazz_engine__strvec_push_clone(&slot->tie_cols, &slot->row_id_input);
        JE_CAP_TRY(align_collect_join_key_types(&slot->children, plan, resolved_sources, request));
    }
    return NULL;
}

static CapErr align_collect_root_window(jazz_engine_CollectLayout *layout, const jazz_engine_AnalyzedQueryPlan *plan, const RSource *source) {
    const jazz_engine_LinearStepVec *parts[2] = {NULL, NULL};
    switch (plan->tag) {
    case JAZZ_ENGINE_AQP_LINEAR: parts[0] = &plan->u.linear.steps; break;
    case JAZZ_ENGINE_AQP_CORRELATED_PATH: parts[0] = &plan->u.correlated.parent.steps; parts[1] = &plan->u.correlated.output_steps; break;
    case JAZZ_ENGINE_AQP_UNION: parts[0] = &plan->u.union_.terminal_steps; break;
    default: break;
    }
    for (u32 p = 0; p < 2; p++) {
        if (!parts[p]) continue;
        JZ_VEC_FOR(const jazz_engine_LinearStep, step, *parts[p]) {
            if (step->tag == JAZZ_ENGINE_STEP_ORDER_BY) {
                JZ_VEC_FOR(const jazz_engine_OrderKey, key, step->u.order_by) JE_CAP_TRY(retain_collect_root_value(layout, &key->value, source));
                groove_TopByOrderVec cols;
                JE_CAP_TRY(window_order_cols(&step->u.order_by, root_input_v, layout, source, &cols));
                order_cols_drop(&layout->root_order_cols);
                layout->root_order_cols = cols;
            } else if (step->tag == JAZZ_ENGINE_STEP_SLICE) {
                JZ_VEC_FOR(const jazz_engine_NormalizedValueRef, value, step->u.slice.tie_breaker) JE_CAP_TRY(retain_collect_root_value(layout, value, source));
                layout->root_offset = step->u.slice.offset;
                layout->root_limit = limit_of(step->u.slice.has_limit, step->u.slice.limit);
                jazz_engine_StrVec ties;
                JE_CAP_TRY(window_tie_cols(&step->u.slice.tie_breaker, root_input_v, layout, source, &ties));
                jazz_engine__strvec_drop(&layout->root_tie_cols);
                layout->root_tie_cols = ties;
            }
        }
    }
    const jazz_engine_CollectFlatField *row = flat_row_id_field(&layout->root_fields);
    if (!row) jz_panic("collector root retains row id");
    if (layout->root_order_cols.len == 0) JZ_VEC_PUSH(layout->root_order_cols, groove__ivm__graph__TopByOrder__asc(s_clone(&row->input)));
    if (layout->root_tie_cols.len == 0) jazz_engine__strvec_push_clone(&layout->root_tie_cols, &row->input);
    JZ_VEC_FOR(const jz_str, occurrence, layout->root_occurrence_inputs)
        if (!jazz_engine__strvec_contains(&layout->root_tie_cols, occurrence)) jazz_engine__strvec_push_clone(&layout->root_tie_cols, occurrence);
    return NULL;
}

/* consumes visible_root */
static CapErr lower_collect_by_app_rows(groove_GraphBuilder *visible_root, const jazz_engine_AppProjectionTree *projection, const jazz_engine_AnalyzedQueryPlan *plan,
                                        const RSource *root_source, const jz_btree *resolved_sources, jazz_engine_LoweringContext *request,
                                        const jz_btree *route_fields, const jz_btree *available_fields, LoweredAppRows *out) {
    const jazz_engine_ParameterDomain *domain;
    jazz_engine_UrErr ue = JE_ARG(LoweringContext__parameter_domain)(request, &domain);
    if (ue) { groove_gb_drop(visible_root); return jazz_engine__ur_into_cap(ue); }
    jazz_engine_ParameterDomain parameter_domain;
    JE_LOW(ParameterDomain__Clone__clone)(&parameter_domain, domain);
    JE_LOW(collect_binding_source_params)(visible_root, &parameter_domain);
    jazz_engine_CollectLayout layout;
    CapErr e = JE_CL(collect_layout)(projection, plan, root_source, resolved_sources, request, route_fields, &parameter_domain, available_fields, &layout);
    JE_LOW(ParameterDomain__drop)(&parameter_domain);
    if (e) { groove_gb_drop(visible_root); return e; }
    groove_GraphBuilder *root_context = NULL;
    groove_GraphBuilderVec association_graphs = {0};
    jz_btree publication_fields = {0};
    if ((e = align_collect_root_window(&layout, plan, root_source))) goto fail;
    if ((e = align_collect_join_key_types(&layout.slots, plan, resolved_sources, request))) goto fail;
    collect_publication_fields(root_source, &layout, &publication_fields);
    bool stable = plan->tag != JAZZ_ENGINE_AQP_UNION;
    memset(out, 0, sizeof *out);
    if (layout.slots.len == 0) {
        /* A collect-all root preserves the source CurrentRow application-cell
         * wrappers. Explicit projections unwrap those cells to their declared
         * logical types. Bind that distinction here while both input and output
         * types are authoritative; consumers must never infer it from the
         * eventual descriptor's field names. */
        bool current_row = true;
        JZ_VEC_FOR(const jazz_engine_CollectFlatField, f, layout.root_fields)
            if (is_public_root_field(f) && !VT_EQ(&f->value_type, &f->output_value_type)) { current_row = false; break; }
        groove_GraphBuilder *anchor;
        e = JE_CL(collect_anchor_graph)(visible_root, root_source, &layout, stable, &anchor);
        visible_root = NULL;
        if (e) goto fail;
        bool has_window = false;
        const jazz_engine_LinearStepVec *steps = root_linear_steps(plan);
        if (steps) JZ_VEC_FOR(const jazz_engine_LinearStep, s, *steps) if (s->tag == JAZZ_ENGINE_STEP_ORDER_BY || s->tag == JAZZ_ENGINE_STEP_SLICE) { has_window = true; break; }
        if (!has_window) {
            jazz_engine_StrVec none = {0}, ties;
            groove_TopByOrderVec order;
            order_cols_clone(&order, &layout.root_order_cols);
            jazz_engine__strvec_clone(&ties, &layout.root_tie_cols);
            anchor = GB(top_by)(anchor, none, order, ties, 0, limit_of(false, 0));
        }
        const jazz_engine_CollectFlatField *row = flat_row_id_field(&layout.root_fields);
        if (!row) jz_panic("collector root retains row id");
        groove_TopByOrderVec order;
        jazz_engine_StrVec ties;
        order_cols_clone(&order, &layout.root_order_cols);
        jazz_engine__strvec_clone(&ties, &layout.root_tie_cols);
        out->graph = GB(collect_root_ordered)(anchor, root_collect_group_fields(s_clone(&row->input), &layout.root_occurrence_inputs, route_fields),
                                              collect_root_output_fields(&layout.root_fields), order, ties, layout.root_offset, layout.root_limit);
        if ((e = JE_CL(collect_output_descriptor)(&layout, &out->descriptor))) { groove_gb_drop(out->graph); goto fail; }
        out->publication_fields = publication_fields;
        /* Route parameters are retained in the collector record so the maintained
         * graph can partition results, but they are not part of the app
         * projection. Nested collectors already apply the same boundary below. */
        jazz_engine__strset_clone(&out->hidden_fields, route_fields);
        out->carrier = current_row ? JAZZ_ENGINE_CARRIER_CURRENT_ROW : JAZZ_ENGINE_CARRIER_LOGICAL;
        collect_layout_root_field_carriers(&layout, &out->field_carriers);
        collect_layout_root_public_field_names(&layout, &out->public_field_names);
        out->terminal.tag = JAZZ_ENGINE_ART_ROOT_COLLECTOR;
        JE_TERM(CollectLayout__drop)(&layout);
        return NULL;
    }
    if ((e = JE_CL(root_collect_context_graph)(groove_gb_clone(visible_root), root_source, &layout, stable, &root_context))) goto fail;
    collect_layout_root_field_carriers(&layout, &out->field_carriers);
    collect_layout_root_public_field_names(&layout, &out->public_field_names);
    JZ_VEC_FOR(const jazz_engine_CollectSlotLayout, slot, layout.slots) {
        str_u8_put(&out->field_carriers, s_clone(&slot->collection_field), JAZZ_ENGINE_CARRIER_LOGICAL);
        /* Collection field names are public query-path identities. They are
         * carried verbatim into the terminal descriptor, so no prefix-based
         * recovery is needed at this boundary. */
        str_str_put(&out->public_field_names, s_clone(&slot->collection_field), s_clone(&slot->collection_field));
    }
    jz_btree no_inherited;
    strset_init(&no_inherited);
    JZ_VEC_FOR(const jazz_engine_CollectSlotLayout, slot, layout.slots) {
        const jazz_engine_CorrelatedPathPlan *path = find_correlated_path(plan, &slot->path);
        if (!path) { e = path_not_correlated(&slot->path); break; }
        if ((e = JE_CL(lower_collect_slot_graphs)(slot, path, groove_gb_clone(root_context), root_source, root_source, &layout, &no_inherited,
                                                  resolved_sources, request, &association_graphs))) break;
    }
    if (e) goto fail_out;
    groove_GraphBuilder *anchor;
    e = JE_CL(collect_anchor_graph)(visible_root, root_source, &layout, stable, &anchor);
    visible_root = NULL;
    if (e) goto fail_out;
    groove_GraphBuilderVec inputs = {0};
    JZ_VEC_PUSH(inputs, anchor);
    jz_vec_extend(&inputs, association_graphs.ptr, association_graphs.len, sizeof(groove_GraphBuilder *));
    jz_vec_drop(&association_graphs, sizeof(groove_GraphBuilder *), NULL);
    groove_GraphBuilder *input = GB(union)(inputs);
    if ((e = JE_CL(collect_output_descriptor)(&layout, &out->descriptor))) { groove_gb_drop(input); goto fail_out; }
    const jazz_engine_CollectFlatField *row = flat_row_id_field(&layout.root_fields);
    if (!row) { groove_gb_drop(input); e = cap_rt("collector root projection did not retain the row id"); goto fail_out; }
    groove_CollectBySlotBuilderVec slot_builders = {0};
    JZ_VEC_FOR(const jazz_engine_CollectSlotLayout, slot, layout.slots) JZ_VEC_PUSH(slot_builders, JE_CL(collect_slot_builder)(slot, JZ_S(&row->input), route_fields));
    out->graph = GB(collect_by_tree_ordered)(input, root_collect_group_fields(s_clone(&row->input), &layout.root_occurrence_inputs, route_fields),
                                             collect_root_output_fields(&layout.root_fields), slot_builders, layout.root_order_cols, layout.root_tie_cols,
                                             layout.root_offset, layout.root_limit);
    memset(&layout.root_order_cols, 0, sizeof layout.root_order_cols);
    memset(&layout.root_tie_cols, 0, sizeof layout.root_tie_cols);
    hidden_source_fields(&root_source->row_shape, &out->hidden_fields);
    strset_extend_clone(&out->hidden_fields, route_fields);
    out->publication_fields = publication_fields;
    out->carrier = JAZZ_ENGINE_CARRIER_LOGICAL;
    out->terminal.tag = JAZZ_ENGINE_ART_ROOT_COLLECTOR;
    jz_btree_drop(&no_inherited);
    groove_gb_drop(root_context);
    JE_TERM(CollectLayout__drop)(&layout);
    return NULL;
fail_out:
    jz_btree_drop(&no_inherited);
    jz_btree_drop(&out->field_carriers);
    jz_btree_drop(&out->public_field_names);
fail:
    groove_gb_drop(visible_root);
    groove_gb_drop(root_context);
    graphs_drop(&association_graphs);
    jz_btree_drop(&publication_fields);
    JE_TERM(CollectLayout__drop)(&layout);
    return e;
}

/* ---------------------------------------------------------------- root join occurrence carriers */
static void typed_field_push(jazz_engine_TypedOutputFieldVec *v, jz_str name, u8 vt) {
    jazz_engine_TypedOutputField f = {name, groove_vt_scalar(vt)};
    JZ_VEC_PUSH(*v, f);
}

CapErr JE_TERM(root_join_occurrence_fields)(const jazz_engine_AnalyzedQueryPlan *plan, const jz_btree *resolved_sources,
                                            jazz_engine_LoweringContext *request, jazz_engine_TypedOutputFieldVec *out) {
    jazz_engine_TypedOutputFieldVec fields = {0};
    *out = fields;
    /* Authorization subplans are decision programs, not public row sets. Their
     * joins prove policy predicates and cannot contribute to a result address. */
    const jazz_engine_QueryProgramRequest *rq = request->request;
    if (rq->policy.tag == JAZZ_ENGINE_PCTX_AUTHORIZATION_SUBPLAN || (rq->output.has_app_rows && !rq->output.app_rows.public_terminal)) return NULL;
    if (plan->tag == JAZZ_ENGINE_AQP_UNION) {
        typed_field_push(out, s_lit("__root_union_arm"), GROOVE_VT_STRING);
        typed_field_push(out, s_lit("__root_union_row"), GROOVE_VT_UUID);
        return NULL;
    }
    const jazz_engine_LinearStepVec *steps = root_linear_steps(plan);
    if (!steps) return NULL;
    for (u32 step_index = 0; step_index < steps->len; step_index++) {
        const jazz_engine_LinearStep *step = &steps->ptr[step_index];
        if (step->tag != JAZZ_ENGINE_STEP_JOIN || step->u.join.mode != JAZZ_ENGINE_JOIN_INNER) continue;
        const jazz_engine_LinearStep *next = step_index + 1 < steps->len ? &steps->ptr[step_index + 1] : NULL;
        if (next && next->tag == JAZZ_ENGINE_STEP_PROJECT) continue;
        const jazz_engine_RelationInputPlan *right = step->u.join.right;
        if (right->tag == JAZZ_ENGINE_RIP_UNION) {
            typed_field_push(&fields, jz_format("__root_join_arm_%u", step_index), GROOVE_VT_STRING);
            typed_field_push(&fields, jz_format("__root_join_row_%u", step_index), GROOVE_VT_UUID);
            continue;
        }
        const jazz_engine_SourceId *source_id = JE_PLAN(RelationInputPlan__root_source)(right);
        if (!source_id) continue;
        const RSource *source;
        CapErr e = JE_RES(ResolvedSource__lookup)(resolved_sources, source_id, "inner join occurrence", &source);
        if (e) { jz_vec_drop(&fields, sizeof(jazz_engine_TypedOutputField), jazz_engine__tof_drop_v); return e; }
        if (!source_id->path.len || source_id->path.ptr[source_id->path.len - 1].tag != JAZZ_ENGINE_ROLE_ALIAS) continue;
        bool exposes_row_id = false;
        if (right->tag == JAZZ_ENGINE_RIP_LINEAR) {
            const jazz_engine_LinearStepVec *rs = &right->u.linear.steps;
            exposes_row_id = true;
            if (rs->len && rs->ptr[rs->len - 1].tag == JAZZ_ENGINE_STEP_PROJECT) {
                exposes_row_id = false;
                JZ_VEC_FOR(const jazz_engine_RowProjection, column, rs->ptr[rs->len - 1].u.project)
                    if (jz_str_eq(&column->output.name, &source->row_shape.row_uuid_field)) { exposes_row_id = true; break; }
            }
        }
        if (!exposes_row_id) continue;
        jz_str name = next && next->tag == JAZZ_ENGINE_STEP_JOIN
                          ? jz_format("__flat_join_source_%u_%.*s", step_index, JE_STR_ARG(source->row_shape.row_uuid_field))
                          : jz_format("__root_join_row_%u", step_index);
        typed_field_push(&fields, name, GROOVE_VT_UUID);
    }
    *out = fields;
    return NULL;
}

/* @@T2@@ */
