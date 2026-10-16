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

/* ---------------------------------------------------------------- small builders */
static groove_LiteralValue lit_bool(bool b) { groove_LiteralValue v; memset(&v, 0, sizeof v); v.tag = GROOVE_LIT_BOOL; v.u.b = b; return v; }
static groove_LiteralValue lit_value(groove_Value value) { groove_LiteralValue l; groove__ivm__op_types__LiteralValue__From_Value__from(&l, &value); return l; }
static groove_FieldIdentity fid_name(jz_str name) { groove_FieldIdentity f; memset(&f, 0, sizeof f); f.tag = GROOVE_FIELD_ID_NAME; f.name = name; return f; }
static groove_ValueType vt_nullable(groove_ValueType t) { return groove_vt_boxed(GROOVE_VT_NULLABLE, t); }
static void pf_push(groove_ProjectFieldVec *v, groove_ProjectField f) { JZ_VEC_PUSH(*v, f); }
static groove_ProjectField pf_named_c(const char *s) { return PF(named)(s_lit(s)); }
static groove_ProjectField pf_ren(jz_str source, const char *output) { return PF(renamed)(source, s_lit(output)); }
static groove_ProjectField pf_lit_str(const char *output, jz_str value) { return PF(literal)(s_lit(output), lit_string(value)); }
static jz_str pfx(const char *prefix, const char *name) { return jz_format("%s%s", prefix, name); }
static jz_str pfx_s(const char *prefix, const jz_str *name) { return jz_format("%s%.*s", prefix, JE_STR_ARG(*name)); }
static jazz_engine_StrVec sv3(const char *a, const char *b, const char *c) {
    jazz_engine_StrVec v = {0};
    jazz_engine__strvec_push_cstr(&v, a); jazz_engine__strvec_push_cstr(&v, b); jazz_engine__strvec_push_cstr(&v, c);
    return v;
}
static void strset_difference(const jz_btree *src, const jz_btree *minus, jz_btree *out) {
    strset_init(out);
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(src, f, unit) if (!jz_btree_contains(minus, f)) jazz_engine__strset_insert_clone(out, f);
}
/* CapabilityReport::explained(UnsupportedReason::Output(Box::new(key.clone())), msg) */
static CapErr cap_output(const jazz_engine_ProgramFactKey *key, const char *msg) {
    jazz_engine_UnsupportedReason r = jazz_engine__ur_new(JAZZ_ENGINE_UR_OUTPUT);
    r.output = jz_alloc(sizeof *r.output);
    JE_OUT(ProgramFactKey__Clone__clone)(r.output, key);
    return JE_RES(CapabilityReport__explained)(r, s_lit(msg));
}
static CapErr cap_output_simple(u8 tag, const char *msg) {
    jazz_engine_ProgramFactKey k = jazz_engine__fact_key_simple(tag);
    CapErr e = cap_output(&k, msg);
    JE_OUT(ProgramFactKey__drop)(&k);
    return e;
}
static CapErr cap_explained_ur(jazz_engine_UrErr gap, const char *msg) { return JE_RES(CapabilityReport__explained)(jazz_engine__ur_take(gap), s_lit(msg)); }
static const groove_DescriptorField *descriptor_field_named(const RSource *source, const jz_str *name) {
    u32 n;
    const groove_DescriptorField *d = groove__records__RecordDescriptor__fields(source->row_shape.descriptor, &n);
    for (u32 i = 0; i < n; i++) if (d[i].has_name && jz_str_eq(&d[i].name, name)) return &d[i];
    return NULL;
}
/* take the graph out of a lowered relation input and drop the rest */
static groove_GraphBuilder *lri_take_graph(jazz_engine_LoweredRelationInput *l) {
    groove_GraphBuilder *g = l->graph;
    l->graph = NULL;
    JE_GL(LoweredRelationInput__drop)(l);
    return g;
}
static groove_GraphBuilder *gb_node(groove_GraphBuilder node) {
    groove_GraphBuilder *g = jz_rc_new(sizeof(groove_GraphBuilder), groove__ivm__graph__GraphBuilder__drop_in_place);
    *g = node;
    return g;
}
static groove_GraphBuilder *gb_exact_join(u8 tag, groove_GraphBuilder *left, groove_GraphBuilder *right, groove_FieldRefVec left_on, groove_FieldRefVec right_on) {
    groove_GraphBuilder node;
    memset(&node, 0, sizeof node);
    node.tag = tag;
    node.u.join.left = left;
    node.u.join.right = right;
    node.u.join.left_on = left_on;
    node.u.join.right_on = right_on;
    node.u.join.comparison = GROOVE_CMP_EXACT;
    return gb_node(node);
}

/* ---------------------------------------------------------------- source metadata fields */
typedef struct VersionWitnessFieldRefs {
    jz_str schema_version_field, tx_time_field, tx_node_field;
    jazz_engine_OptStr branch_or_prefix_field;
} VersionWitnessFieldRefs;
static void vwf_drop(VersionWitnessFieldRefs *v) {
    jz_str_drop(&v->schema_version_field);
    jz_str_drop(&v->tx_time_field);
    jz_str_drop(&v->tx_node_field);
    jazz_engine__optstr_drop(&v->branch_or_prefix_field);
}
static const jazz_engine_SourceMetadataFields *metadata_of(const jazz_engine_SourceRowShape *row_shape, u8 requirement) {
    jazz_engine_SourceMetadataRequirement key = jazz_engine__smr_simple(requirement);
    const jazz_engine_SourceMetadataFields *m = jz_btree_get(&row_shape->metadata, &key);
    JE_RES(SourceMetadataRequirement__drop)(&key);
    return m;
}
static CapErr version_witness_fields(const jazz_engine_SourceRowShape *row_shape, VersionWitnessFieldRefs *out) {
    const jazz_engine_SourceMetadataFields *m = metadata_of(row_shape, JAZZ_ENGINE_SMR_VERSION_WITNESSES);
    if (!m || m->tag != JAZZ_ENGINE_SMF_VERSION_WITNESSES) return cap_rt("resolved source did not provide version witness fields");
    out->schema_version_field = s_clone(&m->a);
    out->tx_time_field = s_clone(&m->b);
    out->tx_node_field = s_clone(&m->c);
    memset(&out->branch_or_prefix_field, 0, sizeof out->branch_or_prefix_field);
    if (m->has_opt0) out->branch_or_prefix_field = jazz_engine__optstr_some(s_clone(&m->opt0));
    return NULL;
}
/* false = None */
static bool settle_position_field(const jazz_engine_SourceRowShape *row_shape, jz_str *out) {
    const jazz_engine_SourceMetadataFields *m = metadata_of(row_shape, JAZZ_ENGINE_SMR_SETTLE_POSITION);
    if (!m || m->tag != JAZZ_ENGINE_SMF_SETTLE_POSITION) return false;
    *out = s_clone(&m->a);
    return true;
}
static void hidden_source_fields(const jazz_engine_SourceRowShape *row_shape, jz_btree *out) {
    strset_init(out);
    const jazz_engine_SourceMetadataRequirement *k;
    const jazz_engine_SourceMetadataFields *m;
    JZ_BTREE_FOR(&row_shape->metadata, k, m) {
        jazz_engine__strset_insert_clone(out, &m->a);
        if (m->tag == JAZZ_ENGINE_SMF_VERSION_WITNESSES || m->tag == JAZZ_ENGINE_SMF_BATCH_MEMBERSHIP) {
            jazz_engine__strset_insert_clone(out, &m->b);
            jazz_engine__strset_insert_clone(out, &m->c);
        } else if (m->tag == JAZZ_ENGINE_SMF_POLICY_WITNESSES) {
            jazz_engine__strset_insert_clone(out, &m->b);
        }
        if (m->has_opt0) jazz_engine__strset_insert_clone(out, &m->opt0);
        if (m->has_opt1) jazz_engine__strset_insert_clone(out, &m->opt1);
    }
}

/* ---------------------------------------------------------------- versioned row schemas */
static jazz_engine_ResultMembershipVersionSchema content_version_fields_schema(jz_str tx_time, jz_str tx_node) {
    jazz_engine_ResultMembershipVersionSchema v;
    memset(&v, 0, sizeof v);
    v.tag = JAZZ_ENGINE_RMV_CONTENT;
    v.content.tx_time_field = tx_time;
    v.content.tx_node_field = tx_node;
    return v;
}
static jazz_engine_ResultMembershipVersionSchema content_version_schema(const VersionWitnessFieldRefs *version) {
    return content_version_fields_schema(s_clone(&version->tx_time_field), s_clone(&version->tx_node_field));
}
static CapErr versioned_row_ref_schema(const RSource *source, jazz_engine_VersionedRowRefSchema *out) {
    VersionWitnessFieldRefs version;
    JE_CAP_TRY(version_witness_fields(&source->row_shape, &version));
    memset(out, 0, sizeof *out);
    out->row.source_field = s_lit("source");
    out->row.table_field = s_lit("table");
    out->row.row_field = s_clone(&source->row_shape.row_uuid_field);
    out->has_version = true;
    out->version = content_version_schema(&version);
    out->branch_or_prefix_field = version.branch_or_prefix_field;
    version.branch_or_prefix_field.some = false;
    vwf_drop(&version);
    return NULL;
}
static CapErr prefixed_versioned_row_ref_schema(const RSource *source, const char *prefix, jazz_engine_VersionedRowRefSchema *out) {
    VersionWitnessFieldRefs version;
    JE_CAP_TRY(version_witness_fields(&source->row_shape, &version));
    memset(out, 0, sizeof *out);
    out->row.source_field = jz_format("%s_source", prefix);
    out->row.table_field = jz_format("%s_table", prefix);
    out->row.row_field = jz_format("%s_row", prefix);
    out->has_version = true;
    out->version = content_version_fields_schema(jz_format("%s_tx_time", prefix), jz_format("%s_tx_node_id", prefix));
    if (version.branch_or_prefix_field.some) out->branch_or_prefix_field = jazz_engine__optstr_some(jz_format("%s_branch_or_prefix", prefix));
    vwf_drop(&version);
    return NULL;
}
static void version_witness_schema(const jazz_engine_SourceId *source_id, const RSource *source, const VersionWitnessFieldRefs *version,
                                   jazz_engine_VersionWitnessSchema *out) {
    memset(out, 0, sizeof *out);
    JE_IN(SourceId__program_source_id)(source_id, &out->source);
    out->descriptor = source->row_shape.descriptor;
    out->identity.table_field = s_lit("table_name");
    out->identity.row_field = s_clone(&source->row_shape.row_uuid_field);
    out->identity.tx_time_field = s_lit("tx_time");
    out->identity.tx_node_field = s_lit("tx_node_id");
    jazz_engine__optstr_clone(&out->identity.branch_or_prefix_field, &version->branch_or_prefix_field);
    out->identity.schema_field = s_lit("schema_version");
    out->identity.layer_field = s_lit("layer");
    out->created_by_field = s_lit("created_by");
    out->created_at_field = s_lit("created_at");
    out->updated_by_field = s_lit("updated_by");
    out->updated_at_field = s_lit("updated_at");
    out->authored_columns_field = s_lit("authored_columns");
    out->deletion_field = s_lit("_deletion");
    jz_btree_init(&out->user_fields, &jazz_engine__STR_STR_MAP_DESC);
    JZ_VEC_FOR(const jazz_model_ColumnSchema, column, source->table_schema->columns)
        str_str_put(&out->user_fields, s_clone(&column->name), F(table_user_column_field)(JZ_S(&source->table_schema->name), JZ_S(&column->name)));
}

static CapErr relation_edge_schema(const jazz_engine_AnalyzedQueryPlan *plan, const RSource *root_source, const jz_btree *resolved_sources,
                                   jazz_engine_RelationEdgeSchema *out) {
    memset(out, 0, sizeof *out);
    const RSource *source = root_source, *target;
    if (plan->tag == JAZZ_ENGINE_AQP_CORRELATED_PATH) {
        const RSource *child;
        JE_CAP_TRY(JE_RES(ResolvedSource__lookup)(resolved_sources, &plan->u.correlated.path.child, "path child", &child));
        JE_CAP_TRY(prefixed_versioned_row_ref_schema(root_source, "source", &out->source));
        CapErr e = prefixed_versioned_row_ref_schema(child, "target", &out->target);
        if (e) { JE_SCH(VersionedRowRefSchema__drop)(&out->source); return e; }
        out->path_field = s_lit("path");
        out->kind_field = s_lit("kind");
        out->role_field = jazz_engine__optstr_some(s_lit("role"));
        return NULL;
    }
    if (plan->tag != JAZZ_ENGINE_AQP_RECURSIVE_RELATION)
        return cap_output_simple(JAZZ_ENGINE_FACT_RELATION_EDGES, "relation edge facts require a path or recursive relation node");
    const jazz_engine_RecursiveRelationPlan *relation = &plan->u.recursive;
    const jazz_engine_SourceId *step_source = JE_PLAN(LinearRoot__source)(&relation->step.root);
    if (!step_source) step_source = JE_PLAN(first_step_source)(relation->step.steps.ptr, relation->step.steps.len);
    if (!step_source) return cap_rt("recursive step source was not resolved");
    JE_CAP_TRY(JE_RES(ResolvedSource__lookup)(resolved_sources, step_source, "recursive step", &target));
    JE_CAP_TRY(versioned_row_ref_schema(source, &out->source));
    CapErr e = versioned_row_ref_schema(target, &out->target);
    if (e) { JE_SCH(VersionedRowRefSchema__drop)(&out->source); return e; }
    out->path_field = s_lit("path");
    out->kind_field = s_lit("kind");
    out->depth_field = jazz_engine__optstr_some(s_lit("depth"));
    out->role_field = jazz_engine__optstr_some(s_lit("role"));
    return NULL;
}

static CapErr path_correlation_coverage_schema(const jazz_engine_AnalyzedQueryPlan *plan, const RSource *root_source,
                                               jazz_engine_PathCorrelationCoverageSchema *out) {
    memset(out, 0, sizeof *out);
    if (plan->tag != JAZZ_ENGINE_AQP_CORRELATED_PATH && plan->tag != JAZZ_ENGINE_AQP_RECURSIVE_RELATION)
        return cap_output_simple(JAZZ_ENGINE_FACT_PATH_CORRELATION_COVERAGE,
                                 "path correlation coverage facts require a path or recursive relation node");
    JE_CAP_TRY(versioned_row_ref_schema(root_source, &out->parent));
    out->path_field = s_lit("path");
    if (plan->tag == JAZZ_ENGINE_AQP_CORRELATED_PATH) {
        out->correlation_field = s_lit("correlation");
        if (plan->u.correlated.requirement == JAZZ_ENGINE_CORR_MATCH_CARDINALITY) out->expected_count_field = jazz_engine__optstr_some(s_lit("expected_count"));
    } else {
        out->correlation_field = s_lit("frontier");
    }
    out->readable_count_field = s_lit("readable_count");
    out->coverage_state_field = s_lit("coverage_state");
    return NULL;
}
/* drop a schema through its ProgramFactSchema variant */
static void fact_schema_drop_as(u8 tag, void *schema, usize size) {
    jazz_engine_ProgramFactSchema s;
    memset(&s, 0, sizeof s);
    s.tag = tag;
    memcpy(&s.u, schema, size);
    JE_SCH(ProgramFactSchema__drop)(&s);
}

/* ---------------------------------------------------------------- witness field lists */
/* Witness field names identify compiler-owned source carriers, including
 * metadata whose spelling can collide with an application column identity. */
static void bind_witness_carrier_fields(groove_ProjectFieldVec *fields) {
    JZ_VEC_FOR(groove_ProjectField, field, *fields) {
        groove_FieldRef *source = NULL;
        switch (field->expression.tag) {
        case GROOVE_PEXPR_FIELD: case GROOVE_PEXPR_NULLABLE: case GROOVE_PEXPR_NULLABLE_FLAT: source = &field->expression.u.field; break;
        case GROOVE_PEXPR_ENUM_TAG_REMAP: case GROOVE_PEXPR_ENUM_REMAP: case GROOVE_PEXPR_RECURSIVE_ENUM_REMAP: source = &field->expression.u.remap.source; break;
        default: break;
        }
        if (source && source->tag == GROOVE_FIELDREF_NAME) source->tag = GROOVE_FIELDREF_STORED_NAME;
    }
}
static void witness_head_fields(groove_ProjectFieldVec *fields, const RSource *source, const char *event_kind) {
    pf_push(fields, pf_lit_str("event_kind", s_lit(event_kind)));
    pf_push(fields, pf_lit_str("table_name", s_clone(&source->table_schema->name)));
}

static CapErr prefixed_version_witness_fields_for_tagged_rows(const RSource *source, const char *event_kind, const char *prefix, groove_ProjectFieldVec *out) {
    if (!source->has_content_version) return cap_rt("resolved source did not provide content version source");
    VersionWitnessFieldRefs version;
    JE_CAP_TRY(version_witness_fields(&source->row_shape, &version));
    groove_ProjectFieldVec fields = {0};
    witness_head_fields(&fields, source, event_kind);
    pf_push(&fields, pf_ren(pfx(prefix, "row_uuid"), "row_uuid"));
    pf_push(&fields, pf_ren(pfx(prefix, "tx_time"), "content_tx_time"));
    pf_push(&fields, pf_ren(pfx(prefix, "tx_node_id"), "content_tx_node_id"));
    pf_push(&fields, pf_ren(pfx(prefix, "tx_time"), "tx_time"));
    pf_push(&fields, pf_ren(pfx(prefix, "tx_node_id"), "tx_node_id"));
    pf_push(&fields, pf_ren(pfx(prefix, "schema_version"), "schema_version"));
    pf_push(&fields, pf_ren(pfx(prefix, "authored_columns"), "authored_columns"));
    pf_push(&fields, pf_ren(pfx(prefix, "created_by"), "created_by"));
    pf_push(&fields, pf_ren(pfx(prefix, "created_at"), "created_at"));
    pf_push(&fields, pf_ren(pfx(prefix, "updated_by"), "updated_by"));
    pf_push(&fields, pf_ren(pfx(prefix, "updated_at"), "updated_at"));
    pf_push(&fields, PF(null_typed)(s_lit("_deletion"), vt_nullable(groove_vt_scalar(GROOVE_VT_U8))));
    /* History storage carries an authored cell at its native type, while an
     * inline/current source may already carry the outer optional-cell wrapper.
     * A version witness has one stable contract in both cases: every user field
     * is Nullable(column type). Flatten an existing wrapper or add exactly one. */
    JZ_VEC_FOR(const jazz_model_ColumnSchema, column, source->table_schema->columns) {
        jz_str user = F(user_column_field)(JZ_S(&column->name));
        pf_push(&fields, PF(nullable_flat)(pfx_s(prefix, &user), F(table_user_column_field)(JZ_S(&source->table_schema->name), JZ_S(&column->name))));
        jz_str_drop(&user);
    }
    if (version.branch_or_prefix_field.some)
        pf_push(&fields, PF(renamed)(pfx_s(prefix, &version.branch_or_prefix_field.s), s_clone(&version.branch_or_prefix_field.s)));
    vwf_drop(&version);
    bind_witness_carrier_fields(&fields);
    *out = fields;
    return NULL;
}
static CapErr unprefixed_version_witness_fields_for_tagged_rows(const RSource *source, const char *event_kind, groove_ProjectFieldVec *out) {
    return prefixed_version_witness_fields_for_tagged_rows(source, event_kind, "", out);
}

static CapErr inline_version_witness_fields_for_tagged_rows(const RSource *source, const char *event_kind, groove_ProjectFieldVec *out) {
    VersionWitnessFieldRefs version;
    JE_CAP_TRY(version_witness_fields(&source->row_shape, &version));
    groove_ProjectFieldVec fields = {0};
    witness_head_fields(&fields, source, event_kind);
    pf_push(&fields, pf_ren(s_clone(&source->row_shape.row_uuid_field), "row_uuid"));
    pf_push(&fields, pf_ren(s_clone(&version.tx_time_field), "content_tx_time"));
    pf_push(&fields, pf_ren(s_clone(&version.tx_node_field), "content_tx_node_id"));
    pf_push(&fields, pf_ren(s_clone(&version.tx_time_field), "tx_time"));
    pf_push(&fields, pf_ren(s_clone(&version.tx_node_field), "tx_node_id"));
    pf_push(&fields, pf_ren(s_clone(&version.schema_version_field), "schema_version"));
    pf_push(&fields, pf_named_c("authored_columns"));
    pf_push(&fields, pf_named_c("created_by"));
    pf_push(&fields, pf_named_c("created_at"));
    pf_push(&fields, pf_named_c("updated_by"));
    pf_push(&fields, pf_named_c("updated_at"));
    pf_push(&fields, PF(null_typed)(s_lit("_deletion"), vt_nullable(groove_vt_scalar(GROOVE_VT_U8))));
    /* CoveredInput sources and ordinary current sources can differ only in
     * whether a missing authored cell has already been wrapped. */
    JZ_VEC_FOR(const jazz_model_ColumnSchema, column, source->table_schema->columns)
        pf_push(&fields, PF(nullable_flat)(F(user_column_field)(JZ_S(&column->name)), F(table_user_column_field)(JZ_S(&source->table_schema->name), JZ_S(&column->name))));
    if (version.branch_or_prefix_field.some) pf_push(&fields, PF(named)(s_clone(&version.branch_or_prefix_field.s)));
    vwf_drop(&version);
    bind_witness_carrier_fields(&fields);
    *out = fields;
    return NULL;
}

static CapErr deletion_witness_fields_for_tagged_rows(const RSource *source, const char *event_kind, groove_ProjectFieldVec *out) {
    groove_ProjectFieldVec fields = {0};
    witness_head_fields(&fields, source, event_kind);
    pf_push(&fields, PF(named)(s_clone(&source->row_shape.row_uuid_field)));
    pf_push(&fields, pf_ren(s_lit("tx_time"), "content_tx_time"));
    pf_push(&fields, pf_ren(s_lit("tx_node_id"), "content_tx_node_id"));
    pf_push(&fields, pf_named_c("tx_time"));
    pf_push(&fields, pf_named_c("tx_node_id"));
    pf_push(&fields, pf_named_c("schema_version"));
    pf_push(&fields, PF(null_typed)(s_lit("authored_columns"), vt_nullable(groove_vt_boxed(GROOVE_VT_ARRAY, groove_vt_scalar(GROOVE_VT_U64)))));
    pf_push(&fields, pf_named_c("created_by"));
    pf_push(&fields, pf_named_c("created_at"));
    pf_push(&fields, pf_named_c("updated_by"));
    pf_push(&fields, pf_named_c("updated_at"));
    pf_push(&fields, PF(nullable)(s_lit("_deletion"), s_lit("_deletion")));
    JZ_VEC_FOR(const jazz_model_ColumnSchema, column, source->table_schema->columns)
        pf_push(&fields, PF(null_typed)(F(table_user_column_field)(JZ_S(&source->table_schema->name), JZ_S(&column->name)), vt_nullable(groove_vt_clone(&column->column_type))));
    VersionWitnessFieldRefs version;
    CapErr e = version_witness_fields(&source->row_shape, &version);
    if (e) { pfvec_drop(&fields); return e; }
    if (version.branch_or_prefix_field.some) pf_push(&fields, PF(named)(s_clone(&version.branch_or_prefix_field.s)));
    vwf_drop(&version);
    bind_witness_carrier_fields(&fields);
    *out = fields;
    return NULL;
}

/* ---------------------------------------------------------------- aggregate schemas */
static groove_ValueType aggregate_unwrapped_input_type(groove_ValueType value_type) {
    if (value_type.tag != GROOVE_VT_NULLABLE) return value_type;
    groove_ValueType inner = *value_type.u.inner;
    jz_free(value_type.u.inner);
    return inner;
}
static CapErr aggregate_source_field_name(const jazz_engine_NormalizedValueRef *value, const RSource *source, jz_str *out) {
    if (value->tag == JAZZ_ENGINE_NVR_SOURCE_FIELD && sid_eq(&value->u.source_field.source, &source->row_shape.source)) {
        JE_UR_CAP_TRY(JE_GL(require_source_field)(source, JZ_S(&value->u.source_field.field), out));
        return NULL;
    }
    if (value->tag == JAZZ_ENGINE_NVR_ROW_ID && value->sub == JAZZ_ENGINE_ROWID_SOURCE && sid_eq(&value->u.source, &source->row_shape.source)) {
        *out = s_clone(&source->row_shape.row_uuid_field);
        return NULL;
    }
    return cap_op("aggregate group keys must be root source fields");
}
static CapErr missing_descriptor_field(const char *fmt, const jz_str *field) {
    jz_str q = dbg_str(field);
    CapErr e = jazz_engine__cap_runtime_fmt(fmt, JE_STR_ARG(q));
    jz_str_drop(&q);
    return e;
}
static CapErr aggregate_output_value_type(const jazz_engine_AggregateExpr *output, const RSource *source, groove_ValueType *out) {
    switch (output->function) {
    case JAZZ_ENGINE_AGG_COUNT: *out = groove_vt_scalar(GROOVE_VT_U64); return NULL;
    case JAZZ_ENGINE_AGG_AVG: *out = vt_nullable(groove_vt_scalar(GROOVE_VT_F64)); return NULL;
    default: break;
    }
    if (!output->has_input) return cap_op("aggregate input is required for sum/min/max");
    jz_str field;
    JE_CAP_TRY(aggregate_source_field_name(&output->input, source, &field));
    /* aggregate_carrier_field_type */
    const groove_DescriptorField *df = descriptor_field_named(source, &field);
    if (!df) {
        CapErr e = missing_descriptor_field("aggregate input field %.*s is missing from resolved descriptor", &field);
        jz_str_drop(&field);
        return e;
    }
    jz_str_drop(&field);
    groove_ValueType value_type = aggregate_unwrapped_input_type(groove_vt_clone(&df->value_type));
    *out = value_type.tag == GROOVE_VT_NULLABLE ? value_type : vt_nullable(value_type);
    return NULL;
}
static void descriptor_fields_drop(groove_DescriptorFieldVec *v) { jz_vec_drop(v, sizeof(groove_DescriptorField), jazz_engine__descriptor_field_drop_v); }

static CapErr aggregate_app_row_descriptor(const jazz_engine_AnalyzedQueryPlan *plan, const RSource *source, groove_RecordDescriptor *out) {
    const jazz_engine_NvrVec *group_by;
    const jazz_engine_AggregateExprVec *outputs;
    if (!JE_PLAN(root_aggregate_step)(plan, &group_by, &outputs)) return cap_rt("aggregate app row descriptor requested for non-aggregate plan");
    groove_DescriptorFieldVec fields = {0};
    CapErr e = NULL;
    JZ_VEC_FOR(const jazz_engine_NormalizedValueRef, value, *group_by) {
        jz_str field;
        if ((e = aggregate_source_field_name(value, source, &field))) goto fail;
        /* aggregate_source_field_name has already selected a physical carrier. */
        const groove_DescriptorField *df = descriptor_field_named(source, &field);
        if (!df) { e = missing_descriptor_field("aggregate group field %.*s is missing from resolved descriptor", &field); jz_str_drop(&field); goto fail; }
        groove_DescriptorField f;
        memset(&f, 0, sizeof f);
        f.has_name = true;
        f.name = field;
        /* lower_aggregate removes the source cell-presence wrapper once before
         * grouping. Its output schema must describe that graph. */
        f.value_type = aggregate_unwrapped_input_type(groove_vt_clone(&df->value_type));
        f.has_identity = df->has_identity;
        if (df->has_identity) groove__records__FieldIdentity__Clone__clone(&f.identity, &df->identity);
        JZ_VEC_PUSH(fields, f);
    }
    JZ_VEC_FOR(const jazz_engine_AggregateExpr, output, *outputs) {
        groove_DescriptorField f;
        memset(&f, 0, sizeof f);
        if ((e = aggregate_output_value_type(output, source, &f.value_type))) goto fail;
        f.has_name = true;
        f.name = F(aggregate_output_field)(JZ_S(&output->output.name));
        f.has_identity = true;
        f.identity = fid_name(F(aggregate_output_field)(JZ_S(&output->output.name)));
        JZ_VEC_PUSH(fields, f);
    }
    *out = groove__records__RecordDescriptor__new_with_fields(fields.ptr, fields.len);
    jz_free(fields.ptr);
    return NULL;
fail:
    descriptor_fields_drop(&fields);
    return e;
}

static CapErr aggregate_group_name(const jazz_engine_NormalizedValueRef *group, const RSource *source, const char *missing, jz_str *out) {
    jz_str carrier;
    JE_CAP_TRY(aggregate_source_field_name(group, source, &carrier));
    if (group->tag == JAZZ_ENGINE_NVR_ROW_ID) { jz_str_drop(&carrier); *out = s_lit("id"); return NULL; }
    bool found = resolved_source_public_name(source, &carrier, out);
    jz_str_drop(&carrier);
    return found ? NULL : cap_rt(missing);
}

/* consumes routing_param_fields */
static CapErr aggregate_result_schema(const jazz_engine_AnalyzedQueryPlan *plan, const RSource *source, jz_btree routing_param_fields,
                                      jazz_engine_AggregateResultSchema *out) {
    memset(out, 0, sizeof *out);
    out->routing_param_fields = routing_param_fields;
    const jazz_engine_NvrVec *group_by;
    const jazz_engine_AggregateExprVec *outputs;
    CapErr e = NULL;
    if (!JE_PLAN(root_aggregate_step)(plan, &group_by, &outputs)) { e = cap_rt("aggregate result schema requested for non-aggregate plan"); goto fail; }
    /* Use the same typed fields as the emitted app-row descriptor. Carrier
     * spellings are diagnostic/output metadata, never a lookup identity. */
    groove_RecordDescriptor descriptor;
    if ((e = aggregate_app_row_descriptor(plan, source, &descriptor))) goto fail;
    u32 n;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(descriptor, &n);
    u32 split = group_by->len < n ? group_by->len : n;
    out->synthetic.table_field = s_lit("table_name");
    out->synthetic.row_field = s_lit("synthetic_row");
    out->synthetic.replacement_field = s_lit("synthetic_replacement");
    jazz_engine__strset_clone(&out->synthetic.routing_param_fields, &out->routing_param_fields);
    for (u32 i = 0; i < n; i++) {
        groove_DescriptorField f;
        groove__records__DescriptorField__Clone__clone(&f, &fields[i]);
        if (i < split) JZ_VEC_PUSH(out->group_key_fields, f);
        else JZ_VEC_PUSH(out->value_fields, f);
    }
    JZ_VEC_FOR(const jazz_engine_NormalizedValueRef, group, *group_by) {
        jz_str name;
        if ((e = aggregate_group_name(group, source, "aggregate group has no declared logical name", &name))) goto fail;
        JZ_VEC_PUSH(out->group_names, name);
    }
    JZ_VEC_FOR(const jazz_engine_AggregateExpr, output, *outputs) jazz_engine__strvec_push_clone(&out->value_names, &output->output.name);
    return NULL;
fail:
    JE_SCH(AggregateResultSchema__drop)(out);
    return e;
}

/* consumes routing_param_fields */
static CapErr aggregate_result_membership_fields(const jazz_engine_AnalyzedQueryPlan *plan, const RSource *source, jz_btree routing_param_fields,
                                                 groove_ProjectFieldVec *out) {
    const jazz_engine_NvrVec *group_by;
    const jazz_engine_AggregateExprVec *outputs;
    groove_ProjectFieldVec fields = {0};
    CapErr e = NULL;
    if (!JE_PLAN(root_aggregate_step)(plan, &group_by, &outputs)) { e = cap_rt("aggregate result fields requested for non-aggregate plan"); goto fail; }
    if (group_by->len > 1) { e = cap_op("multi-column aggregate group result identity is not lowered yet"); goto fail; }
    /* The synthetic table is only a protocol label. Aggregate identity is the
     * group-key value below (or the fixed global token), never a source-table
     * name or an aggregate value. */
    pf_push(&fields, pf_lit_str("table_name", s_lit("aggregate_result")));
    if (group_by->len) {
        jz_str carrier;
        if ((e = aggregate_source_field_name(&group_by->ptr[0], source, &carrier))) goto fail;
        groove_ProjectField f;
        memset(&f, 0, sizeof f);
        f.expression.tag = GROOVE_PEXPR_FIELD;
        f.expression.u.field = groove__ivm__graph__FieldRef__stored_name(carrier);
        f.output_name = s_lit("synthetic_row");
        f.output_identity = fid_name(s_lit("synthetic_row"));
        pf_push(&fields, f);
    } else {
        pf_push(&fields, pf_lit_str("synthetic_row", s_lit("global")));
    }
    JZ_VEC_FOR(const jazz_engine_NormalizedValueRef, group, *group_by) {
        jz_str field;
        if ((e = aggregate_source_field_name(group, source, &field))) goto fail;
        const groove_DescriptorField *df = descriptor_field_named(source, &field);
        groove_ProjectField f;
        memset(&f, 0, sizeof f);
        if (df && df->has_identity) groove__records__FieldIdentity__Clone__clone(&f.output_identity, &df->identity);
        else f.output_identity = fid_name(s_clone(&field));
        f.expression.tag = GROOVE_PEXPR_FIELD;
        f.expression.u.field = groove__ivm__graph__FieldRef__stored_name(s_clone(&field));
        f.output_name = field;
        pf_push(&fields, f);
    }
    JZ_VEC_FOR(const jazz_engine_AggregateExpr, output, *outputs) {
        groove_ProjectField f;
        memset(&f, 0, sizeof f);
        f.expression.tag = GROOVE_PEXPR_FIELD;
        f.expression.u.field = groove__ivm__graph__FieldRef__stored_name(F(aggregate_output_field)(JZ_S(&output->output.name)));
        f.output_name = F(aggregate_output_field)(JZ_S(&output->output.name));
        f.output_identity = fid_name(F(aggregate_output_field)(JZ_S(&output->output.name)));
        pf_push(&fields, f);
    }
    const jz_str *r; const void *unit;
    JZ_BTREE_FOR(&routing_param_fields, r, unit) pf_push(&fields, PF(named)(s_clone(r)));
    jz_btree_drop(&routing_param_fields);
    *out = fields;
    return NULL;
fail:
    jz_btree_drop(&routing_param_fields);
    pfvec_drop(&fields);
    return e;
}

/* ---------------------------------------------------------------- result membership terminal */
/* consumes routing_param_fields */
static CapErr result_membership_fields(const RSource *source, jz_btree routing_param_fields, const jazz_engine_TypedOutputFieldVec *payload_fields,
                                       const jazz_engine_StrVec *occurrence_id_fields, bool include_settle_position, groove_ProjectFieldVec *out) {
    VersionWitnessFieldRefs version;
    CapErr e = version_witness_fields(&source->row_shape, &version);
    if (e) { jz_btree_drop(&routing_param_fields); return e; }
    jz_str settle;
    bool has_settle = include_settle_position && settle_position_field(&source->row_shape, &settle);
    groove_ProjectFieldVec fields = {0};
    pf_push(&fields, pf_lit_str("event_kind", s_lit("result_current")));
    pf_push(&fields, pf_lit_str("table_name", s_clone(&source->table_schema->name)));
    pf_push(&fields, PF(named)(s_clone(&source->row_shape.row_uuid_field)));
    pf_push(&fields, pf_ren(version.tx_time_field, "content_tx_time"));
    pf_push(&fields, pf_ren(version.tx_node_field, "content_tx_node_id"));
    memset(&version.tx_time_field, 0, sizeof(jz_str));
    memset(&version.tx_node_field, 0, sizeof(jz_str));
    if (version.branch_or_prefix_field.some) { pf_push(&fields, PF(named)(version.branch_or_prefix_field.s)); version.branch_or_prefix_field.some = false; }
    vwf_drop(&version);
    JZ_VEC_FOR(const jz_str, field, *occurrence_id_fields)
        if (!jz_str_eq(field, &source->row_shape.row_uuid_field)) pf_push(&fields, PF(named)(s_clone(field)));
    if (has_settle) pf_push(&fields, pf_ren(settle, "settle_position"));
    else pf_push(&fields, PF(null_typed)(s_lit("settle_position"), vt_nullable(groove_vt_scalar(GROOVE_VT_U64))));
    const jz_str *r; const void *unit;
    JZ_BTREE_FOR(&routing_param_fields, r, unit) pf_push(&fields, PF(named)(s_clone(r)));
    jz_btree_drop(&routing_param_fields);
    /* The row identity is already the terminal's authoritative root carrier.
     * Branch payloads also list it among their source cells. */
    JZ_VEC_FOR(const jazz_engine_TypedOutputField, field, *payload_fields)
        if (!jz_str_eq(&field->name, &source->row_shape.row_uuid_field)) pf_push(&fields, PF(named)(s_clone(&field->name)));
    *out = fields;
    return NULL;
}

/* ---------------------------------------------------------------- source projections */
groove_ProjectFieldVec JE_TERM(project_source_fields_from_prefix_rewrapping_nullable)(const RSource *source, const char *prefix, const jz_str *nullable_field) {
    groove_ProjectFieldVec v = {0};
    u32 n;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(source->row_shape.descriptor, &n);
    for (u32 i = 0; i < n; i++) {
        const groove_DescriptorField *field = &fields[i];
        if (!field->has_name) continue;
        const jz_str *name = &field->name;
        groove_FieldRef source_field = groove__ivm__graph__FieldRef__stored_name(pfx_s(prefix, name));
        /* This projection restores a source row after joins or policy filtering.
         * Retain its source identity, rather than turning its private carrier
         * spelling into a new logical application name. */
        groove_ProjectField f;
        memset(&f, 0, sizeof f);
        f.expression.tag = nullable_field && jz_str_eq(nullable_field, name) ? GROOVE_PEXPR_NULLABLE : GROOVE_PEXPR_FIELD;
        f.expression.u.field = source_field;
        f.output_name = s_clone(name);
        if (field->has_identity) groove__records__FieldIdentity__Clone__clone(&f.output_identity, &field->identity);
        else f.output_identity = fid_name(s_clone(name));
        pf_push(&v, f);
    }
    return v;
}
groove_ProjectFieldVec JE_TERM(project_source_fields_from_prefix)(const RSource *source, const char *prefix) {
    return JE_TERM(project_source_fields_from_prefix_rewrapping_nullable)(source, prefix, NULL);
}
groove_ProjectFieldVec JE_TERM(project_source_fields_with_routes_from_prefix)(const RSource *source, const char *prefix, const jz_btree *route_fields) {
    groove_ProjectFieldVec fields = JE_TERM(project_source_fields_from_prefix)(source, prefix);
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(route_fields, f, unit) pf_push(&fields, PF(renamed)(pfx_s(prefix, f), s_clone(f)));
    return fields;
}
groove_ProjectFieldVec JE_TERM(project_source_fields_with_routes)(const RSource *source, const jz_btree *route_fields) {
    return JE_TERM(project_source_fields_with_routes_from_prefix)(source, "", route_fields);
}
static void source_terminal_route_fields(const RSource *source, const jz_btree *root_route_fields, jz_btree *out) {
    strset_intersect(&source->routing_fields, root_route_fields, out);
}
static void terminal_route_fields_for_source(const jazz_engine_SourceId *source_id, const RSource *source, const jazz_engine_SourceId *root_source,
                                             const jz_btree *root_route_fields, const jz_btree *root_terminal_route_fields, jz_btree *out) {
    if (sid_eq(source_id, root_source)) jazz_engine__strset_clone(out, root_terminal_route_fields);
    else source_terminal_route_fields(source, root_route_fields, out);
}

/* ---------------------------------------------------------------- version witness graphs */
/* Recover source-shaped rows from a visible relation which may be a flat joined
 * output. Keep the exact version and prepared-binding route: looking up only
 * the row UUID would allow a different version or route to contribute. */
CapErr JE_TERM(source_rows_for_visible_graph)(const RSource *source, groove_GraphBuilder *visible, const jz_btree *route_fields, groove_GraphBuilder **out) {
    VersionWitnessFieldRefs version;
    CapErr e = version_witness_fields(&source->row_shape, &version);
    if (e) { groove_gb_drop(visible); return e; }
    jazz_engine_StrVec keys = jazz_engine__strvec_one(s_clone(&source->row_shape.row_uuid_field));
    JZ_VEC_PUSH(keys, version.tx_time_field);
    JZ_VEC_PUSH(keys, version.tx_node_field);
    jz_str_drop(&version.schema_version_field);
    jazz_engine__optstr_drop(&version.branch_or_prefix_field);
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(&source->routing_fields, f, unit) if (jz_btree_contains(route_fields, f)) jazz_engine__strvec_push_clone(&keys, f);
    jazz_engine_StrVec keys2;
    jazz_engine__strvec_clone(&keys2, &keys);
    if (route_fields->len == 0) {
        *out = GB(semi_join)(groove_gb_clone(source->graph), visible, keys2, keys);
        return NULL;
    }
    /* A joined table's policy may introduce a route absent from the root source.
     * Recover the source row by exact version and shared routes, retaining every
     * authority route from the admitted visible relation. */
    groove_ProjectFieldVec fields = JE_TERM(project_source_fields_from_prefix)(source, JAZZ_ENGINE_LEFT_JOIN_PREFIX);
    JZ_BTREE_FOR(route_fields, f, unit) pf_push(&fields, PF(renamed)(F(right_field)(JZ_S(f)), s_clone(f)));
    *out = GB(project_fields)(GB(join)(groove_gb_clone(source->graph), visible, keys2, keys), fields);
    return NULL;
}

/* Attach immutable version evidence to rows that have already passed the
 * authority program's visibility graph. A covered-input publication must not
 * re-open the physical source and bypass policy/filter selection.
 * consumes visible_graph */
static CapErr content_version_witness_graph_from_visible_graph(const RSource *source, groove_GraphBuilder *visible_graph, const char *event_kind,
                                                               const jz_btree *routing_param_fields, groove_GraphBuilder **out) {
    groove_GraphBuilder *witness_source;
    groove_ProjectFieldVec witness_fields;
    CapErr e;
    if (source->has_content_version) {
        witness_source = groove_gb_clone(source->content_version.graph);
        e = unprefixed_version_witness_fields_for_tagged_rows(source, event_kind, &witness_fields);
    } else {
        witness_source = groove_gb_clone(source->graph);
        e = inline_version_witness_fields_for_tagged_rows(source, event_kind, &witness_fields);
    }
    if (e) { groove_gb_drop(witness_source); groove_gb_drop(visible_graph); return e; }
    /* Resolve selection against the raw carrier before expanding it into a
     * terminal witness (which duplicates metadata and wraps authored cells). */
    static const char *const key_names[3] = {"row_uuid", "tx_time", "tx_node_id"};
    groove_FieldRefVec witness_keys = {0};
    for (u32 k = 0; k < 3; k++) {
        const groove_ProjectField *found = NULL;
        JZ_VEC_FOR(const groove_ProjectField, field, witness_fields) if (jz_str_eq_cstr(&field->output_name, key_names[k])) { found = field; break; }
        if (!found) jz_panic("witness projection declares its exact version key");
        if (found->expression.tag != GROOVE_PEXPR_FIELD) jz_panic("witness identity is a source field");
        groove_FieldRef r;
        groove__ivm__graph__FieldRef__Clone__clone(&r, &found->expression.u.field);
        JZ_VEC_PUSH(witness_keys, r);
    }
    VersionWitnessFieldRefs version;
    if ((e = version_witness_fields(&source->row_shape, &version))) {
        jz_vec_drop(&witness_keys, sizeof(groove_FieldRef), NULL);
        groove_gb_drop(witness_source); groove_gb_drop(visible_graph); pfvec_drop(&witness_fields);
        return e;
    }
    groove_FieldRefVec visible_keys = {0};
    JZ_VEC_PUSH(visible_keys, groove__ivm__graph__FieldRef__name(s_clone(&source->row_shape.row_uuid_field)));
    JZ_VEC_PUSH(visible_keys, groove__ivm__graph__FieldRef__name(version.tx_time_field));
    JZ_VEC_PUSH(visible_keys, groove__ivm__graph__FieldRef__name(version.tx_node_field));
    jz_str_drop(&version.schema_version_field);
    jazz_engine__optstr_drop(&version.branch_or_prefix_field);
    if (routing_param_fields->len == 0) {
        *out = GB(project_fields)(gb_exact_join(GROOVE_GB_SEMI_JOIN, witness_source, visible_graph, witness_keys, visible_keys), witness_fields);
        return NULL;
    }
    /* A routed publication keeps the visibility relation's bag multiplicity and
     * binding values. Groove prunes unused join input columns after binding the
     * exact keys and the projection's field dependencies. */
    JZ_VEC_FOR(groove_ProjectField, field, witness_fields) {
        groove_FieldRef *key;
        switch (field->expression.tag) {
        case GROOVE_PEXPR_FIELD: case GROOVE_PEXPR_NULLABLE: case GROOVE_PEXPR_NULLABLE_FLAT: key = &field->expression.u.field; break;
        case GROOVE_PEXPR_RECORD_FIELD: key = &field->expression.u.record_field.source; break;
        case GROOVE_PEXPR_ENUM_TAG_REMAP: case GROOVE_PEXPR_ENUM_REMAP: case GROOVE_PEXPR_RECURSIVE_ENUM_REMAP: key = &field->expression.u.remap.source; break;
        default: continue;
        }
        if (key->tag == GROOVE_FIELDREF_RESOLVED) jz_panic("witness carriers are bound by name");
        groove_FieldRef replacement = groove__ivm__graph__FieldRef__stored_name(jz_format("right.%.*s", JE_STR_ARG(key->name)));
        groove__ivm__graph__FieldRef__drop(key);
        *key = replacement;
    }
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(routing_param_fields, f, unit) {
        groove_ProjectField projected = PF(named)(s_clone(f));
        groove__ivm__graph__ProjectExpr__drop(&projected.expression);
        memset(&projected.expression, 0, sizeof projected.expression);
        projected.expression.tag = GROOVE_PEXPR_FIELD;
        projected.expression.u.field = groove__ivm__graph__FieldRef__stored_name(jz_format("left.%.*s", JE_STR_ARG(*f)));
        pf_push(&witness_fields, projected);
    }
    *out = GB(project_fields)(gb_exact_join(GROOVE_GB_JOIN, visible_graph, witness_source, visible_keys, witness_keys), witness_fields);
    return NULL;
}
static CapErr content_version_witness_graph(const RSource *source, const char *event_kind, groove_GraphBuilder **out) {
    jz_btree none;
    strset_init(&none);
    CapErr e = content_version_witness_graph_from_visible_graph(source, groove_gb_clone(source->graph), event_kind, &none, out);
    jz_btree_drop(&none);
    return e;
}

/* Transfer only routes that proved this exact deletion winner. Dedupe the proof
 * side before joining: several policy derivations must not multiply a register
 * witness, and a proof for an older deletion is never sufficient.
 * consumes witness and authorized */
static groove_GraphBuilder *routed_deletion_authorization_graph(groove_GraphBuilder *witness, const jazz_engine_StrVec *witness_fields, const jz_str *row_uuid_field,
                                                                groove_GraphBuilder *authorized, const jz_btree *routes) {
    jazz_engine_StrVec left_on = sv3("", "tx_time", "tx_node_id");
    jz_str_drop(&left_on.ptr[0]);
    left_on.ptr[0] = s_clone(row_uuid_field);
    if (routes->len == 0)
        return GB(semi_join)(witness, GB(project)(authorized, sv3("row_uuid", "tx_time", "tx_node_id")), left_on, sv3("row_uuid", "tx_time", "tx_node_id"));
    jazz_engine_StrVec proof_fields = sv3("row_uuid", "tx_time", "tx_node_id"), proof_fields2, none = {0};
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(routes, f, unit) jazz_engine__strvec_push_clone(&proof_fields, f);
    jazz_engine__strvec_clone(&proof_fields2, &proof_fields);
    groove_GraphBuilder *proof = GB(arg_max_by)(GB(project)(authorized, proof_fields2), proof_fields, none);
    groove_ProjectFieldVec fields = {0};
    JZ_VEC_FOR(const jz_str, w, *witness_fields) pf_push(&fields, PF(renamed)(F(left_field)(JZ_S(w)), s_clone(w)));
    JZ_BTREE_FOR(routes, f, unit) pf_push(&fields, PF(renamed)(F(right_field)(JZ_S(f)), s_clone(f)));
    return GB(project_fields)(GB(join)(witness, proof, left_on, sv3("row_uuid", "tx_time", "tx_node_id")), fields);
}

/* ---------------------------------------------------------------- binding routes */
/* A binding-dependent terminal consumes an explicit typed input. Values and
 * claims are inserted by the owner of that input; compilation only resolves
 * names against its descriptor. */
static jazz_engine_UrErr binding_route_projection(const jz_str *shape, const jz_btree *extra_user_params, const jazz_engine_ParameterDomain *domain,
                                                  const jz_btree *routes, groove_GraphBuilder **graph, groove_ProjectFieldVec *out) {
    jz_btree params;
    jazz_engine__strvtmap_clone(&params, extra_user_params);
    const jz_str *k; const groove_ValueType *t;
    JZ_BTREE_FOR(&domain->user_params, k, t) {
        jz_str key = s_clone(k);
        groove_ValueType ty = groove_vt_clone(t);
        jazz_engine__btree_put(&params, &key, &ty);
    }
    u32 n = params.len + domain->claim_params.len, i = 0;
    jz_str *names = jz_alloc(sizeof(jz_str) * (n ? n : 1));
    groove_ValueType *types = jz_alloc(sizeof(groove_ValueType) * (n ? n : 1));
    JZ_BTREE_FOR(&params, k, t) { names[i] = s_clone(k); types[i] = groove_vt_clone(t); i++; }
    const jazz_engine_ClaimParameter *claim;
    JZ_BTREE_FOR(&domain->claim_params, k, claim) { names[i] = s_clone(k); types[i] = groove_vt_clone(&claim->ty); i++; }
    groove_RecordDescriptor descriptor = groove__records__RecordDescriptor__new(names, types, n);
    jz_free(names);
    jz_free(types);
    jz_btree_drop(&params);
    groove_ProjectFieldVec fields = {0};
    const jz_str *route; const void *unit;
    JZ_BTREE_FOR(routes, route, unit) {
        jz_slice source;
        if (jz_btree_contains(&domain->claim_params, route)) source = JZ_S(route);
        else if (!F(route_param_from_field)(JZ_S(route), &source)) {
            pfvec_drop(&fields);
            return jazz_engine__ur_rt(jz_format("binding route '%.*s' has no parameter slot", JE_STR_ARG(*route)));
        }
        u32 index;
        if (!groove__records__RecordDescriptor__field_index(descriptor, (const char *)source.ptr, source.len, &index)) {
            pfvec_drop(&fields);
            return jazz_engine__ur_rt(jz_format("binding route '%.*s' is absent from its input descriptor", JE_STR_ARG(*route)));
        }
        pf_push(&fields, PF(renamed_resolved)(index, s_clone(route)));
    }
    *graph = GB(binding_source)(s_clone(shape), descriptor);
    *out = fields;
    return NULL;
}

/* Build a literal route field using the descriptor domain that will bind the
 * prepared program. Callers that already have the final domain must use it so
 * literal-only terminals compare the same representation as the binding source. */
static jazz_engine_UrErr route_literal_project_field_for_domain(jz_slice route_field, jazz_engine_LoweringContext *request, const jazz_engine_ParameterDomain *domain,
                                                                groove_ProjectField *out) {
    jz_str route = jazz_engine__str_view(route_field); /* borrowed key */
    jazz_engine_ClaimPath path;
    jz_slice param;
    bool is_claim = F(claim_path_from_param_field)(route_field, &path);
    bool scalar = false;
    jazz_engine_NormalizedValueRef value;
    groove_ValueType ty;
    memset(&value, 0, sizeof value);
    if (is_claim) {
        const jazz_engine_ClaimParameter *claim = jz_btree_get(&domain->claim_params, &route);
        if (claim) {
            value.tag = JAZZ_ENGINE_NVR_CLAIM;
            JE_IN(ClaimPath__Clone__clone)(&value.u.claim, &path);
            ty = groove_vt_clone(&claim->ty);
            scalar = true;
        }
    } else if (F(route_param_from_field)(route_field, &param)) {
        jz_str pkey = jazz_engine__str_view(param);
        const groove_ValueType *t = jz_btree_get(&domain->user_params, &pkey);
        if (t) {
            value.tag = JAZZ_ENGINE_NVR_PARAM;
            value.u.param = jz_str_from_slice(param);
            ty = groove_vt_clone(t);
            scalar = true;
        }
    }
    groove_ProjectExpr expression;
    if (scalar && JE_ARG(LoweringContext__scalar_argument)(request, value, ty, true, &expression)) {
        if (is_claim) JE_IN(ClaimPath__drop)(&path);
        out->expression = expression;
        out->output_name = jz_str_from_slice(route_field);
        out->output_identity = fid_name(jz_str_from_slice(route_field));
        return NULL;
    }
    if (is_claim) {
        groove_Value claim_value;
        jazz_engine_UrErr ue = JE_GL(claim_value)(&path, &request->request->policy, &claim_value);
        JE_IN(ClaimPath__drop)(&path);
        if (ue) return ue;
        /* Prepared subscriptions compare routes against the same
         * descriptor-coerced binding record used at bind time. */
        const jazz_engine_ClaimParameter *claim = jz_btree_get(&domain->claim_params, &route);
        if (claim)
            *out = PF(literal_typed)(jz_str_from_slice(route_field),
                                     lit_value(jazz_engine__query_engine__binding_values__coerce_prepared_binding_value(claim_value, &claim->ty)),
                                     groove_vt_clone(&claim->ty));
        else
            *out = PF(literal)(jz_str_from_slice(route_field), lit_value(claim_value));
        return NULL;
    }
    if (!F(route_param_from_field)(route_field, &param))
        return jazz_engine__ur_rt(jz_format("authorization route field '%.*s' is neither a claim nor user parameter", JE_STR_ARG(route)));
    jz_str pkey = jazz_engine__str_view(param);
    const groove_Value *bound = jz_btree_get(&request->request->input.binding.values, &pkey);
    if (!bound)
        return jazz_engine__ur_rt(jz_format("authorization route field '%.*s' refers to unbound parameter '%.*s'", JE_STR_ARG(route), JE_STR_ARG(pkey)));
    /* The binding descriptor owns the type, not the literal's payload. Keep the
     * route's descriptor identical to the prepared binding source. */
    groove_Value copy;
    groove__records__values__Value__Clone__clone(&copy, bound);
    const groove_ValueType *t = jz_btree_get(&domain->user_params, &pkey);
    if (t)
        *out = PF(literal_typed)(jz_str_from_slice(route_field), lit_value(jazz_engine__query_engine__binding_values__coerce_prepared_binding_value(copy, t)),
                                 groove_vt_clone(t));
    else
        *out = PF(literal)(jz_str_from_slice(route_field), lit_value(copy));
    return NULL;
}
jazz_engine_UrErr JE_TERM(route_literal_project_field)(jz_slice route_field, jazz_engine_LoweringContext *request, groove_ProjectField *out) {
    const jazz_engine_ParameterDomain *domain;
    JE_UR_TRY(JE_ARG(LoweringContext__parameter_domain)(request, &domain));
    return route_literal_project_field_for_domain(route_field, request, domain, out);
}

static CapErr deletion_witness_graph_for_current_register(const RSource *source, const char *event_kind, jazz_engine_LoweringContext *request,
                                                          const jazz_engine_ParameterDomain *parameter_domain, const jz_btree *routing_param_fields,
                                                          groove_GraphBuilder **out) {
    if (!source->has_deletion_register) return cap_rt("resolved source did not provide deletion register source");
    if (!source->has_authorized_deletion_preimage) return cap_rt("resolved source did not provide authorized deletion preimage");
    const jazz_engine_GraphWithRowField *reg = &source->deletion_register;
    const jazz_engine_AuthorizedDeletionPreimage *authorized_preimage = &source->authorized_deletion_preimage;
    /* The raw register owns the exact deletion transaction/branch/schema
     * witness. The sibling is the same source occurrence evaluated under its
     * current IncludeDeleted policy. Match the register's exact winner, not
     * merely a row id. */
    groove_GraphBuilder *authorized_deleted_winner = GB(filter)(groove_gb_clone(authorized_preimage->graph),
        groove__ivm__op_types__PredicateExpr__from_field_literal(GROOVE_PK_EQ, s_lit("__jazz_deleted"), lit_bool(true)));
    const jazz_engine_ProgramBinding *binding_in = &request->request->input.binding;
    CapErr e;
    if (routing_param_fields->len && binding_in->has_source_shape) {
        groove_ProjectFieldVec witness_fields;
        if ((e = deletion_witness_fields_for_tagged_rows(source, event_kind, &witness_fields))) { groove_gb_drop(authorized_deleted_winner); return e; }
        jazz_engine_StrVec witness_names = {0};
        JZ_VEC_FOR(const groove_ProjectField, f, witness_fields) jazz_engine__strvec_push_clone(&witness_names, &f->output_name);
        jz_btree present_routes, missing_routes;
        strset_intersect(&authorized_preimage->routing_fields, routing_param_fields, &present_routes);
        groove_GraphBuilder *witness = routed_deletion_authorization_graph(GB(project_fields)(groove_gb_clone(reg->graph), witness_fields), &witness_names,
                                                                           &source->row_shape.row_uuid_field, authorized_deleted_winner, &present_routes);
        strset_difference(routing_param_fields, &present_routes, &missing_routes);
        if (missing_routes.len == 0) {
            *out = witness;
            goto done;
        }
        groove_GraphBuilder *binding;
        groove_ProjectFieldVec routes;
        jazz_engine_UrErr ue = binding_route_projection(&binding_in->source_shape, &binding_in->extra_user_params, parameter_domain, routing_param_fields,
                                                        &binding, &routes);
        if (ue) {
            groove_gb_drop(witness);
            jazz_engine__strvec_drop(&witness_names);
            jz_btree_drop(&present_routes);
            jz_btree_drop(&missing_routes);
            return jazz_engine__ur_into_cap(ue);
        }
        groove_ProjectFieldVec fields = {0};
        JZ_VEC_FOR(const jz_str, f, witness_names) pf_push(&fields, PF(renamed)(F(left_field)(JZ_S(f)), s_clone(f)));
        const jz_str *f; const void *unit;
        JZ_BTREE_FOR(&present_routes, f, unit) pf_push(&fields, PF(renamed)(F(left_field)(JZ_S(f)), s_clone(f)));
        JZ_BTREE_FOR(&missing_routes, f, unit) pf_push(&fields, PF(renamed)(F(right_field)(JZ_S(f)), s_clone(f)));
        *out = GB(project_fields)(JE_GL(policy_join_if_needed)(witness, GB(project_fields)(binding, routes), jazz_engine__strvec_from_set(&present_routes),
                                                               jazz_engine__strvec_from_set(&present_routes), request), fields);
    done:
        jazz_engine__strvec_drop(&witness_names);
        jz_btree_drop(&present_routes);
        jz_btree_drop(&missing_routes);
        return NULL;
    }
    groove_GraphBuilder *winner = GB(project)(authorized_deleted_winner, sv3("row_uuid", "tx_time", "tx_node_id"));
    jazz_engine_StrVec left_on = jazz_engine__strvec_one(s_clone(&reg->row_uuid_field));
    jazz_engine__strvec_push_cstr(&left_on, "tx_time");
    jazz_engine__strvec_push_cstr(&left_on, "tx_node_id");
    groove_GraphBuilder *authorized_register = GB(semi_join)(groove_gb_clone(reg->graph), winner, left_on, sv3("row_uuid", "tx_time", "tx_node_id"));
    groove_ProjectFieldVec fields;
    if ((e = deletion_witness_fields_for_tagged_rows(source, event_kind, &fields))) { groove_gb_drop(authorized_register); return e; }
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(routing_param_fields, f, unit) {
        groove_ProjectField pf;
        jazz_engine_UrErr ue = JE_TERM(route_literal_project_field)(JZ_S(f), request, &pf);
        if (ue) { pfvec_drop(&fields); groove_gb_drop(authorized_register); return jazz_engine__ur_into_cap(ue); }
        pf_push(&fields, pf);
    }
    *out = GB(project_fields)(authorized_register, fields);
    return NULL;
}

/* ---------------------------------------------------------------- relation edges */
static jz_str correlated_relation_name(const jazz_engine_CorrelatedPathPlan *path) {
    const jazz_engine_SourceRoleVec *roles = &path->path.child.path;
    for (u32 i = roles->len; i-- > 0;) {
        const jazz_engine_SourceRole *role = &roles->ptr[i];
        if (role->tag != JAZZ_ENGINE_ROLE_CORRELATED_CHILD) continue;
        const char *colon = memchr(role->name.ptr, ':', role->name.len);
        if (!colon) return s_clone(&role->name);
        u32 off = (u32)(colon - role->name.ptr) + 1;
        return jz_str_from_slice(jz_slice_of(role->name.ptr + off, role->name.len - off));
    }
    return s_clone(&path->path.child.table);
}

static CapErr correlated_relation_edge_fields(const RSource *source, const RSource *target, const jazz_engine_CorrelatedPathPlan *path, groove_ProjectFieldVec *out) {
    VersionWitnessFieldRefs sv, tv;
    JE_CAP_TRY(version_witness_fields(&source->row_shape, &sv));
    CapErr e = version_witness_fields(&target->row_shape, &tv);
    if (e) { vwf_drop(&sv); return e; }
    groove_ProjectFieldVec fields = {0};
    pf_push(&fields, pf_lit_str("source_source", s_clone(&source->row_shape.source.table)));
    pf_push(&fields, pf_lit_str("source_table", s_clone(&source->table_schema->name)));
    pf_push(&fields, pf_ren(F(left_field)(JZ_S(&source->row_shape.row_uuid_field)), "source_row"));
    pf_push(&fields, pf_ren(F(left_field)(JZ_S(&sv.tx_time_field)), "source_tx_time"));
    pf_push(&fields, pf_ren(F(left_field)(JZ_S(&sv.tx_node_field)), "source_tx_node_id"));
    pf_push(&fields, pf_lit_str("path", correlated_relation_name(path)));
    pf_push(&fields, pf_lit_str("kind", s_lit("array_subquery")));
    pf_push(&fields, pf_lit_str("role", s_lit("terminal")));
    pf_push(&fields, pf_lit_str("target_source", s_clone(&target->row_shape.source.table)));
    pf_push(&fields, pf_lit_str("target_table", s_clone(&target->table_schema->name)));
    pf_push(&fields, pf_ren(F(right_field)(JZ_S(&target->row_shape.row_uuid_field)), "target_row"));
    pf_push(&fields, pf_ren(F(right_field)(JZ_S(&tv.tx_time_field)), "target_tx_time"));
    pf_push(&fields, pf_ren(F(right_field)(JZ_S(&tv.tx_node_field)), "target_tx_node_id"));
    if (sv.branch_or_prefix_field.some) pf_push(&fields, pf_ren(F(left_field)(JZ_S(&sv.branch_or_prefix_field.s)), "source_branch_or_prefix"));
    if (tv.branch_or_prefix_field.some) pf_push(&fields, pf_ren(F(right_field)(JZ_S(&tv.branch_or_prefix_field.s)), "target_branch_or_prefix"));
    vwf_drop(&sv);
    vwf_drop(&tv);
    *out = fields;
    return NULL;
}

/* consumes graph; appends to out */
static CapErr correlated_relation_edge_graphs(const jazz_engine_CorrelatedPathPlan *path, groove_GraphBuilder *graph, const RSource *source,
                                              const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, groove_GraphBuilderVec *out) {
    const RSource *target;
    CapErr e = JE_RES(ResolvedSource__lookup)(resolved_sources, &path->path.child, "path child", &target);
    groove_ProjectFieldVec fields;
    if (!e) e = correlated_relation_edge_fields(source, target, path, &fields);
    if (e) { groove_gb_drop(graph); return e; }
    JZ_VEC_PUSH(*out, GB(project_fields)(groove_gb_clone(graph), fields));
    JZ_VEC_FOR(const jazz_engine_CorrelatedPathPlan, sibling, path->siblings) {
        jazz_engine_LoweredRelationInput lowered;
        jazz_engine_UrErr ue = JE_GL(lower_correlated_path_relation_graph)(sibling, source, resolved_sources, request, &lowered);
        if (ue) { groove_gb_drop(graph); return cap_explained_ur(ue, "sibling correlated path relation facts lower from the shared root graph"); }
        if ((e = correlated_relation_edge_graphs(sibling, lri_take_graph(&lowered), source, resolved_sources, request, out))) { groove_gb_drop(graph); return e; }
    }
    JZ_VEC_FOR(const jazz_engine_CorrelatedPathPlan, nested, path->nested) {
        groove_GraphBuilder *nested_parent = GB(project_fields)(groove_gb_clone(graph), JE_TERM(project_source_fields_from_prefix)(target, JAZZ_ENGINE_RIGHT_JOIN_PREFIX));
        jazz_engine_LoweredRelationInput lowered;
        jazz_engine_UrErr ue = JE_GL(lower_correlated_path_relation_graph_from_parent)(nested, nested_parent, target, resolved_sources, request, true, &lowered);
        if (ue) { groove_gb_drop(graph); return cap_explained_ur(ue, "nested correlated path relation facts lower from parent-child path graphs"); }
        if ((e = correlated_relation_edge_graphs(nested, lri_take_graph(&lowered), target, resolved_sources, request, out))) { groove_gb_drop(graph); return e; }
    }
    groove_gb_drop(graph);
    return NULL;
}

/* consumes graph */
static CapErr relation_edge_graph(const jazz_engine_ProgramFactKey *key, groove_GraphBuilder *graph, const jazz_engine_AnalyzedQueryPlan *plan,
                                  const RSource *source, const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, groove_GraphBuilder **out) {
    if (plan->tag == JAZZ_ENGINE_AQP_CORRELATED_PATH) {
        groove_GraphBuilderVec graphs = {0};
        CapErr e = correlated_relation_edge_graphs(&plan->u.correlated, graph, source, resolved_sources, request, &graphs);
        if (e) { graphs_drop(&graphs); return e; }
        if (graphs.len == 1) { *out = graphs.ptr[0]; jz_free(graphs.ptr); }
        else *out = GB(union)(graphs);
        return NULL;
    }
    if (plan->tag == JAZZ_ENGINE_AQP_RECURSIVE_RELATION) { *out = graph; return NULL; }
    groove_gb_drop(graph);
    return cap_output(key, "relation edge facts require a path or recursive relation node");
}

/* ---------------------------------------------------------------- result occurrence / payload fields */
static const jazz_engine_LinearStepVec *root_linear_steps(const jazz_engine_AnalyzedQueryPlan *plan) {
    if (plan->tag == JAZZ_ENGINE_AQP_LINEAR) return &plan->u.linear.steps;
    if (plan->tag == JAZZ_ENGINE_AQP_UNION) return &plan->u.union_.terminal_steps;
    return NULL;
}
static const jazz_engine_RowProjectionVec *last_project_columns(const jazz_engine_AnalyzedQueryPlan *plan) {
    const jazz_engine_LinearStepVec *steps = root_linear_steps(plan);
    if (!steps || !steps->len || steps->ptr[steps->len - 1].tag != JAZZ_ENGINE_STEP_PROJECT) return NULL;
    return &steps->ptr[steps->len - 1].u.project;
}
static void tofvec_drop(jazz_engine_TypedOutputFieldVec *v) { jz_vec_drop(v, sizeof(jazz_engine_TypedOutputField), jazz_engine__tof_drop_v); }

static CapErr result_occurrence_id_fields(const jazz_engine_AnalyzedQueryPlan *plan, const RSource *source, const jz_btree *resolved_sources,
                                          jazz_engine_LoweringContext *request, jazz_engine_StrVec *out) {
    jazz_engine_StrVec fields = jazz_engine__strvec_one(s_clone(&source->row_shape.row_uuid_field));
    *out = fields;
    if (!sid_eq(&source->row_shape.source, JE_PLAN(AnalyzedQueryPlan__root_source)(plan))) return NULL;
    const jazz_engine_RowProjectionVec *columns = last_project_columns(plan);
    if (columns) {
        JZ_VEC_FOR(const jazz_engine_RowProjection, column, *columns)
            if (jazz_engine__str_starts_with(&column->output.name, "__flat_join_row_")) jazz_engine__strvec_push_clone(&fields, &column->output.name);
    } else {
        jazz_engine_TypedOutputFieldVec occurrence;
        CapErr e = JE_TERM(root_join_occurrence_fields)(plan, resolved_sources, request, &occurrence);
        if (e) { jazz_engine__strvec_drop(&fields); memset(out, 0, sizeof *out); return e; }
        JZ_VEC_FOR(jazz_engine_TypedOutputField, f, occurrence) if (f->ty.tag == GROOVE_VT_UUID) jazz_engine__strvec_push_clone(&fields, &f->name);
        tofvec_drop(&occurrence);
    }
    *out = fields;
    return NULL;
}

static CapErr result_occurrence_union_arm_fields(const jazz_engine_AnalyzedQueryPlan *plan, const RSource *source, const jz_btree *resolved_sources,
                                                 jazz_engine_LoweringContext *request, jz_btree *out) {
    jz_btree_init(out, &jazz_engine__U32_STR_MAP_DESC);
    if (!sid_eq(&source->row_shape.source, JE_PLAN(AnalyzedQueryPlan__root_source)(plan))) return NULL;
    jazz_engine_TypedOutputFieldVec fields;
    JE_CAP_TRY(JE_TERM(root_join_occurrence_fields)(plan, resolved_sources, request, &fields));
    if (plan->tag == JAZZ_ENGINE_AQP_UNION) {
        u32 zero = 0;
        jz_str arm = s_lit("__root_union_arm");
        jazz_engine__btree_put(out, &zero, &arm);
        tofvec_drop(&fields);
        return NULL;
    }
    u32 joined_position = 1;
    jz_str pending_arm = {0};
    bool has_pending = false;
    JZ_VEC_FOR(jazz_engine_TypedOutputField, f, fields) {
        if (f->ty.tag == GROOVE_VT_STRING) {
            if (has_pending) jz_str_drop(&pending_arm);
            pending_arm = s_clone(&f->name);
            has_pending = true;
        } else if (f->ty.tag == GROOVE_VT_UUID) {
            if (has_pending) { jazz_engine__btree_put(out, &joined_position, &pending_arm); has_pending = false; }
            joined_position++;
        }
    }
    if (has_pending) jz_str_drop(&pending_arm);
    tofvec_drop(&fields);
    return NULL;
}

void JE_TERM(flat_join_payload_fields)(const jazz_engine_AnalyzedQueryPlan *plan, jazz_engine_TypedOutputFieldVec *out) {
    jazz_engine_TypedOutputFieldVec v = {0};
    *out = v;
    const jazz_engine_RowProjectionVec *columns = last_project_columns(plan);
    if (!columns) return;
    bool flat = false;
    JZ_VEC_FOR(const jazz_engine_RowProjection, column, *columns)
        if (jazz_engine__str_starts_with(&column->output.name, "__flat_join_row_")) { flat = true; break; }
    if (!flat) return;
    JZ_VEC_FOR(const jazz_engine_RowProjection, column, *columns) {
        if (!memchr(column->output.name.ptr, '.', column->output.name.len)) continue;
        jazz_engine_TypedOutputField f;
        tof_clone(&f, &column->output);
        JZ_VEC_PUSH(v, f);
    }
    *out = v;
}

static void result_payload_fields(const jazz_engine_AnalyzedQueryPlan *plan, const RSource *source, jazz_engine_TypedOutputFieldVec *out) {
    JE_TERM(flat_join_payload_fields)(plan, out);
    if (out->len || !source->requires_result_payload) return;
    jz_btree hidden;
    hidden_source_fields(&source->row_shape, &hidden);
    u32 n;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(source->row_shape.descriptor, &n);
    for (u32 i = 0; i < n; i++) {
        const groove_DescriptorField *field = &fields[i];
        if (!field->has_name) continue;
        const jz_str *name = &field->name;
        if (jz_str_eq(name, &source->row_shape.row_uuid_field) || jz_btree_contains(&hidden, name) || jz_btree_contains(&source->routing_fields, name)) continue;
        jazz_engine_TypedOutputField f = {s_clone(name), groove_vt_clone(&field->value_type)};
        JZ_VEC_PUSH(*out, f);
    }
    jz_btree_drop(&hidden);
}

/* Option<(SourceId, Vec<TypedOutputField>, bool)>: false = None. The bool of the
 * Rust tuple is always true when Some and is not represented. */
static bool projected_multisource_terminal(const jazz_engine_AnalyzedQueryPlan *plan, const RSource *root_source, jazz_engine_SourceId *output_source,
                                           jazz_engine_TypedOutputFieldVec *out) {
    const jazz_engine_RowProjectionVec *columns = last_project_columns(plan);
    if (!columns) return false;
    const jazz_engine_SourceId *found = NULL;
    JZ_VEC_FOR(const jazz_engine_RowProjection, column, *columns) {
        if (column->value.tag == JAZZ_ENGINE_NVR_ROW_ID && column->value.sub == JAZZ_ENGINE_ROWID_SOURCE &&
            jz_str_eq(&column->output.name, &root_source->row_shape.row_uuid_field)) { found = &column->value.u.source; break; }
    }
    if (!found) return false;
    bool is_flat = false;
    JZ_VEC_FOR(const jazz_engine_RowProjection, column, *columns)
        if (jazz_engine__str_starts_with(&column->output.name, "__flat_join_row_")) { is_flat = true; break; }
    if (!is_flat) return false;
    sid_clone(output_source, found);
    jazz_engine_TypedOutputFieldVec v = {0};
    JZ_VEC_FOR(const jazz_engine_RowProjection, column, *columns) {
        jazz_engine_TypedOutputField f;
        tof_clone(&f, &column->output);
        JZ_VEC_PUSH(v, f);
    }
    *out = v;
    return true;
}

void JE_TERM(output_routing_fields)(const jazz_engine_ProgramFactOutput *output, jz_btree *out) {
    const jazz_engine_ProgramFactSchema *s = &output->schema;
    const jz_btree *src = NULL;
    switch (s->tag) {
    case JAZZ_ENGINE_PFS_AUTHORIZED_ROWS: src = &s->u.authorized_rows.routing_param_fields; break;
    case JAZZ_ENGINE_PFS_RESULT_MEMBERSHIP: src = &s->u.result_membership.routing_param_fields; break;
    case JAZZ_ENGINE_PFS_AGGREGATE_RESULT: src = &s->u.aggregate_result.routing_param_fields; break;
    case JAZZ_ENGINE_PFS_PROGRAM_SOURCE_COVERAGE: src = &s->u.program_source_coverage.routing_param_fields; break;
    case JAZZ_ENGINE_PFS_VERSION_WITNESSES: case JAZZ_ENGINE_PFS_REPLACEMENT_WITNESSES: src = &s->u.witnesses.routing_param_fields; break;
    case JAZZ_ENGINE_PFS_READ_FRONTIER_SETTLED: src = &s->u.read_frontier_settled.routing_param_fields; break;
    default: break;
    }
    if (src) jazz_engine__strset_clone(out, src);
    else strset_init(out);
}

/* ---------------------------------------------------------------- sink names */
static jz_str fact_sink_name(const jazz_engine_ProgramFactKey *key) {
    switch (key->tag) {
    case JAZZ_ENGINE_FACT_AUTHORIZED_ROWS: return s_lit("policy.authorized_rows");
    case JAZZ_ENGINE_FACT_RESULT_MEMBERSHIP: return s_lit("maintained.result_current");
    case JAZZ_ENGINE_FACT_VERSION_WITNESSES: return s_lit("maintained.version_content");
    case JAZZ_ENGINE_FACT_REPLACEMENT_WITNESSES: return s_lit("maintained.replacement_content");
    case JAZZ_ENGINE_FACT_RELATION_EDGES: return s_lit("maintained.relation_edges");
    case JAZZ_ENGINE_FACT_PATH_CORRELATION_COVERAGE: return s_lit("maintained.path_coverage");
    case JAZZ_ENGINE_FACT_PROGRAM_SOURCE_COVERAGE: return s_lit("maintained.program_source_coverage");
    default: {
        JE_DBG(d, JE_OUT(ProgramFactKey__Debug__fmt), key);
        jz_str s = jz_format("fact.%.*s", JE_STR_ARG(d));
        jz_str_drop(&d);
        return s;
    }
    }
}
static void push_sanitized(jz_str *out, const char *prefix, const jz_str *name) {
    jz_str_push_cstr(out, prefix);
    for (u32 i = 0; i < name->len; i++) {
        unsigned char ch = (unsigned char)name->ptr[i];
        bool alnum = (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z');
        /* str::replace over chars: a multi-byte char is one replacement */
        if (ch >= 0x80) { while (i + 1 < name->len && ((unsigned char)name->ptr[i + 1] & 0xC0) == 0x80) i++; }
        jz_str_push_char(out, alnum ? (char)ch : '_');
    }
}
static jz_str source_path_sink_fragment(const jazz_engine_SourceId *source) {
    jz_str out = {0};
    for (u32 i = 0; i < source->path.len; i++) {
        const jazz_engine_SourceRole *component = &source->path.ptr[i];
        if (i) jz_str_push_char(&out, '.');
        switch (component->tag) {
        case JAZZ_ENGINE_ROLE_ROOT: jz_str_push_cstr(&out, "root"); break;
        case JAZZ_ENGINE_ROLE_ALIAS: push_sanitized(&out, "", &component->name); break;
        case JAZZ_ENGINE_ROLE_RECURSIVE_SEED: push_sanitized(&out, "recursive_seed_", &component->name); break;
        case JAZZ_ENGINE_ROLE_RECURSIVE_STEP: push_sanitized(&out, "recursive_step_", &component->name); break;
        case JAZZ_ENGINE_ROLE_CORRELATED_CHILD: push_sanitized(&out, "correlated_child_", &component->name); break;
        default: push_sanitized(&out, "policy_", &component->name); break;
        }
    }
    return out;
}
static jz_str scoped_fact_sink_name(const jazz_engine_ProgramFactKey *key, const jazz_engine_SourceId *source) {
    jz_str base = fact_sink_name(key), path = source_path_sink_fragment(source);
    jz_str s = jz_format("%.*s.%.*s.%.*s", JE_STR_ARG(base), JE_STR_ARG(source->table), JE_STR_ARG(path));
    jz_str_drop(&base);
    jz_str_drop(&path);
    return s;
}
static jz_str scoped_deletion_fact_sink_name(const jazz_engine_ProgramFactKey *key, const jazz_engine_SourceId *source) {
    u8 flavour;
    if (!witness_flavour_of(key, &flavour)) return scoped_fact_sink_name(key, source);
    jz_str path = source_path_sink_fragment(source);
    jz_str s = jz_format("maintained.%s.%.*s.%.*s", witness_tag(flavour, WL_DELETION), JE_STR_ARG(source->table), JE_STR_ARG(path));
    jz_str_drop(&path);
    return s;
}

/* ---------------------------------------------------------------- fact schemas */
static void typed_publication_fields(const RSource *source, const jazz_engine_TypedOutputFieldVec *payload_fields, bool flat_join, jz_btree *out) {
    jz_btree_init(out, &jazz_engine__STR_CRPF_MAP_DESC);
    JZ_VEC_FOR(const jazz_engine_TypedOutputField, field, *payload_fields) {
        Crpf binding;
        jz_str name;
        if (flat_join) binding = crpf_result(s_clone(&field->name), JAZZ_ENGINE_CRRV_APPLICATION_CELL);
        else if (resolved_source_public_name(source, &field->name, &name)) binding = source_publication_field(source, name);
        else binding = crpf_result(s_clone(&field->name), jazz_engine__query_engine__publication__CurrentRowResultVisibility__current_row_metadata(JZ_S(&field->name)));
        crpf_put(out, s_clone(&field->name), binding);
    }
}

/* consumes routing_param_fields */
static CapErr fact_output_with_terminal(const jazz_engine_ProgramFactKey *key, u8 terminal, const jazz_engine_AnalyzedQueryPlan *plan,
                                        const jazz_engine_SourceId *source_id, const RSource *source, const jz_btree *resolved_sources,
                                        jazz_engine_LoweringContext *request, jz_btree routing_param_fields, jazz_engine_ProgramFactOutput *out) {
    memset(out, 0, sizeof *out);
    jazz_engine_ProgramFactSchema *schema = &out->schema;
    CapErr e = NULL;
    switch (key->tag) {
    case JAZZ_ENGINE_FACT_AUTHORIZED_ROWS:
        schema->tag = JAZZ_ENGINE_PFS_AUTHORIZED_ROWS;
        schema->u.authorized_rows.row_field = s_clone(&source->row_shape.row_uuid_field);
        schema->u.authorized_rows.routing_param_fields = routing_param_fields;
        break;
    case JAZZ_ENGINE_FACT_RESULT_MEMBERSHIP: {
        const jazz_engine_NvrVec *g; const jazz_engine_AggregateExprVec *o;
        if (JE_PLAN(root_aggregate_step)(plan, &g, &o)) {
            schema->tag = JAZZ_ENGINE_PFS_AGGREGATE_RESULT;
            if ((e = aggregate_result_schema(plan, source, routing_param_fields, &schema->u.aggregate_result))) return e;
            break;
        }
        VersionWitnessFieldRefs version;
        if ((e = version_witness_fields(&source->row_shape, &version))) { jz_btree_drop(&routing_param_fields); return e; }
        jazz_engine_ResultMembershipSchema *rm = &schema->u.result_membership;
        schema->tag = JAZZ_ENGINE_PFS_RESULT_MEMBERSHIP;
        rm->routing_param_fields = routing_param_fields;
        jz_btree_init(&rm->occurrence_union_arm_fields, &jazz_engine__U32_STR_MAP_DESC);
        jz_btree_init(&rm->payload_publication_fields, &jazz_engine__STR_CRPF_MAP_DESC);
        rm->table_field = s_lit("table_name");
        rm->row_field = s_clone(&source->row_shape.row_uuid_field);
        rm->version = content_version_fields_schema(s_lit("content_tx_time"), s_lit("content_tx_node_id"));
        if ((e = result_occurrence_id_fields(plan, source, resolved_sources, request, &rm->occurrence_id_fields)) ||
            (jz_btree_drop(&rm->occurrence_union_arm_fields),
             (e = result_occurrence_union_arm_fields(plan, source, resolved_sources, request, &rm->occurrence_union_arm_fields)))) {
            vwf_drop(&version);
            JE_SCH(ProgramFactSchema__drop)(schema);
            return e;
        }
        jazz_engine_TypedOutputFieldVec flat_join_payload;
        JE_TERM(flat_join_payload_fields)(plan, &flat_join_payload);
        result_payload_fields(plan, source, &rm->payload_fields);
        jz_str settle;
        if (!flat_join_payload.len && settle_position_field(&source->row_shape, &settle)) rm->settle_position_field = jazz_engine__optstr_some(settle);
        jz_btree_drop(&rm->payload_publication_fields);
        typed_publication_fields(source, &rm->payload_fields, flat_join_payload.len != 0, &rm->payload_publication_fields);
        /* Version witnesses may carry either a legacy physical prefix or a
         * branch key. Only view-relative sources use that field as part of
         * public result identity. */
        if (source->requires_result_payload && version.branch_or_prefix_field.some) {
            rm->branch_or_prefix_field = version.branch_or_prefix_field;
            version.branch_or_prefix_field.some = false;
        }
        vwf_drop(&version);
        tofvec_drop(&flat_join_payload);
        break;
    }
    case JAZZ_ENGINE_FACT_PROGRAM_SOURCE_COVERAGE:
        schema->tag = JAZZ_ENGINE_PFS_PROGRAM_SOURCE_COVERAGE;
        JE_IN(SourceId__program_source_id)(source_id, &schema->u.program_source_coverage.source);
        schema->u.program_source_coverage.complete = true;
        schema->u.program_source_coverage.routing_param_fields = routing_param_fields;
        break;
    case JAZZ_ENGINE_FACT_VERSION_WITNESSES:
    case JAZZ_ENGINE_FACT_REPLACEMENT_WITNESSES: {
        VersionWitnessFieldRefs version;
        if ((e = version_witness_fields(&source->row_shape, &version))) { jz_btree_drop(&routing_param_fields); return e; }
        jazz_engine_VersionWitnessSchemas *w = &schema->u.witnesses;
        schema->tag = key->tag == JAZZ_ENGINE_FACT_VERSION_WITNESSES ? JAZZ_ENGINE_PFS_VERSION_WITNESSES : JAZZ_ENGINE_PFS_REPLACEMENT_WITNESSES;
        w->role_field = s_lit("event_kind");
        version_witness_schema(source_id, source, &version, &w->deletion);
        w->has_content = w->has_deletion = true;
        JE_SCH(VersionWitnessSchema__Clone__clone)(&w->content, &w->deletion);
        w->routing_param_fields = routing_param_fields;
        vwf_drop(&version);
        break;
    }
    case JAZZ_ENGINE_FACT_RELATION_EDGES:
        jz_btree_drop(&routing_param_fields);
        schema->tag = JAZZ_ENGINE_PFS_RELATION_EDGES;
        JE_CAP_TRY(relation_edge_schema(plan, source, resolved_sources, &schema->u.relation_edges));
        break;
    case JAZZ_ENGINE_FACT_PATH_CORRELATION_COVERAGE:
        jz_btree_drop(&routing_param_fields);
        schema->tag = JAZZ_ENGINE_PFS_PATH_CORRELATION_COVERAGE;
        JE_CAP_TRY(path_correlation_coverage_schema(plan, source, &schema->u.path_correlation_coverage));
        break;
    default:
        jz_btree_drop(&routing_param_fields);
        return cap_output(key, "requested fact is not lowered yet");
    }
    JE_OUT(ProgramFactKey__Clone__clone)(&out->key, key);
    out->terminal = terminal;
    return NULL;
}
static CapErr fact_output(const jazz_engine_ProgramFactKey *key, const jazz_engine_AnalyzedQueryPlan *plan, const jazz_engine_SourceId *source_id,
                          const RSource *source, const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, jz_btree routing_param_fields,
                          jazz_engine_ProgramFactOutput *out) {
    return fact_output_with_terminal(key, JAZZ_ENGINE_PFT_PRIMARY, plan, source_id, source, resolved_sources, request, routing_param_fields, out);
}

/* ---------------------------------------------------------------- fact graphs */
/* consumes graph and routing_param_fields */
static CapErr fact_terminal_graph(const jazz_engine_ProgramFactKey *key, groove_GraphBuilder *graph, const jazz_engine_AnalyzedQueryPlan *plan,
                                  const RSource *source, const jz_btree *resolved_sources, jazz_engine_LoweringContext *request,
                                  jz_btree routing_param_fields, groove_GraphBuilder **out) {
    CapErr e = NULL;
    switch (key->tag) {
    case JAZZ_ENGINE_FACT_AUTHORIZED_ROWS: {
        groove_ProjectFieldVec fields = {0};
        pf_push(&fields, PF(named)(s_clone(&source->row_shape.row_uuid_field)));
        const jz_str *f; const void *unit;
        JZ_BTREE_FOR(&routing_param_fields, f, unit) pf_push(&fields, PF(named)(s_clone(f)));
        jz_btree_drop(&routing_param_fields);
        *out = GB(project_fields)(graph, fields);
        return NULL;
    }
    case JAZZ_ENGINE_FACT_RESULT_MEMBERSHIP: {
        const jazz_engine_NvrVec *g; const jazz_engine_AggregateExprVec *o;
        groove_ProjectFieldVec fields;
        if (JE_PLAN(root_aggregate_step)(plan, &g, &o)) {
            /* Groove models an ungrouped aggregate as one logical empty group,
             * including COUNT(*) = 0. Keep that identity in the ordinary fact
             * terminal. */
            if ((e = aggregate_result_membership_fields(plan, source, routing_param_fields, &fields))) { groove_gb_drop(graph); return e; }
            *out = GB(project_fields)(graph, fields);
            return NULL;
        }
        jazz_engine_StrVec occurrence_fields;
        jz_btree arms;
        if ((e = result_occurrence_id_fields(plan, source, resolved_sources, request, &occurrence_fields))) goto fail;
        if ((e = result_occurrence_union_arm_fields(plan, source, resolved_sources, request, &arms))) { jazz_engine__strvec_drop(&occurrence_fields); goto fail; }
        const u32 *k; const jz_str *arm;
        JZ_BTREE_FOR(&arms, k, arm) jazz_engine__strvec_push_clone(&occurrence_fields, arm);
        jz_btree_drop(&arms);
        jazz_engine_TypedOutputFieldVec flat_join_payload, payload;
        JE_TERM(flat_join_payload_fields)(plan, &flat_join_payload);
        result_payload_fields(plan, source, &payload);
        e = result_membership_fields(source, routing_param_fields, &payload, &occurrence_fields, flat_join_payload.len == 0, &fields);
        tofvec_drop(&flat_join_payload);
        tofvec_drop(&payload);
        jazz_engine__strvec_drop(&occurrence_fields);
        if (e) { groove_gb_drop(graph); return e; }
        *out = GB(project_fields)(graph, fields);
        return NULL;
    }
    case JAZZ_ENGINE_FACT_VERSION_WITNESSES:
    case JAZZ_ENGINE_FACT_REPLACEMENT_WITNESSES:
        groove_gb_drop(graph);
        jz_btree_drop(&routing_param_fields);
        return content_version_witness_graph(source, key->tag == JAZZ_ENGINE_FACT_VERSION_WITNESSES ? "version_content" : "replacement_content", out);
    case JAZZ_ENGINE_FACT_RELATION_EDGES: {
        jz_btree_drop(&routing_param_fields);
        jazz_engine_RelationEdgeSchema schema;
        if ((e = relation_edge_schema(plan, source, resolved_sources, &schema))) { groove_gb_drop(graph); return e; }
        fact_schema_drop_as(JAZZ_ENGINE_PFS_RELATION_EDGES, &schema, sizeof schema);
        return relation_edge_graph(key, graph, plan, source, resolved_sources, request, out);
    }
    case JAZZ_ENGINE_FACT_PATH_CORRELATION_COVERAGE: {
        jz_btree_drop(&routing_param_fields);
        jazz_engine_PathCorrelationCoverageSchema schema;
        if ((e = path_correlation_coverage_schema(plan, source, &schema))) { groove_gb_drop(graph); return e; }
        fact_schema_drop_as(JAZZ_ENGINE_PFS_PATH_CORRELATION_COVERAGE, &schema, sizeof schema);
        *out = graph;
        return NULL;
    }
    case JAZZ_ENGINE_FACT_PROGRAM_SOURCE_COVERAGE:
        e = cap_output(key, "program source coverage is an independently seeded control-plane terminal");
        goto fail;
    default:
        e = cap_output(key, "requested fact graph is not lowered yet");
        goto fail;
    }
fail:
    groove_gb_drop(graph);
    jz_btree_drop(&routing_param_fields);
    return e;
}

static CapErr seed_coverage_values(bool complete, const char *what, groove_GraphBuilder **out) {
    jz_str name = s_lit("complete");
    groove_ValueType ty = groove_vt_scalar(GROOVE_VT_BOOL);
    groove_RecordDescriptor descriptor = groove__records__RecordDescriptor__new(&name, &ty, 1);
    groove_ValueVec row = {0};
    groove_Value v;
    memset(&v, 0, sizeof v);
    v.tag = GROOVE_V_BOOL;
    v.u.b = complete;
    JZ_VEC_PUSH(row, v);
    jz_err err = GB(values)(descriptor, &row, 1, out);
    jz_vec_drop(&row, sizeof(groove_Value), groove__records__values__Value__drop_v);
    if (!err.code) return NULL;
    jz_str d = jazz_engine__err_display(&err);
    jz_err_drop(&err);
    CapErr e = jazz_engine__cap_runtime_fmt("could not seed %s terminal: %.*s", what, JE_STR_ARG(d));
    jz_str_drop(&d);
    return e;
}

/* Emit a source-completeness receipt for the currently bound program scope.
 * A coverage receipt must exist even when the admitted residual relation is
 * empty. Prepared programs read routes from their binding input, not from the
 * values present while the graph was compiled. */
static CapErr program_source_coverage_graph(jazz_engine_LoweringContext *request, const jazz_engine_ParameterDomain *parameter_domain, bool complete,
                                            const jz_btree *routing_param_fields, groove_GraphBuilder **out) {
    const jazz_engine_ProgramBinding *binding_in = &request->request->input.binding;
    if (routing_param_fields->len && binding_in->has_source_shape) {
        groove_GraphBuilder *binding;
        groove_ProjectFieldVec routes, fields = {0};
        JE_UR_CAP_TRY(binding_route_projection(&binding_in->source_shape, &binding_in->extra_user_params, parameter_domain, routing_param_fields, &binding, &routes));
        pf_push(&fields, PF(literal)(s_lit("complete"), lit_bool(complete)));
        jz_vec_extend(&fields, routes.ptr, routes.len, sizeof(groove_ProjectField));
        jz_free(routes.ptr);
        *out = GB(project_fields)(binding, fields);
        return NULL;
    }
    if (routing_param_fields->len == 0) return seed_coverage_values(complete, "program-source coverage", out);
    groove_GraphBuilder *graph;
    JE_CAP_TRY(seed_coverage_values(complete, "routed program-source coverage", &graph));
    groove_ProjectFieldVec fields = {0};
    pf_push(&fields, pf_named_c("complete"));
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(routing_param_fields, f, unit) {
        groove_ProjectField pf;
        jazz_engine_UrErr ue = route_literal_project_field_for_domain(JZ_S(f), request, parameter_domain, &pf);
        if (ue) { pfvec_drop(&fields); groove_gb_drop(graph); return jazz_engine__ur_into_cap(ue); }
        pf_push(&fields, pf);
    }
    *out = GB(project_fields)(graph, fields);
    return NULL;
}

/* consumes graph */
static CapErr fact_input_graph(const jazz_engine_ProgramFactKey *key, groove_GraphBuilder *graph, const jazz_engine_AnalyzedQueryPlan *plan,
                               const RSource *source, const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, groove_GraphBuilder **out) {
    if (plan->tag == JAZZ_ENGINE_AQP_CORRELATED_PATH &&
        (key->tag == JAZZ_ENGINE_FACT_RELATION_EDGES || key->tag == JAZZ_ENGINE_FACT_PATH_CORRELATION_COVERAGE)) {
        groove_gb_drop(graph);
        jazz_engine_LoweredRelationInput lowered;
        jazz_engine_UrErr ue = JE_GL(lower_correlated_path_relation_graph)(&plan->u.correlated, source, resolved_sources, request, &lowered);
        if (ue) return cap_explained_ur(ue, "correlated path relation facts lower from the parent-child path graph");
        *out = lri_take_graph(&lowered);
        return NULL;
    }
    *out = graph;
    return NULL;
}

/* Lower the authority-visible relation immediately before the root aggregate.
 * The aggregate operator intentionally discards contributor identity; a
 * covered receiver receives version witnesses from this graph and derives the
 * aggregate itself. The input plan is a borrowed view without the last step. */
static CapErr aggregate_input_graph(const jazz_engine_AnalyzedQueryPlan *plan, const RSource *source, const jz_btree *resolved_sources,
                                    jazz_engine_LoweringContext *request, groove_GraphBuilder **out) {
    if (plan->tag != JAZZ_ENGINE_AQP_LINEAR) return cap_rt("aggregate contributor input requires a linear root plan");
    const jazz_engine_LinearStepVec *steps = &plan->u.linear.steps;
    if (!steps->len || steps->ptr[steps->len - 1].tag != JAZZ_ENGINE_STEP_AGGREGATE)
        return cap_rt("aggregate contributor input is missing its terminal aggregate step");
    jazz_engine_AnalyzedQueryPlan input_plan;
    memset(&input_plan, 0, sizeof input_plan);
    input_plan.tag = JAZZ_ENGINE_AQP_LINEAR;
    input_plan.u.linear.root = plan->u.linear.root;
    input_plan.u.linear.steps.ptr = steps->ptr;
    input_plan.u.linear.steps.len = steps->len - 1;
    jazz_engine_LoweredRelationInput lowered;
    JE_UR_CAP_TRY(JE_GL(lower_plan_steps)(groove_gb_clone(source->graph), &input_plan, source, resolved_sources, request, &lowered));
    *out = lri_take_graph(&lowered);
    return NULL;
}

/* ---------------------------------------------------------------- terminal emission */
static void term_push(jazz_engine_LoweredTerminalVec *terminals, jz_str sink, groove_GraphBuilder *graph, jazz_engine_OutputTerminalSchema output) {
    LTerm t = {sink, graph, output};
    JZ_VEC_PUSH(*terminals, t);
}
static jazz_engine_OutputTerminalSchema ots_fact(jazz_engine_ProgramFactOutput fact) {
    jazz_engine_OutputTerminalSchema o;
    memset(&o, 0, sizeof o);
    o.tag = JAZZ_ENGINE_OTS_FACT;
    o.u.fact = fact;
    return o;
}

/* Emit one source's content witness terminal and, when the source has a
 * deletion register, its deletion witness terminal (in that order).
 * consumes visible_graph */
static CapErr push_witness_terminals(jazz_engine_LoweredTerminalVec *terminals, u8 flavour, const jazz_engine_ProgramFactKey *fact,
                                     const jazz_engine_AnalyzedQueryPlan *plan, const jazz_engine_SourceId *source_id, const RSource *resolved_source,
                                     groove_GraphBuilder *visible_graph, const jz_btree *route_fields, const jz_btree *resolved_sources,
                                     jazz_engine_LoweringContext *request, const jazz_engine_ParameterDomain *parameter_domain) {
    jazz_engine_ProgramFactOutput content_output;
    jz_btree routes;
    jazz_engine__strset_clone(&routes, route_fields);
    CapErr e = fact_output_with_terminal(fact, witness_terminal(flavour, WL_CONTENT), plan, source_id, resolved_source, resolved_sources, request, routes, &content_output);
    if (e) { groove_gb_drop(visible_graph); return e; }
    groove_GraphBuilder *graph;
    if ((e = content_version_witness_graph_from_visible_graph(resolved_source, visible_graph, witness_tag(flavour, WL_CONTENT), route_fields, &graph))) {
        JE_SCH(ProgramFactOutput__drop)(&content_output);
        return e;
    }
    term_push(terminals, scoped_fact_sink_name(fact, source_id), graph, ots_fact(content_output));
    /* A content retraction alone means the authority no longer selects that
     * source; it might be policy revocation, and must never be mistaken for a
     * tombstone. When this source has an explicit deletion-register witness,
     * ship that exact deletion layer independently. */
    if (!resolved_source->has_deletion_register) return NULL;
    jazz_engine_ProgramFactOutput deletion_output;
    jazz_engine__strset_clone(&routes, route_fields);
    JE_CAP_TRY(fact_output_with_terminal(fact, witness_terminal(flavour, WL_DELETION), plan, source_id, resolved_source, resolved_sources, request, routes, &deletion_output));
    if ((e = deletion_witness_graph_for_current_register(resolved_source, witness_tag(flavour, WL_DELETION), request, parameter_domain, route_fields, &graph))) {
        JE_SCH(ProgramFactOutput__drop)(&deletion_output);
        return e;
    }
    term_push(terminals, scoped_deletion_fact_sink_name(fact, source_id), graph, ots_fact(deletion_output));
    return NULL;
}

static void terminals_drop(jazz_engine_LoweredTerminalVec *v) { jz_vec_drop(v, sizeof(LTerm), jazz_engine__terminal_drop_v); }

/* consumes graph */
static CapErr lowered_aggregate_terminals(groove_GraphBuilder *graph, jazz_engine_LoweringContext *request, const jazz_engine_AnalyzedQueryPlan *plan,
                                          const RSource *source, const jz_btree *resolved_sources, const jazz_engine_ParameterDomain *parameter_domain,
                                          const jz_btree *routing_param_fields, const jz_btree *available_fields, jazz_engine_LoweredTerminalVec *out) {
    jazz_engine_LoweredTerminalVec terminals = {0};
    const jazz_engine_QueryProgramRequest *rq = request->request;
    /* A maintained authority is an input publisher, not an aggregate-result
     * renderer. Its receiver owns the identical aggregate graph and derives the
     * synthetic row after the admitted source closure is installed. */
    jazz_engine_ProgramFactKey vw = jazz_engine__fact_key_simple(JAZZ_ENGINE_FACT_VERSION_WITNESSES);
    bool authority_publishes_covered_inputs = rq->authorization_mode != JAZZ_ENGINE_AUTH_CLIENT_LOCAL && jz_btree_contains(&rq->output.facts, &vw);
    JE_OUT(ProgramFactKey__drop)(&vw);
    /* An aggregate result cannot tell a receiver which input rows were
     * authorized. Rebuild the graph immediately before the aggregate so the
     * root contributor witness is selected by the same filters and policy graph
     * as the authority result. */
    groove_GraphBuilder *contributor_graph = NULL, *aggregate_graph = NULL;
    jz_btree root_route_fields = {0};
    groove_RecordDescriptor aggregate_descriptor;
    CapErr e;
    if ((e = aggregate_input_graph(plan, source, resolved_sources, request, &contributor_graph))) { groove_gb_drop(graph); return e; }
    /* Current aggregate lowering accepts aggregate expressions over the root
     * occurrence only: the contributor registry is exactly {root -> input graph}. */
    const jazz_engine_SourceId *root_id = JE_PLAN(AnalyzedQueryPlan__root_source)(plan);
    strset_intersect(routing_param_fields, available_fields, &root_route_fields);
    if ((e = aggregate_app_row_descriptor(plan, source, &aggregate_descriptor))) { groove_gb_drop(graph); goto fail; }
    if (root_route_fields.len == 0) {
        aggregate_graph = graph;
    } else {
        groove_ProjectFieldVec fields = {0};
        u32 n;
        const groove_DescriptorField *df = groove__records__RecordDescriptor__fields(aggregate_descriptor, &n);
        for (u32 i = 0; i < n; i++) {
            if (!df[i].has_name) jz_panic("aggregate carrier");
            if (!df[i].has_identity) jz_panic("aggregate identity");
            groove_FieldIdentity id;
            groove__records__FieldIdentity__Clone__clone(&id, &df[i].identity);
            pf_push(&fields, PF(named_with_identity)(s_clone(&df[i].name), id));
        }
        const jz_str *f; const void *unit;
        JZ_BTREE_FOR(&root_route_fields, f, unit) pf_push(&fields, PF(named)(s_clone(f)));
        aggregate_graph = GB(project_fields)(graph, fields);
    }
    if (rq->output.has_app_rows && !authority_publishes_covered_inputs) {
        jz_btree routes;
        jazz_engine__strset_clone(&routes, &root_route_fields);
        jazz_engine_OutputTerminalSchema output;
        memset(&output, 0, sizeof output);
        output.tag = JAZZ_ENGINE_OTS_APP_ROWS;
        jazz_engine_AppRowSchema *rows = &output.u.app_rows;
        rows->terminal.tag = JAZZ_ENGINE_ART_AGGREGATE;
        if ((e = aggregate_result_schema(plan, source, routes, &rows->terminal.aggregate))) goto fail;
        jz_btree_init(&rows->publication_fields, &jazz_engine__STR_CRPF_MAP_DESC);
        jz_btree_init(&rows->field_carriers, &jazz_engine__STR_U8_MAP_DESC);
        jz_btree_init(&rows->public_field_names, &jazz_engine__STR_STR_MAP_DESC);
        const jazz_engine_NvrVec *groups;
        const jazz_engine_AggregateExprVec *outputs;
        if (!JE_PLAN(root_aggregate_step)(plan, &groups, &outputs)) jz_panic("aggregate plan");
        JZ_VEC_FOR(const jazz_engine_NormalizedValueRef, value, *groups) {
            jz_str carrier, name;
            if ((e = aggregate_source_field_name(value, source, &carrier))) break;
            if (value->tag == JAZZ_ENGINE_NVR_ROW_ID) name = s_lit("id");
            else if (!resolved_source_public_name(source, &carrier, &name)) {
                jz_str_drop(&carrier);
                e = cap_rt("aggregate source field has no explicit application name");
                break;
            }
            crpf_put(&rows->publication_fields, carrier, source_publication_field(source, name));
        }
        if (!e) JZ_VEC_FOR(const jazz_engine_AggregateExpr, o, *outputs)
            crpf_put(&rows->publication_fields, F(aggregate_output_field)(JZ_S(&o->output.name)), crpf_result(s_clone(&o->output.name), JAZZ_ENGINE_CRRV_APPLICATION_CELL));
        if (!e) e = aggregate_app_row_descriptor(plan, source, &rows->descriptor);
        if (e) { JE_SCH(OutputTerminalSchema__drop)(&output); goto fail; }
        jazz_engine__strset_clone(&rows->hidden_fields, &root_route_fields);
        rows->carrier = JAZZ_ENGINE_CARRIER_LOGICAL;
        rows->root_union_arm = false;
        term_push(&terminals, s_lit("app_rows"), groove_gb_clone(aggregate_graph), output);
    }
    const jazz_engine_ProgramFactKey *fact; const void *unit;
    JZ_BTREE_FOR(&rq->output.facts, fact, unit) {
        u8 flavour;
        if (fact->tag == JAZZ_ENGINE_FACT_RESULT_MEMBERSHIP) {
            if (authority_publishes_covered_inputs) continue;
            jz_btree none, routes, out_routes;
            jz_btree_init(&none, &jazz_engine__SOURCE_ID_RESOLVED_MAP_DESC);
            jazz_engine__strset_clone(&routes, &root_route_fields);
            jazz_engine_ProgramFactOutput output;
            if ((e = fact_output(fact, plan, root_id, source, &none, request, routes, &output))) goto fail;
            JE_TERM(output_routing_fields)(&output, &out_routes);
            groove_GraphBuilder *g;
            if ((e = fact_terminal_graph(fact, groove_gb_clone(aggregate_graph), plan, source, &none, request, out_routes, &g))) {
                JE_SCH(ProgramFactOutput__drop)(&output);
                goto fail;
            }
            term_push(&terminals, fact_sink_name(fact), g, ots_fact(output));
        } else if (fact->tag == JAZZ_ENGINE_FACT_PROGRAM_SOURCE_COVERAGE) {
            const RSource *resolved_source = jz_btree_get(resolved_sources, root_id);
            if (!resolved_source) { e = cap_rt("aggregate contributor has no resolved descriptor"); goto fail; }
            jz_btree routes;
            jazz_engine__strset_clone(&routes, &root_route_fields);
            jazz_engine_ProgramFactOutput output;
            if ((e = fact_output_with_terminal(fact, JAZZ_ENGINE_PFT_PRIMARY, plan, root_id, resolved_source, resolved_sources, request, routes, &output))) goto fail;
            if (output.schema.tag != JAZZ_ENGINE_PFS_PROGRAM_SOURCE_COVERAGE) jz_panic("program-source coverage key has matching schema");
            groove_GraphBuilder *g;
            if ((e = program_source_coverage_graph(request, parameter_domain, output.schema.u.program_source_coverage.complete, &root_route_fields, &g))) {
                JE_SCH(ProgramFactOutput__drop)(&output);
                goto fail;
            }
            term_push(&terminals, scoped_fact_sink_name(fact, root_id), g, ots_fact(output));
        } else if (witness_flavour_of(fact, &flavour)) {
            /* Aggregate result membership is derived output, not an input
             * snapshot a receiver may replay. Ship the exact versioned
             * contributors for every compiled source so a covered receiver can
             * rebuild the same aggregate graph locally. */
            const RSource *resolved_source = jz_btree_get(resolved_sources, root_id);
            if (!resolved_source) { e = cap_rt("aggregate contributor has no resolved descriptor"); goto fail; }
            if ((e = push_witness_terminals(&terminals, flavour, fact, plan, root_id, resolved_source, groove_gb_clone(contributor_graph), &root_route_fields,
                                            resolved_sources, request, parameter_domain))) goto fail;
        }
    }
    groove_gb_drop(contributor_graph);
    groove_gb_drop(aggregate_graph);
    jz_btree_drop(&root_route_fields);
    *out = terminals;
    return NULL;
fail:
    groove_gb_drop(contributor_graph);
    groove_gb_drop(aggregate_graph);
    jz_btree_drop(&root_route_fields);
    terminals_drop(&terminals);
    return e;
}

/* ---------------------------------------------------------------- covered source members */
/* Add each correlated collector child as an exact post-policy covered input.
 * The output graph for a correlated path contains the parent on the left and
 * the admitted child on the right. Projecting the right source from that graph
 * is the narrow residual frontier. It is intentionally not the raw child source
 * graph. consumes parent_graph */
static CapErr collect_correlated_covered_source_members(const jazz_engine_CorrelatedPathPlan *path, groove_GraphBuilder *parent_graph, const RSource *parent_source,
                                                        const jz_btree *resolved_sources, jazz_engine_LoweringContext *request, const jz_btree *route_fields,
                                                        jz_btree *members) {
    const RSource *child_source;
    CapErr e = JE_RES(ResolvedSource__lookup)(resolved_sources, &path->path.child, "correlated covered", &child_source);
    if (e) { groove_gb_drop(parent_graph); return e; }
    jazz_engine_LoweredRelationInput lowered;
    jazz_engine_UrErr ue = JE_GL(lower_correlated_path_relation_graph_from_parent)(path, groove_gb_clone(parent_graph), parent_source, resolved_sources, request, false, &lowered);
    if (ue) { groove_gb_drop(parent_graph); return jazz_engine__ur_into_cap(ue); }
    groove_GraphBuilder *joined = lri_take_graph(&lowered);
    /* The child descriptor may retain the parent's route carrier, but after the
     * parent/child join that carrier is owned by the left parent input. Reuse
     * the closure boundary's explicit layout. */
    groove_GraphBuilder *child_members = GB(project_fields)(groove_gb_clone(joined), JE_CLO(project_join_contribution_fields_with_root_routes)(child_source, route_fields));
    JE_LOW(union_member)(members, jazz_engine__sid_cloned(&path->path.child), child_members);
    JZ_VEC_FOR(const jazz_engine_CorrelatedPathPlan, sibling, path->siblings) {
        if ((e = collect_correlated_covered_source_members(sibling, groove_gb_clone(parent_graph), parent_source, resolved_sources, request, route_fields, members))) {
            groove_gb_drop(parent_graph);
            groove_gb_drop(joined);
            return e;
        }
    }
    groove_gb_drop(parent_graph);
    /* Nested correlated paths use this child as their left parent. Retain the
     * same root-owned route carrier that scoped its covered input. */
    groove_GraphBuilder *child_parent = GB(project_fields)(joined, JE_CLO(project_join_contribution_fields_with_root_routes)(child_source, route_fields));
    JZ_VEC_FOR(const jazz_engine_CorrelatedPathPlan, nested, path->nested) {
        if ((e = collect_correlated_covered_source_members(nested, groove_gb_clone(child_parent), child_source, resolved_sources, request, route_fields, members))) break;
    }
    groove_gb_drop(child_parent);
    return e;
}

/* ---------------------------------------------------------------- lowered terminals */
static void strset_union(const jz_btree *a, const jz_btree *b, jz_btree *out) {
    jazz_engine__strset_clone(out, a);
    strset_extend_clone(out, b);
}
static bool flat_join_nonempty(const jazz_engine_AnalyzedQueryPlan *plan) {
    jazz_engine_TypedOutputFieldVec v;
    JE_TERM(flat_join_payload_fields)(plan, &v);
    bool nonempty = v.len != 0;
    tofvec_drop(&v);
    return nonempty;
}
static void app_rows_empty_maps(LoweredAppRows *rows) {
    jz_btree_init(&rows->publication_fields, &jazz_engine__STR_CRPF_MAP_DESC);
    strset_init(&rows->hidden_fields);
    jz_btree_init(&rows->field_carriers, &jazz_engine__STR_U8_MAP_DESC);
    jz_btree_init(&rows->public_field_names, &jazz_engine__STR_STR_MAP_DESC);
}
static groove_RecordDescriptor descriptor_of_typed(const jazz_engine_TypedOutputField *const *fields, u32 n) {
    jz_str *names = jz_alloc(sizeof(jz_str) * (n ? n : 1));
    groove_ValueType *types = jz_alloc(sizeof(groove_ValueType) * (n ? n : 1));
    for (u32 i = 0; i < n; i++) { names[i] = s_clone(&fields[i]->name); types[i] = groove_vt_clone(&fields[i]->ty); }
    groove_RecordDescriptor d = groove__records__RecordDescriptor__new(names, types, n);
    jz_free(names);
    jz_free(types);
    return d;
}

/* Authorization consumes existence, not candidate payloads. Project only after
 * all predicates/relations have run, and retain scope routes required by
 * prepared bindings. */
static void non_public_app_rows(const RSource *source, groove_GraphBuilder *visible_root_with_routes, const jz_btree *root_route_fields, LoweredAppRows *rows) {
    const jz_str *row_id = &source->row_shape.row_uuid_field;
    jazz_engine_StrVec fields = jazz_engine__strvec_one(s_clone(row_id));
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(root_route_fields, f, unit) jazz_engine__strvec_push_clone(&fields, f);
    app_rows_empty_maps(rows);
    u32 n;
    const groove_DescriptorField *df = groove__records__RecordDescriptor__fields(source->row_shape.descriptor, &n);
    groove_DescriptorFieldVec kept = {0};
    for (u32 i = 0; i < n; i++) {
        if (!df[i].has_name || !jz_str_eq(&df[i].name, row_id)) continue;
        groove_DescriptorField c;
        groove__records__DescriptorField__Clone__clone(&c, &df[i]);
        JZ_VEC_PUSH(kept, c);
        jz_slice name;
        Crpf binding = F(descriptor_public_name)(&df[i], &name)
                           ? source_publication_field(source, jz_str_from_slice(name))
                           : crpf_result(s_clone(&df[i].name), jazz_engine__query_engine__publication__CurrentRowResultVisibility__current_row_metadata(JZ_S(&df[i].name)));
        crpf_put(&rows->publication_fields, s_clone(&df[i].name), binding);
    }
    rows->descriptor = groove__records__RecordDescriptor__new_with_fields(kept.ptr, kept.len);
    jz_free(kept.ptr);
    rows->graph = GB(project)(visible_root_with_routes, fields);
    rows->carrier = JAZZ_ENGINE_CARRIER_CURRENT_ROW;
    rows->terminal.tag = JAZZ_ENGINE_ART_DIRECT;
}

static jazz_engine_RowProjection row_projection(const char *name_or_null, const jz_str *name, groove_ColumnType ty, jazz_engine_NormalizedValueRef value) {
    jazz_engine_RowProjection p;
    memset(&p, 0, sizeof p);
    p.output.name = name_or_null ? s_lit(name_or_null) : s_clone(name);
    p.output.ty = ty;
    p.value = value;
    return p;
}
static jazz_engine_NormalizedValueRef nvr_root_row_id(const jazz_engine_SourceId *root) {
    jazz_engine_NormalizedValueRef v;
    memset(&v, 0, sizeof v);
    v.tag = JAZZ_ENGINE_NVR_ROW_ID;
    v.sub = JAZZ_ENGINE_ROWID_SOURCE;
    sid_clone(&v.u.source, root);
    return v;
}
static void projections_drop(jazz_engine_RowProjectionVec *v) { jz_vec_drop(v, sizeof(jazz_engine_RowProjection), jazz_engine__projection_drop_v); }

static CapErr relation_app_rows(const jazz_engine_AnalyzedQueryPlan *plan, const RSource *source, const jazz_model_RelationProjectColumnVec *requested,
                                groove_GraphBuilder *graph, const jz_btree *root_route_fields, LoweredAppRows *rows) {
    const jazz_engine_SourceId *root = JE_PLAN(AnalyzedQueryPlan__root_source)(plan);
    jazz_engine_RowProjectionVec columns = {0};
    const jazz_engine_RowProjectionVec *planned = last_project_columns(plan);
    if (planned) {
        jz_vec_clone(&columns, planned, sizeof(jazz_engine_RowProjection), jazz_engine__projection_clone_v);
    } else {
        JZ_VEC_PUSH(columns, row_projection("row_uuid", NULL, groove_vt_scalar(GROOVE_VT_UUID), nvr_root_row_id(root)));
        JZ_VEC_FOR(const jazz_model_RelationProjectColumn, req, *requested) {
            groove_ColumnType ty;
            jazz_engine_NormalizedValueRef value;
            if (req->expr_tag == JAZZ_MODEL_RPE_ROW_ID) {
                if (req->row_id != JAZZ_MODEL_RRI_CURRENT) {
                    projections_drop(&columns);
                    groove_gb_drop(graph);
                    return cap_op("outer/frontier row-id relation projections are not unified yet");
                }
                ty = groove_vt_scalar(GROOVE_VT_UUID);
                value = nvr_root_row_id(root);
            } else if (jz_str_eq_cstr(&req->column, "id")) {
                ty = groove_vt_scalar(GROOVE_VT_UUID);
                value = nvr_root_row_id(root);
            } else {
                const jazz_model_ColumnSchema *found = NULL;
                JZ_VEC_FOR(const jazz_model_ColumnSchema, column, source->table_schema->columns) if (jz_str_eq(&column->name, &req->column)) { found = column; break; }
                if (!found) {
                    jz_str q = dbg_str(&req->column);
                    CapErr e = jazz_engine__cap_operator_fmt("relation output column %.*s is absent from the source schema", JE_STR_ARG(q));
                    jz_str_drop(&q);
                    projections_drop(&columns);
                    groove_gb_drop(graph);
                    return e;
                }
                ty = groove_vt_clone(&found->column_type);
                memset(&value, 0, sizeof value);
                value.tag = JAZZ_ENGINE_NVR_SOURCE_FIELD;
                sid_clone(&value.u.source_field.source, root);
                value.u.source_field.field = s_clone(&req->column);
            }
            JZ_VEC_PUSH(columns, row_projection(NULL, &req->alias, ty, value));
        }
    }
    JZ_VEC(const jazz_engine_TypedOutputField *) outputs = {0};
    JZ_VEC_FOR(const jazz_engine_RowProjection, column, columns) JZ_VEC_PUSH(outputs, &column->output);
    rows->descriptor = descriptor_of_typed(outputs.ptr, outputs.len);
    jz_free(outputs.ptr);
    groove_ProjectFieldVec fields = {0};
    JZ_VEC_FOR(const jazz_engine_RowProjection, column, columns) pf_push(&fields, PF(named)(s_clone(&column->output.name)));
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(root_route_fields, f, unit) pf_push(&fields, PF(named)(s_clone(f)));
    rows->graph = GB(project_fields)(graph, fields);
    app_rows_empty_maps(rows);
    jz_btree requested_names;
    strset_init(&requested_names);
    JZ_VEC_FOR(const jazz_model_RelationProjectColumn, req, *requested) jazz_engine__strset_insert_clone(&requested_names, &req->alias);
    JZ_VEC_FOR(const jazz_engine_RowProjection, column, columns)
        if (!jz_str_eq_cstr(&column->output.name, "row_uuid") && !jz_btree_contains(&requested_names, &column->output.name))
            jazz_engine__strset_insert_clone(&rows->hidden_fields, &column->output.name);
    JZ_BTREE_FOR(&requested_names, f, unit) str_str_put(&rows->public_field_names, s_clone(f), s_clone(f));
    JZ_VEC_FOR(const jazz_model_RelationProjectColumn, req, *requested)
        crpf_put(&rows->publication_fields, s_clone(&req->alias), crpf_result(s_clone(&req->alias), JAZZ_ENGINE_CRRV_APPLICATION_CELL));
    jz_btree_drop(&requested_names);
    projections_drop(&columns);
    rows->carrier = JAZZ_ENGINE_CARRIER_LOGICAL;
    rows->terminal.tag = JAZZ_ENGINE_ART_DIRECT;
    return NULL;
}

/* consumes output_source_id and output_fields */
static CapErr projected_app_rows(jazz_engine_SourceId output_source_id, jazz_engine_TypedOutputFieldVec output_fields, const jz_btree *resolved_sources,
                                 groove_GraphBuilder *graph, const jz_btree *root_route_fields, LoweredAppRows *rows) {
    const RSource *output_source;
    CapErr e = JE_RES(ResolvedSource__lookup)(resolved_sources, &output_source_id, "projected output", &output_source);
    sid_drop(&output_source_id);
    if (e) { tofvec_drop(&output_fields); groove_gb_drop(graph); return e; }
    jz_btree hidden;
    hidden_source_fields(&output_source->row_shape, &hidden);
    JZ_VEC_FOR(const jazz_engine_TypedOutputField, field, output_fields)
        if (jazz_engine__str_starts_with(&field->name, "__flat_join_row_")) jazz_engine__strset_insert_clone(&hidden, &field->name);
    JZ_VEC(const jazz_engine_TypedOutputField *) public_fields = {0};
    JZ_VEC_FOR(const jazz_engine_TypedOutputField, field, output_fields) if (!jz_btree_contains(&hidden, &field->name)) JZ_VEC_PUSH(public_fields, field);
    groove_RecordDescriptor descriptor = descriptor_of_typed(public_fields.ptr, public_fields.len);
    /* Keep the session/parameter routes used to partition this terminal, even
     * though they are not public result columns. */
    groove_ProjectFieldVec fields = {0};
    for (u32 i = 0; i < public_fields.len; i++) pf_push(&fields, PF(named)(s_clone(&public_fields.ptr[i]->name)));
    const jz_str *f; const void *unit;
    JZ_BTREE_FOR(root_route_fields, f, unit) pf_push(&fields, PF(named)(s_clone(f)));
    rows->graph = GB(project_fields)(graph, fields);
    app_rows_empty_maps(rows);
    u32 n;
    const groove_DescriptorField *df = groove__records__RecordDescriptor__fields(descriptor, &n);
    for (u32 i = 0; i < public_fields.len; i++) {
        const jz_str *name = &public_fields.ptr[i]->name;
        u32 index;
        jz_slice pub;
        jz_str public_name;
        if (groove__records__RecordDescriptor__field_index(descriptor, name->ptr, name->len, &index) && index < n && F(descriptor_public_name)(&df[index], &pub))
            public_name = jz_str_from_slice(pub);
        else
            public_name = s_clone(name);
        str_str_put(&rows->public_field_names, s_clone(name), public_name);
        jz_str pname;
        if (!resolved_source_public_name(output_source, name, &pname)) pname = s_clone(name);
        crpf_put(&rows->publication_fields, s_clone(name), source_publication_field(output_source, pname));
    }
    rows->descriptor = descriptor;
    rows->carrier = JAZZ_ENGINE_CARRIER_LOGICAL;
    rows->terminal.tag = JAZZ_ENGINE_ART_DIRECT;
    jz_free(public_fields.ptr);
    jz_btree_drop(&hidden);
    tofvec_drop(&output_fields);
    return NULL;
}

static CapErr push_fact_terminal(jazz_engine_LoweredTerminalVec *terminals, const jazz_engine_ProgramFactKey *fact, jz_str sink, groove_GraphBuilder *input,
                                 const jazz_engine_AnalyzedQueryPlan *plan, const RSource *source, const jz_btree *resolved_sources,
                                 jazz_engine_LoweringContext *request, jazz_engine_ProgramFactOutput output) {
    jz_btree routes;
    JE_TERM(output_routing_fields)(&output, &routes);
    groove_GraphBuilder *graph;
    CapErr e = fact_terminal_graph(fact, input, plan, source, resolved_sources, request, routes, &graph);
    if (e) { jz_str_drop(&sink); JE_SCH(ProgramFactOutput__drop)(&output); return e; }
    term_push(terminals, sink, graph, ots_fact(output));
    return NULL;
}

/* consumes graph */
CapErr JE_TERM(lowered_terminals)(groove_GraphBuilder *graph, jazz_engine_LoweringContext *request, const jazz_engine_AnalyzedQueryPlan *plan,
                                  const RSource *source, const jz_btree *resolved_sources, const jazz_engine_ParameterDomain *parameter_domain,
                                  const jz_btree *routing_param_fields, const jz_btree *available_fields_in, jazz_engine_LoweredTerminalVec *out) {
    const jazz_engine_NvrVec *agg_g; const jazz_engine_AggregateExprVec *agg_o;
    if (JE_PLAN(root_aggregate_step)(plan, &agg_g, &agg_o))
        return lowered_aggregate_terminals(graph, request, plan, source, resolved_sources, parameter_domain, routing_param_fields, available_fields_in, out);
    const jazz_engine_QueryProgramRequest *rq = request->request;
    const jazz_engine_NormalizedRowSetShape *shape = &rq->input.shape;
    const jazz_engine_SourceId *root_id = JE_PLAN(AnalyzedQueryPlan__root_source)(plan);
    jazz_engine_LoweredTerminalVec terminals = {0};
    jz_btree available = {0}, initial_root_route_fields = {0}, root_occurrence_fields = {0}, initial_carriers = {0};
    jz_btree initial_visible_fields = {0}, root_route_fields = {0}, closure_carriers = {0}, visible_root_fields = {0};
    jz_btree member_route_scope = {0}, claim_route_fields = {0}, root_source_route_fields = {0}, members = {0};
    bool has_initial_visible = false, has_visible = false;
    jazz_engine_ClosureLowering initial_closure, closure;
    memset(&initial_closure, 0, sizeof initial_closure);
    memset(&closure, 0, sizeof closure);
    groove_GraphBuilder *visible_root_with_routes = NULL;
    jazz_engine_TypedOutputFieldVec occurrence = {0};
    CapErr e = NULL;
    jz_btree_init(&members, &jazz_engine__SOURCE_ID_GRAPH_MAP_DESC);

    /* Root-union occurrence carriers are introduced by graph lowering rather
     * than by a single source descriptor. They are still real fields on every
     * union branch and must survive both the closure and collector boundaries. */
    jazz_engine__strset_clone(&available, available_fields_in);
    if ((e = JE_TERM(root_join_occurrence_fields)(plan, resolved_sources, request, &occurrence))) goto fail;
    if (plan->tag == JAZZ_ENGINE_AQP_UNION) JZ_VEC_FOR(const jazz_engine_TypedOutputField, f, occurrence) jazz_engine__strset_insert_clone(&available, &f->name);
    strset_intersect(routing_param_fields, &available, &initial_root_route_fields);
    /* Closure evidence is an authorization boundary only for policy-claim
     * routes. Ordinary query parameters can be absent from provenance-only
     * closure graphs. (root_join_occurrence_fields is pure; computed once.) */
    strset_init(&root_occurrence_fields);
    JZ_VEC_FOR(const jazz_engine_TypedOutputField, f, occurrence)
        if (jz_btree_contains(&available, &f->name)) jazz_engine__strset_insert_clone(&root_occurrence_fields, &f->name);
    strset_union(&initial_root_route_fields, &root_occurrence_fields, &initial_carriers);
    if ((e = JE_CLO(lower_closure_membership)(groove_gb_clone(graph), request, plan, source, resolved_sources, &initial_root_route_fields, &initial_carriers,
                                              &initial_closure))) goto fail;
    /* The source graph's conservative field declaration can omit a route
     * carrier introduced by correlated/policy lowering. Discover it from the
     * actual post-policy residual root, then rebuild the complete closure with
     * that exact carrier. */
    has_initial_visible = JE_LOW(graph_declared_output_fields)(initial_closure.visible_root, &initial_visible_fields);
    if (has_initial_visible) strset_intersect(routing_param_fields, &initial_visible_fields, &root_route_fields);
    else jazz_engine__strset_clone(&root_route_fields, &initial_root_route_fields);
    strset_union(&root_route_fields, &root_occurrence_fields, &closure_carriers);
    if (jz_btree_eq(&root_route_fields, &initial_root_route_fields, NULL)) {
        closure = initial_closure;
        memset(&initial_closure, 0, sizeof initial_closure);
        has_visible = has_initial_visible;
        visible_root_fields = initial_visible_fields;
        memset(&initial_visible_fields, 0, sizeof initial_visible_fields);
    } else {
        JE_CLO(ClosureLowering__drop)(&initial_closure);
        memset(&initial_closure, 0, sizeof initial_closure);
        if ((e = JE_CLO(lower_closure_membership)(groove_gb_clone(graph), request, plan, source, resolved_sources, &root_route_fields, &closure_carriers,
                                                  &closure))) goto fail;
        has_visible = JE_LOW(graph_declared_output_fields)(closure.visible_root, &visible_root_fields);
    }
    /* Non-root covered members depend on the root's routes and on their own
     * policy routes, which the root need not carry. */
    strset_union(routing_param_fields, &root_route_fields, &member_route_scope);
    {
        const jazz_engine_ParameterDomain *domain;
        jazz_engine_UrErr ue = JE_ARG(LoweringContext__parameter_domain)(request, &domain);
        if (ue) { e = jazz_engine__ur_into_cap(ue); goto fail; }
        strset_init(&claim_route_fields);
        const jz_str *k; const void *claim;
        JZ_BTREE_FOR(&domain->claim_params, k, claim) if (jz_btree_contains(&root_route_fields, k)) jazz_engine__strset_insert_clone(&claim_route_fields, k);
    }
    /* The root's post-closure graph is the authority's actual admission
     * boundary. Keep that exact graph-local route set on root-only terminals;
     * child occurrences remain constrained by their own descriptors below. */
    if (has_visible) strset_intersect(&root_route_fields, &visible_root_fields, &root_source_route_fields);
    else source_terminal_route_fields(source, &root_route_fields, &root_source_route_fields);
    if (flat_join_nonempty(plan)) {
        if ((e = JE_TERM(source_rows_for_visible_graph)(source, groove_gb_clone(closure.visible_root), &root_source_route_fields, &visible_root_with_routes))) goto fail;
    } else if (root_source_route_fields.len == 0) {
        visible_root_with_routes = groove_gb_clone(closure.visible_root);
    } else {
        visible_root_with_routes = GB(project_fields)(groove_gb_clone(closure.visible_root), JE_TERM(project_source_fields_with_routes)(source, &root_source_route_fields));
    }
    /* Source facts remain partitioned by every authority route, including
     * policy claims. */
    {
        const jazz_engine_SourceId *sid;
        groove_GraphBuilder *const *g;
        JZ_BTREE_FOR(&closure.result_members, sid, g) {
            const RSource *rs;
            if ((e = JE_RES(ResolvedSource__lookup)(resolved_sources, sid, "closure", &rs))) goto fail;
            jz_btree rf;
            terminal_route_fields_for_source(sid, rs, root_id, &member_route_scope, &root_source_route_fields, &rf);
            groove_GraphBuilder *projected = GB(project_fields)(groove_gb_clone(*g), JE_TERM(project_source_fields_with_routes)(rs, &rf));
            jz_btree_drop(&rf);
            jazz_engine_SourceId key = jazz_engine__sid_cloned(sid);
            jz_btree_insert(&members, &key, &projected, NULL);
        }
    }
    JE_LOW(union_member)(&members, jazz_engine__sid_cloned(root_id), groove_gb_clone(visible_root_with_routes));
    /* A flat join has no explicit include path, but every join-side row that
     * contributes to a public root is still receiver input. Derive each
     * contributor from the post-policy visible root. */
    JZ_VEC_FOR(const jazz_engine_JoinContribution, contribution, shape->join_contributions) {
        const RSource *rs;
        if ((e = JE_RES(ResolvedSource__lookup)(resolved_sources, &contribution->source, "join contribution", &rs))) goto fail;
        jz_btree rf;
        source_terminal_route_fields(rs, &member_route_scope, &rf);
        groove_GraphBuilder *g = NULL;
        if (jazz_engine__str_starts_with(&contribution->id, "flat_join:")) {
            e = JE_CLO(flat_join_contribution_membership_graph)(groove_gb_clone(closure.visible_root), contribution, rs, &shape->nodes, resolved_sources, request, &rf, &g);
        } else {
            groove_GraphBuilder *visible_parent;
            const RSource *parent_source;
            if (contribution->parent) {
                groove_GraphBuilder **p = jz_btree_get(&members, contribution->parent);
                if (!p) e = cap_rt("nested join contribution requires its admitted parent");
                else if (!(parent_source = jz_btree_get(resolved_sources, contribution->parent))) e = cap_rt("nested join parent source was not resolved");
                else visible_parent = groove_gb_clone(*p);
            } else {
                visible_parent = groove_gb_clone(closure.visible_root);
                parent_source = source;
            }
            if (!e) e = JE_CLO(join_contribution_membership_graph)(visible_parent, contribution, parent_source, rs, &shape->nodes, resolved_sources, request, &rf, &g);
        }
        jz_btree_drop(&rf);
        if (e) goto fail;
        JE_LOW(union_member)(&members, jazz_engine__sid_cloned(&contribution->source), g);
    }
    /* A caller-requested `inherits` is a receiver-local semi-join. Its parent
     * source must cross the authority boundary, but only after the authority
     * has applied the parent read policy. */
    JZ_VEC_FOR(const jazz_engine_InheritedContribution, contribution, shape->inherited_contributions) {
        const RSource *parent_source;
        if ((e = JE_RES(ResolvedSource__lookup)(resolved_sources, &contribution->source, "inherited contribution", &parent_source))) goto fail;
        jz_btree rf;
        source_terminal_route_fields(parent_source, &member_route_scope, &rf);
        groove_GraphBuilder *g;
        e = JE_CLO(inherited_contribution_membership_graph)(groove_gb_clone(visible_root_with_routes), contribution, source, parent_source, &shape->nodes,
                                                            resolved_sources, request, &rf, &g);
        jz_btree_drop(&rf);
        if (e) goto fail;
        JE_LOW(union_member)(&members, jazz_engine__sid_cloned(&contribution->source), g);
    }
    /* Recursive reachability is receiver-local semantics too. Its admitted
     * access rows are exact source inputs, not authority-only proof. */
    JZ_VEC_FOR(const jazz_engine_ReachableContribution, contribution, shape->reachable_contributions) {
        const RSource *access_source;
        if ((e = JE_RES(ResolvedSource__lookup)(resolved_sources, &contribution->access_source, "reachable contribution", &access_source))) goto fail;
        jz_btree rf;
        source_terminal_route_fields(access_source, &member_route_scope, &rf);
        groove_GraphBuilder *g;
        e = JE_CLO(reachable_contribution_membership_graph)(groove_gb_clone(visible_root_with_routes), contribution, source, access_source, &shape->nodes,
                                                            resolved_sources, request, &rf, &g);
        jz_btree_drop(&rf);
        if (e) goto fail;
        JE_LOW(union_member)(&members, jazz_engine__sid_cloned(&contribution->access_source), g);
        if ((e = JE_CLO(reachable_step_witness_membership_graph)(contribution, &shape->nodes, resolved_sources, request, &root_route_fields, &g))) goto fail;
        JE_LOW(union_member)(&members, jazz_engine__sid_cloned(&contribution->edge_source), g);
        bool has_seed;
        jazz_engine_SourceId seed_source;
        if ((e = JE_CLO(reachable_seed_membership_graph)(contribution, &shape->nodes, resolved_sources, request, &root_route_fields, &has_seed, &seed_source, &g)))
            goto fail;
        if (has_seed) JE_LOW(union_member)(&members, seed_source, g);
    }
    /* A correlated collector owns a distinct compiled source occurrence for each
     * child path. Derive every child source from the already admitted root
     * frontier, then publish it under that child occurrence's own descriptor. */
    if (plan->tag == JAZZ_ENGINE_AQP_CORRELATED_PATH &&
        (e = collect_correlated_covered_source_members(&plan->u.correlated, groove_gb_clone(visible_root_with_routes), source, resolved_sources, request,
                                                       &root_route_fields, &members))) goto fail;

    if (rq->output.has_app_rows) {
        const jazz_engine_AppRowOutputRequest *app_rows = &rq->output.app_rows;
        LoweredAppRows rows;
        memset(&rows, 0, sizeof rows);
        if (!app_rows->public_terminal) {
            non_public_app_rows(source, groove_gb_clone(visible_root_with_routes), &root_route_fields, &rows);
        } else if (app_rows->projection.tag == JAZZ_ENGINE_PP_RELATION) {
            e = relation_app_rows(plan, source, &app_rows->projection.relation, groove_gb_clone(graph), &root_route_fields, &rows);
        } else if (app_rows->projection.tag == JAZZ_ENGINE_PP_TREE) {
            e = lower_collect_by_app_rows(groove_gb_clone(closure.visible_root), &app_rows->projection.tree, plan, source, resolved_sources, request,
                                          &root_route_fields, &available, &rows);
        } else {
            jazz_engine_SourceId output_source_id;
            jazz_engine_TypedOutputFieldVec output_fields;
            if (projected_multisource_terminal(plan, source, &output_source_id, &output_fields)) {
                e = projected_app_rows(output_source_id, output_fields, resolved_sources, groove_gb_clone(graph), &root_route_fields, &rows);
            } else {
                jazz_engine_AppProjectionTree all;
                memset(&all, 0, sizeof all);
                all.fields.tag = JAZZ_ENGINE_FP_ALL;
                e = lower_collect_by_app_rows(groove_gb_clone(closure.visible_root), &all, plan, source, resolved_sources, request, &root_route_fields,
                                              &available, &rows);
            }
        }
        if (e) goto fail;
        /* Prepared multisinks bind route parameters at every terminal. Keep the
         * same compiler-owned route carriers in the app-row schema as in the
         * source-closure terminals. */
        strset_extend_clone(&rows.hidden_fields, &root_route_fields);
        jazz_engine_OutputTerminalSchema output;
        memset(&output, 0, sizeof output);
        output.tag = JAZZ_ENGINE_OTS_APP_ROWS;
        jazz_engine_AppRowSchema *schema = &output.u.app_rows;
        schema->publication_fields = rows.publication_fields;
        schema->descriptor = rows.descriptor;
        schema->hidden_fields = rows.hidden_fields;
        schema->carrier = rows.carrier;
        schema->field_carriers = rows.field_carriers;
        schema->public_field_names = rows.public_field_names;
        schema->terminal = rows.terminal;
        schema->root_union_arm = plan->tag == JAZZ_ENGINE_AQP_UNION;
        term_push(&terminals, s_lit("app_rows"), rows.graph, output);
    }

    bool flat_join = flat_join_nonempty(plan);
    const jazz_engine_ProgramFactKey *fact; const void *unit;
    JZ_BTREE_FOR(&rq->output.facts, fact, unit) {
        u8 flavour;
        jazz_engine_ProgramFactOutput output;
        if (fact->tag == JAZZ_ENGINE_FACT_RESULT_MEMBERSHIP) {
            jz_btree routes;
            jazz_engine__strset_clone(&routes, routing_param_fields);
            if ((e = fact_output(fact, plan, root_id, source, resolved_sources, request, routes, &output))) goto fail;
            /* Closure membership is a root-row projection: it may discard
             * output-only columns while constructing the root closure. A flat
             * join instead needs its complete already-policy-filtered tuple
             * here, including the internal contributor ids. */
            bool occurrence_addressed = output.schema.tag == JAZZ_ENGINE_PFS_RESULT_MEMBERSHIP && output.schema.u.result_membership.occurrence_id_fields.len > 1;
            groove_GraphBuilder *input = !flat_join && !occurrence_addressed ? groove_gb_clone(visible_root_with_routes) : groove_gb_clone(graph);
            if ((e = push_fact_terminal(&terminals, fact, fact_sink_name(fact), input, plan, source, resolved_sources, request, output))) goto fail;
            /* A flat join's one wide primary terminal is its public output. The
             * ordinary closure terminals are source-only membership facts. */
            if (!flat_join) {
                const jazz_engine_SourceId *sid;
                groove_GraphBuilder *const *closure_graph;
                JZ_BTREE_FOR(&closure.result_members, sid, closure_graph) {
                    const RSource *rs;
                    if ((e = JE_RES(ResolvedSource__lookup)(resolved_sources, sid, "closure member", &rs))) goto fail;
                    jazz_engine__strset_clone(&routes, &claim_route_fields);
                    if ((e = fact_output_with_terminal(fact, JAZZ_ENGINE_PFT_PRIMARY, plan, sid, rs, resolved_sources, request, routes, &output))) goto fail;
                    if ((e = push_fact_terminal(&terminals, fact, scoped_fact_sink_name(fact, sid), groove_gb_clone(*closure_graph), plan, rs, resolved_sources,
                                                request, output))) goto fail;
                }
            }
            if (JE_GL(has_explicit_closure_path)(shape)) {
                JZ_VEC_FOR(const jazz_engine_JoinContribution, contribution, shape->join_contributions) {
                    const RSource *rs;
                    if ((e = JE_RES(ResolvedSource__lookup)(resolved_sources, &contribution->source, "join contribution", &rs))) goto fail;
                    jazz_engine__strset_clone(&routes, &claim_route_fields);
                    if ((e = fact_output_with_terminal(fact, JAZZ_ENGINE_PFT_PRIMARY, plan, &contribution->source, rs, resolved_sources, request, routes, &output)))
                        goto fail;
                    groove_GraphBuilder **contribution_graph = jz_btree_get(&members, &contribution->source);
                    if (!contribution_graph) {
                        JE_SCH(ProgramFactOutput__drop)(&output);
                        e = cap_rt("join contribution has no admitted source rows");
                        goto fail;
                    }
                    if ((e = push_fact_terminal(&terminals, fact, scoped_fact_sink_name(fact, &contribution->source), groove_gb_clone(*contribution_graph), plan, rs,
                                                resolved_sources, request, output))) goto fail;
                }
            }
        } else if (witness_flavour_of(fact, &flavour)) {
            const jazz_engine_SourceId *sid;
            const RSource *rs;
            JZ_BTREE_FOR(resolved_sources, sid, rs) {
                /* Coverage is emitted independently below. An exact source with
                 * no visible contribution has an empty input relation, not an
                 * authorization to reopen its storage relation for version facts. */
                groove_GraphBuilder **covered = jz_btree_get(&members, sid);
                if (!covered) continue;
                jz_btree rf;
                terminal_route_fields_for_source(sid, rs, root_id, &member_route_scope, &root_source_route_fields, &rf);
                e = push_witness_terminals(&terminals, flavour, fact, plan, sid, rs, groove_gb_clone(*covered), &rf, resolved_sources, request, parameter_domain);
                jz_btree_drop(&rf);
                if (e) goto fail;
            }
        } else if (fact->tag == JAZZ_ENGINE_FACT_PROGRAM_SOURCE_COVERAGE) {
            /* Closure coverage is control-plane evidence, not a projection of
             * source rows. Seed exactly one complete receipt for every compiled
             * source occurrence. */
            const jazz_engine_SourceId *sid;
            groove_GraphBuilder *const *unused;
            JZ_BTREE_FOR(&members, sid, unused) {
                const RSource *rs = jz_btree_get(resolved_sources, sid);
                if (!rs) { e = cap_rt("covered source has no resolved descriptor"); goto fail; }
                jz_btree rf, routes;
                terminal_route_fields_for_source(sid, rs, root_id, &member_route_scope, &root_source_route_fields, &rf);
                jazz_engine__strset_clone(&routes, &rf);
                if ((e = fact_output_with_terminal(fact, JAZZ_ENGINE_PFT_PRIMARY, plan, sid, rs, resolved_sources, request, routes, &output))) { jz_btree_drop(&rf); goto fail; }
                if (output.schema.tag != JAZZ_ENGINE_PFS_PROGRAM_SOURCE_COVERAGE) jz_panic("program-source coverage key has matching schema");
                groove_GraphBuilder *g;
                e = program_source_coverage_graph(request, parameter_domain, output.schema.u.program_source_coverage.complete, &rf, &g);
                jz_btree_drop(&rf);
                if (e) { JE_SCH(ProgramFactOutput__drop)(&output); goto fail; }
                term_push(&terminals, scoped_fact_sink_name(fact, sid), g, ots_fact(output));
            }
        } else {
            jz_btree terminal_route_fields, routes;
            if (fact->tag == JAZZ_ENGINE_FACT_AUTHORIZED_ROWS) jazz_engine__strset_clone(&terminal_route_fields, &root_route_fields);
            else strset_init(&terminal_route_fields);
            jazz_engine__strset_clone(&routes, &terminal_route_fields);
            if ((e = fact_output(fact, plan, root_id, source, resolved_sources, request, routes, &output))) { jz_btree_drop(&terminal_route_fields); goto fail; }
            groove_GraphBuilder *terminal_graph, *g;
            if ((e = fact_input_graph(fact, groove_gb_clone(graph), plan, source, resolved_sources, request, &terminal_graph))) {
                jz_btree_drop(&terminal_route_fields);
                JE_SCH(ProgramFactOutput__drop)(&output);
                goto fail;
            }
            if ((e = fact_terminal_graph(fact, terminal_graph, plan, source, resolved_sources, request, terminal_route_fields, &g))) {
                JE_SCH(ProgramFactOutput__drop)(&output);
                goto fail;
            }
            term_push(&terminals, fact_sink_name(fact), g, ots_fact(output));
        }
    }
    *out = terminals;
    memset(&terminals, 0, sizeof terminals);
fail:
    terminals_drop(&terminals);
    groove_gb_drop(graph);
    groove_gb_drop(visible_root_with_routes);
    JE_CLO(ClosureLowering__drop)(&initial_closure);
    JE_CLO(ClosureLowering__drop)(&closure);
    tofvec_drop(&occurrence);
    jz_btree_drop(&available);
    jz_btree_drop(&initial_root_route_fields);
    jz_btree_drop(&root_occurrence_fields);
    jz_btree_drop(&initial_carriers);
    jz_btree_drop(&initial_visible_fields);
    jz_btree_drop(&root_route_fields);
    jz_btree_drop(&closure_carriers);
    jz_btree_drop(&visible_root_fields);
    jz_btree_drop(&member_route_scope);
    jz_btree_drop(&claim_route_fields);
    jz_btree_drop(&root_source_route_fields);
    jz_btree_drop(&members);
    return e;
}
