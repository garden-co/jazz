/* schemas.rs: typed output schemas and their derived impls. */
#include "mod.h"

#define SCLONE(f) dst->f = jz_str_clone(&src->f)
#define SDROP(f) jz_str_drop(&s->f)
#define OCLONE(f) jazz_engine__optstr_clone(&dst->f, &src->f)
#define ODROP(f) jazz_engine__optstr_drop(&s->f)
#define SETCLONE(f) jazz_engine__strset_clone(&dst->f, &src->f)

void jazz_engine__optstr_clone(jazz_engine_OptStr *dst, const jazz_engine_OptStr *src) {
    dst->some = src->some;
    dst->s = src->some ? jz_str_clone(&src->s) : (jz_str){0};
}
void jazz_engine__optstr_drop(jazz_engine_OptStr *s) { jz_str_drop(&s->s); s->some = false; }
jazz_engine_OptStr jazz_engine__optstr_some(jz_str s) { jazz_engine_OptStr o = {true, s}; return o; }
void jazz_engine__optstr_debug(jz_str *out, const jazz_engine_OptStr *s) {
    if (!s->some) {
        jazz_engine__fmt__option_none(out);
        return;
    }
    jazz_engine__fmt__tuple_begin(out, "Some");
    jazz_engine__fmt__str(out, &s->s);
    jazz_engine__fmt__tuple_end(out);
}
static bool optstr_eq(const jazz_engine_OptStr *a, const jazz_engine_OptStr *b) { return a->some == b->some && (!a->some || jz_str_eq(&a->s, &b->s)); }
void jazz_engine__descriptor_field_drop_v(void *f) { groove__records__DescriptorField__drop(f); }
void jazz_engine__descriptor_field_clone_v(void *d, const void *s) { groove__records__DescriptorField__Clone__clone(d, s); }
static void u8_clone_v(void *d, const void *s) { *(u8 *)d = *(const u8 *)s; }
static int u32_cmp_v(const void *a, const void *b, void *ctx) { u32 x = *(const u32 *)a, y = *(const u32 *)b; return x < y ? -1 : x > y; }
static void u32_clone_v(void *d, const void *s) { *(u32 *)d = *(const u32 *)s; }
const jz_btree_desc jazz_engine__STR_CRPF_MAP_DESC = {sizeof(jz_str), sizeof(jazz_engine_CurrentRowPublicationField), jz_cmp_str, jazz_engine__str_drop_v, jazz_engine__crpf_drop_v};
const jz_btree_desc jazz_engine__STR_U8_MAP_DESC = {sizeof(jz_str), sizeof(u8), jz_cmp_str, jazz_engine__str_drop_v, NULL};
const jz_btree_desc jazz_engine__U32_STR_MAP_DESC = {sizeof(u32), sizeof(jz_str), u32_cmp_v, NULL, jazz_engine__str_drop_v};

/* ---------------------------------------------------------------- aggregate / app rows */
void JE_SCH(SyntheticResultMembershipSchema__Clone__clone)(jazz_engine_SyntheticResultMembershipSchema *dst, const jazz_engine_SyntheticResultMembershipSchema *src) {
    SCLONE(table_field); SCLONE(row_field); SCLONE(replacement_field); SETCLONE(routing_param_fields);
}
void JE_SCH(SyntheticResultMembershipSchema__drop)(jazz_engine_SyntheticResultMembershipSchema *s) {
    SDROP(table_field); SDROP(row_field); SDROP(replacement_field); jz_btree_drop(&s->routing_param_fields);
}
static void synthetic_debug(jz_str *out, const jazz_engine_SyntheticResultMembershipSchema *s) {
    jazz_engine__fmt__struct_begin(out, "SyntheticResultMembershipSchema");
    jazz_engine__fmt__field(out, "table_field", true);
    jazz_engine__fmt__str(out, &s->table_field);
    jazz_engine__fmt__field(out, "row_field", false);
    jazz_engine__fmt__str(out, &s->row_field);
    jazz_engine__fmt__field(out, "replacement_field", false);
    jazz_engine__fmt__str(out, &s->replacement_field);
    jazz_engine__fmt__field(out, "routing_param_fields", false);
    jazz_engine__fmt__strset(out, &s->routing_param_fields);
    jazz_engine__fmt__struct_end(out);
}
void JE_SCH(AggregateResultSchema__Clone__clone)(jazz_engine_AggregateResultSchema *dst, const jazz_engine_AggregateResultSchema *src) {
    JE_SCH(SyntheticResultMembershipSchema__Clone__clone)(&dst->synthetic, &src->synthetic);
    jz_vec_clone(&dst->group_key_fields, &src->group_key_fields, sizeof(groove_DescriptorField), jazz_engine__descriptor_field_clone_v);
    jazz_engine__strvec_clone(&dst->group_names, &src->group_names);
    jz_vec_clone(&dst->value_fields, &src->value_fields, sizeof(groove_DescriptorField), jazz_engine__descriptor_field_clone_v);
    jazz_engine__strvec_clone(&dst->value_names, &src->value_names);
    SETCLONE(routing_param_fields);
}
void JE_SCH(AggregateResultSchema__drop)(jazz_engine_AggregateResultSchema *s) {
    JE_SCH(SyntheticResultMembershipSchema__drop)(&s->synthetic);
    jz_vec_drop(&s->group_key_fields, sizeof(groove_DescriptorField), jazz_engine__descriptor_field_drop_v);
    jazz_engine__strvec_drop(&s->group_names);
    jz_vec_drop(&s->value_fields, sizeof(groove_DescriptorField), jazz_engine__descriptor_field_drop_v);
    jazz_engine__strvec_drop(&s->value_names);
    jz_btree_drop(&s->routing_param_fields);
}
void JE_SCH(AppRowSchema__Clone__clone)(jazz_engine_AppRowSchema *dst, const jazz_engine_AppRowSchema *src) {
    jz_btree_clone(&dst->publication_fields, &src->publication_fields, jazz_engine__str_clone_v, jazz_engine__crpf_clone_v);
    dst->descriptor = src->descriptor;
    SETCLONE(hidden_fields);
    dst->carrier = src->carrier;
    jz_btree_clone(&dst->field_carriers, &src->field_carriers, jazz_engine__str_clone_v, u8_clone_v);
    jz_btree_clone(&dst->public_field_names, &src->public_field_names, jazz_engine__str_clone_v, jazz_engine__str_clone_v);
    dst->terminal.tag = src->terminal.tag;
    memset(&dst->terminal.aggregate, 0, sizeof dst->terminal.aggregate);
    if (src->terminal.tag == JAZZ_ENGINE_ART_AGGREGATE) JE_SCH(AggregateResultSchema__Clone__clone)(&dst->terminal.aggregate, &src->terminal.aggregate);
    dst->root_union_arm = src->root_union_arm;
}
void JE_SCH(AppRowSchema__drop)(jazz_engine_AppRowSchema *s) {
    jz_btree_drop(&s->publication_fields);
    jz_btree_drop(&s->hidden_fields);
    jz_btree_drop(&s->field_carriers);
    jz_btree_drop(&s->public_field_names);
    if (s->terminal.tag == JAZZ_ENGINE_ART_AGGREGATE) JE_SCH(AggregateResultSchema__drop)(&s->terminal.aggregate);
}
static void carrier_debug_v(jz_str *out, const void *c) { jz_str_push_cstr(out, *(const u8 *)c == JAZZ_ENGINE_CARRIER_CURRENT_ROW ? "CurrentRow" : "Logical"); }
static void crpf_debug_v(jz_str *out, const void *f) { jazz_engine__query_engine__publication__CurrentRowPublicationField__Debug__fmt(out, f); }
static void descriptor_field_debug_v(jz_str *out, const void *v) {
    const groove_DescriptorField *f = v;
    jazz_engine__fmt__struct_begin(out, "DescriptorField");
    jazz_engine__fmt__field(out, "name", true);
    if (f->has_name) {
        jazz_engine__fmt__tuple_begin(out, "Some");
        jazz_engine__fmt__str(out, &f->name);
        jazz_engine__fmt__tuple_end(out);
    } else {
        jazz_engine__fmt__option_none(out);
    }
    jazz_engine__fmt__field(out, "value_type", false);
    groove__records__values__ValueType__Debug__fmt(out, &f->value_type);
    jazz_engine__fmt__struct_end(out);
}
void JE_SCH(AppRowSchema__Debug__fmt)(jz_str *out, const jazz_engine_AppRowSchema *s) {
    jazz_engine__fmt__struct_begin(out, "AppRowSchema");
    jazz_engine__fmt__field(out, "publication_fields", true);
    jazz_engine__fmt__map(out, &s->publication_fields, jazz_engine__fmt__str_v, crpf_debug_v);
    jazz_engine__fmt__field(out, "descriptor", false);
    groove__records__RecordDescriptor__Debug__fmt(out, s->descriptor);
    jazz_engine__fmt__field(out, "hidden_fields", false);
    jazz_engine__fmt__strset(out, &s->hidden_fields);
    jazz_engine__fmt__field(out, "carrier", false);
    carrier_debug_v(out, &s->carrier);
    jazz_engine__fmt__field(out, "field_carriers", false);
    jazz_engine__fmt__map(out, &s->field_carriers, jazz_engine__fmt__str_v, carrier_debug_v);
    jazz_engine__fmt__field(out, "public_field_names", false);
    jazz_engine__fmt__map(out, &s->public_field_names, jazz_engine__fmt__str_v, jazz_engine__fmt__str_v);
    jazz_engine__fmt__field(out, "terminal", false);
    switch (s->terminal.tag) {
    case JAZZ_ENGINE_ART_DIRECT: jz_str_push_cstr(out, "Direct"); break;
    case JAZZ_ENGINE_ART_ROOT_COLLECTOR: jz_str_push_cstr(out, "RootCollector"); break;
    case JAZZ_ENGINE_ART_AGGREGATE: {
        const jazz_engine_AggregateResultSchema *a = &s->terminal.aggregate;
        jazz_engine__fmt__tuple_begin(out, "Aggregate");
        jazz_engine__fmt__struct_begin(out, "AggregateResultSchema");
        jazz_engine__fmt__field(out, "synthetic", true);
        synthetic_debug(out, &a->synthetic);
        jazz_engine__fmt__field(out, "group_key_fields", false);
        jazz_engine__fmt__list(out, a->group_key_fields.ptr, a->group_key_fields.len, sizeof(groove_DescriptorField), descriptor_field_debug_v);
        jazz_engine__fmt__field(out, "group_names", false);
        jazz_engine__fmt__strvec(out, &a->group_names);
        jazz_engine__fmt__field(out, "value_fields", false);
        jazz_engine__fmt__list(out, a->value_fields.ptr, a->value_fields.len, sizeof(groove_DescriptorField), descriptor_field_debug_v);
        jazz_engine__fmt__field(out, "value_names", false);
        jazz_engine__fmt__strvec(out, &a->value_names);
        jazz_engine__fmt__field(out, "routing_param_fields", false);
        jazz_engine__fmt__strset(out, &a->routing_param_fields);
        jazz_engine__fmt__struct_end(out);
        jazz_engine__fmt__tuple_end(out);
        break;
    }
    }
    jazz_engine__fmt__field(out, "root_union_arm", false);
    jazz_engine__fmt__bool(out, s->root_union_arm);
    jazz_engine__fmt__struct_end(out);
}

/* ---------------------------------------------------------------- version identities */
void JE_SCH(VersionIdentityFields__Clone__clone)(jazz_engine_VersionIdentityFields *dst, const jazz_engine_VersionIdentityFields *src) {
    SCLONE(table_field); SCLONE(row_field); SCLONE(tx_time_field); SCLONE(tx_node_field);
    OCLONE(batch_id_field); OCLONE(branch_or_prefix_field); OCLONE(row_digest_field);
    SCLONE(schema_field); SCLONE(layer_field);
}
void JE_SCH(VersionIdentityFields__drop)(jazz_engine_VersionIdentityFields *s) {
    SDROP(table_field); SDROP(row_field); SDROP(tx_time_field); SDROP(tx_node_field);
    ODROP(batch_id_field); ODROP(branch_or_prefix_field); ODROP(row_digest_field);
    SDROP(schema_field); SDROP(layer_field);
}
static bool identity_eq(const jazz_engine_VersionIdentityFields *a, const jazz_engine_VersionIdentityFields *b) {
    return jz_str_eq(&a->table_field, &b->table_field) && jz_str_eq(&a->row_field, &b->row_field) &&
           jz_str_eq(&a->tx_time_field, &b->tx_time_field) && jz_str_eq(&a->tx_node_field, &b->tx_node_field) &&
           optstr_eq(&a->batch_id_field, &b->batch_id_field) && optstr_eq(&a->branch_or_prefix_field, &b->branch_or_prefix_field) &&
           optstr_eq(&a->row_digest_field, &b->row_digest_field) && jz_str_eq(&a->schema_field, &b->schema_field) &&
           jz_str_eq(&a->layer_field, &b->layer_field);
}
static void str_field(jz_str *out, const char *name, const jz_str *v, bool first) {
    jazz_engine__fmt__field(out, name, first);
    jazz_engine__fmt__str(out, v);
}
static void opt_field(jz_str *out, const char *name, const jazz_engine_OptStr *v) {
    jazz_engine__fmt__field(out, name, false);
    jazz_engine__optstr_debug(out, v);
}
void JE_SCH(VersionIdentityFields__Debug__fmt)(jz_str *out, const jazz_engine_VersionIdentityFields *f) {
    jazz_engine__fmt__struct_begin(out, "VersionIdentityFields");
    str_field(out, "table_field", &f->table_field, true);
    str_field(out, "row_field", &f->row_field, false);
    str_field(out, "tx_time_field", &f->tx_time_field, false);
    str_field(out, "tx_node_field", &f->tx_node_field, false);
    opt_field(out, "batch_id_field", &f->batch_id_field);
    opt_field(out, "branch_or_prefix_field", &f->branch_or_prefix_field);
    opt_field(out, "row_digest_field", &f->row_digest_field);
    str_field(out, "schema_field", &f->schema_field, false);
    str_field(out, "layer_field", &f->layer_field, false);
    jazz_engine__fmt__struct_end(out);
}
void JE_SCH(ResultMembershipVersionSchema__Clone__clone)(jazz_engine_ResultMembershipVersionSchema *dst, const jazz_engine_ResultMembershipVersionSchema *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    SCLONE(content.tx_time_field);
    SCLONE(content.tx_node_field);
    if (src->tag == JAZZ_ENGINE_RMV_CONTENT_OR_DELETION) {
        JE_SCH(VersionIdentityFields__Clone__clone)(&dst->deletion, &src->deletion);
        SCLONE(deletion_state_field);
    }
}
void JE_SCH(ResultMembershipVersionSchema__drop)(jazz_engine_ResultMembershipVersionSchema *s) {
    SDROP(content.tx_time_field);
    SDROP(content.tx_node_field);
    if (s->tag == JAZZ_ENGINE_RMV_CONTENT_OR_DELETION) {
        JE_SCH(VersionIdentityFields__drop)(&s->deletion);
        SDROP(deletion_state_field);
    }
}
static void content_debug(jz_str *out, const jazz_engine_ContentVersionFields *c) {
    jazz_engine__fmt__struct_begin(out, "ContentVersionFields");
    str_field(out, "tx_time_field", &c->tx_time_field, true);
    str_field(out, "tx_node_field", &c->tx_node_field, false);
    jazz_engine__fmt__struct_end(out);
}
void JE_SCH(ResultMembershipVersionSchema__Debug__fmt)(jz_str *out, const jazz_engine_ResultMembershipVersionSchema *v) {
    if (v->tag == JAZZ_ENGINE_RMV_CONTENT) {
        jazz_engine__fmt__tuple_begin(out, "Content");
        content_debug(out, &v->content);
        jazz_engine__fmt__tuple_end(out);
        return;
    }
    jazz_engine__fmt__struct_begin(out, "ContentOrDeletion");
    jazz_engine__fmt__field(out, "content", true);
    content_debug(out, &v->content);
    jazz_engine__fmt__field(out, "deletion", false);
    JE_SCH(VersionIdentityFields__Debug__fmt)(out, &v->deletion);
    str_field(out, "deletion_state_field", &v->deletion_state_field, false);
    jazz_engine__fmt__struct_end(out);
}
void JE_SCH(VersionedRowRefSchema__Clone__clone)(jazz_engine_VersionedRowRefSchema *dst, const jazz_engine_VersionedRowRefSchema *src) {
    SCLONE(row.source_field); SCLONE(row.table_field); SCLONE(row.row_field);
    dst->has_version = src->has_version;
    if (src->has_version) JE_SCH(ResultMembershipVersionSchema__Clone__clone)(&dst->version, &src->version);
    else memset(&dst->version, 0, sizeof dst->version);
    OCLONE(branch_or_prefix_field);
}
void JE_SCH(VersionedRowRefSchema__drop)(jazz_engine_VersionedRowRefSchema *s) {
    SDROP(row.source_field); SDROP(row.table_field); SDROP(row.row_field);
    if (s->has_version) JE_SCH(ResultMembershipVersionSchema__drop)(&s->version);
    ODROP(branch_or_prefix_field);
}
void JE_SCH(VersionedRowRefSchema__Debug__fmt)(jz_str *out, const jazz_engine_VersionedRowRefSchema *v) {
    jazz_engine__fmt__struct_begin(out, "VersionedRowRefSchema");
    jazz_engine__fmt__field(out, "row", true);
    jazz_engine__fmt__struct_begin(out, "RowRefSchema");
    str_field(out, "source_field", &v->row.source_field, true);
    str_field(out, "table_field", &v->row.table_field, false);
    str_field(out, "row_field", &v->row.row_field, false);
    jazz_engine__fmt__struct_end(out);
    jazz_engine__fmt__field(out, "version", false);
    if (v->has_version) {
        jazz_engine__fmt__tuple_begin(out, "Some");
        JE_SCH(ResultMembershipVersionSchema__Debug__fmt)(out, &v->version);
        jazz_engine__fmt__tuple_end(out);
    } else {
        jazz_engine__fmt__option_none(out);
    }
    opt_field(out, "branch_or_prefix_field", &v->branch_or_prefix_field);
    jazz_engine__fmt__struct_end(out);
}

/* ---------------------------------------------------------------- witnesses */
static void program_source_id_clone(jazz_protocol_ProgramSourceId *dst, const jazz_protocol_ProgramSourceId *src) {
    dst->table = src->table;
    memset(&dst->path, 0, sizeof dst->path);
    jz_vec_extend(&dst->path, src->path.ptr, src->path.len, 1);
}
void JE_SCH(VersionWitnessSchema__Clone__clone)(jazz_engine_VersionWitnessSchema *dst, const jazz_engine_VersionWitnessSchema *src) {
    program_source_id_clone(&dst->source, &src->source);
    dst->descriptor = src->descriptor;
    JE_SCH(VersionIdentityFields__Clone__clone)(&dst->identity, &src->identity);
    SCLONE(created_by_field); SCLONE(created_at_field); SCLONE(updated_by_field); SCLONE(updated_at_field);
    SCLONE(authored_columns_field); SCLONE(deletion_field);
    jz_btree_clone(&dst->user_fields, &src->user_fields, jazz_engine__str_clone_v, jazz_engine__str_clone_v);
}
void JE_SCH(VersionWitnessSchema__drop)(jazz_engine_VersionWitnessSchema *s) {
    jz_vec_drop(&s->source.path, 1, NULL);
    JE_SCH(VersionIdentityFields__drop)(&s->identity);
    SDROP(created_by_field); SDROP(created_at_field); SDROP(updated_by_field); SDROP(updated_at_field);
    SDROP(authored_columns_field); SDROP(deletion_field);
    jz_btree_drop(&s->user_fields);
}
static bool str_eq_v(const void *a, const void *b) { return jz_str_eq(a, b); }
bool JE_SCH(VersionWitnessSchema__PartialEq__eq)(const jazz_engine_VersionWitnessSchema *a, const jazz_engine_VersionWitnessSchema *b) {
    if (a->source.table != b->source.table || a->source.path.len != b->source.path.len ||
        (a->source.path.len && memcmp(a->source.path.ptr, b->source.path.ptr, a->source.path.len) != 0))
        return false;
    return a->descriptor == b->descriptor && identity_eq(&a->identity, &b->identity) &&
           jz_str_eq(&a->created_by_field, &b->created_by_field) && jz_str_eq(&a->created_at_field, &b->created_at_field) &&
           jz_str_eq(&a->updated_by_field, &b->updated_by_field) && jz_str_eq(&a->updated_at_field, &b->updated_at_field) &&
           jz_str_eq(&a->authored_columns_field, &b->authored_columns_field) && jz_str_eq(&a->deletion_field, &b->deletion_field) &&
           jz_btree_eq(&a->user_fields, &b->user_fields, str_eq_v);
}
void JE_SCH(VersionWitnessSchemas__Clone__clone)(jazz_engine_VersionWitnessSchemas *dst, const jazz_engine_VersionWitnessSchemas *src) {
    memset(dst, 0, sizeof *dst);
    SCLONE(role_field);
    dst->has_content = src->has_content;
    if (src->has_content) JE_SCH(VersionWitnessSchema__Clone__clone)(&dst->content, &src->content);
    dst->has_deletion = src->has_deletion;
    if (src->has_deletion) JE_SCH(VersionWitnessSchema__Clone__clone)(&dst->deletion, &src->deletion);
    SETCLONE(routing_param_fields);
}
void JE_SCH(VersionWitnessSchemas__drop)(jazz_engine_VersionWitnessSchemas *s) {
    SDROP(role_field);
    if (s->has_content) JE_SCH(VersionWitnessSchema__drop)(&s->content);
    if (s->has_deletion) JE_SCH(VersionWitnessSchema__drop)(&s->deletion);
    jz_btree_drop(&s->routing_param_fields);
}

/* ---------------------------------------------------------------- the remaining fact schemas */
static void batch_clone(jazz_engine_BatchIdentityFields *dst, const jazz_engine_BatchIdentityFields *src) { SCLONE(batch_id_field); OCLONE(batch_node_field); }
static void batch_drop(jazz_engine_BatchIdentityFields *s) { SDROP(batch_id_field); ODROP(batch_node_field); }
static void result_id_schema_clone(jazz_engine_ResultIdSchema *dst, const jazz_engine_ResultIdSchema *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    switch (src->tag) {
    case JAZZ_ENGINE_RIS_REAL_ROW: JE_SCH(VersionedRowRefSchema__Clone__clone)(&dst->real_row, &src->real_row); break;
    case JAZZ_ENGINE_RIS_SYNTHETIC_ROW: JE_SCH(SyntheticResultMembershipSchema__Clone__clone)(&dst->synthetic_row, &src->synthetic_row); break;
    case JAZZ_ENGINE_RIS_PATH_TUPLE: SCLONE(path_field); SCLONE(row_field); SCLONE(revision_field); break;
    }
}
static void result_id_schema_drop(jazz_engine_ResultIdSchema *s) {
    switch (s->tag) {
    case JAZZ_ENGINE_RIS_REAL_ROW: JE_SCH(VersionedRowRefSchema__drop)(&s->real_row); break;
    case JAZZ_ENGINE_RIS_SYNTHETIC_ROW: JE_SCH(SyntheticResultMembershipSchema__drop)(&s->synthetic_row); break;
    case JAZZ_ENGINE_RIS_PATH_TUPLE: SDROP(path_field); SDROP(row_field); SDROP(revision_field); break;
    }
}
static void fact_output_clone_v(void *d, const void *s) { JE_SCH(ProgramFactOutput__Clone__clone)(d, s); }
static void fact_output_drop_v(void *o) { JE_SCH(ProgramFactOutput__drop)(o); }
static void rmv_clone(jazz_engine_ResultMembershipVersionSchema *d, const jazz_engine_ResultMembershipVersionSchema *s) { JE_SCH(ResultMembershipVersionSchema__Clone__clone)(d, s); }
static void publication_map_clone(jz_btree *dst, const jz_btree *src) { jz_btree_clone(dst, src, jazz_engine__str_clone_v, jazz_engine__crpf_clone_v); }

void JE_SCH(ProgramFactSchema__Clone__clone)(jazz_engine_ProgramFactSchema *d, const jazz_engine_ProgramFactSchema *s) {
    memset(d, 0, sizeof *d);
    d->tag = s->tag;
    switch (s->tag) {
    case JAZZ_ENGINE_PFS_AUTHORIZED_ROWS: {
        jazz_engine_AuthorizedRowsSchema *dst = &d->u.authorized_rows;
        const jazz_engine_AuthorizedRowsSchema *src = &s->u.authorized_rows;
        SCLONE(row_field); SETCLONE(routing_param_fields);
        break;
    }
    case JAZZ_ENGINE_PFS_RESULT_MEMBERSHIP: {
        jazz_engine_ResultMembershipSchema *dst = &d->u.result_membership;
        const jazz_engine_ResultMembershipSchema *src = &s->u.result_membership;
        SCLONE(table_field); SCLONE(row_field);
        jazz_engine__strvec_clone(&dst->occurrence_id_fields, &src->occurrence_id_fields);
        jz_btree_clone(&dst->occurrence_union_arm_fields, &src->occurrence_union_arm_fields, u32_clone_v, jazz_engine__str_clone_v);
        jz_vec_clone(&dst->payload_fields, &src->payload_fields, sizeof(jazz_engine_TypedOutputField), jazz_engine__tof_clone_v);
        publication_map_clone(&dst->payload_publication_fields, &src->payload_publication_fields);
        OCLONE(branch_or_prefix_field);
        rmv_clone(&dst->version, &src->version);
        OCLONE(settle_position_field);
        SETCLONE(routing_param_fields);
        break;
    }
    case JAZZ_ENGINE_PFS_AGGREGATE_RESULT: JE_SCH(AggregateResultSchema__Clone__clone)(&d->u.aggregate_result, &s->u.aggregate_result); break;
    case JAZZ_ENGINE_PFS_RELATION_EDGES: {
        jazz_engine_RelationEdgeSchema *dst = &d->u.relation_edges;
        const jazz_engine_RelationEdgeSchema *src = &s->u.relation_edges;
        JE_SCH(VersionedRowRefSchema__Clone__clone)(&dst->source, &src->source);
        SCLONE(path_field);
        JE_SCH(VersionedRowRefSchema__Clone__clone)(&dst->target, &src->target);
        SCLONE(kind_field);
        OCLONE(depth_field); OCLONE(edge_id_field); OCLONE(branch_field); OCLONE(role_field); OCLONE(order_field); OCLONE(hole_state_field);
        break;
    }
    case JAZZ_ENGINE_PFS_PATH_CORRELATION_COVERAGE: {
        jazz_engine_PathCorrelationCoverageSchema *dst = &d->u.path_correlation_coverage;
        const jazz_engine_PathCorrelationCoverageSchema *src = &s->u.path_correlation_coverage;
        JE_SCH(VersionedRowRefSchema__Clone__clone)(&dst->parent, &src->parent);
        SCLONE(path_field); SCLONE(correlation_field); OCLONE(expected_count_field); SCLONE(readable_count_field); SCLONE(coverage_state_field);
        break;
    }
    case JAZZ_ENGINE_PFS_PROGRAM_SOURCE_COVERAGE: {
        jazz_engine_ProgramSourceCoverageSchema *dst = &d->u.program_source_coverage;
        const jazz_engine_ProgramSourceCoverageSchema *src = &s->u.program_source_coverage;
        program_source_id_clone(&dst->source, &src->source);
        dst->complete = src->complete;
        SETCLONE(routing_param_fields);
        break;
    }
    case JAZZ_ENGINE_PFS_READ_FRONTIER_SETTLED: {
        jazz_engine_ReadFrontierSettledSchema *dst = &d->u.read_frontier_settled;
        const jazz_engine_ReadFrontierSettledSchema *src = &s->u.read_frontier_settled;
        SCLONE(tier_field); SCLONE(frontier_field); OCLONE(stream_field); OCLONE(through_field); SCLONE(scope_field); SETCLONE(routing_param_fields);
        break;
    }
    case JAZZ_ENGINE_PFS_COMPLETE_TX_PAYLOAD_COVERAGE: {
        jazz_engine_CompleteTxPayloadCoverageSchema *dst = &d->u.complete_tx_payload_coverage;
        const jazz_engine_CompleteTxPayloadCoverageSchema *src = &s->u.complete_tx_payload_coverage;
        batch_clone(&dst->batch, &src->batch);
        SCLONE(tier_field); SCLONE(payload_digest_field); SCLONE(fate_field);
        break;
    }
    case JAZZ_ENGINE_PFS_VIEW_COMPLETE_EXCLUSIVE_COVERAGE: {
        jazz_engine_ViewCompleteExclusiveCoverageSchema *dst = &d->u.view_complete_exclusive_coverage;
        const jazz_engine_ViewCompleteExclusiveCoverageSchema *src = &s->u.view_complete_exclusive_coverage;
        batch_clone(&dst->batch, &src->batch);
        SCLONE(view_scope_field);
        dst->has_result = src->has_result;
        if (src->has_result) result_id_schema_clone(&dst->result, &src->result);
        SCLONE(covered_members_digest_field); SCLONE(tier_field); SCLONE(fate_field);
        break;
    }
    case JAZZ_ENGINE_PFS_VERSION_WITNESSES:
    case JAZZ_ENGINE_PFS_REPLACEMENT_WITNESSES: JE_SCH(VersionWitnessSchemas__Clone__clone)(&d->u.witnesses, &s->u.witnesses); break;
    case JAZZ_ENGINE_PFS_POLICY_DECISION: {
        jazz_engine_PolicyDecisionSchema *dst = &d->u.policy_decision;
        const jazz_engine_PolicyDecisionSchema *src = &s->u.policy_decision;
        SCLONE(outcome_field); OCLONE(required_input_field); OCLONE(reason_field);
        jz_vec_clone(&dst->facts, &src->facts, sizeof(jazz_engine_ProgramFactOutput), fact_output_clone_v);
        break;
    }
    case JAZZ_ENGINE_PFS_POLICY_WITNESSES: {
        jazz_engine_PolicyWitnessSchema *dst = &d->u.policy_witnesses;
        const jazz_engine_PolicyWitnessSchema *src = &s->u.policy_witnesses;
        JE_SCH(VersionedRowRefSchema__Clone__clone)(&dst->protected_, &src->protected_);
        SCLONE(policy_path_field);
        JE_SCH(VersionedRowRefSchema__Clone__clone)(&dst->witness, &src->witness);
        SCLONE(edge_kind_field);
        break;
    }
    case JAZZ_ENGINE_PFS_CONTRIBUTING_MEMBERS: {
        jazz_engine_ContributingMembersSchema *dst = &d->u.contributing_members;
        const jazz_engine_ContributingMembersSchema *src = &s->u.contributing_members;
        result_id_schema_clone(&dst->result, &src->result);
        SCLONE(contributor_field);
        SCLONE(member.table_field); SCLONE(member.row_field); SCLONE(member.data_branch_field); SCLONE(member.row_digest_field);
        batch_clone(&dst->batch, &src->batch);
        SCLONE(contributing_digest_field);
        break;
    }
    case JAZZ_ENGINE_PFS_PREDICATE_READS: {
        jazz_engine_PredicateReadFactSchema *dst = &d->u.predicate_reads;
        const jazz_engine_PredicateReadFactSchema *src = &s->u.predicate_reads;
        SCLONE(table_field); SCLONE(shape_id_field); SCLONE(binding_id_field); SCLONE(canonical_shape_field);
        SETCLONE(binding_value_fields);
        SCLONE(base_snapshot_field); SCLONE(resolved_read_key_field); SCLONE(policy_key_field); SCLONE(source_visibility_field);
        break;
    }
    case JAZZ_ENGINE_PFS_PREDICATE_OUTPUT_SET: {
        jazz_engine_PredicateOutputSetSchema *dst = &d->u.predicate_output_set;
        const jazz_engine_PredicateOutputSetSchema *src = &s->u.predicate_output_set;
        dst->role = src->role;
        SCLONE(table_field); SCLONE(row_field);
        rmv_clone(&dst->version, &src->version);
        SCLONE(shape_id_field); SCLONE(binding_id_field);
        break;
    }
    case JAZZ_ENGINE_PFS_POINT_READS: {
        jazz_engine_PointReadFactSchema *dst = &d->u.point_reads;
        const jazz_engine_PointReadFactSchema *src = &s->u.point_reads;
        SCLONE(table_field); SCLONE(row_field); SCLONE(presence_field); OCLONE(observed_version_field); OCLONE(base_snapshot_field);
        break;
    }
    }
}

void JE_SCH(ProgramFactSchema__drop)(jazz_engine_ProgramFactSchema *d) {
    switch (d->tag) {
    case JAZZ_ENGINE_PFS_AUTHORIZED_ROWS: {
        jazz_engine_AuthorizedRowsSchema *s = &d->u.authorized_rows;
        SDROP(row_field); jz_btree_drop(&s->routing_param_fields);
        break;
    }
    case JAZZ_ENGINE_PFS_RESULT_MEMBERSHIP: {
        jazz_engine_ResultMembershipSchema *s = &d->u.result_membership;
        SDROP(table_field); SDROP(row_field);
        jazz_engine__strvec_drop(&s->occurrence_id_fields);
        jz_btree_drop(&s->occurrence_union_arm_fields);
        jz_vec_drop(&s->payload_fields, sizeof(jazz_engine_TypedOutputField), jazz_engine__tof_drop_v);
        jz_btree_drop(&s->payload_publication_fields);
        ODROP(branch_or_prefix_field);
        JE_SCH(ResultMembershipVersionSchema__drop)(&s->version);
        ODROP(settle_position_field);
        jz_btree_drop(&s->routing_param_fields);
        break;
    }
    case JAZZ_ENGINE_PFS_AGGREGATE_RESULT: JE_SCH(AggregateResultSchema__drop)(&d->u.aggregate_result); break;
    case JAZZ_ENGINE_PFS_RELATION_EDGES: {
        jazz_engine_RelationEdgeSchema *s = &d->u.relation_edges;
        JE_SCH(VersionedRowRefSchema__drop)(&s->source);
        SDROP(path_field);
        JE_SCH(VersionedRowRefSchema__drop)(&s->target);
        SDROP(kind_field);
        ODROP(depth_field); ODROP(edge_id_field); ODROP(branch_field); ODROP(role_field); ODROP(order_field); ODROP(hole_state_field);
        break;
    }
    case JAZZ_ENGINE_PFS_PATH_CORRELATION_COVERAGE: {
        jazz_engine_PathCorrelationCoverageSchema *s = &d->u.path_correlation_coverage;
        JE_SCH(VersionedRowRefSchema__drop)(&s->parent);
        SDROP(path_field); SDROP(correlation_field); ODROP(expected_count_field); SDROP(readable_count_field); SDROP(coverage_state_field);
        break;
    }
    case JAZZ_ENGINE_PFS_PROGRAM_SOURCE_COVERAGE: {
        jazz_engine_ProgramSourceCoverageSchema *s = &d->u.program_source_coverage;
        jz_vec_drop(&s->source.path, 1, NULL);
        jz_btree_drop(&s->routing_param_fields);
        break;
    }
    case JAZZ_ENGINE_PFS_READ_FRONTIER_SETTLED: {
        jazz_engine_ReadFrontierSettledSchema *s = &d->u.read_frontier_settled;
        SDROP(tier_field); SDROP(frontier_field); ODROP(stream_field); ODROP(through_field); SDROP(scope_field); jz_btree_drop(&s->routing_param_fields);
        break;
    }
    case JAZZ_ENGINE_PFS_COMPLETE_TX_PAYLOAD_COVERAGE: {
        jazz_engine_CompleteTxPayloadCoverageSchema *s = &d->u.complete_tx_payload_coverage;
        batch_drop(&s->batch);
        SDROP(tier_field); SDROP(payload_digest_field); SDROP(fate_field);
        break;
    }
    case JAZZ_ENGINE_PFS_VIEW_COMPLETE_EXCLUSIVE_COVERAGE: {
        jazz_engine_ViewCompleteExclusiveCoverageSchema *s = &d->u.view_complete_exclusive_coverage;
        batch_drop(&s->batch);
        SDROP(view_scope_field);
        if (s->has_result) result_id_schema_drop(&s->result);
        SDROP(covered_members_digest_field); SDROP(tier_field); SDROP(fate_field);
        break;
    }
    case JAZZ_ENGINE_PFS_VERSION_WITNESSES:
    case JAZZ_ENGINE_PFS_REPLACEMENT_WITNESSES: JE_SCH(VersionWitnessSchemas__drop)(&d->u.witnesses); break;
    case JAZZ_ENGINE_PFS_POLICY_DECISION: {
        jazz_engine_PolicyDecisionSchema *s = &d->u.policy_decision;
        SDROP(outcome_field); ODROP(required_input_field); ODROP(reason_field);
        jz_vec_drop(&s->facts, sizeof(jazz_engine_ProgramFactOutput), fact_output_drop_v);
        break;
    }
    case JAZZ_ENGINE_PFS_POLICY_WITNESSES: {
        jazz_engine_PolicyWitnessSchema *s = &d->u.policy_witnesses;
        JE_SCH(VersionedRowRefSchema__drop)(&s->protected_);
        SDROP(policy_path_field);
        JE_SCH(VersionedRowRefSchema__drop)(&s->witness);
        SDROP(edge_kind_field);
        break;
    }
    case JAZZ_ENGINE_PFS_CONTRIBUTING_MEMBERS: {
        jazz_engine_ContributingMembersSchema *s = &d->u.contributing_members;
        result_id_schema_drop(&s->result);
        SDROP(contributor_field);
        SDROP(member.table_field); SDROP(member.row_field); SDROP(member.data_branch_field); SDROP(member.row_digest_field);
        batch_drop(&s->batch);
        SDROP(contributing_digest_field);
        break;
    }
    case JAZZ_ENGINE_PFS_PREDICATE_READS: {
        jazz_engine_PredicateReadFactSchema *s = &d->u.predicate_reads;
        SDROP(table_field); SDROP(shape_id_field); SDROP(binding_id_field); SDROP(canonical_shape_field);
        jz_btree_drop(&s->binding_value_fields);
        SDROP(base_snapshot_field); SDROP(resolved_read_key_field); SDROP(policy_key_field); SDROP(source_visibility_field);
        break;
    }
    case JAZZ_ENGINE_PFS_PREDICATE_OUTPUT_SET: {
        jazz_engine_PredicateOutputSetSchema *s = &d->u.predicate_output_set;
        SDROP(table_field); SDROP(row_field);
        JE_SCH(ResultMembershipVersionSchema__drop)(&s->version);
        SDROP(shape_id_field); SDROP(binding_id_field);
        break;
    }
    case JAZZ_ENGINE_PFS_POINT_READS: {
        jazz_engine_PointReadFactSchema *s = &d->u.point_reads;
        SDROP(table_field); SDROP(row_field); SDROP(presence_field); ODROP(observed_version_field); ODROP(base_snapshot_field);
        break;
    }
    }
}

/* ---------------------------------------------------------------- outputs */
void JE_SCH(ProgramFactOutput__Clone__clone)(jazz_engine_ProgramFactOutput *dst, const jazz_engine_ProgramFactOutput *src) {
    JE_OUT(ProgramFactKey__Clone__clone)(&dst->key, &src->key);
    dst->terminal = src->terminal;
    JE_SCH(ProgramFactSchema__Clone__clone)(&dst->schema, &src->schema);
}
void JE_SCH(ProgramFactOutput__drop)(jazz_engine_ProgramFactOutput *o) {
    JE_OUT(ProgramFactKey__drop)(&o->key);
    JE_SCH(ProgramFactSchema__drop)(&o->schema);
}
void JE_SCH(ProgramFactOutput__key)(const jazz_engine_ProgramFactOutput *o, jazz_engine_ProgramFactKey *out) { JE_OUT(ProgramFactKey__Clone__clone)(out, &o->key); }
void JE_SCH(OutputTerminalSchema__Clone__clone)(jazz_engine_OutputTerminalSchema *dst, const jazz_engine_OutputTerminalSchema *src) {
    dst->tag = src->tag;
    if (src->tag == JAZZ_ENGINE_OTS_APP_ROWS) JE_SCH(AppRowSchema__Clone__clone)(&dst->u.app_rows, &src->u.app_rows);
    else JE_SCH(ProgramFactOutput__Clone__clone)(&dst->u.fact, &src->u.fact);
}
void JE_SCH(OutputTerminalSchema__drop)(jazz_engine_OutputTerminalSchema *s) {
    if (s->tag == JAZZ_ENGINE_OTS_APP_ROWS) JE_SCH(AppRowSchema__drop)(&s->u.app_rows);
    else JE_SCH(ProgramFactOutput__drop)(&s->u.fact);
}
void jazz_engine__ots_drop_v(void *s) { JE_SCH(OutputTerminalSchema__drop)(s); }
void jazz_engine__ots_clone_v(void *d, const void *s) { JE_SCH(OutputTerminalSchema__Clone__clone)(d, s); }
void JE_SCH(ProgramOutputSchemas__drop)(jazz_engine_ProgramOutputSchemas *s) { jz_vec_drop(&s->row_set, sizeof(jazz_engine_OutputTerminalSchema), jazz_engine__ots_drop_v); }
