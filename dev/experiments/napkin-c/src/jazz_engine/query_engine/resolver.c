/* resolver.rs: source requests, resolved sources, capability reports. */
#include "mod.h"
#include <stdarg.h>
#include <stdio.h>

/* format!() with a va_list: jz_rt.h only exposes the variadic jz_format. */
static jz_str vformat(const char *fmt, va_list ap) {
    va_list ap2;
    va_copy(ap2, ap);
    int n = vsnprintf(NULL, 0, fmt, ap);
    if (n < 0) jz_panic("a formatting trait implementation returned an error");
    char *buf = jz_alloc((usize)n + 1);
    vsnprintf(buf, (usize)n + 1, fmt, ap2);
    va_end(ap2);
    jz_str s = {buf, (u32)n, (u32)n + 1};
    return s;
}

/* ---------------------------------------------------------------- PolicyAuthorizationPlan / SourceAuthorizationRequest */
void JE_RES(PolicyAuthorizationPlan__Clone__clone)(jazz_engine_PolicyAuthorizationPlan *dst, const jazz_engine_PolicyAuthorizationPlan *src) {
    sid_clone(&dst->protected_source, &src->protected_source);
    dst->role = src->role;
    dst->protected_row_field = jz_str_clone(&src->protected_row_field);
    dst->has_binding_source_shape = src->has_binding_source_shape;
    dst->binding_source_shape = src->has_binding_source_shape ? jz_str_clone(&src->binding_source_shape) : (jz_str){0};
    jazz_engine__strvtmap_clone(&dst->binding_user_params, &src->binding_user_params);
    jz_btree_clone(&dst->binding_claim_params, &src->binding_claim_params, jazz_engine__str_clone_v, jazz_engine__claim_param_clone_v);
}
void JE_RES(PolicyAuthorizationPlan__drop)(jazz_engine_PolicyAuthorizationPlan *p) {
    sid_drop(&p->protected_source);
    jz_str_drop(&p->protected_row_field);
    jz_str_drop(&p->binding_source_shape);
    jz_btree_drop(&p->binding_user_params);
    jz_btree_drop(&p->binding_claim_params);
}
void JE_RES(SourceAuthorizationRequest__Clone__clone)(jazz_engine_SourceAuthorizationRequest *dst, const jazz_engine_SourceAuthorizationRequest *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    if (src->tag == JAZZ_ENGINE_SAR_SYSTEM) return;
    dst->permission_subject = src->permission_subject;
    JE_RES(PolicyAuthorizationPlan__Clone__clone)(&dst->plan, &src->plan);
}
void JE_RES(SourceAuthorizationRequest__drop)(jazz_engine_SourceAuthorizationRequest *a) {
    if (a->tag != JAZZ_ENGINE_SAR_SYSTEM) JE_RES(PolicyAuthorizationPlan__drop)(&a->plan);
}
static void claim_param_debug_v(jz_str *out, const void *p) {
    const jazz_engine_ProgramClaimParam *c = p;
    jazz_engine__fmt__struct_begin(out, "ProgramClaimParam");
    jazz_engine__fmt__field(out, "path", true);
    JE_IN(ClaimPath__Debug__fmt)(out, &c->path);
    jazz_engine__fmt__field(out, "ty", false);
    groove__records__values__ValueType__Debug__fmt(out, &c->ty);
    jazz_engine__fmt__struct_end(out);
}
void JE_RES(SourceAuthorizationRequest__Debug__fmt)(jz_str *out, const jazz_engine_SourceAuthorizationRequest *a) {
    if (a->tag == JAZZ_ENGINE_SAR_SYSTEM) {
        jz_str_push_cstr(out, "System");
        return;
    }
    const jazz_engine_PolicyAuthorizationPlan *p = &a->plan;
    jazz_engine__fmt__struct_begin(out, a->tag == JAZZ_ENGINE_SAR_POLICY_FILTERED ? "PolicyFiltered" : "PolicyProof");
    jazz_engine__fmt__field(out, "permission_subject", true);
    jazz_types__ids__AuthorSubject__Debug__fmt(out, &a->permission_subject);
    jazz_engine__fmt__field(out, "plan", false);
    jazz_engine__fmt__struct_begin(out, "PolicyAuthorizationPlan");
    jazz_engine__fmt__field(out, "protected_source", true);
    sid_debug(out, &p->protected_source);
    jazz_engine__fmt__field(out, "role", false);
    JE_OUT(PolicyDecisionRole__Debug__fmt)(out, p->role);
    jazz_engine__fmt__field(out, "protected_row_field", false);
    jazz_engine__fmt__str(out, &p->protected_row_field);
    jazz_engine__fmt__field(out, "binding_source_shape", false);
    if (p->has_binding_source_shape) {
        jazz_engine__fmt__tuple_begin(out, "Some");
        jazz_engine__fmt__str(out, &p->binding_source_shape);
        jazz_engine__fmt__tuple_end(out);
    } else {
        jazz_engine__fmt__option_none(out);
    }
    jazz_engine__fmt__field(out, "binding_user_params", false);
    jazz_engine__fmt__map(out, &p->binding_user_params, jazz_engine__fmt__str_v, jazz_engine__fmt__vt_v);
    jazz_engine__fmt__field(out, "binding_claim_params", false);
    jazz_engine__fmt__map(out, &p->binding_claim_params, jazz_engine__fmt__str_v, claim_param_debug_v);
    jazz_engine__fmt__struct_end(out);
    jazz_engine__fmt__struct_end(out);
}

/* ---------------------------------------------------------------- requirements */
jazz_engine_SourceMetadataRequirement jazz_engine__smr_simple(u8 tag) {
    jazz_engine_SourceMetadataRequirement r;
    memset(&r, 0, sizeof r);
    r.tag = tag;
    return r;
}
void JE_RES(SourceMetadataRequirement__Clone__clone)(jazz_engine_SourceMetadataRequirement *dst, const jazz_engine_SourceMetadataRequirement *src) {
    *dst = *src;
    dst->author_path = src->tag == JAZZ_ENGINE_SMR_AUTHOR_PATH ? jz_str_clone(&src->author_path) : (jz_str){0};
}
void JE_RES(SourceMetadataRequirement__drop)(jazz_engine_SourceMetadataRequirement *r) { jz_str_drop(&r->author_path); }
int JE_RES(SourceMetadataRequirement__Ord__cmp)(const jazz_engine_SourceMetadataRequirement *a, const jazz_engine_SourceMetadataRequirement *b) {
    if (a->tag != b->tag) return a->tag < b->tag ? -1 : 1;
    if (a->tag == JAZZ_ENGINE_SMR_PROVENANCE) return (int)a->provenance - (int)b->provenance;
    if (a->tag == JAZZ_ENGINE_SMR_AUTHOR_PATH) return jz_str_cmp(&a->author_path, &b->author_path);
    return 0;
}
void JE_RES(SourceMetadataRequirement__Debug__fmt)(jz_str *out, const jazz_engine_SourceMetadataRequirement *r) {
    static const char *const names[] = {"VersionWitnesses", "SettlePosition", "VersionPayloads", "DeletionMarkers", "BatchMembership",
                                        "Coverage", "ValidationReads", "PolicyWitnesses", "Provenance", "AuthorPath"};
    static const char *const prov[] = {"CreatedAt", "CreatedBy", "UpdatedAt", "UpdatedBy"};
    if (r->tag == JAZZ_ENGINE_SMR_PROVENANCE) {
        jz_str_appendf(out, "Provenance(%s)", prov[r->provenance]);
    } else if (r->tag == JAZZ_ENGINE_SMR_AUTHOR_PATH) {
        jazz_engine__fmt__tuple_begin(out, "AuthorPath");
        jazz_engine__fmt__str(out, &r->author_path);
        jazz_engine__fmt__tuple_end(out);
    } else {
        jz_str_push_cstr(out, names[r->tag]);
    }
}
void jazz_engine__smr_drop_v(void *r) { JE_RES(SourceMetadataRequirement__drop)(r); }
void jazz_engine__smr_clone_v(void *d, const void *s) { JE_RES(SourceMetadataRequirement__Clone__clone)(d, s); }
static int smr_cmp_v(const void *a, const void *b, void *ctx) { return JE_RES(SourceMetadataRequirement__Ord__cmp)(a, b); }
static void smr_debug_v(jz_str *out, const void *r) { JE_RES(SourceMetadataRequirement__Debug__fmt)(out, r); }
const jz_btree_desc jazz_engine__SMR_SET_DESC = {sizeof(jazz_engine_SourceMetadataRequirement), 0, smr_cmp_v, jazz_engine__smr_drop_v, NULL};

void JE_RES(FieldRequirement__Clone__clone)(jazz_engine_FieldRequirement *dst, const jazz_engine_FieldRequirement *src) {
    dst->tag = src->tag;
    if (src->tag == JAZZ_ENGINE_FREQ_FIELDS) jazz_engine__strset_clone(&dst->fields, &src->fields);
    else jz_btree_init(&dst->fields, &jazz_engine__STRSET_DESC);
}
void JE_RES(FieldRequirement__drop)(jazz_engine_FieldRequirement *f) { jz_btree_drop(&f->fields); }
bool JE_RES(FieldRequirement__PartialEq__eq)(const jazz_engine_FieldRequirement *a, const jazz_engine_FieldRequirement *b) {
    return a->tag == b->tag && (a->tag != JAZZ_ENGINE_FREQ_FIELDS || jz_btree_eq(&a->fields, &b->fields, NULL));
}
void JE_RES(FieldRequirement__Debug__fmt)(jz_str *out, const jazz_engine_FieldRequirement *f) {
    switch (f->tag) {
    case JAZZ_ENGINE_FREQ_NONE_: jz_str_push_cstr(out, "None"); break;
    case JAZZ_ENGINE_FREQ_ALL: jz_str_push_cstr(out, "All"); break;
    default:
        jazz_engine__fmt__tuple_begin(out, "Fields");
        jazz_engine__fmt__strset(out, &f->fields);
        jazz_engine__fmt__tuple_end(out);
    }
}
void JE_RES(SourceRequirements__Default__default)(jazz_engine_SourceRequirements *r) {
    r->app_fields.tag = JAZZ_ENGINE_FREQ_NONE_;
    jz_btree_init(&r->app_fields.fields, &jazz_engine__STRSET_DESC);
    jz_btree_init(&r->metadata, &jazz_engine__SMR_SET_DESC);
}
void JE_RES(SourceRequirements__Clone__clone)(jazz_engine_SourceRequirements *dst, const jazz_engine_SourceRequirements *src) {
    JE_RES(FieldRequirement__Clone__clone)(&dst->app_fields, &src->app_fields);
    jz_btree_clone(&dst->metadata, &src->metadata, jazz_engine__smr_clone_v, NULL);
}
void JE_RES(SourceRequirements__drop)(jazz_engine_SourceRequirements *r) {
    JE_RES(FieldRequirement__drop)(&r->app_fields);
    jz_btree_drop(&r->metadata);
}
static bool smr_eq_unused(const void *a, const void *b) { return true; }
bool JE_RES(SourceRequirements__PartialEq__eq)(const jazz_engine_SourceRequirements *a, const jazz_engine_SourceRequirements *b) {
    return JE_RES(FieldRequirement__PartialEq__eq)(&a->app_fields, &b->app_fields) && jz_btree_eq(&a->metadata, &b->metadata, smr_eq_unused);
}
void JE_RES(SourceRequirements__Debug__fmt)(jz_str *out, const jazz_engine_SourceRequirements *r) {
    jazz_engine__fmt__struct_begin(out, "SourceRequirements");
    jazz_engine__fmt__field(out, "app_fields", true);
    JE_RES(FieldRequirement__Debug__fmt)(out, &r->app_fields);
    jazz_engine__fmt__field(out, "metadata", false);
    jazz_engine__fmt__set(out, &r->metadata, smr_debug_v);
    jazz_engine__fmt__struct_end(out);
}
void jazz_engine__reqs_drop_v(void *r) { JE_RES(SourceRequirements__drop)(r); }
void jazz_engine__reqs_clone_v(void *d, const void *s) { JE_RES(SourceRequirements__Clone__clone)(d, s); }
const jz_btree_desc jazz_engine__SOURCE_ID_REQ_MAP_DESC = {sizeof(jazz_engine_SourceId), sizeof(jazz_engine_SourceRequirements), jazz_engine__sid_cmp_v, jazz_engine__sid_drop_v, jazz_engine__reqs_drop_v};

void JE_RES(SourceRequest__Clone__clone)(jazz_engine_SourceRequest *dst, const jazz_engine_SourceRequest *src) {
    sid_clone(&dst->source, &src->source);
    dst->visibility = src->visibility;
    JE_RES(SourceAuthorizationRequest__Clone__clone)(&dst->authorization, &src->authorization);
    JE_RES(SourceRequirements__Clone__clone)(&dst->requirements, &src->requirements);
}
void JE_RES(SourceRequest__drop)(jazz_engine_SourceRequest *r) {
    sid_drop(&r->source);
    JE_RES(SourceAuthorizationRequest__drop)(&r->authorization);
    JE_RES(SourceRequirements__drop)(&r->requirements);
}
void JE_RES(SourceRequest__Debug__fmt)(jz_str *out, const jazz_engine_SourceRequest *r) {
    jazz_engine__fmt__struct_begin(out, "SourceRequest");
    jazz_engine__fmt__field(out, "source", true);
    sid_debug(out, &r->source);
    jazz_engine__fmt__field(out, "visibility", false);
    jz_str_push_cstr(out, r->visibility == JAZZ_ENGINE_VIS_VISIBLE ? "Visible" : "IncludeDeleted");
    jazz_engine__fmt__field(out, "authorization", false);
    JE_RES(SourceAuthorizationRequest__Debug__fmt)(out, &r->authorization);
    jazz_engine__fmt__field(out, "requirements", false);
    JE_RES(SourceRequirements__Debug__fmt)(out, &r->requirements);
    jazz_engine__fmt__struct_end(out);
}
void jazz_engine__source_request_drop_v(void *r) { JE_RES(SourceRequest__drop)(r); }
void jazz_engine__source_request_clone_v(void *d, const void *s) { JE_RES(SourceRequest__Clone__clone)(d, s); }

/* ---------------------------------------------------------------- resolved sources */
void JE_RES(SourceMetadataFields__Clone__clone)(jazz_engine_SourceMetadataFields *dst, const jazz_engine_SourceMetadataFields *src) {
    dst->tag = src->tag;
    dst->a = jz_str_clone(&src->a);
    dst->b = jz_str_clone(&src->b);
    dst->c = jz_str_clone(&src->c);
    dst->has_opt0 = src->has_opt0;
    dst->opt0 = src->has_opt0 ? jz_str_clone(&src->opt0) : (jz_str){0};
    dst->has_opt1 = src->has_opt1;
    dst->opt1 = src->has_opt1 ? jz_str_clone(&src->opt1) : (jz_str){0};
}
void JE_RES(SourceMetadataFields__drop)(jazz_engine_SourceMetadataFields *f) {
    jz_str_drop(&f->a);
    jz_str_drop(&f->b);
    jz_str_drop(&f->c);
    jz_str_drop(&f->opt0);
    jz_str_drop(&f->opt1);
}
static void smf_drop_v(void *f) { JE_RES(SourceMetadataFields__drop)(f); }
static void smf_clone_v(void *d, const void *s) { JE_RES(SourceMetadataFields__Clone__clone)(d, s); }
const jz_btree_desc jazz_engine__SMR_FIELDS_MAP_DESC = {sizeof(jazz_engine_SourceMetadataRequirement), sizeof(jazz_engine_SourceMetadataFields), smr_cmp_v, jazz_engine__smr_drop_v, smf_drop_v};

void JE_RES(SourceRowShape__Clone__clone)(jazz_engine_SourceRowShape *dst, const jazz_engine_SourceRowShape *src) {
    sid_clone(&dst->source, &src->source);
    dst->descriptor = src->descriptor;
    dst->row_uuid_field = jz_str_clone(&src->row_uuid_field);
    jz_btree_clone(&dst->metadata, &src->metadata, jazz_engine__smr_clone_v, smf_clone_v);
}
void JE_RES(SourceRowShape__drop)(jazz_engine_SourceRowShape *s) {
    sid_drop(&s->source);
    jz_str_drop(&s->row_uuid_field);
    jz_btree_drop(&s->metadata);
}
static void pcid_clone_v(void *d, const void *s) { *(jazz_types_PhysicalColumnId *)d = *(const jazz_types_PhysicalColumnId *)s; }
void JE_RES(ResolvedSource__Clone__clone)(jazz_engine_ResolvedSource *dst, const jazz_engine_ResolvedSource *src) {
    jz_btree_clone(&dst->stored_column_ids, &src->stored_column_ids, jazz_engine__str_clone_v, pcid_clone_v);
    dst->table_schema = jz_rc_clone(src->table_schema);
    dst->graph = groove_gb_clone(src->graph);
    JE_RES(SourceRowShape__Clone__clone)(&dst->row_shape, &src->row_shape);
    jazz_engine__strset_clone(&dst->routing_fields, &src->routing_fields);
    dst->requires_result_payload = src->requires_result_payload;
    dst->has_content_version = src->has_content_version;
    memset(&dst->content_version, 0, sizeof dst->content_version);
    if (src->has_content_version) {
        dst->content_version.graph = groove_gb_clone(src->content_version.graph);
        dst->content_version.row_uuid_field = jz_str_clone(&src->content_version.row_uuid_field);
    }
    dst->has_deletion_register = src->has_deletion_register;
    memset(&dst->deletion_register, 0, sizeof dst->deletion_register);
    if (src->has_deletion_register) {
        dst->deletion_register.graph = groove_gb_clone(src->deletion_register.graph);
        dst->deletion_register.row_uuid_field = jz_str_clone(&src->deletion_register.row_uuid_field);
    }
    dst->has_authorized_deletion_preimage = src->has_authorized_deletion_preimage;
    memset(&dst->authorized_deletion_preimage, 0, sizeof dst->authorized_deletion_preimage);
    if (src->has_authorized_deletion_preimage) {
        dst->authorized_deletion_preimage.graph = groove_gb_clone(src->authorized_deletion_preimage.graph);
        jazz_engine__strset_clone(&dst->authorized_deletion_preimage.routing_fields, &src->authorized_deletion_preimage.routing_fields);
    }
}
void JE_RES(ResolvedSource__drop)(jazz_engine_ResolvedSource *s) {
    jz_btree_drop(&s->stored_column_ids);
    jz_rc_release(s->table_schema);
    groove_gb_drop(s->graph);
    JE_RES(SourceRowShape__drop)(&s->row_shape);
    jz_btree_drop(&s->routing_fields);
    if (s->has_content_version) {
        groove_gb_drop(s->content_version.graph);
        jz_str_drop(&s->content_version.row_uuid_field);
    }
    if (s->has_deletion_register) {
        groove_gb_drop(s->deletion_register.graph);
        jz_str_drop(&s->deletion_register.row_uuid_field);
    }
    if (s->has_authorized_deletion_preimage) {
        groove_gb_drop(s->authorized_deletion_preimage.graph);
        jz_btree_drop(&s->authorized_deletion_preimage.routing_fields);
    }
}
void jazz_engine__resolved_drop_v(void *s) { JE_RES(ResolvedSource__drop)(s); }
void jazz_engine__resolved_clone_v(void *d, const void *s) { JE_RES(ResolvedSource__Clone__clone)(d, s); }
const jz_btree_desc jazz_engine__SOURCE_ID_RESOLVED_MAP_DESC = {sizeof(jazz_engine_SourceId), sizeof(jazz_engine_ResolvedSource), jazz_engine__sid_cmp_v, jazz_engine__sid_drop_v, jazz_engine__resolved_drop_v};

jazz_engine_CapErr JE_RES(ResolvedSource__lookup)(const jz_btree *sources, const jazz_engine_SourceId *id, const char *role, const jazz_engine_ResolvedSource **out) {
    const jazz_engine_ResolvedSource *source = jz_btree_get(sources, id);
    if (!source) {
        jz_str msg = jz_format("%s source ", role);
        sid_debug(&msg, id);
        jz_str_push_cstr(&msg, " was not resolved");
        return JE_RES(CapabilityReport__runtime)(msg);
    }
    *out = source;
    return NULL;
}

/* ---------------------------------------------------------------- gaps and reports */
void JE_RES(SourceGap__Clone__clone)(jazz_engine_SourceGap *dst, const jazz_engine_SourceGap *src) {
    *dst = *src;
    dst->table = src->tag == JAZZ_ENGINE_GAP_POLICY_PROOF_CYCLE ? jz_str_clone(&src->table) : (jz_str){0};
}
void JE_RES(SourceGap__Debug__fmt)(jz_str *out, const jazz_engine_SourceGap *g) {
    static const char *const names[] = {"LocalAvailabilityInput", "PolicyProofCycle", "HistoricalStorageCut", "SnapshotRef",
                                        "SchemaProjection", "BranchOverlay", "TransactionReadOverlay", "Coverage"};
    if (g->tag != JAZZ_ENGINE_GAP_POLICY_PROOF_CYCLE) {
        jz_str_push_cstr(out, names[g->tag]);
        return;
    }
    jazz_engine__fmt__struct_begin(out, "PolicyProofCycle");
    jazz_engine__fmt__field(out, "table", true);
    jazz_engine__fmt__str(out, &g->table);
    jazz_engine__fmt__field(out, "depth", false);
    jz_str_appendf(out, "%u", g->depth);
    jazz_engine__fmt__struct_end(out);
}
void JE_RES(SourceResolutionError__drop)(jazz_engine_SourceResolutionError *e) {
    if (e->request) {
        JE_RES(SourceRequest__drop)(e->request);
        jz_free(e->request);
    }
    jz_str_drop(&e->gap.table);
}
/* {:#?}: the alternate (pretty) flag only changes whitespace; the C Debug helpers
 * write the compact form for both. */
void JE_RES(SourceResolutionError__Debug__fmt_alternate)(jz_str *out, const jazz_engine_SourceResolutionError *e) {
    jazz_engine__fmt__struct_begin(out, "SourceResolutionError");
    jazz_engine__fmt__field(out, "request", true);
    JE_RES(SourceRequest__Debug__fmt)(out, e->request);
    jazz_engine__fmt__field(out, "gap", false);
    JE_RES(SourceGap__Debug__fmt)(out, &e->gap);
    jazz_engine__fmt__struct_end(out);
}

void JE_RES(UnsupportedReason__Clone__clone)(jazz_engine_UnsupportedReason *dst, const jazz_engine_UnsupportedReason *src) {
    *dst = jazz_engine__ur_new(src->tag);
    switch (src->tag) {
    case JAZZ_ENGINE_UR_SOURCE: JE_RES(SourceGap__Clone__clone)(&dst->source, &src->source); break;
    case JAZZ_ENGINE_UR_UNBOUND_CLAIM: JE_IN(ClaimPath__Clone__clone)(&dst->claim, &src->claim); break;
    case JAZZ_ENGINE_UR_OPERATOR:
    case JAZZ_ENGINE_UR_RUNTIME: dst->message = jz_str_clone(&src->message); break;
    case JAZZ_ENGINE_UR_OUTPUT:
        dst->output = JZ_NEW(jazz_engine_ProgramFactKey);
        JE_OUT(ProgramFactKey__Clone__clone)(dst->output, src->output);
        break;
    }
}
void JE_RES(UnsupportedReason__drop)(jazz_engine_UnsupportedReason *r) {
    switch (r->tag) {
    case JAZZ_ENGINE_UR_SOURCE: jz_str_drop(&r->source.table); break;
    case JAZZ_ENGINE_UR_UNBOUND_CLAIM: JE_IN(ClaimPath__drop)(&r->claim); break;
    case JAZZ_ENGINE_UR_OPERATOR:
    case JAZZ_ENGINE_UR_RUNTIME: jz_str_drop(&r->message); break;
    case JAZZ_ENGINE_UR_OUTPUT:
        JE_OUT(ProgramFactKey__drop)(r->output);
        jz_free(r->output);
        break;
    }
}
void JE_RES(UnsupportedReason__Debug__fmt)(jz_str *out, const jazz_engine_UnsupportedReason *r) {
    switch (r->tag) {
    case JAZZ_ENGINE_UR_SOURCE: jazz_engine__fmt__tuple_begin(out, "Source"); JE_RES(SourceGap__Debug__fmt)(out, &r->source); break;
    case JAZZ_ENGINE_UR_UNBOUND_CLAIM: jazz_engine__fmt__tuple_begin(out, "UnboundClaim"); JE_IN(ClaimPath__Debug__fmt)(out, &r->claim); break;
    case JAZZ_ENGINE_UR_OPERATOR: jazz_engine__fmt__tuple_begin(out, "Operator"); jazz_engine__fmt__str(out, &r->message); break;
    case JAZZ_ENGINE_UR_OUTPUT: jazz_engine__fmt__tuple_begin(out, "Output"); JE_OUT(ProgramFactKey__Debug__fmt)(out, r->output); break;
    case JAZZ_ENGINE_UR_RUNTIME: jazz_engine__fmt__tuple_begin(out, "Runtime"); jazz_engine__fmt__str(out, &r->message); break;
    }
    jazz_engine__fmt__tuple_end(out);
}
static void ur_drop_v(void *r) { JE_RES(UnsupportedReason__drop)(r); }

void JE_RES(ExplainPlan__Clone__clone)(jazz_engine_ExplainPlan *dst, const jazz_engine_ExplainPlan *src) {
    dst->input = jz_str_clone(&src->input);
    jazz_engine__strvec_clone(&dst->read, &src->read);
    jazz_engine__strvec_clone(&dst->policy, &src->policy);
    jazz_engine__strvec_clone(&dst->output, &src->output);
    jazz_engine__strvec_clone(&dst->capabilities, &src->capabilities);
    jazz_engine__strvec_clone(&dst->physical, &src->physical);
}
void JE_RES(ExplainPlan__drop)(jazz_engine_ExplainPlan *p) {
    jz_str_drop(&p->input);
    jazz_engine__strvec_drop(&p->read);
    jazz_engine__strvec_drop(&p->policy);
    jazz_engine__strvec_drop(&p->output);
    jazz_engine__strvec_drop(&p->capabilities);
    jazz_engine__strvec_drop(&p->physical);
}
void JE_RES(CapabilityReport__drop)(jazz_engine_CapabilityReport *r) {
    jz_vec_drop(&r->gaps, sizeof(jazz_engine_UnsupportedReason), ur_drop_v);
    JE_RES(ExplainPlan__drop)(&r->explain);
}
void JE_RES(CapabilityReport__free)(jazz_engine_CapabilityReport *r) {
    if (!r) return;
    JE_RES(CapabilityReport__drop)(r);
    jz_free(r);
}
jazz_engine_CapErr jazz_engine__cap_single(jazz_engine_UnsupportedReason gap) {
    jazz_engine_CapabilityReport *r = JZ_NEW(jazz_engine_CapabilityReport);
    JZ_VEC_PUSH(r->gaps, gap);
    return r;
}
jazz_engine_CapErr JE_RES(CapabilityReport__runtime)(jz_str message) {
    jazz_engine_UnsupportedReason gap = jazz_engine__ur_new(JAZZ_ENGINE_UR_RUNTIME);
    gap.message = message;
    return jazz_engine__cap_single(gap);
}
jazz_engine_CapErr JE_RES(CapabilityReport__operator)(jz_str message) {
    jazz_engine_UnsupportedReason gap = jazz_engine__ur_new(JAZZ_ENGINE_UR_OPERATOR);
    gap.message = message;
    return jazz_engine__cap_single(gap);
}
jazz_engine_CapErr JE_RES(CapabilityReport__explained)(jazz_engine_UnsupportedReason gap, jz_str capability) {
    jazz_engine_CapabilityReport *r = jazz_engine__cap_single(gap);
    JZ_VEC_PUSH(r->explain.capabilities, capability);
    return r;
}
jazz_engine_CapErr jazz_engine__cap_runtime_fmt(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    jz_str s = vformat(fmt, ap);
    va_end(ap);
    return JE_RES(CapabilityReport__runtime)(s);
}
jazz_engine_CapErr jazz_engine__cap_operator_fmt(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    jz_str s = vformat(fmt, ap);
    va_end(ap);
    return JE_RES(CapabilityReport__operator)(s);
}
void jazz_engine__ur_drop_v(void *r) { ur_drop_v(r); }
