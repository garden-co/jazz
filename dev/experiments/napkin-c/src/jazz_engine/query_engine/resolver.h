/* resolver.h: source requests, resolved sources and capability reports
 * (query_engine/resolver.rs). Included from mod.h.
 *
 * CapabilityResult<T> = Result<T, Box<CapabilityReport>>: in C the function returns
 * the boxed report (NULL = Ok) and writes T through an out-parameter. */
#ifndef JAZZ_ENGINE_QUERY_ENGINE_RESOLVER_H
#define JAZZ_ENGINE_QUERY_ENGINE_RESOLVER_H

typedef struct jazz_engine_PolicyAuthorizationPlan {
    jazz_engine_SourceId protected_source;
    u8 role;
    jz_str protected_row_field;
    bool has_binding_source_shape; jz_str binding_source_shape;
    jz_btree binding_user_params;  /* String -> ColumnType */
    jz_btree binding_claim_params; /* String -> ProgramClaimParam */
} jazz_engine_PolicyAuthorizationPlan;
enum { JAZZ_ENGINE_SAR_SYSTEM, JAZZ_ENGINE_SAR_POLICY_FILTERED, JAZZ_ENGINE_SAR_POLICY_PROOF };
typedef struct jazz_engine_SourceAuthorizationRequest {
    u8 tag;
    jazz_types_AuthorSubject permission_subject;
    jazz_engine_PolicyAuthorizationPlan plan;
} jazz_engine_SourceAuthorizationRequest;

enum {
    JAZZ_ENGINE_SMR_VERSION_WITNESSES, JAZZ_ENGINE_SMR_SETTLE_POSITION, JAZZ_ENGINE_SMR_VERSION_PAYLOADS,
    JAZZ_ENGINE_SMR_DELETION_MARKERS, JAZZ_ENGINE_SMR_BATCH_MEMBERSHIP, JAZZ_ENGINE_SMR_COVERAGE,
    JAZZ_ENGINE_SMR_VALIDATION_READS, JAZZ_ENGINE_SMR_POLICY_WITNESSES, JAZZ_ENGINE_SMR_PROVENANCE, JAZZ_ENGINE_SMR_AUTHOR_PATH
};
typedef struct jazz_engine_SourceMetadataRequirement { u8 tag; u8 provenance; jz_str author_path; } jazz_engine_SourceMetadataRequirement;
enum { JAZZ_ENGINE_FREQ_NONE_, JAZZ_ENGINE_FREQ_ALL, JAZZ_ENGINE_FREQ_FIELDS };
typedef struct jazz_engine_FieldRequirement { u8 tag; jz_btree fields; } jazz_engine_FieldRequirement;
typedef struct jazz_engine_SourceRequirements {
    jazz_engine_FieldRequirement app_fields;
    jz_btree metadata; /* BTreeSet<SourceMetadataRequirement> */
} jazz_engine_SourceRequirements;
typedef struct jazz_engine_SourceRequest {
    jazz_engine_SourceId source;
    u8 visibility;
    jazz_engine_SourceAuthorizationRequest authorization;
    jazz_engine_SourceRequirements requirements;
} jazz_engine_SourceRequest;
typedef JZ_VEC(jazz_engine_SourceRequest) jazz_engine_SourceRequestVec;

enum {
    JAZZ_ENGINE_SMF_VERSION_WITNESSES, JAZZ_ENGINE_SMF_SETTLE_POSITION, JAZZ_ENGINE_SMF_DELETION_MARKERS,
    JAZZ_ENGINE_SMF_BATCH_MEMBERSHIP, JAZZ_ENGINE_SMF_COVERAGE, JAZZ_ENGINE_SMF_VALIDATION_READS,
    JAZZ_ENGINE_SMF_POLICY_WITNESSES, JAZZ_ENGINE_SMF_PROVENANCE
};
/* SourceMetadataFields: up to four String fields and up to two optional ones, by variant:
 * VersionWitnesses{a=schema_version, b=tx_time, c=tx_node, opt0=branch_or_prefix}
 * SettlePosition{a}, DeletionMarkers{a=state, opt0=tx_time, opt1=tx_node},
 * BatchMembership{a=batch_id, opt0=branch_or_prefix, b=row_digest, c=batch_kind},
 * Coverage{a}, ValidationReads{a}, PolicyWitnesses{a=policy_path, b=edge_kind}, Provenance{a=field} */
typedef struct jazz_engine_SourceMetadataFields {
    u8 tag;
    jz_str a, b, c;
    bool has_opt0; jz_str opt0;
    bool has_opt1; jz_str opt1;
} jazz_engine_SourceMetadataFields;
typedef struct jazz_engine_SourceRowShape {
    jazz_engine_SourceId source;
    groove_RecordDescriptor descriptor;
    jz_str row_uuid_field;
    jz_btree metadata; /* BTreeMap<SourceMetadataRequirement, SourceMetadataFields> */
} jazz_engine_SourceRowShape;
typedef struct jazz_engine_GraphWithRowField { groove_GraphBuilder *graph; jz_str row_uuid_field; } jazz_engine_GraphWithRowField;
typedef struct jazz_engine_AuthorizedDeletionPreimage { groove_GraphBuilder *graph; jz_btree routing_fields; } jazz_engine_AuthorizedDeletionPreimage;
typedef struct jazz_engine_ResolvedSource {
    jz_btree stored_column_ids;              /* String -> PhysicalColumnId */
    jazz_model_TableSchema *table_schema;    /* Arc<TableSchema> (jz_rc payload) */
    groove_GraphBuilder *graph;
    jazz_engine_SourceRowShape row_shape;
    jz_btree routing_fields;                 /* BTreeSet<String> */
    bool requires_result_payload;
    bool has_content_version; jazz_engine_GraphWithRowField content_version;
    bool has_deletion_register; jazz_engine_GraphWithRowField deletion_register;
    bool has_authorized_deletion_preimage; jazz_engine_AuthorizedDeletionPreimage authorized_deletion_preimage;
} jazz_engine_ResolvedSource;

enum {
    JAZZ_ENGINE_GAP_LOCAL_AVAILABILITY_INPUT, JAZZ_ENGINE_GAP_POLICY_PROOF_CYCLE, JAZZ_ENGINE_GAP_HISTORICAL_STORAGE_CUT,
    JAZZ_ENGINE_GAP_SNAPSHOT_REF, JAZZ_ENGINE_GAP_SCHEMA_PROJECTION, JAZZ_ENGINE_GAP_BRANCH_OVERLAY,
    JAZZ_ENGINE_GAP_TRANSACTION_READ_OVERLAY, JAZZ_ENGINE_GAP_COVERAGE
};
typedef struct jazz_engine_SourceGap { u8 tag; jz_str table; u32 depth; } jazz_engine_SourceGap;
typedef struct jazz_engine_SourceResolutionError { jazz_engine_SourceRequest *request; jazz_engine_SourceGap gap; } jazz_engine_SourceResolutionError;

enum { JAZZ_ENGINE_UR_SOURCE, JAZZ_ENGINE_UR_UNBOUND_CLAIM, JAZZ_ENGINE_UR_OPERATOR, JAZZ_ENGINE_UR_OUTPUT, JAZZ_ENGINE_UR_RUNTIME };
typedef struct jazz_engine_UnsupportedReason {
    u8 tag;
    jazz_engine_SourceGap source;
    jazz_engine_ClaimPath claim;
    jz_str message;                       /* Operator / Runtime */
    jazz_engine_ProgramFactKey *output;   /* Output(Box<_>) */
} jazz_engine_UnsupportedReason;
typedef JZ_VEC(jazz_engine_UnsupportedReason) jazz_engine_UnsupportedReasonVec;
typedef struct jazz_engine_ExplainPlan {
    jz_str input;
    jazz_engine_StrVec read, policy, output, capabilities, physical;
} jazz_engine_ExplainPlan;
typedef struct jazz_engine_CapabilityReport { jazz_engine_UnsupportedReasonVec gaps; jazz_engine_ExplainPlan explain; } jazz_engine_CapabilityReport;
typedef jazz_engine_CapabilityReport *jazz_engine_CapErr; /* NULL = Ok */
#define JE_CAP_TRY(expr) do { jazz_engine_CapErr _ce = (expr); if (_ce) return _ce; } while (0)

/* SourceGraphPreparer trait object (async -> synchronous). Error out-param on failure. */
typedef struct jazz_engine_SourceGraphPreparer {
    bool (*prepare_source_graph)(void *self, const jazz_engine_SourceRequest *request, jazz_engine_ResolvedSource *out,
                                 jazz_engine_SourceResolutionError *err);
    void *self;
} jazz_engine_SourceGraphPreparer;

extern const jz_btree_desc jazz_engine__SMR_SET_DESC;
extern const jz_btree_desc jazz_engine__SMR_FIELDS_MAP_DESC;
extern const jz_btree_desc jazz_engine__SOURCE_ID_RESOLVED_MAP_DESC; /* BTreeMap<SourceId, ResolvedSource> */
extern const jz_btree_desc jazz_engine__SOURCE_ID_REQ_MAP_DESC;      /* BTreeMap<SourceId, SourceRequirements> */
#define JE_RES(x) jazz_engine__query_engine__resolver__##x
void JE_RES(PolicyAuthorizationPlan__Clone__clone)(jazz_engine_PolicyAuthorizationPlan *dst, const jazz_engine_PolicyAuthorizationPlan *src);
void JE_RES(PolicyAuthorizationPlan__drop)(jazz_engine_PolicyAuthorizationPlan *p);
void JE_RES(SourceAuthorizationRequest__Clone__clone)(jazz_engine_SourceAuthorizationRequest *dst, const jazz_engine_SourceAuthorizationRequest *src);
void JE_RES(SourceAuthorizationRequest__drop)(jazz_engine_SourceAuthorizationRequest *a);
void JE_RES(SourceAuthorizationRequest__Debug__fmt)(jz_str *out, const jazz_engine_SourceAuthorizationRequest *a);
void JE_RES(SourceMetadataRequirement__Clone__clone)(jazz_engine_SourceMetadataRequirement *dst, const jazz_engine_SourceMetadataRequirement *src);
void JE_RES(SourceMetadataRequirement__drop)(jazz_engine_SourceMetadataRequirement *r);
int JE_RES(SourceMetadataRequirement__Ord__cmp)(const jazz_engine_SourceMetadataRequirement *a, const jazz_engine_SourceMetadataRequirement *b);
void JE_RES(SourceMetadataRequirement__Debug__fmt)(jz_str *out, const jazz_engine_SourceMetadataRequirement *r);
jazz_engine_SourceMetadataRequirement jazz_engine__smr_simple(u8 tag);
void JE_RES(FieldRequirement__Clone__clone)(jazz_engine_FieldRequirement *dst, const jazz_engine_FieldRequirement *src);
void JE_RES(FieldRequirement__drop)(jazz_engine_FieldRequirement *f);
bool JE_RES(FieldRequirement__PartialEq__eq)(const jazz_engine_FieldRequirement *a, const jazz_engine_FieldRequirement *b);
void JE_RES(FieldRequirement__Debug__fmt)(jz_str *out, const jazz_engine_FieldRequirement *f);
void JE_RES(SourceRequirements__Default__default)(jazz_engine_SourceRequirements *r);
void JE_RES(SourceRequirements__Clone__clone)(jazz_engine_SourceRequirements *dst, const jazz_engine_SourceRequirements *src);
void JE_RES(SourceRequirements__drop)(jazz_engine_SourceRequirements *r);
bool JE_RES(SourceRequirements__PartialEq__eq)(const jazz_engine_SourceRequirements *a, const jazz_engine_SourceRequirements *b);
void JE_RES(SourceRequirements__Debug__fmt)(jz_str *out, const jazz_engine_SourceRequirements *r);
void JE_RES(SourceRequest__Clone__clone)(jazz_engine_SourceRequest *dst, const jazz_engine_SourceRequest *src);
void JE_RES(SourceRequest__drop)(jazz_engine_SourceRequest *r);
void JE_RES(SourceRequest__Debug__fmt)(jz_str *out, const jazz_engine_SourceRequest *r);
void JE_RES(SourceMetadataFields__Clone__clone)(jazz_engine_SourceMetadataFields *dst, const jazz_engine_SourceMetadataFields *src);
void JE_RES(SourceMetadataFields__drop)(jazz_engine_SourceMetadataFields *f);
void JE_RES(SourceRowShape__Clone__clone)(jazz_engine_SourceRowShape *dst, const jazz_engine_SourceRowShape *src);
void JE_RES(SourceRowShape__drop)(jazz_engine_SourceRowShape *s);
void JE_RES(ResolvedSource__Clone__clone)(jazz_engine_ResolvedSource *dst, const jazz_engine_ResolvedSource *src);
void JE_RES(ResolvedSource__drop)(jazz_engine_ResolvedSource *s);
jazz_engine_CapErr JE_RES(ResolvedSource__lookup)(const jz_btree *sources, const jazz_engine_SourceId *id, const char *role, const jazz_engine_ResolvedSource **out);
void JE_RES(SourceResolutionError__drop)(jazz_engine_SourceResolutionError *e);
void JE_RES(SourceResolutionError__Debug__fmt_alternate)(jz_str *out, const jazz_engine_SourceResolutionError *e);
void JE_RES(SourceGap__Clone__clone)(jazz_engine_SourceGap *dst, const jazz_engine_SourceGap *src);
void JE_RES(SourceGap__Debug__fmt)(jz_str *out, const jazz_engine_SourceGap *g);
void JE_RES(UnsupportedReason__Clone__clone)(jazz_engine_UnsupportedReason *dst, const jazz_engine_UnsupportedReason *src);
void JE_RES(UnsupportedReason__drop)(jazz_engine_UnsupportedReason *r);
void JE_RES(UnsupportedReason__Debug__fmt)(jz_str *out, const jazz_engine_UnsupportedReason *r);
void JE_RES(ExplainPlan__Clone__clone)(jazz_engine_ExplainPlan *dst, const jazz_engine_ExplainPlan *src);
void JE_RES(ExplainPlan__drop)(jazz_engine_ExplainPlan *p);
void JE_RES(CapabilityReport__drop)(jazz_engine_CapabilityReport *r);
void JE_RES(CapabilityReport__free)(jazz_engine_CapabilityReport *r); /* drop Box<CapabilityReport> */
jazz_engine_CapErr JE_RES(CapabilityReport__runtime)(jz_str message);
jazz_engine_CapErr JE_RES(CapabilityReport__operator)(jz_str message);
jazz_engine_CapErr JE_RES(CapabilityReport__explained)(jazz_engine_UnsupportedReason gap, jz_str capability);
jazz_engine_CapErr jazz_engine__cap_single(jazz_engine_UnsupportedReason gap);
jazz_engine_CapErr jazz_engine__cap_runtime_fmt(const char *fmt, ...);
jazz_engine_CapErr jazz_engine__cap_operator_fmt(const char *fmt, ...);
void jazz_engine__resolved_drop_v(void *s);
void jazz_engine__resolved_clone_v(void *d, const void *s);
void jazz_engine__smr_drop_v(void *r);
void jazz_engine__smr_clone_v(void *d, const void *s);
void jazz_engine__reqs_drop_v(void *r);
void jazz_engine__reqs_clone_v(void *d, const void *s);
void jazz_engine__source_request_drop_v(void *r);
void jazz_engine__source_request_clone_v(void *d, const void *s);
void jazz_engine__ur_drop_v(void *r);
static inline jazz_engine_UnsupportedReason jazz_engine__ur_new(u8 tag) { jazz_engine_UnsupportedReason r; memset(&r, 0, sizeof r); r.tag = tag; return r; }

#endif
