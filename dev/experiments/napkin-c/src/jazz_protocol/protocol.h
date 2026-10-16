/* protocol.h: types of jazz_protocol::protocol (crates/jazz/layers/protocol/src/protocol.rs)
 * plus the small sibling modules protocol_limits / storage_codec_profile. */
#ifndef JAZZ_PROTOCOL_PROTOCOL_H
#define JAZZ_PROTOCOL_PROTOCOL_H
#include "jz_rt.h"
#include "jazz_protocol/_ext_protocol-types.h"
#include "jazz_types/jazz_types.h"

/* ---------------- error codes (crate-local enum space 0x0400..) ---------------- */
enum {
    /* VersionBundleRunError */
    JAZZ_PROTO_ERR_MALFORMED_VERSION_RECORD = 0x0400,
    JAZZ_PROTO_ERR_EMPTY_RUN,
    JAZZ_PROTO_ERR_BODY_COUNT_MISMATCH,
    JAZZ_PROTO_ERR_OVERRIDE_INDEX_OUT_OF_RANGE,
    JAZZ_PROTO_ERR_DUPLICATE_OVERRIDE,
    JAZZ_PROTO_ERR_TABLE_MISMATCH,
    /* WireContractError (VersionCarrier wraps the run error code above) */
    JAZZ_PROTO_ERR_INVALID_SUPPORTING_REVISION,
    JAZZ_PROTO_ERR_INVALID_SUPPORTING_ROW,
    JAZZ_PROTO_ERR_DUPLICATE_SUPPORTING_ROW,
    /* Result<_, String> / Result<_, &'static str> */
    JAZZ_PROTO_ERR_MSG,
    /* serde custom error inside postcard */
    JAZZ_PROTO_ERR_SERDE_CUSTOM,
    /* postcard::Error::DeserializeBadOption / UnexpectedEnd / BadEncoding */
    JAZZ_PROTO_ERR_PC_BAD_OPTION,
    JAZZ_PROTO_ERR_PC_UNEXPECTED_END,
    JAZZ_PROTO_ERR_PC_SERIALIZE_BUFFER_FULL,
};

typedef groove_InternStr jazz_protocol_InternStr;
typedef jz_btree jazz_protocol_ValueMap; /* BTreeMap<String, Value>, desc jazz_model__btree_desc_str_value */
typedef jz_btree jazz_protocol_StrSet;   /* BTreeSet<String> */
extern const jz_btree_desc jazz_protocol__protocol__DESC_STR_SET;

/* ---------------- keys ---------------- */
typedef struct jazz_protocol_ReadViewKey { jz_uuid id; } jazz_protocol_ReadViewKey;
typedef struct jazz_protocol_SubscriptionKey {
    jazz_model_ShapeId shape_id; jazz_model_BindingId binding_id; jazz_protocol_ReadViewKey read_view;
} jazz_protocol_SubscriptionKey;
typedef jazz_protocol_SubscriptionKey jazz_protocol_BindingViewKey;
typedef struct jazz_protocol_PermissionAdviceRequestId { u8 b[16]; } jazz_protocol_PermissionAdviceRequestId;

typedef struct jazz_protocol_CanonicalPolicyClaims {
    jazz_protocol_ValueMap claims;
    jz_bytes comparison_key;
} jazz_protocol_CanonicalPolicyClaims;
typedef struct jazz_protocol_PolicyBindingKey {
    jazz_types_AuthorSubject identity;
    jazz_protocol_CanonicalPolicyClaims canonical_claims;
} jazz_protocol_PolicyBindingKey;
typedef struct jazz_protocol_AuthorityResultKey {
    jazz_protocol_BindingViewKey binding_view;
    bool has_policy_binding; jazz_protocol_PolicyBindingKey policy_binding;
} jazz_protocol_AuthorityResultKey;
typedef struct jazz_protocol_DelegatedSessionBinding {
    jazz_types_AuthorSubject identity;
    jazz_protocol_ValueMap claims;
} jazz_protocol_DelegatedSessionBinding;

/* ---------------- chunks ---------------- */
typedef struct jazz_protocol_ChunkRequestEntry {
    u64 request_id; groove_Locator locator; u8 expected_hash[32]; u8 remaining_hops;
} jazz_protocol_ChunkRequestEntry;
typedef struct jazz_protocol_ChunkRequestBatch { JZ_VEC(jazz_protocol_ChunkRequestEntry) requests; } jazz_protocol_ChunkRequestBatch;
enum { JAZZ_CHUNK_FOUND = 0, JAZZ_CHUNK_UNAVAILABLE = 1, JAZZ_CHUNK_RETRYABLE = 2 };
typedef struct jazz_protocol_ChunkResponse { u32 tag; jz_bytes found; u32 retry_after_ms; } jazz_protocol_ChunkResponse;
typedef struct jazz_protocol_ChunkResponseEntry { u64 request_id; jazz_protocol_ChunkResponse result; } jazz_protocol_ChunkResponseEntry;
typedef struct jazz_protocol_ChunkResponseBatch { JZ_VEC(jazz_protocol_ChunkResponseEntry) responses; } jazz_protocol_ChunkResponseBatch;
typedef struct jazz_protocol_ChunkUploadStart { groove_LargeValueRef *value_ref; } jazz_protocol_ChunkUploadStart;
typedef struct jazz_protocol_ChunkUploadNodes { groove_LargeValueRef *value_ref; JZ_VEC(groove_StagedChunk) chunks; } jazz_protocol_ChunkUploadNodes;
enum { JAZZ_UPLOAD_NEED = 0, JAZZ_UPLOAD_STAGED, JAZZ_UPLOAD_RATE_LIMITED, JAZZ_UPLOAD_REJECTED };
typedef struct jazz_protocol_ChunkUploadStatus { u32 tag; JZ_VEC(groove_NodeRef) need; } jazz_protocol_ChunkUploadStatus;
typedef struct jazz_protocol_ChunkUploadResult { groove_LargeValueRef *value_ref; jazz_protocol_ChunkUploadStatus status; } jazz_protocol_ChunkUploadResult;

/* ---------------- authorization advice ---------------- */
enum { JAZZ_PAA_INSERT = 0, JAZZ_PAA_READ, JAZZ_PAA_UPDATE, JAZZ_PAA_DELETE };
typedef struct jazz_protocol_PermissionAdviceAction {
    u32 tag; jz_str table; jazz_types_RowUuid row;
    jazz_protocol_ValueMap cells; /* Insert.cells / Update.patch */
} jazz_protocol_PermissionAdviceAction;
enum { JAZZ_ASO_READ = 0, JAZZ_ASO_INSERT, JAZZ_ASO_UPDATE, JAZZ_ASO_DELETE };
typedef struct jazz_protocol_AuthorizationSupportScopeKey {
    u8 support_shape_digest[32];
    jazz_types_AuthorSubject subject;
    u8 claims_digest[32];
    u8 policy_digest[32];
} jazz_protocol_AuthorizationSupportScopeKey;
typedef struct jazz_protocol_AuthorizationOperationKey {
    u32 operation; jz_str table; bool has_row; jazz_types_RowUuid row; u8 candidate_digest[32];
} jazz_protocol_AuthorizationOperationKey;
typedef struct jazz_protocol_AuthorizationScopeReceipt {
    jazz_protocol_AuthorizationSupportScopeKey key;
    u8 authority[16];
    jazz_types_AuthorSubject link;
    u64 authority_epoch, claims_revision, policy_epoch;
    jazz_types_GlobalTime settled_through;
    u64 authorization_progress;
} jazz_protocol_AuthorizationScopeReceipt;
enum { JAZZ_ADVICE_ALLOWED = 0, JAZZ_ADVICE_DENIED, JAZZ_ADVICE_UNKNOWN };

/* ---------------- version records ---------------- */
typedef struct jazz_protocol_RowBase {
    bool has_seq; jazz_types_GlobalTime seq;
    bool has_pending; jazz_model_TxId pending;
} jazz_protocol_RowBase;
typedef struct jazz_protocol_VersionRecord {
    jazz_protocol_InternStr table;
    jazz_types_SchemaVersionId schema_version;
    jazz_model_BranchKey branch_key;
    groove_OwnedRecord record;
    bool has_authored_columns; jazz_protocol_StrSet authored_columns;
    jazz_protocol_RowBase base;
    jz_bytes counter_signs;
} jazz_protocol_VersionRecord;
typedef JZ_VEC(jazz_protocol_VersionRecord) jazz_protocol_VersionRecordVec;
/* WireRowRecord field indices */
enum {
    JAZZ_WRR_ROW_UUID = 0, JAZZ_WRR_CREATED_BY = 1, JAZZ_WRR_CREATED_AT = 2,
    JAZZ_WRR_UPDATED_BY = 3, JAZZ_WRR_UPDATED_AT = 4, JAZZ_WRR_DELETION = 5, JAZZ_WRR_USER_CELLS = 6
};

enum { JAZZ_SCOPE_COMPLETE_TRANSACTION = 0, JAZZ_SCOPE_VIEW_SCOPED = 1 };
typedef struct jazz_protocol_VersionBundle {
    jazz_model_Transaction tx;
    jazz_protocol_VersionRecordVec versions;
    u32 scope;
    jazz_model_Fate fate;
    bool has_global_time; jazz_types_GlobalTime global_time;
    u32 durability;
} jazz_protocol_VersionBundle;
typedef struct jazz_protocol_VersionBundleRef {
    const jazz_model_Transaction *tx;
    const jazz_protocol_VersionRecord *versions; u32 n_versions;
    u32 scope;
    const jazz_model_Fate *fate;
    bool has_global_time; jazz_types_GlobalTime global_time;
    u32 durability;
} jazz_protocol_VersionBundleRef;
typedef JZ_VEC(jazz_protocol_VersionBundleRef) jazz_protocol_VersionBundleRefVec;
typedef JZ_VEC(jazz_protocol_VersionBundle) jazz_protocol_VersionBundleVec;
typedef struct jazz_protocol_VersionBundleRunHeader {
    bool has_table; jazz_protocol_InternStr table;
    jazz_model_Transaction tx;
    u32 scope;
    u32 body_count;
    jazz_model_Fate fate;
    bool has_global_time; jazz_types_GlobalTime global_time;
    u32 durability;
} jazz_protocol_VersionBundleRunHeader;
typedef struct jazz_protocol_VersionBundleRunBody { jazz_protocol_VersionRecordVec versions; } jazz_protocol_VersionBundleRunBody;
typedef struct jazz_protocol_VersionBundleRunOverride {
    u32 body_index;
    bool has_tx; jazz_model_Transaction tx;
    bool has_scope; u32 scope;
    bool has_fate; jazz_model_Fate fate;
    bool has_global_time; bool global_time_some; jazz_types_GlobalTime global_time; /* Option<Option<GlobalTime>> */
    bool has_durability; u32 durability;
} jazz_protocol_VersionBundleRunOverride;
typedef struct jazz_protocol_VersionBundleRun {
    jazz_protocol_VersionBundleRunHeader header;
    JZ_VEC(jazz_protocol_VersionBundleRunBody) bodies;
    JZ_VEC(jazz_protocol_VersionBundleRunOverride) overrides;
} jazz_protocol_VersionBundleRun;
enum { JAZZ_CARRIER_BUNDLE = 0, JAZZ_CARRIER_RUN = 1 };
typedef struct jazz_protocol_VersionCarrier {
    u32 tag;
    union { jazz_protocol_VersionBundle bundle; jazz_protocol_VersionBundleRun run; } u;
} jazz_protocol_VersionCarrier;
typedef JZ_VEC(jazz_protocol_VersionCarrier) jazz_protocol_VersionCarrierVec;

typedef struct jazz_protocol_PeerPayloadInventory {
    JZ_VEC(jazz_model_TxId) complete_tx_payloads;
    bool has_authorization_progress; u64 authorization_progress;
    bool opening_pending;
} jazz_protocol_PeerPayloadInventory;

/* ---------------- result rows ---------------- */
enum { JAZZ_LAYER_CONTENT = 0, JAZZ_LAYER_DELETION = 1, JAZZ_LAYER_CONTENT_OR_DELETION = 2 };
typedef struct jazz_protocol_OptBytes { bool some; jz_bytes v; } jazz_protocol_OptBytes;
typedef struct jazz_protocol_OptTxId { bool some; jazz_model_TxId v; } jazz_protocol_OptTxId;
typedef struct jazz_protocol_RowVersionRefEntry {
    jazz_model_TxId tx;
    bool has_schema_version; jazz_types_SchemaVersionId schema_version;
    u32 layer;
    jazz_protocol_OptTxId batch;
    jazz_protocol_OptBytes branch_or_prefix;
    jazz_protocol_OptBytes row_digest;
} jazz_protocol_RowVersionRefEntry;
typedef struct jazz_protocol_SupportingRow {
    jazz_types_GlobalPhysicalTableId physical_table;
    jazz_protocol_InternStr version_table;
    jazz_types_RowUuid row;
    jazz_protocol_RowVersionRefEntry version;
} jazz_protocol_SupportingRow;
typedef JZ_VEC(jazz_protocol_SupportingRow) jazz_protocol_SupportingRowVec;
enum { JAZZ_SRU_SNAPSHOT = 0, JAZZ_SRU_DELTA = 1, JAZZ_SRU_CATCH_UP = 2 };
typedef struct jazz_protocol_SupportingRowsUpdate {
    u32 tag;
    u8 predecessor[16]; /* Delta / CatchUp */
    u8 revision[16];
    jazz_protocol_SupportingRowVec added;   /* rows / adds / changed */
    jazz_protocol_SupportingRowVec removed; /* - / removes / left */
} jazz_protocol_SupportingRowsUpdate;
typedef struct jazz_protocol_ViewUpdatePayload {
    jazz_protocol_SubscriptionKey subscription;
    jazz_types_GlobalTime settled_through;
    jazz_protocol_VersionCarrierVec version_carriers;
    jazz_protocol_PeerPayloadInventory peer_payload_inventory;
    jazz_protocol_SupportingRowsUpdate supporting_rows;
} jazz_protocol_ViewUpdatePayload;

typedef struct jazz_protocol_ResultRowSource jazz_protocol_ResultRowSource;
enum { JAZZ_RRS_CURRENT = 0, JAZZ_RRS_SNAPSHOT, JAZZ_RRS_HISTORY_CUT, JAZZ_RRS_MERGE, JAZZ_RRS_LENS_PROJECTION, JAZZ_RRS_OVERLAY };
struct jazz_protocol_ResultRowSource {
    u32 tag;
    union {
        jazz_model_SnapshotRef snapshot;
        jazz_types_GlobalTime global_time;
        JZ_VEC(jazz_protocol_ResultRowSource) inputs;
        struct { jazz_types_SchemaVersionId schema_version; jazz_protocol_ResultRowSource *base; } lens;
        struct { jazz_model_TxId tx; jazz_protocol_ResultRowSource *base; } overlay;
    } u;
};
typedef struct jazz_protocol_RealRowMemberEntry {
    jazz_protocol_InternStr table;
    jazz_types_RowUuid row_uuid;
    bool has_occurrence_id; jazz_types_OutputOccurrenceId occurrence_id;
    jazz_protocol_OptTxId content_tx;
    u32 layer;
    jazz_protocol_OptTxId deletion_tx;
    jazz_protocol_ResultRowSource source;
    jazz_protocol_ReadViewKey read_view;
    bool has_schema_version; jazz_types_SchemaVersionId schema_version;
    jazz_protocol_OptBytes branch_or_prefix;
    jazz_protocol_OptBytes row_digest;
    jazz_protocol_OptTxId batch;
    bool has_settle_position; jazz_types_GlobalTime settle_position;
} jazz_protocol_RealRowMemberEntry;
/* ResultRowEntry = (Intern<String>, RowUuid, TxId) */
typedef struct jazz_protocol_ResultRowEntry { jazz_protocol_InternStr table; jazz_types_RowUuid row; jazz_model_TxId tx; } jazz_protocol_ResultRowEntry;
enum { JAZZ_RME_ROW = 0, JAZZ_RME_SYNTHETIC, JAZZ_RME_PATH_TUPLE, JAZZ_RME_TYPED_ROW };
typedef struct jazz_protocol_ResultMemberEntry {
    u32 tag;
    union {
        jazz_protocol_RealRowMemberEntry row; /* Row */
        struct { jz_str table; jz_bytes row; jz_bytes replacement; } synthetic;
        struct {
            jz_str path; jazz_protocol_InternStr source_table; jazz_types_RowUuid source_row;
            jazz_protocol_InternStr target_table; jazz_types_RowUuid target_row;
            jazz_protocol_OptBytes edge_id; jz_bytes revision;
        } path_tuple;
        struct { jazz_protocol_RealRowMemberEntry row; jazz_types_ResultKey occurrence_key; } typed;
    } u;
} jazz_protocol_ResultMemberEntry;

/* ---------------- program facts ---------------- */
enum { JAZZ_EDGE_INCLUDE = 0, JAZZ_EDGE_JOIN, JAZZ_EDGE_RELATION, JAZZ_EDGE_RECURSIVE, JAZZ_EDGE_POLICY };
enum { JAZZ_ROLE_INTERMEDIATE = 0, JAZZ_ROLE_FRONTIER, JAZZ_ROLE_TERMINAL };
enum { JAZZ_HOLE_MATCHED = 0, JAZZ_HOLE_HOLE };
enum { JAZZ_PSR_ROOT = 0, JAZZ_PSR_ALIAS, JAZZ_PSR_RECURSIVE_SEED, JAZZ_PSR_RECURSIVE_STEP, JAZZ_PSR_CORRELATED_CHILD, JAZZ_PSR_POLICY };
typedef struct jazz_protocol_ProgramSourceRole { u32 tag; jz_str name; } jazz_protocol_ProgramSourceRole;
typedef struct jazz_protocol_ProgramSourceId {
    jazz_protocol_InternStr table; JZ_VEC(jazz_protocol_ProgramSourceRole) path;
} jazz_protocol_ProgramSourceId;
#define JAZZ_MAX_PROGRAM_SOURCE_PATH_SEGMENTS 32u
typedef struct jazz_protocol_OptMember { jazz_protocol_ResultMemberEntry *v; } jazz_protocol_OptMember; /* Option<ResultMemberEntry> boxed */
typedef struct jazz_protocol_OptStr { bool some; jz_str v; } jazz_protocol_OptStr;
typedef struct jazz_protocol_OptU32Enum { bool some; u32 v; } jazz_protocol_OptU32Enum;

typedef struct jazz_protocol_ResultMemberPayloadEntry { jazz_protocol_ResultMemberEntry member; jz_bytes descriptor; jz_bytes record; } jazz_protocol_ResultMemberPayloadEntry;
typedef struct jazz_protocol_RelationEdgeEntry {
    jz_str path;
    jazz_protocol_InternStr source_table; jazz_types_RowUuid source_row;
    jazz_protocol_InternStr target_table; jazz_types_RowUuid target_row;
    jazz_protocol_OptU32Enum kind;
    bool has_source_version; jazz_protocol_RowVersionRefEntry source_version;
    bool has_target_version; jazz_protocol_RowVersionRefEntry target_version;
    bool has_depth; u32 depth;
    jazz_protocol_OptBytes edge_id;
    jazz_protocol_OptBytes branch;
    jazz_protocol_OptU32Enum role;
    jazz_protocol_OptBytes order;
    jazz_protocol_OptU32Enum hole_state;
} jazz_protocol_RelationEdgeEntry;
typedef struct jazz_protocol_PathCorrelationCoverageEntry {
    jz_str path; jazz_protocol_InternStr source_table; jazz_types_RowUuid source_row; jz_bytes correlation_key; bool complete;
} jazz_protocol_PathCorrelationCoverageEntry;
typedef struct jazz_protocol_ProgramSourceCoverageEntry { jazz_protocol_ProgramSourceId source; bool complete; } jazz_protocol_ProgramSourceCoverageEntry;
typedef struct jazz_protocol_ReadFrontierSettledEntry { jz_str scope; u32 tier; jazz_protocol_OptStr stream; jz_bytes frontier; } jazz_protocol_ReadFrontierSettledEntry;
typedef struct jazz_protocol_CompleteTxPayloadCoverageEntry { jazz_model_TxId tx; u32 tier; jz_bytes payload_digest; } jazz_protocol_CompleteTxPayloadCoverageEntry;
typedef struct jazz_protocol_ViewCompleteExclusiveCoverageEntry {
    jazz_model_TxId tx; jz_str scope; jazz_protocol_OptMember result; u32 tier; jz_bytes covered_members_digest;
} jazz_protocol_ViewCompleteExclusiveCoverageEntry;
enum { JAZZ_PDO_ALLOWED = 0, JAZZ_PDO_DENIED, JAZZ_PDO_INDETERMINATE_REQUIRES_INPUT, JAZZ_PDO_REQUIRES_COVERAGE };
typedef struct jazz_protocol_PolicyDecisionOutcomeEntry { u32 tag; jz_str s; jz_bytes frontier; } jazz_protocol_PolicyDecisionOutcomeEntry;
typedef struct jazz_protocol_PolicyDecisionEntry { jz_bytes decision; jazz_protocol_PolicyDecisionOutcomeEntry outcome; jazz_protocol_OptStr reason; } jazz_protocol_PolicyDecisionEntry;
typedef struct jazz_protocol_VersionWitnessEntry { jz_str role; jazz_protocol_RowVersionRefEntry version; jazz_protocol_OptMember member; } jazz_protocol_VersionWitnessEntry;
typedef struct jazz_protocol_CoveredInputEntry {
    jazz_protocol_ProgramSourceId source; jazz_protocol_InternStr version_table; jazz_types_RowUuid source_row; jazz_protocol_RowVersionRefEntry version;
} jazz_protocol_CoveredInputEntry;
typedef struct jazz_protocol_PolicyWitnessEntry {
    jazz_protocol_ResultMemberEntry protected_; jz_str policy_path; jazz_protocol_RowVersionRefEntry witness; jazz_protocol_OptU32Enum edge_kind;
} jazz_protocol_PolicyWitnessEntry;
typedef struct jazz_protocol_ContributingMembersEntry {
    jazz_protocol_ResultMemberEntry result; jazz_protocol_ResultMemberEntry contributor; jazz_protocol_OptTxId batch; jazz_protocol_OptStr role;
} jazz_protocol_ContributingMembersEntry;
enum { JAZZ_POSR_BASE = 0, JAZZ_POSR_NOW = 1 };
typedef struct jazz_protocol_PredicateReadEntry {
    u32 role; jazz_model_ShapeId shape_id; jazz_model_BindingId binding_id; jz_bytes predicate; jz_bytes frontier;
} jazz_protocol_PredicateReadEntry;
typedef struct jazz_protocol_PredicateOutputSetEntry {
    u32 role; jazz_protocol_InternStr table; jazz_types_RowUuid row; jazz_protocol_RowVersionRefEntry version;
    jazz_model_ShapeId shape_id; jazz_model_BindingId binding_id;
} jazz_protocol_PredicateOutputSetEntry;
typedef struct jazz_protocol_PointReadEntry {
    bool present; jazz_protocol_InternStr table; jazz_types_RowUuid row;
    bool has_version; jazz_protocol_RowVersionRefEntry version;
    jazz_model_ShapeId shape_id; jazz_model_BindingId binding_id;
} jazz_protocol_PointReadEntry;
enum {
    JAZZ_PFE_RESULT_PAYLOAD = 0, JAZZ_PFE_RELATION_EDGE, JAZZ_PFE_PATH_CORRELATION_COVERAGE,
    JAZZ_PFE_PROGRAM_SOURCE_COVERAGE, JAZZ_PFE_READ_FRONTIER_SETTLED, JAZZ_PFE_COMPLETE_TX_PAYLOAD_COVERAGE,
    JAZZ_PFE_VIEW_COMPLETE_EXCLUSIVE_COVERAGE, JAZZ_PFE_POLICY_DECISION, JAZZ_PFE_VERSION_WITNESS,
    JAZZ_PFE_COVERED_INPUT, JAZZ_PFE_POLICY_WITNESS, JAZZ_PFE_CONTRIBUTING_MEMBERS, JAZZ_PFE_PREDICATE_READ,
    JAZZ_PFE_PREDICATE_OUTPUT_SET, JAZZ_PFE_POINT_READ
};
typedef struct jazz_protocol_ProgramFactEntry {
    u32 tag;
    union {
        jazz_protocol_ResultMemberPayloadEntry result_payload;
        jazz_protocol_RelationEdgeEntry relation_edge;
        jazz_protocol_PathCorrelationCoverageEntry path_correlation;
        jazz_protocol_ProgramSourceCoverageEntry program_source;
        jazz_protocol_ReadFrontierSettledEntry read_frontier;
        jazz_protocol_CompleteTxPayloadCoverageEntry complete_tx;
        jazz_protocol_ViewCompleteExclusiveCoverageEntry view_complete;
        jazz_protocol_PolicyDecisionEntry policy_decision;
        jazz_protocol_VersionWitnessEntry version_witness;
        jazz_protocol_CoveredInputEntry covered_input;
        jazz_protocol_PolicyWitnessEntry policy_witness;
        jazz_protocol_ContributingMembersEntry contributing;
        jazz_protocol_PredicateReadEntry predicate_read;
        jazz_protocol_PredicateOutputSetEntry predicate_output;
        jazz_protocol_PointReadEntry point_read;
    } u;
} jazz_protocol_ProgramFactEntry;

/* ---------------- shapes / read views ---------------- */
enum { JAZZ_SHAPE_QUERY = 0, JAZZ_SHAPE_RELATION = 1 };
typedef struct jazz_protocol_ShapeAst {
    u32 version;
    jazz_types_SchemaVersionId schema_version;
    u32 body_tag;
    jazz_model_Query *query;              /* ShapeBody::Query */
    jazz_model_RelationQuery *relation;   /* ShapeBody::Relation */
} jazz_protocol_ShapeAst;
enum { JAZZ_RVS_CURRENT = 0, JAZZ_RVS_BRANCH_VIEW = 1, JAZZ_RVS_SNAPSHOT = 2 };
typedef struct jazz_protocol_ReadViewSourceSpec {
    u32 tag;
    jazz_model_BranchSelector head;
    bool has_base; jazz_model_BranchViewBase base;
    jazz_model_SnapshotRef snapshot;
} jazz_protocol_ReadViewSourceSpec;
typedef struct jazz_protocol_ReadViewSpec { jazz_protocol_ReadViewSourceSpec source; } jazz_protocol_ReadViewSpec;
enum { JAZZ_BINDING_ORDINARY = 0, JAZZ_BINDING_RELAY_AUTHORITY_SESSION = 1 };
typedef struct jazz_protocol_RegisterShapeOptions {
    u32 tier; jazz_protocol_ReadViewSpec read_view; u32 binding_source;
} jazz_protocol_RegisterShapeOptions;
typedef struct jazz_protocol_CoverageKey {
    jazz_model_ShapeId shape_id; jazz_model_BindingId binding_id;
    jazz_protocol_RegisterShapeOptions opts;
    bool has_policy_binding; jazz_protocol_PolicyBindingKey policy_binding;
} jazz_protocol_CoverageKey;

enum { JAZZ_KSD_FAST = 0, JAZZ_KSD_FAST_WITH_AUTH = 1, JAZZ_KSD_RESERVED2 = 2, JAZZ_KSD_WATERMARK = 3 };
typedef struct jazz_protocol_KnownStateDeclaration {
    u32 tag; u32 completeness; jazz_types_GlobalTime position;
    bool has_authorization_progress; u64 authorization_progress; /* FastWith: always has */
    u8 supporting_revision[16];
} jazz_protocol_KnownStateDeclaration;
typedef struct jazz_protocol_Subscribe {
    jazz_model_ShapeId shape_id;
    jazz_protocol_SubscriptionKey subscription;
    groove_ValueVec values;
    bool has_known_state; jazz_protocol_KnownStateDeclaration known_state;
    bool has_delegated_session; jazz_protocol_DelegatedSessionBinding delegated_session;
} jazz_protocol_Subscribe;
enum { JAZZ_SRR_UNSUPPORTED_SHAPE_CAPABILITY = 0, JAZZ_SRR_PENDING_CATALOGUE, JAZZ_SRR_SERVER_FAILURE, JAZZ_SRR_INVALID_AUTHORITY_SOURCE_CLOSURE };
enum { JAZZ_SSF_TABLE_NOT_FOUND = 0, JAZZ_SSF_SCHEMA_RESOLUTION, JAZZ_SSF_QUERY_VALIDATION, JAZZ_SSF_QUERY_LOWERING, JAZZ_SSF_POLICY_EVALUATION, JAZZ_SSF_INTERNAL };
typedef struct jazz_protocol_SubscribeRejectReason { u32 tag; jz_str text; u32 code; } jazz_protocol_SubscribeRejectReason;

/* ---------------- current rows ---------------- */
#define JAZZ_MAX_CURRENT_ROWS 64u
typedef struct jazz_protocol_CurrentRowCoordinate {
    jazz_types_SchemaVersionId schema; jz_str table; jazz_types_GlobalPhysicalTableId physical_table; jazz_types_RowUuid row;
} jazz_protocol_CurrentRowCoordinate;
typedef JZ_VEC(jazz_protocol_CurrentRowCoordinate) jazz_protocol_CurrentRowCoordinateVec;
typedef struct jazz_protocol_CurrentRowsRequest {
    jazz_protocol_PermissionAdviceRequestId request_id;
    jazz_protocol_CurrentRowCoordinateVec rows;
    bool has_delegated_session; jazz_protocol_DelegatedSessionBinding delegated_session;
} jazz_protocol_CurrentRowsRequest;
enum { JAZZ_CRO_READABLE = 0, JAZZ_CRO_CURRENT_UNAVAILABLE, JAZZ_CRO_UNKNOWN };
typedef struct jazz_protocol_CurrentRowsReceipt {
    jazz_protocol_PermissionAdviceRequestId request_id;
    jazz_protocol_CurrentRowCoordinateVec rows;
    JZ_VEC(u8) outcomes;
    jazz_protocol_PolicyBindingKey context;
    jazz_types_NodeUuid core;
    u64 core_epoch, claims_revision, policy_epoch;
    jazz_types_GlobalTime settled_through;
    u64 authorization_progress;
    jazz_protocol_VersionCarrierVec version_carriers;
} jazz_protocol_CurrentRowsReceipt;

/* ---------------- schemas / catalogue ---------------- */
typedef struct jazz_protocol_SchemaVersion { jazz_types_SchemaVersionId id; jazz_model_JazzSchema schema; } jazz_protocol_SchemaVersion;
typedef struct jazz_protocol_PhysicalColumnIdentity {
    jazz_types_GlobalPhysicalColumnId id;
    jz_btree enum_variants; /* String -> Vec<GlobalPhysicalEnumVariantId> */
} jazz_protocol_PhysicalColumnIdentity;
typedef struct jazz_protocol_PhysicalTableIdentity {
    jazz_types_GlobalPhysicalTableId id;
    jz_btree columns; /* String -> PhysicalColumnIdentity */
} jazz_protocol_PhysicalTableIdentity;
typedef struct jazz_protocol_PhysicalIdentityManifest { jz_btree tables; /* String -> PhysicalTableIdentity */ } jazz_protocol_PhysicalIdentityManifest;
typedef JZ_VEC(jz_uuid) jazz_protocol_UuidVec;

enum { JAZZ_LENS_RENAME_TABLE = 0, JAZZ_LENS_RENAME_COLUMN, JAZZ_LENS_COPY_COLUMN, JAZZ_LENS_ADD_COLUMN,
       JAZZ_LENS_DROP_COLUMN, JAZZ_LENS_TRANSFORM_COLUMN, JAZZ_LENS_REJECT_SOURCE_DELTA };
typedef struct jazz_protocol_LensOp {
    u32 tag;
    jz_str a;            /* from / column / reason */
    jz_str b;            /* to / transform */
    groove_Value value;  /* default / backwards_default */
} jazz_protocol_LensOp;
typedef struct jazz_protocol_TableLens { jz_str source_table; jz_str target_table; JZ_VEC(jazz_protocol_LensOp) ops; } jazz_protocol_TableLens;
typedef JZ_VEC(jazz_protocol_TableLens) jazz_protocol_TableLensVec;
typedef struct jazz_protocol_MigrationLens {
    jazz_types_MigrationLensId id;
    jazz_types_SchemaVersionId source, target;
    jazz_protocol_TableLensVec table_lenses;
} jazz_protocol_MigrationLens;
typedef JZ_VEC(jz_str) jazz_protocol_StrVec;
typedef struct jazz_protocol_SchemaPredecessor {
    jazz_protocol_MigrationLens lens; jazz_protocol_StrVec new_tables; jazz_protocol_StrVec dropped_tables;
} jazz_protocol_SchemaPredecessor;
typedef struct jazz_protocol_SchemaLineagePublication {
    jazz_types_SchemaLineagePublicationId id;
    jazz_protocol_SchemaVersion schema;
    JZ_VEC(jazz_protocol_SchemaPredecessor) predecessors;
    jazz_protocol_PhysicalIdentityManifest physical_identities;
} jazz_protocol_SchemaLineagePublication;
typedef struct jazz_protocol_CurrentWriteSchema { u64 revision; jazz_types_SchemaVersionId schema; } jazz_protocol_CurrentWriteSchema;
typedef struct jazz_protocol_CatalogueAck {
    bool has_revision; u64 revision;
    bool has_schema; jazz_types_SchemaVersionId schema;
    bool has_lens; jazz_types_MigrationLensId lens;
    bool applied;
} jazz_protocol_CatalogueAck;
typedef struct jazz_protocol_SeqLineage { u64 seq; jazz_protocol_SchemaLineagePublication publication; } jazz_protocol_SeqLineage;
typedef struct jazz_protocol_CatalogueSnapshot {
    jazz_protocol_PhysicalIdentityManifest genesis_physical_identities;
    JZ_VEC(jazz_protocol_SchemaVersion) schemas;
    JZ_VEC(jazz_protocol_SeqLineage) lineages;
    jazz_protocol_CurrentWriteSchema current_write_schema;
} jazz_protocol_CatalogueSnapshot;
/* (source schema, manifest) entries of BTreeMap<SchemaVersionId, (JazzSchema, PhysicalIdentityManifest)> */
typedef struct jazz_protocol_LineageSource { jazz_model_JazzSchema schema; jazz_protocol_PhysicalIdentityManifest manifest; } jazz_protocol_LineageSource;

/* ---------------- SyncMessage ---------------- */
enum {
    JAZZ_SM_CHUNK_REQUEST_BATCH = 0, JAZZ_SM_CHUNK_RESPONSE_BATCH, JAZZ_SM_SESSION_CLAIMS, JAZZ_SM_COMMIT_UNIT,
    JAZZ_SM_FATE_UPDATE, JAZZ_SM_REGISTER_SHAPE, JAZZ_SM_SUBSCRIBE, JAZZ_SM_SUBSCRIBE_REJECTED,
    JAZZ_SM_UNSUBSCRIBE, JAZZ_SM_PUBLISH_SCHEMA, JAZZ_SM_PUBLISH_SCHEMA_WITH_LENS, JAZZ_SM_PUBLISH_LENS,
    JAZZ_SM_CATALOGUE_ACK, JAZZ_SM_VIEW_UPDATE, JAZZ_SM_RESERVED14, JAZZ_SM_RESERVED15,
    JAZZ_SM_CATALOGUE_SNAPSHOT, JAZZ_SM_AUTH_SCOPE_INTENT, JAZZ_SM_AUTH_SCOPE_VIEW,
    JAZZ_SM_AUTH_SCOPE_AGGREGATE_RECEIPT, JAZZ_SM_AUTH_SCOPE_UNAVAILABLE, JAZZ_SM_AUTH_SCOPE_DECISION,
    JAZZ_SM_CHUNK_UPLOAD_START, JAZZ_SM_CHUNK_UPLOAD_NODES, JAZZ_SM_CHUNK_UPLOAD_RESULT,
    JAZZ_SM_CURRENT_ROWS_REQUEST, JAZZ_SM_CURRENT_ROWS_RECEIPT, JAZZ_SM_CURRENT_ROWS_CANCEL,
    JAZZ_SM_VIEW_UPDATE_PART, JAZZ_SM_RESERVED29, JAZZ_SM_READ_BARRIER, JAZZ_SM__COUNT
};
typedef struct jazz_protocol_SyncMessage {
    u32 tag;
    union {
        jazz_protocol_ChunkRequestBatch chunk_request_batch;
        jazz_protocol_ChunkResponseBatch chunk_response_batch;
        struct { jazz_types_AuthorSubject identity; jazz_protocol_ValueMap claims; } session_claims;
        struct { jazz_model_Transaction tx; jazz_protocol_VersionRecordVec versions; } commit_unit;
        struct {
            jazz_model_TxId tx_id; jazz_model_Fate fate;
            bool has_global_time; jazz_types_GlobalTime global_time;
            bool has_durability; u32 durability;
        } fate_update;
        struct { jazz_model_ShapeId shape_id; jazz_protocol_ShapeAst ast; jazz_protocol_RegisterShapeOptions opts; } register_shape;
        jazz_protocol_Subscribe subscribe;
        struct { jazz_protocol_SubscriptionKey subscription; jazz_protocol_SubscribeRejectReason reason; } subscribe_rejected;
        struct { jazz_protocol_SubscriptionKey subscription; } unsubscribe;
        struct { jazz_types_AuthorSubject author; jazz_protocol_SchemaVersion *schema; } publish_schema;
        struct { jazz_types_AuthorSubject author; u64 catalogue_seq; jazz_protocol_SchemaLineagePublication *publication; } publish_schema_with_lens;
        struct { jazz_types_AuthorSubject author; jazz_protocol_MigrationLens lens; } publish_lens;
        jazz_protocol_CatalogueAck catalogue_ack;
        jazz_protocol_ViewUpdatePayload view_update; /* ViewUpdate / ViewUpdatePart */
        jazz_protocol_CatalogueSnapshot *catalogue_snapshot;
        struct {
            jazz_protocol_PermissionAdviceRequestId request_id; jazz_protocol_PermissionAdviceAction action;
            bool has_delegated_session; jazz_protocol_DelegatedSessionBinding delegated_session;
        } auth_scope_intent;
        struct {
            jazz_protocol_PermissionAdviceRequestId request_id; jazz_protocol_AuthorizationSupportScopeKey key;
            u16 clause_index, clause_count; jazz_protocol_ViewUpdatePayload view;
        } auth_scope_view;
        struct { jazz_protocol_PermissionAdviceRequestId request_id; jazz_protocol_AuthorizationScopeReceipt receipt; } auth_scope_receipt;
        struct { jazz_protocol_PermissionAdviceRequestId request_id; } request_only; /* Unavailable / CurrentRowsCancel */
        struct { jazz_protocol_PermissionAdviceRequestId request_id; u32 advice; } auth_scope_decision;
        jazz_protocol_ChunkUploadStart chunk_upload_start;
        jazz_protocol_ChunkUploadNodes chunk_upload_nodes;
        jazz_protocol_ChunkUploadResult chunk_upload_result;
        jazz_protocol_CurrentRowsRequest current_rows_request;
        jazz_protocol_CurrentRowsReceipt current_rows_receipt;
        struct { u64 writes_through; } read_barrier;
    } u;
} jazz_protocol_SyncMessage;

/* ---------------- local events (types only; carried in-process) ---------------- */
enum { JAZZ_LE_MUTATE = 0, JAZZ_LE_OPEN_TX, JAZZ_LE_WRITE_IN_TX, JAZZ_LE_COMMIT_TX, JAZZ_LE_ABANDON_TX, JAZZ_LE_QUERY, JAZZ_LE_SUBSCRIBE, JAZZ_LE_UNSUBSCRIBE };
typedef struct jazz_protocol_LocalEvent { u32 tag; jz_str table; jazz_model_TxId tx; u64 query_ref; jazz_protocol_SubscriptionKey subscription; } jazz_protocol_LocalEvent;
typedef struct jazz_protocol_Event { u32 tag; jazz_protocol_SyncMessage sync; jazz_protocol_LocalEvent local; } jazz_protocol_Event;
enum { JAZZ_OUT_SYNC = 0, JAZZ_OUT_QUERY_RESULT, JAZZ_OUT_SUBSCRIPTION_NOTIFICATION, JAZZ_OUT_TX_FATE };
typedef struct jazz_protocol_OutboxMessage {
    u32 tag; jazz_protocol_SyncMessage sync; u64 query_ref; jazz_protocol_SubscriptionKey subscription;
    jazz_model_TxId tx_id; jazz_model_Fate fate;
} jazz_protocol_OutboxMessage;

/* ================= functions ================= */
/* SyncMessage */
jz_err jazz_protocol__protocol__SyncMessage__Serialize__serialize(jz_pc_w *w, const jazz_protocol_SyncMessage *m);
jz_err jazz_protocol__protocol__SyncMessage__Deserialize__deserialize(jz_pc_r *r, jazz_protocol_SyncMessage *out);
void jazz_protocol__protocol__SyncMessage__Clone__clone(jazz_protocol_SyncMessage *dst, const jazz_protocol_SyncMessage *src);
void jazz_protocol__protocol__SyncMessage__drop(jazz_protocol_SyncMessage *m);
void jazz_protocol__protocol__SyncMessage__drop_v(void *m);
u64 jazz_protocol__protocol__SyncMessage__required_wire_features(const jazz_protocol_SyncMessage *m);
jz_err jazz_protocol__protocol__SyncMessage__validate_version_carriers(const jazz_protocol_SyncMessage *m);
jz_err jazz_protocol__protocol__SyncMessage__validate_wire_contract(const jazz_protocol_SyncMessage *m);
const jazz_protocol_VersionRecord *jazz_protocol__protocol__SyncMessage__uploaded_versions(const jazz_protocol_SyncMessage *m, u32 *n);
extern const jazz_types_PcCodec jazz_protocol__protocol__SYNC_MESSAGE_CODEC;

/* VersionRecord */
jz_err jazz_protocol__protocol__VersionRecord__validate_receipt(const jazz_protocol_VersionRecord *v);
void jazz_protocol__protocol__VersionRecord__new(jazz_protocol_VersionRecord *out, jz_str table, jazz_types_SchemaVersionId sv, groove_OwnedRecord record);
jz_err jazz_protocol__protocol__VersionRecord__encode(const jazz_model_TableSchema *table, jazz_types_SchemaVersionId sv, jazz_types_RowUuid row_uuid,
    jazz_types_AuthorSubject created_by, u64 created_at_ms, jazz_types_AuthorSubject updated_by, u64 updated_at_ms,
    const groove_Value *const *cells_by_position, u32 n_cells, bool has_deletion, u32 deletion, jazz_protocol_VersionRecord *out);
jz_slice jazz_protocol__protocol__VersionRecord__table(const jazz_protocol_VersionRecord *v);
jazz_types_RowUuid jazz_protocol__protocol__VersionRecord__row_uuid(const jazz_protocol_VersionRecord *v);
bool jazz_protocol__protocol__VersionRecord__deletes_row(const jazz_protocol_VersionRecord *v);
bool jazz_protocol__protocol__VersionRecord__deletion(const jazz_protocol_VersionRecord *v, u32 *out);
jazz_types_AuthorSubject jazz_protocol__protocol__VersionRecord__created_by(const jazz_protocol_VersionRecord *v);
u64 jazz_protocol__protocol__VersionRecord__created_at_ms(const jazz_protocol_VersionRecord *v);
jazz_types_AuthorSubject jazz_protocol__protocol__VersionRecord__updated_by(const jazz_protocol_VersionRecord *v);
u64 jazz_protocol__protocol__VersionRecord__updated_at_ms(const jazz_protocol_VersionRecord *v);
bool jazz_protocol__protocol__VersionRecord__cell_at(const jazz_protocol_VersionRecord *v, u32 pos, groove_Value *out);
bool jazz_protocol__protocol__VersionRecord__optional_cell_at(const jazz_protocol_VersionRecord *v, u32 pos, groove_Value *out);
u32 jazz_protocol__protocol__VersionRecord__application_cell_count(const jazz_protocol_VersionRecord *v);
void jazz_protocol__protocol__VersionRecord__with_authored_columns(jazz_protocol_VersionRecord *v, bool some, jazz_protocol_StrSet cols);
void jazz_protocol__protocol__VersionRecord__Clone__clone(jazz_protocol_VersionRecord *dst, const jazz_protocol_VersionRecord *src);
void jazz_protocol__protocol__VersionRecord__drop(jazz_protocol_VersionRecord *v);
bool jazz_protocol__protocol__VersionRecord__PartialEq__eq(const jazz_protocol_VersionRecord *a, const jazz_protocol_VersionRecord *b);
int jazz_protocol__protocol__VersionRecord__Ord__cmp(const jazz_protocol_VersionRecord *a, const jazz_protocol_VersionRecord *b);
jz_err jazz_protocol__protocol__validate_version_records(const jazz_protocol_VersionRecord *v, u32 n);

/* bundles / carriers */
jz_err jazz_protocol__protocol__VersionBundleRun__validate(const jazz_protocol_VersionBundleRun *run);
jz_err jazz_protocol__protocol__VersionBundleRun__expand(const jazz_protocol_VersionBundleRun *run, jazz_protocol_VersionBundleVec *out);
jz_err jazz_protocol__protocol__VersionBundleRun__bundle_refs(const jazz_protocol_VersionBundleRun *run, jazz_protocol_VersionBundleRefVec *out);
jz_err jazz_protocol__protocol__VersionBundleRun__from_adjacent_singleton_bundles(jazz_protocol_VersionBundleVec bundles, jazz_protocol_VersionBundleRun *out);
jz_err jazz_protocol__protocol__VersionCarrier__expand(const jazz_protocol_VersionCarrier *c, jazz_protocol_VersionBundleVec *out);
jz_err jazz_protocol__protocol__VersionCarrier__bundle_refs(const jazz_protocol_VersionCarrier *c, jazz_protocol_VersionBundleRefVec *out);
jz_err jazz_protocol__protocol__build_version_carriers_from_singletons(jazz_protocol_VersionBundleVec bundles, jazz_protocol_VersionCarrierVec *out);
jz_err jazz_protocol__protocol__expand_version_carriers(const jazz_protocol_VersionCarrier *c, u32 n, jazz_protocol_VersionBundleVec *out);
void jazz_protocol__protocol__VersionBundle__Clone__clone(jazz_protocol_VersionBundle *dst, const jazz_protocol_VersionBundle *src);
void jazz_protocol__protocol__VersionBundle__drop(jazz_protocol_VersionBundle *b);
void jazz_protocol__protocol__VersionCarrier__Clone__clone(jazz_protocol_VersionCarrier *dst, const jazz_protocol_VersionCarrier *src);
void jazz_protocol__protocol__VersionCarrier__drop(jazz_protocol_VersionCarrier *c);

/* policy keys */
void jazz_protocol__protocol__CanonicalPolicyClaims__new(jazz_protocol_CanonicalPolicyClaims *out, jazz_protocol_ValueMap claims_moved);
bool jazz_protocol__protocol__CanonicalPolicyClaims__PartialEq__eq(const jazz_protocol_CanonicalPolicyClaims *a, const jazz_protocol_CanonicalPolicyClaims *b);
void jazz_protocol__protocol__PolicyBindingKey__from_delegated_session(jazz_protocol_PolicyBindingKey *out, const jazz_protocol_DelegatedSessionBinding *s);
void jazz_protocol__protocol__PolicyBindingKey__from_canonical_parts(jazz_protocol_PolicyBindingKey *out, jazz_types_AuthorSubject identity, jazz_protocol_ValueMap claims_moved);
jz_err jazz_protocol__protocol__PolicyBindingKey__directory_value(const jazz_protocol_PolicyBindingKey *k, groove_Value *out);
jz_err jazz_protocol__protocol__PolicyBindingKey__from_directory_value(jazz_types_AuthorSubject identity, groove_Value value, jazz_protocol_PolicyBindingKey *out);
void jazz_protocol__protocol__PolicyBindingKey__directory_digest(const jazz_protocol_PolicyBindingKey *k, u8 out[32]);
void jazz_protocol__protocol__PolicyBindingKey__Clone__clone(jazz_protocol_PolicyBindingKey *dst, const jazz_protocol_PolicyBindingKey *src);
void jazz_protocol__protocol__PolicyBindingKey__drop(jazz_protocol_PolicyBindingKey *k);
int jazz_protocol__protocol__PolicyBindingKey__Ord__cmp(const jazz_protocol_PolicyBindingKey *a, const jazz_protocol_PolicyBindingKey *b);
void jazz_protocol__protocol__AuthorityResultKey__Clone__clone(jazz_protocol_AuthorityResultKey *dst, const jazz_protocol_AuthorityResultKey *src);
void jazz_protocol__protocol__AuthorityResultKey__Debug__fmt(jz_str *out, const jazz_protocol_AuthorityResultKey *k);
void jazz_protocol__protocol__BindingViewKey__Debug__fmt(jz_str *out, const jazz_protocol_BindingViewKey *k);
void jazz_protocol__protocol__SubscriptionKey__Debug__fmt(jz_str *out, const jazz_protocol_SubscriptionKey *k);
void jazz_protocol__protocol__ReadViewKey__Debug__fmt(jz_str *out, const jazz_protocol_ReadViewKey *k);

/* shapes */
void jazz_protocol__protocol__ShapeAst__Serialize__serialize(jz_pc_w *w, const jazz_protocol_ShapeAst *a);
void jazz_protocol__protocol__ShapeAst__from_validated(jazz_protocol_ShapeAst *out, const jazz_model_ValidatedQuery *q);
void jazz_protocol__protocol__RegisterShapeOptions__Serialize__serialize(jz_pc_w *w, const jazz_protocol_RegisterShapeOptions *o);
void jazz_protocol__protocol__RegisterShapeOptions__Clone__clone(jazz_protocol_RegisterShapeOptions *dst, const jazz_protocol_RegisterShapeOptions *src);
void jazz_protocol__protocol__RegisterShapeOptions__drop(jazz_protocol_RegisterShapeOptions *o);
void jazz_protocol__protocol__RegisterShapeOptions__default(jazz_protocol_RegisterShapeOptions *out);
jazz_protocol_ReadViewKey jazz_protocol__protocol__RegisterShapeOptions__read_view_key(const jazz_protocol_RegisterShapeOptions *o);
bool jazz_protocol__protocol__ReadViewSpec__is_default(const jazz_protocol_ReadViewSpec *s);
void jazz_protocol__protocol__CoverageKey__Clone__clone(jazz_protocol_CoverageKey *dst, const jazz_protocol_CoverageKey *src);
int jazz_protocol__protocol__CoverageKey__Ord__cmp(const jazz_protocol_CoverageKey *a, const jazz_protocol_CoverageKey *b);
bool jazz_protocol__protocol__CoverageKey__PartialEq__eq(const jazz_protocol_CoverageKey *a, const jazz_protocol_CoverageKey *b);
void jazz_protocol__protocol__CoverageKey__drop(jazz_protocol_CoverageKey *k);
void jazz_protocol__protocol__ShapeAst__drop(jazz_protocol_ShapeAst *a);

/* results / facts */
void jazz_protocol__protocol__RowVersionRefEntry__Clone__clone(jazz_protocol_RowVersionRefEntry *dst, const jazz_protocol_RowVersionRefEntry *src);
int jazz_protocol__protocol__RowVersionRefEntry__Ord__cmp(const jazz_protocol_RowVersionRefEntry *a, const jazz_protocol_RowVersionRefEntry *b);
void jazz_protocol__protocol__RowVersionRefEntry__Debug__fmt(jz_str *out, const jazz_protocol_RowVersionRefEntry *e);
void jazz_protocol__protocol__RowVersionRefEntry__drop(jazz_protocol_RowVersionRefEntry *e);
void jazz_protocol__protocol__ResultRowSource__Clone__clone(jazz_protocol_ResultRowSource *dst, const jazz_protocol_ResultRowSource *src);
int jazz_protocol__protocol__ResultRowSource__Ord__cmp(const jazz_protocol_ResultRowSource *a, const jazz_protocol_ResultRowSource *b);
bool jazz_protocol__protocol__ResultRowSource__PartialEq__eq(const jazz_protocol_ResultRowSource *a, const jazz_protocol_ResultRowSource *b);
void jazz_protocol__protocol__ResultRowSource__Debug__fmt(jz_str *out, const jazz_protocol_ResultRowSource *s);
void jazz_protocol__protocol__ResultRowSource__drop(jazz_protocol_ResultRowSource *s);
void jazz_protocol__protocol__RealRowMemberEntry__Clone__clone(jazz_protocol_RealRowMemberEntry *dst, const jazz_protocol_RealRowMemberEntry *src);
int jazz_protocol__protocol__RealRowMemberEntry__Ord__cmp(const jazz_protocol_RealRowMemberEntry *a, const jazz_protocol_RealRowMemberEntry *b);
bool jazz_protocol__protocol__RealRowMemberEntry__PartialEq__eq(const jazz_protocol_RealRowMemberEntry *a, const jazz_protocol_RealRowMemberEntry *b);
void jazz_protocol__protocol__RealRowMemberEntry__Debug__fmt(jz_str *out, const jazz_protocol_RealRowMemberEntry *e);
void jazz_protocol__protocol__RealRowMemberEntry__drop(jazz_protocol_RealRowMemberEntry *e);
void jazz_protocol__protocol__RealRowMemberEntry__current_content(jazz_protocol_RealRowMemberEntry *out, jazz_protocol_ResultRowEntry row);
void jazz_protocol__protocol__RealRowMemberEntry__output_occurrence_id(const jazz_protocol_RealRowMemberEntry *e, jazz_types_OutputOccurrenceId *out);
void jazz_protocol__protocol__ResultMemberEntry__Clone__clone(jazz_protocol_ResultMemberEntry *dst, const jazz_protocol_ResultMemberEntry *src);
int jazz_protocol__protocol__ResultMemberEntry__Ord__cmp(const jazz_protocol_ResultMemberEntry *a, const jazz_protocol_ResultMemberEntry *b);
bool jazz_protocol__protocol__ResultMemberEntry__PartialEq__eq(const jazz_protocol_ResultMemberEntry *a, const jazz_protocol_ResultMemberEntry *b);
void jazz_protocol__protocol__ResultMemberEntry__Debug__fmt(jz_str *out, const jazz_protocol_ResultMemberEntry *e);
void jazz_protocol__protocol__ResultMemberEntry__drop(jazz_protocol_ResultMemberEntry *e);
bool jazz_protocol__protocol__ResultMemberEntry__as_row(const jazz_protocol_ResultMemberEntry *e, jazz_protocol_ResultRowEntry *out);
bool jazz_protocol__protocol__ResultMemberEntry__output_occurrence_id(const jazz_protocol_ResultMemberEntry *e, jazz_types_OutputOccurrenceId *out);
void jazz_protocol__protocol__ResultMemberEntry__From_RealRowMemberEntry__from(jazz_protocol_ResultMemberEntry *out, jazz_protocol_RealRowMemberEntry entry);
void jazz_protocol__protocol__ProgramFactEntry__Clone__clone(jazz_protocol_ProgramFactEntry *dst, const jazz_protocol_ProgramFactEntry *src);
int jazz_protocol__protocol__ProgramFactEntry__Ord__cmp(const jazz_protocol_ProgramFactEntry *a, const jazz_protocol_ProgramFactEntry *b);
void jazz_protocol__protocol__ProgramFactEntry__drop(jazz_protocol_ProgramFactEntry *e);
bool jazz_protocol__protocol__ProgramFactEntry__is_peer_source_closure_fact(const jazz_protocol_ProgramFactEntry *e);
void jazz_protocol__protocol__ProgramSourceId__Debug__fmt(jz_str *out, const jazz_protocol_ProgramSourceId *s);
bool jazz_protocol__protocol__ProgramSourceId__is_wire_valid(const jazz_protocol_ProgramSourceId *s);
bool jazz_protocol__protocol__CoveredInputEntry__is_wire_valid(const jazz_protocol_CoveredInputEntry *e);
void jazz_protocol__protocol__ResultMemberPayloadEntry__Debug__fmt(jz_str *out, const jazz_protocol_ResultMemberPayloadEntry *e);
void jazz_protocol__protocol__PolicyDecisionOutcomeEntry__Debug__fmt(jz_str *out, const jazz_protocol_PolicyDecisionOutcomeEntry *e);
void jazz_protocol__protocol__PredicateOutputSetRoleEntry__Debug__fmt(jz_str *out, u32 role);
void jazz_protocol__protocol__ResultRowLayer__Debug__fmt(jz_str *out, u32 layer);
void jazz_protocol__protocol__SubscribeRejectReason__Debug__fmt(jz_str *out, const jazz_protocol_SubscribeRejectReason *r);
void jazz_protocol__protocol__SubscribeServerFailureCode__Debug__fmt(jz_str *out, u32 code);
void jazz_protocol__protocol__KnownStateCompleteness__Debug__fmt(jz_str *out, u32 c);
bool jazz_protocol__protocol__KnownStateDeclaration__PartialEq__eq(const jazz_protocol_KnownStateDeclaration *a, const jazz_protocol_KnownStateDeclaration *b);
bool jazz_protocol__protocol__SupportingRow__PartialEq__eq(const jazz_protocol_SupportingRow *a, const jazz_protocol_SupportingRow *b);
int jazz_protocol__protocol__SupportingRow__Ord__cmp(const jazz_protocol_SupportingRow *a, const jazz_protocol_SupportingRow *b);
bool jazz_protocol__protocol__SupportingRow__is_wire_valid(const jazz_protocol_SupportingRow *r);
void jazz_protocol__protocol__SupportingRow__Serialize__serialize(jz_pc_w *w, const jazz_protocol_SupportingRow *r);
void jazz_protocol__protocol__SupportingRowsUpdate__snapshot(jazz_protocol_SupportingRowsUpdate *out, jazz_protocol_SupportingRowVec rows);
void jazz_protocol__protocol__ViewUpdatePayload__Clone__clone(jazz_protocol_ViewUpdatePayload *dst, const jazz_protocol_ViewUpdatePayload *src);
bool jazz_protocol__protocol__ViewUpdatePayload__PartialEq__eq(const jazz_protocol_ViewUpdatePayload *a, const jazz_protocol_ViewUpdatePayload *b);
jz_err jazz_protocol__protocol__ViewUpdatePayload__Serialize__serialize(jz_pc_w *w, const jazz_protocol_ViewUpdatePayload *v);
void jazz_protocol__protocol__ViewUpdatePayload__drop(jazz_protocol_ViewUpdatePayload *v);
void jazz_protocol__protocol__PermissionAdviceAction__Clone__clone(jazz_protocol_PermissionAdviceAction *dst, const jazz_protocol_PermissionAdviceAction *src);
void jazz_protocol__protocol__AuthorizationSupportScopeKey__Serialize__serialize(jz_pc_w *w, const jazz_protocol_AuthorizationSupportScopeKey *k);
void jazz_protocol__protocol__SubscriptionKey__Serialize__serialize(jz_pc_w *w, const jazz_protocol_SubscriptionKey *k);
jz_err jazz_protocol__protocol__VersionBundle__Serialize__serialize(jz_pc_w *w, const jazz_protocol_VersionBundle *b);

/* schemas / lineage / lenses */
void jazz_protocol__protocol__SchemaVersion__new(jazz_protocol_SchemaVersion *out, jazz_model_JazzSchema schema);
jz_err jazz_protocol__protocol__durable_public_schema_json(const jazz_model_JazzSchema *schema, jz_bytes *out);
jz_err jazz_protocol__protocol__canonical_catalogue_schema_bytes(const jazz_protocol_SchemaVersion *s, jz_bytes *out);
jz_err jazz_protocol__protocol__SchemaVersion__Serialize__serialize(jz_pc_w *w, const jazz_protocol_SchemaVersion *s);
jz_err jazz_protocol__protocol__SchemaVersion__Deserialize__deserialize(jz_pc_r *r, jazz_protocol_SchemaVersion *out);
void jazz_protocol__protocol__SchemaVersion__drop(jazz_protocol_SchemaVersion *s);
void jazz_protocol__protocol__PhysicalIdentityManifest__allocate(jazz_protocol_PhysicalIdentityManifest *out, const jazz_model_JazzSchema *schema);
jz_err jazz_protocol__protocol__PhysicalIdentityManifest__validate_for_schema(const jazz_protocol_PhysicalIdentityManifest *m, const jazz_model_JazzSchema *schema);
void jazz_protocol__protocol__PhysicalIdentityManifest__drop(jazz_protocol_PhysicalIdentityManifest *m);
void jazz_protocol__protocol__PhysicalIdentityManifest__Clone__clone(jazz_protocol_PhysicalIdentityManifest *dst, const jazz_protocol_PhysicalIdentityManifest *src);
bool jazz_protocol__protocol__PhysicalIdentityManifest__PartialEq__eq(const jazz_protocol_PhysicalIdentityManifest *a, const jazz_protocol_PhysicalIdentityManifest *b);
jazz_types_SchemaLineagePublicationId jazz_protocol__protocol__SchemaLineagePublication__content_id(const jazz_protocol_SchemaLineagePublication *p);
/* sources: jz_btree SchemaVersionId -> LineageSource; history: array of manifests */
jz_err jazz_protocol__protocol__SchemaLineagePublication__validate_predecessor_identities(const jazz_protocol_SchemaLineagePublication *p,
    const jz_btree *sources, const jazz_protocol_PhysicalIdentityManifest *history, u32 n_history);
bool jazz_protocol__protocol__SchemaLineagePublication__PartialEq__eq(const jazz_protocol_SchemaLineagePublication *a, const jazz_protocol_SchemaLineagePublication *b);
void jazz_protocol__protocol__SchemaLineagePublication__Clone__clone(jazz_protocol_SchemaLineagePublication *dst, const jazz_protocol_SchemaLineagePublication *src);
void jazz_protocol__protocol__SchemaLineagePublication__drop(jazz_protocol_SchemaLineagePublication *p);
jz_err jazz_protocol__protocol__SchemaLineagePublication__Serialize__serialize(jz_pc_w *w, const jazz_protocol_SchemaLineagePublication *p);
jz_err jazz_protocol__protocol__SchemaLineagePublication__Deserialize__deserialize(jz_pc_r *r, jazz_protocol_SchemaLineagePublication *out);
bool jazz_protocol__protocol__SchemaPredecessor__PartialEq__eq(const jazz_protocol_SchemaPredecessor *a, const jazz_protocol_SchemaPredecessor *b);
jz_err jazz_protocol__protocol__MigrationLens__new(jazz_types_SchemaVersionId source, jazz_types_SchemaVersionId target, jazz_protocol_TableLensVec table_lenses, jazz_protocol_MigrationLens *out);
jazz_types_MigrationLensId jazz_protocol__protocol__MigrationLens__content_id(const jazz_protocol_MigrationLens *l);
void jazz_protocol__protocol__MigrationLens__Serialize__serialize(jz_pc_w *w, const jazz_protocol_MigrationLens *l);
jz_err jazz_protocol__protocol__MigrationLens__Deserialize__deserialize(jz_pc_r *r, jazz_protocol_MigrationLens *out);
void jazz_protocol__protocol__MigrationLens__drop(jazz_protocol_MigrationLens *l);
void jazz_protocol__protocol__MigrationLens__Clone__clone(jazz_protocol_MigrationLens *dst, const jazz_protocol_MigrationLens *src);
bool jazz_protocol__protocol__MigrationLens__PartialEq__eq(const jazz_protocol_MigrationLens *a, const jazz_protocol_MigrationLens *b);
jz_bytes jazz_protocol__protocol__canonical_lens_bytes(const jazz_protocol_MigrationLens *l);
jz_err jazz_protocol__protocol__decode_canonical_lens_bytes(jz_slice bytes, jazz_protocol_MigrationLens *out);
void jazz_protocol__protocol__put_value(jz_bytes *bytes, const groove_Value *v);
void jazz_protocol__protocol__put_branch_selector(jz_bytes *bytes, const jazz_model_BranchSelector *s);
void jazz_protocol__protocol__put_snapshot_ref(jz_bytes *bytes, const jazz_model_SnapshotRef *s);

/* ---------------- protocol_limits ---------------- */
#define JAZZ_MAX_WIRE_FRAME_BYTES (2u * 1024u * 1024u)
#define JAZZ_MAX_COMMIT_UNIT_VERSIONS 4096u
#define JAZZ_MAX_WIRE_BATCH_FRAMES JAZZ_MAX_COMMIT_UNIT_VERSIONS
#define JAZZ_MAX_LOGICAL_MESSAGE_BYTES (256u * 1024u * 1024u)
#define JAZZ_MAX_ENCODED_MESSAGE_BYTES (JAZZ_MAX_LOGICAL_MESSAGE_BYTES + JAZZ_MAX_LOGICAL_MESSAGE_BYTES / 10u + 24u)
#define JAZZ_MAX_FRAGMENT_REASSEMBLY_IDLE_MS 30000ull
#define JAZZ_MAX_FRAGMENT_REASSEMBLY_AGE_MS (5ull * 60ull * 1000ull)
#define JAZZ_MAX_SHAPE_AST_BYTES (64u * 1024u)
#define JAZZ_MAX_SHAPE_REGISTRATION_BYTES JAZZ_MAX_SHAPE_AST_BYTES
#define JAZZ_MAX_SHAPE_REGISTRATIONS_PER_PEER 1024u
#define JAZZ_MAX_RETAINED_PEER_SHAPES 1024u
#define JAZZ_MAX_KNOWN_STATE_EXACT_REFS 1024u
#define JAZZ_MAX_CHUNK_REQUEST_BATCH_ENTRIES 4u
jz_err jazz_protocol__protocol_limits__validate_wire_frame_len(usize len);
jz_err jazz_protocol__protocol_limits__validate_logical_message_len(usize len);
jz_err jazz_protocol__protocol_limits__validate_shape_ast_size(const jazz_protocol_ShapeAst *ast);
jz_err jazz_protocol__protocol_limits__validate_shape_registration_size(const jazz_protocol_ShapeAst *ast, const jazz_protocol_RegisterShapeOptions *opts);
bool jazz_protocol__protocol_limits__commit_unit_limit_violation(u32 n_versions, jz_str *out);

/* ---------------- storage_codec_profile ---------------- */
jz_err jazz_protocol__storage_codec_profile__node_storage_codec_profile(groove_StorageCodecProfile *out);
jz_err jazz_protocol__storage_codec_profile__catalogue_storage_codec_profile(groove_StorageCodecProfile *out);
jz_err jazz_protocol__storage_codec_profile__account_storage_codec_profile(groove_StorageCodecProfile *out);

#endif
