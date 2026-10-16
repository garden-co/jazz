/* maintained_subscription_view.h: jazz_node::node::maintained_subscription_view (node-core-a unit).
 * Rust: crates/jazz/layers/node/src/node/maintained_subscription_view.rs */
#ifndef JZ_NODE_MAINTAINED_SUBSCRIPTION_VIEW_H
#define JZ_NODE_MAINTAINED_SUBSCRIPTION_VIEW_H
#include "jz_rt.h"
#include "../_ext_node-core-a.h"
#include "codec.h"

#define MSV(x) jazz_node__node__maintained_subscription_view__##x

/* type TableSchemas = BTreeMap<String, TableSchema> (jz_str -> jazz_model_TableSchema) */
typedef jz_btree jzn_TableSchemas;

/* WitnessLayer */
enum { JZN_WL_CONTENT = 0, JZN_WL_DELETION = 1 };

/* Arc<[u8]>: refcounted (jz_rc) jz_bytes */
typedef jz_bytes jzn_ArcBytes;

typedef struct jzn_VersionIdentity { groove_InternStr table; u32 layer; jzn_ArcBytes *raw_record; } jzn_VersionIdentity;
typedef struct jzn_VersionSortKey { groove_InternStr table; jazz_types_RowUuid row_uuid; u32 layer; jzn_ArcBytes *raw_record; } jzn_VersionSortKey;
typedef struct jzn_ReplacementKey { groove_InternStr table; jazz_types_RowUuid row_uuid; u32 layer; } jzn_ReplacementKey;

/* Arc<VersionPayload> (jz_rc payload) */
typedef struct jzn_VersionPayload { jazz_node_VersionRow row; jazz_types_TxId tx_id; jzn_VersionSortKey sort_key; } jzn_VersionPayload;
typedef struct jzn_WeightedVersion { jzn_VersionPayload *payload; i64 weight; } jzn_WeightedVersion;

typedef struct jzn_VersionDecodePlan {
    groove_RecordDescriptor descriptor;
    bool has_branch_idx; u32 branch_idx;
    u32 row_idx, tx_time_idx, tx_node_idx, schema_version_idx;
    u32 created_by_idx, created_at_idx, updated_by_idx, updated_at_idx;
    jz_btree user_indices; /* String -> u32 */
    u32 authored_columns_idx;
} jzn_VersionDecodePlan;

typedef struct jzn_WeightedVersionIndex {
    jz_btree by_tx; /* TxId -> jz_btree(VersionSortKey -> WeightedVersion) */
    usize entry_count, entry_bytes;
} jzn_WeightedVersionIndex;

typedef struct jzn_ReplacementIndex {
    jz_btree content_by_key;  /* ReplacementKey -> jz_btree(VersionIdentity -> WeightedVersion) */
    jz_btree deletion_by_key;
    usize entry_count, entry_bytes, key_bytes;
} jzn_ReplacementIndex;

/* RetainedResultMap<V>: V is i64 (result weights) or ResultMemberPayloadEntry */
typedef struct jzn_RetainedResultMap { jz_btree entries; usize entry_bytes, positive_count; bool payload_values; } jzn_RetainedResultMap;
typedef struct jzn_RetainedResultMembers { jz_btree entries; usize entry_bytes; } jzn_RetainedResultMembers;
typedef struct jzn_WitnessGatedJournal { jz_btree changed, withheld; u64 alias_retargets; usize alias_count; } jzn_WitnessGatedJournal;

typedef struct jazz_node_MaintainedSubscriptionView {
    /* leading fields mirrored by other units (qe-b) */
    bool targeted_refresh_uncertain;
    jz_btree targeted_refresh_tables;     /* BTreeSet<String> */
    /* ClientLocalLiteralToken { _held: Option<Arc<()>> } */
    void *client_local_literal_token;
    jazz_protocol_ReadViewKey read_view;
    jz_btree witness_table_names;         /* (String, SchemaVersionAlias) -> String */
    jzn_RetainedResultMap result_weights;
    jzn_RetainedResultMembers published_result_members;
    bool has_unreconciled_result_members; jzn_RetainedResultMembers unreconciled_result_members;
    bool has_witness_gated_journal; jzn_WitnessGatedJournal witness_gated_journal;
    jzn_RetainedResultMap result_payloads;
    jzn_RetainedResultMap published_result_payloads;
    jz_btree structured_app_rows;         /* Vec<u8> -> jz_btree(Vec<u8> -> i64) */
    jz_btree structured_terminal_records; /* Vec<u8> -> Box<TerminalRecordState> */
    jz_btree structured_root_keys;        /* Vec<u8> -> RowUuid */
    JZ_VEC(jz_bytes) structured_root_key_order;
    bool has_structured_app_row_descriptor; groove_RecordDescriptor structured_app_row_descriptor;
    bool retains_structured_app_rows;
    bool storage_backed_result_materialization;
    jz_btree inline_content_branch_keys;  /* BTreeSet<Vec<u8>> */
    jz_btree program_fact_weights;        /* ProgramFactEntry -> i64 */
    jazz_node_SupportingFrontier supporting;
    jz_btree physical_tables;             /* Intern<String> -> GlobalPhysicalTableId */
    jz_btree selected_deletion_witnesses; /* SupportingRow -> VersionRow */
    jzn_WeightedVersionIndex versions;
    jzn_ReplacementIndex replacements;
} jazz_node_MaintainedSubscriptionView;

typedef struct jazz_node_MaintainedSubscriptionViewFootprint {
    usize result_rows, result_weights, result_payloads, version_identities, version_tx_entries, replacement_entries,
        structured_app_rows, result_weights_bytes, result_payloads_bytes, versions_bytes, supporting_frontier_bytes,
        replacements_bytes, structured_app_rows_bytes, total_heap_bytes;
} jazz_node_MaintainedSubscriptionViewFootprint;

typedef struct jazz_node_ResultTransitions {
    bool authoritative_membership_changed;
    jz_btree authoritative_member_adds;     /* BTreeSet<ResultMemberEntry> */
    jazz_protocol_ResultMemberEntryVec adds, removes;
    jazz_protocol_MemberPayloadPairVec result_payload_adds;
    jazz_protocol_ResultMemberEntryVec result_payload_removes;
    jazz_protocol_ProgramFactEntryVec program_fact_adds, program_fact_removes;
    bool supporting_changed;
    groove_TerminalOperationVec terminal_operations;
    usize observed_result_delta_batches;
    bool requires_authoritative_membership_reconcile;
} jazz_node_ResultTransitions;
void jazz_node_ResultTransitions_default(jazz_node_ResultTransitions *t);
void jazz_node_ResultTransitions_drop(void *t);

/* DecodedMaintainedEvent */
enum {
    JZN_DME_RESULT_CURRENT = 0, JZN_DME_AGGREGATE_RESULT, JZN_DME_VERSION_CONTENT, JZN_DME_VERSION_DELETION,
    JZN_DME_REPLACEMENT_CONTENT, JZN_DME_REPLACEMENT_DELETION, JZN_DME_SHARED_VERSION, JZN_DME_PROGRAM_SOURCE_COVERAGE,
    JZN_DME_RELATION_EDGE, JZN_DME_STRUCTURED_APP_ROW
};
typedef struct jazz_node_DecodedMaintainedEvent {
    u32 tag;
    jazz_protocol_ResultMemberEntry member;           /* ResultCurrent / AggregateResult */
    jazz_protocol_ResultMemberPayloadEntry payload;
    jazz_engine_SyntheticResultMembershipSchema synthetic;
    jazz_engine_StrVec value_fields;
    jazz_protocol_ProgramSourceId source;             /* version / replacement / shared */
    jazz_node_VersionRow row;
    jazz_protocol_ProgramSourceCoverageEntry coverage;
    jazz_protocol_RelationEdgeEntry edge;
    jazz_types_RowUuid root;                          /* StructuredAppRow */
    groove_OwnedRecord record;
} jazz_node_DecodedMaintainedEvent;

/* MaintainedTerminalKind */
enum {
    JZN_MTK_RESULT_CURRENT = 0, JZN_MTK_AGGREGATE_RESULT, JZN_MTK_VERSION_CONTENT, JZN_MTK_VERSION_DELETION,
    JZN_MTK_REPLACEMENT_CONTENT, JZN_MTK_REPLACEMENT_DELETION, JZN_MTK_SHARED_CONTENT, JZN_MTK_SHARED_DELETION,
    JZN_MTK_PROGRAM_SOURCE_COVERAGE, JZN_MTK_RELATION_EDGE, JZN_MTK_ROOT_COLLECTOR_APP_ROWS, JZN_MTK_DIRECT_APP_ROWS,
    JZN_MTK_AGGREGATE_APP_ROWS
};
typedef struct jzn_MaintainedTerminalKind {
    u32 tag;
    union {
        jazz_engine_ResultMembershipSchema result;
        jazz_engine_AggregateResultSchema aggregate;
        jazz_engine_VersionWitnessSchema witness;
        jazz_engine_ProgramSourceCoverageSchema coverage;
        jazz_engine_RelationEdgeSchema relation;
        struct { jazz_engine_AppRowSchema schema; jazz_node_TerminalRootLayout layout; } root_collector;
        jazz_engine_AppRowSchema app_rows;   /* Direct / Aggregate */
    } u;
} jzn_MaintainedTerminalKind;

typedef struct jazz_node_MaintainedTerminalSchemas { jz_btree sinks; /* String -> MaintainedTerminalKind */ } jazz_node_MaintainedTerminalSchemas;

typedef struct jazz_node_TerminalKeyRecord { jz_bytes terminal_key; groove_OwnedRecord record; } jazz_node_TerminalKeyRecord;
typedef JZ_VEC(jazz_node_TerminalKeyRecord) jazz_node_TerminalKeyRecordVec;
typedef JZ_VEC(jazz_protocol_SupportingRow) jzn_SupportingRowVec;

/* ---- public API ---- */
void jazz_node_MaintainedSubscriptionView_default(jazz_node_MaintainedSubscriptionView *m);
void jazz_node_MaintainedSubscriptionView_clone(jazz_node_MaintainedSubscriptionView *dst, const jazz_node_MaintainedSubscriptionView *src);
void jazz_node_MaintainedSubscriptionView_drop(jazz_node_MaintainedSubscriptionView *m);
void MSV(MaintainedSubscriptionView__hold_client_local_literal_token)(jazz_node_MaintainedSubscriptionView *m, void *token /* Arc<()> */);
void MSV(MaintainedSubscriptionView__set_read_view)(jazz_node_MaintainedSubscriptionView *m, jazz_protocol_ReadViewKey read_view);
void MSV(MaintainedSubscriptionView__set_witness_table_names)(jazz_node_MaintainedSubscriptionView *m, jz_btree names);
bool MSV(MaintainedSubscriptionView__uses_storage_backed_result_materialization)(const jazz_node_MaintainedSubscriptionView *m);
void MSV(MaintainedSubscriptionView__enable_storage_backed_result_materialization)(jazz_node_MaintainedSubscriptionView *m);
void MSV(MaintainedSubscriptionView__enable_inline_content_branch_key)(jazz_node_MaintainedSubscriptionView *m, const jazz_model_BranchKey *branch_key);
void MSV(MaintainedSubscriptionView__terminal_schemas_for_program)(const jazz_engine_QueryProgram *program, jazz_node_MaintainedTerminalSchemas *out);
jz_err MSV(MaintainedSubscriptionView__apply_typed_deltas)(jazz_node_MaintainedSubscriptionView *m, const jz_str *sink, const groove_RecordDeltas *deltas,
    const jazz_node_MaintainedTerminalSchemas *schemas, const jzn_TableSchemas *tables, const jazz_node_NodeAliases *node_aliases,
    jazz_node_ResultTransitions *out);
jz_err MSV(MaintainedSubscriptionView__apply_multisink_deltas)(jazz_node_MaintainedSubscriptionView *m, groove_MultisinkBatch deltas,
    const jazz_node_MaintainedTerminalSchemas *schemas, const jzn_TableSchemas *tables, const jazz_node_NodeAliases *node_aliases,
    jazz_node_ResultTransitions *out);
jazz_node_VersionRowVec MSV(MaintainedSubscriptionView__versions_by_tx)(const jazz_node_MaintainedSubscriptionView *m, jazz_types_TxId tx_id);
void MSV(MaintainedSubscriptionView__acknowledged_supporting_rows)(const jazz_node_MaintainedSubscriptionView *m, jzn_SupportingRowRefVec *out);
bool MSV(MaintainedSubscriptionView__single_physical_table)(const jazz_node_MaintainedSubscriptionView *m, const jz_str **table, jazz_types_GlobalPhysicalTableId *id);
void MSV(MaintainedSubscriptionView__supporting_rows)(const jazz_node_MaintainedSubscriptionView *m, jzn_SupportingRowRefVec *out);
bool MSV(MaintainedSubscriptionView__unpublished_supporting_delta)(const jazz_node_MaintainedSubscriptionView *m, jazz_protocol_SupportingRowVec *adds,
    jazz_protocol_SupportingRowVec *removes);
void MSV(MaintainedSubscriptionView__forget_peer_source_closure_baseline)(jazz_node_MaintainedSubscriptionView *m);
void MSV(MaintainedSubscriptionView__acknowledge_peer_source_closure)(jazz_node_MaintainedSubscriptionView *m);
bool MSV(MaintainedSubscriptionView__replace_selected_deletion_witnesses)(jazz_node_MaintainedSubscriptionView *m, jz_btree witnesses);
bool MSV(MaintainedSubscriptionView__replacement_for)(const jazz_node_MaintainedSubscriptionView *m, const jz_str *table, jazz_types_RowUuid row_uuid,
    jazz_node_VersionRow *content, bool *has_deletion, jazz_node_VersionRow *deletion /* NULL ok */);
void MSV(MaintainedSubscriptionView__footprint)(const jazz_node_MaintainedSubscriptionView *m, jazz_node_MaintainedSubscriptionViewFootprint *out);
void MSV(MaintainedSubscriptionView__active_result_members)(const jazz_node_MaintainedSubscriptionView *m, jazz_protocol_ResultMemberEntryVec *out);
const jz_btree *MSV(MaintainedSubscriptionView__published_result_members)(const jazz_node_MaintainedSubscriptionView *m);
jz_err MSV(MaintainedSubscriptionView__structured_app_rows_by_terminal_key)(const jazz_node_MaintainedSubscriptionView *m, jazz_node_TerminalKeyRecordVec *out);
const jz_btree *MSV(MaintainedSubscriptionView__decoded_terminal_records)(const jazz_node_MaintainedSubscriptionView *m);
void MSV(MaintainedSubscriptionView__discard_structured_app_rows)(jazz_node_MaintainedSubscriptionView *m);

const jazz_node_TerminalRootLayout *jazz_node__node__MaintainedTerminalSchemas__terminal_root_layout(const jazz_node_MaintainedTerminalSchemas *s);
bool jazz_node__node__MaintainedTerminalSchemas__has_root_collector(const jazz_node_MaintainedTerminalSchemas *s);
jz_err jazz_node__node__MaintainedTerminalSchemas__current_payload_schema(const jazz_node_MaintainedTerminalSchemas *s, const jazz_engine_ResultMembershipSchema **out);
jz_err jazz_node__node__MaintainedTerminalSchemas__direct_app_row_schema(const jazz_node_MaintainedTerminalSchemas *s, const jazz_engine_AppRowSchema **out);
jz_err jazz_node__node__MaintainedTerminalSchemas__aggregate_app_row_schema(const jazz_node_MaintainedTerminalSchemas *s, const jazz_engine_AppRowSchema **out);
void jazz_node_MaintainedTerminalSchemas_drop(jazz_node_MaintainedTerminalSchemas *s);
void jazz_node_MaintainedTerminalSchemas_clone(jazz_node_MaintainedTerminalSchemas *dst, const jazz_node_MaintainedTerminalSchemas *src);

#endif
