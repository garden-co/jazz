/* schemas.h: typed output schemas (query_engine/schemas.rs). Included from mod.h. */
#ifndef JAZZ_ENGINE_QUERY_ENGINE_SCHEMAS_H
#define JAZZ_ENGINE_QUERY_ENGINE_SCHEMAS_H

typedef struct jazz_engine_OptStr { bool some; jz_str s; } jazz_engine_OptStr;

enum { JAZZ_ENGINE_CARRIER_CURRENT_ROW, JAZZ_ENGINE_CARRIER_LOGICAL };
typedef struct jazz_engine_SyntheticResultMembershipSchema {
    jz_str table_field, row_field, replacement_field;
    jz_btree routing_param_fields;
} jazz_engine_SyntheticResultMembershipSchema;
typedef struct jazz_engine_AggregateResultSchema {
    jazz_engine_SyntheticResultMembershipSchema synthetic;
    groove_DescriptorFieldVec group_key_fields;
    jazz_engine_StrVec group_names;
    groove_DescriptorFieldVec value_fields;
    jazz_engine_StrVec value_names;
    jz_btree routing_param_fields;
} jazz_engine_AggregateResultSchema;
enum { JAZZ_ENGINE_ART_DIRECT, JAZZ_ENGINE_ART_ROOT_COLLECTOR, JAZZ_ENGINE_ART_AGGREGATE };
typedef struct jazz_engine_AppRowTerminal { u8 tag; jazz_engine_AggregateResultSchema aggregate; } jazz_engine_AppRowTerminal;
typedef struct jazz_engine_AppRowSchema {
    jz_btree publication_fields; /* String -> CurrentRowPublicationField */
    groove_RecordDescriptor descriptor;
    jz_btree hidden_fields;      /* BTreeSet<String> */
    u8 carrier;
    jz_btree field_carriers;     /* String -> AppRowCarrier(u8) */
    jz_btree public_field_names; /* String -> String */
    jazz_engine_AppRowTerminal terminal;
    bool root_union_arm;
} jazz_engine_AppRowSchema;

typedef struct jazz_engine_ContentVersionFields { jz_str tx_time_field, tx_node_field; } jazz_engine_ContentVersionFields;
typedef struct jazz_engine_VersionIdentityFields {
    jz_str table_field, row_field, tx_time_field, tx_node_field;
    jazz_engine_OptStr batch_id_field, branch_or_prefix_field, row_digest_field;
    jz_str schema_field, layer_field;
} jazz_engine_VersionIdentityFields;
enum { JAZZ_ENGINE_RMV_CONTENT, JAZZ_ENGINE_RMV_CONTENT_OR_DELETION };
typedef struct jazz_engine_ResultMembershipVersionSchema {
    u8 tag;
    jazz_engine_ContentVersionFields content;
    jazz_engine_VersionIdentityFields deletion;
    jz_str deletion_state_field;
} jazz_engine_ResultMembershipVersionSchema;
typedef struct jazz_engine_RowRefSchema { jz_str source_field, table_field, row_field; } jazz_engine_RowRefSchema;
typedef struct jazz_engine_VersionedRowRefSchema {
    jazz_engine_RowRefSchema row;
    bool has_version; jazz_engine_ResultMembershipVersionSchema version;
    jazz_engine_OptStr branch_or_prefix_field;
} jazz_engine_VersionedRowRefSchema;
typedef struct jazz_engine_AuthorizedRowsSchema { jz_str row_field; jz_btree routing_param_fields; } jazz_engine_AuthorizedRowsSchema;
typedef struct jazz_engine_ResultMembershipSchema {
    jz_str table_field, row_field;
    jazz_engine_StrVec occurrence_id_fields;
    jz_btree occurrence_union_arm_fields; /* usize -> String */
    jazz_engine_TypedOutputFieldVec payload_fields;
    jz_btree payload_publication_fields;  /* String -> CurrentRowPublicationField */
    jazz_engine_OptStr branch_or_prefix_field;
    jazz_engine_ResultMembershipVersionSchema version;
    jazz_engine_OptStr settle_position_field;
    jz_btree routing_param_fields;
} jazz_engine_ResultMembershipSchema;
typedef struct jazz_engine_RelationEdgeSchema {
    jazz_engine_VersionedRowRefSchema source;
    jz_str path_field;
    jazz_engine_VersionedRowRefSchema target;
    jz_str kind_field;
    jazz_engine_OptStr depth_field, edge_id_field, branch_field, role_field, order_field, hole_state_field;
} jazz_engine_RelationEdgeSchema;
typedef struct jazz_engine_PathCorrelationCoverageSchema {
    jazz_engine_VersionedRowRefSchema parent;
    jz_str path_field, correlation_field;
    jazz_engine_OptStr expected_count_field;
    jz_str readable_count_field, coverage_state_field;
} jazz_engine_PathCorrelationCoverageSchema;
typedef struct jazz_engine_VersionWitnessSchema {
    jazz_protocol_ProgramSourceId source;
    groove_RecordDescriptor descriptor;
    jazz_engine_VersionIdentityFields identity;
    jz_str created_by_field, created_at_field, updated_by_field, updated_at_field, authored_columns_field, deletion_field;
    jz_btree user_fields; /* String -> String */
} jazz_engine_VersionWitnessSchema;
typedef struct jazz_engine_VersionWitnessSchemas {
    jz_str role_field;
    bool has_content; jazz_engine_VersionWitnessSchema content;
    bool has_deletion; jazz_engine_VersionWitnessSchema deletion;
    jz_btree routing_param_fields;
} jazz_engine_VersionWitnessSchemas;
typedef struct jazz_engine_ProgramSourceCoverageSchema { jazz_protocol_ProgramSourceId source; bool complete; jz_btree routing_param_fields; } jazz_engine_ProgramSourceCoverageSchema;
typedef struct jazz_engine_ReadFrontierSettledSchema {
    jz_str tier_field, frontier_field; jazz_engine_OptStr stream_field, through_field; jz_str scope_field; jz_btree routing_param_fields;
} jazz_engine_ReadFrontierSettledSchema;
typedef struct jazz_engine_BatchIdentityFields { jz_str batch_id_field; jazz_engine_OptStr batch_node_field; } jazz_engine_BatchIdentityFields;
typedef struct jazz_engine_CompleteTxPayloadCoverageSchema {
    jazz_engine_BatchIdentityFields batch; jz_str tier_field, payload_digest_field, fate_field;
} jazz_engine_CompleteTxPayloadCoverageSchema;
enum { JAZZ_ENGINE_RIS_REAL_ROW, JAZZ_ENGINE_RIS_SYNTHETIC_ROW, JAZZ_ENGINE_RIS_PATH_TUPLE };
typedef struct jazz_engine_ResultIdSchema {
    u8 tag;
    jazz_engine_VersionedRowRefSchema real_row;
    jazz_engine_SyntheticResultMembershipSchema synthetic_row;
    jz_str path_field, row_field, revision_field;
} jazz_engine_ResultIdSchema;
typedef struct jazz_engine_ViewCompleteExclusiveCoverageSchema {
    jazz_engine_BatchIdentityFields batch; jz_str view_scope_field;
    bool has_result; jazz_engine_ResultIdSchema result;
    jz_str covered_members_digest_field, tier_field, fate_field;
} jazz_engine_ViewCompleteExclusiveCoverageSchema;
typedef struct jazz_engine_VisibleBatchMemberFields { jz_str table_field, row_field, data_branch_field, row_digest_field; } jazz_engine_VisibleBatchMemberFields;
typedef struct jazz_engine_ContributingMembersSchema {
    jazz_engine_ResultIdSchema result; jz_str contributor_field; jazz_engine_VisibleBatchMemberFields member;
    jazz_engine_BatchIdentityFields batch; jz_str contributing_digest_field;
} jazz_engine_ContributingMembersSchema;
typedef struct jazz_engine_PolicyWitnessSchema {
    jazz_engine_VersionedRowRefSchema protected_; jz_str policy_path_field; jazz_engine_VersionedRowRefSchema witness; jz_str edge_kind_field;
} jazz_engine_PolicyWitnessSchema;
typedef struct jazz_engine_PredicateReadFactSchema {
    jz_str table_field, shape_id_field, binding_id_field, canonical_shape_field;
    jz_btree binding_value_fields;
    jz_str base_snapshot_field, resolved_read_key_field, policy_key_field, source_visibility_field;
} jazz_engine_PredicateReadFactSchema;
typedef struct jazz_engine_PredicateOutputSetSchema {
    u8 role; jz_str table_field, row_field; jazz_engine_ResultMembershipVersionSchema version; jz_str shape_id_field, binding_id_field;
} jazz_engine_PredicateOutputSetSchema;
typedef struct jazz_engine_PointReadFactSchema {
    jz_str table_field, row_field, presence_field; jazz_engine_OptStr observed_version_field, base_snapshot_field;
} jazz_engine_PointReadFactSchema;
typedef struct jazz_engine_ProgramFactOutput jazz_engine_ProgramFactOutput;
typedef struct jazz_engine_PolicyDecisionSchema {
    jz_str outcome_field; jazz_engine_OptStr required_input_field, reason_field;
    JZ_VEC(jazz_engine_ProgramFactOutput) facts;
} jazz_engine_PolicyDecisionSchema;
enum {
    JAZZ_ENGINE_PFS_AUTHORIZED_ROWS, JAZZ_ENGINE_PFS_RESULT_MEMBERSHIP, JAZZ_ENGINE_PFS_AGGREGATE_RESULT,
    JAZZ_ENGINE_PFS_RELATION_EDGES, JAZZ_ENGINE_PFS_PATH_CORRELATION_COVERAGE, JAZZ_ENGINE_PFS_PROGRAM_SOURCE_COVERAGE,
    JAZZ_ENGINE_PFS_READ_FRONTIER_SETTLED, JAZZ_ENGINE_PFS_COMPLETE_TX_PAYLOAD_COVERAGE, JAZZ_ENGINE_PFS_VIEW_COMPLETE_EXCLUSIVE_COVERAGE,
    JAZZ_ENGINE_PFS_VERSION_WITNESSES, JAZZ_ENGINE_PFS_REPLACEMENT_WITNESSES, JAZZ_ENGINE_PFS_POLICY_DECISION,
    JAZZ_ENGINE_PFS_POLICY_WITNESSES, JAZZ_ENGINE_PFS_CONTRIBUTING_MEMBERS, JAZZ_ENGINE_PFS_PREDICATE_READS,
    JAZZ_ENGINE_PFS_PREDICATE_OUTPUT_SET, JAZZ_ENGINE_PFS_POINT_READS
};
typedef struct jazz_engine_ProgramFactSchema {
    u8 tag;
    union {
        jazz_engine_AuthorizedRowsSchema authorized_rows;
        jazz_engine_ResultMembershipSchema result_membership;
        jazz_engine_AggregateResultSchema aggregate_result;
        jazz_engine_RelationEdgeSchema relation_edges;
        jazz_engine_PathCorrelationCoverageSchema path_correlation_coverage;
        jazz_engine_ProgramSourceCoverageSchema program_source_coverage;
        jazz_engine_VersionWitnessSchemas witnesses; /* VersionWitnesses / ReplacementWitnesses */
        jazz_engine_ReadFrontierSettledSchema read_frontier_settled;
        jazz_engine_CompleteTxPayloadCoverageSchema complete_tx_payload_coverage;
        jazz_engine_ViewCompleteExclusiveCoverageSchema view_complete_exclusive_coverage;
        jazz_engine_PolicyDecisionSchema policy_decision;
        jazz_engine_PolicyWitnessSchema policy_witnesses;
        jazz_engine_ContributingMembersSchema contributing_members;
        jazz_engine_PredicateReadFactSchema predicate_reads;
        jazz_engine_PredicateOutputSetSchema predicate_output_set;
        jazz_engine_PointReadFactSchema point_reads;
    } u;
} jazz_engine_ProgramFactSchema;
enum {
    JAZZ_ENGINE_PFT_PRIMARY, JAZZ_ENGINE_PFT_VERSION_WITNESS_CONTENT, JAZZ_ENGINE_PFT_VERSION_WITNESS_DELETION,
    JAZZ_ENGINE_PFT_REPLACEMENT_WITNESS_CONTENT, JAZZ_ENGINE_PFT_REPLACEMENT_WITNESS_DELETION
};
struct jazz_engine_ProgramFactOutput { jazz_engine_ProgramFactKey key; u8 terminal; jazz_engine_ProgramFactSchema schema; };
enum { JAZZ_ENGINE_OTS_APP_ROWS, JAZZ_ENGINE_OTS_FACT };
typedef struct jazz_engine_OutputTerminalSchema {
    u8 tag;
    union { jazz_engine_AppRowSchema app_rows; jazz_engine_ProgramFactOutput fact; } u;
} jazz_engine_OutputTerminalSchema;
typedef JZ_VEC(jazz_engine_OutputTerminalSchema) jazz_engine_OutputTerminalSchemaVec;
typedef struct jazz_engine_ProgramOutputSchemas { jazz_engine_OutputTerminalSchemaVec row_set; } jazz_engine_ProgramOutputSchemas;

extern const jz_btree_desc jazz_engine__STR_CRPF_MAP_DESC;  /* String -> CurrentRowPublicationField */
extern const jz_btree_desc jazz_engine__STR_U8_MAP_DESC;    /* String -> AppRowCarrier */
extern const jz_btree_desc jazz_engine__U32_STR_MAP_DESC;   /* usize -> String */
#define JE_SCH(x) jazz_engine__query_engine__schemas__##x
void jazz_engine__optstr_clone(jazz_engine_OptStr *dst, const jazz_engine_OptStr *src);
void jazz_engine__optstr_drop(jazz_engine_OptStr *s);
jazz_engine_OptStr jazz_engine__optstr_some(jz_str s);
void jazz_engine__optstr_debug(jz_str *out, const jazz_engine_OptStr *s);
void JE_SCH(SyntheticResultMembershipSchema__Clone__clone)(jazz_engine_SyntheticResultMembershipSchema *dst, const jazz_engine_SyntheticResultMembershipSchema *src);
void JE_SCH(SyntheticResultMembershipSchema__drop)(jazz_engine_SyntheticResultMembershipSchema *s);
void JE_SCH(AggregateResultSchema__Clone__clone)(jazz_engine_AggregateResultSchema *dst, const jazz_engine_AggregateResultSchema *src);
void JE_SCH(AggregateResultSchema__drop)(jazz_engine_AggregateResultSchema *s);
void JE_SCH(AppRowSchema__Clone__clone)(jazz_engine_AppRowSchema *dst, const jazz_engine_AppRowSchema *src);
void JE_SCH(AppRowSchema__drop)(jazz_engine_AppRowSchema *s);
void JE_SCH(AppRowSchema__Debug__fmt)(jz_str *out, const jazz_engine_AppRowSchema *s);
void JE_SCH(VersionIdentityFields__Clone__clone)(jazz_engine_VersionIdentityFields *dst, const jazz_engine_VersionIdentityFields *src);
void JE_SCH(VersionIdentityFields__drop)(jazz_engine_VersionIdentityFields *f);
void JE_SCH(VersionIdentityFields__Debug__fmt)(jz_str *out, const jazz_engine_VersionIdentityFields *f);
void JE_SCH(ResultMembershipVersionSchema__Clone__clone)(jazz_engine_ResultMembershipVersionSchema *dst, const jazz_engine_ResultMembershipVersionSchema *src);
void JE_SCH(ResultMembershipVersionSchema__drop)(jazz_engine_ResultMembershipVersionSchema *v);
void JE_SCH(ResultMembershipVersionSchema__Debug__fmt)(jz_str *out, const jazz_engine_ResultMembershipVersionSchema *v);
void JE_SCH(VersionedRowRefSchema__Clone__clone)(jazz_engine_VersionedRowRefSchema *dst, const jazz_engine_VersionedRowRefSchema *src);
void JE_SCH(VersionedRowRefSchema__drop)(jazz_engine_VersionedRowRefSchema *v);
void JE_SCH(VersionedRowRefSchema__Debug__fmt)(jz_str *out, const jazz_engine_VersionedRowRefSchema *v);
void JE_SCH(VersionWitnessSchema__Clone__clone)(jazz_engine_VersionWitnessSchema *dst, const jazz_engine_VersionWitnessSchema *src);
void JE_SCH(VersionWitnessSchema__drop)(jazz_engine_VersionWitnessSchema *s);
bool JE_SCH(VersionWitnessSchema__PartialEq__eq)(const jazz_engine_VersionWitnessSchema *a, const jazz_engine_VersionWitnessSchema *b);
void JE_SCH(VersionWitnessSchemas__Clone__clone)(jazz_engine_VersionWitnessSchemas *dst, const jazz_engine_VersionWitnessSchemas *src);
void JE_SCH(VersionWitnessSchemas__drop)(jazz_engine_VersionWitnessSchemas *s);
void JE_SCH(ProgramFactSchema__Clone__clone)(jazz_engine_ProgramFactSchema *dst, const jazz_engine_ProgramFactSchema *src);
void JE_SCH(ProgramFactSchema__drop)(jazz_engine_ProgramFactSchema *s);
void JE_SCH(ProgramFactOutput__Clone__clone)(jazz_engine_ProgramFactOutput *dst, const jazz_engine_ProgramFactOutput *src);
void JE_SCH(ProgramFactOutput__drop)(jazz_engine_ProgramFactOutput *o);
void JE_SCH(ProgramFactOutput__key)(const jazz_engine_ProgramFactOutput *o, jazz_engine_ProgramFactKey *out);
void JE_SCH(OutputTerminalSchema__Clone__clone)(jazz_engine_OutputTerminalSchema *dst, const jazz_engine_OutputTerminalSchema *src);
void JE_SCH(OutputTerminalSchema__drop)(jazz_engine_OutputTerminalSchema *s);
void JE_SCH(ProgramOutputSchemas__drop)(jazz_engine_ProgramOutputSchemas *s);
void jazz_engine__ots_drop_v(void *s);
void jazz_engine__ots_clone_v(void *d, const void *s);
void jazz_engine__descriptor_field_drop_v(void *f);
void jazz_engine__descriptor_field_clone_v(void *d, const void *s);

#endif
