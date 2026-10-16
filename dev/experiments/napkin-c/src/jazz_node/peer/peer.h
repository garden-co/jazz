/* jazz_node::peer (peer.rs + peer/subscription_state.rs + peer/delivery.rs):
 * per-peer server-side sync state. Header owned by unit node-core-b.
 * publication.rs / known_state.rs / repair.rs are `include!`d into peer.rs in
 * Rust; here they are separate translation units sharing this header. */
#ifndef JZ_NODE_PEER_H
#define JZ_NODE_PEER_H
#include "jz_rt.h"
#include "jazz_node/node/mod.h"

#define JZP(x) jazz_node__peer__##x
typedef jazz_protocol_SubscriptionKey SubKey;

/* (AuthorSubject, BTreeMap<String, Value>) */
typedef struct jazz_node_PolicyBinding { jazz_types_AuthorSubject identity; jz_btree claims; } jazz_node_PolicyBinding;
void JZP(PolicyBinding__clone)(jazz_node_PolicyBinding *dst, const jazz_node_PolicyBinding *src);
void JZP(PolicyBinding__drop)(jazz_node_PolicyBinding *b);
bool JZP(PolicyBinding__eq)(const jazz_node_PolicyBinding *a, const jazz_types_AuthorSubject *identity, const jz_btree *claims);
void JZP(claims_clone)(jz_btree *dst, const jz_btree *src);

/* ---------------- subscription_state.rs ---------------- */
/* PeerRole */
enum { JZN_PEER_ROLE_RELAY = 0, JZN_PEER_ROLE_CLIENT_LINK = 1 };
typedef struct jazz_node_PeerRole { u32 tag; jazz_types_AuthorSubject identity; } jazz_node_PeerRole;
bool JZP(subscription_state__PeerRole__permission_subject)(jazz_node_PeerRole role, jazz_types_AuthorSubject *out);
bool JZP(PeerRole__eq)(jazz_node_PeerRole a, jazz_node_PeerRole b);

bool JZP(subscription_state__fast_current_membership_position)(const jazz_protocol_KnownStateDeclaration *known_state, jazz_types_GlobalTime *out);
bool JZP(subscription_state__fast_authorization_progress)(const jazz_protocol_KnownStateDeclaration *known_state, u64 *out);
bool JZP(subscription_state__member_settle_position)(const jazz_protocol_ResultMemberEntry *member, jazz_types_GlobalTime *out);
bool JZP(subscription_state__fast_cursor_requires_authoritative_reset)(jazz_types_GlobalTime position, const jz_btree *previous, const jz_btree *current);
bool JZP(subscription_state__fast_cursor_membership_mismatch)(jazz_types_GlobalTime position, const jz_btree *previous, const jz_btree *current);

/* RowKey = (OutputOccurrenceId, Intern<String>, RowUuid) */
typedef struct jazz_node_RowKey { jazz_types_OutputOccurrenceId *occurrence; groove_InternStr table; jazz_types_RowUuid row; } jazz_node_RowKey;
/* MemberIndexKey: Row(RowKey) | Synthetic{table, row} | Member(ResultMemberEntry) */
enum { JZN_MIK_ROW = 0, JZN_MIK_SYNTHETIC = 1, JZN_MIK_MEMBER = 2 };
typedef struct jazz_node_MemberIndexKey {
    u32 tag;
    jazz_node_RowKey row;
    jz_str synthetic_table; jz_bytes synthetic_row;
    jazz_protocol_ResultMemberEntry member;
} jazz_node_MemberIndexKey;
int JZP(MemberIndexKey__cmp)(const void *a, const void *b, void *ctx);
void JZP(MemberIndexKey__clone)(void *dst, const void *src);
void JZP(MemberIndexKey__drop)(void *k);
typedef struct jazz_node_MemberSlot { jazz_protocol_ResultMemberEntry member; usize refcount; } jazz_node_MemberSlot;
void JZP(MemberSlot__clone)(void *dst, const void *src);
void JZP(MemberSlot__drop)(void *s);
extern const jz_btree_desc jzp_desc_member_index;   /* MemberIndexKey -> MemberSlot */
extern const jz_btree_desc jzp_desc_member_set;     /* set<ResultMemberEntry> */
extern const jz_btree_desc jzp_desc_member_states;  /* ResultMemberEntry -> (bool, bool) */

/* CachedPeerQueryPlan */
typedef struct jazz_node_CachedPeerQueryPlan {
    jazz_model_DurabilityTier tier;
    jazz_protocol_ReadViewSpec *read_view; /* Arc (jz_rc) */
    jazz_node_PreparedQueryPlan *plan;     /* Option<Arc>; NULL = None */
} jazz_node_CachedPeerQueryPlan;
jazz_node_CachedPeerQueryPlan *JZP(CachedPeerQueryPlan__with_plan)(const jazz_protocol_RegisterShapeOptions *opts, jazz_node_PreparedQueryPlan *plan);
jazz_node_CachedPeerQueryPlan *JZP(CachedPeerQueryPlan__with_context)(jazz_model_DurabilityTier tier, jazz_protocol_ReadViewSpec *read_view, jazz_node_PreparedQueryPlan *plan);
bool JZP(CachedPeerQueryPlan__has_runtime_plan)(const jazz_node_CachedPeerQueryPlan *p);
void JZP(CachedPeerQueryPlan__replace_runtime_plan)(jazz_node_CachedPeerQueryPlan *p, jazz_node_PreparedQueryPlan *plan);
void JZP(CachedPeerQueryPlan__clear_runtime_plan)(jazz_node_CachedPeerQueryPlan *p);
void JZP(CachedPeerQueryPlan__context)(const jazz_node_CachedPeerQueryPlan *p, jazz_model_DurabilityTier *tier, jazz_protocol_ReadViewSpec **read_view /* new Arc ref */);
void JZP(CachedPeerQueryPlan__drop)(jazz_node_CachedPeerQueryPlan *p);

/* MaintainedSubscriptionViewSubscription */
typedef struct jazz_node_MaintainedSubscriptionViewSubscription {
    groove_MultisinkSubscription *subscription;
    jazz_node_MaintainedSubscriptionView *maintained;
    jazz_node_MaintainedTerminalSchemas *terminal_schemas;
    jz_btree *tables;                                      /* Arc<BTreeMap<String, TableSchema>> (jz_rc) */
    jazz_node_CoveredInputReceiver *covered_input_receiver; /* NULL = None */
    jazz_types_SchemaVersionId result_schema_version;
    bool has_source_authority_result; jazz_protocol_AuthorityResultKey source_authority_result;
    bool initial_received;
} jazz_node_MaintainedSubscriptionViewSubscription;
void JZP(MaintainedSubscriptionViewSubscription__drop)(jazz_node_MaintainedSubscriptionViewSubscription *m);

/* PeerSubscriptionState */
typedef struct jazz_node_PeerSubscriptionState {
    bool has_policy_binding; jazz_node_PolicyBinding policy_binding;
    bool has_authority_result_source; jazz_protocol_AuthorityResultKey authority_result_source;
    bool awaiting_selected_authority_source;
    jz_btree result_member_set;                 /* set<ResultMemberEntry> */
    bool has_supporting_revision; u8 supporting_revision[16];
    jazz_node_LocalAuthorityReconciliation *local_authority;
    jz_btree member_index;                      /* MemberIndexKey -> MemberSlot */
    jazz_node_MaintainedSubscriptionViewSubscription *maintained_subscription_view; /* NULL = None */
    jazz_node_CachedPeerQueryPlan *prepared_query;                                  /* NULL = None */
    bool has_groove_runtime_token; u64 groove_runtime_token;
    bool has_physical_identity_generation; u64 physical_identity_generation;
    u64 authorization_progress;
    bool has_served_authorization_progress;
} jazz_node_PeerSubscriptionState;
jazz_node_PeerSubscriptionState *JZP(PeerSubscriptionState__default)(void);
void JZP(PeerSubscriptionState__drop)(jazz_node_PeerSubscriptionState *s);
void JZP(PeerSubscriptionState__clear_groove_runtime_handles)(jazz_node_PeerSubscriptionState *s);
bool JZP(PeerSubscriptionState__runtime_is_stale)(const jazz_node_PeerSubscriptionState *s, const NS *node);
void JZP(PeerSubscriptionState__acknowledge_published)(jazz_node_PeerSubscriptionState *s, const u8 revision[16]);
void JZP(PeerSubscriptionState__member_result_set)(const jazz_node_PeerSubscriptionState *s, jz_btree *out);
void JZP(PeerSubscriptionState__supporting_rows)(const jazz_node_PeerSubscriptionState *s, jzn_SupportingRowRefVec *out);
void JZP(PeerSubscriptionState__previous_tx_ids)(const jazz_node_PeerSubscriptionState *s, jz_btree *out);

/* RehydratePurpose */
typedef enum { JZN_REHYDRATE_QUERY = 0, JZN_REHYDRATE_AUTHORIZATION_SUPPORT = 1 } jazz_node_RehydratePurpose;
/* MaintainedRehydrateRequest<'a> */
typedef struct jazz_node_MaintainedRehydrateRequest {
    const jazz_model_ValidatedQuery *shape;
    const jazz_model_Binding *binding;
    SubKey subscription;
    const jz_btree *previous_member_result_set;
    bool reset_input_set;
    const jz_str *result_table_filter; /* NULL = None */
    jazz_model_DurabilityTier tier;
    const jazz_protocol_ReadViewSpec *read_view;
    jazz_node_RehydratePurpose purpose;
} jazz_node_MaintainedRehydrateRequest;

/* ---------------- peer.rs ---------------- */
enum { JZN_RTC_ORDINARY_SESSION = 0, JZN_RTC_SCOPE_ISOLATED_CLIENT_RELAY = 1, JZN_RTC_MULTIPLEXED_RELAY = 2 };
typedef struct jazz_node_RelayTransportCapability {
    u32 tag;
    jazz_protocol_DelegatedSessionBinding binding; /* ScopeIsolatedClientRelay */
    u64 admission_epoch;
} jazz_node_RelayTransportCapability;

typedef struct jazz_node_FullDiffFallbackMetrics {
    u64 membership_reconciliations, query_reopens, authorization_support_reopens, runtime_resets;
} jazz_node_FullDiffFallbackMetrics;
u64 JZP(FullDiffFallbackMetrics__total)(const jazz_node_FullDiffFallbackMetrics *m);
void JZP(FullDiffFallbackMetrics__add)(jazz_node_FullDiffFallbackMetrics *m, jazz_node_FullDiffFallbackMetrics other);
typedef struct jazz_node_MaintainedSubscriptionViewMetrics {
    u64 hits_out, unsupported_skips_out, delta_batches_in;
    jazz_node_FullDiffFallbackMetrics full_diff_fallbacks;
    u64 rehydrate_attempts; /* cfg(any(test, feature = "testing")) */
} jazz_node_MaintainedSubscriptionViewMetrics;
typedef struct jazz_node_PeerMetrics {
    u64 view_updates_out, version_bundles_out, duplicate_version_bundles_out, complete_tx_payload_refs_out, result_adds_out, result_removes_out;
    jazz_node_MaintainedSubscriptionViewMetrics *maintained_subscription_view; /* Box */
} jazz_node_PeerMetrics;

typedef struct jazz_node_PeerState {
    u64 publication_owner;
    jazz_node_PeerRole role;
    bool has_permission_identity; jazz_types_AuthorSubject permission_identity;
    jazz_node_RelayTransportCapability transport_capability;
    bool authority_query_delegate;
    jz_btree shipped_complete_tx_payloads;  /* set<TxId> */
    bool ship_complete_exclusive_payloads;
    jz_btree publication_states;            /* SubscriptionKey -> PeerSubscriptionState* */
    jz_btree downstream_known_states;       /* SubscriptionKey -> KnownStateDeclaration */
    bool has_announced_catalogue_fingerprint; u8 announced_catalogue_fingerprint[32];
    jazz_node_PeerMetrics metrics;
} jazz_node_PeerState;
typedef jazz_node_PeerState PS;
void JZP(PeerState__default)(PS *out);
void JZP(PeerState__drop)(PS *p);
jazz_node_PeerSubscriptionState *JZP(PeerState__state)(const PS *p, SubKey sub);        /* get() */
jazz_node_PeerSubscriptionState *JZP(PeerState__state_entry)(PS *p, SubKey sub);        /* entry().or_default() */
const jazz_protocol_KnownStateDeclaration *JZP(PeerState__known_state)(const PS *p, SubKey sub);

/* ---------------- delivery.rs ---------------- */
bool JZP(delivery__result_member_replaces)(const jazz_protocol_ResultMemberEntry *replacement, const jazz_protocol_ResultMemberEntry *previous);
void JZP(delivery__replacement_removals)(const jazz_node_PeerSubscriptionState *previous, const jazz_protocol_ResultMemberEntry *additions, u32 n,
    jazz_protocol_ResultMemberEntryVec *out);
void JZP(delivery__apply_contribution_add)(jazz_node_PeerSubscriptionState *state, const jazz_protocol_ResultMemberEntry *contribution, u32 n,
    jazz_protocol_ResultMemberEntryVec *adds, jazz_protocol_ResultMemberEntryVec *removes);
void JZP(delivery__apply_contribution_remove)(jazz_node_PeerSubscriptionState *state, const jazz_protocol_ResultMemberEntry *contribution, u32 n,
    jazz_protocol_ResultMemberEntryVec *removes);
bool JZP(delivery__duplicate_physical_row_result_set)(const jz_btree *result_set, jazz_node_RowKey *row_key, jazz_model_TxId *first, jazz_model_TxId *second);
bool JZP(delivery__bundle_contains_complete_tx_payload)(const jazz_protocol_VersionBundle *bundle);
void JZP(delivery__view_update_singleton_bundles)(jazz_protocol_VersionCarrier *const *carriers, u32 n, jazz_protocol_VersionBundleVec *out);
jz_str JZP(delivery__storage_read_metrics_buckets)(const groove_StorageReadMetrics *metrics);
void JZP(delivery__binding_values_in_param_order)(const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, groove_ValueVec *out);

/* ---------------- known_state.rs / repair.rs ---------------- */
void JZP(PeerState__forget_subscription)(PS *p, SubKey sub);
void JZP(PeerState__settle_watermark_declaration)(PS *p, SubKey sub);
bool JZP(PeerState__forget_subscription_with_node)(PS *p, NS *node, SubKey sub);
void JZP(PeerState__acknowledged_complete_tx_payloads)(const PS *p, jz_btree *out);
void JZP(PeerState__record_outgoing_view_update_metadata)(PS *p, jazz_protocol_SyncMessage *update);
void JZP(PeerState__record_outgoing_view_update)(PS *p, jazz_protocol_SyncMessage *update);
void JZP(PeerState__apply_outgoing_view_delta)(PS *p, SubKey sub, bool reset_input_set,
    const jazz_protocol_ResultMemberEntry *adds, u32 nadds, const jazz_protocol_ResultMemberEntry *removes, u32 nremoves);

/* ---------------- publication.rs ---------------- */
const jazz_node_MaintainedSubscriptionViewSubscription *JZP(PeerState__maintained_view)(const PS *p, SubKey sub);
bool JZP(read_policy_admits_every_row)(const jazz_model_Query *policy);
bool JZP(row_local_membership)(const jazz_model_Query *query);
bool JZP(query_uses_claims)(const jazz_model_Query *query);
/* ReconciledMaintainedSubscriptionClone */
typedef struct jazz_node_ReconciledMaintainedSubscriptionClone {
    jazz_protocol_SyncMessage *canonical_update; /* NULL = None */
    jazz_protocol_ResultMemberEntryVec source_removes;
    bool source_had_program_fact_transitions;
} jazz_node_ReconciledMaintainedSubscriptionClone;

#endif
