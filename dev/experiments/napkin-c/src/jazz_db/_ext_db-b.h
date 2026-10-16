/* _ext_db-b.h: local mirror declarations of types and functions from other
 * units (groove, jazz_types, jazz_model, jazz_protocol, jazz_engine,
 * jazz_node, and the db-a half of jazz_db) used by unit db-b.
 * Layouts approximate the Rust fields; only what db-b touches is spelled out. */
#ifndef JAZZ_DB_EXT_DB_B_H
#define JAZZ_DB_EXT_DB_B_H
#include "jz_rt.h"

/* ======================= jazz_types ======================= */
typedef u64 jazz_types_TxTime;
typedef u64 jazz_types_GlobalTime;
typedef jz_uuid jazz_types_NodeUuid;
typedef jz_uuid jazz_types_RowUuid;
typedef struct jazz_types_SchemaVersionId { u8 b[32]; } jazz_types_SchemaVersionId;
typedef struct jazz_types_GlobalPhysicalTableId { u8 b[32]; } jazz_types_GlobalPhysicalTableId;
/* AuthorSubject: enum { System, SystemAt(Intern), Authenticated(Intern) } (Copy) */
typedef struct jazz_types_AuthorSubject { u32 tag; const void *interned; } jazz_types_AuthorSubject;
enum { JAZZ_AUTHOR_SYSTEM = 0, JAZZ_AUTHOR_SYSTEM_AT = 1, JAZZ_AUTHOR_AUTHENTICATED = 2 };
static inline bool jazz_types_AuthorSubject_eq(const jazz_types_AuthorSubject *a, const jazz_types_AuthorSubject *b) { return a->tag == b->tag && a->interned == b->interned; }
int jazz_types__ids__AuthorSubject__cmp(const jazz_types_AuthorSubject *a, const jazz_types_AuthorSubject *b);
jz_err jazz_types__ids__AuthorSubject__from_untrusted_canonical(jz_slice canonical, jazz_types_AuthorSubject *out);
void jazz_types__ids__AuthorSubject__debug(jz_str *out, const jazz_types_AuthorSubject *a);
extern const jazz_types_AuthorSubject jazz_types__ids__AuthorSubject__SYSTEM;

typedef const jz_uuid *jazz_types_ObjectId; /* Intern<Uuid> */
jazz_types_ObjectId jazz_types__object__ObjectId__from_uuid(const jz_uuid *u);
/* OutputOccurrenceId { root, joined: SmallVec<[ObjectId;2]>, derivations } */
typedef struct jazz_types_OutputOccurrenceId {
    jazz_types_ObjectId root;
    JZ_VEC(jazz_types_ObjectId) joined;
    JZ_VEC(u8) derivations;
} jazz_types_OutputOccurrenceId;
void jazz_types__object__OutputOccurrenceId__new(jazz_types_OutputOccurrenceId *out, jazz_types_ObjectId root, jazz_types_ObjectId *joined, u32 n);
void jazz_types__object__OutputOccurrenceId__single_source(jazz_types_OutputOccurrenceId *out, jazz_types_ObjectId root);
void jazz_types__object__OutputOccurrenceId__clone(void *dst, const void *src);
void jazz_types__object__OutputOccurrenceId__drop(void *p);
int jazz_types__object__OutputOccurrenceId__cmp(const void *a, const void *b, void *ctx);
bool jazz_types__object__OutputOccurrenceId__eq(const jazz_types_OutputOccurrenceId *a, const jazz_types_OutputOccurrenceId *b);
jz_bytes jazz_types__object__OutputOccurrenceId__canonical_bytes(const jazz_types_OutputOccurrenceId *o);
void jazz_types__object__OutputOccurrenceId__debug(jz_str *out, const jazz_types_OutputOccurrenceId *o);
typedef struct jazz_types_ResultKey { jazz_types_OutputOccurrenceId occ; } jazz_types_ResultKey;

/* debug_env */
bool jazz_types__debug_env__covered_input_trace(void);
/* local_executor::block_on is identity in the synchronous port */

#define JAZZ_TYPES_HLC_MAX_PHYSICAL_MS ((u64)0xFFFFFFFFFFFFull)

/* ======================= jazz_model ======================= */
typedef struct jazz_model_TxId { jazz_types_TxTime time; jz_uuid node; } jazz_model_TxId;
int jazz_model__tx__TxId__cmp(const void *a, const void *b, void *ctx);
u64 jazz_model__tx__TxId__hash(const void *a);
bool jazz_model__tx__TxId__eq(const void *a, const void *b);
void jazz_model__tx__TxId__debug(jz_str *out, const jazz_model_TxId *t);
typedef enum { JAZZ_TX_MERGEABLE = 0, JAZZ_TX_EXCLUSIVE = 1 } jazz_model_TxKind;
typedef enum { JAZZ_DUR_NONE = 0, JAZZ_DUR_LOCAL = 1, JAZZ_DUR_GLOBAL = 2 } jazz_model_DurabilityTier;
void jazz_model__tx__DurabilityTier__debug(jz_str *out, jazz_model_DurabilityTier t);
/* RejectionReason: unit variants + Cascade{root} + custom detail variants */
typedef enum {
    JAZZ_REJ_CLIENT_CLOCK_TOO_FAR_AHEAD, JAZZ_REJ_AUTHORIZATION_DENIED, JAZZ_REJ_EXCLUSIVE_CONFLICT,
    JAZZ_REJ_CAUSALITY_VIOLATION, JAZZ_REJ_CASCADE, JAZZ_REJ_MALFORMED_COMMIT
} jazz_model_RejectionReasonTag;
typedef struct jazz_model_RejectionReason { u32 tag; jazz_model_TxId root; jz_str detail; } jazz_model_RejectionReason;
void jazz_model__tx__RejectionReason__clone(void *dst, const void *src);
void jazz_model__tx__RejectionReason__drop(void *p);
void jazz_model__tx__RejectionReason__debug(jz_str *out, const jazz_model_RejectionReason *r);
typedef enum { JAZZ_FATE_PENDING = 0, JAZZ_FATE_ACCEPTED = 1, JAZZ_FATE_REJECTED = 2 } jazz_model_FateTag;
typedef struct jazz_model_Fate { u32 tag; jazz_model_RejectionReason rejected; } jazz_model_Fate;
void jazz_model__tx__Fate__clone(void *dst, const void *src);
void jazz_model__tx__Fate__drop(void *p);

typedef struct jazz_model_TransactionId { jz_uuid id; } jazz_model_TransactionId; /* public_schema::TransactionId */
void jazz_model__model__public_schema__TransactionId__from_tx_id(jazz_model_TransactionId *out, const jazz_model_TxId *tx);
void jazz_model__model__public_schema__TransactionId__to_string(const jazz_model_TransactionId *t, jz_str *out);
typedef struct jazz_model_OpenTransactionId { u64 v; } jazz_model_OpenTransactionId;
void jazz_model__model__transaction__OpenTransactionId__display(jz_str *out, const jazz_model_OpenTransactionId *t);

/* query */
typedef struct jazz_model_ShapeId { jz_uuid u; } jazz_model_ShapeId;
typedef struct jazz_model_BindingId { jz_uuid u; } jazz_model_BindingId;
typedef struct jazz_model_ValidatedQuery jazz_model_ValidatedQuery; /* Rc-shared, opaque */
typedef struct jazz_model_Binding jazz_model_Binding;
typedef struct jazz_model_Query jazz_model_Query;

/* ======================= groove ======================= */
typedef struct groove_RecordDescriptor groove_RecordDescriptor;
typedef enum {
    GROOVE_V_NULL, GROOVE_V_BOOL, GROOVE_V_INT, GROOVE_V_FLOAT, GROOVE_V_STRING, GROOVE_V_BYTES,
    GROOVE_V_UUID, GROOVE_V_TIMESTAMP, GROOVE_V_NULLABLE, GROOVE_V_ARRAY, GROOVE_V_RECORD, GROOVE_V_JSON
} groove_ValueTag;
typedef struct groove_OwnedRecord { jz_bytes raw; const groove_RecordDescriptor *descriptor; } groove_OwnedRecord;
typedef struct groove_Value groove_Value;
struct groove_Value {
    u8 tag;
    union {
        bool b; i64 i; f64 f; jz_str s; jz_bytes bytes; jz_uuid uuid;
        groove_Value *nullable; /* NULL = None */
        JZ_VEC(groove_Value) array;
        groove_OwnedRecord record;
    } v;
};
void groove__records__Value__clone(void *dst, const void *src);
void groove__records__Value__drop(void *p);
bool groove__records__Value__eq(const void *a, const void *b);
typedef struct groove_ColumnType { u32 tag; struct groove_ColumnType *inner; } groove_ColumnType;
enum { GROOVE_COLTYPE_NULLABLE = 9 };
bool groove__schema__ColumnType__eq(const groove_ColumnType *a, const groove_ColumnType *b);

/* groove::records::ValueType and descriptors */
typedef struct groove_ValueType groove_ValueType;
typedef struct groove_EnumSchema groove_EnumSchema;
typedef enum {
    GROOVE_VT_U8, GROOVE_VT_U16, GROOVE_VT_U32, GROOVE_VT_U64, GROOVE_VT_I32, GROOVE_VT_I64, GROOVE_VT_F64,
    GROOVE_VT_BOOL, GROOVE_VT_STRING, GROOVE_VT_BYTES, GROOVE_VT_INTERNAL, GROOVE_VT_UUID, GROOVE_VT_ENUM_TAG,
    GROOVE_VT_TUPLE, GROOVE_VT_ARRAY, GROOVE_VT_NULLABLE, GROOVE_VT_RECORD, GROOVE_VT_ENUM, GROOVE_VT_U48
} groove_ValueTypeTag;
struct groove_ValueType {
    u32 tag;
    union {
        JZ_VEC(groove_ValueType) tuple;
        groove_ValueType *inner;            /* Array / Nullable */
        groove_RecordDescriptor *record;    /* Record */
        groove_EnumSchema *enum_schema;     /* Enum */
        void *scalar_payload;               /* Internal / EnumTag */
    } v;
};
typedef struct groove_DescriptorField {
    bool has_name; jz_str name;
    bool has_identity; u32 identity_tag; jz_str identity_name; u64 identity_slot;
    groove_ValueType value_type;
} groove_DescriptorField;
typedef struct groove_EnumCase { jz_str name; groove_RecordDescriptor *payload; } groove_EnumCase;
struct groove_EnumSchema { u64 registry_id; jz_str name; JZ_VEC(groove_EnumCase) cases; };
const groove_DescriptorField *groove__records__RecordDescriptor__fields(const groove_RecordDescriptor *d, u32 *n);
const groove_RecordDescriptor *groove__records__RecordDescriptor__new_with_fields(groove_DescriptorField *fields, u32 n);
const groove_RecordDescriptor *groove__records__RecordDescriptor__new(groove_DescriptorField *fields, u32 n);
bool groove__records__RecordDescriptor__eq(const groove_RecordDescriptor *a, const groove_RecordDescriptor *b);
jz_err groove__records__RecordDescriptor__create(const groove_RecordDescriptor *d, const groove_Value *values, u32 n, jz_bytes *out);
void groove__records__values__ValueType__clone(void *dst, const void *src);
void groove__records__values__ValueType__drop(void *p);
bool groove__records__values__ValueType__eq(const void *a, const void *b);
void groove__records__values__ValueType__Serialize__serialize(const groove_ValueType *v, jz_pc_w *w);
jz_err groove__records__values__ValueType__Deserialize__deserialize(jz_pc_r *r, groove_ValueType *out);
jz_err groove__records__values__EnumSchema__new(jz_str name, groove_EnumCase *cases, u32 n, groove_EnumSchema **out);
groove_EnumCase groove__records__values__EnumCase__new(jz_str name, const groove_RecordDescriptor *payload);
void groove__records__OwnedRecord__new(groove_OwnedRecord *out, jz_bytes raw, const groove_RecordDescriptor *d);
void groove__records__OwnedRecord__drop(groove_OwnedRecord *r);
jz_err groove__records__OwnedRecord__to_values(const groove_OwnedRecord *r, JZ_VEC(groove_Value) *out);
jz_err groove__records__BorrowedRecord__to_values(jz_slice raw, const groove_RecordDescriptor *d, JZ_VEC(groove_Value) *out);
jz_err groove__records__BorrowedRecord__get_uuid(jz_slice raw, const groove_RecordDescriptor *d, u32 slot, jz_uuid *out);

/* groove::ivm::runtime::terminal */
typedef struct groove_TerminalPathSegment { u32 tag; jz_str collection; jz_bytes key; } groove_TerminalPathSegment;
typedef enum { GROOVE_TE_INSERT, GROOVE_TE_UPDATE, GROOVE_TE_REMOVE, GROOVE_TE_MOVE } groove_TerminalEditTag;
typedef struct groove_TerminalEdit { u32 tag; u32 index; jz_bytes key; jz_bytes value; } groove_TerminalEdit;
typedef struct groove_TerminalOperation {
    const groove_RecordDescriptor *root_descriptor;
    jz_bytes root_key;
    JZ_VEC(groove_TerminalPathSegment) path;
    groove_TerminalEdit edit;
} groove_TerminalOperation;
void groove__ivm__runtime__terminal__TerminalOperation__clone(void *dst, const void *src);
void groove__ivm__runtime__terminal__TerminalOperation__drop(void *p);
bool groove__ivm__runtime__terminal__TerminalOperation__eq(const void *a, const void *b);
jz_json *groove__ivm__runtime__terminal__TerminalOperation__Serialize__to_json_vec(const groove_TerminalOperation *ops, u32 n, jz_err *err);
typedef struct groove_SubscriptionId { u64 v; } groove_SubscriptionId;

/* ======================= jazz_engine ======================= */
typedef enum { JAZZ_ENGINE_VIS_APPLICATION_CELL, JAZZ_ENGINE_VIS_PUBLIC_PROVENANCE, JAZZ_ENGINE_VIS_HIDDEN_METADATA } jazz_engine_CurrentRowResultVisibility;
typedef enum { JAZZ_ENGINE_PUB_STORED_COLUMN, JAZZ_ENGINE_PUB_RESULT_FIELD, JAZZ_ENGINE_PUB_UNRESOLVED_SOURCE_CELL } jazz_engine_CurrentRowPublicationFieldTag;
typedef struct jazz_engine_CurrentRowPublicationField {
    u32 tag;
    jz_uuid id;          /* StoredColumn */
    jz_str name;         /* output_name / name */
    u32 visibility;      /* ResultField */
} jazz_engine_CurrentRowPublicationField;

/* ======================= jazz_node (basic row types) ======================= */
typedef struct jazz_node_CurrentRow {
    const jz_str *table;              /* groove::Intern<String> */
    groove_OwnedRecord *record;       /* Arc<OwnedRecord> */
    bool deleted;
    void *publication_fields;         /* Arc<Vec<CurrentRowPublicationField>> */
} jazz_node_CurrentRow;
void jazz_node__node__CurrentRow__clone(void *dst, const void *src);
void jazz_node__node__CurrentRow__drop(void *p);
bool jazz_node__node__CurrentRow__eq(const jazz_node_CurrentRow *a, const jazz_node_CurrentRow *b);
void jazz_node__node__CurrentRow__debug(jz_str *out, const jazz_node_CurrentRow *r);
jz_slice jazz_node__node__CurrentRow__table(const jazz_node_CurrentRow *r);
jazz_types_RowUuid jazz_node__node__CurrentRow__row_uuid(const jazz_node_CurrentRow *r);
bool jazz_node__node__CurrentRow__subscription_equivalent(const jazz_node_CurrentRow *a, const jazz_node_CurrentRow *b);
void jazz_node__node__CurrentRow__encoded_record(const jazz_node_CurrentRow *r, const groove_RecordDescriptor **descriptor, jz_slice *raw);
bool jazz_node__node__CurrentRow__raw_field(const jazz_node_CurrentRow *r, jz_slice name, groove_Value *out);
void jazz_node__node__CurrentRow__new(jazz_node_CurrentRow *out, jz_slice table, groove_OwnedRecord record);
void jazz_node__node__CurrentRow__new_with_publication_fields(jazz_node_CurrentRow *out, jz_slice table, groove_OwnedRecord record, void *publication_fields);
void *jazz_node__node__CurrentRow__publication_fields_clone(const jazz_node_CurrentRow *r);
const jazz_engine_CurrentRowPublicationField *jazz_node__node__CurrentRow__publication_fields(const jazz_node_CurrentRow *r, u32 *n);
bool jazz_node__node__CurrentRow__is_deleted(const jazz_node_CurrentRow *r);
typedef struct jazz_node_RelationEdge { u32 from; u32 to; jz_str relation; } jazz_node_RelationEdge;
void jazz_node__node__RelationEdge__clone(void *dst, const void *src);
void jazz_node__node__RelationEdge__drop(void *p);
typedef struct jazz_node_RelationSnapshot {
    u32 root_count;
    JZ_VEC(jazz_node_CurrentRow) rows;
    JZ_VEC(jazz_node_RelationEdge) edges;
} jazz_node_RelationSnapshot;
void jazz_node__node__RelationSnapshot__clone(jazz_node_RelationSnapshot *dst, const jazz_node_RelationSnapshot *src);
void jazz_node__node__RelationSnapshot__drop(jazz_node_RelationSnapshot *s);

/* ======================= jazz_protocol ======================= */
typedef struct jazz_protocol_ReadViewKey { jz_uuid id; } jazz_protocol_ReadViewKey;
typedef struct jazz_protocol_SubscriptionKey {
    jazz_model_ShapeId shape_id; jazz_model_BindingId binding_id; jazz_protocol_ReadViewKey read_view;
} jazz_protocol_SubscriptionKey;
typedef jazz_protocol_SubscriptionKey jazz_protocol_BindingViewKey;
int jazz_protocol__protocol__SubscriptionKey__cmp(const void *a, const void *b, void *ctx);
bool jazz_protocol__protocol__SubscriptionKey__eq(const jazz_protocol_SubscriptionKey *a, const jazz_protocol_SubscriptionKey *b);
void jazz_protocol__protocol__SubscriptionKey__serialize(const jazz_protocol_SubscriptionKey *k, jz_pc_w *w);
int jazz_protocol__protocol__BindingViewKey__cmp(const void *a, const void *b, void *ctx);
typedef struct jazz_protocol_PermissionAdviceRequestId { u8 b[16]; } jazz_protocol_PermissionAdviceRequestId;
typedef enum { JAZZ_ADVICE_ALLOWED, JAZZ_ADVICE_DENIED, JAZZ_ADVICE_UNKNOWN } jazz_protocol_PermissionAdvice;
typedef struct jazz_protocol_PermissionAdviceAction { u32 tag; void *payload; } jazz_protocol_PermissionAdviceAction;
void jazz_protocol__protocol__PermissionAdviceAction__clone(void *dst, const void *src);
void jazz_protocol__protocol__PermissionAdviceAction__drop(void *p);
/* model::branch::BranchSelector { values: BTreeMap<String, BranchColumnValue> } */
typedef struct jazz_model_BranchSelector { jz_btree values; } jazz_model_BranchSelector;
void jazz_model__model__branch__BranchSelector__default(jazz_model_BranchSelector *out);
void jazz_model__model__branch__BranchSelector__clone(void *dst, const void *src);
void jazz_model__model__branch__BranchSelector__drop(void *p);
bool jazz_model__model__branch__BranchSelector__eq(const jazz_model_BranchSelector *a, const jazz_model_BranchSelector *b);
typedef struct jazz_protocol_BranchViewBase { u32 tag; void *payload; } jazz_protocol_BranchViewBase;
void jazz_protocol__protocol__BranchViewBase__clone(void *dst, const void *src);
void jazz_protocol__protocol__BranchViewBase__drop(void *p);
bool jazz_protocol__protocol__BranchViewBase__eq(const jazz_protocol_BranchViewBase *a, const jazz_protocol_BranchViewBase *b);
typedef enum { JAZZ_RVS_CURRENT = 0, JAZZ_RVS_BRANCH_VIEW = 1, JAZZ_RVS_SNAPSHOT = 2, JAZZ_RVS_HISTORICAL = 3 } jazz_protocol_ReadViewSourceSpecTag;
typedef struct jazz_protocol_ReadViewSpec {
    u32 source_tag;
    jazz_model_BranchSelector head; bool has_base; jazz_protocol_BranchViewBase base; /* BranchView */
    void *snapshot;                                                                 /* Snapshot/other */
} jazz_protocol_ReadViewSpec;
void jazz_protocol__protocol__ReadViewSpec__default(jazz_protocol_ReadViewSpec *out);
void jazz_protocol__protocol__ReadViewSpec__clone(void *dst, const void *src);
void jazz_protocol__protocol__ReadViewSpec__drop(void *p);
bool jazz_protocol__protocol__ReadViewSpec__eq(const jazz_protocol_ReadViewSpec *a, const jazz_protocol_ReadViewSpec *b);
bool jazz_protocol__protocol__ReadViewSpec__is_default(const jazz_protocol_ReadViewSpec *s);
void jazz_protocol__protocol__ReadViewSpec__branch_view(jazz_protocol_ReadViewSpec *out, jazz_model_BranchSelector head, bool has_base, jazz_protocol_BranchViewBase base);
typedef enum { JAZZ_BINDING_SOURCE_ORDINARY = 0, JAZZ_BINDING_SOURCE_RELAY_AUTHORITY_SESSION = 1 } jazz_protocol_BindingSource;
typedef struct jazz_protocol_RegisterShapeOptions {
    jazz_model_DurabilityTier tier;
    jazz_protocol_ReadViewSpec read_view;
    u32 binding_source;
    bool include_deleted;
} jazz_protocol_RegisterShapeOptions;
void jazz_protocol__protocol__RegisterShapeOptions__default(jazz_protocol_RegisterShapeOptions *out);
void jazz_protocol__protocol__RegisterShapeOptions__clone(void *dst, const void *src);
void jazz_protocol__protocol__RegisterShapeOptions__drop(void *p);
bool jazz_protocol__protocol__RegisterShapeOptions__eq(const jazz_protocol_RegisterShapeOptions *a, const jazz_protocol_RegisterShapeOptions *b);
jazz_protocol_ReadViewKey jazz_protocol__protocol__RegisterShapeOptions__read_view_key(const jazz_protocol_RegisterShapeOptions *o);
typedef struct jazz_protocol_PolicyBindingKey { jazz_types_AuthorSubject identity; void *canonical_claims; } jazz_protocol_PolicyBindingKey;
void jazz_protocol__protocol__PolicyBindingKey__from_canonical_parts(jazz_protocol_PolicyBindingKey *out, jazz_types_AuthorSubject identity, jz_btree claims);
void jazz_protocol__protocol__PolicyBindingKey__clone(void *dst, const void *src);
void jazz_protocol__protocol__PolicyBindingKey__drop(void *p);
bool jazz_protocol__protocol__PolicyBindingKey__eq(const jazz_protocol_PolicyBindingKey *a, const jazz_protocol_PolicyBindingKey *b);
int jazz_protocol__protocol__PolicyBindingKey__cmp(const void *a, const void *b, void *ctx);
typedef struct jazz_protocol_CoverageKey {
    jazz_model_ShapeId shape_id; jazz_model_BindingId binding_id;
    jazz_protocol_RegisterShapeOptions opts;
    bool has_policy_binding; jazz_protocol_PolicyBindingKey policy_binding;
} jazz_protocol_CoverageKey;
int jazz_protocol__protocol__CoverageKey__cmp(const void *a, const void *b, void *ctx);
void jazz_protocol__protocol__CoverageKey__clone(void *dst, const void *src);
void jazz_protocol__protocol__CoverageKey__drop(void *p);
typedef struct jazz_protocol_AuthorityResultKey { jazz_protocol_BindingViewKey binding_view; bool has_scope; jazz_protocol_PolicyBindingKey scope; } jazz_protocol_AuthorityResultKey;
void jazz_protocol__protocol__AuthorityResultKey__unscoped(jazz_protocol_AuthorityResultKey *out, jazz_protocol_BindingViewKey key);
typedef struct jazz_protocol_DelegatedSessionBinding { jazz_types_AuthorSubject identity; jz_btree claims; } jazz_protocol_DelegatedSessionBinding;
void jazz_protocol__protocol__DelegatedSessionBinding__clone(void *dst, const void *src);
void jazz_protocol__protocol__DelegatedSessionBinding__drop(void *p);
typedef struct jazz_protocol_CurrentRowCoordinate {
    jazz_types_SchemaVersionId schema; jz_str table; jazz_types_GlobalPhysicalTableId physical_table; jazz_types_RowUuid row;
} jazz_protocol_CurrentRowCoordinate;
void jazz_protocol__protocol__CurrentRowCoordinate__clone(void *dst, const void *src);
void jazz_protocol__protocol__CurrentRowCoordinate__drop(void *p);
bool jazz_protocol__protocol__CurrentRowCoordinate__eq(const jazz_protocol_CurrentRowCoordinate *a, const jazz_protocol_CurrentRowCoordinate *b);
typedef enum { JAZZ_CRO_READABLE, JAZZ_CRO_CURRENT_UNAVAILABLE, JAZZ_CRO_UNKNOWN } jazz_protocol_CurrentRowOutcome;
typedef struct jazz_protocol_VersionCarrier { u32 tag; void *payload; } jazz_protocol_VersionCarrier;
void jazz_protocol__protocol__VersionCarrier__clone(void *dst, const void *src);
void jazz_protocol__protocol__VersionCarrier__drop(void *p);
typedef struct jazz_protocol_VersionBundle { void *tx; jz_vec versions; u32 scope; jazz_model_Fate fate; bool has_global_time; u64 global_time; u32 durability; } jazz_protocol_VersionBundle;
void jazz_protocol__protocol__VersionBundle__clone(void *dst, const void *src);
void jazz_protocol__protocol__VersionBundle__drop(void *p);
void jazz_protocol__protocol__VersionBundle__serialize(const jazz_protocol_VersionBundle *b, jz_pc_w *w);
typedef struct jazz_protocol_CurrentRowsRequest {
    jazz_protocol_PermissionAdviceRequestId request_id;
    JZ_VEC(jazz_protocol_CurrentRowCoordinate) rows;
    bool has_delegated_session; jazz_protocol_DelegatedSessionBinding delegated_session;
} jazz_protocol_CurrentRowsRequest;
typedef struct jazz_protocol_CurrentRowsReceipt {
    jazz_protocol_PermissionAdviceRequestId request_id;
    JZ_VEC(jazz_protocol_CurrentRowCoordinate) rows;
    JZ_VEC(u32) outcomes;
    jazz_protocol_PolicyBindingKey context;
    jazz_types_NodeUuid core; u64 core_epoch; u64 claims_revision; u64 policy_epoch;
    jazz_types_GlobalTime settled_through; u64 authorization_progress;
    JZ_VEC(jazz_protocol_VersionCarrier) version_carriers;
} jazz_protocol_CurrentRowsReceipt;
typedef struct jazz_protocol_PeerPayloadInventory { JZ_VEC(jazz_model_TxId) complete_tx_payloads; bool has_authorization_progress; u64 authorization_progress; jz_vec extra; } jazz_protocol_PeerPayloadInventory;
void jazz_protocol__protocol__PeerPayloadInventory__default(jazz_protocol_PeerPayloadInventory *out);
void jazz_protocol__protocol__PeerPayloadInventory__clone(void *dst, const void *src);
void jazz_protocol__protocol__PeerPayloadInventory__drop(void *p);
typedef struct jazz_protocol_SupportingRow { jazz_types_GlobalPhysicalTableId physical_table; const jz_str *version_table; jazz_types_RowUuid row; u8 version[48]; } jazz_protocol_SupportingRow;
void jazz_protocol__protocol__SupportingRow__clone(void *dst, const void *src);
void jazz_protocol__protocol__SupportingRow__serialize(const jazz_protocol_SupportingRow *r, jz_pc_w *w);
int jazz_protocol__protocol__SupportingRow__cmp(const void *a, const void *b, void *ctx);
typedef enum { JAZZ_SRU_SNAPSHOT = 0, JAZZ_SRU_DELTA = 1, JAZZ_SRU_CATCH_UP = 2 } jazz_protocol_SupportingRowsUpdateTag;
typedef JZ_VEC(jazz_protocol_SupportingRow) jazz_protocol_SupportingRowVec;
typedef struct jazz_protocol_SupportingRowsUpdate {
    u32 tag;
    u8 predecessor[16]; u8 revision[16];
    jazz_protocol_SupportingRowVec first;  /* rows / adds / changed */
    jazz_protocol_SupportingRowVec second; /* - / removes / left */
} jazz_protocol_SupportingRowsUpdate;
typedef struct jazz_protocol_ViewUpdatePayload {
    jazz_protocol_SubscriptionKey subscription;
    jazz_types_GlobalTime settled_through;
    JZ_VEC(jazz_protocol_VersionCarrier) version_carriers;
    jazz_protocol_PeerPayloadInventory peer_payload_inventory;
    jazz_protocol_SupportingRowsUpdate supporting_rows;
} jazz_protocol_ViewUpdatePayload;
void jazz_protocol__protocol__ViewUpdatePayload__clone(void *dst, const void *src);
void jazz_protocol__protocol__ViewUpdatePayload__drop(void *p);
u32 jazz_protocol__protocol__ViewUpdatePayload__serialized_size(const jazz_protocol_ViewUpdatePayload *v, bool *ok);
jz_err jazz_protocol__protocol__expand_version_carriers(const jazz_protocol_VersionCarrier *c, u32 n, JZ_VEC(jazz_protocol_VersionBundle) *out);
jz_err jazz_protocol__protocol__build_version_carriers_from_singletons(jazz_protocol_VersionBundle *bundles, u32 n, JZ_VEC(jazz_protocol_VersionCarrier) *out);
void jazz_protocol__protocol__VersionCarrier__bundle(jazz_protocol_VersionCarrier *out, jazz_protocol_VersionBundle b);
typedef enum { JAZZ_CHUNK_FOUND = 0, JAZZ_CHUNK_UNAVAILABLE = 1, JAZZ_CHUNK_RETRYABLE = 2 } jazz_protocol_ChunkResponseTag;
typedef struct jazz_protocol_ChunkResponse { u32 tag; jz_bytes found; u32 retry_after_ms; } jazz_protocol_ChunkResponse;
typedef struct jazz_protocol_ChunkRequestEntry { u64 request_id; jz_bytes locator; u8 expected_hash[32]; u8 remaining_hops; } jazz_protocol_ChunkRequestEntry;
typedef struct jazz_protocol_ChunkResponseEntry { u64 request_id; jazz_protocol_ChunkResponse result; } jazz_protocol_ChunkResponseEntry;
void jazz_protocol__protocol__ChunkResponse__clone(void *dst, const void *src);
void jazz_protocol__protocol__ChunkResponse__drop(void *p);
void jazz_protocol__protocol__ChunkRequestEntry__drop(void *p);
void jazz_protocol__protocol__ChunkResponseEntry__drop(void *p);
void jazz_protocol__protocol__ChunkResponseEntry__clone(void *dst, const void *src);
bool jazz_protocol__protocol__ChunkResponseEntry__eq(const jazz_protocol_ChunkResponseEntry *a, const jazz_protocol_ChunkResponseEntry *b);
typedef enum {
    JAZZ_SRR_UNSUPPORTED_SHAPE_CAPABILITY = 0, JAZZ_SRR_SHAPE_REGISTRATION_PENDING_CATALOGUE_ADMISSION = 1,
    JAZZ_SRR_SERVER_FAILURE = 2, JAZZ_SRR_OTHER = 3
} jazz_protocol_SubscribeRejectReasonTag;
typedef enum {
    JAZZ_SSFC_TABLE_NOT_FOUND, JAZZ_SSFC_QUERY_VALIDATION, JAZZ_SSFC_QUERY_LOWERING, JAZZ_SSFC_POLICY_EVALUATION,
    JAZZ_SSFC_SCHEMA_RESOLUTION, JAZZ_SSFC_INTERNAL
} jazz_protocol_SubscribeServerFailureCode;
typedef struct jazz_protocol_SubscribeRejectReason { u32 tag; jz_str detail; u32 code; } jazz_protocol_SubscribeRejectReason;
void jazz_protocol__protocol__SubscribeRejectReason__clone(void *dst, const void *src);
void jazz_protocol__protocol__SubscribeRejectReason__drop(void *p);
/* SyncMessage (protocol.rs:36). Variant order = serde index. */
typedef enum {
    JAZZ_SM_CHUNK_REQUEST_BATCH, JAZZ_SM_CHUNK_RESPONSE_BATCH, JAZZ_SM_SESSION_CLAIMS, JAZZ_SM_COMMIT_UNIT,
    JAZZ_SM_FATE_UPDATE, JAZZ_SM_REGISTER_SHAPE, JAZZ_SM_SUBSCRIBE, JAZZ_SM_SUBSCRIBE_REJECTED, JAZZ_SM_UNSUBSCRIBE,
    JAZZ_SM_PUBLISH_SCHEMA, JAZZ_SM_PUBLISH_SCHEMA_WITH_LENS, JAZZ_SM_PUBLISH_LENS, JAZZ_SM_CATALOGUE_ACK,
    JAZZ_SM_VIEW_UPDATE, JAZZ_SM_RESERVED14, JAZZ_SM_RESERVED15, JAZZ_SM_CATALOGUE_SNAPSHOT,
    JAZZ_SM_AUTHORIZATION_SCOPE_INTENT, JAZZ_SM_AUTHORIZATION_SCOPE_VIEW, JAZZ_SM_AUTHORIZATION_SCOPE_AGGREGATE_RECEIPT,
    JAZZ_SM_AUTHORIZATION_SCOPE_UNAVAILABLE, JAZZ_SM_AUTHORIZATION_SCOPE_DECISION, JAZZ_SM_CHUNK_UPLOAD_START,
    JAZZ_SM_CHUNK_UPLOAD_NODES, JAZZ_SM_CHUNK_UPLOAD_RESULT, JAZZ_SM_CURRENT_ROWS_REQUEST, JAZZ_SM_CURRENT_ROWS_RECEIPT,
    JAZZ_SM_CURRENT_ROWS_CANCEL, JAZZ_SM_VIEW_UPDATE_PART, JAZZ_SM_RESERVED29, JAZZ_SM_READ_BARRIER,
} jazz_protocol_SyncMessageTag;
typedef struct jazz_protocol_Transaction { jazz_model_TxId tx_id; u64 n_total_writes; void *rest; } jazz_protocol_Transaction;
typedef struct jazz_protocol_SyncMessage {
    u32 tag;
    union {
        JZ_VEC(jazz_protocol_ChunkRequestEntry) chunk_requests;    /* ChunkRequestBatch */
        JZ_VEC(jazz_protocol_ChunkResponseEntry) chunk_responses;  /* ChunkResponseBatch */
        struct { jazz_types_AuthorSubject identity; jz_btree claims; } session_claims;
        struct { jazz_protocol_Transaction tx; jz_vec versions; } commit_unit;
        struct { jazz_model_TxId tx_id; jazz_model_Fate fate; bool has_global_time; u64 global_time; bool has_durability; u32 durability; } fate_update;
        struct { jazz_protocol_SubscriptionKey subscription; jazz_protocol_SubscribeRejectReason reason; } subscribe_rejected;
        struct { jazz_protocol_SubscriptionKey subscription; } unsubscribe;
        jazz_protocol_ViewUpdatePayload view_update;               /* ViewUpdate / ViewUpdatePart */
        struct { jazz_protocol_PermissionAdviceRequestId request_id; void *rest; } scope;  /* AuthorizationScope* */
        struct { jazz_protocol_PermissionAdviceRequestId request_id; jazz_protocol_PermissionAdviceAction action; bool has_delegated; jazz_protocol_DelegatedSessionBinding delegated; } scope_intent;
        struct { u8 object_hash[32]; void *rest; } chunk_upload;   /* value_ref.root.object_hash */
        jazz_protocol_CurrentRowsRequest current_rows_request;
        jazz_protocol_CurrentRowsReceipt current_rows_receipt;
        struct { jazz_protocol_PermissionAdviceRequestId request_id; } current_rows_cancel;
        struct { u64 writes_through; } read_barrier;
        void *boxed;                                               /* PublishSchema* / CatalogueSnapshot / ... */
    } v;
} jazz_protocol_SyncMessage;
void jazz_protocol__protocol__SyncMessage__clone(void *dst, const void *src);
void jazz_protocol__protocol__SyncMessage__drop(void *p);
bool jazz_protocol__protocol__SyncMessage__eq(const jazz_protocol_SyncMessage *a, const jazz_protocol_SyncMessage *b);
void jazz_protocol__protocol__SyncMessage__debug(jz_str *out, const jazz_protocol_SyncMessage *m);
#define JAZZ_PROTOCOL_MAX_CHUNK_REQUEST_BATCH_ENTRIES 4u
#define JAZZ_PROTOCOL_MAX_LOGICAL_MESSAGE_BYTES (256u * 1024u * 1024u)
#define JAZZ_PROTOCOL_MAX_CURRENT_ROWS 64u
jz_err jazz_protocol__protocol_limits__validate_wire_frame_len(u32 len);

/* ---- jazz_protocol::wire ---- */
typedef enum { JAZZ_TE_BACKPRESSURE = 1, JAZZ_TE_FAILED = 2, JAZZ_TE_CLOSED = 3 } jazz_protocol_TransportErrorTag;
/* TransportError { Backpressure, Failed(String), Closed } as jz_err: code = tag, msg = Failed text */
typedef enum { JAZZ_WEC_MALFORMED_FRAME, JAZZ_WEC_INTERNAL, JAZZ_WEC_UNSUPPORTED, JAZZ_WEC_OTHER } jazz_protocol_WireErrorCode;
typedef enum { JAZZ_WR_NEVER, JAZZ_WR_AFTER_RECONNECT } jazz_protocol_WireRetry;
typedef struct jazz_protocol_WireError { u32 code; u32 retry; jz_str message; } jazz_protocol_WireError;
void jazz_protocol__wire__WireError__new(jazz_protocol_WireError *out, u32 code, u32 retry, jz_str message);
void jazz_protocol__wire__WireError__clone(void *dst, const void *src);
void jazz_protocol__wire__WireError__drop(void *p);
void jazz_protocol__wire__WireError__debug(jz_str *out, const jazz_protocol_WireError *e);
/* WireError as jz_err (Result<_, WireError>) */
enum { JAZZ_WIRE_ERROR_CODE = 0x300 };
jazz_protocol_WireError *jazz_protocol__wire__WireError__from_err(jz_err e);
jz_err jazz_protocol__wire__WireError__into_err(jazz_protocol_WireError e);
typedef struct jazz_protocol_WireFeatures { u64 bits; } jazz_protocol_WireFeatures;
typedef struct jazz_protocol_WireSession jazz_protocol_WireSession;
typedef struct jazz_protocol_WireInboundContext jazz_protocol_WireInboundContext; /* opaque, cloneable */
jazz_protocol_WireInboundContext *jazz_protocol__wire__WireInboundContext__new(u16 protocol_version, jazz_protocol_WireFeatures features, jazz_protocol_WireSession *session);
jazz_protocol_WireInboundContext *jazz_protocol__wire__WireInboundContext__clone(const jazz_protocol_WireInboundContext *c);
void jazz_protocol__wire__WireInboundContext__drop(jazz_protocol_WireInboundContext *c);
void jazz_protocol__wire__WireInboundContext__set_trusted_encoder(jazz_protocol_WireInboundContext *c, bool trusted);
jazz_protocol_WireFeatures jazz_protocol__wire__WireInboundContext__negotiated_features(const jazz_protocol_WireInboundContext *c);
bool jazz_protocol__wire__WireInboundContext__validates_receipts(const jazz_protocol_WireInboundContext *c);
jz_err jazz_protocol__wire__WireInboundContext__decode_semantic_payload(const jazz_protocol_WireInboundContext *c, jz_slice payload, jazz_protocol_SyncMessage *out);
typedef enum { JAZZ_CC_CONTROL, JAZZ_CC_REQUESTS, JAZZ_CC_WRITES, JAZZ_CC_DELIVERY, JAZZ_CC_LARGE_VALUE, JAZZ_CC_PROGRESS, JAZZ_CC_AUXILIARY } jazz_protocol_ChannelClass;
#define JAZZ_WIRE_MAX_CHANNELS 64u
#define JAZZ_WIRE_MAX_CHANNEL_QUEUED_MESSAGES 1024u
#define JAZZ_WIRE_CONTROL_RESERVE_MESSAGES 16u
#define JAZZ_WIRE_AUXILIARY_CHANNEL 63u
#define JAZZ_WIRE_PROGRESS_CHANNEL 62u
#define JAZZ_WIRE_PROTOCOL_VERSION 3u
typedef struct jazz_protocol_WireChannelExtent { u16 channel; u32 class_; u64 generation; } jazz_protocol_WireChannelExtent;
typedef struct jazz_protocol_WireChannelEnvelope { jazz_protocol_WireChannelExtent extent; jz_bytes payload; } jazz_protocol_WireChannelEnvelope;
void jazz_protocol__wire__WireChannelEnvelope__drop(jazz_protocol_WireChannelEnvelope *e);
typedef enum { JAZZ_WF_CHANNEL, JAZZ_WF_CHANNEL_CREDIT, JAZZ_WF_ERROR, JAZZ_WF_OTHER } jazz_protocol_WireFrameTag;
typedef struct jazz_protocol_ChannelCreditGrant { u8 b[24]; } jazz_protocol_ChannelCreditGrant;
typedef struct jazz_protocol_WireFrame { u32 tag; jazz_protocol_WireChannelEnvelope channel; jazz_protocol_ChannelCreditGrant credit; jazz_protocol_WireError error; } jazz_protocol_WireFrame;
void jazz_protocol__wire__WireFrame__drop(jazz_protocol_WireFrame *f);
jz_err jazz_protocol__wire__WireInboundContext__decode_frame(const jazz_protocol_WireInboundContext *c, jz_slice frame, jazz_protocol_WireFrame *out);
jz_err jazz_protocol__wire__WireInboundContext__validate_channel_metadata(const jazz_protocol_WireInboundContext *c, const jazz_protocol_WireChannelEnvelope *e);
jz_err jazz_protocol__wire__encode_frame(const jazz_protocol_WireFrame *f, jz_bytes *out);
jz_err jazz_protocol__wire__encode_sync_message_for_features(const jazz_protocol_SyncMessage *m, jazz_protocol_WireFeatures f, jz_bytes *out);
jz_err jazz_protocol__wire__encode_sync_message(const jazz_protocol_SyncMessage *m, jz_bytes *out);
jz_err jazz_protocol__wire__decode_sync_message(jz_slice payload, jazz_protocol_SyncMessage *out);
jz_err jazz_protocol__wire__decode_sync_message_trusted(jz_slice payload, jazz_protocol_SyncMessage *out);
jz_err jazz_protocol__wire__ensure_sync_message_features(const jazz_protocol_SyncMessage *m, jazz_protocol_WireFeatures f);
bool jazz_protocol__wire__encoded_sync_message_len(const jazz_protocol_SyncMessage *m, u32 *out);
jazz_protocol_WireFeatures jazz_protocol__wire__current_wire_features(void);
/* WireTransport trait (byte frames) as a vtable */
typedef struct jazz_protocol_WireTransportVt {
    jz_err (*send_frame)(void *self, jz_bytes frame);
    bool (*try_recv_frame)(void *self, jz_bytes *out);
    void (*drop)(void *self);
} jazz_protocol_WireTransportVt;
typedef struct jazz_protocol_WireTransportDyn { void *self; const jazz_protocol_WireTransportVt *vt; } jazz_protocol_WireTransportDyn;
/* wire::channel_credit */
typedef struct jazz_protocol_BufferLease { void *rc; } jazz_protocol_BufferLease; /* Option<BufferLease>: rc == NULL */
void jazz_protocol__wire__channel_credit__BufferLease__drop(jazz_protocol_BufferLease *l);
typedef struct jazz_protocol_ChannelCredits jazz_protocol_ChannelCredits;
/* SharedChannelCredits = Arc<Mutex<ChannelCredits>> */
typedef struct jazz_protocol_SharedChannelCredits { bool poisoned; jazz_protocol_ChannelCredits *inner; } jazz_protocol_SharedChannelCredits;
jazz_protocol_SharedChannelCredits *jazz_protocol__wire__channel_credit__SharedChannelCredits__clone(jazz_protocol_SharedChannelCredits *c);
void jazz_protocol__wire__channel_credit__SharedChannelCredits__release(jazz_protocol_SharedChannelCredits *c);
jz_err jazz_protocol__wire__channel_credit__ChannelCredits__peek_grant(jazz_protocol_ChannelCredits *c, bool *has, jz_bytes *frame);
jz_err jazz_protocol__wire__channel_credit__ChannelCredits__accept_grant(jazz_protocol_ChannelCredits *c);
jz_err jazz_protocol__wire__channel_credit__ChannelCredits__receive_credit(jazz_protocol_ChannelCredits *c, jazz_protocol_ChannelCreditGrant grant);
bool jazz_protocol__wire__channel_credit__ChannelCredits__has_pending_grant(const jazz_protocol_ChannelCredits *c);
bool jazz_protocol__wire__channel_credit__ChannelCredits__can_send(const jazz_protocol_ChannelCredits *c, u32 class_);
void jazz_protocol__wire__channel_credit__ChannelCredits__close(jazz_protocol_ChannelCredits *c);
void jazz_protocol__wire__channel_credit__ChannelCredits__register_waker(jazz_protocol_ChannelCredits *c, const jz_waker *w);
bool jazz_protocol__wire__channel_credit__ChannelCredits__poll_grant_ready(jazz_protocol_ChannelCredits *c, const jz_waker *w);
/* wire::stream_backend::OrderedChannelBackend */
typedef struct jazz_protocol_OrderedChannelBackend jazz_protocol_OrderedChannelBackend;
typedef struct jazz_protocol_ReceivedChannelPayload { u16 channel; u32 class_; jz_bytes payload; jazz_protocol_BufferLease lease; } jazz_protocol_ReceivedChannelPayload;
jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__new(jazz_protocol_WireInboundContext *ctx, jazz_protocol_OrderedChannelBackend **out);
void jazz_protocol__wire__stream_backend__OrderedChannelBackend__drop(jazz_protocol_OrderedChannelBackend *b);
bool jazz_protocol__wire__stream_backend__OrderedChannelBackend__incomplete_receive_timeout_ms(const jazz_protocol_OrderedChannelBackend *b, u64 *out);
jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__expire(jazz_protocol_OrderedChannelBackend *b);
jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__enqueue(jazz_protocol_OrderedChannelBackend *b, u16 channel, u64 generation, u32 class_, jz_bytes bytes);
jazz_protocol_SharedChannelCredits *jazz_protocol__wire__stream_backend__OrderedChannelBackend__channel_credits(const jazz_protocol_OrderedChannelBackend *b);
void jazz_protocol__wire__stream_backend__OrderedChannelBackend__set_channel_credits(jazz_protocol_OrderedChannelBackend *b, jazz_protocol_SharedChannelCredits *c);
bool jazz_protocol__wire__stream_backend__OrderedChannelBackend__is_idle(const jazz_protocol_OrderedChannelBackend *b, u16 channel);
jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__next_idle_generation(const jazz_protocol_OrderedChannelBackend *b, u16 channel, u64 *out);
bool jazz_protocol__wire__stream_backend__OrderedChannelBackend__has_pending(const jazz_protocol_OrderedChannelBackend *b);
jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__peek_outbound(jazz_protocol_OrderedChannelBackend *b, bool *has, jz_bytes *out);
jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__accept_outbound(jazz_protocol_OrderedChannelBackend *b, bool *completed);
jz_err jazz_protocol__wire__stream_backend__OrderedChannelBackend__receive(jazz_protocol_OrderedChannelBackend *b, jazz_protocol_WireChannelEnvelope frame, u32 encoded_len, bool *has, jazz_protocol_ReceivedChannelPayload *out);
typedef struct jazz_protocol_WireAuthorityEndpoint { jazz_types_NodeUuid node; u64 epoch; } jazz_protocol_WireAuthorityEndpoint;



/* ---- groove::chunks ---- */
typedef struct groove_ChunkRequest { u8 object_hash[32]; jz_bytes locator; } groove_ChunkRequest;
int groove__chunks__ChunkRequest__cmp(const void *a, const void *b, void *ctx);
void groove__chunks__ChunkRequest__clone(void *dst, const void *src);
void groove__chunks__ChunkRequest__drop(void *p);
typedef enum { GROOVE_CHUNK_ERR_UNAVAILABLE, GROOVE_CHUNK_ERR_RETRYABLE, GROOVE_CHUNK_ERR_BACKEND } groove_ChunkErrorTag;
typedef struct groove_ChunkError { u32 tag; u32 retry_after_ms; jz_str backend; } groove_ChunkError;
/* Result<Bytes, ChunkError> */
typedef struct groove_ChunkResult { bool ok; jz_bytes bytes; groove_ChunkError err; } groove_ChunkResult;
typedef enum { GROOVE_CSE_UNAVAILABLE, GROOVE_CSE_LOCATOR_CONFLICT, GROOVE_CSE_INTEGRITY, GROOVE_CSE_BACKEND } groove_ChunkStorageErrorTag;
typedef struct groove_ChunkStorageError { u32 tag; jz_str backend; } groove_ChunkStorageError;
typedef struct groove_LocalChunkReader { void *rc; } groove_LocalChunkReader; /* Rc-backed, cloneable */
groove_LocalChunkReader groove__chunks__LocalChunkReader__clone(const groove_LocalChunkReader *r);
void groove__chunks__LocalChunkReader__drop(groove_LocalChunkReader *r);
/* async get -> synchronous: true + bytes on Ok, false + err on Err */
bool groove__chunks__LocalChunkReader__get(const groove_LocalChunkReader *r, jz_bytes locator, const u8 hash[32], jz_bytes *out, groove_ChunkStorageError *err);
/* MissingChunkResolver trait object */
typedef struct groove_ChunkFutureVt { bool (*poll)(void *self, const jz_waker *cx, groove_ChunkResult *out); void (*drop)(void *self); } groove_ChunkFutureVt;
typedef struct groove_ChunkFuture { void *self; const groove_ChunkFutureVt *vt; } groove_ChunkFuture;
typedef struct groove_MissingChunkResolverVt { groove_ChunkFuture (*resolve)(const void *self, groove_ChunkRequest request); void (*drop)(void *self); } groove_MissingChunkResolverVt;
#define JAZZ_PROTOCOL_MAX_CHUNK_REQUEST_BATCH_ENTRIES 4u


/* ---- jazz_node::api_error / node errors / NodeState ---- */
/* api_error::Error { code: ErrorCode, message: String } is a jz_err whose code is
 * JAZZ_EC_* (db.h). crate::node::Error is a jz_err with JAZZ_NODE_ERR_* codes. */
enum { JAZZ_NODE_ERR_MISSING_TRANSACTION = 0x500, JAZZ_NODE_ERR_OTHER = 0x501 };
jz_err jazz_node__node__api_error__Error__from_node_error(jz_err node_error);
const char *jazz_node__node__api_error__ErrorCode__as_str(u32 code);
typedef struct jazz_node_NodeState jazz_node_NodeState;
bool jazz_node__node__NodeState__transaction_state(jazz_node_NodeState *n, jazz_model_TxId tx, jazz_model_Fate *fate, bool *has_global_time, u64 *global_time, u32 *durability);
jz_err jazz_node__node__NodeState__commit_unit_for(jazz_node_NodeState *n, jazz_model_TxId tx, jazz_protocol_SyncMessage *out);
jz_err jazz_node__node__NodeState__pending_transaction_ids_for_author(jazz_node_NodeState *n, jazz_types_AuthorSubject author, JZ_VEC(jazz_model_TxId) *out);
jz_err jazz_node__node__NodeState__reserve_tx_time_after(jazz_node_NodeState *n, jazz_types_TxTime high_water);
jazz_types_TxTime jazz_node__node__NodeState__tx_time_high_water(const jazz_node_NodeState *n);
/* RejectedTransaction */
typedef struct jazz_model_RejectedTransaction jazz_model_RejectedTransaction;
jazz_model_TxId jazz_model__tx__RejectedTransaction__tx_id(const jazz_model_RejectedTransaction *r);
u32 jazz_model__tx__RejectedTransaction__kind(const jazz_model_RejectedTransaction *r);
void jazz_model__tx__RejectedTransaction__reason(const jazz_model_RejectedTransaction *r, jazz_model_RejectionReason *out);
void jazz_model__tx__RejectedTransaction__drop(jazz_model_RejectedTransaction *r);
void jazz_model__model__public_schema__TransactionId__from_committed_tx(jazz_model_TransactionId *out, jazz_model_TxId tx);
typedef enum { JAZZ_TXKIND_MERGEABLE = 0, JAZZ_TXKIND_EXCLUSIVE = 1 } jazz_model_TxKindTag;

/* ---- jazz_model::schema (JazzSchema / TableSchema / ColumnSchema) ---- */
typedef struct jazz_model_ColumnSchema { jz_str name; groove_ColumnType column_type; bool has_default; groove_Value default_; } jazz_model_ColumnSchema;
typedef struct jazz_model_TableSchema {
    jz_str name;
    JZ_VEC(jazz_model_ColumnSchema) columns;
    void *references; void *merge_strategies; void *indexed_columns; void *composite_indexes;
} jazz_model_TableSchema;
bool jazz_model__schema__TableSchema__references_eq(const jazz_model_TableSchema *a, const jazz_model_TableSchema *b);
bool jazz_model__schema__TableSchema__merge_strategies_eq(const jazz_model_TableSchema *a, const jazz_model_TableSchema *b);
bool jazz_model__schema__TableSchema__indexed_columns_eq(const jazz_model_TableSchema *a, const jazz_model_TableSchema *b);
bool jazz_model__schema__TableSchema__composite_indexes_eq(const jazz_model_TableSchema *a, const jazz_model_TableSchema *b);
typedef struct jazz_model_JazzSchema { JZ_VEC(jazz_model_TableSchema) tables; void *rest; } jazz_model_JazzSchema;
void jazz_model__schema__JazzSchema__clone(void *dst, const void *src);
void jazz_model__schema__JazzSchema__drop(void *p);
jazz_types_SchemaVersionId jazz_model__schema__JazzSchema__version_id(const jazz_model_JazzSchema *s);
/* serde_json::to_vec(schema.public_schema()) */
jz_err jazz_model__schema__JazzSchema__public_schema_json(const jazz_model_JazzSchema *s, jz_bytes *out);
/* protocol lenses */
typedef enum { JAZZ_LENS_ADD_COLUMN, JAZZ_LENS_DROP_COLUMN } jazz_protocol_LensOpTag;
typedef struct jazz_protocol_LensOp { u32 tag; jz_str column; groove_Value default_; } jazz_protocol_LensOp;
typedef struct jazz_protocol_TableLens { jz_str source_table; jz_str target_table; JZ_VEC(jazz_protocol_LensOp) ops; } jazz_protocol_TableLens;
typedef struct jazz_protocol_MigrationLens jazz_protocol_MigrationLens;
jz_err jazz_protocol__protocol__MigrationLens__new(jazz_types_SchemaVersionId source, jazz_types_SchemaVersionId target, jazz_protocol_TableLens *lenses, u32 n, jazz_protocol_MigrationLens **out);
/* blake3 (vendored C library) key derivation */
void jazz_third_party__blake3__derive_key(const char *context, const u8 *p, usize n, u8 out[32]);

/* ---- jazz_db::db::node_runtime (unit db-a) mirrors ---- */
struct jazz_db_ReceivedSyncMessage;
struct jazz_db_SharedAuxiliaryEndpoint;
/* ConnectionSessionContext (node_runtime.rs) */
typedef struct jazz_db_ConnectionSessionContext {
    jazz_protocol_WireAuthorityEndpoint local;
    bool has_remote; jazz_protocol_WireAuthorityEndpoint remote;
    jazz_types_AuthorSubject link_identity;
    jazz_protocol_WireFeatures negotiated_features;
} jazz_db_ConnectionSessionContext;
/* Transport trait (node_runtime.rs) as vtable. TransportError travels as jz_err (JAZZ_TE_*). */
typedef struct jazz_db_TransportVt {
    jz_err (*send)(void *self, jazz_protocol_SyncMessage message);
    jz_err (*try_send)(void *self, jazz_protocol_SyncMessage message, bool *returned, jazz_protocol_SyncMessage *back);
    bool (*try_recv)(void *self, jazz_protocol_SyncMessage *out);
    jz_err (*try_recv_result)(void *self, bool *has, jazz_protocol_SyncMessage *out);
    jz_err (*try_recv_owned_result)(void *self, bool *has, struct jazz_db_ReceivedSyncMessage *out);
    jz_err (*poll_flush)(void *self, u32 *status);
    bool (*has_terminal_failure)(const void *self);
    bool (*remote_wire_error)(const void *self, jazz_protocol_WireError *out);
    bool (*incomplete_receive_timeout_ms)(const void *self, u64 *out);
    struct jazz_db_SharedAuxiliaryEndpoint *(*shared_auxiliary_endpoint)(const void *self);
    void (*set_trusted_encoder)(void *self, bool trusted);
    jazz_protocol_WireInboundContext *(*wire_inbound_context)(const void *self);
    bool (*connection_session_context)(const void *self, jazz_db_ConnectionSessionContext *out);
    bool (*permits_delegated_sessions)(const void *self);
    void (*drop)(void *self);
} jazz_db_TransportVt;
typedef struct jazz_db_Transport { void *self; const jazz_db_TransportVt *vt; } jazz_db_Transport;

/* Result<_, String> errors raised by jazz_db (and its bindings) */
enum { JAZZ_DB_STRING_ERROR = 0x200 };

/* String -> Value map (RowCells / claims): one shared btree desc */
extern const jz_btree_desc jazz_db_str_value_desc;


/* ---- db.rs part 3 externs: query model / node / peer / terminal ---- */
jazz_model_ValidatedQuery *jazz_model__query__ValidatedQuery__clone(const jazz_model_ValidatedQuery *q);
void jazz_model__query__ValidatedQuery__drop(jazz_model_ValidatedQuery *q);
bool jazz_model__query__ValidatedQuery__eq(const jazz_model_ValidatedQuery *a, const jazz_model_ValidatedQuery *b);
const jazz_model_Query *jazz_model__query__ValidatedQuery__query(const jazz_model_ValidatedQuery *q);
jazz_model_ShapeId jazz_model__query__ValidatedQuery__shape_id(const jazz_model_ValidatedQuery *q);
jazz_model_Binding *jazz_model__query__Binding__clone(const jazz_model_Binding *b);
void jazz_model__query__Binding__drop(jazz_model_Binding *b);
bool jazz_model__query__Binding__eq(const jazz_model_Binding *a, const jazz_model_Binding *b);
jazz_model_BindingId jazz_model__query__Binding__binding_id(const jazz_model_Binding *b);
/* Query fields */
typedef struct jazz_model_ArraySubquery jazz_model_ArraySubquery;
struct jazz_model_ArraySubquery { jz_str column_name; jz_str table; JZ_VEC(jazz_model_ArraySubquery) nested_arrays; };
const jazz_model_ArraySubquery *jazz_model__query__Query__array_subqueries(const jazz_model_Query *q, u32 *n);
u32 jazz_model__query__Query__joins_len(const jazz_model_Query *q);
u32 jazz_model__query__Query__reachable_len(const jazz_model_Query *q);
bool jazz_model__query__Query__has_flat_join(const jazz_model_Query *q);
bool jazz_model__query__Query__has_offset(const jazz_model_Query *q);
/* schema helpers */
jz_err jazz_model__schema__JazzSchema__project_branch_selector(const jazz_model_JazzSchema *s, const jazz_model_TableSchema *table, const jazz_model_BranchSelector *branch, jz_btree *selector_out, jz_btree *cells_out);
bool jazz_model__model__branch__BranchSelector__is_root(const jazz_model_BranchSelector *b);

/* jazz_node: ingest trust, peers */
typedef enum { JAZZ_CUT_SESSION, JAZZ_CUT_RELAY, JAZZ_CUT_TRUSTED_BACKEND, JAZZ_CUT_TRUSTED_AUTHORITY, JAZZ_CUT_TRUSTED_ADMIN } jazz_node_CommitUnitTrust;
typedef struct jazz_node_CommitUnitIngestContext {
    jazz_types_AuthorSubject identity; u32 trust; bool admitted_write_authorization; bool version_receipts_validated;
} jazz_node_CommitUnitIngestContext;
bool jazz_node__node__CommitUnitTrust__is_trusted(u32 trust);
typedef enum { JAZZ_PEER_ROLE_RELAY, JAZZ_PEER_ROLE_CLIENT_LINK } jazz_node_PeerRoleTag;
typedef struct jazz_node_PeerRole { u32 tag; jazz_types_AuthorSubject identity; } jazz_node_PeerRole;
typedef struct jazz_node_PeerState jazz_node_PeerState;
jazz_node_PeerRole jazz_node__peer__PeerState__role(const jazz_node_PeerState *p);
bool jazz_node__peer__PeerState__rejects_raw_session_claims(const jazz_node_PeerState *p);
bool jazz_node__peer__PeerState__admits_relay_binding(const jazz_node_PeerState *p, const jazz_types_AuthorSubject *identity, const jz_btree *claims);
bool jazz_node__peer__PeerState__authority_query_delegate(const jazz_node_PeerState *p);
/* (AuthorSubject, BTreeMap<String, Value>) */
typedef struct jazz_db_PolicyBinding { jazz_types_AuthorSubject author; jz_btree claims; } jazz_db_PolicyBinding;

/* jazz_node::NodeState (more) */
jz_err jazz_node__node__NodeState__visible_current_cells_in_branch(jazz_node_NodeState *n, jz_slice table, const jazz_model_BranchSelector *branch, jazz_types_RowUuid row, bool *has, jz_btree *out);
jz_err jazz_node__node__NodeState__relation_snapshot_has_materialized_required_cells(jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape, const jazz_node_RelationSnapshot *snapshot, bool *out);
bool jazz_node__node__NodeState__has_settled_authority_result(const jazz_node_NodeState *n, const jazz_protocol_AuthorityResultKey *key);
bool jazz_node__node__NodeState__opening_pending_for_authority_result(const jazz_node_NodeState *n, const jazz_protocol_AuthorityResultKey *key);
typedef struct jazz_node_ActiveSessionClaimsScope jazz_node_ActiveSessionClaimsScope;
jazz_node_ActiveSessionClaimsScope *jazz_node__node__NodeState__scoped_optional_session_claims(jazz_node_NodeState *n, jazz_types_AuthorSubject author, jz_btree *claims /* NULL = None */);
void jazz_node__node__ActiveSessionClaimsScope__drop(jazz_node_ActiveSessionClaimsScope *s);
jazz_node_NodeState *jazz_node__node__ActiveSessionClaimsScope__node(jazz_node_ActiveSessionClaimsScope *s);
typedef struct jazz_node_LocalMaintainedViewSubscription jazz_node_LocalMaintainedViewSubscription;
void jazz_node__node__LocalMaintainedViewSubscription__drop(jazz_node_LocalMaintainedViewSubscription *m);
typedef JZ_VEC(jazz_types_OutputOccurrenceId) jazz_db_OccurrenceVec;
jz_err jazz_node__node__NodeState__materialize_local_maintained_relation_snapshot_with_occurrences(jazz_node_NodeState *n, const jazz_node_LocalMaintainedViewSubscription *m, jazz_node_RelationSnapshot *snapshot, jazz_db_OccurrenceVec *root_occurrence_ids);
typedef struct jazz_db_OccurrenceRow { jazz_types_OutputOccurrenceId occurrence; jazz_node_CurrentRow row; } jazz_db_OccurrenceRow;
typedef enum { JAZZ_LMU_FLAT, JAZZ_LMU_ORDERED_WINDOW, JAZZ_LMU_STRUCTURED } jazz_node_LocalMaintainedViewSubscriptionUpdateTag;
typedef JZ_VEC(groove_TerminalOperation) jazz_db_TerminalOperationVec;
typedef struct jazz_node_LocalMaintainedViewSubscriptionUpdate {
    u32 tag;
    JZ_VEC(jazz_db_OccurrenceRow) added; jazz_db_OccurrenceVec removed;   /* Flat */
    jazz_node_RelationSnapshot snapshot; jazz_db_OccurrenceVec occurrence_ids; /* OrderedWindow */
    jazz_db_TerminalOperationVec terminal_operations;                     /* Structured */
} jazz_node_LocalMaintainedViewSubscriptionUpdate;
typedef struct jazz_node_PreparedQueryPlanHandle jazz_node_PreparedQueryPlanHandle;
jazz_node_PreparedQueryPlanHandle *jazz_node__node__PreparedQueryPlanHandle__clone(const jazz_node_PreparedQueryPlanHandle *h);
void jazz_node__node__PreparedQueryPlanHandle__drop(jazz_node_PreparedQueryPlanHandle *h);
typedef enum { JAZZ_QAM_TRUSTED_SERVING, JAZZ_QAM_CLIENT_LOCAL } jazz_engine_QueryAuthorizationMode;

/* jazz_node::terminal_root / terminal_record */
typedef struct jazz_node_TerminalRootPublicField {
    jazz_engine_CurrentRowPublicationField publication; jz_str name; jz_str descriptor_field_name; u32 slot; u32 carrier;
} jazz_node_TerminalRootPublicField;
typedef struct jazz_node_TerminalRootLayout {
    jz_str id;
    const groove_RecordDescriptor *root_descriptor;
    u32 root_key_slot;
    jz_str root_key_field_name;
    JZ_VEC(jazz_node_TerminalRootPublicField) public_fields;
    u32 carrier;
    bool root_union_arm;
} jazz_node_TerminalRootLayout;
jz_err jazz_node__node__terminal_root__terminal_root_occurrence_id_with_root_union(jz_slice root_key, bool root_union_arm, jazz_types_OutputOccurrenceId *out);
void *jazz_node__node__terminal_root__terminal_root_publication_fields(const jazz_node_TerminalRootLayout *layout);
typedef struct jazz_node_TerminalRecordState jazz_node_TerminalRecordState;
jz_err jazz_node__node__terminal_record__TerminalRecordState__new(groove_OwnedRecord record, jazz_node_TerminalRecordState **out);
jz_err jazz_node__node__terminal_record__TerminalRecordState__apply(jazz_node_TerminalRecordState *s, const groove_TerminalOperation *op);
jz_err jazz_node__node__terminal_record__TerminalRecordState__update_record(jazz_node_TerminalRecordState *s, groove_OwnedRecord record);
jz_err jazz_node__node__terminal_record__TerminalRecordState__record(const jazz_node_TerminalRecordState *s, groove_OwnedRecord *out);
jazz_node_TerminalRecordState *jazz_node__node__terminal_record__TerminalRecordState__clone(const jazz_node_TerminalRecordState *s);
void jazz_node__node__terminal_record__TerminalRecordState__drop(jazz_node_TerminalRecordState *s);
void jazz_node__node__CurrentRow__publication_fields_vec(const jazz_node_CurrentRow *r, void **out_arc);
void groove__records__OwnedRecord__clone(groove_OwnedRecord *dst, const groove_OwnedRecord *src);
const groove_RecordDescriptor *groove__records__OwnedRecord__descriptor(const groove_OwnedRecord *r);
void jazz_protocol__protocol__PolicyBindingKey__debug(jz_str *out, const jazz_protocol_PolicyBindingKey *k);


/* ---- protocol::authorization_scope::AuthorityContext, row availability externs ---- */
typedef struct jazz_protocol_AuthorityContext {
    u8 authority[16]; jazz_types_AuthorSubject link; u64 connection_id; u64 connection_epoch;
    u64 claims_revision; u64 policy_epoch; u64 authorization_progress; u64 settled_through;
} jazz_protocol_AuthorityContext;
bool jazz_protocol__authorization_scope__AuthorityContext__same_admitted_link(jazz_protocol_AuthorityContext a, jazz_protocol_AuthorityContext b);
const jz_btree *jazz_protocol__protocol__PolicyBindingKey__claims(const jazz_protocol_PolicyBindingKey *k);
void jazz_protocol__protocol__CurrentRowsRequest__clone(jazz_protocol_CurrentRowsRequest *dst, const jazz_protocol_CurrentRowsRequest *src);
void jazz_protocol__protocol__CurrentRowsRequest__drop(void *p);
void jazz_protocol__protocol__CurrentRowsReceipt__clone(void *dst, const void *src);
void jazz_protocol__protocol__CurrentRowsReceipt__drop(void *p);
typedef struct jazz_node_ClientRelayScope jazz_node_ClientRelayScope;
const jazz_node_ClientRelayScope *jazz_node__node__NodeState__client_relay_scope(const jazz_node_NodeState *n);
bool jazz_node__node__relay_scope__ClientRelayScope__admits_session(const jazz_node_ClientRelayScope *s, jazz_types_AuthorSubject session);
jz_err jazz_node__node__NodeState__current_row_coordinate(jazz_node_NodeState *n, jz_slice table, jazz_types_RowUuid row, jazz_protocol_CurrentRowCoordinate *out);
u64 jazz_node__node__NodeState__active_catalogue_seq(const jazz_node_NodeState *n);
jazz_types_GlobalTime jazz_node__node__NodeState__committed_global_time(const jazz_node_NodeState *n);
jz_err jazz_node__node__NodeState__ingest_current_rows_receipt(jazz_node_NodeState *n, const jazz_protocol_CurrentRowsReceipt *receipt);

/* ---- db.rs:3509-3726 helpers ---- */
/* crate::node::Error variants matched by server_failure_code (0x500 range) */
enum {
    JAZZ_NODE_ERR_TABLE_NOT_FOUND = 0x502, JAZZ_NODE_ERR_QUERY = 0x503, JAZZ_NODE_ERR_QUERY_LOWERING = 0x504,
    JAZZ_NODE_ERR_AUTHORIZATION_DENIED = 0x505, JAZZ_NODE_ERR_INVALID_STORED_VALUE = 0x506,
    JAZZ_NODE_ERR_INVALID_CATALOGUE_UPDATE = 0x507, JAZZ_NODE_ERR_MISSING_OPEN_BATCH = 0x508,
};
/* Error::Query(e) if matches!(*e, QueryError::UnknownTable(_)) */
bool jazz_node__node__Error__query_is_unknown_table(const jz_err *e);
typedef struct jazz_model_ShapeAst jazz_model_ShapeAst;
jz_err jazz_node__node__NodeState__validate_shape_ast_for_registration(jazz_node_NodeState *n, jazz_model_ShapeId shape_id, const jazz_model_ShapeAst *ast, bool *has, jazz_model_ValidatedQuery *out);
bool jazz_node__node__LocalMaintainedViewSubscription__compiled_authorization_mode(const jazz_node_LocalMaintainedViewSubscription *m, u32 *out);
/* eprintln!: std's stderr writer (wasi fd_write) */
void jazz_std__eprintln(jz_slice line);

#endif
