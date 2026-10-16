/* read.h: requested read views (query_engine/read.rs, RequestedSourceStage only:
 * the generic SourceResolution stages are one C type each, instantiated once). Included from mod.h. */
#ifndef JAZZ_ENGINE_QUERY_ENGINE_READ_H
#define JAZZ_ENGINE_QUERY_ENGINE_READ_H

enum { JAZZ_ENGINE_FRR_PREDICATE_OUTPUT_BASE, JAZZ_ENGINE_FRR_PREDICATE_OUTPUT_NOW };
enum { JAZZ_ENGINE_SFS_CURRENT, JAZZ_ENGINE_SFS_EXPLICIT };
enum { JAZZ_ENGINE_SSS_COMPATIBLE_PARTITIONS, JAZZ_ENGINE_SSS_SINGLE, JAZZ_ENGINE_SSS_EXPLICIT };
enum { JAZZ_ENGINE_LENS_CANONICAL, JAZZ_ENGINE_LENS_FINGERPRINT };
typedef struct jazz_engine_SchemaProjection {
    u8 schema_family_tag; jazz_types_SchemaFamilyId schema_family;
    u8 storage_tag; jazz_types_SchemaVersionId storage_single; jz_btree storage_explicit; /* BTreeSet<SchemaVersionId> */
    u8 lens_tag; jz_bytes lens_fingerprint;
} jazz_engine_SchemaProjection;
enum { JAZZ_ENGINE_DATA_CURRENT, JAZZ_ENGINE_DATA_BRANCH };
typedef struct jazz_engine_DataSource { u8 tag; jazz_model_BranchKey *branch; } jazz_engine_DataSource;
enum { JAZZ_ENGINE_BVSB_CURRENT, JAZZ_ENGINE_BVSB_SNAPSHOT };
typedef struct jazz_engine_BranchViewSourceBase { u8 tag; jazz_model_BranchKey *key; jazz_model_SnapshotRef *snapshot; } jazz_engine_BranchViewSourceBase;
enum { JAZZ_ENGINE_OVERLAY_PENDING_LOCAL, JAZZ_ENGINE_OVERLAY_DIRECT_BATCH, JAZZ_ENGINE_OVERLAY_ACCEPTED_TRANSACTION, JAZZ_ENGINE_OVERLAY_OPEN_TRANSACTION };
typedef struct jazz_engine_OverlayRef { u8 tag; jazz_engine_BatchId batch; jazz_model_TxId tx; jazz_model_OpenTransactionId open; } jazz_engine_OverlayRef;
typedef JZ_VEC(jazz_engine_OverlayRef) jazz_engine_OverlayRefVec;
enum { JAZZ_ENGINE_MERGE_LAST_WRITE_WINS, JAZZ_ENGINE_MERGE_UNION };
enum {
    JAZZ_ENGINE_SRC_VISIBLE_CURRENT, JAZZ_ENGINE_SRC_BRANCH_VIEW, JAZZ_ENGINE_SRC_HISTORY_CUT, JAZZ_ENGINE_SRC_SNAPSHOT_REF,
    JAZZ_ENGINE_SRC_SETTLED_BINDING_VIEW, JAZZ_ENGINE_SRC_WITH_OVERLAYS, JAZZ_ENGINE_SRC_LENS_PROJECT, JAZZ_ENGINE_SRC_MERGE
};
typedef struct jazz_engine_SourceExpr jazz_engine_SourceExpr;
typedef JZ_VEC(jazz_engine_SourceExpr) jazz_engine_SourceExprVec;
struct jazz_engine_SourceExpr {
    u8 tag;
    jazz_engine_SchemaProjection projection; /* all but WithOverlays / Merge */
    jazz_engine_DataSource data;             /* VisibleCurrent / HistoryCut / SnapshotRef */
    u8 tier;                                 /* VisibleCurrent / BranchView */
    jazz_model_BranchKey *head;              /* BranchView */
    bool has_base; jazz_engine_BranchViewSourceBase base;
    jazz_types_GlobalTime position;          /* HistoryCut */
    jazz_model_Snapshot *snapshot;           /* SnapshotRef */
    bool current_default;                    /* SettledBindingView */
    jazz_protocol_BindingViewKey binding_view;
    bool has_authority_result_key; jazz_protocol_AuthorityResultKey authority_result_key;
    jazz_engine_SourceExpr *input;           /* WithOverlays / LensProject */
    jazz_engine_OverlayRefVec overlays;      /* WithOverlays */
    jazz_engine_SourceExprVec inputs;        /* Merge */
    u8 mode;                                 /* Merge */
};
typedef struct jazz_engine_ReadView {
    jazz_types_SchemaVersionId read_schema, policy_schema;
    jz_btree sources; /* BTreeMap<SourceId, SourceExpr> */
} jazz_engine_ReadView;
typedef struct jazz_engine_QueryReadSet {
    jazz_engine_ReadView primary;
    jz_btree fact_reads; /* BTreeMap<FactReadRole, ReadView> */
} jazz_engine_QueryReadSet;
typedef jazz_engine_QueryReadSet jazz_engine_RequestedReadSet;

extern const jz_btree_desc jazz_engine__SOURCE_EXPR_MAP_DESC;
extern const jz_btree_desc jazz_engine__FACT_READ_MAP_DESC;
void JE_READ(SourceExpr__Clone__clone)(jazz_engine_SourceExpr *dst, const jazz_engine_SourceExpr *src);
void JE_READ(SourceExpr__drop)(jazz_engine_SourceExpr *e);
bool JE_READ(SourceExpr__PartialEq__eq)(const jazz_engine_SourceExpr *a, const jazz_engine_SourceExpr *b);
void JE_READ(SourceExpr__Debug__fmt)(jz_str *out, const jazz_engine_SourceExpr *e);
bool JE_READ(SourceExpr__current_tier)(const jazz_engine_SourceExpr *e, u8 *tier);
bool JE_READ(ReadView__source_current_tier)(const jazz_engine_ReadView *v, const jazz_engine_SourceId *source, u8 *tier);
jazz_types_SchemaVersionId JE_READ(ReadView__read_schema)(const jazz_engine_ReadView *v);
void JE_READ(ReadView__Clone__clone)(jazz_engine_ReadView *dst, const jazz_engine_ReadView *src);
void JE_READ(ReadView__drop)(jazz_engine_ReadView *v);
void JE_READ(ReadView__Debug__fmt)(jz_str *out, const jazz_engine_ReadView *v);
void JE_READ(QueryReadSet__primary)(jazz_engine_ReadView primary, jazz_engine_QueryReadSet *out);
void JE_READ(QueryReadSet__Clone__clone)(jazz_engine_QueryReadSet *dst, const jazz_engine_QueryReadSet *src);
void JE_READ(QueryReadSet__drop)(jazz_engine_QueryReadSet *r);
void JE_READ(QueryReadSet__Debug__fmt)(jz_str *out, const jazz_engine_QueryReadSet *r);
bool JE_READ(ReadView__PartialEq__eq)(const jazz_engine_ReadView *a, const jazz_engine_ReadView *b);
bool JE_READ(QueryReadSet__PartialEq__eq)(const jazz_engine_QueryReadSet *a, const jazz_engine_QueryReadSet *b);

#endif
