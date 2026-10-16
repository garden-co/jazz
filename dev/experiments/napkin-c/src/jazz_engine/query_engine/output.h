/* output.h: row-set output request vocabulary (query_engine/output.rs). Included from mod.h. */
#ifndef JAZZ_ENGINE_QUERY_ENGINE_OUTPUT_H
#define JAZZ_ENGINE_QUERY_ENGINE_OUTPUT_H

enum { JAZZ_ENGINE_FP_ALL, JAZZ_ENGINE_FP_FIELDS };
typedef struct jazz_engine_FieldProjection { u8 tag; jz_btree fields; /* BTreeSet<String> */ } jazz_engine_FieldProjection;
enum { JAZZ_ENGINE_CARD_ONE, JAZZ_ENGINE_CARD_MANY };
enum { JAZZ_ENGINE_HOLE_KEEP_PARENT_WITH_HOLES, JAZZ_ENGINE_HOLE_DROP_INCOMPLETE_PARENT };
typedef struct jazz_engine_AppPathProjection jazz_engine_AppPathProjection;
typedef JZ_VEC(jazz_engine_AppPathProjection) jazz_engine_AppPathProjectionVec;
struct jazz_engine_AppPathProjection {
    jazz_engine_ProgramPathId path;
    jz_str field;
    u8 cardinality;
    jazz_engine_FieldProjection fields;
    jazz_engine_AppPathProjectionVec children;
    u8 hole_policy;
};
typedef struct jazz_engine_AppProjectionTree { jazz_engine_FieldProjection fields; jazz_engine_AppPathProjectionVec paths; } jazz_engine_AppProjectionTree;
enum { JAZZ_ENGINE_PP_SHAPE_DEFAULT, JAZZ_ENGINE_PP_RELATION, JAZZ_ENGINE_PP_TREE };
typedef struct jazz_engine_PayloadProjection {
    u8 tag;
    jazz_model_RelationProjectColumnVec relation;
    jazz_engine_AppProjectionTree tree;
} jazz_engine_PayloadProjection;
typedef struct jazz_engine_AppRowOutputRequest { bool public_terminal; jazz_engine_PayloadProjection projection; } jazz_engine_AppRowOutputRequest;

enum { JAZZ_ENGINE_PDR_READ, JAZZ_ENGINE_PDR_WRITE };
typedef struct jazz_engine_PolicyDecisionFactKey { u8 role; jz_bytes fingerprint; } jazz_engine_PolicyDecisionFactKey;
enum { JAZZ_ENGINE_POSR_BASE, JAZZ_ENGINE_POSR_NOW };

/* read.rs coverage identities used by fact keys */
typedef jz_bytes jazz_engine_BatchId;
enum { JAZZ_ENGINE_SCOPE_PROGRAM, JAZZ_ENGINE_SCOPE_SOURCE, JAZZ_ENGINE_SCOPE_PATH };
typedef struct jazz_engine_CoverageScope { u8 tag; jazz_engine_SourceId source; jazz_engine_ProgramPathId path; } jazz_engine_CoverageScope;
enum { JAZZ_ENGINE_FPOS_GLOBAL_TIME, JAZZ_ENGINE_FPOS_SNAPSHOT, JAZZ_ENGINE_FPOS_TRANSACTION, JAZZ_ENGINE_FPOS_STREAM_POSITION };
typedef struct jazz_engine_FrontierPosition { u8 tag; jazz_types_GlobalTime time; jz_bytes bytes; jazz_model_TxId tx; } jazz_engine_FrontierPosition;
typedef struct jazz_engine_ResolvedFrontier { u8 tier; bool has_stream; jz_str stream; jazz_engine_FrontierPosition through; } jazz_engine_ResolvedFrontier;
enum { JAZZ_ENGINE_FREQ_NONE, JAZZ_ENGINE_FREQ_THROUGH };
typedef struct jazz_engine_FrontierRequirement { u8 tag; jazz_engine_ResolvedFrontier through; } jazz_engine_FrontierRequirement;
typedef struct jazz_engine_CoverageFrontier { jazz_engine_CoverageScope scope; jazz_engine_FrontierRequirement frontier; } jazz_engine_CoverageFrontier;

enum {
    JAZZ_ENGINE_FACT_AUTHORIZED_ROWS, JAZZ_ENGINE_FACT_RESULT_MEMBERSHIP, JAZZ_ENGINE_FACT_RELATION_EDGES,
    JAZZ_ENGINE_FACT_PATH_CORRELATION_COVERAGE, JAZZ_ENGINE_FACT_PROGRAM_SOURCE_COVERAGE, JAZZ_ENGINE_FACT_READ_FRONTIER_SETTLED,
    JAZZ_ENGINE_FACT_COMPLETE_TX_PAYLOAD_COVERAGE, JAZZ_ENGINE_FACT_VIEW_COMPLETE_EXCLUSIVE_COVERAGE,
    JAZZ_ENGINE_FACT_VERSION_WITNESSES, JAZZ_ENGINE_FACT_REPLACEMENT_WITNESSES, JAZZ_ENGINE_FACT_POLICY_DECISION,
    JAZZ_ENGINE_FACT_POLICY_WITNESSES, JAZZ_ENGINE_FACT_CONTRIBUTING_MEMBERS, JAZZ_ENGINE_FACT_PREDICATE_READS,
    JAZZ_ENGINE_FACT_PREDICATE_OUTPUT_SET, JAZZ_ENGINE_FACT_POINT_READS
};
typedef struct jazz_engine_ProgramFactKey {
    u8 tag;
    jazz_engine_CoverageScope scope;       /* ProgramSourceCoverage / ViewCompleteExclusiveCoverage.view */
    jazz_engine_CoverageFrontier frontier; /* ReadFrontierSettled */
    jazz_engine_BatchId batch;             /* CompleteTxPayloadCoverage */
    u8 tier;                               /* CompleteTxPayloadCoverage / ViewCompleteExclusiveCoverage */
    jazz_engine_ResultId *result;          /* ViewCompleteExclusiveCoverage, NULL = None */
    jazz_engine_PolicyDecisionFactKey decision;
    u8 role;                               /* PredicateOutputSet */
    bool present;                          /* PointReads */
} jazz_engine_ProgramFactKey;

typedef struct jazz_engine_RowSetOutputRequest {
    bool has_app_rows; jazz_engine_AppRowOutputRequest app_rows;
    jz_btree facts; /* BTreeSet<ProgramFactKey> */
} jazz_engine_RowSetOutputRequest;

extern const jz_btree_desc jazz_engine__FACT_KEY_SET_DESC;
#define JE_OUT(x) jazz_engine__query_engine__output__##x
#define JE_READ(x) jazz_engine__query_engine__read__##x
void JE_OUT(FieldProjection__Clone__clone)(jazz_engine_FieldProjection *dst, const jazz_engine_FieldProjection *src);
void JE_OUT(FieldProjection__drop)(jazz_engine_FieldProjection *f);
void JE_OUT(FieldProjection__Debug__fmt)(jz_str *out, const jazz_engine_FieldProjection *f);
void JE_OUT(AppPathProjection__Clone__clone)(jazz_engine_AppPathProjection *dst, const jazz_engine_AppPathProjection *src);
void JE_OUT(AppPathProjection__drop)(jazz_engine_AppPathProjection *p);
void JE_OUT(PayloadProjection__Clone__clone)(jazz_engine_PayloadProjection *dst, const jazz_engine_PayloadProjection *src);
void JE_OUT(PayloadProjection__drop)(jazz_engine_PayloadProjection *p);
void JE_OUT(ProgramFactKey__Clone__clone)(jazz_engine_ProgramFactKey *dst, const jazz_engine_ProgramFactKey *src);
void JE_OUT(ProgramFactKey__drop)(jazz_engine_ProgramFactKey *k);
int JE_OUT(ProgramFactKey__Ord__cmp)(const jazz_engine_ProgramFactKey *a, const jazz_engine_ProgramFactKey *b);
void JE_OUT(ProgramFactKey__Debug__fmt)(jz_str *out, const jazz_engine_ProgramFactKey *k);
void JE_OUT(RowSetOutputRequest__Clone__clone)(jazz_engine_RowSetOutputRequest *dst, const jazz_engine_RowSetOutputRequest *src);
void JE_OUT(RowSetOutputRequest__drop)(jazz_engine_RowSetOutputRequest *r);
void JE_OUT(RowSetOutputRequest__Debug__fmt)(jz_str *out, const jazz_engine_RowSetOutputRequest *r);
void JE_OUT(PolicyDecisionRole__Debug__fmt)(jz_str *out, u8 role);
jazz_engine_ProgramFactKey jazz_engine__fact_key_simple(u8 tag);
void jazz_engine__fact_key_drop_v(void *k);
void jazz_engine__fact_key_clone_v(void *d, const void *s);
void jazz_engine__fact_key_debug_v(jz_str *out, const void *k);

void JE_READ(CoverageScope__Clone__clone)(jazz_engine_CoverageScope *dst, const jazz_engine_CoverageScope *src);
void JE_READ(CoverageScope__drop)(jazz_engine_CoverageScope *s);
int JE_READ(CoverageScope__Ord__cmp)(const jazz_engine_CoverageScope *a, const jazz_engine_CoverageScope *b);
bool JE_READ(CoverageScope__PartialEq__eq)(const jazz_engine_CoverageScope *a, const jazz_engine_CoverageScope *b);
void JE_READ(CoverageScope__Debug__fmt)(jz_str *out, const jazz_engine_CoverageScope *s);
void JE_READ(CoverageFrontier__Clone__clone)(jazz_engine_CoverageFrontier *dst, const jazz_engine_CoverageFrontier *src);
void JE_READ(CoverageFrontier__drop)(jazz_engine_CoverageFrontier *f);
int JE_READ(CoverageFrontier__Ord__cmp)(const jazz_engine_CoverageFrontier *a, const jazz_engine_CoverageFrontier *b);
void JE_READ(CoverageFrontier__Debug__fmt)(jz_str *out, const jazz_engine_CoverageFrontier *f);
void JE_READ(BatchId__Debug__fmt)(jz_str *out, const jazz_engine_BatchId *b);
void jazz_engine__tier_debug(jz_str *out, u8 tier);
bool JE_OUT(RowSetOutputRequest__PartialEq__eq)(const jazz_engine_RowSetOutputRequest *a, const jazz_engine_RowSetOutputRequest *b);

#endif
