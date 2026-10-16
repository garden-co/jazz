/* input.h: normalized row-set program input (query_engine/input.rs). Included from mod.h. */
#ifndef JAZZ_ENGINE_QUERY_ENGINE_INPUT_H
#define JAZZ_ENGINE_QUERY_ENGINE_INPUT_H

typedef jz_str jazz_engine_RowSetNodeId;
typedef jz_str jazz_engine_FrontierId;

enum { JAZZ_ENGINE_ROLE_ROOT, JAZZ_ENGINE_ROLE_ALIAS, JAZZ_ENGINE_ROLE_RECURSIVE_SEED, JAZZ_ENGINE_ROLE_RECURSIVE_STEP,
       JAZZ_ENGINE_ROLE_CORRELATED_CHILD, JAZZ_ENGINE_ROLE_POLICY };
typedef struct jazz_engine_SourceRole { u8 tag; jz_str name; } jazz_engine_SourceRole;
typedef JZ_VEC(jazz_engine_SourceRole) jazz_engine_SourceRoleVec;
typedef struct jazz_engine_SourceId { jz_str table; jazz_engine_SourceRoleVec path; } jazz_engine_SourceId;
typedef JZ_VEC(jazz_engine_SourceId) jazz_engine_SourceIdVec;
typedef struct jazz_engine_ProgramPathId { jazz_engine_SourceId owner, child; } jazz_engine_ProgramPathId;

enum { JAZZ_ENGINE_PROV_CREATED_AT, JAZZ_ENGINE_PROV_CREATED_BY, JAZZ_ENGINE_PROV_UPDATED_AT, JAZZ_ENGINE_PROV_UPDATED_BY };
enum { JAZZ_ENGINE_ROWID_SOURCE, JAZZ_ENGINE_ROWID_FRONTIER };
enum {
    JAZZ_ENGINE_NVR_SOURCE_FIELD, JAZZ_ENGINE_NVR_PARAM, JAZZ_ENGINE_NVR_CLAIM, JAZZ_ENGINE_NVR_FRONTIER_COLUMN,
    JAZZ_ENGINE_NVR_ROW_ID, JAZZ_ENGINE_NVR_PROVENANCE, JAZZ_ENGINE_NVR_LITERAL, JAZZ_ENGINE_NVR_TEMPLATE_LITERAL
};
/* NormalizedValueRef. `sub` is the ProvenanceField (Provenance) or RowIdRef tag (RowId). */
typedef struct jazz_engine_NormalizedValueRef {
    u8 tag; u8 sub;
    union {
        struct { jazz_engine_SourceId source; jz_str field; } source_field; /* SourceField */
        jz_str param;                                                     /* Param */
        jazz_engine_ClaimPath claim;                                      /* Claim */
        struct { jazz_engine_FrontierId frontier; jz_str field; } frontier_column;
        jazz_engine_SourceId source;                                      /* RowId(Source) / Provenance.source */
        jazz_engine_FrontierId frontier;                                  /* RowId(Frontier) */
        jz_bytes literal;
        u32 template_literal;
    } u;
} jazz_engine_NormalizedValueRef;
typedef JZ_VEC(jazz_engine_NormalizedValueRef) jazz_engine_NvrVec;

enum { JAZZ_ENGINE_CMP_EQ, JAZZ_ENGINE_CMP_NE, JAZZ_ENGINE_CMP_LT, JAZZ_ENGINE_CMP_LTE, JAZZ_ENGINE_CMP_GT, JAZZ_ENGINE_CMP_GTE };
enum {
    JAZZ_ENGINE_PRED_TRUE, JAZZ_ENGINE_PRED_FALSE, JAZZ_ENGINE_PRED_COMPARE, JAZZ_ENGINE_PRED_IN, JAZZ_ENGINE_PRED_ARRAY_CONTAINS,
    JAZZ_ENGINE_PRED_TEXT_CONTAINS, JAZZ_ENGINE_PRED_IS_NULL, JAZZ_ENGINE_PRED_IS_NOT_NULL, JAZZ_ENGINE_PRED_ENUM_MATCH,
    JAZZ_ENGINE_PRED_AND, JAZZ_ENGINE_PRED_OR, JAZZ_ENGINE_PRED_NOT
};
typedef struct jazz_engine_PredicateExpr jazz_engine_PredicateExpr;
typedef JZ_VEC(jazz_engine_PredicateExpr) jazz_engine_PredicateVec;
struct jazz_engine_PredicateExpr {
    u8 tag; u8 op; /* op: Compare */
    union {
        struct { jazz_engine_NormalizedValueRef left, right; } pair; /* Compare / ArrayContains(value,needle) / TextContains */
        struct { jazz_engine_NormalizedValueRef value; jazz_engine_NvrVec options; } in;
        jazz_engine_NormalizedValueRef value;                        /* IsNull / IsNotNull */
        struct { jazz_engine_NormalizedValueRef value; u32 case_tag; jazz_engine_PredicateExpr *payload; } enum_match;
        jazz_engine_PredicateVec children;                           /* And / Or */
        jazz_engine_PredicateExpr *not_;                             /* Not */
    } u;
};

enum { JAZZ_ENGINE_RRR_SOURCE, JAZZ_ENGINE_RRR_SOURCE_ALTERNATIVES, JAZZ_ENGINE_RRR_PROJECTED };
typedef struct jazz_engine_ResultRowRef {
    u8 tag;
    jazz_engine_SourceId source;      /* Source */
    jz_btree alternatives;            /* SourceAlternatives: BTreeSet<SourceId> */
    jazz_engine_NormalizedValueRef row; /* Projected */
    jazz_engine_NormalizedValueRef *source_discriminator; /* Projected, NULL = None */
} jazz_engine_ResultRowRef;
typedef struct jazz_engine_SyntheticIdentitySpec { jz_str table; jazz_engine_StrVec key_columns, revision_columns; } jazz_engine_SyntheticIdentitySpec;
enum { JAZZ_ENGINE_RESULT_REAL_ROW, JAZZ_ENGINE_RESULT_SYNTHETIC_TUPLE, JAZZ_ENGINE_RESULT_PATH_TUPLE };
typedef struct jazz_engine_ResultId {
    u8 tag;
    union {
        struct { jz_str table; jazz_engine_ResultRowRef row; } real_row;
        jazz_engine_SyntheticIdentitySpec synthetic;
        struct { jazz_engine_ProgramPathId path; jazz_engine_NvrVec revision; } path_tuple;
    } u;
} jazz_engine_ResultId;

typedef struct jazz_engine_TypedOutputField { jz_str name; groove_ColumnType ty; } jazz_engine_TypedOutputField;
typedef JZ_VEC(jazz_engine_TypedOutputField) jazz_engine_TypedOutputFieldVec;

typedef struct jazz_engine_ValueSourceColumn { jz_str name; jazz_engine_NormalizedValueRef value; groove_ColumnType ty; } jazz_engine_ValueSourceColumn;
typedef JZ_VEC(jazz_engine_ValueSourceColumn) jazz_engine_ValueSourceColumnVec;
enum { JAZZ_ENGINE_VSM_BINDING, JAZZ_ENGINE_VSM_INLINE };
typedef struct jazz_engine_RowProjection { jazz_engine_TypedOutputField output; jazz_engine_NormalizedValueRef value; } jazz_engine_RowProjection;
typedef JZ_VEC(jazz_engine_RowProjection) jazz_engine_RowProjectionVec;
typedef struct jazz_engine_UnionInput { jazz_engine_RowSetNodeId node; jz_str label; } jazz_engine_UnionInput;
typedef JZ_VEC(jazz_engine_UnionInput) jazz_engine_UnionInputVec;
enum { JAZZ_ENGINE_JOIN_INNER, JAZZ_ENGINE_JOIN_SEMI, JAZZ_ENGINE_JOIN_NULL_EXTEND };
enum { JAZZ_ENGINE_CORR_OPTIONAL, JAZZ_ENGINE_CORR_AT_LEAST_ONE, JAZZ_ENGINE_CORR_MATCH_CARDINALITY };
enum { JAZZ_ENGINE_AGG_COUNT, JAZZ_ENGINE_AGG_SUM, JAZZ_ENGINE_AGG_AVG, JAZZ_ENGINE_AGG_MIN, JAZZ_ENGINE_AGG_MAX };
typedef struct jazz_engine_AggregateExpr {
    jazz_engine_TypedOutputField output; u8 function;
    bool has_input; jazz_engine_NormalizedValueRef input;
} jazz_engine_AggregateExpr;
typedef JZ_VEC(jazz_engine_AggregateExpr) jazz_engine_AggregateExprVec;
enum { JAZZ_ENGINE_SORT_ASC, JAZZ_ENGINE_SORT_DESC };
typedef struct jazz_engine_OrderKey { jazz_engine_NormalizedValueRef value; u8 direction; } jazz_engine_OrderKey;
typedef JZ_VEC(jazz_engine_OrderKey) jazz_engine_OrderKeyVec;
enum { JAZZ_ENGINE_VIS_VISIBLE, JAZZ_ENGINE_VIS_INCLUDE_DELETED };

enum {
    JAZZ_ENGINE_EXPR_SOURCE, JAZZ_ENGINE_EXPR_VALUE_SOURCE, JAZZ_ENGINE_EXPR_FRONTIER_SOURCE, JAZZ_ENGINE_EXPR_FILTER,
    JAZZ_ENGINE_EXPR_JOIN, JAZZ_ENGINE_EXPR_RECURSIVE_RELATION, JAZZ_ENGINE_EXPR_UNION, JAZZ_ENGINE_EXPR_DISTINCT,
    JAZZ_ENGINE_EXPR_PROJECT, JAZZ_ENGINE_EXPR_CORRELATED_PATH_PROJECTION, JAZZ_ENGINE_EXPR_ORDER_BY, JAZZ_ENGINE_EXPR_SLICE,
    JAZZ_ENGINE_EXPR_AGGREGATE
};
typedef struct jazz_engine_RowSetExpr {
    u8 tag;
    union {
        struct { jazz_engine_SourceId source; u8 visibility; } source;
        struct { jz_str shape; jazz_engine_ValueSourceColumnVec columns; u8 mode; } value_source;
        struct { jazz_engine_FrontierId frontier; jazz_engine_ValueSourceColumnVec columns; } frontier_source;
        struct { jazz_engine_RowSetNodeId input; jazz_engine_PredicateExpr predicate; } filter;
        struct { jazz_engine_RowSetNodeId left, right; u8 mode; jazz_engine_PredicateExpr on; } join;
        struct {
            jazz_engine_RowSetNodeId seed, step; jazz_engine_FrontierId frontier;
            jazz_engine_NormalizedValueRef frontier_key; jazz_engine_NvrVec dedupe_keys; jazz_model_RecursionBound bound;
        } recursive;
        struct { jazz_engine_UnionInputVec inputs; } union_;
        struct { jazz_engine_RowSetNodeId input; jazz_engine_NvrVec keys; } distinct;
        struct { jazz_engine_RowSetNodeId input; jazz_engine_RowProjectionVec columns; } project;
        struct {
            jazz_engine_RowSetNodeId input, child_input; jazz_engine_ProgramPathId path;
            jazz_engine_PredicateExpr correlation; u8 requirement;
        } correlated;
        struct { jazz_engine_RowSetNodeId input; jazz_engine_OrderKeyVec keys; } order_by;
        struct {
            jazz_engine_RowSetNodeId input; jazz_engine_NvrVec partition_by;
            bool has_limit; u32 limit; u32 offset; jazz_engine_NvrVec tie_breaker;
            bool has_rank_output; jazz_engine_TypedOutputField rank_output;
        } slice;
        struct { jazz_engine_RowSetNodeId input; jazz_engine_NvrVec group_by; jazz_engine_AggregateExprVec outputs; } aggregate;
    } u;
} jazz_engine_RowSetExpr;

enum { JAZZ_ENGINE_GATE_REQUIRED, JAZZ_ENGINE_GATE_INNER };
typedef struct jazz_engine_ClosurePathSegment { jazz_engine_SourceId parent, target; jz_str source_field; } jazz_engine_ClosurePathSegment;
typedef JZ_VEC(jazz_engine_ClosurePathSegment) jazz_engine_ClosurePathSegmentVec;
enum { JAZZ_ENGINE_CLOSURE_IMPLICIT_ROOT_REFERENCE, JAZZ_ENGINE_CLOSURE_EXPLICIT_INCLUDE };
typedef struct jazz_engine_ClosurePath {
    u8 tag;
    jz_str id;
    jazz_engine_ClosurePathSegment segment;     /* ImplicitRootReference */
    jazz_engine_ClosurePathSegmentVec segments; /* ExplicitInclude */
    bool has_root_gate; u8 root_gate;
} jazz_engine_ClosurePath;
typedef JZ_VEC(jazz_engine_ClosurePath) jazz_engine_ClosurePathVec;
typedef struct jazz_engine_JoinContribution {
    jazz_engine_SourceId *parent; /* NULL = None */
    jz_str id; jazz_engine_SourceId source; jazz_engine_RowSetNodeId input; jazz_engine_PredicateExpr membership;
} jazz_engine_JoinContribution;
typedef JZ_VEC(jazz_engine_JoinContribution) jazz_engine_JoinContributionVec;
typedef struct jazz_engine_InheritedContribution {
    jz_str id; jazz_engine_SourceId source; jazz_engine_RowSetNodeId input; jazz_engine_PredicateExpr membership;
} jazz_engine_InheritedContribution;
typedef JZ_VEC(jazz_engine_InheritedContribution) jazz_engine_InheritedContributionVec;
typedef struct jazz_engine_ReachableContribution {
    jz_str id; jazz_engine_SourceId access_source, edge_source; jazz_engine_RowSetNodeId access_input; jz_str root_ref_field;
} jazz_engine_ReachableContribution;
typedef JZ_VEC(jazz_engine_ReachableContribution) jazz_engine_ReachableContributionVec;
typedef struct jazz_engine_NormalizedShapeIdentity { jazz_model_ShapeId shape_id; jz_bytes canonical; } jazz_engine_NormalizedShapeIdentity;
typedef struct jazz_engine_NormalizedRowSetShape {
    jazz_engine_NormalizedShapeIdentity identity;
    jazz_engine_RowSetNodeId root;
    jazz_engine_ResultId result;
    jz_btree auxiliary_sources; /* BTreeSet<SourceId> */
    jazz_engine_ClosurePathVec closure_paths;
    jazz_engine_JoinContributionVec join_contributions;
    jazz_engine_InheritedContributionVec inherited_contributions;
    jazz_engine_ReachableContributionVec reachable_contributions;
    jz_btree nodes; /* BTreeMap<RowSetNodeId, RowSetExpr> */
} jazz_engine_NormalizedRowSetShape;

typedef struct jazz_engine_ProgramClaimParam { jazz_engine_ClaimPath path; groove_ColumnType ty; } jazz_engine_ProgramClaimParam;
typedef struct jazz_engine_ProgramBinding {
    jazz_model_BindingId id;
    bool has_source_shape; jz_str source_shape;
    jz_btree extra_user_params; /* String -> ColumnType */
    jz_btree param_types;       /* String -> ColumnType */
    jz_btree claim_params;      /* String -> ProgramClaimParam */
    jz_btree values;            /* String -> Value */
} jazz_engine_ProgramBinding;
typedef struct jazz_engine_RowSetProgramInput { jazz_engine_NormalizedRowSetShape shape; jazz_engine_ProgramBinding binding; } jazz_engine_RowSetProgramInput;
enum { JAZZ_ENGINE_AUTH_TRUSTED_SERVING, JAZZ_ENGINE_AUTH_CLIENT_LOCAL };

/* ---- shared container descriptors ---- */
extern const jz_btree_desc jazz_engine__SOURCE_ID_SET_DESC;         /* BTreeSet<SourceId> */
extern const jz_btree_desc jazz_engine__NODE_EXPR_MAP_DESC;         /* BTreeMap<RowSetNodeId, RowSetExpr> */
extern const jz_btree_desc jazz_engine__STR_CLAIM_PARAM_MAP_DESC;   /* BTreeMap<String, ProgramClaimParam> */
extern const jz_btree_desc jazz_engine__NVR_SET_DESC;               /* BTreeSet<NormalizedValueRef> */
extern const jz_btree_desc jazz_engine__SOURCE_ID_STR_MAP_DESC;     /* BTreeMap<SourceId, String> */
extern const jz_hmap_desc jazz_engine__SOURCE_ID_HSET_DESC;         /* HashSet<SourceId> */

/* ---- derives ---- */
#define JE_IN(x) jazz_engine__query_engine__input__##x
void JE_IN(SourceRole__Clone__clone)(jazz_engine_SourceRole *dst, const jazz_engine_SourceRole *src);
void JE_IN(SourceId__Clone__clone)(jazz_engine_SourceId *dst, const jazz_engine_SourceId *src);
void JE_IN(SourceId__drop)(jazz_engine_SourceId *s);
bool JE_IN(SourceId__PartialEq__eq)(const jazz_engine_SourceId *a, const jazz_engine_SourceId *b);
int JE_IN(SourceId__Ord__cmp)(const jazz_engine_SourceId *a, const jazz_engine_SourceId *b);
u64 JE_IN(SourceId__Hash__hash)(const jazz_engine_SourceId *s);
void JE_IN(SourceId__Debug__fmt)(jz_str *out, const jazz_engine_SourceId *s);
void JE_IN(SourceId__program_source_id)(const jazz_engine_SourceId *s, jazz_protocol_ProgramSourceId *out);
void JE_IN(ProgramPathId__Clone__clone)(jazz_engine_ProgramPathId *dst, const jazz_engine_ProgramPathId *src);
void JE_IN(ProgramPathId__drop)(jazz_engine_ProgramPathId *p);
bool JE_IN(ProgramPathId__PartialEq__eq)(const jazz_engine_ProgramPathId *a, const jazz_engine_ProgramPathId *b);
int JE_IN(ProgramPathId__Ord__cmp)(const jazz_engine_ProgramPathId *a, const jazz_engine_ProgramPathId *b);
void JE_IN(ProgramPathId__Debug__fmt)(jz_str *out, const jazz_engine_ProgramPathId *p);
void JE_IN(ClaimPath__Clone__clone)(jazz_engine_ClaimPath *dst, const jazz_engine_ClaimPath *src);
void JE_IN(ClaimPath__drop)(jazz_engine_ClaimPath *p);
void JE_IN(ClaimPath__Debug__fmt)(jz_str *out, const jazz_engine_ClaimPath *p);
void JE_IN(NormalizedValueRef__Clone__clone)(jazz_engine_NormalizedValueRef *dst, const jazz_engine_NormalizedValueRef *src);
void JE_IN(NormalizedValueRef__drop)(jazz_engine_NormalizedValueRef *v);
bool JE_IN(NormalizedValueRef__PartialEq__eq)(const jazz_engine_NormalizedValueRef *a, const jazz_engine_NormalizedValueRef *b);
int JE_IN(NormalizedValueRef__Ord__cmp)(const jazz_engine_NormalizedValueRef *a, const jazz_engine_NormalizedValueRef *b);
void JE_IN(NormalizedValueRef__Debug__fmt)(jz_str *out, const jazz_engine_NormalizedValueRef *v);
void JE_IN(PredicateExpr__Clone__clone)(jazz_engine_PredicateExpr *dst, const jazz_engine_PredicateExpr *src);
void JE_IN(PredicateExpr__drop)(jazz_engine_PredicateExpr *p);
bool JE_IN(PredicateExpr__PartialEq__eq)(const jazz_engine_PredicateExpr *a, const jazz_engine_PredicateExpr *b);
void JE_IN(PredicateExpr__Debug__fmt)(jz_str *out, const jazz_engine_PredicateExpr *p);
typedef jz_err (*jazz_engine_NvrVisit)(const jazz_engine_NormalizedValueRef *v, void *ctx);
typedef jz_err (*jazz_engine_NvrVisitMut)(jazz_engine_NormalizedValueRef *v, void *ctx);
jz_err JE_IN(PredicateExpr__try_for_each_value)(const jazz_engine_PredicateExpr *p, jazz_engine_NvrVisit f, void *ctx);
jz_err JE_IN(PredicateExpr__try_for_each_value_mut)(jazz_engine_PredicateExpr *p, jazz_engine_NvrVisitMut f, void *ctx);
void JE_IN(ResultRowRef__Clone__clone)(jazz_engine_ResultRowRef *dst, const jazz_engine_ResultRowRef *src);
void JE_IN(ResultRowRef__drop)(jazz_engine_ResultRowRef *r);
void JE_IN(ResultId__Clone__clone)(jazz_engine_ResultId *dst, const jazz_engine_ResultId *src);
void JE_IN(ResultId__drop)(jazz_engine_ResultId *r);
bool JE_IN(ResultId__PartialEq__eq)(const jazz_engine_ResultId *a, const jazz_engine_ResultId *b);
int JE_IN(ResultId__Ord__cmp)(const jazz_engine_ResultId *a, const jazz_engine_ResultId *b);
void JE_IN(ResultId__Debug__fmt)(jz_str *out, const jazz_engine_ResultId *r);
void JE_IN(TypedOutputField__Clone__clone)(jazz_engine_TypedOutputField *dst, const jazz_engine_TypedOutputField *src);
void JE_IN(TypedOutputField__drop)(jazz_engine_TypedOutputField *f);
void JE_IN(TypedOutputField__Debug__fmt)(jz_str *out, const jazz_engine_TypedOutputField *f);
void JE_IN(RowSetExpr__Clone__clone)(jazz_engine_RowSetExpr *dst, const jazz_engine_RowSetExpr *src);
void JE_IN(RowSetExpr__drop)(jazz_engine_RowSetExpr *e);
void JE_IN(RowSetExpr__Debug__fmt)(jz_str *out, const jazz_engine_RowSetExpr *e);
void JE_IN(ClosurePathSegment__Clone__clone)(jazz_engine_ClosurePathSegment *dst, const jazz_engine_ClosurePathSegment *src);
bool JE_IN(ClosurePathSegment__PartialEq__eq)(const jazz_engine_ClosurePathSegment *a, const jazz_engine_ClosurePathSegment *b);
void JE_IN(ClosurePathSegment__drop)(jazz_engine_ClosurePathSegment *s);
void JE_IN(NormalizedRowSetShape__Clone__clone)(jazz_engine_NormalizedRowSetShape *dst, const jazz_engine_NormalizedRowSetShape *src);
void JE_IN(NormalizedRowSetShape__drop)(jazz_engine_NormalizedRowSetShape *s);
void JE_IN(NormalizedRowSetShape__Debug__fmt)(jz_str *out, const jazz_engine_NormalizedRowSetShape *s);
void JE_IN(ProgramClaimParam__Clone__clone)(jazz_engine_ProgramClaimParam *dst, const jazz_engine_ProgramClaimParam *src);
void JE_IN(ProgramClaimParam__drop)(jazz_engine_ProgramClaimParam *p);
void JE_IN(ProgramBinding__Clone__clone)(jazz_engine_ProgramBinding *dst, const jazz_engine_ProgramBinding *src);
void JE_IN(ProgramBinding__drop)(jazz_engine_ProgramBinding *b);
void JE_IN(RowSetProgramInput__Clone__clone)(jazz_engine_RowSetProgramInput *dst, const jazz_engine_RowSetProgramInput *src);
void JE_IN(RowSetProgramInput__drop)(jazz_engine_RowSetProgramInput *i);
void JE_IN(RowSetProgramInput__Debug__fmt)(jz_str *out, const jazz_engine_RowSetProgramInput *i);
void JE_IN(QueryAuthorizationMode__Debug__fmt)(jz_str *out, u8 m);

/* engine-internal short aliases */
#define sid_clone(d, s) JE_IN(SourceId__Clone__clone)(d, s)
#define sid_drop(s) JE_IN(SourceId__drop)(s)
#define sid_eq(a, b) JE_IN(SourceId__PartialEq__eq)(a, b)
#define sid_cmp(a, b) JE_IN(SourceId__Ord__cmp)(a, b)
#define sid_debug(o, s) JE_IN(SourceId__Debug__fmt)(o, s)
#define nvr_clone(d, s) JE_IN(NormalizedValueRef__Clone__clone)(d, s)
#define nvr_drop(v) JE_IN(NormalizedValueRef__drop)(v)
#define nvr_eq(a, b) JE_IN(NormalizedValueRef__PartialEq__eq)(a, b)
#define nvr_cmp(a, b) JE_IN(NormalizedValueRef__Ord__cmp)(a, b)
#define nvr_debug(o, v) JE_IN(NormalizedValueRef__Debug__fmt)(o, v)
#define pred_clone(d, s) JE_IN(PredicateExpr__Clone__clone)(d, s)
#define pred_drop(p) JE_IN(PredicateExpr__drop)(p)
#define pred_debug(o, p) JE_IN(PredicateExpr__Debug__fmt)(o, p)
#define tof_clone(d, s) JE_IN(TypedOutputField__Clone__clone)(d, s)
#define tof_drop(f) JE_IN(TypedOutputField__drop)(f)
void jazz_engine__sid_drop_v(void *s);
void jazz_engine__sid_clone_v(void *d, const void *s);
int jazz_engine__sid_cmp_v(const void *a, const void *b, void *ctx);
void jazz_engine__sid_debug_v(jz_str *out, const void *s);
void jazz_engine__nvr_drop_v(void *v);
void jazz_engine__nvr_clone_v(void *d, const void *s);
void jazz_engine__nvr_debug_v(jz_str *out, const void *v);
int jazz_engine__nvr_cmp_v(const void *a, const void *b, void *ctx);
void jazz_engine__pred_drop_v(void *p);
void jazz_engine__pred_clone_v(void *d, const void *s);
void jazz_engine__tof_drop_v(void *f);
void jazz_engine__tof_clone_v(void *d, const void *s);
void jazz_engine__nvrvec_clone(jazz_engine_NvrVec *dst, const jazz_engine_NvrVec *src);
void jazz_engine__nvrvec_drop(jazz_engine_NvrVec *v);
void jazz_engine__sidset_clone(jz_btree *dst, const jz_btree *src);
jazz_engine_SourceId jazz_engine__sid_cloned(const jazz_engine_SourceId *s);
jazz_engine_NormalizedValueRef jazz_engine__nvr_cloned(const jazz_engine_NormalizedValueRef *v);
jazz_engine_PredicateExpr jazz_engine__pred_cloned(const jazz_engine_PredicateExpr *p);
void jazz_engine__claim_param_drop_v(void *p);
void jazz_engine__claim_param_clone_v(void *d, const void *s);
void jazz_engine__expr_drop_v(void *e);
void jazz_engine__vsc_clone_v(void *d, const void *s);
void jazz_engine__vsc_drop_v(void *p);
void jazz_engine__vsc_debug_v(jz_str *out, const void *p);
void jazz_engine__projection_clone_v(void *d, const void *s);
void jazz_engine__projection_drop_v(void *p);
void jazz_engine__order_key_clone_v(void *d, const void *s);
void jazz_engine__order_key_drop_v(void *p);
void jazz_engine__aggregate_clone_v(void *d, const void *s);
void jazz_engine__aggregate_drop_v(void *p);
void jazz_engine__node_id_debug(jz_str *out, const jz_str *n);
void jazz_engine__frontier_id_debug(jz_str *out, const jz_str *f);
void jazz_engine__nvrs_debug(jz_str *out, const jazz_engine_NvrVec *v);
const char *jazz_engine__join_mode_name(u8 m);
const char *jazz_engine__correlation_name(u8 r);
const char *jazz_engine__aggregate_function_name(u8 f);

#endif
