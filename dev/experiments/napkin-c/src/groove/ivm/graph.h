/* graph.h: port of crates/groove/src/ivm/graph.rs (hash-consed IVM graph IR,
 * builder API, node descriptors and validation). */
#ifndef GROOVE_IVM_GRAPH_H
#define GROOVE_IVM_GRAPH_H
#include "jz_rt.h"
#include "../_ext_groove-ivm-db.h"
#include "op_types.h"
#include "routes.h"
#include "execution_layout.h"
#include "activation.h"

/* ---------------- GraphValidationError ---------------- */
enum {
    GROOVE_GRAPH_ERR_INVALID_NODE_OUTPUT = 0x0A01,
    GROOVE_GRAPH_ERR_FIELD_INDEX_OUT_OF_BOUNDS,
    GROOVE_GRAPH_ERR_INPUT_ARITY_MISMATCH,
    GROOVE_GRAPH_ERR_INPUT_DESCRIPTOR_ARITY_MISMATCH,
    GROOVE_GRAPH_ERR_JOIN_INPUT_DESCRIPTOR_MISMATCH,
    GROOVE_GRAPH_ERR_JOIN_KEY_ARITY_MISMATCH,
    GROOVE_GRAPH_ERR_OUTPUT_DESCRIPTOR_MISMATCH,
    GROOVE_GRAPH_ERR_OUTPUT_FIELD_COUNT_MISMATCH,
    GROOVE_GRAPH_ERR_COLLECT_BY_OUTPUT_DESCRIPTOR_MISMATCH,
    GROOVE_GRAPH_ERR_COLLECT_BY_KEY_FIELD_MUST_BE_SCALAR,
    GROOVE_GRAPH_ERR_TOP_BY_SORT_FIELD_MUST_BE_ORDERABLE,
    GROOVE_GRAPH_ERR_COLLECT_BY_INPUT_IS_TERMINAL,
};
void groove__ivm__graph__GraphValidationError__Debug__fmt(jz_str *out, const jz_err *e);

/* ---------------- builder payload types ---------------- */
typedef struct groove_GraphBuilder groove_GraphBuilder;
typedef groove_GraphBuilder *groove_GraphArc; /* Arc<GraphBuilder>: jz_rc payload */
typedef JZ_VEC(groove_GraphArc) groove_GraphArcVec;
typedef JZ_VEC(groove_GraphBuilder) groove_GraphBuilderVec;

typedef struct groove_TypedGraphTemplate groove_TypedGraphTemplate; /* template.h */
typedef struct groove_TemplateScalarArgument { groove_LiteralValue value; groove_ValueType value_type; } groove_TemplateScalarArgument;
/* Arc<[TemplateScalarArgument]>: jz_rc payload with inline length */
typedef struct groove_ScalarArgSlice { u32 len; groove_TemplateScalarArgument items[]; } groove_ScalarArgSlice;
groove_ScalarArgSlice *groove__ivm__template__arguments__scalar_slice_new(const groove_TemplateScalarArgument *items, u32 n);
void groove__ivm__template__arguments__TemplateScalarArgument__Clone__clone(groove_TemplateScalarArgument *dst, const groove_TemplateScalarArgument *src);
void groove__ivm__template__arguments__TemplateScalarArgument__drop(groove_TemplateScalarArgument *v);
bool groove__ivm__template__arguments__TemplateScalarArgument__PartialEq__eq(const groove_TemplateScalarArgument *a, const groove_TemplateScalarArgument *b);
void groove__ivm__template__arguments__TemplateScalarArgument__Hash__hash(const groove_TemplateScalarArgument *v, groove_Hasher *h);
void groove__ivm__template__arguments__TemplateScalarArgument__Debug__fmt(jz_str *out, const groove_TemplateScalarArgument *v);
/* TypedGraphTemplate impls (template.c) */
bool groove__ivm__template__TypedGraphTemplate__PartialEq__eq(const groove_TypedGraphTemplate *a, const groove_TypedGraphTemplate *b);
void groove__ivm__template__TypedGraphTemplate__Hash__hash(const groove_TypedGraphTemplate *v, groove_Hasher *h);
void groove__ivm__template__TypedGraphTemplate__Debug__fmt(jz_str *out, const groove_TypedGraphTemplate *v);

typedef struct groove_InputSourceId { u64 runtime_namespace, local; } groove_InputSourceId;

typedef struct groove_ProjectField {
    groove_ProjectExpr expression;
    jz_str output_name;
    groove_FieldIdentity output_identity;
} groove_ProjectField;
typedef JZ_VEC(groove_ProjectField) groove_ProjectFieldVec;

typedef struct groove_TopByOrder { groove_FieldRef field; u8 direction; } groove_TopByOrder;
typedef JZ_VEC(groove_TopByOrder) groove_TopByOrderVec;

typedef struct groove_CollectByField { groove_FieldRef field; jz_str output_name; bool unwrap_nullable; } groove_CollectByField;
typedef JZ_VEC(groove_CollectByField) groove_CollectByFieldVec;

typedef struct groove_CollectBySlotBuilder groove_CollectBySlotBuilder;
typedef JZ_VEC(groove_CollectBySlotBuilder) groove_CollectBySlotBuilderVec;
struct groove_CollectBySlotBuilder {
    groove_FieldRefVec group_cols;
    groove_FieldRefVec owner_key_cols;
    groove_CollectByFieldVec child_fields;
    jz_str collection_field;
    groove_CollectBySlotBuilderVec slots;
    groove_TopByOrderVec order_cols;
    groove_FieldRefVec tie_cols;
    bool has_presence_col; groove_FieldRef presence_col;
    bool has_reference_array_col; groove_FieldRef reference_array_col;
    bool reference_order;
    u64 offset;
    groove_TopByLimit limit;
};
#define GROOVE_MAX_COLLECT_BY_TREE_DEPTH 16u

typedef struct groove_CollectByBuilder {
    u8 mode;
    groove_FieldRefVec group_cols;
    groove_CollectByFieldVec parent_fields;
    groove_CollectByFieldVec child_fields;
    jz_str collection_field;
    groove_CollectBySlotBuilderVec slots;
    groove_CollectByFieldVec tuple_fields;
    groove_FieldRefVec occurrence_id_cols;
    groove_TopByOrderVec order_cols;
    groove_FieldRefVec tie_cols;
    u64 offset;
    groove_TopByLimit limit;
} groove_CollectByBuilder;

/* ---------------- GraphBuilder ---------------- */
enum {
    GROOVE_GB_TYPED_TEMPLATE, GROOVE_GB_TEMPLATE_INPUT, GROOVE_GB_TABLE, GROOVE_GB_INLINE_RECORDS,
    GROOVE_GB_INPUT_SOURCE, GROOVE_GB_INDEX, GROOVE_GB_FRONTIER_SOURCE, GROOVE_GB_BINDING_SOURCE,
    GROOVE_GB_RECURSIVE, GROOVE_GB_RECURSIVE_STEP_WITNESS, GROOVE_GB_FILTER, GROOVE_GB_UNWRAP_NULLABLE,
    GROOVE_GB_UNNEST, GROOVE_GB_VARIANT_PROJECT, GROOVE_GB_PROJECT, GROOVE_GB_STREAMING_CHECKSUM,
    GROOVE_GB_UNION, GROOVE_GB_JOIN, GROOVE_GB_SEMI_JOIN, GROOVE_GB_ANTI_JOIN, GROOVE_GB_LOOKUP,
    GROOVE_GB_ARG_MAX_BY, GROOVE_GB_ARG_MIN_BY, GROOVE_GB_TOP_BY, GROOVE_GB_COLLECT_BY, GROOVE_GB_AGGREGATE
};
struct groove_GraphBuilder {
    u8 tag;
    union {
        struct { groove_TypedGraphTemplate *program; groove_GraphArcVec inputs; groove_PredicateVec predicates; groove_ScalarArgSlice *scalars; } typed_template;
        struct { u32 slot; groove_RecordDescriptor output; groove_GraphArc input; /* NULL = None */ } template_input;
        struct { jz_str table; bool has_scan; groove_StaticScanSpec scan; bool has_variant_projection; jz_str variant_projection; } table;
        struct { groove_RecordDescriptor output; groove_BytesVec records; } inline_records;
        struct { groove_InputSourceId id; groove_RecordDescriptor output; } input_source;
        struct {
            jz_str table, index; bool has_scan; groove_StaticScanSpec scan;
            groove_ScanIntersectionVec intersections;
            bool has_candidate_filter; groove_IndexCandidateFilter candidate_filter;
            bool has_row_projection; jz_str row_projection;
        } index;
        struct { jz_str binding; groove_RecordDescriptor output; } frontier_source;
        struct { jz_str shape; groove_RecordDescriptor output; } binding_source;
        struct { groove_GraphArc seed, step, step_witness; jz_str frontier; u32 max_iters; bool truncate_at_max_iters; } recursive;
        struct { groove_GraphArc recursive; } recursive_step_witness;
        struct { groove_GraphArc input; groove_PredicateExpr predicate; u8 comparison; } filter;
        struct { groove_GraphArc input; groove_FieldRef field; } unwrap_nullable;
        struct { groove_GraphArc input; groove_FieldRef array_field; jz_str element_field; } unnest;
        struct { groove_GraphArc input; groove_FieldRef field; jz_str case_; } variant_project;
        struct { groove_GraphArc input; groove_ProjectFieldVec fields; } project;
        struct { groove_GraphArc input; groove_FieldRef field; jz_str output_field; u32 window_bytes, max_bytes_per_turn; } streaming_checksum;
        struct { groove_GraphArcVec inputs; } union_;
        struct { groove_GraphArc left, right; groove_FieldRefVec left_on, right_on; u8 comparison; } join; /* Join/SemiJoin/AntiJoin */
        struct { groove_GraphArc keys; groove_FieldRefVec keys_on; jz_str table; bool has_scan; groove_StaticScanSpec scan; bool has_variant_projection; jz_str variant_projection; } lookup;
        struct { groove_GraphArc input; groove_FieldRefVec group_cols, order_cols; } arg_by; /* ArgMaxBy/ArgMinBy */
        struct { groove_GraphArc input; groove_FieldRefVec group_cols; groove_TopByOrderVec order_cols; groove_FieldRefVec tie_cols; u64 offset; groove_TopByLimit limit; } top_by;
        struct { groove_GraphArc input; groove_CollectByBuilder *collect; } collect_by;
        struct { groove_GraphArc input; groove_FieldRefVec group_cols; groove_AggregateExprVec aggregates; } aggregate;
    } u;
};

/* Arc helpers */
groove_GraphArc groove__ivm__graph__arc_new(groove_GraphBuilder value);
groove_GraphArc groove__ivm__graph__arc_clone(groove_GraphArc a);
void groove__ivm__graph__arc_release(groove_GraphArc a);
void groove__ivm__graph__GraphArcVec__drop(groove_GraphArcVec *v);

/* derives */
void groove__ivm__graph__GraphBuilder__Clone__clone(groove_GraphBuilder *dst, const groove_GraphBuilder *src);
void groove__ivm__graph__GraphBuilder__drop(groove_GraphBuilder *v);
void groove__ivm__graph__GraphBuilder__drop_v(void *v);
bool groove__ivm__graph__GraphBuilder__PartialEq__eq(const groove_GraphBuilder *a, const groove_GraphBuilder *b);
void groove__ivm__graph__GraphBuilder__Hash__hash(const groove_GraphBuilder *v, groove_Hasher *h);
void groove__ivm__graph__GraphBuilder__Debug__fmt(jz_str *out, const groove_GraphBuilder *v);
void groove__ivm__graph__ProjectField__Clone__clone(groove_ProjectField *dst, const groove_ProjectField *src);
void groove__ivm__graph__ProjectField__drop(groove_ProjectField *v);
bool groove__ivm__graph__ProjectField__PartialEq__eq(const groove_ProjectField *a, const groove_ProjectField *b);
void groove__ivm__graph__CollectBySlotBuilder__Clone__clone(groove_CollectBySlotBuilder *dst, const groove_CollectBySlotBuilder *src);
void groove__ivm__graph__CollectBySlotBuilder__drop(groove_CollectBySlotBuilder *v);
bool groove__ivm__graph__CollectBySlotBuilder__PartialEq__eq(const groove_CollectBySlotBuilder *a, const groove_CollectBySlotBuilder *b);
void groove__ivm__graph__CollectByBuilder__Clone__clone(groove_CollectByBuilder *dst, const groove_CollectByBuilder *src);
void groove__ivm__graph__CollectByBuilder__drop(groove_CollectByBuilder *v);
bool groove__ivm__graph__CollectByBuilder__PartialEq__eq(const groove_CollectByBuilder *a, const groove_CollectByBuilder *b);
void groove__ivm__graph__fieldrefvec_clone(groove_FieldRefVec *dst, const groove_FieldRefVec *src);
void groove__ivm__graph__fieldrefvec_drop(groove_FieldRefVec *v);
void groove__ivm__graph__fieldrefvec_from_names(groove_FieldRefVec *out, const jz_str *names, u32 n);

/* InputSourceId */
groove_InputSourceId groove__ivm__graph__InputSourceId__new(u64 runtime_namespace, u64 local);
bool groove__ivm__graph__InputSourceId__belongs_to(groove_InputSourceId self, u64 runtime_namespace);
bool groove__ivm__graph__InputSourceId__was_allocated_by(groove_InputSourceId self, u64 runtime_namespace, u64 next_local);
groove_BindingSourceKey groove__ivm__graph__InputSourceId__binding_key(groove_InputSourceId self);
jz_str groove__ivm__graph__InputSourceId__diagnostic_name(groove_InputSourceId self);

/* CollectBySlotBuilder */
void groove__ivm__graph__CollectBySlotBuilder__new(groove_CollectBySlotBuilder *out, const jz_str *group_cols, u32 n_group,
                                                   groove_CollectByFieldVec child_fields, jz_str collection_field,
                                                   groove_CollectBySlotBuilderVec slots, groove_TopByOrderVec order_cols,
                                                   const jz_str *tie_cols, u32 n_tie, u64 offset, groove_TopByLimit limit);
void groove__ivm__graph__CollectBySlotBuilder__with_presence_col(groove_CollectBySlotBuilder *self, jz_str presence_col);
void groove__ivm__graph__CollectBySlotBuilder__with_owner_key_cols(groove_CollectBySlotBuilder *self, const jz_str *cols, u32 n);

/* GraphBuilder constructors (names are moved in; string arrays are borrowed) */
groove_GraphBuilder groove__ivm__graph__GraphBuilder__table(jz_str table);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__table_scan(jz_str table, groove_StaticScanSpec scan);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__variant_source(jz_str table, jz_str projection_target);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__variant_source_scan(jz_str table, jz_str projection_target, groove_StaticScanSpec scan);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__inline_records(groove_RecordDescriptor output, groove_BytesVec records);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__input_source(groove_InputSourceId id, groove_RecordDescriptor output);
jz_err groove__ivm__graph__GraphBuilder__values(groove_RecordDescriptor output, const groove_ValueVec *rows, u32 n_rows, groove_GraphBuilder *out);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__index(jz_str table, jz_str index);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__index_scan(jz_str table, jz_str index, groove_StaticScanSpec scan);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__variant_index_scan(jz_str table, jz_str index, jz_str projection_target, groove_StaticScanSpec scan);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__variant_index_intersection_scan(jz_str table, jz_str index, groove_StaticScanSpec scan, groove_ScanIntersectionVec intersections, jz_str projection_target);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__variant_index_candidate_scan(jz_str table, jz_str index, groove_StaticScanSpec scan, groove_IndexCandidateFilter candidate_filter, jz_str projection_target);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__frontier_source(jz_str binding, groove_RecordDescriptor output);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__binding_source(jz_str shape, groove_RecordDescriptor output);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__recursive(groove_GraphBuilder seed, groove_GraphBuilder step, jz_str frontier, u32 max_iters);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__recursive_bounded(groove_GraphBuilder seed, groove_GraphBuilder step, jz_str frontier, u32 max_iters);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__recursive_with_step_witness(groove_GraphBuilder seed, groove_GraphBuilder step, groove_GraphBuilder step_witness, jz_str frontier, u32 max_iters, bool truncate_at_max_iters);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__recursive_step_witness(groove_GraphBuilder recursive);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__union(groove_GraphBuilder *inputs, u32 n);
typedef JZ_VEC(const groove_GraphBuilder *) groove_GraphRefVec;
void groove__ivm__graph__GraphBuilder__postorder(const groove_GraphBuilder *self, groove_GraphRefVec *out);
typedef void (*groove_GraphVisitFn)(groove_GraphArc const *input, void *ctx);
void groove__ivm__graph__GraphBuilder__visit_inputs(const groove_GraphBuilder *self, groove_GraphVisitFn visit, void *ctx);
typedef groove_GraphArc (*groove_GraphMapFn)(groove_GraphArc const *input, void *ctx);
void groove__ivm__graph__GraphBuilder__map_inputs(const groove_GraphBuilder *self, groove_GraphMapFn map, void *ctx, groove_GraphBuilder *out);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__join(groove_GraphBuilder left, groove_GraphBuilder right, const jz_str *left_on, u32 nl, const jz_str *right_on, u32 nr);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__policy_join(groove_GraphBuilder left, groove_GraphBuilder right, const jz_str *left_on, u32 nl, const jz_str *right_on, u32 nr);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__semi_join(groove_GraphBuilder left, groove_GraphBuilder right, const jz_str *left_on, u32 nl, const jz_str *right_on, u32 nr);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__anti_join(groove_GraphBuilder left, groove_GraphBuilder right, const jz_str *left_on, u32 nl, const jz_str *right_on, u32 nr);
bool groove__ivm__graph__GraphBuilder__lookup(groove_GraphBuilder right, groove_GraphBuilder keys, const jz_str *keys_on, u32 n, groove_GraphBuilder *out);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__arg_max_by(groove_GraphBuilder input, const jz_str *group_cols, u32 ng, const jz_str *order_cols, u32 no);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__arg_min_by(groove_GraphBuilder input, const jz_str *group_cols, u32 ng, const jz_str *order_cols, u32 no);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__top_by(groove_GraphBuilder input, const jz_str *group_cols, u32 ng, groove_TopByOrderVec order_cols, const jz_str *tie_cols, u32 nt, u64 offset, groove_TopByLimit limit);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__collect_by(groove_GraphBuilder input, const jz_str *group_cols, u32 ng, groove_CollectByFieldVec parent_fields, groove_CollectByFieldVec child_fields, jz_str collection_field, groove_TopByOrderVec order_cols, const jz_str *tie_cols, u32 nt, u64 offset, groove_TopByLimit limit);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__collect_by_tree(groove_GraphBuilder input, const jz_str *group_cols, u32 ng, groove_CollectByFieldVec parent_fields, groove_CollectBySlotBuilderVec slots);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__collect_by_tree_ordered(groove_GraphBuilder input, const jz_str *group_cols, u32 ng, groove_CollectByFieldVec parent_fields, groove_CollectBySlotBuilderVec slots, groove_TopByOrderVec order_cols, const jz_str *tie_cols, u32 nt, u64 offset, groove_TopByLimit limit);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__collect_root_ordered(groove_GraphBuilder input, const jz_str *group_cols, u32 ng, groove_CollectByFieldVec parent_fields, groove_TopByOrderVec order_cols, const jz_str *tie_cols, u32 nt, u64 offset, groove_TopByLimit limit);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__collect_by_expand(groove_GraphBuilder input, const jz_str *group_cols, u32 ng, groove_CollectByFieldVec tuple_fields, const jz_str *occurrence_id_cols, u32 no, groove_TopByOrderVec order_cols, const jz_str *tie_cols, u32 nt, u64 offset, groove_TopByLimit limit);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__expand_by(groove_GraphBuilder input, const jz_str *group_cols, u32 ng, groove_CollectByFieldVec tuple_fields, const jz_str *occurrence_id_cols, u32 no, groove_TopByOrderVec order_cols, const jz_str *tie_cols, u32 nt, u64 offset, groove_TopByLimit limit);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__aggregate(groove_GraphBuilder input, const jz_str *group_cols, u32 ng, groove_AggregateExprVec aggregates);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__filter(groove_GraphBuilder self, groove_PredicateExpr predicate);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__policy_filter(groove_GraphBuilder self, groove_PredicateExpr predicate);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__unwrap_nullable(groove_GraphBuilder self, jz_str field);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__unnest(groove_GraphBuilder self, jz_str array_field, jz_str element_field);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__variant_project(groove_GraphBuilder self, jz_str field, jz_str case_);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__project(groove_GraphBuilder self, const jz_str *fields, u32 n);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__project_fields(groove_GraphBuilder self, groove_ProjectFieldVec fields);
groove_GraphBuilder groove__ivm__graph__GraphBuilder__streaming_checksum(groove_GraphBuilder self, jz_str field, jz_str output_field, u32 window_bytes, u32 max_bytes_per_turn);

/* ProjectField constructors */
groove_ProjectField groove__ivm__graph__ProjectField__record_field(jz_str source, const jz_str *path, u32 n, jz_str output_name);
groove_ProjectField groove__ivm__graph__ProjectField__named(jz_str name);
groove_ProjectField groove__ivm__graph__ProjectField__named_with_identity(jz_str name, groove_FieldIdentity output_identity);
groove_ProjectField groove__ivm__graph__ProjectField__renamed(jz_str source_name, jz_str output_name);
groove_ProjectField groove__ivm__graph__ProjectField__renamed_resolved(u32 source_idx, jz_str output_name);
groove_ProjectField groove__ivm__graph__ProjectField__nullable_flat_resolved(u32 source_idx, jz_str output_name);
groove_ProjectField groove__ivm__graph__ProjectField__renamed_with_identity(jz_str source_name, jz_str output_name, groove_FieldIdentity output_identity);
groove_ProjectField groove__ivm__graph__ProjectField__literal(jz_str output_name, groove_LiteralValue value);
groove_ProjectField groove__ivm__graph__ProjectField__literal_with_identity(jz_str output_name, groove_LiteralValue value, groove_FieldIdentity output_identity);
groove_ProjectField groove__ivm__graph__ProjectField__literal_typed(jz_str output_name, groove_LiteralValue value, groove_ValueType value_type);
groove_ProjectField groove__ivm__graph__ProjectField__null(jz_str output_name);
groove_ProjectField groove__ivm__graph__ProjectField__null_typed(jz_str output_name, groove_ValueType value_type);
groove_ProjectField groove__ivm__graph__ProjectField__nullable(jz_str source_name, jz_str output_name);
groove_ProjectField groove__ivm__graph__ProjectField__nullable_resolved(u32 source_idx, jz_str output_name);
groove_ProjectField groove__ivm__graph__ProjectField__nullable_flat(jz_str source_name, jz_str output_name);
groove_ProjectField groove__ivm__graph__ProjectField__enum_tag_remap(jz_str source_name, jz_str output_name, groove_OptU8Vec tags);
groove_ProjectField groove__ivm__graph__ProjectField__enum_remap(jz_str source_name, jz_str output_name, groove_OptU32Vec tags);
groove_ProjectField groove__ivm__graph__ProjectField__recursive_enum_remap(jz_str source_name, jz_str output_name, groove_ValueType target, groove_RecursiveEnumRemaps remaps);
groove_ProjectField groove__ivm__graph__ProjectField__recursive_enum_remap_omitting_unrepresentable(jz_str source_name, jz_str output_name, groove_ValueType target, groove_RecursiveEnumRemaps remaps);
const groove_FieldRef *groove__ivm__graph__ProjectField__source(const groove_ProjectField *self);

/* CollectByField / TopByOrder */
groove_CollectByField groove__ivm__graph__CollectByField__named(jz_str field);
groove_CollectByField groove__ivm__graph__CollectByField__renamed(jz_str field, jz_str output_name);
groove_CollectByField groove__ivm__graph__CollectByField__renamed_unwrap_nullable(jz_str field, jz_str output_name);
groove_TopByOrder groove__ivm__graph__TopByOrder__asc(jz_str field);
groove_TopByOrder groove__ivm__graph__TopByOrder__desc(jz_str field);

/* ---------------- OpType / descriptors ---------------- */
typedef struct groove_ArrangeOp { groove_StrVec fields; u8 comparison; } groove_ArrangeOp;
enum {
    GROOVE_OP_TABLE_SOURCE, GROOVE_OP_INDEX_SOURCE, GROOVE_OP_INLINE_RECORDS, GROOVE_OP_FRONTIER_SOURCE,
    GROOVE_OP_BINDING_SOURCE, GROOVE_OP_ARRANGE, GROOVE_OP_ARG_MAX_BY, GROOVE_OP_ARG_MIN_BY, GROOVE_OP_TOP_BY,
    GROOVE_OP_COLLECT_BY, GROOVE_OP_RECURSIVE, GROOVE_OP_RECURSIVE_STEP_WITNESS, GROOVE_OP_PERSIST,
    GROOVE_OP_FILTER, GROOVE_OP_MAP_PROJECT, GROOVE_OP_STREAMING_CHECKSUM, GROOVE_OP_UNWRAP_NULLABLE,
    GROOVE_OP_UNNEST, GROOVE_OP_VARIANT_PROJECT, GROOVE_OP_INDEX_BY, GROOVE_OP_JOIN, GROOVE_OP_SEMI_JOIN,
    GROOVE_OP_ANTI_JOIN, GROOVE_OP_LOOKUP, GROOVE_OP_UNION, GROOVE_OP_NEGATE, GROOVE_OP_DISTINCT,
    GROOVE_OP_AGGREGATE
};
typedef struct groove_OpType {
    u8 tag;
    union {
        groove_TableSourceOp table_source;
        groove_IndexSourceOp index_source;
        groove_InlineRecordsOp inline_records;
        groove_FrontierSourceOp frontier_source;
        groove_BindingSourceOp binding_source;
        groove_ArrangeOp arrange;
        groove_ArgExtremumByOp arg_by;   /* ArgMaxBy / ArgMinBy */
        groove_TopByOp top_by;
        groove_CollectByOp *collect_by;  /* Box<CollectByOp> */
        groove_RecursiveOp recursive;
        groove_PersistOp persist;
        groove_FilterOp filter;
        groove_MapProjectOp map_project;
        groove_StreamingChecksumOp streaming_checksum;
        groove_UnwrapNullableOp unwrap_nullable;
        groove_UnnestOp unnest;
        groove_VariantProjectOp variant_project;
        groove_IndexByOp index_by;
        groove_JoinOp join;              /* Join / SemiJoin / AntiJoin */
        groove_LookupOp lookup;
        groove_AggregateOp aggregate;
    } u;
} groove_OpType;
void groove__ivm__graph__OpType__Clone__clone(groove_OpType *dst, const groove_OpType *src);
void groove__ivm__graph__OpType__drop(groove_OpType *v);
bool groove__ivm__graph__OpType__PartialEq__eq(const groove_OpType *a, const groove_OpType *b);
void groove__ivm__graph__OpType__Hash__hash(const groove_OpType *v, groove_Hasher *h);
void groove__ivm__graph__OpType__Debug__fmt(jz_str *out, const groove_OpType *v);

enum { GROOVE_NODE_OUTPUT_RECORDS, GROOVE_NODE_OUTPUT_ARRANGEMENT };
typedef struct groove_NodeOutput { u8 tag; groove_RecordDescriptor records; } groove_NodeOutput; /* Arrangement(ArrangementDescriptor{records}) */
groove_RecordDescriptor groove__ivm__graph__NodeOutput__records(groove_NodeOutput self);
groove_NodeOutput groove__ivm__graph__NodeOutput__From_RecordDescriptor__from(groove_RecordDescriptor d);

typedef struct groove_NodeDescriptor {
    groove_OpType operator_;
    groove_NodeIdVec inputs;
    groove_NodeOutput output;
} groove_NodeDescriptor;
groove_NodeDescriptor groove__ivm__graph__NodeDescriptor__new(groove_OpType op, const groove_NodeId *inputs, u32 n, groove_NodeOutput output);
groove_NodeId groove__ivm__graph__NodeDescriptor__node_id(const groove_NodeDescriptor *self);
groove_NodeId groove__ivm__graph__NodeDescriptor__node_id_of(const groove_OpType *op, const groove_NodeId *inputs, u32 n, const groove_NodeOutput *output);
jz_err groove__ivm__graph__NodeDescriptor__validate(const groove_NodeDescriptor *self, const groove_NodeOutput *input_outputs, u32 n);
void groove__ivm__graph__NodeDescriptor__Clone__clone(groove_NodeDescriptor *dst, const groove_NodeDescriptor *src);
void groove__ivm__graph__NodeDescriptor__drop(groove_NodeDescriptor *v);
bool groove__ivm__graph__NodeDescriptor__PartialEq__eq(const groove_NodeDescriptor *a, const groove_NodeDescriptor *b);
void groove__ivm__graph__NodeDescriptor__Debug__fmt(jz_str *out, const groove_NodeDescriptor *v);

enum { GROOVE_DURABILITY_EPHEMERAL, GROOVE_DURABILITY_DURABLE };
typedef struct groove_NodeDurability { u8 tag; groove_DurableStorage storage; } groove_NodeDurability;
void groove__ivm__graph__NodeDurability__Clone__clone(groove_NodeDurability *dst, const groove_NodeDurability *src);
void groove__ivm__graph__NodeDurability__drop(groove_NodeDurability *v);

enum { GROOVE_RETAINER_SUBSCRIPTION, GROOVE_RETAINER_HYDRATION, GROOVE_RETAINER_PREPARED_SHAPE, GROOVE_RETAINER_DURABLE_SCHEMA_OBJECT };
typedef struct groove_Retainer { u8 tag; jz_str name; } groove_Retainer;
u64 groove__ivm__graph__Retainer__hash_v(const void *k);
bool groove__ivm__graph__Retainer__eq_v(const void *a, const void *b);
void groove__ivm__graph__Retainer__drop_v(void *k);
void groove__ivm__graph__Retainer__Clone__clone(groove_Retainer *dst, const groove_Retainer *src);
void groove__ivm__graph__Retainer__Debug__fmt(jz_str *out, const groove_Retainer *v);
extern const jz_hmap_desc groove__ivm__graph__RETAINER_SET_DESC;

typedef struct groove_GraphNode {
    groove_NodeId id;
    groove_NodeDescriptor descriptor;
    groove_NodeDurability durability;
    jz_hmap children; /* HashSet<NodeId> */
} groove_GraphNode;
bool groove__ivm__graph__GraphNode__is_durable(const groove_GraphNode *self);
void groove__ivm__graph__GraphNode__drop(groove_GraphNode *self);

/* ---------------- IvmGraph ---------------- */
struct groove_IvmGraph {
    groove_ActivationCache activations;
    groove_ExecutionLayoutCache execution_layouts;
    jz_hmap nodes;             /* HashMap<NodeId, GraphNode> */
    jz_hmap table_sources;     /* HashMap<String, HashSet<NodeId>> */
    jz_hmap binding_sources;   /* HashMap<BindingSourceKey, HashSet<NodeId>> */
    jz_hmap frontier_sources;  /* HashMap<String, HashSet<NodeId>> */
    groove_RouteIndex routes;
    groove_NodeIdVec added_since_drain;
};
const groove_RouteIndex *groove__ivm__graph__IvmGraph__routes(const groove_IvmGraph *self);
void groove__ivm__graph__IvmGraph__add_route_barrier(groove_IvmGraph *self, groove_NodeId terminal, groove_NodeId barrier,
                                                     groove_UsizeVec field_indices, groove_ValueTypeVec field_types, jz_bytes key,
                                                     bool has_root_ordering_node, groove_NodeId root_ordering_node);
void groove__ivm__graph__IvmGraph__release_route_barrier(groove_IvmGraph *self, groove_NodeId barrier);
void groove__ivm__graph__IvmGraph__downstream_through_routes(const groove_IvmGraph *self, const groove_NodeId *roots, u32 n, jz_hmap *out);
void groove__ivm__graph__IvmGraph__affected_nodes_through_routes(const groove_IvmGraph *self, const jz_str *tables, u32 nt,
                                                                 const groove_BindingSourceKey *bindings, u32 nb, jz_hmap *out);
bool groove__ivm__graph__IvmGraph__execution_layout(const groove_IvmGraph *self, const groove_NodeId *roots, u32 n,
                                                    groove_ExecutionLayout **out, groove_NodeId *err);
void groove__ivm__graph__IvmGraph__execution_layout_counters(const groove_IvmGraph *self, u64 *builds, u64 *hits);
void groove__ivm__graph__IvmGraph__new(groove_IvmGraph *self);
void groove__ivm__graph__IvmGraph__drop(groove_IvmGraph *self);
groove_NodeId groove__ivm__graph__IvmGraph__dedup_node(groove_IvmGraph *self, groove_NodeDescriptor descriptor, groove_NodeDurability durability);
groove_NodeId groove__ivm__graph__IvmGraph__dedup_node_parts(groove_IvmGraph *self, const groove_OpType *op, groove_NodeIdVec inputs,
                                                             groove_NodeOutput output, groove_NodeDurability durability);
jz_err groove__ivm__graph__IvmGraph__validate_node(const groove_IvmGraph *self, const groove_NodeDescriptor *descriptor);
const groove_GraphNode *groove__ivm__graph__IvmGraph__node(const groove_IvmGraph *self, groove_NodeId id);
const jz_hmap *groove__ivm__graph__IvmGraph__nodes(const groove_IvmGraph *self);
jz_hmap *groove__ivm__graph__IvmGraph__affected_nodes(const groove_IvmGraph *self, const jz_str *tables, u32 nt,
                                                     const groove_BindingSourceKey *bindings, u32 nb);
bool groove__ivm__graph__IvmGraph__activation_plan(const groove_IvmGraph *self, const jz_str *tables, u32 nt,
                                                   const groove_BindingSourceKey *bindings, u32 nb,
                                                   groove_ActivationPlan **out, groove_NodeId *err);
groove_NodeIdVec groove__ivm__graph__IvmGraph__take_added_nodes(groove_IvmGraph *self);
void groove__ivm__graph__IvmGraph__mark_ancestors(const groove_IvmGraph *self, groove_NodeId id, jz_hmap *retained);
void groove__ivm__graph__IvmGraph__remove_node(groove_IvmGraph *self, groove_NodeId id);

/* graph/join_chains.c */
groove_GraphBuilder groove__ivm__graph__join_chains__GraphBuilder__narrow_projected_join_chain(groove_GraphBuilder self);

#endif
