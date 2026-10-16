/* groove::ivm::runtime::record_projection */
#ifndef GROOVE_IVM_RUNTIME_RECORD_PROJECTION_H
#define GROOVE_IVM_RUNTIME_RECORD_PROJECTION_H

#include "subscriptions.h"

typedef struct groove_RootIdentity {
    groove_UsizeVec fields;
    usize group_len;
    groove_NodeIdVec chain;
    bool filtered;
    bool projected;
} groove_RootIdentity;
void groove__ivm__runtime__record_projection__RootIdentity__drop(groove_RootIdentity *r);

enum { GROOVE_WR_YES, GROOVE_WR_NO, GROOVE_WR_UNKNOWN };

/* RootKeyOf = dyn Fn(&[u8]) -> Result<Vec<u8>> */
typedef struct groove_RootKeyOf { jz_err (*call)(void *ctx, const u8 *record, u32 len, jz_bytes *out); void *ctx; } groove_RootKeyOf;

typedef struct groove_IndexedKey { usize index; jz_bytes key; } groove_IndexedKey;
typedef JZ_VEC(groove_IndexedKey) groove_IndexedKeyVec;
typedef struct groove_GroupChanges { groove_IndexedKeyVec removed, added; } groove_GroupChanges;
void groove__ivm__runtime__record_projection__GroupChanges__drop(groove_GroupChanges *c);

enum { GROOVE_SSB_PREFIX, GROOVE_SSB_RANGE };
typedef struct groove_StaticScanBounds { u32 tag; jz_bytes prefix; /* = start */ jz_bytes end; } groove_StaticScanBounds;
void groove__ivm__runtime__record_projection__StaticScanBounds__drop(groove_StaticScanBounds *b);

jz_err groove__ivm__runtime__record_projection__output_is_structured_collect_by(const groove_IvmGraph *g, groove_NodeId node, bool *out);
void groove__ivm__runtime__record_projection__top_by_identity_fields(const groove_TopByOp *top_by, usize width, groove_UsizeVec *out);
jz_err groove__ivm__runtime__record_projection__output_consumes_root_positions(const groove_IvmGraph *g, groove_NodeId output, groove_NodeId ordering, bool *out);
jz_err groove__ivm__runtime__record_projection__root_identity_fields(const groove_IvmGraph *g, groove_NodeId output, groove_NodeId ordering, bool *some, groove_RootIdentity *out);
jz_err groove__ivm__runtime__record_projection__window_record_reaches_output(const groove_IvmGraph *g, const groove_NodeId *chain, u32 n, groove_RecordDescriptor d, const jz_bytes *record, u32 *out);
jz_err groove__ivm__runtime__record_projection__project_window_record(const groove_IvmGraph *g, const groove_NodeId *chain, u32 n, groove_RecordDescriptor d, const u8 *record, u32 len, bool *some, groove_RecordDescriptor *out_desc, jz_bytes *out);
jz_err groove__ivm__runtime__record_projection__apply_group_window_ordering(const groove_WindowedRecord *before, u32 nb, const groove_WindowedRecord *after, u32 na, groove_RootKeyOf key_of, groove_RecordDescriptor root_descriptor, groove_TerminalDeltas *terminal);
bool groove__ivm__runtime__record_projection__apply_changed_group_ordering(const groove_GroupChanges *changes, groove_RecordDescriptor root_descriptor, groove_TerminalDeltas *terminal);
jz_err groove__ivm__runtime__record_projection__project_descriptor(groove_RecordDescriptor input, const groove_ProjectField *fields, u32 n, groove_RecordDescriptor *out);
jz_err groove__ivm__runtime__record_projection__collect_by_descriptor(groove_RecordDescriptor input, const groove_CollectByField *parent, u32 np, const groove_CollectByField *child, u32 nc, const jz_str *collection_field, groove_RecordDescriptor *out);
jz_err groove__ivm__runtime__record_projection__collect_by_root_descriptor(groove_RecordDescriptor input, const groove_CollectByField *parent, u32 np, groove_RecordDescriptor *out);
jz_err groove__ivm__runtime__record_projection__collect_by_tree_descriptor(groove_RecordDescriptor input, const groove_CollectByField *parent, u32 np, const groove_CollectBySlotBuilder *slots, u32 ns, groove_RecordDescriptor *out);
jz_err groove__ivm__runtime__record_projection__collect_by_slots(groove_RecordDescriptor input, const groove_CollectBySlotBuilder *b, u32 nb, const usize *owner, u32 nowner, usize depth, groove_CollectBySlotVec *out);
jz_err groove__ivm__runtime__record_projection__collect_by_expand_descriptor(groove_RecordDescriptor input, const groove_CollectByField *tuple, u32 n, groove_RecordDescriptor *out);
jz_err groove__ivm__runtime__record_projection__collect_by_projections(groove_RecordDescriptor input, const groove_CollectByField *fields, u32 n, groove_CollectByProjectionVec *out);
jz_err groove__ivm__runtime__record_projection__validate_collect_by_key_types(groove_RecordDescriptor input, const usize *idx, u32 n);
jz_err groove__ivm__runtime__record_projection__validate_collect_by_terminality(const groove_GraphBuilder *g);
jz_err groove__ivm__runtime__record_projection__project_field_expr(groove_RecordDescriptor input, const groove_ProjectField *field, groove_ProjectExpr *out);
jz_err groove__ivm__runtime__record_projection__project_record(const groove_ProjectionExpr *exprs, u32 n, const usize (*mapping)[2], u32 nmap, groove_RecordDescriptor output_desc, groove_RecordDescriptor input_desc, const u8 *record, u32 len, jz_bytes *out);
jz_err groove__ivm__runtime__record_projection__project_field_value(const groove_ProjectionExpr *expr, usize output_idx, groove_RecordDescriptor output_desc, groove_RecordDescriptor input_desc, const u8 *record, u32 len, groove_Value *out);
jz_err groove__ivm__runtime__record_projection__remap_enum_tag(groove_Value value, const i32 *tags, u32 n, groove_Value *out);
jz_err groove__ivm__runtime__record_projection__remap_enum(groove_Value value, const i32 *tags, u32 n, groove_Value *out);
jz_err groove__ivm__runtime__record_projection__remap_recursive_enum_value(groove_Value value, const groove_ValueType *source, const groove_ValueType *target, const groove_RecursiveEnumRemaps *remaps, const char *path, u32 path_len, groove_Value *out);
bool groove__ivm__runtime__record_projection__projection_uses_raw_copy(const groove_ProjectionExpr *exprs, u32 n, u32 nmap, groove_RecordDescriptor output_desc);
jz_err groove__ivm__runtime__record_projection__raw_projection_fields(const groove_MapProjectOp *project, groove_RecordDescriptor input_desc, groove_RecordDescriptor output_desc, groove_PreparedProjection **out /* NULL = None */);
jz_err groove__ivm__runtime__record_projection__encode_projection_field_value(groove_RecordDescriptor output_desc, usize field_idx, groove_Value value, jz_bytes *out);
jz_err groove__ivm__runtime__record_projection__resolve_field_ref(groove_RecordDescriptor d, const groove_FieldRef *f, usize *out);
bool groove__ivm__runtime__record_projection__resolve_field_name(groove_RecordDescriptor d, const char *name, u32 len, usize *out);
jz_err groove__ivm__runtime__record_projection__field_ref_name(groove_RecordDescriptor d, const groove_FieldRef *f, jz_str *out);
jz_err groove__ivm__runtime__record_projection__field_name_at(groove_RecordDescriptor d, usize index, jz_str *out);
jz_err groove__ivm__runtime__record_projection__unwrap_nullable_descriptor(groove_RecordDescriptor input, usize field_idx, groove_RecordDescriptor *out);
jz_err groove__ivm__runtime__record_projection__unnest_descriptor(groove_RecordDescriptor input, usize array_field_idx, const jz_str *element_field, groove_RecordDescriptor *out);
jz_err groove__ivm__runtime__record_projection__variant_project_descriptor(groove_RecordDescriptor input, const groove_FieldRef *field, const jz_str *case_name, groove_RecordDescriptor *out);
jz_err groove__ivm__runtime__record_projection__aggregate_descriptor(groove_RecordDescriptor input, const groove_FieldRef *group_cols, u32 ng, const groove_AggregateExpr *aggs, u32 na, groove_RecordDescriptor *out);
groove_RecordDescriptor groove__ivm__runtime__record_projection__index_record_descriptor(void);
const jz_str *groove__ivm__runtime__record_projection__variant_projection_target_name(const groove_VariantProjectionTarget *t);
jz_err groove__ivm__runtime__record_projection__schema_index_input_fields(const groove_TableSchema *t, const groove_IndexSchema *i, groove_StrVec *out);
jz_err groove__ivm__runtime__record_projection__schema_index_input_descriptor(const groove_TableSchema *t, const groove_IndexSchema *i, groove_RecordDescriptor *out);
jz_err groove__ivm__runtime__record_projection__apply_index_by(const groove_IndexByOp *ib, groove_RecordDescriptor input, const groove_RecordDelta *deltas, u32 n, groove_RecordDeltaVec *out);
jz_err groove__ivm__runtime__record_projection__scan_bounds(const groove_StaticScanSpec *scan, groove_StaticScanBounds *out);
bool groove__ivm__runtime__record_projection__scan_max_items(const groove_StaticScanSpec *scan /* nullable */, usize *out);
bool groove__ivm__runtime__record_projection__scan_reversed(const groove_StaticScanSpec *scan /* nullable */);
jz_err groove__ivm__runtime__record_projection__key_matches_static_scan(const u8 *key, u32 len, const groove_StaticScanSpec *scan, bool *out);
jz_err groove__ivm__runtime__record_projection__persisted_index_scan_bounds(u32 index_id, const groove_StaticScanSpec *scan, groove_StaticScanBounds *out);

#endif
