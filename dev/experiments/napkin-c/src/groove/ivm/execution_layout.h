/* execution_layout.h: port of crates/groove/src/ivm/execution_layout.rs. */
#ifndef GROOVE_IVM_EXECUTION_LAYOUT_H
#define GROOVE_IVM_EXECUTION_LAYOUT_H
#include "routes.h"

typedef struct groove_IvmGraph groove_IvmGraph;

/* Arc<ExecutionLayout>: a jz_rc payload. */
typedef struct groove_ExecutionLayout {
    groove_NodeIdVec nodes;
    jz_hmap slots; /* FxHashMap<NodeId, usize> */
    groove_NodeIdVec roots;
    groove_UsizeVec input_counts;
    groove_UsizeVec input_offsets;
    groove_UsizeVec input_slots;
    groove_UsizeVec source_slots;
    groove_OptU32Vec pipeline_predecessors;
    groove_UsizeVec dependent_offsets;
    groove_UsizeVec dependent_slots;
} groove_ExecutionLayout;

/* ExecutionLayoutCache(Mutex<CachedLayouts>); single-threaded: no lock. */
typedef struct groove_ExecutionLayoutCache {
    jz_deque layouts; /* VecDeque<Arc<ExecutionLayout>> */
    u32 weight;
    u64 builds, hits;
} groove_ExecutionLayoutCache;

void groove__ivm__execution_layout__ExecutionLayoutCache__Default__default(groove_ExecutionLayoutCache *self);
void groove__ivm__execution_layout__ExecutionLayoutCache__drop(groove_ExecutionLayoutCache *self);
bool groove__ivm__execution_layout__ExecutionLayoutCache__get(groove_ExecutionLayoutCache *self, const groove_IvmGraph *graph,
                                                              const groove_NodeId *roots, u32 n_roots,
                                                              groove_ExecutionLayout **out, groove_NodeId *err);
void groove__ivm__execution_layout__ExecutionLayoutCache__invalidate(groove_ExecutionLayoutCache *self, bool has_node, groove_NodeId node);
void groove__ivm__execution_layout__ExecutionLayoutCache__counters(const groove_ExecutionLayoutCache *self, u64 *builds, u64 *hits);
const u32 *groove__ivm__execution_layout__ExecutionLayout__dependents(const groove_ExecutionLayout *self, u32 slot, u32 *len);
const u32 *groove__ivm__execution_layout__ExecutionLayout__inputs(const groove_ExecutionLayout *self, u32 slot, u32 *len);
u32 groove__ivm__execution_layout__ExecutionLayout__weight(const groove_ExecutionLayout *self);

/* other unit (ivm/runtime/pipeline.rs) */
bool groove__ivm__runtime__pipeline__supports_node(const groove_IvmGraph *graph, groove_NodeId id);

#endif
