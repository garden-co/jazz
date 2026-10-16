/* groove::ivm::runtime::compilation_cache: reuse compilation of the same
 * immutable fragment, never its evaluation. */
#include "mod.h"

#define SLOTS 4096u

static u64 usize_hash(const void *k) { return jz_hash_u64(*(const usize *)k); }
static bool usize_eq(const void *a, const void *b) { return *(const usize *)a == *(const usize *)b; }
static void entry_drop(void *e) {
    groove_CompilationCacheEntry *entry = e;
    jz_vec_drop(&entry->inputs, sizeof(groove_CompiledInput), NULL);
}
static const jz_hmap_desc ENTRY_MAP = { sizeof(usize), sizeof(groove_CompilationCacheEntry), usize_hash, usize_eq, NULL, entry_drop };
const jz_hmap_desc groove_ivm_rt_USIZE_COMPILED_MAP = { sizeof(usize), sizeof(groove_CompiledNode), usize_hash, usize_eq, NULL, NULL };
const jz_hmap_desc groove_ivm_rt_USIZE_DESC_MAP = { sizeof(usize), sizeof(groove_RecordDescriptor), usize_hash, usize_eq, NULL, NULL };

void groove__ivm__runtime__compilation_cache__CompilationCache__default(groove_CompilationCache *c) { jz_hmap_init(&c->slots, &ENTRY_MAP); }
void groove_CompilationCache_drop(groove_CompilationCache *c) { jz_hmap_drop(&c->slots); }

static usize slot(const groove_GraphBuilder *owner) {
    /* Allocations are aligned. Mix the remaining address bits; this is only a
     * bounded lookup hint, with exact ownership checked below. */
    return (usize)(((uintptr_t)owner >> 4) * 0x9e3779b1u) % SLOTS;
}

static bool eligible(const groove_GraphBuilder *graph) {
    switch (graph->tag) {
    case groove_GB_Table: case groove_GB_TemplateInput: case groove_GB_Index: case groove_GB_Lookup:
    case groove_GB_InlineRecords: case groove_GB_InputSource: case groove_GB_BindingSource:
    case groove_GB_FrontierSource: case groove_GB_Recursive: case groove_GB_RecursiveStepWitness:
    case groove_GB_CollectBy:
        return false;
    case groove_GB_ArgMaxBy: case groove_GB_ArgMinBy:
        if (graph->input->tag == groove_GB_Table) return false;
        return true;
    default:
        return true;
    }
}

typedef struct MatchCtx {
    const groove_CompilationCacheEntry *entry; const jz_hmap *compiled; const jz_hmap *outputs;
    u32 index; bool matches;
} MatchCtx;
static void match_input(groove_GraphBuilder *const *input, void *c) {
    MatchCtx *m = c;
    usize key = (usize)(uintptr_t)*input;
    if (m->index < m->entry->inputs.len) {
        const groove_CompiledInput *exp = &m->entry->inputs.ptr[m->index];
        const groove_CompiledNode *got = jz_hmap_get(m->compiled, &key);
        const groove_RecordDescriptor *out = jz_hmap_get(m->outputs, &key);
        m->matches &= got && groove_CompiledNode_eq(got, &exp->compiled) && out && *out == exp->output;
    } else {
        m->matches = false;
    }
    m->index++;
}

bool groove__ivm__runtime__compilation_cache__CompilationCache__get(groove_CompilationCache *c, groove_GraphBuilder *owner,
    const jz_hmap *compiled, const jz_hmap *outputs, const groove_IvmGraph *graph,
    groove_CompiledNode *out_compiled, groove_RecordDescriptor *out_inferred, u64 *out_logical) {
    usize s = slot(owner);
    const groove_CompilationCacheEntry *entry = jz_hmap_get(&c->slots, &s);
    if (!entry) return false;
    /* Keeping the Weak alive prevents allocation-address reuse; upgrading
     * additionally makes the ownership proof explicit. */
    if (jz_rc_strong(entry->owner) == 0) return false;
    if (entry->owner != owner || !groove__ivm__graph__IvmGraph__node(graph, entry->compiled.node) ||
        (entry->compiled.has_root_ordering_node && !groove__ivm__graph__IvmGraph__node(graph, entry->compiled.root_ordering_node)))
        return false;
    MatchCtx m = { entry, compiled, outputs, 0, true };
    groove__ivm__graph__GraphBuilder__visit_inputs(owner, match_input, &m);
    if (!m.matches || m.index != entry->inputs.len) return false;
    *out_compiled = entry->compiled;
    *out_inferred = entry->inferred;
    *out_logical = entry->logical_nodes;
    return true;
}

typedef struct CollectCtx { const jz_hmap *compiled; const jz_hmap *outputs; groove_CompilationCacheEntry *entry; } CollectCtx;
static void collect_input(groove_GraphBuilder *const *input, void *c) {
    CollectCtx *cc = c;
    usize key = (usize)(uintptr_t)*input;
    const groove_CompiledNode *got = jz_hmap_get(cc->compiled, &key);
    const groove_RecordDescriptor *out = jz_hmap_get(cc->outputs, &key);
    if (!got || !out) jz_panic("index out of bounds: compiled input missing");
    groove_CompiledInput ci = { *got, *out };
    JZ_VEC_PUSH(cc->entry->inputs, ci);
}

void groove__ivm__runtime__compilation_cache__CompilationCache__insert(groove_CompilationCache *c, groove_GraphBuilder *owner,
    const jz_hmap *compiled, const jz_hmap *outputs, groove_CompiledNode result, groove_RecordDescriptor inferred, u64 logical_nodes) {
    if (!eligible(owner)) return;
    groove_CompilationCacheEntry entry = { owner, {0}, inferred, result, logical_nodes };
    CollectCtx cc = { compiled, outputs, &entry };
    groove__ivm__graph__GraphBuilder__visit_inputs(owner, collect_input, &cc);
    /* Bound retained metadata as well as entry count (notably wide Union). */
    if (entry.inputs.len > 64) { entry_drop(&entry); return; }
    usize s = slot(owner);
    jz_hmap_insert(&c->slots, &s, &entry, NULL);
}

/* One visit per immutable DAG fragment. Owners are borrowed for this install;
 * only Weak references may escape it. Deep valid graphs remain iterative. */
typedef struct PendingItem { const groove_GraphBuilder *graph; groove_GraphBuilder *owner; bool visited; } PendingItem;
typedef JZ_VEC(PendingItem) PendingVec;
static void push_input(groove_GraphBuilder *const *input, void *c) {
    PendingItem item = { *input, *input, false };
    JZ_VEC_PUSH(*(PendingVec *)c, item);
}

void groove__ivm__runtime__compilation_cache__compilation_order(const groove_GraphBuilder *graph, groove_CompilationStepVec *out) {
    static const jz_hmap_desc SEEN = { sizeof(usize), 0, usize_hash, usize_eq, NULL, NULL };
    PendingVec pending = {0};
    PendingItem root = { graph, NULL, false };
    JZ_VEC_PUSH(pending, root);
    jz_hmap seen; jz_hmap_init(&seen, &SEEN);
    memset(out, 0, sizeof(*out));
    while (pending.len) {
        PendingItem item = pending.ptr[--pending.len];
        if (item.visited) {
            groove_CompilationStep step = { item.graph, item.owner };
            JZ_VEC_PUSH(*out, step);
        } else {
            usize key = (usize)(uintptr_t)item.graph;
            bool inserted;
            jz_hmap_entry(&seen, &key, &inserted);
            if (inserted) {
                item.visited = true;
                JZ_VEC_PUSH(pending, item);
                groove__ivm__graph__GraphBuilder__visit_inputs(item.graph, push_input, &pending);
            }
        }
    }
    jz_hmap_drop(&seen);
    jz_vec_drop(&pending, sizeof(PendingItem), NULL);
}
