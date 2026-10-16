/* activation.h: port of crates/groove/src/ivm/activation.rs. */
#ifndef GROOVE_IVM_ACTIVATION_H
#define GROOVE_IVM_ACTIVATION_H
#include "execution_layout.h"

typedef JZ_VEC(groove_BindingSourceKey) groove_BindingSourceKeyVec;

/* Arc<ActivationPlan>: a jz_rc payload. affected/relevant are Arc<HashSet<NodeId>>
 * (jz_rc payloads holding a jz_hmap). */
typedef struct groove_ActivationPlan {
    groove_NodeIdVec sources;
    jz_hmap *affected;
    jz_hmap *relevant;
    groove_ExecutionLayout *layout;
    groove_NodeIdVec durable;
    groove_NodeIdVec ephemeral;
    groove_StrVec tables;
    groove_BindingSourceKeyVec bindings;
    groove_NodeIdVec routed;
} groove_ActivationPlan;

typedef struct groove_ActivationCache {
    jz_deque plans; /* VecDeque<Arc<ActivationPlan>> */
    u32 weight;
    u64 builds, hits;
} groove_ActivationCache;

void groove__ivm__activation__ActivationCache__Default__default(groove_ActivationCache *self);
void groove__ivm__activation__ActivationCache__drop(groove_ActivationCache *self);
bool groove__ivm__activation__ActivationCache__get(groove_ActivationCache *self, const groove_IvmGraph *graph,
                                                   groove_NodeIdVec sources, groove_ActivationPlan **out, groove_NodeId *err);
void groove__ivm__activation__ActivationCache__added(groove_ActivationCache *self, const groove_NodeId *inputs, u32 n);
void groove__ivm__activation__ActivationCache__removed(groove_ActivationCache *self, groove_NodeId node);
void groove__ivm__activation__ActivationCache__clear(groove_ActivationCache *self);
jz_hmap *groove__ivm__nodeset_arc_new(jz_hmap set);

#endif
