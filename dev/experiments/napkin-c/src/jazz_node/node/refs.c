/* Port of crates/jazz/layers/node/src/node/refs.rs */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

/* Ref: Local | Remote(Origin) */
typedef enum { JZN_REF_LOCAL, JZN_REF_REMOTE_ORIGIN } jazz_node_Ref;
static const jazz_node_Ref STACK_GLOBAL[1] = { JZN_REF_REMOTE_ORIGIN };
static const jazz_node_Ref STACK_LOCAL[2] = { JZN_REF_LOCAL, JZN_REF_REMOTE_ORIGIN };

const jazz_node_Ref *jazz_node__node__refs__Ref__stack(jazz_model_DurabilityTier tier, u32 *n) {
    switch (tier) {
    case JAZZ_DUR_GLOBAL: *n = 1; return STACK_GLOBAL;
    case JAZZ_DUR_LOCAL:
    case JAZZ_DUR_NONE:
    default: *n = 2; return STACK_LOCAL;
    }
}

jazz_node_PhysicalCurrentClass jazz_node__node__refs__Ref__physical_class(jazz_node_Ref r) {
    switch (r) {
    case JZN_REF_LOCAL: return JZN_PHYS_CLASS_AHEAD;
    case JZN_REF_REMOTE_ORIGIN:
    default: return JZN_PHYS_CLASS_GLOBAL;
    }
}
