/* routes.h: port of crates/groove/src/ivm/routes.rs (route index for
 * prepared-shape bindings, #3288). */
#ifndef GROOVE_IVM_ROUTES_H
#define GROOVE_IVM_ROUTES_H
#include "jz_rt.h"
#include "../_ext_groove-ivm-db.h"
#include "op_types.h"

typedef u64 groove_NodeId; /* NodeId(pub u64) */
typedef JZ_VEC(groove_NodeId) groove_NodeIdVec;
typedef JZ_VEC(groove_ValueType) groove_ValueTypeVec;

/* HashSet<NodeId> descriptor shared by the whole unit (graph.c). */
extern const jz_hmap_desc groove__ivm__NODESET_DESC;

typedef struct groove_RouteTable {
    groove_UsizeVec field_indices;
    groove_ValueTypeVec field_types;
    jz_hmap by_key;              /* HashMap<Vec<u8>, HashSet<NodeId>> */
    jz_hmap root_ordering_nodes; /* HashSet<NodeId> */
} groove_RouteTable;

typedef struct groove_RouteBarrier { groove_NodeId terminal; jz_bytes key; u32 refs; } groove_RouteBarrier;

typedef struct groove_RouteIndex {
    jz_hmap tables;   /* HashMap<NodeId, RouteTable> */
    jz_hmap barriers; /* HashMap<NodeId, Barrier> */
} groove_RouteIndex;

void groove__ivm__routes__RouteIndex__Default__default(groove_RouteIndex *self);
void groove__ivm__routes__RouteIndex__drop(groove_RouteIndex *self);
void groove__ivm__routes__RouteTable__drop(groove_RouteTable *self);
bool groove__ivm__routes__RouteTable__key_of_record(const groove_RouteTable *self, const groove_BorrowedRecord *record, jz_bytes *out);
void groove__ivm__routes__RouteTable__barriers(const groove_RouteTable *self, groove_NodeIdVec *out);
bool groove__ivm__routes__RouteIndex__is_barrier(const groove_RouteIndex *self, groove_NodeId node);
const groove_RouteTable *groove__ivm__routes__RouteIndex__table(const groove_RouteIndex *self, groove_NodeId terminal);
bool groove__ivm__routes__RouteIndex__has_table(const groove_RouteIndex *self, groove_NodeId terminal);
bool groove__ivm__routes__RouteIndex__add(groove_RouteIndex *self, groove_NodeId terminal, groove_NodeId barrier,
                                          groove_UsizeVec field_indices, groove_ValueTypeVec field_types, jz_bytes key,
                                          bool has_root_ordering_node, groove_NodeId root_ordering_node);
bool groove__ivm__routes__RouteIndex__release(groove_RouteIndex *self, groove_NodeId barrier);
bool groove__ivm__routes__RouteIndex__remove(groove_RouteIndex *self, groove_NodeId barrier);
bool groove__ivm__routes__is_routable_type(const groove_ValueType *value_type);
bool groove__ivm__routes__encode_route_key(const groove_Value *values, u32 n_values, const groove_ValueType *types, u32 n_types, jz_bytes *out);

#endif
