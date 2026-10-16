/* positional_order.h: crates/jazz/layers/db/src/positional_order.rs (type-erased K) */
#ifndef JAZZ_DB_POSITIONAL_ORDER_H
#define JAZZ_DB_POSITIONAL_ORDER_H
#include "_ext_db-b.h"

#define JAZZ_DB_PO_NIL UINT32_MAX

/* Per-K operations: one C copy of the generic treap for every key type. */
typedef struct jazz_db_PositionalOrderKey {
    u16 key_size;
    jz_cmp_fn cmp;
    void (*clone)(void *dst, const void *src);
    void (*drop)(void *);
    const jz_btree_desc *slots_desc; /* K -> u32 */
} jazz_db_PositionalOrderKey;

/* Node<K> header; the key bytes follow it in the arena (stride = header + key_size). */
typedef struct jazz_db_PoNode {
    bool has_key;
    u32 left, right, parent, size;
    u64 priority;
} jazz_db_PoNode;

typedef struct jazz_db_PositionalOrder {
    const jazz_db_PositionalOrderKey *k;
    jz_btree slots;          /* K -> u32 */
    u8 *nodes; u32 nodes_len; u32 nodes_cap; /* arena of Node<K> */
    u32 root;
    u64 seed;
    u32 anonymous;
} jazz_db_PositionalOrder;

typedef struct jazz_db_PositionalOrderIter {
    const jazz_db_PositionalOrder *order;
    JZ_VEC(u32) stack;
} jazz_db_PositionalOrderIter;

extern const jazz_db_PositionalOrderKey jazz_db__positional_order__occurrence_key;

void jazz_db__positional_order__PositionalOrder__default(jazz_db_PositionalOrder *o, const jazz_db_PositionalOrderKey *k);
void jazz_db__positional_order__PositionalOrder__drop(jazz_db_PositionalOrder *o);
void jazz_db__positional_order__PositionalOrder__clone(jazz_db_PositionalOrder *dst, const jazz_db_PositionalOrder *src);
/* from_ordered: keys moved in. On duplicate returns false and moves the duplicate into *dup. */
bool jazz_db__positional_order__PositionalOrder__from_ordered(jazz_db_PositionalOrder *o, const jazz_db_PositionalOrderKey *k, void *keys, u32 n, void *dup);
u32 jazz_db__positional_order__PositionalOrder__len(const jazz_db_PositionalOrder *o);
bool jazz_db__positional_order__PositionalOrder__position(const jazz_db_PositionalOrder *o, const void *key, u32 *out);
u32 jazz_db__positional_order__PositionalOrder__anonymous_len(const jazz_db_PositionalOrder *o);
bool jazz_db__positional_order__PositionalOrder__anonymize(jazz_db_PositionalOrder *o, const void *key, u32 *out);
const void *jazz_db__positional_order__PositionalOrder__get(const jazz_db_PositionalOrder *o, u32 position);
const void *jazz_db__positional_order__PositionalOrder__index(const jazz_db_PositionalOrder *o, u32 position);
bool jazz_db__positional_order__PositionalOrder__insert(jazz_db_PositionalOrder *o, u32 position, void *key);
bool jazz_db__positional_order__PositionalOrder__remove(jazz_db_PositionalOrder *o, const void *key, u32 *out);
void jazz_db__positional_order__PositionalOrder__iter(const jazz_db_PositionalOrder *o, jazz_db_PositionalOrderIter *it);
/* next: returns false at end; *key = NULL for an anonymous position */
bool jazz_db__positional_order__Iter__next(jazz_db_PositionalOrderIter *it, const void **key);
void jazz_db__positional_order__Iter__drop(jazz_db_PositionalOrderIter *it);
bool jazz_db__positional_order__PositionalOrder__eq_vec(const jazz_db_PositionalOrder *o, const void *keys, u32 n);
#endif
