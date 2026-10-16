/* foreground_node_lease.c: port of crates/jazz/layers/db/src/foreground_node_lease.rs */
#include "_ext_db-b.h"

typedef struct jazz_db_ForegroundNodeLease {
    jazz_types_NodeUuid node;
    jazz_types_TxTime confirmed_tx_time;
} jazz_db_ForegroundNodeLease;

enum {
    JAZZ_DB_FNL_DUPLICATE_NODE = 1,
    JAZZ_DB_FNL_INACTIVE_LEASE = 2,
};

typedef struct jazz_db_ForegroundNodeLeasePool {
    jz_btree reusable; /* NodeUuid -> TxTime */
    jz_btree active;   /* set NodeUuid */
    jz_btree retired;  /* set NodeUuid */
} jazz_db_ForegroundNodeLeasePool;

static const jz_btree_desc node_time_desc = { sizeof(jz_uuid), sizeof(jazz_types_TxTime), jz_cmp_uuid, NULL, NULL };
static const jz_btree_desc node_set_desc = { sizeof(jz_uuid), 0, jz_cmp_uuid, NULL, NULL };

static jz_err lease_error(u32 code) {
    switch (code) {
    case JAZZ_DB_FNL_DUPLICATE_NODE:
        return jz_err_new(code, "foreground node identity is already known to this lease pool");
    case JAZZ_DB_FNL_INACTIVE_LEASE:
    default:
        return jz_err_new(code, "foreground node lease is not active");
    }
}

void jazz_db__foreground_node_lease__ForegroundNodeLeasePool__default(jazz_db_ForegroundNodeLeasePool *out) {
    jz_btree_init(&out->reusable, &node_time_desc);
    jz_btree_init(&out->active, &node_set_desc);
    jz_btree_init(&out->retired, &node_set_desc);
}

void jazz_db__foreground_node_lease__ForegroundNodeLeasePool__drop(jazz_db_ForegroundNodeLeasePool *p) {
    jz_btree_drop(&p->reusable);
    jz_btree_drop(&p->active);
    jz_btree_drop(&p->retired);
}

bool jazz_db__foreground_node_lease__ForegroundNodeLeasePool__acquire_reusable(jazz_db_ForegroundNodeLeasePool *self, jazz_db_ForegroundNodeLease *out) {
    void *k, *v;
    if (!jz_btree_first(&self->reusable, &k, &v)) return false;
    jazz_types_NodeUuid node = *(jz_uuid *)k;
    jazz_types_TxTime confirmed = *(jazz_types_TxTime *)v;
    jz_btree_remove(&self->reusable, &node, NULL, NULL);
    jz_btree_insert(&self->active, &node, NULL, NULL);
    out->node = node;
    out->confirmed_tx_time = confirmed;
    return true;
}

jz_err jazz_db__foreground_node_lease__ForegroundNodeLeasePool__acquire_fresh(jazz_db_ForegroundNodeLeasePool *self, jazz_types_NodeUuid node, jazz_db_ForegroundNodeLease *out) {
    if (jz_btree_contains(&self->active, &node) || jz_btree_contains(&self->reusable, &node) ||
        jz_btree_contains(&self->retired, &node))
        return lease_error(JAZZ_DB_FNL_DUPLICATE_NODE);
    jz_btree_insert(&self->active, &node, NULL, NULL);
    out->node = node;
    out->confirmed_tx_time = 0;
    return JZ_OK;
}

jz_err jazz_db__foreground_node_lease__ForegroundNodeLeasePool__clean_handoff(jazz_db_ForegroundNodeLeasePool *self, jazz_db_ForegroundNodeLease lease) {
    if (!jz_btree_remove(&self->active, &lease.node, NULL, NULL)) return lease_error(JAZZ_DB_FNL_INACTIVE_LEASE);
    jz_btree_insert(&self->reusable, &lease.node, &lease.confirmed_tx_time, NULL);
    return JZ_OK;
}

jz_err jazz_db__foreground_node_lease__ForegroundNodeLeasePool__retire(jazz_db_ForegroundNodeLeasePool *self, jazz_db_ForegroundNodeLease lease) {
    if (!jz_btree_remove(&self->active, &lease.node, NULL, NULL)) return lease_error(JAZZ_DB_FNL_INACTIVE_LEASE);
    jz_btree_insert(&self->retired, &lease.node, NULL, NULL);
    return JZ_OK;
}
