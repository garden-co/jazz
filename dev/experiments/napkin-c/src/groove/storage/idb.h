/* groove::storage::idb (crates/groove/src/storage/idb.rs) */
#ifndef GROOVE_STORAGE_IDB_H
#define GROOVE_STORAGE_IDB_H
#include "jz_rt.h"
#include "mod.h"
#include "../_ext_groove-storage.h"

#define GROOVE_IDB_MAX_GENERATION_CONFLICT_RETRIES 8u
#define GROOVE_IDB_MAX_CONFLICT_BACKOFF_YIELDS 16u

/* The Rc-shared fields of IdbStorage<S>: tree, column_families,
 * mutation_gate, needs_reset, tree_epoch (one jz_rc payload). */
typedef struct groove_IdbShared {
    idb_tree_IdbTree tree;       /* Rc<RefCell<IdbTree<S>>> */
    jz_btree column_families;    /* Rc<RefCell<BTreeSet<String>>> */
    bool mutation_gate_locked;   /* Rc<futures::lock::Mutex<()>> */
    bool needs_reset;            /* Rc<Cell<bool>> */
    u64 tree_epoch;              /* Rc<Cell<u64>> */
} groove_IdbShared;
typedef struct groove_IdbStorage { groove_IdbShared *shared; } groove_IdbStorage;

jz_err groove__storage__idb__IdbStorage__open(idb_tree_PageStore store, const char *const *column_families, u32 n, groove_IdbStorage *out);
groove_IdbStorage groove__storage__idb__IdbStorage__Clone__clone(const groove_IdbStorage *s);
void groove__storage__idb__IdbStorage__drop(groove_IdbStorage *s);
jz_err groove__storage__idb__IdbStorage__ReopenableStorage__reopen(groove_IdbStorage self, groove_StrVec column_families, groove_IdbStorage *out);
extern const groove_OrderedKvStorageVtbl groove__storage__idb__IdbStorage__OrderedKvStorage__VTBL;
groove_DynStorage groove__storage__idb__IdbStorage__into_dyn(groove_IdbStorage s);
#endif
