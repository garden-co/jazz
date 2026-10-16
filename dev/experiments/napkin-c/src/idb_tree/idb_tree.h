/* idb_tree.h: crate idb_tree (crates/idb-tree), the IndexedDB page-store B-tree. */
#ifndef IDB_TREE_H
#define IDB_TREE_H
#include "jz_rt.h"

typedef u64 idb_tree_PageId;

u64 xxhash_rust__xxh3__xxh3_64(const u8 *in, u32 len);

/* ---------------- page.rs ---------------- */
enum { IDB_VALUE_INLINE = 0, IDB_VALUE_OVERFLOW = 1 };
typedef struct idb_tree_ValueCell {
    u32 tag;
    jz_bytes inline_bytes;   /* Inline */
    idb_tree_PageId head;    /* Overflow */
    u64 len;                 /* Overflow */
} idb_tree_ValueCell;
typedef struct idb_tree_LeafEntry { jz_bytes key; idb_tree_ValueCell value; } idb_tree_LeafEntry;
typedef JZ_VEC(idb_tree_LeafEntry) idb_tree_LeafEntryVec;
typedef JZ_VEC(jz_bytes) idb_tree_KeyVec;
typedef JZ_VEC(idb_tree_PageId) idb_tree_PageIdVec;

enum { IDB_PAGE_LEAF = 0, IDB_PAGE_INTERNAL = 1, IDB_PAGE_OVERFLOW = 2 };
typedef struct idb_tree_Page {
    u32 tag;
    idb_tree_LeafEntryVec entries;   /* Leaf */
    idb_tree_KeyVec keys;            /* Internal */
    idb_tree_PageIdVec children;     /* Internal */
    bool has_next;                   /* Overflow */
    idb_tree_PageId next;            /* Overflow */
    jz_bytes bytes;                  /* Overflow */
} idb_tree_Page;

#define IDB_PAGE_HEADER_LEN (8u + 1u + 8u)
#define IDB_LEAF_BASE_LEN (IDB_PAGE_HEADER_LEN + 1u + 4u)
#define IDB_INTERNAL_BASE_LEN (IDB_PAGE_HEADER_LEN + 1u + 4u + 4u + 8u)

void idb_tree__page__Page__leaf(idb_tree_Page *out);
void idb_tree__page__Page__drop(void *p);
void idb_tree__page__Page__clone(void *dst, const void *src);
void idb_tree__page__ValueCell__drop(void *p);
void idb_tree__page__ValueCell__clone(void *dst, const void *src);
void idb_tree__page__LeafEntry__drop(void *p);
void idb_tree__page__LeafEntry__clone(void *dst, const void *src);
usize idb_tree__page__leaf_entry_len(const jz_bytes *key, const idb_tree_ValueCell *value);
usize idb_tree__page__internal_key_len(const jz_bytes *key);
usize idb_tree__page__encoded_len(const idb_tree_Page *page);
/* Result<_, String>: on error, code IDB_STRING_ERR, msg = the String */
jz_err idb_tree__page__encode_page(const idb_tree_Page *page, jz_bytes *out);
jz_err idb_tree__page__decode_page(const u8 *bytes, u32 len, idb_tree_Page *out);

/* ---------------- store.rs ---------------- */
typedef struct idb_tree_Metadata {
    usize page_size;
    u64 generation;
    bool has_root_page_id;
    idb_tree_PageId root_page_id;
    idb_tree_PageId next_page_id;
} idb_tree_Metadata;
void idb_tree__store__Metadata__empty(idb_tree_Metadata *out, usize page_size);

typedef struct idb_tree_CommitPage { idb_tree_PageId id; jz_bytes bytes; } idb_tree_CommitPage;
typedef struct idb_tree_Commit {
    u64 expected_generation;
    idb_tree_Metadata metadata;
    JZ_VEC(idb_tree_CommitPage) pages;
    idb_tree_PageIdVec deleted_page_ids;
} idb_tree_Commit;
void idb_tree__store__Commit__drop(idb_tree_Commit *c);

/* Box<dyn Fn() -> bool> / Box<dyn FnOnce()> */
typedef struct idb_tree_TreeOwnership {
    bool (*is_live)(void *ctx); /* None when NULL */
    void (*release)(void *ctx); /* None when NULL */
    void *ctx;
    void (*ctx_drop)(void *ctx);
} idb_tree_TreeOwnership;
void idb_tree__store__TreeOwnership__Default__default(idb_tree_TreeOwnership *out);
void idb_tree__store__TreeOwnership__new(idb_tree_TreeOwnership *out, void (*release)(void *), void *ctx, void (*ctx_drop)(void *));
void idb_tree__store__TreeOwnership__revocable(idb_tree_TreeOwnership *out, bool (*is_live)(void *), void (*release)(void *), void *ctx,
                                               void (*ctx_drop)(void *));
bool idb_tree__store__TreeOwnership__is_live(const idb_tree_TreeOwnership *o);
void idb_tree__store__TreeOwnership__Drop__drop(idb_tree_TreeOwnership *o);

/* Option<Vec<u8>> slot of read_pages */
typedef struct idb_tree_PageSlot { bool some; jz_bytes bytes; } idb_tree_PageSlot;
typedef JZ_VEC(idb_tree_PageSlot) idb_tree_PageSlotVec;
void idb_tree__store__PageSlot__drop(void *p);

/* trait PageStore (+ Clone): String errors use code IDB_STRING_ERR */
typedef struct idb_tree_PageStoreVtbl {
    jz_err (*claim_tree_ownership)(void *self, idb_tree_TreeOwnership *out); /* NULL -> default */
    bool (*can_reclaim_obsolete_pages)(void *self);                         /* NULL -> false */
    jz_err (*load_metadata)(void *self, bool *some, idb_tree_Metadata *out);
    jz_err (*read_page)(void *self, idb_tree_PageId id, idb_tree_PageSlot *out);
    jz_err (*read_pages)(void *self, const idb_tree_PageId *ids, u32 n, idb_tree_PageSlotVec *out); /* NULL -> default */
    jz_err (*commit)(void *self, const idb_tree_Commit *commit, idb_tree_Metadata *out);
    void *(*clone)(void *self);
    void (*drop)(void *self);
} idb_tree_PageStoreVtbl;
typedef struct idb_tree_PageStore { const idb_tree_PageStoreVtbl *vt; void *self; } idb_tree_PageStore;
jz_err idb_tree__store__PageStore__claim_tree_ownership(const idb_tree_PageStore *s, idb_tree_TreeOwnership *out);
bool idb_tree__store__PageStore__can_reclaim_obsolete_pages(const idb_tree_PageStore *s);
jz_err idb_tree__store__PageStore__read_pages(const idb_tree_PageStore *s, const idb_tree_PageId *ids, u32 n, idb_tree_PageSlotVec *out);
idb_tree_PageStore idb_tree__store__PageStore__clone(const idb_tree_PageStore *s);
void idb_tree__store__PageStore__drop(idb_tree_PageStore *s);

/* ---------------- lib.rs ---------------- */
enum {
    IDB_ERR_INVALID_OPTIONS = 0x1d00,
    IDB_ERR_MISSING_PAGE,
    IDB_ERR_INVALID_PAGE,
    IDB_ERR_PAGE_TOO_LARGE,
    IDB_ERR_STORE,
    IDB_ERR_GENERATION_CONFLICT,
    IDB_ERR_OWNERSHIP_EXPIRED,
    IDB_ERR_COMMIT_IN_FLIGHT,
    IDB_ERR_COMMIT_ABANDONED,
    IDB_ERR_NOT_RESIDENT,
    IDB_ERR_READ_ONLY_VIEW,
    IDB_STRING_ERR, /* a bare Result<_, String> from page/store code */
};

typedef struct idb_tree_Options { usize page_size; } idb_tree_Options;
void idb_tree__Options__Default__default(idb_tree_Options *out);

typedef struct idb_tree_KeyValue { jz_bytes key; jz_bytes value; } idb_tree_KeyValue;
typedef JZ_VEC(idb_tree_KeyValue) idb_tree_KeyValueVec;
void idb_tree__KeyValue__drop(void *p);

enum { IDB_WRITE_SET = 0, IDB_WRITE_DELETE = 1 };
typedef struct idb_tree_WriteOperation { u32 tag; jz_bytes key; jz_bytes value; } idb_tree_WriteOperation;
typedef JZ_VEC(idb_tree_WriteOperation) idb_tree_WriteOperationVec;
void idb_tree__WriteOperation__drop(void *p);

typedef struct idb_tree_TreeCore idb_tree_TreeCore;
typedef struct idb_tree_IdbTree {
    idb_tree_TreeCore *inner;            /* Rc<RefCell<TreeCore<S>>> */
    idb_tree_TreeOwnership *ownership;   /* Rc<TreeOwnership> */
    u64 *reload_epoch;                   /* Rc<Cell<u64>> */
    bool read_committed;
} idb_tree_IdbTree;

jz_err idb_tree__IdbTree__open(idb_tree_PageStore store, idb_tree_Options options, idb_tree_IdbTree *out);
void idb_tree__IdbTree__read_committed(const idb_tree_IdbTree *t, idb_tree_IdbTree *out);
void idb_tree__IdbTree__clone(idb_tree_IdbTree *dst, const idb_tree_IdbTree *src);
void idb_tree__IdbTree__drop(idb_tree_IdbTree *t);
jz_err idb_tree__IdbTree__reload(const idb_tree_IdbTree *t);
void idb_tree__IdbTree__metadata(const idb_tree_IdbTree *t, idb_tree_Metadata *out);
usize idb_tree__IdbTree__dirty_page_count(const idb_tree_IdbTree *t);
/* Option<bool>: 0 None, 1 Some(false), 2 Some(true) */
jz_err idb_tree__IdbTree__value_equals(const idb_tree_IdbTree *t, jz_slice key, jz_slice expected, u8 *out);
jz_err idb_tree__IdbTree__get(const idb_tree_IdbTree *t, jz_slice key, bool *some, jz_bytes *out);
jz_err idb_tree__IdbTree__put(const idb_tree_IdbTree *t, jz_bytes key, jz_bytes value);
jz_err idb_tree__IdbTree__delete(const idb_tree_IdbTree *t, jz_slice key, bool *deleted);
jz_err idb_tree__IdbTree__write_many(const idb_tree_IdbTree *t, idb_tree_WriteOperationVec operations);
jz_err idb_tree__IdbTree__range(const idb_tree_IdbTree *t, jz_slice start, jz_slice end, idb_tree_KeyValueVec *out);
jz_err idb_tree__IdbTree__range_limit(const idb_tree_IdbTree *t, jz_slice start, jz_slice end, usize limit, idb_tree_KeyValueVec *out);
jz_err idb_tree__IdbTree__range_reverse(const idb_tree_IdbTree *t, jz_slice start, jz_slice end, usize limit, idb_tree_KeyValueVec *out);
jz_err idb_tree__IdbTree__flush(const idb_tree_IdbTree *t);

/* ---------------- web.rs ---------------- */
typedef struct idb_tree_IndexedDbPageStore idb_tree_IndexedDbPageStore;
idb_tree_PageStore idb_tree__web__IndexedDbPageStore__from_js(jz_js handle);
#endif
