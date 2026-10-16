/* crates/idb-tree/src/lib.rs: the copy-on-write page B-tree.
 *
 * TreeCore<S>/IdbTree<S> are generic over S: PageStore + Clone in Rust (and
 * instantiated once, for IndexedDbPageStore, in the browser build). Here S is
 * the idb_tree_PageStore vtable. Every async fn is a synchronous function;
 * page-store I/O completes in the host call (see PORTING.md). */
#include "idb_tree/idb_tree.h"

#define DEFAULT_PAGE_SIZE (16u * 1024u)
#define MIN_PAGE_SIZE 1024u
#define MAX_JS_SAFE_INTEGER 9007199254740991ull
#define PAGE_ID_MAX UINT64_MAX
#define USIZE_MAX ((usize)-1)

/* ---------------- containers ---------------- */
static u64 pid_hash(const void *k) { return jz_hash_u64(*(const u64 *)k); }
static bool pid_eq(const void *a, const void *b) { return *(const u64 *)a == *(const u64 *)b; }
static const jz_hmap_desc PAGES_MAP = {sizeof(u64), sizeof(idb_tree_Page), pid_hash, pid_eq, NULL, idb_tree__page__Page__drop};
static const jz_hmap_desc PID_SET = {sizeof(u64), 0, pid_hash, pid_eq, NULL, NULL};
static const jz_btree_desc PID_BTREE_SET = {sizeof(u64), 0, jz_cmp_u64, NULL, NULL};

static bool set_insert(jz_hmap *set, u64 id) {
    bool inserted;
    jz_hmap_entry(set, &id, &inserted);
    return inserted;
}

static void bytes_drop(void *p) { jz_bytes_drop((jz_bytes *)p); }

/* Vec::split_off: move [at..] into *out */
static void vec_split_off(void *vec, u32 at, usize elem, void *out) {
    jz_vec *v = (jz_vec *)vec;
    jz_vec *o = (jz_vec *)out;
    memset(o, 0, sizeof *o);
    u32 n = v->len - at;
    jz_vec_reserve(o, n, elem);
    memcpy(o->ptr, (u8 *)v->ptr + (usize)at * elem, (usize)n * elem);
    o->len = n;
    v->len = at;
}

typedef struct idb_tree_PathStep { idb_tree_PageId page_id; usize child_index; } idb_tree_PathStep;
typedef JZ_VEC(idb_tree_PathStep) idb_tree_Path;

/* ---------------- Error ---------------- */
static jz_err err_with_msg(u32 code, jz_err source) {
    jz_slice m = source.msg ? jz_str_slice(source.msg) : JZ_LIT("");
    jz_err e;
    switch (code) {
    case IDB_ERR_INVALID_OPTIONS: e = jz_err_fmt(code, "invalid IDBTree options: %.*s", (int)m.len, m.ptr); break;
    case IDB_ERR_INVALID_PAGE: e = jz_err_fmt(code, "invalid IDBTree page: %.*s", (int)m.len, m.ptr); break;
    case IDB_ERR_STORE: e = jz_err_fmt(code, "IDBTree store error: %.*s", (int)m.len, m.ptr); break;
    case IDB_ERR_GENERATION_CONFLICT: e = jz_err_fmt(code, "IDBTree generation conflict: %.*s", (int)m.len, m.ptr); break;
    default: JZ_UNREACHABLE();
    }
    jz_err_drop(&source);
    return e;
}
static jz_err err_invalid_page(const char *msg) { return jz_err_fmt(IDB_ERR_INVALID_PAGE, "invalid IDBTree page: %s", msg); }
static jz_err err_missing_page(idb_tree_PageId id) {
    return jz_err_fmt(IDB_ERR_MISSING_PAGE, "IDBTree page %llu is missing", (unsigned long long)id);
}
static jz_err err_page_too_large(idb_tree_PageId id, usize page_size) {
    return jz_err_fmt(IDB_ERR_PAGE_TOO_LARGE, "IDBTree page %llu exceeds the configured %lu-byte page size", (unsigned long long)id,
                      (unsigned long)page_size);
}
static jz_err err_ownership_expired(void) { return jz_err_new(IDB_ERR_OWNERSHIP_EXPIRED, "IDBTree ownership has expired"); }
static jz_err err_commit_in_flight(void) { return jz_err_new(IDB_ERR_COMMIT_IN_FLIGHT, "an IDBTree commit is already in flight"); }
static jz_err err_commit_abandoned(void) {
    return jz_err_new(IDB_ERR_COMMIT_ABANDONED, "an IDBTree commit was abandoned; reload before using the tree");
}
static jz_err err_not_resident(idb_tree_PageId id) {
    return jz_err_fmt(IDB_ERR_NOT_RESIDENT, "IDBTree page %llu is not resident for a read-committed view", (unsigned long long)id);
}
static jz_err err_read_only_view(void) { return jz_err_new(IDB_ERR_READ_ONLY_VIEW, "a read-committed IDBTree view cannot write"); }

/* ---------------- small value types ---------------- */
void idb_tree__KeyValue__drop(void *p) {
    idb_tree_KeyValue *kv = (idb_tree_KeyValue *)p;
    jz_bytes_drop(&kv->key);
    jz_bytes_drop(&kv->value);
}

void idb_tree__WriteOperation__drop(void *p) {
    idb_tree_WriteOperation *op = (idb_tree_WriteOperation *)p;
    jz_bytes_drop(&op->key);
    if (op->tag == IDB_WRITE_SET) jz_bytes_drop(&op->value);
}

void idb_tree__Options__Default__default(idb_tree_Options *out) { out->page_size = DEFAULT_PAGE_SIZE; }

static jz_err idb_tree__Options__validate(idb_tree_Options self, idb_tree_Options *out) {
    usize ps = self.page_size;
    if (ps < MIN_PAGE_SIZE || (ps & (ps - 1)) != 0 || (u64)ps > 0xffffffffull)
        return jz_err_fmt(IDB_ERR_INVALID_OPTIONS, "invalid IDBTree options: page_size must be a power of two between %u and %u",
                          MIN_PAGE_SIZE, 0xffffffffu);
    *out = self;
    return JZ_OK;
}

enum { REPLACE_ONE = 0, REPLACE_SPLIT = 1 };
typedef struct idb_tree_PageReplacement {
    u32 tag;
    idb_tree_PageId one;
    idb_tree_PageId left;
    jz_bytes separator;
    idb_tree_PageId right;
} idb_tree_PageReplacement;

/* Choose the most byte-balanced boundary for which both encoded pages fit. */
static bool idb_tree__byte_balanced_split(const usize *sizes, u32 count, usize base_len, usize page_size, bool promote_separator,
                                          u32 *out) {
    usize left = base_len;
    usize right = base_len;
    for (u32 i = 0; i < count; i++) right += sizes[i];
    bool has_best = false;
    u32 best_index = 0;
    usize best_bytes = 0, best_count = 0;
    for (u32 index = 0; index < count; index++) {
        usize size = sizes[index];
        right -= size;
        if (!promote_separator) left += size;
        if ((promote_separator || index + 1 < count) && left <= page_size && right <= page_size) {
            u32 boundary = promote_separator ? index : index + 1;
            usize bytes_diff = left > right ? left - right : right - left;
            usize half = count / 2;
            usize count_diff = boundary > half ? boundary - half : half - boundary;
            /* lexicographic (bytes, count) < previous */
            if (!has_best || bytes_diff < best_bytes || (bytes_diff == best_bytes && count_diff < best_count)) {
                has_best = true;
                best_index = boundary;
                best_bytes = bytes_diff;
                best_count = count_diff;
            }
        }
        if (promote_separator) left += size;
    }
    *out = best_index;
    return has_best;
}

/* ---------------- TreeCore ---------------- */
struct idb_tree_TreeCore {
    idb_tree_PageStore store;
    idb_tree_Options options;
    idb_tree_Metadata metadata;
    bool has_durable_root;
    idb_tree_PageId durable_root;
    jz_hmap pages;      /* PageId -> Page */
    jz_btree dirty;     /* BTreeSet<PageId> */
    idb_tree_PageId write_floor;
    jz_btree deleted;   /* BTreeSet<PageId> */
    idb_tree_PageIdVec retirement_undo;
    bool commit_in_flight;
    bool commit_abandoned;
};

static void idb_tree__TreeCore__drop_fields(idb_tree_TreeCore *t) {
    idb_tree__store__PageStore__drop(&t->store);
    jz_hmap_drop(&t->pages);
    jz_btree_drop(&t->dirty);
    jz_btree_drop(&t->deleted);
    jz_vec_drop(&t->retirement_undo, sizeof(idb_tree_PageId), NULL);
}

static void idb_tree__TreeCore__drop(void *p) { idb_tree__TreeCore__drop_fields((idb_tree_TreeCore *)p); }

typedef struct idb_tree_WriteCheckpoint { idb_tree_Metadata metadata; } idb_tree_WriteCheckpoint;

typedef struct idb_tree_PreparedCommit {
    idb_tree_Commit commit;
    jz_btree retired;
} idb_tree_PreparedCommit;

static void idb_tree__PreparedCommit__drop(idb_tree_PreparedCommit *p) {
    idb_tree__store__Commit__drop(&p->commit);
    jz_btree_drop(&p->retired);
}

const idb_tree_Commit *idb_tree__PreparedCommit__commit(const idb_tree_PreparedCommit *p) { return &p->commit; }

/* Attempt<T>: Ready(T) (value via out-param) or Missing(PageId) */
typedef struct idb_tree_Attempt { bool missing; idb_tree_PageId page_id; } idb_tree_Attempt;
static const idb_tree_Attempt READY = {false, 0};
static idb_tree_Attempt missing(idb_tree_PageId id) { idb_tree_Attempt a = {true, id}; return a; }

/* AbandonOnDrop */
typedef struct idb_tree_AbandonOnDrop { idb_tree_TreeCore *inner; } idb_tree_AbandonOnDrop;
static void idb_tree__AbandonOnDrop__Drop__drop(idb_tree_AbandonOnDrop *g) {
    idb_tree_TreeCore *inner = g->inner;
    g->inner = NULL;
    if (inner) {
        inner->commit_in_flight = false;
        inner->commit_abandoned = true;
    }
}

static idb_tree_PageId idb_tree__TreeCore__root_page_id(const idb_tree_TreeCore *t) {
    if (!t->metadata.has_root_page_id) jz_panic("open always installs a root page");
    return t->metadata.root_page_id;
}

static bool idb_tree__TreeCore__page_fits(const idb_tree_TreeCore *t, const idb_tree_Page *page) {
    return idb_tree__page__encoded_len(page) <= t->options.page_size;
}

static bool idb_tree__TreeCore__is_fresh(const idb_tree_TreeCore *t, idb_tree_PageId id) { return id >= t->write_floor; }

static idb_tree_WriteCheckpoint idb_tree__TreeCore__write_checkpoint(idb_tree_TreeCore *t) {
    t->write_floor = t->metadata.next_page_id;
    idb_tree_WriteCheckpoint c = {t->metadata};
    return c;
}

static void idb_tree__TreeCore__rollback_write(idb_tree_TreeCore *t, idb_tree_WriteCheckpoint checkpoint) {
    idb_tree_PageId allocated_start = checkpoint.metadata.next_page_id;
    idb_tree_PageId allocated_end = t->metadata.next_page_id;
    for (idb_tree_PageId id = allocated_start; id < allocated_end; id++) {
        jz_hmap_remove(&t->pages, &id, NULL, NULL);
        jz_btree_remove(&t->dirty, &id, NULL, NULL);
    }
    for (u32 i = 0; i < t->retirement_undo.len; i++) jz_btree_remove(&t->deleted, &t->retirement_undo.ptr[i], NULL, NULL);
    t->retirement_undo.len = 0;
    t->metadata = checkpoint.metadata;
}

static void idb_tree__TreeCore__finish_write(idb_tree_TreeCore *t, idb_tree_WriteCheckpoint checkpoint, bool completed) {
    t->write_floor = PAGE_ID_MAX;
    if (completed) t->retirement_undo.len = 0;
    else idb_tree__TreeCore__rollback_write(t, checkpoint);
}

/* allocate_page takes ownership of `page` (dropped on error) */
static jz_err idb_tree__TreeCore__allocate_page(idb_tree_TreeCore *t, idb_tree_Page *page, idb_tree_PageId *out) {
    idb_tree_PageId page_id = t->metadata.next_page_id;
    jz_err e = JZ_OK;
    if (page_id >= MAX_JS_SAFE_INTEGER) {
        e = err_invalid_page("IDBTree page id space exceeds JavaScript's safe integer range");
    } else if (jz_hmap_get(&t->pages, &page_id) || jz_btree_contains(&t->dirty, &page_id)) {
        e = err_invalid_page("IDBTree next page id already exists");
    } else if (!idb_tree__TreeCore__page_fits(t, page)) {
        e = err_page_too_large(page_id, t->options.page_size);
    }
    if (e.code) {
        idb_tree__page__Page__drop(page);
        return e;
    }
    t->metadata.next_page_id = page_id + 1;
    jz_hmap_insert(&t->pages, &page_id, page, NULL);
    jz_btree_insert(&t->dirty, &page_id, NULL, NULL);
    *out = page_id;
    return JZ_OK;
}

static jz_err idb_tree__TreeCore__open(idb_tree_PageStore store, idb_tree_Options options, idb_tree_TreeCore *tree) {
    jz_err e = idb_tree__Options__validate(options, &options);
    if (e.code) {
        idb_tree__store__PageStore__drop(&store);
        return e;
    }
    bool some = false;
    idb_tree_Metadata metadata;
    e = store.vt->load_metadata(store.self, &some, &metadata);
    if (e.code) {
        idb_tree__store__PageStore__drop(&store);
        return err_with_msg(IDB_ERR_STORE, e);
    }
    if (!some) idb_tree__store__Metadata__empty(&metadata, options.page_size);
    memset(tree, 0, sizeof *tree);
    tree->store = store;
    tree->options = options;
    tree->has_durable_root = metadata.has_root_page_id;
    tree->durable_root = metadata.root_page_id;
    tree->metadata = metadata;
    jz_hmap_init(&tree->pages, &PAGES_MAP);
    jz_btree_init(&tree->dirty, &PID_BTREE_SET);
    tree->write_floor = PAGE_ID_MAX;
    jz_btree_init(&tree->deleted, &PID_BTREE_SET);
    tree->commit_in_flight = false;
    tree->commit_abandoned = false;
    if (tree->metadata.page_size != options.page_size) {
        e = jz_err_fmt(IDB_ERR_INVALID_OPTIONS, "invalid IDBTree options: store uses %lu-byte pages, requested %lu",
                       (unsigned long)tree->metadata.page_size, (unsigned long)options.page_size);
        idb_tree__TreeCore__drop_fields(tree);
        return e;
    }
    if (!tree->metadata.has_root_page_id) {
        idb_tree_Page leaf;
        idb_tree__page__Page__leaf(&leaf);
        idb_tree_PageId root;
        e = idb_tree__TreeCore__allocate_page(tree, &leaf, &root);
        if (e.code) {
            idb_tree__TreeCore__drop_fields(tree);
            return e;
        }
        tree->metadata.has_root_page_id = true;
        tree->metadata.root_page_id = root;
    }
    return JZ_OK;
}

/* ---- descent ---- */
typedef struct idb_tree_Descent {
    idb_tree_PageId page_id;
    const idb_tree_LeafEntryVec *entries;
    idb_tree_Path path;
    jz_hmap visited;
} idb_tree_Descent;

static void idb_tree__Descent__drop(idb_tree_Descent *d) {
    jz_vec_drop(&d->path, sizeof(idb_tree_PathStep), NULL);
    jz_hmap_drop(&d->visited);
}

/* keys.partition_point(|separator| separator <= key) */
static usize partition_point_le(const idb_tree_KeyVec *keys, jz_slice key) {
    u32 lo = 0, hi = keys->len;
    while (lo < hi) {
        u32 mid = lo + (hi - lo) / 2;
        if (jz_slice_cmp(jz_bytes_slice(&keys->ptr[mid]), key) <= 0) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

/* entries.binary_search_by(|(candidate, _)| candidate.cmp(key)) */
static bool entries_search(const idb_tree_LeafEntryVec *entries, jz_slice key, u32 *index) {
    u32 lo = 0, hi = entries->len;
    while (lo < hi) {
        u32 mid = lo + (hi - lo) / 2;
        int c = jz_slice_cmp(jz_bytes_slice(&entries->ptr[mid].key), key);
        if (c == 0) {
            *index = mid;
            return true;
        }
        if (c < 0) lo = mid + 1;
        else hi = mid;
    }
    *index = lo;
    return false;
}

/* Ok(None) is *found = false */
static jz_err idb_tree__TreeCore__resident_descent_from(const idb_tree_TreeCore *t, idb_tree_PageId root, jz_slice key, bool *found,
                                                       idb_tree_Descent *out) {
    idb_tree_PageId page_id = root;
    memset(out, 0, sizeof *out);
    jz_hmap_init(&out->visited, &PID_SET);
    for (;;) {
        if (!set_insert(&out->visited, page_id)) {
            idb_tree__Descent__drop(out);
            return err_invalid_page("tree child graph contains a cycle or shared page");
        }
        const idb_tree_Page *page = (const idb_tree_Page *)jz_hmap_get(&t->pages, &page_id);
        if (!page) {
            idb_tree__Descent__drop(out);
            *found = false;
            return JZ_OK;
        }
        switch (page->tag) {
        case IDB_PAGE_LEAF:
            out->page_id = page_id;
            out->entries = &page->entries;
            *found = true;
            return JZ_OK;
        case IDB_PAGE_INTERNAL: {
            usize child_index = partition_point_le(&page->keys, key);
            idb_tree_PathStep step = {page_id, child_index};
            jz_vec_push(&out->path, &step, sizeof step);
            page_id = page->children.ptr[child_index];
            break;
        }
        case IDB_PAGE_OVERFLOW:
            idb_tree__Descent__drop(out);
            return err_invalid_page("overflow page reached during tree descent");
        }
    }
}

static jz_err idb_tree__TreeCore__resident_descent(const idb_tree_TreeCore *t, jz_slice key, bool *found, idb_tree_Descent *out) {
    return idb_tree__TreeCore__resident_descent_from(t, idb_tree__TreeCore__root_page_id(t), key, found, out);
}

static jz_err idb_tree__TreeCore__missing_page_for_key_from(const idb_tree_TreeCore *t, idb_tree_PageId root, jz_slice key,
                                                           idb_tree_PageId *out) {
    idb_tree_PageId page_id = root;
    jz_hmap visited;
    jz_hmap_init(&visited, &PID_SET);
    jz_err e = JZ_OK;
    for (;;) {
        if (!set_insert(&visited, page_id)) {
            e = err_invalid_page("tree child graph contains a cycle or shared page");
            break;
        }
        const idb_tree_Page *page = (const idb_tree_Page *)jz_hmap_get(&t->pages, &page_id);
        if (!page) {
            *out = page_id;
            break;
        }
        if (page->tag == IDB_PAGE_LEAF) {
            e = err_invalid_page("requested a missing page for a resident descent");
            break;
        } else if (page->tag == IDB_PAGE_INTERNAL) {
            page_id = page->children.ptr[partition_point_le(&page->keys, key)];
        } else {
            e = err_invalid_page("overflow page reached during tree descent");
            break;
        }
    }
    jz_hmap_drop(&visited);
    return e;
}

static jz_err idb_tree__TreeCore__missing_page_for_key(const idb_tree_TreeCore *t, jz_slice key, idb_tree_PageId *out) {
    return idb_tree__TreeCore__missing_page_for_key_from(t, idb_tree__TreeCore__root_page_id(t), key, out);
}

/* ---- overflow chains ---- */
/* Ok(None) is *resident = false */
static jz_err idb_tree__TreeCore__overflow_page(const idb_tree_TreeCore *t, idb_tree_PageId page_id, jz_hmap *visited, bool *resident,
                                               bool *has_next, idb_tree_PageId *next, jz_slice *bytes) {
    if (!set_insert(visited, page_id)) return err_invalid_page("tree graph contains a cycle or shared page");
    const idb_tree_Page *page = (const idb_tree_Page *)jz_hmap_get(&t->pages, &page_id);
    if (!page) {
        *resident = false;
        return JZ_OK;
    }
    if (page->tag != IDB_PAGE_OVERFLOW) return err_invalid_page("value references a non-overflow page");
    *resident = true;
    *has_next = page->has_next;
    *next = page->next;
    *bytes = jz_bytes_slice(&page->bytes);
    return JZ_OK;
}

static jz_err idb_tree__TreeCore__value_resident(const idb_tree_TreeCore *t, const idb_tree_ValueCell *value, jz_hmap *visited,
                                                idb_tree_Attempt *out) {
    *out = READY;
    if (value->tag != IDB_VALUE_OVERFLOW) return JZ_OK;
    bool has_current = true;
    idb_tree_PageId current = value->head;
    while (has_current) {
        bool resident, has_next;
        idb_tree_PageId next;
        jz_slice bytes;
        JZ_TRY(idb_tree__TreeCore__overflow_page(t, current, visited, &resident, &has_next, &next, &bytes));
        if (!resident) {
            *out = missing(current);
            return JZ_OK;
        }
        has_current = has_next;
        current = next;
    }
    return JZ_OK;
}

static jz_err idb_tree__TreeCore__leaf_values_resident(const idb_tree_TreeCore *t, const idb_tree_LeafEntryVec *entries, jz_hmap *visited,
                                                      idb_tree_Attempt *out) {
    for (u32 i = 0; i < entries->len; i++) {
        JZ_TRY(idb_tree__TreeCore__value_resident(t, &entries->ptr[i].value, visited, out));
        if (out->missing) return JZ_OK;
    }
    *out = READY;
    return JZ_OK;
}

static jz_err idb_tree__TreeCore__read_value_resident(const idb_tree_TreeCore *t, const idb_tree_ValueCell *value, jz_hmap *visited,
                                                     idb_tree_Attempt *att, jz_bytes *out) {
    *att = READY;
    switch (value->tag) {
    case IDB_VALUE_INLINE:
        *out = jz_bytes_clone(&value->inline_bytes);
        return JZ_OK;
    case IDB_VALUE_OVERFLOW: {
        jz_bytes output = {0};
        bool has_current = true;
        idb_tree_PageId current = value->head;
        while (has_current) {
            bool resident, has_next;
            idb_tree_PageId next;
            jz_slice bytes;
            jz_err e = idb_tree__TreeCore__overflow_page(t, current, visited, &resident, &has_next, &next, &bytes);
            if (e.code) {
                jz_bytes_drop(&output);
                return e;
            }
            if (!resident) {
                jz_bytes_drop(&output);
                *att = missing(current);
                return JZ_OK;
            }
            jz_bytes_push(&output, bytes.ptr, bytes.len);
            has_current = has_next;
            current = next;
        }
        if ((u64)output.len != value->len) {
            jz_err e = jz_err_fmt(IDB_ERR_INVALID_PAGE, "invalid IDBTree page: overflow value length is %u, expected %llu", output.len,
                                  (unsigned long long)value->len);
            jz_bytes_drop(&output);
            return e;
        }
        *out = output;
        return JZ_OK;
    }
    }
    JZ_UNREACHABLE();
}

/* ---- point reads ---- */
static jz_err idb_tree__TreeCore__try_value_equals(const idb_tree_TreeCore *t, idb_tree_PageId root, jz_slice key, jz_slice expected,
                                                  idb_tree_Attempt *att, u8 *out) {
    bool found;
    idb_tree_Descent d;
    JZ_TRY(idb_tree__TreeCore__resident_descent_from(t, root, key, &found, &d));
    if (!found) {
        idb_tree_PageId id;
        JZ_TRY(idb_tree__TreeCore__missing_page_for_key_from(t, root, key, &id));
        *att = missing(id);
        return JZ_OK;
    }
    *att = READY;
    jz_err e = JZ_OK;
    u32 index;
    if (!entries_search(d.entries, key, &index)) {
        *out = 0;
        goto done;
    }
    const idb_tree_ValueCell *cell = &d.entries->ptr[index].value;
    if (cell->tag == IDB_VALUE_INLINE) {
        *out = jz_slice_cmp(jz_bytes_slice(&cell->inline_bytes), expected) == 0 ? 2 : 1;
        goto done;
    }
    if ((u64)expected.len != cell->len) {
        *out = 1;
        goto done;
    }
    {
        bool has_current = true;
        idb_tree_PageId current = cell->head;
        usize offset = 0;
        while (has_current) {
            bool resident, has_next;
            idb_tree_PageId next;
            jz_slice bytes;
            e = idb_tree__TreeCore__overflow_page(t, current, &d.visited, &resident, &has_next, &next, &bytes);
            if (e.code) goto done;
            if (!resident) {
                *att = missing(current);
                goto done;
            }
            usize end = offset + bytes.len;
            if (end < offset) {
                e = err_invalid_page("overflow value length overflow");
                goto done;
            }
            if (end > expected.len || memcmp(expected.ptr + offset, bytes.ptr, bytes.len) != 0) {
                *out = 1;
                goto done;
            }
            offset = end;
            has_current = has_next;
            current = next;
        }
        *out = offset == expected.len ? 2 : 1;
    }
done:
    idb_tree__Descent__drop(&d);
    return e;
}

static jz_err idb_tree__TreeCore__try_get(const idb_tree_TreeCore *t, idb_tree_PageId root, jz_slice key, idb_tree_Attempt *att, bool *some,
                                         jz_bytes *out) {
    bool found;
    idb_tree_Descent d;
    JZ_TRY(idb_tree__TreeCore__resident_descent_from(t, root, key, &found, &d));
    if (!found) {
        idb_tree_PageId id;
        JZ_TRY(idb_tree__TreeCore__missing_page_for_key_from(t, root, key, &id));
        *att = missing(id);
        return JZ_OK;
    }
    u32 index;
    jz_err e = JZ_OK;
    if (entries_search(d.entries, key, &index)) {
        e = idb_tree__TreeCore__read_value_resident(t, &d.entries->ptr[index].value, &d.visited, att, out);
        *some = !e.code && !att->missing;
    } else {
        *att = READY;
        *some = false;
    }
    idb_tree__Descent__drop(&d);
    return e;
}

/* ---- writes ---- */
static void idb_tree__TreeCore__retire_page(idb_tree_TreeCore *t, idb_tree_PageId id) {
    if (!jz_btree_insert(&t->deleted, &id, NULL, NULL)) JZ_VEC_PUSH(t->retirement_undo, id);
}

static void idb_tree__TreeCore__retire_value(idb_tree_TreeCore *t, const idb_tree_ValueCell *value) {
    if (value->tag != IDB_VALUE_OVERFLOW) return;
    bool has_current = true;
    idb_tree_PageId current = value->head;
    while (has_current) {
        idb_tree_PageId id = current;
        const idb_tree_Page *page = (const idb_tree_Page *)jz_hmap_get(&t->pages, &id);
        if (!page) jz_panic("validated overflow is resident");
        if (page->tag != IDB_PAGE_OVERFLOW) JZ_UNREACHABLE();
        has_current = page->has_next;
        current = page->next;
        idb_tree__TreeCore__retire_page(t, id);
    }
}

/* takes ownership of `value` */
static jz_err idb_tree__TreeCore__build_value(idb_tree_TreeCore *t, jz_bytes value, idb_tree_ValueCell *out) {
    memset(out, 0, sizeof *out);
    if (value.len <= t->options.page_size / 4) {
        out->tag = IDB_VALUE_INLINE;
        out->inline_bytes = value;
        return JZ_OK;
    }
    u64 len = value.len;
    bool has_next = false;
    idb_tree_PageId next = 0;
    usize chunk_size = t->options.page_size > 64 ? t->options.page_size - 64 : 0;
    JZ_ASSERT(chunk_size != 0, "chunk size must be non-zero");
    u32 end = value.len;
    jz_err e = JZ_OK;
    while (end > 0) { /* value.rchunks(chunk_size) */
        u32 start = end >= chunk_size ? end - (u32)chunk_size : 0;
        idb_tree_Page page;
        memset(&page, 0, sizeof page);
        page.tag = IDB_PAGE_OVERFLOW;
        page.has_next = has_next;
        page.next = next;
        page.bytes = jz_bytes_from(value.ptr + start, end - start);
        if (!idb_tree__TreeCore__page_fits(t, &page)) {
            idb_tree__page__Page__drop(&page);
            e = err_invalid_page("overflow chunk does not fit configured page");
            goto fail;
        }
        e = idb_tree__TreeCore__allocate_page(t, &page, &next);
        if (e.code) goto fail;
        has_next = true;
        end = start;
    }
    jz_bytes_drop(&value);
    if (!has_next) jz_panic("large values have at least one chunk");
    out->tag = IDB_VALUE_OVERFLOW;
    out->head = next;
    out->len = len;
    return JZ_OK;
fail:
    jz_bytes_drop(&value);
    return e;
}

static void idb_tree__TreeCore__take_fresh_leaf(idb_tree_TreeCore *t, idb_tree_PageId id, idb_tree_LeafEntryVec *out) {
    idb_tree_Page *page = (idb_tree_Page *)jz_hmap_get(&t->pages, &id);
    if (!page || page->tag != IDB_PAGE_LEAF) jz_panic("descent ended at a resident leaf");
    *out = page->entries; /* mem::take */
    memset(&page->entries, 0, sizeof page->entries);
}

static jz_err idb_tree__TreeCore__split_leaf_entries(const idb_tree_TreeCore *t, idb_tree_PageId page_id, idb_tree_LeafEntryVec *entries,
                                                    idb_tree_LeafEntryVec *right) {
    if (entries->len < 2) return err_page_too_large(page_id, t->options.page_size);
    usize *sizes = (usize *)jz_alloc(sizeof(usize) * entries->len);
    for (u32 i = 0; i < entries->len; i++) sizes[i] = idb_tree__page__leaf_entry_len(&entries->ptr[i].key, &entries->ptr[i].value);
    u32 split;
    bool ok = idb_tree__byte_balanced_split(sizes, entries->len, IDB_LEAF_BASE_LEN, t->options.page_size, false, &split);
    jz_free(sizes);
    if (!ok) return err_page_too_large(page_id, t->options.page_size);
    vec_split_off(entries, split, sizeof(idb_tree_LeafEntry), right);
    return JZ_OK;
}

static void leaf_page(idb_tree_Page *p, idb_tree_LeafEntryVec entries) {
    memset(p, 0, sizeof *p);
    p->tag = IDB_PAGE_LEAF;
    p->entries = entries;
}

static void internal_page(idb_tree_Page *p, idb_tree_KeyVec keys, idb_tree_PageIdVec children) {
    memset(p, 0, sizeof *p);
    p->tag = IDB_PAGE_INTERNAL;
    p->keys = keys;
    p->children = children;
}

/* Split an over-full internal page (consumed), promoting one separator key. */
static jz_err idb_tree__TreeCore__split_internal(const idb_tree_TreeCore *t, idb_tree_PageId page_id, idb_tree_Page *page,
                                                idb_tree_Page *left, jz_bytes *separator, idb_tree_Page *right) {
    if (page->tag != IDB_PAGE_INTERNAL) jz_panic("only internal pages are split here");
    usize *sizes = (usize *)jz_alloc(sizeof(usize) * (page->keys.len ? page->keys.len : 1));
    for (u32 i = 0; i < page->keys.len; i++) sizes[i] = idb_tree__page__internal_key_len(&page->keys.ptr[i]);
    u32 middle;
    bool ok = idb_tree__byte_balanced_split(sizes, page->keys.len, IDB_INTERNAL_BASE_LEN, t->options.page_size, true, &middle);
    jz_free(sizes);
    if (!ok) {
        idb_tree__page__Page__drop(page);
        return err_page_too_large(page_id, t->options.page_size);
    }
    jz_vec_remove(&page->keys, middle, separator, sizeof(jz_bytes));
    idb_tree_KeyVec right_keys;
    idb_tree_PageIdVec right_children;
    vec_split_off(&page->keys, middle, sizeof(jz_bytes), &right_keys);
    vec_split_off(&page->children, middle + 1, sizeof(idb_tree_PageId), &right_children);
    internal_page(left, page->keys, page->children);
    internal_page(right, right_keys, right_children);
    return JZ_OK;
}

static void replacement_drop(idb_tree_PageReplacement *r) {
    if (r->tag == REPLACE_SPLIT) jz_bytes_drop(&r->separator);
}

static jz_err idb_tree__TreeCore__publish_replacement(idb_tree_TreeCore *t, idb_tree_PageReplacement replacement, idb_tree_Path path) {
    jz_err e = JZ_OK;
    for (u32 step = path.len; step > 0; step--) {
        idb_tree_PageId parent_id = path.ptr[step - 1].page_id;
        usize child_index = path.ptr[step - 1].child_index;
        bool fresh = idb_tree__TreeCore__is_fresh(t, parent_id);
        idb_tree_Page parent;
        idb_tree_Page *resident = (idb_tree_Page *)jz_hmap_get(&t->pages, &parent_id);
        if (!resident) jz_panic("descent path remains resident");
        if (fresh) {
            parent = *resident; /* mem::replace(page, Page::leaf()) */
            idb_tree__page__Page__leaf(resident);
        } else {
            idb_tree__page__Page__clone(&parent, resident);
        }
        if (parent.tag != IDB_PAGE_INTERNAL) {
            idb_tree__page__Page__drop(&parent);
            e = err_invalid_page("descent parent is not internal");
            goto fail;
        }
        idb_tree_KeyVec keys = parent.keys;
        idb_tree_PageIdVec children = parent.children;
        if (fresh) {
            if (replacement.tag == REPLACE_ONE) {
                children.ptr[child_index] = replacement.one;
                idb_tree_Page page;
                internal_page(&page, keys, children);
                jz_hmap_insert(&t->pages, &parent_id, &page, NULL);
                /* Every ancestor of a fresh page is fresh and already points at it */
                jz_vec_drop(&path, sizeof(idb_tree_PathStep), NULL);
                return JZ_OK;
            }
            children.ptr[child_index] = replacement.left;
            jz_vec_insert(&keys, (u32)child_index, &replacement.separator, sizeof(jz_bytes));
            jz_vec_insert(&children, (u32)child_index + 1, &replacement.right, sizeof(idb_tree_PageId));
            replacement.tag = REPLACE_ONE; /* separator moved */
            idb_tree_Page page;
            internal_page(&page, keys, children);
            if (idb_tree__TreeCore__page_fits(t, &page)) {
                jz_hmap_insert(&t->pages, &parent_id, &page, NULL);
                jz_vec_drop(&path, sizeof(idb_tree_PathStep), NULL);
                return JZ_OK;
            }
            idb_tree_Page left, right;
            jz_bytes separator;
            if ((e = idb_tree__TreeCore__split_internal(t, parent_id, &page, &left, &separator, &right)).code) goto fail;
            jz_hmap_insert(&t->pages, &parent_id, &left, NULL);
            idb_tree_PageId right_id;
            if ((e = idb_tree__TreeCore__allocate_page(t, &right, &right_id)).code) {
                jz_bytes_drop(&separator);
                goto fail;
            }
            replacement.tag = REPLACE_SPLIT;
            replacement.left = parent_id;
            replacement.separator = separator;
            replacement.right = right_id;
            continue;
        }
        idb_tree__TreeCore__retire_page(t, parent_id);
        if (replacement.tag == REPLACE_ONE) {
            children.ptr[child_index] = replacement.one;
            idb_tree_Page page;
            internal_page(&page, keys, children);
            if ((e = idb_tree__TreeCore__allocate_page(t, &page, &replacement.one)).code) goto fail;
        } else {
            children.ptr[child_index] = replacement.left;
            jz_vec_insert(&keys, (u32)child_index, &replacement.separator, sizeof(jz_bytes));
            jz_vec_insert(&children, (u32)child_index + 1, &replacement.right, sizeof(idb_tree_PageId));
            replacement.tag = REPLACE_ONE;
            idb_tree_Page page;
            internal_page(&page, keys, children);
            if (idb_tree__TreeCore__page_fits(t, &page)) {
                if ((e = idb_tree__TreeCore__allocate_page(t, &page, &replacement.one)).code) goto fail;
            } else {
                idb_tree_Page left, right;
                jz_bytes separator;
                if ((e = idb_tree__TreeCore__split_internal(t, parent_id, &page, &left, &separator, &right)).code) goto fail;
                idb_tree_PageId left_id, right_id;
                if ((e = idb_tree__TreeCore__allocate_page(t, &left, &left_id)).code) {
                    idb_tree__page__Page__drop(&right);
                    jz_bytes_drop(&separator);
                    goto fail;
                }
                if ((e = idb_tree__TreeCore__allocate_page(t, &right, &right_id)).code) {
                    jz_bytes_drop(&separator);
                    goto fail;
                }
                replacement.tag = REPLACE_SPLIT;
                replacement.left = left_id;
                replacement.separator = separator;
                replacement.right = right_id;
            }
        }
    }
    jz_vec_drop(&path, sizeof(idb_tree_PathStep), NULL);

    idb_tree_PageId root;
    if (replacement.tag == REPLACE_ONE) {
        root = replacement.one;
    } else {
        idb_tree_KeyVec keys = {0};
        idb_tree_PageIdVec children = {0};
        jz_vec_push(&keys, &replacement.separator, sizeof(jz_bytes));
        JZ_VEC_PUSH(children, replacement.left);
        JZ_VEC_PUSH(children, replacement.right);
        idb_tree_Page page;
        internal_page(&page, keys, children);
        JZ_TRY(idb_tree__TreeCore__allocate_page(t, &page, &root));
    }
    t->metadata.has_root_page_id = true;
    t->metadata.root_page_id = root;
    return JZ_OK;
fail:
    replacement_drop(&replacement);
    jz_vec_drop(&path, sizeof(idb_tree_PathStep), NULL);
    return e;
}

static jz_err idb_tree__TreeCore__finish_fresh_leaf_write(idb_tree_TreeCore *t, idb_tree_PageId page_id, idb_tree_LeafEntryVec entries,
                                                         idb_tree_Path path) {
    usize total = IDB_LEAF_BASE_LEN;
    JZ_VEC_FOR(idb_tree_LeafEntry, e, entries) total += idb_tree__page__leaf_entry_len(&e->key, &e->value);
    idb_tree_Page page;
    if (total <= t->options.page_size) {
        leaf_page(&page, entries);
        jz_hmap_insert(&t->pages, &page_id, &page, NULL);
        jz_vec_drop(&path, sizeof(idb_tree_PathStep), NULL);
        return JZ_OK;
    }
    idb_tree_LeafEntryVec right_entries;
    jz_err e = idb_tree__TreeCore__split_leaf_entries(t, page_id, &entries, &right_entries);
    if (e.code) {
        jz_vec_drop(&entries, sizeof(idb_tree_LeafEntry), idb_tree__page__LeafEntry__drop);
        jz_vec_drop(&path, sizeof(idb_tree_PathStep), NULL);
        return e;
    }
    jz_bytes separator = jz_bytes_clone(&right_entries.ptr[0].key);
    leaf_page(&page, right_entries);
    idb_tree_PageId right;
    e = idb_tree__TreeCore__allocate_page(t, &page, &right);
    if (e.code) {
        jz_bytes_drop(&separator);
        jz_vec_drop(&entries, sizeof(idb_tree_LeafEntry), idb_tree__page__LeafEntry__drop);
        jz_vec_drop(&path, sizeof(idb_tree_PathStep), NULL);
        return e;
    }
    leaf_page(&page, entries);
    jz_hmap_insert(&t->pages, &page_id, &page, NULL);
    idb_tree_PageReplacement r = {REPLACE_SPLIT, 0, page_id, separator, right};
    return idb_tree__TreeCore__publish_replacement(t, r, path);
}

static jz_err idb_tree__TreeCore__finish_leaf_write(idb_tree_TreeCore *t, idb_tree_PageId page_id, idb_tree_LeafEntryVec entries,
                                                   idb_tree_Path path) {
    idb_tree_Page page;
    leaf_page(&page, entries);
    idb_tree_PageReplacement replacement;
    memset(&replacement, 0, sizeof replacement);
    jz_err e;
    if (idb_tree__TreeCore__page_fits(t, &page)) {
        replacement.tag = REPLACE_ONE;
        if ((e = idb_tree__TreeCore__allocate_page(t, &page, &replacement.one)).code) goto fail;
    } else {
        idb_tree_LeafEntryVec right_entries;
        if ((e = idb_tree__TreeCore__split_leaf_entries(t, page_id, &page.entries, &right_entries)).code) {
            idb_tree__page__Page__drop(&page);
            goto fail;
        }
        jz_bytes separator = jz_bytes_clone(&right_entries.ptr[0].key);
        idb_tree_PageId left, right;
        if ((e = idb_tree__TreeCore__allocate_page(t, &page, &left)).code) {
            jz_bytes_drop(&separator);
            jz_vec_drop(&right_entries, sizeof(idb_tree_LeafEntry), idb_tree__page__LeafEntry__drop);
            goto fail;
        }
        idb_tree_Page right_page;
        leaf_page(&right_page, right_entries);
        if ((e = idb_tree__TreeCore__allocate_page(t, &right_page, &right)).code) {
            jz_bytes_drop(&separator);
            goto fail;
        }
        replacement.tag = REPLACE_SPLIT;
        replacement.left = left;
        replacement.separator = separator;
        replacement.right = right;
    }
    idb_tree__TreeCore__retire_page(t, page_id);
    return idb_tree__TreeCore__publish_replacement(t, replacement, path);
fail:
    jz_vec_drop(&path, sizeof(idb_tree_PathStep), NULL);
    return e;
}

static jz_err idb_tree__TreeCore__try_put(idb_tree_TreeCore *t, jz_slice key, jz_slice value, idb_tree_Attempt *att) {
    bool found;
    idb_tree_Descent d;
    JZ_TRY(idb_tree__TreeCore__resident_descent(t, key, &found, &d));
    if (!found) {
        idb_tree_PageId id;
        JZ_TRY(idb_tree__TreeCore__missing_page_for_key(t, key, &id));
        *att = missing(id);
        return JZ_OK;
    }
    idb_tree_PageId page_id = d.page_id;
    jz_err e;
    if (idb_tree__TreeCore__is_fresh(t, page_id)) {
        jz_hmap_drop(&d.visited);
        idb_tree_ValueCell new_value;
        if ((e = idb_tree__TreeCore__build_value(t, jz_bytes_from(value.ptr, value.len), &new_value)).code) {
            jz_vec_drop(&d.path, sizeof(idb_tree_PathStep), NULL);
            return e;
        }
        idb_tree_LeafEntryVec entries;
        idb_tree__TreeCore__take_fresh_leaf(t, page_id, &entries);
        u32 index;
        if (entries_search(&entries, key, &index)) {
            idb_tree_ValueCell old = entries.ptr[index].value;
            entries.ptr[index].value = new_value;
            idb_tree__TreeCore__retire_value(t, &old);
            idb_tree__page__ValueCell__drop(&old);
        } else {
            idb_tree_LeafEntry entry = {jz_bytes_from(key.ptr, key.len), new_value};
            jz_vec_insert(&entries, index, &entry, sizeof entry);
        }
        JZ_TRY(idb_tree__TreeCore__finish_fresh_leaf_write(t, page_id, entries, d.path));
        *att = READY;
        return JZ_OK;
    }
    idb_tree_Attempt va;
    e = idb_tree__TreeCore__leaf_values_resident(t, d.entries, &d.visited, &va);
    jz_hmap_drop(&d.visited);
    if (e.code || va.missing) {
        jz_vec_drop(&d.path, sizeof(idb_tree_PathStep), NULL);
        if (!e.code) *att = va;
        return e;
    }
    idb_tree_LeafEntryVec entries;
    jz_vec_clone(&entries, d.entries, sizeof(idb_tree_LeafEntry), idb_tree__page__LeafEntry__clone);
    idb_tree_ValueCell new_value;
    if ((e = idb_tree__TreeCore__build_value(t, jz_bytes_from(value.ptr, value.len), &new_value)).code) {
        jz_vec_drop(&entries, sizeof(idb_tree_LeafEntry), idb_tree__page__LeafEntry__drop);
        jz_vec_drop(&d.path, sizeof(idb_tree_PathStep), NULL);
        return e;
    }
    u32 index;
    if (entries_search(&entries, key, &index)) {
        idb_tree__TreeCore__retire_value(t, &entries.ptr[index].value);
        idb_tree__page__ValueCell__drop(&entries.ptr[index].value);
        entries.ptr[index].value = new_value;
    } else {
        idb_tree_LeafEntry entry = {jz_bytes_from(key.ptr, key.len), new_value};
        jz_vec_insert(&entries, index, &entry, sizeof entry);
    }
    JZ_TRY(idb_tree__TreeCore__finish_leaf_write(t, page_id, entries, d.path));
    *att = READY;
    return JZ_OK;
}

static jz_err idb_tree__TreeCore__try_delete(idb_tree_TreeCore *t, jz_slice key, idb_tree_Attempt *att, bool *deleted) {
    bool found;
    idb_tree_Descent d;
    JZ_TRY(idb_tree__TreeCore__resident_descent(t, key, &found, &d));
    if (!found) {
        idb_tree_PageId id;
        JZ_TRY(idb_tree__TreeCore__missing_page_for_key(t, key, &id));
        *att = missing(id);
        return JZ_OK;
    }
    u32 index;
    if (!entries_search(d.entries, key, &index)) {
        idb_tree__Descent__drop(&d);
        *att = READY;
        *deleted = false;
        return JZ_OK;
    }
    idb_tree_PageId page_id = d.page_id;
    jz_err e;
    if (idb_tree__TreeCore__is_fresh(t, page_id)) {
        jz_hmap_drop(&d.visited);
        idb_tree_LeafEntryVec entries;
        idb_tree__TreeCore__take_fresh_leaf(t, page_id, &entries);
        idb_tree_LeafEntry old;
        jz_vec_remove(&entries, index, &old, sizeof old);
        idb_tree__TreeCore__retire_value(t, &old.value);
        idb_tree__page__LeafEntry__drop(&old);
        JZ_TRY(idb_tree__TreeCore__finish_fresh_leaf_write(t, page_id, entries, d.path));
        *att = READY;
        *deleted = true;
        return JZ_OK;
    }
    idb_tree_Attempt va;
    e = idb_tree__TreeCore__leaf_values_resident(t, d.entries, &d.visited, &va);
    jz_hmap_drop(&d.visited);
    if (e.code || va.missing) {
        jz_vec_drop(&d.path, sizeof(idb_tree_PathStep), NULL);
        if (!e.code) *att = va;
        return e;
    }
    idb_tree_LeafEntryVec entries;
    jz_vec_clone(&entries, d.entries, sizeof(idb_tree_LeafEntry), idb_tree__page__LeafEntry__clone);
    idb_tree__TreeCore__retire_value(t, &entries.ptr[index].value);
    idb_tree_LeafEntry removed;
    jz_vec_remove(&entries, index, &removed, sizeof removed);
    idb_tree__page__LeafEntry__drop(&removed);
    JZ_TRY(idb_tree__TreeCore__finish_leaf_write(t, page_id, entries, d.path));
    *att = READY;
    *deleted = true;
    return JZ_OK;
}

static jz_err idb_tree__TreeCore__write_path_resident(const idb_tree_TreeCore *t, jz_slice key, idb_tree_Attempt *att) {
    bool found;
    idb_tree_Descent d;
    JZ_TRY(idb_tree__TreeCore__resident_descent(t, key, &found, &d));
    if (!found) {
        idb_tree_PageId id;
        JZ_TRY(idb_tree__TreeCore__missing_page_for_key(t, key, &id));
        *att = missing(id);
        return JZ_OK;
    }
    jz_err e = idb_tree__TreeCore__leaf_values_resident(t, d.entries, &d.visited, att);
    idb_tree__Descent__drop(&d);
    return e;
}

static jz_err idb_tree__TreeCore__apply_write_many(idb_tree_TreeCore *t, idb_tree_WriteOperationVec operations) {
    jz_err e = JZ_OK;
    for (u32 i = 0; i < operations.len; i++) {
        idb_tree_WriteOperation *op = &operations.ptr[i];
        idb_tree_Attempt att;
        if (op->tag == IDB_WRITE_SET) {
            e = idb_tree__TreeCore__try_put(t, jz_bytes_slice(&op->key), jz_bytes_slice(&op->value), &att);
        } else {
            bool deleted;
            e = idb_tree__TreeCore__try_delete(t, jz_bytes_slice(&op->key), &att, &deleted);
        }
        if (e.code) break;
        if (att.missing) {
            e = jz_err_fmt(IDB_ERR_INVALID_PAGE, "invalid IDBTree page: prepared batch unexpectedly missed page %llu",
                           (unsigned long long)att.page_id);
            break;
        }
    }
    jz_vec_drop(&operations, sizeof(idb_tree_WriteOperation), idb_tree__WriteOperation__drop);
    return e;
}

/* ---- range scans ---- */
static bool idb_tree__TreeCore__scan_stops_at_miss(usize limit, const idb_tree_PageIdVec *missing_ids) {
    return limit != USIZE_MAX && missing_ids->len != 0;
}

static jz_err idb_tree__TreeCore__collect_range_resident(const idb_tree_TreeCore *t, idb_tree_PageId page_id, jz_slice start, jz_slice end,
                                                        usize limit, bool reverse, idb_tree_LeafEntryVec *output,
                                                        idb_tree_PageIdVec *missing_ids, jz_hmap *visited) {
    if (output->len == limit || idb_tree__TreeCore__scan_stops_at_miss(limit, missing_ids)) return JZ_OK;
    if (!set_insert(visited, page_id)) return err_invalid_page("tree child graph contains a cycle or shared page");
    const idb_tree_Page *page = (const idb_tree_Page *)jz_hmap_get(&t->pages, &page_id);
    if (!page) {
        JZ_VEC_PUSH(*missing_ids, page_id);
        return JZ_OK;
    }
    switch (page->tag) {
    case IDB_PAGE_LEAF: {
        if (missing_ids->len != 0) break;
        usize take = limit - output->len;
        u32 n = page->entries.len;
        for (u32 step = 0; step < n && take > 0; step++) {
            const idb_tree_LeafEntry *entry = &page->entries.ptr[reverse ? n - 1 - step : step];
            jz_slice k = jz_bytes_slice(&entry->key);
            if (jz_slice_cmp(k, start) >= 0 && jz_slice_cmp(k, end) < 0) {
                idb_tree_LeafEntry *slot = (idb_tree_LeafEntry *)jz_vec_push_slot(output, sizeof(idb_tree_LeafEntry));
                idb_tree__page__LeafEntry__clone(slot, entry);
                take--;
            }
        }
        break;
    }
    case IDB_PAGE_INTERNAL: {
        u32 n = page->children.len;
        for (u32 step = 0; step < n; step++) {
            u32 index = reverse ? n - 1 - step : step;
            bool below_end = index == 0 || jz_slice_cmp(jz_bytes_slice(&page->keys.ptr[index - 1]), end) < 0;
            bool above_start = index == page->keys.len || jz_slice_cmp(jz_bytes_slice(&page->keys.ptr[index]), start) > 0;
            if (below_end && above_start)
                JZ_TRY(idb_tree__TreeCore__collect_range_resident(t, page->children.ptr[index], start, end, limit, reverse, output,
                                                                  missing_ids, visited));
            if (output->len == limit || idb_tree__TreeCore__scan_stops_at_miss(limit, missing_ids)) break;
        }
        break;
    }
    case IDB_PAGE_OVERFLOW:
        return err_invalid_page(reverse ? "overflow page reached during reverse range traversal"
                                        : "overflow page reached during range traversal");
    }
    return JZ_OK;
}

/* ScanAttempt: Ready(rows) or Missing(page_ids) */
static jz_err idb_tree__TreeCore__materialize_range_values(const idb_tree_TreeCore *t, idb_tree_LeafEntryVec cells, jz_hmap visited,
                                                          idb_tree_KeyValueVec *rows, idb_tree_PageIdVec *missing_ids) {
    memset(rows, 0, sizeof *rows);
    jz_vec_reserve(rows, cells.len, sizeof(idb_tree_KeyValue));
    jz_err e = JZ_OK;
    for (u32 i = 0; i < cells.len; i++) {
        idb_tree_LeafEntry *cell = &cells.ptr[i];
        idb_tree_Attempt att;
        jz_bytes value = {0};
        e = idb_tree__TreeCore__read_value_resident(t, &cell->value, &visited, &att, &value);
        if (e.code) break;
        if (!att.missing && missing_ids->len == 0) {
            idb_tree_KeyValue kv = {cell->key, value};
            memset(&cell->key, 0, sizeof cell->key); /* moved */
            jz_vec_push(rows, &kv, sizeof kv);
        } else if (!att.missing) {
            jz_bytes_drop(&value);
        } else {
            JZ_VEC_PUSH(*missing_ids, att.page_id);
        }
    }
    jz_vec_drop(&cells, sizeof(idb_tree_LeafEntry), idb_tree__page__LeafEntry__drop);
    jz_hmap_drop(&visited);
    if (e.code || missing_ids->len) jz_vec_drop(rows, sizeof(idb_tree_KeyValue), idb_tree__KeyValue__drop);
    return e;
}

static jz_err idb_tree__TreeCore__try_range(const idb_tree_TreeCore *t, idb_tree_PageId root, jz_slice start, jz_slice end, usize limit,
                                           bool reverse, idb_tree_KeyValueVec *rows, idb_tree_PageIdVec *missing_ids) {
    idb_tree_LeafEntryVec cells = {0};
    jz_hmap visited;
    jz_hmap_init(&visited, &PID_SET);
    memset(missing_ids, 0, sizeof *missing_ids);
    jz_err e = idb_tree__TreeCore__collect_range_resident(t, root, start, end, limit, reverse, &cells, missing_ids, &visited);
    if (e.code || missing_ids->len) {
        jz_vec_drop(&cells, sizeof(idb_tree_LeafEntry), idb_tree__page__LeafEntry__drop);
        jz_hmap_drop(&visited);
        return e;
    }
    return idb_tree__TreeCore__materialize_range_values(t, cells, visited, rows, missing_ids);
}

/* ---- commit ---- */
static jz_err idb_tree__TreeCore__prepare_commit(idb_tree_TreeCore *t, bool *some, idb_tree_PreparedCommit *out) {
    if (t->commit_in_flight) return err_commit_in_flight();
    if (t->dirty.len == 0 && t->deleted.len == 0) {
        *some = false;
        return JZ_OK;
    }
    memset(out, 0, sizeof *out);
    jz_vec_reserve(&out->commit.pages, t->dirty.len, sizeof(idb_tree_CommitPage));
    u64 *idp;
    void *unused;
    JZ_BTREE_FOR(&t->dirty, idp, unused) {
        idb_tree_PageId page_id = *idp;
        if (jz_btree_contains(&t->deleted, &page_id)) continue;
        const idb_tree_Page *page = (const idb_tree_Page *)jz_hmap_get(&t->pages, &page_id);
        if (!page) jz_panic("dirty pages remain resident");
        jz_bytes bytes;
        jz_err e = idb_tree__page__encode_page(page, &bytes);
        if (e.code) {
            idb_tree__store__Commit__drop(&out->commit);
            return err_with_msg(IDB_ERR_INVALID_PAGE, e);
        }
        if (bytes.len > t->options.page_size) {
            jz_bytes_drop(&bytes);
            idb_tree__store__Commit__drop(&out->commit);
            return err_page_too_large(page_id, t->options.page_size);
        }
        idb_tree_CommitPage cp = {page_id, bytes};
        jz_vec_push(&out->commit.pages, &cp, sizeof cp);
    }
    t->commit_in_flight = true;
    out->commit.expected_generation = t->metadata.generation;
    out->commit.metadata = t->metadata;
    if (idb_tree__store__PageStore__can_reclaim_obsolete_pages(&t->store)) {
        JZ_BTREE_FOR(&t->deleted, idp, unused) {
            if (!jz_btree_contains(&t->dirty, idp)) JZ_VEC_PUSH(out->commit.deleted_page_ids, *idp);
        }
    }
    jz_btree_clear(&t->dirty);
    out->retired = t->deleted; /* mem::take */
    jz_btree_init(&t->deleted, &PID_BTREE_SET);
    *some = true;
    return JZ_OK;
}

static bool err_contains(const jz_err *e, const char *needle) {
    if (!e->msg) return false;
    u32 n = (u32)strlen(needle);
    for (u32 i = 0; i + n <= e->msg->len; i++)
        if (memcmp(e->msg->ptr + i, needle, n) == 0) return true;
    return false;
}

/* consumes `prepared` and `outcome` */
static jz_err idb_tree__TreeCore__complete_commit(idb_tree_TreeCore *t, idb_tree_PreparedCommit *prepared, jz_err outcome,
                                                 const idb_tree_Metadata *committed) {
    jz_err result = JZ_OK;
    if (!t->commit_in_flight) {
        jz_err_drop(&outcome);
        idb_tree__PreparedCommit__drop(prepared);
        return err_invalid_page("completed an IDBTree commit that was not in flight");
    }
    t->commit_in_flight = false;
    if (!outcome.code) {
        if (committed->generation != prepared->commit.expected_generation + 1) {
            result = jz_err_fmt(IDB_ERR_STORE, "IDBTree store error: commit returned generation %llu, expected %llu",
                                (unsigned long long)committed->generation,
                                (unsigned long long)(prepared->commit.expected_generation + 1));
        } else {
            u64 *idp;
            void *unused;
            JZ_BTREE_FOR(&prepared->retired, idp, unused) jz_hmap_remove(&t->pages, idp, NULL, NULL);
            t->has_durable_root = prepared->commit.metadata.has_root_page_id;
            t->durable_root = prepared->commit.metadata.root_page_id;
            t->metadata.generation = committed->generation;
        }
    } else {
        JZ_VEC_FOR(idb_tree_CommitPage, cp, prepared->commit.pages) {
            if (jz_hmap_get(&t->pages, &cp->id)) jz_btree_insert(&t->dirty, &cp->id, NULL, NULL);
        }
        u64 *idp;
        void *unused;
        JZ_BTREE_FOR(&prepared->retired, idp, unused) jz_btree_insert(&t->deleted, idp, NULL, NULL);
        if (err_contains(&outcome, "generation changed")) result = err_with_msg(IDB_ERR_GENERATION_CONFLICT, outcome);
        else result = err_with_msg(IDB_ERR_STORE, outcome);
    }
    idb_tree__PreparedCommit__drop(prepared);
    return result;
}

/* ---------------- IdbTree ---------------- */
static void ownership_rc_drop(void *p) { idb_tree__store__TreeOwnership__Drop__drop((idb_tree_TreeOwnership *)p); }

jz_err idb_tree__IdbTree__open(idb_tree_PageStore store, idb_tree_Options options, idb_tree_IdbTree *out) {
    idb_tree_TreeOwnership ownership;
    jz_err e = idb_tree__store__PageStore__claim_tree_ownership(&store, &ownership);
    if (e.code) {
        idb_tree__store__PageStore__drop(&store);
        return err_with_msg(IDB_ERR_STORE, e);
    }
    idb_tree_TreeCore tree;
    e = idb_tree__TreeCore__open(store, options, &tree);
    if (e.code) {
        idb_tree__store__TreeOwnership__Drop__drop(&ownership);
        return e;
    }
    if (!idb_tree__store__TreeOwnership__is_live(&ownership)) {
        idb_tree__TreeCore__drop_fields(&tree);
        idb_tree__store__TreeOwnership__Drop__drop(&ownership);
        return err_ownership_expired();
    }
    out->inner = (idb_tree_TreeCore *)jz_rc_new(sizeof(idb_tree_TreeCore), idb_tree__TreeCore__drop);
    *out->inner = tree;
    out->ownership = (idb_tree_TreeOwnership *)jz_rc_new(sizeof(idb_tree_TreeOwnership), ownership_rc_drop);
    *out->ownership = ownership;
    out->reload_epoch = (u64 *)jz_rc_new(sizeof(u64), NULL);
    *out->reload_epoch = 0;
    out->read_committed = false;
    return JZ_OK;
}

void idb_tree__IdbTree__clone(idb_tree_IdbTree *dst, const idb_tree_IdbTree *src) {
    dst->inner = (idb_tree_TreeCore *)jz_rc_clone(src->inner);
    dst->ownership = (idb_tree_TreeOwnership *)jz_rc_clone(src->ownership);
    dst->reload_epoch = (u64 *)jz_rc_clone(src->reload_epoch);
    dst->read_committed = src->read_committed;
}

void idb_tree__IdbTree__drop(idb_tree_IdbTree *t) {
    jz_rc_release(t->inner);
    jz_rc_release(t->ownership);
    jz_rc_release(t->reload_epoch);
}

void idb_tree__IdbTree__read_committed(const idb_tree_IdbTree *t, idb_tree_IdbTree *out) {
    idb_tree__IdbTree__clone(out, t);
    out->read_committed = true;
}

static jz_err idb_tree__IdbTree__ensure_writable(const idb_tree_IdbTree *t) {
    if (t->read_committed) return err_read_only_view();
    return JZ_OK;
}

/* Option<PageId> */
static bool idb_tree__IdbTree__read_root(const idb_tree_IdbTree *t, idb_tree_PageId *root) {
    if (t->read_committed) {
        *root = t->inner->durable_root;
        return t->inner->has_durable_root;
    }
    *root = idb_tree__TreeCore__root_page_id(t->inner);
    return true;
}

static jz_err idb_tree__IdbTree__ensure_live(const idb_tree_IdbTree *t) {
    if (!idb_tree__store__TreeOwnership__is_live(t->ownership)) return err_ownership_expired();
    if (!t->read_committed && t->inner->commit_abandoned) return err_commit_abandoned();
    return JZ_OK;
}

static jz_err idb_tree__IdbTree__hydrate(const idb_tree_IdbTree *t, idb_tree_PageId page_id) {
    if (jz_hmap_get(&t->inner->pages, &page_id)) return JZ_OK;
    idb_tree_PageStore store = idb_tree__store__PageStore__clone(&t->inner->store);
    bool root_before_some = t->inner->metadata.has_root_page_id;
    idb_tree_PageId root_before = t->inner->metadata.root_page_id;
    u64 reload_before = *t->reload_epoch;
    idb_tree_PageSlot slot;
    jz_err result = store.vt->read_page(store.self, page_id, &slot);
    idb_tree__store__PageStore__drop(&store);
    jz_err e = idb_tree__IdbTree__ensure_live(t);
    if (e.code) {
        if (result.code) jz_err_drop(&result);
        else idb_tree__store__PageSlot__drop(&slot);
        return e;
    }
    if (*t->reload_epoch != reload_before || t->inner->metadata.has_root_page_id != root_before_some ||
        (root_before_some && t->inner->metadata.root_page_id != root_before)) {
        if (result.code) jz_err_drop(&result);
        else idb_tree__store__PageSlot__drop(&slot);
        return JZ_OK;
    }
    if (result.code) return err_with_msg(IDB_ERR_STORE, result);
    if (!slot.some) return err_missing_page(page_id);
    usize page_size = t->inner->options.page_size;
    if (slot.bytes.len > page_size) {
        jz_bytes_drop(&slot.bytes);
        return err_page_too_large(page_id, page_size);
    }
    idb_tree_Page page;
    e = idb_tree__page__decode_page(slot.bytes.ptr, slot.bytes.len, &page);
    jz_bytes_drop(&slot.bytes);
    if (e.code) return err_with_msg(IDB_ERR_INVALID_PAGE, e);
    bool inserted;
    idb_tree_Page *dst = (idb_tree_Page *)jz_hmap_entry(&t->inner->pages, &page_id, &inserted);
    if (inserted) *dst = page;
    else idb_tree__page__Page__drop(&page);
    return JZ_OK;
}

/* consumes page_ids */
static jz_err idb_tree__IdbTree__hydrate_many(const idb_tree_IdbTree *t, idb_tree_PageIdVec page_ids) {
    idb_tree_PageStore store = idb_tree__store__PageStore__clone(&t->inner->store);
    idb_tree_PageIdVec wanted = {0};
    JZ_VEC_FOR(idb_tree_PageId, id, page_ids) if (!jz_hmap_get(&t->inner->pages, id)) JZ_VEC_PUSH(wanted, *id);
    jz_vec_drop(&page_ids, sizeof(idb_tree_PageId), NULL);
    jz_err e = JZ_OK;
    if (wanted.len == 0) goto out;
    if (wanted.len == 1) {
        e = idb_tree__IdbTree__hydrate(t, wanted.ptr[0]);
        goto out;
    }
    {
        bool root_before_some = t->inner->metadata.has_root_page_id;
        idb_tree_PageId root_before = t->inner->metadata.root_page_id;
        u64 reload_before = *t->reload_epoch;
        idb_tree_PageSlotVec pages = {0};
        jz_err result = idb_tree__store__PageStore__read_pages(&store, wanted.ptr, wanted.len, &pages);
        if ((e = idb_tree__IdbTree__ensure_live(t)).code) {
            if (result.code) jz_err_drop(&result);
            else jz_vec_drop(&pages, sizeof(idb_tree_PageSlot), idb_tree__store__PageSlot__drop);
            goto out;
        }
        if (*t->reload_epoch != reload_before || t->inner->metadata.has_root_page_id != root_before_some ||
            (root_before_some && t->inner->metadata.root_page_id != root_before)) {
            if (result.code) jz_err_drop(&result);
            else jz_vec_drop(&pages, sizeof(idb_tree_PageSlot), idb_tree__store__PageSlot__drop);
            goto out;
        }
        if (result.code) {
            e = err_with_msg(IDB_ERR_STORE, result);
            goto out;
        }
        if (pages.len != wanted.len) {
            e = jz_err_fmt(IDB_ERR_STORE, "IDBTree store error: read %u pages for %u requested ids", pages.len, wanted.len);
            jz_vec_drop(&pages, sizeof(idb_tree_PageSlot), idb_tree__store__PageSlot__drop);
            goto out;
        }
        usize page_size = t->inner->options.page_size;
        JZ_VEC(idb_tree_Page) decoded = {0};
        jz_vec_reserve(&decoded, pages.len, sizeof(idb_tree_Page));
        for (u32 i = 0; i < pages.len && !e.code; i++) {
            idb_tree_PageId page_id = wanted.ptr[i];
            idb_tree_PageSlot *slot = &pages.ptr[i];
            if (!slot->some) e = err_missing_page(page_id);
            else if (slot->bytes.len > page_size) e = err_page_too_large(page_id, page_size);
            else {
                idb_tree_Page page;
                jz_err de = idb_tree__page__decode_page(slot->bytes.ptr, slot->bytes.len, &page);
                if (de.code) e = err_with_msg(IDB_ERR_INVALID_PAGE, de);
                else jz_vec_push(&decoded, &page, sizeof page);
            }
        }
        jz_vec_drop(&pages, sizeof(idb_tree_PageSlot), idb_tree__store__PageSlot__drop);
        if (e.code) {
            jz_vec_drop(&decoded, sizeof(idb_tree_Page), idb_tree__page__Page__drop);
            goto out;
        }
        for (u32 i = 0; i < decoded.len; i++) {
            bool inserted;
            idb_tree_Page *dst = (idb_tree_Page *)jz_hmap_entry(&t->inner->pages, &wanted.ptr[i], &inserted);
            if (inserted) *dst = decoded.ptr[i];
            else idb_tree__page__Page__drop(&decoded.ptr[i]);
        }
        jz_vec_drop(&decoded, sizeof(idb_tree_Page), NULL);
    }
out:
    jz_vec_drop(&wanted, sizeof(idb_tree_PageId), NULL);
    idb_tree__store__PageStore__drop(&store);
    return e;
}

static jz_err idb_tree__IdbTree__hydrate_for_read(const idb_tree_IdbTree *t, idb_tree_PageId page_id) {
    if (t->read_committed) return err_not_resident(page_id);
    return idb_tree__IdbTree__hydrate(t, page_id);
}

static jz_err idb_tree__IdbTree__hydrate_many_for_read(const idb_tree_IdbTree *t, idb_tree_PageIdVec page_ids) {
    if (t->read_committed) {
        jz_err e = err_not_resident(page_ids.ptr[0]);
        jz_vec_drop(&page_ids, sizeof(idb_tree_PageId), NULL);
        return e;
    }
    return idb_tree__IdbTree__hydrate_many(t, page_ids);
}

jz_err idb_tree__IdbTree__reload(const idb_tree_IdbTree *t) {
    if (!idb_tree__store__TreeOwnership__is_live(t->ownership)) return err_ownership_expired();
    JZ_TRY(idb_tree__IdbTree__ensure_writable(t));
    if (t->inner->commit_in_flight) return err_commit_in_flight();
    idb_tree_PageStore store = idb_tree__store__PageStore__clone(&t->inner->store);
    idb_tree_Options options = t->inner->options;
    idb_tree_TreeCore fresh;
    JZ_TRY(idb_tree__TreeCore__open(store, options, &fresh));
    if (!idb_tree__store__TreeOwnership__is_live(t->ownership)) {
        idb_tree__TreeCore__drop_fields(&fresh);
        return err_ownership_expired();
    }
    idb_tree__TreeCore__drop_fields(t->inner);
    *t->inner = fresh;
    *t->reload_epoch += 1;
    return JZ_OK;
}

void idb_tree__IdbTree__metadata(const idb_tree_IdbTree *t, idb_tree_Metadata *out) { *out = t->inner->metadata; }

usize idb_tree__IdbTree__dirty_page_count(const idb_tree_IdbTree *t) { return t->inner->dirty.len; }

/* read_point / write_point: the closure becomes (fn, ctx) */
typedef jz_err (*idb_try_read_fn)(const idb_tree_TreeCore *t, idb_tree_PageId root, void *ctx, idb_tree_Attempt *att);
typedef jz_err (*idb_try_write_fn)(idb_tree_TreeCore *t, void *ctx, idb_tree_Attempt *att);

/* returns *some=false for a view of an empty store */
static jz_err idb_tree__IdbTree__read_point(const idb_tree_IdbTree *t, idb_try_read_fn try_read, void *ctx, bool *some) {
    for (;;) {
        JZ_TRY(idb_tree__IdbTree__ensure_live(t));
        idb_tree_Attempt att;
        idb_tree_PageId root;
        if (idb_tree__IdbTree__read_root(t, &root)) {
            JZ_TRY(try_read(t->inner, root, ctx, &att));
            *some = true;
        } else {
            att = READY;
            *some = false;
        }
        if (!att.missing) return JZ_OK;
        JZ_TRY(idb_tree__IdbTree__hydrate_for_read(t, att.page_id));
    }
}

static jz_err idb_tree__IdbTree__write_point(const idb_tree_IdbTree *t, idb_try_write_fn try_write, void *ctx) {
    JZ_TRY(idb_tree__IdbTree__ensure_writable(t));
    for (;;) {
        JZ_TRY(idb_tree__IdbTree__ensure_live(t));
        idb_tree_TreeCore *core = t->inner;
        idb_tree_WriteCheckpoint checkpoint = idb_tree__TreeCore__write_checkpoint(core);
        idb_tree_Attempt att;
        jz_err outcome = try_write(core, ctx, &att);
        idb_tree__TreeCore__finish_write(core, checkpoint, !outcome.code && !att.missing);
        JZ_TRY(outcome);
        if (!att.missing) return JZ_OK;
        JZ_TRY(idb_tree__IdbTree__hydrate(t, att.page_id));
    }
}

typedef struct { jz_slice key; jz_slice expected; u8 out; } value_equals_ctx;
static jz_err value_equals_try(const idb_tree_TreeCore *t, idb_tree_PageId root, void *ctx, idb_tree_Attempt *att) {
    value_equals_ctx *c = (value_equals_ctx *)ctx;
    return idb_tree__TreeCore__try_value_equals(t, root, c->key, c->expected, att, &c->out);
}

jz_err idb_tree__IdbTree__value_equals(const idb_tree_IdbTree *t, jz_slice key, jz_slice expected, u8 *out) {
    value_equals_ctx c = {key, expected, 0};
    bool some;
    JZ_TRY(idb_tree__IdbTree__read_point(t, value_equals_try, &c, &some));
    *out = some ? c.out : 0;
    return JZ_OK;
}

typedef struct { jz_slice key; bool some; jz_bytes value; } get_ctx;
static jz_err get_try(const idb_tree_TreeCore *t, idb_tree_PageId root, void *ctx, idb_tree_Attempt *att) {
    get_ctx *c = (get_ctx *)ctx;
    return idb_tree__TreeCore__try_get(t, root, c->key, att, &c->some, &c->value);
}

jz_err idb_tree__IdbTree__get(const idb_tree_IdbTree *t, jz_slice key, bool *some, jz_bytes *out) {
    get_ctx c = {key, false, {0}};
    bool rooted;
    JZ_TRY(idb_tree__IdbTree__read_point(t, get_try, &c, &rooted));
    *some = rooted && c.some;
    if (*some) *out = c.value;
    return JZ_OK;
}

typedef struct { jz_slice key; jz_slice value; } put_ctx;
static jz_err put_try(idb_tree_TreeCore *t, void *ctx, idb_tree_Attempt *att) {
    put_ctx *c = (put_ctx *)ctx;
    return idb_tree__TreeCore__try_put(t, c->key, c->value, att);
}

jz_err idb_tree__IdbTree__put(const idb_tree_IdbTree *t, jz_bytes key, jz_bytes value) {
    put_ctx c = {jz_bytes_slice(&key), jz_bytes_slice(&value)};
    jz_err e = idb_tree__IdbTree__write_point(t, put_try, &c);
    jz_bytes_drop(&key);
    jz_bytes_drop(&value);
    return e;
}

typedef struct { jz_slice key; bool deleted; } delete_ctx;
static jz_err delete_try(idb_tree_TreeCore *t, void *ctx, idb_tree_Attempt *att) {
    delete_ctx *c = (delete_ctx *)ctx;
    return idb_tree__TreeCore__try_delete(t, c->key, att, &c->deleted);
}

jz_err idb_tree__IdbTree__delete(const idb_tree_IdbTree *t, jz_slice key, bool *deleted) {
    delete_ctx c = {key, false};
    JZ_TRY(idb_tree__IdbTree__write_point(t, delete_try, &c));
    *deleted = c.deleted;
    return JZ_OK;
}

jz_err idb_tree__IdbTree__write_many(const idb_tree_IdbTree *t, idb_tree_WriteOperationVec operations) {
    jz_err e;
    if ((e = idb_tree__IdbTree__ensure_live(t)).code || (e = idb_tree__IdbTree__ensure_writable(t)).code) goto fail;
    for (u32 i = 0; i < operations.len; i++) {
        jz_slice key = jz_bytes_slice(&operations.ptr[i].key);
        for (;;) {
            if ((e = idb_tree__IdbTree__ensure_live(t)).code) goto fail;
            idb_tree_Attempt att;
            if ((e = idb_tree__TreeCore__write_path_resident(t->inner, key, &att)).code) goto fail;
            if (!att.missing) break;
            if ((e = idb_tree__IdbTree__hydrate(t, att.page_id)).code) goto fail;
        }
    }
    {
        idb_tree_TreeCore *core = t->inner;
        idb_tree_WriteCheckpoint checkpoint = idb_tree__TreeCore__write_checkpoint(core);
        jz_err outcome = idb_tree__TreeCore__apply_write_many(core, operations);
        idb_tree__TreeCore__finish_write(core, checkpoint, !outcome.code);
        return outcome;
    }
fail:
    jz_vec_drop(&operations, sizeof(idb_tree_WriteOperation), idb_tree__WriteOperation__drop);
    return e;
}

static jz_err idb_tree__IdbTree__scan(const idb_tree_IdbTree *t, jz_slice start, jz_slice end, usize limit, bool reverse,
                                     idb_tree_KeyValueVec *out) {
    for (;;) {
        JZ_TRY(idb_tree__IdbTree__ensure_live(t));
        idb_tree_PageId root;
        idb_tree_PageIdVec missing_ids = {0};
        if (idb_tree__IdbTree__read_root(t, &root)) {
            JZ_TRY(idb_tree__TreeCore__try_range(t->inner, root, start, end, limit, reverse, out, &missing_ids));
        } else {
            memset(out, 0, sizeof *out);
        }
        if (missing_ids.len == 0) return JZ_OK;
        JZ_TRY(idb_tree__IdbTree__hydrate_many_for_read(t, missing_ids));
    }
}

jz_err idb_tree__IdbTree__range(const idb_tree_IdbTree *t, jz_slice start, jz_slice end, idb_tree_KeyValueVec *out) {
    return idb_tree__IdbTree__range_limit(t, start, end, USIZE_MAX, out);
}

jz_err idb_tree__IdbTree__range_limit(const idb_tree_IdbTree *t, jz_slice start, jz_slice end, usize limit, idb_tree_KeyValueVec *out) {
    return idb_tree__IdbTree__scan(t, start, end, limit, false, out);
}

jz_err idb_tree__IdbTree__range_reverse(const idb_tree_IdbTree *t, jz_slice start, jz_slice end, usize limit, idb_tree_KeyValueVec *out) {
    return idb_tree__IdbTree__scan(t, start, end, limit, true, out);
}

jz_err idb_tree__IdbTree__flush(const idb_tree_IdbTree *t) {
    JZ_TRY(idb_tree__IdbTree__ensure_live(t));
    JZ_TRY(idb_tree__IdbTree__ensure_writable(t));
    idb_tree_PageStore store = idb_tree__store__PageStore__clone(&t->inner->store);
    bool some;
    idb_tree_PreparedCommit prepared;
    jz_err e = idb_tree__TreeCore__prepare_commit(t->inner, &some, &prepared);
    if (e.code || !some) {
        idb_tree__store__PageStore__drop(&store);
        return e;
    }
    idb_tree_AbandonOnDrop abandon = {t->inner};
    idb_tree_Metadata committed;
    jz_err outcome = store.vt->commit(store.self, idb_tree__PreparedCommit__commit(&prepared), &committed);
    abandon.inner = NULL;
    idb_tree__AbandonOnDrop__Drop__drop(&abandon);
    idb_tree__store__PageStore__drop(&store);
    return idb_tree__TreeCore__complete_commit(t->inner, &prepared, outcome, &committed);
}
