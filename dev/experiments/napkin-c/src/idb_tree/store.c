/* crates/idb-tree/src/store.rs (MemoryPageStore is test/native-only: not shipped) */
#include "idb_tree/idb_tree.h"

void idb_tree__store__Metadata__empty(idb_tree_Metadata *out, usize page_size) {
    out->page_size = page_size;
    out->generation = 0;
    out->has_root_page_id = false;
    out->root_page_id = 0;
    out->next_page_id = 0;
}

static void commit_page_drop(void *p) { jz_bytes_drop(&((idb_tree_CommitPage *)p)->bytes); }

void idb_tree__store__Commit__drop(idb_tree_Commit *c) {
    jz_vec_drop(&c->pages, sizeof(idb_tree_CommitPage), commit_page_drop);
    jz_vec_drop(&c->deleted_page_ids, sizeof(idb_tree_PageId), NULL);
}

void idb_tree__store__PageSlot__drop(void *p) {
    idb_tree_PageSlot *s = (idb_tree_PageSlot *)p;
    if (s->some) jz_bytes_drop(&s->bytes);
}

void idb_tree__store__TreeOwnership__Default__default(idb_tree_TreeOwnership *out) { memset(out, 0, sizeof *out); }

void idb_tree__store__TreeOwnership__new(idb_tree_TreeOwnership *out, void (*release)(void *), void *ctx, void (*ctx_drop)(void *)) {
    out->is_live = NULL;
    out->release = release;
    out->ctx = ctx;
    out->ctx_drop = ctx_drop;
}

void idb_tree__store__TreeOwnership__revocable(idb_tree_TreeOwnership *out, bool (*is_live)(void *), void (*release)(void *), void *ctx,
                                               void (*ctx_drop)(void *)) {
    out->is_live = is_live;
    out->release = release;
    out->ctx = ctx;
    out->ctx_drop = ctx_drop;
}

bool idb_tree__store__TreeOwnership__is_live(const idb_tree_TreeOwnership *o) {
    return o->is_live == NULL || o->is_live(o->ctx);
}

void idb_tree__store__TreeOwnership__Drop__drop(idb_tree_TreeOwnership *o) {
    void (*release)(void *) = o->release;
    o->release = NULL; /* Option::take */
    if (release) release(o->ctx);
    if (o->ctx_drop) o->ctx_drop(o->ctx);
    o->ctx_drop = NULL;
    o->is_live = NULL;
}

/* ---- PageStore trait default methods ---- */
jz_err idb_tree__store__PageStore__claim_tree_ownership(const idb_tree_PageStore *s, idb_tree_TreeOwnership *out) {
    if (s->vt->claim_tree_ownership) return s->vt->claim_tree_ownership(s->self, out);
    idb_tree__store__TreeOwnership__Default__default(out);
    return JZ_OK;
}

bool idb_tree__store__PageStore__can_reclaim_obsolete_pages(const idb_tree_PageStore *s) {
    if (s->vt->can_reclaim_obsolete_pages) return s->vt->can_reclaim_obsolete_pages(s->self);
    return false;
}

jz_err idb_tree__store__PageStore__read_pages(const idb_tree_PageStore *s, const idb_tree_PageId *ids, u32 n, idb_tree_PageSlotVec *out) {
    if (s->vt->read_pages) return s->vt->read_pages(s->self, ids, n, out);
    memset(out, 0, sizeof *out);
    jz_vec_reserve(out, n, sizeof(idb_tree_PageSlot));
    for (u32 i = 0; i < n; i++) {
        idb_tree_PageSlot slot;
        jz_err e = s->vt->read_page(s->self, ids[i], &slot);
        if (e.code) {
            jz_vec_drop(out, sizeof(idb_tree_PageSlot), idb_tree__store__PageSlot__drop);
            return e;
        }
        jz_vec_push(out, &slot, sizeof slot);
    }
    return JZ_OK;
}

idb_tree_PageStore idb_tree__store__PageStore__clone(const idb_tree_PageStore *s) {
    idb_tree_PageStore c = {s->vt, s->vt->clone(s->self)};
    return c;
}

void idb_tree__store__PageStore__drop(idb_tree_PageStore *s) {
    if (s->self) s->vt->drop(s->self);
    s->self = NULL;
}
