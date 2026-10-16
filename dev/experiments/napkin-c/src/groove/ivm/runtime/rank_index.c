/* groove::ivm::runtime::rank_index: in-memory ordered index with exact ranks
 * and path-copying snapshots (AVL over Rc nodes). One type-erased copy. */
#include "mod.h"

/* Rc<(K, V)> payload: descriptor pointer, then key bytes, then value bytes. */
typedef struct RankEntry { const groove_RankIndexDesc *d; u8 kv[]; } RankEntry;
struct groove_RankNode {
    RankEntry *entry; /* Rc */
    groove_RankNode *left, *right; /* Link = Option<Rc<Node>> */
    u32 height, count;
};

static void entry_drop(void *p) {
    RankEntry *e = p;
    if (e->d->key_drop) e->d->key_drop(e->kv);
    if (e->d->val_drop) e->d->val_drop(e->kv + e->d->key_size);
}
static void node_drop(void *p) {
    groove_RankNode *n = p;
    jz_rc_release(n->entry);
    if (n->left) jz_rc_release(n->left);
    if (n->right) jz_rc_release(n->right);
}
static void node_clone(void *dst, const void *src) {
    groove_RankNode *d = dst; const groove_RankNode *s = src;
    d->entry = jz_rc_clone(s->entry);
    d->left = s->left ? jz_rc_clone(s->left) : NULL;
    d->right = s->right ? jz_rc_clone(s->right) : NULL;
    d->height = s->height;
    d->count = s->count;
}
static groove_RankNode *make_mut(groove_RankNode **link) {
    return jz_rc_make_mut((void **)link, sizeof(groove_RankNode), node_clone);
}

static u32 height(groove_RankNode *const *link) { return *link ? (*link)->height : 0; }
static u32 count(groove_RankNode *const *link) { return *link ? (*link)->count : 0; }

static void refresh(groove_RankNode *n) {
    u32 l = height(&n->left), r = height(&n->right);
    n->height = 1 + (l > r ? l : r);
    n->count = 1 + count(&n->left) + count(&n->right);
}

static void rotate_left(groove_RankNode **link) {
    groove_RankNode *root = *link;
    *link = NULL;
    if (!root) jz_panic("rotation has a root");
    groove_RankNode *r = make_mut(&root);
    groove_RankNode *pivot = r->right;
    r->right = NULL;
    if (!pivot) jz_panic("right-heavy root");
    groove_RankNode *p = make_mut(&pivot);
    r = make_mut(&root);
    r->right = p->left;
    p->left = NULL;
    refresh(make_mut(&root));
    p = make_mut(&pivot);
    p->left = root;
    refresh(make_mut(&pivot));
    *link = pivot;
}

static void rotate_right(groove_RankNode **link) {
    groove_RankNode *root = *link;
    *link = NULL;
    if (!root) jz_panic("rotation has a root");
    groove_RankNode *r = make_mut(&root);
    groove_RankNode *pivot = r->left;
    r->left = NULL;
    if (!pivot) jz_panic("left-heavy root");
    groove_RankNode *p = make_mut(&pivot);
    r = make_mut(&root);
    r->left = p->right;
    p->right = NULL;
    refresh(make_mut(&root));
    p = make_mut(&pivot);
    p->right = root;
    refresh(make_mut(&pivot));
    *link = pivot;
}

static void balance(groove_RankNode **link) {
    if (!*link) return;
    groove_RankNode *node = make_mut(link);
    refresh(node);
    if (height(&node->left) > height(&node->right) + 1) {
        groove_RankNode *left = node->left;
        if (!left) jz_unwrap_failed();
        if (height(&left->right) > height(&left->left)) rotate_left(&node->left);
        rotate_right(link);
    } else if (height(&node->right) > height(&node->left) + 1) {
        groove_RankNode *right = node->right;
        if (!right) jz_unwrap_failed();
        if (height(&right->left) > height(&right->right)) rotate_right(&node->right);
        rotate_left(link);
    }
}

static void insert(groove_RankNode **link, RankEntry *entry) {
    if (!*link) {
        groove_RankNode *n = jz_rc_new(sizeof(groove_RankNode), node_drop);
        n->entry = entry; n->left = NULL; n->right = NULL; n->height = 1; n->count = 1;
        *link = n;
        return;
    }
    groove_RankNode *node = make_mut(link);
    int c = entry->d->cmp(entry->kv, node->entry->kv, NULL);
    if (c < 0) insert(&node->left, entry);
    else if (c > 0) insert(&node->right, entry);
    else {
        jz_rc_release(node->entry);
        node->entry = entry;
        return;
    }
    balance(link);
}

static RankEntry *pop_first(groove_RankNode **link) {
    if (!*link) jz_panic("nonempty successor subtree");
    groove_RankNode *node = make_mut(link);
    if (!node->left) {
        RankEntry *entry = jz_rc_clone(node->entry);
        groove_RankNode *right = node->right;
        node->right = NULL;
        jz_rc_release(*link);
        *link = right;
        return entry;
    }
    RankEntry *entry = pop_first(&node->left);
    balance(link);
    return entry;
}

static void remove_key(groove_RankNode **link, const void *key) {
    if (!*link) return;
    groove_RankNode *node = make_mut(link);
    int c = node->entry->d->cmp(key, node->entry->kv, NULL);
    if (c < 0) remove_key(&node->left, key);
    else if (c > 0) remove_key(&node->right, key);
    else {
        if (!node->left) {
            groove_RankNode *right = node->right; node->right = NULL;
            jz_rc_release(*link); *link = right;
            return;
        }
        if (!node->right) {
            groove_RankNode *left = node->left; node->left = NULL;
            jz_rc_release(*link); *link = left;
            return;
        }
        RankEntry *succ = pop_first(&node->right);
        jz_rc_release(node->entry);
        node->entry = succ;
    }
    balance(link);
}

void groove__ivm__runtime__rank_index__RankIndex__default(groove_RankIndex *r, const groove_RankIndexDesc *d) { r->root = NULL; r->d = d; }
void groove_RankIndex_clone(groove_RankIndex *dst, const groove_RankIndex *src) { dst->d = src->d; dst->root = src->root ? jz_rc_clone(src->root) : NULL; }
void groove_RankIndex_drop(groove_RankIndex *r) { if (r->root) jz_rc_release(r->root); r->root = NULL; }

void groove__ivm__runtime__rank_index__RankIndex__insert(groove_RankIndex *r, const void *key, const void *value) {
    RankEntry *e = jz_rc_new(sizeof(RankEntry) + r->d->key_size + r->d->val_size, entry_drop);
    e->d = r->d;
    memcpy(e->kv, key, r->d->key_size);
    if (r->d->val_size) memcpy(e->kv + r->d->key_size, value, r->d->val_size);
    insert(&r->root, e);
}

void groove__ivm__runtime__rank_index__RankIndex__remove(groove_RankIndex *r, const void *key) { remove_key(&r->root, key); }

bool groove__ivm__runtime__rank_index__RankIndex__contains(const groove_RankIndex *r, const void *key) {
    const groove_RankNode *next = r->root;
    while (next) {
        int c = r->d->cmp(key, next->entry->kv, NULL);
        if (c < 0) next = next->left;
        else if (c > 0) next = next->right;
        else return true;
    }
    return false;
}

/* Number of keys strictly before `key`, whether or not it is present. */
u32 groove__ivm__runtime__rank_index__RankIndex__rank(const groove_RankIndex *r, const void *key) {
    u32 rank = 0;
    const groove_RankNode *next = r->root;
    while (next) {
        int c = r->d->cmp(key, next->entry->kv, NULL);
        if (c < 0) next = next->left;
        else if (c == 0) return rank + count(&next->left);
        else {
            rank += count(&next->left) + 1;
            next = next->right;
        }
    }
    return rank;
}
