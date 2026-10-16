/* Port of crates/jazz/layers/node/src/node/query_eval/policy_factoring.rs
 * Factoring of disjunctive policy branches back into a conjunction of
 * disjunctions. */
#include "jazz_node/node/node_qe_b.h"

/* One conjunct of a policy branch. */
enum { ATOM_FILTER = 0, ATOM_JOIN = 1, ATOM_REACHABLE = 2, ATOM_INHERITS = 3 };
typedef struct Atom {
    u32 tag;
    union {
        jazz_model_Predicate filter;
        jazz_model_JoinVia join;
        jazz_model_ReachableVia reachable;
        jazz_model_InheritsVia inherits;
    } v;
} Atom;
typedef struct KeyedAtom { jz_bytes key; Atom atom; } KeyedAtom;

static void atom_drop(Atom *a)
{
    switch (a->tag) {
    case ATOM_FILTER: jazz_model_Predicate_drop(&a->v.filter); break;
    case ATOM_JOIN: jazz_model_JoinVia_drop(&a->v.join); break;
    case ATOM_REACHABLE: jazz_model_ReachableVia_drop(&a->v.reachable); break;
    case ATOM_INHERITS: jazz_model_InheritsVia_drop(&a->v.inherits); break;
    }
}
static void keyed_atom_drop(void *p)
{
    KeyedAtom *k = p;
    jz_bytes_drop(&k->key);
    atom_drop(&k->atom);
}

/* BTreeSet<usize> and BTreeSet<BTreeSet<usize>> descriptors. */
static const jz_btree_desc USIZE_SET = { sizeof(u32), 0, jz_cmp_u32, NULL, NULL };
static int set_cmp(const void *a, const void *b, void *ctx)
{
    const jz_btree *l = a, *r = b;
    jz_btree_iter li, ri;
    void *lk, *lv, *rk, *rv;
    jz_btree_iter_begin(&li, l);
    jz_btree_iter_begin(&ri, r);
    for (;;) {
        bool lh = jz_btree_next(&li, &lk, &lv);
        bool rh = jz_btree_next(&ri, &rk, &rv);
        if (!lh || !rh) return (int)lh - (int)rh;
        int c = jz_cmp_u32(lk, rk, NULL);
        if (c) return c;
    }
}
static int set_ref_cmp(const void *a, const void *b, void *ctx)
{
    return set_cmp(*(const jz_btree *const *)a, *(const jz_btree *const *)b, ctx);
}
static void set_drop(void *p) { jz_btree_drop(p); }
static const jz_btree_desc SET_OF_SETS = { sizeof(jz_btree), 0, set_cmp, set_drop, NULL };
static const jz_btree_desc SET_OF_SET_REFS = { sizeof(jz_btree *), 0, set_ref_cmp, NULL, NULL };

static u32 jazz_node__node__query_eval__policy_factoring__find(u32 *parent, u32 index)
{
    while (parent[index] != index) {
        parent[index] = parent[parent[index]];
        index = parent[index];
    }
    return index;
}

static bool jazz_node__node__query_eval__policy_factoring__atom_key(const Atom *atom, jz_bytes *out)
{
    jz_bytes buf = {0};
    jz_pc_w w = { &buf };
    switch (atom->tag) {
    case ATOM_FILTER: jz_pc_u8(&w, 0); jazz_model_Predicate_pc_encode(&w, &atom->v.filter); break;
    case ATOM_JOIN: jz_pc_u8(&w, 1); jazz_model_JoinVia_pc_encode(&w, &atom->v.join); break;
    case ATOM_REACHABLE: jz_pc_u8(&w, 2); jazz_model_ReachableVia_pc_encode(&w, &atom->v.reachable); break;
    case ATOM_INHERITS: jz_pc_u8(&w, 3); jazz_model_InheritsVia_pc_encode(&w, &atom->v.inherits); break;
    default: jz_bytes_drop(&buf); return false;
    }
    *out = buf;
    return true;
}

/* branch_atoms(): filters, then joins, reachable, inherits (cloned). */
typedef JZ_VEC(Atom) AtomVec;
static void jazz_node__node__query_eval__policy_factoring__branch_atoms(const jazz_model_PolicyBranch *b, AtomVec *out)
{
    Atom a;
    for (u32 i = 0; i < b->filters.len; i++) { a.tag = ATOM_FILTER; jazz_model_Predicate_clone(&a.v.filter, &b->filters.ptr[i]); jz_vec_push(out, &a, sizeof a); }
    for (u32 i = 0; i < b->joins.len; i++) { a.tag = ATOM_JOIN; jazz_model_JoinVia_clone(&a.v.join, &b->joins.ptr[i]); jz_vec_push(out, &a, sizeof a); }
    for (u32 i = 0; i < b->reachable.len; i++) { a.tag = ATOM_REACHABLE; jazz_model_ReachableVia_clone(&a.v.reachable, &b->reachable.ptr[i]); jz_vec_push(out, &a, sizeof a); }
    for (u32 i = 0; i < b->inherits.len; i++) { a.tag = ATOM_INHERITS; jazz_model_InheritsVia_clone(&a.v.inherits, &b->inherits.ptr[i]); jz_vec_push(out, &a, sizeof a); }
}

/* Rebuilds a partial branch from atom indices. */
static jazz_model_PolicyBranch jazz_node__node__query_eval__policy_factoring__branch_from_atoms(
    const KeyedAtom *atoms, const jz_btree *members)
{
    jazz_model_PolicyBranch branch;
    memset(&branch, 0, sizeof branch);
    u32 *idx; void *unused;
    JZ_BTREE_FOR(members, idx, unused) {
        const Atom *a = &atoms[*idx].atom;
        switch (a->tag) {
        case ATOM_FILTER: { jazz_model_Predicate *s = jz_vec_push_slot(&branch.filters, sizeof *s); jazz_model_Predicate_clone(s, &a->v.filter); break; }
        case ATOM_JOIN: { jazz_model_JoinVia *s = jz_vec_push_slot(&branch.joins, sizeof *s); jazz_model_JoinVia_clone(s, &a->v.join); break; }
        case ATOM_REACHABLE: { jazz_model_ReachableVia *s = jz_vec_push_slot(&branch.reachable, sizeof *s); jazz_model_ReachableVia_clone(s, &a->v.reachable); break; }
        case ATOM_INHERITS: { jazz_model_InheritsVia *s = jz_vec_push_slot(&branch.inherits, sizeof *s); jazz_model_InheritsVia_clone(s, &a->v.inherits); break; }
        }
    }
    return branch;
}

typedef struct Group { u32 root; jz_btree members; } Group;

/* Split `branches` into independent factors, each a list of alternatives.
 * Returns false (None) unless the branches are exactly the cartesian product
 * of at least two factors, one of which has more than one alternative. */
bool jazz_node__node__query_eval__policy_factoring__factor_policy_branches(
    const jazz_model_PolicyBranch *branches, u32 n, jazz_model_PolicyBranchVecVec *out)
{
    if (n < 4) return false;
    bool result = false;
    JZ_VEC(KeyedAtom) atoms = {0};
    JZ_VEC(jz_btree) rows = {0};
    JZ_VEC(Group) groups = {0};
    JZ_VEC(jz_btree) factors = {0};
    u32 *parent = NULL;
    jz_btree distinct;
    jz_btree_init(&distinct, &SET_OF_SET_REFS);
    jz_vec_reserve(&rows, n, sizeof(jz_btree));

    for (u32 b = 0; b < n; b++) {
        jz_btree row;
        jz_btree_init(&row, &USIZE_SET);
        AtomVec batoms = {0};
        jazz_node__node__query_eval__policy_factoring__branch_atoms(&branches[b], &batoms);
        bool ok = true;
        for (u32 i = 0; i < batoms.len; i++) {
            Atom atom = batoms.ptr[i];
            jz_bytes key;
            if (!ok || !jazz_node__node__query_eval__policy_factoring__atom_key(&atom, &key)) {
                ok = false;
                atom_drop(&atom);
                continue;
            }
            u32 index = atoms.len;
            for (u32 j = 0; j < atoms.len; j++) {
                if (jz_bytes_eq(&atoms.ptr[j].key, &key)) { index = j; break; }
            }
            if (index == atoms.len) {
                KeyedAtom ka = { key, atom };
                jz_vec_push(&atoms, &ka, sizeof ka);
            } else {
                jz_bytes_drop(&key);
                atom_drop(&atom);
            }
            jz_btree_insert(&row, &index, NULL, NULL);
        }
        jz_free(batoms.ptr);
        jz_vec_push(&rows, &row, sizeof row);
        if (!ok) goto done;
    }
    for (u32 r = 0; r < rows.len; r++) {
        jz_btree *p = &rows.ptr[r];
        jz_btree_insert(&distinct, &p, NULL, NULL);
    }
    if (distinct.len != rows.len) goto done;

    /* Two atoms belong to the same factor when their joint presence pattern
     * is not the product of their individual ones. */
    parent = jz_alloc(sizeof(u32) * (atoms.len ? atoms.len : 1));
    for (u32 i = 0; i < atoms.len; i++) parent[i] = i;
    for (u32 left = 0; left < atoms.len; left++) {
        for (u32 right = left + 1; right < atoms.len; right++) {
            u32 joint = 0, lpat = 0, rpat = 0; /* BTreeSet<(bool,bool)> as bitsets */
            for (u32 r = 0; r < rows.len; r++) {
                bool l = jz_btree_contains(&rows.ptr[r], &left);
                bool rr = jz_btree_contains(&rows.ptr[r], &right);
                joint |= 1u << ((u32)l * 2 + (u32)rr);
                lpat |= 1u << (u32)l;
                rpat |= 1u << (u32)rr;
            }
            if ((u32)__builtin_popcount(joint) != (u32)__builtin_popcount(lpat) * (u32)__builtin_popcount(rpat)) {
                u32 a = jazz_node__node__query_eval__policy_factoring__find(parent, left);
                u32 bb = jazz_node__node__query_eval__policy_factoring__find(parent, right);
                parent[a] = bb;
            }
        }
    }
    for (u32 atom = 0; atom < atoms.len; atom++) {
        u32 root = jazz_node__node__query_eval__policy_factoring__find(parent, atom);
        Group *found = NULL;
        JZ_VEC_FOR(Group, g, groups) if (g->root == root) { found = g; break; }
        if (found) {
            jz_btree_insert(&found->members, &atom, NULL, NULL);
        } else {
            Group g = { root };
            jz_btree_init(&g.members, &USIZE_SET);
            jz_btree_insert(&g.members, &atom, NULL, NULL);
            jz_vec_push(&groups, &g, sizeof g);
        }
    }
    JZ_VEC_FOR(Group, g, groups) {
        jz_btree alternatives;
        jz_btree_init(&alternatives, &SET_OF_SETS);
        for (u32 r = 0; r < rows.len; r++) {
            jz_btree inter;
            jz_btree_init(&inter, &USIZE_SET);
            u32 *k; void *v;
            JZ_BTREE_FOR(&rows.ptr[r], k, v) if (jz_btree_contains(&g->members, k)) jz_btree_insert(&inter, k, NULL, NULL);
            if (jz_btree_contains(&alternatives, &inter)) jz_btree_drop(&inter);
            else jz_btree_insert(&alternatives, &inter, NULL, NULL);
        }
        jz_vec_push(&factors, &alternatives, sizeof alternatives);
    }
    bool all_small = true;
    JZ_VEC_FOR(jz_btree, f, factors) if (f->len >= 2) { all_small = false; break; }
    if (factors.len < 2 || all_small) goto done;
    /* Only factor when the branch set is exactly the product. */
    usize product_size = 1;
    JZ_VEC_FOR(jz_btree, f, factors) {
        if (__builtin_mul_overflow(product_size, (usize)f->len, &product_size)) goto done;
    }
    if (product_size != rows.len) goto done;

    memset(out, 0, sizeof *out);
    JZ_VEC_FOR(jz_btree, f, factors) {
        jazz_model_PolicyBranchVec alts = {0};
        jz_btree *members; void *v;
        JZ_BTREE_FOR(f, members, v) {
            jazz_model_PolicyBranch br = jazz_node__node__query_eval__policy_factoring__branch_from_atoms(atoms.ptr, members);
            jz_vec_push(&alts, &br, sizeof br);
        }
        jz_vec_push(out, &alts, sizeof alts);
    }
    result = true;

done:
    jz_btree_drop(&distinct);
    jz_free(parent);
    JZ_VEC_FOR(Group, g, groups) jz_btree_drop(&g->members);
    jz_free(groups.ptr);
    jz_vec_drop(&factors, sizeof(jz_btree), set_drop);
    jz_vec_drop(&rows, sizeof(jz_btree), set_drop);
    jz_vec_drop(&atoms, sizeof(KeyedAtom), keyed_atom_drop);
    return result;
}
