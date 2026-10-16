/* result_tree.c: port of crates/jazz/layers/db/src/result_tree.rs */
#include "result_tree.h"

void jazz_db__result_tree__ResultRelation__drop(void *p) {
    jazz_db_ResultRelation *r = p;
    switch (r->tag) {
    case JAZZ_DB_REL_ARRAY: jz_vec_drop(&r->array, sizeof(jazz_db_ResultNode), jazz_db__result_tree__ResultNode__drop); break;
    case JAZZ_DB_REL_NULL: case JAZZ_DB_REL_HOLE: break;
    }
}

static void jazz_db__result_tree__ResultRelation__clone(void *dst, const void *src) {
    const jazz_db_ResultRelation *s = src;
    jazz_db_ResultRelation *d = dst;
    d->tag = s->tag;
    memset(&d->array, 0, sizeof d->array);
    if (s->tag == JAZZ_DB_REL_ARRAY)
        jz_vec_clone(&d->array, &s->array, sizeof(jazz_db_ResultNode), jazz_db__result_tree__ResultNode__clone);
}

static bool jazz_db__result_tree__ResultRelation__eq(const void *a, const void *b) {
    const jazz_db_ResultRelation *x = a, *y = b;
    if (x->tag != y->tag) return false;
    if (x->tag != JAZZ_DB_REL_ARRAY) return true;
    if (x->array.len != y->array.len) return false;
    for (u32 i = 0; i < x->array.len; i++)
        if (!jazz_db__result_tree__ResultNode__eq(&x->array.ptr[i], &y->array.ptr[i])) return false;
    return true;
}

static void str_drop(void *p) { jz_str_drop(p); }
static void str_clone(void *d, const void *s) { *(jz_str *)d = jz_str_clone(s); }

const jz_btree_desc jazz_db__result_tree__relations_desc = {
    sizeof(jz_str), sizeof(jazz_db_ResultRelation), jz_cmp_str, str_drop, jazz_db__result_tree__ResultRelation__drop,
};

void jazz_db__result_tree__ResultNode__drop(void *p) {
    jazz_db_ResultNode *n = p;
    jazz_types__object__OutputOccurrenceId__drop(&n->occurrence);
    jazz_node__node__CurrentRow__drop(&n->row);
    jz_btree_drop(&n->relations);
}

void jazz_db__result_tree__ResultNode__clone(void *dst, const void *src) {
    jazz_db_ResultNode *d = dst;
    const jazz_db_ResultNode *s = src;
    jazz_types__object__OutputOccurrenceId__clone(&d->occurrence, &s->occurrence);
    jazz_node__node__CurrentRow__clone(&d->row, &s->row);
    jz_btree_clone(&d->relations, &s->relations, str_clone, jazz_db__result_tree__ResultRelation__clone);
}

bool jazz_db__result_tree__ResultNode__eq(const void *a, const void *b) {
    const jazz_db_ResultNode *x = a, *y = b;
    return jazz_types__object__OutputOccurrenceId__eq(&x->occurrence, &y->occurrence) &&
           jazz_node__node__CurrentRow__eq(&x->row, &y->row) &&
           jz_btree_eq(&x->relations, &y->relations, jazz_db__result_tree__ResultRelation__eq);
}

void jazz_db__result_tree__ResultTree__drop(jazz_db_ResultTree *t) {
    jz_vec_drop(&t->roots, sizeof(jazz_db_ResultNode), jazz_db__result_tree__ResultNode__drop);
}

jazz_db_ResultRelation jazz_db__result_tree__ResultRelation__empty(void) {
    jazz_db_ResultRelation r;
    memset(&r, 0, sizeof r);
    r.tag = JAZZ_DB_REL_ARRAY;
    return r;
}

void jazz_db__result_tree__ResultTree__apply_reset(jazz_db_ResultTree *self, jazz_db_ResultTree reset) {
    jazz_db__result_tree__ResultTree__drop(self);
    *self = reset;
}

void jazz_db__result_tree__ResultTree__apply_replacement(jazz_db_ResultTree *self, jazz_db_ResultTreeReplacement replacement) {
    JZ_VEC_FOR(jazz_db_ResultNode, node, self->roots) {
        if (jazz_types__object__OutputOccurrenceId__eq(&node->occurrence, &replacement.occurrence)) {
            jazz_db__result_tree__ResultNode__drop(node);
            *node = replacement.parent;
            jazz_types__object__OutputOccurrenceId__drop(&replacement.occurrence);
            return;
        }
    }
    jz_vec_push(&self->roots, &replacement.parent, sizeof(jazz_db_ResultNode));
    jazz_types__object__OutputOccurrenceId__drop(&replacement.occurrence);
}
