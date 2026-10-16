/* result_tree.h: crates/jazz/layers/db/src/result_tree.rs types */
#ifndef JAZZ_DB_RESULT_TREE_H
#define JAZZ_DB_RESULT_TREE_H
#include "_ext_db-b.h"

typedef struct jazz_db_ResultNode jazz_db_ResultNode;
typedef JZ_VEC(jazz_db_ResultNode) jazz_db_ResultNodeVec;
/* ResultRelation { Array(Vec<ResultNode>), Null, Hole } */
typedef enum { JAZZ_DB_REL_ARRAY = 0, JAZZ_DB_REL_NULL = 1, JAZZ_DB_REL_HOLE = 2 } jazz_db_ResultRelationTag;
typedef struct jazz_db_ResultRelation { u32 tag; jazz_db_ResultNodeVec array; } jazz_db_ResultRelation;
struct jazz_db_ResultNode {
    jazz_types_OutputOccurrenceId occurrence;
    jazz_node_CurrentRow row;
    jz_btree relations; /* String -> ResultRelation */
};
typedef struct jazz_db_ResultTree { jazz_db_ResultNodeVec roots; } jazz_db_ResultTree;
typedef struct jazz_db_ResultTreeReplacement {
    jazz_types_OutputOccurrenceId occurrence;
    jazz_db_ResultNode parent;
} jazz_db_ResultTreeReplacement;

extern const jz_btree_desc jazz_db__result_tree__relations_desc;
void jazz_db__result_tree__ResultNode__drop(void *p);
void jazz_db__result_tree__ResultNode__clone(void *dst, const void *src);
bool jazz_db__result_tree__ResultNode__eq(const void *a, const void *b);
void jazz_db__result_tree__ResultRelation__drop(void *p);
void jazz_db__result_tree__ResultTree__drop(jazz_db_ResultTree *t);
jazz_db_ResultRelation jazz_db__result_tree__ResultRelation__empty(void);
void jazz_db__result_tree__ResultTree__apply_reset(jazz_db_ResultTree *self, jazz_db_ResultTree reset);
void jazz_db__result_tree__ResultTree__apply_replacement(jazz_db_ResultTree *self, jazz_db_ResultTreeReplacement replacement);
#endif
