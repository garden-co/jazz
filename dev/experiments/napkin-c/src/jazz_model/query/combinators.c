/* query/combinators.rs: predicate and operand constructors. Also the
 * Predicate::{Eq,Ne,Gt,Gte,Lt,Lte,Contains} tuple-variant constructor fns that
 * ship as fn items (used as `fn(Operand, Operand) -> Predicate`). */
#include <string.h>
#include "jazz_model/jazz_model.h"

#define Q(x) jazz_model__query__##x

jazz_model_Operand Q(col)(jz_str name) { jazz_model_Operand o = {0}; o.tag = JAZZ_OPERAND_COLUMN; o.name = name; return o; }
jazz_model_Operand Q(param)(jz_str name) { jazz_model_Operand o = {0}; o.tag = JAZZ_OPERAND_PARAM; o.name = name; return o; }
jazz_model_Operand Q(claim)(jz_str name) { jazz_model_Operand o = {0}; o.tag = JAZZ_OPERAND_CLAIM; o.name = name; return o; }
jazz_model_Operand Q(lit)(groove_Value value) { jazz_model_Operand o = {0}; o.tag = JAZZ_OPERAND_LITERAL; o.literal = value; return o; }

/* eq / ne / gt / gte / lt / lte / contains, and the Predicate::X(..) variant fns */
jazz_model_Predicate Q(binary)(u8 tag, jazz_model_Operand left, jazz_model_Operand right) {
    jazz_model_Predicate p;
    memset(&p, 0, sizeof(p));
    p.tag = tag;
    p.left = left;
    p.right = right;
    return p;
}
jazz_model_Predicate Q(all_of)(jazz_model_PredicateVec predicates) {
    jazz_model_Predicate p;
    memset(&p, 0, sizeof(p));
    p.tag = JAZZ_PRED_ALL;
    p.children = predicates;
    return p;
}
jazz_model_Predicate Q(any_of)(jazz_model_PredicateVec predicates) {
    jazz_model_Predicate p = Q(all_of)(predicates);
    p.tag = JAZZ_PRED_ANY;
    return p;
}
jazz_model_Predicate Q(not)(jazz_model_Predicate predicate) {
    jazz_model_Predicate p;
    memset(&p, 0, sizeof(p));
    p.tag = JAZZ_PRED_NOT;
    p.inner = Q(Predicate__box)(predicate);
    return p;
}
jazz_model_Predicate Q(in_list)(jazz_model_Operand left, jazz_model_OperandVec values) {
    jazz_model_Predicate p;
    memset(&p, 0, sizeof(p));
    p.tag = JAZZ_PRED_IN;
    p.left = left;
    p.values = values;
    return p;
}
jazz_model_Predicate Q(is_null)(jazz_model_Operand operand) {
    jazz_model_Predicate p;
    memset(&p, 0, sizeof(p));
    p.tag = JAZZ_PRED_IS_NULL;
    p.left = operand;
    return p;
}
