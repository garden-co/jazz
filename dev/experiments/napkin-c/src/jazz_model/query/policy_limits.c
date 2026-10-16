/* query/policy_limits.rs: semantic limits on recursively encoded policy expressions. */
#include "jazz_model/jazz_model.h"

#define P(x) jazz_model__query__policy_limits__##x

void P(PolicyExpressionLimitError__Display__fmt)(jz_str *out, const jazz_model_PolicyExpressionLimitError *e) {
    jz_str_appendf(out, "%s exceeded: observed %llu, maximum %llu", e->limit, (unsigned long long)e->actual,
                   (unsigned long long)e->max);
}

/* impl std::error::Error: surfaced through serde::de::Error::custom */
jz_err P(PolicyExpressionLimitError__into_err)(const jazz_model_PolicyExpressionLimitError *e) {
    jz_str msg = {0};
    P(PolicyExpressionLimitError__Display__fmt)(&msg, e);
    jz_err err = jz_err_fmt(JAZZ_MODEL_ERR_POLICY_LIMIT, "%.*s", (int)msg.len, msg.ptr);
    jz_str_drop(&msg);
    return err;
}

/* Counts one node at `depth` (the root is depth 1). */
bool P(PolicyExpressionBudget__enter)(jazz_model_PolicyExpressionBudget *b, u64 depth, jazz_model_PolicyExpressionLimitError *err) {
    if (depth > JAZZ_MAX_POLICY_EXPRESSION_DEPTH) {
        *err = (jazz_model_PolicyExpressionLimitError){"MAX_POLICY_EXPRESSION_DEPTH", JAZZ_MAX_POLICY_EXPRESSION_DEPTH, depth};
        return false;
    }
    if (b->nodes >= JAZZ_MAX_POLICY_EXPRESSION_NODES) {
        *err = (jazz_model_PolicyExpressionLimitError){"MAX_POLICY_EXPRESSION_NODES", JAZZ_MAX_POLICY_EXPRESSION_NODES, b->nodes + 1};
        return false;
    }
    b->nodes += 1;
    return true;
}

/* Refuses a declared child count the remaining node budget cannot hold. */
bool P(PolicyExpressionBudget__reserve)(const jazz_model_PolicyExpressionBudget *b, u64 children, jazz_model_PolicyExpressionLimitError *err) {
    if (children > JAZZ_MAX_POLICY_EXPRESSION_NODES - b->nodes) {
        u64 actual = b->nodes + children;
        if (actual < b->nodes) actual = UINT64_MAX; /* saturating_add */
        *err = (jazz_model_PolicyExpressionLimitError){"MAX_POLICY_EXPRESSION_NODES", JAZZ_MAX_POLICY_EXPRESSION_NODES, actual};
        return false;
    }
    return true;
}
