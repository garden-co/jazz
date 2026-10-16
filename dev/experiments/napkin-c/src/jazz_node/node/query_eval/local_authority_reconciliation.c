/* Port of crates/jazz/layers/node/src/node/query_eval/local_authority_reconciliation.rs */
#include "jazz_node/node/node_qe_b.h"

bool jazz_node__node__query_eval__local_authority_reconciliation__LocalAuthorityReconciliation__is_due(
    const jazz_node_LocalAuthorityReconciliation *self, const jazz_protocol_AuthorityResultKey *source, u64 generation)
{
    if (!self->has_source || !jazz_protocol_AuthorityResultKey_eq(&self->source, source)) return true;
    return generation != self->generation;
}

void jazz_node__node__query_eval__local_authority_reconciliation__LocalAuthorityReconciliation__replace_source(
    jazz_node_LocalAuthorityReconciliation *self, jazz_protocol_AuthorityResultKey source, u64 generation)
{
    if (self->has_source && jazz_protocol_AuthorityResultKey_eq(&self->source, &source)) {
        /* Re-registering the same immutable source cannot make an older
         * authority snapshot current again. */
        if (generation > self->generation) self->generation = generation;
        jazz_protocol_AuthorityResultKey_drop(&source);
        return;
    }
    if (self->has_source) jazz_protocol_AuthorityResultKey_drop(&self->source);
    self->source = source;
    self->has_source = true;
    self->generation = generation;
}
