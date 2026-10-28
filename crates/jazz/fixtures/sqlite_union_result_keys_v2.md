# SQLite relation projections and UNION result keys

This port executes narrowed and renamed relation outputs and nested UNION arms
using the existing query AST and result-key v2 encoding. It does not change
SQLite row storage, commit bodies or the sync version. The independent field
fixture `sqlite_union_result_keys_v2.json` pins two occurrences of the same UUID,
with distinct `a` and `b` arm labels. Historical fixtures remain unchanged.

A nested arm uses the model's existing byte-length-prefixed path, so arm identity
survives sibling reordering. Each arm applies its own read policy and window
before the outer order and window. Renamed output columns retain their source
value types; large-value comparisons rank the physical source column. Maintained
results include their output names and types in the sharing key.

Remote reads retain upstream prefix delivery. An inner offset requires an explicit
limit, and all inner prefixes together are bounded to 16,384 requested rows.
Predecessor reads use the same schema, identity, claims and read cut as their
outer query. They remain policy checked. Subscriptions maintain those bounded
prefixes even when the visible outer page is unchanged, and release them with the
outer request. An incomplete prefix cannot qualify a settled result.

Older runtimes can continue reading the same stored data and ordinary queries.
A peer that does not implement narrowed projections or nested UNIONs may refuse
those query shapes. Applications using them need support on each serving hop;
this port does not add a rolling-upgrade fallback or infer missing query coverage.
