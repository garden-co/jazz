### What still rejects

Unordered bounded root windows and non-recursive relation-local windows now
lower with ascending source row-id order. For array subqueries, that order and
any explicit child-order tie-break are evaluated independently within each
parent/correlation group.

Bounded windows over recursive closures still reject with
`UnsupportedShapeCapability`. A closure contains tuples produced across seed and
step iterations/depths, but the current public recursive relation has no single
source-child row id (or other declared occurrence identity) that totally orders
those tuples. Defining an order per depth or per iteration would change the
observable window as maintenance scheduling changes. Recursive windows need an
explicit closure-wide identity and ordering contract before they can lower to
`TopBy`.
