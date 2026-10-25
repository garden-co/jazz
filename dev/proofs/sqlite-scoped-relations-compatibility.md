# SQLite request-scoped relation reads

A trusted backend can read existential relation traversals with immutable request
claims, including nested traversals and root ordering, limits and offsets. The
authority runs the original query under that request's identity and claims. It
then follows each returned root's indexed UUID correlations and evaluates every
witness's read policy with the same request snapshot.

Query witnesses and their bounded permission dependencies travel as ordinary
row images and exact coordinates in the existing
`jazz.sqlite.sync.scoped-evidence-answer.v1` receipt. The backend and any trusted
relay re-evaluate against those exact versions. A warm shared cache cannot add a
witness omitted from the receipt. The authority applies the root window once;
the backend verifies that its exact result reproduces the authority's ordered
root list. A missing witness fails with `NotObserved` instead of returning an
incomplete answer. Relays forward the admitted historical versions rather than
following the relation again against their current cache.

For example, two concurrent requests by Alice with different room codes can use
the same backend connection. Each request sees its own rooms and readable links.
A link with Mallory's code does not ship as Alice's query witness. A later request
after link revocation observes the new authority state, even if the backend
previously cached that link through a SYSTEM read.

Each traversal requires a scalar UUID correlation and a leading declared index
(or the target row ID). Query construction rejects uncorrelated, array-valued,
branched and unindexed traversals before executing even an empty read. Lookup
rows, nested traversals, permission dependencies and SQLite VM work consume the
existing scoped evidence budget. There is no fallback to loading a whole table.
Union occurrence coverage, includes, array output, aggregates, history and paired
accepted relation reads retain their existing explicit refusals.

No wire tags, stored bytes, receipt layout or identity admission rules change.
The existing scoped-fetch and scoped-evidence byte fixtures remain authoritative.
Older SQLite builds refuse the newly supported query shapes; a coordinated
unreleased E2EE/SQLite upgrade is still required. Ordinary session connections
cannot submit delegated scoped requests or receive private permission evidence.

`sqlite_scoped_relations` exercises concurrent claim isolation, nested traversal,
relay delivery, pagination, stale-cache revocation, exact witness omission,
unreadable witness exclusion, unbounded-shape refusal and forged delegation.
The existing scoped evidence, byte fixture and existential-join suites remain
unchanged.
