# Authority And Conflicts

## 19. Authority Validation

Exclusive transactions must be validated by an authority before global
acceptance.

Authority-visible history is the history visible to the authority in the
transaction's branch view and catalogue/policy context, excluding unaccepted
proposals that are not valid inputs to the validation decision.

Authority validation uses the authority's current trusted policy catalogue.
This is a security invariant: old locally observed policy facts do not authorize
an exclusive transaction if current authority policy no longer allows it.

Stale read-set comparison, however, must use the same effective policy-filtered
view as the writer user whose transaction is being validated. Hidden rows
must not make a row, absence, predicate, or range read look stale merely because
they exist in authority storage. This avoids false conflicts and avoids leaking
the existence of rows the writer could not see. The validation read context is
therefore parameterized by writer user, branch/source view, current
catalogue/policy, and current authority-trusted history.

Validation checks:

- row reads still observe the same visible version
- absence reads are still absent
- range reads remain valid
- policy dependencies still authorize the operation
- declared constraints remain true

The authority conflict item for exclusive writes is the logical row. Two
exclusive transactions that write different columns of the same row are not
automatically safe merely because column masks are disjoint.

For cross-schema exclusive validation, the authority should normalize
read/write facts into the authority's current catalogue context when possible,
then compare through the lens/structural-layout graph. The original writer
catalogue metadata must still be retained for policy-filtered validation,
diagnostics, and redaction. Mergeable cross-schema writes merge with
translation; exclusive cross-schema writes are accepted only if translated
read/write facts remain valid under current authority policy and visibility.

Column masks are auxiliary metadata for:

- mergeable transactions
- conflict UI
- subscription invalidation
- policy/error explanation
- semantic diffs

Persisted transaction read sets should be a canonical subset of observed facts.
Write facts record table/schema identity, row id, operation, write base, and
column masks.

Read/write sets must be typed in memory. Durable encoding should begin inline on
transaction metadata. Hot side tables may be added when quantitative
measurements justify them.

Read-set entry kinds include:

```text
row
absence
range
policy
page_boundary
```

For updates and deletes, the write path must record the previously visible row
version as the write base.

Read/write sets replace explicit parent pointers as the first-order causality
and validation mechanism. Merge operations may need to walk read/write sets and
history; slow merge walks are acceptable initially.

Open issues:

- predicate/range read-set encoding
- efficient policy-filtered stale-read validation for predicate/range facts
- exact rejection redaction when a current-policy failure and a stale-read
  failure both apply
- validation indexing strategy
- side tables vs inline metadata for hot validation

## 20. Conflict Candidates And Resolution

Current projection rows expose:

- resolved value
- conflict metadata, empty when no conflict is visible

SQLite is responsible for durable storage and efficient candidate retrieval.
The engine should be able to find visible candidates for a logical row by table,
row id, branch/source context, transaction ordering/vector metadata,
schema/layout/catalogue context, operation kind, and policy-filtered
visibility. Complex merge algorithms do not live in SQL; SQL gathers the
candidate facts, then deterministic merge code interprets semantic values.

Conflict metadata may contain:

- candidate transaction ids
- candidate values or encrypted opaque values
- changed column masks
- base/read-set information
- resolution metadata

At minimum, durable non-empty conflict metadata identifies the candidate
transactions and whether the stored visible value is resolved or unresolved.
When a conflict is cleared, the history row must carry an explicit cleared
conflict state so rebuild does not resurrect old metadata.

Mergeable transactions may use per-column or per-field metadata to merge
automatically. Exclusive transactions remain row-granular for correctness.

Merge strategies are deterministic semantic reducers over normalized candidate
values. Built-in reducers may handle simple LWW/counter/set-like cases. Rich
text should be a blessed/built-in merge strategy early, not arbitrary app code
by default. Arbitrary application-defined merge functions are deferred until
catalogue versioning, code distribution, determinism, and unavailable-merge-code
semantics are specified.

Automatic deterministic merge may derive a resolved current value without
appending a new resolution transaction. Eager explicit resolution transactions
may be useful to shorten future history traversals, but they carry semantic
intent: "this conflict was acknowledged and this resolution was recorded." They
must therefore be used carefully rather than treated as invisible cache entries.

Conflict resolution is an ordinary transaction that reads the conflicted row,
writes the chosen value, records resolved candidates, and clears/updates
conflict metadata.

For v0, resolved-from provenance records candidate transaction ids. Additional
strategy, hash, source-branch, and user-intent metadata may be added later when
product conflict UX needs it.

User-facing conflict candidate APIs are policy-filtered. A user must not see
candidate values or infer hidden candidates they are not allowed to read.
Trusted peers and authorities may retrieve broader candidate sets internally
for merge, validation, and diagnostics, subject to redaction at user-facing
boundaries.

Open issues:

- candidate ordering
- multi-base branch conflict shape
- per-column UI/conflict metadata shape
- rich-text merge representation and determinism tests
- when to materialize deterministic automatic merges as explicit resolution
  transactions
- catalogue/versioning story for non-built-in merge code
