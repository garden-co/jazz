# BandBinder learnings

- Page and block ancestry does not currently define permission inheritance.
  BandBinder therefore stores `workspaceId` on every protected row and compiles
  one workspace-membership policy. If ancestry should narrow access further,
  the unanswered semantic question is whether a child intersects its own scope
  with every ancestor scope or inherits the nearest explicit scope.
- Concurrent sibling moves need a product rule before an app test can assert a
  winner. The implemented query surface only assumes stable, app-assigned
  `position` values and bounded `orderBy("position")` pages.
- Suggestions are ordinary member-authored rows which only owners and stage
  managers settle. This does not claim draft branches or conflict resolution;
  those semantics remain deliberately absent.
- Revocation prevents new authority receipts and future sync; it cannot erase
  rows already learned by an offline-capable client. The topology receipt tests
  rejected-write rollback and absence at the owner, not impossible remote
  deletion of a former member's local knowledge.
- The correlated membership receipt depends on the core carrying outer join
  inputs through maintained policy relations. The strict policy keeps exact
  authority receipts for recursive and block-dependent references on both
  insertion and mutation.
- A member can read their own membership grant directly, avoiding a bootstrap
  cycle where reading that grant first requires the workspace it unlocks. The
  browser receipt proves the grant arrives before asking the correlated
  workspace policy to admit it; the remaining core closure defect is tracked by
  [#1871](https://github.com/garden-co/jazz/issues/1871).
- Large rich text and large attachment streaming remain tracked by #1833,
  #1839, and #1844. Small attachment bytes are represented directly today.
- The Rust root-query builder accepts `$createdAt` in `select(...)`, but a
  filtered root query ordered by it needs the provenance key carried through the
  root projection. Both the TS live query and native benchmark preserve and test
  that `$createdAt` ordering.
