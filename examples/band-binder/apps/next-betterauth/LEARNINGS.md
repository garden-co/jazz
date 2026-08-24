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
  inputs through maintained policy relations. The strict policy and its exact
  authority receipts are preserved here, but remain expected failures until the
  core carrier repair is in this branch's stack; BandBinder must not be treated
  as securely runnable before that restack.
- Large rich text and large attachment streaming remain tracked by #1833,
  #1839, and #1844. Small attachment bytes are represented directly today.
- The Rust root-query builder accepts `$createdAt` in `select(...)`, but a
  filtered root query ordered by it currently fails capability lowering because
  the provenance key is absent from the root projection. Both the TS live query
  and native benchmark preserve the intended `$createdAt` ordering; the native
  receipt remains an expected failure until the provenance projection repair is
  in this branch's stack.
