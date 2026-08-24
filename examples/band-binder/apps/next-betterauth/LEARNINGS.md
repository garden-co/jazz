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
  inputs through maintained policy relations. This branch includes that fix and
  keeps the authority-level test as a regression receipt.
- Large rich text and large attachment streaming remain tracked by #1833,
  #1839, and #1844. Small attachment bytes are represented directly today.
