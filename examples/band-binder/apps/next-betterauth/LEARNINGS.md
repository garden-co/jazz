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
- On base `af0d892f0`, the exact compiled-policy receipt reaches the authority
  but triggers `Storage: graph field not found: __root_join_row_0` while
  evaluating a correlated membership policy. The later core fix is
  `84bc4f441` (`retain correlated policy join inputs internally`); restacking
  this example onto current main is required before treating the receipt as a
  BandBinder failure.
- The shared topology harness is not present on this branch's base. After the
  app is stacked on its PR, add one `tests/browser/topology.e2e.test.tsx`
  consumer importing `runTopologyScenario` and `browserTopologyReporter`, plus
  the standard browser Vitest config/manifest entry. Its phases should admit a
  stage manager, create concurrent ordered blocks, revoke them, write locally
  while disconnected, reconnect, and compare exact ids/order on both clients.
- Large rich text and large attachment streaming remain tracked by #1833,
  #1839, and #1844. Small attachment bytes are represented directly today.
