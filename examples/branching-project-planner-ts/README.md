# Branching project planner

This browser example captures a frozen base, stores its reference in an ordinary
scenario row and restores it after reopening a persistent local database.
`tasks.scenario_id` is both a reference and the table's `branchBy` column.

```sh
pnpm --filter branching-project-planner-ts dev
```

1. Select **Change main**. Main and the draft over its live base show the new
   title and estimate. The draft over its frozen base keeps the captured values.
2. Select **Fast-track in draft**. Both draft views show the draft's own estimate;
   main keeps its estimate.
3. Select **Save and reopen**. The database closes cleanly, then the page reloads
   the same account and dataset. It parses the saved reference and restores the
   frozen draft and your edits.

The capture and restoration use only public APIs:

```ts
const captured = await db.captureSnapshot();
const persisted = serializeSnapshotRef(captured);
// Store persisted in a string column. After reopening the same dataset:
const restored = parseSnapshotRef(persisted);
await db.all(app.tasks, {
  branch: draftScenario.id,
  base: [mainScenario.id, restored],
});
```

Capture includes earlier locally committed writes, even while they await server
acceptance. It excludes uncommitted transaction changes and works offline after
the first query or mutation attaches a schema. This example has no sync server.

The reference contains no rows and does not reserve history. Clearing the local
database loses the history it needs. A different device needs authority coverage
or sufficient complete history; a partial cache cannot certify a frozen cut.
The example displays unavailable-history errors without switching to a live base.
If a captured pending write is later rejected, a new read of that snapshot fails.
An already-open frozen subscription keeps its materialised base values through
that rejection and later main edits; its draft head continues to receive edits.

The application owns scenario status and base selection. Its task write policy
follows the ordinary `scenario_id` reference and permits edits only while that
scenario is open. Frozen reads and copy-on-write edits use normal permissions.
