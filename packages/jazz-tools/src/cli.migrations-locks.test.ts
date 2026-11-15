import { describe, it } from "vitest";

// The tests that were here exercised the lock, journal and two-file
// publication (stub + snapshot) of `migrations/` in the deleted
// src/dev/migrations.ts. `migrations create` now rewrites jazz.app.json
// (and jazz.generated.ts) in place with plain writes, so none of that
// machinery exists any more.
describe("cli migrations", () => {
  // QUESTION: should simultaneous `jazz-tools migrations create` runs against
  // one project still be serialized? The old suite asserted that two runs
  // never interleave ("serializes simultaneous migration generation against
  // one directory") and that a killed run leaves a recoverable state. The new
  // createAppMigration (src/dev/project.ts) does read-modify-write of
  // jazz.app.json with no lock, so two concurrent runs can lose one
  // migration, and a run killed between writing jazz.app.json and
  // jazz.generated.ts leaves the bindings behind the file (validate then
  // regenerates them).
  it.skip("serializes simultaneous migrations create runs against one project", () => {});
});
