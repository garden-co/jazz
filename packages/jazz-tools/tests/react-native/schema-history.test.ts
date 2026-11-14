import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { migration as m } from "../../src/migration-namespace.js";
import { ReadTier } from "../../src/runtime/client.js";
import { appArtifact, appPermissions, startTestJazz } from "../../src/testing/index.js";
import { withNativeRelayFixture } from "./fixture.js";

// A React Native client opens its native store along the migrations the app
// ships, as Node and browser clients do, so it stores each column where the
// server does and reads the server's rows after the app gained a version
// (#4777). Opened from the head schema alone it read nothing.
it("reads rows written before the app renamed a column, from a server with schema history", async () => {
  const before = { notes: s.table({ title: s.string() }, {}) };
  const after = { notes: s.table({ body: s.string() }, {}) };
  const rename = m.defineMigration({
    from: before,
    to: after,
    migrate: { notes: { body: m.renameFrom("title") } },
  });
  const allowBefore = s.definePermissions(
    s.defineApp(before, { migrations: "none" }),
    ({ policy }) => [policy.notes.allowRead.always(), policy.notes.allowInsert.always()],
  );
  const allowAfter = s.definePermissions(
    s.defineApp(after, { migrations: "none" }),
    ({ policy }) => [policy.notes.allowRead.always(), policy.notes.allowInsert.always()],
  );
  const beforeApp = s.defineApp(before, {
    artifact: await appArtifact(before),
  });
  const afterApp = s.defineApp(after, {
    artifact: await appArtifact(before, {
      steps: [{ to: after, migration: rename }],
    }),
  });
  const backend = await startTestJazz({
    app: beforeApp,
    permissions: await appPermissions(beforeApp, allowBefore),
  });
  try {
    let written: { id: string; title: string } | undefined;
    await withNativeRelayFixture(
      beforeApp,
      allowBefore,
      async (fixture) => {
        const db = await fixture.createDb();
        written = await db.insert(beforeApp.notes, { title: "written at v1" }).wait({
          tier: "global",
        });
      },
      { appId: backend.appId, upstream: { backend, userId: "native-alice" } },
    );
    await backend.redeploy({
      app: afterApp,
      permissions: await appPermissions(afterApp, allowAfter),
      migration: rename,
    });

    await withNativeRelayFixture(
      afterApp,
      allowAfter,
      async (fixture) => {
        const db = await fixture.createDb();
        expect(await db.all(afterApp.notes, { tier: ReadTier.Remote })).toEqual([
          { id: written!.id, body: "written at v1" },
        ]);
        // Its own write lands where the server keeps the column too.
        const own = await db.insert(afterApp.notes, { body: "written at v2" }).wait({
          tier: "global",
        });
        expect(await db.all(afterApp.notes, { tier: ReadTier.Remote })).toEqual(
          expect.arrayContaining([own]),
        );
      },
      { appId: backend.appId, upstream: { backend, userId: "native-bob" } },
    );
  } finally {
    await backend.stop();
  }
}, 60_000);
