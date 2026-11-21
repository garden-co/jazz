import { expect, it } from "vitest";
import { ReadTier } from "../../src/runtime/client.js";
import { startTestJazz, testApp, testPermissions } from "../../src/testing/index.js";
import { withNativeRelayFixture } from "./fixture.js";

// A React Native client opens its native store along the migrations the app
// ships, as Node and browser clients do, so it stores each column where the
// server does and reads the server's rows after the app gained a version
// (#4777). Opened from the head schema alone it read nothing.
it("reads rows written before the app renamed a column, from a server with schema history", async () => {
  const before = { notes: { columns: { title: "string" } } };
  const after = { notes: { columns: { body: "string" } } };
  const beforeApp = testApp({ tables: before });
  const afterApp = testApp({
    tables: after,
    history: { v1: { tables: before } },
    migrations: [{ from: "v1", migrate: { notes: { body: { renameFrom: "title" } } } }],
  });
  const allowNotes = { format: 1, tables: { notes: { read: "true", insert: "true" } } };
  const allowBefore = testPermissions(beforeApp, allowNotes);
  const allowAfter = testPermissions(afterApp, allowNotes);
  const backend = await startTestJazz({ app: beforeApp, permissions: allowNotes });
  try {
    let written: { id: string; [column: string]: unknown } | undefined;
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
    await backend.redeploy({ app: afterApp, permissions: allowNotes });

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
