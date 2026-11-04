import { expect, it } from "vitest";
import { schema } from "../../src/schema-namespace.js";
import { ReadTier } from "../../src/runtime/client.js";
import * as nativeCommands from "jazz-rn/relay";
import {
  NativeForegroundDb,
  type NativeForegroundModule,
} from "../../src/react-native/native-foreground-db.js";
import { encodeCellsForRow } from "../../src/runtime/native-runtime/native-runtime-adapter.js";
import { startTestJazz } from "../../src/testing/index.js";
import { withNativeRelayFixture } from "./fixture.js";

const app = schema.defineApp(
  { todos: schema.table({ title: schema.string() }, {}) },
  { migrations: "none" },
);
const permissions = schema.definePermissions(app, ({ policy }) => [
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
]);

// A signed-in native user syncs through the app's backend with the app's own
// bearer, as the backend's `authenticate` reads it; the core admits only the
// backend.
it("a signed-in user's write reaches the server through the app's backend and survives a relay restart", async () => {
  const backend = await startTestJazz({ app, permissions });
  try {
    let written: { id: string; title: string } | undefined;
    await withNativeRelayFixture(
      app,
      permissions,
      async (fixture) => {
        const db = await fixture.createDb();
        written = await db.insert(app.todos, { title: "reaches the server" }).wait({
          tier: "global",
        });
        await db.shutdown();

        // A fresh database over the same native store reads it back.
        const reopened = await fixture.createDb();
        expect(await reopened.all(app.todos, { tier: ReadTier.LocalFirst })).toEqual([written]);
      },
      { appId: backend.appId, upstream: { backend, userId: "native-alice" } },
    );
    // Another signed-in user sees it at the server.
    await withNativeRelayFixture(
      app,
      permissions,
      async (fixture) => {
        const db = await fixture.createDb();
        expect(await db.all(app.todos, { tier: ReadTier.Remote })).toEqual([written]);
      },
      { appId: backend.appId, upstream: { backend, userId: "native-bob" } },
    );
  } finally {
    await backend.stop();
  }
}, 60_000);

it("counts offline writes as pending until the app's backend accepts them", async () => {
  const backend = await startTestJazz({ app, permissions });
  try {
    await withNativeRelayFixture(
      app,
      permissions,
      async (fixture) => {
        const observer = await fixture.createDb();
        // Warm the upstream: the session is admitted and in sync.
        expect(await observer.all(app.todos, { tier: ReadTier.Remote })).toEqual([]);
        const native = new NativeForegroundDb(
          fixture.nativeHost.openAttached(fixture.capability),
          nativeCommands as unknown as NativeForegroundModule,
        );
        try {
          expect(native.pendingUploadCount()).toBe(0);
          native.disconnectNativeUpstream();
          for (const title of ["offline one", "offline two"]) {
            await native
              .insert(
                "todos",
                encodeCellsForRow(app.wasmSchema.todos!, { title: { type: "Text", value: title } }),
              )
              .wait("local");
          }
          await expect.poll(() => native.pendingUploadCount()).toBe(2);

          native.reconnectNativeUpstream();
          await expect.poll(() => native.pendingUploadCount(), { timeout: 15_000 }).toBe(0);
          await expect
            .poll(async () => (await observer.all(app.todos, { tier: ReadTier.Remote })).length, {
              timeout: 15_000,
            })
            .toBe(2);
        } finally {
          native.close();
        }
      },
      { appId: backend.appId, upstream: { backend, userId: "native-pending" } },
    );
  } finally {
    await backend.stop();
  }
}, 60_000);
