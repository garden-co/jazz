import { afterEach, describe, expect, it } from "vitest";
import { createJazz, createJazzClient, remote, type Jazz } from "jazz-tools/backend";
import {
  startLocalJazzServer,
  startTestJazz,
  type LocalJazzServerHandle,
  type TestJazz,
} from "jazz-tools/testing";
import permissions from "../permissions";
import { app } from "../schema";
import { memoryAccountStore } from "../../../../testing/accounts";

describe("Better Auth storage boundary", () => {
  let server: LocalJazzServerHandle | undefined;
  let session: Jazz | undefined;
  let front: TestJazz | undefined;
  let client: Awaited<ReturnType<typeof createJazzClient>> | undefined;

  afterEach(async () => {
    await session?.close();
    await client?.close({ timeoutMs: 0 });
    await front?.stop();
    await server?.stop();
  });

  it("allows backend persistence across reopen while denying client reads and writes", async () => {
    // Published as the app's createJazz publishes it, with the migrations
    // it ships.
    server = await startLocalJazzServer({ schema: app, permissions });

    // The app's backend, as its server runs it.
    const openSession = () =>
      createJazz({
        app,
        permissions,
        store: remote({ url: server!.url, appId: server!.appId, secret: server!.adminSecret }),
      });

    session = await openSession();
    const stored = await session.db
      .insert(app.better_auth_user, {
        name: "Persisted auth user",
        email: "persisted@example.test",
        emailVerified: false,
        createdAt: new Date(0),
        updatedAt: new Date(0),
      })
      .wait({ tier: "global" });

    // An ordinary client is a device account (a guest), syncing through the app's
    // backend: a core admits only its app's backend.
    front = await startTestJazz({ core: server, app, permissions });
    client = await createJazzClient({
      app,
      permissions,
      serverUrl: front.url,
      initial: "device",
      store: memoryAccountStore(),
      driver: { type: "memory" },
    });
    await expect(client.db.all(app.better_auth_user, { tier: "remote" })).resolves.toEqual([]);
    await expect(
      client.db
        .insert(app.better_auth_user, {
          name: "Forbidden client user",
          email: "forbidden@example.test",
          emailVerified: false,
          createdAt: new Date(0),
          updatedAt: new Date(0),
        })
        .wait({ tier: "global" }),
    ).rejects.toThrow(/AuthorizationDenied|Write rejected by server authorization/);

    await session.close();
    session = await openSession();
    await expect
      .poll(
        async () =>
          (await session!.db.all(app.better_auth_user, { tier: "remote" })).find(
            (row) => row.id === stored.id,
          ),
        { timeout: 10_000 },
      )
      .toEqual(expect.objectContaining({ email: "persisted@example.test" }));
  }, 30_000);
});
