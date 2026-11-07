import { afterEach, describe, expect, it } from "vitest";
import { createAccountManager, createDb, type Db } from "jazz-tools";
import { createJazz, remote, type Jazz } from "jazz-tools/backend";
import {
  deploy,
  startLocalJazzServer,
  startTestJazz,
  type LocalJazzServerHandle,
  type TestJazz,
} from "jazz-tools/testing";
import permissions from "../permissions";
import { app } from "../schema";

describe("Better Auth storage boundary", () => {
  let server: LocalJazzServerHandle | undefined;
  let session: Jazz | undefined;
  let front: TestJazz | undefined;
  let client: Db | undefined;

  afterEach(async () => {
    await session?.close();
    await client?.shutdown();
    await front?.stop();
    await server?.stop();
  });

  it("allows backend persistence across reopen while denying client reads and writes", async () => {
    server = await startLocalJazzServer();
    await deploy({
      appId: server.appId,
      serverUrl: server.url,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });

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

    // An ordinary client is a local-first guest, syncing through the app's
    // backend: a core admits only its app's backend.
    front = await startTestJazz({ core: server, app, permissions });
    let storedAccount: string | null = null;
    const accounts = await createAccountManager({
      appId: server.appId,
      serverUrl: front.url,
      store: {
        read: async () => storedAccount,
        update: async (transform) => {
          storedAccount = transform(storedAccount);
        },
      },
    });
    client = await createDb({
      appId: server.appId,
      serverUrl: front.url,
      account: accounts.createLocalFirst(),
      driver: { type: "memory" },
    });
    await expect(client.all(app.better_auth_user, { tier: "remote" })).resolves.toEqual([]);
    await expect(
      client
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
