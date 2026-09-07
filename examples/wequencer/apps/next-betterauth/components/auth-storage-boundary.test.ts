import { afterEach, describe, expect, it } from "vitest";
import { createJazzSession } from "jazz-tools/backend";
import { deploy, startLocalJazzServer, type LocalJazzServerHandle } from "jazz-tools/testing";
import permissions from "../permissions";
import { app } from "../schema";

describe("Better Auth storage boundary", () => {
  let server: LocalJazzServerHandle | undefined;
  let session: Awaited<ReturnType<typeof createJazzSession>> | undefined;
  let ordinarySession: Awaited<ReturnType<typeof createJazzSession>> | undefined;

  afterEach(async () => {
    await session?.close();
    await ordinarySession?.close();
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

    const openSession = () =>
      createJazzSession({
        appId: server!.appId,
        app,
        permissions,
        driver: { type: "memory" },
        serverUrl: server!.url,
        initial: { backendSecret: server!.backendSecret },
        env: "test",
      });

    session = await openSession();
    const backend = session.getSnapshot().client!;
    const stored = await backend.db
      .insert(app.better_auth_user, {
        name: "Persisted auth user",
        email: "persisted@example.test",
        emailVerified: false,
        createdAt: new Date(0),
        updatedAt: new Date(0),
      })
      .wait({ tier: "edge" });

    ordinarySession = await createJazzSession({
      appId: server.appId,
      app,
      permissions,
      driver: { type: "memory" },
      serverUrl: server.url,
      env: "test-client",
      initial: "local-first",
    });
    const client = ordinarySession.getSnapshot().client!.db;
    await expect(client.all(app.better_auth_user, { tier: "edge" })).resolves.toEqual([]);
    await expect(
      client
        .insert(app.better_auth_user, {
          name: "Forbidden client user",
          email: "forbidden@example.test",
          emailVerified: false,
          createdAt: new Date(0),
          updatedAt: new Date(0),
        })
        .wait({ tier: "edge" }),
    ).rejects.toThrow(/AuthorizationDenied|Write rejected by server authorization/);

    await session.close();
    session = await openSession();
    await expect
      .poll(
        async () =>
          (
            await session!.getSnapshot().client!.db.all(app.better_auth_user, { tier: "global" })
          ).find((row) => row.id === stored.id),
        { timeout: 10_000 },
      )
      .toEqual(expect.objectContaining({ email: "persisted@example.test" }));
  }, 30_000);
});
