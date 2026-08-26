import { afterEach, describe, expect, it } from "vitest";
import { createJazzContext, type JazzContext } from "jazz-tools/backend";
import { deploy, startLocalJazzServer, type LocalJazzServerHandle } from "jazz-tools/testing";
import permissions from "../permissions";
import { app } from "../schema";

describe("Better Auth storage boundary", () => {
  let server: LocalJazzServerHandle | undefined;
  let context: JazzContext | undefined;

  afterEach(async () => {
    await context?.shutdown();
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

    const openContext = () =>
      createJazzContext({
        appId: server!.appId,
        app,
        permissions,
        driver: { type: "memory" },
        serverUrl: server!.url,
        backendSecret: server!.backendSecret,
        env: "test",
      });

    context = openContext();
    const stored = await context
      .asBackend(app)
      .insert(app.better_auth_user, {
        name: "Persisted auth user",
        email: "persisted@example.test",
        emailVerified: false,
        createdAt: new Date(0),
        updatedAt: new Date(0),
      })
      .wait({ tier: "edge" });

    const client = context.forSession(
      {
        issuer: "https://auth.example.test",
        user_id: "ordinary-client",
        claims: {},
        authMode: "external",
      },
      app,
    );
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

    await context.shutdown();
    context = openContext();
    await expect
      .poll(
        async () =>
          (await context!.asBackend(app).all(app.better_auth_user, { tier: "global" })).find(
            (row) => row.id === stored.id,
          ),
        { timeout: 10_000 },
      )
      .toEqual(expect.objectContaining({ email: "persisted@example.test" }));
  }, 30_000);
});
