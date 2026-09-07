import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { resolveSchemaSource } from "../schema-source.js";
import { createJazzSession } from "./index.js";

const app = s.defineApp({ notes: s.table({ text: s.string() }) });
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.always();
  policy.notes.allowInsert.never();
  policy.notes.allowUpdate.never();
  policy.notes.allowDelete.never();
});

describe("Node shared backend session", () => {
  it("rejects invalid service admission before publishing a client", async () => {
    const appId = randomUUID();
    const server = await startLocalJazzServer({ appId, backendSecret: "expected-service-secret" });
    try {
      await expect(
        createJazzSession({
          appId,
          serverUrl: server.url,
          app,
          driver: { type: "memory" },
          initial: { backendSecret: "incorrect-service-secret" },
        }),
      ).rejects.toThrow("Backend admission failed (401)");
    } finally {
      await server.stop();
    }
  });

  it("preserves SYSTEM provenance, drops authority on transition, and isolates user scopes", async () => {
    const appId = randomUUID();
    const backendSecret = "session-service-secret";
    const adminSecret = "session-publication-secret";
    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });
    const session = await createJazzSession({
      appId,
      serverUrl: server.url,
      app,
      permissions,
      driver: { type: "memory" },
    });
    try {
      await deploy({
        serverUrl: server.url,
        appId,
        adminSecret,
        schema: resolveSchemaSource(app),
        permissions,
      });
      await session.becomeBackend({ backendSecret });
      const first = session.getSnapshot();
      expect(first.status).toBe("ready");
      expect(first.account?.id).toBe("00000000-0000-0000-0000-000000000000");
      expect(first.account?.identity.issuer).toBe("urn:jazz:system");
      expect(JSON.stringify(first.account)).not.toContain(backendSecret);
      const backend = first.client!;
      expect(backend.session?.user).toEqual({
        account: first.account!.id,
        identity: first.account!.identity,
      });
      const initial = await backend.db
        .insert(app.notes, { text: "service" })
        .wait({ tier: "edge" });
      expect(
        await backend.db.one(app.notes.select("$createdBy").where({ id: initial.id })),
      ).toMatchObject({ $createdBy: backend.session?.user });

      await session.createLocalFirst();
      const user = session.getSnapshot();
      await expect(backend.forAccount(user.account!)).rejects.toThrow(/closed|shut down/);
      expect(() => backend.db.insert(app.notes, { text: "stale service" })).toThrow(
        /closed|shut down/,
      );
      await expect(user.client!.forRequest({ headers: {} })).rejects.toThrow("backend account");
      await expect(async () => {
        await user.client!.db.insert(app.notes, { text: "denied user" }).wait({ tier: "edge" });
      }).rejects.toThrow();

      await session.becomeBackend({ backendSecret });
      const current = session.getSnapshot().client!;
      const scoped = await current.forAccount(user.account!);
      await expect(async () => {
        await scoped.insert(app.notes, { text: "denied scoped user" }).wait({ tier: "edge" });
      }).rejects.toThrow();
      const attributed = await current.withAttribution(user.account!);
      const row = await attributed
        .insert(app.notes, { text: "attributed service" })
        .wait({ tier: "edge" });
      expect(
        await current.db.one(app.notes.select("$createdBy").where({ id: row.id })),
      ).toMatchObject({
        $createdBy: { account: user.account!.id, identity: user.account!.identity },
      });
      expect(session.getSnapshot().account?.identity.issuer).toBe("urn:jazz:system");
      await session.close();
      expect(() => scoped.insert(app.notes, { text: "closed scope" })).toThrow(/closed|shut down/);
      await expect(current.forAccount(user.account!)).rejects.toThrow(/closed|shut down/);
    } finally {
      await session.close();
      await server.stop();
    }
  }, 30_000);
});
