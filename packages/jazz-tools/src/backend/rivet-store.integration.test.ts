import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { expect, it, vi } from "vitest";
import { createJazz, createJazzClient, rivet, type BackendClientSession } from "./index.js";
import { schema as s } from "../index.js";
import { readablePermissions } from "../permissions/dsl.js";
import { readableSchema } from "../schema-dsl/define-schema.js";
import { testApp, testPermissions } from "../testing/index.js";

// Run against the runner started as described in docs/content/docs/setup/deployment/rivet.mdx.
it.skipIf(!process.env.JAZZ_RIVET_TEST_ENDPOINT)(
  "syncs permitted writes through a Node backend and keeps them across actor generations",
  async () => {
    const schema = s.defineSchema({ notes: s.table({ title: s.string() }, {}) });
    const rules = schema.definePermissions({ notes: { read: true, insert: { title: "shared" } } });
    const app = testApp({ ...readableSchema(schema)! });
    const permissions = { ...readablePermissions(rules)! };

    const endpoint = process.env.JAZZ_RIVET_TEST_ENDPOINT!;
    const namespace = process.env.RIVET_NAMESPACE ?? "default";
    const token = process.env.RIVET_TOKEN ?? "default";
    const key = randomUUID();
    const jazz = createJazz({
      app,
      permissions,
      store: rivet({
        endpoint,
        namespace,
        token,
        key,
        pool: process.env.RIVET_POOL ?? "jazz",
        secret: process.env.JAZZ_CORE_SECRET!,
      }),
      authenticate: () => ({ userId: "alice", expiresAt: Math.floor(Date.now() / 1000) + 3600 }),
    });
    const server = createServer(jazz.nodeHandler);
    const sessions: BackendClientSession[] = [];
    try {
      await jazz.ready();
      await jazz.attach(server);
      const events = vi.spyOn(server, "emit");
      await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
      const serverUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}${jazz.basePath}`;
      const connect = async () => {
        const session = await createJazzClient({
          appId: jazz.appId,
          app,
          permissions: testPermissions(app, permissions),
          serverUrl,
          driver: { type: "memory" },
        });
        sessions.push(session);
        await session.refresh();
        const { client, error } = session.getSnapshot();
        if (!client) throw error ?? new Error("client did not sign in");
        return client;
      };
      const writer = await connect();
      const reader = await connect();
      await writer.db.insert(app.notes, { title: "shared" }).wait({ tier: "global" });
      expect((await reader.db.all(app.notes, { tier: "remote" })).map((row) => row.title)).toEqual([
        "shared",
      ]);
      await expect(
        writer.db.insert(app.notes, { title: "denied" }).wait({ tier: "global" }),
      ).rejects.toThrow();
      expect(events.mock.calls.some(([event]) => event === "upgrade")).toBe(true);
      for (const session of sessions.splice(0)) await session.close();

      const headers = { authorization: `Bearer ${token}` };
      const actors = new URL("/actors", endpoint);
      actors.search = new URLSearchParams({ name: "jazz", key, namespace }).toString();
      const list = await fetch(actors, { headers });
      expect(list.status).toBe(200);
      const [core] = (await list.json()).actors;
      const generation = async () => {
        const response = await fetch(`${endpoint}/gateway/${core.actor_id}/request/app`, {
          headers: { "x-rivet-token": token },
        });
        expect(response.status).toBe(401);
        const value = response.headers.get("x-rivet-actor-generation");
        expect(value).toBeTruthy();
        return Number(value);
      };
      const before = await generation();
      const sleep = await fetch(
        `${endpoint}/actors/${core.actor_id}/sleep?namespace=${encodeURIComponent(namespace)}`,
        { method: "POST", headers },
      );
      expect(sleep.ok).toBe(true);
      await vi.waitFor(async () => expect(await generation()).toBeGreaterThan(before), {
        timeout: 20_000,
      });
      const reopened = await connect();
      expect(
        (await reopened.db.all(app.notes, { tier: "remote" })).map((row) => row.title),
      ).toEqual(["shared"]);
    } finally {
      for (const session of sessions) await session.close();
      await jazz.close();
      server.closeAllConnections();
      if (server.listening) await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  },
  90_000,
);
