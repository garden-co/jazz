import { randomBytes, randomUUID } from "node:crypto";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { describe, expect, it, vi } from "vitest";
import { createJazz, createJazzClient, rivet, s3, type BackendClientSession } from "./index.js";
import { schema as s } from "../index.js";
import { readablePermissions } from "../permissions/dsl.js";
import { readableSchema } from "../schema-dsl/define-schema.js";
import { testApp, testPermissions } from "../testing/index.js";
import { rivetTestRunner } from "./fixtures/rivet-test-runner.js";

const schema = s.defineSchema({ notes: s.table({ title: s.string() }, {}) });
const rules = schema.definePermissions({ notes: { read: true, insert: { title: "shared" } } });
const app = testApp({ ...readableSchema(schema)! });
const permissions = { ...readablePermissions(rules)! };

describe("SQLite blob storage", () => {
  const runner = rivetTestRunner();
  it("persists CRUD and commits or rolls back multi-row Jazz transactions", async () => {
    const store = { ...runner, key: randomUUID(), blobs: "sqlite" as const };
    const alice = createJazz({ app, permissions, store: rivet(store) });
    const bob = createJazz({ app, permissions, store: rivet(store) });
    try {
      await alice.ready();
      await bob.ready();
      const read = () => bob.db.all(app.notes.orderBy("title"), { tier: "remote" });
      const first = await alice.db.insert(app.notes, { title: "draft" }).wait({ tier: "global" });
      expect(await read()).toEqual([{ id: first.id, title: "draft" }]);
      await alice.db.update(app.notes, first.id, { title: "published" }).wait({ tier: "global" });
      expect(await read()).toEqual([{ id: first.id, title: "published" }]);
      await alice.db.delete(app.notes, first.id).wait({ tier: "global" });
      expect(await read()).toEqual([]);

      const commit = await alice.db.transaction((tx) => [
        tx.insert(app.notes, { title: "one" }).id,
        tx.insert(app.notes, { title: "two" }).id,
      ]);
      await commit.wait({ tier: "global" });
      const [one, two] = commit.value;
      const committed = [
        { id: one, title: "one" },
        { id: two, title: "two" },
      ];
      expect(await read()).toEqual(committed);

      await expect(
        alice.db.serializableTransaction(async (tx) => {
          expect(await tx.one(app.notes.where({ id: one }))).toMatchObject({ title: "one" });
          tx.update(app.notes, one, { title: "discarded" });
          tx.delete(app.notes, two);
          throw new Error("cancel both changes");
        }),
      ).rejects.toThrow("cancel both changes");
      expect(await read()).toEqual(committed);
    } finally {
      await Promise.all([alice.close(), bob.close()]);
    }
  }, 90_000);

  it("syncs permitted writes through a Node backend and keeps them across actor generations", async () => {
    const key = randomUUID();
    const jazz = createJazz({
      app,
      permissions,
      store: rivet({ ...runner, key, blobs: "sqlite" }),
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
      await expect(async () => {
        const batch = await writer.db.transaction((tx) => {
          tx.insert(app.notes, { title: "shared" });
          tx.insert(app.notes, { title: "denied" });
        });
        await batch.wait({ tier: "global" });
      }).rejects.toThrow();
      expect(await reader.db.all(app.notes, { tier: "remote" })).toHaveLength(1);
      expect(events.mock.calls.some(([event]) => event === "upgrade")).toBe(true);
      for (const session of sessions.splice(0)) await session.close();

      await sleepAndWake(key, runner);
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
  }, 90_000);

  it("requires a blob destination", () => {
    // @ts-expect-error Every Rivet store explicitly selects where chunks live.
    expect(() => rivet({ ...runner, key: "missing-blobs" })).toThrow(/blobs is required/);
  });
});

async function sleepAndWake(
  key: string,
  { endpoint, namespace, token }: ReturnType<typeof rivetTestRunner>,
) {
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
}

describe("S3 blob storage", () => {
  const local = rivetTestRunner({ local: true });
  it("moves SQLite chunks to S3 and reads them after actor sleep", async () => {
    const objects = new Map<string, Buffer>();
    const reads: string[] = [];
    const bucket = createServer(async (req, res) => {
      const path = req.url!;
      if (!req.headers.authorization?.startsWith("AWS4-HMAC-SHA256")) {
        res.writeHead(403).end();
      } else if (req.method === "PUT") {
        const parts = [];
        for await (const chunk of req) parts.push(chunk);
        objects.set(path, Buffer.concat(parts));
        res.writeHead(200).end();
      } else if (req.method === "GET" && objects.has(path)) {
        reads.push(path);
        res.end(objects.get(path));
      } else {
        res.writeHead(404).end();
      }
    });
    await new Promise<void>((resolve) => bucket.listen(0, "127.0.0.1", resolve));
    const key = randomUUID();
    const blobs = s3({
      bucket: "notes",
      region: "auto",
      endpoint: `http://127.0.0.1:${(bucket.address() as AddressInfo).port}`,
    });
    const open = (destination: "sqlite" | typeof blobs) =>
      createJazz({ app, permissions, store: rivet({ ...local, key, blobs: destination }) });
    const value = randomBytes(256 * 1024).toString("base64");
    let jazz = open("sqlite");
    try {
      const row = await jazz.db.insert(app.notes, { title: value }).wait({ tier: "global" });
      await jazz.close();
      jazz = open(blobs);
      await jazz.ready();
      await vi.waitFor(
        () => expect([...objects.keys()].some((path) => !path.endsWith(".jazz-owner"))).toBe(true),
        { timeout: 20_000 },
      );
      await jazz.close();
      await sleepAndWake(key, local);
      jazz = open("sqlite");
      await expect(jazz.ready()).rejects.toThrow(/different blob destination/);
      await jazz.close();
      reads.length = 0;
      jazz = open(blobs);
      expect(await jazz.db.one(app.notes.where({ id: row.id }), { tier: "remote" })).toEqual({
        id: row.id,
        title: value,
      });
      expect(reads.some((path) => !path.endsWith(".jazz-owner"))).toBe(true);
    } finally {
      await jazz.close();
      bucket.closeAllConnections();
      await new Promise<void>((resolve) => bucket.close(() => resolve()));
    }
  }, 90_000);
});
