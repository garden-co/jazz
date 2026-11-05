import { randomUUID } from "node:crypto";
import { expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createJazzContext } from "../backend/create-jazz-context.js";
import { localFirstAccountId } from "../accounts/local-first.js";
import { deploy } from "../dev/catalogue.js";
import { startLocalJazzServer } from "../testing/index.js";

// Alice's cold backend shares one connection between requests with different
// claims. Each UNION page must retain its duplicate occurrences and its own
// permissions, including the joined arm, narrowed output and an empty result.
it.each([false, true])(
  "keeps scoped UNION pages and claims isolated through NAPI (narrowed: %s)",
  async (narrowed) => {
    const appId = randomUUID();
    const backendSecret = `scoped-union-backend-${appId}`;
    const adminSecret = `scoped-union-admin-${appId}`;
    const app = s.defineApp({
      rooms: s.table({ title: s.string(), code: s.string() }, {}),
      links: s.table(
        { room: s.uuid(), code: s.string() },
        { roomRelation: s.rel("rooms", "room") },
      ),
    });
    const permissions = s.definePermissions(app, ({ policy, session }) => {
      policy.rooms.allowRead.where({ code: session.claims["code"] });
      policy.links.allowRead.where({ code: session.claims["code"] });
    });
    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });
    const contexts: ReturnType<typeof createJazzContext>[] = [];
    try {
      await deploy({ appId, serverUrl: server.url, adminSecret, schema: app, permissions });
      const open = () => {
        const context = createJazzContext({
          appId,
          app,
          permissions,
          driver: { type: "memory" },
          serverUrl: server.url,
          backendSecret,
          env: "test",
        });
        contexts.push(context);
        return context;
      };
      const writer = open().asBackend(app);
      const ids = new Map<string, string>();
      for (const [title, code] of [
        ["aardvark", "a"],
        ["alpha", "a"],
        ["beta", "b"],
        ["gamma", "a"],
      ] as const) {
        const room = await writer.insert(app.rooms, { title, code }).wait({ tier: "global" });
        ids.set(title, room.id);
      }
      const linkIds = new Map<string, string>();
      for (const [title, code] of [
        ["alpha", "a"],
        ["beta", "b"],
        ["gamma", "mallory"],
      ] as const) {
        const link = await writer
          .insert(app.links, { room: ids.get(title)!, code })
          .wait({ tier: "global" });
        linkIds.set(title, link.id);
      }
      const reader = open();
      const base = {
        issuer: "scoped-union-tests",
        user_id: "alice",
        account_id: localFirstAccountId("alice", "scoped-union-tests"),
        authMode: "external" as const,
      };
      const a = reader.forSession({ ...base, claims: { code: "a" } }, app);
      const b = reader.forSession({ ...base, claims: { code: "b" } }, app);
      const nobody = reader.forSession({ ...base, claims: { code: "nobody" } }, app);
      const all = narrowed ? app.rooms.select("title") : app.rooms;
      const linked = narrowed
        ? app.links.hopTo("roomRelation").select("title")
        : app.links.hopTo("roomRelation");
      const query = app.union({ all, linked }).orderBy("title");
      const page = query.offset(2).limit(2);
      for (let round = 0; round < 3; round++) {
        const [rowsA, rowsB, empty] = await Promise.all([
          a.all(page, { tier: "remote" }),
          b.all(query, { tier: "remote" }),
          nobody.all(query, { tier: "remote" }),
        ]);
        expect(rowsA.map(({ id, title }) => [id, title])).toEqual([
          [ids.get("alpha"), "alpha"],
          [ids.get("gamma"), "gamma"],
        ]);
        expect(rowsB.map(({ id, title }) => [id, title])).toEqual([
          [ids.get("beta"), "beta"],
          [ids.get("beta"), "beta"],
        ]);
        expect(empty).toEqual([]);
        if (narrowed) {
          for (const row of [...rowsA, ...rowsB]) expect(row).not.toHaveProperty("code");
        }
      }
      const reversed = app.union({ linked, all }).orderBy("title").offset(2).limit(2);
      expect((await a.all(reversed, { tier: "remote" })).map((row) => row.title)).toEqual([
        "alpha",
        "gamma",
      ]);
      const armWindows = app
        .union({
          first: all.orderBy("title").offset(1).limit(1),
          second: linked.orderBy("title").limit(1),
        })
        .orderBy("title");
      expect((await a.all(armWindows, { tier: "remote" })).map((row) => row.title)).toEqual([
        "alpha",
        "alpha",
      ]);
      const nested = app
        .union({
          wrapped: armWindows.offset(1).limit(1),
          last: all.where({ title: "gamma" }),
        })
        .orderBy("title");
      expect((await a.all(nested, { tier: "remote" })).map((row) => row.title)).toEqual([
        "alpha",
        "gamma",
      ]);
      // Derived labels must distinguish different pages of the same relation.
      const distinctPages = app
        .union([all.orderBy("title").offset(1).limit(1), all.orderBy("title").offset(2).limit(1)])
        .orderBy("title");
      expect((await a.all(distinctPages, { tier: "remote" })).map((row) => row.title)).toEqual([
        "alpha",
        "gamma",
      ]);
      await writer.delete(app.links, linkIds.get("alpha")!).wait({ tier: "global" });
      expect((await a.all(page, { tier: "remote" })).map((row) => row.title)).toEqual(["gamma"]);
    } finally {
      for (const context of contexts.reverse()) await context.shutdown();
      await server.stop();
    }
  },
  30_000,
);
