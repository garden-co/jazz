import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import {
  createJazz,
  memory,
  sqlite,
  type InspectEvent,
  type InspectGrant,
  type Jazz,
} from "./index.js";
import {
  decodeGrant,
  isLoopbackCallback,
  parseInspectorLink,
  INSPECT_TOKEN_PREFIX,
} from "../inspect/protocol.js";
import { createInspectorUi } from "./inspect-ui.js";
import { requestApproval, tokenStore } from "../inspect/client.js";
import { runInspectCommand } from "../inspect/cli.js";
import { runMcpServer } from "../inspect/mcp.js";

const app = s.defineApp(
  {
    todos: s.table({ title: s.string(), owner: s.uuid(), secretNote: s.string().optional() }, {}),
    notes: s.table({ body: s.string() }, {}),
  },
  { migrations: "none" },
);
const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner: session.user.account });
  policy.todos.allowInsert.where({ owner: session.user.account });
  policy.todos.allowUpdate.never();
  policy.todos.allowDelete.never();
  policy.notes.allowRead.never();
});

const cleanups: Array<() => Promise<unknown>> = [];
const savedNodeEnv = process.env.NODE_ENV;
afterEach(async () => {
  process.env.NODE_ENV = savedNodeEnv;
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

function authenticate(request: Request) {
  const userId = request.headers.get("x-test-user");
  return userId ? { userId } : null;
}

/** The test's inspect hook: staff are named by a header, writers by another. */
function staffOnly(request: Request): InspectGrant | null {
  const staff = request.headers.get("x-staff");
  return staff ? { by: staff, write: request.headers.get("x-staff-write") === "1" } : null;
}

async function start(options: Partial<Parameters<typeof createJazz>[0]> = {}) {
  const events: InspectEvent[] = [];
  const jazz = await createJazz({
    app,
    permissions,
    store: memory(),
    authenticate,
    inspect: staffOnly,
    ...options,
    inspector: { audit: (event) => events.push(event), ...options.inspector },
  });
  cleanups.push(() => jazz.close());
  return { jazz, events };
}

function api(
  jazz: Jazz,
  body: unknown,
  headers: Record<string, string> = {},
  url = "http://localhost:3000/jazz/inspect/api",
): Promise<Response> {
  return jazz.handle(
    new Request(url, {
      method: "POST",
      headers: { "content-type": "application/json", ...headers },
      body: JSON.stringify(body),
    }),
  );
}

/**
 * Serve `jazz` on a real loopback socket through `nodeHandler`, the way a
 * Node host hands Jazz its requests, so the Inspector knows each request's
 * peer address. Returns the origin.
 */
async function serveNode(jazz: Jazz): Promise<string> {
  const server = createServer((req, res) =>
    jazz.nodeHandler(req, res, () => {
      res.statusCode = 404;
      res.end();
    }),
  );
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanups.push(
    () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(resolve);
      }),
  );
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
}

async function ok<T = any>(response: Promise<Response>): Promise<T> {
  const answered = await response;
  const body = await answered.json();
  if (answered.status !== 200) throw new Error(`${answered.status} ${JSON.stringify(body)}`);
  return body as T;
}

async function refused(
  response: Promise<Response>,
): Promise<{ status: number; error: string; message: string }> {
  const answered = await response;
  return { status: answered.status, ...(await answered.json()) };
}

async function accountOf(jazz: Jazz, userId: string): Promise<string> {
  return (await jazz.accountFor(
    new Request("http://app/x", { headers: { "x-test-user": userId } }),
  ))!;
}

/** Rows for alice and bob, written with the backend's authority. */
async function seed(jazz: Jazz) {
  const alice = await accountOf(jazz, "alice");
  const bob = await accountOf(jazz, "bob");
  const a = await jazz.db
    .insert(app.todos, {
      title: "alice's",
      owner: alice,
      secretNote: "a-secret",
    })
    .wait({ tier: "global" });
  const b = await jazz.db
    .insert(app.todos, { title: "bob's", owner: bob, secretNote: "b-secret" })
    .wait({ tier: "global" });
  await jazz.db.insert(app.notes, { body: "hidden from everyone" }).wait({ tier: "global" });
  return { alice, bob, aliceTodo: a.id, bobTodo: b.id };
}

const STAFF = { "x-staff": "ops@example.com" };
const STAFF_WRITER = { "x-staff": "ops@example.com", "x-staff-write": "1" };

describe("Inspector admission", () => {
  it("refuses everyone without a hook outside local development", async () => {
    const { jazz } = await start({ inspect: undefined });
    process.env.NODE_ENV = "production";
    const answer = await refused(api(jazz, { op: "whoami" }));
    expect(answer.status).toBe(401);
    expect(answer.message).toMatch(/needs an `inspect` hook/);
  });

  it("lets this machine in during local development, and nobody through a proxy or another host", async () => {
    const { jazz } = await start({ inspect: undefined });
    const origin = await serveNode(jazz);
    const local = (headers: Record<string, string> = {}) =>
      fetch(`${origin}/jazz/inspect/api`, {
        method: "POST",
        headers: { "content-type": "application/json", ...headers },
        body: JSON.stringify({ op: "whoami" }),
      });
    process.env.NODE_ENV = "development";
    const me = await ok(local());
    expect(me).toMatchObject({ via: "dev", write: true, appId: jazz.appId });
    expect(me.inspectorUrl).toBe(`${origin}/jazz/inspect/`);

    // A proxy in front makes every request look local.
    expect((await refused(local({ "x-forwarded-for": "203.0.113.9" }))).status).toBe(401);
    // Another host name is not this machine.
    expect(
      (await refused(api(jazz, { op: "whoami" }, {}, "http://app.example.com/jazz/inspect/api")))
        .status,
    ).toBe(401);
    // Production never gets the default.
    process.env.NODE_ENV = "production";
    expect((await refused(local())).status).toBe(401);
  });

  it("refuses the dev default where the host can't say who the peer is, whatever Host claims", async () => {
    // `handle(request)` is how Next.js route handlers, Hono, Bun and Deno
    // reach Jazz: no socket, so no peer address. A caller on the network
    // that names `Host: localhost` must not get in (`next dev` listens on
    // every interface).
    const { jazz } = await start({ inspect: undefined });
    process.env.NODE_ENV = "development";
    const answer = await refused(api(jazz, { op: "whoami" }));
    expect(answer.status).toBe(401);
    expect(answer.message).toMatch(/not supported on this host yet/);
    expect(answer.message).toMatch(/Host header proves nothing/);
  });

  it("asks the hook, and `() => null` turns even local development off", async () => {
    const { jazz } = await start({ inspect: () => null });
    process.env.NODE_ENV = "development";
    expect((await refused(api(jazz, { op: "whoami" }))).status).toBe(401);
  });

  it("answers the hook's grant, and refuses cross-site and non-JSON requests", async () => {
    const { jazz, events } = await start();
    expect((await refused(api(jazz, { op: "whoami" }))).status).toBe(401);
    expect(await ok(api(jazz, { op: "whoami" }, STAFF))).toMatchObject({
      by: "ops@example.com",
      via: "hook",
      write: false,
    });
    // A page on another site can't use a staff member's cookies.
    const crossSite = await api(
      jazz,
      { op: "whoami" },
      { ...STAFF, origin: "https://evil.example" },
    );
    expect(crossSite.status).toBe(403);
    // A form post can't be JSON.
    const form = await jazz.handle(
      new Request("http://localhost:3000/jazz/inspect/api", {
        method: "POST",
        headers: { ...STAFF, "content-type": "text/plain" },
        body: JSON.stringify({ op: "whoami" }),
      }),
    );
    expect(form.status).toBe(400);
    expect(events.map((event) => [event.by, event.op, event.error])).toContainEqual([
      "?",
      "whoami",
      "unauthorized",
    ]);
  });
});

describe("Inspector reads", () => {
  it("reads every row with backend authority, and as a user under their permissions", async () => {
    const { jazz, events } = await start();
    const { aliceTodo } = await seed(jazz);
    const all = await ok(
      api(jazz, { op: "query", query: { table: "todos", orderBy: [["title", "asc"]] } }, STAFF),
    );
    expect(all.rows.map((row: any) => row.title)).toEqual(["alice's", "bob's"]);
    expect(all.truncated).toBe(false);
    // Tables nobody may read through permissions are visible too.
    expect(
      (await ok(api(jazz, { op: "query", query: { table: "notes" } }, STAFF))).rows,
    ).toHaveLength(1);

    const asAlice = await ok(
      api(jazz, { op: "query", query: { table: "todos" }, as: "alice" }, STAFF),
    );
    expect(asAlice.rows.map((row: any) => row.id)).toEqual([aliceTodo]);
    expect(
      (await ok(api(jazz, { op: "query", query: { table: "notes" }, as: "alice" }, STAFF))).rows,
    ).toEqual([]);

    // The deep link opens the Inspector on exactly this query, as alice.
    const link = new URL(asAlice.inspectorUrl);
    expect(`${link.origin}${link.pathname}`).toBe("http://localhost:3000/jazz/inspect/");
    expect(parseInspectorLink(link.hash)).toMatchObject({
      query: { table: "todos" },
      as: "alice",
      table: "todos",
    });

    expect(events).toContainEqual(
      expect.objectContaining({
        by: "ops@example.com",
        via: "hook",
        op: "query",
        table: "todos",
        as: "alice",
        rows: 1,
      }),
    );
  });

  it("filters, gets one row with provenance, and reads its history", async () => {
    const { jazz } = await start({
      inspect: (request) => staffOnly(request) && { by: "ops", write: true },
    });
    const { aliceTodo } = await seed(jazz);
    const found = await ok(
      api(
        jazz,
        {
          op: "query",
          query: { table: "todos", where: { title: { contains: "bob" } } },
        },
        STAFF,
      ),
    );
    expect(found.rows.map((row: any) => row.title)).toEqual(["bob's"]);

    const got = await ok(api(jazz, { op: "get", table: "todos", id: aliceTodo }, STAFF));
    expect(got.row).toMatchObject({ id: aliceTodo, title: "alice's" });
    expect(got.row.$createdAt).toEqual(expect.any(String));
    expect(parseInspectorLink(new URL(got.inspectorUrl).hash)).toMatchObject({
      table: "todos",
      row: aliceTodo,
    });
    expect(
      (
        await ok(
          api(
            jazz,
            {
              op: "get",
              table: "todos",
              id: "00000000-0000-0000-0000-000000000001",
            },
            STAFF,
          ),
        )
      ).row,
    ).toBeNull();

    await ok(
      api(
        jazz,
        {
          op: "write",
          ops: [
            {
              kind: "update",
              table: "todos",
              id: aliceTodo,
              values: { title: "renamed" },
            },
          ],
        },
        STAFF,
      ),
    );
    const history = await ok(api(jazz, { op: "history", table: "todos", id: aliceTodo }, STAFF));
    expect(history.versions.map((version: any) => version.title)).toEqual(["alice's", "renamed"]);
  });

  it("caps rows per answer and says so", async () => {
    const { jazz } = await start();
    await seed(jazz);
    const first = await ok(api(jazz, { op: "query", query: { table: "todos", limit: 1 } }, STAFF));
    expect(first.rows).toHaveLength(1);
    expect(first.truncated).toBe(true);
    const bad = await refused(
      api(jazz, { op: "query", query: { table: "todos", limit: 0 } }, STAFF),
    );
    expect(bad).toMatchObject({ status: 400, error: "bad_request" });
  });

  it("names what it can't do", async () => {
    const { jazz } = await start();
    expect(
      await refused(api(jazz, { op: "query", query: { table: "nope" } }, STAFF)),
    ).toMatchObject({ status: 404 });
    expect(
      await refused(api(jazz, { op: "query", query: { table: "todos", includes: {} } }, STAFF)),
    ).toMatchObject({
      status: 400,
      message: expect.stringMatching(/query.includes is not supported/),
    });
    expect(
      await refused(api(jazz, { op: "explain", query: { table: "todos" }, as: "alice" }, STAFF)),
    ).toMatchObject({
      status: 501,
      error: "not_supported",
    });
    expect(await refused(api(jazz, { op: "frobnicate" }, STAFF))).toMatchObject({ status: 400 });
  });

  it("describes the schema, its history tables and permissions", async () => {
    const { jazz } = await start();
    const schema = await ok(api(jazz, { op: "schema" }, STAFF));
    expect(Object.keys(schema.tables).sort()).toEqual(["notes", "todos"]);
    expect(schema.history.sort()).toEqual(["history.notes", "history.todos"]);
    expect(schema.permissions).toBeTruthy();
  });

  it("lists the sessions this instance serves", async () => {
    const { jazz } = await start();
    expect(await ok(api(jazz, { op: "sessions" }, STAFF))).toEqual({
      scope: "this backend instance",
      sessions: [],
    });
  });
});

describe("Inspector UI", () => {
  it("lives at <basePath>/inspect/, and a redirect keeps the deep link's fragment", async () => {
    const { jazz } = await start();
    const bare = await jazz.handle(new Request("http://localhost:3000/jazz/inspect"));
    expect(bare.status).toBe(308);
    expect(bare.headers.get("location")).toBe("/jazz/inspect/");
    const posted = await jazz.handle(
      new Request("http://localhost:3000/jazz/inspect/", { method: "POST" }),
    );
    expect(posted.status).toBe(405);
  });

  it("serves the page unframeable and only the build's assets", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-inspect-ui-"));
    cleanups.push(() => rm(dir, { recursive: true, force: true }));
    await mkdir(join(dir, "assets"));
    await writeFile(join(dir, "backend.html"), "<!doctype html><title>Inspector</title>");
    await writeFile(join(dir, "embedded.html"), "overlay");
    await writeFile(join(dir, "assets", "app-abc.js"), "console.log(1)");
    await writeFile(join(dir, "secret.txt"), "nope");
    const serve = createInspectorUi(dir);
    const request = new Request("http://localhost:3000/jazz/inspect/");

    const page = (await serve(request, "/"))!;
    expect(await page.text()).toContain("Inspector");
    expect(page.headers.get("content-type")).toMatch(/^text\/html/);
    expect(page.headers.get("content-security-policy")).toContain("frame-ancestors 'none'");
    expect(page.headers.get("x-frame-options")).toBe("DENY");
    expect(page.headers.get("referrer-policy")).toBe("no-referrer");
    expect(page.headers.get("cache-control")).toBe("no-store");

    const asset = (await serve(request, "/assets/app-abc.js"))!;
    expect(asset.headers.get("content-type")).toMatch(/^text\/javascript/);
    expect(asset.headers.get("cache-control")).toContain("immutable");

    for (const path of [
      "/embedded.html",
      "/secret.txt",
      "/assets/../secret.txt",
      "/assets/%2e%2e/secret.txt",
      "/assets/missing.js",
      "/assets/app-abc.txt",
      "/%E0%A4%A",
    ]) {
      expect(await serve(request, path), path).toBeNull();
    }
    expect(await createInspectorUi(null)(request, "/")).toBeNull();
  });

  it("hands tokens back by itself only to a loopback callback", () => {
    for (const callback of [
      "http://127.0.0.1:5123/callback",
      "http://localhost:5123/callback",
      "http://[::1]:5123/callback",
    ]) {
      expect(isLoopbackCallback(callback), callback).toBe(true);
    }
    for (const callback of [
      "https://127.0.0.1:5123/callback",
      "http://127.0.0.1/callback",
      "http://127.0.0.1:5123/other",
      "http://127.0.0.1:5123/callback?x=1",
      "http://evil.example.com:5123/callback",
      "http://127.0.0.1.evil.example.com:5123/callback",
      "http://user@127.0.0.1:5123/callback",
      "javascript:alert(1)",
      "not a url",
    ]) {
      expect(isLoopbackCallback(callback), callback).toBe(false);
    }
    expect(decodeGrant("not-base64-json")).toBeNull();
    expect(
      decodeGrant(Buffer.from(JSON.stringify({ state: "s", write: "yes" })).toString("base64url")),
    ).toBeNull();
    expect(
      decodeGrant(
        Buffer.from(JSON.stringify({ state: "s", tables: ["todos"] })).toString("base64url"),
      ),
    ).toEqual({ state: "s", tables: ["todos"] });
  });
});

describe("Inspector writes", () => {
  it("refuses writes unless the grant allows them", async () => {
    const { jazz, events } = await start();
    const answer = await refused(
      api(
        jazz,
        {
          op: "write",
          ops: [{ kind: "insert", table: "notes", values: { body: "x" } }],
        },
        STAFF,
      ),
    );
    expect(answer).toMatchObject({
      status: 403,
      message: expect.stringMatching(/no `write: true`/),
    });
    expect(events.at(-1)).toMatchObject({
      op: "write",
      write: true,
      error: "forbidden",
    });
  });

  it("inserts, updates and deletes in one transaction with backend authority, audited", async () => {
    const { jazz, events } = await start();
    const { bob, bobTodo } = await seed(jazz);
    const done = await ok(
      api(
        jazz,
        {
          op: "write",
          ops: [
            {
              kind: "insert",
              table: "todos",
              values: { title: "added", owner: bob },
            },
            {
              kind: "update",
              table: "todos",
              id: bobTodo,
              values: { title: "fixed" },
            },
          ],
        },
        STAFF_WRITER,
      ),
    );
    expect(done.ids).toHaveLength(2);
    const titles = (await jazz.db.all(app.todos)).map((todo) => todo.title).sort();
    expect(titles).toEqual(["added", "alice's", "fixed"]);
    await ok(
      api(
        jazz,
        { op: "write", ops: [{ kind: "delete", table: "todos", id: bobTodo }] },
        STAFF_WRITER,
      ),
    );
    expect((await jazz.db.all(app.todos)).map((todo) => todo.title).sort()).toEqual([
      "added",
      "alice's",
    ]);
    expect(events.filter((event) => event.op === "write" && !event.error)).toEqual([
      expect.objectContaining({
        by: "ops@example.com",
        write: true,
        table: "todos",
        rows: 2,
      }),
      expect.objectContaining({
        by: "ops@example.com",
        write: true,
        table: "todos",
        rows: 1,
      }),
    ]);
  });

  it("writes as a user under their permissions", async () => {
    const { jazz } = await start();
    const { alice, bob, aliceTodo } = await seed(jazz);
    // alice may insert her own todos but not bob's, and may not update.
    await ok(
      api(
        jazz,
        {
          op: "write",
          ops: [
            {
              kind: "insert",
              table: "todos",
              values: { title: "mine", owner: alice },
            },
          ],
          as: "alice",
        },
        STAFF_WRITER,
      ),
    );
    expect(
      await refused(
        api(
          jazz,
          {
            op: "write",
            ops: [
              {
                kind: "insert",
                table: "todos",
                values: { title: "his", owner: bob },
              },
            ],
            as: "alice",
          },
          STAFF_WRITER,
        ),
      ),
    ).toMatchObject({ status: 403, message: expect.stringMatching(/refused/) });
    expect(
      await refused(
        api(
          jazz,
          {
            op: "write",
            ops: [
              {
                kind: "update",
                table: "todos",
                id: aliceTodo,
                values: { title: "x" },
              },
            ],
            as: "alice",
          },
          STAFF_WRITER,
        ),
      ),
    ).toMatchObject({ status: 403 });
  });

  it("refuses history tables, unknown columns and empty writes", async () => {
    const { jazz } = await start();
    expect(await refused(api(jazz, { op: "write", ops: [] }, STAFF_WRITER))).toMatchObject({
      status: 400,
    });
    expect(
      await refused(
        api(
          jazz,
          {
            op: "write",
            ops: [{ kind: "insert", table: "history.todos", values: {} }],
          },
          STAFF_WRITER,
        ),
      ),
    ).toMatchObject({
      status: 400,
      message: expect.stringMatching(/read-only/),
    });
    expect(
      await refused(
        api(
          jazz,
          {
            op: "write",
            ops: [{ kind: "insert", table: "notes", values: { nope: 1 } }],
          },
          STAFF_WRITER,
        ),
      ),
    ).toMatchObject({
      status: 400,
      message: expect.stringMatching(/no column nope/),
    });
  });
});

describe("agent tokens", () => {
  async function mint(jazz: Jazz, request: Record<string, unknown> = {}, headers = STAFF) {
    return ok<{ token: string; expiresAt: number }>(
      api(jazz, { op: "mint_token", ...request }, headers),
    );
  }
  const bearer = (token: string) => ({ authorization: `Bearer ${token}` });

  it("are minted by a person, read-only by default, and can't mint more", async () => {
    const { jazz, events } = await start();
    await seed(jazz);
    const { token, expiresAt } = await mint(jazz);
    expect(token.startsWith(INSPECT_TOKEN_PREFIX)).toBe(true);
    expect(expiresAt - Date.now() / 1000).toBeGreaterThan(3500);
    const me = await ok(api(jazz, { op: "whoami" }, bearer(token)));
    expect(me).toMatchObject({
      by: "ops@example.com",
      via: "token",
      write: false,
      expiresAt,
    });
    expect(
      (await ok(api(jazz, { op: "query", query: { table: "todos" } }, bearer(token)))).rows,
    ).toHaveLength(2);
    expect(
      await refused(
        api(
          jazz,
          { op: "write", ops: [{ kind: "delete", table: "notes", id: "x" }] },
          bearer(token),
        ),
      ),
    ).toMatchObject({
      status: 403,
      message: expect.stringMatching(/read-only/),
    });
    expect(await refused(api(jazz, { op: "mint_token" }, bearer(token)))).toMatchObject({
      status: 403,
    });
    // The audit log names the token.
    expect(events.find((event) => event.via === "token" && event.op === "query")?.tokenId).toEqual(
      expect.any(String),
    );
  });

  it("allow writes only when the person may write", async () => {
    const { jazz } = await start();
    expect(await refused(api(jazz, { op: "mint_token", write: true }, STAFF))).toMatchObject({
      status: 403,
    });
    const { token } = await mint(jazz, { write: true }, STAFF_WRITER);
    await ok(
      api(
        jazz,
        {
          op: "write",
          ops: [{ kind: "insert", table: "notes", values: { body: "by agent" } }],
        },
        bearer(token),
      ),
    );
  });

  it("can be limited to tables", async () => {
    const { jazz } = await start();
    await seed(jazz);
    const { token } = await mint(jazz, { tables: ["notes"] });
    expect(
      (await ok(api(jazz, { op: "query", query: { table: "notes" } }, bearer(token)))).rows,
    ).toHaveLength(1);
    expect(
      await refused(api(jazz, { op: "query", query: { table: "todos" } }, bearer(token))),
    ).toMatchObject({ status: 403 });
    expect(
      await refused(api(jazz, { op: "history", table: "todos", id: "x" }, bearer(token))),
    ).toMatchObject({ status: 403 });
    expect(Object.keys((await ok(api(jazz, { op: "schema" }, bearer(token)))).tables)).toEqual([
      "notes",
    ]);
    expect(await refused(api(jazz, { op: "mint_token", tables: ["nope"] }, STAFF))).toMatchObject({
      status: 400,
    });
  });

  it("never see redacted columns, nor filter, sort or write by them", async () => {
    const { jazz } = await start({
      inspector: { redact: { todos: ["secretNote"] } },
    });
    await seed(jazz);
    const { token } = await mint(jazz, { write: true }, STAFF_WRITER);
    const rows = (await ok(api(jazz, { op: "query", query: { table: "todos" } }, bearer(token))))
      .rows;
    expect(rows.map((row: any) => row.secretNote)).toEqual(["[redacted]", "[redacted]"]);
    const history = (
      await ok(api(jazz, { op: "history", table: "todos", id: rows[0].id }, bearer(token)))
    ).versions;
    expect(history[0].secretNote).toBe("[redacted]");
    expect(
      await refused(
        api(
          jazz,
          {
            op: "query",
            query: { table: "todos", where: { secretNote: "a-secret" } },
          },
          bearer(token),
        ),
      ),
    ).toMatchObject({
      status: 403,
      message: expect.stringMatching(/redacted/),
    });
    expect(
      await refused(
        api(
          jazz,
          {
            op: "query",
            query: { table: "todos", orderBy: [["secretNote", "asc"]] },
          },
          bearer(token),
        ),
      ),
    ).toMatchObject({ status: 403 });
    expect(
      await refused(
        api(
          jazz,
          {
            op: "write",
            ops: [
              {
                kind: "update",
                table: "todos",
                id: rows[0].id,
                values: { secretNote: "x" },
              },
            ],
          },
          bearer(token),
        ),
      ),
    ).toMatchObject({ status: 403 });
    expect((await ok(api(jazz, { op: "schema" }, bearer(token)))).tables.todos.redacted).toEqual([
      "secretNote",
    ]);
    // People let in by the hook see them.
    const forStaff = (await ok(api(jazz, { op: "query", query: { table: "todos" } }, STAFF))).rows;
    expect(forStaff.map((row: any) => row.secretNote).sort()).toEqual(["a-secret", "b-secret"]);
  });

  it("are refused when tampered with, from another app, or issued before a revocation", async () => {
    const first = await start();
    const { token } = await mint(first.jazz);
    const [payload, mac] = token.slice(INSPECT_TOKEN_PREFIX.length).split(".");
    const forged = Buffer.from(
      JSON.stringify({
        ...JSON.parse(Buffer.from(payload!, "base64url").toString()),
        write: true,
      }),
    ).toString("base64url");
    expect(
      await refused(
        api(first.jazz, { op: "whoami" }, bearer(`${INSPECT_TOKEN_PREFIX}${forged}.${mac}`)),
      ),
    ).toMatchObject({
      status: 401,
      message: expect.stringMatching(/not signed by this backend/),
    });
    // Another backend (another in-memory app) has another key.
    const second = await start();
    expect((await refused(api(second.jazz, { op: "whoami" }, bearer(token)))).status).toBe(401);
    // Revoked: issued before the cut-off.
    const revoked = await start({
      inspector: { tokensIssuedAfter: Date.now() + 60_000 },
    });
    const { token: early } = await mint(revoked.jazz);
    expect(await refused(api(revoked.jazz, { op: "whoami" }, bearer(early)))).toMatchObject({
      status: 401,
      message: expect.stringMatching(/revoked/),
    });
    expect(
      await refused(api(first.jazz, { op: "mint_token", ttlSeconds: 10 * 86400 }, STAFF)),
    ).toMatchObject({ status: 400 });
  });

  it("survive a restart of a SQLite store, and need a secret where the store has none", async () => {
    const dir = await mkdtemp(join(tmpdir(), "jazz-inspect-"));
    cleanups.push(() => rm(dir, { recursive: true, force: true }));
    const once = await start({ store: sqlite({ path: dir }) });
    const { token } = await mint(once.jazz);
    expect(((await stat(join(dir, "inspect-token-secret"))).mode & 0o777).toString(8)).toBe("600");
    await once.jazz.close();
    const again = await start({ store: sqlite({ path: dir }) });
    expect(await ok(api(again.jazz, { op: "whoami" }, bearer(token)))).toMatchObject({
      via: "token",
    });
    // An explicit secret wins, so every instance can share one.
    const shared = await start({
      inspector: { tokenSecret: "shared-secret-of-this-app" },
    });
    expect((await mint(shared.jazz)).token).toBeTruthy();
  });
});

describe("jazz-tools inspect and mcp", () => {
  async function withConfigDir() {
    const dir = await mkdtemp(join(tmpdir(), "jazz-config-"));
    cleanups.push(() => rm(dir, { recursive: true, force: true }));
    return dir;
  }
  /** Routes the CLI's requests to `jazz`, with `headers` added as a person's browser or proxy would. */
  function fetchVia(jazz: Jazz, headers: Record<string, string> = {}): typeof fetch {
    return (async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = new Request(input, init);
      for (const [name, value] of Object.entries(headers)) request.headers.set(name, value);
      return jazz.handle(request);
    }) as typeof fetch;
  }
  /** Routes the CLI's requests to `origin` over a real loopback socket. */
  function fetchAt(origin: string): typeof fetch {
    return (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(new Request(input, init).url);
      return fetch(`${origin}${url.pathname}${url.search}`, init);
    }) as typeof fetch;
  }

  it("runs queries and writes with a stored token, printing JSON", async () => {
    const { jazz } = await start();
    await seed(jazz);
    const config = await withConfigDir();
    const env = {
      JAZZ_CONFIG_DIR: config,
      JAZZ_APP_URL: "http://localhost:3000",
    };
    const { token } = await ok(api(jazz, { op: "mint_token", write: true }, STAFF_WRITER));
    let out = "";
    const io = {
      stdout: (text: string) => (out += text),
      stderr: () => {},
      env,
      fetch: fetchVia(jazz),
    };

    expect(await runInspectCommand(["login", "--token", token], io)).toBe(0);
    expect(JSON.parse(out)).toMatchObject({
      loggedIn: true,
      endpoint: "http://localhost:3000/jazz",
      write: true,
    });
    const stored = JSON.parse(await readFile(join(config, "inspect-tokens.json"), "utf8"));
    expect(stored.tokens["http://localhost:3000/jazz"].token).toBe(token);

    out = "";
    expect(
      await runInspectCommand(["query", '{"table":"todos","where":{"title":"bob\'s"}}'], io),
    ).toBe(0);
    expect(JSON.parse(out).rows.map((row: any) => row.title)).toEqual(["bob's"]);

    out = "";
    expect(await runInspectCommand(["insert", "notes", '{"body":"from the cli"}'], io)).toBe(0);
    const [id] = JSON.parse(out).ids;
    out = "";
    expect(await runInspectCommand(["get", "notes", id], io)).toBe(0);
    expect(JSON.parse(out).row.body).toBe("from the cli");

    out = "";
    expect(await runInspectCommand(["query", "{bad json"], io)).toBe(1);
    expect(JSON.parse(out)).toMatchObject({ error: "bad_request" });

    out = "";
    expect(await runInspectCommand(["logout"], io)).toBe(0);
    out = "";
    expect(await runInspectCommand(["whoami"], io)).toBe(1);
    expect(JSON.parse(out)).toMatchObject({ error: "unauthorized" });
  });

  it("gets a token through the person's approval in the browser, with no file to edit", async () => {
    const { jazz } = await start();
    const config = await withConfigDir();
    const store = tokenStore({ JAZZ_CONFIG_DIR: config });
    const endpoint = "http://localhost:3000/jazz";
    const pending = await requestApproval(
      endpoint,
      { label: "debugging #123" },
      { store, fetch: fetchVia(jazz) },
    );
    const approval = new URL(pending.url);
    expect(`${approval.origin}${approval.pathname}`).toBe("http://localhost:3000/jazz/inspect/");
    const grant = decodeGrant(parseInspectorLink(approval.hash).grant!)!;
    expect(isLoopbackCallback(grant.callback!)).toBe(true);
    expect(grant).toMatchObject({
      label: "debugging #123",
      callback: expect.stringMatching(/^http:\/\/127\.0\.0\.1:\d+\/callback$/),
    });

    // What the Inspector's approval page does once the person approves.
    const { token } = await ok(api(jazz, { op: "mint_token", label: grant.label }, STAFF));
    const wrongState = await fetch(`${grant.callback}?state=guess&token=${token}`);
    expect(wrongState.status).toBe(400);
    const handed = await fetch(`${grant.callback}?state=${grant.state}&token=${token}`);
    expect(handed.status).toBe(200);
    await expect(pending.token).resolves.toMatchObject({
      token,
      by: "ops@example.com",
    });
    expect((await store.get(endpoint))?.token).toBe(token);
  });

  it("serves the same as MCP tools", async () => {
    const { jazz } = await start();
    await seed(jazz);
    process.env.NODE_ENV = "development";
    const { jazz: devJazz } = await start({ inspect: undefined });
    await seed(devJazz);
    const devOrigin = await serveNode(devJazz);
    const config = await withConfigDir();
    const input = new PassThrough();
    const lines: any[] = [];
    const served = runMcpServer({
      app: "http://localhost:3000",
      env: { JAZZ_CONFIG_DIR: config },
      input,
      output: (line) => lines.push(JSON.parse(line)),
      fetch: fetchAt(devOrigin),
    });
    const send = (message: unknown) => input.write(`${JSON.stringify(message)}\n`);
    const answer = async (id: number) => {
      for (let tries = 0; tries < 200; tries += 1) {
        const found = lines.find((line) => line.id === id);
        if (found) return found;
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      throw new Error(`no answer to ${id}`);
    };
    send({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: { protocolVersion: "2025-06-18" },
    });
    expect((await answer(1)).result).toMatchObject({
      protocolVersion: "2025-06-18",
      capabilities: { tools: {} },
    });
    send({ jsonrpc: "2.0", method: "notifications/initialized" });
    send({ jsonrpc: "2.0", id: 2, method: "tools/list" });
    expect((await answer(2)).result.tools.map((tool: any) => tool.name)).toEqual(
      expect.arrayContaining(["whoami", "request_access", "query", "write", "inspector_link"]),
    );
    send({
      jsonrpc: "2.0",
      id: 3,
      method: "tools/call",
      params: { name: "query", arguments: { query: { table: "todos" } } },
    });
    const queried = JSON.parse((await answer(3)).result.content[0].text);
    expect(queried.rows).toHaveLength(2);
    expect(queried.inspectorUrl.startsWith(`${devOrigin}/jazz/inspect/#q=`)).toBe(true);
    send({
      jsonrpc: "2.0",
      id: 4,
      method: "tools/call",
      params: {
        name: "explain",
        arguments: { query: { table: "todos" }, as: "alice" },
      },
    });
    expect((await answer(4)).result).toMatchObject({ isError: true });
    send({
      jsonrpc: "2.0",
      id: 5,
      method: "tools/call",
      params: { name: "request_access", arguments: { reason: "why" } },
    });
    expect(JSON.parse((await answer(5)).result.content[0].text).approveAt).toMatch(
      /\/jazz\/inspect\/#grant=/,
    );
    input.end();
    await served;
  });
});
