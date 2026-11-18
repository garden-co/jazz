// The Jazz core on a Durable Object, in local workerd (Miniflare), spoken to
// over a WebSocket the way the app's backend speaks to a core:
//
//   no credential ──prelude+Hello──► refused (admits only its app's backend)
//   backend ──prelude+Hello──► Hello ──schema──► ok ──insert──► written ──query──► ["first"]
//   restart (same storage) ──backend──► query ──► ["first"]
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { Miniflare } from "miniflare";
import * as fixture from "./pkg/jazz_wasm.js";

const here = fileURLToPath(new URL(".", import.meta.url));
fixture.initSync({ module: readFileSync(join(here, "pkg/jazz_wasm_bg.wasm")) });

// Any non-empty segment names the object's one app.
const APP = "my-app";
const SECRET = "s".repeat(40);
const SCHEMA = fixture.testNotesSchema();

function start(persist) {
  return new Miniflare({
    modules: true,
    scriptPath: join(here, "host-worker.mjs"),
    modulesRoot: join(here, "../.."),
    modulesRules: [
      { type: "ESModule", include: ["**/*.js", "**/*.mjs"] },
      { type: "CompiledWasm", include: ["**/*.wasm"] },
    ],
    compatibilityDate: "2026-04-01",
    durableObjects: { JAZZ: { className: "JazzCore", useSQLite: true } },
    durableObjectsPersist: persist,
    bindings: { JAZZ_CORE_SECRET: SECRET, JAZZ_SCHEMA: SCHEMA },
  });
}

/** A socket to the core, its answers queued as they arrive. */
async function connect(mf, headers = {}) {
  const response = await mf.dispatchFetch(`http://core/apps/${APP}/ws`, {
    headers: { Upgrade: "websocket", ...headers },
  });
  assert.equal(response.status, 101);
  const ws = response.webSocket;
  const answers = [];
  const waiting = [];
  let closed = false;
  ws.addEventListener("message", (event) => {
    for (const answer of JSON.parse(fixture.testDecodeAnswers(new Uint8Array(event.data)))) {
      (waiting.shift() ?? ((a) => answers.push(a)))(answer);
    }
  });
  let closeCode = null;
  ws.addEventListener("close", (event) => {
    closed = true;
    closeCode = event.code;
  });
  ws.accept();
  return {
    send: (message) => ws.send(message),
    next: () =>
      answers.length
        ? Promise.resolve(answers.shift())
        : new Promise((resolve, reject) => {
            waiting.push(resolve);
            setTimeout(() => reject(new Error("no answer within 5 s")), 5000);
          }),
    closed: () => closed,
    closeCode: () => closeCode,
    close: () => ws.close(),
  };
}

const backend = { "X-Jazz-Backend-Credential": SECRET, "X-Jazz-Backend-Access": "backend" };
const prelude = JSON.stringify({ peer_identity: fixture.testSystemIdentity(), auth: {} });

test("a Durable Object core without a published schema answers health and admits its backend", async () => {
  const mf = new Miniflare({
    modules: true,
    scriptPath: join(here, "host-worker.mjs"),
    modulesRoot: join(here, "../.."),
    modulesRules: [
      { type: "ESModule", include: ["**/*.js", "**/*.mjs"] },
      { type: "CompiledWasm", include: ["**/*.wasm"] },
    ],
    compatibilityDate: "2026-04-01",
    durableObjects: { JAZZ: { className: "JazzCore", useSQLite: true } },
    // As the docs deploy it: no JAZZ_SCHEMA.
    bindings: { JAZZ_CORE_SECRET: SECRET },
  });
  try {
    assert.equal(await (await mf.dispatchFetch("http://core/health")).text(), "ok");
    const admit = (headers) =>
      mf.dispatchFetch(`http://core/apps/${APP}/backend/admit`, { method: "POST", headers });
    assert.equal((await admit({ "X-Jazz-Backend-Secret": SECRET })).status, 204);
    const wrong = await admit({ "X-Jazz-Backend-Secret": "w".repeat(40) });
    assert.equal(wrong.status, 401);
    assert.equal((await wrong.json()).error, "Invalid backend secret");
    assert.equal((await admit({})).status, 401);
    // The object's own app id, for the backend's credential alone.
    const app = await mf.dispatchFetch("http://core/app", {
      headers: { "X-Jazz-Backend-Credential": SECRET },
    });
    assert.equal(app.status, 200);
    const { appId } = await app.json();
    assert.match(appId, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    assert.equal((await mf.dispatchFetch("http://core/app")).status, 401);
    const stranger = await mf.dispatchFetch("http://core/app", {
      headers: { "X-Jazz-Backend-Credential": "w".repeat(40) },
    });
    assert.equal(stranger.status, 401);
    // Its app answers under its id as under any other name.
    const byId = await mf.dispatchFetch(`http://core/apps/${appId}/backend/admit`, {
      method: "POST",
      headers: { "X-Jazz-Backend-Secret": SECRET },
    });
    assert.equal(byId.status, 204);
    assert.equal(
      (await mf.dispatchFetch("http://core/apps//backend/admit", { method: "POST" })).status,
      404,
    );
    // A body is read only once the route and secret hold, and at most 8 MiB.
    const big = new Uint8Array(9 * 1024 * 1024);
    const unread = await mf.dispatchFetch(`http://core/apps/${APP}/admin/bundles`, {
      method: "POST",
      headers: { "X-Jazz-Admin-Secret": SECRET },
      body: big,
    });
    assert.equal(unread.status, 413);
    const graph = await mf.dispatchFetch(`http://core/apps/${APP}/admin/migrations/graph`);
    assert.equal(graph.status, 401, "admin routes need the admin secret");

    // Until a schema is published, a backend's socket is told to wait.
    const early = await connect(mf, backend);
    early.send(prelude);
    early.send(fixture.testThinHello());
    assert.match((await early.next()).error, /no schema has been published/);
  } finally {
    await mf.dispose();
  }
});

test("a Durable Object core serves its app's backend and refuses anyone else", async () => {
  const persist = mkdtempSync(join(tmpdir(), "jazz-do-core-"));
  try {
    let mf = start(persist);
    assert.equal(await (await mf.dispatchFetch("http://core/health")).text(), "ok");
    assert.equal((await mf.dispatchFetch(`http://core/apps/${APP}/ws`)).status, 426);

    const stranger = await connect(mf);
    stranger.send(prelude);
    stranger.send(fixture.testThinHello());
    assert.match((await stranger.next()).error, /admits only its app's backend/);

    const ours = await connect(mf, backend);
    ours.send(prelude);
    ours.send(fixture.testThinHello());
    assert.deepEqual(await ours.next(), { hello: true });
    ours.send(fixture.testThinSchema(1n, SCHEMA));
    assert.deepEqual(await ours.next(), { id: 1, ok: true });
    ours.send(fixture.testThinInsertNote(2n, "first"));
    assert.deepEqual(await ours.next(), { id: 2, written: true });
    ours.send(fixture.testThinQueryNotes(3n));
    assert.deepEqual(await ours.next(), { id: 3, titles: ["first"] });
    ours.close();
    await mf.dispose();

    // The object's next instance: the same storage, a fresh core.
    mf = start(persist);
    const again = await connect(mf, backend);
    again.send(prelude);
    again.send(fixture.testThinHello());
    assert.deepEqual(await again.next(), { hello: true });
    again.send(fixture.testThinSchema(1n, SCHEMA));
    assert.deepEqual(await again.next(), { id: 1, ok: true });
    again.send(fixture.testThinQueryNotes(2n));
    assert.deepEqual(await again.next(), { id: 2, titles: ["first"] });
    again.close();
    await mf.dispose();
  } finally {
    rmSync(persist, { recursive: true, force: true });
  }
});

test("a backend publishes its schema to a Durable Object core, which then serves it", async () => {
  const persist = mkdtempSync(join(tmpdir(), "jazz-do-publish-"));
  // As the docs deploy it: no JAZZ_SCHEMA; the backend publishes.
  const open = () =>
    new Miniflare({
      modules: true,
      scriptPath: join(here, "host-worker.mjs"),
      modulesRoot: join(here, "../.."),
      modulesRules: [
        { type: "ESModule", include: ["**/*.js", "**/*.mjs"] },
        { type: "CompiledWasm", include: ["**/*.wasm"] },
      ],
      compatibilityDate: "2026-04-01",
      durableObjects: { JAZZ: { className: "JazzCore", useSQLite: true } },
      durableObjectsPersist: persist,
      bindings: { JAZZ_CORE_SECRET: SECRET },
    });
  const admin = { "X-Jazz-Admin-Secret": SECRET };
  const get = (mf, route) =>
    mf.dispatchFetch(`http://core/apps/${APP}/${route}`, { headers: admin });
  const publish = (mf, body) =>
    mf.dispatchFetch(`http://core/apps/${APP}/admin/bundles`, {
      method: "POST",
      headers: { ...admin, "content-type": "application/json" },
      body,
    });
  try {
    let mf = open();
    const appId = async () =>
      (
        await (
          await mf.dispatchFetch("http://core/app", {
            headers: { "X-Jazz-Backend-Credential": SECRET, "X-Jazz-Backend-Access": "backend" },
          })
        ).json()
      ).appId;
    const firstId = await appId();
    const empty = await get(mf, "admin/migrations/graph");
    assert.equal(empty.status, 200);
    assert.deepEqual((await (await get(mf, "admin/bundles")).json()).bundles, []);

    const malformed = await publish(mf, "{");
    assert.equal(malformed.status, 400);

    const request = fixture.testDeployRequest(SCHEMA);
    const first = await publish(mf, request);
    assert.equal(first.status, 200);
    const published = await first.json();
    assert.equal(published.changed, true);
    const again = await (await publish(mf, request)).json();
    assert.equal(again.changed, false, "publishing the same bundle again changes nothing");
    assert.equal(again.bundle, published.bundle);
    const bundles = (await (await get(mf, "admin/bundles")).json()).bundles;
    assert.deepEqual(
      bundles.map((bundle) => bundle.bundle),
      [published.bundle],
    );
    const graph = await (await get(mf, "admin/migrations/graph")).json();
    assert.ok(JSON.stringify(graph).includes(bundles[0].schemaHash), JSON.stringify(graph));

    const ours = await connect(mf, backend);
    ours.send(prelude);
    ours.send(fixture.testThinHello());
    assert.deepEqual(await ours.next(), { hello: true });
    ours.send(fixture.testThinSchema(1n, SCHEMA));
    assert.deepEqual(await ours.next(), { id: 1, ok: true });
    ours.send(fixture.testThinInsertNote(2n, "published"));
    assert.deepEqual(await ours.next(), { id: 2, written: true });
    ours.close();
    await mf.dispose();

    // The next instance opens on what was published.
    mf = open();
    assert.equal(await appId(), firstId, "the object keeps its app id");
    assert.equal((await (await publish(mf, request)).json()).changed, false);
    const later = await connect(mf, backend);
    later.send(prelude);
    later.send(fixture.testThinHello());
    assert.deepEqual(await later.next(), { hello: true });
    later.send(fixture.testThinSchema(1n, SCHEMA));
    assert.deepEqual(await later.next(), { id: 1, ok: true });
    later.send(fixture.testThinQueryNotes(2n));
    assert.deepEqual(await later.next(), { id: 2, titles: ["published"] });
    later.close();
    await mf.dispose();
  } finally {
    rmSync(persist, { recursive: true, force: true });
  }
});

/** Until `check()` holds, or fail after 5 s. */
async function eventually(check, what) {
  for (let waited = 0; !check(); waited += 20) {
    if (waited > 5000) throw new Error(`not within 5 s: ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

/** An admin request to a test-only route (`durable-object-tests` builds). */
function testRoute(mf, route, body) {
  return mf.dispatchFetch(`http://core/apps/${APP}/test/${route}`, {
    method: "POST",
    headers: { "X-Jazz-Admin-Secret": SECRET, "content-type": "application/json" },
    body: JSON.stringify(body ?? {}),
  });
}

/** A backend's thin connection, past its Hello and schema. */
async function ready(mf) {
  const socket = await connect(mf, backend);
  socket.send(prelude);
  socket.send(fixture.testThinHello());
  assert.deepEqual(await socket.next(), { hello: true });
  socket.send(fixture.testThinSchema(1n, SCHEMA));
  assert.deepEqual(await socket.next(), { id: 1, ok: true });
  return socket;
}

/** Send `bytes` of inserts (64 KiB titles) from id `from` on. */
function flood(socket, bytes, from = 100) {
  const title = "x".repeat(64 * 1024);
  let id = BigInt(from);
  for (let sent = 0; sent < bytes; sent += title.length) {
    try {
      socket.send(fixture.testThinInsertNote(id++, title));
    } catch {
      return;
    }
  }
}

const MiB = 1024 * 1024;

test("a Durable Object core ends a socket that sends more than it can hold, and serves the others", async () => {
  const mf = start(mkdtempSync(join(tmpdir(), "jazz-do-held-")));
  try {
    const flooding = await ready(mf);
    const other = await ready(mf);
    // The engine takes nothing: up to 1 MiB is held, then the socket is shed.
    const limits = await testRoute(mf, "limits", { socket: MiB, global: 64 * MiB, paused: true });
    assert.equal(limits.status, 200);
    assert.deepEqual(
      await limits.json(),
      { socket: 8 * MiB, global: 32 * MiB },
      "the default limits",
    );
    flood(flooding, 2 * MiB);
    const shed = await flooding.next();
    assert.match(shed.error ?? "", /reconnect later/, JSON.stringify(shed));
    await eventually(() => flooding.closed(), "the flooding socket closes");
    assert.equal(other.closed(), false);

    await testRoute(mf, "limits", { socket: 8 * MiB, global: 32 * MiB, paused: false });
    other.send(fixture.testThinInsertNote(2n, "still served"));
    assert.deepEqual(await other.next(), { id: 2, written: true });
    other.close();
  } finally {
    await mf.dispose();
  }
});

test("a Durable Object core sheds the socket holding most when all together hold too much", async () => {
  const mf = start(mkdtempSync(join(tmpdir(), "jazz-do-global-")));
  try {
    const modest = await ready(mf);
    const greedy = await ready(mf);
    await testRoute(mf, "limits", { socket: 3 * MiB, global: 4 * MiB, paused: true });
    // Modest holds 1.5 MiB; greedy passes the 4 MiB both may hold together
    // while still under its own 3.
    flood(modest, 1.5 * MiB);
    await new Promise((resolve) => setTimeout(resolve, 500));
    flood(greedy, 2.9 * MiB);
    const shed = await greedy.next();
    assert.match(shed.error ?? "", /reconnect later/, JSON.stringify(shed));
    await eventually(() => greedy.closed(), "the greedy socket closes");

    await testRoute(mf, "limits", { socket: 8 * MiB, global: 32 * MiB, paused: false });
    const answer = await modest.next();
    assert.equal(answer.written, true, JSON.stringify(answer));
    assert.equal(modest.closed(), false);
    modest.close();
  } finally {
    await mf.dispose();
  }
});

test("a malformed message ends its own socket alone", async () => {
  const mf = start(mkdtempSync(join(tmpdir(), "jazz-do-malformed-")));
  try {
    const broken = await ready(mf);
    const other = await ready(mf);
    broken.send(new Uint8Array([0xff, 0x00, 0x13, 0x37]));
    const error = await broken.next();
    assert.ok(error.error, JSON.stringify(error));
    await eventually(() => broken.closed(), "the malformed socket closes");
    other.send(fixture.testThinInsertNote(2n, "still served"));
    assert.deepEqual(await other.next(), { id: 2, written: true });
    other.close();
  } finally {
    await mf.dispose();
  }
});

test("after a panic the object starts afresh on its storage", async () => {
  const mf = start(mkdtempSync(join(tmpdir(), "jazz-do-panic-")));
  try {
    const before = await ready(mf);
    before.send(fixture.testThinInsertNote(2n, "before the panic"));
    assert.deepEqual(await before.next(), { id: 2, written: true });

    const panicked = await testRoute(mf, "panic").catch((error) => error);
    assert.ok(panicked instanceof Error || panicked.status >= 500, String(panicked?.status));
    await eventually(() => before.closed(), "sockets close after the panic");
    assert.equal(before.closeCode(), 1011);

    // The write the panic interrupted, inside the object's storage
    // transaction, did not stay.
    const rows = await testRoute(mf, "panic-rows");
    assert.deepEqual(await rows.json(), { rows: 0 });

    // A fresh core, on a fresh wasm instance, over the same storage.
    const after = await ready(mf);
    after.send(fixture.testThinQueryNotes(2n));
    assert.deepEqual(await after.next(), { id: 2, titles: ["before the panic"] });
    after.close();
    assert.equal(await (await mf.dispatchFetch("http://core/health")).text(), "ok");
  } finally {
    await mf.dispose();
  }
});
test("a Durable Object core keeps its app's accounts", async () => {
  const persist = mkdtempSync(join(tmpdir(), "jazz-do-accounts-"));
  const open = () =>
    new Miniflare({
      modules: true,
      scriptPath: join(here, "host-worker.mjs"),
      modulesRoot: join(here, "../.."),
      modulesRules: [
        { type: "ESModule", include: ["**/*.js", "**/*.mjs"] },
        { type: "CompiledWasm", include: ["**/*.wasm"] },
      ],
      compatibilityDate: "2026-04-01",
      durableObjects: { JAZZ: { className: "JazzCore", useSQLite: true } },
      durableObjectsPersist: persist,
      bindings: { JAZZ_CORE_SECRET: SECRET },
    });
  const post = (mf, path, headers, body) =>
    mf.dispatchFetch(`http://core/apps/${APP}/admin/accounts/${path}`, {
      method: "POST",
      headers: { "content-type": "application/json", ...headers },
      body: JSON.stringify(body),
    });
  const asBackend = { "X-Jazz-Backend-Secret": SECRET };
  const asAdmin = { "X-Jazz-Admin-Secret": SECRET };
  const alice = { identity: { issuer: "https://idp.example", subject: "alice" } };
  let mf;
  try {
    mf = open();
    // A backend vouches for its users: it registers and resolves them.
    const registered = await post(mf, "login-or-register", asBackend, alice);
    assert.equal(registered.status, 200);
    const account = (await registered.json()).account;
    const resolved = await post(mf, "resolve", asBackend, alice);
    assert.equal((await resolved.json()).account, account);
    // Refusals are sent without a body: the secret is checked first, and
    // Node's fetch cannot answer a 401 to a request that streamed one.
    const refused = (path, headers) =>
      mf.dispatchFetch(`http://core/apps/${APP}/admin/accounts/${path}`, {
        method: "POST",
        headers,
      });
    // A wrong admin secret is not excused by a right backend one.
    const wrong = await refused("resolve", { ...asBackend, "X-Jazz-Admin-Secret": "w".repeat(40) });
    assert.equal(wrong.status, 401);
    // Revoking sessions is the admin's.
    assert.equal((await refused("revoke-sessions", asBackend)).status, 401);
    const asked = { ...alice, revokedBefore: 1000 };
    assert.equal((await post(mf, "revoke-sessions", asAdmin, asked)).status, 200);
    const unknown = await post(mf, "nonsense", asAdmin, alice);
    assert.equal(unknown.status, 404);
    const malformed = await mf.dispatchFetch(`http://core/apps/${APP}/admin/accounts/resolve`, {
      method: "POST",
      headers: asBackend,
      body: "{",
    });
    assert.equal(malformed.status, 400);
    await mf.dispose();

    // The next instance knows alice's account.
    mf = open();
    const again = await post(mf, "resolve", asBackend, alice);
    assert.equal((await again.json()).account, account);
    assert.equal(await (await mf.dispatchFetch("http://core/health")).text(), "ok");
  } finally {
    await mf?.dispose();
    rmSync(persist, { recursive: true, force: true });
  }
});

test("each account route takes the secrets native's does", async () => {
  const mf = start(mkdtempSync(join(tmpdir(), "jazz-do-account-auth-")));
  const wrong = "w".repeat(40);
  const as = {
    none: {},
    wrong: { "X-Jazz-Backend-Secret": wrong },
    "wrong admin, right backend": { "X-Jazz-Admin-Secret": wrong, "X-Jazz-Backend-Secret": SECRET },
    backend: { "X-Jazz-Backend-Secret": SECRET },
    admin: { "X-Jazz-Admin-Secret": SECRET },
    both: { "X-Jazz-Admin-Secret": SECRET, "X-Jazz-Backend-Secret": SECRET },
    "credential, backend": {
      "X-Jazz-Backend-Credential": SECRET,
      "X-Jazz-Backend-Access": "backend",
    },
    "credential, admin": { "X-Jazz-Backend-Credential": SECRET, "X-Jazz-Backend-Access": "admin" },
    "credential, no access": { "X-Jazz-Backend-Credential": SECRET },
    "wrong credential": { "X-Jazz-Backend-Credential": wrong, "X-Jazz-Backend-Access": "admin" },
  };
  const admits = {
    vouching: ["backend", "admin", "both", "credential, backend", "credential, admin"],
    admin: ["admin", "both", "credential, admin"],
  };
  const routes = {
    resolve: "vouching",
    "login-or-register": "vouching",
    "found-local-first": "vouching",
    link: "admin",
    revoke: "admin",
    "revoke-sessions": "admin",
  };
  try {
    for (const [route, authority] of Object.entries(routes)) {
      for (const [who, headers] of Object.entries(as)) {
        // No body: the secrets are checked first, and an admitted request
        // then fails on its (missing) JSON.
        const response = await mf.dispatchFetch(`http://core/apps/${APP}/admin/accounts/${route}`, {
          method: "POST",
          headers,
        });
        const expected = admits[authority].includes(who) ? 400 : 401;
        assert.equal(response.status, expected, `${route} as ${who}`);
      }
    }
  } finally {
    await mf.dispose();
  }
});

test("revoking a user's sessions ends their stated sessions and refuses older ones", async () => {
  const mf = start(mkdtempSync(join(tmpdir(), "jazz-do-revoke-")));
  const identity = { issuer: "https://idp.example", subject: "alice" };
  let bundle;
  const statedAt = (authenticatedAt) =>
    JSON.stringify({
      peer_identity: fixture.testUserIdentity(identity.issuer, identity.subject),
      bundle,
      auth: {
        backend_session: {
          issuer: identity.issuer,
          user_id: identity.subject,
          claims: {},
          authenticatedAt,
        },
      },
    });
  const as = async (authenticatedAt) => {
    const socket = await connect(mf, backend);
    socket.send(statedAt(authenticatedAt));
    socket.send(fixture.testThinHello());
    return [socket, await socket.next()];
  };
  try {
    // A user session is pinned to the permissions the backend published.
    const published = await mf.dispatchFetch(`http://core/apps/${APP}/admin/bundles`, {
      method: "POST",
      headers: { "X-Jazz-Admin-Secret": SECRET, "content-type": "application/json" },
      body: fixture.testDeployRequest(SCHEMA),
    });
    bundle = (await published.json()).bundle;
    const [alice, hello] = await as(1000);
    assert.deepEqual(hello, { hello: true });

    const revoked = await mf.dispatchFetch(
      `http://core/apps/${APP}/admin/accounts/revoke-sessions`,
      {
        method: "POST",
        headers: { "X-Jazz-Admin-Secret": SECRET, "content-type": "application/json" },
        body: JSON.stringify({ identity, revokedBefore: 2000 }),
      },
    );
    assert.equal(revoked.status, 200);
    assert.match((await alice.next()).error ?? "", /session revoked/);
    await eventually(() => alice.closed(), "alice's socket closes");

    const [, older] = await as(1500);
    assert.match(older.error ?? "", /session revoked/);
    const [later, fresh] = await as(3000);
    assert.deepEqual(fresh, { hello: true }, "a session authenticated after the mark is admitted");
    later.close();
  } finally {
    await mf.dispose();
  }
});

test("a guest the backend relays syncs by its own proof, until its identity is revoked", async () => {
  const mf = start(mkdtempSync(join(tmpdir(), "jazz-do-guest-")));
  const audience = "https://my-app.example";
  const guest = JSON.parse(fixture.testGuest(7, audience, BigInt(Math.floor(Date.now() / 1000))));
  const principal = { issuer: guest.issuer, subject: guest.subject };
  try {
    const published = await mf.dispatchFetch(`http://core/apps/${APP}/admin/bundles`, {
      method: "POST",
      headers: { "X-Jazz-Admin-Secret": SECRET, "content-type": "application/json" },
      body: fixture.testDeployRequest(SCHEMA),
    });
    const { bundle } = await published.json();
    const relay = async (proof) => {
      // As jazz-tools relays a guest: the backend credential with no
      // access, and a stand-in in the prelude's secret slot.
      const socket = await connect(mf, { "X-Jazz-Backend-Credential": SECRET });
      socket.send(
        JSON.stringify({
          peer_identity: guest.peer,
          auth: { jwt_token: proof, backend_secret: "stated-by-backend-credential" },
          local_first_audience: audience,
          bundle,
        }),
      );
      socket.send(fixture.testThinHello());
      return [socket, await socket.next()];
    };

    const [, forged] = await relay(guest.proof.slice(0, -4) + "AAAA");
    assert.ok(forged.error, JSON.stringify(forged));

    const [socket, hello] = await relay(guest.proof);
    assert.deepEqual(hello, { hello: true });
    socket.send(fixture.testThinSchema(1n, SCHEMA));
    assert.deepEqual(await socket.next(), { id: 1, ok: true });
    socket.send(fixture.testThinInsertNote(2n, "by a guest"));
    assert.deepEqual(await socket.next(), { id: 2, written: true });

    // Another identity's assignment changing leaves the guest's socket be.
    const other = await mf.dispatchFetch(
      `http://core/apps/${APP}/admin/accounts/login-or-register`,
      {
        method: "POST",
        headers: { "X-Jazz-Backend-Secret": SECRET, "content-type": "application/json" },
        body: JSON.stringify({
          identity: { issuer: "https://idp.example", subject: "someone-else" },
        }),
      },
    );
    assert.equal(other.status, 200);
    socket.send(fixture.testThinInsertNote(3n, "still a guest"));
    assert.deepEqual(await socket.next(), { id: 3, written: true });

    // Revoking the guest's identity ends its live socket.
    const revoked = await mf.dispatchFetch(`http://core/apps/${APP}/admin/accounts/revoke`, {
      method: "POST",
      headers: { "X-Jazz-Admin-Secret": SECRET, "content-type": "application/json" },
      body: JSON.stringify({ approver: principal, target: principal }),
    });
    assert.equal(revoked.status, 204, await revoked.clone().text());
    assert.match((await socket.next()).error ?? "", /identity is not authorized/);
    await eventually(() => socket.closed(), "the guest's socket closes");
  } finally {
    await mf.dispose();
  }
});
