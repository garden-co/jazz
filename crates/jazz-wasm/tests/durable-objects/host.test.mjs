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

const APP = "00000000-0000-0000-0000-00000000a001";
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
    bindings: { JAZZ_APP_ID: APP, JAZZ_CORE_SECRET: SECRET, JAZZ_SCHEMA: SCHEMA },
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
  ws.addEventListener("close", () => {
    closed = true;
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
    bindings: { JAZZ_APP_ID: APP, JAZZ_CORE_SECRET: SECRET },
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
      bindings: { JAZZ_APP_ID: APP, JAZZ_CORE_SECRET: SECRET },
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
      bindings: { JAZZ_APP_ID: APP, JAZZ_CORE_SECRET: SECRET },
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
