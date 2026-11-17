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
