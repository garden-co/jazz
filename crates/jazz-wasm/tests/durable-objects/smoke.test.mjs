// Jazz on a real Durable Object's SQL, in local workerd (Miniflare): no
// Cloudflare account or deployment. Run `run.sh` (it builds the fixture
// package into ./pkg first).
//
//   object #1: write (short + 300 KiB note) ── read
//   restart (a new workerd over the same persisted storage)
//   object #2: read ── callback transaction (refused) ── read
//   scopes: outer(1, inner(2, fails), 3) keeps [1, 3]; whole(4, fails) undone
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { Miniflare } from "miniflare";

const here = fileURLToPath(new URL(".", import.meta.url));

function start(persist) {
  return new Miniflare({
    modules: true,
    scriptPath: join(here, "worker.mjs"),
    modulesRoot: here,
    modulesRules: [
      { type: "ESModule", include: ["**/*.js", "**/*.mjs"] },
      { type: "CompiledWasm", include: ["**/*.wasm"] },
    ],
    compatibilityDate: "2026-04-01",
    durableObjects: { STORE: { className: "Store", useSQLite: true } },
    durableObjectsPersist: persist,
  });
}

async function step(mf, name) {
  const response = await mf.dispatchFetch(`http://smoke/${name}`);
  const body = await response.text();
  assert.equal(response.status, 200, `${name}: ${body}`);
  return JSON.parse(body);
}

const LARGE = 300 * 1024;
const notes = [
  ["large", LARGE, true],
  ["short", 5, true],
];

test("Jazz stores, reads and restarts on a Durable Object's SQL", async () => {
  const persist = mkdtempSync(join(tmpdir(), "jazz-do-"));
  try {
    let mf = start(persist);
    assert.deepEqual(await step(mf, "write"), { written: 2 });
    assert.deepEqual(await step(mf, "read"), notes);
    await mf.dispose();

    // The object's next instance: an empty chunk cache, the same storage.
    mf = start(persist);
    assert.deepEqual(await step(mf, "read"), notes);
    const refused = await step(mf, "transaction");
    assert.match(refused, /is not supported on managed storage \(Durable Objects\) yet/);
    // The refused transaction left nothing behind.
    assert.deepEqual(await step(mf, "read"), notes);
    // Scopes on the object's own transactionSync: a failing inner scope is
    // undone while the outer keeps 1 and 3; a failing scope (4) is undone.
    assert.deepEqual(await step(mf, "scopes"), [1, 3]);
    await mf.dispose();
  } finally {
    rmSync(persist, { recursive: true, force: true });
  }
});
