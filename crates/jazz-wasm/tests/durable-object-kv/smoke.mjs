// workerd smoke test of Jazz's store on Durable Object synchronous KV
// storage. Build first: `pnpm --filter jazz-wasm build:durable-object-kv`.
// Runs locally in Miniflare, never deploys; prints what it measured.
import assert from "node:assert/strict";
import { mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Miniflare } from "miniflare";

const here = dirname(fileURLToPath(import.meta.url));
const pkg = join(here, "pkg");
const persist = mkdtempSync(join(tmpdir(), "jazz-do-kv-"));
const ROWS = Number(process.env.ROWS ?? 5000);
// A new store's page size (default: Jazz's, 4 KiB).
const PAGE_SIZE = process.env.PAGE_SIZE ? Number(process.env.PAGE_SIZE) : undefined;

function harness() {
  const snippets = readdirSync(join(pkg, "snippets"), { recursive: true })
    .filter((file) => file.endsWith(".js"))
    .map((file) => ({ type: "ESModule", path: join(pkg, "snippets", file) }));
  return new Miniflare({
    modulesRoot: here,
    modules: [
      { type: "ESModule", path: join(here, "worker.mjs") },
      { type: "ESModule", path: join(pkg, "jazz_wasm.js") },
      ...snippets,
      { type: "CompiledWasm", path: join(pkg, "jazz_wasm_bg.wasm") },
    ],
    compatibilityDate: "2026-04-01",
    durableObjects: { STORE: { className: "Store", useSQLite: true } },
    durableObjectsPersist: persist,
  });
}

async function request(mf, path, args = [], object = "smoke") {
  const query = new URLSearchParams({ object, args: JSON.stringify(args) });
  const response = await mf.dispatchFetch(`http://do.test${path}?${query}`);
  return { status: response.status, text: await response.text() };
}

async function call(mf, path, args = [], object = "smoke") {
  const { status, text } = await request(mf, path, args, object);
  assert.equal(status, 200, `${path}: ${text}`);
  return JSON.parse(text);
}

const report = [];
const row = (step, result) => {
  report.push({ step, ...result });
};

try {
  let mf = harness();
  assert.deepEqual(await call(mf, "/rollback-probe"), { before: 1, inside: null });

  const opened = await call(mf, "/open", [PAGE_SIZE]);
  row("open (new)", opened);
  const write = await call(mf, "/write", [0, ROWS]);
  row(`write ${ROWS} rows, 1 commit each`, write);
  assert.equal(write.commits, ROWS, "one storage commit per row commit");
  const read = await call(mf, "/read");
  row("read (warm)", read);
  assert.equal(read.rows, ROWS);
  assert.equal(read.doneRows, Math.ceil(ROWS / 3));
  const stored = await call(mf, "/keys");
  await call(mf, "/close");
  // The same schema with few rows, to compare opens with.
  await call(mf, "/open", [PAGE_SIZE], "small");
  await call(mf, "/write", [0, 10], "small");
  await call(mf, "/close", [], "small");
  await mf.dispose();

  // A new workerd process on the same storage: nothing is preloaded.
  mf = harness();
  const small = await call(mf, "/open", [], "small");
  await call(mf, "/close", [], "small");
  const reopened = await call(mf, "/open");
  row("open (existing)", reopened);
  // Opening reads the schema's pages, never the rows'.
  assert.ok(
    reopened.gets <= small.gets + 5,
    `open of ${ROWS} rows read ${reopened.gets} keys, of 10 rows ${small.gets}`,
  );
  const cold = await call(mf, "/read");
  row("read (cold)", cold);
  assert.equal(cold.rows, ROWS);
  const more = await call(mf, "/write", [ROWS, 10]);
  assert.equal(more.commits, 10);
  assert.equal((await call(mf, "/read")).rows, ROWS + 10);

  // Objects sharing an isolate keep their own stores.
  await call(mf, "/open", [], "other");
  assert.equal((await call(mf, "/read", [], "other")).rows, 0);
  await call(mf, "/close", [], "other");

  // A commit storage refuses fails the write, resets the object, and leaves
  // storage at the last whole commit. (The probes keep one open store per
  // isolate, so reopen this object's.)
  await call(mf, "/open");
  const before = await call(mf, "/instance");
  await call(mf, "/fail-next-commit");
  const failed = await request(mf, "/write", [ROWS + 10, 1]);
  assert.equal(failed.status, 500, failed.text);
  assert.match(failed.text, /injected storage failure|could not be written/);
  let after = before;
  for (let i = 0; i < 50 && after === before; i++) {
    await new Promise((resolve) => setTimeout(resolve, 20));
    after = await call(mf, "/instance");
  }
  assert.notEqual(after, before, "the object was reset");
  await call(mf, "/open");
  assert.equal((await call(mf, "/read")).rows, ROWS + 10, "the failed commit never happened");
  assert.equal((await call(mf, "/write", [ROWS + 10, 1])).commits, 1);
  assert.equal((await call(mf, "/read")).rows, ROWS + 11);

  await call(mf, "/close");
  await call(mf, "/destroy");
  const left = await call(mf, "/keys");
  assert.deepEqual(left, { keys: 0, bytes: 0, all: 0 });
  await mf.dispose();

  console.table(report);
  console.log(
    `page size ${PAGE_SIZE ?? 4096}; stored: ${stored.keys} keys, ` +
      `${(stored.bytes / 1024).toFixed(0)} KiB for ${ROWS} rows; ` +
      `open of 10 rows read ${small.gets} keys`,
  );
  console.log("durable-object-kv smoke: ok");
} finally {
  rmSync(persist, { recursive: true, force: true });
}
