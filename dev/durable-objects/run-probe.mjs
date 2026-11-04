import { Miniflare } from "miniflare";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
console.log("miniflare", require("miniflare/package.json").version);
console.log("workerd", require("workerd/package.json").version);

const mf = new Miniflare({
  modules: true,
  scriptPath: new URL("./probe.js", import.meta.url).pathname,
  compatibilityDate: "2026-07-01",
  durableObjects: { PROBE: { className: "Probe", useSQLite: true } },
});
try {
  const results = await (
    await mf.dispatchFetch(`http://probe/${process.argv.includes("--bench") ? "?bench" : ""}`)
  ).json();
  for (const [name, value] of Object.entries(results)) console.log(name.padEnd(48), value);
  assert.equal(results["nested transactionSync, inner throws"], "1");
  assert.equal(results["async transaction, await then throw"], "1");
  assert.equal(results["async helper SQL rolls back with caller"], "1");
  assert.equal(results["async helper SQL commits with caller"], "1,5");
  assert.deepEqual(results["wide integer in affinity-free column"], { type: "text", equal: 0 });
  assert.deepEqual(results["exact cast integer round trips"], [
    { type: "integer", value: "-9223372036854775808" },
    { type: "integer", value: "9007199254740993" },
    { type: "integer", value: "9223372036854775807" },
  ]);
  assert.deepEqual(results["integer epoch and real storage classes"], [
    "integer",
    "real",
    "text",
    "blob",
  ]);
  for (const name of [
    "function arguments 32",
    "function arguments 33",
    "function arguments 127",
    "statement 99999 bytes",
    "statement 100000 bytes",
    "LIKE pattern 50",
  ])
    assert.equal(results[name], 1, name);
  assert.equal(results["columns 100"], "ok");
  assert.equal(results["VM budget IN literals 5000"], 1);
  assert.match(results["VM budget IN literals 8000"], /SQLITE_NOMEM/);
  assert.equal(results["compound select 5"], 5);
  assert.deepEqual(results["internal tables"], ["_cf_KV"]);
  for (const name of [
    "sql BEGIN",
    "sql SAVEPOINT s1",
    "sql RELEASE s1",
    "sql COMMIT",
    "sql ROLLBACK",
    "function arguments 128",
    "bound params 101",
    "statement 100001 bytes",
    "columns 101",
    "compound select 6",
    "AND chain 101",
    "LIKE pattern 51",
  ])
    assert.match(results[name], /^ERR:/, name);
  console.log("Compatibility assertions passed.");
} finally {
  await mf.dispose();
}
