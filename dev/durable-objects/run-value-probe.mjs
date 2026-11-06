import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { Miniflare } from "miniflare";
const require = createRequire(import.meta.url);
const mf = new Miniflare({
  modules: true,
  scriptPath: new URL("./value-probe.js", import.meta.url).pathname,
  compatibilityDate: "2026-07-01",
  durableObjects: { PROBE: { className: "ValueProbe", useSQLite: true } },
});
try {
  const response = await mf.dispatchFetch("http://probe/");
  assert.equal(response.status, 200, await response.clone().text());
  const result = await response.json();
  assert.deepEqual(
    result.exact,
    result.integers.map((value) => ({ type: "integer", value })),
  );
  assert.deepEqual(
    result.matches,
    result.integers.map((_, i) => [i]),
  );
  assert.deepEqual(result.comparisons.bound, [{ type: "integer" }]);
  assert.deepEqual(result.comparisons.exact, result.comparisons.bound);
  assert.deepEqual(result.comparisons.cast, [{ type: "text" }, { type: "integer" }]);
  assert.deepEqual(result.typed, [
    { type: "integer", value: "1" },
    { type: "real", value: 1 },
    { type: "real", value: 1e100 },
    { type: "null", value: null },
    { type: "text", value: "9007199254740993" },
    { type: "text", value: "a\0λ🙂" },
    { type: "blob", value: [0, 127, 128, 255] },
  ]);
  assert.deepEqual(result.aggregate, {
    minimum: result.integers[0],
    maximum: result.integers.at(-1),
  });
  assert.equal(result.sum, "9007199254740993");
  assert.equal(result.rawRounded, "9007199254740992");
  assert.deepEqual(result.realBinding, { type: "real", castType: "real" });
  assert.deepEqual(result.top, ["9223372036854775807", "9007199254740993"]);
  assert.deepEqual(result.returning, [{ type: "integer", value: "9007199254740993" }]);
  assert.equal(result.rollback, "9007199254740993");
  console.log(
    JSON.stringify(
      {
        miniflare: require("miniflare/package.json").version,
        workerd: require("workerd/package.json").version,
        ...result,
      },
      null,
      2,
    ),
  );
  console.log("Exact SQL value assertions passed (platform proof; production adapter unchanged).");
} finally {
  await mf.dispose();
}
