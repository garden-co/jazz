// The packed release build of `jazz-wasm/durable-object` (optimised by
// wasm-opt, its glue patched by reinstantiable.mjs) starts in workerd and
// answers its backend: run after `build:durable-object`, before publishing,
// for breaks only a release build has.
import assert from "node:assert/strict";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { Miniflare } from "miniflare";

const here = fileURLToPath(new URL(".", import.meta.url));
const SECRET = "s".repeat(40);

test("the release Durable Object entry starts and admits its backend", async () => {
  const mf = new Miniflare({
    modules: true,
    scriptPath: join(here, "release-worker.mjs"),
    modulesRoot: join(here, "../.."),
    modulesRules: [
      { type: "ESModule", include: ["**/*.js", "**/*.mjs"] },
      { type: "CompiledWasm", include: ["**/*.wasm"] },
    ],
    compatibilityDate: "2026-04-01",
    durableObjects: { JAZZ: { className: "JazzCore", useSQLite: true } },
    bindings: { JAZZ_CORE_SECRET: SECRET },
  });
  try {
    assert.equal(await (await mf.dispatchFetch("http://core/health")).text(), "ok");
    const app = await mf.dispatchFetch("http://core/app", {
      headers: { "X-Jazz-Backend-Credential": SECRET },
    });
    assert.equal(app.status, 200);
    assert.match((await app.json()).appId, /^[0-9a-f-]{36}$/);
    const admit = await mf.dispatchFetch("http://core/apps/any/backend/admit", {
      method: "POST",
      headers: { "X-Jazz-Backend-Secret": SECRET },
    });
    assert.equal(admit.status, 204);
    // The test-only routes are not in a release build.
    const panic = await mf.dispatchFetch("http://core/apps/any/test/panic", {
      method: "POST",
      headers: { "X-Jazz-Admin-Secret": SECRET },
    });
    assert.equal(panic.status, 404);
  } finally {
    await mf.dispose();
  }
});
