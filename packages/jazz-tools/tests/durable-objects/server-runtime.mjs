import { Miniflare } from "miniflare";
import { readdirSync } from "node:fs";
import { resolve, join } from "node:path";
import { fileURLToPath } from "node:url";
const here = fileURLToPath(new URL(".", import.meta.url));
const root = resolve(here, "../../../..");
const pkg = join(here, "pkg");
export function createServerHarness(app, secret, overrides = {}) {
  return new Miniflare({
    modulesRoot: root,
    modules:
      process.env.JAZZ_DO_PACKAGED === "1"
        ? [
            {
              type: "ESModule",
              path: join(root, "packages/jazz-tools/durable-object-worker/worker.mjs"),
            },
            {
              type: "CompiledWasm",
              path: join(root, "packages/jazz-tools/durable-object-worker/jazz.wasm"),
            },
          ]
        : [
            { type: "ESModule", path: join(here, "catalogue-worker.mjs") },
            { type: "ESModule", path: join(root, "crates/jazz-wasm/durable-object-server.js") },
            { type: "ESModule", path: join(pkg, "jazz_do.js") },
            ...readdirSync(join(pkg, "snippets"), { recursive: true })
              .filter((file) => file.endsWith(".js"))
              .map((file) => ({ type: "ESModule", path: join(pkg, "snippets", file) })),
            { type: "CompiledWasm", path: join(pkg, "jazz_do_bg.wasm") },
          ],
    compatibilityDate: "2026-07-01",
    durableObjects: { JAZZ: { className: "JazzObject", useSQLite: true } },
    bindings: { JAZZ_APP_ID: app, JAZZ_ADMIN_SECRET: secret },
    port: 0,
    host: "127.0.0.1",
    ...overrides,
  });
}
