import { Miniflare } from "miniflare";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { readdirSync } from "node:fs";

const here = dirname(fileURLToPath(import.meta.url));
const pkg = join(here, "pkg");
// wasm-bindgen's inline JS lands under pkg/snippets.
const snippets = readdirSync(join(pkg, "snippets"), { recursive: true })
  .filter((file) => file.endsWith(".js"))
  .map((file) => ({ type: "ESModule", path: join(pkg, "snippets", file) }));
export function createHarness(overrides = {}) {
  return new Miniflare({
    modules: [
      { type: "ESModule", path: join(here, "worker.mjs") },
      { type: "ESModule", path: join(pkg, "jazz_do.js") },
      ...snippets,
      { type: "CompiledWasm", path: join(pkg, "jazz_do_bg.wasm") },
    ],
    modulesRoot: here,
    compatibilityDate: "2026-07-01",
    durableObjects: { AUTHORITY: { className: "Authority", useSQLite: true } },
    bindings: process.env.TRACE ? { TRACE: "1" } : {},
    durableObjectsPersist: process.env.PERSIST ?? false,
    port: Number(process.env.PORT ?? 8787),
    host: "127.0.0.1",
    // INSPECTOR_PORT=9229 opens workerd's inspector, for profile.mjs.
    ...(process.env.INSPECTOR_PORT ? { inspectorPort: Number(process.env.INSPECTOR_PORT) } : {}),
    ...overrides,
  });
}
