// Serve worker.mjs in local workerd (Miniflare) on PORT (default 8787),
// with SQLite-backed Durable Objects persisted under PERSIST (default: in
// memory). Build ./pkg first (see ../README.md).
//
//   node serve.mjs   then   doc-shelf-do-ceiling-ws ws://127.0.0.1:8787
import { Miniflare } from "miniflare";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const pkg = join(here, "pkg");
const mf = new Miniflare({
  modules: [
    { type: "ESModule", path: join(here, "worker.mjs") },
    { type: "ESModule", path: join(pkg, "jazz_do.js") },
    { type: "CompiledWasm", path: join(pkg, "jazz_do_bg.wasm") },
  ],
  modulesRoot: here,
  compatibilityDate: "2026-07-01",
  durableObjects: { AUTHORITY: { className: "Authority", useSQLite: true } },
  durableObjectsPersist: process.env.PERSIST ?? false,
  port: Number(process.env.PORT ?? 8787),
  host: "127.0.0.1",
});
const url = await mf.ready;
console.log(`serving ${url}`);
process.on("SIGINT", async () => {
  await mf.dispose();
  process.exit(0);
});
