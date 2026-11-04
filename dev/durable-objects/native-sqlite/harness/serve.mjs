// Serve worker.mjs in local workerd (Miniflare) on PORT (default 8787),
// with SQLite-backed Durable Objects persisted under PERSIST (default: in
// memory). Build ./pkg first (see ../README.md).
//
//   node serve.mjs   then   doc-shelf-do-ceiling-ws ws://127.0.0.1:8787
import { createHarness } from "./runtime.mjs";
const mf = createHarness();
const url = await mf.ready;
console.log(`serving ${url}`);
process.on("SIGINT", async () => {
  await mf.dispose();
  process.exit(0);
});
