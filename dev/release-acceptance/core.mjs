#!/usr/bin/env node
// Serves the core under test from the external project's installed (and
// already verified) jazz-napi, the way a vercel() Sandbox does: Node owns the
// signals and asks the core to drain.
import { createRequire } from "node:module";
import { join } from "node:path";

const [project, options] = process.argv.slice(2);
const require = createRequire(join(project, "package.json"));
const core = require("jazz-napi").serveCore({
  ...JSON.parse(options),
  adminSecret: process.env.JAZZ_ADMIN_SECRET,
  backendSecret: process.env.JAZZ_BACKEND_SECRET,
});
for (const signal of ["SIGTERM", "SIGINT"]) process.once(signal, () => core.stop());
core.closed().then(
  () => process.exit(0),
  (error) => {
    console.error(error);
    process.exit(1);
  },
);
