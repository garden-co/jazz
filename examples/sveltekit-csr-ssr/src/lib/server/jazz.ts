// #region jazz-server
import {
  createJazzContext,
  createSnapshotBuilder,
  type Db,
  type JazzContext,
} from "jazz-tools/backend";
import { app } from "$lib/schema";
import permissions from "$lib/permissions";

// The `jazzSvelteKit()` managed runtime writes `PUBLIC_JAZZ_*` and
// `BACKEND_SECRET` into `process.env` for server-side code to read, so `pnpm dev`
// works with no `.env`. (Server-side `$env/dynamic/public` does not reflect the
// runtime's late injection — reading `process.env` is the correct way to do this
// with SvelteKit.) In production, set these yourself.
const appId = () => process.env.PUBLIC_JAZZ_APP_ID!;

let context: JazzContext | undefined;
let backend: Db | undefined;

function jazzContext(): JazzContext {
  if (context) return context;
  context = createJazzContext({
    appId: appId(),
    app,
    permissions,
    driver: { type: "memory" },
    serverUrl: process.env.PUBLIC_JAZZ_SERVER_URL!,
    backendSecret: process.env.BACKEND_SECRET,
  });
  return context;
}

// Note that using `asBackend` grants your DB full access to your data
export function backendDb(): Db {
  if (!backend) backend = jazzContext().asBackend();
  return backend;
}

// A fresh builder per server render so prefetches don't bleed between requests.
export function createServerSnapshot() {
  return createSnapshotBuilder({ appId: appId(), schema: app });
}
// #endregion jazz-server
