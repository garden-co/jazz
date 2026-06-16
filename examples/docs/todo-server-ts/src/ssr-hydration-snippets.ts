// #region nextjs-jazz-server
import { createJazzContext, createSnapshotBuilder } from "jazz-tools/backend";
import { app } from "../schema.js";
import permissions from "../permissions.js";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;

const context = createJazzContext({
  appId,
  app,
  permissions,
  driver: { type: "memory" },
  serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
  backendSecret: process.env.BACKEND_SECRET!,
});

// `asBackend()` reads with full access, so use it only for public data. For
// per-user pages, prefetch with `context.forRequest(req)` / `forSession(session)`
// instead, so the snapshot only carries rows that viewer may read.
export const db = context.asBackend();

// A fresh builder per server render so prefetches don't bleed between requests.
export function createServerSnapshot() {
  return createSnapshotBuilder({ appId, schema: app });
}
// #endregion nextjs-jazz-server
