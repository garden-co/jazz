"server-only";

import { app as schemaApp } from "../schema";
import permissions from "../permissions";
import type { Db, Session } from "jazz-tools/backend";

// This is a workaround to resolve NAPI modules correctly in the monorepo
// Real-world apps should just `import { createJazzContext } from "jazz-tools/backend"`
import { createRequire as createRequireFromModule } from "node:module";
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const nodeRequire = createRequire(import.meta.url);
const { createJazzContext, createSnapshotBuilder } = nodeRequire(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;

const context = createJazzContext({
  appId,
  app: schemaApp,
  permissions,
  driver: { type: "memory" },
  serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
  backendSecret: process.env.BACKEND_SECRET!,
});

// Two ways to get a Db to prefetch a snapshot from:
//
// 1. asBackend() has full access to all data. The prefetched rows end up in the
//    page's HTML, which anyone can read, so use it only for data that's genuinely
//    public. This example's todos are shared, so it prefetches with this.
export const db = context.asBackend();

// 2. forSession(session) scopes the prefetch to one viewer, so the snapshot only
//    carries rows that session is allowed to read — the safe choice for per-user
//    data. A real app derives the session from the incoming request
//    (context.forRequest(req)); forSession takes one you already have.
export function dbForSession(session: Session): Db {
  return context.forSession(session);
}

// Each server render produces a fresh builder so prefetches don't bleed
// between requests.
export function createServerSnapshot() {
  return createSnapshotBuilder({
    appId,
    schema: schemaApp,
  });
}
