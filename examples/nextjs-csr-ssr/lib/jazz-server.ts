"server-only";

import { app as schemaApp } from "../schema";
import permissions from "../permissions";

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

// Note that using `asBackend` grants your DB full access to your data
export const db = context.asBackend();

// Each server render produces a fresh builder so prefetches don't bleed
// between requests.
export function createServerSnapshot() {
  return createSnapshotBuilder({
    appId,
    schema: schemaApp,
  });
}
