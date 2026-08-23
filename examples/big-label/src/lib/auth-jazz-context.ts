import type { JazzContext } from "jazz-tools/backend";
import { createRequire as createRequireFromModule } from "node:module";

// Keep the N-API backend runtime external to Next/Turbopack. This mirrors how
// an app consumes the published package, rather than bundling a platform .node
// asset into a route chunk.
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const { createJazzContext } = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

declare global {
  var __bigLabelAuthContext: JazzContext | undefined;
}

/** The sole trusted backend client in this example: auth persistence and bootstrap only. */
export function authJazzContext(): JazzContext {
  if (!globalThis.__bigLabelAuthContext) {
    globalThis.__bigLabelAuthContext = createJazzContext({
      appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
      driver: { type: "memory" },
      serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
      backendSecret: process.env.BACKEND_SECRET!,
      env: process.env.NODE_ENV === "production" ? "prod" : "dev",
      tier: "global",
    });
  }
  return globalThis.__bigLabelAuthContext;
}
