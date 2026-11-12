import "server-only";

import { app as schemaApp } from "../schema";
import permissions from "../permissions";
import type { Db, Jazz } from "jazz-tools/backend";

// This is a workaround to resolve correctly NAPI modules in the monorepo
// Real-world apps should just `import { createJazz } from "jazz-tools/backend"`
import { createRequire as createRequireFromModule } from "node:module";
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const { acceptDeviceAccounts, createJazz, memory, sqlite } = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

type GlobalJazz = typeof globalThis & { __jazzNextCsrSsr?: Promise<Jazz> };
const globalState = globalThis as GlobalJazz;

/**
 * Jazz, inside this Next app: browsers sync with it under `/jazz` (see
 * app/jazz/[...path]/route.ts), and server components and actions read and
 * write through its `db`. There is no sign-in, so browsers are local-first
 * guests. Shared across requests and hot reloads.
 */
export function jazz(): Promise<Jazz> {
  const pending = (globalState.__jazzNextCsrSsr ??= createJazz({
    app: schemaApp,
    permissions,
    // No sign-in: every browser is a guest, so let guests sync.
    authenticate: acceptDeviceAccounts,
    store: process.env.JAZZ_IN_MEMORY === "1" ? memory() : sqlite({ path: ".jazz" }),
  }).ready());
  // A failed start must not poison every later request.
  pending.catch(() => {
    if (globalState.__jazzNextCsrSsr === pending) globalState.__jazzNextCsrSsr = undefined;
  });
  return pending;
}

/** The backend's own database (backend authority), for server components and actions. */
export async function getBackendDb(): Promise<Db> {
  return (await jazz()).db;
}
