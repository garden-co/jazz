import { createRequire as createRequireFromModule } from "node:module";

/**
 * `jazz-tools/backend`, loaded with Node's require at runtime rather than
 * imported. The backend pulls in the native `jazz-napi` binding, which the
 * Next bundler must not try to bundle; a static import from a route or server
 * module makes Turbopack do exactly that.
 */
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;

export const jazzBackend = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");
