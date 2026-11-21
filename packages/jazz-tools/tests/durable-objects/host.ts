import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

const jazzWasm = dirname(createRequire(import.meta.url).resolve("jazz-wasm/package.json"));

/**
 * Whether jazz-wasm's Worker builds are there: the Durable Object core
 * (`pnpm --filter jazz-wasm build:durable-object`) and the backend build
 * `createJazz` runs on (`pnpm --filter jazz-wasm build:backend`).
 */
export const hostBuilt =
  existsSync(join(jazzWasm, "durable-object/pkg/jazz_wasm_do_bg.wasm")) &&
  existsSync(join(jazzWasm, "backend/pkg/jazz_wasm_backend_bg.wasm"));
