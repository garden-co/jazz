import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

/** Whether jazz-wasm's Durable Object build is there (`pnpm --filter jazz-wasm build:durable-object`). */
export const hostBuilt = existsSync(
  join(
    dirname(createRequire(import.meta.url).resolve("jazz-wasm/package.json")),
    "durable-object/pkg",
  ),
);
