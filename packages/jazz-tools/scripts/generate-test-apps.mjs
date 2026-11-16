#!/usr/bin/env node
/**
 * Regenerate the `jazz.generated.ts` of every committed test app: each
 * directory under tests/ and src/ holding a `jazz.app.json` beside a
 * `jazz.generated.ts` (or under an `apps/` directory), as jazz-codegen
 * writes it from the JSON. These fixtures have no schema.ts, which
 * `jazz-tools generate` requires, so this script is their only writer
 * (test fixtures, not an authoring path). Browser and React
 * Native tests can't load jazz-napi, so their apps are committed with their
 * bindings; this keeps those bindings in step with the files.
 *
 *   node scripts/generate-test-apps.mjs           # rewrite stale bindings
 *   node scripts/generate-test-apps.mjs --check   # fail on stale bindings
 */
import { existsSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const check = process.argv.includes("--check");
const napi = createRequire(import.meta.url)("jazz-napi");

function* appDirs(dir, underApps = false) {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (!entry.isDirectory() || entry.name === "node_modules" || entry.name === "dist") continue;
    const child = join(dir, entry.name);
    const inApps = underApps || entry.name === "apps";
    if (
      existsSync(join(child, "jazz.app.json")) &&
      (inApps || existsSync(join(child, "jazz.generated.ts")))
    ) {
      yield child;
    }
    yield* appDirs(child, inApps);
  }
}

let stale = 0;
for (const base of ["tests"]) {
  for (const dir of appDirs(join(root, base))) {
    const appJson = readFileSync(join(dir, "jazz.app.json"), "utf8");
    const target = join(dir, "jazz.generated.ts");
    const current = existsSync(target) ? readFileSync(target, "utf8") : null;
    // As `validate` does: bindings are current when their `source` and
    // format match, however a formatter has laid them out.
    try {
      napi.validateBindings(appJson, current);
      continue;
    } catch {}
    const generated = napi.generate("ts", appJson);
    const shown = relative(root, target);
    if (check) {
      console.error(`stale: ${shown}`);
      stale++;
    } else {
      writeFileSync(target, generated);
      console.log(`wrote ${shown}`);
    }
  }
}
if (stale) {
  console.error(`${stale} test app binding(s) are stale: run node scripts/generate-test-apps.mjs`);
  process.exit(1);
}
