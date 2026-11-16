#!/usr/bin/env node
/**
 * `node dev/gates/app-files-current.mjs <dir>`: whether the app's committed
 * jazz.app.json and jazz.permissions.json are what `jazz-tools generate`
 * writes from its TS, without generating bindings. For apps in a language the
 * npm CLI doesn't generate yet (Kotlin), whose bindings jazz-codegen's golden
 * tests check against the committed JSON. Runs against the built jazz-tools.
 */
import { readFileSync, existsSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const dist = join(root, "packages", "jazz-tools", "dist", "dev");
const dir = resolve(process.argv[2] ?? ".");
const { loadTsApp } = await import(pathToFileURL(join(dist, "ts-project.js")).href);
const { writtenFiles } = await import(pathToFileURL(join(dist, "project.js")).href);

const app = await loadTsApp(dir);
const files = await writtenFiles(app);
const stale = [
  ["jazz.app.json", files.app],
  ["jazz.permissions.json", files.permissions],
].filter(([file, text]) => {
  const path = join(app.dir, file);
  return !existsSync(path) || readFileSync(path, "utf8") !== text;
});
if (stale.length > 0) {
  console.error(
    `${stale.map(([f]) => f).join(" and ")} out of date with the app's TS: run \`npx jazz-tools generate\` (it writes them before the bindings) and commit them.`,
  );
  process.exit(1);
}
console.log("jazz.app.json and jazz.permissions.json are up to date with the app's TS.");
