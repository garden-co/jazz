// Load every ```json title="jazz.app.json" / "jazz.permissions.json" block of the docs through
// jazz-napi, as `jazz-tools generate` would. Needs a built jazz-napi: node docs/scripts/check-app-file-blocks.mjs
// A permissions block is checked against the latest app block on the page with all its tables, or
// against `{/* check-app: <jazz.app.json> */}` right before it. Blocks without `format` are excerpts.
import { createRequire } from "node:module";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
const require = createRequire(new URL("../../packages/jazz-tools/package.json", import.meta.url));
const napi = require("jazz-napi");
const root = process.argv[2] ?? new URL("../content", import.meta.url).pathname;
const files = [];
(function walk(d) {
  for (const f of readdirSync(d)) {
    const p = join(d, f);
    if (statSync(p).isDirectory()) walk(p);
    else if (/\.(mdx|md)$/.test(f)) files.push(p);
  }
})(root);
let bad = 0,
  ok = 0,
  skipped = 0;
for (const file of files) {
  const text = readFileSync(file, "utf8");
  const re = /^([ \t]*)```json([^\n]*)\n([\s\S]*?)^\1```/gm;
  let m,
    lastApp = null;
  const apps = [];
  while ((m = re.exec(text))) {
    const meta = m[2];
    const indent = m[1];
    const body = m[3]
      .split("\n")
      .map((l) => (l.startsWith(indent) ? l.slice(indent.length) : l))
      .join("\n");
    const line = text.slice(0, m.index).split("\n").length;
    const kind = /jazz\.app\.json/.test(meta)
      ? "app"
      : /jazz\.permissions\.json/.test(meta)
        ? "perm"
        : null;
    if (!kind) continue;
    if (/nocheck/.test(meta)) {
      skipped++;
      continue;
    }
    let parsed;
    try {
      parsed = JSON.parse(body);
    } catch (e) {
      console.log(`${file}:${line} JSON: ${e.message}`);
      bad++;
      continue;
    }
    if (!parsed.tables || parsed.format === undefined) {
      console.log(`${file}:${line} excerpt (no tables), skipped`);
      skipped++;
      continue;
    }
    try {
      if (kind === "app") {
        const c = JSON.parse(napi.compileApp(body));
        apps.push({ c, tables: Object.keys(parsed.tables) });
        ok++;
      } else {
        const before = text.slice(0, m.index);
        const hint = /\{\/\* check-app: ([\s\S]*?) \*\/\}\s*$/.exec(before);
        const need = Object.keys(parsed.tables);
        const app = hint
          ? JSON.parse(napi.compileApp(hint[1]))
          : [...apps].reverse().find((a) => need.every((t) => a.tables.includes(t)))?.c;
        if (!app) {
          console.log(`${file}:${line} permissions block with no matching app before it`);
          skipped++;
          continue;
        }
        napi.loadBackendApp(JSON.stringify({ schema: app.schema, relations: app.relations }), body);
        ok++;
      }
    } catch (e) {
      console.log(
        `${file}:${line} ${kind}: ${String(e.message).split("\n").slice(0, 6).join(" | ")}`,
      );
      bad++;
    }
  }
}
console.log(`ok ${ok}, failed ${bad}, skipped ${skipped}`);
process.exit(bad ? 1 : 0);
