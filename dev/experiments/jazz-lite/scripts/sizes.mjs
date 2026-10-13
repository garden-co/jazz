// Build the same todo app twice (full Jazz vs Jazz Lite) and report every emitted asset.
import { build } from "vite";
import * as esbuild from "esbuild";
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const out = path.join(root, "dist-sizes");
fs.rmSync(out, { recursive: true, force: true });

function sizes(file) {
  const b = fs.readFileSync(file);
  return {
    raw: b.length,
    gz: zlib.gzipSync(b, { level: 9 }).length,
    br: zlib.brotliCompressSync(b, {
      params: {
        [zlib.constants.BROTLI_PARAM_QUALITY]: 11,
        [zlib.constants.BROTLI_PARAM_SIZE_HINT]: b.length,
      },
    }).length,
  };
}
const kib = (n) => (n / 1024).toFixed(1).padStart(9);
/** Assets the page can load: walk references from the HTML through every emitted JS file. */
function reachable(dir, html) {
  const files = fs
    .readdirSync(dir, { recursive: true })
    .filter((f) => fs.statSync(path.join(dir, f)).isFile());
  const names = files.map((f) => path.basename(f));
  const seen = new Set();
  const visit = (text) => {
    for (const f of files) {
      const name = path.basename(f);
      if (!seen.has(f) && !/\.(map|html)$/.test(f) && text.includes(name)) {
        seen.add(f);
        if (f.endsWith(".js")) visit(fs.readFileSync(path.join(dir, f), "utf8"));
      }
    }
  };
  void names;
  visit(fs.readFileSync(path.join(dir, html), "utf8"));
  return seen;
}

function report(label, dir, html) {
  const rows = [];
  const loadable = reachable(dir, html);
  for (const f of fs.readdirSync(dir, { recursive: true })) {
    const p = path.join(dir, f);
    if (!fs.statSync(p).isFile() || /\.(map|html)$/.test(f)) continue;
    rows.push({
      f: loadable.has(f) ? f : `${f}  (emitted, never referenced)`,
      loadable: loadable.has(f),
      ...sizes(p),
    });
  }
  rows.sort((a, b) => b.raw - a.raw);
  const total = rows
    .filter((r) => r.loadable)
    .reduce((t, r) => ({ raw: t.raw + r.raw, gz: t.gz + r.gz, br: t.br + r.br }), {
      raw: 0,
      gz: 0,
      br: 0,
    });
  console.log(
    `\n## ${label}\n${"raw KiB".padStart(9)} ${"gzip".padStart(9)} ${"brotli".padStart(9)}  asset`,
  );
  for (const r of rows) console.log(`${kib(r.raw)} ${kib(r.gz)} ${kib(r.br)}  ${r.f}`);
  console.log(`${kib(total.raw)} ${kib(total.gz)} ${kib(total.br)}  TOTAL loadable by the page`);
  return total;
}

const { jazzPlugin } = await import("jazz-tools/dev/vite");
for (const [name, plugins] of [
  ["full", [jazzPlugin()]],
  ["lite", []],
]) {
  await build({
    root,
    logLevel: "warn",
    configFile: false,
    plugins,
    define: {
      "import.meta.env.VITE_JAZZ_APP_ID": JSON.stringify("00000000-0000-0000-0000-000000000000"),
      "import.meta.env.VITE_JAZZ_SERVER_URL": JSON.stringify("https://example.invalid"),
    },
    build: {
      outDir: path.join(out, name),
      emptyOutDir: true,
      rollupOptions: { input: path.join(root, `index-${name}.html`) },
    },
  });
}
const full = report(
  "Full Jazz (jazz-tools 2.0.0-alpha.59, vite + jazzPlugin)",
  path.join(out, "full"),
  "index-full.html",
);
const lite = report("Jazz Lite (same app, same schema)", path.join(out, "lite"), "index-lite.html");
console.log(
  `\nLite / full: raw ${((100 * lite.raw) / full.raw).toFixed(2)}%, gzip ${((100 * lite.gz) / full.gz).toFixed(2)}%, brotli ${((100 * lite.br) / full.br).toFixed(2)}%`,
);

// Attribute the lite bundle by input module with esbuild (same entry, minified).
const meta = await esbuild.build({
  entryPoints: [path.join(root, "app/main-lite.ts")],
  bundle: true,
  minify: true,
  format: "esm",
  platform: "browser",
  write: false,
  metafile: true,
  outfile: path.join(out, "attrib.js"),
});
const inputs = Object.values(meta.metafile.outputs)[0].inputs;
const groups = {};
for (const [file, { bytesInOutput }] of Object.entries(inputs)) {
  const key = file.includes("node_modules/")
    ? file.replace(/.*node_modules\/((@[^/]+\/)?[^/]+)\/(dist\/)?/, "$1: ").replace(/\.js$/, "")
    : file.replace(/^.*jazz-lite\//, "");
  groups[key] = (groups[key] ?? 0) + bytesInOutput;
}
console.log("\n## Lite bundle by module (minified bytes, before compression)");
for (const [k, v] of Object.entries(groups)
  .sort((a, b) => b[1] - a[1])
  .slice(0, 30))
  console.log(`${String(v).padStart(8)}  ${k}`);

// Projections: what a lite app would ship with a DSL-only schema entry point,
// and the lite client runtime on its own.
const dist = path.join(root, "node_modules/jazz-tools/dist");
async function bundleSize(label, contents) {
  const result = await esbuild.build({
    stdin: { contents, resolveDir: root, loader: "ts" },
    bundle: true,
    minify: true,
    format: "esm",
    platform: "browser",
    write: false,
  });
  const file = path.join(out, `${label.replace(/\W+/g, "-")}.js`);
  fs.writeFileSync(file, result.outputFiles[0].contents);
  const s = sizes(file);
  console.log(`${kib(s.raw)} ${kib(s.gz)} ${kib(s.br)}  ${label}`);
}
console.log(
  `\n## Projections\n${"raw KiB".padStart(9)} ${"gzip".padStart(9)} ${"brotli".padStart(9)}`,
);
await bundleSize(
  "lite client runtime only (src/client.ts + protocol)",
  `
  import { createLiteDb } from "./src/client.ts";
  const db = createLiteDb({ url: "ws://x", getToken: () => "" });
  globalThis.db = db;`,
);
await bundleSize(
  "lite todo app with a DSL-only schema entry (no permissions/migrations)",
  `
  import { col } from "${dist}/dsl.js";
  import { rel, reverse } from "${dist}/relationships.js";
  import { defineApp, defineTable } from "${dist}/typed-app.js";
  import { createLiteDb } from "./src/client.ts";
  const s = { ...col, rel, reverse, table: defineTable, defineApp };
  const app = s.defineApp({
    projects: s.table({ name: s.string() }, { todosViaProject: s.reverse("todos", "project") }),
    todos: s.table({ title: s.string(), done: s.boolean(), description: s.string().optional(), owner_id: s.uuid(),
      parentId: s.uuid().optional(), projectId: s.uuid().optional() },
      { parent: s.rel("todos", "parentId"), todosViaParent: s.reverse("todos", "parent"), project: s.rel("projects", "projectId") }),
  });
  const db = createLiteDb({ url: "ws://x", getToken: () => "" });
  db.subscribe(app.todos.where({ done: false }).include({ project: true }), console.log);
  db.insert(app.todos, { title: "x", done: false, owner_id: "y" });`,
);
