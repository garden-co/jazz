// Bundle the whole napkin port with esbuild (minified, ES2022, browser) and
// report raw / gzip-9 / brotli-11, plus a per-area breakdown.
//
// Every module is imported as a namespace and kept alive, the TS analogue of
// "everything reachable from the WASM exports" (almost all Rust code is).
import * as esbuild from "esbuild";
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";
import ts from "typescript";

const root = path.dirname(new URL(import.meta.url).pathname);
const outDir = path.join(root, "dist");
fs.mkdirSync(outDir, { recursive: true });

const files = [];
(function walk(dir) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) walk(p);
    else if (p.endsWith(".ts") && !p.endsWith(".d.ts")) files.push(path.relative(root, p));
  }
})(path.join(root, "src"));
files.sort();

const sizes = (buf) => ({
  raw: buf.length,
  gzip: zlib.gzipSync(buf, { level: 9 }).length,
  brotli: zlib.brotliCompressSync(buf, { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 11 } })
    .length,
});

// Value exports per file, so the entry can keep every export alive without
// materialising namespace objects (which would add every export name as a string).
const program = ts.createProgram(
  files.map((f) => path.join(root, f)),
  {
    target: ts.ScriptTarget.ES2022,
    module: ts.ModuleKind.ES2022,
    moduleResolution: ts.ModuleResolutionKind.Bundler,
    noEmit: true,
    skipLibCheck: true,
  },
);
const checker = program.getTypeChecker();
const valueExports = new Map();
for (const f of files) {
  const sf = program.getSourceFile(path.join(root, f));
  const sym = sf && checker.getSymbolAtLocation(sf);
  const names = [];
  for (const e of sym ? checker.getExportsOfModule(sym) : []) {
    const t = e.flags & ts.SymbolFlags.Alias ? checker.getAliasedSymbol(e) : e;
    if (t.flags & ts.SymbolFlags.Value) names.push(e.name);
  }
  valueExports.set(f, names);
}

function entrySource(list) {
  const lines = [];
  const refs = [];
  list.forEach((f, i) => {
    const names = valueExports.get(f) ?? [];
    if (f.startsWith("src/_extern/")) return; // placeholders, kept only where referenced
    lines.push(
      names.length
        ? `import * as m${i} from "./${f.replace(/\.ts$/, ".js")}";`
        : `import "./${f.replace(/\.ts$/, ".js")}";`,
    );
    for (const n of names)
      refs.push(n === "default" ? `m${i}.default` : `m${i}[${JSON.stringify(n)}]`);
  });
  lines.push(`globalThis.__keep = [${refs.join(",")}];`);
  return lines.join("\n");
}

const AREAS = [
  ["groove/ivm", /^src\/groove\/ivm\//],
  ["groove/db", /^src\/groove\/db\//],
  ["groove/storage", /^src\/groove\/storage\//],
  ["groove/records+values", /^src\/groove\//],
  ["types", /^src\/types\//],
  ["protocol", /^src\/protocol\//],
  ["compression", /^src\/compression\//],
  ["model", /^src\/model\//],
  ["engine", /^src\/engine\//],
  ["node", /^src\/node\//],
  ["db", /^src\/db\//],
  ["wasm bindings", /^src\/wasm\//],
  ["idb-tree", /^src\/idb_tree\//],
  ["wasm-tracing", /^src\/wasm_tracing\//],
  ["support", /^src\/support\//],
  ["extern stubs", /^src\/_extern\//],
];
const areaOf = (f) =>
  f.includes("node_modules") ? "npm deps" : (AREAS.find(([, re]) => re.test(f))?.[0] ?? "other");

async function bundle(name, list, externalOutside) {
  const entry = path.join(root, `.entry-${name}.ts`);
  fs.writeFileSync(entry, entrySource(list));
  const plugins = externalOutside
    ? [
        {
          name: "external-outside",
          setup(b) {
            b.onResolve({ filter: /^\./ }, (a) => {
              if (a.importer === entry) return;
              const abs = path.relative(
                root,
                path.resolve(path.dirname(a.importer), a.path).replace(/\.js$/, ".ts"),
              );
              if (!externalOutside(abs)) return { path: a.path, external: true };
            });
          },
        },
      ]
    : [];
  try {
    const r = await esbuild.build({
      entryPoints: [entry],
      bundle: true,
      minify: true,
      format: "esm",
      target: "es2022",
      platform: "browser",
      outfile: path.join(outDir, `${name}.js`),
      metafile: true,
      pure: ["extern"],
      plugins,
      logLevel: "error",
      legalComments: "none",
    });
    return { buf: fs.readFileSync(path.join(outDir, `${name}.js`)), meta: r.metafile };
  } finally {
    fs.rmSync(entry);
  }
}

const whole = await bundle("all", files);
const total = sizes(whole.buf);
const byArea = {};
for (const [input, v] of Object.entries(Object.values(whole.meta.outputs)[0].inputs)) {
  const a = areaOf(input);
  byArea[a] = (byArea[a] ?? 0) + v.bytesInOutput;
}

// Standalone per-area bundles (other areas external) for per-area gzip.
const standalone = {};
for (const [a] of [...AREAS, ["npm deps"]]) {
  if (a === "npm deps" || a === "support") continue;
  const list = files.filter((f) => areaOf(f) === a);
  if (!list.length) continue;
  const { buf } = await bundle(
    `area-${a.replace(/[^a-z]+/g, "_")}`,
    list,
    (abs) => areaOf(abs) === a,
  );
  standalone[a] = sizes(buf);
}
{
  const { buf } = await bundle(
    "area-support+npm",
    files.filter((f) => areaOf(f) === "support"),
  );
  standalone["support + npm deps"] = sizes(buf);
}

let tsLines = 0;
for (const f of files)
  tsLines += fs
    .readFileSync(path.join(root, f), "utf8")
    .split("\n")
    .filter((l) => l.trim() && !l.trim().startsWith("//")).length;

const report = {
  files: files.length,
  tsLines,
  total,
  minifiedBytesByArea: byArea,
  standaloneByArea: standalone,
};
fs.writeFileSync(path.join(outDir, "report.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));
