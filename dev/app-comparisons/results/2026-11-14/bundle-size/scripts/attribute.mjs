// Attribute a Vite build's real gzip / brotli bytes to packages and Jazz module groups.
//
// Usage: node attribute.mjs <visualizer raw-data .json> <dist/assets dir> [chunk ...]
//   chunks: file names (or prefixes) to count, e.g. the ones a page load fetched.
//
// rollup-plugin-visualizer gzips each module on its own, which overstates totals.
// We scale each chunk's per-module shares to the chunk's real gzip/brotli size,
// so the groups add up to what actually ships.
import { readFileSync, readdirSync } from "node:fs";
import { gzipSync, brotliCompressSync, constants } from "node:zlib";
const [vizFile, assets, ...want] = process.argv.slice(2);
const d = JSON.parse(readFileSync(vizFile, "utf8"));
export const group = (id) => {
  const m = id.match(/node_modules\/(?:\.pnpm\/[^/]+\/node_modules\/)?((?:@[^/]+\/)?[^/]+)\/(.*)/);
  if (!m) return "app";
  const [, pkg, path] = m;
  if (pkg === "jazz-wasm") return "jazz: wasm-bindgen glue";
  if (pkg.startsWith("@noble")) return "jazz dep: @noble crypto";
  if (pkg.startsWith("@opentelemetry")) return "jazz dep: @opentelemetry";
  if (pkg !== "jazz-tools") return pkg;
  const rules = [
    [/runtime\/json-schema/, "json-schema validator"],
    [/^dist\/permissions\//, "permissions DSL"],
    [/native-runtime-adapter|native-row-codec|native-codec|schema-codec/, "native-runtime adapter/codecs"],
    [/^dist\/thin\//, "thin client"],
    [/runtime\/(db|client|subscription-manager|query-adapter|query-builder|row-transformer|value-converter|column-transforms|write-)/, "Db / client core"],
    [/connection-manager|websocket|http-stream|browser-.*-connection|worker-protocol|browser-worker/, "transport / worker plumbing"],
    [/indexeddb|local-first|delete-local-first/, "local-first store"],
    [/^dist\/(session|accounts|local-first-auth)\//, "session / accounts"],
    [/^dist\/(react|react-core)\//, "react bindings"],
    [/typed-app|dsl|ir\.js|migrations|codegen|relationships|schema/, "schema DSL / IR / migrations"],
    [/telemetry/, "telemetry"],
  ];
  for (const [re, g] of rules) if (re.test(path)) return "jazz: " + g;
  return "jazz: other";
};
const chunks = {};
for (const m of Object.values(d.nodeMetas))
  for (const [chunk, uid] of Object.entries(m.moduleParts)) {
    const name = chunk.replace(/^assets\//, "");
    if (want.length && !want.some((w) => name.startsWith(w))) continue;
    (chunks[name] ??= []).push([group(m.id), d.nodeParts[uid].gzipLength]);
  }
const tot = {};
let all = { gz: 0, br: 0 };
for (const [name, mods] of Object.entries(chunks)) {
  const buf = readFileSync(`${assets}/${name}`);
  const gz = gzipSync(buf, { level: 9 }).length, br = brotliCompressSync(buf, { params: { [constants.BROTLI_PARAM_QUALITY]: 11 } }).length;
  all.gz += gz; all.br += br;
  const sum = mods.reduce((a, [, g]) => a + g, 0) || 1;
  for (const [g, s] of mods) { tot[g] ??= { gz: 0, br: 0 }; tot[g].gz += (gz * s) / sum; tot[g].br += (br * s) / sum; }
}
const kb = (x) => (x / 1024).toFixed(1).padStart(7);
console.log(`| group | gzip KB | brotli KB |\n|---|---:|---:|`);
for (const [g, v] of Object.entries(tot).sort((a, b) => b[1].gz - a[1].gz)) if (v.gz > 200) console.log(`| ${g} | ${kb(v.gz)} | ${kb(v.br)} |`);
const jz = Object.entries(tot).filter(([g]) => g.startsWith("jazz")).reduce((a, [, v]) => ({ gz: a.gz + v.gz, br: a.br + v.br }), { gz: 0, br: 0 });
console.log(`| **all Jazz** | ${kb(jz.gz)} | ${kb(jz.br)} |\n| **total (${Object.keys(chunks).length} chunks)** | ${kb(all.gz)} | ${kb(all.br)} |`);
