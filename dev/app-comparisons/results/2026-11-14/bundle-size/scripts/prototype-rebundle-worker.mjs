// Prototype of scripts/bundle-broker-worker.mjs with code splitting (+ optional minify),
// run on the published dist so the lazy OpenTelemetry imports stay lazy.
import { build } from "esbuild";
const [dir, minify] = process.argv.slice(2);
const r = await build({
  entryPoints: { "jazz-broker-worker": `${dir}/_entry.js` },
  outdir: dir, bundle: true, format: "esm", platform: "browser", target: "es2022",
  legalComments: "none", splitting: true, chunkNames: "chunks/[name]-[hash]",
  minify: minify === "1", metafile: true, allowOverwrite: true, logLevel: "warning",
});
for (const [f, o] of Object.entries(r.metafile.outputs)) console.log(String(o.bytes).padStart(8), f.replace(/.*dist\//, ""));
