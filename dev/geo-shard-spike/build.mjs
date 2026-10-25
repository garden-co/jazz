import { build } from "/home/user/jazz/node_modules/.pnpm/node_modules/esbuild/lib/main.js";
const [entry, out] = process.argv.slice(2);
const src = "/home/user/jazz/packages/jazz-tools/src";
await build({
  entryPoints: [entry], bundle: true, platform: "node", format: "esm", outfile: out, logLevel: "error",
  external: ["jazz-wasm", "jazz-napi"],
  plugins: [{ name: "stubs", setup(b) {
    b.onResolve({ filter: /native-artifact-fingerprint-(napi|wasm)\.js$/ }, () => ({ path: ""+new URL("./stub.js", import.meta.url).pathname+"" }));
    b.onResolve({ filter: /^jazz-tools\/permissions$/ }, () => ({ path: src + "/permissions/index.ts" }));
    b.onResolve({ filter: /^jazz-tools$/ }, () => ({ path: src + "/index.ts" }));
  }}],
});
