import { gzipSync } from "node:zlib";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
const root = fileURLToPath(new URL("../../../", import.meta.url));
const workspace = join(root, "crates/jazz-wasm/durable-object");
const output = join(root, "packages/jazz-tools/durable-object-worker");
const pkg = join(workspace, "server-pkg");
const args = [
  "build",
  "jazz-do-server",
  "--target",
  "web",
  "--release",
  "--out-dir",
  "../server-pkg",
];
// FAST is only for local iteration; release workflows explicitly disable it.
if (process.env.FAST === "1") args.push("--no-opt");
args.push("--", "--locked");
if (process.env.FAST === "1")
  args.push(
    "--config",
    'profile.release.lto="off"',
    "--config",
    "profile.release.codegen-units=16",
  );
const built = spawnSync("wasm-pack", args, { cwd: workspace, env: process.env, stdio: "inherit" });
if (built.status !== 0) throw new Error("Durable Object WASM build failed");
const staging = await mkdtemp(join(workspace, ".worker-stage-"));
try {
  const source = `
    import { initSync, WasmServerCore, authorizeDurableObjectAdmin, authorizeDurableObjectAccess } from ${JSON.stringify(join(pkg, "jazz_do_server.js"))};
    import wasmModule from "./jazz.wasm";
    import { createJazzDurableObject, routeJazzRequest } from ${JSON.stringify(join(root, "crates/jazz-wasm/durable-object-server.js"))};
    initSync({ module: wasmModule });
    export const JazzObject = createJazzDurableObject({ WasmServerCore, authorizeDurableObjectAdmin, authorizeDurableObjectAccess });
    export default { fetch: routeJazzRequest };
  `;
  const result = await build({
    stdin: { contents: source, resolveDir: root },
    bundle: true,
    write: false,
    format: "esm",
    platform: "browser",
    target: "es2022",
    external: ["./jazz.wasm"],
    minify: true,
    legalComments: "none",
  });
  const worker = result.outputFiles[0].contents;
  const wasm = await readFile(join(pkg, "jazz_do_server_bg.wasm"));
  const { version } = JSON.parse(
    await readFile(join(root, "packages/jazz-tools/package.json"), "utf8"),
  );
  const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
  const manifest = {
    format: "jazz-do-worker-v1",
    version,
    workerSha256: hash(worker),
    wasmSha256: hash(wasm),
  };
  manifest.digest = hash(
    `${manifest.format}\0${version}\0${manifest.workerSha256}\0${manifest.wasmSha256}`,
  );
  // Compressed strings are upload data, not an executable WASM import. Worker
  // bundlers can include them without custom module rules or filesystem access.
  await writeFile(
    join(staging, "artifact.js"),
    `export const manifest = ${JSON.stringify(manifest)};\n` +
      `export const workerGzip = ${JSON.stringify(gzipSync(worker, { level: 9 }).toString("base64"))};\n` +
      `export const wasmGzip = ${JSON.stringify(gzipSync(wasm, { level: 9 }).toString("base64"))};\n`,
  );
  await writeFile(
    join(staging, "artifact.d.ts"),
    "export const manifest: { format: string; version: string; workerSha256: string; wasmSha256: string; digest: string };\nexport const workerGzip: string;\nexport const wasmGzip: string;\n",
  );
  await mkdir(output, { recursive: true });
  // The manifest and both payloads are published together in one module.
  for (const file of ["artifact.d.ts", "artifact.js"])
    await rename(join(staging, file), join(output, file));
  // Remove the obsolete unpacked representation from earlier builds.
  for (const file of ["worker.mjs", "jazz.wasm", "manifest.json"])
    await rm(join(output, file), { force: true });
  console.log(`Packaged Durable Object host ${version} (${manifest.digest.slice(0, 12)})`);
} finally {
  await rm(staging, { recursive: true, force: true });
}
