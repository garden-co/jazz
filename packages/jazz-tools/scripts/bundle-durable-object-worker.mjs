// Package the production DO host, never the development fixture crate.
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
// Local iteration only. prepack uses the optimized release build by default.
if (process.env.FAST === "1")
  args.push(
    "--no-opt",
    "--",
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
  await writeFile(join(staging, "worker.mjs"), worker);
  await writeFile(join(staging, "jazz.wasm"), wasm);
  await writeFile(join(staging, "manifest.json"), JSON.stringify(manifest, null, 2) + "\n");
  await mkdir(output, { recursive: true });
  // Manifest last: readers verify both hashes and fail closed if racing a build.
  for (const file of ["worker.mjs", "jazz.wasm", "manifest.json"])
    await rename(join(staging, file), join(output, file));
  console.log(`Packaged Durable Object host ${version} (${manifest.digest.slice(0, 12)})`);
} finally {
  await rm(staging, { recursive: true, force: true });
}
