// Packaging verifies the existing build; it must never compile or download one.
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { resolve, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { gunzipSync } from "node:zlib";

export async function verifyDurableObjectArtifact(packageDir) {
  const { version } = JSON.parse(await readFile(join(packageDir, "package.json"), "utf8"));
  const dir = join(packageDir, "durable-object-worker");
  const source = await readFile(join(dir, "artifact.js"), "utf8").catch(() => {
    throw new Error(
      "Missing Durable Object artifact; run pnpm --filter jazz-tools build:durable-objects before packaging",
    );
  });
  const declarations = await readFile(join(dir, "artifact.d.ts"), "utf8");
  for (const name of ["manifest", "workerGzip", "wasmGzip"])
    if (!declarations.includes(`export const ${name}:`))
      throw new Error(`Missing Durable Object declaration: ${name}`);
  const { manifest, workerGzip, wasmGzip } = await import(
    `data:text/javascript;base64,${Buffer.from(source).toString("base64")}`
  );
  if (manifest.format !== "jazz-do-worker-v1" || manifest.version !== version)
    throw new Error("Durable Object artifact format/package version mismatch");
  const worker = gunzipSync(Buffer.from(workerGzip, "base64"));
  const wasm = gunzipSync(Buffer.from(wasmGzip, "base64"));
  const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
  const digest = hash(
    `${manifest.format}\0${version}\0${manifest.workerSha256}\0${manifest.wasmSha256}`,
  );
  if (
    hash(worker) !== manifest.workerSha256 ||
    hash(wasm) !== manifest.wasmSha256 ||
    digest !== manifest.digest
  )
    throw new Error("Durable Object artifact hash mismatch");
  return { manifest, worker, wasm };
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  await verifyDurableObjectArtifact(fileURLToPath(new URL("../", import.meta.url)));
  console.log("Verified packaged Durable Object host");
}
