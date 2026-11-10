import { readFile } from "node:fs/promises";
import type { DurableObjectArtifact } from "../core-providers/durable-objects.js";
export async function loadArtifact(): Promise<DurableObjectArtifact> {
  const base = new URL("../../../durable-object-worker/", import.meta.url);
  const [manifest, worker, wasm] = await Promise.all([
    readFile(new URL("manifest.json", base), "utf8"),
    readFile(new URL("worker.mjs", base)),
    readFile(new URL("jazz.wasm", base)),
  ]);
  const artifact = {
    manifest: JSON.parse(manifest) as DurableObjectArtifact["manifest"],
    worker,
    wasm,
  };
  const pkg = JSON.parse(await readFile(new URL("../../../package.json", import.meta.url), "utf8"));
  if (pkg.version !== artifact.manifest.version)
    throw new Error("Jazz DO host/package version mismatch");
  return artifact;
}
