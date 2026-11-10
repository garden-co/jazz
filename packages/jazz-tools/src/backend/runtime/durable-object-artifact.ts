import pkg from "jazz-tools/package.json" with { type: "json" };
import type { DurableObjectArtifact } from "../core-providers/durable-objects.js";
// Generated with the release host. Literal import lets bundlers include the
// bytes without filesystem access, custom loaders or a remote artifact service.
export async function loadArtifact(): Promise<DurableObjectArtifact> {
  const { manifest, workerGzip, wasmGzip } =
    await import("../../../durable-object-worker/artifact.js");
  if (manifest.version !== pkg.version) throw new Error("Jazz DO host/package version mismatch");
  const inflate = async (encoded: string) => {
    const binary = atob(encoded);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
    return new Uint8Array(await new Response(stream).arrayBuffer());
  };
  const [worker, wasm] = await Promise.all([inflate(workerGzip), inflate(wasmGzip)]);
  return { manifest, worker, wasm };
}
