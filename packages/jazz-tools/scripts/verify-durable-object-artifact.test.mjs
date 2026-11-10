import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { gzipSync } from "node:zlib";
import { verifyDurableObjectArtifact } from "./verify-durable-object-artifact.mjs";

test("packaging requires a complete artifact matching the package version and both payload hashes", async () => {
  const dir = await mkdtemp(join(tmpdir(), "jazz-do-artifact-check-"));
  try {
    await writeFile(join(dir, "package.json"), JSON.stringify({ version: "2.0.0-alpha.60" }));
    await assert.rejects(verifyDurableObjectArtifact(dir), /Missing Durable Object artifact/);
    const out = join(dir, "durable-object-worker");
    await mkdir(out);
    const worker = Buffer.from("worker"),
      wasm = Buffer.from("wasm");
    const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
    const manifest = {
      format: "jazz-do-worker-v1",
      version: "2.0.0-alpha.60",
      workerSha256: hash(worker),
      wasmSha256: hash(wasm),
    };
    manifest.digest = hash(
      `${manifest.format}\0${manifest.version}\0${manifest.workerSha256}\0${manifest.wasmSha256}`,
    );
    const writeArtifact = (w = worker, m = wasm) =>
      writeFile(
        join(out, "artifact.js"),
        `export const manifest=${JSON.stringify(manifest)};export const workerGzip=${JSON.stringify(gzipSync(w).toString("base64"))};export const wasmGzip=${JSON.stringify(gzipSync(m).toString("base64"))};`,
      );
    await writeArtifact();
    await assert.rejects(verifyDurableObjectArtifact(dir), /ENOENT/);
    await writeFile(
      join(out, "artifact.d.ts"),
      "export const manifest: object;\nexport const workerGzip: string;\nexport const wasmGzip: string;\n",
    );
    const result = await verifyDurableObjectArtifact(dir);
    assert.deepEqual(result.worker, worker);
    assert.deepEqual(result.wasm, wasm);
    for (const [w, m] of [
      [Buffer.from("wrong"), wasm],
      [worker, Buffer.from("wrong")],
    ]) {
      await writeArtifact(w, m);
      await assert.rejects(verifyDurableObjectArtifact(dir), /hash mismatch/);
    }
    await writeArtifact();
    await writeFile(join(dir, "package.json"), JSON.stringify({ version: "2.0.0-alpha.61" }));
    await assert.rejects(verifyDurableObjectArtifact(dir), /version mismatch/);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
