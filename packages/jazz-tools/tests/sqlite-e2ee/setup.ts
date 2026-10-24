import { spawn, execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { TestProject } from "vitest/node";
import { verifyManifest } from "../../../../dev/artifacts/provenance.mjs";

declare module "vitest" {
  export interface ProvidedContext {
    sqliteE2eeServer: { appId: string; serverUrl: string; adminSecret: string };
  }
}

// Build the actual SQLite authority from this checkout. This suite needs no
// NAPI binding and must never borrow a stale native test artifact.
export default async function setup(project: TestProject) {
  // Browser projects inherit the root's provided context. Start one authority
  // per suite, rather than also starting one for each browser instance.
  if (!project.isRootProject()) return;
  const root = resolve(fileURLToPath(new URL("../../../..", import.meta.url)));
  const verifyWasm = () => {
    const mismatch = verifyManifest(root, "wasm", "fast");
    if (mismatch) throw new Error(`SQLite browser test requires current WASM: ${mismatch}`);
  };
  verifyWasm();
  // This dedicated suite does not load NAPI. Generate only the WASM expectation
  // from the checked producer manifest; the loader still verifies the actual
  // binary. The ordinary dual-artifact producer remains unchanged.
  const wasmManifest = JSON.parse(
    await readFile(join(root, "crates/jazz-wasm/pkg/.jazz-artifact-manifest.json"), "utf8"),
  );
  const fingerprint = wasmManifest.nativeArtifactFingerprint;
  if (!/^[a-f0-9]{64}$/.test(fingerprint)) throw new Error("WASM fingerprint is missing");
  const marker = join(root, "packages/jazz-tools/src/runtime/native-artifact-fingerprint-wasm.ts");
  const markerSource = `// Generated from the verified SQLite browser test artifact.\nexport const EXPECTED_WASM_ARTIFACT_FINGERPRINT = ${JSON.stringify(fingerprint)} as const;\n`;
  if ((await readFile(marker, "utf8").catch(() => "")) !== markerSource)
    await writeFile(marker, markerSource);
  const build = execFileSync(
    "cargo",
    ["build", "-p", "jazz-cli", "--bin", "jazz-tools", "--message-format=json"],
    { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"], maxBuffer: 16 << 20 },
  );
  const artifact = build
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line))
    .find(
      (entry) =>
        entry.reason === "compiler-artifact" &&
        entry.target.name === "jazz-tools" &&
        entry.executable,
    );
  if (!artifact) throw new Error("Cargo did not produce the SQLite server");
  const directory = await mkdtemp(join(tmpdir(), "jazz-sqlite-e2ee-"));
  const portFile = join(directory, "port");
  const appId = randomUUID();
  const adminSecret = randomUUID();
  const child = spawn(
    artifact.executable,
    [
      "server",
      appId,
      "--port",
      "0",
      "--in-memory",
      "--allow-local-first-auth",
      "--admin-secret",
      adminSecret,
      "--bound-port-file",
      portFile,
      "--shutdown-timeout-secs",
      "1",
    ],
    { cwd: root, stdio: ["ignore", "ignore", "pipe"] },
  );
  let output = "";
  let spawnError: Error | undefined;
  child.once("error", (error) => {
    spawnError = error;
  });
  child.stderr.on("data", (chunk) => {
    output = (output + chunk.toString()).slice(-16384);
  });
  const exited = new Promise<void>((done) => child.once("close", () => done()));
  const stop = async () => {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    const kill = setTimeout(() => child.kill("SIGKILL"), 5000);
    try {
      await exited;
    } finally {
      clearTimeout(kill);
      await rm(directory, { recursive: true, force: true });
    }
  };
  try {
    const deadline = Date.now() + 15000;
    while (Date.now() < deadline) {
      if (spawnError) throw spawnError;
      if (child.exitCode !== null || child.signalCode !== null)
        throw new Error(`SQLite authority exited: ${output}`);
      const port = await readFile(portFile, "utf8").catch(() => "");
      if (/^\d+\n$/.test(port)) {
        project.provide("sqliteE2eeServer", {
          appId,
          serverUrl: `http://127.0.0.1:${port.trim()}`,
          adminSecret,
        });
        return async () => {
          await stop();
          verifyWasm();
          const after = JSON.parse(
            await readFile(join(root, "crates/jazz-wasm/pkg/.jazz-artifact-manifest.json"), "utf8"),
          );
          if (after.nativeArtifactFingerprint !== fingerprint)
            throw new Error("WASM source changed during the SQLite browser test");
        };
      }
      await new Promise((done) => setTimeout(done, 25));
    }
    throw new Error(`SQLite authority did not become ready: ${output}`);
  } catch (error) {
    await stop();
    throw error;
  }
}
