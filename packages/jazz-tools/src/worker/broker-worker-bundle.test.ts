import { describe, expect, it } from "vitest";
import { execFile } from "node:child_process";
import { access, mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

describe("broker worker packaging", () => {
  it("the package bundling script emits a self-contained shipped worker", async () => {
    const packageRoot = fileURLToPath(new URL("../..", import.meta.url));
    const bundleScript = fileURLToPath(
      new URL("../../scripts/bundle-broker-worker.mjs", import.meta.url),
    );
    const outputDir = await mkdtemp(join(tmpdir(), "jazz-broker-worker-bundle-"));
    const outfile = join(outputDir, "jazz-broker-worker.js");
    const wasmOutfile = join(outputDir, "jazz_wasm_bg.wasm");
    const pkgPath = fileURLToPath(new URL("../../package.json", import.meta.url));
    const pkg = JSON.parse(await readFile(pkgPath, "utf8"));
    expect(pkg.scripts["build:runtime"]).toContain("bundle-broker-worker");

    try {
      // Tests must not regenerate the public worker: TypeScript CI starts its
      // browser consumers alongside this node suite. A private output also
      // ensures this remains a genuine bundling receipt rather than inspecting
      // a prior package build.
      await execFileAsync(process.execPath, [bundleScript, "--out-dir", outputDir], {
        cwd: packageRoot,
      });

      const source = await readFile(outfile, "utf8");
      // Consumer bundlers copy this indirectly constructed SharedWorker URL
      // verbatim, so any remaining relative import would 404 in production.
      expect(source).not.toMatch(/\bfrom\s*["']\.\.?\//);
      expect(source).not.toMatch(/\bimport\s*\(\s*["']\.\.?\//);
      expect(source).toMatch(/onconnect/);
      await expect(access(wasmOutfile)).resolves.toBeUndefined();
      expect((await stat(wasmOutfile)).size).toBeGreaterThan(0);
    } finally {
      await rm(outputDir, { recursive: true, force: true });
    }
  });

  it("rejects a test child attempting to replace the sealed public worker", async () => {
    const packageRoot = fileURLToPath(new URL("../..", import.meta.url));
    const bundleScript = fileURLToPath(
      new URL("../../scripts/bundle-broker-worker.mjs", import.meta.url),
    );
    await expect(
      execFileAsync(process.execPath, [bundleScript], {
        cwd: packageRoot,
        env: { ...process.env, JAZZ_TEST_SEALED_TOOLS_DIST: "1" },
      }),
    ).rejects.toMatchObject({
      stderr: expect.stringContaining("worker output is sealed for concurrent tests"),
    });
  });
});
