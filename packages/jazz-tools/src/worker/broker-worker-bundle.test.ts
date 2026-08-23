import { describe, expect, it } from "vitest";
import { execFile } from "node:child_process";
import { access, readFile, rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

describe("broker worker packaging", () => {
  it("the package bundling script emits a self-contained shipped worker", async () => {
    const packageRoot = fileURLToPath(new URL("../..", import.meta.url));
    const bundleScript = fileURLToPath(
      new URL("../../scripts/bundle-broker-worker.mjs", import.meta.url),
    );
    const outfile = fileURLToPath(
      new URL("../../dist/worker/jazz-broker-worker.js", import.meta.url),
    );
    const wasmOutfile = fileURLToPath(
      new URL("../../dist/worker/jazz_wasm_bg.wasm", import.meta.url),
    );
    const pkgPath = fileURLToPath(new URL("../../package.json", import.meta.url));
    const pkg = JSON.parse(await readFile(pkgPath, "utf8"));
    expect(pkg.scripts["build:runtime"]).toContain("bundle-broker-worker");

    // Delete the pre-existing build output so a no-op or broken script cannot
    // false-green by inspecting an artifact produced by an earlier command.
    await Promise.all([rm(outfile, { force: true }), rm(wasmOutfile, { force: true })]);
    await execFileAsync(process.execPath, [bundleScript], { cwd: packageRoot });

    const source = await readFile(outfile, "utf8");
    // Consumer bundlers copy this indirectly constructed SharedWorker URL
    // verbatim, so any remaining relative import would 404 in production.
    expect(source).not.toMatch(/\bfrom\s*["']\.\.?\//);
    expect(source).not.toMatch(/\bimport\s*\(\s*["']\.\.?\//);
    expect(source).toMatch(/onconnect/);
    await expect(access(wasmOutfile)).resolves.toBeUndefined();
  });
});
