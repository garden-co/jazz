import { describe, expect, it } from "vitest";
import { build } from "esbuild";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

describe("broker worker packaging", () => {
  it("bundles into self-contained ESM with no bare runtime imports", async () => {
    const entry = fileURLToPath(new URL("./jazz-broker-worker.ts", import.meta.url));
    const dir = await mkdtemp(join(tmpdir(), "jazz-broker-bundle-"));
    const outfile = join(dir, "jazz-broker-worker.js");
    try {
      await build({
        entryPoints: [entry],
        outfile,
        bundle: true,
        format: "esm",
        platform: "browser",
        target: "es2022",
        legalComments: "none",
      });
      const source = await readFile(outfile, "utf8");
      // Turbopack, webpack and Vite copy this worker verbatim — its SharedWorker
      // URL is indirected past their worker detection — so bare ../runtime/*.js
      // imports would 404 in the worker context. The shipped build must inline them.
      expect(source).not.toMatch(/from\s*["']\.\.\/runtime\//);
      expect(source).not.toMatch(/import\s*\(\s*["']\.\.\//);
      expect(source).toMatch(/onconnect/);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it("ships a bundled broker worker via build:runtime", async () => {
    const pkgPath = fileURLToPath(new URL("../../package.json", import.meta.url));
    const pkg = JSON.parse(await readFile(pkgPath, "utf8"));
    expect(pkg.scripts["build:runtime"]).toContain("bundle-broker-worker");
  });
});
