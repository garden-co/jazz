import { execFileSync, spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, describe, expect, it } from "vitest";
import { optionalPeerImportError } from "./optional-peer.js";

// Vercel's Node runtime starts functions with this flag (read off a deployed
// function's process.execArgv on Node 24), so a CommonJS file there can't
// require() an ES module, though Node 22.12+ allows it everywhere else.
const VERCEL_NODE_FLAGS = ["--no-experimental-require-module"];

const packageDir = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const scratch = mkdtempSync(join(tmpdir(), "jazz-require-esm-"));
afterAll(() => rmSync(scratch, { recursive: true, force: true }));

function writePackage(name: string, files: Record<string, string>): void {
  const dir = join(scratch, "node_modules", name);
  mkdirSync(dir, { recursive: true });
  for (const [file, text] of Object.entries(files)) writeFileSync(join(dir, file), text);
}

describe("optional peers under Vercel's Node flags", () => {
  it("loads the @vercel/sandbox jazz-tools is tested against", () => {
    // @vercel/sandbox 4.1.0 to 5.0.0 fail here (their @vercel/drives
    // require()s the ES-only @workflow/serde): moving the dev dependency to
    // one of them must fail this test, not adopters' deployments.
    const run = spawnSync(
      process.execPath,
      [...VERCEL_NODE_FLAGS, "--input-type=module", "-e", 'await import("@vercel/sandbox");'],
      { cwd: packageDir, encoding: "utf8" },
    );
    expect(run.stderr).toBe("");
    expect(run.status).toBe(0);
  });

  it("explains a peer that require()s an ES module", () => {
    // The shape of @vercel/sandbox 4.1.0's failure on Vercel: an ES entry
    // that imports a CommonJS dependency, which require()s an ES-only one.
    writePackage("esm-only", {
      "package.json": JSON.stringify({ name: "esm-only", type: "module", exports: "./index.js" }),
      "index.js": "export const value = 1;\n",
    });
    writePackage("cjs-dep", {
      "package.json": JSON.stringify({ name: "cjs-dep", main: "./index.js" }),
      "index.js": 'module.exports = require("esm-only");\n',
    });
    const probe = [
      "try {",
      '  await import("cjs-dep");',
      "} catch (error) {",
      "  console.log(JSON.stringify({ code: error.code, message: error.message }));",
      "}",
    ].join("\n");
    const output = execFileSync(
      process.execPath,
      [...VERCEL_NODE_FLAGS, "--input-type=module", "-e", probe],
      { cwd: scratch, encoding: "utf8" },
    );
    const thrown = JSON.parse(output) as { code: string; message: string };
    expect(thrown.code).toBe("ERR_REQUIRE_ESM");
    const cause = Object.assign(new Error(thrown.message), { code: thrown.code });

    const error = optionalPeerImportError("vercel()", "@vercel/sandbox", cause, {
      requireEsm: "Install @vercel/sandbox@4.0.0.",
    });
    expect(error.message).toBe(
      "vercel() could not load the @vercel/sandbox package: it require()s an ES module, which this " +
        "Node process doesn't allow (Vercel's functions run Node with --no-experimental-require-module). " +
        `Install @vercel/sandbox@4.0.0. The error: ${thrown.message}`,
    );
    expect(error.cause).toBe(cause);
  });
});
