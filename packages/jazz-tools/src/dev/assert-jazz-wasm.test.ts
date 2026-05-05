import { mkdtempSync, rmSync, writeFileSync, mkdirSync, realpathSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative, resolve } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { assertJazzWasmInstalled } from "./vite.js";

// Build a synthetic project layout where jazz-wasm is "installed" so
// assertJazzWasmInstalled can find it via Node resolution. Placing the
// fixture outside the workspace prevents real pnpm symlinks from leaking
// into the resolution.
let fixtureRoot: string;
let projectRoot: string;

beforeAll(() => {
  fixtureRoot = realpathSync(mkdtempSync(join(tmpdir(), "jazz-wasm-assert-")));
  projectRoot = join(fixtureRoot, "app");
  const jazzWasmDir = join(projectRoot, "node_modules", "jazz-wasm");
  mkdirSync(jazzWasmDir, { recursive: true });
  writeFileSync(join(projectRoot, "package.json"), JSON.stringify({ name: "host" }));
  writeFileSync(
    join(jazzWasmDir, "package.json"),
    JSON.stringify({ name: "jazz-wasm", version: "0.0.0" }),
  );
});

afterAll(() => {
  rmSync(fixtureRoot, { recursive: true, force: true });
});

describe("assertJazzWasmInstalled", () => {
  it("ASRT-U01 resolves jazz-wasm from an absolute consumer root", () => {
    expect(() => assertJazzWasmInstalled(projectRoot)).not.toThrow();
  });

  it("ASRT-U02 resolves jazz-wasm from a relative consumer root anchored to cwd", () => {
    const fromCwd = relative(process.cwd(), projectRoot);
    // Sanity: the relative form must not itself be absolute, and it must
    // resolve back to the absolute fixture root.
    expect(resolve(fromCwd)).toBe(projectRoot);
    expect(() => assertJazzWasmInstalled(fromCwd)).not.toThrow();
  });
});
