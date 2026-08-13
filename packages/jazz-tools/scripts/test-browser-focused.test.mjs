import test from "node:test";
import assert from "node:assert/strict";
import { resolve, join } from "node:path";
import { mkdirSync, rmdirSync, symlinkSync, unlinkSync } from "node:fs";
import { win32 } from "node:path";
import { isInsideBrowserRoot, parseArgs, run } from "./test-browser-focused.mjs";

test("builds a Vitest command scoped to one file", () => {
  const result = parseArgs(["tests/browser/alpha-public-flow-gate.test.ts"]);
  assert.equal(result.args.at(-1), resolve("tests/browser/alpha-public-flow-gate.test.ts"));
  assert.deepEqual(result.args.slice(0, 5), [
    "exec",
    "vitest",
    "run",
    "--config",
    "vitest.config.browser.ts",
  ]);
});

test("forwards an optional test name pattern", () => {
  assert.deepEqual(
    parseArgs(["tests/browser/alpha-public-flow-gate.test.ts", "-t", "works"]).args.slice(-2),
    ["--testNamePattern", "works"],
  );
});

test("accepts pnpm's forwarded argument separator", () => {
  assert.equal(
    parseArgs(["--", "tests/browser/alpha-public-flow-gate.test.ts"]).file,
    resolve("tests/browser/alpha-public-flow-gate.test.ts"),
  );
});

test("runs the artifact preflight before Vitest and stops on a red preflight", () => {
  const calls = [];
  const spawn = (command, args) => {
    calls.push({ command, args });
    return { status: 17 };
  };
  assert.equal(run(["tests/browser/alpha-public-flow-gate.test.ts"], spawn), 17);
  assert.deepEqual(calls, [
    { command: "node", args: ["../../dev/artifacts/verify-correctness-test-artifacts.mjs"] },
  ]);
});

test("runs Vitest only after the artifact preflight succeeds", () => {
  const calls = [];
  const spawn = (command, args) => {
    calls.push({ command, args });
    return { status: 0 };
  };
  assert.equal(run(["tests/browser/alpha-public-flow-gate.test.ts"], spawn), 0);
  assert.equal(calls[0].command, "node");
  assert.equal(calls[1].command, "pnpm");
  assert.deepEqual(calls[1].args.slice(0, 5), [
    "exec",
    "vitest",
    "run",
    "--config",
    "vitest.config.browser.ts",
  ]);
});

test("rejects missing, multiple, and unknown arguments with specific errors", () => {
  assert.throws(() => parseArgs([]), { message: "expected exactly one browser test file" });
  assert.throws(() => parseArgs(["tests/browser/a.test.ts", "tests/browser/b.test.ts"]), {
    message: "expected exactly one browser test file",
  });
  assert.throws(() => parseArgs(["--watch", "tests/browser/a.test.ts"]), {
    message: "unknown option: --watch",
  });
  assert.throws(() => parseArgs(["tests/browser/missing.test.ts"]), {
    message: /browser test file does not exist/,
  });
});

test("rejects directories and canonical paths outside browser tests", () => {
  const nestedDir = join("tests/browser", ".focused-runner-directory");
  mkdirSync(nestedDir);
  try {
    assert.throws(() => parseArgs([nestedDir]), { message: /browser test path is not a file/ });
  } finally {
    rmdirSync(nestedDir);
  }
  assert.throws(() => parseArgs(["scripts/test-browser-focused.mjs"]), {
    message: "file must be inside tests/browser",
  });
  const link = join("tests/browser", ".focused-runner-outside-link.test.ts");
  try {
    symlinkSync("../../scripts/test-browser-focused.mjs", link);
    assert.throws(() => parseArgs([link]), { message: "file must be inside tests/browser" });
  } finally {
    try {
      unlinkSync(link);
    } catch {}
  }
});

test("rejects cross-volume Windows paths", () => {
  const relativePath = win32.relative("C:\\repo\\tests\\browser", "D:\\other\\file.test.ts");
  assert.equal(isInsideBrowserRoot(relativePath, win32), false);
});
