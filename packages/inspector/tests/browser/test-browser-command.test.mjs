import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import packageJson from "../../package.json" with { type: "json" };

function runBrowserCommand(
  t,
  { sealed = false, prepared = false, buildStatus = 0, testStatus = 0 } = {},
) {
  const directory = mkdtempSync(join(tmpdir(), "jazz-inspector-browser-command-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const root = join(directory, "packages/inspector");
  const write = (path, content, mode) => {
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, content, { mode });
  };
  // Native admission is a separate contract. Forward the real package command
  // to controlled build and browser executables without requiring native builds.
  write(
    join(directory, "dev/gates/run-correctness-consumer.mjs"),
    `
    import { spawnSync } from "node:child_process";
    const [command, ...args] = process.argv.slice(3);
    const result = spawnSync(command, args, { stdio: "inherit" });
    process.exit(result.status ?? 1);
  `,
  );
  write(
    join(root, "package.json"),
    JSON.stringify({
      name: "inspector-command-fixture",
      private: true,
      type: "module",
      scripts: { "build:embedded": "node build.mjs" },
    }),
  );
  write(
    join(root, "build.mjs"),
    `
    import { mkdirSync, writeFileSync } from "node:fs";
    if (process.env.FIXTURE_BUILD_STATUS !== "0") process.exit(Number(process.env.FIXTURE_BUILD_STATUS));
    mkdirSync("dist-embedded", { recursive: true });
    writeFileSync("dist-embedded/embedded.html", "fresh build");
  `,
  );
  write(
    join(root, "node_modules/.bin/playwright"),
    `#!${process.execPath}
    const { readFileSync, writeFileSync } = require("node:fs");
    const html = readFileSync("dist-embedded/embedded.html", "utf8");
    writeFileSync("browser-observed.txt", html);
    process.exit(Number(process.env.FIXTURE_TEST_STATUS));
  `,
    0o755,
  );
  const script = new URL("../../scripts/test-browser.mjs", import.meta.url);
  if (existsSync(script)) {
    mkdirSync(join(root, "scripts"), { recursive: true });
    copyFileSync(script, join(root, "scripts/test-browser.mjs"));
  }
  if (prepared) write(join(root, "dist-embedded/embedded.html"), "prepared build");
  const result = spawnSync("bash", ["-c", packageJson.scripts["test:browser"]], {
    cwd: root,
    encoding: "utf8",
    env: {
      ...process.env,
      JAZZ_TEST_SEALED_INSPECTOR_DIST: sealed ? "1" : "0",
      FIXTURE_BUILD_STATUS: String(buildStatus),
      FIXTURE_TEST_STATUS: String(testStatus),
    },
  });
  const observed = join(root, "browser-observed.txt");
  return { result, observed: existsSync(observed) ? readFileSync(observed, "utf8") : undefined };
}

test("standalone browser tests build the inspector before consuming it", (t) => {
  const { result, observed } = runBrowserCommand(t);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(observed, "fresh build");
});

test("sealed browser tests consume prepared assets without rebuilding them", (t) => {
  const { result, observed } = runBrowserCommand(t, { sealed: true, prepared: true });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(observed, "prepared build");
});

test("missing sealed inspector assets prevent browser tests from starting", (t) => {
  const { result, observed } = runBrowserCommand(t, { sealed: true });
  assert.notEqual(result.status, 0);
  assert.equal(observed, undefined);
});

test("a failed inspector build prevents browser tests from starting", (t) => {
  const { result, observed } = runBrowserCommand(t, { buildStatus: 23 });
  assert.equal(result.status, 23, result.stderr);
  assert.equal(observed, undefined);
});

test("browser failures remain failures when consuming sealed assets", (t) => {
  const { result, observed } = runBrowserCommand(t, {
    sealed: true,
    prepared: true,
    testStatus: 7,
  });
  assert.equal(result.status, 7, result.stderr);
  assert.equal(observed, "prepared build");
});
