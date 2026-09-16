const assert = require("node:assert/strict");
const { spawnSync } = require("node:child_process");
const { copyFileSync, mkdtempSync, rmSync, writeFileSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { test } = require("node:test");

function fixture(t, files, assertions, env = {}) {
  const dir = mkdtempSync(join(tmpdir(), "jazz-loader-test-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  copyFileSync(join(__dirname, "../native-binding.cjs"), join(dir, "native-binding.cjs"));
  for (const [name, source] of Object.entries(files)) writeFileSync(join(dir, name), source);
  const result = spawnSync(
    process.execPath,
    [
      "-e",
      `
    const assert = require('node:assert/strict');
    ${assertions}
  `,
    ],
    {
      cwd: dir,
      encoding: "utf8",
      env: { ...process.env, JAZZ_CORRECTNESS_ARTIFACT_RUN: "", ...env },
    },
  );
  assert.equal(result.status, 0, result.stderr || result.error?.message);
}

test("installed incompatible binary retains nested loader errors and actionable details", (t) => {
  fixture(
    t,
    {
      "native-loader.cjs": `
      const binary = new Error("/fixture/native.node: GLIBC_2.38 not found");
      binary.code = "ERR_DLOPEN_FAILED";
      binary.path = "/fixture/native.node";
      const missing = new Error("Cannot find optional platform package", { cause: binary });
      missing.code = "MODULE_NOT_FOUND";
      const error = new Error("Cannot find native binding. Please try npm i again", { cause: missing });
      globalThis.originalLoaderError = error;
      throw error;
    `,
    },
    `
    assert.throws(() => require('./native-binding.cjs'), error => {
      assert.match(error.message, /binary was found but could not be loaded/);
      assert.match(error.message, /GLIBC_2.38 not found/);
      assert.doesNotMatch(error.message, /artifact is missing|reinstall|npm i/);
      assert.equal(error.code, 'ERR_DLOPEN_FAILED');
      assert.equal(error.cause, globalThis.originalLoaderError);
      assert.equal(error.cause.cause.cause.path, '/fixture/native.node');
      return true;
    });
  `,
  );
});

test("missing artifact retains installation guidance and module error", (t) => {
  fixture(
    t,
    {},
    `
    assert.throws(() => require('./native-binding.cjs'), error => {
      assert.match(error.message, /artifact is missing/);
      assert.match(error.message, /reinstall matching Jazz package versions/);
      assert.equal(error.code, 'MODULE_NOT_FOUND');
      assert.equal(error.cause.code, 'MODULE_NOT_FOUND');
      assert.match(error.cause.message, /native-loader.cjs/);
      return true;
    });
  `,
  );
});

test("pointer failure does not fall back to a packaged binding", (t) => {
  fixture(
    t,
    {
      "native-binding.pointer.cjs": `throw Object.assign(new Error('pointer rejected'), { code: 'ERR_TEST_POINTER' });`,
      "native-loader.cjs": `throw new Error('unexpected packaged fallback');`,
    },
    `
    assert.throws(() => require('./native-binding.cjs'), error => {
      assert.equal(error.code, 'ERR_TEST_POINTER');
      assert.match(error.message, /pointer rejected/);
      assert.doesNotMatch(error.message, /artifact is missing|unexpected packaged fallback/);
      return true;
    });
  `,
  );
});

test("successful packaged binding still returns its fingerprint", (t) => {
  fixture(
    t,
    {
      "native-loader.cjs": `module.exports = { fixture: true };`,
      "native-artifact-fingerprint.cjs": `exports.expectedNativeArtifactFingerprint = 'fixture-fingerprint';`,
    },
    `
    assert.deepEqual(require('./native-binding.cjs'), {
      nativeBinding: { fixture: true },
      expectedNativeArtifactFingerprint: 'fixture-fingerprint',
    });
  `,
  );
});
