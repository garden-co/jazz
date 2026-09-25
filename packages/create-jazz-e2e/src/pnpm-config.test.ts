import assert from "node:assert/strict";
import test from "node:test";

import { parse as parseYaml } from "yaml";

import { renderScaffoldedPnpmConfig } from "./pnpm-config.js";

test("renders Windows tarball paths as exact YAML file overrides", () => {
  const windowsTarballs = {
    "jazz-tools": String.raw`C:\Users\Build Agent\Jazz E2E\_tarballs\jazz-tools-2.0.0.tgz`,
    "jazz-napi": String.raw`C:\Users\Build Agent\Jazz E2E\_tarballs\jazz-napi-2.0.0.tgz`,
    "jazz-wasm": String.raw`C:\Users\Build Agent\Jazz E2E\_tarballs\jazz-wasm-2.0.0.tgz`,
  };
  const posixTarball = "/tmp/Jazz E2E/_tarballs/jazz-tools-2.0.0.tgz";
  const parsed = parseYaml(
    renderScaffoldedPnpmConfig({ ...windowsTarballs, "posix-package": posixTarball }),
  ) as { overrides: Record<string, string> };

  for (const [pkg, tarball] of Object.entries(windowsTarballs)) {
    assert.equal(parsed.overrides[pkg], `file:${tarball}`);
  }
  assert.equal(parsed.overrides["posix-package"], `file:${posixTarball}`);
  assert.equal(parsed.overrides.kysely, "0.28.17");
});
