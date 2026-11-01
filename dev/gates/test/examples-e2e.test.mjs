import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

import {
  ORIGIN,
  renderPnpmWorkspace,
  resolveManifest,
  variantEnv,
} from "../../examples-e2e/run.mjs";
import { VARIANTS } from "../../examples-e2e/variants.mjs";

const root = path.resolve(import.meta.dirname, "../../..");
const tarballs = { "jazz-tools": "/t/jazz-tools-1.tgz", "jazz-napi": "/t/jazz-napi-1.tgz" };

test("workspace ranges become the packed tarballs and catalog ranges their versions", () => {
  const manifest = {
    dependencies: { "jazz-tools": "workspace:*", next: "catalog:default", react: "19.2.4" },
    devDependencies: { typescript: "catalog:" },
  };
  const resolved = resolveManifest(manifest, {
    tarballs,
    catalogs: { default: { next: "16.2.11", typescript: "6.0.2" } },
  });
  assert.deepEqual(resolved, {
    dependencies: { "jazz-tools": "file:/t/jazz-tools-1.tgz", next: "16.2.11", react: "19.2.4" },
    devDependencies: { typescript: "6.0.2" },
  });
  assert.equal(manifest.dependencies["jazz-tools"], "workspace:*", "input is not mutated");
});

test("a variant may not depend on an unpublished workspace package or an unknown catalog entry", () => {
  assert.throws(
    () => resolveManifest({ dependencies: { "jazz-internal": "workspace:*" } }, { tarballs }),
    /does not publish/,
  );
  assert.throws(
    () =>
      resolveManifest({ dependencies: { vite: "catalog:default" } }, { tarballs, catalogs: {} }),
    /not in the workspace catalogs/,
  );
});

test("the standalone workspace pins every packed package", () => {
  const yaml = renderPnpmWorkspace(tarballs);
  assert.match(yaml, /^overrides:\n/);
  assert.match(yaml, /"jazz-tools": "file:\/t\/jazz-tools-1\.tgz"/);
  assert.match(yaml, /"jazz-napi": "file:\/t\/jazz-napi-1\.tgz"/);
});

test("the variant environment never inherits a sync server or app secrets", () => {
  const env = variantEnv(
    { env: { PAYMENT_PROVIDER: "sandbox" } },
    {
      dataPath: "/d",
      baseEnv: {
        PATH: "/bin",
        JAZZ_SERVER_URL: "https://v2.sync.jazz.tools",
        JAZZ_ADMIN_SECRET: "a",
        BACKEND_SECRET: "b",
        NEXT_PUBLIC_JAZZ_APP_ID: "theirs",
      },
    },
  );
  assert.equal(env.PATH, "/bin");
  assert.equal(env.JAZZ_SERVER_URL, undefined);
  assert.equal(env.JAZZ_ADMIN_SECRET, undefined);
  assert.equal(env.BACKEND_SECRET, undefined);
  assert.notEqual(env.NEXT_PUBLIC_JAZZ_APP_ID, "theirs");
  assert.equal(env.NEXT_PUBLIC_APP_ORIGIN, ORIGIN);
  assert.equal(env.JAZZ_DATA_PATH, "/d");
  assert.equal(env.PAYMENT_PROVIDER, "sandbox");
  assert.match(env.BETTER_AUTH_SECRET, /^[0-9a-f]{64}$/);
});

test("the workflow matrix lists exactly the registered variants", () => {
  const workflow = fs.readFileSync(path.join(root, ".github/workflows/examples-e2e.yml"), "utf8");
  const matrix = workflow.match(/\n {8}variant:\n((?: {10}- [a-z-]+\n)+)/)?.[1];
  assert.ok(matrix, "examples-e2e.yml has a variant matrix");
  const listed = [...matrix.matchAll(/- ([a-z-]+)/g)].map((m) => m[1]);
  assert.deepEqual(
    listed,
    VARIANTS.map((v) => v.name),
  );
});

test("every variant is a standalone project the gate can build, start and drive", () => {
  for (const variant of VARIANTS) {
    const dir = path.join(root, variant.dir);
    const manifest = JSON.parse(fs.readFileSync(path.join(dir, "package.json"), "utf8"));
    for (const script of ["build", "start", "test:e2e"]) {
      assert.ok(manifest.scripts?.[script], `${variant.dir} has a "${script}" script`);
      assert.doesNotMatch(
        manifest.scripts[script],
        /\.\.\//,
        `${variant.dir} "${script}" stays inside the variant`,
      );
    }
    assert.ok(
      manifest.devDependencies?.["@playwright/test"],
      `${variant.dir} depends on @playwright/test`,
    );
    const config = fs.readFileSync(path.join(dir, "playwright.config.ts"), "utf8");
    assert.match(config, /JAZZ_E2E_PROD/, `${variant.dir} Playwright config has a production mode`);
  }
});
