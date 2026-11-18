// G62-B2: Jazz's local-first client on Expo web names its broker worker with
// `require.unstable_resolveWorker("…")`, which Expo's Metro rewrites at build
// time (@expo/metro-config's collect-dependencies). The property never exists
// at runtime, so any read of it other than that exact call survives the
// rewrite and sees nothing: a `typeof` guard made every Expo web app fail with
// METRO_WORKER_API_ERROR. This runs Expo's own transform, from this app's Expo
// SDK, over the module jazz-tools ships and checks what reaches the bundle.
import { test } from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";

const require = createRequire(import.meta.url);
const expoDir = dirname(require.resolve("expo/package.json"));
const metroConfigDir = dirname(
  require.resolve("@expo/metro-config/package.json", { paths: [expoDir] }),
);
const fromMetroConfig = createRequire(join(metroConfigDir, "package.json"));
const babel = fromMetroConfig("@babel/core");
const generate = fromMetroConfig("@babel/generator").default;
const collectDependencies = fromMetroConfig(
  "./build/transform-worker/collect-dependencies.js",
).default;

/** The Metro asset module as jazz-tools ships it (built), else its source. */
function shippedMetroAssets() {
  const jazzTools = dirname(require.resolve("jazz-tools/package.json"));
  const built = join(jazzTools, "dist/runtime/browser-worker-assets.metro.js");
  if (existsSync(built)) return readFileSync(built, "utf8");
  const ts = require("typescript");
  const source = readFileSync(
    join(jazzTools, "src/runtime/browser-worker-assets.metro.ts"),
    "utf8",
  );
  return ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
  }).outputText;
}

test("Expo's Metro rewrites every use of require.unstable_resolveWorker in Jazz's web assets", () => {
  const ast = babel.parseSync(shippedMetroAssets(), {
    sourceType: "module",
    configFile: false,
    babelrc: false,
  });
  const { dependencies } = collectDependencies(ast, {
    asyncRequireModulePath: "expo-async-require",
    dependencyMapName: "_dependencyMap",
    dynamicRequires: "reject",
    inlineableCalls: [],
    keepRequireNames: true,
    allowOptionalDependencies: false,
    unstable_allowRequireContext: false,
  });
  const bundled = generate(ast, { comments: false }).code;

  assert.ok(
    dependencies.some(
      (dependency) =>
        dependency.name === "../worker/jazz-broker-worker.js" &&
        dependency.data.asyncType === "worker",
    ),
    "Expo builds the broker worker as a worker chunk",
  );
  assert.ok(
    !bundled.includes("unstable_resolveWorker"),
    `no read of require.unstable_resolveWorker survives into the bundle:\n${bundled}`,
  );
});
