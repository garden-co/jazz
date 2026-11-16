import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
  copyFileSync,
  existsSync,
  mkdtempSync,
  mkdirSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const scriptPath = join(dirname(fileURLToPath(import.meta.url)), "stage-inspector-overlay.mjs");

function makeFixture(t) {
  const root = mkdtempSync(join(tmpdir(), "stage-inspector-overlay-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));

  const jazzToolsScripts = join(root, "packages/jazz-tools/scripts");
  const source = join(root, "packages/inspector/dist-embedded");
  const destination = join(root, "packages/jazz-tools/dist/dev/inspector-overlay/embedded");
  const fixtureScript = join(jazzToolsScripts, "stage-inspector-overlay.mjs");

  mkdirSync(jazzToolsScripts, { recursive: true });
  copyFileSync(scriptPath, fixtureScript);
  mkdirSync(destination, { recursive: true });
  writeFileSync(join(destination, "embedded.html"), "old embedded content");

  return { source, destination, fixtureScript };
}

function runStage(fixtureScript) {
  return spawnSync(process.execPath, [fixtureScript], { encoding: "utf8" });
}

test("staging replaces an existing embedded overlay from a valid source", (t) => {
  const { source, destination, fixtureScript } = makeFixture(t);
  mkdirSync(source, { recursive: true });
  writeFileSync(join(source, "embedded.html"), "new embedded content");

  const result = runStage(fixtureScript);

  assert.equal(result.status, 0, result.stderr);
  assert.equal(readFileSync(join(destination, "embedded.html"), "utf8"), "new embedded content");
});

test("a missing source fails without destroying the existing embedded overlay", (t) => {
  const { destination, fixtureScript } = makeFixture(t);

  const result = runStage(fixtureScript);

  assert.notEqual(result.status, 0, result.stderr);
  assert.equal(readFileSync(join(destination, "embedded.html"), "utf8"), "old embedded content");
});

test("a source without embedded.html fails validation without destroying the existing overlay", (t) => {
  const { source, destination, fixtureScript } = makeFixture(t);
  mkdirSync(source, { recursive: true });
  writeFileSync(join(source, "other-asset.txt"), "not the embedded page");

  const result = runStage(fixtureScript);

  assert.notEqual(result.status, 0, result.stderr);
  assert.equal(readFileSync(join(destination, "embedded.html"), "utf8"), "old embedded content");
});

test("a directory named embedded.html is rejected without replacing the existing overlay", (t) => {
  const { source, destination, fixtureScript } = makeFixture(t);
  mkdirSync(join(source, "embedded.html"), { recursive: true });

  const result = runStage(fixtureScript);

  assert.notEqual(result.status, 0, result.stderr);
  assert.equal(readFileSync(join(destination, "embedded.html"), "utf8"), "old embedded content");
});

test("an embedded.html symlink escaping the source is rejected without replacing the existing overlay", (t) => {
  const { source, destination, fixtureScript } = makeFixture(t);
  mkdirSync(source, { recursive: true });
  const outsideSource = join(dirname(source), "outside.html");
  writeFileSync(outsideSource, "outside content");
  symlinkSync(outsideSource, join(source, "embedded.html"));

  const result = runStage(fixtureScript);

  assert.notEqual(result.status, 0, result.stderr);
  assert.equal(readFileSync(join(destination, "embedded.html"), "utf8"), "old embedded content");
});

test("a previous interrupted promotion restores its backup on the next invocation", (t) => {
  const { destination, fixtureScript } = makeFixture(t);
  const backup = join(dirname(destination), ".inspector-overlay-backup");
  rmSync(destination, { recursive: true, force: true });
  mkdirSync(backup, { recursive: true });
  writeFileSync(join(backup, "embedded.html"), "recoverable embedded content");

  const result = runStage(fixtureScript);

  assert.notEqual(result.status, 0, result.stderr);
  assert.equal(
    readFileSync(join(destination, "embedded.html"), "utf8"),
    "recoverable embedded content",
  );
});

// E2 (alpha.61 rc.1): on Vercel the backend answered "This jazz-tools build has
// no Inspector UI", because it read the UI from disk next to its own module and
// Next.js's output file tracing never copied those files. The UI must reach a
// deployment by following the code's own imports alone.
test("the staged Inspector UI is served from a bundle of the code alone, with no files beside it", async (t) => {
  const { build } = await import("esbuild");
  const { source, fixtureScript } = makeFixture(t);
  const packageRoot = dirname(dirname(fixtureScript));
  const backendDir = join(packageRoot, "dist/backend");
  mkdirSync(join(source, "assets"), { recursive: true });
  writeFileSync(join(source, "embedded.html"), "overlay page");
  writeFileSync(
    join(source, "backend.html"),
    '<!doctype html><title>Jazz Inspector</title><script type="module" src="./assets/app-abc.js"></script>',
  );
  writeFileSync(join(source, "assets", "app-abc.js"), "console.log('inspector')");
  writeFileSync(join(source, "favicon.ico"), Buffer.from([0, 1, 2, 255]));

  // The backend's modules as tsc emits them into dist/backend.
  const realBackend = join(dirname(scriptPath), "../src/backend");
  for (const name of ["inspect-ui", "inspector-ui-files"]) {
    await build({
      entryPoints: [join(realBackend, `${name}.ts`)],
      outfile: join(backendDir, `${name}.js`),
      format: "esm",
      platform: "node",
      logLevel: "silent",
    });
  }
  writeFileSync(join(backendDir, "inspector-ui-files.js.map"), "{}");

  const result = runStage(fixtureScript);
  assert.equal(result.status, 0, result.stderr);
  assert.ok(!existsSync(join(backendDir, "inspector-ui-files.js.map")), "stale map removed");

  // Bundle as a deployment would, then remove the package: only what the
  // bundle carries is left.
  const deployed = mkdtempSync(join(tmpdir(), "inspector-ui-deployed-"));
  t.after(() => rmSync(deployed, { recursive: true, force: true }));
  await build({
    entryPoints: [join(backendDir, "inspect-ui.js")],
    outdir: deployed,
    bundle: true,
    splitting: true,
    format: "esm",
    platform: "node",
    outExtension: { ".js": ".mjs" },
    logLevel: "silent",
  });
  rmSync(packageRoot, { recursive: true, force: true });
  // Each file is its own chunk, loaded when asked for: the code that serves
  // the page doesn't carry the assets.
  const assetBase64 = Buffer.from("console.log('inspector')").toString("base64");
  assert.ok(!readFileSync(join(deployed, "inspect-ui.mjs"), "utf8").includes(assetBase64));

  const { createInspectorUi } = await import(join(deployed, "inspect-ui.mjs"));
  const serve = createInspectorUi();
  const request = new Request("http://localhost:3000/jazz/inspect/");
  const page = await serve(request, "/");
  assert.ok(page, "the page is served");
  assert.match(await page.text(), /Jazz Inspector/);
  // Without the trailing slash (Next.js strips it), its assets still resolve below /inspect/.
  assert.match(await (await serve(request, "")).text(), /src="inspect\/assets\/app-abc\.js"/);
  assert.equal(await (await serve(request, "/embedded.html")).text(), "overlay page");
  assert.equal(
    await (await serve(request, "/assets/app-abc.js")).text(),
    "console.log('inspector')",
  );
  assert.equal(await serve(request, "/assets/missing.js"), null);
});
