// Under `vite dev`, Vite pre-bundles jazz-tools into node_modules/.vite/deps.
// Vite 6 and 7 then resolve the broker worker's
// `new URL("../../worker/jazz-broker-worker.js", import.meta.url)` against
// the pre-bundled chunk, which the dev server answers with index.html, so the
// local-first client never starts (#5198). Jazz detects that it runs from the
// pre-bundle and asks the dev server to resolve the worker by package instead.
//
// This starts a real dev server for each supported Vite major on a project
// that installs this package (a copy, so it is pre-bundled like any npm
// dependency), takes the pre-bundled chunk's own URL, and checks that the URL
// Jazz derives from it serves the broker worker as JavaScript.
import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import {
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const packageDir =
  process.env.JAZZ_VITE_CONSUMER_DIR ?? fileURLToPath(new URL("..", import.meta.url));
const require = createRequire(join(packageDir, "package.json"));

const { vitePrebundledBrokerWorkerUrl } = await import(
  pathToFileURL(join(packageDir, "dist/runtime/browser-worker-config.js")).href
);

/** A project that depends on jazz-tools the way an npm install lays it out. */
function createConsumerProject() {
  const root = mkdtempSync(join(tmpdir(), "jazz-vite-prebundle-"));
  writeFileSync(
    join(root, "package.json"),
    JSON.stringify({ name: "vite-prebundle-consumer", private: true, type: "module" }),
  );
  writeFileSync(
    join(root, "index.html"),
    '<!doctype html><html><body><script type="module" src="/src/main.js"></script></body></html>',
  );
  mkdirSync(join(root, "src"));
  writeFileSync(
    join(root, "src/main.js"),
    'import * as localFirst from "jazz-tools/local-first";\nconsole.log(localFirst);\n',
  );
  const modules = join(root, "node_modules");
  const installed = join(modules, "jazz-tools");
  mkdirSync(installed, { recursive: true });
  // A real directory, not a link: Vite only pre-bundles dependencies that
  // live in node_modules, never workspace links.
  cpSync(join(packageDir, "package.json"), join(installed, "package.json"));
  cpSync(join(packageDir, "dist"), join(installed, "dist"), { recursive: true });
  // jazz-tools' own dependencies, as the package manager installed them.
  const ownModules = join(packageDir, "node_modules");
  for (const entry of readdirSync(ownModules)) {
    if (entry === ".bin" || entry === "jazz-tools" || entry.startsWith(".")) continue;
    if (!existsSync(join(modules, entry)))
      symlinkSync(join(ownModules, entry), join(modules, entry));
  }
  return root;
}

const viteMajors = [
  // The workspace's own Vite 8, whose optimizer already rewrites the literal;
  // pre-bundled Jazz takes the dev-server path there too.
  { name: "vite", specifier: "vite", literalBreaks: false },
  // Vite 7, whose optimizer leaves the literal pointing at a missing file.
  { name: "vite 7", specifier: "vite7", literalBreaks: true },
];

for (const { name, specifier, literalBreaks } of viteMajors) {
  test(`${name} dev serves the broker worker to pre-bundled jazz-tools`, async (t) => {
    const { createServer, version } = await import(pathToFileURL(require.resolve(specifier)).href);
    const root = createConsumerProject();
    const server = await createServer({
      root,
      configFile: false,
      logLevel: "silent",
      server: { port: 0, strictPort: false },
      // The documented config: Jazz's Wasm is excluded, jazz-tools is not.
      optimizeDeps: { exclude: ["jazz-wasm"] },
    });
    t.after(async () => {
      await server.close();
      rmSync(root, { recursive: true, force: true });
    });
    await server.listen();
    const origin = server.resolvedUrls.local[0];

    const main = await fetch(new URL("src/main.js", origin));
    assert.equal(main.status, 200);
    const chunkPath = (await main.text()).match(
      /["'](\/node_modules\/\.vite\/deps\/jazz-tools_local-first\.js\?v=[^"']+)["']/,
    )?.[1];
    assert.ok(chunkPath, `Vite ${version} did not pre-bundle jazz-tools/local-first`);
    const chunkUrl = new URL(chunkPath, origin).href;
    const chunk = await fetch(chunkUrl);
    assert.equal(chunk.status, 200);
    const chunkSource = await chunk.text();
    assert.match(
      chunkSource,
      /jazz-tools\/_worker\/broker/,
      "pre-bundle lacks the dev-server worker path",
    );

    // What Vite 7 made of the worker literal in the pre-bundle: a file that
    // doesn't exist (the bug this guards against). Vite 8 places the literal
    // in another chunk, so it is only checked where it is known to break.
    if (literalBreaks) {
      const literalPath = chunkSource.match(
        /new\s+URL\(\s*(?:\/\*[^*]*\*\/\s*)?["']([^"']*jazz-broker-worker[^"']*)["']\s*,\s*import\.meta\.url/,
      )?.[1];
      assert.ok(literalPath, "pre-bundle lacks the worker literal");
      const literal = await fetch(new URL(literalPath, chunkUrl));
      const body = await literal.text();
      assert.ok(
        literal.status !== 200 || !/installJazzBrokerWorker/.test(body),
        `Vite ${version} now resolves the worker literal (${literalPath}); this test no longer reproduces the bug`,
      );
    }

    // Every module of the pre-bundle shares the chunk directory, so this is
    // the module URL Jazz sees at runtime.
    const workerUrl = vitePrebundledBrokerWorkerUrl(chunkUrl);
    assert.equal(workerUrl, new URL("@id/jazz-tools/_worker/broker", origin).href);

    const worker = await fetch(workerUrl);
    assert.equal(worker.status, 200, `Vite ${version} did not serve ${workerUrl}`);
    assert.match(worker.headers.get("content-type") ?? "", /javascript/);
    const source = await worker.text();
    assert.doesNotMatch(source, /<!doctype html>/i);
    assert.match(source, /installJazzBrokerWorker/);
  });
}
