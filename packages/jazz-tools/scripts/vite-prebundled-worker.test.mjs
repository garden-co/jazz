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
import { createServer as createNetServer } from "node:net";
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
    `import * as localFirst from "jazz-tools/local-first";
import { schema as s } from "jazz-tools";
import { createJazzClient } from "jazz-tools/client";

// The local-first client on this device only, as an app's first run opens
// it: no backend, IndexedDB through the broker SharedWorker.
const app = s.defineApp({ todos: s.table({ title: s.string() }, {}) }, { migrations: "none" });
window.__jazzResult = (async () => {
  const session = await createJazzClient({ serverUrl: null, client: async () => localFirst });
  const db = session.getSnapshot().client.db;
  await db.insert(app.todos, { title: "written under vite dev" }).wait({ tier: "local" });
  const rows = await db.all(app.todos);
  return { localFirst: db.isLocalFirst(), titles: rows.map((row) => row.title) };
})().catch((error) => ({ error: String(error?.name) + ": " + String(error?.message) }));
`,
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

/**
 * A port nothing listens on. Vite treats `port: 0` as its default 5173, and
 * other suites may hold that one.
 */
async function freePort() {
  const probe = createNetServer();
  await new Promise((resolve, reject) => {
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", resolve);
  });
  const { port } = probe.address();
  await new Promise((resolve) => probe.close(resolve));
  return port;
}

/** Every chunk of the pre-bundle reachable from `entryUrl`, with its source. */
async function prebundleChunks(entryUrl) {
  const chunks = new Map();
  const pending = [entryUrl];
  while (pending.length > 0) {
    const url = pending.pop();
    if (chunks.has(url)) continue;
    const response = await fetch(url);
    assert.equal(response.status, 200, `pre-bundle chunk ${url}`);
    const source = await response.text();
    chunks.set(url, source);
    for (const [, path] of source.matchAll(
      /["'](\/node_modules\/\.vite\/deps\/[^"'?]+\.js(?:\?[^"']*)?)["']/g,
    )) {
      pending.push(new URL(path, url).href);
    }
  }
  return chunks;
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
      // An explicit IPv4 loopback: `localhost` may bind only ::1.
      server: {
        host: "127.0.0.1",
        port: await freePort(),
        strictPort: true,
        // jazz-wasm and jazz-tools' dependencies are links into the workspace.
        fs: { strict: false },
      },
      // The documented config: Jazz's Wasm is excluded, jazz-tools is not.
      optimizeDeps: { exclude: ["jazz-wasm"] },
    });
    t.after(async () => {
      await server.close();
      rmSync(root, { recursive: true, force: true });
    });
    await server.listen();
    const { address, port } = server.httpServer.address();
    const origin = `http://${address}:${port}/`;

    const main = await fetch(new URL("src/main.js", origin));
    assert.equal(main.status, 200);
    const chunkPath = (await main.text()).match(
      /["'](\/node_modules\/\.vite\/deps\/jazz-tools_local-first\.js\?v=[^"']+)["']/,
    )?.[1];
    assert.ok(chunkPath, `Vite ${version} did not pre-bundle jazz-tools/local-first`);
    const chunkUrl = new URL(chunkPath, origin).href;
    const chunks = await prebundleChunks(chunkUrl);
    const withWorkerPath = [...chunks].find(([, source]) =>
      /jazz-tools\/_worker\/broker/.test(source),
    );
    assert.ok(withWorkerPath, "pre-bundle lacks the dev-server worker path");

    // What Vite 7 made of the worker literal in the pre-bundle: a file that
    // doesn't exist (the bug this guards against). Vite 8 resolves it.
    if (literalBreaks) {
      let literalUrl;
      for (const [url, source] of chunks) {
        const path = source.match(
          /new\s+URL\(\s*(?:\/\*[^*]*\*\/\s*)?["']([^"']*jazz-broker-worker[^"']*)["']\s*,\s*import\.meta\.url/,
        )?.[1];
        if (path) literalUrl = new URL(path, url).href;
      }
      assert.ok(literalUrl, "pre-bundle lacks the worker literal");
      const literal = await fetch(literalUrl);
      const body = await literal.text();
      assert.ok(
        literal.status !== 200 || !/installJazzBrokerWorker/.test(body),
        `Vite ${version} now resolves the worker literal (${literalUrl}); this test no longer reproduces the bug`,
      );
    }

    // Every module of the pre-bundle shares the chunk directory, so this is
    // the module URL Jazz sees at runtime.
    const workerUrl = vitePrebundledBrokerWorkerUrl(withWorkerPath[0]);
    assert.equal(
      workerUrl,
      new URL("@id/jazz-tools/_worker/broker?worker_file&type=module", origin).href,
    );

    // The export is a dev-only entry whose one import is the bundled worker.
    // The dev server serves it either as is, importing the worker, or with
    // the worker inlined by its own dependency optimizer.
    const entry = await fetch(workerUrl);
    assert.equal(entry.status, 200, `Vite ${version} did not serve ${workerUrl}`);
    assert.match(entry.headers.get("content-type") ?? "", /javascript/);
    let source = await entry.text();
    let workerModuleUrl = workerUrl;
    assert.doesNotMatch(source, /<!doctype html>/i);
    if (!/installJazzBrokerWorker/.test(source)) {
      const bundled = [...source.matchAll(/^\s*import\s+["']([^"']+)["']/gm)]
        .map((match) => match[1])
        .find((specifier) => /jazz-broker-worker\.js/.test(specifier));
      assert.ok(bundled, `the worker entry neither contains nor imports the worker:\n${source}`);
      workerModuleUrl = new URL(bundled, workerUrl).href;
      const worker = await fetch(workerModuleUrl);
      assert.equal(worker.status, 200);
      assert.match(worker.headers.get("content-type") ?? "", /javascript/);
      source = await worker.text();
    }
    assert.match(source, /installJazzBrokerWorker/);

    // The worker loads its WASM through wasm-bindgen's own
    // `new URL("jazz_wasm_bg.wasm", import.meta.url)`. Had the dev server
    // pre-bundled the worker too, that URL would point into .vite/deps and
    // answer index.html: the worker would start and every write would fail.
    const wasmPath = source.match(
      /new\s+URL\(\s*(?:\/\*[^*]*\*\/\s*)?["']([^"']*jazz_wasm_bg\.wasm[^"']*)["']\s*,\s*(?:["']{2}\s*\+\s*)?import\.meta\.url/,
    )?.[1];
    assert.ok(wasmPath, "the worker names no WASM URL");
    const wasm = await fetch(new URL(wasmPath, workerModuleUrl));
    assert.equal(
      wasm.status,
      200,
      `Vite ${version} did not serve the worker's WASM at ${wasmPath}`,
    );
    const wasmBytes = new Uint8Array(await wasm.arrayBuffer());
    assert.deepEqual(
      [...wasmBytes.subarray(0, 4)],
      [0x00, 0x61, 0x73, 0x6d],
      `the worker's WASM at ${wasmPath} is not WebAssembly`,
    );

    // And end to end, in a browser: open the local-first client, write, read.
    const { chromium } = require("playwright");
    const browser = await chromium.launch();
    t.after(() => browser.close());
    const page = await browser.newPage();
    await page.goto(origin);
    const result = await page.evaluate(() => window.__jazzResult);
    assert.deepEqual(result, { localFirst: true, titles: ["written under vite dev"] });
  });
}
