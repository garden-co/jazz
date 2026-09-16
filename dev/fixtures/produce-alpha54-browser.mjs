// Explicit npm-artifact producer. Never run this baseline generator in CI.
import { createServer } from "node:http";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { createRequire } from "node:module";
const require = createRequire(new URL("../../packages/jazz-tools/package.json", import.meta.url));
const { build } = require("esbuild");
const { chromium } = require("playwright");
const [toolsArgument, wasmArgument, outputArgument] = process.argv.slice(2);
if (!outputArgument)
  throw new Error("usage: node produce-alpha54-browser.mjs TOOLS_PACKAGE WASM_PACKAGE NEW_OUTPUT");
const tools = resolve(toolsArgument),
  wasm = resolve(wasmArgument),
  output = resolve(outputArgument);
for (const [root, name] of [
  [tools, "jazz-tools"],
  [wasm, "jazz-wasm"],
]) {
  const pkg = JSON.parse(await readFile(resolve(root, "package.json"), "utf8"));
  if (pkg.name !== name || pkg.version !== "2.0.0-alpha.54")
    throw new Error("requires pinned alpha.54 npm artifacts");
}
await mkdir(output); // Do not overwrite an existing fixture or database.
await build({
  entryPoints: [new URL("./alpha54-browser-page.mjs", import.meta.url).pathname],
  bundle: true,
  format: "esm",
  outfile: resolve(output, "browser-bundle.js"),
  plugins: [
    {
      name: "published-package-only",
      setup(builder) {
        builder.onResolve({ filter: /^published-tools\// }, (args) => ({
          path: resolve(tools, args.path.slice("published-tools/".length)),
        }));
      },
    },
  ],
});
const files = new Map([
  ["/browser-bundle.js", resolve(output, "browser-bundle.js")],
  ["/jazz_wasm_bg.wasm", resolve(wasm, "pkg/jazz_wasm_bg.wasm")],
  ["/jazz-wasm/package/pkg/jazz_wasm_bg.wasm", resolve(wasm, "pkg/jazz_wasm_bg.wasm")],
  [
    "/jazz-tools/package/dist/worker/jazz-broker-worker.js",
    resolve(tools, "dist/worker/jazz-broker-worker.js"),
  ],
]);
const server = createServer(async (request, response) => {
  try {
    const pathname = new URL(request.url, "http://localhost").pathname;
    const file = files.get(pathname);
    if (pathname !== "/" && !file) {
      response.writeHead(404).end();
      return;
    }
    response.setHeader(
      "Content-Type",
      file?.endsWith(".wasm")
        ? "application/wasm"
        : file?.endsWith(".js")
          ? "text/javascript"
          : "text/html",
    );
    response.end(
      file ? await readFile(file) : '<script type="module" src="/browser-bundle.js"></script>',
    );
  } catch (error) {
    response.writeHead(500).end(String(error));
  }
});
await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
let browser;
try {
  browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const page = await browser.newPage();
  page.setDefaultTimeout(30_000);
  await page.goto(`http://127.0.0.1:${server.address().port}`);
  await page.waitForFunction(() => window.run);
  const result = await Promise.race([
    page.evaluate(() => window.run()),
    new Promise((_, reject) => {
      const timer = setTimeout(
        () => reject(new Error("published browser producer timed out")),
        30_000,
      );
      timer.unref();
    }),
  ]);
  await writeFile(
    resolve(output, "browser-corpus.json"),
    JSON.stringify(result.records, null, 2) + "\n",
    {
      flag: "wx",
    },
  );
  await writeFile(
    resolve(output, "browser-receipt.json"),
    JSON.stringify({ chromium: browser.version(), rows: result.rows }, null, 2) + "\n",
    { flag: "wx" },
  );
  console.log("Published alpha.54 browser producer and reopen completed", browser.version());
} finally {
  await browser?.close();
  await new Promise((resolve) => server.close(resolve));
}
