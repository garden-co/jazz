import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { createServer } from "vite";
import react from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";
import { chromium } from "@playwright/test";
import { bench, beforeAll, afterAll } from "vitest";
import { fixtures } from "./fixture.mjs";
import { textEditorCases } from "./metadata.ts";

const root = fileURLToPath(new URL("../", import.meta.url));
const data = await fixtures();
// Expose the existing App's database only in this benchmark's Vite transform.
// The actual editor and provider are used unchanged.
const server = await createServer({
  root,
  configFile: false,
  mode: "test",
  server: { port: 0, host: "127.0.0.1" },
  plugins: [
    {
      name: "text-editor-benchmark",
      enforce: "pre",
      transform(code, id) {
        if (!id.endsWith("/src/main.tsx")) return;
        const anchor = "function App() {\n  const db = useDb();";
        assert.equal(code.split(anchor).length, 2, "benchmark App hook must match once");
        return code.replace(
          anchor,
          `${anchor}\n  window.__textEditorBenchmark = { db, app, serverUrl: import.meta.env.VITE_JAZZ_SERVER_URL, readText: () => { const content = document.querySelector(".cm-content"); return content ? EditorView.findFromDOM(content)?.state.doc.toString() : null; } };`,
        );
      },
      configureServer(vite) {
        vite.middlewares.use(async (req, res, next) => {
          const fixture = data.find((item) => req.url === `/__benchmark_fixture/${item.edits}`);
          if (!fixture) return next();
          try {
            res.setHeader("Content-Type", "application/octet-stream");
            res.end(await readFile(fixture.path));
          } catch (error) {
            next(error);
          }
        });
      },
    },
    react(),
    jazzPlugin({ server: { inMemory: true }, inspector: false }),
  ],
});
let browser;
const contexts = new Set();
const errors = [];
const rounds = Number(process.env.BENCH_ROUNDS || 3);
assert.ok(Number.isInteger(rounds) && rounds > 0);
const filter = process.env.BENCH_FILTER || "";
const editor = (page) => page.getByRole("textbox", { name: "Document" });
async function context() {
  const ctx = await browser.newContext();
  contexts.add(ctx);
  ctx.on("page", (page) =>
    page.on("pageerror", (error) => errors.push(error.stack || String(error))),
  );
  return ctx;
}
async function loaded(page, length) {
  await page.waitForFunction(
    (length) => {
      const editor = document.querySelector('.cm-content[contenteditable="true"]');
      return !!editor && window.__textEditorBenchmark.readText()?.length === length;
    },
    length,
    { timeout: 60_000 },
  );
}
async function close(ctx) {
  await ctx.close();
  contexts.delete(ctx);
}
let base, seedPage;
const ids = new Map();
beforeAll(async () => {
  await server.listen();
  base = `http://127.0.0.1:${server.httpServer.address().port}/`;
  browser = await chromium.launch();
  const writer = await context();
  seedPage = await writer.newPage();
  await seedPage.goto(base);
  await seedPage.waitForFunction(() => window.__textEditorBenchmark);
  for (const fixture of data) ids.set(fixture.edits, await seed(fixture));
}, 120_000);

afterAll(async () => {
  for (const ctx of contexts) await ctx.close();
  await browser?.close();
  await server.close();
});

async function seed(fixture) {
  return seedPage.evaluate(
    async ({ edits, sha256 }) => {
      const { db, app } = window.__textEditorBenchmark;
      const bytes = new Uint8Array(
        await (await fetch(`/__benchmark_fixture/${edits}`)).arrayBuffer(),
      );
      const actual = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)), (b) =>
        b.toString(16).padStart(2, "0"),
      ).join("");
      if (actual !== sha256) throw new Error("Seed byte checksum mismatch");
      const doc = await db.insert(app.documents, { title: "Paper trace" }).wait({ tier: "global" });
      await db
        .insert(app.documentLogs, { documentId: doc.id, contentLog: bytes })
        .wait({ tier: "global" });
      return doc.id;
    },
    { edits: fixture.edits, sha256: fixture.sha256 },
  );
}

const scenarios = textEditorCases.filter(({ name }) => name.includes(filter));
assert.ok(scenarios.length, "BENCH_FILTER must match a case");
for (const scenario of scenarios) {
  const fixture = data.find(({ edits }) => edits === scenario.edits);
  let ctx, page, peerContext, peer, url;
  bench(
    scenario.name,
    async () => {
      if (scenario.operation === "edit") {
        await page.keyboard.insertText("x");
        await loaded(peer, fixture.text.length + 1);
      } else {
        if (scenario.operation === "reload") await page.reload({ waitUntil: "domcontentloaded" });
        else await page.goto(url, { waitUntil: "domcontentloaded" });
        await loaded(page, fixture.text.length);
      }
    },
    {
      iterations: rounds,
      time: 0,
      warmupIterations: 0,
      warmupTime: 0,
      setup(task) {
        // Vitest passes options to Bench, so install per-sample hooks on its Task.
        task.opts.beforeEach = async () => {
          ctx = await context();
          page = await ctx.newPage();
          const id = scenario.operation === "edit" ? await seed(fixture) : ids.get(scenario.edits);
          url = `${base}#${id}`;
          if (scenario.operation !== "open") {
            await page.goto(url);
            await loaded(page, fixture.text.length);
          }
          if (scenario.operation === "reload") {
            await page.evaluate(() => window.__textEditorBenchmark.db.disconnect());
            const jazzHost = new URL(
              await page.evaluate(() => window.__textEditorBenchmark.serverUrl),
            ).host;
            await ctx.routeWebSocket(
              (url) => url.host === jazzHost,
              (socket) => socket.close(),
            );
          } else if (scenario.operation === "edit") {
            peerContext = await context();
            peer = await peerContext.newPage();
            await peer.goto(url);
            await loaded(peer, fixture.text.length);
            await editor(page).click();
            await editor(page).press("ControlOrMeta+End");
          }
        };
        task.opts.afterEach = async () => {
          const expected = fixture.text + (scenario.operation === "edit" ? "x" : "");
          assert.equal(
            await page.evaluate(() => window.__textEditorBenchmark.readText()),
            expected,
          );
          if (peer)
            assert.equal(
              await peer.evaluate(() => window.__textEditorBenchmark.readText()),
              expected,
            );
          assert.equal(errors.length, 0, errors[0]);
          if (peerContext) {
            await close(peerContext);
            peerContext = undefined;
            peer = undefined;
          }
          await close(ctx);
        };
      },
    },
  );
}
