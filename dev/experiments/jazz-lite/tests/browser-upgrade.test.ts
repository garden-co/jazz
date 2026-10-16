/**
 * Real-browser lite → full upgrade: boot cost of full Jazz (lazy chunk, WASM,
 * worker, account, first handover) under network profiles, cold and warm.
 * Prints numbers and asserts the swap never shows an empty or shrinking list.
 * Run with JAZZ_LITE_BROWSER=1 after `npm run sizes` (which builds dist-sizes/upgrade).
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import path from "node:path";
import { fileURLToPath } from "node:url";
import zlib from "node:zlib";
import fs from "node:fs";
import { preview, type Plugin, type PreviewServer } from "vite";
import { chromium, type Browser } from "playwright-core";
import { app } from "../app/schema.js";
import { startHarness, type Harness } from "./harness.js";

const enabled = process.env.JAZZ_LITE_BROWSER === "1";
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CHROME = process.env.CHROME_PATH ?? "/opt/pw-browsers/chromium-1194/chrome-linux/chrome";

interface Profile {
  name: string;
  /** Throughput in Mbit/s and added round-trip latency in ms; undefined = unthrottled. */
  network?: { mbps: number; latencyMs: number };
  cpuSlowdown?: number;
}

const PROFILES: Profile[] = [
  { name: "desktop, unthrottled" },
  { name: "fast 4G (9 Mbps, 60 ms)", network: { mbps: 9, latencyMs: 60 } },
  {
    name: "slow 4G + 4x CPU (1.6 Mbps, 150 ms)",
    network: { mbps: 1.6, latencyMs: 150 },
    cpuSlowdown: 4,
  },
];

/** Serve built assets brotli-compressed, as a CDN would (Vite preview does not compress). */
function brotliAssets(dir: string): Plugin {
  const cache = new Map<string, Buffer>();
  // Compress up front so the first measured run does not pay for brotli q11 of the WASM.
  for (const f of fs.readdirSync(path.join(dir, "assets"))) {
    const file = path.join(dir, "assets", f);
    const raw = fs.readFileSync(file);
    cache.set(
      file,
      zlib.brotliCompressSync(raw, {
        params: {
          [zlib.constants.BROTLI_PARAM_QUALITY]: 11,
          [zlib.constants.BROTLI_PARAM_SIZE_HINT]: raw.length,
        },
      }),
    );
  }
  return {
    name: "brotli-assets",
    configurePreviewServer(server) {
      server.middlewares.use((req, res, next) => {
        const url = (req.url ?? "").split("?")[0]!;
        const file = path.join(dir, url);
        if (!url.startsWith("/assets/") || !fs.existsSync(file)) return next();
        let body = cache.get(file);
        if (!body) {
          const raw = fs.readFileSync(file);
          body = zlib.brotliCompressSync(raw, {
            params: {
              [zlib.constants.BROTLI_PARAM_QUALITY]: 11,
              [zlib.constants.BROTLI_PARAM_SIZE_HINT]: raw.length,
            },
          });
          cache.set(file, body);
        }
        const type = url.endsWith(".wasm") ? "application/wasm" : "text/javascript";
        res.setHeader("Content-Type", type);
        res.setHeader("Content-Encoding", "br");
        res.setHeader("Cache-Control", "public, max-age=31536000, immutable");
        res.end(body);
      });
    },
  };
}

let harness: Harness;
let server: PreviewServer;
let browser: Browser;
let pageBase: string;

describe.skipIf(!enabled)("lite → full upgrade in Chromium", () => {
  beforeAll(async () => {
    harness = await startHarness();
    server = await preview({
      root,
      configFile: false,
      logLevel: "warn",
      plugins: [brotliAssets(path.join(root, "dist-sizes/upgrade"))],
      build: { outDir: path.join(root, "dist-sizes/upgrade") },
      preview: { port: 0, host: "127.0.0.1" },
    });
    const address = server.httpServer.address() as { port: number };
    pageBase = `http://127.0.0.1:${address.port}/index-upgrade.html`;
    browser = await chromium.launch({ executablePath: CHROME });
  }, 180_000);

  afterAll(async () => {
    await browser?.close();
    await new Promise((resolve) => server?.httpServer.close(resolve));
    await harness?.stop();
  });

  for (const profile of PROFILES) {
    it(`boots full Jazz behind a live lite view: ${profile.name}`, async () => {
      const token = await harness.identity(`browser-${profile.name}`);
      const seed = harness.lite(token);
      await seed.ready();
      for (let i = 0; i < 20; i++) {
        await seed.insert(
          app.todos,
          { title: `seed-${i}`, done: false, owner_id: seed.accountId! },
          { tier: "local" },
        );
      }
      const url =
        `${pageBase}?proxy=${encodeURIComponent(harness.proxyUrl)}&token=${encodeURIComponent(token)}` +
        `&appId=${harness.upstream.appId}&server=${encodeURIComponent(harness.upstream.url)}`;

      const context = await browser.newContext();
      const page = await context.newPage();
      const cdp = await context.newCDPSession(page);
      if (profile.network) {
        await cdp.send("Network.enable");
        const bytesPerSecond = (profile.network.mbps * 1_000_000) / 8;
        await cdp.send("Network.emulateNetworkConditions", {
          offline: false,
          latency: profile.network.latencyMs,
          downloadThroughput: bytesPerSecond,
          uploadThroughput: bytesPerSecond,
        });
      }
      if (profile.cpuSlowdown)
        await cdp.send("Emulation.setCPUThrottlingRate", { rate: profile.cpuSlowdown });
      page.on("pageerror", (error) => console.error("[page]", error));

      let expected = 20;
      const run = async (label: string) => {
        await page.goto(url);
        await page.waitForFunction(
          (n) => (window as any).__jazzLite?.results.some((r: any) => r.rows === n),
          expected,
          {
            timeout: 120_000,
          },
        );
        const swap = await page.evaluate(() => (window as any).__jazzLite.upgrade());
        const state = await page.evaluate(() => ({
          marks: (window as any).__jazzLite.marks,
          results: (window as any).__jazzLite.results,
        }));
        const { marks, results } = state as {
          marks: Record<string, number>;
          results: Array<{ rows: number; mode: string }>;
        };
        const firstFull = results.findIndex((r) => r.rows === expected);
        expect(results.slice(firstFull).every((r) => r.rows === expected)).toBe(true);
        expect(swap.handovers.every((h: { forced: boolean }) => !h.forced)).toBe(true);
        console.log(
          `[browser] ${profile.name}, ${label}: lite first result ${marks.firstResult} ms; ` +
            `full Jazz: import ${marks.fullImported - marks.fullImportStart} ms, ` +
            `account (incl. WASM fetch + compile) ${marks.fullAccount - marks.fullImported} ms, ` +
            `Db ready ${marks.fullDbReady - marks.fullAccount} ms, ` +
            `handover ${marks.handedOver - marks.fullDbReady} ms ` +
            `(upgrade total ${marks.handedOver - marks.fullImportStart} ms); renders ${results.length}`,
        );
        // Writes after the swap are local-first again.
        const before = await page.evaluate(() => performance.now());
        await page.evaluate(() => (window as any).__jazzLite.insert("after-swap"));
        const after = await page.evaluate(() => performance.now());
        expected++;
        console.log(
          `[browser] ${profile.name}, ${label}: insert after swap resolved in ${(after - before).toFixed(1)} ms`,
        );
      };
      await run("cold");
      await run("warm (reload: HTTP cache + IndexedDB)");
      await context.close();
    }, 600_000);
  }
});
