/**
 * Custom BrowserCommands for running a second Jazz client in an isolated
 * Playwright BrowserContext.
 *
 * Each command runs on the Node side with full Playwright access. The isolated
 * pages are tracked in a Map keyed by label (e.g. "b").
 */

import { join } from "node:path";
import { createServer } from "node:net";
import type { BrowserCommand } from "vitest/node";
import type { BrowserContext, Page } from "playwright";
import type { LocalJazzServerHandle } from "jazz-tools/testing";
import { startLocalJazzServer, pushSchemaCatalogue } from "jazz-tools/testing";

function findFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.listen(0, () => {
      const port = (srv.address() as { port: number }).port;
      srv.close(() => resolve(port));
    });
    srv.on("error", reject);
  });
}

const isolatedPages = new Map<string, { page: Page; context: BrowserContext }>();

// ---------------------------------------------------------------------------
// openIsolatedApp — create a new BrowserContext + Page and navigate to the
// test harness with config params.
// ---------------------------------------------------------------------------

interface OpenIsolatedAppOpts {
  label: string;
  appId: string;
  dbName: string;
  serverUrl: string;
  playerId?: string;
  physicsSpeed?: number;
  spawnX?: number;
  localAuthToken?: string;
  adminSecret?: string;
}

export const openIsolatedApp: BrowserCommand<[opts: OpenIsolatedAppOpts]> = async (ctx, opts) => {
  const pw = ctx.provider;
  if (pw.name !== "playwright") {
    throw new Error("openIsolatedApp requires the playwright provider");
  }

  // Derive the Vite dev server origin from the test page URL
  const origin = new URL(ctx.page.url()).origin;

  // Get the browser from the existing context to create a new one
  const browser = ctx.context.browser();
  if (!browser) {
    throw new Error("Could not access browser from context");
  }

  const newContext = await browser.newContext();
  const page = await newContext.newPage();

  // Build the harness URL — use the root index.html so all Vite plugin
  // transforms (wasm, topLevelAwait, react) are applied correctly.
  const url = new URL("/index.html", origin);
  url.searchParams.set("appId", opts.appId);
  url.searchParams.set("dbName", opts.dbName);
  url.searchParams.set("serverUrl", opts.serverUrl);
  if (opts.playerId) url.searchParams.set("playerId", opts.playerId);
  if (opts.physicsSpeed !== undefined) {
    url.searchParams.set("physicsSpeed", String(opts.physicsSpeed));
  }
  if (opts.spawnX !== undefined) {
    url.searchParams.set("spawnX", String(opts.spawnX));
  }
  if (opts.localAuthToken) {
    url.searchParams.set("localAuthToken", opts.localAuthToken);
  }
  if (opts.adminSecret) {
    url.searchParams.set("adminSecret", opts.adminSecret);
  }

  // Inject vitest browser runner stub so module transforms work.
  // Vitest's dev server wraps dynamic imports with calls to
  // __vitest_browser_runner__.wrapDynamicImport(). The real
  // implementation checks for __vitest_mocker__ and falls through to
  // a plain call when it's absent, so a simple passthrough stub is
  // sufficient for non-test pages.
  await page.addInitScript(() => {
    const passthrough = (cb: () => unknown) => (typeof cb === "function" ? cb() : cb);
    (window as any).__vitest_browser_runner__ = {
      wrapModule: passthrough,
      wrapDynamicImport: passthrough,
      disposeExceptionTracker: () => {},
      cleanups: [],
    };
  });

  // Log console output from the isolated page for debugging
  page.on("console", (msg) => {
    if (msg.type() === "error" || msg.type() === "warning") {
      console.error(`[isolated:${opts.label}:${msg.type()}] ${msg.text()}`);
    }
  });
  page.on("pageerror", (err) => {
    console.error(`[isolated:${opts.label}] page error:`, err.message);
  });

  await page.goto(url.toString());

  // Wait for the game canvas to render
  await page.waitForSelector('[data-testid="game-canvas"]', {
    timeout: 15000,
  });

  isolatedPages.set(opts.label, { page, context: newContext });
};

// ---------------------------------------------------------------------------
// readIsolatedAttr — read a data attribute from the game container
// ---------------------------------------------------------------------------

export const readIsolatedAttr: BrowserCommand<
  [label: string, attr: string, testId?: string]
> = async (_ctx, label, attr, testId = "game-container") => {
  const entry = isolatedPages.get(label);
  if (!entry) throw new Error(`No isolated page with label "${label}"`);

  const value = await entry.page.locator(`[data-testid="${testId}"]`).getAttribute(`data-${attr}`);

  return value;
};

// ---------------------------------------------------------------------------
// waitForIsolatedAttr — wait until a data attribute matches an expected value
// ---------------------------------------------------------------------------

export const waitForIsolatedAttr: BrowserCommand<
  [label: string, attr: string, expected: string, timeout?: number]
> = async (_ctx, label, attr, expected, timeout = 10000) => {
  const entry = isolatedPages.get(label);
  if (!entry) throw new Error(`No isolated page with label "${label}"`);

  await entry.page.waitForSelector(`[data-testid="game-container"][data-${attr}="${expected}"]`, {
    timeout,
  });
};

// ---------------------------------------------------------------------------
// pressIsolatedKey / releaseIsolatedKey — keyboard input in the isolated page
// ---------------------------------------------------------------------------

export const pressIsolatedKey: BrowserCommand<[label: string, key: string]> = async (
  _ctx,
  label,
  key,
) => {
  const entry = isolatedPages.get(label);
  if (!entry) throw new Error(`No isolated page with label "${label}"`);

  await entry.page.keyboard.down(key);
};

export const releaseIsolatedKey: BrowserCommand<[label: string, key: string]> = async (
  _ctx,
  label,
  key,
) => {
  const entry = isolatedPages.get(label);
  if (!entry) throw new Error(`No isolated page with label "${label}"`);

  await entry.page.keyboard.up(key);
};

// ---------------------------------------------------------------------------
// debugIsolatedState — dump all data attributes from the isolated page
// ---------------------------------------------------------------------------

export const debugIsolatedState: BrowserCommand<[label: string]> = async (_ctx, label) => {
  const entry = isolatedPages.get(label);
  if (!entry) return "no page";

  return await entry.page.evaluate(() => {
    const container = document.querySelector('[data-testid="game-container"]');
    const sync = document.querySelector('[data-testid="sync-debug"]');
    const attrs: Record<string, string | null> = {};
    if (container) {
      for (const attr of container.getAttributeNames()) {
        if (attr.startsWith("data-")) attrs[`container:${attr}`] = container.getAttribute(attr);
      }
    } else {
      attrs["container"] = "NOT FOUND";
    }
    if (sync) {
      for (const attr of sync.getAttributeNames()) {
        if (attr.startsWith("data-")) attrs[`sync:${attr}`] = sync.getAttribute(attr);
      }
    } else {
      attrs["sync"] = "NOT FOUND";
    }
    return JSON.stringify(attrs, null, 2);
  });
};

// ---------------------------------------------------------------------------
// startFreshTestServer / stopFreshTestServer — spin up a dedicated Jazz server
// with an empty event log so tests aren't slowed by accumulated events from
// prior tests.  Each server is identified by a label string.
// ---------------------------------------------------------------------------

const FRESH_ADMIN_SECRET = "test-admin-secret-for-moon-lander-tests";
const FRESH_APP_ID = "00000000-0000-0000-0000-000000000004"; // APP_ID_MULTI

const freshServers = new Map<string, LocalJazzServerHandle>();

// Ensure all fresh servers and isolated pages are cleaned up even if a test
// times out before its finally block runs.
export async function cleanupAll(): Promise<void> {
  await Promise.all([
    ...[...freshServers.values()].map((h) => h.stop().catch(() => {})),
    ...[...isolatedPages.values()].map((e) => e.context.close().catch(() => {})),
  ]);
  freshServers.clear();
  isolatedPages.clear();
}

for (const sig of ["SIGTERM", "SIGINT"] as const) {
  process.on(sig, () => {
    cleanupAll().finally(() => process.exit(0));
  });
}

export const startFreshTestServer: BrowserCommand<[label: string]> = async (_ctx, label) => {
  const port = await findFreePort();
  const handle = await startLocalJazzServer({
    appId: FRESH_APP_ID,
    port,
    adminSecret: FRESH_ADMIN_SECRET,
    allowAnonymous: true,
    enableLogs: false,
  });

  const schemaDir = join(import.meta.dirname ?? __dirname, "../../schema");
  await pushSchemaCatalogue({
    serverUrl: handle.url,
    appId: FRESH_APP_ID,
    adminSecret: FRESH_ADMIN_SECRET,
    schemaDir,
  });

  freshServers.set(label, handle);
  return handle.url;
};

export const stopFreshTestServer: BrowserCommand<[label: string]> = async (_ctx, label) => {
  const handle = freshServers.get(label);
  if (handle) {
    await handle.stop();
    freshServers.delete(label);
  }
};

// ---------------------------------------------------------------------------
// closeIsolatedApp — close page + context and remove from map
// ---------------------------------------------------------------------------

export const closeIsolatedApp: BrowserCommand<[label: string]> = async (_ctx, label) => {
  const entry = isolatedPages.get(label);
  if (!entry) return;

  await entry.page.close().catch(() => {});
  await entry.context.close().catch(() => {});
  isolatedPages.delete(label);
};
