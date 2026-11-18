import type { BrowserContext, Page } from "playwright";
import { evaluateHarnessOperation } from "./evaluate-harness.mjs";
import { jazzAppRequest } from "./testing-server-node.js";

// Another tab of the test page's own browser context: it shares the page's
// origin, so its localStorage, IndexedDB and SharedWorker, as a user's second
// tab does.
const tabs = new Map<string, Page>();
const harness = "/tests/browser/session-tab-harness.ts";

export async function openSessionTab(
  context: BrowserContext,
  currentPage: Page,
  id: string,
  serverUrl: string,
  options: { clearOnSignOut?: boolean; localFirst?: "now"; signedIn?: boolean } = {},
): Promise<void> {
  await closeSessionTab(id);
  const page = await context.newPage();
  tabs.set(id, page);
  await page.exposeFunction("__jazzSessionTabRequest", jazzAppRequest);
  await page.goto(new URL("/tests/browser/remote-db-harness.html", currentPage.url()).toString(), {
    waitUntil: "domcontentloaded",
  });
  await evaluateHarnessOperation(page, harness, "openSessionTab", { serverUrl, ...options });
}

function tab(id: string): Page {
  const page = tabs.get(id);
  if (!page) throw new Error(`No session tab ${id} is open`);
  return page;
}

export function sessionTabState(id: string): Promise<unknown> {
  return evaluateHarnessOperation(tab(id), harness, "sessionTabState", {});
}

export function insertInSessionTab(id: string, title: string): Promise<void> {
  return evaluateHarnessOperation(tab(id), harness, "insertInSessionTab", { title });
}

/** Run one of the tab's actions (see session-tab-harness.ts). */
export function runInSessionTab(
  id: string,
  action: "insertLocally" | "signOut" | "reconnect",
  title?: string,
): Promise<void> {
  const operation = {
    insertLocally: "insertLocallyInSessionTab",
    signOut: "signOutInSessionTab",
    reconnect: "reconnectSessionTab",
  }[action];
  return evaluateHarnessOperation(tab(id), harness, operation, { title });
}

export function holdSessionTabBackend(id: string, hold: boolean): Promise<void> {
  return evaluateHarnessOperation(tab(id), harness, "holdSessionTabBackend", { hold });
}

/** Close the tab as a crash or a discarded tab does: its page just goes away. */
export async function crashSessionTab(id: string): Promise<void> {
  const page = tab(id);
  tabs.delete(id);
  await page.close({ runBeforeUnload: false });
}

export async function closeSessionTab(id: string): Promise<void> {
  const page = tabs.get(id);
  if (!page) return;
  tabs.delete(id);
  await evaluateHarnessOperation(page, harness, "closeSessionTab", {}).catch(() => {});
  await page.close();
}

/** The page locks a Jazz tab holds for its lifetime, by name prefix. */
const JAZZ_PAGE_LOCKS = ["jazz-tab-alive:", "jazz-replica-v1:"];
const cachedPages = new Map<string, { clientIds: string[]; marker: number }>();

/**
 * Navigate the tab to another page of the origin, leaving its page in the
 * back/forward cache (Chromium caches it only with bfcache enabled, as this
 * suite's Chromium has it). Resolves with the Jazz page locks still held by
 * the tab's page while it is away, as the page it went to sees them.
 */
export async function leaveSessionTab(id: string): Promise<{ heldWhileAway: string[] }> {
  const page = tab(id);
  const { clientIds, marker } = await page.evaluate(async (prefixes) => {
    const marker = Math.random();
    (globalThis as { __jazzCacheMarker?: number }).__jazzCacheMarker = marker;
    // A lock query lists every page of the origin: find this page's own
    // client by a lock only it holds, for a moment.
    const probe = `jazz-test-own-client:${marker}`;
    const own = await navigator.locks.request(probe, async () => {
      const held = (await navigator.locks.query()).held ?? [];
      const clientId = held.find((lock) => lock.name === probe)?.clientId;
      return held.filter(
        (lock) =>
          lock.clientId === clientId && prefixes.some((prefix) => lock.name?.startsWith(prefix)),
      );
    });
    return { clientIds: [...new Set(own.map((lock) => lock.clientId!))], marker };
  }, JAZZ_PAGE_LOCKS);
  if (clientIds.length === 0) throw new Error(`Session tab ${id} holds no Jazz page lock`);
  cachedPages.set(id, { clientIds, marker });
  await page.goto(new URL("/tests/browser/remote-db-harness.html?away", page.url()).toString());
  // Let the browser grant what the page let go of.
  await page.waitForTimeout(250);
  const heldWhileAway = await page.evaluate(
    async ({ prefixes, clientIds }) =>
      ((await navigator.locks.query()).held ?? [])
        .filter(
          (lock) =>
            clientIds.includes(lock.clientId!) &&
            prefixes.some((prefix) => lock.name?.startsWith(prefix)),
        )
        .map((lock) => lock.name!),
    { prefixes: JAZZ_PAGE_LOCKS, clientIds },
  );
  return { heldWhileAway };
}

/**
 * Whether this browser keeps a page with a SharedWorker in its back/forward
 * cache, as every local-first tab has one: Chromium 159 does; 145 (that
 * of the Playwright this suite pins) doesn't.
 */
export async function pageCacheKeepsSharedWorkerPages(
  context: BrowserContext,
  currentPage: Page,
): Promise<boolean> {
  const page = await context.newPage();
  try {
    const harness = new URL("/tests/browser/remote-db-harness.html", currentPage.url());
    await page.goto(harness.toString());
    const marker = await page.evaluate(() => {
      const source = URL.createObjectURL(new Blob(["onconnect = () => {};"]));
      new SharedWorker(source).port.start();
      return ((globalThis as { __jazzCacheMarker?: number }).__jazzCacheMarker = Math.random());
    });
    await page.goto(`${harness}?away`);
    await page.goBack({ waitUntil: "commit" });
    await page.waitForFunction(() => document.readyState !== "loading");
    return await page.evaluate(
      (marker) => (globalThis as { __jazzCacheMarker?: number }).__jazzCacheMarker === marker,
      marker,
    );
  } finally {
    await page.close();
  }
}

/**
 * Go back to the page `leaveSessionTab` left. Resolves with whether it came
 * back from the cache (the same page, not a reload) and, if not, why not.
 */
export async function returnToSessionTab(
  id: string,
): Promise<{ restored: boolean; notRestoredReasons: string[] }> {
  const page = tab(id);
  const left = cachedPages.get(id);
  if (!left) throw new Error(`Session tab ${id} did not leave`);
  cachedPages.delete(id);
  // A page shown from the cache fires no load event.
  await page.goBack({ waitUntil: "commit" });
  await page.waitForFunction(() => document.readyState !== "loading");
  return page.evaluate((marker) => {
    const navigation = performance.getEntriesByType("navigation")[0] as
      | (PerformanceNavigationTiming & {
          notRestoredReasons?: { reasons?: Array<{ reason: string }> | null } | null;
        })
      | undefined;
    return {
      restored: (globalThis as { __jazzCacheMarker?: number }).__jazzCacheMarker === marker,
      notRestoredReasons: (navigation?.notRestoredReasons?.reasons ?? []).map((r) => r.reason),
    };
  }, left.marker);
}

/** How the tab's page last loaded ("navigate", "reload", "back_forward"); `null` mid-navigation. */
export async function sessionTabNavigationType(id: string): Promise<string | null> {
  return tab(id)
    .evaluate(() => {
      const entry = performance.getEntriesByType("navigation")[0] as
        | PerformanceNavigationTiming
        | undefined;
      return entry?.type ?? null;
    })
    .catch(() => null);
}
