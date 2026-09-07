import type { Browser, BrowserContext, Page } from "playwright";
import type {
  RemoteBrowserDbCreateInput,
  RemoteBrowserDbWaitForTitleInput,
} from "./remote-db-harness.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";

interface RemoteBrowserDbHandle {
  context: BrowserContext;
  anchorPage: Page | null;
  pages: Page[];
  input: RemoteBrowserDbCreateInput;
  harnessUrl: string;
  lifecycle: string[];
}

const HARNESS_LOAD_COUNT_KEY = "jazz-test:remote-harness-load-count";
const STORAGE_INVALIDATION_RELOAD_MARKER = "jazz:indexeddb-invalidation-reload";

const remoteBrowserDbs = new Map<string, RemoteBrowserDbHandle>();
const remoteHarnessModulePath = "/tests/browser/remote-db-harness.ts";

interface CdpFrameEvent {
  frameId?: string;
  frame?: { id?: string; parentId?: string; url?: string };
  url?: string;
  reason?: string;
}

interface CdpExecutionContextEvent {
  executionContextId?: number;
  context?: { id?: number; auxData?: { frameId?: string; isDefault?: boolean } };
}

function recordRemoteBrowserDbLifecycle(lifecycle: string[], event: string): void {
  lifecycle.push(event);
  // A failing page can emit an unbounded stream of browser console messages.
  // Keep the receipt deterministic and bounded while preserving its causal
  // lifecycle order.
  if (lifecycle.length > 32) lifecycle.splice(0, lifecycle.length - 32);
}

function urlWithoutQuery(url: string): string {
  try {
    const parsed = new URL(url);
    return `${parsed.origin}${parsed.pathname}`;
  } catch {
    return "unavailable";
  }
}

function hasSameHost(first: string, second: string): boolean {
  try {
    return new URL(first).host === new URL(second).host;
  } catch {
    return false;
  }
}

function boundedRemoteBrowserDbErrorReceipt(error: unknown): string {
  const value = error instanceof Error ? error : new Error(String(error));
  // Vitest's browser-command transport serializes the outer error message but
  // discards its `cause`. Keep the evaluated harness failure in that message,
  // with one-line bounded fields so a timeout receipt remains useful and safe
  // for the synthetic browser fixtures.
  const oneLine = (text: string, limit: number) => text.replace(/[\r\n\t]+/g, " ").slice(0, limit);
  return [
    `name=${oneLine(value.name, 160)}`,
    `message=${oneLine(value.message, 1_200)}`,
    `stack=${oneLine(value.stack ?? "unavailable", 2_400)}`,
  ].join(" ");
}

async function observeRemoteBrowserDbPage(
  page: Page,
  lifecycle: string[],
  tabIndex: number,
): Promise<void> {
  const label = `tab=${tabIndex}`;
  let mainFrame = page.mainFrame();
  page.on("framenavigated", (frame) => {
    if (frame !== page.mainFrame()) return;
    mainFrame = frame;
    recordRemoteBrowserDbLifecycle(
      lifecycle,
      `${label} navigated url=${urlWithoutQuery(frame.url())} at=${Date.now()}`,
    );
  });
  page.on("request", (request) => {
    if (!request.isNavigationRequest() || request.frame() !== mainFrame) return;
    recordRemoteBrowserDbLifecycle(
      lifecycle,
      `${label} navigation-request url=${urlWithoutQuery(request.url())} at=${Date.now()}`,
    );
  });
  page.on("frameattached", (frame) =>
    recordRemoteBrowserDbLifecycle(
      lifecycle,
      `${label} frame-attached main=${frame === page.mainFrame()} at=${Date.now()}`,
    ),
  );
  page.on("framedetached", (frame) =>
    recordRemoteBrowserDbLifecycle(
      lifecycle,
      `${label} frame-detached main=${frame === mainFrame} at=${Date.now()}`,
    ),
  );
  page.on("domcontentloaded", () =>
    recordRemoteBrowserDbLifecycle(lifecycle, `${label} domcontentloaded at=${Date.now()}`),
  );
  page.on("load", () =>
    recordRemoteBrowserDbLifecycle(lifecycle, `${label} load at=${Date.now()}`),
  );
  page.on("close", () => recordRemoteBrowserDbLifecycle(lifecycle, `${label} closed`));
  page.on("crash", () => recordRemoteBrowserDbLifecycle(lifecycle, `${label} crashed`));
  page.on("pageerror", () => recordRemoteBrowserDbLifecycle(lifecycle, `${label} pageerror`));
  page.on("console", (message) =>
    recordRemoteBrowserDbLifecycle(lifecycle, `${label} console=${message.type()}`),
  );

  page.on("websocket", (socket) => {
    if (!hasSameHost(socket.url(), page.url())) return;
    socket.on("framereceived", (frame) => {
      if (typeof frame.payload !== "string") return;
      try {
        const type = (JSON.parse(frame.payload) as { type?: unknown }).type;
        if (type === "full-reload" || type === "update") {
          recordRemoteBrowserDbLifecycle(lifecycle, `${label} vite-hmr=${type} at=${Date.now()}`);
        }
      } catch {
        // Never record websocket payloads: sync traffic may carry application data.
      }
    });
  });

  const browser = page.context().browser();
  if (browser?.browserType().name() !== "chromium") return;
  try {
    const cdp = await page.context().newCDPSession(page);
    let mainFrameId: string | undefined;
    const mainExecutionContextIds = new Set<number>();
    cdp.on("Page.frameNavigated", (event: CdpFrameEvent) => {
      if (!event.frame || event.frame.parentId) return;
      mainFrameId = event.frame.id;
      recordRemoteBrowserDbLifecycle(
        lifecycle,
        `${label} cdp-main-navigated url=${urlWithoutQuery(event.frame.url ?? "")} at=${Date.now()}`,
      );
    });
    cdp.on("Page.frameStartedLoading", (event: CdpFrameEvent) => {
      if (mainFrameId && event.frameId !== mainFrameId) return;
      recordRemoteBrowserDbLifecycle(lifecycle, `${label} cdp-main-loading at=${Date.now()}`);
    });
    cdp.on("Page.frameRequestedNavigation", (event: CdpFrameEvent) => {
      if (mainFrameId && event.frameId !== mainFrameId) return;
      recordRemoteBrowserDbLifecycle(
        lifecycle,
        `${label} cdp-navigation-requested reason=${event.reason ?? "unknown"} url=${urlWithoutQuery(event.url ?? "")} at=${Date.now()}`,
      );
    });
    cdp.on("Page.frameScheduledNavigation", (event: CdpFrameEvent) => {
      if (mainFrameId && event.frameId !== mainFrameId) return;
      recordRemoteBrowserDbLifecycle(
        lifecycle,
        `${label} cdp-navigation-scheduled reason=${event.reason ?? "unknown"} url=${urlWithoutQuery(event.url ?? "")} at=${Date.now()}`,
      );
    });
    cdp.on("Runtime.executionContextCreated", (event: CdpExecutionContextEvent) => {
      if (
        event.context?.auxData?.frameId !== mainFrameId ||
        !event.context.auxData.isDefault ||
        event.context.id === undefined
      ) {
        return;
      }
      mainExecutionContextIds.add(event.context.id);
      recordRemoteBrowserDbLifecycle(
        lifecycle,
        `${label} cdp-main-context-created at=${Date.now()}`,
      );
    });
    cdp.on("Runtime.executionContextDestroyed", (event: CdpExecutionContextEvent) => {
      if (!event.executionContextId || !mainExecutionContextIds.delete(event.executionContextId))
        return;
      recordRemoteBrowserDbLifecycle(
        lifecycle,
        `${label} cdp-main-context-destroyed at=${Date.now()}`,
      );
    });
    cdp.on("Runtime.executionContextsCleared", () => {
      mainExecutionContextIds.clear();
      recordRemoteBrowserDbLifecycle(lifecycle, `${label} cdp-contexts-cleared at=${Date.now()}`);
    });
    await Promise.all([cdp.send("Page.enable"), cdp.send("Runtime.enable")]);
  } catch {
    // CDP is diagnostic-only: an unavailable session must not change fixture behavior.
    recordRemoteBrowserDbLifecycle(lifecycle, `${label} cdp-unavailable at=${Date.now()}`);
  }
}

async function remoteBrowserDbLifecycleReceipt(handle: RemoteBrowserDbHandle): Promise<string> {
  const page = handle.pages[0];
  if (!page) return `remote-page=missing lifecycle=${JSON.stringify(handle.lifecycle)}`;
  const pageUrl = page.url();
  const closed = page.isClosed();
  const pageState = closed
    ? { loadCount: "unavailable", invalidationReloadMarker: "unavailable" }
    : await page
        .evaluate(
          ({ loadCountKey, invalidationReloadKey }) => ({
            loadCount: sessionStorage.getItem(loadCountKey) ?? "0",
            invalidationReloadMarker: sessionStorage.getItem(invalidationReloadKey) ?? "absent",
          }),
          {
            loadCountKey: HARNESS_LOAD_COUNT_KEY,
            invalidationReloadKey: STORAGE_INVALIDATION_RELOAD_MARKER,
          },
        )
        .catch(() => ({ loadCount: "unavailable", invalidationReloadMarker: "unavailable" }));
  return [
    `remote-page-url=${pageUrl}`,
    `closed=${closed}`,
    `harness-load-count=${pageState.loadCount}`,
    `invalidation-reload-marker=${pageState.invalidationReloadMarker}`,
    `lifecycle=${JSON.stringify(handle.lifecycle)}`,
  ].join(" ");
}

function getBrowserFromContext(context: BrowserContext): Browser {
  const browser = context.browser();
  if (!browser) {
    throw new Error("Expected an attached Playwright browser for remote browser db commands");
  }
  return browser;
}

function harnessUrlFromPage(page: Page): string {
  const currentUrl = page.url();
  if (!currentUrl) {
    throw new Error("Expected current test page to have a URL before opening remote browser db");
  }
  return new URL("/tests/browser/remote-db-harness.html", currentUrl).toString();
}

async function evaluateHarness<TArgs, TResult>(
  page: Page,
  moduleMethod: string,
  args: TArgs,
): Promise<TResult> {
  return page.evaluate(
    async ({ moduleMethod, args, modulePath }) => {
      const harness = await import(/* @vite-ignore */ modulePath);
      const method = (harness as Record<string, (value: TArgs) => Promise<TResult>>)[moduleMethod];
      if (typeof method !== "function") {
        throw new Error(`Remote browser harness method "${moduleMethod}" is unavailable`);
      }
      return method(args);
    },
    { moduleMethod, args, modulePath: remoteHarnessModulePath },
  );
}

export async function createRemoteBrowserDb(
  currentContext: BrowserContext,
  currentPage: Page,
  input: RemoteBrowserDbCreateInput,
): Promise<string> {
  await closeRemoteBrowserDb(input.id);

  // Every page in a Playwright BrowserContext has its own ES-module realm.
  // The browser fixture's implicit-account cache is consequently page-local;
  // relying on it here would make tabs pointed at one physical root present
  // different account owners to the SharedWorker. Give this remote fixture one
  // opaque local-first credential for its whole lifetime instead.
  const resolvedInput =
    input.jwtToken || input.localFirstSecret
      ? input
      : { ...input, localFirstSecret: generateAuthSecret() };

  const browser = getBrowserFromContext(currentContext);
  const remoteContext = await browser.newContext();
  const lifecycle: string[] = [];
  await remoteContext.addInitScript((key) => {
    const count = Number(sessionStorage.getItem(key) ?? 0);
    sessionStorage.setItem(key, String(count + 1));
  }, HARNESS_LOAD_COUNT_KEY);
  // WebKit tears down a page-less remote context while restart tests close
  // every Jazz-owning page. Keep an inert, opaque-origin page there; it never
  // joins the harness origin's agent cluster or connects to the SharedWorker.
  //
  // Do not retain this page in Firefox. Closed pages there otherwise keep
  // their large page-local WASM realms alive long enough for a restart soak to
  // exhaust the browser process before GC catches up.
  const anchorPage =
    browser.browserType().name() === "webkit" ? await remoteContext.newPage() : null;
  if (anchorPage) {
    await anchorPage.goto("data:text/html,<title>remote-browser-db-anchor</title>", {
      waitUntil: "domcontentloaded",
    });
  }
  const pages: Page[] = [];
  for (let index = 0; index < (resolvedInput.tabCount ?? 1); index += 1) {
    const page = await remoteContext.newPage();
    await observeRemoteBrowserDbPage(page, lifecycle, index);
    await page.goto(harnessUrlFromPage(currentPage), { waitUntil: "domcontentloaded" });
    await evaluateHarness(page, "createRemoteBrowserDb", {
      ...resolvedInput,
      initialRow: index === 0 ? resolvedInput.initialRow : undefined,
    });
    pages.push(page);
  }

  remoteBrowserDbs.set(input.id, {
    context: remoteContext,
    anchorPage,
    pages,
    input: resolvedInput,
    harnessUrl: harnessUrlFromPage(currentPage),
    lifecycle,
  });
}

export async function restartRemoteBrowserDb(id: string): Promise<void> {
  const handle = remoteBrowserDbs.get(id);
  if (!handle) throw new Error(`Remote browser db "${id}" is not open`);
  await Promise.all(
    handle.pages.map((page) =>
      evaluateHarness(page, "closeRemoteBrowserDb", id).catch(() => undefined),
    ),
  );
  await Promise.all(handle.pages.map((page) => page.close()));
  handle.pages.length = 0;
  // With every owner page gone, the browser terminates the SharedWorker. The
  // context remains alive so its origin-scoped IndexedDB survives the restart.
  await new Promise((resolve) => setTimeout(resolve, 100));
  for (let index = 0; index < (handle.input.tabCount ?? 1); index += 1) {
    const page = await handle.context.newPage();
    await page.goto(handle.harnessUrl, { waitUntil: "domcontentloaded" });
    try {
      await evaluateHarness(page, "createRemoteBrowserDb", {
        ...handle.input,
        initialRow: undefined,
      });
    } catch (error) {
      await page.close().catch(() => undefined);
      throw new Error(`Remote browser db "${id}" restart tab ${index} failed`, {
        cause: error,
      });
    }
    handle.pages.push(page);
  }
}

export async function deleteRemoteBrowserIndexedDbAndWaitForReload(
  id: string,
  _dbName: string,
): Promise<void> {
  const handle = remoteBrowserDbs.get(id);
  if (!handle) throw new Error(`Remote browser db "${id}" is not open`);
  const physicalDbName = await evaluateHarness<string, string>(
    handle.pages[0]!,
    "remoteBrowserDbPhysicalName",
    id,
  );
  const previousLoads = await Promise.all(
    handle.pages.map((page) =>
      page.evaluate((key) => Number(sessionStorage.getItem(key) ?? 0), HARNESS_LOAD_COUNT_KEY),
    ),
  );
  await handle.pages[0]!.evaluate((name) => {
    indexedDB.deleteDatabase(name);
  }, physicalDbName);
  await Promise.all(
    handle.pages.map((page, index) =>
      page.waitForFunction(
        ({ key, previousLoads }) => Number(sessionStorage.getItem(key) ?? 0) > previousLoads,
        { key: HARNESS_LOAD_COUNT_KEY, previousLoads: previousLoads[index]! },
        { timeout: 10_000 },
      ),
    ),
  );
}

export async function waitForRemoteBrowserDbTitle(
  input: RemoteBrowserDbWaitForTitleInput,
): Promise<Record<string, unknown>[]> {
  const handle = remoteBrowserDbs.get(input.id);
  if (!handle) {
    throw new Error(`Remote browser db "${input.id}" is not open`);
  }

  try {
    return await evaluateHarness(handle.pages[0]!, "waitForRemoteBrowserDbTitle", input);
  } catch (error) {
    const underlying = boundedRemoteBrowserDbErrorReceipt(error);
    const receipt = await remoteBrowserDbLifecycleReceipt(handle).catch(
      () => "remote-lifecycle-receipt=unavailable",
    );
    throw new Error(
      `Remote browser db title wait failed: ${receipt} underlying-error=${underlying}`,
      { cause: error },
    );
  }
}

export async function insertRemoteBrowserDbRow(
  id: string,
  tabIndex: number,
  row: Record<string, unknown>,
  table?: string,
): Promise<void> {
  const handle = remoteBrowserDbs.get(id);
  const page = handle?.pages[tabIndex];
  if (!page) throw new Error(`Remote browser db "${id}" tab ${tabIndex} is not open`);
  return evaluateHarness(page, "insertRemoteBrowserDbRow", { id, row, table });
}

export async function updateRemoteBrowserDbRow(
  id: string,
  tabIndex: number,
  rowId: string,
  patch: Record<string, unknown>,
  table?: string,
): Promise<void> {
  const handle = remoteBrowserDbs.get(id);
  const page = handle?.pages[tabIndex];
  if (!page) throw new Error(`Remote browser db "${id}" tab ${tabIndex} is not open`);
  await evaluateHarness(page, "updateRemoteBrowserDbRow", { id, rowId, patch, table });
}

export async function queryRemoteBrowserDbRows(
  id: string,
  tabIndex: number,
  tier?: "local" | "edge",
): Promise<Record<string, unknown>[]> {
  const handle = remoteBrowserDbs.get(id);
  const page = handle?.pages[tabIndex];
  if (!page) throw new Error(`Remote browser db "${id}" tab ${tabIndex} is not open`);
  return evaluateHarness(page, "queryRemoteBrowserDbRows", { id, tier });
}

export async function closeRemoteBrowserDb(id: string): Promise<void> {
  const handle = remoteBrowserDbs.get(id);
  if (!handle) {
    return;
  }

  remoteBrowserDbs.delete(id);
  try {
    await evaluateHarness(handle.pages[0]!, "closeRemoteBrowserDb", id);
  } catch {
    // Best effort: page or worker may already be gone.
  }
  await handle.context.close();
}
