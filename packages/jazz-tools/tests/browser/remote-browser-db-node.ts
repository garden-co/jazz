import type { Browser, BrowserContext, Page } from "playwright";
import type {
  RemoteBrowserDbCreateInput,
  RemoteBrowserDbWaitForTitleInput,
} from "./remote-db-harness.js";

interface RemoteBrowserDbHandle {
  context: BrowserContext;
  pages: Page[];
  input: RemoteBrowserDbCreateInput;
  harnessUrl: string;
}

const HARNESS_LOAD_COUNT_KEY = "jazz-test:remote-harness-load-count";

const remoteBrowserDbs = new Map<string, RemoteBrowserDbHandle>();
const remoteHarnessModulePath = "/tests/browser/remote-db-harness.ts";

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

  const browser = getBrowserFromContext(currentContext);
  const remoteContext = await browser.newContext();
  await remoteContext.addInitScript((key) => {
    const count = Number(sessionStorage.getItem(key) ?? 0);
    sessionStorage.setItem(key, String(count + 1));
  }, HARNESS_LOAD_COUNT_KEY);
  const pages: Page[] = [];
  for (let index = 0; index < (input.tabCount ?? 1); index += 1) {
    const page = await remoteContext.newPage();
    await page.goto(harnessUrlFromPage(currentPage), { waitUntil: "domcontentloaded" });
    await evaluateHarness(page, "createRemoteBrowserDb", {
      ...input,
      initialRow: index === 0 ? input.initialRow : undefined,
    });
    pages.push(page);
  }

  remoteBrowserDbs.set(input.id, {
    context: remoteContext,
    pages,
    input,
    harnessUrl: harnessUrlFromPage(currentPage),
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
    await evaluateHarness(page, "createRemoteBrowserDb", {
      ...handle.input,
      initialRow: undefined,
    });
    handle.pages.push(page);
  }
}

export async function deleteRemoteBrowserIndexedDbAndWaitForReload(
  id: string,
  dbName: string,
): Promise<void> {
  const handle = remoteBrowserDbs.get(id);
  if (!handle) throw new Error(`Remote browser db "${id}" is not open`);
  const previousLoads = await Promise.all(
    handle.pages.map((page) =>
      page.evaluate((key) => Number(sessionStorage.getItem(key) ?? 0), HARNESS_LOAD_COUNT_KEY),
    ),
  );
  await handle.pages[0]!.evaluate((name) => {
    indexedDB.deleteDatabase(name);
  }, dbName);
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

  return evaluateHarness(handle.pages[0]!, "waitForRemoteBrowserDbTitle", input);
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
