import type { Browser, BrowserContext, Page } from "playwright";
import type {
  RemoteBrowserDbCreateInput,
  RemoteBrowserDbWaitForTitleInput,
} from "./remote-db-harness.js";

interface RemoteBrowserDbHandle {
  context: BrowserContext;
  pages: Page[];
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
): Promise<void> {
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
  });
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
