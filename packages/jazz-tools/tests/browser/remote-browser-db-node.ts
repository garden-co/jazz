import type { Browser, BrowserContext, Page } from "playwright";
import type {
  RemoteBrowserDbCreateInput,
  RemoteBrowserDbWaitForTitleInput,
} from "./remote-db-harness.js";

interface RemoteBrowserDbHandle {
  context: BrowserContext;
  page: Page;
}

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
  const remotePage = await remoteContext.newPage();
  await remotePage.goto(harnessUrlFromPage(currentPage), { waitUntil: "domcontentloaded" });
  await evaluateHarness(remotePage, "createRemoteBrowserDb", input);

  remoteBrowserDbs.set(input.id, {
    context: remoteContext,
    page: remotePage,
  });
}

export async function waitForRemoteBrowserDbTitle(
  input: RemoteBrowserDbWaitForTitleInput,
): Promise<Record<string, unknown>[]> {
  const handle = remoteBrowserDbs.get(input.id);
  if (!handle) {
    throw new Error(`Remote browser db "${input.id}" is not open`);
  }

  return evaluateHarness(handle.page, "waitForRemoteBrowserDbTitle", input);
}

export async function closeRemoteBrowserDb(id: string): Promise<void> {
  const handle = remoteBrowserDbs.get(id);
  if (!handle) {
    return;
  }

  remoteBrowserDbs.delete(id);
  try {
    await evaluateHarness(handle.page, "closeRemoteBrowserDb", id);
  } catch {
    // Best effort: page or worker may already be gone.
  }
  await handle.context.close();
}
