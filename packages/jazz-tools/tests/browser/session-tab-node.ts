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
