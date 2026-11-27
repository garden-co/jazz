import type { BrowserContext, Page } from "playwright";
import { evaluateHarnessOperation } from "./evaluate-harness.mjs";
import type { EncryptedTabCommand } from "./session-tab-encrypted.js";
import { jazzAppRequest } from "./testing-server-node.js";

const tabs = new Map<string, Page>();

export async function encryptedSessionTab(
  context: BrowserContext,
  currentPage: Page,
  input: EncryptedTabCommand,
): Promise<string[] | void> {
  let page = tabs.get(input.id);
  if (input.action === "open") {
    page = await context.newPage();
    tabs.set(input.id, page);
    await page.exposeFunction("__jazzEncryptedTabRequest", jazzAppRequest);
    await page.goto(new URL("/tests/browser/remote-db-harness.html", currentPage.url()).href, {
      waitUntil: "domcontentloaded",
    });
  }
  if (!page) throw new Error(`No encrypted session tab ${input.id} is open`);
  try {
    return await evaluateHarnessOperation(
      page,
      "/tests/browser/session-tab-encrypted.ts",
      "runEncryptedTabCommand",
      input,
    );
  } finally {
    if (input.action === "close") {
      tabs.delete(input.id);
      await page.close();
    }
  }
}
