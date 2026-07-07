#!/usr/bin/env node
import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { chromium } from "playwright";

const appUrl = process.env.JAZZ_TODO_APP_URL ?? "http://127.0.0.1:5173";
const expectedMode = process.argv.includes("--async-mode")
  ? "async"
  : process.argv.includes("--sync-mode")
    ? "sync"
    : undefined;
const screenshotPath = resolve(
  process.env.JAZZ_TODO_BROWSER_SCREENSHOT ?? "scratchpad/todo-localfirst-browser-receipt.png",
);
const title = `receipt-${Date.now()}`;

await mkdir(dirname(screenshotPath), { recursive: true });

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext();
const page = await context.newPage();
const consoleMessages = [];
page.on("console", (message) => {
  consoleMessages.push({ type: message.type(), text: message.text() });
});

try {
  await page.goto(appUrl, { waitUntil: "networkidle" });
  if (expectedMode) {
    const actualMode = await page.waitForFunction(
      () => {
        const client = globalThis.jazzClient;
        if (!client?.db) return null;
        return "all" in client.db ? "sync" : "async";
      },
      undefined,
      { timeout: 15_000 },
    );
    const mode = await actualMode.jsonValue();
    if (mode !== expectedMode) {
      throw new Error(`Expected ${expectedMode} Jazz client mode, got ${mode}`);
    }
  }
  await page.getByPlaceholder("What needs to be done?").fill(title);
  await page.getByRole("button", { name: "Add" }).click();
  const item = page.locator("#todo-list li", { hasText: title });
  await item.waitFor({ state: "visible", timeout: 15_000 });
  await item.locator(".toggle").click();
  await page.waitForFunction(
    (text) => {
      const rows = [...document.querySelectorAll("#todo-list li")];
      return rows.some((row) => row.textContent?.includes(text) && row.classList.contains("done"));
    },
    title,
    { timeout: 15_000 },
  );
  await item.locator(".delete-btn").click();
  await item.waitFor({ state: "detached", timeout: 15_000 });
  await page.screenshot({ path: screenshotPath, fullPage: true });
  console.log(
    JSON.stringify(
      {
        ok: true,
        appUrl,
        mode: expectedMode ?? "unchecked",
        title,
        screenshotPath,
        consoleMessages,
      },
      null,
      2,
    ),
  );
} finally {
  await browser.close();
}
