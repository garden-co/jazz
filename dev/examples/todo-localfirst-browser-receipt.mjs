#!/usr/bin/env node
import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { chromium } from "playwright";

const appUrl = process.env.JAZZ_TODO_APP_URL ?? "http://127.0.0.1:5173";
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
