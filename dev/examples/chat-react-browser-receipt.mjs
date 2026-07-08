#!/usr/bin/env node
import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { chromium } from "playwright";

const appUrl = process.env.JAZZ_CHAT_APP_URL ?? "http://127.0.0.1:5175";
const expectedMode = process.argv.includes("--async-mode")
  ? "async"
  : process.argv.includes("--sync-mode")
    ? "sync"
    : undefined;
const screenshotPath = resolve(
  process.env.JAZZ_CHAT_BROWSER_SCREENSHOT ?? "scratchpad/chat-react-browser-receipt.png",
);
const messageText = `receipt message ${Date.now()}`;
const fileName = `receipt-${Date.now()}.txt`;
const fileText = `large-value receipt ${Date.now()} ${"x".repeat(32 * 1024)}`;

await mkdir(dirname(screenshotPath), { recursive: true });

const browser = await chromium.launch({ headless: true });
const contextA = await browser.newContext();
const contextB = await browser.newContext();
const pageA = await contextA.newPage();
const pageB = await contextB.newPage();
const consoleMessages = [];

for (const page of [pageA, pageB]) {
  page.on("console", (message) => {
    consoleMessages.push({
      page: page === pageA ? "writer" : "reader",
      type: message.type(),
      text: message.text(),
    });
  });
}

async function assertClientMode(page, mode) {
  if (!mode) return;
  const actualMode = await page.waitForFunction(
    () => {
      const client = globalThis.jazzClient;
      if (!client?.db) return null;
      return "all" in client.db ? "sync" : "async";
    },
    undefined,
    { timeout: 20_000 },
  );
  const value = await actualMode.jsonValue();
  if (value !== mode) {
    throw new Error(`Expected ${mode} Jazz client mode, got ${value}`);
  }
}

async function insertMessage(page, text) {
  await page.waitForFunction(
    () => {
      const editor = document.querySelector("#messageEditor");
      return typeof editor?.__editorHandle?.insertText === "function";
    },
    undefined,
    { timeout: 30_000 },
  );
  await page.evaluate((value) => {
    document.querySelector("#messageEditor").__editorHandle.insertText(value);
  }, text);
  await page
    .getByRole("button")
    .filter({ has: page.locator("svg.lucide-send") })
    .click();
}

async function uploadFile(page, name, body) {
  await page
    .getByRole("button")
    .filter({ has: page.locator("svg.lucide-plus") })
    .click();
  await page.getByRole("menuitem", { name: /file/i }).click();
  await page.waitForSelector('input[type="file"]', { state: "attached" });
  await page.evaluate(
    async ({ fileName, fileBody }) => {
      const input = document.querySelector('input[type="file"]');
      const file = new File([fileBody], fileName, { type: "text/plain" });
      await input.__handleFile(file);
    },
    { fileName: name, fileBody: body },
  );
}

try {
  await pageA.goto(appUrl, { waitUntil: "networkidle" });
  await assertClientMode(pageA, expectedMode);
  await pageA.getByText("Hello world").waitFor({ state: "visible", timeout: 45_000 });
  const chatUrl = pageA.url();

  await insertMessage(pageA, messageText);
  await pageA.getByText(messageText).waitFor({ state: "visible", timeout: 30_000 });
  await uploadFile(pageA, fileName, fileText);
  await pageA.getByText(fileName).waitFor({ state: "visible", timeout: 45_000 });

  await pageB.goto(chatUrl, { waitUntil: "networkidle" });
  await assertClientMode(pageB, expectedMode);
  await pageB.getByText(messageText).waitFor({ state: "visible", timeout: 45_000 });
  await pageB.getByText(fileName).waitFor({ state: "visible", timeout: 45_000 });
  await pageB
    .getByRole("button", { name: /download/i })
    .first()
    .click();

  await pageB.screenshot({ path: screenshotPath, fullPage: true });
  console.log(
    JSON.stringify(
      {
        ok: true,
        appUrl,
        mode: expectedMode ?? "unchecked",
        chatUrl,
        messageText,
        fileName,
        fileBytes: fileText.length,
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
