import { test, expect, chromium, type BrowserContext, type Page } from "@playwright/test";
import { readFile, mkdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { createServer, type ViteDevServer } from "vite";
import { startLocalJazzServer } from "jazz-tools/testing";
import {
  transportGate,
  type TransportGate,
} from "../../../packages/jazz-tools/src/runtime/testing/tcp-transport-gate.js";

// A real File selected through the composer, never injected into an SDK client.
const png = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aE1cAAAAASUVORK5CYII=",
  "base64",
);
const baseURL = "http://127.0.0.1:5183";
let server: Awaited<ReturnType<typeof startLocalJazzServer>>;
let gate: TransportGate;
let vite: ViteDevServer;
const originalEnv = new Map<string, string | undefined>();

test.beforeAll(async () => {
  server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  gate = await transportGate(server.url);
  for (const [key, value] of Object.entries({
    VITE_JAZZ_APP_ID: server.appId,
    VITE_JAZZ_SERVER_URL: gate.url,
    JAZZ_ADMIN_SECRET: server.adminSecret,
  })) {
    originalEnv.set(key, process.env[key]);
    process.env[key] = value;
  }
  vite = await createServer({
    root: fileURLToPath(new URL("../", import.meta.url)),
    server: { host: "127.0.0.1", port: 5183, strictPort: true },
  });
  await vite.listen();
});

test.afterAll(async () => {
  await vite?.close();
  await gate?.close();
  await server?.stop();
  for (const [key, value] of originalEnv) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

async function exactImage(page: Page) {
  const image = page.getByRole("img", { name: "private-pixel.png" });
  await expect(image).toBeVisible();
  await expect
    .poll(() =>
      image.evaluate((node: HTMLImageElement) => node.complete && node.naturalWidth === 1),
    )
    .toBe(true);
  const downloading = page.waitForEvent("download");
  await page.getByRole("link", { name: "Download image" }).click();
  const download = await downloading;
  expect(download.suggestedFilename()).toBe("private-pixel.png");
  expect(await readFile((await download.path())!)).toEqual(png);
}

test("automatic offline chat survives browser process restart and delivers exact encrypted images after reconnect", async ({
  browser,
}) => {
  const profile = test.info().outputPath("owner-profile");
  await mkdir(profile, { recursive: true });
  let ownerContext: BrowserContext | undefined;
  const recipientContext = await browser.newContext({ acceptDownloads: true });
  const outsiderContext = await browser.newContext();
  try {
    ownerContext = await chromium.launchPersistentContext(profile, {
      headless: true,
      acceptDownloads: true,
    });
    let owner = await ownerContext.newPage();
    await owner.goto(baseURL);
    await expect(owner.getByTestId("account-id")).toHaveText(/[0-9a-f-]{36}/);
    const ownerId = (await owner.getByTestId("account-id").textContent())!;
    // Catalogue and automatic account setup are local now. Kill all Jazz sockets,
    // including worker sockets; the Vite web server deliberately remains reachable.
    gate.block();
    await owner.getByRole("button", { name: "New encrypted chat", exact: true }).click();
    await expect(owner.getByTestId("chat-id")).toHaveText(/[0-9a-f-]{36}/);
    const chatId = (await owner.getByTestId("chat-id").textContent())!;
    await owner.getByLabel("Message", { exact: true }).fill("Only our accounts can read this");
    await owner
      .getByLabel("Image attachment")
      .setInputFiles({ name: "private-pixel.png", mimeType: "image/png", buffer: png });
    await owner.getByRole("button", { name: "Send", exact: true }).click();
    await expect(owner.getByTestId("send-status")).toContainText("Saved on this device");
    await expect(owner.getByTestId("message-status")).toHaveText(
      /Saved on this device · (pending acceptance|acceptance unconfirmed)/,
    );
    await exactImage(owner);
    await owner.screenshot({
      path: test.info().outputPath("offline-local-image.png"),
      fullPage: true,
    });

    // Closing a persistent context terminates its Chromium process, not just a
    // tab or Db. Reopening the same profile starts a new process while partitioned.
    await ownerContext.close();
    ownerContext = undefined;
    ownerContext = await chromium.launchPersistentContext(profile, {
      headless: true,
      acceptDownloads: true,
    });
    owner = await ownerContext.newPage();
    await owner.goto(`${baseURL}/?chat=${chatId}`);
    await expect(owner.getByTestId("account-id")).toHaveText(ownerId);
    await expect(owner.getByText("Only our accounts can read this", { exact: true })).toBeVisible();
    await expect(owner.getByTestId("message-status")).toHaveText("Local · acceptance unconfirmed");
    await exactImage(owner);
    await owner.screenshot({
      path: test.info().outputPath("offline-process-reopen.png"),
      fullPage: true,
    });

    gate.unblock();
    // Remote-only hooks can terminate on connection failure. The existing reload
    // action reconnects and subscribes again; it never resubmits the stored image.
    await owner.getByRole("button", { name: "Reload chat", exact: true }).click();
    await expect(owner.getByTestId("message-status")).toHaveText("Available from server");
    const recipient = await recipientContext.newPage();
    await recipient.goto(baseURL);
    await expect(recipient.getByTestId("account-id")).toHaveText(/[0-9a-f-]{36}/);
    const recipientId = (await recipient.getByTestId("account-id").textContent())!;
    expect(recipientId).not.toBe(ownerId);
    await owner.getByLabel("Recipient account ID").fill(recipientId);
    await owner.getByRole("button", { name: "Share chat", exact: true }).click();
    await expect(owner.getByRole("status")).toContainText("Chat shared");
    // Accepted delivery history must survive without the live authority or
    // the in-memory snapshot that completed sharing.
    gate.block();
    await ownerContext.close();
    ownerContext = undefined;
    ownerContext = await chromium.launchPersistentContext(profile, {
      headless: true,
      acceptDownloads: true,
    });
    owner = await ownerContext.newPage();
    await owner.goto(`${baseURL}/?chat=${chatId}`);
    await expect(owner.getByTestId("account-id")).toHaveText(ownerId);
    await expect(owner.getByText("Only our accounts can read this", { exact: true })).toBeVisible();
    await expect(owner.getByTestId("message-status")).toHaveText("Local · acceptance unconfirmed");
    await exactImage(owner);
    await owner.screenshot({
      path: test.info().outputPath("offline-post-share-reopen.png"),
      fullPage: true,
    });
    gate.unblock();
    await owner.getByRole("button", { name: "Reload chat", exact: true }).click();
    await expect(owner.getByTestId("message-status")).toHaveText("Available from server");
    await owner.getByLabel("Recipient account ID").fill(recipientId);
    await owner.getByRole("button", { name: "Share chat", exact: true }).click();
    await expect(owner.getByRole("status")).toContainText("Chat shared");
    await recipient.goto(`${baseURL}/?chat=${chatId}`);
    await expect(recipient.getByTestId("chat-id")).toHaveText(chatId);
    await expect(
      recipient.getByText("Only our accounts can read this", { exact: true }),
    ).toBeVisible();
    await exactImage(recipient);
    await recipient.getByLabel("Message", { exact: true }).fill("Recipient reply");
    await recipient.getByRole("button", { name: "Send", exact: true }).click();
    await expect(recipient.getByTestId("send-status")).toHaveText("Accepted by server");
    await expect(owner.getByText("Recipient reply", { exact: true })).toBeVisible();
    await recipient.reload();
    await expect(recipient.getByTestId("account-id")).toHaveText(recipientId);
    await exactImage(recipient);
    const outsider = await outsiderContext.newPage();
    await outsider.goto(`${baseURL}/?chat=${chatId}`);
    await expect(
      outsider.getByText("You don't have permission to access this chat.", { exact: true }),
    ).toBeVisible();
    await expect(outsider.getByRole("img", { name: "private-pixel.png" })).toHaveCount(0);
    await expect(
      outsider.getByText("Only our accounts can read this", { exact: true }),
    ).toHaveCount(0);
    await recipient.screenshot({
      path: test.info().outputPath("encrypted-chat.png"),
      fullPage: true,
    });
    await owner.getByLabel("Message", { exact: true }).fill("Invalid image fallback");
    await owner.getByLabel("Image attachment").setInputFiles({
      name: "invalid.png",
      mimeType: "image/png",
      buffer: Buffer.from("not a decodable PNG"),
    });
    await owner.getByRole("button", { name: "Send", exact: true }).click();
    const invalidImage = owner.getByRole("article").filter({ hasText: "Invalid image fallback" });
    await expect(
      invalidImage.getByText("Image unavailable or unsupported.", { exact: true }),
    ).toBeVisible();
    await expect(invalidImage.getByRole("img")).toHaveCount(0);
    await expect(invalidImage.getByRole("link", { name: "Download image" })).toHaveCount(0);
  } finally {
    gate.unblock();
    await Promise.all([ownerContext?.close(), recipientContext.close(), outsiderContext.close()]);
  }
});
