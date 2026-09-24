import { test, expect } from "@playwright/test";
import { readFile } from "node:fs/promises";

// Fixed, valid one-pixel PNG; selected as an actual browser File, never injected into the SDK.
const png = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aE1cAAAAASUVORK5CYII=",
  "base64",
);

test("owner shares encrypted text and image; recipient downloads, replies and reloads; outsider is denied", async ({
  browser,
  baseURL,
}) => {
  const ownerContext = await browser.newContext();
  const recipientContext = await browser.newContext({ acceptDownloads: true });
  const outsiderContext = await browser.newContext();
  try {
    const owner = await ownerContext.newPage();
    const recipient = await recipientContext.newPage();
    const outsider = await outsiderContext.newPage();
    await Promise.all([owner.goto(baseURL!), recipient.goto(baseURL!), outsider.goto(baseURL!)]);
    await expect(recipient.getByTestId("account-id")).toHaveText(/[0-9a-f-]{36}/);
    const accountId = (await recipient.getByTestId("account-id").textContent())!;
    await owner.getByRole("button", { name: "New encrypted chat" }).click();
    await expect(owner.getByTestId("chat-id")).toHaveText(/[0-9a-f-]{36}/);
    const chatId = (await owner.getByTestId("chat-id").textContent())!;
    await owner.getByLabel("Recipient account ID").fill(accountId);
    await owner.getByRole("button", { name: "Share chat", exact: true }).click();
    await expect(owner.getByRole("status")).toContainText("Chat shared");
    // Repeating sharing is an explicit safe retry, not another membership row.
    await owner.getByRole("button", { name: "Share chat", exact: true }).click();
    await expect(owner.getByRole("status")).toContainText("Chat shared");
    await owner.getByLabel("Message", { exact: true }).fill("Only our accounts can read this");
    await owner
      .getByLabel("Image attachment")
      .setInputFiles({ name: "private-pixel.png", mimeType: "image/png", buffer: png });
    await owner.getByRole("button", { name: "Send", exact: true }).click();
    await expect(owner.getByTestId("send-status")).toHaveText("Sent");
    await recipient.goto(`${baseURL}/?chat=${chatId}`);
    await expect(
      recipient.getByText("Only our accounts can read this", { exact: true }),
    ).toBeVisible();
    const image = recipient.getByRole("img", { name: "private-pixel.png" });
    await expect(image).toBeVisible();
    await expect
      .poll(() =>
        image.evaluate((node: HTMLImageElement) => node.complete && node.naturalWidth === 1),
      )
      .toBe(true);
    const downloadEvent = recipient.waitForEvent("download");
    await recipient.getByRole("link", { name: "Download image" }).click();
    const download = await downloadEvent;
    expect(download.suggestedFilename()).toBe("private-pixel.png");
    expect(await readFile((await download.path())!)).toEqual(png);
    await recipient.getByLabel("Message", { exact: true }).fill("Recipient reply");
    await recipient.getByRole("button", { name: "Send", exact: true }).click();
    await expect(owner.getByText("Recipient reply", { exact: true })).toBeVisible();
    await recipient.reload();
    await expect(recipient.getByTestId("account-id")).toHaveText(accountId);
    await expect(recipient.getByRole("img", { name: "private-pixel.png" })).toBeVisible();
    await expect
      .poll(() =>
        recipient
          .getByRole("img", { name: "private-pixel.png" })
          .evaluate((node: HTMLImageElement) => node.naturalWidth),
      )
      .toBe(1);
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
  } finally {
    await Promise.all([ownerContext.close(), recipientContext.close(), outsiderContext.close()]);
  }
});
