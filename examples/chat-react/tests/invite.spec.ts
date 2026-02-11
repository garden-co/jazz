import { expect, test } from "@playwright/test";

test.describe("Invite Link Logic", () => {
  test("Should gain access to a private chat via invite link", async ({
    page,
  }) => {
    const randomSecret = `Secret-${Math.random().toString(36).substring(7)}`;
    let inviteLink = "";

    await test.step("Initial Load & Setup", async () => {
      await page.goto("/");
      await page.waitForURL("**/#/chat/*");
      await expect(page.locator("header")).toBeVisible();
    });

    await test.step("Create a new private chat", async () => {
      await page.locator("header button.flex.gap-2.items-center").click();
      await page.getByRole("menuitem", { name: /chat list/i }).click();
      await page.getByRole("button", { name: /new private chat/i }).click();
      await expect(page).toHaveURL(/\/chat\//);
    });

    await test.step("Send secret message", async () => {
      const messageEditor = page.locator(
        '#messageEditor [contenteditable="true"]',
      );
      await messageEditor.fill(randomSecret);
      await page
        .getByRole("button")
        .filter({ has: page.locator(".lucide-send") })
        .click();
      await expect(page.getByText(randomSecret)).toBeVisible();
    });

    await test.step("Generate invite link", async () => {
      await page.locator("button:has(.lucide-plus)").click();
      await page.getByRole("menuitem", { name: /invite/i }).click();

      const inviteInput = page.locator("input#link");
      await expect(inviteInput).toBeVisible();
      inviteLink = await inviteInput.inputValue();
      expect(inviteLink).toContain("/invite/");

      await page.getByRole("button", { name: /done/i }).click();
    });

    await test.step("Log out", async () => {
      await page.locator("header button.flex.gap-2.items-center").click();
      await page.getByRole("menuitem", { name: /profile/i }).click();
      await page.getByRole("button", { name: /log out/i }).click();

      await page.goto("/");
      await page.waitForURL("**/#/chat/*");
    });

    await test.step("Follow invite link and verify access", async () => {
      await page.goto(inviteLink);
      await page.waitForURL("**/#/chat/*");
      await expect(page.getByText(randomSecret)).toBeVisible();
    });
  });
});
