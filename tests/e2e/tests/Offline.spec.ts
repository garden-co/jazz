import { setTimeout } from "node:timers/promises";
import { expect, test } from "@playwright/test";

test.describe("Offline & online sync", () => {
  test("should reload the value when sync is turned off", async ({ page }) => {
    await page.goto("/test-input?syncWhen=never");

    await page.getByRole("textbox").fill("Hello");

    await expect(page.getByRole("textbox")).toHaveValue("Hello");

    await page.reload();

    await expect(page.getByRole("textbox")).toHaveValue("Hello");
  });

  test("should sync the value when sync is on signed up and the user is signed up", async ({
    page,
    browser,
  }) => {
    await page.goto("/test-input?syncWhen=signedUp");

    await page.getByRole("textbox").fill("Hello");

    await expect(page.getByRole("textbox")).toHaveValue("Hello");
    await page.getByRole("button", { name: "Sign Up" }).click();

    const url = new URL(page.url());
    url.searchParams.delete("syncWhen");

    // Create a new incognito instance and try to load the coValue
    const newUserPage = await (await browser.newContext()).newPage();
    await newUserPage.goto(url.toString());

    await expect(newUserPage.getByRole("textbox")).toHaveValue("Hello");
  });

  test("should sync when going online", async ({ page, browser }) => {
    const context = page.context();

    await page.goto("/test-input");

    await context.setOffline(true);

    await page.getByRole("textbox").fill("Hello");

    const url = new URL(page.url());

    await setTimeout(1000);

    await context.setOffline(false);

    // Create a new incognito instance and try to load the coValue
    const newUserPage = await (await browser.newContext()).newPage();
    await newUserPage.goto(url.toString());

    await expect(newUserPage.getByRole("textbox")).toHaveValue("Hello");
  });

  test("should mark the value as available when it becomes available on the sync server", async ({
    page,
    browser,
  }) => {
    await page.goto("/test-input?syncWhen=signedUp");

    await page.getByRole("textbox").fill("Hello");

    await expect(page.getByRole("textbox")).toHaveValue("Hello");

    const url = new URL(page.url());
    url.searchParams.delete("syncWhen");

    // Create a new incognito instance and try to load the coValue
    const newUserPage = await (await browser.newContext()).newPage();
    await newUserPage.goto(url.toString());

    // Sign up to start syncing the value
    await page.getByRole("button", { name: "Sign Up" }).click();

    await expect(newUserPage.getByRole("textbox")).toHaveValue("Hello", {
      timeout: 20_000,
    });
  });
});
