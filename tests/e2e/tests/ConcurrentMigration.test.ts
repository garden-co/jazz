import { expect, test } from "@playwright/test";

test.describe("Concurrent Migration", () => {
  test("should reload without incurring on InvalidSignature", async ({
    page,
  }) => {
    const consoleLogs: string[] = [];
    page.on("console", (message) => {
      consoleLogs.push(message.text());
    });

    await page.goto("/concurrent-migration");

    await page.waitForURL(/concurrent-migration\?projectId/);

    const url = new URL(page.url());

    url.searchParams.set("milestoneName", "Alice");

    await page.goto(url.toString());

    url.searchParams.set("milestoneName", "Bob");

    console.log("Going offline");

    await page.getByRole("button", { name: "Run Migration" }).click();

    await expect(page.getByText("Alice")).toBeVisible();

    await page.reload();

    await expect(page.getByText("Alice")).toBeVisible();

    expect(
      consoleLogs.find((log) => log.includes("InvalidSignature")),
    ).toBeUndefined();
  });

  test("should handle onffline conflicts between two users", async ({
    page,
    browser,
  }) => {
    const consoleLogs: string[] = [];
    page.on("console", (message) => {
      consoleLogs.push(message.text());
    });

    await page.goto("/concurrent-migration");

    await page.waitForURL(/concurrent-migration\?projectId/);

    const url = new URL(page.url());

    const newUserPage = await (await browser.newContext()).newPage();

    url.searchParams.set("milestoneName", "Alice");

    await page.goto(url.toString());

    url.searchParams.set("milestoneName", "Bob");

    await newUserPage.goto(url.toString());

    // Only Alice goes offline, because we need only one user to get into a conflict
    console.log("Going offline");
    page.context().setOffline(true);

    await newUserPage.getByRole("button", { name: "Run Migration" }).click();
    await page.getByRole("button", { name: "Run Migration" }).click();

    await expect(newUserPage.getByText("Bob")).toBeVisible();
    await expect(page.getByText("Alice")).toBeVisible();

    // When Alice goes online, she should see the conflict and merge it
    console.log("Going online");
    page.context().setOffline(false);

    await expect(newUserPage.getByText("Alice")).toBeVisible();
    await expect(newUserPage.getByText("Bob")).toBeVisible();
    await expect(page.getByText("Alice")).toBeVisible();
    await expect(page.getByText("Bob")).toBeVisible();

    expect(
      consoleLogs.find((log) => log.includes("InvalidSignature")),
    ).toBeUndefined();
  });
});
