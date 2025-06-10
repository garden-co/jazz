import { expect, test } from "@playwright/test";

test.describe("Service - Sync", () => {
  test("should pass the message between the two peers", async ({ page }) => {
    await page.goto("/service");

    await page.getByRole("button", { name: "Start a ping-pong" }).click();

    await page.getByTestId("ping-pong").waitFor();

    await expect(page.getByTestId("ping-pong")).toBeVisible();
  });
});
