import { expect, test } from "@playwright/test";

test.describe("test with JavaScript", () => {
  test.use({ javaScriptEnabled: true });
  test("test with JavaScript", async ({ page }) => {
    await page.goto("/");
    const username = page.getByRole("textbox", {
      name: "Your profile name (only",
    });
    await expect(username).toHaveValue("Anonymous user");
  });
});

test.describe("test without JavaScript", () => {
  test.use({ javaScriptEnabled: false });
  test("test without JavaScript", async ({ page }) => {
    await page.goto("/");
    const username = page.getByRole("textbox", {
      name: "Your profile name (only",
    });
    await expect(username).toHaveValue("");
  });
});
