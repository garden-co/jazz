import { expect, test } from "@playwright/test";

test("login & expiration", async ({ page }) => {
  await page.goto("/");

  expect(page.getByText("You're not logged in")).toBeVisible();

  await page.getByRole("button", { name: "Sign in" }).click();

  await page
    .getByRole("textbox", { name: "Email address" })
    .fill("guido+clerk-test@garden.co");

  await page.keyboard.press("Enter");

  await page
    .getByRole("textbox", { name: "Password" })
    .fill("guido+clerk-test@garden.co");

  await page.keyboard.press("Enter");

  await page.waitForURL("/");

  await page.getByText("You're logged in").waitFor({ state: "visible" });

  expect(page.getByText("You're logged in")).toBeVisible();

  await page.goto("/?expirationTest");

  // Simulate expiration by logging out outside of Jazz
  await page.getByRole("button", { name: "Simulate expiration" }).click();

  await page.goto("/");

  await page.getByText("You're not logged in").waitFor({ state: "visible" });
});
