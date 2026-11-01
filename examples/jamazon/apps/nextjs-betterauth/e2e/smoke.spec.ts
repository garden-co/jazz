import { expect, test } from "@playwright/test";

const TIMEOUT = 30_000;

test("a guest's cart survives sign-up, and the paid order survives a reload", async ({ page }) => {
  const run = Date.now().toString(36);
  await page.goto("/");
  // A product card is a link overlay; click the product name it covers.
  const product = await page.locator("a[href^='/product/']").first().getAttribute("aria-label");
  await page.getByText(product!, { exact: true }).first().click();
  await page.getByRole("button", { name: "Add to cart" }).click();
  await expect(page.getByText("1 in your cart.")).toBeVisible({ timeout: TIMEOUT });

  await page.goto("/cart");
  await page.getByRole("link", { name: "Check out" }).click();
  await page.getByLabel("Full name").fill("Ada Lovelace");
  await page.getByRole("textbox", { name: /^Address/ }).fill("1 Analytical Way");
  await page.getByLabel("City").fill("London");
  await page.getByLabel("Postcode").fill("N1 9GU");
  await page.getByLabel("Country").fill("United Kingdom");
  await page.getByRole("button", { name: "Continue to review" }).click();

  // A guest signs up from checkout; the cart comes along.
  await page.getByRole("link", { name: "Sign in to order" }).click();
  await page.getByText("Create account", { exact: true }).first().click();
  await page.getByLabel("Name").fill("Ada");
  await page.getByLabel("Email").fill(`ada-${run}@jamazon.test`);
  await page.getByLabel("Password").fill("correct horse battery");
  await page.locator("form").getByRole("button", { name: "Create account" }).click();

  await expect(page).toHaveURL(/\/checkout$/, { timeout: TIMEOUT });
  await page.getByRole("button", { name: "Place order" }).click();
  await expect(page).toHaveURL(/\/orders\/[^/]+$/, { timeout: TIMEOUT });
  await expect(page.getByRole("heading", { name: /^Order / })).toBeVisible({ timeout: TIMEOUT });
  await page.getByRole("button", { name: /^Pay / }).click();
  await expect(page.getByText("Paid").first()).toBeVisible({ timeout: TIMEOUT });

  await page.reload();
  await expect(page.getByRole("heading", { name: /^Order / })).toBeVisible({ timeout: TIMEOUT });
  await expect(page.getByText("Paid").first()).toBeVisible({ timeout: TIMEOUT });
});
