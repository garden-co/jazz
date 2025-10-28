import { expect, test } from "@playwright/test";

test.describe("Top-level pages load", () => {
  test("homepage loads", async ({ page }) => {
    const response = await page.goto("/");
    expect(response?.ok()).toBeTruthy();
  });

  test("examples page loads", async ({ page }) => {
    const response = await page.goto("/examples");
    expect(response?.ok()).toBeTruthy();
  });

  test("showcase page loads", async ({ page }) => {
    const response = await page.goto("/showcase");
    expect(response?.ok()).toBeTruthy();
  });

  test("cloud page loads", async ({ page }) => {
    const response = await page.goto("/cloud");
    expect(response?.ok()).toBeTruthy();
  });

  test("status page loads", async ({ page }) => {
    const response = await page.goto("/status");
    expect(response?.ok()).toBeTruthy();
  });
});

test.describe("Docs pages load", () => {
  test("docs intro loads", async ({ page }) => {
    const response = await page.goto("/docs");
    expect(response?.ok()).toBeTruthy();
  });

  [
    "react",
    "react-native",
    "react-native-expo",
    "svelte",
    "vanilla",
  ].forEach((framework) => {
    test(`docs for ${framework} loads`, async ({ page }) => {
      const response = await page.goto(`/docs/${framework}`);
      expect(response?.ok()).toBeTruthy();
    });
  });

  test("/docs redirects to /docs/react", async ({ page }) => {
    await page.goto("/docs");
    await expect(page).toHaveURL(/\/docs\/react$/);
  });
});
