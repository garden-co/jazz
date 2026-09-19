import { expect, test, type Page } from "./fixtures.js";
import { startInspectorHandoff } from "../../../jazz-tools/src/dev/inspector-handoff.js";
import { ADMIN_SECRET, APP_ID } from "./test-constants.js";

async function expectProtectedRows(page: Page) {
  const table = page.getByRole("link", { name: "View todos data" });
  await expect(table).toBeVisible({ timeout: 15_000 });
  await table.click();
  await page.getByRole("columnheader", { name: "title", exact: true }).click();
  await expect(page.getByText("First seeded todo", { exact: true })).toBeVisible({
    timeout: 15_000,
  });
}

for (const tier of ["CORE", "STANDALONE"] as const) {
  test(`scoped CLI Inspector reads protected data through ${tier === "CORE" ? "Core" : "Edge"} with read-only controls`, async ({
    page,
  }) => {
    const serverUrl = process.env[`JAZZ_INSPECTOR_TEST_${tier}_SERVER_URL`]!;
    const handoff = await startInspectorHandoff({
      appId: APP_ID,
      serverUrl,
      adminSecret: ADMIN_SECRET,
      inspectorUrl: process.env.JAZZ_INSPECTOR_TEST_WEB_URL!,
    });
    try {
      await page.goto(handoff.url);
      await expect(page.getByText(/Read-only session/)).toBeVisible();
      await expectProtectedRows(page);
      await handoff.done;
      await expect(page.getByRole("button", { name: "Insert row", exact: true })).toBeDisabled();
      await expect(page.getByRole("checkbox", { name: /Toggle done for/ }).first()).toBeDisabled();
      await expect(page.getByRole("button", { name: "Delete row(s)", exact: true })).toBeDisabled();
      expect(page.url()).not.toContain("launch=");
      const storage = await page.evaluate(() => JSON.stringify(localStorage));
      expect(storage).not.toContain(ADMIN_SECRET);
      expect(storage).not.toContain("accessToken");
      await page.getByRole("button", { name: "Log out", exact: true }).click();
      await expect(page.getByText("First seeded todo", { exact: true })).toHaveCount(0);
    } finally {
      handoff.close();
    }
  });
}

test("explicit Inspector edit settles protected data through Edge and is visible to a new reader", async ({
  page,
}) => {
  const serverUrl = process.env.JAZZ_INSPECTOR_TEST_STANDALONE_SERVER_URL!;
  const options = {
    appId: APP_ID,
    serverUrl,
    adminSecret: ADMIN_SECRET,
    inspectorUrl: process.env.JAZZ_INSPECTOR_TEST_WEB_URL!,
  };
  const handoff = await startInspectorHandoff({
    ...options,
    capabilities: ["inspector:read", "inspector:edit"],
  });
  try {
    await page.goto(handoff.url);
    await expect(page.getByText(/Editing enabled/)).toBeVisible();
    await expectProtectedRows(page);
    const row = page
      .getByRole("row")
      .filter({ has: page.getByRole("gridcell", { name: "First seeded todo", exact: true }) });
    const checkbox = row.getByRole("checkbox", { name: /Toggle done for/ });
    const previous = await checkbox.isChecked();
    await checkbox.click();
    await page.getByRole("button", { name: "Save changes", exact: true }).click();
    await expect(page.getByRole("button", { name: "Save changes", exact: true })).toHaveCount(0, {
      timeout: 15_000,
    });
    await page.getByRole("button", { name: "Log out", exact: true }).click();
    const reader = await startInspectorHandoff(options);
    try {
      await page.goto(reader.url);
      await expectProtectedRows(page);
      await expect(row.getByRole("checkbox", { name: /Toggle done for/ })).toBeChecked({
        checked: !previous,
      });
    } finally {
      reader.close();
    }
  } finally {
    handoff.close();
  }
});

async function dashboardLogin(
  page: Page,
  control: { expiresIn?: number; holdRenewal?: boolean } = {},
) {
  const dashboard = process.env.JAZZ_INSPECTOR_TEST_DASHBOARD_URL!;
  await fetch(`${dashboard}/test/control`, { method: "POST", body: JSON.stringify(control) });
  await page.goto(`${dashboard}/login`);
  await page.goto(`/#login=dashboard&appId=${APP_ID}`);
  await page.getByRole("button", { name: "Sign in with dashboard" }).click();
  await expect(page.getByText(/Read-only session/)).toBeVisible({ timeout: 15_000 });
  await expectProtectedRows(page);
}

test("real Jazz data survives automatic renewal then clears when tenant permission is removed", async ({
  page,
}) => {
  let popups = 0;
  page.on("popup", () => popups++);
  const renewed = page.waitForResponse(
    (response) => response.url().endsWith("/inspector/renew") && response.status() === 200,
  );
  await dashboardLogin(page, { expiresIn: 8 });
  await renewed;
  await expect(page.getByText("First seeded todo", { exact: true })).toBeVisible();
  expect(popups).toBe(1);
  await fetch(`${process.env.JAZZ_INSPECTOR_TEST_DASHBOARD_URL}/test/control`, {
    method: "POST",
    body: JSON.stringify({ allowed: false }),
  });
  await expect(page.getByRole("alert")).toContainText("Session ended", { timeout: 15_000 });
  await expect(page.getByText("First seeded todo", { exact: true })).toHaveCount(0);
  expect(popups).toBe(1);
});

test("reload restores from cookies without root or access-token persistence, and logout suppresses restore", async ({
  page,
}) => {
  await dashboardLogin(page);
  let popups = 0;
  page.on("popup", () => popups++);
  await page.reload();
  await expectProtectedRows(page);
  expect(popups).toBe(0);
  expect(await page.evaluate(() => JSON.stringify(localStorage))).not.toMatch(
    /accessToken|inspectorToken|adminSecret/,
  );
  await page.getByRole("button", { name: "Log out", exact: true }).click();
  await page.reload();
  await expect(page.getByRole("button", { name: "Sign in with dashboard" })).toBeVisible();
  await expect(page.getByText("First seeded todo", { exact: true })).toHaveCount(0);
});

test("hard expiry clears real Jazz rows while renewal remains pending", async ({ page }) => {
  await dashboardLogin(page, { expiresIn: 6, holdRenewal: true });
  await expect(page.getByRole("alert")).toContainText("Session ended", { timeout: 10_000 });
  await expect(page.getByText("First seeded todo", { exact: true })).toHaveCount(0);
});
