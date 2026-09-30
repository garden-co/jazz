import { expect, test, type Browser, type Page } from "@playwright/test";

const TIMEOUT = 30_000;

async function operator(browser: Browser, name: string): Promise<Page> {
  const page = await (await browser.newContext()).newPage();
  await page.goto("/");
  await page.getByRole("button", { name: "Create an operator account" }).click();
  await page.getByLabel("Name").fill(name);
  await page.getByLabel("Email").fill(`${name.toLowerCase()}-${Date.now()}@example.com`);
  await page.getByLabel("Password").fill("warehouse-password");
  await page.getByRole("button", { name: "Create account" }).click();
  await expect(page.getByRole("heading", { name: "Choose your warehouse" })).toBeVisible({
    timeout: TIMEOUT,
  });
  await page.getByRole("button", { name: "Join warehouse" }).click();
  await expect(page.getByRole("heading", { name: "Dashboard" })).toBeVisible({ timeout: TIMEOUT });
  return page;
}

async function choose(page: Page, label: string, option: string | RegExp) {
  await page.getByRole("combobox", { name: label }).click();
  await page.getByRole("option", { name: option }).click();
}

async function fillOrder(page: Page, customer: string, item: string, quantity: number) {
  await page.getByRole("link", { name: "New order" }).click();
  await choose(page, "Customer", new RegExp(`^${customer}`));
  await choose(page, "Item, line 1", new RegExp(`^${item}`));
  await page.getByLabel("Quantity").fill(String(quantity));
}

test("two operators race for scarce stock and see each other's orders live", async ({
  browser,
}) => {
  const ada = await operator(browser, "Ada");
  const ben = await operator(browser, "Ben");

  // Ben watches the queue while Ada orders.
  await ben.getByRole("link", { name: "Pending orders" }).click();
  await expect(ben.getByRole("cell", { name: "3003" })).toBeVisible({ timeout: TIMEOUT });

  await fillOrder(ada, "Ada Okafor", "Electric guitar strings", 2);
  await ada.getByRole("button", { name: "Place order" }).click();
  await expect(ada.getByRole("heading", { name: "Order 3006 placed" })).toBeVisible({
    timeout: TIMEOUT,
  });
  await expect(ben.getByRole("cell", { name: "3006" })).toBeVisible({ timeout: TIMEOUT });

  // Resubmitting the same request is safe: same receipt, no second order.
  await ada.getByRole("button", { name: "Submit the same request again" }).click();
  await expect(ada.getByText("Same receipt returned")).toBeVisible({ timeout: TIMEOUT });
  await expect(ada.getByRole("heading", { name: "Order 3006 placed" })).toBeVisible();
  await expect(ben.getByRole("cell", { name: "3007" })).toHaveCount(0);

  // Both order 4 of the 5 vintage amps at the same moment.
  await ada.getByRole("button", { name: "New order" }).click();
  await fillOrder(ada, "Ada Okafor", "Vintage tube amp", 4);
  await fillOrder(ben, "Bruno Silva", "Vintage tube amp", 4);
  await Promise.all([
    ada.getByRole("button", { name: "Place order" }).click(),
    ben.getByRole("button", { name: "Place order" }).click(),
  ]);
  const placed = (page: Page) => page.getByRole("heading", { name: /^Order \d+ placed$/ });
  const refused = (page: Page) => page.getByText("Insufficient stock", { exact: true });
  await expect
    .poll(
      async () =>
        [
          await placed(ada).count(),
          await placed(ben).count(),
          await refused(ada).count(),
          await refused(ben).count(),
        ].join(","),
      { timeout: TIMEOUT },
    )
    .toMatch(/^1,0,0,1$|^0,1,1,0$/);

  // The stock-level report now lists the amp below its reorder level.
  await ada.getByRole("link", { name: "Stock level" }).click();
  await expect(ada.getByRole("cell", { name: "Vintage tube amp" })).toBeVisible({
    timeout: TIMEOUT,
  });
});
