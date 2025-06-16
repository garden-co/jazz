import { expect, test } from "@playwright/test";
import { createOrganization } from "./data";
import { createAccount, initializeKvStore } from "./lib";

initializeKvStore();
const { account, accountID, accountSecret } = await createAccount();

test("should add and delete account in dropdown", async ({ page }) => {
  await page.goto("/");
  await page.getByLabel("Account ID").fill(accountID);
  await page.getByLabel("Account secret").fill(accountSecret);
  await page.getByRole("button", { name: "Add account" }).click();

  await expect(page.getByText("Jazz CoValue Inspector")).toBeVisible();
  await page
    .getByLabel("Account to inspect")
    .selectOption(`Inspector test account <${accountID}>`);

  await page.getByRole("button", { name: "Remove account" }).click();
  await expect(page.getByText("Jazz CoValue Inspector")).not.toBeVisible();
  await expect(page.getByText("Add an account to inspect")).toBeVisible();
  await expect(
    page.getByText(`Inspector test account <${accountID}>`),
  ).not.toBeVisible();
});

test("should inspect account", async ({ page }) => {
  await page.goto("/");
  await page.getByLabel("Account ID").fill(accountID);
  await page.getByLabel("Account secret").fill(accountSecret);
  await page.getByRole("button", { name: "Add account" }).click();
  await page.getByRole("button", { name: "Inspect my account" }).click();

  await expect(page.getByRole("heading", { name: accountID })).toBeVisible();
  await expect(page.getByText("👤 Account")).toBeVisible();

  await page.getByRole("button", { name: "profile {} CoMap name:" }).click();
  await expect(page.getByText("Role: admin")).toBeVisible();
});

test("should inspect CoValue", async ({ page }) => {
  await page.goto("/");
  await page.getByLabel("Account ID").fill(accountID);
  await page.getByLabel("Account secret").fill(accountSecret);
  await page.getByRole("button", { name: "Add account" }).click();

  const organization = createOrganization();

  await account.waitForAllCoValuesSync(); // Ensures that the organization is uploaded

  await page.getByLabel("CoValue ID").fill(organization.id);
  await page.getByRole("button", { name: "Inspect CoValue" }).click();

  await expect(page.getByText(/Garden Computing/)).toHaveCount(2);
  await expect(
    page.getByRole("heading", { name: organization.id }),
  ).toBeVisible();
  await expect(page.getByText("Role: admin")).toBeVisible();

  await page.getByRole("button", { name: /projects/ }).click();
  await expect(page.getByText("Showing 4 of 4")).toBeVisible();

  await page.getByRole("button", { name: "View" }).first().click();
  await expect(
    page.getByText("Jazz is a framework for building collaborative apps."),
  ).toBeVisible();

  await page.getByRole("button", { name: /issues/ }).click();
  await expect(page.getByText("Showing 3 of 3")).toBeVisible();
  await page.getByRole("button", { name: "View" }).first().click();

  await page.getByRole("button", { name: /labels/ }).click();
  // currently broken:
  // await expect(page.getByText("Showing 10 of 10")).toBeVisible();
  await expect(page.getByRole("table").getByRole("row")).toHaveCount(11);

  await page.getByRole("button", { name: "issues" }).click();
  await expect(page.getByRole("table").getByRole("row")).toHaveCount(4);

  await page.getByRole("button", { name: "projects" }).click();
  await expect(page.getByRole("table").getByRole("row")).toHaveCount(5);
});
