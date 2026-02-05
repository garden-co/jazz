import { expect, test } from "@playwright/test";
import { createFile, createOrganization } from "./lib/data";
import {
  addAccount,
  createAccount,
  initializeKvStore,
  inspectCoValue,
} from "./lib/utils";

initializeKvStore();
const { account, accountID, accountSecret } = await createAccount();

const organization = createOrganization();

test("should add and delete account in dropdown", async ({ page }) => {
  await addAccount(page, accountID, accountSecret);

  await expect(page.getByText("Jazz CoValue Inspector")).toBeVisible();

  await expect(
    page.getByRole("button", { name: `Inspector test account <${accountID}>` }),
  ).toBeVisible();

  await page.getByRole("button", { name: "Remove account" }).click();
  await expect(page.getByText("Jazz CoValue Inspector")).not.toBeVisible();
  await expect(
    page.getByText("Select an account to connect to the inspector."),
  ).toBeVisible();
  await expect(
    page.getByText(`👤Inspector test account <${accountID}>`),
  ).not.toBeVisible();
});

test("should inspect account", async ({ page }) => {
  await addAccount(page, accountID, accountSecret);
  await page.getByRole("button", { name: "Inspect my account" }).click();

  await expect(page.locator("article").getByText(accountID)).toBeVisible();
  await expect(page.getByText("👤 Account")).toBeVisible();

  await page.getByRole("button", { name: "profile {} CoMap name:" }).click();
  await expect(page.getByText("Role: admin")).toBeVisible();
});

test("should inspect CoValue", async ({ page }) => {
  await addAccount(page, accountID, accountSecret);

  await account.$jazz.waitForAllCoValuesSync(); // Ensures that the organization is uploaded

  await inspectCoValue(page, organization.$jazz.id);

  await expect(page.getByText(/Garden Computing/)).toHaveCount(3);
  await expect(page.getByText(organization.$jazz.id)).toBeVisible();
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
  await expect(page.getByRole("table").nth(0).getByRole("row")).toHaveCount(11);

  await page.getByRole("button", { name: "issues" }).click();
  await expect(page.getByRole("table").nth(0).getByRole("row")).toHaveCount(4);

  const table = page.getByRole("table");
  const row = table.getByRole("row").nth(1);
  const issue = organization.projects[0].issues[0];

  // Test if table is displaying the Issue data correctly
  await expect(row.getByRole("cell").nth(0)).toHaveText(issue.$jazz.id);
  await expect(row.getByRole("cell").nth(1)).toHaveText(issue.title);
  await expect(row.getByRole("cell").nth(2)).toHaveText(issue.status);
  await expect(
    row
      .getByRole("cell")
      .nth(3)
      .getByRole("button", { name: issue.labels.$jazz.id }),
  ).toBeVisible();
  await expect(
    row
      .getByRole("cell")
      .nth(4)
      .getByRole("button", { name: issue.reactions.$jazz.id }),
  ).toBeVisible();

  if (issue.file) {
    await expect(
      row
        .getByRole("cell")
        .nth(5)
        .getByRole("button", { name: issue.file.$jazz.id }),
    ).toBeVisible();
  }

  if (issue.image) {
    await expect(
      row
        .getByRole("cell")
        .nth(6)
        .getByRole("button", { name: issue.image.$jazz.id }),
    ).toBeVisible();
  }

  // Test if CoMap/grid view is displaying Issue data correctly
  await row.getByRole("button", { name: "View" }).click();
  await expect(page.getByText(issue.title)).toBeVisible();
  await expect(page.getByText(issue.status)).toBeVisible();
  await expect(page.getByRole("button", { name: /labels/ })).toBeVisible();
  await expect(page.getByRole("button", { name: /reactions/ })).toBeVisible();
  await expect(page.getByRole("button", { name: /file/ })).toBeVisible();

  await expect(page.getByRole("button", { name: /^image/ })).toBeVisible();
  await page.pause();

  await page.getByRole("button", { name: "projects" }).click();
  await expect(page.getByRole("table").getByRole("row")).toHaveCount(5);
});

test("should show CoValue type", async ({ page }) => {
  await addAccount(page, accountID, accountSecret);

  await account.$jazz.waitForAllCoValuesSync(); // Ensures that the organization is uploaded

  // Test FileStream
  const file = createFile();
  await inspectCoValue(page, file.$jazz.id);
  await expect(page.getByText("📃 FileStream")).toBeVisible();

  // Test ImageDefinition
  await inspectCoValue(page, organization.image.$jazz.id);
  await expect(page.getByText("🖼️ Image")).toBeVisible();

  // Test CoMap
  await inspectCoValue(page, organization.$jazz.id);
  await expect(page.getByText("{} CoMap")).toBeVisible();

  // Test CoList
  await inspectCoValue(page, organization.projects.$jazz.id);
  await expect(page.getByText("☰ CoList")).toBeVisible();

  // Test CoFeed
  await inspectCoValue(
    page,
    organization.projects[0].issues[0].reactions.$jazz.id,
  );
  await expect(page.getByText("≋ CoFeed")).toBeVisible();

  // Test Account
  await inspectCoValue(page, account.$jazz.id);
  await expect(page.getByText("👤 Account")).toBeVisible();

  // Test Group
  await inspectCoValue(page, organization.$jazz.owner.$jazz.id);
  await expect(page.getByText("👥 Group")).toBeVisible();
});

test("should show Group members", async ({ page }) => {
  await addAccount(page, accountID, accountSecret);

  organization.$jazz.owner.addMember("everyone", "reader");

  await account.$jazz.waitForAllCoValuesSync(); // Ensures that the organization is uploaded
  await inspectCoValue(page, organization.$jazz.id);

  const ownershipText = await page.getByText(/Owned by/).innerText();
  expect(ownershipText).toContain(
    `Group <${organization.$jazz.owner.$jazz.id}>`,
  );

  await page
    .getByRole("button", {
      name: `Group <${organization.$jazz.owner.$jazz.id}>`,
    })
    .click();

  const table = page.getByRole("table");

  const row1 = table.getByRole("row").nth(1);
  await expect(row1.getByRole("cell").nth(0)).toHaveText("everyone");
  await expect(row1.getByRole("cell").nth(1)).toHaveText("reader");

  const row2 = table.getByRole("row").nth(2);
  await expect(row2.getByRole("cell").nth(0)).toHaveText(
    `👤Inspector test account <${account.$jazz.id}>`,
  );
  await expect(row2.getByRole("cell").nth(1)).toHaveText("admin");

  await page
    .locator("article")
    .getByRole("button", {
      name: `Inspector test account <${account.$jazz.id}>`,
    })
    .click();
  await expect(page.getByText("👤 Account")).toBeVisible();
});
