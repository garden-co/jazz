import { expect, test } from "@playwright/test";

test("should show the and update properties", async ({ page }) => {
  await page.goto("/costate");

  await page.getByRole("button", { name: "Select person [0]" }).click();

  await expect(page.getByRole("textbox", { name: "Name" })).toHaveValue("John");
  await expect(page.getByRole("textbox", { name: "Dog" })).toHaveValue("Rex");

  await expect(page.getByTestId("person-name")).toContainText("John");
  await expect(page.getByTestId("person-dog-name")).toContainText("Rex");

  await page.getByRole("textbox", { name: "Name" }).fill("Jane");
  await expect(page.getByTestId("person-name")).toContainText("Jane");

  await page.getByRole("textbox", { name: "Dog" }).fill("Bibi");
  await expect(page.getByTestId("person-dog-name")).toContainText("Bibi");
});

test("should react to id changes", async ({ page }) => {
  await page.goto("/costate");

  await page.getByRole("button", { name: "Select person [0]" }).click();

  await expect(page.getByTestId("person-name")).toContainText("John");
  await expect(page.getByTestId("person-dog-name")).toContainText("Rex");

  await page.getByRole("button", { name: "Select person [1]" }).click();

  await expect(page.getByTestId("person-name")).toContainText("Mathieu");
  await expect(page.getByTestId("person-dog-name")).toContainText("Bibi");

  await page.getByRole("button", { name: "Set undefined" }).click();

  await expect(page.getByTestId("person-name")).not.toBeVisible();
  await expect(page.getByTestId("person-dog-name")).not.toBeVisible();

  await page.getByRole("button", { name: "Select person [0]" }).click();

  await expect(page.getByTestId("person-name")).toContainText("John");
  await expect(page.getByTestId("person-dog-name")).toContainText("Rex");
});

test("should logout correctly", async ({ page }) => {
  await page.goto("/costate");

  await page.getByRole("button", { name: "Select person [0]" }).click();

  await page.getByRole("textbox", { name: "Name" }).fill("Jane");
  await page.getByRole("textbox", { name: "Dog" }).fill("Bibi");

  await page.getByRole("button", { name: "Logout" }).click();

  await page.getByRole("button", { name: "Select person [0]" }).click();

  await expect(page.getByRole("textbox", { name: "Name" })).toHaveValue("John");
  await expect(page.getByRole("textbox", { name: "Dog" })).toHaveValue("Rex");
});

test("should manage branches correctly", async ({ page }) => {
  await page.goto("/costate");

  // Select a person first
  await page.getByRole("button", { name: "Select person [0]" }).click();

  // Check initial branch status
  await expect(page.locator("text=Main branch")).toBeVisible();

  // Create a new branch
  await page.getByRole("textbox", { name: "Create new branch:" }).fill("feature-branch");
  await page.getByRole("button", { name: "Create Branch" }).click();

  // Check that we're now on the new branch
  await expect(page.locator("text=feature-branch")).toBeVisible();
  await expect(page.locator("text=Branch: feature-branch")).toBeVisible();

  // Make changes on the branch
  await page.getByRole("textbox", { name: "Name" }).fill("Jane Branch");
  await page.getByRole("textbox", { name: "Dog" }).fill("Branch Dog");

  // Verify changes are reflected
  await expect(page.getByTestId("person-name")).toContainText("Jane Branch");
  await expect(page.getByTestId("person-dog-name")).toContainText("Branch Dog");

  // Switch back to main branch
  await page.getByRole("button", { name: "Switch to Main" }).click();

  // Verify we're back on main and changes are not visible
  await expect(page.getByTestId("branch-status")).toContainText("Main branch");
  await expect(page.getByTestId("person-name")).toContainText("John");
  await expect(page.getByTestId("person-dog-name")).toContainText("Rex");

  // Create the feature branch again to test merge
  await page.getByRole("textbox", { name: "Create new branch:" }).fill("feature-branch");
  await page.getByRole("button", { name: "Create Branch" }).click();

  await expect(page.getByTestId("branch-status")).toContainText("feature-branch");

  // Make changes on the branch
  await page.getByRole("textbox", { name: "Name" }).fill("Jane Branch");
  await page.getByRole("textbox", { name: "Dog" }).fill("Branch Dog");

  // Merge the branch
  await page.getByRole("button", { name: "Merge to Main" }).click();

  // Verify we're back on main branch
  await expect(page.getByTestId("branch-status")).toContainText("Main branch");

  // Verify the changes are now on main
  await expect(page.getByTestId("person-name")).toContainText("Jane Branch");
  await expect(page.getByTestId("person-dog-name")).toContainText("Branch Dog");
});
