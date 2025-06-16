import { expect, test } from '@playwright/test';

test('should show the and update properties', async ({ page }) => {
  await page.goto('/costate');
  
  await page.getByRole('button', { name: "Select person [0]" }).click();
  
  await expect(page.getByRole("textbox", { name: "Name" })).toHaveValue("John");
  await expect(page.getByRole("textbox", { name: "Dog" })).toHaveValue("Rex");

  await expect(page.getByTestId("person-name")).toContainText("John");
  await expect(page.getByTestId("person-dog-name")).toContainText("Rex");

  await page.getByRole("textbox", { name: "Name" }).fill("Jane");
  await expect(page.getByTestId("person-name")).toContainText("Jane");

  await page.getByRole("textbox", { name: "Dog" }).fill("Bibi");
  await expect(page.getByTestId("person-dog-name")).toContainText("Bibi");
});

test('should react to id changes', async ({ page }) => {
  await page.goto('/costate');

  await page.getByRole('button', { name: "Select person [0]" }).click();

  await expect(page.getByTestId("person-name")).toContainText("John");
  await expect(page.getByTestId("person-dog-name")).toContainText("Rex");
  
  await page.getByRole('button', { name: "Select person [1]" }).click();

  await expect(page.getByTestId("person-name")).toContainText("Mathieu");
  await expect(page.getByTestId("person-dog-name")).toContainText("Bibi");

  await page.getByRole('button', { name: "Set undefined" }).click();

  await expect(page.getByTestId("person-name")).not.toBeVisible();
  await expect(page.getByTestId("person-dog-name")).not.toBeVisible();

  await page.getByRole('button', { name: "Select person [0]" }).click();

  await expect(page.getByTestId("person-name")).toContainText("John");
  await expect(page.getByTestId("person-dog-name")).toContainText("Rex");
})

test('should logout correctly', async ({ page }) => {
  await page.goto('/costate');
  
  await page.getByRole('button', { name: "Select person [0]" }).click();
  
  await page.getByRole("textbox", { name: "Name" }).fill("Jane");
  await page.getByRole("textbox", { name: "Dog" }).fill("Bibi");

  await page.getByRole('button', { name: "Logout" }).click();

  await page.getByRole('button', { name: "Select person [0]" }).click();

  await expect(page.getByRole("textbox", { name: "Name" })).toHaveValue("John");
  await expect(page.getByRole("textbox", { name: "Dog" })).toHaveValue("Rex");
});
