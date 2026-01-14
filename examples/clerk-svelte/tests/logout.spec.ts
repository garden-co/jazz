import { expect, test } from '@playwright/test';

test('login & logout', async ({ page }) => {
  await page.goto('/');

  await expect(page.getByText("You're not logged in")).toBeVisible();

  await page.getByRole('button', { name: 'Sign in' }).click();

  await page.getByRole('textbox', { name: 'Email address' }).fill('joe+clerk-test@garden.co');

  await page.keyboard.press('Enter');

  await page.getByRole('textbox', { name: 'Password' }).fill('joe+clerk-test@garden.co');

  await page.keyboard.press('Enter');

  await page.waitForURL('/');

  await expect(page.getByText("You're logged in")).toBeVisible();

  await page.getByRole('button', { name: 'Logout' }).click();

  await expect(page.getByText("You're not logged in")).toBeVisible();
});
