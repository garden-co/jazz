import { expect, test } from '@playwright/test';

test.describe('Chat App E2E', () => {
  test('Complete User Flow', async ({ page }) => {
    const messageEditor = page.locator('#messageEditor [contenteditable="true"]');
    const sendButton = page.getByRole('button').filter({ has: page.getByTestId('send-message') });
    const menuButton = page.getByRole('button').filter({ has: page.getByTestId('menu') });

    await test.step('Initial Load', async () => {
      await page.goto('/');
      await page.waitForURL('**/#/chat/*');
      await expect(messageEditor).toBeVisible();
      await expect(page.getByText('Hello world')).toBeVisible();
    });

    await test.step('Send a public message', async () => {
      const messageText = 'Hello Public 2';
      await messageEditor.fill(messageText);
      await sendButton.click();
      await expect(page.getByText(messageText)).toBeVisible();
    });

    await test.step('React to a message', async () => {
      await page.getByText('Hello world').first().click();
      await page.getByRole('menuitem', { name: /react/i }).hover();
      await page.getByRole('button', { name: '❤️' }).click();
      await expect(
        page.locator('article').filter({ hasText: 'Hello world' }).getByText('❤️')
      ).toBeVisible();
    });

    await test.step('Delete a message', async () => {
      const msgText = 'Message to delete';
      await messageEditor.fill(msgText);
      await sendButton.click();

      const messageBubble = page.locator('article', { hasText: msgText }).first();
      await messageBubble.click();

      await page.getByRole('menuitem', { name: /delete/i }).click();
      await page
        .getByRole('alertdialog')
        .getByRole('button', { name: /yes, delete it/i })
        .click();

      await expect(page.getByText(msgText)).not.toBeVisible();
    });

    await test.step('Create public chat via List', async () => {
      await menuButton.click();
      await page.getByRole('menuitem', { name: /chat list/i }).click();
      await page.getByRole('button', { name: /new chat/i }).click();
      await expect(page).toHaveURL(/\/chat\//);
      await expect(page.getByText('Hello world')).toBeVisible();
    });

    let privateChatUrl = '';
    await test.step('Create private chat', async () => {
      await menuButton.click();
      await page.getByRole('menuitem', { name: /chat list/i }).click();
      await page.getByRole('button', { name: /new private chat/i }).click();
      await expect(page).toHaveURL(/\/chat\//);
      privateChatUrl = page.url();

      await messageEditor.fill('Secret Data');
      await sendButton.click();
      await expect(page.getByText('Secret Data')).toBeVisible();
    });

    await test.step('Logout and verify private chat access', async () => {
      await menuButton.click({ force: true });
      await page.getByRole('menuitem', { name: /profile/i }).click();
      await page.getByRole('button', { name: /log out/i }).click();
      await expect(page.locator('[role="dialog"]')).toBeHidden();

      await page.goto(privateChatUrl);
      await expect(page.locator('body')).toContainText(/Something went wrong/i);
      await expect(page.getByText('Secret Data')).not.toBeVisible();
    });
  });
});
