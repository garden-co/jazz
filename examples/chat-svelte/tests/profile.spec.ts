import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, test } from '@playwright/test';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

test.describe('Profile Management', () => {
  test('Should update profile name without losing focus', async ({ page }) => {
    await test.step('Initial Load', async () => {
      await page.goto('/');
      const headerName = page.locator('header h3');
      await expect(headerName).toBeVisible({ timeout: 15000 });
    });

    await test.step('Open Profile Sheet', async () => {
      await page.locator('header button.flex.gap-2.items-center').click();
      await page.getByRole('menuitem', { name: /profile/i }).click({ force: true });
      const logoutButton = page.locator('#profile-logout');
      await expect(logoutButton).toBeVisible({ timeout: 15000 });
    });

    const newName = `Test User ${Math.floor(Math.random() * 1000)}`;
    await test.step('Update Name', async () => {
      const nameInput = page.getByLabel('Name');
      await nameInput.fill('');
      await nameInput.type(newName, { delay: 50 });
    });

    await test.step('Close Sheet and Verify', async () => {
      await page.keyboard.press('Escape');
      const headerName = page.locator('header h3');
      await expect(headerName).toHaveText(newName, { timeout: 10000 });
    });
  });

  test('Should change and remove avatar', async ({ page }) => {
    await test.step('Initial Load', async () => {
      await page.goto('/');
      await expect(page.locator('header h3')).toBeVisible({ timeout: 15000 });
    });

    await test.step('Open Profile Sheet', async () => {
      await page.locator('header button.flex.gap-2.items-center').click();
      await page.getByRole('menuitem', { name: /profile/i }).click({ force: true });
    });

    const testImagePath = path.join(__dirname, 'test-avatar.png');
    await test.step('Prepare test file', async () => {
      const tinyPng = Buffer.from(
        'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg==',
        'base64'
      );
      fs.writeFileSync(testImagePath, tinyPng);
    });

    await test.step('Upload Avatar', async () => {
      const fileInput = page.locator('input[type="file"]#avatar');
      await fileInput.setInputFiles(testImagePath);
      const profileAvatarImg = page.locator('[role="dialog"] img.object-cover');
      await expect(profileAvatarImg).toBeVisible({ timeout: 10000 });
    });

    await test.step('Remove Avatar and Cleanup', async () => {
      const removeButton = page.getByRole('button', { name: /remove/i });
      await expect(removeButton).toBeVisible();
      await removeButton.click();

      const profileAvatarImg = page.locator('[role="dialog"] span.relative img.object-cover');
      await expect(profileAvatarImg).not.toBeVisible();
      fs.unlinkSync(testImagePath);
    });
  });
});

test.describe('Account Recovery', () => {
  test('Should recover account using passphrase', async ({ page }) => {
    await page.goto('/');
    const headerName = page.locator('header h3');
    await expect(headerName).toBeVisible({ timeout: 15000 });

    const uniqueName = `Recoverable User ${Math.floor(Math.random() * 10000)}`;
    let passphrase = '';

    await test.step('Set unique name and copy passphrase', async () => {
      await page.locator('header button.flex.gap-2.items-center').click();
      await page.getByRole('menuitem', { name: /profile/i }).click({ force: true });

      const nameInput = page.getByLabel('Name');
      await nameInput.fill(uniqueName);

      await page.getByRole('button', { name: /reveal/i }).click();
      const passphraseTextarea = page.locator('textarea');
      passphrase = await passphraseTextarea.inputValue();
      expect(passphrase).toBeTruthy();
    });

    await test.step('Log Out', async () => {
      const logoutButton = page.locator('#profile-logout');
      await expect(logoutButton).toBeVisible({ timeout: 15000 });
      await logoutButton.scrollIntoViewIfNeeded();
      await logoutButton.click();
      await expect(page.locator('[role="dialog"]')).toBeHidden();
      expect(headerName).not.toHaveText(uniqueName);
    });

    await test.step('Restore account via passphrase', async () => {
      await page.goto('/');
      await page.getByRole('button', { name: /log in/i }).click();
      await page.getByRole('tab', { name: /passphrase/i }).click();
      const textarea = page.locator('div[role="tabpanel"]:has-text("passphrase") textarea');
      await expect(textarea).toBeVisible();
      await textarea.fill(passphrase);
      await page.getByRole('button', { name: /log in using passphrase/i }).click();
    });

    await test.step('Verify restoration', async () => {
      await expect(headerName).toHaveText(uniqueName, { timeout: 15000 });
    });
  });

  test('Should recover account using passkey', async ({ page, browserName }) => {
    test.skip(browserName !== 'chromium', 'Virtual authenticator is best supported in Chromium');

    await page.goto('/');
    const headerName = page.locator('header h3');
    await expect(headerName).toBeVisible({ timeout: 15000 });

    const uniqueName = `Passkey User ${Math.floor(Math.random() * 10000)}`;

    await test.step('Set up Virtual Authenticator', async () => {
      const cdpSession = await page.context().newCDPSession(page);
      await cdpSession.send('WebAuthn.enable');
      await cdpSession.send('WebAuthn.addVirtualAuthenticator', {
        options: {
          protocol: 'ctap2',
          transport: 'internal',
          hasResidentKey: true,
          hasUserVerification: true,
          isUserVerified: true
        }
      });
    });

    await test.step('Register Passkey', async () => {
      await page.locator('header button.flex.gap-2.items-center').click();
      await page.getByRole('menuitem', { name: /profile/i }).click({ force: true });

      const nameInput = page.getByLabel('Name');
      await nameInput.fill(uniqueName);

      const registerButton = page.locator('#passkey-register');
      await registerButton.click();

      await expect(page.locator('[role="dialog"]')).toBeHidden({
        timeout: 15000
      });
    });

    await test.step('Log Out', async () => {
      await page.reload();
      await expect(headerName).toBeVisible({ timeout: 15000 });

      await page.locator('header button.flex.gap-2.items-center').click({ force: true });
      await page.waitForTimeout(300);
      await page.getByRole('menuitem', { name: /profile/i }).click({ force: true });

      const logoutButton = page.locator('#profile-logout');
      await expect(logoutButton).toBeVisible({ timeout: 15000 });
      await logoutButton.scrollIntoViewIfNeeded();
      await logoutButton.click();
      await expect(page.locator('[role="dialog"]')).toBeHidden();
      expect(headerName).not.toHaveText(uniqueName);
    });

    await test.step('Restore account via passkey', async () => {
      await page.goto('/');
      await page.getByRole('button', { name: /log in/i }).click();
      const loginButton = page.locator('#passkey-login');
      await expect(loginButton).toBeVisible();
      await loginButton.click();
    });

    await test.step('Verify restoration', async () => {
      await expect(headerName).toHaveText(uniqueName, { timeout: 15000 });
    });
  });
});
