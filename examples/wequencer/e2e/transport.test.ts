import { test, expect } from '@playwright/test';

test.describe('transport controls', () => {
	test.beforeEach(async ({ page }) => {
		await page.goto('/');
		const startButton = page.locator('.start-prompt button');
		await expect(startButton).toBeVisible({ timeout: 10_000 });
		await startButton.click();
		await expect(page.locator('.transport')).toBeVisible({ timeout: 10_000 });
	});

	test('play button starts playback and shows stop button', async ({ page }) => {
		await expect(page.locator('.play-btn')).toBeVisible();
		await expect(page.locator('.stop-btn')).not.toBeVisible();

		await page.locator('.play-btn').click();

		await expect(page.locator('.stop-btn')).toBeVisible({ timeout: 10_000 });
		await expect(page.locator('.play-btn')).not.toBeVisible();
	});

	test('stop button stops playback and shows play button', async ({ page }) => {
		// Start playback
		await page.locator('.play-btn').click();
		await expect(page.locator('.stop-btn')).toBeVisible({ timeout: 10_000 });

		// Stop playback
		await page.locator('.stop-btn').click();

		await expect(page.locator('.play-btn')).toBeVisible({ timeout: 10_000 });
		await expect(page.locator('.stop-btn')).not.toBeVisible();
	});

});
