import { test, expect } from '@playwright/test';

test.describe('participants', () => {
	test.beforeEach(async ({ page }) => {
		await page.goto('/');
		const startButton = page.locator('.start-prompt button');
		await expect(startButton).toBeVisible({ timeout: 10_000 });
		await startButton.click();
		await expect(page.locator('.participants')).toBeVisible({ timeout: 10_000 });
	});

	test('shows The Band heading', async ({ page }) => {
		await expect(page.locator('.participants h2')).toHaveText('The Band');
	});

	test('current user appears with (you) tag', async ({ page }) => {
		const youTag = page.locator('.you-tag');
		await expect(youTag).toBeVisible({ timeout: 10_000 });
		await expect(youTag).toHaveText('(you)');
	});

	test('participant has an avatar and name', async ({ page }) => {
		const participant = page.locator('.participant').first();
		await expect(participant).toBeVisible({ timeout: 10_000 });
		await expect(participant.locator('.avatar')).toBeVisible();
		await expect(participant.locator('.name')).toBeVisible();

		// Name should not be empty
		const name = await participant.locator('.name').textContent();
		expect(name!.trim().length).toBeGreaterThan(0);
	});

	test('sync playback toggle is present and checked by default', async ({ page }) => {
		const toggle = page.locator('.sync-toggle input[type="checkbox"]');
		await expect(toggle).toBeVisible();
		await expect(toggle).toBeChecked();
	});

	test('sync playback toggle can be unchecked', async ({ page }) => {
		const toggle = page.locator('.sync-toggle input[type="checkbox"]');
		await toggle.uncheck();
		await expect(toggle).not.toBeChecked();
	});
});
