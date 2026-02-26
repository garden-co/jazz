import { test, expect } from '@playwright/test';

test.describe('app loading', () => {
	test('renders the nav with Wequencer title', async ({ page }) => {
		await page.goto('/');
		await expect(page.locator('nav h1')).toHaveText('Wequencer');
	});

	test('shows the Start Wequencing button before audio context is active', async ({ page }) => {
		await page.goto('/');
		const startButton = page.locator('.start-prompt button');
		await expect(startButton).toBeVisible({ timeout: 10_000 });
		await expect(startButton).toHaveText('Start Wequencing');
	});

	test('activating audio context reveals the sequencer grid', async ({ page }) => {
		await page.goto('/');
		const startButton = page.locator('.start-prompt button');
		await expect(startButton).toBeVisible({ timeout: 10_000 });
		await startButton.click();

		// Sequencer grid should appear with instrument rows
		const grid = page.locator('.sequencer .grid');
		await expect(grid).toBeVisible({ timeout: 10_000 });

		// Should have instrument name labels
		const instrumentNames = page.locator('.instrument-name');
		await expect(instrumentNames.first()).toBeVisible();
		const count = await instrumentNames.count();
		expect(count).toBeGreaterThanOrEqual(1);
	});

	test('shows transport controls after audio context is active', async ({ page }) => {
		await page.goto('/');
		await page.locator('.start-prompt button').click();

		const transport = page.locator('.transport');
		await expect(transport).toBeVisible({ timeout: 10_000 });

		// Play button should be visible
		await expect(page.locator('.play-btn')).toBeVisible();
	});

	test('shows participants panel after audio context is active', async ({ page }) => {
		await page.goto('/');
		await page.locator('.start-prompt button').click();

		const participants = page.locator('.participants');
		await expect(participants).toBeVisible({ timeout: 10_000 });
		await expect(participants.locator('h2')).toHaveText('The Band');

		// Current user should appear with (you) tag
		await expect(page.locator('.you-tag')).toBeVisible({ timeout: 10_000 });
	});

	test('shows instrument manager after audio context is active', async ({ page }) => {
		await page.goto('/');
		await page.locator('.start-prompt button').click();

		const manager = page.locator('.instrument-manager');
		await expect(manager).toBeVisible({ timeout: 10_000 });
	});
});
