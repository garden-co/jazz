import { test, expect } from '@playwright/test';

test.describe('Demo App Sync', () => {
  test.beforeEach(async ({ browser }) => {
    // Wait for server to be ready
    await new Promise(resolve => setTimeout(resolve, 1000));
  });

  test('app loads with sync enabled and connects', async ({ page }) => {
    const consoleLogs: string[] = [];
    page.on('console', msg => {
      const text = msg.text();
      console.log('[App]', text);
      consoleLogs.push(text);
    });

    // Navigate to demo app with sync enabled
    await page.goto('/?sync&persist=false');

    // Wait for sync to connect
    await page.waitForTimeout(5000);

    // Verify sync connected
    const syncConnected = consoleLogs.some(log => log.includes('Sync state: connected'));
    expect(syncConnected).toBe(true);
  });

  test('two tabs can connect to sync server', async ({ browser }) => {
    const context1 = await browser.newContext();
    const context2 = await browser.newContext();

    const page1 = await context1.newPage();
    const page2 = await context2.newPage();

    const logs1: string[] = [];
    const logs2: string[] = [];

    page1.on('console', msg => {
      const text = msg.text();
      console.log('[Tab1]', text);
      logs1.push(text);
    });
    page2.on('console', msg => {
      const text = msg.text();
      console.log('[Tab2]', text);
      logs2.push(text);
    });

    try {
      // Navigate both to demo app with sync enabled
      await page1.goto('/?sync&persist=false');
      await page2.goto('/?sync&persist=false');

      // Wait for both to connect
      await page1.waitForTimeout(5000);
      await page2.waitForTimeout(3000);

      // Verify both connected
      const tab1Connected = logs1.some(log => log.includes('Sync state: connected'));
      const tab2Connected = logs2.some(log => log.includes('Sync state: connected'));

      expect(tab1Connected).toBe(true);
      expect(tab2Connected).toBe(true);

    } finally {
      await context1.close();
      await context2.close();
    }
  });

  test('issue edit in one tab syncs to another tab list', async ({ browser }) => {
    const context1 = await browser.newContext();
    const context2 = await browser.newContext();

    const page1 = await context1.newPage();
    const page2 = await context2.newPage();

    page1.on('console', msg => console.log('[Tab1]', msg.text()));
    page2.on('console', msg => console.log('[Tab2]', msg.text()));

    try {
      // Tab1 initializes first, generates fake data
      await page1.goto('/?sync&persist=false');
      await page1.waitForTimeout(8000); // Wait for fake data generation and sync

      // Tab2 initializes with nofake - waits for synced data instead of generating its own
      await page2.goto('/?sync&persist=false&nofake');
      await page2.waitForTimeout(5000); // Wait for sync to receive data

      // Find an issue that exists in tab1 (with fake data)
      const firstIssueRow = page1.locator('[data-issue-id]').first();
      await expect(firstIssueRow).toBeVisible({ timeout: 10000 });
      const issueId = await firstIssueRow.getAttribute('data-issue-id');
      const originalTitle = await firstIssueRow.locator('[data-testid="issue-title"]').textContent();
      console.log(`Found issue ${issueId} with title: ${originalTitle}`);

      // Verify the same issue exists in tab2 (synced from tab1)
      const issueInTab2 = page2.locator(`[data-issue-id="${issueId}"]`);
      await expect(issueInTab2).toBeVisible({ timeout: 10000 });

      // Click on the issue to open detail pane in tab2
      await issueInTab2.click();
      await page2.waitForTimeout(500);

      // Edit the title in tab2's detail pane
      const newTitle = `Edited-${Date.now()}`;
      const titleSpan = page2.locator('span.cursor-pointer.hover\\:text-primary').first();
      await titleSpan.click(); // Click to enter edit mode

      const titleInput = page2.locator('[data-testid="issue-title-input"]');
      await expect(titleInput).toBeVisible({ timeout: 5000 });
      await titleInput.fill(newTitle);

      // Blur to trigger save
      await titleInput.blur();
      await page2.waitForTimeout(2000); // Wait for sync

      // Verify the title updated in tab1's LIST (not detail pane)
      const updatedTitleInTab1 = page1.locator(`[data-issue-id="${issueId}"] [data-testid="issue-title"]`);
      await expect(updatedTitleInTab1).toHaveText(newTitle, { timeout: 10000 });

    } finally {
      await context1.close();
      await context2.close();
    }
  });
});
