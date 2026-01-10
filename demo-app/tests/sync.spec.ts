import { test, expect, Page } from '@playwright/test';

test.describe('Groove Sync', () => {
  test.beforeEach(async ({ browser }) => {
    // Wait for server to be ready
    await new Promise(resolve => setTimeout(resolve, 1000));
  });

  test('two tabs can connect to sync server', async ({ browser }) => {
    // Create two browser contexts (like two separate tabs)
    const context1 = await browser.newContext();
    const context2 = await browser.newContext();

    const page1 = await context1.newPage();
    const page2 = await context2.newPage();

    try {
      // Navigate both to sync test page
      await page1.goto('/?sync-test');
      await page2.goto('/?sync-test');

      // Wait for both to initialize
      await expect(page1.getByTestId('status')).toContainText('Ready', { timeout: 10000 });
      await expect(page2.getByTestId('status')).toContainText('Ready', { timeout: 10000 });

      // Get tab IDs
      const tabId1 = await page1.getByTestId('tabId').textContent();
      const tabId2 = await page2.getByTestId('tabId').textContent();
      expect(tabId1).not.toBe(tabId2);

      // Connect both to sync server
      await page1.getByTestId('connectBtn').click();
      await page2.getByTestId('connectBtn').click();

      // Wait for connection
      await expect(page1.getByTestId('log')).toContainText('Connected', { timeout: 10000 });
      await expect(page2.getByTestId('log')).toContainText('Connected', { timeout: 10000 });

    } finally {
      await context1.close();
      await context2.close();
    }
  });

  test('insert is visible locally', async ({ page }) => {
    await page.goto('/?sync-test');

    // Wait for initialization
    await expect(page.getByTestId('status')).toContainText('Ready', { timeout: 10000 });

    // Insert a test row
    await page.getByTestId('insertBtn').click();

    // Verify it appears in the table contents
    await expect(page.getByTestId('tableContents')).toContainText('Item-', { timeout: 5000 });
  });

  // Test: Tab 2 connects after Tab 1 has already inserted data
  test('new subscriber receives existing data', async ({ browser }) => {
    const context1 = await browser.newContext();
    const context2 = await browser.newContext();

    const page1 = await context1.newPage();
    const page2 = await context2.newPage();

    // Capture console logs
    page1.on('console', msg => console.log('[Tab1]', msg.text()));
    page2.on('console', msg => console.log('[Tab2]', msg.text()));

    try {
      // Tab 1: Initialize and connect
      await page1.goto('/?sync-test');
      await expect(page1.getByTestId('status')).toContainText('Ready', { timeout: 10000 });
      await page1.getByTestId('connectBtn').click();
      await expect(page1.getByTestId('log')).toContainText('Connected', { timeout: 10000 });

      // Tab 1: Insert data BEFORE Tab 2 connects
      await page1.getByTestId('insertBtn').click();
      await expect(page1.getByTestId('tableContents')).toContainText('Item-', { timeout: 5000 });

      // Wait a moment for the push to complete
      await new Promise(resolve => setTimeout(resolve, 500));

      // Tab 2: Now initialize and connect
      await page2.goto('/?sync-test');
      await expect(page2.getByTestId('status')).toContainText('Ready', { timeout: 10000 });
      await page2.getByTestId('connectBtn').click();
      await expect(page2.getByTestId('log')).toContainText('Connected', { timeout: 10000 });

      // Tab 2 should see the data that Tab 1 inserted
      await expect(page2.getByTestId('tableContents')).toContainText('Item-', { timeout: 5000 });

    } finally {
      await context1.close();
      await context2.close();
    }
  });

  // Test full sync: Tab 1 inserts data, Tab 2 receives it via SSE
  test('data syncs between two tabs', async ({ browser }) => {
    const context1 = await browser.newContext();
    const context2 = await browser.newContext();

    const page1 = await context1.newPage();
    const page2 = await context2.newPage();

    // Capture console logs
    page1.on('console', msg => console.log('[Tab1]', msg.text()));
    page2.on('console', msg => console.log('[Tab2]', msg.text()));

    try {
      // Navigate and wait for initialization
      await page1.goto('/?sync-test');
      await page2.goto('/?sync-test');

      await expect(page1.getByTestId('status')).toContainText('Ready', { timeout: 10000 });
      await expect(page2.getByTestId('status')).toContainText('Ready', { timeout: 10000 });

      // Connect both
      await page1.getByTestId('connectBtn').click();
      await page2.getByTestId('connectBtn').click();

      await expect(page1.getByTestId('log')).toContainText('Connected', { timeout: 10000 });
      await expect(page2.getByTestId('log')).toContainText('Connected', { timeout: 10000 });

      // Tab 1 inserts data
      await page1.getByTestId('insertBtn').click();

      // Wait for table to show the item in tab 1
      await expect(page1.getByTestId('tableContents')).toContainText('Item-', { timeout: 5000 });

      // Tab 2 should see the same data via sync
      // NOTE: This requires the sync to actually work end-to-end
      await expect(page2.getByTestId('tableContents')).toContainText('Item-', { timeout: 10000 });

    } finally {
      await context1.close();
      await context2.close();
    }
  });
});
