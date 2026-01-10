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
});
