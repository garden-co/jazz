import { test as base } from "@playwright/test";

export { expect, type Page } from "@playwright/test";

export const test = base.extend({
  // Playwright requires an object pattern even when there are no dependencies.
  // oxlint-disable-next-line no-empty-pattern
  baseURL: async ({}, use) => {
    const url = process.env.JAZZ_INSPECTOR_TEST_WEB_URL;
    if (!url) throw new Error("Inspector browser global setup did not publish its web URL");
    await use(url);
  },
});
