import { beforeEach, afterEach } from "vitest";
import { setupJazzTestSync, createJazzTestAccount } from "jazz-tools/testing";
import { WebhookRegistry } from "../index.js";

// Global test setup
beforeEach(async () => {
  await setupJazzTestSync();
});

// Clean up after each test
afterEach(async () => {
  // Clean up any global state if needed
});
