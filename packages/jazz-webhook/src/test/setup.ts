import { beforeEach, afterEach } from "vitest";
import { setupJazzTestSync } from "jazz-tools/testing";

// Global test setup
beforeEach(async () => {
  await setupJazzTestSync();
});

// Clean up after each test
afterEach(async () => {
  // Clean up any global state if needed
});
