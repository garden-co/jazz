import { beforeEach } from "vitest";
import { cojsonInternals } from "cojson";
import { registerStorageCleanupRunner } from "./src/tools/tests/testStorage.js";

// Use a very high budget to avoid that slow tests fail due to the budget being exceeded.
cojsonInternals.setIncomingMessagesTimeBudget(10000); // 10 seconds

beforeEach(() => {
  registerStorageCleanupRunner();
});
