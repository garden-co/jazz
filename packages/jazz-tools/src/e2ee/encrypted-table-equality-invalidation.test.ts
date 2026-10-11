import { it } from "vitest";
import { runEncryptedTableScenario } from "./fixtures/encrypted-table-scenarios.js";

it.each([
  "equality-membership-invalidation",
  "equality-device-invalidation",
  "equality-device-invalidation-recipient",
  "equality-subscription",
  "equality-subscription-collision",
] as const)(
  "atomically creates a creator-owned space and encrypted data through ordinary mutations (%s)",
  runEncryptedTableScenario,
  120_000,
);

it(
  "retries foreground equality queries and refreshes live results across the same epoch rotation",
  () => runEncryptedTableScenario("equality-epoch-race"),
  120_000,
);
