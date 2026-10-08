import { it } from "vitest";
import { runEncryptedTableScenario } from "./fixtures/encrypted-table-scenarios.js";

it.each([
  "equality-membership-invalidation",
  "equality-device-invalidation",
  "equality-device-invalidation-recipient",
  "equality-subscription",
  "equality-subscription-collision",
  "equality-subscription-epoch",
  "equality-epoch-race",
] as const)(
  "atomically creates a creator-owned space and encrypted data through ordinary mutations (%s)",
  runEncryptedTableScenario,
  120_000,
);
