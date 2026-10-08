import { it } from "vitest";
import { runEncryptedTableScenario } from "./fixtures/encrypted-table-scenarios.js";

it.each([
  "projection",
  "nested-results",
  "nested-projection",
  "nested-reference",
  "subscription-projection",
  "subscription",
  "subscription-error",
] as const)(
  "atomically creates a creator-owned space and encrypted data through ordinary mutations (%s)",
  runEncryptedTableScenario,
  120_000,
);
