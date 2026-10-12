import { it } from "vitest";
import { runEncryptedTableScenario } from "./fixtures/encrypted-table-scenarios.js";

it(
  "retries empty and nonempty equality reads across rotation, keeps live mixed-epoch results complete and rejects missing history",
  () => runEncryptedTableScenario("equality-epoch-race"),
  120_000,
);
