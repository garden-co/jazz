import { it } from "vitest";
import { runOfflineFounderScenario } from "./fixtures/offline-founder-fixture.js";

it(
  "keeps automatic offline initialization fail-closed across plaintext-transaction",
  () => runOfflineFounderScenario("plaintext-transaction"),
  60_000,
);
