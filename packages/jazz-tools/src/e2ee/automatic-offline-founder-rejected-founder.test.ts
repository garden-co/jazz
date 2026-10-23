import { it } from "vitest";
import { runOfflineFounderScenario } from "./fixtures/offline-founder-fixture.js";

it(
  "keeps automatic offline initialization fail-closed across rejected-founder",
  () => runOfflineFounderScenario("rejected-founder"),
  60_000,
);
