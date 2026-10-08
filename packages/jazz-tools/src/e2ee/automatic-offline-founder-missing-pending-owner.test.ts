import { it } from "vitest";
import { runOfflineFounderScenario } from "./fixtures/offline-founder-fixture.js";

it(
  "keeps automatic offline initialization fail-closed across missing-pending-owner",
  () => runOfflineFounderScenario("missing-pending-owner"),
  60_000,
);
