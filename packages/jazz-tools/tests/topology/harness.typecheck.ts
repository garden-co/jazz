import type { TopologyScenario } from "./harness.js";

const cleanup: NonNullable<TopologyScenario["cleanup"]> = async (context) => {
  void context.signal;
  // @ts-expect-error Scenario cleanup cannot register a later compensation.
  context.defer("late cleanup", async () => undefined);
};

void cleanup;
