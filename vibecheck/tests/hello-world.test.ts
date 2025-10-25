import { expect } from "vitest";
import scenario from "@langwatch/scenario";

import { getInstanceId, prepareTest } from "../lib/testing/prepare-test";

import { createV0Agent } from "../agents/v0";
import { createClaudeAgent } from "../agents/claude";

const instanceId = getInstanceId();
const projectName = "Hello World";

const v0Agent = createV0Agent({ projectName, instanceId });
const claudeAgent = createClaudeAgent({ projectName, instanceId });

prepareTest({
  projectName,
  instanceId,
  agent: v0Agent,
  timeoutSeconds: 90,
  judgeCriteria: [
    "Agent did not ask the user any follow-up questions",
    "Agent reasoned about the user's request",
    "Agent should write 'Hello, world!', centered on the screen",
    "Text should be centered using flexbox via Tailwind CSS",
  ],
  script: (agent) => [
    scenario.user("Write 'Hello, world!' centered on the screen"),
    scenario.agent(),
    () => {
      expect(agent.files).toHaveLength(2);
    },
    scenario.judge(),
  ],
});

prepareTest({
  projectName,
  instanceId,
  agent: claudeAgent,
  timeoutSeconds: 90,
  judgeCriteria: [
    "Agent reasoned about the user's request",
    "Agent should write 'Hello, world!', centered on the screen",
    "Text should be centered using flexbox",
  ],
  script: (agent) => [
    scenario.user("Write 'Hello, world!' centered on the screen"),
    scenario.agent(),
    () => {
      expect(agent.files).toHaveLength(1);
    },
    scenario.judge(),
  ],
});
