import { test, expect, afterEach, describe } from "vitest";
import scenario, { ScriptStep } from "@langwatch/scenario";
import { openai } from "@ai-sdk/openai";

import { type CodingAgent } from "../coding-agent";
import { generateReadableId } from "../generate-readable-id";

export function getInstanceId() {
  return process.env.ID || generateReadableId();
}

export function prepareTest({
  projectName,
  agent,
  instanceId,
  judgeCriteria,
  script: generateScript,
  timeoutSeconds = 5 * 60, // 5 minutes
}: {
  projectName: string;
  agent: CodingAgent;
  instanceId: string;
  judgeCriteria: string[];
  script: (agent: CodingAgent) => ScriptStep[];
  timeoutSeconds?: number;
}) {
  const nameParts = [`[${agent.name}]`, projectName, `(${instanceId})`];

  const testName = nameParts.slice(0, 2).join(" ");
  const testInstanceName = nameParts.join(" ");

  describe.concurrent(agent.name, () => {
    afterEach(async () => {
      await agent.cleanupRemote();
    });

    test(
      testInstanceName,
      async () => {
        const result = await scenario.run({
          setId: instanceId,
          id: testName,
          name: testName,
          description: `Test of the coding agent's ability: ${projectName}`,
          verbose: true,
          agents: [
            agent.adapter,
            scenario.userSimulatorAgent(),
            scenario.judgeAgent({
              model: openai("gpt-4.1"),
              criteria: judgeCriteria,
            }),
          ],
          script: generateScript(agent),
        });

        expect(result.success).toBe(true);
      },
      timeoutSeconds * 1000,
    );
  });
}
