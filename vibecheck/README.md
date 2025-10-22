# Vibecheck

**Test how effectively vibe coding agents work with Jazz.**

It uses [Langwatch Scenarios](https://scenario.langwatch.ai) to run and evaluate the coding agents behavior. Wrapped in a regular Vitest.

Vibecheck runs a coding agent on a task, then checks the results:
- programmatically (e.g. right files generated; code compiles, etc)
- using LLM-based evaluation judge (with criteria written in natural language).

Available coding agents ([source](./agents/)):
- v0
- Claude Code

## Setup and run

Install dependencies

```sh
pnpm i
```

Set necessary API keys (to OpenAI for evaluation; _optionally_ to LangWatch platform):

```sh
cp .env.example .env
```

### Set up coding agents

- **v0**: [Generate an API key](https://v0.app/chat/settings/keys) in v0 and set it in `.env` file.

- **Claude Code**: Run `npx claude /login` to start Claude Code in your terminal, then log into your Claude account.


### Run tests

Run all tests

```sh
pnpm test
```

Or all tests specific to a coding agent

```sh
pnpm test:v0
pnpm test:claude
```

Or specific test as you're used to with Vitest

```sh
pnpm test:claude hello-world
```

### Tests artifacts

All code written by coding agents will be saved and organized in `output` directory:

```
output
└── RUN_ID (e.g. cozy-river-bee)
    └── TEST_NAME (e.g. Hello World)
        ├── claude
        │   ├── ...
        │   └── index.html
        └── v0
            ├── ...
            └── package.json
```

## Creating tests

Create a `{name}.test.ts` file in `tests` directory.

1. Use [`prepareTest`](./lib/testing/prepare-test.ts) helper that has Scenario set up with Vitest.

2. Create an [agent](./agents/) instance in the state you want to test: with no prior knowledge, with custom knowledge, links, or connected to MCP.

3. Create a test script (_as in “story”_) for the test run
  - trigger agent by sending “user” message describing the tested task (_e.g. “Create an app that...“_)
  - let the agent have its turn
  - then you can call the LLM-based evaluation judge, or make programmatic assertions about the output

5. Write down the criteria for the evaluation of the results.

```typescript
// hello-world.test.ts

prepareTest({
  projectName,
  instanceId,
  agent: createV0Agent({ ... }), // pass agent instance, along with MCP, links, etc
  timeoutSeconds: 300,

  // Script defines the test turns (e.g. user->agent->assert->judge)
  script: (agent) => [
    scenario.user("Write 'Hello, world!' centered on the screen"),
    scenario.agent(),
    () => {
      expect(agent.files).toHaveLength(2); // make programmatic assertions, regular Vitest
    },
    scenario.judge(), // run evaluation judge, based on criteria
  ],

  // Criteria is used by LLM-backed judge to evaluate the coding agent's work
  judgeCriteria: [
    "Agent did not ask the user any follow-up questions",
    "Agent reasoned about the user's request",
    "Agent should write 'Hello, world!', centered on the screen",
    "Text should be centered using flexbox via Tailwind CSS",
  ],
});
```