import { join } from "node:path";
import { pushSchemaCatalogue, TestingServer } from "jazz-tools/testing";
import {
  ADMIN_SECRET,
  APP_ID,
  SEEDED_TODO_COUNT,
  TEST_BRANCH,
  TEST_ENV,
  TEST_PORT,
} from "../tests/browser/test-constants.js";
import { app } from "../tests/browser/schema.ts";
import { createJazzContext } from "jazz-tools/backend";

const SEED_BATCH_SIZE = 50;

export default async function runServer() {
  const serverHandle = await TestingServer.start({
    appId: APP_ID,
    port: TEST_PORT,
    adminSecret: ADMIN_SECRET,
    backendSecret: "test",
  });

  await pushSchemaCatalogue({
    serverUrl: serverHandle.url,
    appId: serverHandle.appId,
    adminSecret: serverHandle.adminSecret,
    env: TEST_ENV,
    userBranch: TEST_BRANCH,
    schemaDir: join(import.meta.dirname ?? __dirname, "../tests/browser"),
  });

  const context = createJazzContext({
    appId: serverHandle.appId,
    app: app,
    permissions: {},
    driver: { type: "memory" },
    serverUrl: serverHandle.url,
    backendSecret: serverHandle.backendSecret,
    defaultDurabilityTier: "global",
  });

  const sessionedClient = context.asBackend();

  const seedTitles = buildSeedTodoTitles(SEEDED_TODO_COUNT);
  for (let offset = 0; offset < seedTitles.length; offset += SEED_BATCH_SIZE) {
    const batch = seedTitles.slice(offset, offset + SEED_BATCH_SIZE);
    await Promise.all(
      batch.map((title, indexWithinBatch) => {
        const seedIndex = offset + indexWithinBatch;
        return sessionedClient.insertDurable(
          app.todos,
          {
            title: title,
            done: seedIndex % 2 === 1,
          },
          { tier: "global" },
        );
      }),
    );
  }

  await context.shutdown();
  return {
    serverHandle,
  };
}

function buildSeedTodoTitles(count: number): string[] {
  const totalCount = Math.max(2, count);
  const titles = ["First seeded todo", "Second seeded todo"];
  for (let index = titles.length; index < totalCount; index += 1) {
    titles.push(`Seeded todo ${String(index + 1).padStart(6, "0")}`);
  }
  return titles;
}

if (import.meta.url === new URL(process.argv[1], "file://").href) {
  const result = await runServer();

  console.log("Server running at", result.serverHandle.url);
  console.log("Press Ctrl-C to stop");
  console.log(
    "Open dev inspector at http://localhost:5173/#url=" +
      result.serverHandle.url +
      "&adminSecret=" +
      result.serverHandle.adminSecret +
      "&appId=" +
      result.serverHandle.appId,
  );
  setInterval(() => {}, 10_000_000);
}
