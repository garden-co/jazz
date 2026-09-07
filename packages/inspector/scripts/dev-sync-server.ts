import { startLocalJazzServer } from "jazz-tools/testing";
import { deploy } from "jazz-tools/dev";
import {
  ADMIN_SECRET,
  APP_ID,
  SEEDED_TODO_COUNT,
  TEST_PORT,
} from "../tests/browser/test-constants.js";
import { app, permissions } from "../tests/browser/schema.ts";
import { createJazzContext } from "jazz-tools/backend";

const SEED_BATCH_SIZE = 50;

export default async function runServer({ port = TEST_PORT }: { port?: number } = {}) {
  const serverHandle = await startLocalJazzServer({
    appId: APP_ID,
    port,
    adminSecret: ADMIN_SECRET,
    backendSecret: "test",
  });

  let context: ReturnType<typeof createJazzContext> | undefined;
  try {
    await deploy({
      serverUrl: serverHandle.url,
      appId: serverHandle.appId,
      adminSecret: serverHandle.adminSecret,
      schema: app,
      permissions,
    });

    context = createJazzContext({
      appId: serverHandle.appId,
      app: app,
      permissions,
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
          return sessionedClient
            .insert(app.todos, {
              title: title,
              done: seedIndex % 2 === 1,
            })
            .wait({ tier: "global" });
        }),
      );
    }

    await context.shutdown();
    return {
      serverHandle,
    };
  } catch (error) {
    await Promise.all([context?.shutdown(), serverHandle.stop()]).catch((cleanupError) => {
      console.error("Inspector seed cleanup after setup failure failed", cleanupError);
    });
    throw error;
  }
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
    "Open dev inspector at http://localhost:5173/#serverUrl=" +
      result.serverHandle.url +
      "&appId=" +
      result.serverHandle.appId +
      "&adminSecret=" +
      result.serverHandle.adminSecret,
  );
  setInterval(() => {}, 10_000_000);
}
