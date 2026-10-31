import { startLocalJazzServer } from "jazz-tools/testing";
import { deploy } from "jazz-tools/dev";
import {
  ADMIN_SECRET,
  APP_ID,
  SEEDED_TODO_COUNT,
  TEST_PORT,
} from "../tests/browser/test-constants.js";
import { app, permissions } from "../tests/browser/schema.ts";
import { createJazzBackend, type JazzBackend } from "jazz-tools/backend";

const SEED_BATCH_SIZE = 50;

export default async function runServer({
  port = TEST_PORT,
  serverPermissions = permissions,
}: { port?: number; serverPermissions?: typeof permissions } = {}) {
  const serverHandle = await startLocalJazzServer({
    appId: APP_ID,
    port,
    adminSecret: ADMIN_SECRET,
    backendSecret: "test",
  });

  let backend: JazzBackend | undefined;
  try {
    await deploy({
      serverUrl: serverHandle.url,
      appId: serverHandle.appId,
      adminSecret: serverHandle.adminSecret,
      schema: app,
      permissions: serverPermissions,
    });

    backend = await createJazzBackend({
      appId: serverHandle.appId,
      app: app,
      permissions: serverPermissions,
      driver: { type: "memory" },
      serverUrl: serverHandle.url,
      backendSecret: serverHandle.backendSecret,
    });

    const sessionedClient = backend.db;

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

    await backend.close();
    return {
      serverHandle,
    };
  } catch (error) {
    await Promise.all([backend?.close(), serverHandle.stop()]).catch((cleanupError) => {
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
      result.serverHandle.appId,
  );
  setInterval(() => {}, 10_000_000);
}
