import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import { fetchSchemaHashes, JazzClient } from "jazz-tools";
import { TestingServer } from "../../../../crates/jazz-napi/index.js";
import { pushSchemaCatalogue } from "../../../jazz-tools/dist/testing/local-jazz-server.js";
import {
  ADMIN_SECRET,
  APP_ID,
  SEEDED_TODO_COUNT,
  TEST_BRANCH,
  TEST_ENV,
  TEST_PORT,
} from "./test-constants.js";
import { app } from "./schema.ts";

const SEED_BATCH_SIZE = 50;
const RUNTIME_CONFIG_PATH = join(import.meta.dirname ?? __dirname, "runtime-config.json");

export default async function globalSetup(): Promise<() => Promise<void>> {
  const serverHandlePromise = TestingServer.start({
    appId: APP_ID,
    port: TEST_PORT,
    adminSecret: ADMIN_SECRET,
  });

  const serverHandle = await serverHandlePromise;
  await pushSchemaCatalogue({
    serverUrl: serverHandle.url,
    appId: serverHandle.appId,
    adminSecret: serverHandle.adminSecret,
    env: TEST_ENV,
    userBranch: TEST_BRANCH,
    schemaDir: join(import.meta.dirname ?? __dirname, "."),
  });
  const { hashes } = await fetchSchemaHashes(serverHandle.url, {
    adminSecret: serverHandle.adminSecret,
  });
  const publishedSchemaHash = hashes.at(-1);
  if (!publishedSchemaHash) {
    throw new Error("No schema hashes were published during inspector browser global setup.");
  }

  process.env.PUBLISHED_SCHEMA_HASH = publishedSchemaHash;

  const client = await JazzClient.connect({
    appId: APP_ID,
    schema: app.wasmSchema,
    serverUrl: serverHandle.url,
    env: TEST_ENV,
    userBranch: TEST_BRANCH,
    adminSecret: ADMIN_SECRET,
    auth: { localFirstSecret: "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8" },
  });

  const seedTitles = buildSeedTodoTitles(SEEDED_TODO_COUNT);
  for (let offset = 0; offset < seedTitles.length; offset += SEED_BATCH_SIZE) {
    const batch = seedTitles.slice(offset, offset + SEED_BATCH_SIZE);
    await Promise.all(
      batch.map((title, indexWithinBatch) => {
        const seedIndex = offset + indexWithinBatch;
        return client.createDurable(
          "todos",
          {
            title: { type: "Text", value: title },
            done: { type: "Boolean", value: seedIndex % 2 === 1 },
          },
          { tier: "edge" },
        );
      }),
    );
  }

  await client.shutdown();

  return async () => {
    await serverHandle.stop();
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
