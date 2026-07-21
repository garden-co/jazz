import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Db } from "jazz-tools";
import { createJazzContext } from "jazz-tools/backend";
import permissions from "../../../server/permissions.js";
import { app } from "../../../shared/schema.js";

export async function withTestDatabase(run: (database: Db) => Promise<void>) {
  const dataDirectory = mkdtempSync(join(tmpdir(), "jazz-example-test-"));
  const context = createJazzContext({
    appId: randomUUID(),
    app,
    permissions,
    driver: { type: "persistent", dataPath: join(dataDirectory, "jazz.db") },
    env: "test",
    userBranch: "main",
  });

  try {
    await run(context.db());
  } finally {
    await context.shutdown();
    rmSync(dataDirectory, { recursive: true, force: true });
  }
}
