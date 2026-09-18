import { expect, it } from "vitest";
import { build } from "esbuild";
import { execFile as rawExecFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { app, permissions } from "./fixtures/initial-handoff-child.js";

const execFile = promisify(rawExecFile);
it("resumes an excluded author's initial handoff in a fresh Node process", async () => {
  // Under node_modules so the child resolves installed external dependencies;
  // this generated fixture is ignored by the source-bound artefact manifest.
  const cache = join(process.cwd(), "node_modules", ".cache");
  await mkdir(cache, { recursive: true });
  const directory = await mkdtemp(join(cache, "e2ee-process-handoff-"));
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let recipient: Awaited<ReturnType<typeof createDb>> | undefined;
  try {
    const entry = join(directory, "child.mjs");
    await build({
      entryPoints: [fileURLToPath(new URL("./fixtures/initial-handoff-child.ts", import.meta.url))],
      outfile: entry,
      bundle: true,
      platform: "node",
      format: "esm",
      packages: "external",
      logLevel: "silent",
    });
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const account = await localAccountConfig(server.appId, server.url);
    let saved: string | null = null;
    recipient = await createDb({
      ...account,
      e2ee: {
        app,
        store: {
          async read() {
            return saved;
          },
          async update(transform) {
            saved = transform(saved);
          },
        },
      },
    });
    await recipient.e2ee.devices.list();
    const run = async (phase: string, projectId?: string) => {
      const { stdout } = await execFile(
        process.execPath,
        [
          entry,
          JSON.stringify({
            phase,
            projectId,
            directory,
            appId: server.appId,
            serverUrl: server.url,
            recipientId: account.account.id,
          }),
        ],
        {
          env: { ...process.env, JAZZ_E2EE_HANDOFF_CHILD: "1" },
          timeout: 30_000,
          maxBuffer: 128 * 1024,
        },
      );
      const result = stdout.split("\n").find((line) => line.startsWith("E2EE_RESULT "));
      expect(result).toBeDefined();
      return JSON.parse(result!.slice("E2EE_RESULT ".length));
    };
    const before = await run("interrupt");
    expect(
      await recipient.e2ee.explain({ scope: app.projects, identifier: before.projectId }),
    ).toEqual({ state: "unavailable", reason: "space-key-not-delivered" });
    const after = await run("resume", before.projectId);
    expect(after).toEqual(before);
    expect(
      await recipient.e2ee.explain({ scope: app.projects, identifier: before.projectId }),
    ).toEqual({ state: "ready" });
    const deliveries = await recipient.all(app.__e2ee_space_deliveries, { tier: "global" });
    expect(deliveries).toHaveLength(1);
    expect(deliveries[0]).toMatchObject({
      epochId: before.epochId,
      senderDeviceId: before.deviceId,
      recipientAccountId: account.account.id,
    });
    expect(await recipient.all(app.__e2ee_space_successors, { tier: "global" })).toEqual([]);
  } finally {
    await recipient?.shutdown();
    await server.stop();
    await rm(directory, { recursive: true, force: true });
  }
}, 60_000);
