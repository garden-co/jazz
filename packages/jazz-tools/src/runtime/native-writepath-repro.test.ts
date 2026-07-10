import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createJazzContext } from "../backend/create-jazz-context.js";
import { schema as s } from "../index.js";
import { startLocalJazzServer } from "../testing/index.js";

const importSchema = {
  parents: s.table({
    label: s.string(),
    ordinal: s.int(),
  }),
  children: s.table({
    parent: s.ref("parents"),
    label: s.string(),
    ordinal: s.int(),
  }),
};

const importApp = s.defineApp(importSchema);
const txCount = Number(process.env.JAZZ_WRITEPATH_TXS ?? "100");

function rowId(index: number): string {
  return `00000000-0000-4000-8000-${index.toString(16).padStart(12, "0")}`;
}

describe("native write path", () => {
  it("keeps fixed-size transaction cost flat as the store grows", async () => {
    const dir = mkdtempSync(join(tmpdir(), "jazz-writepath-repro-"));
    const context = createJazzContext({
      appId: `writepath-repro-${Date.now()}`,
      app: importApp,
      permissions: {},
      driver: { type: "persistent", dataPath: dir },
      adminSecret: "writepath-repro-admin",
      tier: "local",
    });
    const db = context.asBackend();
    const parentWrite = db.insert(importApp.parents, {
      label: "parent",
      ordinal: 0,
    });
    const parent = parentWrite.value;
    await parentWrite.wait({ tier: "local" });

    const timings: number[] = [];
    try {
      for (let txIndex = 0; txIndex < txCount; txIndex += 1) {
        const started = performance.now();
        const result = db.transaction((tx) => {
          for (let rowIndex = 0; rowIndex < 200; rowIndex += 1) {
            const ordinal = txIndex * 200 + rowIndex;
            tx.upsert(
              importApp.children,
              {
                parent: parent.id,
                label: `child-${ordinal}`,
                ordinal,
              },
              { id: rowId(ordinal) },
            );
          }
        });
        await result.wait({ tier: "local" });
        const elapsed = performance.now() - started;
        timings.push(elapsed);
        if (txIndex < 5 || (txIndex + 1) % 10 === 0) {
          console.info(`[writepath-repro] tx=${txIndex + 1} elapsedMs=${elapsed.toFixed(1)}`);
        }
      }
    } finally {
      await context.shutdown();
      rmSync(dir, { recursive: true, force: true });
    }
    const early = timings.slice(1, 6).reduce((sum, value) => sum + value, 0) / 5;
    const late = timings.slice(-5).reduce((sum, value) => sum + value, 0) / 5;
    console.info(
      `[writepath-repro] earlyAvgMs=${early.toFixed(1)} lateAvgMs=${late.toFixed(1)} ratio=${(late / early).toFixed(2)}`,
    );
    expect(late / Math.max(early, 0.001)).toBeLessThanOrEqual(3);
  }, 120_000);

  it("keeps fixed-size server-backed transaction cost flat as the store grows", async () => {
    const appId = `writepath-server-repro-${Date.now()}`;
    const dir = mkdtempSync(join(tmpdir(), "jazz-writepath-server-repro-"));
    const server = await startLocalJazzServer({
      appId,
      dataDir: dir,
      adminSecret: "writepath-repro-admin",
      backendSecret: "writepath-repro-backend",
    });
    const context = createJazzContext({
      appId,
      app: importApp,
      permissions: {},
      driver: { type: "memory" },
      serverUrl: server.url,
      adminSecret: server.adminSecret,
      backendSecret: server.backendSecret,
      env: "test",
      userBranch: "main",
      tier: "local",
    });
    const db = context.asBackend();
    const parentWrite = db.insert(importApp.parents, {
      label: "parent",
      ordinal: 0,
    });
    const parent = parentWrite.value;
    await parentWrite.wait({ tier: "local" });

    const timings: number[] = [];
    try {
      for (let txIndex = 0; txIndex < txCount; txIndex += 1) {
        const started = performance.now();
        const result = db.transaction((tx) => {
          for (let rowIndex = 0; rowIndex < 200; rowIndex += 1) {
            const ordinal = txIndex * 200 + rowIndex;
            tx.upsert(
              importApp.children,
              {
                parent: parent.id,
                label: `child-${ordinal}`,
                ordinal,
              },
              { id: rowId(ordinal) },
            );
          }
        });
        await result.wait({ tier: "local" });
        const elapsed = performance.now() - started;
        timings.push(elapsed);
        if (txIndex < 5 || (txIndex + 1) % 10 === 0) {
          console.info(
            `[writepath-server-repro] tx=${txIndex + 1} elapsedMs=${elapsed.toFixed(1)}`,
          );
        }
      }
    } finally {
      await context.shutdown();
      await server.stop();
      rmSync(dir, { recursive: true, force: true });
    }
    const early = timings.slice(1, 6).reduce((sum, value) => sum + value, 0) / 5;
    const late = timings.slice(-5).reduce((sum, value) => sum + value, 0) / 5;
    console.info(
      `[writepath-server-repro] earlyAvgMs=${early.toFixed(1)} lateAvgMs=${late.toFixed(1)} ratio=${(late / early).toFixed(2)}`,
    );
    expect(late / Math.max(early, 0.001)).toBeLessThanOrEqual(3);
  }, 180_000);
});
