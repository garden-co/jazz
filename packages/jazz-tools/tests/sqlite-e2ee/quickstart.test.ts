import { expect, inject, it } from "vitest";
import { commands } from "vitest/browser";
import { app } from "../../../../examples/e2ee-chat/quickstart/schema.js";
import permissions from "../../../../examples/e2ee-chat/quickstart/permissions.js";
import { deploy } from "../../src/dev/catalogue.js";
import wasmUrl from "../../../../crates/jazz-wasm/pkg/jazz_wasm_bg.wasm?url";
import type { QuickstartResult } from "./quickstart-node.js";
import type {} from "./setup.js";

it("runs the React quickstart with owner-only sharing, live messages and restored accounts", async () => {
  const server = inject("sqliteE2eeServer");
  await deploy({ ...server, schema: app, permissions });
  const worker = Reflect.get(commands, "sqliteE2eeWorkerUrl");
  const scenario = Reflect.get(commands, "sqliteQuickstartScenario");
  if (typeof worker !== "function" || typeof scenario !== "function")
    throw new Error("Quickstart browser commands are missing");
  const result: QuickstartResult = await scenario.call(commands, {
    appId: server.appId,
    serverUrl: server.serverUrl,
    runtimeSources: {
      wasmUrl: new URL(wasmUrl, location.href).href,
      brokerWorkerUrl: new URL(await worker.call(commands), location.href).href,
      wasmVersion: "sqlite-quickstart-test",
    },
  });
  expect(new Set(result.accounts).size).toBe(3);
  expect(result.bobOpened).toBe(result.accounts[1]);
  expect(result.bobReopened).toBe(result.accounts[1]);
  expect(result.aliceReopened).toBe(result.accounts[0]);
  for (const rows of [
    result.aliceMessages,
    result.bobMessages,
    result.aliceRestoredMessages,
    result.bobRestoredMessages,
  ]) {
    expect(rows).toEqual([result.first, result.reply]);
  }
  expect(result.bobShareControls).toBe(0);
  expect(result.unauthorizedGrant).toMatchObject({ code: "permission_denied" });
  expect(result.malloryBeforeGrant).toEqual([]);
  expect(result.malloryAfterReply).toEqual([]);
  expect(result.malloryMessages).toEqual([]);
  expect(result.alerts).toEqual([[], [], []]);
  expect(result.browserErrors).toEqual([]);
}, 120_000);
