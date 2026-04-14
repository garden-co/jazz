import { createRequire } from "node:module";
import { onTestFinished } from "vitest";
import type { WasmSchema } from "../../drivers/types.js";
import { serializeRuntimeSchema } from "../../drivers/schema-wire.js";
import type { Runtime } from "../client.js";

type NapiModule = typeof import("jazz-napi");
export type TestNapiRuntime = Runtime & {
  close?: () => void;
  transportEverConnected?: () => boolean;
};

const require = createRequire(import.meta.url);

let napiModulePromise: Promise<NapiModule> | null = null;

function registerRuntimeCleanup(runtime: { close?: () => void }): void {
  onTestFinished(() => {
    try {
      runtime.close?.();
    } catch {
      // Best effort cleanup for native runtimes during test shutdown.
    }
  });
}

function formatNapiLoadError(error: unknown): Error {
  const message = error instanceof Error ? error.message : String(error);
  return new Error(
    `jazz-napi build artifacts not found or failed to load. Run \`pnpm --filter jazz-napi build:debug\` first.\n\nOriginal error: ${message}`,
  );
}

export function hasJazzNapiBuild(): boolean {
  try {
    require("jazz-napi");
    return true;
  } catch {
    return false;
  }
}

export async function loadNapiModule(): Promise<NapiModule> {
  if (!napiModulePromise) {
    napiModulePromise = Promise.resolve().then(() => {
      try {
        return require("jazz-napi") as NapiModule;
      } catch (error) {
        throw formatNapiLoadError(error);
      }
    });
  }

  return napiModulePromise;
}

export async function createNapiRuntime(
  schema: WasmSchema,
  opts?: {
    appId?: string;
    env?: string;
    userBranch?: string;
    tier?: string;
  },
): Promise<TestNapiRuntime> {
  const { NapiRuntime } = await loadNapiModule();
  const runtime = NapiRuntime.inMemory(
    serializeRuntimeSchema(schema),
    opts?.appId ?? "test-app",
    opts?.env ?? "test",
    opts?.userBranch ?? "main",
    opts?.tier,
  );

  registerRuntimeCleanup(runtime);

  return runtime as unknown as TestNapiRuntime;
}

export async function createPersistentNapiRuntime(
  schema: WasmSchema,
  dataPath: string,
  opts?: {
    appId?: string;
    env?: string;
    userBranch?: string;
    tier?: string;
  },
): Promise<TestNapiRuntime> {
  const { NapiRuntime } = await loadNapiModule();
  const runtime = new NapiRuntime(
    serializeRuntimeSchema(schema),
    opts?.appId ?? "test-app",
    opts?.env ?? "test",
    opts?.userBranch ?? "main",
    dataPath,
    opts?.tier,
  );

  registerRuntimeCleanup(runtime);

  return runtime as unknown as TestNapiRuntime;
}
