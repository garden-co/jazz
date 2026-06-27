import { createRequire } from "node:module";
import { onTestFinished } from "vitest";
import type { WasmSchema } from "../../drivers/types.js";
import type { Runtime } from "../client.js";
import { CoreRuntime } from "../core-runtime/runtime.js";

type NapiModule = typeof import("jazz-napi");
export type TestNapiCoreRuntime = Runtime & { close?: () => void };

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

export async function createNapiCoreRuntime(
  schema: WasmSchema,
  opts?: {
    appId?: string;
    env?: string;
    userBranch?: string;
    tier?: string;
  },
): Promise<TestNapiCoreRuntime> {
  const { NapiDb } = await loadNapiModule();
  const appId = opts?.appId ?? "test-app";
  const env = opts?.env ?? "test";
  const userBranch = opts?.userBranch ?? "main";
  const runtime = new CoreRuntime(
    {
      openMemory: (schemaBytes, configBytes) =>
        NapiDb.openMemory(schemaBytes, configBytes) as never,
    },
    schema,
    deterministicBytes(`${appId}:${env}:${userBranch}:node`),
    deterministicBytes(`${appId}:${env}:${userBranch}:author`),
    1,
    true,
  );
  void opts?.tier;

  registerRuntimeCleanup(runtime);

  return runtime;
}

export async function createPersistentNapiCoreRuntime(
  schema: WasmSchema,
  dataPath: string,
  opts?: {
    appId?: string;
    env?: string;
    userBranch?: string;
    tier?: string;
  },
): Promise<TestNapiCoreRuntime> {
  const { NapiDb } = await loadNapiModule();
  const appId = opts?.appId ?? "test-app";
  const env = opts?.env ?? "test";
  const userBranch = opts?.userBranch ?? "main";
  const runtime = new CoreRuntime(
    {
      openMemory: (schemaBytes, configBytes) =>
        NapiDb.openMemory(schemaBytes, configBytes) as never,
      openPersistent: (path, schemaBytes, configBytes) =>
        NapiDb.openPersistent(path, schemaBytes, configBytes) as never,
    },
    schema,
    deterministicBytes(`${appId}:${env}:${userBranch}:node`),
    deterministicBytes(`${appId}:${env}:${userBranch}:author`),
    1,
    false,
    { persistentPath: dataPath },
  );
  void opts?.tier;

  registerRuntimeCleanup(runtime);

  return runtime;
}

function deterministicBytes(seed: string): Uint8Array {
  let hash = 0x811c9dc5;
  const bytes = new Uint8Array(16);
  const view = new DataView(bytes.buffer);
  for (let round = 0; round < 4; round += 1) {
    for (let i = 0; i < seed.length; i += 1) {
      hash ^= seed.charCodeAt(i) + round;
      hash = Math.imul(hash, 0x01000193);
    }
    view.setUint32(round * 4, hash >>> 0, true);
  }
  return bytes;
}
