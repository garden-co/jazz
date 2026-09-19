import { createRequire } from "node:module";
import { onTestFinished } from "vitest";
import {
  mergePermissionsIntoWasmSchema,
  type CompiledPermissionsMap,
} from "../../schema-permissions.js";
import type { WasmSchema } from "../../drivers/types.js";
import type { Runtime } from "../client.js";
import { NativeRuntimeAdapter } from "../native-runtime/native-runtime-adapter.js";
import { testAuthorBytes } from "./account-fixtures.js";

type NapiModule = typeof import("jazz-napi");
export type TestNapiNativeRuntimeAdapter = Runtime & { close?: () => void };

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

type NativeRuntimeArgs = ConstructorParameters<typeof NativeRuntimeAdapter>;

/** Construct the native representation inside the fixture boundary. */
export function createNapiRuntimeFixture(
  Runtime: NativeRuntimeArgs[0],
  schema: WasmSchema,
  permissions: CompiledPermissionsMap,
  node: Uint8Array,
  author: Uint8Array,
  sourceId: number,
  historyComplete: boolean,
  options?: NativeRuntimeArgs[6],
): NativeRuntimeAdapter {
  return new NativeRuntimeAdapter(
    Runtime,
    mergePermissionsIntoWasmSchema(schema, permissions),
    node,
    author,
    sourceId,
    historyComplete,
    options,
  );
}

export async function createNapiNativeRuntimeAdapter(
  schema: WasmSchema,
  permissions: CompiledPermissionsMap,
  opts?: {
    appId?: string;
    env?: string;
    peerId?: string;
  },
): Promise<TestNapiNativeRuntimeAdapter> {
  const { NapiDb } = await loadNapiModule();
  const appId = opts?.appId ?? "test-app";
  const env = opts?.env ?? "test";
  const peerId = opts?.peerId ?? "default";
  const runtime = createNapiRuntimeFixture(
    {
      openMemory: (schemaBytes, configBytes) =>
        NapiDb.openMemory(schemaBytes, configBytes) as never,
    },
    schema,
    permissions,
    deterministicBytes(`${appId}:${env}:${peerId}:node`),
    testAuthorBytes(`${appId}:${env}:${peerId}:author`),
    1,
    true,
  );
  registerRuntimeCleanup(runtime);

  return runtime;
}

export async function createPersistentNapiNativeRuntimeAdapter(
  schema: WasmSchema,
  permissions: CompiledPermissionsMap,
  dataPath: string,
  opts?: {
    appId?: string;
    env?: string;
    peerId?: string;
  },
): Promise<TestNapiNativeRuntimeAdapter> {
  const { NapiDb } = await loadNapiModule();
  const appId = opts?.appId ?? "test-app";
  const env = opts?.env ?? "test";
  const peerId = opts?.peerId ?? "default";
  const runtime = createNapiRuntimeFixture(
    {
      openMemory: (schemaBytes, configBytes) =>
        NapiDb.openMemory(schemaBytes, configBytes) as never,
      openPersistent: (path, schemaBytes, configBytes) =>
        NapiDb.openPersistent(path, schemaBytes, configBytes) as never,
    },
    schema,
    permissions,
    deterministicBytes(`${appId}:${env}:${peerId}:node`),
    testAuthorBytes(`${appId}:${env}:${peerId}:author`),
    1,
    false,
    { persistentPath: dataPath },
  );
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
