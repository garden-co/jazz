/**
 * Dedicated Worker entry point for Jazz.
 *
 * Loads the WASM module, hands off to `runAsWorker` in Rust. After handoff
 * the worker no longer drives messages from TS — Rust owns `self.onmessage`.
 */

import {
  readWorkerRuntimeWasmUrl,
  resolveRuntimeConfigSyncInitInput,
  resolveRuntimeConfigWasmUrl,
} from "../runtime/runtime-config.js";
import type { RuntimeSourcesConfig } from "../runtime/context.js";
import { installWasmTelemetry } from "../runtime/sync-telemetry.js";

const DEFAULT_WASM_LOG_LEVEL = "warn";

declare const self: {
  postMessage(msg: unknown, transfer?: Transferable[]): void;
  onmessage: ((event: MessageEvent) => void) | null;
  close(): void;
  location?: { origin?: string; href?: string };
};

interface ShimInitMessage {
  type: "init";
  runtimeSources?: RuntimeSourcesConfig;
  fallbackWasmUrl?: string;
  logLevel?: string;
  telemetryCollectorUrl?: string;
  appId?: string;
}

type VitestBrowserRunner = {
  wrapDynamicImport<T>(loader: () => Promise<T>): Promise<T>;
};

function ensureVitestWorkerImportShim(): void {
  const globalRef = globalThis as typeof globalThis & {
    __vitest_browser_runner__?: VitestBrowserRunner;
  };
  if (globalRef.__vitest_browser_runner__) return;
  globalRef.__vitest_browser_runner__ = {
    wrapDynamicImport<T>(loader: () => Promise<T>): Promise<T> {
      return loader();
    },
  };
}

ensureVitestWorkerImportShim();

let initMessage: ShimInitMessage | null = null;
const pendingMessages: unknown[] = [];
let wasmInitialized = false;
let disposeWasmTelemetry: (() => void) | null = null;

function post(msg: unknown): void {
  self.postMessage(msg);
}

function resolveAbsoluteWasmUrlFromInitError(error: unknown): string | null {
  const origin = self.location?.origin;
  if (!origin) return null;
  const message = error instanceof Error ? error.message : String(error ?? "");
  const match = message.match(/(\/[^"'\s]+\.wasm)/);
  const wasmPath = match?.[1];
  if (!wasmPath) return null;
  return new URL(wasmPath, origin).href;
}

async function runWithRootRelativeFetchSupport<T>(operation: () => Promise<T>): Promise<T> {
  const globalRef = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const originalFetch = globalRef.fetch;
  const origin = self.location?.origin;
  if (typeof originalFetch !== "function" || !origin) return operation();
  const patchedFetch: typeof fetch = (input, init) =>
    originalFetch(
      typeof input === "string" && input.startsWith("/")
        ? new URL(input, origin).toString()
        : input,
      init,
    );
  globalRef.fetch = patchedFetch;
  try {
    return await operation();
  } finally {
    globalRef.fetch = originalFetch;
  }
}

async function ensureWasmInitialized(
  wasmModule: any,
  msg: Pick<ShimInitMessage, "runtimeSources" | "fallbackWasmUrl"> | undefined,
): Promise<void> {
  if (wasmInitialized) return;

  const syncInitInput = resolveRuntimeConfigSyncInitInput(msg?.runtimeSources);
  if (syncInitInput) {
    wasmModule.initSync(syncInitInput);
    wasmInitialized = true;
    return;
  }

  if (typeof wasmModule.default !== "function") {
    wasmInitialized = true;
    return;
  }

  const locationHref = self.location?.href;
  const wasmUrl =
    resolveRuntimeConfigWasmUrl(import.meta.url, locationHref, msg?.runtimeSources) ??
    readWorkerRuntimeWasmUrl(locationHref);

  if (wasmUrl) {
    await wasmModule.default({ module_or_path: wasmUrl });
    wasmInitialized = true;
    return;
  }

  try {
    await runWithRootRelativeFetchSupport(() => wasmModule.default());
    wasmInitialized = true;
  } catch (error) {
    const absoluteWasmUrl =
      resolveAbsoluteWasmUrlFromInitError(error) ?? msg?.fallbackWasmUrl ?? null;
    if (!absoluteWasmUrl) throw error;
    await wasmModule.default({ module_or_path: absoluteWasmUrl });
    wasmInitialized = true;
  }
}

async function startup(): Promise<void> {
  try {
    const wasmModule: any = await import("jazz-wasm");
    if (readWorkerRuntimeWasmUrl(self.location?.href)) {
      await ensureWasmInitialized(wasmModule, undefined);
    }
    post({ type: "ready" });
  } catch (error) {
    post({
      type: "error",
      message: `WASM load failed: ${error instanceof Error ? error.message : String(error)}`,
    });
  }
}

async function bootstrapAndHandoff(init: ShimInitMessage): Promise<void> {
  try {
    const wasmModule: any = await import("jazz-wasm");
    (globalThis as any).__JAZZ_WASM_LOG_LEVEL = init.logLevel ?? DEFAULT_WASM_LOG_LEVEL;
    await ensureWasmInitialized(wasmModule, init);

    disposeWasmTelemetry = installWasmTelemetry({
      wasmModule,
      collectorUrl: init.telemetryCollectorUrl,
      appId: init.appId ?? "",
      runtimeThread: "worker",
    });

    const buffered = pendingMessages.slice();
    pendingMessages.length = 0;
    wasmModule.runAsWorker(init, buffered);
  } catch (error) {
    post({
      type: "error",
      message: `Init failed: ${error instanceof Error ? error.message : String(error)}`,
    });
  }
}

self.onmessage = (event: MessageEvent) => {
  const data = event.data;
  if (
    initMessage === null &&
    data !== null &&
    typeof data === "object" &&
    !(data instanceof Uint8Array) &&
    (data as { type?: string }).type === "init"
  ) {
    initMessage = data as ShimInitMessage;
    void bootstrapAndHandoff(initMessage);
    return;
  }
  pendingMessages.push(data);
};

void startup();

// Keep a reference to dispose telemetry on close. Rust owns the lifecycle, but
// some bundlers wrap workers in a manager that calls close(); make sure the
// dispose runs before the global goes away.
const originalClose = self.close.bind(self);
self.close = () => {
  try {
    disposeWasmTelemetry?.();
  } catch {
    // ignore telemetry teardown errors during close
  }
  originalClose();
};
