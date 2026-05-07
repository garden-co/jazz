/**
 * Dedicated Worker entry point for Jazz — thin WASM-bootstrap shim.
 *
 * Per `specs/todo/projects/rust-owned-worker-bridge/spec.md`, the worker-side
 * runtime host now lives entirely in Rust (`crates/jazz-wasm/src/worker_host.rs`).
 * This file's only responsibility is the bootstrap-handoff dance:
 *
 *  1. Post `{type:"ready"}` so the main thread knows it can send the init.
 *  2. Buffer the first `init` message and any subsequent messages while WASM
 *     is initialising — Rust takes over `self.onmessage` synchronously inside
 *     `runAsWorker`, so messages arriving *during* the bootstrap call still
 *     hit our handler and land in `pendingMessages`.
 *  3. Resolve `runtimeSources` (bundler-specific JS modules / wasm URLs) and
 *     initialise the WASM module.
 *  4. Install JS-side WASM tracing telemetry (it imports `subscribeTraceEntries`
 *     and lives in JS by design).
 *  5. Hand the buffered init + pending messages to `wasmModule.runAsWorker`.
 *     After that call, Rust owns `self.onmessage` / `self.postMessage`.
 */

import type { RuntimeSourcesConfig } from "../runtime/context.js";
import {
  readWorkerRuntimeWasmUrl,
  resolveRuntimeConfigSyncInitInput,
  resolveRuntimeConfigWasmUrl,
} from "../runtime/runtime-config.js";
import { installWasmTelemetry } from "../runtime/sync-telemetry.js";

/**
 * Init message: the only worker-protocol envelope that stays a JS object
 * (everything else rides as binary postcard inside `MainToWorkerWire`).
 * Stays JS because `runtimeSources` carries bundler-resolved JS module/blob
 * refs that don't postcard-serialise, and the shim consumes them locally
 * before handing off to Rust.
 */
interface InitMessage {
  type: "init";
  schemaJson: string;
  appId: string;
  env: string;
  userBranch: string;
  dbName: string;
  clientId: string;
  serverUrl?: string;
  jwtToken?: string;
  adminSecret?: string;
  runtimeSources?: RuntimeSourcesConfig;
  fallbackWasmUrl?: string;
  logLevel?: "error" | "warn" | "info" | "debug" | "trace";
  telemetryCollectorUrl?: string;
}

declare const self: {
  postMessage(msg: unknown, transfer?: Transferable[]): void;
  onmessage: ((event: MessageEvent) => void) | null;
  close(): void;
  location?: { origin?: string; href?: string };
};

type VitestBrowserRunner = {
  wrapDynamicImport<T>(loader: () => Promise<T>): Promise<T>;
};

function ensureVitestWorkerImportShim(): void {
  const globalRef = globalThis as typeof globalThis & {
    __vitest_browser_runner__?: VitestBrowserRunner;
  };
  if (globalRef.__vitest_browser_runner__) return;
  // Vitest browser mode installs this on the page global, but dedicated workers
  // can miss that setup. Provide the same no-op wrapper so transformed worker
  // imports still resolve through the bundler.
  globalRef.__vitest_browser_runner__ = {
    wrapDynamicImport<T>(loader: () => Promise<T>): Promise<T> {
      return loader();
    },
  };
}

ensureVitestWorkerImportShim();

const DEFAULT_WASM_LOG_LEVEL = "warn";
let initMessage: InitMessage | null = null;
// Pre-handoff buffer. Init arrives as a JS object; everything else now arrives
// as Uint8Array (postcard-encoded `MainToWorkerWire`). Rust parses each entry
// post-handoff inside `runAsWorker`.
const pendingMessages: unknown[] = [];
let wasmInitialized = false;

self.onmessage = (event: MessageEvent) => {
  const data = event.data;
  if (
    !initMessage &&
    typeof data === "object" &&
    data !== null &&
    !(data instanceof Uint8Array) &&
    (data as { type?: unknown }).type === "init"
  ) {
    initMessage = data as InitMessage;
    void bootstrapAndHandoff(initMessage);
    return;
  }
  pendingMessages.push(data);
};

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
  msg: Pick<InitMessage, "runtimeSources" | "fallbackWasmUrl"> | undefined,
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
  } catch (error) {
    const absoluteWasmUrl =
      resolveAbsoluteWasmUrlFromInitError(error) ?? msg?.fallbackWasmUrl ?? null;
    if (!absoluteWasmUrl) throw error;
    await wasmModule.default({ module_or_path: absoluteWasmUrl });
  }

  wasmInitialized = true;
}

async function bootstrapAndHandoff(init: InitMessage): Promise<void> {
  try {
    const wasmModule: any = await import("jazz-wasm");
    (globalThis as any).__JAZZ_WASM_LOG_LEVEL = init.logLevel ?? DEFAULT_WASM_LOG_LEVEL;
    await ensureWasmInitialized(wasmModule, init);

    installWasmTelemetry({
      wasmModule,
      collectorUrl: init.telemetryCollectorUrl,
      appId: init.appId,
      runtimeThread: "worker",
    });

    // Hand control to Rust. `runAsWorker` synchronously installs its own
    // `self.onmessage` (replacing ours), then spawns an async task that
    // opens the runtime, drains the buffered messages, and posts `init-ok`.
    wasmModule.runAsWorker(init, pendingMessages.slice());
    pendingMessages.length = 0;
  } catch (e: any) {
    self.postMessage({ type: "error", message: `Init failed: ${e?.message ?? e}` });
  }
}

async function startup(): Promise<void> {
  try {
    const wasmModule: any = await import("jazz-wasm");
    if (readWorkerRuntimeWasmUrl(self.location?.href)) {
      await ensureWasmInitialized(wasmModule, undefined);
    }
    self.postMessage({ type: "ready" });
  } catch (e: any) {
    self.postMessage({ type: "error", message: `WASM load failed: ${e?.message ?? e}` });
  }
}

startup();
