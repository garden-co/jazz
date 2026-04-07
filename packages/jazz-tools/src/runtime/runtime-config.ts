import type { RuntimeConfig } from "./context.js";

function isHttpModuleUrl(moduleUrl: string): boolean {
  const protocol = new URL(moduleUrl).protocol;
  return protocol === "http:" || protocol === "https:";
}

function resolveBrowserAssetBase(locationHref: string): string {
  return new URL("/", locationHref).href;
}

function resolveConfiguredUrl(url: string, locationHref: string | undefined): string {
  if (locationHref) {
    return new URL(url, locationHref).href;
  }

  return new URL(url).href;
}

function resolveConfiguredBaseUrl(
  baseUrl: string,
  locationHref: string | undefined,
): string | null {
  if (!locationHref) {
    return null;
  }

  return new URL(baseUrl, locationHref).href;
}

export function resolveRuntimeConfigSyncInitInput(
  runtime?: RuntimeConfig,
): { module: BufferSource | WebAssembly.Module } | null {
  if (runtime?.wasmModule) {
    return { module: runtime.wasmModule };
  }

  if (runtime?.wasmSource) {
    return { module: runtime.wasmSource };
  }

  return null;
}

export function resolveRuntimeConfigWasmUrl(
  runtimeModuleUrl: string,
  locationHref: string | undefined,
  runtime?: RuntimeConfig,
): string | null {
  if (runtime?.wasmUrl) {
    return resolveConfiguredUrl(runtime.wasmUrl, locationHref);
  }

  if (runtime?.baseUrl) {
    const baseUrl = resolveConfiguredBaseUrl(runtime.baseUrl, locationHref);
    if (baseUrl) {
      return new URL("jazz_wasm_bg.wasm", baseUrl).href;
    }
  }

  if (!locationHref || isHttpModuleUrl(runtimeModuleUrl)) {
    return null;
  }

  return new URL("jazz_wasm_bg.wasm", resolveBrowserAssetBase(locationHref)).href;
}

export function resolveRuntimeConfigWorkerUrl(
  runtimeModuleUrl: string,
  locationHref: string | undefined,
  runtime?: RuntimeConfig,
): string {
  if (runtime?.workerUrl) {
    return resolveConfiguredUrl(runtime.workerUrl, locationHref);
  }

  if (runtime?.baseUrl) {
    const baseUrl = resolveConfiguredBaseUrl(runtime.baseUrl, locationHref);
    if (baseUrl) {
      return new URL("worker/jazz-worker.js", baseUrl).href;
    }
  }

  if (!locationHref || isHttpModuleUrl(runtimeModuleUrl)) {
    return new URL("../worker/jazz-worker.js", runtimeModuleUrl).href;
  }

  return new URL("worker/jazz-worker.js", resolveBrowserAssetBase(locationHref)).href;
}

export function appendWorkerRuntimeWasmUrl(workerUrl: string, wasmUrl: string | null): string {
  if (!wasmUrl) {
    return workerUrl;
  }

  const url = new URL(workerUrl);
  url.searchParams.set("jazz-wasm-url", wasmUrl);
  return url.href;
}

export function readWorkerRuntimeWasmUrl(locationHref: string | undefined): string | null {
  if (!locationHref) {
    return null;
  }

  return new URL(locationHref).searchParams.get("jazz-wasm-url");
}
