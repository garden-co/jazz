import type { RuntimeSourcesConfig } from "./context.js";

function isHttpUrl(moduleUrl: string): boolean {
  const protocol = new URL(moduleUrl).protocol;
  return protocol === "http:" || protocol === "https:";
}

// Turbopack/webpack spawn workers from blob:http(s): URLs; they're opaque bases
// for `new URL(path, href)`, so we treat them as bundled contexts on par with
// http(s): pages and let the bundler-resolved wasm URL win.
function isBundledPageContext(locationHref: string): boolean {
  const protocol = new URL(locationHref).protocol;
  return protocol === "http:" || protocol === "https:" || protocol === "blob:";
}

function resolveBrowserAssetBase(locationHref: string): string {
  return new URL("/", locationHref).href;
}

export function resolveConfiguredUrl(url: string, locationHref: string | undefined): string {
  // If `url` is already absolute, ignore the base. Workers under some bundlers
  // (Turbopack) expose a non-URL `self.location.href`, and `new URL(absolute,
  // badBase)` still throws because the base is validated.
  try {
    return new URL(url).href;
  } catch {
    // url is relative — fall through.
  }
  if (locationHref) {
    try {
      return new URL(url, locationHref).href;
    } catch {
      // base unparseable — fall through.
    }
  }
  return url;
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

function resolveDerivedWasmUrl(
  runtimeModuleUrl: string,
  locationHref: string | undefined,
  allowHttpPageFallback: boolean,
): string | null {
  if (
    !locationHref ||
    isHttpUrl(runtimeModuleUrl) ||
    (!allowHttpPageFallback && isBundledPageContext(locationHref))
  ) {
    return null;
  }

  return new URL("jazz_wasm_bg.wasm", resolveBrowserAssetBase(locationHref)).href;
}

export function resolveRuntimeConfigSyncInitInput(
  runtime?: RuntimeSourcesConfig,
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
  runtime?: RuntimeSourcesConfig,
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

  // In any web-hosted context (HTTP/HTTPS page), we are inside a bundled app.
  // Bundlers (Vite, Turbopack, webpack) transform the `new URL('*.wasm', import.meta.url)`
  // pattern in jazz_wasm.js and bake in the correct asset URL at build time.
  // Returning null lets wasm-bindgen use that bundler-resolved URL.
  // We only fall through to compute a root-relative URL when the page is served
  // from a non-HTTP origin (e.g. file://) — a static HTML page with the WASM
  // copied to the same directory.
  return resolveDerivedWasmUrl(runtimeModuleUrl, locationHref, false);
}
