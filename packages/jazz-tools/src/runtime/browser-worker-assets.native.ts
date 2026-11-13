function unsupported(): never {
  throw new Error("Browser worker assets are unavailable on React Native");
}
export const browserRuntimeModuleUrl = unsupported;
export const bundledBrowserWorkerUrl = unsupported;
export const bundledWasmUrl = unsupported;
export const bundledBrowserWorkerType = unsupported;
