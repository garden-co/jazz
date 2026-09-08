function unsupported(): never {
  throw new Error("Browser worker assets are unavailable on React Native");
}
export const browserRuntimeModuleUrl = unsupported;
export const bundledBrowserWorkerUrl = unsupported;
