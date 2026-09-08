/** Keep browser asset literals visible to bundlers, outside native imports. */
export function browserRuntimeModuleUrl(): string {
  return import.meta.url;
}
export function bundledBrowserWorkerUrl(): string {
  return new URL("../worker/jazz-broker-worker.js", import.meta.url).href;
}
