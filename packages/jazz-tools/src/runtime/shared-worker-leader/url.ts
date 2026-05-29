// packages/jazz-tools/src/runtime/shared-worker-leader/url.ts
import type { RuntimeSourcesConfig } from "../context.js";

function isHttpUrl(url: string): boolean {
  try {
    const protocol = new URL(url).protocol;
    return protocol === "http:" || protocol === "https:";
  } catch {
    return false;
  }
}

function resolveAbsoluteOrRelative(url: string, locationHref: string | undefined): string {
  try {
    return new URL(url).href;
  } catch {
    // not absolute — fall through
  }
  if (locationHref) {
    try {
      return new URL(url, locationHref).href;
    } catch {
      // unparseable base — fall through
    }
  }
  return url;
}

export function resolveSharedWorkerLeaderUrl(
  runtimeModuleUrl: string,
  locationHref: string | undefined,
  runtime?: RuntimeSourcesConfig,
): string {
  if (runtime?.sharedWorkerLeaderUrl) {
    return resolveAbsoluteOrRelative(runtime.sharedWorkerLeaderUrl, locationHref);
  }
  if (runtime?.baseUrl && locationHref) {
    const baseUrl = new URL(runtime.baseUrl, locationHref).href;
    return new URL("shared-worker-leader/shared-worker-leader.js", baseUrl).href;
  }
  if (!locationHref || isHttpUrl(runtimeModuleUrl)) {
    return new URL("../shared-worker-leader/shared-worker-leader.js", runtimeModuleUrl).href;
  }
  return new URL("shared-worker-leader/shared-worker-leader.js", new URL("/", locationHref).href)
    .href;
}
