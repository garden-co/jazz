// The Inspector API of the backend that served this page: one JSON endpoint
// at `<basePath>/inspect/api`, next to the page itself. The browser's own
// sign-in (cookies, or the infra gate in front of the app) rides along, and
// the app's `inspect` hook decides what it may do.

import type { InspectRequest } from "jazz-tools/inspect";

export class InspectorApiError extends Error {
  readonly code: string;
  readonly status: number;
  constructor(code: string, message: string, status: number) {
    super(message);
    this.name = "InspectorApiError";
    this.code = code;
    this.status = status;
  }
}

export type InspectorCall = <T = unknown>(request: InspectRequest) => Promise<T>;

/**
 * The API next to the page at `pageUrl`, which is `<basePath>/inspect/` (or a
 * path below it).
 */
export function apiUrlFor(pageUrl: string): string {
  const url = new URL(pageUrl);
  const at = url.pathname.lastIndexOf("/inspect");
  const base = at === -1 ? "" : url.pathname.slice(0, at);
  return `${url.origin}${base}/inspect/api`;
}

export function createInspectorCall(
  apiUrl: string,
  doFetch: typeof fetch = (...args) => fetch(...args),
): InspectorCall {
  return async <T>(request: InspectRequest): Promise<T> => {
    let response: Response;
    try {
      response = await doFetch(apiUrl, {
        method: "POST",
        credentials: "same-origin",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(request),
      });
    } catch (error) {
      throw new InspectorApiError(
        "failed",
        `Couldn't reach the app's backend: ${error instanceof Error ? error.message : String(error)}`,
        0,
      );
    }
    const text = await response.text();
    let body: unknown;
    try {
      body = JSON.parse(text);
    } catch {
      throw new InspectorApiError(
        "failed",
        `Unexpected answer (${response.status}) from ${apiUrl}`,
        response.status,
      );
    }
    if (!response.ok) {
      const error = body as { error?: string; message?: string };
      throw new InspectorApiError(
        error.error ?? "failed",
        error.message ?? `HTTP ${response.status}`,
        response.status,
      );
    }
    return body as T;
  };
}
