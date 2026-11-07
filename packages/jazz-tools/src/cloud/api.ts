// Client for the Jazz Cloud dashboard's `/api/v1` (the CLI API).

import { canRefresh, getAccessToken, type CloudSessionOptions } from "./session.js";

export class CloudApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "CloudApiError";
  }
}

export interface CloudApi {
  request<T>(method: string, path: string, body?: unknown): Promise<T>;
}

export function createCloudApi(options: CloudSessionOptions): CloudApi {
  const fetchImpl = options.fetch ?? fetch;

  async function send(method: string, path: string, body: unknown, forceRefresh: boolean) {
    const token = await getAccessToken({ ...options, forceRefresh });
    return fetchImpl(`${options.cloudUrl}/api/v1${path}`, {
      method,
      headers: {
        accept: "application/json",
        authorization: `Bearer ${token}`,
        ...(body === undefined ? {} : { "content-type": "application/json" }),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  }

  return {
    async request<T>(method: string, path: string, body?: unknown): Promise<T> {
      let response = await send(method, path, body, false);
      // The stored token may have been revoked or its clock skewed: refresh once.
      if (response.status === 401 && canRefresh(options.env)) {
        response = await send(method, path, body, true);
      }
      const payload = (await response.json().catch(() => null)) as
        | (Record<string, unknown> & { error?: unknown; message?: unknown })
        | null;
      if (!response.ok) {
        const code = typeof payload?.error === "string" ? payload.error : "http_error";
        const message =
          typeof payload?.message === "string"
            ? payload.message
            : `Jazz Cloud request failed with status ${response.status}.`;
        throw new CloudApiError(response.status, code, message);
      }
      return payload as T;
    },
  };
}
