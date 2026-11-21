// Fetch helpers for the app server's REST reads and RPC writes.
export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
    /** The server's code, e.g. "permission_denied" or "catalog_number_taken". */
    public code?: string,
  ) {
    super(message);
  }
}

async function handle<T>(response: Response): Promise<T> {
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new ApiError(response.status, body.error ?? response.statusText, body.code);
  return body as T;
}

export const get = <T>(path: string, signal?: AbortSignal) =>
  fetch(path, { credentials: "same-origin", signal }).then((r) => handle<T>(r));

export async function post<T>(path: string, body: object, signal?: AbortSignal): Promise<T> {
  const response = await fetch(path, {
    method: "POST",
    credentials: "same-origin",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
    signal,
  });
  return handle<T>(response);
}

export async function call<T = unknown>(name: string, args: object): Promise<T> {
  const response = await fetch(`/api/rpc/${name}`, {
    method: "POST",
    credentials: "same-origin",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(args),
  });
  return (await handle<{ result: T }>(response)).result;
}
