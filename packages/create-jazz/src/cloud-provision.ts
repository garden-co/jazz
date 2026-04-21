const DEFAULT_API_URL = "https://v2.dashboard.jazz.tools/api/apps/generate";

export class ProvisionNetworkError extends Error {
  constructor(apiUrl: string, cause: unknown) {
    const causeMessage = cause instanceof Error ? cause.message : String(cause);
    super(`Network error provisioning app at ${apiUrl}: ${causeMessage}`);
    this.name = "ProvisionNetworkError";
    this.cause = cause;
  }
}

export class ProvisionHttpError extends Error {
  readonly status: number;

  constructor(apiUrl: string, status: number) {
    super(`HTTP ${status} error provisioning app at ${apiUrl}`);
    this.name = "ProvisionHttpError";
    this.status = status;
  }
}

export class ProvisionParseError extends Error {
  constructor(message: string, cause?: unknown) {
    super(message);
    this.name = "ProvisionParseError";
    if (cause !== undefined) this.cause = cause;
  }
}

export async function provisionHostedApp({
  apiUrl = DEFAULT_API_URL,
  fetch: fetchFn = globalThis.fetch,
}: {
  apiUrl?: string;
  fetch?: typeof globalThis.fetch;
} = {}): Promise<{ appId: string; adminSecret: string; backendSecret: string }> {
  let response: Response;

  try {
    response = await fetchFn(apiUrl, { method: "POST" });
  } catch (cause) {
    throw new ProvisionNetworkError(apiUrl, cause);
  }

  if (!response.ok) {
    throw new ProvisionHttpError(apiUrl, response.status);
  }

  let body: unknown;
  try {
    body = await response.json();
  } catch (cause) {
    throw new ProvisionParseError(`Invalid JSON in response from ${apiUrl}`, cause);
  }

  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    throw new ProvisionParseError(`Response from ${apiUrl} is not an object`);
  }

  const record = body as Record<string, unknown>;
  const missing = (["appId", "adminSecret", "backendSecret"] as const).filter(
    (key) => typeof record[key] !== "string" || (record[key] as string).length === 0,
  );

  if (missing.length > 0) {
    throw new ProvisionParseError(
      `Response from ${apiUrl} is missing required fields: ${missing.join(", ")}`,
    );
  }

  return {
    appId: record["appId"] as string,
    adminSecret: record["adminSecret"] as string,
    backendSecret: record["backendSecret"] as string,
  };
}
