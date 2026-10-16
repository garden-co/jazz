// Jazz Cloud login: WorkOS CLI Auth (OAuth 2.0 device authorization, RFC 8628)
// against the dashboard's own AuthKit client, plus refresh-token rotation.
//
// The resulting access token is a short-lived WorkOS JWT for the dashboard
// user. The dashboard's `/api/v1` verifies it and applies the same team/app
// permissions as its UI, so no per-app admin secret is involved.

import {
  deleteProfile,
  readProfile,
  saveProfile,
  type CloudProfile,
  type WorkosClientConfig,
} from "./credentials.js";

export const DEFAULT_CLOUD_URL = "https://v2.dashboard.jazz.tools";
const DEVICE_CODE_GRANT = "urn:ietf:params:oauth:grant-type:device_code";
/** Refresh this long before expiry so a request never starts with a dying token. */
const REFRESH_MARGIN_SECONDS = 30;

export class CloudAuthError extends Error {
  constructor(
    readonly code:
      | "not_logged_in"
      | "login_denied"
      | "login_expired"
      | "session_expired"
      | "login_unavailable",
    message: string,
  ) {
    super(message);
    this.name = "CloudAuthError";
  }
}

export interface DeviceAuthorization {
  deviceCode: string;
  userCode: string;
  verificationUri: string;
  verificationUriComplete: string;
  expiresIn: number;
  interval: number;
}

export interface CloudSessionOptions {
  cloudUrl: string;
  env?: NodeJS.ProcessEnv;
  fetch?: typeof fetch;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
}

interface TokenResponse {
  access_token: string;
  refresh_token: string;
  user: { id: string; email: string };
}

export function resolveCloudUrl(flag: string | undefined, env: NodeJS.ProcessEnv): string {
  const raw = flag ?? env.JAZZ_CLOUD_URL ?? DEFAULT_CLOUD_URL;
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new Error(`Invalid Jazz Cloud URL: ${raw}`);
  }
  const loopback = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
  if (url.protocol !== "https:" && !(url.protocol === "http:" && loopback)) {
    throw new Error(
      "The Jazz Cloud URL must use HTTPS (plain HTTP is allowed only for localhost).",
    );
  }
  return url.origin;
}

/** Reads `exp` without verifying: only used to schedule refreshes. */
export function accessTokenExpiry(token: string): number {
  const payload = token.split(".")[1];
  if (!payload) throw new Error("Malformed access token.");
  const claims = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")) as {
    exp?: unknown;
  };
  if (typeof claims.exp !== "number") throw new Error("Access token has no expiry.");
  return claims.exp;
}

async function fetchJson(
  fetchImpl: typeof fetch,
  url: string,
  init: RequestInit,
): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await fetchImpl(url, init);
  const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;
  return { status: response.status, body };
}

function form(values: Record<string, string>): RequestInit {
  return {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded", accept: "application/json" },
    body: new URLSearchParams(values).toString(),
  };
}

export async function fetchWorkosClientConfig(
  cloudUrl: string,
  fetchImpl: typeof fetch = fetch,
): Promise<WorkosClientConfig> {
  const { status, body } = await fetchJson(fetchImpl, `${cloudUrl}/api/v1/cli/config`, {
    headers: { accept: "application/json" },
  });
  const workos = body.workos as Partial<WorkosClientConfig> | undefined;
  if (
    status !== 200 ||
    typeof workos?.clientId !== "string" ||
    typeof workos.apiBaseUrl !== "string"
  ) {
    throw new CloudAuthError(
      "login_unavailable",
      `${cloudUrl} does not support CLI login yet (GET /api/v1/cli/config returned ${status}).`,
    );
  }
  return { clientId: workos.clientId, apiBaseUrl: workos.apiBaseUrl.replace(/\/+$/, "") };
}

export async function startDeviceAuthorization(
  workos: WorkosClientConfig,
  fetchImpl: typeof fetch = fetch,
): Promise<DeviceAuthorization> {
  const { status, body } = await fetchJson(
    fetchImpl,
    `${workos.apiBaseUrl}/user_management/authorize/device`,
    form({ client_id: workos.clientId }),
  );
  if (
    status !== 200 ||
    typeof body.device_code !== "string" ||
    typeof body.user_code !== "string"
  ) {
    throw new CloudAuthError(
      "login_unavailable",
      `Could not start login (status ${status}${typeof body.error === "string" ? `: ${body.error}` : ""}).`,
    );
  }
  return {
    deviceCode: body.device_code,
    userCode: body.user_code,
    verificationUri: String(body.verification_uri),
    verificationUriComplete: String(body.verification_uri_complete ?? body.verification_uri),
    expiresIn: Number(body.expires_in ?? 300),
    interval: Number(body.interval ?? 5),
  };
}

function toTokenResponse(body: Record<string, unknown>): TokenResponse {
  const user = body.user as { id?: unknown; email?: unknown } | undefined;
  if (
    typeof body.access_token !== "string" ||
    typeof body.refresh_token !== "string" ||
    typeof user?.id !== "string"
  ) {
    throw new CloudAuthError("login_unavailable", "Login returned an unexpected response.");
  }
  return {
    access_token: body.access_token,
    refresh_token: body.refresh_token,
    user: { id: user.id, email: typeof user.email === "string" ? user.email : "" },
  };
}

export async function pollDeviceAuthorization(
  workos: WorkosClientConfig,
  authorization: DeviceAuthorization,
  options: Pick<CloudSessionOptions, "fetch" | "now" | "sleep"> = {},
): Promise<TokenResponse> {
  const fetchImpl = options.fetch ?? fetch;
  const now = options.now ?? Date.now;
  const sleep =
    options.sleep ?? ((ms: number) => new Promise((resolve) => setTimeout(resolve, ms)));
  const deadline = now() + authorization.expiresIn * 1000;
  let interval = Math.max(1, authorization.interval);

  while (now() < deadline) {
    await sleep(interval * 1000);
    const { status, body } = await fetchJson(
      fetchImpl,
      `${workos.apiBaseUrl}/user_management/authenticate`,
      form({
        grant_type: DEVICE_CODE_GRANT,
        device_code: authorization.deviceCode,
        client_id: workos.clientId,
      }),
    );
    if (status === 200) return toTokenResponse(body);
    switch (body.error) {
      case "authorization_pending":
        continue;
      case "slow_down":
        interval += 5;
        continue;
      case "access_denied":
        throw new CloudAuthError("login_denied", "Login was denied in the browser.");
      case "expired_token":
        throw new CloudAuthError(
          "login_expired",
          "The login code expired. Run `jazz-tools login` again.",
        );
      default:
        throw new CloudAuthError(
          "login_unavailable",
          `Login failed (status ${status}${typeof body.error === "string" ? `: ${body.error}` : ""}).`,
        );
    }
  }
  throw new CloudAuthError(
    "login_expired",
    "The login code expired. Run `jazz-tools login` again.",
  );
}

export function profileFromTokens(
  cloudUrl: string,
  workos: WorkosClientConfig,
  tokens: TokenResponse,
  createdAt = new Date().toISOString(),
): CloudProfile {
  return {
    cloudUrl,
    workos,
    accessToken: tokens.access_token,
    accessTokenExpiresAt: accessTokenExpiry(tokens.access_token),
    refreshToken: tokens.refresh_token,
    user: tokens.user,
    createdAt,
  };
}

async function refreshProfile(
  profile: CloudProfile,
  options: CloudSessionOptions,
): Promise<CloudProfile> {
  const fetchImpl = options.fetch ?? fetch;
  const { status, body } = await fetchJson(
    fetchImpl,
    `${profile.workos.apiBaseUrl}/user_management/authenticate`,
    form({
      grant_type: "refresh_token",
      refresh_token: profile.refreshToken,
      client_id: profile.workos.clientId,
    }),
  );
  if (status !== 200) {
    // Refresh tokens rotate. If a concurrent jazz-tools process refreshed
    // first, our token was just consumed: pick up the one it stored.
    const current = await readProfile(profile.cloudUrl, options.env);
    if (current && current.refreshToken !== profile.refreshToken) {
      return current;
    }
    // A rejected refresh token is dead for good: forget it so the next command
    // gives a clear "log in" message instead of retrying a revoked session.
    if (status === 400 || status === 401) {
      await deleteProfile(profile.cloudUrl, options.env);
    }
    throw new CloudAuthError(
      "session_expired",
      "Your Jazz Cloud session has expired. Run `jazz-tools login` again.",
    );
  }
  const next = profileFromTokens(
    profile.cloudUrl,
    profile.workos,
    toTokenResponse(body),
    profile.createdAt,
  );
  await saveProfile(next, options.env);
  return next;
}

/**
 * A bearer for `/api/v1`. `JAZZ_CLOUD_TOKEN` wins (for callers that already
 * hold a short-lived token); otherwise the stored login, refreshed if needed.
 */
export async function getAccessToken(
  options: CloudSessionOptions & { forceRefresh?: boolean },
): Promise<string> {
  const env = options.env ?? process.env;
  if (env.JAZZ_CLOUD_TOKEN?.trim()) return env.JAZZ_CLOUD_TOKEN.trim();

  const profile = await readProfile(options.cloudUrl, env);
  if (!profile) {
    throw new CloudAuthError(
      "not_logged_in",
      `Not logged in to ${options.cloudUrl}. Run \`jazz-tools login\`.`,
    );
  }
  const nowSeconds = Math.floor((options.now ?? Date.now)() / 1000);
  if (!options.forceRefresh && profile.accessTokenExpiresAt - REFRESH_MARGIN_SECONDS > nowSeconds) {
    return profile.accessToken;
  }
  return (await refreshProfile(profile, options)).accessToken;
}

export function canRefresh(env: NodeJS.ProcessEnv = process.env): boolean {
  return !env.JAZZ_CLOUD_TOKEN?.trim();
}
