import { schema as s } from "jazz-tools";

export interface JazzAuthRedirectOptions {
  redirectTo?: string | null;
}

export interface JazzAuthClientOptions {
  apiBasePath?: string;
  baseURL: string;
  fetch?: typeof fetch;
  hostedBasePath?: string;
  location?: Pick<Location, "assign">;
}

export interface JazzAuthClient {
  getJwt(): Promise<string | null>;
  getSignInUrl(options?: JazzAuthRedirectOptions): string;
  getSignOutUrl(options?: JazzAuthRedirectOptions): string;
  getSignUpUrl(options?: JazzAuthRedirectOptions): string;
  login(options?: JazzAuthRedirectOptions): string;
  logout(options?: JazzAuthRedirectOptions): string;
  signIn(options?: JazzAuthRedirectOptions): string;
  signOut(options?: JazzAuthRedirectOptions): string;
  signUp(options?: JazzAuthRedirectOptions): string;
  signin(options?: JazzAuthRedirectOptions): string;
}

function normalizeBaseURL(baseURL: string): string {
  return baseURL.replace(/\/+$/, "");
}

function appendRedirectTo(url: URL, redirectTo?: string | null): string {
  if (redirectTo) {
    url.searchParams.set("redirectTo", redirectTo);
  }
  return url.toString();
}

function buildHostedUrl(
  baseURL: string,
  hostedBasePath: string,
  path: string,
  options?: JazzAuthRedirectOptions,
): string {
  return appendRedirectTo(
    new URL(`${hostedBasePath}${path}`, normalizeBaseURL(baseURL)),
    options?.redirectTo,
  );
}

export function createJazzAuthClient(options: JazzAuthClientOptions): JazzAuthClient {
  const apiBasePath = options.apiBasePath ?? "/api/auth";
  const hostedBasePath = options.hostedBasePath ?? "/auth";
  const fetchImpl = options.fetch ?? globalThis.fetch;
  const location = options.location ?? globalThis.location;

  function maybeRedirect(url: string): string {
    location?.assign?.(url);
    return url;
  }

  return {
    async getJwt(): Promise<string | null> {
      const response = await fetchImpl(`${normalizeBaseURL(options.baseURL)}${apiBasePath}/token`, {
        credentials: "include",
        headers: { accept: "application/json" },
        method: "GET",
      });

      if (!response.ok) {
        return null;
      }

      const payload = (await response.json()) as { token?: string | null };
      return payload.token ?? null;
    },
    getSignInUrl(redirectOptions?: JazzAuthRedirectOptions): string {
      return buildHostedUrl(options.baseURL, hostedBasePath, "/sign-in", redirectOptions);
    },
    getSignOutUrl(redirectOptions?: JazzAuthRedirectOptions): string {
      return buildHostedUrl(options.baseURL, hostedBasePath, "/sign-out", redirectOptions);
    },
    getSignUpUrl(redirectOptions?: JazzAuthRedirectOptions): string {
      return buildHostedUrl(options.baseURL, hostedBasePath, "/sign-up", redirectOptions);
    },
    login(redirectOptions?: JazzAuthRedirectOptions): string {
      return maybeRedirect(
        buildHostedUrl(options.baseURL, hostedBasePath, "/sign-in", redirectOptions),
      );
    },
    logout(redirectOptions?: JazzAuthRedirectOptions): string {
      return maybeRedirect(
        buildHostedUrl(options.baseURL, hostedBasePath, "/sign-out", redirectOptions),
      );
    },
    signIn(redirectOptions?: JazzAuthRedirectOptions): string {
      return maybeRedirect(
        buildHostedUrl(options.baseURL, hostedBasePath, "/sign-in", redirectOptions),
      );
    },
    signOut(redirectOptions?: JazzAuthRedirectOptions): string {
      return maybeRedirect(
        buildHostedUrl(options.baseURL, hostedBasePath, "/sign-out", redirectOptions),
      );
    },
    signUp(redirectOptions?: JazzAuthRedirectOptions): string {
      return maybeRedirect(
        buildHostedUrl(options.baseURL, hostedBasePath, "/sign-up", redirectOptions),
      );
    },
    signin(redirectOptions?: JazzAuthRedirectOptions): string {
      return maybeRedirect(
        buildHostedUrl(options.baseURL, hostedBasePath, "/sign-in", redirectOptions),
      );
    },
  };
}

export function jazzAuthTables() {
  return {
    authUsers: s
      .table({
        email: s.string(),
        emailVerified: s.boolean().default(false),
        image: s.string().optional(),
        name: s.string(),
        principalId: s.string(),
      })
      .index("auth_users_email_idx", ["email"])
      .index("auth_users_principal_id_idx", ["principalId"]),
    authAccounts: s
      .table({
        accessToken: s.string().optional(),
        accessTokenExpiresAt: s.timestamp().optional(),
        accountId: s.string(),
        idToken: s.string().optional(),
        password: s.string().optional(),
        providerId: s.string(),
        refreshToken: s.string().optional(),
        refreshTokenExpiresAt: s.timestamp().optional(),
        scope: s.string().optional(),
        userId: s.ref("authUsers"),
      })
      .index("auth_accounts_user_id_idx", ["userId"])
      .index("auth_accounts_provider_account_idx", ["providerId", "accountId"]),
    authSessions: s
      .table({
        expiresAt: s.timestamp(),
        ipAddress: s.string().optional(),
        token: s.string(),
        userAgent: s.string().optional(),
        userId: s.ref("authUsers"),
      })
      .index("auth_sessions_user_id_idx", ["userId"])
      .index("auth_sessions_token_idx", ["token"]),
    authVerifications: s
      .table({
        expiresAt: s.timestamp(),
        identifier: s.string(),
        value: s.string(),
      })
      .index("auth_verifications_identifier_idx", ["identifier"]),
    authRateLimits: s
      .table({
        count: s.int(),
        key: s.string(),
        lastRequest: s.int(),
      })
      .index("auth_rate_limits_key_idx", ["key"]),
    authJwks: s
      .table({
        createdAt: s.timestamp(),
        expiresAt: s.timestamp().optional(),
        privateKey: s.string(),
        publicKey: s.string(),
      })
      .index("auth_jwks_created_at_idx", ["createdAt"]),
  };
}

export type JazzAuthTables = ReturnType<typeof jazzAuthTables>;
