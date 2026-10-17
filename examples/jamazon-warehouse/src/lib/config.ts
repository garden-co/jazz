/**
 * Configuration fails closed. A development run on a loopback origin gets
 * local defaults, so `pnpm dev` works without any setup. A production process
 * (`next build`, `next start`) is always treated as a deployment: it must set
 * NEXT_PUBLIC_APP_ORIGIN and real secrets, and never falls back to localhost
 * or the development secrets below.
 */
export const LOCAL_ORIGIN = "http://localhost:3000";

export const LOCAL_SECRETS = {
  BACKEND_SECRET: "jamazon-warehouse-development-backend-secret",
  BETTER_AUTH_SECRET: "jamazon-warehouse-development-better-auth-secret-7Qm2Xv9Lr4Tz",
} as const;

export type SecretName = keyof typeof LOCAL_SECRETS;

export interface ConfigEnv {
  NODE_ENV?: string;
  NEXT_PUBLIC_APP_ORIGIN?: string;
  NEXT_PUBLIC_JAZZ_APP_ID?: string;
  NEXT_PUBLIC_JAZZ_SERVER_URL?: string;
}

export interface ResolvedConfig {
  appOrigin: string;
  /** Local defaults apply: a non-production process on a loopback origin. */
  isLocalOrigin: boolean;
  jazzAppId: string;
  jazzServerUrl: string;
}

/** Resolve the public configuration, throwing for a production process without an origin. */
export function resolveConfig(env: ConfigEnv): ResolvedConfig {
  const production = env.NODE_ENV === "production";
  if (production && !env.NEXT_PUBLIC_APP_ORIGIN) {
    throw new Error("A production build must set NEXT_PUBLIC_APP_ORIGIN");
  }
  const appOrigin = env.NEXT_PUBLIC_APP_ORIGIN || LOCAL_ORIGIN;
  const loopback = ["localhost", "127.0.0.1"].includes(new URL(appOrigin).hostname);
  return {
    appOrigin,
    isLocalOrigin: !production && loopback,
    jazzAppId: env.NEXT_PUBLIC_JAZZ_APP_ID || "jamazon-warehouse-local",
    jazzServerUrl: env.NEXT_PUBLIC_JAZZ_SERVER_URL || "http://127.0.0.1:4200",
  };
}

/** A server secret: the configured one, or the development default only when local defaults apply. */
export function resolveSecret(
  config: Pick<ResolvedConfig, "isLocalOrigin">,
  name: SecretName,
  configured: string | undefined,
): string {
  if (configured) return configured;
  if (!config.isLocalOrigin) {
    throw new Error(`${name} must be set outside a local development run`);
  }
  return LOCAL_SECRETS[name];
}

// Each variable is read by its full name so Next inlines the public ones into
// the client bundle.
const config = resolveConfig({
  NODE_ENV: process.env.NODE_ENV,
  NEXT_PUBLIC_APP_ORIGIN: process.env.NEXT_PUBLIC_APP_ORIGIN,
  NEXT_PUBLIC_JAZZ_APP_ID: process.env.NEXT_PUBLIC_JAZZ_APP_ID,
  NEXT_PUBLIC_JAZZ_SERVER_URL: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL,
});

export const { appOrigin, isLocalOrigin, jazzAppId, jazzServerUrl } = config;

/** The origins Better Auth trusts: both loopback spellings in development. */
export const trustedOrigins = isLocalOrigin
  ? [appOrigin.replace("127.0.0.1", "localhost"), appOrigin.replace("localhost", "127.0.0.1")]
  : [appOrigin];

/** Read a server secret. Only ever call this on the server. */
export function serverSecret(name: SecretName): string {
  return resolveSecret(config, name, process.env[name]);
}

/** Check every server secret at startup, so a misconfigured deployment fails before serving. */
export function assertServerConfiguration(): void {
  serverSecret("BACKEND_SECRET");
  serverSecret("BETTER_AUTH_SECRET");
}
