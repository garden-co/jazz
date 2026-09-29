/**
 * Local defaults let `pnpm dev` and a bare `pnpm build` work without any
 * configuration. A deployment sets NEXT_PUBLIC_APP_ORIGIN and must then also
 * provide real secrets; see `serverSecret`.
 */
export const LOCAL_ORIGIN = "http://localhost:3000";

export const appOrigin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? LOCAL_ORIGIN;
export const jazzAppId = process.env.NEXT_PUBLIC_JAZZ_APP_ID ?? "jamazon-warehouse-local";
export const jazzServerUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL ?? "http://127.0.0.1:4200";

/** Read a server secret, falling back to a checked-in value only for the local origin. */
export function serverSecret(name: "BACKEND_SECRET" | "BETTER_AUTH_SECRET"): string {
  const configured = process.env[name];
  if (configured) return configured;
  if (appOrigin !== LOCAL_ORIGIN) {
    throw new Error(`${name} must be configured when NEXT_PUBLIC_APP_ORIGIN is not local`);
  }
  return LOCAL_SECRETS[name];
}

export const LOCAL_SECRETS = {
  BACKEND_SECRET: "jamazon-warehouse-development-backend-secret",
  BETTER_AUTH_SECRET: "jamazon-warehouse-development-better-auth-secret-7Qm2Xv9Lr4Tz",
} as const;
