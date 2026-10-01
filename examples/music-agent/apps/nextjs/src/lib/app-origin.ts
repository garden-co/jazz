/**
 * The app's public origin: Better Auth's JWT issuer and audience, the Jazz
 * server's expected issuer and audience, and the JWKS host. One constant so
 * they cannot drift. Production must set it; development uses the local port.
 */
function readAppOrigin(): string {
  const configured = process.env.NEXT_PUBLIC_APP_ORIGIN;
  if (configured) return configured;
  if (process.env.NODE_ENV === "production") {
    throw new Error("NEXT_PUBLIC_APP_ORIGIN must be configured in production");
  }
  return "http://127.0.0.1:3000";
}

export const appOrigin = readAppOrigin();
