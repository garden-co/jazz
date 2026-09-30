/**
 * The app's public origin: Better Auth's base URL and the JWT issuer and
 * audience. One constant, so issuer and audience cannot drift apart.
 * Development defaults to the local dev origin; production must set it.
 */
function appOrigin() {
  const configured = process.env.NEXT_PUBLIC_APP_ORIGIN;
  if (configured) return configured;
  if (process.env.NODE_ENV === "production") {
    throw new Error("NEXT_PUBLIC_APP_ORIGIN must be configured in production");
  }
  return "http://127.0.0.1:3000";
}

export const APP_ORIGIN = appOrigin();
