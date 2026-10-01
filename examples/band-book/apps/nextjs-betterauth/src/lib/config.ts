import { LOCAL_DEFAULTS } from "./build-config.mjs";

// The one place the app reads its deployment settings. Next inlines
// NEXT_PUBLIC_* variables only where they are read literally, so they are read
// here and everything else imports these constants. The defaults are the local
// development values from build-config.mjs, which refuses a partial nonlocal
// configuration before the build starts.

/** The app's own origin. Better Auth issues JWTs with this issuer. */
export const appOrigin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? LOCAL_DEFAULTS.origin;
export const jazzAppId = process.env.NEXT_PUBLIC_JAZZ_APP_ID ?? LOCAL_DEFAULTS.appId;
export const jazzServerUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL ?? LOCAL_DEFAULTS.serverUrl;
/**
 * Better Auth signs JWTs with this issuer and audience, and every verifier
 * (the Jazz server via withJazz, and the server routes) checks both.
 */
export const jwtIssuer = appOrigin;
export const jwtAudience = appOrigin;
/**
 * The Jazz environment, shared by the browser and the backend so both read
 * and write the same data: NEXT_PUBLIC_JAZZ_ENV when set, else "prod" in a
 * production build, else "dev" (whatever port the dev server runs on).
 */
export const jazzEnv =
  process.env.NEXT_PUBLIC_JAZZ_ENV ?? (process.env.NODE_ENV === "production" ? "prod" : "dev");
