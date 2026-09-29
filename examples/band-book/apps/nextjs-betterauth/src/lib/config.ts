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
/** Browser and backend use the same Jazz environment. */
export const jazzEnv = appOrigin === LOCAL_DEFAULTS.origin ? "dev" : "prod";
