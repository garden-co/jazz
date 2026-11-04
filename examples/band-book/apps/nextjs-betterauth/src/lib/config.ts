import { LOCAL_DEFAULTS } from "./build-config.mjs";

// The one place the app reads its public deployment settings. Next inlines
// NEXT_PUBLIC_* variables only where they are read literally, so they are read
// here and everything else imports these constants. The defaults are the local
// development values from build-config.mjs, which refuses a partial nonlocal
// configuration before the build starts.

/** The app's own origin, which Better Auth trusts. */
export const appOrigin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? LOCAL_DEFAULTS.origin;
export const jazzAppId = process.env.NEXT_PUBLIC_JAZZ_APP_ID ?? LOCAL_DEFAULTS.appId;
