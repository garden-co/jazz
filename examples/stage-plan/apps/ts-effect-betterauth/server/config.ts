/** The Jazz app id and origin a local run uses when none is set. */
export const LOCAL_APP_ID = "00000000-0000-0000-0000-00000005a6e2";
export const LOCAL_ORIGIN = "http://127.0.0.1:3000";

/**
 * StagePlan's server configuration. It fails closed: the local defaults apply
 * only to a local run, a non-production process on a loopback origin.
 * Anything else is a deployment and must name its origin, its Jazz app and a
 * Better Auth secret; a remote sync server needs the app's admin secret.
 */
export function serverConfig(env = process.env) {
  const production = env.NODE_ENV === "production";
  const local = !production && isLoopback(env.APP_ORIGIN ?? LOCAL_ORIGIN);
  const missing: string[] = [];
  const required = (name: string, value: string | undefined, fallback?: string) => {
    if (value) return value;
    if (local && fallback !== undefined) return fallback;
    missing.push(name);
    return "";
  };
  const config = {
    production,
    origin: required("APP_ORIGIN", env.APP_ORIGIN, LOCAL_ORIGIN),
    appId: required("VITE_JAZZ_APP_ID", env.VITE_JAZZ_APP_ID, LOCAL_APP_ID),
    betterAuthSecret: required(
      "BETTER_AUTH_SECRET",
      env.BETTER_AUTH_SECRET,
      // Local runs only; never used by a deployment.
      "stage-plan-local-development-secret-not-for-deployments",
    ),
    remote: env.JAZZ_SERVER_URL
      ? {
          url: env.JAZZ_SERVER_URL,
          adminSecret: required("JAZZ_ADMIN_SECRET", env.JAZZ_ADMIN_SECRET),
        }
      : undefined,
    dataPath: env.JAZZ_DATA_PATH || ".jazz",
  };
  if (missing.length) throw new Error(`StagePlan must be started with ${missing.join(", ")}.`);
  return config;
}

function isLoopback(origin: string) {
  const { hostname } = new URL(origin);
  return hostname === "127.0.0.1" || hostname === "localhost";
}
