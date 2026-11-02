/** The Jazz app id and origin a local run uses when none is set. */
export const LOCAL_APP_ID = "00000000-0000-0000-0000-00000005a6e2";
export const LOCAL_ORIGIN = "http://127.0.0.1:3000";

/**
 * StagePlan's server configuration. It fails closed: the local defaults apply
 * only outside production. A production server must name its origin, its Jazz
 * app and a Better Auth secret, and a remote sync server needs both app secrets.
 */
export function serverConfig(env = process.env) {
  const production = env.NODE_ENV === "production";
  const missing: string[] = [];
  const required = (name: string, value: string | undefined, local?: string) => {
    if (value) return value;
    if (!production && local !== undefined) return local;
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
      // Development only; never used by a production server.
      "stage-plan-local-development-secret-not-for-deployments",
    ),
    remote: env.JAZZ_SERVER_URL
      ? {
          url: env.JAZZ_SERVER_URL,
          adminSecret: required("JAZZ_ADMIN_SECRET", env.JAZZ_ADMIN_SECRET),
          backendSecret: required("BACKEND_SECRET", env.BACKEND_SECRET),
        }
      : undefined,
    dataPath: env.JAZZ_DATA_PATH || ".jazz",
  };
  if (missing.length) throw new Error(`StagePlan must be started with ${missing.join(", ")}.`);
  return config;
}
