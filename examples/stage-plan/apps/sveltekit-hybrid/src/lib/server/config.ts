import { dev } from "$app/environment";
import { env } from "$env/dynamic/private";
import { env as publicEnv } from "$env/dynamic/public";
import { LOCAL_APP_ID, LOCAL_ORIGIN } from "$lib/config";

/**
 * StagePlan's server configuration. It fails closed: the local defaults apply
 * only to `pnpm dev`. A production server must name its origin, its Jazz app
 * and a Better Auth secret, and a remote sync server needs both app secrets.
 */
export function serverConfig() {
  const missing: string[] = [];
  const required = (name: string, value: string | undefined, local?: string) => {
    if (value) return value;
    if (dev && local !== undefined) return local;
    missing.push(name);
    return "";
  };
  const config = {
    origin: required("APP_ORIGIN", env.APP_ORIGIN, LOCAL_ORIGIN),
    appId: required("PUBLIC_JAZZ_APP_ID", publicEnv.PUBLIC_JAZZ_APP_ID, LOCAL_APP_ID),
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
