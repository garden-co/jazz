/** The Jazz app id a local run uses when none is set. */
export const LOCAL_APP_ID = "00000000-0000-0000-0000-000000000301";

/**
 * BigLabel's Jazz configuration. Development uses a local app id and keeps
 * the data in `.jazz/`; production must name its app. With JAZZ_SERVER_URL
 * set, the data lives on that sync server (such as Jazz Cloud) instead.
 */
export function jazzConfig(env: Record<string, string | undefined> = process.env) {
  const production = env.NODE_ENV === "production";
  const appId = env.NEXT_PUBLIC_JAZZ_APP_ID || (production ? undefined : LOCAL_APP_ID);
  if (!appId) throw new Error("NEXT_PUBLIC_JAZZ_APP_ID must be configured in production");
  const url = env.JAZZ_SERVER_URL;
  if (!url) return { appId, remote: undefined, dataPath: env.JAZZ_DATA_PATH || ".jazz" };
  const adminSecret = env.JAZZ_ADMIN_SECRET;
  const backendSecret = env.BACKEND_SECRET;
  if (!adminSecret || !backendSecret)
    throw new Error("JAZZ_SERVER_URL needs JAZZ_ADMIN_SECRET and BACKEND_SECRET");
  return { appId, remote: { url, adminSecret, backendSecret }, dataPath: undefined };
}
