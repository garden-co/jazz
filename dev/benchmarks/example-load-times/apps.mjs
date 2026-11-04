// The ten example apps this suite measures, with the production env their
// build and servers share. Client values (origin, app ID, sync server URL) are
// inlined at build time, so build.mjs and the benchmark must use the same
// ports. Each app gets three ports: the CDN-like front the browser talks to
// (`port`), the framework's production server behind it (`port + 2000`) and a
// local Jazz sync server (`port + 1000`). Browsers sync with the app's own
// backend under `/jazz`; a Next.js app's backend (createJazz) keeps its data
// on that sync server. The Vite apps run createJazz inside `vite preview`.
import { join } from "node:path";
import { fileURLToPath } from "node:url";

export const here = fileURLToPath(new URL(".", import.meta.url));
export const repo = join(here, "../../..");
/** Ignored output: the CDN's precompressed copies, pidfiles, results. */
export const generated = join(here, ".generated");

const next = (dir, port) => ({ kind: "next", dir, port });
const vite = (dir, port) => ({ kind: "vite", dir, port });

// The local sync server options of each app, in the docs page's order.
export const apps = {
  "band-book": next("examples/band-book/apps/nextjs-betterauth", 3461),
  "band-chat": next("examples/band-chat/apps/nextjs-betterauth", 3465),
  "big-label": next("examples/big-label", 3462),
  "epic-drop": vite("examples/epic-drop", 5287),
  jamazon: {
    ...next("examples/jamazon/apps/nextjs-betterauth", 3466),
    env: { PAYMENT_PROVIDER: "sandbox" },
  },
  "poster-shop": next("examples/poster-shop/apps/nextjs-betterauth", 3464),
  "record-player": next("examples/record-player/apps/next-betterauth", 3467),
  "stage-plan": vite("examples/stage-plan/apps/react-localfirst", 5392),
  wequencer: next("examples/wequencer/apps/next-betterauth", 3463),
  "world-tour": vite("examples/world-tour", 5391),
};
export const appIds = Object.keys(apps);

const uuid = (n) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;

/** Everything about one app that the build and the servers need. */
export function appConfig(id) {
  const a = apps[id];
  if (!a) throw new Error(`unknown app: ${id}`);
  const i = appIds.indexOf(id) + 1;
  const syncPort = a.port + 1000;
  const backendPort = a.port + 2000;
  const appId = uuid(9000 + i);
  const serverUrl = `http://127.0.0.1:${syncPort}`;
  const origin = `http://127.0.0.1:${a.port}`;
  // Throwaway secrets for a loopback-only benchmark server.
  const backendSecret = `load-times-${id}-backend-secret-0123456789abcdef`;
  const adminSecret = `load-times-${id}-admin-secret`;
  const betterAuthSecret = `load-times-${id}-better-auth-secret-0123456789abcdef0123456789`;
  const env =
    a.kind === "next"
      ? {
          NODE_ENV: "production",
          NEXT_TELEMETRY_DISABLED: "1",
          NEXT_PUBLIC_APP_ORIGIN: origin,
          NEXT_PUBLIC_JAZZ_APP_ID: appId,
          JAZZ_APP_ID: appId,
          // The app's backend keeps its data on the local sync server.
          JAZZ_SERVER_URL: serverUrl,
          JAZZ_ADMIN_SECRET: adminSecret,
          BETTER_AUTH_SECRET: betterAuthSecret,
          ...a.env,
        }
      : { VITE_JAZZ_APP_ID: appId, JAZZ_IN_MEMORY: "1" };
  const sync = { appId, backendSecret, adminSecret };
  const dir = join(repo, a.dir);
  const [assetPrefix, staticDir, buildDir] =
    a.kind === "next"
      ? ["/_next/static/", join(dir, ".next/static"), ".next"]
      : ["/assets/", join(dir, "dist/assets"), "dist"];
  return {
    id,
    ...a,
    dir,
    origin,
    syncPort,
    backendPort,
    env,
    sync,
    assetPrefix,
    staticDir,
    buildDir,
  };
}
