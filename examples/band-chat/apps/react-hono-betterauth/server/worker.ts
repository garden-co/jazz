// BandChat entirely on Cloudflare: one Worker with
// - JazzCore: the app's Jazz core, in a SQLite-backed Durable Object;
// - JazzBackend: the app's backend (createJazz, Better Auth and the Hono
//   routes), in a second Durable Object, since a Jazz keeps its socket to
//   the core open across requests and only a Durable Object can;
// - the Worker's fetch: the built SPA from static assets, and everything
//   under /api, /jazz and /health forwarded to the backend object.
// See wrangler.jsonc for the bindings and README.md ("Deploy to Cloudflare").
import { betterAuth } from "better-auth";
import { Hono } from "hono";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { createJazz, durableObjects, jazzBackend, routeToJazzBackend } from "jazz-tools/backend";
import { app } from "../jazz.generated";
import permissions from "../jazz.permissions.json" with { type: "json" };

export { JazzCore } from "jazz-tools/durable-object";

interface Env {
  /** The JazzCore namespace (its one object holds the app's data). */
  JAZZ: Parameters<typeof durableObjects>[0] extends { binding: infer B } ? B : never;
  JAZZ_BACKEND: Parameters<typeof routeToJazzBackend>[1]["JAZZ_BACKEND"];
  /** The built SPA (wrangler's static assets). */
  ASSETS: { fetch(request: Request): Promise<Response> };
  /** The app's public origin, e.g. https://band-chat.example.workers.dev. */
  APP_ORIGIN: string;
  BETTER_AUTH_SECRET: string;
  /** Admits this backend to its core: 32+ random characters, kept across deployments. */
  JAZZ_CORE_SECRET: string;
}

export const JazzBackend = jazzBackend((env: Env) => {
  for (const name of ["APP_ORIGIN", "BETTER_AUTH_SECRET", "JAZZ_CORE_SECRET"] as const) {
    if (!env[name]) throw new Error(`BandChat on Cloudflare must set ${name} (see README.md)`);
  }
  const jazz = createJazz({
    app,
    permissions,
    store: durableObjects({ binding: env.JAZZ, secret: env.JAZZ_CORE_SECRET }),
    // Better Auth's session cookie decides who the user is.
    authenticate: async (request) => {
      const session = await auth.api.getSession({ headers: request.headers });
      return session
        ? {
            userId: session.user.id,
            expiresAt: Math.floor(session.session.expiresAt.getTime() / 1000),
          }
        : null;
    },
  });
  const auth = betterAuth({
    baseURL: env.APP_ORIGIN,
    trustedOrigins: [env.APP_ORIGIN],
    secret: env.BETTER_AUTH_SECRET,
    // Better Auth's tables live in BandChat's own Jazz, as on Node.
    database: jazzAdapter({ db: async () => (await jazz.ready()).db, schema: app.wasmSchema }),
    emailAndPassword: {
      enabled: true,
      autoSignIn: true,
      minPasswordLength: 8,
      requireEmailVerification: false,
    },
  });

  const routes = new Hono();
  routes.get("/health", (c) => c.text("ok"));
  routes.on(["GET", "POST"], "/api/auth/*", (c) => auth.handler(c.req.raw));
  // `/jazz` itself is served by jazzBackend (sync sockets and requests).
  return { jazz, fetch: routes.fetch };
});

export default {
  fetch(request: Request, env: Env): Promise<Response> {
    const { pathname } = new URL(request.url);
    if (/^\/(api|jazz|health)(\/|$)/.test(pathname)) return routeToJazzBackend(request, env);
    return env.ASSETS.fetch(request);
  },
};
