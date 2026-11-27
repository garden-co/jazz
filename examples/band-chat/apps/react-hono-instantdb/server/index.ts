import { serve } from "@hono/node-server";
import { serveStatic } from "@hono/node-server/serve-static";
import { Hono } from "hono";
import { AuthError, signIn, signUp } from "./auth";

const production = process.env.NODE_ENV === "production";
// In development Vite serves the browser app and proxies /api here; in
// production this server also serves the built app. Instant itself is hosted:
// the browser syncs with it directly.
const port = Number(process.env.PORT ?? (production ? 3000 : 3001));

const app = new Hono();
app.get("/health", (c) => c.text("ok"));
const authRoute = (handler: (body: any) => Promise<{ token: string }>) => async (c: any) => {
  try {
    return c.json(await handler(await c.req.json()));
  } catch (cause) {
    if (cause instanceof AuthError) return c.json({ message: cause.message }, 401);
    throw cause;
  }
};
app.post("/api/auth/sign-up", authRoute(signUp));
app.post("/api/auth/sign-in", authRoute(signIn));
if (production) {
  app.use("/*", serveStatic({ root: "./dist" }));
  // A single-page app: every other path renders index.html.
  app.get("*", serveStatic({ path: "./dist/index.html" }));
}

serve({ fetch: app.fetch, port }, (info) => {
  console.log(`BandChat listening on http://localhost:${info.port}`);
});
