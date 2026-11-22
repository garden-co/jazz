// A stock createJazz backend on the candidate's jazz-tools packages, over the candidate's core
// binary (release-server), as t10 runs it. Users sign in with tokens from t9's OIDC issuer; a
// client without one is a device account.
// Env: PORT, APP_DIR, APP_ID, CORE_URL, CORE_SECRET, USERS_OIDC.
import { readFileSync } from "node:fs";
import { serve } from "@hono/node-server";
import { Hono } from "hono";
import { createRemoteJWKSet, jwtVerify } from "jose";
import { acceptDeviceAccounts, createJazz, remote } from "jazz-tools/backend";

const { PORT, APP_DIR, APP_ID, CORE_URL, CORE_SECRET, USERS_OIDC } = process.env as Record<string, string>;
const { app } = await import(`${APP_DIR}/jazz.generated.ts`);
const permissions = JSON.parse(readFileSync(`${APP_DIR}/jazz.permissions.json`, "utf8"));
const jwks = createRemoteJWKSet(new URL(`${USERS_OIDC}/jwks`));

const jazz = createJazz({
  app,
  permissions,
  store: remote({ url: CORE_URL, appId: APP_ID, secret: CORE_SECRET }),
  authenticate: async (request) => {
    const token = request.headers.get("authorization")?.replace(/^Bearer /, "");
    if (!token) return acceptDeviceAccounts(request);
    try {
      const { payload } = await jwtVerify(token, jwks, {
        issuer: USERS_OIDC,
        audience: "jazz:stageplan-users",
      });
      return { userId: String(payload.sub), expiresAt: Number(payload.exp), claims: {} };
    } catch (error: any) {
      if (process.env.DEBUG_AUTH) console.error("authenticate refused:", error.code ?? "", error.message);
      return null;
    }
  },
});

const hono = new Hono();
hono.get("/up", (c) => c.json({ appId: jazz.appId }));
hono.all("/jazz/*", (c) => jazz.handle(c.req.raw));
await jazz.ready();
const server = serve({ fetch: hono.fetch, port: Number(PORT), hostname: "127.0.0.1" });
jazz.attach(server as any);
console.log("backend up");
