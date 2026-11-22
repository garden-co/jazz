// Stock TS createJazz backend serving StagePlan for the language gate.
// Users: Bearer <OIDC token, aud jazz:stageplan-users> -> userId = sub. Machines: backendClients via trustWorkload.
// Env: APP_DIR (dir with jazz.generated.ts + jazz.permissions.json), PORT (8780), OIDC (http://127.0.0.1:8790), DATA
import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { createRemoteJWKSet, jwtVerify } from "jose";
import { createJazz, sqlite, trustWorkload, acceptDeviceAccounts } from "jazz-tools/backend";
import { readFileSync } from "node:fs";
const APP_DIR = process.env.APP_DIR!,
  PORT = Number(process.env.PORT ?? 8780),
  OIDC = process.env.OIDC ?? "http://127.0.0.1:8790";
// Users come from a SEPARATE issuer: tokens from a backendClients issuer never reach authenticate.
const USERS_OIDC = process.env.USERS_OIDC ?? "http://127.0.0.1:8791";
const { app } = await import(`${APP_DIR}/jazz.generated.ts`);
const permissions = JSON.parse(readFileSync(`${APP_DIR}/jazz.permissions.json`, "utf8"));
const jwks = createRemoteJWKSet(new URL(`${USERS_OIDC}/jwks`));
const jazz = createJazz({
  app,
  permissions,
  store: sqlite({ path: process.env.DATA ?? "./data/jazz" }),
  authenticate: async (req) => {
    const t = req.headers.get("authorization")?.replace(/^Bearer /, "");
    if (!t) return process.env.DEVICE ? acceptDeviceAccounts(req) : null;
    try {
      const { payload } = await jwtVerify(t, jwks, {
        issuer: USERS_OIDC,
        audience: "jazz:stageplan-users",
      });
      return { userId: String(payload.sub), expiresAt: Number(payload.exp), claims: {} };
    } catch (e: any) {
      if (process.env.DEBUG_AUTH) console.error("authenticate refused:", e.code ?? "", e.message);
      return null;
    }
  },
  backendClients: [
    trustWorkload({
      issuer: OIDC,
      audience: "jazz:stageplan-gate",
      claims: { sub: "gate-trusted-job" },
      as: "backend",
    }),
    trustWorkload({
      issuer: OIDC,
      audience: "jazz:stageplan-gate",
      claims: { sub: "gate-scoped-agent" },
      as: () => ({ userId: "scoped-agent" }),
    }),
  ],
});
const h = new Hono();
h.post("/account/:u", async (c) => {
  const a: any = await jazz.createAccountFor(c.req.param("u"));
  return c.json({ id: a.id ?? a });
});
h.all("/jazz/*", (c) => jazz.handle(c.req.raw));
await jazz.ready();
const server = serve({ fetch: h.fetch, port: PORT });
jazz.attach(server as any);
console.log(`stageplan backend up http://127.0.0.1:${PORT}/jazz`);
