// One backend instance of the app on a shared remote core (as on a serverless host).
// Env: PORT, CORE_URL, APP_ID, CORE_SECRET, SIGNED_OUT (file of signed-out user ids, shared by all instances).
import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { readFileSync, appendFileSync } from "node:fs";
import { createJazz, remote } from "jazz-tools/backend";
import { app } from "#app";
import permissions from "#permissions" with { type: "json" };
const port = Number(process.env.PORT), file = process.env.SIGNED_OUT!;
const signedOut = () => { try { return new Set(readFileSync(file, "utf8").split("\n").filter(Boolean)); } catch { return new Set<string>(); } };
const jazz = createJazz({
  app, permissions: permissions as any,
  store: remote({ url: process.env.CORE_URL!, appId: process.env.APP_ID!, secret: process.env.CORE_SECRET! }),
  authenticate: async (req) => {
    const u = req.headers.get("authorization")?.replace(/^Bearer /, "");
    return u && !signedOut().has(u) ? { userId: u, expiresAt: Math.floor(Date.now() / 1000) + 3600 } : null;
  },
});
const h = new Hono();
// The sign-out handler: mark the user signed out for every instance, then revokeUser here.
h.post("/signout/:u", async (c) => { appendFileSync(file, c.req.param("u") + "\n"); const n = await jazz.revokeUser(c.req.param("u")); return c.json({ revokedHere: n }); });
h.post("/account/:u", async (c) => { const acc: any = await jazz.createAccountFor(c.req.param("u")); return c.json({ id: acc.id ?? acc }); });
h.all("/jazz/*", (c) => jazz.handle(c.req.raw));
await jazz.ready();
const server = serve({ fetch: h.fetch, port });
jazz.attach(server as any);
console.log("up " + port);
