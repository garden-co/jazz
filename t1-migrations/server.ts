// Generic test-bed backend: Hono + createJazz + sqlite, bearer "user:claimsJSON" auth, device guests.
import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { createJazz, sqlite, acceptDeviceAccounts } from "jazz-tools/backend";
import { app } from "#app";
import permissions from "#permissions" with { type: "json" };
const port = Number(process.env.PORT ?? 8600);
const jazz = createJazz({
  app, permissions, store: sqlite({ path: process.env.DATA ?? "./data/jazz" }),
  authenticate: async (req) => {
    const a = req.headers.get("authorization")?.replace(/^Bearer /, "");
    if (!a) return acceptDeviceAccounts(req);
    const [userId, claims] = a.split("|");
    return { userId, expiresAt: Math.floor(Date.now() / 1000) + 3600, claims: claims ? JSON.parse(claims) : {} };
  },
});
const h = new Hono();
h.post("/account/:u", async (c) => { const acc: any = await jazz.createAccountFor(c.req.param("u")); return c.json({ id: acc.id ?? acc }); });
h.all("/jazz/*", (c) => jazz.handle(c.req.raw));
await jazz.ready();
const server = serve({ fetch: h.fetch, port });
jazz.attach(server as any);
console.log("up " + port);
