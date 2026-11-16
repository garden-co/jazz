// Generic test-bed backend: Hono + createJazz + sqlite, bearer "user:claimsJSON" auth, device guests.
import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { createJazz, sqlite, acceptDeviceAccounts } from "jazz-tools/backend";
import { app } from "#app";
import permissions from "#permissions";
const port = Number(process.env.PORT ?? 8600);
const jazz = createJazz({
  app,
  permissions,
  store: sqlite({ path: process.env.DATA ?? "./data/jazz" }),
  authenticate: async (req) => {
    const a = req.headers.get("authorization")?.replace(/^Bearer /, "");
    if (!a) return acceptDeviceAccounts(req);
    const [userId, claims] = a.split("|");
    return {
      userId,
      expiresAt: Math.floor(Date.now() / 1000) + 3600,
      claims: claims ? JSON.parse(claims) : {},
    };
  },
});
const h = new Hono();
h.post("/account/:u", async (c) => {
  const acc: any = await jazz.createAccountFor(c.req.param("u"));
  return c.json({ id: acc.id ?? acc });
});
h.post("/seed", async (c) => {
  const o: any = await jazz.db.insert(app.orgs, { name: "Acme" });
  const orgId = o.value?.id ?? o.id;
  for (const [userId, role] of [
    ["ann", "admin"],
    ["ed", "editor"],
    ["vic", "viewer"],
  ] as const) {
    await jazz.createAccountFor(userId);
    await jazz.db.insert(app.members, { orgId, userId, role });
  }
  for (const u of ["out", "staffer", "friend"]) await jazz.createAccountFor(u);
  return c.json({ orgId });
});
h.all("/jazz/*", (c) => jazz.handle(c.req.raw));
await jazz.ready();
const server = serve({ fetch: h.fetch, port });
jazz.attach(server as any);
console.log("up " + port);
