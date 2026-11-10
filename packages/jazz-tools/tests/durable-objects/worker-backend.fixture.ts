import { createJazz, remote, schema as s } from "jazz-tools/backend";
const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.never();
  policy.notes.allowInsert.never();
  policy.notes.allowUpdate.never();
  policy.notes.allowDelete.never();
});
export default {
  async fetch(
    request: Request,
    env: { CORE_URL: string; APP_ID: string; SECRET: string },
    ctx: { waitUntil(work: Promise<unknown>): void },
  ) {
    const jazz = createJazz({
      app,
      permissions,
      authenticate: (req) => {
        const userId = req.headers.get("x-test-user");
        return userId ? { userId } : null;
      },
      store: remote({ url: env.CORE_URL, appId: env.APP_ID, secret: env.SECRET }),
    });
    let upgraded = false;
    try {
      await jazz.ready();
      const path = new URL(request.url).pathname;
      if (path.startsWith("/jazz/")) {
        const response = await jazz.handle(request, ctx);
        upgraded = response.status === 101;
        return response;
      }
      if (path === "/scoped")
        return Response.json(await (await jazz.forReqUser(request)).all(app.notes));
      if (path === "/attributed") {
        const db = await jazz.withAttribution(request);
        await db.insert(app.notes, { title: await request.text() });
      } else if (request.method === "POST")
        await jazz.db.insert(app.notes, { title: await request.text() });
      return Response.json(await jazz.db.all(app.notes));
    } finally {
      if (!upgraded) await jazz.close();
    }
  },
};
