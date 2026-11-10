import { createJazz, durableObjects, schema as s } from "jazz-tools/backend";
const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.never();
  policy.notes.allowInsert.never();
});
export default {
  async fetch(request: Request, env: { ACCOUNT: string; TOKEN: string; SECRET: string }) {
    const jazz = createJazz({
      app,
      permissions,
      store: durableObjects({
        name: "worker-provider-test",
        credentials: { accountId: env.ACCOUNT, apiToken: env.TOKEN },
        secret: env.SECRET,
      }),
    });
    try {
      await jazz.ready();
      if (request.method === "POST")
        await jazz.db.insert(app.notes, { title: await request.text() }).wait({ tier: "global" });
      return Response.json({
        appId: jazz.appId,
        titles: (await jazz.db.all(app.notes)).map((row) => row.title).sort(),
      });
    } finally {
      await jazz.close();
    }
  },
};
