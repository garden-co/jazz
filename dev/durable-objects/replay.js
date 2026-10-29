// Replays jobs from replay-prep.py against Durable Object SQLite: DDL runs,
// every other statement is prepared with EXPLAIN and NULL bindings.
export class Replay {
  constructor(ctx) {
    this.ctx = ctx;
  }
  async fetch(req) {
    const ops = await req.json();
    const sql = this.ctx.storage.sql;
    const res = [];
    for (const [kind, text, params] of ops) {
      try {
        const binds = Array(params).fill(null);
        if (kind === "ddl") sql.exec(text, ...binds);
        else sql.exec("EXPLAIN " + text, ...binds).toArray();
        res.push(null);
      } catch (e) {
        res.push(String(e.message || e));
      }
    }
    return Response.json(res);
  }
}
export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    return env.R.get(env.R.idFromName(u.pathname)).fetch(req);
  },
};
