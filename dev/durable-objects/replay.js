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
    for (const [index, [kind, text, params]] of ops.entries()) {
      if (req.headers.has("x-replay-debug"))
        console.log("replay", new URL(req.url).pathname, index, kind, text);
      try {
        const binds = Array(params).fill(null);
        if (kind === "ddl") sql.exec(text, ...binds);
        // QUERY PLAN prepares without executing the statement. Drain its
        // cursor before commit, but do not enumerate full bytecode: workerd
        // 1.20260730.1 can hang in sqlite3_step/strlen while rendering EXPLAIN
        // cells after schema changes. Bytecode rendering is not SQL execution
        // coverage, and is unnecessary for this prepare/authorizer check.
        else sql.exec("EXPLAIN QUERY PLAN " + text, ...binds).toArray();
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
