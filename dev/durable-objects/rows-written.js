// Replays one connection's write statements (expanded SQL) on Durable Object
// SQLite and sums workerd's billed `rowsWritten` and `rowsRead` per phase.
export class Rows {
  constructor(ctx) {
    this.ctx = ctx;
  }

  async fetch(request) {
    const { ops } = await request.json();
    const sql = this.ctx.storage.sql;
    const phases = {};
    let phase = "setup";
    for (const op of ops) {
      if (op.mark) {
        phase = op.mark;
        continue;
      }
      const p = (phases[phase] ??= {
        statements: 0,
        writes: 0,
        rowsWritten: 0,
        rowsRead: 0,
        errors: [],
      });
      try {
        const binds = op.blobs.map((hex) =>
          Uint8Array.from(hex.match(/../g) ?? [], (b) => parseInt(b, 16)),
        );
        const cursor = sql.exec(op.sql, ...binds);
        cursor.toArray();
        p.statements += 1;
        if (cursor.rowsWritten > 0) p.writes += 1;
        p.rowsWritten += cursor.rowsWritten;
        p.rowsRead += cursor.rowsRead;
      } catch (e) {
        if (p.errors.length < 5)
          p.errors.push(String(e.message || e).slice(0, 160) + " :: " + op.sql.slice(0, 120));
      }
    }
    return Response.json(phases);
  }
}

export default {
  async fetch(request, env) {
    return env.ROWS.get(env.ROWS.idFromName("rows")).fetch(request);
  },
};
