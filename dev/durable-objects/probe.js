// What Durable Object SQLite (ctx.storage.sql) accepts, probed in workerd.
// Run: node run-probe.mjs  (see README.md). Each probe prints its result
// or the error workerd raised.

export class Probe {
  constructor(ctx) {
    this.ctx = ctx;
  }

  async fetch() {
    const storage = this.ctx.storage;
    const sql = storage.sql;
    const out = {};
    const t = async (name, f) => {
      try {
        const r = await f();
        out[name] = r === undefined ? "ok" : r;
      } catch (e) {
        out[name] = "ERR: " + String(e.message || e).slice(0, 160);
      }
    };
    const rows = (q, ...b) => sql.exec(q, ...b).toArray();
    const all = () => sql.exec("select group_concat(x) g from a").one().g;
    sql.exec("create table a(x)");

    // Transactions: SQL transaction statements are refused.
    for (const s of ["BEGIN", "SAVEPOINT s1", "RELEASE s1", "COMMIT", "ROLLBACK"])
      await t("sql " + s, () => sql.exec(s) && undefined);
    await t("nested transactionSync, inner throws", () => {
      storage.transactionSync(() => {
        sql.exec("insert into a values (1)");
        try {
          storage.transactionSync(() => {
            sql.exec("insert into a values (2)");
            throw new Error("inner");
          });
        } catch {}
      });
      return all();
    });
    await t("async transaction, await then throw", async () => {
      try {
        await storage.transaction(async () => {
          sql.exec("insert into a values (3)");
          await new Promise((r) => setTimeout(r, 5));
          throw new Error("boom");
        });
      } catch {}
      return all();
    });

    // PRAGMAs: a read-mostly allowlist.
    for (const p of [
      "journal_mode = WAL",
      "synchronous = NORMAL",
      "cache_size = -65536",
      "user_version",
      "temp_store = MEMORY",
      "page_count",
      "foreign_keys = ON",
      "recursive_triggers = ON",
      "table_info(a)",
      "optimize",
      "data_version",
      "quick_check",
    ])
      await t("pragma " + p, () => JSON.stringify(rows("PRAGMA " + p)).slice(0, 80));

    // Schema features.
    await t("temp table", () => sql.exec("create temp table tt(x)") && undefined);
    await t("attach", () => sql.exec("attach database ':memory:' as o") && undefined);
    await t("trigger", () => {
      sql.exec(
        "create table b(x); create table log(y); create trigger tb after insert on b when new.x > 0 begin insert into log values (new.x * 2); end;",
      );
      sql.exec("insert into b values (21)");
      return sql.exec("select y from log").one().y;
    });
    await t(
      "without rowid",
      () => sql.exec("create table wr(k blob primary key, v) without rowid") && undefined,
    );
    await t(
      "strict",
      () => sql.exec("create table st(k integer primary key, v blob) strict") && undefined,
    );
    await t(
      "partial + expression index",
      () => sql.exec("create index ip on a(x * 2) where x > 3") && undefined,
    );
    await t("fts5", () => sql.exec("create virtual table f using fts5(body)") && undefined);
    await t("rtree", () => sql.exec("create virtual table r using rtree(id, x0, x1)") && undefined);
    await t("table named _cf_x", () => sql.exec("create table _cf_x(x)") && undefined);
    await t(
      "analyze + sqlite_stat1",
      () => (sql.exec("analyze"), rows("select count(*) c from sqlite_stat1")[0].c),
    );

    // Functions: an allowlist; what jazz-db uses is all allowed.
    for (const f of [
      "instr('ab','b')",
      "substr('abc',2)",
      "unhex('00ff')",
      "json_extract('[1,2]','$[1]')",
      "(select count(*) from json_each('[1,2]'))",
      "sqlite_version()",
      "generate_series(1,2)",
      "(select count(*) from carray(1,1))",
    ])
      await t("fn " + f, () => JSON.stringify(rows("select " + f + " v")[0].v));

    // Limits.
    for (const n of [100, 101])
      await t(
        `bound params ${n}`,
        () =>
          rows(
            "select count(*) c from a where x in (" + Array(n).fill("?").join(",") + ")",
            ...Array(n).fill(1),
          )[0].c,
      );
    for (const n of [99_000, 100_000])
      await t(`statement ${n} bytes`, () => rows("select 1 v /*" + "x".repeat(n - 15) + "*/")[0].v);
    await t(
      "150 KB across 30 statements in one exec",
      () =>
        sql.exec(
          Array.from(
            { length: 30 },
            (_, i) => `create table s${i}(x /*${"y".repeat(5000)}*/);`,
          ).join("\n"),
        ) && undefined,
    );
    for (const n of [99, 101])
      await t(
        `AND chain ${n}`,
        () => rows("select " + Array(n).fill("1").join(" AND ") + " v")[0].v,
      );
    for (const n of [100, 101])
      await t(
        `columns ${n}`,
        () =>
          sql.exec(
            `create table c${n}(` + Array.from({ length: n }, (_, i) => "c" + i).join(",") + ")",
          ) && undefined,
      );
    for (const n of [5, 6])
      await t(
        `compound select ${n}`,
        () => rows(Array(n).fill("select 1").join(" union all ")).length,
      );
    for (const n of [50, 51])
      await t(`LIKE pattern ${n}`, () => rows("select 'a' like ? v", "%".repeat(n))[0].v);
    for (const mib of [2, 3])
      await t(
        `blob ${mib} MiB`,
        () =>
          (sql.exec("create table if not exists big(b)"),
          sql.exec("insert into big values (?)", new Uint8Array(mib * 1024 * 1024 - 64))) &&
          undefined,
      );

    // Values: integers come back as JS numbers; BigInt cannot be bound.
    await t("read 2^53 + 1", () => String(rows("select 9007199254740993 v")[0].v));
    await t("bind BigInt", () => rows("select ? v", 9007199254740993n)[0].v);
    await t(
      "bind 2^53 + 1 as text into an INTEGER compare",
      () => (
        sql.exec("create table i(n integer)"),
        sql.exec("insert into i values (9007199254740993)"),
        rows("select count(*) c from i where n = ?", "9007199254740993")[0].c
      ),
    );
    await t("blob comes back as", () =>
      Object.prototype.toString.call(rows("select x'00ff' v")[0].v),
    );

    // Cost of one exec (same text is cached by workerd).
    await t("exec cost", () => {
      let s = performance.now();
      for (let i = 0; i < 20000; i++) sql.exec("select x from a where x = ?", i).toArray();
      const same = ((performance.now() - s) * 1000) / 20000;
      s = performance.now();
      for (let i = 0; i < 20000; i++) sql.exec("select x from a where x = " + i).toArray();
      const unique = ((performance.now() - s) * 1000) / 20000;
      return `same text ${same.toFixed(2)} us, unique text ${unique.toFixed(2)} us`;
    });
    return Response.json(out);
  }
}

export default {
  async fetch(request, env) {
    return env.PROBE.get(env.PROBE.idFromName(crypto.randomUUID())).fetch(request);
  },
};
