// Platform proof for the value boundary, not a replacement Jazz query compiler.
export class ValueProbe {
  constructor(ctx) {
    this.storage = ctx.storage;
  }
  fetch() {
    const sql = this.storage.sql;
    const rows = (query, ...bindings) => sql.exec(query, ...bindings).toArray();
    sql.exec("CREATE TABLE values_probe (id INTEGER PRIMARY KEY, value)");
    const integers = [
      "-9223372036854775808",
      "-9007199254740993",
      "-9007199254740992",
      "-9007199254740991",
      "0",
      "9007199254740991",
      "9007199254740992",
      "9007199254740993",
      "9223372036854775807",
    ];
    for (const [id, value] of integers.entries()) {
      sql.exec("INSERT INTO values_probe VALUES (?, +CAST(? AS INTEGER))", id, value);
    }
    const exact = rows(
      "SELECT typeof(value) AS type, CAST(value AS TEXT) AS value FROM values_probe ORDER BY values_probe.value",
    );
    const matches = integers.map((value) =>
      rows("SELECT id FROM values_probe WHERE value = +CAST(? AS INTEGER)", value).map(
        (row) => row.id,
      ),
    );
    // An integer CAST has affinity. A bare bound integer does not. Removing
    // expression affinity with unary + preserves comparisons with TEXT cells.
    sql.exec("CREATE TABLE comparison_probe (value)");
    sql.exec("INSERT INTO comparison_probe VALUES (?), (10)", "10");
    const comparisons = {
      bound: rows("SELECT typeof(value) AS type FROM comparison_probe WHERE value = ?", 10),
      cast: rows(
        "SELECT typeof(value) AS type FROM comparison_probe WHERE value = CAST(? AS INTEGER)",
        "10",
      ),
      exact: rows(
        "SELECT typeof(value) AS type FROM comparison_probe WHERE value = +CAST(? AS INTEGER)",
        "10",
      ),
    };
    // Decode storage classes, not Number.isInteger: INTEGER and REAL 1.0
    // arrive as the same JavaScript number. A numeric TEXT stays TEXT.
    sql.exec("CREATE TABLE typed_probe (id INTEGER PRIMARY KEY, value)");
    sql.exec("INSERT INTO typed_probe VALUES (0, 1), (1, 1.0), (2, 1e100), (3, NULL)");
    sql.exec(
      "INSERT INTO typed_probe VALUES (4, ?), (5, ?), (6, ?)",
      "9007199254740993",
      "a\0λ🙂",
      new Uint8Array([0, 127, 128, 255]).buffer,
    );
    const typed = rows(`SELECT typeof(value) AS type,
      CASE WHEN typeof(value) = 'integer' THEN CAST(value AS TEXT) ELSE value END AS value
      FROM typed_probe ORDER BY id`).map(({ type, value }) => ({
      type,
      value: type === "blob" ? Array.from(new Uint8Array(value)) : value,
    }));
    const aggregate = rows(`SELECT CAST(min(value) AS TEXT) AS minimum,
      CAST(max(value) AS TEXT) AS maximum FROM values_probe`)[0];
    const sum = rows(`SELECT CAST(sum(value) AS TEXT) AS value FROM values_probe
      WHERE id IN (7, 4)`)[0].value;
    const raw = rows("SELECT value FROM values_probe WHERE id = 7")[0].value;
    const realBinding = rows(
      "SELECT typeof(?) AS type, typeof(CAST(? AS REAL)) AS castType",
      1,
      1,
    )[0];
    // Typed projection must not move LIMIT or ordering outside numeric SQL.
    const top = rows(`SELECT CAST(value AS TEXT) AS exact FROM values_probe
      ORDER BY value DESC LIMIT 2`).map((row) => row.exact);
    const returning = rows(
      `UPDATE values_probe SET value = +CAST(? AS INTEGER)
      WHERE id = 4 RETURNING typeof(value) AS type, CAST(value AS TEXT) AS value`,
      integers[7],
    );
    let rollback;
    try {
      this.storage.transactionSync(() => {
        sql.exec("UPDATE values_probe SET value = +CAST(? AS INTEGER) WHERE id = 4", integers[0]);
        throw new Error("rollback");
      });
    } catch (error) {
      if (error.message !== "rollback") throw error;
      rollback = rows("SELECT CAST(value AS TEXT) AS value FROM values_probe WHERE id = 4")[0]
        .value;
    }
    return Response.json({
      integers,
      exact,
      matches,
      comparisons,
      typed,
      aggregate,
      sum,
      rawRounded: BigInt(raw).toString(),
      realBinding,
      top,
      returning,
      rollback,
    });
  }
}
export default {
  fetch(request, env) {
    return env.PROBE.get(env.PROBE.idFromName("values")).fetch(request);
  },
};
