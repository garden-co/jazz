import { schema as s } from "./index.js";
import type { Db } from "./runtime/db.js";

const base = s.table({ score: s.int(), label: s.string().optional() }, {});
const table = s.table(base.columns, {
  ...base.relations,
  priority: s.computed((row): "low" | "high" => {
    const score: number = row.score;
    const label: string | null = row.label;
    String(label);
    // @ts-expect-error unknown fields are not in the inferred row
    String(row.missing);
    // @ts-expect-error the dependency context is read-only
    row.score = 1;
    return score >= 8 ? "high" : "low";
  }),
});
const app = s.defineApp({ items: table });
declare const db: Db;
db.insert(app.items, { score: 9 });
db.update(app.items, "id", { score: 3 });
// @ts-expect-error computed fields are not insert inputs
db.insert(app.items, { score: 9, priority: "high" });
// @ts-expect-error computed fields cannot be updated
db.update(app.items, "id", { priority: "high" });
// @ts-expect-error computed fields are not stored filters
app.items.where({ priority: "high" });
// @ts-expect-error computed fields cannot be ordered in the stored query
app.items.orderBy("priority", "asc");
app.items.select("priority");
// @ts-expect-error computed fields cannot be indexed
table.indexOnly(["priority"]);
// @ts-expect-error a computed field is not a branch column
table.branchBy("priority");
s.table(base.columns, {
  ...base.relations,
  // @ts-expect-error callback values use the app-facing column type
  invalid: s.computed((row: { score: string }) => row.score),
});
const chained = s.table(table.columns, {
  ...table.relations,
  summary: s.computed((row) => {
    const priority: "low" | "high" = row.priority;
    const label: string | null = row.label;
    const id: string = row.id;
    return `${id}: ${priority} ${label ?? ""}`;
  }),
});
s.defineApp({ items: chained });

const values = s.defineApp({
  items: s.table(base.columns, {
    ...base.relations,
    object: s.computed((row) => ({ score: row.score })),
    list: s.computed((row) => [row.score]),
  }),
});
declare const result: s.RowOf<typeof values.items>;
// @ts-expect-error the computed property is read-only
result.object = { score: 1 };
// @ts-expect-error inferred computed object values are read-only
result.object.score = 1;
// @ts-expect-error inferred computed arrays are read-only
result.list.push(1);

s.definePermissions(app, ({ policy }) => {
  policy.items.allowRead.where((row) => {
    // @ts-expect-error computed values are not stored permission row references
    String(row.priority);
    return policy.items.exists.where({ id: row.id, score: row.score });
  });
  policy.items.allowUpdate.whereOld((row) => {
    // @ts-expect-error computed values are not stored permission row references
    String(row.priority);
    return policy.items.exists.where({ score: row.score });
  });
  policy.items.where((row) => {
    // @ts-expect-error computed values are not stored permission row references
    String(row.priority);
    return { score: row.score };
  });
});
