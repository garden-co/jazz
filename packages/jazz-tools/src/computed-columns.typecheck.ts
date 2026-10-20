import { schema as s } from "./index.js";
import type { Db } from "./runtime/db.js";

const base = s.table({ score: s.int(), label: s.string().optional() }, {});
const table = s.computed(base, {
  priority: {
    dependsOn: ["score"],
    compute: (row): "low" | "high" => {
      const score: number = row.score;
      // @ts-expect-error undeclared dependencies are not in the callback context
      row.label;
      // @ts-expect-error the dependency context is read-only
      row.score = 1;
      return score >= 8 ? "high" : "low";
    },
  },
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
s.computed(base, {
  invalid: {
    // @ts-expect-error a computed dependency must exist in the completed table
    dependsOn: ["unknown"],
    compute: () => 1,
  },
});
s.computed(base, {
  invalid: {
    dependsOn: ["score"],
    // @ts-expect-error dependency values use the app-facing column type
    compute: (row: { score: string }) => row.score,
  },
});
const chained = s.computed(table, {
  summary: {
    dependsOn: ["priority", "label", "id"],
    compute: (row) => {
      const priority: "low" | "high" = row.priority;
      const label: string | null = row.label;
      const id: string = row.id;
      return `${id}: ${priority} ${label ?? ""}`;
    },
  },
});
s.defineApp({ items: chained });
