// Written by hand: alpha.64's stub generator doesn't support payload enums.
import { schema as s, migration as m } from "jazz-tools";

export default m.defineMigration({
  migrate: {
    tickets: {
      category: m.add.enum("bug", "feature", { default: null }),
      severity: m.add.int({ default: null }),
      area: m.add.string({ default: null }),
    },
  },
  dropTables: { legacy_kinds: true },
  fromHash: "3453b5960772",
  toHash: "b6e088fdf048",
  from: {
    tickets: s.table({ title: s.string() }, {}),
    legacy_kinds: s.table(
      { kind: s.enum({ bug: { severity: s.int() }, feature: { area: s.string() } }).optional() },
      {},
    ),
  },
  to: {
    tickets: s.table(
      {
        title: s.string(),
        category: s.enum("bug", "feature").optional(),
        severity: s.int().optional(),
        area: s.string().optional(),
      },
      {},
    ),
  },
});
