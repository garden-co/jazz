import { schema as s, migration as m } from "jazz-tools";

export default m.defineMigration({
  migrate: {
    notes: {
      pinned: m.add.boolean({ default: false }),
    },
  },
  fromHash: "ee20b38fcdbe",
  toHash: "21cb7bdde685",
  from: {
    notes: s.table(
      {
        title: s.string(),
        body: s.string(),
      },
      {},
    ),
  },
  to: {
    notes: s.table(
      {
        title: s.string(),
        body: s.string(),
        pinned: s.boolean().default(false),
      },
      {},
    ),
  },
});
