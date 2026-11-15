import { schema as s, migration as m } from "jazz-tools";

export default m.defineMigration({
  migrate: {
    "tasks": {
      "estimate": m.add.float({ default: null }),
    },
  },
  fromHash: "e22329fe6202",
  toHash: "9e993cb6b8b6",
  from: {
  "tasks": s.table({
    "title": s.string(),
    "score": s.int(),
    "owner_id": s.uuid(),
    "dueAt": s.timestamp().optional(),
    "tags": s.array(s.string()),
    "meta": s.json().optional(),
  }, {

  })
},
  to: {
  "tasks": s.table({
    "title": s.string(),
    "score": s.int(),
    "estimate": s.float().optional(),
    "owner_id": s.uuid(),
    "dueAt": s.timestamp().optional(),
    "tags": s.array(s.string()),
    "meta": s.json().optional(),
  }, {

  })
},
});
