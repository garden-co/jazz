import { schema as s, migration as m } from "jazz-tools";

export default m.defineMigration({
  migrate: {
    "boards": {
      "color": m.add.string({ default: null }),
    },
  },
  fromHash: "82d3aaff21db",
  toHash: "fb4d6e73ed8e",
  from: {
  "boards": s.table({
    "title": s.string(),
    "owner_id": s.uuid(),
  }, {
    "notes": s.reverse("notes", "board"),
  }),
  "notes": s.table({
    "text": s.string(),
    "owner_id": s.uuid(),
    "boardId": s.uuid().optional(),
  }, {
    "board": s.rel("boards", "boardId"),
  })
},
  to: {
  "boards": s.table({
    "title": s.string(),
    "color": s.string().optional(),
    "owner_id": s.uuid(),
  }, {
    "notes": s.reverse("notes", "board"),
  }),
  "notes": s.table({
    "text": s.string(),
    "owner_id": s.uuid(),
    "boardId": s.uuid().optional(),
  }, {
    "board": s.rel("boards", "boardId"),
  })
},
});
