import { schema as s, migration as m } from "jazz-tools";

export default m.defineMigration({
  migrate: {
    team_notes: {
      author_name: m.add.string({ default: "unknown" }),
    },
  },
  fromHash: "3c6e476b058b",
  toHash: "e46d52123791",
  from: {
    team_notes: s.table(
      {
        team: s.string(),
        text: s.string(),
        sent_at: s.timestamp(),
      },
      {},
    ),
  },
  to: {
    team_notes: s.table(
      {
        team: s.string(),
        author_name: s.string(),
        text: s.string(),
        sent_at: s.timestamp(),
      },
      {},
    ),
  },
});
