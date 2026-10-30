// #region computed-columns-ts
import { schema as s } from "jazz-tools";

type Priority = "low" | "medium" | "high";

const tasks = s.table(
  { title: s.string(), score: s.int() },
  {
    priority: s.computed(
      (row): Priority => (row.score >= 8 ? "high" : row.score >= 4 ? "medium" : "low"),
    ),
  },
);

export const app = s.defineApp({ tasks }, { migrations: "none" });
// #endregion computed-columns-ts
