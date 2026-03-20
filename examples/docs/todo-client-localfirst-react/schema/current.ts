// #region schema-react
import { table, col } from "jazz-tools";

table("todos", {
  title: col.string(),
  done: col.boolean(),
});
// #endregion schema-react
