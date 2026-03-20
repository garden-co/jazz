// #region schema-expo
import { table, col } from "jazz-tools";

table("todos", {
  title: col.string(),
  done: col.boolean(),
});
// #endregion schema-expo
