// #region schema-vue
import { col, table } from "jazz-tools";

table("todos", {
  title: col.string(),
  done: col.boolean(),
});
// #endregion schema-vue
